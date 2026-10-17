/**
 * amazon-bulk.mjs — Amazon SKU の対応を CSV でまとめて登録する (試し取込つき・10/17 中原さん「作って」= 古い /apps/warehouse/register の CSV 一括登録の代わり)
 *
 * 流れ (画面 = views/amazon-bulk.ejs・public/me-amazon-bulk.js):
 *   1. POST /api/amazon/bulk/preview { file_name, data_b64 } = 試し取込 (読むだけ・何も書かない)。
 *      文字コード (UTF-8 の BOM あり / なし・Shift_JIS = Excel) を見分け → CSV を読む → 形 (2 つ) を見分け → seller SKU ごとにまとめる (行の順 = 構成の順・1 行目が代表) →
 *      Company DB の今の対応・出品・構成・NE の商品を 1 回ずつまとめて読み、SKU ごとに「作る / 直す / 墓標から戻す / 変わらない / だめ (理由)」と気をつけること (似た SKU など) を返す。
 *      保存してよい SKU には印 (切符の番号と中身の HMAC) を付ける = 保存は「試し取込で見た中身」だけを受ける
 *   2. POST /api/amazon/bulk/apply { ticket, items: [{ seller_sku, name, components, versions, mac }] } = 20 件ずつ。
 *      1 件ずつ 1 つの画面と同じ保存 (lib/amazon-map-write.mjs の saveAmazonMap) を呼ぶ = 門 (MASTER_EDIT_OPEN・段階・持ち主表)・版 (試し取込で読んだ版)・
 *      request_id (切符の番号と seller SKU から決まる = 押し直しても 2 回入らない)・構成品の決まり (componentUnusable・20 行・数 1〜999)・seller SKU の形・変更の記録は全部同じ。
 *      🚨 ここで DB に直接書かない (守りのある保存の道を飛ばさない)
 *   途中で 1 件だめでも、できた分は保存したまま (1 件 = 1 取引)。だめな分は理由を返す。同じ CSV を出し直す = できた分は「変わらない」になる
 * CSV の形 (どちらも 1 行目は見出し):
 *   simple    = seller_sku,name,ne_code,qty (見出しの名前で列を決める・並びは自由。name・qty は無くてもよい = 名前は今の名前 → Amazon の商品名 → 代表の NE の商品名、数は 1)
 *   warehouse = 古い SKU マスタの CSV (/apps/warehouse の「商品コード変換テーブル」・19 列)。1 列目 = seller SKU・3 列目 = 名前・4 列目 = NE の商品コード・5 列目 = 数 (import-sku-master.js と同じ)
 *   同じ seller SKU の行が複数 = 1 つのセット (上の行から順に構成・1 行目が代表)。名前が行で違う = 1 行目の名前 (気をつけること)
 * 上限: ファイル BULK_MAX_BYTES・行 BULK_MAX_ROWS・seller SKU BULK_MAX_SKUS。書けるのは名簿 (MASTER_EDITORS) の人だけ (router の editorGate)
 */
import crypto from 'node:crypto';
import iconv from 'iconv-lite';
import { normSku } from '../../lib/sku-norm.js';
import { COMPANY_ID, MasterWriteError, sha256, stable, REASON_MAX } from '../../lib/master-write.mjs';
import {
  saveAmazonMap, sellerSkuIn, newSellerSkuProblems, componentUnusable, nameBlank, amazonMapVersionsOf,
  AMAZON_JP_SHOP_CODE, MAX_MAP_COMPONENTS, MAX_MAP_QTY, MAX_MAP_NAME,
} from '../../lib/amazon-map-write.mjs';
import { similarAmazonSkus, sellerSkuWarnings, componentUnusableWhy } from './amazon-read.mjs';
import { readReportLoad } from './amazon-unlinked.mjs';

export const BULK_MAX_BYTES = 2 * 1024 * 1024;
export const BULK_MAX_ROWS = 10000;
export const BULK_MAX_SKUS = 2000;
export const BULK_APPLY_CHUNK = 20;
/** 似た seller SKU を確かめる上限 (まだ出品の無い SKU の上から。1 件 = 出品を 2 回読む) */
export const BULK_SIMILAR_MAX = 50;
export const BULK_TICKET_TTL_MS = 60 * 60e3;
export const BULK_FORMATS = Object.freeze({ simple: 'seller_sku,name,ne_code,qty (この画面の形)', warehouse: '古い SKU マスタの CSV (商品コード変換テーブル・19 列)' });
export const BULK_ACTIONS = Object.freeze({ create: '作る', create_new: '作る (出品も)', update: '直す', revive: '墓標から戻す', same: '変わらない', error: 'だめ' });
const SAVE_ACTIONS = new Set(['create', 'create_new', 'update', 'revive']);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const VERSION_RE = /^\d{1,19}:\d{1,19}(:(active|deleted))?$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const CTRL_ALL = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`, 'g');
const BOM = String.fromCharCode(0xfeff);
const bad = (status, reason, message, extra = {}) => new MasterWriteError(status, reason, message, extra);

// ─── 文字コード・CSV ───

/**
 * ファイルのバイト → 文字。UTF-8 (BOM あり / なし) をまず確かめ、UTF-8 として読めなければ Shift_JIS (CP932 = Windows の Excel の「CSV」)。
 *   UTF-16 (Excel の「Unicode テキスト」)・NUL を含む (表計算のファイルそのもの) = 断る。Shift_JIS としても読めない字 (置き換えの字) が出た = 断る
 *   戻り値 { text, encoding: 'utf-8-bom' | 'utf-8' | 'shift_jis' }
 */
export function decodeCsvBytes(buf) {
  const b = Buffer.from(buf);
  if (!b.length) throw bad(400, 'empty_file', 'ファイルが空です');
  if ((b[0] === 0xff && b[1] === 0xfe) || (b[0] === 0xfe && b[1] === 0xff)) throw bad(400, 'bad_encoding', 'UTF-16 (Excel の「Unicode テキスト」) のファイルは読めません。Excel で「CSV UTF-8 (コンマ区切り)」か「CSV (コンマ区切り)」で保存し直してください');
  if (b.includes(0x00)) throw bad(400, 'bad_encoding', 'CSV でないファイルのようです (Excel のファイル (.xlsx) そのものは読めません。「CSV」で保存し直してください)');
  const bom = b[0] === 0xef && b[1] === 0xbb && b[2] === 0xbf;
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bom ? b.subarray(3) : b);
    return { text, encoding: bom ? 'utf-8-bom' : 'utf-8' };
  } catch { /* UTF-8 でない = Shift_JIS を試す */ }
  const text = iconv.decode(b, 'cp932');
  if (text.includes(String.fromCharCode(0xfffd))) throw bad(400, 'bad_encoding', '文字コードが分かりません (UTF-8 でも Shift_JIS でも読めない字があります)。Excel で「CSV UTF-8 (コンマ区切り)」で保存し直してください');
  return { text, encoding: 'shift_jis' };
}
export const ENCODING_WORDS = Object.freeze({ 'utf-8-bom': 'UTF-8 (BOM つき)', 'utf-8': 'UTF-8', shift_jis: 'Shift_JIS (Excel の CSV)' });

/**
 * CSV の文字 → 行 (RFC 4180: "…" で囲む・中の "" = "・囲んだ中のカンマと改行はそのまま)。行の終わり = CRLF / LF / CR。
 *   全部空の行は飛ばす (行の番号 = 見出しを 1 行目として数えた番号を残す)。閉じていない " = 断る (どこで切れたか分からない)
 *   戻り値 [{ no, cells }]
 */
export function parseCsvText(text, { maxRows = BULK_MAX_ROWS + 1 } = {}) {
  const s = String(text).startsWith(BOM) ? String(text).slice(1) : String(text);
  const out = [];
  let cells = []; let cell = ''; let inQ = false; let closedQ = false; let no = 1; let startNo = 1;
  // 🆕 #Codex A R1 Low: 壊れた引用符 ("a001"x・引用符の無い欄の途中の ") は黙って読まずに断る (違う商品コードに変わらないように)
  const broken = () => bad(400, 'bad_csv', `CSV が壊れています (${no} 行目: 引用符 " の使い方が違います。" を含む欄は全体を "…" で囲み、中の " は "" にしてください)`);
  const endCell = () => { cells.push(cell); cell = ''; closedQ = false; };
  const endRow = () => {
    endCell();
    if (cells.some((c) => c.trim() !== '')) {
      out.push({ no: startNo, cells });
      if (out.length > maxRows) throw bad(413, 'too_many_rows', `行が多すぎます (見出しを除いて ${maxRows - 1} 行まで)。ファイルを分けてください`, { max: maxRows - 1 });
    }
    cells = [];
  };
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inQ) {
      if (c === '"') { if (s[i + 1] === '"') { cell += '"'; i++; } else { inQ = false; closedQ = true; } } else { if (c === '\n' || (c === '\r' && s[i + 1] !== '\n')) no++; cell += c; }
      continue;
    }
    if (closedQ && c !== ',' && c !== '\r' && c !== '\n') throw broken();
    if (c === '"') { if (cell !== '') throw broken(); inQ = true; continue; }
    if (c === ',') { endCell(); continue; }
    if (c === '\r' || c === '\n') {
      if (c === '\r' && s[i + 1] === '\n') i++;
      endRow(); no++; startNo = no;
      continue;
    }
    cell += c;
  }
  if (inQ) throw bad(400, 'bad_csv', `CSV が壊れています (${startNo} 行目から始まる " が閉じていません)`);
  if (cell !== '' || cells.length) endRow();
  return out;
}

const HEADER_NAMES = Object.freeze({
  seller_sku: ['seller_sku', 'seller sku', 'sellersku', 'amazon sku', 'amazon の seller sku'],
  name: ['name', '名前', '名前 (社内)', '商品名'],
  ne_code: ['ne_code', 'ne code', 'ne商品コード', 'ne の商品コード', 'neコード', 'ne コード'],
  qty: ['qty', '数量', '数'],
});
const headKey = (v) => String(v ?? '').replace(BOM, '').trim().toLowerCase().replace(/\s+/g, ' ');
/**
 * 見出しの行 → 形と列。simple = 見出しの名前 (seller_sku と ne_code は要る・name と qty は無くてもよい) / warehouse = 古い SKU マスタの CSV (1 列目が sku・5 列以上・
 * または 19 列ちょうど) = 1・3・4・5 列目 (import-sku-master.js の COL_SKU 0・COL_NAME 2・COL_NE 3・COL_QTY 4)。分からない = 断る
 */
export function detectFormat(header) {
  const h = (header || []).map(headKey);
  const at = (k) => h.findIndex((x) => HEADER_NAMES[k].includes(x));
  const col = { seller_sku: at('seller_sku'), name: at('name'), ne_code: at('ne_code'), qty: at('qty') };
  if (col.seller_sku >= 0 && col.ne_code >= 0) return { format: 'simple', col };
  if (h.length >= 5 && (h[0] === 'sku' || h.length === 19)) return { format: 'warehouse', col: { seller_sku: 0, name: 2, ne_code: 3, qty: 4 } };
  throw bad(400, 'unknown_format', '見出しの行が分かりません。1 行目を「seller_sku,name,ne_code,qty」にするか、古い SKU マスタの CSV (1 列目が sku の 19 列) をそのまま使ってください (見本の CSV をダウンロードできます)');
}

/** 全角の数字も数として読む (Excel で全角になった数) */
const toHalfDigits = (s) => String(s ?? '').replace(/[０-９]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0));

/**
 * 行 → seller SKU ごと。戻り値 { format, skus: [{ key, sku, rawSku, line, name, lines: [{ no, code, qty }], errors, warnings }], lineErrors: [{ no, raw, message }] }
 *   key = 正規化した seller SKU (同じ SKU の違う書き方をまとめる)。行の誤りは、その SKU 全体の誤り (一部だけ保存しない)
 */
export function groupCsvRows(records) {
  if (!records.length) throw bad(400, 'empty_file', 'ファイルに行がありません (1 行目は見出し)');
  const { format, col } = detectFormat(records[0].cells);
  const data = records.slice(1);
  if (!data.length) throw bad(400, 'empty_file', '見出しの行の下に、seller SKU の行がありません');
  const by = new Map();
  const lineErrors = [];
  const cellAt = (r, i) => (i >= 0 && i < r.cells.length ? String(r.cells[i] ?? '') : '');
  for (const r of data) {
    const rawSku = cellAt(r, col.seller_sku);
    if (!rawSku.trim()) { lineErrors.push({ no: r.no, raw: '', message: `${r.no} 行目: seller SKU が空です` }); continue; }
    let sku;
    try { sku = sellerSkuIn(rawSku); } catch (e) {
      if (!(e instanceof MasterWriteError)) throw e;
      lineErrors.push({ no: r.no, raw: rawSku.replace(CTRL_ALL, ' ').slice(0, 80), message: `${r.no} 行目: ${e.message}` });
      continue;
    }
    const key = normSku(sku);
    let g = by.get(key);
    if (!g) { g = { key, sku, rawSku: rawSku.trim(), line: r.no, name: '', names: [], lines: [], codes: new Set(), over: 0, errors: [], warnings: [] }; by.set(key, g); }
    if (sku !== g.sku) g.errors.push(`${r.no} 行目: 同じ seller SKU が ${g.line} 行目と違う書き方 (${g.sku} と ${sku}) です。どちらかにそろえてください`);
    const name = cellAt(r, col.name).replace(CTRL_ALL, ' ').trim();
    if (name) { if (!g.name) g.name = name; else if (name !== g.name && !g.names.includes(name)) g.names.push(name); }
    const code = cellAt(r, col.ne_code).trim();
    const qRaw = toHalfDigits(cellAt(r, col.qty)).trim();
    if (!code) { g.errors.push(`${r.no} 行目: NE の商品コードが空です`); continue; }
    if (CTRL_RE.test(code) || [...code].length > 60) { g.errors.push(`${r.no} 行目: NE の商品コード ${code.replace(CTRL_ALL, ' ').slice(0, 60)} が使えない形です (60 文字まで・改行なし)`); continue; }
    const qty = qRaw === '' ? 1 : /^\d{1,4}$/.test(qRaw) ? Number(qRaw) : NaN;
    if (!Number.isInteger(qty) || qty < 1 || qty > MAX_MAP_QTY) { g.errors.push(`${r.no} 行目: ${code} の数「${qRaw}」は 1〜${MAX_MAP_QTY} の整数にしてください`); continue; }
    // 同じ品の確かめは Set (行が多くても 2 乗にしない・#Codex A R1 M)。構成品の上限を超えた行は数えるだけ (持たない)
    const nk = normSku(code);
    if (g.codes.has(nk)) { g.errors.push(`${r.no} 行目: ${code} が同じ seller SKU に 2 回あります (同じ品は 1 行にして数で)`); continue; }
    if (g.lines.length >= MAX_MAP_COMPONENTS) { g.over++; continue; }
    g.codes.add(nk);
    g.lines.push({ no: r.no, code, qty });
  }
  const skus = [...by.values()];
  if (skus.length > BULK_MAX_SKUS) throw bad(413, 'too_many_skus', `seller SKU が多すぎます (1 回に ${BULK_MAX_SKUS} 件まで・いま ${skus.length} 件)。ファイルを分けてください`, { max: BULK_MAX_SKUS, count: skus.length });
  for (const g of skus) {
    if (g.over) g.errors.push(`構成品が ${g.lines.length + g.over} 行あります (1〜${MAX_MAP_COMPONENTS} 行)`);
    delete g.codes;
    if (g.errors.length > 50) g.errors = [...g.errors.slice(0, 50), `ほか ${g.errors.length - 50} 件の誤り`];
    if (g.names.length) g.warnings.push(`名前が行によって違います。上の行の名前「${g.name}」を使います (ほか: ${g.names.slice(0, 3).join(' / ')})`);
    if (g.name) {
      if ([...g.name].length > MAX_MAP_NAME) g.errors.push(`名前は ${MAX_MAP_NAME} 文字までです`);
      else if (nameBlank(g.name)) g.name = '';
    }
  }
  return { format, skus, lineErrors };
}

// ─── 切符 (試し取込で見た中身だけを保存する) ───

let ticketKey = process.env.MASTER_BULK_TICKET_KEY ? Buffer.from(String(process.env.MASTER_BULK_TICKET_KEY)) : crypto.randomBytes(32);
export function __setBulkTicketKey(k) { ticketKey = k ? Buffer.from(String(k)) : crypto.randomBytes(32); }
const hmac = (s) => crypto.createHmac('sha256', ticketKey).update(s).digest('hex');
const sameHex = (a, b) => typeof a === 'string' && typeof b === 'string' && a.length === b.length && /^[0-9a-f]+$/.test(a) && crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
/** 保存する中身 (seller SKU・名前・構成・読んだ版) のハッシュ */
const itemHashOf = (it) => sha256(stable({ sku: it.seller_sku, name: it.name, components: it.components.map((c) => ({ code: c.code, qty: Number(c.qty) })), versions: it.versions }));
export const bulkItemMac = (ticketId, it) => hmac(`amazon-bulk-item|${ticketId}|${itemHashOf(it)}`);
/** 1 件の保存の request_id = 切符の番号と seller SKU から決まる (押し直し = 同じ番号 = 前の結果) */
export function bulkRequestIdOf(ticketId, sellerSku) {
  const h = sha256(`amazon-bulk:${ticketId}:${sellerSku}`);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${'89ab'[parseInt(h[16], 16) % 4]}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}
const TICKET_RE = /^([A-Za-z0-9_-]{10,4000})\.([0-9a-f]{64})$/;
function issueBulkTicket({ actor, fileName, n, nowMs }) {
  const id = crypto.randomUUID();
  const body = { v: 1, id, actor, file: fileName, n, exp: nowMs + BULK_TICKET_TTL_MS };
  const b = Buffer.from(JSON.stringify(body)).toString('base64url');
  return { ticket: `${b}.${hmac(`amazon-bulk-ticket|${b}`)}`, id };
}
export function readBulkTicket(ticket, nowMs = Date.now()) {
  const again = (reason, why) => bad(409, reason, `${why}。何も保存していません。CSV をもう一度「試し取込」してください`);
  const m = TICKET_RE.exec(String(ticket ?? ''));
  if (!m || !sameHex(m[2], hmac(`amazon-bulk-ticket|${m[1]}`))) throw again('ticket_invalid', '試し取込の切符が無いか、正しくありません (サーバーの入れ替えで古くなった・書き換えた)');
  let t;
  try { t = JSON.parse(Buffer.from(m[1], 'base64url').toString('utf8')); } catch { throw again('ticket_invalid', '試し取込の切符を読めません'); }
  if (!t || t.v !== 1 || !UUID_RE.test(String(t.id)) || !Number.isSafeInteger(t.n) || t.n < 1 || t.n > BULK_MAX_SKUS) throw again('ticket_invalid', '試し取込の切符の形が違います');
  if (!(Number(t.exp) > nowMs)) throw again('ticket_expired', `試し取込から ${BULK_TICKET_TTL_MS / 60e3} 分を過ぎました`);
  return t;
}
/** ファイル名 (画面に出す・記録の理由に入れる) = 制御文字を除いて 80 文字まで */
export const cleanFileName = (v) => [...String(v ?? '').replace(CTRL_ALL, ' ').trim()].slice(0, 80).join('') || '(名前なし)';
/** 保存の理由 (変更の記録に残る) = CSV の一括登録・ファイル名・件数 (REASON_MAX まで) */
export const bulkReasonOf = (t) => [...`CSV 一括登録 (${t.file} · ${t.n} 件)`].slice(0, REASON_MAX).join('');

// ─── 試し取込 (読むだけ) ───

/**
 * 試し取込。input = { file_name, data_b64 }・opts = { actor, nowMs }。何も書かない (読むだけ)。
 * 戻り値 { ok, file, encoding, format, counts, rows: [{ seller_sku, action, words, name, name_from, components, from, notes, warnings, errors, item? }], line_errors, ticket, chunk }
 *   item = 保存してよい SKU の中身 + 印 (保存の API にそのまま返す)
 */
export async function bulkPreview(db, input, { actor, nowMs = Date.now() } = {}) {
  if (!actor) throw bad(400, 'invalid_input', '保存する人 (ログインのメール) が分からない');
  const fileName = cleanFileName(input?.file_name);
  const b64 = typeof input?.data_b64 === 'string' ? input.data_b64 : '';
  if (!b64 || !/^[A-Za-z0-9+/]+={0,2}$/.test(b64)) throw bad(400, 'invalid_input', 'ファイルの中身が届いていません (選び直してください)');
  if (Math.floor(b64.length * 3 / 4) > BULK_MAX_BYTES + 3) throw bad(413, 'too_large', `ファイルが大きすぎます (${BULK_MAX_BYTES / 1024 / 1024} MB まで)。ファイルを分けてください`, { max: BULK_MAX_BYTES });
  const buf = Buffer.from(b64, 'base64');
  if (buf.length > BULK_MAX_BYTES) throw bad(413, 'too_large', `ファイルが大きすぎます (${BULK_MAX_BYTES / 1024 / 1024} MB まで)。ファイルを分けてください`, { max: BULK_MAX_BYTES });
  const { text, encoding } = decodeCsvBytes(buf);
  const { format, skus, lineErrors } = groupCsvRows(parseCsvText(text));
  const cur = await readCurrentMaps(db, skus.map((g) => g.sku));
  const codes = [...new Set(skus.flatMap((g) => g.lines.map((l) => l.code)))];
  const ne = await resolveCodes(db, codes);
  const titles = await reportTitles(db, skus.filter((g) => !g.name).map((g) => g.sku), nowMs);
  const rows = [];
  let similarLeft = BULK_SIMILAR_MAX; let similarSkipped = 0;
  for (const g of skus) {
    const c = cur.get(g.sku) || { listing: null, map: null, components: [], norm: '' };
    const row = { seller_sku: g.sku, line: g.line, csv_sku: g.rawSku, errors: [...g.errors], warnings: [...g.warnings], notes: [], name: null, name_from: null, components: [], from: null };
    // 構成品 (保存と同じ決まり = componentUnusable。DB の関数が最後にもう一度確かめる)
    const ids = new Set();
    for (const l of g.lines) {
      const k = ne.get(l.code);
      if (!k || !k.sku_id) { row.errors.push(`${l.no} 行目: NE の商品コード ${l.code} は Company DB にありません`); continue; }
      const why = componentUnusableWhy(k.sku_kind, k.state);
      if (why) { row.errors.push(`${l.no} 行目: ${k.code} は構成品に使えません (${why})`); continue; }
      if (ids.has(k.sku_id)) { row.errors.push(`${l.no} 行目: ${k.code} が同じ seller SKU に 2 回あります (同じ品は 1 行にして数で)`); continue; }
      ids.add(k.sku_id);
      row.components.push({ code: k.code, qty: l.qty, name: k.name, sku_id: k.sku_id, not_in_ne: k.state === 'draft' || k.state === 'ne_pending' });
    }
    if (row.components.some((x) => x.not_in_ne)) row.warnings.push('まだ NE に無い新商品 (下書き・NE 登録待ち) が入っています (NE に入るまで FBA 補充は保留)');
    // まだ出品の無い seller SKU = 先に作れる形だけ (保存の lib と同じ newSellerSkuProblems)
    if (!c.listing) {
      for (const p of newSellerSkuProblems(g.sku, c.norm)) row.errors.push(`まだ出品の無い seller SKU は、この形では先に作れません: ${p}`);
    }
    // 名前 = CSV → 今の対応の名前 → Amazon の出品一覧の商品名 → 出品のタイトル → 代表 (1 行目) の NE の商品名
    const def = [[g.name, 'csv'], [c.map && c.map.name, 'map'], [titles.get(g.sku), 'report'], [c.listing && c.listing.title, 'listing'], [row.components[0] && row.components[0].name, 'ne']]
      .map(([v, f]) => [cleanText(v), f]).find(([v]) => v);
    if (def) { row.name = def[0]; row.name_from = def[1]; } else row.errors.push('名前がありません (CSV の name の列に入れてください)');
    if (row.name_from && row.name_from !== 'csv') row.notes.push(`名前は CSV に無いので ${NAME_FROM_WORDS[row.name_from]} を使います`);
    // する事
    const curComps = c.components.map((x) => `${x.code}×${x.qty}`).join(', ');
    const newComps = row.components.map((x) => `${x.code}×${x.qty}`).join(', ');
    if (row.errors.length) row.action = 'error';
    else if (c.map && c.map.state === 'active') {
      const sameComps = c.components.length === row.components.length && c.components.every((x, i) => String(x.sku_id) === String(row.components[i].sku_id) && Number(x.qty) === Number(row.components[i].qty));
      const sameName = c.map.name === row.name;
      row.action = sameComps && sameName ? 'same' : 'update';
      row.from = { name: c.map.name, components: curComps };
      if (!sameName) row.notes.push(`名前: ${c.map.name} → ${row.name}`);
      if (!sameComps) row.notes.push(`構成: ${curComps} → ${newComps}`);
    } else if (c.map && c.map.state === 'deleted') {
      row.action = 'revive'; row.from = { name: c.map.name, components: '' };
      row.notes.push('削除済み (墓標) の seller SKU をもう一度登録します (登録日は新しくなります)');
    } else if (c.listing) {
      row.action = 'create'; row.from = { name: null, components: curComps };
      if (curComps && curComps !== newComps) row.notes.push(`今の構成 (夜間の取り込み): ${curComps} → ${newComps}`);
    } else {
      row.action = 'create_new';
      row.notes.push('Company DB にまだ出品がありません (保存すると出品も作ります・ASIN は Amazon に出品した後に夜間の取り込みが付けます)');
      row.warnings.push(...sellerSkuWarnings(g.sku));
      if (similarLeft > 0) {
        similarLeft--;
        const sim = await similarAmazonSkus(db, g.sku);
        if (sim.rows.length) row.warnings.push(`似た seller SKU があります (打ち間違いでないか): ${sim.rows.map((x) => x.seller_sku).join('・')}${sim.total > sim.rows.length ? ` ほか ${sim.total - sim.rows.length} 件` : ''}`);
      } else similarSkipped++;
    }
    row.versions = c.versions;
    rows.push(row);
  }
  const saveRows = rows.filter((r) => SAVE_ACTIONS.has(r.action));
  const counts = { skus: rows.length, lines: skus.reduce((a, g) => a + g.lines.length, 0), line_errors: lineErrors.length };
  for (const k of Object.keys(BULK_ACTIONS)) counts[k] = rows.filter((r) => r.action === k).length;
  counts.warn = rows.filter((r) => r.warnings.length).length;
  let ticket = null;
  if (saveRows.length) {
    const t = issueBulkTicket({ actor, fileName, n: saveRows.length, nowMs });
    ticket = t.ticket;
    for (const r of saveRows) {
      const item = { seller_sku: r.seller_sku, name: r.name, components: r.components.map((x) => ({ code: x.code, qty: x.qty })), versions: r.versions };
      r.item = { ...item, mac: bulkItemMac(t.id, item) };
    }
  }
  for (const r of rows) { r.words = BULK_ACTIONS[r.action]; r.components = r.components.map((x) => ({ code: x.code, qty: x.qty, name: x.name })); delete r.versions; }
  console.log(`[amazon-bulk] 試し取込 ${actor}: ${fileName} (${ENCODING_WORDS[encoding]} / ${format}) seller SKU ${counts.skus} 件 = 作る ${counts.create + counts.create_new}・直す ${counts.update}・戻す ${counts.revive}・変わらない ${counts.same}・だめ ${counts.error} / 行の誤り ${counts.line_errors}`);
  return { ok: true, file: fileName, encoding, encoding_words: ENCODING_WORDS[encoding], format, format_words: BULK_FORMATS[format], counts, rows, line_errors: lineErrors, ticket, chunk: BULK_APPLY_CHUNK,
    similar_skipped: similarSkipped, similar_max: BULK_SIMILAR_MAX };
}
const NAME_FROM_WORDS = Object.freeze({ map: '今の対応の名前', report: 'Amazon の出品一覧の商品名', listing: '出品のタイトル', ne: '代表 (1 行目) の NE の商品名' });
const cleanText = (v) => [...String(v ?? '').replace(CTRL_ALL, ' ').trim()].slice(0, MAX_MAP_NAME).join('').trim();

/** seller SKU (sellerSkuIn の後) → 今の出品・対応・構成・版 (正規化したコードで引く・まとめて 3 回) */
async function readCurrentMaps(db, skus) {
  const out = new Map();
  if (!skus.length) return out;
  const rows = (await db.query(`select x as sku, core.norm_code(x) as norm, l.listing_id::text as listing_id, l.listing_code, l.title, l.version::text as version,
        m.name as map_name, m.state as map_state, m.version::text as map_version
      from unnest($1::text[]) as t(x)
      left join core.listings l on l.company_id = $2 and l.mall = 'amazon' and l.shop_code = $3 and l.listing_norm = core.norm_code(x)
      left join core.amazon_sku_maps m on m.listing_id = l.listing_id`, [skus, COMPANY_ID, AMAZON_JP_SHOP_CODE])).rows;
  const lids = rows.filter((r) => r.listing_id).map((r) => r.listing_id);
  const comps = new Map();
  if (lids.length) {
    for (const c of (await db.query(`select c.listing_id::text as listing_id, c.sku_id::text as sku_id, k.code, c.qty from core.listing_components c join core.skus k on k.sku_id = c.sku_id
        where c.listing_id = any($1::bigint[]) order by c.listing_id, c.sort_order, k.code_norm`, [lids])).rows) {
      if (!comps.has(c.listing_id)) comps.set(c.listing_id, []);
      comps.get(c.listing_id).push({ sku_id: c.sku_id, code: c.code, qty: Number(c.qty) });
    }
  }
  for (const r of rows) {
    const listing = r.listing_id ? { listing_id: r.listing_id, version: r.version, title: r.title, listing_code: r.listing_code } : null;
    const map = r.listing_id && r.map_state ? { listing_id: r.listing_id, version: r.map_version, state: r.map_state, name: r.map_name } : null;
    out.set(r.sku, { norm: r.norm || '', listing, map, components: listing ? comps.get(r.listing_id) || [] : [], versions: amazonMapVersionsOf({ listing, map }) });
  }
  return out;
}
/** NE の商品コード → SKU (保存の resolveComponents と同じ引き方 = code_norm = core.norm_code)・名前・種類・登録の状態 (まとめて 1 回) */
async function resolveCodes(db, codes) {
  if (!codes.length) return new Map();
  const rows = (await db.query(`select x as raw, k.sku_id::text as sku_id, k.code, k.name, k.sku_kind, reg.state
      from unnest($1::text[]) as t(x) left join core.skus k on k.company_id = $2 and k.code_norm = core.norm_code(x)
      left join ops.master_registrations reg on reg.sku_id = k.sku_id`, [codes, COMPANY_ID])).rows;
  return new Map(rows.map((r) => [r.raw, r]));
}
/** 名前が CSV に無い SKU の Amazon の商品名 (出品一覧の今の取り込み・読めない = 無し) */
async function reportTitles(db, skus, nowMs) {
  const out = new Map();
  if (!skus.length) return out;
  try {
    const head = await readReportLoad(db, { nowMs });
    if (head.state !== 'ok') return out;
    for (const r of (await db.query(`select x as sku, (select r.item_name from ops.amazon_listing_report r where r.company_id = $2 and r.shop_code = $3 and r.load_id = $4::bigint
          and r.sku_norm = core.norm_code(x) and nullif(btrim(r.item_name), '') is not null order by r.seller_sku limit 1) as t from unnest($1::text[]) as t(x)`,
    [skus, COMPANY_ID, AMAZON_JP_SHOP_CODE, head.load.load_id])).rows) if (r.t) out.set(r.sku, r.t);
  } catch (e) { console.error(`[amazon-bulk] Amazon の商品名を読めない: ${e && e.message}`); }
  return out;
}

// ─── 保存 (20 件ずつ・1 件ずつ 1 つの画面と同じ保存) ───

function itemIn(x, t) {
  const sku = (() => { try { return sellerSkuIn(String(x?.seller_sku ?? '')); } catch { return null; } })();
  if (!sku) throw bad(400, 'invalid_input', 'seller SKU の形が違います。何も保存していません');
  const comps = Array.isArray(x?.components) ? x.components.map((c) => ({ code: String(c?.code ?? ''), qty: Number(c?.qty) })) : null;
  const v = x?.versions;
  const versions = v && typeof v === 'object' && !Array.isArray(v) ? { listing: v.listing ?? null, map: v.map ?? null } : null;
  if (!comps || !versions || ![versions.listing, versions.map].every((y) => y === null || (typeof y === 'string' && VERSION_RE.test(y))) || typeof x?.name !== 'string') {
    throw bad(400, 'invalid_input', `${sku}: 送った中身の形が違います。何も保存していません`);
  }
  const item = { seller_sku: sku, name: x.name, components: comps, versions };
  if (!sameHex(String(x?.mac ?? '').toLowerCase(), bulkItemMac(t.id, item))) throw bad(400, 'ticket_mismatch', `${sku} は試し取込で見た中身と違います (印が違う)。何も保存していません。CSV をもう一度「試し取込」してください`);
  return item;
}
/**
 * 20 件ずつ保存。input = { ticket, items }・opts = { actor, open (MASTER_EDIT_OPEN)・nowMs }。戻り値 { ok, results: [{ seller_sku, ok, result?, error? }] }
 *   切符の人と保存する人が同じ・印が正しい (= 試し取込で「保存してよい」と見た中身) ときだけ保存の lib を呼ぶ。1 件の誤り = その件だけ (ほかは続ける)。
 *   接続が切れた (保存の lib の外の誤り) = 残りは送らない (押し直しで続きから・同じ request_id)
 */
export async function bulkApplyChunk(db, input, { actor, open = false, nowMs = Date.now() } = {}) {
  const t = readBulkTicket(input?.ticket, nowMs);
  if (!actor || t.actor !== actor) throw bad(403, 'ticket_actor', '試し取込をした人と保存する人が違います。何も保存していません');
  const raw = input?.items;
  if (!Array.isArray(raw) || !raw.length) throw bad(400, 'invalid_input', '送る seller SKU がありません');
  if (raw.length > BULK_APPLY_CHUNK) throw bad(400, 'invalid_input', `1 回に送るのは ${BULK_APPLY_CHUNK} 件までです`);
  const items = raw.map((x) => itemIn(x, t));
  if (new Set(items.map((x) => normSku(x.seller_sku))).size !== items.length) throw bad(400, 'invalid_input', '同じ seller SKU が 2 回あります。何も保存していません');
  const reason = bulkReasonOf(t);
  const results = [];
  let broken = null;
  for (const it of items) {
    if (broken) { results.push({ seller_sku: it.seller_sku, ok: false, error: { reason: 'not_sent', message: 'Company DB との接続が切れたので送っていません (もう一度押すと続きから)' } }); continue; }
    try {
      const r = await saveAmazonMap(db, { actor, requestId: bulkRequestIdOf(t.id, it.seller_sku), sellerSku: it.seller_sku, name: it.name, components: it.components, reason, seen: { versions: it.versions } }, { open });
      results.push({ seller_sku: it.seller_sku, ok: true, result: { created: !!r.created, listing_created: !!r.listing_created, revived: !!r.revived, no_change: !!r.no_change, replayed: !!r.replayed, components: r.components || null } });
    } catch (e) {
      // 保存の lib の誤り = 確かな答え。前に残した「サーバーエラー」の押し直し (reason error・replayed) も確かな失敗 = failed (画面がその件で止まり続けない)
      if (e instanceof MasterWriteError) { results.push({ seller_sku: it.seller_sku, ok: false, error: { reason: e.reason === 'error' ? 'failed' : e.reason, message: e.message } }); continue; }
      const locked = e && (e.code === '55P03' || e.code === '40P01' || e.code === '40001');
      if (locked) { results.push({ seller_sku: it.seller_sku, ok: false, error: { reason: 'locked', message: 'ほかの処理とぶつかりました。何も保存していません (CSV をもう一度「試し取込」してから保存してください)' } }); continue; }
      console.error(`[amazon-bulk] ${it.seller_sku}: ${e && e.stack || e}`);
      broken = e;
      results.push({ seller_sku: it.seller_sku, ok: false, error: { reason: 'error', message: 'サーバーエラー (この seller SKU は保存していないかもしれません。もう一度押すと確かめて続けます)' } });
    }
  }
  const ok = results.filter((r) => r.ok).length;
  console.log(`[amazon-bulk] 保存 ${actor}: ${t.file} ${results.length} 件 (できた ${ok}・だめ ${results.length - ok})`);
  return { ok: true, results };
}

/** 見本の CSV (この画面の形・BOM つき UTF-8・CRLF)。1 つの SKU に 2 行 = 2 品のセット (1 行目が代表) */
export function bulkTemplateCsv() {
  const lines = ['seller_sku,name,ne_code,qty', 'pr_sample001,はちみつ 500g,ne-code-1,1', 'pr_sample002,はちみつ 2 個組 + スプーン,ne-code-1,2', 'pr_sample002,はちみつ 2 個組 + スプーン,ne-code-2,1'];
  return `${BOM}${lines.join('\r\n')}\r\n`;
}
