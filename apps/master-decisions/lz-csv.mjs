/**
 * lz-csv.mjs — ロジザード用の 2 つの CSV (毎日の商品マスタ・新商品) を、NE の取得の値から GAS と同じ形で作る
 *   (マスタ正本切替 ③b-2a = 影運転。設計 = AI_reference CompanyDB構想/10 §6.2「③b-2」契約 v1〜v3 と実測)
 *
 * GAS の出力の形 (2026-09-24 の毎日の商品マスタ 5,008 行を、NE の取得の値からバイトまで全部再現して確かめた):
 *   - Shift_JIS・CRLF・最後の行に改行なし・BOM なし・1 行目が見出し
 *   - 文字 = CP932 の表で書く。ただし NEC 特殊文字 (CP932 の 0x87xx = ① ㎏ など) と CP932 に無い文字 (➁・NE の側で化けた U+FFFD) は「?」。
 *     ～ (U+FF5E) と － (U+FF0D) は Windows の対応のまま (0x8160・0x817C)
 *   - 引用符 = カンマ・"・改行を含む値だけ "…" で囲み、中の " は "" に重ねる (" は 2 行で実測・カンマは GAS のコード)
 *   - 仕入単価 = NE の原価の元の値 "N.00" の N (0.00 → 0)。取引先 = NE の仕入先コード (4 桁) のまま
 * 🚨 実測で確かめていない形 (IBM 拡張・半角カナ・Windows と JIS で対応が分かれる文字・制御文字・原価の小数・数字に見える値 など) は
 *    「まだ確かめていない」(unverified) の印を付けて、推測で書く。突き合わせで GAS と同じなら確かめたことになり、違えば「判定できない」
 *    (推測のまま合格にしない。契約 v3「丸め・0 件の形は実測で決める」)
 * このファイルは値を作るだけ (読み書き・DB は呼び手)
 */
import { createRequire } from 'node:module';
import iconv from 'iconv-lite';

export const LZ_CONVERTER_VERSION = 'lz-v1';
/** CP932 の表は iconv-lite の版で決まる。版が変わったら表の実測をやり直す (契約 v2 M6) */
export const ICONV_EXPECTED = '0.6.3';
export const iconvVersion = () => createRequire(import.meta.url)('iconv-lite/package.json').version;

export const DAILY = Object.freeze({ file: 'logizard_shohinmaster_upload.csv', header: Object.freeze(['形式/型番', '商品名', 'ふりがな', '仕入単価', '取引先id']) });
export const NEW = Object.freeze({
  file: 'logizard_bc_upload.csv',
  header: Object.freeze(['商品ID', '商品名', '検索名称', '仕入単価', '有効期限区分', '入荷日管理フラグ', '取引先コード', 'バーコード']),
  human: Object.freeze([4, 5, 7]),   // 人がシートに入れる 3 列 (有効期限区分・入荷日管理フラグ・バーコード) = 空で書き、突き合わせでは比べない (中原さん L-2)
});

const hexCp = (cp) => `U+${cp.toString(16).toUpperCase().padStart(4, '0')}`;
/** 実測で Windows の対応だった文字 (～ －) */
const OBSERVED_SPLIT = new Set([0xff5e, 0xff0d]);
/** Windows (CP932) と JIS で Unicode の対応が分かれる文字。実測していないものは推測 */
const SPLIT = new Set([0xff5e, 0x301c, 0x2225, 0x2016, 0xff0d, 0x2212, 0xffe0, 0x00a2, 0xffe1, 0x00a3, 0xffe2, 0x00ac, 0x2015, 0x2014, 0x00a5, 0x203e, 0x005c]);
/** NEC 特殊文字のうち IBM 拡張にも同じ字があるもの (Ⅰ〜Ⅹ № ℡ ㈱)。GAS が「?」にするか IBM 拡張の番号で書くかは未実測 */
const NEC_IBM_DUP = new Set([0x2160, 0x2161, 0x2162, 0x2163, 0x2164, 0x2165, 0x2166, 0x2167, 0x2168, 0x2169, 0x2116, 0x2121, 0x3231]);

/**
 * 文字列 → GAS と同じ Shift_JIS のバイト列。
 * @returns {{ bytes: Buffer, subs: Array<{at, ch, cp, why}>, unverified: Array<{at, ch, cp, why}> }}
 *   subs = 「?」にした文字 (why: nec_special / not_in_cp932 / ne_replacement = NE の側ですでに化けていた)。at = 何文字目 (0 から)
 */
export function encodeText(s) {
  const out = [], subs = [], unverified = [];
  let at = 0;
  for (const ch of String(s)) {
    const cp = ch.codePointAt(0);
    const pos = at++;
    const note = (list, why) => list.push({ at: pos, ch, cp: hexCp(cp), why });
    if (cp < 0x20 || cp === 0x7f) { note(unverified, 'control'); out.push(cp); continue; }
    if (cp < 0x80 && cp !== 0x5c) { out.push(cp); continue; }
    if (cp === 0xfffd) { note(subs, 'ne_replacement'); out.push(0x3f); continue; }
    if (cp > 0xffff) { note(unverified, 'astral'); note(subs, 'not_in_cp932'); out.push(0x3f); continue; }   // 1 文字で「?」1 つ (未実測)
    if (SPLIT.has(cp) && !OBSERVED_SPLIT.has(cp)) note(unverified, 'jis_windows_split');
    const b = iconv.encode(ch, 'cp932');
    if (b.length === 1 && b[0] === 0x3f) { note(subs, 'not_in_cp932'); out.push(0x3f); continue; }
    if (b.length === 1) { if (b[0] >= 0xa1 && b[0] <= 0xdf) note(unverified, 'halfwidth_kana'); out.push(b[0]); continue; }
    const lead = b[0];
    if (lead === 0x87) { if (NEC_IBM_DUP.has(cp)) note(unverified, 'nec_ibm_dup'); note(subs, 'nec_special'); out.push(0x3f); continue; }
    if (lead === 0xed || lead === 0xee || (lead >= 0xfa && lead <= 0xfc)) note(unverified, 'ibm_ext');
    else if (lead >= 0xf0 && lead <= 0xf9) note(unverified, 'user_defined');
    out.push(b[0], b[1]);
  }
  return { bytes: Buffer.from(out), subs, unverified };
}

/** 1 つのセル = バイト列 (引用符つき)。" は Shift_JIS の 2 バイト目に出ない (0x40 以上) のでバイトで重ねてよい */
export function cellBytes(text) {
  const t = String(text);
  const e = encodeText(t);
  if (!/[",\r\n]/.test(t)) return { ...e, quoted: false };
  const out = [0x22];
  for (const b of e.bytes) { out.push(b); if (b === 0x22) out.push(0x22); }
  out.push(0x22);
  return { bytes: Buffer.from(out), subs: e.subs, unverified: e.unverified, quoted: true };
}

/** シートが数字・日付に読み替えるかもしれない値 (GAS はシートを通す。未実測) */
const NUMBER_LIKE = /^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?%?\s*$/;
const DATE_LIKE = /^\s*\d{1,4}[-/.]\d{1,2}([-/.]\d{1,4})?\s*$/;
function sheetLike(text) {
  if (DATE_LIKE.test(text)) return true;
  if (!NUMBER_LIKE.test(text)) return false;
  return !/^[1-9]\d{0,14}$/.test(text);   // 先頭が 0 でない 15 桁までの整数だけは数字にしても同じ文字に戻る
}

/** NE の原価の元の値 (原価_src = API の値を JSON で包んだ文字) → 仕入単価の文字 */
export function costText(src) {
  let v;
  try { v = JSON.parse(src); } catch { return { text: String(src ?? ''), unverified: 'cost_not_json' }; }
  if (typeof v === 'string' && /^(0|[1-9]\d*)\.00$/.test(v)) return { text: v.slice(0, -3) };
  if (typeof v === 'string' && /^(0|[1-9]\d*)\.\d\d$/.test(v)) return { text: v.replace(/0$/, ''), unverified: 'cost_fraction' };   // 推測 = シートの数字の書き方
  return { text: v == null ? '' : String(v), unverified: 'cost_shape' };
}

/** NE の仕入先コード → 取引先の文字 (GAS は 4 桁に 0 埋め。今の NE は全部 4 桁) */
export function supplierText(s) {
  if (typeof s === 'string' && /^\d{4}$/.test(s)) return { text: s };
  if (typeof s === 'string' && /^\d{1,3}$/.test(s)) return { text: s.padStart(4, '0'), unverified: 'supplier_short' };
  return { text: s == null ? '' : String(s), unverified: 'supplier_shape' };
}

/**
 * 1 行 = 列の文字の並び。推測の印は列の番号つきで返す
 * @param {{ ne_code: string, name: string|null, cost_src: string|null, supplier: string|null }} it
 * @param {'daily'|'new'} kind
 */
export function rowTexts(it, kind) {
  const flags = [];
  const code = String(it.ne_code);
  if (sheetLike(code)) flags.push({ col: 0, why: 'sheet_number_like' });
  const name = it.name == null ? '' : String(it.name);
  const nameWhy = name === '' ? 'empty_name' : sheetLike(name) ? 'sheet_number_like' : null;
  if (nameWhy) flags.push({ col: 1, why: nameWhy }, { col: 2, why: nameWhy });   // 商品名とふりがな (検索名称) は同じ値
  // cost_text = もう仕入単価の文字になっている値 (GAS の入力 logi_hinban.csv の整数。lz-compare.mjs itemsFromLogiHinban)。無ければ NE の原価の元の値から
  const cost = it.cost_text != null ? { text: String(it.cost_text), unverified: it.cost_unverified || null } : costText(it.cost_src), sup = supplierText(it.supplier);
  if (cost.unverified) flags.push({ col: 3, why: cost.unverified });
  if (sup.unverified) flags.push({ col: kind === 'daily' ? 4 : 6, why: sup.unverified });
  const texts = kind === 'daily' ? [code, name, name, cost.text, sup.text] : [code, name, name, cost.text, '', '', sup.text, ''];
  return { texts, flags };
}

/**
 * CSV を作る。元のコードが無い (ne_code が無い) 項目は作らず unmade に理由つきで数える。並び = 元のコードの順 (中原さん L-3)
 * @param {Array<{ code_norm: string, ne_code: string|null, code_reason?: string|null, name, cost_src, supplier }>} items
 * @param {'daily'|'new'} kind
 * @returns {{ bytes: Buffer, rows: Array<{ key: string, code_norm: string, cells: Buffer[], quoted: boolean[], subs: object[], unverified: object[] }>,
 *   unmade: Array<{ code_norm, reason }>, file_unverified: string[], counts: object }}
 */
export function buildLzCsv(items, kind) {
  const spec = kind === 'daily' ? DAILY : kind === 'new' ? NEW : null;
  if (!spec) throw new Error(`知らない種類: ${kind}`);
  const unmade = [];
  const made = [];
  for (const it of items) {
    if (!it.ne_code) { unmade.push({ code_norm: it.code_norm, reason: it.code_reason || 'no_ne_code' }); continue; }
    made.push(it);
  }
  made.sort((a, b) => (a.ne_code < b.ne_code ? -1 : a.ne_code > b.ne_code ? 1 : 0));
  const lines = [Buffer.concat(joinCells(spec.header.map((h) => cellBytes(h).bytes)))];
  const rows = [];
  for (const it of made) {
    const { texts, flags } = rowTexts(it, kind);
    const cells = texts.map((t) => cellBytes(t));
    const subs = [], unverified = [];
    cells.forEach((c, col) => {
      for (const s of c.subs) subs.push({ col, ...s });
      for (const u of c.unverified) unverified.push({ col, ...u });
    });
    for (const f of flags) unverified.push(f);
    rows.push({ key: it.ne_code, code_norm: it.code_norm, cells: cells.map((c) => unquote(c)), quoted: cells.map((c) => c.quoted), subs, unverified });
    lines.push(Buffer.concat(joinCells(cells.map((c) => c.bytes))));
  }
  const crlf = Buffer.from([0x0d, 0x0a]);
  const parts = [];
  lines.forEach((l, i) => { if (i) parts.push(crlf); parts.push(l); });
  return {
    bytes: Buffer.concat(parts), rows, unmade,
    file_unverified: rows.length ? [] : ['zero_rows'],   // 0 件のときの GAS の形は未実測 (見出しだけと推測)
    counts: { target: items.length, made: rows.length, unmade: unmade.length, rows_with_subs: rows.filter((r) => r.subs.length).length, rows_unverified: rows.filter((r) => r.unverified.length).length },
  };
}
function joinCells(bufs) { const out = []; bufs.forEach((b, i) => { if (i) out.push(Buffer.from([0x2c])); out.push(b); }); return out; }
/** 比べるのは引用符を外した中身 (引用符の付き方は別に比べる)。"" は " に戻す (lz-compare の parseCsvBytes と同じ復号) */
export function unquote(c) {
  if (!c.quoted) return c.bytes;
  const inner = c.bytes.subarray(1, -1); const out = [];
  for (let i = 0; i < inner.length; i++) { out.push(inner[i]); if (inner[i] === 0x22) i++; }
  return Buffer.from(out);
}
