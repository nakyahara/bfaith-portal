/**
 * master-ne-delete.mjs — NE にもう登録した商品を「NE から消す」(0072・中原さんの頼み 10/11 の場面 c・決定 10/12 Q1 a / Q2 a / Q3 a / Q4 b)
 *
 * 流れ: ① 消す CSV に入れてよいかを DB が確かめる (ops.ne_del_check・在庫 0 を含む) → ② 消す CSV を作る (ops.ne_del_build・セット → 単品の 2 つまで・記録) →
 *   ③ 人がダウンロードして NE に取り込む ([設定]→[マスタ削除]→[セット商品マスタ] を先に → [商品マスタ]) → ④ 毎朝の照合が「完全な NE の取得に 3 日続けて無い」を数える (0070) →
 *   ⑤ 商品の画面の「NE から消えた (やめる)」(lib/master-gone.mjs・セットも) で Company DB でもやめる。
 *   NE が断った (ダウンロードから 3 日たっても NE にある・単品) = ⑥ 代わりに「取扱中止」の CSV (ops.ne_handling_build・ne-csv.mjs の products:handling と同じ形)
 * NE の削除の CSV の形 (公開のマニュアル ID:8641 のひな形 syohindel.csv): 見出し「コード」の 1 列・Shift_JIS・CRLF (バイト列 83 52 81 5B 83 68 0D 0A)・
 *   単品とセットは別のファイル・削除のフラグの列は無い = 書いたコードが全部消える。コードは NE の元の書き方 (英数字と _- だけ = Shift_JIS でも ASCII と同じ)
 * 🚨 バイト列は DB が作って記録する (ops.ne_del_csv_bytes・sha256 も)。ここの delCsvBytes / handlingCsvBytes は試験と受け取りの照らし合わせ用 (同じバイト列になる)
 * 🚨 在庫 (決定 Q3 a): DB は自分の在庫の日次 (NE・ロジザード) を読む。アプリが読む値 (NE の在庫・引当・発注残 = 商品管理リストの公開の回・FBA の在庫・
 *   ロジザードの今の在庫と引当 = 毎時の写し (別々に)・発注アプリの注文残) は apps/master-edit/ne-delete-stock.mjs が Postgres の取引の前に読んで渡す (DB は確かめられない = 記録に残す)。
 *   どれかを読めない = 断る ({ unreadable })
 * ここ (lib) は形と門 (段階・DB の active・MASTER_EDIT_OPEN) を確かめて関数を呼ぶだけ (DB の関数が鍵・確かめ・約束をやり直す = 呼び手を信じない)
 * 使う所: apps/master-edit (「NE から消す」の画面・商品の画面の箱・名簿 MASTER_DECISION_APPROVERS の人)
 */
import crypto from 'node:crypto';
import { MasterWriteError, ownerGateError, lockMasterWriteShared, COMPANY_ID } from './master-write.mjs';
import { baseGateInTx, lockCutoverSharedInTx } from './master-owner-gate.mjs';
import { buildCsv, specOf as neCsvSpecOf } from '../apps/master-decisions/ne-csv.mjs';

export const NE_DEL_CHECK_FN = 'ops.ne_del_check(bigint, bigint[], jsonb)';
export const NE_DEL_MAX = 500;
export const NE_DEL_REASON_MAX = 200;
/** NE の削除の CSV の見出し「コード」の Shift_JIS (マニュアル ID:8641 のひな形と同じバイト) */
export const SJIS_HEADER_CODE = Object.freeze([0x83, 0x52, 0x81, 0x5b, 0x83, 0x68]);
/** アプリが読む在庫の値の鍵 (DB の ops.ne_del_app_stock_keys() と同じ・試験で照らす) */
export const APP_STOCK_KEYS = Object.freeze(['fba', 'logizard', 'logizard_allocated', 'ne_allocated', 'ne_on_order', 'ne_own', 'po_backorder']);
/** DB の確かめの理由 (problems[].reason) の短い言葉 (画面の見出し。文は DB の text) */
export const NE_DEL_REASON_WORDS = Object.freeze({
  no_sku: '商品が無い', not_company: '会社が違う', not_available: '業務で使える商品でない', kind_exception: '例外の SKU', live_request: 'もう依頼の途中',
  ne_codes_old: 'NE のコードの台帳が古い', not_in_ne: 'NE に無い', gone_from_ne: 'もう NE から消えた', ne_code_bad: 'NE のコードの書き方が決まらない',
  live_file: 'NE 登録の CSV が途中', set_component: 'セットの中身', component_request: '構成の依頼に入っている', own_component_request: 'このセットの構成の依頼が開いている',
  listing_component: '出品の構成に使われている', listing_component_deleted: '削除済みの出品の構成に入っている', parent_of_others: 'ほかの商品の親',
  stock_ne_unknown: 'NE の在庫を確かめられない', stock_ne: 'NE に在庫がある', stock_logizard_unknown: 'ロジザードの在庫を確かめられない', stock_logizard: 'ロジザードに在庫がある',
  stock_app_unknown: '在庫を確かめられない', stock_app: '在庫・引当・注文残がある', not_found: 'Company DB に無い',
  // 断られたとき (ops.ne_del_refused_check)
  no_item: '品目が無い', not_live: '依頼の途中でない', not_single: 'セット', not_downloaded: 'まだダウンロードしていない', too_soon: 'まだ 3 日たっていない',
  no_trusted_fetch: 'NE の取得の記録が無い', fetch_old: 'NE の取得の記録が古い', fetch_before_download: 'ダウンロードの後の取得がまだ',
});
export const NE_DEL_WARNING_WORDS = Object.freeze({ set_first: 'セットのファイルを先に', page_info: 'ページ情報も消える' });
/** 品目の今の様子 (ops.ne_del_item_status の status) の言葉 */
export const NE_DEL_STATUS_WORDS = Object.freeze({
  not_downloaded: 'まだダウンロードしていない', waiting: 'NE に取り込むのを待っています (取り込んだら、毎朝の照合が数えます)',
  gone_counting: 'NE に無い (日数を数えています)', gone_ready: 'NE から消えました → やめる', refused: 'NE が消すのを断ったかも (3 日たっても NE にある)',
  withdrawn: '取り下げた', handled: '代わりに取扱中止の CSV にした', gone: 'やめた (NE から消えた)', recorded: '取扱中止の CSV',
});
export const NE_DEL_KIND_WORDS = Object.freeze({ sets: 'セット商品マスタ', products: '商品マスタ', handling: '取扱中止 (商品の一括登録)' });

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const NE_CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
const bad = (message, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, extra);
const jsonOf = (v) => (typeof v === 'string' ? JSON.parse(v) : v);

/** DB の関数の誤り (「理由: 文」の形) → MasterWriteError の状態 */
const FN_STATUS = Object.freeze({ invalid_input: 400, not_found: 404, not_deletable: 409, not_refused: 409, not_live: 409, file_old: 409, file_changed: 409,
  request_id_reused: 409, before_cutover: 409, master_write_session_required: 403, master_write_session_mismatch: 403, master_write_session_exists: 409 });
function fnError(e) {
  if (e instanceof MasterWriteError) return e;
  const m = /^([a-z_]+): ([\s\S]*)$/.exec(String((e && e.message) || ''));
  if (m && e && ['P0001', 'P0002', '22023', '23505', '42501', '55000'].includes(e.code)) {
    const words = m[1] === 'before_cutover' ? `切替前です (DB が断った: ${m[2]})。何もしていません` : ['not_deletable', 'not_refused', 'not_live'].includes(m[1]) ? `${m[2]}。何もしていません` : m[2];
    return new MasterWriteError(FN_STATUS[m[1]] ?? 409, m[1], words);
  }
  if (e && e.code === '23505' && /master_edit_requests_pkey/.test(String(e.constraint || e.message || ''))) {
    return new MasterWriteError(409, 'request_id_reused', '同じ操作の番号 (request_id) がちょうど使われました。画面を開き直してください');
  }
  if (e && e.code === '23505' && /ux_ne_del_items_live/.test(String(e.constraint || e.message || ''))) {
    return new MasterWriteError(409, 'not_deletable', 'ほかの依頼がちょうど同じ商品を NE から消す依頼にしました。画面を開き直してください。何もしていません');
  }
  return e;
}

/** 0072 の関数があるか (無い = 0072 の前の DB = 画面は「まだ使えない」) */
export async function neDelAvailable(db) {
  return (await db.query('select to_regprocedure($1) is not null as ok', [NE_DEL_CHECK_FN])).rows[0]?.ok === true;
}

// ─── CSV のバイト列 (DB の ops.ne_del_csv_bytes / ops.ne_handling_csv_bytes と同じ・試験と受け取りの照らし合わせ) ───
function codesOk(codes) {
  if (!Array.isArray(codes) || !codes.length) throw new Error('コードが無い');
  for (const c of codes) if (typeof c !== 'string' || !NE_CODE_RE.test(c)) throw new Error(`NE のコードの形が違う: ${String(c).slice(0, 40)}`);
}
/** 消す CSV = 見出し「コード」(Shift_JIS)・CRLF・コード 1 行ずつ (英数字と _- = ASCII)・最後の行も CRLF */
export function delCsvBytes(codes) {
  codesOk(codes);
  return Buffer.concat([Buffer.from(SJIS_HEADER_CODE), Buffer.from(`\r\n${codes.map((c) => `${c}\r\n`).join('')}`, 'ascii')]);
}
/** 取扱中止の CSV = apps/master-decisions/ne-csv.mjs の buildCsv(products:handling) そのもの (見出し syohin_code,toriatukai_kbn・値 1・UTF-8・CRLF) */
export function handlingCsvBytes(codes) {
  codesOk(codes);
  return buildCsv(neCsvSpecOf('products', 'handling'), codes.map((c) => ({ ne_code: c, cell: '1' }))).bytes;
}
export const sha256Hex = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

// ─── 入力の形 ───
/** 理由の形 (前後の空白を除いて 1〜200 字・改行と制御文字なし) */
export function neDelReasonOf(v, example = 'NE で使わなくなった (別のコード xxx で作り直した)') {
  const s = String(v ?? '').trim();
  if (!s) throw bad(`理由を書いてください (例: ${example})`, 'invalid_input', { field: 'reason' });
  if (s.length > NE_DEL_REASON_MAX || CTRL_RE.test(s)) throw bad(`理由は ${NE_DEL_REASON_MAX} 字まで (改行なし)`, 'invalid_input', { field: 'reason' });
  return s;
}
function actorOf(v) {
  const a = String(v ?? '').trim().toLowerCase();
  if (!a || a.length > 320 || CTRL_RE.test(a)) throw bad('操作する人 (ログインのメール) が分からない');
  return a;
}
/** この操作 1 回の番号 (保存の記録の request_id)。送られない・形が違う = 新しく作る */
export const neDelRequestId = (v) => { const s = String(v ?? '').trim().toLowerCase(); return UUID_RE.test(s) ? s : crypto.randomUUID(); };
/** 商品コードの並び (1〜500・前後の空白を除く・同じコードは 1 つに) */
export function neDelCodesOf(v) {
  const list = Array.isArray(v) ? v : typeof v === 'string' ? v.split(/[\s,、]+/) : null;
  if (!list) throw bad('商品コードを入れてください', 'invalid_input', { field: 'codes' });
  const out = [];
  const seen = new Set();
  for (const x of list) {
    const c = String(x ?? '').trim();
    if (!c) continue;
    if (c.length > 60 || CTRL_RE.test(c)) throw bad(`商品コードの形が違う: ${c.slice(0, 40)}`, 'invalid_input', { field: 'codes' });
    const k = c.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(c);
  }
  if (!out.length) throw bad('商品コードを入れてください', 'invalid_input', { field: 'codes' });
  if (out.length > NE_DEL_MAX) throw bad(`1 回に ${NE_DEL_MAX} 件まで`, 'invalid_input', { field: 'codes' });
  return out;
}

/** コード → Company DB の SKU (画面のロールは core.skus を読める)。無いコードは missing */
export async function resolveCodes(db, codes) {
  const rows = (await db.query(`select c.v as code_in, k.sku_id::text as sku_id, k.code, k.code_norm, k.sku_kind
      from pg_catalog.unnest($1::text[]) with ordinality as c(v, o) left join core.skus k on k.company_id = $2 and k.code_norm = core.norm_code(c.v) order by c.o`, [codes, COMPANY_ID])).rows;
  const found = []; const missing = []; const seen = new Set();
  for (const r of rows) {
    if (!r.sku_id) { missing.push(r.code_in); continue; }
    if (seen.has(r.sku_id)) continue;
    seen.add(r.sku_id);
    found.push({ sku_id: String(r.sku_id), code: r.code, code_norm: r.code_norm, sku_kind: r.sku_kind });
  }
  return { found, missing };
}

/**
 * アプリが読んだ在庫 ({ "<sku_id>": 値 }) を作る。単品だけ (セットの在庫は NE に無い)。stockReader(skus) = Map(code_norm → 値 | { unreadable }) /
 * 無い = 全部「読めない」(DB が断る)。読み手が落ちた = 全部「読めない」(何も書かない・確かめも ok にしない)
 */
async function appStockOf(skus, stockReader) {
  const singles = skus.filter((s) => s.sku_kind === 'single');
  const out = {};
  if (!singles.length) return out;
  let map = null;
  let why = '在庫の読み手が無い';
  if (typeof stockReader === 'function') {
    try { map = await stockReader(singles.map((s) => ({ code: s.code, code_norm: s.code_norm }))); } catch (e) { why = `在庫を読めない (${String((e && e.message) || e).slice(0, 120)})`; map = null; }
  }
  for (const s of singles) out[s.sku_id] = (map && map.get(s.code_norm)) || { unreadable: map ? 'この商品の在庫を読めない' : why };
  return out;
}

/**
 * 確かめ (読むだけ)。input = { codes }。opts = { stockReader }。
 * 戻り値 = { all_ok (全部 ok・無いコードも無い), rows: [ops.ne_del_check の答え + word], missing: [Company DB に無いコード], app_stock } / 0072 の前 = { unavailable: true }
 */
export async function checkNeDelete(db, input, { stockReader = null } = {}) {
  const codes = neDelCodesOf(input?.codes);
  if (!(await neDelAvailable(db))) return { all_ok: false, unavailable: true, rows: [], missing: [] };
  const { found, missing } = await resolveCodes(db, codes);
  if (!found.length) return { all_ok: false, rows: [], missing };
  const app = await appStockOf(found, stockReader);
  let rows;
  try {
    rows = jsonOf((await db.query('select ops.ne_del_check_many($1::bigint[], $2::jsonb) as r', [found.map((s) => s.sku_id), JSON.stringify(app)])).rows[0].r);
  } catch (e) { throw fnError(e); }
  rows = rows.map(viewRow);
  return { all_ok: !missing.length && rows.every((r) => r.ok === true), rows, missing, app_stock: app };
}
/** 確かめの答えに見出しの言葉を付ける */
function viewRow(r) {
  return { ...r, problems: (r.problems || []).map((p) => ({ ...p, word: NE_DEL_REASON_WORDS[p.reason] || p.reason })),
    warnings: (r.warnings || []).map((w) => ({ ...w, word: NE_DEL_WARNING_WORDS[w.reason] || w.reason })) };
}

/** 書く操作の門 (今の保存と同じ): 段階 (共有の鍵) → 段階 new_open・DB の active・MASTER_EDIT_OPEN → 夜間の取り込み (マスタの書き込みの共有の鍵) */
async function gateInTx(db, open) {
  await lockCutoverSharedInTx(db);
  const gate = await baseGateInTx(db, { open: open === true, closedWhy: 'この画面の保存はまだ開いていません' });
  if (!gate.ok) throw ownerGateError(gate, (why) => new MasterWriteError(409, 'before_cutover', `切替前です (${why})。何もしていません`));
  await lockMasterWriteShared(db);
  if (!(await neDelAvailable(db))) throw new MasterWriteError(409, 'not_applied', 'Company DB に 0072 (NE から消す CSV) がまだ無い。何もしていません');
}
async function inTx(db, fn) {
  await db.query('begin');
  try {
    const r = await fn();
    await db.query('commit');
    return r;
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が切れた = 元の誤りを上げる */ }
    throw fnError(e);
  }
}

/**
 * 消す CSV を作る (1 つの取引)。input = { actor, codes, reason, requestId, confirm (画面 = 確かめの窓で押した = true) }。opts = { open, stockReader, requireConfirm }
 * 在庫 (アプリの値) は取引の前に読む (SQLite の写し = Postgres の取引に入れない)。戻り値 = DB の関数の答え { ok, request_id, exports, warnings, built_day, replayed? }
 */
export async function buildNeDelete(db, input, { open = false, stockReader = null, requireConfirm = true } = {}) {
  const actor = actorOf(input?.actor);
  const reason = neDelReasonOf(input?.reason);
  if (requireConfirm && input?.confirm !== true) throw bad('確かめの窓で「CSV を作る」を押してください', 'invalid_input', { field: 'confirm' });
  const codes = neDelCodesOf(input?.codes);
  const requestId = neDelRequestId(input?.requestId);
  if (!(await neDelAvailable(db))) throw new MasterWriteError(409, 'not_applied', 'Company DB に 0072 (NE から消す CSV) がまだ無い。何もしていません');
  const { found, missing } = await resolveCodes(db, codes);
  if (missing.length) throw new MasterWriteError(404, 'not_found', `Company DB に無い商品コード: ${missing.slice(0, 10).join('・')}。何もしていません`, { missing });
  const app = await appStockOf(found, stockReader);
  return inTx(db, async () => {
    await gateInTx(db, open);
    return jsonOf((await db.query('select ops.ne_del_build($1::uuid, $2, $3, $4::bigint[], $5::jsonb) as r', [requestId, actor, reason, found.map((s) => s.sku_id), JSON.stringify(app)])).rows[0].r);
  });
}

/**
 * ファイルを受け取る (作った JST の日だけ・中の消す品目が全部まだ依頼の途中のときだけ・初めてのダウンロードを DB が残す)。
 * 戻り値 = { export_id, kind, file_name, sha256, bytes (Buffer), row_count }。受け取ったバイト列の sha256 が記録と違う = 渡さない
 */
export async function downloadNeDelete(db, { actor, exportId }) {
  const a = actorOf(actor);
  if (!/^\d{1,19}$/.test(String(exportId ?? ''))) throw bad('ファイルの番号の形が違う');
  if (!(await neDelAvailable(db))) throw new MasterWriteError(409, 'not_applied', 'Company DB に 0072 (NE から消す CSV) がまだ無い');
  const r = await inTx(db, async () => jsonOf((await db.query('select ops.ne_del_download($1, $2::bigint) as r', [a, String(exportId)])).rows[0].r));
  const bytes = Buffer.from(String(r.bytes_b64 || ''), 'base64');
  if (sha256Hex(bytes) !== r.sha256) throw new MasterWriteError(500, 'sha_mismatch', 'ファイルのバイト列が記録と違う (渡さない)');
  return { export_id: r.export_id, kind: r.kind, file_name: r.file_name, sha256: r.sha256, row_count: r.row_count, bytes };
}

/** 依頼を取り下げる (1 件)。input = { actor, itemId, reason, requestId }。戻り値 = DB の関数の答え */
export async function withdrawNeDelete(db, input, { open = false } = {}) {
  const actor = actorOf(input?.actor);
  const reason = neDelReasonOf(input?.reason, 'NE で消すのをやめた (まだ使う)');
  if (!/^\d{1,19}$/.test(String(input?.itemId ?? ''))) throw bad('品目の番号の形が違う');
  const requestId = neDelRequestId(input?.requestId);
  return inTx(db, async () => {
    await gateInTx(db, open);
    return jsonOf((await db.query('select ops.ne_del_withdraw($1::uuid, $2, $3, $4::bigint) as r', [requestId, actor, reason, String(input.itemId)])).rows[0].r);
  });
}

/** 断られた品目の代わりに取扱中止の CSV を作る。input = { actor, itemIds, reason, requestId, confirm }。戻り値 = DB の関数の答え */
export async function buildHandling(db, input, { open = false, requireConfirm = true } = {}) {
  const actor = actorOf(input?.actor);
  const reason = neDelReasonOf(input?.reason, 'NE が消すのを断った (受注が残っている) = 取扱中止にする');
  if (requireConfirm && input?.confirm !== true) throw bad('確かめの窓で「取扱中止の CSV を作る」を押してください', 'invalid_input', { field: 'confirm' });
  const ids = Array.isArray(input?.itemIds) ? input.itemIds.map((x) => String(x ?? '').trim()) : [];
  if (!ids.length || ids.length > NE_DEL_MAX || ids.some((x) => !/^\d{1,19}$/.test(x))) throw bad(`品目を 1〜${NE_DEL_MAX} 件選んでください`, 'invalid_input', { field: 'item_ids' });
  const requestId = neDelRequestId(input?.requestId);
  return inTx(db, async () => {
    await gateInTx(db, open);
    return jsonOf((await db.query('select ops.ne_handling_build($1::uuid, $2, $3, $4::bigint[]) as r', [requestId, actor, reason, [...new Set(ids)]])).rows[0].r);
  });
}

/**
 * 一覧 (読むだけ)。skuId = その商品だけ / null = 全部。0072 の前・実行権が無い・読めない = null (画面は「まだ使えない」/ 何も出さない)。
 * 戻り値 = ops.ne_del_overview の答え + items[].status.word
 */
export async function readNeDelOverview(db, skuId = null) {
  if (!db) return null;
  if (skuId != null && !/^\d{1,19}$/.test(String(skuId))) return null;
  try {
    if (!(await neDelAvailable(db))) return null;
    const r = jsonOf((await db.query('select ops.ne_del_overview($1::bigint) as r', [skuId == null ? null : String(skuId)])).rows[0].r);
    for (const it of r.items || []) {
      const st = it.status || {};
      st.word = NE_DEL_STATUS_WORDS[st.status] || st.status || '';
      st.gone_problems = (st.gone_problems || []).map((p) => ({ ...p, word: NE_DEL_REASON_WORDS[p.reason] || p.reason }));
      st.refused_problems = (st.refused_problems || []).map((p) => ({ ...p, word: NE_DEL_REASON_WORDS[p.reason] || p.reason }));
      it.status = st;
    }
    return r;
  } catch (e) {
    console.error(`[master-ne-delete] 一覧を読めない: ${String((e && e.message) || e).slice(0, 200)}`);
    return null;
  }
}
