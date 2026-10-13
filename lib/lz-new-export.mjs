/**
 * lz-new-export.mjs — ポータルからロジザード登録の CSV (logizard_bc_upload.csv・8 列) を出す (CompanyDB構想 23 v4 §5'・0081)
 *   今の GAS の「新規商品マスタを CSV 出力」(人が H 列 = バーコードなどを手で入れる) を置き換える。GAS は CSV が使えないときの予備として残す。
 *
 * 出す商品 (候補) = ポータルの新商品の登録で作った単品 (登録の状態 origin = new_entry・やめていない) で、
 *   - ロジザードにまだ無い: 毎晩のロジザードの商品の一覧の写し (Render の SQLite f_inbound_check_product_flags) とバーコードマスタの写し
 *     (f_inbound_check_barcode_master) のどちらにも無い (商品ID の大文字小文字を同じと見る・大文字小文字だけ違う ID = 止める)
 *   - まだ出していない (ops.lz_new_export_items の issued が無い = 同じ商品を 2 回出さない)
 * 止める (理由つきで見せる): バーコードが入っていない・代表の仕入先なし・ロジザードのバーコードマスタの写しでほかの商品の値・
 *   Company DB のほかの商品と重なる (ops.lzbc_value_clash = ほかの商品の JAN・Amazon の FNSKU・ロジザードのバーコード)・この CSV の中で同じ値・
 *   🚨 保存のときに写しと照らせていない値 (lz_copy_state が checked でない) = 出すときに新しい写し (36 時間の中) で照らし直せるまで出さない
 * 列: 商品ID = 商品コード (打ったとおり)・商品名 = 検索名称 = Company DB の名前・仕入単価 = 今の原価 (無い = 0。毎日の商品マスタが NE の原価で上書きする)・
 *   有効期限区分 (E 列) = 有効期限の管理 (あり 02 / なし 01)・入荷日管理フラグ (F 列) = 入荷日の管理 (あり 1 / なし・入力なし 0 = 中原さんの決定 Q1 a)・
 *   取引先コード = 代表の仕入先 (4 桁)・バーコード = 打った値 (空欄 = ロジザードが番号を付ける)
 * 🆕 #1721 R1 High 1: CSV の中身 (文字) は DB の ops.lz_new_export_issue が確かめ直した品目から作って帳面に残す (アプリの byte 列は渡さない)。
 *   ダウンロードのときに Shift_JIS にする (lzFileBytes = apps/master-decisions/lz-csv.mjs の encodeText = GAS と同じ変換)。
 *   アプリが同じ品目から作る byte 列 (buildLzNewCsvBytes) と同じになる (試験 scripts/test-lz-csv-export.mjs [X8])
 * 🚨 Amazon の出品・対応は見ない (中原さん「Amazon に出品するにはいらない」)
 */
import crypto from 'node:crypto';
import { buildLzCsv, encodeText, NEW as LZ_NEW } from '../apps/master-decisions/lz-csv.mjs';
import { COMPANY_ID, MasterWriteError } from './master-write.mjs';
import { readLogizardBarcodes, lzBarcodeReady, lzClashNote } from './logizard-barcode.mjs';
import { lzCopyCheck, lzMirrorOwners, LZ_COPY_FRESH_HOURS } from './lz-mirror.mjs';

export const LZ_EXPORT_MAX_ROWS = 2000;
/** 1 回の CSV に出す件数 (🆕 #1721 R2: 候補が多いときは先の N 件 = 残りは次の回)。試験だけ小さくできる */
let exportMaxRows = LZ_EXPORT_MAX_ROWS;
export function __setLzExportMaxRows(n) { exportMaxRows = n == null ? LZ_EXPORT_MAX_ROWS : n; }
/** ロジザードの商品の一覧の写しが古い = この時間より前 (毎晩取り込む = 36 時間) */
export const LZ_LIST_FRESH_HOURS = LZ_COPY_FRESH_HOURS;
export const LZ_HOLD_WORDS = Object.freeze({
  no_barcode: 'ロジザードのバーコードが入っていません (商品の画面で値か「空欄」を入れる)',
  case_collision: 'ロジザードに大文字・小文字だけ違う商品ID があります (別の商品として登録されるおそれ)',
  lz_taken: 'このバーコードはロジザードでほかの商品が使っています (ロジザードのバーコードマスタの写し)',
  clash: 'このバーコードは Company DB でほかの商品と重なっています',
  lz_unchecked: 'ロジザードの写しと照らせていません (写しが読めない・空・古い)。新しい写しで照らせると出せます',
  dup_in_file: 'この CSV の中にもう 1 つ同じバーコードがあります',
  no_supplier: '代表の仕入先がありません',
});

/** 写しが古いか (時刻が無い・読めない・36 時間より前・5 分より先 = DB の ops.lz_new_export_issue と同じ) */
export const lzListStale = (mirror, nowMs) => {
  const ms = mirror && mirror.list_as_of ? Date.parse(mirror.list_as_of) : NaN;
  return Number.isNaN(ms) || !(ms > nowMs - LZ_LIST_FRESH_HOURS * 3600e3) || ms > nowMs + 5 * 60e3;
};

/** 0081 (帳面と出す関数) があり、画面のロールが出す関数を実行できるか (0081 の後・ロールの流し直しの前 = まだ使えない) */
export async function lzExportReady(db) {
  if (!(await lzBarcodeReady(db))) return false;
  return (await db.query(`select coalesce((select pg_catalog.has_function_privilege(r.oid, p.fn, 'EXECUTE') from pg_catalog.pg_roles r where r.rolname = 'master_edit'), false) as ok
    from (select to_regprocedure('ops.lz_new_export_issue(uuid, text, jsonb, timestamp with time zone, timestamp with time zone)') as fn) p where p.fn is not null`)).rows[0]?.ok === true;
}

/**
 * 候補 (読むだけ)。db = Company DB (master_edit)・mirror = lib/lz-mirror.mjs readLzMirror の答え・nowMs = 今
 * @returns {{ ready: object[], held: object[], in_lz: number, db_ready: boolean, copy, rest: number, max: number }}
 *   🆕 #1721 R2: ready は先の max 件 (商品コードの順)・rest = 残り (次の回)
 *   1 行 = { sku_id, code, name, cost, supplier, expiry_code, inbound_code, inbound_unknown, kind, value, rule_row, rechecked, notes, holds: [理由], hold_notes: { 理由: 一言 } }
 *   copy = バーコードマスタの写しと照らせるか (lzCopyCheck の答え・照らせていない値の照らし直しに使う)
 */
export async function lzNewCandidates(db, mirror, nowMs = Date.now()) {
  const copy = lzCopyCheck(mirror, nowMs);
  if (!(await lzExportReady(db))) return { ready: [], held: [], in_lz: 0, db_ready: false, copy, rest: 0, max: exportMaxRows };
  const rows = (await db.query(`
    select s.sku_id::text as sku_id, s.code, s.name, p.expiry_managed, p.inbound_date_managed,
           (select c.cost_jpy::text from core.sku_costs c where c.sku_id = s.sku_id and c.valid_to is null order by c.valid_from desc limit 1) as cost,
           (select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id where x.sku_id = s.sku_id and x.is_primary limit 1) as supplier
      from core.skus s join core.products p on p.product_id = s.product_id join ops.master_registrations r on r.sku_id = s.sku_id
     where s.company_id = $1 and s.sku_kind = 'single' and r.origin = 'new_entry' and r.state <> 'cancelled'
       and not exists (select 1 from ops.lz_new_export_items i where i.sku_id = s.sku_id and i.state = 'issued')
     order by s.code_norm`, [COMPANY_ID])).rows;
  let inLz = 0;
  const live = [];
  for (const r of rows) {
    const low = String(r.code).toLowerCase();
    const exact = mirror.ids.get(low);   // バーコードマスタの商品ID (書いたとおり)
    if (exact && exact.has(r.code)) { inLz++; continue; }
    // バーコードマスタに大文字・小文字だけ違う商品ID がある = 別の商品として登録されるおそれ = 止める
    if (exact && exact.size) { live.push({ ...r, case_collision: true }); continue; }
    // 商品の一覧 (小文字にした ID) にある = もうある と見る (大文字小文字は分からない・出さない方へ)
    if (mirror.keys.has(low)) { inLz++; continue; }
    live.push({ ...r, case_collision: false });
  }
  const bc = await readLogizardBarcodes(db, live.map((x) => x.sku_id), { ready: true });
  const out = [];
  for (const r of live) {
    const b = bc.get(r.sku_id);
    const holds = [], holdNotes = {};
    let rechecked = false;
    if (r.case_collision) holds.push('case_collision');
    if (!b || !b.kind) holds.push('no_barcode');
    if (!r.supplier) holds.push('no_supplier');
    if (b && b.value) {
      const owner = lzMirrorOwners(mirror, r.code, b.value);
      if (owner) { holds.push('lz_taken'); holdNotes.lz_taken = `${LZ_HOLD_WORDS.lz_taken}: ${owner}`; }
      // Company DB の重なり (ほかの商品の JAN・Amazon の FNSKU・ロジザードのバーコード) = 出さない (readLogizardBarcodes の clash と同じ関数)
      if (b.clash) { holds.push('clash'); holdNotes.clash = lzClashNote(b.clash.kind, b.clash.code, b.value); }
      // 🆕 #1721 R1 High 2: 値は、保存のときの結果にかかわらず、今の写し (36 時間の中) で照らせたときだけ出す (照らした写しの時刻を DB が確かめる)。
      //   rechecked = 保存のときは照らせていなかった値を、今の写しで照らし直した
      if (copy.state !== 'checked') { holds.push('lz_unchecked'); holdNotes.lz_unchecked = `${LZ_HOLD_WORDS.lz_unchecked} (今の写し: ${copy.words}${copy.as_of ? `・${copy.as_of}` : ''})`; }
      else if (b.lz_copy_state !== 'checked') rechecked = true;
    }
    out.push({
      sku_id: r.sku_id, code: r.code, name: r.name, cost: r.cost == null ? null : r.cost, supplier: r.supplier || null,
      expiry_code: r.expiry_managed ? '02' : '01', inbound_code: r.inbound_date_managed ? '1' : '0', inbound_unknown: r.inbound_date_managed == null,
      kind: b ? b.kind : null, value: b ? b.value : null, rule_row: b ? b.rule_row : null, notes: b ? b.notes : [], rechecked, holds, hold_notes: holdNotes,
    });
  }
  // この CSV の中で同じ値 = 両方止める
  const byVal = new Map();
  for (const x of out) if (x.value && !x.holds.length) byVal.set(x.value, (byVal.get(x.value) || 0) + 1);
  for (const x of out) if (x.value && byVal.get(x.value) > 1) x.holds.push('dup_in_file');
  const readyAll = out.filter((x) => !x.holds.length);
  const ready = readyAll.slice(0, exportMaxRows);
  return { ready, held: out.filter((x) => x.holds.length), in_lz: inLz, db_ready: true, copy, rest: readyAll.length - ready.length, max: exportMaxRows };
}

/** 出す行 → CSV の byte 列 (lz-csv.mjs の新商品の形)。行の並び = 商品コードの順 (lz-csv が並べる) */
export function buildLzNewCsvBytes(items) {
  const r = buildLzCsv(items.map((x) => ({
    code_norm: String(x.code).toLowerCase(), ne_code: x.code, name: x.name, cost_text: x.cost == null ? '0' : String(x.cost), supplier: x.supplier,
    expiry_code: x.expiry_code, inbound_code: x.inbound_code, barcode: x.value == null ? '' : x.value,
  })), 'new');
  if (r.unmade.length) throw new Error(`作れない行がある: ${r.unmade.map((u) => `${u.code_norm} (${u.reason})`).join('・')}`);
  return r;
}

/** DB の中身 (文字) → ダウンロードの byte 列 (Shift_JIS・GAS と同じ変換)。引用符は DB が付けている = 全部を 1 つの文字として変換 (lz-csv.mjs cellBytes と同じ byte 列) */
export function lzFileBytes(text) { return encodeText(String(text)).bytes; }
/** 要求の品目のハッシュ = sku_id:rule_row を sku_id の順に「,」でつなぐ (DB の ops.lz_new_export_issue と同じ・押し直しの比べ) */
export function lzRequestHash(want) {
  const xs = (want || []).map((w) => ({ s: String(w && w.sku_id), r: String(w && w.rule_row) }))
    .sort((a, b) => (BigInt(a.s) < BigInt(b.s) ? -1 : BigInt(a.s) > BigInt(b.s) ? 1 : BigInt(a.r) < BigInt(b.r) ? -1 : BigInt(a.r) > BigInt(b.r) ? 1 : 0));
  return crypto.createHash('sha256').update(xs.map((x) => `${x.s}:${x.r}`).join(','), 'utf8').digest('hex');
}
const conflict = (reason, message, extra = {}) => new MasterWriteError(409, reason, message, extra);
/**
 * CSV を出す = 帳面に書く。want = 画面で見た行 [{ sku_id, rule_row }] (画面を開いた後に変わった・止まった = 409 changed・何も書かない)。
 * 写しが無い / 読めない = 409 lz_list_unavailable・商品の一覧の写しが古い (36 時間超・5 分より先) = 409 lz_list_stale (🆕 #1721 R2: 「古いまま作る」は無い = DB も断る)
 * 戻り値 { ok, export_id, file_sha256, rows, replayed? }
 */
export async function issueLzNewExport(db, { requestId, actor, want, mirror, nowMs = Date.now() }) {
  // 押し直し (同じ request_id) = 前の答え (候補を見直す前に = もう出した商品は候補に無い)。🆕 #1721 R1 Medium: 人か要求の品目 (ハッシュ) が違う = 409
  const okWant = Array.isArray(want) && want.length && want.every((w) => w && /^[1-9]\d{0,17}$/.test(String(w.sku_id)) && /^[1-9]\d{0,17}$/.test(String(w.rule_row)));
  if (okWant && (await lzExportReady(db))) {
    const prev = (await db.query('select export_id::text as id, actor_id, text_sha256, request_hash, row_count from ops.lz_new_exports where request_id = $1::uuid', [requestId])).rows[0];
    if (prev) {
      if (prev.actor_id !== actor || prev.request_hash !== lzRequestHash(want)) throw conflict('request_id_reused', '同じ作る番号 (request_id) がほかの人・ほかの商品で使われています。何も作っていません。画面を開き直してください');
      return { ok: true, export_id: prev.id, text_sha256: prev.text_sha256, rows: prev.row_count, replayed: true };
    }
  }
  if (!mirror.ok) throw conflict('lz_list_unavailable', `ロジザードの商品の一覧の写しを読めないので、まだ無い商品か確かめられません (${mirror.reason})。何も作っていません`);
  if (lzListStale(mirror, nowMs)) {
    throw conflict('lz_list_stale', `ロジザードの商品の一覧の写しが古い (${mirror.list_as_of || '時刻なし'}) ので、まだ無い商品か確かめられません。毎晩の取り込みの後に作るか、急ぐときは予備の GAS で作ってください。何も作っていません`);
  }
  if (!okWant || want.length > exportMaxRows) throw new MasterWriteError(400, 'invalid_input', `出す商品は 1〜${exportMaxRows} 件です (画面を開き直してください)`);
  const c = await lzNewCandidates(db, mirror, nowMs);
  if (!c.db_ready) throw new MasterWriteError(503, 'db_not_ready', 'Company DB に 0081 (ロジザード登録の CSV) がまだ入っていないか、権限の流し直しの前です。何も作っていません');
  const byId = new Map(c.ready.map((x) => [x.sku_id, x]));
  const items = [];
  for (const w of want) {
    const x = byId.get(String(w && w.sku_id));
    if (!x || String(x.rule_row) !== String(w.rule_row)) {
      throw conflict('changed', '画面を開いた後に、出す商品かバーコードが変わりました (ほかの人が出した・直した・止まった)。何も作っていません。画面を開き直してください');
    }
    items.push(x);
  }
  buildLzNewCsvBytes(items);   // 作れない行 (形の誤り) は DB に行く前に止める
  await db.query('begin');
  try {
    const r = (await db.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, $4::timestamptz, $5::timestamptz) as r', [requestId, actor,
      JSON.stringify(items.map((x) => ({ sku_id: x.sku_id, code: x.code, rule_row: String(x.rule_row), kind: x.kind, value: x.value, expiry_code: x.expiry_code, inbound_code: x.inbound_code }))),
      mirror.list_as_of, c.copy.state === 'checked' ? c.copy.as_of : null])).rows[0].r;
    await db.query('commit');
    return r;
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || ''));
    const key = m ? m[1] : '';
    if (['stale', 'lz_issued', 'lzbc_taken', 'lz_unchecked', 'lz_list_stale', 'request_id_reused'].includes(key)) throw conflict(key === 'stale' ? 'changed' : key, `${m[2]}。何も作っていません。画面を開き直してください`);
    if (key === 'invalid_input') throw new MasterWriteError(400, 'invalid_input', `${m[2]}。何も作っていません`);
    if (e && e.code === '23505') throw conflict('lz_issued', 'ちょうどほかの人が同じ商品を出しました。何も作っていません。画面を開き直してください');
    throw e;
  }
}

/** 出した CSV の一覧 (新しい順・50 件) */
export async function lzNewExports(db, { limit = 50 } = {}) {
  if (!(await lzExportReady(db))) return [];
  return (await db.query(`select e.export_id::text as id, e.created_at::text as created_at, e.actor_id, e.row_count as rows, e.text_sha256 as sha, e.state,
      e.voided_at::text as voided_at, e.voided_by, e.void_reason, e.lz_list_as_of::text as lz_list_as_of,
      (select string_agg(i.code, '・' order by i.code) from (select code from ops.lz_new_export_items x where x.export_id = e.export_id order by code limit 6) i) as codes
     from ops.lz_new_exports e order by e.export_id desc limit $1`, [limit])).rows;
}
/** ファイル 1 つ = DB が作った中身 (文字) を Shift_JIS の byte 列に (file_bytes・file_sha256 = byte 列のハッシュ) */
export async function lzNewExportFile(db, id) {
  if (!/^[1-9]\d{0,17}$/.test(String(id))) return null;
  if (!(await lzExportReady(db))) return null;
  const f = (await db.query('select export_id::text as id, file_name, file_text, text_sha256, state from ops.lz_new_exports where export_id = $1', [String(id)])).rows[0];
  if (!f) return null;
  const bytes = lzFileBytes(f.file_text);
  return { id: f.id, file_name: f.file_name, state: f.state, text_sha256: f.text_sha256, file_bytes: bytes, file_sha256: crypto.createHash('sha256').update(bytes).digest('hex') };
}
/** 取り消す (理由が要る) */
export async function voidLzNewExport(db, { actor, exportId, reason }) {
  if (!/^[1-9]\d{0,17}$/.test(String(exportId))) throw new MasterWriteError(404, 'not_found', 'ファイルがありません');
  try {
    return (await db.query('select ops.lz_new_export_void($1, $2::bigint, $3) as r', [actor, String(exportId), reason == null ? null : String(reason)])).rows[0].r;
  } catch (e) {
    const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || ''));
    if (m && m[1] === 'invalid_input') throw new MasterWriteError(400, 'invalid_input', `${m[2]}`, { field: 'reason' });
    if (m && m[1] === 'not_found') throw new MasterWriteError(404, 'not_found', m[2]);
    throw e;
  }
}
export { LZ_NEW };
