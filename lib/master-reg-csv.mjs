/**
 * master-reg-csv.mjs — 新商品を NE に登録する CSV (Company DB構想 14 §10 契約 v3 H5 / §4 / 10 §6.2。migration 0052)
 *
 * 既にある商品の値を直す CSV (apps/master-decisions/ne-csv.mjs = fix_ne / to_ne) とは別の物:
 *   ファイル (export) → 商品ごと (item) → CSV の行 → 取り込みの申告 (attempt) → 翌朝の照合の確かめ (check)
 *   商品ごとの状態: built (作った) → issued (配った = ダウンロード) → import_declared (取り込んだと申告) → verified (NE で全部の列が合った) /
 *     partial (NE にあるが違う列がある) / failed (取り込めなかった) / superseded (人が「使わない」にした)
 * 約束:
 *   - ファイルは作ったときの形の版 (schema_version)・見出し・商品ごとの編集の印をまとめた印 (aggregate_token)・中身のハッシュ (payload_hash)・
 *     byte 列の sha256 を固定して残す (変えない・消さない = 0052 の trigger)
 *   - 配った (issued) 後は、その商品の NE に送る欄を直せない (lib/master-write.mjs が 409 reg_csv_issued)。直すには人が「使わない」にして、
 *     NE で何をしたか (直し方) を書く。配る前 (built) に直したら、そのファイルは自動で「使わない」になる (作り直す)
 *   - 「取り込んだ」の申告 = ファイルの番号・sha256 (記録と同じ)・申告した人・時刻。ok / partial = 登録の状態 draft → ne_pending (0051 の関数)
 *   - NE にもうあるコード (最新の照合の NE の元のコード 0041) は新規登録しない = 止める (同じ登録か確かめる / 別のコード)
 *   - 単品は全部の列を 1 行 (空欄を作らない: 値の無い代表・JAN は「empty」)・セットは構成品 1 つで 1 行。セットは構成品が全部 NE 確認済みのときだけ
 *   - 実機の確かめの門: 種類 × 形の版ごとに ops.ne_csv_verified (col = 'new_registration') に ok があるときだけ本番のファイル。無い = 試し用 (REG_TRIAL_ROWS 行まで)。
 *     ok を残せるのは、同じ形の試しのファイルの商品が全部 NE で確かめ済み (verified) のときだけ
 *   - 確かめ (verified / partial / failed) は翌朝の照合 ② (apps/company-db/master-compare): NE の完全な取得の観測を DB に残す → 回が最後まで終わった受け取り →
 *     ops.record_ne_registration_check(回の番号) が受け取りと残した観測を自分で読む (#1571 R1 High 2)。
 *     NE 確認済み (ne_pending → ne_confirmed) にするのもこの関数だけ
 *   - 鍵の順 = request の鍵 → 切替の段階の共有の鍵 → マスタの書き込みの共有の鍵 (短く待つ) → SKU ごとの鍵 (sku_id の順・構成品も) → CSV の鍵 → NE の元のコードの共有の鍵 → 行
 *     (保存・構成の昇格と同じ順)
 *   - 🚨 表を書くのは DB の関数 (ops.ne_reg_build / ne_reg_issue / ne_reg_declare / ne_reg_supersede / ne_reg_record_verified・security definer) だけ。
 *     画面のロールに表の書き込みは無い。関数は形・状態・NE にまだ無いこと・byte 列・確かめる値と行が同じか・sha256 を自分で照らし直す
 *   🚨 在庫の列 (zaiko_su ほか) は見出しに入れない (ne-csv.mjs の FORBIDDEN_HEADERS)
 *   🚨 単品の形 (ne-reg-single-v1) は 10 §6.2 の列名から決めた。新規登録の CSV は実機で未確認 = 門が「試し用」だけにする (中原さんと実機の試しの後に ok)
 */
import crypto from 'node:crypto';
import { normSku } from './sku-norm.js';
import { MASTER_OWNERSHIP, validateOwnership } from '../config/master-ownership.mjs';
import { CSV_LOCK_SQL, SKU_LOCK_SQL } from '../apps/master-decisions/ne-csv-lock.mjs';
import { COLUMNS, FORBIDDEN_HEADERS, readNeCodes, todayRun, jstDate as jstDateMs } from '../apps/master-decisions/ne-csv.mjs';
import { canonicalSupplierCode } from '../apps/company-db/load/sources.mjs';
import { readCutoverPhase, newEntryWritable, CUTOVER_SHARED_LOCK_SQL } from './master-cutover.mjs';
import { deriveSetCdb } from './master-set-rules.js';
import { MasterWriteError, readCurrent, stable, sha256, jstDate, COMPANY_ID, janValid, liveRegItems, guardRegCsvOnSave, REG_LIVE_STATES, lockMasterWriteShared,
  beginMasterWrite } from './master-write.mjs';
export { janValid, liveRegItems, guardRegCsvOnSave, REG_LIVE_STATES };
import { NEW_ENTRY_KEYS } from './master-register.mjs';

/** 形 (種類ごと)。見出しを変えたら版を上げる = 前の版の実機の確かめは引き継がない */
export const REG_SCHEMAS = Object.freeze({
  products: Object.freeze({ kind: 'products', skuKind: 'single', schema: 'ne-reg-single-v1',
    header: Object.freeze(['syohin_code', 'syohin_name', 'sire_code', 'genka_tnk', 'baika_tnk', 'tax_rate', 'toriatukai_kbn', 'daihyo_syohin_code', 'jan_code']) }),
  sets: Object.freeze({ kind: 'sets', skuKind: 'set', schema: 'ne-reg-set-v1',
    header: Object.freeze(['set_syohin_code', 'set_syohin_name', 'set_baika_tnk', 'tax_rate', 'syohin_code', 'suryo']) }),
});
export const REG_KIND_LABELS = Object.freeze({ products: '単品', sets: 'セット' });
/** ops.ne_csv_verified の col (既にある商品の列と混ざらない名前) */
export const REG_VERIFIED_COL = 'new_registration';
/** 実機で確かめていない形 = 試し用のファイルはこの行数まで (0040 の「試し用 5 行まで」と同じ) */
export const REG_TRIAL_ROWS = 5;
export const REG_MAX_ROWS = 1000;
export const REG_ITEM_STATES = Object.freeze({
  built: '作った (まだ配っていない)', issued: '配った (取り込み待ち)', import_declared: '取り込んだと申告 (翌朝の照合待ち)',
  verified: 'NE で確かめた', partial: 'NE にあるが違う列がある', failed: '取り込めなかった', superseded: '使わない',
});
export const REG_RESULTS = Object.freeze({ ok: '全部成功', partial: '一部失敗 (メッセージに「N件失敗」)', rejected_all: '全部だめ' });
/** 構成品が NE にあると言える登録の状態 */
const NE_CONFIRMED_STATES = new Set(['ne_confirmed', 'distributable', 'available']);
const NE_CODES_SHARED_LOCK_SQL = `select pg_advisory_xact_lock_shared(hashtext('ops.ne_codes'))`;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const NEW_CODE_RE = /^[a-z0-9_-]{1,30}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const bad = (message, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, extra);
const conflict = (message, reason, extra = {}) => new MasterWriteError(409, reason, message, extra);

// ─── DB でも書き込みを始める (0050 の ops.begin_master_write・#1563 R3 M2) ───
/**
 * ⑤-2b の書き込みの「操作」と、その操作で書く表。画面のロール master_edit は、同じ取引で ops.begin_master_write をした後でないと書けない
 * (0052 の関数 ops.ne_reg_* / 仕入先の関数は ops.reg_write_session で行を確かめ、誰が・request_id をその行から取る)。
 * 🚨 ⑤-2b の begin は全部 beginRegWrite を通る。⑤-1 の次の形 (begin に操作・書く行を渡す / 表と操作の組 ops.master_write_allowed) に合わせるときは、
 *    ここ (REG_WRITE_OPERATIONS・REG_SAVE_EXTRA_TABLES) と beginRegWrite だけを変える。
 * tables = その操作で書く表 (画面のロールが直接書くのは core.external_ids だけ。ほかは security definer の関数の中で書く)
 */
export const REG_WRITE_OPERATIONS = Object.freeze({
  reg_csv_build: Object.freeze({ label: '新商品の NE 登録の CSV を作る', tables: Object.freeze(['ops.ne_reg_exports', 'ops.ne_reg_export_items', 'ops.ne_reg_export_rows']) }),
  reg_csv_issue: Object.freeze({ label: '新商品の NE 登録の CSV を配る', tables: Object.freeze(['ops.ne_reg_exports', 'ops.ne_reg_export_items']) }),
  reg_csv_declare: Object.freeze({ label: '新商品の NE 登録の CSV を取り込んだと申告',
    tables: Object.freeze(['ops.ne_reg_exports', 'ops.ne_reg_export_items', 'ops.ne_reg_attempts', 'ops.master_registrations', 'ops.master_registration_events']) }),
  reg_csv_supersede: Object.freeze({ label: '新商品の NE 登録の CSV を使わないにする', tables: Object.freeze(['ops.ne_reg_exports', 'ops.ne_reg_export_items']) }),
  reg_csv_verified: Object.freeze({ label: '新商品の NE 登録の CSV の実機の確かめ', tables: Object.freeze(['ops.ne_csv_verified']) }),
  supplier_create: Object.freeze({ label: '新しい仕入先', tables: Object.freeze(['core.suppliers', 'ops.supplier_registrations', 'ops.supplier_registration_events']) }),
  supplier_declare: Object.freeze({ label: '仕入先を NE に登録したと申告', tables: Object.freeze(['ops.supplier_registrations', 'ops.supplier_registration_events']) }),
  supplier_deactivate: Object.freeze({ label: '仕入先の取引停止', tables: Object.freeze(['core.suppliers', 'core.supplier_skus', 'ops.ne_reg_exports', 'ops.ne_reg_export_items']) }),
});
/** ⑤-1 の保存 (sku_edit)・⑤-2a の新商品の登録 (sku_create) の操作に ⑤-2b が足す表 (JAN を足す・外す・まだ配っていない NE 登録の CSV を使わないにする) */
export const REG_SAVE_EXTRA_TABLES = Object.freeze({
  sku_edit: Object.freeze(['core.external_ids', 'ops.ne_reg_exports', 'ops.ne_reg_export_items']),
  sku_create: Object.freeze(['core.external_ids']),
});
/**
 * ⑤-2b の書き込みを DB で始める (取引の中・門 (段階・持ち主表) の後・マスタの書き込みの鍵の前に 1 回)。
 * operation = REG_WRITE_OPERATIONS のキー。requestId が無ければこの操作 1 回の番号を作る。戻り値 = { request_id, operation }
 */
export async function beginRegWrite(db, { operation, requestId = null, actor, ownership, reason = null }) {
  const op = Object.prototype.hasOwnProperty.call(REG_WRITE_OPERATIONS, String(operation)) ? REG_WRITE_OPERATIONS[operation] : null;
  if (!op) throw new Error(`⑤-2b の書き込みの操作が分からない: ${operation}`);
  const rid = requestId || crypto.randomUUID();
  await beginMasterWrite(db, { requestId: rid, actor, reason: reason || op.label, ownership });
  return { request_id: rid, operation };
}

// ─── CSV の書き方 (UTF-8・BOM なし・CRLF・最後の行にも CRLF・カンマ / 引用符 / 前後の空白 (半角・全角) を含むセルだけ引用符) ───
// 🚨 DB の関数 ops.ne_reg_csv_cell (0052) と同じ決まり (関数が行から byte 列を組み直して同じかを見る)。変えるときは両方を同じ PR で
const EDGE_SPACE_RE = /^[ \u3000]|[ \u3000]$/;
const CELL_CTRL_RE = new RegExp(`[\x01-\x1f\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
export const regQuote = (s) => (/[",]/.test(s) || EDGE_SPACE_RE.test(s) ? `"${s.replace(/"/g, '""')}"` : s);
export function buildRegCsv(spec, rows) {
  if (spec.header.some((h) => FORBIDDEN_HEADERS.includes(h))) throw new Error(`出してはいけない列: ${spec.header.join(',')}`);
  for (const r of rows) if (r.length !== spec.header.length) throw new Error(`列の数が見出しと違う: ${r.length} / ${spec.header.length}`);
  for (const r of rows) for (const c of r) if (CELL_CTRL_RE.test(String(c))) throw new Error(`制御文字のセルは書かない: ${JSON.stringify(String(c)).slice(0, 40)}`);
  const lines = [spec.header.join(','), ...rows.map((r) => r.map((c) => regQuote(String(c))).join(','))];
  const bytes = Buffer.from(lines.join('\r\n') + '\r\n', 'utf8');
  return { bytes, sha256: crypto.createHash('sha256').update(bytes).digest('hex'), header: spec.header.join(',') };
}

// ─── 1 つの SKU から CSV の行と NE で確かめる値を作る ───
/**
 * 新規登録の CSV の材料。戻り値 { cur, spec, cells: [[...]], expected, blockers: [], warnings: [], stop: null | 'already_in_ne' }
 * ctx = { today, nc (readNeCodes), regByIds: Map<sku_id, 状態>, supRegByIds: Map<supplier_id, 状態>, live: Map<sku_id, item> }
 */
export async function regMaterialOf(db, skuId, ctx) {
  const cur = await readCurrent(db, skuId, ctx.today);
  const blockers = []; const warnings = [];
  const out = { cur, spec: null, cells: [], expected: null, blockers, warnings, stop: null };
  if (!cur) { blockers.push('商品が無い'); return out; }
  const spec = cur.sku_kind === 'single' ? REG_SCHEMAS.products : cur.sku_kind === 'set' ? REG_SCHEMAS.sets : null;
  out.spec = spec;
  if (!spec) { blockers.push('例外の SKU は NE に登録しない'); return out; }
  const reg = cur.registration?.state ?? null;
  if (!['draft', 'ne_pending'].includes(reg)) blockers.push(`登録の状態が ${reg ?? 'なし'} (下書き・NE 登録待ちの商品だけ CSV にできる)`);
  if (ctx.live.has(cur.sku_id)) {
    const x = ctx.live.get(cur.sku_id);
    blockers.push(`ファイル #${x.export_id} (${REG_ITEM_STATES[x.state] || x.state}) がまだ終わっていない (使わないにするか、翌朝の確かめを待つ)`);
  }
  if (!NEW_CODE_RE.test(cur.code)) blockers.push(`コード ${cur.code} は新しいコードの形 (小文字の英数字・- _・30 字まで) でない`);
  const own = ctx.nc.map.get(`product|${cur.code_norm}`);
  if (own) {
    out.stop = 'already_in_ne';
    blockers.push(`コード ${cur.code} は NE にもうある (${own.state === 'ok' ? own.ne_code : own.state}) = 新規登録しない。同じ登録か確かめる (翌朝の照合) か、別のコードに`);
  }
  const nameCell = COLUMNS['products:name'].cell(cur.name);
  if (!nameCell.ok) blockers.push(`名前を CSV に書けない (${nameCell.reason})`);
  const price = cur.standard_price;
  if (!(Number.isInteger(price) && price >= 1 && price <= 999999999)) blockers.push('標準売価が無い (1〜999,999,999 円)');
  if (spec === REG_SCHEMAS.products) {
    const taxCell = cur.tax_rate === 0.1 ? '10' : cur.tax_rate === 0.08 ? '8' : null;
    if (!taxCell) blockers.push('税率が無い (8% か 10%)');
    const handlingCell = cur.handling === 'active' ? '0' : cur.handling === 'discontinued' ? '1' : null;
    if (!handlingCell) blockers.push('取扱区分が 取扱中 / 中止 でない');
    const cost = cur.cost_today && ['COMPLETE', 'OVERRIDDEN'].includes(cur.cost_today.cost_status) ? cur.cost_today.cost_jpy : null;
    if (!(Number.isInteger(cost) && cost >= 1)) blockers.push('原価が無い (今日の原価・1 円以上)');
    const prim = cur.supplier_rows.filter((x) => x.is_primary);
    let supCode = null;
    if (prim.length !== 1) blockers.push('代表の仕入先が決まっていない (NE の「設定なし」は 9999)');
    else {
      supCode = canonicalSupplierCode(prim[0].code);
      if (!/^\d{4}$/.test(supCode)) blockers.push(`代表の仕入先のコード ${prim[0].code} は NE の形 (4 桁の数字) でない`);
      const st = ctx.supRegByIds.get(String(prim[0].supplier_id));
      if (st && st !== 'ne_confirmed') blockers.push(`代表の仕入先 ${prim[0].code} は「NE に登録した」の申告がまだ`);
    }
    // 代表 (親): なし = empty / あり = 名札か商品の NE の元の書き方
    let parentCell = 'empty'; let parentNorm = null;
    if (cur.parent_product_id) {
      parentNorm = normSku(cur.parent_code ?? '');
      const pr = ctx.nc.map.get(`rep|${parentNorm}`) || ctx.nc.map.get(`product|${parentNorm}`);
      if (!parentNorm || !pr || pr.state !== 'ok') blockers.push(`代表 (親) ${cur.parent_code ?? '(コードなし)'} の NE の書き方が確かめられない`);
      else parentCell = pr.ne_code;
    }
    const jans = cur.jan_rows.filter((j) => j.valid_to == null).map((j) => j.external_value);
    if (jans.length > 1) blockers.push(`JAN が ${jans.length} つある (NE には 1 つ)`);
    for (const j of jans) if (!janValid(j)) blockers.push(`JAN ${j} のチェック数字が合わない`);
    out.cells = [[cur.code, nameCell.ok ? nameCell.cell : '', supCode ?? '', cost ?? '', price ?? '', taxCell ?? '', handlingCell ?? '', parentCell, jans[0] ?? 'empty']];
    out.expected = { kind: 'single', values: { name: cur.name, supplier: supCode ? normSku(supCode) : null, cost, price, tax_rate: cur.tax_rate, handling: cur.handling, parent: parentNorm } };
  } else {
    // セット: 構成 = 開いている構成の依頼 (新しいセットは core.sku_components が空) か、今の構成
    const rows = cur.component_request ? cur.component_request.rows.map((r) => ({ ...r, sort: Number(r.sort) }))
      : cur.components.map((c) => ({ ...c, sort: c.position }));
    rows.sort((a, b) => a.sort - b.sort);
    if (!rows.length) blockers.push('構成品が無い');
    const d = deriveSetCdb(rows, { override: cur.set_sales_class_override, handlingOwn: cur.handling_own, currentHandling: cur.handling, exceptionCost: true });
    const tax = d.tax.taxRate;
    const taxCell = tax === 0.1 ? '10' : tax === 0.08 ? '8' : null;
    if (!taxCell) blockers.push('セットの税率が決まらない (構成品の税率)');
    else if (cur.tax_rate != null && cur.tax_rate !== tax) warnings.push(`保存しているセットの税率 (${cur.tax_rate}) と構成品から導いた税率 (${tax}) が違う (CSV は導いた税率)`);
    const cells = [];
    for (const r of rows) {
      const st = ctx.regByIds.get(String(r.child_sku_id));
      if (!NE_CONFIRMED_STATES.has(st)) blockers.push(`構成品 ${r.code} が NE 確認済みでない (${st ?? '状態なし'})`);
      const sp = ctx.nc.map.get(`product|${normSku(r.code)}`);
      if (!sp || sp.state !== 'ok') blockers.push(`構成品 ${r.code} の NE の書き方が確かめられない`);
      if (!(Number.isInteger(r.qty) && r.qty >= 1 && r.qty <= 999)) blockers.push(`構成品 ${r.code} の数量 ${r.qty}`);
      cells.push([cur.code, nameCell.ok ? nameCell.cell : '', price ?? '', taxCell ?? '', sp && sp.state === 'ok' ? sp.ne_code : '', r.qty]);
    }
    out.cells = cells;
    out.expected = { kind: 'set', values: { name: cur.name, price, children: rows.map((r) => ({ code_norm: normSku(r.code), qty: r.qty })) } };
  }
  return out;
}

// ─── 読む ───
const EXPORT_COLS = `export_id::text as export_id, kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256,
  request_id::text as request_id, ne_codes_run, created_by, created_at::text as created_at, (extract(epoch from created_at) * 1000)::float8 as created_ms, state,
  issued_at::text as issued_at, issued_by, declared_at::text as declared_at, declared_by, closed_at::text as closed_at, closed_by, close_reason`;
const ITEM_COLS = `i.item_id::text as item_id, i.export_id::text as export_id, i.sku_id::text as sku_id, i.code_norm, i.ne_code, i.sku_kind, i.item_token, i.expected,
  i.snapshot_hash, i.row_from, i.row_to, i.state, i.state_changed_at::text as state_changed_at, i.state_changed_by, i.attempt_id::text as attempt_id,
  i.verified_run, i.verified_at::text as verified_at, i.failed_reason, i.superseded_reason, i.superseded_correction`;
export function regFileName(e) {
  const t = new Date(Number(e.created_ms) + 9 * 3600000).toISOString().replace(/[-:]/g, '').replace('T', '_').slice(0, 15);
  return `ne_register_${e.kind}_${t}_${e.export_id}${e.trial ? '_trial' : ''}.csv`;
}
const publicExport = (e) => { const { created_ms, ...rest } = e; return { ...rest, file_name: regFileName(e) }; };

async function applied(db) {
  return (await db.query(`select to_regclass('ops.ne_reg_exports') is not null as ok`)).rows[0].ok;
}
/** 実機で確かめた形か (種類ごと・最後の記録が ok) */
export async function regFormatVerified(db, spec) {
  const r = (await db.query(`select result from ops.ne_csv_verified where kind = $1 and col = $2 and encoding = 'utf8' and header = $3 and converter_version = $4
     order by verified_id desc limit 1`, [spec.kind, REG_VERIFIED_COL, spec.header.join(','), spec.schema])).rows[0];
  return !!r && r.result === 'ok';
}
async function regStatesOf(db, ids) {
  if (!ids.length) return new Map();
  return new Map((await db.query('select sku_id::text as id, state from ops.master_registrations where sku_id = any($1::bigint[])', [ids.map(String)])).rows.map((r) => [r.id, r.state]));
}
/** セットの構成品 (今の構成 + 開いている構成の依頼) の sku_id */
async function childIdsOf(db, setIds) {
  if (!setIds.length) return [];
  return [...new Set((await db.query(`select child_sku_id::text as id from core.sku_components where parent_sku_id = any($1::bigint[])
      union select (x ->> 'sku_id') from ops.sku_component_requests q, jsonb_array_elements(q.rows) x where q.set_sku_id = any($1::bigint[]) and q.status = 'open'`,
  [setIds.map(String)])).rows.map((r) => r.id))];
}
async function supplierStatesOf(db) {
  if (!(await db.query(`select to_regclass('ops.supplier_registrations') is not null as ok`)).rows[0].ok) return new Map();
  return new Map((await db.query('select supplier_id::text as id, state from ops.supplier_registrations')).rows.map((r) => [r.id, r.state]));
}

/** 画面の上: 今日の照合の回・NE の元のコード・形ごとの実機の確かめ・候補 (下書き / NE 登録待ち) と止まる理由・最近のファイル */
export async function regSummary(db, { nowMs = Date.now() } = {}) {
  if (!(await applied(db))) return { applied: false };
  const tr = await todayRun(db, nowMs);
  const nc = await readNeCodes(db);
  const today = jstDate(new Date(nowMs));
  const regs = (await db.query(`select r.sku_id::text as sku_id, r.state, s.code, s.sku_kind from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id
     where r.state in ('draft', 'ne_pending') order by s.code_norm limit 200`)).rows;
  const live = await liveRegItems(db, regs.map((r) => r.sku_id));
  const ctx = { today, nc, live, regByIds: await regStatesOf(db, await childIdsOf(db, regs.filter((r) => r.sku_kind === 'set').map((r) => r.sku_id))),
    supRegByIds: await supplierStatesOf(db) };
  const candidates = [];
  for (const r of regs) {
    const m = await regMaterialOf(db, r.sku_id, ctx);
    candidates.push({ sku_id: r.sku_id, code: r.code, kind: r.sku_kind === 'set' ? 'sets' : 'products', state: r.state, live: live.get(r.sku_id) || null,
      blockers: m.blockers, warnings: m.warnings, stop: m.stop });
  }
  const verified = {};
  for (const spec of Object.values(REG_SCHEMAS)) verified[spec.kind] = { schema: spec.schema, header: spec.header.join(','), ok: await regFormatVerified(db, spec) };
  const exps = (await db.query(`select ${EXPORT_COLS} from ops.ne_reg_exports where state <> 'closed' or export_id in (select export_id from ops.ne_reg_exports order by export_id desc limit 30)
     order by export_id desc`)).rows.map(publicExport);
  const items = exps.length ? (await db.query(`select ${ITEM_COLS}, s.code from ops.ne_reg_export_items i join core.skus s on s.sku_id = i.sku_id where i.export_id = any($1::bigint[]) order by i.export_id, i.row_from`,
    [exps.map((e) => e.export_id)])).rows : [];
  const checks = items.length ? (await db.query(`select distinct on (item_id) item_id::text as item_id, compare_run_id, outcome, detail, recorded_at::text as recorded_at from ops.ne_reg_checks
     where item_id = any($1::bigint[]) order by item_id, check_id desc`, [items.map((i) => i.item_id)])).rows : [];
  const lastCheck = new Map(checks.map((c) => [c.item_id, c]));
  for (const e of exps) e.items = items.filter((i) => i.export_id === e.export_id).map((i) => ({ ...i, last_check: lastCheck.get(i.item_id) || null }));
  return { applied: true, today: tr.today, run: tr.run, today_jst: jstDateMs(nowMs), ne_codes: { run: nc.run, current: !!nc.run && nc.run === tr.run }, verified, candidates, exports: exps,
    trial_rows: REG_TRIAL_ROWS, max_rows: REG_MAX_ROWS };
}

/** 1 つの SKU の新規登録の CSV (画面 B・C の箱)。無ければ [] */
export async function regItemsOfSku(db, skuId) {
  if (!(await applied(db))) return [];
  return (await db.query(`select ${ITEM_COLS}, e.trial, e.state as export_state from ops.ne_reg_export_items i join ops.ne_reg_exports e on e.export_id = i.export_id
     where i.sku_id = $1 order by i.item_id desc limit 10`, [skuId])).rows;
}

/** 配る byte 列 (配った・申告したファイルだけ = 作っただけのファイルは「配る」を押してから)。無い = null */
export async function regExportFile(db, exportId) {
  if (!/^\d{1,19}$/.test(String(exportId))) return null;
  const e = (await db.query(`select ${EXPORT_COLS}, file_bytes from ops.ne_reg_exports where export_id = $1`, [String(exportId)])).rows[0];
  if (!e) return null;
  if (!['issued', 'declared'].includes(e.state)) return { state: e.state, file_name: regFileName(e), bytes: null };
  const bytes = Buffer.from(e.file_bytes);
  if (crypto.createHash('sha256').update(bytes).digest('hex') !== e.sha256) throw new Error(`ファイル ${exportId} の sha256 が記録と違う`);
  return { state: e.state, file_name: regFileName(e), bytes, sha256: e.sha256 };
}

/// ─── 書く ───
// 🚨 新規登録の CSV の表を書くのは DB の関数 (0052・security definer) だけ。画面のロール (master_edit) には表の INSERT / UPDATE / DELETE が無い。
//    ここ (lib) は鍵を取り、読んで分かりやすい理由で先に止め、関数を呼ぶ。関数も同じことを自分で照らし直す (呼び手を信じない)
/** 門: 切替の段階 new_open・持ち主表のハッシュ・この種類の列の持ち主が company・MASTER_EDIT_OPEN (新商品の登録と同じ) */
async function assertOpen(db, kind, { ownership, open }) {
  const phase = await readCutoverPhase(db, { inTx: true });
  const keys = NEW_ENTRY_KEYS[kind === 'sets' ? 'set' : 'single'];
  const loadKeys = keys.filter((k) => ownership[k] !== 'company');
  if (!newEntryWritable(phase, ownership) || !open || loadKeys.length) {
    throw conflict(`切替前です: 新商品の NE 登録の CSV はまだ使えません (${!phase.readable ? '段階が読めない' : phase.phase !== 'new_open' ? `段階 ${phase.phase}` : !open ? 'MASTER_EDIT_OPEN なし' : '持ち主表'})`,
      'before_cutover', { phase: phase.readable ? phase.phase : null, load_keys: loadKeys });
  }
}
async function lockSkus(db, ids) {
  const list = [...new Set(ids.map(String))].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0));
  for (const id of list) await db.query(SKU_LOCK_SQL, [id]);
  return list;
}
/** 1 つの取引。beforeCommit = 試験だけ (commit の直前に待つ) */
async function tx(db, fn, beforeCommit = null) {
  await db.query('begin');
  try { const r = await fn(); if (beforeCommit) await beforeCommit(); await db.query('commit'); return r; } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
}
function actorOf(v) {
  const a = String(v ?? '').trim().toLowerCase();
  if (!a || a.length > 320 || CTRL_RE.test(a)) throw bad('操作する人 (ログインのメール) が分からない');
  return a;
}
const needId = (id) => { if (!/^\d{1,19}$/.test(String(id ?? ''))) throw bad('ファイルの番号が違う'); return String(id); };

/** DB の関数の誤り (「理由: 文」の形) → MasterWriteError。理由が無い誤りはそのまま上げる */
const FN_STATUS = Object.freeze({ not_found: 404, invalid_input: 400, expected_mismatch: 400, bytes_mismatch: 400, too_many: 400, caller_hash: 400 });
async function callFn(db, sql, params) {
  try {
    return (await db.query(sql, params)).rows[0].r;
  } catch (e) {
    const m = /^([a-z_]+): ([\s\S]*)$/.exec(String((e && e.message) || ''));
    if (m && e && ['P0001', 'P0002', '22023', '23505'].includes(e.code)) throw new MasterWriteError(FN_STATUS[m[1]] ?? 409, m[1], m[2]);
    throw e;
  }
}
async function readExport(db, exportId) {
  return (await db.query(`select ${EXPORT_COLS} from ops.ne_reg_exports where export_id = $1`, [exportId])).rows[0] ?? null;
}

/**
 * ファイルを作る。input = { actor, kind: 'products' | 'sets', codes: [...], requestId }
 * opts = { ownership, open, nowMs }。1 つでも止まる商品があれば作らない (誤りに商品ごとの理由)
 */
export async function buildRegExport(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const nowMs = opts.nowMs ?? Date.now();
  const actor = actorOf(input?.actor);
  const spec = Object.prototype.hasOwnProperty.call(REG_SCHEMAS, String(input?.kind)) ? REG_SCHEMAS[input.kind] : null;
  if (!spec) throw bad('種類は 単品 (products) か セット (sets)');
  const requestId = String(input?.requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(requestId)) throw bad('作る操作の番号 (request_id) の形が違う。画面を開き直してください');
  const codes = Array.isArray(input?.codes) ? input.codes.map((c) => normSku(String(c ?? ''))).filter(Boolean) : [];
  if (!codes.length || codes.length > REG_MAX_ROWS) throw bad(`商品を 1〜${REG_MAX_ROWS} 件選んでください`);
  if (new Set(codes).size !== codes.length) throw bad('同じ商品が 2 回');
  const wantKey = sha256(stable({ kind: spec.kind, codes: [...codes].sort(), actor }));
  return tx(db, async () => {
    // 鍵: request → 切替の段階 (共有) → (開いているかを見る) → マスタの書き込み (共有・短く待つ) → SKU ごと (商品 + 構成品・依頼の構成品) → CSV → NE の元のコード (共有) → 行
    await db.query(`select pg_advisory_xact_lock(hashtextextended('ops.ne_reg_request:' || $1::text, 0))`, [requestId]);
    const prev = (await db.query(`select ${EXPORT_COLS} from ops.ne_reg_exports where request_id = $1`, [requestId])).rows[0];
    if (prev) {
      const prevCodes = (await db.query('select code_norm from ops.ne_reg_export_items where export_id = $1', [prev.export_id])).rows.map((r) => r.code_norm);
      if (sha256(stable({ kind: prev.kind, codes: prevCodes.sort(), actor: prev.created_by })) !== wantKey) throw conflict('同じ番号 (request_id) で違う中身が来ました。画面を開き直してください', 'request_id_reused');
      return { export: publicExport(prev), replayed: true };
    }
    await db.query(CUTOVER_SHARED_LOCK_SQL);
    await assertOpen(db, spec.kind, { ownership, open: opts.open === true });
    // DB でも始める (誰が・request_id = この行。関数 ops.ne_reg_build はこの行と同じ request_id・誰が だけを受ける)
    await beginRegWrite(db, { operation: 'reg_csv_build', requestId, actor, ownership });
    await lockMasterWriteShared(db);
    const skus =(await db.query('select sku_id::text as id, code_norm, sku_kind from core.skus where company_id = $1 and code_norm = any($2::text[])', [COMPANY_ID, codes])).rows;
    const byNorm = new Map(skus.map((s) => [s.code_norm, s]));
    const missing = codes.filter((c) => !byNorm.has(c));
    if (missing.length) throw bad(`Company DB に無い商品: ${missing.join('・')}`, 'not_found');
    const wrongKind = codes.filter((c) => byNorm.get(c).sku_kind !== spec.skuKind);
    if (wrongKind.length) throw bad(`${REG_KIND_LABELS[spec.kind]}でない商品: ${wrongKind.join('・')}`);
    const ids = codes.map((c) => byNorm.get(c).id);
    const childIds = spec.kind === 'sets' ? await childIdsOf(db, ids) : [];
    await lockSkus(db, [...ids, ...childIds]);
    await db.query(CSV_LOCK_SQL);
    await db.query(NE_CODES_SHARED_LOCK_SQL);
    await db.query('select sku_id from core.skus where sku_id = any($1::bigint[]) order by sku_id for update', [ids]);
    const tr = await todayRun(db, nowMs);
    const nc = await readNeCodes(db);
    if (!tr.today || !nc.run || nc.run !== tr.run) {
      throw conflict('今日 (JST) の照合の回と、その回の NE の元のコード (NE に同じコードが無いことの確かめ) がまだありません。今朝の照合の後に作ってください', 'no_today_run');
    }
    const today = jstDate(new Date(nowMs));
    const ctx = { today, nc, live: await liveRegItems(db, ids), regByIds: await regStatesOf(db, [...new Set(childIds)]), supRegByIds: await supplierStatesOf(db) };
    const mats = [];
    for (const id of ids) mats.push(await regMaterialOf(db, id, ctx));
    const stopped = mats.filter((m) => m.blockers.length).map((m) => ({ code: m.cur?.code, stop: m.stop, blockers: m.blockers }));
    if (stopped.length) {
      const inNe = stopped.filter((s) => s.stop === 'already_in_ne');
      throw conflict(`CSV にできない商品が ${stopped.length} 件あります (何も作っていません)${inNe.length ? `。NE にもうあるコード: ${inNe.map((s) => s.code).join('・')}` : ''}`,
        inNe.length ? 'already_in_ne' : 'not_ready', { items: stopped });
    }
    const rowCount = mats.reduce((n, m) => n + m.cells.length, 0);
    if (rowCount > REG_MAX_ROWS) throw bad(`1 つのファイルに ${REG_MAX_ROWS} 行まで (今 ${rowCount} 行)`);
    const verified = await regFormatVerified(db, spec);
    if (!verified && rowCount > REG_TRIAL_ROWS) {
      throw conflict(`この形 (${spec.schema}) は実機でまだ確かめていないので、試し用 = ${REG_TRIAL_ROWS} 行までです (今 ${rowCount} 行)`, 'trial_limit', { rows: rowCount });
    }
    const allRows = mats.flatMap((m) => m.cells.map((r) => r.map(String)));
    const csv = buildRegCsv(spec, allRows);
    // 書くのは関数: 鍵の後に Company DB の今の値から行と確かめる値を作り直し、送った行・確かめる値と完全に同じときだけ作る (#1571 Codex R1 High 1)。
    //   印とハッシュ 4 つ (item_token・snapshot_hash・payload_hash・aggregate_token) は関数が計算する (送らない)。cost_day = 原価を見た東京の日
    const r = await callFn(db, 'select ops.ne_reg_build($1::jsonb, $2::bytea) as r', [JSON.stringify({
      request_id: requestId, actor, kind: spec.kind, schema_version: spec.schema, header: csv.header, ne_codes_run: nc.run, cost_day: today,
      items: mats.map((m) => ({ sku_id: String(m.cur.sku_id), expected: m.expected, rows: m.cells.map((row) => row.map(String)) })),
    }), csv.bytes]);
    const e = await readExport(db, String(r.export_id));
    return { export: publicExport(e), ...(r.replayed ? { replayed: true } : {}), warnings: mats.flatMap((m) => m.warnings.map((w) => `${m.cur.code}: ${w}`)) };
  }, opts.beforeCommit);
}

/** ファイルの商品の SKU の鍵 → CSV の鍵 → 読む (書き手は全部この鍵の後に書く = 読んだ後に変わらない。行の鍵は関数が取る) */
async function lockExport(db, exportId) {
  const ids = (await db.query('select sku_id::text as id from ops.ne_reg_export_items where export_id = $1', [exportId])).rows.map((r) => r.id);
  await lockSkus(db, ids);
  await db.query(CSV_LOCK_SQL);
  const e = await readExport(db, exportId);
  if (!e) throw new MasterWriteError(404, 'not_found', `ファイル #${exportId} がありません`);
  const items = (await db.query(`select ${ITEM_COLS} from ops.ne_reg_export_items i where i.export_id = $1 order by i.row_from`, [exportId])).rows;
  return { e, items };
}
/** 書く前の鍵: 切替の段階 (共有) → 開いているか → DB でも始める (operation) → マスタの書き込み (共有・短く待つ) → ファイルの SKU → CSV */
async function openAndLockExport(db, exportId, { ownership, open, operation, actor, requestId = null }) {
  await db.query(CUTOVER_SHARED_LOCK_SQL);
  const e0 = await readExport(db, exportId);
  if (!e0) throw new MasterWriteError(404, 'not_found', `ファイル #${exportId} がありません`);
  await assertOpen(db, e0.kind, { ownership, open: open === true });
  await beginRegWrite(db, { operation, requestId, actor, ownership });
  await lockMasterWriteShared(db);
  return lockExport(db, exportId);
}

/** 配る (ダウンロードの前に押す)。built → issued (商品も)。もう配った = そのまま (もう一度ダウンロードできる)。使わない商品が 1 つでもある = 配らない (作り直す) */
export async function issueRegExport(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const actor = actorOf(input?.actor);
  const exportId = needId(input?.exportId);
  return tx(db, async () => {
    const { e } = await openAndLockExport(db, exportId, { ownership, open: opts.open, operation: 'reg_csv_issue', actor, requestId: opts.requestId });
    if (e.state === 'closed') throw conflict(`ファイル #${exportId} は閉じています (${e.close_reason})。配れません`, 'closed');
    if (e.state !== 'built') return { export: publicExport(e), already: true };
    const r = await callFn(db, 'select ops.ne_reg_issue($1::bigint, $2) as r', [exportId, actor]);
    const x = await readExport(db, exportId);
    if (r.refused) return { export: publicExport(x), refused: true, reason: r.reason, codes: r.codes };
    return { export: publicExport(x), already: !!r.already };
  });
}

/**
 * NE に取り込んだと申告する。input = { actor, exportId, sha256 (ファイルの記録と同じ), result: ok | partial | rejected_all, neMessage?, importedAt?, note? }
 * ok / partial = 商品 issued → import_declared・登録の状態 draft → ne_pending (根拠は DB の関数がこの申告の記録から自分で読む) / rejected_all = 商品 failed・ファイルを閉じる。
 * 申告したファイルにもう一度 = 試みを足すだけ
 */
export async function declareRegExport(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const actor = actorOf(input?.actor);
  const exportId = needId(input?.exportId);
  const sha = String(input?.sha256 ?? '').trim().toLowerCase();
  if (!HEX64.test(sha)) throw bad('取り込んだファイルの sha256 (64 桁) を入れてください (画面のファイルの sha256 と同じか確かめる)');
  const result = String(input?.result ?? '');
  if (!Object.prototype.hasOwnProperty.call(REG_RESULTS, result)) throw bad(`結果は ${Object.keys(REG_RESULTS).join(' / ')} のどれか`);
  const msg = input?.neMessage == null ? null : String(input.neMessage).trim() || null;
  if (msg && (msg.length > 1000 || /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(msg))) throw bad('NE のメッセージは 1,000 字まで (制御文字なし)');
  const note = input?.note == null ? null : String(input.note).trim() || null;
  if (note && (note.length > 500 || CTRL_RE.test(note))) throw bad('メモは 500 字まで (改行なし)');
  let importedAt = null;
  if (input?.importedAt != null && String(input.importedAt).trim()) {
    const ms = Date.parse(String(input.importedAt));
    if (!Number.isFinite(ms)) throw bad('取り込んだ時刻が読めない');
    importedAt = new Date(ms).toISOString();
  }
  return tx(db, async () => {
    const { e, items } = await openAndLockExport(db, exportId, { ownership, open: opts.open, operation: 'reg_csv_declare', actor, requestId: opts.requestId });
    if (e.sha256 !== sha) throw conflict('sha256 がこのファイルの記録と違います (違うファイルを取り込んでいないか確かめてください)。何も記録していません', 'sha256_mismatch');
    if (e.state === 'built') throw conflict('先に「配る (ダウンロード)」を押してください', 'not_issued');
    if (e.state === 'closed') throw conflict(`ファイル #${exportId} は閉じています (${e.close_reason})。取り込んでしまったなら、つかいかたの「使わないファイルを取り込んでしまったら」を見てください`, 'closed');
    if (importedAt && e.issued_at && Date.parse(importedAt) < Date.parse(e.issued_at) - 60000) throw bad('取り込んだ時刻が配った時刻より前です');
    if (importedAt && Date.parse(importedAt) > Date.now()) throw bad('取り込んだ時刻が今より後です');
    const r = await callFn(db, 'select ops.ne_reg_declare($1::bigint, $2, $3, $4, $5, $6::timestamptz, $7) as r', [exportId, actor, sha, result, msg, importedAt, note]);
    const out = { state: r.state, attempt_id: String(r.attempt_id) };
    if (r.again) return { ...out, first_declared_at: e.declared_at, again: true };
    if (r.state === 'closed') return { ...out, failed: items.filter((i) => i.state === 'issued').length };
    return { ...out, ne_pending: r.ne_pending || [] };
  }, opts.beforeCommit);
}

/**
 * 使わないにする (人)。input = { actor, exportId, reason, correction (NE で何をしたか = 直し方), confirm: true }
 * まだ終わっていない商品 (built / issued / import_declared / partial) を superseded・ファイルを閉じる。登録の状態は変えない
 */
export async function supersedeRegExport(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const actor = actorOf(input?.actor);
  const exportId = needId(input?.exportId);
  const text = (v, label) => {
    const s = String(v ?? '').trim();
    if (!s) throw bad(`${label}を書いてください`);
    if (s.length > 500 || CTRL_RE.test(s)) throw bad(`${label}は 500 字まで (改行なし)`);
    return s;
  };
  const reason = text(input?.reason, '理由');
  const correction = text(input?.correction, 'NE で何をしたか (取り込んでいない / NE で消した / NE の画面で直した など)');
  if (input?.confirm !== true) throw bad('「NE の状態を確かめた」に印を付けてください');
  return tx(db, async () => {
    const { e } = await openAndLockExport(db, exportId, { ownership, open: opts.open, operation: 'reg_csv_supersede', actor, requestId: opts.requestId });
    if (e.state === 'closed') throw conflict(`ファイル #${exportId} はもう閉じています (${e.close_reason})`, 'closed');
    const r = await callFn(db, 'select ops.ne_reg_supersede($1::bigint, $2, $3, $4) as r', [exportId, actor, reason, correction]);
    return { state: r.state, superseded: Number(r.superseded) };
  });
}

/**
 * 実機で確かめた結果を残す (種類ごと・今の形の版)。ok = 同じ形の試しのファイル (exportId) の商品が全部 NE で確かめ済み (verified) のときだけ / ng はいつでも。
 * opts = { ownership, open } (ほかの書き込みと同じ門: 切替の後だけ・DB でも始める)
 */
export async function recordRegVerified(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const actor = actorOf(input?.actor);
  const spec = Object.prototype.hasOwnProperty.call(REG_SCHEMAS, String(input?.kind)) ? REG_SCHEMAS[input.kind] : null;
  if (!spec) throw bad('種類は products / sets');
  const result = String(input?.result ?? '');
  if (!['ok', 'ng'].includes(result)) throw bad('結果は ok / ng');
  const note = input?.note == null ? null : String(input.note).trim() || null;
  if (note && (note.length > 500 || CTRL_RE.test(note))) throw bad('メモは 500 字まで');
  const exportId = input?.exportId == null || input.exportId === '' ? null : needId(input.exportId);
  if (result === 'ok' && !exportId) throw bad('ok は、確かめに使った試しのファイルの番号が要ります');
  return tx(db, async () => {
    // 門 (段階の共有の鍵 → 開いているか) → DB でも始める → マスタの書き込みの共有の鍵 → CSV の鍵 (ほかの書き込みと同じ順)
    await db.query(CUTOVER_SHARED_LOCK_SQL);
    await assertOpen(db, spec.kind, { ownership, open: opts.open === true });
    await beginRegWrite(db, { operation: 'reg_csv_verified', requestId: opts.requestId, actor, ownership });
    await lockMasterWriteShared(db);
    await db.query(CSV_LOCK_SQL);
    if (exportId) {
      const e = (await db.query(`select kind, schema_version, header, trial from ops.ne_reg_exports where export_id = $1`, [exportId])).rows[0];
      if (!e) throw new MasterWriteError(404, 'not_found', `ファイル #${exportId} がありません`);
      if (e.kind !== spec.kind || e.schema_version !== spec.schema || e.header !== spec.header.join(',')) throw conflict('ファイルの種類・形の版・見出しが今の形と違う', 'export_mismatch');
      if (result === 'ok') {
        const st = (await db.query('select state, count(*)::int as n from ops.ne_reg_export_items where export_id = $1 group by state', [exportId])).rows;
        if (!st.length || st.some((x) => x.state !== 'verified')) throw conflict(`ファイル #${exportId} の商品がまだ全部 NE で確かめ済みになっていません (${st.map((x) => `${x.state} ${x.n}`).join('・')})`, 'not_verified');
      }
    }
    const r = await callFn(db, 'select ops.ne_reg_record_verified($1, $2, $3, $4, $5, $6, $7::bigint) as r', [actor, spec.kind, spec.schema, spec.header.join(','), result, note, exportId]);
    return { verified_id: String(r.verified_id) };
  });
}
