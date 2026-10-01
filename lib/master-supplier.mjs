/**
 * master-supplier.mjs — 仕入先の新しいコード・「NE に登録した」の申告・取引停止 (Company DB構想 14 §9 v2 M1・M5 / §10 契約 v3 Medium 3 / 10 §3.2。migration 0052)
 *
 * 使うところ: 切替の後の発注アプリの「マスタ管理 > 仕入先」(書き込み先を Company DB に差し替えるのは切替の手順 = ⑥。この PR では画面は変えない)
 * コードの決まり (新しい仕入先だけ):
 *   - 数字だけ・1〜4 桁で入れる → 4 桁に 0 で埋める (NE の形 = '1' → '0001'。apps/company-db/load/sources.mjs の canonicalSupplierCode と同じ)
 *   - 0000 と 9999 (NE の「設定なし」) は使えない
 *   - 一意 = 4 桁に揃えた形で、前からある仕入先 (数字以外・5 桁以上も) と同じにならない
 *   - 前からある仕入先の数字以外・5 桁以上のコードはそのまま (変えない・この決まりで落とさない)
 * 状態 (ops.supplier_registrations・新しい仕入先だけに行): ne_pending (作った) → ne_confirmed (人が「NE に登録した」と申告・誰・いつ・根拠)。
 *   行が無い = 前からある仕入先 (今までどおり使える)。ne_pending の仕入先は代表の仕入先に選べない (lib/master-write.mjs・DB の trigger)
 * 取引停止 = active を false (物理の削除はしない = DB の trigger が拒む)。代表の仕入先に使っている間は止められない (同じ取引で付け替えれば止められる)
 * 🚨 状態の表 (ops.supplier_registrations) を書くのは DB の関数 (ops.create_supplier_registration / ops.declare_supplier_in_ne・security definer・0052) だけ。
 *    仕入先の行 (core.suppliers・supplier_skus) を書くロールの権限は、発注アプリの書き込み先を差し替える ⑥ で決める (今の画面のロール master_edit には無い)
 * 門と鍵 (保存の道と同じ): 切替の段階の共有の鍵 → 段階 new_open・持ち主表のハッシュ・仕入先の列 (suppliers.*) の持ち主が company (付け替えは supplier_skus.is_primary も)
 *   → DB でも書き込みを始める (0050 の ops.begin_master_write を lib/master-reg-csv.mjs の beginRegWrite で。操作 = supplier_create / supplier_declare / supplier_deactivate。
 *     画面のロールに渡したとき (⑥) も、DB の関数がこの行の 誰が を使う) → マスタの書き込みの共有の鍵 (夜間ロードが持っていれば短く待って 409 nightly_load) → (付け替え) 商品の SKU の鍵 (sku_id の順) → CSV の鍵 → 仕入先の行
 * 付け替え = 代表の仕入先を変える = NE に送る欄: NE に取り込む CSV (0040) の primary_supplier が出ている商品 = 409 csv_issued /
 *   新商品の NE 登録の CSV (0052) を配った後の商品 = 409 reg_csv_issued (作っただけのファイルは使わないにする)。画面の保存と同じ決まり
 */
import { canonicalSupplierCode } from '../apps/company-db/load/sources.mjs';
import { MASTER_OWNERSHIP, validateOwnership } from '../config/master-ownership.mjs';
import { CSV_LOCK_SQL, SKU_LOCK_SQL } from '../apps/master-decisions/ne-csv-lock.mjs';
import { readCutoverPhase, newEntryWritable, CUTOVER_SHARED_LOCK_SQL } from './master-cutover.mjs';
import { MasterWriteError, COMPANY_ID, SOURCE_SYSTEM, assertSupplierConfirmed, lockMasterWriteShared, issuedCsv, guardRegCsvOnSave } from './master-write.mjs';
import { beginRegWrite } from './master-reg-csv.mjs';

/** 仕入先の列の持ち主のキー (全部 company = Company DB が正のときだけ、ここで仕入先を作る・申告する・止める) */
export const SUPPLIER_OWNER_KEYS = Object.freeze(['suppliers.name', 'suppliers.order_method', 'suppliers.lead_time_days']);

export const SUPPLIER_STATES = Object.freeze({ ne_pending: 'NE 登録待ち (申告がまだ)', ne_confirmed: 'NE に登録した (申告済み)' });
export const RESERVED_SUPPLIER_CODES = Object.freeze(['0000', '9999']);
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const bad = (message, field, reason = 'invalid_input') => new MasterWriteError(400, reason, message, { field });
const toHalf = (s) => String(s).replace(/[０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0));

/** 新しい仕入先のコードの形 (DB を読まない)。戻り値 { ok, code (4 桁), message } */
export function validateNewSupplierCode(raw) {
  const no = (message) => ({ ok: false, code: null, message });
  const s = toHalf(typeof raw === 'string' || typeof raw === 'number' ? String(raw) : '').trim();
  if (!s) return no('仕入先のコードを入れてください');
  if (!/^\d{1,4}$/.test(s)) return no('新しい仕入先のコードは 1〜4 桁の数字です (0 で埋めて 4 桁になります。数字以外・5 桁以上は前からある仕入先だけ)');
  const code = s.padStart(4, '0');
  if (RESERVED_SUPPLIER_CODES.includes(code)) return no(`${code} は使えません (0000 は無効・9999 は NE の「設定なし」)`);
  return { ok: true, code, message: null };
}

function actorOf(v) {
  const a = String(v ?? '').trim().toLowerCase();
  if (!a || a.length > 320 || CTRL_RE.test(a)) throw bad('操作する人 (ログインのメール) が分からない', 'actor');
  return a;
}
function textOf(v, label, field, max, { required = false } = {}) {
  if (v == null || String(v).trim() === '') { if (required) throw bad(`${label}を入れてください`, field); return null; }
  const s = String(v).trim();
  if ([...s].length > max) throw bad(`${label}は ${max} 字までです`, field);
  if (CTRL_RE.test(s)) throw bad(`${label}に改行や制御文字は入れられません`, field);
  return s;
}
async function tx(db, fn) {
  await db.query('begin');
  try { const r = await fn(); await db.query('commit'); return r; } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
}
async function audit(db, actor, reason) {
  await db.query(`select set_config('core.actor_type', 'human', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
      set_config('core.request_id', '', true), set_config('core.reason', $3, true), set_config('core.run_id', '', true)`, [actor, SOURCE_SYSTEM, reason || '']);
}
/** 門 (取引の中・最初に): 段階の共有の鍵 → 段階・持ち主表・仕入先の列の持ち主 → DB でも始める (operation) → マスタの書き込みの共有の鍵 */
async function openGate(db, ownership, extraKeys = [], { operation, actor, reason = null } = {}) {
  await db.query(CUTOVER_SHARED_LOCK_SQL);
  const phase = await readCutoverPhase(db, { inTx: true });
  const loadKeys = [...SUPPLIER_OWNER_KEYS, ...extraKeys].filter((k) => ownership[k] !== 'company');
  if (!newEntryWritable(phase, ownership) || loadKeys.length) {
    const why = !phase.readable ? '段階が読めない' : phase.phase !== 'new_open' ? `段階 ${phase.phase}` : loadKeys.length ? `持ち主が load: ${loadKeys.join('・')}` : '持ち主表が段階の記録と違う';
    throw new MasterWriteError(409, 'before_cutover', `切替前です: 仕入先はまだ NE・発注アプリが正です (${why})。何も書いていません`,
      { load_keys: loadKeys, phase: phase.readable ? phase.phase : null });
  }
  await beginRegWrite(db, { operation, actor, ownership, reason });
  await lockMasterWriteShared(db);
}
async function supplierByCode(db, code, { forUpdate = false } = {}) {
  return (await db.query(`select s.supplier_id::text as id, s.code, s.name, s.active, r.state as reg_state from core.suppliers s
      left join ops.supplier_registrations r on r.supplier_id = s.supplier_id
     where s.company_id = $1 and s.code_norm = core.norm_code($2)${forUpdate ? ' for update of s' : ''}`, [COMPANY_ID, String(code ?? '')])).rows[0] ?? null;
}

/**
 * 新しい仕入先を作る (状態 ne_pending)。input = { actor, code, name, orderMethod?, leadTimeDays? }
 * 名前に運用メモ (【FAX発注】など) を混ぜない (10 §9 D = 運用メモは発注メモの欄へ)
 */
export async function createSupplier(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const actor = actorOf(input?.actor);
  const cv = validateNewSupplierCode(input?.code);
  if (!cv.ok) throw bad(cv.message, 'code');
  const name = textOf(input?.name, '仕入先名', 'name', 100, { required: true });
  if (/[【】]/.test(name)) throw bad('仕入先名に【…】の運用メモは入れません (発注メモの欄へ)', 'name');
  const orderMethod = textOf(input?.orderMethod, '発注方法', 'order_method', 40);
  let lead = null;
  if (input?.leadTimeDays != null && String(input.leadTimeDays).trim() !== '') {
    lead = Number(toHalf(String(input.leadTimeDays)).trim());
    if (!Number.isInteger(lead) || lead < 0 || lead > 365) throw bad('リードタイムは 0〜365 日の整数', 'lead_time_days');
  }
  return tx(db, async () => {
    await audit(db, actor, '新しい仕入先');
    await openGate(db, ownership, [], { operation: 'supplier_create', actor });
    await db.query(`select pg_advisory_xact_lock(hashtextextended('core.new_supplier:' || $1::text, 0))`, [cv.code]);
    const clash = (await db.query(`select code from core.suppliers where company_id = $1 and (code_norm = core.norm_code($2) or core.canonical_supplier_code(code) = $2)`, [COMPANY_ID, cv.code])).rows[0];
    if (clash) throw new MasterWriteError(409, 'supplier_code_taken', `仕入先のコード ${cv.code} はもうあります (${clash.code})`, { field: 'code' });
    const id = (await db.query(`insert into core.suppliers (company_id, code, name, order_method, lead_time_days, created_by_type, created_by_id)
       values ($1, $2, $3, $4, $5, 'human', $6) returning supplier_id::text as id`, [COMPANY_ID, cv.code, name, orderMethod, lead, actor])).rows[0].id;
    // 状態の行は関数が作る (この取引で作った仕入先の行だけ = 前からある仕入先を「新しい」にしない)
    await db.query('select ops.create_supplier_registration($1::bigint, $2)', [id, actor]);
    return { ok: true, supplier_id: id, code: cv.code, state: 'ne_pending' };
  });
}

/**
 * 「NE に登録した」と申告する (ne_pending → ne_confirmed)。input = { actor, code, neCode (NE の仕入先の画面で見たコード), note? }
 * 根拠 = NE で見たコードが Company DB のコードと同じであること + 誰・いつ
 */
export async function declareSupplierInNe(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const actor = actorOf(input?.actor);
  const neCode = textOf(input?.neCode, 'NE の仕入先の画面で見たコード', 'ne_code', 20, { required: true });
  const note = textOf(input?.note, 'メモ', 'note', 200);
  return tx(db, async () => {
    await openGate(db, ownership, [], { operation: 'supplier_declare', actor });
    const s = await supplierByCode(db, input?.code);
    if (!s) throw new MasterWriteError(404, 'not_found', `仕入先 ${input?.code} は Company DB にありません`);
    if (!s.reg_state) throw new MasterWriteError(409, 'not_new_supplier', `仕入先 ${s.code} は前からある仕入先です (申告は要りません)`);
    if (s.reg_state === 'ne_confirmed') return { ok: true, code: s.code, state: 'ne_confirmed', already: true };
    if (canonicalSupplierCode(neCode) !== canonicalSupplierCode(s.code)) throw bad(`NE で見たコード ${neCode} が Company DB のコード ${s.code} と違います (NE の画面を確かめてください)`, 'ne_code');
    // 書くのは関数 (行の鍵・状態・コードが同じかを自分で照らし直す)
    const r = (await db.query('select ops.declare_supplier_in_ne($1::bigint, $2, $3, $4) as r', [s.id, actor, neCode, note])).rows[0].r;
    return { ok: true, code: s.code, state: r.state, ...(r.already ? { already: true } : {}) };
  });
}

/**
 * 取引停止 (active = false)。input = { actor, code, reason, reassignTo? }
 * 代表の仕入先に使っている商品があれば止めない (409 supplier_in_use・商品の一覧)。reassignTo = 同じ取引で代表を付け替える先 (使える仕入先)。
 * 付け替えは画面の保存と同じ決まり (NE に取り込む CSV・新商品の NE 登録の CSV が出ている商品は 409)
 */
export async function deactivateSupplier(db, input, opts = {}) {
  const ownership = validateOwnership(opts.ownership || MASTER_OWNERSHIP);
  const actor = actorOf(input?.actor);
  const reason = textOf(input?.reason, '理由', 'reason', 200, { required: true });
  const reassign = input?.reassignTo != null && String(input.reassignTo).trim() !== '';
  return tx(db, async () => {
    await audit(db, actor, reason);
    await openGate(db, ownership, reassign ? ['supplier_skus.is_primary'] : [], { operation: 'supplier_deactivate', actor, reason });
    const s0 = await supplierByCode(db, input?.code);
    if (!s0) throw new MasterWriteError(404, 'not_found', `仕入先 ${input?.code} は Company DB にありません`);
    const usedOf = async (lock) => (await db.query(`select x.sku_id::text as sku_id, k.code, k.code_norm from core.supplier_skus x join core.skus k on k.sku_id = x.sku_id
       where x.supplier_id = $1 and x.is_primary order by k.code_norm${lock ? ' for update of x' : ''}`, [s0.id])).rows;
    // 鍵: 商品の SKU (sku_id の順) → CSV → 仕入先の行 → 仕入先ごとの商品の行。鍵の後に読み直して、増えていたら (ほかの保存が代表にした) もう一度
    const planned = reassign ? (await usedOf(false)).map((u) => u.sku_id) : [];
    for (const id of [...new Set(planned)].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0))) await db.query(SKU_LOCK_SQL, [id]);
    if (reassign) await db.query(CSV_LOCK_SQL);
    const s = await supplierByCode(db, input?.code, { forUpdate: true });
    if (!s.active) return { ok: true, code: s.code, active: false, already: true };
    const used = await usedOf(true);
    let moved = []; let superseded = [];
    if (used.length) {
      if (!reassign) {
        throw new MasterWriteError(409, 'supplier_in_use', `仕入先 ${s.code} は ${used.length} 件の商品の代表の仕入先です (${used.slice(0, 10).map((u) => u.code).join('・')}${used.length > 10 ? ' ほか' : ''})。付け替える先を選んでください`,
          { skus: used.map((u) => u.code) });
      }
      const extra = used.filter((u) => !planned.includes(u.sku_id));
      if (extra.length) throw new MasterWriteError(409, 'retry', `付け替える商品がちょうど増えました (${extra.map((u) => u.code).join('・')})。何もしていません。もう一度押してください`);
      const to = await supplierByCode(db, input.reassignTo, { forUpdate: true });
      if (!to) throw bad(`付け替える先の仕入先 ${input.reassignTo} がありません`, 'reassign_to');
      if (to.id === s.id) throw bad('付け替える先が同じ仕入先です', 'reassign_to');
      if (!to.active) throw bad(`付け替える先の仕入先 ${to.code} は取引停止です`, 'reassign_to');
      await assertSupplierConfirmed(db, to);
      // NE に送る欄 (代表の仕入先) の CSV が出ている商品は付け替えない (画面の保存と同じ)
      for (const u of used) {
        const issued = await issuedCsv(db, u.code_norm, ['primary_supplier']);
        if (issued.length) {
          throw new MasterWriteError(409, 'csv_issued', `${u.code} の代表の仕入先が入った NE に取り込む CSV (ファイル ${[...new Set(issued.map((x) => `#${x.export_id}`))].join('・')}) が出ています。何もしていません`,
            { exports: issued, sku: u.code });
        }
      }
      superseded = (await guardRegCsvOnSave(db, { skuIds: used.map((u) => u.sku_id), actor, what: '代表の仕入先' })).superseded;
      for (const u of used) {
        await db.query('update core.supplier_skus set is_primary = false where supplier_id = $1 and sku_id = $2', [s.id, u.sku_id]);
        await db.query(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) values ($1, $2, $3, true, 'human', $4)
           on conflict (supplier_id, sku_id) do update set is_primary = true`, [COMPANY_ID, to.id, u.sku_id, actor]);
      }
      moved = used.map((u) => u.code);
    }
    await db.query('update core.suppliers set active = false where supplier_id = $1', [s.id]);
    return { ok: true, code: s.code, active: false, reassigned: moved, ...(superseded.length ? { reg_csv_superseded: superseded } : {}) };
  });
}
