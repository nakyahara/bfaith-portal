/**
 * master-supplier.mjs — 仕入先の新しいコード・「NE に登録した」の申告・取引停止 (Company DB構想 14 §9 v2 M1・M5 / §10 契約 v3 Medium 3 / 10 §3.2。migration 0053)
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
 * 🚨 書くのは DB の関数だけ (0053・security definer・#1571 R1 High 3): ops.create_supplier / declare_supplier_in_ne / deactivate_supplier。
 *    どれも「仕入先の約束」(supplier_create / supplier_declare / supplier_deactivate = 0051 の ops.master_write_sessions) を鍵の後に書き、
 *    相手 (仕入先・付け替える SKU) を DB が決め、保存の記録 done を書く。lib は形を確かめ、門 (段階・持ち主表・仕入先の列の持ち主) と
 *    マスタの書き込みの共有の鍵 (夜間ロードが持っていれば短く待って 409 nightly_load) を取って関数を呼ぶ。
 *    仕入先の行 (core.suppliers・supplier_skus) を書くロール = 発注アプリの書き込み先を差し替える ⑥ で決める (今は画面のロール master_edit に関数を渡さない)
 * 付け替え = 代表の仕入先を変える = NE に送る欄: NE に取り込む CSV (0040) の primary_supplier が出ている商品 = 409 csv_issued /
 *   新商品の NE 登録の CSV (0053) を配った後の商品 = 409 reg_csv_issued (作っただけのファイルは使わないにする)。画面の保存と同じ決まり (関数が見る)
 */
import { baseGateInTx, lockCutoverSharedInTx } from './master-owner-gate.mjs';
import { MasterWriteError, ownerGateError, SOURCE_SYSTEM, lockMasterWriteShared } from './master-write.mjs';
import { opRequestId } from './master-reg-csv.mjs';

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
/**
 * 門 (取引の中・最初に): 段階の共有の鍵 → 段階・持ち主表・仕入先の列の持ち主 → マスタの書き込みの共有の鍵 (約束は DB の関数が鍵の後に書く)。
 * 🆕 広げる道 PR-2: 持ち主表 = DB の active (土台の門 lib/master-owner-gate.mjs・読めない = 503・code_behind = 409)。戻り値 = その持ち主表 (DB の関数に渡す)。
 *   仕入先は今までどおり MASTER_EDIT_OPEN を見ない (open: true で土台の門を通す)
 */
async function openGate(db, extraKeys = []) {
  await lockCutoverSharedInTx(db);   // 55P03 は 1 回だけ取り直す (設計 M8)
  const gate = await baseGateInTx(db, { open: true });
  const phase = gate.phase;
  const refuse = (why, loadKeys = []) => new MasterWriteError(409, 'before_cutover', `切替前です: 仕入先はまだ NE・発注アプリが正です (${why})。何も書いていません`,
    { load_keys: loadKeys, phase: phase.readable ? phase.phase : null });
  if (!gate.ok) throw ownerGateError(gate, (why) => refuse(!phase.readable ? '段階が読めない' : phase.phase !== 'new_open' ? `段階 ${phase.phase}` : '持ち主表が段階の記録と違う'));
  const loadKeys = [...SUPPLIER_OWNER_KEYS, ...extraKeys].filter((k) => gate.ownership[k] !== 'company');
  if (loadKeys.length) throw refuse(`持ち主が load: ${loadKeys.join('・')}`, loadKeys);
  await lockMasterWriteShared(db);
  return gate.ownership;
}
/** DB の関数の誤り (「理由: 文」) → MasterWriteError (画面の言葉は関数の文のまま) */
const FN_STATUS = Object.freeze({ not_found: 404, invalid_input: 400, ne_code_mismatch: 400, supplier_not_confirmed: 400 });
async function callFn(db, sql, params, { field = null } = {}) {
  try {
    return (await db.query(sql, params)).rows[0].r;
  } catch (e) {
    const m = /^([a-z_]+): ([\s\S]*)$/.exec(String((e && e.message) || ''));
    if (m && e && ['P0001', 'P0002', '22023', '23505', '42501'].includes(e.code)) {
      const extra = { ...(field ? { field } : {}) };
      if (m[1] === 'supplier_in_use') { const mm = /\(([^()]*)\)$/.exec(m[2]); if (mm) extra.skus = mm[1].split('・'); }
      throw new MasterWriteError(FN_STATUS[m[1]] ?? 409, m[1], m[2], extra);
    }
    throw e;
  }
}

/**
 * 新しい仕入先を作る (状態 ne_pending)。input = { actor, code, name, orderMethod?, leadTimeDays?, requestId? }
 * 名前に運用メモ (【FAX発注】など) を混ぜない (10 §9 D = 運用メモは発注メモの欄へ)。書くのは DB の関数 ops.create_supplier (約束 supplier_create)
 */
export async function createSupplier(db, input, opts = {}) {
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
    const ownership = await openGate(db);
    const r = await callFn(db, 'select ops.create_supplier($1::uuid, $2, $3, $4::jsonb, $5, $6, $7, $8::integer) as r',
      [opRequestId(input?.requestId), actor, '新しい仕入先', JSON.stringify(ownership), cv.code, name, orderMethod, lead], { field: 'code' });
    return { ok: true, supplier_id: r.supplier_id, code: r.code, state: r.state };
  });
}

/**
 * 「NE に登録した」と申告する (ne_pending → ne_confirmed)。input = { actor, code, neCode (NE の仕入先の画面で見たコード), note?, requestId? }
 * 根拠 = NE で見たコードが Company DB のコードと同じであること + 誰・いつ。書くのは DB の関数 ops.declare_supplier_in_ne (約束 supplier_declare)
 */
export async function declareSupplierInNe(db, input, opts = {}) {
  const actor = actorOf(input?.actor);
  const neCode = textOf(input?.neCode, 'NE の仕入先の画面で見たコード', 'ne_code', 20, { required: true });
  const note = textOf(input?.note, 'メモ', 'note', 200);
  return tx(db, async () => {
    const ownership = await openGate(db);
    const r = await callFn(db, 'select ops.declare_supplier_in_ne($1::uuid, $2, $3::jsonb, $4, $5, $6) as r',
      [opRequestId(input?.requestId), actor, JSON.stringify(ownership), String(input?.code ?? ''), neCode, note], { field: 'ne_code' });
    return { ok: true, code: r.code, state: r.state, ...(r.already ? { already: true } : {}) };
  });
}

/**
 * 取引停止 (active = false)。input = { actor, code, reason, reassignTo?, requestId? }
 * 代表の仕入先に使っている商品があれば止めない (409 supplier_in_use・商品の一覧)。reassignTo = 同じ取引で代表を付け替える先 (使える仕入先)。
 * 付け替えは画面の保存と同じ決まり (NE に取り込む CSV・新商品の NE 登録の CSV が出ている商品は 409)。書くのは DB の関数 ops.deactivate_supplier (約束 supplier_deactivate)
 */
export async function deactivateSupplier(db, input, opts = {}) {
  const actor = actorOf(input?.actor);
  const reason = textOf(input?.reason, '理由', 'reason', 200, { required: true });
  const reassign = input?.reassignTo != null && String(input.reassignTo).trim() !== '';
  return tx(db, async () => {
    await audit(db, actor, reason);
    const ownership = await openGate(db, reassign ? ['supplier_skus.is_primary'] : []);
    const r = await callFn(db, 'select ops.deactivate_supplier($1::uuid, $2, $3, $4::jsonb, $5, $6) as r',
      [opRequestId(input?.requestId), actor, reason, JSON.stringify(ownership), String(input?.code ?? ''), reassign ? String(input.reassignTo) : null], { field: reassign ? 'reassign_to' : null });
    if (r.already) return { ok: true, code: r.code, active: false, already: true };
    return { ok: true, code: r.code, active: false, reassigned: r.reassigned || [], ...((r.reg_csv_superseded || []).length ? { reg_csv_superseded: r.reg_csv_superseded } : {}) };
  });
}
