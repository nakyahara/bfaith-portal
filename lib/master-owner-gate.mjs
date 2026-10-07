/**
 * master-owner-gate.mjs — 新しい画面の書き込みの門を「配ったコードの持ち主表」ではなく **DB の active の持ち主の記録** に従わせる
 *   (広げる道 PR-2・設計 newentry_widen v11 §2 G6〜G8・G15・G16・§7.1・Codex R1 H4 / M8・R2 M4 二層の門・M5)
 *
 * 持ち主の正 = DB の epoch 1 つ (ops.master_ownership_state の active)。書く取引で、段階の共有の鍵の後に DB の active を読み、それを持ち主表として使う
 *   (DB の関数 = ops.begin_master_write・ops.register_new_sku・ops.reg_write_gate・ops.amazon_map_begin も、渡した持ち主表を段階の記録と active に照らす = 同じものを渡す)。
 *   config/master-ownership.mjs (configured) は「次の予定」だけ = 画面の保存はもう読まない。
 *
 * 二層の門 (Codex R2 M4):
 *   1 層目 = 土台の門 (baseGateInTx): 段階 new_open・MASTER_EDIT_OPEN・DB の active を読める・このコードの能力 (code_behind でない)・段階の記録 = active。
 *            全部の書き込み (保存・新商品・NE 登録の CSV の全部の操作・Amazon・仕入先) が通る。認証 (名簿) は router
 *   2 層目 = 新規開始の門 (newEntryGateInTx): 新商品の作成・NE 登録の CSV の build・まだ配っていない CSV の issue だけ。
 *            DB の「開放の許可 (lease)」が今有効 (ops.new_entry_lease_valid(kind)・PR-1 の 0058) かつ 非常の止め (env MASTER_NEW_ENTRY_STOP=1) が入っていない。
 *            許可は毎朝の照合 ② (始めに閉じる → 結果) の次の段が出し、期限は翌朝 07:00 (JST・daily-sync の始まり)。本番の許可は single だけ = セットの新規開始は閉じたまま
 *            後始末 (declare・supersede・partial / failed / verified の記録) はここを通さない (設計 v11 §3.7・§7.1)
 *
 * 🚨 読めない = 閉じる (fail-closed):
 *   - active を読めない (関数が無い・実行権が無い (PR-1 の SECURITY DEFINER + grant の前)・壊れた値) = 503 owner_unreadable (既存の門の 503 と同じ形)
 *   - DB の active に、このコードが扱えない company のキー (config/master-capability.mjs の COMPANY_CAPABLE の外) = 409 code_behind (古い build は意図的に止まる・設計 G8・R1 M8)
 *   - 許可の関数が無い (0058 の前) = 409 new_entry_closed / 呼べない = 503 new_entry_lease_unreadable
 * 🚨 段階の鍵 (共有) の 55P03 (widen が排他で持っている間に lock_timeout) は 1 回だけ取り直す (savepoint で・取引の中の前の鍵はそのまま。設計 M8)
 */
import { readCutoverPhase, newEntryWritable, ownershipHash, CUTOVER_SHARED_LOCK_SQL } from './master-cutover.mjs';
import { OWNED_COLUMNS, OWNERS } from '../config/master-ownership.mjs';
import { COMPANY_CAPABLE, codeBehindKeys } from '../config/master-capability.mjs';

/** DB の active の持ち主表を読む関数 (0055・PR-1 の 0058 で SECURITY DEFINER + 画面のロールに実行権) */
export const ACTIVE_MAP_FN = 'ops.master_ownership_active_map()';
export const ACTIVE_MAP_SQL = 'select ops.master_ownership_active_map() as m';
/** 新商品の開放の許可 (lease) が今有効か (PR-1 の 0058・設計 v11 §3.7)。画面のロールに実行権 */
export const NEW_ENTRY_LEASE_FN = 'ops.new_entry_lease_valid(text)';
export const NEW_ENTRY_LEASE_SQL = 'select ops.new_entry_lease_valid($1::text) as ok';
/**
 * 新規開始の鍵を取って許可を返す (PR-1 の 0058・master_edit に EXECUTE・確定の形 10/6)。
 * DB の中で許可の鍵 (共有) を取り、今の許可 (boolean) を返す = 鍵の数式を JS に複製しない (版の鍵は無くなった・newentry_min_plan)。
 * 🚨 鍵の順 (0058・全部の書き手で同じ = deadlock しない): request → 許可 → 段階 → マスタの書き込み → SKU → CSV → NE のコード
 *    = 新規開始の取引は、段階の鍵より **前** にこれを呼ぶ (acquireNewEntryLocksInTx)。後の門 (newEntryGateInTx) はその答えを使うだけ
 */
export const ACQUIRE_NEW_ENTRY_LOCKS_FN = 'ops.acquire_new_entry_locks(text)';
export const ACQUIRE_NEW_ENTRY_LOCKS_SQL = 'select ops.acquire_new_entry_locks($1::text) as ok';
/** 0058 が入っているか (試みの表か門の記録の 2 版がある)。0058 の印があるのに関数が無い = 作り忘れ・消えた = fail-closed にする */
export const HAS_0058_SQL = `select to_regclass('ops.master_widen_attempts') is not null
  or to_regprocedure('ops.record_legacy_gate_ack_v2(text,text,text,jsonb,text,text,integer,timestamptz,text,text,text[],boolean,text)') is not null as ok`;
/** 非常の止め (入っていれば許可があっても新規開始を閉じる)。要求ごとに読む = 再起動なしで止められる */
export const NEW_ENTRY_STOP_ENV = 'MASTER_NEW_ENTRY_STOP';
export const newEntryStopped = (env = process.env) => String(env[NEW_ENTRY_STOP_ENV] ?? '').trim() === '1';
/** 新規開始の門の種類 (許可は種類ごと) */
export const NEW_ENTRY_KINDS = Object.freeze(['single', 'set']);

export const OWNER_UNREADABLE_WRITE_MESSAGE = '列ごとの持ち主 (Company DB の切替の記録) を読めないので、保存を止めています。何も保存していません。少し待ってもう一度。続くときは管理者へ';
export const codeBehindMessage = (keys) => `このサーバーのプログラムが古いので保存を止めています (Company DB の持ち主 ${keys.join('・')} をこのプログラムは扱えません)。何も保存していません。配り直しを管理者へ`;
export const NEW_ENTRY_CLOSED_WHY = Object.freeze({
  stopped: `新商品の登録は止めています (非常の止め ${NEW_ENTRY_STOP_ENV}=1)`,
  no_function: '新商品の開放の許可の仕組みがまだ Company DB にありません (0058 の前)',
  no_lease: '新商品の開放の許可がありません (毎朝の確かめ (照合の区分の差 0) が通っていない・期限切れ (翌朝 7:00)・取り消し)',
});

const ALL_LOAD = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
/** 試験だけ: このコードの能力を差し替える (本番は config/master-capability.mjs の COMPANY_CAPABLE のまま。試験の DB は全部の列を company にする) */
let capableOverride = null;
export function __setCapableForTest(list) { capableOverride = list ? Object.freeze([...list]) : null; }
export const capableNow = () => capableOverride || COMPANY_CAPABLE;
const isObj = (v) => !!v && typeof v === 'object' && !Array.isArray(v);

/**
 * DB の active の持ち主表を持ち主表の形にする (記録に無い列 = load を足す・ハッシュは 1 つの式)。
 * 知らないキー (このコードの OWNED_COLUMNS に無い) は load なら捨てる (ハッシュは load を数えない = 同じ)・company なら code_behind に残す。
 * @returns {{ readable: true, map, hash, code_behind: string[] } | { readable: false, error }}
 */
export function activeOwnershipOf(raw, capable = capableNow()) {
  if (!isObj(raw)) return { readable: false, error: '持ち主の記録の形が違う (object でない)' };
  const bad = Object.entries(raw).filter(([, v]) => !OWNERS.includes(v)).map(([k]) => k);
  if (bad.length) return { readable: false, error: `持ち主の記録の値が違う (${bad.join('・')})` };
  const code_behind = codeBehindKeys(raw, capable);
  const known = new Set(OWNED_COLUMNS);
  const map = { ...ALL_LOAD };
  for (const [k, v] of Object.entries(raw)) if (known.has(k)) map[k] = v;
  const sorted = Object.fromEntries(Object.keys(map).sort().map((k) => [k, map[k]]));
  // ハッシュは記録のまま (知らない company のキーも数える) = 段階の記録と比べる値。知らないキーがあれば code_behind で閉じる
  return { readable: true, map: Object.freeze(sorted), hash: ownershipHash(raw), code_behind };
}

/**
 * DB の active を読む。🚨 取引の中で読めない (誤り) = その取引はもう使えない = 呼び手はすぐ断る (投げる)。
 * savepoint = true は取引の中で「読めなくても取引を続けたい」とき (画面の読む取引)
 */
export async function readActiveOwnership(db, { capable = capableNow(), savepoint = false } = {}) {
  if (savepoint) await db.query('savepoint master_owner_read');
  try {
    const r = (await db.query(ACTIVE_MAP_SQL)).rows[0];
    if (savepoint) await db.query('release savepoint master_owner_read');
    return activeOwnershipOf(r ? r.m : null, capable);
  } catch (e) {
    if (savepoint) { try { await db.query('rollback to savepoint master_owner_read'); await db.query('release savepoint master_owner_read'); } catch { /* */ } }
    return { readable: false, error: `持ち主を読めない: ${String((e && e.message) || e).slice(0, 300)}`, pg_code: e && e.code ? String(e.code) : null };
  }
}

/**
 * 段階の鍵を共有で取る (取引の中・保存の流れの最初の方)。widen (PR-1) が排他で持っていて lock_timeout (55P03) になったら 1 回だけ取り直す。
 * savepoint で取る = 失敗しても取引の前の鍵 (request_id の鍵など) はそのまま。2 回目も 55P03 = そのまま投げる (呼び手の 409 locked)
 */
export async function lockCutoverSharedInTx(db, { onRetry = null } = {}) {
  for (let attempt = 1; ; attempt++) {
    await db.query('savepoint master_cutover_lock');
    try {
      await db.query(CUTOVER_SHARED_LOCK_SQL);
      await db.query('release savepoint master_cutover_lock');
      return { attempts: attempt };
    } catch (e) {
      try { await db.query('rollback to savepoint master_cutover_lock'); await db.query('release savepoint master_cutover_lock'); } catch { /* 接続が切れた = 元の誤りを上げる */ }
      if (e && e.code === '55P03' && attempt < 2) { if (onRetry) await onRetry(e); continue; }
      throw e;
    }
  }
}

/**
 * 土台の門 (1 層目)。段階の共有の鍵を取った後 (取引の中) に呼ぶ。
 * 順: 段階を読めない / new_open でない (409 before_cutover) → active を読めない (503 owner_unreadable) → code_behind (409) →
 *     段階の記録 ≠ active (409 before_cutover) → MASTER_EDIT_OPEN なし (409 before_cutover)
 * @returns {{ ok: true, phase, ownership, hash } | { ok: false, status, reason, why, phase, code_behind?, error? }}
 */
export async function baseGateInTx(db, { open, capable = capableNow(), closedWhy = 'この画面の保存はまだ開いていません' } = {}) {
  const phase = await readCutoverPhase(db, { inTx: true });
  const no = (status, reason, why, extra = {}) => ({ ok: false, status, reason, why, phase, ...extra });
  if (!phase.readable) return no(409, 'before_cutover', '切替の段階が読めない');
  if (phase.phase !== 'new_open') return no(409, 'before_cutover', `切替の段階が ${phase.phase}`);
  const a = await readActiveOwnership(db, { capable });
  if (!a.readable) return no(503, 'owner_unreadable', OWNER_UNREADABLE_WRITE_MESSAGE, { error: a.error });
  if (a.code_behind.length) return no(409, 'code_behind', codeBehindMessage(a.code_behind), { code_behind: a.code_behind });
  if (!newEntryWritable(phase, a.map)) return no(409, 'before_cutover', '持ち主表が切替のときの記録と違う', { ownership: a.map });
  if (open !== true) return no(409, 'before_cutover', closedWhy, { ownership: a.map });
  return { ok: true, phase, ownership: a.map, hash: a.hash };
}

/** 種類の名前をそろえる (NE 登録の CSV の products / sets → single / set) */
export const entryKindOf = (kind) => (kind === 'sets' || kind === 'set' ? 'set' : kind === 'products' || kind === 'single' ? 'single' : null);

/**
 * 新規開始の鍵を取る (request の鍵の後・**段階の鍵より前**・同じ取引)。関数が無い (0058 の前) = 鍵は取らない。
 * 戻り値 { kind, hasFn, valid } / 呼べない = { kind, hasFn: true, error, refusal } (取引はもう使えない = 呼び手は refusal をすぐ投げる)
 */
export async function acquireNewEntryLocksInTx(db, kind) {
  const k = entryKindOf(kind);
  if (!k) return { kind: k, hasFn: false };
  const hasFn = (await db.query('select to_regprocedure($1) is not null as ok', [ACQUIRE_NEW_ENTRY_LOCKS_FN])).rows[0]?.ok === true;
  if (!hasFn) return { kind: k, hasFn: false };
  try {
    const v = (await db.query(ACQUIRE_NEW_ENTRY_LOCKS_SQL, [k])).rows[0]?.ok;
    return { kind: k, hasFn: true, valid: v === true };
  } catch (e) {
    const error = String((e && e.message) || e).slice(0, 300);
    return { kind: k, hasFn: true, error, refusal: { ok: false, status: 503, reason: 'new_entry_lease_unreadable', cause: 'unreadable', kind: k, error,
      why: '新商品の開放の許可を読めないので止めています。少し待ってもう一度。続くときは管理者へ' } };
  }
}
/**
 * 新規開始の門 (2 層目)。土台の門を通った後 (同じ取引) に呼ぶ。新商品の作成・CSV の build・未配布の CSV の issue だけ。
 * 判定は先に取った鍵の答え (pre = acquireNewEntryLocksInTx) だけを使う (ここでは DB を読まない・鍵も取らない = 段階の鍵の後に許可の鍵を取らない)。
 * 非常の止め → 関数が無い (0058 の前) → 読めない (503) → 許可が無い・期限切れ・取り消し = 409 new_entry_closed
 * @returns {{ ok: true } | { ok: false, status, reason, why, cause, error? }}
 */
export function newEntryGateInTx(db, kind, { env = process.env, pre = null } = {}) {
  const k = entryKindOf(kind);
  const no = (status, reason, cause, why, extra = {}) => ({ ok: false, status, reason, cause, why, kind: k, ...extra });
  if (!k) return no(400, 'invalid_input', 'kind', `知らない種類: ${kind}`);
  if (newEntryStopped(env)) return no(409, 'new_entry_closed', 'stopped', NEW_ENTRY_CLOSED_WHY.stopped);
  if (!pre || pre.kind !== k || !pre.hasFn) return no(409, 'new_entry_closed', 'no_function', NEW_ENTRY_CLOSED_WHY.no_function);
  if (pre.error) return pre.refusal;
  if (pre.valid !== true) return no(409, 'new_entry_closed', 'no_lease', NEW_ENTRY_CLOSED_WHY.no_lease);
  return { ok: true, kind: k };
}

/**
 * 画面 (見せ方だけ) の持ち主: 取引の外で読む (読めなくても画面は出す = 閉じた側で見せる)。保存は必ず取引の中で読み直す。
 * @returns {{ readable, map, hash, code_behind, error }}  readable = false のとき map = 全部 load (欄を閉じる側)
 */
export async function readScreenOwnership(db, { capable = capableNow(), savepoint = false } = {}) {
  if (!db) return { readable: false, map: ALL_LOAD, hash: null, code_behind: [], error: 'Company DB につながらない' };
  const a = await readActiveOwnership(db, { capable, savepoint });
  return a.readable ? a : { ...a, map: ALL_LOAD, hash: null, code_behind: [] };
}
/** 画面: この種類の新規開始が開いているか (見せ方だけ・取引の外)。{ open, cause, why } */
export async function readScreenNewEntry(db, kind, { env = process.env } = {}) {
  if (!db) return { open: false, cause: 'no_db', why: 'Company DB につながらない' };
  const k = entryKindOf(kind);
  if (newEntryStopped(env)) return { open: false, cause: 'stopped', why: NEW_ENTRY_CLOSED_WHY.stopped };
  try {
    const has = (await db.query('select to_regprocedure($1) is not null as ok', [NEW_ENTRY_LEASE_FN])).rows[0]?.ok === true;
    if (!has) return { open: false, cause: 'no_function', why: NEW_ENTRY_CLOSED_WHY.no_function };
    const ok = (await db.query(NEW_ENTRY_LEASE_SQL, [k])).rows[0]?.ok === true;
    return ok ? { open: true, cause: null, why: '' } : { open: false, cause: 'no_lease', why: NEW_ENTRY_CLOSED_WHY.no_lease };
  } catch (e) {
    return { open: false, cause: 'unreadable', why: `新商品の開放の許可を読めない: ${String((e && e.message) || e).slice(0, 200)}` };
  }
}

/** 持ち主の「画面の答え」から、その段階で保存を開けるか (段階の記録 = active・code_behind でない)。見せ方だけ */
export function screenOwnerWritable(phase, owner) {
  return !!owner && owner.readable === true && owner.code_behind.length === 0 && newEntryWritable(phase, owner.map);
}
