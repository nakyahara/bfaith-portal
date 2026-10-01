/**
 * ownership-state.mjs — 持ち主の設定の epoch (0053 の ops.master_ownership_state。マスタ正本切替 ④a・Codex #1564 R1 H1)
 *
 *   configured = config/master-ownership.mjs (コードに書いた「こうしたい」。これだけでは何も変わらない)
 *   prepared   = 人が prepare で記録した「次にこれにする」。明示して頼んだロード (usePrepared) と、そのロードの後の写しの世代だけが使う
 *   active     = 今使っている持ち主。夜間ロード (engine.mjs)・miniPC の写し (publish/fetch.mjs) はこれ。
 *                prepared の世代で miniPC の作り直し + 入れた後の確かめが通ったときだけ activate で prepared → active
 *   行が無い (0053 の前・まだ誰も prepare していない) = active は全部 'load' (今までと同じ)
 * 🚨 読む (resolve) は夜間ロード・写しの両方。書く (prepare / activate / cancel) は scripts/company-db/master-ownership-epoch.mjs だけ (DB を作ったユーザー)
 * 🚨 このファイルは engine.mjs が読む = master-publish.js (warehouse) を import しない (循環を作らない)
 */
import crypto from 'node:crypto';
import { OWNED_COLUMNS, validateOwnership } from '../../../config/master-ownership.mjs';

/** 全部 'load' (行が無いときの active) */
export const ALL_LOAD = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
/** 持ち主の設定のハッシュ (engine.mjs が ops.load_materials に残すのと同じ式 = epoch) */
export const ownershipHashOf = (o) => crypto.createHash('sha256').update(JSON.stringify(Object.keys(o || {}).sort().map((k) => [k, o[k]]))).digest('hex');
export const sortedOwnership = (o) => Object.fromEntries(Object.keys(o || {}).sort().map((k) => [k, o[k]]));

const rowsOf = async (db, sql, params) => (await db.query(sql, params)).rows;
/** 表があるか (0053 の前 = 無い) */
async function hasStateTable(db) {
  return (await rowsOf(db, `select to_regclass('ops.master_ownership_state') is not null as ok`))[0].ok === true;
}
/**
 * 記録した持ち主を読む。壊れている (ハッシュが中身と違う・知らない列・知らない値) = 使わない (投げる)。
 * 記録に無い列 (記録の後の PR で OWNED_COLUMNS に足した列) = 'load' として足す (夜間ロードは今までどおり。#1564 の見直し M-4)。
 * @returns {{ map, hash, stored_hash, filled }}  hash = 足した後の持ち主のハッシュ (夜間ロード・写し・作り直しが記録するのと同じ = 実際に効く持ち主)・
 *   stored_hash = 記録したハッシュ・filled = 'load' として足した列
 */
export function checkedMap(map, hash, what) {
  const broken = (msg) => Object.assign(new Error(`${what} の持ち主${msg}`), { code: 'OWNERSHIP_STATE_BROKEN' });
  if (!map || typeof map !== 'object' || Array.isArray(map)) throw broken('が読めない');
  if (ownershipHashOf(map) !== hash) throw broken('のハッシュが中身と違う');
  const filled = OWNED_COLUMNS.filter((k) => !Object.hasOwn(map, k));
  let m;
  try { m = validateOwnership({ ...Object.fromEntries(filled.map((k) => [k, 'load'])), ...map }); } catch (e) { throw broken(`が不正: ${e.message}`); }
  return { map: sortedOwnership(m), hash: ownershipHashOf(m), stored_hash: hash, filled };
}

/**
 * 今の状態を読む。壊れていれば投げる (推測で持ち主を決めない)
 * @returns {{ state: 'ok'|'missing'|'no_table', active: { map, hash, stored_hash, filled, activated_at, activated_by }, prepared: { map, hash, stored_hash, filled, prepared_at, prepared_by }|null }}
 */
export async function readOwnershipState(db) {
  const def = { map: sortedOwnership(ALL_LOAD), hash: ownershipHashOf(ALL_LOAD), stored_hash: null, filled: [], activated_at: null, activated_by: null };
  if (!(await hasStateTable(db))) return { state: 'no_table', active: def, prepared: null };
  // 時刻は UTC の ISO (マイクロ秒まで) = 写しの世代の cdb_read_at と文字のまま比べられる (activate の「prepare の後に読んだ世代」)
  const iso = (c) => `to_char(${c} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
  const r = (await rowsOf(db, `select active_hash, active_map, ${iso('activated_at')} as activated_at, activated_by, prepared_hash, prepared_map, ${iso('prepared_at')} as prepared_at, prepared_by
    from ops.master_ownership_state where id = 1`))[0];
  if (!r) return { state: 'missing', active: def, prepared: null };
  return {
    state: 'ok',
    active: { ...checkedMap(r.active_map, r.active_hash, 'active'), activated_at: r.activated_at, activated_by: r.activated_by },
    prepared: r.prepared_hash ? { ...checkedMap(r.prepared_map, r.prepared_hash, 'prepared'), prepared_at: r.prepared_at, prepared_by: r.prepared_by } : null,
  };
}

/**
 * 夜間ロードが使う持ち主。既定 = active (行が無い = 全部 load)。usePrepared = 切替の日に明示して頼んだロードだけ (prepared が無ければ投げる)
 * @returns {{ ownership: object, epoch: 'active'|'prepared'|'default', hash: string, state: string }}
 */
export async function resolveLoadOwnership(db, { usePrepared = false } = {}) {
  const st = await readOwnershipState(db);
  if (usePrepared) {
    if (!st.prepared) throw Object.assign(new Error('prepared の持ち主が無い (先に master-ownership-epoch.mjs prepare)'), { code: 'NO_PREPARED_OWNERSHIP' });
    return { ownership: st.prepared.map, epoch: 'prepared', hash: st.prepared.hash, state: st.state };
  }
  return { ownership: st.active.map, epoch: st.state === 'ok' ? 'active' : 'default', hash: st.active.hash, state: st.state };
}

/**
 * 1 行を用意する (無ければ active = 全部 load で作る) → 行の鍵を取る。取引の中で呼ぶ。
 * 2 人が同時に最初の prepare をしても、後の人は前の人の commit を待ってから何もしない (on conflict) = 重複で落ちない・init は 1 回だけ
 */
async function ensureRow(db, actor) {
  const h = ownershipHashOf(ALL_LOAD);
  const made = (await rowsOf(db, `insert into ops.master_ownership_state (id, active_hash, active_map, activated_by) values (1, $1, $2::jsonb, $3)
    on conflict (id) do nothing returning id`, [h, JSON.stringify(sortedOwnership(ALL_LOAD)), actor])).length > 0;
  if (made) await db.query(`insert into ops.master_ownership_events (action, ownership_hash, ownership, actor) values ('init', $1, $2::jsonb, $3)`, [h, JSON.stringify(sortedOwnership(ALL_LOAD)), actor]);
  await db.query('select 1 from ops.master_ownership_state where id = 1 for update');
}
const inTx = async (db, fn) => {
  await db.query('begin');
  try { const r = await fn(); await db.query('commit'); return r; } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
};

/** prepare: 「次にこれにする」を記録する (active と同じなら何もしない = 投げる)。ほかの確かめ (一緒に切り替える組など) は呼び手 */
export async function prepareOwnership(db, { map, actor }) {
  const m = sortedOwnership(validateOwnership(map));
  const h = ownershipHashOf(m);
  return inTx(db, async () => {
    await ensureRow(db, actor);
    const cur = (await rowsOf(db, 'select active_hash, active_map from ops.master_ownership_state where id = 1'))[0];
    // 実際に効く持ち主 (記録に無い列 = load を足した後) で比べる
    if (checkedMap(cur.active_map, cur.active_hash, 'active').hash === h) throw Object.assign(new Error('今の active と同じ持ち主 (用意するものが無い)'), { code: 'SAME_AS_ACTIVE' });
    await db.query(`update ops.master_ownership_state set prepared_hash = $1, prepared_map = $2::jsonb, prepared_at = now(), prepared_by = $3, updated_at = now() where id = 1`, [h, JSON.stringify(m), actor]);
    await db.query(`insert into ops.master_ownership_events (action, ownership_hash, ownership, actor) values ('prepare', $1, $2::jsonb, $3)`, [h, JSON.stringify(m), actor]);
    return { prepared_hash: h };
  });
}

/** 切替の段階の表 (⑤-1 の 0051 の ops.master_cutover_state。lib/master-cutover.mjs) の名前と、activate してよい段階 */
export const CUTOVER_TABLE = 'ops.master_cutover_state';
export const ACTIVATE_PHASE = 'frozen';
/**
 * 切替の段階を読む (activate の取引の中)。段階を変える取引 (⑤-1 の ops.set_master_cutover_phase = 排他の鍵) と並ぶように共有の鍵を持つ。
 * 表が無い = null (⑤-1 の前)
 */
async function readCutoverPhaseInTx(db) {
  const has = (await rowsOf(db, `select to_regclass('${CUTOVER_TABLE}') is not null as ok`))[0].ok === true;
  if (!has) return null;
  await db.query(`select pg_advisory_xact_lock_shared(hashtext('ops.master_cutover'))`);
  const r = (await rowsOf(db, `select phase from ${CUTOVER_TABLE} where id = 1`))[0];
  return r ? r.phase : null;
}

/**
 * activate: prepared → active (expectHash = 確かめた世代の持ち主のハッシュ・expectPreparedAt = 証拠を集めたときの prepare の時刻。どちらかが違えば投げる)。
 *   確かめの証拠は呼び手が集める。🚨 同じ持ち主でも prepare をやり直した (時刻が変わった) = 前の証拠では active にしない (#1564 Codex R2 Medium 4)。
 *   比べるのは行の鍵を取った後 (証拠を集めた後・activate の前に別の人が prepare し直しても通さない)
 * 🚨 切替の段階が frozen (古い入口を止めた後・持ち主を C にする前) のときだけ (#1564 の見直し M-1)。
 *   段階の表が無い (⑤-1 の前) = 断る / legacy_open (古い入口がまだ正) = 断る / company_owner・new_open (もう切り替えた後) = 断る。
 *   持ち主の正を 1 つにする残り (⑤-1 の company_owner に進む条件 = active_hash と owner_hash が同じ・新しい画面が DB の active を読む) は ⑤ / ⑥ で結ぶ
 */
export async function activateOwnership(db, { expectHash, expectPreparedAt, actor, evidence }) {
  if (!expectPreparedAt) throw Object.assign(new Error('証拠を集めたときの prepare の時刻 (expectPreparedAt) が要る'), { code: 'PREPARED_AT_REQUIRED' });
  return inTx(db, async () => {
    const phase = await readCutoverPhaseInTx(db);
    if (phase == null) throw Object.assign(new Error(`切替の段階の表 (${CUTOVER_TABLE}) が無い・行が無い = active にしない (⑤-1 の migration の後・段階 ${ACTIVATE_PHASE} で)`), { code: 'CUTOVER_STATE_MISSING' });
    if (phase !== ACTIVATE_PHASE) throw Object.assign(new Error(`切替の段階が ${phase} = active にしない (${ACTIVATE_PHASE} = 古い入口を止めた後だけ)`), { code: 'CUTOVER_PHASE_NOT_FROZEN', phase });
    const cur = (await rowsOf(db, `select prepared_hash, prepared_map, to_char(prepared_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as prepared_at
      from ops.master_ownership_state where id = 1 for update`))[0];
    if (!cur || !cur.prepared_hash) throw Object.assign(new Error('prepared の持ち主が無い'), { code: 'NO_PREPARED_OWNERSHIP' });
    if (cur.prepared_at !== expectPreparedAt) {
      throw Object.assign(new Error(`証拠を集めた後に prepare がやり直された (${expectPreparedAt} → ${cur.prepared_at}) = この証拠では active にしない (写し・作り直し・確かめからやり直す)`), { code: 'PREPARED_CHANGED' });
    }
    // 確かめた世代の持ち主 (実際に効く持ち主のハッシュ = 記録に無い列は load を足した後) と比べる
    const eff = checkedMap(cur.prepared_map, cur.prepared_hash, 'prepared');
    if (eff.hash !== expectHash) throw Object.assign(new Error(`確かめた世代の持ち主 (${String(expectHash).slice(0, 12)}) が prepared (${eff.hash.slice(0, 12)}) と違う`), { code: 'PREPARED_MISMATCH' });
    await db.query(`update ops.master_ownership_state set active_hash = prepared_hash, active_map = prepared_map, activated_at = now(), activated_by = $1, activated_evidence = $2::jsonb,
      prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null, updated_at = now() where id = 1`, [actor, JSON.stringify(evidence ?? null)]);
    await db.query(`insert into ops.master_ownership_events (action, ownership_hash, ownership, actor, evidence) values ('activate', $1, $2, $3, $4::jsonb)`,
      [cur.prepared_hash, cur.prepared_map, actor, JSON.stringify(evidence ?? null)]);
    return { active_hash: eff.hash, stored_hash: cur.prepared_hash };
  });
}

/** cancel: prepared を取り消す (active は変えない) */
export async function cancelPrepared(db, { actor }) {
  return inTx(db, async () => {
    const cur = (await rowsOf(db, 'select prepared_hash, prepared_map from ops.master_ownership_state where id = 1 for update'))[0];
    if (!cur || !cur.prepared_hash) return { cancelled: false };
    await db.query(`update ops.master_ownership_state set prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null, updated_at = now() where id = 1`);
    await db.query(`insert into ops.master_ownership_events (action, ownership_hash, ownership, actor) values ('cancel_prepare', $1, $2, $3)`, [cur.prepared_hash, cur.prepared_map, actor]);
    return { cancelled: true };
  });
}
