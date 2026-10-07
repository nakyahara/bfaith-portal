/**
 * widen-state.mjs — 広げる道 (new_open のまま持ち主に company のキーを足す) の DB の関数を呼ぶだけの部品 (0058。設計 = 広げる道 v10 §3・§5)
 *
 *   prepare (試みを作る) → 手の入口を止めた記録 → 全部のプロセスの ack → 回収のロード → prepared のロード → 写し・作り直し・確かめ → check → widen
 *   判定は DB の 1 つの本体 (ops._widen_judge)。ここは呼ぶだけ (判定を JS で二重に書かない)
 * 🚨 commit の番号 (base_commit_seq・試みの中の 2 つのロード) は bigint = 文字のまま扱う (JS の Number にしない。設計 v10 §7.1 の 5)
 * 🚨 書く関数は DB の持ち主 (COMPANY_DB_URL) だけが実行できる。check は watcher (COMPANY_DB_WATCH_URL) でも読める (ops.widen_check_readonly)
 */

const one = async (db, sql, params) => (await db.query(sql, params)).rows[0];

/** 今開いている試み (prepared) を読む。無い・0058 の前 = null。番号は文字 */
export async function readOpenWidenAttempt(db) {
  const has = (await one(db, `select to_regclass('ops.master_widen_attempts') is not null as ok`)).ok === true;
  if (!has) return null;
  return (await one(db, `select widen_prepare_id::text as widen_prepare_id, company_id, added_keys, active_hash, prepared_hash,
      to_char(prepared_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as prepared_at, base_commit_seq::text as base_commit_seq, loader_fingerprint,
      manifest_hash, prepared_by, state
    from ops.master_widen_attempts where state = 'prepared'`)) ?? null;
}

/**
 * prepare --widen: 試みを作る (DB の関数 ops.prepare_master_widen = epoch の排他の鍵の後に base_commit_seq を同じ取引で取る)。
 * loaderFingerprint = 夜間ロードの規則の指紋 (engine.mjs の LOAD_RULE_FINGERPRINT・この checkout)
 * @param {{ companyId: number, map: object, loaderFingerprint: string, manifest: object, actor: string }} p
 * @returns {{ widen_prepare_id, prepared_hash, prepared_at, base_commit_seq: string, added_keys: string[], manifest_hash, required_manual_entries: string[] }}
 */
export async function prepareWiden(db, { companyId, map, loaderFingerprint, manifest, actor }) {
  return (await one(db, 'select ops.prepare_master_widen($1::integer, $2::jsonb, $3, $4::jsonb, $5) as r',
    [companyId, JSON.stringify(map), loaderFingerprint, JSON.stringify(manifest), actor])).r;
}

/** 手の入口 (NE の画面など) を止めた記録 (DB の時刻)。止めてよいのは試みの「要る入口」だけ */
export async function recordWidenManualStop(db, { attemptId, entryId, stoppedBy, note = null }) {
  return (await one(db, 'select ops.record_widen_manual_stop($1::uuid, $2, $3, $4) as r', [attemptId, entryId, stoppedBy, note])).r;
}

/** cancel: prepared を消して試みを cancelled に (active は変えない) */
export async function cancelWiden(db, { attemptId, actor }) {
  return (await one(db, 'select ops.cancel_master_widen($1::uuid, $2) as r', [attemptId, actor])).r;
}

/** 読むだけの判定 (watcher でも)。widen と同じ本体 = 同じ答え */
export async function widenCheck(db, { attemptId, companyId }) {
  return (await one(db, 'select ops.widen_check_readonly($1::uuid, $2::integer) as r', [attemptId, companyId])).r;
}

/**
 * widen (apply)。取引の中で lock_timeout (既定 5 秒) を置いてから DB の関数を呼ぶ (夜間ロードの最中は 5 秒で諦める = 55P03)。
 * evidence = 写しの証拠 (activationEvidence と同じ・load_commit_seq は文字)
 */
export async function widenOwnership(db, { attemptId, companyId, actor, evidence, lockTimeout = '5s' }) {
  if (!/^[0-9]{1,3}(ms|s)$/.test(lockTimeout)) throw Object.assign(new Error(`lockTimeout の形が違う: ${lockTimeout}`), { code: 'BAD_LOCK_TIMEOUT' });
  await db.query('begin');
  try {
    await db.query(`set local lock_timeout = '${lockTimeout}'`);
    const r = (await one(db, 'select ops.widen_master_ownership($1::uuid, $2::integer, $3, $4::jsonb) as r', [attemptId, companyId, actor, JSON.stringify(evidence)])).r;
    await db.query('commit');
    return r;
  } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
}
