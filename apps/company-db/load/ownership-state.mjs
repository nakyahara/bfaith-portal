/**
 * ownership-state.mjs — 持ち主の設定の epoch (0055 の ops.master_ownership_state。マスタ正本切替 ④a・Codex #1564 R1 H1)
 *
 *   configured = config/master-ownership.mjs (コードに書いた「こうしたい」。これだけでは何も変わらない)
 *   prepared   = 人が prepare で記録した「次にこれにする」。明示して頼んだロード (usePrepared) と、そのロードの後の写しの世代だけが使う
 *   active     = 今使っている持ち主。夜間ロード (engine.mjs)・miniPC の写し (publish/fetch.mjs) はこれ。
 *                prepared の世代で miniPC の作り直し + 入れた後の確かめが通ったときだけ activate で prepared → active
 *   行が無い (0055 の前・まだ誰も prepare していない) = active は全部 'load' (今までと同じ)
 * 🚨 読む (resolve) は夜間ロード・写しの両方。書く (prepare / activate / cancel) は scripts/company-db/master-ownership-epoch.mjs だけ (DB を作ったユーザー)
 * 🚨 このファイルは engine.mjs が読む = master-publish.js (warehouse) を import しない (循環を作らない)
 */
import { OWNED_COLUMNS, validateOwnership } from '../../../config/master-ownership.mjs';
import { ownershipHash } from '../../../lib/master-cutover.mjs';

/** 全部 'load' (行が無いときの active) */
export const ALL_LOAD = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
/**
 * 持ち主の設定のハッシュ = lib/master-cutover.mjs の ownershipHash (load の列は数えない = 記録に無い列は load と同じ。#1564 Codex R3 Medium)。
 *   engine.mjs が ops.load_materials に残す・写しの世代・⑤-1 の段階の記録 (owner_hash)・0055 の ops.ownership_hash と同じ 1 つの式
 */
export const ownershipHashOf = ownershipHash;
export const sortedOwnership = (o) => Object.fromEntries(Object.keys(o || {}).sort().map((k) => [k, o[k]]));

const rowsOf = async (db, sql, params) => (await db.query(sql, params)).rows;
/** 表があるか (0055 の前 = 無い) */
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
 * 古い入口の門 (lib/master-legacy-gate.mjs・⑤-3b) が使う「持ち主が C の列」= active と prepared の C の列を合わせたもの。
 *   🚨 prepared も数える: prepare (切替の日) から activate までの間に、古い入口から入れた値は --use-prepared のロード・写しの材料に入らず、
 *      activate の後の夜間ロードは C の列の既にある行を上書きしない = 黙って消える。門は legacy_open の間は持ち主を読まない = prepare しただけでは閉じない
 *      (閉じ始めるのは frozen にした時点)。cancel で prepared が消えると、prepared だけで C だった列 (active では load) の入口は frozen のままでも再び開く (activate の後は prepared が無く active が C = 閉じたまま・cancel は対象外)。
 *      prepare をまたいで書き終えた値は、frozen の後の最後の active (全部 load) のロードで回収する (README の 2a・2b)
 *   🚨 読めない (表が無い = 0055 の前・権限が無い・記録が壊れている) = { readable: false } = 門は閉じる側 (fail-closed)。行が無い = 全部 load (誰も prepare していない)
 *   config (configured) は見ない (デプロイの成果物 = 場所ごとに切り替わる時刻が違う。契約 v3 H1)
 * @returns {{ readable: boolean, company: string[]|null, active_hash: string|null, prepared_hash: string|null, state: string|null, error: string|null }}
 */
export async function readGateOwnership(db) {
  try {
    const st = await readOwnershipState(db);
    if (st.state === 'no_table') return { readable: false, company: null, active_hash: null, prepared_hash: null, state: st.state, error: '持ち主の epoch の表 (ops.master_ownership_state・0055) が無い' };
    const company = new Set(Object.keys(st.active.map).filter((k) => st.active.map[k] === 'company'));
    if (st.prepared) for (const k of Object.keys(st.prepared.map)) if (st.prepared.map[k] === 'company') company.add(k);
    return { readable: true, company: [...company].sort(), active_hash: st.active.hash, prepared_hash: st.prepared ? st.prepared.hash : null, state: st.state, error: null };
  } catch (e) {
    return { readable: false, company: null, active_hash: null, prepared_hash: null, state: null, error: `持ち主を読めない: ${String((e && e.message) || e).slice(0, 300)}` };
  }
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
 * 持ち主の epoch の鍵 (0055 の ops.master_ownership_lock_key() = 4705310055。#1564 Codex R3 High 1)。
 *   夜間ロード (engine.mjs・明示の --use-prepared も) = 取引の冒頭に共有で取ってから、取引の中で epoch を読む (読んだ epoch で書き終わるまで持つ)
 *   prepare / activate / cancel = 取引の冒頭に排他で取る = ロードの途中で epoch が変わらない (古い epoch を読んだロードが新しい active の後に commit しない)
 * 🚨 鍵の順 (全部の書き手で同じ = デッドロックしない): 持ち主の epoch (0055) → 切替の段階 (0051 の hashtext('ops.master_cutover')) →
 *   マスタの書き込み (0051 の core.master_write_lock_key() = 4705310051) → 親子 (0036 の core.parent_lock_key() = 4705310036) → 行。
 *   夜間ロード = epoch 共有 → 書き込み 排他 → 親子 / activate = epoch 排他 → 段階 共有 → 行 / 画面の保存 = 段階 共有 → 書き込み 共有 (epoch は取らない)
 */
export const OWNERSHIP_LOCK_EXISTS_SQL = `select to_regprocedure('ops.master_ownership_lock_key()') is not null as ok`;
export const OWNERSHIP_SHARED_LOCK_SQL = 'select pg_advisory_xact_lock_shared(ops.master_ownership_lock_key())';
export const OWNERSHIP_EXCLUSIVE_LOCK_SQL = 'select pg_advisory_xact_lock(ops.master_ownership_lock_key())';

/**
 * 最後に commit した夜間ロード = 0055 の ops.master_load_commits の番号 (commit_seq) が一番大きい行 (#1564 Codex R4 Medium 2)。
 *   番号は DB が commit の直前に振る (epoch の鍵とマスタの書き込みの鍵を持ったまま = 番号の順 = commit の順)。送り手の時計 (started_at / finished_at)・
 *   場所 (host) では並べない。どの場所から流したロードでも数える (毎晩の cron・--use-prepared の明示のロード)。dry-run は行が無い。
 *   activate が「証拠の世代の後にロードが入っていない」を見る・写し (publish/fetch.mjs) がどのロードの世代かを決める
 * @returns {{ commit_seq: string, ingest_run_id: string, epoch: string, ownership_hash: string, host: string|null }|null}  表が無い・行が無い = null
 *   🚨 commit_seq は bigint を 10 進の文字のまま (JS の Number にしない = 2^53 を超えても丸めない。広げる道 PR-1・設計 v10 §7.1 の 5)
 */
export async function latestLoadCommit(db) {
  if ((await rowsOf(db, `select to_regclass('ops.master_load_commits') is not null as ok`))[0].ok !== true) return null;
  const r = (await rowsOf(db, `select c.commit_seq::text as seq, c.ingest_run_id, c.epoch, c.ownership_hash, c.host from ops.master_load_commits c order by c.commit_seq desc limit 1`))[0];
  return r ? { commit_seq: r.seq, ingest_run_id: r.ingest_run_id, epoch: r.epoch, ownership_hash: r.ownership_hash, host: r.host } : null;   // 並べるのは SQL の数 (文字の列の名前で並べない = '9' > '10' にしない)。値は文字のまま
}

/** commit の番号 (bigint) の文字か = 先頭が 0 でない 10 進 (bigint の上限まで)。Number・BigInt は受けない (呼び手は文字で持つ = 丸めた番号で比べない) */
export const isCommitSeqText = (v) => typeof v === 'string' && /^[1-9][0-9]{0,18}$/.test(v) && (v.length < 19 || v <= '9223372036854775807');

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
/** 持ち主のコマンドの取引: 冒頭に epoch の鍵を排他で取る (夜間ロードの取引が終わるまで待つ = ロードの途中で epoch を変えない) */
const inTx = async (db, fn) => {
  await db.query('begin');
  try {
    if ((await rowsOf(db, OWNERSHIP_LOCK_EXISTS_SQL))[0].ok === true) await db.query(OWNERSHIP_EXCLUSIVE_LOCK_SQL);
    const r = await fn(); await db.query('commit'); return r;
  } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
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
    // prepare の時刻 = epoch の鍵を取った後の今 (取引を始めた時刻ではない = 待っていた夜間ロードの commit より後。activate の「prepare の後に読んだ世代」の基準)
    await db.query(`update ops.master_ownership_state set prepared_hash = $1, prepared_map = $2::jsonb, prepared_at = clock_timestamp(), prepared_by = $3, updated_at = now() where id = 1`, [h, JSON.stringify(m), actor]);
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
 *   expectLoadCommitSeq = 証拠の世代が読んだ夜間ロードの commit の番号 (0055 の ops.master_load_commits)。最後に commit したロードがこれでない = 断る
 *   (LOAD_AFTER_EVIDENCE。古い active で走ったロードが証拠の後に C の列を NE の値で書いた = 証拠の世代はもう DB と同じでない。#1564 Codex R3 High 1・R4 Medium 2)。
 *   最後のロードの持ち主が prepared でない = 断る (LOAD_EPOCH_MISMATCH)
 * 🚨 切替の段階が frozen (prepared の C の列の古い入口を止めた後・持ち主を C にする前) のときだけ (#1564 の見直し M-1)。
 *   段階の表が無い (⑤-1 の前) = 断る / legacy_open (古い入口が全部開いている) = 断る / company_owner・new_open (もう切り替えた後) = 断る。
 *   (frozen = 持ち主が C (active ∪ prepared) の列の古い入口だけ閉じている。load の列の入口は開いたまま = ⑤-3b)
 *   持ち主の正を 1 つにする残り (⑤-1 の company_owner に進む条件 = active_hash と owner_hash が同じ・新しい画面が DB の active を読む) は ⑤ / ⑥ で結ぶ
 */
export async function activateOwnership(db, { expectHash, expectPreparedAt, expectLoadCommitSeq, actor, evidence }) {
  if (!expectPreparedAt) throw Object.assign(new Error('証拠を集めたときの prepare の時刻 (expectPreparedAt) が要る'), { code: 'PREPARED_AT_REQUIRED' });
  if (!isCommitSeqText(expectLoadCommitSeq)) {   // 10 進の文字 (bigint のまま。Number は受けない)
    throw Object.assign(new Error('証拠の世代が読んだ夜間ロードの commit の番号 (expectLoadCommitSeq) が要る'), { code: 'LOAD_COMMIT_REQUIRED' });
  }
  return inTx(db, async () => {
    const phase = await readCutoverPhaseInTx(db);
    if (phase == null) throw Object.assign(new Error(`切替の段階の表 (${CUTOVER_TABLE}) が無い・行が無い = active にしない (⑤-1 の migration の後・段階 ${ACTIVATE_PHASE} で)`), { code: 'CUTOVER_STATE_MISSING' });
    if (phase !== ACTIVATE_PHASE) throw Object.assign(new Error(`切替の段階が ${phase} = active にしない (${ACTIVATE_PHASE} = C にする列の古い入口を止めた後だけ)`), { code: 'CUTOVER_PHASE_NOT_FROZEN', phase });
    const cur = (await rowsOf(db, `select prepared_hash, prepared_map, to_char(prepared_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as prepared_at
      from ops.master_ownership_state where id = 1 for update`))[0];
    if (!cur || !cur.prepared_hash) throw Object.assign(new Error('prepared の持ち主が無い'), { code: 'NO_PREPARED_OWNERSHIP' });
    if (cur.prepared_at !== expectPreparedAt) {
      throw Object.assign(new Error(`証拠を集めた後に prepare がやり直された (${expectPreparedAt} → ${cur.prepared_at}) = この証拠では active にしない (書きかけ 0 → 最後の active (全部 load) のロード (README の 2b・17 §4.2 #5) → その run_id の report の成功 + 照合 ② → --use-prepared のロード からやり直す)`), { code: 'PREPARED_CHANGED' });
    }
    // 確かめた世代の持ち主 (実際に効く持ち主のハッシュ = 記録に無い列は load を足した後) と比べる
    const eff = checkedMap(cur.prepared_map, cur.prepared_hash, 'prepared');
    if (eff.hash !== expectHash) throw Object.assign(new Error(`確かめた世代の持ち主 (${String(expectHash).slice(0, 12)}) が prepared (${eff.hash.slice(0, 12)}) と違う`), { code: 'PREPARED_MISMATCH' });
    // 証拠の世代の後に夜間ロードが入った = active にしない (#1564 Codex R3 High 1)。epoch の鍵 (排他) の後に見る =
    //   古い active を読んで走っていたロードは、ここより前に commit している (その書き込みは証拠の世代に入っていない = 写しからやり直す)
    const last = await latestLoadCommit(db);
    if (!last || last.commit_seq !== expectLoadCommitSeq) {
      throw Object.assign(new Error(`証拠の世代の後に夜間ロード (${last ? `${last.ingest_run_id} = 番号 ${last.commit_seq}` : 'なし'}) が入った (証拠 = 番号 ${expectLoadCommitSeq}) = この証拠では active にしない (最後の active (全部 load) のロード (README の 2b・17 §4.2 #5) → その run_id の report の成功 + 照合 ② → --use-prepared のロード からやり直す)`),
        { code: 'LOAD_AFTER_EVIDENCE', last_load: last ? last.ingest_run_id : null, last_commit_seq: last ? last.commit_seq : null });
    }
    if (last.ownership_hash !== eff.hash) {
      throw Object.assign(new Error(`最後に commit したロード (${last.ingest_run_id}) の持ち主が prepared でない (${last.epoch}) = active にしない`), { code: 'LOAD_EPOCH_MISMATCH', last_load: last.ingest_run_id });
    }
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
