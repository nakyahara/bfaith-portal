/**
 * master-cutover.mjs — 商品マスタの切替の段階を読む・進める・門の記録を書く (Company DB構想 10 §8・14 §5。Codex ⑤-R1 H1・PR #1563 R1 H1。0050 の ops.master_cutover_state)
 *
 * 段階 (一方向・1 段ずつ): legacy_open (古い入口 = NE・/register が正) → frozen (古い入口を止めた) → company_owner (持ち主が Company DB) → new_open (新しい画面で保存できる)
 * 使うところ:
 *   - 新しい画面の保存 (lib/master-write.mjs): newEntryWritable(段階, 持ち主表) = 段階が new_open かつ 持ち主表のハッシュが段階の記録と同じ (+ 列が company・MASTER_EDIT_OPEN は呼び手)
 *   - 古い入口 (⑤-3 で塞ぐ /register・会計アプリの POST など。Render と miniPC の両方): legacyWritable が false なら 410 + 新しい画面へ。
 *     プロセスごとに起動と一定の間隔 (ops.master_cutover_ack_fresh_minutes() = 15 分より短く) で recordLegacyGateAck を呼ぶ
 *     (build_id・古い入口の一覧 manifest・持ち主表のハッシュ・今の段階・書きかけの数。時刻はサーバーの時計。書けるのはロール master_gate)
 *   - 手の操作 (scripts/company-db/master-cutover.mjs): 見る・証拠つきで 1 段進める (運用のロール master_ops だけ実行できる)
 * 🚨 読めない (表が無い・つながらない・壊れた値) = 閉じている (fail-closed)。新しい画面も古い入口も「書けない」側に倒す
 */
import crypto from 'node:crypto';

export const CUTOVER_PHASES = Object.freeze(['legacy_open', 'frozen', 'company_owner', 'new_open']);
export const PHASE_LABELS = Object.freeze({ legacy_open: '切替前 (NE・/register が正)', frozen: '切替中 (古い入口を止めた)', company_owner: '切替中 (持ち主が Company DB)', new_open: '切替後 (新しい画面で保存できる)' });
/** 段階を読む取引が持つ共有の鍵 (段階を変える ops.set_master_cutover_phase は排他で取る = 保存の途中で段階が変わらない) */
export const CUTOVER_SHARED_LOCK_SQL = `select pg_advisory_xact_lock_shared(hashtext('ops.master_cutover'))`;

/** 持ち主表のハッシュ (キーの順によらない)。段階の記録・門の記録と、動いているコードの持ち主表が同じかを比べる */
export function ownershipHash(ownership) {
  const keys = Object.keys(ownership || {}).sort();
  return crypto.createHash('sha256').update(JSON.stringify(keys.map((k) => [k, ownership[k]]))).digest('hex');
}

/**
 * 今の段階を読む。db = { query } (node-postgres の adapter / PGlite)。inTx = 保存の取引の中 (誤りはそのまま上げる = 取引ごと止める)
 * 戻り値 { readable, phase, owner_hash, changed_at, changed_by, error }。読めなければ readable = false・phase = null
 */
export async function readCutoverPhase(db, { inTx = false } = {}) {
  try {
    const r = (await db.query(`select phase, owner_hash, changed_at::text as changed_at, changed_by from ops.master_cutover_state where id = 1`)).rows[0];
    if (!r || !CUTOVER_PHASES.includes(r.phase)) return { readable: false, phase: null, owner_hash: null, changed_at: null, changed_by: null, error: r ? `知らない段階 ${r.phase}` : '段階の行が無い' };
    return { readable: true, phase: r.phase, owner_hash: r.owner_hash ?? null, changed_at: r.changed_at, changed_by: r.changed_by, error: null };
  } catch (e) {
    if (inTx) throw e;
    return { readable: false, phase: null, owner_hash: null, changed_at: null, changed_by: null, error: String(e && e.message || e) };
  }
}

/** 新しい画面で保存してよい段階か (読めない = いいえ)。ownership を渡すと、持ち主表のハッシュが段階の記録と同じことも見る */
export function newEntryWritable(s, ownership = undefined) {
  if (!s || s.readable !== true || s.phase !== 'new_open') return false;
  if (ownership !== undefined && s.owner_hash !== ownershipHash(ownership)) return false;
  return true;
}
/** 古い入口で書いてよい段階か (読めない = いいえ = 止める。⑤-3 の塞ぎ方が使う) */
export const legacyWritable = (s) => !!s && s.readable === true && s.phase === 'legacy_open';

/**
 * 1 段進める (人の手の操作だけ)。evidence = 証拠 (ops.set_master_cutover_phase の検査):
 *   共通:            { expected_builds: { render: [build_id...], minipc: [...] }, manifest_hash, owner_hash }
 *                    (15 分以内の記録をプロセスごとに見て、全部が予定の build・同じ manifest・同じ持ち主表・今の段階を見ている・場所ごとに 1 つ以上)
 *   → frozen:        + drain: { done: true, checked_by, checked_at }・manual_entries_stopped: [{ id, by, at }] (manifest の kind = manual の id と完全に同じ集合)
 *   → company_owner: owner_hash = 新しい持ち主表 (company)。記録は frozen に入った後・書きかけ 0
 *   → new_open:      owner_hash = company_owner のときと同じ。記録は company_owner に入った後・書きかけ 0
 */
export async function advanceCutoverPhase(db, { to, actor, evidence, note = null }) {
  if (!CUTOVER_PHASES.includes(to)) throw Object.assign(new Error(`知らない段階: ${to}`), { code: 'CUTOVER_INVALID' });
  if (!actor) throw Object.assign(new Error('誰が進めたか (actor) が要る'), { code: 'CUTOVER_INVALID' });
  if (!evidence || typeof evidence !== 'object') throw Object.assign(new Error('証拠 (evidence) が要る'), { code: 'CUTOVER_INVALID' });
  return (await db.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb, $4) as r', [to, actor, JSON.stringify(evidence), note])).rows[0].r;
}

/**
 * 門の記録を書く (⑤-3 の古い入口の門が呼ぶ。⑤-1 では呼び手なし)。DB の関数 ops.record_legacy_gate_ack (security definer・ロール master_gate) を呼ぶだけ。
 * host = 'render' / 'minipc'・instanceId = プロセスの名前 (Render の instance・miniPC の PC 名 + 仕組みの名前)・manifest = { entries: [{ id, kind: 'code' | 'manual' }] }
 * phaseSeen = いま読んだ段階 (DB の段階と違えば拒む = 読み直してから)・inflightCount = 古い入口の書きかけの数。戻り値 { ack_id, manifest_hash, acked_at }
 */
export async function recordLegacyGateAck(db, { host, instanceId, buildId, manifest, ownership, phaseSeen, inflightCount = 0, oldestInflightAt = null }) {
  return (await db.query('select ops.record_legacy_gate_ack($1, $2, $3, $4::jsonb, $5, $6, $7, $8::timestamptz) as r',
    [host, instanceId, buildId, JSON.stringify(manifest), ownershipHash(ownership), phaseSeen, inflightCount, oldestInflightAt])).rows[0].r;
}

/** 古い入口の一覧のハッシュ (DB と同じ計算 = DB に計算させる。証拠の manifest_hash に使う) */
export async function manifestHashOf(db, manifest) {
  return (await db.query('select ops.legacy_manifest_hash($1::jsonb) as h', [JSON.stringify(manifest)])).rows[0].h;
}
