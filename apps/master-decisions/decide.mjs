/**
 * decide.mjs — マスタの判断 (照合 ② の判断の候補) を読む・決める (D2'。Company DB構想 10 §6.1.1「D2' 判断の画面と API の契約 v1」)
 *
 * 読む: 候補 (ops.master_decision_candidates) + 最新の判断 (approved / rejected / revoked の最後) + 完了 (action_done)。
 *   「今の回に出ている」= 候補の最後に見た回 = 最新の照合の回 (観測の最後)。
 * 決める: 1 つの取引で候補の行を**指紋の順に for update** (照合の完了の関数 ops.record_decision_done と同じ行を先に取る = D1 契約 v3) → 1 件ずつ確かめて出来事を書く。
 *   確かめ = 候補がある / 承認・却下は今の回に出ていて画面が見た回と同じ / 最新の判断が画面で見たものと同じ / 選べる解決 / 直す目標の値の型 / 取り消しは判断があるとき。
 *   1 件ずつ savepoint (1 件の失敗で全部を捨てない。飛ばした理由を返す)
 * 🚨 誰が決められるかは router (名簿)。ここは書く人のメールを受け取るだけ
 */

export const RESOLUTIONS = Object.freeze(['accept_difference', 'fix_ne', 'fix_cdb', 'fix_input', 'spec']);
export const KINDS = Object.freeze(['approved', 'rejected', 'revoked']);
export const MAX_ITEMS = 500;
const FP_RE = /^[0-9a-f]{64}$/;
const RUN_RE = /^mc_\d{8}T\d{9}Z_[0-9a-f]{6}$/;
const ABSENT = '__absent__';
const FIX = new Set(['fix_ne', 'fix_cdb']);

/** 入力の誤り (400) */
export class DecideError extends Error {
  constructor(message, reason = 'invalid_input') { super(message); this.code = 'VALIDATION'; this.reason = reason; }
}

/** 最新の照合の回 (観測の最後)。無ければ null */
export async function latestRun(db) {
  const r = (await db.query(`select compare_run_id, observed_at::text as observed_at from ops.master_decision_observations order by observed_at desc, compare_run_id desc limit 1`)).rows[0];
  return r ? { compare_run_id: r.compare_run_id, observed_at: r.observed_at } : null;
}

const CANDIDATE_SQL = `
  with latest as (
    select distinct on (fingerprint) fingerprint, event_id, kind, resolution, target, actor, created_at, note
      from ops.master_decision_events where kind in ('approved', 'rejected', 'revoked') %FILTER%
     order by fingerprint, event_id desc
  )
  select c.fingerprint, c.subject_key, c.code_norm, c.col, c.child, c.cls, c.reason_kind, c.semantic, c.print, c.resolutions, c.proposal,
         c.first_seen_run, c.first_seen_at::text as first_seen_at, c.last_seen_run, c.last_seen_at::text as last_seen_at, c.seen_count,
         l.event_id, l.kind, l.resolution, l.target, l.actor, l.created_at::text as decided_at, l.note,
         (select d.created_at::text from ops.master_decision_events d where d.kind = 'action_done' and d.approved_event_id = l.event_id limit 1) as done_at
    from ops.master_decision_candidates c
    left join latest l on l.fingerprint = c.fingerprint
   %WHERE%`;

/**
 * 1 行を画面の形に。状態 = 最新の判断が approved → approved (直す承認で完了があり今の回にまた出ている = 再発 = pending・今の回に無い = done) / rejected / それ以外 = pending
 */
export function shapeCandidate(r, latest) {
  const current = !!latest && r.last_seen_run === latest.compare_run_id;
  const decision = r.event_id == null ? null : { event_id: Number(r.event_id), kind: r.kind, resolution: r.resolution, target: r.target, actor: r.actor, at: r.decided_at, note: r.note };
  let status = 'pending', reoccurred = false;
  if (decision && decision.kind === 'approved') {
    if (FIX.has(decision.resolution) && r.done_at) { if (current) { status = 'pending'; reoccurred = true; } else status = 'done'; } else status = 'approved';
  } else if (decision && decision.kind === 'rejected') status = 'rejected';
  const p = r.print || {};
  return { fingerprint: r.fingerprint, subject_key: r.subject_key, code_norm: r.code_norm, col: r.col, child: r.child, cls: r.cls, reason_kind: r.reason_kind, semantic: r.semantic,
    resolutions: r.resolutions || [], proposal: r.proposal, n: p.n ?? null, n_state: p.n_state ?? null, c: p.c ?? null, reason: p.reason ?? null, problem: p.problem ?? null, sku_kind: p.sku_kind ?? null,
    first_seen_at: r.first_seen_at, last_seen_at: r.last_seen_at, last_seen_run: r.last_seen_run, seen_count: Number(r.seen_count), current, status, reoccurred, decision, done_at: r.done_at };
}

/** 画面の上の件数 (今の回に出ている候補を 理由の種類 × 状態 で) */
export async function summary(db) {
  const latest = await latestRun(db);
  const rows = (await db.query(CANDIDATE_SQL.replace('%FILTER%', '').replace('%WHERE%', ''))).rows.map((r) => shapeCandidate(r, latest));
  const byReason = {};
  const total = { pending: 0, approved: 0, rejected: 0, done: 0 };
  for (const c of rows) {
    if (!c.current) continue;
    const k = c.reason_kind;
    byReason[k] ??= { pending: 0, approved: 0, rejected: 0, done: 0 };
    byReason[k][c.status]++; total[c.status]++;
  }
  return { latest, total, by_reason: byReason, candidates: rows.length, current: rows.filter((c) => c.current).length };
}

/**
 * 候補の一覧。filters = { view: current|all, status: pending|approved|rejected|done|any, reason, cls, q, limit, offset }
 */
export async function listCandidates(db, filters = {}) {
  const view = filters.view === 'all' ? 'all' : 'current';
  const status = ['pending', 'approved', 'rejected', 'done', 'any'].includes(filters.status) ? filters.status : 'pending';
  const limit = Math.min(Math.max(Number(filters.limit) || 200, 1), 1000), offset = Math.max(Number(filters.offset) || 0, 0);
  const q = String(filters.q || '').trim().toLowerCase();
  const latest = await latestRun(db);
  let rows = (await db.query(CANDIDATE_SQL.replace('%FILTER%', '').replace('%WHERE%', ''))).rows.map((r) => shapeCandidate(r, latest));
  if (view === 'current') rows = rows.filter((c) => c.current);
  if (status !== 'any') rows = rows.filter((c) => c.status === status);
  if (filters.reason) rows = rows.filter((c) => c.reason_kind === filters.reason);
  if (filters.cls) rows = rows.filter((c) => c.cls === filters.cls);
  if (q) rows = rows.filter((c) => c.code_norm.includes(q) || c.subject_key.toLowerCase().includes(q) || String(c.child || '').includes(q));
  rows.sort((a, b) => (a.reason_kind < b.reason_kind ? -1 : a.reason_kind > b.reason_kind ? 1 : a.code_norm < b.code_norm ? -1 : a.code_norm > b.code_norm ? 1 : String(a.col).localeCompare(String(b.col))));
  return { latest, total: rows.length, offset, limit, items: rows.slice(offset, offset + limit) };
}

/** 1 つの候補の出来事の履歴 (完了も) */
export async function candidateEvents(db, fingerprint) {
  if (!FP_RE.test(String(fingerprint))) throw new DecideError('指紋の形が違う');
  const latest = await latestRun(db);
  const c = (await db.query(CANDIDATE_SQL.replace('%FILTER%', 'and fingerprint = $1').replace('%WHERE%', 'where c.fingerprint = $1'), [fingerprint])).rows[0];
  if (!c) return null;
  const events = (await db.query(`select event_id, kind, resolution, target, approved_event_id, actor_type, actor, note, observed, created_at::text as created_at
    from ops.master_decision_events where fingerprint = $1 order by event_id`, [fingerprint])).rows.map((e) => ({ ...e, event_id: Number(e.event_id), approved_event_id: e.approved_event_id == null ? null : Number(e.approved_event_id) }));
  return { candidate: shapeCandidate(c, latest), print: c.print, events };
}

// ─────────── 直す目標の値 ───────────
/** 列ごとの目標の値の型 (照合が見る Company DB の形。compare-ne の neUnit / cdbUnit と同じ) */
export function validTargetValue(col, v) {
  switch (col) {
    case 'name': return typeof v === 'string' && v.trim().length > 0;
    case 'handling': return v === 'active' || v === 'discontinued';
    case 'tax_rate': return v === 0.1 || v === 0.08;
    case 'standard_price_jpy': case 'cost': return typeof v === 'number' && Number.isFinite(v) && v > 0;
    case 'primary_supplier': return typeof v === 'string' && v.trim().length > 0;
    case 'components': return v === ABSENT || (Number.isInteger(v) && v > 0);
    case 'exists': return typeof v === 'boolean';
    case 'kind': return v === 'single' || v === 'set';
    default: return false;
  }
}
/** 画面で値を入れなかったときの目標の値 (提案から)。無ければ undefined */
export function defaultTargetValue(c, resolution) {
  const p = c.proposal || {};
  if (resolution === 'fix_ne' && p.op === 'set_ne_value' && p.value !== undefined && p.value !== null) return p.value;
  if (resolution === 'fix_cdb') {   // manual を CDB で直す = NE の値に合わせる
    const n = c.print?.n;
    if (n === '(無い)') return ABSENT;
    if (n !== undefined && n !== null && typeof n !== 'object') return n;
  }
  return undefined;
}

/**
 * 決める。items = [{ fingerprint, shown_last_seen_run, shown_event_id, target_value? }]
 * @returns {Promise<{ applied: Array<{ fingerprint, event_id }>, skipped: Array<{ fingerprint, reason, message? }>, latest }>}
 */
export async function applyDecisions(db, { actor, kind, resolution = null, note = null, items }) {
  if (!actor || typeof actor !== 'string') throw new DecideError('決める人 (メール) が無い');
  if (!KINDS.includes(kind)) throw new DecideError(`kind は ${KINDS.join(' / ')} のどれか`);
  if (kind === 'approved' && !RESOLUTIONS.includes(resolution)) throw new DecideError(`承認には解決 (${RESOLUTIONS.join(' / ')}) が要る`);
  if (kind !== 'approved' && resolution != null) throw new DecideError('却下・取り消しに解決は付けない');
  if (note != null && (typeof note !== 'string' || note.length > 500)) throw new DecideError('メモは 500 字まで');
  if (!Array.isArray(items) || !items.length) throw new DecideError('items が空');
  if (items.length > MAX_ITEMS) throw new DecideError(`1 回に ${MAX_ITEMS} 件まで`);
  const seen = new Set();
  for (const it of items) {
    if (!it || !FP_RE.test(String(it.fingerprint))) throw new DecideError('指紋の形が違う');
    if (seen.has(it.fingerprint)) throw new DecideError(`同じ指紋が 2 回: ${it.fingerprint}`);
    seen.add(it.fingerprint);
    if (kind !== 'revoked' && !RUN_RE.test(String(it.shown_last_seen_run))) throw new DecideError('shown_last_seen_run (画面が見た回) が無い');
    if (it.shown_event_id != null && !Number.isInteger(it.shown_event_id)) throw new DecideError('shown_event_id は整数か null');
  }
  if (items.some((it) => it.target_value !== undefined) && items.length > 1) throw new DecideError('目標の値を入れるのは 1 件ずつ');
  const fps = [...seen].sort();
  const applied = [], skipped = [];
  await db.query('begin');
  try {
    // 候補の行を指紋の順に取る (照合の完了の関数と同じ行・同じ順 = 待ち合いはしてもデッドロックしない)
    const cands = new Map((await db.query(`select fingerprint, subject_key, col, child, resolutions, proposal, print, last_seen_run from ops.master_decision_candidates
      where fingerprint = any($1::text[]) order by fingerprint for update`, [fps])).rows.map((r) => [r.fingerprint, r]));
    const latest = await latestRun(db);
    const lastDecision = new Map((await db.query(`select distinct on (fingerprint) fingerprint, event_id, kind from ops.master_decision_events
      where fingerprint = any($1::text[]) and kind in ('approved', 'rejected', 'revoked') order by fingerprint, event_id desc`, [fps])).rows.map((r) => [r.fingerprint, { event_id: Number(r.event_id), kind: r.kind }]));
    const byFp = new Map(items.map((it) => [it.fingerprint, it]));
    for (const fp of fps) {
      const it = byFp.get(fp), c = cands.get(fp);
      const skip = (reason, message = null) => skipped.push({ fingerprint: fp, reason, ...(message ? { message } : {}) });
      if (!c) { skip('not_found'); continue; }
      const last = lastDecision.get(fp) || null;
      if (kind !== 'revoked') {
        if (!latest || c.last_seen_run !== latest.compare_run_id) { skip('not_current'); continue; }        // 差が消えた・変わった (指紋が変わると古い指紋の最後の回は動かない)
        if (c.last_seen_run !== it.shown_last_seen_run) { skip('stale_view'); continue; }                    // 画面を開いた後に新しい照合
      }
      if ((last ? last.event_id : null) !== (it.shown_event_id ?? null)) { skip('decided_meanwhile'); continue; }   // ほかの人が先に決めた
      let target = null;
      if (kind === 'approved') {
        if (!(c.resolutions || []).includes(resolution)) { skip('resolution_not_allowed'); continue; }
        if (FIX.has(resolution)) {
          const v = it.target_value !== undefined ? it.target_value : defaultTargetValue(c, resolution);
          if (v === undefined) { skip('needs_target'); continue; }
          if (!validTargetValue(c.col, v)) { skip('invalid_target'); continue; }
          target = { subject_key: c.subject_key, col: c.col, child: c.child ?? null, value: v };
        }
      } else if (kind === 'revoked' && (!last || last.kind === 'revoked')) { skip('nothing_to_revoke'); continue; }
      await db.query('savepoint one_decision');
      try {
        const ev = (await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor, shown_fingerprint, note)
          values ($1, $2, $3, $4::jsonb, 'user', $5, $1, $6) returning event_id`, [fp, kind, kind === 'approved' ? resolution : null, target ? JSON.stringify(target) : null, actor, note || null])).rows[0];
        await db.query('release savepoint one_decision');
        applied.push({ fingerprint: fp, event_id: Number(ev.event_id) });
      } catch (e) {
        await db.query('rollback to savepoint one_decision');
        skip('db_rejected', String(e && e.message).slice(0, 200));
      }
    }
    await db.query('commit');
    return { applied, skipped, latest };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    throw e;
  }
}
