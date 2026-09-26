/**
 * decide.mjs — マスタの判断 (照合 ② の判断の候補) を読む・決める (D2'。Company DB構想 10 §6.1.1「D2' 判断の画面と API の契約 v1」)
 *
 * 読む: 候補 (ops.master_decision_candidates) + 最新の判断 (approved / rejected / revoked の最後) + 完了 (action_done)。
 *   「今の回に出ている」= 候補の最後に見た回 = 最新の照合の回 (0034 の照合の回の記録 = 候補 0 件の回も。blocked・台帳に書けなかった回は入らない)。
 * 決める: 1 つの取引で候補の行を**指紋の順に for update** (照合の完了の関数 ops.record_decision_done と同じ行を先に取る = D1 契約 v3) → 1 件ずつ確かめて出来事を書く。
 *   確かめ = 候補がある / 承認・却下は今の回に出ていて画面が見た回と同じ / 最新の判断が画面で見たものと同じ / 選べる解決 / 直す目標の値の型 / 取り消しは判断があるとき。
 *   1 件ずつ savepoint (1 件の失敗で全部を捨てない。飛ばした理由を返す)
 * 🚨 誰が決められるかは router (名簿)。ここは書く人のメールを受け取るだけ
 */

import { normSku } from '../../lib/sku-norm.js';
import { canonicalSupplierCode } from '../company-db/load/sources.mjs';

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

/**
 * 最新の照合の回 = 判断の台帳に書けた最後の回 (0034 の ops.master_compare_runs = 候補 0 件の回も入る。Codex #1481 R1 High)。
 * blocked・台帳に書けなかった回は入らない = 最後に判定して書けた回のまま (画面にその日時を出す)。0034 の前は観測の最後 (候補 0 件の回は分からない)
 */
export async function latestRun(db) {
  const hasRuns = (await db.query(`select to_regclass('ops.master_compare_runs') is not null as ok`)).rows[0].ok;
  const r = (await db.query(hasRuns
    ? `select compare_run_id, observed_at::text as observed_at, candidates from ops.master_compare_runs order by observed_at desc, compare_run_id desc limit 1`
    : `select compare_run_id, observed_at::text as observed_at, null::int as candidates from ops.master_decision_observations order by observed_at desc, compare_run_id desc limit 1`)).rows[0];
  return r ? { compare_run_id: r.compare_run_id, observed_at: r.observed_at, candidates: r.candidates == null ? null : Number(r.candidates) } : null;
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
const BAD = Object.freeze({ ok: false });
const ok = (value) => ({ ok: true, value });
const text = (x) => (typeof x === 'string' ? x.trim() : typeof x === 'number' && Number.isFinite(x) ? String(x) : '');
/**
 * 目標の値を、照合が完了を確かめるときの形 (compare-ne の neUnit / cdbUnit = Company DB の形) にそろえる。
 * 画面の入力 (文字) もここで列ごとに読む (商品名・仕入先コードは文字のまま。Codex #1481 R1 Medium)
 *   売価・原価 = 1 以上の整数 (照合は円を整数に丸めて比べる = 100.5 は完了しない) / 税率 = 0.1・0.08 (10・8・10% も) /
 *   代表の仕入先 = 照合と同じ正規化 (1 つだけの配列はほどく。複数は黙って 1 つに絞らない) / 構成 = 1 以上の整数か「無い」(子を消す) /
 *   有無 = true・false (あり・なし) / 種類 = single・set (単品・セット) / 取扱区分 = active・discontinued (取扱中・取扱中止) / 商品名 = 空でない文字
 * @returns {{ ok: true, value: any } | { ok: false }}
 */
export function normalizeTarget(col, v) {
  const s = text(v);
  switch (col) {
    case 'name': return typeof v === 'string' && v.trim() ? ok(v.trim()) : BAD;
    case 'handling':
      if (v === 'active' || s === '取扱中') return ok('active');
      if (v === 'discontinued' || s === '取扱中止' || s === 'ﾒｰｶｰ取扱中止') return ok('discontinued');
      return BAD;
    case 'tax_rate': {
      const n = typeof v === 'number' ? v : /^\d+(\.\d+)?%?$/.test(s) ? Number(s.replace('%', '')) : NaN;
      if (n === 0.1 || n === 10) return ok(0.1);
      if (n === 0.08 || n === 8) return ok(0.08);
      return BAD;
    }
    case 'standard_price_jpy': case 'cost': {
      const n = typeof v === 'number' ? v : /^\d{1,3}(,\d{3})+$|^\d+$/.test(s) ? Number(s.replace(/,/g, '')) : NaN;
      return Number.isInteger(n) && n > 0 ? ok(n) : BAD;
    }
    case 'primary_supplier': {
      let x = v;
      if (Array.isArray(x)) { if (x.length !== 1) return BAD; x = x[0]; }
      const t = text(x);
      return t ? ok(normSku(canonicalSupplierCode(t))) : BAD;
    }
    case 'components': {
      if (v === ABSENT || s === '無い' || s === '(無い)' || s === 'なし') return ok(ABSENT);
      const n = typeof v === 'number' ? v : /^\d+$/.test(s) ? Number(s) : NaN;
      return Number.isInteger(n) && n > 0 ? ok(n) : BAD;
    }
    case 'exists':
      if (v === true || s === 'true' || s === 'あり') return ok(true);
      if (v === false || s === 'false' || s === 'なし') return ok(false);
      return BAD;
    case 'kind':
      if (v === 'single' || s === '単品') return ok('single');
      if (v === 'set' || s === 'セット') return ok('set');
      return BAD;
    default: return BAD;
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
 * 決める。items = [{ fingerprint, shown_last_seen_run, shown_event_id, target_value? (型つき) | target_text? (画面の入力の文字 = 列ごとに読む) }]
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
  if (items.some((it) => it.target_value !== undefined || it.target_text !== undefined) && items.length > 1) throw new DecideError('目標の値を入れるのは 1 件ずつ');
  if (items.some((it) => it.target_text !== undefined && typeof it.target_text !== 'string')) throw new DecideError('target_text は文字');
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
          const given = it.target_text !== undefined ? it.target_text : it.target_value;
          const raw = given !== undefined ? given : defaultTargetValue(c, resolution);
          if (raw === undefined) { skip('needs_target'); continue; }
          const nv = normalizeTarget(c.col, raw);
          // 入れた値が読めない = invalid_target / 提案の値が目標にできない (複数の仕入先など) = 値を入れて 1 件ずつ
          if (!nv.ok) { skip(given !== undefined ? 'invalid_target' : 'needs_target'); continue; }
          target = { subject_key: c.subject_key, col: c.col, child: c.child ?? null, value: nv.value };
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
