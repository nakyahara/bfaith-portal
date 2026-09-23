/**
 * SP広告 検索KW — 夜間 AI の依頼・予約・結果 (PR3a・2026-09-23)
 *
 * 設計 = 正本『Amazon_SP広告KW自動生成_設計方針_20260922.md』§5「PR3 実装計画 v2 / v2.1 / v2.2」(Codex 計画レビュー R1〜R3)。
 * DB は product-hub の 3 表 (ph_ad_kw_ai_jobs / ph_ad_kw_ai_generations / ph_ad_kw_ai_proposals、db.js)。
 * 画面の API と miniPC の実行役 (PR3b) 用の service-api は router.js。ここは DB とロジックだけ。
 *
 * 守りたいこと:
 *   ① 材料 (packet) は受付時に固定して保存する。claim は常に同じ packet を返す (再 claim で別の材料にならない)
 *   ② AI を呼ぶ前にサーバーで予約する (generation = 永続の予算。1 job 1 回・1 日の上限)。予約後に結果が来なければ
 *      needs_review (成否不明を自動で作り直さない)
 *   ③ 結果の保存・候補・提案記録・job 完了・generation の最終処分・監査は 1 トランザクション。最終処分 (accepted / rejected /
 *      discarded) は戻らない。確定済み + 同じ payload の再送は保存済みの receipt を返す (応答断のあとの再送で二重にしない)
 *   ④ lease が切れたあとに許すのは generation による結果の復旧だけ (reserve / fail / release は lease が有効なときだけ)
 *   ⑤ 観測か AI だけの提案かはサーバーが固定 packet の観測語と照合して決める (AI の申告は使わない)。根拠 ID は packet の obs_id だけ
 *   ⑥ AI の提案は採否を作らない (初期状態は未採用)。既存の候補の採否を戻さない。候補の observed_json (観測記録) には足さない
 *   ⑦ 機能フラグ AD_KW_AI_ENABLED=1 が無ければ、新規受付・claim・reserve を受けない (予約済みの結果の復旧は受ける)
 */
import { createHash, randomBytes } from 'node:crypto';
import { logEvent } from '../db.js';
import { DECISION_MATCH_TYPES, normalizeKeyword } from './ad-keywords-export.js';
import { ASIN_RE } from '../../../lib/asin.js';
import { REQUEST_OPEN_STATUSES, latestDecisionsOf } from './ad-keywords.js';

export const PACKET_VERSION = 1;
export const RULES_VERSION = 'adkw-ai-v1';
export const LEASE_MIN = 40;
export const MAX_RETRIES = 3;
export const MAX_KEYWORDS = 60;
export const MAX_BASIS = 5;
export const REASON_MAX = 200;
export const PACKET_MAX_OBS = 300;
export const PACKET_MAX_BYTES = 60_000;
export const PAYLOAD_MAX_BYTES = 200_000;
export const SPEC_MAX = 40;
export const ADOPTED_MAX = 100;   // 採用語・種・採用 ASIN にも上限 (観測以外で総量を使い切らない — Codex #1429 R1 #3)
export const SEEDS_MAX = 20;
export const ADOPTED_ASINS_MAX = 20;
export const ACTIVE_JOB_STATUSES = ['queued', 'running', 'retry_wait'];
// 再試行してよい失敗 (通信・時間切れ・利用上限・ロック待ち)。それ以外 (課金・認証・モデル不一致・出力不正…) は failed
export const RETRYABLE_CODES = ['network', 'timeout', 'quota', 'cli_failed', 'lock_busy', 'deadline'];
export const JOB_STATUS_JA = {
  queued: '今夜の実行待ち', running: '実行中', retry_wait: '再試行待ち', done: '完了', needs_review: '要確認 (結果が届かないまま止まった)',
  failed: '失敗', cancelled: '取消 (依頼が閉じた)',
};
export const RESULT_KIND_JA = { complete: '全部受理', partial: '一部受理 (不正な候補は棄却)', empty: '提案 0 件', rejected: '全部棄却 (出力が不正)' };

const nowIso = () => new Date().toISOString();
const jstDay = (ms = Date.now()) => new Date(ms + 9 * 3600_000).toISOString().slice(0, 10);
const parseJson = (s, fallback) => { try { const v = JSON.parse(s); return v == null ? fallback : v; } catch (_) { return fallback; } };
/** キーの順に依存しない JSON (hash 用) */
export function canonicalJson(v) {
  if (Array.isArray(v)) return '[' + v.map(canonicalJson).join(',') + ']';
  if (v && typeof v === 'object') return '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canonicalJson(v[k])).join(',') + '}';
  return JSON.stringify(v === undefined ? null : v);
}
export const sha256 = (s) => createHash('sha256').update(s).digest('hex');

export function aiEnabled() { return process.env.AD_KW_AI_ENABLED === '1'; }
/** evidence の CHECK に 'ai' が入っているか (作り直しが失敗していれば AI の受付を止める — R1 P2) */
export function aiSchemaReady(db) {
  const sql = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'ph_ad_kw_evidence'").get()?.sql || '';
  return /'ai'/.test(sql) && !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ph_ad_kw_ai_jobs'").get();
}
export function dailyCap() {
  const n = Number.parseInt(process.env.AD_KW_AI_DAILY_CAP || '', 10);
  return Number.isFinite(n) && n >= 0 ? n : 10;
}

// ─── 固定 packet ───────────────────────────────────────────────────────────────
/**
 * いまの依頼の材料から packet を作る (受付時に 1 回だけ呼び、保存する)。
 * 観測語 = 候補表の語のうち、サジェスト・ABA (ASIN → 語) の取得回で観測されたもの。種・人の編集語・過去の AI 提案・語 → ASIN の自動取得は観測にしない
 */
export function buildPacket(db, draft, requestId) {
  const evidence = new Map(db.prepare('SELECT id, source, seed, status, fetched_at, coverage_json FROM ph_ad_kw_evidence WHERE request_id = ?').all(requestId).map((e) => [e.id, e]));
  const isObsEvidence = (e) => e && (e.source === 'suggest' || (e.source === 'aba' && e.seed !== '*top_asins*')) && e.status !== 'failed';
  const observations = [];
  let omitted = 0;
  // origin では選ばない: AI が先に出した語でも、あとでサジェスト・ABA に観測されれば材料 (観測記録 = observed_json + evidence で判定 — Codex #1429 R1 #1)
  for (const c of db.prepare(`SELECT id, value, value_norm, observed_json FROM ph_ad_kw_candidates WHERE request_id = ? AND kind = 'kw' ORDER BY sort_key, id`).all(requestId)) {
    const obs = parseJson(c.observed_json, []).filter((o) => isObsEvidence(evidence.get(o.evidence_id)));
    if (obs.length === 0) continue;
    if (observations.length >= PACKET_MAX_OBS) { omitted += 1; continue; }
    const evs = obs.map((o) => evidence.get(o.evidence_id));
    const sources = [...new Set(evs.map((e) => e.source))];
    const abaWeek = evs.map((e) => (e.source === 'aba' ? parseJson(e.coverage_json, {}).week_start : null)).find(Boolean) || null;
    observations.push({
      obs_id: 'o' + (observations.length + 1), value: c.value, value_norm: c.value_norm, sources,
      seeds: [...new Set(obs.map((o) => o.seed))].slice(0, 3), fetched_at: evs[0].fetched_at || null, week_start: abaWeek,
    });
  }
  const specs = db.prepare('SELECT id, spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draft.id)
    .slice(0, SPEC_MAX).map((r) => ({ spec_id: 's' + r.id, key: String(r.spec_key || '').slice(0, 40), value: String(r.spec_value || '').slice(0, 200) }))
    .filter((s) => s.key && s.value);
  const dec = latestDecisionsOf(db, requestId);
  const adopted = [], adoptedAsins = [];
  let omittedAdopted = 0;
  for (const c of db.prepare('SELECT id, kind FROM ph_ad_kw_candidates WHERE request_id = ? ORDER BY sort_key, id').all(requestId)) {
    const d = dec.get(c.id);
    if (!d || d.decision !== 'adopt') continue;
    if (c.kind === 'kw') { if (adopted.length < ADOPTED_MAX) adopted.push({ value: String(d.keyword || '').slice(0, 80), match_type: d.match_type }); else omittedAdopted += 1; }
    else if (c.kind === 'asin' && adoptedAsins.length < ADOPTED_ASINS_MAX) adoptedAsins.push(d.keyword);
  }
  const seeds = [...new Set(db.prepare(`SELECT seed FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND status != 'failed' ORDER BY id`).all(requestId).map((r) => String(r.seed).slice(0, 60)))].slice(0, SEEDS_MAX);
  const decisionVersion = db.prepare('SELECT COALESCE(MAX(id), 0) AS v FROM ph_ad_kw_decisions WHERE request_id = ?').get(requestId).v;
  const packet = {
    packet_version: PACKET_VERSION, rules_version: RULES_VERSION,
    product: { name: String(draft.name || '').slice(0, 200), specs },
    seeds, observations, adopted, adopted_asins: adoptedAsins, decision_version: decisionVersion,
    limits: { omitted_observations: omitted, omitted_adopted: omittedAdopted, max_keywords: MAX_KEYWORDS, max_basis: MAX_BASIS },
  };
  // 総量の上限: 超えたら観測を後ろから削る (削った数を残す)。観測以外は上の上限で収まる大きさ
  const observedBeforeTrim = packet.observations.length;
  while (Buffer.byteLength(canonicalJson(packet)) > PACKET_MAX_BYTES && packet.observations.length > 0) {
    packet.observations.pop();
    packet.limits.omitted_observations += 1;
  }
  // 削っても収まらない / 観測が全部削れた = 容量の問題 (材料不足とは別の理由で断る)
  packet.limits.too_large = Buffer.byteLength(canonicalJson(packet)) > PACKET_MAX_BYTES || (observedBeforeTrim > 0 && packet.observations.length === 0);
  return packet;
}
/** stale の判定に使う「いまの材料の版」(商品名・仕様・採否版) */
export function currentInputVersion(db, draft, requestId) {
  const p = buildPacket(db, draft, requestId);
  return sha256(canonicalJson({ product: p.product, decision_version: p.decision_version }));
}
const inputVersionOfPacket = (packet) => sha256(canonicalJson({ product: packet.product, decision_version: packet.decision_version }));

// ─── 受付 (画面) ───────────────────────────────────────────────────────────────
/**
 * AI の依頼を受け付ける (packet を固定)。同じ冪等キー + 同じ packet = 同じ依頼。同じキーで別の packet = 409。
 * @returns {{ok:true, job, reused:boolean}|{code, error}}
 */
export function requestAiJob(db, draft, requestId, { idempotencyKey, actor } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AI の案出しは準備中です (夜間の実行役がまだ入っていません)' };
  if (!aiSchemaReady(db)) return { code: 'ai_schema', error: 'AI 用の表の準備ができていません (サーバーのログを確認してください)' };
  const key = String(idempotencyKey || '').trim();
  if (!key || key.length > 100) return { code: 'bad_key', error: '依頼の識別子がありません (画面を読み直してください)' };
  return db.transaction(() => {
    const req = db.prepare('SELECT * FROM ph_ad_kw_requests WHERE id = ?').get(requestId);
    if (!req || req.draft_id !== draft.id) return { code: 'not_found', error: '依頼がありません' };
    if (!REQUEST_OPEN_STATUSES.includes(req.status)) return { code: 'closed', error: 'この依頼は閉じています' };
    if (Number(draft.own_brand) !== 1) return { code: 'not_found', error: '自社商品ではありません' };
    const packet = buildPacket(db, draft, req.id);
    const packetJson = canonicalJson(packet);
    const packetHash = sha256(packetJson);
    const same = db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE request_id = ? AND idempotency_key = ?').get(req.id, key);
    if (same) {
      if (same.packet_hash !== packetHash) return { code: 'key_conflict', error: '同じ依頼キーで材料が変わっています (画面を読み直してください)' };
      return { ok: true, job: same, reused: true };
    }
    if (packet.limits.too_large) return { code: 'packet_too_large', error: '材料が大きすぎて AI に渡せません (採用語や仕様が多すぎる可能性)。管理者に連絡してください' };
    if (packet.observations.length === 0) return { code: 'no_material', error: '材料がありません (先にサジェストを集めるか、競合 ASIN の注文ワードを引いてください)' };
    const active = db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE request_id = ? AND status IN ('queued', 'running', 'retry_wait')`).get(req.id);
    if (active) return { code: 'active_exists', error: 'AI の依頼はすでに待ち・実行中です', job: active };
    const id = Number(db.prepare(`
      INSERT INTO ph_ad_kw_ai_jobs (request_id, draft_id, idempotency_key, status, packet_json, packet_hash, packet_version, requested_by)
      VALUES (?, ?, ?, 'queued', ?, ?, ?, ?)
    `).run(req.id, draft.id, key, packetJson, packetHash, PACKET_VERSION, actor || null).lastInsertRowid);
    logEvent(db, draft.id, 'ad_kw_ai_requested', `#${req.id} AI 依頼 ${id} (観測 ${packet.observations.length} 語${packet.limits.omitted_observations ? `・上限で ${packet.limits.omitted_observations} 語省略` : ''})`, actor);
    return { ok: true, job: db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(id), reused: false };
  })();
}

/** 要確認 (needs_review) を人が確認済みにする。予約済みの生成の結果があとで届いたら、この job の履歴として受ける */
export function reviewAiJob(db, draft, jobId, actor) {
  return db.transaction(() => {
    const job = db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(jobId);
    if (!job || job.draft_id !== draft.id) return { code: 'not_found', error: 'AI の依頼がありません' };
    if (job.status !== 'needs_review') return { code: 'bad_state', error: '要確認の依頼ではありません' };
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed', reviewed_by = ?, reviewed_at = ?, updated_at = ?, finished_at = COALESCE(finished_at, ?) WHERE id = ? AND status = 'needs_review'`)
      .run(actor || null, nowIso(), nowIso(), nowIso(), job.id);
    logEvent(db, draft.id, 'ad_kw_ai_reviewed', `AI 依頼 ${job.id} を確認済みに (${job.error_code || ''})`, actor);
    return { ok: true };
  })();
}

// ─── 実行役 (service-api) ─────────────────────────────────────────────────────
const jobById = (db, id) => db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(id) || null;
const genOfJob = (db, jobId) => db.prepare('SELECT * FROM ph_ad_kw_ai_generations WHERE job_id = ?').get(jobId) || null;
/** job の親 (依頼と商品) がまだ有効か。無効なら理由 */
function parentInvalid(db, job) {
  const req = db.prepare('SELECT status FROM ph_ad_kw_requests WHERE id = ?').get(job.request_id);
  if (!req || !REQUEST_OPEN_STATUSES.includes(req.status)) return 'request_closed';
  const d = db.prepare('SELECT own_brand FROM product_drafts WHERE id = ?').get(job.draft_id);
  if (!d || Number(d.own_brand) !== 1) return 'not_own_brand';
  return null;
}
const backoffMs = (retries) => 15 * 60_000 * 2 ** Math.max(0, retries - 1);

/** 期限切れの running を回収する (claim / queue の前)。予約あり → needs_review / 予約なし → retry_wait (上限で failed) */
export function recoverExpired(db, now = Date.now()) {
  const nowS = new Date(now).toISOString();
  let n = 0;
  for (const job of db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE status = 'running' AND lease_until < ?`).all(nowS)) {
    const gen = genOfJob(db, job.id);
    if (gen && gen.status === 'reserved') {
      n += db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'needs_review', error_code = 'lease_expired_after_reserve', error = '予約後に結果が届かないまま期限が切れた (成否不明・自動では作り直さない)', lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ? AND status = 'running' AND lease_until < ?`).run(nowS, job.id, nowS).changes;
      continue;
    }
    const retries = job.retries + 1;
    if (retries >= MAX_RETRIES) {
      n += db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed', retries = ?, error_code = 'lease_expired', error = '実行役の期限切れが続いた', lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ? AND status = 'running' AND lease_until < ?`).run(retries, nowS, nowS, job.id, nowS).changes;
    } else {
      n += db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'retry_wait', retries = ?, next_run_at = ?, error_code = 'lease_expired', lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ? AND status = 'running' AND lease_until < ?`).run(retries, new Date(now + backoffMs(retries)).toISOString(), nowS, job.id, nowS).changes;
    }
  }
  return n;
}

/** キューの要約 (実行役の「仕事なし」判定・監視用) */
export function queueSummary(db, now = Date.now()) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    recoverExpired(db, now);
    const c = (sql, ...a) => db.prepare(sql).get(...a).n;
    const oldest = db.prepare(`SELECT MIN(created_at) AS t FROM ph_ad_kw_ai_jobs WHERE status IN ('queued', 'retry_wait')`).get().t;
    return {
      enabled: aiEnabled(),
      claimable: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'queued' OR (status = 'retry_wait' AND next_run_at <= ?)`, nowS),
      running: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'running'`),
      retry_wait: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'retry_wait' AND next_run_at > ?`, nowS),
      needs_review: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'needs_review'`),
      reserved_today: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_generations WHERE reserved_day = ?`, jstDay(now)),
      daily_cap: dailyCap(),
      oldest_wait_min: oldest ? Math.floor((now - Date.parse(oldest)) / 60_000) : null,
    };
  })();
}

/**
 * 1 件取る (lease)。取れなければ {ok:true, job:null}。親が無効な依頼はここで cancelled にして次へ
 * @returns {{ok:true, job:null}|{ok:true, job:{job_id, lease_token, lease_until, packet, packet_hash}}|{code, error}}
 */
export function claimAiJob(db, { runnerRunId, now = Date.now() } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AD_KW_AI_ENABLED が無効です' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    recoverExpired(db, now);
    for (let guard = 0; guard < 50; guard++) {
      const job = db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE status = 'queued' OR (status = 'retry_wait' AND next_run_at <= ?) ORDER BY id LIMIT 1`).get(nowS);
      if (!job) return { ok: true, job: null };
      const bad = parentInvalid(db, job);
      if (bad) {
        db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'cancelled', error_code = ?, updated_at = ?, finished_at = ? WHERE id = ? AND status = ?`).run(bad, nowS, nowS, job.id, job.status);
        continue;
      }
      const token = randomBytes(16).toString('hex');
      const until = new Date(now + LEASE_MIN * 60_000).toISOString();
      const ch = db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'running', lease_token = ?, lease_until = ?, runner_run_id = ?, claims = claims + 1, updated_at = ? WHERE id = ? AND status = ?`)
        .run(token, until, runnerRunId ? String(runnerRunId).slice(0, 80) : null, nowS, job.id, job.status).changes;
      if (ch !== 1) continue;
      return { ok: true, job: { job_id: job.id, lease_token: token, lease_until: until, packet: JSON.parse(job.packet_json), packet_hash: job.packet_hash } };
    }
    return { ok: true, job: null };
  })();
}

/** lease が有効な running の job を返す。無効なら理由 */
function liveLease(db, jobId, leaseToken, nowS) {
  const job = jobById(db, jobId);
  if (!job) return { code: 'not_found', error: 'AI の依頼がありません' };
  if (job.status !== 'running' || !leaseToken || job.lease_token !== String(leaseToken)) return { code: 'lease_lost', error: 'この実行役の lease ではありません (取り直されたか終了済み)' };
  if (!job.lease_until || job.lease_until < nowS) return { code: 'lease_expired', error: 'lease の期限が切れています' };
  return { job };
}

/** AI を呼ぶ前の予約 (1 job 1 回・1 日の上限)。@returns {{ok:true, generation_id}|{code, error}} */
export function reserveGeneration(db, jobId, { leaseToken, model, promptVersion, now = Date.now() } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AD_KW_AI_ENABLED が無効です' };
  const m = String(model || '').trim(), pv = String(promptVersion || '').trim();
  if (!m || m.length > 80 || !pv || pv.length > 80) return { code: 'bad_request', error: 'model と prompt_version が要ります' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    const bad = parentInvalid(db, l.job);
    if (bad) {
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'cancelled', error_code = ?, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`).run(bad, nowS, nowS, l.job.id);
      return { code: 'parent_invalid', error: `依頼が有効ではありません (${bad})` };
    }
    if (genOfJob(db, l.job.id)) return { code: 'already_reserved', error: 'この依頼の AI 呼び出しは予約済みです (1 依頼 1 回)' };
    const day = jstDay(now);
    if (db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_ai_generations WHERE reserved_day = ?').get(day).n >= dailyCap()) {
      return { code: 'daily_cap', error: `今日の AI 呼び出しの上限 (${dailyCap()} 回) に達しました` };
    }
    const gid = Number(db.prepare(`INSERT INTO ph_ad_kw_ai_generations (job_id, lease_token, runner_run_id, status, model, prompt_version, reserved_day, reserved_at) VALUES (?, ?, ?, 'reserved', ?, ?, ?, ?)`)
      .run(l.job.id, l.job.lease_token, l.job.runner_run_id, m, pv, day, nowS).lastInsertRowid);
    return { ok: true, generation_id: gid };
  })();
}

/** 失敗の報告。再試行の可否はサーバーが code で決める。予約済みなら needs_review (成否不明) */
export function failAiJob(db, jobId, { leaseToken, code, message, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  const c = String(code || 'other').slice(0, 40);
  const msg = String(message || '').slice(0, 500);
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    const job = l.job;
    if (genOfJob(db, job.id)) {
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'needs_review', error_code = ?, error = ?, lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`).run(c, msg || '予約後に失敗が報告された (成否不明)', nowS, job.id);
      return { ok: true, status: 'needs_review' };
    }
    const retries = job.retries + 1;
    if (RETRYABLE_CODES.includes(c) && retries < MAX_RETRIES) {
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'retry_wait', retries = ?, next_run_at = ?, error_code = ?, error = ?, lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`)
        .run(retries, new Date(now + backoffMs(retries)).toISOString(), c, msg, nowS, job.id);
      return { ok: true, status: 'retry_wait' };
    }
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed', retries = ?, error_code = ?, error = ?, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`).run(retries, c, msg, nowS, nowS, job.id);
    logEvent(db, job.draft_id, 'ad_kw_ai_failed', `AI 依頼 ${job.id} ${c}`, 'ph-nightly');
    return { ok: true, status: 'failed' };
  })();
}

/** 手放す (予約前だけ queued へ)。予約後は needs_review */
export function releaseAiJob(db, jobId, { leaseToken, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    if (genOfJob(db, l.job.id)) {
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'needs_review', error_code = 'released_after_reserve', error = '予約後に手放された (成否不明)', lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`).run(nowS, l.job.id);
      return { ok: true, status: 'needs_review' };
    }
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'queued', lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`).run(nowS, l.job.id);
    return { ok: true, status: 'queued' };
  })();
}

// ─── 結果の検証と保存 ─────────────────────────────────────────────────────────
/**
 * AI の出力を検証する (純粋・DB なし)。外側が壊れていれば {fatal}。候補ごとに受理 / 棄却 (理由)
 * @returns {{fatal:string|null, accepted:Array, rejected:Array<{index, keyword, reason}>}}
 */
export function validateOutput(output, packet) {
  const obsIds = new Set((packet.observations || []).map((o) => o.obs_id));
  const obsNorms = new Set((packet.observations || []).map((o) => o.value_norm));
  if (!output || typeof output !== 'object' || Array.isArray(output) || !Array.isArray(output.keywords)) return { fatal: 'keywords (配列) がありません', accepted: [], rejected: [] };
  if (output.keywords.length > MAX_KEYWORDS) return { fatal: `提案が多すぎます (${output.keywords.length} > ${MAX_KEYWORDS})`, accepted: [], rejected: [] };
  const accepted = [], rejected = [], seen = new Set();
  output.keywords.forEach((k, index) => {
    const raw = k && typeof k === 'object' ? k.keyword : null;
    const rej = (reason) => rejected.push({ index, keyword: typeof raw === 'string' ? raw.slice(0, 80) : null, reason });
    if (!k || typeof k !== 'object' || typeof raw !== 'string') return rej('bad_shape');
    if (k.kind != null && k.kind !== 'kw') return rej('kind_not_allowed');       // 除外 KW・商品ターゲットは PR3 では受けない
    const value = normalizeKeyword(raw);
    if (!value) return rej('bad_keyword');
    if (ASIN_RE.test(value.toUpperCase())) return rej('asin_not_allowed');
    const norm = value.toLowerCase();
    if (seen.has(norm)) return rej('duplicate');
    const basis = k.basis_obs_ids == null ? [] : k.basis_obs_ids;
    if (!Array.isArray(basis) || basis.length > MAX_BASIS || basis.some((b) => typeof b !== 'string' || !obsIds.has(b))) return rej('bad_basis');
    const reason = k.reason == null ? null : (typeof k.reason === 'string' ? k.reason.replace(/[\x00-\x1f\x7f]/g, ' ').trim().slice(0, REASON_MAX) : undefined);
    if (reason === undefined) return rej('bad_reason');
    const hint = typeof k.match_hint === 'string' && DECISION_MATCH_TYPES.includes(k.match_hint) ? k.match_hint : null;   // 参考値。採用値は人が決める
    seen.add(norm);
    accepted.push({ value, value_norm: norm, basis: [...new Set(basis)], reason: reason || null, match_hint: hint, observed: obsNorms.has(norm) ? 'observed' : 'ai_only' });
  });
  return { fatal: null, accepted, rejected };
}

/**
 * 結果の提出 (generation 単位)。lease が切れていても、予約済みの generation なら受ける (復旧)。
 * @returns {{ok:true, receipt}|{code, error}}
 */
export function submitGenerationResult(db, generationId, { packetHash, output, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  const payload = { packet_hash: String(packetHash || ''), output: output === undefined ? null : output };
  const payloadJson = canonicalJson(payload);
  if (Buffer.byteLength(payloadJson) > PAYLOAD_MAX_BYTES) return { code: 'too_large', error: '結果が大きすぎます' };
  const payloadHash = sha256(payloadJson);
  return db.transaction(() => {
    const gen = db.prepare('SELECT * FROM ph_ad_kw_ai_generations WHERE id = ?').get(generationId);
    if (!gen) return { code: 'not_found', error: '予約がありません' };
    if (gen.status !== 'reserved') {
      if (gen.payload_hash === payloadHash) return { ok: true, receipt: parseJson(gen.receipt_json, null), replay: true };   // 応答断のあとの再送
      return { code: 'already_finalized', error: `この予約は確定済みです (${gen.status})。別の内容は受けません` };
    }
    const job = jobById(db, gen.job_id);
    if (!job) return { code: 'not_found', error: 'AI の依頼がありません' };
    if (payload.packet_hash !== job.packet_hash) return { code: 'packet_mismatch', error: '材料 (packet) の版が違います' };
    // 確定: reserved → 最終処分。以降この generation は戻らない
    const finalize = (status, receipt, discardReason = null) => {
      const ch = db.prepare(`UPDATE ph_ad_kw_ai_generations SET status = ?, payload_json = ?, payload_hash = ?, receipt_json = ?, discard_reason = ?, finalized_at = ? WHERE id = ? AND status = 'reserved'`)
        .run(status, payloadJson, payloadHash, JSON.stringify(receipt), discardReason, nowS, gen.id).changes;
      if (ch !== 1) throw new Error('generation was finalized concurrently');
    };
    const bad = job.status === 'cancelled' ? (job.error_code || 'job_cancelled') : parentInvalid(db, job);
    if (bad) {
      const receipt = { generation_id: gen.id, job_id: job.id, disposition: 'discarded', reason: bad, payload_hash: payloadHash, accepted: 0, rejected: 0 };
      finalize('discarded', receipt, bad);
      if (job.status !== 'cancelled') db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'cancelled', error_code = ?, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = COALESCE(finished_at, ?) WHERE id = ?`).run(bad, nowS, nowS, job.id);
      logEvent(db, job.draft_id, 'ad_kw_ai_discarded', `AI 依頼 ${job.id} の結果を破棄 (${bad})`, 'ph-nightly');
      return { ok: true, receipt };
    }
    const packet = JSON.parse(job.packet_json);
    const v = validateOutput(payload.output, packet);
    if (v.fatal) {
      const receipt = { generation_id: gen.id, job_id: job.id, disposition: 'rejected', reason: v.fatal, payload_hash: payloadHash, accepted: 0, rejected: 0, result_kind: 'rejected' };
      finalize('rejected', receipt);
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed', result_kind = 'rejected', error_code = 'invalid_output', error = ?, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`).run(v.fatal, nowS, nowS, job.id);
      logEvent(db, job.draft_id, 'ad_kw_ai_rejected', `AI 依頼 ${job.id} の出力を棄却 (${v.fatal})`, 'ph-nightly');
      return { ok: true, receipt };
    }
    const resultKind = v.accepted.length === 0 ? (v.rejected.length === 0 ? 'empty' : 'rejected') : (v.rejected.length === 0 ? 'complete' : 'partial');
    const reasons = {};
    for (const r of v.rejected) reasons[r.reason] = (reasons[r.reason] || 0) + 1;
    const evStatus = v.accepted.length === 0 ? 'empty' : (v.rejected.length ? 'partial' : 'success');
    const evId = Number(db.prepare(`
      INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, options_json, coverage_json, raw_json, error, fetched_at, created_by)
      VALUES (?, 'ai', ?, ?, ?, ?, ?, NULL, ?, 'ph-nightly')
    `).run(job.request_id, `ai:job${job.id}`, evStatus, JSON.stringify({ job_id: job.id, generation_id: gen.id, model: gen.model, prompt_version: gen.prompt_version }),
      JSON.stringify({ accepted: v.accepted.length, rejected: v.rejected.length, reasons, result_kind: resultKind }), JSON.stringify(v.rejected), nowS).lastInsertRowid);
    const findCand = db.prepare(`SELECT id FROM ph_ad_kw_candidates WHERE request_id = ? AND kind = 'kw' AND value_norm = ?`);
    const insCand = db.prepare(`
      INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key)
      VALUES (?, 'kw', ?, ?, 'ai', ?, '[]', 0, ?)
    `);
    const insProp = db.prepare(`
      INSERT INTO ph_ad_kw_ai_proposals (job_id, generation_id, candidate_id, value, value_norm, basis_obs_ids, reason, match_hint, observed)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    let newCands = 0;
    v.accepted.forEach((a, i) => {
      let cand = findCand.get(job.request_id, a.value_norm);
      if (!cand) {
        const cid = Number(insCand.run(job.request_id, a.value, a.value_norm, evId, `ai|${String(job.id).padStart(8, '0')}|${String(i).padStart(3, '0')}`).lastInsertRowid);
        cand = { id: cid };
        newCands += 1;
      }
      insProp.run(job.id, gen.id, cand.id, a.value, a.value_norm, JSON.stringify(a.basis), a.reason, a.match_hint, a.observed);
    });
    const receipt = { generation_id: gen.id, job_id: job.id, disposition: v.accepted.length || !v.rejected.length ? 'accepted' : 'rejected', payload_hash: payloadHash,
      accepted: v.accepted.length, rejected: v.rejected.length, new_candidates: newCands, result_kind: resultKind, reasons };
    finalize(receipt.disposition, receipt);
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'done', result_kind = ?, accepted = ?, rejected = ?, error_code = NULL, error = NULL, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`)
      .run(resultKind, v.accepted.length, v.rejected.length, nowS, nowS, job.id);
    logEvent(db, job.draft_id, 'ad_kw_ai_result', `AI 依頼 ${job.id}: 受理 ${v.accepted.length} (新しい候補 ${newCands}) / 棄却 ${v.rejected.length}`, 'ph-nightly');
    return { ok: true, receipt };
  })();
}

// ─── 画面の状態 ────────────────────────────────────────────────────────────────
/** stateForDraft に載せる AI の状態。proposals = 候補 id → 最新の提案 (理由・観測・根拠の語) */
export function aiStateFor(db, draft, request) {
  const enabled = aiEnabled(), schema = aiSchemaReady(db);
  const base = { enabled: enabled && schema, reason_disabled: !enabled ? '準備中 (夜間の実行役がまだ入っていません)' : (!schema ? '表の準備ができていません' : null),
    jobs: [], active: null, can_request: false, proposals: {}, labels: { status: JOB_STATUS_JA, result_kind: RESULT_KIND_JA } };
  if (!request || !schema) return base;
  let now = null;
  try { now = buildPacket(db, draft, request.id); } catch (_) { now = null; }
  const productNow = now ? sha256(canonicalJson(now.product)) : null;
  const jobs = db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE request_id = ? ORDER BY id DESC LIMIT 5').all(request.id).map((j) => {
    const packet = parseJson(j.packet_json, {});
    return {
      id: j.id, status: j.status, status_ja: JOB_STATUS_JA[j.status] || j.status, result_kind: j.result_kind, result_kind_ja: j.result_kind ? RESULT_KIND_JA[j.result_kind] : null,
      accepted: j.accepted, rejected: j.rejected, error_code: j.error_code, error: j.error, created_at: j.created_at, finished_at: j.finished_at, requested_by: j.requested_by,
      observations: (packet.observations || []).length, omitted: packet.limits?.omitted_observations || 0,
      // 旧材料 = 商品情報 (名前・仕様) か採否版が、頼んだときから変わった。採否版は画面でも比べ直す (採否の保存のたびに — Codex #1429 R1 #5)
      stale_product: productNow != null && sha256(canonicalJson(packet.product || {})) !== productNow,
      packet_decision_version: packet.decision_version ?? null,
      stale: now != null && inputVersionOfPacket(packet) !== inputVersionOfPacket(now), reviewed_by: j.reviewed_by,
    };
  });
  const active = jobs.find((j) => ACTIVE_JOB_STATUSES.includes(j.status)) || null;
  const obsCount = now ? now.observations.length : 0;   // いまの観測語 (観測記録で判定)
  const proposals = {};
  const obsValueByJob = new Map();
  for (const p of db.prepare(`
    SELECT p.*, j.packet_json FROM ph_ad_kw_ai_proposals p JOIN ph_ad_kw_ai_jobs j ON j.id = p.job_id
    WHERE j.request_id = ? ORDER BY p.job_id, p.id
  `).all(request.id)) {
    // 新しい依頼 (job id が大きい) の提案を表示する。古い依頼の結果が遅れて届いても上書きしない (履歴は表に残る — Codex #1429 R1 #2)
    if (!obsValueByJob.has(p.job_id)) obsValueByJob.set(p.job_id, new Map((parseJson(p.packet_json, {}).observations || []).map((o) => [o.obs_id, o.value])));
    const values = obsValueByJob.get(p.job_id);
    const prev = proposals[p.candidate_id];
    proposals[p.candidate_id] = { job_id: p.job_id, reason: p.reason, observed: p.observed, match_hint: p.match_hint,
      basis: parseJson(p.basis_obs_ids, []).map((id) => values.get(id)).filter(Boolean), count: (prev ? prev.count : 0) + 1,
      job_stale: now != null && inputVersionOfPacket(parseJson(p.packet_json, {})) !== inputVersionOfPacket(now) };
  }
  return { ...base, jobs, active, can_request: base.enabled && !active && REQUEST_OPEN_STATUSES.includes(request.status) && obsCount > 0 && !(now && now.limits.too_large),
    too_large: !!(now && now.limits.too_large), proposals };
}
