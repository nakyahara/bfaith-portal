/**
 * SP広告 検索KW — 夜間 AI の依頼・予約・結果 (PR3a・2026-09-23) + おまかせ全自動 (PR3c・2026-09-26)
 *
 * 設計 = 正本『Amazon_SP広告KW自動生成_設計方針_20260922.md』§5「PR3 実装計画 v2 / v2.1 / v2.2」「PR3c 計画 v1〜v3」(Codex 計画レビュー)。
 * DB は product-hub の 4 表 (ph_ad_kw_ai_jobs / _generations / _proposals / _probes、db.js)。
 * 画面の API と miniPC の実行役 (PR3b / PR3c-2) 用の service-api は router.js。ここは DB とロジックだけ。
 *
 * job の種類:
 *   manual = 人が「AI に頼む」を押した 1 段 (受付時に材料 packet を固定 → 最終案)
 *   auto   = 夜に自社商品を自動で受け付けた 2 段 (中原さん 9/26「Amazon のタイトルだけ渡してる」「自社商品は全部自動」)。
 *            stage: seeds (商品情報から AI が種 KW) → collecting (Render がサジェスト・ABA を 1 回 1 照会で集める = probe) → final (最終案)
 *
 * 守りたいこと:
 *   ① 材料 (packet) は段ごとに固定して保存する。claim は常に同じ packet を返す (再 claim で別の材料にならない)
 *   ② AI を呼ぶ前にサーバーで予約する (generation = 永続の予算。段ごとに 1 回・1 日の上限)。予約後に結果が来なければ
 *      needs_review (成否不明を自動で作り直さない)。予約の判定は「いまの段」の generation だけを見る
 *   ③ 結果の保存・候補・提案記録・job の遷移・generation の最終処分・監査は 1 トランザクション。最終処分は戻らない。
 *      確定済み + 同じ payload の再送は保存済みの receipt を返す
 *   ④ lease が切れたあとに許すのは generation による結果の復旧だけ (reserve / fail / release / collect / finalize は lease が有効なときだけ)
 *   ⑤ 観測か AI だけの提案かはサーバーが固定 packet の観測語と照合して決める。根拠 ID は packet の obs_id だけ
 *   ⑥ AI の提案は採否を作らない (初期状態は未採用)。既存の候補の採否を戻さない
 *   ⑦ 機能フラグ AD_KW_AI_ENABLED=1 が無ければ、新規受付・claim・reserve・collect を受けない (予約済みの結果の復旧は受ける)
 *   ⑧ おまかせの材料は probe (job 専用の下書き) にだけ貯め、候補・採否 (競合 ASIN の自動採用)・evidence は最終案の保存と同じ txn で書く
 *      (人の収集・採否と衝突しない・再送で増えない)。probe の保存は job の lease と probe の run_token の両方が一致したときだけ
 */
import { createHash, randomBytes } from 'node:crypto';
import { logEvent, adKwAiTablesV2 } from '../db.js';
import { DECISION_MATCH_TYPES, normalizeKeyword } from './ad-keywords-export.js';
import { ASIN_RE } from '../../../lib/asin.js';
import {
  REQUEST_OPEN_STATUSES, latestDecisionsOf, openRequestOf, normalizeSeed, productSnapshotOf, inputHashOf,
  applySuggestOutcome, applyAbaOutcome, applyAutoAsinsOutcome, rankCompetitorAsins, AUTO_ASINS_PER_RUN,
} from './ad-keywords.js';

export const PACKET_VERSION = 1;
export const RULES_VERSION = 'adkw-ai-v1';
export const SEED_RULES_VERSION = 'adkw-auto-seeds-v1';
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
// おまかせ (PR3c)
export const AUTO_SEEDS_REQUEST_MIN = 3; // AI に頼む数 = 3〜5
export const AUTO_SEEDS_MIN = 1;         // 受ける数 = 不正な種を除いて 1 つ以上 (1〜2 でも材料集めは進める — Codex #1467 R1 Low)
export const AUTO_SEEDS_MAX = 5;         // 種 1 つ ≒ 47 回のサジェスト。5 種 ≒ 人が目視していた量
export const AUTO_TERMS_MAX = 20;        // 競合を探す語 (観測語から種ごとに順番に)
export const AUTO_ASIN_LOOKUPS = 3;      // 競合の検索語を引く ASIN (上位 3)
export const PROBE_RUN_MS = 2 * 60_000;  // 1 照会の実行権の期限 (サジェスト 45 秒・ABA 30 秒 + 余裕)
export const PROBE_MAX_NIGHTS = 3;       // 同じ照会が 3 晩失敗したら打ち切り
export const RETRY_LATER_MS = 12 * 3600_000;   // 再試行できる取得失敗 → その晩はやめて次の晩
export const AUTO_ACTOR_AI = 'auto:ai';  // おまかせが書いた材料 (evidence) の記録者
export const ACTIVE_JOB_STATUSES = ['queued', 'running', 'retry_wait'];
// 再試行してよい失敗 (通信・時間切れ・利用上限・ロック待ち)。それ以外 (課金・認証・モデル不一致・出力不正…) は failed
export const RETRYABLE_CODES = ['network', 'timeout', 'quota', 'cli_failed', 'lock_busy', 'deadline'];
export const JOB_STATUS_JA = {
  queued: '今夜の実行待ち', running: '実行中', retry_wait: '再試行待ち', done: '完了', needs_review: '要確認 (結果が届かないまま止まった)',
  needs_input: '材料が見つからない (種を入れて集めてください)', failed: '失敗', cancelled: '取消 (依頼が閉じた)',
};
export const STAGE_JA = { seeds: '種 KW を考える', collecting: 'サジェスト・ABA を集める', final: '最終案を作る' };
export const RESULT_KIND_JA = { complete: '全部受理', partial: '一部受理 (不正な候補は棄却)', empty: '提案 0 件', rejected: '全部棄却 (出力が不正)' };
const PROBE_FINAL = ['ok', 'empty', 'incomplete', 'skipped', 'gave_up'];

const nowIso = () => new Date().toISOString();
const jstDay = (ms = Date.now()) => new Date(ms + 9 * 3600_000).toISOString().slice(0, 10);
/** JST のその日の 0 時 (UTC の ISO) */
const jstDayStartIso = (ms = Date.now()) => new Date(Date.parse(jstDay(ms) + 'T00:00:00Z') - 9 * 3600_000).toISOString();
const parseJson = (s, fallback) => { try { const v = JSON.parse(s); return v == null ? fallback : v; } catch (_) { return fallback; } };
/** キーの順に依存しない JSON (hash 用) */
export function canonicalJson(v) {
  if (Array.isArray(v)) return '[' + v.map(canonicalJson).join(',') + ']';
  if (v && typeof v === 'object') return '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canonicalJson(v[k])).join(',') + '}';
  return JSON.stringify(v === undefined ? null : v);
}
export const sha256 = (s) => createHash('sha256').update(s).digest('hex');

export function aiEnabled() { return process.env.AD_KW_AI_ENABLED === '1'; }
/** evidence の CHECK に 'ai' が入っていて、AI の表が PR3c の定義か (作り直しが失敗していれば新規を止める — R1 P2 / PR3c R2 ④) */
export function aiSchemaReady(db) {
  const sql = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'ph_ad_kw_evidence'").get()?.sql || '';
  return /'ai'/.test(sql) && !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ph_ad_kw_ai_jobs'").get() && adKwAiTablesV2(db);
}
export function dailyCap() {
  const n = Number.parseInt(process.env.AD_KW_AI_DAILY_CAP || '', 10);
  return Number.isFinite(n) && n >= 0 ? n : 20;   // おまかせ 1 件 = 2 回 (種 + 最終案)
}
// おまかせの対象 (中原さん 2026-09-26「今ある中は chlorellap だけ。今後、新商品に入ってくるものだけ」)。
//   = NE コードが AUTO_TARGET_NE_CODES の商品 + ポータルで AUTO_TARGET_SINCE 以降に登録した商品 (Notion の既存カードの取り込みは古い商品なので数えない)。
//   対象外の商品も、画面の「おまかせで作る」で人が頼める (rerunAuto)
export const AUTO_TARGET_SINCE_DEFAULT = '2026-09-26T08:15:00.000Z';   // 2026-09-26 17:15 JST
export const AUTO_TARGET_NE_CODES = ['chlorellap'];
export function autoTargetSince() {
  const v = String(process.env.AD_KW_AUTO_SINCE || '').trim();
  return v && Number.isFinite(Date.parse(v)) ? new Date(Date.parse(v)).toISOString() : AUTO_TARGET_SINCE_DEFAULT;
}
/** おまかせの自動受付の対象か (1 商品) */
export function isAutoTarget(draft) {
  if (!draft) return false;
  if (AUTO_TARGET_NE_CODES.includes(String(draft.ne_code || '').trim().toLowerCase())) return true;
  return draft.source === 'portal' && String(draft.created_at || '') >= autoTargetSince();
}
export function autoDailyCap() {
  const n = Number.parseInt(process.env.AD_KW_AUTO_DAILY || '', 10);
  return Number.isFinite(n) && n >= 0 ? n : 3;
}

// ─── 固定 packet ───────────────────────────────────────────────────────────────
const specsOf = (db, draftId) => db.prepare('SELECT id, spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draftId)
  .slice(0, SPEC_MAX).map((r) => ({ spec_id: 's' + r.id, key: String(r.spec_key || '').slice(0, 40), value: String(r.spec_value || '').slice(0, 200) }))
  .filter((s) => s.key && s.value);
/** 採否版 = その依頼の採否の最新 id */
const decisionVersionOf = (db, requestId) => db.prepare('SELECT COALESCE(MAX(id), 0) AS v FROM ph_ad_kw_decisions WHERE request_id = ?').get(requestId).v;
/** 採用中の語と ASIN (packet に入れる形) */
function adoptedOf(db, requestId) {
  const dec = latestDecisionsOf(db, requestId);
  const adopted = [], adoptedAsins = [];
  let omittedAdopted = 0;
  for (const c of db.prepare('SELECT id, kind FROM ph_ad_kw_candidates WHERE request_id = ? ORDER BY sort_key, id').all(requestId)) {
    const d = dec.get(c.id);
    if (!d || d.decision !== 'adopt') continue;
    if (c.kind === 'kw') { if (adopted.length < ADOPTED_MAX) adopted.push({ value: String(d.keyword || '').slice(0, 80), match_type: d.match_type }); else omittedAdopted += 1; }
    else if (c.kind === 'asin' && adoptedAsins.length < ADOPTED_ASINS_MAX) adoptedAsins.push(d.keyword);
  }
  return { adopted, adoptedAsins, omittedAdopted };
}
/** 総量の上限: 超えたら観測を後ろから削る。削っても収まらない / 観測が全部削れた = too_large */
function trimPacket(packet) {
  const observedBeforeTrim = packet.observations.length;
  while (Buffer.byteLength(canonicalJson(packet)) > PACKET_MAX_BYTES && packet.observations.length > 0) {
    packet.observations.pop();
    packet.limits.omitted_observations += 1;
  }
  // true は false より短いので、印を立てても大きさは増えない。返す packet は必ず上限以内か too_large
  packet.limits.too_large = Buffer.byteLength(canonicalJson(packet)) > PACKET_MAX_BYTES || (observedBeforeTrim > 0 && packet.observations.length === 0);
  return packet;
}

/**
 * いまの依頼の材料から packet を作る (manual の受付時に 1 回だけ呼び、保存する)。
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
  const { adopted, adoptedAsins, omittedAdopted } = adoptedOf(db, requestId);
  const seeds = [...new Set(db.prepare(`SELECT seed FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND status != 'failed' ORDER BY id`).all(requestId).map((r) => String(r.seed).slice(0, 60)))].slice(0, SEEDS_MAX);
  const packet = {
    packet_version: PACKET_VERSION, rules_version: RULES_VERSION,
    product: { name: String(draft.name || '').slice(0, 200), specs: specsOf(db, draft.id) },
    seeds, observations, adopted, adopted_asins: adoptedAsins, decision_version: decisionVersionOf(db, requestId),
    // too_large は最初から入れておく (あとで足すと、その分だけ上限を超えうる — Codex #1429 R2 #2)
    limits: { omitted_observations: omitted, omitted_adopted: omittedAdopted, max_keywords: MAX_KEYWORDS, max_basis: MAX_BASIS, too_large: false },
  };
  return trimPacket(packet);
}
/** stale の判定に使う「いまの材料の版」(商品名・仕様・採否版) */
export function currentInputVersion(db, draft, requestId) {
  const p = buildPacket(db, draft, requestId);
  return sha256(canonicalJson({ product: p.product, decision_version: p.decision_version }));
}
const inputVersionOf = (product, decisionVersion) => sha256(canonicalJson({ product, decision_version: decisionVersion }));
/**
 * job の「採否版の実効値」。おまかせは最終保存の txn で自分で競合 ASIN を採用する (採否版が進む) ので、
 * packet の版と自分の採用の間に他の採否が無ければ、自分の採用の最後を版とみなす (自分の採用で旧材料にしない — PR3c R2 ⑥)。
 * 間に人の採否があれば packet の版のまま (= 旧材料として出る)
 */
function effectiveDecisionVersion(db, job, packet) {
  const dv = packet?.decision_version ?? null;
  if (dv == null || job.own_decision_min == null || job.own_decision_max == null) return dv;
  const between = db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_decisions WHERE request_id = ? AND id > ? AND id < ?').get(job.request_id, dv, job.own_decision_min).n;
  return between === 0 ? job.own_decision_max : dv;
}

// ─── 受付 (画面・manual) ─────────────────────────────────────────────────────
/**
 * AI の依頼を受け付ける (packet を固定)。同じ冪等キー + 同じ packet = 同じ依頼。同じキーで別の packet = 409。
 * @returns {{ok:true, job, reused:boolean}|{code, error}}
 */
export function requestAiJob(db, draft, requestId, { idempotencyKey, actor } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AI の案出しは準備中です (夜間の実行役がまだ入っていません)' };
  if (!aiSchemaReady(db)) return { code: 'ai_schema', error: 'AI 用の表の準備ができていません (サーバーのログを確認してください)' };
  const key = String(idempotencyKey || '').trim();
  if (!key || key.length > 100 || key.startsWith('auto:')) return { code: 'bad_key', error: '依頼の識別子がありません (画面を読み直してください)' };
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
    if (packet.observations.length === 0) return { code: 'no_material', error: '材料がありません (先にサジェストを集めるか、競合 ASIN の検索語を引いてください)' };
    const active = db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE request_id = ? AND status IN ('queued', 'running', 'retry_wait')`).get(req.id);
    if (active) return { code: 'active_exists', error: active.mode === 'auto' ? 'おまかせ (夜の自動) がこの依頼で動いています。終わってから頼んでください' : 'AI の依頼はすでに待ち・実行中です', job: active };
    const id = Number(db.prepare(`
      INSERT INTO ph_ad_kw_ai_jobs (request_id, draft_id, idempotency_key, mode, stage, status, packet_json, packet_hash, packet_version, requested_by)
      VALUES (?, ?, ?, 'manual', 'final', 'queued', ?, ?, ?, ?)
    `).run(req.id, draft.id, key, packetJson, packetHash, PACKET_VERSION, actor || null).lastInsertRowid);
    logEvent(db, draft.id, 'ad_kw_ai_requested', `#${req.id} AI 依頼 ${id} (観測 ${packet.observations.length} 語${packet.limits.omitted_observations ? `・上限で ${packet.limits.omitted_observations} 語省略` : ''})`, actor);
    return { ok: true, job: db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(id), reused: false };
  })();
}

/**
 * 止まった AI の依頼を人が「確認済み」にする。
 * needs_review (予約後に結果が届かないまま止まった) → failed (確認済み)。failed / needs_input → 状態はそのまま、確認済みの印だけ (監視の警告が消える — PR3c R2 ⑦)。
 * 予約済みの生成の結果があとで届いたら、この job の履歴として受ける (確認済みの job は再開しない)
 */
export function reviewAiJob(db, draft, jobId, actor) {
  return db.transaction(() => {
    const job = db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(jobId);
    if (!job || job.draft_id !== draft.id) return { code: 'not_found', error: 'AI の依頼がありません' };
    if (!['needs_review', 'failed', 'needs_input'].includes(job.status) || job.reviewed_by) return { code: 'bad_state', error: '確認が要る依頼ではありません' };
    const next = job.status === 'needs_review' ? 'failed' : job.status;
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = ?, reviewed_by = ?, reviewed_at = ?, updated_at = ?, finished_at = COALESCE(finished_at, ?) WHERE id = ? AND status = ?`)
      .run(next, actor || 'unknown', nowIso(), nowIso(), nowIso(), job.id, job.status);
    logEvent(db, draft.id, 'ad_kw_ai_reviewed', `AI 依頼 ${job.id} を確認済みに (${job.status}${job.error_code ? ' ' + job.error_code : ''})`, actor);
    return { ok: true };
  })();
}

// ─── おまかせの受付 (auto-enqueue / やり直す) ─────────────────────────────────
/** 楽天タイトル (ハブの AI 原稿) */
const rakutenTitleOf = (db, draftId) => {
  const r = db.prepare(`SELECT content FROM draft_ai_outputs WHERE draft_id = ? AND kind = 'rakuten_title'`).get(draftId);
  return r && r.content ? String(r.content).replace(/[\x00-\x1f\x7f]/g, ' ').trim().slice(0, 200) : null;
};
/** おまかせの種の packet (受付時に固定) */
/** 商品の ASIN (ASIN の欄 → 無ければ Amazon の URL の /dp/ASIN) */
export function asinOfDraft(draft) {
  const a = String(draft?.asin || '').trim().toUpperCase();
  if (ASIN_RE.test(a)) return a;
  const m = /\/(?:dp|gp\/product)\/([A-Z0-9]{10})(?:[/?#]|$)/i.exec(String(draft?.amazon_url || ''));
  return m && ASIN_RE.test(m[1].toUpperCase()) ? m[1].toUpperCase() : null;
}
/**
 * おまかせの種の packet (受付時に固定)。amazon = fetchAmazonCatalog の結果 (タイトル・ブランド・カテゴリ・箇条書き・説明)。
 * 中原さん「Amazon のリンク先の情報も考慮して」(2026-09-26) → 種と最終案の両方の材料 (最終 packet は product_extra をそのまま持つ)
 */
export function buildSeedPacket(db, draft, { amazon = null, amazonTitle = null } = {}) {
  const am = amazon && amazon.title ? amazon : (amazonTitle ? { title: amazonTitle } : null);
  return {
    packet_version: PACKET_VERSION, rules_version: SEED_RULES_VERSION,
    product: { name: String(draft.name || '').slice(0, 200), specs: specsOf(db, draft.id) },
    product_extra: {
      amazon_title: am ? String(am.title).slice(0, 300) : null,
      amazon_brand: am && am.brand ? String(am.brand).slice(0, 100) : null,
      amazon_category: am && am.category ? String(am.category).slice(0, 100) : null,
      amazon_bullets: am && Array.isArray(am.bullets) ? am.bullets.slice(0, 10).map((b) => String(b).slice(0, 300)) : [],
      amazon_description: am && am.description ? String(am.description).slice(0, 1500) : null,
      rakuten_title: rakutenTitleOf(db, draft.id), asin: asinOfDraft(draft),
    },
    limits: { seeds_request_min: AUTO_SEEDS_REQUEST_MIN, seeds_max: AUTO_SEEDS_MAX, seeds_accept_min: AUTO_SEEDS_MIN },
  };
}
/** おまかせの対象か (受付の txn の中で再確認する) */
function autoEligible(db, draftId, round, { requireTarget = false } = {}) {
  const d = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!d || Number(d.own_brand) !== 1 || d.status === 'excluded') return { skip: 'not_eligible' };
  if (requireTarget && !isAutoTarget(d)) return { skip: 'not_target' };
  if (db.prepare(`SELECT 1 FROM ph_ad_kw_ai_jobs WHERE draft_id = ? AND mode = 'auto' AND auto_round >= ?`).get(draftId, round)) return { skip: 'already' };
  const open = openRequestOf(db, draftId);
  if (open && db.prepare(`SELECT 1 FROM ph_ad_kw_ai_jobs WHERE request_id = ? AND status IN ('queued', 'running', 'retry_wait')`).get(open.id)) return { skip: 'active_exists' };
  return { draft: d, open };
}
/** 受付の本体 (1 商品・1 txn)。依頼 = 開いている依頼があればそれ / 無ければ auto:<draft>:<round> で新しく作る (PR3c R2 ⑤) */
function insertAutoJob(db, draftId, round, { amazon, idempotencyKey, actor, now = Date.now(), dailyLimit = null, requireTarget = false }) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    // 夜の自動受付は、保存の txn の中で残り枠を数え直す (タイトル待ちの間に別の受付が枠を使っていても超えない — Codex #1467 R1 #1)
    if (dailyLimit != null && autoRoom(db, now, dailyLimit) <= 0) return { skip: 'daily_cap' };
    const e = autoEligible(db, draftId, round, { requireTarget });
    if (e.skip) return e;
    let req = e.open;
    if (!req) {
      const snap = productSnapshotOf(e.draft);
      const reqKey = `auto:${draftId}:${round}`;
      const same = db.prepare('SELECT * FROM ph_ad_kw_requests WHERE draft_id = ? AND idempotency_key = ?').get(draftId, reqKey);
      if (same) return { skip: 'request_closed' };   // 同じ round の依頼が閉じている (取消・置き換え) = 作り直さない
      const rid = Number(db.prepare(`
        INSERT INTO ph_ad_kw_requests (draft_id, idempotency_key, product_snapshot_json, input_hash, requested_by) VALUES (?, ?, ?, ?, ?)
      `).run(draftId, reqKey, JSON.stringify(snap), inputHashOf(snap), actor || AUTO_ACTOR_AI).lastInsertRowid);
      req = db.prepare('SELECT * FROM ph_ad_kw_requests WHERE id = ?').get(rid);
      logEvent(db, draftId, 'ad_kw_request', `#${rid} (おまかせ)`, actor || AUTO_ACTOR_AI);
    }
    const seedPacket = buildSeedPacket(db, e.draft, { amazon });
    const seedJson = canonicalJson(seedPacket);
    const id = Number(db.prepare(`
      INSERT INTO ph_ad_kw_ai_jobs (request_id, draft_id, idempotency_key, mode, auto_round, stage, status, seed_packet_json, seed_packet_hash, packet_version, requested_by, created_at, updated_at)
      VALUES (?, ?, ?, 'auto', ?, 'seeds', 'queued', ?, ?, ?, ?, ?, ?)
    `).run(req.id, draftId, idempotencyKey, round, seedJson, sha256(seedJson), PACKET_VERSION, actor || AUTO_ACTOR_AI, nowS, nowS).lastInsertRowid);
    logEvent(db, draftId, 'ad_kw_ai_auto', `#${req.id} おまかせ ${id} (回 ${round}${amazon ? `・Amazon の商品ページ (箇条書き ${(amazon.bullets || []).length}${amazon.description ? '・説明あり' : ''})` : '・Amazon の商品ページなし'})`, actor || AUTO_ACTOR_AI);
    return { job: db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(id) };
  })();
}
/** Amazon の商品ページの情報 (取れなければ null・受付は止めない) */
async function catalogWithin(titleFetcher, asin, budgetLeftMs) {
  if (!titleFetcher || !asin || budgetLeftMs < 3_000) return null;
  try {
    const r = await titleFetcher(asin, { timeoutMs: Math.min(15_000, budgetLeftMs) });
    return r && r.ok && r.title ? r : null;
  } catch (_) { return null; }
}

/**
 * 自動受付の残り枠 = min(1 日の上限 − 今日 (JST) 受け付けた数, 1 日の上限 − 動いている (待ち・実行中・再試行待ち) おまかせの数)。
 * 後ろの条件 = 1 晩で終わらない分が積み上がり続けない (受付 3 件 ≠ 完了 3 件)
 */
function autoRoom(db, now, cap) {
  const today = db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE mode = 'auto' AND created_at >= ?`).get(jstDayStartIso(now)).n;
  const active = db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE mode = 'auto' AND status IN ('queued', 'running', 'retry_wait')`).get().n;
  return Math.min(cap - today, cap - active);
}

/**
 * 夜の自動受付 (実行役が run-ph-generate.ps1 の広告の段の最初に呼ぶ)。自社商品で、おまかせが 1 度も無いものを新しい順に。
 * 1 日の上限 (AD_KW_AUTO_DAILY・既定 3) はその日 (JST) に作ったおまかせの数でサーバーが数える → 同じ夜の再送は残り枠まで追加で受け付ける (PR3c R2 ⑧)。
 * Amazon タイトルは txn の外で先に取る (1 件 15 秒・全体 40 秒)。保存の txn で対象条件を再確認する
 * @returns {Promise<{ok:true, enqueued:Array<{draft_id, job_id, title:boolean}>, skipped:Array, today:number, cap:number}|{code, error}>}
 */
export async function autoEnqueue(db, { titleFetcher = null, now = Date.now(), budgetMs = 40_000 } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AD_KW_AI_ENABLED が無効です' };
  if (!aiSchemaReady(db)) return { code: 'ai_schema', error: 'AI 用の表の準備ができていません' };
  const cap = autoDailyCap();
  const since = jstDayStartIso(now);
  const todayCount = () => db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE mode = 'auto' AND created_at >= ?`).get(since).n;
  const remaining = autoRoom(db, now, cap);
  const enqueued = [], skipped = [];
  if (remaining <= 0) return { ok: true, enqueued, skipped, today: todayCount(), cap };
  // 対象 = 自社商品・除外でない・おまかせが 1 度も無い・その依頼で AI が動いていない。新しい順 (id の大きい順・固定)
  const targets = db.prepare(`
    SELECT d.id, d.asin, d.amazon_url FROM product_drafts d
    WHERE d.own_brand = 1 AND d.status != 'excluded'
      AND (LOWER(TRIM(d.ne_code)) IN (${AUTO_TARGET_NE_CODES.map(() => '?').join(', ')}) OR (d.source = 'portal' AND d.created_at >= ?))
      AND NOT EXISTS (SELECT 1 FROM ph_ad_kw_ai_jobs j WHERE j.draft_id = d.id AND j.mode = 'auto')
      AND NOT EXISTS (SELECT 1 FROM ph_ad_kw_ai_jobs j JOIN ph_ad_kw_requests r ON r.id = j.request_id
                      WHERE j.draft_id = d.id AND r.status IN ('collecting', 'review_ready') AND j.status IN ('queued', 'running', 'retry_wait'))
    ORDER BY d.id DESC LIMIT ?
  `).all(...AUTO_TARGET_NE_CODES, autoTargetSince(), remaining);
  const t0 = Date.now();
  for (const t of targets) {
    if (autoRoom(db, now, cap) <= 0) break;
    const amazon = await catalogWithin(titleFetcher, asinOfDraft(t), budgetMs - (Date.now() - t0));
    const r = insertAutoJob(db, t.id, 1, { amazon, idempotencyKey: 'auto:1', actor: AUTO_ACTOR_AI, now, dailyLimit: cap, requireTarget: true });
    if (r.skip === 'daily_cap') break;
    if (r.job) enqueued.push({ draft_id: t.id, job_id: r.job.id, title: !!amazon });
    else skipped.push({ draft_id: t.id, reason: r.skip });
  }
  return { ok: true, enqueued, skipped, today: todayCount(), cap };
}

/**
 * 画面の「おまかせをやり直す」。前のおまかせが終わっている (完了・失敗・材料なし・取消・要確認) ときだけ、回を 1 つ進めて受け付ける。
 * 冪等キー = 画面の操作ごと (同じ操作の再送は同じ job — PR3c R2 ⑤)。1 日の上限には数えない (人の操作)
 */
export async function rerunAuto(db, draft, { idempotencyKey, actor, titleFetcher = null, now = Date.now() } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AI の案出しは準備中です' };
  if (!aiSchemaReady(db)) return { code: 'ai_schema', error: 'AI 用の表の準備ができていません' };
  const key = String(idempotencyKey || '').trim();
  if (!key || key.length > 80) return { code: 'bad_key', error: '操作の識別子がありません (画面を読み直してください)' };
  const jobKey = 'auto:rerun:' + key;
  const same = db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE draft_id = ? AND mode = 'auto' AND idempotency_key = ?`).get(draft.id, jobKey);
  if (same) return { ok: true, job: same, reused: true };
  if (Number(draft.own_brand) !== 1) return { code: 'not_found', error: '自社商品ではありません' };
  const last = db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE draft_id = ? AND mode = 'auto' ORDER BY auto_round DESC LIMIT 1`).get(draft.id);
  if (last && ACTIVE_JOB_STATUSES.includes(last.status)) return { code: 'active_exists', error: 'おまかせが動いています (終わってからやり直してください)' };
  const round = (last ? last.auto_round : 0) + 1;
  const amazon = await catalogWithin(titleFetcher, asinOfDraft(draft), 15_000);
  const r = insertAutoJob(db, draft.id, round, { amazon, idempotencyKey: jobKey, actor, now });
  if (r.job) return { ok: true, job: r.job, reused: false };
  // 別の操作が先に同じ回を作った (二重押し・別タブ)
  const again = db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE draft_id = ? AND mode = 'auto' AND idempotency_key = ?`).get(draft.id, jobKey);
  if (again) return { ok: true, job: again, reused: true };
  const msg = { active_exists: 'この商品の依頼で AI が動いています (終わってからやり直してください)', already: '別の操作ですでにやり直しを受け付けました (画面を読み直してください)',
    not_eligible: '自社商品ではないか、除外されています', request_closed: '依頼が閉じています (画面を読み直してください)' }[r.skip] || r.skip;
  return { code: r.skip === 'not_eligible' ? 'not_found' : 'conflict', error: msg };
}

// ─── 実行役 (service-api) ─────────────────────────────────────────────────────
const jobById = (db, id) => db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE id = ?').get(id) || null;
/** job のいまの段。旧い表 (stage 列なし・作り直し失敗) の job は final (manual の 1 段) とみなす — Codex #1467 R2 #1 */
const stageOf = (job) => job.stage ?? 'final';
/** その段の予約の段 (collecting には予約が無い) */
const genStageOf = (job) => (stageOf(job) === 'seeds' ? 'seeds' : stageOf(job) === 'final' ? 'final' : null);
/** job のその段の generation。旧い表 (stage 列なし) では final とみなす (PR3c R2 ④) */
function genOf(db, jobId, stage) {
  if (!stage) return null;
  return db.prepare('SELECT * FROM ph_ad_kw_ai_generations WHERE job_id = ? ORDER BY id').all(jobId).find((g) => (g.stage ?? 'final') === stage) || null;
}
const currentGen = (db, job) => genOf(db, job.id, genStageOf(job));
/** その段の固定 packet */
function packetOfStage(job) {
  if (stageOf(job) === 'seeds') return { packet: parseJson(job.seed_packet_json, null), hash: job.seed_packet_hash };
  if (stageOf(job) === 'final') return { packet: parseJson(job.packet_json, null), hash: job.packet_hash };
  return { packet: null, hash: null };
}
/** job の親 (依頼と商品) がまだ有効か。無効なら理由 */
function parentInvalid(db, job) {
  const req = db.prepare('SELECT status FROM ph_ad_kw_requests WHERE id = ?').get(job.request_id);
  if (!req || !REQUEST_OPEN_STATUSES.includes(req.status)) return 'request_closed';
  const d = db.prepare('SELECT own_brand FROM product_drafts WHERE id = ?').get(job.draft_id);
  if (!d || Number(d.own_brand) !== 1) return 'not_own_brand';
  return null;
}
const backoffMs = (retries) => 15 * 60_000 * 2 ** Math.max(0, retries - 1);
const cancelJob = (db, job, code, nowS) => db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'cancelled', error_code = ?, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = COALESCE(finished_at, ?) WHERE id = ?`).run(code, nowS, nowS, job.id);

/** 期限切れの running を回収する (claim / queue の前)。いまの段の予約あり → needs_review / 予約なし → retry_wait (上限で failed) */
export function recoverExpired(db, now = Date.now()) {
  const nowS = new Date(now).toISOString();
  let n = 0;
  for (const job of db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE status = 'running' AND lease_until < ?`).all(nowS)) {
    const gen = currentGen(db, job);
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

/** キューの要約 (実行役の「仕事なし」判定・監視用)。needs_input / failed_unreviewed = 人の確認待ち (確認済みにすると消える — PR3c R2 ⑦) */
export function queueSummary(db, now = Date.now()) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    recoverExpired(db, now);
    const c = (sql, ...a) => db.prepare(sql).get(...a).n;
    const oldest = db.prepare(`SELECT MIN(created_at) AS t FROM ph_ad_kw_ai_jobs WHERE status IN ('queued', 'retry_wait')`).get().t;
    const ready = aiSchemaReady(db);
    // 旧い表 (作り直し失敗) では mode などの列が無い → 既存の項目だけ返す (監視が壊れない — Codex #1467 R1 #4)
    if (!ready) {
      return {
        enabled: aiEnabled(), schema_ready: false,
        claimable: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'queued' OR (status = 'retry_wait' AND next_run_at <= ?)`, nowS),
        running: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'running'`),
        retry_wait: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'retry_wait' AND next_run_at > ?`, nowS),
        needs_review: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'needs_review'`),
        needs_input: 0,
        failed_unreviewed: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'failed' AND reviewed_by IS NULL`),
        reserved_today: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_generations WHERE reserved_day = ?`, jstDay(now)),
        daily_cap: dailyCap(), auto_today: 0, auto_daily_cap: autoDailyCap(),
        oldest_wait_min: oldest ? Math.floor((now - Date.parse(oldest)) / 60_000) : null,
      };
    }
    return {
      enabled: aiEnabled(),
      schema_ready: true,
      claimable: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'queued' OR (status = 'retry_wait' AND next_run_at <= ?)`, nowS),
      running: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'running'`),
      retry_wait: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'retry_wait' AND next_run_at > ?`, nowS),
      needs_review: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'needs_review'`),
      needs_input: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'needs_input' AND reviewed_by IS NULL`),
      failed_unreviewed: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE status = 'failed' AND reviewed_by IS NULL`),
      reserved_today: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_generations WHERE reserved_day = ?`, jstDay(now)),
      daily_cap: dailyCap(),
      auto_today: c(`SELECT COUNT(*) AS n FROM ph_ad_kw_ai_jobs WHERE mode = 'auto' AND created_at >= ?`, jstDayStartIso(now)),
      auto_daily_cap: autoDailyCap(),
      oldest_wait_min: oldest ? Math.floor((now - Date.parse(oldest)) / 60_000) : null,
    };
  })();
}

/**
 * 1 件取る (lease)。取れなければ {ok:true, job:null}。親が無効な依頼はここで cancelled にして次へ。
 * capabilities に 'auto' が無い実行役 (PR3b の旧い版) には、おまかせを渡さない (PR3c R1 #5)。順 = 続き (final → collecting) → 新しい種
 * @returns {{ok:true, job:null}|{ok:true, job:{job_id, mode, stage, lease_token, lease_until, packet, packet_hash}}|{code, error}}
 */
export function claimAiJob(db, { runnerRunId, capabilities = [], now = Date.now() } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AD_KW_AI_ENABLED が無効です' };
  if (!aiSchemaReady(db)) return { code: 'ai_schema', error: 'AI 用の表の準備ができていません' };
  const auto = Array.isArray(capabilities) && capabilities.includes('auto');
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    recoverExpired(db, now);
    for (let guard = 0; guard < 50; guard++) {
      const job = db.prepare(`
        SELECT * FROM ph_ad_kw_ai_jobs WHERE (status = 'queued' OR (status = 'retry_wait' AND next_run_at <= ?)) ${auto ? '' : "AND mode = 'manual'"}
        ORDER BY CASE stage WHEN 'final' THEN 0 WHEN 'collecting' THEN 1 ELSE 2 END, id LIMIT 1
      `).get(nowS);
      if (!job) return { ok: true, job: null };
      const bad = parentInvalid(db, job);
      if (bad) { cancelJob(db, job, bad, nowS); continue; }
      const token = randomBytes(16).toString('hex');
      const until = new Date(now + LEASE_MIN * 60_000).toISOString();
      const ch = db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'running', lease_token = ?, lease_until = ?, runner_run_id = ?, claims = claims + 1, updated_at = ? WHERE id = ? AND status = ?`)
        .run(token, until, runnerRunId ? String(runnerRunId).slice(0, 80) : null, nowS, job.id, job.status).changes;
      if (ch !== 1) continue;
      const p = packetOfStage(job);
      return { ok: true, job: { job_id: job.id, mode: job.mode, stage: job.stage, lease_token: token, lease_until: until, packet: p.packet, packet_hash: p.hash } };
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

/** AI を呼ぶ前の予約 (いまの段に 1 回・1 日の上限)。stage を送るなら job のいまの段と一致すること。@returns {{ok:true, generation_id, stage}|{code, error}} */
export function reserveGeneration(db, jobId, { leaseToken, model, promptVersion, stage = null, now = Date.now() } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AD_KW_AI_ENABLED が無効です' };
  if (!aiSchemaReady(db)) return { code: 'ai_schema', error: 'AI 用の表の準備ができていません' };
  const m = String(model || '').trim(), pv = String(promptVersion || '').trim();
  if (!m || m.length > 80 || !pv || pv.length > 80) return { code: 'bad_request', error: 'model と prompt_version が要ります' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    const bad = parentInvalid(db, l.job);
    if (bad) {
      cancelJob(db, l.job, bad, nowS);
      return { code: 'parent_invalid', error: `依頼が有効ではありません (${bad})` };
    }
    const gs = genStageOf(l.job);
    if (!gs) return { code: 'bad_stage', error: '材料集めの途中です (予約できる段ではありません)' };
    if (stage && stage !== gs) return { code: 'bad_stage', error: `いまの段は ${gs} です (${stage} ではありません)` };
    if (currentGen(db, l.job)) return { code: 'already_reserved', error: 'この段の AI 呼び出しは予約済みです (1 段 1 回)' };
    const day = jstDay(now);
    if (db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_ai_generations WHERE reserved_day = ?').get(day).n >= dailyCap()) {
      return { code: 'daily_cap', error: `今日の AI 呼び出しの上限 (${dailyCap()} 回) に達しました` };
    }
    const p = packetOfStage(l.job);
    const gid = Number(db.prepare(`INSERT INTO ph_ad_kw_ai_generations (job_id, stage, packet_hash, lease_token, runner_run_id, status, model, prompt_version, reserved_day, reserved_at) VALUES (?, ?, ?, ?, ?, 'reserved', ?, ?, ?, ?)`)
      .run(l.job.id, gs, p.hash, l.job.lease_token, l.job.runner_run_id, m, pv, day, nowS).lastInsertRowid);
    return { ok: true, generation_id: gid, stage: gs };
  })();
}

/** 失敗の報告。再試行の可否はサーバーが code で決める。いまの段が予約済みなら needs_review (成否不明) */
export function failAiJob(db, jobId, { leaseToken, code, message, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  const c = String(code || 'other').slice(0, 40);
  const msg = String(message || '').slice(0, 500);
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    const job = l.job;
    if (currentGen(db, job)) {
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

/** 手放す (いまの段の予約前だけ queued へ・段と材料はそのまま・retries に数えない)。予約後は needs_review */
export function releaseAiJob(db, jobId, { leaseToken, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    if (currentGen(db, l.job)) {
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'needs_review', error_code = 'released_after_reserve', error = '予約後に手放された (成否不明)', lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`).run(nowS, l.job.id);
      return { ok: true, status: 'needs_review' };
    }
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'queued', lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`).run(nowS, l.job.id);
    return { ok: true, status: 'queued', stage: l.job.stage };
  })();
}

// ─── おまかせの材料集め (probe・1 回 = 1 外部照会) ────────────────────────────
/** サジェストの結果の語を prefix の順 (そのまま → あ〜わ → a〜z) に並べる */
function suggestWordsInOrder(result, seed) {
  const order = new Map((result?.prefixes || []).map((p, i) => [p.source, i]));
  const out = [], seen = new Set();
  const list = (result?.suggestions || []).map((s, i) => ({ s, i, o: order.has(s?.source || 'base') ? order.get(s?.source || 'base') : 999 }))
    .sort((a, b) => a.o - b.o || a.i - b.i);
  for (const { s } of list) {
    const v = normalizeKeyword(s?.keyword, { seed });
    if (!v) continue;
    const n = v.toLowerCase();
    if (seen.has(n)) continue;
    seen.add(n);
    out.push({ value: v, value_norm: n, source: s.source || 'base' });
  }
  return out;
}
/** 競合を探す語 = 種ごとに順番に 1 語ずつ (各種の中は prefix 順)・重複なし・最大 20 (PR3c R1 #9) */
export function pickTerms(seedWords) {
  const lists = seedWords.map((w) => w.slice());
  const out = [], seen = new Set();
  let progressed = true;
  while (out.length < AUTO_TERMS_MAX && progressed) {
    progressed = false;
    for (const l of lists) {
      while (l.length) {
        const w = l.shift();
        if (seen.has(w.value_norm)) continue;
        seen.add(w.value_norm);
        out.push(w.value_norm);
        progressed = true;
        break;
      }
      if (out.length >= AUTO_TERMS_MAX) break;
    }
  }
  return out;
}
const probesOf = (db, jobId) => new Map(db.prepare('SELECT * FROM ph_ad_kw_ai_probes WHERE job_id = ? ORDER BY seq, id').all(jobId).map((p) => [p.step_key, p]));
const isFinalProbe = (p) => !!p && PROBE_FINAL.includes(p.status);

/**
 * 次の照会 (固定の順): ① 種ごとのサジェスト → ② 競合を探す語 (terms) → ③ 上位 3 の競合 ASIN の検索語。
 * 入力は probe を作るときに固定 (terms の語・ASIN は保存済みの結果から決まる = 同じ材料なら同じ次)。
 * @returns {null | {step_key, kind, seq, input, skip?:string}}  null = 全部確定 (finalize できる)
 */
export function planNext(db, job) {
  const seeds = parseJson(job.seeds_json, []);
  const probes = probesOf(db, job.id);
  const seedWords = [];
  for (let i = 0; i < seeds.length; i++) {
    const key = 'suggest:' + seeds[i];
    const p = probes.get(key);
    if (!isFinalProbe(p)) return { step_key: key, kind: 'suggest', seq: 10 + i, input: { seed: seeds[i] } };
    const o = parseJson(p.outcome_json, null);
    seedWords.push(p.status === 'ok' && o?.result ? suggestWordsInOrder(o.result, seeds[i]) : []);
  }
  const tp = probes.get('terms');
  if (!isFinalProbe(tp)) {
    const terms = tp ? parseJson(tp.input_json, {}).terms || [] : pickTerms(seedWords);
    return { step_key: 'terms', kind: 'terms', seq: 100, input: { terms }, skip: terms.length === 0 ? 'no_observed_terms' : null };
  }
  if (!['ok', 'incomplete'].includes(tp.status)) return null;   // 競合が出ない (empty・skipped・打ち切り) → ASIN の照会なし
  const own = String(parseJson(job.seed_packet_json, {})?.product_extra?.asin || '').toUpperCase();
  const top = rankCompetitorAsins(parseJson(tp.outcome_json, {})?.result?.items || []).filter((x) => x.asin !== own).slice(0, AUTO_ASIN_LOOKUPS);
  for (let i = 0; i < top.length; i++) {
    const key = 'asin:' + top[i].asin;
    if (!isFinalProbe(probes.get(key))) return { step_key: key, kind: 'asin', seq: 200 + i, input: { asin: top[i].asin } };
  }
  return null;
}

/**
 * 取得結果の状態表 (外側の通信と内側の取得を分ける — PR3c R2 ③)。
 * @returns {{status:'ok'|'empty'|'incomplete', } | {retry:true, code}}
 */
export function classifyProbe(kind, outcome) {
  if (!outcome || outcome.ok !== true) return { retry: true, code: outcome?.code || 'unknown' };
  const r = outcome.result || {};
  if (kind === 'suggest') {
    const s = r.summary || {};
    if ((Number(s.success) || 0) > 0) return { status: 'ok' };                         // failed / unrun が混じっても材料として使う
    if ((Number(s.requested) || 0) > 0 && Number(s.empty) === Number(s.requested)) return { status: 'empty' };   // 全 prefix が正常に 0 件
    return { retry: true, code: 'incomplete_fetch' };                                  // 全部 failed / unrun = 取れていない
  }
  const items = Array.isArray(r.items) ? r.items : [];
  if (items.length === 0 || items.every((it) => it.status === 'no_week')) return { retry: true, code: 'no_week' };
  if (items.some((it) => it.status === 'found')) return { status: items.every((it) => it.status === 'found' || it.status === 'none') ? 'ok' : 'incomplete' };
  if (items.every((it) => it.status === 'none')) return { status: 'empty' };             // 網羅の証明つきの 0 件
  return { status: 'incomplete' };                                                      // not_covered = 「無い」とは言えない
}

/**
 * おまかせの材料を 1 つ集める (実行役が lease 中に順に呼ぶ)。サーバーが次の照会を決め、1 回 = 1 外部照会。
 * 通信は txn の外。前 (実行権の取得) と後 (保存) で job の lease・段・親を確かめ、保存は probe の run_token も一致したときだけ (PR3c R2 ①)。
 * 再試行できる取得失敗 → その job はその晩やめる (retry_wait・12 時間後・retries に数えない)。同じ照会が 3 晩失敗 → gave_up
 * @param clients {suggest(seed), terms(terms[]), asin(asin)} (テストで差し替え)
 * @returns {Promise<{ok:true, done:true}|{ok:true, done:false, step_key, status}|{ok:true, in_progress:true, step_key}|{ok:true, stop:'retry_later', step_key, code}|{code, error}>}
 */
export async function collectStep(db, jobId, { leaseToken, clients, now = () => Date.now() } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AD_KW_AI_ENABLED が無効です' };
  if (!aiSchemaReady(db)) return { code: 'ai_schema', error: 'AI 用の表の準備ができていません' };
  // 1) 実行権を取る (短い txn)
  const claim = db.transaction(() => {
    const t = now(), nowS = new Date(t).toISOString();
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    const job = l.job;
    if (job.mode !== 'auto' || job.stage !== 'collecting') return { code: 'bad_stage', error: '材料集めの段ではありません' };
    const bad = parentInvalid(db, job);
    if (bad) { cancelJob(db, job, bad, nowS); return { code: 'parent_invalid', error: `依頼が有効ではありません (${bad})` }; }
    for (let guard = 0; guard < 100; guard++) {
      const next = planNext(db, job);
      if (!next) return { done: true };
      const ex = db.prepare('SELECT * FROM ph_ad_kw_ai_probes WHERE job_id = ? AND step_key = ?').get(job.id, next.step_key);
      if (next.skip) {   // 観測語が無くて照会を作れない → 確定 (skipped) して次へ
        if (ex) db.prepare(`UPDATE ph_ad_kw_ai_probes SET status = 'skipped', error = ?, run_token = NULL, run_until = NULL, finished_at = ? WHERE id = ?`).run(next.skip, nowS, ex.id);
        else db.prepare(`INSERT INTO ph_ad_kw_ai_probes (job_id, step_key, seq, kind, input_json, status, error, finished_at) VALUES (?, ?, ?, ?, ?, 'skipped', ?, ?)`).run(job.id, next.step_key, next.seq, next.kind, JSON.stringify(next.input), next.skip, nowS);
        continue;
      }
      if (ex && ex.status === 'running' && ex.run_until && ex.run_until > nowS) return { in_progress: true, step_key: next.step_key };
      const token = randomBytes(12).toString('hex');
      const until = new Date(t + PROBE_RUN_MS).toISOString();
      if (ex) {
        db.prepare(`UPDATE ph_ad_kw_ai_probes SET status = 'running', run_token = ?, run_until = ?, attempts = attempts + 1 WHERE id = ?`).run(token, until, ex.id);
        return { step: { ...next, input: parseJson(ex.input_json, next.input) }, token };
      }
      db.prepare(`INSERT INTO ph_ad_kw_ai_probes (job_id, step_key, seq, kind, input_json, status, run_token, run_until, attempts) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, 1)`)
        .run(job.id, next.step_key, next.seq, next.kind, JSON.stringify(next.input), token, until);
      return { step: next, token };
    }
    return { code: 'internal', error: 'materials plan did not converge' };
  })();
  if (claim.code) return claim;
  if (claim.done) return { ok: true, done: true };
  if (claim.in_progress) return { ok: true, in_progress: true, step_key: claim.step_key };
  // 2) 外部照会 (txn の外)
  const { step, token } = claim;
  let outcome;
  try {
    if (step.kind === 'suggest') outcome = await clients.suggest(step.input.seed);
    else if (step.kind === 'terms') outcome = await clients.terms(step.input.terms);
    else outcome = await clients.asin(step.input.asin);
  } catch (e) {
    outcome = { ok: false, code: 'unreachable', message: e?.message || String(e) };
  }
  // 3) 保存 (job の lease と probe の run_token の両方が一致したときだけ)
  return db.transaction(() => {
    const t = now(), nowS = new Date(t).toISOString();
    const probe = db.prepare('SELECT * FROM ph_ad_kw_ai_probes WHERE job_id = ? AND step_key = ?').get(jobId, step.step_key);
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    if (!probe || probe.status !== 'running' || probe.run_token !== token) return { code: 'probe_lost', error: 'この照会の実行権を失いました (別の実行に取られた)' };
    const bad = parentInvalid(db, l.job);
    if (bad) { cancelJob(db, l.job, bad, nowS); return { code: 'parent_invalid', error: `依頼が有効ではありません (${bad})` }; }
    const cls = classifyProbe(step.kind, outcome);
    const outcomeJson = JSON.stringify(outcome && outcome.ok ? { ok: true, result: outcome.result } : { ok: false, code: outcome?.code || 'unknown', message: String(outcome?.message || '').slice(0, 300) });
    if (!cls.retry) {
      db.prepare(`UPDATE ph_ad_kw_ai_probes SET status = ?, outcome_json = ?, error = NULL, run_token = NULL, run_until = NULL, finished_at = ? WHERE id = ?`).run(cls.status, outcomeJson, nowS, probe.id);
      return { ok: true, done: false, step_key: step.step_key, status: cls.status };
    }
    const day = jstDay(t);
    const nights = probe.nights_failed + (probe.last_fail_day === day ? 0 : 1);
    if (nights >= PROBE_MAX_NIGHTS) {
      db.prepare(`UPDATE ph_ad_kw_ai_probes SET status = 'gave_up', outcome_json = ?, error = ?, nights_failed = ?, last_fail_day = ?, run_token = NULL, run_until = NULL, finished_at = ? WHERE id = ?`)
        .run(outcomeJson, cls.code, nights, day, nowS, probe.id);
      return { ok: true, done: false, step_key: step.step_key, status: 'gave_up' };
    }
    db.prepare(`UPDATE ph_ad_kw_ai_probes SET status = 'failed', outcome_json = ?, error = ?, nights_failed = ?, last_fail_day = ?, run_token = NULL, run_until = NULL WHERE id = ?`)
      .run(outcomeJson, cls.code, nights, day, probe.id);
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'retry_wait', next_run_at = ?, error_code = ?, error = ?, lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`)
      .run(new Date(t + RETRY_LATER_MS).toISOString(), 'material_' + cls.code, `材料の取得に失敗 (${step.step_key}) — 次の晩に続きから`, nowS, jobId);
    return { ok: true, stop: 'retry_later', step_key: step.step_key, code: cls.code };
  })();
}

/** probe から最終案の観測語 (obs_id つき) を作る。サジェスト (種の順・prefix 順) → ABA (ASIN の順・検索頻度順位) */
function observationsFromProbes(db, job, seeds) {
  const probes = [...probesOf(db, job.id).values()];
  const byNorm = new Map();
  const add = (value, norm, source, seedOrAsin, weekStart) => {
    let o = byNorm.get(norm);
    if (!o) { o = { value, value_norm: norm, sources: [], seeds: [], week_start: null }; byNorm.set(norm, o); }
    if (!o.sources.includes(source)) o.sources.push(source);
    if (o.seeds.length < 3 && !o.seeds.includes(seedOrAsin)) o.seeds.push(seedOrAsin);
    if (weekStart && !o.week_start) o.week_start = weekStart;
  };
  for (const seed of seeds) {
    const p = probes.find((x) => x.step_key === 'suggest:' + seed);
    if (!p || p.status !== 'ok') continue;
    for (const w of suggestWordsInOrder(parseJson(p.outcome_json, {}).result, seed)) add(w.value, w.value_norm, 'suggest', seed);
  }
  for (const p of probes.filter((x) => x.kind === 'asin' && ['ok', 'incomplete'].includes(x.status)).sort((a, b) => a.seq - b.seq)) {
    const r = parseJson(p.outcome_json, {}).result || {};
    const item = (r.items || [])[0];
    if (!item || !Array.isArray(item.terms)) continue;
    const rankOf = (t) => (Number.isFinite(Number(t.search_frequency_rank)) ? Number(t.search_frequency_rank) : 9999999);
    for (const t of [...item.terms].sort((a, b) => rankOf(a) - rankOf(b))) {
      const v = normalizeKeyword(t.search_term);
      if (v) add(v, v.toLowerCase(), 'aba', parseJson(p.input_json, {}).asin, r.week?.week_start || null);
    }
  }
  return [...byNorm.values()];
}

/**
 * 材料集めを終えて最終案の packet を固定する (初回だけ作って保存・再送は同じ packet を返す — PR3c R1 #5)。
 * 観測 0: 全照会が ok / empty / skipped → needs_input (人が種を入れて集め、既存の「AI に頼む」で続ける) / 打ち切り・不完全あり → failed (material_unavailable)
 * @returns {{ok:true, stage:'final', packet, packet_hash}|{ok:true, status:'needs_input'|'failed', reason}|{code, error}}
 */
export function finalizeAutoJob(db, jobId, { leaseToken, now = Date.now() } = {}) {
  if (!aiEnabled()) return { code: 'ai_disabled', error: 'AD_KW_AI_ENABLED が無効です' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    const job = l.job;
    if (job.mode !== 'auto') return { code: 'bad_stage', error: 'おまかせの依頼ではありません' };
    if (job.stage === 'final') return { ok: true, stage: 'final', packet: parseJson(job.packet_json, null), packet_hash: job.packet_hash, replay: true };
    if (job.stage !== 'collecting') return { code: 'bad_stage', error: '材料集めの段ではありません' };
    const bad = parentInvalid(db, job);
    if (bad) { cancelJob(db, job, bad, nowS); return { code: 'parent_invalid', error: `依頼が有効ではありません (${bad})` }; }
    if (planNext(db, job)) return { code: 'not_ready', error: 'まだ集めていない材料があります' };
    if (db.prepare(`SELECT 1 FROM ph_ad_kw_ai_probes WHERE job_id = ? AND status IN ('running', 'failed')`).get(job.id)) return { code: 'not_ready', error: '確定していない照会があります' };
    const seeds = parseJson(job.seeds_json, []);
    const seedPacket = parseJson(job.seed_packet_json, {});
    const all = observationsFromProbes(db, job, seeds);
    const probes = [...probesOf(db, job.id).values()];
    if (all.length === 0) {
      const normal = probes.every((p) => ['ok', 'empty', 'skipped'].includes(p.status));
      const status = normal ? 'needs_input' : 'failed';
      const reason = normal ? 'no_observations' : 'material_unavailable';
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = ?, error_code = ?, error = ?, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`)
        .run(status, reason, normal ? 'サジェストにも ABA にも語が見つかりませんでした (種を入れて集め、「AI に頼む」で続けてください)' : '材料が取れないまま打ち切りました (miniPC・ABA の取込を確認してください)', nowS, nowS, job.id);
      logEvent(db, job.draft_id, 'ad_kw_ai_auto_no_material', `おまかせ ${job.id}: ${reason}`, AUTO_ACTOR_AI);
      return { ok: true, status, reason };
    }
    let omitted = 0;
    const observations = [];
    for (const o of all) {
      if (observations.length >= PACKET_MAX_OBS) { omitted += 1; continue; }
      observations.push({ obs_id: 'o' + (observations.length + 1), value: o.value, value_norm: o.value_norm, sources: o.sources, seeds: o.seeds, fetched_at: null, week_start: o.week_start });
    }
    const tp = probes.find((p) => p.step_key === 'terms');
    const own = String(seedPacket?.product_extra?.asin || '').toUpperCase();
    // 競合の順位 (ABA のクリック上位 3・自動採用と同じ並びと上限) は、人の採否 (adopted_asins) とは別の項目 (Codex #1467 R1 #3)
    const competitors = tp && ['ok', 'incomplete'].includes(tp.status)
      ? rankCompetitorAsins(parseJson(tp.outcome_json, {})?.result?.items || []).filter((x) => x.asin !== own).slice(0, AUTO_ASINS_PER_RUN).map((x) => x.asin) : [];
    const { adopted, adoptedAsins, omittedAdopted } = adoptedOf(db, job.request_id);
    const packet = trimPacket({
      packet_version: PACKET_VERSION, rules_version: RULES_VERSION,
      product: seedPacket.product || { name: '', specs: [] }, product_extra: seedPacket.product_extra || null,
      seeds, observations, adopted, adopted_asins: adoptedAsins, competitor_asins: competitors, decision_version: decisionVersionOf(db, job.request_id),
      limits: { omitted_observations: omitted, omitted_adopted: omittedAdopted, max_keywords: MAX_KEYWORDS, max_basis: MAX_BASIS, too_large: false },
      materials: probes.map((p) => ({ step: p.step_key, status: p.status })),
    });
    if (packet.limits.too_large) {
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed', error_code = 'packet_too_large', error = '材料が大きすぎて AI に渡せません', lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`).run(nowS, nowS, job.id);
      return { ok: true, status: 'failed', reason: 'packet_too_large' };
    }
    const packetJson = canonicalJson(packet);
    const packetHash = sha256(packetJson);
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET stage = 'final', packet_json = ?, packet_hash = ?, updated_at = ? WHERE id = ? AND stage = 'collecting'`).run(packetJson, packetHash, nowS, job.id);
    return { ok: true, stage: 'final', packet, packet_hash: packetHash };
  })();
}

/**
 * おまかせの材料を候補表に反映する (最終案の保存の txn の中 — PR3c ⑧)。
 * サジェスト (ok / empty) → evidence + 観測の候補 / terms → 競合 ASIN の候補 + 自動採用 (最大 10・auto:aba) / ASIN → evidence + 観測の候補 (register:false)。
 * 失敗・打ち切りの照会は反映しない (材料の記録は probe に残る)
 * @returns {{decision_ids:number[], suggest:number, asins:number, aba:number}}
 */
function applyAutoMaterials(db, job) {
  const cur = db.prepare('SELECT * FROM ph_ad_kw_requests WHERE id = ?').get(job.request_id);
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(job.draft_id);
  const out = { decision_ids: [], suggest: 0, asins: 0, aba: 0 };
  for (const p of [...probesOf(db, job.id).values()]) {
    const o = parseJson(p.outcome_json, null);
    if (!o || o.ok !== true) continue;
    const input = parseJson(p.input_json, {});
    if (p.kind === 'suggest' && ['ok', 'empty'].includes(p.status)) {
      const seed = normalizeSeed(input.seed).seed;
      if (!seed) continue;
      applySuggestOutcome(db, cur, seed, o, { actor: AUTO_ACTOR_AI });
      out.suggest += 1;
    } else if (p.kind === 'terms' && ['ok', 'empty', 'incomplete'].includes(p.status)) {
      // 自分の ASIN は ASIN の欄が空なら Amazon の URL から (URL だけの商品で自分を競合として採用しない — Codex #1477 R1)
      const r = applyAutoAsinsOutcome(db, { ...draft, asin: asinOfDraft(draft) }, cur, input.terms || [], 0, o, AUTO_ACTOR_AI);
      out.decision_ids.push(...(r.decision_ids || []));
      out.asins += (r.added || []).length;
    } else if (p.kind === 'asin' && ['ok', 'empty', 'incomplete'].includes(p.status) && ASIN_RE.test(String(input.asin || ''))) {
      applyAbaOutcome(db, cur, input.asin, o, { actor: AUTO_ACTOR_AI, register: false });
      out.aba += 1;
    }
  }
  return out;
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
 * 種の出力を検証する (純粋)。{"seeds":["…"]}・1〜5 個・種の規則 (60 文字・制御文字なし)・ASIN / URL は不可・重複は 1 つ
 * @returns {{fatal:string|null, seeds:string[], rejected:Array<{index, seed, reason}>}}
 */
export function validateSeedsOutput(output) {
  if (!output || typeof output !== 'object' || Array.isArray(output) || !Array.isArray(output.seeds)) return { fatal: 'seeds (配列) がありません', seeds: [], rejected: [] };
  if (output.seeds.length > 20) return { fatal: `種が多すぎます (${output.seeds.length})`, seeds: [], rejected: [] };
  const seeds = [], rejected = [], seen = new Set();
  output.seeds.forEach((raw, index) => {
    const rej = (reason) => rejected.push({ index, seed: typeof raw === 'string' ? raw.slice(0, 60) : null, reason });
    if (typeof raw !== 'string') return rej('bad_shape');
    const ns = normalizeSeed(raw);
    if (ns.error) return rej('bad_seed');
    if (ASIN_RE.test(ns.seed.toUpperCase()) || /https?:|www\./i.test(ns.seed)) return rej('not_allowed');
    const k = ns.seed.toLowerCase();
    if (seen.has(k)) return rej('duplicate');
    if (seeds.length >= AUTO_SEEDS_MAX) return rej('too_many');
    seen.add(k);
    seeds.push(ns.seed);
  });
  if (seeds.length < AUTO_SEEDS_MIN) return { fatal: '使える種が 1 つもありません', seeds: [], rejected };
  return { fatal: null, seeds, rejected };
}

/**
 * 結果の提出 (generation 単位)。lease が切れていても、予約済みの generation なら受ける (復旧)。
 * 段 = generation の段 (旧い表では final)。照合する packet = generation の packet_hash (旧い表では job の packet_hash — PR3c R2 ④)
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
    const stage = gen.stage ?? 'final';
    if (payload.packet_hash !== (gen.packet_hash ?? job.packet_hash)) return { code: 'packet_mismatch', error: '材料 (packet) の版が違います' };
    // 確定: reserved → 最終処分。以降この generation は戻らない
    const finalize = (status, receipt, discardReason = null) => {
      const ch = db.prepare(`UPDATE ph_ad_kw_ai_generations SET status = ?, payload_json = ?, payload_hash = ?, receipt_json = ?, discard_reason = ?, finalized_at = ? WHERE id = ? AND status = 'reserved'`)
        .run(status, payloadJson, payloadHash, JSON.stringify(receipt), discardReason, nowS, gen.id).changes;
      if (ch !== 1) throw new Error('generation was finalized concurrently');
    };
    const bad = job.status === 'cancelled' ? (job.error_code || 'job_cancelled') : parentInvalid(db, job);
    if (bad) {
      const receipt = { generation_id: gen.id, job_id: job.id, stage, disposition: 'discarded', reason: bad, payload_hash: payloadHash, accepted: 0, rejected: 0 };
      finalize('discarded', receipt, bad);
      if (job.status !== 'cancelled') cancelJob(db, job, bad, nowS);
      logEvent(db, job.draft_id, 'ad_kw_ai_discarded', `AI 依頼 ${job.id} の結果を破棄 (${bad})`, 'ph-nightly');
      return { ok: true, receipt };
    }
    if (stage === 'seeds') return submitSeedsResult(db, gen, job, payload, payloadHash, finalize, nowS);

    // 最終案 (manual / auto)
    // おまかせの材料は AI の出力の良し悪しに関係なく反映する (取れた材料は人が使える)。いまの段の job に対してだけ
    const applyMaterials = job.mode === 'auto' && job.stage === 'final';
    const mat = applyMaterials ? applyAutoMaterials(db, job) : null;
    const ownRange = mat && mat.decision_ids.length ? [Math.min(...mat.decision_ids), Math.max(...mat.decision_ids)] : [null, null];
    // 自分の採用の範囲は PR3c の表の列。旧い表 (作り直し失敗) でも結果の保存を通すため、範囲があるときだけ書く (PR3c R2 ④)
    const ownSql = ownRange[0] != null ? ", own_decision_min = " + Number(ownRange[0]) + ", own_decision_max = " + Number(ownRange[1]) : "";
    const packet = JSON.parse(job.packet_json);
    const v = validateOutput(payload.output, packet);
    if (v.fatal) {
      const receipt = { generation_id: gen.id, job_id: job.id, stage, disposition: 'rejected', reason: v.fatal, payload_hash: payloadHash, accepted: 0, rejected: 0, result_kind: 'rejected', materials: mat };
      finalize('rejected', receipt);
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed', result_kind = 'rejected', error_code = 'invalid_output', error = ?${ownSql}, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`)
        .run(v.fatal, nowS, nowS, job.id);
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
    `).run(job.request_id, `ai:job${job.id}`, evStatus, JSON.stringify({ job_id: job.id, generation_id: gen.id, model: gen.model, prompt_version: gen.prompt_version, mode: job.mode }),
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
    // おまかせの最終案で、サジェスト・ABA に観測された語の提案は最初から採用 (完全一致＋フレーズ一致・auto:ai)。
    // 人が一度でも採否を付けた候補は変えない。AI だけの語 (観測なし) は未採用のまま。競合 ASIN の自動採用 (auto:aba) と同じ「要らなければ却下」
    const hasDecision = db.prepare('SELECT 1 FROM ph_ad_kw_decisions WHERE candidate_id = ? LIMIT 1');
    const insAdopt = db.prepare(`
      INSERT INTO ph_ad_kw_decisions (candidate_id, request_id, decision, keyword, match_type, supersedes_decision_id, actor)
      VALUES (?, ?, 'adopt', ?, 'exact_phrase', NULL, ?)
    `);
    const adoptIds = [];
    let newCands = 0;
    v.accepted.forEach((a, i) => {
      let cand = findCand.get(job.request_id, a.value_norm);
      if (!cand) {
        const cid = Number(insCand.run(job.request_id, a.value, a.value_norm, evId, `ai|${String(job.id).padStart(8, '0')}|${String(i).padStart(3, '0')}`).lastInsertRowid);
        cand = { id: cid };
        newCands += 1;
      }
      insProp.run(job.id, gen.id, cand.id, a.value, a.value_norm, JSON.stringify(a.basis), a.reason, a.match_hint, a.observed);
      if (applyMaterials && a.observed === 'observed' && !hasDecision.get(cand.id)) {
        adoptIds.push(Number(insAdopt.run(cand.id, job.request_id, a.value, AUTO_ACTOR_AI).lastInsertRowid));
      }
    });
    // 自分の採用 (競合 ASIN + 観測のある提案) の範囲。同じ txn で続けて入れたので連番 (他の書き込みは挟まらない)
    const ownIds = [...(mat ? mat.decision_ids : []), ...adoptIds];
    const ownSqlDone = ownIds.length ? ", own_decision_min = " + Math.min(...ownIds) + ", own_decision_max = " + Math.max(...ownIds) : "";
    const receipt = { generation_id: gen.id, job_id: job.id, stage, disposition: v.accepted.length || !v.rejected.length ? 'accepted' : 'rejected', payload_hash: payloadHash,
      accepted: v.accepted.length, rejected: v.rejected.length, new_candidates: newCands, result_kind: resultKind, reasons, materials: mat, auto_adopted: adoptIds.length };
    finalize(receipt.disposition, receipt);
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'done', result_kind = ?, accepted = ?, rejected = ?${ownSqlDone}, error_code = NULL, error = NULL, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`)
      .run(resultKind, v.accepted.length, v.rejected.length, nowS, nowS, job.id);
    logEvent(db, job.draft_id, 'ad_kw_ai_result', `AI 依頼 ${job.id}: 受理 ${v.accepted.length} (新しい候補 ${newCands}) / 棄却 ${v.rejected.length}`
      + (mat ? `・材料 サジェスト ${mat.suggest} 種 / 競合 ASIN 自動採用 ${mat.asins} / 競合の検索語 ${mat.aba}・観測のある提案を自動採用 ${adoptIds.length}` : ''), 'ph-nightly');
    return { ok: true, receipt };
  })();
}

/**
 * 種の結果 (おまかせの 1 段目)。種は履歴として保存し、collecting へ進めるのは:
 *   running (いまの実行) → そのまま collecting (lease を保つ = 同じ実行役が続ける)
 *   needs_review で未確認・同じ依頼にこの job より新しい job が無い (状態を問わない) → collecting・queued (次の claim で続き。古い token は戻さない — PR3c R2 ②)
 *   それ以外 (確認済み・取消・新しい job あり) → 状態は変えない
 */
function submitSeedsResult(db, gen, job, payload, payloadHash, finalize, nowS) {
  const v = validateSeedsOutput(payload.output);
  if (v.fatal) {
    const receipt = { generation_id: gen.id, job_id: job.id, stage: 'seeds', disposition: 'rejected', reason: v.fatal, payload_hash: payloadHash, seeds: [], rejected: v.rejected.length };
    finalize('rejected', receipt);
    if (['running', 'needs_review'].includes(job.status) && job.stage === 'seeds' && !job.reviewed_by) {
      db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed', error_code = 'invalid_output', error = ?, lease_token = NULL, lease_until = NULL, updated_at = ?, finished_at = ? WHERE id = ?`).run('種: ' + v.fatal, nowS, nowS, job.id);
    }
    logEvent(db, job.draft_id, 'ad_kw_ai_rejected', `おまかせ ${job.id} の種を棄却 (${v.fatal})`, 'ph-nightly');
    return { ok: true, receipt };
  }
  const newer = db.prepare('SELECT 1 FROM ph_ad_kw_ai_jobs WHERE request_id = ? AND id > ?').get(job.request_id, job.id);
  const leaseLive = job.status === 'running' && !!job.lease_until && job.lease_until >= nowS;
  let next = null;
  // running で lease が生きている = いまの実行役が続ける。running でも lease が切れていれば (回収の前に結果が届いた) 古い token は使わせない
  // = 再開の条件 (未確認・新しい job なし) を満たせば queued から続き (retries は増やさない — Codex #1467 R1 #2)
  if (job.stage === 'seeds' && leaseLive) next = 'running';
  else if (job.stage === 'seeds' && (job.status === 'running' || job.status === 'needs_review') && !job.reviewed_by && !newer) next = 'queued';
  if (next === 'running') {
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET stage = 'collecting', seeds_json = ?, error_code = NULL, error = NULL, updated_at = ? WHERE id = ?`).run(JSON.stringify(v.seeds), nowS, job.id);
  } else if (next === 'queued') {
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET stage = 'collecting', status = 'queued', seeds_json = ?, error_code = NULL, error = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`).run(JSON.stringify(v.seeds), nowS, job.id);
  } else if (job.stage === 'seeds' && !job.seeds_json) {
    db.prepare('UPDATE ph_ad_kw_ai_jobs SET seeds_json = ?, updated_at = ? WHERE id = ?').run(JSON.stringify(v.seeds), nowS, job.id);   // 履歴だけ
  }
  const receipt = { generation_id: gen.id, job_id: job.id, stage: 'seeds', disposition: 'accepted', payload_hash: payloadHash, seeds: v.seeds, rejected: v.rejected.length,
    next_stage: next ? 'collecting' : null, resumed: next === 'queued' };
  finalize('accepted', receipt);
  logEvent(db, job.draft_id, 'ad_kw_ai_seeds', `おまかせ ${job.id}: 種 ${v.seeds.join(' / ')}${next ? '' : ' (履歴のみ・続きは動かさない)'}`, 'ph-nightly');
  return { ok: true, receipt };
}

// ─── 画面の状態 ────────────────────────────────────────────────────────────────
/** stateForDraft に載せる AI の状態。proposals = 候補 id → 最新の提案 (理由・観測・根拠の語)。auto = この商品のおまかせの最新 */
export function aiStateFor(db, draft, request) {
  const enabled = aiEnabled(), schema = aiSchemaReady(db);
  const base = { enabled: enabled && schema, reason_disabled: !enabled ? '準備中 (夜間の実行役がまだ入っていません)' : (!schema ? '表の準備ができていません' : null),
    jobs: [], active: null, can_request: false, proposals: {}, auto: null, labels: { status: JOB_STATUS_JA, result_kind: RESULT_KIND_JA, stage: STAGE_JA } };
  if (!schema) return base;
  // おまかせ (依頼が無くても出す: 夜に自動で依頼を作る前の「待ち」を見せる)
  const lastAuto = db.prepare(`SELECT * FROM ph_ad_kw_ai_jobs WHERE draft_id = ? AND mode = 'auto' ORDER BY auto_round DESC LIMIT 1`).get(draft.id);
  base.auto = {
    job: lastAuto ? { id: lastAuto.id, round: lastAuto.auto_round, status: lastAuto.status, status_ja: JOB_STATUS_JA[lastAuto.status] || lastAuto.status,
      stage: lastAuto.stage, stage_ja: STAGE_JA[lastAuto.stage] || lastAuto.stage, error_code: lastAuto.error_code, error: lastAuto.error,
      seeds: parseJson(lastAuto.seeds_json, []), created_at: lastAuto.created_at, finished_at: lastAuto.finished_at, reviewed_by: lastAuto.reviewed_by,
      amazon_title: parseJson(lastAuto.seed_packet_json, {})?.product_extra?.amazon_title || null } : null,
    can_rerun: enabled && Number(draft.own_brand) === 1 && (lastAuto ? !ACTIVE_JOB_STATUSES.includes(lastAuto.status) : !isAutoTarget(draft)),
    waiting: enabled && Number(draft.own_brand) === 1 && !lastAuto && isAutoTarget(draft),
    out_of_scope: Number(draft.own_brand) === 1 && !lastAuto && !isAutoTarget(draft),
  };
  if (!request) return base;
  let now = null;
  try { now = buildPacket(db, draft, request.id); } catch (_) { now = null; }
  const productNow = now ? sha256(canonicalJson(now.product)) : null;
  const jobs = db.prepare('SELECT * FROM ph_ad_kw_ai_jobs WHERE request_id = ? ORDER BY id DESC LIMIT 5').all(request.id).map((j) => {
    const packet = parseJson(j.packet_json, {}) || {};
    const effDv = j.packet_json ? effectiveDecisionVersion(db, j, packet) : null;
    return {
      id: j.id, mode: j.mode, stage: j.stage, stage_ja: STAGE_JA[j.stage] || j.stage,
      status: j.status, status_ja: JOB_STATUS_JA[j.status] || j.status, result_kind: j.result_kind, result_kind_ja: j.result_kind ? RESULT_KIND_JA[j.result_kind] : null,
      accepted: j.accepted, rejected: j.rejected, error_code: j.error_code, error: j.error, created_at: j.created_at, finished_at: j.finished_at, requested_by: j.requested_by,
      observations: (packet.observations || []).length, omitted: packet.limits?.omitted_observations || 0,
      // 旧材料 = 商品情報 (名前・仕様) か採否版が、頼んだときから変わった。採否版は画面でも比べ直す (採否の保存のたびに — Codex #1429 R1 #5)
      stale_product: !!j.packet_json && productNow != null && sha256(canonicalJson(packet.product || {})) !== productNow,
      packet_decision_version: j.packet_json ? effDv : null,
      stale: !!j.packet_json && now != null && inputVersionOf(packet.product, effDv) !== inputVersionOf(now.product, now.decision_version), reviewed_by: j.reviewed_by,
      needs_ack: ['needs_review', 'failed', 'needs_input'].includes(j.status) && !j.reviewed_by,
    };
  });
  const active = jobs.find((j) => ACTIVE_JOB_STATUSES.includes(j.status)) || null;
  const obsCount = now ? now.observations.length : 0;   // いまの観測語 (観測記録で判定)
  const proposals = {};
  const obsValueByJob = new Map();
  for (const p of db.prepare(`
    SELECT p.*, j.packet_json, j.request_id, j.own_decision_min, j.own_decision_max FROM ph_ad_kw_ai_proposals p JOIN ph_ad_kw_ai_jobs j ON j.id = p.job_id
    WHERE j.request_id = ? ORDER BY p.job_id, p.id
  `).all(request.id)) {
    // 新しい依頼 (job id が大きい) の提案を表示する。古い依頼の結果が遅れて届いても上書きしない (履歴は表に残る — Codex #1429 R1 #2)
    const packet = parseJson(p.packet_json, {}) || {};
    if (!obsValueByJob.has(p.job_id)) obsValueByJob.set(p.job_id, new Map((packet.observations || []).map((o) => [o.obs_id, o.value])));
    const values = obsValueByJob.get(p.job_id);
    const prev = proposals[p.candidate_id];
    const effDv = effectiveDecisionVersion(db, p, packet);
    proposals[p.candidate_id] = { job_id: p.job_id, reason: p.reason, observed: p.observed, match_hint: p.match_hint,
      basis: parseJson(p.basis_obs_ids, []).map((id) => values.get(id)).filter(Boolean), count: (prev ? prev.count : 0) + 1,
      // 画面で採否版を比べ直すための材料 (採否を保存するたびに行を描き直す — Codex #1429 R2 #1)
      job_stale_product: now != null && sha256(canonicalJson(packet.product || {})) !== sha256(canonicalJson(now.product)),
      job_decision_version: effDv,
      job_stale: now != null && inputVersionOf(packet.product, effDv) !== inputVersionOf(now.product, now.decision_version) };
  }
  return { ...base, jobs, active, can_request: base.enabled && !active && REQUEST_OPEN_STATUSES.includes(request.status) && obsCount > 0 && !(now && now.limits.too_large),
    too_large: !!(now && now.limits.too_large), proposals };
}
