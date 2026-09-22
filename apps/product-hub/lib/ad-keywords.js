/**
 * SP広告 検索キーワード — 依頼・サジェスト収集・採否・コピー履歴 (PR1・2026-09-23)
 *
 * 設計 = 『Amazon_SP広告KW自動生成_設計方針_20260922.md』§4.3 / §4.5 / §4.8 / §5 PR1。
 * DB は product-hub の 5 表 (ph_ad_kw_requests / evidence / candidates / decisions / exports、db.js)。
 * HTTP は router.js、miniPC 呼び出しは lib/keyword-suggest-client.js。ここは DB とロジックだけ。
 *
 * 守りたいこと:
 *   ① 失敗・0 件・一部・未実行を混ぜない (evidence.status と coverage を必ず残す)
 *   ② 収集は依頼ごとに 1 つずつ (lease token)。取消・置き換え・奪われたあとの結果は保存しない
 *   ③ 採否は append-only (最新行がいまの採否)。人の API だけが書く
 *   ④ コピー本文は採否版を参照した固定の中身。「コピー済み」は「Amazon 登録済み」ではない
 */
import { createHash, randomBytes } from 'node:crypto';
import { logEvent } from '../db.js';
import { MATCH_TYPES, MATCH_TYPE_JA, normalizeKeyword, exportSnapshot } from './ad-keywords-export.js';

export const REQUEST_OPEN_STATUSES = ['collecting', 'review_ready'];
export const DECISIONS = ['adopt', 'hold', 'reject', 'undecided'];
export const DECISION_JA = { adopt: '採用', hold: '保留', reject: '却下', undecided: '未採用' };
export const SEED_MAX_LEN = 60;                 // miniPC 側 (keyword-suggest-service.js) と同じ
export const MAX_SEEDS_PER_REQUEST = 20;        // 1 種 ≈ 47 リクエスト。20 種 ≈ 940 回 (人が目視していた量の上限)
export const COLLECT_STALE_MS = 3 * 60 * 1000;  // 1 回 ≈ 10〜20 秒 + Render 側の待ち 45 秒。これを越えた「収集中」は死んだ収集とみなす

const nowIso = () => new Date().toISOString();

/** 種 KW の正規化 (miniPC 側 normalizeSeed と同じ規則)。@returns {{seed:string}|{error:string}} */
export function normalizeSeed(raw) {
  const s = String(raw ?? '').replace(/[\x00-\x1f\x7f]/g, ' ').replace(/[\s　]+/g, ' ').trim();
  if (!s) return { error: '種になるキーワードを入力してください' };
  if (s.length > SEED_MAX_LEN) return { error: `種は ${SEED_MAX_LEN} 文字までです` };
  return { seed: s };
}

/** 依頼時点で固定する商品情報。変わったら「旧情報に基づく」と出す (§4.5) */
export function productSnapshotOf(draft) {
  return { name: draft.name ?? null, ne_code: draft.ne_code ?? null, asin: draft.asin ?? null };
}
export function inputHashOf(snapshot) {
  return createHash('sha256').update(JSON.stringify(snapshot)).digest('hex').slice(0, 16);
}

/** いま開いている依頼 (収集中 or レビュー可)。1 ドラフトに 0 or 1 */
export function openRequestOf(db, draftId) {
  return db.prepare(`
    SELECT * FROM ph_ad_kw_requests WHERE draft_id = ? AND status IN ('collecting', 'review_ready') ORDER BY id DESC LIMIT 1
  `).get(draftId) || null;
}
const requestById = (db, id) => db.prepare('SELECT * FROM ph_ad_kw_requests WHERE id = ?').get(id) || null;

/**
 * 依頼を用意する。冪等キーが同じなら同じ依頼、開いている依頼があればそれを返す。
 * restart=true のときだけ、開いている依頼を superseded にして新しく作る (以前の採否は消えない・上書きしない)
 * @returns {{ok:true, request, reused:boolean}|{code:string, error:string}}
 */
export function ensureRequest(db, draft, { idempotencyKey, actor, restart = false } = {}) {
  const key = String(idempotencyKey || '').trim();
  if (!key || key.length > 100) return { code: 'bad_key', error: '依頼の識別子がありません (画面を読み直してください)' };
  return db.transaction(() => {
    const same = db.prepare('SELECT * FROM ph_ad_kw_requests WHERE draft_id = ? AND idempotency_key = ?').get(draft.id, key);
    if (same) return { ok: true, request: same, reused: true };
    const open = openRequestOf(db, draft.id);
    if (open && !restart) return { ok: true, request: open, reused: true };
    if (open) {
      db.prepare(`UPDATE ph_ad_kw_requests SET status = 'superseded', collecting_seed = NULL, collecting_token = NULL,
        collecting_since = NULL, updated_at = ? WHERE id = ?`).run(nowIso(), open.id);
    }
    const snap = productSnapshotOf(draft);
    const info = db.prepare(`
      INSERT INTO ph_ad_kw_requests (draft_id, idempotency_key, product_snapshot_json, input_hash, supersedes_request_id, requested_by)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(draft.id, key, JSON.stringify(snap), inputHashOf(snap), open ? open.id : null, actor || null);
    const request = requestById(db, info.lastInsertRowid);
    logEvent(db, draft.id, 'ad_kw_request', open ? `#${request.id} (以前の #${open.id} は置き換え)` : `#${request.id}`, actor);
    return { ok: true, request, reused: false };
  })();
}

/** 依頼を取り消す。収集中でも取り消せる (進行中の結果は finishCollect で捨てられる) */
export function cancelRequest(db, draft, requestId, actor) {
  return db.transaction(() => {
    const cur = requestById(db, requestId);
    if (!cur || cur.draft_id !== draft.id) return { code: 'not_found', error: '依頼がありません' };
    if (!REQUEST_OPEN_STATUSES.includes(cur.status)) return { code: 'closed', error: 'この依頼はすでに閉じています' };
    db.prepare(`UPDATE ph_ad_kw_requests SET status = 'cancelled', collecting_seed = NULL, collecting_token = NULL,
      collecting_since = NULL, updated_at = ? WHERE id = ?`).run(nowIso(), cur.id);
    logEvent(db, draft.id, 'ad_kw_cancelled', `#${cur.id}`, actor);
    return { ok: true };
  })();
}

/** 収集の開始 (lease を取る)。同じ依頼で同時に 2 つ走らせない。@returns {{ok:true, token}|{code, error?, evidence?}} */
export function beginCollect(db, request, seed) {
  return db.transaction(() => {
    const cur = requestById(db, request.id);
    if (!cur || !REQUEST_OPEN_STATUSES.includes(cur.status)) {
      return { code: 'closed', error: 'この依頼は閉じています (取消されたか、新しい依頼に置き換わりました)' };
    }
    const since = cur.collecting_since ? Date.parse(cur.collecting_since) : NaN;
    if (cur.status === 'collecting' && Number.isFinite(since) && Date.now() - since < COLLECT_STALE_MS) {
      return { code: 'busy', error: `「${cur.collecting_seed}」を収集中です。終わってからもう一度押してください` };
    }
    const ev = db.prepare(`SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND seed = ?`).get(cur.id, seed);
    if (ev && ev.status !== 'failed') return { code: 'exists', evidence: ev };   // 取得済み (失敗だけは再試行できる)
    const n = db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND status != 'failed'`).get(cur.id).n;
    if (n >= MAX_SEEDS_PER_REQUEST) return { code: 'too_many', error: `種は 1 依頼につき ${MAX_SEEDS_PER_REQUEST} 個までです` };
    const token = randomBytes(8).toString('hex');
    db.prepare(`UPDATE ph_ad_kw_requests SET status = 'collecting', collecting_seed = ?, collecting_token = ?,
      collecting_since = ?, updated_at = ? WHERE id = ?`).run(seed, token, nowIso(), nowIso(), cur.id);
    return { ok: true, token };
  })();
}

/** 材料 1 件の状態。全部失敗は failed、1 つでも取れていれば partial (失敗や未実行がある) か success */
export function evidenceStatusOf(summary) {
  const s = summary || {};
  const success = Number(s.success) || 0, empty = Number(s.empty) || 0, failed = Number(s.failed) || 0, unrun = Number(s.unrun) || 0;
  if (failed === 0 && unrun === 0) return success > 0 ? 'success' : 'empty';
  return (success > 0 || empty > 0) ? 'partial' : 'failed';
}

/**
 * 収集の終了 (結果の保存)。lease (token・種) が一致するときだけ保存する。
 * 取消・置き換え・別の収集に奪われた後の結果は捨てる (§5「取消後の結果を保存しない」)。
 * @param outcome collectSuggestions の戻り値 {ok:true,result} | {ok:false,code,message}
 */
export function finishCollect(db, request, seed, token, outcome, { actor } = {}) {
  return db.transaction(() => {
    const cur = requestById(db, request.id);
    if (!cur) return { code: 'closed', error: '依頼がありません' };
    const leaseOk = cur.status === 'collecting' && cur.collecting_token === token && cur.collecting_seed === seed;
    if (!leaseOk) {
      // lease を持っていないので状態も触らない (持ち主が片づける)
      return { code: cur.status === 'cancelled' ? 'cancelled' : 'lost_lease', error: '収集の途中で依頼が閉じられたため、結果は保存しませんでした' };
    }
    const release = db.prepare(`UPDATE ph_ad_kw_requests SET status = 'review_ready', collecting_seed = NULL, collecting_token = NULL,
      collecting_since = NULL, updated_at = ? WHERE id = ?`);
    // 前回失敗した種の再試行 = 失敗行を置き換える (失敗行に候補は無いので採否に影響しない)
    db.prepare(`DELETE FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND seed = ? AND status = 'failed'`).run(cur.id, seed);
    const insEv = db.prepare(`
      INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json, error, fetched_at, created_by)
      VALUES (?, 'suggest', ?, ?, ?, ?, ?, ?, ?)
    `);
    if (!outcome || outcome.ok !== true) {
      const code = outcome?.code || 'unknown';
      const info = insEv.run(cur.id, seed, 'failed', '{}', '[]', `${code}: ${outcome?.message || ''}`.trim(), null, actor || null);
      release.run(nowIso(), cur.id);
      logEvent(db, cur.draft_id, 'ad_kw_collect_failed', `#${cur.id} 「${seed}」 ${code}`, actor);
      return { ok: true, evidence: db.prepare('SELECT * FROM ph_ad_kw_evidence WHERE id = ?').get(info.lastInsertRowid), added: 0, merged: 0 };
    }
    const r = outcome.result || {};
    const summary = r.summary || {};
    const status = evidenceStatusOf(summary);
    const info = insEv.run(cur.id, seed, status, JSON.stringify(summary), JSON.stringify(r.prefixes || []),
      null, r.fetchedAt || nowIso(), actor || null);
    const evidenceId = Number(info.lastInsertRowid);

    // 候補 = 観測した語。同じ依頼に同じ語 (大小文字違い含む) が既にあれば観測の一覧に足すだけ (先勝ち)。
    // 並び = 種の取得順 → prefix の順 (そのまま → あ〜わ → a〜z) → 語。総合点は付けない (§4.8)
    const prefixOrder = new Map((r.prefixes || []).map((p, i) => [p.source, i]));
    const find = db.prepare(`SELECT id, observed_json FROM ph_ad_kw_candidates WHERE request_id = ? AND kind = 'kw' AND value_norm = ?`);
    const upd = db.prepare('UPDATE ph_ad_kw_candidates SET observed_json = ?, observed_count = ? WHERE id = ?');
    const ins = db.prepare(`
      INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key)
      VALUES (?, 'kw', ?, ?, 'observed', ?, ?, 1, ?)
    `);
    let added = 0, merged = 0;
    const seenHere = new Set();
    for (const s of r.suggestions || []) {
      const value = normalizeKeyword(s?.keyword, { seed });
      if (!value) continue;
      const norm = value.toLowerCase();
      if (seenHere.has(norm)) continue;
      seenHere.add(norm);
      const obs = { evidence_id: evidenceId, seed, source: s.source || 'base' };
      const ex = find.get(cur.id, norm);
      if (ex) {
        let list = [];
        try { list = JSON.parse(ex.observed_json) || []; } catch (_) { list = []; }
        list.push(obs);
        upd.run(JSON.stringify(list), list.length, ex.id);
        merged += 1;
        continue;
      }
      const so = prefixOrder.has(obs.source) ? prefixOrder.get(obs.source) : 999;
      ins.run(cur.id, value, norm, evidenceId, JSON.stringify([obs]),
        `${String(evidenceId).padStart(8, '0')}|${String(so).padStart(3, '0')}|${norm}`);
      added += 1;
    }
    release.run(nowIso(), cur.id);
    logEvent(db, cur.draft_id, 'ad_kw_collected', `#${cur.id} 「${seed}」 ${status} 候補+${added} (既出 ${merged})`, actor);
    return { ok: true, evidence: db.prepare('SELECT * FROM ph_ad_kw_evidence WHERE id = ?').get(evidenceId), added, merged };
  })();
}

/**
 * 種 1 つを収集する (lease → miniPC → 保存)。collector = collectSuggestions (テストで差し替え)。
 * @returns {Promise<{ok:true, collected:boolean, evidence, added, merged, reused?:boolean, error?:string}|{ok:false, code, error}>}
 *   collected=false は「miniPC から取れなかった」(失敗として記録済み)。ok=false は「受け付けなかった」(何も記録しない)
 */
export async function collectSeed(db, draft, request, rawSeed, { actor, alphabet = false, collector } = {}) {
  const ns = normalizeSeed(rawSeed);
  if (ns.error) return { ok: false, code: 'bad_seed', error: ns.error };
  const seed = ns.seed;
  const b = beginCollect(db, request, seed);
  if (!b.ok) {
    if (b.code === 'exists') return { ok: true, collected: b.evidence.status !== 'failed', reused: true, evidence: b.evidence, added: 0, merged: 0 };
    return { ok: false, code: b.code, error: b.error };
  }
  let outcome;
  try {
    outcome = await collector(seed, { alphabet: !!alphabet });
  } catch (e) {
    outcome = { ok: false, code: 'unreachable', message: e?.message || String(e) };
  }
  const f = finishCollect(db, request, seed, b.token, outcome, { actor });
  if (!f.ok) return { ok: false, code: f.code, error: f.error };
  const failed = f.evidence.status === 'failed';
  return { ok: true, collected: !failed, evidence: f.evidence, added: f.added, merged: f.merged, error: failed ? f.evidence.error : undefined };
}

/** 候補ごとの最新の採否。Map<candidate_id, row> */
export function latestDecisionsOf(db, requestId) {
  const rows = db.prepare(`
    SELECT d.* FROM ph_ad_kw_decisions d
    JOIN (SELECT candidate_id, MAX(id) AS mid FROM ph_ad_kw_decisions WHERE request_id = ? GROUP BY candidate_id) m ON m.mid = d.id
  `).all(requestId);
  return new Map(rows.map((r) => [r.candidate_id, r]));
}

/**
 * 採否を 1 件記録する (append-only)。adopt のときは語とマッチタイプを人が確定する。
 * @param body {decision, keyword?, match_type?}
 */
export function recordDecision(db, draft, candidateId, body, actor) {
  const decision = String(body?.decision || '');
  if (!DECISIONS.includes(decision)) return { code: 'bad_decision', error: '採否の値が不正です' };
  return db.transaction(() => {
    const cand = db.prepare(`
      SELECT c.*, r.draft_id AS draft_id, r.status AS request_status
      FROM ph_ad_kw_candidates c JOIN ph_ad_kw_requests r ON r.id = c.request_id WHERE c.id = ?
    `).get(candidateId);
    if (!cand || cand.draft_id !== draft.id) return { code: 'not_found', error: '候補がありません' };
    if (!REQUEST_OPEN_STATUSES.includes(cand.request_status)) return { code: 'closed', error: '閉じた依頼の採否は変えられません (新しい依頼で集め直してください)' };
    let keyword = null, matchType = null;
    if (decision === 'adopt') {
      keyword = normalizeKeyword(body?.keyword == null || body.keyword === '' ? cand.value : body.keyword);
      if (!keyword) return { code: 'bad_keyword', error: '語が空か、80 文字を超えています' };
      matchType = String(body?.match_type || '');
      if (!MATCH_TYPES.includes(matchType)) return { code: 'bad_match_type', error: 'マッチタイプ (完全一致 / フレーズ一致 / 部分一致) を選んでください' };
    }
    const prev = db.prepare('SELECT id FROM ph_ad_kw_decisions WHERE candidate_id = ? ORDER BY id DESC LIMIT 1').get(cand.id);
    const info = db.prepare(`
      INSERT INTO ph_ad_kw_decisions (candidate_id, request_id, decision, keyword, match_type, supersedes_decision_id, actor)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(cand.id, cand.request_id, decision, keyword, matchType, prev ? prev.id : null, actor || null);
    logEvent(db, draft.id, 'ad_kw_decision',
      `「${cand.value}」→ ${DECISION_JA[decision]}${matchType ? ` (${MATCH_TYPE_JA[matchType]}${keyword !== cand.value ? `・語を「${keyword}」に` : ''})` : ''}`, actor);
    return { ok: true, decision: db.prepare('SELECT * FROM ph_ad_kw_decisions WHERE id = ?').get(info.lastInsertRowid) };
  })();
}

/** いま採用されている語 (最新の採否が adopt のもの) */
export function adoptedOf(db, requestId) {
  return [...latestDecisionsOf(db, requestId).values()]
    .filter((d) => d.decision === 'adopt')
    .sort((a, b) => a.candidate_id - b.candidate_id)
    .map((d) => ({ candidate_id: d.candidate_id, keyword: d.keyword, match_type: d.match_type }));
}

const decisionVersionOf = (db, requestId) =>
  db.prepare('SELECT COALESCE(MAX(id), 0) AS v FROM ph_ad_kw_decisions WHERE request_id = ?').get(requestId).v;

function exportView(row) {
  let body = null, copied = {};
  try { body = JSON.parse(row.body_json); } catch (_) { body = null; }
  try { copied = JSON.parse(row.copied_json || '{}') || {}; } catch (_) { copied = {}; }
  return {
    id: row.id, request_id: row.request_id, kind: row.kind, decision_version: row.decision_version,
    body, copied, created_by: row.created_by, created_at: row.created_at,
  };
}

/**
 * コピー本文を固定する。同じ採否版・同じ中身なら前回の履歴を返す (二重に作らない)
 * @returns {{ok:true, export, reused:boolean}|{code, error}}
 */
export function createExport(db, draft, requestId, actor) {
  return db.transaction(() => {
    const cur = requestById(db, requestId);
    if (!cur || cur.draft_id !== draft.id) return { code: 'not_found', error: '依頼がありません' };
    if (!REQUEST_OPEN_STATUSES.includes(cur.status)) return { code: 'closed', error: 'この依頼は閉じています' };
    const snap = exportSnapshot(adoptedOf(db, cur.id));
    if (snap.total === 0) return { code: 'nothing', error: '採用した語がありません (採用してマッチタイプを選んでから)' };
    const version = decisionVersionOf(db, cur.id);
    const body = JSON.stringify(snap);
    const hash = createHash('sha256').update(body).digest('hex').slice(0, 16);
    const last = db.prepare('SELECT * FROM ph_ad_kw_exports WHERE request_id = ? ORDER BY id DESC LIMIT 1').get(cur.id);
    if (last && last.decision_version === version && last.body_hash === hash) return { ok: true, export: exportView(last), reused: true };
    const info = db.prepare(`
      INSERT INTO ph_ad_kw_exports (request_id, draft_id, kind, decision_version, body_json, body_hash, created_by)
      VALUES (?, ?, 'search_keywords', ?, ?, ?, ?)
    `).run(cur.id, draft.id, version, body, hash, actor || null);
    logEvent(db, draft.id, 'ad_kw_export', `#${cur.id} 採否版 ${version}・${snap.total} 語`, actor);
    return { ok: true, export: exportView(db.prepare('SELECT * FROM ph_ad_kw_exports WHERE id = ?').get(info.lastInsertRowid)), reused: false };
  })();
}

/** 「コピーした」印 (クリップボードに書けたあとに呼ぶ)。Amazon に登録した印ではない */
export function markCopied(db, draft, exportId, matchType, actor) {
  if (!MATCH_TYPES.includes(matchType)) return { code: 'bad_match_type', error: 'マッチタイプが不正です' };
  const row = db.prepare('SELECT * FROM ph_ad_kw_exports WHERE id = ? AND draft_id = ?').get(exportId, draft.id);
  if (!row) return { code: 'not_found', error: 'コピー履歴がありません' };
  let copied = {};
  try { copied = JSON.parse(row.copied_json || '{}') || {}; } catch (_) { copied = {}; }
  copied[matchType] = nowIso();
  db.prepare('UPDATE ph_ad_kw_exports SET copied_json = ? WHERE id = ?').run(JSON.stringify(copied), row.id);
  logEvent(db, draft.id, 'ad_kw_copied', `コピー履歴 #${row.id} ${MATCH_TYPE_JA[matchType]}`, actor);
  return { ok: true, copied };
}

// ─── 表示の作法 (§4.8): 「参考」と書かず、出典ごとに具体的に ───

/** 取得範囲の文 (例: 「47 回中 3 回に候補あり・41 回は 0 件・3 回失敗・上限で 0 回未実行」) */
export function coverageText(coverage) {
  const c = coverage || {};
  if (!Number.isFinite(Number(c.requested)) || !('requested' in c)) return '取得範囲の記録がありません';
  const parts = [`${c.requested} 回中 ${Number(c.success) || 0} 回に候補あり・${Number(c.empty) || 0} 回は 0 件`];
  if (Number(c.failed) > 0) parts.push(`${c.failed} 回失敗`);
  if (Number(c.unrun) > 0) parts.push(`上限で ${c.unrun} 回未実行`);
  return parts.join('・');
}

/** 取得日の表示 (JST の月日)。 */
export function fetchedDateText(iso) {
  const t = iso ? Date.parse(iso) : NaN;
  if (!Number.isFinite(t)) return '取得日不明';
  const d = new Date(t + 9 * 3600 * 1000);
  return `${d.getUTCMonth() + 1}月${d.getUTCDate()}日`;
}

/** 候補の出典の一文 (§4.8 サジェスト行) */
export function observedText(fetchedAt) {
  return `Amazon サジェストで観測・取得日 ${fetchedDateText(fetchedAt)}。検索回数は不明`;
}

/** prefix の出典ラベル: base → そのまま / hiragana:あ → +あ / alphabet:a → +a */
export function sourceLabel(source) {
  const s = String(source || 'base');
  if (s === 'base') return 'そのまま';
  const i = s.indexOf(':');
  return i > 0 ? `+${s.slice(i + 1)}` : s;
}

export const EVIDENCE_STATUS_JA = { success: '取得済み', partial: '一部取得', empty: '候補なし (0 件)', failed: '取得失敗' };

/**
 * 画面用の状態 (JSON にして埋め込む / GET で返す)。開いている依頼が無ければ request=null
 */
export function stateForDraft(db, draft, { configured = false } = {}) {
  const base = {
    configured: !!configured, request: null, stale: false, seeds: [], candidates: [], adopted_count: 0,
    exports: [], decision_version: 0,
    limits: { seed_max_len: SEED_MAX_LEN, max_seeds: MAX_SEEDS_PER_REQUEST },
    labels: { decision: DECISION_JA, match_type: MATCH_TYPE_JA, evidence_status: EVIDENCE_STATUS_JA },
    match_types: MATCH_TYPES,
  };
  const request = openRequestOf(db, draft.id);
  if (!request) return base;
  let snapshot = null;
  try { snapshot = JSON.parse(request.product_snapshot_json); } catch (_) { snapshot = null; }
  const stale = inputHashOf(productSnapshotOf(draft)) !== request.input_hash;
  const since = request.collecting_since ? Date.parse(request.collecting_since) : NaN;
  const collectingStale = request.status === 'collecting' && (!Number.isFinite(since) || Date.now() - since >= COLLECT_STALE_MS);
  const counts = new Map(db.prepare(
    'SELECT evidence_id, COUNT(*) AS n FROM ph_ad_kw_candidates WHERE request_id = ? GROUP BY evidence_id'
  ).all(request.id).map((r) => [r.evidence_id, r.n]));
  const seeds = db.prepare('SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? ORDER BY id').all(request.id).map((e) => {
    let coverage = {};
    try { coverage = JSON.parse(e.coverage_json) || {}; } catch (_) { coverage = {}; }
    return {
      id: e.id, seed: e.seed, source: e.source, status: e.status, status_ja: EVIDENCE_STATUS_JA[e.status] || e.status,
      coverage, coverage_text: e.status === 'failed' && !('requested' in coverage) ? '' : coverageText(coverage),
      fetched_at: e.fetched_at, fetched_text: e.fetched_at ? observedText(e.fetched_at) : null,
      error: e.error, candidate_count: counts.get(e.id) || 0,
    };
  });
  const decisions = latestDecisionsOf(db, request.id);
  const candidates = db.prepare('SELECT * FROM ph_ad_kw_candidates WHERE request_id = ? ORDER BY sort_key, id').all(request.id).map((c) => {
    let observed = [];
    try { observed = JSON.parse(c.observed_json) || []; } catch (_) { observed = []; }
    const d = decisions.get(c.id) || null;
    return {
      id: c.id, value: c.value, origin: c.origin, evidence_id: c.evidence_id, observed_count: c.observed_count,
      observed: observed.map((o) => ({ ...o, source_label: sourceLabel(o.source) })),
      decision: d ? { id: d.id, decision: d.decision, keyword: d.keyword, match_type: d.match_type, actor: d.actor, created_at: d.created_at } : null,
    };
  });
  const exports = db.prepare('SELECT * FROM ph_ad_kw_exports WHERE request_id = ? ORDER BY id DESC LIMIT 5').all(request.id).map(exportView);
  return {
    ...base,
    request: {
      id: request.id, status: request.status, collecting_seed: request.collecting_seed, collecting_since: request.collecting_since,
      collecting_stale: collectingStale, created_at: request.created_at, requested_by: request.requested_by, snapshot,
    },
    stale, seeds, candidates,
    adopted_count: candidates.filter((c) => c.decision && c.decision.decision === 'adopt').length,
    exports, decision_version: decisionVersionOf(db, request.id),
  };
}
