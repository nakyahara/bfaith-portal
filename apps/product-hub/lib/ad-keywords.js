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
 *
 * lease の決まり (PR #1408 Codex R1 補足で明文化):
 *   - 収集を始めるとき requests 行に token を置く。保存は token・種・状態が一致するときだけ
 *   - COLLECT_STALE_MS (3 分) は「**奪ってよくなる**期限」であって「保存できなくなる期限」ではない。
 *     3 分を過ぎても誰も奪っていなければ、遅れて終わった収集の結果は正しいので保存する。
 *     奪われた (token が変わった) / 取消された / 置き換えられた ときだけ捨てる
 */
import { createHash, randomBytes } from 'node:crypto';
import { logEvent } from '../db.js';
import { MATCH_TYPES, MATCH_TYPE_JA, COPY_BLOCKS, COPY_BLOCK_JA, normalizeKeyword, exportSnapshot } from './ad-keywords-export.js';
import { parseAsinList } from '../../../lib/asin.js';

export const MAX_ASINS_PER_REQUEST = 5;   // 競合 ASIN (商品ターゲット) は 1 依頼 5 件まで (検索して上位を目視で選ぶ量)

export const REQUEST_OPEN_STATUSES = ['collecting', 'review_ready'];
export const DECISIONS = ['adopt', 'hold', 'reject', 'undecided'];
export const DECISION_JA = { adopt: '採用', hold: '保留', reject: '却下', undecided: '未採用' };
export const SEED_MAX_LEN = 60;                 // miniPC 側 (keyword-suggest-service.js) と同じ
export const MAX_SEEDS_PER_REQUEST = 20;        // 1 種 ≈ 47 リクエスト。20 種 ≈ 940 回 (人が目視していた量の上限)
export const COLLECT_STALE_MS = 3 * 60 * 1000;  // miniPC の全体期限 40 秒 + Render 側の待ち 45 秒 + 余裕。これを越えた「収集中」は奪ってよい

const nowIso = () => new Date().toISOString();
const parseJson = (s, fallback) => { try { const v = JSON.parse(s); return v == null ? fallback : v; } catch (_) { return fallback; } };

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
const evidenceById = (db, id) => db.prepare('SELECT * FROM ph_ad_kw_evidence WHERE id = ?').get(id) || null;
/** その種の最新の取得回 (取り直しは行を足すので、最新 = いまの状態) */
const latestEvidenceOfSeed = (db, requestId, seed) =>
  db.prepare(`SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND seed = ? ORDER BY id DESC LIMIT 1`).get(requestId, seed) || null;
/** その種に「取れた」取得回 (failed 以外) があるか */
const seedHasValid = (db, requestId, seed) =>
  !!db.prepare(`SELECT 1 FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND seed = ? AND status != 'failed' LIMIT 1`).get(requestId, seed);
/** 「取れた」種の数 (上限の判定用。失敗しかない種は数えない) */
const validSeedCount = (db, requestId) =>
  db.prepare(`SELECT COUNT(DISTINCT seed) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND status != 'failed'`).get(requestId).n;

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
    // 同じキーで閉じた依頼 (取消・置き換え済み) を返すと、呼び手はそのまま収集して 409 で詰まる。閉じていることを伝える
    if (same && !REQUEST_OPEN_STATUSES.includes(same.status)) return { code: 'closed', error: 'この依頼は閉じています (画面を読み直してください)' };
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

/**
 * 収集の開始 (lease を取る)。同じ依頼で同時に 2 つ走らせない。
 * 取得済みの種は再利用する (exists)。ただし 失敗 / 取り直し指定の一部取得 / 条件を広げる (a〜z を足す) は取りに行く
 * (PR #1408 R1 #5)。
 * @returns {{ok:true, token, existing:object|null}|{code, error?, evidence?}}
 */
export function beginCollect(db, request, seed, { alphabet = false, retake = false } = {}) {
  return db.transaction(() => {
    const cur = requestById(db, request.id);
    if (!cur || !REQUEST_OPEN_STATUSES.includes(cur.status)) {
      return { code: 'closed', error: 'この依頼は閉じています (取消されたか、新しい依頼に置き換わりました)' };
    }
    const since = cur.collecting_since ? Date.parse(cur.collecting_since) : NaN;
    if (cur.status === 'collecting' && Number.isFinite(since) && Date.now() - since < COLLECT_STALE_MS) {
      return { code: 'busy', error: `「${cur.collecting_seed}」を収集中です。終わってからもう一度押してください` };
    }
    const ev = latestEvidenceOfSeed(db, cur.id, seed);
    if (ev) {
      const opts = parseJson(ev.options_json, {});
      const widen = !!alphabet && !opts.alphabet;                        // 条件を広げる → 取り直す
      const retakeable = ev.status === 'failed' || (retake && ev.status === 'partial');
      if (!widen && !retakeable) return { code: 'exists', evidence: ev };
    }
    // 上限は「取れた種」で数える。失敗しかない種を取り直して取れると 1 つ増えるので、そのときも数える (R2 #10)
    if (!seedHasValid(db, cur.id, seed) && validSeedCount(db, cur.id) >= MAX_SEEDS_PER_REQUEST) {
      return { code: 'too_many', error: `種は 1 依頼につき ${MAX_SEEDS_PER_REQUEST} 個までです` };
    }
    const token = randomBytes(8).toString('hex');
    db.prepare(`UPDATE ph_ad_kw_requests SET status = 'collecting', collecting_seed = ?, collecting_token = ?,
      collecting_since = ?, updated_at = ? WHERE id = ?`).run(seed, token, nowIso(), nowIso(), cur.id);
    return { ok: true, token, existing: ev };
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
 * 材料 (evidence) は**取得回ごとに 1 行を足す** (上書きしない)。取り直しが失敗しても前の取得回は残る。
 * 候補の観測は「その語を観測した取得回」を指すので、前回だけで観測した語の日時・出典は今回の結果に書き換わらない (R2 #4)。
 * 同じ種・同じ出方 (prefix) の観測は 1 つ (取り直しで二重に足さない)。
 * @param outcome collectSuggestions の戻り値 {ok:true,result} | {ok:false,code,message}
 * @returns {{ok:true, collected:boolean, evidence, added, merged, previous_ok?:boolean, error?:string}|{code, error}}
 */
export function finishCollect(db, request, seed, token, outcome, { actor, alphabet = false } = {}) {
  return db.transaction(() => {
    const cur = requestById(db, request.id);
    if (!cur) return { code: 'closed', error: '依頼がありません' };
    const leaseOk = cur.status === 'collecting' && cur.collecting_token === token && cur.collecting_seed === seed;
    if (!leaseOk) {
      // lease を持っていないので状態も触らない (持ち主が片づける)
      return { code: cur.status === 'cancelled' ? 'cancelled' : 'lost_lease', error: '収集の途中で依頼が閉じられたため、結果は保存しませんでした' };
    }
    const release = () => db.prepare(`UPDATE ph_ad_kw_requests SET status = 'review_ready', collecting_seed = NULL, collecting_token = NULL,
      collecting_since = NULL, updated_at = ? WHERE id = ?`).run(nowIso(), cur.id);
    const previousOk = seedHasValid(db, cur.id, seed);
    const insert = (row) => Number(db.prepare(`
      INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, options_json, coverage_json, raw_json, error, fetched_at, created_by)
      VALUES (?, 'suggest', ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(cur.id, seed, row.status, row.options_json, row.coverage_json, row.raw_json, row.error, row.fetched_at, actor || null).lastInsertRowid);
    if (!outcome || outcome.ok !== true) {
      const code = outcome?.code || 'unknown';
      const error = `${code}: ${outcome?.message || ''}`.trim();
      const evidence = evidenceById(db, insert({ status: 'failed', options_json: JSON.stringify({ alphabet: !!alphabet }), coverage_json: '{}', raw_json: '[]', error, fetched_at: null }));
      release();
      logEvent(db, cur.draft_id, 'ad_kw_collect_failed', `#${cur.id} 「${seed}」 ${code}${previousOk ? ' (前の取得回はそのまま)' : ''}`, actor);
      return { ok: true, collected: false, evidence, added: 0, merged: 0, previous_ok: previousOk, error };
    }
    const r = outcome.result || {};
    const summary = r.summary || {};
    const status = evidenceStatusOf(summary);
    const evidenceId = insert({
      status, options_json: JSON.stringify({ alphabet: !!(r.options?.alphabet ?? alphabet) }),
      coverage_json: JSON.stringify(summary), raw_json: JSON.stringify(r.prefixes || []), error: null, fetched_at: r.fetchedAt || nowIso(),
    });

    // 候補 = 観測した語。同じ依頼に同じ語 (大小文字違い含む) が既にあれば観測の一覧に足すだけ (先勝ち)。
    // 同じ種・同じ出方で既に観測していれば足さない (取り直しで二重にしない)。
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
        const list = parseJson(ex.observed_json, []);
        if (list.some((o) => o.seed === seed && o.source === obs.source)) continue;   // 同じ種・同じ出方 → 二重に足さない
        list.push(obs);
        upd.run(JSON.stringify(list), list.length, ex.id);
        merged += 1;
        continue;
      }
      const so = prefixOrder.has(obs.source) ? prefixOrder.get(obs.source) : 999;
      // 並びの先頭は「その種の最初の取得回」の id (取り直しても種の並びが動かないように)
      const firstId = db.prepare(`SELECT MIN(id) AS m FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'suggest' AND seed = ?`).get(cur.id, seed).m || evidenceId;
      ins.run(cur.id, value, norm, evidenceId, JSON.stringify([obs]),
        `${String(firstId).padStart(8, '0')}|${String(so).padStart(3, '0')}|${norm}`);
      added += 1;
    }
    release();
    logEvent(db, cur.draft_id, 'ad_kw_collected', `#${cur.id} 「${seed}」 ${status} 候補+${added} (既出 ${merged})${previousOk ? ' (取り直し)' : ''}`, actor);
    return { ok: true, collected: true, evidence: evidenceById(db, evidenceId), added, merged, previous_ok: previousOk };
  })();
}

/**
 * 種 1 つを収集する (lease → miniPC → 保存)。collector = collectSuggestions (テストで差し替え)。
 * @returns {Promise<{ok:true, collected:boolean, evidence, added, merged, reused?:boolean, error?:string}|{ok:false, code, error}>}
 *   collected=false は「miniPC から取れなかった」(失敗として記録済み)。ok=false は「受け付けなかった」(何も記録しない)
 */
export async function collectSeed(db, draft, request, rawSeed, { actor, alphabet = false, retake = false, collector } = {}) {
  const ns = normalizeSeed(rawSeed);
  if (ns.error) return { ok: false, code: 'bad_seed', error: ns.error };
  const seed = ns.seed;
  const b = beginCollect(db, request, seed, { alphabet: !!alphabet, retake: !!retake });
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
  const f = finishCollect(db, request, seed, b.token, outcome, { actor, alphabet: !!alphabet });
  if (!f.ok) return { ok: false, code: f.code, error: f.error };
  return { ok: true, collected: f.collected, evidence: f.evidence, added: f.added, merged: f.merged, previous_ok: f.previous_ok, error: f.error };
}

/**
 * 競合 ASIN を人が入れる (PR2-C)。= 商品ターゲットの候補 (kind='asin'・origin='input')。
 * 材料 (evidence) は source='input' の行 (誰がいつ入れたか)。自動では出さない (検索して上位を目視で選ぶのは人)。
 * 自分の ASIN・形式違い・重複・上限超えは弾く。
 * @returns {{ok:true, added:string[], skipped:Array<{asin,reason}>, invalid:string[]}|{code, error}}
 */
export function addCompetitorAsins(db, draft, requestId, rawList, actor) {
  const { asins, invalid } = parseAsinList(rawList, { max: 50 });
  if (asins.length === 0 && invalid.length === 0) return { code: 'bad_asin', error: 'ASIN を入力してください (10 桁の英数字。Amazon の URL でも可)' };
  return db.transaction(() => {
    const cur = requestById(db, requestId);
    if (!cur || cur.draft_id !== draft.id) return { code: 'not_found', error: '依頼がありません' };
    if (!REQUEST_OPEN_STATUSES.includes(cur.status)) return { code: 'closed', error: 'この依頼は閉じています' };
    const own = String(draft.asin || '').trim().toUpperCase();
    const existing = new Set(db.prepare(`SELECT value_norm FROM ph_ad_kw_candidates WHERE request_id = ? AND kind = 'asin'`).all(cur.id).map((r) => r.value_norm));
    const added = [], skipped = [];
    const insEv = db.prepare(`
      INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, options_json, coverage_json, raw_json, error, fetched_at, created_by)
      VALUES (?, 'input', ?, 'success', '{}', '{}', ?, NULL, ?, ?)
    `);
    const insCand = db.prepare(`
      INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key)
      VALUES (?, 'asin', ?, ?, 'input', ?, ?, 1, ?)
    `);
    for (const asin of asins) {
      if (asin === own) { skipped.push({ asin, reason: '自分の商品の ASIN です' }); continue; }
      if (existing.has(asin)) { skipped.push({ asin, reason: '入力済み' }); continue; }
      if (existing.size >= MAX_ASINS_PER_REQUEST) { skipped.push({ asin, reason: `1 依頼 ${MAX_ASINS_PER_REQUEST} 件まで` }); continue; }
      const now = nowIso();
      const evId = Number(insEv.run(cur.id, asin, JSON.stringify({ added_by: actor || null, added_at: now }), now, actor || null).lastInsertRowid);
      insCand.run(cur.id, asin, asin, evId, JSON.stringify([{ evidence_id: evId, seed: asin, source: 'input' }]), `asin|${String(evId).padStart(8, '0')}`);
      existing.add(asin);
      added.push(asin);
    }
    if (added.length) logEvent(db, draft.id, 'ad_kw_asin_added', `#${cur.id} ${added.join(', ')}`, actor);
    return { ok: true, added, skipped, invalid };
  })();
}

// ─── 競合 ASIN の ABA 検索語 (PR2-B2)。miniPC の aba.db に取込済みの週を引くだけ (走査しない・Render から Amazon を呼ばない) ───
// 状態 = miniPC (aba-service.js) の 4 状態 + failed (通信・応答の失敗)。「該当なし」は証明つき (full かつ捨てた行 0 かつ prune 前の週) のときだけ
export const ABA_STATUS_JA = {
  found: '該当あり', none: '該当なし (その週のクリック上位 3 に入っていない)',
  not_covered: '判定できない (取込が不完全)', no_week: 'レポート無し (取込済みの週がまだない)', failed: '取得失敗',
};
export const ABA_COVERAGE_JA = { complete: '網羅 (その週の全語)', partial: '一部 (欠けがあり得る)', unknown: '不明' };
export const ABA_REASON_JA = {
  mode_unknown: '取込時の保存方式が記録されていない', incomplete_ingest: '取込で捨てた行がある', not_watched: 'その週は監視 ASIN の分しか保存していない',
  pruned: '保持期限で一部を消した週', week_not_found: '指定した週の取込がない', no_ingested_week: '取込済みの週がまだない',
};
/** 対象週の表示 (例: 9月13日〜9月19日)。週は UTC の日曜〜土曜 (aba.db の週) */
export function abaWeekText(weekStart, weekEnd) {
  const md = (ymd) => { const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd || '')); return m ? `${Number(m[2])}月${Number(m[3])}日` : null; };
  const a = md(weekStart), b = md(weekEnd);
  if (!a) return null;
  return b ? `${a}〜${b}` : a;
}
/** ABA の出典の一文 (§4.8 ABA 行) */
export function abaObservedText(asin, weekStart, weekEnd) {
  return `競合 ASIN ${asin} がクリック上位 3 商品に含まれた検索語・対象週 ${abaWeekText(weekStart, weekEnd) || '不明'}`;
}
const latestAbaEvidence = (db, requestId, asin) =>
  db.prepare(`SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = ? ORDER BY id DESC LIMIT 1`).get(requestId, asin) || null;
/** ABA の材料 1 行の要約 (画面用) */
export function abaSummaryOf(ev) {
  if (!ev) return null;
  const c = parseJson(ev.coverage_json, {});
  const status = c.aba_status || 'failed';
  return {
    evidence_id: ev.id, status, status_ja: ABA_STATUS_JA[status] || status,
    coverage: c.coverage || null, coverage_ja: c.coverage ? (ABA_COVERAGE_JA[c.coverage] || c.coverage) : null,
    reason: c.reason || null, reason_ja: c.reason ? (ABA_REASON_JA[c.reason] || c.reason) : null,
    week_start: c.week_start || null, week_end: c.week_end || null, week_text: abaWeekText(c.week_start, c.week_end),
    observed_text: c.week_start ? abaObservedText(ev.seed, c.week_start, c.week_end) : null,
    term_count: Number(c.term_count) || 0, proof: c.proof || null, fetched_at: ev.fetched_at, error: ev.error,
  };
}

/**
 * 競合 ASIN 1 件の ABA 検索語を引く (受付 → miniPC → 保存)。lookup = lookupAbaTerms (テストで差し替え)。
 * 材料 (evidence) は source='aba'・seed=ASIN で**取得回ごとに 1 行** (上書きしない)。候補は kind='kw' origin='observed'
 * (観測 {seed: ASIN, source:'aba', week_start, rank, click_position, click_share, conversion_share})。
 * 取得済み (failed 以外) なら miniPC を呼ばず再利用。retake = 取り直す (新しい週の取込があればその週で。前の材料と候補・採否は残る)
 * 🚨 lease は取らない (aba.db の索引引きは 1 秒前後・読むだけ・別の収集と並んでも害が無い)。
 *    代わりに、待つ間に別の画面が同じ ASIN・同じ週を保存していれば二重に足さない (finishAbaLookup)
 * @returns {Promise<{ok:true, looked_up:boolean, reused?:boolean, evidence, added, merged, previous_ok?:boolean, error?:string, aba}|{ok:false, code, error}>}
 *   looked_up=false は「miniPC から取れなかった」(失敗として記録済み)。ok=false は「受け付けなかった」(何も記録しない)
 */
export async function lookupAbaForAsin(db, draft, candidateId, { actor, retake = false, lookup } = {}) {
  const cand = db.prepare(`
    SELECT c.*, r.draft_id AS draft_id, r.status AS request_status
    FROM ph_ad_kw_candidates c JOIN ph_ad_kw_requests r ON r.id = c.request_id WHERE c.id = ? AND c.kind = 'asin'
  `).get(candidateId);
  if (!cand || cand.draft_id !== draft.id) return { ok: false, code: 'not_found', error: '競合 ASIN がありません' };
  if (!REQUEST_OPEN_STATUSES.includes(cand.request_status)) return { ok: false, code: 'closed', error: 'この依頼は閉じています' };
  const asin = cand.value;
  const latest = latestAbaEvidence(db, cand.request_id, asin);
  if (latest && latest.status !== 'failed' && !retake) {
    return { ok: true, looked_up: true, reused: true, evidence: latest, added: 0, merged: 0, aba: abaSummaryOf(latest) };
  }
  const beforeId = latest ? latest.id : 0;
  let outcome;
  try {
    // register = miniPC の監視 ASIN に登録する (保持期限のあともその ASIN の語が残る)。人が入れた ASIN だけ (自動で増やさない)
    outcome = await lookup(asin, { register: true });
  } catch (e) {
    outcome = { ok: false, code: 'unreachable', message: e?.message || String(e) };
  }
  return finishAbaLookup(db, cand, outcome, { actor, beforeId });
}

/** ABA の結果の保存 (1 トランザクション)。閉じた依頼には保存しない。失敗も取得回として残す (0 件と混ぜない) */
function finishAbaLookup(db, cand, outcome, { actor, beforeId }) {
  return db.transaction(() => {
    const cur = requestById(db, cand.request_id);
    if (!cur || !REQUEST_OPEN_STATUSES.includes(cur.status)) {
      return { ok: false, code: cur && cur.status === 'cancelled' ? 'cancelled' : 'closed', error: '待っている間に依頼が閉じられたため、結果は保存しませんでした' };
    }
    const asin = cand.value;
    const previousOk = !!db.prepare(`SELECT 1 FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = ? AND status != 'failed' LIMIT 1`).get(cur.id, asin);
    const insert = (row) => Number(db.prepare(`
      INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, options_json, coverage_json, raw_json, error, fetched_at, created_by)
      VALUES (?, 'aba', ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(cur.id, asin, row.status, row.options_json, row.coverage_json, row.raw_json, row.error, row.fetched_at, actor || null).lastInsertRowid);
    if (!outcome || outcome.ok !== true) {
      const code = outcome?.code || 'unknown';
      const error = `${code}: ${outcome?.message || ''}`.trim();
      const evidence = evidenceById(db, insert({ status: 'failed', options_json: '{}', coverage_json: '{}', raw_json: '[]', error, fetched_at: null }));
      logEvent(db, cur.draft_id, 'ad_kw_aba_failed', `#${cur.id} ${asin} ${code}${previousOk ? ' (前の取得回はそのまま)' : ''}`, actor);
      return { ok: true, looked_up: false, evidence, added: 0, merged: 0, previous_ok: previousOk, error, aba: abaSummaryOf(evidence) };
    }
    const r = outcome.result;
    const item = r.items[0];
    const week = r.week || null;
    // 待つ間に別の画面が同じ ASIN・同じ週を保存していれば、それを返す (二重に足さない)
    if (week) {
      const dup = db.prepare(`SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = ? AND id > ? AND status != 'failed' ORDER BY id DESC LIMIT 1`).get(cur.id, asin, beforeId);
      if (dup && parseJson(dup.coverage_json, {}).week_start === week.week_start) {
        return { ok: true, looked_up: true, reused: true, evidence: dup, added: 0, merged: 0, previous_ok: previousOk, aba: abaSummaryOf(dup) };
      }
    }
    // evidence.status: found = success (網羅) / partial (欠けあり)・none = empty (証明つきの 0 件)・
    // not_covered = partial (取れたが「無い」とは言えない)・no_week = failed (レポート無し。取込が進めば「もう一度」で取れる)
    const status = item.status === 'found' ? (item.coverage === 'complete' ? 'success' : 'partial')
      : item.status === 'none' ? 'empty' : item.status === 'not_covered' ? 'partial' : 'failed';
    const coverage = {
      aba_status: item.status, proof: item.proof || null, coverage: item.coverage, reason: item.reason || null,
      week_start: week ? week.week_start : null, week_end: week ? week.week_end : null, week_ingested_at: week ? (week.ingested_at || null) : null,
      week_mode: week ? (week.mode || null) : null, week_coverage: r.week_coverage || null, term_count: item.terms.length, registered: r.registered === true,
    };
    const error = item.status === 'no_week' ? `no_week: ${ABA_REASON_JA[item.reason] || item.reason || 'レポート無し'}` : null;
    const evidenceId = insert({
      status, options_json: JSON.stringify({ register: true, requested_week: r.requested_week || null }),
      coverage_json: JSON.stringify(coverage), raw_json: JSON.stringify(item.terms), error, fetched_at: nowIso(),
    });
    // 候補 = その週にその ASIN がクリック上位 3 に入った検索語。並び = ASIN (最初の取得回) → ABA の検索頻度順位 (小さい順) → 語。
    // 同じ語がサジェストで既にあれば観測を足すだけ (先勝ち)。同じ ASIN・同じ週の観測は 1 つ (取り直しで二重にしない)
    const find = db.prepare(`SELECT id, observed_json FROM ph_ad_kw_candidates WHERE request_id = ? AND kind = 'kw' AND value_norm = ?`);
    const upd = db.prepare('UPDATE ph_ad_kw_candidates SET observed_json = ?, observed_count = ? WHERE id = ?');
    const ins = db.prepare(`
      INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key)
      VALUES (?, 'kw', ?, ?, 'observed', ?, ?, 1, ?)
    `);
    const firstId = db.prepare(`SELECT MIN(id) AS m FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = ?`).get(cur.id, asin).m || evidenceId;
    const rankOf = (t) => (Number.isFinite(Number(t.search_frequency_rank)) ? Number(t.search_frequency_rank) : 9999999);
    const terms = [...item.terms].sort((a, b) => rankOf(a) - rankOf(b));
    let added = 0, merged = 0;
    const seenHere = new Set();
    for (const t of terms) {
      const value = normalizeKeyword(t.search_term);
      if (!value) continue;
      const norm = value.toLowerCase();
      if (seenHere.has(norm)) continue;
      seenHere.add(norm);
      const obs = {
        evidence_id: evidenceId, seed: asin, source: 'aba', week_start: coverage.week_start,
        rank: t.search_frequency_rank ?? null, click_position: t.click_position ?? null,
        click_share: t.click_share ?? null, conversion_share: t.conversion_share ?? null, department: t.department ?? null,
      };
      const ex = find.get(cur.id, norm);
      if (ex) {
        const list = parseJson(ex.observed_json, []);
        if (list.some((o) => o.seed === asin && o.source === 'aba' && o.week_start === obs.week_start)) continue;
        list.push(obs);
        upd.run(JSON.stringify(list), list.length, ex.id);
        merged += 1;
        continue;
      }
      ins.run(cur.id, value, norm, evidenceId, JSON.stringify([obs]), `${String(firstId).padStart(8, '0')}|${String(rankOf(t)).padStart(7, '0')}|${norm}`);
      added += 1;
    }
    logEvent(db, cur.draft_id, 'ad_kw_aba_looked_up',
      `#${cur.id} ${asin} ${item.status}${coverage.week_start ? ` 週 ${coverage.week_start}` : ''} 候補+${added} (既出 ${merged})${previousOk ? ' (取り直し)' : ''}`, actor);
    const evidence = evidenceById(db, evidenceId);
    return { ok: true, looked_up: true, reused: false, evidence, added, merged, previous_ok: previousOk, aba: abaSummaryOf(evidence) };
  })();
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
    if (decision === 'adopt' && cand.kind === 'asin') {
      // 商品ターゲット: 値は ASIN そのもの・マッチタイプは無い (キーワードのブロックに混ぜない)
      keyword = cand.value;
      if (body?.match_type) return { code: 'bad_match_type', error: '商品ターゲット (ASIN) にマッチタイプはありません' };
    } else if (decision === 'adopt') {
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
      `${cand.kind === 'asin' ? '商品ターゲット ' : ''}「${cand.value}」→ ${DECISION_JA[decision]}${matchType ? ` (${MATCH_TYPE_JA[matchType]}${keyword !== cand.value ? `・語を「${keyword}」に` : ''})` : ''}`, actor);
    return { ok: true, decision: db.prepare('SELECT * FROM ph_ad_kw_decisions WHERE id = ?').get(info.lastInsertRowid) };
  })();
}

/** いま採用されている語 / 商品ターゲット (最新の採否が adopt のもの)。kind = 'kw' | 'asin' */
export function adoptedOf(db, requestId) {
  const kindOf = new Map(db.prepare('SELECT id, kind FROM ph_ad_kw_candidates WHERE request_id = ?').all(requestId).map((c) => [c.id, c.kind]));
  return [...latestDecisionsOf(db, requestId).values()]
    .filter((d) => d.decision === 'adopt')
    .sort((a, b) => a.candidate_id - b.candidate_id)
    .map((d) => ({ candidate_id: d.candidate_id, kind: kindOf.get(d.candidate_id) || 'kw', keyword: d.keyword, match_type: d.match_type }));
}

const decisionVersionOf = (db, requestId) =>
  db.prepare('SELECT COALESCE(MAX(id), 0) AS v FROM ph_ad_kw_decisions WHERE request_id = ?').get(requestId).v;

function exportView(row) {
  return {
    id: row.id, request_id: row.request_id, kind: row.kind, decision_version: row.decision_version,
    body: parseJson(row.body_json, null), copied: parseJson(row.copied_json || '{}', {}),
    created_by: row.created_by, created_at: row.created_at,
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
    if (snap.total === 0) return { code: 'nothing', error: '採用したものがありません (語はマッチタイプを選んで採用・商品ターゲットは ASIN を採用してから)' };
    const version = decisionVersionOf(db, cur.id);
    const body = JSON.stringify(snap);
    const hash = createHash('sha256').update(body).digest('hex').slice(0, 16);
    const last = db.prepare('SELECT * FROM ph_ad_kw_exports WHERE request_id = ? ORDER BY id DESC LIMIT 1').get(cur.id);
    if (last && last.decision_version === version && last.body_hash === hash) return { ok: true, export: exportView(last), reused: true };
    const info = db.prepare(`
      INSERT INTO ph_ad_kw_exports (request_id, draft_id, kind, decision_version, body_json, body_hash, created_by)
      VALUES (?, ?, 'ad_copy', ?, ?, ?, ?)
    `).run(cur.id, draft.id, version, body, hash, actor || null);
    logEvent(db, draft.id, 'ad_kw_export', `#${cur.id} 採否版 ${version}・語 ${snap.keyword_total}・商品ターゲット ${snap.target_total}`, actor);
    return { ok: true, export: exportView(db.prepare('SELECT * FROM ph_ad_kw_exports WHERE id = ?').get(info.lastInsertRowid)), reused: false };
  })();
}

/** 「コピーした」印 (クリップボードに書けたあとに呼ぶ)。Amazon に登録した印ではない */
export function markCopied(db, draft, exportId, block, actor) {
  if (!COPY_BLOCKS.includes(block)) return { code: 'bad_match_type', error: 'コピーのブロック (マッチタイプ / 商品ターゲット) が不正です' };
  const row = db.prepare('SELECT * FROM ph_ad_kw_exports WHERE id = ? AND draft_id = ?').get(exportId, draft.id);
  if (!row) return { code: 'not_found', error: 'コピー履歴がありません' };
  const copied = parseJson(row.copied_json || '{}', {});
  copied[block] = nowIso();
  db.prepare('UPDATE ph_ad_kw_exports SET copied_json = ? WHERE id = ?').run(JSON.stringify(copied), row.id);
  logEvent(db, draft.id, 'ad_kw_copied', `コピー履歴 #${row.id} ${COPY_BLOCK_JA[block]}`, actor);
  return { ok: true, copied };
}

// ─── 表示の作法 (§4.8): 「参考」と書かず、出典ごとに具体的に ───

const STOPPED_JA = { deadline: '全体の期限で打ち切り', aborted: '中断で打ち切り', stuck: '通信が決着せず打ち切り' };

/** 取得範囲の文 (例: 「47 回中 3 回に候補あり・41 回は 0 件・3 回失敗・上限で 0 回未実行」) */
export function coverageText(coverage) {
  const c = coverage || {};
  if (!('requested' in c) || !Number.isFinite(Number(c.requested))) return '取得範囲の記録がありません';
  const parts = [`${c.requested} 回中 ${Number(c.success) || 0} 回に候補あり・${Number(c.empty) || 0} 回は 0 件`];
  if (Number(c.failed) > 0) parts.push(`${c.failed} 回失敗`);
  if (Number(c.unrun) > 0) parts.push(`${STOPPED_JA[c.stopped] ? STOPPED_JA[c.stopped] + '・' : '上限で '}${c.unrun} 回未実行`);
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

/** prefix の出典ラベル: base → そのまま / hiragana:あ → +あ / alphabet:a → +a / aba → ABA (競合 ASIN の検索語) */
export function sourceLabel(source) {
  const s = String(source || 'base');
  if (s === 'base') return 'そのまま';
  if (s === 'aba') return 'ABA';
  const i = s.indexOf(':');
  return i > 0 ? `+${s.slice(i + 1)}` : s;
}

export const EVIDENCE_STATUS_JA = { success: '取得済み', partial: '一部取得', empty: '候補なし (0 件)', failed: '取得失敗' };

/**
 * 画面用の状態 (JSON にして埋め込む / GET で返す)。開いている依頼が無ければ request=null
 */
export function stateForDraft(db, draft, { configured = false } = {}) {
  const base = {
    configured: !!configured, request: null, stale: false, seeds: [], candidates: [], asins: [], adopted_count: 0, adopted_asin_count: 0,
    own_asin: draft.asin ? String(draft.asin).trim().toUpperCase() : null,
    exports: [], decision_version: 0,
    limits: { seed_max_len: SEED_MAX_LEN, max_seeds: MAX_SEEDS_PER_REQUEST, max_asins: MAX_ASINS_PER_REQUEST, stale_ms: COLLECT_STALE_MS },
    labels: { decision: DECISION_JA, match_type: MATCH_TYPE_JA, copy_block: COPY_BLOCK_JA, evidence_status: EVIDENCE_STATUS_JA,
      aba_status: ABA_STATUS_JA, aba_coverage: ABA_COVERAGE_JA, aba_reason: ABA_REASON_JA },
    match_types: MATCH_TYPES,
  };
  const request = openRequestOf(db, draft.id);
  if (!request) return base;
  const snapshot = parseJson(request.product_snapshot_json, null);
  const stale = inputHashOf(productSnapshotOf(draft)) !== request.input_hash;
  const since = request.collecting_since ? Date.parse(request.collecting_since) : NaN;
  const collectingStale = request.status === 'collecting' && (!Number.isFinite(since) || Date.now() - since >= COLLECT_STALE_MS);
  const decisions = latestDecisionsOf(db, request.id);
  const evidenceRows = db.prepare('SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? ORDER BY id').all(request.id);
  const fetchedAtOf = new Map(evidenceRows.map((e) => [e.id, e.fetched_at]));
  // ABA の材料 (取得回) の対象週 (観測ごとに「週 ○月○日〜」を付ける)
  const abaWeekOf = new Map(evidenceRows.filter((e) => e.source === 'aba').map((e) => { const c = parseJson(e.coverage_json, {}); return [e.id, abaWeekText(c.week_start, c.week_end)]; }));
  const allCandidates = db.prepare('SELECT * FROM ph_ad_kw_candidates WHERE request_id = ? ORDER BY sort_key, id').all(request.id).map((c) => {
    const observed = parseJson(c.observed_json, []);
    const d = decisions.get(c.id) || null;
    const first = observed[0] || null;
    return {
      id: c.id, kind: c.kind, value: c.value, origin: c.origin, evidence_id: c.evidence_id, observed_count: c.observed_count,
      // 最初に観測した種と取得日 (取り直しても最初の観測の日付のまま — 別の取得回の結果に書き換えない)。
      // first_source = 最初の観測の出方 (aba なら「競合 ASIN の検索語」のグループに出す)
      seed: first ? first.seed : null, first_source: first ? (first.source || null) : null, first_fetched_at: first ? (fetchedAtOf.get(first.evidence_id) || null) : null,
      observed: observed.map((o) => ({ ...o, source_label: sourceLabel(o.source), fetched_at: fetchedAtOf.get(o.evidence_id) || null,
        ...(o.source === 'aba' ? { week_text: abaWeekOf.get(o.evidence_id) || abaWeekText(o.week_start) } : {}) })),
      decision: d ? { id: d.id, decision: d.decision, keyword: d.keyword, match_type: d.match_type, actor: d.actor, created_at: d.created_at } : null,
    };
  });
  // 検索語の候補 (kind='kw') と 商品ターゲットの候補 (kind='asin'・人の入力) は画面で別の表に出す
  const candidates = allCandidates.filter((c) => c.kind === 'kw');
  // ASIN ごとの ABA の取得回 (source='aba'・seed=ASIN)。いまの状態は最新の行。取り直しが失敗しても「取れた回」は残っている
  const abaBySeed = new Map();
  for (const e of evidenceRows) {
    if (e.source !== 'aba') continue;
    if (!abaBySeed.has(e.seed)) abaBySeed.set(e.seed, []);
    abaBySeed.get(e.seed).push(e);
  }
  const abaCandidateCount = new Map();
  for (const c of candidates) {
    for (const sd of new Set(c.observed.filter((o) => o.source === 'aba').map((o) => o.seed))) abaCandidateCount.set(sd, (abaCandidateCount.get(sd) || 0) + 1);
  }
  const asins = allCandidates.filter((c) => c.kind === 'asin').map((c) => {
    const rows = abaBySeed.get(c.value) || [];
    const latest = rows.length ? rows[rows.length - 1] : null;
    return {
      id: c.id, asin: c.value, added_by: (parseJson(evidenceRows.find((e) => e.id === c.evidence_id)?.raw_json, {}) || {}).added_by || null,
      added_at: fetchedAtOf.get(c.evidence_id) || null, decision: c.decision,
      aba: abaSummaryOf(latest), aba_fetch_count: rows.length,
      aba_previous_ok: !!(latest && latest.status === 'failed' && rows.some((e) => e.status !== 'failed')),
      aba_candidate_count: abaCandidateCount.get(c.value) || 0,
    };
  });
  // 種ごとの数: 観測した語 (別の種で先に出た語も含む) と、この種で初めて出た語 (PR #1408 R1 #9)
  const observedCount = new Map(), newCount = new Map();
  for (const c of candidates) {
    if (c.seed) newCount.set(c.seed, (newCount.get(c.seed) || 0) + 1);
    for (const sd of new Set(c.observed.map((o) => o.seed))) observedCount.set(sd, (observedCount.get(sd) || 0) + 1);
  }
  // 種 = サジェストの取得回の行をまとめる (input = 人が入れた ASIN の材料は「種」ではない)。いまの状態は最新の行。取り直しが失敗しても「取れた回」は残っている
  const bySeed = new Map();
  for (const e of evidenceRows) {
    if (e.source !== 'suggest') continue;
    if (!bySeed.has(e.seed)) bySeed.set(e.seed, []);
    bySeed.get(e.seed).push(e);
  }
  const seeds = [...bySeed.entries()].map(([seed, rows]) => {
    const latest = rows[rows.length - 1];
    const lastOk = [...rows].reverse().find((e) => e.status !== 'failed') || null;
    const coverage = parseJson(latest.coverage_json, {});
    const previousOk = latest.status === 'failed' && !!lastOk;
    return {
      id: latest.id, first_id: rows[0].id, seed, source: latest.source, status: latest.status,
      status_ja: previousOk ? '取り直し失敗 (前の取得回は残っています)' : (EVIDENCE_STATUS_JA[latest.status] || latest.status),
      previous_ok: previousOk, fetch_count: rows.length,
      options: parseJson(latest.options_json, {}),
      coverage, coverage_text: 'requested' in coverage ? coverageText(coverage) : '',
      fetched_at: latest.fetched_at || (lastOk ? lastOk.fetched_at : null),
      fetched_text: latest.fetched_at ? observedText(latest.fetched_at) : (lastOk && lastOk.fetched_at ? observedText(lastOk.fetched_at) + '（前の取得回）' : null),
      error: latest.error, candidate_count: observedCount.get(seed) || 0, new_count: newCount.get(seed) || 0,
    };
  });
  const exports = db.prepare('SELECT * FROM ph_ad_kw_exports WHERE request_id = ? ORDER BY id DESC LIMIT 5').all(request.id).map(exportView);
  return {
    ...base,
    request: {
      id: request.id, status: request.status, collecting_seed: request.collecting_seed, collecting_since: request.collecting_since,
      collecting_stale: collectingStale, created_at: request.created_at, requested_by: request.requested_by, snapshot,
    },
    stale, seeds, candidates, asins,
    adopted_count: candidates.filter((c) => c.decision && c.decision.decision === 'adopt').length,
    adopted_asin_count: asins.filter((c) => c.decision && c.decision.decision === 'adopt').length,
    exports, decision_version: decisionVersionOf(db, request.id),
  };
}
