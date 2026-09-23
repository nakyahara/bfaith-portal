/**
 * ABA (Amazon Brand Analytics 検索用語レポート) の参照 サービスAPI (SP広告KW PR2-B1・2026-09-23)
 * /service-api/aba にマウント
 *
 * 呼び手 = Render の product-hub (「📣 SP広告KW」タブ・競合 ASIN の注文ワード)。
 * 🚨 **走査しない・レポートを取りに行かない**。aba.db に**取込済みの週**を引くだけ (方針 B: 週次取込を full にして全語を保存。
 *    『Amazon_SP広告KW自動生成_設計方針_20260922.md』§5「進め方の決定」)。
 *
 * 「その週に行が無い = その週のクリック上位 3 に入っていない」と言ってよい条件 (Codex #1414 R1・R2):
 *   - その週の**取込時の mode が full** (aba_weeks.mode。いまの env ではなく、取り込んだときの値) かつ **捨てた行が 0** (skipped_count = 0)
 *   - watched の週は、その ASIN が**その週の走査済み** (last_scanned_week = その週) かつ **捨てた行が 0** のときだけ
 *   - **保持期限で非監視の語を消した週 (aba_weeks.pruned_at) は除く** = 何が消えたか分からないので「無い」とも「全部ある」とも言わない
 *   - それ以外 (mode 不明・捨てた行あり・未走査・prune 済み) は `not_covered` = 「該当なし」とは言わない (該当なし ≠ 注文なし)
 *   `found` も「行がある」という意味で、注文の証明ではない。coverage (complete / partial / unknown) を別に付ける
 *
 * 守ること:
 *   - 週は呼び手が固定できる (`week_start`)。無ければ取込済みの最新週。指定した週が無ければ最新週に**代替しない** (no_week)
 *   - 週の台帳と検索語は同じ読み取りトランザクションで取る (--force の取り直しが割り込んでも組み合わせがずれない)
 *   - 監視登録は**既定でしない** (`register:true` のときだけ)。full 取込中は書込が待たされる (BEGIN IMMEDIATE) ので、登録は読み取りの外で・失敗しても照会は返す
 *   - ASIN は 1 回 5 件まで (形式は lib/asin.js)。指標は原値 (順位 = search_frequency_rank・click_position はクリックシェア順位・share は 0〜1)。集計・換算はしない
 */
import { Router } from 'express';
import { okResponse, errorResponse } from './error-handler.js';
import { initAbaDB } from '../aba-keywords/db.js';
import { parseAsinList } from '../../lib/asin.js';

const router = Router();

export const MAX_ASINS = 5;
export const ASIN_STATUSES = ['found', 'none', 'not_covered', 'no_week'];

/** 取込済みの最新週 (parsed_count > 0 = レポートを最後まで読んで台帳を書いた週)。無ければ null */
export function latestIngestedWeek(db) {
  return db.prepare(`
    SELECT week_start, week_end, ingested_at, term_count, row_count, parsed_count, mode, skipped_count, pruned_at
    FROM aba_weeks WHERE parsed_count > 0 ORDER BY week_start DESC LIMIT 1
  `).get() || null;
}
/** 指定した週の台帳。無ければ null (最新週に代替しない) */
export function ingestedWeek(db, weekStart) {
  return db.prepare(`
    SELECT week_start, week_end, ingested_at, term_count, row_count, parsed_count, mode, skipped_count, pruned_at
    FROM aba_weeks WHERE week_start = ? AND parsed_count > 0
  `).get(weekStart) || null;
}

/**
 * その週の取込が「全部そろっている」と言えるか (週の単位)。full で捨てた行が 0 で、保持期限の削除 (prune) をしていない週だけ complete。
 * @returns {'complete'|'partial'|'unknown'}
 */
export function weekCoverage(week) {
  if (!week) return 'unknown';
  if (week.pruned_at) return 'partial';                       // 非監視の語を消したあと = 何が消えたか分からない (mode 不明より先に見る)
  if (week.mode == null) return 'unknown';
  if (week.mode === 'full') return week.skipped_count === 0 ? 'complete' : (week.skipped_count == null ? 'unknown' : 'partial');
  return 'partial';   // watched = 監視 ASIN 絡みの語だけ
}
/**
 * その週の解析で行を捨てていないか (捨てていれば、その行が当該 ASIN の語だった可能性を排除できない — Codex #1414 R2 #2)。
 * @returns {'clean'|'dirty'|'unknown'}
 */
export function weekParseQuality(week) {
  if (!week || week.skipped_count == null) return 'unknown';
  return week.skipped_count === 0 ? 'clean' : 'dirty';
}

/**
 * ASIN ごとに、取込済みの週の検索語を引く (読むだけ)。
 * @param {import('better-sqlite3').Database} db aba.db
 * @param {string[]} asins 正規化済み
 * @param {{weekStart?:string|null, register?:boolean}} opts weekStart = 固定したい週 (無ければ最新)。register = 監視に登録する (既定 false)
 * @returns {{week: object|null, requested_week: string|null, week_coverage: string, items: Array, registered: boolean, register_errors: Array}}
 */
export function lookupAsins(db, asins, { weekStart = null, register = false } = {}) {
  const selTerms = db.prepare(`
    SELECT department, search_term, search_frequency_rank, click_position, click_share, conversion_share
    FROM aba_search_terms WHERE asin = ? AND week_start = ?
    ORDER BY search_frequency_rank ASC, department ASC, search_term ASC
  `);
  const selWatch = db.prepare('SELECT last_scanned_week FROM aba_watch_asins WHERE asin = ?');
  // 週の台帳と検索語を同じスナップショットで読む (better-sqlite3 の transaction は読み取りだけでも 1 つの txn)
  const read = db.transaction(() => {
    const week = weekStart ? ingestedWeek(db, weekStart) : latestIngestedWeek(db);
    const coverage = weekCoverage(week);
    const items = [];
    for (const asin of asins) {
      if (!week) { items.push({ asin, status: 'no_week', proof: null, coverage: 'unknown', reason: weekStart ? 'week_not_found' : 'no_ingested_week', terms: [] }); continue; }
      const terms = selTerms.all(asin, week.week_start).map((r) => ({
        search_term: r.search_term, department: r.department,
        search_frequency_rank: r.search_frequency_rank, click_position: r.click_position,
        click_share: r.click_share, conversion_share: r.conversion_share,
      }));
      const w = selWatch.get(asin);
      const scannedThisWeek = !!(w && w.last_scanned_week === week.week_start);   // その週を走査した証拠 (>= ではない)
      const quality = weekParseQuality(week);                                      // 捨てた行が 0 か
      const pruned = !!week.pruned_at;
      // その ASIN について「上位 3 の語を網羅している」と言える条件:
      //   full: 週が complete (捨てた行 0・prune 前) / watched: その週を走査済み かつ 捨てた行 0 かつ prune 前
      const asinComplete = week.mode === 'full' ? coverage === 'complete'
        : (week.mode === 'watched' ? (scannedThisWeek && quality === 'clean' && !pruned) : false);
      // prune 済みは (mode や skipped_count が不明でも) partial・reason=pruned に統一 (Codex R3 任意)
      const asinCoverage = asinComplete ? 'complete' : (pruned ? 'partial' : (week.mode == null || quality === 'unknown' ? 'unknown' : 'partial'));
      if (terms.length > 0) {
        // found = 行がある (注文の証明ではない)。coverage = その ASIN の上位 3 の語が全部そろっているか
        items.push({ asin, status: 'found', proof: week.mode === 'full' ? 'week_ingested' : (scannedThisWeek ? 'scanned' : null), coverage: asinCoverage, reason: pruned ? 'pruned' : null, terms });
        continue;
      }
      if (asinComplete) { items.push({ asin, status: 'none', proof: week.mode === 'full' ? 'week_ingested' : 'scanned', coverage: 'complete', reason: null, terms: [] }); continue; }
      const reason = pruned ? 'pruned' : week.mode == null ? 'mode_unknown'
        : (week.mode === 'full' ? 'incomplete_ingest' : (scannedThisWeek ? 'incomplete_ingest' : 'not_watched'));
      items.push({ asin, status: 'not_covered', proof: null, coverage: 'unknown', reason, terms: [] });
    }
    return { week, week_coverage: coverage, items };
  });
  const result = read();
  // 監視登録は読み取りの外で、1 件ずつ。full 取込中 (BEGIN IMMEDIATE) は busy になり得るので、失敗しても照会の結果は返す
  const registerErrors = [];
  if (register) {
    const upsert = db.prepare(`
      INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count)
      VALUES (?, datetime('now'), datetime('now'), 1)
      ON CONFLICT(asin) DO UPDATE SET last_queried_at = datetime('now'), query_count = query_count + 1
    `);
    for (const asin of asins) {
      try { upsert.run(asin); } catch (e) { registerErrors.push({ asin, error: e.code || e.message }); }
    }
  }
  return { ...result, requested_week: weekStart || null, registered: register && registerErrors.length === 0, register_errors: registerErrors };
}

/**
 * POST /service-api/aba/lookup
 * body: { asins: string[] | string (1〜5 件), week_start?: 'YYYY-MM-DD' (固定したい週。無ければ最新), register?: boolean (既定 false) }
 * → { ok, result: { week, requested_week, week_coverage, items:[{asin, status, proof, coverage, reason, terms:[…]}], registered, register_errors, invalid } }
 */
router.post('/lookup', (req, res) => {
  const body = req.body || {};
  const raw = Array.isArray(body.asins) ? body.asins.map((x) => String(x)).join(' ') : String(body.asins ?? '');
  const { asins, invalid } = parseAsinList(raw, { max: MAX_ASINS + 1 });
  if (asins.length === 0) {
    return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: `asins が要ります (10 桁の ASIN)${invalid.length ? `。形式が違う: ${invalid.slice(0, 5).join(', ')}` : ''}`, requestId: req.requestId });
  }
  if (asins.length > MAX_ASINS) {
    return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: `asins は 1 回 ${MAX_ASINS} 件までです`, requestId: req.requestId });
  }
  const weekStart = body.week_start == null || body.week_start === '' ? null : String(body.week_start);
  if (weekStart && !/^\d{4}-\d{2}-\d{2}$/.test(weekStart)) {
    return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'week_start は YYYY-MM-DD (日曜)', requestId: req.requestId });
  }
  let db;
  try {
    db = initAbaDB();
  } catch (e) {
    return errorResponse(res, { status: 503, error: 'ABA_DB_UNAVAILABLE', message: `aba.db を開けません: ${e.message}`, requestId: req.requestId });
  }
  try {
    const result = lookupAsins(db, asins, { weekStart, register: body.register === true });
    okResponse(res, { result: { ...result, invalid } });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'ABA_LOOKUP_ERROR', message: e.message, requestId: req.requestId });
  }
});

export default router;
