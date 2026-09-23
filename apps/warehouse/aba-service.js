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
 *
 * 口は 2 つ: `/lookup` = ASIN → その ASIN が上位 3 に入った検索語 / `/terms` = 検索語 → その語のクリック上位 3 の ASIN (競合 ASIN の自動取得・2026-09-23)。
 *   `/terms` の none (その週にその語が無い) も、週が complete のときだけ。watched の週は監視 ASIN が絡む語しか無いので not_covered
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

// ─── 検索語 → クリック上位 3 の ASIN (SP広告KW の競合 ASIN 自動取得・2026-09-23) ───
// 中原さん「競合 ASIN は自動取得して、こちらで不採用のものを消す運用にしたい」。PA-API は使えない (AssociateNotEligible) ので
// ABA のクリック上位 3 を競合の出どころにする。ここは「その語のその週の上位 3」を返すだけ (集計・並べ替え・採否は Render 側)
export const MAX_TERMS = 50;
export const TERM_MAX_LEN = 200;
export const TERM_STATUSES = ['found', 'none', 'not_covered', 'no_week'];

/** 呼び手の語の正規化 (制御文字 → 空白・全角空白を含む空白を 1 つに・前後を落とす)。空なら null */
export function normalizeTerm(raw) {
  const s = String(raw ?? '').replace(/[\x00-\x1f\x7f]/g, ' ').replace(/[\s　]+/g, ' ').trim();
  return s && s.length <= TERM_MAX_LEN ? s : null;
}
/**
 * ABA の search_term と突き合わせる候補。ABA は語を加工せず保存しているが (aba-report-parser.js)、Amazon 側で小文字化されていることがある
 * → そのまま / 小文字 / NFKC (全角英数 → 半角) + 小文字 の順に試し、最初に当たったものを matched_term として返す
 */
export function termVariants(term) {
  const out = [];
  for (const v of [term, term.toLowerCase(), term.normalize('NFKC').toLowerCase()]) if (!out.includes(v)) out.push(v);
  return out;
}

// 週の部門 (department) の一覧。主キーが (week_start, department, search_term, click_position) なので、部門を固定すると索引で引ける。
// 一覧は主キーの索引を「次の部門」へ飛びながら取る (1 部門 1 回の索引検索。週の全行 ≈ 130 万行は読まない)。
// 🚨 覚えておかない: 取り直しの途中で数えた一覧を覚えると、あとで増えた部門の語を「無い (none)」と誤って言う
export function departmentsOf(db, weekStart) {
  return db.prepare(`
    WITH RECURSIVE d(x) AS (
      SELECT MIN(department) FROM aba_search_terms WHERE week_start = @w
      UNION ALL
      SELECT (SELECT MIN(department) FROM aba_search_terms WHERE week_start = @w AND department > d.x) FROM d WHERE d.x IS NOT NULL
    )
    SELECT x FROM d WHERE x IS NOT NULL
  `).all({ w: weekStart }).map((r) => r.x);
}

/**
 * 語ごとに、取込済みの週のクリック上位 3 (ASIN) を引く (読むだけ・監視登録はしない)。
 * 「その週にその語が無い」を none と言えるのは、週が complete (full・捨てた行 0・prune 前) のときだけ
 * (watched の週は監視 ASIN が絡む語しか保存していない = 無いことの証明にならない)。
 * @param {string[]} terms normalizeTerm 済み・重複なし
 * @returns {{week, requested_week, week_coverage, items: Array<{term, matched_term, status, coverage, reason, search_frequency_rank, department, asins: Array}>}}
 */
export function lookupTerms(db, terms, { weekStart = null } = {}) {
  const read = db.transaction(() => {
    const week = weekStart ? ingestedWeek(db, weekStart) : latestIngestedWeek(db);
    const coverage = weekCoverage(week);
    if (!week) {
      return { week: null, week_coverage: coverage, items: terms.map((term) => ({ term, matched_term: null, status: 'no_week', coverage: 'unknown', reason: weekStart ? 'week_not_found' : 'no_ingested_week', search_frequency_rank: null, department: null, asins: [] })) };
    }
    const depts = departmentsOf(db, week.week_start);
    const sel = db.prepare(`
      SELECT department, search_term, search_frequency_rank, click_position, asin, product_title, click_share, conversion_share
      FROM aba_search_terms WHERE week_start = ? AND department = ? AND search_term = ? ORDER BY click_position ASC
    `);
    const quality = weekParseQuality(week);
    const pruned = !!week.pruned_at;
    const items = [];
    for (const term of terms) {
      let rows = [], matched = null;
      for (const v of termVariants(term)) {
        for (const d of depts) rows = rows.concat(sel.all(week.week_start, d, v));
        if (rows.length) { matched = v; break; }
      }
      if (rows.length) {
        // 部門が複数あれば部門ごとに上位 3 がある。どの部門の行かを残す (まとめて並べ替えない)
        items.push({
          term, matched_term: matched, status: 'found',
          // その語の行は取込時にまとめて保存される (watched でも語のグループごと)。捨てた行があれば欠けている可能性
          coverage: pruned ? 'partial' : (quality === 'clean' ? 'complete' : (quality === 'unknown' ? 'unknown' : 'partial')),
          reason: pruned ? 'pruned' : null,
          search_frequency_rank: Math.min(...rows.map((r) => r.search_frequency_rank)),
          department: rows[0].department,
          asins: rows.map((r) => ({ asin: r.asin, department: r.department, click_position: r.click_position, product_title: r.product_title ?? null,
            click_share: r.click_share, conversion_share: r.conversion_share, search_frequency_rank: r.search_frequency_rank })),
        });
        continue;
      }
      if (coverage === 'complete') { items.push({ term, matched_term: null, status: 'none', coverage: 'complete', reason: null, search_frequency_rank: null, department: null, asins: [] }); continue; }
      const reason = pruned ? 'pruned' : week.mode == null ? 'mode_unknown' : (week.mode === 'full' ? 'incomplete_ingest' : 'watched_mode');
      items.push({ term, matched_term: null, status: 'not_covered', coverage: 'unknown', reason, search_frequency_rank: null, department: null, asins: [] });
    }
    return { week, week_coverage: coverage, items };
  });
  return { ...read(), requested_week: weekStart || null };
}

/**
 * POST /service-api/aba/terms
 * body: { terms: string[] (1〜50 語), week_start?: 'YYYY-MM-DD' }
 * → { ok, result: { week, requested_week, week_coverage, items:[{term, matched_term, status, coverage, reason, search_frequency_rank, department, asins:[…]}], invalid } }
 */
router.post('/terms', (req, res) => {
  const body = req.body || {};
  if (!Array.isArray(body.terms)) {
    return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'terms (語の配列) が要ります', requestId: req.requestId });
  }
  const terms = [], invalid = [];
  for (const raw of body.terms) {
    const t = normalizeTerm(raw);
    if (!t) { invalid.push(String(raw ?? '').slice(0, 50)); continue; }
    if (!terms.includes(t)) terms.push(t);
  }
  if (terms.length === 0) {
    return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: `terms が空です (1 語 ${TERM_MAX_LEN} 文字まで)`, requestId: req.requestId });
  }
  if (terms.length > MAX_TERMS) {
    return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: `terms は 1 回 ${MAX_TERMS} 語までです`, requestId: req.requestId });
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
    okResponse(res, { result: { ...lookupTerms(db, terms, { weekStart }), invalid } });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'ABA_LOOKUP_ERROR', message: e.message, requestId: req.requestId });
  }
});

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
