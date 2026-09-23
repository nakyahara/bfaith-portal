/**
 * ABA (Amazon Brand Analytics 検索用語レポート) の参照 サービスAPI (SP広告KW PR2-B・2026-09-23)
 * /service-api/aba にマウント
 *
 * 呼び手 = Render の product-hub (「📣 SP広告KW」タブ・競合 ASIN の注文ワード)。
 * 🚨 **走査しない・レポートを取りに行かない**。aba.db に**取込済みの最新週**を引くだけ (方針 B: 週次取込を full にして全語を保存。
 *    『Amazon_SP広告KW自動生成_設計方針_20260922.md』§5「進め方の決定」)。
 *    「調査済み」の証明 = その週の取込が完了していること (aba_weeks の行 = parsed_count / row_count)。
 *    full モードなら「行が無い」= その週のクリック上位 3 に入っていない (証明つき)。
 *    watched モードでは監視 ASIN 絡みの語しか保存していないので、監視登録済みで走査済み (last_scanned_week ≥ 週) のときだけ「無い」と言える。
 *    それ以外は「取込対象外 (次の週次取込から)」と返し、「該当なし」とは言わない (該当なし ≠ 注文なし)。
 *
 * 守ること:
 *   - ASIN は 1 回 5 件まで (形式は lib/asin.js)。照会した ASIN は監視に登録する (watched モードでも次週から拾えるように)
 *   - 指標は原値のまま返す (順位 = search_frequency_rank・click_position はクリックシェア順位 (掲載順ではない)・share は 0〜1)。
 *     ボリューム・CVR・成果には換算しない。集計 (top3 の合計) も足さない
 *   - 応答には対象週 (week_start / week_end / ingested_at / parsed_count / row_count) と mode を必ず付ける
 */
import { Router } from 'express';
import { okResponse, errorResponse } from './error-handler.js';
import { initAbaDB } from '../aba-keywords/db.js';
import { parseAsinList } from '../../lib/asin.js';

const router = Router();

export const MAX_ASINS = 5;
export const ASIN_STATUSES = ['found', 'none', 'not_covered', 'no_week'];

/** 週次取込の保存モード (fetch-aba-search-terms.js と同じ判定) */
export function ingestMode() {
  return process.env.ABA_INGEST_MODE === 'full' ? 'full' : 'watched';
}

/** 取込済みの最新週 (parsed_count > 0 = レポートを最後まで読んで台帳を書いた週)。無ければ null */
export function latestIngestedWeek(db) {
  return db.prepare(`
    SELECT week_start, week_end, ingested_at, term_count, row_count, parsed_count
    FROM aba_weeks WHERE parsed_count > 0 ORDER BY week_start DESC LIMIT 1
  `).get() || null;
}

/**
 * ASIN ごとに、取込済みの最新週の検索語を引く。
 * @param {import('better-sqlite3').Database} db aba.db
 * @param {string[]} asins 正規化済み
 * @param {{mode?:'full'|'watched', register?:boolean}} opts register = 監視に登録する (既定 true)
 * @returns {{week: object|null, mode: string, items: Array<{asin, status, proof, terms: Array}>}}
 */
export function lookupAsins(db, asins, { mode = ingestMode(), register = true } = {}) {
  const week = latestIngestedWeek(db);
  const upsertWatch = db.prepare(`
    INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count)
    VALUES (?, datetime('now'), datetime('now'), 1)
    ON CONFLICT(asin) DO UPDATE SET last_queried_at = datetime('now'), query_count = query_count + 1
  `);
  const selTerms = db.prepare(`
    SELECT department, search_term, search_frequency_rank, click_position, click_share, conversion_share
    FROM aba_search_terms WHERE asin = ? AND week_start = ?
    ORDER BY search_frequency_rank ASC, department ASC, search_term ASC
  `);
  const selWatch = db.prepare('SELECT last_scanned_week FROM aba_watch_asins WHERE asin = ?');
  const items = [];
  const tx = db.transaction(() => {
    for (const asin of asins) {
      if (register) upsertWatch.run(asin);
      if (!week) { items.push({ asin, status: 'no_week', proof: null, terms: [] }); continue; }
      const terms = selTerms.all(asin, week.week_start).map((r) => ({
        search_term: r.search_term, department: r.department,
        search_frequency_rank: r.search_frequency_rank, click_position: r.click_position,
        click_share: r.click_share, conversion_share: r.conversion_share,
      }));
      if (terms.length > 0) { items.push({ asin, status: 'found', proof: 'week_ingested', terms }); continue; }
      if (mode === 'full') { items.push({ asin, status: 'none', proof: 'week_ingested', terms: [] }); continue; }
      // watched: 監視登録済みでその週を走査していれば「無い」と言える。そうでなければ取込対象外
      const w = selWatch.get(asin);
      if (w && w.last_scanned_week && w.last_scanned_week >= week.week_start) items.push({ asin, status: 'none', proof: 'scanned', terms: [] });
      else items.push({ asin, status: 'not_covered', proof: null, terms: [] });
    }
  });
  tx();
  return { week, mode, items };
}

/**
 * POST /service-api/aba/lookup
 * body: { asins: string[] | string }  (1〜5 件)
 * → { ok, result: { week, mode, items:[{asin, status:'found'|'none'|'not_covered'|'no_week', proof, terms:[…]}] } }
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
  let db;
  try {
    db = initAbaDB();
  } catch (e) {
    return errorResponse(res, { status: 503, error: 'ABA_DB_UNAVAILABLE', message: `aba.db を開けません: ${e.message}`, requestId: req.requestId });
  }
  try {
    const result = lookupAsins(db, asins, { register: body.register !== false });
    okResponse(res, { result: { ...result, invalid } });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'ABA_LOOKUP_ERROR', message: e.message, requestId: req.requestId });
  }
});

export default router;
