/**
 * linegift-listing-diff.js — LINEギフト finance DQ の listing_diff_pct (NE 由来の売上 と LINEギフト API 由来の売上 の突き合わせ) の本体
 *
 * 🚨 なぜ切り出したか (2026-09-21):
 *   今までは「f_sales_by_listing (NE の受注。**受注日の月**)」と「f_linegift_finance_sku_daily_v1 (fact。**受取日の月**)」を月で切って比べていた。
 *   LINEギフトは 受注 → 受取 に 0〜8 日かかる (8 割は 0〜1 日) ので、月末の 1〜2 日ぶんの受注が翌月の受取に流れ、月の売上の 4〜5% が **構造的に** ずれる。
 *   本番の実測 (9/21。差 = |fact − listing| ÷ listing):
 *       受取日の月で比べる (今まで): 5 月 4.09% / 6 月 4.59% / 7 月 0.52% / 8 月 **5.06%** (隣の月と合わせると 5+6 月 0.02%・8+9 月 0.36% = 打ち消し合っている)
 *       受注日の月で比べる (これ):   5 月 0.09% / 6 月 0.03% / 7 月 0.12% / 8 月 0.21%
 *   8 月は error のしきい値 5% をわずかに超え、9/17 から毎朝 ❌ = daily-sync が 8 月の LINEギフト finance の Render への同期を見送り続けていた (データの欠けではなく、比べ方の問題)。
 *
 * 直し方 = **同じ「受注日の月」で比べる**。fact は (受取日 × SKU) に集約済みで注文ごとの受注日を持たない (bought_date_jst は MIN) ので、
 *   元の raw_linegift_orders を **fact と同じ条件** (build SQL の Step 1 = received の whitelist) で、受注日の月に数える。
 *   (raw を受取日の月で数えた値は fact の月の合計と 1 円も違わない = fact は raw の純粋な集計。本番で確認)
 *
 * 🚨 比べる相手を raw に変えると、今までの検査が結果として見ていた「fact が raw からちゃんと作られているか」を誰も見なくなる (Codex #1398 R1:
 *   raw と listing が 100 万円・fact だけ 92 万円でも全検査が info になり、同期ゲートを通った)。→ **fact ↔ raw (受取日の月) の一致** を別の検査にする (linegiftFactRawMismatch)。
 *   こちらは受取日どうし = 構造的なずれが無い = 1 つでも違えば error (本番は 2026-03〜09 の全部の鍵が 1 円も違わず一致。9/21 確認)。listing が無い月・重複期間でも省かない。
 *
 * 当月・前月の月初 (recent_past) は、受取がまだの受注が fact に居ない = 構造的に fact が小さい → 緩いしきい値 (当月用) を使う。過去月は厳しいまま (warn 1% / error 5%)。
 */

/** fact に入る行の条件 = sql/linegift/build_f_linegift_finance_sku_daily_v1.sql の Step 1 (silver) と同じ (月の条件を除く)。試験がソースと突き合わせる */
export const FACT_WHITELIST_SQL = `status = 'received' AND received_date_jst IS NOT NULL AND stock_count IS NOT NULL AND stock_count > 0 AND selling_price IS NOT NULL AND selling_price > 0 AND sku_code IS NOT NULL AND TRIM(sku_code) <> ''`;

/**
 * fact ↔ raw の一致 (受取日の月)。raw を build SQL と同じ式で (受取日 × 正規化 SKU) に集約し、fact の行と鍵ごとに比べる。
 *   式 = build SQL の Step 1〜2: 月は strftime('%Y%m', received_date_jst)・SKU は LOWER(TRIM())・数量は INTEGER に・金額は REAL に・ROUND(SUM(価格 × 数量), 2)
 * @returns {{ keys, missingInFact, extraInFact, amountDiffers, mismatched, rawJpy, factJpy, examples }}
 *   mismatched = missingInFact + extraInFact + amountDiffers (0 が正常)。examples = 食い違った鍵を 3 つまで
 */
export function linegiftFactRawMismatch(db, monthStr) {
  if (!/^\d{4}-\d{2}$/.test(String(monthStr))) throw new Error(`month は 'YYYY-MM': ${monthStr}`);
  const ymInt = Number(monthStr.replace('-', ''));
  const rows = db.prepare(`
    WITH r AS (SELECT received_date_jst AS d, LOWER(TRIM(sku_code)) AS k, ROUND(SUM(CAST(selling_price AS REAL) * CAST(stock_count AS INTEGER)), 2) AS p
                 FROM raw_linegift_orders WHERE ${FACT_WHITELIST_SQL} AND CAST(strftime('%Y%m', received_date_jst) AS INTEGER) = ? GROUP BY 1, 2),
         f AS (SELECT date_jst AS d, sku_code AS k, gross_sales_jpy_incl AS p FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?),
         u AS (SELECT d, k FROM r UNION SELECT d, k FROM f)
    SELECT u.d, u.k, r.p AS raw_p, f.p AS fact_p,
           CASE WHEN f.d IS NULL THEN 'missing_in_fact' WHEN r.d IS NULL THEN 'extra_in_fact' WHEN ABS(r.p - f.p) > 0.005 THEN 'amount_differs' ELSE 'same' END AS kind
      FROM u LEFT JOIN r ON r.d = u.d AND r.k = u.k LEFT JOIN f ON f.d = u.d AND f.k = u.k ORDER BY u.d, u.k`).all(ymInt, monthStr);
  const out = { keys: rows.length, missingInFact: 0, extraInFact: 0, amountDiffers: 0, mismatched: 0, rawJpy: 0, factJpy: 0, examples: [] };
  for (const x of rows) {
    out.rawJpy += Number(x.raw_p || 0); out.factJpy += Number(x.fact_p || 0);
    if (x.kind === 'same') continue;
    out.mismatched++;
    if (x.kind === 'missing_in_fact') out.missingInFact++; else if (x.kind === 'extra_in_fact') out.extraInFact++; else out.amountDiffers++;
    if (out.examples.length < 3) out.examples.push({ date: x.d, sku: String(x.k).slice(0, 40), kind: x.kind, raw_jpy: x.raw_p, fact_jpy: x.fact_p });
  }
  return out;
}

/**
 * @param db better-sqlite3 の接続 (warehouse.db)
 * @param {string} monthStr 'YYYY-MM'
 * @returns {{ listingAvail, listingJpy, boughtBasisJpy, receivedBasisFactJpy, notReceivedJpy, diffPct, receivedBasisDiffPct }}
 *   diffPct = 受注日の月どうしの差 (判定に使う)。receivedBasisDiffPct = 今までの比べ方 (参考。details に残す)
 *   notReceivedJpy = その月に受注して、状態が received でも cancel でもない注文の額 (入金待ちなども含む参考値。判定には足し戻さない)
 *   receivedWithoutBoughtDate = fact に入る行なのに受注日が空の件数 (どの受注月にも数えられない = 差の原因を見分けるための参考。本番は 0 件)
 */
export function linegiftListingDiff(db, monthStr) {
  if (!/^\d{4}-\d{2}$/.test(String(monthStr))) throw new Error(`month は 'YYYY-MM': ${monthStr}`);
  const num = (sql, ...p) => Number(db.prepare(sql).get(...p)?.p || 0);
  const listingJpy = num(`SELECT SUM(売上金額) AS p FROM f_sales_by_listing WHERE モール = 'linegift' AND substr(日付, 1, 7) = ?`, monthStr);
  const boughtBasisJpy = num(`SELECT SUM(selling_price * stock_count) AS p FROM raw_linegift_orders WHERE ${FACT_WHITELIST_SQL} AND substr(bought_date_jst, 1, 7) = ?`, monthStr);
  const receivedBasisFactJpy = num(`SELECT SUM(gross_sales_jpy_incl) AS p FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?`, monthStr);
  const notReceivedJpy = num(`SELECT SUM(selling_price * stock_count) AS p FROM raw_linegift_orders WHERE status NOT IN ('received', 'cancel') AND substr(bought_date_jst, 1, 7) = ?`, monthStr);
  const receivedWithoutBoughtDate = num(`SELECT COUNT(*) AS p FROM raw_linegift_orders WHERE ${FACT_WHITELIST_SQL} AND (bought_date_jst IS NULL OR TRIM(bought_date_jst) = '')`);
  const listingAvail = listingJpy > 0;
  const pct = (x) => (listingAvail ? Math.abs(x - listingJpy) / Math.abs(listingJpy) * 100 : null);
  return { listingAvail, listingJpy, boughtBasisJpy, receivedBasisFactJpy, notReceivedJpy, receivedWithoutBoughtDate, diffPct: pct(boughtBasisJpy), receivedBasisDiffPct: pct(receivedBasisFactJpy) };
}

/**
 * しきい値の選び方: 過去月 = past。当月と前月の月初 (recent_past) = current (受取がまだの受注が fact に居ないぶん、構造的に fact が小さい)
 * @param {'current'|'recent_past'|'past'} mode  finance-dq-month-mode.js の monthMode()
 */
export function listingDiffThreshold(mode, past, current) {
  return mode === 'past' ? past : current;
}

/** @returns {'info'|'warn'|'error'}  重複期間 (旧 CSV と並走していた月) は今までどおり info */
export function listingDiffSeverity(diffPct, threshold, { isDuplicatePeriod = false } = {}) {
  if (isDuplicatePeriod || diffPct === null) return 'info';
  if (diffPct > threshold.error) return 'error';
  if (diffPct > threshold.warn) return 'warn';
  return 'info';
}
