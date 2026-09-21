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
 * 当月・前月の月初 (recent_past) は、受取がまだの受注が fact に居ない = 構造的に fact が小さい → 緩いしきい値 (当月用) を使う。過去月は厳しいまま (warn 1% / error 5%)。
 */

/** fact に入る行の条件 = sql/linegift/build_f_linegift_finance_sku_daily_v1.sql の Step 1 (silver) と同じ (月の条件を除く)。試験がソースと突き合わせる */
export const FACT_WHITELIST_SQL = `status = 'received' AND received_date_jst IS NOT NULL AND stock_count IS NOT NULL AND stock_count > 0 AND selling_price IS NOT NULL AND selling_price > 0 AND sku_code IS NOT NULL AND TRIM(sku_code) <> ''`;

/**
 * @param db better-sqlite3 の接続 (warehouse.db)
 * @param {string} monthStr 'YYYY-MM'
 * @returns {{ listingAvail, listingJpy, boughtBasisJpy, receivedBasisFactJpy, notReceivedJpy, diffPct, receivedBasisDiffPct }}
 *   diffPct = 受注日の月どうしの差 (判定に使う)。receivedBasisDiffPct = 今までの比べ方 (参考。details に残す)
 *   notReceivedJpy = その月に受注して、まだ受取になっていない・取消でない注文の額 (当月の差の説明用)
 */
export function linegiftListingDiff(db, monthStr) {
  if (!/^\d{4}-\d{2}$/.test(String(monthStr))) throw new Error(`month は 'YYYY-MM': ${monthStr}`);
  const num = (sql, ...p) => Number(db.prepare(sql).get(...p)?.p || 0);
  const listingJpy = num(`SELECT SUM(売上金額) AS p FROM f_sales_by_listing WHERE モール = 'linegift' AND substr(日付, 1, 7) = ?`, monthStr);
  const boughtBasisJpy = num(`SELECT SUM(selling_price * stock_count) AS p FROM raw_linegift_orders WHERE ${FACT_WHITELIST_SQL} AND substr(bought_date_jst, 1, 7) = ?`, monthStr);
  const receivedBasisFactJpy = num(`SELECT SUM(gross_sales_jpy_incl) AS p FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?`, monthStr);
  const notReceivedJpy = num(`SELECT SUM(selling_price * stock_count) AS p FROM raw_linegift_orders WHERE status NOT IN ('received', 'cancel') AND substr(bought_date_jst, 1, 7) = ?`, monthStr);
  const listingAvail = listingJpy > 0;
  const pct = (x) => (listingAvail ? Math.abs(x - listingJpy) / Math.abs(listingJpy) * 100 : null);
  return { listingAvail, listingJpy, boughtBasisJpy, receivedBasisFactJpy, notReceivedJpy, diffPct: pct(boughtBasisJpy), receivedBasisDiffPct: pct(receivedBasisFactJpy) };
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
