/**
 * 仕入れ先向け売れ筋共有 — 集計ロジック
 *
 * 全モール（Amazon / 楽天 / Yahoo! / au PAY / LINEギフト / Qoo10）の
 * SKU 日次売上マート（mirror_*_finance_sku_daily）を NE 商品コードに名寄せし、
 * 仕入先コード単位で商品別の「直近7日 / 30日の販売個数・売上」を集計する。
 *
 * 設計上の確定事項（Codex レビュー反映）:
 *  - 販売個数は units_net_sold（返品・キャンセル差引後の純販売数）を採用。
 *  - 集計対象日は「データ最新日 - 2日（D-2）」までに締める。直近日は確定遅延で揺れるため。
 *  - SKU を NE 商品コードに解決できない行は集計から除外（fail-closed）。
 *    Amazon はセット品（1 SKU が複数 NE コードに分解される）も二重計上回避のため除外。
 *    除外分は社内向けに未解決率として別途集計する（公開ページには出さない）。
 *  - 原価・粗利は一切扱わない（公開しない方針）。
 */

// 各モールの日次売上 fact から (mall, date_jst, ne_code, units, sales) を取り出す定義。
// Amazon は seller_sku→ne_code を mirror_sku_resolved で解決する必要があるため別扱い。
const NON_AMAZON_MALLS = [
  { mall: 'rakuten',  table: 'mirror_rakuten_finance_sku_daily',  sales: 'gross_sales_jpy_incl' },
  { mall: 'yahoo',    table: 'mirror_yahoo_finance_sku_daily',    sales: 'gross_sales_jpy_incl' },
  { mall: 'aupay',    table: 'mirror_aupay_finance_sku_daily',    sales: 'gross_sales_jpy_incl' },
  { mall: 'linegift', table: 'mirror_linegift_finance_sku_daily', sales: 'gross_sales_jpy_incl' },
  { mall: 'qoo10',    table: 'mirror_qoo10_finance_sku_daily',    sales: 'customer_paid_jpy_incl' },
];

export const MALL_LABELS = {
  amazon: 'Amazon', rakuten: '楽天', yahoo: 'Yahoo!',
  aupay: 'au PAY', linegift: 'LINEギフト', qoo10: 'Qoo10',
};

// 1 SKU が 1 つの NE コードにのみ解決される（=単品 or 同一商品の n 個入り）行のみを
// Amazon の売上として採用する CTE。複数 NE コードに分解されるセット品は売上二重計上を
// 避けるため除外する。qty は構成数（n 個入りパックなら販売個数 = units × qty で実数換算）。
const AMAZON_MAP_CTE = `
  amz_map AS (
    SELECT seller_sku, MIN(ne_code) AS ne_code, MAX(quantity) AS qty
    FROM mirror_sku_resolved
    GROUP BY seller_sku
    HAVING COUNT(DISTINCT ne_code) = 1
  )
`;

// 全モール統合の日次行（date 範囲は @start〜@cutoff）。
function unifiedDailyCTE() {
  const parts = [`
    SELECT 'amazon' AS mall, a.date_jst AS date_jst, m.ne_code AS ne_code,
           CAST(a.units_net_sold AS REAL) * COALESCE(m.qty, 1) AS units, a.sales_principal_jpy AS sales
      FROM mirror_amazon_finance_sku_daily a
      JOIN amz_map m ON m.seller_sku = a.seller_sku
      WHERE a.date_jst BETWEEN @start AND @cutoff`];
  // 注: 非Amazonモールは fact 行の ne_code をそのまま採用する。これは
  // mirror_*_finance_sku_daily 側が「セット親SKUと構成品展開行を二重に持たない」
  // （= 1 fact 行 = 1 NEコードの売上）ことを前提にしている。マート生成側の不変条件。
  for (const c of NON_AMAZON_MALLS) {
    parts.push(`
    SELECT '${c.mall}' AS mall, date_jst AS date_jst, ne_code AS ne_code,
           CAST(units_net_sold AS REAL) AS units, ${c.sales} AS sales
      FROM ${c.table}
      WHERE ne_code IS NOT NULL AND trim(ne_code) <> '' AND date_jst BETWEEN @start AND @cutoff`);
  }
  return parts.join('\n    UNION ALL\n');
}

// YYYY-MM-DD 文字列に日数を加減算（JST 文字列のまま日付計算）。
function shiftDate(ymd, days) {
  const d = new Date(`${ymd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

// 各モール fact の最新 date_jst（データのある table のみ）。
function tableMaxDates(db) {
  const tables = ['mirror_amazon_finance_sku_daily', ...NON_AMAZON_MALLS.map(c => c.table)];
  const maxes = [];
  for (const t of tables) {
    const row = db.prepare(`SELECT MAX(date_jst) AS d FROM ${t}`).get();
    if (row && row.d) maxes.push(row.d);
  }
  return maxes;
}

// a, b は YYYY-MM-DD。b - a の日数。
function daysBetween(a, b) {
  return Math.round((new Date(`${b}T00:00:00Z`) - new Date(`${a}T00:00:00Z`)) / 86400000);
}

/**
 * 集計期間（窓）を決める。
 * cutoff は「稼働中の全モールが確定済みの日 - 2日」。
 *   - モールごとに同期遅延が異なるため、最も遅れている稼働モールの最新日に合わせる
 *     （= 各モール MAX の最小値）。これで未到着モールをゼロ扱いした過少集計を防ぐ。
 *   - ただし 14日以上遅れているモール（停止・季節休止）は確定日計算から除外する
 *     （死んだモールが cutoff を過去へ引きずるのを防ぐ）。
 * 7日窓 / 30日窓 / 前7日窓 / 前30日窓 を返す。
 */
export function resolveWindows(db) {
  const maxes = tableMaxDates(db);
  if (!maxes.length) return null;
  const globalMax = maxes.reduce((a, b) => (b > a ? b : a));
  const active = maxes.filter(m => daysBetween(m, globalMax) <= 14);
  const confirmedAcross = active.reduce((a, b) => (b < a ? b : a)); // 稼働モール MAX の最小
  const cutoff = shiftDate(confirmedAcross, -2);
  return {
    latest: globalMax,
    confirmedAcross,
    cutoff,
    w7start:   shiftDate(cutoff, -6),
    w30start:  shiftDate(cutoff, -29),
    prev7start: shiftDate(cutoff, -13),
    prev7end:   shiftDate(cutoff, -7),
    prev30start: shiftDate(cutoff, -59),
    prev30end:   shiftDate(cutoff, -30),
    start:      shiftDate(cutoff, -59), // unified が読む全体範囲
  };
}

/**
 * 指定仕入先の商品別売上サマリを返す。
 * @returns { windows, products: [{ne_code, product_name, units7, sales7, units30, sales30,
 *            avgPerDay30, units7prev, units30prev, lastSold, malls:{amazon:{units,sales},...}}], totals }
 */
export function getSupplierSummary(db, supplierCode) {
  const w = resolveWindows(db);
  if (!w) return { windows: null, products: [], totals: null };

  const params = {
    supplier: supplierCode,
    start: w.start, cutoff: w.cutoff,
    w7start: w.w7start, w30start: w.w30start,
    prev7start: w.prev7start, prev7end: w.prev7end,
    prev30start: w.prev30start, prev30end: w.prev30end,
  };

  const rows = db.prepare(`
    WITH ${AMAZON_MAP_CTE},
    unified AS (
      ${unifiedDailyCTE()}
    ),
    scoped AS (
      SELECT u.*
      FROM unified u
      JOIN mirror_products p ON p.商品コード = u.ne_code
      WHERE p.仕入先コード = @supplier
    )
    SELECT
      s.ne_code AS ne_code,
      COALESCE(p.商品名, '') AS product_name,
      SUM(CASE WHEN s.date_jst BETWEEN @w7start  AND @cutoff THEN s.units ELSE 0 END) AS units7,
      SUM(CASE WHEN s.date_jst BETWEEN @w7start  AND @cutoff THEN s.sales ELSE 0 END) AS sales7,
      SUM(CASE WHEN s.date_jst BETWEEN @w30start AND @cutoff THEN s.units ELSE 0 END) AS units30,
      SUM(CASE WHEN s.date_jst BETWEEN @w30start AND @cutoff THEN s.sales ELSE 0 END) AS sales30,
      SUM(CASE WHEN s.date_jst BETWEEN @prev7start  AND @prev7end  THEN s.units ELSE 0 END) AS units7prev,
      SUM(CASE WHEN s.date_jst BETWEEN @prev30start AND @prev30end THEN s.units ELSE 0 END) AS units30prev,
      MAX(CASE WHEN s.units > 0 THEN s.date_jst END) AS lastSold
    FROM scoped s
    JOIN mirror_products p ON p.商品コード = s.ne_code
    GROUP BY s.ne_code, p.商品名
    HAVING units30 <> 0 OR sales30 <> 0 OR units7 <> 0
    ORDER BY sales30 DESC, units30 DESC
  `).all(params);

  // モール別内訳（30日窓）。
  const mallRows = db.prepare(`
    WITH ${AMAZON_MAP_CTE},
    unified AS (
      ${unifiedDailyCTE()}
    )
    SELECT u.ne_code AS ne_code, u.mall AS mall,
           SUM(u.units) AS units, SUM(u.sales) AS sales
    FROM unified u
    JOIN mirror_products p ON p.商品コード = u.ne_code
    WHERE p.仕入先コード = @supplier AND u.date_jst BETWEEN @w30start AND @cutoff
    GROUP BY u.ne_code, u.mall
  `).all(params);

  const mallByNe = {};
  for (const r of mallRows) {
    (mallByNe[r.ne_code] ||= {})[r.mall] = { units: r.units, sales: r.sales };
  }

  const products = rows.map(r => ({
    ne_code: r.ne_code,
    product_name: r.product_name,
    units7: r.units7, sales7: Math.round(r.sales7),
    units30: r.units30, sales30: Math.round(r.sales30),
    avgPerDay30: r.units30 / 30,
    units7prev: r.units7prev, units30prev: r.units30prev,
    lastSold: r.lastSold || null,
    malls: mallByNe[r.ne_code] || {},
  }));

  const totals = {
    productCount: products.length,
    units7: products.reduce((a, p) => a + p.units7, 0),
    units30: products.reduce((a, p) => a + p.units30, 0),
    sales7: products.reduce((a, p) => a + p.sales7, 0),
    sales30: products.reduce((a, p) => a + p.sales30, 0),
  };

  return { windows: w, products, totals };
}

/**
 * 社内向け: SKU 名寄せの未解決率（30日窓、全社）。
 * 仕入先別の数字が「過少表示」になっていないかの健全性チェックに使う。
 */
export function getUnresolvedStats(db) {
  const w = resolveWindows(db);
  if (!w) return null;
  const p = { w30start: w.w30start, cutoff: w.cutoff };

  // Amazon: 単品解決できた売上 vs できなかった売上。
  const amz = db.prepare(`
    WITH ${AMAZON_MAP_CTE}
    SELECT
      SUM(a.sales_principal_jpy) AS total,
      SUM(CASE WHEN m.seller_sku IS NULL THEN a.sales_principal_jpy ELSE 0 END) AS unresolved
    FROM mirror_amazon_finance_sku_daily a
    LEFT JOIN amz_map m ON m.seller_sku = a.seller_sku
    WHERE a.date_jst BETWEEN @w30start AND @cutoff
  `).get(p);

  let total = amz.total || 0;
  let unresolved = amz.unresolved || 0;
  for (const c of NON_AMAZON_MALLS) {
    const r = db.prepare(`
      SELECT
        SUM(${c.sales}) AS total,
        SUM(CASE WHEN ne_code IS NULL OR trim(ne_code) = '' THEN ${c.sales} ELSE 0 END) AS unresolved
      FROM ${c.table}
      WHERE date_jst BETWEEN @w30start AND @cutoff
    `).get(p);
    total += r.total || 0;
    unresolved += r.unresolved || 0;
  }

  const rate = total > 0 ? unresolved / total : 0;
  return {
    windows: w,
    totalSales30: Math.round(total),
    unresolvedSales30: Math.round(unresolved),
    unresolvedRate: rate,
    level: rate > 0.10 ? 'block' : rate > 0.05 ? 'warn' : rate > 0.01 ? 'notice' : 'ok',
  };
}
