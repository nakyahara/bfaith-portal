/**
 * amazon-finance-v4-reconcile.js — 日次の財務 (f_amazon_finance_sku_daily_v1) と v4 (v_amazon_sku_profit_actual_v4) の突き合わせ (2026-09-29)
 *
 * 2 つは計算の決まりが違う。決まりの違いを 1 つずつ引いてから比べ、残り (説明できない差) だけを見る。
 * 2026-09-29 の本番 (1〜9 月): 残りはどの月も 0 円 (#1522 の符号つきの正味・#1525 のポイントの後)。
 *
 *   決まりの違い (日次の財務 − v4 の向き):
 *   ① 原価        日次 = 作った日の原価 × 返品を引いた数 / v4 = 今の原価 × 注文数 → 原価を引く前で比べる
 *   ② ポイント    日次だけが引く (points_jpy・#1525) → 日次に足し戻す
 *   ③ 送料の税    v4 は送料・ギフト包装の売上に ShippingTax / GiftWrapTax を含める → v4 から引く
 *   ④ 返品の管理手数料  v4 の手数料は Commission だけ (RefundCommission を含めない) → v4 に足す (負)
 *   ⑤ 返金の範囲  v4 は Refund の本体だけ。日次は Refund_Retrocharge / Order_Retrocharge / Chargeback Refund / A-to-z の本体
 *                  と 送料・ギフト包装の返金・返品の手数料 (RestockingFee) も → v4 に足す
 *   ③〜⑤ は月の集計の縦長の表 (fact_amazon_settlement_monthly_long・出現順つきの重複除去の後) から数える
 *
 * 使うところ: run-amazon-finance-dq.js (機械の関所) / validate-v4-reference.js (人が読む報告)
 */

const LONG_ADJ = `
  SUM(CASE WHEN component_family = 'price' AND transaction_type = 'Order' AND component_type IN ('ShippingTax', 'GiftWrapTax') THEN value_micro ELSE 0 END) / 1e6 AS ship_tax,
  SUM(CASE WHEN component_family = 'fee' AND component_type = 'RefundCommission' THEN value_micro ELSE 0 END) / 1e6 AS refund_commission,
  SUM(CASE WHEN component_family = 'price' AND (
        (transaction_type IN ('Refund', 'Refund_Retrocharge', 'Order_Retrocharge', 'Chargeback Refund', 'A-to-z Guarantee Refund') AND component_type IN ('Shipping', 'GiftWrap', 'RestockingFee'))
     OR (transaction_type IN ('Refund_Retrocharge', 'Order_Retrocharge', 'Chargeback Refund', 'A-to-z Guarantee Refund') AND component_type = 'Principal'))
      THEN value_micro ELSE 0 END) / 1e6 AS other_refund`;

const V4_PROFIT = `gross_margin_excl_tax + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + refund_principal_jpy + reversal_reimbursement_jpy`;

/**
 * 月ごと (month = 'YYYY-MM' を渡せばその月だけ)。
 * 戻り値の行 = { month, profit_d, profit_v4 (そのまま), raw_diff, cogs_d, cogs_v4, points, ship_tax, refund_commission, other_refund,
 *                cmp_d (日次: 原価を引く前 + ポイント), cmp_v4 (v4: 原価を引く前 − 送料の税 + 返品の管理手数料 + 返金の範囲), resid (cmp_d − cmp_v4), resid_pct }
 */
export function reconcileMonthly(db, { month = null } = {}) {
  const ym = month ? Number(month.replace('-', '')) : null;
  const rows = db.prepare(`
    WITH d AS (
      SELECT CAST(replace(substr(date_jst, 1, 7), '-', '') AS INTEGER) AS ym,
             SUM(profit_amount) AS profit, SUM(cogs_amount) AS cogs, SUM(points_jpy) AS points
        FROM f_amazon_finance_sku_daily_v1
       WHERE (? IS NULL OR substr(date_jst, 1, 7) = ?)
       GROUP BY 1),
    v AS (
      SELECT year_month_int AS ym, SUM(${V4_PROFIT}) AS profit, SUM(cogs_excl_tax) AS cogs
        FROM v_amazon_sku_profit_actual_v4
       WHERE (? IS NULL OR year_month_int = ?)
       GROUP BY 1),
    k AS (
      SELECT year_month_int AS ym, ${LONG_ADJ}
        FROM fact_amazon_settlement_monthly_long
       WHERE (? IS NULL OR year_month_int = ?)
       GROUP BY 1)
    SELECT COALESCE(d.ym, v.ym) AS ym,
           COALESCE(d.profit, 0) AS profit_d, COALESCE(v.profit, 0) AS profit_v4,
           COALESCE(d.cogs, 0) AS cogs_d, COALESCE(v.cogs, 0) AS cogs_v4, COALESCE(d.points, 0) AS points,
           COALESCE(k.ship_tax, 0) AS ship_tax, COALESCE(k.refund_commission, 0) AS refund_commission, COALESCE(k.other_refund, 0) AS other_refund
      FROM d FULL OUTER JOIN v ON v.ym = d.ym LEFT JOIN k ON k.ym = COALESCE(d.ym, v.ym)
     ORDER BY 1`).all(month, month, ym, ym, ym, ym);
  return rows.map((r) => {
    const cmpD = r.profit_d + r.cogs_d + r.points;
    const cmpV4 = r.profit_v4 + r.cogs_v4 - r.ship_tax + r.refund_commission + r.other_refund;
    const resid = cmpD - cmpV4;
    return {
      month: `${String(r.ym).slice(0, 4)}-${String(r.ym).slice(4)}`,
      ...r, raw_diff: r.profit_d - r.profit_v4,
      cmp_d: cmpD, cmp_v4: cmpV4, resid,
      resid_pct: cmpV4 !== 0 ? Math.abs(resid) / Math.abs(cmpV4) * 100 : (resid === 0 ? 0 : 100),
    };
  });
}

/** SKU × 月で、決まりの違いを引いた後の残りの大きい順 (|残り| > minYen) */
export function reconcileSkuTop(db, { month = null, minYen = 1, limit = 20 } = {}) {
  const ym = month ? Number(month.replace('-', '')) : null;
  const rows = db.prepare(`
    WITH d AS (
      SELECT CAST(replace(substr(date_jst, 1, 7), '-', '') AS INTEGER) AS ym, seller_sku AS sku,
             SUM(profit_amount + cogs_amount + points_jpy) AS cmp
        FROM f_amazon_finance_sku_daily_v1
       WHERE (? IS NULL OR substr(date_jst, 1, 7) = ?)
       GROUP BY 1, 2),
    v AS (
      SELECT year_month_int AS ym, seller_sku AS sku, SUM(${V4_PROFIT} + cogs_excl_tax) AS p
        FROM v_amazon_sku_profit_actual_v4
       WHERE (? IS NULL OR year_month_int = ?)
       GROUP BY 1, 2),
    k AS (
      SELECT year_month_int AS ym, seller_sku_normalized AS sku, ${LONG_ADJ}
        FROM fact_amazon_settlement_monthly_long
       WHERE (? IS NULL OR year_month_int = ?)
       GROUP BY 1, 2),
    j AS (
      SELECT COALESCE(d.ym, v.ym) AS ym, COALESCE(d.sku, v.sku) AS sku, COALESCE(d.cmp, 0) AS cmp_d,
             COALESCE(v.p, 0) - COALESCE(k.ship_tax, 0) + COALESCE(k.refund_commission, 0) + COALESCE(k.other_refund, 0) AS cmp_v4
        FROM d FULL OUTER JOIN v ON v.ym = d.ym AND v.sku = d.sku
        LEFT JOIN k ON k.ym = COALESCE(d.ym, v.ym) AND k.sku = COALESCE(d.sku, v.sku))
    SELECT ym, sku, cmp_d, cmp_v4, cmp_d - cmp_v4 AS resid FROM j
     WHERE ABS(cmp_d - cmp_v4) > ?
     ORDER BY ABS(cmp_d - cmp_v4) DESC LIMIT ?`).all(month, month, ym, ym, ym, ym, minYen, limit);
  return rows.map((r) => ({ month: `${String(r.ym).slice(0, 4)}-${String(r.ym).slice(4)}`, ...r }));
}
