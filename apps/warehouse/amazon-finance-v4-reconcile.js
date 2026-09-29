/**
 * amazon-finance-v4-reconcile.js — 日次の財務 (f_amazon_finance_sku_daily_v1) と v4 (v_amazon_sku_profit_actual_v4) の突き合わせ (2026-09-29)
 *
 * 2 つは計算の決まりが違う。決まりの違いを 1 つずつ引いてから比べ、残り (説明できない差) だけを見る。
 * 2026-09-29 の本番 (1〜9 月): 残りはどの月も・どの SKU × 月も 0 円 (#1522 の符号つきの正味・#1525 のポイントの後)。
 *
 *   決まりの違い (日次の財務 − v4 の向き):
 *   ① 原価        日次 = 作った日の原価 × 返品を引いた数 / v4 = 今の原価 × 注文数 → 原価を引く前で比べる
 *   ② ポイント    日次だけが引く (points_jpy・#1525) → 日次に足し戻す
 *   ③ 送料の税    v4 は送料・ギフト包装の売上に ShippingTax / GiftWrapTax を含める → v4 から引く
 *   ④ 返品の管理手数料  v4 の手数料は Commission だけ (RefundCommission を含めない) → v4 に足す (負)
 *   ⑤ 返金の範囲  v4 は Refund の本体だけ。日次は Refund_Retrocharge / Order_Retrocharge / Chargeback Refund / A-to-z の本体
 *                  と 送料・ギフト包装の返金・返品の手数料 (RestockingFee) も → v4 に足す
 *   ⑥ SAFE-T (Other) 日次は取引の種類 Other + price_type SAFE-T Reimbursement の補てんも safe_t に入れる (2026-09-30 D-63)。v4 は取引の種類
 *                  SAFE-T Reimbursement だけ → v4 に足す。縦長の表の 'other' は price_type を持たない = 取引の種類 Other の 'other' を全部数える
 *                  (V1 / V2 とも Other の SKU のある行は price_type SAFE-T Reimbursement = amazon-settlement-v2.js の ⑤。ほかの price_type の Other が出たら残りに出る = 見逃さない向き)
 *   ⑦ 補てんの取り消し 日次は PAYMENT_RETRACTION_ITEMS を reversal_reimbursement に入れる (2026-09-30 D-63)。v4 は入れない → v4 に足す (負)
 *   (SKU のある納品不備は日次の財務から外した (D-63) が、利益には前から入っていない = 決まりの違いにならない)
 *   ③〜⑦ は月の集計の縦長の表 (fact_amazon_settlement_monthly_long・出現順つきの重複除去の後) から数える
 *
 * 🚨 残りは利益の純額の差。項目どうし・SKU どうしの打ち消しを見逃さないように (Codex #1531 R1):
 *   - SKU × 月で比べて、月は「残りの合計」と「残りの絶対値の合計 (resid_abs)」の両方を出す (SKU A +1 万 / B −1 万 を見つける)
 *   - 売上だけの残り (rev_resid = 日次の売上 − (v4 の売上 − 送料の税)) も別に出す (売上と手数料が同じだけ多い、を見つける)
 *   - SKU の鍵は 日次・v4・縦長の表の 3 つを全部合わせる (どれか 1 つにだけある SKU も落とさない)
 *
 * 使うところ: run-amazon-finance-dq.js (機械の関所) / validate-v4-reference.js (人が読む報告)
 */

const LONG_ADJ = `
  SUM(CASE WHEN component_family = 'price' AND transaction_type = 'Order' AND component_type IN ('ShippingTax', 'GiftWrapTax') THEN value_micro ELSE 0 END) / 1e6 AS ship_tax,
  SUM(CASE WHEN component_family = 'fee' AND component_type = 'RefundCommission' THEN value_micro ELSE 0 END) / 1e6 AS refund_commission,
  SUM(CASE WHEN component_family = 'price' AND (
        (transaction_type IN ('Refund', 'Refund_Retrocharge', 'Order_Retrocharge', 'Chargeback Refund', 'A-to-z Guarantee Refund') AND component_type IN ('Shipping', 'GiftWrap', 'RestockingFee'))
     OR (transaction_type IN ('Refund_Retrocharge', 'Order_Retrocharge', 'Chargeback Refund', 'A-to-z Guarantee Refund') AND component_type = 'Principal'))
      THEN value_micro ELSE 0 END) / 1e6 AS other_refund,
  SUM(CASE WHEN component_family = 'other' AND transaction_type = 'Other' THEN value_micro ELSE 0 END) / 1e6 AS safe_t_other,
  SUM(CASE WHEN component_family = 'other' AND transaction_type = 'PAYMENT_RETRACTION_ITEMS' THEN value_micro ELSE 0 END) / 1e6 AS retraction`;

/**
 * v4 の SKU × 月のうち、日次の財務にも行がありうるもの (DQ の行数・集合差で使う)。v4 の別名 = v4
 *   SKU のある納品不備は日次の財務から外した (2026-09-30 D-63) = 縦長の表の行が納品不備だけの SKU × 月は v4 にだけある → 数えない
 *   (Easy Ship の割り振りだけの行を日次の側で数えないのと同じ向き)
 */
export const V4_SKU_HAS_DAILY_SQL = `EXISTS (SELECT 1 FROM fact_amazon_settlement_monthly_long l4
  WHERE l4.year_month_int = v4.year_month_int AND l4.seller_sku_normalized = v4.seller_sku AND TRIM(l4.seller_sku_normalized) <> ''
    AND l4.transaction_type NOT IN ('BuyerRecharge', 'Previous Reserve Amount Balance', 'Current Reserve Amount')
    AND l4.transaction_type NOT LIKE 'Inbound Defect Fee%')`;   // 日次の silver の対象の条件と全部そろえる (Codex #1551 R1 M1: 納品不備 + BuyerRecharge だけの SKU × 月は日次に行が無い)

const V4_PROFIT = `gross_margin_excl_tax + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + refund_principal_jpy + reversal_reimbursement_jpy`;

/** SKU × 月の全部の行 (3 つの集まりの鍵を合わせる)。month = 'YYYY-MM' を渡せばその月だけ */
function skuRows(db, month) {
  const ym = month ? Number(month.replace('-', '')) : null;
  return db.prepare(`
    WITH d AS (
      SELECT CAST(replace(substr(date_jst, 1, 7), '-', '') AS INTEGER) AS ym, seller_sku AS sku,
             SUM(profit_amount) AS profit, SUM(cogs_amount) AS cogs, SUM(points_jpy) AS points,
             SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS rev
        FROM f_amazon_finance_sku_daily_v1
       WHERE (? IS NULL OR substr(date_jst, 1, 7) = ?)
       GROUP BY 1, 2),
    v AS (
      SELECT year_month_int AS ym, seller_sku AS sku, SUM(${V4_PROFIT}) AS profit, SUM(cogs_excl_tax) AS cogs,
             SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS rev
        FROM v_amazon_sku_profit_actual_v4
       WHERE (? IS NULL OR year_month_int = ?)
       GROUP BY 1, 2),
    k AS (
      SELECT year_month_int AS ym, seller_sku_normalized AS sku, ${LONG_ADJ}
        FROM fact_amazon_settlement_monthly_long
       WHERE (? IS NULL OR year_month_int = ?)
       GROUP BY 1, 2),
    keys AS (SELECT ym, sku FROM d UNION SELECT ym, sku FROM v UNION SELECT ym, sku FROM k)
    SELECT keys.ym, keys.sku,
           COALESCE(d.profit, 0) AS profit_d, COALESCE(v.profit, 0) AS profit_v4,
           COALESCE(d.cogs, 0) AS cogs_d, COALESCE(v.cogs, 0) AS cogs_v4, COALESCE(d.points, 0) AS points,
           COALESCE(d.rev, 0) AS rev_d, COALESCE(v.rev, 0) AS rev_v4,
           COALESCE(k.ship_tax, 0) AS ship_tax, COALESCE(k.refund_commission, 0) AS refund_commission, COALESCE(k.other_refund, 0) AS other_refund,
           COALESCE(k.safe_t_other, 0) AS safe_t_other, COALESCE(k.retraction, 0) AS retraction,
           (d.sku IS NOT NULL) AS in_d, (v.sku IS NOT NULL) AS in_v4, (k.sku IS NOT NULL) AS in_long
      FROM keys
      LEFT JOIN d ON d.ym = keys.ym AND d.sku = keys.sku
      LEFT JOIN v ON v.ym = keys.ym AND v.sku = keys.sku
      LEFT JOIN k ON k.ym = keys.ym AND k.sku = keys.sku
     ORDER BY 1, 2`).all(month, month, ym, ym, ym, ym).map((r) => {
    const cmpD = r.profit_d + r.cogs_d + r.points;
    const cmpV4 = r.profit_v4 + r.cogs_v4 - r.ship_tax + r.refund_commission + r.other_refund + r.safe_t_other + r.retraction;
    return { ...r, month: `${String(r.ym).slice(0, 4)}-${String(r.ym).slice(4)}`, cmp_d: cmpD, cmp_v4: cmpV4, resid: cmpD - cmpV4, rev_resid: r.rev_d - (r.rev_v4 - r.ship_tax) };
  });
}

/**
 * 月ごと。戻り値の行 = { month, profit_d, profit_v4, raw_diff, cogs_d, cogs_v4, points, ship_tax, refund_commission, other_refund, safe_t_other, retraction,
 *   cmp_d, cmp_v4, resid (残りの合計), resid_abs (SKU ごとの残りの絶対値の合計), rev_resid / rev_resid_abs (売上だけ),
 *   long_only_skus (縦長の表にだけある SKU の数), resid_pct (残りの絶対値の合計 ÷ |cmp_v4|) }
 */
export function reconcileMonthly(db, { month = null } = {}) {
  const byMonth = new Map();
  for (const r of skuRows(db, month)) {
    const m = byMonth.get(r.month) || { month: r.month, profit_d: 0, profit_v4: 0, cogs_d: 0, cogs_v4: 0, points: 0, ship_tax: 0, refund_commission: 0, other_refund: 0,
      safe_t_other: 0, retraction: 0, cmp_d: 0, cmp_v4: 0, resid: 0, resid_abs: 0, rev_resid: 0, rev_resid_abs: 0, long_only_skus: 0, skus: 0 };
    for (const k of ['profit_d', 'profit_v4', 'cogs_d', 'cogs_v4', 'points', 'ship_tax', 'refund_commission', 'other_refund', 'safe_t_other', 'retraction', 'cmp_d', 'cmp_v4', 'resid', 'rev_resid']) m[k] += r[k];
    m.resid_abs += Math.abs(r.resid); m.rev_resid_abs += Math.abs(r.rev_resid); m.skus++;
    if (r.in_long && !r.in_d && !r.in_v4) m.long_only_skus++;
    byMonth.set(r.month, m);
  }
  return [...byMonth.values()].map((m) => ({
    ...m, raw_diff: m.profit_d - m.profit_v4,
    // 分子 = 利益の残りの絶対値 + 売上の残りの絶対値 (項目どうしの打ち消しも % に入れる。Codex #1531 R2)
    resid_pct: m.cmp_v4 !== 0 ? (m.resid_abs + m.rev_resid_abs) / Math.abs(m.cmp_v4) * 100 : (m.resid_abs + m.rev_resid_abs === 0 ? 0 : 100),
  }));
}

/** SKU × 月で、決まりの違いを引いた後の残り (利益 or 売上) の大きい順 (|残り| >= minYen) */
export function reconcileSkuTop(db, { month = null, minYen = 1, limit = 20 } = {}) {
  return skuRows(db, month)
    .filter((r) => Math.abs(r.resid) >= minYen || Math.abs(r.rev_resid) >= minYen)
    .sort((a, b) => Math.max(Math.abs(b.resid), Math.abs(b.rev_resid)) - Math.max(Math.abs(a.resid), Math.abs(a.rev_resid)))
    .slice(0, limit);
}
