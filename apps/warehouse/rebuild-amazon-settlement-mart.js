/**
 * rebuild-amazon-settlement-mart.js — Phase 3.5 mart builder (永続化)
 *
 * 目的:
 *   - silver view (v_amazon_settlement_unified) → fact_amazon_settlement_monthly_long
 *   - long → fact_amazon_settlement_monthly_wide (SKU x month に pivot)
 *   - Phase 3.5: Refund qty 推定 (Settlement の Refund 行は qty=NULL → SKU 単価で割って推定)
 *
 * 使い方:
 *   node apps/warehouse/rebuild-amazon-settlement-mart.js               # settlement_refresh_queue の dirty months
 *   node apps/warehouse/rebuild-amazon-settlement-mart.js --ym 202604   # 単月
 *   node apps/warehouse/rebuild-amazon-settlement-mart.js --all         # raw に存在する全月
 *   node apps/warehouse/rebuild-amazon-settlement-mart.js --dry-run     # 対象月だけ表示して書き込まない
 *
 * Refund qty 推定アルゴリズム (Phase 3.5):
 *   sku_unit_price_micro = SUM(Order.Principal price) / SUM(Order qty)  [SKU 全期間]
 *   qty_customer_refunded     = ROUND(ABS(refund_principal_micro) / sku_unit_price_micro)
 *   qty_a_to_z_refund         = ROUND(ABS(atoz_principal_micro) / sku_unit_price_micro)
 *   qty_marketplace_guarantee = ROUND(ABS(mp_principal_micro) / sku_unit_price_micro)
 *   qty_net_sold = qty_ordered - 上記 3 つの推定値合計
 *   注: SKU 全期間平均単価のため、価格改定の影響で誤差が出るが、現データでカバレッジ 100%
 */

import { initDB, getDB } from './db.js';

const args = process.argv.slice(2);
const isDryRun = args.includes('--dry-run');
const isAll = args.includes('--all');
const ymArgIdx = args.indexOf('--ym');
const explicitYm = ymArgIdx >= 0 ? Number(args[ymArgIdx + 1]) : null;

function pickTargetYms(db) {
  if (explicitYm) return [explicitYm];
  if (isAll) {
    return db.prepare(`
      SELECT DISTINCT year_month_int FROM raw_amazon_settlement_lines
      WHERE seller_sku_normalized IS NOT NULL
      ORDER BY year_month_int
    `).all().map(r => r.year_month_int);
  }
  return db.prepare(`
    SELECT year_month_int FROM settlement_refresh_queue
    WHERE processed_at IS NULL
    ORDER BY year_month_int
  `).all().map(r => r.year_month_int);
}

function rebuildLong(db, ym, generatedAt) {
  db.prepare(`DELETE FROM fact_amazon_settlement_monthly_long WHERE year_month_int = ?`).run(ym);

  // 月単位 silver dedup を temp table に閉じ込めて 5 INSERT で再利用
  // (v_amazon_settlement_unified を直接使うと WINDOW 関数が全 raw を partition するため遅い)
  db.exec(`DROP TABLE IF EXISTS _silver_month`);
  db.prepare(`
    CREATE TEMP TABLE _silver_month AS
    WITH dedup AS (
      SELECT l.*,
             ROW_NUMBER() OVER (
               PARTITION BY l.source_settlement_id, l.business_line_key
               ORDER BY CASE l.source_layer
                          WHEN 'sp_api_v1' THEN 1
                          WHEN 'manual_csv' THEN 2
                          ELSE 3
                        END,
                        l.ingested_at DESC
             ) AS rn
      FROM raw_amazon_settlement_lines l
      WHERE l.year_month_int = ? AND l.seller_sku_normalized IS NOT NULL
    )
    SELECT * FROM dedup WHERE rn = 1
  `).run(ym);
  db.exec(`CREATE INDEX _silver_month_idx ON _silver_month(seller_sku_normalized, transaction_type)`);

  // qty (transaction_type を component_type に)
  db.prepare(`
    INSERT INTO fact_amazon_settlement_monthly_long
      (year_month_int, seller_sku_normalized, transaction_type, component_family, component_type, value_micro, row_count, source_layer, generated_at)
    SELECT year_month_int, seller_sku_normalized, transaction_type,
           'qty', transaction_type,
           SUM(quantity_purchased), COUNT(*), source_layer, ?
    FROM _silver_month
    WHERE quantity_purchased IS NOT NULL
    GROUP BY year_month_int, seller_sku_normalized, transaction_type, source_layer
  `).run(generatedAt);

  // price
  db.prepare(`
    INSERT INTO fact_amazon_settlement_monthly_long
      (year_month_int, seller_sku_normalized, transaction_type, component_family, component_type, value_micro, row_count, source_layer, generated_at)
    SELECT year_month_int, seller_sku_normalized, transaction_type,
           'price', price_type,
           SUM(price_amount_micro), COUNT(*), source_layer, ?
    FROM _silver_month
    WHERE price_amount_micro IS NOT NULL AND price_type IS NOT NULL
    GROUP BY year_month_int, seller_sku_normalized, transaction_type, price_type, source_layer
  `).run(generatedAt);

  // fee
  db.prepare(`
    INSERT INTO fact_amazon_settlement_monthly_long
      (year_month_int, seller_sku_normalized, transaction_type, component_family, component_type, value_micro, row_count, source_layer, generated_at)
    SELECT year_month_int, seller_sku_normalized, transaction_type,
           'fee', item_related_fee_type,
           SUM(item_related_fee_amount_micro), COUNT(*), source_layer, ?
    FROM _silver_month
    WHERE item_related_fee_amount_micro IS NOT NULL AND item_related_fee_type IS NOT NULL
    GROUP BY year_month_int, seller_sku_normalized, transaction_type, item_related_fee_type, source_layer
  `).run(generatedAt);

  // promotion
  db.prepare(`
    INSERT INTO fact_amazon_settlement_monthly_long
      (year_month_int, seller_sku_normalized, transaction_type, component_family, component_type, value_micro, row_count, source_layer, generated_at)
    SELECT year_month_int, seller_sku_normalized, transaction_type,
           'promotion', promotion_type,
           SUM(promotion_amount_micro), COUNT(*), source_layer, ?
    FROM _silver_month
    WHERE promotion_amount_micro IS NOT NULL AND promotion_type IS NOT NULL
    GROUP BY year_month_int, seller_sku_normalized, transaction_type, promotion_type, source_layer
  `).run(generatedAt);

  // other (price/fee/promotion/qty 全て NULL の行 = WAREHOUSE_DAMAGE 等の direct_payment 系)
  db.prepare(`
    INSERT INTO fact_amazon_settlement_monthly_long
      (year_month_int, seller_sku_normalized, transaction_type, component_family, component_type, value_micro, row_count, source_layer, generated_at)
    SELECT year_month_int, seller_sku_normalized, transaction_type,
           'other', '__NONE__',
           SUM(COALESCE(direct_payment_amount_micro,0) + COALESCE(other_amount_micro,0)
               + COALESCE(misc_fee_amount_micro,0) + COALESCE(other_fee_amount_micro,0)),
           COUNT(*), source_layer, ?
    FROM _silver_month
    WHERE price_amount_micro IS NULL AND item_related_fee_amount_micro IS NULL
      AND promotion_amount_micro IS NULL AND quantity_purchased IS NULL
    GROUP BY year_month_int, seller_sku_normalized, transaction_type, source_layer
  `).run(generatedAt);

  return db.prepare(`SELECT COUNT(*) AS n FROM fact_amazon_settlement_monthly_long WHERE year_month_int = ?`).get(ym).n;
}

function ensureSkuUnitPriceTemp(db) {
  db.exec(`DROP TABLE IF EXISTS _sku_unit_price`);
  db.exec(`
    CREATE TEMP TABLE _sku_unit_price AS
    WITH src AS (
      SELECT seller_sku_normalized,
             SUM(CASE WHEN price_type='Principal' THEN price_amount_micro ELSE 0 END) AS principal_micro,
             -- qty は qty 専用行のみ集計 (price_type/fee_type が NULL = 数量情報行)
             -- これを限定しないと Order の Principal/Tax/Fee/Promotion 行と二重カウントの恐れ (Codex round 1 指摘)
             SUM(CASE WHEN quantity_purchased > 0
                          AND price_type IS NULL
                          AND item_related_fee_type IS NULL
                          AND promotion_type IS NULL
                       THEN quantity_purchased ELSE 0 END) AS qty
      FROM raw_amazon_settlement_lines
      WHERE transaction_type='Order' AND seller_sku_normalized IS NOT NULL
      GROUP BY seller_sku_normalized
    )
    SELECT seller_sku_normalized,
           CAST(principal_micro * 1.0 / NULLIF(qty, 0) AS INTEGER) AS unit_price_micro
    FROM src
    WHERE qty > 0 AND principal_micro > 0
  `);
  db.exec(`CREATE UNIQUE INDEX _sku_unit_idx ON _sku_unit_price(seller_sku_normalized)`);
}

function rebuildWide(db, ym, generatedAt) {
  db.prepare(`DELETE FROM fact_amazon_settlement_monthly_wide WHERE year_month_int = ?`).run(ym);

  db.prepare(`
    INSERT INTO fact_amazon_settlement_monthly_wide (
      year_month_int, seller_sku_normalized,
      qty_ordered, qty_customer_refunded, qty_a_to_z_refund, qty_marketplace_guarantee,
      qty_warehouse_damage, qty_warehouse_lost, qty_reversal_reimbursement,
      qty_removal_completed, qty_safe_t, qty_net_sold,
      sales_principal_micro, sales_tax_micro, sales_shipping_micro, sales_giftwrap_micro,
      refund_principal_micro, refund_tax_micro, refund_shipping_micro,
      commission_micro, fba_fulfillment_micro, fba_storage_micro, closing_fee_micro,
      shipping_chargeback_micro, giftwrap_chargeback_micro,
      promotion_micro,
      warehouse_damage_micro, warehouse_lost_micro, reversal_reimbursement_micro, safe_t_micro,
      fee_adjustment_micro,
      source_layer_summary, raw_row_count, generated_at
    )
    WITH agg AS (
      SELECT
        l.year_month_int,
        l.seller_sku_normalized,
        SUM(CASE WHEN l.component_family='qty' AND l.transaction_type='Order' THEN l.value_micro ELSE 0 END) AS qty_ordered,
        SUM(CASE WHEN l.component_family='qty' AND l.transaction_type IN ('WAREHOUSE_DAMAGE','WAREHOUSE_DAMAGE_EXCEPTION') THEN l.value_micro ELSE 0 END) AS qty_warehouse_damage,
        SUM(CASE WHEN l.component_family='qty' AND l.transaction_type='WAREHOUSE_LOST' THEN l.value_micro ELSE 0 END) AS qty_warehouse_lost,
        SUM(CASE WHEN l.component_family='qty' AND l.transaction_type='REVERSAL_REIMBURSEMENT' THEN l.value_micro ELSE 0 END) AS qty_reversal_reimbursement,
        SUM(CASE WHEN l.component_family='qty' AND l.transaction_type='RemovalComplete' THEN l.value_micro ELSE 0 END) AS qty_removal_completed,
        SUM(CASE WHEN l.component_family='qty' AND l.transaction_type IN ('SAFE-T Reimbursement','SAFE-T補てん') THEN l.value_micro ELSE 0 END) AS qty_safe_t,

        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='Order' AND l.component_type='Principal' THEN l.value_micro ELSE 0 END) AS sales_principal_micro,
        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='Order' AND l.component_type='Tax' THEN l.value_micro ELSE 0 END) AS sales_tax_micro,
        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='Order' AND l.component_type IN ('Shipping','ShippingTax') THEN l.value_micro ELSE 0 END) AS sales_shipping_micro,
        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='Order' AND l.component_type IN ('GiftWrap','GiftWrapTax') THEN l.value_micro ELSE 0 END) AS sales_giftwrap_micro,

        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='Refund' AND l.component_type='Principal' THEN l.value_micro ELSE 0 END) AS refund_principal_micro,
        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='Refund' AND l.component_type='Tax' THEN l.value_micro ELSE 0 END) AS refund_tax_micro,
        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='Refund' AND l.component_type IN ('Shipping','ShippingTax') THEN l.value_micro ELSE 0 END) AS refund_shipping_micro,

        SUM(CASE WHEN l.component_family='price' AND l.transaction_type='A-to-z Guarantee Refund' AND l.component_type='Principal' THEN l.value_micro ELSE 0 END) AS atoz_principal_micro,
        SUM(CASE WHEN l.component_family='price' AND l.transaction_type IN ('マーケットプレイス保証申請','Goodwill Concession') AND l.component_type='Principal' THEN l.value_micro ELSE 0 END) AS mp_principal_micro,

        SUM(CASE WHEN l.component_family='fee' AND l.component_type='Commission' THEN l.value_micro ELSE 0 END) AS commission_micro,
        SUM(CASE WHEN l.component_family='fee' AND l.component_type IN ('FBAPerUnitFulfillmentFee','FBA Pick & Pack Fee') THEN l.value_micro ELSE 0 END) AS fba_fulfillment_micro,
        SUM(CASE WHEN l.component_family='other' AND l.transaction_type IN ('Storage Fee','Storage Fee - Correction','Storage Fee - Reversal','StorageRenewalBilling') THEN l.value_micro ELSE 0 END) AS fba_storage_micro,
        SUM(CASE WHEN l.component_family='fee' AND l.component_type IN ('FixedClosingFee','VariableClosingFee') THEN l.value_micro ELSE 0 END) AS closing_fee_micro,
        SUM(CASE WHEN l.component_family='fee' AND l.component_type='ShippingChargeback' THEN l.value_micro ELSE 0 END) AS shipping_chargeback_micro,
        SUM(CASE WHEN l.component_family='fee' AND l.component_type='GiftwrapChargeback' THEN l.value_micro ELSE 0 END) AS giftwrap_chargeback_micro,

        SUM(CASE WHEN l.component_family='promotion' THEN l.value_micro ELSE 0 END) AS promotion_micro,

        SUM(CASE WHEN l.component_family='other' AND l.transaction_type IN ('WAREHOUSE_DAMAGE','WAREHOUSE_DAMAGE_EXCEPTION') THEN l.value_micro ELSE 0 END) AS warehouse_damage_micro,
        SUM(CASE WHEN l.component_family='other' AND l.transaction_type='WAREHOUSE_LOST' THEN l.value_micro ELSE 0 END) AS warehouse_lost_micro,
        SUM(CASE WHEN l.component_family='other' AND l.transaction_type='REVERSAL_REIMBURSEMENT' THEN l.value_micro ELSE 0 END) AS reversal_reimbursement_micro,
        SUM(CASE WHEN l.component_family='other' AND l.transaction_type IN ('SAFE-T Reimbursement','SAFE-T補てん') THEN l.value_micro ELSE 0 END) AS safe_t_micro,
        SUM(CASE WHEN l.component_family='other' AND l.transaction_type IN ('Fee Adjustment','Order_Retrocharge','Overpaid Fees Adjustment') THEN l.value_micro ELSE 0 END) AS fee_adjustment_micro,

        GROUP_CONCAT(DISTINCT l.source_layer) AS source_layer_summary,
        SUM(l.row_count) AS raw_row_count
      FROM fact_amazon_settlement_monthly_long l
      WHERE l.year_month_int = ?
      GROUP BY l.year_month_int, l.seller_sku_normalized
    ),
    est AS (
      SELECT a.*,
             COALESCE(CAST(ROUND(ABS(a.refund_principal_micro) * 1.0 / NULLIF(u.unit_price_micro, 0)) AS INTEGER), 0) AS qty_customer_refunded,
             COALESCE(CAST(ROUND(ABS(a.atoz_principal_micro)   * 1.0 / NULLIF(u.unit_price_micro, 0)) AS INTEGER), 0) AS qty_a_to_z_refund,
             COALESCE(CAST(ROUND(ABS(a.mp_principal_micro)     * 1.0 / NULLIF(u.unit_price_micro, 0)) AS INTEGER), 0) AS qty_marketplace_guarantee
      FROM agg a
      LEFT JOIN _sku_unit_price u ON u.seller_sku_normalized = a.seller_sku_normalized
    )
    SELECT
      e.year_month_int, e.seller_sku_normalized,
      e.qty_ordered,
      e.qty_customer_refunded, e.qty_a_to_z_refund, e.qty_marketplace_guarantee,
      e.qty_warehouse_damage, e.qty_warehouse_lost, e.qty_reversal_reimbursement,
      e.qty_removal_completed, e.qty_safe_t,
      e.qty_ordered - e.qty_customer_refunded - e.qty_a_to_z_refund - e.qty_marketplace_guarantee AS qty_net_sold,
      e.sales_principal_micro, e.sales_tax_micro, e.sales_shipping_micro, e.sales_giftwrap_micro,
      e.refund_principal_micro, e.refund_tax_micro, e.refund_shipping_micro,
      e.commission_micro, e.fba_fulfillment_micro, e.fba_storage_micro, e.closing_fee_micro,
      e.shipping_chargeback_micro, e.giftwrap_chargeback_micro,
      e.promotion_micro,
      e.warehouse_damage_micro, e.warehouse_lost_micro, e.reversal_reimbursement_micro, e.safe_t_micro,
      e.fee_adjustment_micro,
      e.source_layer_summary, e.raw_row_count, ?
    FROM est e
  `).run(ym, generatedAt);

  return db.prepare(`SELECT COUNT(*) AS n FROM fact_amazon_settlement_monthly_wide WHERE year_month_int = ?`).get(ym).n;
}

function markProcessed(db, ym, processedAt) {
  db.prepare(`UPDATE settlement_refresh_queue SET processed_at = ? WHERE year_month_int = ?`).run(processedAt, ym);
}

async function main() {
  await initDB();
  const db = getDB();
  db.pragma('busy_timeout = 10000');

  const yms = pickTargetYms(db);
  if (yms.length === 0) {
    console.log('対象月なし (queue が空、--all/--ym 指定もなし)');
    return;
  }

  const generatedAt = new Date().toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  console.log(`[mart-rebuild] 対象月: ${yms.join(', ')} ${isDryRun ? '(dry-run)' : ''}`);

  // SKU 単価 temp テーブル (全期間、月またぎで使い回し)
  if (!isDryRun) {
    ensureSkuUnitPriceTemp(db);
    const skuCount = db.prepare(`SELECT COUNT(*) AS n FROM _sku_unit_price`).get().n;
    console.log(`[mart-rebuild] SKU 単価テーブル: ${skuCount} SKU`);
  }

  for (const ym of yms) {
    const t0 = Date.now();
    if (isDryRun) {
      console.log(`  ${ym}: dry-run skip`);
      continue;
    }

    const tx = db.transaction(() => {
      const longRows = rebuildLong(db, ym, generatedAt);
      const wideRows = rebuildWide(db, ym, generatedAt);
      markProcessed(db, ym, generatedAt);
      return { longRows, wideRows };
    });
    const { longRows, wideRows } = tx();

    const ms = Date.now() - t0;
    console.log(`  ${ym}: long=${longRows} wide=${wideRows} (${ms}ms)`);
  }

  console.log('[mart-rebuild] 完了');
}

main().catch(e => {
  console.error('[mart-rebuild] エラー:', e);
  process.exit(1);
});
