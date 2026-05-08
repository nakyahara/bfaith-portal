#!/usr/bin/env node
/**
 * canonical_metric_mapping_v1 の coverage 検証 + fixture test。
 *
 * Phase 1 ticket: #1-0
 * Mapping: docs/amazon-finance/canonical-metric-mapping.md (v1)
 * Reference SQL: sql/amazon/canonical_metric_mapping_v1.sql
 *
 * 終了コード:
 *   0 = coverage 基準を満たす (UNKNOWN < 0.5%, abs amount < 0.1%, fixture 100% 一致)
 *   1 = 基準逸脱
 *
 * 使い方 (miniPC SSH):
 *   node scripts/amazon-finance/check-metric-mapping.js C:/Users/bfaith/bfaith-portal/data
 */

import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

const DATA_DIR = (process.env.DATA_DIR || process.argv[2] || '').trim();
if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR environment variable or first argument is required.');
  process.exit(2);
}
const DB_PATH = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(DB_PATH)) {
  console.error(`FATAL: warehouse.db not found at ${DB_PATH}`);
  process.exit(2);
}

const db = new Database(DB_PATH, { readonly: true });

// ============================================================
// canonical_metric_mapping_v1 の bucket 分類
// ============================================================
const TX_BUCKETS = {
  order: ['Order'],
  refund: ['Refund', 'A-to-z Guarantee Refund', 'Refund_Retrocharge', 'Order_Retrocharge'],
  reimbursement: [
    'WAREHOUSE_DAMAGE', 'WAREHOUSE_LOST', 'WAREHOUSE_DAMAGE_EXCEPTION',
    'REVERSAL_REIMBURSEMENT', 'SAFE-T Reimbursement', 'Goodwill Concession',
  ],
  fee: [
    'Amazon Easy Ship Charges', 'RemovalComplete',
    'Inbound Defect Fee - Barcode cannot be scanned',
    'Subscription Fee', 'StorageRenewalBilling', 'Storage Fee', 'ServiceFee',
    'Storage Fee - Reversal', 'Storage Fee - Correction',
  ],
  adjustment: ['Fee Adjustment', 'Overpaid Fees Adjustment'],
  reserve: ['BuyerRecharge', 'Previous Reserve Amount Balance', 'Current Reserve Amount'],
};

const ALL_KNOWN = new Set();
for (const list of Object.values(TX_BUCKETS)) for (const tx of list) ALL_KNOWN.add(tx);

// ============================================================
// 1. coverage: bucket 別 row count / abs amount
// ============================================================
console.log('=== Canonical Metric Mapping v1 — Coverage Check ===');
console.log(`DB: ${DB_PATH}\n`);

const allTx = db.prepare(`
  SELECT
    transaction_type,
    COUNT(*) AS row_count,
    SUM(
      ABS(COALESCE(price_amount_micro, 0))
      + ABS(COALESCE(item_related_fee_amount_micro, 0))
      + ABS(COALESCE(promotion_amount_micro, 0))
      + ABS(COALESCE(other_amount_micro, 0))
    ) AS abs_micro_total
  FROM raw_amazon_settlement_lines
  GROUP BY transaction_type
`).all();

let totalRows = 0;
let totalAbsMicro = 0;
const bucketAgg = {};

for (const row of allTx) {
  totalRows += row.row_count;
  totalAbsMicro += row.abs_micro_total || 0;

  let bucket = '__UNKNOWN__';
  for (const [b, list] of Object.entries(TX_BUCKETS)) {
    if (list.includes(row.transaction_type)) {
      bucket = b;
      break;
    }
  }
  if (!bucketAgg[bucket]) bucketAgg[bucket] = { row_count: 0, abs_micro: 0, transaction_types: [] };
  bucketAgg[bucket].row_count += row.row_count;
  bucketAgg[bucket].abs_micro += row.abs_micro_total || 0;
  bucketAgg[bucket].transaction_types.push(`${row.transaction_type} (${row.row_count})`);
}

console.log('--- bucket 集計 ---');
const bucketOrder = ['order', 'refund', 'reimbursement', 'fee', 'adjustment', 'reserve', '__UNKNOWN__'];
for (const b of bucketOrder) {
  const agg = bucketAgg[b];
  if (!agg) {
    console.log(`  ${b}: (なし)`);
    continue;
  }
  const pctRows = (100 * agg.row_count / totalRows).toFixed(3);
  const pctAmt = (100 * agg.abs_micro / totalAbsMicro).toFixed(3);
  console.log(`  ${b}: ${agg.row_count.toLocaleString()} 行 (${pctRows}%) / ${(agg.abs_micro / 1e6).toFixed(0).toLocaleString()} 円 (${pctAmt}%)`);
}

const unknown = bucketAgg['__UNKNOWN__'] || { row_count: 0, abs_micro: 0, transaction_types: [] };
const unknownRowsPct = 100 * unknown.row_count / totalRows;
const unknownAmtPct = 100 * unknown.abs_micro / totalAbsMicro;

if (unknown.transaction_types.length > 0) {
  console.log('\n--- __UNKNOWN__ 詳細 ---');
  for (const t of unknown.transaction_types) console.log(`  - ${t}`);
}

// ============================================================
// 2. 判定
// ============================================================
const ROW_PCT_THRESHOLD = 0.5;     // 0.5% 未満
const AMT_PCT_THRESHOLD = 0.1;     // 0.1% 未満

console.log('\n--- Acceptance Criteria 判定 ---');
const rowOk = unknownRowsPct < ROW_PCT_THRESHOLD;
const amtOk = unknownAmtPct < AMT_PCT_THRESHOLD;
console.log(`  __UNKNOWN__ row 率: ${unknownRowsPct.toFixed(3)}% (< ${ROW_PCT_THRESHOLD}% 必要) ${rowOk ? '✓ PASS' : '✗ FAIL'}`);
console.log(`  __UNKNOWN__ abs amount 率: ${unknownAmtPct.toFixed(3)}% (< ${AMT_PCT_THRESHOLD}% 必要) ${amtOk ? '✓ PASS' : '✗ FAIL'}`);

// ============================================================
// 3. fixture test (主要 transaction pattern)
// ============================================================
console.log('\n--- Fixture Test (15 pattern) ---');

const FIXTURES = [
  { transaction_type: 'Order', price_type: 'Principal', expected_metric: 'sales_principal_jpy' },
  { transaction_type: 'Order', price_type: 'Shipping', expected_metric: 'sales_shipping_jpy' },
  { transaction_type: 'Order', price_type: 'GiftWrap', expected_metric: 'sales_giftwrap_jpy' },
  { transaction_type: 'Order', price_type: 'Tax', expected_metric: 'sales_tax_jpy' },
  { transaction_type: 'Order', price_type: 'ShippingTax', expected_metric: 'sales_tax_jpy' },
  { transaction_type: 'Order', item_related_fee_type: 'Commission', expected_metric: 'commission_jpy' },
  { transaction_type: 'Order', item_related_fee_type: 'FBAPerUnitFulfillmentFee', expected_metric: 'fba_fulfillment_jpy' },
  { transaction_type: 'Order', item_related_fee_type: 'ShippingChargeback', expected_metric: 'shipping_chargeback_jpy' },
  { transaction_type: 'Order', item_related_fee_type: 'GiftwrapChargeback', expected_metric: 'giftwrap_chargeback_jpy' },
  { transaction_type: 'Order', promotion_amount_micro_set: true, expected_metric: 'promotion_jpy' },
  { transaction_type: 'Refund', price_type: 'Principal', expected_metric: 'refund_principal_jpy' },
  { transaction_type: 'WAREHOUSE_DAMAGE', expected_metric: 'warehouse_damage_jpy' },
  { transaction_type: 'WAREHOUSE_LOST', expected_metric: 'warehouse_lost_jpy' },
  { transaction_type: 'SAFE-T Reimbursement', expected_metric: 'safe_t_jpy' },
  { transaction_type: 'REVERSAL_REIMBURSEMENT', expected_metric: 'reversal_reimbursement_jpy' },
];

function determineMetric(row) {
  // mapping ロジック (canonical-metric-mapping.md と完全一致)
  let bucket = '__UNKNOWN__';
  for (const [b, list] of Object.entries(TX_BUCKETS)) {
    if (list.includes(row.transaction_type)) { bucket = b; break; }
  }

  // sales (order bucket)
  if (bucket === 'order') {
    if (row.price_type === 'Principal') return 'sales_principal_jpy';
    if (row.price_type === 'Shipping') return 'sales_shipping_jpy';
    if (row.price_type === 'GiftWrap') return 'sales_giftwrap_jpy';
  }
  // tax
  if (['Tax', 'ShippingTax', 'GiftWrapTax'].includes(row.price_type)) return 'sales_tax_jpy';

  // fees
  if (row.item_related_fee_type === 'Commission') return 'commission_jpy';
  if (row.item_related_fee_type === 'FBAPerUnitFulfillmentFee') return 'fba_fulfillment_jpy';
  if (row.item_related_fee_type === 'ShippingChargeback') return 'shipping_chargeback_jpy';
  if (row.item_related_fee_type === 'GiftwrapChargeback') return 'giftwrap_chargeback_jpy';

  // storage
  if (['Storage Fee', 'StorageRenewalBilling', 'Storage Fee - Reversal', 'Storage Fee - Correction'].includes(row.transaction_type)) {
    return 'fba_storage_jpy';
  }

  // promotion
  if (row.promotion_amount_micro_set) return 'promotion_jpy';

  // refund principal
  if (bucket === 'refund' && row.price_type === 'Principal') return 'refund_principal_jpy';

  // reimbursement
  if (['WAREHOUSE_DAMAGE', 'WAREHOUSE_DAMAGE_EXCEPTION'].includes(row.transaction_type)) return 'warehouse_damage_jpy';
  if (row.transaction_type === 'WAREHOUSE_LOST') return 'warehouse_lost_jpy';
  if (row.transaction_type === 'SAFE-T Reimbursement') return 'safe_t_jpy';
  if (['REVERSAL_REIMBURSEMENT', 'Goodwill Concession', 'Fee Adjustment', 'Overpaid Fees Adjustment'].includes(row.transaction_type)) {
    return 'reversal_reimbursement_jpy';
  }

  return null; // 上記いずれも該当しない
}

let fixturePass = 0;
let fixtureFail = 0;
for (const fx of FIXTURES) {
  const got = determineMetric(fx);
  const ok = got === fx.expected_metric;
  console.log(`  ${ok ? '✓' : '✗'} ${JSON.stringify(fx)}`);
  if (!ok) console.log(`     expected: ${fx.expected_metric}, got: ${got}`);
  if (ok) fixturePass++; else fixtureFail++;
}

const fixtureOk = fixtureFail === 0;
console.log(`\n  Fixture: ${fixturePass}/${FIXTURES.length} PASS ${fixtureOk ? '✓' : '✗'}`);

// ============================================================
// 4. 最終判定
// ============================================================
db.close();
console.log('\n--- 最終判定 ---');
const allOk = rowOk && amtOk && fixtureOk;
if (allOk) {
  console.log('✓ Mapping v1 coverage check PASSED');
  process.exit(0);
} else {
  console.log('✗ Mapping v1 coverage check FAILED');
  process.exit(1);
}
