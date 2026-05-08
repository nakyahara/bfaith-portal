#!/usr/bin/env node
/**
 * raw_amazon_settlement_lines の transaction_type / *_type / *_reason_description
 * の distinct 値を抽出し、baseline metrics と一緒に JSON で出力する。
 *
 * Phase 1 ticket: #1-0a の検証フェーズ
 *
 * sqlite3 CLI が miniPC に無いため、check_raw_amazon_settlement_lines.sql の
 * 主要セクションを Node で実行できるようにしたもの。
 *
 * 使い方 (miniPC SSH):
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data \
 *     node scripts/contracts/extract-amazon-source-distinct-values.js
 *
 * 出力: stdout に整形済 JSON
 */

import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

// DATA_DIR は env または第1引数で受ける (SSH 経由時の env 渡し問題回避)
const DATA_DIR = (process.env.DATA_DIR || process.argv[2] || '').trim();
if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR environment variable or first argument is required.');
  console.error('  Example: node ... C:/Users/bfaith/bfaith-portal/data');
  process.exit(2);
}

const DB_PATH = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(DB_PATH)) {
  console.error(`FATAL: warehouse.db not found at ${DB_PATH}`);
  process.exit(2);
}

const db = new Database(DB_PATH, { readonly: true });

const result = {
  contract_version: 'v1',
  table: 'raw_amazon_settlement_lines',
  generated_at: new Date().toISOString(),
  baseline: {},
  distinct: {},
};

// 1. baseline
result.baseline.total_rows = db.prepare(`SELECT COUNT(*) AS c FROM raw_amazon_settlement_lines`).get().c;

const dateRange = db.prepare(`
  SELECT MIN(economic_date) AS min_d, MAX(economic_date) AS max_d, COUNT(DISTINCT economic_date) AS dc
  FROM raw_amazon_settlement_lines WHERE economic_date IS NOT NULL
`).get();
result.baseline.economic_date_min = dateRange.min_d;
result.baseline.economic_date_max = dateRange.max_d;
result.baseline.economic_date_distinct_count = dateRange.dc;

result.baseline.monthly_row_count = db.prepare(`
  SELECT substr(economic_date, 1, 7) AS month_jst, COUNT(*) AS c
  FROM raw_amazon_settlement_lines WHERE economic_date IS NOT NULL
  GROUP BY 1 ORDER BY 1
`).all();

// year_month_int との整合性
const ymCheck = db.prepare(`
  SELECT
    SUM(CASE WHEN year_month_int = CAST(strftime('%Y', economic_date) AS INTEGER) * 100
                                   + CAST(strftime('%m', economic_date) AS INTEGER) THEN 1 ELSE 0 END) AS match_rows,
    SUM(CASE WHEN year_month_int <> CAST(strftime('%Y', economic_date) AS INTEGER) * 100
                                    + CAST(strftime('%m', economic_date) AS INTEGER) THEN 1 ELSE 0 END) AS mismatch_rows
  FROM raw_amazon_settlement_lines WHERE economic_date IS NOT NULL
`).get();
result.baseline.year_month_int_match_rows = ymCheck.match_rows;
result.baseline.year_month_int_mismatch_rows = ymCheck.mismatch_rows;

// physical_line_hash 重複
const hashCheck = db.prepare(`
  SELECT COUNT(*) - COUNT(DISTINCT physical_line_hash) AS dup_count FROM raw_amazon_settlement_lines
`).get();
result.baseline.physical_line_hash_duplicate_count = hashCheck.dup_count;

// currency
result.baseline.currency_distinct = db.prepare(`
  SELECT currency, COUNT(*) AS c FROM raw_amazon_settlement_lines GROUP BY currency ORDER BY c DESC
`).all();

// seller_sku_normalized null 率
const skuNullCheck = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN seller_sku_normalized IS NULL THEN 1 ELSE 0 END) AS null_count,
    SUM(CASE WHEN trim(seller_sku_normalized) = '' THEN 1 ELSE 0 END) AS empty_count
  FROM raw_amazon_settlement_lines
`).get();
result.baseline.seller_sku_normalized_null_count = skuNullCheck.null_count;
result.baseline.seller_sku_normalized_empty_count = skuNullCheck.empty_count;
result.baseline.seller_sku_normalized_null_or_empty_rate_pct =
  Math.round(10000 * (skuNullCheck.null_count + skuNullCheck.empty_count) / skuNullCheck.total) / 100;

// 2. distinct 値抽出
const cols = [
  'transaction_type',
  'price_type',
  'item_related_fee_type',
  'shipment_fee_type',
  'order_fee_type',
  'promotion_type',
];

for (const col of cols) {
  result.distinct[col] = db.prepare(`
    SELECT COALESCE(${col}, '__NULL__') AS value, COUNT(*) AS c
    FROM raw_amazon_settlement_lines GROUP BY 1 ORDER BY c DESC
  `).all();
}

// other_fee_reason_description は値が長いので TOP 50
result.distinct.other_fee_reason_description_top50 = db.prepare(`
  SELECT COALESCE(other_fee_reason_description, '__NULL__') AS value, COUNT(*) AS c
  FROM raw_amazon_settlement_lines GROUP BY 1 ORDER BY c DESC LIMIT 50
`).all();

// 3. 金額列の min/max (micro 単位の妥当性確認)
result.baseline.amount_min_max = db.prepare(`
  SELECT
    MIN(price_amount_micro) AS min_price_micro,
    MAX(price_amount_micro) AS max_price_micro,
    MIN(item_related_fee_amount_micro) AS min_item_fee_micro,
    MAX(item_related_fee_amount_micro) AS max_item_fee_micro,
    MIN(shipment_fee_amount_micro) AS min_shipment_fee_micro,
    MAX(shipment_fee_amount_micro) AS max_shipment_fee_micro,
    MIN(order_fee_amount_micro) AS min_order_fee_micro,
    MAX(order_fee_amount_micro) AS max_order_fee_micro,
    MIN(promotion_amount_micro) AS min_promotion_micro,
    MAX(promotion_amount_micro) AS max_promotion_micro,
    MIN(other_amount_micro) AS min_other_amount_micro,
    MAX(other_amount_micro) AS max_other_amount_micro
  FROM raw_amazon_settlement_lines
`).get();

db.close();
console.log(JSON.stringify(result, null, 2));
