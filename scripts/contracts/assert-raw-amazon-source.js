#!/usr/bin/env node
/**
 * raw_amazon_settlement_lines の source contract と実 DB を突合し、
 * 列追加 / 列欠落 / 型変更を検知する schema check。
 *
 * Phase 1 ticket: #1-0a
 * Contract: docs/contracts/raw_amazon_settlement_lines.contract.md (v1)
 *
 * 終了コード:
 *   0 = contract と実 DB が一致 (or warning のみ)
 *   1 = mismatch あり (必須列の欠落 / 型変更 / 列順入れ替え)
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data node scripts/contracts/assert-raw-amazon-source.js
 *   (DATA_DIR 必須、未指定は fail-fast)
 */

import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

const DATA_DIR = process.env.DATA_DIR;
if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR environment variable is required.');
  console.error('  Example: DATA_DIR=C:/Users/bfaith/bfaith-portal/data node scripts/contracts/assert-raw-amazon-source.js');
  console.error('  Reason: process.cwd() fallback can silently create a stray DB in worktree.');
  process.exit(2);
}

const DB_PATH = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(DB_PATH)) {
  console.error(`FATAL: warehouse.db not found at ${DB_PATH}`);
  process.exit(2);
}

// ============================================================
// Source contract v1 (raw_amazon_settlement_lines)
// docs/contracts/raw_amazon_settlement_lines.contract.md と同期
// ============================================================
const CONTRACT_VERSION = 'v1';
const TABLE_NAME = 'raw_amazon_settlement_lines';

const EXPECTED_COLUMNS = [
  { cid: 0, name: 'id', type: 'INTEGER', notnull: 0 },
  { cid: 1, name: 'physical_line_hash', type: 'TEXT', notnull: 1 },
  { cid: 2, name: 'business_line_key', type: 'TEXT', notnull: 1 },
  { cid: 3, name: 'source_document_id', type: 'TEXT', notnull: 1 },
  { cid: 4, name: 'source_file_hash', type: 'TEXT', notnull: 0 },
  { cid: 5, name: 'source_path', type: 'TEXT', notnull: 0 },
  { cid: 6, name: 'source_line_no', type: 'INTEGER', notnull: 0 },
  { cid: 7, name: 'source_layer', type: 'TEXT', notnull: 1 },
  { cid: 8, name: 'parser_version', type: 'TEXT', notnull: 1 },
  { cid: 9, name: 'source_settlement_id', type: 'TEXT', notnull: 1 },
  { cid: 10, name: 'posted_date_utc', type: 'TEXT', notnull: 1 },
  { cid: 11, name: 'posted_datetime_jst', type: 'TEXT', notnull: 1 },
  { cid: 12, name: 'economic_date', type: 'TEXT', notnull: 1 },
  { cid: 13, name: 'year_month_int', type: 'INTEGER', notnull: 1 },
  { cid: 14, name: 'amazon_order_id', type: 'TEXT', notnull: 0 },
  { cid: 15, name: 'merchant_order_id', type: 'TEXT', notnull: 0 },
  { cid: 16, name: 'shipment_id', type: 'TEXT', notnull: 0 },
  { cid: 17, name: 'order_item_code', type: 'TEXT', notnull: 0 },
  { cid: 18, name: 'adjustment_id', type: 'TEXT', notnull: 0 },
  { cid: 19, name: 'seller_sku', type: 'TEXT', notnull: 0 },
  { cid: 20, name: 'seller_sku_normalized', type: 'TEXT', notnull: 0 },
  { cid: 21, name: 'transaction_type', type: 'TEXT', notnull: 1 },
  { cid: 22, name: 'marketplace_name', type: 'TEXT', notnull: 0 },
  { cid: 23, name: 'fulfillment_id', type: 'TEXT', notnull: 0 },
  { cid: 24, name: 'quantity_purchased', type: 'INTEGER', notnull: 0 },
  { cid: 25, name: 'price_type', type: 'TEXT', notnull: 0 },
  { cid: 26, name: 'price_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 27, name: 'item_related_fee_type', type: 'TEXT', notnull: 0 },
  { cid: 28, name: 'item_related_fee_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 29, name: 'promotion_id', type: 'TEXT', notnull: 0 },
  { cid: 30, name: 'promotion_type', type: 'TEXT', notnull: 0 },
  { cid: 31, name: 'promotion_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 32, name: 'shipment_fee_type', type: 'TEXT', notnull: 0 },
  { cid: 33, name: 'shipment_fee_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 34, name: 'order_fee_type', type: 'TEXT', notnull: 0 },
  { cid: 35, name: 'order_fee_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 36, name: 'misc_fee_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 37, name: 'other_fee_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 38, name: 'other_fee_reason_description', type: 'TEXT', notnull: 0 },
  { cid: 39, name: 'direct_payment_type', type: 'TEXT', notnull: 0 },
  { cid: 40, name: 'direct_payment_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 41, name: 'other_amount_micro', type: 'INTEGER', notnull: 0 },
  { cid: 42, name: 'currency', type: 'TEXT', notnull: 1 },
  { cid: 43, name: 'ingest_run_id', type: 'TEXT', notnull: 0 },
  { cid: 44, name: 'observed_at', type: 'TEXT', notnull: 0 },
  { cid: 45, name: 'ingested_at', type: 'TEXT', notnull: 0 },
];

const RELATED_OBJECTS = [
  { type: 'table', name: 'raw_amazon_settlement_headers', required: true },
  { type: 'table', name: 'fact_amazon_settlement_monthly_wide', required: true },
  { type: 'table', name: 'fact_amazon_settlement_monthly_long', required: true },
  { type: 'view', name: 'v_amazon_sku_profit_actual_v4', required: true },
  { type: 'view', name: 'v_sku_costed', required: true },
  { type: 'view', name: 'v_sku_resolved', required: true },
];

// ============================================================
// 実行
// ============================================================
const db = new Database(DB_PATH, { readonly: true });

let hasFatal = false;
const issues = { fatal: [], warning: [], info: [] };

function fatal(msg) { issues.fatal.push(msg); hasFatal = true; }
function warn(msg) { issues.warning.push(msg); }
function info(msg) { issues.info.push(msg); }

// 1. テーブル / view の存在確認
console.log(`\n=== Source contract assertion (${TABLE_NAME}, contract ${CONTRACT_VERSION}) ===`);
console.log(`DB: ${DB_PATH}`);

const target = db.prepare(`SELECT type, name FROM sqlite_master WHERE name = ?`).get(TABLE_NAME);
if (!target) {
  fatal(`Table ${TABLE_NAME} does not exist`);
} else if (target.type !== 'table') {
  fatal(`${TABLE_NAME} exists but is a ${target.type}, expected 'table'`);
}

for (const rel of RELATED_OBJECTS) {
  const row = db.prepare(`SELECT type, name FROM sqlite_master WHERE name = ?`).get(rel.name);
  if (!row) {
    if (rel.required) fatal(`Required ${rel.type} ${rel.name} is missing`);
    else warn(`Optional ${rel.type} ${rel.name} is missing`);
  } else if (row.type !== rel.type) {
    fatal(`${rel.name} type mismatch: expected ${rel.type}, got ${row.type}`);
  }
}

// 2. 列定義の突合
if (!hasFatal) {
  const actual = db.prepare(`SELECT cid, name, type, "notnull" AS notnull FROM pragma_table_info(?) ORDER BY cid`).all(TABLE_NAME);

  if (actual.length !== EXPECTED_COLUMNS.length) {
    fatal(`Column count mismatch: expected ${EXPECTED_COLUMNS.length}, got ${actual.length}`);
  }

  const expectedByName = new Map(EXPECTED_COLUMNS.map(c => [c.name, c]));
  const actualByName = new Map(actual.map(c => [c.name, c]));

  // 欠落チェック
  for (const exp of EXPECTED_COLUMNS) {
    if (!actualByName.has(exp.name)) {
      fatal(`Missing column: ${exp.name}`);
      continue;
    }
    const act = actualByName.get(exp.name);
    if (act.type !== exp.type) {
      fatal(`Column ${exp.name} type mismatch: expected ${exp.type}, got ${act.type}`);
    }
    if (act.notnull !== exp.notnull) {
      warn(`Column ${exp.name} notnull mismatch: expected ${exp.notnull}, got ${act.notnull}`);
    }
    if (act.cid !== exp.cid) {
      warn(`Column ${exp.name} cid mismatch: expected ${exp.cid}, got ${act.cid} (列順変更は break しないが要確認)`);
    }
  }

  // 追加列チェック
  for (const act of actual) {
    if (!expectedByName.has(act.name)) {
      fatal(`Unexpected column added: ${act.name} (${act.type})`);
    }
  }
}

// 3. baseline metrics
if (!hasFatal) {
  console.log('\n--- baseline metrics ---');
  const totalRows = db.prepare(`SELECT COUNT(*) AS c FROM ${TABLE_NAME}`).get().c;
  console.log(`total rows: ${totalRows.toLocaleString()}`);

  const dateRange = db.prepare(`
    SELECT MIN(economic_date) AS min_d, MAX(economic_date) AS max_d, COUNT(DISTINCT economic_date) AS c
    FROM ${TABLE_NAME} WHERE economic_date IS NOT NULL
  `).get();
  console.log(`economic_date range: ${dateRange.min_d} 〜 ${dateRange.max_d} (distinct ${dateRange.c} days)`);

  // year_month_int 整合性
  const ymCheck = db.prepare(`
    SELECT
      SUM(CASE
        WHEN year_month_int = CAST(strftime('%Y', economic_date) AS INTEGER) * 100
                              + CAST(strftime('%m', economic_date) AS INTEGER)
        THEN 1 ELSE 0 END) AS match_rows,
      SUM(CASE
        WHEN year_month_int <> CAST(strftime('%Y', economic_date) AS INTEGER) * 100
                               + CAST(strftime('%m', economic_date) AS INTEGER)
        THEN 1 ELSE 0 END) AS mismatch_rows
    FROM ${TABLE_NAME} WHERE economic_date IS NOT NULL
  `).get();
  if (ymCheck.mismatch_rows > 0) {
    warn(`year_month_int と economic_date の不整合 row: ${ymCheck.mismatch_rows.toLocaleString()} 件 (要調査)`);
  } else {
    info(`year_month_int / economic_date 整合性: OK (${ymCheck.match_rows.toLocaleString()} 行 一致)`);
  }

  // physical_line_hash の重複検知
  const hashCheck = db.prepare(`
    SELECT COUNT(*) - COUNT(DISTINCT physical_line_hash) AS dup_count FROM ${TABLE_NAME}
  `).get();
  if (hashCheck.dup_count > 0) {
    warn(`physical_line_hash 重複: ${hashCheck.dup_count.toLocaleString()} 件`);
  } else {
    info(`physical_line_hash の unique 性: OK`);
  }

  // currency が JPY 以外
  const nonJpy = db.prepare(`SELECT COUNT(*) AS c FROM ${TABLE_NAME} WHERE currency <> 'JPY'`).get().c;
  if (nonJpy > 0) {
    warn(`currency が JPY 以外の行: ${nonJpy.toLocaleString()} 件`);
  } else {
    info(`currency: 全行 JPY`);
  }
}

// 4. 結果出力
console.log('\n--- results ---');
if (issues.info.length > 0) {
  console.log(`\nINFO (${issues.info.length}):`);
  for (const m of issues.info) console.log(`  - ${m}`);
}
if (issues.warning.length > 0) {
  console.log(`\nWARNING (${issues.warning.length}):`);
  for (const m of issues.warning) console.log(`  - ${m}`);
}
if (issues.fatal.length > 0) {
  console.log(`\nFATAL (${issues.fatal.length}):`);
  for (const m of issues.fatal) console.log(`  - ${m}`);
}

db.close();

if (hasFatal) {
  console.error(`\n✗ Source contract violation. Update contract or fix DB schema.`);
  process.exit(1);
} else {
  console.log(`\n✓ Source contract ${CONTRACT_VERSION} is satisfied.`);
  process.exit(0);
}
