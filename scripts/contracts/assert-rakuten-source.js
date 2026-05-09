#!/usr/bin/env node
/**
 * raw_rakuten_orders の source contract と実 DB を突合し、
 * 列追加 / 列欠落 / 型変更を検知する schema check。
 *
 * Phase 1a ticket: #R-0a (楽天売上分析)
 * Contract: docs/contracts/raw_rakuten_orders.contract.md (v1)
 * 検査 SQL: sql/contracts/check_raw_rakuten_orders.sql (詳細統計はこちら)
 *
 * 終了コード:
 *   0 = contract と実 DB が一致 (or warning のみ)
 *   1 = error あり (必須列の欠落 / 型変更 / NOT NULL 違反 / 依存先欠落)
 *   2 = env 不備
 *
 * 注: 列順 (cid) は本 assert では検証しない。列順が変わっても列名で照合するため動作に影響なし。
 *     列順を業務契約に含めたくなったら EXPECTED_COLUMNS の cid を比較するロジックを追加すること。
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data node scripts/contracts/assert-rakuten-source.js
 *   (DATA_DIR 必須、未指定は fail-fast、cwd fallback の事故防止)
 */

import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

const DATA_DIR = (process.env.DATA_DIR || process.argv[2] || '').trim();
if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR environment variable or first argument is required.');
  console.error('  Example: node scripts/contracts/assert-rakuten-source.js C:/Users/bfaith/bfaith-portal/data');
  process.exit(2);
}

const DB_PATH = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(DB_PATH)) {
  console.error(`FATAL: warehouse.db not found at ${DB_PATH}`);
  process.exit(2);
}

const CONTRACT_VERSION = 'v1';
const TABLE_NAME = 'raw_rakuten_orders';

// docs/contracts/raw_rakuten_orders.contract.md の §2 と同期
const EXPECTED_COLUMNS = [
  { cid: 0,  name: 'id',                      type: 'INTEGER', notnull: 0 },
  { cid: 1,  name: 'order_number',            type: 'TEXT',    notnull: 1 },
  { cid: 2,  name: 'order_date',              type: 'TEXT',    notnull: 1 },
  { cid: 3,  name: 'order_status',            type: 'INTEGER', notnull: 1 },
  { cid: 4,  name: 'goods_price',             type: 'REAL',    notnull: 1 },
  { cid: 5,  name: 'goods_tax',               type: 'REAL',    notnull: 0 },
  { cid: 6,  name: 'total_price',             type: 'REAL',    notnull: 1 },
  { cid: 7,  name: 'request_price',           type: 'REAL',    notnull: 0 },
  { cid: 8,  name: 'postage_price',           type: 'REAL',    notnull: 1 },
  { cid: 9,  name: 'coupon_shop_price',       type: 'REAL',    notnull: 1 },
  { cid: 10, name: 'coupon_all_total_price',  type: 'REAL',    notnull: 1 },
  { cid: 11, name: 'item_detail_id',          type: 'INTEGER', notnull: 1 },
  { cid: 12, name: 'item_number',             type: 'TEXT',    notnull: 1 },
  { cid: 13, name: 'item_name',               type: 'TEXT',    notnull: 0 },
  { cid: 14, name: 'price',                   type: 'REAL',    notnull: 1 },
  { cid: 15, name: 'price_tax_incl',          type: 'REAL',    notnull: 1 },
  { cid: 16, name: 'units',                   type: 'INTEGER', notnull: 1 },
  { cid: 17, name: 'tax_rate',                type: 'REAL',    notnull: 1 },
  { cid: 18, name: 'selected_choice',         type: 'TEXT',    notnull: 0 },
  { cid: 19, name: 'delete_item_flag',        type: 'INTEGER', notnull: 1 },
  { cid: 20, name: 'synced_at',               type: 'TEXT',    notnull: 1 },
];

// 値域 invariant (assert で検出すべき業務違反)
//   - order_status: 700/900/200/300/500/600 のみ (新値が出たら警告 = forward incompatible 兆候)
const KNOWN_ORDER_STATUSES = new Set([700, 900, 200, 300, 500, 600]);
//   - delete_item_flag: 全件 0 (1 が出たら運用変更検出)
const ALLOWED_DELETE_FLAGS = new Set([0]);
//   - tax_rate: 0.10 / 0.08 のみ (新税率追加で過去率と並ぶ場合は要再検証)
const KNOWN_TAX_RATES = new Set([0.10, 0.08]);

const db = new Database(DB_PATH, { readonly: true });

console.log(`=== assert-rakuten-source ${CONTRACT_VERSION} ===`);
console.log(`DB: ${DB_PATH}`);
console.log(`Table: ${TABLE_NAME}\n`);

let exitCode = 0;
const warnings = [];
const errors = [];

// ----------------------------------------------------------------
// 1. 列定義 一致確認
// ----------------------------------------------------------------
console.log('--- 1. 列定義照合 ---');
const actual = db.prepare(`PRAGMA table_info(${TABLE_NAME})`).all();
console.log(`  expected: ${EXPECTED_COLUMNS.length} cols, actual: ${actual.length} cols`);

const actualByName = new Map(actual.map(c => [c.name, c]));

for (const exp of EXPECTED_COLUMNS) {
  const act = actualByName.get(exp.name);
  if (!act) {
    errors.push(`COLUMN MISSING: ${exp.name} (expected at cid=${exp.cid}, type=${exp.type})`);
    continue;
  }
  if (act.type !== exp.type) {
    errors.push(`TYPE MISMATCH: ${exp.name} expected=${exp.type}, actual=${act.type}`);
  }
  if (act.notnull !== exp.notnull) {
    warnings.push(`NOTNULL MISMATCH: ${exp.name} expected=${exp.notnull}, actual=${act.notnull}`);
  }
}

const expectedNames = new Set(EXPECTED_COLUMNS.map(c => c.name));
for (const act of actual) {
  if (!expectedNames.has(act.name)) {
    warnings.push(`COLUMN ADDED (forward-compatible): ${act.name} ${act.type}`);
  }
}

// ----------------------------------------------------------------
// 2. order_status 値域確認
// ----------------------------------------------------------------
console.log('\n--- 2. order_status 値域 ---');
const statuses = db.prepare(`SELECT order_status, COUNT(*) AS c FROM ${TABLE_NAME} GROUP BY order_status ORDER BY c DESC`).all();
for (const s of statuses) {
  console.log(`  status=${s.order_status}: ${s.c.toLocaleString()}`);
  if (!KNOWN_ORDER_STATUSES.has(s.order_status)) {
    warnings.push(`UNKNOWN order_status: ${s.order_status} (count=${s.c}) — known values: ${[...KNOWN_ORDER_STATUSES].join(',')}`);
  }
}

// ----------------------------------------------------------------
// 3. delete_item_flag 確認 (全件 0 期待)
// ----------------------------------------------------------------
console.log('\n--- 3. delete_item_flag ---');
const flags = db.prepare(`SELECT delete_item_flag, COUNT(*) AS c FROM ${TABLE_NAME} GROUP BY delete_item_flag`).all();
for (const f of flags) {
  console.log(`  flag=${f.delete_item_flag}: ${f.c.toLocaleString()}`);
  if (!ALLOWED_DELETE_FLAGS.has(f.delete_item_flag)) {
    warnings.push(`UNEXPECTED delete_item_flag: ${f.delete_item_flag} (count=${f.c}) — allowed: ${[...ALLOWED_DELETE_FLAGS].join(',')}`);
  }
}

// ----------------------------------------------------------------
// 3.5. tax_rate 値域確認 (Codex PR #75 #3 追加)
// ----------------------------------------------------------------
console.log('\n--- 3.5. tax_rate 値域 (0.10 / 0.08 のみ期待) ---');
const taxRates = db.prepare(`SELECT tax_rate, COUNT(*) AS c FROM ${TABLE_NAME} GROUP BY tax_rate ORDER BY c DESC`).all();
for (const t of taxRates) {
  console.log(`  tax_rate=${t.tax_rate}: ${t.c.toLocaleString()}`);
  if (!KNOWN_TAX_RATES.has(t.tax_rate)) {
    warnings.push(`UNKNOWN tax_rate: ${t.tax_rate} (count=${t.c}) — known values: ${[...KNOWN_TAX_RATES].join(',')}`);
  }
}

// ----------------------------------------------------------------
// 4. 主要列 NULL 率
// ----------------------------------------------------------------
console.log('\n--- 4. 主要列 NULL 率 (notnull 列で 0% 期待) ---');
const nulls = db.prepare(`
  SELECT
    SUM(CASE WHEN order_number IS NULL THEN 1 ELSE 0 END)   AS null_order_number,
    SUM(CASE WHEN order_date   IS NULL THEN 1 ELSE 0 END)   AS null_order_date,
    SUM(CASE WHEN item_number  IS NULL THEN 1 ELSE 0 END)   AS null_item_number,
    SUM(CASE WHEN price_tax_incl IS NULL THEN 1 ELSE 0 END) AS null_price_tax_incl,
    SUM(CASE WHEN units        IS NULL THEN 1 ELSE 0 END)   AS null_units,
    SUM(CASE WHEN tax_rate     IS NULL THEN 1 ELSE 0 END)   AS null_tax_rate
  FROM ${TABLE_NAME}
`).get();
for (const [k, v] of Object.entries(nulls)) {
  console.log(`  ${k}: ${v}`);
  if (v > 0) {
    errors.push(`NOTNULL VIOLATION: ${k} = ${v} rows`);
  }
}

// ----------------------------------------------------------------
// 5. fact 依存先の存在確認 (build 時に LEFT JOIN するテーブル)
// ----------------------------------------------------------------
console.log('\n--- 5. fact 依存先存在確認 ---');
for (const tname of ['fact_promotion_cost', 'fact_returns', 'f_rakuten_sku_map']) {
  const exists = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(tname);
  if (exists) {
    const c = db.prepare(`SELECT COUNT(*) AS c FROM "${tname}"`).get().c;
    console.log(`  ${tname}: ${c.toLocaleString()} rows ✓`);
  } else {
    errors.push(`DEPENDENCY MISSING: ${tname} table not found (build will fail)`);
  }
}

// ----------------------------------------------------------------
// 6. 結果サマリ
// ----------------------------------------------------------------
console.log('\n=== 結果 ===');
if (errors.length === 0 && warnings.length === 0) {
  console.log('✓ contract と実 DB は完全一致 (warning も無し)');
} else {
  if (warnings.length > 0) {
    console.log(`⚠ warnings (${warnings.length}):`);
    for (const w of warnings) console.log(`  - ${w}`);
  }
  if (errors.length > 0) {
    console.log(`✗ errors (${errors.length}):`);
    for (const e of errors) console.log(`  - ${e}`);
    exitCode = 1;
  }
}

db.close();
process.exit(exitCode);
