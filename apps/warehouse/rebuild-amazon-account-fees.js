#!/usr/bin/env node
/**
 * rebuild-amazon-account-fees.js — Amazon アカウント単位フィー月次 fact (amazon-dashboard PR-C)
 *
 * settlement raw のうち SKU に紐付かないアカウント単位フィー (月次保管料 / 長期在庫追加手数料 /
 * 返送・廃棄 / 納品不備 / 低在庫手数料 / 月額登録料) を月次×fee_type で集計する。
 * これらは f_amazon_finance_sku_daily_v1 (SKU 粒度) に載らないため、SKU 別利益の合計と
 * アカウント全体の実利益の差分になる (2026-07-06 実測: 月 70〜80 万円規模)。
 *
 * 金額は raw の other_amount_micro 由来 (2026-07-06 実測で全額この列)。
 * **符号は Amazon のまま保持 (負 = 費用)**。Correction/Reversal も同 fee_type に合算 (net)。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/rebuild-amazon-account-fees.js --months 14
 *
 * 冪等: 対象期間を DELETE → INSERT (数百行、1 tx)。
 */
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthsBack = Math.min(Math.max(parseInt(getArg('--months'), 10) || 14, 1), 60);

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

// JST 今日から monthsBack ヶ月前の月初
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const fromMonth = new Date(Date.UTC(nowJst.getUTCFullYear(), nowJst.getUTCMonth() - (monthsBack - 1), 1));
const fromDate = fromMonth.toISOString().slice(0, 10);

const db = new Database(dbPath);
db.pragma('busy_timeout = 5000');

db.exec(`CREATE TABLE IF NOT EXISTS f_amazon_account_fees_monthly_v1 (
  month_start_jst TEXT NOT NULL CHECK(month_start_jst GLOB '????-??-01'),
  fee_type        TEXT NOT NULL,
  amount_jpy      REAL NOT NULL DEFAULT 0,   -- Amazon 符号のまま (負 = 費用)
  row_count       INTEGER NOT NULL DEFAULT 0,
  built_at        TEXT NOT NULL,
  PRIMARY KEY (month_start_jst, fee_type)
)`);

// transaction_type → fee_type mapping (2026-07-06 実データの distinct から作成)
// 未知の account-level fee が増えたら 'other_account_fee' に落ちて金額は漏れない
const builtAt = new Date().toISOString();
const result = db.transaction(() => {
  db.prepare(`DELETE FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst >= ?`).run(fromDate);
  const info = db.prepare(`
    INSERT INTO f_amazon_account_fees_monthly_v1 (month_start_jst, fee_type, amount_jpy, row_count, built_at)
    SELECT
      substr(economic_date, 1, 7) || '-01' AS month_start_jst,
      CASE
        WHEN transaction_type IN ('Storage Fee', 'Storage Fee - Correction', 'Storage Fee - Reversal') THEN 'storage'
        WHEN transaction_type = 'StorageRenewalBilling' THEN 'long_term_storage'
        WHEN transaction_type = 'RemovalComplete' THEN 'removal'
        WHEN transaction_type LIKE 'Inbound Defect Fee%' THEN 'inbound_defect'
        WHEN transaction_type LIKE '%LowInventory%' OR transaction_type LIKE '%Low-Inventory%' THEN 'low_inventory'
        WHEN transaction_type = 'Subscription Fee' THEN 'subscription'
        ELSE 'other_account_fee'
      END AS fee_type,
      SUM(COALESCE(other_amount_micro, 0)) / 1000000.0 AS amount_jpy,
      COUNT(*) AS row_count,
      ? AS built_at
    FROM raw_amazon_settlement_lines
    WHERE economic_date >= ?
      -- SKU 無し行のみ対象。SKU 付きフィー行 (Inbound Defect 等の一部) は
      -- SKU daily fact 側に流れるため、ここに入れると二重計上になる
      AND (seller_sku_normalized IS NULL OR seller_sku_normalized = '')
      AND (
        transaction_type IN (
          'Storage Fee', 'Storage Fee - Correction', 'Storage Fee - Reversal',
          'StorageRenewalBilling', 'RemovalComplete', 'Subscription Fee'
        )
        OR transaction_type LIKE 'Inbound Defect Fee%'
        OR transaction_type LIKE '%LowInventory%' OR transaction_type LIKE '%Low-Inventory%'
      )
    GROUP BY 1, 2
  `).run(builtAt, fromDate);
  return info.changes;
})();

const rows = db.prepare(`
  SELECT month_start_jst, fee_type, ROUND(amount_jpy) amount, row_count
  FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst >= ? ORDER BY 1, 2
`).all(fromDate);
db.close();

console.log(`✓ f_amazon_account_fees_monthly_v1 rebuilt: ${result} rows (from ${fromDate})`);
for (const r of rows) console.log(`  ${r.month_start_jst.slice(0, 7)} ${r.fee_type}: ¥${r.amount.toLocaleString()} (${r.row_count} lines)`);
process.exit(0);
