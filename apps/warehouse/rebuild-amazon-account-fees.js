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
// 🚨 2026-09-28: Amazon が決済の取引の名前を変えていた (7 月から保管料・長期保管料、6 月から返送料) のに古い名前しか拾わず、
//   7〜9 月の保管料 (月 30〜66 万円)・長期保管料 (月 約 10 万円)・返送料が 0 = アカウント全体の利益が月 40〜80 万円多く出ていた。
//   新しい名前を足した + 分けられない SKU なしの取引が出たら最後の行を ⚠️ にする (daily-sync で「全部 OK」に数えない = 次に名前が変わったら気づく)
const FEE_TYPE_RULES = [
  // [fee_type, 完全一致の名前, 前方一致の名前]
  ['storage', ['Storage Fee', 'Storage Fee - Correction', 'Storage Fee - Reversal'], ['FBA Inventory Storage Fee']],   // 2026-07〜 FBA Inventory Storage Fee
  ['long_term_storage', ['StorageRenewalBilling'], ['FBA Long Term Storage Fee']],                                        // 2026-07〜 FBA Long Term Storage Fee
  ['removal', ['RemovalComplete'], ['FBA Removal Order']],                                                                // 2026-06〜 FBA Removal Order: Return Fee
  ['inbound_defect', [], ['Inbound Defect Fee']],
  ['low_inventory', [], []],   // '%LowInventory%' / '%Low-Inventory%' (下の LIKE)
  ['subscription', ['Subscription Fee'], []],
];
// アカウント単位の手数料に入れない SKU なしの取引 (今までも入れていない。これ以外の SKU なしの取引が出たら ⚠️)
//   Easy Ship の料金 = 注文ごとの配送料 (別で扱う・2026-09-28 時点で扱いは中原さんに確認中) / 預かり金の出し入れ (Current / Previous Reserve = 相殺) /
//   調整 (Fee Adjustment・Goodwill・Retrocharge・Overpaid・ServiceFee・BuyerRecharge)
const NOT_ACCOUNT_FEE = ['Amazon Easy Ship Charges', 'Current Reserve Amount', 'Previous Reserve Amount Balance', 'Fee Adjustment', 'Goodwill Concession',
  'Order_Retrocharge', 'Refund_Retrocharge', 'Overpaid Fees Adjustment', 'ServiceFee', 'BuyerRecharge'];
const q = (x) => `'${String(x).replace(/'/g, "''")}'`;
const matchSql = (exact, prefix) => [
  ...(exact.length ? [`transaction_type IN (${exact.map(q).join(', ')})`] : []),
  ...prefix.map((p) => `transaction_type LIKE ${q(p.replace(/[%_]/g, '') + '%')}`),
].join(' OR ');
const LOW_INV_SQL = `transaction_type LIKE '%LowInventory%' OR transaction_type LIKE '%Low-Inventory%'`;
const FEE_FILTER_SQL = [...FEE_TYPE_RULES.filter(([t]) => t !== 'low_inventory').map(([, e, p]) => matchSql(e, p)).filter(Boolean), LOW_INV_SQL].map((x) => `(${x})`).join(' OR ');
const FEE_CASE_SQL = `CASE ${FEE_TYPE_RULES.map(([t, e, p]) => `WHEN ${t === 'low_inventory' ? LOW_INV_SQL : matchSql(e, p)} THEN '${t}'`).join(' ')} ELSE 'other_account_fee' END`;
const builtAt = new Date().toISOString();
const result = db.transaction(() => {
  db.prepare(`DELETE FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst >= ?`).run(fromDate);
  const info = db.prepare(`
    INSERT INTO f_amazon_account_fees_monthly_v1 (month_start_jst, fee_type, amount_jpy, row_count, built_at)
    WITH occ AS (
      -- 同一 settlement が sp_api_v1 / manual_csv の両 layer で raw に存在し得るため、
      -- SKU別 mart (rebuild-amazon-settlement-mart.js) と同じ business dedup を挟む
      -- (Codex High 指摘: dedup 無しだと保管料/LTSF が二重計上)
      -- 🚨 同じ文書の中の出現順 (occ) を鍵に足す (本物の同じ鍵の別々の行を潰さない。db.js の v_amazon_settlement_unified と同じ形。2026-09-28)
      SELECT source_settlement_id, business_line_key, source_document_id, source_layer, ingested_at,
        economic_date, transaction_type, other_amount_micro,
        DENSE_RANK() OVER (PARTITION BY source_settlement_id, business_line_key, source_document_id ORDER BY source_line_no) AS occ
      FROM raw_amazon_settlement_lines
      WHERE economic_date >= ?
        -- SKU 無し行のみ対象。SKU 付きフィー行 (Inbound Defect 等の一部) は
        -- SKU daily fact 側に流れるため、ここに入れると二重計上になる
        AND (seller_sku_normalized IS NULL OR seller_sku_normalized = '')
        AND (${FEE_FILTER_SQL})
    ),
    dedup AS (
      SELECT economic_date, transaction_type, other_amount_micro,
        ROW_NUMBER() OVER (
          PARTITION BY source_settlement_id, business_line_key, occ
          ORDER BY CASE source_layer
                     WHEN 'sp_api_v1' THEN 1
                     WHEN 'sp_api_v2' THEN 1
                     WHEN 'manual_csv' THEN 2
                     ELSE 3
                   END,
                   ingested_at DESC,
                   source_document_id
        ) AS rn
      FROM occ
    )
    SELECT
      substr(economic_date, 1, 7) || '-01' AS month_start_jst,
      ${FEE_CASE_SQL} AS fee_type,
      SUM(COALESCE(other_amount_micro, 0)) / 1000000.0 AS amount_jpy,
      COUNT(*) AS row_count,
      ? AS built_at
    FROM dedup
    WHERE rn = 1
    GROUP BY 1, 2
  `).run(fromDate, builtAt);
  return info.changes;
})();

// 分けられない SKU なしの取引 (手数料の分け方にも、入れない一覧にも無い名前) = 名前が変わった手数料の疑い
const unknownTx = db.prepare(`
  SELECT transaction_type t, COUNT(*) n, SUM(COALESCE(other_amount_micro, 0) + COALESCE(item_related_fee_amount_micro, 0) + COALESCE(price_amount_micro, 0)) / 1000000.0 a,
         GROUP_CONCAT(DISTINCT substr(economic_date, 1, 7)) ms
    FROM raw_amazon_settlement_lines
   WHERE economic_date >= ? AND (seller_sku_normalized IS NULL OR seller_sku_normalized = '')
     AND NOT (${FEE_FILTER_SQL}) AND transaction_type NOT IN (${NOT_ACCOUNT_FEE.map(q).join(', ')})
   GROUP BY 1 ORDER BY 1`).all(fromDate);
const rows = db.prepare(`
  SELECT month_start_jst, fee_type, ROUND(amount_jpy) amount, row_count
  FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst >= ? ORDER BY 1, 2
`).all(fromDate);
db.close();

console.log(`✓ f_amazon_account_fees_monthly_v1 rebuilt: ${result} rows (from ${fromDate})`);
for (const r of rows) console.log(`  ${r.month_start_jst.slice(0, 7)} ${r.fee_type}: ¥${r.amount.toLocaleString()} (${r.row_count} lines)`);
if (unknownTx.length) {
  console.log(`⚠️ アカウント単位の手数料に分けられない SKU なしの取引 ${unknownTx.length} 種類: ${unknownTx.map((u) => `${u.t} (${u.n} 行・¥${Math.round(u.a).toLocaleString()}・${u.ms})`).join(' / ')} → rebuild-amazon-account-fees.js の FEE_TYPE_RULES か NOT_ACCOUNT_FEE に足す (名前が変わった手数料なら集計から漏れている)`);
} else {
  console.log(`✓ アカウント単位の手数料 ${rows.length} 行 (${fromDate} 〜)・分けられない SKU なしの取引 0`);
}
process.exit(0);
