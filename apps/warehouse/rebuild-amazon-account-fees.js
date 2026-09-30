#!/usr/bin/env node
/**
 * rebuild-amazon-account-fees.js — Amazon アカウント単位フィー月次 fact (amazon-dashboard PR-C)
 *
 * settlement raw のうち SKU に紐付かないアカウント単位フィー (月次保管料 / 長期在庫追加手数料 /
 * 返送・廃棄 / 納品不備 / 低在庫手数料 / 月額登録料) を月次×fee_type で集計する。
 * これらは f_amazon_finance_sku_daily_v1 (SKU 粒度) に載らないため、SKU 別利益の合計と
 * アカウント全体の実利益の差分になる (2026-07-06 実測: 月 70〜80 万円規模)。
 * 🆕 2026-09-30 (D-63): 納品不備 (Inbound Defect Fee…) は SKU の付いた行も入れる (日次の財務からは外した = 二重にしない。amazon-account-fee-rules.js の SKU_ACCOUNT_FEE_TYPES)。
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
import { FEE_TYPE_RULES, NOT_ACCOUNT_FEE, CONFIRMED_NAMES, SKU_ACCOUNT_FEE_TYPES } from './amazon-account-fee-rules.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthsBack = Math.min(Math.max(parseInt(getArg('--months'), 10) || 14, 1), 60);

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

// JST 今日から monthsBack ヶ月前の月初。--from-month YYYY-MM があればその月から (daily-sync が月の手数料のやり残しまでさかのぼるとき・2026-09-29 F2b-2)
const fromMonthArg = getArg('--from-month');
if (fromMonthArg != null && !/^\d{4}-(0[1-9]|1[0-2])$/.test(fromMonthArg)) { console.error(`FATAL: --from-month は YYYY-MM: ${fromMonthArg}`); process.exit(2); }
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const fromMonth = fromMonthArg ? new Date(`${fromMonthArg}-01T00:00:00Z`) : new Date(Date.UTC(nowJst.getUTCFullYear(), nowJst.getUTCMonth() - (monthsBack - 1), 1));
const fromDate = fromMonth.toISOString().slice(0, 10);

const db = new Database(dbPath);
// 待ち時間は db.js と同じ決め (daily-sync は WAREHOUSE_DB_BUSY_TIMEOUT_MS=60000 を渡す・無ければ / 不正なら 5 秒。Codex #1526 R1)
const busyEnv = Number(process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS);
db.pragma(`busy_timeout = ${process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS && Number.isInteger(busyEnv) && busyEnv >= 0 ? busyEnv : 5000}`);
// 🚨 2026-09-29: SKU なしの行だけの索引 (決済の行 約 440 万行のうち SKU なしはごく一部)。
//   索引が無いと下の 3 つの問い合わせが毎回ほぼ全行 (14 か月) をなめ、朝の daily-sync の制限時間 300 秒を超えて止まった
//   (9/29 朝 ETIMEDOUT = Render への送信も飛んだ。9/28 夜の手動の作り直しでも 297 秒)。
//   問い合わせは INDEXED BY でこの索引を使う (使えない形に変わったら黙って遅くならずにエラーで止まる)。
//   初回だけ索引を作る時間がかかる (IF NOT EXISTS)
db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_nosku_econ ON raw_amazon_settlement_lines(economic_date)
  WHERE seller_sku_normalized IS NULL OR seller_sku_normalized = ''`);

db.exec(`CREATE TABLE IF NOT EXISTS f_amazon_account_fees_monthly_v1 (
  month_start_jst TEXT NOT NULL CHECK(month_start_jst GLOB '????-??-01'),
  fee_type        TEXT NOT NULL,
  amount_jpy      REAL NOT NULL DEFAULT 0,   -- Amazon 符号のまま (負 = 費用)
  row_count       INTEGER NOT NULL DEFAULT 0,
  built_at        TEXT NOT NULL,
  PRIMARY KEY (month_start_jst, fee_type)
)`);

// 手数料の分け方 (FEE_TYPE_RULES / NOT_ACCOUNT_FEE / CONFIRMED_NAMES) は amazon-account-fee-rules.js (1 か所。Company DB へ財務を送る送り手も同じものを使う。2026-09-29 F2b-2)
const q = (x) => `'${String(x).replace(/'/g, "''")}'`;
const likePrefix = (p) => `transaction_type LIKE ${q(String(p).replace(/[\\%_]/g, (c) => '\\' + c) + '%')} ESCAPE '\\'`;   // % と _ はその文字として
const matchSql = (exact, prefix) => [
  ...(exact.length ? [`transaction_type IN (${exact.map(q).join(', ')})`] : []),
  ...prefix.map(likePrefix),
].join(' OR ');
const LOW_INV_SQL = `transaction_type LIKE '%LowInventory%' OR transaction_type LIKE '%Low-Inventory%'`;
const FEE_FILTER_SQL = [...FEE_TYPE_RULES.filter(([t]) => t !== 'low_inventory').map(([, e, p]) => matchSql(e, p)).filter(Boolean), LOW_INV_SQL].map((x) => `(${x})`).join(' OR ');
const CONFIRMED_SQL = `(transaction_type IN (${CONFIRMED_NAMES.map(q).join(', ')}) OR ${likePrefix('Inbound Defect Fee')} OR ${LOW_INV_SQL})`;
const FEE_CASE_SQL = `CASE ${FEE_TYPE_RULES.map(([t, e, p]) => `WHEN ${t === 'low_inventory' ? LOW_INV_SQL : matchSql(e, p)} THEN '${t}'`).join(' ')} ELSE 'other_account_fee' END`;
// 🆕 2026-09-30 (D-63): SKU の付いた行でも月の手数料に入れる種類 (納品不備だけ = SKU_ACCOUNT_FEE_TYPES)。日次の財務の build は同じ行を silver から外す (二重にしない)
//   SKU の有無の決め = 日次の財務の silver と同じ (NOT NULL かつ TRIM <> '')。空白だけの SKU はどちらにも入らない (送り手も整形できないで止める)
//   索引 = その行だけの部分索引 (決済の行 約 440 万行のうち数十行)。問い合わせの WHERE を索引の WHERE と同じ文字にする (INDEXED BY = 使えなければ黙って遅くならずにエラー)
//   🚨 SKU_ACCOUNT_FEE_TYPES か納品不備の名前の決めを変えたら、索引の名前も変える (IF NOT EXISTS は古い定義のまま残る → INDEXED BY がエラーで止まって気づく)
const SKU_FEE_SQL = `TRIM(seller_sku_normalized) <> '' AND (${FEE_TYPE_RULES.filter(([t]) => SKU_ACCOUNT_FEE_TYPES.includes(t)).map(([, e, p]) => matchSql(e, p)).join(' OR ')})`;
db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_skufee_econ ON raw_amazon_settlement_lines(economic_date) WHERE ${SKU_FEE_SQL}`);
const builtAt = new Date().toISOString();
const result = db.transaction(() => {
  db.prepare(`DELETE FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst >= ?`).run(fromDate);
  const info = db.prepare(`
    INSERT INTO f_amazon_account_fees_monthly_v1 (month_start_jst, fee_type, amount_jpy, row_count, built_at)
    WITH src AS (
      -- SKU 無し行 (手数料の分け方に当たる行)
      SELECT source_settlement_id, business_line_key, source_document_id, source_line_no, source_layer, ingested_at,
        economic_date, transaction_type, other_amount_micro, item_related_fee_amount_micro
      FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_nosku_econ
      WHERE economic_date >= ?
        AND (seller_sku_normalized IS NULL OR seller_sku_normalized = '')
        AND (${FEE_FILTER_SQL})
      UNION ALL
      -- SKU 付き行のうち月の手数料に入れる種類 (納品不備・2026-09-30 D-63)。ほかの SKU 付き行 (保管料・調整など) は
      -- 日次の財務の側 = ここに入れると二重になる。納品不備は日次の財務の silver から外している
      SELECT source_settlement_id, business_line_key, source_document_id, source_line_no, source_layer, ingested_at,
        economic_date, transaction_type, other_amount_micro, item_related_fee_amount_micro
      FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_skufee_econ
      WHERE economic_date >= ?
        AND ${SKU_FEE_SQL}
    ),
    occ AS (
      -- 同一 settlement が sp_api_v1 / manual_csv の両 layer で raw に存在し得るため、
      -- SKU別 mart (rebuild-amazon-settlement-mart.js) と同じ business dedup を挟む
      -- (Codex High 指摘: dedup 無しだと保管料/LTSF が二重計上)
      -- 🚨 同じ文書の中の出現順 (occ) を鍵に足す (本物の同じ鍵の別々の行を潰さない。db.js の v_amazon_settlement_unified と同じ形。2026-09-28)
      --   business_line_key は SKU と取引の種類を含む = SKU 無しの行と SKU 付きの行が同じ鍵になることはない (合わせてから数えても別々に数えても同じ)
      SELECT source_settlement_id, business_line_key, source_document_id, source_layer, ingested_at,
        economic_date, transaction_type, other_amount_micro, item_related_fee_amount_micro,
        DENSE_RANK() OVER (PARTITION BY source_settlement_id, business_line_key, source_document_id ORDER BY source_line_no) AS occ
      FROM src
    ),
    dedup AS (
      SELECT economic_date, transaction_type, other_amount_micro, item_related_fee_amount_micro,
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
      -- 金額は other-amount と item-related-fee-amount の両方 (Easy Ship は月で列が違う。ほかの手数料は other-amount だけ)
      SUM(COALESCE(other_amount_micro, 0) + COALESCE(item_related_fee_amount_micro, 0)) / 1000000.0 AS amount_jpy,
      COUNT(*) AS row_count,
      ? AS built_at
    FROM dedup
    WHERE rn = 1
    GROUP BY 1, 2
  `).run(fromDate, fromDate, builtAt);
  return info.changes;
})();

// ⚠️ ① 前方一致で手数料に入れたが確かめていない名前 (金額は入っている。人が確かめて CONFIRMED_NAMES に足す)
const unconfirmedTx = db.prepare(`
  SELECT transaction_type t, ${FEE_CASE_SQL} f, COUNT(*) n, SUM(COALESCE(other_amount_micro, 0) + COALESCE(item_related_fee_amount_micro, 0)) / 1000000.0 a, GROUP_CONCAT(DISTINCT substr(economic_date, 1, 7)) ms
    FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_nosku_econ
   WHERE economic_date >= ? AND (seller_sku_normalized IS NULL OR seller_sku_normalized = '') AND (${FEE_FILTER_SQL}) AND NOT ${CONFIRMED_SQL}
   GROUP BY 1 ORDER BY 1`).all(fromDate);
// ⚠️ ② 分けられない SKU なしの取引 (手数料の分け方にも、入れない一覧にも無い名前) = 名前が変わった手数料の疑い (金額は入らない)
const unknownTx = db.prepare(`
  SELECT transaction_type t, COUNT(*) n, SUM(COALESCE(other_amount_micro, 0) + COALESCE(item_related_fee_amount_micro, 0) + COALESCE(price_amount_micro, 0)) / 1000000.0 a,
         GROUP_CONCAT(DISTINCT substr(economic_date, 1, 7)) ms
    FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_nosku_econ
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
// 最後の行 (daily-sync が朝の通知に載せる)。金額は raw の延べ (重複をまとめる前)
const warns = [];
if (unknownTx.length) warns.push(`分けられない SKU なしの取引 ${unknownTx.length} 種類 (集計に入っていない): ${unknownTx.map((u) => `${u.t} (延べ ${u.n} 行・¥${Math.round(u.a).toLocaleString()}・${u.ms})`).join(' / ')} → FEE_TYPE_RULES か NOT_ACCOUNT_FEE に足す`);
if (unconfirmedTx.length) warns.push(`前方一致で入れた未確認の名前 ${unconfirmedTx.length} 種類 (集計に入っている): ${unconfirmedTx.map((u) => `${u.t} → ${u.f} (延べ ${u.n} 行・¥${Math.round(u.a).toLocaleString()}・${u.ms})`).join(' / ')} → 確かめて CONFIRMED_NAMES に足す`);
if (warns.length) console.log(`⚠️ アカウント単位の手数料: ${warns.join(' ／ ')} (rebuild-amazon-account-fees.js)`);
else console.log(`✓ アカウント単位の手数料 ${rows.length} 行 (${fromDate} 〜)・分けられない SKU なしの取引 0・未確認の名前 0`);
process.exit(0);
