/**
 * snapshot-ne-stock.js — NE 自社在庫の日次スナップショット
 *
 * raw_ne_products が NE API で毎朝上書きされるため、上書き直後にこのスクリプトを呼んで
 * その日の在庫状態を ne_stock_daily_snapshot に履歴化する。
 *
 * daily-sync.js から「NE API 取得成功直後」に呼ばれる前提。
 * 既存日付分があれば INSERT OR REPLACE で上書きする (再走対応)。
 *
 * business_date 解決順:
 *   1. process.env.WAREHOUSE_BUSINESS_DATE (daily-sync が JST で確定して渡す)
 *   2. CLI 引数 (--date=YYYY-MM-DD)
 *   3. 実行時刻を JST に変換した日付 (単独実行のフォールバック)
 *
 * 単独実行も可: node apps/warehouse/snapshot-ne-stock.js [--date=YYYY-MM-DD]
 */
import { initDB, getDB } from './db.js';

/** Date を JST (UTC+9) の YYYY-MM-DD に変換 */
function toJstDate(d) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function resolveBusinessDate() {
  if (process.env.WAREHOUSE_BUSINESS_DATE) return process.env.WAREHOUSE_BUSINESS_DATE;
  const cliArg = process.argv.slice(2).find(a => a.startsWith('--date='));
  if (cliArg) return cliArg.split('=')[1];
  return toJstDate(new Date());
}

export function snapshotNeStock(businessDate) {
  if (!businessDate || !/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) {
    throw new Error(`business_date が不正: ${businessDate}`);
  }
  const db = getDB();

  // raw_ne_products → ne_stock_daily_snapshot へ全件複製
  // 在庫数 NULL は除外 (カラムは NOT NULL)
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO ne_stock_daily_snapshot (business_date, 商品コード, 在庫数, captured_at)
    SELECT ?, 商品コード, 在庫数, strftime('%Y-%m-%dT%H:%M:%fZ','now')
    FROM raw_ne_products
    WHERE 在庫数 IS NOT NULL
  `);

  const t0 = Date.now();
  const result = db.transaction(() => stmt.run(businessDate))();
  const elapsed = Date.now() - t0;

  return { businessDate, copiedRows: result.changes, elapsedMs: elapsed };
}

// ─── CLI ───
const isMain = process.argv[1]?.endsWith('snapshot-ne-stock.js');
if (isMain) {
  await initDB();
  const businessDate = resolveBusinessDate();
  console.log(`[ne-stock-snapshot] business_date=${businessDate} 開始`);
  const result = snapshotNeStock(businessDate);
  console.log(`[ne-stock-snapshot] ✅ 完了: ${result.copiedRows}件 (${result.elapsedMs}ms)`);
  process.exit(0);
}
