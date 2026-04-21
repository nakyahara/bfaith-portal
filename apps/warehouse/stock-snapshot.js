/**
 * 月末在庫スナップショット取り込み
 *
 * ロジザード raw_lz_inventory（全件洗い替え、最新状態のみ保持）を集計して
 * stock_monthly_snapshot に「指定年月の月末在庫」として保存する。
 *
 * 想定呼び出し:
 *   - 毎月末 23:59 頃に captureMonthlyStockSnapshot() を cron 実行（Phase 1 は手動）
 *   - 手動で特定年月に対して再集計したい時は `node stock-snapshot.js 2026-03` 等
 *
 * 注: raw_lz_inventory は日次洗い替えのため、過去月の実値は取れない。
 *     Phase 0/1 では CSV バックフィルを別経路で行い、これとは別に今月分だけ積み増す運用。
 *     （設計書 セクション14 "stock_monthly_snapshot 初期化方針" 参照）
 */
import { getDB, initDB } from './db.js';

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

function currentYearMonth() {
  return new Date().toISOString().slice(0, 7); // YYYY-MM
}

/**
 * raw_lz_inventory を集計して stock_monthly_snapshot に保存。
 * @param {string} yearMonth - 'YYYY-MM' 形式。省略時は今月。
 * @param {string} source - snapshot_source の値。既定 'logizard_auto'
 * @returns {{ ok: boolean, yearMonth?: string, count?: number, reason?: string }}
 */
export function captureMonthlyStockSnapshot(yearMonth = currentYearMonth(), source = 'logizard_auto') {
  // Codex PR2a review Low #3 反映: 月範囲まで検証（"2026-00" や "2026-13" を弾く）
  const m = /^(\d{4})-(\d{2})$/.exec(yearMonth);
  if (!m) throw new Error(`年月形式不正: "${yearMonth}" (期待: YYYY-MM)`);
  const mm = parseInt(m[2], 10);
  if (mm < 1 || mm > 12) throw new Error(`年月月範囲外: "${yearMonth}" (月は01〜12)`);

  const db = getDB();
  const ts = now();

  // raw_lz_inventory は同一商品ID × 複数ロケ/ロットで複数行ある前提、商品コード単位に集計
  const rows = db.prepare(`
    SELECT 商品ID as 商品コード,
           SUM(COALESCE(在庫数, 0)) as 月末在庫数,
           SUM(COALESCE(引当数, 0)) as 月末引当数,
           MAX(synced_at) as captured_at
    FROM raw_lz_inventory
    WHERE 商品ID IS NOT NULL AND TRIM(商品ID) != ''
    GROUP BY 商品ID
  `).all();

  if (rows.length === 0) {
    console.log('[stock-snapshot] raw_lz_inventory にデータなし、スキップ');
    return { ok: false, reason: 'no-data', count: 0 };
  }

  const stmt = db.prepare(`
    INSERT INTO stock_monthly_snapshot
      (年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(年月, 商品コード) DO UPDATE SET
      月末在庫数 = excluded.月末在庫数,
      月末引当数 = excluded.月末引当数,
      snapshot_source = excluded.snapshot_source,
      captured_at = excluded.captured_at,
      updated_at = excluded.updated_at
  `);

  const tx = db.transaction((items) => {
    for (const r of items) {
      stmt.run(yearMonth, r.商品コード, r.月末在庫数, r.月末引当数, source, r.captured_at || ts, ts);
    }
  });
  tx(rows);

  // WAL肥大化防止
  try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

  console.log(`[stock-snapshot] ${yearMonth}: ${rows.length} 商品のスナップショットを保存（source=${source}）`);
  return { ok: true, yearMonth, count: rows.length, source };
}

// ─── 単体実行 ───
const isMain = process.argv[1]?.includes('stock-snapshot');
if (isMain) {
  await initDB();
  const yearMonth = process.argv[2] || currentYearMonth();
  const source = process.argv[3] || 'logizard_auto';
  try {
    const result = captureMonthlyStockSnapshot(yearMonth, source);
    console.log('\n結果:', JSON.stringify(result, null, 2));
    process.exit(result.ok ? 0 : 1);
  } catch (e) {
    console.error('[stock-snapshot] 実行失敗:', e.message);
    process.exit(1);
  }
}
