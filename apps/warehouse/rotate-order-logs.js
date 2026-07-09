/**
 * rotate-order-logs.js — raw_*_orders_log 3本の保持期間ローテーション (監査PR-12(b))
 *
 * 背景 (2026-07-09 実測): raw_sp_orders_log 3.53M行/1.3GB、raw_rakuten_orders_log
 * 725K行/469MB、raw_yahoo_orders_log 276K行/80MB (index込み計約2.3GB)。
 * 書込のみで読み手ゼロ・DELETE経路ゼロの純無限成長 = warehouse.db.bak 150GB事故と同じ轍。
 *
 * 方式:
 *   1. ingested_at < (今日 - RETENTION日) の行を月単位で data/archive/order-logs/ に
 *      jsonl.gz エクスポート (ファイル名に実行日を含め、再実行でも上書きしない)
 *   2. アーカイブ成功した月の行だけ DELETE (月単位tx = 途中失敗しても月単位で整合)
 *   3. 最後に wal_checkpoint(TRUNCATE) で WAL を回収
 *
 * 注意: SQLite は DELETE してもファイルは縮まない (空きページは再利用される)。
 * VACUUM は 35GB DB では2倍のディスクと長時間ロックが要るため行わない (成長が止まれば十分)。
 *
 * 実行: node apps/warehouse/rotate-order-logs.js  (daily-sync から毎朝、手動も可)
 * env: RAW_ORDER_LOG_RETENTION_DAYS (既定 60)
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { initDB, getDB } from './db.js';

const RETENTION_DAYS = Math.max(parseInt(process.env.RAW_ORDER_LOG_RETENTION_DAYS, 10) || 60, 14);
const TABLES = ['raw_sp_orders_log', 'raw_rakuten_orders_log', 'raw_yahoo_orders_log'];
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const ARCHIVE_DIR = path.join(DATA_DIR, 'archive', 'order-logs');

// ingested_at は 'YYYY-MM-DD HH:MM:SS' (UTC、nowSql形式)。同形式で cutoff を作る
const cutoff = new Date(Date.now() - RETENTION_DAYS * 86400000)
  .toISOString().replace('T', ' ').slice(0, 19);
const runStamp = new Date().toISOString().replace(/[:.]/g, '').slice(0, 15);

await initDB();
const db = getDB();
fs.mkdirSync(ARCHIVE_DIR, { recursive: true });

console.log(`[rotate-logs] 保持=${RETENTION_DAYS}日, cutoff=${cutoff}`);
let totalArchived = 0, totalDeleted = 0, hadError = false;

for (const table of TABLES) {
  const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table);
  if (!exists) { console.log(`[rotate-logs] ${table}: 不存在 skip`); continue; }

  const months = db.prepare(
    `SELECT DISTINCT substr(ingested_at, 1, 7) m FROM "${table}" WHERE ingested_at < ? ORDER BY m`
  ).all(cutoff).map(r => r.m);
  if (months.length === 0) { console.log(`[rotate-logs] ${table}: 対象なし`); continue; }

  for (const month of months) {
    try {
      const rows = db.prepare(
        `SELECT * FROM "${table}" WHERE substr(ingested_at, 1, 7) = ? AND ingested_at < ?`
      ).all(month, cutoff);
      if (rows.length === 0) continue;

      // 1) アーカイブ (temp→rename で書きかけファイルを残さない)
      const outName = `${table}-${month}-${runStamp}.jsonl.gz`;
      const outPath = path.join(ARCHIVE_DIR, outName);
      const jsonl = rows.map(r => JSON.stringify(r)).join('\n') + '\n';
      fs.writeFileSync(outPath + '.tmp', zlib.gzipSync(jsonl, { level: 6 }));
      fs.renameSync(outPath + '.tmp', outPath);

      // 2) アーカイブ成功後に同条件で DELETE (月単位tx)
      const del = db.prepare(
        `DELETE FROM "${table}" WHERE substr(ingested_at, 1, 7) = ? AND ingested_at < ?`
      ).run(month, cutoff);

      totalArchived += rows.length;
      totalDeleted += del.changes;
      console.log(`[rotate-logs] ${table} ${month}: archive=${rows.length}行 → ${outName}, delete=${del.changes}行`);
    } catch (e) {
      // 月単位で独立: 1月分の失敗は他の月・他のテーブルを止めない (無音にはしない)
      hadError = true;
      console.error(`[rotate-logs] ERROR ${table} ${month}: ${e.message}`);
    }
  }
}

try {
  const cp = db.pragma('wal_checkpoint(TRUNCATE)');
  console.log(`[rotate-logs] wal_checkpoint: ${JSON.stringify(cp)}`);
} catch (e) {
  console.warn(`[rotate-logs] wal_checkpoint失敗 (無害): ${e.message}`);
}

console.log(`[rotate-logs] 完了: archive=${totalArchived}行, delete=${totalDeleted}行, error=${hadError ? 'あり' : 'なし'}`);
if (hadError) process.exitCode = 1;
