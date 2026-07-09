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
      // 1) アーカイブ (Codex R1 high: sp log は月100万行超のため .all()+gzipSync は OOM リスク
      //    → iterate() + gzip ストリームで1行ずつ書き出す。temp→rename で書きかけを残さない)
      const outName = `${table}-${month}-${runStamp}.jsonl.gz`;
      const outPath = path.join(ARCHIVE_DIR, outName);
      const gz = zlib.createGzip({ level: 6 });
      const sink = fs.createWriteStream(outPath + '.tmp');
      gz.pipe(sink);
      let archived = 0;
      for (const row of db.prepare(
        `SELECT * FROM "${table}" WHERE substr(ingested_at, 1, 7) = ? AND ingested_at < ?`
      ).iterate(month, cutoff)) {
        if (!gz.write(JSON.stringify(row) + '\n')) {
          await new Promise((r) => gz.once('drain', r));
        }
        archived++;
      }
      await new Promise((resolve, reject) => {
        sink.on('finish', resolve);
        sink.on('error', reject);
        gz.on('error', reject);
        gz.end();
      });
      if (archived === 0) { fs.rmSync(outPath + '.tmp', { force: true }); continue; }
      fs.renameSync(outPath + '.tmp', outPath);

      // 2) アーカイブ成功後に DELETE (5万行ずつのバッチtx = 巨大txによる長ロック/WAL膨張を回避)
      let deleted = 0;
      const delStmt = db.prepare(
        `DELETE FROM "${table}" WHERE id IN (
           SELECT id FROM "${table}" WHERE substr(ingested_at, 1, 7) = ? AND ingested_at < ? LIMIT 50000
         )`
      );
      while (true) {
        const r = delStmt.run(month, cutoff);
        deleted += r.changes;
        if (r.changes < 50000) break;
      }

      totalArchived += archived;
      totalDeleted += deleted;
      console.log(`[rotate-logs] ${table} ${month}: archive=${archived}行 → ${outName}, delete=${deleted}行`);
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
