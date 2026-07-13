#!/usr/bin/env node
/**
 * import-aupay-data.js — au PAYマーケット分析CSV incoming/ 取込 CLI (mall-csv-fetcher P1-A)
 *
 * <DATA_DIR>/incoming/aupay-data/ 直下の *.csv / *.json / *.zip を走査して warehouse.db へ UPSERT。
 * 対象: 店舗分析6種 (売上/参照元/検索/ページ/商品=日次+月次、リピート=月次コホート)
 *       + PM広告日次 + PM検索クエリ週次JSON。種別はヘッダ/JSON kind 自動判別。
 *
 * ⭐手動DLがこのフォルダに置くだけで即取込される。
 * 成功 → processed/YYYY-MM/、失敗 → failed/ へ移動 (削除しない)。sha256+論理日付 冪等。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/import-aupay-data.js [--dry-run]
 *   node apps/warehouse/import-aupay-data.js --data-dir C:\path\to\data
 *
 * exit code: 0=成功 (対象0件・duplicate含む) / 1=いずれか失敗 / 2=env・引数エラー
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { ensureAupayDataTables, importAupayFile, prepareAupayFile } from './aupay-data-lib.js';
import { expandFile } from './rakuten-ads-rpp-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const isDryRun = args.includes('--dry-run');

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required (env or --data-dir)'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const incomingDir = path.join(DATA_DIR, 'incoming', 'aupay-data');
const processedRoot = path.join(incomingDir, 'processed');
const failedDir = path.join(incomingDir, 'failed');
for (const d of [incomingDir, processedRoot, failedDir]) fs.mkdirSync(d, { recursive: true });

function moveTo(srcPath, destDir) {
  fs.mkdirSync(destDir, { recursive: true });
  const base = path.basename(srcPath);
  let dest = path.join(destDir, base);
  if (fs.existsSync(dest)) {
    const ts = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
    const ext = path.extname(base);
    dest = path.join(destDir, `${path.basename(base, ext)}-${ts}${ext}`);
  }
  fs.renameSync(srcPath, dest);
  return dest;
}

// mtime昇順で処理: scope置換 (DELETE→INSERT) のため「後に置かれたファイル=最新」が勝つ。
// ファイル名昇順だと手動DL等の任意名が新しい取得版の後に処理され巻き戻る (Codex R2 Medium)
const entries = fs.readdirSync(incomingDir, { withFileTypes: true })
  .filter(e => e.isFile() && /\.(csv|json|zip)$/i.test(e.name))
  .map(e => {
    let mtime = 0;
    try { mtime = fs.statSync(path.join(incomingDir, e.name)).mtimeMs; } catch { /* 直後に読込で拾う */ }
    return { name: e.name, mtime };
  })
  .sort((a, b) => (a.mtime - b.mtime) || a.name.localeCompare(b.name))
  .map(e => e.name);

console.log(`=== au PAY分析CSV取込 (incoming: ${incomingDir}) ===`);
console.log(`対象: ${entries.length} ファイル${isDryRun ? ' [DRY RUN]' : ''}`);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
ensureAupayDataTables(db);

if (entries.length === 0) {
  console.log('取込対象なし (正常終了)');
  db.close();
  process.exit(0);
}

let okCount = 0, dupCount = 0, failCount = 0;
try {
  for (const name of entries) {
    const srcPath = path.join(incomingDir, name);
    let buffer;
    try {
      buffer = fs.readFileSync(srcPath);
    } catch (e) {
      console.error(`✗ ${name}: 読込失敗 (${e.message})`);
      failCount++;
      continue;
    }
    const sha256 = crypto.createHash('sha256').update(buffer).digest('hex');

    if (isDryRun) {
      const g = expandFile(name, buffer);
      if (g.error) { console.log(`  [dry] ✗ ${name}: ${g.error}`); failCount++; continue; }
      let allOk = true;
      for (const f of g.files) {
        const p = prepareAupayFile(f.name, f.buffer);
        if (p.ok) console.log(`  [dry] ✓ ${f.name}: ${p.label} ${p.records.length}件 (${p.dateFrom}〜${p.dateTo})`);
        else { console.log(`  [dry] ✗ ${f.name}: ${p.error}`); allOk = false; }
      }
      if (allOk) okCount++; else failCount++;
      continue;
    }

    const outcome = importAupayFile(db, { name, buffer, sha256, source: 'incoming' });
    for (const r of outcome.results) {
      if (r.ok) {
        console.log(`  ✓ ${r.file}: ${r.label} ${r.rows}件 (insert ${r.inserted} / update ${r.updated}) ${r.date_from}〜${r.date_to}`);
        for (const w of r.warnings || []) console.log(`    ⚠ ${w}`);
      } else console.log(`  ${r.duplicate ? '↷' : '✗'} ${r.file}: ${r.error}`);
    }

    try {
      if (outcome.status === 'ok' || outcome.status === 'duplicate') {
        const ym = new Date().toISOString().slice(0, 7);
        const dest = moveTo(srcPath, path.join(processedRoot, ym));
        console.log(`    → ${path.relative(incomingDir, dest)}`);
        if (outcome.status === 'ok') okCount++; else dupCount++;
      } else {
        const dest = moveTo(srcPath, failedDir);
        console.log(`    → ${path.relative(incomingDir, dest)} (要確認)`);
        failCount++;
      }
    } catch (e) {
      console.error(`  ⚠ ${name}: ファイル移動失敗 (${e.message})。手で移動してください`);
      if (outcome.status === 'ok' || outcome.status === 'duplicate') okCount++; else failCount++;
    }
  }
} finally {
  db.close();
}

console.log(`\n=== summary: ok=${okCount} duplicate=${dupCount} failed=${failCount} ===`);
process.exit(failCount > 0 ? 1 : 0);
