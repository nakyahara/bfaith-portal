#!/usr/bin/env node
/**
 * import-qoo10-data.js — Qoo10 Analytics xlsx incoming/ 取込 CLI (mall-csv-fetcher P1-Q R1)
 *
 * <DATA_DIR>/incoming/qoo10-data/ 直下の *.xlsx を走査して warehouse.db へ scope置換取込。
 * 対象: 店舗CVR (チャネル52列+KPI) / 日付・商品別トラフィック。種別はヘッダ自動判別。
 * mtime昇順 (後に置かれた最新版が勝つ)。成功→processed/YYYY-MM/、失敗→failed/。
 *
 * 使い方: DATA_DIR=... node apps/warehouse/import-qoo10-data.js [--dry-run]
 * exit code: 0=成功 (対象0件・duplicate含む) / 1=いずれか失敗 / 2=env・引数エラー
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { ensureQoo10DataTables, importQoo10File, prepareQoo10File } from './qoo10-data-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const isDryRun = args.includes('--dry-run');

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required (env or --data-dir)'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const incomingDir = path.join(DATA_DIR, 'incoming', 'qoo10-data');
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

const entries = fs.readdirSync(incomingDir, { withFileTypes: true })
  .filter(e => e.isFile() && /\.xlsx$/i.test(e.name))
  .map(e => {
    let mtime = 0;
    try { mtime = fs.statSync(path.join(incomingDir, e.name)).mtimeMs; } catch { /* 読込時に拾う */ }
    return { name: e.name, mtime };
  })
  .sort((a, b) => (a.mtime - b.mtime) || a.name.localeCompare(b.name))
  .map(e => e.name);

console.log(`=== Qoo10 Analytics取込 (incoming: ${incomingDir}) ===`);
console.log(`対象: ${entries.length} ファイル${isDryRun ? ' [DRY RUN]' : ''}`);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
ensureQoo10DataTables(db);

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
      const p = await prepareQoo10File(name, buffer);
      if (p.ok) { console.log(`  [dry] ✓ ${name}: ${p.label} (${p.dateFrom}〜${p.dateTo})`); okCount++; }
      else { console.log(`  [dry] ✗ ${name}: ${p.error}`); failCount++; }
      continue;
    }

    const outcome = await importQoo10File(db, { name, buffer, sha256, source: 'incoming' });
    for (const r of outcome.results) {
      if (r.ok) {
        console.log(`  ✓ ${r.file}: ${r.label} → ${r.detail} (${r.date_from}〜${r.date_to})`);
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
