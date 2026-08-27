#!/usr/bin/env node
/**
 * import-yahoo-review.js — Yahoo 商品レビュー ZIP/CSV incoming/ 取込 CLI (P2-Y PR-Y-A)
 *
 * <DATA_DIR>/incoming/yahoo-review/ 直下の *.zip / *.csv を走査して warehouse.db へ UPSERT
 * (fact_yahoo_reviews、identity/revision 方式)。成功 → processed/YYYY-MM/、失敗 → failed/。sha256 冪等。
 * 低評価 (★1-2 の初回観測・★3以上→★2以下の遷移) と identity 衝突は GChat 通知 (キュー方式、fail-soft)。
 * 削除検知はファイル名に窓マーカー `_d<from>_<to>_` があるもの (= downloader が全量検証済み) だけ。
 *
 * 実行: node apps/warehouse/import-yahoo-review.js [--data-dir <dir>] [--dry-run]
 * env: DATA_DIR / GCHAT_WEBHOOK_MALL_FETCH (or GCHAT_WEBHOOK) / NOTIFY_LOW_REVIEW=0 で通知抑止
 * exit code: 0=成功 (対象0件・duplicate含む) / 1=いずれか失敗 / 2=env・引数エラー
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { ensureYahooReviewTables, importYahooReviewFile, prepareYahooReviewFile } from './yahoo-review-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const isDryRun = args.includes('--dry-run');
if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required (env or --data-dir)'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const incomingDir = path.join(DATA_DIR, 'incoming', 'yahoo-review');
const processedRoot = path.join(incomingDir, 'processed');
const failedDir = path.join(incomingDir, 'failed');
for (const d of [incomingDir, processedRoot, failedDir]) fs.mkdirSync(d, { recursive: true });

function moveTo(srcPath, destDir) {
  fs.mkdirSync(destDir, { recursive: true });
  let dest = path.join(destDir, path.basename(srcPath));
  if (fs.existsSync(dest)) {
    const ext = path.extname(dest); const base = path.basename(dest, ext);
    dest = path.join(destDir, `${base}_${Date.now()}${ext}`);
  }
  fs.renameSync(srcPath, dest);
  return dest;
}

/** 低評価・衝突の通知 (キュー方式。本文・注文IDは載せない) */
async function notifyLowRatings(db) {
  const queued = db.prepare(`SELECT review_identity, kind, item_name, product_code, rating, date_jst FROM yahoo_review_low_notify_queue ORDER BY queued_at, review_identity`).all();
  if (queued.length === 0) return;
  if (process.env.NOTIFY_LOW_REVIEW === '0') { console.log(`  (低評価通知は NOTIFY_LOW_REVIEW=0 のためスキップ: ${queued.length}件はキューに残置)`); return; }
  const webhook = (process.env.GCHAT_WEBHOOK_MALL_FETCH || process.env.GCHAT_WEBHOOK || '').trim();
  if (!webhook) { console.log(`  ⚠ Yahoo 低評価レビュー ${queued.length}件 (GCHAT_WEBHOOK 未設定。キューに残置し次回リトライ)`); return; }
  const low = queued.filter((q) => q.kind !== 'conflict');
  const conflicts = queued.filter((q) => q.kind === 'conflict');
  const lines = [];
  if (low.length) {
    lines.push(`🔻 *Yahoo!ショッピング 低評価レビュー検知 (${low.length}件)*`);
    for (const r of low.slice(0, 10)) lines.push(`・${'★'.repeat(r.rating)} ${(r.item_name || r.product_code || '').slice(0, 40)} (${r.date_jst}${r.kind === 'transition' ? '、★3以上から変更' : ''})`);
    if (low.length > 10) lines.push(`…ほか ${low.length - 10} 件`);
    lines.push('ストクリ > 評価 > 商品レビューチェックツール で確認・返信');
  }
  if (conflicts.length) {
    lines.push(`⚠️ *Yahoo レビュー identity 衝突 ${conflicts.length}件* (同一注文×商品に内容の違うレビューが複数。取込・配信対象から外しました=fail-closed。設計書 §Y1 のスキーマ拡張が必要か確認)`);
  }
  try {
    const res = await fetch(webhook, { method: 'POST', headers: { 'Content-Type': 'application/json; charset=UTF-8' }, body: JSON.stringify({ text: lines.join('\n') }), signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const del = db.prepare(`DELETE FROM yahoo_review_low_notify_queue WHERE review_identity = ?`);
    const tx = db.transaction(() => { for (const r of queued) del.run(r.review_identity); });
    tx();
    console.log(`  📣 Yahoo 低評価/衝突 ${queued.length}件 → GChat通知 (${res.status})、キュー消化`);
  } catch (e) {
    console.log(`  ⚠ GChat通知失敗 (キューに残置、次回リトライ): ${e.message}`);
  }
}

const entries = fs.readdirSync(incomingDir, { withFileTypes: true })
  .filter((e) => e.isFile() && /\.(zip|csv)$/i.test(e.name)).map((e) => e.name).sort();
console.log(`=== Yahoo レビュー取込 (incoming: ${incomingDir}) ===`);
console.log(`対象: ${entries.length} ファイル${isDryRun ? ' [DRY RUN]' : ''}`);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
ensureYahooReviewTables(db);

// 終了は exitCode + 自然終了 (GChat fetch 直後の process.exit() は libuv abort を踏む — 楽天版 #614 の教訓)
if (entries.length === 0) {
  console.log('取込対象なし (正常終了)');
  if (!isDryRun) await notifyLowRatings(db);
  db.close();
  process.exitCode = 0;
} else {
  let okCount = 0, dupCount = 0, failCount = 0, newLowCount = 0;
  try {
    for (const name of entries) {
      const srcPath = path.join(incomingDir, name);
      let buffer;
      try { buffer = fs.readFileSync(srcPath); } catch (e) { console.error(`✗ ${name}: 読込失敗 (${e.message})`); failCount++; continue; }
      const sha256 = crypto.createHash('sha256').update(buffer).digest('hex');
      if (isDryRun) {
        const p = prepareYahooReviewFile(name, buffer);
        if (p.ok) { console.log(`  [dry] ✓ ${name}: ${p.label} (${p.dateFrom}〜${p.dateTo})`); okCount++; }
        else { console.log(`  [dry] ✗ ${name}: ${p.error}`); failCount++; }
        continue;
      }
      const outcome = importYahooReviewFile(db, { name, buffer, sha256, source: 'incoming' });
      for (const r of outcome.results) {
        if (r.ok) {
          const delNote = (r.missed || r.deleted) ? ` / 不在+1 ${r.missed} / 削除確定 ${r.deleted}` : '';
          console.log(`  ✓ ${r.file}: ${r.label} (insert ${r.inserted} / update ${r.updated} / 変化なし ${r.unchanged}${delNote}) ${r.date_from}〜${r.date_to}`);
          for (const w of r.warnings || []) console.log(`    ⚠ ${w}`);
        } else {
          console.log(`  ${r.duplicate ? '↷' : '✗'} ${r.file}: ${r.error}`);
        }
      }
      newLowCount += outcome.newLowRatings.length;
      try {
        if (outcome.status === 'ok' || outcome.status === 'duplicate') {
          const dest = moveTo(srcPath, path.join(processedRoot, new Date().toISOString().slice(0, 7)));
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
    if (!isDryRun) await notifyLowRatings(db);
  } finally {
    db.close();
  }
  console.log(`\n=== summary: ok=${okCount} duplicate=${dupCount} failed=${failCount} 新規低評価=${newLowCount} ===`);
  process.exitCode = failCount > 0 ? 1 : 0;
}
