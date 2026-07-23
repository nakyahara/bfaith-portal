#!/usr/bin/env node
/**
 * import-rakuten-review.js — RMSレビューCSV incoming/ 取込 CLI (mall-csv-fetcher P2 PR-A)
 *
 * <DATA_DIR>/incoming/rakuten-review/ 直下の *.csv を走査して warehouse.db へ UPSERT。
 * 成功 → processed/YYYY-MM/、失敗 → failed/ へ移動。sha256 冪等。
 *
 * ★低評価通知: 取込で「新規に現れた ★2以下」を GChat に通知する (GCHAT_WEBHOOK、fail-soft)。
 *   レビュー本文は通知に載せない (PII 最小化 — 設計書§4 C1)。NOTIFY_LOW_REVIEW=0 で無効化。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/import-rakuten-review.js [--dry-run]
 *
 * exit code: 0=成功 (対象0件・duplicate含む) / 1=いずれか失敗 / 2=env・引数エラー
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { ensureRakutenReviewTables, importReviewFile, prepareReviewFile } from './rakuten-review-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const isDryRun = args.includes('--dry-run');

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required (env or --data-dir)'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const incomingDir = path.join(DATA_DIR, 'incoming', 'rakuten-review');
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

/** 低評価GChat通知。タイトル・本文を載せる (2026-07-20 中原さん要望。レビューは楽天上で
 *  公開済みの内容のため社内通知に出してよい)。注文番号は引き続き載せない (PII最小化)。
 *  キュー方式: 新規★1-2は rakuten_review_low_notify_queue に積み、送信2xxで削除。
 *  送信失敗時はキューに残る → 次回実行 (翌朝daily-sync) で自動リトライ (Codex R2 High:
 *  取込duplicate化で通知が恒久欠落するのを防ぐ)。通知失敗で取込は失敗にしない (fail-soft) */
async function notifyLowRatings(dbRw) {
  // 移行前に積まれた旧キュー行 (title/body=NULL) は fact から補完 (Codex Medium)
  const queued = dbRw.prepare(`
    SELECT q.review_url, q.review_type, q.item_name, q.rating, q.posted_at,
           COALESCE(q.title, f.title) AS title, COALESCE(q.body, f.body) AS body
      FROM rakuten_review_low_notify_queue q
      LEFT JOIN fact_rakuten_reviews f ON f.review_url = q.review_url
     ORDER BY q.posted_at
  `).all();
  if (queued.length === 0) return;
  if ((process.env.NOTIFY_LOW_REVIEW || '1') === '0') {
    console.log(`  (低評価通知は NOTIFY_LOW_REVIEW=0 のためスキップ: ${queued.length}件はキューに残置)`);
    return;
  }
  const webhook = (process.env.GCHAT_WEBHOOK_MALL_FETCH || process.env.GCHAT_WEBHOOK || '').trim();
  if (!webhook) {
    console.log(`  ⚠ 低評価レビュー ${queued.length}件 (GCHAT_WEBHOOK 未設定。キューに残置し次回リトライ)`);
    return;
  }
  const lines = queued.slice(0, 10).map(r => {
    const stars = '★'.repeat(r.rating) + '☆'.repeat(5 - r.rating);
    const what = r.review_type === 'shop' ? 'ショップレビュー' : (r.item_name || '(商品名不明)').slice(0, 40);
    // レビュー内容 (タイトル・本文とも改行は畳み、結合後300字で切る。GChatの読みやすさ優先)
    const title = (r.title || '').replace(/\s+/g, ' ').trim();
    const body = (r.body || '').replace(/\s+/g, ' ').trim();
    const content = [title && `「${title}」`, body].filter(Boolean).join(' ');
    const contentLine = content
      ? `\n    ${content.length > 300 ? `${content.slice(0, 300)}…` : content}`
      : '';
    return `・${stars} ${what}\n    投稿: ${r.posted_at}${contentLine}`;
  });
  if (queued.length > 10) lines.push(`…ほか ${queued.length - 10} 件`);
  const text = [
    `🔻 *楽天 低評価レビュー検知 (${queued.length}件)*`,
    ...lines,
    'レビューチェックツール: https://review.rms.rakuten.co.jp/ (RMS>コミュニティ>みんなのレビュー)',
  ].join('\n');
  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=UTF-8' },
      body: JSON.stringify({ text }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const del = dbRw.prepare(`DELETE FROM rakuten_review_low_notify_queue WHERE review_url = ?`);
    const tx = dbRw.transaction(() => { for (const r of queued) del.run(r.review_url); });
    tx();
    console.log(`  📣 低評価レビュー ${queued.length}件 → GChat通知 (${res.status})、キュー消化`);
  } catch (e) {
    console.log(`  ⚠ GChat通知失敗 (キューに残置、次回リトライ): ${e.message}`);
  }
}

const entries = fs.readdirSync(incomingDir, { withFileTypes: true })
  .filter(e => e.isFile() && /\.csv$/i.test(e.name))
  .map(e => e.name)
  .sort();

console.log(`=== 楽天レビューCSV取込 (incoming: ${incomingDir}) ===`);
console.log(`対象: ${entries.length} ファイル${isDryRun ? ' [DRY RUN]' : ''}`);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
ensureRakutenReviewTables(db);

if (entries.length === 0) {
  console.log('取込対象なし (正常終了)');
  // 前回送信失敗分のキュー消化だけは行う (通知リトライは取込対象の有無と独立)
  if (!isDryRun) await notifyLowRatings(db);
  db.close();
  process.exit(0);
}

let okCount = 0, dupCount = 0, failCount = 0;
let newLowCount = 0;
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
      const p = prepareReviewFile(name, buffer);
      if (p.ok) { console.log(`  [dry] ✓ ${name}: ${p.label} (${p.dateFrom}〜${p.dateTo})`); okCount++; }
      else { console.log(`  [dry] ✗ ${name}: ${p.error}`); failCount++; }
      continue;
    }

    const outcome = importReviewFile(db, { name, buffer, sha256, source: 'incoming' });
    for (const r of outcome.results) {
      if (r.ok) {
        const delNote = (r.missed || r.deleted) ? ` / 不在+1 ${r.missed} / 削除確定 ${r.deleted}` : '';
        console.log(`  ✓ ${r.file}: ${r.label} (insert ${r.inserted} / update ${r.updated} / 変化なし ${r.unchanged}${delNote}) ${r.date_from}〜${r.date_to}`);
        for (const w of r.warnings || []) console.log(`    ⚠ ${w}`);
      } else {
        console.log(`  ${r.duplicate ? '↷' : '✗'} ${r.file}: ${r.error}`);
      }
    }
    newLowCount += outcome.newLowRatings.length; // キュー追加は取込tx内 (lib側) で完了済み

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
  if (!isDryRun) await notifyLowRatings(db);
} finally {
  db.close();
}

console.log(`\n=== summary: ok=${okCount} duplicate=${dupCount} failed=${failCount} 新規低評価=${newLowCount} ===`);
process.exit(failCount > 0 ? 1 : 0);
