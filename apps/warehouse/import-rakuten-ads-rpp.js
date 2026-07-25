#!/usr/bin/env node
/**
 * import-rakuten-ads-rpp.js — 楽天RPPレポート incoming/ 取込 CLI (mall-csv-fetcher P1)
 *
 * miniPCローカル incoming/ フォルダが正規投入口 (B案確定 2026-07-09):
 *   - 自動DL (rakuten-rpp-download.mjs) がここへ置く
 *   - 手動バックアップも同じフォルダへ置くだけ (Googleドライブ同期は補助投入口 →
 *     ローカルへ吸い上げてからここに置く。DBにDriveを直接読ませない)
 *
 * 処理:
 *   <DATA_DIR>/incoming/rakuten-ads/ 直下の *.csv / *.zip を走査し、
 *   1物理ファイル=1トランザクションで warehouse.db の fact へ UPSERT。
 *   成功 → processed/YYYY-MM/ へ移動 / 失敗・対象外 → failed/ へ移動 (削除はしない)。
 *   冪等: 同一 sha256 取込済みは duplicate としてスキップ (processed へ移動)。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/import-rakuten-ads-rpp.js
 *   node apps/warehouse/import-rakuten-ads-rpp.js --data-dir C:\path\to\data [--dry-run]
 *
 * exit code: 0=成功 (対象0件含む。duplicateも成功扱い) / 1=いずれか失敗 / 2=env・引数エラー
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { ensureRppTables, importPhysicalFile, prepareFile, expandFile } from './rakuten-ads-rpp-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const isDryRun = args.includes('--dry-run');

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required (env or --data-dir)'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const INCOMING_SUBDIR = path.join('incoming', 'rakuten-ads');
const incomingDir = path.join(DATA_DIR, INCOMING_SUBDIR);
const processedRoot = path.join(incomingDir, 'processed');
const failedDir = path.join(incomingDir, 'failed');
for (const d of [incomingDir, processedRoot, failedDir]) fs.mkdirSync(d, { recursive: true });

// 移動先に同名があっても上書きしない (タイムスタンプ付与でリネーム)
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

// ─── 取得鮮度ウォッチドッグ: 自動DL (fetch-all) が無音で止まっていないか ───
// Task Scheduler自体の停止・fetch-allの無音死は誰も通知できない → 独立スケジュールで走る
// この取込 (daily-sync内) が report_fetch_log の鮮度を毎朝検分し、古ければ GChat へ直接通知。
// テーブルが空 = fetch-all 未稼働 (移設前) とみなし警告しない。通知は fail-soft (取込の成否は変えない)
const STALE_FETCH_HOURS = 48;
async function warnIfFetchStale(db) {
  let last;
  try {
    last = db.prepare(`SELECT MAX(fetched_at) AS m, COUNT(*) AS c FROM report_fetch_log WHERE mall = 'rakuten'`).get();
  } catch { return; }
  if (!last || last.c === 0 || !last.m) return; // 未稼働
  const ageH = (Date.now() - new Date(last.m).getTime()) / 3600000;
  if (!(ageH > STALE_FETCH_HOURS)) return;
  const msg = [
    `⚠️ *モールCSV自動取得が止まっている可能性 (rakuten)*`,
    `report_fetch_log の最終記録が ${Math.round(ageH)} 時間前 (${last.m})。fetch-all が実行されていない疑い。`,
    `確認: ①miniPCのTask Scheduler (fetch-all タスクの前回結果) ②scripts/mall-csv-fetcher/logs/ の最新ログ ③手動実行 node scripts/mall-csv-fetcher/fetch-all.mjs`,
    `無停止手順: RMSから手動DL→incoming/rakuten-ads/ に置けば取込は継続`,
  ].join('\n');
  console.warn(`⚠ 自動DLの最終記録が${Math.round(ageH)}時間前 (${last.m})。fetch-all停止の疑い`);
  const webhook = process.env.GCHAT_WEBHOOK_MALL_FETCH || process.env.GCHAT_WEBHOOK;
  if (!webhook) { console.warn('[stale-fetch] [NOTIFY:status=skipped] GCHAT_WEBHOOK未設定'); return; }
  try {
    const res = await fetch(webhook, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: msg }),
    });
    console.log(`[stale-fetch] [NOTIFY:status=${res.ok ? 'sent' : 'failed'}]`);
  } catch (e) {
    console.error(`[stale-fetch] [NOTIFY:status=failed] ${e.message}`);
  }
}

const entries = fs.readdirSync(incomingDir, { withFileTypes: true })
  .filter(e => e.isFile() && /\.(csv|zip)$/i.test(e.name))
  .map(e => e.name)
  .sort();

console.log(`=== 楽天RPPレポート取込 (incoming: ${incomingDir}) ===`);
console.log(`対象: ${entries.length} ファイル${isDryRun ? ' [DRY RUN]' : ''}`);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
ensureRppTables(db);
if (!isDryRun) await warnIfFetchStale(db); // 取込0件の朝こそ鮮度を検分する

// ⚠️ 直前の warnIfFetchStale() は GChat通知 (fetch/undici) を打つことがある。その直後の
//   process.exit() は Windows node で libuv assertion (UV_HANDLE_CLOSING / abort) を踏み、
//   取込が成功していても daily-sync に失敗と見なされる (#614 楽天レビュー取込と同じ地雷)。
//   → exitCode + 自然終了にする
if (entries.length === 0) {
  console.log('取込対象なし (正常終了)');
  db.close();
  process.exitCode = 0;
} else {
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
        // DRY RUN: parse 検証のみ (DB書込・ファイル移動なし)
        const g = expandFile(name, buffer);
        if (g.error) { console.log(`  [dry] ✗ ${name}: ${g.error}`); failCount++; continue; }
        let allOk = true;
        for (const f of g.files) {
          const p = prepareFile(f.name, f.buffer);
          if (p.ok) {
            console.log(`  [dry] ✓ ${f.name}: ${p.recipe.label} ${p.records.length}件 (期間 ${p.reportStart || '?'}〜${p.reportEnd || '?'})`);
          } else {
            console.log(`  [dry] ✗ ${f.name}: ${p.error}`);
            allOk = false;
          }
        }
        if (allOk) okCount++; else failCount++;
        continue;
      }

      const outcome = importPhysicalFile(db, { name, buffer, sha256, source: 'incoming' });
      for (const r of outcome.results) {
        if (r.ok) console.log(`  ✓ ${r.file}: ${r.label} ${r.rows}件 (insert ${r.inserted} / update ${r.updated}) ${r.date_from}〜${r.date_to}`);
        else console.log(`  ${r.duplicate ? '↷' : '✗'} ${r.file}: ${r.error}`);
      }

      // ファイル移動 (DB commit 後): ok/duplicate → processed/YYYY-MM/、error/skipped → failed/
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
        // 移動失敗は取込自体の成否に影響させないが、翌回 duplicate 検知で再取込は防がれる
        console.error(`  ⚠ ${name}: ファイル移動失敗 (${e.message})。手で移動してください`);
        if (outcome.status === 'ok' || outcome.status === 'duplicate') okCount++; else failCount++;
      }
    }
  } finally {
    db.close();
  }

  console.log(`\n=== summary: ok=${okCount} duplicate=${dupCount} failed=${failCount} ===`);
  // 通信 (mirror POST / GChat通知) 直後の process.exit() は Windows node で libuv assertion
  // (UV_HANDLE_CLOSING / abort) を踏み、成功しているのに失敗扱いになる → exitCode + 自然終了 (#614 と同根)
  process.exitCode = failCount > 0 ? 1 : 0;
}
