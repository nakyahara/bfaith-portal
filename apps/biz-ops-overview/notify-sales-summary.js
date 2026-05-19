#!/usr/bin/env node
/**
 * notify-sales-summary.js — biz-ops-overview 売上サマリ GChat 通知
 *
 * 2026-05-19 中原さん要望: 在庫サマリと同じ GChat スペース (GCHAT_WEBHOOK_INSIGHT) に売上を「追記」。
 * Codex 助言: 在庫送信と独立した別メッセージで送る (片方失敗で全落ち防止)。
 *
 * 想定実行: miniPC daily-sync 後の cron、または独立 Task Scheduler。
 * 在庫サマリ (apps/profit-analysis/notify-job.js) の直後に走らせる想定。
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data node apps/biz-ops-overview/notify-sales-summary.js
 *   --dry-run で送信なし、出力のみ
 *
 * env:
 *   DATA_DIR                必須 (warehouse.db のディレクトリ)
 *   GCHAT_WEBHOOK_INSIGHT   未設定なら skip して exit 0 (in-tact、cron 失敗にしない)
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { getSalesSummary, formatGChatSummary } from './lib/sales-summary.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

const args = process.argv.slice(2);
const isDryRun = args.includes('--dry-run');
const DATA_DIR = (process.env.DATA_DIR || '').trim();

if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR is required');
  process.exit(2);
}

const webhookUrl = process.env.GCHAT_WEBHOOK_INSIGHT;
if (!webhookUrl && !isDryRun) {
  // GChat 未設定なら静かに skip (cron 失敗扱いにしない、memory: feedback_notify_status_terminal_marker.md)
  console.log('[notify-sales-summary] [NOTIFY:status=skipped] GCHAT_WEBHOOK_INSIGHT 未設定、送信スキップ');
  process.exit(0);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exit(2);
}

const db = new Database(dbPath, { readonly: true });
let summary;
try {
  summary = getSalesSummary(db);
} catch (e) {
  // view 未作成等で集計失敗 → cron 失敗扱いにせず status=failed で exit 1
  console.error(`[notify-sales-summary] [NOTIFY:status=failed] sales summary 取得失敗: ${e.message}`);
  db.close();
  process.exit(1);
} finally {
  // 集計後すぐ close (GChat 通信中の lock 不要)
}
db.close();

const text = formatGChatSummary(summary);

if (isDryRun) {
  console.log('--- DRY RUN ---');
  console.log(text);
  console.log('[notify-sales-summary] [NOTIFY:status=dry_run]');
  process.exit(0);
}

try {
  // Codex R1 Low #1 反映: GChat メッセージ長制限ガード (~4000 chars、安全側 3500)
  // Codex R2 Low #1: 切り詰め時の ``` 閉じ fence 確保 (notify-job と同型)
  const MAX_LEN = 3500;
  let sendText = text;
  if (text.length > MAX_LEN) {
    console.warn(`[notify-sales-summary] text_len=${text.length} > ${MAX_LEN}、末尾切り詰めて送信 (TODO: fence 安全な短縮版へ)`);
    sendText = text.slice(0, MAX_LEN - 50) + '\n```\n…(省略、長すぎ)';
  }
  const result = await sendGChatMessage(webhookUrl, sendText);
  console.log(`[notify-sales-summary] [NOTIFY:status=sent] GChat OK (status=${result.status}, text_len=${sendText.length})`);
  process.exit(0);
} catch (e) {
  console.error(`[notify-sales-summary] [NOTIFY:status=failed] GChat 送信失敗: ${e.message}`);
  // 送信失敗は exit 1 (cron 通知失敗で alert)、ただし在庫通知とは独立なので在庫サマリは影響なし
  process.exit(1);
}
