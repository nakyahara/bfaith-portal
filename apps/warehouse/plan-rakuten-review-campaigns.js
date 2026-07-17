#!/usr/bin/env node
/**
 * plan-rakuten-review-campaigns.js — フォロー/クーポン planner CLI (mall-csv-fetcher P2 PR-C1)
 *
 * shadow 運用: 「らくらくーぽんなら何をいつ送るか」を rakuten_campaign_actions に記録するだけ。
 * **送信は一切しない** (SMTP・クーポン発行APIのコードはこのCLIに存在しない)。
 * らくらくーぽん実績との突合 (PR-C2) の材料を毎日積む。
 *
 * サブコマンド:
 *   plan     計画の作成+状態遷移 (既定。daily-sync から毎朝実行)
 *   stats    状態サマリ (PIIなし)
 *
 * env: DATA_DIR (必須)
 * exit code: 0=成功 / 1=失敗 / 2=env・引数エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { ensureCampaignTables, planCampaigns, campaignStats } from './rakuten-review-campaign-lib.js';
import { ensureContactTables } from './rakuten-review-contacts-lib.js';
import { ensureRakutenReviewTables } from './rakuten-review-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : 'plan';

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
// attempts/grants の REFERENCES を実際に強制する (SQLite は接続ごとに有効化が必要 — Codex C1-R2)。
// PR-C4 の sender 接続でも必ず有効化すること
db.pragma('foreign_keys = ON');
// planner は contacts / reviews を読むため、初回実行順に依らず全テーブルを冪等に確保する
ensureRakutenReviewTables(db);
ensureContactTables(db);
ensureCampaignTables(db);

try {
  if (mode === 'plan') {
    const c = planCampaigns(db);
    console.log(`[campaign] plan完了 (shadow): フォロー新規 ${c.followInserted} / クーポン新規 ${c.couponInserted} / ` +
      `ready昇格 ${c.promotedReady} / 抑止 ${c.suppressed} / 期限切れ ${c.expired} / 取消 ${c.cancelled} / ` +
      `再スケジュール ${c.rescheduled} / ownership追加 ${c.ownershipInserted}`);
  } else if (mode === 'stats') {
    const s = campaignStats(db);
    console.log(`coupon_epoch: ${s.epoch || '(未設定=plan未実行)'}`);
    console.log(`予定超過で待機中 (planned): ${s.dueOverdue}件 / 今後24時間の送信予定 (planned): ${s.dueNext24h}件 / 本日(JST) ready昇格: ${s.readyTodayJst}件`);
    for (const row of s.byStatus) {
      console.log(`  ${row.action_type} ${row.status}${row.reason ? ` (${row.reason})` : ''}: ${row.n}件`);
    }
    if (s.byStatus.length === 0) console.log('  (action なし)');
  } else {
    console.error(`FATAL: unknown mode '${mode}' (plan / stats)`);
    process.exitCode = 2;
  }
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exitCode = 1;
} finally {
  db.close();
}
