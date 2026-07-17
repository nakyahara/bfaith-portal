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
 *   vendor   らくらくーぽん実績の日次件数を転記 (PR-C2 突合の材料)
 *            例: vendor --date 2026-07-18 --follow 512 --coupon 14
 *   report   shadow 突合レポート (--days 14 既定 / --from --to 指定可)
 *
 * env: DATA_DIR (必須)
 * exit code: 0=成功 / 1=失敗 / 2=env・引数エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import {
  ensureCampaignTables, planCampaigns, campaignStats,
  recordVendorDaily, shadowComparisonReport, jstDateOf,
} from './rakuten-review-campaign-lib.js';
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
  } else if (mode === 'vendor') {
    const date = getArg('--date');
    const follow = getArg('--follow');
    const coupon = getArg('--coupon');
    if (!date || (follow == null && coupon == null)) {
      console.error('FATAL: vendor --date YYYY-MM-DD と --follow N / --coupon N の少なくとも一方が必要');
      process.exitCode = 2;
    } else {
      if (follow != null) recordVendorDaily(db, { dateJst: date, actionType: 'follow', count: Number(follow) });
      if (coupon != null) recordVendorDaily(db, { dateJst: date, actionType: 'coupon', count: Number(coupon) });
      console.log(`[campaign] vendor実績を記録: ${date} follow=${follow ?? '(変更なし)'} coupon=${coupon ?? '(変更なし)'}`);
    }
  } else if (mode === 'report') {
    const todayJst = jstDateOf(new Date().toISOString());
    let from = getArg('--from'), to = getArg('--to');
    if (!from || !to) {
      const days = Math.min(Math.max(parseInt(getArg('--days'), 10) || 14, 1), 92);
      to = todayJst;
      from = new Date(Date.parse(`${todayJst}T00:00:00+09:00`) - (days - 1) * 86400000).toISOString();
      from = jstDateOf(from);
    }
    const rep = shadowComparisonReport(db, { fromJst: from, toJst: to });
    console.log(`=== shadow突合レポート ${rep.fromJst} 〜 ${rep.toJst} (JST) ===`);
    console.log('日付        | follow vendor/shadow/差 | coupon vendor/shadow/差');
    if (rep.days.length === 0) console.log('  (期間内のデータなし)');
    for (const d of rep.days) {
      const f = `${d.follow_vendor ?? '-'} / ${d.follow_shadow} / ${d.follow_diff ?? '-'}`;
      const c = `${d.coupon_vendor ?? '-'} / ${d.coupon_shadow} / ${d.coupon_diff ?? '-'}`;
      console.log(`${d.date_jst}  | ${f.padEnd(23)} | ${c}`);
    }
    console.log('--- would-send にならなかった予定の内訳 (期間内 scheduled) ---');
    if (rep.reasons.length === 0) console.log('  (なし)');
    for (const r of rep.reasons) {
      console.log(`  ${r.action_type} ${r.status}${r.reason ? ` (${r.reason})` : ''}: ${r.n}件`);
    }
    console.log('※ vendor件数は「vendor --date ... --follow N --coupon N」で転記 (らくらくーぽん メール配信履歴画面)');
  } else {
    console.error(`FATAL: unknown mode '${mode}' (plan / stats / vendor / report)`);
    process.exitCode = 2;
  }
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exitCode = 1;
} finally {
  db.close();
}
