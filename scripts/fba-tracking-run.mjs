#!/usr/bin/env node
/**
 * FBA納品 追跡番号の自動投入 — 実行入口 (miniPC の定期実行から呼ぶ)
 *
 *   # プレビュー (何も書き込まない。まずこれで内容を見る)
 *   node scripts/fba-tracking-run.mjs
 *   node scripts/fba-tracking-run.mjs --file "C:\path\to\fukutsu_tuiseki.csv"
 *
 *   # 本番投入
 *   node scripts/fba-tracking-run.mjs --commit
 *
 *   --date YYYYMMDD  期待する出荷日 (既定 = 実行時のJST当日)
 *   --no-notify      GChat へ通知しない
 *
 * 環境変数:
 *   SP_API_* / AWS_*                  SP-API (既存)
 *   GOOGLE_SERVICE_ACCOUNT_KEY        Drive読み取り (既存・--file 指定時は不要)
 *   FBA_TRACKING_DRIVE_FOLDER_ID      追跡出力フォルダのID
 *   FBA_TRACKING_FILENAME             既定 fukutsu_tuiseki.csv
 *   GCHAT_WEBHOOK_JOBS                通知先 (要対応スペース)
 *
 * 終了コード: 0=正常/警告 / 1=中断 (人が直すまで登録できない)
 *   ⭐FBA納品が無い日は普通にあるので「対象なし」で 1 を返さない。
 *     毎晩ニセの警報が鳴ると本当の異常が埋もれる (jobs-monitor の設計方針と同じ)。
 *     期限切れ等の「人が画面で入力すべき件」は GChat の本文で伝え、終了コードは 0 にする。
 */
import 'dotenv/config';
import { runTrackingJob, formatSummary, notifySummary } from '../apps/fba-replenishment/tracking-runner.js';

const argv = process.argv.slice(2);
const flag = (name) => argv.includes(name);
const value = (name) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};

const summary = await runTrackingJob({
  file: value('--file'),
  folderId: value('--folder-id'),
  filename: value('--filename') || process.env.FBA_TRACKING_FILENAME,
  expectYmd: value('--date'),
  commit: flag('--commit'),
});

console.log(formatSummary(summary));
console.log('\n--- 詳細 ---');
console.log(JSON.stringify(summary, null, 2));

if (!flag('--no-notify')) await notifySummary(summary);

const bad = summary.blocked.length > 0 || summary.failed.length > 0;
if (bad) {
  console.error('\n中断または失敗があります。上の内容を確認してください。');
  process.exit(1);
}
