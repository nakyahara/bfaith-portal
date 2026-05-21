/**
 * biz-ops-overview 売上サマリ GChat 通知ジョブ (in-server cron)
 *
 * 2026-05-19 中原さん要望: 在庫サマリ通知 (apps/profit-analysis/notify-job.js) と同じ
 * 経営インサイトスペース (GCHAT_WEBHOOK_INSIGHT) に「売上サマリ」を独立メッセージで送信。
 * Codex 助言: 在庫送信と独立 (片方失敗で全落ち防止)。
 *
 * Feature flag: SALES_NOTIFY_ENABLED=true で有効化 (Dark Launch、在庫通知の DARK_LAUNCH と同パターン)
 *
 * 環境変数:
 *   SALES_NOTIFY_ENABLED  - 'true'/'1' で起動時にスケジュール、未設定/'false'はスキップ
 *   SALES_NOTIFY_CRON     - cron式 (デフォルト '0 0 * * *' = UTC00:00 = JST 09:00)
 *   GCHAT_WEBHOOK_INSIGHT - 経営インサイトスペースの Webhook URL (在庫通知と共用)
 *
 * cron 時刻設計 (2026-05-21 修正):
 *   - ⚠️ miniPC daily-sync は JST 07:00 開始で f_sales→mirror sync 完了は ~08:20 (3か月分)。
 *     当初 07:00 通知にしていたら取り込み前の古い mirror を読んで前日値が出る事故 (5/21朝)。
 *   - そのため通知は daily-sync 完了後の JST 09:00 に発火 (mirror が当日分で更新済みの想定)。
 *   - 万一 sync 未完了でも sales-summary の鮮度ガードが「⚠️ データが古い」を出す (二重防御)。
 */
import cron from 'node-cron';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { getSalesSummary, formatGChatSummary } from './lib/sales-summary.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

/**
 * 通知 1 回分を実行。例外は飲み込まずログ出力のみ。
 * 在庫通知 (runNotificationJob) と独立 = 例外伝播しない (Codex 助言)
 */
export async function runSalesNotificationJob() {
  const webhookUrl = process.env.GCHAT_WEBHOOK_INSIGHT;
  if (!webhookUrl) {
    console.warn('[biz-ops-overview/notify-job] GCHAT_WEBHOOK_INSIGHT が未設定。売上通知をスキップ。');
    return { ok: false, reason: 'webhook_not_set' };
  }

  try {
    const db = getMirrorDB();
    const summary = getSalesSummary(db);
    const text = formatGChatSummary(summary);

    // Codex R1 Low #1 反映: GChat 文字数上限ガード (Google Chat ~4000 chars 想定、安全側 3500)
    // Codex R2 Low #1: 単純 slice で ``` コードブロック途中切れの risk あり、
    //   現状 6 モール × 3 列 ~1000 文字で発火しない想定 (= 監視のみ)。
    //   将来モール追加で MAX_LEN 超過するなら、formatGChatSummary 側でセクション単位短縮版を別生成すること。
    const MAX_LEN = 3500;
    let sendText = text;
    if (text.length > MAX_LEN) {
      console.warn(`[biz-ops-overview/notify-job] text_len=${text.length} > ${MAX_LEN}、末尾切り詰めて送信 (TODO: fence 安全な短縮版へ)`);
      sendText = text.slice(0, MAX_LEN - 50) + '\n```\n…(省略、長すぎ)';
    }
    console.log(`[biz-ops-overview/notify-job] 送信開始 asOf=${summary.asOf} text_len=${sendText.length}`);
    const result = await sendGChatMessage(webhookUrl, sendText);
    console.log(`[biz-ops-overview/notify-job] 送信成功 status=${result.status}`);
    return { ok: true, asOf: summary.asOf };
  } catch (e) {
    console.error('[biz-ops-overview/notify-job] 売上通知送信失敗:', e.message);
    return { ok: false, reason: e.message };
  }
}

/**
 * cron スケジュール起動。
 * SALES_NOTIFY_ENABLED='true' のときだけ schedule する (Dark Launch)。
 */
export function startSalesNotificationJob() {
  const enabled = process.env.SALES_NOTIFY_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[biz-ops-overview/notify-job] SALES_NOTIFY_ENABLED が未設定/falseのためスケジュールしない (Dark Launch)');
    return null;
  }
  const cronExpr = process.env.SALES_NOTIFY_CRON || '0 0 * * *'; // UTC 00:00 = JST 09:00 (daily-sync 完了 ~08:20 の後)
  if (!cron.validate(cronExpr)) {
    console.error(`[biz-ops-overview/notify-job] SALES_NOTIFY_CRON が不正: ${cronExpr}`);
    return null;
  }
  const task = cron.schedule(cronExpr, () => {
    runSalesNotificationJob().catch(e => {
      console.error('[biz-ops-overview/notify-job] cron 実行中に未捕捉例外:', e);
    });
  }, { timezone: 'UTC' });
  console.log(`[biz-ops-overview/notify-job] cron schedule 起動: '${cronExpr}' (UTC) — JST 09:00 想定 (daily-sync 完了後、mirror 更新済みを読む)`);
  return task;
}
