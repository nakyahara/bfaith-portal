// ai-insights: 月次締めリマインダー + 確定後変更検知 cron (PR-3。要件 §6)
//
// 毎日1回 (JST朝)、前月の締め状態をチェックして GChat へ通知する:
//   - 15日/20日: 未宣言リマインダー (各1回。毎日催促しない=形骸化防止)
//   - 宣言後 7日 MF 同期成功なし → 警告 (1回)
//   - 確定版発行後に対象月 PL が変化 (pl_hash 差分) → 「確定後変更あり・訂正版候補」通知 (1回)
//     ※自動再発行はしない。訂正するかは人間が決める (⚙️で再オープン→再宣言)
//
// 既存パターン踏襲 (profit-analysis/notify-job.js): node-cron + ENABLED flag (Dark Launch)
//   + CRON env (UTC) + GCHAT_WEBHOOK_INSIGHT + 例外は飲み込みログのみ

import cron from 'node-cron';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { jstDateStr } from '../../lib/jst-date.js';
import { previousMonthYm, computeMonthPlHash } from './facts-monthly.js';
import { nowIso } from './db.js';

/**
 * チェック本体 (テスト可能に分離。today/post を注入できる)
 * @returns {Array<string>} 送信した通知種別 (テスト用)
 */
export async function runClosingChecks(db, { today, post }) {
  const sent = [];
  const ym = previousMonthYm(new Date(`${today}T12:00:00+09:00`));
  const day = Number(today.slice(8, 10));
  const now = nowIso();

  db.prepare(
    "INSERT OR IGNORE INTO ai_monthly_closing (year_month, status, updated_at) VALUES (?, 'open', ?)"
  ).run(ym, now);
  const row = db.prepare('SELECT * FROM ai_monthly_closing WHERE year_month = ?').get(ym);

  // 送信権を条件付き UPDATE で「先に」原子的に獲得してから投稿する (Codex PR-3 high 対応):
  // 重複稼働インスタンス・「投稿後マーク前のクラッシュ」でも二重送信しない。
  // 投稿に失敗したらベストエフォートでマーカーを戻す (戻せない場合は「送られない」側に倒す
  // = 二重送信より安全。リマインダーは15/20日の二段構えでカバー)
  // extraWhere: 通知の「資格条件」も claim と同時に検証する (行取得後〜claim前に宣言/確定が
  // 進んだ場合、不要になった通知を送らない。Codex 2巡目 medium 対応)
  const claimAndPost = async (col, kind, text, extraWhere = '', extraParams = []) => {
    const claimed = db.prepare(
      `UPDATE ai_monthly_closing SET ${col} = ?, updated_at = ?
       WHERE year_month = ? AND ${col} IS NULL ${extraWhere}`
    ).run(now, now, ym, ...extraParams);
    if (claimed.changes !== 1) return false;
    try {
      await post(text);
      sent.push(kind);
      return true;
    } catch (e) {
      db.prepare(`UPDATE ai_monthly_closing SET ${col} = NULL, updated_at = ? WHERE year_month = ?`)
        .run(nowIso(), ym);
      throw e;
    }
  };

  // ── 未宣言リマインダー (15日 / 20日の二段。各1回) ──
  // 15日窓 (15〜19日) を全部逃した場合は 20日通知に集約する (同時に2通は出さない=狼少年防止)
  if (row.status === 'open' || row.status === 'reopened') {
    if (day >= 20 && !row.reminder_20_sent_at) {
      await claimAndPost('reminder_20_sent_at', 'reminder_20', [
        `*🔔 ${ym} 月次締めリマインダー (2回目)*`,
        `20日を過ぎましたが ${ym} の締め宣言がまだです。`,
        'MFの仕訳入力が終わっていたら、⚙️「AI経営レポート設定」画面で確定宣言をお願いします。',
        '宣言があるまで月次レポートは暫定版のままです。',
      ].join('\n'), "AND status IN ('open','reopened')");
    } else if (day >= 15 && day < 20 && !row.reminder_15_sent_at) {
      await claimAndPost('reminder_15_sent_at', 'reminder_15', [
        `*🔔 ${ym} 月次締めリマインダー*`,
        `${ym} の締め宣言がまだです。MFの仕訳入力完了後、⚙️「AI経営レポート設定」画面で確定宣言をすると、確定版の月次レポートが発行されます。`,
      ].join('\n'), "AND status IN ('open','reopened')");
    }
  }

  // ── 宣言後 7日 MF 同期なし警告 ──
  if (row.status === 'declared' && row.declared_at && !row.sync_stall_notified_at && !row.final_report_id) {
    const latestSync = db.prepare(`
      SELECT MAX(synced_at) AS s FROM mirror_mf_publish_runs WHERE status = 'success'
    `).get();
    const declaredPlus7 = new Date(new Date(row.declared_at).getTime() + 7 * 86400000).toISOString();
    if (now > declaredPlus7 && (!latestSync?.s || latestSync.s <= row.declared_at)) {
      await claimAndPost('sync_stall_notified_at', 'sync_stall', [
        `*⚠️ ${ym} 確定版が発行できません*`,
        `締め宣言 (${row.declared_at.slice(0, 10)}) から7日以上、MF同期が成功していません。`,
        'miniPC の daily-sync / MF publish の状態を確認してください。',
      ].join('\n'), "AND status = 'declared' AND final_report_id IS NULL");
    }
  }

  // ── 確定後変更検知 (pl_hash 差分) → 訂正版候補通知 (自動再発行しない) ──
  if (row.final_report_id && row.final_pl_hash && !row.change_notified_at && row.status === 'declared') {
    const currentHash = computeMonthPlHash(db, ym);
    if (currentHash && currentHash !== row.final_pl_hash) {
      await claimAndPost('change_notified_at', 'post_final_change', [
        `*⚠️ ${ym} 確定後にMF会計データが変更されています*`,
        '確定版レポート発行後、対象月のPLに変化を検知しました (過年度仕訳の追加・修正の可能性)。',
        '内容を確認し、レポートを作り直す場合は ⚙️画面で再オープン → 再宣言してください (訂正版が発行されます)。',
        '自動では再発行しません。',
      ].join('\n'), 'AND status = ? AND final_pl_hash = ?', ['declared', row.final_pl_hash]);
    }
  }
  return sent;
}

export function startAiInsightsNotifyJob() {
  const enabled = process.env.AI_INSIGHTS_NOTIFY_ENABLED === 'true' || process.env.AI_INSIGHTS_NOTIFY_ENABLED === '1';
  if (!enabled) {
    console.log('[ai-insights] notify-job disabled (AI_INSIGHTS_NOTIFY_ENABLED != true)');
    return;
  }
  const cronExpr = process.env.AI_INSIGHTS_NOTIFY_CRON || '0 1 * * *'; // UTC 01:00 = JST 10:00
  cron.schedule(cronExpr, async () => {
    try {
      const webhook = process.env.GCHAT_WEBHOOK_INSIGHT;
      if (!webhook) {
        console.error('[ai-insights] notify-job: GCHAT_WEBHOOK_INSIGHT 未設定のためスキップ');
        return;
      }
      const db = getMirrorDB();
      const sent = await runClosingChecks(db, {
        today: jstDateStr(new Date()),
        post: (text) => sendGChatMessage(webhook, text),
      });
      console.log(`[ai-insights] notify-job done: ${sent.length ? sent.join(',') : 'no-op'}`);
    } catch (e) {
      console.error('[ai-insights] notify-job error (飲み込み):', e.message);
    }
  }, { timezone: 'UTC' });
  console.log(`[ai-insights] notify-job scheduled: ${cronExpr} (UTC)`);
}
