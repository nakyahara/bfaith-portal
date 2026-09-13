/**
 * FBA箱詰め記録 — 本社への通知 (Google Chat)。中原さん 2026-09-10
 *
 * いろはが iPad で「作業を終える」と、送り状・箱ラベル用の一覧 (report.js / views/report.ejs) のリンクを送る。
 * 通知先 = env GCHAT_WEBHOOK_FBA_BOX (他アプリの GCHAT_WEBHOOK_* と同じ流儀)。
 * ⭐未設定・送信失敗でも throw しない — 完了は DB でもう成立している。通知のために現場を止めない。
 *   送れたか/送れなかったかは fbx_events (notify_run_done) に残す (router 側)
 */
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

export const WEBHOOK_ENV = 'GCHAT_WEBHOOK_FBA_BOX';

const defaultSender = (webhook, text) => sendGChatMessage(webhook, text, { timeoutMs: 10000 });
let sender = defaultSender;

/** テスト用: 送信関数を差し替える (本物の Chat に投げない)。null で元に戻す */
export function setNotifySender(fn) { sender = fn || defaultSender; }

/**
 * 本社のスペースへ送る。{ sent: true } / { sent: false, reason }。throw しない
 * @param {string} text
 */
export async function notifyHq(text) {
  const webhook = process.env[WEBHOOK_ENV];
  if (!webhook) return { sent: false, reason: 'no_webhook' };
  if (typeof text !== 'string' || !text) return { sent: false, reason: 'empty' };
  try {
    await sender(webhook, text);
    return { sent: true };
  } catch (e) {
    return { sent: false, reason: (e && e.message) || String(e) };
  }
}
