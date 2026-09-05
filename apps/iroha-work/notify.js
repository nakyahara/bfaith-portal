/**
 * いろは在庫化作業アプリ — 職員への通知 (Google Chat)。
 *
 * いまは「🧰 資材が足りない」の申告だけ (監修レポート 2026-09-05「あった方がよい機能」:
 * 資材不足で止まったら、どの資材が何個足りないかを 1 タップで残し、職員に通知)。
 *
 * 通知先 = env GCHAT_WEBHOOK_IROHA (他アプリの GCHAT_WEBHOOK_* と同じ流儀)。
 * ⭐未設定・送信失敗でも throw しない — 通知は付け足しで、札とタイマー停止 (本体の操作) は先に成立している。
 *   現場を止めないため。送れなかったことは操作履歴 (f_iroha_app_events) に残す (router 側)
 */
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

export const WEBHOOK_ENV = 'GCHAT_WEBHOOK_IROHA';

const defaultSender = (webhook, text) => sendGChatMessage(webhook, text, { timeoutMs: 10000 });
let sender = defaultSender;

/** テスト用: 送信関数を差し替える (本物の Chat に投げない)。null で元に戻す */
export function setNotifySender(fn) { sender = fn || defaultSender; }

/** 読み手ファースト: 何が・どの商品で・だれが・いつ・次に何をするか (GChat 通知の流儀) */
export function materialsShortageText({ title, productCode, note, workerName, deviceLabel, at = new Date() } = {}) {
  const jst = new Date(at.getTime() + 9 * 3600 * 1000);
  const when = `${jst.getUTCMonth() + 1}/${jst.getUTCDate()} ${String(jst.getUTCHours()).padStart(2, '0')}:${String(jst.getUTCMinutes()).padStart(2, '0')}`;
  return [
    '🧰 *資材が足りません* — いろは在庫化',
    `商品: ${title || '(名称なし)'}${productCode ? ` (${productCode})` : ''}`,
    `足りないもの: ${note || '(書かれていません)'}`,
    `止めた人: ${workerName || '—'}${deviceLabel ? ` / ${deviceLabel}` : ''} ・ ${when}`,
    '→ 職員: 資材を届けたら、iPad で「▶ 作業をはじめる」を押すと札が外れます',
  ].join('\n');
}

/**
 * 職員のスペースへ送る。{ sent: true } / { sent: false, reason }。throw しない
 * @param {string} text
 */
export async function notifyStaff(text) {
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
