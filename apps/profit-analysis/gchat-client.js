/**
 * Google Chat Webhook クライアント
 *
 * 経営インサイトスペース向けの通知送信。
 * Webhook URL は環境変数 GCHAT_WEBHOOK_INSIGHT で指定する。
 */

/**
 * テキストメッセージをGChat Webhookに送信する。
 * 失敗時は throw して呼び出し側でハンドリング。
 *
 * @param {string} webhookUrl  GChat incoming webhook URL
 * @param {string} text        送信メッセージ本文 (mrkdwn 形式)
 * @param {object} opts        { timeoutMs?: number = 30000 }
 * @returns {Promise<{ok:boolean, status:number}>}
 */
export async function sendGChatMessage(webhookUrl, text, opts = {}) {
  if (!webhookUrl || typeof webhookUrl !== 'string') {
    throw new Error('webhookUrl is required');
  }
  if (typeof text !== 'string' || text.length === 0) {
    throw new Error('text is required and must be non-empty string');
  }
  const timeoutMs = opts.timeoutMs ?? 30000;

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=UTF-8' },
      body: JSON.stringify({ text }),
      signal: controller.signal,
    });
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`GChat webhook returned ${res.status}: ${body.slice(0, 200)}`);
    }
    return { ok: true, status: res.status };
  } finally {
    clearTimeout(t);
  }
}
