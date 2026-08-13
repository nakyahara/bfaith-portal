/**
 * 欠品の即時通知 (要件§5.6 確定 2026-08-11 → 2026-08-13 中原さん要望で LINE を第一候補に)。
 *
 * 送信先 (設定されているものへ送る。両方あれば両方):
 *   - LINE: env PICKING_LINE_CHANNEL_TOKEN (LINE公式アカウントの Messaging API
 *     チャネルアクセストークン)。⚠ LINE Notify は2025-03終了のため Messaging API を使う。
 *     env PICKING_LINE_TO (カンマ区切りの userId/groupId) があれば push、
 *     無ければ broadcast (公式アカウントを友だち追加した全員に届く。社内専用アカウント前提)
 *   - Google Chat: env PICKING_ALERT_WEBHOOK (旧経路。互換のため残す)
 *
 * - どちらも未設定なら何もしない (導入前でも動く)
 * - fail-soft: 通知失敗でピッキング作業は止めない (呼び出し側は fire-and-forget)
 * - 在庫修正・出荷保留の後続対応は通知を受けた管理者が行う (システムは記録と通知まで)
 */

const TIMEOUT_MS = 5000;

/** 読み手ファースト (現場の管理者が次の行動を決められる形) の欠品メッセージ。 */
export function buildShortageText({ batch, line, worker, shortageQty }) {
  const picked = line.qty - shortageQty;
  return [
    '🚨 ピッキング欠品',
    `${batch.folder_name || ''}｜${batch.hikiate_class}`,
    `ロケ: ${line.locationLabel || line.location}`,
    `商品: ${line.product_name || ''} (${line.sku})`,
    `欠品 ${shortageQty}個 / 指示 ${line.qty}個${picked > 0 ? ` (${picked}個は確保済み)` : ''}`,
    `作業者: ${worker}`,
  ].join('\n');
}

async function postJson(url, headers, body) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status}: ${t.slice(0, 150)}`);
    }
  } finally {
    clearTimeout(timer);
  }
}

/** LINE Messaging API へ送信。宛先指定 (push) か全友だち (broadcast)。 */
async function sendLine(text) {
  const token = process.env.PICKING_LINE_CHANNEL_TOKEN;
  if (!token) return false;
  const headers = { Authorization: `Bearer ${token}` };
  const messages = [{ type: 'text', text }];
  const to = String(process.env.PICKING_LINE_TO || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (to.length > 0) {
    for (const id of to) {
      await postJson('https://api.line.me/v2/bot/message/push', headers, { to: id, messages });
    }
  } else {
    await postJson('https://api.line.me/v2/bot/message/broadcast', headers, { messages });
  }
  return true;
}

/** Google Chat webhook へ送信 (旧経路・互換)。 */
async function sendGChat(text) {
  const webhook = process.env.PICKING_ALERT_WEBHOOK;
  if (!webhook) return false;
  await postJson(webhook, {}, { text });
  return true;
}

/**
 * 欠品通知。設定済みの経路すべてへ送る。片方の失敗でもう片方を止めない。
 * @returns {'disabled'|'sent'} 全経路失敗は throw (呼び出し側が warn ログ)
 */
export async function notifyShortage(info) {
  if (!process.env.PICKING_LINE_CHANNEL_TOKEN && !process.env.PICKING_ALERT_WEBHOOK) return 'disabled';
  const text = buildShortageText(info);
  const results = await Promise.allSettled([sendLine(text), sendGChat(text)]);
  const failures = results.filter((r) => r.status === 'rejected');
  const sent = results.some((r) => r.status === 'fulfilled' && r.value === true);
  if (!sent) {
    throw new Error(failures.map((f) => f.reason?.message).join(' / ') || '送信先なし');
  }
  if (failures.length > 0) {
    console.warn(`[picking-notify] 一部経路の送信失敗: ${failures.map((f) => f.reason?.message).join(' / ')}`);
  }
  return 'sent';
}
