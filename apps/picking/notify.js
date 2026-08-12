/**
 * Google Chat への欠品即時通知 (要件§5.6 確定 2026-08-11: 欠品=管理者チャットへ通知)。
 *
 * - env PICKING_ALERT_WEBHOOK (GChat webhook URL)。未設定なら何もしない (導入前でも動く)
 * - fail-soft: 通知失敗でピッキング作業は止めない (呼び出し側は fire-and-forget)
 * - 在庫修正・出荷保留の後続対応は通知を受けた管理者が行う (システムは記録と通知まで)
 */

const TIMEOUT_MS = 5000;

/** 読み手ファースト (現場の管理者が次の行動を決められる形) で欠品を通知する。 */
export async function notifyShortage({ batch, line, worker, shortageQty }) {
  const webhook = process.env.PICKING_ALERT_WEBHOOK;
  if (!webhook) return 'disabled';
  const picked = line.qty - shortageQty;
  const text = [
    '🚨 ピッキング欠品',
    `${batch.folder_name || ''}｜${batch.hikiate_class}`,
    `ロケ: ${line.locationLabel || line.location}`,
    `商品: ${line.product_name || ''} (${line.sku})`,
    `欠品 ${shortageQty}個 / 指示 ${line.qty}個${picked > 0 ? ` (${picked}個は確保済み)` : ''}`,
    `作業者: ${worker}`,
  ].join('\n');
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal: ctrl.signal,
    });
    if (!res.ok) throw new Error(`GChat webhook HTTP ${res.status}`);
    return 'sent';
  } finally {
    clearTimeout(timer);
  }
}
