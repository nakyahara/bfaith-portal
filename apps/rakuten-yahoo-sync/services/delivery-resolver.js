/**
 * notion_delivery_label → Yahoo (delivery, postage_set, applies_sagawa_price) を引く resolver。
 * ローカル RYS src/services/delivery-resolver.js の ES module 翻訳 (Phase B2 R1-R2 完成版)。
 *
 * 設計原則:
 *   - delivery_mapping (8 seed、 001_initial.sql で投入済) を pure-ish lookup
 *   - 該当無しは null を返す (readiness-check が blocked 判定)
 *   - Codex B2 R1 L-2 反映: trim 後空文字も明示的に null
 */

export function resolveDelivery(db, notionDeliveryLabel) {
  if (!notionDeliveryLabel || typeof notionDeliveryLabel !== 'string') return null;
  const label = notionDeliveryLabel.trim();
  if (label === '') return null;
  const row = db.prepare(`
    SELECT notion_delivery_label, yahoo_delivery, yahoo_postage_set, applies_sagawa_price, note
      FROM delivery_mapping
     WHERE notion_delivery_label = ?
  `).get(label);
  return row || null;
}

/**
 * Notion 'Yahoo価格(佐川)' を採用すべきかを判定。
 *   - 配送方法が applies_sagawa_price=1 (定形外 系 2 種) かつ
 *   - Notion yahoo_price_sagawa が non-null/non-zero → true
 */
export function shouldUseSagawaPrice(deliveryRow, yahooPriceSagawa) {
  if (!deliveryRow) return false;
  if (deliveryRow.applies_sagawa_price !== 1) return false;
  return yahooPriceSagawa !== null && yahooPriceSagawa !== undefined && yahooPriceSagawa > 0;
}
