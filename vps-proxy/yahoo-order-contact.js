/**
 * yahoo-order-contact.js — /yahoo/orderContact の純粋ロジック (P2-Y PR-Y-B、らくらくーぽん Yahoo版置換)
 *
 * aupay-proxy.js から require される CommonJS モジュール。副作用なし (テスト可能)。
 *   - 注文IDの形式検証
 *   - orderInfo XML から宛先関連フィールドだけを抜く (CDATA 対応)。**メールアドレスは戻り値にだけ入れ、ログには出さない**
 *   - X-SWS-Authorize-Status の判定 (公開鍵認証失敗は HTTP 200 でも失敗扱い — Yahoo 公式ヘルプ)
 */
'use strict';

const ORDER_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$/;
const CONTACT_FIELDS = 'OrderId,OrderStatus,ShipStatus,ShipDate,SocialGiftType,BillMailAddress';

function isValidOrderId(orderId) {
  return typeof orderId === 'string' && ORDER_ID_RE.test(orderId);
}

/** <Tag>value</Tag> / <Tag><![CDATA[value]]></Tag> の最初の一致を返す (無ければ null、空タグは '') */
function xmlText(text, tag) {
  const m = String(text).match(new RegExp(`<${tag}>(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([^<]*))</${tag}>`));
  if (!m) return null;
  return m[1] != null ? m[1] : (m[2] || '');
}

/**
 * orderInfo のレスポンス XML → { ok, error, code, contact }
 *   contact = { orderId, orderStatus, shipStatus, shipDate, socialGiftType, email }
 */
function parseOrderContactXml(text) {
  const s = String(text || '');
  const errCode = xmlText(s, 'Code');
  const errMsg = xmlText(s, 'Message');
  if (/<Error>/.test(s) && errCode) return { ok: false, error: 'yahoo_error', code: errCode, message: (errMsg || '').slice(0, 120) };
  const status = xmlText(s, 'Status');
  if (status && status !== 'OK' && status !== '0') return { ok: false, error: 'yahoo_status', code: status, message: (errMsg || '').slice(0, 120) };
  if (!/<OrderInfo>/.test(s)) return { ok: false, error: 'no_order_info', code: 'no_order_info', message: 'OrderInfo が無い' };
  const contact = {
    orderId: xmlText(s, 'OrderId') || '',
    orderStatus: xmlText(s, 'OrderStatus') || '',
    shipStatus: xmlText(s, 'ShipStatus') || '',
    shipDate: xmlText(s, 'ShipDate') || '',
    socialGiftType: xmlText(s, 'SocialGiftType') ?? '',
    email: (xmlText(s, 'BillMailAddress') || '').trim(),
  };
  return { ok: true, contact };
}

/** 公開鍵認証結果の判定。ヘッダ無し (none) は「署名を付けていない」= 設定不備なので失敗扱い */
function authorizeStatusOk(headerValue) {
  return String(headerValue || '').toLowerCase() === 'authorized';
}

module.exports = { isValidOrderId, parseOrderContactXml, authorizeStatusOk, CONTACT_FIELDS, xmlText };
