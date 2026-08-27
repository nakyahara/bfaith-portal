/**
 * yahoo-order-contact-lib.js — Yahoo 注文の宛先を「送信直前に」VPS プロキシから取る (P2-Y PR-Y-B)
 *
 * 設計 (『らくらくーぽんYahoo版_置換_要件設計_20260827.md』§Y2、約款第10条): 購入者メールアドレスは DB/ファイルに
 * 保存しない。sender (PR-Y-C4) が claim 後にこの関数で取得 → メモリ上で使って破棄する。
 *
 * 戻り値: { email, orderStatus, shipStatus, shipDate, socialGiftType }
 * 失敗: Error に code と retryable を付けて throw
 *   - retryable=true  : ネットワーク断 / timeout / 429 / 5xx / プロキシの公開鍵認証失敗 (502 public_key_auth_failed) / 403 (secret 不一致は設定不備だが送信前なので再試行側)
 *   - retryable=false : 注文が存在しない・取得不能 (Yahoo が明確なエラーを返した) / 注文IDが不正 / ソーシャルギフト
 * PII: この関数はメールアドレスをログに出さない。呼び出し側も同様にすること
 */

export const RETRYABLE_CODES = new Set(['network', 'timeout', 'http_429', 'http_5xx', 'http_403', 'public_key_auth_failed', 'proxy_error']);

function mkErr(code, message, retryable) {
  const e = new Error(`${code}: ${message}`);
  e.code = code;
  e.retryable = retryable;
  return e;
}

/**
 * @param {string} orderId
 * @param {object} opts { proxyUrl, secret, fetchImpl, timeoutMs }
 */
export async function fetchYahooOrderContact(orderId, opts = {}) {
  const proxyUrl = (opts.proxyUrl || process.env.YAHOO_PROXY_URL || '').trim().replace(/\/$/, '');
  const secret = (opts.secret || process.env.YAHOO_PROXY_SECRET || '').trim();
  const fetchImpl = opts.fetchImpl || fetch;
  const timeoutMs = opts.timeoutMs || 20000;
  if (!proxyUrl || !secret) throw mkErr('config', 'YAHOO_PROXY_URL / YAHOO_PROXY_SECRET が未設定', false);
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$/.test(String(orderId || ''))) throw mkErr('invalid_order_id', '注文IDの形式が不正', false);

  let res;
  try {
    res = await fetchImpl(`${proxyUrl}/yahoo/orderContact`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Proxy-Secret': secret },
      body: JSON.stringify({ orderId }),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (e) {
    throw mkErr(e?.name === 'TimeoutError' ? 'timeout' : 'network', String(e?.message || e).slice(0, 80), true);
  }
  let body = null;
  try { body = await res.json(); } catch { body = null; }
  if (res.status === 429) throw mkErr('http_429', `rate limited (Retry-After=${res.headers.get('retry-after') || '-'})`, true);
  if (res.status === 403) throw mkErr('http_403', 'proxy secret rejected', true);
  if (res.status >= 500) {
    const code = body?.error === 'public_key_auth_failed' ? 'public_key_auth_failed' : (body?.error || 'http_5xx');
    throw mkErr(code, String(body?.message || body?.authorizeStatus || res.status).slice(0, 80), true);
  }
  if (res.status === 404 || body?.error === 'order_not_found' || body?.error === 'yahoo_error' || body?.error === 'yahoo_status') {
    throw mkErr('order_not_found', String(body?.code || body?.message || 'not found').slice(0, 80), false);
  }
  if (!res.ok || !body?.ok || !body.contact) throw mkErr('proxy_error', `unexpected response ${res.status}`, true);
  const c = body.contact;
  if (c.socialGiftType && c.socialGiftType !== '0') throw mkErr('social_gift', `SocialGiftType=${c.socialGiftType}`, false);
  if (!c.email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(c.email)) throw mkErr('no_email', 'BillMailAddress が空/不正', false);
  return { email: c.email, orderStatus: c.orderStatus || '', shipStatus: c.shipStatus || '', shipDate: c.shipDate || '', socialGiftType: c.socialGiftType || '' };
}
