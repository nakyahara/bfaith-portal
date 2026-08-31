/**
 * yahoo-review-sender-adapter.js — Yahoo 版の送信アダプタ (PR-Y-C4)
 *
 * createSenderEngine(adapter) に渡す 6 項目を Yahoo 用に埋める。
 * 楽天との違いはここに閉じ込め、送信の状態機械 (claim → attempt 予約 → 送信 → 確定) は共通。
 *
 *   monthlyCouponFor : yahoo_campaign_coupons を engine の形に正規化 (当月が未発行なら
 *                      「今使える発行済みクーポン」へフォールバック。最終判定は couponUsableCheck)
 *   couponUrlOk      : Yahoo のクーポン獲得URLだけを通す (楽天ドメイン判定を継承しない)
 *   resolveRecipient : **PII 非保持** — DB には宛先を持たず、送信直前に VPS 経由で取得する。
 *                      配信停止の照合もここで行う (VIEW の masked_email_hash は NULL なので
 *                      planner の共通ゲートでは効かない — Codex Y-C4 R2 High)
 *   buildMail        : Yahoo 文面 (リンクは Yahoo ドメインのみ)
 *   messageIdFor     : `yrc-` prefix (楽天の `rrc-` と衝突させない)
 *   fromHeader       : info@b-faith.biz (Gmail の send-as エイリアス)
 */

import { getCouponRow, isValidCouponUrl, usableCouponFor } from './yahoo-review-coupon-lib.js';
import { fetchYahooOrderContact } from './yahoo-order-contact-lib.js';
import { hmacEmail, isSuppressedHash } from './yahoo-review-suppression-lib.js';
import {
  buildFollowMail, buildCouponMail, buildCouponMailLowRating, messageIdFor, SHOP_NAME, FROM_ADDRESS,
} from './yahoo-review-mail-lib.js';

const YAHOO_ORDER_STATUS_CANCELLED = '4';
const YAHOO_SHIP_STATUS_SHIPPED = '3';

/** 'YYYY/MM/DD HH:MM' (JST) → ISO。台帳は画面と同じ表記で持っているのでここで変換する */
export function couponTimeToIso(v, secs) {
  const m = String(v || '').match(/^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2})$/);
  if (!m) return null;
  return `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${secs}+09:00`;
}

/** 台帳の行を engine の形 ({ status, pc_get_url, coupon_start, coupon_end }) に正規化。
 *  終了は「23:00 の分まで」= 23:00:59 (画面の終了時刻が時単位なので切り上げず保守側に倒す) */
const normalizeRow = (r) => (r ? {
  ...r,
  status: r.status,
  pc_get_url: r.coupon_url,
  coupon_start: couponTimeToIso(r.coupon_start, '00'),
  coupon_end: couponTimeToIso(r.coupon_end, '59'),
} : null);

/**
 * 送信に使うクーポン。まず当月分、無ければ **今の時点で使える発行済みクーポン** を返す。
 *
 * Yahoo のクーポンは vendor と同じく「月初〜翌月末」= 2 か月ぶんが重なる。当月キーだけを見ると、
 * 月初の発行が 1 日落ちただけでその月のクーポンメールが全部止まる (前月分がまだ有効なのに)。
 * → フォールバックを入れる (Codex Y-C5 R1 Medium)。期間・状態・URL の検証は
 *   呼び出し側の couponUsableCheck が最終的に行うので、ここが緩くても送信条件は緩まない。
 */
export function monthlyCouponFor(db, month, nowIso = new Date().toISOString()) {
  const cur = normalizeRow(getCouponRow(db, month));
  if (cur && cur.status === 'issued') return cur;
  return normalizeRow(usableCouponFor(db, nowIso)) || cur;
}

/**
 * 送信直前の宛先解決。取得した値はメモリ上だけで使い、DB には書かない。
 * @param keys suppression 照合用の HMAC 鍵 (Buffer)。**必須** — 無ければ照合できないので送らない
 */
export async function resolveRecipient(db, action, keys, opts = {}) {
  const orderId = String(action?.order_number || '');
  const c = await fetchYahooOrderContact(orderId, opts); // 失敗は err.code / err.retryable 付き
  // 最新の注文状態でもう一度ゲート (planner の判断から時間が経っている。ここが最も新しい事実)
  if (c.orderStatus === YAHOO_ORDER_STATUS_CANCELLED) {
    throw Object.assign(new Error('order_cancelled'), { code: 'order_cancelled', retryable: false });
  }
  if (String(c.shipStatus || '') !== YAHOO_SHIP_STATUS_SHIPPED) {
    throw Object.assign(new Error('not_shipped'), { code: 'not_shipped', retryable: false });
  }
  if (c.socialGiftType && !['0', ''].includes(String(c.socialGiftType))) {
    throw Object.assign(new Error('social_gift'), { code: 'social_gift', retryable: false });
  }
  const email = String(c.email || '').trim();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    throw Object.assign(new Error('no_email'), { code: 'no_email', retryable: false });
  }
  // 配信停止の照合 (Codex Y-C4 R2 High)。鍵が無い = 照合できない → 送らない (fail-closed)。
  // 「照合できないから素通り」にすると配信停止の申し出を無視して送ってしまう
  if (!keys) throw Object.assign(new Error('suppression_key_missing'), { code: 'suppression_key_missing', retryable: false });
  if (db && isSuppressedHash(db, hmacEmail(email, keys))) {
    throw Object.assign(new Error('suppressed'), { code: 'suppressed', retryable: false });
  }
  return email;
}

export function buildMailForAction(action, _nowIso = new Date().toISOString()) {
  if (action.action_type === 'follow') {
    return buildFollowMail({ orderNumber: action.order_number, shippingIso: action.shipping_datetime });
  }
  if (!action.monthlyCoupon?.pc_get_url) throw new Error('MAIL: クーポンURLが無い状態で coupon メールを組み立てようとした');
  const ctx = { couponUrl: action.monthlyCoupon.pc_get_url, couponEndIso: action.monthlyCoupon.coupon_end };
  return action.has_low_active_review ? buildCouponMailLowRating(ctx) : buildCouponMail(ctx);
}

/** @param opts { proxyUrl, secret, fetchImpl, timeoutMs } — テストと本番の注入点 */
export function createYahooSenderAdapter(opts = {}) {
  return Object.freeze({
    mall: 'yahoo',
    monthlyCouponFor,
    couponUrlOk: isValidCouponUrl,
    resolveRecipient: (db, action, keys) => resolveRecipient(db, action, keys, opts),
    buildMail: buildMailForAction,
    messageIdFor,
    fromHeader: `"${SHOP_NAME}" <${FROM_ADDRESS}>`,
  });
}

export const YAHOO_SENDER_ADAPTER = createYahooSenderAdapter();
