/**
 * rakuten-review-mail-lib.js — フォロー/クーポンメールの文面 (mall-csv-fetcher P2 PR-C4)
 *
 * らくらくーぽん置換のメール文面3種。vendor (らくらくーぽん) の転記 (設計書v1.2冒頭) を土台に、
 * 以下を意図的に変更している:
 *   - 宛名 (お客様姓/名) なし: contacts は PII 最小化でマスクメールと発送日しか持たない (PR-B設計)。
 *     氏名を取らない選択の帰結として、宛名は「お客様」固定
 *   - 商品別レビュー投稿URLなし: contacts は商品情報を持たない。v1 は購入履歴ページへの
 *     静的リンク+手順案内 (商品別URLが必要になったら contacts 拡張を別PRで)
 *   - 配信停止 = 本メールへの返信方式 (vendor は Greenwich hosted URL)。
 *     suppress CLI (fetch-rakuten-review-contacts.js suppress) で登録する運用
 *   - フォローメールの主目的 = 商品の到着・状態確認 (楽天ガイドライン: 広告宣伝が主目的はNG。
 *     レビュー依頼は従として記載)
 *
 * 規約留意 (設計書§2): 評価によって特典を変えない (低評価も同一クーポン・文面のみ変更) /
 * メール内に楽天外リンクを置かない (リンクは楽天ドメインのみ。署名の自社URLも置かない)。
 *
 * ★中原さん確認ポイント: SHOP_SIGNATURE の営業時間 (仮値)。文面の言い回し。
 */

export const SHOP_NAME = '雑貨イズム';
export const FROM_ADDRESS = 'info@b-faith.biz';

// vendor 署名 = 住所+営業時間の固定文 (営業時間は仮値 — ★中原さん確認)
export const SHOP_SIGNATURE = `──────────────────────────────
雑貨イズム 楽天市場店 (B-Faith株式会社)
〒564-0038 大阪府吹田市南清和園町41-36
営業時間: 9:00〜17:00 (土日祝を除く)
──────────────────────────────`;

const UNSUBSCRIBE_NOTE = `※今後このようなご案内が不要な場合は、お手数ですが本メールにそのまま「配信停止」とご返信ください。以後の配信を停止いたします。`;

const PURCHASE_HISTORY_URL = 'https://order.my.rakuten.co.jp/';
const MY_COUPON_URL = 'https://coupon.rakuten.co.jp/myCoupon';

const fmtJstDate = (iso) => {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  const d = new Date(t + 9 * 3600 * 1000);
  return `${d.getUTCFullYear()}年${d.getUTCMonth() + 1}月${d.getUTCDate()}日`;
};

/** フォローメール (発送10日後)。ctx: { orderNumber, shippingIso } */
export function buildFollowMail(ctx) {
  const shipDate = fmtJstDate(ctx.shippingIso);
  if (!ctx.orderNumber || !shipDate) throw new Error('MAIL_FOLLOW: orderNumber / shippingIso が必要');
  const subject = `【${SHOP_NAME}】商品は無事にお手元に届きましたでしょうか`;
  const text = `お客様

この度は${SHOP_NAME}をご利用いただき、誠にありがとうございます。

${shipDate}に発送いたしましたご注文 (注文番号: ${ctx.orderNumber}) は、
無事にお手元に届きましたでしょうか。

万一、商品が未着の場合や、破損・不具合などがございましたら、
本メールへのご返信にてお気軽にお知らせください。すぐに対応いたします。

■ もしよろしければレビューをお願いします
商品にご満足いただけましたら、ご感想をお聞かせいただけますと幸いです。
レビューをご投稿いただいた方には、次回のお買い物で使える
5％OFFクーポンをお送りしております。

レビューは購入履歴ページからご投稿いただけます:
${PURCHASE_HISTORY_URL}
(該当のご注文の「商品レビューを書く」からご投稿ください)

${UNSUBSCRIBE_NOTE}

${SHOP_SIGNATURE}`;
  return { subject, text };
}

/** クーポンメール (レビュー投稿検知後)。ctx: { couponUrl, couponEndIso } */
export function buildCouponMail(ctx) {
  const endDate = fmtJstDate(ctx.couponEndIso);
  if (!ctx.couponUrl || !endDate) throw new Error('MAIL_COUPON: couponUrl / couponEndIso が必要');
  const subject = `【${SHOP_NAME}】レビュー投稿ありがとうございます！クーポンをお届けします`;
  const text = `お客様

この度はレビューをご投稿いただき、誠にありがとうございます。
スタッフ一同、大変励みになっております。

ささやかではございますが、お礼として次回のお買い物で使える
クーポンをお送りいたします。

■ 次回購入に使える5％割引クーポン
・値引き: ご注文金額の5％OFF
・有効期限: ${endDate} まで
・ご利用条件: お一人様1回まで / 他のクーポンとの併用不可

▼ クーポンの受け取りはこちら
${ctx.couponUrl}

受け取ったクーポンは myクーポンページでご確認いただけます:
${MY_COUPON_URL}

またのご利用を心よりお待ちしております。

${UNSUBSCRIBE_NOTE}

${SHOP_SIGNATURE}`;
  return { subject, text };
}

/** 低評価 (★1-2) 向けクーポンメール。特典は同一 (規約: 評価で特典を変えるのは禁止)、文面のみ変更 */
export function buildCouponMailLowRating(ctx) {
  const endDate = fmtJstDate(ctx.couponEndIso);
  if (!ctx.couponUrl || !endDate) throw new Error('MAIL_COUPON_LOW: couponUrl / couponEndIso が必要');
  const subject = `【${SHOP_NAME}】貴重なご意見ありがとうございます`;
  const text = `お客様

この度はレビューにて貴重なご意見をお寄せいただき、誠にありがとうございます。

ご期待に沿えなかった点につきまして、真摯に受け止めております。
いただいたご意見は今後の商品選定・サービス改善に必ず活かしてまいります。

もし商品の不具合や配送の問題などがございましたら、
本メールへのご返信にてお知らせください。個別に対応させていただきます。

日頃の感謝を込めて、レビューをご投稿いただいた皆様に
次回のお買い物で使えるクーポンをお送りしております。

■ 次回購入に使える5％割引クーポン
・値引き: ご注文金額の5％OFF
・有効期限: ${endDate} まで
・ご利用条件: お一人様1回まで / 他のクーポンとの併用不可

▼ クーポンの受け取りはこちら
${ctx.couponUrl}

${UNSUBSCRIBE_NOTE}

${SHOP_SIGNATURE}`;
  return { subject, text };
}

export const TEMPLATE_BUILDERS = {
  follow: buildFollowMail,
  coupon: buildCouponMail,
  'coupon-low': buildCouponMailLowRating,
};

/** テンプレ確認用のダミーコンテキスト (send-test 用。実データを使わない) */
export function sampleContext(template) {
  const base = {
    orderNumber: '373343-20260701-0000000000',
    shippingIso: '2026-07-10T12:00:00+09:00',
    couponUrl: 'https://coupon.rakuten.co.jp/getCoupon?getkey=SAMPLE--&rt=',
    couponEndIso: '2026-09-30T23:59:59+09:00',
  };
  if (!TEMPLATE_BUILDERS[template]) throw new Error(`未知のテンプレート: ${template}`);
  return base;
}

/** Message-ID (決定的 = action 単位の at-most-once 証跡。delivery_attempts.message_id UNIQUE と対) */
export function messageIdFor(actionId, dedupeKey) {
  return `<rrc-${actionId}-${String(dedupeKey).slice(0, 12)}@b-faith.biz>`;
}
