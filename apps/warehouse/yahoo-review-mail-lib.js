/**
 * yahoo-review-mail-lib.js — Yahoo!ショッピング版 フォロー/クーポンメールの文面 (PR-Y-C4)
 *
 * らくらくフォロー (vendor) の実テンプレート (全量調査 §2.5 に全文転記) を土台に、
 * 楽天版 (rakuten-review-mail-lib.js) と同じ方針で以下を意図的に変更している:
 *   - 宛名なし: 要件設計 §Y2 の PII 非保持 (約款第10条) により氏名を取得しない
 *     → vendor の `[%お客様姓%] [%お客様名%] 様` は「お客様」固定に置き換え
 *   - 配信停止 = 本メールへの返信方式 (vendor は Greenwich hosted URL)。
 *     suppress 登録は送信 CLI 側の運用コマンドで行う
 *   - 発送日・注文番号を本文に出す (vendor の `[%発送日%]` 相当。到着確認が主目的であることを明示)
 *
 * リンクは Yahoo! 系ドメインのみに限定する (ALLOWED_LINK_HOSTS。テストで検証)。
 * vendor が本文で使っている 3 URL をそのまま採用:
 *   - レビュー投稿一覧  https://shopping.yahoo.co.jp/review/contribution/list
 *     ⭐楽天版と違い商品別URLではなく共通ページ → 商品情報を持たない設計のままで vendor と同等
 *   - レビューの書き方  https://support.yahoo-net.jp/PccShopping/s/article/H000005878
 *   - 獲得済みクーポン  https://shopping.yahoo.co.jp/my/coupon/
 *
 * 規約留意: 評価によって特典を変えない (低評価も同一クーポン・文面のみ変更)。
 * 署名は vendor 実物 (メールアカウント設定) を転記済み。
 */

export const SHOP_NAME = '雑貨イズム';
export const SHOP_NAME_FULL = '雑貨イズムYahoo!ショッピング店';
export const FROM_ADDRESS = 'info@b-faith.biz';

// 署名 = vendor「メールアカウント設定」の実物 (楽天版と同一)
export const SHOP_SIGNATURE = `--------------------------------
雑貨イズム
〒564-0038
大阪府吹田市南清和園町41-36
info@b-faith.biz

営業時間：月〜金、10時〜16時
休業日：土日祝祭日、夏期休暇、年末年始、GW期間
--------------------------------`;

const UNSUBSCRIBE_NOTE = '※今後このようなご案内が不要な場合は、お手数ですが本メールにそのまま「配信停止」とご返信ください。以後の配信を停止いたします。';

export const REVIEW_POST_URL = 'https://shopping.yahoo.co.jp/review/contribution/list';
export const REVIEW_HOWTO_URL = 'https://support.yahoo-net.jp/PccShopping/s/article/H000005878';
export const MY_COUPON_URL = 'https://shopping.yahoo.co.jp/my/coupon/';

/** 本文に出してよいリンクのホスト (これ以外が混ざったらテストで落とす) */
export const ALLOWED_LINK_HOSTS = Object.freeze(['shopping.yahoo.co.jp', 'support.yahoo-net.jp']);

const fmtJstDate = (iso) => {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  const d = new Date(t + 9 * 3600 * 1000);
  return `${d.getUTCFullYear()}年${d.getUTCMonth() + 1}月${d.getUTCDate()}日`;
};

/** フォローメール (発送10日後の正午)。ctx: { orderNumber, shippingIso } */
export function buildFollowMail(ctx) {
  const shipDate = fmtJstDate(ctx?.shippingIso);
  if (!ctx?.orderNumber || !shipDate) throw new Error('MAIL_FOLLOW: orderNumber / shippingIso が必要');
  const subject = `【${SHOP_NAME}】商品は無事にお手元に届きましたでしょうか`;
  const text = `お客様

この度は「${SHOP_NAME_FULL}」へご注文いただきまして、誠にありがとうございます。

${shipDate}に発送いたしましたご注文 (注文ID: ${ctx.orderNumber}) は、
無事にお手元に届きましたでしょうか。

万一、商品が未着の場合や、破損・不具合などがございましたら、
本メールへのご返信にてお気軽にお知らせください。すぐに対応いたします。

■ もしよろしければレビューをお願いします
商品にご満足いただけましたら、ご感想をお聞かせいただけますと幸いです。
レビューをご投稿いただいた方には、次回のお買い物で使える
5％割引クーポンをお送りしております。

＜レビュー投稿ページ＞
${REVIEW_POST_URL}

＜レビューの書き方＞
${REVIEW_HOWTO_URL}

新商品の企画や、よりお買い物が楽しめるお店作りの参考にさせていただきます。
今後とも「${SHOP_NAME_FULL}」を何卒よろしくお願いいたします。

${UNSUBSCRIBE_NOTE}

${SHOP_SIGNATURE}`;
  return { subject, text };
}

/** クーポンの内容説明 (フォーマットは vendor の枠を踏襲) */
const couponBlock = (endDate, couponUrl) => `┌…★次回使えるお得なクーポンはこちら★…┐
・値引きプラン：ご注文金額の5％割引
・ご利用条件　：ストア内全商品 / 他のクーポンとの併用不可
・有効期限　　：${endDate} まで

＼下記URLをクリックしてクーポンを取得！／
${couponUrl}
└…★☆★☆★☆★☆★☆★☆★☆★☆★☆…┘

※クーポンのご利用期間・ご利用条件は「獲得済みクーポン一覧」ページからもご確認いただけます。
${MY_COUPON_URL}`;

/** クーポンメール (レビュー投稿検知の翌日正午)。ctx: { couponUrl, couponEndIso } */
export function buildCouponMail(ctx) {
  const endDate = fmtJstDate(ctx?.couponEndIso);
  if (!ctx?.couponUrl || !endDate) throw new Error('MAIL_COUPON: couponUrl / couponEndIso が必要');
  const subject = `【${SHOP_NAME}】レビュー投稿ありがとうございます！クーポンをお届けします`;
  const text = `お客様

いつも「${SHOP_NAME_FULL}」をご利用いただき、誠にありがとうございます。

この度はレビューのご投稿ありがとうございました。
貴重なご意見をお聞かせいただき、スタッフ一同心より感謝しております。

商品の状態にはできる限り万全を期しておりますが、
何かございましたらお気軽に本メールへご返信ください。

感謝の気持ちを込めて、次回使えるお得なクーポンをお届けいたします。

${couponBlock(endDate, ctx.couponUrl)}

またのご利用を心よりお待ちしております。

${UNSUBSCRIBE_NOTE}

${SHOP_SIGNATURE}`;
  return { subject, text };
}

/** 低評価 (★1-2) 向け。特典は同一 (規約: 評価で特典を変えない)、文面のみ変更 */
export function buildCouponMailLowRating(ctx) {
  const endDate = fmtJstDate(ctx?.couponEndIso);
  if (!ctx?.couponUrl || !endDate) throw new Error('MAIL_COUPON_LOW: couponUrl / couponEndIso が必要');
  const subject = `【${SHOP_NAME}】貴重なご意見ありがとうございます`;
  const text = `お客様

この度はレビューにて貴重なご意見をお寄せいただき、誠にありがとうございます。

ご期待に沿えなかった点につきまして、真摯に受け止めております。
いただいたご意見は今後の商品選定・サービス改善に必ず活かしてまいります。

もし商品の不具合や配送の問題などがございましたら、
本メールへのご返信にてお知らせください。個別に対応させていただきます。

日頃の感謝を込めて、レビューをご投稿いただいた皆様に
次回のお買い物で使えるクーポンをお送りしております。

${couponBlock(endDate, ctx.couponUrl)}

${UNSUBSCRIBE_NOTE}

${SHOP_SIGNATURE}`;
  return { subject, text };
}

export const TEMPLATE_BUILDERS = {
  follow: buildFollowMail,
  coupon: buildCouponMail,
  'coupon-low': buildCouponMailLowRating,
};

/** テンプレ確認用のダミー (send-test 用。実データを使わない) */
export function sampleContext(template) {
  if (!TEMPLATE_BUILDERS[template]) throw new Error(`未知のテンプレート: ${template}`);
  return {
    orderNumber: 'b-faith01-10288444',
    shippingIso: '2026-08-18T00:00:00+09:00',
    couponUrl: 'https://shopping.yahoo.co.jp/coupon/interior/SAMPLE0123456789ABCD',
    couponEndIso: '2026-10-31T23:00:59+09:00',
  };
}

/**
 * Message-ID (決定的 = action 単位の at-most-once 証跡)。
 * 楽天は `rrc-`。action の id は楽天/Yahoo で別テーブルの連番なので、prefix を分けないと
 * 別モールの別メールが同じ Message-ID を名乗ってしまう → Yahoo は `yrc-`
 */
export function messageIdFor(actionId, dedupeKey) {
  return `<yrc-${actionId}-${String(dedupeKey).slice(0, 12)}@b-faith.biz>`;
}
