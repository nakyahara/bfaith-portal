/**
 * 価格改定エンジン — 6モードの価格計算ロジック
 *
 * モード一覧:
 *   FBA状態合わせ   — FBA出品者の最安値に合わせる
 *   状態合わせ      — 全出品者の最安値に合わせる
 *   FBA最安値       — FBA最安値に合わせる
 *   最安値          — 全出品者の絶対的な最安値に合わせる
 *   カート          — カートボックス価格に合わせる
 *   カスタム        — 全パラメータ自由設定
 */

const SELLER_ID = process.env.SP_API_SELLER_ID || 'A6HMLHKUYJC27';

/**
 * ANY_OFFER_CHANGED通知ペイロードを内部フォーマットに変換
 * 通知に含まれるデータだけで価格計算可能（追加API呼び出し不要）
 */
export function parseNotificationPayload(notification) {
  const data = notification.Payload?.AnyOfferChangedNotification;
  if (!data) return null;

  const trigger = data.OfferChangeTrigger;
  const summary = data.Summary || {};
  const rawOffers = data.Offers || [];

  // オファーを内部フォーマットに変換
  const offers = rawOffers.map(o => ({
    sellerId: o.SellerId,
    isFba: o.IsFulfilledByAmazon,
    isBuyBoxWinner: o.IsBuyBoxWinner,
    listingPrice: o.ListingPrice?.Amount || 0,
    shipping: o.Shipping?.Amount || 0,
    totalPrice: (o.ListingPrice?.Amount || 0) + (o.Shipping?.Amount || 0),
    points: o.Points?.PointsNumber || 0,
    sellerFeedbackRating: o.SellerFeedbackRating?.SellerPositiveFeedbackRating || null,
    sellerFeedbackCount: o.SellerFeedbackRating?.FeedbackCount || 0,
  }));

  // BuyBox価格
  const buyBoxEntry = summary.BuyBoxPrices?.find(b => b.Condition === 'New' || b.condition === 'New');
  const buyBoxPrice = buyBoxEntry?.LandedPrice?.Amount || null;

  return {
    asin: trigger.ASIN,
    buyBoxPrice,
    offers,
    fbaOffers: offers.filter(o => o.isFba),
    fbmOffers: offers.filter(o => !o.isFba),
    changeType: trigger.OfferChangeType,
    eventTime: trigger.TimeOfOfferChange,
  };
}

/**
 * 競合オファーから自社を除外
 */
function excludeSelf(offers) {
  return offers.filter(o => o.sellerId !== SELLER_ID);
}

/**
 * 価格計算メイン
 * @param {object} product - productsテーブルのレコード
 * @param {object} offersData - getItemOffers() の戻り値
 * @returns {{ newPrice: number|null, reason: string, competitorPrice: number|null, buyBoxPrice: number|null }}
 */
export function calculateNewPrice(product, offersData) {
  const mode = product.price_tracking;
  const currentPrice = product.selling_price;
  const lossStopper = product.loss_stopper || 0;
  const highStopper = product.high_stopper || 0;

  if (!mode || !currentPrice) {
    return { newPrice: null, reason: '追従モードまたは現在価格が未設定', competitorPrice: null, buyBoxPrice: offersData.buyBoxPrice };
  }

  const allOffers = excludeSelf(offersData.offers || []);
  const fbaOffers = allOffers.filter(o => o.isFba);
  const buyBoxPrice = offersData.buyBoxPrice;

  let targetPrice = null;
  let reason = '';

  switch (mode) {
    case 'FBA状態合わせ':
      targetPrice = getLowestTotal(fbaOffers);
      reason = targetPrice ? `FBA最安値 ¥${targetPrice} に合わせ` : '競合FBA出品者なし';
      break;

    case '状態合わせ':
      targetPrice = getLowestTotal(allOffers);
      reason = targetPrice ? `全出品者最安値 ¥${targetPrice} に合わせ` : '競合出品者なし';
      break;

    case 'FBA最安値':
      targetPrice = getLowestTotal(fbaOffers);
      reason = targetPrice ? `FBA最安値 ¥${targetPrice} に追従` : '競合FBA出品者なし';
      break;

    case '最安値':
      targetPrice = getLowestTotal(allOffers);
      reason = targetPrice ? `絶対最安値 ¥${targetPrice} に追従` : '競合出品者なし';
      break;

    case 'カート':
      targetPrice = buyBoxPrice;
      reason = targetPrice ? `カート価格 ¥${targetPrice} に合わせ` : 'カート価格取得不可';
      break;

    case 'カスタム':
      // カスタムモードは将来実装（Phase 4）
      return { newPrice: null, reason: 'カスタムモードは未実装', competitorPrice: null, buyBoxPrice };

    default:
      return { newPrice: null, reason: `不明なモード: ${mode}`, competitorPrice: null, buyBoxPrice };
  }

  if (targetPrice === null) {
    // 競合なし → 高値ストッパーまで引き上げ
    if (highStopper > 0 && currentPrice < highStopper) {
      return {
        newPrice: highStopper,
        reason: '競合なし → 高値ストッパーまで引き上げ',
        competitorPrice: null,
        buyBoxPrice,
      };
    }
    return { newPrice: null, reason: reason + ' → 価格維持', competitorPrice: null, buyBoxPrice };
  }

  let newPrice = targetPrice;

  // 自分が最安値の場合 → 次の出品者の価格まで引き上げ（値下げ合戦防止）
  const myOffer = (offersData.offers || []).find(o => o.sellerId === SELLER_ID);
  if (myOffer) {
    const othersAboveMe = allOffers
      .filter(o => o.totalPrice > myOffer.totalPrice)
      .sort((a, b) => a.totalPrice - b.totalPrice);

    if (allOffers.length > 0 && getLowestTotal(allOffers) >= myOffer.totalPrice && othersAboveMe.length > 0) {
      // 自分が最安 → 次の出品者の価格に合わせる
      newPrice = othersAboveMe[0].totalPrice;
      reason += ` → 自社最安のため次の出品者 ¥${newPrice} に引き上げ`;
    }
  }

  // 赤字ストッパー
  if (lossStopper > 0 && newPrice < lossStopper) {
    newPrice = lossStopper;
    reason += ` → 赤字ストッパー ¥${lossStopper} で制限`;
  }

  // 高値ストッパー
  if (highStopper > 0 && newPrice > highStopper) {
    newPrice = highStopper;
    reason += ` → 高値ストッパー ¥${highStopper} で制限`;
  }

  // 価格変更なし
  if (newPrice === currentPrice) {
    return { newPrice: null, reason: reason + ' → 現在価格と同じため変更なし', competitorPrice: targetPrice, buyBoxPrice };
  }

  return {
    newPrice: Math.round(newPrice),
    reason,
    competitorPrice: targetPrice,
    buyBoxPrice,
  };
}

/**
 * オファー配列から最安の合計価格を取得
 */
function getLowestTotal(offers) {
  if (!offers || offers.length === 0) return null;
  return Math.min(...offers.map(o => o.totalPrice));
}
