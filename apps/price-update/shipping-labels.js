/**
 * price-update / モールの配送方法 番号 → 名前
 *
 * モールの商品APIが返すのは番号だけで、名前 (定形外・ネコポス 等) は各モールの管理画面側の
 * マスタにある。API では引けないので、管理画面の一覧をここに写して持つ。
 *
 * ★ここは「モールの設定を書き写したもの」であって、計算に使う値ではない (表示のためだけ)。
 *   実際の送料は NE の m_products.送料 を使っている (概算粗利の根拠)。
 *   モール別の送料を粗利に反映するかは M2 の前に決める (2026-08-31 時点は未決)。
 *
 * 更新のしかた: モールの管理画面で配送方法を足したら、ここに1行足す。
 * 番号が表に無ければ画面には番号だけが出る (勝手に別の名前を当てない — fail-soft)。
 */

/**
 * 楽天: variants[sku].shipping.shippingMethodGroup の番号。
 * 出典 = 楽天の配送方法セット一覧 (2026-08-31 中原さん提供)
 */
export const RAKUTEN_SHIPPING_METHODS = {
  1: '定形外',
  2: 'クリックポスト（現在使用不可）',
  3: '飛脚宅配便',
  4: 'ゆうパック',
  5: 'ネコポス',
  6: 'クリックポスト',
  7: 'ヤマト運輸宅急便',
  8: '宅急便50サイズ以下',
  9: 'ゆうパケットパフ',
};

/**
 * Yahoo: getItem の <PostageSet> の番号 (商品ごとに変わるのはこの値)。
 * 出典 = Yahoo!ストアの送料設定一覧 (2026-08-31 中原さん提供)。★楽天とは別の採番。
 */
export const YAHOO_POSTAGE_SETS = {
  1: 'デフォルト設定',
  2: 'ネコポス（現在使用不可）',
  3: '定形外',
  4: '佐川急便',
  5: 'あすつくレターパック',
  6: 'ネコポス',
  7: '定形外と追加料金宅配便',
  8: '定形外と追加料金ネコポス',
  9: 'クリックポスト',
  10: '宅急便ヤマト50サイズ用',
  11: 'クロネコ宅急便',
  12: 'ゆうパケットパフ',
  13: '昆虫用_宅急便',
};

/** 番号を「番号 (名前)」にする。表に無ければ番号だけ返す (誤った名前を当てない) */
export function labelOf(map, id) {
  if (id == null || id === '') return null;
  const name = map[String(id)] ?? map[Number(id)];
  return name ? `${id} (${name})` : String(id);
}

/**
 * au PAY の送料設定区分 (postageSegment)。
 * ★番号の意味はまだ確かめていない。実物では 2 が返ってくるのを見ただけで、
 *   どの番号が何を指すかは au PAY の仕様書で確認していない。
 *   **推測で名前を当てると、送料の読み違いがそのまま粗利の嘘になる**ので、
 *   分かるまでは番号のまま出す (楽天・Yahoo の表と同じ扱いにはしない)。
 */
const AUPAY_POSTAGE_SEGMENTS = {};
export function aupayPostageLabel(id) { return labelOf(AUPAY_POSTAGE_SEGMENTS, id); }

export function rakutenShippingLabel(id) { return labelOf(RAKUTEN_SHIPPING_METHODS, id); }
export function yahooPostageLabel(id) { return labelOf(YAHOO_POSTAGE_SETS, id); }

/** 名前だけ (送料マスタとの突合に使う)。表に無ければ null */
export function nameOf(map, id) {
  if (id == null || id === '') return null;
  return map[String(id)] ?? map[Number(id)] ?? null;
}
export function rakutenShippingName(id) { return nameOf(RAKUTEN_SHIPPING_METHODS, id); }
export function yahooPostageName(id) { return nameOf(YAHOO_POSTAGE_SETS, id); }
