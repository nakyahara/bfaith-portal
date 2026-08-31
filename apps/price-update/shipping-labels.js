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
 * ⚠️2026-08-31 時点で対応表は未入手。分かったらここに書く (楽天とは別の採番)。
 * 例: 実データでは 合皮補修シート=6 / すそ上げテープ=6 / ハッカ油=12
 */
export const YAHOO_POSTAGE_SETS = {
  // 6: '?',
  // 12: '?',
};

/** 番号を「番号 (名前)」にする。表に無ければ番号だけ返す (誤った名前を当てない) */
export function labelOf(map, id) {
  if (id == null || id === '') return null;
  const name = map[String(id)] ?? map[Number(id)];
  return name ? `${id} (${name})` : String(id);
}

export function rakutenShippingLabel(id) { return labelOf(RAKUTEN_SHIPPING_METHODS, id); }
export function yahooPostageLabel(id) { return labelOf(YAHOO_POSTAGE_SETS, id); }
