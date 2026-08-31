/**
 * price-update / モール別の配送関係費を決める
 *
 * 同じ商品でも 楽天=定形外 / Yahoo=ネコポス のようにモールごとに配送方法が違い、
 * 送料が変わる (2026-08-31 実機で確認)。粗利をモール別に正しく出すため、
 * 「そのモールの配送方法」に対応する社内の配送関係費を引く。
 *
 * 参照する既存マスタ = `shipping_rates` (mirror_shipping_rates)。
 *   1行 = 配送方法。送料 + 出荷作業料 + 想定梱包資材費 + 想定人件費 = **配送関係費合計**。
 *   商品側 (m_products) は 送料コード でこの表を指し、送料 列には配送関係費合計が入っている。
 *
 * ★勝手に近い名前へ寄せない。決められない時は「不明」と言って、商品マスタの値に戻す。
 *   送料を1つ取り違えると粗利がまるごとずれ、それを根拠に値付けしてしまうため。
 */

/** 名前の比較キー (全角カッコ・空白の揺れを吸収) */
function nameKey(v) {
  return String(v == null ? '' : v)
    .replace(/[（(]/g, '(').replace(/[）)]/g, ')')
    .replace(/\s+/g, '')
    .trim()
    .toLowerCase();
}

/** mirror_shipping_rates を読み込む (小さい表なのでそのまま全件) */
export function loadShippingRates(db) {
  try {
    const rows = db.prepare(`
      SELECT shipping_code, 運送会社 AS carrier, 小分類区分名称 AS name,
             最大重量 AS maxWeight, 送料 AS postage, 配送関係費合計 AS total
        FROM mirror_shipping_rates
    `).all();
    const byCode = new Map(rows.map((r) => [String(r.shipping_code), r]));
    const byName = new Map(rows.map((r) => [nameKey(r.name), r]));
    return { rows, byCode, byName, available: rows.length > 0 };
  } catch {
    // 表がまだ無い環境 (mirror 未同期) でも画面は出す
    return { rows: [], byCode: new Map(), byName: new Map(), available: false };
  }
}

/**
 * モールの配送方法名 → 社内の配送関係費。
 *
 * @param {object} rates loadShippingRates() の戻り
 * @param {object} p
 * @param {string|null} p.mallMethodName モール側の配送方法名 (例 'ネコポス' / '定形外')
 * @param {string|null} p.neShippingCode 商品マスタの送料コード
 * @param {number|null} p.neShippingCost 商品マスタの送料 (= その送料コードの配送関係費合計)
 * @returns {{cost:number|null, source:string, label:string|null, exact:boolean}}
 *   source: 'mall' = モールの配送方法で引けた / 'product' = 商品マスタの値 / 'unknown' = 決められない
 */
export function resolveMallShippingCost(rates, { mallMethodName, neShippingCode, neShippingCost }) {
  const productCost = neShippingCost == null ? null : Number(neShippingCost);
  const productRow = neShippingCode != null ? rates.byCode.get(String(neShippingCode)) : null;

  if (!mallMethodName) {
    // モール側が分からない (未確定・手動モール) → 商品マスタの値をそのまま使う
    return { cost: productCost, source: 'product', label: productRow?.name || null, exact: false };
  }

  // 1) 名前が完全に一致する配送方法があれば、それが答え (ネコポス / ゆうパケットパフ 等)
  const hit = rates.byName.get(nameKey(mallMethodName));
  if (hit && hit.total != null) {
    return { cost: Number(hit.total), source: 'mall', label: hit.name, exact: true };
  }

  // 2) モール側が「定形外」「宅急便」のような**まとめた呼び方**の場合、重さ・サイズの段が決まらない。
  //    商品マスタの配送方法が同じ系統ならその段を使う (定形外 → 定形外規格内(50g以内) など)
  if (productRow && nameKey(productRow.name).startsWith(nameKey(mallMethodName))) {
    return { cost: productCost, source: 'product', label: productRow.name, exact: false };
  }

  // 3) 決められない。商品マスタの値に戻すが「不明」と言う (黙って近い名前に寄せない)
  return { cost: productCost, source: 'unknown', label: productRow?.name || null, exact: false };
}
