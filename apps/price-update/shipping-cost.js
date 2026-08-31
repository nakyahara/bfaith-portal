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
  //    同じ系統なら、実際にどの段で送っているかを知っている商品マスタの値を使う。
  //    ★モールの配送方法名は「設定セットの名前」であって送り方そのものではない
  //      (楽天の「定形外」セットで実際は定形内で送っている、が実運用 — 中原さん 2026-08-31)。
  //      系統さえ合っていれば商品マスタが正なので、警告は出さない
  if (productRow && sameFamily(mallMethodName, productRow.name)) {
    return { cost: productCost, source: 'product', label: productRow.name, exact: false };
  }

  // 3) 系統も違う (例: モール=佐川急便 / 商品マスタ=定形内)。決められないので
  //    商品マスタの値に戻すが「不明」と言う (黙って近い名前に寄せない)。
  //    ここが出たら、モール側か商品マスタのどちらかの登録がずれている合図
  return { cost: productCost, source: 'unknown', label: productRow?.name || null, exact: false };
}

/**
 * 配送方法の系統 (運送会社・サービスの大枠) が同じか。
 * 重さ・サイズの段の違い (定形内 / 定形外規格内(50g以内) など) は同じ系統として扱う。
 */
export function familyOf(name) {
  const n = nameKey(name);
  if (!n) return null;
  if (/^定形/.test(n)) return '郵便定形';                       // 定形内 / 定形外規格内 / 定形外規格外
  if (n.includes('ネコポス')) return 'ネコポス';
  if (n.includes('ゆうパケット')) return 'ゆうパケット';
  if (n.includes('クリックポスト')) return 'クリックポスト';
  if (n.includes('レターパック')) return 'レターパック';
  if (n.includes('ゆうパック')) return 'ゆうパック';
  if (/宅急便|クロネコ|ヤマト/.test(n)) return 'ヤマト宅急便';
  if (/佐川|飛脚/.test(n)) return '佐川';
  return null;                                                   // 分類できない = 同系統とみなさない
}

function sameFamily(a, b) {
  const fa = familyOf(a);
  return fa != null && fa === familyOf(b);
}
