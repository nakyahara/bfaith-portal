/**
 * rakuten-sku-map-build.js — 楽天 全SKU → f_rakuten_sku_map の行 を組み立てる (純粋関数)
 *
 * rebuild-rakuten-sku-map.js から切り出した。ここが間違うと
 * 「価格一括改定で色違いの商品ページにたどり着けない」「粗利分析で楽天の売上が商品に紐づかない」が起きる。
 *
 * 1 SKU は 3 つのコードを持つ (すべて同じ 1 SKU の別名):
 *   AM = systemSkuNumber (システム連携用SKU番号) / AL = skuManageNumber (SKU管理番号) / W = itemNumber (商品番号)
 * どのコードから引いても NE 商品コードに届くよう、3 つとも行にする (rakuten_code が主キー)。
 *
 * ★manage_number (商品管理番号) を全行に持たせる (2026-09-01):
 *   W (商品番号) は 1 商品に 1 つなので、カラバリ 12 色は同じ W を共有する。
 *   rakuten_code が主キーである以上、W の行は 12 色のうち 1 色にしか作れない。
 *   残りの 11 色は W 行を持たず「商品ページ (管理番号) にたどり着けない」= 価格を取れない・変えられない
 *   状態だった (楽天出品 5,531 コードのうち 1,688 コード = 30%)。
 *   → AM/AL の行にもその SKU の manage_number を入れ、どの行からでも商品ページへ届くようにする。
 */

/** AL (SKU管理番号) として意味の無い値。これらは NE コードに解決しない */
export const INVALID_AL = new Set(['normal-inventory', 'normal-size', 'normal', '']);

/** 同じ rakuten_code に複数の SKU が当たったときの優先順 (小さいほど優先) */
export const PRIORITY = { am: 1, al: 2, w: 3 };

/**
 * 1 SKU を NE 商品コードに解決する。AM → AL → W の順で m_products に当てる。
 * @param {{systemSkuNumber?:string, skuManageNumber?:string, itemNumber?:string}} sku
 * @param {Map<string,string>} productMap 小文字の商品コード → 正本表記の商品コード
 * @returns {{ne_code:string, resolution:'am'|'al'|'w'}|null}
 */
export function resolveSku(sku, productMap) {
  const am = (sku.systemSkuNumber || '').toLowerCase();
  const al = (sku.skuManageNumber || '').toLowerCase();
  const w  = (sku.itemNumber || '').toLowerCase();

  if (am && productMap.has(am)) return { ne_code: productMap.get(am), resolution: 'am' };
  if (al && !INVALID_AL.has(al) && productMap.has(al)) return { ne_code: productMap.get(al), resolution: 'al' };
  if (w && productMap.has(w)) return { ne_code: productMap.get(w), resolution: 'w' };
  return null;
}

/**
 * 全 SKU から f_rakuten_sku_map の行を組み立てる。
 * @param {Array<object>} skus  /items/all-skus の skus (manageNumber を含む)
 * @param {Map<string,string>} productMap
 * @returns {{mappings: Map<string, {ne_code:string, source:string, priority:number, manage_number:string|null}>,
 *            resolvedCount:number, unresolvedCount:number, withoutManageNumber:number}}
 */
export function buildMappings(skus, productMap) {
  const mappings = new Map();   // rakuten_code → { ne_code, source, priority, manage_number }
  let resolvedCount = 0;
  let unresolvedCount = 0;
  let withoutManageNumber = 0;

  for (const sku of skus) {
    const result = resolveSku(sku, productMap);
    if (!result) { unresolvedCount++; continue; }
    resolvedCount++;

    const am = (sku.systemSkuNumber || '').toLowerCase();
    const al = (sku.skuManageNumber || '').toLowerCase();
    const w  = (sku.itemNumber || '').toLowerCase();
    // 商品管理番号はそのまま (楽天の規約で小文字英数のみ。加工せず API に渡せる形で持つ)
    const manageNumber = String(sku.manageNumber || '').trim() || null;
    if (!manageNumber) withoutManageNumber++;

    // 解決で使われたコードは確実に登録 (権威あり)。
    // それ以外のコードも同じ ne_code に対応付ける (任意のコードから引けるように)
    const candidates = [];
    if (am) candidates.push({ code: am, src: 'am' });
    if (al && !INVALID_AL.has(al)) candidates.push({ code: al, src: 'al' });
    if (w) candidates.push({ code: w, src: 'w' });

    for (const c of candidates) {
      const existing = mappings.get(c.code);
      const newPriority = PRIORITY[c.src];
      if (!existing || newPriority < existing.priority) {
        mappings.set(c.code, { ne_code: result.ne_code, source: c.src, priority: newPriority, manage_number: manageNumber });
      }
    }
  }
  return { mappings, resolvedCount, unresolvedCount, withoutManageNumber };
}
