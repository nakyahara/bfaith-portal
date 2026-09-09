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
 * ★どの NE 商品かを**決める**のは AM → (空欄なら) W だけ (2026-09-09 中原さん指示)。
 *   AL は「引くためのキー」にしか使わない。詳しくは resolveSku のコメント。
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
 * 索引だけに使うコード。
 * 🚨 `al` はここにしか出てこない = **どの NE 商品かを決めるのには使わない**。
 *    行の `source` が 'al' なら「AL から引ける行」という意味で、
 *    「AL で商品を決めた」ではない (2026-09-09 にルールを変えた。resolveSku を見よ)
 */
export const INDEX_ONLY_SOURCES = new Set(['al']);

/**
 * 1 SKU を NE 商品コードに解決する。
 *
 * 🚨 **業務ルール (中原さん 2026-09-09)**:
 *    「システム連携用SKU番号と紐づけて。システム連携用SKU番号が空欄なら商品番号と紐づけて」
 *    = **AM (systemSkuNumber) → 空欄なら W (itemNumber / 商品番号)**。
 *
 * 🚨 **AL (SKU管理番号) は解決に使わない**。2026-09-09 に実害が出た:
 *    商品ページ `treemuddler200` は 商品番号 `treemuddler100-2` に紐づけるべきなのに、
 *    AL が商品管理番号と同じ `treemuddler200` で、**たまたま同名の別 NE 商品**に文字列一致し、
 *    その原価 (¥330) が想定利益に使われていた。AL は楽天が自動採番することがあり、
 *    「NE の商品コードと一致した = 同じ商品」とは言えない。
 *    ※ 引くためのキー (索引) としては AL も登録する。決めるのに使わない、という話
 *
 * 🚨 **AM が入っているのに当たらないときは W へ落とさない**。ルールの条件は「空欄なら」であって
 *    「当たらなければ」ではない。落とすと、また別商品の原価を静かに拾う。
 *    当たらない = NE 側の登録が要る、なので未解決のまま返して件数で見えるようにする
 *
 * @param {{systemSkuNumber?:string, skuManageNumber?:string, itemNumber?:string}} sku
 * @param {Map<string,string>} productMap 小文字の商品コード → 正本表記の商品コード
 * @returns {{ne_code:string, resolution:'am'|'w'}|{ne_code:null, reason:'am_unmatched'|'w_unmatched'}}
 */
export function resolveSku(sku, productMap) {
  const am = (sku.systemSkuNumber || '').toLowerCase();
  const w  = (sku.itemNumber || '').toLowerCase();

  if (am) {
    if (productMap.has(am)) return { ne_code: productMap.get(am), resolution: 'am' };
    return { ne_code: null, reason: 'am_unmatched' };
  }
  if (w && productMap.has(w)) return { ne_code: productMap.get(w), resolution: 'w' };
  return { ne_code: null, reason: 'w_unmatched' };
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
  // 🚨 「当たらなかった理由」を数える。AM が入っているのに NE に無い (= 登録が要る) 件数が
  //    見えないと、ルールを厳しくした影響が分からないまま件数だけ減る
  const byResolution = {};
  const unresolvedByReason = {};

  for (const sku of skus) {
    const result = resolveSku(sku, productMap);
    if (!result || !result.ne_code) {
      unresolvedCount++;
      const reason = result?.reason || 'unknown';
      unresolvedByReason[reason] = (unresolvedByReason[reason] || 0) + 1;
      continue;
    }
    resolvedCount++;
    byResolution[result.resolution] = (byResolution[result.resolution] || 0) + 1;

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
  return { mappings, resolvedCount, unresolvedCount, withoutManageNumber, byResolution, unresolvedByReason };
}
