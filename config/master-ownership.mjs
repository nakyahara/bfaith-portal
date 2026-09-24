/**
 * master-ownership.mjs — 商品・仕入先マスタの「列ごとの持ち主」(Company DB構想 10 §3・§5.2)
 *
 * 夜間ロード (apps/company-db/nightly.mjs → load/engine.mjs) は毎晩、SQLite の写し (NE の商品 + /register の上書き表 +
 * 発注アプリ) から Company DB を作り直す。このままでは Company DB で直した値が翌朝に戻る。
 * ここで「その列を夜間ロードが直してよいか」を 1 か所で決める。
 *
 *   'load'    = 夜間ロードが SQLite の値に合わせる (= 2026-09-24 までの動き)
 *   'company' = Company DB が正。夜間ロードは **既にある行を上書きしない** (新しく見つかった行にだけ最初の値を入れる)。
 *               空欄を埋める (coalesce) こともしない = わざと消した値が翌朝に戻らない (Codex R2)
 *
 * 🚨 切替日 (10 §8。人の入力を一斉にポータルへ移す日) までは **全部 'load'** のまま。切替日に対象の列をまとめて 'company' にする。
 * 🚨 列を足すときは engine.mjs でその列を実際に見ているかを確かめる (ここに書いただけでは効かない)。知らないキーは起動時に落とす。
 *
 * ここに無いもの:
 *   - 仕入先ごとの先方品番・入数・ロット・発注条件 (supplier_skus) = 発注アプリ (purchase-orders) が正 (D-44)。夜間ロードは発注アプリの値に合わせ続ける
 *   - 在庫・引当・受注・出荷 = NE / ロジザードが正 (Company DB は写し)
 */
export const OWNERS = Object.freeze(['load', 'company']);

/**
 * engine.mjs が実際に見ている列の一覧 (= 書いてよいキー)。🚨 MASTER_OWNERSHIP とは別に持つ:
 * MASTER_OWNERSHIP 自身を「正しいキーの一覧」にすると、設定に typo のキー ('products.nmae': 'company') を足しても
 * 検査を通り、本物の 'products.name' は 'load' のまま = 守ったつもりの列が上書きされる (Codex PR #1440 R1 Medium)。
 * engine.mjs の loadOwns('…') とこの一覧が一致することは test-master-ownership.mjs が機械で見る
 */
export const OWNED_COLUMNS = Object.freeze([
  'products.name', 'products.sales_class', 'products.status',
  'skus.name', 'skus.sku_kind', 'skus.tax_rate', 'skus.tax_class', 'skus.handling',
  'sku_costs', 'sku_components', 'listing_components.amazon',
  'suppliers.name', 'suppliers.order_method', 'suppliers.lead_time_days',
]);

export const MASTER_OWNERSHIP = Object.freeze({
  // 単品の商品 (core.products)
  'products.name': 'load',
  'products.sales_class': 'load',
  'products.status': 'load',          // 単品の取扱中 / 中止。バリエーションの名札の状態 (子から決める) もこれに従う
  // SKU (core.skus)
  'skus.name': 'load',
  'skus.sku_kind': 'load',
  'skus.tax_rate': 'load',
  'skus.tax_class': 'load',
  'skus.handling': 'load',
  // 行ごと
  'sku_costs': 'load',                // 原価 (有効期間の付け替え)。'company' なら夜間ロードは原価の行を作らない・閉じない
  'sku_components': 'load',           // セット構成。'company' なら夜間ロードは構成を足さない・直さない・消さない (manual は今も守られる)
  'listing_components.amazon': 'load',// Amazon SKU ↔ NE コード (FBA のマップ。D-43)。'company' なら Amazon の出品の構成に触らない (出品そのもの・ASIN・FNSKU は続ける)
  // 仕入先 (core.suppliers)
  'suppliers.name': 'load',
  'suppliers.order_method': 'load',
  'suppliers.lead_time_days': 'load',
});

/** 知らないキー・知らない値を落とす (typo で「守ったつもり」を作らない) */
export function validateOwnership(ownership = MASTER_OWNERSHIP) {
  const problems = [];
  const known = new Set(OWNED_COLUMNS);   // 設定そのものではなく、独立した一覧で見る
  for (const [k, v] of Object.entries(ownership || {})) {
    if (!known.has(k)) problems.push(`知らない列: ${k}`);
    if (!OWNERS.includes(v)) problems.push(`${k} の持ち主が不正: ${v} ('load' か 'company')`);
  }
  for (const k of OWNED_COLUMNS) if (!Object.prototype.hasOwnProperty.call(ownership || {}, k)) problems.push(`持ち主が書かれていない列: ${k}`);
  if (problems.length) throw Object.assign(new Error(`master-ownership: ${problems.join(' / ')}`), { code: 'OWNERSHIP_INVALID' });
  return ownership;
}

/** 起動時にも検査する (config を壊したまま夜間ロードが走らないように) */
validateOwnership(MASTER_OWNERSHIP);

/** 夜間ロードがこの列を直してよいか */
export const loadOwns = (ownership, key) => {
  if (!OWNED_COLUMNS.includes(key) || !Object.prototype.hasOwnProperty.call(ownership, key)) throw Object.assign(new Error(`master-ownership: 知らない列 ${key}`), { code: 'OWNERSHIP_INVALID' });
  return ownership[key] === 'load';
};

/** Company DB が正になっている列の一覧 (report に残す) */
export const companyOwned = (ownership = MASTER_OWNERSHIP) => Object.keys(ownership).filter((k) => ownership[k] === 'company');
