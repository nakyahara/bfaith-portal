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
 * 🚨 ここは configured = **次に prepare する予定** (コードに書いた「こうしたい」) だけ。持ち主の正は DB の epoch (active)。
 *    このコードが company として扱えるキー (capable) と持ち主の読み方の版 (protocol) は config/master-capability.mjs (広げる道 PR-0)。
 *    configured の company は capable の中だけ (試験 apps/company-db/test-master-capability.mjs・prepare も断る)。
 *    これを変えてデプロイしても夜間ロード・写し・古い入口の門・画面の保存は変わらない
 *    (夜間ロード・写しは Company DB の epoch = ops.master_ownership_state の active を、古い入口の門は active ∪ prepared を見る。apps/company-db/load/ownership-state.mjs。
 *     画面の保存・新商品・NE 登録の CSV・Amazon は広げる道 PR-2 (#1640・protocol 2) から書く取引の中で DB の active を読む = lib/master-owner-gate.mjs)。
 *    configured を読むのは prepare / prepare --widen (master-ownership-epoch.mjs) が記録する持ち主表と、証跡に出すハッシュ (configured_hash)。
 *    🚨 ただしこのファイルは夜間ロードの規則の指紋 (engine.mjs の LOAD_RULE_FILES) に入る = 変えると夜間ロードの記録・照合 ①・widen の試みの指紋が変わる。
 *    夜間ロード (Render) とその朝の照合 ① (miniPC) は同じ build でなければならない = 朝の照合の後〜次の夜間ロードの前に両方へ配り、build と configured_hash を両方で確かめる (revert も同じ・Codex #1646 R1)。
 *    切替の日に人が readiness → prepare → frozen → 書きかけ 0 → 最後の active (全部 load) のロード (prepare をまたいだ古い書き込みの回収) → そのロードの run_id の report の成功 + 照合 ② →
 *    --use-prepared のロード → 写し・確かめ → activate で active にする (AI_reference 17 §4.2 の表が正・#1610)。
 * 🆕 2026-10-05 の切替 (中原さんの決定 10/4・10 §13) で C にした 13 キー = 下の 'company' (skus.sku_kind を除く)。写せない・手当ての PR が無い列 (products.parent・
 *    sku_components・listing_components.amazon・suppliers の 4 つ) は 'load' のまま (⑦-2・④b の後)
 * 🆕 2026-10-07 skus.sku_kind = 'company' (広げる道の手順の 1 = configured。中原さんの決定 10/6)。扱いのコードは #1641 (capable)・DB は 0058・画面と門は #1640 で配り済み。
 *    これを Render と miniPC の両方に配ってから `master-ownership-epoch.mjs prepare --widen --company 1` (足すキー = configured の company − active = skus.sku_kind だけ)
 *    → … → widen で DB の active に入った時に初めて切り替わる。配ってから widen までの間は DB の active (sku_kind = load) のまま = 今までの動き
 *    (以前の「ここだけ書き換えて配らない」は protocol 1 の画面が配った config を DB の記録と照らしていた頃の注意。protocol 2 では当てはまらない)。
 *    company になったときの動き:
 *    夜間ロード =既にある SKU の区分を NE に合わせない・NE と区分が違う SKU は conflicts (sku_kind_held)・判断の記録 (decisions.skus.kind_held) に残し、
 *    商品の行・束ねの親・セットの構成・構成の観測は社内の区分で決める・区分の最終形の正規化 (load でも) /
 *    写し = 区分も m_products.商品区分 に写す (C 単品・NE セット = 単品として・ほかの食い違い = 前の行のまま。master-publish.js の PUBLISH_COLUMNS.kind) /
 *    照合 ② = 区分の差を判断の一覧に (company_owned・NE は NE の画面で)・生の区分差の数 (sku_kind_raw_mismatch)
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
  'products.name', 'products.sales_class', 'products.status', 'products.parent',
  'skus.name', 'skus.sku_kind', 'skus.tax_rate', 'skus.tax_class', 'skus.handling',
  'sku_costs', 'sku_components', 'listing_components.amazon',
  'suppliers.name', 'suppliers.order_method', 'suppliers.lead_time_days',
  // 0027 (②c-2)
  'skus.standard_price', 'skus.shipping', 'skus.reorder_months', 'suppliers.contacts', 'supplier_skus.is_primary',
  // 0053 (⑤-2b・契約 v3 H6): 商品の JAN (core.external_ids の system = 'jan')
  'external_ids.jan',
]);

export const MASTER_OWNERSHIP = Object.freeze({
  // 単品の商品 (core.products)
  'products.name': 'company',
  'products.sales_class': 'company',
  'products.status': 'company',          // 単品の取扱中 / 中止。バリエーションの名札の状態 (子から決める) もこれに従う
  'products.parent': 'load',          // 代表関係 (親子 = 色違い・サイズ違いの名札)。'company' なら夜間ロードは名札を作らず、親を付けない・変えない・外さない (D3)
  // SKU (core.skus)
  'skus.name': 'company',
  'skus.sku_kind': 'company',            // 区分 (単品 / セット / 例外)。広げる道 (prepare --widen → widen) で DB の active に入れる。'company' なら夜間ロードは既にある SKU の区分を NE に合わせない
  'skus.tax_rate': 'company',
  'skus.tax_class': 'company',
  'skus.handling': 'company',
  // 行ごと
  'sku_costs': 'company',                // 原価 (有効期間の付け替え)。'company' なら夜間ロードは原価の行を作らない・閉じない
  'sku_components': 'load',           // セット構成。'company' なら夜間ロードは構成を足さない・直さない・消さない (manual は今も守られる)
  'listing_components.amazon': 'load',// Amazon SKU ↔ NE コード (FBA のマップ。D-43)。'company' なら SKU マスタ・Sheet の構成は材料にしない (FBM の完全一致は対応の無い出品にだけ続ける・
                                      //   出品そのもの・ASIN・FNSKU は続ける)。対応 (core.amazon_sku_maps・0054) がある出品は持ち主によらず触らない (16 §7 M10・⑦-1)
  // 仕入先 (core.suppliers)
  'suppliers.name': 'load',
  'suppliers.order_method': 'load',
  'suppliers.lead_time_days': 'load',
  // 0027 (②c-2) で足した列
  'skus.standard_price': 'company',      // 標準売価 (standard_price_jpy)
  'skus.shipping': 'company',            // 自社の計算用の送料 (shipping_code / shipping_method / shipping_cost_jpy をまとめて)
  'skus.reorder_months': 'company',      // 推奨保有月数。商品管理リストの公開 snapshot が使えない日は 'load' でも触らない
  'suppliers.contacts': 'load',       // 連絡先 6 列 (email_to / email_cc / contact_name / fax_number / relay_to / order_memo をまとめて)
  'supplier_skus.is_primary': 'company', // 代表の仕入先 (NE の商品の仕入先コード)。コードが空の商品は触らない
  // 0053 (⑤-2b) で足した列
  'external_ids.jan': 'company',         // 商品の JAN。'company' なら夜間ロードは商品の JAN を足さない・外さない (JAN の観測と解決の記録は続ける)
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
