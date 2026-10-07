/**
 * master-legacy-entries.mjs — 商品・仕入先マスタの「古い入口」の一覧 (Company DB構想 14 §5・§9 v2 M2・§10 契約 v3 H1 / 10 §4 / PR #1565 Codex R1)
 *
 * 古い入口 = NE の写し (warehouse.db の上書き表・Render の mirror_products・product-hub の税率・profit-calculator の JSON・
 * 発注アプリと売れ筋共有の仕入先) に人がマスタを書く API・画面・手で流す CLI・人の操作で動く取込。
 * 切替の段階 (ops.master_cutover_state・lib/master-cutover.mjs) が legacy_open のあいだは全部書ける。frozen 以降は
 * 🆕 ⑤-3b (2026-10-04): **その入口が書く列 (owner_cols) のどれかの持ち主が C の入口だけ**閉じる (owner_match = 'all' = 列が全部 C のときだけ閉じる)。
 *   持ち主 = Company DB の epoch (ops.master_ownership_state の active と prepared の C の列。config/master-ownership.mjs = configured は見ない)。
 *   列を分けて切り替える (10/5 は 13 キー) ため: C にしない列 (Amazon SKU の対応・仕入先・セットの構成など) の入口まで閉じると業務が止まる
 * 🚨 段階を読めない・(legacy_open 以外で) 持ち主を読めない = 書けない側 (fail-closed)。lib/master-legacy-gate.mjs が毎回読む
 * owner_cols = その入口が書く (書き換える・外へ出す) 持ち主表のキー (config/master-ownership.mjs の OWNED_COLUMNS)。全部の入口に要る (起動時に落とす)
 *
 * この一覧がそのまま門の設定 (lib/master-legacy-gate.mjs の masterLegacyGate(app) が app ごとに読む)。
 * scripts/test-master-legacy-entries.mjs がルートの定義と関数の呼び出しをたどって「マスタの表・列に書くのに一覧に無い口」を落とす
 * (新しい入口を黙って足せないように)。この一覧を機械で読める形にした manifest の sha256 (lib/master-legacy-gate.mjs の manifestHash) が
 * 門の記録 (ops.master_legacy_gate_acks.manifest_hash・⑤-1) に入る
 *
 * kind:
 *   route        = API。閉じたら 410 (段階が読めない = 503)。when = 道の一部の値で絞る (例: /api/masters/:kind の kind = suppliers だけ)
 *   route_field  = API のうち、その欄 (field) を送ったときだけ閉じる (ほかの欄の保存は今までどおり)
 *   route_part   = API は通すが、マスタの部分 (part) だけ書かない (handler が res.locals.masterLegacyWrite.writable のときだけ書く)
 *   screen       = 画面。帯「マスタは新しい画面で直します ↗」+ 書く部品を隠す (res.locals.masterLegacy)
 *   cli          = 手で流す CLI。mode ごと ('*' = そのファイルの全部の mode)。何もしないで終了コード 3
 *   job          = 定期の取込。毎回段階を読み、閉じていれば丸ごと止める (ログを残す)
 *   recheck      = true = 時間のかかる取込。書く直前にもう一度読む (legacyRecheck / CLI は 2 回目の legacyCliGate)
 * host: minipc (社内の WarehouseServer と CLI) / render (bfaith-portal.onrender.com)
 */
import { OWNED_COLUMNS } from './master-ownership.mjs';

export const MASTER_EDIT_URL = 'https://bfaith-portal.onrender.com/apps/master-edit/';

/** 閉じたときの動き (機械で読む値 → 人が読む説明) */
export const WHEN_FROZEN = Object.freeze({
  http_410: 'API は 410 {error:"master_frozen", message, url}。段階が読めない = 503 {error:"master_phase_unreadable", message, url}。何も書かない',
  field_410: 'その欄を送ったときだけ 410 / 503 (ほかの欄だけの保存は通る)。何も書かない',
  part_skip: 'API は通すが、マスタの部分 (税率の初期値・写し) は書かない',
  banner: '画面は「マスタは新しい画面で直します ↗」の帯を出し、書く部品を隠す (見るだけ)',
  exit_nonzero: 'CLI は何も書かないで終了コード 3 (段階が読めないときも 3)',
  job_skip: '定期の取込を丸ごと止める (閉じている = ログと ok の ping・読めない = fail の ping)',
  new_entry_409: '新商品の古い作り方: 作れる種類が全部閉じた = 409 {error:"master_new_entry_moved", kinds, message, url: 新しい新商品の画面}・段階 / 持ち主が読めない = 503。何も書かない。'
    + '入口が開いていても、閉じた種類 (例: 単品) を作る要求は handler が 409 (refuseLegacyNewKind)',
});

const R = (o) => Object.freeze({ kind: 'route', when_frozen: 'http_410', ...o });
/**
 * 新商品を作る古い道 (product-hub の下書き・NE のコードから一括・自動取込) の列 = 新しい「新商品の登録」(lib/master-register.mjs の NEW_ENTRY_KEYS の
 * 単品とセットを合わせたもの。試験が同じことを確かめる)。owner_match = 'all' = 全部 C になって新しい登録で単品もセットも作れるようになるまで、古い道は開けたまま
 */
export const NEW_PRODUCT_COLS = Object.freeze(['products.name', 'products.sales_class', 'products.status', 'sku_components', 'sku_costs', 'skus.handling', 'skus.name',
  'skus.reorder_months', 'skus.shipping', 'skus.sku_kind', 'skus.standard_price', 'skus.tax_class', 'skus.tax_rate', 'supplier_skus.is_primary']);
/**
 * 🆕 広げる道 PR-6 (2026-10-06・設計 v3 G17): 新商品の古い作り方の「種類ごとの門」。種類 (単品 / セット) ごとに、新しい登録 (lib/master-register.mjs の
 * NEW_ENTRY_KEYS) のその種類の列が**全部 C** になったら、その種類の作成だけ閉じる (試験が NEW_ENTRY_KEYS と同じことを確かめる)。
 *   単品 = 13 キー (10/4 の C の 12 キー + skus.sku_kind)。skus.sku_kind を C に広げた瞬間 (prepare から = active ∪ prepared) に単品の作成が閉じる
 *   セット = sku_components も要る = sku_components を広げるまで今までどおり
 * 入口の new_kinds = その入口が作れる種類。owner_cols = その種類の列を合わせたもの・owner_match = 'all' (= 全部の種類が閉じたときだけ入口ごと閉じる)。
 * 入口ごと開いている間は、handler が作る種類を決めて refuseLegacyNewKind (lib/master-legacy-gate.mjs) で確かめる (閉じた種類 = 409 + 新しい画面へ案内)
 */
export const NEW_KIND_COLS = Object.freeze({
  single: Object.freeze(['skus.name', 'products.name', 'skus.sku_kind', 'skus.tax_rate', 'skus.tax_class', 'skus.handling', 'products.status', 'products.sales_class',
    'skus.standard_price', 'skus.shipping', 'skus.reorder_months', 'supplier_skus.is_primary', 'sku_costs']),
  set: Object.freeze(['skus.name', 'skus.sku_kind', 'skus.tax_rate', 'skus.tax_class', 'skus.handling', 'products.sales_class',
    'skus.standard_price', 'skus.shipping', 'skus.reorder_months', 'sku_costs', 'sku_components']),
});
export const NEW_KIND_LABELS = Object.freeze({ single: '単品', set: 'セット' });
/** 新しい「新商品の登録」の画面 (種類が閉じたときの案内先) */
export const NEW_ENTRY_URL = `${MASTER_EDIT_URL}new`;
/** 入口が作れる種類 (new_kinds) の列を合わせたもの (owner_cols の決め方。起動時の検査と試験が同じ関数を使う) */
export function newKindCols(kinds) {
  return [...new Set(kinds.flatMap((k) => NEW_KIND_COLS[k] || []))];
}
/** profit-calculator の NE 用 CSV (NE の商品マスタの一括取込) が書く列 = 商品名 (syohin_name)・仕入先 (sire_code)・原価 (genka_tnk)・売価 (baika_tnk)・代表コード (daihyo_syohin_code)・セットの数量 (suryo) */
const NE_CSV_COLS = Object.freeze(['skus.name', 'products.name', 'sku_costs', 'skus.standard_price', 'supplier_skus.is_primary', 'products.parent', 'sku_components']);
/** 発注アプリの仕入先の行 (po_suppliers) の全部の列 = 名前・発注方法 (send_method)・リードタイム (lead_days)・連絡先 6 列 (order_memo を含む)。Codex #1610 R1 Medium */
const PO_SUPPLIER_COLS = Object.freeze(['suppliers.name', 'suppliers.order_method', 'suppliers.lead_time_days', 'suppliers.contacts']);
const S = (o) => Object.freeze({ kind: 'screen', when_frozen: 'banner', method: 'GET', ...o });

/** 閉じる入口 (段階 frozen / company_owner / new_open で owner_cols の列の持ち主が C のとき・段階か持ち主が読めないとき) */
export const LEGACY_ENTRIES = Object.freeze([
  // ─── 10 §4 #2: miniPC のマスタ登録 /apps/warehouse/register (上書き表 + m_products) ───
  ...[
    ['POST', '/api/shipping', ['product_shipping', 'm_products'], ['skus.shipping']],
    ['POST', '/api/genka', ['exception_genka', 'm_products'], ['sku_costs']],
    ['POST', '/api/csv/shipping', ['product_shipping', 'm_products'], ['skus.shipping'], true],
    ['POST', '/api/csv/genka', ['exception_genka', 'm_products'], ['sku_costs'], true],
    ['POST', '/api/csv/m-sku-master', ['m_sku_master', 'm_sku_components'], ['listing_components.amazon'], true],
    ['DELETE', '/api/shipping/:sku', ['product_shipping'], ['skus.shipping']],
    ['DELETE', '/api/genka/:sku', ['exception_genka'], ['sku_costs']],
    ['DELETE', '/api/sales_class/:sku', ['product_sales_class', 'm_products'], ['products.sales_class']],
    ['DELETE', '/api/tax_rate/:sku', ['product_tax_rate', 'm_products'], ['skus.tax_rate', 'skus.tax_class']],
    ['POST', '/api/sales_class', ['product_sales_class', 'm_products'], ['products.sales_class']],
    ['POST', '/api/csv/sales_class', ['product_sales_class', 'm_products'], ['products.sales_class'], true],
    ['POST', '/api/reorder_setting', ['m_reorder_setting'], ['skus.reorder_months']],
    ['DELETE', '/api/reorder_setting/:sku', ['m_reorder_setting'], ['skus.reorder_months']],
    ['POST', '/api/csv/reorder_setting', ['m_reorder_setting'], ['skus.reorder_months'], true],
    ['POST', '/api/tax_rate', ['product_tax_rate', 'm_products'], ['skus.tax_rate', 'skus.tax_class']],
    ['POST', '/api/csv/tax_rate', ['product_tax_rate', 'm_products'], ['skus.tax_rate', 'skus.tax_class'], true],
  ].map(([method, p, writes, cols, recheck]) => R({ id: `warehouse:${method}:${p}`, app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', method, path: p, writes, owner_cols: cols, ref: '10 §4 #2', ...(recheck ? { recheck: true } : {}) })),
  // 同じ router に載る SKU マスタ (Amazon SKU ↔ NE コード) の API (D-43: SKU タブも閉じる)
  ...[['POST', '/api/m-sku-master'], ['PUT', '/api/m-sku-master/:sku'], ['DELETE', '/api/m-sku-master/:sku']]
    .map(([method, p]) => R({ id: `warehouse:${method}:${p}`, app: 'warehouse', host: 'minipc', file: 'apps/warehouse/sku-master-api.js', mount: '/apps/warehouse', method, path: p, writes: ['m_sku_master', 'm_sku_components'], owner_cols: ['listing_components.amazon'], ref: '10 §4 #2 (D-43)' })),
  // 画面は書く部品を列ごとに隠す (apps/warehouse/router.js の REGISTER_WRITE_PARTS = SKU タブ (Amazon SKU の対応) とそれ以外を分ける・⑤-3b)
  S({ id: 'warehouse:screen:/register', app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', path: '/register', owner_cols: ['skus.shipping', 'sku_costs', 'products.sales_class', 'skus.tax_rate', 'skus.tax_class', 'skus.reorder_months', 'listing_components.amazon'], ref: '10 §4 #2' }),
  S({ id: 'warehouse:screen:/', app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', path: '/', owner_cols: ['skus.shipping', 'sku_costs'], ref: '10 §4 #2 (データウェアハウスの画面の送料・原価)' }),

  // ─── 10 §4 #4: Render の会計アプリ 5 つの POST /register (mirror_products の税率・売上分類。翌朝の入れ替えで消える) ───
  ...['aupay-accounting', 'yahoo-accounting', 'mercari-accounting', 'linegift-accounting', 'qoo10-accounting'].flatMap((app) => [
    R({ id: `${app}:POST:/register`, app, host: 'render', file: `apps/${app}/router.js`, mount: `/apps/${app}`, method: 'POST', path: '/register', writes: ['mirror_products'], owner_cols: ['skus.tax_rate', 'skus.tax_class', 'products.sales_class'], ref: '10 §4 #4' }),
    S({ id: `${app}:screen:/`, app, host: 'render', file: `apps/${app}/router.js`, mount: `/apps/${app}`, path: '/', owner_cols: ['skus.tax_rate', 'products.sales_class'], ref: '10 §4 #4' }),
  ]),

  // ─── 10 §4 #5: fba-profitability の原価の手入力 (mirror_products。翌朝消える) ───
  R({ id: 'fba-profitability:POST:/api/update-cost', app: 'fba-profitability', host: 'render', file: 'apps/fba-profitability/router.js', mount: '/apps/fba-profitability', method: 'POST', path: '/api/update-cost', writes: ['mirror_products'], owner_cols: ['sku_costs'], ref: '10 §4 #5' }),
  S({ id: 'fba-profitability:screen:/', app: 'fba-profitability', host: 'render', file: 'apps/fba-profitability/router.js', mount: '/apps/fba-profitability', path: '/', owner_cols: ['sku_costs'], ref: '10 §4 #5' }),

  // ─── 10 §4 #6: product-hub の税率 (draft_yahoo.tax_rate)。閉じたら出品・画面・試算は Company DB の税率 (services/listing-tax.mjs) ───
  ...[['POST', '/api/drafts/:id'], ['POST', '/api/drafts/:id/yahoo']].map(([method, p]) => Object.freeze({
    id: `product-hub:${method}:${p}:tax_rate`, kind: 'route_field', field: 'tax_rate', when_frozen: 'field_410', app: 'product-hub', host: 'render',
    file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method, path: p, writes: ['draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #6',
  })),
  // Notion の取込 (Notion の税率を draft_yahoo.tax_rate に書く) = 閉じる (R1 H4)
  // dry_run = 書かない試し (プレビュー) の見分け方。段階を読めないときだけ、試しは注意つきで通す (閉じた後は 410。中間レビュー 2 回目 Low)
  //   'body_true' = 本文の dry_run が true のときだけ試し / 'body_not_false' = 本文の dry_run が false でなければ試し (既定が試し)
  ...[['POST', '/api/notion-import'], ['POST', '/api/notion-import-by-status', 'body_not_false']].map(([method, p, dryRun]) => R({
    id: `product-hub:${method}:${p}`, app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method, path: p,
    ...(dryRun ? { dry_run: dryRun } : {}), writes: ['draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #6 (PR #1565 R1 H4)',
  })),
  // 古い新商品の作り方 (人が下書きを作る /new・NE のコードから一括登録・NE が先の自動取込を手で回す・Notion の画像 DB の移植) = 種類ごとの門 (広げる道 PR-6・G17):
  //   その種類 (new_kinds) の新しい登録の列 (NEW_KIND_COLS) が全部 C (active ∪ prepared) になったら、その種類の作成だけ閉じる (409 + 新しい新商品の画面へ)。
  //   全部の種類が閉じたら入口ごと閉じる (owner_cols = 種類の列を合わせたもの・owner_match 'all'。10 §4 #11・Codex ⑤-2a M5)。
  //   POST /api/drafts・NE のコードから一括 = 単品もセットも作れる (NE のセットのコードならセット・それ以外 = 単品) = 開いている間は handler が種類を決めて確かめる。
  //   自動取込を手で回す = NE の単品だけ (new-product-intake.js の selectCandidates)・Notion の画像 DB の移植 = 自社の商品ページ (種類を見ない) = 単品として閉じる (閉じる側)
  //   10/6 の時点 (sku_kind・sku_components が load) = 全部開いたまま。
  // 税率 (NE の初期値) も draft_yahoo に書くが、税率が C の間は出品・画面・試算が Company DB の税率を使う (services/listing-tax.mjs) = 古い値は使われない
  ...[['POST', '/api/drafts', null, ['single', 'set']], ['POST', '/api/register-codes', 'body_true', ['single', 'set']], ['POST', '/api/intake/run', 'body_true', ['single']],
    ['POST', '/api/notion-image-import', 'body_not_false', ['single']]].map(([method, p, dryRun, kinds]) => R({
    id: `product-hub:${method}:${p}`, app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method, path: p, when_frozen: 'new_entry_409',
    ...(dryRun ? { dry_run: dryRun } : {}), writes: ['product_drafts (新商品)', 'draft_yahoo.tax_rate'], new_kinds: Object.freeze([...kinds]), owner_cols: Object.freeze(newKindCols(kinds)), owner_match: 'all',
    ref: '10 §4 #11 (Codex ⑤-2a M5)・#6 (PR #1565 R1 H4)・⑤-3b・広げる道 PR-6 (種類ごとの門)',
  })),
  // セットを作る (企画中のセット・仮コード) = 作るのは通す、親の税率だけ写さない (R1 H4)
  Object.freeze({
    id: 'product-hub:POST:/api/drafts/:id/set-drafts:tax_rate', kind: 'route_part', part: 'tax_rate', when_frozen: 'part_skip', app: 'product-hub', host: 'render',
    file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method: 'POST', path: '/api/drafts/:id/set-drafts', writes: ['draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #6 (PR #1565 R1 H4)',
  }),
  S({ id: 'product-hub:screen:/detail/:id', app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', path: '/detail/:id', owner_cols: ['skus.tax_rate'], ref: '10 §4 #6 (税率の欄 = Company DB の税率を見せるだけ)' }),
  // 画面: 全部の種類が閉じた = 帯 (登録のボタンを隠す)。一部の種類だけ閉じた = 案内 (例:「単品の新商品は新しい画面で」・ボタンは残す = セットは作れる)
  ...[['/new', '10 §4 #11 (登録のボタンを隠す)・⑤-3b・広げる道 PR-6 (単品だけ閉じた = 案内)'], ['/list', '広げる道 PR-6 (NE のコードから一括・自動取込のカードに種類の案内)']].map(([p, ref]) => S({
    id: `product-hub:screen:${p}`, app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', path: p,
    new_kinds: Object.freeze(['single', 'set']), owner_cols: Object.freeze(newKindCols(['single', 'set'])), owner_match: 'all', ref,
  })),
  // NE が先の新商品の自動取込 = NE の単品だけ (selectCandidates) = 単品の列が全部 C で丸ごと止める (Codex ⑤-2a M5・⑤-3b・広げる道 PR-6)
  Object.freeze({ id: 'job:product-hub:intake-cron', kind: 'job', when_frozen: 'job_skip', host: 'render', file: 'apps/product-hub/intake-cron.js', writes: ['product_drafts (新商品)', 'draft_yahoo.tax_rate'],
    new_kinds: Object.freeze(['single']), owner_cols: Object.freeze(newKindCols(['single'])), owner_match: 'all', ref: '10 §4 #11 (NE が先の新商品の自動取込。Codex ⑤-2a M5)・⑤-3b・広げる道 PR-6' }),

  // ─── NE への 2 つ目の出口 (14 §5) と profit-calculator の仕入先 (10 §4 #9・14 §9 M2) ───
  R({ id: 'profit-calculator:GET:/api/products/csv/ne', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'GET', path: '/api/products/csv/ne', writes: ['NE の商品 (CSV の出口)'], owner_cols: NE_CSV_COLS, ref: '14 §5 (NE への 2 つ目の出口 → マスタの判断の CSV へ)' }),
  R({ id: 'profit-calculator:POST:/api/suppliers', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'POST', path: '/api/suppliers', writes: ['suppliers.json'], owner_cols: ['suppliers.name'], ref: '10 §4 #9・14 §9 M2' }),
  R({ id: 'profit-calculator:DELETE:/api/suppliers', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'DELETE', path: '/api/suppliers', writes: ['suppliers.json'], owner_cols: ['suppliers.name'], ref: '10 §4 #9・14 §9 M2' }),
  ...[['/', 'index.html (仕入れ先の追加)', ['suppliers.name']], ['/research', 'list.html (仕入れ先の追加・変更)', ['suppliers.name']], ['/products', 'products.html (NE 用 CSV)', NE_CSV_COLS], ['/suppliers', 'suppliers.html (仕入れ先マスタ)', ['suppliers.name']]]
    .map(([p, note, cols]) => S({ id: `profit-calculator:screen:${p}`, app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', path: p, owner_cols: cols, ref: `10 §4 #9・14 §5 ${note}` })),

  // ─── 10 §4 #7: 発注アプリの仕入先 (po_suppliers)。仕入先の列 (suppliers.*) が C になったら閉じる (⑤-3b。10/5 は load = 開いたまま) = 切替の手順で書き込み先を Company DB に替えるまで仕入先は見るだけ (R1 H4) ───
  ...[['POST', '/api/masters/:kind'], ['DELETE', '/api/masters/:kind/:id'], ['POST', '/api/masters/:kind/csv']].map(([method, p]) => R({
    id: `purchase-orders:${method}:${p}:suppliers`, app: 'purchase-orders', host: 'render', file: 'apps/purchase-orders/router.js', mount: '/apps/purchase-orders',
    method, path: p, when: Object.freeze({ param: 'kind', in: Object.freeze(['suppliers']) }), writes: ['po_suppliers'], owner_cols: PO_SUPPLIER_COLS, ref: '10 §4 #7 (PR #1565 R1 H4)',
    ...(p.endsWith('/csv') ? { recheck: true } : {}),
  })),
  R({ id: 'purchase-orders:POST:/api/email/recipients/csv', app: 'purchase-orders', host: 'render', file: 'apps/purchase-orders/router.js', mount: '/apps/purchase-orders', method: 'POST', path: '/api/email/recipients/csv', writes: ['po_suppliers'], owner_cols: ['suppliers.contacts', 'suppliers.order_method'], ref: '10 §4 #7 (宛先の CSV。発注方法が空なら email を入れる = send_method も書く)', recheck: true }),
  R({ id: 'purchase-orders:POST:/api/import', app: 'purchase-orders', host: 'render', file: 'apps/purchase-orders/router.js', mount: '/apps/purchase-orders', method: 'POST', path: '/api/import', writes: ['po_suppliers'], owner_cols: PO_SUPPLIER_COLS, ref: '10 §4 #7 (マスタの一括取込。仕入先を含む = 🚨 仕入先の列が C になった後は仕入先でない種類のファイルもまとめて止まる = 仕入先でないマスタはマスタ管理の各タブの CSV で入れる)', recheck: true }),
  // ─── 10 §4 #8: 仕入先向け売れ筋共有の表示名 (supplier_share_master) ───
  R({ id: 'supplier-sales:POST:/api/supplier-name', app: 'supplier-sales', host: 'render', file: 'apps/supplier-sales/router.js', mount: '/apps/supplier-sales', method: 'POST', path: '/api/supplier-name', writes: ['supplier_share_master'], owner_cols: ['suppliers.name'], ref: '10 §4 #8 (PR #1565 R1 H4)' }),

  // ─── 10 §4 #10: 手で流す取込 (miniPC)。🚨 ファイル単位ではなく書く表 / mode 単位 (csv-import.js の受注・ロジザードなどは止めない) ───
  ...[['product_shipping', ['product_shipping'], ['skus.shipping']], ['exception_genka', ['exception_genka'], ['sku_costs']]].map(([mode, writes, cols]) => Object.freeze({
    id: `cli:csv-import.js:${mode}`, kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/csv-import.js', mode, writes, owner_cols: cols, recheck: true, ref: '10 §4 #10 (全部消して入れ直す)',
  })),
  Object.freeze({ id: 'cli:import-sales-class.js', kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/import-sales-class.js', mode: '*', writes: ['product_sales_class'], owner_cols: ['products.sales_class'], recheck: true, ref: '10 §4 #10' }),
  Object.freeze({ id: 'cli:import-sku-master.js', kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/import-sku-master.js', mode: '*', writes: ['m_sku_master', 'm_sku_components'], owner_cols: ['listing_components.amazon'], recheck: true, ref: '10 §4 #2・#10 (D-43)。--dry-run も止める (書く表がマスタだけのファイル)' }),
  Object.freeze({ id: 'cli:migrate-reorder-setting-initial.js', kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/migrate-reorder-setting-initial.js', mode: '*', writes: ['m_reorder_setting'], owner_cols: ['skus.reorder_months'], recheck: true, ref: '14 §9 M2。--dry-run も止める (書く表がマスタだけのファイル)' }),
]);

/**
 * 閉じない口 (理由の種類を決めて、試験が種類ごとに確かめる。R1: 「理由の文字があれば通す」をやめた)
 *   replication    = 写しの口 (人の入口ではない)。試験: そのルートの定義に guard (例 requireSyncKey) が付いている
 *   already_closed = 別の門で閉じ済み。試験: guard が router.use で全部の書き込みの前に掛かっている
 *   manual         = 機械では閉じられない入口 (NE の画面・GAS)。コードを持たない (file・method・path を持たない)。切替の証拠 manual_entries_stopped に載せる。
 *                    🆕 広げる道 PR-6: owner_cols (人が書くキー) が必須 = manifest に残る。広げるときは足すキーと重なる手の入口だけを止める (manualEntriesForKeys)
 *   seed_on_read   = 読むとき、ファイルが無い・空なら初期データ (コードの中の値) を書くだけ (人の入力ではない・あるファイルは変えない)。
 *                    試験: writer_file に guard (初期データを書く呼び出し) と existsSync (無いときだけ) がある
 *   company_db_outbox = Company DB の新しい登録の知らせを取り込む新しい道 (⑤-2a)。試験: writer_file に guard (MASTER_EDIT_OPEN の確かめ) がある
 */
export const LEGACY_EXEMPT = Object.freeze([
  Object.freeze({ id: 'warehouse-mirror:POST:/api/sync', kind: 'replication', host: 'render', file: 'apps/warehouse-mirror/router.js', method: 'POST', path: '/api/sync', guard: 'requireSyncKey', reason: 'miniPC → Render の写し (人の入口ではない。切替後は ④ の写しが同じ口で Company DB の値を運ぶ)' }),
  Object.freeze({ id: 'warehouse:render-writes', kind: 'already_closed', host: 'render', file: 'apps/warehouse/router.js', guard: 'rejectWritesOnRender', reason: '10 §4 #3 = Render では warehouse の書き込みは全部 409 (切替と関係なく閉じ済み)' }),
  Object.freeze({ id: 'profit-calculator:GET:/api/suppliers', kind: 'seed_on_read', host: 'render', file: 'apps/profit-calculator/router.js', method: 'GET', path: '/api/suppliers', writer_file: 'apps/profit-calculator/suppliers.js', guard: 'saveSuppliers(DEFAULT_SUPPLIERS)', reason: '仕入れ先の一覧を読むとき、suppliers.json が無い・空なら初期データ (DEFAULT_SUPPLIERS) を書くだけ。人の入力ではない・あるファイルは変えない (仕入れ先の追加・削除は閉じる入口)' }),
  Object.freeze({ id: 'product-hub:GET:/board:cdb-card-intake', kind: 'company_db_outbox', host: 'render', file: 'apps/product-hub/router.js', method: 'GET', path: '/board', writer_file: 'lib/product-hub-outbox.mjs', guard: 'if (!cardSweepEnabled()) return', reason: 'ボードを開いたときに、新しい「新商品の登録」(Company DB) の知らせから出品カードを作る = 新しい道 (古い入口ではない)。知らせは new_open の保存 (ops.register_new_sku) だけが書く・MASTER_EDIT_OPEN = 1 の Render だけ取り込む (⑤-2a)' }),
  // 🆕 広げる道 PR-6 (設計 v3 G12・Codex R2 M3): 手の入口も owner_cols (その入口で人が書く持ち主表のキー) を必ず持つ = manifest に残る。
  //   広げる (widen) とき、DB は「manifest の手の入口のうち、owner_cols が足すキーと重なるもの」= 止めた証拠の一覧と完全に同じか、を確かめる (PR-1)。
  //   🚨 少なく書く = 止めるべき人の入口を止めずに広げる (危ない側)。迷ったら入れる (広げるときに止める入口が増えるだけ)
  Object.freeze({
    id: 'ne:item-screen', kind: 'manual', host: 'ne',
    // NE の商品マスタの画面・セット商品の画面・NE の商品マスタの CSV 取込で人が書く値 (夜間ロードが mirror_products / mirror_set_components から読む列)。
    // 区分 (単品 / セット) は ne:set-kind に分けた (区分だけを広げるときに NE の画面全部を止めなくてよいように)
    owner_cols: Object.freeze(['products.name', 'products.sales_class', 'products.status', 'products.parent', 'skus.name', 'skus.tax_rate', 'skus.tax_class', 'skus.handling',
      'skus.standard_price', 'skus.shipping', 'sku_costs', 'sku_components', 'supplier_skus.is_primary', 'external_ids.jan']),
    reason: '10 §4 #1。機械では閉じられない = 運用で禁止・切替の証拠 (manual_entries_stopped) に担当者と止めた時刻 (契約 v3 H1)',
  }),
  Object.freeze({
    id: 'ne:set-kind', kind: 'manual', host: 'ne', owner_cols: Object.freeze(['skus.sku_kind']),
    // 区分が変わる NE の操作 = 「もうある単品と同じコードのセットを作る」「セットを消す」(mirror_products の 商品区分 が変わる = 夜間ロードの skus.sku_kind)。
    // ふつうのセットの構成の直し (sku_components) は ne:item-screen。広げた後 (C) の運用 = 区分を変えない・変えたいときは中原さんに (設計 v3 §4.1 f)
    reason: '広げる道 PR-6 (設計 v3 §4.1 e・G12)。skus.sku_kind を広げる前に止めて、止めた人と時刻を widen の証拠に載せる',
  }),
  Object.freeze({
    id: 'gas:logizard-sheet-and-sku-map', kind: 'manual', host: 'google',
    // #12 = ロジザード用 CSV の加工 (NE の商品名・バーコードをロジザードへ出す)・#13 = 商品コード変換テーブルの往復 (m_sku_master = Amazon SKU ↔ NE コード)
    owner_cols: Object.freeze(['listing_components.amazon', 'skus.name', 'external_ids.jan']),
    reason: '10 §4 #12・#13。⑥ で順番に止める・切替の証拠 (manual_entries_stopped) に載せる',
  }),
]);
/** 手の入口 (NE の画面・GAS)。owner_cols つき */
export const MANUAL_ENTRIES = Object.freeze(LEGACY_EXEMPT.filter((e) => e.kind === 'manual'));
/**
 * 足すキー (keys) から導いた「止める手の入口」の id (並べたもの)。owner_cols がキーと 1 つでも重なる手の入口 (widen の証拠と完全に同じ集合にする・PR-1)。
 * entries = manifest の entries (DB に残った形) も渡せる (kind = 'manual' の行だけ見る)。owner_cols が無い・配列でない手の入口 = 投げる (黙って外さない)
 */
export function manualEntriesForKeys(keys, entries = MANUAL_ENTRIES) {
  const want = new Set(keys);
  return entries.filter((e) => e.kind === 'manual').filter((e) => {
    if (!Array.isArray(e.owner_cols) || !e.owner_cols.length) throw new Error(`master-legacy-entries: 手の入口に owner_cols が無い: ${e.id}`);
    return e.owner_cols.some((k) => want.has(k));
  }).map((e) => e.id).sort();
}

/**
 * CLI のうち、マスタの表に書かないので止めない mode (試験が「全部の mode を数えた」か・実際に動くかを見る)
 */
export const CLI_KEEP_MODES = Object.freeze({
  'apps/warehouse/csv-import.js': Object.freeze({
    products: 'NE の商品の写し (raw_ne_products) = NE の観測',
    sets: 'NE のセットの写し (raw_ne_set_products) = NE の観測',
    orders: '受注',
    logizard: 'ロジザードの在庫',
    shipping_rates: '送料の表 (送料コード → 方法・金額)。商品ごとの値ではない',
  }),
});

/**
 * マスタの表・列 (試験が「ここに書く口は一覧にあるか」を数える)。
 *   tables  = SQL で書く表
 *   columns = 表の中の列だけマスタ (product-hub の税率)。match = その書き方 (関数の呼び方か SQL)
 *   files   = JSON のファイル (profit-calculator の仕入れ先)。match = そのファイルに書く文字
 * 🚨 試験は同じファイルの中の文字だけでなく、関数の呼び出しをたどって (ファイルをまたいで) 書く口を探す
 */
export const MASTER_WRITE_TARGETS = Object.freeze({
  tables: Object.freeze(['product_shipping', 'exception_genka', 'product_sales_class', 'product_tax_rate', 'm_reorder_setting', 'm_sku_master', 'm_sku_components', 'mirror_products', 'm_products', 'po_suppliers', 'supplier_share_master']),
  columns: Object.freeze([
    // writers = その表の汎用の書き手の関数。呼ぶ側にその列 (tax_rate) の文字があるときだけ「その列を書く」と数える
    Object.freeze({ id: 'draft_yahoo.tax_rate', table: 'draft_yahoo', column: 'tax_rate', writers: Object.freeze(['upsertDraftYahoo']) }),
  ]),
  files: Object.freeze([
    Object.freeze({ id: 'suppliers.json', match: 'SUPPLIERS_FILE' }),
  ]),
  /** 新商品を作る (古い新商品の作り方 = Codex ⑤-2a M5)。product-hub の下書き (product_drafts) を INSERT するかたまり */
  new_products: Object.freeze([
    Object.freeze({ id: 'product_drafts (新商品の下書き)', match: /INSERT\s+INTO\s+product_drafts\b/i }),
  ]),
  /**
   * 外への出口 (GET でも数える = 読むだけに見えて、NE のマスタを書き換えるファイルを作る。Codex #1565 R2 Low 4)。
   * NE の商品マスタの一括取込の CSV (syohin_code と 原価・売価・仕入先・セットの数量の列) を作るかたまり
   */
  exports: Object.freeze([
    Object.freeze({ id: 'NE の商品マスタ取込の CSV (syohin_code・genka_tnk / baika_tnk / sire_code / suryo)', match: /\b(?:set_)?syohin_code\b[\s\S]*\b(?:genka_tnk|baika_tnk|sire_code|suryo)\b/ }),
  ]),
  /** 表の名前を変数で渡す書き手 (SQL の文字に表の名前が出ない)。file のかたまりにこの文字があれば「書く」 */
  dynamic: Object.freeze([
    Object.freeze({ id: 'po_suppliers (発注アプリの MASTER_DEFS)', file: 'apps/purchase-orders/router.js', match: Object.freeze(['upsertMasterRow(def, row);', 'upsertMasterRow(MASTER_DEFS[', 'DELETE FROM ${def.table}']) }),
  ]),
});

/** 一覧の id が重ならないこと・kind と when_frozen が知っている値 (起動時に落とす) */
{
  const seen = new Set();
  const KINDS = new Set(['route', 'route_field', 'route_part', 'screen', 'cli', 'job']);
  for (const e of LEGACY_ENTRIES) {
    if (seen.has(e.id)) throw new Error(`master-legacy-entries: id が重なっている: ${e.id}`);
    seen.add(e.id);
    if (!KINDS.has(e.kind)) throw new Error(`master-legacy-entries: 知らない kind: ${e.id} ${e.kind}`);
    if (!Object.prototype.hasOwnProperty.call(WHEN_FROZEN, e.when_frozen)) throw new Error(`master-legacy-entries: 知らない when_frozen: ${e.id} ${e.when_frozen}`);
    // ⑤-3b: 閉じるかどうかを列で決める = 全部の入口に、知っている列のキー (1 つ以上) が要る (typo で「閉じたつもり」を作らない)
    if (!Array.isArray(e.owner_cols) || !e.owner_cols.length) throw new Error(`master-legacy-entries: owner_cols が無い: ${e.id}`);
    for (const k of e.owner_cols) if (!OWNED_COLUMNS.includes(k)) throw new Error(`master-legacy-entries: 知らない列 ${k}: ${e.id}`);
    if (e.owner_match !== undefined && !['any', 'all'].includes(e.owner_match)) throw new Error(`master-legacy-entries: 知らない owner_match: ${e.id} ${e.owner_match}`);
    // 広げる道 PR-6: 種類ごとの門。owner_cols = その種類の列を合わせたもの・owner_match 'all' (全部の種類が閉じたときだけ入口ごと閉じる) でないと、種類の門と入口の門が食い違う
    if (e.new_kinds !== undefined) {
      if (!Array.isArray(e.new_kinds) || !e.new_kinds.length || !e.new_kinds.every((k) => Object.hasOwn(NEW_KIND_COLS, k))) throw new Error(`master-legacy-entries: 知らない new_kinds: ${e.id}`);
      const want = newKindCols(e.new_kinds);
      if (e.owner_match !== 'all' || e.owner_cols.length !== want.length || !want.every((k) => e.owner_cols.includes(k))) throw new Error(`master-legacy-entries: new_kinds の入口は owner_cols = 種類の列・owner_match 'all': ${e.id}`);
    }
    if ((e.when_frozen === 'new_entry_409') !== (e.new_kinds !== undefined && e.kind === 'route')) throw new Error(`master-legacy-entries: new_entry_409 は new_kinds の route だけ: ${e.id}`);
  }
  for (const k of Object.keys(NEW_KIND_COLS)) for (const c of NEW_KIND_COLS[k]) if (!OWNED_COLUMNS.includes(c)) throw new Error(`master-legacy-entries: NEW_KIND_COLS.${k} に知らない列 ${c}`);
  for (const e of LEGACY_EXEMPT) {
    if (seen.has(e.id)) throw new Error(`master-legacy-entries: id が重なっている: ${e.id}`);
    seen.add(e.id);
    if (!['replication', 'already_closed', 'manual', 'seed_on_read', 'company_db_outbox'].includes(e.kind)) throw new Error(`master-legacy-entries: 閉じない口の知らない kind: ${e.id} ${e.kind}`);
    // 広げる道 PR-6 (G12): 手の入口も、知っている列のキー (1 つ以上・重ならない) が要る (typo で「止めたつもり」を作らない)
    if (e.kind === 'manual') {
      if (!Array.isArray(e.owner_cols) || !e.owner_cols.length) throw new Error(`master-legacy-entries: 手の入口に owner_cols が無い: ${e.id}`);
      for (const k of e.owner_cols) if (!OWNED_COLUMNS.includes(k)) throw new Error(`master-legacy-entries: 知らない列 ${k}: ${e.id}`);
      if (new Set(e.owner_cols).size !== e.owner_cols.length) throw new Error(`master-legacy-entries: owner_cols が重なっている: ${e.id}`);
    }
  }
}

/** app (router) ごとの入口 (門が読む) */
export function entriesForApp(app) {
  return LEGACY_ENTRIES.filter((e) => e.app === app && ['route', 'route_field', 'route_part', 'screen'].includes(e.kind));
}
/** CLI の入口 (ファイルと mode で引く。'*' = 全部の mode) */
export function cliEntry(file, mode) {
  return LEGACY_ENTRIES.find((e) => e.kind === 'cli' && e.file === file && (e.mode === '*' || e.mode === mode)) || null;
}
export function entryById(id) {
  return LEGACY_ENTRIES.find((e) => e.id === id) || null;
}
