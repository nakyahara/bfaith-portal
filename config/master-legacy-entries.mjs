/**
 * master-legacy-entries.mjs — 商品・仕入先マスタの「古い入口」の一覧 (Company DB構想 14 §5・§9 v2 M2・§10 契約 v3 H1 / 10 §4)
 *
 * 古い入口 = NE の写し (warehouse.db の上書き表・Render の mirror_products・product-hub・profit-calculator の JSON) に
 * 人がマスタを書く API・画面・手で流す CLI。切替の段階 (ops.master_cutover_state・lib/master-cutover.mjs) が
 * frozen 以降になったら、持ち主表 (config/master-ownership.mjs) がまだ 'load' でも、ここに載っている入口は全部書けない。
 * 🚨 門は持ち主表ではなく「段階」で決める (契約 v3 H1: 古い入口を先に閉じてから持ち主を company にする順番を作るため)
 * 🚨 段階が読めない (Company DB に届かない・表が無い・壊れた値) = 書けない側 (fail-closed)。lib/master-legacy-gate.mjs が読む
 *
 * この一覧がそのまま門の設定 (lib/master-legacy-gate.mjs の masterLegacyGate(app) が app ごとに読む) で、
 * scripts/test-master-legacy-entries.mjs がルートの定義を数えて「マスタの表に書くのに一覧に無いルート」を落とす
 * (新しい入口を黙って足せないように)。
 *
 * kind:
 *   route        = API。when_frozen = 'http_410' (段階が読めない = 503)
 *   route_field  = API のうち、その欄 (field) を送ったときだけ閉じる (ほかの欄の保存は今までどおり)
 *   screen       = 画面。帯「マスタは新しい画面で直します ↗」+ 書く部品を隠す (res.locals.masterLegacy)
 *   cli          = 手で流す CLI。mode ごと ('*' = そのファイルの全部の mode)。何もしないで終了コード 3
 * host: minipc (社内の WarehouseServer と CLI) / render (bfaith-portal.onrender.com)
 */
export const MASTER_EDIT_URL = 'https://bfaith-portal.onrender.com/apps/master-edit/';

/**
 * 古い入口の門の版 (1 か所だけ)。⑤-1 の ops.master_legacy_gate_acks.legacy_gates_version に書き、
 * 段階を legacy_open から進める関数が「必要な全部の環境がこの版以上の門を載せた」かを見る (⑤-1 Codex R1)。
 * 🚨 門を足す・閉じ方を変えたら 1 上げる (古い門のままの環境で frozen に進めない)
 */
export const LEGACY_GATES_VERSION = 1;

/** 閉じたときの動き (機械で読む値 → 人が読む説明) */
export const WHEN_FROZEN = Object.freeze({
  http_410: 'API は 410 {error:"master_frozen", message, url}。段階が読めない = 503 {error:"master_phase_unreadable", message, url}。何も書かない',
  field_410: 'その欄を送ったときだけ 410 / 503 (ほかの欄だけの保存は通る)。何も書かない',
  banner: '画面は「マスタは新しい画面で直します ↗」の帯を出し、書く部品を隠す (見るだけ)',
  exit_nonzero: 'CLI は何も書かないで終了コード 3 (段階が読めないときも 3)',
});

const R = (o) => Object.freeze({ kind: 'route', when_frozen: 'http_410', ...o });

/** 閉じる入口 (段階 frozen / company_owner / new_open と、段階が読めないとき) */
export const LEGACY_ENTRIES = Object.freeze([
  // ─── 10 §4 #2: miniPC のマスタ登録 /apps/warehouse/register (上書き表 + m_products) ───
  ...[
    ['POST', '/api/shipping', ['product_shipping', 'm_products'], ['skus.shipping']],
    ['POST', '/api/genka', ['exception_genka', 'm_products'], ['sku_costs']],
    ['POST', '/api/csv/shipping', ['product_shipping', 'm_products'], ['skus.shipping']],
    ['POST', '/api/csv/genka', ['exception_genka', 'm_products'], ['sku_costs']],
    ['POST', '/api/csv/m-sku-master', ['m_sku_master', 'm_sku_components'], ['listing_components.amazon']],
    ['DELETE', '/api/shipping/:sku', ['product_shipping'], ['skus.shipping']],
    ['DELETE', '/api/genka/:sku', ['exception_genka'], ['sku_costs']],
    ['DELETE', '/api/sales_class/:sku', ['product_sales_class', 'm_products'], ['products.sales_class']],
    ['DELETE', '/api/tax_rate/:sku', ['product_tax_rate', 'm_products'], ['skus.tax_rate', 'skus.tax_class']],
    ['POST', '/api/sales_class', ['product_sales_class', 'm_products'], ['products.sales_class']],
    ['POST', '/api/csv/sales_class', ['product_sales_class', 'm_products'], ['products.sales_class']],
    ['POST', '/api/reorder_setting', ['m_reorder_setting'], ['skus.reorder_months']],
    ['DELETE', '/api/reorder_setting/:sku', ['m_reorder_setting'], ['skus.reorder_months']],
    ['POST', '/api/csv/reorder_setting', ['m_reorder_setting'], ['skus.reorder_months']],
    ['POST', '/api/tax_rate', ['product_tax_rate', 'm_products'], ['skus.tax_rate', 'skus.tax_class']],
    ['POST', '/api/csv/tax_rate', ['product_tax_rate', 'm_products'], ['skus.tax_rate', 'skus.tax_class']],
  ].map(([method, p, writes, cols]) => R({ id: `warehouse:${method} ${p}`, app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', method, path: p, writes, owner_cols: cols, ref: '10 §4 #2' })),
  // 同じ router に載る SKU マスタ (Amazon SKU ↔ NE コード) の API (D-43: SKU タブも閉じる)
  ...[['POST', '/api/m-sku-master'], ['PUT', '/api/m-sku-master/:sku'], ['DELETE', '/api/m-sku-master/:sku']]
    .map(([method, p]) => R({ id: `warehouse:${method} ${p}`, app: 'warehouse', host: 'minipc', file: 'apps/warehouse/sku-master-api.js', mount: '/apps/warehouse', method, path: p, writes: ['m_sku_master', 'm_sku_components'], owner_cols: ['listing_components.amazon'], ref: '10 §4 #2 (D-43)' })),
  Object.freeze({ id: 'warehouse:screen /register', kind: 'screen', when_frozen: 'banner', app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', method: 'GET', path: '/register', ref: '10 §4 #2' }),
  Object.freeze({ id: 'warehouse:screen /', kind: 'screen', when_frozen: 'banner', app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', method: 'GET', path: '/', ref: '10 §4 #2 (データウェアハウスの画面の送料・原価)' }),

  // ─── 10 §4 #4: Render の会計アプリ 5 つの POST /register (mirror_products の税率・売上分類。翌朝の入れ替えで消える) ───
  ...['aupay-accounting', 'yahoo-accounting', 'mercari-accounting', 'linegift-accounting', 'qoo10-accounting'].flatMap((app) => [
    R({ id: `${app}:POST /register`, app, host: 'render', file: `apps/${app}/router.js`, mount: `/apps/${app}`, method: 'POST', path: '/register', writes: ['mirror_products'], owner_cols: ['skus.tax_rate', 'products.sales_class'], ref: '10 §4 #4' }),
    Object.freeze({ id: `${app}:screen /`, kind: 'screen', when_frozen: 'banner', app, host: 'render', file: `apps/${app}/router.js`, mount: `/apps/${app}`, method: 'GET', path: '/', ref: '10 §4 #4' }),
  ]),

  // ─── 10 §4 #5: fba-profitability の原価の手入力 (mirror_products。翌朝消える) ───
  R({ id: 'fba-profitability:POST /api/update-cost', app: 'fba-profitability', host: 'render', file: 'apps/fba-profitability/router.js', mount: '/apps/fba-profitability', method: 'POST', path: '/api/update-cost', writes: ['mirror_products'], owner_cols: ['sku_costs'], ref: '10 §4 #5' }),
  Object.freeze({ id: 'fba-profitability:screen /', kind: 'screen', when_frozen: 'banner', app: 'fba-profitability', host: 'render', file: 'apps/fba-profitability/router.js', mount: '/apps/fba-profitability', method: 'GET', path: '/', ref: '10 §4 #5' }),

  // ─── 10 §4 #6: product-hub の税率 (draft_yahoo.tax_rate の手入力)。閉じたら Company DB の税率を見せるだけ ───
  ...[['POST', '/api/drafts/:id'], ['POST', '/api/drafts/:id/yahoo']].map(([method, p]) => Object.freeze({
    id: `product-hub:${method} ${p} (tax_rate)`, kind: 'route_field', field: 'tax_rate', when_frozen: 'field_410', app: 'product-hub', host: 'render',
    file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method, path: p, writes: ['draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #6',
  })),
  Object.freeze({ id: 'product-hub:screen /detail/:id', kind: 'screen', when_frozen: 'banner', app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method: 'GET', path: '/detail/:id', ref: '10 §4 #6 (税率の欄 = Company DB の税率を見せるだけ)' }),

  // ─── NE への 2 つ目の出口 (14 §5) と profit-calculator の仕入先 (10 §4 #9・14 §9 M2) ───
  R({ id: 'profit-calculator:GET /api/products/csv/ne', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'GET', path: '/api/products/csv/ne', writes: ['NE の商品 (CSV の出口)'], owner_cols: ['skus.name', 'sku_costs', 'skus.standard_price', 'supplier_skus.is_primary', 'sku_components'], ref: '14 §5 (NE への 2 つ目の出口 → マスタの判断の CSV へ)' }),
  R({ id: 'profit-calculator:POST /api/suppliers', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'POST', path: '/api/suppliers', writes: ['suppliers.json'], owner_cols: ['suppliers.name'], ref: '10 §4 #9・14 §9 M2' }),
  R({ id: 'profit-calculator:DELETE /api/suppliers', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'DELETE', path: '/api/suppliers', writes: ['suppliers.json'], owner_cols: ['suppliers.name'], ref: '10 §4 #9・14 §9 M2' }),
  ...[['/', 'index.html (仕入れ先の追加)'], ['/research', 'list.html (仕入れ先の追加・変更)'], ['/products', 'products.html (NE 用 CSV)'], ['/suppliers', 'suppliers.html (仕入れ先マスタ)']]
    .map(([p, note]) => Object.freeze({ id: `profit-calculator:screen ${p}`, kind: 'screen', when_frozen: 'banner', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'GET', path: p, ref: `10 §4 #9・14 §5 ${note}` })),

  // ─── 10 §4 #10: 手で流す取込 (miniPC)。🚨 ファイル単位ではなく書く表 / mode 単位 (csv-import.js の受注・ロジザードなどは止めない) ───
  ...[['product_shipping', ['product_shipping'], ['skus.shipping']], ['exception_genka', ['exception_genka'], ['sku_costs']]].map(([mode, writes, cols]) => Object.freeze({
    id: `cli:csv-import.js ${mode}`, kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/csv-import.js', mode, writes, owner_cols: cols, ref: '10 §4 #10 (全部消して入れ直す)',
  })),
  Object.freeze({ id: 'cli:import-sales-class.js', kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/import-sales-class.js', mode: '*', writes: ['product_sales_class'], owner_cols: ['products.sales_class'], ref: '10 §4 #10' }),
  Object.freeze({ id: 'cli:import-sku-master.js', kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/import-sku-master.js', mode: '*', writes: ['m_sku_master', 'm_sku_components'], owner_cols: ['listing_components.amazon'], ref: '10 §4 #2・#10 (D-43)。--dry-run も止める (書く表がマスタだけのファイル)' }),
  Object.freeze({ id: 'cli:migrate-reorder-setting-initial.js', kind: 'cli', when_frozen: 'exit_nonzero', host: 'minipc', file: 'apps/warehouse/migrate-reorder-setting-initial.js', mode: '*', writes: ['m_reorder_setting'], owner_cols: ['skus.reorder_months'], ref: '14 §9 M2。--dry-run も止める (書く表がマスタだけのファイル)' }),
]);

/**
 * 閉じない (この PR では塞がない) もの。ルートを数える試験は、マスタの表に書くルートがここにあれば通す (理由を必ず書く)。
 * 切替の手順 (10 §8.2・§8.3) の確かめる一覧でもある
 */
export const LEGACY_NOT_CLOSED = Object.freeze([
  // 発注アプリの仕入先タブ (po_suppliers。/api/masters/:kind の kind = suppliers) と宛先の CSV: 閉じずに、切替の手順の中で書き込み先を
  // Company DB に差し替える (14 §9 v2 H1)。先方品番・ロット・発注条件 (vendor-map・conditions) は発注アプリが正のまま (D-44) = マスタの表ではない
  ...[['POST', '/api/masters/:kind'], ['DELETE', '/api/masters/:kind/:id'], ['POST', '/api/masters/:kind/csv'], ['POST', '/api/email/recipients/csv'], ['POST', '/api/import']]
    .map(([method, p]) => Object.freeze({ id: `purchase-orders:${method} ${p}`, kind: 'route', host: 'render', file: 'apps/purchase-orders/router.js', method, path: p, reason: '仕入先タブ・宛先は閉じない。切替の手順 (14 §9 v2 H1) で書き込み先を Company DB に差し替える (段階とは別の手順)' })),
  Object.freeze({ id: 'supplier-sales:POST /api/supplier-name', kind: 'route', host: 'render', file: 'apps/supplier-sales/router.js', method: 'POST', path: '/api/supplier-name', reason: '10 §4 #8 = 売れ筋共有の表示名。core.suppliers を読む作り直しは別 (切替の手順で止める)' }),
  Object.freeze({ id: 'warehouse-mirror:POST /api/sync', kind: 'route', host: 'render', file: 'apps/warehouse-mirror/router.js', method: 'POST', path: '/api/sync', reason: 'miniPC → Render の写し (人の入口ではない。切替後は ④ の写しが同じ口を使う)' }),
  // 人の入口ではない・別の段で閉じる
  Object.freeze({ id: 'ne:商品画面', kind: 'manual', host: 'ne', reason: '10 §4 #1。機械では閉じられない = 運用で禁止・切替の証跡に担当者と止めた時刻 (契約 v3 H1)' }),
  Object.freeze({ id: 'product-hub:ph-ne-intake', kind: 'job', host: 'render', file: 'apps/product-hub/services/new-product-intake.js', reason: '10 §4 #11。NE の新商品から下書きを作る (税率の初期値も NE から)。起点を Company DB の新商品にするのは ⑤-2 の後' }),
  Object.freeze({ id: 'product-hub:税率の自動の初期値', kind: 'route', host: 'render', file: 'apps/product-hub/router.js', method: 'POST', path: '/api/drafts', reason: '下書きを作るときに NE の税率を draft_yahoo.tax_rate に入れる (router.js の /api/drafts 作成・set-derive.js・notion-import.js)。人の入口ではない。モールへ出す税率を Company DB から取る直しは ⑥' }),
  Object.freeze({ id: 'gas:ロジザード情報連携シート / 商品コード変換テーブル', kind: 'manual', host: 'google', reason: '10 §4 #12・#13。⑥ で順番に止める' }),
  Object.freeze({ id: 'warehouse:Render 版の書き込み', kind: 'route', host: 'render', file: 'apps/warehouse/router.js', reason: '10 §4 #3 = rejectWritesOnRender が Render では全部 409 (切替と関係なく閉じ済み)' }),
]);

/**
 * CLI のうち、マスタの表に書かないので止めない mode (試験が「全部の mode を数えた」かを見る)
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
 * マスタの表 (試験が「このどれかに書くルート・ファイルは一覧に載っているか」を数える)。
 * SQL で書く表と、関数で書く口 (JSON のファイル・product-hub の税率) の両方
 */
export const MASTER_WRITE_TARGETS = Object.freeze({
  tables: Object.freeze(['product_shipping', 'exception_genka', 'product_sales_class', 'product_tax_rate', 'm_reorder_setting', 'm_sku_master', 'm_sku_components', 'mirror_products', 'm_products', 'po_suppliers', 'supplier_share_master']),
  /** 関数・欄で書く口 (ファイルの相対パス → そのファイルの中で探す文字) */
  calls: Object.freeze({
    'apps/profit-calculator/router.js': Object.freeze(['addSupplier(', 'deleteSupplier(', "'/api/products/csv/ne'"]),
    'apps/product-hub/router.js': Object.freeze(['tax_rate']),
    'apps/purchase-orders/router.js': Object.freeze(['upsertMasterRow(def, row);', 'upsertMasterRow(MASTER_DEFS[', 'DELETE FROM ${def.table}']),
    'apps/supplier-sales/router.js': Object.freeze(['upsertSupplierName(']),
    'apps/warehouse/router.js': Object.freeze(['importSkuMasterCSV(']),
  }),
});

/** 一覧の id が重ならないこと (起動時に落とす) */
{
  const seen = new Set();
  for (const e of [...LEGACY_ENTRIES, ...LEGACY_NOT_CLOSED]) {
    if (seen.has(e.id)) throw new Error(`master-legacy-entries: id が重なっている: ${e.id}`);
    seen.add(e.id);
    if (e.when_frozen && !Object.prototype.hasOwnProperty.call(WHEN_FROZEN, e.when_frozen)) throw new Error(`master-legacy-entries: 知らない when_frozen: ${e.id} ${e.when_frozen}`);
  }
}

/** app (router) ごとの入口 (門が読む) */
export function entriesForApp(app) {
  return LEGACY_ENTRIES.filter((e) => e.app === app && (e.kind === 'route' || e.kind === 'route_field' || e.kind === 'screen'));
}
/** CLI の入口 (ファイルと mode で引く。'*' = 全部の mode) */
export function cliEntry(file, mode) {
  return LEGACY_ENTRIES.find((e) => e.kind === 'cli' && e.file === file && (e.mode === '*' || e.mode === mode)) || null;
}
export function entryById(id) {
  return LEGACY_ENTRIES.find((e) => e.id === id) || null;
}
