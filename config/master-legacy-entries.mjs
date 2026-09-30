/**
 * master-legacy-entries.mjs — 商品・仕入先マスタの「古い入口」の一覧 (Company DB構想 14 §5・§9 v2 M2・§10 契約 v3 H1 / 10 §4 / PR #1565 Codex R1)
 *
 * 古い入口 = NE の写し (warehouse.db の上書き表・Render の mirror_products・product-hub の税率・profit-calculator の JSON・
 * 発注アプリと売れ筋共有の仕入先) に人がマスタを書く API・画面・手で流す CLI・人の操作で動く取込。
 * 切替の段階 (ops.master_cutover_state・lib/master-cutover.mjs) が frozen 以降になったら、持ち主表 (config/master-ownership.mjs) が
 * まだ 'load' でも、ここに載っている入口は全部書けない。
 * 🚨 門は持ち主表ではなく「段階」で決める (契約 v3 H1: 古い入口を先に閉じてから持ち主を company にする順番を作るため)
 * 🚨 段階が読めない (Company DB に届かない・表が無い・壊れた値) = 書けない側 (fail-closed)。lib/master-legacy-gate.mjs が毎回読む
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
export const MASTER_EDIT_URL = 'https://bfaith-portal.onrender.com/apps/master-edit/';

/** 閉じたときの動き (機械で読む値 → 人が読む説明) */
export const WHEN_FROZEN = Object.freeze({
  http_410: 'API は 410 {error:"master_frozen", message, url}。段階が読めない = 503 {error:"master_phase_unreadable", message, url}。何も書かない',
  field_410: 'その欄を送ったときだけ 410 / 503 (ほかの欄だけの保存は通る)。何も書かない',
  part_skip: 'API は通すが、マスタの部分 (税率の初期値・写し) は書かない',
  banner: '画面は「マスタは新しい画面で直します ↗」の帯を出し、書く部品を隠す (見るだけ)',
  exit_nonzero: 'CLI は何も書かないで終了コード 3 (段階が読めないときも 3)',
  job_skip: '定期の取込を丸ごと止める (閉じている = ログと ok の ping・読めない = fail の ping)',
});

const R = (o) => Object.freeze({ kind: 'route', when_frozen: 'http_410', ...o });
const S = (o) => Object.freeze({ kind: 'screen', when_frozen: 'banner', method: 'GET', ...o });

/** 閉じる入口 (段階 frozen / company_owner / new_open と、段階が読めないとき) */
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
  S({ id: 'warehouse:screen:/register', app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', path: '/register', ref: '10 §4 #2' }),
  S({ id: 'warehouse:screen:/', app: 'warehouse', host: 'minipc', file: 'apps/warehouse/router.js', mount: '/apps/warehouse', path: '/', ref: '10 §4 #2 (データウェアハウスの画面の送料・原価)' }),

  // ─── 10 §4 #4: Render の会計アプリ 5 つの POST /register (mirror_products の税率・売上分類。翌朝の入れ替えで消える) ───
  ...['aupay-accounting', 'yahoo-accounting', 'mercari-accounting', 'linegift-accounting', 'qoo10-accounting'].flatMap((app) => [
    R({ id: `${app}:POST:/register`, app, host: 'render', file: `apps/${app}/router.js`, mount: `/apps/${app}`, method: 'POST', path: '/register', writes: ['mirror_products'], owner_cols: ['skus.tax_rate', 'products.sales_class'], ref: '10 §4 #4' }),
    S({ id: `${app}:screen:/`, app, host: 'render', file: `apps/${app}/router.js`, mount: `/apps/${app}`, path: '/', ref: '10 §4 #4' }),
  ]),

  // ─── 10 §4 #5: fba-profitability の原価の手入力 (mirror_products。翌朝消える) ───
  R({ id: 'fba-profitability:POST:/api/update-cost', app: 'fba-profitability', host: 'render', file: 'apps/fba-profitability/router.js', mount: '/apps/fba-profitability', method: 'POST', path: '/api/update-cost', writes: ['mirror_products'], owner_cols: ['sku_costs'], ref: '10 §4 #5' }),
  S({ id: 'fba-profitability:screen:/', app: 'fba-profitability', host: 'render', file: 'apps/fba-profitability/router.js', mount: '/apps/fba-profitability', path: '/', ref: '10 §4 #5' }),

  // ─── 10 §4 #6: product-hub の税率 (draft_yahoo.tax_rate)。閉じたら出品・画面・試算は Company DB の税率 (services/listing-tax.mjs) ───
  ...[['POST', '/api/drafts/:id'], ['POST', '/api/drafts/:id/yahoo']].map(([method, p]) => Object.freeze({
    id: `product-hub:${method}:${p}:tax_rate`, kind: 'route_field', field: 'tax_rate', when_frozen: 'field_410', app: 'product-hub', host: 'render',
    file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method, path: p, writes: ['draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #6',
  })),
  // Notion の取込 (Notion の税率を draft_yahoo.tax_rate に書く) = 閉じる (R1 H4)
  ...[['POST', '/api/notion-import'], ['POST', '/api/notion-import-by-status']].map(([method, p]) => R({
    id: `product-hub:${method}:${p}`, app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method, path: p,
    writes: ['draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #6 (PR #1565 R1 H4)',
  })),
  // 古い新商品の作り方 (人が下書きを作る /new・NE のコードから一括登録・NE が先の自動取込を手で回す) = 閉じる。
  // 新商品は新しい登録の画面から Company DB 経由でだけ作る (10 §4 #11・Codex ⑤-2a M5)。税率 (NE の初期値) も書くので R1 H4 の対象でもある
  ...[['POST', '/api/drafts'], ['POST', '/api/register-codes'], ['POST', '/api/intake/run'], ['POST', '/api/notion-image-import']].map(([method, p]) => R({
    id: `product-hub:${method}:${p}`, app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method, path: p,
    writes: ['product_drafts (新商品)', 'draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #11 (Codex ⑤-2a M5)・#6 (PR #1565 R1 H4)',
  })),
  // セットを作る (企画中のセット・仮コード) = 作るのは通す、親の税率だけ写さない (R1 H4)
  Object.freeze({
    id: 'product-hub:POST:/api/drafts/:id/set-drafts:tax_rate', kind: 'route_part', part: 'tax_rate', when_frozen: 'part_skip', app: 'product-hub', host: 'render',
    file: 'apps/product-hub/router.js', mount: '/apps/product-hub', method: 'POST', path: '/api/drafts/:id/set-drafts', writes: ['draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #6 (PR #1565 R1 H4)',
  }),
  S({ id: 'product-hub:screen:/detail/:id', app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', path: '/detail/:id', ref: '10 §4 #6 (税率の欄 = Company DB の税率を見せるだけ)' }),
  S({ id: 'product-hub:screen:/new', app: 'product-hub', host: 'render', file: 'apps/product-hub/router.js', mount: '/apps/product-hub', path: '/new', ref: '10 §4 #11 (登録のボタンを隠す)' }),
  Object.freeze({ id: 'job:product-hub:intake-cron', kind: 'job', when_frozen: 'job_skip', host: 'render', file: 'apps/product-hub/intake-cron.js', writes: ['product_drafts (新商品)', 'draft_yahoo.tax_rate'], owner_cols: ['skus.tax_rate'], ref: '10 §4 #11 (NE が先の新商品の自動取込。Codex ⑤-2a M5)' }),

  // ─── NE への 2 つ目の出口 (14 §5) と profit-calculator の仕入先 (10 §4 #9・14 §9 M2) ───
  R({ id: 'profit-calculator:GET:/api/products/csv/ne', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'GET', path: '/api/products/csv/ne', writes: ['NE の商品 (CSV の出口)'], owner_cols: ['skus.name', 'sku_costs', 'skus.standard_price', 'supplier_skus.is_primary', 'sku_components'], ref: '14 §5 (NE への 2 つ目の出口 → マスタの判断の CSV へ)' }),
  R({ id: 'profit-calculator:POST:/api/suppliers', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'POST', path: '/api/suppliers', writes: ['suppliers.json'], owner_cols: ['suppliers.name'], ref: '10 §4 #9・14 §9 M2' }),
  R({ id: 'profit-calculator:DELETE:/api/suppliers', app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', method: 'DELETE', path: '/api/suppliers', writes: ['suppliers.json'], owner_cols: ['suppliers.name'], ref: '10 §4 #9・14 §9 M2' }),
  ...[['/', 'index.html (仕入れ先の追加)'], ['/research', 'list.html (仕入れ先の追加・変更)'], ['/products', 'products.html (NE 用 CSV)'], ['/suppliers', 'suppliers.html (仕入れ先マスタ)']]
    .map(([p, note]) => S({ id: `profit-calculator:screen:${p}`, app: 'profit-calculator', host: 'render', file: 'apps/profit-calculator/router.js', mount: '/apps/profit-calculator', path: p, ref: `10 §4 #9・14 §5 ${note}` })),

  // ─── 10 §4 #7: 発注アプリの仕入先 (po_suppliers)。frozen から閉じる = 切替の手順で書き込み先を Company DB に替えるまで仕入先は見るだけ (R1 H4) ───
  ...[['POST', '/api/masters/:kind'], ['DELETE', '/api/masters/:kind/:id'], ['POST', '/api/masters/:kind/csv']].map(([method, p]) => R({
    id: `purchase-orders:${method}:${p}:suppliers`, app: 'purchase-orders', host: 'render', file: 'apps/purchase-orders/router.js', mount: '/apps/purchase-orders',
    method, path: p, when: Object.freeze({ param: 'kind', in: Object.freeze(['suppliers']) }), writes: ['po_suppliers'], owner_cols: ['suppliers.name', 'suppliers.order_method', 'suppliers.lead_time_days', 'suppliers.contacts'], ref: '10 §4 #7 (PR #1565 R1 H4)',
    ...(p.endsWith('/csv') ? { recheck: true } : {}),
  })),
  R({ id: 'purchase-orders:POST:/api/email/recipients/csv', app: 'purchase-orders', host: 'render', file: 'apps/purchase-orders/router.js', mount: '/apps/purchase-orders', method: 'POST', path: '/api/email/recipients/csv', writes: ['po_suppliers'], owner_cols: ['suppliers.contacts'], ref: '10 §4 #7 (宛先の CSV)', recheck: true }),
  R({ id: 'purchase-orders:POST:/api/import', app: 'purchase-orders', host: 'render', file: 'apps/purchase-orders/router.js', mount: '/apps/purchase-orders', method: 'POST', path: '/api/import', writes: ['po_suppliers'], owner_cols: ['suppliers.name'], ref: '10 §4 #7 (マスタの一括取込。仕入先を含む = 🚨 frozen 以降は仕入先でない種類のファイルもまとめて止まる = 仕入先でないマスタはマスタ管理の各タブの CSV で入れる)', recheck: true }),
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
 *   manual         = 機械では閉じられない入口 (NE の画面・GAS)。コードを持たない (file・method・path を持たない)。切替の証拠 manual_entries_stopped に載せる
 */
export const LEGACY_EXEMPT = Object.freeze([
  Object.freeze({ id: 'warehouse-mirror:POST:/api/sync', kind: 'replication', host: 'render', file: 'apps/warehouse-mirror/router.js', method: 'POST', path: '/api/sync', guard: 'requireSyncKey', reason: 'miniPC → Render の写し (人の入口ではない。切替後は ④ の写しが同じ口で Company DB の値を運ぶ)' }),
  Object.freeze({ id: 'warehouse:render-writes', kind: 'already_closed', host: 'render', file: 'apps/warehouse/router.js', guard: 'rejectWritesOnRender', reason: '10 §4 #3 = Render では warehouse の書き込みは全部 409 (切替と関係なく閉じ済み)' }),
  Object.freeze({ id: 'ne:item-screen', kind: 'manual', host: 'ne', reason: '10 §4 #1。機械では閉じられない = 運用で禁止・切替の証拠 (manual_entries_stopped) に担当者と止めた時刻 (契約 v3 H1)' }),
  Object.freeze({ id: 'gas:logizard-sheet-and-sku-map', kind: 'manual', host: 'google', reason: '10 §4 #12・#13。⑥ で順番に止める・切替の証拠 (manual_entries_stopped) に載せる' }),
]);

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
  }
  for (const e of LEGACY_EXEMPT) {
    if (seen.has(e.id)) throw new Error(`master-legacy-entries: id が重なっている: ${e.id}`);
    seen.add(e.id);
    if (!['replication', 'already_closed', 'manual'].includes(e.kind)) throw new Error(`master-legacy-entries: 閉じない口の知らない kind: ${e.id} ${e.kind}`);
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
