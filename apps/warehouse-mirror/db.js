/**
 * warehouse-mirror DB — Render側のミラーデータベース
 *
 * ミニPCのwarehouse.dbから送信された2次加工データを格納。
 * 正本はミニPC。ここは読み取り専用の派生データストア。
 *
 * テーブル命名規則:
 *   mirror_*  — ミニPCから同期されたデータ
 *   mart_*    — ツール用に加工したデータ（将来）
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'warehouse-mirror.db');

let db = null;

export function initMirrorDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  db = new Database(DB_FILE);
  db.pragma('journal_mode = WAL');
  createTables();
  console.log('[Mirror] 初期化完了');
  return db;
}

export function getMirrorDB() {
  if (!db) throw new Error('warehouse-mirror.db が初期化されていません');
  return db;
}

// 既存テーブルへのカラム追加ヘルパー（冪等、空catchでエラー握り潰さない）
//   Codex PR1 review Medium #4 反映
function addColumnIfMissing(table, column, typeClause) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  if (!cols.includes(column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${typeClause}`);
  }
}

function createTables() {
  // mirror_products — 統合商品マスタ（m_productsのミラー）
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_products (
    product_id                INTEGER PRIMARY KEY,
    商品コード                TEXT UNIQUE NOT NULL,
    商品名                    TEXT,
    商品区分                  TEXT NOT NULL,
    取扱区分                  TEXT,
    標準売価                  REAL,
    原価                      REAL,
    原価ソース                TEXT,
    原価状態                  TEXT NOT NULL,
    送料                      REAL,
    送料コード                TEXT,
    配送方法                  TEXT,
    消費税率                  REAL,
    税区分                    TEXT,
    在庫数                    INTEGER,
    引当数                    INTEGER,
    仕入先コード              TEXT,
    セット構成品数            INTEGER,
    売上分類                  INTEGER,
    代表商品コード            TEXT,
    seasonality_flag          INTEGER DEFAULT 0,
    season_months             TEXT,
    new_product_flag          INTEGER DEFAULT 0,
    new_product_launch_date   TEXT,
    updated_at                TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_sku ON mirror_products(商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_status ON mirror_products(取扱区分)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_type ON mirror_products(商品区分)');
  // 既存テーブルへのカラム追加（マイグレーション）
  addColumnIfMissing('mirror_products', '売上分類', 'INTEGER');
  addColumnIfMissing('mirror_products', '代表商品コード', 'TEXT');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_rep ON mirror_products(代表商品コード)');
  // 商品収益性ダッシュボード Phase 1 追加カラム（季節性・新商品フラグ）
  addColumnIfMissing('mirror_products', 'seasonality_flag', 'INTEGER DEFAULT 0');
  addColumnIfMissing('mirror_products', 'season_months', 'TEXT');
  addColumnIfMissing('mirror_products', 'new_product_flag', 'INTEGER DEFAULT 0');
  addColumnIfMissing('mirror_products', 'new_product_launch_date', 'TEXT');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_season ON mirror_products(seasonality_flag)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_new ON mirror_products(new_product_flag)');

  // mirror_set_components — セット構成マスタ
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_set_components (
    セット商品コード  TEXT NOT NULL,
    構成商品コード    TEXT NOT NULL,
    数量              INTEGER NOT NULL DEFAULT 1,
    構成商品名        TEXT,
    構成商品原価      REAL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (セット商品コード, 構成商品コード)
  )`);

  // mirror_sku_resolved — SKU紐付け解決済みビューのミラー（v_sku_resolved の結果）
  // 設計:
  //   - source='master': m_sku_master/m_sku_components 由来（人手キュレート、商品名あり）
  //     SKU管理統合 Step 4-0 で v_sku_resolved は master only 化済 (sku_map fallback 撤廃)
  //   - source_updated_at は m_sku_master.updated_at
  //   - synced_at はこのミラーへの取り込み時刻
  //   - source='auto' は SKU管理統合 Step 4-0 で廃止だが、過渡期 row の互換のため列値は許容
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sku_resolved (
    seller_sku         TEXT NOT NULL,
    ne_code            TEXT NOT NULL,
    quantity           INTEGER NOT NULL,
    source             TEXT NOT NULL CHECK (source IN ('master', 'auto')),
    商品名             TEXT,
    source_updated_at  TEXT,
    synced_at          TEXT NOT NULL,
    PRIMARY KEY (seller_sku, ne_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirres_sku ON mirror_sku_resolved(seller_sku)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirres_ne ON mirror_sku_resolved(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirres_src ON mirror_sku_resolved(source)');

  // mirror_inv_daily_summary — 日次在庫スナップショットの集計結果ミラー
  // 元: ミニPC warehouse.db.inv_daily_summary
  // category = 'fba_warehouse' | 'fba_inbound' | 'own_warehouse' | 'fba_us_warehouse' | 'fba_us_inbound'
  // source_status = 'ok' | 'partial' | 'failed' | 'no_source'  (no_source は UI で「データなし」表示)
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_inv_daily_summary (
    business_date      TEXT NOT NULL,
    market             TEXT NOT NULL DEFAULT 'jp',
    category           TEXT NOT NULL,
    total_qty          INTEGER NOT NULL,
    total_value        REAL,
    resolved_count     INTEGER NOT NULL DEFAULT 0,
    unresolved_count   INTEGER NOT NULL DEFAULT 0,
    cost_missing_count INTEGER NOT NULL DEFAULT 0,
    source_status      TEXT NOT NULL,
    source_row_count   INTEGER,
    captured_at        TEXT,
    synced_at          TEXT NOT NULL,
    PRIMARY KEY (business_date, market, category)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mir_inv_daily_date ON mirror_inv_daily_summary(business_date)');

  // mirror_inv_daily_detail — 詳細層 (D-1c、直近365日のみmirror)
  // 元: ミニPC warehouse.db.inv_daily_detail
  // 差分sync方式: ミニPCから直近7日分を毎日 UPSERT で送信、365日より古い分を Render 側で DELETE
  // detail 5,000-6,000行/日 × 365 = 約220万行で、SQLite/Render disk で扱える範囲
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_inv_daily_detail (
    business_date              TEXT NOT NULL,
    market                     TEXT NOT NULL DEFAULT 'jp',
    category                   TEXT NOT NULL,
    source_system              TEXT NOT NULL,
    source_item_code           TEXT NOT NULL,
    ne_code                    TEXT NOT NULL,
    qty                        INTEGER NOT NULL,
    unit_cost                  REAL,
    total_value                REAL,
    cost_status                TEXT NOT NULL,
    cost_source                TEXT,
    resolution_method          TEXT,
    is_bundle_expanded         INTEGER NOT NULL DEFAULT 0,
    component_qty              INTEGER,
    product_name               TEXT,
    source_product_name        TEXT,
    supplier_code              TEXT,
    product_type               TEXT,
    handling_class             TEXT,
    sales_class                INTEGER,
    representative_product_code TEXT,
    order_lot_size             INTEGER,
    seasonality_flag           INTEGER,
    season_months              TEXT,
    new_product_flag           INTEGER,
    new_product_launch_date    TEXT,
    last_sold_date             TEXT,
    sales_7d_qty               INTEGER,
    sales_30d_qty              INTEGER,
    sales_90d_qty              INTEGER,
    sales_7d_value             REAL,
    sales_30d_value            REAL,
    sales_90d_value            REAL,
    working_first_seen         TEXT,
    fba_unfulfillable_qty      INTEGER,
    reserved_qty               INTEGER,
    pending_order_qty          INTEGER,
    location_code              TEXT,
    last_purchase_date         TEXT,
    snapshot_run_id            TEXT,
    synced_at                  TEXT NOT NULL,
    PRIMARY KEY (business_date, market, category, source_system, source_item_code, ne_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mir_idd_date ON mirror_inv_daily_detail(business_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mir_idd_date_cat ON mirror_inv_daily_detail(business_date, category)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mir_idd_ne ON mirror_inv_daily_detail(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mir_idd_supplier ON mirror_inv_daily_detail(supplier_code)');

  // 派生view: DOS / 回転率 / 滞留判定
  db.exec('DROP VIEW IF EXISTS v_mir_inv_daily_metrics');
  db.exec(`CREATE VIEW v_mir_inv_daily_metrics AS
    SELECT
      business_date, market, category, source_system, source_item_code, ne_code,
      qty, total_value,
      sales_7d_qty, sales_30d_qty, sales_90d_qty,
      sales_7d_value, sales_30d_value, sales_90d_value,
      last_sold_date,
      CASE WHEN sales_30d_qty > 0 THEN ROUND(qty * 30.0 / sales_30d_qty, 1) ELSE NULL END AS days_of_supply,
      CASE WHEN qty > 0 AND sales_30d_qty > 0 THEN ROUND(365.0 * sales_30d_qty / (qty * 30.0), 2) ELSE NULL END AS turnover_yearly,
      CASE WHEN (sales_90d_qty IS NULL OR sales_90d_qty = 0) AND qty > 0 THEN 1 ELSE 0 END AS is_stale,
      product_name, supplier_code,
      product_type, handling_class, sales_class,
      seasonality_flag, new_product_flag,
      cost_status, resolution_method
    FROM mirror_inv_daily_detail
  `);

  // mirror_sales_monthly — 月次集計（24ヶ月分）
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sales_monthly (
    月                TEXT NOT NULL,
    商品コード        TEXT NOT NULL,
    モール            TEXT NOT NULL,
    商品名            TEXT,
    数量              INTEGER NOT NULL DEFAULT 0,
    直接販売数        INTEGER DEFAULT 0,
    セット経由数      INTEGER DEFAULT 0,
    売上金額          REAL,
    注文数            INTEGER,
    データ種別        TEXT NOT NULL,    -- 'by_product' | 'by_listing'
    チャネル          TEXT DEFAULT '',
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (月, 商品コード, モール, データ種別, チャネル)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirs_month ON mirror_sales_monthly(月)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirs_sku ON mirror_sales_monthly(商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirs_mall ON mirror_sales_monthly(モール)');

  // mirror_sales_daily — 日次集計（直近90日分）
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sales_daily (
    日付              TEXT NOT NULL,
    商品コード        TEXT NOT NULL,
    モール            TEXT NOT NULL,
    商品名            TEXT,
    数量              INTEGER NOT NULL DEFAULT 0,
    直接販売数        INTEGER DEFAULT 0,
    セット経由数      INTEGER DEFAULT 0,
    売上金額          REAL,
    注文数            INTEGER,
    データ種別        TEXT NOT NULL,    -- 'by_product' | 'by_listing'
    チャネル          TEXT DEFAULT '',
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (日付, 商品コード, モール, データ種別, チャネル)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mird_date ON mirror_sales_daily(日付)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mird_sku ON mirror_sales_daily(商品コード)');

  // mirror_rakuten_sku_map — 楽天コード(AM/AL/W) → NE商品コード マッピング
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_rakuten_sku_map (
    rakuten_code      TEXT PRIMARY KEY,
    ne_code           TEXT NOT NULL,
    source            TEXT NOT NULL,     -- 'am' | 'al' | 'w'
    updated_at        TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirr_rskm_ne ON mirror_rakuten_sku_map(ne_code)');

  // mirror_sync_status — 同期状態
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sync_status (
    key               TEXT PRIMARY KEY,
    value             TEXT,
    updated_at        TEXT
  )`);

  // ---- Phase 1 #1-4a: sync_run_chunks (Render 側 chunk ledger)
  // chunk 受信のたびに INSERT、apply 成功で applied_at を更新
  // 同一 (run_id, entity, chunk_index) は idempotent (tx 内で existing 確認、checksum 一致なら短絡)
  // contract_version / scope_from / scope_to / chunk_count は run 単位の不変条件として
  // 同一 run の異なる chunk が来たとき同値かを検証 (Codex Round 12 #2)
  db.exec(`CREATE TABLE IF NOT EXISTS sync_run_chunks (
    run_id            TEXT NOT NULL,
    entity            TEXT NOT NULL,
    chunk_index       INTEGER NOT NULL,
    chunk_count       INTEGER NOT NULL,
    row_count         INTEGER NOT NULL,
    payload_checksum  TEXT NOT NULL,
    contract_version  INTEGER NOT NULL,
    scope_from        TEXT NOT NULL CHECK(scope_from GLOB '????-??-??'),
    scope_to          TEXT NOT NULL CHECK(scope_to   GLOB '????-??-??'),
    received_at       TEXT NOT NULL,
    applied_at        TEXT,
    PRIMARY KEY (run_id, entity, chunk_index)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_src_run_id ON sync_run_chunks(run_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_src_entity_received ON sync_run_chunks(entity, received_at)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_src_entity_scope ON sync_run_chunks(entity, scope_from, scope_to)');

  // MF Phase 1a (Codex review #88 反映): finalize cross-check 用に
  // 親 mf_publish_runs.run_id を ledger に記録 (NULL=非MF entity、Amazon/Rakuten 既存 row は NULL のまま)
  addColumnIfMissing('sync_run_chunks', 'mf_source_run_id', 'INTEGER');
  db.exec('CREATE INDEX IF NOT EXISTS idx_src_mf_source_run ON sync_run_chunks(mf_source_run_id) WHERE mf_source_run_id IS NOT NULL');

  // mirror_amazon_finance_sku_daily — Phase 1 #1-4 (Render 側 daily fact mirror)
  // miniPC の f_amazon_finance_sku_daily_v1 の payload を受信。
  // contract_version は sync_contracts.contract_version と整合。
  // PK: (date_jst, seller_sku, asin_norm) — asin_norm 物理列で NULL 排除 (Codex Round 8 推奨)
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_amazon_finance_sku_daily (
    date_jst                    TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    seller_sku                  TEXT NOT NULL CHECK(trim(seller_sku) <> ''),
    asin_norm                   TEXT NOT NULL DEFAULT '',
    product_name                TEXT NOT NULL DEFAULT '',
    units_ordered               REAL NOT NULL DEFAULT 0,
    units_refunded_customer     REAL NOT NULL DEFAULT 0,
    units_marketplace_guarantee REAL NOT NULL DEFAULT 0,
    units_a_to_z_refund         REAL NOT NULL DEFAULT 0,
    units_net_sold              REAL NOT NULL DEFAULT 0,
    sales_principal_jpy         REAL NOT NULL DEFAULT 0,
    sales_shipping_jpy          REAL NOT NULL DEFAULT 0,
    sales_giftwrap_jpy          REAL NOT NULL DEFAULT 0,
    sales_tax_jpy               REAL NOT NULL DEFAULT 0,
    commission_jpy              REAL NOT NULL DEFAULT 0,
    fba_fulfillment_jpy         REAL NOT NULL DEFAULT 0,
    fba_storage_jpy             REAL NOT NULL DEFAULT 0,
    closing_fee_jpy             REAL NOT NULL DEFAULT 0,
    shipping_chargeback_jpy     REAL NOT NULL DEFAULT 0,
    giftwrap_chargeback_jpy     REAL NOT NULL DEFAULT 0,
    promotion_jpy               REAL NOT NULL DEFAULT 0,
    warehouse_damage_jpy        REAL NOT NULL DEFAULT 0,
    warehouse_lost_jpy          REAL NOT NULL DEFAULT 0,
    safe_t_jpy                  REAL NOT NULL DEFAULT 0,
    refund_principal_jpy        REAL NOT NULL DEFAULT 0,
    reversal_reimbursement_jpy  REAL NOT NULL DEFAULT 0,
    misc_fee_jpy                REAL NOT NULL DEFAULT 0,
    other_fee_jpy               REAL NOT NULL DEFAULT 0,
    other_amount_jpy            REAL NOT NULL DEFAULT 0,
    unit_cost_snapshot          REAL,
    cost_snapshot_date_jst      TEXT,
    latest_unit_cost_reference  REAL,
    cogs_amount                 REAL NOT NULL DEFAULT 0,
    profit_amount               REAL NOT NULL DEFAULT 0,
    is_cost_complete            INTEGER NOT NULL DEFAULT 0,
    cost_status                 TEXT NOT NULL CHECK (
      cost_status IN ('complete','missing_cost','partial_cost','late_bound_after_close')
    ),
    source_run_id               TEXT NOT NULL,
    source_row_hash             TEXT NOT NULL,
    synced_at                   TEXT NOT NULL,
    PRIMARY KEY (date_jst, seller_sku, asin_norm)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_date ON mirror_amazon_finance_sku_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_sku ON mirror_amazon_finance_sku_daily(seller_sku)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_month ON mirror_amazon_finance_sku_daily(substr(date_jst, 1, 7))');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_run ON mirror_amazon_finance_sku_daily(source_run_id)');

  // mirror_rakuten_finance_sku_daily — 楽天 Phase 1a #R-3b (Render 側 daily fact mirror)
  // miniPC の f_rakuten_finance_sku_daily_v1 の payload を受信。
  // contract_version は sync_contracts.contract_version と整合。
  // PK: (date_jst, rakuten_code) — Amazon と異なり asin_norm は楽天にない
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_rakuten_finance_sku_daily (
    date_jst                          TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    rakuten_code                      TEXT NOT NULL CHECK(trim(rakuten_code) <> ''),
    ne_code                           TEXT,
    sku_resolution                    TEXT NOT NULL CHECK (
      sku_resolution IN ('resolved', 'unresolved', 'direct_master')
    ),
    product_name                      TEXT NOT NULL DEFAULT '',
    units_ordered                     INTEGER NOT NULL DEFAULT 0,
    units_cancelled                   INTEGER NOT NULL DEFAULT 0,
    units_net_sold                    INTEGER NOT NULL DEFAULT 0,
    -- Phase 1b 按分関連
    allocated_units_cancelled         INTEGER NOT NULL DEFAULT 0,
    units_cancelled_same_day_matched  INTEGER NOT NULL DEFAULT 0,
    allocation_method                 TEXT NOT NULL DEFAULT 'no_refund' CHECK (
      allocation_method IN ('monthly_proportion', 'no_refund')
    ),
    cancel_exceeds_ordered_warning    INTEGER NOT NULL DEFAULT 0,
    sales_principal_jpy_incl          REAL NOT NULL DEFAULT 0,
    sales_postage_jpy_incl            REAL NOT NULL DEFAULT 0,
    coupon_shop_jpy_incl              REAL NOT NULL DEFAULT 0,
    coupon_all_jpy_incl               REAL NOT NULL DEFAULT 0,
    promotion_jpy_incl                REAL NOT NULL DEFAULT 0,
    refund_amount_jpy_incl            REAL NOT NULL DEFAULT 0,
    -- Phase 1b 按分 refund 関連
    allocated_refund_amount_jpy_incl  REAL NOT NULL DEFAULT 0,
    refund_amount_same_day_matched_jpy_incl REAL NOT NULL DEFAULT 0,
    mall_fee_jpy_incl                 REAL NOT NULL DEFAULT 0,
    shipping_cost_jpy_incl            REAL NOT NULL DEFAULT 0,
    shipping_quality                  TEXT NOT NULL CHECK (
      shipping_quality IN ('actual', 'estimated_rates', 'estimated_fallback', 'missing')
    ),
    unit_cost_snapshot_incl           REAL,
    cost_snapshot_date_jst            TEXT,
    latest_unit_cost_reference_incl   REAL,
    cogs_amount_jpy_incl              REAL NOT NULL DEFAULT 0,
    gross_sales_jpy_incl              REAL NOT NULL DEFAULT 0,
    net_sales_jpy_incl                REAL NOT NULL DEFAULT 0,
    variable_margin_jpy_incl          REAL NOT NULL DEFAULT 0,
    refund_adjusted_net_sales_jpy_incl REAL NOT NULL DEFAULT 0,
    cost_status                       TEXT NOT NULL CHECK (
      cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
    ),
    is_cost_complete                  INTEGER NOT NULL DEFAULT 0,
    data_quality_score                INTEGER NOT NULL DEFAULT 0
                                      CHECK (data_quality_score BETWEEN 0 AND 100),
    price_variance_warning            INTEGER NOT NULL DEFAULT 0,
    source_layer_summary              TEXT NOT NULL DEFAULT '',
    source_row_count                  INTEGER NOT NULL DEFAULT 0,
    built_at                          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_run_id                     TEXT NOT NULL,
    source_row_hash                   TEXT NOT NULL,
    synced_at                         TEXT NOT NULL,
    PRIMARY KEY (date_jst, rakuten_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mrfsd_date ON mirror_rakuten_finance_sku_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mrfsd_ne ON mirror_rakuten_finance_sku_daily(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mrfsd_month ON mirror_rakuten_finance_sku_daily(substr(date_jst, 1, 7))');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mrfsd_run ON mirror_rakuten_finance_sku_daily(source_run_id)');

  // Phase 1b migration: 既存 mirror_rakuten_finance_sku_daily に新列追加 (idempotent)
  // CREATE TABLE IF NOT EXISTS は既存 table に新列を追加しないので、ALTER TABLE で動的 migrate
  const mrfsdCols = new Set(
    db.prepare("PRAGMA table_info(mirror_rakuten_finance_sku_daily)").all().map(c => c.name)
  );
  const phase1bMirrorCols = [
    { name: 'allocated_units_cancelled',         def: 'INTEGER NOT NULL DEFAULT 0' },
    { name: 'units_cancelled_same_day_matched',  def: 'INTEGER NOT NULL DEFAULT 0' },
    { name: 'allocation_method',                 def: "TEXT NOT NULL DEFAULT 'no_refund'" },
    { name: 'cancel_exceeds_ordered_warning',    def: 'INTEGER NOT NULL DEFAULT 0' },
    { name: 'allocated_refund_amount_jpy_incl',  def: 'REAL NOT NULL DEFAULT 0' },
    { name: 'refund_amount_same_day_matched_jpy_incl', def: 'REAL NOT NULL DEFAULT 0' },
  ];
  for (const c of phase1bMirrorCols) {
    if (!mrfsdCols.has(c.name)) {
      console.log(`[mirror-db] Phase 1b migration: ALTER TABLE mirror_rakuten_finance_sku_daily ADD COLUMN ${c.name}`);
      db.exec(`ALTER TABLE mirror_rakuten_finance_sku_daily ADD COLUMN ${c.name} ${c.def}`);
    }
  }

  // mirror_yahoo_finance_sku_daily — Yahoo Phase 1 Y-3b (Render 側 daily fact mirror)
  // miniPC の f_yahoo_finance_sku_daily_v1 の payload を受信
  // PK: (date_jst, yahoo_sku_key) — yahoo_sku_key = item_id-sub_code or item_id (variant 別 or 親 SKU)
  // 設計書 v0.4: g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1a設計書_v0.4_20260510.md
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_yahoo_finance_sku_daily (
    date_jst                          TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    yahoo_sku_key                     TEXT NOT NULL CHECK(trim(yahoo_sku_key) <> ''),
    ne_code                           TEXT,
    variant_key                       TEXT NOT NULL DEFAULT '',
    resolution_method                 TEXT NOT NULL CHECK (
      resolution_method IN ('sub_match', 'parent_match', 'manual_map', 'unresolved')
    ),
    unresolved_sku_flag               INTEGER NOT NULL DEFAULT 0,
    product_name                      TEXT NOT NULL DEFAULT '',
    units_ordered                     INTEGER NOT NULL DEFAULT 0,
    units_cancelled                   INTEGER NOT NULL DEFAULT 0,
    units_net_sold                    INTEGER NOT NULL DEFAULT 0,
    sales_principal_jpy_incl          REAL NOT NULL DEFAULT 0,
    sales_postage_jpy_incl            REAL NOT NULL DEFAULT 0,
    gross_sales_jpy_incl              REAL NOT NULL DEFAULT 0,
    net_sales_before_point_jpy_incl   REAL NOT NULL DEFAULT 0,
    listing_sales_estimated_jpy_incl  REAL NOT NULL DEFAULT 0,
    coupon_shop_jpy_incl              REAL NOT NULL DEFAULT 0,
    use_point_jpy_incl                REAL NOT NULL DEFAULT 0,
    mall_fee_jpy_incl                 REAL NOT NULL DEFAULT 0,
    mall_fee_calc_method              TEXT NOT NULL DEFAULT 'estimated_10pct' CHECK (
      mall_fee_calc_method IN ('estimated_10pct', 'actual_statement')
    ),
    mall_fee_estimate_delta_jpy       REAL,
    shipping_cost_jpy_incl            REAL NOT NULL DEFAULT 0,
    shipping_quality                  TEXT NOT NULL CHECK (
      shipping_quality IN ('actual', 'estimated_rates', 'estimated_fallback', 'missing')
    ),
    unit_cost_snapshot_incl           REAL,
    cost_snapshot_date_jst            TEXT,
    latest_unit_cost_reference_incl   REAL,
    cogs_amount_jpy_incl              REAL NOT NULL DEFAULT 0,
    variable_margin_partial_jpy_incl  REAL NOT NULL DEFAULT 0,
    variable_margin_full_jpy_incl     REAL,
    refund_adjusted_net_sales_jpy_incl REAL,
    margin_confidence                 TEXT NOT NULL DEFAULT 'partial' CHECK (
      margin_confidence IN ('partial', 'full')
    ),
    margin_full_finalized_at          TEXT,
    pay_charge_audit_jpy_incl         REAL NOT NULL DEFAULT 0,
    ship_charge_audit_jpy_incl        REAL NOT NULL DEFAULT 0,
    discount_audit_jpy_incl           REAL NOT NULL DEFAULT 0,
    cost_status                       TEXT NOT NULL CHECK (
      cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
    ),
    is_cost_complete                  INTEGER NOT NULL DEFAULT 0,
    data_quality_score                INTEGER NOT NULL DEFAULT 0
                                      CHECK (data_quality_score BETWEEN 0 AND 100),
    price_variance_warning            INTEGER NOT NULL DEFAULT 0,
    source_layer_summary              TEXT NOT NULL DEFAULT '',
    source_row_count                  INTEGER NOT NULL DEFAULT 0,
    built_at                          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_run_id                     TEXT NOT NULL,
    source_row_hash                   TEXT NOT NULL,
    synced_at                         TEXT NOT NULL,
    PRIMARY KEY (date_jst, yahoo_sku_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_myfsd_date  ON mirror_yahoo_finance_sku_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_myfsd_ne    ON mirror_yahoo_finance_sku_daily(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_myfsd_month ON mirror_yahoo_finance_sku_daily(substr(date_jst, 1, 7))');
  db.exec('CREATE INDEX IF NOT EXISTS idx_myfsd_run   ON mirror_yahoo_finance_sku_daily(source_run_id)');

  // Phase 1c-3 用 migration framework (Codex R5 #1 反映、PRAGMA table_info 方式)
  // 現状追加列なし、Phase 1c-3 着手時にここに ALTER TABLE 追加する形
  const myfsdCols = new Set(
    db.prepare("PRAGMA table_info(mirror_yahoo_finance_sku_daily)").all().map(c => c.name)
  );
  // Phase 1a 時点で全列 DDL に含まれてるので追加なし
  // Phase 1c-3 着手時:
  //   { name: 'settlement_fee_jpy_incl', def: 'REAL' }
  //   { name: 'psr_jpy_incl', def: 'REAL' }
  //   { name: 'ad_spend_jpy_incl', def: 'REAL' }
  //   ... (full margin 用列、楽天 + Yahoo 共通スキーマ化検討)

  // mirror_amazon_sku_fees — Amazon手数料キャッシュ（粗利ダッシュボード用）
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_amazon_sku_fees (
    seller_sku          TEXT PRIMARY KEY,
    asin                TEXT,
    fulfillment_channel TEXT,
    referral_fee        REAL,
    referral_fee_rate   REAL,
    fba_fee             REAL,
    variable_closing_fee REAL,
    per_item_fee        REAL,
    total_fee           REAL,
    price_used          REAL,
    fetched_at          TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirasf_asin ON mirror_amazon_sku_fees(asin)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirasf_channel ON mirror_amazon_sku_fees(fulfillment_channel)');

  // ─── mart_rakuten: 楽天売上集計ツール用 ───

  // mart_rakuten_monthly_summary — 月次確定集計
  db.exec(`CREATE TABLE IF NOT EXISTS mart_rakuten_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    by_tax            TEXT,
    by_segment        TEXT,
    excluded          TEXT,
    mf_row            TEXT,
    ad_cost           REAL DEFAULT 0,
    billing           TEXT,
    confirmed_at      TEXT NOT NULL
  )`);

  // mart_rakuten_upload_log — アップロード履歴
  db.exec(`CREATE TABLE IF NOT EXISTS mart_rakuten_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── mart_amazon: Amazon売上集計ツール用 ───

  // mart_amazon_monthly_summary — 月次確定集計
  db.exec(`CREATE TABLE IF NOT EXISTS mart_amazon_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    by_tax            TEXT,
    by_segment        TEXT,
    excluded          TEXT,
    mf_row            TEXT,
    ad_cost           REAL DEFAULT 0,
    confirmed_at      TEXT NOT NULL,
    csv_filename      TEXT
  )`);

  // mart_amazon_upload_log — アップロード履歴
  db.exec(`CREATE TABLE IF NOT EXISTS mart_amazon_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    filename          TEXT,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── mart_amazon_usa: 米国Amazon売上集計ツール用 ───
  // 全売上=セグメント4(輸出)、USD→JPY換算が必要。税率分類なし。
  db.exec(`CREATE TABLE IF NOT EXISTS mart_amazon_usa_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    exchange_rate     REAL,      -- 確定時のUSD→JPYレート
    usd_row           TEXT,      -- JSON: USDベース集計
    jpy_row           TEXT,      -- JSON: JPY換算後集計
    mgmt_row          TEXT,      -- JSON: 管理会計用15列集計（セグメント4・円建）
    cost_total        REAL,      -- 原価合計(税抜・円)
    ad_cost           REAL DEFAULT 0,  -- 広告費(税込・円・手入力)
    confirmed_at      TEXT NOT NULL,
    csv_filename      TEXT
  )`);
  // 既存テーブルに ad_cost カラムが無ければ追加
  try {
    const cols = db.prepare("PRAGMA table_info(mart_amazon_usa_monthly_summary)").all();
    if (!cols.some(c => c.name === 'ad_cost')) {
      db.exec('ALTER TABLE mart_amazon_usa_monthly_summary ADD COLUMN ad_cost REAL DEFAULT 0');
    }
  } catch {}

  db.exec(`CREATE TABLE IF NOT EXISTS mart_amazon_usa_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    filename          TEXT,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── mart_yahoo: Yahoo!売上集計ツール用 ───

  // mart_yahoo_monthly_summary — 月次確定集計
  db.exec(`CREATE TABLE IF NOT EXISTS mart_yahoo_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    by_tax            TEXT,
    by_segment        TEXT,
    excluded          TEXT,
    mf_row            TEXT,
    ad_cost           REAL DEFAULT 0,
    billing           TEXT,
    confirmed_at      TEXT NOT NULL
  )`);

  // mart_yahoo_upload_log — アップロード履歴
  db.exec(`CREATE TABLE IF NOT EXISTS mart_yahoo_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── mart_aupay: auペイマーケット売上集計ツール用 ───

  // mart_aupay_monthly_summary — 月次確定集計
  db.exec(`CREATE TABLE IF NOT EXISTS mart_aupay_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    by_tax            TEXT,
    by_segment        TEXT,
    excluded          TEXT,
    mf_row            TEXT,
    pf_fee            REAL DEFAULT 0,
    ad_cost           REAL DEFAULT 0,
    confirmed_at      TEXT NOT NULL
  )`);

  // mart_aupay_upload_log — アップロード履歴
  db.exec(`CREATE TABLE IF NOT EXISTS mart_aupay_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── mart_linegift: LINEギフト売上集計ツール用 ───

  // mart_linegift_monthly_summary — 月次確定集計
  db.exec(`CREATE TABLE IF NOT EXISTS mart_linegift_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    by_tax            TEXT,
    by_segment        TEXT,
    excluded          TEXT,
    mf_row            TEXT,
    pf_fee            REAL DEFAULT 0,
    ad_cost           REAL DEFAULT 0,
    confirmed_at      TEXT NOT NULL
  )`);

  // mart_linegift_upload_log — アップロード履歴
  db.exec(`CREATE TABLE IF NOT EXISTS mart_linegift_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── mart_qoo10: Qoo10売上集計ツール用 ───

  // mart_qoo10_monthly_summary — 月次確定集計
  db.exec(`CREATE TABLE IF NOT EXISTS mart_qoo10_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    by_tax            TEXT,
    by_segment        TEXT,
    excluded          TEXT,
    mf_row            TEXT,
    pf_fee            REAL DEFAULT 0,
    ad_cost           REAL DEFAULT 0,
    confirmed_at      TEXT NOT NULL
  )`);

  // mart_qoo10_upload_log — アップロード履歴
  db.exec(`CREATE TABLE IF NOT EXISTS mart_qoo10_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── mart_mercari: メルカリショップス売上集計ツール用 ───

  // mart_mercari_monthly_summary — 月次確定集計
  db.exec(`CREATE TABLE IF NOT EXISTS mart_mercari_monthly_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL UNIQUE,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    by_tax            TEXT,
    by_segment        TEXT,
    excluded          TEXT,
    mf_row            TEXT,
    pf_fee            REAL DEFAULT 0,
    shipping_fee      REAL DEFAULT 0,
    coupon_total      REAL DEFAULT 0,
    confirmed_at      TEXT NOT NULL
  )`);

  // mart_mercari_upload_log — アップロード履歴
  db.exec(`CREATE TABLE IF NOT EXISTS mart_mercari_upload_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month        TEXT NOT NULL,
    total_rows        INTEGER,
    resolved_count    INTEGER,
    unresolved_count  INTEGER,
    uploaded_at       TEXT NOT NULL
  )`);

  // ─── 管理会計用 統合テーブル ───

  // mart_monthly_segment_sales — 全モール統合月次セグメント別集計（税抜）
  db.exec(`CREATE TABLE IF NOT EXISTS mart_monthly_segment_sales (
    year_month      TEXT NOT NULL,
    mall_id         TEXT NOT NULL,
    segment         INTEGER NOT NULL,
    sales           REAL NOT NULL DEFAULT 0,
    cost            REAL NOT NULL DEFAULT 0,
    pf_fee          REAL NOT NULL DEFAULT 0,
    ad_cost         REAL NOT NULL DEFAULT 0,
    confirmed_at    TEXT,
    confirmed_by    TEXT,
    source_file     TEXT,
    source_hash     TEXT,
    import_run_id   TEXT,
    logic_version   TEXT DEFAULT 'v1',
    PRIMARY KEY (year_month, mall_id, segment)
  )`);

  // mart_monthly_shared_costs — 月次共通費用（運賃・資材費）※互換維持
  db.exec(`CREATE TABLE IF NOT EXISTS mart_monthly_shared_costs (
    year_month      TEXT PRIMARY KEY,
    freight_total   REAL NOT NULL DEFAULT 0,
    material_total  REAL NOT NULL DEFAULT 0,
    confirmed_at    TEXT,
    source_file     TEXT,
    freight_detail  TEXT DEFAULT '{}',
    material_detail TEXT DEFAULT '{}'
  )`);

  // ─── 売上分類別粗利集計（管理会計） ───

  // mgmt_freight_costs — 運賃明細（ヒストリカル保持）
  db.exec(`CREATE TABLE IF NOT EXISTS mgmt_freight_costs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month      TEXT NOT NULL CHECK(year_month GLOB '????-??'),
    carrier         TEXT NOT NULL,
    amount          INTEGER NOT NULL DEFAULT 0,
    cost_scope      TEXT NOT NULL DEFAULT 'shared',
    target_segment  INTEGER,
    target_mall_id  TEXT,
    note            TEXT,
    entered_by      TEXT,
    entered_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(year_month, carrier)
  )`);

  // mgmt_material_costs — 資材費明細（ヒストリカル保持）
  db.exec(`CREATE TABLE IF NOT EXISTS mgmt_material_costs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month      TEXT NOT NULL CHECK(year_month GLOB '????-??'),
    supplier        TEXT NOT NULL,
    amount          INTEGER NOT NULL DEFAULT 0,
    note            TEXT,
    entered_by      TEXT,
    entered_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(year_month, supplier)
  )`);

  // mgmt_monthly_closing — 月次締めヘッダ
  db.exec(`CREATE TABLE IF NOT EXISTS mgmt_monthly_closing (
    year_month      TEXT PRIMARY KEY CHECK(year_month GLOB '????-??'),
    fiscal_year     INTEGER NOT NULL,
    fiscal_month    INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'draft',
    freight_total   INTEGER NOT NULL DEFAULT 0,
    material_total  INTEGER NOT NULL DEFAULT 0,
    confirmed_at    TEXT,
    confirmed_by    TEXT,
    calc_version    TEXT DEFAULT 'v1',
    source_hash     TEXT
  )`);

  // mgmt_monthly_pl — 月次PL（PF×セグメント別確定集計）
  db.exec(`CREATE TABLE IF NOT EXISTS mgmt_monthly_pl (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month      TEXT NOT NULL CHECK(year_month GLOB '????-??'),
    mall_id         TEXT NOT NULL,
    segment         INTEGER NOT NULL,
    sales           INTEGER NOT NULL DEFAULT 0,
    sales_ratio     REAL DEFAULT 0,
    cost            INTEGER NOT NULL DEFAULT 0,
    pf_fee          INTEGER NOT NULL DEFAULT 0,
    ad_cost         INTEGER NOT NULL DEFAULT 0,
    freight         INTEGER NOT NULL DEFAULT 0,
    material        INTEGER NOT NULL DEFAULT 0,
    variable_cost   INTEGER NOT NULL DEFAULT 0,
    gross_profit    INTEGER NOT NULL DEFAULT 0,
    gross_margin    REAL DEFAULT 0,
    fiscal_year     INTEGER,
    UNIQUE(year_month, mall_id, segment)
  )`);

  // ─── 商品収益性ダッシュボード タブB（在庫整理・撤退判断支援） ───

  // mirror_stock_monthly_snapshot — 月末在庫スナップショット（ミニPC→Render同期）
  //   GMROI計算の「移動平均在庫数」算出に使用
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_stock_monthly_snapshot (
    年月              TEXT NOT NULL,
    商品コード        TEXT NOT NULL,
    月末在庫数        INTEGER NOT NULL DEFAULT 0,
    月末引当数        INTEGER DEFAULT 0,
    snapshot_source   TEXT,
    captured_at       TEXT,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (年月, 商品コード)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_msms_month ON mirror_stock_monthly_snapshot(年月)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_msms_sku ON mirror_stock_monthly_snapshot(商品コード)');

  // product_retirement_status — 撤退判断ステータス
  //   ★ Render側のみの業務状態テーブル（ミニPC同期対象外）
  //   ユーザー操作で更新、判断時メトリクス・閾値・処分率をスナップショット保存
  db.exec(`CREATE TABLE IF NOT EXISTS product_retirement_status (
    ne_product_code       TEXT PRIMARY KEY,
    status                TEXT NOT NULL,
    decided_by            TEXT,
    decided_at            TEXT,
    reason                TEXT,
    next_review_date      TEXT,
    plan_details_json     TEXT,
    decision_metrics_json TEXT,
    thresholds_json       TEXT,
    disposal_rate         REAL,
    updated_at            TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_prs_status ON product_retirement_status(status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_prs_next_review ON product_retirement_status(next_review_date)');

  // dashboard_settings — ダッシュボード設定（閾値マトリクス・早期警戒設定・処分率デフォルト等）
  //   Render側のみ、画面から編集可能
  //   key の例: 'retirement_thresholds', 'early_warning', 'disposal_rate_default'
  db.exec(`CREATE TABLE IF NOT EXISTS dashboard_settings (
    key          TEXT PRIMARY KEY,
    value_json   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    updated_by   TEXT
  )`);

  // ========================================================================
  // ▼▼▼ MFクラウド会計ダッシュボード Phase 1a (Codex 4 ラウンド確定) ▼▼▼
  //
  // 命名規則:
  //   mirror_mf_*       miniPC mart_mf_* から sync 受信した read-only データ
  //   mart_mf_anomaly_* Render local writable (ack/snooze 操作テーブル)
  //
  // run_id 設計:
  //   - miniPC mf_publish_runs.run_id (INTEGER) を Render でそのまま PK 採用
  //   - mirror_mf_publish_runs.status: 'pending_sync' → 'success' を finalize endpoint で flip
  //   - 'failed' run は Render に送らない (miniPC 監査のみ)
  //
  // 公開 VIEW:
  //   v_mirror_mf_*_latest  WHERE run_id = MAX success run のみ公開
  //
  // GC ポリシー (Phase 2 別途実装):
  //   Render 側は直近 7 日 + 各月初 run のみ永久保持、それ以外は削除
  //   mf_publish_runs 自体は永久保持 (監査)
  // ========================================================================

  // mirror_mf_publish_runs — publish run カタログ (miniPC 由来 run_id を PK に保持)
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_publish_runs (
    run_id          INTEGER PRIMARY KEY,
    scope           TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending_sync','success','failed')),
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    error_message   TEXT,
    source_run_hash TEXT,
    synced_at       TEXT NOT NULL,
    finalized_at    TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_publish_runs_scope_status ON mirror_mf_publish_runs(scope, status, started_at DESC)');

  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_executive_top (
    run_id                              INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    snapshot_date                       TEXT NOT NULL,
    current_month_ym                    TEXT NOT NULL,
    sales_mtd_excl_tax                  INTEGER NOT NULL DEFAULT 0,
    gross_profit_mtd_excl_tax           INTEGER NOT NULL DEFAULT 0,
    operating_income_mtd_excl_tax       INTEGER NOT NULL DEFAULT 0,
    sales_month_end_forecast            INTEGER NOT NULL DEFAULT 0,
    gross_profit_month_end_forecast     INTEGER NOT NULL DEFAULT 0,
    operating_income_month_end_forecast INTEGER NOT NULL DEFAULT 0,
    forecast_status                     TEXT NOT NULL DEFAULT '会計確定待ち',
    yoy_sales_pct                       REAL,
    yoy_gross_profit_pct                REAL,
    yoy_operating_income_pct            REAL,
    cash_balance_total                  INTEGER NOT NULL DEFAULT 0,
    cash_balance_json                   TEXT NOT NULL DEFAULT '[]',
    danger_signals_json                 TEXT NOT NULL DEFAULT '[]',
    data_window_from                    TEXT,
    data_window_to                      TEXT,
    reliability_label                   TEXT NOT NULL DEFAULT '業務速報',
    source_row_hash                     TEXT NOT NULL,
    synced_at                           TEXT NOT NULL,
    PRIMARY KEY (run_id)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_executive_top_snapshot ON mirror_mf_executive_top(snapshot_date)');

  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_pl_monthly (
    run_id           INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    month_ym         TEXT NOT NULL,
    role_key         TEXT NOT NULL,
    amount_excl_tax  INTEGER NOT NULL DEFAULT 0,
    tax_amount       INTEGER NOT NULL DEFAULT 0,
    line_count       INTEGER NOT NULL DEFAULT 0,
    is_realized_only INTEGER NOT NULL DEFAULT 1,
    source_row_hash  TEXT NOT NULL,
    synced_at        TEXT NOT NULL,
    PRIMARY KEY (run_id, month_ym, role_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_pl_monthly_role ON mirror_mf_pl_monthly(role_key, month_ym)');

  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_channel_sales (
    run_id                      INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    month_ym                    TEXT NOT NULL,
    channel_key                 TEXT NOT NULL,
    channel_display_name        TEXT NOT NULL,
    gross_sales_excl_tax        INTEGER NOT NULL DEFAULT 0,
    pf_fee_excl_tax             INTEGER NOT NULL DEFAULT 0,
    ad_cost_excl_tax            INTEGER NOT NULL DEFAULT 0,
    fba_fee_excl_tax            INTEGER NOT NULL DEFAULT 0,
    net_sales_after_pf_excl_tax INTEGER NOT NULL DEFAULT 0,
    unmapped_amount_excl_tax    INTEGER NOT NULL DEFAULT 0,
    mapping_coverage_pct        REAL NOT NULL DEFAULT 0,
    source_row_hash             TEXT NOT NULL,
    synced_at                   TEXT NOT NULL,
    PRIMARY KEY (run_id, month_ym, channel_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_channel_sales_month ON mirror_mf_channel_sales(month_ym, channel_key)');

  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_cash_events_daily (
    run_id           INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    movement_date    TEXT NOT NULL,
    bank_account_key TEXT NOT NULL,
    direction        TEXT NOT NULL CHECK (direction IN ('in','out')),
    amount_excl_tax  INTEGER NOT NULL DEFAULT 0,
    event_count      INTEGER NOT NULL DEFAULT 0,
    source_row_hash  TEXT NOT NULL,
    synced_at        TEXT NOT NULL,
    PRIMARY KEY (run_id, movement_date, bank_account_key, direction)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_cash_events_bank ON mirror_mf_cash_events_daily(bank_account_key, movement_date)');

  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_balance_snapshot_monthly (
    run_id                   INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    month_ym                 TEXT NOT NULL,
    account_key              TEXT NOT NULL,
    account_name             TEXT NOT NULL,
    sub_account_name         TEXT,
    role_key                 TEXT,
    closing_balance_excl_tax INTEGER NOT NULL DEFAULT 0,
    source_row_hash          TEXT NOT NULL,
    synced_at                TEXT NOT NULL,
    PRIMARY KEY (run_id, month_ym, account_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_balance_role ON mirror_mf_balance_snapshot_monthly(role_key, month_ym)');

  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_anomaly_signals (
    run_id             INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    signal_id          INTEGER NOT NULL,
    detected_at        TEXT NOT NULL,
    signal_code        TEXT NOT NULL,
    signal_key         TEXT,
    severity           TEXT NOT NULL CHECK (severity IN ('info','warn','high','critical')),
    severity_rank      INTEGER,
    title              TEXT NOT NULL,
    description        TEXT NOT NULL,
    observed_value     REAL,
    threshold_value    REAL,
    related_entity_key TEXT,
    recommended_action TEXT,
    source_mart        TEXT,
    source_row_hash    TEXT NOT NULL,
    synced_at          TEXT NOT NULL,
    PRIMARY KEY (run_id, signal_id)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_anomaly_severity ON mirror_mf_anomaly_signals(severity_rank DESC, detected_at DESC)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_anomaly_signal_key ON mirror_mf_anomaly_signals(signal_key)');

  // mart_mf_* (Render local writable) — ack/snooze 等のユーザー操作
  db.exec(`CREATE TABLE IF NOT EXISTS mart_mf_anomaly_signal_state (
    signal_key     TEXT PRIMARY KEY,
    status         TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','ack','snoozed','closed')),
    suppress_until TEXT,
    resolved_at    TEXT,
    acked_by       TEXT,
    acked_at       TEXT,
    version_no     INTEGER NOT NULL DEFAULT 1,
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mart_mf_signal_state_status ON mart_mf_anomaly_signal_state(status, suppress_until)');

  db.exec(`CREATE TABLE IF NOT EXISTS mart_mf_anomaly_state_audit (
    audit_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_key  TEXT NOT NULL,
    action      TEXT NOT NULL CHECK (action IN ('ack','snooze','unsnooze','close','reopen')),
    actor       TEXT,
    detail      TEXT,
    occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mart_mf_state_audit_signal ON mart_mf_anomaly_state_audit(signal_key, occurred_at DESC)');

  // 公開 VIEW (latest_successful_run のみ参照)
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_executive_top_latest');
  db.exec(`CREATE VIEW v_mirror_mf_executive_top_latest AS
    SELECT m.* FROM mirror_mf_executive_top m
    WHERE m.run_id = (SELECT MAX(run_id) FROM mirror_mf_publish_runs WHERE status = 'success' AND scope IN ('all','executive_top'))`);
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_pl_monthly_latest');
  db.exec(`CREATE VIEW v_mirror_mf_pl_monthly_latest AS
    SELECT m.* FROM mirror_mf_pl_monthly m
    WHERE m.run_id = (SELECT MAX(run_id) FROM mirror_mf_publish_runs WHERE status = 'success' AND scope IN ('all','base','pl_monthly'))`);
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_channel_sales_latest');
  db.exec(`CREATE VIEW v_mirror_mf_channel_sales_latest AS
    SELECT m.* FROM mirror_mf_channel_sales m
    WHERE m.run_id = (SELECT MAX(run_id) FROM mirror_mf_publish_runs WHERE status = 'success' AND scope IN ('all','channel_sales'))`);
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_cash_events_daily_latest');
  db.exec(`CREATE VIEW v_mirror_mf_cash_events_daily_latest AS
    SELECT m.* FROM mirror_mf_cash_events_daily m
    WHERE m.run_id = (SELECT MAX(run_id) FROM mirror_mf_publish_runs WHERE status = 'success' AND scope IN ('all','base','cash_events_daily'))`);
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_balance_snapshot_monthly_latest');
  db.exec(`CREATE VIEW v_mirror_mf_balance_snapshot_monthly_latest AS
    SELECT m.* FROM mirror_mf_balance_snapshot_monthly m
    WHERE m.run_id = (SELECT MAX(run_id) FROM mirror_mf_publish_runs WHERE status = 'success' AND scope IN ('all','base','balance_snapshot_monthly'))`);
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_anomaly_signals_latest');
  db.exec(`CREATE VIEW v_mirror_mf_anomaly_signals_latest AS
    SELECT m.*, s.status AS state_status, s.suppress_until, s.acked_by, s.acked_at
    FROM mirror_mf_anomaly_signals m
    LEFT JOIN mart_mf_anomaly_signal_state s ON s.signal_key = m.signal_key
    WHERE m.run_id = (SELECT MAX(run_id) FROM mirror_mf_publish_runs WHERE status = 'success' AND scope IN ('all','anomaly_signals'))`);
  // ▲▲▲ MFクラウド会計ダッシュボード Phase 1a 終了 ▲▲▲
}
