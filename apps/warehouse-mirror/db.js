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
  // PRAGMA は接続単位の設定。SQLite のデフォルトは foreign_keys=OFF / recursive_triggers=OFF なので、
  // f_mis_shipments の FK 制約 と append-only trigger を機能させるために毎接続で明示する必要がある。
  // 設計書: 誤出荷管理システム_設計書_v5.md (中身 v7.3) の「実装要件」セクション参照。
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  // WAL 下では synchronous=NORMAL が安全かつ高速 (commit ごとの fsync を省き、checkpoint 時のみ同期)。
  // Render の persistent disk は network-attached で fsync が遅く、daily-sync 取込 / mgmt 自動同期 /
  // マート再構築など書き込みのたびに体感が固まる主因になりうるため明示する (既定 FULL → NORMAL)。
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('recursive_triggers = ON');
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
  //   - sort_order は m_sku_components.sort_order 由来。セット構成の表示順 + 代表 ne_code
  //     (sort_order=0) の確定に使う。FBA 在庫補充の mirror 直読み adapter (primary ne_code 決定) が依存。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sku_resolved (
    seller_sku         TEXT NOT NULL,
    ne_code            TEXT NOT NULL,
    quantity           INTEGER NOT NULL,
    source             TEXT NOT NULL CHECK (source IN ('master', 'auto')),
    商品名             TEXT,
    source_updated_at  TEXT,
    sort_order         INTEGER NOT NULL DEFAULT 0,
    synced_at          TEXT NOT NULL,
    PRIMARY KEY (seller_sku, ne_code)
  )`);
  // 既存 mirror DB への migration (CREATE TABLE IF NOT EXISTS は新列を足さない)
  addColumnIfMissing('mirror_sku_resolved', 'sort_order', 'INTEGER NOT NULL DEFAULT 0');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirres_sku ON mirror_sku_resolved(seller_sku)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirres_ne ON mirror_sku_resolved(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirres_src ON mirror_sku_resolved(source)');

  // mirror_sku_master — m_sku_master の 1 SKU = 1 行 ミラー
  // 用途: 「マスタ登録ツールで登録済みだが Sheets 側の商品コード変換テーブルに未記載」の差分検出
  // 経緯: mirror_sku_resolved は seller_sku × ne_code 粒度 (派生 view) で SKU 一覧用途に不自然なため、
  //       m_sku_master.{seller_sku, 商品名, created_at, updated_at} を 1 SKU 1 行で持つ表を別途立てる
  //       (Codex review 2026-05-13: A''案 = master の mirror を独立に持つ)
  // source_created_at / source_updated_at は m_sku_master の ISO 8601 UTC ('YYYY-MM-DDTHH:MM:SS.fffZ') を素通し
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sku_master (
    seller_sku         TEXT NOT NULL PRIMARY KEY,
    商品名             TEXT,
    source_created_at  TEXT,
    source_updated_at  TEXT,
    synced_at          TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mir_sku_master_updated ON mirror_sku_master(source_updated_at)');

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

  // mirror_amazon_ads_sku_daily — Amazon 広告費 SKU/ASIN 別 (amazon-dashboard PR-A、2026-07-06)
  // miniPC fact_ad_spend (spAdvertisedProduct 由来、日本語列) の英語キー payload を受信。
  // target は LOWER 済み SKU または ASIN (target_granularity で区別)。
  // Auto-Targeting 等の未配賦広告費は含まない → campaign_daily との差分で導出。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_amazon_ads_sku_daily (
    date_jst            TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    mall                TEXT NOT NULL DEFAULT 'amazon',
    campaign_id         TEXT NOT NULL,
    ad_type             TEXT NOT NULL DEFAULT 'SP',
    target              TEXT NOT NULL CHECK(trim(target) <> ''),
    target_granularity  TEXT NOT NULL CHECK(target_granularity IN ('sku','asin')),
    clicks              INTEGER NOT NULL DEFAULT 0,
    impressions         INTEGER NOT NULL DEFAULT 0,
    ad_cost             REAL NOT NULL DEFAULT 0,
    ad_sales            REAL NOT NULL DEFAULT 0,
    ad_units            INTEGER NOT NULL DEFAULT 0,
    source_run_id       TEXT NOT NULL,
    source_row_hash     TEXT NOT NULL,
    synced_at           TEXT NOT NULL,
    PRIMARY KEY (date_jst, mall, campaign_id, ad_type, target, target_granularity)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_maask_date ON mirror_amazon_ads_sku_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_maask_target ON mirror_amazon_ads_sku_daily(target)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_maask_month ON mirror_amazon_ads_sku_daily(substr(date_jst, 1, 7))');

  // mirror_amazon_ads_campaign_daily — Amazon 広告費 キャンペーン単位 (amazon-dashboard PR-A)
  // miniPC fact_ad_spend_campaign (spCampaigns 由来) の英語キー payload を受信。
  // キャンペーン単位の全広告費 = Amazon Ads Console と一致する正本。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_amazon_ads_campaign_daily (
    date_jst        TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    mall            TEXT NOT NULL DEFAULT 'amazon',
    campaign_id     TEXT NOT NULL,
    campaign_name   TEXT NOT NULL DEFAULT '',
    ad_type         TEXT NOT NULL DEFAULT 'SP',
    campaign_status TEXT NOT NULL DEFAULT '',
    clicks          INTEGER NOT NULL DEFAULT 0,
    impressions     INTEGER NOT NULL DEFAULT 0,
    ad_cost         REAL NOT NULL DEFAULT 0,
    ad_sales_1d     REAL NOT NULL DEFAULT 0,
    ad_sales_7d     REAL NOT NULL DEFAULT 0,
    ad_sales_14d    REAL NOT NULL DEFAULT 0,
    ad_sales_30d    REAL NOT NULL DEFAULT 0,
    ad_units_1d     INTEGER NOT NULL DEFAULT 0,
    source_run_id   TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    synced_at       TEXT NOT NULL,
    PRIMARY KEY (date_jst, mall, campaign_id, ad_type)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_maacd_date ON mirror_amazon_ads_campaign_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_maacd_month ON mirror_amazon_ads_campaign_daily(substr(date_jst, 1, 7))');

  // mirror_amazon_account_fees_monthly — アカウント単位フィー月次 (amazon-dashboard PR-C)
  // SKU に紐付かない保管料/長期在庫追加手数料/返送等。date_jst = 月初日 (YYYY-MM-01)。
  // 金額は Amazon 符号のまま (負 = 費用、Correction/Reversal 込み net)。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_amazon_account_fees_monthly (
    date_jst        TEXT NOT NULL CHECK(date_jst GLOB '????-??-01'),
    fee_type        TEXT NOT NULL CHECK(fee_type IN (
      'storage','long_term_storage','removal','inbound_defect','low_inventory','subscription','other_account_fee'
    )),
    amount_jpy      REAL NOT NULL DEFAULT 0,
    row_count       INTEGER NOT NULL DEFAULT 0,
    source_run_id   TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    synced_at       TEXT NOT NULL,
    PRIMARY KEY (date_jst, fee_type)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_maafm_date ON mirror_amazon_account_fees_monthly(date_jst)');

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

  // mirror_aupay_finance_sku_daily — au PAY マーケット Phase 1 A-2 (Render 側 daily fact mirror)
  // miniPC の f_aupay_finance_sku_daily_v1 の payload を受信
  // PK: (date_jst, aupay_sku_key) — aupay_sku_key = item_code (variant 粒度は variant_key=item_option 保持)
  // 設計書 v0.4: g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_aupay_finance_sku_daily (
    date_jst                          TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    aupay_sku_key                     TEXT NOT NULL CHECK(trim(aupay_sku_key) <> ''),
    ne_code                           TEXT,
    variant_key                       TEXT NOT NULL DEFAULT '',
    resolution_method                 TEXT NOT NULL CHECK (
      resolution_method IN ('master_match', 'manual_map', 'unresolved')
    ),
    unresolved_sku_flag               INTEGER NOT NULL DEFAULT 0,
    product_name                      TEXT NOT NULL DEFAULT '',
    units_ordered                     INTEGER NOT NULL DEFAULT 0,
    units_cancelled                   INTEGER NOT NULL DEFAULT 0,
    units_net_sold                    INTEGER NOT NULL DEFAULT 0,
    sales_principal_jpy_incl          REAL NOT NULL DEFAULT 0,
    postage_allocated_jpy_incl        REAL NOT NULL DEFAULT 0,
    gross_sales_jpy_incl              REAL NOT NULL DEFAULT 0,
    net_sales_after_coupon_jpy_incl   REAL NOT NULL DEFAULT 0,
    request_price_jpy_incl            REAL NOT NULL DEFAULT 0,
    coupon_shop_jpy_incl              REAL NOT NULL DEFAULT 0,
    gift_point_jpy_incl               REAL NOT NULL DEFAULT 0,
    use_ponta_point_jpy_incl          REAL NOT NULL DEFAULT 0,
    use_au_point_jpy_incl             REAL NOT NULL DEFAULT 0,
    premium_member_point_jpy_incl     REAL NOT NULL DEFAULT 0,
    point_cost_pending_jpy_incl       REAL NOT NULL DEFAULT 0,
    tax_normal_sales_jpy_incl         REAL NOT NULL DEFAULT 0,
    tax_reduced_sales_jpy_incl        REAL NOT NULL DEFAULT 0,
    tax_free_sales_jpy_incl           REAL NOT NULL DEFAULT 0,
    mall_fee_jpy_incl                 REAL,
    mall_fee_rate_applied             REAL,
    mall_fee_calc_method              TEXT NOT NULL DEFAULT 'unknown' CHECK (
      mall_fee_calc_method IN ('estimated_rate', 'actual_statement', 'unknown')
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
    before_discount_jpy_incl          REAL NOT NULL DEFAULT 0,
    detail_discount_jpy_incl          REAL NOT NULL DEFAULT 0,
    charge_allocated_jpy_incl         REAL NOT NULL DEFAULT 0,
    item_option_jpy_incl              REAL NOT NULL DEFAULT 0,
    gift_wrapping_jpy_incl            REAL NOT NULL DEFAULT 0,
    cost_status                       TEXT NOT NULL CHECK (
      cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
    ),
    is_cost_complete                  INTEGER NOT NULL DEFAULT 0,
    data_quality_score                INTEGER NOT NULL DEFAULT 0
                                      CHECK (data_quality_score BETWEEN 0 AND 100),
    price_variance_warning            INTEGER NOT NULL DEFAULT 0,
    order_count                       INTEGER NOT NULL DEFAULT 0,
    line_count                        INTEGER NOT NULL DEFAULT 0,
    source_layer_summary              TEXT NOT NULL DEFAULT '',
    source_row_count                  INTEGER NOT NULL DEFAULT 0,
    built_at                          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_run_id                     TEXT NOT NULL,
    source_row_hash                   TEXT NOT NULL,
    synced_at                         TEXT NOT NULL,
    PRIMARY KEY (date_jst, aupay_sku_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_date  ON mirror_aupay_finance_sku_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_ne    ON mirror_aupay_finance_sku_daily(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_month ON mirror_aupay_finance_sku_daily(substr(date_jst, 1, 7))');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mafsd_run   ON mirror_aupay_finance_sku_daily(source_run_id)');
  // Phase B (full margin backfill) 着手時の migration framework
  const mafsdCols = new Set(
    db.prepare("PRAGMA table_info(mirror_aupay_finance_sku_daily)").all().map(c => c.name)
  );
  void mafsdCols; // Phase A 時点で全列 DDL に含まれてるので追加なし

  // mirror_linegift_finance_sku_daily — LINEギフト Phase 1 A-3 (Render 側 daily fact mirror)
  // miniPC の f_linegift_finance_sku_daily_v1 の payload を受信
  // PK: (date_jst, sku_code) — sku_code = variation.code (LOWER(TRIM())、100% master_match 想定)
  // 設計書 v0.5: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.5_20260515.md §9
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_linegift_finance_sku_daily (
    date_jst                          TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    sku_code                          TEXT NOT NULL CHECK(trim(sku_code) <> ''),
    ne_code                           TEXT,
    parent_item_code                  TEXT NOT NULL DEFAULT '',
    variant_key                       TEXT NOT NULL DEFAULT '',
    resolution_method                 TEXT NOT NULL CHECK (
      resolution_method IN ('master_match', 'parent_match', 'unresolved')
    ),
    unresolved_sku_flag               INTEGER NOT NULL DEFAULT 0,
    product_name                      TEXT NOT NULL DEFAULT '',
    units_ordered                     INTEGER NOT NULL DEFAULT 0,
    units_cancelled                   INTEGER NOT NULL DEFAULT 0,
    units_net_sold                    INTEGER NOT NULL DEFAULT 0,
    sales_principal_jpy_incl          REAL NOT NULL DEFAULT 0,
    gross_sales_jpy_incl              REAL NOT NULL DEFAULT 0,
    mall_fee_jpy_incl                 REAL NOT NULL DEFAULT 0,
    mall_fee_calc_method              TEXT NOT NULL DEFAULT 'actual_api' CHECK (
      mall_fee_calc_method IN ('actual_api', 'actual_statement', 'estimated_rate', 'unknown')
    ),
    mall_fee_estimate_delta_jpy       REAL,
    shipping_cost_jpy_incl            REAL NOT NULL DEFAULT 0,
    shipping_quality                  TEXT NOT NULL CHECK (
      shipping_quality IN ('no_shipping_in_api', 'actual_api', 'estimated_rates', 'estimated_fallback', 'missing')
    ),
    unit_cost_snapshot_incl           REAL,
    cost_snapshot_date_jst            TEXT,
    latest_unit_cost_reference_incl   REAL,
    cogs_amount_jpy_incl              REAL NOT NULL DEFAULT 0,
    variable_margin_jpy_incl          REAL NOT NULL DEFAULT 0,
    refund_adjusted_net_sales_jpy_incl REAL,
    margin_confidence                 TEXT NOT NULL DEFAULT 'provisional_full_candidate' CHECK (
      margin_confidence IN ('provisional_full_candidate', 'full_minus_returns', 'full')
    ),
    margin_full_finalized_at          TEXT,
    -- LINEギフト 特有 audit
    recognized_on_jst                 TEXT,
    bought_date_jst                   TEXT,
    delivered_lag_days                INTEGER,
    received_lag_days                 INTEGER,
    is_delivery_by_hand               INTEGER,
    delivery_agent                    TEXT,
    -- 90日境界 frozen horizon
    first_seen_in_api_at              TEXT,
    last_seen_in_api_at               TEXT,
    is_frozen_after_horizon           INTEGER NOT NULL DEFAULT 0,
    -- 品質
    cost_status                       TEXT NOT NULL CHECK (
      cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
    ),
    is_cost_complete                  INTEGER NOT NULL DEFAULT 0,
    data_quality_score                INTEGER NOT NULL DEFAULT 0
                                      CHECK (data_quality_score BETWEEN 0 AND 100),
    -- メタ
    order_count                       INTEGER NOT NULL DEFAULT 0,
    line_count                        INTEGER NOT NULL DEFAULT 0,
    source_layer_summary              TEXT NOT NULL DEFAULT '',
    source_row_count                  INTEGER NOT NULL DEFAULT 0,
    built_at                          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_run_id                     TEXT NOT NULL,
    source_row_hash                   TEXT NOT NULL,
    synced_at                         TEXT NOT NULL,
    PRIMARY KEY (date_jst, sku_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlfsd_date   ON mirror_linegift_finance_sku_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlfsd_ne     ON mirror_linegift_finance_sku_daily(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlfsd_month  ON mirror_linegift_finance_sku_daily(substr(date_jst, 1, 7))');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlfsd_run    ON mirror_linegift_finance_sku_daily(source_run_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlfsd_parent ON mirror_linegift_finance_sku_daily(parent_item_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlfsd_frozen ON mirror_linegift_finance_sku_daily(is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1');

  // mirror_linegift_orders — LINEギフト v1.2 (2026-05-28、PR-H)
  //   miniPC raw_linegift_orders から「集計用 subset」だけを Render に sync。
  //   時間帯ヒートマップ (曜日 × 00-23 時) と将来のリピート/同梱分析の基盤。
  //   ★ PII 列 (user_name=LINE ID / address_* / delivery_* / sku_name / parent_item_name)
  //     は Render に送らない (sender 側で SELECT 列を制限、本テーブルにも列を持たない)。
  //   PK: order_id (1注文1商品 = order_id 単一 PK、miniPC raw と同じ grain)
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_linegift_orders (
    order_id                  TEXT NOT NULL PRIMARY KEY,
    status                    TEXT,                    -- 'received' / 'cancelled' 等
    sku_code                  TEXT,                    -- variation.code (LOWER(TRIM())、mirror_products 解決用)
    parent_item_code          TEXT,                    -- item.code (親軸集計用)
    selling_price             REAL,                    -- 売価 (税込・送料込み)
    fee                       REAL,                    -- モール手数料 (税込)
    -- タイムスタンプ (JST、時間帯ヒートマップで使用)
    bought_at_jst             TEXT NOT NULL CHECK(bought_at_jst GLOB '????-??-??T??:??:*'),
    bought_date_jst           TEXT NOT NULL CHECK(bought_date_jst GLOB '????-??-??'),
    received_date_jst         TEXT,                    -- recognized_on (whitelist=received で集計の基準日)
    -- 90日境界凍結
    is_frozen_after_horizon   INTEGER NOT NULL DEFAULT 0 CHECK (is_frozen_after_horizon IN (0, 1)),
    -- メタ
    source_run_id             TEXT,
    synced_at                 TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlo_bought_at ON mirror_linegift_orders(bought_at_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlo_received  ON mirror_linegift_orders(received_date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlo_status    ON mirror_linegift_orders(status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlo_sku       ON mirror_linegift_orders(sku_code)');

  // mirror_qoo10_finance_sku_daily — Qoo10 Phase 1 A-3 (Render 側 daily fact mirror)
  // 設計書 v0.11: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §9
  // miniPC の f_qoo10_finance_sku_daily_v1 の payload を受信
  // PK: (date_jst, sku_code) — sku_code = master_match 後の解決値 (combined/option_only/seller_only 各 tier) or '__UNRESOLVED__:%'
  // ★ resolution_method は 2 値、match_tier (combined/option_only/seller_only/unresolved) で内訳監視
  // ★ shipping は m_products.送料 採用 (Amazon FBM 同型、shipping_quality='estimated_rates' 常態)
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_qoo10_finance_sku_daily (
    date_jst                          TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    sku_code                          TEXT NOT NULL CHECK(trim(sku_code) <> ''),
    ne_code                           TEXT,
    qoo10_item_id                     INTEGER,
    parent_item_code                  TEXT NOT NULL DEFAULT '',
    variant_key                       TEXT NOT NULL DEFAULT '',
    resolution_method                 TEXT NOT NULL CHECK (
      resolution_method IN ('master_match', 'unresolved')
    ),
    match_tier                        TEXT NOT NULL DEFAULT 'unresolved' CHECK (
      match_tier IN ('combined', 'option_only', 'seller_only', 'unresolved')
    ),
    unresolved_sku_flag               INTEGER NOT NULL DEFAULT 0,
    product_name                      TEXT NOT NULL DEFAULT '',
    units_ordered                     INTEGER NOT NULL DEFAULT 0,
    units_cancelled                   INTEGER NOT NULL DEFAULT 0,
    units_net_sold                    INTEGER NOT NULL DEFAULT 0,
    -- 売上 3 列
    gmv_list_price_jpy_incl           REAL NOT NULL DEFAULT 0,
    customer_paid_jpy_incl            REAL NOT NULL DEFAULT 0,
    net_settlement_api_jpy_incl       REAL NOT NULL DEFAULT 0,
    -- 手数料
    platform_fee_jpy_incl             REAL NOT NULL DEFAULT 0,
    mall_fee_calc_method              TEXT NOT NULL DEFAULT 'actual_api' CHECK (
      mall_fee_calc_method IN ('actual_api', 'actual_statement', 'estimated_rate', 'unknown')
    ),
    settle_price_formula_scope        TEXT NOT NULL DEFAULT 'domestic_non_cod' CHECK (
      settle_price_formula_scope IN ('domestic_non_cod', 'oversea', 'cod', 'mixed')
    ),
    -- 海外/COD
    extra_fee_oversea_jpy_incl        REAL NOT NULL DEFAULT 0,
    cod_fee_jpy_incl                  REAL NOT NULL DEFAULT 0,
    -- promo 集計
    megawari_order_count              INTEGER NOT NULL DEFAULT 0,
    megawari_discount_amount_jpy_incl REAL NOT NULL DEFAULT 0,
    megapo_order_count                INTEGER NOT NULL DEFAULT 0,
    megapo_discount_amount_jpy_incl   REAL NOT NULL DEFAULT 0,
    other_promo_order_count           INTEGER NOT NULL DEFAULT 0,
    other_promo_discount_jpy_incl     REAL NOT NULL DEFAULT 0,
    total_platform_promo_jpy_incl     REAL NOT NULL DEFAULT 0,
    qoo10_cart_discount_jpy_incl      REAL NOT NULL DEFAULT 0,
    seller_discount_api_jpy_incl      REAL NOT NULL DEFAULT 0,
    shop_promo_burden_jpy_incl        REAL NOT NULL DEFAULT 0,
    shop_promo_burden_status          TEXT NOT NULL DEFAULT 'pending_settlement_csv' CHECK (
      shop_promo_burden_status IN ('pending_settlement_csv', 'actual_statement', 'not_applicable')
    ),
    -- domestic_non_cod 別集計 (DQ check #8 用)
    domestic_non_cod_line_count           INTEGER NOT NULL DEFAULT 0,
    domestic_non_cod_formula_match_count  INTEGER NOT NULL DEFAULT 0,
    -- 送料
    shipping_cost_jpy_incl            REAL NOT NULL DEFAULT 0,
    shipping_quality                  TEXT NOT NULL CHECK (
      shipping_quality IN ('no_shipping_in_api', 'actual_api', 'estimated_rates', 'estimated_fallback', 'missing')
    ),
    -- 原価
    unit_cost_snapshot_incl           REAL,
    cost_snapshot_date_jst            TEXT,
    latest_unit_cost_reference_incl   REAL,
    cogs_amount_jpy_incl              REAL NOT NULL DEFAULT 0,
    -- 利益
    variable_margin_jpy_incl          REAL NOT NULL DEFAULT 0,
    variable_margin_full_jpy_incl     REAL,
    margin_confidence                 TEXT NOT NULL DEFAULT 'partial_pending_settlement_csv' CHECK (
      margin_confidence IN ('partial_pending_settlement_csv', 'partial_minus_returns', 'full', 'partial_legacy_fields_missing')
    ),
    margin_full_finalized_at          TEXT,
    -- audit
    delivered_lag_days                INTEGER,
    shipping_lag_days                 INTEGER,
    oversea_count                     INTEGER NOT NULL DEFAULT 0,
    payment_methods_json              TEXT,
    -- 90日境界 frozen horizon
    first_seen_in_api_at              TEXT,
    last_seen_in_api_at               TEXT,
    is_frozen_after_horizon           INTEGER NOT NULL DEFAULT 0,
    -- 品質
    cost_status                       TEXT NOT NULL CHECK (
      cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
    ),
    is_cost_complete                  INTEGER NOT NULL DEFAULT 0,
    data_quality_score                INTEGER NOT NULL DEFAULT 0
                                      CHECK (data_quality_score BETWEEN 0 AND 100),
    -- メタ
    order_count                       INTEGER NOT NULL DEFAULT 0,
    line_count                        INTEGER NOT NULL DEFAULT 0,
    source_layer_summary              TEXT NOT NULL DEFAULT '',
    source_row_count                  INTEGER NOT NULL DEFAULT 0,
    built_at                          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_run_id                     TEXT NOT NULL,
    source_row_hash                   TEXT NOT NULL,
    synced_at                         TEXT NOT NULL,
    PRIMARY KEY (date_jst, sku_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mqfsd_date   ON mirror_qoo10_finance_sku_daily(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mqfsd_ne     ON mirror_qoo10_finance_sku_daily(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mqfsd_month  ON mirror_qoo10_finance_sku_daily(substr(date_jst, 1, 7))');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mqfsd_run    ON mirror_qoo10_finance_sku_daily(source_run_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mqfsd_tier   ON mirror_qoo10_finance_sku_daily(match_tier)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mqfsd_frozen ON mirror_qoo10_finance_sku_daily(is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1');

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
    pf_fee            REAL DEFAULT 0,
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
    pf_fee            REAL DEFAULT 0,
    confirmed_at      TEXT NOT NULL
  )`);
  // 既存DBへの migration: 楽天/Yahoo summary に pf_fee 列を追加（mgmt-accounting が読む）
  addColumnIfMissing('mart_rakuten_monthly_summary', 'pf_fee', 'REAL DEFAULT 0');
  addColumnIfMissing('mart_yahoo_monthly_summary', 'pf_fee', 'REAL DEFAULT 0');

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

  // migration: 各モール集計の year_month を正規化（'2026-3-' 等 → '2026-03'）。
  // CSV日付の月ゼロ埋め漏れで slice(0,7) が生成した不正値を修復し、mgmt-accounting や
  // 各アプリ履歴が '2026-03' で一致して引けるようにする。collision 時は安全側で skip（無損失）。
  const normalizeYearMonth = (table) => {
    let bad;
    try {
      bad = db.prepare(`SELECT year_month FROM ${table} WHERE year_month NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'`).all();
    } catch { return; }
    for (const r of bad) {
      const m = String(r.year_month).match(/(\d{4})\D*(\d{1,2})/);
      if (!m) continue;
      const fixed = `${m[1]}-${String(parseInt(m[2], 10)).padStart(2, '0')}`;
      if (fixed === r.year_month) continue;
      if (db.prepare(`SELECT 1 FROM ${table} WHERE year_month = ? LIMIT 1`).get(fixed)) {
        console.warn(`[normalize-ym ${table}] '${r.year_month}' → '${fixed}' は既存と衝突のためスキップ`);
        continue;
      }
      db.prepare(`UPDATE ${table} SET year_month = ? WHERE year_month = ?`).run(fixed, r.year_month);
      console.log(`[normalize-ym ${table}] '${r.year_month}' → '${fixed}'`);
    }
  };
  for (const t of [
    'mart_amazon_monthly_summary', 'mart_rakuten_monthly_summary', 'mart_yahoo_monthly_summary',
    'mart_aupay_monthly_summary', 'mart_qoo10_monthly_summary', 'mart_linegift_monthly_summary',
    'mart_mercari_monthly_summary', 'mart_amazon_usa_monthly_summary',
  ]) normalizeYearMonth(t);

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

  // Phase 1d-2: 期 (FY) 累計 + 平均残高 mart
  //   miniPC mart_mf_fy_summary と同形 (1 run につき 第N期 + 第N-1期 の 2 行)
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_fy_summary (
    run_id                   INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    fy_number                INTEGER NOT NULL,
    fy_start_ym              TEXT NOT NULL,
    fy_end_ym                TEXT NOT NULL,
    cumulative_through_ym    TEXT NOT NULL,
    months_in_cumulative     INTEGER NOT NULL,
    is_fy_completed          INTEGER NOT NULL DEFAULT 0,
    sales_cum                INTEGER NOT NULL DEFAULT 0,
    cogs_cum                 INTEGER NOT NULL DEFAULT 0,
    gross_profit_cum         INTEGER NOT NULL DEFAULT 0,
    sgae_cum                 INTEGER NOT NULL DEFAULT 0,
    operating_income_cum     INTEGER NOT NULL DEFAULT 0,
    non_op_revenue_cum       INTEGER NOT NULL DEFAULT 0,
    non_op_expense_cum       INTEGER NOT NULL DEFAULT 0,
    ordinary_income_cum      INTEGER NOT NULL DEFAULT 0,
    personnel_cost_cum       INTEGER NOT NULL DEFAULT 0,
    ar_average               INTEGER NOT NULL DEFAULT 0,
    inventory_average        INTEGER NOT NULL DEFAULT 0,
    ap_average               INTEGER NOT NULL DEFAULT 0,
    total_asset_average      INTEGER NOT NULL DEFAULT 0,
    ar_opening               INTEGER NOT NULL DEFAULT 0,
    ar_closing               INTEGER NOT NULL DEFAULT 0,
    inventory_opening        INTEGER NOT NULL DEFAULT 0,
    inventory_closing        INTEGER NOT NULL DEFAULT 0,
    ap_opening               INTEGER NOT NULL DEFAULT 0,
    ap_closing               INTEGER NOT NULL DEFAULT 0,
    total_asset_opening      INTEGER NOT NULL DEFAULT 0,
    total_asset_closing      INTEGER NOT NULL DEFAULT 0,
    cash_closing             INTEGER NOT NULL DEFAULT 0,
    short_loan_closing       INTEGER NOT NULL DEFAULT 0,
    long_loan_closing        INTEGER NOT NULL DEFAULT 0,
    current_liab_closing     INTEGER NOT NULL DEFAULT 0,
    total_equity_closing     INTEGER NOT NULL DEFAULT 0,
    source_row_hash          TEXT NOT NULL,
    synced_at                TEXT NOT NULL,
    PRIMARY KEY (run_id, fy_number)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_fy_summary_fy ON mirror_mf_fy_summary(fy_number DESC)');

  // Phase 1d-3b: BS section別月末 mart
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_bs_monthly (
    run_id                  INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    month_ym                TEXT NOT NULL,
    cash_total              INTEGER NOT NULL DEFAULT 0,
    ar_total                INTEGER NOT NULL DEFAULT 0,
    inventory_total         INTEGER NOT NULL DEFAULT 0,
    other_current_asset     INTEGER NOT NULL DEFAULT 0,
    current_asset_total     INTEGER NOT NULL DEFAULT 0,
    tangible_fixed_asset    INTEGER NOT NULL DEFAULT 0,
    investment_other        INTEGER NOT NULL DEFAULT 0,
    fixed_asset_total       INTEGER NOT NULL DEFAULT 0,
    total_asset             INTEGER NOT NULL DEFAULT 0,
    ap_total                INTEGER NOT NULL DEFAULT 0,
    short_loan_total        INTEGER NOT NULL DEFAULT 0,
    other_current_liab      INTEGER NOT NULL DEFAULT 0,
    current_liab_total      INTEGER NOT NULL DEFAULT 0,
    long_loan_total         INTEGER NOT NULL DEFAULT 0,
    other_fixed_liab        INTEGER NOT NULL DEFAULT 0,
    fixed_liab_total        INTEGER NOT NULL DEFAULT 0,
    total_liab              INTEGER NOT NULL DEFAULT 0,
    capital                 INTEGER NOT NULL DEFAULT 0,
    retained                INTEGER NOT NULL DEFAULT 0,
    total_equity            INTEGER NOT NULL DEFAULT 0,
    bs_other_total          INTEGER NOT NULL DEFAULT 0,
    retained_balance        INTEGER NOT NULL DEFAULT 0,
    current_period_profit   INTEGER NOT NULL DEFAULT 0,
    display_total_equity    INTEGER NOT NULL DEFAULT 0,
    source_row_hash         TEXT NOT NULL,
    synced_at               TEXT NOT NULL,
    PRIMARY KEY (run_id, month_ym)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_bs_monthly_ym ON mirror_mf_bs_monthly(month_ym)');

  // Phase 1d-3b: BS 細目 mart (account × sub_account 単位、UI breakdown 用)
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_mf_bs_subaccount_monthly (
    run_id                   INTEGER NOT NULL REFERENCES mirror_mf_publish_runs(run_id) ON DELETE CASCADE,
    month_ym                 TEXT NOT NULL,
    account_name             TEXT NOT NULL,
    sub_account_name         TEXT NOT NULL DEFAULT '',
    role_key                 TEXT,
    section                  TEXT NOT NULL,
    closing_balance_excl_tax INTEGER NOT NULL DEFAULT 0,
    is_hub_null_sub          INTEGER NOT NULL DEFAULT 0,
    source_row_hash          TEXT NOT NULL,
    synced_at                TEXT NOT NULL,
    PRIMARY KEY (run_id, month_ym, account_name, sub_account_name)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_bs_subaccount_ym ON mirror_mf_bs_subaccount_monthly(month_ym)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirror_mf_bs_subaccount_sec ON mirror_mf_bs_subaccount_monthly(month_ym, section)');

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
  // ---- mirror_f_sales_by_listing — biz-ops-overview 業務目線の全モール売上 mirror (2026-05-19 PR #156)
  // hotfix v3: 列名を英語化 (date_jst 等)。router.js の date_range strategy が `WHERE date_jst IN (...)` を
  //   ハードコード使用しているため、日本語列だと 500 エラーになる。他 mirror_*_finance_sku_daily も英語列で揃ってる。
  // PK: (date_jst, mall, item_code, channel) — miniPC f_sales_by_listing (日本語列) からは sync runner 側で mapping
  // ⚠️ migration: PR #156 で日本語列で作成した既存 mirror_f_sales_by_listing が残ってる場合、
  //   CREATE TABLE IF NOT EXISTS は no-op なので英語列にならない → 旧列検知して DROP + 再作成
  try {
    const existingCols = db.prepare("PRAGMA table_info(mirror_f_sales_by_listing)").all().map(c => c.name);
    if (existingCols.length > 0 && !existingCols.includes('date_jst')) {
      console.warn('[mirror] mirror_f_sales_by_listing に date_jst 列なし (PR #156 旧 DDL の名残) → DROP して英語列で再作成');
      db.exec('DROP TABLE IF EXISTS mirror_f_sales_by_listing');
    }
  } catch (e) {
    // テーブル未存在等は無視 (CREATE で作る)
  }
  // 販売速度 モール別ミラー（仕入れ先売れ筋共有の「速報モール別」用）。
  //   miniPC f_sales_velocity_by_product_mall を /api/sync(velocity_mall) で受信。
  //   商品コード=NE商品コード、mall=platform / 'amazon_fba'、qty_7d/30d は注文ベース数量。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_f_sales_velocity_by_product_mall (
    商品コード   TEXT NOT NULL,
    mall         TEXT NOT NULL,
    qty_7d       INTEGER NOT NULL DEFAULT 0,
    qty_30d      INTEGER NOT NULL DEFAULT 0,
    as_of_date   TEXT NOT NULL,
    synced_at    TEXT NOT NULL,
    PRIMARY KEY (商品コード, mall)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mfsvm_code ON mirror_f_sales_velocity_by_product_mall(商品コード)');

  db.exec(`CREATE TABLE IF NOT EXISTS mirror_f_sales_by_listing (
    date_jst          TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    month_ym          TEXT NOT NULL,
    mall              TEXT NOT NULL,
    item_code         TEXT NOT NULL,
    channel           TEXT NOT NULL DEFAULT '',
    item_name         TEXT,
    units             INTEGER NOT NULL DEFAULT 0,
    sales_jpy_incl    REAL,
    order_count       INTEGER,
    data_source       TEXT,
    source_updated_at TEXT NOT NULL,
    source_run_id     TEXT NOT NULL,
    source_row_hash   TEXT NOT NULL,
    synced_at         TEXT NOT NULL,
    PRIMARY KEY (date_jst, mall, item_code, channel)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mfsbl_date   ON mirror_f_sales_by_listing(date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mfsbl_mall   ON mirror_f_sales_by_listing(mall, date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mfsbl_month  ON mirror_f_sales_by_listing(month_ym)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mfsbl_run    ON mirror_f_sales_by_listing(source_run_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mfsbl_item   ON mirror_f_sales_by_listing(item_code, date_jst)');

  // ---- 商品管理リスト スナップショット ミラー (⑤、商品管理リスト④の published run を受信) ----
  //   ミニPC build-product-management-snapshot.js → sync-to-render.js → ここ。
  //   受信時に checksum 検証 (recompute == 送信元 payload_checksum) してから atomic swap。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_pml_snapshot_rows (
    run_id                TEXT NOT NULL,
    商品コード            TEXT NOT NULL,
    商品名                TEXT,
    仕入先                TEXT,
    取扱区分              TEXT,
    商品区分              TEXT,
    売上分類              INTEGER,
    最終仕入日            TEXT,
    在庫保管日数          INTEGER,
    総在庫数              INTEGER,
    FBA在庫数             INTEGER,
    フリー在庫            INTEGER,
    注残数                INTEGER,
    引当数                INTEGER,
    総在庫数_引当なし     INTEGER,
    販売数7日_FBA         INTEGER,
    販売数7日_FBA以外     INTEGER,
    販売数7日_合計        INTEGER,
    販売数30日_FBA        INTEGER,
    販売数30日_FBA以外    INTEGER,
    販売数30日_合計       INTEGER,
    発注ロット単位        INTEGER,
    推奨保有月数          REAL,
    売価                  REAL,
    原価                  REAL,
    想定見込み利益        REAL,
    概算利益率            REAL,
    代表商品コード        TEXT,
    ロケーションコード    TEXT,
    商品分類タグ          TEXT,
    登録日                TEXT,
    PRIMARY KEY (run_id, 商品コード)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mpsr_run ON mirror_pml_snapshot_rows(run_id)');
  // 公開ポインタ + メタ (単一行 id=1)。Render 画面/CSV/JSON はこの run_id のみ参照。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_pml_published (
    id                        INTEGER PRIMARY KEY CHECK (id = 1),
    run_id                    TEXT NOT NULL,
    status                    TEXT NOT NULL,
    as_of_date                TEXT,
    generated_at              TEXT,
    payload_checksum          TEXT,
    row_count                 INTEGER,
    src_ne_products_synced_at TEXT,
    src_velocity_as_of        TEXT,
    src_fba_business_date     TEXT,
    src_reorder_updated_at    TEXT,
    ne_fba_overlap            INTEGER,
    published_at              TEXT,
    synced_at                 TEXT NOT NULL
  )`);
  // 既存テーブルへの列追加 (売上分類=商品管理リストの「商品区分 1自社/2AMC/3仕入」)
  addColumnIfMissing('mirror_pml_snapshot_rows', '売上分類', 'INTEGER');
  // FBA鮮度メタ (Part2 オンデマンドFBA更新: daily=朝の日次 / live=オンデマンドRESTOCK)
  addColumnIfMissing('mirror_pml_published', 'fba_source_kind', 'TEXT');
  addColumnIfMissing('mirror_pml_published', 'fba_source_run_id', 'TEXT');
  addColumnIfMissing('mirror_pml_published', 'fba_fetched_at', 'TEXT');
  addColumnIfMissing('mirror_pml_published', 'fba_latest_row_count', 'INTEGER');

  // ---- biz-ops-overview: 全モール売上日次統合 view (2026-05-19 PR #156)
  db.exec('DROP VIEW IF EXISTS v_mall_sales_daily_unified');
  db.exec(`CREATE VIEW v_mall_sales_daily_unified AS
    SELECT
      mall,
      date_jst,
      sales_jpy_incl AS sales_gross_jpy_incl,
      units AS units_net_sold,
      synced_at AS data_synced_at,
      'mirror_f_sales_by_listing' AS source_fact
    FROM mirror_f_sales_by_listing`);

  db.exec('DROP VIEW IF EXISTS v_mirror_mf_anomaly_signals_latest');
  db.exec(`CREATE VIEW v_mirror_mf_anomaly_signals_latest AS
    SELECT m.*, s.status AS state_status, s.suppress_until, s.acked_by, s.acked_at
    FROM mirror_mf_anomaly_signals m
    LEFT JOIN mart_mf_anomaly_signal_state s ON s.signal_key = m.signal_key
    WHERE m.run_id = (SELECT MAX(run_id) FROM mirror_mf_publish_runs WHERE status = 'success' AND scope IN ('all','anomaly_signals'))`);
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_fy_summary_latest');
  // Phase 1d-2 Codex review: empty run で latest が空に切り替わるのを防ぐため、
  //   mirror_mf_fy_summary に実在する最新 success run を選ぶ (空 run は飛ばす)
  db.exec(`CREATE VIEW v_mirror_mf_fy_summary_latest AS
    SELECT m.* FROM mirror_mf_fy_summary m
    WHERE m.run_id = (
      SELECT MAX(fs.run_id)
      FROM mirror_mf_fy_summary fs
      JOIN mirror_mf_publish_runs r ON r.run_id = fs.run_id
      WHERE r.status = 'success' AND r.scope IN ('all','fy_summary')
    )`);
  // Phase 1d-3b: BS section / 細目 の latest VIEW (同じく実在 run 選択)
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_bs_monthly_latest');
  db.exec(`CREATE VIEW v_mirror_mf_bs_monthly_latest AS
    SELECT m.* FROM mirror_mf_bs_monthly m
    WHERE m.run_id = (
      SELECT MAX(b.run_id) FROM mirror_mf_bs_monthly b
      JOIN mirror_mf_publish_runs r ON r.run_id = b.run_id
      WHERE r.status = 'success' AND r.scope IN ('all','base','bs_monthly')
    )`);
  db.exec('DROP VIEW IF EXISTS v_mirror_mf_bs_subaccount_monthly_latest');
  db.exec(`CREATE VIEW v_mirror_mf_bs_subaccount_monthly_latest AS
    SELECT m.* FROM mirror_mf_bs_subaccount_monthly m
    WHERE m.run_id = (
      SELECT MAX(b.run_id) FROM mirror_mf_bs_subaccount_monthly b
      JOIN mirror_mf_publish_runs r ON r.run_id = b.run_id
      WHERE r.status = 'success' AND r.scope IN ('all','base','bs_subaccount')
    )`);
  // ▲▲▲ MFクラウド会計ダッシュボード Phase 1a/1d-2/1d-3b 終了 ▲▲▲

  // ▼▼▼ 誤出荷管理システム f_mis_shipments / f_mis_shipment_status_history ▼▼▼
  // 設計書: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_設計書_v5.md (中身 v7.3)
  // Codex 16 ラウンドレビュー完全 FIX、PR: feature/mis-shipment
  createMisShipmentTables();
  createGiftsetTables();
  createSupplierShareTables();
}

// ▼▼▼ 仕入れ先向け売れ筋共有（apps/supplier-sales）正本テーブル ▼▼▼
// 方針: Render 完結書込（mirror_* と prefix 分離、ミニPC 不使用）。
//   売上データ自体は mirror_*_finance_sku_daily を読むだけで、このアプリは
//   「仕入先コード→表示名」と「公開共有トークン」のみを正本として持つ。
// セキュリティ: 公開トークンURLは認証なしで仕入先別売上を出すため "弱い認可" として扱う。
//   生トークンは DB に保存せず SHA-256 ハッシュのみ保存（漏洩時の被害最小化）。
function createSupplierShareTables() {
  // 仕入先コード→表示名（NE の仕入先コードは数値/記号のみで人間が読めないため）。
  db.exec(`
    CREATE TABLE IF NOT EXISTS supplier_share_master (
      仕入先コード   TEXT PRIMARY KEY,
      表示名         TEXT NOT NULL,
      memo           TEXT,
      created_at     TEXT NOT NULL,
      updated_at     TEXT NOT NULL,
      CHECK (trim(仕入先コード) <> ''),
      CHECK (trim(表示名) <> '')
    )
  `);
  // 公開共有トークン。token_hash = sha256(raw token)（解決用）。
  // token_plain = 生トークン（社内管理画面で過去発行URLを再表示するために保持）。
  //   この共有URLは売上・販売数のみ（原価非開示）で、DB 自体に同じデータがあるため、
  //   生トークン保持の追加リスクは限定的（DB アクセス権者＝既にデータ閲覧可）。
  //   token_plain は requireAppAccess 配下の社内 API でのみ返し、公開側には出さない。
  db.exec(`
    CREATE TABLE IF NOT EXISTS supplier_share_tokens (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash       TEXT NOT NULL UNIQUE,
      token_plain      TEXT,
      仕入先コード     TEXT NOT NULL,
      label            TEXT,
      active           INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      expires_at       TEXT,
      revoked_at       TEXT,
      created_by       TEXT,
      created_at       TEXT NOT NULL,
      last_accessed_at TEXT,
      access_count     INTEGER NOT NULL DEFAULT 0,
      CHECK (trim(仕入先コード) <> ''),
      CHECK (trim(token_hash) <> '')
    )
  `);
  // 既存 DB(PR #368 で作成済) への migration: 生トークン列を追加（既存行は NULL=再表示不可）
  addColumnIfMissing('supplier_share_tokens', 'token_plain', 'TEXT');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sst_supplier ON supplier_share_tokens(仕入先コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sst_hash ON supplier_share_tokens(token_hash)');
}

// ▼▼▼ ギフトセット組み依頼（apps/giftset-assembly）正本テーブル ▼▼▼
// f_mis_shipments と同じ方針: Render 完結書込（mirror_* と prefix 分離、ミニPC 不使用）。
// 構成品コードは mirror_products.商品コード に存在することを API レベルで検証する。
// ロジザード「出荷予定登録(卸)Excel貼り付け FS01_05」の商品ID = この構成品コード。
function createGiftsetTables() {
  // ギフトセット定義ヘッダー。giftset_code は自動採番(gs_...)。写真/動画は Drive の URL を持つ。
  // production_lot_size: 1ロットあたりの完成個数 (デフォルト 1)。
  //   例) agneyemask5 (5個入り箱) をばらして使う relaxgiftset は 1ロット=5個でしか
  //   作れない (= 半端な箱開封不可) ため production_lot_size=5、構成品の数量は
  //   「1ロット分」の数量で登録する。
  // マニュアル系カラム (Codex review 推奨、Notion 継続 + URL 連携方針):
  //   manual_url / manual_source: 製造マニュアルの保管場所 URL とその種別 (notion/drive/internal)。
  //     manual_source を持つ理由は、将来 Notion を卒業して Drive や内製 CMS に移行した
  //     場合の切替容易性 (URL だけ持ってると種別判別不能で、UI/作業カード側の挙動を
  //     正しく分岐できないため)。
  //   estimated_duration_min: 1ロット製造の目安時間 (分)。負荷見積/進捗遅延検知に使う。
  //   difficulty: 1-5 の難易度。委託先での人員配置判断材料。
  db.exec(`
    CREATE TABLE IF NOT EXISTS f_giftset (
      giftset_code             TEXT PRIMARY KEY,
      giftset_name             TEXT NOT NULL,
      photo_url                TEXT,
      video_url                TEXT,
      unit_price               REAL CHECK (unit_price IS NULL OR unit_price >= 0),
      memo                     TEXT,
      production_lot_size      INTEGER NOT NULL DEFAULT 1
        CHECK (production_lot_size BETWEEN 1 AND 10000),
      manual_url               TEXT,
      manual_source            TEXT
        CHECK (manual_source IS NULL OR manual_source IN ('notion', 'drive', 'internal')),
      estimated_duration_min   INTEGER
        CHECK (estimated_duration_min IS NULL OR (estimated_duration_min >= 1 AND estimated_duration_min <= 100000)),
      difficulty               INTEGER
        CHECK (difficulty IS NULL OR difficulty BETWEEN 1 AND 5),
      is_active                INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
      created_at               TEXT NOT NULL,
      updated_at               TEXT NOT NULL,
      created_by               TEXT,
      updated_by               TEXT,
      CHECK (trim(giftset_code) <> ''),
      CHECK (trim(giftset_name) <> ''),
      -- manual_url と manual_source は 'どちらも空' or 'どちらも値あり' のみ許可 (片方だけ
      -- 入る状態は不整合)。URL を入れるなら必ず source を選ばせる UX を強制。
      CHECK (
        (manual_url IS NULL AND manual_source IS NULL)
        OR (manual_url IS NOT NULL AND length(trim(manual_url)) > 0 AND manual_source IS NOT NULL)
      )
    )
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_f_giftset_active ON f_giftset(is_active)');
  // 既存 f_giftset への production_lot_size 追加 (本番 DB 用 migration、idempotent)。
  // ALTER ADD COLUMN は CHECK 句を新カラムに付けられないため、ここではデフォルト 1 で追加し、
  // アプリ層 (giftset-assembly/db.js upsertGiftset) で範囲検証を行う。
  addColumnIfMissing('f_giftset', 'production_lot_size', 'INTEGER NOT NULL DEFAULT 1');
  // マニュアル系カラム migration (本番 DB 用、idempotent)。NULL 許容 (=既存セットは未設定)。
  // CHECK 句は ALTER ADD COLUMN では付けられないので、アプリ層で範囲検証する。
  addColumnIfMissing('f_giftset', 'manual_url', 'TEXT');
  addColumnIfMissing('f_giftset', 'manual_source', 'TEXT');
  addColumnIfMissing('f_giftset', 'estimated_duration_min', 'INTEGER');
  addColumnIfMissing('f_giftset', 'difficulty', 'INTEGER');
  // 起動時整合性チェック (Codex review R1 medium #2)。
  //   ALTER ADD COLUMN は新カラムに CHECK を付けられないので、もし他経路 (手 SQL 等) で
  //   不正値 (NULL / 0 / 負 / 過大) が入っていれば検知し、安全側 (=1) に正規化して警告ログ。
  //   黙って動作させると出荷予定数が誤計算される (lots 計算が破綻) ため、必ず警告を出す。
  const anomalies = db.prepare(
    `SELECT giftset_code, production_lot_size FROM f_giftset
       WHERE production_lot_size IS NULL
          OR production_lot_size < 1
          OR production_lot_size > 10000`
  ).all();
  if (anomalies.length > 0) {
    console.warn(
      `[mirror-db] f_giftset.production_lot_size に不正値 ${anomalies.length} 行を検出、` +
      `安全側 1 に正規化します: ` + anomalies.map(a => `${a.giftset_code}=${a.production_lot_size}`).join(', ')
    );
    db.prepare(
      `UPDATE f_giftset SET production_lot_size = 1
        WHERE production_lot_size IS NULL
           OR production_lot_size < 1
           OR production_lot_size > 10000`
    ).run();
  }

  // マニュアル系カラム整合性チェック (起動時、ALTER ADD COLUMN 経路では CHECK が効かないため)。
  //   検知対象 (いずれも NULL に揃える):
  //     - manual_url / manual_source の片方だけ埋まっている (ペア整合違反)
  //     - manual_url が空文字 (trim 後 length 0)
  //     - manual_source が enum (notion|drive|internal) 範囲外
  //     - manual_url のスキームが http/https でない (Codex R1 medium #1、javascript:/file:/ftp: 等を排除)
  //     - manual_url の長さが 2000 字超 (DoS / Notion API リジェクト対策)
  //   UI/API 経路では validation 済みだが、手 SQL や旧アプリで入った異常値の早期発見。
  const manualAnomalies = db.prepare(
    `SELECT giftset_code, manual_url, manual_source FROM f_giftset
       WHERE (manual_url IS NULL AND manual_source IS NOT NULL)
          OR (manual_url IS NOT NULL AND length(trim(manual_url)) > 0 AND manual_source IS NULL)
          OR (manual_url IS NOT NULL AND length(trim(manual_url)) = 0)
          OR (manual_source IS NOT NULL AND manual_source NOT IN ('notion','drive','internal'))
          OR (manual_url IS NOT NULL
              AND length(trim(manual_url)) > 0
              AND lower(trim(manual_url)) NOT LIKE 'http://%'
              AND lower(trim(manual_url)) NOT LIKE 'https://%')
          OR (manual_url IS NOT NULL AND length(manual_url) > 2000)`
  ).all();
  if (manualAnomalies.length > 0) {
    console.warn(
      `[mirror-db] f_giftset.manual_url / manual_source の不整合 ${manualAnomalies.length} 行を検出、` +
      `NULL に揃えます: ` + manualAnomalies.map(a => `${a.giftset_code}(url=${a.manual_url ? 'set' : 'null'}, src=${a.manual_source})`).join(', ')
    );
    db.prepare(
      `UPDATE f_giftset SET manual_url = NULL, manual_source = NULL
        WHERE (manual_url IS NULL AND manual_source IS NOT NULL)
           OR (manual_url IS NOT NULL AND length(trim(manual_url)) > 0 AND manual_source IS NULL)
           OR (manual_url IS NOT NULL AND length(trim(manual_url)) = 0)
           OR (manual_source IS NOT NULL AND manual_source NOT IN ('notion','drive','internal'))
           OR (manual_url IS NOT NULL
               AND length(trim(manual_url)) > 0
               AND lower(trim(manual_url)) NOT LIKE 'http://%'
               AND lower(trim(manual_url)) NOT LIKE 'https://%')
           OR (manual_url IS NOT NULL AND length(manual_url) > 2000)`
    ).run();
  }
  // 数値カラムの異常値検知 (NULL は許可、それ以外は範囲外なら NULL に戻す + 警告)。
  const numAnomalies = db.prepare(
    `SELECT giftset_code, estimated_duration_min, difficulty FROM f_giftset
       WHERE (estimated_duration_min IS NOT NULL
              AND (estimated_duration_min < 1 OR estimated_duration_min > 100000))
          OR (difficulty IS NOT NULL AND (difficulty < 1 OR difficulty > 5))`
  ).all();
  if (numAnomalies.length > 0) {
    console.warn(
      `[mirror-db] f_giftset.estimated_duration_min / difficulty の異常値 ${numAnomalies.length} 行を検出、` +
      `NULL に揃えます: ` + numAnomalies.map(a =>
        `${a.giftset_code}(dur=${a.estimated_duration_min}, diff=${a.difficulty})`).join(', ')
    );
    db.prepare(
      `UPDATE f_giftset SET estimated_duration_min = NULL
        WHERE estimated_duration_min IS NOT NULL
          AND (estimated_duration_min < 1 OR estimated_duration_min > 100000)`
    ).run();
    db.prepare(
      `UPDATE f_giftset SET difficulty = NULL
        WHERE difficulty IS NOT NULL AND (difficulty < 1 OR difficulty > 5)`
    ).run();
  }

  // ギフトセット構成（1ギフト = N構成品）。
  // 数量の単位: 1ロット分 (production_lot_size 個セット分) の数量。
  //   production_lot_size = 1 (デフォルト) なら「1セットあたり」と同義。
  //   production_lot_size > 1 (例: relaxgiftset_lot5) なら「N個ロット 1 回分」の数量を登録する。
  db.exec(`
    CREATE TABLE IF NOT EXISTS f_giftset_components (
      giftset_code  TEXT NOT NULL
        REFERENCES f_giftset(giftset_code) ON DELETE CASCADE,
      商品コード     TEXT NOT NULL,
      数量          INTEGER NOT NULL DEFAULT 1 CHECK (数量 BETWEEN 1 AND 100000),
      sort_order    INTEGER NOT NULL DEFAULT 0,
      商品名         TEXT,
      created_at    TEXT NOT NULL,
      updated_at    TEXT NOT NULL,
      PRIMARY KEY (giftset_code, 商品コード),
      CHECK (trim(商品コード) <> '')
    )
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_f_giftset_comp_parent ON f_giftset_components(giftset_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_f_giftset_comp_child ON f_giftset_components(商品コード)');
}

function createMisShipmentTables() {
  // 正本テーブル (mirror_* と prefix で責任分離、こちらは Render 完結書込)
  db.exec(`
    CREATE TABLE IF NOT EXISTS f_mis_shipments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_submission_id TEXT NOT NULL UNIQUE,
      payload_hash TEXT NOT NULL
        CHECK (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
      version INTEGER NOT NULL DEFAULT 0,

      occurred_on TEXT NOT NULL CHECK (occurred_on GLOB '????-??-??'),
      reported_at TEXT NOT NULL,

      mall_order_id TEXT
        CHECK (mall_order_id IS NULL OR length(mall_order_id) <= 100),
      order_id_unknown INTEGER NOT NULL DEFAULT 0
        CHECK (order_id_unknown IN (0, 1)),
      mall TEXT
        CHECK (mall IS NULL OR mall IN
          ('amazon','rakuten','yahoo','linegift','mercari','aupay','qoo10','other')),
      sku_snapshot TEXT
        CHECK (sku_snapshot IS NULL OR length(sku_snapshot) <= 100),
      product_name_snapshot TEXT,
      ordered_qty_snapshot INTEGER,
      order_date_snapshot TEXT,
      lookup_source TEXT NOT NULL
        CHECK (lookup_source IN ('mirror_auto','manual','unknown')),

      mis_type TEXT NOT NULL
        CHECK (mis_type IN
          ('wrong_item','wrong_qty','damage','missing','wrong_address','mix_up','other')),
      qty_affected INTEGER NOT NULL CHECK (qty_affected BETWEEN 1 AND 1000),
      loss_amount_jpy INTEGER NOT NULL DEFAULT 0
        CHECK (loss_amount_jpy BETWEEN 0 AND 10000000),

      process_stage TEXT NOT NULL
        CHECK (process_stage IN
          ('picking','packing','labeling','inspection','handover','unknown')),
      root_cause_stage TEXT NOT NULL DEFAULT 'unknown'
        CHECK (root_cause_stage IN
          ('receiving','supplier','master_data','picking','packing','labeling',
           'inspection','system','other','unknown')),
      root_cause_note TEXT
        CHECK (root_cause_note IS NULL OR length(root_cause_note) <= 2000),

      mix_up_group_id TEXT,

      status TEXT NOT NULL DEFAULT 'reported'
        CHECK (status IN ('reported','investigating','resolved','closed')),
      reporter_note TEXT
        CHECK (reporter_note IS NULL OR length(reporter_note) <= 2000),

      reported_by TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      updated_by TEXT NOT NULL,
      deleted_at TEXT,
      deleted_by TEXT,

      CHECK (
        (order_id_unknown = 1 AND mall_order_id IS NULL AND lookup_source = 'unknown')
        OR
        (order_id_unknown = 0 AND mall_order_id IS NOT NULL)
      ),
      CHECK (
        (lookup_source = 'unknown'
           AND mall IS NULL
           AND sku_snapshot IS NULL
           AND product_name_snapshot IS NULL
           AND ordered_qty_snapshot IS NULL
           AND order_date_snapshot IS NULL)
        OR
        (lookup_source IN ('mirror_auto','manual') AND mall IS NOT NULL)
      ),
      -- mix_up_group_id は mix_up 時のみ存在、それ以外では必ず NULL (Codex round 14 high 指摘: 両方向制約)
      CHECK (
        (mis_type = 'mix_up' AND mix_up_group_id IS NOT NULL)
        OR
        (mis_type != 'mix_up' AND mix_up_group_id IS NULL)
      )
    )
  `);

  // 状態履歴 (append-only、trigger で UPDATE/DELETE/REPLACE を全部ブロック)
  db.exec(`
    CREATE TABLE IF NOT EXISTS f_mis_shipment_status_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      mis_shipment_id INTEGER NOT NULL REFERENCES f_mis_shipments(id),
      from_status TEXT
        CHECK (from_status IS NULL OR from_status IN
          ('reported','investigating','resolved','closed')),
      to_status TEXT NOT NULL
        CHECK (to_status IN ('reported','investigating','resolved','closed')),
      changed_by TEXT NOT NULL,
      changed_at TEXT NOT NULL,
      change_note TEXT
    )
  `);

  // append-only 強制 trigger (Codex round 14/15 指摘)
  // recursive_triggers = ON で REPLACE INTO 経由の内部 DELETE もブロック
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_mis_status_history_no_update
      BEFORE UPDATE ON f_mis_shipment_status_history
      BEGIN
        SELECT RAISE(ABORT, 'f_mis_shipment_status_history is append-only (UPDATE forbidden)');
      END
  `);
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_mis_status_history_no_delete
      BEFORE DELETE ON f_mis_shipment_status_history
      BEGIN
        SELECT RAISE(ABORT, 'f_mis_shipment_status_history is append-only (DELETE forbidden)');
      END
  `);

  // インデックス (partial 多用、deleted_at NULL 条件)
  // 注: client_submission_id は UNIQUE 制約で SQLite 自動 index あり、明示 index は不要
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mis_occurred
             ON f_mis_shipments(occurred_on) WHERE deleted_at IS NULL`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mis_mall_occurred
             ON f_mis_shipments(mall, occurred_on) WHERE deleted_at IS NULL`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mis_status
             ON f_mis_shipments(status) WHERE deleted_at IS NULL`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mis_order_id
             ON f_mis_shipments(mall_order_id) WHERE deleted_at IS NULL`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mis_mix_up_group
             ON f_mis_shipments(mix_up_group_id) WHERE mix_up_group_id IS NOT NULL`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_status_hist
             ON f_mis_shipment_status_history(mis_shipment_id, changed_at)`);
  // ▲▲▲ 誤出荷管理システム ▲▲▲
}
