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

  // mirror_sku_map — Amazon SKU→NE商品コード対応（旧、互換維持）
  // ※新規ツールは mirror_sku_resolved を参照すること。本テーブルは段階廃止予定。
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sku_map (
    seller_sku        TEXT NOT NULL,
    ne_code           TEXT NOT NULL,
    asin              TEXT,
    商品名            TEXT,
    数量              INTEGER DEFAULT 1,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (seller_sku, ne_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirsku_sku ON mirror_sku_map(seller_sku)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirsku_ne ON mirror_sku_map(ne_code)');

  // mirror_sku_resolved — SKU紐付け解決済みビューのミラー（v_sku_resolved の結果）
  // 設計:
  //   - source='master': m_sku_master/m_sku_components 由来（人手キュレート、商品名あり）
  //   - source='auto'  : sku_map 由来（自動検出、master未登録SKUのみfallback、商品名なし）
  //   - 商品名はNULL許容（auto時はNULLになる）
  //   - source_updated_at は元データの更新時刻（master=updated_at, auto=sku_map.synced_at）
  //   - synced_at はこのミラーへの取り込み時刻
  // 新規ツールはこのテーブルだけ見ればよい（master優先＋fallbackロジックはミニPC側で確定済み）
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
}
