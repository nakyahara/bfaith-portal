/**
 * warehouse.db — 社内マスターデータ基盤
 *
 * better-sqlite3 使用（ファイルベース、メモリ制限なし）
 *
 * テーブル構成:
 *   NE系: raw_ne_products / raw_ne_orders / raw_ne_set_products
 *   SP-API系: raw_sp_orders_log / raw_sp_orders
 *   ロジザード: raw_lz_inventory
 *   マッピング: sku_map / product_shipping / shipping_rates / exception_genka
 *   マスタ: shops / sync_meta
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'warehouse.db');

let db = null;

// ─── DB初期化 ───

export async function initDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  db = new Database(DB_FILE);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  // 書き込みロック競合時の待機時間。常駐サーバは短く(5秒)、バッチは
  // WAREHOUSE_DB_BUSY_TIMEOUT_MS で長め(60秒)に上書きする運用。
  // 不正値は warning を出して 5000ms にフォールバック（運用事故時に気づけるように）。
  const DEFAULT_BUSY_TIMEOUT_MS = 5000;
  const rawBusyTimeout = process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS;
  let busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS;
  if (rawBusyTimeout !== undefined && rawBusyTimeout !== '') {
    const n = Number(rawBusyTimeout);
    if (Number.isInteger(n) && n >= 0) {
      busyTimeoutMs = n;
    } else {
      console.warn(`[Warehouse] WAREHOUSE_DB_BUSY_TIMEOUT_MS=${JSON.stringify(rawBusyTimeout)} は無効値、${DEFAULT_BUSY_TIMEOUT_MS}ms にフォールバック`);
    }
  }
  db.pragma(`busy_timeout = ${busyTimeoutMs}`);
  createTables();
  console.log(`[Warehouse] 初期化完了 (busy_timeout=${busyTimeoutMs}ms)`);
  return db;
}

export function getDB() {
  if (!db) throw new Error('warehouse.db が初期化されていません。initDB() を先に呼んでください');
  return db;
}

// better-sqlite3はファイルベースなのでsaveToFile不要。互換性のため空関数を残す
export function saveToFile() {}

// ─── テーブル作成 ───

// 既存テーブルへのカラム追加ヘルパー（冪等、空catchでエラー握り潰さない）
//   Codex PR1 review Medium #4 反映
function addColumnIfMissing(table, column, typeClause) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  if (!cols.includes(column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${typeClause}`);
  }
}

function createTables() {
  // 1. NE商品マスタ（UPSERT上書き）
  db.exec(`CREATE TABLE IF NOT EXISTS raw_ne_products (
    商品コード          TEXT PRIMARY KEY,
    商品名              TEXT,
    仕入先コード        TEXT,
    原価                REAL,
    売価                REAL,
    取扱区分            TEXT,
    代表商品コード      TEXT,
    ロケーションコード  TEXT,
    配送業者            TEXT,
    発注ロット単位      INTEGER,
    最終仕入日          TEXT,
    商品分類タグ        TEXT,
    作成日              TEXT,
    在庫数              INTEGER,
    引当数              INTEGER,
    最終更新日          TEXT,
    消費税率            REAL,
    発注残数            INTEGER,
    synced_at           TEXT
  )`);

  // 2. NE受注明細（追記蓄積、重複排除）
  db.exec(`CREATE TABLE IF NOT EXISTS raw_ne_orders (
    伝票番号            TEXT,
    受注番号            TEXT,
    受注状態区分        TEXT,
    受注状態            TEXT,
    受注キャンセル      TEXT,
    受注キャンセル日    TEXT,
    受注日              TEXT,
    店舗コード          TEXT,
    出荷確定日          TEXT,
    明細行番号          INTEGER,
    レコードナンバー    TEXT,
    キャンセル区分      TEXT,
    商品コード          TEXT,
    商品名              TEXT,
    商品OP              TEXT,
    受注数              INTEGER,
    引当数              INTEGER,
    小計金額            REAL,
    synced_at           TEXT,
    PRIMARY KEY (伝票番号, 明細行番号)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_orders_date ON raw_ne_orders(受注日)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_orders_product ON raw_ne_orders(商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_orders_shop ON raw_ne_orders(店舗コード)');
  // 同梱商品検索 (cross-sell) の伝票番号 join を安定化させるための明示 index。
  // PK 先頭列でも equality lookup は効くが、planner の選択が変わるのを避ける。
  db.exec('CREATE INDEX IF NOT EXISTS idx_orders_voucher ON raw_ne_orders(伝票番号)');

  // 3. セット商品マスタ
  db.exec(`CREATE TABLE IF NOT EXISTS raw_ne_set_products (
    セット商品コード    TEXT,
    セット商品名        TEXT,
    セット販売価格      REAL,
    商品コード          TEXT,
    数量                INTEGER,
    セット在庫数        INTEGER,
    代表商品コード      TEXT,
    synced_at           TEXT,
    PRIMARY KEY (セット商品コード, 商品コード)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_set_parent ON raw_ne_set_products(セット商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_set_child ON raw_ne_set_products(商品コード)');

  // 4. ロジザード在庫（全件洗い替え）
  db.exec(`CREATE TABLE IF NOT EXISTS raw_lz_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    商品ID              TEXT,
    商品名              TEXT,
    バーコード          TEXT,
    ブロック略称        TEXT,
    ロケ                TEXT,
    品質区分名          TEXT,
    有効期限            TEXT,
    入荷日              TEXT,
    在庫数              INTEGER,
    引当数              INTEGER,
    ロケ業務区分        TEXT,
    商品予備項目004     TEXT,
    最終入荷日          TEXT,
    最終出荷日          TEXT,
    ブロック引当順      TEXT,
    在庫日              TEXT,
    synced_at           TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_lz_product ON raw_lz_inventory(商品ID)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_lz_expiry ON raw_lz_inventory(有効期限)');

  // 5. SKUマッピング（Amazon SKU ↔ NE商品コード、1 SKU = 複数NE商品コード可）
  db.exec(`CREATE TABLE IF NOT EXISTS sku_map (
    seller_sku          TEXT NOT NULL,
    asin                TEXT,
    商品名              TEXT,
    ne_code             TEXT NOT NULL,
    数量                INTEGER DEFAULT 1,
    synced_at           TEXT,
    PRIMARY KEY (seller_sku, ne_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sku_map_asin ON sku_map(asin)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sku_map_ne ON sku_map(ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sku_map_sku ON sku_map(seller_sku)');

  // 6. 配送区分マスタ
  db.exec(`CREATE TABLE IF NOT EXISTS shipping_rates (
    shipping_code       TEXT PRIMARY KEY,
    大分類区分          TEXT,
    運送会社            TEXT,
    小分類区分名称      TEXT,
    梱包サイズ          TEXT,
    最大重量            TEXT,
    追跡有無            TEXT,
    送料                REAL,
    出荷作業料          REAL,
    想定梱包資材費      REAL,
    想定人件費          REAL,
    配送関係費合計      REAL,
    備考                TEXT,
    synced_at           TEXT
  )`);

  // 7. 商品別送料マスタ（商品→配送方法紐付け）
  db.exec(`CREATE TABLE IF NOT EXISTS product_shipping (
    sku                 TEXT PRIMARY KEY,
    product_name        TEXT,
    shipping_code       TEXT,
    ship_method         TEXT,
    ship_cost           REAL,
    note                TEXT,
    synced_at           TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_prodship_code ON product_shipping(shipping_code)');

  // 8. 特殊商品原価（NEに存在しないSKUの原価）
  db.exec(`CREATE TABLE IF NOT EXISTS exception_genka (
    sku                 TEXT PRIMARY KEY,
    genka               REAL,
    商品名              TEXT,
    synced_at           TEXT
  )`);

  // 8b. 商品別消費税率（手動登録、例外商品等NE税率がない場合用）
  db.exec(`CREATE TABLE IF NOT EXISTS product_tax_rate (
    sku                 TEXT PRIMARY KEY,
    tax_rate            REAL NOT NULL,
    商品名              TEXT,
    synced_at           TEXT
  )`);

  // 9. SP-API注文ログ（append-only）
  db.exec(`CREATE TABLE IF NOT EXISTS raw_sp_orders_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    source_report_type TEXT,
    source_window_start TEXT,
    source_window_end TEXT,
    amazon_order_id TEXT NOT NULL,
    merchant_order_id TEXT,
    purchase_date TEXT,
    last_updated_date TEXT,
    order_status TEXT,
    fulfillment_channel TEXT,
    sales_channel TEXT,
    asin TEXT,
    seller_sku TEXT,
    title TEXT,
    quantity INTEGER,
    item_price REAL,
    item_tax REAL,
    shipping_price REAL,
    shipping_tax REAL,
    promotion_discount REAL,
    currency TEXT,
    item_status TEXT,
    ingested_at TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_log_batch ON raw_sp_orders_log(batch_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_log_order ON raw_sp_orders_log(amazon_order_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_log_date ON raw_sp_orders_log(purchase_date)');

  // 10. SP-API注文（current snapshot）
  db.exec(`CREATE TABLE IF NOT EXISTS raw_sp_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    amazon_order_id TEXT NOT NULL,
    merchant_order_id TEXT,
    purchase_date TEXT,
    last_updated_date TEXT,
    order_status TEXT,
    fulfillment_channel TEXT,
    sales_channel TEXT,
    asin TEXT,
    seller_sku TEXT,
    title TEXT,
    quantity INTEGER,
    item_price REAL,
    item_tax REAL,
    shipping_price REAL,
    shipping_tax REAL,
    promotion_discount REAL,
    currency TEXT,
    item_status TEXT,
    synced_at TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_orders_order_id ON raw_sp_orders(amazon_order_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_orders_date ON raw_sp_orders(purchase_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_orders_sku ON raw_sp_orders(seller_sku)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_orders_asin ON raw_sp_orders(asin)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sp_orders_channel ON raw_sp_orders(fulfillment_channel)');

  // 11. Yahoo!ショッピング受注ログ（append-only）
  db.exec(`CREATE TABLE IF NOT EXISTS raw_yahoo_orders_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id            TEXT NOT NULL,
    source_window_start TEXT,
    source_window_end   TEXT,
    order_id            TEXT NOT NULL,
    order_time          TEXT,
    last_update_time    TEXT,
    order_status        TEXT,
    pay_status          TEXT,
    ship_status         TEXT,
    total_price         REAL,
    pay_charge          REAL,
    ship_charge         REAL,
    discount            REAL,
    use_point           REAL,
    line_id             INTEGER,
    item_id             TEXT,
    title               TEXT,
    sub_code            TEXT,
    unit_price          REAL,
    original_price      REAL,
    quantity            INTEGER,
    item_tax_ratio      REAL,
    coupon_discount     REAL,
    ingested_at         TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_yh_log_batch ON raw_yahoo_orders_log(batch_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_yh_log_order ON raw_yahoo_orders_log(order_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_yh_log_date ON raw_yahoo_orders_log(order_time)');

  // 12. Yahoo!ショッピング受注（最新状態、order_id+line_id単位）
  db.exec(`CREATE TABLE IF NOT EXISTS raw_yahoo_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id            TEXT NOT NULL,
    order_time          TEXT,
    last_update_time    TEXT,
    order_status        TEXT,
    pay_status          TEXT,
    ship_status         TEXT,
    total_price         REAL,
    pay_charge          REAL,
    ship_charge         REAL,
    discount            REAL,
    use_point           REAL,
    line_id             INTEGER,
    item_id             TEXT,
    title               TEXT,
    sub_code            TEXT,
    unit_price          REAL,
    original_price      REAL,
    quantity            INTEGER,
    item_tax_ratio      REAL,
    coupon_discount     REAL,
    synced_at           TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_yh_orders_order ON raw_yahoo_orders(order_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_yh_orders_date ON raw_yahoo_orders(order_time)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_yh_orders_item ON raw_yahoo_orders(item_id)');

  // 13. 店舗マスタ
  db.exec(`CREATE TABLE IF NOT EXISTS shops (
    shop_code           TEXT PRIMARY KEY,
    shop_name           TEXT,
    platform            TEXT
  )`);

  // 店舗マスタは毎回洗い替え
  db.exec('DELETE FROM shops');
  insertDefaultShops();

  // ─── VIEWs ───

  // 統合商品マスタビュー（利益計算用）
  db.exec('DROP VIEW IF EXISTS v_product_master');
  db.exec(`
    CREATE VIEW v_product_master AS
    WITH
    set_costs AS (
      SELECT
        s.セット商品コード as 商品コード,
        ROUND(SUM(COALESCE(p.原価, 0) * s.数量), 2) as セット原価合計,
        MIN(CASE WHEN p.取扱区分 = '取扱中' THEN 1 ELSE 0 END) as 全構成品取扱中,
        MIN(COALESCE(p.消費税率, 10)) as セット消費税率
      FROM raw_ne_set_products s
      LEFT JOIN raw_ne_products p ON s.商品コード = p.商品コード COLLATE NOCASE
      GROUP BY s.セット商品コード
    ),
    resolved AS (
      SELECT
        p.商品コード,
        p.商品名,
        p.売価,
        CASE
          WHEN sc.商品コード IS NOT NULL AND sc.全構成品取扱中 = 0 THEN '取扱中止'
          ELSE p.取扱区分
        END as 取扱区分,
        p.仕入先コード,
        CASE
          WHEN sc.商品コード IS NOT NULL THEN sc.セット消費税率
          ELSE p.消費税率
        END as 消費税率,
        p.在庫数,
        p.引当数,
        p.代表商品コード,
        p.ロケーションコード,
        p.発注ロット単位,
        CASE
          WHEN p.原価 > 0 THEN p.原価
          WHEN sc.セット原価合計 IS NOT NULL THEN sc.セット原価合計
          WHEN eg.genka IS NOT NULL THEN eg.genka
          ELSE NULL
        END as 原価,
        CASE
          WHEN p.原価 > 0 THEN 'NE'
          WHEN sc.セット原価合計 IS NOT NULL THEN 'セット'
          WHEN eg.genka IS NOT NULL THEN '例外'
          ELSE '不明'
        END as 原価ソース,
        ps.ship_cost as 送料,
        ps.ship_method as 配送方法,
        ps.shipping_code as 配送コード,
        CASE WHEN sc.商品コード IS NOT NULL THEN 1 ELSE 0 END as is_set
      FROM raw_ne_products p
      LEFT JOIN set_costs sc ON p.商品コード = sc.商品コード COLLATE NOCASE
      LEFT JOIN exception_genka eg ON p.商品コード = eg.sku COLLATE NOCASE
      LEFT JOIN product_shipping ps ON p.商品コード = ps.sku COLLATE NOCASE
    )
    SELECT
      *,
      CASE WHEN 原価 IS NOT NULL THEN ROUND(売価 - 原価 - COALESCE(送料, 0), 2) ELSE NULL END as 粗利,
      CASE WHEN 売価 > 0 AND 原価 IS NOT NULL THEN ROUND((売価 - 原価 - COALESCE(送料, 0)) * 100.0 / 売価, 1) ELSE NULL END as 粗利率
    FROM resolved
  `);

  // 商品別販売集計VIEW
  db.exec('DROP VIEW IF EXISTS v_sales_by_product');
  db.exec(`
    CREATE VIEW v_sales_by_product AS
    SELECT
      COALESCE(sm.ne_code, o.seller_sku) as 商品コード,
      o.title as 商品名,
      'amazon' as platform,
      CASE WHEN o.fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END as channel,
      SUBSTR(o.purchase_date, 1, 7) as month,
      SUBSTR(o.purchase_date, 1, 10) as date,
      SUM(o.quantity) as 数量,
      SUM(o.item_price) as 売上金額,
      COUNT(DISTINCT o.amazon_order_id) as 注文数,
      'sp_api' as data_source
    FROM raw_sp_orders o
    LEFT JOIN sku_map sm ON o.seller_sku = sm.seller_sku COLLATE NOCASE
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY COALESCE(sm.ne_code, o.seller_sku), o.title, platform, channel, month, date
    UNION ALL
    SELECT
      r.item_number as 商品コード, r.item_name as 商品名,
      'rakuten' as platform, 'rakuten' as channel,
      SUBSTR(r.order_date, 1, 7) as month, SUBSTR(r.order_date, 1, 10) as date,
      SUM(r.units) as 数量, SUM(r.price_tax_incl * r.units) as 売上金額,
      COUNT(DISTINCT r.order_number) as 注文数, 'rakuten_api' as data_source
    FROM raw_rakuten_orders r
    WHERE r.delete_item_flag = 0 AND r.order_status != 900
    GROUP BY r.item_number, r.item_name, platform, channel, month, date
    UNION ALL
    SELECT
      o.商品コード, o.商品名, s.platform, s.platform as channel,
      SUBSTR(o.受注日, 1, 7) as month, SUBSTR(o.受注日, 1, 10) as date,
      SUM(o.受注数) as 数量, SUM(o.小計金額) as 売上金額,
      COUNT(DISTINCT o.伝票番号) as 注文数, 'ne' as data_source
    FROM raw_ne_orders o
    LEFT JOIN shops s ON o.店舗コード = s.shop_code
    WHERE o.キャンセル区分 = '有効'
      AND COALESCE(s.platform, '') NOT IN ('_ignore', 'amazon_fbm', 'rakuten')
    GROUP BY o.商品コード, o.商品名, s.platform, channel, month, date
  `);

  // 未登録データ検出VIEW
  // 注: 商品区分は m_products にのみ存在するので INNER JOIN で引く (raw_ne_products にはない)
  db.exec('DROP VIEW IF EXISTS v_missing_data');
  db.exec(`
    CREATE VIEW v_missing_data AS
    SELECT 'shipping' as missing_type, p.商品コード, p.商品名, p.売価, p.原価, p.取扱区分, NULL as last_sold
    FROM raw_ne_products p
    INNER JOIN m_products mp ON p.商品コード = mp.商品コード COLLATE NOCASE
    LEFT JOIN product_shipping ps ON p.商品コード = ps.sku COLLATE NOCASE
    WHERE mp.商品区分 = '単品' AND ps.sku IS NULL
    UNION ALL
    SELECT 'genka' as missing_type, p.商品コード, p.商品名, p.売価, p.原価, p.取扱区分, NULL as last_sold
    FROM raw_ne_products p
    INNER JOIN m_products mp ON p.商品コード = mp.商品コード COLLATE NOCASE
    LEFT JOIN exception_genka eg ON p.商品コード = eg.sku COLLATE NOCASE
    LEFT JOIN raw_ne_set_products sp ON p.商品コード = sp.セット商品コード
    WHERE mp.商品区分 = '単品' AND (p.原価 IS NULL OR p.原価 = 0) AND eg.sku IS NULL AND sp.セット商品コード IS NULL
    UNION ALL
    SELECT DISTINCT 'sku_map' as missing_type, o.seller_sku as 商品コード, o.title as 商品名,
      NULL as 売価, NULL as 原価, NULL as 取扱区分, MAX(SUBSTR(o.purchase_date, 1, 10)) as last_sold
    FROM raw_sp_orders o
    LEFT JOIN sku_map sm ON o.seller_sku = sm.seller_sku COLLATE NOCASE
    WHERE sm.seller_sku IS NULL AND o.order_status NOT IN ('Cancelled')
    GROUP BY o.seller_sku, o.title
  `);

  // 12. 変更履歴ログ（手動管理テーブルのCRUD操作を記録）
  db.exec(`CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    table_name TEXT NOT NULL,
    record_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    old_data TEXT,
    new_data TEXT,
    operator TEXT DEFAULT 'admin'
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_audit_table ON audit_log(table_name)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_audit_key ON audit_log(record_key)');

  // 12b. 売上分類マスタ（手動入力）
  db.exec(`CREATE TABLE IF NOT EXISTS product_sales_class (
    sku               TEXT PRIMARY KEY,
    sales_class       INTEGER NOT NULL,
    商品名            TEXT,
    synced_at         TEXT
  )`);
  // CHECK制約はSQLiteでは制限があるため、取り込み時にバリデーション

  // ─── 統合商品マスタ系 ───

  // 13. m_products（統合商品マスタ）
  db.exec(`CREATE TABLE IF NOT EXISTS m_products (
    product_id                INTEGER PRIMARY KEY AUTOINCREMENT,
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
    seasonality_flag          INTEGER DEFAULT 0,
    season_months             TEXT,
    new_product_flag          INTEGER DEFAULT 0,
    new_product_launch_date   TEXT,
    updated_at                TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mp_sku ON m_products(商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mp_status ON m_products(取扱区分)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mp_type ON m_products(商品区分)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mp_cost_status ON m_products(原価状態)');
  // 既存DBへのカラム追加マイグレーション（商品収益性ダッシュボード Phase 1）
  addColumnIfMissing('m_products', 'seasonality_flag', 'INTEGER DEFAULT 0');
  addColumnIfMissing('m_products', 'season_months', 'TEXT');
  addColumnIfMissing('m_products', 'new_product_flag', 'INTEGER DEFAULT 0');
  addColumnIfMissing('m_products', 'new_product_launch_date', 'TEXT');

  // 14. m_products_staging（常設staging）
  db.exec(`CREATE TABLE IF NOT EXISTS m_products_staging (
    product_id                INTEGER PRIMARY KEY AUTOINCREMENT,
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
    seasonality_flag          INTEGER DEFAULT 0,
    season_months             TEXT,
    new_product_flag          INTEGER DEFAULT 0,
    new_product_launch_date   TEXT,
    updated_at                TEXT NOT NULL
  )`);
  addColumnIfMissing('m_products_staging', 'seasonality_flag', 'INTEGER DEFAULT 0');
  addColumnIfMissing('m_products_staging', 'season_months', 'TEXT');
  addColumnIfMissing('m_products_staging', 'new_product_flag', 'INTEGER DEFAULT 0');
  addColumnIfMissing('m_products_staging', 'new_product_launch_date', 'TEXT');

  // 15. m_set_components（セット構成マスタ）
  db.exec(`CREATE TABLE IF NOT EXISTS m_set_components (
    セット商品コード  TEXT NOT NULL,
    構成商品コード    TEXT NOT NULL,
    数量              INTEGER NOT NULL DEFAULT 1,
    構成商品名        TEXT,
    構成商品原価      REAL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (セット商品コード, 構成商品コード)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_msc_parent ON m_set_components(セット商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_msc_child ON m_set_components(構成商品コード)');

  // 16. m_set_components_staging（常設staging）
  db.exec(`CREATE TABLE IF NOT EXISTS m_set_components_staging (
    セット商品コード  TEXT NOT NULL,
    構成商品コード    TEXT NOT NULL,
    数量              INTEGER NOT NULL DEFAULT 1,
    構成商品名        TEXT,
    構成商品原価      REAL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (セット商品コード, 構成商品コード)
  )`);

  // 17. f_sales_by_listing（モール別・ページ単位の日次集計）
  db.exec(`CREATE TABLE IF NOT EXISTS f_sales_by_listing (
    日付              TEXT NOT NULL,
    月                TEXT NOT NULL,
    モール            TEXT NOT NULL,
    モール商品コード  TEXT NOT NULL,
    チャネル          TEXT NOT NULL DEFAULT '',
    商品名            TEXT,
    数量              INTEGER NOT NULL DEFAULT 0,
    売上金額          REAL,
    注文数            INTEGER,
    データソース      TEXT,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (日付, モール, モール商品コード, チャネル)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_fsl_month ON f_sales_by_listing(月)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_fsl_mall ON f_sales_by_listing(モール)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_fsl_item ON f_sales_by_listing(モール商品コード)');

  // 18. f_sales_by_product（NE商品コード単位・全モール横断の日次集計、縦持ち）
  // 売上金額: 単品(直接販売)は元の販売金額、セット経由分はセット販売金額を構成数比で按分
  db.exec(`CREATE TABLE IF NOT EXISTS f_sales_by_product (
    日付              TEXT NOT NULL,
    商品コード        TEXT NOT NULL,
    モール            TEXT NOT NULL,
    商品名            TEXT,
    数量              INTEGER NOT NULL DEFAULT 0,
    直接販売数        INTEGER DEFAULT 0,
    セット経由数      INTEGER DEFAULT 0,
    売上金額          REAL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (日付, 商品コード, モール)
  )`);
  // マイグレーション: 既存テーブルに 売上金額 列がなければ追加
  try {
    const fspCols = db.prepare('PRAGMA table_info(f_sales_by_product)').all().map(c => c.name);
    if (!fspCols.includes('売上金額')) {
      db.exec('ALTER TABLE f_sales_by_product ADD COLUMN 売上金額 REAL');
    }
  } catch {}
  db.exec('CREATE INDEX IF NOT EXISTS idx_fsp_month ON f_sales_by_product(SUBSTR(日付, 1, 7))');
  db.exec('CREATE INDEX IF NOT EXISTS idx_fsp_sku ON f_sales_by_product(商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_fsp_mall ON f_sales_by_product(モール)');

  // 19. unmapped_sales（マッピング失敗退避）
  db.exec(`CREATE TABLE IF NOT EXISTS unmapped_sales (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    日付              TEXT NOT NULL,
    モール            TEXT NOT NULL,
    モール商品コード  TEXT NOT NULL,
    商品名            TEXT,
    数量              INTEGER,
    売上金額          REAL,
    失敗理由          TEXT,
    created_at        TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_unmapped_date ON unmapped_sales(日付)');

  // ─── その他メタ ───

  // 20. 同期メタデータ
  db.exec(`CREATE TABLE IF NOT EXISTS sync_meta (
    key                 TEXT PRIMARY KEY,
    value               TEXT,
    updated_at          TEXT
  )`);

  // 21. Amazon SKU手数料キャッシュ（粗利ダッシュボード用）
  //     SP-API getMyFeesEstimates でバッチ取得した結果を保存
  //     更新頻度: 月1全SKU + 週1直近売れたSKU
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_sku_fees (
    seller_sku          TEXT NOT NULL,
    asin                TEXT,
    fulfillment_channel TEXT,
    referral_fee        REAL,
    referral_fee_rate   REAL,
    fba_fee             REAL,
    variable_closing_fee REAL,
    per_item_fee        REAL,
    total_fee           REAL,
    price_used          REAL,
    fetched_at          TEXT NOT NULL,
    PRIMARY KEY (seller_sku)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_asf_asin ON amazon_sku_fees(asin)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_asf_channel ON amazon_sku_fees(fulfillment_channel)');

  // 22. 月末在庫スナップショット（商品収益性ダッシュボード タブB 移動平均計算用）
  //     ロジザード raw_lz_inventory から月末時点を切り出して保存
  //     GMROI 計算で「移動平均在庫数 × 原価」の基礎になる
  db.exec(`CREATE TABLE IF NOT EXISTS stock_monthly_snapshot (
    年月              TEXT NOT NULL,
    商品コード        TEXT NOT NULL,
    月末在庫数        INTEGER NOT NULL DEFAULT 0,
    月末引当数        INTEGER DEFAULT 0,
    snapshot_source   TEXT,
    captured_at       TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (年月, 商品コード)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sms_month ON stock_monthly_snapshot(年月)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sms_sku ON stock_monthly_snapshot(商品コード)');

  // 22. SKUマスタ（人手キュレートの seller_sku 単位の正本）
  // sku_map（自動検出、毎日DELETE+INSERT）と分離。商品名は社内独自名。
  db.exec(`CREATE TABLE IF NOT EXISTS m_sku_master (
    seller_sku  TEXT NOT NULL PRIMARY KEY,
    商品名      TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    created_by  TEXT,
    updated_by  TEXT,
    CHECK (trim(seller_sku) <> ''),
    CHECK (trim(商品名) <> ''),
    CHECK (seller_sku = lower(seller_sku) AND trim(seller_sku) = seller_sku)
  )`);

  // 23. SKU構成（1 SKU = N components、セット商品で複数 ne_code）
  // FK: seller_sku → m_sku_master.seller_sku（CASCADE削除）
  // ne_code は raw_ne_products.商品コード に存在することをAPIレベルでバリデート
  db.exec(`CREATE TABLE IF NOT EXISTS m_sku_components (
    seller_sku  TEXT NOT NULL,
    ne_code     TEXT NOT NULL,
    数量        INTEGER NOT NULL DEFAULT 1,
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (seller_sku, ne_code),
    FOREIGN KEY (seller_sku) REFERENCES m_sku_master(seller_sku) ON DELETE CASCADE,
    CHECK (数量 > 0),
    CHECK (trim(seller_sku) <> ''),
    CHECK (trim(ne_code) <> ''),
    CHECK (seller_sku = lower(seller_sku) AND trim(seller_sku) = seller_sku),
    CHECK (ne_code = lower(ne_code) AND trim(ne_code) = ne_code)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_m_sku_components_ne ON m_sku_components(ne_code)');

  // 24. NE在庫の日次スナップショット
  // raw_ne_products は毎朝 NE API で全件上書きされるため履歴が消える。
  // daily-sync.js が NE API 取得成功直後に raw_ne_products を本テーブルへ複製し履歴化する。
  // 用途: 過去日の自社倉庫在庫金額の再計算 (原価変動があっても遡って計算可能)
  // business_date は JST 固定 (UTC癖回避)
  db.exec(`CREATE TABLE IF NOT EXISTS ne_stock_daily_snapshot (
    business_date  TEXT NOT NULL,
    商品コード     TEXT NOT NULL,
    在庫数         INTEGER NOT NULL,
    captured_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (business_date, 商品コード)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ne_stock_snap_code ON ne_stock_daily_snapshot(商品コード)');

  // 25. 在庫スナップショット日次サマリ (UI 推移グラフ用)
  // ne_stock_daily_snapshot + fba.db.daily_snapshots を SKU解決+原価で金額化した結果。
  // PR-B 集計スクリプト snapshot-inventory-aggregate.js が毎朝書く。
  // 米国FBA将来追加に備えて market 列、SP-API 失敗日を欠損として識別するため source_status・row_count を持つ。
  db.exec(`CREATE TABLE IF NOT EXISTS inv_daily_summary (
    business_date     TEXT NOT NULL,
    market            TEXT NOT NULL DEFAULT 'jp',
    category          TEXT NOT NULL,
    total_qty         INTEGER NOT NULL,
    total_value       REAL,
    resolved_count    INTEGER NOT NULL DEFAULT 0,
    unresolved_count  INTEGER NOT NULL DEFAULT 0,
    cost_missing_count INTEGER NOT NULL DEFAULT 0,
    source_status     TEXT NOT NULL,
    source_row_count  INTEGER,
    captured_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (business_date, market, category),
    CHECK (category IN ('fba_warehouse', 'fba_inbound', 'own_warehouse', 'fba_us')),
    CHECK (source_status IN ('ok', 'partial', 'failed', 'no_source'))
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_daily_summary_date ON inv_daily_summary(business_date)');

  // 26. 在庫スナップショット詳細層 (drill-down + AI 分析用)
  // 1日 ~5,000-6,000行 × 365日 = 約 220万行/年。SQLite で十分扱える。
  // PK は (business_date, market, category, source_system, source_item_code, ne_code)
  // - source_item_code: FBA=seller_sku / 自社=ne_code (両方を統一する意味で)
  // - is_bundle_expanded=1 のとき、source_item_code は親SKU、ne_code は構成品
  // 業務定義 (Codex 推奨でコメント明記):
  //   qty             : 販売可能在庫 (FBA は4列合算 / 自社は raw_ne_products.在庫数)
  //   reserved_qty    : 自社のみ - 引当数 (販売中で出荷待ち)
  //   pending_order_qty: 自社のみ - 発注残数 (発注済み未着、SKU別)
  //   fba_unfulfillable_qty: FBAのみ - 販売不可在庫 (評価対象外、健全性指標)
  // 属性凍結ルール: m_products / raw_ne_products の snapshot 時点値をコピー保持
  //   理由: 商品名・仕入先等が将来変わっても過去データの解釈を保持
  db.exec(`CREATE TABLE IF NOT EXISTS inv_daily_detail (
    business_date              TEXT NOT NULL,
    market                     TEXT NOT NULL DEFAULT 'jp',
    category                   TEXT NOT NULL,
    source_system              TEXT NOT NULL,
    source_item_code           TEXT NOT NULL,
    ne_code                    TEXT NOT NULL,
    -- 数量・金額
    qty                        INTEGER NOT NULL,
    unit_cost                  REAL,
    total_value                REAL,
    cost_status                TEXT NOT NULL,
    cost_source                TEXT,
    resolution_method          TEXT,
    is_bundle_expanded         INTEGER NOT NULL DEFAULT 0,
    component_qty              INTEGER,
    -- m_products 凍結 (両カテゴリ共通)
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
    -- 売上履歴 (snapshot 時点)
    last_sold_date             TEXT,
    sales_7d_qty               INTEGER,
    sales_30d_qty              INTEGER,
    sales_90d_qty              INTEGER,
    sales_7d_value             REAL,
    sales_30d_value            REAL,
    sales_90d_value            REAL,
    -- FBA 固有 (own_warehouse は NULL)
    working_first_seen         TEXT,
    fba_unfulfillable_qty      INTEGER,
    -- 自社倉庫 固有 (FBA は NULL)
    reserved_qty               INTEGER,
    pending_order_qty          INTEGER,
    location_code              TEXT,
    last_purchase_date         TEXT,
    -- メタ
    snapshot_run_id            TEXT NOT NULL,
    ingested_at                TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (business_date, market, category, source_system, source_item_code, ne_code),
    CHECK (category IN ('fba_warehouse', 'fba_inbound', 'own_warehouse', 'fba_us', 'pending_orders')),
    CHECK (source_system IN ('ne', 'fba')),
    CHECK (cost_status IN ('ok', 'cost_missing', 'ne_missing')),
    CHECK (cost_source IS NULL OR cost_source IN ('m_products', 'fallback', 'missing')),
    CHECK (resolution_method IS NULL OR resolution_method IN ('master', 'sku_map', 'direct', 'unresolved')),
    CHECK (sales_class IS NULL OR sales_class IN (1, 2, 3, 4))
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_date ON inv_daily_detail(business_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_date_cat ON inv_daily_detail(business_date, category)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_date_ne ON inv_daily_detail(business_date, ne_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_supplier ON inv_daily_detail(supplier_code)');

  // 27. 集計実行ログ (Codex 推奨「壊れた run は採用しない」ガード)
  // snapshot_run_id ごとに件数・金額・NULL率を記録、status=failed の run は UI で除外
  db.exec(`CREATE TABLE IF NOT EXISTS inv_daily_run_log (
    snapshot_run_id      TEXT PRIMARY KEY,
    business_date        TEXT NOT NULL,
    started_at           TEXT NOT NULL,
    finished_at          TEXT,
    status               TEXT NOT NULL,
    detail_total_rows    INTEGER,
    detail_total_value   REAL,
    cost_missing_count   INTEGER,
    ne_missing_count     INTEGER,
    error_message        TEXT,
    notes                TEXT,
    CHECK (status IN ('running', 'ok', 'partial', 'failed'))
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_runlog_date ON inv_daily_run_log(business_date)');

  // 28. 派生指標ビュー (DOS / ABC / 回転率)
  // 再計算可能なので保存しない。view で常に最新計算
  db.exec('DROP VIEW IF EXISTS v_inv_daily_metrics');
  db.exec(`CREATE VIEW v_inv_daily_metrics AS
    SELECT
      business_date, market, category, source_system, source_item_code, ne_code,
      qty,
      total_value,
      sales_7d_qty, sales_30d_qty, sales_90d_qty,
      sales_7d_value, sales_30d_value, sales_90d_value,
      last_sold_date,
      -- DOS (Days Of Supply): 在庫数 ÷ (30日売上 ÷ 30) = 在庫数 × 30 / 30日売上
      CASE
        WHEN sales_30d_qty > 0 THEN ROUND(qty * 30.0 / sales_30d_qty, 1)
        ELSE NULL
      END AS days_of_supply,
      -- 回転率(年): 365日 ÷ DOS = 365 × 30日売上 ÷ (在庫数 × 30) = 365 × 売上 / (30 × 在庫)
      CASE
        WHEN qty > 0 AND sales_30d_qty > 0 THEN ROUND(365.0 * sales_30d_qty / (qty * 30.0), 2)
        ELSE NULL
      END AS turnover_yearly,
      -- 滞留判定: 90日売上0 かつ 在庫>0
      CASE WHEN (sales_90d_qty IS NULL OR sales_90d_qty = 0) AND qty > 0 THEN 1 ELSE 0 END AS is_stale,
      product_name,
      supplier_code,
      product_type, handling_class, sales_class,
      seasonality_flag, new_product_flag,
      cost_status, resolution_method
    FROM inv_daily_detail
  `);

  // 29. ビュー: SKU紐付け解決（master優先 + sku_map フォールバック）
  // 既存 sku_map（自動検出）は m_sku_master 未登録の seller_sku のみフォールバック対象
  // fallback遮断条件は COLLATE NOCASE で大文字小文字差を吸収（万一データ混入時の二重計上防止）
  // 依存順 (v_sku_costed → v_sku_resolved) で DROP してから再作成、定義変更を確実に反映
  db.exec('DROP VIEW IF EXISTS v_sku_costed');
  db.exec('DROP VIEW IF EXISTS v_sku_resolved');
  db.exec(`CREATE VIEW v_sku_resolved AS
    SELECT
      c.seller_sku,
      c.ne_code,
      c.数量,
      'master' AS source
    FROM m_sku_components c
    UNION ALL
    SELECT
      s.seller_sku,
      s.ne_code,
      s.数量,
      'auto' AS source
    FROM sku_map s
    WHERE NOT EXISTS (
      SELECT 1 FROM m_sku_master m WHERE m.seller_sku = s.seller_sku COLLATE NOCASE
    )
  `);

  // 25. ビュー: SKU紐付け＋原価解決
  // raw_ne_products から原価をJOIN。原価NULLは cost_status='cost_missing' で示す
  // exception_genka はレガシー、新規参照しない方針
  db.exec(`CREATE VIEW v_sku_costed AS
    SELECT
      v.seller_sku,
      v.ne_code,
      v.数量,
      v.source,
      p.原価 AS 単価,
      CASE
        WHEN p.商品コード IS NULL THEN 'ne_missing'
        WHEN p.原価 IS NULL THEN 'cost_missing'
        ELSE 'ok'
      END AS cost_status
    FROM v_sku_resolved v
    LEFT JOIN raw_ne_products p ON v.ne_code = p.商品コード
  `);
}

function insertDefaultShops() {
  // NE店舗コード一覧（2026-03-31確認）
  // 集計時の注意:
  //   - コード7（ライジングAmazon）: 集計対象外（使用していない）
  //   - コード15（FBA納品）: 集計対象外（FBA納品プラン用の内部伝票。実売上ではない）
  //   - コード4（Amazon店）: FBM（自社出荷）のみ。FBA販売はNEを経由しないためSP-APIから別途取得が必要
  //   - コード11/14（LINEギフト）: 途中で切替があり2つ存在。現在はどちらか片方のみ使用
  const shops = [
    ['1', '雑貨イズム楽天市場店', 'rakuten'],
    ['2', '雑貨イズムYahoo!店', 'yahoo'],
    ['3', 'ヤフオク店', 'yahoo_auction'],
    ['4', '雑貨イズムAmazon店', 'amazon_fbm'],
    ['5', '雑貨イズムauPay!店', 'aupay'],
    ['6', '雑貨イズムQoo10店', 'qoo10'],
    ['7', 'ライジングAmazon', '_ignore'],
    ['8', '雑貨イズムメルカリshops', 'mercari'],
    ['9', 'ラクマ', 'rakuma'],
    ['10', '卸', 'wholesale'],
    ['11', 'LINEギフト', 'linegift'],
    ['12', '雑貨イズムMy Smart Store店', 'mysmartstore'],
    ['13', '雑貨イズムdショッピング', 'dshopping'],
    ['14', 'LINE ギフト', 'linegift'],
    ['15', 'FBA納品', '_ignore'],
  ];

  const stmt = db.prepare('INSERT OR IGNORE INTO shops (shop_code, shop_name, platform) VALUES (?, ?, ?)');
  for (const [code, name, platform] of shops) {
    stmt.run(code, name, platform);
  }
  console.log('[Warehouse] 店舗マスタ初期投入完了');
}

// ─── 統計取得 ───

export function getStats() {
  const tables = ['raw_ne_products', 'raw_ne_orders', 'raw_ne_set_products', 'raw_sp_orders', 'raw_sp_orders_log', 'raw_rakuten_orders', 'raw_rakuten_orders_log', 'raw_lz_inventory', 'sku_map', 'product_shipping', 'shipping_rates', 'exception_genka', 'product_sales_class', 'shops', 'm_products', 'm_set_components', 'f_sales_by_listing', 'f_sales_by_product', 'unmapped_sales', 'amazon_sku_fees', 'stock_monthly_snapshot', 'm_sku_master', 'm_sku_components'];
  const stats = {};

  for (const table of tables) {
    try {
      const row = db.prepare(`SELECT COUNT(*) as cnt FROM ${table}`).get();
      stats[table] = row.cnt;
    } catch {
      stats[table] = 0;
    }
  }

  // 受注の日付範囲
  try {
    const range = db.prepare('SELECT MIN(受注日) as min_date, MAX(受注日) as max_date FROM raw_ne_orders').get();
    if (range.min_date) {
      stats.ne_order_date_range = { min: range.min_date, max: range.max_date };
    }
  } catch {}

  try {
    const range = db.prepare('SELECT MIN(purchase_date) as min_date, MAX(purchase_date) as max_date FROM raw_sp_orders').get();
    if (range.min_date) {
      stats.sp_order_date_range = { min: range.min_date, max: range.max_date };
    }
  } catch {}

  // 同期メタデータ
  try {
    const rows = db.prepare('SELECT key, value FROM sync_meta ORDER BY key').all();
    stats.sync_meta = {};
    for (const row of rows) {
      stats.sync_meta[row.key] = row.value;
    }
  } catch {
    stats.sync_meta = {};
  }

  return stats;
}

export function updateSyncMeta(key, value) {
  db.prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(key, value, new Date().toISOString().replace('T', ' ').slice(0, 19));
}
