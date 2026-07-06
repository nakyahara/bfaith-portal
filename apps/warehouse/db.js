/**
 * warehouse.db — 社内マスターデータ基盤
 *
 * better-sqlite3 使用（ファイルベース、メモリ制限なし）
 *
 * テーブル構成:
 *   NE系: raw_ne_products / raw_ne_orders / raw_ne_set_products
 *   SP-API系: raw_sp_orders_log / raw_sp_orders
 *   ロジザード: raw_lz_inventory
 *   マッピング: m_sku_master / m_sku_components / product_shipping / shipping_rates / exception_genka
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
  // SKU管理統合 Step 6 (2026-05-04): Amazon SKU解決を sku_map → v_sku_components_first (master only) に切替
  //   セット親代表 ne_code (sort_order=0) を採用。未登録 SKU は seller_sku のまま。
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
    LEFT JOIN v_sku_components_first sm ON o.seller_sku = sm.seller_sku COLLATE NOCASE
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
  // SKU管理統合 Step 6 (2026-05-04): missing_type='sku_map' の判定を sku_map 不在 → m_sku_master 不在 に変更
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
    LEFT JOIN m_sku_master mm ON o.seller_sku = mm.seller_sku COLLATE NOCASE
    WHERE mm.seller_sku IS NULL AND o.order_status NOT IN ('Cancelled')
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

  // 12c. 発注設定マスタ（手動入力）— 推奨保有月数（「何ヶ月分の在庫を持つか」の係数）
  //   商品管理リストの「推奨保有在庫」列に相当。販売データから自動算出できない手動パラメータ。
  //   新商品は GET /api/reorder/unregistered（未登録リスト）に上がってきて、register UI で登録する。
  db.exec(`CREATE TABLE IF NOT EXISTS m_reorder_setting (
    sku               TEXT PRIMARY KEY,
    推奨保有月数      REAL NOT NULL CHECK (推奨保有月数 >= 0 AND 推奨保有月数 <= 60),
    商品名            TEXT,
    updated_by        TEXT,
    synced_at         TEXT
  )`);
  // 値域は DB CHECK + API/取り込み側バリデーションの二重防御

  // 12d. 販売速度サマリ（商品管理リスト用）— FBA/FBA以外 × 7日/30日 の純販売数(受注日ベース)
  //   FBA      = raw_sp_orders(fulfillment_channel='Amazon') を SKUマップで NE商品コードへ解決・セット展開
  //   FBA以外  = raw_ne_orders(キャンセル区分='有効'、_ignore店舗除外) を商品コード単位で集計
  //     ※ NE受注は Amazon FBM/楽天/Yahoo/auPay/LINEギフト/メルカリ/Qoo10/卸 を含み、FBAは構造的に非含有
  //       (ne_fba_overlap 実測 2026-06-08: FBA注文ID∩NE受注番号=0/175,097)。
  //   毎朝 rebuild-sales-velocity.js で洗い替え(as_of=前日JST)。母集団は売上のある商品コードのみ(疎)。
  db.exec(`CREATE TABLE IF NOT EXISTS f_sales_velocity_by_product (
    商品コード        TEXT PRIMARY KEY,
    qty_7d_fba        INTEGER NOT NULL DEFAULT 0,
    qty_7d_nonfba     INTEGER NOT NULL DEFAULT 0,
    qty_7d_total      INTEGER NOT NULL DEFAULT 0,
    qty_30d_fba       INTEGER NOT NULL DEFAULT 0,
    qty_30d_nonfba    INTEGER NOT NULL DEFAULT 0,
    qty_30d_total     INTEGER NOT NULL DEFAULT 0,
    as_of_date        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
  )`);

  // 12d-2. 販売速度 モール別版（仕入れ先売れ筋共有の「速報モール別」用）
  //   f_sales_velocity_by_product と同じ注文ベース集計を mall 次元込みで持つ。
  //   mall = shops.platform（rakuten/yahoo/aupay/qoo10/mercari/linegift/amazon_fbm 等）
  //          ＋ Amazon FBA は SP-API 由来を 'amazon_fba' として別計上（FBM=NE と二重計上しない）。
  //   商品コード は NE商品コード（NE側はセット展開済 / FBA側は seller_sku→ne_code→構成数 展開）。
  db.exec(`CREATE TABLE IF NOT EXISTS f_sales_velocity_by_product_mall (
    商品コード        TEXT NOT NULL,
    mall              TEXT NOT NULL,
    qty_7d            INTEGER NOT NULL DEFAULT 0,
    qty_30d           INTEGER NOT NULL DEFAULT 0,
    as_of_date        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (商品コード, mall)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_fsvm_code ON f_sales_velocity_by_product_mall(商品コード)');

  // 12e. 商品管理リスト スナップショット（現スプレッドシート互換の完成行を warehouse 側で確定）
  //   meta(run_id) + rows(run_id, 商品コード) を append、published_run_id を一括切替(atomic publish)。
  //   Render/GAS/画面は published_run_id のみ参照。母集団は m_products 起点(全件 left join)。
  //   ソース: m_products + raw_ne_products(自社在庫/最終仕入日等) + inv_daily_detail(FBA在庫)
  //          + f_sales_velocity_by_product(販売数) + m_reorder_setting(推奨保有月数)。
  db.exec(`CREATE TABLE IF NOT EXISTS product_management_snapshot_meta (
    run_id                    TEXT PRIMARY KEY,
    as_of_date                TEXT,
    generated_at              TEXT NOT NULL,
    status                    TEXT NOT NULL,          -- ok / partial / failed
    row_count                 INTEGER NOT NULL DEFAULT 0,
    payload_checksum          TEXT,
    -- ソース別 watermark / 健全性
    src_ne_products_synced_at TEXT,
    src_velocity_as_of        TEXT,
    src_fba_business_date     TEXT,
    src_reorder_updated_at    TEXT,
    fba_unmapped_qty          INTEGER,
    ne_fba_overlap            INTEGER,
    notes                     TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS product_management_snapshot_rows (
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
  // 公開ポインタ（単一行、id=1）。生成完了後に run_id を一括切替。
  db.exec(`CREATE TABLE IF NOT EXISTS product_management_published (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    run_id        TEXT NOT NULL,
    published_at  TEXT NOT NULL
  )`);
  // 既存テーブルへの列追加 (売上分類=商品管理リストの「商品区分 1自社/2AMC/3仕入」)
  addColumnIfMissing('product_management_snapshot_rows', '売上分類', 'INTEGER');

  // 商品管理リスト オンデマンドFBA更新 (Part2): RESTOCK ライブ在庫 (warehouse.db 内、全件入替)
  //   build --fba-source=live がこの表を v_sku_resolved で展開して FBA在庫数を算出する。
  //   daily_snapshots(日次の不変スナップショット)は触らず、ライブ取得はこの1表だけに入れる。
  //   全行が同一 source_run_id / fetched_at を持つ (取得ジョブが1トランザクションで全件置換)。
  db.exec(`CREATE TABLE IF NOT EXISTS fba_restock_live (
    amazon_sku            TEXT PRIMARY KEY,
    fba_available         INTEGER DEFAULT 0,
    fba_fc_transfer       INTEGER DEFAULT 0,
    fba_fc_processing     INTEGER DEFAULT 0,
    fba_customer_order    INTEGER DEFAULT 0,
    fba_inbound_working   INTEGER DEFAULT 0,
    fba_inbound_shipped   INTEGER DEFAULT 0,
    fba_inbound_received  INTEGER DEFAULT 0,
    source_run_id         TEXT NOT NULL,
    fetched_at            TEXT NOT NULL
  )`);
  // PMLスナップショットの「FBA値の鮮度」メタ (daily=朝の日次 / live=オンデマンドRESTOCK)
  addColumnIfMissing('product_management_snapshot_meta', 'fba_source_kind', 'TEXT');        // 'daily' | 'live'
  addColumnIfMissing('product_management_snapshot_meta', 'fba_source_run_id', 'TEXT');      // live時: RESTOCK取得run_id
  addColumnIfMissing('product_management_snapshot_meta', 'fba_fetched_at', 'TEXT');         // live時: SP-API取得時刻
  addColumnIfMissing('product_management_snapshot_meta', 'fba_latest_row_count', 'INTEGER');// live時: 取得SKU件数

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
  // SKU管理統合で sku_map (自動検出) を廃止し、本マスタに一本化 (Step 1〜7)。商品名は社内独自名。
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
    CHECK (category IN ('fba_warehouse', 'fba_inbound', 'own_warehouse', 'fba_us', 'fba_us_warehouse', 'fba_us_inbound')),
    CHECK (source_status IN ('ok', 'partial', 'failed', 'no_source'))
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_daily_summary_date ON inv_daily_summary(business_date)');

  // --- migration: 既存テーブルの CHECK 制約を新カテゴリ (fba_us_warehouse/fba_us_inbound) 対応に更新 ---
  {
    const sqlRow = db.prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name='inv_daily_summary'`).get();
    if (sqlRow && !sqlRow.sql.includes('fba_us_warehouse')) {
      console.log('[Warehouse] inv_daily_summary CHECK 制約を migration: fba_us_warehouse/fba_us_inbound 追加');
      db.exec(`
        BEGIN TRANSACTION;
        CREATE TABLE inv_daily_summary_new (
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
          CHECK (category IN ('fba_warehouse', 'fba_inbound', 'own_warehouse', 'fba_us', 'fba_us_warehouse', 'fba_us_inbound')),
          CHECK (source_status IN ('ok', 'partial', 'failed', 'no_source'))
        );
        INSERT INTO inv_daily_summary_new SELECT * FROM inv_daily_summary;
        DROP TABLE inv_daily_summary;
        ALTER TABLE inv_daily_summary_new RENAME TO inv_daily_summary;
        COMMIT;
      `);
      db.exec('CREATE INDEX IF NOT EXISTS idx_inv_daily_summary_date ON inv_daily_summary(business_date)');
    }
  }

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
    CHECK (category IN ('fba_warehouse', 'fba_inbound', 'own_warehouse', 'fba_us', 'fba_us_warehouse', 'fba_us_inbound', 'pending_orders')),
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

  // --- migration: inv_daily_detail CHECK 制約も同様に更新 ---
  {
    const sqlRow = db.prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name='inv_daily_detail'`).get();
    if (sqlRow && !sqlRow.sql.includes('fba_us_warehouse')) {
      console.log('[Warehouse] inv_daily_detail CHECK 制約を migration: fba_us_warehouse/fba_us_inbound 追加');
      // DROP VIEW + DDL + INSERT を1トランザクションに包む
      // (途中失敗で view だけ消えてテーブルが残る状態を防止。後続 createTables で view 再作成)
      db.exec(`
        BEGIN TRANSACTION;
        DROP VIEW IF EXISTS v_inv_daily_metrics;
        CREATE TABLE inv_daily_detail_new (
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
          snapshot_run_id            TEXT NOT NULL,
          ingested_at                TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          PRIMARY KEY (business_date, market, category, source_system, source_item_code, ne_code),
          CHECK (category IN ('fba_warehouse', 'fba_inbound', 'own_warehouse', 'fba_us', 'fba_us_warehouse', 'fba_us_inbound', 'pending_orders')),
          CHECK (source_system IN ('ne', 'fba')),
          CHECK (cost_status IN ('ok', 'cost_missing', 'ne_missing')),
          CHECK (cost_source IS NULL OR cost_source IN ('m_products', 'fallback', 'missing')),
          CHECK (resolution_method IS NULL OR resolution_method IN ('master', 'sku_map', 'direct', 'unresolved')),
          CHECK (sales_class IS NULL OR sales_class IN (1, 2, 3, 4))
        );
        INSERT INTO inv_daily_detail_new SELECT * FROM inv_daily_detail;
        DROP TABLE inv_daily_detail;
        ALTER TABLE inv_daily_detail_new RENAME TO inv_daily_detail;
        COMMIT;
      `);
      db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_date ON inv_daily_detail(business_date)');
      db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_date_cat ON inv_daily_detail(business_date, category)');
      db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_date_ne ON inv_daily_detail(business_date, ne_code)');
      db.exec('CREATE INDEX IF NOT EXISTS idx_inv_dd_supplier ON inv_daily_detail(supplier_code)');
    }
  }

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

  // 29. ビュー: SKU紐付け解決
  // SKU管理統合 Step 4-0 (2026-05-04): auto fallback 撤廃、m_sku_components 直接ラッパーへ
  //   旧: master優先 + sku_map fallback (UNION ALL)
  //   新: m_sku_components 直接、master only。sort_order を新規公開
  //   sku_map writer は Step 3a/3b で停止済み、reader (本ビュー経由) も本変更で master only に
  // 依存順 (v_sku_costed → v_sku_resolved → v_sku_components_first) で DROP してから再作成
  db.exec('DROP VIEW IF EXISTS v_sku_costed');
  db.exec('DROP VIEW IF EXISTS v_sku_resolved');
  db.exec('DROP VIEW IF EXISTS v_sku_components_first');
  db.exec(`CREATE VIEW v_sku_resolved AS
    SELECT
      seller_sku,
      ne_code,
      数量,
      sort_order,
      'master' AS source
    FROM m_sku_components
  `);

  // 30. ビュー: セット親代表ne_code (sort_order=0 のみ)
  // SKU管理統合 Step 4-0 (2026-05-04): 売上分析アプリ v_sales_unified の旧 sku_map JOIN 置換用。
  //   1 SKU = 1 行になり、JOIN による行増殖を防ぐ。Phase 4-C / Step 6 で多数のビューに適用済。
  db.exec(`CREATE VIEW v_sku_components_first AS
    SELECT seller_sku, ne_code, 数量
    FROM m_sku_components
    WHERE sort_order = 0
  `);

  // 25. ビュー: SKU紐付け＋原価解決
  // raw_ne_products から原価をJOIN。原価NULLは cost_status='cost_missing' で示す
  // exception_genka はレガシー、新規参照しない方針
  // 25. ビュー: SKU紐付け＋原価解決 (Phase 2.4.6 v2、2026-05-05〜2026-05-06 確立)
  //   修正点 (旧版から):
  //     - raw_ne_products → m_products に統一 (原価正本)
  //     - direct_master fallback 追加: m_products に直接マッチする SKU (Amazon SKU = 商品コード)
  //     - NOT IN → NOT EXISTS (NULL safe)
  //   結果: 原価カバー 71.9% → 99.4%
  db.exec(`DROP VIEW IF EXISTS v_sku_costed`);
  db.exec(`CREATE VIEW v_sku_costed AS
    WITH resolved AS (
      SELECT seller_sku, ne_code, 数量, source
      FROM v_sku_resolved
    ),
    cost_master AS (
      SELECT 商品コード AS product_code, 原価 AS unit_cost
      FROM m_products
      WHERE 商品コード IS NOT NULL
    )
    SELECT
      r.seller_sku,
      r.ne_code,
      r.数量,
      r.source,
      cm.unit_cost AS 単価,
      CASE
        WHEN r.ne_code IS NULL THEN 'ne_missing'
        WHEN cm.product_code IS NULL OR cm.unit_cost IS NULL THEN 'cost_missing'
        ELSE 'ok'
      END AS cost_status
    FROM resolved r
    LEFT JOIN cost_master cm ON cm.product_code = r.ne_code

    UNION ALL

    -- direct_master fallback: 自社内製コード (Amazon SKU = 商品コード)
    SELECT
      p.商品コード AS seller_sku,
      p.商品コード AS ne_code,
      1 AS 数量,
      'direct_master' AS source,
      p.原価 AS 単価,
      CASE WHEN p.原価 IS NULL THEN 'cost_missing' ELSE 'ok' END AS cost_status
    FROM m_products p
    WHERE p.商品コード IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM v_sku_resolved r WHERE r.seller_sku = p.商品コード
      )
  `);

  // 31. Phase 3.1.x: Settlement Report 一本化 (2026-05-05〜2026-05-06)
  // raw / silver / mart 構造、詳細は g:/共有ドライブ/AI_reference/システム設計/売上分析アプリ_Phase3_Settlement_Report移行設計_20260505.md
  // tables は IF NOT EXISTS、views は DROP + CREATE で常に最新版を強制適用

  // ---- bronze: raw_amazon_settlement_headers / raw_amazon_settlement_lines ----
  db.exec(`CREATE TABLE IF NOT EXISTS raw_amazon_settlement_headers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    physical_line_hash TEXT NOT NULL UNIQUE,
    business_line_key  TEXT NOT NULL,
    source_document_id TEXT NOT NULL,
    source_file_hash   TEXT,
    source_path        TEXT,
    source_line_no     INTEGER,
    source_layer       TEXT NOT NULL,
    parser_version     TEXT NOT NULL,
    source_settlement_id  TEXT NOT NULL,
    settlement_start_date TEXT,
    settlement_end_date   TEXT,
    deposit_date          TEXT,
    total_amount_micro    INTEGER NOT NULL,
    currency              TEXT NOT NULL DEFAULT 'JPY',
    ingest_run_id TEXT, observed_at TEXT, ingested_at TEXT
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_headers_settlement ON raw_amazon_settlement_headers(source_settlement_id)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_headers_dedup ON raw_amazon_settlement_headers(source_settlement_id, business_line_key, source_layer, ingested_at)`);

  db.exec(`CREATE TABLE IF NOT EXISTS raw_amazon_settlement_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    physical_line_hash TEXT NOT NULL UNIQUE,
    business_line_key  TEXT NOT NULL,
    source_document_id TEXT NOT NULL,
    source_file_hash   TEXT,
    source_path        TEXT,
    source_line_no     INTEGER,
    source_layer       TEXT NOT NULL,
    parser_version     TEXT NOT NULL,
    source_settlement_id TEXT NOT NULL,
    posted_date_utc      TEXT NOT NULL,
    posted_datetime_jst  TEXT NOT NULL,
    economic_date        TEXT NOT NULL,
    year_month_int       INTEGER NOT NULL,
    amazon_order_id        TEXT,
    merchant_order_id      TEXT,
    shipment_id            TEXT,
    order_item_code        TEXT,
    adjustment_id          TEXT,
    seller_sku             TEXT,
    seller_sku_normalized  TEXT,
    transaction_type   TEXT NOT NULL,
    marketplace_name   TEXT,
    fulfillment_id     TEXT,
    quantity_purchased INTEGER,
    price_type                       TEXT,
    price_amount_micro               INTEGER,
    item_related_fee_type            TEXT,
    item_related_fee_amount_micro    INTEGER,
    promotion_id                     TEXT,
    promotion_type                   TEXT,
    promotion_amount_micro           INTEGER,
    shipment_fee_type                TEXT,
    shipment_fee_amount_micro        INTEGER,
    order_fee_type                   TEXT,
    order_fee_amount_micro           INTEGER,
    misc_fee_amount_micro            INTEGER,
    other_fee_amount_micro           INTEGER,
    other_fee_reason_description     TEXT,
    direct_payment_type              TEXT,
    direct_payment_amount_micro      INTEGER,
    other_amount_micro               INTEGER,
    currency                         TEXT NOT NULL DEFAULT 'JPY',
    ingest_run_id TEXT, observed_at TEXT, ingested_at TEXT
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_dedup       ON raw_amazon_settlement_lines(source_settlement_id, business_line_key, source_layer, ingested_at)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_month_sku   ON raw_amazon_settlement_lines(year_month_int, seller_sku_normalized)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_economic    ON raw_amazon_settlement_lines(economic_date)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_posted_utc  ON raw_amazon_settlement_lines(posted_date_utc)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_tx_price    ON raw_amazon_settlement_lines(transaction_type, price_type)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_tx_fee      ON raw_amazon_settlement_lines(transaction_type, item_related_fee_type)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_settlement  ON raw_amazon_settlement_lines(source_settlement_id)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_lines_order       ON raw_amazon_settlement_lines(amazon_order_id)`);

  // ---- dim: 自動 INSERT で蓄積 ----
  db.exec(`CREATE TABLE IF NOT EXISTS dim_amazon_transaction_type (
    transaction_type TEXT PRIMARY KEY,
    group_label TEXT, is_revenue_negative INTEGER, display_order INTEGER, description TEXT,
    observed_first_at TEXT, observed_last_at TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS dim_amazon_price_type (
    price_type TEXT PRIMARY KEY, group_label TEXT,
    observed_first_at TEXT, observed_last_at TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS dim_amazon_fee_type (
    fee_type TEXT PRIMARY KEY, fee_category TEXT, fee_subcategory TEXT,
    observed_first_at TEXT, observed_last_at TEXT
  )`);

  // ---- bridge: 販売月リステート用 ----
  db.exec(`CREATE TABLE IF NOT EXISTS bridge_amazon_order_sale_month (
    amazon_order_id        TEXT NOT NULL,
    order_item_code        TEXT NOT NULL,
    seller_sku_normalized  TEXT NOT NULL,
    original_year_month    INTEGER NOT NULL,
    original_economic_date TEXT NOT NULL,
    original_settlement_id TEXT NOT NULL,
    ingested_at            TEXT,
    PRIMARY KEY (amazon_order_id, order_item_code, seller_sku_normalized)
  ) WITHOUT ROWID`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_bridge_year_month ON bridge_amazon_order_sale_month(original_year_month)`);

  // ---- mart: long fact ----
  db.exec(`CREATE TABLE IF NOT EXISTS fact_amazon_settlement_monthly_long (
    year_month_int    INTEGER NOT NULL,
    seller_sku_normalized TEXT NOT NULL,
    transaction_type  TEXT NOT NULL,
    component_family  TEXT NOT NULL,
    component_type    TEXT NOT NULL,
    value_micro       INTEGER NOT NULL,
    row_count         INTEGER NOT NULL,
    source_layer      TEXT NOT NULL,
    generated_at      TEXT NOT NULL,
    PRIMARY KEY (year_month_int, seller_sku_normalized, transaction_type, component_family, component_type, source_layer)
  ) WITHOUT ROWID`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_long_sku_month ON fact_amazon_settlement_monthly_long(seller_sku_normalized, year_month_int)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_long_tx_family ON fact_amazon_settlement_monthly_long(transaction_type, component_family)`);

  // ---- mart: wide table (主要指標固定、INTEGER micro) ----
  db.exec(`CREATE TABLE IF NOT EXISTS fact_amazon_settlement_monthly_wide (
    year_month_int       INTEGER NOT NULL,
    seller_sku_normalized TEXT NOT NULL,
    qty_ordered                  INTEGER DEFAULT 0,
    qty_customer_refunded        INTEGER DEFAULT 0,
    qty_a_to_z_refund            INTEGER DEFAULT 0,
    qty_marketplace_guarantee    INTEGER DEFAULT 0,
    qty_warehouse_damage         INTEGER DEFAULT 0,
    qty_warehouse_lost           INTEGER DEFAULT 0,
    qty_reversal_reimbursement   INTEGER DEFAULT 0,
    qty_removal_completed        INTEGER DEFAULT 0,
    qty_safe_t                   INTEGER DEFAULT 0,
    qty_net_sold                 INTEGER DEFAULT 0,
    sales_principal_micro        INTEGER DEFAULT 0,
    sales_tax_micro              INTEGER DEFAULT 0,
    sales_shipping_micro         INTEGER DEFAULT 0,
    sales_giftwrap_micro         INTEGER DEFAULT 0,
    refund_principal_micro       INTEGER DEFAULT 0,
    refund_tax_micro             INTEGER DEFAULT 0,
    refund_shipping_micro        INTEGER DEFAULT 0,
    commission_micro             INTEGER DEFAULT 0,
    fba_fulfillment_micro        INTEGER DEFAULT 0,
    fba_storage_micro            INTEGER DEFAULT 0,
    closing_fee_micro            INTEGER DEFAULT 0,
    shipping_chargeback_micro    INTEGER DEFAULT 0,
    giftwrap_chargeback_micro    INTEGER DEFAULT 0,
    promotion_micro              INTEGER DEFAULT 0,
    warehouse_damage_micro       INTEGER DEFAULT 0,
    warehouse_lost_micro         INTEGER DEFAULT 0,
    reversal_reimbursement_micro INTEGER DEFAULT 0,
    safe_t_micro                 INTEGER DEFAULT 0,
    fee_adjustment_micro         INTEGER DEFAULT 0,
    source_layer_summary         TEXT,
    raw_row_count                INTEGER DEFAULT 0,
    generated_at                 TEXT NOT NULL,
    PRIMARY KEY (year_month_int, seller_sku_normalized)
  ) WITHOUT ROWID`);

  // ---- refresh queue ----
  db.exec(`CREATE TABLE IF NOT EXISTS settlement_refresh_queue (
    year_month_int INTEGER PRIMARY KEY,
    reason         TEXT,
    enqueued_at    TEXT NOT NULL,
    processed_at   TEXT
  )`);

  // ---- Phase 1 #1-7a: job_locks (concurrency guard)
  // daily-sync / mart rebuild / sync の重複起動防止
  // - acquire 時に expires_at < now() なら takeover 可
  // - heartbeat で expires_at を延長
  // - release で DELETE
  // helper: apps/warehouse/job-locks.js
  db.exec(`CREATE TABLE IF NOT EXISTS job_locks (
    job_name      TEXT PRIMARY KEY,
    holder_id     TEXT NOT NULL,
    acquired_at   TEXT NOT NULL,
    expires_at    TEXT NOT NULL,
    heartbeat_at  TEXT NOT NULL
  )`);

  // ---- Phase 1 #1-8a: DQ gate / anomaly alert
  // monthly validation の差分を 7 bucket に分類して永続化、6 check で品質ゲート
  // severity='error' の breach で GChat 通知 (Phase 1.x で組み込み)
  // runner: apps/warehouse/run-amazon-finance-dq.js
  db.exec(`CREATE TABLE IF NOT EXISTS dq_run_results (
    run_id          TEXT NOT NULL,
    check_name      TEXT NOT NULL,
    severity        TEXT NOT NULL CHECK (severity IN ('info', 'warn', 'error')),
    actual_value    REAL,
    threshold_value REAL,
    details_json    TEXT,
    checked_at      TEXT NOT NULL,
    PRIMARY KEY (run_id, check_name)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dq_run_results_severity
    ON dq_run_results (severity, checked_at)`);

  // ---- Phase 1 #1-4: sync_contracts (entity-driven sync registry)
  // 各 entity (mirror テーブル) ごとに contract version / source / target /
  // grain / key 列 / payload schema / clear strategy / apply mode を明示。
  // sync-amazon-finance-daily.js (Phase 1) / 既存 sync-to-render.js (将来 refactor) から参照。
  db.exec(`CREATE TABLE IF NOT EXISTS sync_contracts (
    entity                TEXT PRIMARY KEY,
    contract_version      INTEGER NOT NULL,
    source_system         TEXT NOT NULL,
    source_object         TEXT NOT NULL,
    target_table          TEXT NOT NULL,
    grain_definition      TEXT NOT NULL,
    key_columns_json      TEXT NOT NULL,
    payload_schema_json   TEXT NOT NULL,
    clear_strategy        TEXT NOT NULL,
    apply_mode            TEXT NOT NULL,
    enabled               INTEGER NOT NULL DEFAULT 1,
    owner                 TEXT NOT NULL,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
  )`);

  // ---- LINEギフト Phase 1 A-3: contract auto-seed (Codex R3 critical + R4 critical 反映)
  // 将来別環境デプロイ時の手動 seed 漏れを防止。INSERT ... ON CONFLICT DO UPDATE (= UPSERT) で idempotent。
  // ⚠️ INSERT OR REPLACE は SQLite で実は DELETE+INSERT になり、sync_runs.entity の外部キー違反で
  //   initDB() が落ちる (sync_runs に履歴がある状態で 2回目以降の起動で発生、R4 critical 反映)。
  // 既存の手動 seed SQL (sql/sync/seed_linegift_finance_sku_daily.sql) と完全同内容。
  // TODO (将来別 PR): Amazon / 楽天 / Yahoo / au PAY も同様の auto-seed に統一する
  db.exec(`
    INSERT INTO sync_contracts (
      entity, contract_version, source_system, source_object, target_table,
      grain_definition, key_columns_json, payload_schema_json,
      clear_strategy, apply_mode, enabled, owner, created_at, updated_at
    ) VALUES (
      'linegift_finance_sku_daily', 1, 'minipc-warehouse',
      'f_linegift_finance_sku_daily_v1', 'mirror_linegift_finance_sku_daily',
      'one row = one (date_jst, sku_code) — sku_code = variation.code (LOWER(TRIM())、LINEギフト の variant 主キー、100% master_match 想定)',
      '["date_jst","sku_code"]',
      '{"required":["date_jst","sku_code"],"date_jst_pattern":"^\\d{4}-\\d{2}-\\d{2}$","cost_status_enum":["complete","missing_cost","partial_cost","late_bound_after_close"],"resolution_method_enum":["master_match","parent_match","unresolved"],"shipping_quality_enum":["no_shipping_in_api","actual_api","estimated_rates","estimated_fallback","missing"],"margin_confidence_enum":["provisional_full_candidate","full_minus_returns","full"],"mall_fee_calc_method_enum":["actual_api","actual_statement","estimated_rate","unknown"]}',
      'scope_clear_per_run', 'insert_or_replace', 1, 'phase1-linegift-finance',
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(entity) DO UPDATE SET
      contract_version    = excluded.contract_version,
      source_system       = excluded.source_system,
      source_object       = excluded.source_object,
      target_table        = excluded.target_table,
      grain_definition    = excluded.grain_definition,
      key_columns_json    = excluded.key_columns_json,
      payload_schema_json = excluded.payload_schema_json,
      clear_strategy      = excluded.clear_strategy,
      apply_mode          = excluded.apply_mode,
      enabled             = excluded.enabled,
      owner               = excluded.owner,
      -- created_at は既存維持 (UPSERT 側で触らない)、updated_at のみ毎回更新
      updated_at          = excluded.updated_at
  `);

  // ---- biz-ops-overview f_sales_by_listing: contract auto-seed (LINEギフト/Qoo10 同型、2026-05-19 PR #155 patch v2)
  // 業務目線の真の売上 (API 直接集計、税込、全モール統合) を Render mirror に sync
  // Phase 1 fact (粗利分析向け) とは別系統、status filter/SKU 解決/按分なし
  db.exec(`
    INSERT INTO sync_contracts (
      entity, contract_version, source_system, source_object, target_table,
      grain_definition, key_columns_json, payload_schema_json,
      clear_strategy, apply_mode, enabled, owner, created_at, updated_at
    ) VALUES (
      'f_sales_by_listing', 1, 'minipc-warehouse',
      'f_sales_by_listing', 'mirror_f_sales_by_listing',
      'one row = one (日付, モール, モール商品コード, チャネル) — 全モール (Amazon/楽天/Yahoo/au PAY/LINEギフト/Qoo10/メルカリ) 統合、API 直接集計の業務目線真の売上',
      '["date","mall","item_code","channel"]',
      '{"required":["date","mall","item_code"],"date_pattern":"^\\d{4}-\\d{2}-\\d{2}$"}',
      'scope_clear_per_run', 'insert_or_replace', 1, 'biz-ops-overview',
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(entity) DO UPDATE SET
      contract_version    = excluded.contract_version,
      source_system       = excluded.source_system,
      source_object       = excluded.source_object,
      target_table        = excluded.target_table,
      grain_definition    = excluded.grain_definition,
      key_columns_json    = excluded.key_columns_json,
      payload_schema_json = excluded.payload_schema_json,
      clear_strategy      = excluded.clear_strategy,
      apply_mode          = excluded.apply_mode,
      enabled             = excluded.enabled,
      owner               = excluded.owner,
      updated_at          = excluded.updated_at
  `);

  // ---- Qoo10 Phase 1 A-3: contract auto-seed (LINEギフト同型パターン、2026-05-19)
  // sku_code は 3 段 fallback (combined / option_only / seller_only) 後の解決値 or '__UNRESOLVED__:%'
  // resolution_method は 2 値 ('master_match'/'unresolved')、match_tier 列で 4 値内訳監視
  db.exec(`
    INSERT INTO sync_contracts (
      entity, contract_version, source_system, source_object, target_table,
      grain_definition, key_columns_json, payload_schema_json,
      clear_strategy, apply_mode, enabled, owner, created_at, updated_at
    ) VALUES (
      'qoo10_finance_sku_daily', 1, 'minipc-warehouse',
      'f_qoo10_finance_sku_daily_v1', 'mirror_qoo10_finance_sku_daily',
      'one row = one (date_jst, sku_code) — sku_code = 3 段 fallback (combined/option_only/seller_only) 後の解決値、unresolved は __UNRESOLVED__:%',
      '["date_jst","sku_code"]',
      '{"required":["date_jst","sku_code"],"date_jst_pattern":"^\\d{4}-\\d{2}-\\d{2}$","cost_status_enum":["complete","missing_cost","partial_cost","late_bound_after_close"],"resolution_method_enum":["master_match","unresolved"],"match_tier_enum":["combined","option_only","seller_only","unresolved"],"shipping_quality_enum":["no_shipping_in_api","actual_api","estimated_rates","estimated_fallback","missing"],"margin_confidence_enum":["partial_pending_settlement_csv","partial_minus_returns","full","partial_legacy_fields_missing"],"mall_fee_calc_method_enum":["actual_api","actual_statement","estimated_rate","unknown"],"settle_price_formula_scope_enum":["domestic_non_cod","oversea","cod","mixed"],"shop_promo_burden_status_enum":["pending_settlement_csv","actual_statement","not_applicable"]}',
      'scope_clear_per_run', 'insert_or_replace', 1, 'phase1-qoo10-finance',
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(entity) DO UPDATE SET
      contract_version    = excluded.contract_version,
      source_system       = excluded.source_system,
      source_object       = excluded.source_object,
      target_table        = excluded.target_table,
      grain_definition    = excluded.grain_definition,
      key_columns_json    = excluded.key_columns_json,
      payload_schema_json = excluded.payload_schema_json,
      clear_strategy      = excluded.clear_strategy,
      apply_mode          = excluded.apply_mode,
      enabled             = excluded.enabled,
      owner               = excluded.owner,
      updated_at          = excluded.updated_at
  `);

  // ---- Amazon 広告費 mirror sync: contract auto-seed (amazon-dashboard PR-A、2026-07-06)
  // fact_ad_spend (SKU別) / fact_ad_spend_campaign (キャンペーン単位) を Render mirror へ sync。
  // 広告レポートは attribution 遡及で過去日の値が更新されるため scope_clear_per_run。
  // payload は英語キー (mirror 側英語列、f_sales_by_listing PR #156 と同方針)。
  db.exec(`
    INSERT INTO sync_contracts (
      entity, contract_version, source_system, source_object, target_table,
      grain_definition, key_columns_json, payload_schema_json,
      clear_strategy, apply_mode, enabled, owner, created_at, updated_at
    ) VALUES (
      'amazon_ads_sku_daily', 1, 'minipc-warehouse',
      'fact_ad_spend', 'mirror_amazon_ads_sku_daily',
      'one row = one (date_jst, mall, campaign_id, ad_type, target, target_granularity) — spAdvertisedProduct 由来の SKU/ASIN 別広告費 (Auto-Targeting 未配賦分は含まない)',
      '["date_jst","mall","campaign_id","ad_type","target","target_granularity"]',
      '{"required":["date_jst","mall","campaign_id","ad_type","target","target_granularity"],"date_jst_pattern":"^\\d{4}-\\d{2}-\\d{2}$","target_granularity_enum":["sku","asin"]}',
      'scope_clear_per_run', 'insert_or_replace', 1, 'amazon-dashboard',
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(entity) DO UPDATE SET
      contract_version    = excluded.contract_version,
      source_system       = excluded.source_system,
      source_object       = excluded.source_object,
      target_table        = excluded.target_table,
      grain_definition    = excluded.grain_definition,
      key_columns_json    = excluded.key_columns_json,
      payload_schema_json = excluded.payload_schema_json,
      clear_strategy      = excluded.clear_strategy,
      apply_mode          = excluded.apply_mode,
      enabled             = excluded.enabled,
      owner               = excluded.owner,
      updated_at          = excluded.updated_at
  `);

  db.exec(`
    INSERT INTO sync_contracts (
      entity, contract_version, source_system, source_object, target_table,
      grain_definition, key_columns_json, payload_schema_json,
      clear_strategy, apply_mode, enabled, owner, created_at, updated_at
    ) VALUES (
      'amazon_ads_campaign_daily', 1, 'minipc-warehouse',
      'fact_ad_spend_campaign', 'mirror_amazon_ads_campaign_daily',
      'one row = one (date_jst, mall, campaign_id, ad_type) — spCampaigns 由来のキャンペーン単位全広告費 (Amazon Ads Console と一致する正本)',
      '["date_jst","mall","campaign_id","ad_type"]',
      '{"required":["date_jst","mall","campaign_id","ad_type"],"date_jst_pattern":"^\\d{4}-\\d{2}-\\d{2}$"}',
      'scope_clear_per_run', 'insert_or_replace', 1, 'amazon-dashboard',
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(entity) DO UPDATE SET
      contract_version    = excluded.contract_version,
      source_system       = excluded.source_system,
      source_object       = excluded.source_object,
      target_table        = excluded.target_table,
      grain_definition    = excluded.grain_definition,
      key_columns_json    = excluded.key_columns_json,
      payload_schema_json = excluded.payload_schema_json,
      clear_strategy      = excluded.clear_strategy,
      apply_mode          = excluded.apply_mode,
      enabled             = excluded.enabled,
      owner               = excluded.owner,
      updated_at          = excluded.updated_at
  `);

  // ---- Amazon アカウント単位フィー月次: contract auto-seed (amazon-dashboard PR-C、2026-07-06)
  // SKU に紐付かない保管料/長期在庫追加手数料/返送等。grain は月次だが clear 機構と
  // 整合させるため key は月初日 (date_jst = YYYY-MM-01)。金額は Amazon 符号のまま (負=費用)。
  db.exec(`
    INSERT INTO sync_contracts (
      entity, contract_version, source_system, source_object, target_table,
      grain_definition, key_columns_json, payload_schema_json,
      clear_strategy, apply_mode, enabled, owner, created_at, updated_at
    ) VALUES (
      'amazon_account_fees_monthly', 1, 'minipc-warehouse',
      'f_amazon_account_fees_monthly_v1', 'mirror_amazon_account_fees_monthly',
      'one row = one (date_jst = month start YYYY-MM-01, fee_type) — SKU 無し settlement 行のアカウント単位フィー月次 net',
      '["date_jst","fee_type"]',
      '{"required":["date_jst","fee_type"],"date_jst_pattern":"^\\d{4}-\\d{2}-01$","fee_type_enum":["storage","long_term_storage","removal","inbound_defect","low_inventory","subscription","other_account_fee"]}',
      'scope_clear_per_run', 'insert_or_replace', 1, 'amazon-dashboard',
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(entity) DO UPDATE SET
      contract_version    = excluded.contract_version,
      source_system       = excluded.source_system,
      source_object       = excluded.source_object,
      target_table        = excluded.target_table,
      grain_definition    = excluded.grain_definition,
      key_columns_json    = excluded.key_columns_json,
      payload_schema_json = excluded.payload_schema_json,
      clear_strategy      = excluded.clear_strategy,
      apply_mode          = excluded.apply_mode,
      enabled             = excluded.enabled,
      owner               = excluded.owner,
      updated_at          = excluded.updated_at
  `);

  // ---- Phase 1 #1-4a: sync_runs (run ledger、miniPC 側で sync 開始記録)
  // status 遷移: started → applied (全 chunk Render から 2xx)
  //              | → failed (途中失敗、error_message に記録)
  //              | → backed_out (backout-sync-run.js で逆操作)
  // replay_of_run_id: 旧 run の replay の場合、起動時に env REPLAY_OF_RUN_ID から INSERT
  db.exec(`CREATE TABLE IF NOT EXISTS sync_runs (
    run_id                    TEXT PRIMARY KEY,
    entity                    TEXT NOT NULL,
    contract_version          INTEGER NOT NULL,
    source_host               TEXT NOT NULL,
    scope_from                TEXT NOT NULL CHECK(scope_from GLOB '????-??-??'),
    scope_to                  TEXT NOT NULL CHECK(scope_to   GLOB '????-??-??'),
    chunk_count_expected      INTEGER NOT NULL,
    chunk_count_received      INTEGER NOT NULL DEFAULT 0,
    row_count_received        INTEGER NOT NULL DEFAULT 0,
    payload_checksum_manifest TEXT,
    status                    TEXT NOT NULL CHECK (
      status IN ('started','applied','failed','backed_out')
    ),
    replay_of_run_id          TEXT,
    error_message             TEXT,
    started_at                TEXT NOT NULL,
    completed_at              TEXT,
    applied_at                TEXT,
    FOREIGN KEY (entity) REFERENCES sync_contracts(entity)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sync_runs_entity_status ON sync_runs(entity, status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(started_at)');

  db.exec(`CREATE TABLE IF NOT EXISTS accounting_diff_buckets (
    run_id        TEXT NOT NULL,
    month_jst     TEXT NOT NULL,
    seller_sku    TEXT NOT NULL,
    asin_norm     TEXT NOT NULL DEFAULT '',
    bucket_code   TEXT NOT NULL,
    bucket_amount REAL NOT NULL,
    row_count     INTEGER NOT NULL,
    details_json  TEXT,
    created_at    TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_acct_diff_buckets_run
    ON accounting_diff_buckets (run_id, bucket_code)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_acct_diff_buckets_month
    ON accounting_diff_buckets (month_jst, bucket_code)`);

  // ---- Phase 3.4: fact_ad_spend_campaign (spCampaigns 経由、全広告費取得)
  // 既存 fact_ad_spend (spAdvertisedProduct 経由、SKU 別) は 47% 漏れ (Auto-Targeting unallocated)
  // 本テーブルで campaign 単位の全広告費を保持、月次集計時に「全 - SKU 別 = unallocated」で導出可
  db.exec(`CREATE TABLE IF NOT EXISTS fact_ad_spend_campaign (
    日付          TEXT NOT NULL,
    モール        TEXT NOT NULL,
    キャンペーンID TEXT NOT NULL,
    キャンペーン名 TEXT,
    広告タイプ    TEXT NOT NULL DEFAULT 'SP',
    キャンペーンステータス TEXT,
    クリック数        INTEGER DEFAULT 0,
    インプレッション  INTEGER DEFAULT 0,
    広告費            REAL DEFAULT 0,
    広告経由売上_1d   REAL DEFAULT 0,
    広告経由売上_7d   REAL DEFAULT 0,
    広告経由売上_14d  REAL DEFAULT 0,
    広告経由売上_30d  REAL DEFAULT 0,
    広告経由数量_1d   INTEGER DEFAULT 0,
    ingested_at   TEXT NOT NULL,
    PRIMARY KEY (日付, モール, キャンペーンID, 広告タイプ)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fas_campaign_date ON fact_ad_spend_campaign(日付)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fas_campaign_mall ON fact_ad_spend_campaign(モール, 日付)`);

  // ---- silver view (always recreate) ----
  db.exec(`DROP VIEW IF EXISTS v_amazon_settlement_unified`);
  db.exec(`CREATE VIEW v_amazon_settlement_unified AS
    WITH dedup AS (
      SELECT
        l.*,
        ROW_NUMBER() OVER (
          PARTITION BY l.source_settlement_id, l.business_line_key
          ORDER BY CASE l.source_layer
                     WHEN 'sp_api_v1' THEN 1
                     WHEN 'manual_csv' THEN 2
                     ELSE 3
                   END,
                   l.ingested_at DESC
        ) AS rn
      FROM raw_amazon_settlement_lines l
    )
    SELECT
      d.id, d.physical_line_hash, d.business_line_key,
      d.source_document_id, d.source_file_hash, d.source_path, d.source_line_no,
      d.source_layer, d.parser_version, d.source_settlement_id,
      d.posted_date_utc, d.posted_datetime_jst, d.economic_date, d.year_month_int,
      d.amazon_order_id, d.merchant_order_id, d.shipment_id, d.order_item_code, d.adjustment_id,
      d.seller_sku, d.seller_sku_normalized,
      d.transaction_type, d.marketplace_name, d.fulfillment_id,
      d.quantity_purchased,
      d.price_type, d.price_amount_micro,
      d.item_related_fee_type, d.item_related_fee_amount_micro,
      d.promotion_id, d.promotion_type, d.promotion_amount_micro,
      d.shipment_fee_type, d.shipment_fee_amount_micro,
      d.order_fee_type, d.order_fee_amount_micro,
      d.misc_fee_amount_micro, d.other_fee_amount_micro, d.other_fee_reason_description,
      d.direct_payment_type, d.direct_payment_amount_micro, d.other_amount_micro,
      d.currency, d.ingest_run_id, d.observed_at, d.ingested_at,
      CASE
        WHEN d.transaction_type = 'Order' AND d.quantity_purchased IS NOT NULL THEN 'exact'
        ELSE 'mixed'
      END AS recognition_basis,
      'settled' AS settlement_status,
      d.source_layer AS canonical_source,
      COALESCE(dt.group_label, 'unknown') AS tx_group_label,
      COALESCE(dt.is_revenue_negative, 0) AS tx_is_revenue_negative
    FROM dedup d
    LEFT JOIN dim_amazon_transaction_type dt ON dt.transaction_type = d.transaction_type
    WHERE d.rn = 1
  `);

  // ---- v4 mart view (Phase 3.1.x、符号バグ修正済 + 広告費 + cogs JOIN) ----
  db.exec(`DROP VIEW IF EXISTS v_amazon_sku_profit_actual_v4`);
  db.exec(`CREATE VIEW v_amazon_sku_profit_actual_v4 AS
    WITH cogs_by_sku AS (
      SELECT
        seller_sku,
        SUM(単価 * 数量) AS unit_cogs_excl_tax,
        MAX(単価) AS one_unit_cost,
        MAX(数量) AS components_qty
      FROM v_sku_costed
      GROUP BY seller_sku
    )
    SELECT
      w.year_month_int,
      printf('%d-%02d', w.year_month_int / 100, w.year_month_int % 100) AS year_month,
      w.seller_sku_normalized AS seller_sku,
      w.qty_ordered, w.qty_customer_refunded, w.qty_marketplace_guarantee, w.qty_a_to_z_refund, w.qty_net_sold,
      w.sales_principal_micro / 1000000.0 AS sales_principal_jpy,
      w.sales_shipping_micro / 1000000.0 AS sales_shipping_jpy,
      w.sales_giftwrap_micro / 1000000.0 AS sales_giftwrap_jpy,
      w.sales_tax_micro / 1000000.0 AS sales_tax_jpy,
      w.commission_micro / 1000000.0 AS commission_jpy,
      w.fba_fulfillment_micro / 1000000.0 AS fba_fulfillment_jpy,
      w.fba_storage_micro / 1000000.0 AS fba_storage_jpy,
      w.closing_fee_micro / 1000000.0 AS closing_fee_jpy,
      w.shipping_chargeback_micro / 1000000.0 AS shipping_chargeback_jpy,
      w.giftwrap_chargeback_micro / 1000000.0 AS giftwrap_chargeback_jpy,
      w.promotion_micro / 1000000.0 AS promotion_jpy,
      w.warehouse_damage_micro / 1000000.0 AS warehouse_damage_jpy,
      w.warehouse_lost_micro / 1000000.0 AS warehouse_lost_jpy,
      w.safe_t_micro / 1000000.0 AS safe_t_jpy,
      w.refund_principal_micro / 1000000.0 AS refund_principal_jpy,
      w.reversal_reimbursement_micro / 1000000.0 AS reversal_reimbursement_jpy,
      COALESCE(ad.ad_cost, 0) AS ad_cost_jpy,
      COALESCE(ad.ad_attributed_sales, 0) AS ad_attributed_sales_jpy,
      COALESCE(ad_total.ad_cost_total, 0) AS ad_cost_total_month_jpy,
      c.one_unit_cost AS unit_cost_excl_tax,
      c.components_qty AS sku_components_qty,
      COALESCE(c.unit_cogs_excl_tax * w.qty_ordered, 0) AS cogs_excl_tax,
      -- 真の売上 = sales_principal + sales_shipping (買い手送料収入) + sales_giftwrap (ギフト料金収入)
      -- 中原さん 2026-05-07 指摘: 配送料・ギフト包装手数料は seller の収入なので売上に含める
      (w.sales_principal_micro / 1000000.0)
        + (w.sales_shipping_micro / 1000000.0)
        + (w.sales_giftwrap_micro / 1000000.0)
        - COALESCE(c.unit_cogs_excl_tax * w.qty_ordered, 0)
        + (w.commission_micro / 1000000.0)
        + (w.fba_fulfillment_micro / 1000000.0)
        + (w.fba_storage_micro / 1000000.0)
        + (w.closing_fee_micro / 1000000.0)
        + (w.shipping_chargeback_micro / 1000000.0)
        + (w.giftwrap_chargeback_micro / 1000000.0)
        + (w.promotion_micro / 1000000.0)
        AS gross_margin_excl_tax,
      (w.sales_principal_micro / 1000000.0)
        + (w.sales_shipping_micro / 1000000.0)
        + (w.sales_giftwrap_micro / 1000000.0)
        - COALESCE(c.unit_cogs_excl_tax * w.qty_ordered, 0)
        + (w.commission_micro / 1000000.0)
        + (w.fba_fulfillment_micro / 1000000.0)
        + (w.fba_storage_micro / 1000000.0)
        + (w.closing_fee_micro / 1000000.0)
        + (w.shipping_chargeback_micro / 1000000.0)
        + (w.giftwrap_chargeback_micro / 1000000.0)
        + (w.promotion_micro / 1000000.0)
        - COALESCE(ad.ad_cost, 0)
        AS contribution_margin_excl_tax,
      -- 中原さん 2026-05-08 指摘: 広告費引く前 + warehouse 補填 = 経営層が頭で見ている利益率
      -- gross_margin_excl_tax (広告費・補填なし) と settlement_margin_excl_tax (広告費引く + 補填あり) の中間
      (w.sales_principal_micro / 1000000.0)
        + (w.sales_shipping_micro / 1000000.0)
        + (w.sales_giftwrap_micro / 1000000.0)
        - COALESCE(c.unit_cogs_excl_tax * w.qty_ordered, 0)
        + (w.commission_micro / 1000000.0)
        + (w.fba_fulfillment_micro / 1000000.0)
        + (w.fba_storage_micro / 1000000.0)
        + (w.closing_fee_micro / 1000000.0)
        + (w.shipping_chargeback_micro / 1000000.0)
        + (w.giftwrap_chargeback_micro / 1000000.0)
        + (w.promotion_micro / 1000000.0)
        + (w.warehouse_damage_micro / 1000000.0)
        + (w.warehouse_lost_micro / 1000000.0)
        + (w.safe_t_micro / 1000000.0)
        + (w.refund_principal_micro / 1000000.0)
        + (w.reversal_reimbursement_micro / 1000000.0)
        AS gross_margin_with_reimbursement_excl_tax,
      (w.sales_principal_micro / 1000000.0)
        + (w.sales_shipping_micro / 1000000.0)
        + (w.sales_giftwrap_micro / 1000000.0)
        - COALESCE(c.unit_cogs_excl_tax * w.qty_ordered, 0)
        + (w.commission_micro / 1000000.0)
        + (w.fba_fulfillment_micro / 1000000.0)
        + (w.fba_storage_micro / 1000000.0)
        + (w.closing_fee_micro / 1000000.0)
        + (w.shipping_chargeback_micro / 1000000.0)
        + (w.giftwrap_chargeback_micro / 1000000.0)
        + (w.promotion_micro / 1000000.0)
        - COALESCE(ad.ad_cost, 0)
        + (w.warehouse_damage_micro / 1000000.0)
        + (w.warehouse_lost_micro / 1000000.0)
        + (w.safe_t_micro / 1000000.0)
        + (w.refund_principal_micro / 1000000.0)
        + (w.reversal_reimbursement_micro / 1000000.0)
        AS settlement_margin_excl_tax,
      w.source_layer_summary,
      p.商品名 AS product_name
    FROM fact_amazon_settlement_monthly_wide w
    LEFT JOIN cogs_by_sku c ON c.seller_sku = w.seller_sku_normalized
    LEFT JOIN m_products p ON p.商品コード = w.seller_sku_normalized
    LEFT JOIN (
      SELECT
        CAST(strftime('%Y%m', 日付) AS INTEGER) AS year_month_int,
        LOWER(ターゲット) AS seller_sku,
        SUM(広告費) AS ad_cost,
        SUM(広告経由売上) AS ad_attributed_sales
      FROM fact_ad_spend
      WHERE モール = 'amazon' AND ターゲット粒度 IN ('asin', 'sku')
      GROUP BY year_month_int, LOWER(ターゲット)
    ) ad ON ad.year_month_int = w.year_month_int AND ad.seller_sku = w.seller_sku_normalized
    LEFT JOIN (
      SELECT
        CAST(strftime('%Y%m', 日付) AS INTEGER) AS year_month_int,
        SUM(広告費) AS ad_cost_total
      FROM fact_ad_spend_campaign
      WHERE モール = 'amazon'
      GROUP BY year_month_int
    ) ad_total ON ad_total.year_month_int = w.year_month_int
  `);

  // ---- Phase 3.4: 月次集計 view ----
  db.exec(`DROP VIEW IF EXISTS v_amazon_monthly_full_summary`);
  db.exec(`CREATE VIEW v_amazon_monthly_full_summary AS
    WITH sku_agg AS (
      SELECT
        year_month_int,
        SUM(qty_ordered) AS total_qty_ordered,
        SUM(qty_net_sold) AS total_qty_net_sold,
        SUM(sales_principal_jpy + COALESCE(sales_shipping_jpy, 0) + COALESCE(sales_giftwrap_jpy, 0)) AS total_sales_jpy,
        SUM(sales_principal_jpy) AS total_sales_principal_only_jpy,
        SUM(COALESCE(sales_shipping_jpy, 0)) AS total_sales_shipping_jpy,
        SUM(COALESCE(sales_giftwrap_jpy, 0)) AS total_sales_giftwrap_jpy,
        SUM(cogs_excl_tax) AS total_cogs_jpy,
        SUM(commission_jpy) AS total_commission_jpy,
        SUM(fba_fulfillment_jpy) AS total_fba_fulfillment_jpy,
        SUM(promotion_jpy) AS total_promotion_jpy,
        SUM(gross_margin_excl_tax) AS total_gross_margin_jpy,
        SUM(ad_cost_jpy) AS total_ad_cost_sku_jpy,
        SUM(contribution_margin_excl_tax) AS total_contribution_margin_partial_jpy,
        MAX(ad_cost_total_month_jpy) AS ad_cost_total_campaign_jpy
      FROM v_amazon_sku_profit_actual_v4
      GROUP BY year_month_int
    )
    SELECT
      year_month_int,
      printf('%d-%02d', year_month_int / 100, year_month_int % 100) AS year_month,
      total_qty_ordered, total_qty_net_sold,
      total_sales_jpy, total_cogs_jpy,
      total_commission_jpy, total_fba_fulfillment_jpy, total_promotion_jpy,
      total_gross_margin_jpy,
      total_ad_cost_sku_jpy,
      ad_cost_total_campaign_jpy,
      (ad_cost_total_campaign_jpy - total_ad_cost_sku_jpy) AS ad_cost_unallocated_jpy,
      total_contribution_margin_partial_jpy AS contribution_margin_sku_only_jpy,
      (total_contribution_margin_partial_jpy - (ad_cost_total_campaign_jpy - total_ad_cost_sku_jpy))
        AS contribution_margin_full_jpy,
      ROUND(total_gross_margin_jpy * 100.0 / NULLIF(total_sales_jpy, 0), 1) AS gross_margin_pct,
      ROUND((total_contribution_margin_partial_jpy - (ad_cost_total_campaign_jpy - total_ad_cost_sku_jpy)) * 100.0 / NULLIF(total_sales_jpy, 0), 1) AS contribution_margin_full_pct
    FROM sku_agg
  `);

  // ---- 切替判定 view (v3 vs v4) ----
  db.exec(`DROP VIEW IF EXISTS v_settlement_v3_v4_validation`);
  db.exec(`CREATE VIEW v_settlement_v3_v4_validation AS
    WITH v3_monthly AS (
      SELECT LOWER(TRIM(モール商品コード)) AS seller_sku_normalized,
             CAST(strftime('%Y%m', 日付) AS INTEGER) AS year_month_int,
             SUM(数量) AS qty_v3,
             SUM(原価_税込)/1.1 AS cost_excl_v3,
             SUM(商品売上)/1.1 AS sales_excl_v3
      FROM v_amazon_sku_profit_actual
      GROUP BY LOWER(TRIM(モール商品コード)), year_month_int
    ),
    v4_monthly AS (
      SELECT seller_sku_normalized, year_month_int,
             qty_net_sold AS qty_v4,
             (sales_principal_micro / 1000000.0) AS sales_v4
      FROM fact_amazon_settlement_monthly_wide
    )
    SELECT
      COALESCE(v4.year_month_int, v3.year_month_int) AS year_month_int,
      COALESCE(v4.seller_sku_normalized, v3.seller_sku_normalized) AS seller_sku_normalized,
      v3.qty_v3, v4.qty_v4,
      COALESCE(v4.qty_v4, 0) - COALESCE(v3.qty_v3, 0) AS qty_delta,
      v3.sales_excl_v3, v4.sales_v4,
      COALESCE(v4.sales_v4, 0) - COALESCE(v3.sales_excl_v3, 0) AS sales_delta,
      CASE
        WHEN ABS(COALESCE(v4.qty_v4, 0) - COALESCE(v3.qty_v3, 0)) <= MAX(1, CAST(ABS(COALESCE(v3.qty_v3, 0)) * 0.001 AS INTEGER))
        THEN 1 ELSE 0
      END AS qty_validated_flag
    FROM v4_monthly v4
    LEFT JOIN v3_monthly v3
      ON v4.seller_sku_normalized = v3.seller_sku_normalized
      AND v4.year_month_int = v3.year_month_int
  `);

  // ---- m_products trigger 廃止 (差分バッチ record-m-products-history.js に置き換え)
  // 旧 trigger が残っていると rebuild-m-products.js の DELETE+INSERT 全件で
  // m_products_history に全件 noise が入るため、initDB で必ず DROP (idempotent)
  // (Codex round 3 指摘: trigger 削除箇所が record-m-products-history.js 内のみだと
  //  rebuild-m-products.js → record-m-products-history.js の順では初回 cron で間に合わない)
  db.exec(`DROP TRIGGER IF EXISTS trg_m_products_after_insert`);
  db.exec(`DROP TRIGGER IF EXISTS trg_m_products_after_update`);
  db.exec(`DROP TRIGGER IF EXISTS trg_m_products_after_delete`);

  // ---- 監査 view ----
  db.exec(`DROP VIEW IF EXISTS v_settlement_unknown_dims`);
  db.exec(`CREATE VIEW v_settlement_unknown_dims AS
    SELECT 'transaction_type' AS dim_name, l.transaction_type AS dim_value, COUNT(*) AS row_count
    FROM raw_amazon_settlement_lines l
    LEFT JOIN dim_amazon_transaction_type d ON d.transaction_type = l.transaction_type
    WHERE d.group_label IS NULL OR d.transaction_type IS NULL
    GROUP BY l.transaction_type
    UNION ALL
    SELECT 'price_type', l.price_type, COUNT(*)
    FROM raw_amazon_settlement_lines l
    LEFT JOIN dim_amazon_price_type d ON d.price_type = l.price_type
    WHERE l.price_type IS NOT NULL AND (d.group_label IS NULL OR d.price_type IS NULL)
    GROUP BY l.price_type
    UNION ALL
    SELECT 'fee_type', l.item_related_fee_type, COUNT(*)
    FROM raw_amazon_settlement_lines l
    LEFT JOIN dim_amazon_fee_type d ON d.fee_type = l.item_related_fee_type
    WHERE l.item_related_fee_type IS NOT NULL AND (d.fee_category IS NULL OR d.fee_type IS NULL)
    GROUP BY l.item_related_fee_type
  `);

  // ---- biz-ops-overview: 全モール売上日次統合 view (2026-05-19、PR #155 で f_sales_by_listing 経由に変更)
  // 用途: miniPC GChat 売上サマリ通知 (notify-sales-summary.js)
  // 旧 (PR #153): Phase 1 fact (f_*_finance_sku_daily_v1) 参照 → status filter + SKU 解決 + 按分で
  //   業務目線の真の売上と 60%+ 乖離する不適切実装 (5/18 全合計 ¥1.27M vs 真値 ¥3.30M)。
  // 新: f_sales_by_listing 参照 = 全モール API 直接、税込、業務目線の真の売上、メルカリも含まれる
  // Render 側 (warehouse-mirror/db.js) は mirror_f_sales_by_listing 経由
  db.exec('DROP VIEW IF EXISTS v_mall_sales_daily_unified');
  db.exec(`CREATE VIEW v_mall_sales_daily_unified AS
    SELECT
      モール AS mall,
      日付 AS date_jst,
      売上金額 AS sales_gross_jpy_incl,
      数量 AS units_net_sold,
      updated_at AS data_synced_at,
      'f_sales_by_listing' AS source_fact
    FROM f_sales_by_listing`);
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
  const tables = ['raw_ne_products', 'raw_ne_orders', 'raw_ne_set_products', 'raw_sp_orders', 'raw_sp_orders_log', 'raw_rakuten_orders', 'raw_rakuten_orders_log', 'raw_lz_inventory', 'product_shipping', 'shipping_rates', 'exception_genka', 'product_sales_class', 'm_reorder_setting', 'f_sales_velocity_by_product', 'product_management_snapshot_rows', 'shops', 'm_products', 'm_set_components', 'f_sales_by_listing', 'f_sales_by_product', 'unmapped_sales', 'amazon_sku_fees', 'stock_monthly_snapshot', 'm_sku_master', 'm_sku_components'];
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
