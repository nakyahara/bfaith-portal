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
  createTables();
  console.log('[Warehouse] 初期化完了');
  return db;
}

export function getDB() {
  if (!db) throw new Error('warehouse.db が初期化されていません。initDB() を先に呼んでください');
  return db;
}

// better-sqlite3はファイルベースなのでsaveToFile不要。互換性のため空関数を残す
export function saveToFile() {}

// ─── テーブル作成 ───

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

  // 5. SKUマッピング（Amazon SKU ↔ NE商品コード）
  db.exec(`CREATE TABLE IF NOT EXISTS sku_map (
    seller_sku          TEXT PRIMARY KEY,
    asin                TEXT,
    商品名              TEXT,
    ne_code             TEXT,
    数量                INTEGER DEFAULT 1,
    synced_at           TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sku_map_asin ON sku_map(asin)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_sku_map_ne ON sku_map(ne_code)');

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

  // 11. 店舗マスタ
  db.exec(`CREATE TABLE IF NOT EXISTS shops (
    shop_code           TEXT PRIMARY KEY,
    shop_name           TEXT,
    platform            TEXT
  )`);

  // 店舗マスタは毎回洗い替え
  db.exec('DELETE FROM shops');
  insertDefaultShops();

  // 12. 同期メタデータ
  db.exec(`CREATE TABLE IF NOT EXISTS sync_meta (
    key                 TEXT PRIMARY KEY,
    value               TEXT,
    updated_at          TEXT
  )`);
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
  const tables = ['raw_ne_products', 'raw_ne_orders', 'raw_ne_set_products', 'raw_sp_orders', 'raw_sp_orders_log', 'raw_lz_inventory', 'sku_map', 'product_shipping', 'shipping_rates', 'exception_genka', 'shops'];
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
