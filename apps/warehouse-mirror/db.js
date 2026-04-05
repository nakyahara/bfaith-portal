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

function createTables() {
  // mirror_products — 統合商品マスタ（m_productsのミラー）
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_products (
    product_id        INTEGER PRIMARY KEY,
    商品コード        TEXT UNIQUE NOT NULL,
    商品名            TEXT,
    商品区分          TEXT NOT NULL,
    取扱区分          TEXT,
    標準売価          REAL,
    原価              REAL,
    原価ソース        TEXT,
    原価状態          TEXT NOT NULL,
    送料              REAL,
    送料コード        TEXT,
    配送方法          TEXT,
    消費税率          REAL,
    税区分            TEXT,
    在庫数            INTEGER,
    引当数            INTEGER,
    仕入先コード      TEXT,
    セット構成品数    INTEGER,
    売上分類          INTEGER,
    updated_at        TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_sku ON mirror_products(商品コード)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_status ON mirror_products(取扱区分)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mirp_type ON mirror_products(商品区分)');

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

  // mirror_sku_map — Amazon SKU→NE商品コード対応
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

  // mirror_sync_status — 同期状態
  db.exec(`CREATE TABLE IF NOT EXISTS mirror_sync_status (
    key               TEXT PRIMARY KEY,
    value             TEXT,
    updated_at        TEXT
  )`);
}
