/**
 * warehouse.db — 社内マスターデータ基盤
 *
 * テーブル構成:
 *   1. raw_ne_products — NE商品マスタ（UPSERT上書き）
 *   2. raw_ne_orders   — NE受注明細（追記蓄積、重複排除）
 *   3. shops           — 店舗マスタ（手動メンテ）
 *
 * マスターキー: NE商品コード
 * エンコーディング: NEのCSVはcp932 → スクリプト側でUTF-8変換してからINSERT
 */
import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'warehouse.db');

let db = null;

// ─── DB初期化 ───

export async function initDB() {
  const SQL = await initSqlJs();

  if (fs.existsSync(DB_FILE)) {
    const buf = fs.readFileSync(DB_FILE);
    db = new SQL.Database(buf);
    console.log('[Warehouse] 既存DB読み込み完了');
  } else {
    db = new SQL.Database();
    console.log('[Warehouse] 新規DB作成');
  }

  createTables();
  saveToFile();
  console.log('[Warehouse] 初期化完了');
  return db;
}

export function getDB() {
  if (!db) throw new Error('warehouse.db が初期化されていません。initDB() を先に呼んでください');
  return db;
}

export function saveToFile() {
  if (!db) return;
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const data = db.export();
  fs.writeFileSync(DB_FILE, Buffer.from(data));
}

// ─── テーブル作成 ───

function createTables() {
  // 1. NE商品マスタ（UPSERT上書き）
  db.run(`CREATE TABLE IF NOT EXISTS raw_ne_products (
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
    synced_at           TEXT DEFAULT (datetime('now'))
  )`);

  // 2. NE受注明細（追記蓄積、重複排除）
  db.run(`CREATE TABLE IF NOT EXISTS raw_ne_orders (
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
    synced_at           TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (伝票番号, 明細行番号)
  )`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_orders_date ON raw_ne_orders(受注日)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_orders_product ON raw_ne_orders(商品コード)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_orders_shop ON raw_ne_orders(店舗コード)`);

  // 3. セット商品マスタ（NEからCSV取得）
  db.run(`CREATE TABLE IF NOT EXISTS raw_ne_set_products (
    セット商品コード    TEXT,
    セット商品名        TEXT,
    セット販売価格      REAL,
    商品コード          TEXT,
    数量                INTEGER,
    セット在庫数        INTEGER,
    代表商品コード      TEXT,
    synced_at           TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (セット商品コード, 商品コード)
  )`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_set_parent ON raw_ne_set_products(セット商品コード)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_set_child ON raw_ne_set_products(商品コード)`);

  // 4. 店舗マスタ
  db.run(`CREATE TABLE IF NOT EXISTS shops (
    shop_code           TEXT PRIMARY KEY,
    shop_name           TEXT,
    platform            TEXT
  )`);

  // 初期データ投入（店舗マスタ）
  const shopCount = db.exec('SELECT COUNT(*) FROM shops');
  if (shopCount[0]?.values[0][0] === 0) {
    insertDefaultShops();
  }

  // 同期メタデータ
  db.run(`CREATE TABLE IF NOT EXISTS sync_meta (
    key                 TEXT PRIMARY KEY,
    value               TEXT,
    updated_at          TEXT DEFAULT (datetime('now'))
  )`);
}

function insertDefaultShops() {
  // NEの店舗コード一覧（設計書より）
  const shops = [
    ['1', 'Amazon', 'amazon'],
    ['2', 'Amazon FBA', 'amazon'],
    ['3', '楽天市場', 'rakuten'],
    ['4', 'Yahoo!ショッピング', 'yahoo'],
    ['5', 'au PAY マーケット', 'aupay'],
    ['6', 'Qoo10', 'qoo10'],
    ['7', 'メルカリShops', 'mercari'],
    ['8', 'LINEギフト', 'linegift'],
    ['9', 'ヤフオク', 'yahoo_auction'],
    ['10', '卸', 'wholesale'],
    ['11', 'Dショッピング', 'dshopping'],
    ['14', '楽天2号店', 'rakuten'],
  ];

  const stmt = db.prepare('INSERT OR IGNORE INTO shops (shop_code, shop_name, platform) VALUES (?, ?, ?)');
  for (const [code, name, platform] of shops) {
    stmt.run([code, name, platform]);
  }
  stmt.free();
  console.log('[Warehouse] 店舗マスタ初期投入完了');
}

// ─── 統計取得 ───

export function getStats() {
  const tables = ['raw_ne_products', 'raw_ne_orders', 'raw_ne_set_products', 'shops'];
  const stats = {};

  for (const table of tables) {
    try {
      const result = db.exec(`SELECT COUNT(*) FROM ${table}`);
      stats[table] = result[0]?.values[0][0] || 0;
    } catch {
      stats[table] = 0;
    }
  }

  // 受注の日付範囲
  try {
    const range = db.exec(`SELECT MIN(受注日), MAX(受注日) FROM raw_ne_orders`);
    if (range[0]?.values[0][0]) {
      stats.order_date_range = { min: range[0].values[0][0], max: range[0].values[0][1] };
    }
  } catch {}

  // 同期メタデータ
  try {
    const meta = db.exec('SELECT key, value FROM sync_meta ORDER BY key');
    stats.sync_meta = {};
    if (meta[0]) {
      for (const row of meta[0].values) {
        stats.sync_meta[row[0]] = row[1];
      }
    }
  } catch {
    stats.sync_meta = {};
  }

  return stats;
}

export function updateSyncMeta(key, value) {
  db.run('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, datetime("now"))', [key, value]);
  saveToFile();
}
