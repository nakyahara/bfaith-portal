/**
 * SQLite DB管理 — 仕入れリサーチ & 商品登録
 * sql.js パターン（mercari-sync/settings-db.js 準拠）
 */
import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const DB_FILE = path.join(DATA_DIR, 'profit.db');

let db = null;

// ===== ヘルパー =====
function queryAll(sql, params = []) {
  const stmt = db.prepare(sql);
  if (params.length > 0) stmt.bind(params);
  const results = [];
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

function saveToFile() {
  if (!db) return;
  const data = db.export();
  fs.writeFileSync(DB_FILE, Buffer.from(data));
}

export async function initDb() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  const SQL = await initSqlJs();

  if (fs.existsSync(DB_FILE)) {
    const buf = fs.readFileSync(DB_FILE);
    db = new SQL.Database(buf);
  } else {
    db = new SQL.Database();
  }

  db.run(`
    CREATE TABLE IF NOT EXISTS research (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asin TEXT NOT NULL,
      product_name TEXT,
      amazon_url TEXT,
      image_url TEXT,
      category TEXT,
      sales_rank INTEGER,
      offer_count INTEGER,
      jan TEXT,
      part_number TEXT,
      supplier_code TEXT,
      supplier_name TEXT,
      maker_url TEXT,
      quote_url TEXT,
      comment TEXT,
      wholesale_price INTEGER,
      tax_rate INTEGER DEFAULT 10,
      wholesale_price_with_tax INTEGER,
      lot INTEGER DEFAULT 1,
      selling_price INTEGER,
      referral_fee INTEGER,
      fba_fee INTEGER,
      storage_fee INTEGER,
      total_fee INTEGER,
      profit INTEGER,
      profit_rate REAL,
      judgment TEXT,
      fulfillment TEXT DEFAULT 'FBA',
      loss_stopper INTEGER,
      high_stopper INTEGER,
      price_tracking TEXT DEFAULT 'カート',
      quantity TEXT,
      expiry_date TEXT,
      variation_flag INTEGER DEFAULT 0,
      ama_single_or_set TEXT,
      purchase_flag INTEGER DEFAULT 0,
      quote_requested_at TEXT,
      quote_responded_at TEXT,
      quote_note TEXT,
      status TEXT DEFAULT 'リサーチ',
      created_at TEXT DEFAULT (datetime('now','localtime')),
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // マイグレーション: 既存テーブルに新カラムを追加
  const existingCols = queryAll('PRAGMA table_info(research)').map(r => r.name);
  const newCols = [
    ['quantity', 'TEXT'],
    ['expiry_date', 'TEXT'],
    ['variation_flag', 'INTEGER DEFAULT 0'],
    ['ama_single_or_set', 'TEXT'],
    ['purchase_flag', 'INTEGER DEFAULT 0'],
    ['quote_requested_at', 'TEXT'],
    ['quote_responded_at', 'TEXT'],
    ['quote_note', 'TEXT'],
  ];
  for (const [col, type] of newCols) {
    if (!existingCols.includes(col)) {
      db.run(`ALTER TABLE research ADD COLUMN ${col} ${type}`);
    }
  }

  db.run(`
    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      research_id INTEGER REFERENCES research(id),
      asin TEXT NOT NULL,
      product_name TEXT,
      jan TEXT,
      part_number TEXT,
      supplier_code TEXT,
      supplier_name TEXT,
      wholesale_price_with_tax INTEGER,
      selling_price INTEGER,
      total_fee INTEGER,
      profit INTEGER,
      profit_rate REAL,
      fulfillment TEXT DEFAULT 'FBA',
      loss_stopper INTEGER,
      high_stopper INTEGER,
      price_tracking TEXT DEFAULT 'カート',
      ne_product_code TEXT,
      status TEXT DEFAULT '発注済',
      created_at TEXT DEFAULT (datetime('now','localtime')),
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  saveToFile();
}

// ===== Research =====

export function saveResearch(data) {
  const n = (v) => v === undefined ? null : v; // undefinedをnullに変換
  db.run(`
    INSERT INTO research (
      asin, product_name, amazon_url, image_url, category, sales_rank, offer_count,
      jan, part_number, supplier_code, supplier_name, maker_url, quote_url, comment,
      wholesale_price, tax_rate, wholesale_price_with_tax, lot,
      selling_price, referral_fee, fba_fee, storage_fee, total_fee,
      profit, profit_rate, judgment,
      fulfillment, loss_stopper, high_stopper, price_tracking,
      quantity, expiry_date, variation_flag, ama_single_or_set, purchase_flag,
      quote_requested_at, quote_responded_at, quote_note,
      status
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `, [
    n(data.asin), n(data.productName), n(data.amazonUrl), n(data.imageUrl),
    n(data.category), n(data.salesRank), n(data.offerCount),
    n(data.jan), n(data.partNumber), n(data.supplierCode), n(data.supplierName),
    n(data.makerUrl), n(data.quoteUrl), n(data.comment),
    n(data.wholesalePrice), n(data.taxRate), n(data.wholesalePriceWithTax), n(data.lot),
    n(data.sellingPrice), n(data.referralFee), n(data.fbaFee), n(data.storageFee), n(data.totalFee),
    n(data.profit), n(data.profitRate), n(data.judgment),
    n(data.fulfillment), n(data.lossStopper), n(data.highStopper), n(data.priceTracking),
    n(data.quantity), n(data.expiryDate), n(data.variationFlag), n(data.amaSingleOrSet), n(data.purchaseFlag),
    n(data.quoteRequestedAt), n(data.quoteRespondedAt), n(data.quoteNote),
    data.status || 'リサーチ',
  ]);
  saveToFile();

  const result = db.exec('SELECT last_insert_rowid() as id');
  return result[0].values[0][0];
}

export function getResearch(filters = {}) {
  let sql = 'SELECT * FROM research';
  const conditions = [];
  const params = [];

  if (filters.status) {
    conditions.push('status = ?');
    params.push(filters.status);
  }
  if (filters.search) {
    conditions.push('(asin LIKE ? OR product_name LIKE ? OR supplier_name LIKE ?)');
    const s = `%${filters.search}%`;
    params.push(s, s, s);
  }

  if (conditions.length > 0) sql += ' WHERE ' + conditions.join(' AND ');
  sql += ' ORDER BY id DESC';

  return queryAll(sql, params);
}

export function getResearchById(id) {
  const rows = queryAll('SELECT * FROM research WHERE id = ?', [id]);
  return rows.length > 0 ? rows[0] : null;
}

export function updateResearchStatus(id, status) {
  db.run("UPDATE research SET status = ?, updated_at = datetime('now','localtime') WHERE id = ?", [status, id]);
  saveToFile();
}

// 許可されたカラムのみ更新
const RESEARCH_EDITABLE = new Set([
  'supplier_code', 'supplier_name', 'maker_url', 'quote_url', 'comment',
  'wholesale_price', 'tax_rate', 'wholesale_price_with_tax', 'lot',
  'selling_price', 'referral_fee', 'fba_fee', 'storage_fee', 'total_fee',
  'profit', 'profit_rate', 'judgment',
  'fulfillment', 'loss_stopper', 'high_stopper', 'price_tracking',
  'quantity', 'expiry_date', 'variation_flag', 'ama_single_or_set', 'purchase_flag',
  'quote_requested_at', 'quote_responded_at', 'quote_note', 'status',
  'jan', 'part_number',
]);

export function updateResearch(id, data) {
  const fields = [];
  const params = [];
  for (const [key, value] of Object.entries(data)) {
    if (RESEARCH_EDITABLE.has(key)) {
      fields.push(`${key} = ?`);
      params.push(value === undefined ? null : value);
    }
  }
  if (fields.length === 0) return;
  fields.push("updated_at = datetime('now','localtime')");
  params.push(id);
  db.run(`UPDATE research SET ${fields.join(', ')} WHERE id = ?`, params);
  saveToFile();
}

// ===== Products =====

export function promoteToProduct(researchId) {
  const r = getResearchById(researchId);
  if (!r) throw new Error('リサーチデータが見つかりません');

  db.run(`
    INSERT INTO products (
      research_id, asin, product_name, jan, part_number,
      supplier_code, supplier_name, wholesale_price_with_tax,
      selling_price, total_fee, profit, profit_rate,
      fulfillment, loss_stopper, high_stopper, price_tracking
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `, [
    r.id, r.asin, r.product_name, r.jan, r.part_number,
    r.supplier_code, r.supplier_name, r.wholesale_price_with_tax,
    r.selling_price, r.total_fee, r.profit, r.profit_rate,
    r.fulfillment, r.loss_stopper, r.high_stopper, r.price_tracking,
  ]);

  updateResearchStatus(researchId, '発注済');
  saveToFile();

  const result = db.exec('SELECT last_insert_rowid() as id');
  return result[0].values[0][0];
}

export function getProducts(filters = {}) {
  let sql = 'SELECT * FROM products';
  const conditions = [];
  const params = [];

  if (filters.status) {
    conditions.push('status = ?');
    params.push(filters.status);
  }
  if (filters.search) {
    conditions.push('(asin LIKE ? OR product_name LIKE ?)');
    const s = `%${filters.search}%`;
    params.push(s, s);
  }

  if (conditions.length > 0) sql += ' WHERE ' + conditions.join(' AND ');
  sql += ' ORDER BY id DESC';

  return queryAll(sql, params);
}

export function updateProductStatus(id, status) {
  db.run("UPDATE products SET status = ?, updated_at = datetime('now','localtime') WHERE id = ?", [status, id]);
  saveToFile();
}

export function updateProduct(id, data) {
  const fields = [];
  const params = [];
  for (const [key, value] of Object.entries(data)) {
    fields.push(`${key} = ?`);
    params.push(value);
  }
  fields.push("updated_at = datetime('now','localtime')");
  params.push(id);
  db.run(`UPDATE products SET ${fields.join(', ')} WHERE id = ?`, params);
  saveToFile();
}
