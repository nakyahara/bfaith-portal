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
    // v2.1: 一括リサーチ品質管理カラム
    ['match_type', 'TEXT'],           // jan / part_number / keyword / none
    ['match_confidence', 'TEXT'],     // high / medium / low
    ['composite_score', 'INTEGER'],   // 総合スコア（0〜85）
    ['competition_score', 'INTEGER'], // 競合強度スコア（0〜100）
    ['amazon_seller', 'INTEGER DEFAULT 0'], // Amazon本体出品（0 or 1）
    ['source', 'TEXT'],              // データ元（bulk-research / manual）
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
      asin TEXT,
      product_name TEXT,
      amazon_url TEXT,
      image_url TEXT,
      jan TEXT,
      part_number TEXT,
      supplier_code TEXT,
      supplier_name TEXT,
      maker_url TEXT,
      category TEXT,
      sales_rank INTEGER,
      offer_count INTEGER,
      comment TEXT,
      wholesale_price INTEGER,
      tax_rate TEXT,
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
      point_rate TEXT DEFAULT '1倍',
      ama_single_or_set TEXT DEFAULT '単品',
      variation_flag TEXT DEFAULT 'なし',
      ne_product_code TEXT,
      ne_supplier_code TEXT,
      ne_cost_excl_tax INTEGER,
      ne_selling_price INTEGER,
      ne_order_lot INTEGER,
      ne_tax_rate TEXT DEFAULT '10%',
      ne_registration_type TEXT DEFAULT '単品',
      ne_representative_code TEXT,
      ne_breakdown_code TEXT,
      ne_breakdown_qty INTEGER,
      status TEXT DEFAULT '発注済',
      created_at TEXT DEFAULT (datetime('now','localtime')),
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // productsテーブルのマイグレーション
  const prodCols = queryAll('PRAGMA table_info(products)').map(r => r.name);
  const newProdCols = [
    ['amazon_url', 'TEXT'], ['image_url', 'TEXT'], ['maker_url', 'TEXT'],
    ['category', 'TEXT'], ['sales_rank', 'INTEGER'], ['offer_count', 'INTEGER'],
    ['comment', 'TEXT'], ['wholesale_price', 'INTEGER'], ['tax_rate', 'TEXT'],
    ['lot', 'INTEGER DEFAULT 1'], ['referral_fee', 'INTEGER'], ['fba_fee', 'INTEGER'],
    ['storage_fee', 'INTEGER'], ['judgment', 'TEXT'],
    ['point_rate', "TEXT DEFAULT '1倍'"], ['ama_single_or_set', "TEXT DEFAULT '単品'"],
    ['variation_flag', "TEXT DEFAULT 'なし'"],
    ['ne_supplier_code', 'TEXT'], ['ne_cost_excl_tax', 'INTEGER'],
    ['ne_selling_price', 'INTEGER'], ['ne_order_lot', 'INTEGER'],
    ['ne_tax_rate', "TEXT DEFAULT '10%'"], ['ne_registration_type', "TEXT DEFAULT '単品'"],
    ['ne_representative_code', 'TEXT'], ['ne_breakdown_code', 'TEXT'],
    ['ne_breakdown_qty', 'INTEGER'],
  ];
  for (const [col, type] of newProdCols) {
    if (!prodCols.includes(col)) {
      db.run(`ALTER TABLE products ADD COLUMN ${col} ${type}`);
    }
  }

  // 在庫一覧テーブル（価格改定ツール用）
  db.run(`
    CREATE TABLE IF NOT EXISTS listings (
      sku TEXT PRIMARY KEY,
      asin TEXT,
      product_name TEXT,
      image_url TEXT,
      price REAL,
      shipping_price REAL DEFAULT 0,
      quantity INTEGER DEFAULT 0,
      status TEXT DEFAULT 'Active',
      condition TEXT DEFAULT '新品',
      fulfillment TEXT DEFAULT 'FBM',
      open_date TEXT,
      listing_id TEXT,
      item_description TEXT,
      cost_price REAL,
      loss_stopper REAL,
      high_stopper REAL,
      price_tracking TEXT DEFAULT 'しない',
      point INTEGER DEFAULT 0,
      referral_fee REAL DEFAULT 0,
      fba_fee REAL DEFAULT 0,
      buybox_price REAL,
      buybox_shipping REAL,
      buybox_points INTEGER DEFAULT 0,
      buybox_condition TEXT,
      offers_updated TEXT,
      total_sold INTEGER DEFAULT 0,
      last_updated TEXT,
      old_price REAL,
      price_changed INTEGER DEFAULT 0,
      synced_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // listings マイグレーション
  const listingsCols = queryAll('PRAGMA table_info(listings)').map(r => r.name);
  if (listingsCols.length > 0) {
    const newListingCols = [
      ['cost_price', 'REAL'],
      ['loss_stopper', 'REAL'],
      ['high_stopper', 'REAL'],
      ['price_tracking', "TEXT DEFAULT 'しない'"],
      ['point', 'INTEGER DEFAULT 0'],
      ['referral_fee', 'REAL DEFAULT 0'],
      ['fba_fee', 'REAL DEFAULT 0'],
      ['buybox_price', 'REAL'],
      ['buybox_shipping', 'REAL'],
      ['buybox_points', 'INTEGER DEFAULT 0'],
      ['buybox_condition', 'TEXT'],
      ['offers_updated', 'TEXT'],
      ['total_sold', 'INTEGER DEFAULT 0'],
      ['last_sold_date', 'TEXT'],
      ['last_updated', 'TEXT'],
      ['old_price', 'REAL'],
      ['price_changed', 'INTEGER DEFAULT 0'],
    ];
    for (const [col, type] of newListingCols) {
      if (!listingsCols.includes(col)) {
        try { db.run(`ALTER TABLE listings ADD COLUMN ${col} ${type}`); } catch (e) {}
      }
    }
  }

  // 同期メタ情報テーブル
  db.run(`
    CREATE TABLE IF NOT EXISTS sync_meta (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `);

  // セット商品内訳テーブル
  db.run(`
    CREATE TABLE IF NOT EXISTS product_set_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
      syohin_code TEXT NOT NULL,
      suryo INTEGER NOT NULL DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // 価格変動履歴テーブル（価格改定機能用）
  db.run(`
    CREATE TABLE IF NOT EXISTS price_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
      asin TEXT NOT NULL,
      sku TEXT,
      old_price INTEGER,
      new_price INTEGER,
      reason TEXT,
      mode TEXT,
      competitor_price INTEGER,
      buy_box_price INTEGER,
      created_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // 一括リサーチセッションテーブル（結果永続化用）
  db.run(`
    CREATE TABLE IF NOT EXISTS bulk_research_sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      settings TEXT,
      results TEXT,
      total_count INTEGER DEFAULT 0,
      processed_count INTEGER DEFAULT 0,
      status TEXT DEFAULT '処理中',
      created_at TEXT DEFAULT (datetime('now','localtime')),
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // productsテーブル: 価格改定用カラム追加
  const priceRevisionCols = [
    ['last_checked_at', 'TEXT'],
    ['last_price_changed_at', 'TEXT'],
    ['competitor_price', 'INTEGER'],
    ['price_change_count_today', 'INTEGER DEFAULT 0'],
    ['price_change_count_date', 'TEXT'],
    ['sku', 'TEXT'],
  ];
  const prodCols2 = queryAll('PRAGMA table_info(products)').map(r => r.name);
  for (const [col, type] of priceRevisionCols) {
    if (!prodCols2.includes(col)) {
      db.run(`ALTER TABLE products ADD COLUMN ${col} ${type}`);
    }
  }

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
      match_type, match_confidence, composite_score, competition_score, amazon_seller, source,
      status
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    n(data.matchType), n(data.matchConfidence), n(data.compositeScore), n(data.competitionScore), n(data.amazonSeller), n(data.source),
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

// 商品を直接保存（商品計算ページから）
export function saveProduct(data) {
  const n = (v) => v === undefined ? null : v;
  db.run(`
    INSERT INTO products (
      research_id, asin, product_name, amazon_url, image_url, jan, part_number,
      supplier_code, supplier_name, maker_url, category, sales_rank, offer_count, comment,
      wholesale_price, tax_rate, wholesale_price_with_tax, lot,
      selling_price, referral_fee, fba_fee, storage_fee, total_fee,
      profit, profit_rate, judgment,
      fulfillment, loss_stopper, high_stopper, price_tracking, point_rate,
      ama_single_or_set, variation_flag,
      ne_product_code, ne_supplier_code, ne_cost_excl_tax, ne_selling_price,
      ne_order_lot, ne_tax_rate, ne_registration_type,
      ne_representative_code,
      status
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `, [
    n(data.researchId), n(data.asin), n(data.productName), n(data.amazonUrl), n(data.imageUrl),
    n(data.jan), n(data.partNumber),
    n(data.supplierCode), n(data.supplierName), n(data.makerUrl),
    n(data.category), n(data.salesRank), n(data.offerCount), n(data.comment),
    n(data.wholesalePrice), n(data.taxRate), n(data.wholesalePriceWithTax), n(data.lot),
    n(data.sellingPrice), n(data.referralFee), n(data.fbaFee), n(data.storageFee), n(data.totalFee),
    n(data.profit), n(data.profitRate), n(data.judgment),
    n(data.fulfillment), n(data.lossStopper), n(data.highStopper), n(data.priceTracking), n(data.pointRate),
    n(data.amaSingleOrSet), n(data.variationFlag),
    n(data.neProductCode), n(data.neSupplierCode), n(data.neCostExclTax), n(data.neSellingPrice),
    n(data.neOrderLot), n(data.neTaxRate), n(data.neRegistrationType),
    n(data.neRepresentativeCode),
    data.status || '発注済',
  ]);
  saveToFile();
  const result = db.exec('SELECT last_insert_rowid() as id');
  const productId = result[0].values[0][0];

  // セット内訳の保存
  if (data.setItems && Array.isArray(data.setItems)) {
    for (const item of data.setItems) {
      if (item.syohinCode) {
        db.run('INSERT INTO product_set_items (product_id, syohin_code, suryo) VALUES (?,?,?)',
          [productId, item.syohinCode, item.suryo || 1]);
      }
    }
    saveToFile();
  }
  return productId;
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
  sql += ' ORDER BY created_at DESC, id DESC';

  return queryAll(sql, params);
}

export function updateProductStatus(id, status) {
  db.run("UPDATE products SET status = ?, updated_at = datetime('now','localtime') WHERE id = ?", [status, id]);
  saveToFile();
}

const PRODUCT_EDITABLE = new Set([
  'asin', 'product_name', 'amazon_url', 'image_url', 'jan', 'part_number',
  'supplier_code', 'supplier_name', 'maker_url', 'category', 'comment',
  'wholesale_price', 'tax_rate', 'wholesale_price_with_tax', 'lot',
  'selling_price', 'referral_fee', 'fba_fee', 'storage_fee', 'total_fee',
  'profit', 'profit_rate', 'judgment',
  'fulfillment', 'loss_stopper', 'high_stopper', 'price_tracking', 'point_rate',
  'ama_single_or_set', 'variation_flag',
  'ne_product_code', 'ne_supplier_code', 'ne_cost_excl_tax', 'ne_selling_price',
  'ne_order_lot', 'ne_tax_rate', 'ne_registration_type', 'ne_representative_code',
  'status',
]);

export function updateProduct(id, data) {
  const fields = [];
  const params = [];
  for (const [key, value] of Object.entries(data)) {
    if (PRODUCT_EDITABLE.has(key)) {
      fields.push(`${key} = ?`);
      params.push(value === undefined ? null : value);
    }
  }
  if (fields.length > 0) {
    fields.push("updated_at = datetime('now','localtime')");
    params.push(id);
    db.run(`UPDATE products SET ${fields.join(', ')} WHERE id = ?`, params);
    saveToFile();
  }
}

// ===== Product Set Items =====

export function getSetItems(productId) {
  return queryAll('SELECT * FROM product_set_items WHERE product_id = ? ORDER BY id', [productId]);
}

export function saveSetItems(productId, items) {
  db.run('DELETE FROM product_set_items WHERE product_id = ?', [productId]);
  for (const item of items) {
    if (item.syohinCode || item.syohin_code) {
      db.run('INSERT INTO product_set_items (product_id, syohin_code, suryo) VALUES (?,?,?)',
        [productId, item.syohinCode || item.syohin_code, item.suryo || 1]);
    }
  }
  saveToFile();
}

export function deleteProduct(id) {
  db.run('DELETE FROM product_set_items WHERE product_id = ?', [id]);
  db.run('DELETE FROM products WHERE id = ?', [id]);
  saveToFile();
}

export function getProductById(id) {
  const rows = queryAll('SELECT * FROM products WHERE id = ?', [id]);
  return rows.length > 0 ? rows[0] : null;
}

// ===== Listings（在庫一覧・価格改定用） =====

export function upsertListing(item) {
  // 既存レコードがあるか確認
  const existing = queryAll('SELECT * FROM listings WHERE sku = ?', [item.sku]);

  if (existing.length > 0) {
    const old = existing[0];
    // 既存レコードの場合: レポートで更新されるフィールドだけ上書き、ユーザー設定は保持
    // ただし、出品時に仕入価格等が渡された場合はそちらで上書き
    const setCost = item.cost_price ? ', cost_price = ?' : '';
    const setLoss = item.loss_stopper ? ', loss_stopper = ?' : '';
    const setHigh = item.high_stopper ? ', high_stopper = ?' : '';
    const setTrack = item.price_tracking ? ', price_tracking = ?' : '';

    const params = [
      item.asin, item.product_name, item.image_url, item.price, item.shipping_price || 0,
      item.quantity, item.status, item.condition, item.fulfillment, item.open_date,
      item.listing_id, item.item_description,
      item.price, item.price,
    ];
    if (item.cost_price) params.push(item.cost_price);
    if (item.loss_stopper) params.push(item.loss_stopper);
    if (item.high_stopper) params.push(item.high_stopper);
    if (item.price_tracking) params.push(item.price_tracking);
    params.push(item.sku);

    db.run(`
      UPDATE listings SET
        asin = ?, product_name = ?, image_url = ?, price = ?, shipping_price = ?,
        quantity = ?, status = ?, condition = ?, fulfillment = ?, open_date = ?,
        listing_id = ?, item_description = ?,
        old_price = CASE WHEN price != ? THEN price ELSE old_price END,
        price_changed = CASE WHEN price != ? THEN 1 ELSE 0 END
        ${setCost}${setLoss}${setHigh}${setTrack},
        last_updated = datetime('now','localtime'),
        synced_at = datetime('now','localtime')
      WHERE sku = ?
    `, params);
  } else {
    // 新規レコード — 仕入価格・ストッパー・価格追従も含めて登録
    db.run(`
      INSERT INTO listings (
        sku, asin, product_name, image_url, price, shipping_price,
        quantity, status, condition, fulfillment, open_date,
        listing_id, item_description,
        cost_price, loss_stopper, high_stopper, price_tracking,
        synced_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
    `, [
      item.sku, item.asin, item.product_name, item.image_url, item.price, item.shipping_price || 0,
      item.quantity, item.status, item.condition, item.fulfillment, item.open_date,
      item.listing_id, item.item_description,
      item.cost_price || null, item.loss_stopper || null, item.high_stopper || null,
      item.price_tracking || 'しない',
    ]);
  }
}

export function syncListings(listings) {
  for (const item of listings) {
    upsertListing(item);
  }
  setSyncMeta('listings_last_sync', new Date().toISOString());
  saveToFile();
}

export function getListings() {
  return queryAll('SELECT * FROM listings ORDER BY product_name');
}

export function updateListing(sku, data, skipSave = false) {
  const EDITABLE = new Set([
    'price', 'quantity', 'cost_price', 'loss_stopper', 'high_stopper',
    'price_tracking', 'point', 'status', 'total_sold', 'last_sold_date',
  ]);
  const fields = [];
  const params = [];
  for (const [key, value] of Object.entries(data)) {
    if (EDITABLE.has(key)) {
      fields.push(`${key} = ?`);
      params.push(value === undefined ? null : value);
    }
  }
  if (fields.length === 0) return 0;
  fields.push("last_updated = datetime('now','localtime')");
  params.push(sku);
  db.run(`UPDATE listings SET ${fields.join(', ')} WHERE sku = ?`, params);
  const changes = db.getRowsModified();
  if (!skipSave) saveToFile();
  return changes;
}

export function bulkSave() {
  saveToFile();
}

export function getSyncMeta(key) {
  const rows = queryAll('SELECT value FROM sync_meta WHERE key = ?', [key]);
  return rows.length > 0 ? rows[0].value : null;
}

export function setSyncMeta(key, value) {
  db.run('INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, ?)', [key, value]);
  saveToFile();
}

// ===== Amazon商品同期 =====

/**
 * Amazonレポートから全商品をDBに同期（upsert）
 * @param {Array} listings - getActiveListingsReport().listings
 * @returns {{ inserted: number, updated: number, total: number }}
 */
export function syncProductsFromListings(listings) {
  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  // 最初の1件のキーをログに出す（デバッグ用）
  if (listings.length > 0) {
    const keys = Object.keys(listings[0]);
    console.log('[Sync] レポートキー数:', keys.length);
    console.log('[Sync] 全キー:', keys.join(' | '));
    console.log('[Sync] 1行目:', JSON.stringify(listings[0]).slice(0, 800));
  }

  // キー名を動的検索するヘルパー（完全一致 → 正規化一致の順で検索）
  function findVal(row, patterns) {
    // まず完全一致
    for (const p of patterns) {
      if (row[p] !== undefined && row[p] !== '') return row[p];
    }
    // 次に正規化一致（小文字化 + 記号除去）
    for (const key of Object.keys(row)) {
      const lk = key.toLowerCase().replace(/[\s_\-・]/g, '');
      for (const p of patterns) {
        if (lk === p.toLowerCase().replace(/[\s_\-・]/g, '')) return row[key];
      }
    }
    return '';
  }

  for (const row of listings) {
    const asin = findVal(row, ['asin1', 'asin', 'ASIN1', 'ASIN', '商品ID', '商品id']);
    const sku = findVal(row, ['seller-sku', 'seller_sku', 'sellersku', 'sku', 'SKU', '出品者SKU', '出品者sku']);

    if (!asin && !sku) { skipped++; continue; }

    const productName = findVal(row, ['item-name', 'item_name', 'itemname', '商品名']);
    const price = parseFloat(findVal(row, ['price', 'Price', '価格']) || '0') || null;
    const fulfillmentRaw = findVal(row, ['fulfillment-channel', 'fulfillment_channel', 'fulfillmentchannel', 'フルフィルメント・チャンネル']);
    const fulfillment = fulfillmentRaw.includes('Amazon') || fulfillmentRaw.includes('AMAZON') ? 'FBA' : 'FBM';
    const imageUrl = findVal(row, ['image-url', 'image_url', 'imageurl']);

    // 既存チェック: ASINのみで照合（SKU違いの重複を防ぐ）、なければSKUでも照合
    let existing = queryAll('SELECT id FROM products WHERE asin = ? LIMIT 1', [asin]);
    if (existing.length === 0 && sku) {
      existing = queryAll('SELECT id FROM products WHERE sku = ? OR ne_product_code = ? LIMIT 1', [sku, sku]);
    }

    if (existing.length > 0) {
      db.run(`
        UPDATE products SET
          product_name = COALESCE(NULLIF(?, ''), product_name),
          selling_price = COALESCE(?, selling_price),
          fulfillment = ?,
          image_url = COALESCE(NULLIF(?, ''), image_url),
          sku = COALESCE(NULLIF(?, ''), sku),
          updated_at = datetime('now','localtime')
        WHERE id = ?
      `, [productName, price, fulfillment, imageUrl, sku, existing[0].id]);
      updated++;
    } else {
      db.run(`
        INSERT INTO products (asin, product_name, selling_price, fulfillment, image_url, sku, ne_product_code, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, '出品中')
      `, [asin, productName, price, fulfillment, imageUrl, sku, sku]);
      inserted++;
    }
  }

  saveToFile();
  console.log(`[Sync] 結果: 全${listings.length}件, 新規${inserted}, 更新${updated}, スキップ${skipped}`);

  // デバッグ情報
  const debug = {};
  if (listings.length > 0) {
    debug.allKeys = Object.keys(listings[0]);
    debug.sampleRow = listings[0];
    debug.detectedAsin = findVal(listings[0], ['asin1', 'asin', 'ASIN1', 'ASIN']);
    debug.detectedSku = findVal(listings[0], ['seller-sku', 'seller_sku', 'sellersku', 'sku', 'SKU']);
  }

  return { inserted, updated, skipped, total: listings.length, debug };
}

// ===== Price History (価格改定) =====

export function savePriceHistory({ productId, asin, sku, oldPrice, newPrice, reason, mode, competitorPrice, buyBoxPrice }) {
  db.run(`
    INSERT INTO price_history (product_id, asin, sku, old_price, new_price, reason, mode, competitor_price, buy_box_price)
    VALUES (?,?,?,?,?,?,?,?,?)
  `, [productId, asin, sku, oldPrice, newPrice, reason, mode, competitorPrice, buyBoxPrice]);
  saveToFile();
}

export function getPriceHistory(productId, limit = 50) {
  return queryAll('SELECT * FROM price_history WHERE product_id = ? ORDER BY id DESC LIMIT ?', [productId, limit]);
}

export function getRecentPriceHistory(limit = 100) {
  return queryAll('SELECT * FROM price_history ORDER BY id DESC LIMIT ?', [limit]);
}

/**
 * 通知が来ていない商品を取得（フォールバック対象）
 * last_checked_at が NULL または24時間以上前の、追従設定済み商品
 */
export function getStaleTrackingProducts(hoursThreshold = 24) {
  return queryAll(`
    SELECT * FROM products
    WHERE price_tracking IS NOT NULL AND price_tracking != '' AND price_tracking != 'しない'
      AND sku IS NOT NULL AND sku != ''
      AND (last_checked_at IS NULL OR last_checked_at < datetime('now', 'localtime', '-${hoursThreshold} hours'))
    ORDER BY last_checked_at ASC
  `);
}

/**
 * 30日以上前の価格履歴を削除
 */
export function cleanupOldPriceHistory(days = 30) {
  const result = queryAll(`SELECT COUNT(*) as cnt FROM price_history WHERE created_at < datetime('now', 'localtime', '-${days} days')`);
  const count = result[0]?.cnt || 0;
  if (count > 0) {
    db.run(`DELETE FROM price_history WHERE created_at < datetime('now', 'localtime', '-${days} days')`);
    saveToFile();
  }
  return count;
}

export function getTrackingProducts() {
  return queryAll("SELECT * FROM products WHERE price_tracking IS NOT NULL AND price_tracking != '' AND sku IS NOT NULL AND sku != ''");
}

export function updateProductPriceInfo(id, data) {
  const fields = [];
  const params = [];
  const allowed = ['selling_price', 'competitor_price', 'last_checked_at', 'last_price_changed_at', 'price_change_count_today', 'price_change_count_date'];
  for (const [key, value] of Object.entries(data)) {
    if (allowed.includes(key)) {
      fields.push(`${key} = ?`);
      params.push(value === undefined ? null : value);
    }
  }
  if (fields.length === 0) return;
  fields.push("updated_at = datetime('now','localtime')");
  params.push(id);
  db.run(`UPDATE products SET ${fields.join(', ')} WHERE id = ?`, params);
  saveToFile();
}

// ===== Bulk Research Sessions =====

export function saveBulkSession(data) {
  db.run(`
    INSERT INTO bulk_research_sessions (name, settings, results, total_count, processed_count, status)
    VALUES (?, ?, ?, ?, ?, ?)
  `, [
    data.name,
    JSON.stringify(data.settings || {}),
    JSON.stringify(data.results || []),
    data.totalCount || 0,
    data.processedCount || 0,
    data.status || '完了',
  ]);
  saveToFile();
  const result = db.exec('SELECT last_insert_rowid() as id');
  return result[0].values[0][0];
}

export function updateBulkSession(id, data) {
  const fields = [];
  const params = [];
  if (data.name !== undefined) { fields.push('name = ?'); params.push(data.name); }
  if (data.settings !== undefined) { fields.push('settings = ?'); params.push(JSON.stringify(data.settings)); }
  if (data.results !== undefined) { fields.push('results = ?'); params.push(JSON.stringify(data.results)); }
  if (data.totalCount !== undefined) { fields.push('total_count = ?'); params.push(data.totalCount); }
  if (data.processedCount !== undefined) { fields.push('processed_count = ?'); params.push(data.processedCount); }
  if (data.status !== undefined) { fields.push('status = ?'); params.push(data.status); }
  if (fields.length === 0) return;
  fields.push("updated_at = datetime('now','localtime')");
  params.push(id);
  db.run(`UPDATE bulk_research_sessions SET ${fields.join(', ')} WHERE id = ?`, params);
  saveToFile();
}

export function getBulkSessions() {
  return queryAll('SELECT id, name, total_count, processed_count, status, created_at, updated_at FROM bulk_research_sessions ORDER BY updated_at DESC');
}

export function getBulkSessionById(id) {
  const rows = queryAll('SELECT * FROM bulk_research_sessions WHERE id = ?', [id]);
  if (rows.length === 0) return null;
  const row = rows[0];
  try { row.settings = JSON.parse(row.settings || '{}'); } catch { row.settings = {}; }
  try { row.results = JSON.parse(row.results || '[]'); } catch { row.results = []; }
  return row;
}

export function deleteBulkSession(id) {
  db.run('DELETE FROM bulk_research_sessions WHERE id = ?', [id]);
  saveToFile();
}
