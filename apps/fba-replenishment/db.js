/**
 * SQLite DB管理 — FBA在庫補充システム
 * sql.js パターン（profit-calculator/db.js 準拠）
 */
import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const DB_FILE = path.join(DATA_DIR, 'fba.db');

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

function queryOne(sql, params = []) {
  const rows = queryAll(sql, params);
  return rows[0] || null;
}

function run(sql, params = []) {
  db.run(sql, params);
}

function saveToFile() {
  if (!db) return;
  const data = db.export();
  fs.writeFileSync(DB_FILE, Buffer.from(data));
}

// ===== 初期化 =====
export async function initDb() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  const SQL = await initSqlJs();

  if (fs.existsSync(DB_FILE)) {
    const buf = fs.readFileSync(DB_FILE);
    db = new SQL.Database(buf);
  } else {
    db = new SQL.Database();
  }

  // --- 1. sku_mapping: 商品コード変換（スプシ同期） ---
  db.run(`
    CREATE TABLE IF NOT EXISTS sku_mapping (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      amazon_sku TEXT NOT NULL UNIQUE,
      asin TEXT,
      product_name TEXT,
      ne_code TEXT,
      logizard_code TEXT,
      fnsku TEXT,
      jan TEXT,
      is_set INTEGER DEFAULT 0,
      set_components TEXT,
      per_unit_volume REAL,
      storage_type TEXT,
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 2. sku_exceptions: FBA優先送りマスタ ---
  db.run(`
    CREATE TABLE IF NOT EXISTS sku_exceptions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      amazon_sku TEXT NOT NULL UNIQUE,
      exception_type TEXT NOT NULL CHECK(exception_type IN ('send_all', 'keep_minimum')),
      keep_minimum_qty INTEGER DEFAULT 0,
      reason TEXT,
      created_at TEXT DEFAULT (datetime('now','localtime')),
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 3. warehouse_inventory: ロジザードCSV（毎回上書き） ---
  db.run(`
    CREATE TABLE IF NOT EXISTS warehouse_inventory (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      logizard_code TEXT NOT NULL,
      product_name TEXT,
      location TEXT,
      quantity INTEGER DEFAULT 0,
      expiry_date TEXT,
      lot_no TEXT,
      is_y_location INTEGER DEFAULT 0,
      uploaded_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 4. facility_inventory: 施設在庫（スプシ同期） ---
  db.run(`
    CREATE TABLE IF NOT EXISTS facility_inventory (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_code TEXT NOT NULL,
      product_name TEXT,
      facility_name TEXT,
      quantity INTEGER DEFAULT 0,
      synced_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 5. shipment_plans: 納品計画 ---
  db.run(`
    CREATE TABLE IF NOT EXISTS shipment_plans (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      plan_date TEXT NOT NULL,
      status TEXT DEFAULT 'draft' CHECK(status IN ('draft', 'confirmed', 'shipped', 'completed')),
      total_skus INTEGER DEFAULT 0,
      total_units INTEGER DEFAULT 0,
      note TEXT,
      created_at TEXT DEFAULT (datetime('now','localtime')),
      confirmed_at TEXT,
      shipped_at TEXT,
      completed_at TEXT
    )
  `);

  // 納品計画の明細行
  db.run(`
    CREATE TABLE IF NOT EXISTS shipment_plan_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      plan_id INTEGER NOT NULL REFERENCES shipment_plans(id),
      amazon_sku TEXT NOT NULL,
      asin TEXT,
      product_name TEXT,
      recommended_qty INTEGER DEFAULT 0,
      adjusted_qty INTEGER DEFAULT 0,
      reason TEXT,
      urgency_score REAL DEFAULT 0,
      days_of_supply REAL,
      fba_available INTEGER DEFAULT 0,
      fba_inbound INTEGER DEFAULT 0,
      warehouse_qty INTEGER DEFAULT 0,
      alert_type TEXT,
      alert_message TEXT,
      your_price REAL,
      featured_offer_price REAL,
      lowest_price REAL,
      sales_rank INTEGER,
      units_sold_7d INTEGER DEFAULT 0,
      units_sold_30d INTEGER DEFAULT 0,
      units_sold_60d INTEGER DEFAULT 0,
      units_sold_90d INTEGER DEFAULT 0
    )
  `);

  // --- 6. shipment_daily_summary: 日別サマリー ---
  db.run(`
    CREATE TABLE IF NOT EXISTS shipment_daily_summary (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      summary_date TEXT NOT NULL UNIQUE,
      total_skus INTEGER DEFAULT 0,
      total_units INTEGER DEFAULT 0,
      urgent_skus INTEGER DEFAULT 0,
      low_inventory_fee_skus INTEGER DEFAULT 0,
      excess_skus INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 7. daily_snapshots: トレンド蓄積用 ---
  db.run(`
    CREATE TABLE IF NOT EXISTS daily_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_date TEXT NOT NULL,
      amazon_sku TEXT NOT NULL,
      fba_available INTEGER DEFAULT 0,
      fba_inbound INTEGER DEFAULT 0,
      days_of_supply REAL,
      units_sold_7d INTEGER DEFAULT 0,
      units_sold_30d INTEGER DEFAULT 0,
      sales_rank INTEGER,
      your_price REAL,
      featured_offer_price REAL,
      UNIQUE(snapshot_date, amazon_sku)
    )
  `);

  // --- 8. settings: 設定値 ---
  db.run(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // デフォルト設定を投入
  const defaults = [
    ['target_days_high_volume_small', '40'],
    ['target_days_high_volume_large', '30'],
    ['target_days_medium', '35'],
    ['target_days_low_volume_small', '120'],
    ['target_days_low_volume_large', '60'],
    ['target_days_seasonal', '50'],
    ['high_volume_threshold', '100'],
    ['low_volume_threshold', '20'],
    ['small_volume_cm3', '500'],
    ['large_volume_cm3', '5000'],
    ['weekday_boost_thu_fri', '1.5'],
    ['low_inventory_fee_threshold_days', '14'],
    ['excess_inventory_dos_threshold', '90'],
    ['trend_surge_ratio', '2.0'],
    ['trend_stop_ratio', '0.3'],
    ['cart_alert_level2_ratio', '0.8'],
    ['cart_alert_level3_ratio', '0.5'],
    ['missing_bsr_threshold', '5000'],
  ];
  for (const [key, value] of defaults) {
    db.run(`INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)`, [key, value]);
  }

  saveToFile();
  console.log('[FBA-DB] 初期化完了 — テーブル8個');
}

// ===== SP-APIレポートデータの一括保存 =====

/**
 * PLANNING_DATAレポートの結果をdaily_snapshotsに保存し、
 * shipment_plan_items用のデータとして返す
 */
export function savePlanningData(rows, snapshotDate) {
  const today = snapshotDate || new Date().toISOString().slice(0, 10);

  db.run('BEGIN TRANSACTION');
  try {
    for (const row of rows) {
      // daily_snapshots に保存
      db.run(`
        INSERT OR REPLACE INTO daily_snapshots
          (snapshot_date, amazon_sku, fba_available, fba_inbound, days_of_supply,
           units_sold_7d, units_sold_30d, sales_rank, your_price, featured_offer_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        today,
        row.sku || row['merchant-sku'] || '',
        parseInt(row['available'] || row['afn-fulfillable-quantity'] || 0),
        parseInt(row['inbound-shipped'] || 0) + parseInt(row['inbound-working'] || 0) + parseInt(row['inbound-received'] || 0),
        parseFloat(row['days-of-supply'] || 0),
        parseInt(row['units-shipped-t7'] || 0),
        parseInt(row['units-shipped-t30'] || 0),
        parseInt(row['sales-rank'] || 0),
        parseFloat(row['your-price'] || 0),
        parseFloat(row['featuredoffer-price'] || 0),
      ]);
    }
    db.run('COMMIT');
    saveToFile();
    return rows.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

/**
 * SKUマッピングを一括更新（スプシ同期）
 */
export function upsertSkuMappings(mappings) {
  db.run('BEGIN TRANSACTION');
  try {
    for (const m of mappings) {
      db.run(`
        INSERT INTO sku_mapping (amazon_sku, asin, product_name, ne_code, logizard_code, fnsku, jan, is_set, set_components, per_unit_volume, storage_type, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(amazon_sku) DO UPDATE SET
          asin=excluded.asin, product_name=excluded.product_name,
          ne_code=excluded.ne_code, logizard_code=excluded.logizard_code,
          fnsku=excluded.fnsku, jan=excluded.jan,
          is_set=excluded.is_set, set_components=excluded.set_components,
          per_unit_volume=excluded.per_unit_volume, storage_type=excluded.storage_type,
          updated_at=datetime('now','localtime')
      `, [
        m.amazon_sku, m.asin || null, m.product_name || null,
        m.ne_code || null, m.logizard_code || null, m.fnsku || null, m.jan || null,
        m.is_set ? 1 : 0, m.set_components ? JSON.stringify(m.set_components) : null,
        m.per_unit_volume || null, m.storage_type || null,
      ]);
    }
    db.run('COMMIT');
    saveToFile();
    return mappings.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

/**
 * ロジザードCSVデータの上書き保存
 */
export function replaceWarehouseInventory(items) {
  db.run('BEGIN TRANSACTION');
  try {
    db.run('DELETE FROM warehouse_inventory');
    for (const item of items) {
      db.run(`
        INSERT INTO warehouse_inventory (logizard_code, product_name, location, quantity, expiry_date, lot_no, is_y_location)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `, [
        item.logizard_code, item.product_name || null, item.location || null,
        parseInt(item.quantity || 0), item.expiry_date || null, item.lot_no || null,
        item.is_y_location ? 1 : 0,
      ]);
    }
    db.run('COMMIT');
    saveToFile();
    return items.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

// ===== 読み取り =====

export function getSkuMappings() {
  return queryAll('SELECT * FROM sku_mapping ORDER BY amazon_sku');
}

export function getSkuMapping(amazonSku) {
  return queryOne('SELECT * FROM sku_mapping WHERE amazon_sku = ?', [amazonSku]);
}

export function getSkuExceptions() {
  return queryAll('SELECT * FROM sku_exceptions ORDER BY amazon_sku');
}

export function getWarehouseInventory() {
  return queryAll('SELECT * FROM warehouse_inventory ORDER BY logizard_code');
}

export function getWarehouseQtyByCode(logizardCode) {
  const row = queryOne(
    'SELECT SUM(quantity) as total FROM warehouse_inventory WHERE logizard_code = ?',
    [logizardCode]
  );
  return row?.total || 0;
}

export function getSettings() {
  const rows = queryAll('SELECT key, value FROM settings');
  const obj = {};
  for (const r of rows) obj[r.key] = r.value;
  return obj;
}

export function getSetting(key) {
  const row = queryOne('SELECT value FROM settings WHERE key = ?', [key]);
  return row?.value || null;
}

export function updateSetting(key, value) {
  db.run(`INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now','localtime'))`, [key, value]);
  saveToFile();
}

export function getDailySnapshots(sku, days = 30) {
  return queryAll(
    'SELECT * FROM daily_snapshots WHERE amazon_sku = ? ORDER BY snapshot_date DESC LIMIT ?',
    [sku, days]
  );
}

export function getLatestSnapshots() {
  const latestDate = queryOne('SELECT MAX(snapshot_date) as d FROM daily_snapshots');
  if (!latestDate?.d) return [];
  return queryAll('SELECT * FROM daily_snapshots WHERE snapshot_date = ?', [latestDate.d]);
}

// ===== 納品計画 =====

export function createShipmentPlan(planDate, items) {
  db.run('BEGIN TRANSACTION');
  try {
    db.run(`
      INSERT INTO shipment_plans (plan_date, total_skus, total_units)
      VALUES (?, ?, ?)
    `, [planDate, items.length, items.reduce((s, i) => s + (i.adjusted_qty || i.recommended_qty || 0), 0)]);

    const planId = queryOne('SELECT last_insert_rowid() as id').id;

    for (const item of items) {
      db.run(`
        INSERT INTO shipment_plan_items
          (plan_id, amazon_sku, asin, product_name, recommended_qty, adjusted_qty,
           reason, urgency_score, days_of_supply, fba_available, fba_inbound,
           warehouse_qty, alert_type, alert_message, your_price, featured_offer_price,
           lowest_price, sales_rank, units_sold_7d, units_sold_30d, units_sold_60d, units_sold_90d)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        planId, item.amazon_sku, item.asin || null, item.product_name || null,
        item.recommended_qty || 0, item.adjusted_qty || item.recommended_qty || 0,
        item.reason || null, item.urgency_score || 0, item.days_of_supply || null,
        item.fba_available || 0, item.fba_inbound || 0, item.warehouse_qty || 0,
        item.alert_type || null, item.alert_message || null,
        item.your_price || null, item.featured_offer_price || null,
        item.lowest_price || null, item.sales_rank || null,
        item.units_sold_7d || 0, item.units_sold_30d || 0,
        item.units_sold_60d || 0, item.units_sold_90d || 0,
      ]);
    }

    db.run('COMMIT');
    saveToFile();
    return planId;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

export function getShipmentPlans(limit = 30) {
  return queryAll('SELECT * FROM shipment_plans ORDER BY created_at DESC LIMIT ?', [limit]);
}

export function getShipmentPlanItems(planId) {
  return queryAll('SELECT * FROM shipment_plan_items WHERE plan_id = ? ORDER BY urgency_score DESC', [planId]);
}

export function updateShipmentPlanStatus(planId, status) {
  const col = status === 'confirmed' ? 'confirmed_at' : status === 'shipped' ? 'shipped_at' : status === 'completed' ? 'completed_at' : null;
  db.run(`UPDATE shipment_plans SET status = ? ${col ? `, ${col} = datetime('now','localtime')` : ''} WHERE id = ?`, [status, planId]);
  saveToFile();
}

// ===== SKU例外マスタ =====

export function upsertSkuException(amazonSku, exceptionType, keepMinimumQty, reason) {
  db.run(`
    INSERT INTO sku_exceptions (amazon_sku, exception_type, keep_minimum_qty, reason, updated_at)
    VALUES (?, ?, ?, ?, datetime('now','localtime'))
    ON CONFLICT(amazon_sku) DO UPDATE SET
      exception_type=excluded.exception_type, keep_minimum_qty=excluded.keep_minimum_qty,
      reason=excluded.reason, updated_at=datetime('now','localtime')
  `, [amazonSku, exceptionType, keepMinimumQty || 0, reason || null]);
  saveToFile();
}

export function deleteSkuException(amazonSku) {
  db.run('DELETE FROM sku_exceptions WHERE amazon_sku = ?', [amazonSku]);
  saveToFile();
}
