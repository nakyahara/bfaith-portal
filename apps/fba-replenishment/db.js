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
      non_fba_sales_7d INTEGER DEFAULT 0,
      non_fba_sales_30d INTEGER DEFAULT 0,
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // マイグレーション: sku_mapping新カラム
  const skuCols = queryAll('PRAGMA table_info(sku_mapping)').map(r => r.name);
  for (const col of ['non_fba_sales_7d', 'non_fba_sales_30d']) {
    if (!skuCols.includes(col)) {
      db.run(`ALTER TABLE sku_mapping ADD COLUMN ${col} INTEGER DEFAULT 0`);
    }
  }

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
      block TEXT,
      quantity INTEGER DEFAULT 0,
      reserved INTEGER DEFAULT 0,
      available_qty INTEGER DEFAULT 0,
      expiry_date TEXT,
      barcode TEXT,
      lot_no TEXT,
      is_y_location INTEGER DEFAULT 0,
      uploaded_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // マイグレーション: warehouse_inventory新カラム
  const whCols = queryAll('PRAGMA table_info(warehouse_inventory)').map(r => r.name);
  for (const [col, type] of [['block','TEXT'],['reserved','INTEGER DEFAULT 0'],['available_qty','INTEGER DEFAULT 0'],['barcode','TEXT'],['last_arrival_date','TEXT'],['location_biz_type','TEXT'],['block_alloc_order','INTEGER DEFAULT 9999'],['biz_priority','TEXT']]) {
    if (!whCols.includes(col)) {
      db.run(`ALTER TABLE warehouse_inventory ADD COLUMN ${col} ${type}`);
    }
  }

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
      product_name TEXT,
      fba_available INTEGER DEFAULT 0,
      fba_inbound_working INTEGER DEFAULT 0,
      fba_inbound_shipped INTEGER DEFAULT 0,
      fba_inbound_received INTEGER DEFAULT 0,
      working_first_seen TEXT,
      days_of_supply REAL,
      units_sold_7d INTEGER DEFAULT 0,
      units_sold_30d INTEGER DEFAULT 0,
      sales_rank INTEGER,
      your_price REAL,
      featured_offer_price REAL,
      UNIQUE(snapshot_date, amazon_sku)
    )
  `);

  // マイグレーション: 既存テーブルに新カラム追加
  const snapCols = queryAll('PRAGMA table_info(daily_snapshots)').map(r => r.name);
  for (const col of ['fba_inbound_working', 'fba_inbound_shipped', 'fba_inbound_received']) {
    if (!snapCols.includes(col)) {
      db.run(`ALTER TABLE daily_snapshots ADD COLUMN ${col} INTEGER DEFAULT 0`);
    }
  }
  if (!snapCols.includes('working_first_seen')) {
    db.run(`ALTER TABLE daily_snapshots ADD COLUMN working_first_seen TEXT`);
  }
  if (!snapCols.includes('product_name')) {
    db.run(`ALTER TABLE daily_snapshots ADD COLUMN product_name TEXT`);
  }

  // --- 8. non_fba_sales_snapshots: 他CH売上の日次スナップショット（60日保持） ---
  db.run(`
    CREATE TABLE IF NOT EXISTS non_fba_sales_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_date TEXT NOT NULL,
      amazon_sku TEXT NOT NULL,
      non_fba_sales_7d INTEGER DEFAULT 0,
      non_fba_sales_30d INTEGER DEFAULT 0,
      UNIQUE(snapshot_date, amazon_sku)
    )
  `);

  // --- 9. stockout_hidden: FBA欠品リストの非表示SKU ---
  db.run(`
    CREATE TABLE IF NOT EXISTS stockout_hidden (
      amazon_sku TEXT PRIMARY KEY,
      reason TEXT,
      hidden_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 10. new_product_hidden: 新規商品リストの非表示SKU ---
  db.run(`
    CREATE TABLE IF NOT EXISTS new_product_hidden (
      amazon_sku TEXT PRIMARY KEY,
      reason TEXT,
      hidden_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 11. shipment_draft: 納品作業ドラフト（1つだけ保持） ---
  db.run(`
    CREATE TABLE IF NOT EXISTS shipment_draft (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      amazon_sku TEXT NOT NULL,
      ship_qty INTEGER DEFAULT 0,
      checked INTEGER DEFAULT 0,
      from_stockout INTEGER DEFAULT 0,
      UNIQUE(amazon_sku)
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS shipment_draft_meta (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `);

  // --- 11. settings: 設定値 ---
  db.run(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // デフォルト設定を投入
  const defaults = [
    // 目標日数（推奨に上がった時に何日分送るか）
    ['target_days_high_volume_small', '40'],
    ['target_days_high_volume_large', '30'],
    ['target_days_medium', '35'],
    ['target_days_low_volume_small', '180'],  // 低回転小型: 半年分
    ['target_days_low_volume_large', '90'],    // 低回転大型: 3ヶ月分
    ['target_days_seasonal', '50'],
    // 発注点（供給日数がこれを下回ったら推奨に上がる）
    ['reorder_point_high_volume', '21'],  // 高回転: 売れるのが速いので早めにトリガー
    ['reorder_point_medium', '21'],
    ['reorder_point_low_volume', '14'],  // 低回転: ゆっくり売れるので短めでOK
    ['reorder_point_seasonal', '21'],
    // 日次SKU上限
    ['daily_sku_limit', '100'],
    // 販売量閾値
    ['high_volume_threshold', '100'],
    ['low_volume_threshold', '20'],
    ['small_volume_cm3', '500'],
    ['large_volume_cm3', '5000'],
    ['fba_weekly_threshold', '10'],  // 7日売上がこれ以上→低在庫手数料リスク→中回転扱い
    ['min_shipment_cover_days', '7'],  // 推奨数がこの日数分に満たない場合は除外（FBA在庫0は例外）
    ['round_unit', '5'],              // 丸め単位（個）
    ['round_threshold', '20'],        // この数量を超えたら丸め適用
    ['location_adjust_pct', '10'],    // ロケ補正の許容範囲（%）
    ['weekday_boost_thu_fri', '1.5'],
    ['low_inventory_fee_threshold_days', '14'],
    ['excess_inventory_dos_threshold', '90'],
    ['trend_surge_ratio', '2.0'],
    ['trend_stop_ratio', '0.3'],
    ['cart_alert_level2_ratio', '0.8'],
    ['cart_alert_level3_ratio', '0.5'],
    ['missing_bsr_threshold', '5000'],
    ['working_expiry_days', '7'],
    ['non_fba_reserve_days', '60'],
    // 納品プラン設定
    ['inbound_ship_from_name', ''],
    ['inbound_ship_from_address1', ''],
    ['inbound_ship_from_address2', ''],
    ['inbound_ship_from_city', ''],
    ['inbound_ship_from_state', ''],
    ['inbound_ship_from_postal_code', ''],
    ['inbound_ship_from_phone', ''],
    ['inbound_ship_from_country', 'JP'],
    ['inbound_label_owner', 'AMAZON'],
    ['inbound_prep_owner', 'NONE'],
  ];
  for (const [key, value] of defaults) {
    db.run(`INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)`, [key, value]);
  }

  // マイグレーション: 旧デフォルト値を新デフォルト値に更新
  const migrations = [
    ['target_days_low_volume_small', '120', '180'],  // 120日→180日（半年分）
    ['target_days_low_volume_large', '60', '90'],     // 60日→90日（3ヶ月分）
    ['non_fba_reserve_days', '14', '60'],             // 14日→60日（他CH確保を強化）
    ['reorder_point_high_volume', '14', '21'],        // 高回転: 14日→21日（売れるのが速いので早めにトリガー）
    ['reorder_point_low_volume', '30', '14'],         // 低回転: 30日→14日（ゆっくり売れるので短めでOK）
    ['inbound_prep_owner', 'SELLER', 'NONE'],          // prep不要な商品でエラーになるためNONEに変更
  ];
  for (const [key, oldVal, newVal] of migrations) {
    db.run(`UPDATE settings SET value = ?, updated_at = datetime('now','localtime') WHERE key = ? AND value = ?`, [newVal, key, oldVal]);
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
      const sku = row.sku || '';
      const workingQty = row.fba_inbound_working || 0;

      // working_first_seen の自動判定:
      // - 今回 working > 0 で、前回も working > 0 → 前回の first_seen を引き継ぐ
      // - 今回 working > 0 で、前回は working = 0 → 今日が初検知
      // - 今回 working = 0 → null
      let workingFirstSeen = null;
      if (workingQty > 0) {
        const prev = queryOne(
          `SELECT working_first_seen, fba_inbound_working
           FROM daily_snapshots
           WHERE amazon_sku = ? AND snapshot_date < ?
           ORDER BY snapshot_date DESC LIMIT 1`,
          [sku, today]
        );
        if (prev && prev.fba_inbound_working > 0 && prev.working_first_seen) {
          // 前回もworking中 → 初検知日を引き継ぐ
          workingFirstSeen = prev.working_first_seen;
        } else {
          // 新たにworkingが発生 → 今日が初検知
          workingFirstSeen = today;
        }
      }

      db.run(`
        INSERT OR REPLACE INTO daily_snapshots
          (snapshot_date, amazon_sku, product_name, fba_available,
           fba_inbound_working, fba_inbound_shipped, fba_inbound_received,
           working_first_seen,
           days_of_supply, units_sold_7d, units_sold_30d,
           sales_rank, your_price, featured_offer_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        today,
        sku,
        row.product_name || '',
        row.fba_available || 0,
        workingQty,
        row.fba_inbound_shipped || 0,
        row.fba_inbound_received || 0,
        workingFirstSeen,
        row.days_of_supply || 0,
        row.units_sold_7d || 0,
        row.units_sold_30d || 0,
        row.sales_rank || 0,
        row.your_price || 0,
        row.featured_offer_price || 0,
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
        INSERT INTO sku_mapping (amazon_sku, asin, product_name, ne_code, logizard_code, fnsku, jan, is_set, set_components, per_unit_volume, storage_type, non_fba_sales_7d, non_fba_sales_30d, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(amazon_sku) DO UPDATE SET
          asin=excluded.asin, product_name=excluded.product_name,
          ne_code=excluded.ne_code, logizard_code=excluded.logizard_code,
          fnsku=excluded.fnsku, jan=excluded.jan,
          is_set=excluded.is_set, set_components=excluded.set_components,
          per_unit_volume=excluded.per_unit_volume, storage_type=excluded.storage_type,
          non_fba_sales_7d=excluded.non_fba_sales_7d, non_fba_sales_30d=excluded.non_fba_sales_30d,
          updated_at=datetime('now','localtime')
      `, [
        m.amazon_sku, m.asin || null, m.product_name || null,
        m.ne_code || null, m.logizard_code || null, m.fnsku || null, m.jan || null,
        m.is_set ? 1 : 0, m.set_components ? JSON.stringify(m.set_components) : null,
        m.per_unit_volume || null, m.storage_type || null,
        m.non_fba_sales_7d || 0, m.non_fba_sales_30d || 0,
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
        INSERT INTO warehouse_inventory
          (logizard_code, product_name, location, block, quantity, reserved, available_qty,
           expiry_date, barcode, lot_no, is_y_location, last_arrival_date,
           location_biz_type, block_alloc_order, biz_priority)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        item.logizard_code, item.product_name || null, item.location || null,
        item.block || null,
        parseInt(item.quantity || 0), parseInt(item.reserved || 0), parseInt(item.available_qty || 0),
        item.expiry_date || null, item.barcode || null, item.lot_no || null,
        item.is_y_location ? 1 : 0,
        item.last_arrival_date || null,
        item.location_biz_type || null,
        parseInt(item.block_alloc_order || 9999),
        item.biz_priority || null,
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
    'SELECT SUM(quantity) as total, SUM(available_qty) as available FROM warehouse_inventory WHERE logizard_code = ? AND is_y_location = 0',
    [logizardCode]
  );
  return { total: row?.total || 0, available: row?.available || 0 };
}

export function getWarehouseYQtyByCode(logizardCode) {
  const row = queryOne(
    'SELECT SUM(quantity) as total FROM warehouse_inventory WHERE logizard_code = ? AND is_y_location = 1',
    [logizardCode]
  );
  return row?.total || 0;
}

// ロケーション在庫を引当優先順で取得（卸し→通販、ブロック引当順昇順、ロケ昇順）
export function getWarehouseLocationsByCode(logizardCode) {
  return queryAll(`
    SELECT location, block, available_qty, location_biz_type, block_alloc_order, expiry_date
    FROM warehouse_inventory
    WHERE logizard_code = ? AND is_y_location = 0 AND available_qty > 0
    ORDER BY
      CASE WHEN location_biz_type = '卸し' THEN 0
           WHEN location_biz_type = '通販' THEN 1
           ELSE 2 END,
      block_alloc_order ASC,
      location ASC
  `, [logizardCode]);
}

/**
 * 商品コード別の在庫サマリー（倉庫在庫 + Yロケ在庫）
 */
export function getWarehouseSummary() {
  return queryAll(`
    SELECT
      logizard_code,
      MAX(product_name) as product_name,
      SUM(CASE WHEN is_y_location = 0 THEN quantity ELSE 0 END) as warehouse_qty,
      SUM(CASE WHEN is_y_location = 0 THEN available_qty ELSE 0 END) as warehouse_available,
      SUM(CASE WHEN is_y_location = 1 THEN quantity ELSE 0 END) as y_location_qty,
      MIN(CASE WHEN expiry_date != '' AND expiry_date IS NOT NULL THEN expiry_date END) as earliest_expiry,
      MAX(last_arrival_date) as last_arrival_date,
      COUNT(DISTINCT location) as location_count
    FROM warehouse_inventory
    GROUP BY logizard_code
    ORDER BY logizard_code
  `);
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

// ===== 他CH売上スナップショット =====

/**
 * SKUマッピング同期時に他CH売上を日次スナップショットとして保存（UPSERT）
 * 手動同期時はその日のデータを上書き
 */
export function saveNonFbaSalesSnapshot(mappings, snapshotDate) {
  const today = snapshotDate || new Date().toISOString().slice(0, 10);

  db.run('BEGIN TRANSACTION');
  try {
    for (const m of mappings) {
      if (!m.amazon_sku) continue;
      db.run(`
        INSERT INTO non_fba_sales_snapshots (snapshot_date, amazon_sku, non_fba_sales_7d, non_fba_sales_30d)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(snapshot_date, amazon_sku) DO UPDATE SET
          non_fba_sales_7d = excluded.non_fba_sales_7d,
          non_fba_sales_30d = excluded.non_fba_sales_30d
      `, [today, m.amazon_sku, m.non_fba_sales_7d || 0, m.non_fba_sales_30d || 0]);
    }

    // 60日超のデータを削除
    db.run(`DELETE FROM non_fba_sales_snapshots WHERE snapshot_date < date('now', '-60 days')`);

    db.run('COMMIT');
    saveToFile();
    return mappings.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

/**
 * SKUの他CH売上の60日間最大値を取得
 * 欠品期間の0を無視して、実力値を返す
 */
export function getNonFbaMax60d(amazonSku) {
  const row = queryOne(`
    SELECT MAX(non_fba_sales_30d) as max_30d, MAX(non_fba_sales_7d) as max_7d
    FROM non_fba_sales_snapshots
    WHERE amazon_sku = ? AND snapshot_date >= date('now', '-60 days')
  `, [amazonSku]);
  return { max_30d: row?.max_30d || 0, max_7d: row?.max_7d || 0 };
}

/**
 * 全SKUの60日間最大値を一括取得（推奨リスト生成用）
 */
export function getAllNonFbaMax60d() {
  return queryAll(`
    SELECT amazon_sku, MAX(non_fba_sales_30d) as max_30d, MAX(non_fba_sales_7d) as max_7d
    FROM non_fba_sales_snapshots
    WHERE snapshot_date >= date('now', '-60 days')
    GROUP BY amazon_sku
  `);
}

// ===== FBA欠品 非表示管理 =====

export function getStockoutHidden() {
  return queryAll('SELECT * FROM stockout_hidden ORDER BY hidden_at DESC');
}

export function hideStockoutSku(amazonSku, reason) {
  db.run(`INSERT OR REPLACE INTO stockout_hidden (amazon_sku, reason, hidden_at) VALUES (?, ?, datetime('now','localtime'))`,
    [amazonSku, reason || null]);
  saveToFile();
}

export function unhideStockoutSku(amazonSku) {
  db.run('DELETE FROM stockout_hidden WHERE amazon_sku = ?', [amazonSku]);
  saveToFile();
}

export function hideStockoutSkuBulk(skus, reason) {
  db.run('BEGIN TRANSACTION');
  try {
    for (const sku of skus) {
      db.run(`INSERT OR REPLACE INTO stockout_hidden (amazon_sku, reason, hidden_at) VALUES (?, ?, datetime('now','localtime'))`,
        [sku, reason || null]);
    }
    db.run('COMMIT');
    saveToFile();
    return skus.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

// ===== 新規商品 非表示管理 =====

export function getNewProductHidden() {
  return queryAll('SELECT * FROM new_product_hidden ORDER BY hidden_at DESC');
}

export function hideNewProductSkuBulk(skus, reason) {
  db.run('BEGIN TRANSACTION');
  try {
    for (const sku of skus) {
      db.run(`INSERT OR REPLACE INTO new_product_hidden (amazon_sku, reason, hidden_at) VALUES (?, ?, datetime('now','localtime'))`,
        [sku, reason || null]);
    }
    db.run('COMMIT');
    saveToFile();
    return skus.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

export function unhideNewProductSku(amazonSku) {
  db.run('DELETE FROM new_product_hidden WHERE amazon_sku = ?', [amazonSku]);
  saveToFile();
}

// ===== 納品作業ドラフト =====

export function saveDraft(items, memo) {
  db.run('BEGIN TRANSACTION');
  try {
    db.run('DELETE FROM shipment_draft');
    for (const item of items) {
      db.run(`INSERT INTO shipment_draft (amazon_sku, ship_qty, checked, from_stockout) VALUES (?, ?, ?, ?)`,
        [item.amazon_sku, item.ship_qty || 0, item.checked ? 1 : 0, item.from_stockout ? 1 : 0]);
    }
    // メタ情報を保存
    db.run(`INSERT OR REPLACE INTO shipment_draft_meta (key, value) VALUES ('saved_at', datetime('now','localtime'))`);
    db.run(`INSERT OR REPLACE INTO shipment_draft_meta (key, value) VALUES ('memo', ?)`, [memo || '']);
    db.run(`INSERT OR REPLACE INTO shipment_draft_meta (key, value) VALUES ('item_count', ?)`, [String(items.length)]);
    db.run(`INSERT OR REPLACE INTO shipment_draft_meta (key, value) VALUES ('total_qty', ?)`,
      [String(items.reduce((s, i) => s + (i.ship_qty || 0), 0))]);
    db.run('COMMIT');
    saveToFile();
    return items.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

export function getDraft() {
  const items = queryAll('SELECT * FROM shipment_draft ORDER BY amazon_sku');
  const metaRows = queryAll('SELECT * FROM shipment_draft_meta');
  const meta = {};
  for (const r of metaRows) meta[r.key] = r.value;
  return { items, meta };
}

export function clearDraft() {
  db.run('DELETE FROM shipment_draft');
  db.run('DELETE FROM shipment_draft_meta');
  saveToFile();
}
