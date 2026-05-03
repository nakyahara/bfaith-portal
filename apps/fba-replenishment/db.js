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
  // FBA倉庫内在庫の完全な4分類化 (月末棚卸しツール定義と一致):
  //   fba_warehouse = fba_available + fba_fc_transfer + fba_fc_processing + fba_customer_order
  // RESTOCK レポート (GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT) から取得する。
  for (const col of ['fba_fc_transfer', 'fba_fc_processing', 'fba_customer_order']) {
    if (!snapCols.includes(col)) {
      db.run(`ALTER TABLE daily_snapshots ADD COLUMN ${col} INTEGER DEFAULT 0`);
    }
  }
  // 販売不可在庫 (D-1b inv_daily_detail で fba_unfulfillable_qty として参照)
  if (!snapCols.includes('fba_unfulfillable')) {
    db.run('ALTER TABLE daily_snapshots ADD COLUMN fba_unfulfillable INTEGER DEFAULT 0');
  }

  // --- US用 daily_snapshots (NA リージョン、JP テーブルとの SKU衝突回避のため別テーブル) ---
  db.run(`
    CREATE TABLE IF NOT EXISTS daily_snapshots_us (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_date TEXT NOT NULL,
      amazon_sku TEXT NOT NULL,
      product_name TEXT,
      fba_available INTEGER DEFAULT 0,
      fba_inbound_working INTEGER DEFAULT 0,
      fba_inbound_shipped INTEGER DEFAULT 0,
      fba_inbound_received INTEGER DEFAULT 0,
      fba_fc_transfer INTEGER DEFAULT 0,
      fba_fc_processing INTEGER DEFAULT 0,
      fba_customer_order INTEGER DEFAULT 0,
      fba_unfulfillable INTEGER DEFAULT 0,
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
  db.run('CREATE INDEX IF NOT EXISTS idx_dailysnap_us_date ON daily_snapshots_us(snapshot_date)');

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
    ['working_expiry_days', '3'],
    ['non_fba_reserve_days', '60'],
    // 長期欠品SKUをFBA欠品タブの「長期(復活余地)」に分類する Amazon推奨数の閾値
    ['oos_amazon_reco_threshold', '11'],
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
    ['working_expiry_days', '7', '3'],                  // 準備中在庫の有効日数: 7日→3日（放置プラン判定を早める）
  ];
  for (const [key, oldVal, newVal] of migrations) {
    db.run(`UPDATE settings SET value = ?, updated_at = datetime('now','localtime') WHERE key = ? AND value = ?`, [newVal, key, oldVal]);
  }

  // --- 12. provisional_items: Amazon仮確定 ---
  db.run(`
    CREATE TABLE IF NOT EXISTS provisional_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      amazon_sku TEXT NOT NULL UNIQUE,
      product_name TEXT,
      fnsku TEXT,
      ship_qty INTEGER DEFAULT 0,
      fba_available INTEGER DEFAULT 0,
      units_sold_7d INTEGER DEFAULT 0,
      units_sold_30d INTEGER DEFAULT 0,
      warehouse_raw INTEGER DEFAULT 0,
      recommended_qty INTEGER DEFAULT 0,
      urgency_score REAL DEFAULT 0,
      set_components TEXT,
      asin TEXT,
      expiry_date TEXT,
      created_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // マイグレーション: provisional_items新カラム
  const provCols = queryAll('PRAGMA table_info(provisional_items)').map(r => r.name);
  if (!provCols.includes('asin')) db.run('ALTER TABLE provisional_items ADD COLUMN asin TEXT');
  if (!provCols.includes('expiry_date')) db.run('ALTER TABLE provisional_items ADD COLUMN expiry_date TEXT');

  db.run(`
    CREATE TABLE IF NOT EXISTS provisional_meta (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      key TEXT UNIQUE NOT NULL,
      value TEXT
    )
  `);

  // --- 13. export_history: 出力履歴 ---
  db.run(`
    CREATE TABLE IF NOT EXISTS export_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      filename TEXT NOT NULL,
      item_count INTEGER DEFAULT 0,
      total_qty INTEGER DEFAULT 0,
      file_data BLOB,
      created_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 14. restock_latest: RESTOCKレポート最新1回分（発注判定の主軸データソース） ---
  db.run(`
    CREATE TABLE IF NOT EXISTS restock_latest (
      amazon_sku TEXT PRIMARY KEY,
      fnsku TEXT,
      asin TEXT,
      product_name TEXT,
      fba_available INTEGER DEFAULT 0,
      fba_inbound_working INTEGER DEFAULT 0,
      fba_inbound_shipped INTEGER DEFAULT 0,
      fba_inbound_received INTEGER DEFAULT 0,
      fba_unfulfillable INTEGER DEFAULT 0,
      units_sold_30d INTEGER DEFAULT 0,
      amazon_recommended_qty INTEGER,
      amazon_recommended_date TEXT,
      alert_type TEXT,
      your_price REAL,
      days_of_supply REAL,
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 15. planning_latest: PLANNINGレポート最新1回分（補助、取得失敗許容） ---
  db.run(`
    CREATE TABLE IF NOT EXISTS planning_latest (
      amazon_sku TEXT PRIMARY KEY,
      units_sold_7d INTEGER,
      units_sold_60d INTEGER,
      units_sold_90d INTEGER,
      sales_7d REAL,
      sales_30d REAL,
      sales_60d REAL,
      sales_90d REAL,
      featured_offer_price REAL,
      lowest_price REAL,
      sales_rank INTEGER,
      is_seasonal TEXT,
      season_name TEXT,
      short_term_dos REAL,
      long_term_dos REAL,
      low_inv_fee_applied TEXT,
      low_inv_fee_exempt TEXT,
      estimated_excess_qty INTEGER,
      estimated_storage_cost REAL,
      per_unit_volume REAL,
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  // --- 16. ever_seen_skus: 過去にFBAで観測したSKU（新規商品タブの判定用） ---
  // ※ユーザー方針により初期は空スタート。RESTOCK/PLANNING取得毎に追記していく
  db.run(`
    CREATE TABLE IF NOT EXISTS ever_seen_skus (
      amazon_sku TEXT PRIMARY KEY,
      first_seen_at TEXT DEFAULT (datetime('now','localtime')),
      last_seen_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);

  saveToFile();
  console.log('[FBA-DB] 初期化完了');
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

      // 在庫列のソース優先順位:
      //   月末棚卸しツールは RESTOCK レポート (4列+3列) で fba_warehouse を計算するので、
      //   日次も RESTOCK を source of truth とする。
      //   → PLANNING の ON CONFLICT DO UPDATE では在庫7列 (4既存 + 3新規) を除外し、
      //     PLANNING 固有列 (sales/price/days_of_supply/sales_rank/working_first_seen) のみ更新。
      //   新規 INSERT 時はデフォルト 0 が入るが、後続/前段の saveRestockInventoryToDailySnapshot で上書きされる。
      //   (snapshot-fba-stock.js は RESTOCK→PLANNING の順序で実行)
      db.run(`
        INSERT INTO daily_snapshots
          (snapshot_date, amazon_sku, product_name, fba_available,
           fba_inbound_working, fba_inbound_shipped, fba_inbound_received,
           working_first_seen,
           days_of_supply, units_sold_7d, units_sold_30d,
           sales_rank, your_price, featured_offer_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(snapshot_date, amazon_sku) DO UPDATE SET
          product_name = excluded.product_name,
          working_first_seen = excluded.working_first_seen,
          days_of_supply = excluded.days_of_supply,
          units_sold_7d = excluded.units_sold_7d,
          units_sold_30d = excluded.units_sold_30d,
          sales_rank = excluded.sales_rank,
          your_price = excluded.your_price,
          featured_offer_price = excluded.featured_offer_price
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
 * RESTOCK レポートから FBA 在庫の追加3区分 (FC移管中・FC処理中・出荷待ち) を
 * daily_snapshots に書き込む。月末棚卸しツールの fba_warehouse 計算 (4列合算)
 * と一致させるための補完。
 *
 * - PLANNING で先に行を作成済み (savePlanningData) なら、その行の追加列を UPDATE
 * - PLANNING に無い SKU でも RESTOCK にあれば INSERT (sales/price 列は 0/null)
 * - 既存の fba_available/fba_inbound_* も RESTOCK の値で上書き (定義一致のため)
 *
 * rows: normalizeRestockRow() の戻り値の配列
 */
export function saveRestockInventoryToDailySnapshot(rows, snapshotDate) {
  if (!rows || rows.length === 0) return { updated: 0, inserted: 0 };
  const today = snapshotDate || new Date().toISOString().slice(0, 10);

  let updated = 0, inserted = 0;
  db.run('BEGIN TRANSACTION');
  try {
    for (const row of rows) {
      const sku = row.amazon_sku || '';
      if (!sku) continue;
      const exists = queryOne(
        'SELECT 1 FROM daily_snapshots WHERE snapshot_date = ? AND amazon_sku = ?',
        [today, sku]
      );
      if (exists) {
        db.run(`
          UPDATE daily_snapshots SET
            fba_available = ?,
            fba_inbound_working = ?,
            fba_inbound_shipped = ?,
            fba_inbound_received = ?,
            fba_fc_transfer = ?,
            fba_fc_processing = ?,
            fba_customer_order = ?
          WHERE snapshot_date = ? AND amazon_sku = ?
        `, [
          row.fba_available || 0,
          row.fba_inbound_working || 0,
          row.fba_inbound_shipped || 0,
          row.fba_inbound_received || 0,
          row.fba_fc_transfer || 0,
          row.fba_fc_processing || 0,
          row.fba_customer_order || 0,
          today, sku,
        ]);
        updated++;
      } else {
        // PLANNING に該当 SKU がない場合: RESTOCK 単独で INSERT (sales/price は0)
        db.run(`
          INSERT INTO daily_snapshots
            (snapshot_date, amazon_sku, product_name,
             fba_available, fba_inbound_working, fba_inbound_shipped, fba_inbound_received,
             fba_fc_transfer, fba_fc_processing, fba_customer_order)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          today, sku, row.product_name || '',
          row.fba_available || 0,
          row.fba_inbound_working || 0,
          row.fba_inbound_shipped || 0,
          row.fba_inbound_received || 0,
          row.fba_fc_transfer || 0,
          row.fba_fc_processing || 0,
          row.fba_customer_order || 0,
        ]);
        inserted++;
      }
    }
    db.run('COMMIT');
    saveToFile();
    return { updated, inserted };
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

/**
 * ミニPCから同期されたPLANNINGスナップショットをそのままupsertする。
 *
 * 通常の savePlanningData() と違い:
 *   - Render側DBの前日行を参照して working_first_seen を再計算しない
 *   - payload（ミニPC側で正しく記録された値）をそのまま使う
 *   → Render DBに履歴ギャップがあっても working_first_seen 判定が狂わない
 *
 * rows は getLatestSnapshots() 形式（amazon_sku, working_first_seen を含む）
 */
export function savePlanningDataWithHistory(rows, snapshotDate) {
  if (!rows || rows.length === 0) return 0;
  const date = snapshotDate || rows[0]?.snapshot_date || new Date().toISOString().slice(0, 10);

  db.run('BEGIN TRANSACTION');
  try {
    for (const row of rows) {
      const sku = row.amazon_sku || row.sku || '';
      if (!sku) continue;

      // savePlanningData と同じ理由で ON CONFLICT DO UPDATE。
      // 在庫7列 (4既存 + 3新規) は RESTOCK が source of truth なので PLANNING で上書きしない。
      // PLANNING 固有列のみ更新する。
      db.run(`
        INSERT INTO daily_snapshots
          (snapshot_date, amazon_sku, product_name, fba_available,
           fba_inbound_working, fba_inbound_shipped, fba_inbound_received,
           working_first_seen,
           days_of_supply, units_sold_7d, units_sold_30d,
           sales_rank, your_price, featured_offer_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(snapshot_date, amazon_sku) DO UPDATE SET
          product_name = excluded.product_name,
          working_first_seen = excluded.working_first_seen,
          days_of_supply = excluded.days_of_supply,
          units_sold_7d = excluded.units_sold_7d,
          units_sold_30d = excluded.units_sold_30d,
          sales_rank = excluded.sales_rank,
          your_price = excluded.your_price,
          featured_offer_price = excluded.featured_offer_price
      `, [
        date,
        sku,
        row.product_name || '',
        row.fba_available || 0,
        row.fba_inbound_working || 0,
        row.fba_inbound_shipped || 0,
        row.fba_inbound_received || 0,
        row.working_first_seen || null,
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
 * US 専用 daily_snapshots_us への UPSERT
 * planning rows と restock rows をマージして1回で書き込み (シンプル統合版)
 * RESTOCK が source of truth (4列在庫合算)、PLANNING は補助 (sales/price/days_of_supply)
 *
 * @param {object} params
 * @param {Array} params.planningRows - normalizePlanningRow 後の配列
 * @param {Array} params.restockRows - normalizeRestockRow 後の配列
 * @param {string} params.snapshotDate - YYYY-MM-DD (JST)
 */
export function saveUsDailySnapshots({ planningRows = [], restockRows = [], snapshotDate }) {
  const today = snapshotDate || new Date().toISOString().slice(0, 10);

  // RESTOCK を主軸 (在庫7列の source of truth)
  // PLANNING を sales/price 補完用に Map化
  const planningMap = new Map(planningRows.map(r => [(r.sku || '').toLowerCase(), r]));

  // RESTOCK ベースで全 SKU リスト (RESTOCK にない PLANNING-only SKU は別途処理)
  const restockMap = new Map(restockRows.map(r => [(r.amazon_sku || '').toLowerCase(), r]));
  const allSkus = new Set([...restockMap.keys(), ...planningMap.keys()]);

  let inserted = 0, updated = 0;
  db.run('BEGIN TRANSACTION');
  try {
    for (const sku of allSkus) {
      if (!sku) continue;
      const r = restockMap.get(sku) || {};
      const p = planningMap.get(sku) || {};
      const existing = queryOne('SELECT id FROM daily_snapshots_us WHERE snapshot_date = ? AND amazon_sku = ?', [today, sku]);

      const productName = r.product_name || p.product_name || '';
      const fbaAvailable = r.fba_available ?? p.fba_available ?? 0;
      const inboundWorking = r.fba_inbound_working ?? p.fba_inbound_working ?? 0;
      const inboundShipped = r.fba_inbound_shipped ?? p.fba_inbound_shipped ?? 0;
      const inboundReceived = r.fba_inbound_received ?? p.fba_inbound_received ?? 0;
      const fcTransfer = r.fba_fc_transfer ?? 0;
      const fcProcessing = r.fba_fc_processing ?? 0;
      const customerOrder = r.fba_customer_order ?? 0;
      const unfulfillable = r.fba_unfulfillable ?? p.fba_unfulfillable ?? 0;
      const dos = p.days_of_supply ?? null;
      const sold7d = p.units_sold_7d ?? 0;
      const sold30d = p.units_sold_30d ?? r.units_sold_30d ?? 0;
      const salesRank = p.sales_rank ?? null;
      const yourPrice = p.your_price ?? null;
      const featuredPrice = p.featured_offer_price ?? null;

      if (existing) {
        db.run(`
          UPDATE daily_snapshots_us SET
            product_name=?, fba_available=?, fba_inbound_working=?, fba_inbound_shipped=?, fba_inbound_received=?,
            fba_fc_transfer=?, fba_fc_processing=?, fba_customer_order=?, fba_unfulfillable=?,
            days_of_supply=?, units_sold_7d=?, units_sold_30d=?,
            sales_rank=?, your_price=?, featured_offer_price=?
          WHERE snapshot_date = ? AND amazon_sku = ?
        `, [
          productName, fbaAvailable, inboundWorking, inboundShipped, inboundReceived,
          fcTransfer, fcProcessing, customerOrder, unfulfillable,
          dos, sold7d, sold30d, salesRank, yourPrice, featuredPrice,
          today, sku,
        ]);
        updated++;
      } else {
        db.run(`
          INSERT INTO daily_snapshots_us
            (snapshot_date, amazon_sku, product_name,
             fba_available, fba_inbound_working, fba_inbound_shipped, fba_inbound_received,
             fba_fc_transfer, fba_fc_processing, fba_customer_order, fba_unfulfillable,
             days_of_supply, units_sold_7d, units_sold_30d,
             sales_rank, your_price, featured_offer_price)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          today, sku, productName,
          fbaAvailable, inboundWorking, inboundShipped, inboundReceived,
          fcTransfer, fcProcessing, customerOrder, unfulfillable,
          dos, sold7d, sold30d, salesRank, yourPrice, featuredPrice,
        ]);
        inserted++;
      }
    }
    db.run('COMMIT');
    saveToFile();
    return { inserted, updated };
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
        INSERT INTO sku_mapping (amazon_sku, asin, product_name, ne_code, logizard_code, jan, is_set, set_components, per_unit_volume, storage_type, non_fba_sales_7d, non_fba_sales_30d, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(amazon_sku) DO UPDATE SET
          asin=excluded.asin, product_name=excluded.product_name,
          ne_code=excluded.ne_code, logizard_code=excluded.logizard_code,
          jan=excluded.jan,
          is_set=excluded.is_set, set_components=excluded.set_components,
          per_unit_volume=excluded.per_unit_volume, storage_type=excluded.storage_type,
          non_fba_sales_7d=excluded.non_fba_sales_7d, non_fba_sales_30d=excluded.non_fba_sales_30d,
          updated_at=datetime('now','localtime')
      `, [
        m.amazon_sku, m.asin || null, m.product_name || null,
        m.ne_code || null, m.logizard_code || null, m.jan || null,
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

export function getAllSnapshotSkus() {
  return queryAll('SELECT DISTINCT amazon_sku FROM daily_snapshots').map(r => r.amazon_sku);
}

// ===== RESTOCK / PLANNING 最新データ (Phase1: dual-write先) =====

/**
 * RESTOCKレポート最新データを全入替で保存
 * 件数急減ガード: 前回件数の50%以下なら破棄して前回データ維持
 * @param {Array} rows normalizeRestockRow() 済みの配列
 * @returns {{saved: number, skipped: boolean, reason?: string}}
 */
export function saveRestockLatest(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return { saved: 0, skipped: true, reason: 'empty_or_invalid' };
  }
  const prevCount = queryOne('SELECT COUNT(*) as c FROM restock_latest')?.c || 0;
  if (prevCount > 0 && rows.length < prevCount * 0.5) {
    console.warn(`[FBA-DB] RESTOCK件数急減ガード発動: 新${rows.length} < 前回${prevCount}×0.5、更新スキップ`);
    return { saved: 0, skipped: true, reason: 'count_guard', prev: prevCount, new: rows.length };
  }
  db.run('BEGIN TRANSACTION');
  try {
    db.run('DELETE FROM restock_latest');
    const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
    for (const r of rows) {
      db.run(`
        INSERT INTO restock_latest
          (amazon_sku, fnsku, asin, product_name, fba_available,
           fba_inbound_working, fba_inbound_shipped, fba_inbound_received,
           fba_unfulfillable, units_sold_30d, amazon_recommended_qty,
           amazon_recommended_date, alert_type, your_price, days_of_supply, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        r.amazon_sku, r.fnsku || null, r.asin || null, r.product_name || null,
        r.fba_available || 0,
        r.fba_inbound_working || 0, r.fba_inbound_shipped || 0, r.fba_inbound_received || 0,
        r.fba_unfulfillable || 0, r.units_sold_30d || 0,
        r.amazon_recommended_qty === null || r.amazon_recommended_qty === undefined ? null : r.amazon_recommended_qty,
        r.amazon_recommended_date || null, r.alert_type || null,
        r.your_price || null, r.days_of_supply || null, now,
      ]);
      // ever_seen_skus にも記録
      db.run(`
        INSERT INTO ever_seen_skus (amazon_sku, first_seen_at, last_seen_at)
        VALUES (?, ?, ?)
        ON CONFLICT(amazon_sku) DO UPDATE SET last_seen_at = excluded.last_seen_at
      `, [r.amazon_sku, now, now]);
    }
    db.run('COMMIT');
    saveToFile();
    return { saved: rows.length, skipped: false };
  } catch (e) {
    db.run('ROLLBACK');
    console.error('[FBA-DB] saveRestockLatest failed:', e.message);
    throw e;
  }
}

export function getRestockLatest() {
  return queryAll('SELECT * FROM restock_latest');
}

export function getRestockLatestMap() {
  const rows = getRestockLatest();
  const map = {};
  for (const r of rows) map[r.amazon_sku] = r;
  return map;
}

/**
 * PLANNINGレポート最新データを全入替で保存 (補助データ、取得失敗OK)
 * 件数ゼロは許容 (PLANNING は欠落しうる)
 * @param {Array} rows normalizePlanningRow() 済みの配列
 */
export function savePlanningLatest(rows) {
  if (!Array.isArray(rows)) {
    return { saved: 0, skipped: true, reason: 'invalid' };
  }
  // PLANNINGは補助なので件数ガードは緩め (前回の30%以下のみ破棄)
  const prevCount = queryOne('SELECT COUNT(*) as c FROM planning_latest')?.c || 0;
  if (prevCount > 0 && rows.length > 0 && rows.length < prevCount * 0.3) {
    console.warn(`[FBA-DB] PLANNING件数急減: 新${rows.length} < 前回${prevCount}×0.3、更新スキップ`);
    return { saved: 0, skipped: true, reason: 'count_guard', prev: prevCount, new: rows.length };
  }
  db.run('BEGIN TRANSACTION');
  try {
    db.run('DELETE FROM planning_latest');
    const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
    for (const r of rows) {
      db.run(`
        INSERT INTO planning_latest
          (amazon_sku, units_sold_7d, units_sold_60d, units_sold_90d,
           sales_7d, sales_30d, sales_60d, sales_90d,
           featured_offer_price, lowest_price, sales_rank,
           is_seasonal, season_name, short_term_dos, long_term_dos,
           low_inv_fee_applied, low_inv_fee_exempt,
           estimated_excess_qty, estimated_storage_cost, per_unit_volume, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        r.sku || r.amazon_sku,
        r.units_sold_7d ?? null, r.units_sold_60d ?? null, r.units_sold_90d ?? null,
        r.sales_7d ?? null, r.sales_30d ?? null, r.sales_60d ?? null, r.sales_90d ?? null,
        r.featured_offer_price ?? null, r.lowest_price ?? null, r.sales_rank ?? null,
        r.is_seasonal || null, r.season_name || null,
        r.short_term_dos ?? null, r.long_term_dos ?? null,
        r.low_inv_fee_applied || null, r.low_inv_fee_exempt || null,
        r.estimated_excess_qty ?? null, r.estimated_storage_cost ?? null, r.per_unit_volume ?? null, now,
      ]);
      // ever_seen_skus にも追記
      const sku = r.sku || r.amazon_sku;
      if (sku) {
        db.run(`
          INSERT INTO ever_seen_skus (amazon_sku, first_seen_at, last_seen_at)
          VALUES (?, ?, ?)
          ON CONFLICT(amazon_sku) DO UPDATE SET last_seen_at = excluded.last_seen_at
        `, [sku, now, now]);
      }
    }
    db.run('COMMIT');
    saveToFile();
    return { saved: rows.length, skipped: false };
  } catch (e) {
    db.run('ROLLBACK');
    console.error('[FBA-DB] savePlanningLatest failed:', e.message);
    throw e;
  }
}

export function getPlanningLatest() {
  return queryAll('SELECT * FROM planning_latest');
}

export function getPlanningLatestMap() {
  const rows = getPlanningLatest();
  const map = {};
  for (const r of rows) map[r.amazon_sku] = r;
  return map;
}

// ===== ever_seen_skus: 過去にFBAで観測したSKU（新規商品タブ判定用） =====

export function getAllEverSeenSkus() {
  return queryAll('SELECT amazon_sku FROM ever_seen_skus').map(r => r.amazon_sku);
}

export function isEverSeenSku(amazonSku) {
  return !!queryOne('SELECT 1 FROM ever_seen_skus WHERE amazon_sku = ?', [amazonSku]);
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

// ===== FNSKU一括更新 =====
export function updateFnskuBatch(items) {
  db.run('BEGIN TRANSACTION');
  try {
    for (const item of items) {
      if (item.sku && item.fnsku) {
        db.run('UPDATE sku_mapping SET fnsku = ? WHERE amazon_sku = ?', [item.fnsku, item.sku]);
      }
    }
    db.run('COMMIT');
    saveToFile();
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

/**
 * ミニPCからの同期用: FNSKUの最新状態をそのまま反映（nullなら明示的にクリア）
 * updateFnskuBatch は falsy を無視するため、旧FNSKUが残り続ける問題があった。
 * この関数は payload 通りに上書きするので、FNSKUが外された場合も正しく反映される。
 */
export function syncFnskuBatch(items) {
  db.run('BEGIN TRANSACTION');
  try {
    for (const item of items) {
      if (!item.sku) continue;
      db.run('UPDATE sku_mapping SET fnsku = ? WHERE amazon_sku = ?', [item.fnsku || null, item.sku]);
    }
    db.run('COMMIT');
    saveToFile();
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

// ===== Amazon仮確定 =====

export function saveProvisionalItems(items) {
  db.run('BEGIN TRANSACTION');
  try {
    db.run('DELETE FROM provisional_items');
    for (const item of items) {
      db.run(`
        INSERT INTO provisional_items
          (amazon_sku, product_name, fnsku, ship_qty, fba_available,
           units_sold_7d, units_sold_30d, warehouse_raw,
           recommended_qty, urgency_score, set_components, asin, expiry_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        item.amazon_sku,
        item.product_name || null,
        item.fnsku || null,
        parseInt(item.ship_qty || 0),
        parseInt(item.fba_available || 0),
        parseInt(item.units_sold_7d || 0),
        parseInt(item.units_sold_30d || 0),
        parseInt(item.warehouse_raw || 0),
        parseInt(item.recommended_qty || 0),
        parseFloat(item.urgency_score || 0),
        item.set_components || null,
        item.asin || null,
        item.expiry_date || null,
      ]);
    }
    // メタ情報を保存
    db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('saved_at', datetime('now','localtime'))`);
    db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('item_count', ?)`, [String(items.length)]);
    db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('total_qty', ?)`,
      [String(items.reduce((s, i) => s + (parseInt(i.ship_qty) || 0), 0))]);
    db.run('COMMIT');
    saveToFile();
    return items.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

/**
 * 既存の仮確定データに差分マージ（同一SKUは更新、新規は追加）
 */
export function mergeProvisionalItems(items) {
  db.run('BEGIN TRANSACTION');
  try {
    for (const item of items) {
      db.run(`
        INSERT INTO provisional_items
          (amazon_sku, product_name, fnsku, ship_qty, fba_available,
           units_sold_7d, units_sold_30d, warehouse_raw,
           recommended_qty, urgency_score, set_components, asin, expiry_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(amazon_sku) DO UPDATE SET
          product_name=excluded.product_name,
          fnsku=excluded.fnsku,
          ship_qty=excluded.ship_qty,
          fba_available=excluded.fba_available,
          units_sold_7d=excluded.units_sold_7d,
          units_sold_30d=excluded.units_sold_30d,
          warehouse_raw=excluded.warehouse_raw,
          recommended_qty=excluded.recommended_qty,
          urgency_score=excluded.urgency_score,
          set_components=excluded.set_components,
          asin=excluded.asin,
          expiry_date=excluded.expiry_date
      `, [
        item.amazon_sku,
        item.product_name || null,
        item.fnsku || null,
        parseInt(item.ship_qty || 0),
        parseInt(item.fba_available || 0),
        parseInt(item.units_sold_7d || 0),
        parseInt(item.units_sold_30d || 0),
        parseInt(item.warehouse_raw || 0),
        parseInt(item.recommended_qty || 0),
        parseFloat(item.urgency_score || 0),
        item.set_components || null,
        item.asin || null,
        item.expiry_date || null,
      ]);
    }
    // メタ情報を更新
    const count = queryOne('SELECT COUNT(*) as cnt FROM provisional_items');
    const total = queryOne('SELECT SUM(ship_qty) as total FROM provisional_items');
    db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('saved_at', datetime('now','localtime'))`);
    db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('item_count', ?)`, [String(count?.cnt || 0)]);
    db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('total_qty', ?)`, [String(total?.total || 0)]);
    db.run('COMMIT');
    saveToFile();
    return items.length;
  } catch (e) {
    db.run('ROLLBACK');
    throw e;
  }
}

export function getProvisionalItems() {
  const items = queryAll('SELECT * FROM provisional_items ORDER BY urgency_score DESC');
  const metaRows = queryAll('SELECT * FROM provisional_meta');
  const meta = {};
  for (const r of metaRows) meta[r.key] = r.value;
  return { items, meta };
}

export function clearProvisionalItems() {
  db.run('DELETE FROM provisional_items');
  db.run('DELETE FROM provisional_meta');
  saveToFile();
}

export function updateProvisionalItemQty(amazonSku, qty) {
  db.run('UPDATE provisional_items SET ship_qty = ? WHERE amazon_sku = ?', [parseInt(qty) || 0, amazonSku]);
  // メタの合計数量も更新
  const total = queryOne('SELECT SUM(ship_qty) as total FROM provisional_items');
  db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('total_qty', ?)`, [String(total?.total || 0)]);
  saveToFile();
}

export function removeProvisionalItem(amazonSku) {
  db.run('DELETE FROM provisional_items WHERE amazon_sku = ?', [amazonSku]);
  // メタ更新
  const count = queryOne('SELECT COUNT(*) as cnt FROM provisional_items');
  const total = queryOne('SELECT SUM(ship_qty) as total FROM provisional_items');
  db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('item_count', ?)`, [String(count?.cnt || 0)]);
  db.run(`INSERT OR REPLACE INTO provisional_meta (key, value) VALUES ('total_qty', ?)`, [String(total?.total || 0)]);
  saveToFile();
}

// ===== 出力履歴 =====

export function saveExportHistory(type, filename, itemCount, totalQty, fileData) {
  db.run(
    `INSERT INTO export_history (type, filename, item_count, total_qty, file_data) VALUES (?, ?, ?, ?, ?)`,
    [type, filename, itemCount, totalQty, fileData]
  );
  // タイプ別に100件を超えたら古いものを削除
  const oldest = queryAll(
    `SELECT id FROM export_history WHERE type = ? ORDER BY created_at DESC LIMIT -1 OFFSET 100`,
    [type]
  );
  for (const row of oldest) {
    db.run(`DELETE FROM export_history WHERE id = ?`, [row.id]);
  }
  saveToFile();
}

export function getExportHistoryList() {
  return queryAll(
    `SELECT id, type, filename, item_count, total_qty, created_at FROM export_history ORDER BY created_at DESC LIMIT 100`
  );
}

export function getExportHistoryFile(id) {
  return queryOne(`SELECT * FROM export_history WHERE id = ?`, [id]);
}
