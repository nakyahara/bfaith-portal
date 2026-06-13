/**
 * SQLite DB管理 — FBA在庫補充システム
 * sql.js パターン（profit-calculator/db.js 準拠）
 */
import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
// SKUマスタ直結 PR2: mirror 直読み adapter 用 (better-sqlite3 / 別DB)。
// import 時点では mirror DB を初期化しない (getMirrorDB を呼んだ時に lazy、未初期化なら throw)。
// FBA DB(sql.js) と mirror DB(better-sqlite3) はエンジンが違うので結合は JS 側で行う。
import { getMirrorDB } from '../warehouse-mirror/db.js';

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

  // --- 1b. fba_sku_attrs: FBAローカル属性 (fnsku/asin) ---
  // SKUマスタ直結 PR2: マッピング正本を mirror_sku_resolved に移すと sku_mapping の fnsku/asin が失われるため、
  // FBA 固有のローカル属性を分離して保持する。fnsku は SP-API snapshot 由来 (sheet にはない)。
  //   sheet モード: fnsku の正は従来どおり sku_mapping.fnsku (本テーブルは dual-write で追従)
  //   mirror モード: fnsku/asin の正は本テーブル (mirror_sku_resolved には fnsku/asin が無いため)
  db.run(`
    CREATE TABLE IF NOT EXISTS fba_sku_attrs (
      amazon_sku TEXT PRIMARY KEY,
      asin TEXT,
      fnsku TEXT,
      source TEXT,
      updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);
  // backfill: 既存 sku_mapping の asin/fnsku から未登録 SKU 分だけ初期投入 (非空 attrs は上書きしない)。
  // INSERT ... WHERE NOT EXISTS なので既存 fba_sku_attrs 行には一切触れない。
  db.run(`
    INSERT INTO fba_sku_attrs (amazon_sku, asin, fnsku, source)
    SELECT m.amazon_sku, m.asin, m.fnsku, 'sheet_backfill'
    FROM sku_mapping m
    WHERE m.amazon_sku NOT IN (SELECT amazon_sku FROM fba_sku_attrs)
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

  // --- 10b. replenishment_excluded: 納品推奨から恒久的に除外するSKU ---
  // 用途: 販売はしていたが価格が合わない等で「もう FBA に納品しない」と決めた商品。
  //   stockout_hidden / new_product_hidden が「そのタブの一時非表示」なのに対し、
  //   これは全推奨 (納品推奨 / FBA欠品 / 新規商品) 横断の恒久除外。reason に理由を残す。
  db.run(`
    CREATE TABLE IF NOT EXISTS replenishment_excluded (
      amazon_sku TEXT PRIMARY KEY,
      reason TEXT,
      excluded_at TEXT DEFAULT (datetime('now','localtime'))
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
      sku_list TEXT,
      created_at TEXT DEFAULT (datetime('now','localtime'))
    )
  `);
  // マイグレーション: sku_list (出力ファイルに含まれる amazon_sku の JSON 配列。
  // 後から恒久除外された SKU を含む古い履歴の再DLをブロックするため)
  {
    const ehCols = queryAll('PRAGMA table_info(export_history)').map(r => r.name);
    if (!ehCols.includes('sku_list')) db.run(`ALTER TABLE export_history ADD COLUMN sku_list TEXT`);
  }

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

      // working_first_seen: JP と同じ自動判定 (前回 working > 0 なら引き継ぎ、新規なら今日)
      let workingFirstSeen = null;
      if (inboundWorking > 0) {
        const prev = queryOne(
          `SELECT working_first_seen, fba_inbound_working
           FROM daily_snapshots_us
           WHERE amazon_sku = ? AND snapshot_date < ?
           ORDER BY snapshot_date DESC LIMIT 1`,
          [sku, today]
        );
        workingFirstSeen = (prev && prev.fba_inbound_working > 0 && prev.working_first_seen)
          ? prev.working_first_seen
          : today;
      }

      if (existing) {
        db.run(`
          UPDATE daily_snapshots_us SET
            product_name=?, fba_available=?, fba_inbound_working=?, fba_inbound_shipped=?, fba_inbound_received=?,
            fba_fc_transfer=?, fba_fc_processing=?, fba_customer_order=?, fba_unfulfillable=?,
            working_first_seen=?,
            days_of_supply=?, units_sold_7d=?, units_sold_30d=?,
            sales_rank=?, your_price=?, featured_offer_price=?
          WHERE snapshot_date = ? AND amazon_sku = ?
        `, [
          productName, fbaAvailable, inboundWorking, inboundShipped, inboundReceived,
          fcTransfer, fcProcessing, customerOrder, unfulfillable,
          workingFirstSeen,
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
             working_first_seen,
             days_of_supply, units_sold_7d, units_sold_30d,
             sales_rank, your_price, featured_offer_price)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          today, sku, productName,
          fbaAvailable, inboundWorking, inboundShipped, inboundReceived,
          fcTransfer, fcProcessing, customerOrder, unfulfillable,
          workingFirstSeen,
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

// ===== SKUマッピング取得 (SKUマスタ直結 PR2) =====
// FBA_SKU_MAPPING_SOURCE 環境変数でソースを切替:
//   'sheet'  (default) : 従来どおり sku_mapping (Google Sheet 同期)
//   'mirror'           : warehouse-mirror.db の mirror_sku_resolved 直読み (master 正本)
//   'shadow'           : 本番は sheet を返すが、内部で mirror も組み立てて差分を log (観測用、非破壊)
// 切替の本番化は PR4。PR2 では default sheet / shadow とも本番挙動を変えない。
function getSkuMappingSource() {
  return process.env.FBA_SKU_MAPPING_SOURCE || 'sheet';
}

// ===== non_fba_sales (他チャネル=FBA以外の販売数) のソース選択 (PR3) =====
// 従来は Google Sheet「商品コード変換テーブル」(Step3 同期 → sku_mapping) に依存していたが、
// 商品管理リスト案件で NE受注の FBA以外 7日/30日 を商品コード単位に集計する
// f_sales_velocity_by_product → published snapshot が出来、Renderミラー
// (mirror_pml_snapshot_rows.販売数{7,30}日_FBA以外) まで届いている。これを正にしてシート卒業する。
//
// FBA_NONFBA_SOURCE:
//   'sheet'  (default): 従来どおり sku_mapping の non_fba_sales_*
//   'pml'             : ミラーの商品管理リスト published snapshot を 商品コード(=代表ne_code)単位で採用。
//                       snapshot が未公開/未同期/不整合なら sheet へ自動フォールバック (fail-safe)。
//   'shadow'          : 本番は sheet を返すが pml との差分を log するだけ (観測用、非破壊)。
// 突合キー = SKU の代表 ne_code (sort_order 先頭)。snapshot は m_products 全件網羅のため、
// 「snapshot 在り & 該当 ne_code 無し」= その商品の他CH販売は 0 とみなす (0 採用)。
// ★ このフラグは mirror 読み経路 (FBA_SKU_MAPPING_SOURCE=mirror) でのみ効く。sheet/shadow モードの
//   getSkuMappings() は sku_mapping を素直に返すため non_fba も sheet のまま。本番は mirror 稼働中なのでOK。
function getNonFbaSource() {
  return (process.env.FBA_NONFBA_SOURCE || 'sheet').toLowerCase();
}

let _pmlNonFbaCache = null; // { at, map | null }
// published snapshot の 商品コード→{n7,n30} Map。未公開/テーブル無し/mirror未init は null (=sheetフォールバック)。
function getNonFbaFromPmlMap() {
  const now = Date.now();
  if (_pmlNonFbaCache && (now - _pmlNonFbaCache.at) < 60000) return _pmlNonFbaCache.map; // 日次更新なので60秒メモで十分
  let map = null;
  try {
    const mdb = getMirrorDB();
    const pub = mdb.prepare('SELECT run_id, status, row_count FROM mirror_pml_published WHERE id = 1').get();
    if (pub && pub.run_id && pub.status === 'ok') {
      const rows = mdb.prepare(
        'SELECT 商品コード AS code, 販売数7日_FBA以外 AS n7, 販売数30日_FBA以外 AS n30 FROM mirror_pml_snapshot_rows WHERE run_id = ?'
      ).all(pub.run_id);
      // 整合性チェック: 行が空 / row_count と不一致 = 壊れた published とみなし sheet フォールバック。
      //   (空Mapを「snapshot在り」と誤採用すると全SKUの他CH販売が 0 化してしまうため)
      if (rows.length > 0 && (pub.row_count == null || rows.length === pub.row_count)) {
        map = new Map();
        for (const r of rows) map.set(normSku(r.code), { n7: Number(r.n7) || 0, n30: Number(r.n30) || 0 });
      } else {
        console.warn(`[FBA] mirror_pml published 不整合 (rows=${rows.length} / row_count=${pub.row_count} / status=${pub.status}) → non_fba は sheet フォールバック`);
      }
    }
  } catch (e) {
    map = null; // mirror_pml_* 未作成 / 未公開 / mirror 未init → sheet フォールバック
  }
  _pmlNonFbaCache = { at: now, map };
  return map;
}

// 代表 ne_code から non_fba_sales を解決。pmlMap 不在(=snapshot無し)時は sheetRow にフォールバック。
function resolveNonFba(repNe, sheetRow, pmlMap) {
  if (pmlMap) {
    const p = pmlMap.get(normSku(repNe));
    return { non_fba_sales_7d: p?.n7 || 0, non_fba_sales_30d: p?.n30 || 0 };
  }
  return { non_fba_sales_7d: sheetRow?.non_fba_sales_7d || 0, non_fba_sales_30d: sheetRow?.non_fba_sales_30d || 0 };
}

// shadow: sheet と pml の non_fba_30d 集計差を log (1時間に1回)。切替前の本番検証用、非破壊。
let _nonFbaShadowAt = 0;
function logNonFbaShadowDiff(bySku, sheetMap) {
  const now = Date.now();
  if (now - _nonFbaShadowAt < 3600000) return;
  _nonFbaShadowAt = now;
  const pmlMap = getNonFbaFromPmlMap();
  if (!pmlMap) { console.warn('[FBA shadow] non_fba: pml snapshot 無し → sheet のみ'); return; }
  let n = 0, exact = 0, sheetSum = 0, pmlSum = 0, pml0sheetPos = 0, sheet0pmlPos = 0;
  for (const [sku, compRows] of bySku) {
    const sheetV = sheetMap.get(normSku(sku))?.non_fba_sales_30d || 0;
    const pmlV = pmlMap.get(normSku(compRows[0]?.ne_code))?.n30 || 0;
    n++; sheetSum += sheetV; pmlSum += pmlV;
    if (sheetV === pmlV) exact++;
    if (pmlV === 0 && sheetV > 0) pml0sheetPos++;
    if (sheetV === 0 && pmlV > 0) sheet0pmlPos++;
  }
  const pct = n ? (exact / n * 100).toFixed(1) : '0';
  console.log(`[FBA shadow] non_fba_30d sheet vs pml: n=${n} exact=${exact}(${pct}%) sheetSum=${sheetSum} pmlSum=${pmlSum} diff=${sheetSum - pmlSum} pml0&sheet>0=${pml0sheetPos} sheet0&pml>0=${sheet0pmlPos}`);
}

export function getSkuMappingsFromSheet() {
  return queryAll('SELECT * FROM sku_mapping ORDER BY amazon_sku');
}

// mirror_sku_resolved の component 行群 (sort_order 昇順) を既存 sku_mapping と同じ shape の1行に変換。
//   - ne_code / logizard_code = 代表 (sort_order 先頭) の ne_code
//   - is_set = 構成 > 1 or 単品でも qty > 1
//   - set_components = [{ne_code, qty}] の JSON 文字列 (既存 DB shape 互換)
//   - asin / fnsku = fba_sku_attrs 由来 (mirror には無い)
//   - non_fba_sales_* = 呼び出し側 (getSkuMappings*FromMirror) が FBA_NONFBA_SOURCE に応じて
//       sheet(sku_mapping) か pml(商品管理リストsnapshot) で解決して nonFba 引数に渡す (PR3)
//   - per_unit_volume / storage_type = null (元々 snap 由来、mapping 側未使用)
function buildMirrorRow(amazonSku, compRows, attr, nonFba, registeredAt) {
  const components = compRows.map(c => ({ ne_code: c.ne_code, qty: c.quantity }));
  const isSet = components.length > 1 || (components[0] && (Number(components[0].qty) || 1) > 1);
  return {
    amazon_sku: amazonSku,
    asin: attr?.asin || null,
    product_name: compRows[0]?.['商品名'] || '',
    ne_code: components[0]?.ne_code || null,
    logizard_code: components[0]?.ne_code || null,
    fnsku: attr?.fnsku || null,
    jan: null,
    is_set: isSet ? 1 : 0,
    set_components: JSON.stringify(components),
    per_unit_volume: null,
    storage_type: null,
    non_fba_sales_7d: nonFba?.non_fba_sales_7d || 0,
    non_fba_sales_30d: nonFba?.non_fba_sales_30d || 0,
    updated_at: null,
    // SKUマスタ登録日 (m_sku_master.created_at → mirror_sku_master.source_created_at)。
    // 新規商品タブで「いつ登録した商品か」を表示するため。ISO 8601 UTC 文字列 or null。
    registered_at: registeredAt || null,
  };
}

// SKU/コード正規化 (case 非依存の突き合わせ用、PR4)
const normSku = (v) => String(v ?? '').trim().toLowerCase();

// 案C: mirror は seller_sku を小文字保存するので、amazon_sku の「元ケース」を FBA 側データから復元する
// (consumer 無改修で動かすため。recData.find / mappingMap など多数の exact 比較を壊さない)。
// 優先度 低→高 の順に入れて後勝ち上書き: sku_mapping → ever_seen → daily_snapshots → restock_latest → planning_latest
function buildAmazonSkuCaseMap() {
  const m = new Map();
  // first-write-wins: 高優先のソースを先に入れる。
  // calc-engine の主キー snap.amazon_sku は restock_latest 由来 (snapshots = restockRows.map(...)) なので、
  // emit する amazon_sku を restock_latest のケースに揃えれば mappingMap[snap.amazon_sku] が確実に一致し SKU 欠落しない。
  const add = (sku) => { const k = normSku(sku); if (k && !m.has(k)) m.set(k, sku); };
  for (const sql of [
    'SELECT amazon_sku FROM restock_latest',          // 最優先 (推奨生成の主キー)
    'SELECT amazon_sku FROM planning_latest',
    'SELECT DISTINCT amazon_sku FROM daily_snapshots',
    'SELECT amazon_sku FROM ever_seen_skus',
    'SELECT amazon_sku FROM sku_mapping',
  ]) {
    try { for (const r of queryAll(sql)) add(r.amazon_sku); } catch (e) { /* テーブル未作成等は無視 */ }
  }
  return m;
}

export function getSkuMappingsFromMirror() {
  const mdb = getMirrorDB(); // better-sqlite3 (mirror 未初期化なら throw)
  const rows = mdb.prepare(`
    SELECT seller_sku, ne_code, quantity, 商品名, sort_order
    FROM mirror_sku_resolved
    WHERE source = 'master'
    ORDER BY seller_sku, sort_order, ne_code
  `).all();
  // JS group by seller_sku (SQL GROUP BY ではなく順序保持の JS group)
  const bySku = new Map();
  for (const r of rows) {
    if (!bySku.has(r.seller_sku)) bySku.set(r.seller_sku, []);
    bySku.get(r.seller_sku).push(r);
  }
  // FBA ローカル属性 (sql.js) を norm キーで Map 化して JS join (mirror 小文字 ⇔ 元ケース attrs の取りこぼし防止)
  const attrs = new Map();
  for (const a of queryAll('SELECT amazon_sku, asin, fnsku FROM fba_sku_attrs')) attrs.set(normSku(a.amazon_sku), a);
  const nonFba = new Map();
  for (const s of queryAll('SELECT amazon_sku, non_fba_sales_7d, non_fba_sales_30d FROM sku_mapping')) nonFba.set(normSku(s.amazon_sku), s);
  // 登録日 (source_created_at) を mirror_sku_master から norm キーで join (1 SKU 1 行)。
  const regAt = new Map();
  try {
    for (const r of mdb.prepare('SELECT seller_sku, source_created_at FROM mirror_sku_master').all()) {
      regAt.set(normSku(r.seller_sku), r.source_created_at || null);
    }
  } catch (e) { /* mirror_sku_master 未作成 (旧 mirror) は登録日なしで続行 */ }
  const caseMap = buildAmazonSkuCaseMap();

  // non_fba_sales のソース選択 (PR3)。pml モードのみ snapshot を引く (sheet/shadow は従来どおり sheet 値)。
  const nonFbaSource = getNonFbaSource();
  const pmlMap = (nonFbaSource === 'pml') ? getNonFbaFromPmlMap() : null;
  if (nonFbaSource === 'pml' && !pmlMap) {
    console.warn('[FBA] FBA_NONFBA_SOURCE=pml だが商品管理リスト snapshot が未公開/未同期 → sheet にフォールバック');
  }
  if (nonFbaSource === 'shadow') { try { logNonFbaShadowDiff(bySku, nonFba); } catch (e) { /* best-effort */ } }

  const result = [];
  for (const [sku, compRows] of bySku) {
    const k = normSku(sku);
    const origSku = caseMap.get(k) || sku; // 元ケース復元、無ければ小文字 fallback
    const nf = resolveNonFba(compRows[0]?.ne_code, nonFba.get(k), pmlMap);
    result.push(buildMirrorRow(origSku, compRows, attrs.get(k), nf, regAt.get(k)));
  }
  result.sort((x, y) => String(x.amazon_sku).localeCompare(String(y.amazon_sku)));
  return result;
}

function getSkuMappingFromMirror(amazonSku) {
  const mdb = getMirrorDB();
  const k = normSku(amazonSku);
  const compRows = mdb.prepare(`
    SELECT seller_sku, ne_code, quantity, 商品名, sort_order
    FROM mirror_sku_resolved
    WHERE source = 'master' AND seller_sku = ?
    ORDER BY sort_order, ne_code
  `).all(k);
  if (compRows.length === 0) return null;
  // 補助 join も norm キーで (single も case 非依存に)
  const attrRows = queryAll('SELECT asin, fnsku FROM fba_sku_attrs WHERE LOWER(TRIM(amazon_sku)) = ?', [k]);
  const nfRows = queryAll('SELECT non_fba_sales_7d, non_fba_sales_30d FROM sku_mapping WHERE LOWER(TRIM(amazon_sku)) = ?', [k]);
  let registeredAt = null;
  try {
    const mr = mdb.prepare('SELECT source_created_at FROM mirror_sku_master WHERE LOWER(TRIM(seller_sku)) = ?').get(k);
    registeredAt = mr?.source_created_at || null;
  } catch (e) { /* mirror_sku_master 未作成は無視 */ }
  // non_fba_sales のソース選択 (PR3、list と同じ規則)。pml 不在は sheet フォールバック。
  const pmlMap = (getNonFbaSource() === 'pml') ? getNonFbaFromPmlMap() : null;
  const nf = resolveNonFba(compRows[0]?.ne_code, nfRows[0], pmlMap);
  // 呼び出し側が渡したケースをそのまま返す (元ケース保持)
  return buildMirrorRow(amazonSku, compRows, attrRows[0], nf, registeredAt);
}

// set_components (文字列 or 配列) を比較用の正規化文字列に (ne_code は case 無視、qty 含む)
function normComponents(sc) {
  let arr = [];
  if (Array.isArray(sc)) arr = sc;
  else if (typeof sc === 'string' && sc) { try { arr = JSON.parse(sc); } catch { arr = []; } }
  if (!Array.isArray(arr)) arr = [];
  return arr.map(c => `${String(c.ne_code || '').trim().toLowerCase()}:${c.qty ?? c.数量 ?? 1}`).sort().join(',');
}

// shadow: sheet と mirror の core 差分を集計 (確定はしない、観測用)
// ★ case 非依存で突き合わせる: SKUマスタ/mirror は seller_sku・ne_code を小文字正規化して保存する一方、
//   スプレッドシート(sku_mapping)は元の大小文字を保持する。case 区別で突き合わせると同一SKUが
//   sheet_only(大文字) と mirror_only(小文字) に二重計上され擬似差分が大量発生するため、
//   キー・ne_code・fnsku は norm() で小文字揃えして比較する (case 差自体は実害でないので無視)。
//   ※ ただし PR4 の本番切替では adapter/consumer 側で case-insensitive 化が必須 (元バグ再発防止)。
function compareSkuMappings(sheetRows, mirrorRows) {
  const norm = (v) => String(v ?? '').trim().toLowerCase();
  const sheetMap = new Map(sheetRows.map(r => [norm(r.amazon_sku), r]));
  const mirrorMap = new Map(mirrorRows.map(r => [norm(r.amazon_sku), r]));
  const observed = new Set([...getAllEverSeenSkus(), ...getAllSnapshotSkus()].map(norm));
  const sheetOnly = [], mirrorOnlyObserved = [], mirrorOnlyScopeUnknown = [], fieldDiffs = [];
  for (const [k, r] of sheetMap) if (!mirrorMap.has(k)) sheetOnly.push(r.amazon_sku);
  for (const [k, r] of mirrorMap) {
    if (!sheetMap.has(k)) (observed.has(k) ? mirrorOnlyObserved : mirrorOnlyScopeUnknown).push(r.amazon_sku);
  }
  for (const [k, s] of sheetMap) {
    const m = mirrorMap.get(k);
    if (!m) continue;
    const d = {};
    if (norm(s.ne_code) !== norm(m.ne_code)) d.ne_code = [s.ne_code, m.ne_code];
    if ((s.is_set ? 1 : 0) !== (m.is_set ? 1 : 0)) d.is_set = [s.is_set, m.is_set];
    if (normComponents(s.set_components) !== normComponents(m.set_components)) d.components = true;
    if (norm(s.fnsku) !== norm(m.fnsku)) d.fnsku = [s.fnsku, m.fnsku];
    if (Object.keys(d).length) fieldDiffs.push({ amazon_sku: s.amazon_sku, ...d });
  }
  // ★切替前ゲート (PR4 修正): 母集団は「いま推奨で実際に使う SKU」= calc-engine と同じ
  //   restock_latest + 最新 daily_snapshots に限定する。ever_seen / 全 snapshot は廃番を含む歴史的集合で、
  //   それらが mirror に無いのは当然 (旧シートにも無い) → ゲートに使うと大量過検知する (旧実装の誤り)。
  //   mappingMap は norm 参照なので case 差では落ちない。落ちるのは「現行SKUの norm キーが mirror に無い」場合のみ。
  //   判定: mirror_missing_observed が sheet_missing_baseline と同じ (理想は両方 0/小) なら、
  //         現行シートと同等の網羅性 = 切替えても推奨から失われる現行 SKU は増えない。
  const mirrorExactSet = new Set(mirrorRows.map(r => r.amazon_sku));
  const mirrorNormSet = new Set(mirrorRows.map(r => norm(r.amazon_sku)));
  const sheetNormSet = new Set(sheetRows.map(r => norm(r.amazon_sku)));
  const currentSkus = [...new Set([
    ...getRestockLatest().map(r => r.amazon_sku),
    ...getLatestSnapshots().map(r => r.amazon_sku),
  ])].filter(Boolean);
  const missingFromMirror = currentSkus.filter(s => !mirrorNormSet.has(norm(s)));
  const missingFromSheet  = currentSkus.filter(s => !sheetNormSet.has(norm(s)));
  // ★★最重要ゲート: 「今シートでマッピングできてる現行SKUのうち、mirror に無い=切替で実際に落ちる」数。
  //   net 比較 (missing 同士) では相殺で隠れる regression を、集合差で直接捕捉する。0 で「1件も落ちない」が確定。
  const wouldDropOnSwitch = currentSkus.filter(s => sheetNormSet.has(norm(s)) && !mirrorNormSet.has(norm(s)));
  const wouldGainOnSwitch = currentSkus.filter(s => mirrorNormSet.has(norm(s)) && !sheetNormSet.has(norm(s)));
  const caseMismatchObserved = currentSkus.filter(s => mirrorNormSet.has(norm(s)) && !mirrorExactSet.has(s));
  // 参考: 歴史的 ever_seen + 全 snapshot のうち mirror に無い数 (廃番を含むので大きくて正常。ゲートではない)
  const everSeenMissing = [...new Set([...getAllEverSeenSkus(), ...getAllSnapshotSkus()])]
    .filter(Boolean).filter(s => !mirrorNormSet.has(norm(s)));
  // case-only collision: norm 後に複数の異なる原ケースが衝突する SKU 数 (sheet 側で検出、>0 は要注意)
  const normGroups = new Map();
  for (const r of sheetRows) {
    const k = norm(r.amazon_sku);
    if (!normGroups.has(k)) normGroups.set(k, new Set());
    normGroups.get(k).add(r.amazon_sku);
  }
  const caseCollisions = [...normGroups.values()].filter(set => set.size > 1).length;
  return {
    summary: {
      sheet_count: sheetRows.length,
      mirror_count: mirrorRows.length,
      sheet_only: sheetOnly.length,                       // sheet にあるが mirror に無い
      mirror_only_observed: mirrorOnlyObserved.length,    // mirror のみ & FBA観測あり = core 差分 (要調査)
      mirror_only_scope_unknown: mirrorOnlyScopeUnknown.length, // mirror のみ & 未観測 = 新規商品候補 or 非Amazon混入
      field_diffs: fieldDiffs.length,                     // 共通SKUの core フィールド差分
      current_recommendation_skus: currentSkus.length,    // いま推奨で使う現行SKU数 (ゲートの母集団)
      would_drop_on_switch: wouldDropOnSwitch.length,      // ★★最重要ゲート: シートで推奨できてるのにmirror切替で落ちる現行SKU。0必須
      would_gain_on_switch: wouldGainOnSwitch.length,      // 参考: mirror切替で新たに拾える現行SKU
      mirror_missing_observed: missingFromMirror.length,  // 現行SKUのうち mirror に無い数 (sheetにも無い分を含む)
      sheet_missing_baseline: missingFromSheet.length,    // 基準: 同じ現行SKUのうち現行シートに無い数
      mirror_case_mismatch_observed: caseMismatchObserved.length, // 参考: case のみ違い (norm参照で無害)
      mapping_case_collisions: caseCollisions,            // 参考: 大小文字だけ違う別SKUの衝突数 (>0 は要確認)
      ever_seen_missing_info: everSeenMissing.length,     // 参考のみ: 歴史的(廃番含む)集合の欠落数。大きくて正常、ゲートではない
    },
    samples: [
      ...sheetOnly.slice(0, 10).map(s => ({ type: 'sheet_only', amazon_sku: s })),
      ...mirrorOnlyObserved.slice(0, 10).map(s => ({ type: 'mirror_only_observed', amazon_sku: s })),
      ...mirrorOnlyScopeUnknown.slice(0, 10).map(s => ({ type: 'mirror_only_scope_unknown', amazon_sku: s })),
      ...wouldDropOnSwitch.slice(0, 20).map(s => ({ type: 'would_drop_on_switch', amazon_sku: s })),
      ...wouldGainOnSwitch.slice(0, 10).map(s => ({ type: 'would_gain_on_switch', amazon_sku: s })),
      ...fieldDiffs.slice(0, 10),
    ],
  };
}

const SHADOW_TTL_MS = 60000;
let _shadowLastRun = 0;
// shadow 差分の実行は完全隔離 + TTL。失敗しても sheet 結果には一切影響しない。
function runShadowDiff(sheetRows) {
  try {
    const now = Date.now();
    if (now - _shadowLastRun < SHADOW_TTL_MS) return; // ログ洪水 / event loop ブロック回避
    _shadowLastRun = now;
    let mirrorRows;
    try {
      mirrorRows = getSkuMappingsFromMirror();
    } catch (e) {
      console.warn('[FBA shadow] mirror 読取スキップ (未初期化/読取失敗): ' + e.message);
      return;
    }
    const diff = compareSkuMappings(sheetRows, mirrorRows);
    console.log('[FBA shadow] sku_mapping diff summary: ' + JSON.stringify(diff.summary));
    if (diff.samples.length) console.log('[FBA shadow] sku_mapping diff samples: ' + JSON.stringify(diff.samples));
  } catch (e) {
    console.warn('[FBA shadow] diff 失敗 (本番は sheet 継続): ' + e.message);
  }
}

export function getSkuMappings() {
  const mode = getSkuMappingSource();
  if (mode === 'mirror') return getSkuMappingsFromMirror();
  const sheetRows = getSkuMappingsFromSheet();
  if (mode === 'shadow') runShadowDiff(sheetRows); // 本番は必ず sheet を返す
  return sheetRows;
}

export function getSkuMapping(amazonSku) {
  const mode = getSkuMappingSource();
  if (mode === 'mirror') return getSkuMappingFromMirror(amazonSku);
  // sheet / shadow: 単票は sheet を返す (差分集計は getSkuMappings 側で TTL 付き実行)
  return queryOne('SELECT * FROM sku_mapping WHERE amazon_sku = ?', [amazonSku]);
}

export function getSkuExceptions() {
  return queryAll('SELECT * FROM sku_exceptions ORDER BY amazon_sku');
}

export function getWarehouseInventory() {
  return queryAll('SELECT * FROM warehouse_inventory ORDER BY logizard_code');
}

// PR4: logizard_code は case 非依存で引く (mirror は小文字、倉庫CSVは元ケース)。
// LOWER(TRIM()) 同士で突き合わせ。sheet モードでも同一値同士なので無害。
export function getWarehouseQtyByCode(logizardCode) {
  const row = queryOne(
    'SELECT SUM(quantity) as total, SUM(available_qty) as available FROM warehouse_inventory WHERE LOWER(TRIM(logizard_code)) = LOWER(TRIM(?)) AND is_y_location = 0',
    [logizardCode]
  );
  return { total: row?.total || 0, available: row?.available || 0 };
}

export function getWarehouseYQtyByCode(logizardCode) {
  const row = queryOne(
    'SELECT SUM(quantity) as total FROM warehouse_inventory WHERE LOWER(TRIM(logizard_code)) = LOWER(TRIM(?)) AND is_y_location = 1',
    [logizardCode]
  );
  return row?.total || 0;
}

// ロケーション在庫を引当優先順で取得（卸し→通販、ブロック引当順昇順、ロケ昇順）
export function getWarehouseLocationsByCode(logizardCode) {
  return queryAll(`
    SELECT location, block, available_qty, location_biz_type, block_alloc_order, expiry_date
    FROM warehouse_inventory
    WHERE LOWER(TRIM(logizard_code)) = LOWER(TRIM(?)) AND is_y_location = 0 AND available_qty > 0
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
  // PR4: logizard_code を LOWER(TRIM()) で集約 (倉庫CSVに ABC/abc 混在があっても1行に合算)。
  // 利用側 (warehouseMap/warehouseSummaryMap) は normCode キーで引くので by-code 系 (LOWER(TRIM())) と一致する。
  // 表示用 logizard_code は代表値 (MIN) を返す。
  return queryAll(`
    SELECT
      MIN(logizard_code) as logizard_code,
      MAX(product_name) as product_name,
      SUM(CASE WHEN is_y_location = 0 THEN quantity ELSE 0 END) as warehouse_qty,
      SUM(CASE WHEN is_y_location = 0 THEN available_qty ELSE 0 END) as warehouse_available,
      SUM(CASE WHEN is_y_location = 1 THEN quantity ELSE 0 END) as y_location_qty,
      MIN(CASE WHEN expiry_date != '' AND expiry_date IS NOT NULL THEN expiry_date END) as earliest_expiry,
      MAX(last_arrival_date) as last_arrival_date,
      COUNT(DISTINCT location) as location_count
    FROM warehouse_inventory
    GROUP BY LOWER(TRIM(logizard_code))
    ORDER BY 1
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

// ===== ever_stocked: 過去に一度でも「実FBA在庫 / 入荷」があったSKU =====
// 新規商品タブ / FBA欠品タブの正しい振り分け軸。
//   ever_seen_skus は RESTOCK/PLANNING レポートに「載った」だけで付くため、まだ一度も
//   FBAへ納品していない新規出品 (出荷可能在庫0・入荷0) まで「観測済み」になってしまい、
//   本来「新規商品」であるべきSKUが新規商品タブから漏れていた (2026-06-13 修正)。
//   そこで「FBA倉庫在庫の構成列 (fba_available + fc_transfer + fc_processing + customer_order)、
//   入荷 (working/shipped/received)、不良在庫 (unfulfillable)、working_first_seen のいずれかを
//   一度でも観測したSKU」だけを ever_stocked として返す。
//   ※ fba_warehouse = fba_available + fba_fc_transfer + fba_fc_processing + fba_customer_order なので、
//     fba_available=0 でも FC間移動/処理中/顧客注文引当の在庫があれば「在庫経験あり」= 新規ではない。
//   daily_snapshots は履歴蓄積 (過去に在庫を持って今0でも拾える)。これら fc_*/unfulfillable 列は
//     initDb の ALTER 移行で必ず存在する。restock_latest は当日分の補完 (fc_* 列は無く unfulfillable まで)。
export function getEverStockedSkus() {
  let fromSnap = [];
  try {
    fromSnap = queryAll(`
      SELECT DISTINCT amazon_sku FROM daily_snapshots
      WHERE fba_available > 0
         OR fba_inbound_working > 0
         OR fba_inbound_shipped > 0
         OR fba_inbound_received > 0
         OR fba_fc_transfer > 0
         OR fba_fc_processing > 0
         OR fba_customer_order > 0
         OR fba_unfulfillable > 0
         OR (working_first_seen IS NOT NULL AND TRIM(working_first_seen) <> '')
    `).map(r => r.amazon_sku);
  } catch (e) { /* 旧スキーマ (移行前) は無視 */ }
  let fromRestock = [];
  try {
    fromRestock = queryAll(`
      SELECT amazon_sku FROM restock_latest
      WHERE fba_available > 0
         OR fba_inbound_working > 0
         OR fba_inbound_shipped > 0
         OR fba_inbound_received > 0
         OR fba_unfulfillable > 0
    `).map(r => r.amazon_sku);
  } catch (e) { /* restock_latest 未作成は無視 */ }
  return [...new Set([...fromSnap, ...fromRestock])];
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

// ===== 納品推奨 恒久除外管理 (replenishment_excluded) =====
// 全推奨 (納品推奨 / FBA欠品 / 新規商品) 横断で「もう納品しない」SKU を管理する。

export function getReplenishmentExcluded() {
  // 商品名/ASIN を sku_mapping から引いて返す (管理画面で「何を除外したか」を商品名で確認できるように)。
  // amazon_sku は必ず返るのでサーバ側の除外セット生成 (.map(r=>r.amazon_sku)) はそのまま動く。
  try {
    return queryAll(`
      SELECT e.amazon_sku, e.reason, e.excluded_at,
             m.product_name AS product_name, m.asin AS asin
      FROM replenishment_excluded e
      LEFT JOIN sku_mapping m ON e.amazon_sku = m.amazon_sku
      ORDER BY e.excluded_at DESC
    `);
  } catch (e) {
    // JOIN が失敗しても最低限 amazon_sku/reason/excluded_at は返す (一覧が真っ白/エラーにならない)
    console.error('[FBA] getReplenishmentExcluded JOIN 失敗、単純SELECTにフォールバック:', e.message);
    return queryAll('SELECT amazon_sku, reason, excluded_at, NULL AS product_name, NULL AS asin FROM replenishment_excluded ORDER BY excluded_at DESC');
  }
}

export function excludeReplenishmentSku(amazonSku, reason) {
  db.run(`INSERT OR REPLACE INTO replenishment_excluded (amazon_sku, reason, excluded_at) VALUES (?, ?, datetime('now','localtime'))`,
    [amazonSku, reason || null]);
  saveToFile();
}

export function unexcludeReplenishmentSku(amazonSku) {
  db.run('DELETE FROM replenishment_excluded WHERE amazon_sku = ?', [amazonSku]);
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
        // dual-write: mirror モードの正となる fba_sku_attrs にも追従 (falsy は無視 = 旧FNSKU保持)
        db.run(
          `INSERT INTO fba_sku_attrs (amazon_sku, fnsku, source, updated_at)
           VALUES (?, ?, 'restock', datetime('now','localtime'))
           ON CONFLICT(amazon_sku) DO UPDATE SET fnsku=excluded.fnsku, source='restock', updated_at=excluded.updated_at`,
          [item.sku, item.fnsku]
        );
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
      // dual-write: null も反映 (FNSKU が外れた商品をクリア)。sku_mapping に無い mirror-only SKU でも upsert。
      db.run(
        `INSERT INTO fba_sku_attrs (amazon_sku, fnsku, source, updated_at)
         VALUES (?, ?, 'planning', datetime('now','localtime'))
         ON CONFLICT(amazon_sku) DO UPDATE SET fnsku=excluded.fnsku, source='planning', updated_at=excluded.updated_at`,
        [item.sku, item.fnsku || null]
      );
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

export function saveExportHistory(type, filename, itemCount, totalQty, fileData, skuList) {
  // skuList: 出力ファイルに含まれる amazon_sku 配列 (再DL時の除外チェック用)。null 可。
  const skuListJson = Array.isArray(skuList) ? JSON.stringify(skuList) : null;
  db.run(
    `INSERT INTO export_history (type, filename, item_count, total_qty, file_data, sku_list) VALUES (?, ?, ?, ?, ?, ?)`,
    [type, filename, itemCount, totalQty, fileData, skuListJson]
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
