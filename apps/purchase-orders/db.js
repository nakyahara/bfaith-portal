/**
 * purchase-orders DB — 仕入先発注補助システムのテーブル定義
 *
 * warehouse-mirror.db の中に po_* テーブルを追加する (inventory-monthly と同方式)。
 * 商品・在庫・販売データは既存の mirror_pml_snapshot_rows (published run) を読むだけで、
 * 本アプリが正本を持つのは「発注条件マスタ類」と「発注ドラフト/履歴」のみ。
 *
 *   - po_suppliers          仕入先 (コード→名称・発注方法メモ)
 *   - po_order_conditions   発注条件グループ (最低数量/最低金額 等)
 *   - po_material_groups    原料グループ (自社製造品の原料 最低発注量)
 *   - po_product_attrs      商品→グループ紐付け (容量/個・ケースロット含む)
 *   - po_orders             発注ヘッダ (draft/issued)
 *   - po_order_items        発注明細
 *
 * 初期データは旧「発注条件マスタ」スプレッドシート由来のCSVを管理画面から取込む。
 * 仕入先コードは正規形 (先頭ゼロ除去、'0001'→'1') で保持し、PML側 join 時にも同じ正規化を通す。
 * 商品コードは原文保持し、照合は LOWER(TRIM()) で行う。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

let initialized = false;

/** 仕入先コード正規化: 全角空白trim + 先頭ゼロ除去 ('0001'→'1')。数字以外はそのまま */
export function normSupplierCode(v) {
  const s = String(v == null ? '' : v).trim();
  if (!s) return '';
  if (/^\d+$/.test(s)) return String(parseInt(s, 10));
  return s;
}

/** 商品コード照合キー */
export function normProductCode(v) {
  return String(v == null ? '' : v).trim().toLowerCase();
}

export function initPurchaseOrders() {
  const db = getMirrorDB();

  db.exec(`CREATE TABLE IF NOT EXISTS po_suppliers (
    supplier_code   TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    order_memo      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS po_order_conditions (
    condition_id    TEXT PRIMARY KEY,
    supplier_code   TEXT,
    maker_name      TEXT,
    display_name    TEXT NOT NULL,
    condition_type  TEXT NOT NULL,
    condition_value REAL NOT NULL CHECK(condition_value >= 0),
    unit            TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS po_material_groups (
    group_id        TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    min_order_qty   REAL CHECK(min_order_qty IS NULL OR min_order_qty >= 0),
    unit            TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS po_product_attrs (
    product_code      TEXT PRIMARY KEY,
    product_key       TEXT NOT NULL,
    condition_id      TEXT,
    material_group_id TEXT,
    capacity_per_unit REAL CHECK(capacity_per_unit IS NULL OR capacity_per_unit > 0),
    case_group        TEXT,
    case_lot          REAL,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
  )`);
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_po_attrs_key ON po_product_attrs(product_key)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_attrs_cond ON po_product_attrs(condition_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_attrs_mat ON po_product_attrs(material_group_id)');

  db.exec(`CREATE TABLE IF NOT EXISTS po_orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_code   TEXT NOT NULL,
    supplier_name   TEXT NOT NULL,
    status          TEXT NOT NULL CHECK(status IN ('draft','issued')),
    note            TEXT,
    pml_as_of_date  TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    issued_at       TEXT
  )`);
  // 仕入先ごとに draft は同時に1件だけ (issuedは何件でも)
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_po_orders_draft
           ON po_orders(supplier_code) WHERE status='draft'`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_orders_supplier ON po_orders(supplier_code, status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_orders_issued ON po_orders(status, issued_at)');

  db.exec(`CREATE TABLE IF NOT EXISTS po_order_items (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id      INTEGER NOT NULL REFERENCES po_orders(id) ON DELETE CASCADE,
    product_code  TEXT NOT NULL,
    product_key   TEXT NOT NULL,
    product_name  TEXT,
    qty           INTEGER NOT NULL CHECK(qty > 0),
    unit_cost     REAL,
    UNIQUE(order_id, product_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_items_order ON po_order_items(order_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_items_key ON po_order_items(product_key)');

  initialized = true;
  return db;
}

export function getDB() {
  if (!initialized) initPurchaseOrders();
  return getMirrorDB();
}
