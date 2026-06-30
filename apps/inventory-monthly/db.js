/**
 * inventory-monthly DB — 月末棚卸し履歴テーブル定義
 *
 * warehouse-mirror.db の中に追加で 3 テーブルを作成する:
 *   - inv_snapshot         月次サマリー
 *   - inv_snapshot_detail  SKU毎明細
 *   - inv_snapshot_pending 発注後未着商品（手動入力分）
 *
 * 参照する既存テーブル（warehouse-mirror.db, 同期）:
 *   - mirror_products / mirror_sku_resolved / mirror_set_components
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

let initialized = false;

export function initInventoryMonthly() {
  const db = getMirrorDB();

  db.exec(`CREATE TABLE IF NOT EXISTS inv_snapshot (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   TEXT NOT NULL,
    fba_warehouse   REAL NOT NULL DEFAULT 0,
    fba_inbound     REAL NOT NULL DEFAULT 0,
    own_warehouse   REAL NOT NULL DEFAULT 0,
    fba_us          REAL NOT NULL DEFAULT 0,
    fba_us_inbound  REAL NOT NULL DEFAULT 0,
    pending_orders  REAL NOT NULL DEFAULT 0,
    manual_adjustment      REAL NOT NULL DEFAULT 0,
    manual_adjustment_note TEXT,
    total           REAL NOT NULL DEFAULT 0,
    note            TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(snapshot_date)
  )`);

  // マイグレーション: 既存 inv_snapshot に新列を後付けする。
  //   - fba_us_inbound         ⑤ 米国FBA在庫輸送中（手動金額・税抜、マイナス不可）
  //   - manual_adjustment      ⑥ 手動調整在庫金額（符号付き＝マイナス可）
  //   - manual_adjustment_note ⑥ のメモ
  // ALTER TABLE ADD COLUMN は定数DEFAULTなら NOT NULL でも可。既存行は 0 / NULL になる。
  const invCols = new Set(db.prepare('PRAGMA table_info(inv_snapshot)').all().map(c => c.name));
  if (!invCols.has('fba_us_inbound')) db.exec('ALTER TABLE inv_snapshot ADD COLUMN fba_us_inbound REAL NOT NULL DEFAULT 0');
  if (!invCols.has('manual_adjustment')) db.exec('ALTER TABLE inv_snapshot ADD COLUMN manual_adjustment REAL NOT NULL DEFAULT 0');
  if (!invCols.has('manual_adjustment_note')) db.exec('ALTER TABLE inv_snapshot ADD COLUMN manual_adjustment_note TEXT');

  db.exec(`CREATE TABLE IF NOT EXISTS inv_snapshot_detail (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id     INTEGER NOT NULL,
    category        TEXT NOT NULL,
    seller_sku      TEXT,
    商品コード      TEXT,
    商品名          TEXT,
    数量            INTEGER NOT NULL,
    原価            REAL NOT NULL,
    金額            REAL NOT NULL,
    原価状態        TEXT,
    FOREIGN KEY(snapshot_id) REFERENCES inv_snapshot(id) ON DELETE CASCADE
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_detail_snap ON inv_snapshot_detail(snapshot_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_detail_cat ON inv_snapshot_detail(snapshot_id, category)');

  db.exec(`CREATE TABLE IF NOT EXISTS inv_snapshot_pending (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id     INTEGER NOT NULL,
    supplier_name   TEXT NOT NULL,
    amount          REAL NOT NULL,
    note            TEXT,
    FOREIGN KEY(snapshot_id) REFERENCES inv_snapshot(id) ON DELETE CASCADE
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_pending_snap ON inv_snapshot_pending(snapshot_id)');

  initialized = true;
  return db;
}

export function getDB() {
  if (!initialized) initInventoryMonthly();
  return getMirrorDB();
}
