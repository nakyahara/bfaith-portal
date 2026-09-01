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

  // 原価の手入力履歴 (2026-09 追加)。
  // 月末保存時に原価が取れず 0 円計上になった明細へ、履歴詳細ページから原価を手入力した記録。
  //   - 監査証跡 (誰がいつ何円を入れたか = 合計が変わった理由)
  //   - 翌月も同じ商品がマスタ未登録のままなら「前回入力値」として提示する
  // item_key = 'code:<商品コード小文字>' or 'sku:<seller_sku小文字>' (商品コード未解決の Amazon SKU)
  // ⚠️ inv_snapshot への FK は張らない (Codex R1 High #2): CSV 再アップロード / force 再保存は
  //   同日 snapshot を DELETE→INSERT で作り直すため、FK CASCADE だと監査記録ごと消える。
  //   このテーブルは追記専用のログとして snapshot の世代を跨いで残す (snapshot_date で引く)。
  db.exec(`CREATE TABLE IF NOT EXISTS inv_snapshot_cost_fix (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id     INTEGER NOT NULL,
    snapshot_date   TEXT NOT NULL,
    item_key        TEXT NOT NULL,
    商品コード      TEXT,
    seller_sku      TEXT,
    商品名          TEXT,
    原価            REAL NOT NULL,
    rows_updated    INTEGER NOT NULL,
    delta_value     REAL NOT NULL,
    created_by      TEXT,
    created_at      TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_cost_fix_snap ON inv_snapshot_cost_fix(snapshot_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inv_cost_fix_key ON inv_snapshot_cost_fix(item_key, snapshot_date, id)');

  fixMislabeledSnapshotDates20260801(db);

  initialized = true;
  return db;
}

/**
 * 一回限りのデータ訂正 (2026-08-01) — 月末棚卸し履歴の基準日ズレ修復。
 *
 * 月初にCSVをアップした際、日付欄のデフォルトが「当月末」だったため、
 * 前月末データが翌月末の日付ラベルで保存されていた (デフォルトは #655 で修正済み):
 *   誤 2026-06-30 (created_at 2026-05-31T22:38 UTC = 6/1朝JST作成) → 正 2026-05-31
 *   誤 2026-07-31 (created_at 2026-07-01T00:50 UTC = 7/1朝JST作成) → 正 2026-06-30
 * 中身のCSVは月初朝DL = 前月末断面なので、ラベルの付け替えだけで正しくなる
 * (発注後未着・手動調整もそのまま正しい月に付いて残る)。
 *
 * ガード: 「2026-05-31 が存在しない」かつ「対象2行が上記 created_at で存在する」
 * 場合のみ実行。実行後・別環境 (miniPC/テスト等の同居コード) ではガードが成立せず
 * 完全に no-op なので冪等。訂正失敗でもツール全体は止めない (次回 init 時に再試行)。
 */
function fixMislabeledSnapshotDates20260801(db) {
  try {
    const get = (d) => db.prepare('SELECT id, created_at, total FROM inv_snapshot WHERE snapshot_date = ?').get(d);
    const r531 = get('2026-05-31');
    const r630 = get('2026-06-30');
    const r731 = get('2026-07-31');
    if (r531 || !r630 || !r731) return;
    // 本番Render DBで確認済みの実値に完全一致した場合のみ実行 (Codex High:
    // 日単位のstartsWith緩和ガードだと同居環境で偶然一致→正常履歴を誤って動かし得る)
    if (r630.created_at !== '2026-05-31 22:38:09' || r630.total !== 230256859) return;
    if (r731.created_at !== '2026-07-01 00:50:37' || r731.total !== 227113054 || r731.id !== 26) return;
    const upd = db.prepare("UPDATE inv_snapshot SET snapshot_date = ?, note = COALESCE(note || ' ', '') || ? WHERE id = ?");
    db.transaction(() => {
      upd.run('2026-05-31', '[日付訂正 2026-08-01: 誤6/30→正5/31 (月初アップ時の日付デフォルト事故)]', r630.id);
      upd.run('2026-06-30', '[日付訂正 2026-08-01: 誤7/31→正6/30 (月初アップ時の日付デフォルト事故)]', r731.id);
    })();
    console.log(`[inventory-monthly] 日付訂正migration実行: id=${r630.id} 6/30→5/31, id=${r731.id} 7/31→6/30`);
  } catch (e) {
    console.error('[inventory-monthly] 日付訂正migration失敗 (次回initで再試行):', e.message);
  }
}

export function getDB() {
  if (!initialized) initInventoryMonthly();
  return getMirrorDB();
}
