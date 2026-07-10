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

  // PK は正規化キー (product_key)。大文字小文字/空白違いの重複登録を1行に collapse する (Codex R1 Medium)
  db.exec(`CREATE TABLE IF NOT EXISTS po_product_attrs (
    product_key       TEXT PRIMARY KEY,
    product_code      TEXT NOT NULL,
    condition_id      TEXT,
    material_group_id TEXT,
    capacity_per_unit REAL CHECK(capacity_per_unit IS NULL OR capacity_per_unit > 0),
    case_group        TEXT,
    case_lot          REAL CHECK(case_lot IS NULL OR case_lot > 0),
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_attrs_cond ON po_product_attrs(condition_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_attrs_mat ON po_product_attrs(material_group_id)');

  // 「選べる◯種セット」構成商品 (楽天/Yahooの項目選択肢セット販売、在庫を切らさない前提の商品)。
  // 在庫+注残 が min_stock 以下になったら要発注リストに「選べるセット構成の在庫減」として載せる。
  // min_stock NULL は既定値 (logic.js SELECTABLE_DEFAULT_MIN) を使う。
  db.exec(`CREATE TABLE IF NOT EXISTS po_selectable_products (
    product_key   TEXT PRIMARY KEY,
    product_code  TEXT NOT NULL,
    set_names     TEXT,
    min_stock     REAL CHECK(min_stock IS NULL OR min_stock >= 0),
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
  )`);

  // NE商品マスタCSV オーバーレイ (日中に手動DLした最新値で 在庫/注残/原価等 を上書き計算する)
  // 正本は毎朝の PML。overlay は「その日の作業用の一時データ」で、翌朝同期より古くなったら自動で無視される。
  db.exec(`CREATE TABLE IF NOT EXISTS po_ne_overlay_meta (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    uploaded_at   TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    filename      TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS po_ne_overlay_rows (
    product_key   TEXT PRIMARY KEY,
    product_code  TEXT NOT NULL,
    仕入先コード  TEXT,
    原価          REAL,
    売価          REAL,
    取扱区分      TEXT,
    発注ロット単位 REAL,
    最終仕入日    TEXT,
    在庫数        REAL,
    引当数        REAL,
    発注残数      REAL
  )`);

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
  // 発注時点の発注条件グループのスナップショット (月次上限=出荷制限の累計判定用)。既存DBには後付け
  try { db.exec('ALTER TABLE po_order_items ADD COLUMN condition_id TEXT'); } catch (e) { /* 既に存在 */ }

  initLedgerSchema(db);

  initialized = true;
  return db;
}

/** 既存テーブルへの後付け列 (冪等)。ALTERはCHECKを追加できないため列間整合はアプリ層+整合性検査で担保する */
function addCol(db, table, colDef) {
  try { db.exec(`ALTER TABLE ${table} ADD COLUMN ${colDef}`); } catch (e) { /* 既に存在 */ }
}

/**
 * P13a: 発注ライフサイクル (発注残・消込) の台帳スキーマ。
 * 設計正本 = Downloads『発注管理システム_要件定義_20260710.md』v8。
 *
 * 柱:
 *  - 数量イベント台帳 po_item_events (append-only) が唯一の正本。型は receipt/shortage/cancel/reversal の4種
 *  - 逆仕訳は全量のみ・1イベント1回 (UNIQUE(reverses_id) + INSERTトリガで同一明細・同数量・同業務日付を強制)
 *  - ヘッダの closed は closed_at IS NOT NULL が正 (既存 status CHECK には触れない=テーブル再作成回避)。
 *    close_reason は保存せず導出 (残0かつ有効cutoffあり→manual / なし→completed)
 *  - 残数は v_po_item_balance ビューで導出 (キャッシュ列なし)。⚠️ tracked 判定は呼び出し側が
 *    po_orders (issued_at >= settings.tracking_started_at) で絞ること。legacy 行の残数に意味はない
 *  - 移行境界: tracking_started_at 以後に issued されたPOのみ残数管理 (それ以前=legacy、イベント登録拒否)
 */
function initLedgerSchema(db) {
  // ── 既存テーブル拡張 (nullable のみ) ──
  addCol(db, 'po_orders', 'requested_date TEXT');   // 希望納期 (確定時に明細へスナップショット)
  addCol(db, 'po_orders', 'closed_at TEXT');        // NULL=オープン。閉鎖遷移時のUTC時刻 (再オープンでNULL)
  addCol(db, 'po_orders', 'po_number TEXT');        // issue時に採番 (PO-YYYY-NNNN)
  addCol(db, 'po_orders', 'tracking_mode TEXT');    // 'tracked' = 発行時に固定される業務属性。正は issued_at>=境界
  addCol(db, 'po_orders', 'origin TEXT');           // NULL / 'migration' (移行専用PO)
  addCol(db, 'po_orders', 'send_blocked INTEGER');  // 1=メール/発注書出力の対象外 (移行PO誤発注防止)
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_po_orders_number ON po_orders(po_number) WHERE po_number IS NOT NULL`);

  addCol(db, 'po_order_items', 'requested_date TEXT');        // 明細単位希望納期 (確定時スナップショット、issued後不変)
  addCol(db, 'po_order_items', 'promised_date TEXT');         // 仕入先回答納期 (最新値。履歴=po_item_history)
  addCol(db, 'po_order_items', 'next_expected_date TEXT');    // 次回入荷予定日 (分納)
  addCol(db, 'po_order_items', 'next_expected_qty INTEGER');
  addCol(db, 'po_order_items', 'next_action_date TEXT');      // 確認中の期限
  addCol(db, 'po_order_items', 'remainder_disposition TEXT'); // awaiting_delivery / awaiting_confirmation / NULL

  // ── 数量イベント台帳 (append-only) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_item_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_item_id   INTEGER NOT NULL REFERENCES po_order_items(id),
    event_type      TEXT NOT NULL CHECK(event_type IN ('receipt','shortage','cancel','reversal')),
    qty             INTEGER NOT NULL CHECK(qty > 0),
    source          TEXT CHECK(source IS NULL OR source IN ('manual','logizard','migration')),
    reason_code     TEXT,
    note            TEXT,
    inbound_item_id INTEGER,
    reverses_id     INTEGER REFERENCES po_item_events(id),
    effective_date  TEXT NOT NULL CHECK(effective_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    recorded_at     TEXT NOT NULL,
    actor_type      TEXT NOT NULL CHECK(actor_type IN ('user','system','ai_agent','migration')),
    actor           TEXT,
    CHECK ((event_type = 'reversal') = (reverses_id IS NOT NULL)),
    CHECK (event_type <> 'receipt' OR source IS NOT NULL),
    CHECK (event_type <> 'receipt' OR (source = 'logizard') = (inbound_item_id IS NOT NULL)),
    CHECK (event_type = 'receipt' OR (inbound_item_id IS NULL AND source IS NULL)),
    CHECK (event_type <> 'receipt' OR reason_code IS NULL),
    CHECK (event_type <> 'shortage' OR reason_code IN ('supplier_shortage','own_decision','cutoff','other')),
    CHECK (event_type <> 'cancel' OR reason_code IS NULL),
    CHECK (event_type <> 'reversal' OR (reason_code = 'correction' AND NULLIF(TRIM(note),'') IS NOT NULL)),
    CHECK (event_type <> 'shortage' OR reason_code <> 'other' OR NULLIF(TRIM(note),'') IS NOT NULL)
  )`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_po_events_reversal ON po_item_events(reverses_id) WHERE reverses_id IS NOT NULL`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_events_item ON po_item_events(order_item_id, event_type)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_events_inbound ON po_item_events(inbound_item_id)');

  // append-only 強制 (保守はメンテモード=トリガ一時DROPでのみ解除)
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_po_events_no_update BEFORE UPDATE ON po_item_events
           BEGIN SELECT RAISE(ABORT, 'po_item_events is append-only'); END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_po_events_no_delete BEFORE DELETE ON po_item_events
           BEGIN SELECT RAISE(ABORT, 'po_item_events is append-only'); END`);
  // 逆仕訳の不変条件 (要件H-2): 元イベントが同一明細・非reversal・同数量・同業務日付。多重逆仕訳はUNIQUEが防ぐ
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_po_events_reversal_check BEFORE INSERT ON po_item_events
           WHEN NEW.event_type = 'reversal'
           BEGIN
             SELECT CASE WHEN NOT EXISTS (
               SELECT 1 FROM po_item_events o
               WHERE o.id = NEW.reverses_id AND o.order_item_id = NEW.order_item_id
                 AND o.event_type <> 'reversal' AND o.qty = NEW.qty AND o.effective_date = NEW.effective_date
             ) THEN RAISE(ABORT, 'invalid reversal: 元イベント不一致 (明細/数量/業務日付/種別)') END;
           END`);

  // ── 明細変更履歴 (append-only。change_id で同一操作の複数フィールド変更を束ねる) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_item_history (
    id            INTEGER PRIMARY KEY,
    order_item_id INTEGER NOT NULL REFERENCES po_order_items(id),
    change_id     TEXT NOT NULL,
    field         TEXT NOT NULL CHECK(field IN ('promised_date','next_expected_date','next_expected_qty','remainder_disposition','next_action_date')),
    old_value     TEXT,
    new_value     TEXT,
    note          TEXT,
    created_at    TEXT NOT NULL,
    actor         TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_ih_item ON po_item_history(order_item_id, created_at)');
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_po_ih_no_update BEFORE UPDATE ON po_item_history
           BEGIN SELECT RAISE(ABORT, 'po_item_history is append-only'); END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_po_ih_no_delete BEFORE DELETE ON po_item_history
           BEGIN SELECT RAISE(ABORT, 'po_item_history is append-only'); END`);

  // ── write API の冪等化 (Idempotency-Key はコマンド単位・全体UNIQUE) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_commands (
    idempotency_key TEXT PRIMARY KEY,
    request_hash    TEXT NOT NULL,
    result_json     TEXT,
    created_at      TEXT NOT NULL
  )`);

  // ── PO番号採番 (JST年ごと連番。欠番許容) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_number_sequences (
    year        INTEGER PRIMARY KEY,
    last_number INTEGER NOT NULL CHECK(last_number >= 0)
  )`);

  // ── 設定 (現在値テーブル。変更は前後値を po_audit_log に同一txn記録) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_settings (
    key          TEXT PRIMARY KEY,
    value        TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    changed_by   TEXT,
    reason       TEXT
  )`);

  // ── 監査ログ (専用履歴のない操作に限定: 設定変更・手動クローズ・再オープン・移行操作) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_type  TEXT NOT NULL CHECK(actor_type IN ('user','system','ai_agent','migration')),
    actor       TEXT,
    action      TEXT NOT NULL,
    resource    TEXT NOT NULL,
    detail_json TEXT,
    request_id  TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_audit ON po_audit_log(resource, occurred_at)');

  // ── 発行の世代ゲート (要件C-1): 境界設定後は必要列 (po_number/issued_at/tracking_mode) の無い issued を物理拒否。
  //    旧コード・保守SQLによる不完全発行を防ぐ。境界前 (未設定) は既存挙動のまま ──
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_po_orders_issue_gate_ins BEFORE INSERT ON po_orders
           WHEN NEW.status = 'issued'
             AND EXISTS (SELECT 1 FROM po_settings WHERE key = 'tracking_started_at')
             AND (NEW.po_number IS NULL OR NEW.issued_at IS NULL OR NEW.tracking_mode IS NULL)
           BEGIN SELECT RAISE(ABORT, 'issue gate: po_number/issued_at/tracking_mode が必要です (旧経路からの発行は禁止)'); END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_po_orders_issue_gate_upd BEFORE UPDATE OF status ON po_orders
           WHEN NEW.status = 'issued' AND OLD.status <> 'issued'
             AND EXISTS (SELECT 1 FROM po_settings WHERE key = 'tracking_started_at')
             AND (NEW.po_number IS NULL OR NEW.issued_at IS NULL OR NEW.tracking_mode IS NULL)
           BEGIN SELECT RAISE(ABORT, 'issue gate: po_number/issued_at/tracking_mode が必要です (旧経路からの発行は禁止)'); END`);

  // ── 残数の集約ビュー (唯一の導出定義。有効イベント = 非reversal かつ 未逆仕訳) ──
  // ⚠️ 全明細が対象になる。tracked 判定 (issued_at>=境界) は呼び出し側で po_orders と JOIN して絞ること
  db.exec('DROP VIEW IF EXISTS v_po_item_balance');
  db.exec(`CREATE VIEW v_po_item_balance AS
    SELECT
      i.id            AS order_item_id,
      i.order_id      AS order_id,
      i.qty           AS ordered_qty,
      COALESCE(SUM(CASE WHEN e.event_type = 'receipt'  THEN e.qty END), 0) AS received_qty,
      COALESCE(SUM(CASE WHEN e.event_type = 'shortage' THEN e.qty END), 0) AS shortage_qty,
      COALESCE(SUM(CASE WHEN e.event_type = 'cancel'   THEN e.qty END), 0) AS cancelled_qty,
      COALESCE(SUM(CASE WHEN e.event_type = 'shortage' AND e.reason_code = 'cutoff' THEN e.qty END), 0) AS cutoff_qty,
      i.qty - COALESCE(SUM(CASE WHEN e.event_type IN ('receipt','shortage','cancel') THEN e.qty END), 0) AS remaining_qty
    FROM po_order_items i
    LEFT JOIN po_item_events e
      ON e.order_item_id = i.id
     AND e.event_type <> 'reversal'
     AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)
    GROUP BY i.id`);
}

export function getDB() {
  if (!initialized) initPurchaseOrders();
  return getMirrorDB();
}
