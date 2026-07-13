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

  // スキーマ移行は原子的に (途中失敗で半端な世代を残さない。ビューのDROP/CREATE含む)
  db.transaction(() => initLedgerSchema(db))();

  initialized = true;
  return db;
}

/** 既存テーブルへの後付け列 (冪等)。列存在はメタデータ照会で確認する (例外握り潰しでエラー種別を誤認しない)。
 *  ALTERはCHECKを追加できないため、列間整合はトリガ+アプリ層+整合性検査で担保する */
function addCol(db, table, colName, colDef) {
  const has = db.prepare(`PRAGMA table_info(${table})`).all().some(c => c.name === colName);
  if (!has) db.exec(`ALTER TABLE ${table} ADD COLUMN ${colName} ${colDef}`);
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
  addCol(db, 'po_orders', 'requested_date', 'TEXT');   // 希望納期 (確定時に明細へスナップショット)
  addCol(db, 'po_orders', 'closed_at', 'TEXT');        // NULL=オープン。閉鎖遷移時のUTC時刻 (再オープンでNULL)
  addCol(db, 'po_orders', 'po_number', 'TEXT');        // issue時に採番 (PO-YYYY-NNNN)
  addCol(db, 'po_orders', 'tracking_mode', 'TEXT');    // 'tracked' = 発行時に固定される業務属性。正は issued_at>=境界
  addCol(db, 'po_orders', 'origin', 'TEXT');           // NULL / 'migration' (移行専用PO) / 'supplement' (確定後の電話等追加分)
  addCol(db, 'po_orders', 'send_blocked', 'INTEGER');  // 1=メール/発注書出力の対象外 (移行PO誤発注防止。supplementは任意)
  addCol(db, 'po_orders', 'ne_slip_number', 'TEXT');   // NE発注伝票番号 (NE発注残初期取込の冪等キー。移行PO以外はNULL)
  addCol(db, 'po_orders', 'parent_order_id', 'INTEGER'); // 追加発注 (origin='supplement') の元PO。それ以外はNULL
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_po_orders_number ON po_orders(po_number) WHERE po_number IS NOT NULL`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_po_orders_ne_slip ON po_orders(ne_slip_number) WHERE ne_slip_number IS NOT NULL`);

  // FBA更新ジョブの完了検知記録 (jobId単位で一度だけ発注サイクルをリセットする冪等キー)
  db.exec(`CREATE TABLE IF NOT EXISTS po_cycle_fba_jobs (
    job_id  TEXT PRIMARY KEY,
    done_at TEXT NOT NULL
  )`);

  addCol(db, 'po_order_items', 'requested_date', 'TEXT');        // 明細単位希望納期 (確定時スナップショット、issued後不変)
  addCol(db, 'po_order_items', 'promised_date', 'TEXT');         // 仕入先回答納期 (最新値。履歴=po_item_history)
  addCol(db, 'po_order_items', 'next_expected_date', 'TEXT');    // 次回入荷予定日 (分納)
  addCol(db, 'po_order_items', 'next_expected_qty', 'INTEGER');
  addCol(db, 'po_order_items', 'next_action_date', 'TEXT');      // 確認中の期限
  addCol(db, 'po_order_items', 'remainder_disposition', 'TEXT'); // awaiting_delivery / awaiting_confirmation / NULL

  // 台帳トリガは毎起動でDROP→CREATEし常に最新定義にする (Codex P13a-R3 High-1:
  // IF NOT EXISTSでは初期化済みDBに定義修正が反映されず、旧名の残骸も残る)。init全体が1txnなので原子的
  for (const t of db.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_po_%'").all()) {
    db.exec(`DROP TRIGGER IF EXISTS ${t.name}`);
  }

  // 発行済みPO・明細の不変性 (Codex P13a-R1 High-1): 台帳の基準値 (発注数・商品・発行属性) を
  // 直接SQL・保守スクリプトからも守る。数量減=取消イベント、数量増=新規発注が唯一の経路
  db.exec(`CREATE TRIGGER trg_po_orders_issued_immutable
           BEFORE UPDATE OF status, issued_at, po_number, tracking_mode, requested_date, supplier_code, origin, ne_slip_number, parent_order_id ON po_orders
           WHEN OLD.status = 'issued' AND (
             NEW.status IS NOT OLD.status OR NEW.issued_at IS NOT OLD.issued_at OR
             NEW.po_number IS NOT OLD.po_number OR NEW.tracking_mode IS NOT OLD.tracking_mode OR
             NEW.requested_date IS NOT OLD.requested_date OR NEW.supplier_code IS NOT OLD.supplier_code OR
             NEW.origin IS NOT OLD.origin OR NEW.ne_slip_number IS NOT OLD.ne_slip_number OR
             NEW.parent_order_id IS NOT OLD.parent_order_id
           )
           BEGIN SELECT RAISE(ABORT, 'issued order is immutable (発行済みPOの発行属性は変更不可)'); END`);
  db.exec(`CREATE TRIGGER trg_po_items_issued_immutable
           BEFORE UPDATE OF qty, order_id, product_code, product_key, product_name, unit_cost, requested_date, condition_id ON po_order_items
           WHEN (SELECT status FROM po_orders WHERE id = OLD.order_id) = 'issued' AND (
             NEW.qty IS NOT OLD.qty OR NEW.order_id IS NOT OLD.order_id OR
             NEW.product_code IS NOT OLD.product_code OR NEW.product_key IS NOT OLD.product_key OR
             NEW.product_name IS NOT OLD.product_name OR NEW.unit_cost IS NOT OLD.unit_cost OR
             NEW.requested_date IS NOT OLD.requested_date OR NEW.condition_id IS NOT OLD.condition_id
           )
           BEGIN SELECT RAISE(ABORT, 'issued item is immutable (発行済み明細の数量/商品/単価は変更不可。数量減=取消イベント)'); END`);
  db.exec(`CREATE TRIGGER trg_po_items_issued_no_delete
           BEFORE DELETE ON po_order_items
           WHEN (SELECT status FROM po_orders WHERE id = OLD.order_id) = 'issued'
           BEGIN SELECT RAISE(ABORT, 'issued item is immutable (発行済み明細は削除不可)'); END`);
  db.exec(`CREATE TRIGGER trg_po_orders_issued_no_delete
           BEFORE DELETE ON po_orders
           WHEN OLD.status = 'issued'
           BEGIN SELECT RAISE(ABORT, 'issued order is immutable (発行済みPOは削除不可)'); END`);
  // 発行済みPOへの明細追加は無条件拒否 (issueOrderは draft で明細を作ってから issued へ遷移する)
  db.exec(`CREATE TRIGGER trg_po_items_issued_no_insert2
           BEFORE INSERT ON po_order_items
           WHEN (SELECT status FROM po_orders WHERE id = NEW.order_id) = 'issued'
           BEGIN SELECT RAISE(ABORT, 'issued order is immutable (発行済みPOへの明細追加は不可。追加発注は新規POで)'); END`);
  // origin属性の列間規則 (Codex NE取込R1 Medium-5): origin='migration' ⇔ ne_slip_number 必須 + send_blocked=1。
  // origin='supplement' (確定後の電話等追加分) ⇔ parent_order_id 必須 + 親は発行済み。
  // 直接SQL・別コード経路からメール送信可能な移行POや、親のない/親が未発行の追加発注POを作らせない
  const ORIGIN_RULES = `
             SELECT CASE
               WHEN NEW.origin IS NOT NULL AND NEW.origin NOT IN ('migration', 'supplement')
                 THEN RAISE(ABORT, 'origin rules: origin は NULL/migration/supplement のみ')
               WHEN NEW.origin = 'migration' AND (NEW.ne_slip_number IS NULL OR trim(NEW.ne_slip_number) = '' OR NEW.send_blocked IS NOT 1)
                 THEN RAISE(ABORT, 'origin rules: 移行POは ne_slip_number と send_blocked=1 が必須')
               WHEN NEW.ne_slip_number IS NOT NULL AND NEW.origin IS NOT 'migration'
                 THEN RAISE(ABORT, 'origin rules: ne_slip_number は移行POのみ')
               WHEN NEW.origin = 'supplement' AND NEW.parent_order_id IS NULL
                 THEN RAISE(ABORT, 'origin rules: 追加発注POは parent_order_id が必須')
               WHEN NEW.parent_order_id IS NOT NULL AND NEW.origin IS NOT 'supplement'
                 THEN RAISE(ABORT, 'origin rules: parent_order_id は追加発注POのみ')
               WHEN NEW.parent_order_id IS NOT NULL AND NEW.parent_order_id = NEW.id
                 THEN RAISE(ABORT, 'origin rules: 自分自身を親にはできません')
               WHEN NEW.parent_order_id IS NOT NULL
                    AND (SELECT status FROM po_orders WHERE id = NEW.parent_order_id) IS NOT 'issued'
                 THEN RAISE(ABORT, 'origin rules: 親POが存在しないか発行済みではありません')
             END;`;
  db.exec(`CREATE TRIGGER trg_po_orders_migration_attrs_ins BEFORE INSERT ON po_orders
           BEGIN ${ORIGIN_RULES} END`);
  db.exec(`CREATE TRIGGER trg_po_orders_migration_attrs_upd BEFORE UPDATE OF origin, ne_slip_number, send_blocked, parent_order_id ON po_orders
           BEGIN ${ORIGIN_RULES} END`);
  // disposition/next_* の列間規則 (Codex P13a-R2 Medium-1): 直接SQLでも規則違反を保存できない。
  // INSERT側も同じ規則で保護 (draft明細に不正状態を仕込んでissueで固定させない、R3 Medium-1)。
  // next_expected_qty ≤ 残数 は集計を要するためアプリ層+整合性検査で担保
  db.exec(`CREATE TRIGGER trg_po_items_plan_rules_ins
           BEFORE INSERT ON po_order_items
           BEGIN
             SELECT CASE
               WHEN NEW.remainder_disposition IS NOT NULL
                    AND NEW.remainder_disposition NOT IN ('awaiting_delivery','awaiting_confirmation')
                 THEN RAISE(ABORT, 'plan rules: disposition不正')
               WHEN NEW.remainder_disposition = 'awaiting_delivery'
                    AND (NEW.next_expected_date IS NULL OR NEW.next_expected_qty IS NULL OR NEW.next_action_date IS NOT NULL)
                 THEN RAISE(ABORT, 'plan rules: 分納待ちは次回予定日+数量が必須 (期限はNULL)')
               WHEN NEW.remainder_disposition = 'awaiting_confirmation'
                    AND (NEW.next_action_date IS NULL OR NEW.next_expected_date IS NOT NULL OR NEW.next_expected_qty IS NOT NULL)
                 THEN RAISE(ABORT, 'plan rules: 確認中は期限が必須 (次回予定はNULL)')
               WHEN NEW.remainder_disposition IS NULL
                    AND (NEW.next_expected_date IS NOT NULL OR NEW.next_expected_qty IS NOT NULL OR NEW.next_action_date IS NOT NULL)
                 THEN RAISE(ABORT, 'plan rules: dispositionなしでnext_*は保存不可')
               WHEN NEW.next_expected_qty IS NOT NULL AND NEW.next_expected_qty <= 0
                 THEN RAISE(ABORT, 'plan rules: 次回予定数量は正の整数')
             END;
           END`);
  db.exec(`CREATE TRIGGER trg_po_items_plan_rules
           BEFORE UPDATE OF remainder_disposition, next_expected_date, next_expected_qty, next_action_date ON po_order_items
           BEGIN
             SELECT CASE
               WHEN NEW.remainder_disposition IS NOT NULL
                    AND NEW.remainder_disposition NOT IN ('awaiting_delivery','awaiting_confirmation')
                 THEN RAISE(ABORT, 'plan rules: disposition不正')
               WHEN NEW.remainder_disposition = 'awaiting_delivery'
                    AND (NEW.next_expected_date IS NULL OR NEW.next_expected_qty IS NULL OR NEW.next_action_date IS NOT NULL)
                 THEN RAISE(ABORT, 'plan rules: 分納待ちは次回予定日+数量が必須 (期限はNULL)')
               WHEN NEW.remainder_disposition = 'awaiting_confirmation'
                    AND (NEW.next_action_date IS NULL OR NEW.next_expected_date IS NOT NULL OR NEW.next_expected_qty IS NOT NULL)
                 THEN RAISE(ABORT, 'plan rules: 確認中は期限が必須 (次回予定はNULL)')
               WHEN NEW.remainder_disposition IS NULL
                    AND (NEW.next_expected_date IS NOT NULL OR NEW.next_expected_qty IS NOT NULL OR NEW.next_action_date IS NOT NULL)
                 THEN RAISE(ABORT, 'plan rules: dispositionなしでnext_*は保存不可')
               WHEN NEW.next_expected_qty IS NOT NULL AND NEW.next_expected_qty <= 0
                 THEN RAISE(ABORT, 'plan rules: 次回予定数量は正の整数')
             END;
           END`);

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
    recorded_at     TEXT NOT NULL CHECK(recorded_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*Z'),
    actor_type      TEXT NOT NULL CHECK(actor_type IN ('user','system','ai_agent','migration')),
    actor           TEXT,
    -- ⚠️ SQLiteのCHECKは式がNULLだと通る。NULLになり得る列の比較は IS / IS NOT NULL で明示する (Codex P13a-R3 High-2)
    CHECK ((event_type = 'reversal') = (reverses_id IS NOT NULL)),
    CHECK (event_type <> 'receipt' OR source IS NOT NULL),
    CHECK (event_type <> 'receipt' OR (source = 'logizard') = (inbound_item_id IS NOT NULL)),
    CHECK (event_type = 'receipt' OR (inbound_item_id IS NULL AND source IS NULL)),
    CHECK (event_type <> 'receipt' OR reason_code IS NULL),
    CHECK (event_type <> 'shortage' OR (reason_code IS NOT NULL AND reason_code IN ('supplier_shortage','own_decision','cutoff','other'))),
    CHECK (event_type <> 'cancel' OR reason_code IS NULL),
    CHECK (event_type <> 'reversal' OR (reason_code IS 'correction' AND NULLIF(TRIM(note),'') IS NOT NULL)),
    CHECK (event_type <> 'shortage' OR reason_code IS NOT 'other' OR NULLIF(TRIM(note),'') IS NOT NULL)
  )`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_po_events_reversal ON po_item_events(reverses_id) WHERE reverses_id IS NOT NULL`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_events_item ON po_item_events(order_item_id, event_type)');
  // ⚠️ inbound_item_id にUNIQUEは張らない: 1入庫行→複数PO明細への分割割当を許す仕様 (要件F-5)。
  //    二重計上防止は P14 で「割当合計 ≤ 入庫良品数」のtxn内検証+整合性検査として実装する
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_events_inbound ON po_item_events(inbound_item_id)');

  // append-only 強制 (保守はメンテモード=トリガ一時DROPでのみ解除)
  db.exec(`CREATE TRIGGER trg_po_events_no_update BEFORE UPDATE ON po_item_events
           BEGIN SELECT RAISE(ABORT, 'po_item_events is append-only'); END`);
  db.exec(`CREATE TRIGGER trg_po_events_no_delete BEFORE DELETE ON po_item_events
           BEGIN SELECT RAISE(ABORT, 'po_item_events is append-only'); END`);
  // 逆仕訳の不変条件 (要件H-2): 元イベントが同一明細・非reversal・同数量・同業務日付。多重逆仕訳はUNIQUEが防ぐ
  db.exec(`CREATE TRIGGER trg_po_events_reversal_check BEFORE INSERT ON po_item_events
           WHEN NEW.event_type = 'reversal'
           BEGIN
             SELECT CASE WHEN NOT EXISTS (
               SELECT 1 FROM po_item_events o
               WHERE o.id = NEW.reverses_id AND o.order_item_id = NEW.order_item_id
                 AND o.event_type <> 'reversal' AND o.qty = NEW.qty AND o.effective_date = NEW.effective_date
             ) THEN RAISE(ABORT, 'invalid reversal: 元イベント不一致 (明細/数量/業務日付/種別)') END;
           END`);
  // 全イベント共通の対象スコープ (Codex P13a-R1 High-2): 親POが issued かつ tracked (境界以後)。
  // アプリ層 (appendPoItemEvent) と同じ不変条件をDB側でも強制し、直接SQL・旧コードの書込を拒否する
  db.exec(`CREATE TRIGGER trg_po_events_scope BEFORE INSERT ON po_item_events
           BEGIN
             SELECT CASE WHEN NOT EXISTS (
               SELECT 1 FROM po_order_items i JOIN po_orders o ON o.id = i.order_id
               WHERE i.id = NEW.order_item_id AND o.status = 'issued'
                 AND o.issued_at >= (SELECT value FROM po_settings WHERE key = 'tracking_started_at')
             ) THEN RAISE(ABORT, 'event scope: tracked な issued 明細のみイベント登録できます') END;
           END`);
  // 通常イベント (非reversal): クローズ済みへは不可、残数超過 (負残) を物理拒否
  db.exec(`CREATE TRIGGER trg_po_events_normal_check BEFORE INSERT ON po_item_events
           WHEN NEW.event_type <> 'reversal'
           BEGIN
             SELECT CASE WHEN (SELECT o.closed_at FROM po_orders o JOIN po_order_items i ON i.order_id = o.id WHERE i.id = NEW.order_item_id) IS NOT NULL
               THEN RAISE(ABORT, 'event scope: クローズ済みPOへの通常イベントは不可 (訂正は逆仕訳で)') END;
             SELECT CASE WHEN NEW.qty > (
               SELECT i.qty - COALESCE(SUM(CASE WHEN e.event_type IN ('receipt','shortage','cancel') THEN e.qty END), 0)
               FROM po_order_items i
               LEFT JOIN po_item_events e ON e.order_item_id = i.id AND e.event_type <> 'reversal'
                 AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)
               WHERE i.id = NEW.order_item_id
             ) THEN RAISE(ABORT, 'event insert: 残数超過') END;
           END`);
  // イベント登録後にヘッダの closed_at をDB側で再計算 (Codex P13a-R2 High-2):
  // 直接SQLのイベント登録でも「closedはイベントから導出」の不変条件が崩れない。
  // recursive_triggers=ON のため下の closed_at ガードも発火するが、本トリガは常に整合値を書くので通過する
  db.exec(`CREATE TRIGGER trg_po_events_closure AFTER INSERT ON po_item_events
           BEGIN
             UPDATE po_orders SET
               closed_at = CASE WHEN (
                 SELECT COALESCE(SUM(i.qty), 0) FROM po_order_items i WHERE i.order_id = po_orders.id
               ) - (
                 SELECT COALESCE(SUM(e.qty), 0) FROM po_item_events e
                 JOIN po_order_items i2 ON i2.id = e.order_item_id
                 WHERE i2.order_id = po_orders.id AND e.event_type <> 'reversal'
                   AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)
               ) = 0 THEN COALESCE(closed_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
               ELSE NULL END,
               updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = (SELECT order_id FROM po_order_items WHERE id = NEW.order_item_id);
           END`);
  // closed_at の直接UPDATEガード: 残数と矛盾する開閉・形式不正を拒否 (イベントと無関係な開閉を防ぐ)
  db.exec(`CREATE TRIGGER trg_po_orders_closed_guard
           BEFORE UPDATE OF closed_at ON po_orders
           WHEN NEW.closed_at IS NOT OLD.closed_at
           BEGIN
             SELECT CASE WHEN OLD.closed_at IS NOT NULL AND NEW.closed_at IS NOT NULL
               THEN RAISE(ABORT, 'closed_at guard: 閉鎖時刻の改変は不可') END;
             SELECT CASE WHEN NEW.closed_at IS NOT NULL
               AND (OLD.status <> 'issued' OR NOT EXISTS (SELECT 1 FROM po_order_items i WHERE i.order_id = OLD.id))
               THEN RAISE(ABORT, 'closed_at guard: draft/明細のないPOはクローズできません') END;
             SELECT CASE WHEN NEW.closed_at IS NOT NULL
               AND NEW.closed_at NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*Z'
               THEN RAISE(ABORT, 'closed_at guard: 時刻形式が不正') END;
             SELECT CASE WHEN NEW.closed_at IS NOT NULL AND (
                 SELECT COALESCE(SUM(i.qty), 0) FROM po_order_items i WHERE i.order_id = OLD.id
               ) - (
                 SELECT COALESCE(SUM(e.qty), 0) FROM po_item_events e
                 JOIN po_order_items i2 ON i2.id = e.order_item_id
                 WHERE i2.order_id = OLD.id AND e.event_type <> 'reversal'
                   AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)
               ) > 0 THEN RAISE(ABORT, 'closed_at guard: 残数があるPOはクローズできません') END;
             SELECT CASE WHEN NEW.closed_at IS NULL
               AND EXISTS (SELECT 1 FROM po_order_items i WHERE i.order_id = OLD.id)
               AND (
                 SELECT COALESCE(SUM(i.qty), 0) FROM po_order_items i WHERE i.order_id = OLD.id
               ) - (
                 SELECT COALESCE(SUM(e.qty), 0) FROM po_item_events e
                 JOIN po_order_items i2 ON i2.id = e.order_item_id
                 WHERE i2.order_id = OLD.id AND e.event_type <> 'reversal'
                   AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)
               ) = 0 THEN RAISE(ABORT, 'closed_at guard: 全消込済みPOは再オープンできません (訂正は逆仕訳で)') END;
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
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS uq_po_ih_change ON po_item_history(order_item_id, change_id, field)');
  db.exec(`CREATE TRIGGER trg_po_ih_no_update BEFORE UPDATE ON po_item_history
           BEGIN SELECT RAISE(ABORT, 'po_item_history is append-only'); END`);
  db.exec(`CREATE TRIGGER trg_po_ih_no_delete BEFORE DELETE ON po_item_history
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

  // ── ロジザード入庫実績 (P14。原本=append志向、訂正は supersede+競合キュー) ──
  // 「NE仕入れ取込用データ」CSVには安定した明細番号が無いため、行ID = 内容ハッシュ+同一内容の出現順。
  // 訂正版CSV (同一伝票NOの再出力で行が変わる) は、消えた/変わった行を superseded=1 にし新行を追加する。
  // superseded行に有効な割当 (receiptイベント) が残っていれば「訂正競合」として整合性検査が報告し、
  // 人間が逆仕訳→再割当で解消する (要件v8 F-5 / Codex R5 C-3)。明細番号列が将来来たら安定IDへ移行可能
  db.exec(`CREATE TABLE IF NOT EXISTS po_inbound_batches (
    id            INTEGER PRIMARY KEY,
    file_name     TEXT,
    file_hash     TEXT NOT NULL UNIQUE,
    row_count     INTEGER NOT NULL CHECK(row_count >= 0),
    receipt_count INTEGER NOT NULL DEFAULT 0,
    imported_at   TEXT NOT NULL,
    actor         TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS po_inbound_receipts (
    id            INTEGER PRIMARY KEY,
    source        TEXT NOT NULL DEFAULT 'logizard',
    source_key    TEXT NOT NULL,
    receipt_date  TEXT,
    first_batch_id INTEGER REFERENCES po_inbound_batches(id),
    last_batch_id  INTEGER REFERENCES po_inbound_batches(id),
    imported_at   TEXT NOT NULL,
    UNIQUE(source, source_key)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS po_inbound_items (
    id            INTEGER PRIMARY KEY,
    receipt_id    INTEGER NOT NULL REFERENCES po_inbound_receipts(id),
    line_key      TEXT NOT NULL,
    batch_id      INTEGER NOT NULL REFERENCES po_inbound_batches(id),
    supplier_code TEXT,
    product_code  TEXT NOT NULL,
    product_key   TEXT NOT NULL,
    good_qty      INTEGER NOT NULL CHECK(good_qty >= 0),
    defective_qty INTEGER NOT NULL DEFAULT 0 CHECK(defective_qty >= 0),
    unit_cost     REAL,
    payload_json  TEXT CHECK(payload_json IS NULL OR json_valid(payload_json)),
    superseded    INTEGER NOT NULL DEFAULT 0 CHECK(superseded IN (0,1)),
    superseded_batch_id INTEGER REFERENCES po_inbound_batches(id),
    created_at    TEXT NOT NULL,
    UNIQUE(receipt_id, line_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_inb_match ON po_inbound_items(product_key, superseded)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_inb_receipt ON po_inbound_items(receipt_id)');
  // 「対象外」は履歴型 (誰がいつなぜ・解除も記録)。対象外の判定は revoked_at IS NULL の行が存在するか
  db.exec(`CREATE TABLE IF NOT EXISTS po_inbound_ignores (
    id              INTEGER PRIMARY KEY,
    inbound_item_id INTEGER NOT NULL REFERENCES po_inbound_items(id),
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    actor           TEXT,
    revoked_at      TEXT,
    revoked_by      TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_inb_ign ON po_inbound_ignores(inbound_item_id, revoked_at)');
  // 割当が残る入庫の「対象外」化を物理拒否 (アプリ検証の最終防衛、Codex P14-R1 High-1)
  db.exec(`CREATE TRIGGER trg_po_inbound_ignore_guard BEFORE INSERT ON po_inbound_ignores
           WHEN NEW.revoked_at IS NULL
           BEGIN
             SELECT CASE WHEN (
               SELECT COALESCE(SUM(e.qty), 0) FROM po_item_events e
               WHERE e.inbound_item_id = NEW.inbound_item_id AND e.event_type = 'receipt'
                 AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)
             ) > 0 THEN RAISE(ABORT, 'inbound ignore: 割当が残る入庫は対象外にできません (先に逆仕訳を)') END;
           END`);
  // logizard入荷の割当ガード: 参照先の実在・非supersede・非ignore・割当合計≤入庫良品数 (要件v8 F-5)。
  // ※入庫テーブル作成後に定義する (トリガ本文のテーブル参照はCREATE時に解決される)
  db.exec(`CREATE TRIGGER trg_po_events_inbound_capacity BEFORE INSERT ON po_item_events
           WHEN NEW.event_type = 'receipt' AND NEW.inbound_item_id IS NOT NULL
           BEGIN
             SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM po_inbound_items x WHERE x.id = NEW.inbound_item_id AND x.superseded = 0)
               THEN RAISE(ABORT, 'inbound: 入庫明細が存在しないか、訂正版で無効化されています') END;
             SELECT CASE WHEN EXISTS (SELECT 1 FROM po_inbound_ignores g WHERE g.inbound_item_id = NEW.inbound_item_id AND g.revoked_at IS NULL)
               THEN RAISE(ABORT, 'inbound: 対象外に指定された入庫明細です') END;
             -- 商品・仕入先の一致を書込時にも強制 (候補UIのフィルタは整合性境界ではない、Codex P14-R2 High-1)
             SELECT CASE WHEN (SELECT x.product_key FROM po_inbound_items x WHERE x.id = NEW.inbound_item_id)
               <> (SELECT i.product_key FROM po_order_items i WHERE i.id = NEW.order_item_id)
               THEN RAISE(ABORT, 'inbound: 入庫と発注の商品が一致しません') END;
             SELECT CASE WHEN (SELECT x.supplier_code FROM po_inbound_items x WHERE x.id = NEW.inbound_item_id) IS NOT NULL
               AND (SELECT x.supplier_code FROM po_inbound_items x WHERE x.id = NEW.inbound_item_id)
                 <> (SELECT o.supplier_code FROM po_orders o JOIN po_order_items i ON i.order_id = o.id WHERE i.id = NEW.order_item_id)
               THEN RAISE(ABORT, 'inbound: 入庫と発注の仕入先が一致しません') END;
             SELECT CASE WHEN (
               SELECT COALESCE(SUM(e.qty), 0) FROM po_item_events e
               WHERE e.inbound_item_id = NEW.inbound_item_id AND e.event_type = 'receipt'
                 AND NOT EXISTS (SELECT 1 FROM po_item_events r WHERE r.reverses_id = e.id)
             ) + NEW.qty > (SELECT good_qty FROM po_inbound_items WHERE id = NEW.inbound_item_id)
               THEN RAISE(ABORT, 'inbound: 割当合計が入庫の良品数を超えます') END;
           END`);

  // ── 発注書メール送信 (P15) ──
  // 仕入先マスタ拡張 (宛先・担当者・送信方法。既存GAS「仕入先ごとの発注メール送信先一覧」の移植先)
  addCol(db, 'po_suppliers', 'email_to', 'TEXT');       // カンマ区切り複数可 (保存時に正規化・検証)
  addCol(db, 'po_suppliers', 'email_cc', 'TEXT');
  addCol(db, 'po_suppliers', 'contact_name', 'TEXT');   // 「様」は送信時に自動補完
  addCol(db, 'po_suppliers', 'send_method', 'TEXT');    // email / fax / web / none (NULL=未設定)
  addCol(db, 'po_suppliers', 'lead_days', 'INTEGER');   // 標準リードタイム (欠品リスクP17で使用)

  // 先方管理番号対応表 (アメージングクラフト/ビーフリー等。添付CSVに先方番号列を付与する)
  db.exec(`CREATE TABLE IF NOT EXISTS po_vendor_code_map (
    supplier_code TEXT NOT NULL,
    product_key   TEXT NOT NULL,
    product_code  TEXT NOT NULL,
    vendor_code   TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (supplier_code, product_key)
  )`);

  // 未紐付けの先方管理番号 (仮登録)。出荷明細→ロジザード入荷予定変換で対応表に無い番号が出たとき
  // ここに置いておき、後日 (NE登録→翌朝PML反映後) 商品と紐づけて対応表へ昇格する (中原さん要望 2026-07-13)
  db.exec(`CREATE TABLE IF NOT EXISTS po_vendor_code_pending (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_code TEXT NOT NULL,
    vendor_code   TEXT NOT NULL,      -- 出荷明細の原文 (表示用)
    vendor_code_norm TEXT NOT NULL,   -- trim+大文字化 (照合キー。変換の逆引きと同じ正規化。大小文字違いの重複防止)
    vendor_name   TEXT,               -- 出荷明細に載っていた先方の商品名 (紐づけ時のヒント)
    last_qty      INTEGER,            -- 直近に出荷明細で見た数量 (参考)
    memo          TEXT,
    status        TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','linked','dismissed')),
    linked_product_code TEXT,         -- linked時の弊社商品コード
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    UNIQUE (supplier_code, vendor_code_norm)
  )`);

  // メール送信ジョブ (outbox方式。txn内でGmail APIを呼ばない。delivery_key で二重送信の確率を構造的に低減:
  // 送信前に sending をcommit → Message-IDヘッダ+本文に埋込 → lease切れ再送前にGmail照合、不明時は自動再送しない)
  db.exec(`CREATE TABLE IF NOT EXISTS po_email_jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id      INTEGER NOT NULL REFERENCES po_orders(id),
    status        TEXT NOT NULL CHECK(status IN ('queued','sending','sent','failed','unknown','cancelled')),
    is_dry_run    INTEGER NOT NULL DEFAULT 0 CHECK(is_dry_run IN (0,1)),
    scheduled_at  TEXT,
    delivery_key  TEXT NOT NULL UNIQUE,
    to_addr       TEXT NOT NULL,
    cc_addr       TEXT,
    subject       TEXT NOT NULL,
    body          TEXT NOT NULL,
    attachment_name TEXT NOT NULL,
    attachment_csv  TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    is_resend     INTEGER NOT NULL DEFAULT 0 CHECK(is_resend IN (0,1)),
    resend_of     INTEGER REFERENCES po_email_jobs(id),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    generation    INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
    sending_started_at TEXT,
    gmail_message_id   TEXT,
    error         TEXT,
    created_at    TEXT NOT NULL,
    sent_at       TEXT,
    actor         TEXT,
    CHECK ((is_resend = 1) = (resend_of IS NOT NULL))
  )`);
  // 既存DB (本ブランチの旧世代スキーマ) への後付け移行 (新規DBはCREATEに含まれる)
  addCol(db, 'po_email_jobs', 'generation', 'INTEGER NOT NULL DEFAULT 0');
  addCol(db, 'po_email_jobs', 'scheduled_at', 'TEXT');
  // 同一PO+同一内容の二重送信を禁止 (再送は is_resend=1 で明示。dry-runは本送信のdedupを妨げない)。
  // unknown (結果不明) も対象 — 不明のまま新規の通常送信で状態機械を迂回させない (Codex P15-R3 High)。
  // 部分インデックスの条件は定義変更があり得るため、トリガ同様に毎起動DROP→CREATEで最新定義を保証 (Codex P15-R11 High)
  // failed も対象 (failedの再試行と新規通常送信の併存で二重送信させない。別内容で送り直す場合は failed を取消してから、Codex P15-R12 High)。
  // 旧定義のDBに重複グループが残っているとCREATE UNIQUEが失敗しアプリが起動不能になるため、
  // 先に検出して対象ジョブを明示した診断エラーで止める (安全側 fail-closed、Codex P15-R13 High)
  {
    const dups = db.prepare(`SELECT order_id, content_hash, GROUP_CONCAT(id || ':' || status) AS jobs
      FROM po_email_jobs
      WHERE is_resend = 0 AND is_dry_run = 0 AND status IN ('queued','sending','sent','unknown','failed')
      GROUP BY order_id, content_hash HAVING COUNT(*) > 1`).all();
    if (dups.length) {
      throw new Error(`po_email_jobs に同一内容の重複ジョブがあります (dedup強化前の残骸)。` +
        `該当を確認し、重複側を cancelled にしてから再起動してください: ` +
        dups.map(d => `order=${d.order_id} jobs=[${d.jobs}]`).join(' / '));
    }
  }
  db.exec('DROP INDEX IF EXISTS uq_po_email_dedup');
  db.exec(`CREATE UNIQUE INDEX uq_po_email_dedup ON po_email_jobs(order_id, content_hash)
           WHERE is_resend = 0 AND is_dry_run = 0 AND status IN ('queued','sending','sent','unknown','failed')`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_email_status ON po_email_jobs(status, created_at)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_email_order ON po_email_jobs(order_id, created_at)');
  // 不正な状態遷移をDB側でも拒否 (sent/cancelled は終端)。
  //  - failed  = Gmail送信要求「前」の失敗 (トークン取得等) → 再試行可 (queued)
  //  - unknown = Gmail送信要求「後」の失敗 = 実際は届いている可能性 → 自動・通常再試行は不可。
  //              照合で1件確認→sent。人間がGmail送信済みを確認して「未送信」を宣言した場合のみ queued へ
  db.exec(`CREATE TRIGGER trg_po_email_transition BEFORE UPDATE OF status ON po_email_jobs
           WHEN NEW.status IS NOT OLD.status
           BEGIN
             SELECT CASE
               WHEN OLD.status = 'sent' OR OLD.status = 'cancelled'
                 THEN RAISE(ABORT, 'email job: 終端状態からの遷移は不可')
               WHEN OLD.status = 'queued' AND NEW.status NOT IN ('sending','cancelled')
                 THEN RAISE(ABORT, 'email job: queued からは sending/cancelled のみ')
               WHEN OLD.status = 'sending' AND NEW.status NOT IN ('sent','failed','unknown')
                 THEN RAISE(ABORT, 'email job: sending からは sent/failed/unknown のみ')
               WHEN OLD.status = 'failed' AND NEW.status NOT IN ('queued','cancelled')
                 THEN RAISE(ABORT, 'email job: failed からは queued(再試行)/cancelled のみ')
               WHEN OLD.status = 'unknown' AND NEW.status NOT IN ('sent','queued')
                 THEN RAISE(ABORT, 'email job: unknown からは sent(照合)/queued(人間確認後) のみ (取消は不可=送信済みの可能性を隠さない)')
               WHEN NEW.status = 'sent' AND NEW.sent_at IS NULL
                 THEN RAISE(ABORT, 'email job: sent には sent_at が必要')
             END;
           END`);

  // ── 設定 (現在値テーブル。変更は前後値を po_audit_log に同一txn記録) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_settings (
    key          TEXT PRIMARY KEY,
    value        TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    changed_by   TEXT,
    reason       TEXT
  )`);
  // 移行境界は初回確定後は不変 (Codex P13a-R1 High-3): 変更すると既存POのtracked/legacy判定が遡って変わるため。
  // 万一の修正はメンテモード (トリガを一時DROP) + 全件整合性検査を伴う専用手順でのみ行う
  db.exec(`CREATE TRIGGER trg_po_settings_boundary_lock_upd BEFORE UPDATE ON po_settings
           WHEN OLD.key = 'tracking_started_at'
           BEGIN SELECT RAISE(ABORT, 'tracking_started_at is immutable (移行境界は変更不可)'); END`);
  db.exec(`CREATE TRIGGER trg_po_settings_boundary_lock_del BEFORE DELETE ON po_settings
           WHEN OLD.key = 'tracking_started_at'
           BEGIN SELECT RAISE(ABORT, 'tracking_started_at is immutable (移行境界は削除不可)'); END`);
  // 初回INSERTも形式検証 (不正な境界値が一度入ると不変トリガで直せず全PO判定が壊れる、Codex P13a-R2 High-4)
  db.exec(`CREATE TRIGGER trg_po_settings_boundary_lock_ins BEFORE INSERT ON po_settings
           WHEN NEW.key = 'tracking_started_at'
             AND (length(NEW.value) <> 24
                  OR NEW.value NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z')
           BEGIN SELECT RAISE(ABORT, 'tracking_started_at: UTC ISO形式 (24文字) が必要です'); END`);

  // ── 監査ログ (専用履歴のない操作に限定: 設定変更・手動クローズ・再オープン・移行操作) ──
  db.exec(`CREATE TABLE IF NOT EXISTS po_audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL CHECK(occurred_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*Z'),
    actor_type  TEXT NOT NULL CHECK(actor_type IN ('user','system','ai_agent','migration')),
    actor       TEXT,
    action      TEXT NOT NULL,
    resource    TEXT NOT NULL,
    detail_json TEXT CHECK(detail_json IS NULL OR json_valid(detail_json)),
    request_id  TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_po_audit ON po_audit_log(resource, occurred_at)');

  // ── 発行の世代ゲート (要件C-1): 境界設定後は必要列の無い/形式不正な issued を物理拒否。
  //    値の形式まで検査する (非NULLだけでは 'x' 等が通る、Codex P13a-R1 High-8)。境界前 (未設定) は既存挙動のまま ──
  // GLOBの'*'は任意文字列のため、桁・長さまで固定する ('PO-2026-abc' 等を弾く、Codex P13a-R2 High-3)。
  // po_number: PO-YYYY- + 4桁以上の数字のみ / issued_at: new Date().toISOString() 形式 (24文字固定)
  const GATE_COND = `EXISTS (SELECT 1 FROM po_settings WHERE key = 'tracking_started_at')
             AND (NEW.tracking_mode IS NOT 'tracked'
                  OR NEW.po_number IS NULL
                  OR NEW.po_number NOT GLOB 'PO-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]*'
                  OR substr(NEW.po_number, 9) GLOB '*[^0-9]*'
                  OR NEW.issued_at IS NULL OR length(NEW.issued_at) <> 24
                  OR NEW.issued_at NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z')`;
  // 境界確定後、issued の直接INSERTは全面拒否 (Codex P13a-R4 Medium-2: 属性を揃えた明細ゼロissuedの直接作成を防ぐ)。
  // 正規経路は draft でヘッダ+明細を作って issued へ UPDATE (issueOrder) のみ
  db.exec(`CREATE TRIGGER trg_po_orders_issue_gate_ins BEFORE INSERT ON po_orders
           WHEN NEW.status = 'issued'
             AND EXISTS (SELECT 1 FROM po_settings WHERE key = 'tracking_started_at')
           BEGIN SELECT RAISE(ABORT, 'issue gate: 発行は draft→issued 経由のみ (直接INSERT禁止)'); END`);
  db.exec(`CREATE TRIGGER trg_po_orders_issue_gate_upd BEFORE UPDATE OF status ON po_orders
           WHEN NEW.status = 'issued' AND OLD.status <> 'issued' AND (${GATE_COND}
                OR NOT EXISTS (SELECT 1 FROM po_order_items WHERE order_id = NEW.id))
           BEGIN SELECT RAISE(ABORT, 'issue gate: po_number/issued_at/tracking_mode が不正か明細がありません'); END`);

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

  // ── 注残の正本 (データ契約) ──
  // 2026-07-13 以降、発注はこのアプリで行い NE には登録しない。そのため NE 由来の
  // mirror_pml_snapshot_rows.注残数 は今後更新されない legacy 値 (ゼロ化ではなく古い値が残る)。
  // 【契約】業務上の注残の正本 = po_* 台帳 (集計は v_ledger_backorder_by_product が唯一のロジック)。
  //         PML 行を読むアプリは v_pml_rows_authoritative を使う。
  //         mirror_pml_snapshot_rows.注残数 の直接参照は禁止 (smoke の静的契約テストが検出する)。

  // 商品別のアプリ台帳注残 (残>0のオープン tracked 発注のみ、移行PO含む)。
  // logic.js loadLedgerBackorders / 要発注判定 / PML出力の全消費者がこのビューを使う (集計ロジックの一本化)
  db.exec('DROP VIEW IF EXISTS v_ledger_backorder_by_product');
  db.exec(`CREATE VIEW v_ledger_backorder_by_product AS
    SELECT i.product_key AS product_key, SUM(b.remaining_qty) AS backorder_qty
    FROM po_order_items i
    JOIN po_orders o ON o.id = i.order_id
    JOIN v_po_item_balance b ON b.order_item_id = i.id
    WHERE o.status = 'issued' AND o.tracking_mode = 'tracked' AND o.closed_at IS NULL AND b.remaining_qty > 0
      AND o.issued_at >= (SELECT value FROM po_settings WHERE key = 'tracking_started_at')
    GROUP BY i.product_key`);

  // published PML + アプリ台帳注残 の正本ビュー。注残数はアプリ台帳値 (PMLに載るNE由来値は使わない)。
  // 由来がわかるよう backorder_source / 参考として ne_backorder_qty (legacy) も持つ
  db.exec('DROP VIEW IF EXISTS v_pml_rows_authoritative');
  db.exec(`CREATE VIEW v_pml_rows_authoritative AS
    SELECT
      r.商品コード, r.商品名, r.仕入先, r.取扱区分, r.商品区分, r.売上分類, r.最終仕入日, r.在庫保管日数,
      r.総在庫数, r.FBA在庫数, r.フリー在庫,
      COALESCE(l.backorder_qty, 0) AS 注残数,
      r.引当数, r.総在庫数_引当なし,
      r.販売数7日_FBA, r.販売数7日_FBA以外, r.販売数7日_合計,
      r.販売数30日_FBA, r.販売数30日_FBA以外, r.販売数30日_合計,
      r.発注ロット単位, r.推奨保有月数, r.売価, r.原価, r.想定見込み利益, r.概算利益率,
      r.代表商品コード, r.ロケーションコード, r.商品分類タグ, r.登録日,
      'app' AS backorder_source,
      r.注残数 AS ne_backorder_qty,
      r.run_id AS run_id
    FROM mirror_pml_snapshot_rows r
    LEFT JOIN v_ledger_backorder_by_product l ON l.product_key = lower(trim(r.商品コード))
    WHERE r.run_id = (SELECT run_id FROM mirror_pml_published WHERE id = 1)`);
}

export function getDB() {
  if (!initialized) initPurchaseOrders();
  return getMirrorDB();
}
