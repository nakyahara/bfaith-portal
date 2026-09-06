/**
 * FBA納品 箱詰め記録 (iPad) — DB 層
 *
 * 正本 = AI_reference『システム設計\FBA納品箱詰め記録_要件定義_20260902.md』(Codex 4巡 approve)
 *
 * 設計の柱 (要件 §9/§10, Codex R1 B4/B5/B7, R2 S1/S2):
 *   - 専用 better-sqlite3 DB (DATA_DIR/fba-box.db)。picking-prep (fba.db, sql.js) へは読み取りのみ
 *   - fbx_placements = 物理投入イベント (取消は revoked_at で論理化・行は消さない)。
 *     箱内連番 box_seq は取消後も再利用しない (監査・積み付け分析の正本)
 *   - 割当は BEGIN IMMEDIATE の1トランザクションで残数検証 → 挿入 → 監査イベント。
 *     冪等性キー UNIQUE(device_key, request_id) で再送二重登録を防ぐ
 *   - fbx_events = 完全 append-only の監査正本
 *   - 端末/登録コード/作業者(職員PIN) は iroha-work と同方式 (テーブル名だけ fbx_)
 */
import crypto from 'crypto';
import path from 'path';
import fs from 'fs';
import Database from 'better-sqlite3';
import { matchExcelSheetsToGroups } from './service.js';

const utcNow = () => new Date().toISOString();
const hashToken = (t) => crypto.createHash('sha256').update(String(t)).digest('hex');
const enrollHash = (c) => crypto.createHash('sha256').update('fbx-enroll:' + String(c)).digest('hex');

export const DEVICE_TTL_MS = 400 * 24 * 3600 * 1000;

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'fba-box.db');

let db = null;

/**
 * 接続 (冪等作成)。Render では DATA_DIR (永続ディスク) が無ければ起動時に止める —
 * 揮発FSに書いてデプロイで消える事故を許さない (要件 §10 / Codex R2 S3)
 */
export function getDB() {
  if (db) return db;
  if (process.env.RENDER && !process.env.DATA_DIR) {
    throw new Error('[fba-box] DATA_DIR が未設定です。永続ディスクなしでは起動できません');
  }
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  db = new Database(DB_FILE);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');
  createTables(db);
  return db;
}

/** テスト用: 一時ファイルDBに差し替える */
export function _openForTest(file) {
  if (db) { try { db.close(); } catch { /* noop */ } }
  db = new Database(file);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');
  createTables(db);
  return db;
}

/** fbx_boxes の DDL (新規作成と void 移行のテーブル再構築で共用) */
const BOXES_DDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      pack_group_id INTEGER NOT NULL REFERENCES fbx_pack_groups(id),
      box_no        INTEGER NOT NULL CHECK (box_no >= 1),
      box_code      TEXT NOT NULL,
      material_code TEXT,
      status        TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed','void')),
      measured_weight_kg REAL CHECK (measured_weight_kg IS NULL OR measured_weight_kg > 0),
      closed_at     TEXT,
      closed_by     TEXT,
      closed_reason TEXT,
      cushion_level TEXT CHECK (cushion_level IS NULL OR cushion_level IN ('none','little','much')),
      reopen_count  INTEGER NOT NULL DEFAULT 0,
      content_version INTEGER NOT NULL DEFAULT 0,
      voided_at     TEXT,
      voided_by     TEXT,
      void_reason   TEXT,
      created_by    TEXT,
      created_at    TEXT NOT NULL,
      UNIQUE(pack_group_id, box_no)
    );`;

/**
 * 梱包グループ = picking-prep のプラン別シート (Excel 添付前) または STA パックリストのシート (添付後)。
 * PR2.5 (Excel 後付け) で excel_file_id / packing_group_id を NULL 許容にし、picking 由来の
 * source_slot_id / source_label を持つ。excel_sheet_name = 書き込み先シート名 (添付後のみ)
 */
const PACK_GROUPS_DDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id           INTEGER NOT NULL REFERENCES fbx_runs(id),
      excel_file_id    INTEGER REFERENCES fbx_excel_files(id),
      sheet_name       TEXT NOT NULL,
      excel_sheet_name TEXT,
      packing_group_id TEXT,
      display_name     TEXT NOT NULL,
      source_slot_id   TEXT,
      source_label     TEXT,
      box_count_hint   INTEGER,
      max_box_columns  INTEGER,
      structure_json   TEXT,
      UNIQUE(run_id, packing_group_id),
      UNIQUE(run_id, source_slot_id)
    );`;

/**
 * 商品行。Excel 添付前は excel_row / seller_sku が NULL で match_state='pending'。
 * origin = 行の由来 (picking: picking-prep のシート行 / excel: Excel の SKU 行)。不変。
 * picking_only = 添付した Excel に無い picking 行 (出力対象外。投入があれば出荷前チェックでブロック)
 * retired = 前の Excel にだけあった行 (origin=excel) が差し替えで消えたが、取消済み投入の履歴があるため残した行 (表示・判定から除外)
 */
const ROWS_DDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id         INTEGER NOT NULL REFERENCES fbx_runs(id),
      pack_group_id  INTEGER NOT NULL REFERENCES fbx_pack_groups(id),
      origin         TEXT NOT NULL DEFAULT 'picking' CHECK (origin IN ('picking','excel')),
      excel_row      INTEGER,
      seller_sku     TEXT,
      asin           TEXT,
      fnsku          TEXT NOT NULL,
      excel_id       TEXT,
      product_name   TEXT,
      planned_qty    INTEGER NOT NULL CHECK (planned_qty >= 0),
      plan_no        TEXT,
      source_slot_id TEXT,
      picking_row_no INTEGER,
      picking_qty    INTEGER,
      match_state    TEXT NOT NULL DEFAULT 'matched'
                     CHECK (match_state IN ('matched','qty_mismatch','excel_only','picking_only','pending','retired')),
      requires_expiry INTEGER CHECK (requires_expiry IN (0,1)),
      UNIQUE(pack_group_id, excel_row)
    );`;

/** 出力対象外の行 (Excel に無い / 差し替えで消えた)。完了判定・出力・iPad 表示から除外 */
const EXCLUDED_ROW_STATES = ['picking_only', 'retired'];

/**
 * テーブル再構築 (CHECK / NOT NULL は ALTER できない)。公式手順 = 新表→コピー→DROP→RENAME を
 * foreign_keys OFF の下で1トランザクションで行い、最後に foreign_key_check。失敗時は元のまま
 */
function rebuildTable(d, name, ddl, insertSql, label) {
  const fkWasOn = d.pragma('foreign_keys', { simple: true }) === 1;
  if (fkWasOn) d.pragma('foreign_keys = OFF');
  try {
    d.transaction(() => {
      d.exec(`DROP TABLE IF EXISTS ${name}_new`);   // 前回の失敗で残った中間表を使い回さない
      d.exec(ddl(`${name}_new`));
      d.exec(insertSql);
      d.exec(`DROP TABLE ${name}`);
      d.exec(`ALTER TABLE ${name}_new RENAME TO ${name}`);
      const bad = d.prepare('PRAGMA foreign_key_check').all();
      if (bad.length > 0) throw new Error(`[fba-box] ${name} 移行後の foreign_key_check に失敗: ${JSON.stringify(bad.slice(0, 3))}`);
    })();
    console.log(`[fba-box] ${name} を${label}へ移行しました`);
  } finally {
    if (fkWasOn) d.pragma('foreign_keys = ON');
  }
}

/** PR1 で作られた fbx_boxes (status CHECK が open/closed のみ) を void 対応に再構築する。冪等 */
function migrateBoxesVoid(d) {
  const sql = d.prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'fbx_boxes'`).get()?.sql || '';
  if (sql.includes("'void'")) return;
  rebuildTable(d, 'fbx_boxes', BOXES_DDL,
    `INSERT INTO fbx_boxes_new (id, pack_group_id, box_no, box_code, material_code, status, measured_weight_kg,
        closed_at, closed_by, closed_reason, cushion_level, reopen_count, created_by, created_at)
      SELECT id, pack_group_id, box_no, box_code, material_code, status, measured_weight_kg,
        closed_at, closed_by, closed_reason, cushion_level, reopen_count, created_by, created_at FROM fbx_boxes`,
    'void 対応スキーマ');
  // 再構築後に PR2.6 以降の列を足し直す (rebuildTable は BOXES_DDL の形で作るので content_version は含まれる)
}

/** PR2.5: Excel 後付け — fbx_pack_groups / fbx_rows の NOT NULL・CHECK を緩める。冪等 */
function migratePickingFirst(d) {
  const groupCols = new Set(d.prepare('PRAGMA table_info(fbx_pack_groups)').all().map((c) => c.name));
  if (!groupCols.has('source_slot_id')) {
    rebuildTable(d, 'fbx_pack_groups', PACK_GROUPS_DDL,
      `INSERT INTO fbx_pack_groups_new (id, run_id, excel_file_id, sheet_name, excel_sheet_name, packing_group_id, display_name,
          source_slot_id, source_label, box_count_hint, max_box_columns, structure_json)
        SELECT id, run_id, excel_file_id, sheet_name, sheet_name, packing_group_id, display_name,
          NULL, NULL, box_count_hint, max_box_columns, structure_json FROM fbx_pack_groups`,
      'Excel 後付け対応 (excel_file_id NULL 許容)');
  }
  const rowsSql = d.prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'fbx_rows'`).get()?.sql || '';
  if (!rowsSql.includes("'retired'")) {
    const rowCols = new Set(d.prepare('PRAGMA table_info(fbx_rows)').all().map((c) => c.name));
    // 既存行 (PR2 = Excel 先行で作られた) の由来は excel。origin 列が既にあればそのまま
    const originExpr = rowCols.has('origin') ? 'origin' : `'excel'`;
    rebuildTable(d, 'fbx_rows', ROWS_DDL,
      `INSERT INTO fbx_rows_new (id, run_id, pack_group_id, origin, excel_row, seller_sku, asin, fnsku, excel_id, product_name, planned_qty,
          plan_no, source_slot_id, picking_row_no, picking_qty, match_state, requires_expiry)
        SELECT id, run_id, pack_group_id, ${originExpr}, excel_row, seller_sku, asin, fnsku, excel_id, product_name, planned_qty,
          plan_no, source_slot_id, picking_row_no, picking_qty, match_state, requires_expiry FROM fbx_rows`,
      'Excel 後付け対応 (excel_row NULL 許容・origin・pending/picking_only/retired)');
    d.exec('CREATE INDEX IF NOT EXISTS idx_fbx_rows_run ON fbx_rows(run_id)');
  }
}

export function createTables(d = getDB()) {
  d.exec(`
    -- 納品回。source_run_id = picking-prep の picking_run_history.id (スナップショット元)
    CREATE TABLE IF NOT EXISTS fbx_runs (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      source_run_id  INTEGER NOT NULL,
      delivery_date  TEXT,
      title          TEXT NOT NULL,
      status         TEXT NOT NULL DEFAULT 'setup'
                     CHECK (status IN ('setup','active','done','cancelled')),
      data_version   INTEGER NOT NULL DEFAULT 0,
      plan_source_hash TEXT,
      match_summary  TEXT,
      created_by     TEXT,
      created_at     TEXT NOT NULL,
      activated_at   TEXT,
      done_at        TEXT
    );

    -- アップロードされた STA パックリストExcel (原本は stored_path に非破壊で隔離保存)
    CREATE TABLE IF NOT EXISTS fbx_excel_files (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id        INTEGER NOT NULL REFERENCES fbx_runs(id),
      original_name TEXT,
      stored_path   TEXT NOT NULL,
      sha256        TEXT NOT NULL,
      fingerprint   TEXT NOT NULL,
      metadata_json TEXT,
      uploaded_by   TEXT,
      uploaded_at   TEXT NOT NULL
    );

    -- 梱包グループ (= picking のプラン別シート / Excel 添付後はパックリストのシート)。箱の親 (Codex R1 B3)
    ${PACK_GROUPS_DDL('fbx_pack_groups')}

    -- 商品行 (Excel 添付後は Excel の SKU 行が正本。picking-prep 側は plan_no と表示名の補強)
    ${ROWS_DDL('fbx_rows')}
    CREATE INDEX IF NOT EXISTS idx_fbx_rows_run ON fbx_rows(run_id);

    -- 行単位の作業メタ (担当者・不足確定)
    CREATE TABLE IF NOT EXISTS fbx_row_work (
      row_id          INTEGER PRIMARY KEY REFERENCES fbx_rows(id),
      label_worker    TEXT,
      check_worker    TEXT,
      shortage_qty    INTEGER CHECK (shortage_qty IS NULL OR shortage_qty > 0),
      shortage_reason TEXT,
      shortage_by     TEXT,
      updated_at      TEXT
    );

    -- 箱資材 (物理情報。tare_g は PR3 重量補助で使用)
    CREATE TABLE IF NOT EXISTS fbx_box_materials (
      code   TEXT PRIMARY KEY,
      name   TEXT NOT NULL,
      tare_g INTEGER,
      sort   INTEGER NOT NULL DEFAULT 0,
      active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1))
    );

    -- 輸送箱。UNIQUE(pack_group_id, box_no)。closed_reason / cushion_level = 結果シグナル (Codex R2 S2)
    -- void = 使わなかった箱の取消 (PR2)。box_no は再利用しない (箱札に書いた番号を動かさない)
    ${BOXES_DDL('fbx_boxes')}

    -- 物理投入 (append-only 正本。取消は revoked_at)。box_seq = 箱内連番 (取消後も再利用しない)
    CREATE TABLE IF NOT EXISTS fbx_placements (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id        INTEGER NOT NULL REFERENCES fbx_runs(id),
      row_id        INTEGER NOT NULL REFERENCES fbx_rows(id),
      box_id        INTEGER NOT NULL REFERENCES fbx_boxes(id),
      qty           INTEGER NOT NULL CHECK (qty > 0),
      expiry        TEXT,
      box_seq       INTEGER NOT NULL,
      placement_layer TEXT CHECK (placement_layer IS NULL OR placement_layer IN ('bottom','middle','top')),
      layer_source  TEXT CHECK (layer_source IS NULL OR layer_source IN ('manual')),
      worker_id     INTEGER,
      worker_name   TEXT,
      device_key    TEXT NOT NULL,
      request_id    TEXT NOT NULL,
      request_hash  TEXT,
      created_at    TEXT NOT NULL,
      revoked_at    TEXT,
      revoked_by    TEXT,
      revoke_reason TEXT,
      UNIQUE(box_id, box_seq),
      UNIQUE(device_key, request_id)
    );
    CREATE INDEX IF NOT EXISTS idx_fbx_placements_row ON fbx_placements(row_id);
    CREATE INDEX IF NOT EXISTS idx_fbx_placements_box ON fbx_placements(box_id);

    -- 監査正本 (完全 append-only)
    CREATE TABLE IF NOT EXISTS fbx_events (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      at           TEXT NOT NULL,
      run_id       INTEGER,
      action       TEXT NOT NULL,
      target_type  TEXT,
      target_id    INTEGER,
      worker_id    INTEGER,
      worker_name  TEXT,
      device_label TEXT,
      payload      TEXT,
      ok           INTEGER NOT NULL CHECK (ok IN (0,1)),
      error        TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_fbx_events_run ON fbx_events(run_id, id);

    -- 作業者 (いろは名簿。iroha-work と同思想で staff.db とは分ける)
    CREATE TABLE IF NOT EXISTS fbx_workers (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      display_name TEXT NOT NULL,
      worker_type  TEXT NOT NULL CHECK (worker_type IN ('member','staff')),
      active       INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      sort_order   INTEGER NOT NULL DEFAULT 0,
      pin_hash     TEXT,
      pin_salt     TEXT,
      pin_fails    INTEGER NOT NULL DEFAULT 0,
      pin_lock_until TEXT,
      created_at   TEXT NOT NULL,
      created_by   TEXT
    );

    CREATE TABLE IF NOT EXISTS fbx_devices (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash   TEXT NOT NULL UNIQUE,
      label        TEXT NOT NULL,
      created_by   TEXT,
      created_at   TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at   TEXT
    );

    CREATE TABLE IF NOT EXISTS fbx_enroll_codes (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      code_hash      TEXT NOT NULL UNIQUE,
      label          TEXT NOT NULL,
      created_by     TEXT,
      created_at     TEXT NOT NULL,
      expires_at     TEXT NOT NULL,
      used_at        TEXT,
      used_device_id INTEGER,
      attempts       INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS fbx_enroll_attempts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip TEXT,
      at TEXT NOT NULL,
      ok INTEGER NOT NULL CHECK (ok IN (0,1))
    );

    CREATE TABLE IF NOT EXISTS fbx_meta (
      key   TEXT PRIMARY KEY,
      value TEXT
    );

    -- 商品画像 (Amazon カタログ MAIN 画像 URL。miniPC の SP-API 経由で取得、FNSKU 単位のキャッシュ — images.js)
    CREATE TABLE IF NOT EXISTS fbx_product_images (
      fnsku      TEXT PRIMARY KEY,
      asin       TEXT,
      image_url  TEXT,
      status     TEXT NOT NULL CHECK (status IN ('ok','none','error')),
      fetched_at TEXT NOT NULL
    );

    -- ── 重量補助 (PR3・要件 §7)。商品キーは FNSKU (fbx_product_images と同じ。§7 の内部 product_id は、
    --    このアプリが箱の中身を FNSKU でしか参照しないため見送り = 実装補足に記載) ──

    -- 参考単重: Amazon カタログの梱包重量 (miniPC の SP-API 経由・画像と同じ 1 回の呼び出しで取る)。
    -- weight_g は 1 個あたり g。応答は kg 小数2桁なので精度は 10g 単位 = 参考値 (実測が入れば実測が勝つ)
    CREATE TABLE IF NOT EXISTS fbx_weight_refs (
      fnsku         TEXT PRIMARY KEY,
      asin          TEXT,
      weight_g      REAL CHECK (weight_g IS NULL OR weight_g > 0),
      raw_value     TEXT,
      source        TEXT NOT NULL DEFAULT 'sp_api_package',
      status        TEXT NOT NULL CHECK (status IN ('ok','none','error')),
      error_message TEXT,
      fetched_at    TEXT NOT NULL
    );

    -- 実測単重: 現場のはかりで「N個で M g」。1個あたりは unit_g (= total_g / sample_qty)。
    -- 取消は revoked_at で論理化 (行は消さない = 逆算分析の生データ)
    CREATE TABLE IF NOT EXISTS fbx_weight_measurements (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      fnsku        TEXT NOT NULL,
      sample_qty   INTEGER NOT NULL CHECK (sample_qty >= 1),
      total_g      REAL NOT NULL CHECK (total_g > 0),
      unit_g       REAL NOT NULL CHECK (unit_g > 0),
      method       TEXT NOT NULL DEFAULT 'scale' CHECK (method IN ('scale','manual')),
      note         TEXT,
      run_id       INTEGER REFERENCES fbx_runs(id),
      worker_id    INTEGER,
      worker_name  TEXT,
      device_label TEXT,
      measured_at  TEXT NOT NULL,
      revoked_at   TEXT,
      revoked_by   TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_fbx_wmeas_fnsku ON fbx_weight_measurements(fnsku);

    -- 採用単重 (projection): 実測 (有効な最新) 優先 → 参考値。source = 採用根拠
    CREATE TABLE IF NOT EXISTS fbx_weight_current (
      fnsku      TEXT PRIMARY KEY,
      unit_g     REAL NOT NULL CHECK (unit_g > 0),
      source     TEXT NOT NULL CHECK (source IN ('measured','catalog')),
      basis_id   INTEGER,
      sample_qty INTEGER,
      updated_at TEXT NOT NULL
    );

    -- 重量ルール (業務ルール = 資材マスタの物理情報とは分ける。要件 §7)。
    -- target_g = 黄警告 (目標28kg) / limit_g = クローズをブロック (絶対上限30kg・職員PIN例外のみ)。
    -- 納品回の開始時に fbx_runs へスナップショットするので、後でルールを変えても過去の回の判定は動かない
    CREATE TABLE IF NOT EXISTS fbx_weight_rules (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      target_g       INTEGER NOT NULL CHECK (target_g > 0),
      limit_g        INTEGER NOT NULL CHECK (limit_g > 0),
      effective_from TEXT NOT NULL,
      updated_by     TEXT,
      updated_at     TEXT NOT NULL
    );

    -- Excel 出力の版 (要件 F-7)。data_version = 出力時点の fbx_runs.data_version。
    -- snapshot_json = 書いたセルと箱↔Amazon箱番号の対応 (出力後の変更は run.data_version が進むので「旧版」と分かる)
    CREATE TABLE IF NOT EXISTS fbx_exports (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id        INTEGER NOT NULL REFERENCES fbx_runs(id),
      excel_file_id INTEGER NOT NULL REFERENCES fbx_excel_files(id),
      data_version  INTEGER NOT NULL,
      file_name     TEXT NOT NULL,
      stored_path   TEXT NOT NULL,
      sha256        TEXT NOT NULL,
      snapshot_json TEXT NOT NULL,
      verify_json   TEXT,
      created_by    TEXT,
      created_at    TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_fbx_exports_run ON fbx_exports(run_id, id);
  `);

  // 列追加のマイグレーション (CREATE IF NOT EXISTS は既存表を変えない —
  // [[feedback_schema_change_needs_migration_and_real_test]])。冪等
  const colsOf = (table) => new Set(d.prepare(`PRAGMA table_info(${table})`).all().map((c) => c.name));
  const addColumn = (table, col, ddl) => { if (!colsOf(table).has(col)) d.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${ddl}`); };
  addColumn('fbx_placements', 'request_hash', 'TEXT');
  // PR2: STAアップ済みの記録 (以後は原則ロック) / 資材の外寸 (Excel の幅・長さ・高さ欄)
  addColumn('fbx_runs', 'sta_uploaded_at', 'TEXT');
  addColumn('fbx_runs', 'sta_export_id', 'INTEGER');
  addColumn('fbx_box_materials', 'width_cm', 'REAL');
  addColumn('fbx_box_materials', 'length_cm', 'REAL');
  addColumn('fbx_box_materials', 'height_cm', 'REAL');
  migrateBoxesVoid(d);
  migratePickingFirst(d);
  // 出力 (版) ごとの STA アップ済み (Codex PR2.5 #1: 複数 Excel はファイル単位で管理)。旧 run.sta_export_id から補完
  addColumn('fbx_exports', 'sta_uploaded_at', 'TEXT');
  addColumn('fbx_exports', 'sta_uploaded_by', 'TEXT');
  // 不足の内訳 (理由別 [{reason, qty}])。作業完了時の自動確定で既存の不足 (例: 破損 2) と混ざるときに理由を失わない (Codex R13 #2)
  addColumn('fbx_row_work', 'shortage_detail', 'TEXT');
  // PR2.6-R1: 「確認した人」の由来。auto = 投入から自動で入った / manual = 人が選んだ (自動では動かさない)。
  // 移行前からある値は source NULL = manual 扱い (勝手に消さない)
  addColumn('fbx_row_work', 'check_worker_source', 'TEXT');
  addColumn('fbx_row_work', 'check_worker_placement_id', 'INTEGER');
  addColumn('fbx_product_images', 'error_message', 'TEXT');   // 画像が出ない理由 (管理画面の診断)
  // 箱の中身が変わるたびに +1。読み合わせ画面が持っている版と違えばクローズを拒否する (Codex R18 #1)
  addColumn('fbx_boxes', 'content_version', 'INTEGER NOT NULL DEFAULT 0');
  // PR3 重量補助: 納品回ごとのルールのスナップショット (開始後にルールを変えても判定は動かさない)
  addColumn('fbx_runs', 'weight_target_g', 'INTEGER');
  addColumn('fbx_runs', 'weight_limit_g', 'INTEGER');
  // PR3: クローズ時点の推定の生データ (実測との乖離ヒント + 後日の逆算分析。要件 §7 末尾)
  addColumn('fbx_boxes', 'est_weight_g_at_close', 'REAL');
  addColumn('fbx_boxes', 'est_unknown_qty_at_close', 'INTEGER');
  addColumn('fbx_boxes', 'tare_g_at_close', 'INTEGER');
  // 上限超えを承認して閉じた箱 (例外運用なので箱そのものに残す — 出荷前チェックで探せるように)
  addColumn('fbx_boxes', 'limit_override_by', 'TEXT');
  addColumn('fbx_boxes', 'limit_override_at', 'TEXT');
  // 2026-09-03: miniPC の応答キーを取り違えて (mainImage ← 実際は image) 全商品が「画像なし」になったキャッシュを捨てる。
  // 現行コードは none/error に必ず理由を書くので、理由が空の none = 旧バグの結果だけが消える (以後は 0 件)
  d.exec(`DELETE FROM fbx_product_images WHERE status = 'none' AND error_message IS NULL`);
  // 同日: 応答の包み方 (result の入れ子) を取り違えて全商品が「Amazon に画像がありません」になった分を 1 回だけ捨てる。
  // 「本当に画像が無い商品」も巻き込むが、次のアクセスで取り直して同じ結論になるだけ (fbx_meta で 1 回限り)
  if (!d.prepare(`SELECT value FROM fbx_meta WHERE key = 'image_cache_reset_20260903'`).get()) {
    const n = d.prepare(`DELETE FROM fbx_product_images WHERE status = 'none' AND error_message = 'Amazon に画像がありません'`).run().changes;
    d.prepare(`INSERT INTO fbx_meta (key, value) VALUES ('image_cache_reset_20260903', ?)`).run(String(n));
    if (n > 0) console.log(`[fba-box] 画像キャッシュを ${n} 件リセットしました (応答形の修正で取り直します)`);
  }
  d.exec(`UPDATE fbx_exports SET sta_uploaded_at = (SELECT r.sta_uploaded_at FROM fbx_runs r WHERE r.sta_export_id = fbx_exports.id)
    WHERE sta_uploaded_at IS NULL AND EXISTS (SELECT 1 FROM fbx_runs r WHERE r.sta_export_id = fbx_exports.id AND r.sta_uploaded_at IS NOT NULL)`);

  // 資材の初期値 (管理画面で編集可能にするのは後続PR。tare_g は現行の実測目安)
  const seeded = d.prepare('SELECT COUNT(*) c FROM fbx_box_materials').get().c;
  if (seeded === 0) {
    const ins = d.prepare('INSERT INTO fbx_box_materials (code, name, tare_g, sort) VALUES (?, ?, ?, ?)');
    ins.run('box140', '140サイズ段ボール', 900, 1);
    ins.run('box160', '160サイズ段ボール', 1200, 2);
    ins.run('other', 'その他', null, 9);
  }

  // 重量ルールの初期値 (要件 §7: 目標28kg = 黄警告 / 絶対上限30kg = クローズをブロック)
  if (d.prepare('SELECT COUNT(*) c FROM fbx_weight_rules').get().c === 0) {
    d.prepare(`INSERT INTO fbx_weight_rules (target_g, limit_g, effective_from, updated_by, updated_at)
      VALUES (?, ?, ?, ?, ?)`).run(28000, 30000, utcNow(), 'system', utcNow());
  }
  // PR3 のデプロイ時点で既に始まっている (active) / 終わった (done) 回にもルールを焼き付ける (Codex PR3 #2)。
  // 入れておかないと、作業中の回の判定が後のルール変更で動いてしまう。setup の回は有効化時に入る。冪等 (NULL のみ)
  const curRule = d.prepare('SELECT target_g, limit_g FROM fbx_weight_rules ORDER BY effective_from DESC, id DESC LIMIT 1').get();
  if (curRule) {
    d.prepare(`UPDATE fbx_runs SET weight_target_g = COALESCE(weight_target_g, ?), weight_limit_g = COALESCE(weight_limit_g, ?)
      WHERE status IN ('active','done') AND (weight_target_g IS NULL OR weight_limit_g IS NULL)`)
      .run(curRule.target_g, curRule.limit_g);
  }
  // 採用単重 (projection) を起動時に作り直して、元データとの食い違いを自己修復する (Codex PR3 #5)
  rebuildWeightCurrent(d);
}

// ───────────────────────── 監査イベント ─────────────────────────

export function logEvent({ runId = null, action, targetType = null, targetId = null, workerId = null, workerName = null, deviceLabel = null, payload = null, ok, error = null }, d = getDB()) {
  d.prepare(`INSERT INTO fbx_events (at, run_id, action, target_type, target_id, worker_id, worker_name, device_label, payload, ok, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(utcNow(), runId, action, targetType, targetId, workerId, workerName, deviceLabel,
      payload == null ? null : JSON.stringify(payload), ok ? 1 : 0,
      error == null ? null : String(error).slice(0, 300));
}

export function safeLogEvent(entry, d) {
  try { logEvent(entry, d); } catch (e) { console.error('[fba-box] 監査イベントの記録に失敗 (結果はそのまま返す)', e); }
}

/**
 * Excel の中身に影響する変更 (割当・箱・不足) のたびに data_version を進める。
 * 出力済み Excel の data_version と比べて「旧版」を判定する (要件 F-7)。呼び出し側のトランザクション内で使う
 */
function bumpRunVersion(d, runId) {
  d.prepare('UPDATE fbx_runs SET data_version = data_version + 1 WHERE id = ?').run(Number(runId));
}

/**
 * 納品回に重量ルール (目標・上限) を焼き付ける (要件 §7「run開始時に適用値をスナップショット」)。
 * 既に入っている回は触らない = 作業中にルールを変えても判定は動かない
 */
function snapshotWeightRules(d, runId) {
  const r = getWeightRules(d);
  d.prepare(`UPDATE fbx_runs SET weight_target_g = COALESCE(weight_target_g, ?), weight_limit_g = COALESCE(weight_limit_g, ?) WHERE id = ?`)
    .run(r.target_g, r.limit_g, Number(runId));
}

const safeJson = (s, def = null) => { try { return s == null ? def : JSON.parse(s); } catch { return def; } };

export function listEvents(limit = 100, runId = null) {
  const d = getDB();
  if (runId) return d.prepare('SELECT * FROM fbx_events WHERE run_id = ? ORDER BY id DESC LIMIT ?').all(runId, Number(limit) || 100);
  return d.prepare('SELECT * FROM fbx_events ORDER BY id DESC LIMIT ?').all(Number(limit) || 100);
}

// ───────────────────────── 納品回 (run) ─────────────────────────

/**
 * 納品回の作成 (Excel突合済みのデータ一式を1トランザクションで登録)。
 * groups = [{ sheetName, packingGroupId, displayName, boxCountHint, maxBoxColumns, structure, rows: [...] }]
 * 各 row = { excelRow, sellerSku, asin, fnsku, excelId, productName, plannedQty,
 *            planNo, sourceSlotId, pickingRowNo, pickingQty, matchState }
 */
export function createRun({ sourceRunId, deliveryDate, title, planSourceHash, matchSummary, excelFile, groups, createdBy }) {
  const d = getDB();
  const now = utcNow();
  return d.transaction(() => {
    const dup = d.prepare(`SELECT id FROM fbx_runs WHERE source_run_id = ? AND status IN ('setup','active')`).get(sourceRunId);
    if (dup) return { ok: false, error: 'duplicate_run', message: `このピッキング実行 (ID ${sourceRunId}) の納品回は既にあります (回 #${dup.id})。続きはそちらで作業してください`, runId: dup.id };
    const run = d.prepare(`INSERT INTO fbx_runs (source_run_id, delivery_date, title, status, plan_source_hash, match_summary, created_by, created_at)
      VALUES (?, ?, ?, 'setup', ?, ?, ?, ?)`)
      .run(sourceRunId, deliveryDate || null, title, planSourceHash || null,
        matchSummary == null ? null : JSON.stringify(matchSummary), createdBy || null, now);
    const runId = Number(run.lastInsertRowid);
    const file = d.prepare(`INSERT INTO fbx_excel_files (run_id, original_name, stored_path, sha256, fingerprint, metadata_json, uploaded_by, uploaded_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(runId, excelFile.originalName || null, excelFile.storedPath, excelFile.sha256, excelFile.fingerprint,
        excelFile.metadata == null ? null : JSON.stringify(excelFile.metadata), createdBy || null, now);
    const fileId = Number(file.lastInsertRowid);
    const insGroup = d.prepare(`INSERT INTO fbx_pack_groups (run_id, excel_file_id, sheet_name, excel_sheet_name, packing_group_id, display_name, box_count_hint, max_box_columns, structure_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insRow = d.prepare(`INSERT INTO fbx_rows (run_id, pack_group_id, origin, excel_row, seller_sku, asin, fnsku, excel_id, product_name, planned_qty, plan_no, source_slot_id, picking_row_no, picking_qty, match_state)
      VALUES (?, ?, 'excel', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const g of groups) {
      const gi = insGroup.run(runId, fileId, g.sheetName, g.sheetName, g.packingGroupId, g.displayName,
        g.boxCountHint ?? null, g.maxBoxColumns ?? null, g.structure == null ? null : JSON.stringify(g.structure));
      const groupId = Number(gi.lastInsertRowid);
      for (const r of g.rows) {
        insRow.run(runId, groupId, r.excelRow, r.sellerSku, r.asin || null, r.fnsku, r.excelId || null,
          r.productName || null, r.plannedQty, r.planNo || null, r.sourceSlotId || null,
          r.pickingRowNo ?? null, r.pickingQty ?? null, r.matchState || 'matched');
      }
    }
    logEvent({ runId, action: 'run_create', targetType: 'run', targetId: runId, deviceLabel: createdBy, ok: true, payload: { sourceRunId } }, d);
    return { ok: true, runId };
  }).immediate();
}

/**
 * PR2.5: picking-prep の実行から納品回を作る (Excel なし = 現場起点)。
 * グループ = プラン別シート (通常/危険/大型…)、行 = シート行 (FNSKU・数量・商品名・納品プランNo)。
 * Excel は後から attachExcelToRun で添付する (出力に必要なのは Excel だけ、作業には不要)。
 * 同じ picking 実行の納品回が既にあれば作らず already で返す (自動作成と iPad からの作成が重ならない)
 */
export function createRunFromPicking({ pickingRun, planSheets, createdBy, activate = true }) {
  const sourceRunId = Number(pickingRun?.id);
  if (!Number.isInteger(sourceRunId) || sourceRunId <= 0) return { ok: false, error: 'bad_request', message: 'ピッキング実行IDが不正です' };
  const sheets = (planSheets || []).filter((s) => s && Array.isArray(s.rows) && s.rows.length > 0);
  if (sheets.length === 0) return { ok: false, error: 'no_rows', message: 'プラン別シートに商品行がありません' };
  const d = getDB();
  const now = utcNow();
  return d.transaction(() => {
    const dup = d.prepare(`SELECT id, status FROM fbx_runs WHERE source_run_id = ? AND status IN ('setup','active','done')`).get(sourceRunId);
    if (dup) return { ok: true, already: true, runId: dup.id, status: dup.status };
    const title = pickingRun.delivery_date
      ? `${pickingRun.delivery_date} 納品分`
      : `実行#${sourceRunId} (${String(pickingRun.run_at || '').slice(0, 16)})`;
    const run = d.prepare(`INSERT INTO fbx_runs (source_run_id, delivery_date, title, status, created_by, created_at, activated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)`)
      .run(sourceRunId, pickingRun.delivery_date || null, title, activate ? 'active' : 'setup', createdBy || null, now, activate ? now : null);
    const runId = Number(run.lastInsertRowid);
    if (activate) snapshotWeightRules(d, runId);
    const insGroup = d.prepare(`INSERT INTO fbx_pack_groups (run_id, sheet_name, display_name, source_slot_id, source_label) VALUES (?, ?, ?, ?, ?)`);
    const insRow = d.prepare(`INSERT INTO fbx_rows (run_id, pack_group_id, origin, seller_sku, fnsku, product_name, planned_qty, plan_no, source_slot_id, picking_row_no, picking_qty, match_state)
      VALUES (?, ?, 'picking', ?, ?, ?, ?, ?, ?, ?, ?, 'pending')`);
    let rowCount = 0;
    for (const [i, s] of sheets.entries()) {
      const label = String(s.label || s.sheet || `P${i + 1}`).trim();
      const slotId = String(s.slotId || s.sheet || `slot${i + 1}`);
      const gi = insGroup.run(runId, String(s.sheet || label), label, slotId, label);
      const groupId = Number(gi.lastInsertRowid);
      for (const r of s.rows) {
        const qty = Number.parseInt(r.qty, 10);
        insRow.run(runId, groupId, r.sku ? String(r.sku).trim() : null, String(r.fnsku || '').trim().toUpperCase(),
          r.productName || null, Number.isFinite(qty) && qty > 0 ? qty : 0, `${label}_${r.no}`, slotId, Number(r.no) || null,
          Number.isFinite(qty) ? qty : null);
        rowCount++;
      }
    }
    logEvent({ runId, action: 'run_create', targetType: 'run', targetId: runId, deviceLabel: createdBy, ok: true,
      payload: { sourceRunId, from: 'picking', sheets: sheets.length, rows: rowCount } }, d);
    if (activate) logEvent({ runId, action: 'run_activate', targetType: 'run', targetId: runId, deviceLabel: createdBy, ok: true, payload: { auto: true } }, d);
    return { ok: true, created: true, runId, status: activate ? 'active' : 'setup', groups: sheets.length, rows: rowCount };
  }).immediate();
}

/** picking 実行ID → 納品回 (setup/active/done のうち最新)。iPad の一覧で「作業開始/続き」を出し分ける */
export function getRunBySource(sourceRunId) {
  return getDB().prepare(`SELECT id, status, title FROM fbx_runs WHERE source_run_id = ? AND status IN ('setup','active','done') ORDER BY id DESC LIMIT 1`)
    .get(Number(sourceRunId)) || null;
}

/**
 * PR2.5: 納品回に STA パックリスト Excel を添付する (作業の前でも途中でも後でもよい)。
 * シート↔グループは FNSKU の重なりで自動対応 (service.matchExcelSheetsToGroups)、
 * 行は FNSKU で突合 (matched / qty_mismatch / excel_only を Excel 側から、picking_only を既存行側から)。
 * Excel が正本: planned_qty は Excel の値に更新する (数量差は警告として残す)。
 * 再添付 (差し替え) は STA アップ済みでなければ可。1 ファイル = 1 プラン (P1/P2 で別ファイル) なので
 * 納品回に複数ファイルを添付できる (グループごとに excel_file_id を持つ)
 */
export function attachExcelToRun({ runId, parsed, file, actor }) {
  const d = getDB();
  const now = utcNow();
  return d.transaction(() => {
    const run = d.prepare('SELECT * FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    if (run.status === 'cancelled') return { ok: false, error: 'bad_status', message: 'この納品回は取消済みです' };
    if (run.sta_uploaded_at) return { ok: false, error: 'sta_uploaded', message: 'STA アップ済みの納品回には Excel を差し替えできません' };
    const groups = d.prepare('SELECT * FROM fbx_pack_groups WHERE run_id = ? ORDER BY id').all(run.id);
    // 行ごとの投入数・確定不足 (Codex PR2.5 #3: Excel の予定数が投入数を下回る添付は拒否)
    const rowsByGroup = new Map(groups.map((g) => [g.id, d.prepare(`SELECT w.*,
        COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0) AS placed,
        (SELECT COUNT(*) FROM fbx_placements p WHERE p.row_id = w.id) AS placement_rows,
        COALESCE((SELECT rw.shortage_qty FROM fbx_row_work rw WHERE rw.row_id = w.id), 0) AS shortage
      FROM fbx_rows w WHERE w.pack_group_id = ? AND w.match_state != 'retired' ORDER BY w.id`).all(g.id)]));
    const match = matchExcelSheetsToGroups(parsed.sheets, groups.map((g) => ({ id: g.id, name: g.sheet_name, fnskus: rowsByGroup.get(g.id).map((r) => r.fnsku) })));
    if (!match.ok) return { ok: false, error: 'unmatched_sheet', message: match.message, issues: match.issues };
    // 対応先グループの現在の Excel が (現行データ版で) STA アップ済みなら差し替え不可 (Codex PR2.5 #1)
    const uploadedFile = d.prepare(`SELECT id FROM fbx_exports WHERE run_id = ? AND excel_file_id = ? AND sta_uploaded_at IS NOT NULL AND data_version = ? LIMIT 1`);
    for (const a of match.assignments) {
      const g = groups.find((x) => x.id === a.groupId);
      if (g.excel_file_id && uploadedFile.get(run.id, g.excel_file_id, run.data_version)) {
        return { ok: false, error: 'file_uploaded', message: `${g.sheet_name} の Excel は既に STA アップ済みとして記録されています。差し替えは中原さんに連絡してください` };
      }
    }

    // 事前検証 (拒否条件を全て集めてから書く): 予定数 < 投入+不足 / 消える excel_only 行に投入あり / FNSKU 重複
    const conflicts = [];
    const plans = [];
    for (const a of match.assignments) {
      const sheet = parsed.sheets[a.sheetIndex];
      const g = groups.find((x) => x.id === a.groupId);
      const existing = new Map(rowsByGroup.get(g.id).map((r) => [normFnsku(r.fnsku), r]));
      const seen = new Set();
      for (const er of sheet.skuRows) {
        const key = normFnsku(er.fnsku);
        if (!key) continue;
        if (seen.has(key)) return { ok: false, error: 'duplicate_identity', message: `Excel 内で FNSKU ${er.fnsku} が重複しています (手動転記に切り替えてください)` };
        seen.add(key);
        // 投入済み > Excel の予定 は拒否 (不足は後で予定に合わせて縮める/伸ばすので条件に入れない — Codex R14 #1)
        const row = existing.get(key);
        if (row && row.placed > er.plannedQty) {
          conflicts.push({ kind: 'over_placed', group: g.sheet_name, fnsku: er.fnsku, excelQty: er.plannedQty, placed: row.placed, shortage: row.shortage });
        }
      }
      for (const [key, row] of existing) {
        if (!seen.has(key) && row.origin === 'excel' && row.placed > 0) {
          conflicts.push({ kind: 'excel_only_placed', group: g.sheet_name, fnsku: row.fnsku, placed: row.placed });
        }
      }
      plans.push({ a, sheet, g, existing, seen });
    }
    if (conflicts.length > 0) {
      return { ok: false, error: 'attach_conflict', conflicts,
        message: '添付できません: ' + conflicts.map((c) => c.kind === 'over_placed'
          ? `${c.fnsku} は Excel の予定 ${c.excelQty} より投入 ${c.placed}${c.shortage ? '+不足' + c.shortage : ''} が多い`
          : `${c.fnsku} は前の Excel にだけあり投入 ${c.placed} が残っている`).join(' / ') + ' — 記録の取消・不足の解除をしてから添付するか、STA のプランを確認してください' };
    }

    const fileInfo = d.prepare(`INSERT INTO fbx_excel_files (run_id, original_name, stored_path, sha256, fingerprint, metadata_json, uploaded_by, uploaded_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(run.id, file.originalName || null, file.storedPath, file.sha256, parsed.fingerprint, JSON.stringify(parsed.metadata || {}), actor || null, now);
    const fileId = Number(fileInfo.lastInsertRowid);
    const updGroup = d.prepare(`UPDATE fbx_pack_groups SET excel_file_id = ?, excel_sheet_name = ?, packing_group_id = ?, box_count_hint = ?, max_box_columns = ?, structure_json = ? WHERE id = ?`);
    const clearRows = d.prepare('UPDATE fbx_rows SET excel_row = NULL WHERE pack_group_id = ?');
    const updRow = d.prepare(`UPDATE fbx_rows SET excel_row = ?, seller_sku = COALESCE(?, seller_sku), asin = ?, excel_id = ?, product_name = COALESCE(product_name, ?),
      planned_qty = ?, match_state = ? WHERE id = ?`);
    const setState = d.prepare('UPDATE fbx_rows SET match_state = ? WHERE id = ?');
    const delRow = d.prepare('DELETE FROM fbx_rows WHERE id = ?');
    const delRowWork = d.prepare('DELETE FROM fbx_row_work WHERE row_id = ?');
    const insRow = d.prepare(`INSERT INTO fbx_rows (run_id, pack_group_id, origin, excel_row, seller_sku, asin, fnsku, excel_id, product_name, planned_qty, match_state)
      VALUES (?, ?, 'excel', ?, ?, ?, ?, ?, ?, ?, 'excel_only')`);
    const liveBoxCount = d.prepare(`SELECT COUNT(*) c FROM fbx_boxes WHERE pack_group_id = ? AND status != 'void'`);
    const summary = [];
    const warnings = [];
    for (const { a, sheet, g, existing, seen } of plans) {
      const structure = {
        headerRow: sheet.headerRow, headers: sheet.headers, boxColumns: sheet.boxColumns, boxNames: sheet.boxNames || {},
        totalBoxes: sheet.totalBoxes, boxNameRow: sheet.boxNameRow, dimRows: sheet.dimRows,
      };
      updGroup.run(fileId, sheet.sheetName, sheet.packingGroupId, sheet.totalBoxes?.value ?? null, sheet.maxBoxColumns ?? null, JSON.stringify(structure), g.id);
      clearRows.run(g.id);
      const counts = { matched: 0, qty_mismatch: 0, excel_only: 0, picking_only: 0, retired: 0 };
      for (const er of sheet.skuRows) {
        const key = normFnsku(er.fnsku);
        if (!key) continue;
        const row = existing.get(key);
        if (row && row.origin === 'picking') {
          const pickQty = row.picking_qty ?? row.planned_qty;
          const state = pickQty === er.plannedQty ? 'matched' : 'qty_mismatch';
          updRow.run(er.row, er.sku || null, er.asin || null, er.excelId || null, er.productName || null, er.plannedQty, state, row.id);
          counts[state]++;
          if (state === 'qty_mismatch') warnings.push({ kind: 'qty_mismatch', group: g.sheet_name, fnsku: er.fnsku, excelQty: er.plannedQty, pickingQty: pickQty });
        } else if (row) {
          // 前の Excel 由来の行が今回も載っている → excel_only のまま更新 (由来は変えない — Codex PR2.5 #4)
          updRow.run(er.row, er.sku || null, er.asin || null, er.excelId || null, er.productName || null, er.plannedQty, 'excel_only', row.id);
          counts.excel_only++;
          warnings.push({ kind: 'excel_only', group: g.sheet_name, fnsku: er.fnsku, excelQty: er.plannedQty });
        } else {
          insRow.run(run.id, g.id, er.row, er.sku || null, er.asin || null, String(er.fnsku).trim().toUpperCase(), er.excelId || null, er.productName || null, er.plannedQty);
          counts.excel_only++;
          warnings.push({ kind: 'excel_only', group: g.sheet_name, fnsku: er.fnsku, excelQty: er.plannedQty });
        }
      }
      for (const [key, row] of existing) {
        if (seen.has(key)) continue;
        if (row.origin === 'picking') {
          setState.run('picking_only', row.id);
          counts.picking_only++;
          warnings.push({ kind: 'picking_only', group: g.sheet_name, fnsku: row.fnsku, pickingQty: row.planned_qty });
        } else if (row.placement_rows === 0) {
          // 前の Excel にだけあった行で記録が一切ない → 再生成のため消す (履歴は fbx_events に残る)
          delRowWork.run(row.id);
          delRow.run(row.id);
          counts.retired++;
        } else {
          setState.run('retired', row.id);   // 取消済み投入の履歴がある (FK) → 残して除外
          counts.retired++;
        }
      }
      const boxes = liveBoxCount.get(g.id).c;
      if (sheet.maxBoxColumns && boxes > sheet.maxBoxColumns) {
        warnings.push({ kind: 'box_overflow', group: g.sheet_name, boxes, maxBoxColumns: sheet.maxBoxColumns });
      }
      summary.push({ groupId: g.id, sheetName: g.sheet_name, excelSheet: sheet.sheetName, packingGroupId: sheet.packingGroupId, overlap: a.overlap, ...counts });
    }
    // 添付で予定数が変わった行の不足を予定に合わせる (Codex R13 #1 / R14 #1,#2):
    //   作業中 (active): 不足が「予定 − 投入」を超えていれば縮める (伸ばさない — 残りは現場が入れる)
    //   完了済み (done): 投入+不足 = 予定 に揃える (増えた分は not_shipped、減った分は末尾から削る)。iPad から直せないため
    // 内訳 (理由別) は shortageBreakdownFor で保つ
    let recomputed = 0;
    {
      const affected = d.prepare(`SELECT w.id, w.planned_qty, w.fnsku,
          COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0) AS placed,
          COALESCE(rw.shortage_qty, 0) AS shortage, rw.shortage_reason, rw.shortage_detail
        FROM fbx_rows w LEFT JOIN fbx_row_work rw ON rw.row_id = w.id
        WHERE w.run_id = ? AND w.excel_row IS NOT NULL AND w.match_state NOT IN ('picking_only','retired')`).all(run.id);
      const up = d.prepare(`INSERT INTO fbx_row_work (row_id, shortage_qty, shortage_reason, shortage_detail, shortage_by, updated_at) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(row_id) DO UPDATE SET shortage_qty = excluded.shortage_qty, shortage_reason = excluded.shortage_reason, shortage_detail = excluded.shortage_detail,
          shortage_by = excluded.shortage_by, updated_at = excluded.updated_at`);
      for (const r of affected) {
        const gap = r.planned_qty - r.placed;   // placed ≤ planned は over_placed で保証済み
        const need = run.status === 'done' ? gap : Math.min(r.shortage, gap);
        if (need === r.shortage) continue;
        const bd = shortageBreakdownFor({ shortage: r.shortage, reason: r.shortage_reason, detail: r.shortage_detail }, need);
        if (need <= 0) {
          d.prepare('UPDATE fbx_row_work SET shortage_qty = NULL, shortage_reason = NULL, shortage_detail = NULL, updated_at = ? WHERE row_id = ?').run(now, r.id);
        } else {
          up.run(r.id, need, bd.reason || 'not_shipped', bd.detail, actor || null, now);
        }
        logEvent({ runId: run.id, action: 'row_shortage', targetType: 'row', targetId: r.id, deviceLabel: actor, ok: true,
          payload: { shortageQty: Math.max(0, need), reason: bd.reason, auto: true, via: 'excel_attach', runStatus: run.status, from: r.shortage, planned: r.planned_qty, placed: r.placed, detail: safeJson(bd.detail, null) } }, d);
        recomputed++;
        warnings.push({ kind: 'shortage_recomputed', fnsku: r.fnsku, planned: r.planned_qty, placed: r.placed, shortageFrom: r.shortage, shortageTo: Math.max(0, need) });
      }
    }
    bumpRunVersion(d, run.id);
    logEvent({ runId: run.id, action: 'excel_attach', targetType: 'excel', targetId: fileId, deviceLabel: actor, ok: true,
      payload: { fingerprint: parsed.fingerprint, sheets: summary, warnings: warnings.length, recomputed } }, d);
    // まだ Excel が無いグループ (今回の対象外で、かつ以前の添付も無い) — Codex PR2.5 #7
    const stillNoExcel = d.prepare('SELECT id FROM fbx_pack_groups WHERE run_id = ? AND excel_file_id IS NULL').all(run.id).map((x) => x.id);
    return { ok: true, excelFileId: fileId, groups: summary, warnings, unassignedGroups: stillNoExcel };
  }).immediate();
}

const normFnsku = (s) => String(s ?? '').trim().toUpperCase();

/** setup → active (本社が突合結果を確認して有効化) */
export function activateRun(runId, actor) {
  const d = getDB();
  return d.transaction(() => {
    const run = d.prepare('SELECT * FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    if (run.status === 'active') return { ok: true, already: true };
    if (run.status !== 'setup') return { ok: false, error: 'bad_status', message: `この納品回は ${run.status} のため有効化できません` };
    d.prepare(`UPDATE fbx_runs SET status = 'active', activated_at = ?, data_version = data_version + 1 WHERE id = ?`)
      .run(utcNow(), run.id);
    snapshotWeightRules(d, run.id);
    logEvent({ runId: run.id, action: 'run_activate', targetType: 'run', targetId: run.id, deviceLabel: actor, ok: true }, d);
    return { ok: true };
  }).immediate();
}

/** active → done / cancelled (本社)。done は全箱クローズが条件 */
export function setRunStatus(runId, status, actor) {
  if (status !== 'done' && status !== 'cancelled') return { ok: false, error: 'bad_request', message: '不正なステータスです' };
  const d = getDB();
  return d.transaction(() => {
    const run = d.prepare('SELECT * FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    if (run.status === status) return { ok: true, already: true };
    if (run.status === 'done' || run.status === 'cancelled') {
      return { ok: false, error: 'bad_status', message: `この納品回は既に ${run.status} です` };
    }
    if (status === 'done') {
      const openBoxes = d.prepare(`SELECT COUNT(*) c FROM fbx_boxes b JOIN fbx_pack_groups g ON g.id = b.pack_group_id
        WHERE g.run_id = ? AND b.status = 'open'`).get(run.id).c;
      if (openBoxes > 0) return { ok: false, error: 'open_boxes', message: `開いたままの箱が ${openBoxes} 箱あります。先に全ての箱を閉じてください` };
      // 全行が「投入 + 確定不足 = 予定」でなければ完了できない (Codex PR1 #7)。
      // picking_only (添付した Excel に無い行 = プランから外れた商品) は対象外
      const bad = d.prepare(`SELECT COUNT(*) c FROM fbx_rows w
        LEFT JOIN fbx_row_work rw ON rw.row_id = w.id
        WHERE w.run_id = ? AND w.match_state NOT IN ('picking_only','retired')
          AND COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0)
              + COALESCE(rw.shortage_qty, 0) != w.planned_qty`).get(run.id).c;
      if (bad > 0) return { ok: false, error: 'rows_incomplete', message: `投入数と不足の合計が予定数と合わない商品が ${bad} 行あります。iPad で入力を終えるか、不足を確定してから完了にしてください` };
    }
    // 状態変更は Excel の中身を変えないので data_version は進めない (出力済みの版を「旧版」にしない)
    d.prepare('UPDATE fbx_runs SET status = ?, done_at = ? WHERE id = ?')
      .run(status, status === 'done' ? utcNow() : null, run.id);
    logEvent({ runId: run.id, action: `run_${status}`, targetType: 'run', targetId: run.id, deviceLabel: actor, ok: true }, d);
    return { ok: true };
  }).immediate();
}

/**
 * 作業を終える (職員 / 本社)。全部入らなくても完了できる (中原さん 9/3: 破損・自社出荷の引当で FBA に出さない商品がある)。
 *   - 中身のある開いた箱は先に閉じてもらう (実測重量が要る) → open_boxes
 *   - 空の開いた箱は取消して進める
 *   - 未投入が残る行があれば acknowledge=false では incomplete で一覧を返す (最後のアラート)。
 *     acknowledge=true なら残数を「今回は納品しない (not_shipped)」の不足として確定してから done にする
 */
export function finishRun({ runId, acknowledge = false, worker, deviceLabel }) {
  const d = getDB();
  return d.transaction(() => {
    const run = d.prepare('SELECT * FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    if (run.status === 'done') return { ok: true, already: true };
    if (run.status !== 'active') return { ok: false, error: 'bad_status', message: `この納品回は ${run.status} です` };
    const openBoxes = d.prepare(`SELECT b.*, COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.box_id = b.id AND p.revoked_at IS NULL), 0) AS total_qty
      FROM fbx_boxes b JOIN fbx_pack_groups g ON g.id = b.pack_group_id WHERE g.run_id = ? AND b.status = 'open' ORDER BY b.id`).all(run.id);
    const withContents = openBoxes.filter((b) => b.total_qty > 0);
    if (withContents.length > 0) {
      return { ok: false, error: 'open_boxes', boxes: withContents.map((b) => ({ id: b.id, code: b.box_code, qty: b.total_qty })),
        message: `閉じていない箱が ${withContents.length} 箱あります (${withContents.map((b) => b.box_code).join(', ')})。読み合わせて重さを入れて閉じてから完了してください` };
    }
    const rows = d.prepare(`SELECT w.*,
        COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0) AS placed,
        COALESCE(rw.shortage_qty, 0) AS shortage, rw.shortage_reason, rw.shortage_detail
      FROM fbx_rows w LEFT JOIN fbx_row_work rw ON rw.row_id = w.id
      WHERE w.run_id = ? AND w.match_state NOT IN ('picking_only','retired') ORDER BY w.pack_group_id, w.id`).all(run.id)
      .map((r) => ({ ...r, remaining: r.planned_qty - r.placed - r.shortage })).filter((r) => r.remaining !== 0);
    // 投入+不足 > 予定 は不変条件違反 (通常は addPlacement / 添付で防いでいる)。完了させず専用エラー (Codex R13 #3)
    const over = rows.filter((r) => r.remaining < 0);
    if (over.length > 0) {
      return { ok: false, error: 'over_planned', rows: over.map((r) => ({ id: r.id, fnsku: r.fnsku, name: r.product_name, planNo: r.plan_no, planned: r.planned_qty, placed: r.placed, shortage: r.shortage })),
        message: `予定より多く入っている商品が ${over.length} 行あります。記録を取り消すか不足を解除してから完了してください` };
    }
    if (rows.length > 0 && !acknowledge) {
      return { ok: false, error: 'incomplete', rows: rows.map((r) => ({ id: r.id, fnsku: r.fnsku, name: r.product_name, planNo: r.plan_no, planned: r.planned_qty, placed: r.placed, shortage: r.shortage, remaining: r.remaining })),
        message: `まだ入っていない商品が ${rows.length} 行あります (合計 ${rows.reduce((a, r) => a + Math.max(0, r.remaining), 0)} 個)。このまま完了すると、残りは「今回は納品しない」として記録されます` };
    }
    const now = utcNow();
    const voided = [];
    for (const b of openBoxes) {
      d.prepare(`UPDATE fbx_boxes SET status = 'void', voided_at = ?, voided_by = ?, void_reason = ? WHERE id = ?`)
        .run(now, worker?.display_name || null, '作業完了時に未使用', b.id);
      logEvent({ runId: run.id, action: 'box_void', targetType: 'box', targetId: b.id, workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
        payload: { boxNo: b.box_no, boxCode: b.box_code, reason: '作業完了時に未使用', auto: true } }, d);
      voided.push(b.box_code);
    }
    // 既存の不足 (例: 破損 2) があれば理由を上書きせず内訳で持つ: [{破損 2}, {not_shipped 3}] (Codex R13 #2 / R14 #2)
    const upShort = d.prepare(`INSERT INTO fbx_row_work (row_id, shortage_qty, shortage_reason, shortage_detail, shortage_by, updated_at) VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(row_id) DO UPDATE SET shortage_qty = excluded.shortage_qty, shortage_reason = excluded.shortage_reason, shortage_detail = excluded.shortage_detail,
        shortage_by = excluded.shortage_by, updated_at = excluded.updated_at`);
    let notShipped = 0;
    for (const r of rows) {
      const total = r.shortage + r.remaining;
      const bd = shortageBreakdownFor({ shortage: r.shortage, reason: r.shortage_reason, detail: r.shortage_detail }, total);
      upShort.run(r.id, total, bd.reason || 'not_shipped', bd.detail, worker?.display_name || null, now);
      logEvent({ runId: run.id, action: 'row_shortage', targetType: 'row', targetId: r.id, workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
        payload: { shortageQty: total, reason: 'not_shipped', auto: true, remaining: r.remaining, detail: safeJson(bd.detail, null) } }, d);
      notShipped++;
    }
    if (notShipped > 0 || voided.length > 0) bumpRunVersion(d, run.id);
    d.prepare(`UPDATE fbx_runs SET status = 'done', done_at = ? WHERE id = ?`).run(now, run.id);
    logEvent({ runId: run.id, action: 'run_done', targetType: 'run', targetId: run.id, workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { via: 'finish', notShippedRows: notShipped, voidedBoxes: voided } }, d);
    return { ok: true, notShipped, voidedBoxes: voided };
  }).immediate();
}

// ───────────────────────── 商品画像キャッシュ (images.js から使う) ─────────────────────────

/** 納品回の行のうち画像が未取得 (または none/error で retryAfterMs 以上前) の FNSKU */
export function listRowsNeedingCatalog(runId, { retryAfterMs = 24 * 3600 * 1000, limit = 80 } = {}) {
  const cutoff = new Date(Date.now() - retryAfterMs).toISOString();
  // 画像と参考単重は miniPC の同じ 1 回の呼び出しで取れる (PR3) ので、どちらかが要る商品を1つの集合で返す。
  // 行の asin (Excel 添付で入る) とキャッシュの asin が違えば取り直す (Excel 差し替えで別商品になった — Codex R13 #4)
  return getDB().prepare(`SELECT w.fnsku, MAX(w.asin) AS asin, MAX(w.seller_sku) AS seller_sku,
      MAX(i.status) AS image_status, MAX(g.status) AS weight_status FROM fbx_rows w
      LEFT JOIN fbx_product_images i ON i.fnsku = w.fnsku
      LEFT JOIN fbx_weight_refs g ON g.fnsku = w.fnsku
      WHERE w.run_id = ? AND w.fnsku != '' AND w.match_state != 'retired'
        AND (i.fnsku IS NULL OR (i.status != 'ok' AND i.fetched_at <= ?)
             OR (w.asin IS NOT NULL AND w.asin != '' AND i.asin IS NOT NULL AND UPPER(w.asin) != UPPER(i.asin))
             OR g.fnsku IS NULL OR (g.status != 'ok' AND g.fetched_at <= ?)
             OR (w.asin IS NOT NULL AND w.asin != '' AND g.asin IS NOT NULL AND UPPER(w.asin) != UPPER(g.asin)))
      GROUP BY w.fnsku LIMIT ?`).all(Number(runId), cutoff, cutoff, Number(limit));
}

export function upsertProductImage({ fnsku, asin, url, status, error = null }) {
  const st = ['ok', 'none', 'error'].includes(status) ? status : (url ? 'ok' : 'none');
  getDB().prepare(`INSERT INTO fbx_product_images (fnsku, asin, image_url, status, error_message, fetched_at) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(fnsku) DO UPDATE SET asin = COALESCE(excluded.asin, fbx_product_images.asin), image_url = excluded.image_url,
      status = excluded.status, error_message = excluded.error_message, fetched_at = excluded.fetched_at`)
    .run(String(fnsku).trim().toUpperCase(), asin || null, url || null, st, error ? String(error).slice(0, 300) : null, utcNow());
}

/** 画像キャッシュの状態 (管理画面の診断用) */
export function listProductImages(fnskus) {
  const list = [...new Set((fnskus || []).map((f) => String(f).trim().toUpperCase()).filter(Boolean))];
  if (list.length === 0) return [];
  const out = [];
  const stmt = getDB().prepare('SELECT * FROM fbx_product_images WHERE fnsku = ?');
  for (const f of list) { const r = stmt.get(f); if (r) out.push(r); }
  return out;
}

// ───────────────────────── 重量補助 (PR3・要件 §7) ─────────────────────────

const round1 = (v) => Math.round(Number(v) * 10) / 10;
const DEFAULT_RULES = { target_g: 28000, limit_g: 30000 };

/** 現行の重量ルール (適用開始が今以前で最新のもの) */
export function getWeightRules(d = getDB()) {
  const r = d.prepare(`SELECT * FROM fbx_weight_rules WHERE effective_from <= ? ORDER BY effective_from DESC, id DESC LIMIT 1`).get(utcNow())
    || d.prepare('SELECT * FROM fbx_weight_rules ORDER BY id DESC LIMIT 1').get();
  return r || { ...DEFAULT_RULES, effective_from: null, updated_by: null, updated_at: null };
}

/** ルールの変更 (本社の管理画面)。履歴として1行足す (過去の回はスナップショットを見るので影響しない) */
export function setWeightRules({ targetG, limitG, actor }) {
  const t = Math.round(Number(targetG));
  const l = Math.round(Number(limitG));
  if (!Number.isFinite(t) || t <= 0 || !Number.isFinite(l) || l <= 0) {
    return { ok: false, error: 'bad_value', message: '目標・上限は正の数 (g) で入れてください' };
  }
  if (t > l) return { ok: false, error: 'bad_value', message: '目標 (黄警告) は上限以下にしてください' };
  if (l > 100000) return { ok: false, error: 'bad_value', message: '上限が大きすぎます (100kg まで)' };
  const d = getDB();
  d.prepare(`INSERT INTO fbx_weight_rules (target_g, limit_g, effective_from, updated_by, updated_at) VALUES (?, ?, ?, ?, ?)`)
    .run(t, l, utcNow(), actor || null, utcNow());
  safeLogEvent({ action: 'weight_rules_set', targetType: 'rules', deviceLabel: actor || null, ok: true, payload: { targetG: t, limitG: l } });
  return { ok: true, targetG: t, limitG: l };
}

/**
 * 納品回に適用する重量ルール。開始時にスナップショットした値を使い、無ければ現行ルール
 * (要件 §7「run開始時に適用値をスナップショット」)
 */
export function runWeightLimits(run) {
  const cur = getWeightRules();
  return {
    targetG: Number(run?.weight_target_g) > 0 ? Number(run.weight_target_g) : cur.target_g,
    limitG: Number(run?.weight_limit_g) > 0 ? Number(run.weight_limit_g) : cur.limit_g,
    snapshotted: Number(run?.weight_target_g) > 0 && Number(run?.weight_limit_g) > 0,
  };
}

/** 参考単重のキャッシュ更新 (weights は images と同じ取得ループから呼ばれる) */
export function upsertWeightRef({ fnsku, asin, weightG, raw = null, status, error = null }) {
  const g = Number(weightG) > 0 ? round1(weightG) : null;
  const st = ['ok', 'none', 'error'].includes(status) ? status : (g ? 'ok' : 'none');
  const key = String(fnsku).trim().toUpperCase();
  const d = getDB();
  // 元データと採用値 (projection) は同じトランザクションで動かす (Codex PR3 #5)
  d.transaction(() => {
    d.prepare(`INSERT INTO fbx_weight_refs (fnsku, asin, weight_g, raw_value, source, status, error_message, fetched_at)
      VALUES (?, ?, ?, ?, 'sp_api_package', ?, ?, ?)
      ON CONFLICT(fnsku) DO UPDATE SET asin = COALESCE(excluded.asin, fbx_weight_refs.asin), weight_g = excluded.weight_g,
        raw_value = excluded.raw_value, status = excluded.status, error_message = excluded.error_message, fetched_at = excluded.fetched_at`)
      .run(key, asin || null, g, raw == null ? null : String(raw).slice(0, 60), st, error ? String(error).slice(0, 300) : null, utcNow());
    recomputeWeightCurrent(key, d);
  }).immediate();
}

/**
 * 採用値 (projection) を全件作り直す。起動時に 1 回走らせて、元データとの食い違いを自己修復する
 * (Codex PR3 #5)。件数 = これまでに重さが分かった商品の数なので軽い
 */
export function rebuildWeightCurrent(d = getDB()) {
  // 採用値にだけ残っている孤児行も対象にする (元データが消えていれば recomputeWeightCurrent が消す)。
  // キーの取得も同じトランザクションの中で行う
  let n = 0;
  d.transaction(() => {
    const keys = d.prepare(`SELECT fnsku FROM fbx_weight_refs
      UNION SELECT fnsku FROM fbx_weight_measurements
      UNION SELECT fnsku FROM fbx_weight_current`).all();
    for (const k of keys) recomputeWeightCurrent(k.fnsku, d);
    n = keys.length;
  })();
  return n;
}

/**
 * 採用単重の作り直し (projection)。実測 (有効な最新) → 参考値 の順。どちらも無ければ行を消す
 * = 「単重不明」。実測値への自動昇格はしない (要件 §7: 逆算は保証しない)
 */
export function recomputeWeightCurrent(fnsku, d = getDB()) {
  const key = String(fnsku).trim().toUpperCase();
  if (!key) return null;
  const m = d.prepare(`SELECT * FROM fbx_weight_measurements WHERE fnsku = ? AND revoked_at IS NULL
    ORDER BY measured_at DESC, id DESC LIMIT 1`).get(key);
  const ref = d.prepare(`SELECT * FROM fbx_weight_refs WHERE fnsku = ? AND status = 'ok' AND weight_g > 0`).get(key);
  const chosen = m ? { unitG: m.unit_g, source: 'measured', basisId: m.id, sampleQty: m.sample_qty }
    : (ref ? { unitG: ref.weight_g, source: 'catalog', basisId: null, sampleQty: null } : null);
  if (!chosen) { d.prepare('DELETE FROM fbx_weight_current WHERE fnsku = ?').run(key); return null; }
  // 中身が変わらないときは書かない (updated_at = 採用値が変わった時刻。起動時の作り直しで全件が今の時刻にならないように)
  const now = d.prepare('SELECT unit_g, source, basis_id, sample_qty FROM fbx_weight_current WHERE fnsku = ?').get(key);
  if (now && now.unit_g === chosen.unitG && now.source === chosen.source
      && (now.basis_id ?? null) === (chosen.basisId ?? null) && (now.sample_qty ?? null) === (chosen.sampleQty ?? null)) {
    return chosen;
  }
  d.prepare(`INSERT INTO fbx_weight_current (fnsku, unit_g, source, basis_id, sample_qty, updated_at) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(fnsku) DO UPDATE SET unit_g = excluded.unit_g, source = excluded.source,
      basis_id = excluded.basis_id, sample_qty = excluded.sample_qty, updated_at = excluded.updated_at`)
    .run(key, chosen.unitG, chosen.source, chosen.basisId, chosen.sampleQty, utcNow());
  return chosen;
}

/**
 * 実測単重の登録 (中原さん要望「何個で何g」)。1個ずつ量ると軽い商品で誤差が大きいので、
 * まとめて量って個数で割る。作業者の操作でよい (PIN 不要 — 数の訂正と同じ 9/3 の判断)
 */
export function addWeightMeasurement({ fnsku, sampleQty, totalG, method = 'scale', note = null, runId = null, worker, deviceLabel }) {
  const key = String(fnsku || '').trim().toUpperCase();
  if (!key) return { ok: false, error: 'bad_fnsku', message: '商品 (FNSKU) が分かりません' };
  const n = Math.round(Number(sampleQty));
  const total = Math.round(Number(totalG) * 10) / 10;
  if (!Number.isInteger(n) || n < 1 || n > 9999) return { ok: false, error: 'bad_qty', message: '量った個数を正しく入れてください' };
  if (!Number.isFinite(total) || total <= 0 || total > 200000) return { ok: false, error: 'bad_weight', message: '量った重さ (g) を正しく入れてください' };
  const unit = round1(total / n);
  if (unit <= 0) return { ok: false, error: 'bad_weight', message: '1個あたりが 0g になります。単位 (g) を確かめてください' };
  const d = getDB();
  return d.transaction(() => {
    // 単重は全ての納品回で共通のマスタになる → 打ち間違い・古い画面からの FNSKU を通さない (Codex PR3 #6):
    // 「いま作業している納品回に実在する商品」だけ受ける
    const run = d.prepare('SELECT id, status FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'run_required', message: '納品回が分かりません (画面を開き直してください)' };
    if (run.status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    const inRun = d.prepare(`SELECT 1 AS x FROM fbx_rows WHERE run_id = ? AND fnsku = ? AND match_state != 'retired' LIMIT 1`).get(run.id, key);
    if (!inRun) return { ok: false, error: 'not_in_run', message: 'この商品はこの納品回にありません (画面を開き直してください)' };
    const info = d.prepare(`INSERT INTO fbx_weight_measurements
        (fnsku, sample_qty, total_g, unit_g, method, note, run_id, worker_id, worker_name, device_label, measured_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(key, n, total, unit, ['scale', 'manual'].includes(method) ? method : 'scale',
        note ? String(note).slice(0, 200) : null, run.id,
        worker?.id || null, worker?.display_name || null, deviceLabel || null, utcNow());
    recomputeWeightCurrent(key, d);
    logEvent({ runId: run.id, action: 'weight_measure', targetType: 'product', targetId: null,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { fnsku: key, sampleQty: n, totalG: total, unitG: unit, method } }, d);
    return { ok: true, id: Number(info.lastInsertRowid), unitG: unit, sampleQty: n, totalG: total };
  }).immediate();
}

/**
 * 実測の取消 (打ち間違い)。行は消さず revoked_at を立てて採用値を作り直す。
 * 単重は全ての納品回で共通のマスタなので、現場が取り消せるのは**いま作業している回で登録した記録**だけ
 * (過去回・別回のものは職員のみ — Codex PR3 R2 #4)
 */
export function revokeWeightMeasurement({ id, runId = null, byStaff = false, worker, deviceLabel }) {
  const d = getDB();
  return d.transaction(() => {
    const m = d.prepare('SELECT * FROM fbx_weight_measurements WHERE id = ?').get(Number(id));
    if (!m) return { ok: false, error: 'not_found', message: '記録が見つかりません' };
    if (m.revoked_at) return { ok: false, error: 'already_revoked', message: 'この記録は取消済みです' };
    if (!byStaff) {
      const run = m.run_id ? d.prepare('SELECT id, status FROM fbx_runs WHERE id = ?').get(m.run_id) : null;
      if (!run || run.status !== 'active' || Number(runId) !== run.id) {
        return { ok: false, error: 'staff_required',
          message: 'この重さは今の納品回で登録したものではありません (取り消すには職員の確認が必要です)' };
      }
    }
    d.prepare('UPDATE fbx_weight_measurements SET revoked_at = ?, revoked_by = ? WHERE id = ?')
      .run(utcNow(), worker?.display_name || deviceLabel || null, m.id);
    const after = recomputeWeightCurrent(m.fnsku, d);
    logEvent({ runId: m.run_id, action: 'weight_measure_revoke', targetType: 'product', targetId: m.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { fnsku: m.fnsku, unitG: m.unit_g } }, d);
    return { ok: true, current: after };
  }).immediate();
}

/** ある商品の実測履歴 (取消も含む。iPad・管理画面の「重さの記録」) */
export function listWeightMeasurements(fnsku, limit = 20) {
  return getDB().prepare(`SELECT * FROM fbx_weight_measurements WHERE fnsku = ? ORDER BY id DESC LIMIT ?`)
    .all(String(fnsku || '').trim().toUpperCase(), Number(limit));
}

/** 納品回の商品ごとの単重 (採用値・参考値・実測件数)。管理画面の一覧と iPad の表示に使う */
export function listRunWeights(runId) {
  return getDB().prepare(`SELECT w.fnsku, MAX(w.product_name) AS product_name, MAX(w.plan_no) AS plan_no,
      MAX(w.planned_qty) AS planned_qty,
      c.unit_g, c.source, c.sample_qty, c.updated_at,
      g.weight_g AS ref_g, g.status AS ref_status, g.error_message AS ref_error, g.fetched_at AS ref_at,
      (SELECT COUNT(*) FROM fbx_weight_measurements m WHERE m.fnsku = w.fnsku AND m.revoked_at IS NULL) AS meas_count
    FROM fbx_rows w
    LEFT JOIN fbx_weight_current c ON c.fnsku = w.fnsku
    LEFT JOIN fbx_weight_refs g ON g.fnsku = w.fnsku
    WHERE w.run_id = ? AND w.fnsku != '' AND w.match_state != 'retired'
    GROUP BY w.fnsku ORDER BY MAX(w.pack_group_id), MAX(w.excel_row), MAX(w.picking_row_no)`).all(Number(runId));
}

/**
 * 箱の推定重量 = Σ(数量 × 採用単重) + 資材の自重。
 * 単重不明の商品がある箱は「推定 21.4kg (重量不明 12点を除く)」と欠損数をセットで出す (要件 §7)
 */
export function boxWeightEstimates(runId, d = getDB()) {
  const boxes = d.prepare(`SELECT b.id, b.material_code, m.tare_g FROM fbx_boxes b
      JOIN fbx_pack_groups g ON g.id = b.pack_group_id
      LEFT JOIN fbx_box_materials m ON m.code = b.material_code
      WHERE g.run_id = ? AND b.status != 'void'`).all(Number(runId));
  const items = d.prepare(`SELECT p.box_id, w.fnsku, MAX(w.product_name) AS product_name, SUM(p.qty) AS qty,
      MAX(c.unit_g) AS unit_g, MAX(c.source) AS source
    FROM fbx_placements p JOIN fbx_rows w ON w.id = p.row_id
    LEFT JOIN fbx_weight_current c ON c.fnsku = w.fnsku
    WHERE p.run_id = ? AND p.revoked_at IS NULL GROUP BY p.box_id, w.fnsku`).all(Number(runId));
  const byBox = new Map();
  for (const b of boxes) {
    const tareG = Number.isFinite(Number(b.tare_g)) && b.tare_g != null ? Number(b.tare_g) : null;
    byBox.set(b.id, { boxId: b.id, contentG: 0, tareG, tareKnown: tareG != null, estG: tareG || 0, unknownQty: 0, unknownItems: [] });
  }
  for (const it of items) {
    const e = byBox.get(it.box_id);
    if (!e) continue;
    if (Number(it.unit_g) > 0) { e.contentG += Number(it.unit_g) * Number(it.qty); e.estG += Number(it.unit_g) * Number(it.qty); }
    else { e.unknownQty += Number(it.qty); e.unknownItems.push({ fnsku: it.fnsku, productName: it.product_name, qty: Number(it.qty) }); }
  }
  for (const e of byBox.values()) {
    e.contentG = round1(e.contentG);
    e.estG = round1(e.estG);
    e.complete = e.unknownQty === 0 && e.tareKnown;   // 「あと約N個」を出してよいのはこのときだけ
  }
  return byBox;
}

/** 1箱分の推定 (クローズ時の判定に使う。同じトランザクションの接続で読む) */
export function estimateBoxWeight(boxId, d = getDB()) {
  const b = d.prepare(`SELECT b.id, b.material_code, m.tare_g, g.run_id FROM fbx_boxes b
      JOIN fbx_pack_groups g ON g.id = b.pack_group_id
      LEFT JOIN fbx_box_materials m ON m.code = b.material_code WHERE b.id = ?`).get(Number(boxId));
  if (!b) return null;
  const items = d.prepare(`SELECT w.fnsku, MAX(w.product_name) AS product_name, SUM(p.qty) AS qty, MAX(c.unit_g) AS unit_g
    FROM fbx_placements p JOIN fbx_rows w ON w.id = p.row_id
    LEFT JOIN fbx_weight_current c ON c.fnsku = w.fnsku
    WHERE p.box_id = ? AND p.revoked_at IS NULL GROUP BY w.fnsku`).all(b.id);
  const tareG = Number.isFinite(Number(b.tare_g)) && b.tare_g != null ? Number(b.tare_g) : null;
  let contentG = 0, unknownQty = 0;
  const unknownItems = [];
  for (const it of items) {
    if (Number(it.unit_g) > 0) contentG += Number(it.unit_g) * Number(it.qty);
    else { unknownQty += Number(it.qty); unknownItems.push({ fnsku: it.fnsku, productName: it.product_name, qty: Number(it.qty) }); }
  }
  return {
    boxId: b.id, contentG: round1(contentG), tareG, tareKnown: tareG != null,
    estG: round1(contentG + (tareG || 0)), unknownQty, unknownItems, complete: unknownQty === 0 && tareG != null,
  };
}

/**
 * 実測と推定の乖離ヒント (要件 §7)。絶対 500g 以上 かつ 相対 5% 以上のときだけ出す。
 * 単重不明の商品や自重未登録の資材があるときは、そもそも推定が当てにならないので黙る
 */
export function weightMismatchHint(measuredG, est) {
  if (!est || !est.complete || !(est.estG > 0)) return null;
  const diff = Number(measuredG) - est.estG;
  const abs = Math.abs(diff);
  if (abs < 500 || abs / est.estG < 0.05) return null;
  return {
    diffG: round1(diff), estG: est.estG, measuredG: round1(measuredG),
    message: `はかりの ${round1(measuredG / 1000)}kg と推定 ${round1(est.estG / 1000)}kg が ${round1(abs / 1000)}kg 違います。数量か単重のどちらかが怪しいので、中身をもう一度確かめてください`,
  };
}

export function listRuns(limit = 30) {
  return getDB().prepare(`SELECT r.*,
      (SELECT COUNT(*) FROM fbx_rows w WHERE w.run_id = r.id) AS row_count,
      (SELECT COUNT(*) FROM fbx_pack_groups g WHERE g.run_id = r.id) AS group_count
    FROM fbx_runs r ORDER BY r.id DESC LIMIT ?`).all(Number(limit) || 30);
}

export function getRun(runId) {
  return getDB().prepare('SELECT * FROM fbx_runs WHERE id = ?').get(Number(runId)) || null;
}

/**
 * 作業画面用の全状態。rows は placed (有効割当合計) 付き、boxes は中身サマリ付き
 */
export function getRunState(runId) {
  const d = getDB();
  const run = getRun(runId);
  if (!run) return null;
  const groups = d.prepare('SELECT * FROM fbx_pack_groups WHERE run_id = ? ORDER BY id').all(run.id);
  const rows = d.prepare(`SELECT w.*,
      COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0) AS placed,
      rw.label_worker, rw.check_worker, rw.check_worker_source, rw.shortage_qty, rw.shortage_reason, rw.shortage_detail, rw.shortage_by,
      (SELECT i.image_url FROM fbx_product_images i WHERE i.fnsku = w.fnsku AND i.status = 'ok') AS image_url
    FROM fbx_rows w LEFT JOIN fbx_row_work rw ON rw.row_id = w.id
    WHERE w.run_id = ? ORDER BY w.pack_group_id, w.excel_row, w.picking_row_no, w.id`).all(run.id);
  const boxes = d.prepare(`SELECT b.*,
      COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.box_id = b.id AND p.revoked_at IS NULL), 0) AS total_qty,
      (SELECT COUNT(DISTINCT p.row_id) FROM fbx_placements p WHERE p.box_id = b.id AND p.revoked_at IS NULL) AS sku_count
    FROM fbx_boxes b JOIN fbx_pack_groups g ON g.id = b.pack_group_id
    WHERE g.run_id = ? ORDER BY b.pack_group_id, b.box_no`).all(run.id);
  const placements = d.prepare(`SELECT p.* FROM fbx_placements p
    WHERE p.run_id = ? AND p.revoked_at IS NULL ORDER BY p.box_id, p.box_seq`).all(run.id);
  const excelFiles = d.prepare(`SELECT id, original_name, sha256, fingerprint, uploaded_by, uploaded_at FROM fbx_excel_files WHERE run_id = ? ORDER BY id`).all(run.id);
  assignAmazonBoxNumbers(groups, boxes);
  // PR3 重量補助: 箱ごとの推定 (Σ数量×採用単重 + 資材自重) と、商品ごとの採用単重・この回のルール
  const estimates = boxWeightEstimates(run.id, d);
  for (const b of boxes) b.est = estimates.get(b.id) || null;
  const weights = {};
  for (const w of d.prepare(`SELECT c.fnsku, c.unit_g, c.source, c.sample_qty FROM fbx_weight_current c
      WHERE c.fnsku IN (SELECT fnsku FROM fbx_rows WHERE run_id = ?)`).all(run.id)) {
    weights[w.fnsku] = { unitG: w.unit_g, source: w.source, sampleQty: w.sample_qty };
  }
  const weightLimits = runWeightLimits(run);
  const latest = d.prepare(`SELECT id, data_version, file_name, sha256, created_by, created_at
    FROM fbx_exports WHERE run_id = ? ORDER BY id DESC LIMIT 1`).get(run.id) || null;
  const exportState = {
    latest,
    stale: !!(latest && latest.data_version < run.data_version),
    staUploadedAt: run.sta_uploaded_at || null,
    staExportId: run.sta_export_id || null,
  };
  return { run, groups, rows, boxes, placements, exportState, excelFiles, weights, weightLimits };
}

/**
 * Amazon 側の箱番号 (Excel の「輸送箱n」列) を割り当てる。取消 (void) した箱は飛ばして
 * box_no 順に 1..N を詰める → 欠番があると箱札の番号 (box_no) と Amazon 番号がずれるので、
 * boxes[].amazon_box_no / amazon_name を画面・箱札・チェックリストに出す
 */
function assignAmazonBoxNumbers(groups, boxes) {
  for (const g of groups) {
    const names = safeJson(g.structure_json, {})?.boxNames || {};
    let n = 0;
    for (const b of boxes.filter((x) => x.pack_group_id === g.id).sort((a, c) => a.box_no - c.box_no)) {
      if (b.status === 'void') { b.amazon_box_no = null; b.amazon_name = null; continue; }
      n += 1;
      b.amazon_box_no = n;
      b.amazon_name = names[String(n)] || `B${n}`;
    }
  }
}

// ───────────────────────── 箱 ─────────────────────────

// 箱コード = グループ名-連番 (例: 通常-1)。Amazon 側の「P1 - B1」の B (=Box) は現場に意味が無いので付けない (中原さん 9/3)
const boxCodeOf = (groupName, boxNo) => `${groupName}-${boxNo}`;

export function createBox({ packGroupId, materialCode, worker, deviceLabel }) {
  const d = getDB();
  return d.transaction(() => {
    const g = d.prepare('SELECT g.*, r.status AS run_status FROM fbx_pack_groups g JOIN fbx_runs r ON r.id = g.run_id WHERE g.id = ?')
      .get(Number(packGroupId));
    if (!g) return { ok: false, error: 'not_found', message: '梱包グループが見つかりません' };
    if (g.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません (本社に確認してください)' };
    if (materialCode != null && materialCode !== '') {
      const mat = d.prepare('SELECT code FROM fbx_box_materials WHERE code = ? AND active = 1').get(String(materialCode));
      if (!mat) return { ok: false, error: 'bad_material', message: '資材の種類が不正です' };
    }
    const next = (d.prepare('SELECT COALESCE(MAX(box_no), 0) + 1 AS n FROM fbx_boxes WHERE pack_group_id = ?').get(g.id)).n;
    // 上限 = Excel テンプレの箱列数 (それ以上はテンプレに書けない — 超えるなら本社がSTAで箱数を増やして再DL)
    if (g.max_box_columns && next > g.max_box_columns) {
      return { ok: false, error: 'box_limit', message: `このグループの箱はテンプレ上限 (${g.max_box_columns}箱) に達しています。本社に連絡してください` };
    }
    const info = d.prepare(`INSERT INTO fbx_boxes (pack_group_id, box_no, box_code, material_code, created_by, created_at)
      VALUES (?, ?, ?, ?, ?, ?)`)
      .run(g.id, next, boxCodeOf(g.display_name, next), materialCode || null, worker?.display_name || null, utcNow());
    const boxId = Number(info.lastInsertRowid);
    bumpRunVersion(d, g.run_id);
    logEvent({ runId: g.run_id, action: 'box_create', targetType: 'box', targetId: boxId,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { boxNo: next, materialCode: materialCode || null } }, d);
    return { ok: true, boxId, boxNo: next, boxCode: boxCodeOf(g.display_name, next) };
  }).immediate();
}

/**
 * 箱クローズ。実測重量kg必須 (Amazon Excel の箱重量欄になる)。読み合わせは画面側の責務。
 * PR3: 上限 (既定30kg) 超えは職員の承認 (staffApproved) が無いとブロック。閉じた時点の推定を
 * 生データとして残し、推定と大きく違うときはヒントを返す (閉じること自体は止めない)
 */
export function closeBox({ boxId, measuredKg, closedReason, cushionLevel, worker, deviceLabel, expectedContentVersion = null,
  staffApproved = false, approvedBy = null }) {
  // 桁は 1g (小数3桁) まで。Excel へはこの値をそのまま書く (書き込み側で丸めない — Codex PR2 #5)
  const kg = Math.round(Number(measuredKg) * 1000) / 1000;
  if (!Number.isFinite(kg) || kg <= 0 || kg > 200) {
    return { ok: false, error: 'bad_weight', message: '実測重量 (kg) を正しく入力してください' };
  }
  const reason = closedReason == null || closedReason === '' ? null : String(closedReason);
  const REASONS = ['weight_limit', 'volume_full', 'fragile', 'items_done', 'group_done', 'other'];
  if (reason && !REASONS.includes(reason)) return { ok: false, error: 'bad_reason', message: '締め理由が不正です' };
  const cushion = cushionLevel == null || cushionLevel === '' ? null : String(cushionLevel);
  if (cushion && !['none', 'little', 'much'].includes(cushion)) return { ok: false, error: 'bad_cushion', message: '緩衝材の量が不正です' };
  const d = getDB();
  return d.transaction(() => {
    const b = d.prepare(`SELECT b.*, g.run_id, r.status AS run_status, r.weight_target_g, r.weight_limit_g FROM fbx_boxes b
      JOIN fbx_pack_groups g ON g.id = b.pack_group_id JOIN fbx_runs r ON r.id = g.run_id WHERE b.id = ?`)
      .get(Number(boxId));
    if (!b) return { ok: false, error: 'not_found', message: '箱が見つかりません' };
    if (b.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    if (b.status === 'closed') return { ok: false, error: 'already_closed', message: 'この箱は既に閉じられています' };
    if (b.status === 'void') return { ok: false, error: 'box_void', message: 'この箱は取消済みです' };
    // 読み合わせを始めてから中身が変わっていたら閉じさせない (Codex R18 #1: 古い重量で閉じる事故を防ぐ)
    if (expectedContentVersion != null && Number(expectedContentVersion) !== b.content_version) {
      return { ok: false, error: 'box_changed', contentVersion: b.content_version,
        message: 'この箱の中身が変わりました (他の iPad で直された可能性)。もう一度読み合わせてから閉じてください' };
    }
    const qty = d.prepare('SELECT COALESCE(SUM(qty),0) q FROM fbx_placements WHERE box_id = ? AND revoked_at IS NULL').get(b.id).q;
    if (qty === 0) return { ok: false, error: 'empty_box', message: '空の箱は閉じられません (使わない箱は職員が「箱を取消」してください)' };
    // 絶対上限 (既定30kg) は Amazon の受入条件。実測で判定する — 推定は単重不明があると当てにならない
    const limits = runWeightLimits(b);
    const measuredG = Math.round(kg * 1000);
    if (measuredG > limits.limitG && !staffApproved) {
      return { ok: false, error: 'over_limit', limitKg: limits.limitG / 1000, measuredKg: kg,
        message: `${kg}kg は 1箱の上限 ${limits.limitG / 1000}kg を超えています。中身を分けてください (どうしてもこのまま閉じるなら職員の承認が要ります)` };
    }
    const est = estimateBoxWeight(b.id, d);
    const overLimit = measuredG > limits.limitG;
    d.prepare(`UPDATE fbx_boxes SET status = 'closed', measured_weight_kg = ?, closed_at = ?, closed_by = ?, closed_reason = ?, cushion_level = ?,
        est_weight_g_at_close = ?, est_unknown_qty_at_close = ?, tare_g_at_close = ?,
        limit_override_by = ?, limit_override_at = ? WHERE id = ?`)
      .run(kg, utcNow(), worker?.display_name || null, reason, cushion,
        est ? est.estG : null, est ? est.unknownQty : null, est ? est.tareG : null,
        overLimit ? (approvedBy || '(不明)') : null, overLimit ? utcNow() : null, b.id);
    bumpRunVersion(d, b.run_id);
    logEvent({ runId: b.run_id, action: 'box_close', targetType: 'box', targetId: b.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { measuredKg: kg, closedReason: reason, cushionLevel: cushion, totalQty: qty,
        estG: est?.estG ?? null, estUnknownQty: est?.unknownQty ?? null, tareG: est?.tareG ?? null,
        overLimit: overLimit ? { limitG: limits.limitG, approvedBy } : undefined } }, d);
    return {
      ok: true,
      overTarget: measuredG > limits.targetG,
      overLimit,
      targetKg: limits.targetG / 1000, limitKg: limits.limitG / 1000,
      hint: weightMismatchHint(measuredG, est),
    };
  }).immediate();
}

/** 再オープン (職員のみ・理由必須)。実測重量は履歴 (イベント) に残して現在値を解除 → 再クローズで再計測必須 */
export function reopenBox({ boxId, reason, worker, deviceLabel }) {
  const r = String(reason || '').trim();
  if (!r) return { ok: false, error: 'reason_required', message: '再オープンの理由を入力してください' };
  const d = getDB();
  return d.transaction(() => {
    const b = d.prepare(`SELECT b.*, g.run_id, run.status AS run_status FROM fbx_boxes b
      JOIN fbx_pack_groups g ON g.id = b.pack_group_id JOIN fbx_runs run ON run.id = g.run_id WHERE b.id = ?`)
      .get(Number(boxId));
    if (!b) return { ok: false, error: 'not_found', message: '箱が見つかりません' };
    if (b.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    if (b.status !== 'closed') return { ok: false, error: 'not_closed', message: 'この箱は閉じられていません' };
    d.prepare(`UPDATE fbx_boxes SET status = 'open', measured_weight_kg = NULL, closed_at = NULL, closed_by = NULL,
      closed_reason = NULL, cushion_level = NULL, reopen_count = reopen_count + 1,
      est_weight_g_at_close = NULL, est_unknown_qty_at_close = NULL, tare_g_at_close = NULL,
      limit_override_by = NULL, limit_override_at = NULL WHERE id = ?`).run(b.id);
    bumpRunVersion(d, b.run_id);
    logEvent({ runId: b.run_id, action: 'box_reopen', targetType: 'box', targetId: b.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { reason: r, previousWeightKg: b.measured_weight_kg, previousClosedAt: b.closed_at } }, d);
    return { ok: true };
  }).immediate();
}

/**
 * 使わなかった箱の取消 (職員のみ・理由必須)。中身が残っていれば先に取り消してもらう。
 * box_no は再利用しない (箱札の番号を動かさない) ので Amazon 箱番号は詰め直しになる —
 * 画面で「G1-B3 → Amazon P1 - B2」の対応を出す (assignAmazonBoxNumbers)
 */
export function voidBox({ boxId, reason, worker, deviceLabel }) {
  const r = String(reason || '').trim();
  if (!r) return { ok: false, error: 'reason_required', message: '取消の理由を入力してください' };
  const d = getDB();
  return d.transaction(() => {
    const b = d.prepare(`SELECT b.*, g.run_id, run.status AS run_status FROM fbx_boxes b
      JOIN fbx_pack_groups g ON g.id = b.pack_group_id JOIN fbx_runs run ON run.id = g.run_id WHERE b.id = ?`)
      .get(Number(boxId));
    if (!b) return { ok: false, error: 'not_found', message: '箱が見つかりません' };
    if (b.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    if (b.status === 'void') return { ok: true, already: true };
    const qty = d.prepare('SELECT COALESCE(SUM(qty),0) q FROM fbx_placements WHERE box_id = ? AND revoked_at IS NULL').get(b.id).q;
    if (qty > 0) return { ok: false, error: 'not_empty', message: `この箱には ${qty} 個の記録があります。先に中身を取り消してから箱を取消してください` };
    d.prepare(`UPDATE fbx_boxes SET status = 'void', voided_at = ?, voided_by = ?, void_reason = ?,
      measured_weight_kg = NULL, closed_at = NULL, closed_by = NULL,
      est_weight_g_at_close = NULL, est_unknown_qty_at_close = NULL, tare_g_at_close = NULL,
      limit_override_by = NULL, limit_override_at = NULL WHERE id = ?`)
      .run(utcNow(), worker?.display_name || null, r.slice(0, 200), b.id);
    bumpRunVersion(d, b.run_id);
    logEvent({ runId: b.run_id, action: 'box_void', targetType: 'box', targetId: b.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { boxNo: b.box_no, boxCode: b.box_code, reason: r, previousStatus: b.status } }, d);
    return { ok: true };
  }).immediate();
}

export function listBoxContents(boxId) {
  return getDB().prepare(`SELECT p.*, w.seller_sku, w.fnsku, w.product_name, w.plan_no
    FROM fbx_placements p JOIN fbx_rows w ON w.id = p.row_id
    WHERE p.box_id = ? AND p.revoked_at IS NULL ORDER BY p.box_seq`).all(Number(boxId));
}

/** 箱の現在値 (読み合わせ画面が content_version を持つため) */
export function getBox(boxId) {
  return getDB().prepare('SELECT * FROM fbx_boxes WHERE id = ?').get(Number(boxId)) || null;
}

// ───────────────────────── 割当 (物理投入) ─────────────────────────

/**
 * 割当の追加。要件 F-2: BEGIN IMMEDIATE 1トランザクションで
 *   冪等性 → 行/箱/回の状態 → 残数 → 期限制約 → box_seq 採番 → 挿入 → 監査
 * layer は任意 (manual のみ。自動推定は保存しない — Codex R2 S2)
 */
export function addPlacement({ runId, rowId, boxId, qty, expiry, layer, worker, deviceKey, deviceLabel, requestId }) {
  const q = Number(qty);
  if (!Number.isInteger(q) || q <= 0 || q > 100000) return { ok: false, error: 'bad_qty', message: '個数は1以上の整数で入力してください' };
  if (!deviceKey || !requestId) return { ok: false, error: 'bad_request', message: 'request_id がありません (画面を更新してください)' };
  const lay = layer == null || layer === '' ? null : String(layer);
  if (lay && !['bottom', 'middle', 'top'].includes(lay)) return { ok: false, error: 'bad_layer', message: '配置 (下/中/上) の値が不正です' };
  let exp = expiry == null || expiry === '' ? null : String(expiry);
  if (exp != null) {
    if (!/^\d{4}-\d{2}(-\d{2})?$/.test(exp)) return { ok: false, error: 'bad_expiry', message: '期限は YYYY-MM または YYYY-MM-DD で入力してください' };
    const probe = exp.length === 7 ? `${exp}-01` : exp;
    const t = Date.parse(probe + 'T00:00:00Z');
    if (!Number.isFinite(t)) return { ok: false, error: 'bad_expiry', message: '実在する日付を入力してください' };
    if (t < Date.now()) return { ok: false, error: 'past_expiry', message: '過去の期限は入力できません (現物を確認してください)' };
  }
  // 冪等キーはリクエスト内容に結び付ける (Codex PR1 #5): 同キーで内容が違えば 409。
  // hash はクライアントが送った生の値で計算する (期限の引き継ぎ等のサーバー側補完より前 —
  // 再送は同じ生値で来るので一致する)
  const requestHash = crypto.createHash('sha256')
    .update(JSON.stringify([Number(runId), Number(rowId), Number(boxId), q, expiry ?? null, lay]))
    .digest('hex');
  const d = getDB();
  return d.transaction(() => {
    // 冪等性: 同じ端末×request_id は前回結果を返す (再送で二重登録しない)
    const prev = d.prepare('SELECT * FROM fbx_placements WHERE device_key = ? AND request_id = ?').get(String(deviceKey), String(requestId));
    if (prev) {
      if (prev.request_hash !== requestHash) {
        return { ok: false, error: 'idempotency_conflict', message: '同じ操作IDで内容の違う記録が既にあります (画面を更新してやり直してください)' };
      }
      return { ok: true, already: true, placementId: prev.id, boxSeq: prev.box_seq,
        placed: placedOf(d, prev.row_id) };
    }
    const row = d.prepare('SELECT w.*, r.status AS run_status FROM fbx_rows w JOIN fbx_runs r ON r.id = w.run_id WHERE w.id = ?')
      .get(Number(rowId));
    if (!row) return { ok: false, error: 'not_found', message: '商品行が見つかりません (画面を更新してください)' };
    if (row.run_id !== Number(runId)) return { ok: false, error: 'bad_request', message: '納品回と商品行が一致しません (画面を更新してください)' };
    if (row.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    const excluded = rowExcludedError(row);
    if (excluded) return excluded;
    const box = d.prepare('SELECT * FROM fbx_boxes WHERE id = ?').get(Number(boxId));
    if (!box) return { ok: false, error: 'not_found', message: '箱が見つかりません (画面を更新してください)' };
    if (box.pack_group_id !== row.pack_group_id) {
      return { ok: false, error: 'wrong_group', message: 'この箱は別の梱包グループの箱です。同じシートの箱を選んでください' };
    }
    if (box.status === 'void') return { ok: false, error: 'box_void', message: 'この箱は取消済みです。別の箱を選んでください' };
    if (box.status !== 'open') return { ok: false, error: 'box_closed', message: 'この箱は閉じられています (職員が再オープンすれば入れられます)' };
    // 残数 = 予定 − 投入済み − 確定不足 (Codex PR1 #4: 不足確定後にその分を超えて入れられない)
    const placed = placedOf(d, row.id);
    const shortage = d.prepare('SELECT COALESCE(shortage_qty, 0) s FROM fbx_row_work WHERE row_id = ?').get(row.id)?.s || 0;
    if (placed + shortage + q > row.planned_qty) {
      const rem = Math.max(0, row.planned_qty - placed - shortage);
      return { ok: false, error: 'over_qty', placed, plannedQty: row.planned_qty, shortage,
        message: `予定数を超えます (予定 ${row.planned_qty} / 入力済み ${placed}${shortage ? ` / 不足確定 ${shortage}` : ''})。残りは ${rem} 個です` };
    }
    // 期限制約 (要件 §5-3): 同一納品回×同一商品行は1期限。既存と異なる期限はブロック
    if (exp != null) {
      const other = d.prepare(`SELECT DISTINCT expiry FROM fbx_placements
        WHERE row_id = ? AND revoked_at IS NULL AND expiry IS NOT NULL AND expiry != ?`).get(row.id, exp);
      if (other) {
        return { ok: false, error: 'expiry_conflict', existing: other.expiry,
          message: `この商品は既に期限 ${other.expiry} で記録されています。期限が2種類あるときは職員を呼んでください (Amazonは同一プラン内の同一商品は1期限)` };
      }
    } else {
      const existing = d.prepare(`SELECT DISTINCT expiry FROM fbx_placements
        WHERE row_id = ? AND revoked_at IS NULL AND expiry IS NOT NULL`).get(row.id);
      if (existing) exp = existing.expiry;   // 2回目以降の入力は既存期限を引き継ぐ (入力の手間削減)
    }
    // box_seq: 取消済みも含めた最大+1 (欠番は再利用しない — 監査・分析の正本)
    const seq = d.prepare('SELECT COALESCE(MAX(box_seq), 0) + 1 AS n FROM fbx_placements WHERE box_id = ?').get(box.id).n;
    const info = d.prepare(`INSERT INTO fbx_placements
      (run_id, row_id, box_id, qty, expiry, box_seq, placement_layer, layer_source, worker_id, worker_name, device_key, request_id, request_hash, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(row.run_id, row.id, box.id, q, exp, seq, lay, lay ? 'manual' : null,
        worker?.id ?? null, worker?.display_name ?? null, String(deviceKey), String(requestId), requestHash, utcNow());
    const placementId = Number(info.lastInsertRowid);
    d.prepare('UPDATE fbx_boxes SET content_version = content_version + 1 WHERE id = ?').run(box.id);
    bumpRunVersion(d, row.run_id);
    // 確認した人がまだ空なら、入れた人を同じトランザクションで記録する (別POSTにしない)
    const autoCheck = syncAutoCheckWorker(d, row.id, { runId: row.run_id, worker, deviceLabel });
    logEvent({ runId: row.run_id, action: 'placement_add', targetType: 'placement', targetId: placementId,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { rowId: row.id, boxId: box.id, boxNo: box.box_no, qty: q, expiry: exp, layer: lay, boxSeq: seq } }, d);
    return { ok: true, placementId, boxSeq: seq, placed: placed + q, plannedQty: row.planned_qty, expiry: exp,
      checkWorker: autoCheck?.checkWorker ?? null };
  }).immediate();
}

function placedOf(d, rowId) {
  return d.prepare('SELECT COALESCE(SUM(qty),0) q FROM fbx_placements WHERE row_id = ? AND revoked_at IS NULL').get(rowId).q;
}

/**
 * 「確認した人」の自動記録 (Codex PR2.6-R1 high#1・#2 / medium#1)。
 * 自動で入る値 = **その行に残っている一番古い有効な投入をした人**。投入・取消・数の修正と同じ
 * トランザクションで呼ぶので (a) 他端末と競合して別の人の名前を上書きすることがなく、
 * (b) 通信断で「入れた記録はあるのに確認した人が空」になることもない。
 * 人が選んだ名前 (source='manual'。移行前からある値 = source NULL も含む) には触らない。
 * 取消・数0への修正で有効な投入が 0 件になれば、自動で入れた分だけ消す。
 */
function syncAutoCheckWorker(d, rowId, { runId, worker, deviceLabel } = {}) {
  const rw = d.prepare('SELECT check_worker, check_worker_source, check_worker_placement_id FROM fbx_row_work WHERE row_id = ?').get(rowId);
  const same = (checkWorker, source) => ({ checkWorker, source, changed: false });
  if (rw?.check_worker && rw.check_worker_source !== 'auto') {
    return same(rw.check_worker, rw.check_worker_source ?? 'manual');   // 人が選んだ名前は動かさない
  }
  const p = d.prepare(`SELECT id, worker_name FROM fbx_placements
    WHERE row_id = ? AND revoked_at IS NULL AND worker_name IS NOT NULL AND worker_name != ''
    ORDER BY id LIMIT 1`).get(rowId);
  const name = p ? String(p.worker_name).slice(0, 30) : null;
  const pid = p ? p.id : null;
  if (!rw && name == null) return same(null, null);                                       // 記録も無く、消すものも無い
  if (rw && (rw.check_worker ?? null) === name
    && (rw.check_worker_placement_id ?? null) === pid) {
    return same(name, name ? 'auto' : null);                                              // 変化なし (updated_at も動かさない)
  }
  d.prepare(`INSERT INTO fbx_row_work (row_id, check_worker, check_worker_source, check_worker_placement_id, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(row_id) DO UPDATE SET check_worker = excluded.check_worker,
      check_worker_source = excluded.check_worker_source,
      check_worker_placement_id = excluded.check_worker_placement_id,
      updated_at = excluded.updated_at`)
    .run(rowId, name, name ? 'auto' : null, pid, utcNow());
  logEvent({ runId, action: 'row_check_worker_auto', targetType: 'row', targetId: rowId,
    workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
    payload: { from: rw?.check_worker ?? null, to: name, placementId: pid } }, d);
  return { checkWorker: name, source: name ? 'auto' : null, changed: true };
}

/**
 * Excel の差し替えで対象外になった行 (retired) / 添付した Excel に無い行 (picking_only) への更新は拒否
 * (Codex PR2.5 R2: 古い画面からの投入が成功すると、完了判定・出力から外れた「幽霊の投入」になる)
 */
function rowExcludedError(row) {
  if (row.match_state === 'retired') return { ok: false, error: 'row_excluded', message: 'この商品は Excel の差し替えで対象外になりました (画面を更新してください)' };
  if (row.match_state === 'picking_only') return { ok: false, error: 'row_excluded', message: 'この商品は添付した Excel (納品プラン) に無いため入れられません (職員・本社に確認してください)' };
  return null;
}

/**
 * 割当の取消 (論理)。
 * 入れる数を間違えたときの訂正は現場で完結させる (中原さん 9/3: 「間違って個数を入れて修正する場合に PIN は要らない」)
 * → 端末・経過時間・箱の開閉では縛らない。誰が・いつ・どの端末で取り消したかは fbx_events に残る。
 * 職員として明示的に行う場合 (byStaff) だけ理由を必須にする。
 * ⚠ 閉じた箱の中身を変えると実測重量が合わなくなる → 呼び出し側 (adjustPlacement / 画面) が量り直しを案内する
 */
export function revokePlacement({ placementId, byStaff = false, reason, worker, deviceKey, deviceLabel }) {
  const d = getDB();
  return d.transaction(() => {
    const p = d.prepare(`SELECT p.*, b.status AS box_status, r.status AS run_status FROM fbx_placements p
      JOIN fbx_boxes b ON b.id = p.box_id JOIN fbx_runs r ON r.id = p.run_id WHERE p.id = ?`)
      .get(Number(placementId));
    if (!p) return { ok: false, error: 'not_found', message: '割当が見つかりません' };
    if (p.revoked_at) return { ok: true, already: true };
    if (p.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません (本社が完了にしています)' };
    if (byStaff && !String(reason || '').trim()) {
      return { ok: false, error: 'reason_required', message: '取消の理由を入力してください' };
    }
    // 閉じた箱の中身が変わったら実測重量は合わない → 箱を開けて量り直しにする (Codex R17 #2:
    // 古い重量が Excel に出る事故を防ぐ。開いた箱は出荷前チェックでブロックされる)
    if (p.box_status === 'closed') {
      d.prepare(`UPDATE fbx_boxes SET status = 'open', measured_weight_kg = NULL, closed_at = NULL, closed_by = NULL,
        closed_reason = NULL, cushion_level = NULL, reopen_count = reopen_count + 1,
        est_weight_g_at_close = NULL, est_unknown_qty_at_close = NULL, tare_g_at_close = NULL,
        limit_override_by = NULL, limit_override_at = NULL WHERE id = ?`).run(p.box_id);
      logEvent({ runId: p.run_id, action: 'box_reopen', targetType: 'box', targetId: p.box_id,
        workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
        payload: { reason: '中身の訂正で自動オープン', auto: true, placementId: p.id } }, d);
    }
    d.prepare('UPDATE fbx_placements SET revoked_at = ?, revoked_by = ?, revoke_reason = ? WHERE id = ?')
      .run(utcNow(), worker?.display_name || null, reason ? String(reason).slice(0, 200) : null, p.id);
    d.prepare('UPDATE fbx_boxes SET content_version = content_version + 1 WHERE id = ?').run(p.box_id);
    bumpRunVersion(d, p.run_id);
    // 自動で入った確認担当は、取消のあとに残っている投入に合わせ直す (0件なら消す)。人が選んだ名前は残す
    syncAutoCheckWorker(d, p.row_id, { runId: p.run_id, worker, deviceLabel });
    // 監査: 誰が (worker) どの端末から (deviceLabel) 誰の記録を (originDevice) 取り消したか (Codex R17 #3)
    logEvent({ runId: p.run_id, action: 'placement_revoke', targetType: 'placement', targetId: p.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { rowId: p.row_id, boxId: p.box_id, qty: p.qty, byStaff, reason: reason || null,
        originWorker: p.worker_name || null, originDeviceKey: p.device_key, actorDeviceKey: String(deviceKey ?? ''),
        otherDevice: p.device_key !== String(deviceKey ?? ''), boxReopened: p.box_status === 'closed' } }, d);
    return { ok: true, placed: placedOf(d, p.row_id), boxReopened: p.box_status === 'closed' };
  }).immediate();
}

/**
 * 割当の数の修正 (中原さん 9/3: 入れる数を間違えて押したとき、リストの商品から直せるように。PIN は要らない)。
 * = 元の記録の取消 + 同じ箱・同じ期限・同じ配置で新しい数の記録 を 1 トランザクションで行う (append-only は保つ)。
 * 新しい数 0 = 取消だけ。閉じた箱でも直せる (重さが変わるので画面で量り直しを案内する)
 */
export function adjustPlacement({ placementId, qty, byStaff = false, reason, worker, deviceKey, deviceLabel, requestId }) {
  const q = Number(qty);
  if (!Number.isInteger(q) || q < 0 || q > 100000) return { ok: false, error: 'bad_qty', message: '個数は 0 以上の整数で入力してください' };
  const d = getDB();
  const fail = (r) => { const e = new Error('adjust_failed'); e.result = r; throw e; };
  const reqId = requestId ? String(requestId) : `adj-${Number(placementId)}-${Date.now()}`;
  try {
    return d.transaction(() => {
      const p = d.prepare('SELECT * FROM fbx_placements WHERE id = ?').get(Number(placementId));
      if (!p) return { ok: false, error: 'not_found', message: '記録が見つかりません' };
      // 再送 (通信断でのリトライ) の冪等性 (Codex R17 #1): 同じ端末×request_id で既に入れ直していればその結果を返す。
      // qty=0 (取消だけ) の再送は、元の記録が取消済みなら成功として返す
      const prevAdd = deviceKey ? d.prepare('SELECT * FROM fbx_placements WHERE device_key = ? AND request_id = ?').get(String(deviceKey), reqId) : null;
      if (prevAdd && prevAdd.id !== p.id) {
        return { ok: true, already: true, placementId: prevAdd.id, revokedId: p.id, placed: placedOf(d, prevAdd.row_id), from: p.qty, to: prevAdd.qty };
      }
      if (p.revoked_at) {
        if (q === 0) return { ok: true, already: true, placementId: null, revokedId: p.id, placed: placedOf(d, p.row_id), from: p.qty, to: 0 };
        return { ok: false, error: 'revoked', message: 'この記録は既に取り消されています (画面を更新してください)' };
      }
      if (q === p.qty) return { ok: true, unchanged: true, placementId: p.id, placed: placedOf(d, p.row_id) };
      const rv = revokePlacement({ placementId: p.id, byStaff, reason: reason || (byStaff ? '数の修正' : null), worker, deviceKey, deviceLabel });
      if (!rv.ok) return rv;
      // 閉じた箱だった場合は revokePlacement が箱を開けている (量り直し) → 入れ直しは普通に通る
      const boxReopened = !!rv.boxReopened;
      if (q === 0) return { ok: true, placementId: null, placed: rv.placed, revokedId: p.id, from: p.qty, to: 0, boxReopened };
      const add = addPlacement({ runId: p.run_id, rowId: p.row_id, boxId: p.box_id, qty: q, expiry: p.expiry, layer: p.placement_layer,
        worker, deviceKey, deviceLabel, requestId: reqId });
      if (!add.ok) fail(add);   // 入れ直せない (残数超など) → 取消ごと戻す
      logEvent({ runId: p.run_id, action: 'placement_adjust', targetType: 'placement', targetId: add.placementId,
        workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
        payload: { from: p.qty, to: q, revokedId: p.id, boxId: p.box_id, byStaff, boxReopened,
          originWorker: p.worker_name || null, originDeviceKey: p.device_key, actorDeviceKey: String(deviceKey ?? ''),
          otherDevice: p.device_key !== String(deviceKey ?? '') } }, d);
      return { ok: true, placementId: add.placementId, revokedId: p.id, placed: add.placed, from: p.qty, to: q, boxReopened };
    }).immediate();
  } catch (e) {
    if (e.result) return e.result;
    throw e;
  }
}

/**
 * 割当の配置 (下/中/上) を後から付け替え (読み合わせ画面から。manual のみ)。
 * 閉じた箱の記録の変更は職員のみ (Codex PR1 #8: 読み合わせ後の監査対象データを勝手に変えない)
 */
export function setPlacementLayer({ placementId, layer, byStaff = false, worker, deviceLabel }) {
  const lay = layer == null || layer === '' ? null : String(layer);
  if (lay && !['bottom', 'middle', 'top'].includes(lay)) return { ok: false, error: 'bad_layer', message: '配置の値が不正です' };
  const d = getDB();
  return d.transaction(() => {
    const p = d.prepare(`SELECT p.*, b.status AS box_status, r.status AS run_status FROM fbx_placements p
      JOIN fbx_boxes b ON b.id = p.box_id JOIN fbx_runs r ON r.id = p.run_id WHERE p.id = ?`)
      .get(Number(placementId));
    if (!p) return { ok: false, error: 'not_found', message: '割当が見つかりません' };
    if (p.revoked_at) return { ok: false, error: 'revoked', message: '取消済みの割当です' };
    if (p.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    if (p.box_status === 'closed' && !byStaff) {
      return { ok: false, error: 'staff_required', message: '閉じた箱の記録の変更は職員のみです' };
    }
    d.prepare('UPDATE fbx_placements SET placement_layer = ?, layer_source = ? WHERE id = ?')
      .run(lay, lay ? 'manual' : null, p.id);
    logEvent({ runId: p.run_id, action: 'placement_layer', targetType: 'placement', targetId: p.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { from: p.placement_layer, to: lay } }, d);
    return { ok: true };
  }).immediate();
}

// ───────────────────────── 行の作業メタ ─────────────────────────

export function setRowWorkers({ rowId, labelWorker, checkWorker, worker, deviceLabel }) {
  const d = getDB();
  return d.transaction(() => {
    const row = d.prepare('SELECT w.*, r.status AS run_status FROM fbx_rows w JOIN fbx_runs r ON r.id = w.run_id WHERE w.id = ?')
      .get(Number(rowId));
    if (!row) return { ok: false, error: 'not_found', message: '商品行が見つかりません' };
    if (row.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    const excluded = rowExcludedError(row);
    if (excluded) return excluded;
    const label = labelWorker === undefined ? undefined : (labelWorker ? String(labelWorker).slice(0, 30) : null);
    const check = checkWorker === undefined ? undefined : (checkWorker ? String(checkWorker).slice(0, 30) : null);
    // 人が選んだ名前は source='manual' = 以後 syncAutoCheckWorker が動かさない。「— 消す —」で
    // 空にしたときは source も消す (次に入れた人が自動で入るようになる)
    const checkSource = check === undefined ? null : (check ? 'manual' : null);
    const setCheck = check === undefined ? 0 : 1;
    d.prepare(`INSERT INTO fbx_row_work (row_id, label_worker, check_worker, check_worker_source, check_worker_placement_id, updated_at)
      VALUES (?, ?, ?, ?, NULL, ?)
      ON CONFLICT(row_id) DO UPDATE SET
        label_worker = CASE WHEN ? THEN excluded.label_worker ELSE fbx_row_work.label_worker END,
        check_worker = CASE WHEN ? THEN excluded.check_worker ELSE fbx_row_work.check_worker END,
        check_worker_source = CASE WHEN ? THEN excluded.check_worker_source ELSE fbx_row_work.check_worker_source END,
        check_worker_placement_id = CASE WHEN ? THEN NULL ELSE fbx_row_work.check_worker_placement_id END,
        updated_at = excluded.updated_at`)
      .run(row.id, label === undefined ? null : label, check === undefined ? null : check, checkSource, utcNow(),
        label === undefined ? 0 : 1, setCheck, setCheck, setCheck);
    logEvent({ runId: row.run_id, action: 'row_workers', targetType: 'row', targetId: row.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { labelWorker: label, checkWorker: check } }, d);
    return { ok: true };
  }).immediate();
}

/** 不足確定 (職員のみ。router 側で PIN 確認済みの前提)。shortageQty = 足りなかった個数 */
/**
 * 不足 (= 送る数の修正) の理由。stock_short = 棚の在庫が予定より少ない / not_shipped = 今回は FBA に納品しない
 * (破損・自社出荷の引当など。作業完了時の自動確定にも使う)
 */
export const SHORTAGE_REASONS = ['missing', 'damaged', 'bad_label', 'wrong_item', 'expiry_issue', 'hq_order', 'stock_short', 'not_shipped', 'other'];
export const SHORTAGE_REASON_JA = { missing: '現物がない', damaged: '破損', bad_label: 'ラベル不良', wrong_item: '商品違い', expiry_issue: '期限が合わない',
  hq_order: '本社指示', stock_short: '在庫が少ない', not_shipped: '今回は納品しない', other: 'その他' };

/**
 * 不足の内訳 (理由別) を新しい合計に合わせる (Codex R14 #2)。
 * 増分は「今回は納品しない (not_shipped)」へ、減分は末尾 (= 直近に足した not_shipped) から削る。
 * @returns { reason: 先頭の理由 | null, detail: 2 件以上なら JSON | null }
 */
export function shortageBreakdownFor({ shortage = 0, reason = null, detail = null }, newQty) {
  let entries = safeJson(detail, null);
  if (!Array.isArray(entries) || entries.length === 0) entries = shortage > 0 ? [{ reason: reason || 'other', qty: shortage }] : [];
  entries = entries.map((e) => ({ reason: e.reason || 'other', qty: Number(e.qty) || 0 })).filter((e) => e.qty > 0);
  const cur = entries.reduce((a, e) => a + e.qty, 0);
  const target = Math.max(0, Number(newQty) || 0);
  if (target > cur) {
    const ns = entries.find((e) => e.reason === 'not_shipped');
    if (ns) ns.qty += target - cur; else entries.push({ reason: 'not_shipped', qty: target - cur });
  } else if (target < cur) {
    let cut = cur - target;
    for (let i = entries.length - 1; i >= 0 && cut > 0; i--) {
      const take = Math.min(entries[i].qty, cut);
      entries[i].qty -= take; cut -= take;
    }
    entries = entries.filter((e) => e.qty > 0);
  }
  if (entries.length === 0) return { reason: null, detail: null };
  return { reason: entries[0].reason, detail: entries.length > 1 ? JSON.stringify(entries) : null };
}

/**
 * 送る数の修正 (職員)。「予定 30 だが棚に 25 しかない」→ 送る数 25 = 不足 5 (理由 stock_short) として記録する
 * (中原さん 9/3)。修正前 (予定) → 修正後 (送る数) は fbx_row_work の shortage で復元でき、本社画面に出す。
 * 送る数 = 予定 に戻せば不足は消える。投入済みより少なくはできない
 */
export function setRowSendQty({ rowId, sendQty, reason = 'stock_short', worker, deviceLabel }) {
  const q = Number(sendQty);
  if (!Number.isInteger(q) || q < 0) return { ok: false, error: 'bad_qty', message: '送る数は 0 以上の整数で入力してください' };
  const reasonKey = String(reason || 'stock_short');
  if (!SHORTAGE_REASONS.includes(reasonKey)) return { ok: false, error: 'bad_reason', message: '理由を選んでください' };
  const d = getDB();
  return d.transaction(() => {
    const row = d.prepare(`SELECT w.*, r.status AS run_status,
        COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0) AS placed,
        COALESCE((SELECT rw.shortage_qty FROM fbx_row_work rw WHERE rw.row_id = w.id), 0) AS shortage
      FROM fbx_rows w JOIN fbx_runs r ON r.id = w.run_id WHERE w.id = ?`).get(Number(rowId));
    if (!row) return { ok: false, error: 'not_found', message: '商品行が見つかりません' };
    if (row.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    const excluded = rowExcludedError(row);
    if (excluded) return excluded;
    if (q > row.planned_qty) return { ok: false, error: 'bad_qty', message: `送る数は予定 (${row.planned_qty}) を超えられません (増やすときは本社が STA のプランを直してください)` };
    if (q < row.placed) return { ok: false, error: 'bad_qty', message: `既に ${row.placed} 個入っています。送る数を ${row.placed} より少なくするには先に記録を取り消してください` };
    const before = row.planned_qty - row.shortage;
    const shortage = row.planned_qty - q;
    if (shortage === 0) {
      d.prepare('UPDATE fbx_row_work SET shortage_qty = NULL, shortage_reason = NULL, shortage_detail = NULL, shortage_by = NULL, updated_at = ? WHERE row_id = ?').run(utcNow(), row.id);
    } else {
      d.prepare(`INSERT INTO fbx_row_work (row_id, shortage_qty, shortage_reason, shortage_detail, shortage_by, updated_at) VALUES (?, ?, ?, NULL, ?, ?)
        ON CONFLICT(row_id) DO UPDATE SET shortage_qty = excluded.shortage_qty, shortage_reason = excluded.shortage_reason, shortage_detail = NULL, shortage_by = excluded.shortage_by, updated_at = excluded.updated_at`)
        .run(row.id, shortage, reasonKey, worker?.display_name || null, utcNow());
    }
    bumpRunVersion(d, row.run_id);
    logEvent({ runId: row.run_id, action: 'row_send_qty', targetType: 'row', targetId: row.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { planned: row.planned_qty, from: before, to: q, shortage, reason: shortage ? reasonKey : null } }, d);
    return { ok: true, planned: row.planned_qty, sendQty: q, shortage, from: before };
  }).immediate();
}

export function setRowShortage({ rowId, shortageQty, reason, worker, deviceLabel }) {
  const REASONS = SHORTAGE_REASONS;
  const reasonKey = String(reason || '');
  if (!REASONS.includes(reasonKey)) return { ok: false, error: 'bad_reason', message: '不足の理由を選んでください' };
  const d = getDB();
  return d.transaction(() => {
    const row = d.prepare(`SELECT w.*, r.status AS run_status,
        COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0) AS placed
      FROM fbx_rows w JOIN fbx_runs r ON r.id = w.run_id WHERE w.id = ?`).get(Number(rowId));
    if (!row) return { ok: false, error: 'not_found', message: '商品行が見つかりません' };
    if (row.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    const excluded = rowExcludedError(row);
    if (excluded) return excluded;
    const remaining = row.planned_qty - row.placed;
    const q = Number(shortageQty);
    if (!Number.isInteger(q) || q <= 0 || q > remaining) {
      return { ok: false, error: 'bad_qty', message: `不足数は 1〜${remaining} で入力してください (残数を超えられません)` };
    }
    d.prepare(`INSERT INTO fbx_row_work (row_id, shortage_qty, shortage_reason, shortage_detail, shortage_by, updated_at) VALUES (?, ?, ?, NULL, ?, ?)
      ON CONFLICT(row_id) DO UPDATE SET shortage_qty = excluded.shortage_qty,
        shortage_reason = excluded.shortage_reason, shortage_detail = NULL, shortage_by = excluded.shortage_by, updated_at = excluded.updated_at`)
      .run(row.id, q, reasonKey, worker?.display_name || null, utcNow());
    bumpRunVersion(d, row.run_id);
    logEvent({ runId: row.run_id, action: 'row_shortage', targetType: 'row', targetId: row.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { shortageQty: q, reason: reasonKey } }, d);
    return { ok: true };
  }).immediate();
}

/** 不足確定の解除 (職員) */
export function clearRowShortage({ rowId, worker, deviceLabel }) {
  const d = getDB();
  return d.transaction(() => {
    const row = d.prepare('SELECT w.*, r.status AS run_status FROM fbx_rows w JOIN fbx_runs r ON r.id = w.run_id WHERE w.id = ?')
      .get(Number(rowId));
    if (!row) return { ok: false, error: 'not_found', message: '商品行が見つかりません' };
    if (row.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    const excluded = rowExcludedError(row);
    if (excluded) return excluded;
    d.prepare('UPDATE fbx_row_work SET shortage_qty = NULL, shortage_reason = NULL, shortage_detail = NULL, shortage_by = NULL, updated_at = ? WHERE row_id = ?')
      .run(utcNow(), row.id);
    bumpRunVersion(d, row.run_id);
    logEvent({ runId: row.run_id, action: 'row_shortage_clear', targetType: 'row', targetId: row.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true }, d);
    return { ok: true };
  }).immediate();
}

// ───────────────────────── 作業者 (fbx_workers。iroha-work と同方式) ─────────────────────────

export function listWorkers(includeInactive = false) {
  return getDB().prepare(`SELECT id, display_name, worker_type, active, sort_order,
      (pin_hash IS NOT NULL) AS pin_set
    FROM fbx_workers ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY sort_order, id`).all();
}

export function getWorker(id) {
  const n = Number(id);
  if (!Number.isInteger(n) || n <= 0) return null;
  return getDB().prepare('SELECT id, display_name, worker_type, active FROM fbx_workers WHERE id = ?').get(n) || null;
}

export function addWorker({ displayName, workerType, actor }) {
  const name = String(displayName || '').trim();
  if (!name || name.length > 30) return { ok: false, error: 'bad_name', message: '名前は1〜30文字で入力してください' };
  if (workerType !== 'member' && workerType !== 'staff') return { ok: false, error: 'bad_type', message: '区分は 利用者 / 職員 のどちらかです' };
  const d = getDB();
  const dup = d.prepare('SELECT id FROM fbx_workers WHERE display_name = ? AND active = 1').get(name);
  if (dup) return { ok: false, error: 'duplicate', message: `「${name}」は既に登録されています` };
  const info = d.prepare('INSERT INTO fbx_workers (display_name, worker_type, active, created_at, created_by) VALUES (?, ?, 1, ?, ?)')
    .run(name, workerType, utcNow(), actor || null);
  return { ok: true, id: Number(info.lastInsertRowid) };
}

export function setWorkerActive(id, active) {
  return getDB().prepare('UPDATE fbx_workers SET active = ? WHERE id = ?').run(active ? 1 : 0, Number(id)).changes > 0;
}

/**
 * PIN 設定済みの有効な職員の数。0 のときだけ iPad からの名簿登録を無ゲートで許す (初期登録 = bootstrap)。
 * 端末Cookie 自体が本社発行の6桁コードで守られているので、最初の職員1人はいろはが自分で登録できる
 */
export function countStaffWithPin() {
  return getDB().prepare(`SELECT COUNT(*) c FROM fbx_workers WHERE worker_type = 'staff' AND active = 1 AND pin_hash IS NOT NULL`).get().c;
}

const PIN_MAX_FAILS = 5;
const PIN_LOCK_MS = 10 * 60 * 1000;
const pinHash = (salt, pin) => crypto.scryptSync(String(pin), `fbx-pin:${salt}`, 32).toString('hex');

export function setWorkerPin(id, pin, actor) {
  const p = String(pin || '').trim();
  if (!/^\d{4,8}$/.test(p)) return { ok: false, error: 'bad_pin', message: 'PINは4〜8桁の数字で設定してください' };
  const w = getWorker(id);
  if (!w) return { ok: false, error: 'not_found', message: '作業者が見つかりません' };
  if (w.worker_type !== 'staff') return { ok: false, error: 'not_staff', message: 'PINを設定できるのは職員だけです' };
  const salt = crypto.randomBytes(16).toString('hex');
  const d = getDB();
  d.transaction(() => {
    d.prepare('UPDATE fbx_workers SET pin_hash = ?, pin_salt = ?, pin_fails = 0, pin_lock_until = NULL WHERE id = ?')
      .run(pinHash(salt, p), salt, Number(id));
    // 初回の PIN 設定で名簿の初期登録 (bootstrap) を恒久的に閉じる (Codex PR2 #3: 後で職員が全員無効になっても
    // 端末が自動的に無ゲートへ戻らない。復旧は本社の管理画面から)
    d.prepare(`INSERT INTO fbx_meta (key, value) VALUES ('roster_bootstrap_done', '1') ON CONFLICT(key) DO NOTHING`).run();
  }).immediate();
  safeLogEvent({ action: 'pin_set', workerId: w.id, workerName: w.display_name, deviceLabel: actor || null, ok: true });
  return { ok: true };
}

/**
 * 名簿の初期登録モードか: PIN 持ちの有効な職員が 0 人 かつ 一度も PIN が設定されたことがない。
 * (PIN が一度でも設定されたら fbx_meta の roster_bootstrap_done で閉じる)
 */
export function isRosterBootstrap() {
  if (countStaffWithPin() > 0) return false;
  const done = getDB().prepare(`SELECT value FROM fbx_meta WHERE key = 'roster_bootstrap_done'`).get();
  return !done;
}

export function verifyWorkerPin(id, pin) {
  const d = getDB();
  return d.transaction(() => {
    const row = d.prepare('SELECT id, worker_type, pin_hash, pin_salt, pin_fails, pin_lock_until FROM fbx_workers WHERE id = ?').get(Number(id));
    if (!row || row.worker_type !== 'staff') return { ok: false, error: 'pin_required', message: '職員を選んでください' };
    if (!row.pin_hash) return { ok: false, error: 'pin_required', message: 'この職員にはPINが未設定です (管理画面で設定してください)' };
    if (row.pin_lock_until && Date.parse(row.pin_lock_until) > Date.now()) {
      return { ok: false, error: 'pin_locked', message: 'PINの間違いが続いたため一時的にロックしました。10分ほど待ってください' };
    }
    const p = String(pin || '').trim();
    if (!p || pinHash(row.pin_salt, p) !== row.pin_hash) {
      const fails = (row.pin_fails || 0) + 1;
      const lockUntil = fails >= PIN_MAX_FAILS ? new Date(Date.now() + PIN_LOCK_MS).toISOString() : null;
      d.prepare('UPDATE fbx_workers SET pin_fails = ?, pin_lock_until = COALESCE(?, pin_lock_until) WHERE id = ?')
        .run(lockUntil ? 0 : fails, lockUntil, row.id);
      if (lockUntil) return { ok: false, error: 'pin_locked', message: 'PINの間違いが続いたため一時的にロックしました。10分ほど待ってください' };
      return { ok: false, error: p ? 'pin_invalid' : 'pin_required', message: p ? 'PINが違います' : '職員のPINを入れてください' };
    }
    d.prepare('UPDATE fbx_workers SET pin_fails = 0, pin_lock_until = NULL WHERE id = ?').run(row.id);
    return { ok: true };
  }).immediate();
}

export function _clearPinFails() {
  getDB().prepare('UPDATE fbx_workers SET pin_fails = 0, pin_lock_until = NULL').run();
}

// ───────────────────────── 端末・登録コード (iroha-work と同方式) ─────────────────────────

export function createDevice(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const token = crypto.randomBytes(32).toString('base64url');
  const info = getDB().prepare('INSERT INTO fbx_devices (token_hash, label, created_by, created_at) VALUES (?, ?, ?, ?)')
    .run(hashToken(token), l, actor, utcNow());
  return { token, id: Number(info.lastInsertRowid) };
}

export function verifyDevice(token) {
  if (!token) return null;
  const d = getDB();
  const row = d.prepare('SELECT * FROM fbx_devices WHERE token_hash = ? AND revoked_at IS NULL').get(hashToken(token));
  if (!row) return null;
  const now = utcNow();
  if (Date.parse(now) - Date.parse(row.created_at) > DEVICE_TTL_MS) return null;
  if (!row.last_seen_at || Date.parse(now) - Date.parse(row.last_seen_at) > 3600_000) {
    d.prepare('UPDATE fbx_devices SET last_seen_at = ? WHERE id = ?').run(now, row.id);
  }
  return row;
}

export function revokeDevice(id) {
  return getDB().prepare('UPDATE fbx_devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL')
    .run(utcNow(), Number(id)).changes > 0;
}

export function listDevices() {
  return getDB().prepare('SELECT id, label, created_by, created_at, last_seen_at, revoked_at FROM fbx_devices ORDER BY id').all();
}

export const ENROLL_TTL_MS = 10 * 60 * 1000;
export const ENROLL_MAX_ATTEMPTS = 5;
export const ENROLL_RATE_WINDOW_MS = 10 * 60 * 1000;
export const ENROLL_RATE_PER_IP = 8;
export const ENROLL_RATE_GLOBAL = 40;

export function createEnrollCode(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const d = getDB();
  const now = new Date();
  const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
  d.transaction(() => {
    const revokedAt = new Date(now.getTime() - 1000).toISOString();
    d.prepare('UPDATE fbx_enroll_codes SET expires_at = ? WHERE used_at IS NULL AND expires_at > ?')
      .run(revokedAt, now.toISOString());
    d.prepare('INSERT INTO fbx_enroll_codes (code_hash, label, created_by, created_at, expires_at) VALUES (?, ?, ?, ?, ?)')
      .run(enrollHash(code), l, actor, now.toISOString(), new Date(now.getTime() + ENROLL_TTL_MS).toISOString());
  }).immediate();
  return { code, label: l, expiresAt: new Date(now.getTime() + ENROLL_TTL_MS).toISOString() };
}

export function redeemEnrollCode(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return { ok: false, error: 'bad_code', message: '6桁の数字を入力してください' };
  const d = getDB();
  return d.transaction(() => {
    const row = d.prepare('SELECT * FROM fbx_enroll_codes WHERE code_hash = ?').get(enrollHash(c));
    if (!row) return { ok: false, error: 'bad_code', message: '登録コードが違います' };
    if (row.attempts >= ENROLL_MAX_ATTEMPTS) return { ok: false, error: 'too_many', message: 'このコードは無効です (発行し直してください)' };
    if (row.used_at) return { ok: false, error: 'used', message: 'この登録コードは使用済みです (発行し直してください)' };
    if (Date.parse(row.expires_at) < Date.now()) return { ok: false, error: 'expired', message: '登録コードの有効期限が切れています (発行し直してください)' };
    const { token, id } = createDevice(row.label, `enroll:${row.created_by}`);
    d.prepare('UPDATE fbx_enroll_codes SET used_at = ?, used_device_id = ? WHERE id = ?').run(utcNow(), id, row.id);
    return { ok: true, token, label: row.label };
  }).immediate();
}

export function recordEnrollAttempt({ ip = null, ok = false } = {}) {
  const d = getDB();
  d.prepare('INSERT INTO fbx_enroll_attempts (ip, at, ok) VALUES (?, ?, ?)')
    .run(ip ? String(ip).slice(0, 64) : null, utcNow(), ok ? 1 : 0);
  d.prepare('DELETE FROM fbx_enroll_attempts WHERE at < ?')
    .run(new Date(Date.now() - 24 * 3600 * 1000).toISOString());
}

export function checkEnrollRate({ ip = null } = {}) {
  const d = getDB();
  const since = new Date(Date.now() - ENROLL_RATE_WINDOW_MS).toISOString();
  const mine = ip ? d.prepare('SELECT COUNT(*) c FROM fbx_enroll_attempts WHERE ip = ? AND ok = 0 AND at > ?')
    .get(String(ip).slice(0, 64), since).c : 0;
  if (mine >= ENROLL_RATE_PER_IP) {
    return { allowed: false, error: 'rate_limited', message: '試行が多すぎます。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  const all = d.prepare('SELECT COUNT(*) c FROM fbx_enroll_attempts WHERE ok = 0 AND at > ?').get(since).c;
  if (all >= ENROLL_RATE_GLOBAL) {
    return { allowed: false, error: 'rate_limited', message: '登録の受付を一時的に停止しています。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  return { allowed: true };
}

export function countEnrollAttempt(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return;
  getDB().prepare('UPDATE fbx_enroll_codes SET attempts = attempts + 1 WHERE code_hash = ?').run(enrollHash(c));
}

export function listActiveEnrollCodes() {
  return getDB().prepare(`SELECT id, label, created_by, created_at, expires_at, attempts
    FROM fbx_enroll_codes WHERE used_at IS NULL AND expires_at > ? AND attempts < ? ORDER BY id DESC`)
    .all(utcNow(), ENROLL_MAX_ATTEMPTS);
}

export function listMaterials(includeInactive = false) {
  return getDB().prepare(`SELECT * FROM fbx_box_materials ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY sort, code`).all();
}

/** 資材の追加・編集 (管理者)。外寸 cm は Excel の幅・長さ・高さ欄に入る (未設定なら空欄 = STA 画面で入力) */
export function upsertMaterial({ code, name, tareG, widthCm, lengthCm, heightCm, sort, active, actor }) {
  const c = String(code || '').trim().toLowerCase();
  if (!/^[a-z0-9_-]{1,20}$/.test(c)) return { ok: false, error: 'bad_code', message: 'コードは英数字・-・_ で1〜20文字' };
  const n = String(name || '').trim();
  if (!n || n.length > 30) return { ok: false, error: 'bad_name', message: '名前は1〜30文字で入力してください' };
  const num = (v, label, { integer = false, max = 100000 } = {}) => {
    if (v == null || v === '') return null;
    const x = Number(v);
    if (!Number.isFinite(x) || x < 0 || x > max || (integer && !Number.isInteger(x))) throw new Error(`${label} の値が不正です`);
    return x;
  };
  let vals;
  try {
    // 外寸は 0.1cm 単位に丸めて保存 (Excel へはそのまま書く — 書き込み側で丸めない)
    const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);
    vals = { tare: num(tareG, '自重g', { integer: true }), w: r1(num(widthCm, '幅cm', { max: 500 })), l: r1(num(lengthCm, '長さcm', { max: 500 })), h: r1(num(heightCm, '高さcm', { max: 500 })) };
  } catch (e) {
    return { ok: false, error: 'bad_number', message: e.message };
  }
  const d = getDB();
  return d.transaction(() => {
    const old = d.prepare('SELECT * FROM fbx_box_materials WHERE code = ?').get(c) || null;
    d.prepare(`INSERT INTO fbx_box_materials (code, name, tare_g, width_cm, length_cm, height_cm, sort, active)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(code) DO UPDATE SET name = excluded.name, tare_g = excluded.tare_g, width_cm = excluded.width_cm,
        length_cm = excluded.length_cm, height_cm = excluded.height_cm, sort = excluded.sort, active = excluded.active`)
      .run(c, n, vals.tare, vals.w, vals.l, vals.h, Number.isInteger(Number(sort)) ? Number(sort) : 0, active === false ? 0 : 1);
    // 外寸は Excel の寸法欄に出る = 変更したらこの資材の箱を持つ未アップの納品回の版を進める (Codex PR2 #1)。
    // そうしないと外寸変更前の出力が「最新」のまま STA アップ済みにできてしまう
    let bumpedRuns = [];
    const dimsChanged = !!old && (old.width_cm !== vals.w || old.length_cm !== vals.l || old.height_cm !== vals.h);
    if (dimsChanged) {
      bumpedRuns = d.prepare(`SELECT DISTINCT g.run_id FROM fbx_boxes b JOIN fbx_pack_groups g ON g.id = b.pack_group_id
        JOIN fbx_runs r ON r.id = g.run_id
        WHERE b.material_code = ? AND b.status != 'void' AND r.status IN ('active','done') AND r.sta_uploaded_at IS NULL`).all(c).map((x) => x.run_id);
      for (const runId of bumpedRuns) {
        bumpRunVersion(d, runId);
        logEvent({ runId, action: 'material_dims_changed', targetType: 'run', targetId: runId, deviceLabel: actor || null, ok: true,
          payload: { code: c, from: { w: old.width_cm, l: old.length_cm, h: old.height_cm }, to: { w: vals.w, l: vals.l, h: vals.h } } }, d);
      }
    }
    logEvent({ action: 'material_upsert', deviceLabel: actor || null, ok: true, payload: { code: c, name: n, ...vals, active: active !== false, bumpedRuns } }, d);
    return { ok: true, code: c, bumpedRuns };
  }).immediate();
}

// ───────────────────────── Excel 出力 (PR2, 要件 F-6 / F-7 / F-7b) ─────────────────────────

/**
 * 出荷前チェックリスト (F-6)。blockers があれば Excel 出力不可、warnings は出力可だが本社が確認する。
 * 併せて箱↔Amazon箱番号の対応・期限一覧 (F-7b: Excel には書かず STA 画面へ転記する) を返す
 */
export function exportReadiness(runId) {
  const st = getRunState(runId);
  if (!st) return null;
  const { run, groups, rows, boxes, placements, exportState } = st;
  const blockers = [], warnings = [];
  const boxBrief = (b) => ({ id: b.id, code: b.box_code, boxNo: b.box_no, amazonBoxNo: b.amazon_box_no, amazonName: b.amazon_name, qty: b.total_qty, status: b.status });
  const detailJa = (s) => { const arr = safeJson(s, null); return Array.isArray(arr) ? arr.map((x) => `${SHORTAGE_REASON_JA[x.reason] || x.reason} ${x.qty}`).join(' + ') : null; };
  const rowBrief = (r) => ({ id: r.id, fnsku: r.fnsku, sku: r.seller_sku, name: r.product_name, planNo: r.plan_no, planned: r.planned_qty, placed: r.placed,
    shortage: r.shortage_qty || 0, sendQty: r.planned_qty - (r.shortage_qty || 0), reason: r.shortage_reason || null,
    reasonJa: r.shortage_reason ? (detailJa(r.shortage_detail) || SHORTAGE_REASON_JA[r.shortage_reason] || r.shortage_reason) : null });

  if (run.status !== 'active' && run.status !== 'done') {
    blockers.push({ code: 'run_status', message: `納品回が「${run.status}」のため出力できません` });
  }
  const noExcel = groups.filter((g) => !g.excel_file_id);
  if (noExcel.length > 0) {
    blockers.push({ code: 'no_excel', message: `STA のパックリスト Excel が未添付のグループがあります: ${noExcel.map((g) => g.sheet_name).join(' / ')} (本社が管理画面で添付してください。箱詰め作業自体は続けられます)` });
  }
  const pickingOnlyPlaced = rows.filter((r) => r.match_state === 'picking_only' && r.placed > 0);
  if (pickingOnlyPlaced.length > 0) {
    blockers.push({ code: 'picking_only_placed', message: `添付した Excel に無い商品が箱に入っています (${pickingOnlyPlaced.length} 行)。STA のプランを確認するか、記録を取り消してください`, rows: pickingOnlyPlaced.map(rowBrief) });
  }
  const live = boxes.filter((b) => b.status !== 'void');
  if (live.length === 0) blockers.push({ code: 'no_boxes', message: '箱がひとつもありません' });
  // picking_only / retired (Excel に無い = プランから外れた) 行は完了判定から外す
  const incomplete = rows.filter((r) => !EXCLUDED_ROW_STATES.includes(r.match_state) && r.placed + (r.shortage_qty || 0) !== r.planned_qty);
  if (incomplete.length > 0) {
    blockers.push({ code: 'rows_incomplete', message: `投入数+不足 が予定数と合わない商品が ${incomplete.length} 行あります (iPad で入力を終えるか、職員が不足を確定してください)`, rows: incomplete.map(rowBrief) });
  }
  const openBoxes = live.filter((b) => b.status === 'open' && b.total_qty > 0);
  if (openBoxes.length > 0) blockers.push({ code: 'open_boxes', message: `閉じていない箱が ${openBoxes.length} 箱あります (読み合わせ→実測重量→閉じる)`, boxes: openBoxes.map(boxBrief) });
  const emptyBoxes = live.filter((b) => b.total_qty === 0);
  if (emptyBoxes.length > 0) blockers.push({ code: 'empty_boxes', message: `空の箱が ${emptyBoxes.length} 箱あります (使わない箱は職員が iPad で「箱を取消」してください)`, boxes: emptyBoxes.map(boxBrief) });
  const noWeight = live.filter((b) => b.status === 'closed' && !(Number(b.measured_weight_kg) > 0));
  if (noWeight.length > 0) blockers.push({ code: 'no_weight', message: `実測重量のない箱が ${noWeight.length} 箱あります`, boxes: noWeight.map(boxBrief) });
  for (const g of groups) {
    const n = live.filter((b) => b.pack_group_id === g.id).length;
    if (g.max_box_columns && n > g.max_box_columns) {
      blockers.push({ code: 'box_overflow', message: `${g.sheet_name}: 箱数 ${n} がテンプレの上限 ${g.max_box_columns} を超えています (STA で箱数を増やして再DL→差し替え)` });
    }
  }

  const unchecked = rows.filter((r) => !r.check_worker);
  if (unchecked.length > 0) warnings.push({ code: 'unchecked_rows', message: `確認担当が未記録の商品が ${unchecked.length} 行あります`, rows: unchecked.map(rowBrief) });
  const shortages = rows.filter((r) => r.shortage_qty > 0);
  if (shortages.length > 0) warnings.push({ code: 'shortage_rows', message: `送る数を予定から修正した商品が ${shortages.length} 行あります (修正前 → 修正後と理由を確認。Excel の数量は入れた分だけ。STA 側の予定数量との差は Amazon 側で調整)`, rows: shortages.map(rowBrief) });
  const matchWarn = rows.filter((r) => ['qty_mismatch', 'excel_only', 'picking_only'].includes(r.match_state));
  if (matchWarn.length > 0) warnings.push({ code: 'match_warnings', message: `Excel との突合で注意のあった商品が ${matchWarn.length} 行あります (qty_mismatch=数量差 / excel_only=Excel にだけある / picking_only=Excel に無い)`, rows: matchWarn.map((r) => ({ ...rowBrief(r), matchState: r.match_state })) });
  const gaps = live.filter((b) => b.amazon_box_no !== b.box_no);
  if (gaps.length > 0) warnings.push({ code: 'box_gap', message: `取消した箱があるため、箱札の番号と Amazon の箱番号がずれます (${gaps.length} 箱)。箱ラベルを貼るときは対応表を見てください`, boxes: gaps.map(boxBrief) });
  const mats = new Map(listMaterials(true).map((m) => [m.code, m]));
  const noDims = live.filter((b) => { const m = mats.get(b.material_code); return !(m && m.width_cm > 0 && m.length_cm > 0 && m.height_cm > 0); });
  if (noDims.length > 0) warnings.push({ code: 'no_dims', message: `外寸が未設定の資材の箱が ${noDims.length} 箱あります (Excel の幅・長さ・高さは空欄 → STA 画面で入力。管理画面の資材で外寸を登録すると次回から自動)`, boxes: noDims.map(boxBrief) });
  // PR3: 上限超えは職員の承認でしか閉じられないが、通ってしまった箱は本社にも見えるようにする
  const wl = runWeightLimits(run);
  const overLimit = live.filter((b) => b.status === 'closed' && Number(b.measured_weight_kg) * 1000 > wl.limitG);
  if (overLimit.length > 0) {
    warnings.push({ code: 'over_weight_limit', message: `1箱の上限 ${wl.limitG / 1000}kg を超えた箱が ${overLimit.length} 箱あります (職員の承認で閉じた箱)。Amazon 側で受入不可・追加料金になることがあります`,
      boxes: overLimit.map((b) => ({ ...boxBrief(b), weightKg: b.measured_weight_kg, approvedBy: b.limit_override_by || null })) });
  }
  if (exportState.latest) {
    warnings.push(exportState.stale
      ? { code: 'stale_export', message: `前回の出力 (#${exportState.latest.id}, ${exportState.latest.created_at.slice(0, 16).replace('T', ' ')}) の後にデータが変わっています。再出力してください` }
      : { code: 'exported', message: `出力済み (#${exportState.latest.id})。データは変わっていません` });
  }
  // ファイル単位の STA アップ状況 (複数 Excel のとき、どれがまだかを出す)
  const fileStates = exportFileStates(run.id, run.data_version);
  if (run.sta_uploaded_at) warnings.push({ code: 'sta_uploaded', message: `STA アップ済み (${run.sta_uploaded_at.slice(0, 16).replace('T', ' ')})。再出力は原則不要です` });
  else if (fileStates.some((f) => f.uploaded)) {
    warnings.push({ code: 'sta_partial', message: `STA アップ済み ${fileStates.filter((f) => f.uploaded).length} / ${fileStates.length} ファイル。残り: ${fileStates.filter((f) => !f.uploaded).map((f) => f.fileName).join(', ')}` });
  }

  // 期限一覧 (F-7b): 行×期限で集約 (同一行は1期限がルール)
  const rowById = new Map(rows.map((r) => [r.id, r]));
  const expMap = new Map();
  for (const p of placements) {
    if (!p.expiry) continue;
    const key = `${p.row_id}|${p.expiry}`;
    if (!expMap.has(key)) { const r = rowById.get(p.row_id); expMap.set(key, { rowId: p.row_id, fnsku: r?.fnsku, sku: r?.seller_sku, name: r?.product_name, expiry: p.expiry, qty: 0 }); }
    expMap.get(key).qty += p.qty;
  }
  const expiries = [...expMap.values()].sort((a, b) => (a.fnsku || '').localeCompare(b.fnsku || ''));

  const groupsOut = groups.map((g) => ({
    id: g.id, sheetName: g.sheet_name, displayName: g.display_name, packingGroupId: g.packing_group_id,
    maxBoxColumns: g.max_box_columns, excelFileId: g.excel_file_id, excelAttached: !!g.excel_file_id,
    excelSheetName: g.excel_sheet_name || null,
    rowCounts: rows.filter((r) => r.pack_group_id === g.id).reduce((a, r) => { a[r.match_state] = (a[r.match_state] || 0) + 1; return a; }, {}),
    boxes: live.filter((b) => b.pack_group_id === g.id).map((b) => ({ ...boxBrief(b), weightKg: b.measured_weight_kg, material: b.material_code,
      dims: (() => { const m = mats.get(b.material_code); return m ? { width: m.width_cm, length: m.length_cm, height: m.height_cm } : null; })() })),
  }));
  return { ok: blockers.length === 0, blockers, warnings, groups: groupsOut, expiries, exportState, excelFiles: st.excelFiles, fileStates,
    run: { id: run.id, title: run.title, status: run.status, dataVersion: run.data_version, staUploadedAt: run.sta_uploaded_at || null } };
}

/**
 * 納品回に現在付いている Excel ファイル (グループが参照中) ごとの、現行データ版での出力・STA アップ状況。
 * 納品回を done にするのは全ファイルが現行版でアップ済みになったとき (Codex PR2.5 #1)
 */
function exportFileStates(runId, dataVersion, d = getDB()) {
  const files = d.prepare(`SELECT DISTINCT f.id, f.original_name FROM fbx_pack_groups g JOIN fbx_excel_files f ON f.id = g.excel_file_id
    WHERE g.run_id = ? ORDER BY f.id`).all(runId);
  return files.map((f) => {
    const latest = d.prepare(`SELECT id, data_version, sta_uploaded_at FROM fbx_exports WHERE run_id = ? AND excel_file_id = ? ORDER BY id DESC LIMIT 1`).get(runId, f.id) || null;
    const uploaded = d.prepare(`SELECT id FROM fbx_exports WHERE run_id = ? AND excel_file_id = ? AND data_version = ? AND sta_uploaded_at IS NOT NULL LIMIT 1`).get(runId, f.id, dataVersion) || null;
    return { fileId: f.id, fileName: f.original_name, latestExportId: latest?.id || null, latestStale: !!(latest && latest.data_version < dataVersion), uploaded: !!uploaded, uploadedExportId: uploaded?.id || null };
  });
}

/**
 * Excel 書き込み指示 (write_packlist.py の sheets) と、版として保存するスナップショットを組み立てる。
 * 書くセル = 箱数 (M3 相当) / SKU行×Amazon箱番号列の投入数 (0 は空欄のまま) / 箱ごとの実測kg・外寸。
 * 期限は書かない (JP テンプレ v1.1 に列が無い — 要件 §4)。
 * PR2.5: 1 納品回に複数の Excel (P1/P2 で別ファイル) があり得るので、添付ファイルごとに 1 出力 (exports[])
 */
export function buildExportPayload(runId) {
  const ready = exportReadiness(runId);
  if (!ready) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
  if (!ready.ok) return { ok: false, error: 'not_ready', message: '出荷前チェックに未解決の項目があります', readiness: ready };
  const d = getDB();
  const st = getRunState(runId);
  const qtyByRowBox = new Map();
  for (const p of st.placements) {
    const k = `${p.row_id}|${p.box_id}`;
    qtyByRowBox.set(k, (qtyByRowBox.get(k) || 0) + p.qty);
  }
  const files = new Map();   // excel_file_id → { excelFile, sheets, groups }
  for (const g of st.groups) {
    if (!g.excel_file_id) return { ok: false, error: 'no_excel', message: `${g.sheet_name}: Excel が未添付です` };
    const structure = safeJson(g.structure_json);
    if (!structure?.boxColumns || !structure?.totalBoxes || !structure?.dimRows) {
      return { ok: false, error: 'no_structure', message: `${g.sheet_name}: テンプレ構造の記録がありません (Excel を添付し直してください)` };
    }
    let f = files.get(g.excel_file_id);
    if (!f) {
      const excelFile = d.prepare('SELECT * FROM fbx_excel_files WHERE id = ?').get(g.excel_file_id);
      if (!excelFile) return { ok: false, error: 'no_excel', message: `${g.sheet_name}: 原本Excelの記録がありません` };
      f = { excelFile, sheets: [], groups: [] };
      files.set(g.excel_file_id, f);
    }
    const sheets = f.sheets;
    const snapshotGroups = f.groups;
    const gBoxes = st.boxes.filter((b) => b.pack_group_id === g.id && b.status !== 'void').sort((a, b) => a.amazon_box_no - b.amazon_box_no);
    // Excel に行がある商品だけ (picking_only / retired = Excel に無い行は excel_row が NULL → 出力対象外。投入があれば readiness で止まる)
    const gRows = st.rows.filter((r) => r.pack_group_id === g.id && r.excel_row != null && !EXCLUDED_ROW_STATES.includes(r.match_state));
    const cells = [{ row: structure.totalBoxes.row, col: structure.totalBoxes.col, value: gBoxes.length, kind: 'total_boxes' }];
    const rg = ready.groups.find((x) => x.id === g.id);
    for (const b of gBoxes) {
      const col = structure.boxColumns[String(b.amazon_box_no)];
      if (!col) return { ok: false, error: 'box_overflow', message: `${g.sheet_name}: 箱 ${b.box_code} の列がテンプレにありません` };
      for (const r of gRows) {
        const q = qtyByRowBox.get(`${r.id}|${b.id}`) || 0;
        if (q > 0) cells.push({ row: r.excel_row, col, value: q, kind: 'qty' });
      }
      cells.push({ row: structure.dimRows.weight, col, value: Number(b.measured_weight_kg), kind: 'weight' });
      const dims = rg?.boxes.find((x) => x.id === b.id)?.dims;
      if (dims && dims.width > 0 && dims.length > 0 && dims.height > 0) {
        cells.push({ row: structure.dimRows.width, col, value: dims.width, kind: 'width' });
        cells.push({ row: structure.dimRows.length, col, value: dims.length, kind: 'length' });
        cells.push({ row: structure.dimRows.height, col, value: dims.height, kind: 'height' });
      }
    }
    // 書かない入力セル (数量 0・未使用の箱列・寸法未設定) は明示的に空にする (Codex PR2 #2 の二重防御。
    // 取込時に記入済みテンプレを拒否しているので通常は既に空だが、原本に値が残っていても混入させない)
    const writtenKeys = new Set(cells.map((c) => `${c.row}|${c.col}`));
    const allCols = Object.values(structure.boxColumns);
    const targetRows = [...gRows.map((r) => r.excel_row), ...Object.values(structure.dimRows)];
    for (const row of targetRows) {
      for (const col of allCols) {
        if (!writtenKeys.has(`${row}|${col}`)) cells.push({ row, col, value: null, kind: 'clear' });
      }
    }
    sheets.push({ sheetName: g.excel_sheet_name || g.sheet_name, cells });
    snapshotGroups.push({
      groupId: g.id, sheetName: g.sheet_name, excelSheetName: g.excel_sheet_name || g.sheet_name, displayName: g.display_name, packingGroupId: g.packing_group_id, totalBoxes: gBoxes.length,
      boxes: gBoxes.map((b) => ({
        boxId: b.id, boxCode: b.box_code, boxNo: b.box_no, amazonBoxNo: b.amazon_box_no, amazonName: b.amazon_name,
        weightKg: b.measured_weight_kg, material: b.material_code, dims: rg?.boxes.find((x) => x.id === b.id)?.dims || null,
        contents: gRows.map((r) => ({ rowId: r.id, fnsku: r.fnsku, sku: r.seller_sku, qty: qtyByRowBox.get(`${r.id}|${b.id}`) || 0 })).filter((x) => x.qty > 0),
      })),
    });
  }
  const exports = [...files.values()].map((f) => ({
    excelFile: f.excelFile, sheets: f.sheets,
    snapshot: { dataVersion: st.run.data_version, excelFileId: f.excelFile.id, groups: f.groups, expiries: ready.expiries, warnings: ready.warnings.map((w) => w.code) },
  }));
  return { ok: true, exports, dataVersion: st.run.data_version };
}

/**
 * 出力結果の記録 (版)。複数ファイルは 1 トランザクションでまとめて登録 (Codex PR2.5 #2: 半端な版を残さない)。
 * data_version は buildExportPayload 時点の値。全ファイル同一版が前提 (違えば登録しない)。
 * 書き込み中に iPad で変更が入っていれば stale=true (次の出力で「旧版」として出る)
 */
export function recordExportBatch({ runId, items, createdBy }) {
  const list = Array.isArray(items) ? items : [];
  if (list.length === 0) return { ok: false, error: 'bad_request', message: '記録する出力がありません' };
  const versions = new Set(list.map((x) => Number(x.dataVersion)));
  if (versions.size !== 1) return { ok: false, error: 'version_mismatch', message: '出力ファイル間でデータ版が違います。もう一度出力してください' };
  const dataVersion = [...versions][0];
  const d = getDB();
  return d.transaction(() => {
    const run = d.prepare('SELECT id, data_version FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    const ins = d.prepare(`INSERT INTO fbx_exports (run_id, excel_file_id, data_version, file_name, stored_path, sha256, snapshot_json, verify_json, created_by, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const now = utcNow();
    const exportIds = [];
    for (const x of list) {
      const info = ins.run(run.id, Number(x.excelFileId), dataVersion, x.fileName, x.storedPath, x.sha256, JSON.stringify(x.snapshot), x.verify == null ? null : JSON.stringify(x.verify), createdBy || null, now);
      const exportId = Number(info.lastInsertRowid);
      exportIds.push(exportId);
      logEvent({ runId: run.id, action: 'excel_export', targetType: 'export', targetId: exportId, deviceLabel: createdBy, ok: true,
        payload: { dataVersion, sha256: x.sha256, excelFileId: Number(x.excelFileId), changedDuringWrite: run.data_version !== dataVersion } }, d);
    }
    return { ok: true, exportIds, exportId: exportIds[0], stale: run.data_version !== dataVersion };
  }).immediate();
}

/** 1 ファイルの記録 (recordExportBatch の薄い包み) */
export function recordExport({ runId, excelFileId, dataVersion, fileName, storedPath, sha256, snapshot, verify, createdBy }) {
  return recordExportBatch({ runId, createdBy, items: [{ excelFileId, dataVersion, fileName, storedPath, sha256, snapshot, verify }] });
}

export function listExports(runId) {
  return getDB().prepare(`SELECT e.id, e.run_id, e.excel_file_id, e.data_version, e.file_name, e.sha256, e.created_by, e.created_at,
      (e.data_version < r.data_version) AS stale, (e.sta_uploaded_at IS NOT NULL) AS sta_uploaded, e.sta_uploaded_at
    FROM fbx_exports e JOIN fbx_runs r ON r.id = e.run_id WHERE e.run_id = ? ORDER BY e.id DESC`).all(Number(runId));
}

export function getExport(id) {
  const row = getDB().prepare('SELECT * FROM fbx_exports WHERE id = ?').get(Number(id));
  if (!row) return null;
  return { ...row, snapshot: safeJson(row.snapshot_json), verify: safeJson(row.verify_json) };
}

/**
 * 「STA にアップした」の記録 = 出力 (版) 単位。指定した版が最新データと一致していることが条件 (旧版のアップは事故)。
 * 同じファイルの現行版が既に記録済みなら別の出力は拒否 (Codex PR2 #4: 監査証跡を守る)。同じ出力の再記録は冪等。
 * 納品回に付いている全ファイルが現行版でアップ済みになったとき、納品回を done にして iPad からの変更を止める
 * (Codex PR2.5 #1: 複数 Excel のうち 1 つで全体を完了にしない)
 */
export function markStaUploaded({ runId, exportId, actor }) {
  const d = getDB();
  return d.transaction(() => {
    const run = d.prepare('SELECT * FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    const ex = d.prepare('SELECT * FROM fbx_exports WHERE id = ? AND run_id = ?').get(Number(exportId), run.id);
    if (!ex) return { ok: false, error: 'not_found', message: '出力の記録が見つかりません' };
    if (ex.sta_uploaded_at) return { ok: true, already: true, runDone: !!run.sta_uploaded_at, files: exportFileStates(run.id, run.data_version, d) };
    if (run.sta_uploaded_at) return { ok: false, error: 'already_uploaded', message: 'この納品回は既に STA アップ済み (完了) です' };
    if (ex.data_version !== run.data_version) {
      return { ok: false, error: 'stale_export', message: 'この出力の後にデータが変わっています。最新データで再出力し、そのファイルを STA にアップしてから記録してください' };
    }
    const other = d.prepare(`SELECT id FROM fbx_exports WHERE run_id = ? AND excel_file_id = ? AND data_version = ? AND sta_uploaded_at IS NOT NULL AND id != ?`)
      .get(run.id, ex.excel_file_id, run.data_version, ex.id);
    if (other) {
      return { ok: false, error: 'already_uploaded', message: `このファイルは既に出力 #${other.id} を STA アップ済みとして記録しています。別の版を記録するには本社で確認のうえ中原さんに連絡してください` };
    }
    if (run.status !== 'active' && run.status !== 'done') return { ok: false, error: 'bad_status', message: `この納品回は ${run.status} です` };
    const now = utcNow();
    d.prepare('UPDATE fbx_exports SET sta_uploaded_at = ?, sta_uploaded_by = ? WHERE id = ?').run(now, actor || null, ex.id);
    logEvent({ runId: run.id, action: 'export_sta_uploaded', targetType: 'export', targetId: ex.id, deviceLabel: actor, ok: true, payload: { dataVersion: ex.data_version, excelFileId: ex.excel_file_id } }, d);
    const files = exportFileStates(run.id, run.data_version, d);
    const allUploaded = files.length > 0 && files.every((f) => f.uploaded);
    if (allUploaded) {
      d.prepare(`UPDATE fbx_runs SET sta_uploaded_at = ?, sta_export_id = ?, status = 'done', done_at = COALESCE(done_at, ?) WHERE id = ?`)
        .run(now, ex.id, now, run.id);
      logEvent({ runId: run.id, action: 'run_sta_uploaded', targetType: 'run', targetId: run.id, deviceLabel: actor, ok: true, payload: { dataVersion: ex.data_version, files: files.length } }, d);
    }
    return { ok: true, runDone: allUploaded, files, remaining: files.filter((f) => !f.uploaded).map((f) => f.fileName) };
  }).immediate();
}
