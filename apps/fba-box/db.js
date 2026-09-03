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

const utcNow = () => new Date().toISOString();
const hashToken = (t) => crypto.createHash('sha256').update(String(t)).digest('hex');
const enrollHash = (c) => crypto.createHash('sha256').update('fbx-enroll:' + String(c)).digest('hex');

export const DEVICE_TTL_MS = 400 * 24 * 3600 * 1000;
/** 利用者が自分の割当を取り消せる猶予 (それを過ぎたら職員PIN) */
export const SELF_UNDO_MS = 10 * 60 * 1000;

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
      voided_at     TEXT,
      voided_by     TEXT,
      void_reason   TEXT,
      created_by    TEXT,
      created_at    TEXT NOT NULL,
      UNIQUE(pack_group_id, box_no)
    );`;

/**
 * PR1 で作られた fbx_boxes (status CHECK が open/closed のみ) を void 対応に再構築する。
 * SQLite は CHECK を ALTER できないので公式手順 (新表→コピー→DROP→RENAME) を
 * foreign_keys OFF の下で1トランザクションで行い、最後に foreign_key_check。冪等
 */
function migrateBoxesVoid(d) {
  const sql = d.prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'fbx_boxes'`).get()?.sql || '';
  if (sql.includes("'void'")) return;
  const fkWasOn = d.pragma('foreign_keys', { simple: true }) === 1;
  if (fkWasOn) d.pragma('foreign_keys = OFF');
  try {
    d.transaction(() => {
      d.exec(BOXES_DDL('fbx_boxes_new'));
      d.exec(`INSERT INTO fbx_boxes_new (id, pack_group_id, box_no, box_code, material_code, status, measured_weight_kg,
                closed_at, closed_by, closed_reason, cushion_level, reopen_count, created_by, created_at)
              SELECT id, pack_group_id, box_no, box_code, material_code, status, measured_weight_kg,
                closed_at, closed_by, closed_reason, cushion_level, reopen_count, created_by, created_at FROM fbx_boxes`);
      d.exec('DROP TABLE fbx_boxes');
      d.exec('ALTER TABLE fbx_boxes_new RENAME TO fbx_boxes');
      const bad = d.prepare('PRAGMA foreign_key_check').all();
      if (bad.length > 0) throw new Error(`[fba-box] fbx_boxes 移行後の foreign_key_check に失敗: ${JSON.stringify(bad.slice(0, 3))}`);
    })();
    console.log('[fba-box] fbx_boxes を void 対応スキーマへ移行しました');
  } finally {
    if (fkWasOn) d.pragma('foreign_keys = ON');
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

    -- Amazon 梱包グループ (= パックリストのシート)。箱の親 (Codex R1 B3)
    CREATE TABLE IF NOT EXISTS fbx_pack_groups (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id           INTEGER NOT NULL REFERENCES fbx_runs(id),
      excel_file_id    INTEGER NOT NULL REFERENCES fbx_excel_files(id),
      sheet_name       TEXT NOT NULL,
      packing_group_id TEXT NOT NULL,
      display_name     TEXT NOT NULL,
      box_count_hint   INTEGER,
      max_box_columns  INTEGER,
      structure_json   TEXT,
      UNIQUE(run_id, packing_group_id)
    );

    -- 商品行 (Excel の SKU 行が正本。picking-prep 側は plan_no と表示名の補強)
    CREATE TABLE IF NOT EXISTS fbx_rows (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id         INTEGER NOT NULL REFERENCES fbx_runs(id),
      pack_group_id  INTEGER NOT NULL REFERENCES fbx_pack_groups(id),
      excel_row      INTEGER NOT NULL,
      seller_sku     TEXT NOT NULL,
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
                     CHECK (match_state IN ('matched','qty_mismatch','excel_only')),
      requires_expiry INTEGER CHECK (requires_expiry IN (0,1)),
      UNIQUE(pack_group_id, excel_row)
    );
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

  // 資材の初期値 (管理画面で編集可能にするのは後続PR。tare_g は現行の実測目安)
  const seeded = d.prepare('SELECT COUNT(*) c FROM fbx_box_materials').get().c;
  if (seeded === 0) {
    const ins = d.prepare('INSERT INTO fbx_box_materials (code, name, tare_g, sort) VALUES (?, ?, ?, ?)');
    ins.run('box140', '140サイズ段ボール', 900, 1);
    ins.run('box160', '160サイズ段ボール', 1200, 2);
    ins.run('other', 'その他', null, 9);
  }
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
    const insGroup = d.prepare(`INSERT INTO fbx_pack_groups (run_id, excel_file_id, sheet_name, packing_group_id, display_name, box_count_hint, max_box_columns, structure_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    const insRow = d.prepare(`INSERT INTO fbx_rows (run_id, pack_group_id, excel_row, seller_sku, asin, fnsku, excel_id, product_name, planned_qty, plan_no, source_slot_id, picking_row_no, picking_qty, match_state)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const g of groups) {
      const gi = insGroup.run(runId, fileId, g.sheetName, g.packingGroupId, g.displayName,
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
      // 全行が「投入 + 確定不足 = 予定」でなければ完了できない (Codex PR1 #7)
      const bad = d.prepare(`SELECT COUNT(*) c FROM fbx_rows w
        LEFT JOIN fbx_row_work rw ON rw.row_id = w.id
        WHERE w.run_id = ?
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
      rw.label_worker, rw.check_worker, rw.shortage_qty, rw.shortage_reason
    FROM fbx_rows w LEFT JOIN fbx_row_work rw ON rw.row_id = w.id
    WHERE w.run_id = ? ORDER BY w.pack_group_id, w.excel_row`).all(run.id);
  const boxes = d.prepare(`SELECT b.*,
      COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.box_id = b.id AND p.revoked_at IS NULL), 0) AS total_qty,
      (SELECT COUNT(DISTINCT p.row_id) FROM fbx_placements p WHERE p.box_id = b.id AND p.revoked_at IS NULL) AS sku_count
    FROM fbx_boxes b JOIN fbx_pack_groups g ON g.id = b.pack_group_id
    WHERE g.run_id = ? ORDER BY b.pack_group_id, b.box_no`).all(run.id);
  const placements = d.prepare(`SELECT p.* FROM fbx_placements p
    WHERE p.run_id = ? AND p.revoked_at IS NULL ORDER BY p.box_id, p.box_seq`).all(run.id);
  assignAmazonBoxNumbers(groups, boxes);
  const latest = d.prepare(`SELECT id, data_version, file_name, sha256, created_by, created_at
    FROM fbx_exports WHERE run_id = ? ORDER BY id DESC LIMIT 1`).get(run.id) || null;
  const exportState = {
    latest,
    stale: !!(latest && latest.data_version < run.data_version),
    staUploadedAt: run.sta_uploaded_at || null,
    staExportId: run.sta_export_id || null,
  };
  return { run, groups, rows, boxes, placements, exportState };
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

const boxCodeOf = (groupName, boxNo) => `${groupName}-B${boxNo}`;

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

/** 箱クローズ。実測重量kg必須 (Amazon Excel の箱重量欄になる)。読み合わせは画面側の責務 */
export function closeBox({ boxId, measuredKg, closedReason, cushionLevel, worker, deviceLabel }) {
  const kg = Number(measuredKg);
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
    const b = d.prepare(`SELECT b.*, g.run_id, r.status AS run_status FROM fbx_boxes b
      JOIN fbx_pack_groups g ON g.id = b.pack_group_id JOIN fbx_runs r ON r.id = g.run_id WHERE b.id = ?`)
      .get(Number(boxId));
    if (!b) return { ok: false, error: 'not_found', message: '箱が見つかりません' };
    if (b.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    if (b.status === 'closed') return { ok: false, error: 'already_closed', message: 'この箱は既に閉じられています' };
    if (b.status === 'void') return { ok: false, error: 'box_void', message: 'この箱は取消済みです' };
    const qty = d.prepare('SELECT COALESCE(SUM(qty),0) q FROM fbx_placements WHERE box_id = ? AND revoked_at IS NULL').get(b.id).q;
    if (qty === 0) return { ok: false, error: 'empty_box', message: '空の箱は閉じられません (使わない箱は職員が「箱を取消」してください)' };
    d.prepare(`UPDATE fbx_boxes SET status = 'closed', measured_weight_kg = ?, closed_at = ?, closed_by = ?, closed_reason = ?, cushion_level = ? WHERE id = ?`)
      .run(kg, utcNow(), worker?.display_name || null, reason, cushion, b.id);
    bumpRunVersion(d, b.run_id);
    logEvent({ runId: b.run_id, action: 'box_close', targetType: 'box', targetId: b.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { measuredKg: kg, closedReason: reason, cushionLevel: cushion, totalQty: qty } }, d);
    return { ok: true };
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
      closed_reason = NULL, cushion_level = NULL, reopen_count = reopen_count + 1 WHERE id = ?`).run(b.id);
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
      measured_weight_kg = NULL, closed_at = NULL, closed_by = NULL WHERE id = ?`)
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
    bumpRunVersion(d, row.run_id);
    logEvent({ runId: row.run_id, action: 'placement_add', targetType: 'placement', targetId: placementId,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { rowId: row.id, boxId: box.id, boxNo: box.box_no, qty: q, expiry: exp, layer: lay, boxSeq: seq } }, d);
    return { ok: true, placementId, boxSeq: seq, placed: placed + q, plannedQty: row.planned_qty, expiry: exp };
  }).immediate();
}

function placedOf(d, rowId) {
  return d.prepare('SELECT COALESCE(SUM(qty),0) q FROM fbx_placements WHERE row_id = ? AND revoked_at IS NULL').get(rowId).q;
}

/**
 * 割当の取消 (論理)。利用者 = 自分の端末の直近 SELF_UNDO_MS 以内のみ。職員 (byStaff) = いつでも理由付き。
 * クローズ済みの箱の割当は職員のみ
 */
export function revokePlacement({ placementId, byStaff = false, reason, worker, deviceKey, deviceLabel }) {
  const d = getDB();
  return d.transaction(() => {
    const p = d.prepare(`SELECT p.*, b.status AS box_status, r.status AS run_status FROM fbx_placements p
      JOIN fbx_boxes b ON b.id = p.box_id JOIN fbx_runs r ON r.id = p.run_id WHERE p.id = ?`)
      .get(Number(placementId));
    if (!p) return { ok: false, error: 'not_found', message: '割当が見つかりません' };
    if (p.revoked_at) return { ok: true, already: true };
    if (p.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    if (!byStaff) {
      if (p.device_key !== String(deviceKey)) {
        return { ok: false, error: 'staff_required', message: '他の端末の記録の取り消しは職員のみです' };
      }
      if (Date.now() - Date.parse(p.created_at) > SELF_UNDO_MS) {
        return { ok: false, error: 'staff_required', message: '時間が経った記録の取り消しは職員のみです (職員を呼んでください)' };
      }
      if (p.box_status === 'closed') {
        return { ok: false, error: 'staff_required', message: '閉じた箱の記録の取り消しは職員のみです' };
      }
    } else if (!String(reason || '').trim()) {
      return { ok: false, error: 'reason_required', message: '取消の理由を入力してください' };
    }
    d.prepare('UPDATE fbx_placements SET revoked_at = ?, revoked_by = ?, revoke_reason = ? WHERE id = ?')
      .run(utcNow(), worker?.display_name || null, reason ? String(reason).slice(0, 200) : null, p.id);
    bumpRunVersion(d, p.run_id);
    logEvent({ runId: p.run_id, action: 'placement_revoke', targetType: 'placement', targetId: p.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { rowId: p.row_id, boxId: p.box_id, qty: p.qty, byStaff, reason: reason || null } }, d);
    return { ok: true, placed: placedOf(d, p.row_id) };
  }).immediate();
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
    const label = labelWorker === undefined ? undefined : (labelWorker ? String(labelWorker).slice(0, 30) : null);
    const check = checkWorker === undefined ? undefined : (checkWorker ? String(checkWorker).slice(0, 30) : null);
    d.prepare(`INSERT INTO fbx_row_work (row_id, label_worker, check_worker, updated_at) VALUES (?, ?, ?, ?)
      ON CONFLICT(row_id) DO UPDATE SET
        label_worker = CASE WHEN ? THEN excluded.label_worker ELSE fbx_row_work.label_worker END,
        check_worker = CASE WHEN ? THEN excluded.check_worker ELSE fbx_row_work.check_worker END,
        updated_at = excluded.updated_at`)
      .run(row.id, label === undefined ? null : label, check === undefined ? null : check, utcNow(),
        label === undefined ? 0 : 1, check === undefined ? 0 : 1);
    logEvent({ runId: row.run_id, action: 'row_workers', targetType: 'row', targetId: row.id,
      workerId: worker?.id, workerName: worker?.display_name, deviceLabel, ok: true,
      payload: { labelWorker: label, checkWorker: check } }, d);
    return { ok: true };
  }).immediate();
}

/** 不足確定 (職員のみ。router 側で PIN 確認済みの前提)。shortageQty = 足りなかった個数 */
export function setRowShortage({ rowId, shortageQty, reason, worker, deviceLabel }) {
  const REASONS = ['missing', 'damaged', 'bad_label', 'wrong_item', 'expiry_issue', 'hq_order', 'other'];
  const reasonKey = String(reason || '');
  if (!REASONS.includes(reasonKey)) return { ok: false, error: 'bad_reason', message: '不足の理由を選んでください' };
  const d = getDB();
  return d.transaction(() => {
    const row = d.prepare(`SELECT w.*, r.status AS run_status,
        COALESCE((SELECT SUM(p.qty) FROM fbx_placements p WHERE p.row_id = w.id AND p.revoked_at IS NULL), 0) AS placed
      FROM fbx_rows w JOIN fbx_runs r ON r.id = w.run_id WHERE w.id = ?`).get(Number(rowId));
    if (!row) return { ok: false, error: 'not_found', message: '商品行が見つかりません' };
    if (row.run_status !== 'active') return { ok: false, error: 'run_not_active', message: 'この納品回は作業できる状態ではありません' };
    const remaining = row.planned_qty - row.placed;
    const q = Number(shortageQty);
    if (!Number.isInteger(q) || q <= 0 || q > remaining) {
      return { ok: false, error: 'bad_qty', message: `不足数は 1〜${remaining} で入力してください (残数を超えられません)` };
    }
    d.prepare(`INSERT INTO fbx_row_work (row_id, shortage_qty, shortage_reason, shortage_by, updated_at) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(row_id) DO UPDATE SET shortage_qty = excluded.shortage_qty,
        shortage_reason = excluded.shortage_reason, shortage_by = excluded.shortage_by, updated_at = excluded.updated_at`)
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
    d.prepare('UPDATE fbx_row_work SET shortage_qty = NULL, shortage_reason = NULL, shortage_by = NULL, updated_at = ? WHERE row_id = ?')
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
  getDB().prepare('UPDATE fbx_workers SET pin_hash = ?, pin_salt = ?, pin_fails = 0, pin_lock_until = NULL WHERE id = ?')
    .run(pinHash(salt, p), salt, Number(id));
  safeLogEvent({ action: 'pin_set', workerId: w.id, workerName: w.display_name, deviceLabel: actor || null, ok: true });
  return { ok: true };
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
    vals = { tare: num(tareG, '自重g', { integer: true }), w: num(widthCm, '幅cm', { max: 500 }), l: num(lengthCm, '長さcm', { max: 500 }), h: num(heightCm, '高さcm', { max: 500 }) };
  } catch (e) {
    return { ok: false, error: 'bad_number', message: e.message };
  }
  const d = getDB();
  d.prepare(`INSERT INTO fbx_box_materials (code, name, tare_g, width_cm, length_cm, height_cm, sort, active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(code) DO UPDATE SET name = excluded.name, tare_g = excluded.tare_g, width_cm = excluded.width_cm,
      length_cm = excluded.length_cm, height_cm = excluded.height_cm, sort = excluded.sort, active = excluded.active`)
    .run(c, n, vals.tare, vals.w, vals.l, vals.h, Number.isInteger(Number(sort)) ? Number(sort) : 0, active === false ? 0 : 1);
  safeLogEvent({ action: 'material_upsert', deviceLabel: actor || null, ok: true, payload: { code: c, name: n, ...vals, active: active !== false } });
  return { ok: true, code: c };
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
  const rowBrief = (r) => ({ id: r.id, fnsku: r.fnsku, sku: r.seller_sku, name: r.product_name, planned: r.planned_qty, placed: r.placed, shortage: r.shortage_qty || 0 });

  if (run.status !== 'active' && run.status !== 'done') {
    blockers.push({ code: 'run_status', message: `納品回が「${run.status}」のため出力できません` });
  }
  const live = boxes.filter((b) => b.status !== 'void');
  if (live.length === 0) blockers.push({ code: 'no_boxes', message: '箱がひとつもありません' });
  const incomplete = rows.filter((r) => r.placed + (r.shortage_qty || 0) !== r.planned_qty);
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
  if (shortages.length > 0) warnings.push({ code: 'shortage_rows', message: `不足確定の商品が ${shortages.length} 行あります (Excel の数量は投入数のみ。STA 側の予定数量との差は Amazon 側で調整)`, rows: shortages.map((r) => ({ ...rowBrief(r), reason: r.shortage_reason })) });
  const matchWarn = rows.filter((r) => r.match_state !== 'matched');
  if (matchWarn.length > 0) warnings.push({ code: 'match_warnings', message: `突合で注意のあった商品が ${matchWarn.length} 行あります`, rows: matchWarn.map((r) => ({ ...rowBrief(r), matchState: r.match_state })) });
  const gaps = live.filter((b) => b.amazon_box_no !== b.box_no);
  if (gaps.length > 0) warnings.push({ code: 'box_gap', message: `取消した箱があるため、箱札の番号と Amazon の箱番号がずれます (${gaps.length} 箱)。箱ラベルを貼るときは対応表を見てください`, boxes: gaps.map(boxBrief) });
  const mats = new Map(listMaterials(true).map((m) => [m.code, m]));
  const noDims = live.filter((b) => { const m = mats.get(b.material_code); return !(m && m.width_cm > 0 && m.length_cm > 0 && m.height_cm > 0); });
  if (noDims.length > 0) warnings.push({ code: 'no_dims', message: `外寸が未設定の資材の箱が ${noDims.length} 箱あります (Excel の幅・長さ・高さは空欄 → STA 画面で入力。管理画面の資材で外寸を登録すると次回から自動)`, boxes: noDims.map(boxBrief) });
  if (exportState.latest) {
    warnings.push(exportState.stale
      ? { code: 'stale_export', message: `前回の出力 (#${exportState.latest.id}, ${exportState.latest.created_at.slice(0, 16).replace('T', ' ')}) の後にデータが変わっています。再出力してください` }
      : { code: 'exported', message: `出力済み (#${exportState.latest.id})。データは変わっていません` });
  }
  if (run.sta_uploaded_at) warnings.push({ code: 'sta_uploaded', message: `STA アップ済み (${run.sta_uploaded_at.slice(0, 16).replace('T', ' ')})。再出力は原則不要です` });

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
    maxBoxColumns: g.max_box_columns,
    boxes: live.filter((b) => b.pack_group_id === g.id).map((b) => ({ ...boxBrief(b), weightKg: b.measured_weight_kg, material: b.material_code,
      dims: (() => { const m = mats.get(b.material_code); return m ? { width: m.width_cm, length: m.length_cm, height: m.height_cm } : null; })() })),
  }));
  return { ok: blockers.length === 0, blockers, warnings, groups: groupsOut, expiries, exportState,
    run: { id: run.id, title: run.title, status: run.status, dataVersion: run.data_version, staUploadedAt: run.sta_uploaded_at || null } };
}

/**
 * Excel 書き込み指示 (write_packlist.py の sheets) と、版として保存するスナップショットを組み立てる。
 * 書くセル = 箱数 (M3 相当) / SKU行×Amazon箱番号列の投入数 (0 は空欄のまま) / 箱ごとの実測kg・外寸。
 * 期限は書かない (JP テンプレ v1.1 に列が無い — 要件 §4)
 */
export function buildExportPayload(runId) {
  const ready = exportReadiness(runId);
  if (!ready) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
  if (!ready.ok) return { ok: false, error: 'not_ready', message: '出荷前チェックに未解決の項目があります', readiness: ready };
  const d = getDB();
  const st = getRunState(runId);
  const file = d.prepare('SELECT * FROM fbx_excel_files WHERE run_id = ? ORDER BY id DESC LIMIT 1').get(st.run.id);
  if (!file) return { ok: false, error: 'no_excel', message: '原本Excelの記録がありません' };
  const qtyByRowBox = new Map();
  for (const p of st.placements) {
    const k = `${p.row_id}|${p.box_id}`;
    qtyByRowBox.set(k, (qtyByRowBox.get(k) || 0) + p.qty);
  }
  const sheets = [];
  const snapshotGroups = [];
  for (const g of st.groups) {
    const structure = safeJson(g.structure_json);
    if (!structure?.boxColumns || !structure?.totalBoxes || !structure?.dimRows) {
      return { ok: false, error: 'no_structure', message: `${g.sheet_name}: テンプレ構造の記録がありません (PR1 より前の納品回?)` };
    }
    const gBoxes = st.boxes.filter((b) => b.pack_group_id === g.id && b.status !== 'void').sort((a, b) => a.amazon_box_no - b.amazon_box_no);
    const gRows = st.rows.filter((r) => r.pack_group_id === g.id);
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
    sheets.push({ sheetName: g.sheet_name, cells });
    snapshotGroups.push({
      groupId: g.id, sheetName: g.sheet_name, displayName: g.display_name, packingGroupId: g.packing_group_id, totalBoxes: gBoxes.length,
      boxes: gBoxes.map((b) => ({
        boxId: b.id, boxCode: b.box_code, boxNo: b.box_no, amazonBoxNo: b.amazon_box_no, amazonName: b.amazon_name,
        weightKg: b.measured_weight_kg, material: b.material_code, dims: rg?.boxes.find((x) => x.id === b.id)?.dims || null,
        contents: gRows.map((r) => ({ rowId: r.id, fnsku: r.fnsku, sku: r.seller_sku, qty: qtyByRowBox.get(`${r.id}|${b.id}`) || 0 })).filter((x) => x.qty > 0),
      })),
    });
  }
  return {
    ok: true, sheets, excelFile: file,
    snapshot: { dataVersion: st.run.data_version, groups: snapshotGroups, expiries: ready.expiries, warnings: ready.warnings.map((w) => w.code) },
  };
}

/** 出力結果の記録 (版)。data_version は buildExportPayload 時点の値 (書き込み中の変更は次の出力で「旧版」として出る) */
export function recordExport({ runId, excelFileId, dataVersion, fileName, storedPath, sha256, snapshot, verify, createdBy }) {
  const d = getDB();
  return d.transaction(() => {
    const run = d.prepare('SELECT id, data_version FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    const info = d.prepare(`INSERT INTO fbx_exports (run_id, excel_file_id, data_version, file_name, stored_path, sha256, snapshot_json, verify_json, created_by, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(run.id, Number(excelFileId), Number(dataVersion), fileName, storedPath, sha256, JSON.stringify(snapshot), verify == null ? null : JSON.stringify(verify), createdBy || null, utcNow());
    const exportId = Number(info.lastInsertRowid);
    logEvent({ runId: run.id, action: 'excel_export', targetType: 'export', targetId: exportId, deviceLabel: createdBy, ok: true,
      payload: { dataVersion: Number(dataVersion), sha256, changedDuringWrite: run.data_version !== Number(dataVersion) } }, d);
    return { ok: true, exportId, stale: run.data_version !== Number(dataVersion) };
  }).immediate();
}

export function listExports(runId) {
  return getDB().prepare(`SELECT e.id, e.run_id, e.data_version, e.file_name, e.sha256, e.created_by, e.created_at,
      (e.data_version < r.data_version) AS stale, (r.sta_export_id = e.id) AS sta_uploaded
    FROM fbx_exports e JOIN fbx_runs r ON r.id = e.run_id WHERE e.run_id = ? ORDER BY e.id DESC`).all(Number(runId));
}

export function getExport(id) {
  const row = getDB().prepare('SELECT * FROM fbx_exports WHERE id = ?').get(Number(id));
  if (!row) return null;
  return { ...row, snapshot: safeJson(row.snapshot_json), verify: safeJson(row.verify_json) };
}

/**
 * 「STA にアップした」の記録。指定した版が最新データと一致していることが条件 (旧版のアップは事故)。
 * 記録後は納品回を done にして iPad からの変更を止める (要件 F-7: STAアップ済み後は原則ロック)
 */
export function markStaUploaded({ runId, exportId, actor }) {
  const d = getDB();
  return d.transaction(() => {
    const run = d.prepare('SELECT * FROM fbx_runs WHERE id = ?').get(Number(runId));
    if (!run) return { ok: false, error: 'not_found', message: '納品回が見つかりません' };
    const ex = d.prepare('SELECT * FROM fbx_exports WHERE id = ? AND run_id = ?').get(Number(exportId), run.id);
    if (!ex) return { ok: false, error: 'not_found', message: '出力の記録が見つかりません' };
    if (ex.data_version !== run.data_version) {
      return { ok: false, error: 'stale_export', message: 'この出力の後にデータが変わっています。最新データで再出力し、そのファイルを STA にアップしてから記録してください' };
    }
    if (run.status !== 'active' && run.status !== 'done') return { ok: false, error: 'bad_status', message: `この納品回は ${run.status} です` };
    d.prepare(`UPDATE fbx_runs SET sta_uploaded_at = ?, sta_export_id = ?, status = 'done', done_at = COALESCE(done_at, ?) WHERE id = ?`)
      .run(utcNow(), ex.id, utcNow(), run.id);
    logEvent({ runId: run.id, action: 'run_sta_uploaded', targetType: 'export', targetId: ex.id, deviceLabel: actor, ok: true, payload: { dataVersion: ex.data_version } }, d);
    return { ok: true };
  }).immediate();
}
