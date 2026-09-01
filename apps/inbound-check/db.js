/**
 * 入荷受付チェック (apps/inbound-check) — データ層
 *
 * 接続は warehouse-mirror の getMirrorDB() を共有 (Render 完結)。テーブルは本モジュールが
 * 初回利用時に冪等作成する (mirror 本体の createTables に足さない = mirror 初期化を道連れにしない)。
 *
 * 設計正本 = AI_reference『システム設計/入荷受付チェック_要件定義_20260901.md』v1.1 §7
 *  - 取込バッチ単位: 一覧も確認状態も batch に紐づく。新バッチを active にした時点で全行 unchecked
 *  - 同時操作: line_state の原子的な条件付き UPDATE + version。負けたら conflict + 現在状態を返す
 *  - 旧 batch_id を持った操作は stale_batch で拒否
 *  - 0件は正常。拒否は「ヘッダ不正 / 列数不一致 / 壊れたSJIS / 同一ハッシュ / 生成時刻が active より古い」のみ
 *  - events は append-only (取消は reverted_event_id で打ち消し先を指す)
 */
import crypto from 'crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { listTapCandidates, getStaffByNo, tapName } from '../staff/db.js';
import { parseInboundCsv } from './csv.js';

const utcNow = () => new Date().toISOString();
const hashToken = t => crypto.createHash('sha256').update(String(t)).digest('hex');

export const BATCH_SOURCES = ['auto', 'drive_retry', 'manual_upload'];
export const RETENTION_DAYS = 365;      // events / batches / line_state
export const LOG_RETENTION_DAYS = 90;   // 取込ログ (失敗理由)
// 端末トークンの有効期間 (Cookie の Max-Age だけに頼らずサーバー側でも検証)
export const DEVICE_TTL_MS = 400 * 24 * 3600 * 1000;

let ensured = false;
let ensuredFor = null;

/** テーブルを冪等作成した接続を返す */
export function getDB() {
  const db = getMirrorDB();
  if (!ensured || ensuredFor !== db) {
    createTables(db);
    ensured = true;
    ensuredFor = db;
  }
  return db;
}

export function createTables(db = getMirrorDB()) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS f_inbound_check_batches (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      source           TEXT NOT NULL CHECK (source IN ('auto','drive_retry','manual_upload')),
      file_name        TEXT,
      file_hash        TEXT NOT NULL UNIQUE,
      csv_generated_at TEXT NOT NULL,
      data_max_at      TEXT,             -- 明細の 更新日時/作成日時 の最大 (CSV 内の信頼できる時刻。0件なら NULL)
      row_count        INTEGER NOT NULL CHECK (row_count >= 0),
      slip_count       INTEGER NOT NULL CHECK (slip_count >= 0),
      imported_at      TEXT NOT NULL,
      imported_by      TEXT,
      status           TEXT NOT NULL CHECK (status IN ('active','superseded')),
      note             TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS uq_ic_batches_active ON f_inbound_check_batches(status) WHERE status = 'active';

    CREATE TABLE IF NOT EXISTS f_inbound_check_slips (
      batch_id      INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      ar_no         TEXT NOT NULL,
      planned_date  TEXT,
      received_date TEXT,
      status        TEXT,
      line_count    INTEGER NOT NULL,
      seq           INTEGER NOT NULL,
      PRIMARY KEY (batch_id, ar_no)
    );

    CREATE TABLE IF NOT EXISTS f_inbound_check_lines (
      batch_id      INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      line_key      TEXT NOT NULL,
      ar_no         TEXT NOT NULL,
      line_no       INTEGER NOT NULL,
      detail_no     INTEGER NOT NULL,
      product_id    TEXT NOT NULL,
      code_key      TEXT NOT NULL,
      product_name  TEXT,
      barcode       TEXT,
      planned_qty   INTEGER NOT NULL,
      received_qty  INTEGER,
      seq           INTEGER NOT NULL,
      PRIMARY KEY (batch_id, line_key)
    );
    CREATE INDEX IF NOT EXISTS idx_ic_lines_code ON f_inbound_check_lines(batch_id, code_key);

    CREATE TABLE IF NOT EXISTS f_inbound_check_line_state (
      batch_id       INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      line_key       TEXT NOT NULL,
      status         TEXT NOT NULL CHECK (status IN ('unchecked','checked')),
      version        INTEGER NOT NULL DEFAULT 1,
      checked_by     TEXT,
      checked_device TEXT,
      checked_at     TEXT,
      PRIMARY KEY (batch_id, line_key)
    );

    CREATE TABLE IF NOT EXISTS f_inbound_check_events (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id          INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      line_key          TEXT NOT NULL,
      ar_no             TEXT NOT NULL,
      action            TEXT NOT NULL CHECK (action IN ('check','uncheck')),
      worker            TEXT,
      device_id         INTEGER,
      device_label      TEXT,
      created_at        TEXT NOT NULL,
      reverted_event_id INTEGER REFERENCES f_inbound_check_events(id)
    );
    CREATE INDEX IF NOT EXISTS idx_ic_events_batch ON f_inbound_check_events(batch_id, line_key, id);

    CREATE TABLE IF NOT EXISTS f_inbound_check_import_log (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      at         TEXT NOT NULL,
      actor      TEXT,
      source     TEXT NOT NULL,
      file_name  TEXT,
      ok         INTEGER NOT NULL CHECK (ok IN (0,1)),
      batch_id   INTEGER,
      message    TEXT
    );

    CREATE TABLE IF NOT EXISTS f_inbound_check_devices (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash   TEXT NOT NULL UNIQUE,
      label        TEXT NOT NULL,
      created_by   TEXT NOT NULL,
      created_at   TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at   TEXT
    );

  `);
  // 作業者表は PR1 (#1055) で作ったが、スタッフマスタ (apps/staff) に一本化したので廃止。
  // 無条件 DROP はしない (Codex R4 High): 行が 0 のときだけ落とす。誰かが先に登録していたら表を残して警告
  // (その名前はスタッフマスタに手で登録してから、この表を手動で落とす)
  const legacy = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_inbound_check_workers'").get();
  if (legacy) {
    const n = db.prepare('SELECT COUNT(*) c FROM f_inbound_check_workers').get().c;
    if (n === 0) db.exec('DROP TABLE f_inbound_check_workers');
    else console.warn(`[inbound-check] 旧 f_inbound_check_workers に ${n} 行あるため DROP しません (スタッフマスタへ移してから手動で削除)`);
  }
  // 確認イベントに staff_id (スタッフマスタの id) を後付け。worker (表示名) は従来どおり残す
  const cols = db.prepare('PRAGMA table_info(f_inbound_check_events)').all().map(c => c.name);
  if (!cols.includes('staff_id')) db.exec('ALTER TABLE f_inbound_check_events ADD COLUMN staff_id INTEGER');
}

// ───────────────────────── 端末 (iPad) ─────────────────────────

export function createDevice(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const token = crypto.randomBytes(32).toString('base64url');
  const info = getDB().prepare(`INSERT INTO f_inbound_check_devices (token_hash, label, created_by, created_at) VALUES (?, ?, ?, ?)`)
    .run(hashToken(token), l, actor, utcNow());
  return { token, id: Number(info.lastInsertRowid) };
}

export function verifyDevice(token) {
  if (!token) return null;
  const db = getDB();
  const row = db.prepare('SELECT * FROM f_inbound_check_devices WHERE token_hash = ? AND revoked_at IS NULL').get(hashToken(token));
  if (!row) return null;
  const now = utcNow();
  if (Date.parse(now) - Date.parse(row.created_at) > DEVICE_TTL_MS) return null;
  if (!row.last_seen_at || Date.parse(now) - Date.parse(row.last_seen_at) > 3600_000) {
    db.prepare('UPDATE f_inbound_check_devices SET last_seen_at = ? WHERE id = ?').run(now, row.id);
  }
  return row;
}

export function revokeDevice(id) {
  return getDB().prepare('UPDATE f_inbound_check_devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL').run(utcNow(), id).changes > 0;
}

export function listDevices() {
  return getDB().prepare('SELECT id, label, created_by, created_at, last_seen_at, revoked_at FROM f_inbound_check_devices ORDER BY id').all();
}

// ───────────────────────── 作業者 (名前タップ) = スタッフマスタ (apps/staff) を参照 ─────────────────────────
// 自前の作業者表は持たない (2026-09-01 中原さん方針: 「人」の正本は staff.db に1つ)。
// 名前タップの候補 = 有効なスタッフ。code = staff_no (スタッフ管理番号)、name = 短い表記 (無ければ正式表記)

export function listWorkers() {
  return listTapCandidates().map(c => ({ code: c.staff_no, name: c.name, staff_id: c.staff_id, sort: c.sort }));
}

/** staff_no → {code, name, staff_id, active}。無ければ null */
export function getWorker(code) {
  const s = getStaffByNo(code);
  if (!s) return null;
  return { code: s.staff_no, name: tapName(s), staff_id: s.id, active: s.active };
}

// ───────────────────────── 取込 ─────────────────────────

export function getActiveBatch() {
  return getDB().prepare("SELECT * FROM f_inbound_check_batches WHERE status = 'active'").get() || null;
}

export function getBatch(id) {
  return getDB().prepare('SELECT * FROM f_inbound_check_batches WHERE id = ?').get(Number(id)) || null;
}

export function listBatches(limit = 30) {
  return getDB().prepare('SELECT * FROM f_inbound_check_batches ORDER BY id DESC LIMIT ?').all(Math.max(1, Math.min(200, Number(limit) || 30)));
}

export function listImportLog(limit = 20) {
  return getDB().prepare('SELECT * FROM f_inbound_check_import_log ORDER BY id DESC LIMIT ?').all(Math.max(1, Math.min(200, Number(limit) || 20)));
}

function logImport(db, { actor, source, fileName, ok, batchId, message }) {
  db.prepare('INSERT INTO f_inbound_check_import_log (at, actor, source, file_name, ok, batch_id, message) VALUES (?, ?, ?, ?, ?, ?, ?)')
    .run(utcNow(), actor || null, source, fileName || null, ok ? 1 : 0, batchId || null, message || null);
}

function isIsoDate(s) {
  return typeof s === 'string' && !Number.isNaN(Date.parse(s));
}

/**
 * CSV を取り込んで新しい active バッチにする。
 * @param {Buffer} buffer
 * @param {object} o  { fileName, source, actor, generatedAt (ISO。無ければ今) }
 * @returns {ok:true, batch, rowCount, slipCount} | {ok:false, error, message, batch?}
 *   error: bad_csv | duplicate_file | older_file
 */
export function importCsv(buffer, { fileName = null, source = 'manual_upload', actor = null, generatedAt = null } = {}) {
  if (!BATCH_SOURCES.includes(source)) throw new Error(`不正な source: ${source}`);
  const db = getDB();
  const fileHash = crypto.createHash('sha256').update(buffer).digest('hex');
  const genAt = isIsoDate(generatedAt) ? new Date(generatedAt).toISOString() : utcNow();

  let parsed;
  try {
    parsed = parseInboundCsv(buffer);
  } catch (e) {
    logImport(db, { actor, source, fileName, ok: false, message: e.message });
    return { ok: false, error: 'bad_csv', message: e.message };
  }
  // CSV 内の信頼できる時刻 = 明細の 更新日時/作成日時 の最大 (ブラウザの File.lastModified は改変可能なので
  // 新旧判定の主キーにしない — Codex R3 High)。0件 CSV は明細が無いので null
  let dataMaxAt = null;
  for (const r of parsed.rows) {
    for (const t of [r.updated_at, r.created_at]) {
      if (t && (!dataMaxAt || Date.parse(t) > Date.parse(dataMaxAt))) dataMaxAt = t;
    }
  }

  const dupResult = dup => {
    const message = `同じ内容のCSVは取込済みです (バッチ#${dup.id}、${dup.imported_at})`;
    logImport(db, { actor, source, fileName, ok: false, batchId: dup.id, message });
    return { ok: false, error: 'duplicate_file', message, batch: dup };
  };

  const tx = db.transaction(() => {
    // 重複判定はトランザクション内で (同時取込で両方が通過→UNIQUE 例外 500 を防ぐ — Codex R3 Medium)
    const dup = db.prepare('SELECT * FROM f_inbound_check_batches WHERE file_hash = ?').get(fileHash);
    if (dup) return dupResult(dup);
    const active = getActiveBatch();
    if (active) {
      // ①明細時刻で判定 (両方に明細がある時)。②明細時刻で判定できない時 (どちらかが0件) は生成時刻で判定
      if (dataMaxAt && active.data_max_at && Date.parse(dataMaxAt) < Date.parse(active.data_max_at)) {
        const message = `CSVの明細が現在の一覧より古い (明細の最終更新 ${dataMaxAt} < ${active.data_max_at}) ため取り込みません`;
        logImport(db, { actor, source, fileName, ok: false, batchId: active.id, message });
        return { ok: false, error: 'older_file', message, batch: active };
      }
      if (Date.parse(genAt) < Date.parse(active.csv_generated_at)) {
        const message = `CSVの生成時刻 (${genAt}) が現在の一覧 (${active.csv_generated_at}) より古いため取り込みません`;
        logImport(db, { actor, source, fileName, ok: false, batchId: active.id, message });
        return { ok: false, error: 'older_file', message, batch: active };
      }
    }
    // 旧 active を先に superseded にする (active の部分ユニーク索引があるため)
    db.prepare("UPDATE f_inbound_check_batches SET status = 'superseded' WHERE status = 'active'").run();
    const slipsMap = new Map();
    for (const r of parsed.rows) {
      if (!slipsMap.has(r.ar_no)) slipsMap.set(r.ar_no, { ar_no: r.ar_no, planned_date: r.planned_date, received_date: r.received_date, status: r.status, line_count: 0, seq: slipsMap.size + 1 });
      slipsMap.get(r.ar_no).line_count++;
    }
    const now = utcNow();
    const info = db.prepare(`INSERT INTO f_inbound_check_batches
      (source, file_name, file_hash, csv_generated_at, data_max_at, row_count, slip_count, imported_at, imported_by, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')`)
      .run(source, fileName, fileHash, genAt, dataMaxAt, parsed.rows.length, slipsMap.size, now, actor);
    const batchId = Number(info.lastInsertRowid);
    const insSlip = db.prepare('INSERT INTO f_inbound_check_slips (batch_id, ar_no, planned_date, received_date, status, line_count, seq) VALUES (?, ?, ?, ?, ?, ?, ?)');
    for (const s of slipsMap.values()) insSlip.run(batchId, s.ar_no, s.planned_date, s.received_date, s.status, s.line_count, s.seq);
    const insLine = db.prepare(`INSERT INTO f_inbound_check_lines
      (batch_id, line_key, ar_no, line_no, detail_no, product_id, code_key, product_name, barcode, planned_qty, received_qty, seq)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insState = db.prepare("INSERT INTO f_inbound_check_line_state (batch_id, line_key, status, version) VALUES (?, ?, 'unchecked', 1)");
    for (const r of parsed.rows) {
      insLine.run(batchId, r.line_key, r.ar_no, r.line_no, r.detail_no, r.product_id, r.code_key, r.product_name, r.barcode, r.planned_qty, r.received_qty, r.seq);
      insState.run(batchId, r.line_key);
    }
    logImport(db, { actor, source, fileName, ok: true, batchId, message: `${parsed.rows.length}行 / ${slipsMap.size}伝票` });
    cleanupOld(db);
    return { ok: true, batch: getBatch(batchId), rowCount: parsed.rows.length, slipCount: slipsMap.size };
  });
  try {
    return tx.immediate();
  } catch (e) {
    // immediate tx 同士は直列化されるので通常ここには来ないが、UNIQUE(file_hash) 違反は 409 相当に正規化する
    if (/UNIQUE constraint failed: f_inbound_check_batches\.file_hash/.test(e.message)) {
      const dup = db.prepare('SELECT * FROM f_inbound_check_batches WHERE file_hash = ?').get(fileHash);
      if (dup) return dupResult(dup);
    }
    throw e;
  }
}

/** 保持期間を過ぎたバッチ (子は CASCADE) と取込ログを削除。active は消さない */
export function cleanupOld(db = getDB(), now = new Date()) {
  const cut = new Date(now.getTime() - RETENTION_DAYS * 86400_000).toISOString();
  const logCut = new Date(now.getTime() - LOG_RETENTION_DAYS * 86400_000).toISOString();
  const a = db.prepare("DELETE FROM f_inbound_check_batches WHERE status = 'superseded' AND imported_at < ?").run(cut).changes;
  const b = db.prepare('DELETE FROM f_inbound_check_import_log WHERE at < ?').run(logCut).changes;
  return { batches: a, logs: b };
}

// ───────────────────────── 表示用の結合 (入数・行き先・ピックロケ) ─────────────────────────

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/**
 * 商品ごとの補助情報。
 *  info: f_inbound_info (入数 / BCシール / 直接ピックロケ保管 / BF保管荷姿 / いろは在庫化作業有無 / memo)
 *  locs: mirror_logizard_stock を商品×ロケで集約。P3F (ピックロケ) を在庫数の多い順に最大3件、
 *        引けない商品は loc_source='none' = 「ピックロケの登録なし」。フリーロケ運用なので
 *        空いている保管ロケに入れてよい状態であり、異常ではない (中原さん 2026-09-01)。
 *        P3F が無ければ他ブロックの先頭1件を「保管」として返す。JOIN で明細を増やさず商品単位で集約
 */
export function productInfoMap(codeKeys) {
  const db = getDB();
  const keys = [...new Set(codeKeys.map(k => String(k || '').trim().toLowerCase()).filter(Boolean))];
  const map = new Map();
  for (const k of keys) map.set(k, { info: null, pick_locs: [], other_locs: [], loc_source: 'none' });
  if (keys.length === 0) return map;
  const chunk = 400;
  for (let i = 0; i < keys.length; i += chunk) {
    const part = keys.slice(i, i + chunk);
    const ph = part.map(() => '?').join(',');
    if (tableExists(db, 'f_inbound_info')) {
      const rows = db.prepare(`SELECT code_key, 入数, 入庫時BCシール貼りフラグ AS bc_seal, 直接ピックロケ保管 AS direct_pick,
          BF保管荷姿 AS storage_form, いろは在庫化作業有無 AS iroha, memo
        FROM f_inbound_info WHERE code_key IN (${ph})`).all(...part);
      for (const r of rows) {
        const m = map.get(r.code_key);
        if (m) m.info = { irisu: r.入数, bc_seal: r.bc_seal, direct_pick: r.direct_pick, storage_form: r.storage_form, iroha: r.iroha, memo: r.memo };
      }
    }
    if (tableExists(db, 'mirror_logizard_stock')) {
      const rows = db.prepare(`SELECT lower(trim(商品ID)) AS k, COALESCE(ブロック略称,'') AS block, COALESCE(ロケ,'') AS loc,
          SUM(在庫数) AS qty
        FROM mirror_logizard_stock
        WHERE lower(trim(商品ID)) IN (${ph}) AND 在庫数 > 0
        GROUP BY k, block, loc`).all(...part);
      for (const r of rows) {
        const m = map.get(r.k);
        if (!m) continue;
        const label = r.block && r.loc ? `${r.block}-${r.loc}` : (r.block || r.loc || '');
        if (!label) continue;
        const entry = { loc: label, qty: r.qty };
        if (/^P3F/i.test(r.block)) m.pick_locs.push(entry); else m.other_locs.push(entry);
      }
    }
  }
  for (const m of map.values()) {
    m.pick_locs.sort((a, b) => b.qty - a.qty || a.loc.localeCompare(b.loc));
    m.other_locs.sort((a, b) => b.qty - a.qty || a.loc.localeCompare(b.loc));
    m.pick_locs = m.pick_locs.slice(0, 3);
    m.other_locs = m.other_locs.slice(0, 1);
    m.loc_source = m.pick_locs.length ? 'pick' : (m.other_locs.length ? 'storage' : 'none');
  }
  return map;
}

/** 前回 (直前の superseded バッチ) で確認済みだった行 → 参考表示用 {line_key → {by, at}} */
function previousCheckedMap(db, activeId) {
  const prev = db.prepare("SELECT id FROM f_inbound_check_batches WHERE status = 'superseded' AND id < ? ORDER BY id DESC LIMIT 1").get(activeId);
  const m = new Map();
  if (!prev) return m;
  for (const r of db.prepare("SELECT line_key, checked_by, checked_at FROM f_inbound_check_line_state WHERE batch_id = ? AND status = 'checked'").all(prev.id)) {
    m.set(r.line_key, { by: r.checked_by, at: r.checked_at });
  }
  return m;
}

/**
 * iPad 一覧の状態。active バッチが無ければ { batch:null, slips:[], lines:[] }
 * 各行 = 明細 + 状態 + 補助情報 + 前回確認 (参考)
 */
export function getState() {
  const db = getDB();
  const batch = getActiveBatch();
  if (!batch) return { batch: null, slips: [], lines: [], totals: { lines: 0, checked: 0 } };
  const slips = db.prepare('SELECT * FROM f_inbound_check_slips WHERE batch_id = ? ORDER BY seq').all(batch.id);
  const lines = db.prepare(`SELECT l.*, s.status AS check_status, s.version, s.checked_by, s.checked_device, s.checked_at
    FROM f_inbound_check_lines l JOIN f_inbound_check_line_state s ON s.batch_id = l.batch_id AND s.line_key = l.line_key
    WHERE l.batch_id = ? ORDER BY l.seq`).all(batch.id);
  const info = productInfoMap(lines.map(l => l.code_key));
  const prev = previousCheckedMap(db, batch.id);
  const checkedBySlip = new Map();
  for (const l of lines) {
    const x = info.get(l.code_key) || { info: null, pick_locs: [], other_locs: [], loc_source: 'none' };
    l.info = x.info;
    l.pick_locs = x.pick_locs;
    l.other_locs = x.other_locs;
    l.loc_source = x.loc_source;
    l.prev_checked = prev.get(l.line_key) || null;
    if (l.check_status === 'checked') checkedBySlip.set(l.ar_no, (checkedBySlip.get(l.ar_no) || 0) + 1);
  }
  for (const s of slips) s.checked_count = checkedBySlip.get(s.ar_no) || 0;
  const checked = lines.filter(l => l.check_status === 'checked').length;
  return { batch, slips, lines, totals: { lines: lines.length, checked } };
}

// ───────────────────────── 消し込み ─────────────────────────

function currentState(db, batchId, lineKey) {
  return db.prepare('SELECT * FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = ?').get(batchId, lineKey) || null;
}

/**
 * 1タップ確認 / 取消。
 * @returns {ok:true, state} | {ok:false, error:'stale_batch'|'not_found'|'conflict'|'bad_request', current?}
 */
export function applyCheck({ batchId, lineKey, action, expectVersion, worker, staffId = null, deviceId = null, deviceLabel = null }) {
  if (!['check', 'uncheck'].includes(action)) return { ok: false, error: 'bad_request', message: 'action が不正です' };
  const bid = Number(batchId);
  if (!Number.isInteger(bid) || !lineKey) return { ok: false, error: 'bad_request', message: 'batch_id / line_key が不正です' };
  // version は必須 (省略を許すと条件が無効化され「画面が見ていた状態」の検証にならない — Codex R3 High)
  if (!Number.isSafeInteger(expectVersion) || expectVersion < 1) return { ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' };
  const w = String(worker || '').trim();
  if (!w) return { ok: false, error: 'bad_request', message: '作業者を選んでください' };
  const db = getDB();
  const tx = db.transaction(() => {
    const active = getActiveBatch();
    if (!active || active.id !== bid) return { ok: false, error: 'stale_batch', message: '一覧が更新されました。最新の一覧を読み込み直してください', activeBatchId: active ? active.id : null };
    const line = db.prepare('SELECT ar_no FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?').get(bid, lineKey);
    if (!line) return { ok: false, error: 'not_found', message: '明細が見つかりません' };
    const now = utcNow();
    let res;
    if (action === 'check') {
      res = db.prepare(`UPDATE f_inbound_check_line_state
        SET status = 'checked', version = version + 1, checked_by = ?, checked_device = ?, checked_at = ?
        WHERE batch_id = ? AND line_key = ? AND status = 'unchecked' AND version = ?`)
        .run(w, deviceLabel, now, bid, lineKey, expectVersion);
    } else {
      res = db.prepare(`UPDATE f_inbound_check_line_state
        SET status = 'unchecked', version = version + 1, checked_by = NULL, checked_device = NULL, checked_at = NULL
        WHERE batch_id = ? AND line_key = ? AND status = 'checked' AND version = ?`)
        .run(bid, lineKey, expectVersion);
    }
    if (res.changes === 0) {
      return { ok: false, error: 'conflict', message: '他の端末で先に更新されました', current: currentState(db, bid, lineKey) };
    }
    let reverted = null;
    if (action === 'uncheck') {
      const last = db.prepare(`SELECT id FROM f_inbound_check_events WHERE batch_id = ? AND line_key = ? AND action = 'check' ORDER BY id DESC LIMIT 1`).get(bid, lineKey);
      reverted = last ? last.id : null;
    }
    db.prepare(`INSERT INTO f_inbound_check_events (batch_id, line_key, ar_no, action, worker, staff_id, device_id, device_label, created_at, reverted_event_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(bid, lineKey, line.ar_no, action, w, staffId, deviceId, deviceLabel, now, reverted);
    return { ok: true, state: currentState(db, bid, lineKey) };
  });
  return tx.immediate();
}

// ───────────────────────── 履歴 ─────────────────────────

export function listEvents(batchId, limit = 5000) {
  return getDB().prepare(`SELECT e.*, l.product_id, l.product_name, l.planned_qty
    FROM f_inbound_check_events e
    LEFT JOIN f_inbound_check_lines l ON l.batch_id = e.batch_id AND l.line_key = e.line_key
    WHERE e.batch_id = ? ORDER BY e.id LIMIT ?`).all(Number(batchId), Math.max(1, Math.min(50000, Number(limit) || 5000)));
}

/** 履歴 CSV (UTF-8 BOM)。取消は reverted_event_id で打ち消し先を示す */
export function eventsCsv(batchId) {
  const rows = listEvents(batchId, 50000);
  const esc = v => {
    let s = v == null ? '' : String(v);
    // Excel の数式インジェクション対策: 先頭が = + - @ タブ CR の値はアポストロフィで無害化 (Codex R3 Medium)
    if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const head = ['event_id', 'batch_id', 'AR番号', '明細キー', '商品ID', '商品名', '予定数', '操作', '作業者', '端末', '日時(UTC)', '打ち消した確認ID'];
  const lines = [head.join(',')];
  for (const r of rows) {
    lines.push([r.id, r.batch_id, r.ar_no, r.line_key, r.product_id, r.product_name, r.planned_qty,
      r.action === 'check' ? '確認' : '取消', r.worker, r.device_label, r.created_at, r.reverted_event_id].map(esc).join(','));
  }
  return '﻿' + lines.join('\r\n') + '\r\n';
}
