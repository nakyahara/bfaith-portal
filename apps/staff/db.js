/**
 * スタッフマスタ (apps/staff) — データ層
 *
 * 「人」の正本。アプリごとの作業者リスト (picking の pk_workers / inquiry-hub の staff_members /
 * product-hub の ph_staff) とは毛色が違う表で、将来の勤怠・シフト (staff_shifts / staff_attendance) の
 * 親になる。今はその「形」だけ = staff 1表 + 変更履歴。
 *
 * - 専用 DB `staff.db` (DATA_DIR)。倉庫ミラー (warehouse-mirror.db) には混ぜない (再構築対象にしない)
 * - staff_no = スタッフ管理番号。人が読む番号で、0001〜0003 と 入社日 YYYYMMDD が混在する運用のため TEXT
 * - display_name = 正式表記 / short_name = 名前タップ用の短い表記 (任意。無ければ display_name)
 * - 退職・契約終了は物理削除しない (active=0 + left_on)。他アプリの履歴が staff_id で参照するため
 * - 初回起動時に seed/initial-staff.json を投入 (staff_no 冪等)
 */
import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// DB パスは接続を開くときに評価する (テストで DATA_DIR を切り替えて closeStaffDB() → 開き直せるように)
const dataDir = () => process.env.DATA_DIR || path.join(process.cwd(), 'data');
export const dbFile = () => path.join(dataDir(), 'staff.db');
const SEED_FILE = path.join(__dirname, 'seed', 'initial-staff.json');

export const STAFF_KINDS = ['employee', 'part_time', 'contractor', 'iroha', 'other'];
export const STAFF_KIND_LABELS = { employee: '社員', part_time: 'パート・アルバイト', contractor: '外注', iroha: 'いろは', other: 'その他' };

const utcNow = () => new Date().toISOString();
let db = null;

export function getStaffDB() {
  if (db) return db;
  if (!fs.existsSync(dataDir())) fs.mkdirSync(dataDir(), { recursive: true });
  db = new Database(dbFile());
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  createTables(db);
  seedInitialStaff(db);
  return db;
}

/** テスト用: 接続を閉じる (DATA_DIR を切り替えて開き直すため) */
export function closeStaffDB() {
  if (db) { try { db.close(); } catch { /* noop */ } db = null; }
}

function createTables(d) {
  d.exec(`
    CREATE TABLE IF NOT EXISTS staff (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      staff_no     TEXT NOT NULL UNIQUE CHECK (trim(staff_no) <> ''),
      display_name TEXT NOT NULL CHECK (trim(display_name) <> ''),
      short_name   TEXT,
      kana         TEXT,
      kind         TEXT CHECK (kind IS NULL OR kind IN ('employee','part_time','contractor','iroha','other')),
      portal_email TEXT,
      joined_on    TEXT,
      left_on      TEXT,
      active       INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      sort         INTEGER NOT NULL DEFAULT 0,
      note         TEXT,
      version      INTEGER NOT NULL DEFAULT 1,
      created_at   TEXT NOT NULL,
      updated_at   TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_staff_active ON staff(active, sort);
    -- 変更履歴 (append-only)。誰がいつ何を変えたか。勤怠・シフトが乗ったときの監査の土台
    CREATE TABLE IF NOT EXISTS staff_audit (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      staff_id    INTEGER NOT NULL REFERENCES staff(id),
      action      TEXT NOT NULL CHECK (action IN ('create','update','deactivate','reactivate','seed')),
      before_json TEXT,
      after_json  TEXT,
      actor       TEXT,
      at          TEXT NOT NULL
    );
    -- 監査表は append-only を DB で強制 (コメント上の規約にしない — Codex R4 Medium)
    CREATE TRIGGER IF NOT EXISTS trg_staff_audit_no_update BEFORE UPDATE ON staff_audit
      BEGIN SELECT RAISE(ABORT, 'staff_audit is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_staff_audit_no_delete BEFORE DELETE ON staff_audit
      BEGIN SELECT RAISE(ABORT, 'staff_audit is append-only'); END;
  `);
}

/** YYYY-MM-DD の実在日付か */
export function isYmd(s) {
  if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

const EDITABLE = ['staff_no', 'display_name', 'short_name', 'kana', 'kind', 'portal_email', 'joined_on', 'left_on', 'sort', 'note'];

function normalize(fields) {
  const out = {};
  for (const k of EDITABLE) {
    if (!(k in fields)) continue;
    let v = fields[k];
    if (v == null) { out[k] = null; continue; }
    v = String(v).trim();
    if (k === 'sort') { const n = Number(v); if (!Number.isInteger(n)) throw new Error('並び順は整数で入力してください'); out[k] = n; continue; }
    if (v === '') { out[k] = k === 'staff_no' || k === 'display_name' ? '' : null; continue; }
    if (k === 'kind' && !STAFF_KINDS.includes(v)) throw new Error(`区分が不正です (${v})`);
    if ((k === 'joined_on' || k === 'left_on') && !isYmd(v)) throw new Error(`${k === 'joined_on' ? '入社日' : '退職日'}は実在する日付を YYYY-MM-DD で入力してください`);
    if (k === 'portal_email') v = v.toLowerCase();
    if (k === 'staff_no' && !/^[0-9A-Za-z_-]{1,20}$/.test(v)) throw new Error('スタッフ管理番号は英数字・ハイフン・アンダースコア 1〜20 文字です');
    if ((k === 'display_name' || k === 'short_name') && v.length > 40) throw new Error('名前は 40 文字までです');
    out[k] = v;
  }
  return out;
}

function audit(d, staffId, action, before, after, actor) {
  d.prepare('INSERT INTO staff_audit (staff_id, action, before_json, after_json, actor, at) VALUES (?, ?, ?, ?, ?, ?)')
    .run(staffId, action, before ? JSON.stringify(before) : null, after ? JSON.stringify(after) : null, actor || null, utcNow());
}

/** 初期データ。行ごとに staff_no で冪等 (既存行は触らない・不足分だけ補う — Codex R4 Medium)。
 *  seed の並び = sort。staff_no が YYYYMMDD 形式なら joined_on に写す */
export function seedInitialStaff(d = getStaffDB()) {
  if (!fs.existsSync(SEED_FILE)) return { seeded: 0 };
  const rows = JSON.parse(fs.readFileSync(SEED_FILE, 'utf8'));
  const now = utcNow();
  const ins = d.prepare(`INSERT OR IGNORE INTO staff (staff_no, display_name, short_name, joined_on, active, sort, created_at, updated_at)
    VALUES (?, ?, ?, ?, 1, ?, ?, ?)`);
  let n = 0;
  d.transaction(() => {
    rows.forEach((r, i) => {
      const no = String(r.staff_no).trim();
      const m = /^(\d{4})(\d{2})(\d{2})$/.exec(no);
      const joined = r.joined_on || (m ? `${m[1]}-${m[2]}-${m[3]}` : null);
      const info = ins.run(no, String(r.display_name).trim(), r.short_name || null, joined, (i + 1) * 10, now, now);
      if (info.changes) { n++; audit(d, Number(info.lastInsertRowid), 'seed', null, { staff_no: no, display_name: r.display_name }, 'seed'); }
    });
  })();
  return { seeded: n };
}

// ───────────────────────── 参照 ─────────────────────────

/** 名前タップに出す表記 */
export function tapName(s) {
  return (s.short_name && s.short_name.trim()) || s.display_name;
}

export function listStaff({ includeInactive = false } = {}) {
  return getStaffDB().prepare(`SELECT * FROM staff ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY sort, id`).all();
}

export function getStaff(id) {
  return getStaffDB().prepare('SELECT * FROM staff WHERE id = ?').get(Number(id)) || null;
}

export function getStaffByNo(staffNo) {
  return getStaffDB().prepare('SELECT * FROM staff WHERE staff_no = ?').get(String(staffNo || '').trim()) || null;
}

/** 他アプリ向け: 名前タップの候補 [{staff_id, staff_no, name, display_name, sort}] (有効のみ) */
export function listTapCandidates() {
  return listStaff().map(s => ({ staff_id: s.id, staff_no: s.staff_no, name: tapName(s), display_name: s.display_name, sort: s.sort }));
}

// ───────────────────────── 更新 ─────────────────────────

export function createStaff(fields, actor) {
  const d = getStaffDB();
  const f = normalize(fields);
  if (!f.staff_no) throw new Error('スタッフ管理番号は必須です');
  if (!f.display_name) throw new Error('名前は必須です');
  return d.transaction(() => {
    if (d.prepare('SELECT 1 FROM staff WHERE staff_no = ?').get(f.staff_no)) throw new Error(`スタッフ管理番号 ${f.staff_no} は既に使われています`);
    const now = utcNow();
    const sort = f.sort ?? ((d.prepare('SELECT MAX(sort) m FROM staff').get().m || 0) + 10);
    const info = d.prepare(`INSERT INTO staff (staff_no, display_name, short_name, kana, kind, portal_email, joined_on, left_on, active, sort, note, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)`)
      .run(f.staff_no, f.display_name, f.short_name ?? null, f.kana ?? null, f.kind ?? null, f.portal_email ?? null, f.joined_on ?? null, f.left_on ?? null, sort, f.note ?? null, now, now);
    const row = getStaff(info.lastInsertRowid);
    audit(d, row.id, 'create', null, row, actor);
    return row;
  }).immediate();
}

const isVersion = v => Number.isSafeInteger(v) && v >= 1;

/** 楽観ロック (version 必須) 付き更新。変更が無い列は触らない。
 *  競合判定は `UPDATE ... WHERE id=? AND version=?` の更新件数で行う (読んでから書くまでの隙を作らない — Codex R4 High) */
export function updateStaff(id, fields, actor, expectVersion) {
  if (!isVersion(expectVersion)) return { ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' };
  const d = getStaffDB();
  const f = normalize(fields);
  if ('staff_no' in f && !f.staff_no) throw new Error('スタッフ管理番号は必須です');
  if ('display_name' in f && !f.display_name) throw new Error('名前は必須です');
  return d.transaction(() => {
    const before = getStaff(id);
    if (!before) return { ok: false, error: 'not_found' };
    if (before.version !== expectVersion) return { ok: false, error: 'conflict', current: before };
    if (f.staff_no && f.staff_no !== before.staff_no && d.prepare('SELECT 1 FROM staff WHERE staff_no = ? AND id <> ?').get(f.staff_no, before.id)) {
      throw new Error(`スタッフ管理番号 ${f.staff_no} は既に使われています`);
    }
    const keys = Object.keys(f);
    if (keys.length === 0) return { ok: true, staff: before, changed: false };
    const set = keys.map(k => `${k} = @${k}`).join(', ');
    const info = d.prepare(`UPDATE staff SET ${set}, version = version + 1, updated_at = @now WHERE id = @id AND version = @v`)
      .run({ ...f, now: utcNow(), id: before.id, v: expectVersion });
    if (info.changes === 0) return { ok: false, error: 'conflict', current: getStaff(before.id) };
    const after = getStaff(before.id);
    audit(d, before.id, 'update', before, after, actor);
    return { ok: true, staff: after, changed: true };
  }).immediate();
}

/** 有効/無効 (version 必須・条件付き UPDATE)。無効化 = 退職日 (指定 or 既存 or 今日)。削除はしない */
export function setStaffActive(id, active, actor, { leftOn = null, expectVersion } = {}) {
  if (typeof active !== 'boolean') return { ok: false, error: 'bad_request', message: 'active は true/false で指定してください' };
  if (!isVersion(expectVersion)) return { ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' };
  if (leftOn != null && leftOn !== '' && !isYmd(leftOn)) return { ok: false, error: 'bad_request', message: '退職日は実在する日付を YYYY-MM-DD で入力してください' };
  const d = getStaffDB();
  return d.transaction(() => {
    const before = getStaff(id);
    if (!before) return { ok: false, error: 'not_found' };
    if (before.version !== expectVersion) return { ok: false, error: 'conflict', current: before };
    const now = utcNow();
    const info = active
      ? d.prepare('UPDATE staff SET active = 1, left_on = NULL, version = version + 1, updated_at = ? WHERE id = ? AND version = ?').run(now, before.id, expectVersion)
      : d.prepare('UPDATE staff SET active = 0, left_on = COALESCE(?, left_on, substr(?, 1, 10)), version = version + 1, updated_at = ? WHERE id = ? AND version = ?').run(leftOn || null, now, now, before.id, expectVersion);
    if (info.changes === 0) return { ok: false, error: 'conflict', current: getStaff(before.id) };
    const after = getStaff(before.id);
    audit(d, before.id, active ? 'reactivate' : 'deactivate', before, after, actor);
    return { ok: true, staff: after };
  }).immediate();
}

export function listAudit(staffId, limit = 100) {
  return getStaffDB().prepare('SELECT * FROM staff_audit WHERE staff_id = ? ORDER BY id DESC LIMIT ?').all(Number(staffId), Math.max(1, Math.min(1000, Number(limit) || 100)));
}
