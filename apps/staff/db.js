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
 * - 2026-09-10 (中原さん): **いろはの利用者・職員もこの表に入れる** (役割 iroha)。いろは在庫化 / FBA箱詰めの
 *   名簿はこの表の鏡 (roster-link.js) になり、**職員PIN もここ**で 1 つに持つ (pin_hash / pin_salt)。
 *   PIN の salt は方式ごと丸ごと保存する (`staff-pin:<hex>`) — 各アプリに散っていた既存の PIN を
 *   ハッシュのまま持ち越せるように (`iroha-pin:<hex>` / `fbx-pin:<hex>` もそのまま照合できる)
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// DB パスは接続を開くときに評価する (テストで DATA_DIR を切り替えて closeStaffDB() → 開き直せるように)
const dataDir = () => process.env.DATA_DIR || path.join(process.cwd(), 'data');
export const dbFile = () => path.join(dataDir(), 'staff.db');
const SEED_FILE = path.join(__dirname, 'seed', 'initial-staff.json');

export const STAFF_KINDS = ['employee', 'part_time', 'contractor', 'iroha', 'other'];
export const STAFF_KIND_LABELS = { employee: '社員', part_time: 'パート・アルバイト', contractor: '外注', iroha: 'いろは', other: 'その他' };

/**
 * 役割 = 「どの現場の名前タップに出すか」。雇用区分 (kind) とは別の軸で、1人が複数持てる。
 * 倉庫系アプリ (ピッキング・梱包・入荷受付チェック) は warehouse の人だけを名前タップに出す
 * (事務担当が倉庫の一覧に並ばないように — 中原さん 2026-09-01)。
 */
export const STAFF_ROLES = ['warehouse', 'office', 'iroha'];
export const STAFF_ROLE_LABELS = { warehouse: '倉庫作業 (ピッキング・梱包・入荷)', office: '事務', iroha: 'いろは現場 (在庫化・FBA箱詰め)' };
export const STAFF_ROLE_SHORT = { warehouse: '倉庫', office: '事務', iroha: 'いろは' };
export const WAREHOUSE_ROLE = 'warehouse';
/** いろはの現場 (在庫化 / FBA箱詰め) の名前タップに出す役割。この役割だけの人と 利用者 (kind=iroha) は miniPC 向け export に出さない */
export const IROHA_ROLE = 'iroha';

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
    -- 役割 (どの現場の名前タップに出すか)。1人が複数持てる
    CREATE TABLE IF NOT EXISTS staff_roles (
      staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
      role     TEXT NOT NULL CHECK (role IN ('warehouse','office','iroha')),
      PRIMARY KEY (staff_id, role)
    );
    CREATE INDEX IF NOT EXISTS idx_staff_roles_role ON staff_roles(role);
    -- 名簿の世代 (roster_rev)。人・役割・PIN の有無が変わるたびに +1。鏡 (いろは在庫化 / FBA箱詰め) は
    -- これを見て「変わっていなければ写さない」— 毎回 全行を比べない
    CREATE TABLE IF NOT EXISTS staff_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    -- 紐付け直し (同じ人の行を from → to にまとめた) の記録。鏡 (いろは在庫化 / FBA箱詰め) は写すたびに
    -- id 順に冪等に適用する = プロセス内で登録されていない鏡・あとから起動した鏡でも追いつく (Codex #1301 R2 High#1/#2)
    CREATE TABLE IF NOT EXISTS staff_merges (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      from_staff_id INTEGER NOT NULL REFERENCES staff(id),
      to_staff_id   INTEGER NOT NULL REFERENCES staff(id),
      actor         TEXT,
      at            TEXT NOT NULL
    );
    -- 監査表は append-only を DB で強制 (コメント上の規約にしない — Codex R4 Medium)
    CREATE TRIGGER IF NOT EXISTS trg_staff_audit_no_update BEFORE UPDATE ON staff_audit
      BEGIN SELECT RAISE(ABORT, 'staff_audit is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_staff_audit_no_delete BEFORE DELETE ON staff_audit
      BEGIN SELECT RAISE(ABORT, 'staff_audit is append-only'); END;
  `);
  // 職員PIN (2026-09-10 共通化)。ハッシュだけ持つ。pin_salt は方式を含む文字列 (冒頭コメント)
  const cols = new Set(d.prepare('PRAGMA table_info(staff)').all().map(c => c.name));
  const addCol = (col, ddl) => { if (!cols.has(col)) d.exec(`ALTER TABLE staff ADD COLUMN ${col} ${ddl}`); };
  addCol('pin_hash', 'TEXT');
  addCol('pin_salt', 'TEXT');
  addCol('pin_fails', 'INTEGER NOT NULL DEFAULT 0');
  addCol('pin_lock_until', 'TEXT');
  addCol('pin_set_at', 'TEXT');
  migrateRolesCheck(d);
}

/**
 * staff_roles の CHECK 制約に iroha を足す。SQLite は CHECK を ALTER できないので、古い制約の表だけ作り直す。
 * 判定は sqlite_master の DDL 文字列 ('iroha' を含むか)。行は全部写す (FK は staff_id → staff なので順序は問わない)
 */
function migrateRolesCheck(d) {
  const ddl = d.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'staff_roles'").get()?.sql || '';
  if (ddl.includes("'iroha'")) return;
  d.transaction(() => {
    d.exec(`
      CREATE TABLE staff_roles_new (
        staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
        role     TEXT NOT NULL CHECK (role IN ('warehouse','office','iroha')),
        PRIMARY KEY (staff_id, role)
      );
      INSERT INTO staff_roles_new (staff_id, role) SELECT staff_id, role FROM staff_roles;
      DROP TABLE staff_roles;
      ALTER TABLE staff_roles_new RENAME TO staff_roles;
      CREATE INDEX IF NOT EXISTS idx_staff_roles_role ON staff_roles(role);
    `);
  })();
}

// ───────────────────────── 名簿の世代 ─────────────────────────

/** 鏡 (他アプリの名簿) が「写し直す必要があるか」を知るための番号。人・役割・PIN の有無を変えたら bumpRosterRev */
export function getRosterRev(d = getStaffDB()) {
  const r = d.prepare("SELECT value FROM staff_meta WHERE key = 'roster_rev'").get();
  return r ? Number(r.value) : 0;
}
function bumpRosterRev(d) {
  d.prepare("INSERT INTO staff_meta (key, value) VALUES ('roster_rev', '1') ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)").run();
}

/** staff.db の 1 トランザクション (BEGIN IMMEDIATE)。中で呼ぶ createStaff 等は savepoint になる */
export function withStaffTx(fn) { return getStaffDB().transaction(fn).immediate(); }

/** 紐付け直しの記録 (from の行は to と同じ人だった)。鏡はこれを写すたびに冪等に適用する */
export function recordStaffMerge(fromId, toId, actor, d = getStaffDB()) {
  d.prepare('INSERT INTO staff_merges (from_staff_id, to_staff_id, actor, at) VALUES (?, ?, ?, ?)').run(Number(fromId), Number(toId), actor || null, utcNow());
  bumpRosterRev(d);
}
export function listStaffMerges(d = getStaffDB()) {
  return d.prepare('SELECT id, from_staff_id, to_staff_id, actor, at FROM staff_merges ORDER BY id').all();
}

/**
 * 「職員PIN を一度でも設定したことがある」。FBA箱詰めの名簿 bootstrap (PIN 持ちが 0 人なら iPad から無ゲート登録) を
 * 恒久に閉じるための共通の事実。どの経路 (スタッフマスタ画面 / いろは在庫化 / FBA箱詰め) で設定しても立つ (Codex #1301 R2 Medium)
 */
export function staffPinEverSet(d = getStaffDB()) {
  return !!d.prepare("SELECT 1 FROM staff_meta WHERE key = 'pin_ever_set'").get();
}
function markPinEverSet(d) {
  d.prepare("INSERT INTO staff_meta (key, value) VALUES ('pin_ever_set', '1') ON CONFLICT(key) DO NOTHING").run();
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
  const insRole = d.prepare('INSERT OR IGNORE INTO staff_roles (staff_id, role) VALUES (?, ?)');
  let n = 0, roled = 0;
  d.transaction(() => {
    rows.forEach((r, i) => {
      const no = String(r.staff_no).trim();
      const m = /^(\d{4})(\d{2})(\d{2})$/.exec(no);
      const joined = r.joined_on || (m ? `${m[1]}-${m[2]}-${m[3]}` : null);
      const info = ins.run(no, String(r.display_name).trim(), r.short_name || null, joined, (i + 1) * 10, now, now);
      let id = info.changes ? Number(info.lastInsertRowid) : null;
      if (id) { n++; audit(d, id, 'seed', null, { staff_no: no, display_name: r.display_name }, 'seed'); }
      // 役割は「まだ1つも役割が無い人」にだけ入れる (画面で外した役割を seed が復活させない)
      const roles = Array.isArray(r.roles) && r.roles.length ? r.roles.filter(x => STAFF_ROLES.includes(x)) : [WAREHOUSE_ROLE];
      if (!id) {
        const ex = d.prepare('SELECT id FROM staff WHERE staff_no = ?').get(no);
        if (!ex) return;
        id = ex.id;
        if (d.prepare('SELECT 1 FROM staff_roles WHERE staff_id = ?').get(id)) return;
      }
      for (const role of roles) insRole.run(id, role);
      roled++;
    });
  })();
  return { seeded: n, roled };
}

// ───────────────────────── 参照 ─────────────────────────

/** 読み出しに使う列。🚨 pin_hash / pin_salt は絶対に出さない (監査 JSON にも乗るため、SELECT * を使わない) */
const PUBLIC_COLS = 'id, staff_no, display_name, short_name, kana, kind, portal_email, joined_on, left_on, active, sort, note, version, created_at, updated_at, pin_set_at, (pin_hash IS NOT NULL) AS pin_set';

/** 名前タップに出す表記 */
export function tapName(s) {
  return (s.short_name && s.short_name.trim()) || s.display_name;
}

/** 役割を各行に配列で載せる (staff_id → ['warehouse', …]) */
function attachRoles(rows) {
  if (rows.length === 0) return rows;
  const map = new Map();
  for (const r of getStaffDB().prepare('SELECT staff_id, role FROM staff_roles').all()) {
    if (!map.has(r.staff_id)) map.set(r.staff_id, []);
    map.get(r.staff_id).push(r.role);
  }
  for (const s of rows) s.roles = (map.get(s.id) || []).sort();
  return rows;
}

export function listStaff({ includeInactive = false, role = null } = {}) {
  const rows = attachRoles(getStaffDB().prepare(`SELECT ${PUBLIC_COLS} FROM staff ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY sort, id`).all());
  return role ? rows.filter(s => s.roles.includes(role)) : rows;
}

/** 役割の付け外し (指定した集合に置き換える)。空配列も許す (どの現場にも出さない) */
export function setStaffRoles(id, roles, actor) {
  const d = getStaffDB();
  const list = [...new Set((Array.isArray(roles) ? roles : []).map(r => String(r).trim()))];
  const bad = list.filter(r => !STAFF_ROLES.includes(r));
  if (bad.length) return { ok: false, error: 'bad_request', message: `役割が不正です: ${bad.join(', ')}` };
  return d.transaction(() => {
    const before = getStaff(id);
    if (!before) return { ok: false, error: 'not_found' };
    const cur = d.prepare('SELECT role FROM staff_roles WHERE staff_id = ?').all(before.id).map(r => r.role).sort();
    d.prepare('DELETE FROM staff_roles WHERE staff_id = ?').run(before.id);
    const ins = d.prepare('INSERT INTO staff_roles (staff_id, role) VALUES (?, ?)');
    for (const r of list) ins.run(before.id, r);
    const after = list.slice().sort();
    if (cur.join(',') !== after.join(',')) {
      audit(d, before.id, 'update', { ...before, roles: cur }, { ...before, roles: after }, actor);
      bumpRosterRev(d);
    }
    return { ok: true, staff: { ...getStaff(before.id), roles: after } };
  }).immediate();
}

export function getStaff(id) {
  const row = getStaffDB().prepare(`SELECT ${PUBLIC_COLS} FROM staff WHERE id = ?`).get(Number(id)) || null;
  return row ? attachRoles([row])[0] : null;
}

export function getStaffByNo(staffNo) {
  const row = getStaffDB().prepare(`SELECT ${PUBLIC_COLS} FROM staff WHERE staff_no = ?`).get(String(staffNo || '').trim()) || null;
  return row ? attachRoles([row])[0] : null;
}

/**
 * 他アプリ向け: 名前タップの候補 (有効のみ)。
 * 既定は**倉庫作業の役割を持つ人だけ** — 事務担当がピッキング・梱包・入荷の一覧に並ばないように
 * (中原さん 2026-09-01)。役割を指定しなければ全員が欲しい呼び出し側は role:null を渡す
 */
export function listTapCandidates({ role = WAREHOUSE_ROLE } = {}) {
  return listStaff({ role }).map(s => ({
    staff_id: s.id, staff_no: s.staff_no, name: tapName(s), display_name: s.display_name, sort: s.sort, roles: s.roles,
  }));
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
    bumpRosterRev(d);
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
    // 区分を いろは (利用者) に変えたら PIN は消す — 利用者は PIN を持たない (Codex #1301 R1 Medium)
    if (f.kind === 'iroha') d.prepare('UPDATE staff SET pin_hash = NULL, pin_salt = NULL, pin_fails = 0, pin_lock_until = NULL, pin_set_at = NULL WHERE id = ?').run(before.id);
    const after = getStaff(before.id);
    audit(d, before.id, 'update', before, after, actor);
    bumpRosterRev(d);
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
    bumpRosterRev(d);
    return { ok: true, staff: after };
  }).immediate();
}

export function listAudit(staffId, limit = 100) {
  return getStaffDB().prepare('SELECT * FROM staff_audit WHERE staff_id = ? ORDER BY id DESC LIMIT ?').all(Number(staffId), Math.max(1, Math.min(1000, Number(limit) || 100)));
}

// ───────────────────────── 職員PIN (2026-09-10 共通化) ─────────────────────────
// いろは在庫化 / FBA箱詰め のアプリごとに持っていた PIN をここに 1 つにする。
// ハッシュは scrypt (短い数字PINは sha256 だと漏えい時に総当たりが容易)。失敗ロックは DB に持つ (再起動で消えない)。
// 🚨 pin_salt は「方式:塩」の文字列を丸ごと保存する。既存アプリの PIN (`iroha-pin:<hex>` / `fbx-pin:<hex>`) を
//    そのまま持ち越して照合できるようにするため。新しく設定するときは `staff-pin:<hex>`

const PIN_MAX_FAILS = 5;
const PIN_LOCK_MS = 10 * 60 * 1000;
export const pinHashWithSalt = (saltStr, pin) => crypto.scryptSync(String(pin), String(saltStr), 32).toString('hex');

/** PIN を設定できるのは いろはの利用者 (kind = iroha) 以外 = 職員 */
export function canHoldPin(s) { return !!s && s.kind !== 'iroha'; }

export function setStaffPin(id, pin, actor) {
  const p = String(pin || '').trim();
  if (!/^\d{4,8}$/.test(p)) return { ok: false, error: 'bad_pin', message: 'PINは4〜8桁の数字で設定してください' };
  const d = getStaffDB();
  return d.transaction(() => {
    const s = getStaff(id);
    if (!s) return { ok: false, error: 'not_found', message: 'スタッフが見つかりません' };
    if (!canHoldPin(s)) return { ok: false, error: 'not_staff', message: 'PINを設定できるのは職員だけです' };
    const salt = `staff-pin:${crypto.randomBytes(16).toString('hex')}`;
    const now = utcNow();
    d.prepare('UPDATE staff SET pin_hash = ?, pin_salt = ?, pin_fails = 0, pin_lock_until = NULL, pin_set_at = ? WHERE id = ?')
      .run(pinHashWithSalt(salt, p), salt, now, s.id);
    // 監査には「設定した」事実だけ (before/after に PIN は入らない — PUBLIC_COLS)
    audit(d, s.id, 'update', { ...s }, { ...s, pin_set: 1, pin_set_at: now }, actor);
    markPinEverSet(d);
    bumpRosterRev(d);
    return { ok: true };
  }).immediate();
}

/**
 * 他アプリに残っていた PIN のハッシュを持ち越す (移行専用)。塩は方式込みの文字列で受ける。
 * すでに PIN がある人には上書きしない (先に共通化された方を正とする)
 */
export function importStaffPinHash(id, { pinHash, pinSalt }, actor) {
  if (!pinHash || !pinSalt) return { ok: false, error: 'bad_request' };
  const d = getStaffDB();
  return d.transaction(() => {
    const s = getStaff(id);
    if (!s) return { ok: false, error: 'not_found' };
    if (!canHoldPin(s)) return { ok: false, error: 'not_staff', message: '利用者には PIN を持たせません' };   // Codex #1301 R1 Medium
    if (s.pin_set) return { ok: true, kept: true };
    d.prepare('UPDATE staff SET pin_hash = ?, pin_salt = ?, pin_fails = 0, pin_lock_until = NULL, pin_set_at = ? WHERE id = ?')
      .run(String(pinHash), String(pinSalt), utcNow(), s.id);
    audit(d, s.id, 'update', { ...s }, { ...s, pin_set: 1 }, actor || 'roster-migrate');
    markPinEverSet(d);
    bumpRosterRev(d);
    return { ok: true, kept: false };
  }).immediate();
}

/**
 * PIN 照合。連続失敗 5 回で 10 分ロック (DB 永続 — 再起動で回避できない)。
 * @returns {ok:true} | {ok:false, error:'pin_required'|'pin_invalid'|'pin_locked'}
 */
export function verifyStaffPin(id, pin) {
  const d = getStaffDB();
  return d.transaction(() => {
    const row = d.prepare('SELECT id, kind, pin_hash, pin_salt, pin_fails, pin_lock_until FROM staff WHERE id = ?').get(Number(id));
    if (!row || !canHoldPin(row)) return { ok: false, error: 'pin_required', message: '職員を選んでください' };
    if (!row.pin_hash) return { ok: false, error: 'pin_required', message: 'この職員にはPINが未設定です (管理画面で設定してください)' };
    if (row.pin_lock_until && Date.parse(row.pin_lock_until) > Date.now()) {
      return { ok: false, error: 'pin_locked', message: 'PINの間違いが続いたため一時的にロックしました。10分ほど待ってください' };
    }
    const p = String(pin || '').trim();
    if (!p || pinHashWithSalt(row.pin_salt, p) !== row.pin_hash) {
      const fails = (row.pin_fails || 0) + 1;
      const lockUntil = fails >= PIN_MAX_FAILS ? new Date(Date.now() + PIN_LOCK_MS).toISOString() : null;
      d.prepare('UPDATE staff SET pin_fails = ?, pin_lock_until = COALESCE(?, pin_lock_until) WHERE id = ?')
        .run(lockUntil ? 0 : fails, lockUntil, row.id);
      if (lockUntil) return { ok: false, error: 'pin_locked', message: 'PINの間違いが続いたため一時的にロックしました。10分ほど待ってください' };
      return { ok: false, error: p ? 'pin_invalid' : 'pin_required', message: p ? 'PINが違います' : '職員のPINを入れてください' };
    }
    d.prepare('UPDATE staff SET pin_fails = 0, pin_lock_until = NULL WHERE id = ?').run(row.id);
    return { ok: true };
  }).immediate();
}

/** テスト用: PIN ロックと失敗カウンタを消す */
export function _clearStaffPinFails() {
  getStaffDB().prepare('UPDATE staff SET pin_fails = 0, pin_lock_until = NULL').run();
}

// ───────────────────────── 他アプリの名簿から来る人 ─────────────────────────

/**
 * 名前の照合キー。空白 (半角/全角) を無視し NFKC で寄せる — picking の staff-sync と同じ規則。
 * 「同じ人か」は**完全一致**でしか判断しない (部分一致・読みの推測はしない)
 */
export function nameKey(s) {
  return String(s == null ? '' : s).normalize('NFKC').replace(/[\s\u3000]+/g, '').trim();
}

/**
 * 自動採番のスタッフ管理番号 (いろはの iPad から追加した人など、番号を人が決めていないとき)。
 * 既存の人の番号体系 (0001〜 / YYYYMMDD) と混ざらない接頭辞 + 連番。あとから管理画面で書き換えてよい
 */
export function nextGeneratedStaffNo(prefix = 'IROHA', d = getStaffDB()) {
  const rows = d.prepare('SELECT staff_no FROM staff WHERE staff_no LIKE ?').all(`${prefix}-%`);
  const re = new RegExp(`^${prefix}-(\\d+)$`);
  let max = 0;
  for (const r of rows) { const m = re.exec(r.staff_no); if (m) max = Math.max(max, Number(m[1])); }
  return `${prefix}-${String(max + 1).padStart(3, '0')}`;
}
