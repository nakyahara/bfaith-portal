/**
 * いろは在庫化作業アプリ (iPad) — DB 層
 *
 * 正本の考え方 (要件定義 v1.2 §1.5 / §1.7):
 *   進捗ステータスの正本 = Notion「在庫化作業管理」(当面)。ここに持つのはそのキャッシュと、
 *   アプリ固有のインフラ (端末・作業者・操作履歴) だけ。
 *
 * ⭐作業者名簿 (f_iroha_workers) は staff.db (apps/staff) と分ける。
 *   「人の正本は staff.db に1つ」(2026-09-01) は B-Faith の雇用スタッフの話 —
 *   いろはの利用者は就労支援B型の利用者で雇用スタッフではなく、名簿の性質が違う
 *   (プライバシー配慮・表示名運用。Codex設計相談R1 Q7)
 *
 * 接続は warehouse-mirror の getMirrorDB() を共有 (inbound-check と同じ。台帳・作業仕様
 * マスタ・販売/在庫ミラーと同じファイルにあるので JOIN も参照もそのまま効く)。
 */
import crypto from 'crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const utcNow = () => new Date().toISOString();
const hashToken = (t) => crypto.createHash('sha256').update(String(t)).digest('hex');
const enrollHash = (c) => crypto.createHash('sha256').update('iroha-enroll:' + String(c)).digest('hex');

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
    -- Notion カードのキャッシュ (正本は Notion。表示と絞り込みのためだけに持つ)。
    -- payload = パース済みプロパティの JSON (列を増やさず項目追加に耐える)
    CREATE TABLE IF NOT EXISTS f_iroha_app_notion_cache (
      page_id          TEXT PRIMARY KEY,
      status           TEXT,
      title            TEXT,
      product_code     TEXT,
      dedupe_key       TEXT,
      url              TEXT,
      last_edited_time TEXT,
      payload          TEXT,
      fetched_at       TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_iroha_cache_status ON f_iroha_app_notion_cache(status);

    -- 同期状態などのメタ (last_refresh_at / last_refresh_error / truncated)
    CREATE TABLE IF NOT EXISTS f_iroha_app_meta (
      key   TEXT PRIMARY KEY,
      value TEXT
    );

    -- いろは名簿 (利用者/職員)。⭐staff.db とは別 (冒頭コメント参照)
    CREATE TABLE IF NOT EXISTS f_iroha_workers (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      display_name TEXT NOT NULL,
      worker_type  TEXT NOT NULL CHECK (worker_type IN ('member','staff')),
      active       INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      sort_order   INTEGER NOT NULL DEFAULT 0,
      created_at   TEXT NOT NULL,
      created_by   TEXT
    );

    -- 操作履歴 (append-only。Codex R2「操作履歴」強く推奨)。
    -- ステータス変更など Notion への書き込みは成功・失敗ともここに残す
    CREATE TABLE IF NOT EXISTS f_iroha_app_events (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      at           TEXT NOT NULL,
      action       TEXT NOT NULL,
      page_id      TEXT,
      worker_id    INTEGER,
      worker_name  TEXT,
      device_label TEXT,
      from_value   TEXT,
      to_value     TEXT,
      ok           INTEGER NOT NULL CHECK (ok IN (0,1)),
      error        TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_iroha_events_page ON f_iroha_app_events(page_id, id);

    -- 端末 (iPad)。inbound-check と同じ方式 (トークンはハッシュのみ保存)
    CREATE TABLE IF NOT EXISTS f_iroha_app_devices (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash   TEXT NOT NULL UNIQUE,
      label        TEXT NOT NULL,
      created_by   TEXT,
      created_at   TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at   TEXT
    );

    CREATE TABLE IF NOT EXISTS f_iroha_app_enroll_codes (
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

    CREATE TABLE IF NOT EXISTS f_iroha_app_enroll_attempts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip TEXT,
      at TEXT NOT NULL,
      ok INTEGER NOT NULL CHECK (ok IN (0,1))
    );
  `);
}

// ───────────────────────── Notion キャッシュ ─────────────────────────

/** 取得結果でキャッシュを全置換する (1トランザクション。部分更新にしない — 消えたカードを残さない) */
export function replaceCache(pages, { fetchedAt = utcNow() } = {}) {
  const db = getDB();
  db.transaction(() => {
    db.prepare('DELETE FROM f_iroha_app_notion_cache').run();
    const ins = db.prepare(`INSERT INTO f_iroha_app_notion_cache
      (page_id, status, title, product_code, dedupe_key, url, last_edited_time, payload, fetched_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const p of pages) {
      ins.run(p.pageId, p.status, p.title, p.productCode, p.dedupeKey, p.url, p.lastEditedTime,
        JSON.stringify(p.props), fetchedAt);
    }
    setMeta(db, 'last_refresh_at', fetchedAt);
    setMeta(db, 'last_refresh_error', null);
  }).immediate();
}

export function listCache() {
  return getDB().prepare('SELECT * FROM f_iroha_app_notion_cache ORDER BY page_id').all();
}

/** ステータス変更が成功したとき、次の全体更新を待たずキャッシュへ反映する */
export function updateCacheStatus(pageId, status, lastEditedTime) {
  return getDB().prepare(`UPDATE f_iroha_app_notion_cache
    SET status = ?, last_edited_time = COALESCE(?, last_edited_time) WHERE page_id = ?`)
    .run(status, lastEditedTime || null, pageId).changes;
}

export function removeCachePage(pageId) {
  return getDB().prepare('DELETE FROM f_iroha_app_notion_cache WHERE page_id = ?').run(pageId).changes;
}

function setMeta(db, key, value) {
  if (value == null) db.prepare('DELETE FROM f_iroha_app_meta WHERE key = ?').run(key);
  else db.prepare(`INSERT INTO f_iroha_app_meta (key, value) VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(key, String(value));
}

export function getMeta(key) {
  const r = getDB().prepare('SELECT value FROM f_iroha_app_meta WHERE key = ?').get(key);
  return r ? r.value : null;
}

export function setMetaValue(key, value) { setMeta(getDB(), key, value); }

// ───────────────────────── 作業者 (いろは名簿) ─────────────────────────

export function listIrohaWorkers(includeInactive = false) {
  return getDB().prepare(`SELECT id, display_name, worker_type, active, sort_order
    FROM f_iroha_workers ${includeInactive ? '' : 'WHERE active = 1'}
    ORDER BY sort_order, id`).all();
}

export function getIrohaWorker(id) {
  const n = Number(id);
  if (!Number.isInteger(n) || n <= 0) return null;
  return getDB().prepare('SELECT id, display_name, worker_type, active FROM f_iroha_workers WHERE id = ?').get(n) || null;
}

export function addIrohaWorker({ displayName, workerType, actor }) {
  const name = String(displayName || '').trim();
  if (!name || name.length > 30) return { ok: false, error: 'bad_name', message: '名前は1〜30文字で入力してください' };
  if (workerType !== 'member' && workerType !== 'staff') {
    return { ok: false, error: 'bad_type', message: '区分は 利用者 / 職員 のどちらかです' };
  }
  const db = getDB();
  const dup = db.prepare('SELECT id FROM f_iroha_workers WHERE display_name = ? AND active = 1').get(name);
  if (dup) return { ok: false, error: 'duplicate', message: `「${name}」は既に登録されています` };
  const info = db.prepare(`INSERT INTO f_iroha_workers (display_name, worker_type, active, created_at, created_by)
    VALUES (?, ?, 1, ?, ?)`).run(name, workerType, utcNow(), actor || null);
  return { ok: true, id: Number(info.lastInsertRowid) };
}

export function setIrohaWorkerActive(id, active) {
  return getDB().prepare('UPDATE f_iroha_workers SET active = ? WHERE id = ?')
    .run(active ? 1 : 0, Number(id)).changes > 0;
}

// ───────────────────────── 操作履歴 ─────────────────────────

export function logEvent({ action, pageId = null, workerId = null, workerName = null, deviceLabel = null, from = null, to = null, ok, error = null }) {
  getDB().prepare(`INSERT INTO f_iroha_app_events
    (at, action, page_id, worker_id, worker_name, device_label, from_value, to_value, ok, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(utcNow(), action, pageId, workerId, workerName, deviceLabel,
      from == null ? null : String(from), to == null ? null : String(to),
      ok ? 1 : 0, error == null ? null : String(error).slice(0, 300));
}

export function listEvents(limit = 100) {
  return getDB().prepare('SELECT * FROM f_iroha_app_events ORDER BY id DESC LIMIT ?').all(Number(limit) || 100);
}

// ───────────────────────── 端末 (iPad) — inbound-check と同じ方式 ─────────────────────────

export function createDevice(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const token = crypto.randomBytes(32).toString('base64url');
  const info = getDB().prepare('INSERT INTO f_iroha_app_devices (token_hash, label, created_by, created_at) VALUES (?, ?, ?, ?)')
    .run(hashToken(token), l, actor, utcNow());
  return { token, id: Number(info.lastInsertRowid) };
}

export function verifyDevice(token) {
  if (!token) return null;
  const db = getDB();
  const row = db.prepare('SELECT * FROM f_iroha_app_devices WHERE token_hash = ? AND revoked_at IS NULL').get(hashToken(token));
  if (!row) return null;
  const now = utcNow();
  if (Date.parse(now) - Date.parse(row.created_at) > DEVICE_TTL_MS) return null;
  if (!row.last_seen_at || Date.parse(now) - Date.parse(row.last_seen_at) > 3600_000) {
    db.prepare('UPDATE f_iroha_app_devices SET last_seen_at = ? WHERE id = ?').run(now, row.id);
  }
  return row;
}

export function revokeDevice(id) {
  return getDB().prepare('UPDATE f_iroha_app_devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL')
    .run(utcNow(), Number(id)).changes > 0;
}

export function listDevices() {
  return getDB().prepare('SELECT id, label, created_by, created_at, last_seen_at, revoked_at FROM f_iroha_app_devices ORDER BY id').all();
}

// ── 登録コード (6桁・10分・1回。総当たり対策も inbound-check と同じ) ──

export const ENROLL_TTL_MS = 10 * 60 * 1000;
export const ENROLL_MAX_ATTEMPTS = 5;
export const ENROLL_RATE_WINDOW_MS = 10 * 60 * 1000;
export const ENROLL_RATE_PER_IP = 8;
export const ENROLL_RATE_GLOBAL = 40;

export function createEnrollCode(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const db = getDB();
  const now = new Date();
  const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
  db.transaction(() => {
    // 有効なコードは常に1つだけ (総当たりの当たり確率を 100万分の1 に抑える)
    const revokedAt = new Date(now.getTime() - 1000).toISOString();
    db.prepare('UPDATE f_iroha_app_enroll_codes SET expires_at = ? WHERE used_at IS NULL AND expires_at > ?')
      .run(revokedAt, now.toISOString());
    db.prepare(`INSERT INTO f_iroha_app_enroll_codes (code_hash, label, created_by, created_at, expires_at)
      VALUES (?, ?, ?, ?, ?)`)
      .run(enrollHash(code), l, actor, now.toISOString(), new Date(now.getTime() + ENROLL_TTL_MS).toISOString());
  }).immediate();
  return { code, label: l, expiresAt: new Date(now.getTime() + ENROLL_TTL_MS).toISOString() };
}

export function redeemEnrollCode(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return { ok: false, error: 'bad_code', message: '6桁の数字を入力してください' };
  const db = getDB();
  return db.transaction(() => {
    const row = db.prepare('SELECT * FROM f_iroha_app_enroll_codes WHERE code_hash = ?').get(enrollHash(c));
    if (!row) return { ok: false, error: 'bad_code', message: '登録コードが違います' };
    if (row.attempts >= ENROLL_MAX_ATTEMPTS) return { ok: false, error: 'too_many', message: 'このコードは無効です (発行し直してください)' };
    if (row.used_at) return { ok: false, error: 'used', message: 'この登録コードは使用済みです (発行し直してください)' };
    if (Date.parse(row.expires_at) < Date.now()) return { ok: false, error: 'expired', message: '登録コードの有効期限が切れています (発行し直してください)' };
    const { token, id } = createDevice(row.label, `enroll:${row.created_by}`);
    db.prepare('UPDATE f_iroha_app_enroll_codes SET used_at = ?, used_device_id = ? WHERE id = ?').run(utcNow(), id, row.id);
    return { ok: true, token, label: row.label };
  }).immediate();
}

export function recordEnrollAttempt({ ip = null, ok = false } = {}) {
  const db = getDB();
  db.prepare('INSERT INTO f_iroha_app_enroll_attempts (ip, at, ok) VALUES (?, ?, ?)')
    .run(ip ? String(ip).slice(0, 64) : null, utcNow(), ok ? 1 : 0);
  db.prepare('DELETE FROM f_iroha_app_enroll_attempts WHERE at < ?')
    .run(new Date(Date.now() - 24 * 3600 * 1000).toISOString());
}

export function checkEnrollRate({ ip = null } = {}) {
  const db = getDB();
  const since = new Date(Date.now() - ENROLL_RATE_WINDOW_MS).toISOString();
  const mine = ip ? db.prepare('SELECT COUNT(*) c FROM f_iroha_app_enroll_attempts WHERE ip = ? AND ok = 0 AND at > ?')
    .get(String(ip).slice(0, 64), since).c : 0;
  if (mine >= ENROLL_RATE_PER_IP) {
    return { allowed: false, error: 'rate_limited', message: '試行が多すぎます。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  const all = db.prepare('SELECT COUNT(*) c FROM f_iroha_app_enroll_attempts WHERE ok = 0 AND at > ?').get(since).c;
  if (all >= ENROLL_RATE_GLOBAL) {
    return { allowed: false, error: 'rate_limited', message: '登録の受付を一時的に停止しています。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  return { allowed: true };
}

export function countEnrollAttempt(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return;
  getDB().prepare('UPDATE f_iroha_app_enroll_codes SET attempts = attempts + 1 WHERE code_hash = ?').run(enrollHash(c));
}

export function listActiveEnrollCodes() {
  return getDB().prepare(`SELECT id, label, created_by, created_at, expires_at, attempts
    FROM f_iroha_app_enroll_codes WHERE used_at IS NULL AND expires_at > ? AND attempts < ?
    ORDER BY id DESC`).all(utcNow(), ENROLL_MAX_ATTEMPTS);
}
