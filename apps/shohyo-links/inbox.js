/**
 * shohyo-links — 証憑の受け箱 (voucher_inbox)
 *
 * 「証憑はここに入れれば、あとはロボットがMFの仕訳に貼る」の台帳。
 * ファイル実体は DATA_DIR/shohyo-vouchers/<sha256>.<ext> (正本はMF側のBox。ここは貼るまでの待機所)。
 *
 * status の遷移:
 *   new → (突合) → proposed            … 一意に決まった (提案モードでは人が承認、自動モードでは即添付)
 *                → waiting_registration … 相手の明細がまだ未仕訳 (登録されたら次の周期で貼る)
 *                → ambiguous / no_match … 人が候補から選ぶ
 *                → attached             … MFに貼れた (mf_file_id を持つ)
 *   人の操作: excluded (貼らない) / manual で任意の仕訳へ
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { getShohyoDB } from './db.js';

export const INBOX_STATUSES = ['new', 'proposed', 'waiting_registration', 'ambiguous', 'no_match', 'attached', 'excluded', 'error'];
const OPEN_STATUSES = ['new', 'proposed', 'waiting_registration', 'ambiguous', 'no_match', 'error'];
const EXT_BY_MIME = { 'application/pdf': 'pdf', 'image/jpeg': 'jpg', 'image/png': 'png' };

let ready = false;
function db() {
  const d = getShohyoDB();
  if (!ready) {
    d.exec(`CREATE TABLE IF NOT EXISTS voucher_inbox (
      id                   INTEGER PRIMARY KEY AUTOINCREMENT,
      sha256               TEXT NOT NULL UNIQUE,
      file_name            TEXT NOT NULL,
      mime                 TEXT NOT NULL DEFAULT '',
      size                 INTEGER NOT NULL DEFAULT 0,
      stored_path          TEXT NOT NULL DEFAULT '',
      source               TEXT NOT NULL DEFAULT 'upload',
      vendor_id            INTEGER,
      vendor_name          TEXT NOT NULL DEFAULT '',
      doc_date             TEXT NOT NULL DEFAULT '',
      amount               INTEGER,
      note                 TEXT NOT NULL DEFAULT '',
      status               TEXT NOT NULL DEFAULT 'new',
      match_tx_id          TEXT NOT NULL DEFAULT '',
      match_journal_id     TEXT NOT NULL DEFAULT '',
      match_journal_number INTEGER,
      match_strength       TEXT NOT NULL DEFAULT '',
      match_reason         TEXT NOT NULL DEFAULT '',
      candidates_json      TEXT NOT NULL DEFAULT '[]',
      mf_file_id           TEXT NOT NULL DEFAULT '',
      attached_at          TEXT NOT NULL DEFAULT '',
      last_checked_at      TEXT NOT NULL DEFAULT '',
      error                TEXT NOT NULL DEFAULT '',
      created_at           TEXT NOT NULL,
      updated_at           TEXT NOT NULL
    )`);
    d.exec('CREATE INDEX IF NOT EXISTS idx_voucher_inbox_status ON voucher_inbox(status)');
    d.exec(`CREATE TABLE IF NOT EXISTS voucher_attach_log (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      inbox_id       INTEGER NOT NULL,
      journal_id     TEXT NOT NULL,
      journal_number INTEGER,
      tx_id          TEXT NOT NULL DEFAULT '',
      mf_file_id     TEXT NOT NULL DEFAULT '',
      mode           TEXT NOT NULL,
      actor          TEXT NOT NULL DEFAULT '',
      reason         TEXT NOT NULL DEFAULT '',
      created_at     TEXT NOT NULL
    )`);
    d.exec(`CREATE TABLE IF NOT EXISTS shohyo_settings (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )`);
    ready = true;
  }
  return d;
}

export function storageDir() {
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data');
  const dir = path.join(dataDir, 'shohyo-vouchers');
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

// ---- 設定 (自動添付ON/OFF = 提案モード) ----

export function getSetting(key, fallback = '') {
  const row = db().prepare('SELECT value FROM shohyo_settings WHERE key = ?').get(key);
  return row ? row.value : fallback;
}

export function setSetting(key, value) {
  db().prepare('INSERT INTO shohyo_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value')
    .run(key, String(value));
}

/** 自動添付 (strong一致を人の承認なしで貼る)。既定 OFF = 提案モード */
export function autoAttachEnabled() {
  return getSetting('auto_attach', '0') === '1';
}

// ---- 受け入れ ----

/**
 * ファイルを受け箱に入れる。同じ内容 (sha256) は二重登録しない (既存行を返す・duplicate=true)
 * @param {Buffer} buffer
 * @param {{ file_name, mime?, source?, vendor_id?, vendor_name?, doc_date?, amount?, note? }} meta
 */
export function addToInbox(buffer, meta) {
  const sha256 = crypto.createHash('sha256').update(buffer).digest('hex');
  const existing = db().prepare('SELECT * FROM voucher_inbox WHERE sha256 = ?').get(sha256);
  if (existing) return { row: existing, duplicate: true };
  const ext = EXT_BY_MIME[meta.mime] || (path.extname(meta.file_name || '').slice(1).toLowerCase() || 'bin');
  const stored = path.join(storageDir(), `${sha256}.${ext}`);
  fs.writeFileSync(stored, buffer);
  const now = new Date().toISOString();
  const res = db().prepare(`INSERT INTO voucher_inbox
    (sha256, file_name, mime, size, stored_path, source, vendor_id, vendor_name, doc_date, amount, note, status, created_at, updated_at)
    VALUES (@sha256, @file_name, @mime, @size, @stored_path, @source, @vendor_id, @vendor_name, @doc_date, @amount, @note, 'new', @now, @now)`)
    .run({
      sha256, file_name: String(meta.file_name || 'voucher').slice(0, 255), mime: meta.mime || '', size: buffer.length,
      stored_path: stored, source: meta.source || 'upload',
      vendor_id: meta.vendor_id ? Number(meta.vendor_id) : null, vendor_name: String(meta.vendor_name || '').slice(0, 200),
      doc_date: /^\d{4}-\d{2}-\d{2}$/.test(String(meta.doc_date || '')) ? meta.doc_date : '',
      amount: Number.isFinite(Number(meta.amount)) && Number(meta.amount) > 0 ? Math.round(Number(meta.amount)) : null,
      note: String(meta.note || '').slice(0, 500), now,
    });
  return { row: getInbox(res.lastInsertRowid), duplicate: false };
}

export function getInbox(id) {
  const row = db().prepare('SELECT * FROM voucher_inbox WHERE id = ?').get(id);
  return row ? hydrate(row) : null;
}

function hydrate(row) {
  let candidates = [];
  try { candidates = JSON.parse(row.candidates_json || '[]'); } catch { /* 壊れていても一覧は出す */ }
  return { ...row, candidates };
}

export function listInbox({ status = null, limit = 500 } = {}) {
  const rows = status
    ? db().prepare('SELECT * FROM voucher_inbox WHERE status = ? ORDER BY doc_date DESC, id DESC LIMIT ?').all(status, limit)
    : db().prepare('SELECT * FROM voucher_inbox ORDER BY doc_date DESC, id DESC LIMIT ?').all(limit);
  return rows.map(hydrate);
}

export function listOpenInbox() {
  const q = OPEN_STATUSES.map(() => '?').join(',');
  return db().prepare(`SELECT * FROM voucher_inbox WHERE status IN (${q}) ORDER BY id`).all(...OPEN_STATUSES).map(hydrate);
}

export function countByStatus() {
  const out = Object.fromEntries(INBOX_STATUSES.map(s => [s, 0]));
  for (const r of db().prepare('SELECT status, COUNT(*) AS c FROM voucher_inbox GROUP BY status').all()) out[r.status] = r.c;
  return out;
}

export function readFile(row) {
  return fs.readFileSync(row.stored_path);
}

/** 人が直せる項目 (日付・金額・支払先・メモ)。直したら突合をやり直すため status は new に戻す */
export function updateInboxMeta(id, body) {
  const cur = getInbox(id);
  if (!cur) return null;
  if (cur.status === 'attached') throw new Error('already_attached');
  const data = {
    vendor_id: body.vendor_id === undefined ? cur.vendor_id : (body.vendor_id ? Number(body.vendor_id) : null),
    vendor_name: body.vendor_name === undefined ? cur.vendor_name : String(body.vendor_name || '').slice(0, 200),
    doc_date: body.doc_date === undefined ? cur.doc_date : (/^\d{4}-\d{2}-\d{2}$/.test(String(body.doc_date || '')) ? body.doc_date : ''),
    amount: body.amount === undefined ? cur.amount : (Number(body.amount) > 0 ? Math.round(Number(body.amount)) : null),
    note: body.note === undefined ? cur.note : String(body.note || '').slice(0, 500),
  };
  db().prepare(`UPDATE voucher_inbox SET vendor_id=@vendor_id, vendor_name=@vendor_name, doc_date=@doc_date, amount=@amount, note=@note,
    status='new', match_tx_id='', match_journal_id='', match_journal_number=NULL, match_strength='', match_reason='', candidates_json='[]',
    updated_at=@now WHERE id=@id`).run({ ...data, now: new Date().toISOString(), id });
  return getInbox(id);
}

export function setMatch(id, { status, tx_id = '', journal_id = '', journal_number = null, strength = '', reason = '', candidates = [] }) {
  db().prepare(`UPDATE voucher_inbox SET status=@status, match_tx_id=@tx_id, match_journal_id=@journal_id, match_journal_number=@journal_number,
    match_strength=@strength, match_reason=@reason, candidates_json=@candidates, last_checked_at=@now, error='', updated_at=@now WHERE id=@id`)
    .run({ status, tx_id, journal_id, journal_number, strength, reason, candidates: JSON.stringify(candidates).slice(0, 20000), now: new Date().toISOString(), id });
}

export function markAttached(id, { journal_id, journal_number = null, tx_id = '', mf_file_id, mode, actor = '', reason = '' }) {
  const now = new Date().toISOString();
  const d = db();
  const tx = d.transaction(() => {
    d.prepare(`UPDATE voucher_inbox SET status='attached', match_journal_id=@journal_id, match_journal_number=@journal_number, match_tx_id=@tx_id,
      mf_file_id=@mf_file_id, attached_at=@now, error='', updated_at=@now WHERE id=@id`)
      .run({ journal_id, journal_number, tx_id, mf_file_id, now, id });
    d.prepare(`INSERT INTO voucher_attach_log (inbox_id, journal_id, journal_number, tx_id, mf_file_id, mode, actor, reason, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(id, journal_id, journal_number, tx_id, mf_file_id, mode, actor, reason, now);
  });
  tx();
  return getInbox(id);
}

export function markError(id, message) {
  db().prepare(`UPDATE voucher_inbox SET status='error', error=?, last_checked_at=?, updated_at=? WHERE id=?`)
    .run(String(message).slice(0, 500), new Date().toISOString(), new Date().toISOString(), id);
}

export function setStatus(id, status) {
  if (!INBOX_STATUSES.includes(status)) throw new Error('bad_status');
  db().prepare('UPDATE voucher_inbox SET status=?, updated_at=? WHERE id=?').run(status, new Date().toISOString(), id);
  return getInbox(id);
}

export function listAttachLog(limit = 200) {
  return db().prepare(`SELECT l.*, i.file_name, i.vendor_name, i.amount, i.doc_date FROM voucher_attach_log l
    JOIN voucher_inbox i ON i.id = l.inbox_id ORDER BY l.id DESC LIMIT ?`).all(limit);
}

/** 添付済み証憑のうち他の証憑が既に確定している明細ID (証憑側の一意性チェック用) */
export function takenTransactionIds() {
  return new Set(db().prepare(`SELECT match_tx_id FROM voucher_inbox WHERE match_tx_id != '' AND status IN ('attached','proposed','waiting_registration')`)
    .all().map(r => r.match_tx_id));
}
