/**
 * site-contact — コーポレートサイト問い合わせの永続化 (専用DB: DATA_DIR/site-contact.db)
 * 冪等キー=受付ID (siteのPages Functionsが発行するulid)。INSERT OR IGNOREで重複配送を無害化する
 * (設計正本=AI_reference『ブログ自動化_サイト建て替え_20260731\設計書_v1.3』§11)。
 */
import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'site-contact.db');

let db = null;

export function getContactDB() {
  if (db) return db;
  fs.mkdirSync(DATA_DIR, { recursive: true });
  db = new Database(DB_FILE);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  db.exec(`CREATE TABLE IF NOT EXISTS inquiries (
    id           TEXT PRIMARY KEY,
    category     TEXT NOT NULL,
    name         TEXT NOT NULL,
    email        TEXT NOT NULL,
    message      TEXT NOT NULL,
    client_ip    TEXT,
    site_sent_at TEXT,
    received_at  TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_inq_received ON inquiries(received_at)');
  return db;
}

/** 挿入。既存idなら何もしない (冪等)。戻り値: true=新規 / false=重複 */
export function insertInquiry(row) {
  const d = getContactDB();
  const res = d
    .prepare(
      `INSERT OR IGNORE INTO inquiries (id, category, name, email, message, client_ip, site_sent_at, received_at)
       VALUES (@id, @category, @name, @email, @message, @client_ip, @site_sent_at, @received_at)`
    )
    .run(row);
  return res.changes === 1;
}

export function recentInquiries(limit = 50) {
  return getContactDB()
    .prepare('SELECT id, category, name, email, message, received_at FROM inquiries ORDER BY received_at DESC LIMIT ?')
    .all(limit);
}
