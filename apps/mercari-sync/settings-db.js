/**
 * 設定値をSQLiteで管理するモジュール。
 * sql.js（Pure JS SQLite）を使用。
 */
import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_FILE = process.env.MERCARI_DB_PATH || path.join(__dirname, 'settings.db');

let db = null;

export async function initDb() {
  const SQL = await initSqlJs();
  if (fs.existsSync(DB_FILE)) {
    const buf = fs.readFileSync(DB_FILE);
    db = new SQL.Database(buf);
  } else {
    db = new SQL.Database();
  }

  db.run(`
    CREATE TABLE IF NOT EXISTS config (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS category_mapping (
      rakuten_genre_id  TEXT PRIMARY KEY,
      mercari_category  TEXT NOT NULL
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS registered_items (
      item_code     TEXT PRIMARY KEY,
      registered_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
      source        TEXT NOT NULL DEFAULT 'manual'
    )
  `);
  saveToFile();
}

function saveToFile() {
  if (!db) return;
  const data = db.export();
  fs.writeFileSync(DB_FILE, Buffer.from(data));
}

export function getConfig(key, defaultValue = '') {
  const stmt = db.prepare('SELECT value FROM config WHERE key=?');
  stmt.bind([key]);
  if (stmt.step()) {
    const val = stmt.getAsObject().value;
    stmt.free();
    return val;
  }
  stmt.free();
  return defaultValue;
}

export function setConfig(key, value) {
  db.run('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)', [key, value]);
  saveToFile();
}

export function getAllConfig() {
  const result = {};
  const rows = db.exec('SELECT key, value FROM config');
  if (rows.length > 0) {
    for (const row of rows[0].values) {
      result[row[0]] = row[1];
    }
  }
  return result;
}

export function getCategoryMappings() {
  const rows = db.exec('SELECT rakuten_genre_id, mercari_category FROM category_mapping ORDER BY rakuten_genre_id');
  if (rows.length === 0) return [];
  return rows[0].values.map(r => ({ rakuten_genre_id: r[0], mercari_category: r[1] }));
}

export function saveCategoryMappings(mappings) {
  db.run('DELETE FROM category_mapping');
  for (const m of mappings) {
    db.run('INSERT INTO category_mapping (rakuten_genre_id, mercari_category) VALUES (?, ?)',
      [m.rakuten_genre_id, m.mercari_category]);
  }
  saveToFile();
}

export function getOperationMode() {
  return getConfig('operation_mode', 'csv');
}

// --- 登録済み商品管理 ---

export function getRegisteredItems() {
  const rows = db.exec('SELECT item_code FROM registered_items');
  if (rows.length === 0) return new Set();
  return new Set(rows[0].values.map(r => r[0]));
}

export function getRegisteredItemCount() {
  const rows = db.exec('SELECT COUNT(*) as cnt FROM registered_items');
  return rows.length > 0 ? rows[0].values[0][0] : 0;
}

export function addRegisteredItems(codes, source = 'manual') {
  for (const code of codes) {
    const trimmed = code.trim();
    if (trimmed) {
      db.run('INSERT OR IGNORE INTO registered_items (item_code, source) VALUES (?, ?)', [trimmed, source]);
    }
  }
  saveToFile();
}

export function clearRegisteredItems() {
  db.run('DELETE FROM registered_items');
  saveToFile();
}

// --- 除外設定 ---

export function getExcludedItems() {
  const raw = getConfig('excluded_items', '');
  if (!raw.trim()) return new Set();
  return new Set(raw.split('\n').map(l => l.trim()).filter(Boolean));
}

export function getExcludedImagePositions() {
  const raw = getConfig('excluded_image_positions', '');
  if (!raw.trim()) return new Set();
  const positions = new Set();
  for (const part of raw.replace(/、/g, ',').split(',')) {
    const n = parseInt(part.trim());
    if (!isNaN(n)) positions.add(n);
  }
  return positions;
}

export function getExcludedImagePatterns() {
  const raw = getConfig('excluded_image_patterns', '');
  if (!raw.trim()) return [];
  return raw.split('\n').map(l => l.trim()).filter(Boolean);
}
