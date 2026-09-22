/**
 * 設定値をSQLiteで管理するモジュール。
 * sql.js（Pure JS SQLite）を使用。
 */
import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
// 🚨 ほかのプロセスが書いた mercari-settings.db を黙って上書きしない歯止め (fba.db で 2026-09-20 に起きた「後から保存した側が相手の行を消す」事故と同じ形)
import { loadGuarded, saveGuarded } from '../../lib/sqljs-guard.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const DB_FILE = process.env.MERCARI_DB_PATH || path.join(DATA_DIR, 'mercari-settings.db');

let db = null;
let SQLMod = null;   // initSqlJs() の結果 (外から書き換えられたファイルを読み直すのに使う)
let gen = null;      // このプロセスが最後に「読んだ / 書いた」時点のファイルの世代 (lib/sqljs-guard.js。中は見ない)

export async function initDb() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const SQL = await initSqlJs();
  SQLMod = SQL;
  // lock の中で読む (相手が書いている途中のファイルを読まない)。書きかけのファイルは読まずに失敗 = 控えから戻す
  const loaded = loadGuarded({ file: DB_FILE, SQL });
  db = loaded.db;
  gen = loaded.gen;

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

/**
 * メモリの DB をファイルへ書き戻す (sql.js = **ファイル全体** を書く)。
 * 🚨 読んだ後にほかのプロセスがファイルを書き換えていたら **上書きしない** (SQLJS_DB_EXTERNAL_WRITE)。上書きすると相手の行がファイルごと消える。
 *    そのときはファイルを読み直して (= このプロセスの未保存の変更は捨てる。読み直さないと以後の保存が全部はじかれ続ける) 例外を投げる。
 *    呼び出し元 (router の API) は失敗を返し、やり直せば (読み直した後なので) 通る。ここで必ず警告を出す (握りつぶされても記録は残る)
 */
function saveToFile() {
  if (!db) return;
  ensureSynced('save');
  try {
    gen = saveGuarded({ file: DB_FILE, db, gen });
  } catch (e) {
    console.warn(`[mercari-settings] 🚨 ${path.basename(DB_FILE)} を保存していない (${e.code || ''}): ${e.message}`);
    // 上書きしなかった = 読み直す。ほかの失敗 (lock・I/O) = メモリに未保存の変更が残る → 読み直すまで使わない (needsReload)
    if (e.code === 'SQLJS_DB_EXTERNAL_WRITE') reloadFromFile(); else needsReload = true;
    throw e;
  }
}

// 🚨 保存に失敗したのに読み直せていない = メモリに未保存の変更が残っている状態。その間は検索にも保存にも使わせない (次の操作で読み直す。Codex #1407 R3)
let needsReload = false;
function ensureSynced(op) {
  if (!needsReload) return;
  if (reloadFromFile()) {
    if (op === 'save') throw Object.assign(new Error(`${path.basename(DB_FILE)} を読み直したので、この変更は保存していない = もう一度実行する`), { code: 'SQLJS_DB_NEEDS_RELOAD' });
    return;
  }
  throw Object.assign(new Error(`${path.basename(DB_FILE)} を読み直せていない (前の保存と読み直しが失敗したまま) = 復旧するまで検索・保存に使わない。もう一度実行する`), { code: 'SQLJS_DB_NEEDS_RELOAD' });
}

/** 外から書き換えられたファイルを読み直す (このプロセスの未保存の変更は捨てる) → true。読み直せなければ false (needsReload を立てる。元の例外は隠さない) */
function reloadFromFile() {
  try {
    const r = loadGuarded({ file: DB_FILE, SQL: SQLMod });
    const old = db;
    db = r.db;
    gen = r.gen;
    needsReload = false;
    try { old?.close(); } catch { /* 閉じられなくても新しい側は使える */ }
    console.warn(`[mercari-settings] ${path.basename(DB_FILE)} を読み直した (未保存の変更は捨てた)。もう一度実行する`);
    return true;
  } catch (e) {
    needsReload = true;
    console.warn(`[mercari-settings] 🚨 ${path.basename(DB_FILE)} を読み直せない (${e.code || ''}): ${e.message} = 復旧するまで検索・保存は SQLJS_DB_NEEDS_RELOAD で止まる`);
    return false;
  }
}

export function getConfig(key, defaultValue = '') {
  ensureSynced('read');
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
  ensureSynced('read');
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
  ensureSynced('read');
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
  ensureSynced('read');
  return getConfig('operation_mode', 'csv');
}

// --- 登録済み商品管理 ---

export function getRegisteredItems() {
  ensureSynced('read');
  const rows = db.exec('SELECT item_code FROM registered_items');
  if (rows.length === 0) return new Set();
  return new Set(rows[0].values.map(r => r[0]));
}

export function getRegisteredItemCount() {
  ensureSynced('read');
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
  ensureSynced('read');
  const raw = getConfig('excluded_items', '');
  if (!raw.trim()) return new Set();
  return new Set(raw.split('\n').map(l => l.trim()).filter(Boolean));
}

export function getExcludedImagePositions() {
  ensureSynced('read');
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
  ensureSynced('read');
  const raw = getConfig('excluded_image_patterns', '');
  if (!raw.trim()) return [];
  return raw.split('\n').map(l => l.trim()).filter(Boolean);
}
