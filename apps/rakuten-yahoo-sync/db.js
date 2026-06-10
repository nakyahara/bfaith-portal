/**
 * RakutenYahooSync (RYS) SQLite (better-sqlite3) ラッパー + migration runner。
 *
 * 設計原則 (Codex Phase E R3-R4 確定):
 *   - 専用 DB (rakuten-yahoo-sync.db) を持つ (warehouse-mirror.db との分離)
 *   - migration は db/migrations/NNN_*.sql を user_version で順次 apply
 *   - foreign_keys / WAL は接続時に有効化
 *
 * warehouse-mirror.db への参照は専用 helper (getMirrorReadonlyDB) で
 *   {readonly: true, fileMustExist: true} を強制 (physical read-only)。
 *   Codex R3 H-2 / R4 で明示された安全策。
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const DB_FILE = process.env.RYS_DB_FILE || path.join(DATA_DIR, 'rakuten-yahoo-sync.db');
const MIGRATIONS_DIR = path.join(__dirname, 'db', 'migrations');

let _db = null;

/**
 * migrations フォルダから NNN_*.sql を昇順で集める。
 *   NNN は migration version (3 桁ゼロ埋め推奨だが parseInt で許容)。
 *
 * Codex E-2 R1 Low-1: 同 version の重複 file は起動時 throw で fail-closed。
 *   例: 001_initial.sql と 001_legacy.sql が両方あると、 どちらが「正」 か不定で
 *       挙動が緩い (両方 apply される) → 静かに schema 競合する可能性。
 */
function listMigrationFiles() {
  if (!fs.existsSync(MIGRATIONS_DIR)) return [];
  const entries = fs.readdirSync(MIGRATIONS_DIR)
    .filter((f) => /^\d+_.+\.sql$/.test(f))
    .map((f) => ({
      file: f,
      version: parseInt(f.split('_')[0], 10),
      path: path.join(MIGRATIONS_DIR, f),
    }))
    .filter((e) => Number.isFinite(e.version));
  entries.sort((a, b) => a.version - b.version);
  // 重複 version 検出
  const seen = new Map();
  for (const e of entries) {
    if (seen.has(e.version)) {
      throw new Error(
        `[rys-migrate] duplicate migration version ${e.version}: ${seen.get(e.version)} vs ${e.file}`
      );
    }
    seen.set(e.version, e.file);
  }
  return entries;
}

/**
 * user_version より新しい migration を順次 apply する。
 *   - 1 migration を 1 transaction で実行
 *   - 失敗時は throw、 user_version は更新しない
 *   - apply 後 user_version を migration version に更新
 */
export function runMigrations(db, { verbose = false } = {}) {
  const files = listMigrationFiles();
  if (files.length === 0) return { applied: [], finalVersion: 0 };
  const before = db.pragma('user_version', { simple: true });
  const applied = [];
  for (const m of files) {
    if (m.version <= before) continue;
    const sql = fs.readFileSync(m.path, 'utf8');
    const tx = db.transaction(() => {
      db.exec(sql);
      db.pragma(`user_version = ${m.version}`);
    });
    try {
      tx();
      applied.push(m.file);
      if (verbose) console.log(`[rys-migrate] applied ${m.file}`);
    } catch (e) {
      const wrapped = new Error(`[rys-migrate] ${m.file} failed: ${e.message}`);
      wrapped.cause = e;
      throw wrapped;
    }
  }
  const finalVersion = db.pragma('user_version', { simple: true });
  return { applied, finalVersion };
}

function openConnection(dbPath) {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');
  db.pragma('temp_store = MEMORY');
  return db;
}

/**
 * 共有 DB connection を取得 (初回 open + migration 実行)。
 *   - 同 process 内 singleton (sync-lock とは別レイヤー)
 *   - runMigrations は冪等なので毎回呼び出して OK
 */
export function getDB({ verbose = false } = {}) {
  if (_db) return _db;
  _db = openConnection(DB_FILE);
  runMigrations(_db, { verbose });
  return _db;
}

export function closeDB() {
  if (_db) {
    _db.close();
    _db = null;
  }
}

export function getCurrentDBPath() {
  return DB_FILE;
}

/**
 * warehouse-mirror.db への読み取り専用接続。
 *   Codex Phase E R3 High-2 / R4 反映:
 *   - {readonly: true, fileMustExist: true} を物理的に強制 (慣習に頼らない)
 *   - mirror 経路は通常 getMirrorDB() (RW connection) と別 instance を持つ
 *   - 開く側 (RYS) は close 責任あり
 */
export function openMirrorReadonly() {
  const mirrorPath = process.env.WAREHOUSE_MIRROR_DB_FILE
    || path.join(DATA_DIR, 'warehouse-mirror.db');
  return new Database(mirrorPath, { readonly: true, fileMustExist: true });
}
