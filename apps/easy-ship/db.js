/**
 * easy-ship (Amazon Easy Ship 梱包サイズマスター) の DB 層。
 *
 * 設計 (shipping-work の規約に準拠):
 * - アプリ専用の easy-ship.db (better-sqlite3 / WAL)
 * - スキーマは PRAGMA user_version で版管理。CREATE IF NOT EXISTS 頼みにしない
 * - 日時カラムは UTC 'YYYY-MM-DDTHH:MM:SSZ'
 * - テーブル名は es_ プレフィクス
 * - SKU は大文字小文字違いの併存を DB レベルで禁止 (LOWER(sku) の一意インデックス)。
 *   照合の大小区別は env EASY_SHIP_SKU_CASE_INSENSITIVE で切替 (既定: 区別する)
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

export const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'easy-ship.db');

const SCHEMA_VERSION = 2;

let db = null;

export function utcNow() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

export function initEasyShipDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) {
    try {
      db.close();
    } catch {}
    db = null;
  }
  db = new Database(DB_FILE);
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  migrate();
  return db;
}

export function getDB() {
  if (!db) throw new Error('easy-ship DB が初期化されていません (initEasyShipDB を先に呼ぶ)');
  return db;
}

function migrate() {
  let v = db.pragma('user_version', { simple: true });
  if (v > SCHEMA_VERSION) {
    throw new Error(
      `easy-ship.db の user_version=${v} がコードの期待 ${SCHEMA_VERSION} より新しい (古いコードで起動している)`,
    );
  }
  while (v < SCHEMA_VERSION) {
    const next = v + 1;
    const step = MIGRATIONS[next];
    if (!step) throw new Error(`easy-ship migration v${next} が未定義`);
    db.transaction(() => {
      // Render のデプロイは新旧プロセスが重なるため、ロックを取ってから版数を読み直す
      if (db.pragma('user_version', { simple: true }) >= next) return;
      step();
      db.pragma(`user_version = ${next}`);
    }).immediate();
    const applied = db.pragma('user_version', { simple: true });
    if (applied < next) {
      throw new Error(`easy-ship migration v${next} 適用後も user_version が ${applied} のまま`);
    }
    v = applied;
  }
}

function createCoreTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS es_package_size_master (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku TEXT NOT NULL,
      package_size_code TEXT NOT NULL,
      package_size_label TEXT NOT NULL,
      amazon_option_value TEXT NOT NULL DEFAULT '',
      is_active INTEGER NOT NULL DEFAULT 1,
      note TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    -- 大小文字違いのSKU併存を禁止 (照合設定に関わらず。誤選択・レース条件防止)
    CREATE UNIQUE INDEX IF NOT EXISTS es_psm_sku_lower_uq
      ON es_package_size_master (LOWER(sku));

    CREATE TABLE IF NOT EXISTS es_operation_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku TEXT,
      action TEXT NOT NULL,
      result TEXT NOT NULL,
      message TEXT,
      user_identifier TEXT,
      browser_identifier TEXT,
      page_url TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS es_logs_created_idx ON es_operation_logs (created_at);
  `);
}

function createComboTable() {
  // 組み合わせマスター (数量2以上・同梱注文用)。
  // combo_key = 注文構成の正規化キー: 小文字sku*数量 を辞書順に '|' 連結 (例 "a-1*2|b-2*1")。
  // 適用は構成の完全一致のみ (部分一致・類似構成には適用しない)
  db.exec(`
    CREATE TABLE IF NOT EXISTS es_combo_size_master (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      combo_key TEXT NOT NULL,
      items_json TEXT NOT NULL,
      package_size_label TEXT NOT NULL,
      amazon_option_value TEXT NOT NULL DEFAULT '',
      is_active INTEGER NOT NULL DEFAULT 1,
      note TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS es_csm_key_uq ON es_combo_size_master (combo_key);
  `);
}

const MIGRATIONS = {
  1: createCoreTables,
  2: createComboTable,
};
