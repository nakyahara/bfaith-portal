/**
 * ABAキーワード DB — セラースプライト置換 (注文ワード)
 *
 * Amazonブランド分析「Amazon検索用語 (Top Search Terms)」の週次レポートを蓄積し、
 * Chrome拡張からの ASIN 逆引きに応える。warehouse.db とは分離した専用DB
 * (数百万行/週 になり得るため。warehouse.db のバックアップ時間にも影響させない)。
 *
 * 保持方針:
 *   - 直近 ABA_KEEP_WEEKS 週 (既定26) は全検索語を保持 → 任意ASINの逆引きが可能
 *   - それより古い週は「一度でも拡張から照会された ASIN (aba_watch_asins) が
 *     上位3クリックに含まれる検索語」だけ残す → 定点観測ASINは履歴が消えない
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'aba.db');

let db = null;

export function initAbaDB() {
  if (db) return db;
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  db = new Database(DB_FILE);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 10000');
  createTables();
  return db;
}

export function getAbaDB() {
  if (!db) throw new Error('aba.db 未初期化 (initAbaDB を先に呼ぶ)');
  return db;
}

export function closeAbaDB() {
  if (db) { try { db.close(); } catch { /* 既closeは無視 */ } db = null; }
}

function createTables() {
  // 週の取込台帳 (冪等キー)。row_count>0 の週は取込済みとして再取得しない
  db.exec(`
    CREATE TABLE IF NOT EXISTS aba_weeks (
      week_start TEXT PRIMARY KEY,   -- YYYY-MM-DD (日曜, JST)
      week_end   TEXT NOT NULL,      -- YYYY-MM-DD (土曜)
      ingested_at TEXT NOT NULL,
      term_count INTEGER NOT NULL DEFAULT 0,
      row_count  INTEGER NOT NULL DEFAULT 0
    );
  `);

  // 1行 = (週, 検索語, クリック順位1..3)。ABAレポートの生データ
  db.exec(`
    CREATE TABLE IF NOT EXISTS aba_search_terms (
      week_start TEXT NOT NULL,
      department TEXT NOT NULL DEFAULT '',
      search_term TEXT NOT NULL,
      search_frequency_rank INTEGER NOT NULL,
      click_position INTEGER NOT NULL,   -- clickShareRank (1..3)
      asin TEXT NOT NULL,
      product_title TEXT,
      click_share REAL,
      conversion_share REAL,
      PRIMARY KEY (week_start, department, search_term, click_position)
    );
    CREATE INDEX IF NOT EXISTS idx_aba_terms_asin
      ON aba_search_terms (asin, week_start);
  `);

  // 拡張から照会された ASIN = 定点観測対象 (古い週の prune から除外)
  db.exec(`
    CREATE TABLE IF NOT EXISTS aba_watch_asins (
      asin TEXT PRIMARY KEY,
      first_queried_at TEXT NOT NULL,
      last_queried_at TEXT NOT NULL,
      query_count INTEGER NOT NULL DEFAULT 0
    );
  `);

  // 検索結果ページ用: SP-API Catalog Items のキャッシュ (BSR/梱包/ブランド)
  db.exec(`
    CREATE TABLE IF NOT EXISTS aba_catalog_cache (
      asin TEXT PRIMARY KEY,
      payload TEXT NOT NULL,       -- 正規化済みJSON
      fetched_at TEXT NOT NULL
    );
  `);

  // 出品者 (buy box) キャッシュ — ABA_EXT_OFFERS=1 のときだけ使う
  db.exec(`
    CREATE TABLE IF NOT EXISTS aba_offers_cache (
      asin TEXT PRIMARY KEY,
      payload TEXT NOT NULL,
      fetched_at TEXT NOT NULL
    );
  `);
}
