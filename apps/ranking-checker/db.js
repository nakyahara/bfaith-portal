/**
 * 楽天順位チェッカー SQLite ラッパー
 *
 * Phase1 R2 変更 (Codexレビュー反映):
 *   - products に client_id (UI UUID) を追加、商品同一性の唯一キーとする
 *     → keyword / product_code を編集しても履歴を失わない
 *   - rank列に CHECK (NULL or -1 or 1..100)
 *   - run_log に FK (run_id ON DELETE CASCADE, product_id ON DELETE SET NULL)
 *   - stale running を failed に遷移する markStaleRunning()
 *   - unchecked ID を事前 materialize する helper を追加
 *     （iterator を API 待ちで保持しない）
 *   - review_count UPDATE を直接代入 (null への戻しを許容)
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const DB_FILE = process.env.RANKCHECK_DB_FILE || path.join(DATA_DIR, 'ranking-checker.db');

/**
 * スキーマバージョン。schemaを変えるたびに +1 し、initSchema も追従させる。
 * 旧バージョンのDBを誤って開いた場合は openDb() が abort する。
 */
const SCHEMA_VERSION = 1;

let _db = null;
let _stmts = null;
let _expectedFingerprint = null;

/**
 * sqlite_master の CREATE 文から schema fingerprint を計算する。
 * user_version 一致でも列欠落・CHECK違い等の drift が起きるため、
 * fingerprint で同世代内の破損も検出する。
 */
function schemaFingerprint(db) {
  const rows = db.prepare(
    `SELECT type, name, sql FROM sqlite_master
      WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'
        AND sql IS NOT NULL
      ORDER BY type, name`
  ).all();
  // 空白を1つにまとめて機械差分を吸収
  const joined = rows.map(r => `${r.type}:${r.name}:${r.sql.replace(/\s+/g, ' ').trim()}`).join('\n');
  return crypto.createHash('sha1').update(joined).digest('hex');
}

function expectedFingerprint() {
  if (_expectedFingerprint) return _expectedFingerprint;
  const mem = new Database(':memory:');
  mem.pragma('foreign_keys = ON');
  initSchema(mem);
  _expectedFingerprint = schemaFingerprint(mem);
  mem.close();
  return _expectedFingerprint;
}

function openDb() {
  if (_db) return _db;
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const db = new Database(DB_FILE);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');

  // CREATE TABLE IF NOT EXISTS では旧バージョンのテーブルが無改造で残るため、
  // user_version で明示的にスキーマ世代を管理し、ミスマッチなら停止する。
  const productsExists = !!db.prepare(
    `SELECT name FROM sqlite_master WHERE type='table' AND name='products'`
  ).get();
  const currentVersion = db.pragma('user_version', { simple: true });

  if (!productsExists) {
    // 新規DB: 最新スキーマを作成してバージョン刻印
    initSchema(db);
    db.pragma(`user_version = ${SCHEMA_VERSION}`);
  } else if (currentVersion === SCHEMA_VERSION) {
    // 同世代: IF NOT EXISTS は冪等なので念のため再適用
    initSchema(db);
  } else {
    // 旧世代 or 未刻印: 暗黙のデータ破壊を避け、運用に判断を委ねる
    db.close();
    throw new Error(
      `[rankcheck db] schema version mismatch: file=${currentVersion}, code=${SCHEMA_VERSION}. ` +
      `migrateまたは ${DB_FILE} のリネーム/削除を実施してください。`
    );
  }

  // fingerprint 検査: 同世代でも列欠落や CHECK差異を検出する
  const actual = schemaFingerprint(db);
  const expected = expectedFingerprint();
  if (actual !== expected) {
    db.close();
    throw new Error(
      `[rankcheck db] schema fingerprint mismatch: actual=${actual.slice(0, 8)}, expected=${expected.slice(0, 8)}. ` +
      `手動変更か旧コード由来の drift の可能性があります。${DB_FILE} を確認してください。`
    );
  }

  _db = db;
  _stmts = prepareStatements(db);
  return db;
}

function initSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS products (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id       TEXT    NOT NULL UNIQUE,
      keyword         TEXT    NOT NULL,
      product_code    TEXT,
      own_url         TEXT,
      yahoo_url       TEXT,
      amazon_url      TEXT,
      amazon_asin     TEXT,
      competitor1_url TEXT,
      competitor2_url TEXT,
      review_count    INTEGER,
      created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
      updated_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now'))
    );

    CREATE TABLE IF NOT EXISTS rank_history (
      product_id       INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
      date             TEXT    NOT NULL,
      own_rank         INTEGER CHECK (own_rank         IS NULL OR own_rank         = -1 OR (own_rank         BETWEEN 1 AND 100)),
      competitor1_rank INTEGER CHECK (competitor1_rank IS NULL OR competitor1_rank = -1 OR (competitor1_rank BETWEEN 1 AND 100)),
      competitor2_rank INTEGER CHECK (competitor2_rank IS NULL OR competitor2_rank = -1 OR (competitor2_rank BETWEEN 1 AND 100)),
      yahoo_own_rank   INTEGER CHECK (yahoo_own_rank   IS NULL OR yahoo_own_rank   = -1 OR (yahoo_own_rank   BETWEEN 1 AND 100)),
      amazon_own_rank  INTEGER CHECK (amazon_own_rank  IS NULL OR amazon_own_rank  = -1 OR (amazon_own_rank  BETWEEN 1 AND 100)),
      checked_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
      PRIMARY KEY (product_id, date)
    );

    CREATE INDEX IF NOT EXISTS ix_rank_history_date ON rank_history(date);

    CREATE TABLE IF NOT EXISTS run_state (
      run_id     TEXT    PRIMARY KEY,
      started_at TEXT    NOT NULL,
      ended_at   TEXT,
      total      INTEGER NOT NULL DEFAULT 0,
      done       INTEGER NOT NULL DEFAULT 0,
      status     TEXT    NOT NULL,
      error      TEXT
    );

    CREATE TABLE IF NOT EXISTS run_log (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id     TEXT    NOT NULL REFERENCES run_state(run_id) ON DELETE CASCADE,
      product_id INTEGER          REFERENCES products(id) ON DELETE SET NULL,
      ts         TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
      level      TEXT    NOT NULL,
      msg        TEXT    NOT NULL
    );

    CREATE INDEX IF NOT EXISTS ix_run_log_run ON run_log(run_id);
  `);
}

function prepareStatements(db) {
  return {
    countProducts: db.prepare(`SELECT COUNT(*) AS n FROM products`),

    getProductById: db.prepare(`
      SELECT id, client_id, keyword, product_code, own_url, yahoo_url, amazon_url, amazon_asin,
             competitor1_url, competitor2_url, review_count, created_at, updated_at
        FROM products WHERE id = ?
    `),
    getProductByClientId: db.prepare(`
      SELECT id FROM products WHERE client_id = ?
    `),
    insertProduct: db.prepare(`
      INSERT INTO products
        (client_id, keyword, product_code, own_url, yahoo_url, amazon_url, amazon_asin,
         competitor1_url, competitor2_url, review_count)
      VALUES (@client_id, @keyword, @product_code, @own_url, @yahoo_url, @amazon_url, @amazon_asin,
              @competitor1_url, @competitor2_url, @review_count)
    `),
    updateProduct: db.prepare(`
      UPDATE products
         SET keyword = @keyword,
             product_code = @product_code,
             own_url = @own_url,
             yahoo_url = @yahoo_url,
             amazon_url = @amazon_url,
             amazon_asin = @amazon_asin,
             competitor1_url = @competitor1_url,
             competitor2_url = @competitor2_url,
             review_count = @review_count,
             updated_at = strftime('%Y-%m-%d %H:%M:%S', 'now')
       WHERE id = @id
    `),
    updateReviewCount: db.prepare(`
      UPDATE products
         SET review_count = @review_count,
             updated_at = strftime('%Y-%m-%d %H:%M:%S', 'now')
       WHERE id = @id
    `),
    updateProductCode: db.prepare(`
      UPDATE products
         SET product_code = @product_code,
             updated_at = strftime('%Y-%m-%d %H:%M:%S', 'now')
       WHERE id = @id
    `),
    deleteProduct: db.prepare(`DELETE FROM products WHERE id = ?`),

    getHistory: db.prepare(`
      SELECT date, own_rank, competitor1_rank, competitor2_rank,
             yahoo_own_rank, amazon_own_rank, checked_at
        FROM rank_history
       WHERE product_id = ?
       ORDER BY date ASC
    `),
    getLatestHistory: db.prepare(`
      SELECT date, own_rank, competitor1_rank, competitor2_rank,
             yahoo_own_rank, amazon_own_rank, checked_at
        FROM rank_history
       WHERE product_id = ?
       ORDER BY date DESC
       LIMIT 1
    `),
    hasHistoryForDate: db.prepare(`
      SELECT 1 FROM rank_history WHERE product_id = ? AND date = ?
    `),
    upsertHistory: db.prepare(`
      INSERT INTO rank_history
        (product_id, date, own_rank, competitor1_rank, competitor2_rank,
         yahoo_own_rank, amazon_own_rank, checked_at)
      VALUES (@product_id, @date, @own_rank, @competitor1_rank, @competitor2_rank,
              @yahoo_own_rank, @amazon_own_rank, strftime('%Y-%m-%d %H:%M:%S', 'now'))
      ON CONFLICT(product_id, date) DO UPDATE SET
        own_rank         = excluded.own_rank,
        competitor1_rank = excluded.competitor1_rank,
        competitor2_rank = excluded.competitor2_rank,
        yahoo_own_rank   = excluded.yahoo_own_rank,
        amazon_own_rank  = excluded.amazon_own_rank,
        checked_at       = excluded.checked_at
    `),
    deleteOldHistory: db.prepare(`DELETE FROM rank_history WHERE date < ?`),

    // 事前に ID のみ取得するクエリ。戻り値は最大 3566 要素の ID 配列で負荷軽微。
    listUncheckedIdsForDate: db.prepare(`
      SELECT p.id
        FROM products p
       WHERE NOT EXISTS (
         SELECT 1 FROM rank_history h WHERE h.product_id = p.id AND h.date = ?
       )
       ORDER BY p.id ASC
    `),
    listAllIds: db.prepare(`SELECT id FROM products ORDER BY id ASC`),
    iterAllProductsStream: db.prepare(`
      SELECT id, client_id, keyword, product_code, own_url, yahoo_url, amazon_url, amazon_asin,
             competitor1_url, competitor2_url, review_count
        FROM products
       ORDER BY id ASC
    `),

    insertRun: db.prepare(`
      INSERT INTO run_state (run_id, started_at, total, done, status)
      VALUES (@run_id, strftime('%Y-%m-%d %H:%M:%f', 'now'), @total, 0, 'running')
    `),
    updateRunProgress: db.prepare(`
      UPDATE run_state SET done = @done WHERE run_id = @run_id
    `),
    finishRun: db.prepare(`
      UPDATE run_state
         SET ended_at = strftime('%Y-%m-%d %H:%M:%S', 'now'),
             status = @status,
             error = @error
       WHERE run_id = @run_id
    `),
    markStaleRunning: db.prepare(`
      UPDATE run_state
         SET status = 'failed',
             error = COALESCE(error, '') || 'marked stale at next startRun',
             ended_at = strftime('%Y-%m-%d %H:%M:%S', 'now')
       WHERE status = 'running'
    `),
    getRunningRun: db.prepare(`
      SELECT run_id, started_at, total, done FROM run_state WHERE status = 'running' LIMIT 1
    `),
    getLatestRun: db.prepare(`
      SELECT run_id, started_at, ended_at, total, done, status, error
        FROM run_state ORDER BY started_at DESC, run_id DESC LIMIT 1
    `),
    insertLog: db.prepare(`
      INSERT INTO run_log (run_id, product_id, level, msg) VALUES (?, ?, ?, ?)
    `),
    deleteOldRuns: db.prepare(`
      DELETE FROM run_state
       WHERE started_at < datetime('now', '-' || ? || ' days')
    `),
    deleteOldLogs: db.prepare(`
      DELETE FROM run_log
       WHERE ts < datetime('now', '-' || ? || ' days')
    `),
  };
}

function s() {
  if (!_stmts) openDb();
  return _stmts;
}

// ── Products ──

export function countProducts() {
  openDb();
  return s().countProducts.get().n;
}

export function getProduct(id) {
  openDb();
  return s().getProductById.get(id) || null;
}

export function getProductIdByClientId(clientId) {
  openDb();
  const row = s().getProductByClientId.get(clientId);
  return row ? row.id : null;
}

/**
 * client_id (UUID) を唯一の同一性として products を upsert する。
 * clientId を呼び出し側が必ず指定。新規商品なら UI または migrate で UUID 生成済み。
 */
export function upsertProduct(p) {
  openDb();
  if (!p.client_id) throw new Error('upsertProduct requires client_id');
  const existing = s().getProductByClientId.get(p.client_id);
  const params = {
    client_id: p.client_id,
    keyword: p.keyword || '',
    product_code: p.product_code || null,
    own_url: p.own_url || null,
    yahoo_url: p.yahoo_url || null,
    amazon_url: p.amazon_url || null,
    amazon_asin: p.amazon_asin || null,
    competitor1_url: p.competitor1_url || null,
    competitor2_url: p.competitor2_url || null,
    review_count: p.review_count != null ? p.review_count : null,
  };
  if (existing) {
    s().updateProduct.run({ id: existing.id, ...params });
    return existing.id;
  }
  const info = s().insertProduct.run(params);
  return info.lastInsertRowid;
}

export function updateProductReviewCount(id, review_count) {
  openDb();
  s().updateReviewCount.run({ id, review_count });
}

export function updateProductCode(id, product_code) {
  openDb();
  s().updateProductCode.run({ id, product_code });
}

export function deleteProduct(id) {
  openDb();
  s().deleteProduct.run(id);
}

// ── History ──

export function getHistory(productId) {
  openDb();
  return s().getHistory.all(productId);
}

export function getLatestHistory(productId) {
  openDb();
  return s().getLatestHistory.get(productId) || null;
}

export function hasHistoryForDate(productId, date) {
  openDb();
  return !!s().hasHistoryForDate.get(productId, date);
}

/**
 * 1商品×1日分の UPSERT。値:
 *   - 順位: 1..100
 *   - 圏外: null
 *   - APIエラー: -1
 */
export function upsertHistory(row) {
  openDb();
  const norm = {
    product_id: row.product_id,
    date: row.date,
    own_rank: row.own_rank != null ? row.own_rank : null,
    competitor1_rank: row.competitor1_rank != null ? row.competitor1_rank : null,
    competitor2_rank: row.competitor2_rank != null ? row.competitor2_rank : null,
    yahoo_own_rank: row.yahoo_own_rank != null ? row.yahoo_own_rank : null,
    amazon_own_rank: row.amazon_own_rank != null ? row.amazon_own_rank : null,
  };
  s().upsertHistory.run(norm);
}

export function cleanupOldHistory(days) {
  openDb();
  const cutoff = jstDateOffset(-days);
  const info = s().deleteOldHistory.run(cutoff);
  return info.changes;
}

function jstDateOffset(daysDelta) {
  const now = new Date(Date.now() + 9 * 60 * 60 * 1000);
  now.setUTCDate(now.getUTCDate() + daysDelta);
  return now.toISOString().slice(0, 10);
}

// ── ID list materialize (iterator + API 待ちの同時開き回避) ──

export function listUncheckedIdsForDate(date) {
  openDb();
  return s().listUncheckedIdsForDate.all(date).map(r => r.id);
}

export function listAllIds() {
  openDb();
  return s().listAllIds.all().map(r => r.id);
}

// ── 全件ストリーミング (UI legacy / CSV から利用) ──

export function* iterAllProducts() {
  openDb();
  yield* s().iterAllProductsStream.iterate();
}

// ── Runs ──

let _runSeq = 0;
export function startRun(total) {
  openDb();
  // 17桁(YYYYMMDDHHMMSSmmm) + 4桁シーケンスで同プロセス内単調増加を保証
  const now = new Date();
  const iso = now.toISOString().replace(/[-T:.]/g, '').slice(0, 17);
  _runSeq = (_runSeq + 1) % 10000;
  const runId = `${iso}-${String(_runSeq).padStart(4, '0')}`;
  s().insertRun.run({ run_id: runId, total });
  return runId;
}

export function markStaleRunning() {
  openDb();
  return s().markStaleRunning.run().changes;
}

export function updateRunProgress(runId, done) {
  openDb();
  s().updateRunProgress.run({ run_id: runId, done });
}

export function finishRun(runId, status, error) {
  openDb();
  s().finishRun.run({ run_id: runId, status, error: error || null });
}

export function getRunningRun() {
  openDb();
  return s().getRunningRun.get() || null;
}

export function getLatestRun() {
  openDb();
  return s().getLatestRun.get() || null;
}

export function logRun(runId, level, msg, productId = null) {
  openDb();
  s().insertLog.run(runId, productId, level, String(msg).slice(0, 2000));
}

export function cleanupOldRunMeta(days) {
  openDb();
  const runs = s().deleteOldRuns.run(days).changes;
  const logs = s().deleteOldLogs.run(days).changes;
  return { runs, logs };
}

// ── UI互換: { products: [{ id(=client_id), ...master, history: [...] }] } ──

export function exportLegacyShape() {
  openDb();
  const products = [];
  for (const p of s().iterAllProductsStream.iterate()) {
    const history = s().getHistory.all(p.id).map(h => {
      // DB の -1 は UI 互換で 'error' 文字列に戻す。UI が rank を文字列で扱う前提のため。
      const entry = { date: h.date };
      if (h.own_rank != null) entry.own_rank = h.own_rank === -1 ? 'error' : h.own_rank;
      if (h.competitor1_rank != null) entry.competitor1_rank = h.competitor1_rank === -1 ? 'error' : h.competitor1_rank;
      if (h.competitor2_rank != null) entry.competitor2_rank = h.competitor2_rank === -1 ? 'error' : h.competitor2_rank;
      if (h.yahoo_own_rank != null) entry.yahoo_own_rank = h.yahoo_own_rank === -1 ? 'error' : h.yahoo_own_rank;
      if (h.amazon_own_rank != null) entry.amazon_own_rank = h.amazon_own_rank === -1 ? 'error' : h.amazon_own_rank;
      return entry;
    });
    products.push({
      id: p.client_id,
      keyword: p.keyword,
      product_code: p.product_code || '',
      own_url: p.own_url || '',
      yahoo_url: p.yahoo_url || '',
      amazon_url: p.amazon_url || '',
      amazon_asin: p.amazon_asin || '',
      competitor1_url: p.competitor1_url || '',
      competitor2_url: p.competitor2_url || '',
      review_count: p.review_count,
      history,
    });
  }
  return { products };
}

// ── utils ──

export function generateClientId() {
  return typeof crypto.randomUUID === 'function' ? crypto.randomUUID() : 'rid-' + crypto.randomBytes(16).toString('hex');
}

export function getDb() {
  openDb();
  return _db;
}

export function closeDb() {
  if (_db) { _db.close(); _db = null; _stmts = null; }
}

export { DB_FILE };
