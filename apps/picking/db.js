/**
 * picking DB — ピッキング支援システム (スマホピッキング)
 *
 * 設計書: AI_reference/システム設計/ピッキング支援システム_要件定義_20260811.md
 *         AI_reference/システム設計/ピッキング支援システム_実装計画_20260811.md
 *
 * shipping-work と同方針の専用DB (picking.db) を DATA_DIR に持つ。
 * 作業計測 (明細単位の時間) は本アプリが正本。高頻度書き込みのため mirror と分離する。
 *
 * 方針:
 *   - 日時カラムは全て UTC 'YYYY-MM-DDTHH:MM:SSZ'。work_date / instruct_date のみ JST 'YYYY-MM-DD'
 *   - 時刻はサーバー時刻を正とする (端末のオフラインキュー再送でも受信時刻ではなく
 *     イベント内の相対情報を使わない = PR2 で shown_at/done_at をサーバーが刻む)
 *   - 操作 (pk_events) は追記型。バッチ・明細の状態はそこから導出できる範囲で UPDATE する
 *   - 取消は物理削除せず validity='invalid' で残す (計測の再現性を守る)
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

export const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'picking.db');

let db = null;

/** UTC 'YYYY-MM-DDTHH:MM:SSZ' (秒精度)。本DBの日時カラムの正準形式。 */
export function utcNow() {
  return new Date().toISOString().slice(0, 19) + 'Z';
}

const JST_DATE_FORMATTER = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit',
});
/** JST 日付 'YYYY-MM-DD' (作業日用。UTC 環境でも正しく動く)。 */
export function jstToday(date = new Date()) {
  return JST_DATE_FORMATTER.format(date);
}

export const BATCH_STATUSES = ['ready', 'picking', 'paused', 'done', 'cancelled'];

export const STATUS_LABELS = {
  ready: '未着手',
  picking: 'ピッキング中',
  paused: '中断中',
  done: '完了',
  cancelled: '取消',
};

// スキーマ版数 (PRAGMA user_version)。変更時は MIGRATIONS に追記して番号を上げる。
const SCHEMA_VERSION = 1;

export function initPickingDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) { try { db.close(); } catch { /* close済み等は無視 */ } db = null; }
  db = new Database(DB_FILE);
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  migrate();
  return db;
}

/**
 * バージョン式マイグレーション (shipping-work/db.js と同規約)。
 * BEGIN IMMEDIATE でロックを取り、取得後に user_version を読み直す
 * (Render のデプロイは新旧プロセスが重なるため、外で読んだ値のまま進めると二重適用する)。
 */
function migrate() {
  let v = db.pragma('user_version', { simple: true });
  if (v > SCHEMA_VERSION) {
    throw new Error(`picking.db の user_version=${v} がコードの期待 ${SCHEMA_VERSION} より新しい (ロールバック不可)`);
  }
  while (v < SCHEMA_VERSION) {
    const next = v + 1;
    const step = MIGRATIONS[next];
    if (!step) throw new Error(`picking migration v${next} が未定義`);
    db.transaction(() => {
      if (db.pragma('user_version', { simple: true }) >= next) return;
      step();
      db.pragma(`user_version = ${next}`);
    }).immediate();
    const applied = db.pragma('user_version', { simple: true });
    if (applied < next) {
      throw new Error(`picking migration v${next}: user_version が ${applied} のまま更新されていない`);
    }
    v = applied;
  }
}

const MIGRATIONS = {
  1: createCoreTables,
};

function createCoreTables() {
  // 引当バッチ (= CS03002 の1ファイル = トータルピッキングバッチ番号 TB… 1つ)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tb_no           TEXT NOT NULL UNIQUE,   -- トータルピッキングバッチ番号 (取込の冪等キー)
    hikiate_class   TEXT NOT NULL,          -- 引当分類 (パターン表示名。取込時に推定+人が確認)
    folder_name     TEXT,                   -- 出荷_XX (任意。shipping-log との突合キー)
    work_date       TEXT NOT NULL,          -- 取込日 JST 'YYYY-MM-DD'
    instruct_date   TEXT,                   -- 出荷指示日 'YYYY-MM-DD'
    composition     TEXT NOT NULL,          -- 単品 / 1SKU複数個 / アソート / 混在
    delivery_method TEXT,                   -- 配送方法名 (CSVヘッダ相当の代表値)
    invoice_soft    TEXT,                   -- 送り状発行ソフト名
    line_count      INTEGER NOT NULL,       -- 集約後の明細数 (pk_lines 行数)
    slip_count      INTEGER NOT NULL,       -- 伝票数 (DISTINCT 出荷伝票NO)
    total_qty       INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'ready'
      CHECK(status IN ('ready','picking','paused','done','cancelled')),
    worker          TEXT,                   -- 作業者 email (PR2)
    started_at      TEXT,
    finished_at     TEXT,
    paused_total_sec INTEGER NOT NULL DEFAULT 0,
    validity        TEXT NOT NULL DEFAULT 'valid' CHECK(validity IN ('valid','invalid')),
    imported_by     TEXT NOT NULL,          -- 取込者 email
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_batches_date ON pk_batches(work_date, status)');

  // ロケーション×SKU 集約後の表示明細 (ピッキング順)。作業画面はこれを seq 順に出す (PR2)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_lines (
    batch_id     INTEGER NOT NULL REFERENCES pk_batches(id),
    seq          INTEGER NOT NULL,        -- 1始まり。ロケーション昇順 → SKU昇順
    location     TEXT NOT NULL,           -- ロジザードのロケーション8桁 (例 00201604)
    block        TEXT,                    -- ブロック略称 (例 P3FB)。表示は P3FB-002-016-04
    sku          TEXT NOT NULL,           -- 商品ID = NE商品コード
    product_name TEXT,
    barcode      TEXT,                    -- 将来のスキャン照合用 (PR1では保存のみ)
    qty          INTEGER NOT NULL,        -- SUM(出荷指示数)
    status       TEXT NOT NULL DEFAULT 'pending'
      CHECK(status IN ('pending','done','shortage')),
    shown_at     TEXT,                    -- 明細を表示した時刻 (PR2)
    done_at      TEXT,                    -- 「次へ」= ピック完了時刻 (PR2)
    shortage_qty INTEGER,                 -- 欠品数量 (PR4)
    PRIMARY KEY (batch_id, seq)
  )`);

  // 伝票粒度の生明細 (集約前)。アソート仕分け・NE/shipping-log/誤出荷との突合・分析用
  db.exec(`CREATE TABLE IF NOT EXISTS pk_slip_lines (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   INTEGER NOT NULL REFERENCES pk_batches(id),
    slip_no    TEXT NOT NULL,             -- 出荷伝票NO (SP…)
    picking_no TEXT,                      -- ピッキングNO (PC…)
    ne_slip_no TEXT,                      -- 荷主出荷NO = NE伝票番号
    sku        TEXT NOT NULL,
    qty        INTEGER NOT NULL,
    location   TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_slip_lines_batch ON pk_slip_lines(batch_id)');

  // 操作イベント (追記型・冪等)。PR2 の作業APIが全てここを通る。
  // op_id は端末生成 (ms+乱数)。同一 op_id の再送は保存済み結果を返す (sw_operations と同思想)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    op_id    TEXT NOT NULL UNIQUE,
    batch_id INTEGER NOT NULL REFERENCES pk_batches(id),
    worker   TEXT NOT NULL,
    event    TEXT NOT NULL,   -- start/next/back/shortage/pause/resume/cancel/complete
    line_seq INTEGER,
    payload_json TEXT,
    result_json  TEXT NOT NULL,   -- 再送に返す保存済み結果
    at       TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_events_batch ON pk_events(batch_id, id)');

  // 楽天白抜き画像キャッシュ (PR3 で書き込み)。ne_code = NE商品コード (小文字正規化)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_product_images (
    ne_code       TEXT PRIMARY KEY,
    manage_number TEXT,               -- 解決した楽天商品管理番号
    white_bg_url  TEXT,               -- 白抜き画像 (第一候補)
    top_image_url TEXT,               -- images[0] (フォールバック)
    status        TEXT NOT NULL DEFAULT 'ok' CHECK(status IN ('ok','not_found','error')),
    fetched_at    TEXT NOT NULL
  )`);
}

export function getDB() {
  if (!db) initPickingDB();
  return db;
}

/** バッチ一覧: 指定作業日 + それ以前の未完了持ち越し。cancelled は当日分のみ表示。 */
export function listBatches(workDate) {
  return getDB().prepare(`
    SELECT b.*,
      (SELECT COUNT(*) FROM pk_lines l WHERE l.batch_id = b.id AND l.status != 'pending') AS done_lines
    FROM pk_batches b
    WHERE (b.work_date = ?
        OR (b.work_date < ? AND b.status IN ('ready','picking','paused')))
    ORDER BY b.work_date, b.id
  `).all(workDate, workDate);
}

export function getBatch(id) {
  return getDB().prepare('SELECT * FROM pk_batches WHERE id = ?').get(id);
}

export function getBatchByTbNo(tbNo) {
  return getDB().prepare('SELECT * FROM pk_batches WHERE tb_no = ?').get(tbNo);
}

export function listLines(batchId) {
  return getDB().prepare(
    'SELECT * FROM pk_lines WHERE batch_id = ? ORDER BY seq'
  ).all(batchId);
}
