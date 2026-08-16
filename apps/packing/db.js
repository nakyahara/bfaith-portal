/**
 * packing DB — 梱包支援システム (iPad梱包)
 *
 * 設計書: AI_reference/システム設計/梱包支援システム_要件定義_20260815.md
 *
 * DBファイルは picking.db に同居する (要件§7.3: 「picking.db に追加・所有は packing」)。
 *   - pk_pack_* テーブルの所有者は packing。picking 所有テーブル (pk_batches / pk_slip_lines 等)
 *     は参照JOINのみ可・直接UPDATE禁止 (要件§7.1)
 *   - スキーマ版数は picking の PRAGMA user_version と衝突しないよう、
 *     専用メタテーブル pk_pack_meta の schema_version で管理する
 *   - 接続は picking と共有しない (別 Database インスタンス)。WAL + busy_timeout で並存し、
 *     将来のプロセス分離 (要件§7.2) をプロセス内共有状態で阻害しない
 *
 * 方針 (picking/db.js と同規約):
 *   - 日時カラムは全て UTC 'YYYY-MM-DDTHH:MM:SSZ'。work_date のみ JST 'YYYY-MM-DD'
 *   - 操作 (pk_pack_events) は追記型。取消は物理削除せず validity='invalid'
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

export const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'picking.db');

let db = null;

/** UTC 'YYYY-MM-DDTHH:MM:SSZ' (秒精度)。 */
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

export const STATUS_LABELS = {
  ready: '未着手',
  packing: '梱包中',
  paused: '中断中',
  done: '完了',
  cancelled: '取消',
};

export const MATCH_LABELS = {
  ok: 'ピッキングと一致',
  mismatch: '⚠ ピッキングと不一致 (承認済み)',
  no_picking: '⚠ ピッキング未取込 (承認済み)',
};

const SCHEMA_VERSION = 1;

export function initPackingDB() {
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
 * メタテーブル版マイグレーション。picking の user_version 方式と同じ二重適用ガード
 * (BEGIN IMMEDIATE 内で版を読み直す) を、pk_pack_meta の行で行う。
 */
function migrate() {
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_meta (
    key TEXT PRIMARY KEY, value TEXT NOT NULL
  )`);
  const readVersion = () => Number(db.prepare(
    "SELECT value FROM pk_pack_meta WHERE key = 'schema_version'"
  ).get()?.value ?? 0);
  let v = readVersion();
  if (v > SCHEMA_VERSION) {
    throw new Error(`packing スキーマの version=${v} がコードの期待 ${SCHEMA_VERSION} より新しい (ロールバック不可)`);
  }
  while (v < SCHEMA_VERSION) {
    const next = v + 1;
    const step = MIGRATIONS[next];
    if (!step) throw new Error(`packing migration v${next} が未定義`);
    db.transaction(() => {
      if (readVersion() >= next) return;
      step();
      db.prepare(`INSERT INTO pk_pack_meta (key, value) VALUES ('schema_version', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(String(next));
    }).immediate();
    const applied = readVersion();
    if (applied < next) throw new Error(`packing migration v${next}: version が ${applied} のまま更新されていない`);
    v = applied;
  }
}

// v1 は PR1 マージまで自由に編集してよい (本番デプロイ前のため既存テーブルは存在しない)。
// マージ後のスキーマ変更は必ず v2 以降の migration として追記する
const MIGRATIONS = {
  1: createCoreTables,
};

function createCoreTables() {
  // 梱包バッチ (= 納品書CSV 1ファイル)。tb_key は picking の pk_batches.tb_no と同じ
  // 正規化 (ソート済みTB一覧のカンマ結合) — 前工程との突合キー
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_batches (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    tb_key           TEXT NOT NULL UNIQUE,
    folder_name      TEXT,                   -- 出荷_XX
    work_date        TEXT NOT NULL,          -- 取込日 JST 'YYYY-MM-DD'
    sagyo_date       TEXT,                   -- 出荷作業日 'YYYY-MM-DD' (CSVの値)
    slip_count       INTEGER NOT NULL,
    line_count       INTEGER NOT NULL,
    total_qty        INTEGER NOT NULL,
    pk_batch_id      INTEGER,                -- 突合した pk_batches.id (参照のみ。FKは張らない
                                             -- = picking 側の削除・作り直しに巻き込まれない)
    match_status     TEXT NOT NULL CHECK(match_status IN ('ok','mismatch','no_picking')),
    match_json       TEXT,                   -- 突合差分の詳細 (mismatch 時)
    status           TEXT NOT NULL DEFAULT 'ready'
      CHECK(status IN ('ready','packing','paused','done','cancelled')),
    worker           TEXT,
    started_at       TEXT,
    finished_at      TEXT,
    paused_total_sec INTEGER NOT NULL DEFAULT 0,
    validity         TEXT NOT NULL DEFAULT 'valid' CHECK(validity IN ('valid','invalid')),
    csv_sha256       TEXT NOT NULL,
    imported_by      TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_batches_date ON pk_pack_batches(work_date, status)');

  // 出荷伝票 (納品書1枚)。seq = CSVの出現順 = 納品書PDFの束の順 (要件§4: 実データで完全一致確認済み)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_slips (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id           INTEGER NOT NULL REFERENCES pk_pack_batches(id),
    seq                INTEGER NOT NULL,     -- 1始まり (納品書順)
    ne_slip_no         TEXT NOT NULL,        -- 荷主出荷NO = NE伝票番号 (紙との目視照合キー)
    slip_no            TEXT NOT NULL,        -- 出荷伝票NO (SP…)
    picking_no         TEXT,                 -- ピッキングNO (PC…)
    matehan_bc         TEXT,                 -- マテハン用BC (スキャン不採用のため保持のみ)
    mall               TEXT,                 -- 取引先名 (モール店舗名)
    delivery_method_id TEXT,
    delivery_method    TEXT,
    material           TEXT,                 -- 引当抽出グループ1 (梱包資材/ラインのマーカー)
    box_count          INTEGER NOT NULL DEFAULT 1,
    warn_json          TEXT,                 -- 警告バッジ配列 (gift/noshi/comment/multi_box/…)
    gift_message       TEXT,
    noshi              TEXT,
    comments_json      TEXT,                 -- {header,footer,warehouse,customer} 空でないもののみ
    delivery_date      TEXT,                 -- 配達指定日
    delivery_time      TEXT,                 -- 配達時間帯
    status             TEXT NOT NULL DEFAULT 'pending'
      CHECK(status IN ('pending','done','held','cancelled')),
    hold_location      TEXT,                 -- 保留トレー番号 (PR2)
    hold_reason        TEXT,                 -- repick/shipping_change/other (PR2)
    shown_at           TEXT,
    done_at            TEXT,
    UNIQUE (batch_id, seq)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_slips_batch ON pk_pack_slips(batch_id, seq)');

  // 伝票明細 (納品書の行)。並びは CSV の行順 (= 納品書の印字順)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_lines (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    slip_id      INTEGER NOT NULL REFERENCES pk_pack_slips(id),
    line_no      INTEGER,                    -- 出荷予定行NO
    sku          TEXT NOT NULL,              -- 商品ID = NE商品コード
    product_name TEXT,
    qty          INTEGER NOT NULL,
    barcode      TEXT,
    print_name   TEXT,                       -- 印字商品名 (納品書に実際に載る名称。セット品はこちらが実体)
    short_name   TEXT,                       -- 送り状備考1 (読み上げ短縮名の第一候補。正規化はPR2)
    expiry       TEXT,                       -- 有効期限
    lot          TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_lines_slip ON pk_pack_lines(slip_id)');

  // 操作イベント (追記型・op_id 冪等)。作業API (PR2) が全てここを通る。
  // 「次へ」1タップは packing_completed (現在伝票) と slip_opened (次伝票) の2イベントに
  // 分離して記録する (要件§5.11)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    op_id        TEXT NOT NULL UNIQUE,
    batch_id     INTEGER NOT NULL REFERENCES pk_pack_batches(id),
    worker       TEXT NOT NULL,
    event        TEXT NOT NULL,
    slip_seq     INTEGER,
    payload_json TEXT,
    result_json  TEXT NOT NULL,
    at           TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_events_batch ON pk_pack_events(batch_id, id)');

  // 取込の監査ログ (追記型・削除不可)。突合結果の承認 (mismatch_ack) もここに残る
  db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_import_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL REFERENCES pk_pack_batches(id),
    tb_key       TEXT NOT NULL,
    action       TEXT NOT NULL CHECK(action IN ('create','overwrite','match_update')),
    csv_sha256   TEXT NOT NULL,
    folder_name  TEXT,
    slip_count   INTEGER NOT NULL,
    line_count   INTEGER NOT NULL,
    total_qty    INTEGER NOT NULL,
    match_status TEXT NOT NULL,
    match_acked  INTEGER NOT NULL DEFAULT 0, -- 1 = 不一致/未取込を人が明示承認して取り込んだ
    before_json  TEXT,                       -- 上書き時: 変更前の集計値・ハッシュ
    actor        TEXT NOT NULL,
    at           TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_import_logs_batch ON pk_pack_import_logs(batch_id, id)');
}

export function getDB() {
  if (!db) initPackingDB();
  return db;
}

/** バッチ一覧: 指定作業日 + それ以前の未完了持ち越し。cancelled は当日分のみ。 */
export function listPackBatches(workDate) {
  return getDB().prepare(`
    SELECT b.*,
      (SELECT COUNT(*) FROM pk_pack_slips s WHERE s.batch_id = b.id AND s.status = 'done') AS done_slips
    FROM pk_pack_batches b
    WHERE (b.work_date = ?
        OR (b.work_date < ? AND b.status IN ('ready','packing','paused')))
    ORDER BY b.work_date, b.id
  `).all(workDate, workDate);
}

export function getPackBatch(id) {
  return getDB().prepare('SELECT * FROM pk_pack_batches WHERE id = ?').get(id);
}

export function getPackBatchByTbKey(tbKey) {
  return getDB().prepare('SELECT * FROM pk_pack_batches WHERE tb_key = ?').get(tbKey);
}

/** 伝票一覧 (納品書順)。明細は listPackLines で別取得。 */
export function listPackSlips(batchId) {
  return getDB().prepare(
    'SELECT * FROM pk_pack_slips WHERE batch_id = ? ORDER BY seq'
  ).all(batchId);
}

/** バッチ内の全明細 (slip_id → 行の配列)。 */
export function listPackLinesBySlip(batchId) {
  const map = new Map();
  for (const l of getDB().prepare(`
    SELECT l.* FROM pk_pack_lines l
    JOIN pk_pack_slips s ON s.id = l.slip_id
    WHERE s.batch_id = ? ORDER BY s.seq, l.id
  `).all(batchId)) {
    if (!map.has(l.slip_id)) map.set(l.slip_id, []);
    map.get(l.slip_id).push(l);
  }
  return map;
}
