/**
 * shipping-work DB — 出荷作業管理アプリ (Notion「スタッフ用デイリー業務」置き換え)
 *
 * 設計書: AI_reference/システム設計/出荷作業管理アプリ_要件定義_20260801.md (v2)
 *         AI_reference/システム設計/出荷作業管理アプリ_実装計画_20260801.md
 *
 * warehouse-mirror.db とは分離した専用DB (shipping-work.db) を DATA_DIR に持つ。
 * 作業計測データは本アプリが正本。高頻度書き込みのため mirror 同期とロックを共有しない。
 *
 * 方針 (実装計画§3):
 *   - 日時カラムは全て UTC 'YYYY-MM-DDTHH:MM:SSZ' (inquiry-hub と同形式)。work_date のみ JST 'YYYY-MM-DD'
 *   - 時刻はサーバー時刻を正とする。ブラウザ時刻は使わない
 *   - 計測 (sw_sessions)・遷移履歴 (sw_status_events)・監査 (sw_audit_logs) は追記型。
 *     UPDATE は sessions の終了系カラムのみ、DELETE はどのテーブルにも行わない
 *   - 進行中セッションの排他は部分 UNIQUE INDEX (batch_id, process WHERE outcome='open')
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'shipping-work.db');

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

// バッチの正規ステータス。遷移ルールは service.js が持つ (PR3)。
export const BATCH_STATUSES = [
  'ready',        // 本日のやること
  'picking',      // ピッキング中
  'picked',       // ピッキング完了
  'sorting',      // 仕分け中 (アソートのみ)
  'sorted',       // 仕分け完了
  'packing',      // 梱包作業中
  'done',         // 完了
  'hold',         // 保留 (解除で hold_from_status へ復帰)
  'stock_return', // 在庫戻し
  'cancelled',    // 取消
];

/** カンバン表示用のステータス日本語ラベル (現行 Notion の呼称を維持)。 */
export const STATUS_LABELS = {
  ready: '本日のやること',
  picking: 'ピッキング中',
  picked: 'ピッキング完了',
  sorting: '仕分け中',
  sorted: '仕分け完了',
  packing: '梱包作業中',
  done: '完了',
  hold: '保留',
  stock_return: '在庫戻し',
  cancelled: '取消',
};

export function initShippingWorkDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) { try { db.close(); } catch { /* close済み等は無視 */ } db = null; }
  db = new Database(DB_FILE);
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  createTables();
  seedMasters();
  return db;
}

export function getDB() {
  if (!db) initShippingWorkDB();
  return db;
}

function createTables() {
  // 区分マスタ。kind: shipping_no / bunrui / packing_method / carrier /
  //   pause_reason_pick / pause_reason_pack / mistake_kind / print_trouble_reason
  db.exec(`CREATE TABLE IF NOT EXISTS sw_masters (
    kind   TEXT NOT NULL,
    code   TEXT NOT NULL,
    label  TEXT NOT NULL,
    sort   INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    PRIMARY KEY (kind, code)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS sw_batches (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    work_date      TEXT NOT NULL,                -- JST 'YYYY-MM-DD'
    shipping_no    TEXT NOT NULL,                -- sw_masters(shipping_no).code
    bunrui         TEXT NOT NULL,                -- 単品/同一商品複数個/アソート
    packing_method TEXT NOT NULL,                -- 梱包機3種 or 手動
    carriers_json  TEXT NOT NULL DEFAULT '[]',   -- 配送種別 code の JSON 配列
    slip_count     INTEGER,                      -- 伝票件数
    note           TEXT,
    status         TEXT NOT NULL DEFAULT 'ready'
      CHECK(status IN ('ready','picking','picked','sorting','sorted','packing','done','hold','stock_return','cancelled')),
    hold_from_status TEXT,                       -- 保留解除時の復帰先
    pdf_path       TEXT,                         -- 添付帳票 (ピッキングリスト等)。PR2で添付UI
    created_by     INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sw_batches_date ON sw_batches(work_date, status)');

  // ステータス遷移履歴 (追記型・削除不可)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_status_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES sw_batches(id),
    from_status TEXT,
    to_status   TEXT NOT NULL,
    actor_id    INTEGER NOT NULL,
    via         TEXT NOT NULL CHECK(via IN ('button','auto','admin')),
    reason      TEXT,
    at          TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sw_events_batch ON sw_status_events(batch_id)');

  // 工程別計測セッション (picking/sorting/packing 共通構造・追記型)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_sessions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id          INTEGER NOT NULL REFERENCES sw_batches(id),
    process           TEXT NOT NULL CHECK(process IN ('picking','sorting','packing')),
    worker_id         INTEGER NOT NULL,
    requested_at      TEXT NOT NULL,   -- 開始ボタン押下 (正常時の計測開始)
    print_accepted_at TEXT,            -- 印刷ブリッジ受付 (バイアス検証用・PR6)
    ended_at          TEXT,
    outcome           TEXT NOT NULL DEFAULT 'open'
      CHECK(outcome IN ('open','completed','voided','print_failed','cancelled')),
    void_reason       TEXT,            -- 印刷トラブル理由等
    flags_json        TEXT NOT NULL DEFAULT '[]',  -- 異常候補: too_short/reprint/too_long 等
    op_id             TEXT             -- 冪等キー (PR3)
  )`);
  // 同一バッチ×工程の進行中セッションは 1 つ (リースの実体)
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_sw_sessions_active
    ON sw_sessions(batch_id, process) WHERE outcome='open'`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sw_sessions_worker ON sw_sessions(worker_id, requested_at)');
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_sw_sessions_opid ON sw_sessions(op_id) WHERE op_id IS NOT NULL');

  // 保留区間 (セッションに紐付く。理由必須)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_pauses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  INTEGER NOT NULL REFERENCES sw_sessions(id),
    reason      TEXT NOT NULL,
    reason_note TEXT,
    paused_at   TEXT NOT NULL,
    resumed_at  TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sw_pauses_session ON sw_pauses(session_id)');

  // 印刷ジョブ (業務上の要求) と試行 (投入ごと)。PR6 でブリッジ接続
  db.exec(`CREATE TABLE IF NOT EXISTS sw_print_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL REFERENCES sw_batches(id),
    session_id   INTEGER REFERENCES sw_sessions(id),
    doc_type     TEXT NOT NULL DEFAULT 'picking_list',
    pdf_path     TEXT NOT NULL,
    printer      TEXT,
    status       TEXT NOT NULL DEFAULT 'requested'
      CHECK(status IN ('requested','leased','spooling','done','failed')),
    requested_by INTEGER NOT NULL,
    requested_at TEXT NOT NULL
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS sw_print_attempts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id         INTEGER NOT NULL REFERENCES sw_print_jobs(id),
    attempt_no     INTEGER NOT NULL,
    version_no     INTEGER NOT NULL,   -- 再印刷帳票に「再印刷 v2」を印字
    reprint_reason TEXT,
    result         TEXT,
    error          TEXT,
    leased_at      TEXT,
    reported_at    TEXT
  )`);

  // ミス記録 (現行 Notion の4分類を維持)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_mistakes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES sw_batches(id),
    process     TEXT,
    kind        TEXT NOT NULL,
    count       INTEGER NOT NULL CHECK(count > 0),
    note        TEXT,
    recorded_by INTEGER NOT NULL,
    at          TEXT NOT NULL
  )`);

  // 管理者修正・重要操作 (追記のみ。UPDATE/DELETE はアプリ層で行わない)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_audit_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id    INTEGER NOT NULL,
    action      TEXT NOT NULL,
    target      TEXT,
    before_json TEXT,
    after_json  TEXT,
    reason      TEXT,
    at          TEXT NOT NULL
  )`);

  // 設定 (休憩時間帯・異常閾値・プリンタルーティング)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )`);
}

// ─── マスタ初期データ (現行 Notion「スタッフ用デイリー業務」の区分をそのまま引き継ぐ) ───

/** ①〜㊿ の丸数字 (Unicode: ①-⑳ / ㉑-㉟ / ㊱-㊿)。現場の呼称を変えないため表示に使う。 */
function circled(n) {
  if (n >= 1 && n <= 20) return String.fromCodePoint(0x2460 + n - 1);
  if (n >= 21 && n <= 35) return String.fromCodePoint(0x3251 + n - 21);
  if (n >= 36 && n <= 50) return String.fromCodePoint(0x32B1 + n - 36);
  return String(n);
}

function seedMasters() {
  const rows = [];
  for (let n = 1; n <= 45; n++) {
    rows.push(['shipping_no', `s${String(n).padStart(2, '0')}`, `出荷No${circled(n)}`, n]);
  }
  rows.push(['shipping_no', 'linegift', 'LINEギフト', 46]);

  [['tanpin', '単品'], ['same_multi', '同一商品複数個'], ['assort', '複数(アソート)']]
    .forEach(([code, label], i) => rows.push(['bunrui', code, label, i + 1]));

  [
    ['machine_trifold_pass', '梱包機【三つ折り】【パスライン】'],
    ['machine_bifold_pass', '梱包機【二つ折り】【パスライン】'],
    ['machine_melt', '梱包機【メルトライン】'],
    ['manual', '手動'],
  ].forEach(([code, label], i) => rows.push(['packing_method', code, label, i + 1]));

  [
    'ネコポス', 'クリックポスト', 'ゆうパケット', 'ゆうパケットパフ', '定形外', 'レターパック',
    'AES【ネコポス】', 'AES【5060サイズ】', '50サイズ', '60サイズ以上',
    '宅急便コンパクト', '宅急便', 'ゆうパック',
  ].forEach((label, i) => rows.push(['carrier', `c${String(i + 1).padStart(2, '0')}`, label, i + 1]));

  [
    '在庫不足', '商品が見つからない', '商品情報確認', '管理者確認待ち',
    '他作業への応援', '設備トラブル', 'その他',
  ].forEach((label, i) => rows.push(['pause_reason_pick', `pk${i + 1}`, label, i + 1]));

  [
    '商品不足', '商品違い', '破損', '梱包資材不足', '複数個口対応',
    '特殊梱包', '送り状不備', '管理者確認待ち', 'その他',
  ].forEach((label, i) => rows.push(['pause_reason_pack', `pc${i + 1}`, label, i + 1]));

  [
    ['toriwasure', '取忘れ'], ['torisugi', '取りすぎ'],
    ['machigai', '取り間違い'], ['other', 'その他'],
  ].forEach(([code, label], i) => rows.push(['mistake_kind', code, label, i + 1]));

  [
    ['jam', '紙詰まり'], ['no_output', '出てこない'],
    ['wrong_doc', '帳票違い'], ['other', 'その他'],
  ].forEach(([code, label], i) => rows.push(['print_trouble_reason', code, label, i + 1]));

  const ins = db.prepare(
    'INSERT OR IGNORE INTO sw_masters (kind, code, label, sort) VALUES (?, ?, ?, ?)'
  );
  const tx = db.transaction(() => { for (const r of rows) ins.run(...r); });
  tx();
}

/** kind ごとの有効マスタ一覧 (sort順)。 */
export function listMasters(kind) {
  return getDB().prepare(
    'SELECT code, label, sort FROM sw_masters WHERE kind = ? AND active = 1 ORDER BY sort'
  ).all(kind);
}

/** カンバン用: 指定作業日のバッチ + 未完了の持ち越しバッチ。 */
export function listKanbanBatches(workDate) {
  return getDB().prepare(`
    SELECT b.*,
      (SELECT label FROM sw_masters m WHERE m.kind='shipping_no' AND m.code=b.shipping_no) AS shipping_no_label,
      (SELECT label FROM sw_masters m WHERE m.kind='bunrui' AND m.code=b.bunrui) AS bunrui_label,
      (SELECT label FROM sw_masters m WHERE m.kind='packing_method' AND m.code=b.packing_method) AS packing_method_label
    FROM sw_batches b
    WHERE b.work_date = ?
       OR b.status NOT IN ('done','cancelled','stock_return')
    ORDER BY b.work_date, b.id
  `).all(workDate);
}
