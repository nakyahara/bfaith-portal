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

export const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
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

// スキーマ版数 (PRAGMA user_version)。変更時は MIGRATIONS に追記して番号を上げる。
const SCHEMA_VERSION = 6;

export function initShippingWorkDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) { try { db.close(); } catch { /* close済み等は無視 */ } db = null; }
  db = new Database(DB_FILE);
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  migrate();
  seedMasters();
  return db;
}

/**
 * バージョン式マイグレーション (Codex PR1レビュー#3)。
 * CREATE IF NOT EXISTS 頼みだと既存DBにスキーマ変更が当たらないため、
 * user_version で管理し、各版をトランザクションで適用する。
 * 失敗時は throw して起動を止める (router.js が import 時に init するため boot が失敗する)。
 */
function migrate() {
  let v = db.pragma('user_version', { simple: true });
  if (v > SCHEMA_VERSION) {
    throw new Error(`shipping-work.db の user_version=${v} がコードの期待 ${SCHEMA_VERSION} より新しい (ロールバック不可)`);
  }
  while (v < SCHEMA_VERSION) {
    const next = v + 1;
    const step = MIGRATIONS[next];
    if (!step) throw new Error(`shipping-work migration v${next} が未定義`);
    // 通常はこちらでトランザクションを張る。
    // テーブル作り直しのように PRAGMA をトランザクション外で切り替える必要がある版は
    // { manual: true } を宣言し、自分でトランザクションと user_version を管理する。
    // immediate = BEGIN IMMEDIATE。migration は読んでから書くので、DEFERRED だと
    // 読み取り後の書込み昇格時に SQLITE_BUSY_SNAPSHOT (busy_timeout で待てない) になりうる
    if (step.manual) step.run(next);
    else {
      db.transaction(() => {
        step();
        db.pragma(`user_version = ${next}`);
      }).immediate();
    }
    // manual 版が user_version の更新を忘れると、そのプロセスだけ適用済みとして起動し
    // 次回起動で同じ migration が再実行される (非冪等なら部分適用や起動不能)。
    // ローカル変数を進める前に実DBの値で必ず突き合わせる
    const applied = db.pragma('user_version', { simple: true });
    if (applied !== next) {
      throw new Error(`shipping-work migration v${next}: user_version が ${applied} のまま更新されていない`);
    }
    v = next;
  }
}

/** テーブルに指定の列があるか (migration の冪等判定用)。 */
function hasColumn(table, column) {
  return db.prepare(`PRAGMA table_info(${table})`).all().some((c) => c.name === column);
}

const MIGRATIONS = {
  1: createCoreTables,
  // v2: actor系カラムを INTEGER → TEXT(email) に変更 (portal のセッションは数値IDを持たず
  // email が既存アプリ共通のactor識別子のため。mis-shipment 等と同規約)。
  // v1スキーマの本番DBは業務データ0件のうちに適用する前提。データがあれば安全側で停止。
  2: () => {
    const hasData = ['sw_batches', 'sw_sessions', 'sw_audit_logs'].some(
      (t) => db.prepare(`SELECT COUNT(*) c FROM ${t}`).get().c > 0
    );
    if (hasData) throw new Error('shipping-work migration v2: 業務データが存在するため自動再作成を中止 (手動対応が必要)');
    for (const t of ['sw_print_attempts', 'sw_print_jobs', 'sw_pauses', 'sw_sessions',
      'sw_status_events', 'sw_mistakes', 'sw_audit_logs', 'sw_batches']) {
      db.exec(`DROP TABLE IF EXISTS ${t}`);
    }
    createCoreTables();
  },
  // v3: 保留中セッションを「一人一作業」制約の対象外にする (PR3)。
  // 保留理由マスタに「他作業への応援」がある = 保留して別の作業へ移る運用が要件に含まれるが、
  // v1/v2 の worker 部分UNIQUE は保留中も1作業と数えるため応援に行けなかった。
  // paused 列を制約条件に加えることで「進行中は1つ・保留中は何件でも持てる」をDBで保証する。
  // (PR1のコメント「応援・交代の例外運用が必要になったら PR3 で migration により見直す」の実施)
  3: () => {
    db.exec('ALTER TABLE sw_sessions ADD COLUMN paused INTEGER NOT NULL DEFAULT 0 CHECK(paused IN (0,1))');
    // 既存の未解除保留があれば paused=1 に揃える (v2以前のDBからの移行時)
    db.exec(`UPDATE sw_sessions SET paused = 1 WHERE outcome = 'open' AND EXISTS
      (SELECT 1 FROM sw_pauses p WHERE p.session_id = sw_sessions.id AND p.resumed_at IS NULL)`);
    db.exec('DROP INDEX IF EXISTS idx_sw_sessions_worker_open');
    db.exec(`CREATE UNIQUE INDEX idx_sw_sessions_worker_open
      ON sw_sessions(worker) WHERE outcome='open' AND paused=0`);
  },
  // v4: 冪等性をセッションの op_id 相乗りから専用テーブルへ移す (Codex PR3レビュー#1/#2/#4)。
  // sw_sessions.op_id だけでは「同じ op_id を別バッチに送った再送」を成功扱いしてしまい、
  // 完了・保留・再開は op_id を持たないため通信タイムアウト後の遅延再送で記録が壊れる。
  // 全作業操作を (op_id, 操作種別, 対象, 入力) で束縛し、一致した再送だけ前回結果を返す。
  // 進行中セッションの op_id を sw_operations へ backfill することは意図的にしない。
  // 旧セッションからは「手動開始 (start:) だったのか『完了して次を開始』(start-next:) だったのか」
  // を判別できず、誤った操作種別で記録すると再送時の照合がかえって狂う
  // (Codex PR3レビュー round3 #medium)。
  // 本番DBは v2 (業務データ0件) からこの版へ上がるため、冪等記録を持たない
  // in-flight セッションはそもそも存在しない。将来 sw_operations 導入前の状態から
  // 移行する必要が生じた場合も、判別できない以上 backfill せず 409 を返す方が安全。
  4: createOperationsTable,
  // v5: 実作業秒数 (終了 - 開始 - 保留合計) を完了時に保存する。
  // 集計側で再計算もできるが、異常候補フラグ (too_short/too_long) がどの数値で立ったのかを
  // 後から検証できるようにするため、判定に使った値そのものを残す。
  // これは計測が主目的のアプリなので、指標の再現性を記録側で担保する。
  5: () => {
    db.exec('ALTER TABLE sw_sessions ADD COLUMN active_sec INTEGER');
  },
  // v6: 帳票を Google Drive のリンクでも指せるようにする。
  // 現場は既にピッキングリストPDFを Drive (出荷_01〜45 フォルダ) に置いてリンクで見ており、
  // アプリへのファイルアップロードを必須にすると二重管理になる。
  // 「アップロードしたPDF」か「Driveリンク」のどちらかがあれば帳票ありとして扱う。
  // sw_print_jobs.pdf_path は NOT NULL のままだと doc_url だけの帳票を記録できないが、
  // SQLite は列制約を後から変更できないためテーブルを作り直す (SQLite公式の12ステップ手順)。
  // FK 参照元 (sw_print_attempts) があり、PRAGMA foreign_keys はトランザクション内で
  // 切り替えられないため、この版だけ manual にして自分でトランザクションを張る。
  // 既存の印刷ジョブ・試行は行ごとコピーして保持する (稼働後でも安全に上げられるように)。
  6: {
    manual: true,
    run(version) {
      // 変更する PRAGMA は先に全て控えてから触る (どこで失敗しても finally で元に戻せるように)
      const fkWas = db.pragma('foreign_keys', { simple: true });
      const legacyWas = db.pragma('legacy_alter_table', { simple: true });
      try {
        db.pragma('foreign_keys = OFF');
        // RENAME 時に他テーブルのFK句を書き換えさせない (参照名は sw_print_jobs のまま維持したい)
        db.pragma('legacy_alter_table = ON');
        db.transaction(() => {
          if (!hasColumn('sw_batches', 'doc_url')) {
            db.exec('ALTER TABLE sw_batches ADD COLUMN doc_url TEXT');
          }
          // 新規DBは createPrintTables が既に新形で作っているので、旧形のときだけ入れ替える
          if (!hasColumn('sw_print_jobs', 'doc_url')) {
            db.exec(`CREATE TABLE sw_print_jobs_v6 (
              id           INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id     INTEGER NOT NULL REFERENCES sw_batches(id),
              session_id   INTEGER REFERENCES sw_sessions(id),
              doc_type     TEXT NOT NULL DEFAULT 'picking_list',
              pdf_path     TEXT,
              doc_url      TEXT,
              printer      TEXT,
              status       TEXT NOT NULL DEFAULT 'requested'
                CHECK(status IN ('requested','leased','spooling','spooled','failed','uncertain','cancelled')),
              requested_by TEXT NOT NULL,
              requested_at TEXT NOT NULL,
              CHECK(pdf_path IS NOT NULL OR doc_url IS NOT NULL)
            )`);
            db.exec(`INSERT INTO sw_print_jobs_v6
              (id, batch_id, session_id, doc_type, pdf_path, doc_url, printer, status, requested_by, requested_at)
              SELECT id, batch_id, session_id, doc_type, pdf_path, NULL, printer, status, requested_by, requested_at
              FROM sw_print_jobs`);
            db.exec('DROP TABLE sw_print_jobs');
            db.exec('ALTER TABLE sw_print_jobs_v6 RENAME TO sw_print_jobs');
          }
          // 参照が壊れていないことを確認してからコミットする
          const bad = db.pragma('foreign_key_check');
          if (bad.length > 0) {
            throw new Error(`shipping-work migration v6: 外部キー違反 ${bad.length} 件のため中止`);
          }
          db.pragma(`user_version = ${version}`);
        }).immediate();
      } finally {
        db.pragma(`legacy_alter_table = ${legacyWas ? 'ON' : 'OFF'}`);
        db.pragma(`foreign_keys = ${fkWas ? 'ON' : 'OFF'}`);
      }
    },
  },
};

/**
 * 印刷ジョブ (業務上の要求) と試行 (投入ごと)。PR6 でブリッジ接続。
 * 帳票は「アップロードしたPDF (pdf_path)」か「Driveリンク (doc_url)」のどちらか。
 * PR6 のブリッジは doc_url の場合サービスアカウントで Drive から取得する想定。
 */
function createPrintTables() {
  db.exec(`CREATE TABLE IF NOT EXISTS sw_print_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL REFERENCES sw_batches(id),
    session_id   INTEGER REFERENCES sw_sessions(id),
    doc_type     TEXT NOT NULL DEFAULT 'picking_list',
    pdf_path     TEXT,                 -- DOCS_DIR 相対のファイル名 (アップロード時)
    doc_url      TEXT,                 -- Google Drive 等のリンク (リンク運用時)
    printer      TEXT,
    -- spooled = スプーラー投入成功まで (物理印刷完了は保証しない)。
    -- uncertain = タイムアウト等で結果不明 (failed と区別。管理者が実物確認)。
    -- cancelled = 未印刷ジョブの取消。(Codex PR1レビュー#2: 状態集合はCHECK変更が
    -- テーブル再作成になるため PR1 で確保)
    status       TEXT NOT NULL DEFAULT 'requested'
      CHECK(status IN ('requested','leased','spooling','spooled','failed','uncertain','cancelled')),
    requested_by TEXT NOT NULL,      -- 要求者 email
    requested_at TEXT NOT NULL,
    CHECK(pdf_path IS NOT NULL OR doc_url IS NOT NULL)
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
    reported_at    TEXT,
    UNIQUE(job_id, attempt_no)
  )`);
}

/** 冪等操作の記録。全作業APIがここを通り、同一 op_id の再送は保存済み結果を返す。 */
function createOperationsTable() {
  db.exec(`CREATE TABLE IF NOT EXISTS sw_operations (
    op_id       TEXT PRIMARY KEY,
    worker      TEXT NOT NULL,
    operation   TEXT NOT NULL,      -- start/complete/pause/resume/trouble
    target_kind TEXT NOT NULL,      -- batch/session
    target_id   INTEGER NOT NULL,
    params_hash TEXT,               -- 入力内容のハッシュ (別入力での使い回しを検知)
    result_json TEXT NOT NULL,
    at          TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sw_operations_worker ON sw_operations(worker, at)');
}

export function getDB() {
  if (!db) initShippingWorkDB();
  return db;
}

// 正準DDL。fresh DBはv1でこれを作り、v2は旧v1スキーマのDBを作り直すために同じ関数を使う
// (どちらの経路でも最終形が一致する)。
function createCoreTables() {
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
    pdf_path       TEXT,                         -- 添付帳票のファイル名 (DOCS_DIR相対)
    -- doc_url (帳票のリンク) は v6 の ALTER で追加する。
    -- この関数は v1/v2 時点の正準DDLで、以降の版は MIGRATIONS の ALTER で重ねる規約
    created_by     TEXT NOT NULL,                -- 作成者 email
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    -- 業務キー: 同じ作業日に同じ出荷Noのバッチは1つ (現行Notion運用と同じ。
    -- 二重クリック・再送による二重作成の防止。Codex PR1レビュー#1)
    UNIQUE(work_date, shipping_no)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sw_batches_date ON sw_batches(work_date, status)');

  // ステータス遷移履歴 (追記型・削除不可)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_status_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES sw_batches(id),
    from_status TEXT,
    to_status   TEXT NOT NULL,
    actor       TEXT NOT NULL,      -- 操作者 email ('system'=自動遷移)
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
    worker            TEXT NOT NULL,   -- 作業者 email
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
  // 一人一作業の原則 (要件v2: 完了しないと次の仕事を取得できない) をDB制約で保証。
  // 応援・交代の例外運用が必要になったら PR3 で migration により見直す (Codex PR1レビュー#5)
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_sw_sessions_worker_open
    ON sw_sessions(worker) WHERE outcome='open'`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_sw_sessions_worker ON sw_sessions(worker, requested_at)');
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
  // 未解除の保留は1セッション1つ (連打・再送による保留時間の二重計上防止。Codex PR1レビュー#6)
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_sw_pauses_active
    ON sw_pauses(session_id) WHERE resumed_at IS NULL`);

  createPrintTables();

  // ミス記録 (現行 Notion の4分類を維持)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_mistakes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES sw_batches(id),
    process     TEXT,
    kind        TEXT NOT NULL,
    count       INTEGER NOT NULL CHECK(count > 0),
    note        TEXT,
    recorded_by TEXT NOT NULL,       -- 記録者 email
    at          TEXT NOT NULL
  )`);

  // 管理者修正・重要操作 (追記のみ。UPDATE/DELETE はアプリ層で行わない)
  db.exec(`CREATE TABLE IF NOT EXISTS sw_audit_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    actor       TEXT NOT NULL,       -- 操作者 email
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

/**
 * 帳票が紐付いていないバッチは開始できない (開始=印刷が成立しないため)。
 * 帳票は「アップロードしたPDF」か「Driveリンク」のどちらかで指定する。
 */
export function batchHasDocument(batch) {
  return !!(batch?.pdf_path || batch?.doc_url);
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

  // コード定義を初期マスタの正本とし、label/sort はデプロイごとに追従させる。
  // active は運用での停止設定を守るため上書きしない (Codex PR1レビュー#9)
  const ins = db.prepare(`
    INSERT INTO sw_masters (kind, code, label, sort) VALUES (?, ?, ?, ?)
    ON CONFLICT(kind, code) DO UPDATE SET label = excluded.label, sort = excluded.sort
  `);
  const tx = db.transaction(() => { for (const r of rows) ins.run(...r); });
  tx();
}

/** kind ごとの有効マスタ一覧 (sort順)。 */
export function listMasters(kind) {
  return getDB().prepare(
    'SELECT code, label, sort FROM sw_masters WHERE kind = ? AND active = 1 ORDER BY sort'
  ).all(kind);
}

/** マスタ code の存在チェック (active のみ)。 */
export function isValidMaster(kind, code) {
  return !!getDB().prepare(
    'SELECT 1 FROM sw_masters WHERE kind = ? AND code = ? AND active = 1'
  ).get(kind, code);
}

// ─── バッチ CRUD (PR2: 管理者のみ。router 側で admin check 済みの前提) ───

/** 帳票PDFの保存ディレクトリ。この外のパスは serve しない (パストラバーサル防止)。 */
export const DOCS_DIR = path.join(DATA_DIR, 'shipping-work-docs');

/** 監査ログ追記 (追記のみ。修正・削除APIは作らない)。actor は email。 */
export function addAuditLog(actor, action, target, before, after, reason) {
  getDB().prepare(`
    INSERT INTO sw_audit_logs (actor, action, target, before_json, after_json, reason, at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(actor, action, target,
    before == null ? null : JSON.stringify(before),
    after == null ? null : JSON.stringify(after),
    reason ?? null, utcNow());
}

/** ステータス遷移イベント追記。バッチ本体の status 更新は呼び出し側のトランザクションで行う。 */
export function addStatusEvent(batchId, fromStatus, toStatus, actor, via, reason) {
  getDB().prepare(`
    INSERT INTO sw_status_events (batch_id, from_status, to_status, actor, via, reason, at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(batchId, fromStatus, toStatus, actor, via, reason ?? null, utcNow());
}

/**
 * バッチ作成 (status_events と同一トランザクション)。
 * UNIQUE(work_date, shipping_no) 違反は SQLITE_CONSTRAINT_UNIQUE で throw (router 側で 409 に変換)。
 */
export function createBatch({ workDate, shippingNo, bunrui, packingMethod, carriers, slipCount, note, pdfPath, docUrl }, actor) {
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const info = db.prepare(`
      INSERT INTO sw_batches
        (work_date, shipping_no, bunrui, packing_method, carriers_json, slip_count, note, status, pdf_path, doc_url, created_by, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?)
    `).run(workDate, shippingNo, bunrui, packingMethod,
      JSON.stringify(carriers ?? []), slipCount ?? null, note ?? null,
      pdfPath ?? null, docUrl ?? null, actor, now, now);
    const id = Number(info.lastInsertRowid);
    addStatusEvent(id, null, 'ready', actor, 'admin', 'バッチ作成');
    return id;
  })();
}

/**
 * バッチ更新 (ready のみ・作業開始後は PR5 の手動修正フローで扱う)。
 * 更新できたら true。ready でなくなっていたら false (楽観排他)。
 */
export function updateBatch(id, { bunrui, packingMethod, carriers, slipCount, note, pdfPath, docUrl }, actor) {
  const db = getDB();
  return db.transaction(() => {
    const before = db.prepare('SELECT * FROM sw_batches WHERE id = ?').get(id);
    if (!before || before.status !== 'ready') return { ok: false };
    // PDF は未選択なら既存を保持 (COALESCE)。doc_url は空欄=消したい意図なのでそのまま反映する。
    // ただし両方空になると帳票なしバッチができてしまうため、呼び出し側で検証済みの前提
    const res = db.prepare(`
      UPDATE sw_batches SET bunrui=?, packing_method=?, carriers_json=?, slip_count=?, note=?,
        pdf_path=COALESCE(?, pdf_path), doc_url=?, updated_at=?
      WHERE id=? AND status='ready'
    `).run(bunrui, packingMethod, JSON.stringify(carriers ?? []), slipCount ?? null,
      note ?? null, pdfPath ?? null, docUrl ?? null, utcNow(), id);
    if (res.changes === 0) return { ok: false };
    const after = db.prepare('SELECT * FROM sw_batches WHERE id = ?').get(id);
    addAuditLog(actor, 'batch_update', `sw_batches:${id}`, before, after, null);
    // PDF差し替え時は呼び出し側が旧ファイルを削除する (成功後のみ。Codex PR2レビュー#1)
    return { ok: true, replacedPdf: pdfPath ? before.pdf_path : null };
  })();
}

/**
 * バッチ取消 (ready のみ。hold は進行中セッション・未解除保留を持つため、
 * それらの同時クローズが必要 = PR5 の手動修正フローで扱う。Codex PR2レビュー#2)。
 * 取消できたら true。対象外ステータスなら false。理由必須。
 */
export function cancelBatch(id, actor, reason) {
  const db = getDB();
  return db.transaction(() => {
    const before = db.prepare('SELECT * FROM sw_batches WHERE id = ?').get(id);
    if (!before || before.status !== 'ready') return false;
    db.prepare("UPDATE sw_batches SET status='cancelled', updated_at=? WHERE id=?")
      .run(utcNow(), id);
    addStatusEvent(id, before.status, 'cancelled', actor, 'admin', reason);
    addAuditLog(actor, 'batch_cancel', `sw_batches:${id}`, { status: before.status }, { status: 'cancelled' }, reason);
    return true;
  })();
}

/** 管理画面用: 指定作業日の全バッチ (cancelled 含む・作成順)。 */
export function listAdminBatches(workDate) {
  return getDB().prepare(`
    SELECT b.*,
      (SELECT label FROM sw_masters m WHERE m.kind='shipping_no' AND m.code=b.shipping_no) AS shipping_no_label,
      (SELECT label FROM sw_masters m WHERE m.kind='bunrui' AND m.code=b.bunrui) AS bunrui_label,
      (SELECT label FROM sw_masters m WHERE m.kind='packing_method' AND m.code=b.packing_method) AS packing_method_label
    FROM sw_batches b WHERE b.work_date = ? ORDER BY b.id
  `).all(workDate);
}

export function getBatch(id) {
  return getDB().prepare('SELECT * FROM sw_batches WHERE id = ?').get(id);
}

/**
 * カンバン用: 指定作業日のバッチ + それ以前の未完了持ち越しバッチ。
 * 未来日の未完了は含めない (Codex PR1レビュー#4)。
 * cancelled はカンバンに出さないため取得段階で除外し、件数表示と一致させる (同#8)。
 * active_worker/active_since は進行中セッション (open) の担当者と開始時刻 (PR3: カード表示用)。
 */
export function listKanbanBatches(workDate) {
  return getDB().prepare(`
    SELECT b.*,
      (SELECT label FROM sw_masters m WHERE m.kind='shipping_no' AND m.code=b.shipping_no) AS shipping_no_label,
      (SELECT label FROM sw_masters m WHERE m.kind='bunrui' AND m.code=b.bunrui) AS bunrui_label,
      (SELECT label FROM sw_masters m WHERE m.kind='packing_method' AND m.code=b.packing_method) AS packing_method_label,
      -- 工程が増える PR4 では同一バッチに picking と packing の open が並びうるため、
      -- 最新セッション1件に固定する (スカラーサブクエリの暗黙の先頭行依存を避ける)
      (SELECT s.worker FROM sw_sessions s WHERE s.batch_id=b.id AND s.outcome='open'
        ORDER BY s.id DESC LIMIT 1) AS active_worker,
      (SELECT s.requested_at FROM sw_sessions s WHERE s.batch_id=b.id AND s.outcome='open'
        ORDER BY s.id DESC LIMIT 1) AS active_since
    FROM sw_batches b
    WHERE b.status != 'cancelled'
      AND (b.work_date = ?
        OR (b.work_date < ? AND b.status NOT IN ('done','stock_return')))
    ORDER BY b.work_date, b.id
  `).all(workDate, workDate);
}
