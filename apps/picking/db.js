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
import crypto from 'node:crypto';
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
const SCHEMA_VERSION = 17;

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

// v1 は PR1 マージまで自由に編集してよい (本番デプロイ前のため既存DBは存在しない)。
// マージ後のスキーマ変更は必ず v2 以降の migration として追記する
const MIGRATIONS = {
  1: createCoreTables,
  // v2: 共用端末認証 (2026-08-12 中原さん方針)。
  // 倉庫のiPhoneは複数の作業者が使い回すため、個人ログインではなく
  // 「端末を一度登録 → 作業者は名前タップで選択」方式にする。
  2: () => {
    // 登録済み端末。トークンは平文で保存せずハッシュのみ (漏えい時に使い回せない)
    db.exec(`CREATE TABLE IF NOT EXISTS pk_devices (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash TEXT NOT NULL UNIQUE,
      label      TEXT NOT NULL,        -- 端末名 (例: 倉庫iPhone1)
      created_by TEXT NOT NULL,        -- 登録した管理者 email
      created_at TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at TEXT                  -- 失効 (NULL = 有効)
    )`);
    // 作業者マスタ (現行Notionの担当者selectに相当)。アカウントは持たない
    db.exec(`CREATE TABLE IF NOT EXISTS pk_workers (
      code   TEXT PRIMARY KEY,         -- 例 w01
      name   TEXT NOT NULL,            -- 表示名 (例: 星)
      sort   INTEGER NOT NULL DEFAULT 0,
      active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1))
    )`);
  },
  // v3: 中断 (PR4)。中断中は pause_started_at に開始時刻を持ち、再開時に
  // paused_total_sec へ加算する (中断時間はピッキング時間から除外 — 要件§5.6)
  3: () => {
    db.exec('ALTER TABLE pk_batches ADD COLUMN pause_started_at TEXT');
    db.exec('ALTER TABLE pk_batches ADD COLUMN pause_reason TEXT');
  },
  // v4: Drive自動取込の台帳 (Codex設計相談 2026-08-13)。
  // 「どのDriveファイルのどの版を取り込んだ/失敗した」を記録し、ポーリングの冪等と
  // 失敗の可視化 (黙殺しない) を担う。(drive_file_id, modified_time) が版のキー
  4: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_drive_imports (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      drive_file_id TEXT NOT NULL,
      modified_time TEXT NOT NULL,     -- Drive側の更新時刻 (版の識別)
      filename      TEXT NOT NULL,
      folder_name   TEXT,
      source_type   TEXT NOT NULL DEFAULT 'csv' CHECK(source_type IN ('csv','pdf')),
      status        TEXT NOT NULL CHECK(status IN ('imported','failed','skipped')),
      error         TEXT,
      batch_id      INTEGER REFERENCES pk_batches(id),
      attempts      INTEGER NOT NULL DEFAULT 1,
      first_seen_at TEXT NOT NULL,
      processed_at  TEXT NOT NULL,
      UNIQUE (drive_file_id, modified_time)
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_drive_imports_at ON pk_drive_imports(processed_at)');
  },
  // v5: 端末の用途 (2026-08-17)。倉庫の掲示モニターは常時表示のため物理的に誰でも触れる。
  // 作業用iPhoneと同じ端末Cookieを持たせると、その画面から作業APIを叩けてしまうので
  // kind='board' を作り、掲示ルート以外を router 側で拒否する (読み取り専用端末)
  5: () => {
    db.exec(`ALTER TABLE pk_devices ADD COLUMN kind TEXT NOT NULL DEFAULT 'worker'
      CHECK(kind IN ('worker','board'))`);
  },
  // v6: 🔴ピッキング漏れバッチ (2026-08-21 中原さん指示)。梱包からの再ピック依頼
  // (不足・品違い) をタスク一覧ではなく通常のピッキングバッチとして生成する。
  // origin='repick' は計測 (サマリ/ボード/フロア/Notion) の対象外
  6: () => {
    db.exec("ALTER TABLE pk_batches ADD COLUMN origin TEXT NOT NULL DEFAULT 'import'");
    db.exec('ALTER TABLE pk_batches ADD COLUMN origin_ref TEXT');      // 依頼元 (例: 出荷_02 #95)
    db.exec('ALTER TABLE pk_batches ADD COLUMN requested_by TEXT');    // 依頼者 (梱包担当)
    db.exec('ALTER TABLE pk_batches ADD COLUMN pack_task_id INTEGER'); // pk_pack_tasks.id (状態同期キー)
  },
  // v7: 現場間アラート (2026-08-21 中原さん指示)。ピッキング⇄梱包のヘッダーバナー
  // (カート/台車/リフト/商品下ろし)。picking所有・両アプリからはpicking serviceの関数経由
  7: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_floor_alerts (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      direction    TEXT NOT NULL CHECK(direction IN ('to_packing','to_picking')),
      kind         TEXT NOT NULL,
      message      TEXT NOT NULL,
      requested_by TEXT,
      created_at   TEXT NOT NULL,
      acked_at     TEXT,
      acked_by     TEXT
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_floor_alerts_active ON pk_floor_alerts(direction, acked_at)');
  },
  // v8: ロケーション動線マスタ (NEXTサイン・2026-08-23 中原さん指示)。
  // ピッキングフロア(P3F)を一筆書きに並べた「面」(棚の片側) の定義。表示順 (pk_lines.seq) には
  // 一切影響させず、作業画面の「次はどう動くか」の見せ方にだけ使う。
  // 取込は全置換 + 履歴 (CSV全文+sha256) で、間違った版は前の版を入れ直して戻せる。
  // 中身の規約・検証は apps/picking/location-faces.js、判定は next-sign-core.cjs
  8: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_location_face_imports (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      imported_at  TEXT NOT NULL,
      actor        TEXT NOT NULL,          -- 取込者 email / 'seed' (同梱CSVによる初期化)
      filename     TEXT,
      sha256       TEXT NOT NULL,
      face_count   INTEGER NOT NULL,
      total_slots  INTEGER NOT NULL,       -- 総マス数 (一筆書きの長さ)
      csv_text     TEXT NOT NULL           -- 取り込んだCSV全文 (復元用)
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS pk_location_faces (
      seq_no       INTEGER PRIMARY KEY,    -- 面番号 = 歩く順 (1..N)
      block        TEXT NOT NULL,          -- 'P3FA'
      col          TEXT NOT NULL,          -- '001' (ロケ8桁の1〜3桁目 = 列 = 棚1本)
      ren_from     INTEGER NOT NULL,       -- この面の最初の連 (歩き始める端)
      ren_to       INTEGER NOT NULL,       -- この面の最後の連
      face_kind    TEXT NOT NULL CHECK(face_kind IN ('front','back','cont','single')),
      rack_id      TEXT NOT NULL,          -- 物理的な1本の棚。列をまたぐことがある (A008+B001)
      move_in      TEXT NOT NULL           -- 前の面からこの面へ入る体の動き
        CHECK(move_in IN ('start','forward_turn','turn_around','around_stairs','across_stairs','hang','move')),
      reliable     INTEGER NOT NULL DEFAULT 1 CHECK(reliable IN (0,1)),   -- 0 = 相対表現を出さずコードのみ
      direction    TEXT NOT NULL DEFAULT 'left' CHECK(direction IN ('left','right')), -- 棚に向かって連が増える側
      storage_kind TEXT NOT NULL DEFAULT 'shelf' CHECK(storage_kind IN ('shelf','pocket')),
      slot_from    INTEGER NOT NULL,       -- 一筆書きの通し位置
      slot_to      INTEGER NOT NULL,
      note         TEXT,
      import_id    INTEGER NOT NULL REFERENCES pk_location_face_imports(id),
      UNIQUE (block, col, ren_from)
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_location_faces_col ON pk_location_faces(block, col)');
  },
  // v9: バリエーション (楽天SKU) ごとの画像 (2026-08-25 中原さん指摘: 白抜きは商品共通なので
  // 「香りNo.4」の行に「No.8」の写真が出る)。variants[SKU].images[0] を第一候補にする。
  // 既存キャッシュのうち SKU粒度のコード (ne_code ≠ 商品管理番号) は取り直し対象にする
  9: () => {
    const cols = db.prepare('PRAGMA table_info(pk_product_images)').all().map((c) => c.name);
    if (!cols.includes('variant_image_url')) {
      db.exec('ALTER TABLE pk_product_images ADD COLUMN variant_image_url TEXT');
    }
    db.exec(`UPDATE pk_product_images SET fetched_at = '2000-01-01T00:00:00Z'
      WHERE status = 'ok' AND manage_number IS NOT NULL AND lower(ne_code) <> lower(manage_number)`);
  },
  // v10: 欠品フローv2 (要件『ピッキング欠品フローv2_要件定義_20260826.md』・中原さん承認 2026-08-26)。
  //   欠品ボタン押下〜判断確定を「欠品対応セッション」として計測から除外し (paused_total_sec に加算)、
  //   判断結果 (他ロケで確保した数・残りをどうするか) を明細に持つ。履歴は pk_events が正
  10: () => {
    db.exec('ALTER TABLE pk_batches ADD COLUMN shortage_open_at TEXT');     // 対応中の欠品セッション開始時刻
    db.exec('ALTER TABLE pk_batches ADD COLUMN shortage_open_seq INTEGER'); // その明細 seq
    db.exec('ALTER TABLE pk_lines ADD COLUMN alt_block TEXT');       // 他ロケで確保: ブロック
    db.exec('ALTER TABLE pk_lines ADD COLUMN alt_location TEXT');    // 他ロケで確保: ロケ
    db.exec('ALTER TABLE pk_lines ADD COLUMN alt_qty INTEGER');      // 他ロケで確保した数
    db.exec('ALTER TABLE pk_lines ADD COLUMN remaining_qty INTEGER'); // 確保できなかった残り (0=全量他ロケで確保)
    db.exec("ALTER TABLE pk_lines ADD COLUMN remaining TEXT");        // 残りの扱い: 'later'(後で取りに行く) | 'none'(どこにもない)
  },
  // v11: 欠品フローv2 PR2 — picking↔packing 連携 (要件 ピッキング欠品フローv2_20260826 §4.3/4.4)。
  //   - pk_shortage_allocations: 不足数を**受注に配賦**した結果。梱包画面の 🕒/❌ バッジの元。
  //     ⭐「同一SKUの全伝票に表示」は不採用 (欠品1個で10伝票が保留に見える) — 配賦した伝票だけに出す
  //   - pk_later_requests: 「後で取りに行く」の依頼。梱包バッチが取込済みなら pk_pack_tasks
  //     (kind='repick') へ展開され、既存の再ピック機構 (1行バッチ→受領) にそのまま乗る。
  //     未取込なら pending_binding で待ち、取込後の reconcile/ポーラーで展開する
  11: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_shortage_allocations (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id   INTEGER NOT NULL REFERENCES pk_batches(id),
      line_seq   INTEGER NOT NULL,
      sku        TEXT NOT NULL,
      ne_slip_no TEXT NOT NULL,             -- 配賦先の受注 (NE伝票番号)
      qty        INTEGER NOT NULL,
      kind       TEXT NOT NULL CHECK(kind IN ('later','none')),
      created_at TEXT NOT NULL
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_shortage_alloc_line ON pk_shortage_allocations(batch_id, line_seq)');
    db.exec(`CREATE TABLE IF NOT EXISTS pk_later_requests (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id      INTEGER NOT NULL REFERENCES pk_batches(id),
      line_seq      INTEGER NOT NULL,
      sku           TEXT NOT NULL,
      product_name  TEXT,
      qty           INTEGER NOT NULL,       -- 後で取りに行く数 (= その明細の remaining_qty)
      from_block    TEXT,
      from_location TEXT,                   -- 元ロケ (取りに行く場所の参考)
      requested_by  TEXT NOT NULL,          -- ピッカー
      status        TEXT NOT NULL DEFAULT 'pending_binding'
        CHECK(status IN ('pending_binding','requested','cancelled')),
      merged_task_ids TEXT,                 -- 梱包側の再ピックに合流した先 (カンマ区切り task id)。
                                            -- 合流先が着手済みなら back を拒否するために持つ
      created_at    TEXT NOT NULL,
      updated_at    TEXT NOT NULL
    )`);
    db.exec("CREATE INDEX IF NOT EXISTS idx_pk_later_requests_status ON pk_later_requests(status)");
  },
  // v12: スタッフマスタ (Render apps/staff) との紐付け (2026-09-01 中原さん方針「人の正本は1つ」)。
  //   pk_workers はそのまま残し (code は作業実績が参照する不変キー)、どの staff と同じ人かを持つ。
  //   名前 (pk_workers.name) は同期で staff の表記に寄せるが、過去の実績 (pk_batches.worker 等) は
  //   打刻時点の表示名を保存しているため遡って変わらない = 意図した挙動 (履歴を書き換えない)。
  //   source: 'local' = この picking で作られた行 (staff 未登録) / 'staff' = スタッフマスタ由来
  12: () => {
    const cols = db.prepare('PRAGMA table_info(pk_workers)').all().map((c) => c.name);
    if (!cols.includes('staff_id')) db.exec('ALTER TABLE pk_workers ADD COLUMN staff_id INTEGER');
    if (!cols.includes('staff_no')) db.exec('ALTER TABLE pk_workers ADD COLUMN staff_no TEXT');
    if (!cols.includes('source')) db.exec("ALTER TABLE pk_workers ADD COLUMN source TEXT NOT NULL DEFAULT 'local'");
    // 同じ staff を2行に紐づけない (紐付けの取り違えを DB で防ぐ)。
    // 部分適用済みDB (列だけ足された状態で手を入れた等) に重複があると索引作成が謎のエラーで落ちるため、
    // 先に検出して「どの行が重複か」を出す (Codex R5 Low)
    const dupStaff = db.prepare(`SELECT staff_id, group_concat(code) codes FROM pk_workers
      WHERE staff_id IS NOT NULL GROUP BY staff_id HAVING COUNT(*) > 1`).all();
    if (dupStaff.length) {
      throw new Error('picking migration v12: 同じ staff_id を持つ作業者が複数います → '
        + dupStaff.map(d => `staff_id=${d.staff_id} (${d.codes})`).join(', ')
        + '。どちらか一方の staff_id を NULL にしてから再起動してください');
    }
    db.exec('CREATE UNIQUE INDEX IF NOT EXISTS uq_pk_workers_staff ON pk_workers(staff_id) WHERE staff_id IS NOT NULL');
    // 同期の状態 (1行固定)。最終同期時刻・結果を管理画面に出し、黙って止まるのを防ぐ
    db.exec(`CREATE TABLE IF NOT EXISTS pk_staff_sync_state (
      id            INTEGER PRIMARY KEY CHECK(id = 1),
      synced_at     TEXT NOT NULL,
      ok            INTEGER NOT NULL CHECK(ok IN (0,1)),
      staff_count   INTEGER,
      linked        INTEGER,
      added         INTEGER,
      renamed       INTEGER,
      deactivated   INTEGER,
      unmatched     TEXT,                 -- 警告 (JSON 配列): 紐付け保留・消えた紐付け済み・スタッフマスタ未登録
      generated_at  TEXT,                 -- 今回取得した export の generated_at (失敗時も記録)
      error         TEXT,
      -- 次回の判定基準。成功時だけ進める (失敗で基準が緩むと激減・巻き戻りを見逃す)
      active_staff_count INTEGER,         -- 前回成功時の有効スタッフ数 (激減ガードの基準)
      last_generated_at  TEXT             -- 前回成功時の generated_at (後着した古い応答を弾く)
    )`);
  },
  // v13: 引当分類の出どころを記録する (2026-09-04 障害の再発防止)。
  //   Drive の 引当パターン_*.txt が取れないと importBatch は CSV からの**推定値**で確定してしまい、
  //   「もっともらしい別の分類」が黙って入る (実際に 出荷_17 が《2つ折り》→《3つ折り》になった)。
  //   確定の根拠を残し、推定のときは画面と GChat で分かるようにする。
  //   NULL = v13 より前に取り込んだ行 (出どころ不明) なので警告の対象にしない
  13: () => {
    db.exec('ALTER TABLE pk_batches ADD COLUMN class_source TEXT');   // 'txt' | 'suggested' | 'manual'
  },
  // v14: 現場間バナーに「対象伝票を開く」リンクを持たせる (例外処理監査 PR-1・2026-09-05)。
  //   3階の「在庫なし」を1階の全端末に赤バナーで知らせ、その伝票へ直接飛べるようにする
  //   (以前は在庫なしが1階のどの画面にも出ず、伝票が「⏳再ピック対応待ち」のまま何日も残った)
  //   task_id = 元になった梱包タスク (在庫なしバナー)。タスクが在庫なしでなくなったら resolved_at で消す
  //   (以前はメッセージ文字列でしか重複排除できず、見つかった/届けた後も4時間残った — Codex R1)
  14: () => {
    db.exec('ALTER TABLE pk_floor_alerts ADD COLUMN link TEXT');
    db.exec('ALTER TABLE pk_floor_alerts ADD COLUMN task_id INTEGER');
    db.exec('ALTER TABLE pk_floor_alerts ADD COLUMN resolved_at TEXT');
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_floor_alerts_task ON pk_floor_alerts(task_id, kind)');
  },
  // v15: バナーの汎用キー (例外処理監査 PR-2)。ピッカーの欠品 (🕒後で/❌どこにもない) を1階の全端末へ出すバナーは
  //   タスクではなく配賦 (受注) に紐づくので、'alloc:<batch>:<seq>:<ne_slip_no>' を ref_key に持ち、
  //   back で取り消したとき・1階が閉じたときに ref_key で解決する
  15: () => {
    db.exec('ALTER TABLE pk_floor_alerts ADD COLUMN ref_key TEXT');
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_floor_alerts_ref ON pk_floor_alerts(ref_key)');
  },
  // v16 (例外処理監査 PR-5): 再ピックバッチの理由。'later' = ピッカー自身の「後で取りに行く」/ 'shortage' = 梱包の不足 /
  //   'wrong_item' = 梱包の品違い。名前 (hikiate_class) と作業画面の案内 (元ロケの意味・届け先) を分けるため。
  //   既存行は reconcileRepickBatches が pk_pack_tasks から埋める
  16: () => {
    db.exec('ALTER TABLE pk_batches ADD COLUMN repick_reason TEXT');
    // 再ピックバッチ ↔ タスクの結合 (reconcile・同期) 用
    db.exec("CREATE INDEX IF NOT EXISTS idx_pk_batches_repick_task ON pk_batches(pack_task_id) WHERE origin = 'repick'");
  },
  // v17 (例外処理監査 PR-6): 「後で取りに行く」依頼を誰がいつ取り下げたか (管理画面からの取り下げ・back)
  17: () => {
    db.exec('ALTER TABLE pk_later_requests ADD COLUMN cancelled_by TEXT');
    db.exec('ALTER TABLE pk_later_requests ADD COLUMN cancelled_at TEXT');
  },
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
    csv_sha256      TEXT NOT NULL,          -- 取込CSVのハッシュ (同一内容の再送判定・監査)
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

  // 取込の監査ログ (追記型・削除不可)。上書き取込で「誰が何件から何件へ変えたか」を残す
  db.exec(`CREATE TABLE IF NOT EXISTS pk_import_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES pk_batches(id),
    tb_no       TEXT NOT NULL,
    action      TEXT NOT NULL CHECK(action IN ('create','overwrite')),
    csv_sha256  TEXT NOT NULL,
    hikiate_class TEXT NOT NULL,
    folder_name TEXT,
    line_count  INTEGER NOT NULL,
    slip_count  INTEGER NOT NULL,
    total_qty   INTEGER NOT NULL,
    before_json TEXT,               -- 上書き時: 変更前の集計値・分類・ハッシュ
    actor       TEXT NOT NULL,
    at          TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_pk_import_logs_batch ON pk_import_logs(batch_id, id)');

  // 楽天白抜き画像キャッシュ (PR3 で書き込み)。ne_code = NE商品コード (小文字正規化)
  db.exec(`CREATE TABLE IF NOT EXISTS pk_product_images (
    ne_code       TEXT PRIMARY KEY,
    manage_number TEXT,               -- 解決した楽天商品管理番号
    white_bg_url  TEXT,               -- 白抜き画像 (商品共通・第二候補)
    top_image_url TEXT,               -- images[0] (フォールバック)
    variant_image_url TEXT,           -- バリエーション画像 variants[SKU].images[0] (第一候補・v9)
    status        TEXT NOT NULL DEFAULT 'ok' CHECK(status IN ('ok','not_found','error')),
    fetched_at    TEXT NOT NULL
  )`);
}

export function getDB() {
  if (!db) initPickingDB();
  return db;
}

/** バッチ一覧: 指定作業日のみ (日付が変わるとリセット — 中原さん指示 2026-08-18。
 *  梱包 #852 と同じ仕様。過去分は日付ピッカーで参照)。 */
export function listBatches(workDate) {
  return getDB().prepare(`
    SELECT b.*,
      (SELECT COUNT(*) FROM pk_lines l WHERE l.batch_id = b.id AND l.status != 'pending') AS done_lines
    FROM pk_batches b
    WHERE b.work_date = ?
    ORDER BY b.work_date, b.id
  `).all(workDate);
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

// ─── 共用端末 (v2) ───

const hashToken = (t) => crypto.createHash('sha256').update(String(t)).digest('hex');

export const DEVICE_KINDS = ['worker', 'board'];

/**
 * 端末を登録し、平文トークンを返す (保存はハッシュのみ。トークンはこの1回しか得られない)。
 * kind='board' は掲示モニター用 = 読み取り専用 (作業画面・作業APIは router が拒否する)。
 */
export function createDevice(label, actor, kind = 'worker') {
  if (!DEVICE_KINDS.includes(kind)) throw new Error(`不正な端末用途: ${kind}`);
  const token = crypto.randomBytes(32).toString('base64url');
  getDB().prepare(`
    INSERT INTO pk_devices (token_hash, label, created_by, created_at, kind)
    VALUES (?, ?, ?, ?, ?)
  `).run(hashToken(token), String(label).trim(), actor, utcNow(), kind);
  return token;
}

// 端末トークンの有効期間 (サーバー側でも検証する。Cookie の Max-Age だけに頼ると
// 盗まれたトークンを手動送信された場合に失効するまで無期限に使えてしまう)
const DEVICE_TTL_MS = 400 * 24 * 3600 * 1000;

/** トークン検証。有効なら端末行を返し last_seen_at を更新 (1時間に1回程度に間引く)。 */
export function verifyDevice(token) {
  if (!token) return null;
  const db = getDB();
  const row = db.prepare(
    'SELECT * FROM pk_devices WHERE token_hash = ? AND revoked_at IS NULL'
  ).get(hashToken(token));
  if (!row) return null;
  const now = utcNow();
  if (Date.parse(now) - Date.parse(row.created_at) > DEVICE_TTL_MS) return null;   // 期限切れ=再登録
  if (!row.last_seen_at || Date.parse(now) - Date.parse(row.last_seen_at) > 3600_000) {
    db.prepare('UPDATE pk_devices SET last_seen_at = ? WHERE id = ?').run(now, row.id);
  }
  return row;
}

export function revokeDevice(id) {
  return getDB().prepare(
    'UPDATE pk_devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL'
  ).run(utcNow(), id).changes > 0;
}

export function listDevices() {
  return getDB().prepare('SELECT id, label, kind, created_by, created_at, last_seen_at, revoked_at FROM pk_devices ORDER BY id').all();
}

// ─── 作業者マスタ (v2) ───

export function listWorkers(includeInactive = false) {
  return getDB().prepare(
    `SELECT code, name, sort, active, staff_id, staff_no, source FROM pk_workers ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY sort, code`
  ).all();
}

export function getWorker(code) {
  return getDB().prepare('SELECT code, name, active FROM pk_workers WHERE code = ?').get(code);
}

/** 作業者の追加。code は自動採番 (w01, w02, …)。 */
export function addWorker(name) {
  const db = getDB();
  const n = db.prepare("SELECT COUNT(*) c FROM pk_workers").get().c + 1;
  const code = `w${String(n).padStart(2, '0')}`;
  db.prepare('INSERT INTO pk_workers (code, name, sort) VALUES (?, ?, ?)').run(code, String(name).trim(), n);
  return code;
}

export function setWorkerActive(code, active) {
  return getDB().prepare('UPDATE pk_workers SET active = ? WHERE code = ?')
    .run(active ? 1 : 0, code).changes > 0;
}
