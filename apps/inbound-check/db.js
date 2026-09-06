/**
 * 入荷受付チェック (apps/inbound-check) — データ層
 *
 * 接続は warehouse-mirror の getMirrorDB() を共有 (Render 完結)。テーブルは本モジュールが
 * 初回利用時に冪等作成する (mirror 本体の createTables に足さない = mirror 初期化を道連れにしない)。
 *
 * 設計正本 = AI_reference『システム設計/入荷受付チェック_要件定義_20260901.md』v1.1 §7
 *  - 取込バッチ単位: 一覧も確認状態も batch に紐づく。新バッチを active にした時点で全行 unchecked
 *  - 同時操作: line_state の原子的な条件付き UPDATE + version。負けたら conflict + 現在状態を返す
 *  - 旧 batch_id を持った操作は stale_batch で拒否
 *  - 0件は正常。拒否は「ヘッダ不正 / 列数不一致 / 壊れたSJIS / 同一ハッシュ / 生成時刻が active より古い」のみ
 *  - events は append-only (取消は reverted_event_id で打ち消し先を指す)
 */
import crypto from 'crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { getImageMap, queueEnsureImages } from '../picking/images.js';
import { listTapCandidates, getStaffByNo, tapName } from '../staff/db.js';
import { parseInboundCsv } from './csv.js';
// いろは在庫化アプリのタスク (PR-B): 「いろはで在庫化」の確定と同じトランザクションで f_iroha_tasks に 1 枚作る。
// 取消 (やり直し・再取込) も同じ tx でタスク側へ伝える。同じ warehouse-mirror.db の同じ接続
import { createTaskForDestination } from '../iroha-work/task-intake.js';
import { requestCancellation } from '../iroha-work/tasks-db.js';

const utcNow = () => new Date().toISOString();

/**
 * 業務日 (JST の日付)。**サーバーが受け取った時刻**で決める。
 * クライアント時刻や CSV 内の日付は使わない (端末の時計ずれで前日の一覧に混ざるため)。
 */
export function workDateJst(d = new Date()) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}
const hashToken = t => crypto.createHash('sha256').update(String(t)).digest('hex');

/**
 * 登録コード専用のハッシュ。6桁 = 100万通りしかないので、単純な sha256 だと
 * DB が漏れた時点で総当たりで逆算できてしまう。サーバー秘密鍵で HMAC して、
 * DB だけでは復元できないようにする (秘密鍵が変われば既存コードは無効 = 10分で失効するので実害なし)。
 */
function enrollHash(code) {
  const secret = process.env.SESSION_SECRET || process.env.STAFF_EXPORT_TOKEN || 'inbound-check-enroll';
  return crypto.createHmac('sha256', secret).update(String(code)).digest('hex');
}

export const BATCH_SOURCES = ['auto', 'drive_retry', 'manual_upload'];
export const RETENTION_DAYS = 365;      // events / batches / line_state
export const LOG_RETENTION_DAYS = 90;   // 取込ログ (失敗理由)
// 端末トークンの有効期間 (Cookie の Max-Age だけに頼らずサーバー側でも検証)
export const DEVICE_TTL_MS = 400 * 24 * 3600 * 1000;

let ensured = false;
let ensuredFor = null;

/** テーブルを冪等作成した接続を返す */
export function getDB() {
  const db = getMirrorDB();
  if (!ensured || ensuredFor !== db) {
    createTables(db);
    ensured = true;
    ensuredFor = db;
  }
  return db;
}

export function createTables(db = getMirrorDB()) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS f_inbound_check_batches (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      source           TEXT NOT NULL CHECK (source IN ('auto','drive_retry','manual_upload')),
      file_name        TEXT,
      file_hash        TEXT NOT NULL UNIQUE,
      csv_generated_at TEXT NOT NULL,
      data_max_at      TEXT,             -- 明細の 更新日時/作成日時 の最大 (CSV 内の信頼できる時刻。0件なら NULL)
      row_count        INTEGER NOT NULL CHECK (row_count >= 0),
      slip_count       INTEGER NOT NULL CHECK (slip_count >= 0),
      imported_at      TEXT NOT NULL,
      imported_by      TEXT,
      status           TEXT NOT NULL CHECK (status IN ('active','superseded')),
      note             TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS uq_ic_batches_active ON f_inbound_check_batches(status) WHERE status = 'active';

    CREATE TABLE IF NOT EXISTS f_inbound_check_slips (
      batch_id      INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      ar_no         TEXT NOT NULL,
      planned_date  TEXT,
      received_date TEXT,
      status        TEXT,
      line_count    INTEGER NOT NULL,
      seq           INTEGER NOT NULL,
      PRIMARY KEY (batch_id, ar_no)
    );

    CREATE TABLE IF NOT EXISTS f_inbound_check_lines (
      batch_id      INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      line_key      TEXT NOT NULL,
      ar_no         TEXT NOT NULL,
      line_no       INTEGER NOT NULL,
      detail_no     INTEGER NOT NULL,
      product_id    TEXT NOT NULL,
      code_key      TEXT NOT NULL,
      product_name  TEXT,
      barcode       TEXT,
      planned_qty   INTEGER NOT NULL,
      received_qty  INTEGER,
      seq           INTEGER NOT NULL,
      PRIMARY KEY (batch_id, line_key)
    );
    CREATE INDEX IF NOT EXISTS idx_ic_lines_code ON f_inbound_check_lines(batch_id, code_key);

    CREATE TABLE IF NOT EXISTS f_inbound_check_line_state (
      batch_id       INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      line_key       TEXT NOT NULL,
      status         TEXT NOT NULL CHECK (status IN ('unchecked','checked')),
      version        INTEGER NOT NULL DEFAULT 1,
      checked_by     TEXT,
      checked_device TEXT,
      checked_at     TEXT,
      PRIMARY KEY (batch_id, line_key)
    );

    CREATE TABLE IF NOT EXISTS f_inbound_check_events (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id          INTEGER NOT NULL REFERENCES f_inbound_check_batches(id) ON DELETE CASCADE,
      line_key          TEXT NOT NULL,
      ar_no             TEXT NOT NULL,
      action            TEXT NOT NULL CHECK (action IN ('check','uncheck')),
      worker            TEXT,
      device_id         INTEGER,
      device_label      TEXT,
      created_at        TEXT NOT NULL,
      reverted_event_id INTEGER REFERENCES f_inbound_check_events(id)
    );
    CREATE INDEX IF NOT EXISTS idx_ic_events_batch ON f_inbound_check_events(batch_id, line_key, id);

    -- 入荷ごとの行き先 (いろはで在庫化 / B-Faith で入庫) の実績。
    -- ⚠batch_id は値として持つだけで FK にしない: バッチは保持期間で消えるが、
    --   「いつ・何を・何個いろはへ送ることにしたか」は後から見返せないと困るため残す。
    -- append-only: 消し込みを取り消したときも行は消さず cancelled_at を立てる
    CREATE TABLE IF NOT EXISTS f_inbound_check_destinations (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id      INTEGER NOT NULL,
      line_key      TEXT NOT NULL,
      ar_no         TEXT NOT NULL,
      product_id    TEXT NOT NULL,
      product_name  TEXT,
      planned_qty   INTEGER,
      destination   TEXT NOT NULL CHECK (destination IN ('iroha','bfaith')),
      decided_from  TEXT NOT NULL CHECK (decided_from IN ('master','chosen')),
      worker        TEXT,
      staff_id      INTEGER,
      device_label  TEXT,
      decided_at    TEXT NOT NULL,
      cancelled_at  TEXT,
      cancelled_by  TEXT,
      expiry_date   TEXT              -- 期限管理商品のときに入力した有効期限 (YYYY-MM-DD / YYYY-MM)
    );
    CREATE INDEX IF NOT EXISTS idx_ic_dest_at ON f_inbound_check_destinations(decided_at);
    CREATE INDEX IF NOT EXISTS idx_ic_dest_line ON f_inbound_check_destinations(batch_id, line_key, id);

    -- 商品ごとの「期限管理あり/なし」。
    -- ⚠ロジザードの商品マスタに設定がある項目だが、入荷受付CSV には出てこない。
    --   当面は在庫データ (mirror_logizard_stock の有効期限) から推定し、違っていれば
    --   この表で人が上書きする。将来ロジザード商品マスタを取り込めたら source='logizard' で埋める
    CREATE TABLE IF NOT EXISTS f_inbound_check_product_flags (
      code_key       TEXT PRIMARY KEY,
      expiry_managed INTEGER NOT NULL CHECK (expiry_managed IN (0,1)),
      source         TEXT NOT NULL CHECK (source IN ('manual','logizard')),
      updated_at     TEXT NOT NULL,
      updated_by     TEXT
    );

    -- いろは作業仕様マスタ (旧「作業内容管理マスター」シートの DB 化。work-master.js が使う)。
    -- ⭐持つのは「いろは作業に固有の属性」だけ。商品名・仕入先・取扱区分は mirror_products を JOIN。
    --   在庫化要否 (旧 在庫化必要FLG) はここに持たない — 正本は f_inbound_info.いろは在庫化作業有無。
    --   units_per_container = いろはで1容器に詰める数。f_inbound_info.入数 (仕入箱入数) とは別概念
    CREATE TABLE IF NOT EXISTS f_iroha_work_master (
      code_key            TEXT PRIMARY KEY,
      商品コード          TEXT NOT NULL,
      material_code       TEXT,
      storage_container   TEXT,
      units_per_container INTEGER CHECK (units_per_container IS NULL OR units_per_container >= 0),
      process_count       INTEGER CHECK (process_count IS NULL OR process_count >= 0),
      note                TEXT,
      version             INTEGER NOT NULL DEFAULT 1,
      updated_at          TEXT NOT NULL,
      updated_by          TEXT
    );

    -- Notion sweep の多重実行防止 lease (notion-sync.js。期限切れは自動回収 = 永久ロックにならない)
    CREATE TABLE IF NOT EXISTS f_inbound_check_notion_lease (
      id         INTEGER PRIMARY KEY CHECK (id = 1),
      holder     TEXT NOT NULL,
      expires_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS f_inbound_check_import_log (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      at         TEXT NOT NULL,
      actor      TEXT,
      source     TEXT NOT NULL,
      file_name  TEXT,
      ok         INTEGER NOT NULL CHECK (ok IN (0,1)),
      batch_id   INTEGER,
      message    TEXT
    );

    -- iPad をログインなしで登録するための使い捨てコード (管理者が PC で発行 → iPad で入力)。
    -- ⚠ホーム画面の PWA と Safari は Cookie 保存領域が別なので、PWA 側で登録を完結させる必要がある
    CREATE TABLE IF NOT EXISTS f_inbound_check_enroll_codes (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      code_hash  TEXT NOT NULL UNIQUE,
      label      TEXT NOT NULL,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      used_at    TEXT,
      used_device_id INTEGER,
      attempts   INTEGER NOT NULL DEFAULT 0
    );

    -- 登録コードの引き換え試行 (総当たり対策のレート制限に使う)。
    -- ⚠ここに記録するのは「試したこと」だけで、入力されたコードは保存しない
    CREATE TABLE IF NOT EXISTS f_inbound_check_enroll_attempts (
      id   INTEGER PRIMARY KEY AUTOINCREMENT,
      ip   TEXT,
      at   TEXT NOT NULL,
      ok   INTEGER NOT NULL CHECK (ok IN (0,1))
    );
    CREATE INDEX IF NOT EXISTS idx_ic_enroll_attempts ON f_inbound_check_enroll_attempts(at);

    -- 数えた数の正本。**append-only・1タップ1行**。
    -- ⚠合計を上書きする作りにすると、A が 11箱・B が 3箱 を同時に保存したとき片方が消える。
    --   足し算 (add) と打ち消し (reversal) だけで表現し、UPDATE / DELETE はしない。
    -- ⚠論理キーは batch_id ではなく (work_date, line_key, code_key)。
    --   同日中に何度取り込んでも数えた数が引き継がれ、翌日は自然に 0 から始まる。
    -- client_event_id = 1タップごとにクライアントが振る UUID。**再送は同じIDを使う**ので
    --   「コミットされたが応答だけ失われた」ケースで二重加算しない。
    CREATE TABLE IF NOT EXISTS f_inbound_check_quantity_events (
      event_seq          INTEGER PRIMARY KEY AUTOINCREMENT,
      client_event_id    TEXT NOT NULL UNIQUE,
      work_date          TEXT NOT NULL,
      batch_id           INTEGER NOT NULL,
      line_key           TEXT NOT NULL,
      code_key           TEXT NOT NULL,
      ar_no              TEXT NOT NULL,
      product_id         TEXT NOT NULL,
      action             TEXT NOT NULL CHECK (action IN ('add','reversal')),
      quantity           INTEGER NOT NULL CHECK (quantity > 0),
      input_kind         TEXT NOT NULL CHECK (input_kind IN ('box','loose','fill_remaining','correction','backfill')),
      unit_size          INTEGER CHECK (unit_size IS NULL OR unit_size > 0),
      reverses_event_seq INTEGER REFERENCES f_inbound_check_quantity_events(event_seq),
      replaces_event_seq INTEGER REFERENCES f_inbound_check_quantity_events(event_seq),
      worker             TEXT NOT NULL,
      staff_id           INTEGER,
      device_id          INTEGER,
      device_label       TEXT,
      client_occurred_at TEXT,
      received_at        TEXT NOT NULL,
      CHECK ((action = 'add' AND reverses_event_seq IS NULL)
          OR (action = 'reversal' AND reverses_event_seq IS NOT NULL AND replaces_event_seq IS NULL))
    );
    -- 同じ加算を二度打ち消せない
    CREATE UNIQUE INDEX IF NOT EXISTS uq_ic_qty_reversal ON f_inbound_check_quantity_events(reverses_event_seq)
      WHERE reverses_event_seq IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_ic_qty_logical ON f_inbound_check_quantity_events(work_date, line_key, code_key, event_seq);

    CREATE TABLE IF NOT EXISTS f_inbound_check_devices (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash   TEXT NOT NULL UNIQUE,
      label        TEXT NOT NULL,
      created_by   TEXT NOT NULL,
      created_at   TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at   TEXT
    );

    -- 🏷 値札 (BCシール) の印刷ジョブ f_inbound_check_print_jobs は下の ensurePrintJobsTable() で作る
    --    (列の追加・NOT NULL の緩和は CREATE IF NOT EXISTS では効かないため、作り直しの経路を持つ)

    -- 🔍 商品のバーコード (JAN / FNSKU) の控え。入荷受付伝票に無い商品の値札を出すために要る。
    --    Render には完全なバーコードマスタが無い (ロジザードの商品マスタ CSV にバーコード列は無く、
    --    在庫ミラーは在庫ゼロの商品の行が消え、値札CSVは入荷予定7日分だけ)。
    --    見つけたとき (取込行・在庫ミラー・入荷予定) に控え、無ければ人が入れる。source で出どころを残す
    CREATE TABLE IF NOT EXISTS f_inbound_check_barcodes (
      code_key     TEXT PRIMARY KEY,
      barcode      TEXT NOT NULL,
      barcode_type TEXT NOT NULL CHECK (barcode_type IN ('jan','fnsku')),
      source       TEXT NOT NULL CHECK (source IN ('line','stock','schedule','manual')),
      updated_at   TEXT NOT NULL,
      updated_by   TEXT
    );

  `);
  ensurePrintJobsTable(db);
  // 🏷 印刷エージェント (倉庫PC) も同じ端末表で扱う (kind で区別)。iPad と同じ発行・失効の導線に乗せる。
  //   printer_name は**サーバー側が端末に紐づけて持つ** (エージェント側の設定ミスで別のプリンターに出さない)
  addCol(db, 'f_inbound_check_devices', 'kind', "TEXT NOT NULL DEFAULT 'ipad'");
  addCol(db, 'f_inbound_check_devices', 'printer_name', 'TEXT');
  addCol(db, 'f_inbound_check_devices', 'heartbeat_at', 'TEXT');
  addCol(db, 'f_inbound_check_devices', 'heartbeat_note', 'TEXT');
  addCol(db, 'f_inbound_check_devices', 'heartbeat_json', 'TEXT');
  // 作業者表は PR1 (#1055) で作ったが、スタッフマスタ (apps/staff) に一本化したので廃止。
  // 無条件 DROP はしない (Codex R4 High): 行が 0 のときだけ落とす。誰かが先に登録していたら表を残して警告
  // (その名前はスタッフマスタに手で登録してから、この表を手動で落とす)
  const legacy = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_inbound_check_workers'").get();
  if (legacy) {
    const n = db.prepare('SELECT COUNT(*) c FROM f_inbound_check_workers').get().c;
    if (n === 0) db.exec('DROP TABLE f_inbound_check_workers');
    else console.warn(`[inbound-check] 旧 f_inbound_check_workers に ${n} 行あるため DROP しません (スタッフマスタへ移してから手動で削除)`);
  }
  // 確認イベントに staff_id (スタッフマスタの id) を後付け。worker (表示名) は従来どおり残す
  const cols = db.prepare('PRAGMA table_info(f_inbound_check_events)').all().map(c => c.name);
  if (!cols.includes('staff_id')) db.exec('ALTER TABLE f_inbound_check_events ADD COLUMN staff_id INTEGER');

  // 取込バッチの業務日 (JST)。同日中の再取込で確認状態を引き継ぐ判定に使う。
  // ⚠これが無いと 11:45 の取込で午前中の確認が全部消える (miniPC は 08:40 と 11:45 の2回取得する)
  const bcols = db.prepare('PRAGMA table_info(f_inbound_check_batches)').all().map(c => c.name);
  if (!bcols.includes('work_date')) {
    db.exec('ALTER TABLE f_inbound_check_batches ADD COLUMN work_date TEXT');
    // 既存バッチは取込時刻から埋める (UTC 保存なので +9 時間して JST の日付にする)
    db.prepare("UPDATE f_inbound_check_batches SET work_date = date(imported_at, '+9 hours') WHERE work_date IS NULL").run();
  }
  db.exec('CREATE INDEX IF NOT EXISTS idx_ic_batches_work_date ON f_inbound_check_batches(work_date, id)');

  migrateQuantity(db);
}

/**
 * 🏷 値札 (BCシール) の印刷ジョブ (print-queue.js)。iPad が積み、倉庫PCの印刷エージェントが pull で取る。
 * 🚨 lease した時点でジョブ JSON を渡している (= 紙が出たかもしれない) ので、期限切れでも queued へ戻さない。
 *    状態は安全の要なので CHECK で DB 側にも書く (未知の状態が入ると監視から外れて「気づかないまま出ない」になる)
 * source = 'line' (入荷受付伝票の明細から) / 'product' (商品を探して出す = 伝票に無い商品)。
 *   product のとき batch_id / line_key は NULL
 */
const PRINT_JOBS_COLUMNS = `
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      client_request_id TEXT NOT NULL UNIQUE,      -- iPad の冪等ID (二重タップ・応答消失の再送で2枚出ない)
      source            TEXT NOT NULL DEFAULT 'line' CHECK (source IN ('line','product')),
      batch_id          INTEGER,                   -- source='line' のとき必須
      line_key          TEXT,
      code_key          TEXT NOT NULL,
      product_code      TEXT NOT NULL,
      product_name      TEXT NOT NULL,
      barcode           TEXT NOT NULL,
      barcode_type      TEXT NOT NULL CHECK (barcode_type IN ('jan','fnsku')),
      pack_qty          TEXT NOT NULL DEFAULT '',  -- 入数 (空 = 印字しない)
      copies            INTEGER NOT NULL CHECK (copies BETWEEN 1 AND 50),
      printer_name      TEXT NOT NULL,             -- 積んだ時点の出力先。エージェントはこの名前にだけ出す
      target_device_id  INTEGER NOT NULL REFERENCES f_inbound_check_devices(id),
      requested_by      TEXT,
      requested_device  TEXT,
      acknowledged_job_id INTEGER,                -- 直前の unknown ジョブを「実物を見て出ていなかった」と確認した証跡 (その ID)
      acknowledged_at   TEXT,                     -- (unknown 側) 人が実物を確認して再発行した時刻。以後この lease の遅延報告は受け付けない
      state             TEXT NOT NULL CHECK (state IN ('queued','leased','submitted','completed','failed','manual','unknown')),
      lease_device_id   INTEGER REFERENCES f_inbound_check_devices(id),
      lease_token       TEXT,                      -- 報告時の照合 (別の端末・古い lease の報告を弾く)
      lease_expires_at  TEXT,                      -- 報告の受付期限 (過ぎたら unknown)
      spool_job_id      TEXT,
      error             TEXT,
      created_at        TEXT NOT NULL,
      updated_at        TEXT NOT NULL,
      leased_at         TEXT,
      submitted_at      TEXT,
      finished_at       TEXT,
      alerted_state     TEXT,                      -- 通知し終えた状態 (送信成功後にだけ入れる)
      CHECK (state NOT IN ('leased','submitted')
             OR (lease_device_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
      CHECK (source <> 'line' OR (batch_id IS NOT NULL AND line_key IS NOT NULL))
`;
const PRINT_JOBS_INDEXES = (name) => `
    CREATE INDEX IF NOT EXISTS idx_ic_print_jobs_state ON ${name}(state, id);
    CREATE INDEX IF NOT EXISTS idx_ic_print_jobs_line ON ${name}(batch_id, line_key, id);
    CREATE INDEX IF NOT EXISTS idx_ic_print_jobs_product ON ${name}(source, code_key, id);
`;

/**
 * 印刷ジョブ表を作る。既に旧版 (batch_id NOT NULL・source 列なし = 2026-09-05 の初版) があれば
 * **1回だけ作り直して行を写す** — CREATE IF NOT EXISTS は列も NOT NULL も変えないので ([[feedback_schema_change_needs_migration_and_real_test]])。
 * 進行中のジョブ (leased/submitted) も写すので、エージェントの報告は作り直しをまたいで通る (id は保つ)
 */
function ensurePrintJobsTable(db) {
  const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_inbound_check_print_jobs'").get();
  if (!exists) {
    db.exec(`CREATE TABLE f_inbound_check_print_jobs (${PRINT_JOBS_COLUMNS});` + PRINT_JOBS_INDEXES('f_inbound_check_print_jobs'));
    return { created: true };
  }
  const cols = db.prepare('PRAGMA table_info(f_inbound_check_print_jobs)').all();
  const byName = Object.fromEntries(cols.map(c => [c.name, c]));
  const needsRebuild = !byName.source || (byName.batch_id && byName.batch_id.notnull === 1) || (byName.line_key && byName.line_key.notnull === 1);
  if (!needsRebuild) {
    db.exec(PRINT_JOBS_INDEXES('f_inbound_check_print_jobs'));
    return { created: false, rebuilt: false };
  }
  // 旧版 → 新版へ写す。旧版に無い列 (source) は既定値 'line' で埋まる。列名は両方にあるものだけ並べる
  const newCols = PRINT_JOBS_COLUMNS.split('\n').map(l => l.trim()).filter(l => /^[a-z_]+\s/.test(l)).map(l => l.split(/\s+/)[0]);
  const common = newCols.filter(c => byName[c]);
  let rows = 0;
  db.transaction(() => {
    db.exec('DROP TABLE IF EXISTS f_inbound_check_print_jobs__new');
    db.exec(`CREATE TABLE f_inbound_check_print_jobs__new (${PRINT_JOBS_COLUMNS})`);
    rows = db.prepare(`INSERT INTO f_inbound_check_print_jobs__new (${common.join(', ')}) SELECT ${common.join(', ')} FROM f_inbound_check_print_jobs`).run().changes;
    db.exec('DROP TABLE f_inbound_check_print_jobs');
    db.exec('ALTER TABLE f_inbound_check_print_jobs__new RENAME TO f_inbound_check_print_jobs');
    db.exec(PRINT_JOBS_INDEXES('f_inbound_check_print_jobs'));
  })();
  console.log(`[inbound-check] f_inbound_check_print_jobs を作り直しました (source 列 / batch_id NULL 可。${rows} 行を引き継ぎ)`);
  return { created: false, rebuilt: true, rows };
}

/** 列がなければ足す (SQLite の ALTER TABLE ADD COLUMN は冪等でないので自前で見る) */
function addCol(db, table, col, ddl) {
  const cols = db.prepare('PRAGMA table_info(' + table + ')').all().map(c => c.name);
  if (cols.includes(col)) return false;
  db.exec('ALTER TABLE ' + table + ' ADD COLUMN ' + col + ' ' + ddl);
  return true;
}

/**
 * 数量 (部分確認) のための列追加と、既存データのバックフィル。
 *
 * ⭐**バックフィルまでが1セット**。既に確認済みの行を found_qty=0 のまま残すと、
 *   画面に「0 / 106個」と出て現場が数え直す羽目になる。さらに同日の再取込で
 *   数量イベントの集計が 0 になり、午後の取込で数量が消える。
 *   合成の add イベント (client_event_id='backfill:...') まで作って初めて整合する。
 */
function migrateQuantity(db) {
  const added = addCol(db, 'f_inbound_check_line_state', 'found_qty', 'INTEGER NOT NULL DEFAULT 0');
  addCol(db, 'f_inbound_check_line_state', 'quantity_version', 'INTEGER NOT NULL DEFAULT 1');
  addCol(db, 'f_inbound_check_line_state', 'quantity_work_date', 'TEXT');
  // exact = 予定どおり / shortage = 不足のまま確定 / excess = 予定より多い
  addCol(db, 'f_inbound_check_line_state', 'finalized_result', 'TEXT');
  // 確定時に作った行き先台帳の行。やり直したときに「その行だけ」取り消すために持つ
  addCol(db, 'f_inbound_check_line_state', 'destination_id', 'INTEGER');
  // 今から数える箱の入数 (今回の検品だけに使う。商品マスタには書かない)
  addCol(db, 'f_inbound_check_line_state', 'current_pack_qty', 'INTEGER');

  addCol(db, 'f_inbound_check_events', 'client_operation_id', 'TEXT');
  addCol(db, 'f_inbound_check_events', 'result', 'TEXT');
  addCol(db, 'f_inbound_check_events', 'found_qty', 'INTEGER');
  addCol(db, 'f_inbound_check_events', 'planned_qty_snapshot', 'INTEGER');
  addCol(db, 'f_inbound_check_events', 'quantity_version', 'INTEGER');
  addCol(db, 'f_inbound_check_events', 'destination_id', 'INTEGER');
  addCol(db, 'f_inbound_check_events', 'client_occurred_at', 'TEXT');
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_ic_check_operation ON f_inbound_check_events(client_operation_id)
    WHERE client_operation_id IS NOT NULL`);

  addCol(db, 'f_inbound_check_destinations', 'work_date', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'code_key', 'TEXT');
  // 実際に見つけた数。いろはへ送る数は予定数ではなくこちらが正しい
  addCol(db, 'f_inbound_check_destinations', 'actual_qty', 'INTEGER');
  addCol(db, 'f_inbound_check_destinations', 'cancel_reason', 'TEXT');
  // 確認前に詳細パネルから先に入れておける有効期限 (中原さん 2026-09-02:
  // 「詳細の期限管理のところで期限を入れられるように。入れてあれば確認時に聞かなくていい」)。
  // 期限は入荷ごとに変わるので line_state (バッチ限り) に持つ — 翌日には自然に消える
  addCol(db, 'f_inbound_check_line_state', 'pending_expiry', 'TEXT');
  // Notion 作業カード (いろは行き) の outbox 状態 (notion-sync.js が使う)。
  // 作成の成功 (synced_at) と取消反映の成功 (cancelled_at) は別の列に持つ — synced_at を
  // 取消時に上書きすると「いつカードを作ったか」が消える (Codex設計相談R1 2026-09-02)
  addCol(db, 'f_inbound_check_destinations', 'notion_page_id', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_synced_at', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_payload', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_error', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_attempt_count', 'INTEGER');
  addCol(db, 'f_inbound_check_destinations', 'notion_next_retry_at', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_cancelled_at', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_cancel_error', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_cancelled_prev_status', 'TEXT');
  // 回収用の永続ランダムキー (カードの「台帳キー」プロパティと対)。行IDは DB 作り直しで
  // 振り直されるため回収キーにしない (Codex R1 #8)。カード作成の**前に**保存される
  addCol(db, 'f_inbound_check_destinations', 'notion_dedupe_key', 'TEXT');
  // 取消反映の再試行は送信側 (notion_next_retry_at) と**別の列**で制御する。
  // 共用すると、送信エラーで永久ブロックした行の「取消」まで巻き込まれ、
  // 取消済みの作業指示が Notion に有効なまま残る (Codex R3 High)
  addCol(db, 'f_inbound_check_destinations', 'notion_cancel_next_retry_at', 'TEXT');
  addCol(db, 'f_inbound_check_destinations', 'notion_cancel_attempt_count', 'INTEGER');

  // 作り方動画のリンク (中原さんFB⑥ 2026-09-02。いろは作業アプリ PR4 で登録・表示)。
  // 旧シートの「作業動画URL」列は全て空で持ち込まなかった — 今後はアプリから育てる
  addCol(db, 'f_iroha_work_master', 'video_url', 'TEXT');
  // 大きさ (嵩)。ふだんは商品の配送方法から見なすが、分からない商品は職員がここで登録する
  // (S=小 / M=中 / L=大)。明日どれをやるかの並びにだけ使う — 要件 §W-2
  addCol(db, 'f_iroha_work_master', 'size_class', "TEXT CHECK (size_class IS NULL OR size_class IN ('S','M','L'))");
  // 期限シールを貼る商品か (1=あり / 0=なし / NULL=未登録)。あるときだけ画面の上に赤で出す
  // (貼り忘れると出荷できない — 中原さん 2026-09-05)
  addCol(db, 'f_iroha_work_master', 'expiry_seal', 'INTEGER CHECK (expiry_seal IS NULL OR expiry_seal IN (0,1))');

  if (!added) return;   // ここから先は列を足した初回だけ

  db.transaction(() => {
    db.prepare("UPDATE f_inbound_check_batches SET work_date = date(imported_at, '+9 hours') WHERE work_date IS NULL").run();
    db.prepare("UPDATE f_inbound_check_destinations SET work_date = date(decided_at, '+9 hours') WHERE work_date IS NULL").run();
    db.prepare(`UPDATE f_inbound_check_destinations SET code_key = (
        SELECT l.code_key FROM f_inbound_check_lines l
        WHERE l.batch_id = f_inbound_check_destinations.batch_id AND l.line_key = f_inbound_check_destinations.line_key LIMIT 1)
      WHERE code_key IS NULL`).run();
    db.prepare('UPDATE f_inbound_check_destinations SET actual_qty = COALESCE(planned_qty, 0) WHERE actual_qty IS NULL').run();

    db.prepare(`UPDATE f_inbound_check_line_state SET quantity_work_date =
        (SELECT b.work_date FROM f_inbound_check_batches b WHERE b.id = f_inbound_check_line_state.batch_id)
      WHERE quantity_work_date IS NULL`).run();

    // 旧「確認済み」= 予定数が全部あった、とみなす
    db.prepare(`UPDATE f_inbound_check_line_state
      SET found_qty = COALESCE((SELECT MAX(l.planned_qty, 0) FROM f_inbound_check_lines l
            WHERE l.batch_id = f_inbound_check_line_state.batch_id AND l.line_key = f_inbound_check_line_state.line_key), 0),
          finalized_result = 'exact'
      WHERE status = 'checked'`).run();

    // active バッチだけ、同日引き継ぎに使える合成 add を作る (集計の正本はイベント表なので必須)
    db.prepare(`INSERT INTO f_inbound_check_quantity_events
      (client_event_id, work_date, batch_id, line_key, code_key, ar_no, product_id,
       action, quantity, input_kind, worker, device_label, client_occurred_at, received_at)
      SELECT 'backfill:' || s.batch_id || ':' || s.line_key, b.work_date, s.batch_id, s.line_key,
             l.code_key, l.ar_no, l.product_id, 'add', l.planned_qty, 'backfill',
             COALESCE(s.checked_by, 'migration'), s.checked_device,
             COALESCE(s.checked_at, b.imported_at), COALESCE(s.checked_at, b.imported_at)
      FROM f_inbound_check_line_state s
      JOIN f_inbound_check_batches b ON b.id = s.batch_id
      JOIN f_inbound_check_lines l ON l.batch_id = s.batch_id AND l.line_key = s.line_key
      WHERE b.status = 'active' AND s.status = 'checked' AND l.planned_qty > 0
        AND NOT EXISTS (SELECT 1 FROM f_inbound_check_quantity_events q
                        WHERE q.client_event_id = 'backfill:' || s.batch_id || ':' || s.line_key)`).run();

    // 確認済み行を、今ある行き先台帳の最新行につなぐ (やり直し時にその行だけ取り消せるように)
    db.prepare(`UPDATE f_inbound_check_line_state SET destination_id = (
        SELECT d.id FROM f_inbound_check_destinations d
        WHERE d.batch_id = f_inbound_check_line_state.batch_id AND d.line_key = f_inbound_check_line_state.line_key
          AND d.cancelled_at IS NULL ORDER BY d.id DESC LIMIT 1)
      WHERE status = 'checked'`).run();

    db.prepare(`UPDATE f_inbound_check_events SET result = 'exact',
        found_qty = (SELECT l.planned_qty FROM f_inbound_check_lines l
          WHERE l.batch_id = f_inbound_check_events.batch_id AND l.line_key = f_inbound_check_events.line_key),
        planned_qty_snapshot = (SELECT l.planned_qty FROM f_inbound_check_lines l
          WHERE l.batch_id = f_inbound_check_events.batch_id AND l.line_key = f_inbound_check_events.line_key)
      WHERE action = 'check' AND result IS NULL`).run();
  }).immediate();
}

// ───────────────────────── 端末 (iPad) ─────────────────────────

/**
 * 端末を登録し、平文トークンを返す (保存はハッシュのみ。トークンはこの1回しか得られない)。
 * kind='agent' は倉庫PCの値札印刷エージェント — 出力先プリンターを**サーバー側で**この端末に紐づける
 * (エージェント側の設定ミスで別のプリンターに値札を出さないため)。
 */
export function createDevice(label, actor, { kind = 'ipad', printerName = null } = {}) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  if (kind !== 'ipad' && kind !== 'agent') throw new Error('端末の種別が不正です');
  const db = getDB();
  const token = crypto.randomBytes(32).toString('base64url');
  return db.transaction(() => {
    let printer = null;
    if (kind === 'agent') {
      const chk = validatePrinterName(db, printerName, null);
      if (!chk.ok) throw new Error(chk.message);
      printer = chk.name;
    }
    const info = db.prepare(`INSERT INTO f_inbound_check_devices (token_hash, label, created_by, created_at, kind, printer_name) VALUES (?, ?, ?, ?, ?, ?)`)
      .run(hashToken(token), l, actor, utcNow(), kind, printer);
    return { token, id: Number(info.lastInsertRowid) };
  }).immediate();
}

/**
 * プリンター名の検査。🚨 有効なエージェント同士で同名は許さない — Windows のプリンター名は PC ごとの
 * ローカル名なので、倉庫PCと出荷PCの両方に「Brother QL-700」があると、どちらの実機から出るか決められない
 */
function validatePrinterName(db, printerName, selfId) {
  const name = String(printerName == null ? '' : printerName).trim();
  if (!name || name.length > 120) return { ok: false, error: 'bad_printer', message: 'プリンター名を1〜120文字で入力してください (「プリンターとスキャナー」の表記どおり)' };
  const other = db.prepare(`SELECT id, label FROM f_inbound_check_devices WHERE kind = 'agent' AND revoked_at IS NULL AND printer_name = ? AND id <> ?`)
    .get(name, selfId == null ? -1 : selfId);
  if (other) return { ok: false, error: 'duplicate_printer', message: `「${name}」は別の端末「${other.label}」に登録済みです。プリンター名は PC ごとのローカル名なので、同じ名前だとどちらの実機か決められません (どちらかの PC で名前を変えてください)` };
  return { ok: true, name };
}

/** 印刷エージェントの出力先プリンターを付け替える。印刷待ちのジョブは古い名前のまま残るので manual に倒す (lease 時にも同じ検査がある) */
export function setAgentPrinter(deviceId, printerName) {
  const db = getDB();
  return db.transaction(() => {
    const dev = db.prepare(`SELECT id, printer_name FROM f_inbound_check_devices WHERE id = ? AND kind = 'agent' AND revoked_at IS NULL`).get(deviceId);
    if (!dev) return { ok: false, error: 'not_agent', message: '有効な印刷エージェント端末ではありません' };
    const chk = validatePrinterName(db, printerName, deviceId);
    if (!chk.ok) return chk;
    const now = utcNow();
    db.prepare('UPDATE f_inbound_check_devices SET printer_name = ? WHERE id = ?').run(chk.name, deviceId);
    const dropped = dev.printer_name === chk.name ? 0 : db.prepare(`UPDATE f_inbound_check_print_jobs SET state = 'manual', error = ?, finished_at = ?, updated_at = ?
      WHERE target_device_id = ? AND state = 'queued' AND printer_name <> ?`)
      .run(`出力先プリンター名が変わったため自動印刷を取り消しました (${dev.printer_name} → ${chk.name})`, now, now, deviceId, chk.name).changes;
    return { ok: true, printer_name: chk.name, cancelled: dropped };
  }).immediate();
}

export function verifyDevice(token) {
  if (!token) return null;
  const db = getDB();
  const row = db.prepare('SELECT * FROM f_inbound_check_devices WHERE token_hash = ? AND revoked_at IS NULL').get(hashToken(token));
  if (!row) return null;
  const now = utcNow();
  if (Date.parse(now) - Date.parse(row.created_at) > DEVICE_TTL_MS) return null;
  if (!row.last_seen_at || Date.parse(now) - Date.parse(row.last_seen_at) > 3600_000) {
    db.prepare('UPDATE f_inbound_check_devices SET last_seen_at = ? WHERE id = ?').run(now, row.id);
  }
  return row;
}

// ── 登録コード (iPad をログインなしで登録する) ──
export const ENROLL_TTL_MS = 10 * 60 * 1000;   // 10分で失効
export const ENROLL_MAX_ATTEMPTS = 5;          // 打ち間違い5回で無効 (総当たり対策)

/** 6桁の登録コードを発行する。返すのはこの1回だけ (保存はハッシュのみ) */
export function createEnrollCode(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const db = getDB();
  // 紛らわしい 0/O・1/I を避けるため数字のみ 6 桁。crypto で偏りなく引く
  const now = new Date();
  const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
  db.transaction(() => {
    // ⭐有効なコードは常に1つだけにする (総当たりの当たり確率を 100万分の1 に抑える)。
    //   2台続けて登録するときは、1台目が終わってから次を発行する運用
    // 期限は「1秒前」にする。同じミリ秒だと expires_at < now が成り立たず期限切れと見なされない
    const revokedAt = new Date(now.getTime() - 1000).toISOString();
    db.prepare("UPDATE f_inbound_check_enroll_codes SET expires_at = ? WHERE used_at IS NULL AND expires_at > ?")
      .run(revokedAt, now.toISOString());
    db.prepare(`INSERT INTO f_inbound_check_enroll_codes (code_hash, label, created_by, created_at, expires_at)
      VALUES (?, ?, ?, ?, ?)`)
      .run(enrollHash(code), l, actor, now.toISOString(), new Date(now.getTime() + ENROLL_TTL_MS).toISOString());
  }).immediate();
  return { code, label: l, expiresAt: new Date(now.getTime() + ENROLL_TTL_MS).toISOString() };
}

/**
 * 登録コードを引き換えて端末を作る。成功したらコードは使用済みになる。
 * @returns {ok:true, token, label} | {ok:false, error:'bad_code'|'expired'|'used'|'too_many'}
 */
export function redeemEnrollCode(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return { ok: false, error: 'bad_code', message: '6桁の数字を入力してください' };
  const db = getDB();
  return db.transaction(() => {
    const row = db.prepare('SELECT * FROM f_inbound_check_enroll_codes WHERE code_hash = ?').get(enrollHash(c));
    if (!row) return { ok: false, error: 'bad_code', message: '登録コードが違います' };
    if (row.attempts >= ENROLL_MAX_ATTEMPTS) return { ok: false, error: 'too_many', message: 'このコードは無効です (発行し直してください)' };
    if (row.used_at) return { ok: false, error: 'used', message: 'この登録コードは使用済みです (発行し直してください)' };
    if (Date.parse(row.expires_at) < Date.now()) return { ok: false, error: 'expired', message: '登録コードの有効期限が切れています (発行し直してください)' };
    const { token, id } = createDevice(row.label, `enroll:${row.created_by}`);
    db.prepare('UPDATE f_inbound_check_enroll_codes SET used_at = ?, used_device_id = ? WHERE id = ?').run(utcNow(), id, row.id);
    return { ok: true, token, label: row.label };
  }).immediate();
}

// レート制限 (総当たり対策)。6桁 = 100万通りなので、**存在しないコードの試行も数える**必要がある
// (旧実装は実在する行しか数えず、総当たりに対して無力だった — セキュリティレビュー指摘)
export const ENROLL_RATE_WINDOW_MS = 10 * 60 * 1000;  // 直近10分を見る
export const ENROLL_RATE_PER_IP = 8;                  // 同じ相手からの失敗 8 回で打ち止め
export const ENROLL_RATE_GLOBAL = 40;                 // 分散して来られた場合の全体上限

/** 引き換えの試行を記録する (成功・失敗とも)。入力されたコード自体は保存しない */
export function recordEnrollAttempt({ ip = null, ok = false } = {}) {
  const db = getDB();
  db.prepare('INSERT INTO f_inbound_check_enroll_attempts (ip, at, ok) VALUES (?, ?, ?)').run(ip ? String(ip).slice(0, 64) : null, utcNow(), ok ? 1 : 0);
  // 古い記録は溜めない (レート制限に使うのは直近だけ)
  db.prepare('DELETE FROM f_inbound_check_enroll_attempts WHERE at < ?').run(new Date(Date.now() - 24 * 3600 * 1000).toISOString());
}

/**
 * 引き換えを受け付けてよいか。直近10分の**失敗**回数で判断する。
 * @returns {allowed:true} | {allowed:false, error:'rate_limited', message}
 */
export function checkEnrollRate({ ip = null } = {}) {
  const db = getDB();
  const since = new Date(Date.now() - ENROLL_RATE_WINDOW_MS).toISOString();
  const mine = ip ? db.prepare('SELECT COUNT(*) c FROM f_inbound_check_enroll_attempts WHERE ip = ? AND ok = 0 AND at > ?').get(String(ip).slice(0, 64), since).c : 0;
  if (mine >= ENROLL_RATE_PER_IP) {
    return { allowed: false, error: 'rate_limited', message: '試行が多すぎます。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  const all = db.prepare('SELECT COUNT(*) c FROM f_inbound_check_enroll_attempts WHERE ok = 0 AND at > ?').get(since).c;
  if (all >= ENROLL_RATE_GLOBAL) {
    return { allowed: false, error: 'rate_limited', message: '登録の受付を一時的に停止しています。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  return { allowed: true };
}

/** 打ち間違いを数える (そのコードが実在するときだけ。上限に達したらそのコードを無効化する) */
export function countEnrollAttempt(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return;
  getDB().prepare('UPDATE f_inbound_check_enroll_codes SET attempts = attempts + 1 WHERE code_hash = ?').run(enrollHash(c));
}

/** 発行済みで、まだ使えるコードの一覧 (画面表示用。コード自体は出さない) */
export function listActiveEnrollCodes() {
  return getDB().prepare(`SELECT id, label, created_by, created_at, expires_at, attempts
    FROM f_inbound_check_enroll_codes WHERE used_at IS NULL AND expires_at > ? AND attempts < ?
    ORDER BY id DESC`).all(utcNow(), ENROLL_MAX_ATTEMPTS);
}

export function revokeDevice(id) {
  return getDB().prepare('UPDATE f_inbound_check_devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL').run(utcNow(), id).changes > 0;
}

export function listDevices() {
  return getDB().prepare(`SELECT id, label, created_by, created_at, last_seen_at, revoked_at, kind, printer_name, heartbeat_at, heartbeat_note
    FROM f_inbound_check_devices ORDER BY id`).all();
}

// ───────────────────────── 作業者 (名前タップ) = スタッフマスタ (apps/staff) を参照 ─────────────────────────
// 自前の作業者表は持たない (2026-09-01 中原さん方針: 「人」の正本は staff.db に1つ)。
// 名前タップの候補 = 有効なスタッフ。code = staff_no (スタッフ管理番号)、name = 短い表記 (無ければ正式表記)

export function listWorkers() {
  return listTapCandidates().map(c => ({ code: c.staff_no, name: c.name, staff_id: c.staff_id, sort: c.sort }));
}

/** staff_no → {code, name, staff_id, active}。無ければ null */
export function getWorker(code) {
  const s = getStaffByNo(code);
  if (!s) return null;
  return { code: s.staff_no, name: tapName(s), staff_id: s.id, active: s.active };
}

// ───────────────────────── 取込 ─────────────────────────

export function getActiveBatch() {
  return getDB().prepare("SELECT * FROM f_inbound_check_batches WHERE status = 'active'").get() || null;
}

export function getBatch(id) {
  return getDB().prepare('SELECT * FROM f_inbound_check_batches WHERE id = ?').get(Number(id)) || null;
}

export function listBatches(limit = 30) {
  return getDB().prepare('SELECT * FROM f_inbound_check_batches ORDER BY id DESC LIMIT ?').all(Math.max(1, Math.min(200, Number(limit) || 30)));
}

export function listImportLog(limit = 20) {
  return getDB().prepare('SELECT * FROM f_inbound_check_import_log ORDER BY id DESC LIMIT ?').all(Math.max(1, Math.min(200, Number(limit) || 20)));
}

function logImport(db, { actor, source, fileName, ok, batchId, message }) {
  db.prepare('INSERT INTO f_inbound_check_import_log (at, actor, source, file_name, ok, batch_id, message) VALUES (?, ?, ?, ?, ?, ?, ?)')
    .run(utcNow(), actor || null, source, fileName || null, ok ? 1 : 0, batchId || null, message || null);
}

function isIsoDate(s) {
  return typeof s === 'string' && !Number.isNaN(Date.parse(s));
}

/**
 * CSV を取り込んで新しい active バッチにする。
 * @param {Buffer} buffer
 * @param {object} o  { fileName, source, actor, generatedAt (ISO。無ければ今) }
 * @returns {ok:true, batch, rowCount, slipCount} | {ok:false, error, message, batch?}
 *   error: bad_csv | duplicate_file | older_file
 */
export function importCsv(buffer, { fileName = null, source = 'manual_upload', actor = null, generatedAt = null } = {}) {
  if (!BATCH_SOURCES.includes(source)) throw new Error(`不正な source: ${source}`);
  const db = getDB();
  const fileHash = crypto.createHash('sha256').update(buffer).digest('hex');
  const genAt = isIsoDate(generatedAt) ? new Date(generatedAt).toISOString() : utcNow();

  let parsed;
  try {
    parsed = parseInboundCsv(buffer);
  } catch (e) {
    logImport(db, { actor, source, fileName, ok: false, message: e.message });
    return { ok: false, error: 'bad_csv', message: e.message };
  }
  // CSV 内の信頼できる時刻 = 明細の 更新日時/作成日時 の最大 (ブラウザの File.lastModified は改変可能なので
  // 新旧判定の主キーにしない — Codex R3 High)。0件 CSV は明細が無いので null
  let dataMaxAt = null;
  for (const r of parsed.rows) {
    for (const t of [r.updated_at, r.created_at]) {
      if (t && (!dataMaxAt || Date.parse(t) > Date.parse(dataMaxAt))) dataMaxAt = t;
    }
  }

  const dupResult = dup => {
    const message = `同じ内容のCSVは取込済みです (バッチ#${dup.id}、${dup.imported_at})`;
    logImport(db, { actor, source, fileName, ok: false, batchId: dup.id, message });
    return { ok: false, error: 'duplicate_file', message, batch: dup };
  };

  const tx = db.transaction(() => {
    // 重複判定はトランザクション内で (同時取込で両方が通過→UNIQUE 例外 500 を防ぐ — Codex R3 Medium)
    const dup = db.prepare('SELECT * FROM f_inbound_check_batches WHERE file_hash = ?').get(fileHash);
    if (dup) return dupResult(dup);
    const active = getActiveBatch();
    if (active) {
      // ①明細時刻で判定 (両方に明細がある時)。②明細時刻で判定できない時 (どちらかが0件) は生成時刻で判定
      if (dataMaxAt && active.data_max_at && Date.parse(dataMaxAt) < Date.parse(active.data_max_at)) {
        const message = `CSVの明細が現在の一覧より古い (明細の最終更新 ${dataMaxAt} < ${active.data_max_at}) ため取り込みません`;
        logImport(db, { actor, source, fileName, ok: false, batchId: active.id, message });
        return { ok: false, error: 'older_file', message, batch: active };
      }
      if (Date.parse(genAt) < Date.parse(active.csv_generated_at)) {
        const message = `CSVの生成時刻 (${genAt}) が現在の一覧 (${active.csv_generated_at}) より古いため取り込みません`;
        logImport(db, { actor, source, fileName, ok: false, batchId: active.id, message });
        return { ok: false, error: 'older_file', message, batch: active };
      }
    }
    // ⭐同日中の再取込は確認状態を引き継ぐ。旧 active を superseded にする前に掴んでおく
    const prevActive = active;
    // 旧 active を先に superseded にする (active の部分ユニーク索引があるため)
    db.prepare("UPDATE f_inbound_check_batches SET status = 'superseded' WHERE status = 'active'").run();
    const slipsMap = new Map();
    for (const r of parsed.rows) {
      if (!slipsMap.has(r.ar_no)) slipsMap.set(r.ar_no, { ar_no: r.ar_no, planned_date: r.planned_date, received_date: r.received_date, status: r.status, line_count: 0, seq: slipsMap.size + 1 });
      slipsMap.get(r.ar_no).line_count++;
    }
    const now = utcNow();
    const workDate = workDateJst(new Date(now));
    const info = db.prepare(`INSERT INTO f_inbound_check_batches
      (source, file_name, file_hash, csv_generated_at, data_max_at, row_count, slip_count, imported_at, imported_by, status, work_date)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)`)
      .run(source, fileName, fileHash, genAt, dataMaxAt, parsed.rows.length, slipsMap.size, now, actor, workDate);
    const batchId = Number(info.lastInsertRowid);
    const insSlip = db.prepare('INSERT INTO f_inbound_check_slips (batch_id, ar_no, planned_date, received_date, status, line_count, seq) VALUES (?, ?, ?, ?, ?, ?, ?)');
    for (const s of slipsMap.values()) insSlip.run(batchId, s.ar_no, s.planned_date, s.received_date, s.status, s.line_count, s.seq);
    const insLine = db.prepare(`INSERT INTO f_inbound_check_lines
      (batch_id, line_key, ar_no, line_no, detail_no, product_id, code_key, product_name, barcode, planned_qty, received_qty, seq)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insState = db.prepare(`INSERT INTO f_inbound_check_line_state
      (batch_id, line_key, status, version, checked_by, checked_device, checked_at,
       found_qty, quantity_version, quantity_work_date, finalized_result, destination_id, current_pack_qty)
      VALUES (?, ?, ?, 1, ?, ?, ?, ?, 1, ?, ?, ?, ?)`);
    // ⭐同日中の再取込だけ確認状態を引き継ぐ (要件定義 v1.3 §11.6)。
    //   引き継ぐ条件 = 明細キー (AR|行|詳細行) と商品 (code_key) と予定数 が全部同じ。
    //   予定数が変わった / 商品が差し替わった明細は、数えたものが違うので必ず未確認に戻す。
    //   翌日は従来どおり全部未確認から始める (§2 確定事項⑤「毎朝リセット」を守る)。
    //
    //   ⚠**見つけた数は引き継がない代わりに数え直しもしない**。数量イベントの論理キーが
    //     (work_date, line_key, code_key) なので、同日なら集計がそのまま残り、翌日は 0 になる。
    //     コピーしないぶん、同じ数が二重に入る事故が起きない。
    const carry = new Map();
    const sameDay = !!(prevActive && prevActive.work_date === workDate);
    if (sameDay) {
      for (const p of db.prepare(`SELECT s.line_key, s.status, s.checked_by, s.checked_device, s.checked_at,
            s.finalized_result, s.destination_id, s.current_pack_qty, l.code_key, l.planned_qty
          FROM f_inbound_check_line_state s
          JOIN f_inbound_check_lines l ON l.batch_id = s.batch_id AND l.line_key = s.line_key
          WHERE s.batch_id = ?`).all(prevActive.id)) {
        carry.set(p.line_key, p);
      }
    }
    const cancelDest = db.prepare(`UPDATE f_inbound_check_destinations
      SET cancelled_at = ?, cancelled_by = 'import', cancel_reason = ? WHERE id = ? AND cancelled_at IS NULL`);
    // 行き先の取消は「実際に取り消せた (1 行)」ときだけ在庫化アプリのタスクにも伝える
    const cancelDestAndTask = (destinationId, reason) => {
      if (cancelDest.run(now, reason, destinationId).changes !== 1) return false;   // 既に取消済み等 (数えない)
      requestCancellation({ destinationId, source: 'inbound_import', actor: actor || 'import' });
      return true;
    };
    // ⭐新しい CSV から行ごと消えた確認済み行 (伝票の明細が削除された) も、行き先を取り消す。
    //   ループは新 CSV の行しか見ないので、ここで先に拾う (Codex PR-B R1 #1: 消えた明細の作業指示が生き続けていた)
    const incomingKeys = new Set(parsed.rows.map((r) => r.line_key));
    let removed = 0;
    for (const p of carry.values()) {
      if (p.status === 'checked' && p.destination_id && !incomingKeys.has(p.line_key) && cancelDestAndTask(p.destination_id, 'line_removed')) removed++;
    }
    let carried = 0;
    for (const r of parsed.rows) {
      insLine.run(batchId, r.line_key, r.ar_no, r.line_no, r.detail_no, r.product_id, r.code_key, r.product_name, r.barcode, r.planned_qty, r.received_qty, r.seq);
      const p = carry.get(r.line_key);
      const sameProduct = !!(p && p.code_key === r.code_key);
      const same = sameProduct && p.planned_qty === r.planned_qty;
      // 見つけた数は同日の集計から復元する (同じ商品を数えていた場合だけ数が残る)
      const found = sameDay ? quantitySum(db, workDate, r.line_key, r.code_key) : 0;
      const keepChecked = same && p.status === 'checked';
      if (keepChecked) carried++;
      // 確認を引き継げない行の行き先実績は取り消す (いろはへ送る数が二重計上されないように)
      if (p && p.status === 'checked' && p.destination_id && !keepChecked) {
        cancelDestAndTask(p.destination_id, sameProduct ? 'planned_changed' : 'product_changed');
      }
      insState.run(batchId, r.line_key, keepChecked ? 'checked' : 'unchecked',
        keepChecked ? p.checked_by : null, keepChecked ? p.checked_device : null, keepChecked ? p.checked_at : null,
        found, workDate, keepChecked ? p.finalized_result : null,
        keepChecked ? p.destination_id : null, sameProduct ? p.current_pack_qty : null);
    }
    logImport(db, { actor, source, fileName, ok: true, batchId,
      message: `${parsed.rows.length}行 / ${slipsMap.size}伝票` + (carried ? ` (同日の確認 ${carried}行を引き継ぎ)` : '') + (removed ? ` (消えた明細の行き先 ${removed}件を取消)` : '') });
    cleanupOld(db);
    return { ok: true, batch: getBatch(batchId), rowCount: parsed.rows.length, slipCount: slipsMap.size, carriedOver: carried, imageSkus: parsed.rows.map(r => r.product_id) };
  });
  try {
    const r = tx.immediate();
    // 取込直後に、キャッシュに無い商品の画像だけ埋めにいく (ピッキングと同じキュー・fire-and-forget)。
    // 失敗しても一覧は出る。⚠ここに置くのは手動アップロードと Drive 自動取込の両方が通る唯一の場所だから
    if (r.ok && r.imageSkus && r.imageSkus.length) {
      try { queueEnsureImages(r.imageSkus, `入荷受付 ${fileName || source}`); } catch (e2) { console.warn('[inbound-check] 画像解決を飛ばしました:', e2.message); }
    }
    if (r.imageSkus) delete r.imageSkus;
    return r;
  } catch (e) {
    // immediate tx 同士は直列化されるので通常ここには来ないが、UNIQUE(file_hash) 違反は 409 相当に正規化する
    if (/UNIQUE constraint failed: f_inbound_check_batches\.file_hash/.test(e.message)) {
      const dup = db.prepare('SELECT * FROM f_inbound_check_batches WHERE file_hash = ?').get(fileHash);
      if (dup) return dupResult(dup);
    }
    throw e;
  }
}

/** 保持期間を過ぎたバッチ (子は CASCADE) と取込ログを削除。active は消さない */
export function cleanupOld(db = getDB(), now = new Date()) {
  const cut = new Date(now.getTime() - RETENTION_DAYS * 86400_000).toISOString();
  const logCut = new Date(now.getTime() - LOG_RETENTION_DAYS * 86400_000).toISOString();
  const a = db.prepare("DELETE FROM f_inbound_check_batches WHERE status = 'superseded' AND imported_at < ?").run(cut).changes;
  const b = db.prepare('DELETE FROM f_inbound_check_import_log WHERE at < ?').run(logCut).changes;
  // 🏷 値札の印刷ジョブは取込ログと同じ 90 日 (終わったものだけ。進行中は消さない)
  const c = db.prepare(`DELETE FROM f_inbound_check_print_jobs WHERE state NOT IN ('queued','leased','submitted') AND updated_at < ?`).run(logCut).changes;
  return { batches: a, logs: b, printJobs: c };
}

// ───────────────────────── 表示用の結合 (入数・行き先・ピックロケ) ─────────────────────────

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/**
 * 商品ごとの補助情報。
 *  info: f_inbound_info (入数 / BCシール / 直接ピックロケ保管 / BF保管荷姿 / いろは在庫化作業有無 / memo)
 *  locs: mirror_logizard_stock を商品×ロケで集約。P3F (ピックロケ) を在庫数の多い順に最大3件、
 *        引けない商品は loc_source='none' = 「ピックロケの登録なし」。フリーロケ運用なので
 *        空いている保管ロケに入れてよい状態であり、異常ではない (中原さん 2026-09-01)。
 *        P3F が無ければ他ブロックの先頭1件を「保管」として返す。JOIN で明細を増やさず商品単位で集約
 */
export function productInfoMap(codeKeys) {
  const db = getDB();
  const keys = [...new Set(codeKeys.map(k => String(k || '').trim().toLowerCase()).filter(Boolean))];
  const map = new Map();
  for (const k of keys) map.set(k, { info: null, pick_locs: [], other_locs: [], loc_source: 'none', expiry_from_stock: false, expiry_flag: null });
  if (keys.length === 0) return map;
  const chunk = 400;
  for (let i = 0; i < keys.length; i += chunk) {
    const part = keys.slice(i, i + chunk);
    const ph = part.map(() => '?').join(',');
    if (tableExists(db, 'f_inbound_info')) {
      const rows = db.prepare(`SELECT code_key, 商品コード AS code, 入数, 入庫時BCシール貼りフラグ AS bc_seal, 直接ピックロケ保管 AS direct_pick,
          BF保管荷姿 AS storage_form, いろは在庫化作業有無 AS iroha, memo, version
        FROM f_inbound_info WHERE code_key IN (${ph})`).all(...part);
      for (const r of rows) {
        const m = map.get(r.code_key);
        // version は iPad の編集で expect_version として送り返す (楽観ロック)。これが無いと編集できない
        if (m) m.info = { code: r.code, irisu: r.入数, bc_seal: r.bc_seal, direct_pick: r.direct_pick, storage_form: r.storage_form, iroha: r.iroha, memo: r.memo, version: r.version };
      }
    }
    if (tableExists(db, 'mirror_logizard_stock')) {
      const rows = db.prepare(`SELECT lower(trim(商品ID)) AS k, COALESCE(ブロック略称,'') AS block, COALESCE(ロケ,'') AS loc,
          SUM(在庫数) AS qty, MAX(CASE WHEN COALESCE(trim(有効期限),'') <> '' THEN 1 ELSE 0 END) AS has_exp
        FROM mirror_logizard_stock
        WHERE lower(trim(商品ID)) IN (${ph}) AND 在庫数 > 0
        GROUP BY k, block, loc`).all(...part);
      for (const r of rows) {
        const m = map.get(r.k);
        if (!m) continue;
        const label = r.block && r.loc ? `${r.block}-${r.loc}` : (r.block || r.loc || '');
        if (!label) continue;
        const entry = { loc: label, qty: r.qty };
        if (r.has_exp) m.expiry_from_stock = true;   // 在庫に有効期限が入っている = 期限管理商品とみなす
        if (/^P3F/i.test(r.block)) m.pick_locs.push(entry); else m.other_locs.push(entry);
      }
    }
    // 人が設定した「期限管理あり/なし」は在庫からの推定より優先する
    if (tableExists(db, 'f_inbound_check_product_flags')) {
      for (const r of db.prepare(`SELECT code_key, expiry_managed, source FROM f_inbound_check_product_flags WHERE code_key IN (${ph})`).all(...part)) {
        const m = map.get(r.code_key);
        if (m) m.expiry_flag = { managed: !!r.expiry_managed, source: r.source };
      }
    }
  }
  for (const m of map.values()) {
    m.expiry_managed = m.expiry_flag ? m.expiry_flag.managed : m.expiry_from_stock;
    m.expiry_source = m.expiry_flag ? m.expiry_flag.source : (m.expiry_from_stock ? 'stock' : 'none');
    m.pick_locs.sort((a, b) => b.qty - a.qty || a.loc.localeCompare(b.loc));
    m.other_locs.sort((a, b) => b.qty - a.qty || a.loc.localeCompare(b.loc));
    m.pick_locs = m.pick_locs.slice(0, 3);
    m.other_locs = m.other_locs.slice(0, 1);
    m.loc_source = m.pick_locs.length ? 'pick' : (m.other_locs.length ? 'storage' : 'none');
  }
  return map;
}

// ─────────────────── 🔍 商品を探す (入荷受付伝票に無い商品の入庫情報・値札) ───────────────────
// 中原さん 2026-09-06:「ロジザードの入荷リストになくても入荷受付伝票と同じフォーマットでシール印字できて、
// 入庫情報管理の情報を編集＆参照できる機能が欲しい。仕入れが多くないところは入荷受付伝票に登録していないから。
// 仕入先から商品を絞り込めるとありがたい」

/** 商品コードのキー (取込・入庫情報と同じ規則 = lower(trim)) */
const codeKeyOf = (s) => String(s == null ? '' : s).trim().toLowerCase();

/**
 * 商品のバーコード (JAN/FNSKU) を探す。Render に完全なマスタは無いので、控え → 見つかる場所を順に見る。
 *   ① f_inbound_check_barcodes (人が入れた / 前に見つけて控えた)
 *   ② f_inbound_check_lines (入荷受付伝票の明細。新しい取込ほど後)
 *   ③ mirror_logizard_stock (在庫ミラー。在庫がある間だけ行がある)
 *   ④ f_inbound_schedule (値札CSV = 入荷予定 7日分)
 * ②〜④ で見つけたら控えに写す (在庫が消えても次から引ける)。数字だけ = JAN / 英字を含む英数字 = FNSKU、
 * それ以外の値は使わない (値札に刷れない)
 * @returns {{barcode:string, barcode_type:'jan'|'fnsku', source:string}|null}
 */
export function resolveBarcode(codeKey, db = getDB()) {
  const k = codeKeyOf(codeKey);
  if (!k) return null;
  const typeOf = (bc) => {
    const s = String(bc == null ? '' : bc).trim();
    if (!s) return null;
    if (/^[0-9]+$/.test(s)) return 'jan';
    if (/^[A-Za-z0-9]+$/.test(s) && /[A-Za-z]/.test(s)) return 'fnsku';
    return null;
  };
  const cached = db.prepare('SELECT barcode, barcode_type, source FROM f_inbound_check_barcodes WHERE code_key = ?').get(k);
  if (cached) return cached;
  const candidates = [];
  if (tableExists(db, 'f_inbound_check_lines')) {
    candidates.push(['line', db.prepare(`SELECT barcode FROM f_inbound_check_lines WHERE code_key = ? AND barcode IS NOT NULL AND trim(barcode) <> ''
      ORDER BY batch_id DESC, seq DESC LIMIT 1`).get(k)?.barcode]);
  }
  if (tableExists(db, 'mirror_logizard_stock')) {
    candidates.push(['stock', db.prepare(`SELECT バーコード AS bc FROM mirror_logizard_stock WHERE LOWER(TRIM(商品ID)) = ? AND バーコード IS NOT NULL AND trim(バーコード) <> '' LIMIT 1`).get(k)?.bc]);
  }
  if (tableExists(db, 'f_inbound_schedule')) {
    candidates.push(['schedule', db.prepare(`SELECT バーコード AS bc FROM f_inbound_schedule WHERE code_key = ? AND バーコード IS NOT NULL AND trim(バーコード) <> '' LIMIT 1`).get(k)?.bc]);
  }
  for (const [source, bc] of candidates) {
    const t = typeOf(bc);
    if (!t) continue;
    const barcode = String(bc).trim();
    db.prepare(`INSERT INTO f_inbound_check_barcodes (code_key, barcode, barcode_type, source, updated_at) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(code_key) DO NOTHING`).run(k, barcode, t, source, utcNow());
    return { barcode, barcode_type: t, source };
  }
  return null;
}

/**
 * 人がバーコードを入れる / 直す (控えを上書き。source='manual')。
 * 数字だけ = JAN、英字を含む英数字 = FNSKU。それ以外は拒否 (値札に刷れない形を残さない)
 */
export function setProductBarcode(codeKey, barcode, actor = null) {
  const k = codeKeyOf(codeKey);
  const s = String(barcode == null ? '' : barcode).trim();
  if (!k) return { ok: false, error: 'bad_request', message: '商品が指定されていません' };
  if (!s) {
    getDB().prepare("DELETE FROM f_inbound_check_barcodes WHERE code_key = ? AND source = 'manual'").run(k);
    return { ok: true, cleared: true };
  }
  const type = /^[0-9]+$/.test(s) ? 'jan' : (/^[A-Za-z0-9]+$/.test(s) && /[A-Za-z]/.test(s) ? 'fnsku' : null);
  if (!type) return { ok: false, error: 'bad_barcode', message: 'バーコードは数字だけ (JAN) か 英数字 (FNSKU) で入れてください' };
  if (s.length > 40) return { ok: false, error: 'bad_barcode', message: 'バーコードが長すぎます' };
  getDB().prepare(`INSERT INTO f_inbound_check_barcodes (code_key, barcode, barcode_type, source, updated_at, updated_by)
    VALUES (?, ?, ?, 'manual', ?, ?)
    ON CONFLICT(code_key) DO UPDATE SET barcode = excluded.barcode, barcode_type = excluded.barcode_type,
      source = 'manual', updated_at = excluded.updated_at, updated_by = excluded.updated_by`)
    .run(k, s, type, utcNow(), actor);
  return { ok: true, barcode: s, barcode_type: type };
}

/** 仕入先の一覧 (商品マスタに紐づくものだけ・商品数つき)。絞り込みのプルダウン用 */
export function listProductSuppliers({ includeInactive = false } = {}) {
  const db = getDB();
  if (!tableExists(db, 'mirror_products')) return [];
  const hasSup = tableExists(db, 'po_suppliers');
  const where = includeInactive ? '' : "AND p.取扱区分 = '取扱中'";
  return db.prepare(`SELECT p.仕入先コード AS code, ${hasSup ? 's.name' : 'NULL'} AS name, COUNT(*) AS products
    FROM mirror_products p ${hasSup ? 'LEFT JOIN po_suppliers s ON s.supplier_code = p.仕入先コード' : ''}
    WHERE p.仕入先コード IS NOT NULL AND trim(p.仕入先コード) <> '' AND p.商品区分 = '単品' ${where}
    GROUP BY p.仕入先コード ORDER BY products DESC, code`).all();
}

/**
 * 商品を探す (商品マスタ mirror_products が元)。単品だけ (セットは値札を貼らない)。既定は取扱中のみ。
 * @param {object} o { supplier, q, includeInactive, limit, offset }
 *   q = 商品名 / 商品コード / バーコード (控えにあるもの) の部分一致
 * @returns {{rows: Array, total: number}}  rows には入庫情報 (info)・ピックロケ・期限管理・バーコードを付ける
 */
export function searchProducts({ supplier = null, q = '', includeInactive = false, limit = 50, offset = 0 } = {}) {
  const db = getDB();
  if (!tableExists(db, 'mirror_products')) return { rows: [], total: 0, missing_master: true };
  const hasSup = tableExists(db, 'po_suppliers');
  const conds = ["p.商品区分 = '単品'"];
  const args = [];
  if (!includeInactive) conds.push("p.取扱区分 = '取扱中'");
  if (supplier) { conds.push('p.仕入先コード = ?'); args.push(String(supplier)); }
  const term = String(q || '').trim();
  if (term) {
    const like = `%${term.replace(/[%_]/g, (c) => '\\' + c)}%`;
    conds.push(`(p.商品名 LIKE ? ESCAPE '\\' OR p.商品コード LIKE ? ESCAPE '\\' OR LOWER(TRIM(p.商品コード)) IN (SELECT code_key FROM f_inbound_check_barcodes WHERE barcode LIKE ? ESCAPE '\\'))`);
    args.push(like, like, like);
  }
  const lim = Math.min(200, Math.max(1, Number(limit) || 50));
  const off = Math.max(0, Number(offset) || 0);
  const whereSql = conds.join(' AND ');
  const total = db.prepare(`SELECT COUNT(*) AS n FROM mirror_products p WHERE ${whereSql}`).get(...args).n;
  const rows = db.prepare(`SELECT p.商品コード AS product_id, p.商品名 AS product_name, p.取扱区分 AS handling,
      p.仕入先コード AS supplier_code, ${hasSup ? 's.name' : 'NULL'} AS supplier_name
    FROM mirror_products p ${hasSup ? 'LEFT JOIN po_suppliers s ON s.supplier_code = p.仕入先コード' : ''}
    WHERE ${whereSql} ORDER BY p.商品名 LIMIT ? OFFSET ?`).all(...args, lim, off);
  const keys = rows.map(r => codeKeyOf(r.product_id));
  const info = productInfoMap(keys);
  for (const r of rows) {
    r.code_key = codeKeyOf(r.product_id);
    const x = info.get(r.code_key) || { info: null, pick_locs: [], other_locs: [], loc_source: 'none', expiry_managed: false, expiry_source: 'none' };
    r.info = x.info;
    r.pick_locs = x.pick_locs;
    r.other_locs = x.other_locs;
    r.loc_source = x.loc_source;
    r.expiry_managed = !!x.expiry_managed;
    r.expiry_source = x.expiry_source || 'none';
    const bc = resolveBarcode(r.code_key, db);
    r.barcode = bc ? bc.barcode : null;
    r.barcode_type = bc ? bc.barcode_type : null;
    r.barcode_source = bc ? bc.source : null;
    r.pack_qty = r.info && Number.isInteger(r.info.irisu) && r.info.irisu > 0 ? r.info.irisu : null;
  }
  return { rows, total };
}

/** 商品1件 (印刷キューが商品モードで積むときの元。画面の値を信じない) */
export function getProductForPrint(productCode) {
  const db = getDB();
  const k = codeKeyOf(productCode);
  if (!k || !tableExists(db, 'mirror_products')) return null;
  const p = db.prepare('SELECT 商品コード AS product_id, 商品名 AS product_name FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ? LIMIT 1').get(k);
  if (!p) return null;
  const bc = resolveBarcode(k, db);
  return { product_id: p.product_id, code_key: k, product_name: p.product_name, barcode: bc ? bc.barcode : null, barcode_type: bc ? bc.barcode_type : null };
}

/**
 * このバッチで確認済みの行に紐づく「決まったこと」= 行き先 / 有効期限 / 実数。
 * 取り消していない最新の1件を採る (やり直すと新しい行が積まれるため)
 */
function decidedMap(db, batchId) {
  const m = new Map();
  for (const r of db.prepare(`SELECT line_key, destination, expiry_date, actual_qty
    FROM f_inbound_check_destinations
    WHERE batch_id = ? AND cancelled_at IS NULL ORDER BY id`).all(batchId)) {
    m.set(r.line_key, r);   // 後勝ち = 最新
  }
  return m;
}

/** 前回 (直前の superseded バッチ) で確認済みだった行 → 参考表示用 {line_key → {by, at}} */
function previousCheckedMap(db, activeId) {
  const prev = db.prepare("SELECT id FROM f_inbound_check_batches WHERE status = 'superseded' AND id < ? ORDER BY id DESC LIMIT 1").get(activeId);
  const m = new Map();
  if (!prev) return m;
  for (const r of db.prepare("SELECT line_key, checked_by, checked_at FROM f_inbound_check_line_state WHERE batch_id = ? AND status = 'checked'").all(prev.id)) {
    m.set(r.line_key, { by: r.checked_by, at: r.checked_at });
  }
  return m;
}

/**
 * 商品画像。ピッキング画面が使っている pk_product_images (楽天の白抜き/バリエーション画像の
 * キャッシュ) をそのまま流用する。**同じ DATA_DIR の picking.db なので Render 内で完結する**。
 *   - 解決済みでなければ null → 画面はプレースホルダ。取込時に queueEnsureImages で埋めにいく
 *   - picking 側が未初期化・テーブル無しでも一覧は出す (画像は「あれば嬉しい」もの)
 */
export function productImageMap(productIds) {
  const keys = [...new Set(productIds.map(k => String(k || '').trim()).filter(Boolean))];
  const map = new Map();
  if (keys.length === 0) return map;
  try {
    for (const [sku, img] of getImageMap(keys)) {
      const url = img && img.url ? img.url : null;
      if (url) map.set(String(sku).trim().toLowerCase(), url);
    }
  } catch (e) {
    console.warn('[inbound-check] 画像の取得を飛ばしました:', e.message);
  }
  return map;
}

/**
 * iPad 一覧の状態。active バッチが無ければ { batch:null, slips:[], lines:[] }
 * 各行 = 明細 + 状態 + 補助情報 + 前回確認 (参考)
 */
export function getState() {
  const db = getDB();
  const batch = getActiveBatch();
  if (!batch) return { batch: null, slips: [], lines: [], totals: { lines: 0, checked: 0, partial: 0, undecided: 0, toIroha: 0, toIrohaQty: 0 } };
  const slips = db.prepare('SELECT * FROM f_inbound_check_slips WHERE batch_id = ? ORDER BY seq').all(batch.id);
  const lines = db.prepare(`SELECT l.*, s.status AS check_status, s.version, s.checked_by, s.checked_device, s.checked_at,
      s.found_qty, s.quantity_version, s.finalized_result, s.current_pack_qty, s.destination_id, s.pending_expiry
    FROM f_inbound_check_lines l JOIN f_inbound_check_line_state s ON s.batch_id = l.batch_id AND s.line_key = l.line_key
    WHERE l.batch_id = ? ORDER BY l.seq`).all(batch.id);
  const info = productInfoMap(lines.map(l => l.code_key));
  const decided = decidedMap(db, batch.id);   // 確認時に決まった 行き先 / 有効期限 / 実数
  const images = productImageMap(lines.map(l => l.product_id));
  const prev = previousCheckedMap(db, batch.id);
  const checkedBySlip = new Map();
  const partialBySlip = new Map();
  let partial = 0;
  for (const l of lines) {
    const x = info.get(l.code_key) || { info: null, pick_locs: [], other_locs: [], loc_source: 'none', expiry_managed: false, expiry_source: 'none' };
    l.info = x.info;
    l.pick_locs = x.pick_locs;
    l.other_locs = x.other_locs;
    l.loc_source = x.loc_source;
    l.image_url = images.get(String(l.product_id || '').trim().toLowerCase()) || null;
    const d = decided.get(l.line_key) || null;
    l.expiry_date = d ? d.expiry_date : null;   // 確認したときに入力した有効期限 (未確認なら null)
    l.pending_expiry = l.pending_expiry || null; // 確認前に詳細パネルで先入力した有効期限
    l.destination = d ? d.destination : null;
    l.expiry_managed = !!x.expiry_managed;      // 期限管理商品か (在庫の有効期限から推定 or 手動設定)
    l.expiry_source = x.expiry_source || 'none';
    l.dest = resolveDestination(l.info, { expiryManaged: l.expiry_managed });   // 行き先と、確認の前に決める項目
    l.prev_checked = prev.get(l.line_key) || null;
    // 数量 (部分確認)。「一部」は status ではなく found_qty から導出する (要件定義 v1.3 §11.3)
    l.found_qty = Number(l.found_qty) || 0;
    l.remaining_qty = l.planned_qty - l.found_qty;
    l.quantity_relation = quantityRelation(l.found_qty, l.planned_qty);
    // 1箱の入数: この検品で使っている値 → 無ければ入庫情報のマスタ値
    l.pack_qty = Number.isInteger(l.current_pack_qty) && l.current_pack_qty > 0 ? l.current_pack_qty
      : (l.info && Number.isInteger(l.info.irisu) && l.info.irisu > 0 ? l.info.irisu : null);
    if (l.check_status === 'checked') checkedBySlip.set(l.ar_no, (checkedBySlip.get(l.ar_no) || 0) + 1);
    else if (l.found_qty > 0) { partial++; partialBySlip.set(l.ar_no, (partialBySlip.get(l.ar_no) || 0) + 1); }
  }
  for (const s of slips) {
    s.checked_count = checkedBySlip.get(s.ar_no) || 0;
    s.partial_count = partialBySlip.get(s.ar_no) || 0;
  }
  const checked = lines.filter(l => l.check_status === 'checked').length;
  // 未確認のうち「行き先が決まっていない」件数 = 画面上部のアラート
  // アラート = 行き先 (いろは在庫化) が未設定のもの。期限入力は毎回聞くので件数には入れない
  const undecided = lines.filter(l => l.check_status !== 'checked' && l.dest.missing.includes('iroha')).length;
  // ⭐いろは行きは**商品マスタの判定ではなく行き先台帳から**数える。
  //   「状況による」でその場選択した行はマスタが書き変わらないので、マスタから数えると 0 件になる
  const ir = db.prepare(`SELECT COUNT(*) AS c, COALESCE(SUM(d.actual_qty), 0) AS q
    FROM f_inbound_check_destinations d
    JOIN f_inbound_check_line_state s ON s.destination_id = d.id AND s.batch_id = ?
    WHERE d.destination = 'iroha' AND d.cancelled_at IS NULL`).get(batch.id);
  // 業務日が変わったのに当日の取込がまだ来ていない = 前日の一覧。数量操作は受け付けない
  const dayStale = !!(batch.work_date && batch.work_date !== workDateJst());
  return {
    batch, slips, lines, day_stale: dayStale, field_options: fieldOptions(),
    totals: { lines: lines.length, checked, partial, undecided, toIroha: ir ? ir.c : 0, toIrohaQty: ir ? Number(ir.q) : 0 },
  };
}

// ─────────────────── 行き先 (いろは / B-Faith) の判定 ───────────────────

export const IROHA_YES = '有り';
export const IROHA_NO = '無し';
const NA_VALUES = new Set(['', '－', '-', 'ー', '―']);   // 「未記入」と同じ扱いにする表記
const blank = v => NA_VALUES.has(String(v == null ? "" : v).trim());

/**
 * この明細を「いろはで在庫化」に回すのか「B-Faith で入庫」するのかを、入庫情報から決める。
 *
 * 中原さん 2026-09-01: 「すでにデータがあるものはそのデータに沿ってやる。もしデータが
 * なければその時に選択して、その選択したデータがデータベースに登録される」
 *
 *   いろは在庫化作業有無 = 有り → いろは行き。B-Faith での入庫作業が無いので他は要らない
 *   いろは在庫化作業有無 = 無し → B-Faith 入庫。**ラベル (BCシール) と入数が要る**
 *                                  (中原さん: 「選択しないと進めない形にしてほしい」)
 *   それ以外 (未記入・「状況による」等) → その場で選んでもらう
 *
 * ⭐未記入のときだけ、選んだ値を入庫情報に書き戻す。「状況による」のように**人が意図して
 *   入れた値は上書きしない** (今回の判断は台帳にだけ残す)
 *
 * 期限管理商品は、**入荷のたびに有効期限が変わる**ので毎回 missing に 'expiry' が入る
 * (商品マスタに持てる値ではない)。
 *
 * @returns {{destination: string|null, missing: string[], writeBack: boolean}}
 *   missing = 確認の前に決めないといけない項目 (iroha / bc_seal / irisu / expiry)
 */
export function resolveDestination(info, { expiryManaged = false } = {}) {
  const withExpiry = r => (expiryManaged ? { ...r, missing: [...r.missing, 'expiry'] } : r);
  if (!info) return withExpiry({ destination: null, missing: ['iroha'], writeBack: true });
  const iroha = String(info.iroha == null ? "" : info.iroha).trim();
  if (iroha === IROHA_YES) return withExpiry({ destination: 'iroha', missing: [], writeBack: false });
  if (iroha !== IROHA_NO) {
    // 未記入 = 書き戻す / 「状況による」等 = 今回だけの判断 (master は触らない)
    return withExpiry({ destination: null, missing: ['iroha'], writeBack: blank(iroha) });
  }
  const missing = [];
  if (blank(info.bc_seal)) missing.push('bc_seal');
  if (!Number.isInteger(info.irisu) || info.irisu <= 0) missing.push('irisu');
  return withExpiry({ destination: 'bfaith', missing, writeBack: true });
}

/** 明細1件分の補助情報 (入庫情報 + 期限管理)。行き先の判定に使う */
export function infoForLine(codeKey) {
  const m = productInfoMap([codeKey]).get(String(codeKey || "").trim().toLowerCase());
  return { info: m?.info || null, expiryManaged: !!m?.expiry_managed };
}

/**
 * 「期限管理あり/なし」を人が設定する。在庫からの推定を上書きする。
 * ロジザード商品マスタを取り込めるようになったら source='logizard' で同じ表を埋める
 */
/**
 * 確認前の有効期限の先入力 (詳細パネルから)。expiryDate = 'YYYY-MM-DD' / 'YYYY-MM' / null(消す)。
 * 形式検証は router (parseExpiry) が済ませてから渡す。確認済みの行は変更不可 (やり直してから)。
 * 楽観ロックは持たない — 同じ行の期限を2台で同時に打つ状況は実務上なく、後勝ちで困らない
 */
export function setPendingExpiry({ batchId, lineKey, expiryDate }) {
  const db = getDB();
  const active = getActiveBatch();
  if (!active || active.id !== Number(batchId)) return { ok: false, error: 'stale_batch', message: '一覧が更新されました' };
  // 前日の一覧に今日の期限を入れさせない (数量APIと同じ day_stale ガード — Codex #1116 R1 Med-4)
  if (active.work_date && active.work_date !== workDateJst()) {
    return { ok: false, error: 'stale_work_date', message: '本日の入荷一覧を待っています (前日の一覧には記録できません)' };
  }
  const r = db.prepare(`UPDATE f_inbound_check_line_state SET pending_expiry = ?
    WHERE batch_id = ? AND line_key = ? AND status = 'unchecked'`).run(expiryDate || null, active.id, lineKey);
  if (r.changes === 0) {
    const cur = db.prepare('SELECT status FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = ?').get(active.id, lineKey);
    if (!cur) return { ok: false, error: 'not_found', message: 'その明細が見つかりません' };
    return { ok: false, error: 'finalized', message: '確認済みの行です。期限を直すには先に「やり直す」を押してください' };
  }
  return { ok: true, pending_expiry: expiryDate || null };
}

/** 確認時に使う: 先入力された有効期限 (無ければ null) */
export function pendingExpiryFor(batchId, lineKey) {
  const r = getDB().prepare('SELECT pending_expiry FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = ?')
    .get(batchId, lineKey);
  return r ? r.pending_expiry || null : null;
}

export function setExpiryManaged(codeKey, managed, actor) {
  const k = String(codeKey || '').trim().toLowerCase();
  if (!k) throw new Error('商品が指定されていません');
  getDB().prepare(`INSERT INTO f_inbound_check_product_flags (code_key, expiry_managed, source, updated_at, updated_by)
    VALUES (?, ?, 'manual', ?, ?)
    ON CONFLICT(code_key) DO UPDATE SET expiry_managed = excluded.expiry_managed,
      source = 'manual', updated_at = excluded.updated_at, updated_by = excluded.updated_by`)
    .run(k, managed ? 1 : 0, utcNow(), String(actor || '').trim() || null);
  return { ok: true, code_key: k, expiry_managed: !!managed };
}

// ───────────────────────── 消し込み ─────────────────────────

// ─────────────────── 行き先の台帳 (いろはへ送った実績) ───────────────────

/**
 * 行き先の実績。既定は「いろは行き・取り消されていないもの」を新しい順。
 * バッチが保持期間で消えたあとも残る (テーブルを CASCADE にしていないため)
 */
export function listDestinations({ destination = 'iroha', from = null, to = null, includeCancelled = false, limit = 2000 } = {}) {
  const where = [];
  const params = [];
  if (destination && destination !== 'all') { where.push('destination = ?'); params.push(destination); }
  if (!includeCancelled) where.push('cancelled_at IS NULL');
  if (from) { where.push('decided_at >= ?'); params.push(String(from)); }
  if (to) { where.push('decided_at <= ?'); params.push(String(to)); }
  return getDB().prepare(`SELECT * FROM f_inbound_check_destinations
    ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
    ORDER BY id DESC LIMIT ?`).all(...params, Math.max(1, Math.min(20000, Number(limit) || 2000)));
}

/** 行き先の CSV (UTF-8 BOM)。いろはへの持ち出しリストとして使う */
export function destinationsCsv(opts = {}) {
  const rows = listDestinations({ ...opts, limit: 20000 });
  const esc = v => {
    let s = v == null ? '' : String(v);
    if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;   // 表計算ソフトが数式として解釈しないように
    return `"${s.replace(/"/g, '""')}"`;
  };
  const head = ['決定日時', '行き先', '入荷管理番号', '商品ID', '商品名', '予定数', '有効期限', '判断', '作業者', '端末', '取消日時'];
  const body = rows.map(r => [
    r.decided_at, r.destination === 'iroha' ? 'いろは在庫化' : 'B-Faith入庫', r.ar_no, r.product_id, r.product_name,
    r.planned_qty, r.expiry_date, r.decided_from === 'chosen' ? 'その場で選択' : '入庫情報どおり', r.worker, r.device_label, r.cancelled_at,
  ].map(esc).join(','));
  return '\ufeff' + [head.map(esc).join(','), ...body].join('\r\n') + '\r\n';
}

// ───────────────────────── 数量 (部分確認) ─────────────────────────
//
// 要件定義 v1.3 §11。設計の要点:
//  - **partial は DB の状態にしない**。status は unchecked / checked の2値のまま持ち、
//    「一部」は status='unchecked' かつ found_qty>0 から導出する (CHECK 制約の作り替えを避ける)
//  - **数量は加算イベントの合計**。絶対値の上書きは禁止 (A の 11箱と B の 3箱 が競合して片方消える)
//  - **論理キーは (work_date, line_key, code_key)**。同日中は何度取り込んでも数が残り、翌日は 0 から
//  - 打ち消し・訂正は行を消さず reversal を積む。訂正 = reversal + 新 add (replaces_event_seq で元を指す)

/** 見つけた数 = 加算 − 打ち消し。行が1件も無ければ 0 */
export function quantitySum(db, workDate, lineKey, codeKey) {
  const r = db.prepare(`SELECT COALESCE(SUM(CASE action WHEN 'add' THEN quantity ELSE -quantity END), 0) AS q
    FROM f_inbound_check_quantity_events WHERE work_date = ? AND line_key = ? AND code_key = ?`)
    .get(workDate, lineKey, codeKey);
  return r ? Number(r.q) : 0;
}

/** 予定数との関係。画面のボタンと文言はこれで決まる */
export function quantityRelation(found, planned) {
  if (found < planned) return 'shortage';
  if (found > planned) return 'excess';
  return 'exact';
}

export const QTY_INPUT_KINDS = ['box', 'loose', 'fill_remaining', 'correction'];
const MAX_EVENTS_PER_REQUEST = 10;   // 通常1件。訂正 (reversal + add) で2件。それ以上は誤用

/** 呼び出し側に 4xx/409 を返させるための中断 (トランザクションを確実に巻き戻す) */
class Abort extends Error {
  constructor(result) { super(result.error || 'abort'); this.result = result; }
}

const isId = v => typeof v === 'string' && v.length >= 8 && v.length <= 64 && /^[A-Za-z0-9:_.-]+$/.test(v);
const posInt = v => Number.isSafeInteger(v) && v > 0;

/** 数量操作の共通ガード。active バッチ・業務日・行の存在・確定済みかどうかを見る */
function loadForQuantity(db, batchId, lineKey) {
  const active = getActiveBatch();
  if (!active || active.id !== Number(batchId)) {
    return { error: { ok: false, error: 'stale_batch', message: '一覧が更新されました。最新の一覧を読み込み直してください', activeBatchId: active ? active.id : null } };
  }
  // ⚠日付が変わったら昨日の一覧には数を足させない (今日の荷物が昨日の行に混ざる)
  const today = workDateJst();
  if (active.work_date && active.work_date !== today) {
    return { error: { ok: false, error: 'stale_work_date', message: '本日の入荷一覧を待っています (前日の一覧には記録できません)', workDate: active.work_date } };
  }
  const line = db.prepare('SELECT * FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?').get(active.id, lineKey);
  if (!line) return { error: { ok: false, error: 'not_found', message: '明細が見つかりません' } };
  const state = db.prepare('SELECT * FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = ?').get(active.id, lineKey);
  if (!state) return { error: { ok: false, error: 'not_found', message: '明細の状態が見つかりません' } };
  return { active, line, state, workDate: active.work_date || today };
}

function quantityState(db, batchId, lineKey) {
  const s = db.prepare(`SELECT status, version, found_qty, quantity_version, finalized_result, current_pack_qty, checked_by, checked_at
    FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = ?`).get(batchId, lineKey);
  return s || null;
}

/**
 * 数を足す / 打ち消す / 訂正する。**1タップ1イベント**で記録する。
 *
 * @param {object} o
 *   events[] = { client_event_id, action:'add'|'reversal', quantity, input_kind, unit_size?,
 *                reverses_event_seq?, replaces_event_seq?, client_occurred_at? }
 * @returns {ok:true, state, accepted, replayed} | {ok:false, error, ...}
 *   error: stale_batch | stale_work_date | not_found | finalized | conflict |
 *          bad_request | already_reversed | idempotency_conflict | negative_total
 */
export function applyQuantityEvents({ batchId, lineKey, expectQuantityVersion, events, packQty = null,
  worker, staffId = null, deviceId = null, deviceLabel = null }) {
  const w = String(worker || '').trim();
  if (!w) return { ok: false, error: 'bad_request', message: '作業者を選んでください' };
  if (!Array.isArray(events) || events.length === 0 || events.length > MAX_EVENTS_PER_REQUEST) {
    return { ok: false, error: 'bad_request', message: '数量の指定がありません' };
  }
  if (!Number.isSafeInteger(expectQuantityVersion) || expectQuantityVersion < 1) {
    return { ok: false, error: 'bad_request', message: 'expect_quantity_version (正の整数) が必要です' };
  }
  for (const e of events) {
    if (!e || typeof e !== 'object') return { ok: false, error: 'bad_request', message: 'イベントの形式が不正です' };
    if (!isId(e.client_event_id)) return { ok: false, error: 'bad_request', message: 'client_event_id が不正です' };
    if (e.action !== 'add' && e.action !== 'reversal') return { ok: false, error: 'bad_request', message: 'action が不正です' };
    if (!posInt(e.quantity) || e.quantity > 1_000_000) return { ok: false, error: 'bad_request', message: '数量は1以上の整数で指定してください' };
    if (!QTY_INPUT_KINDS.includes(e.input_kind)) return { ok: false, error: 'bad_request', message: 'input_kind が不正です' };
    if (e.unit_size != null && !posInt(e.unit_size)) return { ok: false, error: 'bad_request', message: '入数は1以上の整数で指定してください' };
    if (e.action === 'reversal' && !posInt(e.reverses_event_seq)) return { ok: false, error: 'bad_request', message: '打ち消す対象がありません' };
  }
  const db = getDB();
  const tx = db.transaction(() => {
    const g = loadForQuantity(db, batchId, lineKey);
    if (g.error) throw new Abort(g.error);
    const { active, line, state, workDate } = g;
    // 確定済みの行は数量を動かせない。直すなら先に「やり直す」
    if (state.status === 'checked') {
      throw new Abort({ ok: false, error: 'finalized', message: '確認済みです。数を直すには先に「やり直す」を押してください', current: quantityState(db, active.id, lineKey) });
    }

    // ── 冪等: 同じ client_event_id は1回だけ計上する ──
    //    (コミット直後に応答だけ失われたときの押し直しで二重加算しないため)
    const seen = db.prepare('SELECT * FROM f_inbound_check_quantity_events WHERE client_event_id = ?');
    const fresh = [];
    for (const e of events) {
      const prev = seen.get(e.client_event_id);
      if (!prev) { fresh.push(e); continue; }
      const same = prev.line_key === line.line_key && prev.code_key === line.code_key
        && prev.action === e.action && prev.quantity === e.quantity && prev.input_kind === e.input_kind;
      if (!same) {
        throw new Abort({ ok: false, error: 'idempotency_conflict', message: '同じ操作IDで違う内容が送られました (画面を読み込み直してください)' });
      }
    }
    if (fresh.length === 0) {
      // 全部再生だった = 前回の送信は成功していた。version は上げない
      return { ok: true, replayed: true, accepted: [], state: quantityState(db, active.id, lineKey) };
    }
    // 再生が混ざったまま version 照合すると、成功済みの分だけ version が進んでいて必ず conflict になる。
    // 「新しいイベントがある」ときだけ version を見る
    if (state.quantity_version !== expectQuantityVersion) {
      throw new Abort({ ok: false, error: 'conflict', message: '他の端末で先に数が入りました。最新の数を表示します', current: quantityState(db, active.id, lineKey) });
    }

    const ins = db.prepare(`INSERT INTO f_inbound_check_quantity_events
      (client_event_id, work_date, batch_id, line_key, code_key, ar_no, product_id, action, quantity, input_kind,
       unit_size, reverses_event_seq, replaces_event_seq, worker, staff_id, device_id, device_label, client_occurred_at, received_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const now = utcNow();
    const accepted = [];
    for (const e of fresh) {
      let reverses = null, replaces = null;
      if (e.action === 'reversal') {
        const target = db.prepare('SELECT * FROM f_inbound_check_quantity_events WHERE event_seq = ?').get(e.reverses_event_seq);
        if (!target || target.work_date !== workDate || target.line_key !== line.line_key || target.code_key !== line.code_key) {
          throw new Abort({ ok: false, error: 'not_found', message: '打ち消す対象の記録が見つかりません' });
        }
        if (target.action !== 'add') throw new Abort({ ok: false, error: 'bad_request', message: '打ち消せるのは加算だけです' });
        if (target.quantity !== e.quantity) throw new Abort({ ok: false, error: 'bad_request', message: '打ち消す数が元の記録と一致しません' });
        const already = db.prepare('SELECT 1 FROM f_inbound_check_quantity_events WHERE reverses_event_seq = ?').get(e.reverses_event_seq);
        if (already) throw new Abort({ ok: false, error: 'already_reversed', message: 'その記録は既に打ち消されています' });
        reverses = e.reverses_event_seq;
      } else if (e.replaces_event_seq != null) {
        // 訂正の後半 (新しい入数での add)。同じリクエスト内の reversal が指す先と一致すること
        if (!posInt(e.replaces_event_seq)) throw new Abort({ ok: false, error: 'bad_request', message: 'replaces_event_seq が不正です' });
        const ok = events.some(x => x.action === 'reversal' && x.reverses_event_seq === e.replaces_event_seq);
        if (!ok) throw new Abort({ ok: false, error: 'bad_request', message: '訂正は打ち消しと同時に送ってください' });
        replaces = e.replaces_event_seq;
      }
      const info = ins.run(e.client_event_id, workDate, active.id, line.line_key, line.code_key, line.ar_no, line.product_id,
        e.action, e.quantity, e.input_kind, e.unit_size ?? null, reverses, replaces,
        w, staffId, deviceId, deviceLabel, e.client_occurred_at ? String(e.client_occurred_at).slice(0, 40) : null, now);
      accepted.push({ client_event_id: e.client_event_id, event_seq: Number(info.lastInsertRowid) });
    }

    const found = quantitySum(db, workDate, line.line_key, line.code_key);
    if (found < 0) throw new Abort({ ok: false, error: 'negative_total', message: '打ち消しが多すぎます (合計が0未満になります)' });
    db.prepare(`UPDATE f_inbound_check_line_state
      SET found_qty = ?, quantity_version = quantity_version + 1, quantity_work_date = ?,
          current_pack_qty = COALESCE(?, current_pack_qty)
      WHERE batch_id = ? AND line_key = ?`)
      .run(found, workDate, posInt(packQty) ? packQty : null, active.id, line.line_key);
    return { ok: true, replayed: false, accepted, state: quantityState(db, active.id, lineKey) };
  });
  try {
    return tx.immediate();
  } catch (e) {
    if (e instanceof Abort) return e.result;
    // 打ち消しの一意制約 (同時に同じ加算を打ち消した) は 409 に正規化する
    if (/UNIQUE constraint failed: f_inbound_check_quantity_events\.reverses_event_seq/.test(e.message)) {
      return { ok: false, error: 'already_reversed', message: 'その記録は既に打ち消されています' };
    }
    if (/UNIQUE constraint failed: f_inbound_check_quantity_events\.client_event_id/.test(e.message)) {
      return { ok: false, error: 'idempotency_conflict', message: '同じ操作IDが同時に送られました (画面を読み込み直してください)' };
    }
    throw e;
  }
}

/** その行の数量イベント (訂正パネル用)。打ち消し済みの加算には reversed=1 を立てる */
export function listQuantityEvents(batchId, lineKey) {
  const db = getDB();
  const line = db.prepare('SELECT line_key, code_key FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?').get(Number(batchId), String(lineKey || ''));
  if (!line) return [];
  const batch = getBatch(batchId);
  if (!batch) return [];
  const workDate = batch.work_date || workDateJst();
  return db.prepare(`SELECT e.event_seq, e.action, e.quantity, e.input_kind, e.unit_size, e.worker,
      e.replaces_event_seq, e.reverses_event_seq, e.received_at,
      EXISTS (SELECT 1 FROM f_inbound_check_quantity_events r WHERE r.reverses_event_seq = e.event_seq) AS reversed
    FROM f_inbound_check_quantity_events e
    WHERE e.work_date = ? AND e.line_key = ? AND e.code_key = ? ORDER BY e.event_seq`)
    .all(workDate, line.line_key, line.code_key);
}

/**
 * 確定 (finalize)。「全部あり」「残りも全部あり」「不足で確定」「超過で確定」の全部がここを通る。
 *
 * ⭐**入庫情報の書き戻しも同じトランザクションに入れる**。現行実装は router 側で先に
 *   マスタを書いてから確認を実行していたので、**確認が 409 で失敗したのにマスタだけ変わる**
 *   ことがあった (Codex 指摘)。decide は「検証 + 書き戻し」を行うコールバックで、
 *   ここで失敗したらイベントも状態もマスタも丸ごと巻き戻る。
 *
 * @param {object} o
 *   result   'exact' | 'shortage' | 'excess'  … 確定後の found と予定数の関係。人が選んだ意味
 *   mode     'current'        … 今の found_qty で確定
 *            'fill_remaining' … 残り (planned - found) を1イベント足してから exact 確定
 *   fillEvent { client_event_id, client_occurred_at? }  … mode='fill_remaining' のとき必須
 *   decide(line, foundQty) → {ok:true, destination, decidedFrom, expiryDate, warning} | {ok:false, ...}
 */
export function finalizeLine({ batchId, lineKey, expectVersion, expectQuantityVersion, result, mode = 'current',
  fillEvent = null, clientOperationId = null, worker, staffId = null, deviceId = null, deviceLabel = null, decide = null }) {
  const w = String(worker || '').trim();
  if (!w) return { ok: false, error: 'bad_request', message: '作業者を選んでください' };
  if (!['exact', 'shortage', 'excess'].includes(result)) return { ok: false, error: 'bad_request', message: 'result が不正です' };
  if (!['current', 'fill_remaining'].includes(mode)) return { ok: false, error: 'bad_request', message: 'mode が不正です' };
  if (!Number.isSafeInteger(expectVersion) || expectVersion < 1) return { ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' };
  if (!Number.isSafeInteger(expectQuantityVersion) || expectQuantityVersion < 1) return { ok: false, error: 'bad_request', message: 'expect_quantity_version (正の整数) が必要です' };
  if (mode === 'fill_remaining' && (!fillEvent || !isId(fillEvent.client_event_id))) {
    return { ok: false, error: 'bad_request', message: '不足分を足すための client_event_id が必要です' };
  }
  if (clientOperationId != null && !isId(clientOperationId)) return { ok: false, error: 'bad_request', message: 'client_operation_id が不正です' };

  const db = getDB();
  const tx = db.transaction(() => {
    // 再送 (応答だけ失われた) は、同じ操作IDなら何もせず現在状態を返す
    if (clientOperationId) {
      const done = db.prepare('SELECT batch_id, line_key FROM f_inbound_check_events WHERE client_operation_id = ?').get(clientOperationId);
      if (done) return { ok: true, replayed: true, state: quantityState(db, done.batch_id, done.line_key) };
    }
    const g = loadForQuantity(db, batchId, lineKey);
    if (g.error) throw new Abort(g.error);
    const { active, line, state, workDate } = g;
    if (state.status === 'checked') {
      throw new Abort({ ok: false, error: 'finalized', message: '既に確認済みです', current: quantityState(db, active.id, lineKey) });
    }
    if (state.version !== expectVersion || state.quantity_version !== expectQuantityVersion) {
      throw new Abort({ ok: false, error: 'conflict', message: '他の端末で先に更新されました。最新の状態を表示します', current: quantityState(db, active.id, lineKey) });
    }

    let found = quantitySum(db, workDate, line.line_key, line.code_key);
    if (mode === 'fill_remaining') {
      const rest = line.planned_qty - found;
      if (rest <= 0) throw new Abort({ ok: false, error: 'result_mismatch', message: '不足はありません (すでに予定数に達しています)', current: quantityState(db, active.id, lineKey) });
      db.prepare(`INSERT INTO f_inbound_check_quantity_events
        (client_event_id, work_date, batch_id, line_key, code_key, ar_no, product_id, action, quantity, input_kind,
         worker, staff_id, device_id, device_label, client_occurred_at, received_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'add', ?, 'fill_remaining', ?, ?, ?, ?, ?, ?)`)
        .run(fillEvent.client_event_id, workDate, active.id, line.line_key, line.code_key, line.ar_no, line.product_id,
          rest, w, staffId, deviceId, deviceLabel,
          fillEvent.client_occurred_at ? String(fillEvent.client_occurred_at).slice(0, 40) : null, utcNow());
      found = quantitySum(db, workDate, line.line_key, line.code_key);
    }
    // 人が選んだ意味と、実際の数が食い違ったまま確定させない
    if (quantityRelation(found, line.planned_qty) !== result) {
      throw new Abort({ ok: false, error: 'result_mismatch',
        message: `数が変わっています (${found} / 予定 ${line.planned_qty})。もう一度確認してください`,
        current: { ...quantityState(db, active.id, lineKey), found_qty: found } });
    }

    // 行き先・BCシール・入数・有効期限。**ここで失敗したらマスタへの書き戻しごと巻き戻る**
    let decided = { destination: null, decidedFrom: null, expiryDate: null, warning: null };
    if (decide) {
      const d = decide(line, found);
      if (!d || !d.ok) throw new Abort(d && d.abort ? d.abort : { ok: false, error: 'destination_required', message: '行き先を決めてください' });
      decided = d;
    }

    const now = utcNow();
    // 残りを足して確定した場合は数量も動いたので quantity_version も進める
    const upd = db.prepare(`UPDATE f_inbound_check_line_state
      SET status = 'checked', version = version + 1, quantity_version = quantity_version + ?, found_qty = ?, finalized_result = ?,
          checked_by = ?, checked_device = ?, checked_at = ?
      WHERE batch_id = ? AND line_key = ? AND status = 'unchecked' AND version = ?`)
      .run(mode === 'fill_remaining' ? 1 : 0, found, result, w, deviceLabel, now, active.id, line.line_key, expectVersion);
    if (upd.changes === 0) {
      throw new Abort({ ok: false, error: 'conflict', message: '他の端末で先に更新されました', current: quantityState(db, active.id, lineKey) });
    }
    let destinationId = null;
    if (decided.destination) {
      const di = db.prepare(`INSERT INTO f_inbound_check_destinations
        (batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, staff_id,
         device_label, decided_at, expiry_date, work_date, code_key, actual_qty)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(active.id, line.line_key, line.ar_no, line.product_id, line.product_name, line.planned_qty,
          decided.destination, decided.decidedFrom || 'master', w, staffId, deviceLabel, now,
          decided.expiryDate || null, workDate, line.code_key, found);
      destinationId = Number(di.lastInsertRowid);
      db.prepare('UPDATE f_inbound_check_line_state SET destination_id = ? WHERE batch_id = ? AND line_key = ?')
        .run(destinationId, active.id, line.line_key);
      // ⭐いろは行きは、その場で在庫化アプリのタスクになる (17:30 を待たない)。失敗すれば確定ごと巻き戻る
      if (decided.destination === 'iroha') {
        const dest = db.prepare('SELECT * FROM f_inbound_check_destinations WHERE id = ?').get(destinationId);
        createTaskForDestination(dest, { actor: w, barcode: line.barcode || null });
      }
    }
    db.prepare(`INSERT INTO f_inbound_check_events
      (batch_id, line_key, ar_no, action, worker, staff_id, device_id, device_label, created_at,
       client_operation_id, result, found_qty, planned_qty_snapshot, quantity_version, destination_id, client_occurred_at)
      VALUES (?, ?, ?, 'check', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(active.id, line.line_key, line.ar_no, w, staffId, deviceId, deviceLabel, now,
        clientOperationId, result, found, line.planned_qty, state.quantity_version, destinationId, null);
    return { ok: true, replayed: false, state: quantityState(db, active.id, lineKey), warning: decided.warning || null };
  });
  try {
    return tx.immediate();
  } catch (e) {
    if (e instanceof Abort) return e.result;
    if (/UNIQUE constraint failed: f_inbound_check_events\.client_operation_id/.test(e.message)) {
      return { ok: false, error: 'idempotency_conflict', message: '同じ操作が同時に送られました (画面を読み込み直してください)' };
    }
    throw e;
  }
}

/**
 * やり直す (reopen)。確認を解除するだけで **数えた数は残す**。
 * 数量記録まで消したいときは打ち消しイベントを積む (別導線)。
 */
export function reopenLine({ batchId, lineKey, expectVersion, expectQuantityVersion, clientOperationId = null,
  worker, staffId = null, deviceId = null, deviceLabel = null }) {
  const w = String(worker || '').trim();
  if (!w) return { ok: false, error: 'bad_request', message: '作業者を選んでください' };
  if (!Number.isSafeInteger(expectVersion) || expectVersion < 1) return { ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' };
  if (clientOperationId != null && !isId(clientOperationId)) return { ok: false, error: 'bad_request', message: 'client_operation_id が不正です' };
  const db = getDB();
  const tx = db.transaction(() => {
    if (clientOperationId) {
      const done = db.prepare('SELECT batch_id, line_key FROM f_inbound_check_events WHERE client_operation_id = ?').get(clientOperationId);
      if (done) return { ok: true, replayed: true, state: quantityState(db, done.batch_id, done.line_key) };
    }
    const g = loadForQuantity(db, batchId, lineKey);
    if (g.error) throw new Abort(g.error);
    const { active, line, state } = g;
    if (state.status !== 'checked') {
      throw new Abort({ ok: false, error: 'conflict', message: 'この行は確認済みではありません', current: quantityState(db, active.id, lineKey) });
    }
    if (expectQuantityVersion != null && state.quantity_version !== expectQuantityVersion) {
      throw new Abort({ ok: false, error: 'conflict', message: '他の端末で先に更新されました', current: quantityState(db, active.id, lineKey) });
    }
    const now = utcNow();
    // ⭐やり直すとき、確定に使った有効期限を「先入力」に戻す (Codex #1116 Med-3 の選択肢b)。
    //   一覧タグに 📅入力済 として**見える**ので黙って再利用にはならない。期限を直したい
    //   やり直しは、詳細パネルで選び直せばそのまま上書きされる。
    //   SET の右辺は更新前の行の値を見る (SQLite仕様) ので destination_id=NULL と同時でも参照できる
    const upd = db.prepare(`UPDATE f_inbound_check_line_state
      SET status = 'unchecked', version = version + 1, finalized_result = NULL, destination_id = NULL,
          pending_expiry = COALESCE((SELECT expiry_date FROM f_inbound_check_destinations WHERE id = f_inbound_check_line_state.destination_id), pending_expiry),
          checked_by = NULL, checked_device = NULL, checked_at = NULL
      WHERE batch_id = ? AND line_key = ? AND status = 'checked' AND version = ?`)
      .run(active.id, line.line_key, expectVersion);
    if (upd.changes === 0) {
      throw new Abort({ ok: false, error: 'conflict', message: '他の端末で先に更新されました', current: quantityState(db, active.id, lineKey) });
    }
    // ⭐取り消すのは「この確認で作った1行」だけ。line_key 全体を消すと、同日に引き継いだ
    //   前の確認の実績まで巻き添えで消える
    if (state.destination_id) {
      db.prepare("UPDATE f_inbound_check_destinations SET cancelled_at = ?, cancelled_by = ?, cancel_reason = 'reopen' WHERE id = ? AND cancelled_at IS NULL")
        .run(now, w, state.destination_id);
      // 在庫化アプリのタスクにも伝える (未着手・実績なしは自動で取消、着手後は職員の要確認に倒す)
      requestCancellation({ destinationId: state.destination_id, source: 'inbound_reversal', actor: w });
    }
    const last = db.prepare("SELECT id FROM f_inbound_check_events WHERE batch_id = ? AND line_key = ? AND action = 'check' ORDER BY id DESC LIMIT 1")
      .get(active.id, line.line_key);
    db.prepare(`INSERT INTO f_inbound_check_events
      (batch_id, line_key, ar_no, action, worker, staff_id, device_id, device_label, created_at, reverted_event_id,
       client_operation_id, found_qty, planned_qty_snapshot, quantity_version, destination_id)
      VALUES (?, ?, ?, 'uncheck', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(active.id, line.line_key, line.ar_no, w, staffId, deviceId, deviceLabel, now, last ? last.id : null,
        clientOperationId, state.found_qty, line.planned_qty, state.quantity_version, state.destination_id || null);
    return { ok: true, replayed: false, state: quantityState(db, active.id, lineKey) };
  });
  try {
    return tx.immediate();
  } catch (e) {
    if (e instanceof Abort) return e.result;
    if (/UNIQUE constraint failed: f_inbound_check_events\.client_operation_id/.test(e.message)) {
      return { ok: false, error: 'idempotency_conflict', message: '同じ操作が同時に送られました' };
    }
    throw e;
  }
}

// ─────────────────── 入庫情報の選択肢 ───────────────────

// 現場が実際に使っている表記。**専用の表は作らない** — 選択肢は f_inbound_info の
// 実データから作り、新しい値を保存したら次から選択肢に並ぶ (自分で育つ)。
// この定数は「まだ1件も入っていない列でも空にしない」ための土台
const FIELD_SEEDS = {
  いろは在庫化作業有無: ['有り', '無し', '状況による'],
  入庫時BCシール貼りフラグ: ['BCシール貼付必要', '不要'],
  直接ピックロケ保管: ['直接ピックロケ', '無'],
  BF保管荷姿: ['そのまま', 'バラ', '内箱で保管', '20L折りコン入替', '120サイズ入替'],
};
export const OPTION_FIELDS = Object.keys(FIELD_SEEDS);
// いろは=有り のとき他項目に入る「未記入」の印。選択肢には出さない
const NA_MARK = String.fromCharCode(0xFF0D);

/**
 * 詳細パネルのプルダウンに出す選択肢。
 *
 * 中原さん 2026-09-02:「自由に入れられる形をやめてセレクトにしてほしい (文字を消さないと
 * 入れられないので)。自由に入れる場合は『新規で登録』を選ぶと入れられて、それが登録されると
 * 選択肢が増えるようにしたい」
 *
 * ⭐**よく使われている順**に並べる (件数の多い表記が上)。現場が普段使う値が上に来る。
 *   種類が増えすぎないよう1列あたり上位30件まで。土台の値は必ず含める
 */
export function fieldOptions() {
  const db = getDB();
  const out = {};
  const has = tableExists(db, 'f_inbound_info');
  for (const [field, seeds] of Object.entries(FIELD_SEEDS)) {
    const seen = new Map();   // 表記ゆれを潰さない (現場の表記をそのまま出す) が、完全一致の重複だけまとめる
    if (has) {
      // 列名は固定の4つだけ (FIELD_SEEDS のキー) なので SQL への埋め込みは安全
      const rows = db.prepare(`SELECT ${field} AS v, COUNT(*) AS n FROM f_inbound_info
        WHERE ${field} IS NOT NULL AND trim(${field}) <> '' AND trim(${field}) <> ?
        GROUP BY v ORDER BY n DESC, v LIMIT 30`).all(NA_MARK);
      for (const r of rows) {
        const v = String(r.v || '').trim();
        if (v) seen.set(v, r.n);
      }
    }
    for (const s of seeds) if (!seen.has(s)) seen.set(s, 0);
    out[field] = [...seen.keys()];
  }
  return out;
}

// ─────────────────── 完了した伝票の一覧 (棚入れ・確認用) ───────────────────

/**
 * 入荷番号 (AR) 単位で「確認し終えた明細」をまとめて返す。
 *
 * 中原さん 2026-09-02:「入荷番号のリストが全部チェックされたら一覧を表示させてほしい。
 * そのときに期限があるものは期限と数量を一覧にしてほしい。画像とかは要らない。
 * その一覧は PC からも見れるように」
 *
 * ⭐出所は **f_inbound_check_destinations だけ**。この表は batch_id を FK にしていないので、
 *   取込バッチが保持期間で消えたあとも一覧が残る (棚入れ後に見返せないと意味がない)。
 * ⚠「完了」= その AR の**確認済み行数が、その日にその AR で見た明細数と一致**すること。
 *   明細数は台帳だけでは分からないので、active バッチにある AR は f_inbound_check_lines で
 *   照合し、消えたバッチの AR は「台帳にある行数 = 完了行数」として扱う (全部確認したから残っている)
 *
 * @param {object} o { days, arNo, workDate, includeIncomplete }
 * @returns {Array<{ar_no, work_date, done, line_count, checked_count, expiry_count, lines: [...]}>}
 */
export function listCompletedSlips({ days = 14, arNo = null, workDate = null, includeIncomplete = false } = {}) {
  const db = getDB();
  const where = ['d.cancelled_at IS NULL'];
  const params = [];
  if (arNo) { where.push('d.ar_no = ?'); params.push(String(arNo)); }
  if (workDate) { where.push('d.work_date = ?'); params.push(String(workDate)); }
  else {
    const n = Math.max(1, Math.min(400, Number(days) || 14));
    where.push("d.work_date >= date('now', '+9 hours', ?)");
    params.push(`-${n - 1} days`);
  }
  const rows = db.prepare(`SELECT d.ar_no, d.work_date, d.line_key, d.product_id, d.product_name,
      d.planned_qty, d.actual_qty, d.expiry_date, d.destination, d.worker, d.decided_at, d.code_key
    FROM f_inbound_check_destinations d
    WHERE ${where.join(' AND ')}
    ORDER BY d.work_date DESC, d.ar_no, d.id`).all(...params);

  // その AR に本来何行あるか。active バッチにある分だけ分かる (消えたバッチは台帳の行数で代用)
  const planned = new Map();
  for (const r of db.prepare(`SELECT b.work_date, l.ar_no, COUNT(*) AS n
    FROM f_inbound_check_lines l JOIN f_inbound_check_batches b ON b.id = l.batch_id
    WHERE b.status = 'active' GROUP BY b.work_date, l.ar_no`).all()) {
    planned.set(`${r.work_date}|${r.ar_no}`, r.n);
  }

  const bySlip = new Map();
  for (const r of rows) {
    const key = `${r.work_date}|${r.ar_no}`;
    if (!bySlip.has(key)) {
      bySlip.set(key, { ar_no: r.ar_no, work_date: r.work_date, lines: [], workers: new Set(), last_at: r.decided_at });
    }
    const s = bySlip.get(key);
    // 同じ明細が2度出たら最新を採る (取り消して確認し直した場合)
    const idx = s.lines.findIndex(x => x.line_key === r.line_key);
    if (idx >= 0) s.lines[idx] = r; else s.lines.push(r);
    if (r.worker) s.workers.add(r.worker);
    if (r.decided_at > s.last_at) s.last_at = r.decided_at;
  }

  const out = [];
  for (const s of bySlip.values()) {
    const total = planned.get(`${s.work_date}|${s.ar_no}`) ?? s.lines.length;
    const done = s.lines.length >= total;
    if (!done && !includeIncomplete) continue;
    out.push({
      ar_no: s.ar_no, work_date: s.work_date, done,
      line_count: total, checked_count: s.lines.length,
      expiry_count: s.lines.filter(l => l.expiry_date).length,
      iroha_count: s.lines.filter(l => l.destination === 'iroha').length,
      workers: [...s.workers], last_at: s.last_at,
      // 期限があるものを先に。棚入れのとき「期限を書く商品」から片付けたい
      lines: s.lines.slice().sort((a, b) => (b.expiry_date ? 1 : 0) - (a.expiry_date ? 1 : 0)
        || String(a.product_id).localeCompare(String(b.product_id))),
    });
  }
  out.sort((a, b) => (b.work_date || "").localeCompare(a.work_date || "") || (b.last_at || "").localeCompare(a.last_at || ""));
  return out;
}

/** 完了一覧の CSV (UTF-8 BOM)。PC で開いて印刷・保管する用 */
export function completedSlipsCsv(opts = {}) {
  const slips = listCompletedSlips({ ...opts });
  const esc = v => {
    let s = v == null ? '' : String(v);
    if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;   // 表計算ソフトが数式として解釈しないように
    return `"${s.replace(/"/g, '""')}"`;
  };
  const head = ['作業日', '入荷管理番号', '商品ID', '商品名', '予定数', '実数', '有効期限', '行き先', '作業者', '確認日時'];
  const body = [];
  for (const s of slips) {
    for (const l of s.lines) {
      body.push([s.work_date, s.ar_no, l.product_id, l.product_name, l.planned_qty,
        l.actual_qty == null ? l.planned_qty : l.actual_qty, l.expiry_date,
        l.destination === 'iroha' ? 'いろは在庫化' : 'B-Faith入庫', l.worker, l.decided_at].map(esc).join(','));
    }
  }
  return '\ufeff' + [head.map(esc).join(','), ...body].join('\r\n') + '\r\n';
}

// ───────────────────────── 履歴 ─────────────────────────

export function listEvents(batchId, limit = 5000) {
  return getDB().prepare(`SELECT e.*, l.product_id, l.product_name, l.planned_qty
    FROM f_inbound_check_events e
    LEFT JOIN f_inbound_check_lines l ON l.batch_id = e.batch_id AND l.line_key = e.line_key
    WHERE e.batch_id = ? ORDER BY e.id LIMIT ?`).all(Number(batchId), Math.max(1, Math.min(50000, Number(limit) || 5000)));
}

/** 履歴 CSV (UTF-8 BOM)。取消は reverted_event_id で打ち消し先を示す */
export function eventsCsv(batchId) {
  const rows = listEvents(batchId, 50000);
  const esc = v => {
    let s = v == null ? '' : String(v);
    // Excel の数式インジェクション対策: 先頭が = + - @ タブ CR の値はアポストロフィで無害化 (Codex R3 Medium)
    if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const head = ['event_id', 'batch_id', 'AR番号', '明細キー', '商品ID', '商品名', '予定数', '操作', '作業者', '端末', '日時(UTC)', '打ち消した確認ID'];
  const lines = [head.join(',')];
  for (const r of rows) {
    lines.push([r.id, r.batch_id, r.ar_no, r.line_key, r.product_id, r.product_name, r.planned_qty,
      r.action === 'check' ? '確認' : '取消', r.worker, r.device_label, r.created_at, r.reverted_event_id].map(esc).join(','));
  }
  return '﻿' + lines.join('\r\n') + '\r\n';
}
