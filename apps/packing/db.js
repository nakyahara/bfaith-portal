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

const SCHEMA_VERSION = 20;

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
  // v2: 作業画面 = 納品書PDF同等の1伝票1画面 (中原さん指示 2026-08-16)。
  // 送り先 (名前・〒・住所) とサイト受注番号・注文日・納品書印字ヘッダを表示するため保存する。
  // 表示不要と明示されたもの (電話番号・購入者情報・決済方法・金額系・コメント本文・のし内容) は
  // 引き続き保存しない/表示しない (電話番号は列自体を持たない)
  2: () => {
    for (const col of [
      'recipient_name TEXT',   // 配送先名
      'recipient_zip TEXT',    // 配送先郵便番号
      'recipient_pref TEXT',   // 配送先都道府県
      'recipient_addr TEXT',   // 配送先住所1〜3 の連結
      'site_order_no TEXT',    // サイト受注№
      'order_date TEXT',       // 注文日 'YYYY-MM-DD'
      'print_header1 TEXT',    // 納品書印字ヘッダ1 (例: レターパック500 — 資材指示)
    ]) {
      db.exec(`ALTER TABLE pk_pack_slips ADD COLUMN ${col}`);
    }
  },
  // v3: 運用基盤 (2026-08-16)。
  //   - Drive自動取込の台帳 (picking pk_drive_imports と同設計。版=(file_id, modified_time))
  //   - iPad の登録端末 (picking pk_devices と同設計。Cookie path が /apps/packing のため専用表)
  //   - 中断 (pause) の状態列 (中断時間は梱包時間から除外する — picking v3 と同じ)
  3: () => {
    db.exec('ALTER TABLE pk_pack_batches ADD COLUMN pause_started_at TEXT');
    db.exec('ALTER TABLE pk_pack_batches ADD COLUMN pause_reason TEXT');
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_drive_imports (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      drive_file_id TEXT NOT NULL,
      modified_time TEXT NOT NULL,
      filename      TEXT NOT NULL,
      folder_name   TEXT,
      status        TEXT NOT NULL CHECK(status IN ('imported','failed','skipped')),
      error         TEXT,
      batch_id      INTEGER REFERENCES pk_pack_batches(id),
      attempts      INTEGER NOT NULL DEFAULT 1,
      first_seen_at TEXT NOT NULL,
      processed_at  TEXT NOT NULL,
      UNIQUE (drive_file_id, modified_time)
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_drive_imports_at ON pk_pack_drive_imports(processed_at)');
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_devices (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash   TEXT NOT NULL UNIQUE,
      label        TEXT NOT NULL,
      created_by   TEXT NOT NULL,
      created_at   TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at   TEXT
    )`);
    // ④ 配送方法変更 (要件§5.7 最小構成): 梱包者の提案 → 事務の対応状態を持つ。
    // 対象伝票は held (hold_reason='shipping_change') になり、completed/rejected で pending に戻る
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_ship_changes (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id        INTEGER NOT NULL REFERENCES pk_pack_batches(id),
      slip_seq        INTEGER NOT NULL,
      ne_slip_no      TEXT NOT NULL,
      folder_name     TEXT,
      current_method  TEXT,
      proposed_method TEXT NOT NULL,
      reason          TEXT NOT NULL,
      requested_by    TEXT NOT NULL,
      status          TEXT NOT NULL DEFAULT 'requested'
        CHECK(status IN ('requested','accepted','rejected','completed','cancelled')),
      office_by       TEXT,
      updated_at      TEXT NOT NULL,
      created_at      TEXT NOT NULL
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_ship_changes ON pk_pack_ship_changes(status, id)');
  },
  // v4: ①再ピック / ②棚戻し / ③ピッキングミス記録 (要件§5.4〜5.6・Phase 2)
  4: () => {
    // 再ピック・棚戻しタスク。DBの行が正本 (GChat通知はチャネル)。
    // 実行UIは apps/picking (要件§7.1) だが、更新は packing の service (applyTaskAction) を通す
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_tasks (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id     INTEGER NOT NULL REFERENCES pk_pack_batches(id),
      slip_seq     INTEGER,               -- 依頼元伝票 (余りはバッチ単位でNULLあり)
      kind         TEXT NOT NULL CHECK(kind IN ('repick','return')),
      sku          TEXT NOT NULL,
      product_name TEXT,
      req_qty      INTEGER NOT NULL,
      location     TEXT,                  -- pk_lines由来の参考ロケ (戻し先の正ではない — 要件§5.5)
      block        TEXT,
      folder_name  TEXT,                  -- 依頼元 出荷_XX (届け先の識別)
      status       TEXT NOT NULL DEFAULT 'requested'
        CHECK(status IN ('requested','claimed','fulfilled','returned','received','unavailable','cancelled')),
      requested_by TEXT NOT NULL,
      claimed_by   TEXT,
      incident_id  INTEGER,               -- 対応するミス候補 (wrong_itemは2タスクが同じidを指す)
      created_at   TEXT NOT NULL,
      updated_at   TEXT NOT NULL
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_tasks_open ON pk_pack_tasks(status, id)');
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_tasks_batch ON pk_pack_tasks(batch_id, slip_seq)');
    // ピッキングミス候補 (③)。作業中=candidate → 梱包完了サマリで confirmed / withdrawn (要件§5.6 2段階)
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_incidents (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id          INTEGER NOT NULL REFERENCES pk_pack_batches(id),
      slip_seq          INTEGER,
      kind              TEXT NOT NULL CHECK(kind IN ('shortage','excess','wrong_item')),
      sku               TEXT NOT NULL,     -- 期待SKU (excessは余った実SKU)
      actual_sku        TEXT,              -- wrong_item: 実際に入っていたSKU
      qty               INTEGER NOT NULL,
      status            TEXT NOT NULL DEFAULT 'candidate'
        CHECK(status IN ('candidate','withdrawn','confirmed')),
      attributed_worker TEXT,              -- 確定時: pk_batches.worker (ピッキング担当)
      detected_by       TEXT NOT NULL,     -- 検知した梱包者
      confirmed_by      TEXT,
      created_at        TEXT NOT NULL,
      updated_at        TEXT NOT NULL
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_incidents_batch ON pk_pack_incidents(batch_id, status)');
  },
  // v5: ④通知の配送保証 (Codexレビュー high: 事務キュー廃止後、GChat失敗で依頼が事実上消える)。
  // 通知成否を行に記録し、失敗分はポーラーが再送する
  5: () => {
    db.exec('ALTER TABLE pk_pack_ship_changes ADD COLUMN notified_at TEXT');
    db.exec('ALTER TABLE pk_pack_ship_changes ADD COLUMN notify_error TEXT');
  },
  // v6: 梱包機ライン管理 (PAS-LINE/MELT-LINE — 紙台帳の置き換え。中原さん指示 2026-08-18)。
  // 梱包機バッチは1伝票1画面ではなく工程単位で記録する:
  //   sort = MELT-LINE の事前仕分け (final_count = 配送変更を差し引いた最終通過件数)
  //   run  = 機械流し (final_count = 出荷完了件数 / manual_count = うち手動で流した件数)
  6: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_line_runs (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id      INTEGER NOT NULL REFERENCES pk_pack_batches(id),
      phase         TEXT NOT NULL CHECK(phase IN ('sort','run')),
      started_at    TEXT,
      finished_at   TEXT,
      planned_count INTEGER,
      final_count   INTEGER,
      manual_count  INTEGER,
      note          TEXT,
      worker        TEXT,
      updated_at    TEXT NOT NULL,
      UNIQUE (batch_id, phase)
    )`);
  },
  // v7: ライン運用改善 (中原さん指示 2026-08-18 実機フィードバック)。
  // MELT仕分けは「他の方法で出荷する件数」を入力し、機械に流す件数=伝票数-除外を自動計算
  7: () => {
    db.exec('ALTER TABLE pk_pack_line_runs ADD COLUMN excluded_count INTEGER');
    // v6時代の記録済み仕分け行をバックフィル (Codex medium: NULLのままだと「除外0件」と矛盾表示)
    db.exec(`UPDATE pk_pack_line_runs SET excluded_count = planned_count - final_count
      WHERE phase = 'sort' AND final_count IS NOT NULL AND excluded_count IS NULL`);
  },
  // v8: 🖨伝票再印刷依頼 (2026-08-21 中原さん指示)。現場→事務班への口頭指示をボタン化。
  // 通知が実質の伝達経路 (④配送変更と同型: 成否記録+ポーラー再送)。
  // pdf_token = 抜き出した送り状PDFの配信URL用 (推測不能・SAが閲覧者共有のためDrive保存は不可)
  8: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_reprints (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id       INTEGER NOT NULL REFERENCES pk_pack_batches(id),
      slip_seq       INTEGER NOT NULL,
      ne_slip_no     TEXT NOT NULL,
      site_order_no  TEXT,
      folder_name    TEXT,
      recipient_name TEXT,
      requested_by   TEXT NOT NULL,
      pdf_token      TEXT,
      pdf_error      TEXT,
      notified_at    TEXT,
      notify_error   TEXT,
      created_at     TEXT NOT NULL
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_reprints_at ON pk_pack_reprints(created_at)');
  },
  // v9: 再ピック運用の現場フィードバック対応 (中原さん指示 2026-08-23)。
  //   - actual_name: 品違いの「実際に入っていた商品」は記録時に在庫検索で特定 (自由入力文字列を
  //     SKU列に入れない)。表示名はサーバー由来をここに保持する
  //   - blocked_since / blocked_total_sec: 「ピッキングミスで梱包できない待ち時間」
  //     (未処理ゼロ+保留あり=再ピック待ちで手が止まる区間) は梱包時間に含めない (中断と同型)
  9: () => {
    db.exec('ALTER TABLE pk_pack_incidents ADD COLUMN actual_name TEXT');
    db.exec('ALTER TABLE pk_pack_batches ADD COLUMN blocked_since TEXT');
    db.exec('ALTER TABLE pk_pack_batches ADD COLUMN blocked_total_sec INTEGER NOT NULL DEFAULT 0');
  },
  // v10: 梱包資材の表示・現場登録 (要件定義 = AI_reference『梱包資材表示_要件定義_20260823.md』v1.7)。
  //   - 判定順: hidden (分類 hide_card) → held (④依頼中) → header (伝票指定) → rule (登録) →
  //     candidates (分類別候補) → unknown。配送種別は print_header1 の完全一致辞書から導出
  //   - ルールは version CAS + active 部分一意。イベントは追記型で通知 outbox を兼ねる
  //   - views は伝票1行の表示観測ログ (未レビュー値の検出・採用実績。意図的に FK なし)
  10: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_materials (
      code         TEXT PRIMARY KEY CHECK(code NOT GLOB '*[^a-z0-9_]*' AND length(code) BETWEEN 2 AND 40),
      name         TEXT NOT NULL,
      color        TEXT,
      image_file   TEXT,
      sort_order   INTEGER NOT NULL DEFAULT 100 CHECK(sort_order BETWEEN 0 AND 9999),
      is_active    INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL, updated_by TEXT
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_header_map (
      header_value  TEXT PRIMARY KEY,
      base_delivery_code TEXT NOT NULL CHECK(base_delivery_code IN
        ('nekopos','yupacket_puff','teikeigai','letterpack','takkyubin50','takkyubin60plus','aes','unsupported')),
      material_code TEXT REFERENCES pk_pack_materials(code),
      updated_at TEXT NOT NULL, updated_by TEXT
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_classes (
      class_value TEXT PRIMARY KEY,
      aes_kind    TEXT CHECK(aes_kind IN ('mail','other')),
      hide_card   INTEGER NOT NULL DEFAULT 0 CHECK(hide_card IN (0,1)),
      sort_order  INTEGER NOT NULL DEFAULT 100,
      updated_at TEXT NOT NULL, updated_by TEXT
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_class_materials (
      class_value   TEXT NOT NULL REFERENCES pk_pack_classes(class_value),
      material_code TEXT NOT NULL REFERENCES pk_pack_materials(code),
      sort_order    INTEGER NOT NULL DEFAULT 100,
      updated_at TEXT NOT NULL, updated_by TEXT,
      PRIMARY KEY (class_value, material_code)
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_material_rules (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      combo_key     TEXT NOT NULL CHECK(combo_key GLOB 'v[0-9]*|*' AND length(combo_key) <= 4000),
      combo_detail  TEXT NOT NULL,
      delivery_code TEXT NOT NULL CHECK(delivery_code IN
        ('nekopos','yupacket_puff','teikeigai','letterpack','takkyubin50','takkyubin60plus','aes_mail','aes_other')),
      material_code TEXT NOT NULL REFERENCES pk_pack_materials(code),
      status        TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled')),
      version       INTEGER NOT NULL DEFAULT 1 CHECK(version >= 1),
      created_by TEXT NOT NULL, created_at TEXT NOT NULL,
      updated_by TEXT, updated_at TEXT NOT NULL
    )`);
    db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pk_pack_material_rules_active
      ON pk_pack_material_rules(combo_key, delivery_code) WHERE status='active'`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_material_rules_key ON pk_pack_material_rules(combo_key, delivery_code)');
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_material_events (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      op_id           TEXT NOT NULL UNIQUE,
      request_hash    TEXT NOT NULL,
      response_json   TEXT,
      action          TEXT NOT NULL CHECK(action IN ('register','change','undo','admin_edit','admin_disable')),
      rule_id         INTEGER NOT NULL REFERENCES pk_pack_material_rules(id),
      rule_version    INTEGER NOT NULL CHECK(rule_version >= 1),
      combo_key TEXT NOT NULL, delivery_code TEXT NOT NULL,
      delivery_raw TEXT, hikiate_class TEXT, header_raw TEXT,
      batch_id INTEGER, slip_seq INTEGER, ne_slip_no TEXT, folder_name TEXT,
      shown_source    TEXT CHECK(shown_source IN ('header','rule','candidates','unknown','held','hidden')),
      before_code TEXT, after_code TEXT,
      worker          TEXT NOT NULL,
      created_at      TEXT NOT NULL,
      undo_expires_at TEXT, undone_at TEXT,
      target_event_id INTEGER REFERENCES pk_pack_material_events(id),
      notify_status   TEXT NOT NULL DEFAULT 'none'
        CHECK(notify_status IN ('none','pending','sending','sent','cancelled','failed')),
      notify_due_at TEXT, next_attempt_at TEXT, claimed_at TEXT, claim_token TEXT,
      attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
      resend_requested_at TEXT, resend_by TEXT,
      undo_key_version INTEGER NOT NULL DEFAULT 1,
      notified_at TEXT, notify_error TEXT,
      CHECK ((action IN ('register','change')) = (undo_expires_at IS NOT NULL)),
      CHECK ((action IN ('register','change')) <= (after_code IS NOT NULL)),
      CHECK ((action = 'change') <= (before_code IS NOT NULL)),
      CHECK ((action = 'undo') = (target_event_id IS NOT NULL)),
      CHECK ((action = 'change') = (notify_status <> 'none')),
      CHECK ((notify_status NOT IN ('pending','sending')) OR (notify_due_at IS NOT NULL AND next_attempt_at IS NOT NULL)),
      CHECK ((notify_status <> 'sending') OR (claim_token IS NOT NULL AND claimed_at IS NOT NULL)),
      CHECK ((notify_status <> 'sent') OR (notified_at IS NOT NULL))
    )`);
    db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pk_pack_material_events_undo
      ON pk_pack_material_events(target_event_id) WHERE target_event_id IS NOT NULL`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_pk_pack_material_events_outbox
      ON pk_pack_material_events(next_attempt_at) WHERE notify_status='pending'`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_pk_pack_material_events_sending
      ON pk_pack_material_events(claimed_at) WHERE notify_status='sending'`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_material_events_rule ON pk_pack_material_events(rule_id, id)');
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_material_views (
      batch_id INTEGER NOT NULL, slip_seq INTEGER NOT NULL,
      delivery_raw TEXT, hikiate_class TEXT, delivery_code TEXT NOT NULL,
      header_raw TEXT,
      combo_key TEXT,
      source TEXT NOT NULL CHECK(source IN ('header','rule','candidates','unknown','held','hidden')),
      material_code TEXT, rule_id INTEGER,
      first_shown_at TEXT NOT NULL, last_shown_at TEXT NOT NULL,
      completed_at TEXT,
      completed_source TEXT, completed_material_code TEXT, completed_rule_id INTEGER,
      PRIMARY KEY (batch_id, slip_seq)
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_material_views_src ON pk_pack_material_views(source, last_shown_at)');
  },
  // v11: ライン工程の一時中断 (2026-08-25 現場意見: 「終了」を中断のつもりで押して作業終了扱いになった)。
  //   中断そのものはバッチ単位 (pause/resume・status='paused') を流用し、工程行にも中断秒を持たせて
  //   工程の所要時間 (started→finished) から差し引けるようにする
  11: () => {
    db.exec('ALTER TABLE pk_pack_line_runs ADD COLUMN paused_total_sec INTEGER NOT NULL DEFAULT 0');
  },
  // v12: 📭「送り状がなかった」通知 (2026-08-26 中原さん指示)。再印刷と同じ伝達経路
  //   (事務スペースへ即時通知+送り状PDF追送+未通知は再送) なので同じ表に kind で区別する
  12: () => {
    db.exec("ALTER TABLE pk_pack_reprints ADD COLUMN kind TEXT NOT NULL DEFAULT 'reprint'");
  },
  // v13: 送り状自動印刷 P0/P1 (要件定義 送り状自動印刷_20260827)。
  //   - pdf_by: ページの特定方法 ('manifest' = 注文番号の完全一致 / 'slip_no' / 'name' / 'position')
  //   - pdf_printable: 1 = 自動印刷してよい (manifest経路+白紙検査を通過した場合のみ)。
  //     位置推定は照合漏れで1ページずれて別人の送り状を掴み得るので、人が見る前提の
  //     リンク添付には使うが自動印刷の根拠にはしない
  //   - pdf_ink_ratio: 白紙判定に使った非白ピクセル率 (しきい値の実データ較正用に残す)
  13: () => {
    db.exec('ALTER TABLE pk_pack_reprints ADD COLUMN pdf_by TEXT');
    db.exec('ALTER TABLE pk_pack_reprints ADD COLUMN pdf_printable INTEGER NOT NULL DEFAULT 0');
    db.exec('ALTER TABLE pk_pack_reprints ADD COLUMN pdf_ink_ratio REAL');
  },
  // v14: 送り状自動印刷 P2 — 印刷キュー (要件定義 送り状自動印刷_20260827 §6)。
  //   出荷PCの印刷エージェントが pull で取りに来る。miniPC からは出荷PCへ一切繋がない
  //   (出荷PCの固定IP・受信ポート開放が不要になる)。
  //   ⭐端末は既存の pk_pack_devices を kind で区別して使う (iPad と同じ発行・失効導線に乗せる)。
  //     プリンター名は**サーバ側が端末に紐づけて持つ** — エージェント側の設定ミスで
  //     別のプリンターに送り状を出さないため (要件§6.2)
  14: () => {
    db.exec(`ALTER TABLE pk_pack_devices ADD COLUMN kind TEXT NOT NULL DEFAULT 'ipad'
             CHECK (kind IN ('ipad','agent'))`);
    db.exec('ALTER TABLE pk_pack_devices ADD COLUMN printer_name TEXT');
    db.exec('ALTER TABLE pk_pack_devices ADD COLUMN heartbeat_at TEXT');
    db.exec('ALTER TABLE pk_pack_devices ADD COLUMN heartbeat_note TEXT');
    // 1つの再印刷につき印刷ジョブは1つ (UNIQUE)。「もう一度押した」で二重に紙が出ないようにする。
    //   queued → leased → dispatched → submitted → completed / failed
    //          ↘ manual (誰も取りに来ない)        ↘ unknown (報告が来ない)
    // 🚨 **dispatched (PDFを渡した) 時点で紙が出た可能性がある**ため、そこから先は期限切れでも
    //    自動で配り直さない。二重印刷より欠落を選び、人に知らせて実物を見てもらう (要件§6.1)。
    // 🚨 状態は安全の要なので CHECK でDB側にも書いておく (保守SQL・アプリのバグで未知の状態が
    //    入ると、監視の対象からも外れて「気づかないまま出ない」になる)
    db.exec(`CREATE TABLE IF NOT EXISTS pk_print_jobs (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      reprint_id       INTEGER NOT NULL UNIQUE REFERENCES pk_pack_reprints(id),
      pdf_token        TEXT NOT NULL,
      pdf_sha256       TEXT NOT NULL,       -- 配信時に実物と突合 (差し替わったPDFを刷らない)
      ne_slip_no       TEXT NOT NULL,
      folder_name      TEXT,
      printer_name     TEXT,                -- lease したデバイスに紐づく実際の出力先
      state            TEXT NOT NULL CHECK (state IN
                         ('queued','leased','dispatched','submitted','completed','failed','manual','unknown')),
      lease_device_id  INTEGER REFERENCES pk_pack_devices(id),
      lease_token      TEXT,                -- 報告時の照合 (期限切れ後の遅れた報告を弾く)
      lease_expires_at TEXT,
      attempt_count    INTEGER NOT NULL DEFAULT 0,
      spool_job_id     TEXT,
      error            TEXT,
      created_at       TEXT NOT NULL,
      updated_at       TEXT NOT NULL,
      submitted_at     TEXT,
      finished_at      TEXT,
      alerted_state    TEXT,                -- GChatに鳴らし終えた状態 (送信成功後にだけ入れる)
      -- lease を持っている状態では、誰がいつまで持っているかが必ず埋まっている
      CHECK (state NOT IN ('leased','dispatched')
             OR (lease_device_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL))
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_print_jobs_state ON pk_print_jobs(state, id)');
  },
  // v15: 出力先を**引当分類 (= 送り状発行ソフト) ごとに決める** (中原さん指示 2026-08-28)。
  //   引当分類によって送り状を出すソフトが違い (DENZOU / ヤマトB2 / ゆうプリR / 汎用送り状)、
  //   したがって物理プリンターも違う。v14 の「1エージェント=1プリンター固定」では足りない。
  //
  //   出荷フォルダには必ず `okurijo_<slug>_*.csv` があり、この slug が発行ソフトの振り分けキー
  //   (正本 = ロジザード作業自動化/hikiate-patterns.csv の「送り状発行ソフト」列)。
  //   slug → プリンター名 を人が管理画面で登録し、サーバがジョブごとに出力先を決める。
  //   🚨 対応表に無い slug は**自動印刷しない** (どこに出るか決まっていないものを刷らない)。
  15: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_print_routes (
      slug         TEXT PRIMARY KEY,     -- okurijo_<slug>_*.csv の slug (aes / nekoposu / 50size ...)
      printer_name TEXT NOT NULL,        -- 出力先。エージェントはこの名前にだけ出す
      note         TEXT,
      updated_by   TEXT NOT NULL,
      updated_at   TEXT NOT NULL
    )`);
    // 1台のPCに複数のプリンターがぶら下がる (出荷PC / 倉庫PC など)。
    // エージェントは**自分に登録されたプリンター宛のジョブしか受け取れない**
    // 🚨 printer_name は UNIQUE。Windowsのプリンター名は**PCごとのローカル名**なので、
    //    出荷PCと倉庫PCの両方に同じ「Brother QL-720」があると、どちらも同じジョブを取れて
    //    しまい「別の物理プリンターから黙って送り状が出る」= 一番避けたい事故になる。
    //    同名がある場合は登録時に弾き、どちらかを改名してもらう
    db.exec(`CREATE TABLE IF NOT EXISTS pk_print_agent_printers (
      device_id    INTEGER NOT NULL REFERENCES pk_pack_devices(id),
      printer_name TEXT NOT NULL UNIQUE,
      PRIMARY KEY (device_id, printer_name)
    )`);
    // v14 で 1台1プリンターとして登録済みの端末を引き継ぐ (登録し直しをさせない)。
    // 🚨 同名が複数端末にあったら OR IGNORE で片方を黙って捨てない —
    //    どちらの実機が正しいかは業務判断で、勝手に選ぶと誤ったPCから送り状が出る
    const dup = db.prepare(`SELECT printer_name, COUNT(*) c FROM pk_pack_devices
      WHERE kind='agent' AND revoked_at IS NULL AND printer_name IS NOT NULL AND printer_name <> ''
      GROUP BY printer_name HAVING c > 1`).all();
    if (dup.length > 0) {
      throw new Error(`同じプリンター名が複数の印刷エージェントに登録されています `
        + `(${dup.map((d) => d.printer_name).join(', ')})。`
        + 'どちらの実機か決められないため移行を中止しました。'
        + '⚠ この状態では packing の管理画面も開けません (packing だけが無効になり picking は動きます)。'
        + `data/picking.db に対して直接 `
        + `UPDATE pk_pack_devices SET revoked_at=datetime('now') WHERE id=<残さない方のID>; `
        + 'を実行してから再起動してください '
        + '(端末の一覧: SELECT id,label,printer_name FROM pk_pack_devices WHERE kind=\'agent\';)');
    }
    db.exec(`INSERT INTO pk_print_agent_printers (device_id, printer_name)
      SELECT id, printer_name FROM pk_pack_devices
      WHERE kind='agent' AND revoked_at IS NULL AND printer_name IS NOT NULL AND printer_name <> ''`);
    // ジョブは「どの分類の送り状か」を持つ (画面と監査のため。出力先の決定は enqueue 時に済む)
    db.exec('ALTER TABLE pk_print_jobs ADD COLUMN slug TEXT');
    // 🚨 出力先は「プリンター名」だけでなく「どの端末に出させるか」も焼き付ける。
    //    名前はPCごとのローカル名なので、名前の持ち主が付け替わると、積んであったジョブが
    //    別の物理プリンターから出てしまう (UNIQUE は同時点の重複しか防げない)
    db.exec('ALTER TABLE pk_print_jobs ADD COLUMN target_device_id INTEGER REFERENCES pk_pack_devices(id)');
    // v14 までに積まれた未処理ジョブは出力先が決まっていない (printer_name も slug も無い)。
    // 名前一致でしか lease されなくなるので、放置すると黙って滞留する → 人に回す
    db.exec(`UPDATE pk_print_jobs SET state='manual', finished_at=datetime('now'),
      updated_at=datetime('now'), error='出力先の決め方が変わったため手動印刷へ回しました'
      WHERE state IN ('queued','leased') AND (printer_name IS NULL OR printer_name = '')`);
  },
  // v16: 欠品フローv2 PR2 — ピッカーの「後で取りに行く」から展開された repick タスクに
  //   出自 (pk_later_requests.id) を持たせる。ピッカーが back で取り下げるとき、
  //   この依頼から生まれたタスクだけを正確に取消するため (SKU/伝票の一致では、梱包側が
  //   自分で出した再ピック依頼まで巻き込みかねない)
  16: () => {
    db.exec('ALTER TABLE pk_pack_tasks ADD COLUMN later_request_id INTEGER');
  },
  // v17: MELT 仕分けの「他の方法で出荷」の内訳に PAS-LINE へ移した件数を持つ (2026-08-31 現場意見:
  //   3つ折りで PAS へ移した分が PAS の機械カウンタに乗り、PAS 側の累計 (164+37=201) と
  //   カウンタ (202) が合わなかった)。to_pas_count ⊆ excluded_count。PAS の本日累計に加算する
  17: () => {
    db.exec('ALTER TABLE pk_pack_line_runs ADD COLUMN to_pas_count INTEGER');
  },
  // v18: 取りこぼしの見張り (2026-09-04 障害の再発防止)。
  //   ピッキングに来ているのに梱包へ来ていない出荷グループ / 引当分類が推定値のまま確定した
  //   バッチを見つけて GChat に鳴らす。**検知した時点で行を作る (outbox)** ので、webhook が
  //   落ちていても異常を見失わない (当日を跨いでも未送信行として残り、次に送れたときに鳴る)。
  //   未送信行は日付で切り捨てない (何日後でも送れたときに鳴る)。
  18: () => {
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_miss_alerts (
      alert_key   TEXT PRIMARY KEY,     -- <work_date>:<kind>:<pk_batch_id> (フォルダ名は使い回されるので使わない)
      kind        TEXT NOT NULL,          -- 'not_imported' | 'class_suggested'
      work_date   TEXT NOT NULL,
      pk_batch_id INTEGER,                -- pk_batches.id (調査用。FKは張らない = picking 側の作り直しに巻き込まれない)
      folder_name TEXT NOT NULL,          -- 表示用
      detail      TEXT,
      attempts    INTEGER NOT NULL DEFAULT 0,
      last_error  TEXT,
      notified_at TEXT,                   -- 送れたときだけ入る (送信前に印を付けない)
      created_at  TEXT NOT NULL
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_miss_alerts_pending ON pk_pack_miss_alerts(notified_at)');
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_miss_alerts_date ON pk_pack_miss_alerts(work_date)');
  },
  // v19: v18 のテーブル定義を後から直したので、**既に v18 を当てたDBにも列を足す**。
  //   マイグレーションは一度当たると再実行されない。定義だけ書き換えると、新しいDBには
  //   列があるのに v18 適用済みDBには無い、という食い違いが残る (Codexレビュー High)。
  //   新規DB (v18 で pk_batch_id 込みで作られる) では ALTER をスキップする
  19: () => {
    const cols = db.prepare('PRAGMA table_info(pk_pack_miss_alerts)').all().map((c) => c.name);
    if (!cols.includes('pk_batch_id')) db.exec('ALTER TABLE pk_pack_miss_alerts ADD COLUMN pk_batch_id INTEGER');
    db.exec('DROP INDEX IF EXISTS idx_pk_pack_miss_alerts_date');
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_miss_alerts_pending ON pk_pack_miss_alerts(notified_at)');
  },
  // v20: 3階「在庫なし」の扱い (例外処理監査 PR-1・Q1 決定 2026-09-05)。
  //   - pk_pack_tasks.close_reason: 'stockout' = 1階が「在庫なしを確認」して閉じた (status='cancelled' と組)。
  //     未確認の在庫なし (unavailable) と確認済みの終端を区別する (Codex R1 High)
  //   - fulfilled_qty / unavailable_qty: 部分確保 (5個中2個は他ロケで確保・3個は在庫なし) の内訳
  //   - pk_pack_stockouts: 「在庫なしを確認」の事務通知の outbox。伝票を閉じるのと同じトランザクションで行を作り、
  //     送れたときだけ notified_at。未送信はポーラーが再送する (配送変更④と同型。通知はチャネル・行が正本)
  20: () => {
    // 列は存在確認してから足す (v19 と同じ作法 — 部分適用済み DB や、テストで古い版を手作りした DB でも落ちない)
    const hasTable = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pk_pack_tasks'").get();
    if (hasTable) {
      const cols = db.prepare('PRAGMA table_info(pk_pack_tasks)').all().map((c) => c.name);
      if (!cols.includes('close_reason')) db.exec('ALTER TABLE pk_pack_tasks ADD COLUMN close_reason TEXT');
      if (!cols.includes('fulfilled_qty')) db.exec('ALTER TABLE pk_pack_tasks ADD COLUMN fulfilled_qty INTEGER');
      if (!cols.includes('unavailable_qty')) db.exec('ALTER TABLE pk_pack_tasks ADD COLUMN unavailable_qty INTEGER');
    }
    db.exec(`CREATE TABLE IF NOT EXISTS pk_pack_stockouts (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id       INTEGER NOT NULL REFERENCES pk_pack_batches(id),
      slip_seq       INTEGER NOT NULL,
      ne_slip_no     TEXT NOT NULL,
      site_order_no  TEXT,
      recipient_name TEXT,
      folder_name    TEXT,
      items_json     TEXT NOT NULL,        -- [{sku, name, qty, claimedBy, at}]
      worker         TEXT NOT NULL,        -- 確認した梱包者
      notified_at    TEXT,
      notify_error   TEXT,
      claimed_at     TEXT,                 -- 送信中の印 (router とポーラーが同じ行を同時に送らない。10分で失効)
      created_at     TEXT NOT NULL
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS idx_pk_pack_stockouts_pending ON pk_pack_stockouts(notified_at)');
  },
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
    to_pas_count     INTEGER,               -- v17: 除外のうち PAS-LINE へ移した件数 (MELT 仕分けのみ)
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

/** バッチ一覧: 指定作業日のみ (過去分は日付ピッカーで参照。中原さん指示 2026-08-18 で持ち越し表示を廃止)。 */
export function listPackBatches(workDate) {
  return getDB().prepare(`
    SELECT b.*,
      (SELECT COUNT(*) FROM pk_pack_slips s WHERE s.batch_id = b.id AND s.status = 'done') AS done_slips
    FROM pk_pack_batches b
    WHERE b.work_date = ?
    ORDER BY b.id
  `).all(workDate);
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

// ─── 登録端末 (v3。picking pk_devices と同設計・packing 所有) ───

import crypto from 'node:crypto';
const hashToken = (t) => crypto.createHash('sha256').update(String(t)).digest('hex');
const DEVICE_TTL_MS = 400 * 24 * 3600 * 1000;

/**
 * 端末を登録し、平文トークンを返す (保存はハッシュのみ。トークンはこの1回しか得られない)。
 * kind='agent' は出荷PCの印刷エージェント — 出力先プリンターを**サーバ側で**この端末に紐づける
 * (エージェント側の設定ミスで別のプリンターに送り状を出さないため)。
 */
export function createDevice(label, actor, { kind = 'ipad', printerName = null } = {}) {
  const token = crypto.randomBytes(32).toString('base64url');
  const info = getDB().prepare(`
    INSERT INTO pk_pack_devices (token_hash, label, created_by, created_at, kind, printer_name)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(hashToken(token), String(label).trim(), actor, utcNow(), kind,
    printerName ? String(printerName).trim() : null);
  return { token, id: Number(info.lastInsertRowid) };
}

/** トークン検証。有効なら端末行を返し last_seen_at を更新 (1時間に1回程度に間引く)。 */
export function verifyDevice(token) {
  if (!token) return null;
  const db = getDB();
  const row = db.prepare(
    'SELECT * FROM pk_pack_devices WHERE token_hash = ? AND revoked_at IS NULL'
  ).get(hashToken(token));
  if (!row) return null;
  const now = utcNow();
  if (Date.parse(now) - Date.parse(row.created_at) > DEVICE_TTL_MS) return null;
  if (!row.last_seen_at || Date.parse(now) - Date.parse(row.last_seen_at) > 3600_000) {
    db.prepare('UPDATE pk_pack_devices SET last_seen_at = ? WHERE id = ?').run(now, row.id);
  }
  return row;
}

export function revokeDevice(id) {
  const db = getDB();
  return db.transaction(() => {
    const changed = db.prepare(
      'UPDATE pk_pack_devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL'
    ).run(utcNow(), id).changes > 0;
    // 失効した端末がプリンター名を握ったままだと、代わりのPCを同じ名前で登録できない。
    // 名前は「いま印刷できる端末」だけが持つ
    if (changed) db.prepare('DELETE FROM pk_print_agent_printers WHERE device_id = ?').run(id);
    return changed;
  }).immediate();
}

export function listDevices() {
  const rows = getDB().prepare(`SELECT id, label, created_by, created_at, last_seen_at, revoked_at,
    kind, printer_name, heartbeat_at, heartbeat_note FROM pk_pack_devices ORDER BY id`).all();
  const printers = getDB().prepare(
    'SELECT device_id, printer_name FROM pk_print_agent_printers ORDER BY printer_name').all();
  for (const r of rows) {
    r.printers = printers.filter((p) => p.device_id === r.id).map((p) => p.printer_name);
  }
  return rows;
}

/** エージェント端末に「このPCから出せるプリンター」を登録する (全置換)。 */
export function setAgentPrinters(deviceId, printerNames) {
  const db = getDB();
  const raw = (printerNames || []).map((n) => String(n || '').trim());
  // 不正な項目を黙って捨てると「登録したつもりが入っていない」= 印刷されないのに気づけない
  if (raw.some((n) => !n || n.length > 120)) {
    return { ok: false, reason: 'bad_printer', message: 'プリンター名は1〜120文字で入力してください' };
  }
  const names = [...new Set(raw)];
  // 🚨 検査と書き込みを1つのトランザクションにまとめる。分けると、同じ名前を同時に
  //    登録した2人が両方とも事前検査を通り、片方が UNIQUE 例外で落ちる。
  //    そのとき新規登録側は「端末だけできてプリンターが空」の状態を残してしまう
  try {
    return db.transaction(() => {
      const dev = db.prepare(
        "SELECT id FROM pk_pack_devices WHERE id=? AND kind='agent' AND revoked_at IS NULL").get(deviceId);
      if (!dev) return { ok: false, reason: 'not_agent', message: '有効な印刷エージェント端末ではありません' };
      // 同名プリンターが別の**有効な**端末に登録済み = どちらの物理プリンターか決められない
      const taken = names.filter((n) => db.prepare(`SELECT p.device_id FROM pk_print_agent_printers p
        JOIN pk_pack_devices d ON d.id = p.device_id AND d.revoked_at IS NULL
        WHERE p.printer_name=? AND p.device_id<>?`).get(n, deviceId));
      if (taken.length > 0) {
        return {
          ok: false,
          reason: 'duplicate_printer',
          message: `「${taken.join('」「')}」は別の端末に登録済みです。`
            + 'プリンター名はPCごとのローカル名なので、同じ名前だとどちらの実機か決められません '
            + '(どちらかのPCで名前を変えてください)',
        };
      }
      // 手放そうとしている名前に未処理のジョブが残っていたら拒否する。
      // その名前を別のPCが引き取ると、積んであったジョブが別の実機から出てしまう
      const dropping = db.prepare('SELECT printer_name FROM pk_print_agent_printers WHERE device_id=?')
        .all(deviceId).map((r) => r.printer_name).filter((n) => !names.includes(n));
      const busy = dropping.filter((n) => db.prepare(
        "SELECT 1 FROM pk_print_jobs WHERE printer_name=? AND state IN ('queued','leased','dispatched','submitted')")
        .get(n));
      if (busy.length > 0) {
        return {
          ok: false,
          reason: 'printer_busy',
          message: `「${busy.join('」「')}」にはまだ印刷待ちの送り状があります。`
            + '先にそれが片づく (または手動印刷へ回る) のを待ってから外してください',
        };
      }
      db.prepare('DELETE FROM pk_print_agent_printers WHERE device_id=?').run(deviceId);
      const ins = db.prepare('INSERT INTO pk_print_agent_printers (device_id, printer_name) VALUES (?,?)');
      for (const n of names) ins.run(deviceId, n);
      // 画面の互換表示用に代表1つを残す (出力先の決定には使わない)
      db.prepare('UPDATE pk_pack_devices SET printer_name=? WHERE id=?').run(names[0] ?? null, deviceId);
      return { ok: true, printers: names };
    }).immediate();
  } catch (e) {
    // 同時実行で UNIQUE に当たった場合も、例外ではなく同じ「重複」として返す
    if (String(e.code || '').includes('SQLITE_CONSTRAINT')) {
      return { ok: false, reason: 'duplicate_printer', message: 'そのプリンター名は別の端末に登録されています' };
    }
    throw e;
  }
}

/** その端末から出せるプリンター名の一覧。 */
export function agentPrintersOf(deviceId) {
  return getDB().prepare(
    'SELECT printer_name FROM pk_print_agent_printers WHERE device_id=? ORDER BY printer_name')
    .all(deviceId).map((r) => r.printer_name);
}
