/**
 * いろは在庫化作業アプリ (iPad) — DB 層
 *
 * 正本の考え方 (要件定義 v1.2 §1.5 / §1.7):
 *   進捗ステータスの正本 = Notion「在庫化作業管理」(当面)。ここに持つのはそのキャッシュと、
 *   アプリ固有のインフラ (端末・作業者・操作履歴) だけ。
 *
 * ⭐作業者名簿 (f_iroha_workers) は staff.db (apps/staff) と分ける。
 *   「人の正本は staff.db に1つ」(2026-09-01) は B-Faith の雇用スタッフの話 —
 *   いろはの利用者は就労支援B型の利用者で雇用スタッフではなく、名簿の性質が違う
 *   (プライバシー配慮・表示名運用。Codex設計相談R1 Q7)
 *
 * 接続は warehouse-mirror の getMirrorDB() を共有 (inbound-check と同じ。台帳・作業仕様
 * マスタ・販売/在庫ミラーと同じファイルにあるので JOIN も参照もそのまま効く)。
 */
import crypto from 'crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { FACILITIES } from './tasks.js';

const utcNow = () => new Date().toISOString();

// 作業のやり方の選択肢 (資材セット・保管箱) の DDL。作成と作り直し (下) で同じ定義を使う
const workOptionsDDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      kind            TEXT NOT NULL CHECK (kind IN ('material','container')),
      code            TEXT NOT NULL,
      normalized_code TEXT NOT NULL,
      image_url       TEXT,
      sort_order      INTEGER NOT NULL DEFAULT 0,
      active          INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      created_at      TEXT NOT NULL,
      created_by      TEXT,
      UNIQUE(kind, normalized_code)
    );`;

// 在庫化タスク / ラベル待ちの DDL (作成と作り直しで共用。列一覧は INSERT … SELECT にも使う)
const TASKS_COLS = ['id', 'destination_id', 'notion_page_id', 'legacy_status', 'status', 'close_reason', 'facility_code', 'hold_reason_code', 'hold_reason_note',
  'planned_date', 'priority_class', 'priority_note', 'product_code', 'product_name', 'qty', 'arrival_date', 'ar_no', 'barcode', 'expiry', 'supplier', 'handling',
  'master_snapshot', 'payload', 'started_at', 'ready_at', 'closed_at', 'closed_by', 'cancellation_requested_at', 'cancellation_source',
  'migration_review', 'migration_note', 'import_batch_id', 'external_ready', 'version', 'created_at', 'created_by', 'updated_at', 'updated_by'];
const tasksDDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      destination_id   INTEGER,
      notion_page_id   TEXT,
      legacy_status    TEXT,
      status           TEXT NOT NULL CHECK (status IN ('not_started','in_progress','on_hold','ready_for_stocking','closed')),
      close_reason     TEXT CHECK (close_reason IS NULL OR close_reason IN ('stocked','cancelled','out_of_scope')),
      facility_code    TEXT NOT NULL DEFAULT 'iroha' REFERENCES f_iroha_facilities(code),
      hold_reason_code TEXT CHECK (hold_reason_code IS NULL OR hold_reason_code IN ('materials_shortage','label_shortage','awaiting_instruction','other')),
      hold_reason_note TEXT,
      planned_date     TEXT,
      external_ready   INTEGER NOT NULL DEFAULT 0 CHECK (external_ready IN (0,1)),
      priority_class   TEXT,
      priority_note    TEXT,
      product_code     TEXT,
      product_name     TEXT,
      qty              INTEGER,
      arrival_date     TEXT,
      ar_no            TEXT,
      barcode          TEXT,
      expiry           TEXT,
      supplier         TEXT,
      handling         TEXT,
      master_snapshot  TEXT,
      payload          TEXT,
      started_at       TEXT,
      ready_at         TEXT,
      closed_at        TEXT,
      closed_by        TEXT,
      cancellation_requested_at TEXT,
      cancellation_source       TEXT,
      migration_review INTEGER NOT NULL DEFAULT 0 CHECK (migration_review IN (0,1)),
      migration_note   TEXT,
      import_batch_id  TEXT,
      version          INTEGER NOT NULL DEFAULT 1,
      created_at       TEXT NOT NULL,
      created_by       TEXT,
      updated_at       TEXT NOT NULL,
      updated_by       TEXT,
      -- 状態の不変条件は DB でも守る (サービス層 validateTaskInvariants と同じ規則。一経路の検証漏れで壊れない — Codex A1 R1 #7)
      CHECK ((status = 'closed') = (close_reason IS NOT NULL)),
      CHECK ((status = 'closed') = (closed_at IS NOT NULL)),
      CHECK ((status = 'on_hold') = (hold_reason_code IS NOT NULL)),
      CHECK (hold_reason_code IS NULL OR hold_reason_code <> 'other' OR (hold_reason_note IS NOT NULL AND TRIM(hold_reason_note) <> ''))
    );`;
const TASKS_INDEX_DDL = `
    CREATE UNIQUE INDEX IF NOT EXISTS idx_iroha_tasks_destination ON f_iroha_tasks(destination_id) WHERE destination_id IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_iroha_tasks_notion ON f_iroha_tasks(notion_page_id) WHERE notion_page_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_iroha_tasks_status ON f_iroha_tasks(status, facility_code);
    CREATE INDEX IF NOT EXISTS idx_iroha_tasks_code ON f_iroha_tasks(product_code);`;
const LABEL_COLS = ['id', 'task_id', 'occurred_on', 'recorded_by_worker_id', 'recorded_by_name', 'label_ordered', 'lot_expiry', 'qty', 'location', 'reattach',
  'line_notified_on', 're_notified_on', 'restocked_on', 'done', 'note', 'version', 'created_at', 'updated_at'];
const labelWaitsDDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id                    INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id               INTEGER NOT NULL REFERENCES f_iroha_tasks(id),
      occurred_on           TEXT,
      recorded_by_worker_id INTEGER,
      recorded_by_name      TEXT,
      label_ordered         INTEGER NOT NULL DEFAULT 0 CHECK (label_ordered IN (0,1)),
      lot_expiry            TEXT,
      qty                   INTEGER,
      location              TEXT CHECK (location IS NULL OR location IN ('Z','Y','none')),
      reattach              INTEGER NOT NULL DEFAULT 0 CHECK (reattach IN (0,1)),
      line_notified_on      TEXT,
      re_notified_on        TEXT,
      restocked_on          TEXT,
      done                  INTEGER NOT NULL DEFAULT 0 CHECK (done IN (0,1)),
      note                  TEXT,
      version               INTEGER NOT NULL DEFAULT 1,
      created_at            TEXT NOT NULL,
      updated_at            TEXT NOT NULL
    );`;
const LABEL_INDEX_DDL = 'CREATE INDEX IF NOT EXISTS idx_iroha_label_waits_task ON f_iroha_label_waits(task_id, id);';

// 作業時間セッション / 完成写真。⭐page_id は「Notion 時代の証跡」なので NULL 可 (アプリ正本のカードは task_id で紐づく)。
// どちらも無い行は作れない (CHECK)。古い版 (page_id NOT NULL) は migrateSessionMediaSchema で作り直す
const sessionsDDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      page_id        TEXT,
      task_id        INTEGER REFERENCES f_iroha_tasks(id),
      product_code   TEXT,
      title_snapshot TEXT,
      worker_id      INTEGER NOT NULL,
      worker_name    TEXT NOT NULL,
      device_label   TEXT,
      started_at     TEXT NOT NULL,
      ended_at       TEXT,
      end_reason     TEXT CHECK (end_reason IS NULL OR end_reason IN ('done','pause','admin')),
      raw_seconds    INTEGER,
      voided_at      TEXT,
      voided_by      TEXT,
      void_reason    TEXT,
      master_snapshot TEXT,
      CHECK (page_id IS NOT NULL OR task_id IS NOT NULL)
    );`;
const SESSIONS_INDEX_DDL = `
    CREATE INDEX IF NOT EXISTS idx_iroha_sessions_page ON f_iroha_work_sessions(page_id, id);
    CREATE INDEX IF NOT EXISTS idx_iroha_sessions_task ON f_iroha_work_sessions(task_id, id);`;
const mediaDDL = (name) => `
    CREATE TABLE IF NOT EXISTS ${name} (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      operation_id  TEXT NOT NULL UNIQUE,
      page_id       TEXT,
      task_id       INTEGER REFERENCES f_iroha_tasks(id),
      product_code  TEXT,
      kind          TEXT NOT NULL CHECK (kind IN ('photo','video')),
      mime          TEXT,
      size          INTEGER,
      local_path    TEXT,
      drive_file_id TEXT,
      drive_url     TEXT,
      status        TEXT NOT NULL CHECK (status IN ('stored','uploaded','synced')),
      error         TEXT,
      attempt_count INTEGER NOT NULL DEFAULT 0,
      next_retry_at TEXT,
      worker_id     INTEGER,
      worker_name   TEXT,
      device_label  TEXT,
      created_at    TEXT NOT NULL,
      uploaded_at   TEXT,
      synced_at     TEXT,
      deleted_at    TEXT,
      deleted_by    TEXT,
      delete_token_hash  TEXT,
      uploader_device_id INTEGER,
      unavailable_at TEXT,
      CHECK (page_id IS NOT NULL OR task_id IS NOT NULL)
    );`;
const MEDIA_INDEX_DDL = `
    CREATE INDEX IF NOT EXISTS idx_iroha_media_page ON f_iroha_card_media(page_id, id);
    CREATE INDEX IF NOT EXISTS idx_iroha_media_task ON f_iroha_card_media(task_id, id);`;

/**
 * 作業時間・写真の作り直し (アプリ正本のカードは Notion ページを持たないため page_id を NULL 可にし、task_id + FK + CHECK を付ける)。
 * 判定は「新しい定義に必要なもの」を個別に見る (page_id が NULL 可か / task_id 列と FK / 両方 NULL 禁止の CHECK / 新 DDL の全列) —
 * 一部だけ足りない途中の版も作り直す (Codex A1b R1 #5)。既存行はそのまま移す (page_id は残る = Notion 時代の証跡)。
 * 列は「新 DDL と旧テーブルの共通列」だけコピーするので、列が少ない版からでも通る。
 * 移す前後で件数が一致すること・DB 全体の FK 検査 (作り直した表を参照する側も含む — 同 R1 #4) を通ることを確かめ、
 * 通らなければ全部戻す。冪等
 */
// 表ごとに「新しい定義に必要なもの」(列の有無だけでなく UNIQUE・CHECK も。欠けた途中版を見逃さない — Codex A1b R2 #2)
const SESSION_MEDIA_COMMON_DDL = [/task_id\s+INTEGER REFERENCES f_iroha_tasks\(id\)/, /CHECK \(page_id IS NOT NULL OR task_id IS NOT NULL\)/];
const SESSIONS_REQUIRED_DDL = [...SESSION_MEDIA_COMMON_DDL, /end_reason IS NULL OR end_reason IN \('done','pause','admin'\)/];
const MEDIA_REQUIRED_DDL = [...SESSION_MEDIA_COMMON_DDL, /operation_id\s+TEXT NOT NULL UNIQUE/, /kind IN \('photo','video'\)/, /status IN \('stored','uploaded','synced'\)/];
function sessionMediaNeedsRebuild(db, table, ddl, required) {
  const sql = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?").get(table)?.sql;
  if (!sql) return false;   // 無ければ CREATE IF NOT EXISTS が新定義で作る
  const info = db.prepare(`PRAGMA table_info(${table})`).all();
  if (info.some((c) => c.name === 'page_id' && c.notnull === 1)) return true;
  if (required.some((re) => !re.test(sql))) return true;
  const have = new Set(info.map((c) => c.name));
  const want = [...ddl('x').matchAll(/^\s+([a-z_]+)\s+(?:INTEGER|TEXT)/gm)].map((m) => m[1]);
  return want.some((c) => !have.has(c));
}
function migrateSessionMediaSchema(db) {
  const targets = [
    { table: 'f_iroha_work_sessions', ddl: sessionsDDL, index: SESSIONS_INDEX_DDL, required: SESSIONS_REQUIRED_DDL },
    { table: 'f_iroha_card_media', ddl: mediaDDL, index: MEDIA_INDEX_DDL, required: MEDIA_REQUIRED_DDL },
  ].filter((t) => sessionMediaNeedsRebuild(db, t.table, t.ddl, t.required));
  if (targets.length === 0) return false;
  const fkWasOn = db.pragma('foreign_keys', { simple: true }) === 1;
  if (fkWasOn) db.pragma('foreign_keys = OFF');
  try {
    db.transaction(() => {
      for (const { table, ddl, index } of targets) {
        const tmp = `${table}__new`;
        db.exec(`DROP TABLE IF EXISTS ${tmp}`);   // 中断で残った作業表があれば捨てる (行は元の表にある)
        db.exec(ddl(tmp));
        const newCols = db.prepare(`PRAGMA table_info(${tmp})`).all().map((c) => c.name);
        const oldCols = new Set(db.prepare(`PRAGMA table_info(${table})`).all().map((c) => c.name));
        const cols = newCols.filter((c) => oldCols.has(c));
        const before = db.prepare(`SELECT COUNT(*) c FROM ${table}`).get().c;
        db.exec(`INSERT INTO ${tmp} (${cols.join(', ')}) SELECT ${cols.join(', ')} FROM ${table}`);
        const after = db.prepare(`SELECT COUNT(*) c FROM ${tmp}`).get().c;
        if (before !== after) throw new Error(`${table} の作り直しを中止しました (件数不一致 ${before} → ${after})`);
        db.exec(`DROP TABLE ${table}`);
        db.exec(`ALTER TABLE ${tmp} RENAME TO ${table}`);
        db.exec(index);
      }
      const bad = db.pragma('foreign_key_check');
      if (bad.length > 0) throw new Error(`作業時間・写真の作り直しを中止しました (FK 違反): ${JSON.stringify(bad.slice(0, 5))}`);
    })();
  } finally {
    if (fkWasOn) db.pragma('foreign_keys = ON');
  }
  console.log(`[iroha-work] ${targets.map((t) => t.table).join(' / ')} を新しい定義で作り直しました (page_id NULL 可・task_id FK・CHECK)`);
  return true;
}

// 「新しい定義」に必要な制約 (個別に検査 — 一部だけ足りない版も作り直す。Codex A1 R3 Low)
const TASKS_REQUIRED_DDL = [
  /\(status = 'closed'\) = \(close_reason IS NOT NULL\)/, /\(status = 'closed'\) = \(closed_at IS NOT NULL\)/,
  /\(status = 'on_hold'\) = \(hold_reason_code IS NOT NULL\)/, /hold_reason_code <> 'other'/, /REFERENCES f_iroha_facilities/,
];
const LABEL_REQUIRED_DDL = [/REFERENCES f_iroha_tasks/, /label_ordered IN \(0,1\)/, /location IN \('Z','Y','none'\)/, /reattach IN \(0,1\)/, /done IN \(0,1\)/];
const FK_CHECK_TABLES = ['f_iroha_tasks', 'f_iroha_label_waits', 'f_iroha_work_sessions', 'f_iroha_card_media', 'f_iroha_app_events'];

/**
 * f_iroha_tasks / f_iroha_label_waits の作り直し: CHECK・FK の無い (または一部足りない) 古い版が残っていたら、行をそのまま
 * 移して新しい定義に入れ替える (CREATE IF NOT EXISTS は制約を足せない — Codex A1 R2 #1)。判定は sqlite_master の DDL 文字列。
 * 子テーブル (sessions 等) が参照していても親を作り直せるよう、その間だけ foreign_keys を OFF にする。
 * 制約を入れる前提として、同じトランザクションで孤立参照を補正 (task の無いラベル待ちは __orphan へ退避、子テーブルの
 * 宙ぶらりんな task_id は NULL に戻す = page_id は残るので次のバックフィルで埋め直せる) してから FK を検査し、
 * 違反が残れば全部戻す (Codex A1 R3 Medium)。冪等
 */
function migrateTasksSchema(db) {
  const sqlOf = (name) => db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?").get(name)?.sql || '';
  const lacks = (sql, required) => !!sql && required.some((re) => !re.test(sql));
  const needTasks = lacks(sqlOf('f_iroha_tasks'), TASKS_REQUIRED_DDL);
  const needLabel = lacks(sqlOf('f_iroha_label_waits'), LABEL_REQUIRED_DDL);
  if (!needTasks && !needLabel) return false;
  const fkWasOn = db.pragma('foreign_keys', { simple: true }) === 1;
  if (fkWasOn) db.pragma('foreign_keys = OFF');
  const fixed = { orphanLabelWaits: 0, unlinked: {} };
  try {
    db.transaction(() => {
      if (needTasks) {
        db.exec(tasksDDL('f_iroha_tasks__new'));
        // 旧テーブルに無い列は写さない (既定値のまま)。NOT NULL の列に NULL が入っていたら既定値に寄せる
        const have = new Set(db.prepare('PRAGMA table_info(f_iroha_tasks)').all().map((c) => c.name));
        const cols = TASKS_COLS.filter((c) => have.has(c));
        const pick = (c) => (c === 'external_ready' || c === 'migration_review' ? `COALESCE(${c}, 0)` : c);
        db.exec(`INSERT INTO f_iroha_tasks__new (${cols.join(', ')}) SELECT ${cols.map(pick).join(', ')} FROM f_iroha_tasks`);
        db.exec('DROP TABLE f_iroha_tasks');
        db.exec('ALTER TABLE f_iroha_tasks__new RENAME TO f_iroha_tasks');
        db.exec(TASKS_INDEX_DDL);
      }
      if (needLabel) {
        db.exec(labelWaitsDDL('f_iroha_label_waits__new'));
        const haveL = new Set(db.prepare('PRAGMA table_info(f_iroha_label_waits)').all().map((c) => c.name));
        const colsL = LABEL_COLS.filter((c) => haveL.has(c));
        db.exec(`INSERT INTO f_iroha_label_waits__new (${colsL.join(', ')}) SELECT ${colsL.join(', ')} FROM f_iroha_label_waits`);
        db.exec('DROP TABLE f_iroha_label_waits');
        db.exec('ALTER TABLE f_iroha_label_waits__new RENAME TO f_iroha_label_waits');
        db.exec(LABEL_INDEX_DDL);
      }
      // ① task の無いラベル待ちは消さずに退避
      db.exec('CREATE TABLE IF NOT EXISTS f_iroha_label_waits__orphan AS SELECT * FROM f_iroha_label_waits WHERE 0');
      const orphanIds = db.prepare('SELECT id FROM f_iroha_label_waits w WHERE NOT EXISTS (SELECT 1 FROM f_iroha_tasks t WHERE t.id = w.task_id)').all().map((r) => r.id);
      if (orphanIds.length > 0) {
        const list = orphanIds.join(',');
        db.exec(`INSERT INTO f_iroha_label_waits__orphan SELECT * FROM f_iroha_label_waits WHERE id IN (${list}); DELETE FROM f_iroha_label_waits WHERE id IN (${list});`);
        fixed.orphanLabelWaits = orphanIds.length;
      }
      // ② 子テーブルの task_id が存在しない task を指していれば外す (page_id は残る)
      for (const t of ['f_iroha_work_sessions', 'f_iroha_card_media', 'f_iroha_app_events']) {
        if (!db.prepare(`PRAGMA table_info(${t})`).all().some((c) => c.name === 'task_id')) continue;
        fixed.unlinked[t] = db.prepare(`UPDATE ${t} SET task_id = NULL WHERE task_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM f_iroha_tasks x WHERE x.id = ${t}.task_id)`).run().changes;
      }
      // ③ 検査。補正できない違反 (拠点コードの誤り等) が残れば throw → トランザクションごと戻る
      const bad = FK_CHECK_TABLES.flatMap((t) => db.pragma(`foreign_key_check(${t})`));
      if (bad.length > 0) throw new Error(`タスク表の作り直しを中止しました (補正できない FK 違反): ${JSON.stringify(bad.slice(0, 5))}`);
    })();
  } finally {
    if (fkWasOn) db.pragma('foreign_keys = ON');
  }
  console.log(`[iroha-work] ${[needTasks && 'f_iroha_tasks', needLabel && 'f_iroha_label_waits'].filter(Boolean).join(' / ')} を CHECK・FK 付きに作り直しました`
    + (fixed.orphanLabelWaits ? ` (孤立ラベル待ち ${fixed.orphanLabelWaits} 件を __orphan へ退避)` : '')
    + (Object.values(fixed.unlinked).some(Boolean) ? ` (宙ぶらりんの task_id を外した: ${JSON.stringify(fixed.unlinked)})` : ''));
  return true;
}

/**
 * f_iroha_work_options の作り直し: normalized_code が無い古い版 (UNIQUE(kind, code)) が残っていたら、
 * 同じ正規化規則で行を統合して新しい定義に入れ替える (CREATE IF NOT EXISTS は列を増やさない — Codex 選択肢 R2 #1)。
 * 統合規則: 表記は半角のものを優先 / 1つでも有効なら有効 / 画像は最初に見つかったもの / sort_order は最小 (=使用回数最大) /
 * created_at は最古。冪等 (2回目は何もしない)
 */
function migrateWorkOptionsSchema(db) {
  const cols = db.prepare('PRAGMA table_info(f_iroha_work_options)').all().map((c) => c.name);
  if (cols.length === 0 || cols.includes('normalized_code')) return false;
  const rows = db.prepare('SELECT * FROM f_iroha_work_options ORDER BY id').all();
  const merged = new Map();
  for (const r of rows) {
    const code = String(r.code || '').replace(/\s+/g, ' ').trim();
    const norm = normalizeOptionCode(code);
    if (!norm) continue;
    const key = `${r.kind}|${norm}`;
    const m = merged.get(key);
    if (!m) {
      merged.set(key, { kind: r.kind, code, norm, canonical: code.normalize('NFKC') === code, image_url: r.image_url || null,
        sort_order: r.sort_order ?? 0, active: r.active ? 1 : 0, created_at: r.created_at || utcNow(), created_by: r.created_by || null });
      continue;
    }
    if (!m.canonical && code.normalize('NFKC') === code) { m.code = code; m.canonical = true; }
    m.image_url = m.image_url || r.image_url || null;
    m.sort_order = Math.min(m.sort_order, r.sort_order ?? 0);
    m.active = m.active || (r.active ? 1 : 0);
    if (r.created_at && r.created_at < m.created_at) m.created_at = r.created_at;
  }
  db.transaction(() => {
    db.exec(workOptionsDDL('f_iroha_work_options__new'));
    const ins = db.prepare(`INSERT INTO f_iroha_work_options__new (kind, code, normalized_code, image_url, sort_order, active, created_at, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const m of merged.values()) ins.run(m.kind, m.code, m.norm, m.image_url, m.sort_order, m.active, m.created_at, m.created_by);
    db.exec('DROP TABLE f_iroha_work_options');
    db.exec('ALTER TABLE f_iroha_work_options__new RENAME TO f_iroha_work_options');
  })();
  console.log(`[iroha-work] f_iroha_work_options を normalized_code 付きに作り直しました (${rows.length}行 → ${merged.size}行)`);
  return true;
}
const hashToken = (t) => crypto.createHash('sha256').update(String(t)).digest('hex');
const enrollHash = (c) => crypto.createHash('sha256').update('iroha-enroll:' + String(c)).digest('hex');

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
    -- Notion カードのキャッシュ (正本は Notion。表示と絞り込みのためだけに持つ)。
    -- payload = パース済みプロパティの JSON (列を増やさず項目追加に耐える)
    CREATE TABLE IF NOT EXISTS f_iroha_app_notion_cache (
      page_id          TEXT PRIMARY KEY,
      status           TEXT,
      title            TEXT,
      product_code     TEXT,
      dedupe_key       TEXT,
      url              TEXT,
      last_edited_time TEXT,
      payload          TEXT,
      fetched_at       TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_iroha_cache_status ON f_iroha_app_notion_cache(status);

    -- 同期状態などのメタ (last_refresh_at / last_refresh_error / truncated)
    CREATE TABLE IF NOT EXISTS f_iroha_app_meta (
      key   TEXT PRIMARY KEY,
      value TEXT
    );

    -- 作業のやり方の選択肢 (資材セット・保管箱)。中原さん 2026-09-03: 編集はテキスト入力でなく、
    -- Excel (作業仕様マスタ) にある値を初期値の選択肢にしてタップで選ぶ。初見のものはその場で追加 (職員PIN)。
    -- 画像は後から付ける (image_url)。code = 表示名 兼 f_iroha_work_master に入る値そのもの
    -- normalized_code = 比較用 (NFKC・空白統一・大文字化)。表記揺れ (D-8 / d-8 / Ｄ－８) を別候補にしない (Codex R1 #3)。
    -- 古い版 (normalized_code 無し) からの作り直しは migrateWorkOptionsSchema (下)
    ${workOptionsDDL('f_iroha_work_options')}

    -- いろは名簿 (利用者/職員)。⭐staff.db とは別 (冒頭コメント参照)。
    -- pin_hash/pin_salt = 職員PIN (棚入完了の変更などの職員限定操作の本人確認。Codex PR1 #1:
    -- worker_id は画面で自由に選べる自己申告なので、それだけで職員権限にしない)
    CREATE TABLE IF NOT EXISTS f_iroha_workers (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      display_name TEXT NOT NULL,
      worker_type  TEXT NOT NULL CHECK (worker_type IN ('member','staff')),
      active       INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      sort_order   INTEGER NOT NULL DEFAULT 0,
      pin_hash     TEXT,
      pin_salt     TEXT,
      pin_fails    INTEGER NOT NULL DEFAULT 0,
      pin_lock_until TEXT,
      created_at   TEXT NOT NULL,
      created_by   TEXT
    );

    -- ─── アプリ正本化 (要件定義 v1.1・2026-09-03。状態モデルは tasks.js) ───
    -- 拠点 (いろは + 外部施設)。初期値は tasks.js の FACILITIES (seedFacilities)
    CREATE TABLE IF NOT EXISTS f_iroha_facilities (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      code       TEXT NOT NULL UNIQUE,
      name       TEXT NOT NULL,
      external   INTEGER NOT NULL DEFAULT 0 CHECK (external IN (0,1)),
      active     INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      sort_order INTEGER NOT NULL DEFAULT 0
    );

    -- 在庫化タスク (= 入荷明細 1 件)。v1.1 でアプリの正本になる。
    --   destination_id = 入荷受付台帳 (f_inbound_check_destinations.id)。Notion からの初期取込分は notion_page_id で冪等
    --   表示用の商品情報はカード作成時の値、作業仕様は master_snapshot (作成時の JSON — 後でマスタが変わっても指示は変えない)
    --   終了 (closed) も削除せず残す (作業時間・写真の履歴)。一覧・カンバンは OPEN_STATUSES だけ
    --   migration_review = 取込時に状態を推定した行 (施設名ステータス等)。職員が確認して 0 にする
    --   (DDL は tasksDDL — 古い版 (CHECK/FK 無し) が残っていれば migrateTasksSchema で作り直す)
    ${tasksDDL('f_iroha_tasks')}
    ${TASKS_INDEX_DDL}

    -- ラベル待ち (『ラベル待ち管理.xlsx』の DB 化。要件 v1.1 §C)。保留理由 label_shortage に付随する追跡
    ${labelWaitsDDL('f_iroha_label_waits')}
    ${LABEL_INDEX_DDL}

    -- 操作履歴 (append-only。Codex R2「操作履歴」強く推奨)。
    -- ステータス変更など Notion への書き込みは成功・失敗ともここに残す
    CREATE TABLE IF NOT EXISTS f_iroha_app_events (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      at           TEXT NOT NULL,
      action       TEXT NOT NULL,
      page_id      TEXT,
      worker_id    INTEGER,
      worker_name  TEXT,
      device_label TEXT,
      from_value   TEXT,
      to_value     TEXT,
      ok           INTEGER NOT NULL CHECK (ok IN (0,1)),
      error        TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_iroha_events_page ON f_iroha_app_events(page_id, id);

    -- 作業時間セッション (要件定義 §4 / Codex R1 Q3)。個人単位 = 複数人同時作業は複数行。
    --   raw_seconds はサーバー時刻の差分 (iPad の時計を信じない)。承認・補正 (approved) は後続PR。
    --   voided = 誤操作の論理削除 (行は消さず集計から外す — 実測値の除外フラグ)
    --   紐づけ先: Notion 正本の間は page_id、アプリ正本のカードは task_id (v1.1)
    ${sessionsDDL('f_iroha_work_sessions')}

    -- 完成写真・動画 (要件定義 §6 / §1.7 ②outbox)。
    -- ⭐operation_id 付き outbox: 受信時にまず行を作り (status=stored, 実体は DATA_DIR)、
    --   Drive へは裏で送って成功するまで再試行する。再送されても operation_id で二重登録しない。
    --   URL だけを持ち、画像そのものは DB に入れない (Codex「写真をタスク列に詰めない」)。
    --   deleted_at = 論理削除 (撮り直し。物理削除はしない)
    ${mediaDDL('f_iroha_card_media')}

    -- Notion「完成写真」貼り直しのページ単位キュー (Codex PR3 #1: 最後の1件を削除したときも
    -- 「空にする」PATCH が必要 — メディア行の状態だけでは表現できない)。
    -- revision = 要求のたびに +1。PATCH 中に新しい要求が来たら完了扱いにしない (PR3-R2)
    CREATE TABLE IF NOT EXISTS f_iroha_media_page_sync (
      page_id       TEXT PRIMARY KEY,
      revision      INTEGER NOT NULL DEFAULT 0,
      requested_at  TEXT NOT NULL,
      attempt_count INTEGER NOT NULL DEFAULT 0,
      next_retry_at TEXT,
      error         TEXT
    );

    -- 端末 (iPad)。inbound-check と同じ方式 (トークンはハッシュのみ保存)
    CREATE TABLE IF NOT EXISTS f_iroha_app_devices (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash   TEXT NOT NULL UNIQUE,
      label        TEXT NOT NULL,
      created_by   TEXT,
      created_at   TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at   TEXT
    );

    CREATE TABLE IF NOT EXISTS f_iroha_app_enroll_codes (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      code_hash      TEXT NOT NULL UNIQUE,
      label          TEXT NOT NULL,
      created_by     TEXT,
      created_at     TEXT NOT NULL,
      expires_at     TEXT NOT NULL,
      used_at        TEXT,
      used_device_id INTEGER,
      attempts       INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS f_iroha_app_enroll_attempts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip TEXT,
      at TEXT NOT NULL,
      ok INTEGER NOT NULL CHECK (ok IN (0,1))
    );
  `);

  // ── 既存テーブルへの列追加 (冪等。CREATE IF NOT EXISTS は列を増やさない) ──
  const addCol = (table, col, ddl) => {
    const cols = db.prepare(`PRAGMA table_info(${table})`).all().map((c) => c.name);
    if (!cols.includes(col)) db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${ddl}`);
  };
  // 作業開始時点の作業仕様スナップショット (§1.7 ④: 後で仕様が変わっても
  // 「当時何を見て作業したか」を残す。JSON)
  // 外部施設に出す準備ができたか (状態とは別のチェック。Notion のチェックボックスの置き換え — 中原さん 2026-09-03)
  addCol('f_iroha_tasks', 'external_ready', 'INTEGER NOT NULL DEFAULT 0 CHECK (external_ready IN (0,1))');
  addCol('f_iroha_work_sessions', 'master_snapshot', 'TEXT');
  // Drive 側で消えた写真の印 (配信で 404/410 を見たら付け、表示と「前回の完成形」候補から外す。
  // 管理画面の再実行で解除 — Codex R1 #5)
  addCol('f_iroha_card_media', 'unavailable_at', 'TEXT');
  // 選択肢テーブルが normalized_code 無しの古い版なら作り直す (列追加だけでは UNIQUE を差し替えられない)
  migrateWorkOptionsSchema(db);
  // 拠点の初期値 (無ければ足す。名前の変更は管理画面から — 今は無いので tasks.js を正とする)。
  // タスク表の作り直し (facility_code の FK 検査) より前に入れておく
  const insFac = db.prepare('INSERT OR IGNORE INTO f_iroha_facilities (code, name, external, active, sort_order) VALUES (?, ?, ?, 1, ?)');
  for (const f of FACILITIES) insFac.run(f.code, f.name, f.external, f.sort_order);
  // タスク表が CHECK/FK 無し (または一部足りない) 古い版なら作り直す (子テーブルの task_id 追加より前に)
  migrateTasksSchema(db);
  // v1.1 正本化: 作業時間・写真・履歴を task に紐づける (page_id は Notion 時代の証跡として残す — Codex 設計相談 R3)。
  // REFERENCES は宣言する (mirror DB は foreign_keys=ON。存在確認はサービス層でも行う)
  addCol('f_iroha_work_sessions', 'task_id', 'INTEGER REFERENCES f_iroha_tasks(id)');
  addCol('f_iroha_card_media', 'task_id', 'INTEGER REFERENCES f_iroha_tasks(id)');
  addCol('f_iroha_app_events', 'task_id', 'INTEGER REFERENCES f_iroha_tasks(id)');
  // 古い版 (page_id NOT NULL) の作業時間・写真は作り直す — アプリ正本のカードは Notion ページを持たない (A1b)
  migrateSessionMediaSchema(db);
  // 索引は作り直しの後に張る (最初の版には task_id 列が無く、先に張ると起動で落ちる)
  db.exec(`
    ${SESSIONS_INDEX_DDL}
    ${MEDIA_INDEX_DDL}
    CREATE INDEX IF NOT EXISTS idx_iroha_events_task ON f_iroha_app_events(task_id, id);
  `);
  // video_url は inbound-check 側でも足すが、いろは単独経路の起動でも保証する
  // (このアプリが先に f_iroha_work_master を SELECT すると no such column になるため)
  if (db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_iroha_work_master'").get()) {
    addCol('f_iroha_work_master', 'video_url', 'TEXT');
  }

  // 「1作業者につき活動中セッション1件」は**DBの制約**で保証する (Codex PR2 #1:
  // アプリ側のトランザクション検査だけだと、将来の別経路・移行コードから重複を作れる)。
  // 部分ユニークを張る前に、万一の既存重複 (最新以外) を admin 終了で閉じておく
  db.exec('DROP INDEX IF EXISTS idx_iroha_sessions_open');
  db.prepare(`UPDATE f_iroha_work_sessions
    SET ended_at = ?, end_reason = 'admin',
        raw_seconds = MAX(0, CAST((julianday(?) - julianday(started_at)) * 86400 AS INTEGER))
    WHERE ended_at IS NULL AND id NOT IN (
      SELECT MAX(id) FROM f_iroha_work_sessions WHERE ended_at IS NULL GROUP BY worker_id)`)
    .run(utcNow(), utcNow());
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_iroha_sessions_open_uniq
    ON f_iroha_work_sessions(worker_id) WHERE ended_at IS NULL`);
}

// ───────────────────────── Notion キャッシュ ─────────────────────────

/** 取得結果でキャッシュを全置換する (1トランザクション。部分更新にしない — 消えたカードを残さない) */
export function replaceCache(pages, { fetchedAt = utcNow() } = {}) {
  const db = getDB();
  db.transaction(() => {
    db.prepare('DELETE FROM f_iroha_app_notion_cache').run();
    const ins = db.prepare(`INSERT INTO f_iroha_app_notion_cache
      (page_id, status, title, product_code, dedupe_key, url, last_edited_time, payload, fetched_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const p of pages) {
      ins.run(p.pageId, p.status, p.title, p.productCode, p.dedupeKey, p.url, p.lastEditedTime,
        JSON.stringify(p.props), fetchedAt);
    }
    setMeta(db, 'last_refresh_at', fetchedAt);
    setMeta(db, 'last_refresh_error', null);
  }).immediate();
}

export function listCache() {
  return getDB().prepare('SELECT * FROM f_iroha_app_notion_cache ORDER BY page_id').all();
}

export function getCachePage(pageId) {
  return getDB().prepare('SELECT * FROM f_iroha_app_notion_cache WHERE page_id = ?').get(String(pageId)) || null;
}

/** ステータス変更が成功したとき、次の全体更新を待たずキャッシュへ反映する */
export function updateCacheStatus(pageId, status, lastEditedTime) {
  return getDB().prepare(`UPDATE f_iroha_app_notion_cache
    SET status = ?, last_edited_time = COALESCE(?, last_edited_time) WHERE page_id = ?`)
    .run(status, lastEditedTime || null, pageId).changes;
}

/**
 * 1ページ分を upsert する (parsePage の結果)。
 * 全置換の取得に含まれなかった直近変更ページの復元用 — 例: 全体取得の最中に
 * 棚入完了→作業中 へ変えると、未完了クエリ (変更前) にも完了クエリ (変更後) にも
 * 入らず、UPDATE では0件になり行ごと消えてしまう (Codex PR1-R2 #1)
 */
export function upsertCachePage(p, fetchedAt = utcNow()) {
  getDB().prepare(`INSERT INTO f_iroha_app_notion_cache
    (page_id, status, title, product_code, dedupe_key, url, last_edited_time, payload, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(page_id) DO UPDATE SET
      status = excluded.status, title = excluded.title, product_code = excluded.product_code,
      dedupe_key = excluded.dedupe_key, url = excluded.url, last_edited_time = excluded.last_edited_time,
      payload = excluded.payload, fetched_at = excluded.fetched_at`)
    .run(p.pageId, p.status, p.title, p.productCode, p.dedupeKey, p.url, p.lastEditedTime,
      JSON.stringify(p.props), fetchedAt);
}

export function removeCachePage(pageId) {
  return getDB().prepare('DELETE FROM f_iroha_app_notion_cache WHERE page_id = ?').run(pageId).changes;
}

function setMeta(db, key, value) {
  if (value == null) db.prepare('DELETE FROM f_iroha_app_meta WHERE key = ?').run(key);
  else db.prepare(`INSERT INTO f_iroha_app_meta (key, value) VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(key, String(value));
}

export function getMeta(key) {
  const r = getDB().prepare('SELECT value FROM f_iroha_app_meta WHERE key = ?').get(key);
  return r ? r.value : null;
}

export function setMetaValue(key, value) { setMeta(getDB(), key, value); }
/** タスクごとの作業時間の合計 (秒。取り消した記録は除く)。履歴画面で「何分かかったか」を出す */
export function workSecondsByTask(taskIds) {
  const ids = [...new Set((taskIds || []).map(Number).filter((n) => Number.isSafeInteger(n) && n > 0))];
  const out = new Map();
  if (ids.length === 0) return out;
  const db = getDB();
  for (let i = 0; i < ids.length; i += 400) {
    const chunk = ids.slice(i, i + 400);
    const rows = db.prepare(`SELECT task_id, SUM(COALESCE(raw_seconds, 0)) AS secs, COUNT(DISTINCT worker_id) AS people
      FROM f_iroha_work_sessions WHERE voided_at IS NULL AND ended_at IS NOT NULL AND task_id IN (${chunk.map(() => '?').join(',')})
      GROUP BY task_id`).all(...chunk);
    for (const r of rows) out.set(r.task_id, { seconds: Number(r.secs) || 0, people: r.people });
  }
  return out;
}

/** 正本: 'notion' | 'app' (管理画面 /admin/source で切替。入荷受付の Notion 送信もこれを見る) */
export function sourceOfTruth() { return getMeta('source_of_truth') === 'app' ? 'app' : 'notion'; }

// ───────────────────────── 作業者 (いろは名簿) ─────────────────────────

export function listIrohaWorkers(includeInactive = false) {
  // pin_set は「設定済みかどうか」のフラグだけ (ハッシュは出さない)
  return getDB().prepare(`SELECT id, display_name, worker_type, active, sort_order,
      (pin_hash IS NOT NULL) AS pin_set
    FROM f_iroha_workers ${includeInactive ? '' : 'WHERE active = 1'}
    ORDER BY sort_order, id`).all();
}

export function getIrohaWorker(id) {
  const n = Number(id);
  if (!Number.isInteger(n) || n <= 0) return null;
  return getDB().prepare('SELECT id, display_name, worker_type, active FROM f_iroha_workers WHERE id = ?').get(n) || null;
}

export function addIrohaWorker({ displayName, workerType, actor }) {
  const name = String(displayName || '').trim();
  if (!name || name.length > 30) return { ok: false, error: 'bad_name', message: '名前は1〜30文字で入力してください' };
  if (workerType !== 'member' && workerType !== 'staff') {
    return { ok: false, error: 'bad_type', message: '区分は 利用者 / 職員 のどちらかです' };
  }
  const db = getDB();
  const dup = db.prepare('SELECT id FROM f_iroha_workers WHERE display_name = ? AND active = 1').get(name);
  if (dup) return { ok: false, error: 'duplicate', message: `「${name}」は既に登録されています` };
  const info = db.prepare(`INSERT INTO f_iroha_workers (display_name, worker_type, active, created_at, created_by)
    VALUES (?, ?, 1, ?, ?)`).run(name, workerType, utcNow(), actor || null);
  return { ok: true, id: Number(info.lastInsertRowid) };
}

export function setIrohaWorkerActive(id, active) {
  return getDB().prepare('UPDATE f_iroha_workers SET active = ? WHERE id = ?')
    .run(active ? 1 : 0, Number(id)).changes > 0;
}

// ── 職員PIN (職員限定操作の本人確認。worker_id の自己申告を信用しない — Codex PR1 #1) ──
// ハッシュは scrypt (短い数字PINは sha256 だと漏えい時に総当たりが容易 — セキュリティレビュー指摘)。
// 失敗ロックは DB に持つ (プロセス内 Map だと再起動で消える — Codex PR1-R2 #4)

const PIN_MAX_FAILS = 5;
const PIN_LOCK_MS = 10 * 60 * 1000;
const pinHash = (salt, pin) => crypto.scryptSync(String(pin), `iroha-pin:${salt}`, 32).toString('hex');

export function setWorkerPin(id, pin, actor) {
  const p = String(pin || '').trim();
  if (!/^\d{4,8}$/.test(p)) return { ok: false, error: 'bad_pin', message: 'PINは4〜8桁の数字で設定してください' };
  const w = getIrohaWorker(id);
  if (!w) return { ok: false, error: 'not_found', message: '作業者が見つかりません' };
  if (w.worker_type !== 'staff') return { ok: false, error: 'not_staff', message: 'PINを設定できるのは職員だけです' };
  const salt = crypto.randomBytes(16).toString('hex');
  getDB().prepare('UPDATE f_iroha_workers SET pin_hash = ?, pin_salt = ?, pin_fails = 0, pin_lock_until = NULL WHERE id = ?')
    .run(pinHash(salt, p), salt, Number(id));
  // 監査ログの失敗で設定済みの結果を失敗に見せない (Codex PR1-R2 #5)
  try {
    logEvent({ action: 'pin_set', workerId: w.id, workerName: w.display_name, deviceLabel: actor || null, ok: true });
  } catch (e) { console.error('[iroha-work] PIN設定の履歴記録に失敗 (設定自体は完了)', e); }
  return { ok: true };
}

/**
 * PIN 照合。連続失敗 5 回で 10 分ロック (DB 永続 — 再起動で回避できない)。
 * @returns {ok:true} | {ok:false, error:'pin_required'|'pin_invalid'|'pin_locked'}
 */
export function verifyWorkerPin(id, pin) {
  const db = getDB();
  return db.transaction(() => {
    const row = db.prepare('SELECT id, pin_hash, pin_salt, pin_fails, pin_lock_until FROM f_iroha_workers WHERE id = ?').get(Number(id));
    if (!row || !row.pin_hash) return { ok: false, error: 'pin_required', message: 'この職員にはPINが未設定です (管理画面で設定してください)' };
    if (row.pin_lock_until && Date.parse(row.pin_lock_until) > Date.now()) {
      return { ok: false, error: 'pin_locked', message: 'PINの間違いが続いたため一時的にロックしました。10分ほど待ってください' };
    }
    const p = String(pin || '').trim();
    if (!p || pinHash(row.pin_salt, p) !== row.pin_hash) {
      const fails = (row.pin_fails || 0) + 1;
      const lockUntil = fails >= PIN_MAX_FAILS ? new Date(Date.now() + PIN_LOCK_MS).toISOString() : null;
      db.prepare('UPDATE f_iroha_workers SET pin_fails = ?, pin_lock_until = COALESCE(?, pin_lock_until) WHERE id = ?')
        .run(lockUntil ? 0 : fails, lockUntil, row.id);
      if (lockUntil) return { ok: false, error: 'pin_locked', message: 'PINの間違いが続いたため一時的にロックしました。10分ほど待ってください' };
      return { ok: false, error: p ? 'pin_invalid' : 'pin_required', message: p ? 'PINが違います' : '職員のPINを入れてください' };
    }
    db.prepare('UPDATE f_iroha_workers SET pin_fails = 0, pin_lock_until = NULL WHERE id = ?').run(row.id);
    return { ok: true };
  }).immediate();
}

/** テスト用: PIN ロックと失敗カウンタを消す */
export function _clearPinFails() {
  getDB().prepare('UPDATE f_iroha_workers SET pin_fails = 0, pin_lock_until = NULL').run();
}

// ───────────────────────── 操作履歴 ─────────────────────────

export function logEvent({ action, pageId = null, workerId = null, workerName = null, deviceLabel = null, from = null, to = null, ok, error = null }) {
  getDB().prepare(`INSERT INTO f_iroha_app_events
    (at, action, page_id, worker_id, worker_name, device_label, from_value, to_value, ok, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(utcNow(), action, pageId, workerId, workerName, deviceLabel,
      from == null ? null : String(from), to == null ? null : String(to),
      ok ? 1 : 0, error == null ? null : String(error).slice(0, 300));
}

export function listEvents(limit = 100) {
  return getDB().prepare('SELECT * FROM f_iroha_app_events ORDER BY id DESC LIMIT ?').all(Number(limit) || 100);
}

// ───────────────────────── 作業時間セッション ─────────────────────────

// 終了忘れの目印 (自動確定はしない — Codex R1 Q3「未終了時間を自動確定しない」)
export const SESSION_WARN_HOURS = 6;

/**
 * 作業開始。⭐1作業者につき活動中セッションは1件 (要件定義 §1.7 ⑤)。
 * 別カードで作業中なら busy (どのカードかを返す — 画面が誘導する)
 */
export function startSession({ pageId = null, taskId = null, productCode = null, title = null, worker, deviceLabel = null, masterSnapshot = undefined }) {
  const db = getDB();
  const now = utcNow();
  if (pageId == null && taskId == null) return { ok: false, error: 'bad_request', message: 'カードが指定されていません' };
  return db.transaction(() => {
    const open = db.prepare(`SELECT id, page_id, task_id, title_snapshot, started_at FROM f_iroha_work_sessions
      WHERE worker_id = ? AND ended_at IS NULL`).get(worker.id);
    if (open) {
      // 同じカードなら成功扱いで既存セッションを返す (応答が消えた再送で
      // 「実際は動いているのに開始できない」状態にしない — Codex PR2 #2)
      const same = taskId != null ? Number(open.task_id) === Number(taskId) : open.page_id === pageId;
      if (same) return { ok: true, already: true, sessionId: open.id, startedAt: open.started_at };
      return { ok: false, error: 'busy', open,
        message: `「${open.title_snapshot || '別のカード'}」の作業がまだ終わっていません。先にそちらを終了・中断してください` };
    }
    // 開始時点の作業仕様を残す (§1.7 ④)。呼び元 (router) が「画面に見えていた実効値」
    // (マスタ+カードのフォールバック合成 = service.masterOf) を渡す — Codex PR4-R2 #1。
    // 渡されなければマスタ行の生値で代用 (テスト・移行経路用)
    let snapshot = null;
    if (masterSnapshot !== undefined) {
      snapshot = masterSnapshot == null ? null : JSON.stringify(masterSnapshot);
    } else {
      const hasWm = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_iroha_work_master'").get();
      if (productCode && hasWm) {
        const wm = db.prepare('SELECT material_code, storage_container, units_per_container, process_count, note, video_url, version FROM f_iroha_work_master WHERE code_key = ?')
          .get(String(productCode).trim().toLowerCase());
        if (wm) snapshot = JSON.stringify(wm);
      }
    }
    const info = db.prepare(`INSERT INTO f_iroha_work_sessions
      (page_id, task_id, product_code, title_snapshot, worker_id, worker_name, device_label, started_at, master_snapshot)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(pageId, taskId == null ? null : Number(taskId), productCode, title, worker.id, worker.display_name, deviceLabel, now, snapshot);
    return { ok: true, sessionId: Number(info.lastInsertRowid), startedAt: now };
  }).immediate();
}

/**
 * 作業終了・中断。**開始時に発行した sessionId を必ず指定する** (Codex PR2-R2 P2:
 * 「その作業者の活動中の行」を閉じる方式だと、遅延再送が後から始めた別セッションを誤終了する)。
 * 同じ sessionId が既に終了済みなら成功扱いで返す (冪等)。
 * raw_seconds はサーバー時刻の差分で確定する (上書きしない)。
 * @returns {ok, session, remainingActive} remainingActive = このカードでまだ作業中の人数
 */
export function stopSession({ pageId = null, taskId = null, workerId, sessionId, reason }) {
  if (reason !== 'done' && reason !== 'pause') return { ok: false, error: 'bad_request', message: '終了の種類が不正です' };
  const sid = Number(sessionId);
  if (!Number.isInteger(sid) || sid <= 0) return { ok: false, error: 'bad_request', message: 'session_id が必要です (画面を更新してください)' };
  const db = getDB();
  const now = utcNow();
  const byTask = taskId != null;
  return db.transaction(() => {
    const remainingOn = () => db.prepare(`SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE ${byTask ? 'task_id = ?' : 'page_id = ?'} AND ended_at IS NULL`)
      .get(byTask ? Number(taskId) : pageId).c;
    const row = db.prepare('SELECT * FROM f_iroha_work_sessions WHERE id = ?').get(sid);
    const sameCard = row && (byTask ? Number(row.task_id) === Number(taskId) : row.page_id === pageId);
    if (!row || !sameCard || row.worker_id !== Number(workerId)) {
      return { ok: false, error: 'not_started', message: 'このカードで作業をはじめた記録がありません (画面を更新してください)' };
    }
    if (row.ended_at) {
      // 再送 (応答消失) — 対象セッションはもう閉じている。後続の新しいセッションには触らない
      return { ok: true, already: true,
        session: { id: row.id, raw_seconds: row.raw_seconds, started_at: row.started_at, ended_at: row.ended_at },
        remainingActive: remainingOn() };
    }
    const raw = Math.max(0, Math.floor((Date.parse(now) - Date.parse(row.started_at)) / 1000));
    db.prepare('UPDATE f_iroha_work_sessions SET ended_at = ?, end_reason = ?, raw_seconds = ? WHERE id = ?')
      .run(now, reason, raw, row.id);
    return { ok: true, session: { id: row.id, raw_seconds: raw, started_at: row.started_at, ended_at: now }, remainingActive: remainingOn() };
  }).immediate();
}

/** 活動中セッションを page_id → [{id, worker_id, worker_name, started_at}] で返す (一覧表示・終了ボタン用) */
export function activeSessionsByPage() {
  const map = new Map();
  for (const r of getDB().prepare(`SELECT id, page_id, worker_id, worker_name, started_at
    FROM f_iroha_work_sessions WHERE ended_at IS NULL AND page_id IS NOT NULL ORDER BY started_at`).all()) {
    if (!map.has(r.page_id)) map.set(r.page_id, []);
    map.get(r.page_id).push(r);
  }
  return map;
}

/** 同上。アプリ正本のカード用に task_id で引く */
export function activeSessionsByTask() {
  const map = new Map();
  for (const r of getDB().prepare(`SELECT id, task_id, worker_id, worker_name, started_at
    FROM f_iroha_work_sessions WHERE ended_at IS NULL AND task_id IS NOT NULL ORDER BY started_at`).all()) {
    if (!map.has(r.task_id)) map.set(r.task_id, []);
    map.get(r.task_id).push(r);
  }
  return map;
}

/**
 * 商品コードごとの実測 (カード単位の合計作業時間を平均)。voided は集計から外す。
 * @returns Map<code_key, { avgSeconds, cards, lastSeconds }>
 */
export function estimateByProduct() {
  // カード単位 = task_id (アプリ正本) か page_id (Notion 時代)。同じカードの複数人・複数回を 1 件にまとめる
  const rows = getDB().prepare(`SELECT LOWER(TRIM(product_code)) AS k, COALESCE('t' || task_id, page_id) AS card, SUM(raw_seconds) AS total, MAX(ended_at) AS last_end
    FROM f_iroha_work_sessions
    WHERE ended_at IS NOT NULL AND voided_at IS NULL AND product_code IS NOT NULL AND raw_seconds > 0
    GROUP BY LOWER(TRIM(product_code)), COALESCE('t' || task_id, page_id)`).all();
  const byCode = new Map();
  for (const r of rows) {
    if (!byCode.has(r.k)) byCode.set(r.k, []);
    byCode.get(r.k).push(r);
  }
  const out = new Map();
  for (const [k, list] of byCode) {
    list.sort((a, b) => String(a.last_end).localeCompare(String(b.last_end)));
    const totals = list.map(x => Number(x.total) || 0);
    out.set(k, {
      avgSeconds: Math.round(totals.reduce((a, b) => a + b, 0) / totals.length),
      cards: totals.length,
      lastSeconds: totals[totals.length - 1],
    });
  }
  return out;
}

/**
 * 管理画面用: **活動中は全件** + 終了済みは直近 limit 件。
 * ⚠活動中を件数制限に含めない — 6時間超の終了忘れが新しい記録に押し流されて
 *   「取り消す唯一の導線」ごと見えなくなる (Codex PR2 #3)
 */
export function listSessionsForAdmin(limit = 50) {
  const db = getDB();
  const open = db.prepare('SELECT * FROM f_iroha_work_sessions WHERE ended_at IS NULL ORDER BY started_at').all();
  const closed = db.prepare('SELECT * FROM f_iroha_work_sessions WHERE ended_at IS NOT NULL ORDER BY id DESC LIMIT ?')
    .all(Number(limit) || 50);
  const rows = [...open, ...closed];
  const now = Date.now();
  for (const r of rows) {
    r.elapsed_seconds = r.ended_at ? r.raw_seconds : Math.max(0, Math.floor((now - Date.parse(r.started_at)) / 1000));
    r.warn_long = !r.ended_at && r.elapsed_seconds > SESSION_WARN_HOURS * 3600;
  }
  return rows;
}

/**
 * セッションの取り消し (論理削除)。活動中なら同時に end_reason='admin' で閉じる。
 * 行は消さない — 実測の集計から外れるだけ (Codex R2「実測値の除外フラグ」)
 */
export function voidSession(id, actor, reason) {
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const row = db.prepare('SELECT * FROM f_iroha_work_sessions WHERE id = ?').get(Number(id));
    if (!row) return { ok: false, error: 'not_found', message: 'セッションが見つかりません' };
    if (row.voided_at) return { ok: false, error: 'already_voided', message: '既に取り消し済みです' };
    if (!row.ended_at) {
      const raw = Math.max(0, Math.floor((Date.parse(now) - Date.parse(row.started_at)) / 1000));
      db.prepare('UPDATE f_iroha_work_sessions SET ended_at = ?, end_reason = ?, raw_seconds = ? WHERE id = ?')
        .run(now, 'admin', raw, row.id);
    }
    db.prepare('UPDATE f_iroha_work_sessions SET voided_at = ?, voided_by = ?, void_reason = ? WHERE id = ?')
      .run(now, actor || null, reason ? String(reason).slice(0, 200) : null, row.id);
    return { ok: true };
  }).immediate();
}

// ───────────────────────── 端末 (iPad) — inbound-check と同じ方式 ─────────────────────────

export function createDevice(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const token = crypto.randomBytes(32).toString('base64url');
  const info = getDB().prepare('INSERT INTO f_iroha_app_devices (token_hash, label, created_by, created_at) VALUES (?, ?, ?, ?)')
    .run(hashToken(token), l, actor, utcNow());
  return { token, id: Number(info.lastInsertRowid) };
}

export function verifyDevice(token) {
  if (!token) return null;
  const db = getDB();
  const row = db.prepare('SELECT * FROM f_iroha_app_devices WHERE token_hash = ? AND revoked_at IS NULL').get(hashToken(token));
  if (!row) return null;
  const now = utcNow();
  if (Date.parse(now) - Date.parse(row.created_at) > DEVICE_TTL_MS) return null;
  if (!row.last_seen_at || Date.parse(now) - Date.parse(row.last_seen_at) > 3600_000) {
    db.prepare('UPDATE f_iroha_app_devices SET last_seen_at = ? WHERE id = ?').run(now, row.id);
  }
  return row;
}

export function revokeDevice(id) {
  return getDB().prepare('UPDATE f_iroha_app_devices SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL')
    .run(utcNow(), Number(id)).changes > 0;
}

export function listDevices() {
  return getDB().prepare('SELECT id, label, created_by, created_at, last_seen_at, revoked_at FROM f_iroha_app_devices ORDER BY id').all();
}

// ── 登録コード (6桁・10分・1回。総当たり対策も inbound-check と同じ) ──

export const ENROLL_TTL_MS = 10 * 60 * 1000;
export const ENROLL_MAX_ATTEMPTS = 5;
export const ENROLL_RATE_WINDOW_MS = 10 * 60 * 1000;
export const ENROLL_RATE_PER_IP = 8;
export const ENROLL_RATE_GLOBAL = 40;

export function createEnrollCode(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const db = getDB();
  const now = new Date();
  const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
  db.transaction(() => {
    // 有効なコードは常に1つだけ (総当たりの当たり確率を 100万分の1 に抑える)
    const revokedAt = new Date(now.getTime() - 1000).toISOString();
    db.prepare('UPDATE f_iroha_app_enroll_codes SET expires_at = ? WHERE used_at IS NULL AND expires_at > ?')
      .run(revokedAt, now.toISOString());
    db.prepare(`INSERT INTO f_iroha_app_enroll_codes (code_hash, label, created_by, created_at, expires_at)
      VALUES (?, ?, ?, ?, ?)`)
      .run(enrollHash(code), l, actor, now.toISOString(), new Date(now.getTime() + ENROLL_TTL_MS).toISOString());
  }).immediate();
  return { code, label: l, expiresAt: new Date(now.getTime() + ENROLL_TTL_MS).toISOString() };
}

export function redeemEnrollCode(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return { ok: false, error: 'bad_code', message: '6桁の数字を入力してください' };
  const db = getDB();
  return db.transaction(() => {
    const row = db.prepare('SELECT * FROM f_iroha_app_enroll_codes WHERE code_hash = ?').get(enrollHash(c));
    if (!row) return { ok: false, error: 'bad_code', message: '登録コードが違います' };
    if (row.attempts >= ENROLL_MAX_ATTEMPTS) return { ok: false, error: 'too_many', message: 'このコードは無効です (発行し直してください)' };
    if (row.used_at) return { ok: false, error: 'used', message: 'この登録コードは使用済みです (発行し直してください)' };
    if (Date.parse(row.expires_at) < Date.now()) return { ok: false, error: 'expired', message: '登録コードの有効期限が切れています (発行し直してください)' };
    const { token, id } = createDevice(row.label, `enroll:${row.created_by}`);
    db.prepare('UPDATE f_iroha_app_enroll_codes SET used_at = ?, used_device_id = ? WHERE id = ?').run(utcNow(), id, row.id);
    return { ok: true, token, label: row.label };
  }).immediate();
}

export function recordEnrollAttempt({ ip = null, ok = false } = {}) {
  const db = getDB();
  db.prepare('INSERT INTO f_iroha_app_enroll_attempts (ip, at, ok) VALUES (?, ?, ?)')
    .run(ip ? String(ip).slice(0, 64) : null, utcNow(), ok ? 1 : 0);
  db.prepare('DELETE FROM f_iroha_app_enroll_attempts WHERE at < ?')
    .run(new Date(Date.now() - 24 * 3600 * 1000).toISOString());
}

export function checkEnrollRate({ ip = null } = {}) {
  const db = getDB();
  const since = new Date(Date.now() - ENROLL_RATE_WINDOW_MS).toISOString();
  const mine = ip ? db.prepare('SELECT COUNT(*) c FROM f_iroha_app_enroll_attempts WHERE ip = ? AND ok = 0 AND at > ?')
    .get(String(ip).slice(0, 64), since).c : 0;
  if (mine >= ENROLL_RATE_PER_IP) {
    return { allowed: false, error: 'rate_limited', message: '試行が多すぎます。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  const all = db.prepare('SELECT COUNT(*) c FROM f_iroha_app_enroll_attempts WHERE ok = 0 AND at > ?').get(since).c;
  if (all >= ENROLL_RATE_GLOBAL) {
    return { allowed: false, error: 'rate_limited', message: '登録の受付を一時的に停止しています。しばらく待ってから、管理者にコードを発行し直してもらってください' };
  }
  return { allowed: true };
}

export function countEnrollAttempt(code) {
  const c = String(code || '').trim();
  if (!/^\d{6}$/.test(c)) return;
  getDB().prepare('UPDATE f_iroha_app_enroll_codes SET attempts = attempts + 1 WHERE code_hash = ?').run(enrollHash(c));
}

export function listActiveEnrollCodes() {
  return getDB().prepare(`SELECT id, label, created_by, created_at, expires_at, attempts
    FROM f_iroha_app_enroll_codes WHERE used_at IS NULL AND expires_at > ? AND attempts < ?
    ORDER BY id DESC`).all(utcNow(), ENROLL_MAX_ATTEMPTS);
}

// ─── 作業のやり方の選択肢 (資材セット・保管箱) ───
// 中原さん 2026-09-03: 編集はテキスト入力でなく候補からタップ。Excel (作業仕様マスタ) の値を初期値に、
// 初見のものはその場で追加。画像は後から付ける

export const OPTION_KINDS = ['material', 'container'];
const OPTION_LABEL = { material: '資材', container: '保管箱' };
const OPTION_COLS = 'id, kind, code, image_url, sort_order, active';

/** 比較用の正規化: NFKC (全角英数・全角空白→半角) + 連続空白を1つ + trim + 大文字化。表示は入力どおり (Codex R1 #3) */
export function normalizeOptionCode(code) {
  return String(code == null ? '' : code).normalize('NFKC').replace(/\s+/g, ' ').trim().toUpperCase();
}
/** 表示用の整形 (連続空白を1つ・trim。文字種は変えない) */
const displayCode = (code) => String(code == null ? '' : code).replace(/\s+/g, ' ').trim();

/** 選択肢一覧。kind を省くと全種類、includeInactive で無効も (管理画面用)。並び = よく使う順 (sort_order 昇順 = 使用回数の負数) */
export function listWorkOptions(kind = null, includeInactive = false) {
  return getDB().prepare(`SELECT ${OPTION_COLS} FROM f_iroha_work_options
    WHERE (? IS NULL OR kind = ?) ${includeInactive ? '' : 'AND active = 1'} ORDER BY kind, sort_order, code`).all(kind, kind);
}

/** 画面用: { material: [...], container: [...] } */
export function workOptionsByKind(includeInactive = false) {
  const out = { material: [], container: [] };
  for (const r of listWorkOptions(null, includeInactive)) out[r.kind].push(r);
  return out;
}

/**
 * 追加。表記揺れは normalized_code で同一視。
 * @param allowReactivate 同じ値が無効で残っているとき有効に戻してよいか — 管理者だけ true。
 *   職員の「＋新しく登録」で管理者の無効化を解除できないようにする (Codex R1 #1)
 * @returns {ok:true, option, already?, reactivated?} | {ok:false, error, message}
 */
export function addWorkOption({ kind, code, actor, allowReactivate = false }) {
  if (!OPTION_KINDS.includes(kind)) return { ok: false, error: 'bad_kind', message: '種類は 資材 / 保管箱 のどちらかです' };
  const c = displayCode(code);
  const norm = normalizeOptionCode(c);
  if (!c || !norm || c.length > 100) return { ok: false, error: 'bad_code', message: `${OPTION_LABEL[kind]}は1〜100文字で入力してください` };
  const db = getDB();
  const dup = db.prepare(`SELECT ${OPTION_COLS} FROM f_iroha_work_options WHERE kind = ? AND normalized_code = ?`).get(kind, norm);
  if (dup) {
    if (dup.active) return { ok: true, already: true, option: dup };
    if (!allowReactivate) {
      return { ok: false, error: 'inactive_option', message: `「${dup.code}」は管理者が候補から外しています (戻すのは管理画面から)` };
    }
    db.prepare('UPDATE f_iroha_work_options SET active = 1 WHERE id = ?').run(dup.id);
    return { ok: true, already: true, reactivated: true, option: { ...dup, active: 1 } };
  }
  const info = db.prepare(`INSERT INTO f_iroha_work_options (kind, code, normalized_code, active, created_at, created_by) VALUES (?, ?, ?, 1, ?, ?)`)
    .run(kind, c, norm, utcNow(), actor || null);
  return { ok: true, option: db.prepare(`SELECT ${OPTION_COLS} FROM f_iroha_work_options WHERE id = ?`).get(Number(info.lastInsertRowid)) };
}

export function setWorkOptionActive(id, active) {
  return getDB().prepare('UPDATE f_iroha_work_options SET active = ? WHERE id = ?').run(active ? 1 : 0, Number(id)).changes > 0;
}

/**
 * 画像リンクの検証。全 iPad が候補表示のたびに読みに行くので、任意の外部 URL は許さない (Codex R1 #2:
 * 追跡 URL・LAN 内アドレス・巨大画像)。許可 = ポータル内 (/apps/… の相対パス。将来 Render 経由配信の写真) か、
 * https の許可ホストだけ。認証情報つきは不可
 */
const IMAGE_HOST_ALLOW = ['drive.google.com', 'lh3.googleusercontent.com'];
const PORTAL_ORIGIN = 'https://bfaith-portal.onrender.com';
// ポータル内で画像として使えるのは、いろはアプリの配信エンドポイントそのものだけ (将来増えたらここに足す)
const PORTAL_IMAGE_PATH = /^\/apps\/iroha-work\/api\/media\/\d+\/file$/;
/**
 * ポータル内のパスの検証。固定 origin で解析し、正規化後のパスが配信エンドポイントそのものであることを確かめる
 * (%2e%2e や混在エンコードで /apps/ の外へ出られない — Codex 選択肢 R2 #2)。percent-encoding 入り・クエリ・ハッシュは丸ごと不可。
 * 相対パスでも、同じポータルの絶対 URL でも同じ検証を通す (Codex 選択肢 R3: 絶対 URL で許可パスを迂回させない)
 */
function validatePortalImagePath(p) {
  const bad = { ok: false, message: 'ポータル内のリンクは /apps/iroha-work/api/media/<番号>/file だけ使えます' };
  let url;
  try { url = new URL(p, PORTAL_ORIGIN); } catch { return bad; }
  let decoded;
  try { decoded = decodeURIComponent(url.pathname); } catch { return bad; }
  if (url.origin !== PORTAL_ORIGIN || url.pathname !== decoded || decoded.includes('..') || !PORTAL_IMAGE_PATH.test(decoded) || url.search || url.hash) return bad;
  return { ok: true, value: decoded };
}
export function validateOptionImageUrl(raw) {
  const u = String(raw || '').trim();
  if (!u) return { ok: true, value: null };
  if (u.length > 500) return { ok: false, message: '画像リンクが長すぎます (500文字まで)' };
  if (u.startsWith('/')) return validatePortalImagePath(u);
  let url;
  try { url = new URL(u); } catch { return { ok: false, message: '画像は https のリンクか、ポータル内 (/apps/…) のパスを入れてください' }; }
  if (url.protocol !== 'https:') return { ok: false, message: '画像は https のリンクだけ使えます' };
  if (url.username || url.password) return { ok: false, message: '認証情報つきのリンクは使えません' };
  // 同じポータルの絶対 URL は、相対パスと同じ制限 (配信エンドポイントだけ)。保存は相対パスに揃える
  if (url.origin === PORTAL_ORIGIN) return validatePortalImagePath(url.pathname + url.search + url.hash);
  if (!IMAGE_HOST_ALLOW.includes(url.hostname)) return { ok: false, message: `画像のリンク先は ${IMAGE_HOST_ALLOW.join(' / ')} か、ポータル内の配信エンドポイントだけ使えます` };
  return { ok: true, value: url.toString() };
}

/** 画像リンクを設定 (空なら外す)。後で Drive 保存の写真 (Render 経由配信) に差し替えられるよう URL で持つ */
export function setWorkOptionImage(id, imageUrl) {
  const v = validateOptionImageUrl(imageUrl);
  if (!v.ok) return { ok: false, error: 'bad_url', message: v.message };
  const n = getDB().prepare('UPDATE f_iroha_work_options SET image_url = ? WHERE id = ?').run(v.value, Number(id)).changes;
  return n > 0 ? { ok: true } : { ok: false, error: 'not_found', message: '選択肢が見つかりません' };
}

/**
 * f_iroha_work_master (Excel 取込・その場登録の値) に出てくる資材・保管箱を候補に補充する。
 * マスタが変わった時だけ走らせる (件数 + 最終更新のフィンガープリント。成功した時だけ記憶するので、
 * 失敗すれば次回また試す — Codex R1 #5 #6)。表記揺れは正規化で1つにまとめ、使用回数を sort_order に
 * (多い順 = 上に出る。手動追加分は 0 = 末尾)
 * @returns {{material:number, container:number, skipped:boolean}} 追加件数
 */
let seedFingerprint = null;
export function seedWorkOptionsFromMaster({ force = false } = {}) {
  const db = getDB();
  const out = { material: 0, container: 0, skipped: false };
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_iroha_work_master'").get()) return out;
  const fp = db.prepare('SELECT COUNT(*) c, MAX(updated_at) u FROM f_iroha_work_master').get();
  const key = `${fp.c}|${fp.u || ''}`;
  if (!force && seedFingerprint === key) { out.skipped = true; return out; }
  const ins = db.prepare(`INSERT OR IGNORE INTO f_iroha_work_options (kind, code, normalized_code, active, sort_order, created_at, created_by)
    VALUES (?, ?, ?, 1, 0, ?, 'seed:work_master')`);
  const bump = db.prepare('UPDATE f_iroha_work_options SET sort_order = ? WHERE kind = ? AND normalized_code = ?');
  const now = utcNow();
  db.transaction(() => {
    for (const [kind, col] of [['material', 'material_code'], ['container', 'storage_container']]) {
      const rows = db.prepare(`SELECT ${col} v, COUNT(*) n FROM f_iroha_work_master WHERE ${col} IS NOT NULL AND TRIM(${col}) <> '' GROUP BY ${col}`).all();
      const merged = new Map();   // normalized → { code (表記: 半角のものを優先、無ければ最初に見たもの), n (合算) }
      for (const r of rows) {
        const c = displayCode(r.v); const k = normalizeOptionCode(c);
        if (!k) continue;
        const m = merged.get(k) || { code: c, n: 0, canonical: false };
        if (!m.canonical && c.normalize('NFKC') === c) { m.code = c; m.canonical = true; }
        m.n += r.n; merged.set(k, m);
      }
      for (const [k, m] of merged) {
        out[kind] += ins.run(kind, m.code, k, now).changes;
        bump.run(-m.n, kind, k);
      }
    }
  })();
  seedFingerprint = key;
  return out;
}
export function _resetSeedFingerprint() { seedFingerprint = null; }
