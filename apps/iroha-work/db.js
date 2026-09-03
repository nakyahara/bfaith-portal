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

const utcNow = () => new Date().toISOString();
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
    CREATE TABLE IF NOT EXISTS f_iroha_work_options (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      kind       TEXT NOT NULL CHECK (kind IN ('material','container')),
      code       TEXT NOT NULL,
      image_url  TEXT,
      sort_order INTEGER NOT NULL DEFAULT 0,
      active     INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
      created_at TEXT NOT NULL,
      created_by TEXT,
      UNIQUE(kind, code)
    );

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
    -- ⭐v1 は Notion が正本なのでタスク表は無く、Notion の page_id に紐づける。
    --   raw_seconds はサーバー時刻の差分 (iPad の時計を信じない)。承認・補正 (approved) は後続PR。
    --   voided = 誤操作の論理削除 (行は消さず集計から外す — 実測値の除外フラグ)
    CREATE TABLE IF NOT EXISTS f_iroha_work_sessions (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      page_id        TEXT NOT NULL,
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
      void_reason    TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_iroha_sessions_page ON f_iroha_work_sessions(page_id, id);

    -- 完成写真・動画 (要件定義 §6 / §1.7 ②outbox)。
    -- ⭐operation_id 付き outbox: 受信時にまず行を作り (status=stored, 実体は DATA_DIR)、
    --   Drive へは裏で送って成功するまで再試行する。再送されても operation_id で二重登録しない。
    --   URL だけを持ち、画像そのものは DB に入れない (Codex「写真をタスク列に詰めない」)。
    --   deleted_at = 論理削除 (撮り直し。物理削除はしない)
    CREATE TABLE IF NOT EXISTS f_iroha_card_media (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      operation_id  TEXT NOT NULL UNIQUE,
      page_id       TEXT NOT NULL,
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
      delete_token_hash TEXT,
      uploader_device_id INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_iroha_media_page ON f_iroha_card_media(page_id, id);

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
  addCol('f_iroha_work_sessions', 'master_snapshot', 'TEXT');
  // Drive 側で消えた写真の印 (配信で 404/410 を見たら付け、表示と「前回の完成形」候補から外す。
  // 管理画面の再実行で解除 — Codex R1 #5)
  addCol('f_iroha_card_media', 'unavailable_at', 'TEXT');
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
export function startSession({ pageId, productCode = null, title = null, worker, deviceLabel = null, masterSnapshot = undefined }) {
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const open = db.prepare(`SELECT id, page_id, title_snapshot, started_at FROM f_iroha_work_sessions
      WHERE worker_id = ? AND ended_at IS NULL`).get(worker.id);
    if (open) {
      // 同じカードなら成功扱いで既存セッションを返す (応答が消えた再送で
      // 「実際は動いているのに開始できない」状態にしない — Codex PR2 #2)
      if (open.page_id === pageId) return { ok: true, already: true, sessionId: open.id, startedAt: open.started_at };
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
      (page_id, product_code, title_snapshot, worker_id, worker_name, device_label, started_at, master_snapshot)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(pageId, productCode, title, worker.id, worker.display_name, deviceLabel, now, snapshot);
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
export function stopSession({ pageId, workerId, sessionId, reason }) {
  if (reason !== 'done' && reason !== 'pause') return { ok: false, error: 'bad_request', message: '終了の種類が不正です' };
  const sid = Number(sessionId);
  if (!Number.isInteger(sid) || sid <= 0) return { ok: false, error: 'bad_request', message: 'session_id が必要です (画面を更新してください)' };
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const remainingOn = () => db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE page_id = ? AND ended_at IS NULL')
      .get(pageId).c;
    const row = db.prepare('SELECT * FROM f_iroha_work_sessions WHERE id = ?').get(sid);
    if (!row || row.page_id !== pageId || row.worker_id !== Number(workerId)) {
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
    FROM f_iroha_work_sessions WHERE ended_at IS NULL ORDER BY started_at`).all()) {
    if (!map.has(r.page_id)) map.set(r.page_id, []);
    map.get(r.page_id).push(r);
  }
  return map;
}

/**
 * 商品コードごとの実測 (カード単位の合計作業時間を平均)。voided は集計から外す。
 * @returns Map<code_key, { avgSeconds, cards, lastSeconds }>
 */
export function estimateByProduct() {
  const rows = getDB().prepare(`SELECT LOWER(TRIM(product_code)) AS k, page_id, SUM(raw_seconds) AS total, MAX(ended_at) AS last_end
    FROM f_iroha_work_sessions
    WHERE ended_at IS NOT NULL AND voided_at IS NULL AND product_code IS NOT NULL AND raw_seconds > 0
    GROUP BY LOWER(TRIM(product_code)), page_id`).all();
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

/** 選択肢一覧。kind を省くと全種類、includeInactive で無効も (管理画面用) */
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
 * 追加。同じ値が無効で残っていれば有効に戻す (値の正規化 = 連続空白を1つに・前後 trim)。
 * @returns {ok:true, option, already?} | {ok:false, error, message}
 */
export function addWorkOption({ kind, code, actor }) {
  if (!OPTION_KINDS.includes(kind)) return { ok: false, error: 'bad_kind', message: '種類は 資材 / 保管箱 のどちらかです' };
  const c = String(code || '').replace(/\s+/g, ' ').trim();
  if (!c || c.length > 100) return { ok: false, error: 'bad_code', message: `${OPTION_LABEL[kind]}は1〜100文字で入力してください` };
  const db = getDB();
  const dup = db.prepare(`SELECT ${OPTION_COLS} FROM f_iroha_work_options WHERE kind = ? AND code = ?`).get(kind, c);
  if (dup) {
    if (!dup.active) db.prepare('UPDATE f_iroha_work_options SET active = 1 WHERE id = ?').run(dup.id);
    return { ok: true, already: true, option: { ...dup, active: 1 } };
  }
  const info = db.prepare(`INSERT INTO f_iroha_work_options (kind, code, active, created_at, created_by) VALUES (?, ?, 1, ?, ?)`)
    .run(kind, c, utcNow(), actor || null);
  return { ok: true, option: db.prepare(`SELECT ${OPTION_COLS} FROM f_iroha_work_options WHERE id = ?`).get(Number(info.lastInsertRowid)) };
}

export function setWorkOptionActive(id, active) {
  return getDB().prepare('UPDATE f_iroha_work_options SET active = ? WHERE id = ?').run(active ? 1 : 0, Number(id)).changes > 0;
}

/** 画像 (http(s) リンク)。空なら外す。後で Drive 保存の写真に差し替えられるよう URL で持つ */
export function setWorkOptionImage(id, imageUrl) {
  const u = String(imageUrl || '').trim();
  if (u && !/^https?:\/\//i.test(u)) return { ok: false, error: 'bad_url', message: '画像は http(s) のリンクを入れてください' };
  const n = getDB().prepare('UPDATE f_iroha_work_options SET image_url = ? WHERE id = ?').run(u || null, Number(id)).changes;
  return n > 0 ? { ok: true } : { ok: false, error: 'not_found', message: '選択肢が見つかりません' };
}

/**
 * f_iroha_work_master (Excel 取込・その場登録の値) に出てくる資材・保管箱を候補に補充する (INSERT OR IGNORE)。
 * Excel を取り込み直したあとも、一覧を開いたときに新しい値が拾われる (呼び元で数分に1回)
 * @returns {{material:number, container:number}} 追加件数
 */
export function seedWorkOptionsFromMaster() {
  const db = getDB();
  const out = { material: 0, container: 0 };
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'f_iroha_work_master'").get()) return out;
  const ins = db.prepare(`INSERT OR IGNORE INTO f_iroha_work_options (kind, code, active, created_at, created_by) VALUES (?, ?, 1, ?, 'seed:work_master')`);
  const now = utcNow();
  db.transaction(() => {
    for (const [kind, col] of [['material', 'material_code'], ['container', 'storage_container']]) {
      const rows = db.prepare(`SELECT DISTINCT TRIM(${col}) v FROM f_iroha_work_master WHERE ${col} IS NOT NULL AND TRIM(${col}) <> ''`).all();
      for (const r of rows) out[kind] += ins.run(kind, r.v, now).changes;
    }
  })();
  return out;
}
