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

    CREATE TABLE IF NOT EXISTS f_inbound_check_devices (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      token_hash   TEXT NOT NULL UNIQUE,
      label        TEXT NOT NULL,
      created_by   TEXT NOT NULL,
      created_at   TEXT NOT NULL,
      last_seen_at TEXT,
      revoked_at   TEXT
    );

  `);
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
}

// ───────────────────────── 端末 (iPad) ─────────────────────────

export function createDevice(label, actor) {
  const l = String(label || '').trim();
  if (!l || l.length > 40) throw new Error('端末名は1〜40文字');
  const token = crypto.randomBytes(32).toString('base64url');
  const info = getDB().prepare(`INSERT INTO f_inbound_check_devices (token_hash, label, created_by, created_at) VALUES (?, ?, ?, ?)`)
    .run(hashToken(token), l, actor, utcNow());
  return { token, id: Number(info.lastInsertRowid) };
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
  return getDB().prepare('SELECT id, label, created_by, created_at, last_seen_at, revoked_at FROM f_inbound_check_devices ORDER BY id').all();
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
      (batch_id, line_key, status, version, checked_by, checked_device, checked_at) VALUES (?, ?, ?, 1, ?, ?, ?)`);
    // ⭐同日中の再取込だけ確認状態を引き継ぐ (要件定義 v1.3 §11.6)。
    //   引き継ぐ条件 = 明細キー (AR|行|詳細行) と商品 (code_key) と予定数 が全部同じ。
    //   予定数が変わった / 商品が差し替わった明細は、数えたものが違うので必ず未確認に戻す。
    //   翌日は従来どおり全部未確認から始める (§2 確定事項⑤「毎朝リセット」を守る)。
    const carry = new Map();
    if (prevActive && prevActive.work_date === workDate) {
      for (const p of db.prepare(`SELECT s.line_key, s.status, s.checked_by, s.checked_device, s.checked_at, l.code_key, l.planned_qty
          FROM f_inbound_check_line_state s
          JOIN f_inbound_check_lines l ON l.batch_id = s.batch_id AND l.line_key = s.line_key
          WHERE s.batch_id = ? AND s.status = 'checked'`).all(prevActive.id)) {
        carry.set(p.line_key, p);
      }
    }
    let carried = 0;
    for (const r of parsed.rows) {
      insLine.run(batchId, r.line_key, r.ar_no, r.line_no, r.detail_no, r.product_id, r.code_key, r.product_name, r.barcode, r.planned_qty, r.received_qty, r.seq);
      const p = carry.get(r.line_key);
      const same = p && p.code_key === r.code_key && p.planned_qty === r.planned_qty;
      if (same) carried++;
      insState.run(batchId, r.line_key, same ? 'checked' : 'unchecked',
        same ? p.checked_by : null, same ? p.checked_device : null, same ? p.checked_at : null);
    }
    logImport(db, { actor, source, fileName, ok: true, batchId,
      message: `${parsed.rows.length}行 / ${slipsMap.size}伝票` + (carried ? ` (同日の確認 ${carried}行を引き継ぎ)` : '') });
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
  return { batches: a, logs: b };
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
  if (!batch) return { batch: null, slips: [], lines: [], totals: { lines: 0, checked: 0 } };
  const slips = db.prepare('SELECT * FROM f_inbound_check_slips WHERE batch_id = ? ORDER BY seq').all(batch.id);
  const lines = db.prepare(`SELECT l.*, s.status AS check_status, s.version, s.checked_by, s.checked_device, s.checked_at
    FROM f_inbound_check_lines l JOIN f_inbound_check_line_state s ON s.batch_id = l.batch_id AND s.line_key = l.line_key
    WHERE l.batch_id = ? ORDER BY l.seq`).all(batch.id);
  const info = productInfoMap(lines.map(l => l.code_key));
  const images = productImageMap(lines.map(l => l.product_id));
  const prev = previousCheckedMap(db, batch.id);
  const checkedBySlip = new Map();
  for (const l of lines) {
    const x = info.get(l.code_key) || { info: null, pick_locs: [], other_locs: [], loc_source: 'none', expiry_managed: false, expiry_source: 'none' };
    l.info = x.info;
    l.pick_locs = x.pick_locs;
    l.other_locs = x.other_locs;
    l.loc_source = x.loc_source;
    l.image_url = images.get(String(l.product_id || '').trim().toLowerCase()) || null;
    l.expiry_managed = !!x.expiry_managed;      // 期限管理商品か (在庫の有効期限から推定 or 手動設定)
    l.expiry_source = x.expiry_source || 'none';
    l.dest = resolveDestination(l.info, { expiryManaged: l.expiry_managed });   // 行き先と、確認の前に決める項目
    l.prev_checked = prev.get(l.line_key) || null;
    if (l.check_status === 'checked') checkedBySlip.set(l.ar_no, (checkedBySlip.get(l.ar_no) || 0) + 1);
  }
  for (const s of slips) s.checked_count = checkedBySlip.get(s.ar_no) || 0;
  const checked = lines.filter(l => l.check_status === 'checked').length;
  // 未確認のうち「行き先が決まっていない」件数 = 画面上部のアラート
  // アラート = 行き先 (いろは在庫化) が未設定のもの。期限入力は毎回聞くので件数には入れない
  const undecided = lines.filter(l => l.check_status !== 'checked' && l.dest.missing.includes('iroha')).length;
  const toIroha = lines.filter(l => l.check_status === 'checked' && l.dest.destination === 'iroha').length;
  return { batch, slips, lines, totals: { lines: lines.length, checked, undecided, toIroha } };
}

/** 確認の前に行き先を決めるために要る明細1件 (active バッチかどうかは applyCheck が見る) */
export function getLineForCheck(batchId, lineKey) {
  return getDB().prepare('SELECT ar_no, line_key, product_id, product_name, code_key, planned_qty FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?')
    .get(Number(batchId), String(lineKey || "")) || null;
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

function currentState(db, batchId, lineKey) {
  return db.prepare('SELECT * FROM f_inbound_check_line_state WHERE batch_id = ? AND line_key = ?').get(batchId, lineKey) || null;
}

/**
 * 1タップ確認 / 取消。
 * @returns {ok:true, state} | {ok:false, error:'stale_batch'|'not_found'|'conflict'|'bad_request', current?}
 */
export function applyCheck({ batchId, lineKey, action, expectVersion, worker, staffId = null, deviceId = null, deviceLabel = null, destination = null, decidedFrom = null, expiryDate = null }) {
  if (!['check', 'uncheck'].includes(action)) return { ok: false, error: 'bad_request', message: 'action が不正です' };
  const bid = Number(batchId);
  if (!Number.isInteger(bid) || !lineKey) return { ok: false, error: 'bad_request', message: 'batch_id / line_key が不正です' };
  // version は必須 (省略を許すと条件が無効化され「画面が見ていた状態」の検証にならない — Codex R3 High)
  if (!Number.isSafeInteger(expectVersion) || expectVersion < 1) return { ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' };
  const w = String(worker || '').trim();
  if (!w) return { ok: false, error: 'bad_request', message: '作業者を選んでください' };
  const db = getDB();
  const tx = db.transaction(() => {
    const active = getActiveBatch();
    if (!active || active.id !== bid) return { ok: false, error: 'stale_batch', message: '一覧が更新されました。最新の一覧を読み込み直してください', activeBatchId: active ? active.id : null };
    const line = db.prepare('SELECT ar_no, product_id, product_name, planned_qty FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?').get(bid, lineKey);
    if (!line) return { ok: false, error: 'not_found', message: '明細が見つかりません' };
    const now = utcNow();
    let res;
    if (action === 'check') {
      res = db.prepare(`UPDATE f_inbound_check_line_state
        SET status = 'checked', version = version + 1, checked_by = ?, checked_device = ?, checked_at = ?
        WHERE batch_id = ? AND line_key = ? AND status = 'unchecked' AND version = ?`)
        .run(w, deviceLabel, now, bid, lineKey, expectVersion);
    } else {
      res = db.prepare(`UPDATE f_inbound_check_line_state
        SET status = 'unchecked', version = version + 1, checked_by = NULL, checked_device = NULL, checked_at = NULL
        WHERE batch_id = ? AND line_key = ? AND status = 'checked' AND version = ?`)
        .run(bid, lineKey, expectVersion);
    }
    if (res.changes === 0) {
      return { ok: false, error: 'conflict', message: '他の端末で先に更新されました', current: currentState(db, bid, lineKey) };
    }
    let reverted = null;
    if (action === 'uncheck') {
      const last = db.prepare(`SELECT id FROM f_inbound_check_events WHERE batch_id = ? AND line_key = ? AND action = 'check' ORDER BY id DESC LIMIT 1`).get(bid, lineKey);
      reverted = last ? last.id : null;
    }
    db.prepare(`INSERT INTO f_inbound_check_events (batch_id, line_key, ar_no, action, worker, staff_id, device_id, device_label, created_at, reverted_event_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(bid, lineKey, line.ar_no, action, w, staffId, deviceId, deviceLabel, now, reverted);
    // 行き先の台帳。確認したら1行足し、取り消したら消さずに cancelled を立てる (append-only)
    if (action === 'check' && destination) {
      db.prepare(`INSERT INTO f_inbound_check_destinations
        (batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, staff_id, device_label, decided_at, expiry_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(bid, lineKey, line.ar_no, line.product_id, line.product_name, line.planned_qty, destination, decidedFrom || 'master', w, staffId, deviceLabel, now, expiryDate);
    } else if (action === 'uncheck') {
      db.prepare('UPDATE f_inbound_check_destinations SET cancelled_at = ?, cancelled_by = ? WHERE batch_id = ? AND line_key = ? AND cancelled_at IS NULL')
        .run(now, w, bid, lineKey);
    }
    return { ok: true, state: currentState(db, bid, lineKey) };
  });
  return tx.immediate();
}

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
