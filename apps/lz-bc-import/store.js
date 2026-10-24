/**
 * lz-bc-import/store.js — 「ロジザードに取り込む」ボタンの帳面と状態 (Render の SQLite `DATA_DIR/lz-bc-import.db`)
 *
 * なぜ (2026-10-17 中原さん「ロジザードに新商品の登録自動アップロードって動いているの？ できればボタン押してその都度動かすものもほしい」):
 *   ロジザード登録の CSV (ops.lz_new_exports・0081) を作った後、今までは人がダウンロード → 共有ドライブに置く → Stream Deck で auto-barcode.js、だった
 *   (事務所のメイン PC を作り直して C:\tools\logizard-automation が無くなり、Stream Deck の道は今は動かない)。
 *   → ポータルの画面のボタンで、miniPC (もうロジザードの自動化が動いている PC) に「その CSV の中身そのもの」を取り込ませる。
 *
 * 運び方 = miniPC が取りに来る (新しい受け口を miniPC に開けない):
 *   画面のボタン → この帳面に「待ち」(queued) を 1 つ積む → miniPC の bc-import-runner.js (1 分おきの定時) が Bearer LZ_BC_RUNNER_TOKEN で
 *   ポータルに取りに来る (claim) → ポータルが Company DB の帳面のファイル (取り消していない・sha256 が積んだときと同じ) を渡す →
 *   miniPC が ① (取込) を押す直前に「押す」(executing) を知らせる (断られたら押さない) → 結果 (件数・エラー件数・ログ) を返す → ② (書き出し) の結果を返す。
 *   ロジザードの毎日の商品マスタの取込 (lz-daily-import) と同じ「miniPC → Render の機械の口 (Bearer)」の形 (apps/logizard-import-state)。
 *
 * 決まり (どれも帳面の関数の中で見る = 画面で隠すだけにしない):
 *   - 動いているのは全部で 1 つだけ (queued / claimed / executing / exporting)。表の一意の索引でも守る
 *   - 同じ押し直し (request_id) = 前の答え (2 つ積まない)。同じ request_id でほかのファイル・ほかの人 = 断る
 *   - 取り消した CSV・中身が変わった CSV (sha256 が違う) = 積まない・渡さない
 *   - もう取り込んだ CSV (imported / import_error / 取り込めたかの確かめで「取り込んだ」) = 理由を書いたときだけもう一度
 *   - 取り込めたか分からない回 (unknown) が残っている CSV = 人がロジザードのインポート履歴を確かめて「取り込んだ / 取り込んでいない」を記すまで積まない
 *   - ダウンロードした CSV = 「手でロジザードに取り込んでいない」の確かめが要る (手と自動の 2 回取込を防ぐ)
 *   - 夜: JST 23:45〜01:30 は積まない・23:50〜01:30 は渡さない・押させない (miniPC の毎晩の商品マスタの取込 00:15〜00:55 と同じ共通アカウント。
 *     auto-barcode.js の夜の止め 00:00〜01:30 の前に余白)
 *   - ロジザードの毎日の商品マスタの取込の状態 (apps/logizard-import-state) が 鍵を持っている・取込の途中 (importing / imported_unverified)・
 *     手の取込が開いている = 積まない・渡さない・押させない (呼び手が importBlock を渡す)
 *   - miniPC が 3 分より前から来ていない = 積まない (待ちが溜まらない)。積んで 15 分取りに来ない = expired (何もしていない)
 *   - 鍵の期限 (lease): 取った (claimed) のに 10 分 何も来ない = not_executed (押す前 = 何もしていない)。
 *     押す (executing) と言った後に 8 分 結果が来ない = unknown (人が履歴で確かめる)。② の途中で 10 分 = 取り込んだ・② は分からない
 *   - 押すと言えるのは 取った鍵 (claim_token) で・鍵の期限の中・やめてと言われていない (cancel_requested) ときの 1 回だけ
 * 置き場所 = Render の SQLite (Company DB の移行は足さない = 0081 の帳面には書かない。ファイルの中身と取り消しは 0081 の帳面が正・
 *   この帳面は「ロジザードに取り込ませた回」の記録)。
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

export const LIMITS = Object.freeze({
  queueTtlMs: 15 * 60e3,      // 積んでから取りに来るまで
  claimLeaseMs: 10 * 60e3,    // 取ってから「押す」まで (ログイン・プレビュー)
  execLeaseMs: 8 * 60e3,      // 「押す」から ① の結果まで (結果の待ちは最長 180 秒)
  pressWindowMs: 90e3,        // 「押す」から 実行ボタン・始める確認の OK を押し終えるまで (miniPC が守る・期限 execLeaseMs より十分短い)
  exportLeaseMs: 10 * 60e3,   // ① の結果から ② の結果まで
  runnerAliveMs: 3 * 60e3,    // miniPC が来ていると見る間 (1 分おきの定時)
  logBytes: 32 * 1024,        // 1 回の記録に残すログ (後ろを残す)
  reasonMin: 4, reasonMax: 200,
});
export const ACTIVE = Object.freeze(['queued', 'claimed', 'executing', 'exporting']);
export const STATES = Object.freeze([...ACTIVE, 'imported', 'import_error', 'not_executed', 'unknown', 'cancelled', 'expired']);
export const STATE_WORDS = Object.freeze({
  queued: '待っています (miniPC が取りに来るのを待つ)',
  claimed: 'miniPC が準備中 (ログイン・取り込む前の確かめ)',
  executing: 'ロジザードで取り込んでいます',
  exporting: '取り込みました · バーコードマスタを書き出しています',
  imported: '取り込みました',
  import_error: '取り込みましたが、エラーか件数の違いがあります (ロジザードで確かめる)',
  not_executed: '取り込んでいません (押す前に止めた)',
  unknown: '取り込めたか分かりません (ロジザードのインポート履歴で確かめる)',
  cancelled: 'やめました (取り込んでいない)',
  expired: 'miniPC が取りに来ませんでした (取り込んでいない)',
});
export const EXPORT_WORDS = Object.freeze({ skipped: 'しなかった', ok: '書き出した', failed: '書けなかった', unknown: '分からない (バーコードマスタ.csv の更新時刻を確かめる)' });

export class BcImportError extends Error {
  constructor(code, message, status = 409, extra = {}) { super(message); this.code = code; this.status = status; this.extra = extra; }
}
const fail = (code, message, status = 409, extra = {}) => { throw new BcImportError(code, message, status, extra); };

export const LEDGER_TABLES = Object.freeze(['bc_jobs', 'bc_meta', 'bc_runner', 'bc_downloads', 'bc_void_holds']);
export const ledgerFile = () => path.join(process.env.DATA_DIR || path.join(process.cwd(), 'data'), 'lz-bc-import.db');
/**
 * もうある帳面を、表を足す前に確かめる (0 byte・表が欠けた・始めた時刻が無い = 壊れた帳面。空として作り直すと、
 * 押した後の回の記録が消えて「動いていない」になる = 例外で止める。Codex R7・R8 High)
 * @returns {boolean}  true = まだ無い (新しく作ってよい)
 */
function checkExistingLedger(file) {
  let st;
  try { st = fs.statSync(file); } catch (e) { if (e && e.code === 'ENOENT') return true; throw e; }   // 無いと言えるのは ENOENT だけ
  if (!st.isFile()) throw new Error('帳面がファイルでない');
  if (!st.size) throw new Error('帳面のファイルが 0 byte');
  const ro = new Database(file, { readonly: true, fileMustExist: true });
  try {
    const names = new Set(ro.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all().map((r) => r.name));
    for (const t of LEDGER_TABLES) if (!names.has(t)) throw new Error(`帳面の表 ${t} が無い`);
    startedAtOf(ro.prepare("SELECT value FROM bc_meta WHERE key = 'started_at'").get());   // 無い・形が違う = 例外 (Codex R9 Medium)
  } finally { ro.close(); }
  return false;
}

export function openLzBcDb(file = ledgerFile()) {
  if (file !== ':memory:') {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    checkExistingLedger(file);   // 壊れている = 例外 (作り直さない)
  }
  const db = new Database(file);
  if (file !== ':memory:') db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  db.exec(`
    CREATE TABLE IF NOT EXISTS bc_jobs (
      job_id           INTEGER PRIMARY KEY AUTOINCREMENT,
      request_id       TEXT NOT NULL UNIQUE,
      export_id        TEXT NOT NULL CHECK (export_id GLOB '[1-9]*' AND export_id NOT GLOB '*[^0-9]*'),
      file_sha256      TEXT NOT NULL CHECK (length(file_sha256) = 64),
      text_sha256      TEXT NOT NULL CHECK (length(text_sha256) = 64),
      rows             INTEGER NOT NULL CHECK (rows BETWEEN 1 AND 2000),
      with_export      INTEGER NOT NULL CHECK (with_export IN (0, 1)),
      requested_by     TEXT NOT NULL,
      requested_at     INTEGER NOT NULL,
      rerun_reason     TEXT,
      confirm_not_manual INTEGER NOT NULL DEFAULT 0,
      state            TEXT NOT NULL CHECK (state IN ('queued','claimed','executing','exporting','imported','import_error','not_executed','unknown','cancelled','expired')),
      claim_token      TEXT,
      runner_id        TEXT,
      claimed_at       INTEGER,
      lease_until      INTEGER,
      cancel_requested_by TEXT,
      cancel_requested_at INTEGER,
      executing_at     INTEGER,
      finished_at      INTEGER,
      import_summary   TEXT,
      import_total     INTEGER,
      import_errors    INTEGER,
      export_state     TEXT CHECK (export_state IS NULL OR export_state IN ('skipped','ok','failed','unknown')),
      export_rows      INTEGER,
      export_detail    TEXT,
      reason           TEXT,
      detail           TEXT,
      log              TEXT,
      resolved_outcome TEXT CHECK (resolved_outcome IS NULL OR resolved_outcome IN ('imported','not_imported')),
      resolved_by      TEXT,
      resolved_at      INTEGER,
      resolved_note    TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_bc_jobs_one_active ON bc_jobs ((1)) WHERE state IN ('queued','claimed','executing','exporting');
    CREATE INDEX IF NOT EXISTS ix_bc_jobs_export ON bc_jobs (export_id, job_id);
    CREATE TABLE IF NOT EXISTS bc_events (
      id      INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id  INTEGER,
      at      INTEGER NOT NULL,
      kind    TEXT NOT NULL,
      by      TEXT,
      detail  TEXT
    );
    CREATE TRIGGER IF NOT EXISTS bc_events_no_update BEFORE UPDATE ON bc_events BEGIN SELECT RAISE(ABORT, 'bc_events は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS bc_events_no_delete BEFORE DELETE ON bc_events BEGIN SELECT RAISE(ABORT, 'bc_events は追記だけ'); END;
    CREATE TABLE IF NOT EXISTS bc_runner (
      id        INTEGER PRIMARY KEY CHECK (id = 1),
      runner_id TEXT,
      version   TEXT,
      seen_at   INTEGER,
      busy      TEXT
    );
    CREATE TABLE IF NOT EXISTS bc_downloads (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      export_id TEXT NOT NULL,
      by        TEXT NOT NULL,
      at        INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS ix_bc_downloads_export ON bc_downloads (export_id);
    CREATE TABLE IF NOT EXISTS bc_meta (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bc_void_holds (
      export_id TEXT PRIMARY KEY,
      by        TEXT NOT NULL,
      token     TEXT NOT NULL,
      until     INTEGER NOT NULL
    );
  `);
  // 帳面を始めた時刻 (これより前に作った CSV はダウンロードの記録が無い = 手で取り込んだか分からない。Codex R3 High)
  db.prepare("INSERT OR IGNORE INTO bc_meta (key, value) VALUES ('started_at', ?)").run(String(Date.now()));
  return db;
}
/** 帳面を始めた時刻 (ms) */
export function startedAt(db) {
  return startedAtOf(db.prepare("SELECT value FROM bc_meta WHERE key = 'started_at'").get());
}
/** 始めた時刻の値 = 正の安全な整数 (ms・2001 年〜) だけ。無い・空・数でない・0 = 壊れた帳面 = 例外 (「前に作った CSV」の見分けが効かなくなる) */
function startedAtOf(row) {
  const v = row ? String(row.value) : '';
  if (!/^[1-9][0-9]{11,15}$/.test(v) || !Number.isSafeInteger(Number(v))) throw new Error('帳面の始めた時刻が無いか形が違う');
  return Number(v);
}
/** ダウンロードの記録が当てにならない CSV か (帳面を始める前に作った・作った時刻が分からない) */
export function legacyExport(db, createdMs) {
  const started = startedAt(db);   // 先に確かめる (壊れた帳面 = 例外 = 積まない。Codex R10 Medium)
  return !Number.isFinite(createdMs) || createdMs < started;
}

let shared = null;
/** この process の 1 つの接続 (画面の口と機械の口が同じものを使う) */
export function getLzBcDb() { return (shared ||= openLzBcDb()); }
export function __setLzBcDb(db) { shared = db; }

/**
 * ロジザードの毎日の商品マスタの「手の取込」を始める口 (apps/logizard-import-state/admin-router.js) が呼ぶ:
 * いまボタンの回が押した後 (executing / exporting)、または押した後に「取り込めたか分からない」のまま (人が記していない) か。
 * そう = 手の取込を始めさせない (Codex R5・R6 High。分からない回はロジザード側の取込が終わったと言えない = 記すか、遅れて届いた結果で外れる)。
 * 逆向き (手の取込が開いている間は押させない) は markExecuting が見る。どちらも同じ process の中の同期の処理 = 確かめと始めるの間に入れ違わない。
 * 帳面のファイルがまだ無い (ボタンを一度も使っていない) = 動いていない。ファイルがあるのに読めない = { error } (呼び手は始めない側に倒す。Codex R6 High)
 * @returns {null | { job_id, export_id, state } | { error: string }}
 */
export function bcPressInProgress(now = Date.now()) {
  const file = ledgerFile();
  try {
    // まだ接続が無く、帳面のファイルも無い (ENOENT) = ボタンを一度も使っていない = 動いていない (ファイルは作らない)。
    //   ある帳面は getLzBcDb (openLzBcDb) が作り直す前に確かめる = 壊れている = 例外 = 読めない側
    if (!shared && checkExistingLedger(file)) return null;
    const db = getLzBcDb();
    startedAt(db);   // 開いた後に壊れた (始めた時刻が空・形が違う) = 読めない側 (Codex R10 Medium)
    sweep(db, now);
    return busyJobIn(db);
  } catch (e) {
    console.error(`[lz-bc-import] 帳面を読めない (手の取込は始めない): ${e && e.message}`);
    return { error: String((e && e.message) || e).slice(0, 160) };
  }
}
/**
 * 押した後 (executing / exporting) か、押した後に分からないまま (unknown・記していない) の回 (全部の CSV で 1 つ)。
 * ② の途中で期限が切れた回 (imported・② は分からない) は外す: ① はロジザードの結果の文で取り込みが終わったと確かめてあり、
 * ② はロジザードの書き出し (照会) だけ = 手の取込 (別のアカウント・商品マスタへの取込) と重なっても登録は重ならない
 */
function busyJobIn(db) {
  return db.prepare("SELECT job_id, export_id, state FROM bc_jobs WHERE state IN ('executing','exporting') OR (state = 'unknown' AND resolved_outcome IS NULL) ORDER BY job_id LIMIT 1").get() || null;
}

// ── 時刻 (JST) ──
const jstMin = (now) => { const d = new Date(now + 9 * 3600e3); return d.getUTCHours() * 60 + d.getUTCMinutes(); };
const hhmm = (now) => { const m = jstMin(now); return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`; };
/** 夜: 積まない = 23:45〜01:30・渡さない / 押させない = 23:50〜01:30 (JST・Render の時計) */
export function nightOf(now) {
  const m = jstMin(now);
  const inRange = (from) => m >= from || m < 90;
  return { enqueue: inRange(23 * 60 + 45), claim: inRange(23 * 60 + 50), hhmm: hhmm(now) };
}
/** 次の 23:50 (JST・渡さない / 押させない時間の始まり) までの ms (Render の時計)。もうその時間 = 0 */
export function msUntilClaimNight(now) {
  if (nightOf(now).claim) return 0;
  const DAY = 24 * 3600e3;
  const msOfDay = ((now + 9 * 3600e3) % DAY + DAY) % DAY;
  return (23 * 60 + 50) * 60e3 - msOfDay;
}
export const NIGHT_WORDS = 'JST 23:45〜01:30 は取り込みません (miniPC が毎晩の商品マスタを同じアカウントで取り込む時間。01:30 を過ぎてから押してください)';

const ev = (db, jobId, now, kind, by = null, detail = null) =>
  db.prepare('INSERT INTO bc_events (job_id, at, kind, by, detail) VALUES (?, ?, ?, ?, ?)').run(jobId, now, kind, by, detail == null ? null : JSON.stringify(detail));
const jobOf = (db, id) => db.prepare('SELECT * FROM bc_jobs WHERE job_id = ?').get(id);
const tailLog = (prev, add) => {
  const s = `${prev || ''}${add ? String(add) : ''}`;
  if (Buffer.byteLength(s, 'utf8') <= LIMITS.logBytes) return s;
  const b = Buffer.from(s, 'utf8');
  return '…(前は省いた)\n' + b.subarray(b.length - LIMITS.logBytes).toString('utf8').replace(/^[^\n]*\n/, '');
};
const clean = (v, max) => (v == null ? null : String(v).slice(0, max));
const checkText = (v, what) => {
  if (typeof v !== 'string') fail('invalid_input', `${what}を ${LIMITS.reasonMin}〜${LIMITS.reasonMax} 字で書いてください`, 400);
  const t = v.trim();
  if ([...t].length < LIMITS.reasonMin || [...t].length > LIMITS.reasonMax) fail('invalid_input', `${what}を ${LIMITS.reasonMin}〜${LIMITS.reasonMax} 字で書いてください`, 400);
  return t;
};

/** 期限の過ぎたものを片付ける (どの口でも最初に呼ぶ) */
export function sweep(db, now) {
  const tx = db.transaction(() => {
    for (const j of db.prepare("SELECT * FROM bc_jobs WHERE state IN ('queued','claimed','executing','exporting')").all()) {
      if (j.state === 'queued' && now - j.requested_at > LIMITS.queueTtlMs) {
        db.prepare("UPDATE bc_jobs SET state = 'expired', finished_at = ?, reason = 'not_claimed', detail = ? WHERE job_id = ?").run(now, `${Math.round(LIMITS.queueTtlMs / 60e3)} 分の間に miniPC が取りに来なかった (取り込んでいない)`, j.job_id);
        ev(db, j.job_id, now, 'expired');
      } else if (j.state === 'claimed' && j.lease_until <= now) {
        db.prepare("UPDATE bc_jobs SET state = 'not_executed', finished_at = ?, reason = 'runner_lost', detail = ? WHERE job_id = ?").run(now, 'miniPC が取った後、押す前に知らせが途切れた (押していない = 取り込んでいない)', j.job_id);
        ev(db, j.job_id, now, 'lease_expired', null, { from: 'claimed' });
      } else if (j.state === 'executing' && j.lease_until <= now) {
        db.prepare("UPDATE bc_jobs SET state = 'unknown', finished_at = ?, reason = 'no_result', detail = ? WHERE job_id = ?").run(now, '押した後の結果が届かない。ロジザードのインポート履歴で取り込めたか確かめてください', j.job_id);
        ev(db, j.job_id, now, 'lease_expired', null, { from: 'executing' });
      } else if (j.state === 'exporting' && j.lease_until <= now) {
        // ポータルが ② を Drive に書いた (書けなかった) 結果があれば、そのまま残す (Codex R1 High)
        db.prepare("UPDATE bc_jobs SET state = 'imported', finished_at = ?, export_detail = CASE WHEN export_state IS NULL THEN ? ELSE export_detail END, export_state = COALESCE(export_state, 'unknown') WHERE job_id = ?").run(now, '② の結果が届かない (バーコードマスタ.csv を確かめる)', j.job_id);
        ev(db, j.job_id, now, 'lease_expired', null, { from: 'exporting' });
      }
    }
  });
  tx();
}

/**
 * miniPC が来た (取りに来るたび)。exportUpload = ② のファイルをポータルに送れる runner か (true / false・古い runner は言わない = null = 分からない)。
 *   表を変えないように bc_meta に置く (runner_export_upload = '1' / '0'・分からない = 消す)
 */
export function touchRunner(db, { runnerId, version = null, busy = null, exportUpload = null, now }) {
  db.transaction(() => {
    db.prepare(`INSERT INTO bc_runner (id, runner_id, version, seen_at, busy) VALUES (1, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET runner_id = excluded.runner_id, version = excluded.version, seen_at = excluded.seen_at, busy = excluded.busy`)
      .run(clean(runnerId, 80), clean(version, 80), now, clean(busy, 200));
    if (typeof exportUpload === 'boolean') db.prepare("INSERT INTO bc_meta (key, value) VALUES ('runner_export_upload', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").run(exportUpload ? '1' : '0');
    else db.prepare("DELETE FROM bc_meta WHERE key = 'runner_export_upload'").run();
  })();
}
const metaOf = (db, key) => { const r = db.prepare('SELECT value FROM bc_meta WHERE key = ?').get(key); return r ? r.value : null; };
export function runnerView(db, now) {
  const r = db.prepare('SELECT * FROM bc_runner WHERE id = 1').get();
  if (!r || !r.seen_at) return { seen_at: null, alive: false, busy: null, runner_id: null, version: null, export_upload: null };
  const eu = metaOf(db, 'runner_export_upload');
  return { seen_at: r.seen_at, alive: now - r.seen_at <= LIMITS.runnerAliveMs, busy: r.busy, runner_id: r.runner_id, version: r.version, export_upload: eu === '1' ? true : eu === '0' ? false : null };
}
/** 最後に ② で Drive のバーコードマスタ.csv を書いた回 (画面の③) */
export function lastDriveExport(db) {
  try { const v = metaOf(db, 'drive_last'); return v ? JSON.parse(v) : null; } catch { return null; }
}

/** その CSV のロジザードへの取り込みの様子 */
export function exportImportState(db, exportId) {
  const jobs = db.prepare('SELECT * FROM bc_jobs WHERE export_id = ? ORDER BY job_id DESC').all(String(exportId));
  const imported = jobs.find((j) => j.state === 'imported' || j.state === 'import_error' || (j.state === 'unknown' && j.resolved_outcome === 'imported')) || null;
  return {
    last: jobs[0] || null,
    imported,
    // 🆕 2026-10-24 ボタンで取り込めた回 (エラーの無い取り込み・確かめで「取り込んだ」) = 手の道のダウンロードもさせない (同じ CSV を 2 回取り込まない)。
    //   見るのは「押した最後の回」だけ (押す前に終わった回 = やめた・期限切れ・押していない は飛ばす)。その後のもう一度 (理由つき) が
    //   エラー・「取り込んでいない」で終わった = 手の道に戻せる (#Codex R2 Medium: 前の成功がずっとダウンロードを塞がない)
    importedOk: ((j) => (j && (j.state === 'imported' || (j.state === 'unknown' && j.resolved_outcome === 'imported')) ? j : null))(
      jobs.find((j) => !['cancelled', 'expired', 'not_executed'].includes(j.state) && !ACTIVE.includes(j.state))),
    unresolved: jobs.find((j) => j.state === 'unknown' && !j.resolved_outcome) || null,
    active: jobs.find((j) => ACTIVE.includes(j.state)) || null,
    downloads: db.prepare('SELECT COUNT(*) AS n, MAX(at) AS last_at FROM bc_downloads WHERE export_id = ?').get(String(exportId)),
    count: jobs.length,
  };
}

/**
 * 画面のボタン = 積む。呼び手 (画面の口) が Company DB の帳面を読んだ答え (exportState・fileSha256・textSha256・rows) と
 * ロジザードの取込の状態の止め (importBlock = null か { code, message }) を渡す。
 */
export function enqueue(db, { requestId, exportId, exportState, fileSha256, textSha256, seenTextSha256 = null, rows, withExport = true, by, rerunReason = null, confirmNotManual = false, importBlock = null, exportCreatedMs = NaN, now }) {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(String(requestId || ''))) fail('invalid_input', '押した番号 (request_id) の形が違います。画面を開き直してください', 400);
  if (!/^[1-9]\d{0,17}$/.test(String(exportId || ''))) fail('not_found', 'ファイルがありません', 404);
  if (!by) fail('invalid_input', '誰が押したか分かりません', 400);
  sweep(db, now);
  return db.transaction(() => {
    const prev = db.prepare('SELECT * FROM bc_jobs WHERE request_id = ?').get(requestId);
    if (prev) {
      if (prev.export_id !== String(exportId) || prev.requested_by !== by) fail('request_id_reused', '同じ押した番号 (request_id) がほかのファイル・ほかの人で使われています。画面を開き直してください');
      return { job_id: prev.job_id, state: prev.state, replayed: true };
    }
    if (exportState !== 'issued') fail('voided', '取り消した CSV は取り込めません (作り直してください)');
    if (voidHeld(db, exportId, now)) fail('voiding', 'この CSV はいま取り消しの途中です。画面を開き直してください');
    if (seenTextSha256 && seenTextSha256 !== textSha256) fail('changed', '画面を開いた後にファイルの中身が変わりました。画面を開き直してください');
    if (!/^[0-9a-f]{64}$/.test(String(fileSha256)) || !/^[0-9a-f]{64}$/.test(String(textSha256)) || !Number.isInteger(rows) || rows < 1 || rows > 2000) fail('invalid_input', 'ファイルの形が違います', 400);
    const n = nightOf(now);
    if (n.enqueue) fail('night', `いま ${n.hhmm} (JST)。${NIGHT_WORDS}`);
    const rv = runnerView(db, now);
    if (!rv.alive) fail('runner_offline', `miniPC がポータルに来ていません (最後 ${rv.seen_at ? new Date(rv.seen_at).toISOString() : 'まだ一度も'})。miniPC の定時 [Logizard-BcImport] を確かめるか、ダウンロードして予備の道で取り込んでください`);
    if (importBlock) fail(importBlock.code || 'lz_import_busy', importBlock.message);
    const act = db.prepare("SELECT * FROM bc_jobs WHERE state IN ('queued','claimed','executing','exporting') LIMIT 1").get();
    if (act) fail('busy', `ほかの取り込み (#${act.job_id}・CSV #${act.export_id}) が動いています。終わってから押してください`, 409, { job_id: act.job_id });
    const st = exportImportState(db, exportId);
    if (st.unresolved) fail('unresolved', `前の取り込み (#${st.unresolved.job_id}) が「取り込めたか分からない」のままです。ロジザードのインポート履歴を確かめて、その回に「取り込んだ / 取り込んでいない」を記してから押してください`, 409, { job_id: st.unresolved.job_id });
    // ほかの CSV の回が「取り込めたか分からない」のまま = その取込がロジザードで終わったと言えない = 新しく積まない (Codex R6 High)
    const other = busyJobIn(db);
    if (other) fail('unresolved_other', `ほかの取り込み (#${other.job_id}・CSV #${other.export_id}) が「取り込めたか分からない」のままです。その回をロジザードのインポート履歴で確かめて記してから押してください`, 409, { job_id: other.job_id });
    let reason = null;
    if (st.imported) {
      if (rerunReason == null || rerunReason === '') fail('already_imported', `この CSV はもう取り込みました (#${st.imported.job_id})。もう一度取り込むときは理由を書いてください (ロジザードに 2 回登録されることがあります)`, 409, { job_id: st.imported.job_id });
      reason = checkText(rerunReason, 'もう一度取り込む理由');
    } else if (rerunReason != null && rerunReason !== '') reason = checkText(rerunReason, 'もう一度取り込む理由');
    if (legacyExport(db, exportCreatedMs) && confirmNotManual !== true) fail('downloaded', 'この CSV はボタンができる前に作ったので、ダウンロードして手で取り込んだかポータルでは分かりません。手でロジザードに取り込んでいないことを確かめてから、印を付けて押してください (2 回取り込まない)', 409, { legacy: true });
    if (st.downloads.n > 0 && confirmNotManual !== true) fail('downloaded', 'この CSV はダウンロードされています。手でロジザードに取り込んでいないことを確かめてから、印を付けて押してください (2 回取り込まない)', 409, { downloads: st.downloads.n });
    const info = db.prepare(`INSERT INTO bc_jobs (request_id, export_id, file_sha256, text_sha256, rows, with_export, requested_by, requested_at, rerun_reason, confirm_not_manual, state)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued')`).run(requestId, String(exportId), fileSha256, textSha256, rows, withExport ? 1 : 0, by, now, reason, confirmNotManual === true ? 1 : 0);
    const id = Number(info.lastInsertRowid);
    ev(db, id, now, 'queued', by, { export_id: String(exportId), with_export: !!withExport, rerun: !!reason, downloaded: st.downloads.n > 0 });
    return { job_id: id, state: 'queued', replayed: false };
  })();
}

/** 取りに来た miniPC に渡す候補 (いちばん古い待ち)。無い = null */
export function nextQueued(db, now) {
  sweep(db, now);
  return db.prepare("SELECT * FROM bc_jobs WHERE state = 'queued' ORDER BY job_id LIMIT 1").get() || null;
}

/** 渡す前に断る (取り消した・中身が変わった) = 押していない */
export function refuseQueued(db, { jobId, reason, detail, now }) {
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    if (!j || j.state !== 'queued') return { changed: false };
    db.prepare("UPDATE bc_jobs SET state = 'not_executed', finished_at = ?, reason = ?, detail = ? WHERE job_id = ?").run(now, reason, clean(detail, 300), jobId);
    ev(db, jobId, now, 'refused', 'portal', { reason });
    return { changed: true };
  })();
}

/** 渡す (queued → claimed)。鍵 (claim_token) は miniPC だけが持つ */
export function claimJob(db, { jobId, runnerId, now }) {
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    if (!j || j.state !== 'queued') return null;
    const token = crypto.randomBytes(24).toString('hex');
    db.prepare("UPDATE bc_jobs SET state = 'claimed', claim_token = ?, runner_id = ?, claimed_at = ?, lease_until = ? WHERE job_id = ? AND state = 'queued'")
      .run(token, clean(runnerId, 80), now, now + LIMITS.claimLeaseMs, jobId);
    ev(db, jobId, now, 'claimed', clean(runnerId, 80));
    return { job: jobOf(db, jobId), claimToken: token };
  })();
}

const tokenOk = (j, token) => {
  if (!j) fail('not_found', 'その回はありません', 404);
  const a = Buffer.from(String(j.claim_token || '')), b = Buffer.from(String(token || ''));
  if (!j.claim_token || a.length !== b.length || !crypto.timingSafeEqual(a, b)) fail('bad_token', '鍵 (claim_token) が違います', 403);
};

/**
 * ① の実行ボタンを押す直前 (claimed → executing)。断る = 押さない (その回は not_executed)。
 * blockedWhy = ロジザードの取込の状態の止め (null か { code, message })
 */
export function markExecuting(db, { jobId, claimToken, importBlock = null, now }) {
  sweep(db, now);
  // 断るときも「押していない」をその回に書いてから断る (書いた後に例外を投げると取引ごと戻る = 取引の外で投げる)
  const r = db.transaction(() => {
    const j = jobOf(db, jobId);
    tokenOk(j, claimToken);
    if (j.state !== 'claimed') fail('bad_state', `押せません (いまの状態 ${j.state})`);
    const stop = (reason, detail) => {
      db.prepare("UPDATE bc_jobs SET state = 'not_executed', finished_at = ?, reason = ?, detail = ? WHERE job_id = ?").run(now, reason, detail, jobId);
      ev(db, jobId, now, 'execute_refused', 'portal', { reason });
      return { refused: { reason, detail } };
    };
    if (j.cancel_requested_at) return stop('cancelled', `押す前に ${j.cancel_requested_by} がやめました (取り込んでいない)`);
    const n = nightOf(now);
    if (n.claim) return stop('night', `いま ${n.hhmm} (JST)。夜の時間なので押しません (取り込んでいない)`);
    if (importBlock) return stop(importBlock.code || 'lz_import_busy', `${importBlock.message} (押していない = 取り込んでいない)`);
    if (voidHeld(db, j.export_id, now)) return stop('voiding', 'CSV の取り消しの途中なので押しません (取り込んでいない)');
    // 押してよい間 = 90 秒と、Render の時計で 23:50 (押させない時間の始まり) の 5 秒前までの短い方。10 秒も無い = 押させない (Codex R4 Medium)
    const win = Math.min(LIMITS.pressWindowMs, msUntilClaimNight(now) - 5000);
    if (win < 10e3) return stop('night', `いま ${n.hhmm} (JST)。23:50 までに押し終えられないので押しません (取り込んでいない)`);
    db.prepare("UPDATE bc_jobs SET state = 'executing', executing_at = ?, lease_until = ? WHERE job_id = ?").run(now, now + LIMITS.execLeaseMs, jobId);
    ev(db, jobId, now, 'executing', j.runner_id);
    // 押してよい間 (miniPC は自分の時計でこの間だけ実行ボタンと始める確認の OK を押す = 止まった miniPC が期限の後に押さない。Codex R1 High)
    return { ok: true, state: 'executing', press_window_ms: win };
  })();
  if (r.refused) fail(r.refused.reason, r.refused.detail);
  return r;
}

/**
 * ① の結果の見方。ok = ロジザードの結果の文で エラー件数 0 かつ 総件数 = 行数。
 * 成功の文でも件数が読めない・合わない = import_error (取り込んだが確かめが合わない)。エラーの文 = import_error。ほか = unknown
 */
export function importVerdict(rows, r) {
  if (!r || !r.status) return { state: 'unknown', detail: '① の結果が分からない' };
  const c = r.counts || {};
  if (r.status === 'ok') {
    if (c.errors === 0 && c.total === rows) return { state: 'ok', detail: null };
    return { state: 'import_error', detail: `結果の文の件数が合いません (総件数 ${c.total ?? '読めない'}・エラー件数 ${c.errors ?? '読めない'}・この CSV は ${rows} 行)` };
  }
  if (r.status === 'error') return { state: 'import_error', detail: `ロジザードがエラーを出しました (総件数 ${c.total ?? '—'}・エラー件数 ${c.errors ?? '—'})` };
  return { state: 'unknown', detail: '① の結果が分からない (押した後に途切れた)' };
}

const setImport = (db, j, r, v, now, log) => {
  const c = (r && r.counts) || {};
  db.prepare('UPDATE bc_jobs SET import_summary = ?, import_total = ?, import_errors = ?, log = ? WHERE job_id = ?')
    .run(clean(r && r.summary, 300), Number.isInteger(c.total) ? c.total : null, Number.isInteger(c.errors) ? c.errors : null, tailLog(j.log, log), j.job_id);
  ev(db, j.job_id, now, 'import_result', j.runner_id, { verdict: v.state, total: c.total ?? null, errors: c.errors ?? null });
};

/** ① の結果 (途中の知らせ・任意)。executing → exporting (② をする) / imported / import_error / unknown */
export function reportImport(db, { jobId, claimToken, importResult, log = null, now }) {
  sweep(db, now);
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    tokenOk(j, claimToken);
    if (!(j.state === 'executing' || (j.state === 'unknown' && j.reason === 'no_result' && !j.resolved_outcome))) return { ok: true, state: j.state, already: true };
    const v = importVerdict(j.rows, importResult);
    setImport(db, j, importResult, v, now, log);
    if (v.state === 'ok' && j.with_export) {
      db.prepare("UPDATE bc_jobs SET state = 'exporting', lease_until = ?, reason = NULL, detail = NULL, finished_at = NULL WHERE job_id = ?").run(now + LIMITS.exportLeaseMs, jobId);
    } else {
      const st = v.state === 'ok' ? 'imported' : v.state;
      db.prepare('UPDATE bc_jobs SET state = ?, finished_at = ?, reason = ?, detail = ?, export_state = ? WHERE job_id = ?')
        .run(st, now, st === 'imported' ? null : st, v.detail, 'skipped', jobId);
    }
    return { ok: true, state: jobOf(db, jobId).state };
  })();
}

/**
 * 終わりの知らせ (いつも最後に 1 回・届かなければ miniPC が次の回に送り直す)。
 * executed = 「押す」がポータルに通ったか (miniPC の申告)。importResult = { status: ok|error|unknown, summary, counts } | null。
 * exportResult = { status: ok|failed|skipped, rows, detail } | null。
 */
export function reportFinal(db, { jobId, claimToken, executed = false, importResult = null, exportResult = null, detail = null, reason = null, log = null, now }) {
  sweep(db, now);
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    tokenOk(j, claimToken);
    const late = j.state === 'unknown' && j.reason === 'no_result' && !j.resolved_outcome;   // 期限の後に届いた本当の結果
    const lateExport = j.state === 'imported' && j.export_state === 'unknown';
    const exp = exportResult && ['ok', 'failed', 'skipped'].includes(exportResult.status) ? exportResult : null;
    const setExport = (fallback) => {
      const s = exp ? exp.status : fallback;
      db.prepare('UPDATE bc_jobs SET export_state = ?, export_rows = ?, export_detail = ? WHERE job_id = ?')
        .run(s, exp && Number.isInteger(exp.rows) ? exp.rows : null, clean(exp && exp.detail, 300), jobId);
    };
    const done = (state, why, words) => {
      db.prepare('UPDATE bc_jobs SET state = ?, finished_at = ?, reason = ?, detail = ?, log = ? WHERE job_id = ?').run(state, now, why, clean(words, 300), tailLog(jobOf(db, jobId).log, log), jobId);
      ev(db, jobId, now, 'final', j.runner_id, { state, reason: why });
      return { ok: true, state };
    };
    if (j.state === 'claimed') {
      // 押す前に止まった (押すと言っていない = 押していない)。miniPC が押したと言う = 食い違い = 分からない側
      if (executed) return done('unknown', 'mismatch', 'miniPC は押したと言うが、ポータルは「押す」を受けていない。ロジザードのインポート履歴で確かめてください');
      return done('not_executed', clean(reason, 60) || 'failed_before_execute', detail || '押す前に止まった (取り込んでいない)');
    }
    if ((j.state === 'executing' || late) && !executed && !importResult) {
      // 「押す」はポータルで通ったが、その応答が miniPC に届かなかった (miniPC は押す前に止めた = 実行ボタンは押していない)
      return done('not_executed', clean(reason, 60) || 'not_pressed', detail || '「押す」の応答が届かず、押す前に止めた (取り込んでいない)');
    }
    if (j.state === 'executing' || late) {
      const v = importVerdict(j.rows, importResult);
      if (importResult) setImport(db, j, importResult, v, now, null);
      if (v.state === 'ok') { setExport(j.with_export ? 'failed' : 'skipped'); return done('imported', null, detail); }
      setExport('skipped');
      return done(v.state, v.state, [v.detail, detail].filter(Boolean).join(' · '));
    }
    if (j.state === 'exporting' || lateExport) {
      // ② のファイルをポータルが Drive に書いた (書けなかった) = その結果のまま (miniPC の申告では変えない)
      // ポータルが受けたのに結果が無い (Drive に書いた後に落ちた・応答が miniPC に届かない) = miniPC の「失敗」でなく分からない (Codex R2 Medium)
      if (exportUploadBegun(db, jobId) && !exportUploadDone(db, jobId)) {
        db.prepare("UPDATE bc_jobs SET export_state = 'unknown', export_rows = NULL, export_detail = ? WHERE job_id = ?").run('② のファイルはポータルが受けたが、共有ドライブに書けたか分からない (バーコードマスタ.csv の更新時刻を確かめる)', jobId);
      } else if (!exportUploadDone(db, jobId)) setExport('unknown');
      return done('imported', null, detail);
    }
    if (ACTIVE.includes(j.state)) fail('bad_state', `いまの状態 ${j.state} には終わりを書けません`);
    // もう終わっている (送り直し) = 何もしない
    if (log) db.prepare('UPDATE bc_jobs SET log = ? WHERE job_id = ?').run(tailLog(j.log, log), jobId);
    return { ok: true, state: j.state, already: true };
  })();
}

/**
 * ② のファイルを受ける前の確かめ (runner-router の /jobs/:id/export-file)。② の途中 (exporting・結果がまだ) か、期限で「② は分からない」にした後だけ。
 *   もう結果がある (ok / failed / skipped) = 断る (2 回書かない)
 */
const hasEvent = (db, jobId, kind) => !!db.prepare('SELECT 1 FROM bc_events WHERE job_id = ? AND kind = ? LIMIT 1').get(jobId, kind);
/** ポータルが ② のファイルを受けた (Drive に書きに行った) 回か / その結果を書いた回か */
export const exportUploadBegun = (db, jobId) => hasEvent(db, jobId, 'export_upload_begin');
export const exportUploadDone = (db, jobId) => hasEvent(db, jobId, 'export_upload');
export function beginExportUpload(db, { jobId, claimToken, now }) {
  sweep(db, now);
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    tokenOk(j, claimToken);
    if (!j.with_export) fail('bad_state', 'この回は ② をしない回です');
    // 1 つの回で Drive に書きに行くのは 1 回だけ (送り直し・期限の後でも 2 回書かない。Codex R1 High)
    if (exportUploadBegun(db, jobId)) fail('bad_state', 'この回の ② のファイルはもう受けました');
    // 後の回がもう Drive に書いた = 古い中身で巻き戻さない (遅れて届いた古い回。Codex R2 High)
    const last = lastDriveExport(db);
    if (last && Number(last.job_id) > jobId) fail('bad_state', `後の取り込み (#${last.job_id}) がもう共有ドライブに書いたので、この回の ② は書きません`);
    const lateExport = j.state === 'imported' && j.export_state === 'unknown';
    if (!(j.state === 'exporting' && j.export_state == null) && !lateExport) fail('bad_state', `いまの状態 ${j.state} (② ${j.export_state || 'まだ'}) には ② のファイルを受けません`);
    ev(db, jobId, now, 'export_upload_begin', j.runner_id);
    return { job_id: j.job_id, rows: j.rows };
  })();
}
/**
 * ② の結果 (ポータルが Drive に書いた・書けなかった・書けたか分からない)。status = ok | failed | unknown。ok = 最後に書いた回として覚える (画面の③)。
 *   受けた回 (beginExportUpload) なら、終わりの知らせ・期限の後でも、この結果が勝つ (Drive の本当の様子。Codex R1 Medium)
 */
export function recordExportUpload(db, { jobId, claimToken, status, rows = null, detail = null, modifiedTime = null, now }) {
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    tokenOk(j, claimToken);
    if (!exportUploadBegun(db, jobId) || exportUploadDone(db, jobId)) return { ok: true, state: j.state, already: true };
    const s = status === 'ok' ? 'ok' : status === 'unknown' ? 'unknown' : 'failed';
    const n = Number.isInteger(rows) ? rows : null;
    db.prepare('UPDATE bc_jobs SET export_state = ?, export_rows = ?, export_detail = ? WHERE job_id = ?').run(s, n, clean(detail, 300), jobId);
    ev(db, jobId, now, 'export_upload', j.runner_id, { status: s, rows: n });
    const last = lastDriveExport(db);
    if (s === 'ok' && !(last && Number(last.job_id) > jobId)) db.prepare("INSERT INTO bc_meta (key, value) VALUES ('drive_last', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").run(JSON.stringify({ at: now, job_id: jobId, rows: n, modified_time: modifiedTime }));
    return { ok: true, state: j.state };
  })();
}

/** やめる (画面): 待ち = すぐやめる・取った後で押す前 = 「押す」を断る印 */
export function cancelJob(db, { jobId, by, now }) {
  sweep(db, now);
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    if (!j) fail('not_found', 'その回はありません', 404);
    if (j.state === 'queued') {
      db.prepare("UPDATE bc_jobs SET state = 'cancelled', finished_at = ?, reason = 'cancelled', detail = ?, cancel_requested_by = ?, cancel_requested_at = ? WHERE job_id = ?").run(now, `${by} がやめた (取り込んでいない)`, by, now, jobId);
      ev(db, jobId, now, 'cancelled', by);
      return { ok: true, state: 'cancelled' };
    }
    if (j.state === 'claimed') {
      if (!j.cancel_requested_at) {
        db.prepare('UPDATE bc_jobs SET cancel_requested_by = ?, cancel_requested_at = ? WHERE job_id = ?').run(by, now, jobId);
        ev(db, jobId, now, 'cancel_requested', by);
      }
      return { ok: true, state: 'claimed', cancel_requested: true };
    }
    fail('too_late', j.state === 'executing' || j.state === 'exporting' ? 'もう押しました。やめられません (終わるのを待ってください)' : 'もう終わっています');
  })();
}

/** 取り込めたか分からない回を、人がロジザードのインポート履歴を確かめて記す */
export function resolveJob(db, { jobId, outcome, note, by, now }) {
  if (!['imported', 'not_imported'].includes(outcome)) fail('invalid_input', '「取り込んだ」か「取り込んでいない」を選んでください', 400);
  const t = checkText(note, '確かめたこと (ロジザードのインポート履歴の日時・ファイル名など)');
  sweep(db, now);
  return db.transaction(() => {
    const j = jobOf(db, jobId);
    if (!j) fail('not_found', 'その回はありません', 404);
    if (j.state !== 'unknown') fail('bad_state', '「取り込めたか分からない」回だけ記せます');
    if (j.resolved_outcome) return { ok: true, already: true, outcome: j.resolved_outcome };
    db.prepare('UPDATE bc_jobs SET resolved_outcome = ?, resolved_by = ?, resolved_at = ?, resolved_note = ? WHERE job_id = ?').run(outcome, by, now, t, jobId);
    ev(db, jobId, now, 'resolved', by, { outcome });
    return { ok: true, outcome };
  })();
}

/** ダウンロードの記録 (手で取り込んだかもしれない印) */
export function recordDownload(db, { exportId, by, now }) {
  sweep(db, now);
  // 積んだ・取り込んでいる CSV はダウンロードさせない (ボタンの道と手の道で 2 回取り込まない。Codex R1 High)。
  //   確かめと印は 1 つの取引 = 積むときの「ダウンロードしたか」の確かめと入れ違わない
  db.transaction(() => {
    const st = exportImportState(db, exportId);
    // 取り込めたか分からない回が残る = 人が確かめて記すまでダウンロードもさせない (手で取り込み直さない。Codex R4 High)
    if (st.unresolved) fail('import_unresolved', `この CSV の取り込み (#${st.unresolved.job_id}) が「取り込めたか分からない」のままです。ロジザードのインポート履歴を確かめて画面で記してからダウンロードしてください`);
    const act = st.active;
    if (act) fail('import_running', `この CSV はボタンでロジザードに取り込む途中です (#${act.job_id})。手で取り込むなら、先に画面で「やめる」(押す前だけ) か終わるのを待ってください`);
    // 🆕 2026-10-24 (中原さん「ダウンロードを押しても落ちない」): ボタンで取り込めた CSV = 手でもう一度取り込まない (入らなかった商品があるなら取り消して作り直す)
    if (st.importedOk) fail('imported_by_button', `この CSV はボタンでロジザードに取り込みました (#${st.importedOk.job_id})。手でもう一度取り込まないでください (ダウンロードできません)`);
    db.prepare('INSERT INTO bc_downloads (export_id, by, at) VALUES (?, ?, ?)').run(String(exportId), String(by || ''), now);
  })();
}

/** 取り消しの途中の印 (確かめ → Company DB で取り消す までの間に、積む・渡す・押すをさせない。Codex R1 High) */
// 長め (Company DB の取り消しは文 20 秒・鍵 10 秒で切れる = 途中で切れない。process が落ちて外せなかった印もこの後は効かない)
export const VOID_HOLD_MS = 10 * 60e3;
export function voidHeld(db, exportId, now) {
  const h = db.prepare('SELECT * FROM bc_void_holds WHERE export_id = ?').get(String(exportId));
  return !!(h && h.until > now);
}
/**
 * 取り消してよいかを確かめて、その場で取り消しの途中の印を付ける (1 つの取引)。だめ = 例外。
 * ほかの人の取り消しの途中 (生きた印) = 断る (上書きしない・Codex R2 High)。返す token でだけ外せる
 */
export function reserveVoid(db, { exportId, by, now }) {
  sweep(db, now);
  const token = crypto.randomBytes(16).toString('hex');
  db.transaction(() => {
    if (voidHeld(db, exportId, now)) fail('voiding', 'この CSV はいまほかの人が取り消しています。画面を開き直してください');
    const blk = voidBlockIn(db, exportId);
    if (blk) fail(blk.code, blk.message);
    db.prepare('INSERT INTO bc_void_holds (export_id, by, token, until) VALUES (?, ?, ?, ?) ON CONFLICT(export_id) DO UPDATE SET by = excluded.by, token = excluded.token, until = excluded.until').run(String(exportId), String(by || ''), token, now + VOID_HOLD_MS);
  })();
  return token;
}
/** 取り消せなかった = 自分の印だけ外す (取り消せた = 印を残す。Company DB でも取り消してあるので、渡すときにも断る) */
export function releaseVoid(db, exportId, token) {
  db.prepare('DELETE FROM bc_void_holds WHERE export_id = ? AND token = ?').run(String(exportId), String(token || ''));
}

/** CSV を取り消してよいか (動いている・分からないまま・取り込んだ = 断る)。null = よい */
export function voidBlock(db, exportId, now) {
  sweep(db, now);
  return voidBlockIn(db, exportId);
}
function voidBlockIn(db, exportId) {
  const st = exportImportState(db, exportId);
  if (st.active) return { code: 'import_running', message: `この CSV をロジザードに取り込んでいます (#${st.active.job_id})。終わってから取り消してください` };
  if (st.unresolved) return { code: 'import_unresolved', message: `この CSV の取り込み (#${st.unresolved.job_id}) が「取り込めたか分からない」のままです。ロジザードのインポート履歴を確かめて記してから取り消してください` };
  // 1 件でも入ったかもしれない回 (取り込んだ・エラーや件数違い = 一部は入った見込み・確かめで「取り込んだ」) が 1 つでもあれば取り消さない (Codex R1 High)
  if (st.imported) return { code: 'imported', message: `この CSV はロジザードに取り込みました (#${st.imported.job_id}・${STATE_WORDS[st.imported.state]})。取り消すと同じ商品がまた CSV に出て 2 回登録になるので、取り消せません (入らなかった商品は、ロジザードで直すか、ロジザードの写しが新しくなった後に Claude と相談)` };
  return null;
}

/** 画面に出す回の形 (ログは最後の回だけ) */
export function jobView(j, { withLog = false } = {}) {
  if (!j) return null;
  return {
    job_id: j.job_id, export_id: j.export_id, rows: j.rows, with_export: !!j.with_export, requested_by: j.requested_by, requested_at: j.requested_at,
    rerun_reason: j.rerun_reason, confirm_not_manual: !!j.confirm_not_manual, state: j.state, state_words: STATE_WORDS[j.state],
    claimed_at: j.claimed_at, executing_at: j.executing_at, finished_at: j.finished_at, cancel_requested: !!j.cancel_requested_at,
    import_summary: j.import_summary, import_total: j.import_total, import_errors: j.import_errors,
    export_state: j.export_state, export_words: j.export_state ? EXPORT_WORDS[j.export_state] : null, export_rows: j.export_rows, export_detail: j.export_detail,
    reason: j.reason, detail: j.detail, resolved_outcome: j.resolved_outcome, resolved_by: j.resolved_by, resolved_at: j.resolved_at, resolved_note: j.resolved_note,
    file_sha256: j.file_sha256,
    ...(withLog ? { log: j.log || '' } : {}),
  };
}

/** 画面の全部 (miniPC・動いている回・最近の回・CSV ごとの様子) */
export function screenStatus(db, { now, exportIds = [] }) {
  sweep(db, now);
  const recent = db.prepare('SELECT * FROM bc_jobs ORDER BY job_id DESC LIMIT 20').all();
  const active = db.prepare("SELECT * FROM bc_jobs WHERE state IN ('queued','claimed','executing','exporting') LIMIT 1").get() || null;
  const byExport = {};
  for (const id of exportIds) {
    const st = exportImportState(db, id);
    byExport[id] = {
      last: jobView(st.last), imported_job: st.imported ? st.imported.job_id : null, unresolved_job: st.unresolved ? st.unresolved.job_id : null,
      active_job: st.active ? st.active.job_id : null, imported_ok_job: st.importedOk ? st.importedOk.job_id : null, downloads: st.downloads.n, count: st.count,
    };
  }
  return {
    now, started_at: startedAt(db), night: nightOf(now), runner: runnerView(db, now), drive_last: lastDriveExport(db),
    active: jobView(active, { withLog: true }),
    unresolved_any: jobView(db.prepare("SELECT * FROM bc_jobs WHERE state = 'unknown' AND resolved_outcome IS NULL ORDER BY job_id LIMIT 1").get() || null),
    latest: jobView(recent[0] || null, { withLog: true }),
    recent: recent.map((j) => jobView(j)), byExport,
  };
}
