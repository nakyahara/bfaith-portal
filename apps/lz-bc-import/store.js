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
export const EXPORT_WORDS = Object.freeze({ skipped: 'しない', ok: '書き出した', failed: '書き出せなかった (バーコードマスタ.csv は前のまま)', unknown: '分からない' });

export class BcImportError extends Error {
  constructor(code, message, status = 409, extra = {}) { super(message); this.code = code; this.status = status; this.extra = extra; }
}
const fail = (code, message, status = 409, extra = {}) => { throw new BcImportError(code, message, status, extra); };

export function openLzBcDb(file = path.join(process.env.DATA_DIR || path.join(process.cwd(), 'data'), 'lz-bc-import.db')) {
  if (file !== ':memory:') fs.mkdirSync(path.dirname(file), { recursive: true });
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
  `);
  return db;
}

let shared = null;
/** この process の 1 つの接続 (画面の口と機械の口が同じものを使う) */
export function getLzBcDb() { return (shared ||= openLzBcDb()); }
export function __setLzBcDb(db) { shared = db; }

// ── 時刻 (JST) ──
const jstMin = (now) => { const d = new Date(now + 9 * 3600e3); return d.getUTCHours() * 60 + d.getUTCMinutes(); };
const hhmm = (now) => { const m = jstMin(now); return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`; };
/** 夜: 積まない = 23:45〜01:30・渡さない / 押させない = 23:50〜01:30 (JST・Render の時計) */
export function nightOf(now) {
  const m = jstMin(now);
  const inRange = (from) => m >= from || m < 90;
  return { enqueue: inRange(23 * 60 + 45), claim: inRange(23 * 60 + 50), hhmm: hhmm(now) };
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
        db.prepare("UPDATE bc_jobs SET state = 'imported', finished_at = ?, export_state = 'unknown', export_detail = ? WHERE job_id = ?").run(now, '② の結果が届かない (バーコードマスタ.csv を確かめる)', j.job_id);
        ev(db, j.job_id, now, 'lease_expired', null, { from: 'exporting' });
      }
    }
  });
  tx();
}

/** miniPC が来た (取りに来るたび) */
export function touchRunner(db, { runnerId, version = null, busy = null, now }) {
  db.prepare(`INSERT INTO bc_runner (id, runner_id, version, seen_at, busy) VALUES (1, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET runner_id = excluded.runner_id, version = excluded.version, seen_at = excluded.seen_at, busy = excluded.busy`)
    .run(clean(runnerId, 80), clean(version, 80), now, clean(busy, 200));
}
export function runnerView(db, now) {
  const r = db.prepare('SELECT * FROM bc_runner WHERE id = 1').get();
  if (!r || !r.seen_at) return { seen_at: null, alive: false, busy: null, runner_id: null, version: null };
  return { seen_at: r.seen_at, alive: now - r.seen_at <= LIMITS.runnerAliveMs, busy: r.busy, runner_id: r.runner_id, version: r.version };
}

/** その CSV のロジザードへの取り込みの様子 */
export function exportImportState(db, exportId) {
  const jobs = db.prepare('SELECT * FROM bc_jobs WHERE export_id = ? ORDER BY job_id DESC').all(String(exportId));
  const imported = jobs.find((j) => j.state === 'imported' || j.state === 'import_error' || (j.state === 'unknown' && j.resolved_outcome === 'imported')) || null;
  return {
    last: jobs[0] || null,
    imported,
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
export function enqueue(db, { requestId, exportId, exportState, fileSha256, textSha256, seenTextSha256 = null, rows, withExport = true, by, rerunReason = null, confirmNotManual = false, importBlock = null, now }) {
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
    let reason = null;
    if (st.imported) {
      if (rerunReason == null || rerunReason === '') fail('already_imported', `この CSV はもう取り込みました (#${st.imported.job_id})。もう一度取り込むときは理由を書いてください (ロジザードに 2 回登録されることがあります)`, 409, { job_id: st.imported.job_id });
      reason = checkText(rerunReason, 'もう一度取り込む理由');
    } else if (rerunReason != null && rerunReason !== '') reason = checkText(rerunReason, 'もう一度取り込む理由');
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
    db.prepare("UPDATE bc_jobs SET state = 'executing', executing_at = ?, lease_until = ? WHERE job_id = ?").run(now, now + LIMITS.execLeaseMs, jobId);
    ev(db, jobId, now, 'executing', j.runner_id);
    return { ok: true, state: 'executing' };
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
    if (j.state === 'executing' || late) {
      const v = importVerdict(j.rows, importResult);
      if (importResult) setImport(db, j, importResult, v, now, null);
      if (v.state === 'ok') { setExport(j.with_export ? 'failed' : 'skipped'); return done('imported', null, detail); }
      setExport('skipped');
      return done(v.state, v.state, [v.detail, detail].filter(Boolean).join(' · '));
    }
    if (j.state === 'exporting' || lateExport) {
      setExport('unknown');
      return done('imported', null, detail);
    }
    if (ACTIVE.includes(j.state)) fail('bad_state', `いまの状態 ${j.state} には終わりを書けません`);
    // もう終わっている (送り直し) = 何もしない
    if (log) db.prepare('UPDATE bc_jobs SET log = ? WHERE job_id = ?').run(tailLog(j.log, log), jobId);
    return { ok: true, state: j.state, already: true };
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
  db.prepare('INSERT INTO bc_downloads (export_id, by, at) VALUES (?, ?, ?)').run(String(exportId), String(by || ''), now);
}

/** CSV を取り消してよいか (動いている・分からないまま・取り込んだ = 断る)。null = よい */
export function voidBlock(db, exportId, now) {
  sweep(db, now);
  const st = exportImportState(db, exportId);
  if (st.active) return { code: 'import_running', message: `この CSV をロジザードに取り込んでいます (#${st.active.job_id})。終わってから取り消してください` };
  if (st.unresolved) return { code: 'import_unresolved', message: `この CSV の取り込み (#${st.unresolved.job_id}) が「取り込めたか分からない」のままです。ロジザードのインポート履歴を確かめて記してから取り消してください` };
  const cleanImport = st.imported && (st.imported.state === 'imported' || st.imported.resolved_outcome === 'imported');
  if (cleanImport) return { code: 'imported', message: `この CSV はロジザードに取り込みました (#${st.imported.job_id})。取り消すと同じ商品がまた CSV に出て 2 回登録になるので、取り消せません (直すのはロジザードで)` };
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
      active_job: st.active ? st.active.job_id : null, downloads: st.downloads.n, count: st.count,
    };
  }
  return {
    now, night: nightOf(now), runner: runnerView(db, now),
    active: jobView(active, { withLog: true }),
    latest: jobView(recent[0] || null, { withLog: true }),
    recent: recent.map((j) => jobView(j)), byExport,
  };
}
