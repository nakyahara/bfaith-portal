/**
 * jobs-monitor の状態保存 (専用SQLite)
 *
 * mirror DB とはファイルを分ける — 監視は「一番壊れてはいけない部分」なので、
 * mirror 側のDDL障害・ロック・バックアップと運命を共にしない。
 * データは小さい (ジョブ数×1行 + 通知状態)。
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'jobs-monitor.db');

let db = null;

export function getJobsDB() {
  if (db) return db;
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  db = new Database(DB_FILE);
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS job_state (
      job_id       TEXT PRIMARY KEY,
      last_ok_at   INTEGER,          -- 最終 ok ping = 「やるべきことを全部終えた」(UTC ms)
      last_ping_at INTEGER NOT NULL, -- 最終 ping (status問わず)
      last_status  TEXT NOT NULL,    -- ok | fail | start | partial
      last_note    TEXT
    );
    CREATE TABLE IF NOT EXISTS alert_state (
      job_id        TEXT PRIMARY KEY,
      status        TEXT NOT NULL,   -- 通知済みの status (late / overdue)
      first_at      INTEGER NOT NULL,
      last_alert_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS meta (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS entry_seen (
      job_id        TEXT PRIMARY KEY, -- 台帳エントリを初めて評価した時刻 (初回締切の起点)
      first_seen_at INTEGER NOT NULL
    );
  `);
  // 「生きているか」と「終わったか」を分けるための後付け列 (2026-08-02)。
  // CREATE TABLE IF NOT EXISTS は既存テーブルに列を足さないので明示的に足す。
  const cols = new Set(db.prepare('PRAGMA table_info(job_state)').all().map((c) => c.name));
  if (!cols.has('last_alive_at')) {
    // ok / partial のどちらでも進む。「今日も動いた」の印
    db.exec('ALTER TABLE job_state ADD COLUMN last_alive_at INTEGER');
    // 既存行は ok しか無かったので、それを生存の印として引き継ぐ
    db.exec('UPDATE job_state SET last_alive_at = last_ok_at WHERE last_alive_at IS NULL');
  }
  if (!cols.has('partial_streak')) {
    // ok で 0 に戻る。「動いてはいるが終わっていない」が何回続いたか
    db.exec('ALTER TABLE job_state ADD COLUMN partial_streak INTEGER NOT NULL DEFAULT 0');
  }
  return db;
}

/**
 * 台帳エントリの「初めて見た時刻」を記録しつつ全件返す。
 * ping が一度も来ないジョブ (配線忘れ・ジョブ自体が動いていない) も、
 * この時刻を起点に締切超過へ昇格させるために使う。
 */
export function ensureFirstSeen(ids, nowMs) {
  const d = getJobsDB();
  const ins = d.prepare('INSERT OR IGNORE INTO entry_seen (job_id, first_seen_at) VALUES (?, ?)');
  const tx = d.transaction((list) => { for (const id of list) ins.run(id, nowMs); });
  tx(ids);
  const out = {};
  for (const r of d.prepare('SELECT * FROM entry_seen').all()) out[r.job_id] = r.first_seen_at;
  return out;
}

/**
 * ping を記録する。3つの時刻を別々に進めるのが肝:
 *   last_ok_at    … ok のときだけ = 「やるべきことを全部終えた」
 *   last_alive_at … ok と partial = 「今日も動いた」
 *   last_ping_at  … status問わず (fail/start も含む・デバッグ用)
 * partial_streak は partial で増え ok で 0 に戻る。
 * これで「毎日動いてはいるが永遠に終わらない」を検知できる (2026-08-02 Codexレビュー)。
 */
export function recordPing(jobId, status, note, nowMs) {
  const d = getJobsDB();
  d.prepare(`
    INSERT INTO job_state (job_id, last_ok_at, last_alive_at, last_ping_at, last_status, last_note, partial_streak)
    VALUES (
      @id,
      CASE WHEN @status = 'ok' THEN @now ELSE NULL END,
      CASE WHEN @status IN ('ok', 'partial') THEN @now ELSE NULL END,
      @now, @status, @note,
      CASE WHEN @status = 'partial' THEN 1 ELSE 0 END
    )
    ON CONFLICT(job_id) DO UPDATE SET
      last_ok_at    = CASE WHEN @status = 'ok' THEN @now ELSE job_state.last_ok_at END,
      last_alive_at = CASE WHEN @status IN ('ok', 'partial') THEN @now ELSE job_state.last_alive_at END,
      last_ping_at  = @now,
      last_status   = @status,
      last_note     = @note,
      partial_streak = CASE
        WHEN @status = 'ok'      THEN 0
        WHEN @status = 'partial' THEN job_state.partial_streak + 1
        ELSE job_state.partial_streak
      END
  `).run({ id: jobId, status, note: note || null, now: nowMs });
}

/** { jobId: { lastOkAtMs, lastPingAtMs, lastStatus } } の形で全状態を返す */
export function getStates() {
  const d = getJobsDB();
  const out = {};
  for (const r of d.prepare('SELECT * FROM job_state').all()) {
    out[r.job_id] = {
      lastOkAtMs: r.last_ok_at,
      // 旧データ (列を足す前) は ok しか無かったので ok を生存の印として使う
      lastAliveAtMs: r.last_alive_at ?? r.last_ok_at,
      lastPingAtMs: r.last_ping_at,
      lastStatus: r.last_status,
      lastNote: r.last_note,
      partialStreak: r.partial_streak ?? 0,
    };
  }
  return out;
}

export function getAlertState(jobId) {
  return getJobsDB().prepare('SELECT * FROM alert_state WHERE job_id = ?').get(jobId) || null;
}

export function setAlertState(jobId, status, firstAt, lastAlertAt) {
  getJobsDB().prepare(`
    INSERT INTO alert_state (job_id, status, first_at, last_alert_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(job_id) DO UPDATE SET status = excluded.status,
      first_at = excluded.first_at, last_alert_at = excluded.last_alert_at
  `).run(jobId, status, firstAt, lastAlertAt);
}

export function clearAlertState(jobId) {
  getJobsDB().prepare('DELETE FROM alert_state WHERE job_id = ?').run(jobId);
}

export function getMeta(key) {
  const r = getJobsDB().prepare('SELECT value FROM meta WHERE key = ?').get(key);
  return r ? r.value : null;
}

export function setMeta(key, value) {
  getJobsDB().prepare(`
    INSERT INTO meta (key, value) VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `).run(key, String(value));
}
