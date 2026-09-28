/**
 * logizard-import-state/store.js — ロジザードの毎日の商品マスタの取込の「状態」(マスタ正本切替 ③c-1b-1)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b 契約 v3」H1・H4・H5・H6。
 * 自動の ③ (miniPC の 00:20) と戻し方の手の ③ (Stream Deck の PC の auto-barcode.js --only-daily) が
 * **この 1 つの状態と鍵を共用**する (ローカルの状態と真偽だけの旗を後で合わせる作りにしない)。
 *
 * 持つもの (1 行 = id 1):
 *   init_id   初期化の識別子 (各 PC のローカルの印と照合する。H5)
 *   state     idle / importing / imported_unverified / verified / unknown / partial / verify_failed
 *   halted    自動の取込を人が止めた (戻し方の手の ③ はこれが立っているときだけ)
 *   run_*     いまの (最後の) 取込の実行 ID・誰が (auto / manual_daily)・中身 (CSV の sha256・行数など)
 *   lock_*    鍵 (持ち主・目的 import / verify・実行 ID・期限)。**鍵が切れても state は戻らない** (H1)
 * 出来事 (events) は追記だけ (更新・削除はトリガーで断る)。
 *
 * 状態の動き (H4・H6):
 *   idle|verified --(鍵 import・CSV の sha256 と行数)--> importing          … 実行ボタンを押す直前に書く
 *   importing --> imported_unverified (自動の成功) | manual_done (手の ③ の成功 → idle) | partial | unknown | failed_before_execute (→ 前の状態)
 *   imported_unverified --(鍵 verify か同じ回の import)--> verified | verify_failed
 *   importing で鍵が切れたまま = markUnknown で unknown (起動したときに importing が残っている = 自動では二度と押さない)
 *   unknown | partial | verify_failed --resolve (人が履歴を確かめて)--> idle
 * 始めてよい条件:
 *   自動 (auto)       import = halted でない かつ state が idle / verified。verify = state が imported_unverified かつ run_by が auto
 *   手の ③ (manual_daily) import = halted かつ state が idle / verified
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';

export const STATES = Object.freeze(['idle', 'importing', 'imported_unverified', 'verified', 'unknown', 'partial', 'verify_failed']);
export const HOLDERS = Object.freeze(['auto', 'manual_daily']);
export const MAX_TTL_SEC = 600;
const UNRESOLVED = new Set(['importing', 'imported_unverified', 'unknown', 'partial', 'verify_failed']);

export class ImportStateError extends Error {
  constructor(code, message, status = 409) { super(message); this.code = code; this.status = status; }
}
const fail = (code, message, status) => { throw new ImportStateError(code, message, status); };

const rid = (prefix, now) => `${prefix}_${new Date(now).toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;
const RUN_ID_RE = /^[a-z0-9][a-z0-9_-]{5,80}$/i;
const BY_RE = /^[^\u0000-\u001f]{1,60}$/;

/** DB を開く (無ければ作る)。file = ':memory:' で試験 */
export function openImportStateDb(file = path.join(process.env.DATA_DIR || path.join(process.cwd(), 'data'), 'logizard-import-state.db')) {
  if (file !== ':memory:') fs.mkdirSync(path.dirname(file), { recursive: true });
  const db = new Database(file);
  if (file !== ':memory:') db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS import_state (
      id              INTEGER PRIMARY KEY CHECK (id = 1),
      init_id         TEXT NOT NULL,
      state           TEXT NOT NULL,
      prev_state      TEXT,
      halted          INTEGER NOT NULL DEFAULT 0,
      halted_reason   TEXT,
      halted_by       TEXT,
      halted_at       INTEGER,
      run_id          TEXT,
      run_by          TEXT,
      run_detail      TEXT,
      lock_token      TEXT,
      lock_holder     TEXT,
      lock_purpose    TEXT,
      lock_run_id     TEXT,
      lock_expires_at INTEGER,
      notified_at     INTEGER,
      updated_at      INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS import_events (
      id      INTEGER PRIMARY KEY AUTOINCREMENT,
      at      INTEGER NOT NULL,
      kind    TEXT NOT NULL,
      run_id  TEXT,
      by      TEXT,
      detail  TEXT
    );
    CREATE TRIGGER IF NOT EXISTS import_events_no_update BEFORE UPDATE ON import_events BEGIN SELECT RAISE(ABORT, 'import_events は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS import_events_no_delete BEFORE DELETE ON import_events BEGIN SELECT RAISE(ABORT, 'import_events は追記だけ'); END;
  `);
  return db;
}

function row(db) { return db.prepare('SELECT * FROM import_state WHERE id = 1').get() || null; }
function event(db, now, kind, runId, by, detail) {
  db.prepare('INSERT INTO import_events (at, kind, run_id, by, detail) VALUES (?, ?, ?, ?, ?)').run(now, kind, runId ?? null, by ?? null, detail == null ? null : JSON.stringify(detail));
}
function update(db, now, fields) {
  const keys = Object.keys(fields);
  db.prepare(`UPDATE import_state SET ${keys.map((k) => `${k} = @${k}`).join(', ')}, updated_at = @__now WHERE id = 1`).run({ ...fields, __now: now });
}
const lockActive = (r, now) => !!(r && r.lock_token && r.lock_expires_at > now);
function mustRow(db) { const r = row(db); if (!r) fail('not_initialized', 'まだ初期化していない (--init)', 404); return r; }
function checkInit(r, initId) { if (!initId || initId !== r.init_id) fail('init_mismatch', `初期化の識別子が違う (ポータル = ${r.init_id}・手元 = ${initId || 'なし'})。記録の消失か設定違い = 止める`); }
function checkBy(by) { if (!BY_RE.test(String(by || ''))) fail('bad_request', 'by (誰が) が要る', 400); }
function checkRunId(runId) { if (!RUN_ID_RE.test(String(runId || ''))) fail('bad_request', '実行 ID の形が違う', 400); }

/** 見る (鍵の期限が切れていれば lock は null) */
export function getStatus(db, { now = Date.now(), events = 20 } = {}) {
  const r = row(db);
  const ev = db.prepare('SELECT * FROM import_events ORDER BY id DESC LIMIT ?').all(Math.max(0, Math.min(200, events)))
    .map((e) => ({ ...e, detail: e.detail ? JSON.parse(e.detail) : null }));
  if (!r) return { initialized: false, events: ev };
  return {
    initialized: true, init_id: r.init_id, state: r.state, halted: !!r.halted, halted_reason: r.halted_reason, halted_by: r.halted_by, halted_at: r.halted_at,
    run: r.run_id ? { run_id: r.run_id, by: r.run_by, detail: r.run_detail ? JSON.parse(r.run_detail) : null } : null,
    lock: lockActive(r, now) ? { holder: r.lock_holder, purpose: r.lock_purpose, run_id: r.lock_run_id, expires_at: r.lock_expires_at } : null,
    lock_expired: !!(r.lock_token && !lockActive(r, now)),
    notified_at: r.notified_at, updated_at: r.updated_at, events: ev,
  };
}

/** 初期化 (まだ無いときだけ。あれば断る = 履歴や消失を上書きしない。H5) */
export function init(db, { by, note = null, now = Date.now() }) {
  checkBy(by);
  return db.transaction(() => {
    if (row(db)) fail('already_initialized', 'もう初期化してある (消失からの復旧は recover)');
    const initId = rid('lzi', now);
    db.prepare('INSERT INTO import_state (id, init_id, state, halted, updated_at) VALUES (1, ?, ?, 0, ?)').run(initId, 'idle', now);
    event(db, now, 'init', null, by, { init_id: initId, note });
    return { init_id: initId };
  })();
}

/**
 * 消失からの復旧 (人がロジザードのインポート履歴を確かめてから)。初期化の識別子を作り直す。
 * ポータルの状態が無い (ポータル側の消失) = 取込の途中だったか分からない = **止めた状態 (halted) で作る**。
 * ポータルの状態がある (手元の印の消失) = 状態はそのまま・識別子だけ新しく。
 */
export function recover(db, { by, note, now = Date.now() }) {
  checkBy(by);
  if (!note || String(note).trim().length < 4) fail('bad_request', 'recover には note (何を確かめたか) が要る', 400);
  return db.transaction(() => {
    const r = row(db);
    const initId = rid('lzi', now);
    if (!r) {
      db.prepare('INSERT INTO import_state (id, init_id, state, halted, halted_reason, halted_by, halted_at, updated_at) VALUES (1, ?, ?, 1, ?, ?, ?, ?)')
        .run(initId, 'idle', 'recovered: ポータルの状態を作り直した (取込の途中だったか分からない)', by, now, now);
    } else {
      update(db, now, { init_id: initId });
    }
    event(db, now, 'recover', null, by, { init_id: initId, prev_init_id: r ? r.init_id : null, note });
    return { init_id: initId, halted: !r ? true : !!r.halted };
  })();
}

/** 鍵を取る (始めてよい条件は上)。返す lock_token は結果を書くときに使う */
export function acquire(db, { initId, holder, purpose, runId, ttlSec = 180, by, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  if (!HOLDERS.includes(holder)) fail('bad_request', `holder は ${HOLDERS.join(' / ')}`, 400);
  if (!['import', 'verify'].includes(purpose)) fail('bad_request', 'purpose は import / verify', 400);
  const ttl = Math.min(MAX_TTL_SEC, Math.max(30, Math.floor(Number(ttlSec) || 0)));
  return db.transaction(() => {
    const r = mustRow(db);
    checkInit(r, initId);
    if (lockActive(r, now)) fail('busy', `鍵はほかが持っている (${r.lock_holder}・${r.lock_purpose}・${r.lock_run_id}・期限 ${new Date(r.lock_expires_at).toISOString()})`);
    if (holder === 'auto' && purpose === 'import') {
      if (r.halted) fail('halted', `自動の取込は止めてある (${r.halted_reason || ''})`);
      if (!['idle', 'verified'].includes(r.state)) fail('state', `始められない状態: ${r.state} (${r.run_id || ''})`);
    } else if (holder === 'auto' && purpose === 'verify') {
      if (r.state !== 'imported_unverified' || r.run_by !== 'auto') fail('state', `確かめる取込が無い: ${r.state}`);
      if (runId !== r.run_id) fail('run_mismatch', `確かめるのは ${r.run_id}`);
    } else if (holder === 'manual_daily' && purpose === 'import') {
      if (!r.halted) fail('not_halted', '戻し方の手の ③ は、自動の取込を止めてから (halt)');
      if (!['idle', 'verified'].includes(r.state)) fail('state', `始められない状態: ${r.state} (${r.run_id || ''})`);
    } else fail('bad_request', `${holder} は ${purpose} できない`, 400);
    const token = crypto.randomUUID();
    const expires = now + ttl * 1000;
    update(db, now, { lock_token: token, lock_holder: holder, lock_purpose: purpose, lock_run_id: runId, lock_expires_at: expires });
    event(db, now, 'lock_acquire', runId, by, { holder, purpose, expires_at: expires });
    return { lock_token: token, expires_at: expires };
  })();
}

/** 鍵を延ばす (切れていたら断る = 呼び手は実行ボタンを押す前なら止める) */
export function extend(db, { lockToken, ttlSec = 180, now = Date.now() }) {
  const ttl = Math.min(MAX_TTL_SEC, Math.max(30, Math.floor(Number(ttlSec) || 0)));
  return db.transaction(() => {
    const r = mustRow(db);
    if (!lockToken || r.lock_token !== lockToken || !lockActive(r, now)) fail('lock_lost', '鍵が切れた・ほかに移った');
    const expires = now + ttl * 1000;
    update(db, now, { lock_expires_at: expires });
    return { expires_at: expires };
  })();
}

/** 鍵を返す (state は変えない) */
export function release(db, { lockToken, by, now = Date.now() }) {
  return db.transaction(() => {
    const r = mustRow(db);
    if (!lockToken || r.lock_token !== lockToken) return { released: false };
    update(db, now, { lock_token: null, lock_holder: null, lock_purpose: null, lock_run_id: null, lock_expires_at: null });
    event(db, now, 'lock_release', r.lock_run_id, by, null);
    return { released: true };
  })();
}

/**
 * 状態を進める。鍵の token と実行 ID が合うこと (**期限が切れていても**、その回の結果は書ける = 取込の途中で鍵が切れても結果を失わない。
 * importing / imported_unverified の間はほかが鍵を取れない = 取り違えない)。
 * @param {object} p.detail  importing では { csv_sha256, rows, ... } が要る
 */
export function transition(db, { lockToken, runId, to, detail = null, by, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  return db.transaction(() => {
    const r = mustRow(db);
    if (!lockToken || r.lock_token !== lockToken || r.lock_run_id !== runId) fail('lock_lost', '鍵が違う (この回の鍵ではない)');
    const from = r.state;
    const purpose = r.lock_purpose, holder = r.lock_holder;
    const set = (fields) => update(db, now, fields);
    if (to === 'importing') {
      if (purpose !== 'import' || !['idle', 'verified'].includes(from)) fail('bad_transition', `${from} → importing はできない`);
      if (holder === 'auto' && r.halted) fail('halted', '自動の取込は止めてある');
      if (holder === 'manual_daily' && !r.halted) fail('not_halted', '自動の取込が止まっていない');
      if (!detail || !/^[0-9a-f]{64}$/.test(String(detail.csv_sha256 || '')) || !Number.isSafeInteger(detail.rows) || detail.rows < 1) fail('bad_request', 'importing には CSV の sha256 と行数が要る', 400);
      set({ state: 'importing', prev_state: from, run_id: runId, run_by: holder, run_detail: JSON.stringify({ ...detail, started_at: now }), notified_at: null });
    } else if (['imported_unverified', 'partial', 'unknown', 'failed_before_execute', 'manual_done'].includes(to)) {
      if (from !== 'importing' || r.run_id !== runId) fail('bad_transition', `${from} → ${to} はできない`);
      if (to === 'imported_unverified' && r.run_by !== 'auto') fail('bad_transition', '手の ③ の成功は manual_done');
      if (to === 'manual_done' && r.run_by !== 'manual_daily') fail('bad_transition', '自動の成功は imported_unverified');
      const next = to === 'failed_before_execute' ? (r.prev_state || 'idle') : to === 'manual_done' ? 'idle' : to;
      const d = r.run_detail ? JSON.parse(r.run_detail) : {};
      set({ state: next, run_detail: JSON.stringify({ ...d, result: to, result_detail: detail, result_at: now }) });
    } else if (to === 'verified' || to === 'verify_failed') {
      if (from !== 'imported_unverified' || r.run_id !== runId) fail('bad_transition', `${from} → ${to} はできない`);
      const d = r.run_detail ? JSON.parse(r.run_detail) : {};
      set({ state: to, run_detail: JSON.stringify({ ...d, verify: to, verify_detail: detail, verify_at: now }) });
    } else fail('bad_request', `知らない行き先: ${to}`, 400);
    event(db, now, 'transition', runId, by, { from, to, detail });
    return { state: row(db).state };
  })();
}

/** 起動したときに importing が残っている (鍵は切れている) = 結果が分からない = unknown に移す (自動では二度と押さない。H4) */
export function markUnknown(db, { runId, by, reason = null, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  return db.transaction(() => {
    const r = mustRow(db);
    if (r.state !== 'importing' || r.run_id !== runId) fail('bad_transition', `unknown にできるのは importing の回だけ (今 = ${r.state}・${r.run_id})`);
    if (lockActive(r, now)) fail('busy', 'まだ鍵が生きている = 動いている途中かもしれない');
    const d = r.run_detail ? JSON.parse(r.run_detail) : {};
    update(db, now, { state: 'unknown', run_detail: JSON.stringify({ ...d, result: 'unknown', result_detail: { reason }, result_at: now }) });
    event(db, now, 'mark_unknown', runId, by, { reason });
    return { state: 'unknown' };
  })();
}

/**
 * 解除 (人がロジザードのインポート履歴を確かめてから。H6)。unknown / partial / verify_failed → idle (halted はそのまま)。
 * partial (状態か outcome) は partial_check (対象外の列に差が無い・差のある商品が全部次の夜の対象にある) か repaired (人が直した) が要る。
 */
export function resolve(db, { runId, outcome, note, by, partialCheck = null, repaired = false, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  if (!['imported', 'not_imported', 'partial'].includes(outcome)) fail('bad_request', 'outcome は imported / not_imported / partial', 400);
  if (!note || String(note).trim().length < 4) fail('bad_request', 'note (何を確かめたか) が要る', 400);
  return db.transaction(() => {
    const r = mustRow(db);
    if (!['unknown', 'partial', 'verify_failed'].includes(r.state)) fail('bad_transition', `解除するものが無い (今 = ${r.state})`);
    if (r.run_id !== runId) fail('run_mismatch', `解除するのは ${r.run_id}`);
    if (r.state === 'partial' || outcome === 'partial') {
      const okCheck = partialCheck && partialCheck.non_target_unchanged === true && partialCheck.all_in_next_csv === true;
      if (!okCheck && repaired !== true) fail('partial_unchecked', '一部だけの取込の解除には、対象外の列に差が無いこと・差のある商品が全部次の夜の対象にあること (partial_check) か、人が直したこと (repaired) が要る');
    }
    const d = r.run_detail ? JSON.parse(r.run_detail) : {};
    update(db, now, { state: 'idle', run_detail: JSON.stringify({ ...d, resolved: { outcome, note, by, partial_check: partialCheck, repaired, at: now, from: r.state } }) });
    event(db, now, 'resolve', runId, by, { from: r.state, outcome, note, partial_check: partialCheck, repaired });
    return { state: 'idle' };
  })();
}

/** 自動の取込を止める (戻し方の手の ③ の前・人の判断) */
export function halt(db, { by, reason, now = Date.now() }) {
  checkBy(by);
  if (!reason || String(reason).trim().length < 4) fail('bad_request', 'reason が要る', 400);
  return db.transaction(() => {
    mustRow(db);
    update(db, now, { halted: 1, halted_reason: String(reason).slice(0, 300), halted_by: by, halted_at: now });
    event(db, now, 'halt', null, by, { reason });
    return { halted: true };
  })();
}

/** 自動の取込を再開する = 未解決の取込が無い (state が idle / verified)・鍵が空いている ときだけ (H6) */
export function resume(db, { by, note, now = Date.now() }) {
  checkBy(by);
  if (!note || String(note).trim().length < 4) fail('bad_request', 'note が要る', 400);
  return db.transaction(() => {
    const r = mustRow(db);
    if (UNRESOLVED.has(r.state)) fail('state', `未解決の取込がある (${r.state}・${r.run_id})。先に resolve`);
    if (lockActive(r, now)) fail('busy', '鍵をほかが持っている');
    update(db, now, { halted: 0, halted_reason: null, halted_by: null, halted_at: null });
    event(db, now, 'resume', null, by, { note });
    return { halted: false };
  })();
}

/** 止まったことを GChat に送れた (送れていなければ次の回で再送する。H9) */
export function markNotified(db, { runId, by, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  return db.transaction(() => {
    const r = mustRow(db);
    if (r.run_id !== runId) fail('run_mismatch', `今の回は ${r.run_id}`);
    update(db, now, { notified_at: now });
    event(db, now, 'notified', runId, by, null);
    return { notified_at: now };
  })();
}
