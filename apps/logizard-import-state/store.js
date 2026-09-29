/**
 * logizard-import-state/store.js — ロジザードの毎日の商品マスタの取込の「状態」(マスタ正本切替 ③c-1b-1)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b 契約 v3」H1・H4・H5・H6。
 * 自動の ③ (miniPC の 00:20) と少数件の試験が **この 1 つの状態と鍵を共用**する (ローカルの状態と真偽だけの旗を後で合わせる作りにしない)。
 * **③c-1b-3b 契約 (v4 + 設計 R1・2026-09-29)**: 戻し方は「人がどの端末でもブラウザでロジザードに取り込む」= 手の取込 (manual session)。
 *   旧い手の ③ (持ち主 manual_daily・Stream Deck の PC で押す) はやめた (鍵を取れない)。
 *
 * 持つもの (1 行 = id 1):
 *   init_id   初期化の識別子 (各 PC のローカルの印と照合する。H5)
 *   state     idle / importing / imported_unverified / verified / unknown / partial / verify_failed
 *   halted    自動の取込を人が止めた旗 (state とは別。手の取込はこれが立っているときだけ始められる)
 *   run_*     いまの (最後の) 取込の実行 ID・誰が (auto)・中身 (CSV の sha256・行数など)
 *   lock_*    鍵 (持ち主・目的 import / verify・実行 ID・期限)。**鍵が切れても state は戻らない** (H1)
 * 出来事 (events) は追記だけ (更新・削除はトリガーで断る)。
 *
 * 状態の動き (H4・H6):
 *   idle|verified --(鍵 import・CSV の sha256 と行数)--> importing          … 実行ボタンを押す直前に書く
 *   importing --> imported_unverified (成功を読んだ。自動も手の ③ も) | partial | unknown | failed_before_execute (→ 前の状態)
 *   imported_unverified --(鍵 verify か同じ回の import。取り込んだ側が確かめる)--> verified | verify_failed
 *   **手の ③ も確かめ (verified) を通る** (契約 v3 H4 に例外は無い。Codex #1513 R1 High)
 *   importing で鍵が切れたまま = markUnknown で unknown (起動したときに importing が残っている = 自動では二度と押さない)
 *   unknown | partial | verify_failed --resolve (人が履歴を確かめて)--> idle
 * 始めてよい条件:
 *   自動 (auto)       import = halted でない かつ state が idle / verified
 *   (旧い手の ③ manual_daily はやめた = 断る)
 *   確かめ (verify)    state が imported_unverified かつ run_by が同じ持ち主・実行 ID が同じ
 * 鍵を無効にする (Codex #1513 R1): resolve・markUnknown は鍵を消す (解除した回を古い鍵で再開させない)。
 *   importing に進むのは**期限内の鍵**・**今の初期化の世代で取った鍵**・**まだ始めていない鍵** (1 つの鍵で始めるのは 1 回だけ。Codex #1513 R2) だけ。
 *   recover はまだ始めていない鍵を消す。
 * **一度始めた実行 ID は二度と使えない** (import_runs に残す。鍵を取るとき・始めるときに断る = 古い resolve などの要求が、同じ実行 ID の新しい回に当たらない。Codex #1513 R3)
 * 断りの文言に、送られてきた値 (init_id・行き先・持ち主など) を入れない (決まった文言とポータルが持つ値だけ。Codex #1513 R2 Low)
 *
 * ③c-1b-2b 契約 v3 (2026-09-28):
 *   K9 知らせ済みは「状態」と「状態を変えた出来事の番号 (state_event_id)」に結ぶ。状態が変わるたびに知らせ済みは消える。
 *      notified は今の状態と出来事の番号が送られてきたものと同じときだけ (古い知らせの完了で新しい状態を知らせ済みにしない)。
 *   E  importing の詳細に mode (nightly / test / manual) と target_as_of (対象の日) が要る。開始の履歴 (import_runs) に残す。
 *      **nightly は同じ対象の日に 1 回だけ** (resolve の後も。手元の済みの印に頼らない)。test は数えない。
 *
 * ③c-1b-3b 契約 (v4 + 設計 R1) — 手の取込・再適用待ち・毎晩の成果物・知らせ・設定:
 *   手の取込 (manual_sessions): open → completed_ok | needs_review | cancelled。始める = halted・state idle / verified・生きた鍵なし・開いた手の取込なし・
 *     確認待ちの needs_review なし (1 つの取引)。CSV は毎晩の成果物 (判定 pass) か、移行の段階 (cutover_phase = transition) の間だけ GAS の CSV。
 *     CSV の中身・識別・使うロジザードのアカウント (登録済みの一覧から) を固定して持つ (人はこの CSV をダウンロードしてロジザードに置く)。
 *     終える = 結果の文・ロジザードの履歴 (ファイル名 = 出した名前・日時 = 始めた後・アカウント = 固定したもの) が全部合う + 結果が成功 = completed_ok /
 *     それ以外 = needs_review (管理者の確認 ack まで resume できない)。開いている間 = 自動の鍵を取れない・resume できない。
 *   再適用待ち = (手の取込, 商品) ごとの義務 (reapply_obligations)。閉じる = reapply_closures (reapplied = 毎晩の取込が verified になった取引の中で、
 *     その回の importing より前にあった義務のうち、その回の成果物にある商品だけ / waived = 人が理由を書いて特定の義務だけ)。どちらも追記だけ。
 *   毎晩の成果物 (daily_artifacts): バイト列から sha256・行数・CSV の形を計算し直して受け取る (申告を信じない)。同じ source_run_id で中身が違う = 断る。
 *     **nightly の importing は同じ識別の成果物 (判定 pass) があるときだけ** (K3-1)。
 *   知らせの outbox: halt・残った再適用待ち・needs_review を同じ取引で積む (送るのは定時の入口と画面。送れた = sent_at)。
 *   設定: cutover_phase (無い → transition → cutover の一方通行・無い / cutover = GAS の CSV を断る)・lz_accounts (手の取込で使うロジザードのアカウント)。
 *   **機能の旗 LZ_MANUAL_V4=on** (Codex #1537 R1 High): 立つまでは今までの動き (旧い手の ③ manual_daily を使える・nightly に成果物は要らない・
 *     手の取込 / 義務 / waiver は断る = disabled)。成果物の受け取り・設定・outbox は旗に依らない (切替の前から成果物を貯める)。
 *     旗を立てるのは、成果物の受け口・画面・毎晩の本番がそろった切替のとき (manual_daily の拒否と成果物の必須を同時に)。
 *     旗を外した後も、もう開いている / 確認待ちの手の取込は終える・取り消す・確認できる (片付け。新しく始めるのと waiver は旗が要る)。
 *
 * ③c-1b-2b-2 契約 v3 (毎晩の本番・2026-09-30) — ポータル側 (2b-2a-1):
 *   N1 時刻の元は Render の時計: status の clock (server_now・JST の日・expected_target_as_of = JST の前の日・始めてよい窓 [00:15, 00:50)・
 *      nightly_deadline_at = 00:55)。nightly の importing はこの時計で 窓・対象の日 = 前の日・実行 ID の形 (lzim_night_…) を照らす。
 *      nightly の回の確かめのやり直しの鍵 (acquire verify) も同じ窓。
 *   N4 副作用の無い nightly-readiness (本当の nightly と同じ照らしの関数と順番 = autoImportProblems → nightlyFormatProblems → nightlyProblems)。
 *      始められるか = ready (口の ok は通信の成功)。clock は status (未初期化も)・鍵を取る / 延ばす応答にも。
 *   N5 outbox は止め・要確認を先に、再適用待ちを後に。N6 nightly_last = 最後の nightly の回の履歴 (出来事から・今の状態をそのまま付けない)。
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import { validateImportCsv, parseImportResult, judgeImportResult } from '../master-decisions/lz-import-check.mjs';

export const STATES = Object.freeze(['idle', 'importing', 'imported_unverified', 'verified', 'unknown', 'partial', 'verify_failed']);
export const HOLDERS = Object.freeze(['auto']);   // 旧い手の ③ (manual_daily) はやめた (③c-1b-3b v4)
export const MAX_TTL_SEC = 600;
export const MODES = Object.freeze({ auto: ['nightly', 'test'] });
export const SESSION_STATUSES = Object.freeze(['open', 'completed_ok', 'needs_review', 'cancelled']);
export const CUTOVER_PHASES = Object.freeze(['transition', 'cutover']);
export const LIMITS = Object.freeze({ csvBytes: 4 * 1024 * 1024, rows: 20000, resultText: 2000, note: 500, fileName: 200, waive: 20000 });
const ARTIFACT_KEEP_MS = 14 * 86400000;
const SOURCE_RUN_RE = /^lzd_[0-9A-Za-z_]{1,80}$/;
const ACCOUNT_RE = /^[^\u0000-\u001f\u007f]{1,60}$/;
/** ③c-1b-3b v4 の旗 (呼ぶたびに読む = 試験で切り替えられる) */
export const v4On = () => String(process.env.LZ_MANUAL_V4 || '').trim().toLowerCase() === 'on';
const LEGACY_HOLDERS = Object.freeze(['auto', 'manual_daily']);
const LEGACY_MODES = Object.freeze({ auto: ['nightly', 'test'], manual_daily: ['manual'] });
const MINUTE = 60000;
/** 知らせに書く画面の場所 (ダッシュボードのカードは作らない = 知らせから開く。③c-1b-3b-4b) */
export const ADMIN_PAGE_URL = 'https://bfaith-portal.onrender.com/apps/logizard-import-state/admin';
const PRIVATE_EVENTS = new Set(['manual_open', 'manual_complete', 'manual_cancel', 'manual_ack', 'setting', 'reapply_waive']);
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const UNRESOLVED = new Set(['importing', 'imported_unverified', 'unknown', 'partial', 'verify_failed']);
/** 毎晩の本番の時刻 (JST・分)。始めてよい = [startFrom, startTo)・締め切り = deadline (③c-1b-2b-2 N1) */
export const NIGHTLY = Object.freeze({ startFromMin: 15, startToMin: 50, deadlineMin: 55 });
/** 毎晩の本番の実行 ID の形 (エンジンの newRunId = lzim_night_ + UTC の YYYYMMDDTHHMMSS + _ + 16 進 6 桁) */
export const NIGHTLY_RUN_RE = /^lzim_night_\d{8}T\d{6}_[0-9a-f]{6}$/;
const JST_MS = 9 * 3600000;
/**
 * Render の時計で見た毎晩の時刻 (N1)。miniPC はこれを単調な時計に写して、窓・対象の日・締め切りを同じ時計で見る。
 * @returns {{ server_now, jst_date, expected_target_as_of, start_window: { from, to }, nightly_deadline_at, in_start_window }}
 */
export function nightlyClock(now) {
  const t = Number(now);
  const jstDay = new Date(t + JST_MS).toISOString().slice(0, 10);
  const dayStart = Date.parse(`${jstDay}T00:00:00Z`) - JST_MS;   // その日の JST 00:00 (ms)
  const from = dayStart + NIGHTLY.startFromMin * MINUTE, to = dayStart + NIGHTLY.startToMin * MINUTE;
  return {
    server_now: t, jst_date: jstDay, expected_target_as_of: new Date(dayStart - 1 + JST_MS).toISOString().slice(0, 10),
    start_window: { from, to }, nightly_deadline_at: dayStart + NIGHTLY.deadlineMin * MINUTE, in_start_window: t >= from && t < to,
  };
}

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
      lock_init_id    TEXT,
      lock_started    INTEGER NOT NULL DEFAULT 0,
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
    CREATE TABLE IF NOT EXISTS import_runs (
      run_id      TEXT PRIMARY KEY,
      by          TEXT NOT NULL,
      started_at  INTEGER NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS import_runs_no_update BEFORE UPDATE ON import_runs BEGIN SELECT RAISE(ABORT, 'import_runs は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS import_runs_no_delete BEFORE DELETE ON import_runs BEGIN SELECT RAISE(ABORT, 'import_runs は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS import_events_no_update BEFORE UPDATE ON import_events BEGIN SELECT RAISE(ABORT, 'import_events は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS import_events_no_delete BEFORE DELETE ON import_events BEGIN SELECT RAISE(ABORT, 'import_events は追記だけ'); END;
  `);
  // 列を足す (③c-1b-2b 契約 v3 K9・E。前からある表にも足す = Render の今の DB)
  const cols = (t) => new Set(db.prepare(`PRAGMA table_info(${t})`).all().map((c) => c.name));
  const st = cols('import_state');
  if (!st.has('state_event_id')) db.exec('ALTER TABLE import_state ADD COLUMN state_event_id INTEGER');
  if (!st.has('notified_for')) db.exec('ALTER TABLE import_state ADD COLUMN notified_for INTEGER');
  if (!st.has('halted_event_id')) db.exec('ALTER TABLE import_state ADD COLUMN halted_event_id INTEGER');   // 止めの番号 (画面が見ていた止めと今の止めを照らす。Codex #1542 R1 High)
  // 止まっているのに番号が無い (前からの止め) = 番号を付ける (付けないと再開も手の取込も番号を照らせない。Codex #1542 R2 Medium)
  const hv = db.prepare('SELECT halted, halted_event_id FROM import_state WHERE id = 1').get();
  if (hv && hv.halted && hv.halted_event_id == null) {
    db.transaction(() => {
      const id = Number(db.prepare('INSERT INTO import_events (at, kind, run_id, by, detail) VALUES (?, ?, NULL, ?, ?)').run(Date.now(), 'halt_revision', 'migration', JSON.stringify({ note: '前からの止めに番号を付けた' })).lastInsertRowid);
      db.prepare('UPDATE import_state SET halted_event_id = ? WHERE id = 1').run(id);
    })();
  }
  const ir = cols('import_runs');
  for (const c of ['mode', 'target_as_of', 'source_run_id']) if (!ir.has(c)) db.exec(`ALTER TABLE import_runs ADD COLUMN ${c} TEXT`);
  // ③c-1b-3b 契約 (v4 + 設計 R1): 手の取込・再適用待ちの義務・毎晩の成果物・毎晩の取込の義務の区切り・知らせの outbox・設定 (前からある DB にも足す・何度開いても同じ)
  db.exec(`
    CREATE TABLE IF NOT EXISTS manual_sessions (
      session_id    TEXT PRIMARY KEY,
      status        TEXT NOT NULL CHECK (status IN ('open', 'completed_ok', 'needs_review', 'cancelled')),
      opened_by     TEXT NOT NULL,
      opened_at     INTEGER NOT NULL,
      lz_account    TEXT NOT NULL,
      source_kind   TEXT NOT NULL CHECK (source_kind IN ('cdb_artifact', 'gas_upload')),
      source_run_id TEXT,
      target_as_of  TEXT NOT NULL,
      csv_sha256    TEXT NOT NULL,
      rows          INTEGER NOT NULL,
      csv           BLOB NOT NULL,
      download_name TEXT NOT NULL,
      closed_by     TEXT,
      closed_at     INTEGER,
      close_detail  TEXT,
      ack_by        TEXT,
      ack_at        INTEGER,
      ack_note      TEXT,
      updated_at    INTEGER NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS manual_sessions_no_delete BEFORE DELETE ON manual_sessions BEGIN SELECT RAISE(ABORT, 'manual_sessions は消さない'); END;
    CREATE TRIGGER IF NOT EXISTS manual_sessions_fixed BEFORE UPDATE ON manual_sessions
      WHEN NEW.session_id IS NOT OLD.session_id OR NEW.opened_by IS NOT OLD.opened_by OR NEW.opened_at IS NOT OLD.opened_at OR NEW.lz_account IS NOT OLD.lz_account
        OR NEW.source_kind IS NOT OLD.source_kind OR NEW.source_run_id IS NOT OLD.source_run_id OR NEW.target_as_of IS NOT OLD.target_as_of
        OR NEW.csv_sha256 IS NOT OLD.csv_sha256 OR NEW.rows IS NOT OLD.rows OR NEW.csv IS NOT OLD.csv OR NEW.download_name IS NOT OLD.download_name
        OR (OLD.status <> 'open' AND NEW.status IS NOT OLD.status)
        OR (OLD.status = 'open' AND NEW.status <> 'open' AND (NEW.closed_at IS NULL OR NEW.closed_by IS NULL OR NEW.close_detail IS NULL))
        OR (NEW.status = 'open' AND (NEW.closed_at IS NOT NULL OR NEW.closed_by IS NOT NULL OR NEW.close_detail IS NOT NULL))
        OR (OLD.closed_at IS NOT NULL AND (NEW.closed_by IS NOT OLD.closed_by OR NEW.closed_at IS NOT OLD.closed_at OR NEW.close_detail IS NOT OLD.close_detail))
        OR (OLD.closed_at IS NULL AND NEW.closed_at IS NOT NULL AND NEW.status = 'open')
        OR (OLD.ack_at IS NOT NULL AND (NEW.ack_by IS NOT OLD.ack_by OR NEW.ack_at IS NOT OLD.ack_at OR NEW.ack_note IS NOT OLD.ack_note))
        OR (OLD.ack_at IS NULL AND (NEW.ack_by IS NOT NULL OR NEW.ack_note IS NOT NULL OR NEW.ack_at IS NOT NULL)
            AND (NEW.status <> 'needs_review' OR NEW.ack_at IS NULL OR NEW.ack_by IS NULL OR NEW.ack_note IS NULL))
      BEGIN SELECT RAISE(ABORT, 'manual_sessions の識別・閉じた状態・閉じと確認の記録は変えない (1 回だけ一式で)'); END;
    CREATE TABLE IF NOT EXISTS reapply_obligations (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id  TEXT NOT NULL,
      product_id  TEXT NOT NULL,
      created_at  INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS reapply_obligations_product ON reapply_obligations (product_id);
    CREATE TRIGGER IF NOT EXISTS reapply_obligations_no_update BEFORE UPDATE ON reapply_obligations BEGIN SELECT RAISE(ABORT, 'reapply_obligations は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS reapply_obligations_no_delete BEFORE DELETE ON reapply_obligations BEGIN SELECT RAISE(ABORT, 'reapply_obligations は追記だけ'); END;
    CREATE TABLE IF NOT EXISTS reapply_closures (
      obligation_id INTEGER PRIMARY KEY,
      kind          TEXT NOT NULL CHECK (kind IN ('reapplied', 'waived')),
      run_id        TEXT,
      by            TEXT NOT NULL,
      note          TEXT,
      at            INTEGER NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS reapply_closures_no_update BEFORE UPDATE ON reapply_closures BEGIN SELECT RAISE(ABORT, 'reapply_closures は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS reapply_closures_no_delete BEFORE DELETE ON reapply_closures BEGIN SELECT RAISE(ABORT, 'reapply_closures は追記だけ'); END;
    CREATE TABLE IF NOT EXISTS daily_artifacts (
      source_run_id TEXT PRIMARY KEY,
      target_as_of  TEXT NOT NULL,
      verdict       TEXT NOT NULL CHECK (verdict IN ('pass', 'fail')),
      csv_sha256    TEXT NOT NULL,
      rows          INTEGER NOT NULL,
      csv           BLOB NOT NULL,
      received_at   INTEGER NOT NULL,
      received_by   TEXT NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS daily_artifacts_no_update BEFORE UPDATE ON daily_artifacts BEGIN SELECT RAISE(ABORT, 'daily_artifacts は変えない'); END;
    -- 成果物の識別の台帳 (中身の整理の後も残す = 同じ source_run_id の違う中身をいつまでも断る。Codex #1537 R1 High)
    CREATE TABLE IF NOT EXISTS artifact_ledger (
      source_run_id TEXT PRIMARY KEY,
      target_as_of  TEXT NOT NULL,
      verdict       TEXT NOT NULL,
      csv_sha256    TEXT NOT NULL,
      rows          INTEGER NOT NULL,
      first_at      INTEGER NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS artifact_ledger_no_update BEFORE UPDATE ON artifact_ledger BEGIN SELECT RAISE(ABORT, 'artifact_ledger は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS artifact_ledger_no_delete BEFORE DELETE ON artifact_ledger BEGIN SELECT RAISE(ABORT, 'artifact_ledger は追記だけ'); END;
    CREATE TABLE IF NOT EXISTS nightly_snapshots (
      run_id            TEXT PRIMARY KEY,
      source_run_id     TEXT NOT NULL,
      max_obligation_id INTEGER NOT NULL,
      taken_at          INTEGER NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS nightly_snapshots_no_update BEFORE UPDATE ON nightly_snapshots BEGIN SELECT RAISE(ABORT, 'nightly_snapshots は追記だけ'); END;
    CREATE TRIGGER IF NOT EXISTS nightly_snapshots_no_delete BEFORE DELETE ON nightly_snapshots BEGIN SELECT RAISE(ABORT, 'nightly_snapshots は追記だけ'); END;
    CREATE TABLE IF NOT EXISTS outbox (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      kind        TEXT NOT NULL,
      dedupe_key  TEXT NOT NULL UNIQUE,
      text        TEXT NOT NULL,
      created_at  INTEGER NOT NULL,
      sent_at     INTEGER,
      sent_by     TEXT
    );
    CREATE TRIGGER IF NOT EXISTS outbox_no_delete BEFORE DELETE ON outbox BEGIN SELECT RAISE(ABORT, 'outbox は消さない'); END;
    CREATE TRIGGER IF NOT EXISTS outbox_only_sent BEFORE UPDATE ON outbox
      WHEN NEW.id IS NOT OLD.id OR NEW.kind IS NOT OLD.kind OR NEW.dedupe_key IS NOT OLD.dedupe_key OR NEW.text IS NOT OLD.text OR NEW.created_at IS NOT OLD.created_at
        OR OLD.sent_at IS NOT NULL OR NEW.sent_at IS NULL OR NEW.sent_by IS NULL
      BEGIN SELECT RAISE(ABORT, 'outbox は送れた印 (sent_at と sent_by を一緒に) だけ・1 回だけ'); END;
    CREATE TABLE IF NOT EXISTS settings (
      key         TEXT PRIMARY KEY,
      value       TEXT NOT NULL,
      updated_by  TEXT NOT NULL,
      updated_at  INTEGER NOT NULL
    );
  `);
  return db;
}

function row(db) { return db.prepare('SELECT * FROM import_state WHERE id = 1').get() || null; }
const runUsed = (db, runId) => !!db.prepare('SELECT 1 FROM import_runs WHERE run_id = ?').get(runId);
function event(db, now, kind, runId, by, detail) {
  return Number(db.prepare('INSERT INTO import_events (at, kind, run_id, by, detail) VALUES (?, ?, ?, ?, ?)').run(now, kind, runId ?? null, by ?? null, detail == null ? null : JSON.stringify(detail)).lastInsertRowid);
}
/** 状態を変えた出来事 = 知らせ済みを消して、その出来事の番号を持つ (K9) */
function stateEvent(db, now, kind, runId, by, detail) {
  const id = event(db, now, kind, runId, by, detail);
  update(db, now, { state_event_id: id, notified_at: null, notified_for: null });
  return id;
}
function update(db, now, fields) {
  const keys = Object.keys(fields);
  db.prepare(`UPDATE import_state SET ${keys.map((k) => `${k} = @${k}`).join(', ')}, updated_at = @__now WHERE id = 1`).run({ ...fields, __now: now });
}
const lockActive = (r, now) => !!(r && r.lock_token && r.lock_expires_at > now);
function mustRow(db) { const r = row(db); if (!r) fail('not_initialized', 'まだ初期化していない (--init)', 404); return r; }
function checkInit(r, initId) { if (!initId || initId !== r.init_id) fail('init_mismatch', `初期化の識別子が違う (ポータル = ${r.init_id})。記録の消失か設定違い = 止める`); }
function checkBy(by) { if (!BY_RE.test(String(by || ''))) fail('bad_request', 'by (誰が) が要る', 400); }
function checkRunId(runId) { if (!RUN_ID_RE.test(String(runId || ''))) fail('bad_request', '実行 ID の形が違う', 400); }
/** メモ・理由 = 文字 (オブジェクト・配列・数は断る)・4〜LIMITS.note 文字 (Codex #1541 R1 Medium) */
function checkNote(note, what = 'note') { if (typeof note !== 'string' || note.trim().length < 4 || note.length > LIMITS.note) fail('bad_request', `${what} (4〜${LIMITS.note} 文字の文字列) が要る`, 400); }
/** 省けるメモ・理由 (null / undefined = 無し。あれば文字で上限まで) */
function checkOptText(v, what) { if (v != null && (typeof v !== 'string' || v.length > LIMITS.note)) fail('bad_request', `${what} は ${LIMITS.note} 文字までの文字列`, 400); }
const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');
const isRealDate = (x) => DATE_RE.test(String(x)) && new Date(`${x}T00:00:00Z`).toISOString().slice(0, 10) === x;
const jstDate = (ms) => new Date(ms + 9 * 3600000).toISOString().slice(0, 10);
/** 取り込む CSV (毎日の形) を読む。壊れている・大きすぎる = 断る */
function readDailyCsv(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 1 || buf.length > LIMITS.csvBytes) fail('bad_request', `CSV は 1〜${LIMITS.csvBytes} バイト`, 400);
  const v = validateImportCsv(buf);
  if (!v.ok) fail('bad_csv', `CSV の形が違う (${v.reason})`, 400);
  if (v.table.length > LIMITS.rows) fail('bad_csv', `CSV の行が多すぎる (${LIMITS.rows} まで)`, 400);
  return v.table;
}
function settingOf(db, key, dflt) { const r = db.prepare('SELECT value FROM settings WHERE key = ?').get(key); return r ? JSON.parse(r.value) : dflt; }
const openSessionOf = (db) => db.prepare("SELECT * FROM manual_sessions WHERE status = 'open'").get() || null;
const unackedReviewOf = (db) => db.prepare("SELECT * FROM manual_sessions WHERE status = 'needs_review' AND ack_at IS NULL ORDER BY opened_at LIMIT 1").get() || null;
const OPEN_OBLIGATIONS = 'FROM reapply_obligations o LEFT JOIN reapply_closures c ON c.obligation_id = o.id WHERE c.obligation_id IS NULL';
const openObligationCount = (db) => db.prepare(`SELECT COUNT(*) AS n ${OPEN_OBLIGATIONS}`).get().n;
/** 残っている義務の指紋 (番号だけ読む) */
const openFingerprint = (db) => sha256(Buffer.from(db.prepare(`SELECT o.id ${OPEN_OBLIGATIONS} ORDER BY o.id`).all().map((o) => o.id).join(','))).slice(0, 16);
function needV4() { if (!v4On()) fail('disabled', '手の取込 (③c-1b-3b v4) はまだ使えない (LZ_MANUAL_V4 が立っていない)'); }
/** 知らせを積む (同じ dedupe_key は 1 回だけ) */
function outboxPut(db, now, kind, dedupeKey, text) {
  db.prepare('INSERT OR IGNORE INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run(kind, dedupeKey, String(text).slice(0, 3000), now);
  return db.prepare('SELECT id FROM outbox WHERE dedupe_key = ?').get(dedupeKey).id;   // 今回積んだ (か前に積んだ同じ) 知らせの番号 = 画面の口が真っ先に送る
}
const sessionMeta = (x) => (x ? { session_id: x.session_id, status: x.status, opened_by: x.opened_by, opened_at: x.opened_at, lz_account: x.lz_account, source_kind: x.source_kind,
  source_run_id: x.source_run_id, target_as_of: x.target_as_of, csv_sha256: x.csv_sha256, rows: x.rows, download_name: x.download_name,
  closed_by: x.closed_by, closed_at: x.closed_at, close_detail: x.close_detail ? JSON.parse(x.close_detail) : null, ack_by: x.ack_by, ack_at: x.ack_at, ack_note: x.ack_note } : null);

/**
 * 最後の毎晩の回 (N6): 開始の履歴 (import_runs) と、その実行 ID の出来事から。今の状態 (import_state) をそのまま付けない
 * (その後に試験の回が走った・解除した でも、その夜の回の結末を返す)。
 * last_state = その回の最後の状態の動き (transition の行き先・mark_unknown = unknown)・resolved = その回を解除したか (真偽)・resolution = 解除の中身 (outcome・from)
 */
function nightlyLast(db) {
  const x = db.prepare("SELECT run_id, target_as_of, source_run_id, started_at FROM import_runs WHERE mode = 'nightly' ORDER BY started_at DESC, rowid DESC LIMIT 1").get();
  if (!x) return null;
  let lastState = null, resolution = null;
  for (const e of db.prepare("SELECT kind, detail FROM import_events WHERE run_id = ? AND kind IN ('transition', 'mark_unknown', 'resolve') ORDER BY id").all(x.run_id)) {
    const d = e.detail ? JSON.parse(e.detail) : {};
    if (e.kind === 'transition') lastState = d.to;
    else if (e.kind === 'mark_unknown') lastState = 'unknown';
    else resolution = { outcome: d.outcome ?? null, from: d.from ?? null };
  }
  return { run_id: x.run_id, target_as_of: x.target_as_of, source_run_id: x.source_run_id, started_at: x.started_at, last_state: lastState, resolved: resolution !== null, resolution };
}

/**
 * 毎晩の本番を始められるか (副作用なし・N4)。本当の nightly と同じ照らし (autoImportProblems + nightlyProblems) の結果を全部返す
 * (止めている間にも「成果物の無い nightly が断られる」を確かめられる = 切替の手順)
 */
export function nightlyReadiness(db, { sourceRunId, csvSha256, rows, targetAsOf, now = Date.now() }) {
  const r = mustRow(db);
  // 本当の道と同じ順番: 鍵の照らし (acquire) → 識別の形 → 毎晩の照らし (transition)。形が違う = codes に bad_request (投げない。Codex #1546 R1 Medium)
  const ident = { target: targetAsOf, sourceRunId, csvSha256, rows };
  const fmt = nightlyFormatProblems(ident);
  const problems = [...autoImportProblems(db, r, now), ...fmt, ...(fmt.length ? [] : nightlyProblems(db, ident, now))];
  const a = db.prepare('SELECT source_run_id, target_as_of, verdict, csv_sha256, rows FROM daily_artifacts WHERE source_run_id = ?').get(String(sourceRunId ?? ''));
  return {
    // ready = 始められるか (口の ok は通信の成功 = 別。Codex #1546 R1 High の直し方 = 名前を分ける。契約 v3 N4 の文言も ready に)
    ready: problems.length === 0, codes: problems.map((p) => p[0]), messages: problems.map((p) => p[1]),
    manual: { v4: v4On() }, cutover_phase: getSettings(db).cutover_phase, clock: nightlyClock(now),   // manual は status と同じ形・設定の読み方は画面と同じ (無い = cutover)
    state: r.state, halted: !!r.halted, lock_active: lockActive(r, now), manual_open: !!openSessionOf(db),
    nightly_started: !!db.prepare("SELECT 1 FROM import_runs WHERE mode = 'nightly' AND target_as_of = ?").get(String(targetAsOf ?? '')),
    artifact: a ? { found: true, verdict: a.verdict, same: a.csv_sha256 === csvSha256 && a.rows === rows && a.target_as_of === targetAsOf } : { found: false, verdict: null, same: false },
  };
}

/** 見る (鍵の期限が切れていれば lock は null) */
export function getStatus(db, { now = Date.now(), events = 20, reveal = false } = {}) {
  const r = row(db);
  // 手の取込・設定・waiver の出来事は、機械の口では種類と時刻だけ (誰・アカウント・メモは画面の口。Codex #1537 R1 Medium)
  const ev = db.prepare('SELECT * FROM import_events ORDER BY id DESC LIMIT ?').all(Math.max(0, Math.min(200, events)))
    .map((e) => (!reveal && PRIVATE_EVENTS.has(e.kind) ? { ...e, by: null, detail: null } : { ...e, detail: e.detail ? JSON.parse(e.detail) : null }));   // reveal = 画面の口 (管理者) だけ
  if (!r) return { initialized: false, events: ev, clock: nightlyClock(now) };   // 時計はまだ初期化していなくても (N1)
  return {
    initialized: true, init_id: r.init_id, state: r.state, halted: !!r.halted, halted_reason: r.halted_reason, halted_by: r.halted_by, halted_at: r.halted_at,
    halt_revision: r.halted ? (r.halted_event_id ?? null) : null,
    run: r.run_id ? { run_id: r.run_id, by: r.run_by, detail: r.run_detail ? JSON.parse(r.run_detail) : null } : null,
    lock: lockActive(r, now) ? { holder: r.lock_holder, purpose: r.lock_purpose, run_id: r.lock_run_id, expires_at: r.lock_expires_at } : null,
    lock_expired: !!(r.lock_token && !lockActive(r, now)),
    state_event_id: r.state_event_id ?? null,
    notified: r.notified_for != null && r.notified_for === r.state_event_id,   // 今の状態を知らせたか (K9)
    notified_at: r.notified_at, updated_at: r.updated_at, events: ev,
    clock: nightlyClock(now),   // 時刻の元 = Render の時計 (N1)
    nightly_last: nightlyLast(db),   // 最後の毎晩の回の履歴 (N6)
    // 機械の口 (Bearer) には数と真偽だけ (誰・アカウント・メモ・設定は画面の口 = 3b-4)
    manual: {
      v4: v4On(),
      open: !!openSessionOf(db),
      needs_review_unacked: !!unackedReviewOf(db),
      pending_reapply: openObligationCount(db),
      outbox_unsent: db.prepare('SELECT COUNT(*) AS n FROM outbox WHERE sent_at IS NULL').get().n,
    },
  };
}

/** 初期化 (まだ無いときだけ。あれば断る = 履歴や消失を上書きしない。H5) */
export function init(db, { by, note = null, now = Date.now() }) {
  checkBy(by);
  return db.transaction(() => {
    if (row(db)) fail('already_initialized', 'もう初期化してある (消失からの復旧は recover)');
    // 状態の行が無くても、出来事 (履歴) が残っている = 状態の行が消えた = 初回ではない (Codex #1513 R1 Medium)
    const n = db.prepare('SELECT COUNT(*) AS n FROM import_events').get().n;
    if (n > 0) fail('history_exists', `状態は無いが履歴が ${n} 件ある = 消失。ロジザードの履歴を確かめて recover`);
    const initId = rid('lzi', now);
    db.prepare('INSERT INTO import_state (id, init_id, state, halted, updated_at) VALUES (1, ?, ?, 0, ?)').run(initId, 'idle', now);
    stateEvent(db, now, 'init', null, by, { init_id: initId, note });
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
  checkNote(note, 'recover の note (何を確かめたか)');
  return db.transaction(() => {
    const r = row(db);
    const initId = rid('lzi', now);
    if (!r) {
      db.prepare('INSERT INTO import_state (id, init_id, state, halted, halted_reason, halted_by, halted_at, updated_at) VALUES (1, ?, ?, 1, ?, ?, ?, ?)')
        .run(initId, 'idle', 'recovered: ポータルの状態を作り直した (取込の途中だったか分からない)', by, now, now);
    } else {
      // まだ始めていない鍵 (idle / verified のときの鍵) は消す = 復旧の前のプロセスに始めさせない (Codex #1513 R1 Medium)。
      // 取込の途中 (importing / imported_unverified) の鍵は残す = その回の結果は書ける
      const clearLock = ['idle', 'verified'].includes(r.state) ? { lock_token: null, lock_holder: null, lock_purpose: null, lock_run_id: null, lock_expires_at: null, lock_init_id: null } : {};
      update(db, now, { init_id: initId, ...clearLock });
    }
    // 状態を作り直した (ポータル側の消失) ときだけ状態の出来事 (手元の印の消失 = 状態はそのまま・知らせ済みも変えない)
    const eventId = (r ? event : stateEvent)(db, now, 'recover', null, by, { init_id: initId, prev_init_id: r ? r.init_id : null, note });
    if (!r || r.halted) update(db, now, { halted_event_id: eventId });   // 止めた状態で作った・止めたまま作り直した = 番号を変える (古い画面を stale に)
    // ポータルの状態を作り直した = 自動を止めた = halt と同じ知らせを同じ取引で積む (K3-4。Codex #1537 R2 Medium)
    if (!r) outboxPut(db, now, 'halt', `halt:${eventId}`, `⏸ ロジザードの取込の状態をポータルで作り直した (${by}) = 自動の取込は止めた状態: ${String(note).slice(0, 300)}
ロジザードの履歴を確かめてから resume`);
    return { init_id: initId, halted: !r ? true : !!r.halted };
  })();
}

/**
 * 自動の取込 (持ち主 auto・import) の鍵を取れない理由 (acquire と nightly-readiness で共用 = 同じ照らし。N4)。
 * 順番は acquire の断りの順 (busy → halted → state → manual_open)
 */
function autoImportProblems(db, r, now) {
  const out = [];
  if (lockActive(r, now)) out.push(['busy', `鍵はほかが持っている (${r.lock_holder}・${r.lock_purpose}・${r.lock_run_id}・期限 ${new Date(r.lock_expires_at).toISOString()})`]);
  if (r.halted) out.push(['halted', `自動の取込は止めてある (${r.halted_reason || ''})`]);
  if (!['idle', 'verified'].includes(r.state)) out.push(['state', `始められない状態: ${r.state} (${r.run_id || ''})`]);
  if (openSessionOf(db)) out.push(['manual_open', '手の取込が開いている (終えるか取り消してから)']);
  return out;
}

/**
 * 毎晩の本番の importing を断る理由 (transition と nightly-readiness で共用。N1・N4・E・K3-1)。
 * 実行 ID の形 → Render の時刻の窓 → 対象の日 = 前の日 → 同じ対象の日にもう始めた → (旗が立っていれば) 同じ識別の成果物 (判定 pass)
 * @param {{ target: string, sourceRunId, csvSha256, rows, runId?: string }} p  runId が無い (readiness) = 形は見ない
 */
function nightlyProblems(db, { target, sourceRunId, csvSha256, rows, runId = undefined }, now) {
  const out = [];
  const c = nightlyClock(now);
  if (runId !== undefined && !NIGHTLY_RUN_RE.test(String(runId))) out.push(['bad_run_id', '毎晩の本番の実行 ID の形が違う (lzim_night_…)']);
  if (!c.in_start_window) out.push(['outside_window', `毎晩の本番を始めてよいのは Render の時刻で JST 00:${NIGHTLY.startFromMin}〜00:${NIGHTLY.startToMin} (今 = ${new Date(c.server_now + JST_MS).toISOString().slice(11, 19)})`]);
  if (target !== c.expected_target_as_of) out.push(['stale_target', `毎晩の本番の対象の日は Render の時刻で前の日 (${c.expected_target_as_of}) だけ`]);
  if (db.prepare("SELECT 1 FROM import_runs WHERE mode = 'nightly' AND target_as_of = ?").get(String(target))) out.push(['nightly_done', 'その対象の日の毎晩の取込はもう始めた (1 回だけ)']);
  if (v4On()) {
    const a = db.prepare('SELECT source_run_id, target_as_of, verdict, csv_sha256, rows FROM daily_artifacts WHERE source_run_id = ?').get(String(sourceRunId ?? ''));
    if (!a || a.verdict !== 'pass' || a.csv_sha256 !== csvSha256 || a.rows !== rows || a.target_as_of !== target) out.push(['artifact_missing', '毎晩の取込は、同じ識別 (source_run_id・sha256・行数・対象の日) の成果物 (判定 pass) がポータルにあるときだけ (K3-1)']);
  }
  return out;
}
/** 毎晩の本番の識別の形 (readiness と transition で共用 = 同じ順番。Codex #1546 R1 Medium)。形が違う = 400 bad_request */
function nightlyFormatProblems({ target, sourceRunId, csvSha256, rows }) {
  const bad = [];
  if (!isRealDate(target)) bad.push('target_as_of (実在の日 YYYY-MM-DD)');
  if (!SOURCE_RUN_RE.test(String(sourceRunId ?? ''))) bad.push('source_run_id (lzd_…)');
  if (!/^[0-9a-f]{64}$/.test(String(csvSha256 ?? ''))) bad.push('csv_sha256 (64 桁)');
  if (!Number.isSafeInteger(rows) || rows < 1) bad.push('rows (1 以上の整数)');
  return bad.length ? [['bad_request', `毎晩の本番の識別の形が違う: ${bad.join('・')}`]] : [];
}
const failFirst = (problems) => { if (problems.length) fail(problems[0][0], problems[0][1], ['bad_run_id', 'bad_request'].includes(problems[0][0]) ? 400 : 409); };

/** 鍵を取る (始めてよい条件は上)。返す lock_token は結果を書くときに使う */
export function acquire(db, { initId, holder, purpose, runId, ttlSec = 180, by, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  const v4 = v4On();
  if (v4 && holder === 'manual_daily') fail('retired', '旧い手の ③ (manual_daily) はやめた。手で取り込むときはポータルの画面の「手の取込」で (③c-1b-3b v4)', 400);
  const holders = v4 ? HOLDERS : LEGACY_HOLDERS;
  if (!holders.includes(holder)) fail('bad_request', `holder は ${holders.join(' / ')}`, 400);
  if (!['import', 'verify'].includes(purpose)) fail('bad_request', 'purpose は import / verify', 400);
  const ttl = Math.min(MAX_TTL_SEC, Math.max(30, Math.floor(Number(ttlSec) || 0)));
  return db.transaction(() => {
    const r = mustRow(db);
    checkInit(r, initId);
    if (lockActive(r, now)) fail('busy', `鍵はほかが持っている (${r.lock_holder}・${r.lock_purpose}・${r.lock_run_id}・期限 ${new Date(r.lock_expires_at).toISOString()})`);
    if (holder === 'auto' && purpose === 'import') {
      failFirst(autoImportProblems(db, r, now));   // nightly-readiness と同じ照らし (N4)
    } else if (purpose === 'verify') {
      if (r.state !== 'imported_unverified' || r.run_by !== holder) fail('state', `確かめる取込が無い: ${r.state} (${r.run_by || ''})`);
      if (runId !== r.run_id) fail('run_mismatch', `確かめるのは ${r.run_id}`);
      // 毎晩の回の確かめのやり直しも Render の時刻の窓だけ (昼は知らせだけ = K3-5・N1)
      const rd = r.run_detail ? JSON.parse(r.run_detail) : {};
      if (rd.mode === 'nightly' && !nightlyClock(now).in_start_window) fail('outside_window', `毎晩の回の確かめのやり直しは Render の時刻で JST 00:${NIGHTLY.startFromMin}〜00:${NIGHTLY.startToMin} だけ`);
    } else if (!v4 && holder === 'manual_daily' && purpose === 'import') {
      // (旗が立つまでの今までの動き。一度始めた実行 ID は下で断る)
      if (!r.halted) fail('not_halted', '戻し方の手の ③ は、自動の取込を止めてから (halt)');
      if (!['idle', 'verified'].includes(r.state)) fail('state', `始められない状態: ${r.state} (${r.run_id || ''})`);
    } else fail('bad_request', 'その持ち主と目的の組み合わせはできない', 400);
    if (purpose === 'import' && runUsed(db, runId)) fail('run_used', 'その実行 ID ではもう取込を始めた (新しい実行 ID で)');
    const token = crypto.randomUUID();
    const expires = now + ttl * 1000;
    update(db, now, { lock_token: token, lock_holder: holder, lock_purpose: purpose, lock_run_id: runId, lock_expires_at: expires, lock_init_id: r.init_id, lock_started: 0 });
    event(db, now, 'lock_acquire', runId, by, { holder, purpose, expires_at: expires });
    return { lock_token: token, expires_at: expires, clock: nightlyClock(now) };   // 鍵の応答にも Render の時計 (N1)
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
    return { expires_at: expires, clock: nightlyClock(now) };
  })();
}

/** 鍵を返す (state は変えない) */
export function release(db, { lockToken, by, now = Date.now() }) {
  return db.transaction(() => {
    const r = mustRow(db);
    if (!lockToken || r.lock_token !== lockToken) return { released: false };
    update(db, now, { lock_token: null, lock_holder: null, lock_purpose: null, lock_run_id: null, lock_expires_at: null, lock_init_id: null });
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
      // 始めるのは期限内の鍵・今の初期化の世代で取った鍵だけ (結果は期限切れでも書けるが、始めることはできない。Codex #1513 R1 Medium)
      if (!lockActive(r, now)) fail('lock_lost', '鍵の期限が切れた = 始めない (取り直す)');
      if (r.lock_init_id !== r.init_id) fail('init_mismatch', '鍵を取った後に初期化の識別子が変わった = 始めない');
      if (r.lock_started) fail('lock_used', 'この鍵ではもう始めた (次の取込は鍵を取り直す)');
      if (runUsed(db, runId)) fail('run_used', 'その実行 ID ではもう取込を始めた (新しい実行 ID で)');
      // モードと対象の日 (E)。nightly は同じ対象の日に 1 回だけ (resolve の後も)
      const mode = detail && detail.mode, target = detail && detail.target_as_of;
      const v4 = v4On();
      if (!((v4 ? MODES : LEGACY_MODES)[holder] || []).includes(mode) || !DATE_RE.test(String(target || ''))) fail('bad_request', 'importing には mode (auto = nightly / test・手の ③ = manual) と target_as_of (YYYY-MM-DD) が要る', 400);
      // 毎晩の本番 = 実行 ID の形・Render の時刻の窓・対象の日 = 前の日・1 回だけ・成果物 (N1・E・K3-1)。import_runs に書く前 (断ったら何も残らない)
      if (mode === 'nightly') {
        const ident = { target, sourceRunId: detail.source_run_id, csvSha256: detail.csv_sha256, rows: detail.rows };
        failFirst([...nightlyFormatProblems(ident), ...nightlyProblems(db, { ...ident, runId }, now)]);   // 形 → 毎晩の照らし (readiness と同じ順番)
      }
      db.prepare('INSERT INTO import_runs (run_id, by, started_at, mode, target_as_of, source_run_id) VALUES (?, ?, ?, ?, ?, ?)')
        .run(runId, holder, now, mode, target, detail.source_run_id == null ? null : String(detail.source_run_id).slice(0, 120));
      if (holder === 'auto' && r.halted) fail('halted', '自動の取込は止めてある');
      if (!v4 && holder === 'manual_daily' && !r.halted) fail('not_halted', '自動の取込が止まっていない');
      if (openSessionOf(db)) fail('manual_open', '手の取込が開いている (終えるか取り消してから)');
      if (!detail || !/^[0-9a-f]{64}$/.test(String(detail.csv_sha256 || '')) || !Number.isSafeInteger(detail.rows) || detail.rows < 1) fail('bad_request', 'importing には CSV の sha256 と行数が要る', 400);
      // 毎晩は、同じ識別 (source_run_id・sha256・行数・対象の日) の成果物 (判定 pass) がポータルにあるときだけ (K3-1)。
      // この回の前にあった再適用待ちの義務の区切りを残す (verified でこの区切りまでの義務だけ閉じる。K3-2)
      if (v4 && mode === 'nightly') {
        const a = db.prepare('SELECT source_run_id FROM daily_artifacts WHERE source_run_id = ?').get(String(detail.source_run_id));   // 照らしは上 (nightlyProblems)
        const max = db.prepare('SELECT COALESCE(MAX(id), 0) AS m FROM reapply_obligations').get().m;
        db.prepare('INSERT INTO nightly_snapshots (run_id, source_run_id, max_obligation_id, taken_at) VALUES (?, ?, ?, ?)').run(runId, a.source_run_id, max, now);
      }
      set({ state: 'importing', prev_state: from, run_id: runId, run_by: holder, run_detail: JSON.stringify({ ...detail, started_at: now }), notified_at: null, lock_started: 1 });
    } else if (['imported_unverified', 'partial', 'unknown', 'failed_before_execute'].includes(to)) {
      if (from !== 'importing' || r.run_id !== runId) fail('bad_transition', `${from} → ${to} はできない`);
      const next = to === 'failed_before_execute' ? (r.prev_state || 'idle') : to;
      const d = r.run_detail ? JSON.parse(r.run_detail) : {};
      set({ state: next, run_detail: JSON.stringify({ ...d, result: to, result_detail: detail, result_at: now }) });
    } else if (to === 'verified' || to === 'verify_failed') {
      if (from !== 'imported_unverified' || r.run_id !== runId) fail('bad_transition', `${from} → ${to} はできない`);
      const d = r.run_detail ? JSON.parse(r.run_detail) : {};
      set({ state: to, run_detail: JSON.stringify({ ...d, verify: to, verify_detail: detail, verify_at: now }) });
      if (to === 'verified') closeForNightly(db, now, runId, by);   // 同じ取引 (K3-2)
    } else fail('bad_request', '知らない行き先', 400);
    const eventId = stateEvent(db, now, 'transition', runId, by, { from, to, detail });
    return { state: row(db).state, state_event_id: eventId };
  })();
}

/** 起動したときに importing が残っている (鍵は切れている) = 結果が分からない = unknown に移す (自動では二度と押さない。H4) */
export function markUnknown(db, { runId, by, reason = null, now = Date.now() }) {
  checkBy(by); checkRunId(runId); checkOptText(reason, 'reason');
  return db.transaction(() => {
    const r = mustRow(db);
    if (r.state !== 'importing' || r.run_id !== runId) fail('bad_transition', `unknown にできるのは importing の回だけ (今 = ${r.state}・${r.run_id})`);
    if (lockActive(r, now)) fail('busy', 'まだ鍵が生きている = 動いている途中かもしれない');
    const d = r.run_detail ? JSON.parse(r.run_detail) : {};
    update(db, now, { state: 'unknown', run_detail: JSON.stringify({ ...d, result: 'unknown', result_detail: { reason }, result_at: now }),
      lock_token: null, lock_holder: null, lock_purpose: null, lock_run_id: null, lock_expires_at: null, lock_init_id: null });
    const eventId = stateEvent(db, now, 'mark_unknown', runId, by, { reason });
    return { state: 'unknown', state_event_id: eventId };
  })();
}

/**
 * 解除 (人がロジザードのインポート履歴を確かめてから。H6)。unknown / partial / verify_failed → idle (halted はそのまま)。
 * partial (状態か outcome) は partial_check (対象外の列に差が無い・差のある商品が全部次の夜の対象にある) か repaired (人が直した) が要る。
 */
export function resolve(db, { runId, outcome, note, by, partialCheck = null, repaired = false, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  if (!['imported', 'not_imported', 'partial'].includes(outcome)) fail('bad_request', 'outcome は imported / not_imported / partial', 400);
  checkNote(note, 'note (何を確かめたか)');
  return db.transaction(() => {
    const r = mustRow(db);
    if (!['unknown', 'partial', 'verify_failed'].includes(r.state)) fail('bad_transition', `解除するものが無い (今 = ${r.state})`);
    if (r.run_id !== runId) fail('run_mismatch', `解除するのは ${r.run_id}`);
    if (r.state === 'partial' || outcome === 'partial') {
      const okCheck = partialCheck && partialCheck.non_target_unchanged === true && partialCheck.all_in_next_csv === true;
      if (!okCheck && repaired !== true) fail('partial_unchecked', '一部だけの取込の解除には、対象外の列に差が無いこと・差のある商品が全部次の夜の対象にあること (partial_check) か、人が直したこと (repaired) が要る');
    }
    const d = r.run_detail ? JSON.parse(r.run_detail) : {};
    // 鍵を消す = 解除した回を古い鍵で再開させない (期限内の鍵でも。Codex #1513 R1 High)
    update(db, now, { state: 'idle', run_detail: JSON.stringify({ ...d, resolved: { outcome, note, by, partial_check: partialCheck, repaired, at: now, from: r.state } }),
      lock_token: null, lock_holder: null, lock_purpose: null, lock_run_id: null, lock_expires_at: null, lock_init_id: null });
    const eventId = stateEvent(db, now, 'resolve', runId, by, { from: r.state, outcome, note, partial_check: partialCheck, repaired });
    return { state: 'idle', state_event_id: eventId };
  })();
}

/** 自動の取込を止める (戻し方の手の ③ の前・人の判断) */
export function halt(db, { by, reason, now = Date.now() }) {
  checkBy(by);
  checkNote(reason, 'reason');
  return db.transaction(() => {
    mustRow(db);
    update(db, now, { halted: 1, halted_reason: String(reason).slice(0, 300), halted_by: by, halted_at: now });
    const eventId = event(db, now, 'halt', null, by, { reason });
    update(db, now, { halted_event_id: eventId });   // 止めの番号 = この止め (止め直すたびに変わる)
    // どこから止めても同じ取引で知らせを積む (送るのは定時の入口と画面。K3-4)
    const outboxId = outboxPut(db, now, 'halt', `halt:${eventId}`, `⏸ ロジザードの毎日の商品マスタの自動の取込を止めた (${by}): ${String(reason).slice(0, 300)}\n戻し方 = 画面の「手の取込」・再開 = 未解決の取込と開いた手の取込が無いときに resume\n画面 ▶ ${ADMIN_PAGE_URL}`);
    return { halted: true, outbox_id: outboxId, halt_revision: eventId };
  })();
}

/** 自動の取込を再開する = 未解決の取込が無い (state が idle / verified)・鍵が空いている ときだけ (H6) */
/**
 * 見ていた止めの番号と今の止めを照らす (必須・どの入口でも = 画面・機械の口・CLI。違う = 止めた後に止め直された・再開された = 読み直す。
 * Codex #1542 R1 High・R2 High)。止めてあることは呼び手が先に確かめる
 */
function checkHaltRevision(r, expected) {
  if (!Number.isSafeInteger(expected) || expected < 1) fail('bad_request', 'expected_halt_revision (見ていた止めの番号・status の halt_revision) が要る', 400);
  if ((r.halted_event_id ?? null) !== expected) fail('stale', '止めを見た後に止め直された・再開された = 読み直して今の止めの理由を見てから');
}

export function resume(db, { by, note, expectedHaltRevision, now = Date.now() }) {
  checkBy(by);
  checkNote(note);
  return db.transaction(() => {
    const r = mustRow(db);
    if (!r.halted) fail('not_halted', '止めてない (もう再開してある)');
    checkHaltRevision(r, expectedHaltRevision);
    if (UNRESOLVED.has(r.state)) fail('state', `未解決の取込がある (${r.state}・${r.run_id})。先に resolve`);
    if (lockActive(r, now)) fail('busy', '鍵をほかが持っている');
    const open = openSessionOf(db);
    if (open) fail('manual_open', `手の取込 ${open.session_id} が開いている (終えるか取り消してから)`);
    const review = unackedReviewOf(db);
    if (review) fail('needs_review', `手の取込 ${review.session_id} が確認待ち (needs_review)。ロジザードの履歴を見て確認 (ack) してから`);
    update(db, now, { halted: 0, halted_reason: null, halted_by: null, halted_at: null, halted_event_id: null });
    event(db, now, 'resume', null, by, { note });
    return { halted: false };
  })();
}

/**
 * 止まったことを GChat に送れた (送れていなければ次の回で再送する。H9)。
 * 知らせたのが「今の状態・今の出来事の番号」のときだけ知らせ済みにする (K9: 送っている間に状態が変わった = stale = 新しい状態を知らせ直す)
 */
export function markNotified(db, { runId, state, stateEventId, by, now = Date.now() }) {
  checkBy(by); checkRunId(runId);
  if (!STATES.includes(state) || !Number.isSafeInteger(Number(stateEventId))) fail('bad_request', 'notified には state と state_event_id が要る', 400);
  return db.transaction(() => {
    const r = mustRow(db);
    if (r.run_id !== runId) fail('run_mismatch', `今の回は ${r.run_id}`);
    if (r.state !== state || r.state_event_id !== Number(stateEventId)) fail('stale', `知らせた後に状態が変わった (今 = ${r.state}・出来事 ${r.state_event_id}) = 今の状態を知らせ直す`);
    update(db, now, { notified_at: now, notified_for: r.state_event_id });
    event(db, now, 'notified', runId, by, { state, state_event_id: r.state_event_id });
    return { notified_at: now, state, state_event_id: r.state_event_id };
  })();
}

/** 毎晩の取込が verified になった取引の中で: その回の区切りまでの未解決の義務のうち、その回の成果物にある商品だけ閉じる。残り = 知らせ (K3-2) */
function closeForNightly(db, now, runId, by) {
  const snap = db.prepare('SELECT * FROM nightly_snapshots WHERE run_id = ?').get(runId);
  if (!snap) return null;   // 試験の取込 (test) は義務を閉じない
  const a = db.prepare('SELECT csv FROM daily_artifacts WHERE source_run_id = ?').get(snap.source_run_id);
  // 成果物が無い (整理で消えた) = 閉じられない = 義務は残して知らせる (verified は書く)
  const ids = new Set(a ? readDailyCsv(Buffer.from(a.csv)).map((x) => x[0]) : []);
  const open = db.prepare('SELECT o.id, o.product_id FROM reapply_obligations o LEFT JOIN reapply_closures c ON c.obligation_id = o.id WHERE c.obligation_id IS NULL AND o.id <= ?').all(snap.max_obligation_id);
  const ins = db.prepare("INSERT INTO reapply_closures (obligation_id, kind, run_id, by, note, at) VALUES (?, 'reapplied', ?, ?, NULL, ?)");
  let closed = 0;
  for (const o of open) if (ids.has(o.product_id)) { ins.run(o.id, runId, by, now); closed++; }
  const remaining = openObligationCount(db);
  if (remaining) {
    const head = db.prepare(`SELECT DISTINCT o.product_id ${OPEN_OBLIGATIONS} ORDER BY o.id LIMIT 5`).all().map((o) => o.product_id).join(', ');
    outboxPut(db, now, 'pending_reapply', `pending:${openFingerprint(db)}:${jstDate(now)}`,
      `⚠️ ロジザードの再適用待ちが ${remaining} 件残っている (毎晩の取込 ${runId} の成果物に無い商品・例 ${head})。Company DB の待ち・対象外かを確かめて、残す理由があれば画面で waiver\n画面 ▶ ${ADMIN_PAGE_URL}`);
  }
  event(db, now, 'reapply', runId, by, { closed, remaining, artifact: a ? snap.source_run_id : null });
  return { closed, remaining };
}

/**
 * 毎晩の成果物を受け取る (K3-1)。バイト列から sha256・行数・CSV の形を計算し直す (申告と違う = 断る)。
 * 同じ source_run_id: 同じ中身 = そのまま (冪等) / 違う中身 = 断る。14 日より前は整理する (新しい 3 つと、取込の途中の回が使うものは残す)。
 */
export function putArtifact(db, { sourceRunId, targetAsOf, verdict, csvBuf, sha256: claimedSha, rows: claimedRows, by, now = Date.now() }) {
  checkBy(by);
  if (!SOURCE_RUN_RE.test(String(sourceRunId || ''))) fail('bad_request', 'source_run_id (lzd_…) の形が違う', 400);
  if (!isRealDate(targetAsOf)) fail('bad_request', 'target_as_of (実在の日 YYYY-MM-DD) が要る', 400);
  if (!['pass', 'fail'].includes(verdict)) fail('bad_request', 'verdict は pass / fail', 400);
  const table = readDailyCsv(csvBuf);
  const got = sha256(csvBuf);
  if (claimedSha !== got || claimedRows !== table.length) fail('mismatch', '申告の sha256・行数が中身と違う', 400);
  return db.transaction(() => {
    // 台帳 (整理の後も残る) と照らす: 同じ識別 = 中身がまだあればそのまま・整理の後なら入れ直す / 違う = 断る
    const led = db.prepare('SELECT target_as_of, verdict, csv_sha256, rows FROM artifact_ledger WHERE source_run_id = ?').get(sourceRunId);
    if (led && (led.csv_sha256 !== got || led.target_as_of !== targetAsOf || led.verdict !== verdict || led.rows !== table.length)) fail('conflict', 'その source_run_id は別の中身で受け取り済み');
    if (led && db.prepare('SELECT 1 FROM daily_artifacts WHERE source_run_id = ?').get(sourceRunId)) return { stored: false, same: true, source_run_id: sourceRunId, target_as_of: targetAsOf, verdict, csv_sha256: got, rows: table.length };
    if (!led) db.prepare('INSERT INTO artifact_ledger (source_run_id, target_as_of, verdict, csv_sha256, rows, first_at) VALUES (?, ?, ?, ?, ?, ?)').run(sourceRunId, targetAsOf, verdict, got, table.length, now);
    db.prepare('INSERT INTO daily_artifacts (source_run_id, target_as_of, verdict, csv_sha256, rows, csv, received_at, received_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
      .run(sourceRunId, targetAsOf, verdict, got, table.length, csvBuf, now, by);
    event(db, now, 'artifact_put', null, by, { source_run_id: sourceRunId, target_as_of: targetAsOf, verdict, csv_sha256: got, rows: table.length });
    // 整理 (手の取込は CSV の写しを自分で持つ = 成果物を整理しても消えない)
    db.prepare(`DELETE FROM daily_artifacts WHERE received_at < ?
      AND source_run_id NOT IN (SELECT source_run_id FROM daily_artifacts ORDER BY received_at DESC LIMIT 3)
      AND source_run_id NOT IN (SELECT s.source_run_id FROM nightly_snapshots s JOIN import_state st ON st.run_id = s.run_id AND st.state IN ('importing', 'imported_unverified'))`).run(now - ARTIFACT_KEEP_MS);
    return { stored: true, same: !!led, source_run_id: sourceRunId, target_as_of: targetAsOf, verdict, csv_sha256: got, rows: table.length };
  })();
}

/** 成果物の識別 (中身は返さない) */
export function listArtifacts(db, { limit = 14 } = {}) {
  return db.prepare('SELECT source_run_id, target_as_of, verdict, csv_sha256, rows, received_at, received_by FROM daily_artifacts ORDER BY target_as_of DESC, received_at DESC LIMIT ?').all(Math.max(1, Math.min(60, Number(limit) || 14)));
}
export function getArtifact(db, { sourceRunId }) {
  return db.prepare('SELECT source_run_id, target_as_of, verdict, csv_sha256, rows, received_at, received_by FROM daily_artifacts WHERE source_run_id = ?').get(String(sourceRunId ?? '')) || null;
}

/** 設定 (無い = cutover_phase は cutover = GAS の CSV を断る・lz_accounts は空 = 手の取込を始められない) */
export function getSettings(db) {
  return { cutover_phase: settingOf(db, 'cutover_phase', 'cutover'), lz_accounts: settingOf(db, 'lz_accounts', []) };
}
export function setSetting(db, { key, value, by, now = Date.now() }) {
  checkBy(by);
  let v = value;
  if (key === 'cutover_phase') { if (!CUTOVER_PHASES.includes(v)) fail('bad_request', `cutover_phase は ${CUTOVER_PHASES.join(' / ')}`, 400); }
  else if (key === 'lz_accounts') {
    if (!Array.isArray(v) || !v.length || v.length > 20 || v.some((x) => typeof x !== 'string' || !ACCOUNT_RE.test(x))) fail('bad_request', 'lz_accounts は 1〜20 個のアカウント名 (文字列)', 400);
    v = [...new Set(v)];
  } else fail('bad_request', '知らない設定', 400);
  return db.transaction(() => {
    mustRow(db);
    // 移行の段階は 無い → transition → cutover の一方通行 (切替の後に GAS の CSV を開き直さない。Codex #1537 R1 Medium・K3-8)
    if (key === 'cutover_phase') {
      const cur = settingOf(db, 'cutover_phase', null);
      if (cur === 'cutover' && v !== 'cutover') fail('one_way', '切替の後 (cutover) は transition に戻さない (GAS への戻しはシステム全体の手順 = K3-8)');
    }
    db.prepare('INSERT INTO settings (key, value, updated_by, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_by = excluded.updated_by, updated_at = excluded.updated_at')
      .run(key, JSON.stringify(v), by, now);
    event(db, now, 'setting', null, by, { key, value: v });
    return { key, value: v };
  })();
}

/**
 * 手の取込を始める (K3・v4 §1)。条件は 1 つの取引で照らす: 初期化済み・halted・state idle / verified・生きた鍵なし・開いた手の取込なし・確認待ちの needs_review なし。
 * source = { kind: 'cdb_artifact', sourceRunId } (判定 pass の成果物) | { kind: 'gas_upload', csvBuf, targetAsOf } (cutover_phase = transition の間だけ・対象の日は今日か昨日 (JST))。
 * CSV の中身・識別・使うロジザードのアカウント (lz_accounts から) を固定して持ち、全部の商品の再適用待ちの義務を同じ取引で足す。
 */
export function openManualSession(db, { by, lzAccount, source, expectedHaltRevision, now = Date.now() }) {
  needV4(); checkBy(by);
  if (!source || typeof source !== 'object') fail('bad_request', 'source が要る', 400);
  return db.transaction(() => {
    const r = mustRow(db);
    if (!r.halted) fail('not_halted', '手の取込は、自動の取込を止めてから (halt)');
    checkHaltRevision(r, expectedHaltRevision);
    if (!['idle', 'verified'].includes(r.state)) fail('state', `始められない状態: ${r.state} (${r.run_id || ''})。先に resolve`);
    if (lockActive(r, now)) fail('busy', '鍵をほかが持っている');
    const open = openSessionOf(db);
    if (open) fail('manual_open', `手の取込 ${open.session_id} が開いている`);
    const review = unackedReviewOf(db);
    if (review) fail('needs_review', `手の取込 ${review.session_id} が確認待ち (needs_review)`);
    const settings = getSettings(db);
    if (typeof lzAccount !== 'string' || !settings.lz_accounts.includes(lzAccount)) fail('bad_account', '使うロジザードのアカウントは登録済みの一覧から (lz_accounts)', 400);
    let csvBuf, targetAsOf, sourceRunId = null;
    if (source.kind === 'cdb_artifact') {
      const a = db.prepare('SELECT * FROM daily_artifacts WHERE source_run_id = ?').get(String(source.sourceRunId ?? ''));
      if (!a || a.verdict !== 'pass') fail('artifact_missing', 'その成果物 (判定 pass) が無い');
      csvBuf = Buffer.from(a.csv); targetAsOf = a.target_as_of; sourceRunId = a.source_run_id;
      if (sha256(csvBuf) !== a.csv_sha256) fail('artifact_broken', '成果物の中身が識別と違う');
    } else if (source.kind === 'gas_upload') {
      if (settings.cutover_phase !== 'transition') fail('gas_closed', 'GAS の CSV は移行の段階 (cutover_phase = transition) の間だけ');
      const today = jstDate(now), yesterday = jstDate(now - 86400000);
      if (![today, yesterday].includes(source.targetAsOf)) fail('bad_request', 'GAS の CSV の対象の日は今日か昨日 (JST)', 400);
      csvBuf = source.csvBuf; targetAsOf = source.targetAsOf;
    } else fail('bad_request', 'source.kind は cdb_artifact / gas_upload', 400);
    const table = readDailyCsv(csvBuf);
    const sessionId = rid('lzm', now);
    const downloadName = `${sessionId}.csv`;
    db.prepare(`INSERT INTO manual_sessions (session_id, status, opened_by, opened_at, lz_account, source_kind, source_run_id, target_as_of, csv_sha256, rows, csv, download_name, updated_at)
      VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(sessionId, by, now, lzAccount, source.kind, sourceRunId, targetAsOf, sha256(csvBuf), table.length, csvBuf, downloadName, now);
    const ins = db.prepare('INSERT INTO reapply_obligations (session_id, product_id, created_at) VALUES (?, ?, ?)');
    for (const x of table) ins.run(sessionId, x[0], now);
    event(db, now, 'manual_open', null, by, { session_id: sessionId, source_kind: source.kind, source_run_id: sourceRunId, target_as_of: targetAsOf, csv_sha256: sha256(csvBuf), rows: table.length, lz_account: lzAccount });
    return { session_id: sessionId, download_name: downloadName, csv_sha256: sha256(csvBuf), rows: table.length, target_as_of: targetAsOf, source_kind: source.kind, source_run_id: sourceRunId };
  })();
}

/** 手の取込の CSV (人がダウンロードしてロジザードに置くのはこれだけ) */
export function manualSessionCsv(db, { sessionId }) {
  const x = db.prepare('SELECT status, download_name, csv, csv_sha256 FROM manual_sessions WHERE session_id = ?').get(String(sessionId ?? ''));
  if (!x) fail('not_found', 'その手の取込は無い', 404);
  // 開いている手の取込だけ (閉じた後に古い画面から取ってロジザードに置く = 記録の外の取込。Codex #1542 R1 Medium)
  if (x.status !== 'open') fail('not_open', 'この手の取込はもう閉じている (画面を読み直す)');
  return { download_name: x.download_name, csv: Buffer.from(x.csv), csv_sha256: x.csv_sha256 };
}
export function getManualSession(db, { sessionId }) {
  return sessionMeta(db.prepare('SELECT * FROM manual_sessions WHERE session_id = ?').get(String(sessionId ?? '')));
}
export function listManualSessions(db, { limit = 20 } = {}) {
  return db.prepare('SELECT * FROM manual_sessions ORDER BY opened_at DESC LIMIT ?').all(Math.max(1, Math.min(100, Number(limit) || 20))).map(sessionMeta);
}

/**
 * 手の取込を終える (K3-3)。結果の文 (今回新しく出た表示)・ロジザードの履歴 (ファイル名・日時 (ms)・アカウント)・メモ。
 * 通常の完了 (completed_ok) = 結果が成功 (総件数 = 行数・エラー 0) かつ 履歴のファイル名 = 出した名前・アカウント = 固定したもの・
 *   日時 = **分まで** (ロジザードの履歴は分までの前提・秒は 0 で渡す = 違う = 400) で「始めた分 < 履歴 ≦ 今の分」。
 *   **始めた分と同じ分の履歴 = 要確認** (始める前の取込と見分けられない = 安全側。Codex #1541 R1 High)。実物の履歴の画面で確かめる。
 * どれかが違う = needs_review (管理者の確認まで resume できない。知らせを積む)。
 */
export function completeManualSession(db, { sessionId, resultText, history, note = null, by, now = Date.now() }) {
  checkBy(by);   // 旗が無くても、もう開いている手の取込は終えられる (旗を外した後の片付け。Codex #1537 R2 Medium)
  if (typeof resultText !== 'string' || !resultText.trim() || resultText.length > LIMITS.resultText) fail('bad_request', `結果の文 (1〜${LIMITS.resultText} 文字) が要る`, 400);
  if (!history || typeof history !== 'object' || typeof history.fileName !== 'string' || history.fileName.length > LIMITS.fileName
    || !Number.isSafeInteger(history.at) || history.at % MINUTE !== 0 || typeof history.account !== 'string' || history.account.length > 60) fail('bad_request', 'history = { fileName, at (ms・分まで = 秒は 0), account } が要る', 400);
  checkOptText(note, 'note');
  return db.transaction(() => {
    mustRow(db);
    const x = db.prepare('SELECT * FROM manual_sessions WHERE session_id = ?').get(String(sessionId ?? ''));
    if (!x) fail('not_found', 'その手の取込は無い', 404);
    if (x.status !== 'open') fail('bad_transition', `終えられるのは開いている手の取込だけ (今 = ${x.status})`);
    const parsed = parseImportResult(resultText);
    const judged = judgeImportResult(parsed, x.rows);
    const mismatches = [];
    if (history.fileName !== x.download_name) mismatches.push('file_name');
    const floorMin = (ms) => Math.floor(ms / MINUTE) * MINUTE;
    if (history.at <= floorMin(x.opened_at) || history.at > floorMin(now)) mismatches.push('history_time');
    if (history.account !== x.lz_account) mismatches.push('account');
    const status = judged.to === 'imported_unverified' && !mismatches.length ? 'completed_ok' : 'needs_review';
    const detail = { result_text: resultText, parsed, judged, history: { fileName: history.fileName, at: history.at, account: history.account }, mismatches, note };   // 原文も残す (読めない文も。Codex #1537 R2 Medium)
    db.prepare('UPDATE manual_sessions SET status = ?, closed_by = ?, closed_at = ?, close_detail = ?, updated_at = ? WHERE session_id = ?').run(status, by, now, JSON.stringify(detail), now, x.session_id);
    const eventId = event(db, now, 'manual_complete', null, by, { session_id: x.session_id, status, judged, mismatches });
    const outboxId = status !== 'needs_review' ? null : outboxPut(db, now, 'manual_review', `manual_review:${x.session_id}`,
      `⚠️ ロジザードの手の取込 ${x.session_id} が確認待ち (needs_review): ${[judged.to !== 'imported_unverified' ? `結果 = ${judged.why}` : null, mismatches.length ? `履歴と合わない = ${mismatches.join('・')}` : null].filter(Boolean).join('・')}。ロジザードの履歴を見て画面で確認 (ack) するまで自動を再開できない\n画面 ▶ ${ADMIN_PAGE_URL}`);
    return { session_id: x.session_id, status, judged, mismatches, event_id: eventId, outbox_id: outboxId };
  })();
}

/** 手の取込を取り消す (ロジザードに置かなかった)。義務は残す (自動が入れ直すので害は無い) */
export function cancelManualSession(db, { sessionId, note, by, now = Date.now() }) {
  checkBy(by); checkNote(note);   // 旗が無くても取り消せる (片付け)
  return db.transaction(() => {
    mustRow(db);
    const x = db.prepare('SELECT * FROM manual_sessions WHERE session_id = ?').get(String(sessionId ?? ''));
    if (!x) fail('not_found', 'その手の取込は無い', 404);
    if (x.status !== 'open') fail('bad_transition', `取り消せるのは開いている手の取込だけ (今 = ${x.status})`);
    db.prepare("UPDATE manual_sessions SET status = 'cancelled', closed_by = ?, closed_at = ?, close_detail = ?, updated_at = ? WHERE session_id = ?").run(by, now, JSON.stringify({ cancelled: true, note }), now, x.session_id);
    event(db, now, 'manual_cancel', null, by, { session_id: x.session_id, note });
    return { session_id: x.session_id, status: 'cancelled' };
  })();
}

/** 確認待ち (needs_review) を管理者が確認した (ロジザードの履歴を見て)。義務は残る */
export function acknowledgeManualSession(db, { sessionId, note, by, now = Date.now() }) {
  checkBy(by); checkNote(note);   // 旗が無くても確認できる (片付け)
  return db.transaction(() => {
    mustRow(db);
    const x = db.prepare('SELECT * FROM manual_sessions WHERE session_id = ?').get(String(sessionId ?? ''));
    if (!x) fail('not_found', 'その手の取込は無い', 404);
    if (x.status !== 'needs_review' || x.ack_at != null) fail('bad_transition', `確認するものが無い (今 = ${x.status}${x.ack_at != null ? '・確認済み' : ''})`);
    db.prepare('UPDATE manual_sessions SET ack_by = ?, ack_at = ?, ack_note = ?, updated_at = ? WHERE session_id = ?').run(by, now, note, now, x.session_id);
    event(db, now, 'manual_ack', null, by, { session_id: x.session_id, note });
    return { session_id: x.session_id, status: 'needs_review', acknowledged: true };
  })();
}

/** 残っている再適用待ちの義務 */
export function listPending(db, { limit = 200, afterId = 0, productId = null } = {}) {
  const count = openObligationCount(db);
  // 番号の後から (ページ送り)・商品で探す (完全一致) = 何件あっても全部の義務に届く (Codex #1541 R1 Medium)
  const lim = Math.max(1, Math.min(5000, Number(limit) || 200));
  const after = Number.isSafeInteger(afterId) && afterId > 0 ? afterId : 0;
  const items = productId == null
    ? db.prepare(`SELECT o.id, o.session_id, o.product_id, o.created_at ${OPEN_OBLIGATIONS} AND o.id > ? ORDER BY o.id LIMIT ?`).all(after, lim)
    : db.prepare(`SELECT o.id, o.session_id, o.product_id, o.created_at ${OPEN_OBLIGATIONS} AND o.id > ? AND o.product_id = ? ORDER BY o.id LIMIT ?`).all(after, String(productId), lim);
  return { count, fingerprint: count ? openFingerprint(db) : null, items, next_after: items.length === lim ? items[items.length - 1].id : null };
}

/** 特定の義務だけを理由を書いて閉じる (waived)。閉じていない義務だけ (違う = 全部断る) */
export function waiveObligations(db, { obligationIds, note, by, now = Date.now() }) {
  needV4(); checkBy(by); checkNote(note);
  if (!Array.isArray(obligationIds) || !obligationIds.length || obligationIds.length > LIMITS.waive || obligationIds.some((x) => !Number.isSafeInteger(x) || x < 1)) fail('bad_request', `obligation_ids (1〜${LIMITS.waive} 個の番号) が要る`, 400);
  const ids = [...new Set(obligationIds)];
  return db.transaction(() => {
    mustRow(db);
    const q = db.prepare('SELECT o.id, c.obligation_id AS closed FROM reapply_obligations o LEFT JOIN reapply_closures c ON c.obligation_id = o.id WHERE o.id = ?');
    const bad = ids.filter((id) => { const r = q.get(id); return !r || r.closed != null; });
    if (bad.length) fail('not_open', `閉じていない義務だけ (無い・閉じ済み = ${bad.length} 件)`);
    const ins = db.prepare("INSERT INTO reapply_closures (obligation_id, kind, run_id, by, note, at) VALUES (?, 'waived', NULL, ?, ?, ?)");
    for (const id of ids) ins.run(id, by, note, now);
    event(db, now, 'reapply_waive', null, by, { count: ids.length, note });
    return { waived: ids.length };
  })();
}

/** まだ送れていない知らせ (古い順) */
/** 1 つの知らせ (まだ送れていないときだけ) */
export function outboxGet(db, { id }) {
  return db.prepare('SELECT id, kind, dedupe_key, text, created_at FROM outbox WHERE id = ? AND sent_at IS NULL').get(Number(id)) || null;
}
export function outboxPending(db, { limit = 20 } = {}) {
  // 止め・要確認を先に、再適用待ちを後に (古い再適用待ちが多くても新しい止めが後ろに隠れない。N5)。同じ組の中は古い順
  return db.prepare(`SELECT id, kind, dedupe_key, text, created_at FROM outbox WHERE sent_at IS NULL
    ORDER BY CASE kind WHEN 'halt' THEN 0 WHEN 'manual_review' THEN 0 WHEN 'pending_reapply' THEN 2 ELSE 1 END, id LIMIT ?`).all(Math.max(1, Math.min(100, Number(limit) || 20)));
}
/** 知らせを送れた (1 回だけ。もう送れた = そのまま) */
export function outboxMarkSent(db, { id, by, now = Date.now() }) {
  checkBy(by);
  if (!Number.isSafeInteger(id) || id < 1) fail('bad_request', 'id が要る', 400);
  return db.transaction(() => {
    const x = db.prepare('SELECT id, sent_at FROM outbox WHERE id = ?').get(id);
    if (!x) fail('not_found', 'その知らせは無い', 404);
    if (x.sent_at != null) return { id, sent_at: x.sent_at, already: true };
    db.prepare('UPDATE outbox SET sent_at = ?, sent_by = ? WHERE id = ?').run(now, by, id);
    return { id, sent_at: now, already: false };
  })();
}

/**
 * 画面の口 (管理者・③c-1b-3b-4a) の全部の見え方: 状態 (出来事の中身も)・開いた手の取込・確認待ち・最近の手の取込・待ち・送れていない知らせ・設定・成果物。
 * 機械の口 (getStatus) には出さないもの (誰・アカウント・メモ・設定) もここでは出す (管理者だけ)
 */
export function getAdminOverview(db, { now = Date.now() } = {}) {
  const status = getStatus(db, { now, events: 50, reveal: true });
  if (!status.initialized) return { status };
  return {
    status,
    manual_sessions: { open: sessionMeta(openSessionOf(db)), needs_review_unacked: sessionMeta(unackedReviewOf(db)), recent: listManualSessions(db, { limit: 10 }) },
    pending: listPending(db, { limit: 500 }),
    outbox: outboxPending(db, { limit: 50 }),
    settings: getSettings(db),
    artifacts: listArtifacts(db, { limit: 14 }),
  };
}
