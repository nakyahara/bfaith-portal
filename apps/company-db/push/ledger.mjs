/**
 * ledger.mjs — miniPC 側の「Company DB に何を送ったか」の台帳 (SQLite。DATA_DIR/company-db-push.db)。D5a / D5b
 *
 * 種類 (kind) ごとに独立: 'shipment' (NE 伝票。D5a の表と鍵をそのまま使う) / 'order:<mall>' (モールの注文。D5b)。lock・世代・受領記録・初期化の印も種類ごと。
 *
 * なぜ別ファイルか: warehouse.db は NE 取込 (ne-api.js / auto-import) が書く表で、送り手は**読むだけ**にする (表を足さない・WAL を長く持たない)。
 * なぜ台帳か (PR #1336 Codex R1 #1〜#3): raw の synced_at (秒精度・取込開始時の時刻を数分後まで使う) をカーソルにすると、
 *   読んでいる途中の更新や遅れて commit された古い時刻を飛び越えて **変更が永久に届かない**。台帳に「行ごとの指紋 (ヘッダ + 明細)」を持ち、
 *   毎回 raw を全部読んで指紋を比べれば、時刻に頼らずに「変わった行」が決まる。投入済みの行は範囲の条件 (D-28) から外れても追跡する。
 * 台帳と Render の食い違い (Codex R2 #2 / R3 #1〜#3):
 *   - 台帳は**作り直せる写し** (バックアップの対象にしない)。失くしたら次の run が Render から投入済みの鍵を取り戻して追跡対象にし (fp = '' = 未確認)、
 *     世代を Render の最大に合わせ、全部を送り直す ('same' が返るだけ)
 *   - Render を過去の時点に復元・作り直したら、台帳が持つ「最後に受領確認した chunk」(last_receipt) が Render に無いので分かる → 指紋を空にして全部送り直す。手動なら `--reset-ledger`
 *   - 追跡対象 (鍵) と送付確認済み (fp <> '') を分ける。停止の見張り (Render の件数 < 送付確認済み) は確認済みだけを数える
 *
 * 持つもの:
 *   shipments_sent  (kind 'shipment') 伝票番号 → 指紋 (fp。'' = 追跡するだけで未確認) / 送った世代 / 時刻
 *   sent            (他の kind) kind + 鍵 → 指紋 / 世代 / 時刻
 *   outbox          今回送る行 (kind・run ごと。raw を読み終えてから送る = raw の読み取りスナップショットを HTTP の間まで持たない。Codex R2 #6)。送ったら消す。送らずに残った鍵は次の run で追跡対象に引き継ぐ
 *   meta            batch_seq (世代。取引の中で +1) / lock (送り手の排他 = 持ち主・pid・開始・心拍) / last_receipt / initialized。kind 'shipment' は素の鍵、他は '<kind>:' を前に付ける
 *   runs            送った run の記録 (kind / mode / 世代 / 件数 / 成否)
 */
import Database from 'better-sqlite3';
import path from 'node:path';

export const LEDGER_FILE = 'company-db-push.db';
export const LOCK_KEY = 'lock';
export const SEQ_KEY = 'batch_seq';
export const RECEIPT_KEY = 'last_receipt';
export const DEFAULT_LOCK_TTL_MS = 15 * 60 * 1000;   // 心拍 (chunk ごと・走査 5,000 行ごと) がこれより古い lock は死んだ run のもの (daily-sync の 30 分 timeout で殺された送り手は finally を通らない)

const nowNaive = (d = new Date()) => d.toISOString().replace('T', ' ').slice(0, 19);
/** そのプロセスが生きているか (Windows でも process.kill(pid, 0) で確かめられる。pid が無ければ死んだ扱い) */
export function defaultIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return e && e.code === 'EPERM'; }   // EPERM = 居るが触れない
}
export class LockLostError extends Error { constructor(m = 'lock を奪われた (別の送り手が走り始めた) ので止める') { super(m); this.code = 'LOCK_LOST'; } }

function migrate(db) {
  db.exec(`
    create table if not exists shipments_sent (ne_slip_no text primary key, fp text not null, batch_seq integer not null, sent_at text not null);
    create table if not exists sent (kind text not null, key text not null, fp text not null, batch_seq integer not null, sent_at text not null, primary key (kind, key));
    create table if not exists meta (key text primary key, value text, updated_at text);
    create table if not exists runs (run_id text primary key, mode text not null, started_at text not null, finished_at text, batch_seq integer, scanned integer, in_scope integer,
      changed integer, sent integer, applied integer, same integer, stale integer, failed integer, transform_errors integer, ok integer, note text);
  `);
  const cols = (t) => db.prepare(`pragma table_info(${t})`).all().map((c) => c.name);
  if (!cols('runs').includes('kind')) db.exec(`alter table runs add column kind text not null default 'shipment'`);
  const outboxCols = db.prepare(`select name from sqlite_master where type = 'table' and name = 'outbox'`).get() ? cols('outbox') : null;
  if (!outboxCols) {
    db.exec(`create table outbox (seq integer primary key autoincrement, kind text not null, run_id text not null, key text not null, fp text not null, payload text not null, n_lines integer not null, n_bytes integer not null, unique (kind, key))`);
  } else if (!outboxCols.includes('kind')) {
    // D5a の形 (ne_slip_no unique) → kind + key に。残っていた行は伝票として引き継ぐ
    db.exec(`
      create table outbox_v2 (seq integer primary key autoincrement, kind text not null, run_id text not null, key text not null, fp text not null, payload text not null, n_lines integer not null, n_bytes integer not null, unique (kind, key));
      insert into outbox_v2 (kind, run_id, key, fp, payload, n_lines, n_bytes) select 'shipment', run_id, ne_slip_no, fp, payload, n_lines, n_bytes from outbox order by seq;
      drop table outbox;
      alter table outbox_v2 rename to outbox;
    `);
  }
}

export function openLedger(fileOrDataDir, { memory = false, kind = 'shipment' } = {}) {
  const file = memory ? ':memory:' : path.join(fileOrDataDir, LEDGER_FILE);
  const db = new Database(file, { timeout: 30000 });
  if (!memory) db.pragma('journal_mode = WAL');
  migrate(db);
  const legacy = kind === 'shipment';
  const mk = (k) => (legacy ? k : `${kind}:${k}`);   // meta の鍵
  const sentTable = legacy ? 'shipments_sent' : 'sent';
  const keyCol = legacy ? 'ne_slip_no' : 'key';
  const kindWhere = legacy ? '' : ` and kind = '${kind}'`;
  const stmt = {
    getMeta: db.prepare('select value from meta where key = ?'),
    putMeta: db.prepare('insert into meta (key, value, updated_at) values (?, ?, ?) on conflict (key) do update set value = excluded.value, updated_at = excluded.updated_at'),
    delMeta: db.prepare('delete from meta where key = ?'),
    upsertSent: legacy
      ? db.prepare('insert into shipments_sent (ne_slip_no, fp, batch_seq, sent_at) values (?, ?, ?, ?) on conflict (ne_slip_no) do update set fp = excluded.fp, batch_seq = excluded.batch_seq, sent_at = excluded.sent_at')
      : db.prepare(`insert into sent (kind, key, fp, batch_seq, sent_at) values ('${kind}', ?, ?, ?, ?) on conflict (kind, key) do update set fp = excluded.fp, batch_seq = excluded.batch_seq, sent_at = excluded.sent_at`),
    trackKey: legacy
      ? db.prepare(`insert into shipments_sent (ne_slip_no, fp, batch_seq, sent_at) values (?, '', 0, ?) on conflict (ne_slip_no) do nothing`)
      : db.prepare(`insert into sent (kind, key, fp, batch_seq, sent_at) values ('${kind}', ?, '', 0, ?) on conflict (kind, key) do nothing`),
    loadFps: db.prepare(`select ${keyCol} as key, fp from ${sentTable} where 1 = 1${kindWhere}`),
    countTracked: db.prepare(`select count(*) as n from ${sentTable} where 1 = 1${kindWhere}`),
    countConfirmed: db.prepare(`select count(*) as n from ${sentTable} where fp <> ''${kindWhere}`),
    resetFps: db.prepare(`update ${sentTable} set fp = '' where 1 = 1${kindWhere}`),
    pushOutbox: db.prepare(`insert into outbox (kind, run_id, key, fp, payload, n_lines, n_bytes) values ('${kind}', ?, ?, ?, ?, ?, ?)`),
    takeOutbox: db.prepare(`select seq, key, fp, payload, n_lines, n_bytes from outbox where kind = '${kind}' and run_id = ? and seq > ? order by seq limit ?`),
    countOutbox: db.prepare(`select count(*) as n, coalesce(max(seq), 0) as max_seq from outbox where kind = '${kind}' and run_id = ?`),
    outboxKeys: db.prepare(`select key from outbox where kind = '${kind}'`),
    clearOutbox: db.prepare(`delete from outbox where kind = '${kind}'`),
    delOutbox: db.prepare('delete from outbox where seq = ?'),
    upsertRun: db.prepare(`insert into runs (run_id, kind, mode, started_at, finished_at, batch_seq, scanned, in_scope, changed, sent, applied, same, stale, failed, transform_errors, ok, note)
      values (@run_id, '${kind}', @mode, @started_at, @finished_at, @batch_seq, @scanned, @in_scope, @changed, @sent, @applied, @same, @stale, @failed, @transform_errors, @ok, @note)
      on conflict (run_id) do update set finished_at = excluded.finished_at, batch_seq = excluded.batch_seq, scanned = excluded.scanned, in_scope = excluded.in_scope, changed = excluded.changed,
        sent = excluded.sent, applied = excluded.applied, same = excluded.same, stale = excluded.stale, failed = excluded.failed, transform_errors = excluded.transform_errors, ok = excluded.ok, note = excluded.note`),
    lastRuns: db.prepare(`select * from runs where kind = '${kind}' order by started_at desc limit ?`),
  };
  const getMeta = (k) => { const r = stmt.getMeta.get(mk(k)); return r && r.value != null ? String(r.value) : null; };
  const putMeta = (k, v, at) => stmt.putMeta.run(mk(k), v, nowNaive(at));
  const txImmediate = (fn) => db.transaction(fn).immediate();
  const readLock = () => { const raw = getMeta(LOCK_KEY); if (!raw) return null; try { return JSON.parse(raw); } catch { return { owner: '?', pid: null, started_at: null, heartbeat_at: null }; } };
  const assertOwner = (owner) => { const held = readLock(); if (!held || held.owner !== owner) throw new LockLostError(); return held; };
  const keyOf = (r) => (r.key ?? r.ne_slip_no);
  const api = {
    db, kind,
    getMeta,
    putMeta: (k, v, at = new Date()) => { putMeta(k, v, at); },
    /**
     * 送り手の排他。持ち主の pid が生きていて心拍 (heartbeat_at) が ttl 以内のときだけ拒む。それ以外 (死んだ・心拍が古い) は奪う。
     * 戻り値 = { ok, held: {owner, pid, started_at, heartbeat_at} | null }
     */
    acquireLock: ({ owner, pid = process.pid, now = new Date(), ttlMs = DEFAULT_LOCK_TTL_MS, isAlive = defaultIsAlive }) => txImmediate(() => {
      const held = readLock();
      if (held) {
        const beat = held.heartbeat_at || held.started_at;
        const fresh = beat ? now.getTime() - Date.parse(beat) < ttlMs : false;
        if (fresh && isAlive(held.pid)) return { ok: false, held };
      }
      putMeta(LOCK_KEY, JSON.stringify({ owner, pid, started_at: now.toISOString(), heartbeat_at: now.toISOString() }), now);
      return { ok: true, held: null };
    }),
    /** 心拍を打つ (chunk ごと・走査の途中)。持ち主でなければ false (奪われている = 送るのをやめる) */
    renewLock: (owner, now = new Date()) => txImmediate(() => {
      const held = readLock();
      if (!held || held.owner !== owner) return false;
      putMeta(LOCK_KEY, JSON.stringify({ ...held, heartbeat_at: now.toISOString() }), now);
      return true;
    }),
    assertLock: (owner) => txImmediate(() => assertOwner(owner)),
    releaseLock: (owner) => txImmediate(() => {
      const held = readLock(); if (!held) return false;
      if (held.owner !== owner) return false;   // 奪われていたら触らない
      stmt.delMeta.run(mk(LOCK_KEY)); return true;
    }),
    /** 世代を取引の中で +1 して返す (2 つのプロセスが同じ世代を取れない。owner を渡せば持ち主の確認も同じ取引で) */
    nextBatchSeq: (now = new Date(), owner = null) => txImmediate(() => {
      if (owner) assertOwner(owner);
      const next = (Number(getMeta(SEQ_KEY)) || 0) + 1;
      putMeta(SEQ_KEY, String(next), now);
      return next;
    }),
    currentBatchSeq: () => Number(getMeta(SEQ_KEY)) || 0,
    /** Render 側の最大世代に追いつかせる (台帳を失くした・作り直したとき。次の世代 = max + 1 になる) */
    ensureBatchSeqAtLeast: (n, now = new Date()) => txImmediate(() => {
      const cur = Number(getMeta(SEQ_KEY)) || 0;
      if (n > cur) { putMeta(SEQ_KEY, String(n), now); return n; }
      return cur;
    }),
    isInitialized: () => getMeta('initialized') === '1',
    markInitialized: (at = new Date()) => { putMeta('initialized', '1', at); },
    /** 送付済み・追跡中の指紋を全部 (鍵 → fp。'' = 追跡するだけ) */
    loadFingerprints: () => { const m = new Map(); for (const r of stmt.loadFps.iterate()) m.set(r.key, r.fp); return m; },
    countTracked: () => stmt.countTracked.get().n,
    countConfirmed: () => stmt.countConfirmed.get().n,
    /** applied / same が返った行を書く (1 取引) */
    markSent: (rows, batchSeq, at = new Date()) => { const t = nowNaive(at); db.transaction(() => { for (const r of rows) stmt.upsertSent.run(keyOf(r), r.fp, batchSeq, t); })(); },
    /** 追跡対象に加える (指紋は '' = 次の run で送る)。既にあれば触らない。戻り値 = 加えた数。owner を渡せば持ち主の確認と同じ取引 */
    trackKeys: (keys, at = new Date(), owner = null) => txImmediate(() => { if (owner) assertOwner(owner); const t = nowNaive(at); let n = 0; for (const k of keys) n += stmt.trackKey.run(String(k), t).changes; return n; }),
    /** 指紋を空にする (鍵は残す) = 次の run で全部送り直す。受領記録も忘れる。owner を渡せば持ち主の確認と同じ取引 */
    resetFingerprints: (owner = null) => txImmediate(() => { if (owner) assertOwner(owner); const n = stmt.resetFps.run().changes; stmt.delMeta.run(mk(RECEIPT_KEY)); return n; }),
    getLastReceipt: () => { const raw = getMeta(RECEIPT_KEY); if (!raw) return null; try { return JSON.parse(raw); } catch { return null; } },
    // ── outbox ──
    clearOutbox: () => stmt.clearOutbox.run().changes,
    outboxKeys: () => stmt.outboxKeys.all().map((r) => r.key),
    countOutbox: (runId) => stmt.countOutbox.get(runId),
    pushOutbox: (runId, rows) => { db.transaction(() => { for (const r of rows) stmt.pushOutbox.run(runId, keyOf(r), r.fp, r.payload, r.n_lines, r.n_bytes); })(); },
    /** seq が afterSeq より大きい行を、行数・明細数・バイト数の上限まで取る (最低 1 行) */
    takeOutbox: (runId, afterSeq, { maxRows, maxLines, maxBytes }) => {
      const out = []; let lines = 0, bytes = 0;
      for (const r of stmt.takeOutbox.all(runId, afterSeq, maxRows)) {
        if (out.length && (lines + r.n_lines > maxLines || bytes + r.n_bytes > maxBytes)) break;
        out.push({ ...r, ne_slip_no: r.key }); lines += r.n_lines; bytes += r.n_bytes;
      }
      return out;
    },
    /** 送り終えた行を消し、applied / same の行を送付済みに書き、受領記録を残す (1 取引。持ち主でなければ LockLostError で何も書かない) */
    ackOutbox: (rows, sentRows, batchSeq, { owner, receipt, at = new Date() }) => txImmediate(() => {
      if (owner) assertOwner(owner);
      const t = nowNaive(at);
      for (const r of rows) stmt.delOutbox.run(r.seq);
      for (const r of sentRows) stmt.upsertSent.run(keyOf(r), r.fp, batchSeq, t);
      if (receipt) putMeta(RECEIPT_KEY, JSON.stringify(receipt), at);
    }),
    /** 前回送らずに残った outbox の鍵を追跡対象に引き継いでから outbox を空にする (1 取引。持ち主でなければ何も書かない。Codex R4 #1) */
    carryOverOutbox: (owner, at = new Date()) => txImmediate(() => {
      assertOwner(owner);
      const keys = stmt.outboxKeys.all().map((r) => r.key);
      const t = nowNaive(at); let carried = 0;
      for (const k of keys) carried += stmt.trackKey.run(k, t).changes;
      const cleared = stmt.clearOutbox.run().changes;
      return { leftover: keys.length, carried, cleared };
    }),
    vacuum: () => { if (!memory) db.exec('vacuum'); },
    recordRun: (r) => { stmt.upsertRun.run({ finished_at: null, batch_seq: null, scanned: null, in_scope: null, changed: null, sent: null, applied: null, same: null, stale: null, failed: null, transform_errors: null, ok: null, note: null, ...r }); },
    lastRuns: (n = 5) => stmt.lastRuns.all(n),
    close: () => db.close(),
  };
  // 伝票の呼び名 (D5a の互換)
  api.trackSlips = api.trackKeys;
  api.outboxSlips = api.outboxKeys;
  return api;
}
