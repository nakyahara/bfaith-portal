/**
 * ledger.mjs — miniPC 側の「Company DB に何を送ったか」の台帳 (SQLite。DATA_DIR/company-db-push.db)。D5a
 *
 * なぜ別ファイルか: warehouse.db は NE 取込 (ne-api.js / auto-import) が書く表で、送り手は**読むだけ**にする (表を足さない・WAL を長く持たない)。
 * なぜ台帳か (PR #1336 Codex R1 #1〜#3): raw の synced_at (秒精度・取込開始時の時刻を数分後まで使う) をカーソルにすると、
 *   読んでいる途中の更新や遅れて commit された古い時刻を飛び越えて **変更が永久に届かない**。台帳に「伝票ごとの指紋 (ヘッダ + 明細)」を持ち、
 *   毎回 raw を全部読んで指紋を比べれば、時刻に頼らずに「変わった伝票」が決まる。投入済みの伝票は範囲の条件 (D-28) から外れても追跡する。
 * 台帳と Render の食い違い (Codex R2 #2 / R3 #1〜#3):
 *   - 台帳は**作り直せる写し** (バックアップの対象にしない)。失くしたら次の run が Render から投入済みの伝票番号を取り戻して追跡対象にし (fp = '' = 未確認)、
 *     世代を Render の最大に合わせ、全部を送り直す ('same' が返るだけ)
 *   - Render を過去の時点に復元・作り直したら、台帳が持つ「最後に受領確認した chunk」(last_receipt) が Render に無いので分かる → 指紋を空にして全部送り直す。手動なら `--reset-ledger`
 *   - 追跡対象 (伝票番号) と送付確認済み (fp <> '') を分ける。停止の見張り (Render の伝票数 < 送付確認済み) は確認済みだけを数える
 *
 * 持つもの:
 *   shipments_sent  伝票番号 → 指紋 (fp。'' = 追跡するだけで未確認) / 送った世代 / 時刻。'applied' か 'same' が返った伝票だけ指紋を書く (failed / stale は書かない = 次回また送る)
 *   outbox          今回送る伝票 (run ごと。raw を読み終えてから送る = raw の読み取りスナップショットを HTTP の間まで持たない。Codex R2 #6)。送ったら消す。送らずに残った伝票番号は次の run で追跡対象に引き継ぐ
 *   meta            batch_seq (世代。取引の中で +1) / lock (送り手の排他 = 持ち主・pid・開始・心拍) / last_receipt (最後に受領確認した run_id / chunk_index / 内容の指紋)
 *   runs            送った run の記録 (mode / 世代 / 件数 / 成否)
 */
import Database from 'better-sqlite3';
import path from 'node:path';

export const LEDGER_FILE = 'company-db-push.db';
export const LOCK_KEY = 'lock';
export const SEQ_KEY = 'batch_seq';
export const RECEIPT_KEY = 'last_receipt';
export const DEFAULT_LOCK_TTL_MS = 15 * 60 * 1000;   // 心拍 (chunk ごと・走査 5,000 伝票ごと) がこれより古い lock は死んだ run のもの (daily-sync の 30 分 timeout で殺された送り手は finally を通らない)

const nowNaive = (d = new Date()) => d.toISOString().replace('T', ' ').slice(0, 19);
/** そのプロセスが生きているか (Windows でも process.kill(pid, 0) で確かめられる。pid が無ければ死んだ扱い) */
export function defaultIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return e && e.code === 'EPERM'; }   // EPERM = 居るが触れない
}
export class LockLostError extends Error { constructor(m = 'lock を奪われた (別の送り手が走り始めた) ので止める') { super(m); this.code = 'LOCK_LOST'; } }

export function openLedger(fileOrDataDir, { memory = false } = {}) {
  const file = memory ? ':memory:' : path.join(fileOrDataDir, LEDGER_FILE);
  const db = new Database(file, { timeout: 30000 });
  if (!memory) db.pragma('journal_mode = WAL');
  db.exec(`
    create table if not exists shipments_sent (ne_slip_no text primary key, fp text not null, batch_seq integer not null, sent_at text not null);
    create table if not exists outbox (seq integer primary key autoincrement, run_id text not null, ne_slip_no text not null unique, fp text not null, payload text not null, n_lines integer not null, n_bytes integer not null);
    create table if not exists meta (key text primary key, value text, updated_at text);
    create table if not exists runs (run_id text primary key, mode text not null, started_at text not null, finished_at text, batch_seq integer, scanned integer, in_scope integer,
      changed integer, sent integer, applied integer, same integer, stale integer, failed integer, transform_errors integer, ok integer, note text);
  `);
  const stmt = {
    getMeta: db.prepare('select value from meta where key = ?'),
    putMeta: db.prepare('insert into meta (key, value, updated_at) values (?, ?, ?) on conflict (key) do update set value = excluded.value, updated_at = excluded.updated_at'),
    delMeta: db.prepare('delete from meta where key = ?'),
    upsertSent: db.prepare('insert into shipments_sent (ne_slip_no, fp, batch_seq, sent_at) values (?, ?, ?, ?) on conflict (ne_slip_no) do update set fp = excluded.fp, batch_seq = excluded.batch_seq, sent_at = excluded.sent_at'),
    trackSlip: db.prepare(`insert into shipments_sent (ne_slip_no, fp, batch_seq, sent_at) values (?, '', 0, ?) on conflict (ne_slip_no) do nothing`),
    pushOutbox: db.prepare('insert into outbox (run_id, ne_slip_no, fp, payload, n_lines, n_bytes) values (?, ?, ?, ?, ?, ?)'),
    takeOutbox: db.prepare('select seq, ne_slip_no, fp, payload, n_lines, n_bytes from outbox where run_id = ? and seq > ? order by seq limit ?'),
    delOutbox: db.prepare('delete from outbox where seq = ?'),
    upsertRun: db.prepare(`insert into runs (run_id, mode, started_at, finished_at, batch_seq, scanned, in_scope, changed, sent, applied, same, stale, failed, transform_errors, ok, note)
      values (@run_id, @mode, @started_at, @finished_at, @batch_seq, @scanned, @in_scope, @changed, @sent, @applied, @same, @stale, @failed, @transform_errors, @ok, @note)
      on conflict (run_id) do update set finished_at = excluded.finished_at, batch_seq = excluded.batch_seq, scanned = excluded.scanned, in_scope = excluded.in_scope, changed = excluded.changed,
        sent = excluded.sent, applied = excluded.applied, same = excluded.same, stale = excluded.stale, failed = excluded.failed, transform_errors = excluded.transform_errors, ok = excluded.ok, note = excluded.note`),
  };
  const getMeta = (k) => { const r = stmt.getMeta.get(k); return r && r.value != null ? String(r.value) : null; };
  const txImmediate = (fn) => db.transaction(fn).immediate();
  const readLock = () => { const raw = getMeta(LOCK_KEY); if (!raw) return null; try { return JSON.parse(raw); } catch { return { owner: '?', pid: null, started_at: null, heartbeat_at: null }; } };
  const assertOwner = (owner) => { const held = readLock(); if (!held || held.owner !== owner) throw new LockLostError(); return held; };
  return {
    db,
    getMeta,
    putMeta: (k, v, at = new Date()) => { stmt.putMeta.run(k, v, nowNaive(at)); },
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
      stmt.putMeta.run(LOCK_KEY, JSON.stringify({ owner, pid, started_at: now.toISOString(), heartbeat_at: now.toISOString() }), nowNaive(now));
      return { ok: true, held: null };
    }),
    /** 心拍を打つ (chunk ごと・走査の途中)。持ち主でなければ false (奪われている = 送るのをやめる) */
    renewLock: (owner, now = new Date()) => txImmediate(() => {
      const held = readLock();
      if (!held || held.owner !== owner) return false;
      stmt.putMeta.run(LOCK_KEY, JSON.stringify({ ...held, heartbeat_at: now.toISOString() }), nowNaive(now));
      return true;
    }),
    /** 持ち主か (奪われていたら LockLostError) */
    assertLock: (owner) => txImmediate(() => assertOwner(owner)),
    releaseLock: (owner) => txImmediate(() => {
      const held = readLock(); if (!held) return false;
      if (held.owner !== owner) return false;   // 奪われていたら触らない
      stmt.delMeta.run(LOCK_KEY); return true;
    }),
    /** 世代を取引の中で +1 して返す (2 つのプロセスが同じ世代を取れない。owner を渡せば持ち主の確認も同じ取引で) */
    nextBatchSeq: (now = new Date(), owner = null) => txImmediate(() => {
      if (owner) assertOwner(owner);
      const next = (Number(getMeta(SEQ_KEY)) || 0) + 1;
      stmt.putMeta.run(SEQ_KEY, String(next), nowNaive(now));
      return next;
    }),
    currentBatchSeq: () => Number(getMeta(SEQ_KEY)) || 0,
    /** Render 側の最大世代に追いつかせる (台帳を失くした・作り直したとき。次の世代 = max + 1 になる) */
    ensureBatchSeqAtLeast: (n, now = new Date()) => txImmediate(() => {
      const cur = Number(getMeta(SEQ_KEY)) || 0;
      if (n > cur) { stmt.putMeta.run(SEQ_KEY, String(n), nowNaive(now)); return n; }
      return cur;
    }),
    /** 一度でも run が Render の状態を確かめたか (新しいファイル = 台帳を失くした・初回 → Render から投入済みを取り戻す判定に使う) */
    isInitialized: () => getMeta('initialized') === '1',
    markInitialized: (at = new Date()) => { stmt.putMeta.run('initialized', '1', nowNaive(at)); },
    /** 送付済み・追跡中の指紋を全部 (伝票番号 → fp。'' = 追跡するだけ) */
    loadFingerprints: () => { const m = new Map(); for (const r of db.prepare('select ne_slip_no, fp from shipments_sent').iterate()) m.set(r.ne_slip_no, r.fp); return m; },
    /** 追跡している伝票の数 (未確認を含む) */
    countTracked: () => db.prepare('select count(*) as n from shipments_sent').get().n,
    /** 送付確認済み (fp <> '') の数 = Render にあるはずの数 */
    countConfirmed: () => db.prepare(`select count(*) as n from shipments_sent where fp <> ''`).get().n,
    /** applied / same が返った伝票を書く (1 取引) */
    markSent: (rows, batchSeq, at = new Date()) => { const t = nowNaive(at); db.transaction(() => { for (const r of rows) stmt.upsertSent.run(r.ne_slip_no, r.fp, batchSeq, t); })(); },
    /** 追跡対象に加える (指紋は '' = 次の run で送る)。既にあれば触らない。戻り値 = 加えた数。owner を渡せば持ち主の確認と同じ取引 (奪われていれば何も書かない) */
    trackSlips: (slips, at = new Date(), owner = null) => txImmediate(() => { if (owner) assertOwner(owner); const t = nowNaive(at); let n = 0; for (const s of slips) n += stmt.trackSlip.run(String(s), t).changes; return n; }),
    /** 指紋を空にする (伝票番号は残す) = 次の run で全部送り直す (Render を復元・作り直したとき)。受領記録も忘れる。owner を渡せば持ち主の確認と同じ取引 */
    resetFingerprints: (owner = null) => txImmediate(() => { if (owner) assertOwner(owner); const n = db.prepare(`update shipments_sent set fp = ''`).run().changes; stmt.delMeta.run(RECEIPT_KEY); return n; }),
    /** 前回送らずに残った outbox の伝票番号を追跡対象に引き継いでから outbox を空にする (1 取引。持ち主でなければ何も書かない = 後続の送り手の outbox を消さない。Codex R4 #1) */
    carryOverOutbox: (owner, at = new Date()) => txImmediate(() => {
      assertOwner(owner);
      const slips = db.prepare('select ne_slip_no from outbox').all().map((r) => r.ne_slip_no);
      const t = nowNaive(at); let carried = 0;
      for (const s of slips) carried += stmt.trackSlip.run(s, t).changes;
      const cleared = db.prepare('delete from outbox').run().changes;
      return { leftover: slips.length, carried, cleared };
    }),
    /** 最後に受領確認した chunk ({ run_id, chunk_index, payload_checksum }) */
    getLastReceipt: () => { const raw = getMeta(RECEIPT_KEY); if (!raw) return null; try { return JSON.parse(raw); } catch { return null; } },
    // ── outbox ──
    clearOutbox: () => db.prepare('delete from outbox').run().changes,
    /** outbox に残っている伝票番号 (送らずに死んだ run の分 = 次の run で追跡対象に引き継ぐ) */
    outboxSlips: () => db.prepare('select ne_slip_no from outbox').all().map((r) => r.ne_slip_no),
    countOutbox: (runId) => db.prepare('select count(*) as n, coalesce(max(seq), 0) as max_seq from outbox where run_id = ?').get(runId),
    pushOutbox: (runId, rows) => { db.transaction(() => { for (const r of rows) stmt.pushOutbox.run(runId, r.ne_slip_no, r.fp, r.payload, r.n_lines, r.n_bytes); })(); },
    /** seq が afterSeq より大きい行を、伝票数・明細数・バイト数の上限まで取る (最低 1 行) */
    takeOutbox: (runId, afterSeq, { maxRows, maxLines, maxBytes }) => {
      const out = []; let lines = 0, bytes = 0;
      for (const r of stmt.takeOutbox.all(runId, afterSeq, maxRows)) {
        if (out.length && (lines + r.n_lines > maxLines || bytes + r.n_bytes > maxBytes)) break;
        out.push(r); lines += r.n_lines; bytes += r.n_bytes;
      }
      return out;
    },
    /** 送り終えた行を消し、applied / same の伝票を送付済みに書き、受領記録を残す (1 取引。持ち主でなければ LockLostError で何も書かない) */
    ackOutbox: (rows, sentRows, batchSeq, { owner, receipt, at = new Date() }) => txImmediate(() => {
      if (owner) assertOwner(owner);
      const t = nowNaive(at);
      for (const r of rows) stmt.delOutbox.run(r.seq);
      for (const r of sentRows) stmt.upsertSent.run(r.ne_slip_no, r.fp, batchSeq, t);
      if (receipt) stmt.putMeta.run(RECEIPT_KEY, JSON.stringify(receipt), t);
    }),
    vacuum: () => { if (!memory) db.exec('vacuum'); },
    recordRun: (r) => { stmt.upsertRun.run({ finished_at: null, batch_seq: null, scanned: null, in_scope: null, changed: null, sent: null, applied: null, same: null, stale: null, failed: null, transform_errors: null, ok: null, note: null, ...r }); },
    lastRuns: (n = 5) => db.prepare('select * from runs order by started_at desc limit ?').all(n),
    close: () => db.close(),
  };
}
