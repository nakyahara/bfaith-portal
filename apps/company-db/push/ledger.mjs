/**
 * ledger.mjs — miniPC 側の「Company DB に何を送ったか」の台帳 (SQLite。DATA_DIR/company-db-push.db)。D5a
 *
 * なぜ別ファイルか: warehouse.db は NE 取込 (ne-api.js / auto-import) が書く表で、送り手は**読むだけ**にする (表を足さない・WAL を長く持たない)。
 * なぜ台帳か (PR #1336 Codex R1 #1〜#3): raw の synced_at (秒精度・取込開始時の時刻を数分後まで使う) をカーソルにすると、
 *   読んでいる途中の更新や遅れて commit された古い時刻を飛び越えて **変更が永久に届かない**。台帳に「伝票ごとの指紋 (ヘッダ + 明細)」を持ち、
 *   毎回 raw を全部読んで指紋を比べれば、時刻に頼らずに「変わった伝票」が決まる。投入済みの伝票は範囲の条件 (D-28) から外れても追跡する。
 * 台帳は**作り直せる写し** (バックアップの対象にしない): 失くしても次の run が Render の世代に合わせて採番を直し、全部を送り直す ('same' が返るだけ)。
 *   Render を過去の時点に復元したときは `--reset-ledger` で指紋を空にして全部送り直す (伝票番号は残す = 範囲から外れた投入済みの追跡を失わない)
 *
 * 持つもの:
 *   shipments_sent  伝票番号 → 指紋 (fp) / 送った世代 / 時刻。'applied' か 'same' が返った伝票だけ書く (failed / stale は書かない = 次回また送る)
 *   outbox          今回送る伝票 (raw を読み終えてから送る = raw の読み取りスナップショットを HTTP の間まで持たない。Codex R2 #6)。送ったら消す
 *   meta            batch_seq (世代。取引の中で +1) / lock (送り手の排他 = 持ち主・pid・開始・心拍。pid が生きていて心拍が新しいときだけ拒む。Codex R2 #5)
 *   runs            送った run の記録 (mode / 世代 / 件数 / 成否)
 */
import Database from 'better-sqlite3';
import path from 'node:path';

export const LEDGER_FILE = 'company-db-push.db';
export const LOCK_KEY = 'lock';
export const SEQ_KEY = 'batch_seq';
export const DEFAULT_LOCK_TTL_MS = 15 * 60 * 1000;   // 心拍 (chunk ごと) がこれより古い lock は死んだ run のもの (daily-sync の 30 分 timeout で殺された送り手は finally を通らない)

const nowNaive = (d = new Date()) => d.toISOString().replace('T', ' ').slice(0, 19);
/** そのプロセスが生きているか (Windows でも process.kill(pid, 0) で確かめられる。pid が無ければ死んだ扱い) */
export function defaultIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return e && e.code === 'EPERM'; }   // EPERM = 居るが触れない
}

export function openLedger(fileOrDataDir, { memory = false } = {}) {
  const file = memory ? ':memory:' : path.join(fileOrDataDir, LEDGER_FILE);
  const db = new Database(file, { timeout: 30000 });
  if (!memory) db.pragma('journal_mode = WAL');
  db.exec(`
    create table if not exists shipments_sent (ne_slip_no text primary key, fp text not null, batch_seq integer not null, sent_at text not null);
    create table if not exists outbox (seq integer primary key autoincrement, ne_slip_no text not null unique, fp text not null, payload text not null, n_lines integer not null, n_bytes integer not null);
    create table if not exists meta (key text primary key, value text, updated_at text);
    create table if not exists runs (run_id text primary key, mode text not null, started_at text not null, finished_at text, batch_seq integer, scanned integer, in_scope integer,
      changed integer, sent integer, applied integer, same integer, stale integer, failed integer, transform_errors integer, ok integer, note text);
  `);
  const stmt = {
    getMeta: db.prepare('select value from meta where key = ?'),
    putMeta: db.prepare('insert into meta (key, value, updated_at) values (?, ?, ?) on conflict (key) do update set value = excluded.value, updated_at = excluded.updated_at'),
    delMeta: db.prepare('delete from meta where key = ?'),
    upsertSent: db.prepare('insert into shipments_sent (ne_slip_no, fp, batch_seq, sent_at) values (?, ?, ?, ?) on conflict (ne_slip_no) do update set fp = excluded.fp, batch_seq = excluded.batch_seq, sent_at = excluded.sent_at'),
    pushOutbox: db.prepare('insert into outbox (ne_slip_no, fp, payload, n_lines, n_bytes) values (?, ?, ?, ?, ?)'),
    takeOutbox: db.prepare('select seq, ne_slip_no, fp, payload, n_lines, n_bytes from outbox where seq > ? order by seq limit ?'),
    delOutbox: db.prepare('delete from outbox where seq = ?'),
    upsertRun: db.prepare(`insert into runs (run_id, mode, started_at, finished_at, batch_seq, scanned, in_scope, changed, sent, applied, same, stale, failed, transform_errors, ok, note)
      values (@run_id, @mode, @started_at, @finished_at, @batch_seq, @scanned, @in_scope, @changed, @sent, @applied, @same, @stale, @failed, @transform_errors, @ok, @note)
      on conflict (run_id) do update set finished_at = excluded.finished_at, batch_seq = excluded.batch_seq, scanned = excluded.scanned, in_scope = excluded.in_scope, changed = excluded.changed,
        sent = excluded.sent, applied = excluded.applied, same = excluded.same, stale = excluded.stale, failed = excluded.failed, transform_errors = excluded.transform_errors, ok = excluded.ok, note = excluded.note`),
  };
  const getMeta = (k) => { const r = stmt.getMeta.get(k); return r && r.value != null ? String(r.value) : null; };
  const txImmediate = (fn) => db.transaction(fn).immediate();
  const readLock = () => { const raw = getMeta(LOCK_KEY); if (!raw) return null; try { return JSON.parse(raw); } catch { return { owner: '?', pid: null, started_at: null, heartbeat_at: null }; } };
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
    /** 心拍を打つ (chunk ごと)。持ち主でなければ false (奪われている = 送るのをやめる) */
    renewLock: (owner, now = new Date()) => txImmediate(() => {
      const held = readLock();
      if (!held || held.owner !== owner) return false;
      stmt.putMeta.run(LOCK_KEY, JSON.stringify({ ...held, heartbeat_at: now.toISOString() }), nowNaive(now));
      return true;
    }),
    releaseLock: (owner) => txImmediate(() => {
      const held = readLock(); if (!held) return false;
      if (held.owner !== owner) return false;   // 奪われていたら触らない
      stmt.delMeta.run(LOCK_KEY); return true;
    }),
    /** 世代を取引の中で +1 して返す (2 つのプロセスが同じ世代を取れない) */
    nextBatchSeq: (now = new Date()) => txImmediate(() => {
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
    /** 送付済みの指紋を全部 (伝票番号 → fp) */
    loadFingerprints: () => { const m = new Map(); for (const r of db.prepare('select ne_slip_no, fp from shipments_sent').iterate()) m.set(r.ne_slip_no, r.fp); return m; },
    countSent: () => db.prepare('select count(*) as n from shipments_sent').get().n,
    /** applied / same が返った伝票を書く (1 取引) */
    markSent: (rows, batchSeq, at = new Date()) => { const t = nowNaive(at); db.transaction(() => { for (const r of rows) stmt.upsertSent.run(r.ne_slip_no, r.fp, batchSeq, t); })(); },
    /** 指紋を空にする (伝票番号は残す) = 次の run で全部送り直す (Render を復元・作り直したとき) */
    resetFingerprints: () => db.prepare(`update shipments_sent set fp = ''`).run().changes,
    // ── outbox ──
    clearOutbox: () => db.prepare('delete from outbox').run().changes,
    countOutbox: () => db.prepare('select count(*) as n, coalesce(max(seq), 0) as max_seq from outbox').get(),
    pushOutbox: (rows) => { db.transaction(() => { for (const r of rows) stmt.pushOutbox.run(r.ne_slip_no, r.fp, r.payload, r.n_lines, r.n_bytes); })(); },
    /** seq が afterSeq より大きい行を、伝票数・明細数・バイト数の上限まで取る (最低 1 行) */
    takeOutbox: (afterSeq, { maxRows, maxLines, maxBytes }) => {
      const out = []; let lines = 0, bytes = 0;
      for (const r of stmt.takeOutbox.all(afterSeq, maxRows)) {
        if (out.length && (lines + r.n_lines > maxLines || bytes + r.n_bytes > maxBytes)) break;
        out.push(r); lines += r.n_lines; bytes += r.n_bytes;
      }
      return out;
    },
    /** 送り終えた行を消し、applied / same の伝票を送付済みに書く (1 取引) */
    ackOutbox: (rows, sentRows, batchSeq, at = new Date()) => { const t = nowNaive(at); db.transaction(() => { for (const r of rows) stmt.delOutbox.run(r.seq); for (const r of sentRows) stmt.upsertSent.run(r.ne_slip_no, r.fp, batchSeq, t); })(); },
    vacuum: () => { if (!memory) db.exec('vacuum'); },
    recordRun: (r) => { stmt.upsertRun.run({ finished_at: null, batch_seq: null, scanned: null, in_scope: null, changed: null, sent: null, applied: null, same: null, stale: null, failed: null, transform_errors: null, ok: null, note: null, ...r }); },
    lastRuns: (n = 5) => db.prepare('select * from runs order by started_at desc limit ?').all(n),
    close: () => db.close(),
  };
}
