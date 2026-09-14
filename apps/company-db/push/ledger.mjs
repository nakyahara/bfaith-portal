/**
 * ledger.mjs — miniPC 側の「Company DB に何を送ったか」の台帳 (SQLite。DATA_DIR/company-db-push.db)。D5a
 *
 * なぜ別ファイルか: warehouse.db は NE 取込 (ne-api.js / auto-import) が書く表で、送り手は**読むだけ**にする (表を足さない・WAL を長く持たない)。
 * なぜ台帳か (PR #1336 Codex R1 #1〜#3): raw の synced_at (秒精度・取込開始時の時刻を数分後まで使う) をカーソルにすると、
 *   読んでいる途中の更新や遅れて commit された古い時刻を飛び越えて **変更が永久に届かない**。台帳に「伝票ごとの指紋 (ヘッダ + 明細)」を持ち、
 *   毎回 raw を全部読んで指紋を比べれば、時刻に頼らずに「変わった伝票」が決まる (34 万伝票で十数秒)。投入済みの伝票は範囲の条件 (D-28) から外れても追跡する。
 *
 * 持つもの:
 *   shipments_sent  伝票番号 → 指紋 (fp) / 送った世代 / 時刻。'applied' か 'same' が返った伝票だけ書く (failed / stale は書かない = 次回また送る)
 *   meta            batch_seq (世代。取引の中で +1 = 2 つのプロセスが同じ世代を取れない) / lock (送り手の排他。持ち主と開始時刻。ttl を過ぎた lock は死んだとみなす)
 *   runs            送った run の記録 (mode / 世代 / 件数 / 成否。バックフィルの進み具合と、翌朝の「何が失敗したか」)
 */
import Database from 'better-sqlite3';
import path from 'node:path';

export const LEDGER_FILE = 'company-db-push.db';
export const LOCK_KEY = 'lock';
export const SEQ_KEY = 'batch_seq';
export const DEFAULT_LOCK_TTL_MS = 6 * 3600 * 1000;   // daily-sync のステップは 30 分で切られる。6 時間残っている lock は死んだ run のもの

const nowNaive = (d = new Date()) => d.toISOString().replace('T', ' ').slice(0, 19);

export function openLedger(fileOrDataDir, { memory = false } = {}) {
  const file = memory ? ':memory:' : path.join(fileOrDataDir, LEDGER_FILE);
  const db = new Database(file, { timeout: 30000 });
  if (!memory) db.pragma('journal_mode = WAL');
  db.exec(`
    create table if not exists shipments_sent (ne_slip_no text primary key, fp text not null, batch_seq integer not null, sent_at text not null);
    create table if not exists meta (key text primary key, value text, updated_at text);
    create table if not exists runs (run_id text primary key, mode text not null, started_at text not null, finished_at text, batch_seq integer, scanned integer, in_scope integer,
      changed integer, sent integer, applied integer, same integer, stale integer, failed integer, transform_errors integer, ok integer, note text);
  `);
  const stmt = {
    getMeta: db.prepare('select value from meta where key = ?'),
    putMeta: db.prepare('insert into meta (key, value, updated_at) values (?, ?, ?) on conflict (key) do update set value = excluded.value, updated_at = excluded.updated_at'),
    delMeta: db.prepare('delete from meta where key = ?'),
    upsertSent: db.prepare('insert into shipments_sent (ne_slip_no, fp, batch_seq, sent_at) values (?, ?, ?, ?) on conflict (ne_slip_no) do update set fp = excluded.fp, batch_seq = excluded.batch_seq, sent_at = excluded.sent_at'),
    upsertRun: db.prepare(`insert into runs (run_id, mode, started_at, finished_at, batch_seq, scanned, in_scope, changed, sent, applied, same, stale, failed, transform_errors, ok, note)
      values (@run_id, @mode, @started_at, @finished_at, @batch_seq, @scanned, @in_scope, @changed, @sent, @applied, @same, @stale, @failed, @transform_errors, @ok, @note)
      on conflict (run_id) do update set finished_at = excluded.finished_at, batch_seq = excluded.batch_seq, scanned = excluded.scanned, in_scope = excluded.in_scope, changed = excluded.changed,
        sent = excluded.sent, applied = excluded.applied, same = excluded.same, stale = excluded.stale, failed = excluded.failed, transform_errors = excluded.transform_errors, ok = excluded.ok, note = excluded.note`),
  };
  const getMeta = (k) => { const r = stmt.getMeta.get(k); return r && r.value != null ? String(r.value) : null; };
  const txImmediate = (fn) => db.transaction(fn).immediate();
  return {
    db,
    getMeta,
    putMeta: (k, v, at = new Date()) => { stmt.putMeta.run(k, v, nowNaive(at)); },
    /** 送り手の排他。ttl を過ぎた lock は死んだ run のものとみなして奪う。戻り値 = { ok, held: {owner, started_at} | null } */
    acquireLock: ({ owner, now = new Date(), ttlMs = DEFAULT_LOCK_TTL_MS }) => txImmediate(() => {
      const raw = getMeta(LOCK_KEY);
      if (raw) {
        let held = null; try { held = JSON.parse(raw); } catch { held = { owner: '?', started_at: null }; }
        const age = held && held.started_at ? now.getTime() - Date.parse(held.started_at) : Infinity;
        if (age < ttlMs) return { ok: false, held };
      }
      stmt.putMeta.run(LOCK_KEY, JSON.stringify({ owner, started_at: now.toISOString() }), nowNaive(now));
      return { ok: true, held: null };
    }),
    releaseLock: (owner) => txImmediate(() => {
      const raw = getMeta(LOCK_KEY); if (!raw) return false;
      let held = null; try { held = JSON.parse(raw); } catch { held = null; }
      if (held && held.owner !== owner) return false;   // 奪われていたら触らない
      stmt.delMeta.run(LOCK_KEY); return true;
    }),
    /** 世代を取引の中で +1 して返す (2 つのプロセスが同じ世代を取れない) */
    nextBatchSeq: (now = new Date()) => txImmediate(() => {
      const next = (Number(getMeta(SEQ_KEY)) || 0) + 1;
      stmt.putMeta.run(SEQ_KEY, String(next), nowNaive(now));
      return next;
    }),
    currentBatchSeq: () => Number(getMeta(SEQ_KEY)) || 0,
    /** 送付済みの指紋を全部 (伝票番号 → fp) */
    loadFingerprints: () => { const m = new Map(); for (const r of db.prepare('select ne_slip_no, fp from shipments_sent').iterate()) m.set(r.ne_slip_no, r.fp); return m; },
    countSent: () => db.prepare('select count(*) as n from shipments_sent').get().n,
    /** applied / same が返った伝票を書く (1 取引) */
    markSent: (rows, batchSeq, at = new Date()) => { const t = nowNaive(at); db.transaction(() => { for (const r of rows) stmt.upsertSent.run(r.ne_slip_no, r.fp, batchSeq, t); })(); },
    recordRun: (r) => { stmt.upsertRun.run({ finished_at: null, batch_seq: null, scanned: null, in_scope: null, changed: null, sent: null, applied: null, same: null, stale: null, failed: null, transform_errors: null, ok: null, note: null, ...r }); },
    lastRuns: (n = 5) => db.prepare('select * from runs order by started_at desc limit ?').all(n),
    close: () => db.close(),
  };
}
