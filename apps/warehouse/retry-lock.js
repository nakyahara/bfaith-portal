/**
 * retry-lock.js — 自動再試行 (retry-failed-jobs.js) の回を 1 つずつしか動かさない排他 (2026-09-29・#1536 Codex R2 Medium / #1538)
 *
 * なぜ要るか: 再試行は 08:30 / 10:00 / 11:30 に Task Scheduler が起動する。1 回の中の工程は 30〜60 分の上限が 10 個以上並ぶ
 *   (Amazon Settlement 60 分 → Company DB の送り手 30 分 …) = 90 分の間隔を超えうる。前の回が終わる前に次の回が同じ retry-state を読むと、
 *   同じ工程が並んで走る (SQLite の書き込みが重なる)・片方が消した state をもう片方が古い remaining_jobs で書き戻す (復旧済みの工程をまた走らせる・誤った最終失敗の通知)。
 *   朝の daily-sync と再試行が重なるのも同じ (daily-sync が最後に retry-state を書く)。
 *
 * 決め:
 *   - data/retry-failed-jobs.lock.json を排他的に作る (flag 'wx')。持ち主 = { token, pid, started_at }
 *   - 既にあって持ち主が生きている = 前の回がまだ動いている → 見送る (retry-state には触らない = 動いている回が結果を書く・次の回が拾う)
 *     「生きている」= その pid が生きている node で、**そのプロセスが lock の started_at より前から動いている** (後から始まったプロセス = pid の使い回し = 残骸)。
 *       期限では奪わない (長い工程 = DB バックアップ最大 6 時間 を実行中の生きた回から奪わない。#1538 Codex R2 Medium)。started_at が未来・読めない lock も残骸
 *   - 持ち主が死んでいる・期限切れ・壊れた lock = 残骸 → 自分が読んだ中身とバイト一致するものだけを回収してから取り直す (daily-sync の lock と同じ手順 = 名前を変えて奪う)
 *   - 朝の daily-sync と互いに避ける: **どちらも「自分の lock を書いた後に相手の lock を見る」** (daily-sync.js も retry の lock を見る) = 同時に起動しても、少なくとも片方は相手に気づいて退く
 *   - 外すのは自分の token の lock だけ (中身を先に読んで自分のものでなければ触らない)
 *   - 残る狭い競合: 3 つの回が同時に同じ残骸を回収し合うと、読んでから名前を変えるまでの間に別の回の新しい lock を外しうる (定刻の起動は 90 分おき = 起きない。手で何度も同時に起動しない)
 */
import fs from 'fs';
import crypto from 'crypto';
import { execFileSync } from 'child_process';

export const START_SKEW_MS = 120000;           // プロセスの開始時刻と lock の started_at の許す差 (lock はプロセスが始まった後に書く = 開始時刻 ≦ started_at + この差)
export const FUTURE_SKEW_MS = 10 * 60 * 1000;  // started_at がこれより未来 = 壊れた lock
export const RETRY_SLOTS_JST = ['08:30', '10:00', '11:30'];   // Task Scheduler の WarehouseDailySyncRetry1〜3 (台帳 warehouse-daily-sync)
/** いま (JST) から後に残っている再試行の時刻 (daily-sync が 11:30 より後に retry-state を書いた日は空 = その日は自動で再試行されない) */
export function remainingRetrySlots(now = new Date()) {
  const hm = new Date(now.getTime() + 9 * 3600 * 1000).toISOString().slice(11, 16);
  return RETRY_SLOTS_JST.filter((s) => s > hm);
}

function isPidAlive(pid) {
  try { process.kill(pid, 0); return true; } catch (e) { return !!(e && e.code === 'EPERM'); }
}
/** その pid が生きている node か (pid の使い回しで別のプロセスを「生きている」と見ない。tasklist が使えなければ生きている側に倒す = 並走しない) */
export function isAliveNodeProcess(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  if (!isPidAlive(pid)) return false;
  if (process.platform !== 'win32') return true;
  try {
    const out = execFileSync('tasklist', ['/FI', `PID eq ${pid}`, '/FO', 'CSV', '/NH'], { encoding: 'utf-8', timeout: 10000 });
    return /^"node(\.exe)?"/i.test(out.trim());
  } catch {
    return true;
  }
}

/** そのプロセスの開始時刻 (Windows = PowerShell の Get-Process)。取れなければ null */
export function processStartTime(pid) {
  if (process.platform !== 'win32') return null;
  try {
    const out = execFileSync('powershell', ['-NoProfile', '-NonInteractive', '-Command', `(Get-Process -Id ${pid} -ErrorAction Stop).StartTime.ToUniversalTime().ToString('o')`], { encoding: 'utf-8', timeout: 15000, windowsHide: true });
    const t = Date.parse(out.trim());
    return Number.isFinite(t) ? new Date(t) : null;
  } catch {
    return null;
  }
}
/**
 * lock の持ち主 (pid・started_at) がいま動いているか = pid が生きている node で、そのプロセスが started_at より前 (+ 許す差) から動いている。
 *   開始時刻が取れなければ生きている側に倒す (並走しない)
 */
export function isAliveNodeSince(pid, startedAt, { startTimeOf = processStartTime, aliveNode = isAliveNodeProcess } = {}) {
  if (!aliveNode(pid)) return false;
  const lockAt = Date.parse(startedAt);
  if (!Number.isFinite(lockAt)) return false;
  const start = startTimeOf(pid);
  if (!start) return true;
  return start.getTime() <= lockAt + START_SKEW_MS;
}

const readRaw = (f) => { try { return fs.readFileSync(f, 'utf-8'); } catch { return null; } };
const parse = (raw) => { try { return raw == null ? null : JSON.parse(raw); } catch { return null; } };

/** lock の中身の持ち主がいま動いているか。isAlive(pid, started_at)。started_at が読めない・未来 = 壊れた lock = 動いていない */
function holderAlive(j, { isAlive, now }) {
  if (!j || !Number.isInteger(j.pid)) return false;
  const t = Date.parse(j.started_at);
  if (!Number.isFinite(t) || t > now.getTime() + FUTURE_SKEW_MS) return false;
  return !!isAlive(j.pid, j.started_at);
}
/** 別の回 (daily-sync / 再試行) の lock の持ち主がいま動いていれば { pid, started_at }・いなければ null */
export function otherRunAlive(lockFile, { isAlive = isAliveNodeSince, now = new Date() } = {}) {
  const j = parse(readRaw(lockFile));
  return holderAlive(j, { isAlive, now }) ? { pid: j.pid, started_at: j.started_at } : null;
}

/** 名前を変えて奪い、中身が matchFn に合えば消す・合わなければ戻す (daily-sync.js の claimAndDeleteLock と同じ手順) */
function claimAndDelete(lockFile, matchFn) {
  const claim = `${lockFile}.claim-${process.pid}-${crypto.randomUUID()}`;
  try { fs.renameSync(lockFile, claim); } catch { return 'gone'; }
  const raw = readRaw(claim);
  let mine = false;
  try { mine = raw !== null && matchFn(raw); } catch { mine = false; }
  if (mine) { try { fs.unlinkSync(claim); } catch { /* 残っても実害なし */ } return 'deleted'; }
  try { fs.renameSync(claim, lockFile); return 'restored'; }
  catch { try { fs.unlinkSync(claim); } catch { /* 残っても実害なし */ } return 'conflict'; }
}

/**
 * 取る。戻り値 = { ok: true, token, lockFile, recovered } / { ok: false, reason, error?, dailySync? }
 *   error = lock を書けない (data の異常) = 呼び手は通知して止まる / dailySync = 朝の daily-sync が動いているので見送った
 */
export function acquireRetryLock({ lockFile, dailySyncLockFile = null, isAlive = isAliveNodeSince, now = new Date(), pid = process.pid }) {
  const dsBusy = () => (dailySyncLockFile ? otherRunAlive(dailySyncLockFile, { isAlive, now }) : null);
  const ds0 = dsBusy();
  if (ds0) return { ok: false, dailySync: true, reason: `朝の daily-sync がまだ動いている (pid ${ds0.pid}・開始 ${ds0.started_at})` };
  let recovered = null;
  for (let i = 0; i < 3; i++) {
    const token = `${pid}-${crypto.randomUUID()}`;
    try {
      fs.writeFileSync(lockFile, JSON.stringify({ token, pid, started_at: now.toISOString() }), { flag: 'wx' });
      // 自分の lock を書いた後にもう一度 daily-sync を見る (daily-sync も同じ順 = 同時に起動しても片方は必ず気づく。#1538 Codex R1 High)
      const ds1 = dsBusy();
      if (ds1) {
        releaseRetryLock({ ok: true, token, lockFile });
        return { ok: false, dailySync: true, reason: `朝の daily-sync が動き始めた (pid ${ds1.pid}・開始 ${ds1.started_at})` };
      }
      return { ok: true, token, lockFile, recovered };
    } catch (e) {
      if (e.code !== 'EEXIST') return { ok: false, error: true, reason: `lock を書けない (${e.message})。data の異常を確かめる: ${lockFile}` };
    }
    const raw = readRaw(lockFile);
    if (raw === null) continue;   // 消えた直後 = もう一度 wx で決着
    const prev = parse(raw);
    if (holderAlive(prev, { isAlive, now })) return { ok: false, reason: `前の再試行の回がまだ動いている (pid ${prev.pid}・開始 ${prev.started_at})` };
    const r = claimAndDelete(lockFile, (x) => x === raw);   // 残骸 (持ち主が死んでいる・pid の使い回し・壊れている) = 読んだ中身と一致するものだけ回収
    if (r === 'restored' || r === 'conflict') return { ok: false, reason: 'lock の回収で別の再試行の回と競合した (その回が動く)' };
    recovered = prev ? `pid ${prev.pid}・開始 ${prev.started_at}` : '(壊れた lock)';
  }
  return { ok: false, reason: 'lock を取れない (取り合いが続いた)' };
}

/** 外す (自分の token の lock だけ。先に中身を読み、自分のものでなければ名前も変えない = 他の回の lock を一瞬も外さない) */
export function releaseRetryLock(handle) {
  if (!handle || !handle.ok) return 'none';
  const j = parse(readRaw(handle.lockFile));
  if (!j || j.token !== handle.token) return 'not-mine';
  return claimAndDelete(handle.lockFile, (raw) => { const x = parse(raw); return !!x && x.token === handle.token; });
}
