/**
 * retry-lock.js — 自動再試行 (retry-failed-jobs.js) の回を 1 つずつしか動かさない排他 (2026-09-29・#1536 Codex R2 Medium)
 *
 * なぜ要るか: 再試行は 08:30 / 10:00 / 11:30 に Task Scheduler が起動する。1 回の中の工程は 30〜60 分の上限が 10 個以上並ぶ
 *   (Amazon Settlement 60 分 → Company DB の送り手 30 分 …) = 90 分の間隔を超えうる。前の回が終わる前に次の回が同じ retry-state を読むと、
 *   同じ工程が並んで走る (SQLite の書き込みが重なる)・片方が消した state をもう片方が古い remaining_jobs で書き戻す (復旧済みの工程をまた走らせる・誤った最終失敗の通知)。
 *   朝の daily-sync がまだ動いている間に再試行が走るのも同じ (daily-sync が最後に retry-state を書く)。
 *
 * 決め:
 *   - data/retry-failed-jobs.lock.json を排他的に作る (flag 'wx')。持ち主 = { token, pid, started_at }
 *   - 既にあって持ち主の node が生きている = 前の回がまだ動いている → 見送る (retry-state には触らない = 動いている回が結果を書く・次の回が拾う)
 *   - 持ち主が死んでいる・壊れた lock = 残骸 → 自分が読んだ中身とバイト一致するものだけを回収してから取り直す (daily-sync の lock と同じ手順 = 名前を変えて奪う)
 *   - 朝の daily-sync の lock (data/daily-sync.lock.json) の持ち主が生きている → 見送る
 *   - 外すのは自分の token の lock だけ
 */
import fs from 'fs';
import crypto from 'crypto';
import { execFileSync } from 'child_process';

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

const readRaw = (f) => { try { return fs.readFileSync(f, 'utf-8'); } catch { return null; } };
const parse = (raw) => { try { return raw == null ? null : JSON.parse(raw); } catch { return null; } };

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
 * 取る。戻り値 = { ok: true, token, lockFile, recovered } / { ok: false, reason, error? }
 *   error = lock を書けない (data の異常) = 呼び手は通知して止まる
 */
export function acquireRetryLock({ lockFile, dailySyncLockFile = null, isAlive = isAliveNodeProcess, now = new Date(), pid = process.pid }) {
  if (dailySyncLockFile) {
    const ds = parse(readRaw(dailySyncLockFile));
    if (ds && Number.isInteger(ds.pid) && isAlive(ds.pid)) return { ok: false, reason: `朝の daily-sync がまだ動いている (pid ${ds.pid}・開始 ${ds.started_at})` };
  }
  let recovered = null;
  for (let i = 0; i < 3; i++) {
    const token = `${pid}-${crypto.randomUUID()}`;
    try {
      fs.writeFileSync(lockFile, JSON.stringify({ token, pid, started_at: now.toISOString() }), { flag: 'wx' });
      return { ok: true, token, lockFile, recovered };
    } catch (e) {
      if (e.code !== 'EEXIST') return { ok: false, error: true, reason: `lock を書けない (${e.message})。data の異常を確かめる: ${lockFile}` };
    }
    const raw = readRaw(lockFile);
    if (raw === null) continue;   // 消えた直後 = もう一度 wx で決着
    const prev = parse(raw);
    if (prev && Number.isInteger(prev.pid) && isAlive(prev.pid)) return { ok: false, reason: `前の再試行の回がまだ動いている (pid ${prev.pid}・開始 ${prev.started_at})` };
    const r = claimAndDelete(lockFile, (x) => x === raw);   // 残骸 (持ち主が死んでいる・壊れている) = 読んだ中身と一致するものだけ回収
    if (r === 'restored' || r === 'conflict') return { ok: false, reason: 'lock の回収で別の再試行の回と競合した (その回が動く)' };
    recovered = prev ? `pid ${prev.pid}・開始 ${prev.started_at}` : '(壊れた lock)';
  }
  return { ok: false, reason: 'lock を取れない (取り合いが続いた)' };
}

/** 外す (自分の token の lock だけ) */
export function releaseRetryLock(handle) {
  if (!handle || !handle.ok) return 'none';
  return claimAndDelete(handle.lockFile, (raw) => { const j = parse(raw); return !!j && j.token === handle.token; });
}
