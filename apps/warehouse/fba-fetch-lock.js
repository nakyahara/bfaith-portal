/**
 * fba-fetch-lock.js — SP-API レポート取得のプロセス跨ぎ排他
 *
 * cron (snapshot-fba-stock.js) と手動実行 (fba-service.js /fetch-reports) が
 * 同時に走ると SP-API 呼び出しが重複し、レート制限・データ破損を招く。
 * lockfile ベースで両方を排他する。
 *
 * 原子性: fs.openSync(path, 'wx') (排他フラグ) で TOCTOU 回避。
 * 所有権: release 時に PID 一致確認、別プロセスの lock を消さない。
 *
 * 使い方:
 *   const lock = acquireFbaFetchLock('cron');
 *   if (!lock.acquired) {
 *     console.log('既に実行中:', lock.holder);
 *     return;
 *   }
 *   try { ... } finally { releaseFbaFetchLock(lock); }
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');
const LOCK_PATH = path.join(PROJECT_DIR, 'data', 'fba-fetch.lock');

// SP-API レポート取得は polling 含めて最大 10 分程度。stale 判定は 30 分。
const STALE_THRESHOLD_MS = 30 * 60 * 1000;

function readLockMeta() {
  try {
    return JSON.parse(fs.readFileSync(LOCK_PATH, 'utf8'));
  } catch {
    return null;
  }
}

/**
 * @param {string} source - 'cron' | 'manual' など、保持者の識別子
 * @returns {{acquired: boolean, lockPath?: string, holder?: object, ownerPid?: number, ownerToken?: string}}
 */
export function acquireFbaFetchLock(source) {
  fs.mkdirSync(path.dirname(LOCK_PATH), { recursive: true });

  // ownerToken: PID だけだと PID 再利用で誤一致するため、ランダム文字列を併記
  const ownerToken = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const meta = {
    pid: process.pid,
    ownerToken,
    source,
    started_at: new Date().toISOString(),
    hostname: process.env.COMPUTERNAME || process.env.HOSTNAME || 'unknown',
  };
  const payload = JSON.stringify(meta, null, 2);

  // 試行1: 排他作成 (TOCTOU 回避)
  try {
    const fd = fs.openSync(LOCK_PATH, 'wx');
    try {
      fs.writeSync(fd, payload);
    } finally {
      fs.closeSync(fd);
    }
    return { acquired: true, lockPath: LOCK_PATH, ownerToken, holder: meta };
  } catch (e) {
    if (e.code !== 'EEXIST') {
      console.error('[fba-fetch-lock] acquire 例外:', e.message);
      return { acquired: false, holder: { error: e.message } };
    }
  }

  // 既に存在: stale チェック
  const existing = readLockMeta();
  if (!existing || !existing.started_at) {
    // 不正な lock → 削除して再試行
    try { fs.unlinkSync(LOCK_PATH); } catch {}
    return acquireFbaFetchLock(source);
  }
  const ageMs = Date.now() - new Date(existing.started_at).getTime();
  if (ageMs >= STALE_THRESHOLD_MS) {
    // stale → 強制取得 (古い lock を消して排他作成リトライ)
    console.warn('[fba-fetch-lock] stale lock を上書き:', existing);
    try { fs.unlinkSync(LOCK_PATH); } catch {}
    return acquireFbaFetchLock(source);
  }
  // 有効な保持者あり
  return { acquired: false, holder: existing };
}

/**
 * lock を解放。所有権チェック付き (別プロセスが取った lock を消さない)
 * @param {{lockPath: string, ownerToken: string} | string} lockOrPath - acquire の戻り値全体、または lockPath 文字列 (旧API互換)
 */
export function releaseFbaFetchLock(lockOrPath) {
  const lockPath = (lockOrPath && typeof lockOrPath === 'object' && lockOrPath.lockPath) || lockOrPath || LOCK_PATH;
  const myToken = (lockOrPath && typeof lockOrPath === 'object') ? lockOrPath.ownerToken : null;

  const existing = readLockMeta();
  if (!existing) return; // 既に消えてる
  if (myToken && existing.ownerToken && existing.ownerToken !== myToken) {
    // 別プロセスが取った lock → 触らない
    console.warn('[fba-fetch-lock] release: 別プロセスの lock のため削除スキップ', { my: myToken, existing: existing.ownerToken });
    return;
  }
  try {
    fs.unlinkSync(lockPath);
  } catch (e) {
    if (e.code !== 'ENOENT') console.error('[fba-fetch-lock] release 失敗:', e.message);
  }
}
