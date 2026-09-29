#!/usr/bin/env node
/**
 * test-retry-single-run.mjs — 自動再試行の回を 1 つずつにする排他 (apps/warehouse/retry-lock.js) の試験
 *   前の回が生きている = 見送り / 持ち主が死んでいる・壊れた lock = 回収して取る / 朝の daily-sync が生きている = 見送り /
 *   外すのは自分の token だけ (回収された後に古い回が外しても新しい回の lock を消さない) / retry-failed-jobs.js の main が lock の中で動く
 * 実行: node scripts/test-retry-single-run.mjs (一時フォルダだけ)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { acquireRetryLock, releaseRetryLock, otherRunAlive, remainingRetrySlots, RETRY_LOCK_TTL_MS } from '../apps/warehouse/retry-lock.js';

let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'retry-lock-test-'));
const lockFile = path.join(dir, 'retry-failed-jobs.lock.json');
const dsLock = path.join(dir, 'daily-sync.lock.json');
const alive = new Set();
const isAlive = (pid) => alive.has(pid);
const clean = () => { for (const f of fs.readdirSync(dir)) fs.rmSync(path.join(dir, f), { force: true }); alive.clear(); };
const NOW = new Date('2026-09-30T00:00:00Z');
const at = (msAgo) => new Date(NOW.getTime() - msAgo).toISOString();

t('1 回目は取れる・持ち主が生きている間の 2 回目は見送り (retry-state には触らない)', () => {
  clean(); alive.add(101);
  const a = acquireRetryLock({ lockFile, isAlive, pid: 101 });
  assert.equal(a.ok, true);
  const b = acquireRetryLock({ lockFile, isAlive, pid: 202 });
  assert.equal(b.ok, false); assert.match(b.reason, /前の再試行の回がまだ動いている \(pid 101/);
  assert.equal(releaseRetryLock(a), 'deleted');
  assert.equal(fs.existsSync(lockFile), false);
  assert.equal(acquireRetryLock({ lockFile, isAlive, pid: 202 }).ok, true);
});
t('持ち主が死んでいる lock・壊れた lock は残骸として回収して取る', () => {
  clean();
  fs.writeFileSync(lockFile, JSON.stringify({ token: 'x', pid: 303, started_at: '2026-09-29T00:00:00Z' }));
  const a = acquireRetryLock({ lockFile, isAlive, pid: 404 });
  assert.equal(a.ok, true); assert.match(a.recovered, /pid 303/);
  releaseRetryLock(a);
  fs.writeFileSync(lockFile, '{壊れた');
  const b = acquireRetryLock({ lockFile, isAlive, pid: 404 });
  assert.equal(b.ok, true); assert.equal(b.recovered, '(壊れた lock)');
});
t('朝の daily-sync の持ち主が生きている間は見送る・死んでいれば取る', () => {
  clean(); alive.add(505);
  fs.writeFileSync(dsLock, JSON.stringify({ run_id: 'r', pid: 505, started_at: at(3600 * 1000) }));
  const a = acquireRetryLock({ lockFile, dailySyncLockFile: dsLock, isAlive, pid: 606, now: NOW });
  assert.equal(a.ok, false); assert.equal(a.dailySync, true); assert.match(a.reason, /朝の daily-sync がまだ動いている/);
  assert.equal(fs.existsSync(lockFile), false);
  alive.delete(505);
  assert.equal(acquireRetryLock({ lockFile, dailySyncLockFile: dsLock, isAlive, pid: 606, now: NOW }).ok, true);
});
t('自分の lock を書いた後に daily-sync が動き始めていたら、自分の lock を外して退く (逆の順の起動・#1538 Codex R1 High)', () => {
  clean();
  fs.writeFileSync(dsLock, JSON.stringify({ run_id: 'r', pid: 505, started_at: at(60 * 1000) }));
  let calls = 0;
  const flip = (pid) => (pid === 505 ? ++calls >= 2 : alive.has(pid));   // 1 回目の確かめでは死んでいる・書いた後の 2 回目では生きている
  const a = acquireRetryLock({ lockFile, dailySyncLockFile: dsLock, isAlive: flip, pid: 606, now: NOW });
  assert.equal(a.ok, false); assert.equal(a.dailySync, true); assert.match(a.reason, /動き始めた/);
  assert.equal(fs.existsSync(lockFile), false);   // 自分の lock は外した
});
t('期限: 持ち主の pid が生きていても started_at が期限より古い lock は残骸 (pid の使い回しで永久に見送らない・#1538 Codex R1 Medium)', () => {
  clean(); alive.add(111);
  fs.writeFileSync(lockFile, JSON.stringify({ token: 'x', pid: 111, started_at: at(RETRY_LOCK_TTL_MS + 60000) }));
  const a = acquireRetryLock({ lockFile, isAlive, pid: 222, now: NOW });
  assert.equal(a.ok, true); assert.match(a.recovered, /pid 111/);
  releaseRetryLock(a);
  fs.writeFileSync(dsLock, JSON.stringify({ run_id: 'r', pid: 111, started_at: at(13 * 3600 * 1000) }));   // daily-sync の lock も 12 時間より古ければ動いていない扱い
  assert.equal(otherRunAlive(dsLock, { isAlive, now: NOW, ttlMs: 12 * 3600 * 1000 }), null);
  assert.equal(acquireRetryLock({ lockFile, dailySyncLockFile: dsLock, isAlive, pid: 222, now: NOW }).ok, true);
});
t('外すのは自分の token だけ (回収された古い回が後から外しても、新しい回の lock は消えない)', () => {
  clean();
  const old = acquireRetryLock({ lockFile, isAlive, pid: 707 });   // 707 は「死んだ」扱い (alive に入れない)
  const fresh = acquireRetryLock({ lockFile, isAlive, pid: 808 });   // 残骸として回収して取る
  assert.equal(fresh.ok, true); assert.ok(fresh.recovered);
  assert.equal(releaseRetryLock(old), 'not-mine');   // 中身を先に読んで自分のものでない = 名前も変えない (#1538 Codex R1 Low)
  assert.deepEqual(fs.readdirSync(dir).filter((f) => f.includes('.claim-')), []);
  assert.equal(JSON.parse(fs.readFileSync(lockFile, 'utf8')).token, fresh.token);
  assert.equal(releaseRetryLock(fresh), 'deleted');
});
t('lock を書けない (フォルダが無い) = error で止まる側', () => {
  const r = acquireRetryLock({ lockFile: path.join(dir, 'no-such-dir', 'x.json'), isAlive, pid: 909 });
  assert.equal(r.ok, false); assert.equal(r.error, true);
});
t('残っている再試行の時刻 (JST): 7:00 = 3 つ・9:00 = 2 つ・11:31 = 無い', () => {
  assert.deepEqual(remainingRetrySlots(new Date('2026-09-29T22:00:00Z')), ['08:30', '10:00', '11:30']);
  assert.deepEqual(remainingRetrySlots(new Date('2026-09-30T00:00:00Z')), ['10:00', '11:30']);
  assert.deepEqual(remainingRetrySlots(new Date('2026-09-30T02:31:00Z')), []);
});
t('daily-sync.js: 自分の lock を書いた後に再試行の lock を見て、動いていれば自分の lock を外して止まる・通知は残っている時刻だけ', () => {
  const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
  const ds = fs.readFileSync(path.join(root, 'apps/warehouse/daily-sync.js'), 'utf8');
  assert.match(ds, /myLockRunId = runId;\s*\/\/[^\n]*\n\s*const retryRun = otherRunAlive\(RETRY_LOCK_FILE, \{ isAlive: isAliveNodeProcess, ttlMs: RETRY_LOCK_TTL_MS \}\);\s*if \(retryRun\) \{\s*releaseLock\(\);/);
  assert.match(ds, /RETRY_LOCK_FILE = path\.join\(PROJECT_DIR, 'data', 'retry-failed-jobs\.lock\.json'\)/);
  assert.match(ds, /const slots = remainingRetrySlots\(new Date\(\)\);/);
  const rj = fs.readFileSync(path.join(root, 'apps/warehouse/retry-failed-jobs.js'), 'utf8');
  assert.match(rj, /RETRY_LOCK_FILE = path\.join\(PROJECT_DIR, 'data', 'retry-failed-jobs\.lock\.json'\)/);
  assert.match(rj, /if \(lock\.error \|\| lock\.dailySync \|\| fs\.existsSync\(RETRY_STATE_FILE\)\) await notify/);
});
t('retry-failed-jobs.js: main は lock を取ってから state を読み、終わったら (失敗しても) 外す・daily-sync と同じ lock ファイルを見る', () => {
  const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
  const src = fs.readFileSync(path.join(root, 'apps/warehouse/retry-failed-jobs.js'), 'utf8');
  const iLock = src.indexOf('const lock = acquireRetryLock('), iLoad = src.indexOf('const loadResult = loadState();');
  assert.ok(iLock > 0 && iLoad > iLock, 'lock を取る前に state を読んでいる');
  assert.match(src, /try \{\s*await runLocked\(\);\s*\} finally \{\s*releaseRetryLock\(lock\);/);
  assert.match(src, /DAILY_SYNC_LOCK_FILE = path\.join\(PROJECT_DIR, 'data', 'daily-sync\.lock\.json'\)/);
  const ds = fs.readFileSync(path.join(root, 'apps/warehouse/daily-sync.js'), 'utf8');
  assert.match(ds, /const LOCK_FILE = path\.join\(PROJECT_DIR, 'data', 'daily-sync\.lock\.json'\);/);
});

fs.rmSync(dir, { recursive: true, force: true });
console.log(`\n${ng === 0 ? '✅' : '❌'} 再試行の排他: ${ok} ok / ${ng} NG`);
process.exitCode = ng === 0 ? 0 : 1;
