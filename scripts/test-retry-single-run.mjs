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
import { acquireRetryLock, releaseRetryLock } from '../apps/warehouse/retry-lock.js';

let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'retry-lock-test-'));
const lockFile = path.join(dir, 'retry-failed-jobs.lock.json');
const dsLock = path.join(dir, 'daily-sync.lock.json');
const alive = new Set();
const isAlive = (pid) => alive.has(pid);
const clean = () => { for (const f of fs.readdirSync(dir)) fs.rmSync(path.join(dir, f), { force: true }); alive.clear(); };

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
  fs.writeFileSync(dsLock, JSON.stringify({ run_id: 'r', pid: 505, started_at: '2026-09-29T22:00:00Z' }));
  const a = acquireRetryLock({ lockFile, dailySyncLockFile: dsLock, isAlive, pid: 606 });
  assert.equal(a.ok, false); assert.match(a.reason, /朝の daily-sync がまだ動いている/);
  assert.equal(fs.existsSync(lockFile), false);
  alive.delete(505);
  assert.equal(acquireRetryLock({ lockFile, dailySyncLockFile: dsLock, isAlive, pid: 606 }).ok, true);
});
t('外すのは自分の token だけ (回収された古い回が後から外しても、新しい回の lock は消えない)', () => {
  clean();
  const old = acquireRetryLock({ lockFile, isAlive, pid: 707 });   // 707 は「死んだ」扱い (alive に入れない)
  const fresh = acquireRetryLock({ lockFile, isAlive, pid: 808 });   // 残骸として回収して取る
  assert.equal(fresh.ok, true); assert.ok(fresh.recovered);
  assert.equal(releaseRetryLock(old), 'restored');   // 中身が違う = 戻す
  assert.equal(JSON.parse(fs.readFileSync(lockFile, 'utf8')).token, fresh.token);
  assert.equal(releaseRetryLock(fresh), 'deleted');
});
t('lock を書けない (フォルダが無い) = error で止まる側', () => {
  const r = acquireRetryLock({ lockFile: path.join(dir, 'no-such-dir', 'x.json'), isAlive, pid: 909 });
  assert.equal(r.ok, false); assert.equal(r.error, true);
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
