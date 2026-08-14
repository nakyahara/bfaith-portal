/**
 * rate-limiter のセマフォリーク回帰テスト。
 * 2026-08-14: クライアント切断 ('finish' 不発火) で rakuten セマフォが active=1/pending=5 の
 * まま固着し、以後の全リクエストが 429 になった障害の再発防止。
 *   node apps/warehouse/test-rate-limiter.mjs
 */
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { rateLimitMiddleware, getRateLimitStatus } from './rate-limiter.js';

function fakeRes() {
  const res = new EventEmitter();
  res.destroyed = false;
  res.writableEnded = false;
  res.statusCode = null;
  res.body = null;
  res.status = (c) => { res.statusCode = c; return res; };
  res.json = (b) => { res.body = b; return res; };
  return res;
}
const fakeReq = () => ({ destroyed: false, requestId: 'test' });
const tick = () => new Promise((r) => setImmediate(r));
const mall = 'qoo10';   // 他テストと干渉しないモールを使う
const mw = rateLimitMiddleware(mall);
const stat = () => getRateLimitStatus()[mall];

let passed = 0;
const t = (name) => { passed++; console.log(`  ok: ${name}`); };

// 1. 正常完了: finish + close が両方発火しても release は1回 (activeが負にならない)
{
  const res = fakeRes();
  let nexted = false;
  await mw(fakeReq(), res, () => { nexted = true; });
  assert.equal(nexted, true);
  assert.equal(stat().active, 1);
  res.emit('finish');
  res.emit('close');
  await tick();
  assert.equal(stat().active, 0, 'finish+closeの二重発火でも1回だけrelease');
  t('正常完了はfinishでrelease (close二重発火でも1回)');
}

// 2. クライアント切断: finish は発火せず close だけでも release される (今回の障害の核心)
{
  const res = fakeRes();
  await mw(fakeReq(), res, () => {});
  assert.equal(stat().active, 1);
  res.emit('close');   // タイムアウト切断 = finish なし
  await tick();
  assert.equal(stat().active, 0, 'closeだけでもreleaseされる');
  t('切断 (closeのみ) でもセマフォがリークしない');
}

// 3. acquire 待ちの間に切断: 取得後すぐ release され、処理 (next) には進まない
{
  const res1 = fakeRes();
  await mw(fakeReq(), res1, () => {});           // res1 が active=1 で占有
  const res2 = fakeRes();
  let next2 = false;
  const p2 = mw(fakeReq(), res2, () => { next2 = true; });   // res2 は pending
  await tick();
  assert.equal(stat().pending, 1);
  res2.destroyed = true;                          // 待ちの間に切断 (closeは取得前に発火済み)
  res2.emit('close');
  res1.emit('finish');                            // res1 完了 → res2 が取得
  await p2;
  await tick();
  assert.equal(next2, false, '切断済みリクエストは処理に進まない');
  assert.equal(stat().active, 0, '取得後すぐreleaseされて固着しない');
  assert.equal(stat().pending, 0);
  t('acquire待ち中の切断は取得後即release・nextに進まない');
}

// 4. pending>=5 は 429 RATE_LIMIT_QUEUE_FULL (既存挙動の回帰)
{
  const holder = fakeRes();
  await mw(fakeReq(), holder, () => {});          // active=1
  const waiters = [];
  for (let i = 0; i < 5; i++) {
    const r = fakeRes();
    waiters.push({ res: r, p: mw(fakeReq(), r, () => {}) });
  }
  await tick();
  assert.equal(stat().pending, 5);
  const rejected = fakeRes();
  await mw(fakeReq(), rejected, () => { throw new Error('429のはずがnextに進んだ'); });
  assert.equal(rejected.statusCode, 429);
  assert.equal(rejected.body.error, 'RATE_LIMIT_QUEUE_FULL');
  // 後始末: holder → waiters の順に完了させて全解放
  holder.emit('finish');
  for (const w of waiters) { await tick(); w.res.emit('finish'); }
  await Promise.all(waiters.map((w) => w.p));
  await tick();
  assert.equal(stat().active, 0);
  assert.equal(stat().pending, 0);
  t('pending>=5は429・完了後に全解放');
}

console.log(`\ntest-rate-limiter: ${passed} 件 pass`);
