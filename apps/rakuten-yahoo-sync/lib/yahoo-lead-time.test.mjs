/**
 * 発送日 ID の事前確認: 一度の失敗をプロセス終了まで覚えない (2026-10-22 修正の再発防止)。
 * 実行: node --test apps/rakuten-yahoo-sync/lib/yahoo-lead-time.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runLeadTimePreflight, _resetForTest, FAILURE_TTL_MS } from './yahoo-lead-time.js';

test('失敗は短い間だけ覚え、過ぎたら取り直す', async (t) => {
  _resetForTest();
  delete process.env.RYS_LEAD_TIME_VERIFIED;
  let calls = 0;
  const fail = async () => { calls++; throw new Error('proxy 502'); };
  const r1 = await runLeadTimePreflight({ caller: fail });
  assert.equal(r1.ok, false);
  const r2 = await runLeadTimePreflight({ caller: fail });
  assert.equal(r2.ok, false);
  assert.equal(calls, 1, '短い間は覚えた失敗を返す (sweep で叩き続けない)');
  t.mock.timers.enable({ apis: ['Date'], now: Date.now() + FAILURE_TTL_MS + 1 });
  const r3 = await runLeadTimePreflight({ caller: async () => { calls++; return { ok: true, targetId: 1000 }; } });
  assert.equal(r3.ok, true, '失敗の記憶が切れたら取り直して成功になる');
  assert.equal(calls, 2);
  t.mock.timers.reset();
});

test('緊急回避の env は覚えた失敗より先に効く', async () => {
  _resetForTest();
  await runLeadTimePreflight({ caller: async () => ({ ok: false, error: 'x' }) });
  process.env.RYS_LEAD_TIME_VERIFIED = '1';
  try {
    const r = await runLeadTimePreflight();
    assert.equal(r.ok, true);
    assert.equal(r.source, 'env_override_stub');
  } finally {
    delete process.env.RYS_LEAD_TIME_VERIFIED;
    _resetForTest();
  }
});
