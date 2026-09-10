'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { hash } = require('./common.cjs');
const { RunLedger } = require('./run-ledger.cjs');

function setup(t) { const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-run-ledger-')); t.after(() => fs.rmSync(dir, { recursive: true, force: true })); return dir; }
function input(run_id = 'run-1') { return { run_id, target_date: '2026-09-10', deadline: '2026-09-10T05:30:00+09:00', input_hash: hash('input'), input_version: 'seed-v1', model_plan: { R01: 'claude-sonnet-5' } }; }

test('run開始時に空の予算状態を永続化し、同じrunの二重実行をロックする', (t) => {
  const ledger = new RunLedger(setup(t)); const session = ledger.acquire(input());
  assert.equal(ledger.inspect('run-1').run.status, 'running');
  assert.throws(() => ledger.acquire(input()), /RUN_LOCKED/);
  session.release();
  const resumed = ledger.acquire(input()); assert.equal(resumed.budget().snapshot().calls.length, 0); resumed.release();
});

test('AI呼出前の予約は永続化され、障害後に自動で同じ呼出を再実行しない', (t) => {
  t.mock.timers.enable({apis:['Date'],now:new Date('2026-09-09T12:00:00Z')});
  const ledger = new RunLedger(setup(t)); const session = ledger.acquire(input());
  const budget = session.budget(); budget.reserve('R01'); session.saveBudget(budget.snapshot()); session.release();
  assert.throws(() => ledger.acquire(input()), /RUN_NEEDS_REVIEW/);
  assert.equal(ledger.inspect('run-1').run.finish_reason, 'UNRESOLVED_RESERVATION');
  assert.equal(ledger.inspect('run-1').lock, null);
});

test('モデル結果はメタデータだけを保存し、完了時にロックを解放する', (t) => {
  const ledger = new RunLedger(setup(t)); const session = ledger.acquire(input());
  session.recordStage('R01', { status: 'OK', requested_model: 'claude-sonnet-5', actual_model: 'claude-sonnet-5', usage: { kind: 'measured', input_tokens: 12, output_tokens: 5 }, response: '保存してはいけない本文' });
  session.finish('partial', 'MARKET_LOOKUP_PENDING');
  const state = ledger.inspect('run-1');
  assert.equal(state.run.stage_results[0].status, 'OK');
  assert.doesNotMatch(JSON.stringify(state.run), /保存してはいけない本文/);
  assert.equal(state.lock, null);
});

test('期限切れのロックだけを明示的な復旧操作で解除できる', (t) => {
  let now = Date.parse('2026-09-09T00:00:00Z'); const ledger = new RunLedger(setup(t), { now: () => now }); const session = ledger.acquire(input()); session.closed = true;
  assert.throws(() => ledger.recoverStaleLock('run-1', { max_age_ms: 60000, now }), /LOCK_NOT_STALE/);
  now += 60000; assert.equal(ledger.recoverStaleLock('run-1', { max_age_ms: 60000, now }).recovered_run_id, 'run-1');
  assert.equal(ledger.inspect('run-1').lock, null);
});

test('異なる入力、完了済みrun、不正なrun IDで再開しない', (t) => {
  const ledger = new RunLedger(setup(t)); const session = ledger.acquire(input()); session.finish('completed');
  assert.throws(() => ledger.acquire(input()), /RUN_FINALIZED/);
  assert.throws(() => ledger.acquire({ ...input(), run_id: '../bad' }), /INVALID_RUN_ID/);
  const active = ledger.acquire({ ...input('run-2'), input_hash: hash('two') }); active.release();
  assert.throws(() => ledger.acquire({ ...input('run-2'), input_hash: hash('other') }), /RUN_INPUT_MISMATCH/);
});
