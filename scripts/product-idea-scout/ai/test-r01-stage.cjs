'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { hash } = require('./common.cjs');
const { RunLedger } = require('./run-ledger.cjs');
const { buildR01Input } = require('./seeds.cjs');
const { evaluateR01, R01_PROMPT } = require('./r01-stage.cjs');
const received_at = '2026-09-09T09:00:00+09:00';

function setup(t) { const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-r01-')); t.after(() => fs.rmSync(dir, { recursive: true, force: true })); return dir; }
function input() { return buildR01Input({ run_id: 'r01-run', received_at, representative_seeds: [{ raw_kw: '赤飯 蒸し布' }] }); }
function session(t, input) { t.mock.timers.enable({apis:['Date'],now:new Date('2026-09-09T12:00:00Z')}); const ledger = new RunLedger(setup(t)); return ledger.acquire({ run_id: input.run_id, target_date: '2026-09-10', deadline: '2026-09-10T05:30:00+09:00', input_hash: hash(input), input_version: 'seed-v1', model_plan: { R01: 'claude-sonnet-5' } }); }
function output(input) { return { schema_version: input.schema_version, run_id: input.run_id, stage: 'R01', rule_version: input.rule_version, unknowns: [], requested_evidence: [], items: [{ kw: '蒸し器 布', use: '蒸し器で食材を包む', target: '家庭用蒸し器', form: '布', spec_hypothesis: '綿の2枚組', from_seed_id: input.seeds[0].seed_id, why_this_seed: '蒸し布という入力語を具体化', to_verify: ['競合の実在'], novelty_check: '既存案との照合が必要' }] }; }
async function fakeInvoke(stage, _prompt, options) {
  const reservation = options.budget.reserve(stage); await options.save_budget(options.budget.snapshot());
  options.budget.finish(reservation.id, { status: 'OK', usage: { kind: 'measured', input_tokens: 10, output_tokens: 5 } }); await options.save_budget(options.budget.snapshot());
  return { status: 'OK', requested_model: 'claude-sonnet-5', actual_model: 'claude-sonnet-5', effort: 'low', usage: { kind: 'measured', input_tokens: 10, output_tokens: 5 }, response: JSON.stringify(output(options.input)) };
}

test('R01は固定プロンプト、保存済み予約、形式検証を通して結果を返す', async (t) => {
  const value = input(); const run = session(t, value); let seen = '';
  const result = await evaluateR01(value, { session: run, invokeFn: async (stage, prompt, options) => { seen = prompt; options.input = value; return fakeInvoke(stage, prompt, options); } });
  assert.equal(result.status, 'OK'); assert.equal(result.output.items[0].kw, '蒸し器 布'); assert.match(seen, /<untrusted_seed_input>/); assert.match(R01_PROMPT, /candidate_idは書かない/);
  assert.equal(run.state.budget.calls[0].status, 'OK'); assert.equal(run.state.stage_results[0].actual_model, 'claude-sonnet-5'); run.finish('partial', 'MARKET_LOOKUP_PENDING');
});

test('R01の形式不正は結果を公開せず、検証失敗として台帳へ残す', async (t) => {
  const value = input(); const run = session(t, value);
  const result = await evaluateR01(value, { session: run, invokeFn: async (stage, _prompt, options) => { const r = options.budget.reserve(stage); await options.save_budget(options.budget.snapshot()); options.budget.finish(r.id, { status: 'OK' }); await options.save_budget(options.budget.snapshot()); return { status: 'OK', response: '{bad json}' }; } });
  assert.equal(result.status, 'VALIDATION_FAILED'); assert.equal(run.state.stage_results.at(-1).stage, 'R01_validation'); run.finish('blocked', 'R01_VALIDATION_FAILED');
});
