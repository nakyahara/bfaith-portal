'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { normalizeRepresentativeSeeds, capabilitySeeds, buildR01Input } = require('./seeds.cjs');
const received_at = '2026-09-09T09:00:00+09:00';

test('代表入力は訂正履歴を残し、単語不足は捨てずに要確認として保持する', () => {
  const result = normalizeRepresentativeSeeds([
    { raw_kw: 'グレープ　袋', corrected_kw: 'クレープ 袋', correction_reason: '本人訂正' },
    { raw_kw: '晒し' },
  ], { received_at });
  assert.equal(result.accepted.length, 2);
  assert.deepEqual(result.accepted[0].corrections, [{ from: 'グレープ 袋', to: 'クレープ 袋', reason: '本人訂正' }]);
  assert.ok(result.accepted[1].input_flags.includes('INPUT_NEEDS_DISAMBIGUATION'));
});

test('重複、除外語、性能表現、カテゴリだけの入力を区別して扱う', () => {
  const result = normalizeRepresentativeSeeds([
    { raw_kw: '赤飯 蒸し布' }, { raw_kw: '赤飯 蒸し布' }, { raw_kw: '商標A 布' },
    { raw_kw: '畳 シート 防カビ' }, { raw_kw: 'キッチン用品' },
  ], { received_at, excluded_terms: ['商標A'] });
  assert.equal(result.accepted.length, 2);
  assert.equal(result.duplicates[0].status, 'duplicate');
  assert.equal(result.rejected[0].reject_reason, 'EXCLUDED_TERM');
  assert.ok(result.accepted.find((seed) => seed.normalized_kw === '畳 シート 防カビ').input_flags.includes('PERFORMANCE_OR_REGULATORY_CHECK_REQUIRED'));
  assert.equal(result.held[0].hold_reason, 'GENERIC_INPUT');
});

test('能力根拠はC1/C2だけを製造起点の種にし、C3を能力の証明にしない', () => {
  const seeds = capabilitySeeds([
    { evidence_id: 'CE-1', process: '裁断', confidence: 'C1', materials: ['綿'] },
    { evidence_id: 'CE-2', process: '竹製品加工', confidence: 'C2' },
    { evidence_id: 'CE-3', process: '成形', confidence: 'C3' },
  ], { received_at });
  assert.deepEqual(seeds.map((seed) => seed.capability_refs[0]), ['CE-1', 'CE-2']);
  assert.equal(seeds[0].manufacturing_status, 'C1');
});

test('R01入力は探索種・既存案・見送り履歴を分離し、入力監査を同梱する', () => {
  const out = buildR01Input({
    run_id: 'run-seed-1', received_at, representative_seeds: [{ raw_kw: '赤飯 蒸し布' }],
    capability_evidence: [{ evidence_id: 'CE-1', process: '裁断', confidence: 'C1', materials: ['綿'] }],
    known_ideas: [{ idea_id: 'I-1', kw: ['蒸し布'] }], previous_decisions: [{ idea_id: 'I-2', intent: '見送る' },], quota: { max_candidates: 5 },
  });
  assert.equal(out.stage, 'R01');
  assert.equal(out.seeds.length, 2);
  assert.equal(out.known_ideas[0].idea_id, 'I-1');
  assert.equal(out.input_audit.accepted.length, 1);
  assert.equal(out.quota.max_candidates, 5);
});

test('R01に渡す種が無ければ止まり、過大な候補枠を許さない', () => {
  assert.throws(() => buildR01Input({ run_id: 'none', received_at, capability_evidence: [] }), /INVALID_R01_SEED_COUNT/);
  assert.throws(() => buildR01Input({ run_id: 'quota', received_at, representative_seeds: [{ raw_kw: '赤飯 蒸し布' }], quota: { max_candidates: 13 } }), /INVALID_R01_QUOTA/);
});

test('R01は最大5種へ交互に配分し、残りの種を次回キューとして監査に残す', () => {
  const out = buildR01Input({
    run_id: 'run-queue-1', received_at,
    representative_seeds: [{ raw_kw: '赤飯 蒸し布' }, { raw_kw: '封緘 シール' }, { raw_kw: '無地 手拭い' }, { raw_kw: 'クレープ 袋' }],
    capability_evidence: [
      { evidence_id: 'CE-1', process: '裁断', confidence: 'C1' }, { evidence_id: 'CE-2', process: '縫製', confidence: 'C1' }, { evidence_id: 'CE-3', process: '印刷', confidence: 'C1' },
    ],
  });
  assert.equal(out.seeds.length, 5);
  assert.deepEqual(out.seeds.map((seed) => seed.origin), ['representative_example', 'manufacturing', 'representative_example', 'manufacturing', 'representative_example']);
  assert.equal(out.input_audit.deferred_seed_ids.length, 2);
});
