'use strict';

// R00: 人の探索語と能力根拠を、R01へ渡せる型付きの「探索種」に整える。
// ここでは需要・製造可否を判定せず、入力由来と未確認事項を失わない。
const fs = require('node:fs');
const { hash, requireValue: check, dateMs } = require('./common.cjs');

const ORIGINS = new Set(['representative_example', 'manufacturing', 'market', 'manual']);
const PERFORMANCE_TERMS = /防カビ|抗菌|防臭|除菌|防虫|撥水|耐熱|医療|治療|予防/;
const GENERIC_ONLY = /^(?:用品|商品|グッズ|雑貨|キッチン用品|生活用品|収納用品)$/;

function clean(value) {
  return String(value || '').normalize('NFKC').replace(/[\u3000\t\r\n]+/g, ' ').replace(/\s+/g, ' ').trim();
}
function stableSeedId(origin, kw) {
  return `S-${hash({ origin, kw }).slice(0, 12).toUpperCase()}`;
}
function hasExcludedTerm(kw, terms) {
  return terms.some((term) => term && kw.toLocaleLowerCase('ja').includes(term.toLocaleLowerCase('ja')));
}
function seedFlags(kw) {
  const tokens = kw.split(' ').filter(Boolean);
  const flags = [];
  if (tokens.length < 2) flags.push('INPUT_NEEDS_DISAMBIGUATION');
  if (GENERIC_ONLY.test(kw)) flags.push('GENERIC_INPUT');
  if (PERFORMANCE_TERMS.test(kw)) flags.push('PERFORMANCE_OR_REGULATORY_CHECK_REQUIRED');
  return flags;
}

function normalizeRepresentativeSeeds(rows, { excluded_terms = [], received_at } = {}) {
  check(Array.isArray(rows) && rows.length <= 50, 'INVALID_SEED_COUNT');
  check(Number.isFinite(dateMs(received_at)), 'INVALID_RECEIVED_AT');
  check(Array.isArray(excluded_terms) && excluded_terms.every((term) => typeof term === 'string'), 'INVALID_EXCLUDED_TERMS');
  const excluded = excluded_terms.map(clean).filter(Boolean);
  const seen = new Map();
  const accepted = [], held = [], rejected = [], duplicates = [];
  for (const row of rows) {
    check(row && typeof row === 'object', 'INVALID_SEED');
    const origin = row.origin || 'representative_example';
    check(ORIGINS.has(origin), 'INVALID_SEED_ORIGIN');
    const original_kw = clean(row.raw_kw);
    const corrected_kw = clean(row.corrected_kw || original_kw);
    check(original_kw.length >= 1 && original_kw.length <= 120 && corrected_kw.length >= 1 && corrected_kw.length <= 120, 'INVALID_SEED_KEYWORD');
    const normalized_kw = corrected_kw;
    const seed_id = stableSeedId(origin, normalized_kw);
    const common = {
      seed_id, origin, original_kw, normalized_kw,
      received_at, source_note: clean(row.source_note) || null,
      corrections: corrected_kw === original_kw ? [] : [{ from: original_kw, to: corrected_kw, reason: clean(row.correction_reason) || '入力訂正' }],
      input_flags: seedFlags(normalized_kw),
    };
    if (hasExcludedTerm(normalized_kw, excluded)) {
      rejected.push({ ...common, status: 'rejected', reject_reason: 'EXCLUDED_TERM' });
      continue;
    }
    if (seen.has(seed_id)) {
      duplicates.push({ ...common, status: 'duplicate', duplicate_of: seen.get(seed_id) });
      continue;
    }
    seen.set(seed_id, seed_id);
    if (common.input_flags.includes('GENERIC_INPUT')) held.push({ ...common, status: 'hold', hold_reason: 'GENERIC_INPUT' });
    else accepted.push({ ...common, status: common.input_flags.length ? 'needs_disambiguation' : 'ready' });
  }
  return { accepted, held, rejected, duplicates };
}

function capabilitySeeds(evidence, { received_at } = {}) {
  check(Array.isArray(evidence), 'INVALID_CAPABILITY_EVIDENCE');
  check(Number.isFinite(dateMs(received_at)), 'INVALID_RECEIVED_AT');
  const seen = new Set();
  return evidence.filter((item) => item && ['C1', 'C2'].includes(item.confidence)).map((item) => {
    check(typeof item.evidence_id === 'string' && item.evidence_id, 'INVALID_CAPABILITY_ID');
    check(typeof item.process === 'string' && item.process, 'INVALID_CAPABILITY_PROCESS');
    const seed_id = stableSeedId('manufacturing', item.evidence_id);
    if (seen.has(seed_id)) return null;
    seen.add(seed_id);
    return {
      seed_id, origin: 'manufacturing', original_kw: item.process, normalized_kw: item.process,
      received_at, source_note: '能力根拠から生成。用途・需要は未確認。', corrections: [], input_flags: [], status: 'ready',
      process: item.process, materials: Array.isArray(item.materials) ? item.materials.map(clean).filter(Boolean) : [],
      capability_refs: [item.evidence_id], manufacturing_status: item.confidence,
    };
  }).filter(Boolean);
}

function asR01Seed(seed) {
  return {
    seed_id: seed.seed_id,
    origin: seed.origin,
    input_kw: seed.normalized_kw,
    process: seed.process || null,
    materials: seed.materials || [],
    capability_refs: seed.capability_refs || [],
    note: [seed.source_note, ...seed.input_flags].filter(Boolean).join(' / ') || null,
    manufacturing_status: seed.manufacturing_status || 'C3',
  };
}

function selectR01Seeds(representative, manufacturing, maxSeeds) {
  check(Number.isInteger(maxSeeds) && maxSeeds >= 3 && maxSeeds <= 5, 'INVALID_R01_SEED_QUOTA');
  // 代表の観察と製造根拠を交互に選び、得意工程だけへの収束を入力段階で防ぐ。
  const human = [...representative].sort((a, b) => (a.status === 'ready' ? 0 : 1) - (b.status === 'ready' ? 0 : 1));
  const factory = [...manufacturing];
  const selected = [];
  while (selected.length < maxSeeds && (human.length || factory.length)) {
    if (human.length && selected.length < maxSeeds) selected.push(human.shift());
    if (factory.length && selected.length < maxSeeds) selected.push(factory.shift());
  }
  return {
    selected,
    deferred_seed_ids: [...human, ...factory].map((seed) => seed.seed_id),
  };
}

function buildR01Input(input) {
  check(input && typeof input === 'object', 'INVALID_R01_INPUT');
  check(typeof input.run_id === 'string' && input.run_id, 'INVALID_RUN_ID');
  const received_at = input.received_at;
  const representative = normalizeRepresentativeSeeds(input.representative_seeds || [], { excluded_terms: input.excluded_terms || [], received_at });
  const manufacturing = capabilitySeeds(input.capability_evidence || [], { received_at });
  const seedMax = input.seed_quota?.max_seeds ?? 5;
  const selection = selectR01Seeds(representative.accepted, manufacturing, seedMax);
  const seeds = selection.selected.map(asR01Seed);
  check(seeds.length > 0, 'INVALID_R01_SEED_COUNT');
  const max = input.quota?.max_candidates ?? 12;
  check(Number.isInteger(max) && max >= 1 && max <= 12, 'INVALID_R01_QUOTA');
  return {
    schema_version: 'w04-r01-2', run_id: input.run_id, stage: 'R01',
    seeds, known_ideas: Array.isArray(input.known_ideas) ? input.known_ideas : [],
    previous_decisions: Array.isArray(input.previous_decisions) ? input.previous_decisions : [],
    excluded_terms: (input.excluded_terms || []).map(clean).filter(Boolean), quota: { max_candidates: max },
    rule_version: 'w04-20260909', input_audit: { ...representative, deferred_seed_ids: selection.deferred_seed_ids },
  };
}

if (require.main === module) {
  const raw = fs.readFileSync(0, 'utf8');
  process.stdout.write(`${JSON.stringify(buildR01Input(JSON.parse(raw)))}\n`);
}
module.exports = { clean, stableSeedId, seedFlags, normalizeRepresentativeSeeds, capabilitySeeds, selectR01Seeds, buildR01Input };
