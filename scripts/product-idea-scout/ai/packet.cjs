'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { hash, requireValue: check, dateMs } = require('./common.cjs');
const pick = (x, keys) => Object.fromEntries(keys.filter(k => x[k] !== undefined).map(k => [k, structuredClone(x[k])]));
const text = x => typeof x === 'string' && x.trim().length > 0;
const unique = (rows, key) => {
  check(rows.every(x => text(x[key])), 'INVALID_ID', key);
  check(new Set(rows.map(x => x[key])).size === rows.length, 'DUPLICATE_ID', key);
};
const schemas = {
  observation: ['observation_id','asin','parent_asin','source','observed_at','price','price_kind','retrieved_at','monthly_units','monthly_units_kind','demand_observed_at','brand','origin_country','title_excerpt','url'],
  capability: ['evidence_id','process','confidence','maker','scope','confirmed_on','expires_at'],
  own: ['product_id','relation','note'],
  decision: ['idea_id','candidate_id','revision_id','decision','reason','decided_at','evidence_ids'],
};
function cleanRows(rows, schema) { return (rows || []).map(row => pick(row, schemas[schema])); }
function buildPacket(input, selectedIds, now = new Date().toISOString()) {
  check(input.stage === 'R03', 'INVALID_STAGE');
  check(input.schema_version === 'w04-r03-2' && input.rule_version === 'w04-20260909' && text(input.run_id), 'INVALID_SCHEMA');
  check(Array.isArray(selectedIds) && selectedIds.length > 0 && selectedIds.length <= 5, 'CANDIDATE_LIMIT');
  check(new Set(selectedIds).size === selectedIds.length, 'DUPLICATE_ID');
  unique(input.candidates, 'candidate_id');
  const candidates = selectedIds.map(id => {
    const row = input.candidates.find(c => c.candidate_id === id);
    check(row, 'UNKNOWN_CANDIDATE', id);
    const c = pick(row, ['candidate_id','kw','use','target','form','spec_hypothesis','pack_qty']);
    c.economics = pick(row.economics || {}, ['target_price','selling_fee','shipping_total','other_cost','target_profit_rate','target_profit','allowable_landed_cost','unit_allowable_cost','tax_basis','basis','computed_by','evidence_id']);
    check(c.economics.computed_by === 'program', 'ECONOMICS_NOT_COMPUTED');
    check(Number.isInteger(c.pack_qty) && c.pack_qty > 0, 'INVALID_PACK_QTY');
    for (const key of ['target_price','allowable_landed_cost','unit_allowable_cost']) check(Number.isFinite(c.economics[key]) && c.economics[key] >= 0, 'INVALID_ECONOMICS', key);
    check(Math.abs(c.economics.allowable_landed_cost / c.pack_qty - c.economics.unit_allowable_cost) < 0.000001, 'ECONOMICS_UNIT_MISMATCH');
    c.observations = cleanRows(row.observations, 'observation');
    for (let i=0; i<c.observations.length; i++) {
      const basis=row.observations[i].price_basis;
      if (basis) c.observations[i].price_basis=pick(basis,['currency','divisor','evidence_id']);
    }
    c.capability_refs = cleanRows(row.capability_refs, 'capability');
    unique(c.observations, 'observation_id'); unique(c.capability_refs, 'evidence_id');
    check(!c.capability_refs.some(e => c.observations.some(o => o.observation_id === e.evidence_id)), 'DUPLICATE_ID');
    c.own_matches = cleanRows(row.own_matches, 'own');
    c.previous_decisions = cleanRows(row.previous_decisions, 'decision');
    c.brand_signals = pick(row.brand_signals || {}, ['top1_share_of_observed_units','titles_with_brand_name_ratio','kw_is_trademark','coverage_note','evidence_id']);
    c.lookup_evidence = pick(row.lookup_evidence || {}, ['evidence_id','status','note','source_version','scope','checked_at']);
    c.missing_fields = [...new Set(row.missing_fields || [])];
    check(c.missing_fields.every(text), 'INVALID_MISSING_FIELDS');
    if (!c.observations.length) c.missing_fields.push('observations');
    for (const o of c.observations) {
      if (!Number.isFinite(dateMs(o.observed_at)) || dateMs(o.observed_at) > dateMs(now) || dateMs(now) - dateMs(o.observed_at) > 7 * 86400000) c.missing_fields.push(o.observation_id + ':freshness');
      check(/^https:\/\/[^\s]+$/.test(o.url || ''), 'INVALID_SOURCE_URL');
      if (o.asin !== undefined) check(/^[A-Z0-9]{10}$/.test(o.asin), 'INVALID_ASIN');
    }
    for (const e of c.capability_refs) {
      if (!Number.isFinite(dateMs(e.confirmed_on)) || !Number.isFinite(dateMs(e.expires_at)) || dateMs(e.expires_at) <= dateMs(now) || dateMs(e.confirmed_on) > dateMs(now)) c.missing_fields.push(e.evidence_id + ':freshness');
    }
    return c;
  });
  return { run_id: input.run_id, stage: 'R03', schema_version: input.schema_version, rule_version: input.rule_version,
    target_q: [...(input.target_q || [])], candidates };
}
function cacheKey(packet, versions) {
  for (const key of ['rule_version','prompt_version','model_id','effort','source_version','decision_version']) check(text(versions[key]) && versions[key] !== 'unknown', 'CACHE_VERSION_REQUIRED', key);
  const content = structuredClone(packet); delete content.run_id;
  return hash({ content, versions });
}
class PacketCache {
  constructor(directory) { this.directory = directory; }
  get(key, now = Date.now()) {
    check(/^[a-f0-9]{64}$/.test(key), 'INVALID_CACHE_KEY');
    try {
      const row = JSON.parse(fs.readFileSync(path.join(this.directory, key + '.json'), 'utf8'));
      if (row.key !== key || !Number.isFinite(dateMs(row.expires_at)) || dateMs(row.expires_at) <= now || hash(row.result) !== row.result_hash) return null;
      return row.result;
    } catch (e) { if (e.code === 'ENOENT' || e instanceof SyntaxError) return null; throw e; }
  }
  put(key, result, expires_at, now = Date.now()) {
    check(/^[a-f0-9]{64}$/.test(key), 'INVALID_CACHE_KEY');
    check(dateMs(expires_at) > now, 'CACHE_EXPIRED');
    fs.mkdirSync(this.directory, { recursive: true });
    // Partial writes are misses (JSON + checksum). Expired entries can be refreshed.
    try { fs.writeFileSync(path.join(this.directory, key + '.json'), JSON.stringify({ key, expires_at, result, result_hash: hash(result) }), { flag: 'w', mode: 0o600 }); }
    catch (e) { if (e.code !== 'EEXIST') throw e; }
  }
}
function candidateDistribution(rows) {
  unique(rows, 'candidate_id');
  const distribution = { total: rows.length, process: {}, material: {}, price_band: {} };
  const add = (axis, value) => { distribution[axis][value] = (distribution[axis][value] || 0) + 1; };
  for (const r of rows) {
    for (const p of new Set(r.processes?.length ? r.processes : ['unknown'])) add('process', p);
    for (const m of new Set(r.materials?.length ? r.materials : ['unknown'])) add('material', m);
    const p = r.target_price;
    add('price_band', !Number.isFinite(p) || p <= 0 ? 'unknown' : p <= 750 ? '<=750' : p <= 1000 ? '751-1000' : p <= 2000 ? '1001-2000' : '>2000');
  }
  return distribution;
}
function usageRecord(usage, inputText = '', outputText = '') {
  const measured = usage && Number.isInteger(usage.input_tokens) && usage.input_tokens >= 0 && Number.isInteger(usage.output_tokens) && usage.output_tokens >= 0;
  return measured ? { kind: 'measured', input_tokens: usage.input_tokens, output_tokens: usage.output_tokens, cached_input_tokens: usage.cached_input_tokens ?? null }
    : { kind: 'estimated', input_tokens: [...inputText].length, output_tokens: [...outputText].length, estimator: 'unicode-codepoints (planning only, not provider quota)' };
}
module.exports = { buildPacket, cacheKey, PacketCache, candidateDistribution, usageRecord, pick };
