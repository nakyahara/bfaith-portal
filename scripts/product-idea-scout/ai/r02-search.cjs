'use strict';
const { requireValue: check, hash } = require('./common.cjs');
const { validateOutput } = require('./validate.cjs');
const UNSAFE_TERM = /https?:|[\r\n;`]|\$\(|\b(?:SELECT|INSERT|DELETE|UPDATE|curl|powershell|cmd\.exe)\b/i;
function makeSearchPlan(input, output, { max_searches = 12 } = {}) {
  check(Number.isInteger(max_searches) && max_searches >= 1 && max_searches <= 12, 'INVALID_SEARCH_LIMIT');
  const validation = validateOutput(input, output);
  check(validation.valid, 'R01_OUTPUT_INVALID');
  const entries = output.items.slice(0, max_searches).map((item, index) => {
    const term = item.kw.normalize('NFKC').replace(/\s+/g, ' ').trim();
    check(term.length >= 3 && term.length <= 120 && !UNSAFE_TERM.test(term), 'INVALID_SEARCH_TERM');
    return { search_id: `KS-${String(index + 1).padStart(2, '0')}-${hash([input.run_id, term]).slice(0, 8)}`, kw: term, from_seed_id: item.from_seed_id };
  });
  return { schema_version: 'r02-keyword-search-plan-v1', run_id: input.run_id, source_stage: 'R01', max_tokens: entries.length * 10, entries };
}
function extractAsins(json) { return [...new Set((json?.asinList || json?.products || []).map(x => typeof x === 'string' ? x : x?.asin).filter(x => /^[A-Z0-9]{10}$/.test(x)))].slice(0, 20); }
async function executeSearchPlan(plan, { keepaCall, deadlineMs, now = () => new Date().toISOString() } = {}) {
  check(plan?.schema_version === 'r02-keyword-search-plan-v1' && Array.isArray(plan.entries) && plan.entries.length <= 12, 'INVALID_SEARCH_PLAN');
  check(typeof keepaCall === 'function', 'KEEPA_CALL_REQUIRED');
  const results = [];
  for (const entry of plan.entries) {
    const json = await keepaCall('/search', { type: 'product', term: entry.kw, 'asins-only': 1 }, null, { deadlineMs });
    results.push({ ...entry, source: 'Keepa Product Search', observed_at: now(), asins: extractAsins(json), tokens_left: Number.isFinite(json?.tokensLeft) ? json.tokensLeft : null });
  }
  return { schema_version: 'r02-keyword-search-result-v1', run_id: plan.run_id, source_plan_hash: hash(plan), results, asins: [...new Set(results.flatMap(r => r.asins))] };
}
module.exports = { makeSearchPlan, extractAsins, executeSearchPlan };
