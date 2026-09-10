'use strict';
const { canonical, hash, dateMs } = require('./common.cjs');
const isText = v => typeof v === 'string' && v.trim().length > 0;
const texts = value => typeof value === 'string' ? [value] : Array.isArray(value) ? value.flatMap(texts) : value && typeof value === 'object' ? Object.values(value).flatMap(texts) : typeof value === 'number' ? [String(value)] : [];
const numbers = value => [...texts(value).join(' ').replace(/(?<=\d),(?=\d{3})/g,'').matchAll(/\d+(?:\.\d+)?/g)].map(m => m[0]);
function validateOutput(input, output, policy = {}) {
  const errors = [];
  const fail = (rule, code, candidate_id = null) => errors.push({rule,code,candidate_id});
  if (typeof output === 'string') { try { output = JSON.parse(output); } catch { return {valid:false,errors:[{rule:'共通1',code:'INVALID_JSON'}]}; } }
  if (!output || output.schema_version !== input.schema_version || output.stage !== input.stage || output.run_id !== input.run_id || output.rule_version !== input.rule_version || input.rule_version !== 'w04-20260909' || input.schema_version !== 'w04-' + String(input.stage).toLowerCase() + '-2' || !Array.isArray(output.items)) {
    return {valid:false,errors:[{rule:'共通1',code:'INVALID_ENVELOPE'}]};
  }
  if (!['R01','R03','R06'].includes(input.stage)) fail('共通1','INVALID_STAGE');
  const requests = (rows, id, allowed = policy) => {
    if (!Array.isArray(rows)) { fail('共通5','INVALID_REQUEST_LIST',id); return; }
    for (const r of rows) {
      if (!r || !['search_term','asin','doc_id'].includes(r.kind) || !isText(r.value) || !isText(r.why)) { fail('共通5','INVALID_REQUEST',id); continue; }
      // Values are data only. Exact allowlists are supplied by the retrieval planner, not by AI output.
      const list = ({search_term:allowed.search_terms,asin:allowed.asins,doc_id:allowed.doc_ids})[r.kind] || [];
      if (!list.includes(r.value)) fail('共通5','REQUEST_NOT_ALLOWED',id);
      if (r.kind === 'asin' && !/^[A-Z0-9]{10}$/.test(r.value)) fail('共通5','INVALID_ASIN',id);
      if (/https?:|[\r\n;`]|\$\(|\b(?:SELECT|INSERT|DELETE|UPDATE|curl|powershell|cmd\.exe)\b/i.test(r.value)) fail('共通5','EXECUTABLE_REQUEST',id);
      if (Object.keys(r).some(k => !['kind','value','why'].includes(k))) fail('共通5','EXTRA_REQUEST_FIELD',id);
    }
  };
  if (output.requested_evidence !== undefined) requests(output.requested_evidence, null);
  const cap = input.stage === 'R01' ? Math.min(12,input.quota?.max_candidates ?? 12) : input.stage === 'R03' ? 5 : Math.min(6,input.quota?.max_cards ?? 6);
  if (output.items.length > cap) fail(input.stage + ' 上限','ITEM_LIMIT');
  const seen = new Set();
  for (const item of output.items) {
    if (!item || typeof item !== 'object') { fail('共通1','INVALID_ITEM'); continue; }
    const id = item.candidate_id;
    const arrays = input.stage==='R01' ? ['to_verify'] : input.stage==='R03' ? ['claims','counter_evidence','unknowns','requested_evidence'] : ['open_items','evidence_ids','decision_options'];
    if(arrays.some(k=>!Array.isArray(item[k]))) {fail('共通1','INVALID_ITEM_SHAPE',id);continue;}
    if(input.stage==='R03' && (item.claims.some(c=>!c || typeof c!=='object' || !Array.isArray(c.evidence_ids)) || item.counter_evidence.some(c=>!c || typeof c!=='object') || !item.q_findings || typeof item.q_findings!=='object')) {fail('共通1','INVALID_ITEM_SHAPE',id);continue;}
    if(input.stage==='R06' && item.open_items.some(v=>typeof v!=='string')) {fail('共通1','INVALID_ITEM_SHAPE',id);continue;}
    if (input.stage === 'R01') {
      // R01 has no candidate_id in W04. The program assigns IDs after validation.
      if (!(input.seeds || []).some(s => s.seed_id === item.from_seed_id)) fail('共通8','UNKNOWN_SEED');
      for (const k of ['kw','use','target','form','spec_hypothesis','why_this_seed','novelty_check']) if (!isText(item[k])) fail('R01 A','MISSING_' + k);
      if ((item.kw || '').trim().split(/[\s　]+/u).length < 2) fail('R01 A','KW_TOO_BROAD');
      if ((input.excluded_terms || []).some(t => (item.kw || '').includes(t))) fail('R01 B','EXCLUDED_TERM');
      if (!Array.isArray(item.to_verify) || !item.to_verify.length) fail('R01 F','MISSING_TO_VERIFY');
      const ideaKey = canonical([item.use,item.target,item.form]);
      if (seen.has(ideaKey) || (input.known_ideas || []).some(k => canonical([k.use,k.target,k.form]) === ideaKey)) fail('R01 C','DUPLICATE_IDEA');
      seen.add(ideaKey);
      if (id !== undefined) fail('共通8','R01_ID_MUST_BE_ASSIGNED_BY_PROGRAM');
      continue;
    }
    const source = (input.candidates || input.approved || []).find(c => c.candidate_id === id);
    if (!source) { fail('共通8','UNKNOWN_CANDIDATE',id); continue; }
    if (seen.has(id)) fail('共通8','DUPLICATE_CANDIDATE',id);
    seen.add(id);
    const evidence = new Set([...(source.observations || []).map(x=>x.observation_id),...(source.capability_refs || []).map(x=>x.evidence_id),...(source.own_matches || []).map(x=>x.product_id),...[source.economics?.evidence_id,source.brand_signals?.evidence_id,source.lookup_evidence?.evidence_id].filter(Boolean),...Object.keys(source.evidence_registry || {})]);
    const inspectRefs = obj => {
      if (!obj || typeof obj !== 'object') return;
      if ('evidence_ids' in obj) {
        if (!Array.isArray(obj.evidence_ids) || obj.evidence_ids.some(e => !evidence.has(e))) fail('共通8','UNKNOWN_EVIDENCE',id);
      }
      for (const v of Object.values(obj)) inspectRefs(v);
    };
    if (input.stage === 'R03') {
      inspectRefs(item);
      if (!['continue','hold','reject'].includes(item.decision)) fail('R03 A','INVALID_DECISION',id);
      if (!Array.isArray(item.claims)) fail('共通3','MISSING_CLAIMS',id);
      for (const claim of item.claims || []) {
        if (!isText(claim.text) || !['事実','推論','未確認'].includes(claim.type)) fail('共通3','INVALID_CLAIM',id);
        if (claim.type === '事実' && !claim.evidence_ids?.length) fail('共通3','FACT_WITHOUT_EVIDENCE',id);
      }
      for (const q of input.target_q || []) {
        const finding = item.q_findings?.[q];
        if (!finding || !['pass','short','violation'].includes(finding.result) || !isText(finding.reason) || !Array.isArray(finding.evidence_ids)) fail('共通1','MISSING_Q_FINDING',id);
      }
      const brand = item.first_recall_brand;
      if (!brand || !['observed_signal','no_signal'].includes(brand.basis) || !['あり','なし','未確認'].includes(brand.verdict) || !Array.isArray(brand.signal_used)) fail('R03 C','INVALID_BRAND_SIGNAL',id);
      const signals = Object.entries(source.brand_signals || {}).filter(([k,v]) => !['coverage_note','evidence_id'].includes(k) && v !== null && v !== undefined).map(([k,v])=>k+'='+v);
      if (source.brand_signals?.evidence_id && signals.length) signals.push(source.brand_signals.evidence_id);
      if (brand?.basis === 'observed_signal' && (!Array.isArray(brand.signal_used) || !brand.signal_used.length || brand.signal_used.some(s=>!signals.includes(s)))) fail('R03 C','FABRICATED_SIGNAL',id);
      if (brand?.basis === 'no_signal' && brand.verdict === 'あり' && item.decision === 'reject') fail('R03 C','REJECT_WITHOUT_SIGNAL',id);
      if (item.decision === 'reject' && !(item.claims || []).some(c=>c.type==='事実' && c.evidence_ids?.some(e=>(source.observations||[]).some(o=>o.observation_id===e))) && !(Array.isArray(brand?.signal_used) && brand.signal_used.some(s=>signals.includes(s)))) fail('R03 B','REJECT_WITHOUT_OBSERVATION',id);
      if (item.economics !== undefined && canonical(item.economics) !== canonical(source.economics)) fail('R03 D','ECONOMICS_CHANGED',id);
      if (!Array.isArray(item.counter_evidence) || !item.counter_evidence.length) fail('R03 H','MISSING_COUNTER_EVIDENCE',id);
      if (!Array.isArray(item.unknowns)) fail('R03 G','MISSING_UNKNOWNS',id);
      if (!isText(item.short_reason) || !isText(item.spec_hypothesis)) fail('共通1','MISSING_DESCRIPTION',id);
      const question = item.one_question_draft || '';
      for (const v of [source.pack_qty,source.economics?.unit_allowable_cost]) if (!numbers(question).includes(String(v))) fail('R03 I','QUESTION_MISSING_ECONOMICS',id);
      for (const q of ['Q06','Q10']) { if (item.q_findings?.[q]?.result === 'pass' && (source.lookup_evidence?.status !== 'complete' || !source.lookup_evidence.source_version || !source.lookup_evidence.scope || !Number.isFinite(dateMs(source.lookup_evidence.checked_at)))) fail('R03 G','LOOKUP_NOT_COMPLETE',id); }
      const allowed = policy.by_candidate?.[id] || {};
      requests(item.requested_evidence, id, {...allowed, asins:[...new Set([...(allowed.asins||[]),...(source.observations||[]).map(o=>o.asin).filter(Boolean)])], doc_ids:[...new Set([...(allowed.doc_ids||[]),...(source.capability_refs||[]).map(e=>e.evidence_id)])]});
    } else if (input.stage === 'R06') {
      inspectRefs(item);
      const pv = source.program_validation;
      if (!pv || pv.status !== 'passed' || !isText(pv.revision) || !Number.isFinite(dateMs(pv.validated_at)) || !Array.isArray(pv.unresolved_blockers) || pv.unresolved_blockers.length || Array.from({length:10},(_,n)=>'Q'+String(n+1).padStart(2,'0')).some(q=>pv.q_results?.[q]!=='pass') || !Array.isArray(pv.evidence_refs) || !pv.evidence_refs.length || pv.evidence_refs.some(e=>!Object.hasOwn(source.evidence_registry||{},e))) fail('R06 A','NOT_VALIDATED_FOR_PUBLICATION',id);
      if (!['new','recheck','carryover_undecided'].includes(source.edition_class) || item.edition_class !== source.edition_class) fail('R06 J','EDITION_CLASS_CHANGED',id);
      if (!Array.isArray(item.evidence_ids) || (pv?.evidence_refs||[]).some(e=>!item.evidence_ids?.includes(e))) fail('共通8','PUBLICATION_EVIDENCE_MISSING',id);
      for (const k of ['headline','why_now','why_makeable','own_relation','economics_line','amc_message','rank_reason']) if (!isText(item[k])) fail('R06 D','MISSING_' + k,id);
      const allowedNumbers = new Set(['1',...numbers(source)]);
      const rendered = [item.headline,item.why_now,item.why_makeable,item.own_relation,item.economics_line,item.amc_message,item.open_items];
      if (numbers(rendered).some(n=>!allowedNumbers.has(n))) fail('R06 B','NUMBER_CHANGED_OR_INVENTED',id);
      const allowedDates = new Set(texts(source).join(' ').match(/\d{4}-\d{2}-\d{2}/g) || []);
      if ((texts(rendered).join(' ').match(/\d{4}-\d{2}-\d{2}/g)||[]).some(d=>!allowedDates.has(d))) fail('R06 B','DATE_CHANGED_OR_INVENTED',id);
      const line = item.economics_line || '';
      const esc = s => String(s).replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
      if (!new RegExp('1(?:個|枚|本|袋|組)あたり\\s*'+esc(source.economics?.unit_allowable_cost)+'円').test(line) || !new RegExp('入数\\s*'+esc(source.pack_qty)+'(?:[^0-9]|$)').test(line)) fail('R06 C','MISSING_UNIT_COST_OR_PACK',id);
      if (!Array.isArray(item.open_items) || (source.unknowns||[]).some(u=>!item.open_items.some(t=>t.includes(u)))) fail('R06 E','UNKNOWN_HIDDEN',id);
      if (source.condition_hint && !texts([item.open_items,item.headline]).join(' ').includes(source.condition_hint)) fail('R06 G','CONDITION_HIDDEN',id);
      const message = item.amc_message || '';
      for (const v of [source.economics?.target_price,source.economics?.unit_allowable_cost,source.demand?.monthly_units_max,source.demand?.matched_competitors]) if (v !== undefined && !numbers(message).includes(String(v))) fail('R06 D','MESSAGE_MISSING_NUMBER',id);
      for (const v of [source.demand?.observed_at,source.demand?.source]) if (!v || !message.includes(v)) fail('R06 D','MESSAGE_MISSING_SOURCE',id);
      if (!/御社の利益/.test(message) || !/(でしょうか|ますか|[？?])/.test(message)) fail('R06 D','MESSAGE_NOT_A_QUESTION',id);
      if (canonical(item.decision_options) !== canonical(['相談したい','保留','見送る'])) fail('R06 D','INVALID_DECISION_OPTIONS',id);
      if (!Number.isInteger(item.rank) || item.rank < 1 || item.rank > output.items.length) fail('R06 F','INVALID_RANK',id);
    }
  }
  if (input.stage === 'R03' && (input.candidates || []).some(c=>!seen.has(c.candidate_id))) fail('R03 J','CANDIDATE_OMITTED');
  if (input.stage === 'R06') {
    if (!Array.isArray(output.omitted)) fail('R06 J','OMITTED_LIST_REQUIRED');
    const omitted = new Set();
    for (const row of output.omitted || []) {
      if (!row || !isText(row.candidate_id) || !isText(row.reason) || seen.has(row.candidate_id) || omitted.has(row.candidate_id) || !(input.approved||[]).some(c=>c.candidate_id===row.candidate_id)) fail('R06 J','INVALID_OMISSION');
      else omitted.add(row.candidate_id);
    }
    if ((input.approved||[]).some(c=>!seen.has(c.candidate_id) && !omitted.has(c.candidate_id))) fail('R06 J','SILENT_OMISSION');
  }
  if (input.stage === 'R06' && new Set(output.items.map(i=>i.rank)).size !== output.items.length) fail('R06 F','DUPLICATE_RANK');
  return { valid: errors.length === 0, errors, output_hash: hash(output), semantic_review_required: true };
}
module.exports = { validateOutput };
