'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');
const {createMarketIndex,findMarketEvidence}=require('./market-input.cjs');
const now=Date.parse('2026-09-10T00:00:00Z');
const row=(asin='B000000001',extra={})=>({asin,title:'試験 布 綿',observedAt:'2026-09-09T00:00:00Z',priceBuyBox:980,monthlySold:90,...extra});
const index=rows=>createMarketIndex(rows.map(r=>JSON.stringify(r)).join('\n'),{now});
const request={candidate_id:'C1',search_terms:['試験 布']};
test('audit separates missing, future, stale and fresh; latest dated duplicate wins',()=>{
 const i=index([row(),row('B000000001',{observedAt:null}),row('B000000002',{observedAt:null}),row('B000000003',{observedAt:'2026-08-01'}),row('B000000004',{observedAt:'2027-01-01'})]);
 assert.equal(i.summary.unique_asins,4);assert.equal(i.summary.fresh,1);assert.equal(i.summary.missing,1);assert.equal(i.summary.stale,1);assert.equal(i.summary.future,1);
});
test('no dates invented from file creation or extraction time',()=>{
 const r=findMarketEvidence(index([row('B000000001',{observedAt:null})]),request);assert.equal(r.observations.length,0);assert.equal(r.coverage.excluded_for_freshness,1);assert.equal(r.search_market_verified,false);
});
test('title match never implies search rank or semantic fit; price needs provenance',()=>{
 const r=findMarketEvidence(index([row()]),request);assert.equal(r.semantic_match,'unverified');assert.equal(r.retrieval_kind,'local_title_match');assert.equal(r.observations[0].price,null);assert.ok(r.missing_fields.includes('価格単位未確認'));
 const p=findMarketEvidence(index([row()]),{...request,price_policy:{currency:'JPY',divisor:1,evidence_id:'UNIT-FIXTURE'}});assert.equal(p.observations[0].price,980);
});
test('literal keywords and NFKC matching do not execute regular expressions',()=>{
 assert.equal(findMarketEvidence(index([row()]),{...request,search_terms:['.*']}).observations.length,0);
 assert.equal(findMarketEvidence(index([row('B000000001',{title:'試験　布'})]),request).observations.length,1);
});
test('truncation and parent ambiguity stay explicit; missing demand never becomes zero',()=>{
 const r=findMarketEvidence(index([row(),row('B000000002',{monthlySold:-1})]),{...request,limit:1});assert.equal(r.coverage.truncated,true);assert.equal(r.observations[0].parent_asin,null);
 const all=findMarketEvidence(index([row('B000000002',{monthlySold:null})]),request);assert.equal(all.observations[0].monthly_units,null);
});
test('source changes invalidate observation IDs, while candidate-local relevance stays separate',()=>{
 const a=findMarketEvidence(index([row()]),request),b=findMarketEvidence(index([row('B000000001',{priceBuyBox:1000})]),request);assert.notEqual(a.observations[0].observation_id,b.observations[0].observation_id);
 const c=findMarketEvidence(index([row()]),{...request,candidate_id:'C2'});assert.equal(c.observations[0].observation_id,a.observations[0].observation_id);assert.notEqual(c.candidate_id,a.candidate_id);
});
test('malformed JSONL and invalid ASIN fail rather than silently lose rows',()=>{
 assert.throws(()=>createMarketIndex('{bad',{now}));assert.throws(()=>index([row('bad')]),/INVALID_ASIN/);
});
