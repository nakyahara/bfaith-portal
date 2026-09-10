'use strict';
const { test }=require('node:test'); const assert=require('node:assert/strict');
const { makeSearchPlan, extractAsins, executeSearchPlan }=require('./r02-search.cjs');
const input={schema_version:'w04-r01-2',run_id:'run-r02',stage:'R01',rule_version:'w04-20260909',seeds:[{seed_id:'S1'}],known_ideas:[],excluded_terms:['商標'],quota:{max_candidates:12}};
const output={...input,items:[{kw:'赤飯 蒸し布',use:'包む',target:'蒸し器',form:'布',spec_hypothesis:'2枚',from_seed_id:'S1',why_this_seed:'種',to_verify:['競合'],novelty_check:'未探索'}],unknowns:[],requested_evidence:[]};
test('R01検証済みKWだけを最大12回・各10トークンの検索計画へ変換する',()=>{const p=makeSearchPlan(input,output);assert.equal(p.entries.length,1);assert.equal(p.max_tokens,10);assert.equal(p.entries[0].kw,'赤飯 蒸し布');});
test('Keepa検索はASINを最大20件に正規化し、詳細取得へ渡せる結果を返す',async()=>{const p=makeSearchPlan(input,output);const r=await executeSearchPlan(p,{keepaCall:async(_e,q)=>{assert.equal(q.type,'product');assert.equal(q['asins-only'],1);return {tokensLeft:90,asinList:['B000000001','B000000001','bad']};},now:()=> '2026-09-09T00:00:00Z'});assert.deepEqual(r.asins,['B000000001']);assert.equal(r.results[0].tokens_left,90);});
test('不正なR01出力と不正ASINを検索へ渡さない',()=>{assert.throws(()=>makeSearchPlan(input,{...output,items:[{...output.items[0],kw:'商標 布'}]}),/R01_OUTPUT_INVALID/);assert.deepEqual(extractAsins({asinList:['X','B000000001']}),['B000000001']);});
