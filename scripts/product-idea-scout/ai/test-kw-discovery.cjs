'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');
const {RunBudget}=require('./budget.cjs');const {chooseBatch,validateBatch,discover,validateDiscovery,sourceEvidence}=require('./kw-discovery.cjs');
const {latestJudgements,learningContext}=require('./kw-learning.cjs');const {keywordId}=require('./kw-core.cjs');
const row=(n,category='園芸')=>({asin:'B'+String(n).padStart(9,'0'),title:'用途'+n+' 土受けシート',categoryPath:category,priceNew:500,packageMm:[150,100,10],packageWeightG:50,monthlySold:100,observedAt:'2026-09-10T00:00:00Z'});
const judgement=(kw,decision,extra={})=>({candidate_id:keywordId(kw),kw,use:kw,decision,reason:'代表の判断理由',reason_codes:[],decided_at:new Date().toISOString(),...extra});
test('3案の上限をなくし、未確認商品を巡回。重複ASINを同じ巡回で再び渡さない',()=>{
 const rows=Array.from({length:20},(_,i)=>row(i,i%2?'園芸':'台所'));const scan={cycle:1,seen_asins:[]};const first=chooseBatch(rows,scan,[],8);scan.seen_asins=first.map(r=>r.asin);const second=chooseBatch(rows,scan,[],8);
 assert.equal(first.length,8);assert.equal(second.length,8);assert.ok(second.every(r=>!scan.seen_asins.includes(r.asin)));assert.equal(new Set(first.map(r=>r.categoryPath)).size,2);
});
test('判断の訂正を優先し、保留を否定せず、旧判断も新しい用途の例として検索できる',()=>{
 const a=judgement('土受け','reject',{event_seq:1}),b=judgement('土受け','adopt',{event_seq:2});const result=latestJudgements([b,a]);assert.equal(result[0].decision,'adopt');
 const profile=learningContext([a,b,judgement('台所 シート','hold')],[row(1)]);assert.equal(profile.counts.adopt,1);assert.equal(profile.counts.reject,0);assert.equal(profile.counts.hold,1);
 assert.equal(profile.examples.length,2);assert.notEqual(profile.version,learningContext([a],[row(1)]).version);
});
test('判断に応じて探索の優先順位が変わり、別用途の探索分も残る',()=>{
 const rows=Array.from({length:20},(_,i)=>({...row(i),title:i<10?'園芸 土受けシート':'台所 水切りマット',categoryPath:i<10?'園芸':'台所'}));const scan={cycle:1,seen_asins:[]};
 const a=chooseBatch(rows,scan,[judgement('土受けシート','adopt')],8),b=chooseBatch(rows,scan,[judgement('水切りマット','adopt')],8);
 assert.ok(a.filter(r=>r.categoryPath==='園芸').length>b.filter(r=>r.categoryPath==='園芸').length);assert.ok(a.some(r=>r.categoryPath==='台所'));assert.ok(b.some(r=>r.categoryPath==='園芸'));
});
test('見た商品の説明がない出力や存在しない判断例を学習根拠にする出力を拒否',()=>{
 const pool=[row(1),row(2)],context=learningContext([],pool);const item={kw:'土受けシート',use:'土を受ける',idea:'シート',reason:'用途が明確',seed_asins:[pool[0].asin],learning_refs:[]};
 assert.throws(()=>validateBatch({items:[item],no_idea:[]},pool,context),/UNACCOUNTED/);
 assert.throws(()=>validateBatch({items:[{...item,learning_refs:['fake']}],no_idea:[{asin:pool[1].asin,reason:'同用途'}]},pool,context),/UNKNOWN_LEARNING/);
 assert.equal(validateBatch({items:[item],no_idea:[{asin:pool[1].asin,reason:'同用途'}]},pool,context).items.length,1);
});
test('大量案ルートは既存の総呼出数を守り、旧モデル比較ルートの上限は変えない',()=>{
 const make=profile=>new RunBudget({run_id:'budget',deadline:new Date(Date.now()+80*60000).toISOString(),profile});const broad=make('kw-discovery-v2');
 for(let i=0;i<7;i++)broad.reserve('R01');assert.throws(()=>broad.reserve('R01'),/STAGE_CALL_LIMIT/);broad.reserve('R01',{retry:true});assert.throws(()=>broad.reserve('R01',{retry:true}),/CALL_LIMIT/);
 const old=make('default');for(let i=0;i<3;i++)old.reserve('R01');assert.throws(()=>old.reserve('R01'),/STAGE_CALL_LIMIT/);
});
test('保存日時を最新需要日とすり替えず、保存時の参考値は別に保持する',()=>{
 const e=sourceEvidence(row(1),Date.now());assert.equal(e.monthly_units,null);assert.equal(e.price,null);assert.equal(e.recorded_monthly_units,100);assert.equal(e.recorded_price,500);
});
test('8案すべてを人の判定用に残し、判断と巡回状況を次の実行へ渡す',async()=>{
 const rows=Array.from({length:8},(_,i)=>row(i));const state={scan:{cycle:1,seen_asins:[]},history:[]};let saved;const prompts=[];
 const session={state:{deadline:new Date(Date.now()+80*60000).toISOString()},budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}};
 const feedback=[judgement('土受けシート','adopt',{reason_codes:['use_clear']})];
 const result=await discover({run_id:'broad',day:'2026-09-10',rows,judgements:feedback,state,session,execution:{attestations:{}},saveState:async s=>saved=structuredClone(s),saveStage:async()=>{},invokeFn:async(_stage,prompt)=>{
   prompts.push(prompt);const input=JSON.parse(prompt.split('<untrusted_data>\n')[1].split('\n</untrusted_data>')[0]);if(_stage==='R03')return {status:'OK',actual_model:'test',response:JSON.stringify({items:input.candidates.map(i=>({candidate_id:i.candidate_id,decision:'propose',codes:[],reason:'鉢の土を床にこぼさない用途がある',matched_asins:i.seed_asins,buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'集合住宅の室内で植え替える人が床に土をこぼさずに片付けるためのシート'}))})};return {status:'OK',actual_model:'test',response:JSON.stringify({items:input.products.map((r,i)=>({kw:'用途'+i+' 土受けシート',use:'用途'+i,idea:'シート案',reason:'用途が明確',seed_asins:[r.asin],learning_refs:[feedback[0].candidate_id]})),no_idea:[]})};
 }});
 assert.equal(result.items.length,8);assert.equal(result.new_count,8);assert.equal(result.target_count,null);assert.equal(result.status,'completed');assert.equal(result.coverage.remaining_in_cycle,0);assert.equal(prompts.length,2);
 const sent=JSON.parse(prompts[0].split('<untrusted_data>\n')[1].split('\n</untrusted_data>')[0]);assert.equal(sent.products[0].saved_keepa.price,500);assert.equal(sent.products[0].saved_keepa.purchased_lower_bound,100);assert.match(sent.products[0].saved_keepa.note,/保存時/);
 assert.ok(result.items.every(i=>i.state==='draft'&&i.search_verified===false));assert.equal(saved.scan.seen_asins.length,8);assert.equal(saved.checkpoint.recoverable_edition.items.length,8);assert.equal(result.learning_audit.judgement_count,1);validateDiscovery(result);
});
test('枠切れや壊れた出力で、未確認の商品を確認済みにしない',async()=>{
 const state={scan:{cycle:1,seen_asins:[]},history:[]};const r=await discover({run_id:'failed',day:'2026-09-10',rows:[row(1)],state,session:{state:{deadline:new Date(Date.now()+80*60000).toISOString()},budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}},execution:{attestations:{}},saveState:async()=>{},saveStage:async()=>{},invokeFn:async()=>({status:'QUOTA_BLOCKED'})});
 assert.equal(r.status,'failed');assert.equal(r.coverage.examined_this_run,0);assert.equal(state.scan.seen_asins.length,0);
});
