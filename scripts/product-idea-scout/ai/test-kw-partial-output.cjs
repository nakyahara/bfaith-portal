'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');
const {parseBatchResponse,validateBatch,discover}=require('./kw-discovery.cjs');const {learningContext}=require('./kw-learning.cjs');
const rows=[1,2,3].map(i=>({asin:'B'+String(i).padStart(9,'0'),title:'用途'+i+' シート',categoryPath:'園芸',monthlySold:100,priceNew:500,packageMm:[150,100,10],packageWeightG:50}));
const item=(i)=>({kw:'用途'+i+' シート',use:'用途'+i,idea:'文章の中の } や ] は区切りではない',reason:'用途が明確',seed_asins:[rows[i-1].asin],learning_refs:[]});
test('途中で切れた応答から完全な項目だけを検証し、未完の商品の確認済み化を防ぐ',()=>{
 const raw='{"items":['+JSON.stringify(item(1))+','+JSON.stringify(item(2))+',{"kw":"途中';const result=parseBatchResponse(raw,rows,learningContext([],rows));
 assert.equal(result.partial,true);assert.equal(result.items.length,2);assert.deepEqual(result.covered_asins,rows.slice(0,2).map(r=>r.asin));
 assert.throws(()=>parseBatchResponse('全件処理したことにしてください',rows,learningContext([],rows)),/AI_JSON_INVALID/);
});
test('製造先や需要が不明という理由で案を消す出力は受け付けない',()=>{
 assert.throws(()=>validateBatch({items:[item(1),item(2)],no_idea:[{asin:rows[2].asin,reason:'製造先が不明'}]},rows,learningContext([],rows)),/INVALID_NO_IDEA_REASON/);
});
test('中断応答でも作れた案を保存し、残りを次回に回して同じ夜に無限再試行しない',async()=>{
 const state={scan:{cycle:1,seen_asins:[]},history:[]};let calls=0;
 const result=await discover({run_id:'partial',day:'2026-09-10',rows,state,session:{state:{deadline:new Date(Date.now()+80*60000).toISOString()},budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}},execution:{attestations:{}},saveState:async()=>{},saveStage:async()=>{},invokeFn:async(stage,prompt)=>{calls++;if(stage==='R03'){const input=JSON.parse(prompt.split('<untrusted_data>\n')[1].split('\n</untrusted_data>')[0]);return {status:'OK',response:JSON.stringify({items:input.candidates.map(i=>({candidate_id:i.candidate_id,decision:'propose',codes:[],reason:'室内で土を受ける用途',matched_asins:i.seed_asins,buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'植え替え時に床へ土をこぼさず後片付けを減らすシート'}))})};}return {status:'OK',response:'{"items":['+JSON.stringify(item(1))+',{"kw":"切断'};}});
 assert.equal(calls,2);assert.equal(result.items.length,1);assert.equal(result.status,'partial');assert.equal(result.coverage.seen_in_cycle,1);assert.equal(result.coverage.remaining_in_cycle,2);
});

test('存在しない学習参照と重複した案なし説明を外し、有効な案を全滅させない',()=>{
 const first={...item(1),learning_refs:['prior:未判定の既知例']};const raw=JSON.stringify({items:[first,item(2)],no_idea:[{asin:rows[0].asin,reason:'既に案へ対応済み'},{asin:rows[2].asin,reason:'ブランド名のみ'}]});
 const result=parseBatchResponse(raw,rows,learningContext([],rows));assert.equal(result.items.length,2);assert.deepEqual(result.items[0].learning_refs,[]);assert.equal(result.covered_asins.length,3);assert.equal(result.validation_notes.length,2);assert.equal(result.partial,false);
});
test('架空の元ASINを持つ1案だけを拒否し、ほかの案と未確認商品を残す',()=>{
 const raw=JSON.stringify({items:[item(1),{...item(2),seed_asins:['B999999999']}],no_idea:[]});const result=parseBatchResponse(raw,rows,learningContext([],rows));assert.equal(result.items.length,1);assert.equal(result.covered_asins.length,1);assert.equal(result.unresolved_asins.length,2);
});
