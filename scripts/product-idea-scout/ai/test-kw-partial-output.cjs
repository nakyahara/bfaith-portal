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
test('文章の途中の {"items":[...]} は、途中で切れた返事の救出にも使わない (Codex R2)',()=>{
 const decoy='{"items":['+JSON.stringify(item(1))+'],"no_idea":[]}';
 for(const raw of ['前置き '+decoy+'\n本当の答え {"items":[{"kw":"途中','説明 "items":['+JSON.stringify(item(1))+']'])assert.throws(()=>parseBatchResponse(raw,rows,learningContext([],rows)),/AI_JSON_INVALID/);
 // 先頭のコードブロック内の切れた返事は、これまでどおり完全な項目だけ救う。値の中の "items" という文字は鍵と見なさない
 const lead='```json\n{"note":"items","items":['+JSON.stringify(item(2))+',{"kw":"途中';
 const r=parseBatchResponse(lead,rows,learningContext([],rows));assert.equal(r.partial,true);assert.deepEqual(r.items.map(i=>i.kw),['用途2 シート']);
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
test('説明文つきの返事でも組を打ち切らずに最後まで回し、選別で落ちた案は用途・元商品つきで残す (2026-10-06)',async()=>{
 const state={scan:{cycle:1,seen_asins:[]},history:[]};const stages=[];
 const data=prompt=>JSON.parse(prompt.split('<untrusted_data>\n')[1].split('\n</untrusted_data>')[0]);
 const result=await discover({run_id:'prose',day:'2026-10-06',rows,state,batchSize:1,session:{state:{deadline:new Date(Date.now()+80*60000).toISOString()},budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}},execution:{attestations:{}},saveState:async()=>{},saveStage:async()=>{},invokeFn:async(stage,prompt)=>{stages.push(stage);const input=data(prompt);
  if(stage==='R03')return {status:'OK',response:'```json\n'+JSON.stringify({items:input.candidates.map(i=>i.kw==='用途1 シート'?{candidate_id:i.candidate_id,decision:'propose',codes:[],reason:'室内で土を受ける用途',matched_asins:i.seed_asins,buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'植え替え時に床へ土をこぼさず後片付けを減らすシート'}:{candidate_id:i.candidate_id,decision:'defer',codes:['insufficient_market_evidence'],reason:'根拠の商品が1件だけ',matched_asins:i.seed_asins})})+'\n```\n選別しました。'};
  const n=rows.findIndex(r=>r.asin===input.products[0].asin)+1;return {status:'OK',response:'```json\n'+JSON.stringify({items:[item(n)],no_idea:[]})+'\n```\n\n提供した1案は {用途'+n+'} です。'};}});
 assert.deepEqual(stages,['R01','R03','R01','R03','R01','R03']);assert.equal(result.status,'completed');assert.equal(result.coverage.seen_in_cycle,3);
 assert.deepEqual(result.items.map(i=>i.kw),['用途1 シート']);assert.equal(result.screened_out.length,2);
 for(const r of result.screened_out){const n=r.kw.match(/用途(\d)/)[1];assert.equal(r.use,'用途'+n);assert.equal(r.idea_reason,'用途が明確');assert.deepEqual(r.sources,[{asin:rows[n-1].asin,title:'用途'+n+' シート'}]);assert.equal(r.by,'R03');}
 // 公開の入口 (validateDiscovery) は新しい形を通し、壊れた見送り記録は止める (Codex R2)
 const {validateDiscovery}=require('./kw-discovery.cjs');validateDiscovery(result);
 for(const bad of [{kw:'別のKW'},{decision:'propose'},{sources:[{asin:'bad',title:''}]},{codes:['<script>']},{use:''}])assert.throws(()=>validateDiscovery({...result,screened_out:[{...result.screened_out[0],...bad}]}),/INVALID_SCREENED_OUT/);
});
