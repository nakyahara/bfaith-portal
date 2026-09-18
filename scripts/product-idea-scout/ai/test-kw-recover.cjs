'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {recover,appendHistory}=require('./kw-recover.cjs');const {keywordId}=require('./kw-core.cjs');
const idle=async()=>{};
const row=n=>({asin:'B'+String(n).padStart(9,'0'),title:'用途'+n+' 土受けシート',categoryPath:'園芸',priceNew:500,packageMm:[150,100,10],packageWeightG:50,monthlySold:100,observedAt:new Date().toISOString()});
function fixture(){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'kw-recover-')),state=path.join(dir,'state');
 fs.mkdirSync(path.join(state,'editions'),{recursive:true});fs.mkdirSync(path.join(state,'runs'),{recursive:true});
 const rows=[row(1),row(2)];
 fs.writeFileSync(path.join(dir,'products.jsonl'),rows.map(r=>JSON.stringify(r)).join('\n')+'\n');
 fs.writeFileSync(path.join(dir,'own.json'),JSON.stringify({families:[{familyKey:'既存の土受けタオル',salesClass:1,products:[{name:'既存の土受けタオル'}]}]}));
 fs.writeFileSync(path.join(state,'editions','kw-src.json'),JSON.stringify({schema_version:'kw-discovery-v2',run_id:'kw-src',day:'2026-09-17',status:'failed',items:[]}));
 const cards=rows.map((r,i)=>({kw:'土受けシート'+i,use:'鉢の土を受ける',idea:'室内の植え替え用シート',reason:'用途が明確で方針に合う',seed_asins:[r.asin],learning_refs:[]}));
 fs.writeFileSync(path.join(state,'runs','kw-src.batch-1-response.json'),JSON.stringify({input_asins:rows.map(r=>r.asin),response:JSON.stringify({items:cards,no_idea:[]}),
  metadata:{batch:1,status:'OK',requested_model:'claude-sonnet-5',actual_model:'claude-sonnet-5',usage:{kind:'measured',input_tokens:2,output_tokens:100}}}));
 const review=(c,over={})=>({candidate_id:keywordId(c.kw),decision:'propose',codes:[],reason:'室内の植え替えで土の片付けを減らす',matched_asins:c.seed_asins,
  buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'ベランダのない住まいで鉢を植え替えるとき、床の土汚れと後片付けを減らす',...over});
 // 2件目は 9/18 に実際に起きた形 (提案なのに own_overlap が unknown)
 fs.writeFileSync(path.join(state,'runs','kw-src.screen-1-response.json'),JSON.stringify({response:JSON.stringify({items:[review(cards[0]),review(cards[1],{own_overlap:'unknown'})]}),
  metadata:{status:'OK',requested_model:'claude-opus-5',actual_model:'claude-opus-5',usage:{kind:'measured',input_tokens:2,output_tokens:80}}}));
 return {dir,state,cards,config:{source_file:path.join(dir,'products.jsonl'),own_file:path.join(dir,'own.json'),state_dir:state,source_run_id:'kw-src'}};
}
test('0件で終わった回から、生き残っていた案だけを作り直す',async()=>{
 const f=fixture();
 try{
  const edition=await recover(f.config,{collectorIdleFn:idle});
  assert.equal(edition.run_id,'kw-src-recovered');assert.equal(edition.day,'2026-09-17');
  assert.deepEqual(edition.items.map(i=>i.kw),[f.cards[0].kw]);
  assert.deepEqual([edition.new_count,edition.submitted_count],[1,1]);
  assert.ok(edition.items.every(i=>i.screening.decision==='propose'&&i.state==='draft'));
  // 保存した応答を再生しただけで、AIは呼んでいない
  assert.ok(edition.model_audit.every(a=>a.usage.kind==='replayed'));
  assert.deepEqual(edition.screened_out.filter(r=>r.codes.includes('invalid_screen_response')).map(r=>r.kw),[f.cards[1].kw]);
  // 元の回の記録は書き換えない
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(f.state,'editions','kw-src.json'),'utf8')).items,[]);
  assert.equal(fs.existsSync(path.join(f.state,'discovery-state.json')),false);
  assert.equal(fs.existsSync(path.join(f.state,'active.lock')),false);
  // 2度目は作り直さない。保存した応答が消えていても、作った版をそのまま返す
  fs.rmSync(path.join(f.state,'runs','kw-src.batch-1-response.json'));
  assert.deepEqual(await recover(f.config,{collectorIdleFn:idle}),edition);
 }finally{fs.rmSync(f.dir,{recursive:true,force:true});}
});
test('作り直した案を次回の重複判定へ引き継ぎ、巡回進捗は変えない',async()=>{
 const f=fixture();
 try{
  fs.writeFileSync(path.join(f.state,'discovery-state.json'),JSON.stringify({history:[],scan:{cycle:3,seen_asins:['B000000001']}}));
  const edition=await recover(f.config,{collectorIdleFn:idle});
  assert.equal(appendHistory(f.config,edition),1);
  const state=JSON.parse(fs.readFileSync(path.join(f.state,'discovery-state.json'),'utf8'));
  assert.deepEqual(state.history.map(i=>i.kw),[f.cards[0].kw]);
  assert.deepEqual(state.scan,{cycle:3,seen_asins:['B000000001']});
  assert.equal(appendHistory(f.config,edition),0);
 }finally{fs.rmSync(f.dir,{recursive:true,force:true});}
});
test('案が載っている回・2組以上のAI呼出がある回・記録のない回は作り直さない',async()=>{
 const f=fixture();
 try{
  fs.writeFileSync(path.join(f.state,'editions','kw-has.json'),JSON.stringify({schema_version:'kw-discovery-v2',run_id:'kw-has',day:'2026-09-16',items:[{kw:'すでに載っている案'}]}));
  await assert.rejects(()=>recover({...f.config,source_run_id:'kw-has'},{collectorIdleFn:idle}),/SOURCE_RUN_HAD_ITEMS/);
  await assert.rejects(()=>recover({...f.config,source_run_id:'kw-missing'},{collectorIdleFn:idle}),/SOURCE_EDITION_REQUIRED/);
  fs.writeFileSync(path.join(f.state,'runs','kw-src.batch-2-response.json'),'{}');
  await assert.rejects(()=>recover(f.config,{collectorIdleFn:idle}),/MULTI_BATCH_NOT_SUPPORTED/);
  assert.equal(fs.existsSync(path.join(f.state,'editions','kw-src-recovered.json')),false);
 }finally{fs.rmSync(f.dir,{recursive:true,force:true});}
});
test('商品データが変わって材料が欠けたら、黙って案を減らさずに止める',async()=>{
 const f=fixture();
 try{
  const rows=fs.readFileSync(path.join(f.dir,'products.jsonl'),'utf8').split('\n').filter(Boolean).map(l=>JSON.parse(l));
  rows[0].monthlySold=0;
  fs.writeFileSync(path.join(f.dir,'products.jsonl'),rows.map(r=>JSON.stringify(r)).join('\n')+'\n');
  await assert.rejects(()=>recover(f.config,{collectorIdleFn:idle}),/SOURCE_ROWS_CHANGED/);
  assert.equal(fs.existsSync(path.join(f.state,'editions','kw-src-recovered.json')),false);
 }finally{fs.rmSync(f.dir,{recursive:true,force:true});}
});
test('日次実行と重なっている間は作り直さない',async()=>{
 const f=fixture();
 try{
  fs.writeFileSync(path.join(f.state,'active.lock'),'{}');
  await assert.rejects(()=>recover(f.config,{collectorIdleFn:idle}),/KW_RUN_LOCKED/);
  fs.rmSync(path.join(f.state,'active.lock'));
  assert.equal((await recover(f.config,{collectorIdleFn:idle})).items.length,1);
 }finally{fs.rmSync(f.dir,{recursive:true,force:true});}
});
test('公開でつまずいても、作り直した同じ案をそのまま送り直せる',async()=>{
 const f=fixture();
 try{
  const {publish}=require('./kw-publish.cjs');const {hash}=require('./common.cjs');
  const posts=[];let stored=null,failOnce=true;
  const fetchFn=async(url,options)=>{
   if(options.method==='POST'){const body=JSON.parse(options.body);posts.push(body);
    if(failOnce){failOnce=false;return {ok:false,status:503,json:async()=>({})};}
    stored=body;return {ok:true,json:async()=>({run_id:body.run_id,body_hash:hash(body)})};}
   if(url.includes('?since='))return {ok:true,json:async()=>({history:[],feedback_cursor:0,feedback_has_more:false})};
   return {ok:true,json:async()=>({run_id:stored.run_id,body_hash:hash(stored)})};};
  const options={env:{MIRROR_SYNC_KEY:'test-only'},fetchFn,runFn:async()=>recover(f.config,{collectorIdleFn:idle,lock:false})};
  await assert.rejects(()=>publish(f.config,options),/PORTAL_HTTP_503/);
  const sent=await publish(f.config,options);
  assert.equal(sent.run_id,'kw-src-recovered');assert.equal(sent.new_count,1);
  assert.deepEqual(posts[0],posts[1]);
  assert.equal(appendHistory(f.config,posts[1]),0);
 fs.writeFileSync(path.join(f.state,'discovery-state.json'),JSON.stringify({history:[],scan:{cycle:1,seen_asins:[]}}));
  assert.equal(appendHistory(f.config,posts[1]),1);
 }finally{fs.rmSync(f.dir,{recursive:true,force:true});}
});
