'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {scopeGate,sourceGate,candidateGate,ownMatches,filterSources}=require('./kw-filters.cjs');
const {validateScreen,partitionScreen,screenCandidates}=require('./kw-screen.cjs');const {RunBudget}=require('./budget.cjs');
const base={asin:'B000000001',title:'園芸 植え替えシート',categoryPath:'園芸',monthlySold:100,priceNew:200,packageMm:[150,100,10],packageWeightG:80,brand:'小さなメーカー'};
const item={candidate_id:'test-id',kw:'植え替えシート',use:'鉢の土を受ける',idea:'室内の植え替え用シート',seed_asins:[base.asin]};
test('電池・電気とアパレルを除外し、非通電の手入れ用品と布素材を残す',()=>{
 for(const s of ['水槽 LED ライト','植物育成ライト','水槽ヒーター','USB エアポンプ','デジタルノギス 電池付き','露出スイッチ','充電ケース','アームカバー UV','作業帽','犬用シューズ'])assert.equal(scopeGate(s).status,'exclude',s);
 for(const s of ['電気ケトル 洗浄剤','エアコン 掃除ブラシ','モバイルバッテリー 耐火ケース','衣類用 補修シート','晒し布','手動ドリル'])assert.equal(scopeGate(s).status,'pass',s);
});
test('購入50点、重量、価格と既定NGを適用。低価格・食品というだけで落とさない',()=>{
 assert.equal(sourceGate(base).status,'pass');assert.equal(sourceGate({...base,title:'クエン酸 食品用',priceNew:100}).status,'pass');
 for(const [change,code]of [[{monthlySold:null},'demand_missing'],[{monthlySold:49},'demand_missing'],[{packageWeightG:501},'oversize_source'],[{priceNew:2501},'price_outside_baseline'],[{title:'マグネット補助板'},'known_commodity'],[{title:'精密ドライバー'},'known_commodity'],[{brand:'花王'},'major_brand_source'],[{packageMm:[358,296,39]},'outside_small_size'],[{packageMm:null},'size_missing']])assert.equal(sourceGate({...base,...change}).code,code);
 assert.deepEqual(filterSources([base,{...base,asin:'B000000002',title:'USBポンプ'},{...base,asin:'B000000003',monthlySold:null}]).counts,{pass:1,exclude:1,defer:1});
});
test('入力が非電気でもAIが電気製品へ変えた案を除外し、判定済み再掲を抑える',()=>{
 assert.equal(candidateGate({...item,kw:'USB 植物育成ライト'},[base]).code,'electrical');
 assert.equal(candidateGate(item,[base],[{kw:item.kw,decision:'reject'}]).code,'already_seen');
 assert.ok(ownMatches({kw:'水槽 底砂 コケ抑制'},['水槽の底砂 珪砂','液体のり']).includes('水槽の底砂 珪砂'));
});
const review={candidate_id:item.candidate_id,decision:'propose',codes:[],reason:'室内の植え替えで土の片付けを減らす',matched_asins:[base.asin],buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'ベランダのない住まいで鉢を植え替えるとき、床の土汚れと後片付けを減らす'};
test('判定前の案を自動通過させず、ブランド・重複・検討理由の不足を拒否する',()=>{
 assert.equal(validateScreen({items:[review]},[item])[0].decision,'propose');
 for(const change of [{buy_by:'brand'},{own_overlap:'unknown'},{commodity:'avoid'},{opportunity:''},{matched_asins:['B999999999']}])assert.throws(()=>validateScreen({items:[{...review,...change}]},[item]));
 assert.throws(()=>validateScreen({items:[]},[item]),/SCREEN_COUNT_MISMATCH/);
});
test('固定枠へ絞らず、選別結果で推薦・調査待ち・除外を分ける',async()=>{
 const out=await screenCandidates([item,{...item,candidate_id:'electric',kw:'電池式ライト'}],[base],{ownNames:[],execution:{attestations:{}},session:{budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}},invokeFn:async()=>({status:'OK',response:JSON.stringify({items:[{...review,decision:'defer',codes:['no_opportunity'],reason:'元の商品名の言い換えだけで検討する理由がまだない'}]})})});
 assert.equal(out.items.length,0);assert.equal(out.records.filter(r=>r.decision==='exclude').length,1);assert.equal(out.records.filter(r=>r.decision==='defer').length,1);
});
test('選別付きルートは発案3回と選別3回を同じ利用枠に収める',()=>{
 const b=new RunBudget({run_id:'screen-budget',deadline:new Date(Date.now()+80*60000).toISOString(),profile:'kw-screened-v3'});for(let n=0;n<3;n++){b.reserve('R01');b.reserve('R03');}assert.throws(()=>b.reserve('R01'),/STAGE_CALL_LIMIT/);assert.throws(()=>b.reserve('R03'),/STAGE_CALL_LIMIT/);
});

test('方針選別前の旧88案版は公開前に止まり、送信しない',async()=>{
 const fs=require('fs'),os=require('os'),path=require('path');const dir=fs.mkdtempSync(path.join(os.tmpdir(),'kw-old-version-'));let posted=false;
 try{await assert.rejects(()=>require('./kw-publish.cjs').publish({state_dir:dir},{env:{MIRROR_SYNC_KEY:'test-only'},fetchFn:async(_url,options)=>{if(options.method==='POST')posted=true;return {ok:true,json:async()=>({history:[],feedback_cursor:0,feedback_has_more:false})};},runFn:async()=>({schema_version:'kw-discovery-v2',policy_version:'kw-discovery-20260910-2',items:[]})}),/EDITION_REQUIRES_RESCREEN/);assert.equal(posted,false);}finally{fs.rmSync(dir,{recursive:true,force:true});}
});
const many=[0,1,2,3,4,5].map(n=>({...item,candidate_id:'many-'+n,kw:'植え替えシート'+n}));
const reviewOf=(c,over={})=>({...review,candidate_id:c.candidate_id,...over});
const runner={ownNames:[],execution:{attestations:{}},session:{budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}}};
test('選別回答の1件が形式を外しても、その候補だけ外して残りの案は残す',()=>{
 const items=many.map(c=>reviewOf(c));
 items[1]={...items[1],own_overlap:'unknown'};
 items[2]={...items[2],decision:'exclude',codes:['history_duplicate','own_overlap'],reason:'既存の取扱品と同じ用途'};
 assert.throws(()=>validateScreen({items},many));
 const {reviews,invalid,unreliable}=partitionScreen({items},many);
 assert.equal(reviews.length,4);assert.ok(reviews.every(r=>r.decision==='propose'));assert.equal(unreliable,false);
 assert.deepEqual(invalid.map(i=>i.code),['SCREEN_NOT_ELIGIBLE','INVALID_SCREEN_CODE']);
 assert.deepEqual(invalid.map(i=>i.kw),[many[1].kw,many[2].kw]);
 const short=partitionScreen({items:items.slice(1)},many);
 assert.deepEqual(short.invalid.map(i=>[i.kw,i.code]),[[many[0].kw,'MISSING_SCREEN_ROW'],[many[1].kw,'SCREEN_NOT_ELIGIBLE'],[many[2].kw,'INVALID_SCREEN_CODE']]);
 assert.deepEqual([short.reviews.length,short.unreliable],[3,true]);
});
test('回答の多くが形式を外していたら、また有効な回答が残らなければ、選別を信用しない',()=>{
 assert.equal(partitionScreen({items:many.map(c=>reviewOf(c,{own_overlap:'unknown'}))},many).unreliable,true);
 const one=partitionScreen({items:[reviewOf(many[0],{own_overlap:'unknown'})]},[many[0]]);
 assert.deepEqual([one.reviews.length,one.invalid.length,one.unreliable],[0,1,true]);
});
test('回答行の重複・欠落・並び順で採否が変わらない',()=>{
 const three=many.slice(0,3);
 const rows=[reviewOf(three[0]),reviewOf(three[0],{own_overlap:'unknown'}),reviewOf(three[2])];
 const a=partitionScreen({items:rows},three),b=partitionScreen({items:[rows[1],rows[0],rows[2]]},three);
 assert.deepEqual(a.reviews.map(r=>r.candidate_id),[three[2].candidate_id]);
 assert.deepEqual(a.reviews.map(r=>r.candidate_id),b.reviews.map(r=>r.candidate_id));
 assert.deepEqual(a.invalid.map(i=>[i.kw,i.code]),[[three[0].kw,'DUPLICATE_SCREEN_ID'],[three[1].kw,'MISSING_SCREEN_ROW']]);
 assert.deepEqual(a.invalid,b.invalid);
 const unknown=partitionScreen({items:[reviewOf(three[0]),{...reviewOf(three[1]),candidate_id:'not-a-candidate'},reviewOf(three[2])]},three);
 assert.equal(unknown.unknown_rows,1);
 assert.deepEqual(unknown.invalid.map(i=>[i.kw,i.code]),[[three[1].kw,'MISSING_SCREEN_ROW']]);
});
test('壊れた行・欠けた行が混ざっても、ほかの案は提案へ進めて記録に残す',async()=>{
 const cands=many.map(c=>({...c,seed_asins:[base.asin]}));
 const answer={items:[reviewOf(cands[0]),null,{...reviewOf(cands[2]),codes:42},reviewOf(cands[3]),reviewOf(cands[4]),reviewOf(cands[5])]};
 const saved=[];
 const out=await screenCandidates(cands,[base],{...runner,saveStage:async(name,value)=>{saved.push([name,value]);},invokeFn:async()=>({status:'OK',response:JSON.stringify(answer)})});
 assert.equal(out.items.length,4);assert.equal(out.invalid_reviews,2);assert.equal(out.audit.unknown_screen_rows,1);
 const dropped=out.records.filter(r=>r.codes.includes('invalid_screen_response'));
 assert.deepEqual(dropped.map(r=>[r.kw,r.decision]),[[cands[1].kw,'defer'],[cands[2].kw,'defer']]);
 assert.match(dropped[0].reason,/MISSING_SCREEN_ROW/);assert.match(dropped[1].reason,/INVALID_SCREEN_CODE/);
 const validation=saved.find(([name])=>name==='screen-1-validation');
 assert.ok(validation);assert.deepEqual([validation[1].valid,validation[1].invalid.length,validation[1].unknown_rows,validation[1].unreliable],[4,2,1,false]);
});
test('信用できない回答でも、外した候補を記録へ保存してから止める',async()=>{
 const cands=many.map(c=>({...c,seed_asins:[base.asin]}));const saved=[];
 const answer={items:cands.map(c=>reviewOf(c,{own_overlap:'unknown'}))};
 await assert.rejects(()=>screenCandidates(cands,[base],{...runner,saveStage:async(name,value)=>{saved.push([name,value]);},invokeFn:async()=>({status:'OK',response:JSON.stringify(answer)})}),/SCREEN_RESPONSE_UNRELIABLE/);
 const validation=saved.find(([name])=>name==='screen-1-validation');
 assert.ok(validation);assert.equal(validation[1].invalid.length,6);assert.equal(validation[1].unreliable,true);
});
test('回答の行数が増減しても候補ごとに扱い、判断履歴のない理由もその行だけ落とす',()=>{
 const three=many.slice(0,3);
 const extra=partitionScreen({items:[reviewOf(three[0]),reviewOf(three[1]),reviewOf(three[2]),{...reviewOf(three[2])},{...reviewOf(three[0]),candidate_id:'unknown-id'}]},three);
 assert.deepEqual([extra.reviews.length,extra.unknown_rows],[2,1]);
 assert.deepEqual(extra.invalid.map(i=>[i.kw,i.code]),[[three[2].kw,'DUPLICATE_SCREEN_ID']]);
 const feedback={...reviewOf(three[1]),decision:'defer',codes:['feedback_constraint'],reason:'代表の同じ懸念が解消していない'};
 const rows={items:[reviewOf(three[0]),feedback,reviewOf(three[2])]};
 const noHistory=partitionScreen(rows,three,{feedback_context:false});
 assert.deepEqual(noHistory.invalid.map(i=>[i.kw,i.code]),[[three[1].kw,'FEEDBACK_CONTEXT_REQUIRED']]);
 assert.deepEqual([noHistory.reviews.length,noHistory.unreliable],[2,false]);
 assert.equal(partitionScreen(rows,three).invalid.length,0);
});
test('見送りだけの回答は通し、許容ちょうどは止めず、超えたら止める',()=>{
 const allDefer=many.map(c=>reviewOf(c,{decision:'defer',codes:['no_opportunity'],reason:'元の商品名の言い換えで検討する理由がない'}));
 const out=partitionScreen({items:allDefer},many);
 assert.deepEqual([out.reviews.length,out.invalid.length,out.tolerated,out.unreliable],[6,0,2,false]);
 const two=allDefer.map((r,n)=>n<2?{...r,codes:[]}:r);
 assert.deepEqual([partitionScreen({items:two},many).invalid.length,partitionScreen({items:two},many).unreliable],[2,false]);
 const over=allDefer.map((r,n)=>n<3?{...r,codes:[]}:r);
 assert.deepEqual([partitionScreen({items:over},many).invalid.length,partitionScreen({items:over},many).unreliable],[3,true]);
 const pair=many.slice(0,2);
 const half=partitionScreen({items:[reviewOf(pair[0]),reviewOf(pair[1],{own_overlap:'unknown'})]},pair);
 assert.deepEqual([half.reviews.length,half.invalid.length,half.tolerated,half.unreliable],[1,1,1,false]);
});
