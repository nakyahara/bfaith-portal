'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {scopeGate,sourceGate,candidateGate,ownMatches,filterSources}=require('./kw-filters.cjs');
const {validateScreen,screenCandidates}=require('./kw-screen.cjs');const {RunBudget}=require('./budget.cjs');
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
