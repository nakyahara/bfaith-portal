'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');const core=require('./kw-core.cjs');
const {generate}=require('./kw-pipeline.cjs');const {invoke}=require('./cli.cjs');
const stamp='2026-09-10T00:00:00Z';const now=Date.parse(stamp);
const p=(a,title,categoryPath)=>({asin:a,title,categoryPath});
const pool=[p('B000000001','園芸 植え替えシート','園芸'),p('B000000002','鍋 保護シート','調理')];
const kw=core.validateKeywords({items:[{kw:'植え替えシート',use:'土を受ける',idea:'土を受けるシート',reason:'用途名で探す',seed_asins:['B000000001']}]},pool);
const o=(asin,units=100)=>({asin,source:'Keepa Product Request',url:'https://www.amazon.co.jp/dp/'+asin,observed_at:stamp,demand_observed_at:stamp,price:500,monthly_units:units,parent_asin:'B000000009',title_excerpt:'園芸シート'});
const market={results:[{kw:'植え替えシート',observations:[{asin:'B000000001'},{asin:'B000000002'}]}],observations:[o('B000000001'),o('B000000002')]};
const review=c=>({candidate_id:c.candidate_id,decision:'retain',exclusion_code:'none',matched_asins:['B000000001','B000000002'],match_reason:'用途一致',policy_reason:'用途名で検索',competition_note:'比較が必要',unknowns:[]});
test('自然な商品名1語を受け付け、汎用カテゴリと架空の元ASINは拒否する',()=>{
 assert.equal(kw[0].kw,'植え替えシート');assert.throws(()=>core.validateKeywords({items:[{...kw[0],kw:'用品'}]},pool),/GENERIC/);
 assert.throws(()=>core.validateKeywords({items:[{...kw[0],seed_asins:['B099999999']}]},pool),/UNKNOWN_SEED/);
});
test('探索種はカテゴリを分散し、古い価格と購入数をAIへ渡さない',()=>{
 const rows=Array.from({length:60},(_,i)=>p('B'+String(i).padStart(9,'0'),'商品 '+i,i<50?'A':'B'));
 const selected=core.selectPool(rows,'2026-09-10',10);assert.equal(selected.length,10);assert.equal(new Set(selected.map(p=>p.category)).size,2);
 assert.ok(selected.every(p=>!('monthlySold'in p)));assert.deepEqual(selected,core.selectPool(rows,'2026-09-10',10));
});
test('製造先・原価・入数がなくても根拠付きKW案を保持、親商品の需要を足さない',()=>{
 const c=core.assembleCandidates(kw,market,[],[],now);const out=core.finalize(c,[review(c[0])],{run_id:'test',day:'2026-09-10',now:stamp});
 assert.equal(out.items[0].state,'idea');assert.equal(out.items[0].demand_lower_bound,100);assert.equal(out.items[0].search_volume,null);assert.equal(out.status,'partial');core.validateEdition(out,now);
});
test('古い需要は新しくダウンロードしても復活させず、追加確認案として残す',()=>{
 const m=structuredClone(market);m.observations.forEach(o=>o.demand_observed_at='2020-01-01T00:00:00Z');const c=core.assembleCandidates(kw,m,[],[],now);
 const out=core.finalize(c,[review(c[0])],{run_id:'test',day:'2026-09-10',now:stamp});assert.equal(out.items[0].state,'research');assert.equal(out.new_count,0);
});
test('候補間の根拠流用と証拠なしの自社重複除外を拒否する',()=>{
 const c=core.assembleCandidates(kw,market,[],[],now);
 assert.throws(()=>core.validateReviews({items:[{...review(c[0]),matched_asins:['B099999999']}]},c),/UNKNOWN_MATCHED/);
 assert.throws(()=>core.validateReviews({items:[{...review(c[0]),decision:'exclude',exclusion_code:'confirmed_own_duplicate'}]},c),/NO_OWN/);
});
test('既存KW再掲を新規に数えず、数量捏造・外部URLを画面へ通さない',()=>{
 const c=core.assembleCandidates(kw,market,[],[{kw:'植え替えシート'}],now);const out=core.finalize(c,[review(c[0])],{run_id:'test',day:'2026-09-10',now:stamp});assert.equal(out.new_count,0);
 const bad=structuredClone(out);bad.items[0].demand_lower_bound=999;assert.throws(()=>core.validateEdition(bad,now),/INVALID_CARD_DEMAND/);
 bad.items[0].evidence[0].url='https://evil.example';assert.throws(()=>core.validateEdition(bad,now),/INVALID_EVIDENCE_URL/);
});
test('日付の古い確認済み課金設定を保持し、取消された記録は推論しない',async()=>{
 let calls=0;const opts={cwd:__dirname,env:{},command:{file:'unused'},billing_attestation:{provider:'claude',additional_usage_disabled:true,checked_by:'user',checked_at:'2025-01-01T00:00:00Z'},budget:{reserve:()=>({id:1}),snapshot:()=>({}),finish:()=>{}},save_budget:async()=>{},execute:async(_c,args)=>{
 if(args[0]==='--version')return {code:0,stdout:'version'};if(args[0]==='auth')return {code:0,stdout:JSON.stringify({loggedIn:true,authMethod:'claude.ai',subscriptionType:'max'})};calls++;return {code:0,stdout:JSON.stringify({type:'assistant',message:{model:'claude-sonnet-5'}})+'\n'+JSON.stringify({type:'result',result:'ok'})};}};
 assert.equal((await invoke('R01','test',opts)).status,'OK');opts.billing_attestation.revoked=true;assert.equal((await invoke('R01','test',opts)).status,'BILLING_UNVERIFIED');assert.equal(calls,1);
});
test('Keepa→KW→競合→評価→独立検査→編集の接続で未知製造の案を出す',async()=>{
 const saves=[];const session={budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}};
 const outputs={R01:{items:kw.map(({candidate_id,...k})=>k)},R03:{items:[review(kw[0])]},R05:{issues:[]},R06:{items:[{candidate_id:kw[0].candidate_id,idea:'土を受けるシート',policy_reason:'用途で探す商品'}]}};
 const keepaMinute=Math.floor(now/60000)-21564000;
 const result=await generate({run_id:'connection',day:'2026-09-10',rows:pool,session,execution:{attestations:{}},saveMarket:async()=>{},saveStage:async(s)=>saves.push(s),now:()=>stamp,keepaCall:async(endpoint)=>endpoint==='/search'?{asinList:['B000000001','B000000002']}:{products:['B000000001','B000000002'].map(asin=>({asin,domainId:5,productType:0,lastUpdate:keepaMinute,lastSoldUpdate:keepaMinute,monthlySold:100,title:'園芸シート',stats:{current:[0,500]}}))},invokeFn:async stage=>({status:stage==='R05'?'MODEL_UNVERIFIED':'OK',actual_model:stage==='R05'?'unknown':'test',response:JSON.stringify(outputs[stage])})});
 assert.deepEqual(saves,['R01','R03','R06','R05']);assert.equal(result.items[0].state,'idea');assert.equal(result.model_audit[3].actual_model,'unknown');assert.equal(result.warnings.length,2);
});
