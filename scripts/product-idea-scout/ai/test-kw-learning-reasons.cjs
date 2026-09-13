'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {interpretJudgement,learningContext,preferenceScore,preferenceScorer,relatedHistory,sameKeyword,LEARNING_RULE_VERSION}=require('./kw-learning.cjs');
const {catalogNames}=require('./kw-catalog.cjs');
const {candidateGate,ownMatches}=require('./kw-filters.cjs');
const {screenCandidates}=require('./kw-screen.cjs');
const {discover}=require('./kw-discovery.cjs');
const {keywordId}=require('./kw-core.cjs');
const event=(kw,decision,reason,extra={})=>({candidate_id:keywordId(kw),kw,use:kw,category:'園芸',decision,reason,reason_codes:[],event_seq:1,...extra});
const source={asin:'B000000001',title:'植え替え 土受け シート',categoryPath:'園芸',priceNew:500,monthlySold:100,packageMm:[150,100,10],packageWeightG:20,brand:'小さなメーカー'};
const candidate={candidate_id:keywordId('植え替え シート'),kw:'植え替え シート',use:'鉢の土を受ける',idea:'植え替え時の土受け',reason:'室内の片付けを減らす',seed_asins:[source.asin],learning_refs:[]};
const session=()=>({state:{deadline:new Date(Date.now()+80*60000).toISOString()},budget:()=>({}),saveBudget:()=>{},recordStage:()=>{}});
const parse=prompt=>JSON.parse(prompt.split('<untrusted_data>\n')[1].split('\n</untrusted_data>')[0]);
test('見送りのまま方向性への肯定と価格の懸念を別々に保持する',()=>{
 const e=event('園芸 シート','reject','選定はいいけど、他社の価格が安すぎる');const before=JSON.stringify(e),s=interpretJudgement(e);
 assert.equal(s.direction,'positive');assert.deepEqual(s.blockers,['price_competition']);assert.equal(s.weight,2);assert.equal(s.actionable_positive,false);assert.equal(JSON.stringify(e),before);
 assert.ok(preferenceScore(source,[e])>0);
});
test('既存品・過去案はいいでも新規の採用例に数えない',()=>{
 const a=event('園芸 シート','adopt','すでに商品化している。選定はいい。'),b=event('土受け トレー','adopt','選定はよい。過去に提案済み',{event_seq:2});
 const context=learningContext([a,b],[]);assert.equal(context.counts.adopt,2);assert.equal(context.direction_counts.positive,2);assert.equal(context.actionable_positive_count,0);assert.equal(context.constraint_counts.own_duplicate,1);assert.equal(context.constraint_counts.already_proposed,1);assert.equal(context.rule_version,LEARNING_RULE_VERSION);
});
test('金型や安全への懸念を用途全体の嫌いに変えず、意味の取れない理由は未知にする',()=>{
 for(const reason of ['これを作るのに金型が必要。','事故時の責任が重い']){const s=interpretJudgement(event('園芸 シート','reject',reason));assert.equal(s.direction,'unknown');assert.equal(s.weight,0);assert.equal(s.blockers.length,1);}
 const unknown=interpretJudgement(event('園芸 シート','hold','まだ何とも言えない'));assert.equal(unknown.direction,'unknown');assert.equal(unknown.weight,0);assert.deepEqual(unknown.blockers,[]);
});
test('理由の否定文と相反する記録を肯定・既出と誤読しない',()=>{
 const s=interpretJudgement(event('園芸 シート','reject','金型が必要ない。中国製品が強いとは思わない。過去に提案済みではない。すでに商品化していない。選定はいいとは思わない。'));
 assert.deepEqual(s.blockers,[]);assert.equal(s.direction,'negative');
 const conflict=interpretJudgement(event('園芸 シート','reject','方向性は良くない',{reason_codes:['use_clear']}));assert.equal(conflict.conflict,true);assert.equal(conflict.weight,0);
});
test('構造化した理由を優先し、訂正した判断だけを探索順へ使う',()=>{
 const a=event('植え替え','reject','理由は選択済み',{reason_codes:['use_clear','price_competition']}),b={...a,event_seq:2,decision:'hold',reason_codes:[],reason:'再確認'};
 assert.equal(interpretJudgement(a).direction,'positive');assert.equal(preferenceScorer([b,a])(source),0);
 assert.notEqual(learningContext([a],[]).version,learningContext([b],[]).version);
});
test('別の商品への見送りで同じカテゴリ全体を下げず、高速・参照計算を一致させる',()=>{
 const es=[event('植え替え シート','reject','選定はいいが、競合が安い'),event('排水 トレー','reject','方向性は良くない',{event_seq:2}),event('土受け','hold','再確認',{event_seq:3})];
 const fast=preferenceScorer(es);for(const title of ['植え替え シート','排水 トレー','土受け シート','無関係な植木鉢']){const row={title,categoryPath:'園芸'};assert.equal(fast(row),preferenceScore(row,es));}
 assert.equal(fast({title:'無関係な植木鉢',categoryPath:'園芸'}),0);
});
test('語順違いの既出KWを抑え、別の用途・付属品は一致と断定しない',()=>{
 assert.equal(sameKeyword('植え替え シート','シート 植え替え'),true);assert.equal(sameKeyword('植え替え シート','植え替えシート'),true);assert.equal(sameKeyword('植え替え シート','植え替え シート 補修テープ'),false);
 assert.equal(candidateGate(candidate,[source],[{kw:'シート 植え替え',decision:'adopt'}]).code,'already_seen');
});
test('過去案の重複記録より最新の人の理由を選別入力へ残す',()=>{
 const kw='植え替え トレー',old={candidate_id:keywordId(kw),kw,screened:true},latest=event(kw,'reject','選定はよい。過去に提案済み',{event_seq:20});
 const previous=relatedHistory(candidate,[...Array(20).fill(old),latest],12);assert.equal(previous.length,1);assert.equal(previous[0].reason,latest.reason);assert.equal(previous[0].interpretation.direction,'positive');
});
test('自社と取扱品の名称を区別して抽出し、価格・仕入れ情報をAI向けに持ち出さない',()=>{
 const result=catalogNames({families:[{salesClass:1,familyKey:'土受け',products:[{name:'土受けシート 詳細名',cost:99,supplierCode:'private'}]},{salesClass:2,familyKey:'AMC参考: 水やり器',products:[{name:'水やり器 詳細名'}]},{salesClass:3,familyKey:'範囲外'}]});
 assert.deepEqual(result.ownNames,['土受け','土受けシート 詳細名']);assert.deepEqual(result.handledNames,['水やり器','水やり器 詳細名']);assert.ok(!JSON.stringify(result).includes('private'));assert.ok(!JSON.stringify(result).includes('cost'));
});
test('自社または既存取扱品との名称一致はAIを呼ばずに除外する',async()=>{
 for(const key of ['ownNames','handledNames']){let calls=0;const r=await screenCandidates([candidate],[source],{[key]:['シート 植え替え'],invokeFn:async()=>{calls++;}});assert.equal(calls,0);assert.equal(r.records[0].codes[0],'own_duplicate');}
});
test('選別にも理由・既存取扱品・最新判断を渡し、懸念の繰り返しを調査待ちにできる',async()=>{
 const judged=event('植え替え トレー','reject','選定はいいが、競合が安い');let input;
 const result=await screenCandidates([candidate],[source],{handledNames:['植え替えマット 現行品'],judgements:[judged],history:[{...judged,reason:'古い記録',event_seq:0},judged],execution:{attestations:{}},session:session(),invokeFn:async(stage,prompt)=>{assert.equal(stage,'R03');input=parse(prompt);return {status:'OK',response:JSON.stringify({items:[{candidate_id:candidate.candidate_id,decision:'defer',codes:['feedback_constraint'],reason:'同用途の廉価品と比べて、代表の価格懸念を解消する違いがない',matched_asins:[]}]})};}});
 assert.equal(input.learning.direction_counts.positive,1);assert.equal(input.learning.constraint_counts.price_competition,1);assert.deepEqual(input.candidates[0].handled_matches,['植え替えマット 現行品']);assert.equal(input.candidates[0].own_matches.length,0);assert.equal(input.candidates[0].previous[0].reason,judged.reason);assert.equal(result.items.length,0);assert.equal(result.audit.learning_rule_version,LEARNING_RULE_VERSION);
});
test('発案から選別まで同じ理由の解釈と分離した商品台帳を渡す',async()=>{
 const judged=event('園芸 土受け','reject','選定はよい。過去に提案済み');const seen={};
 const result=await discover({run_id:'reason-integration',day:'2026-09-13',rows:[source],ownNames:['植え替え用 土受けタオル'],handledNames:['植え替えマット 現行品'],judgements:[judged],state:{history:[],scan:{cycle:1,seen_asins:[]}},session:session(),execution:{attestations:{}},saveState:async()=>{},saveStage:async()=>{},invokeFn:async(stage,prompt)=>{seen[stage]=parse(prompt);return {status:'OK',response:JSON.stringify(stage==='R01'?{items:[candidate],no_idea:[]}:{items:[{candidate_id:candidate.candidate_id,decision:'propose',codes:[],reason:'片付けがしやすい',matched_asins:[source.asin],buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'室内で鉢を植え替える際に土を集めて戻しやすい袋状の土受け'}]})};}});
 assert.equal(result.items.length,1);assert.equal(seen.R01.learning.rule_version,LEARNING_RULE_VERSION);assert.equal(seen.R03.learning.rule_version,LEARNING_RULE_VERSION);assert.ok(seen.R01.company_reference.handled_names.length>0);assert.equal(result.learning_audit.direction_counts.positive,1);
});

test('名称照合で汎用の洗浄剤より同じ対象物の既存品を先に出す',()=>{
 const names=[...Array.from({length:20},(_,i)=>'対象'+i+' 洗浄剤'),'タンク クリーナー'];
 assert.equal(ownMatches({kw:'タンク 洗浄剤'},names)[0],'タンク クリーナー');
});
