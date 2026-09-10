'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const crypto=require('node:crypto');
const {hash}=require('./common.cjs');
const {buildPacket,cacheKey,PacketCache,candidateDistribution,usageRecord}=require('./packet.cjs');
const {validateOutput}=require('./validate.cjs');
const {billingEnvironment,configHazards,classifyError,preflight,invoke,invocationArgs,parseResponse}=require('./cli.cjs');
const {RunBudget}=require('./budget.cjs');
const {sample,threeMonthsBefore,buildSealedSet}=require('./sealed-set.cjs');
const now='2026-09-09T00:00:00Z';
function source(id='C1') {return {candidate_id:id,kw:'試験 布',use:'覆う',target:'容器',form:'布',spec_hypothesis:'2枚',pack_qty:2,
  economics:{target_price:980,allowable_landed_cost:430,unit_allowable_cost:215,basis:'合成例',computed_by:'program'},
  observations:[{observation_id:'OB-'+id,asin:'B000000001',source:'合成例',observed_at:'2026-09-08',price:980,monthly_units:90,url:'https://www.amazon.co.jp/dp/B000000001',title_excerpt:'テスト'}],
  capability_refs:[{evidence_id:'EV-'+id,process:'裁断',confidence:'C1',maker:'AMC確認済み',scope:'布',confirmed_on:'2026-04-18',expires_at:'2027-04-18'}],
  own_matches:[{product_id:'P1',relation:'類似',note:'別用途'}],previous_decisions:[],missing_fields:[],brand_signals:{top1_share_of_observed_units:0.22}};}
function packet(rows=[source()]){return {run_id:'run-1',stage:'R03',schema_version:'w04-r03-2',rule_version:'w04-20260909',target_q:['Q02'],candidates:rows};}
function response(input=packet()){return {run_id:input.run_id,stage:input.stage,schema_version:input.schema_version,rule_version:input.rule_version,items:input.candidates.map(c=>({candidate_id:c.candidate_id,decision:'continue',q_findings:{Q02:{result:'pass',reason:'実在',evidence_ids:[c.observations[0].observation_id]}},claims:[{text:'実在',type:'事実',evidence_ids:[c.observations[0].observation_id]}],counter_evidence:[{text:'競合範囲が限定',evidence_ids:[]}],first_recall_brand:{basis:'observed_signal',verdict:'なし',signal_used:['top1_share_of_observed_units=0.22']},spec_hypothesis:'2枚',one_question_draft:'入数2、1枚あたり215円で収まりますか',unknowns:[],requested_evidence:[],short_reason:'相談可能'}))};}
const versions={rule_version:'1',prompt_version:'1',model_id:'claude-opus-5',effort:'medium',source_version:'s1',decision_version:'d1'};
test('packet excludes internal cost, supplier and sales fields at every structured boundary',()=>{
 const s=source();s.cost=123;s.supplierCode='PRIVATE';s.qtyAll=777;s.own_matches[0].cost=99;s.observations[0].supplierCode='PRIVATE';
 const p=buildPacket(packet([s]),['C1'],now);assert.doesNotMatch(JSON.stringify(p),/PRIVATE|supplierCode|qtyAll|"cost"/);
 assert.equal(p.candidates[0].economics.unit_allowable_cost,215);
});
test('unknown IDs, duplicate candidates and arithmetic mismatch are refused',()=>{
 assert.throws(()=>buildPacket(packet(),['missing'],now),/UNKNOWN_CANDIDATE/);
 assert.throws(()=>buildPacket(packet(),['C1','C1'],now),/DUPLICATE/);
 const s=source();s.economics.unit_allowable_cost=214;assert.throws(()=>buildPacket(packet([s]),['C1'],now),/ECONOMICS_UNIT_MISMATCH/);
});
test('missing, future and stale evidence dates stay missing',()=>{
 for(const date of [undefined,'2026-01-01','2027-01-01']){const s=source();s.observations[0].observed_at=date;const p=buildPacket(packet([s]),['C1'],now);assert.ok(p.candidates[0].missing_fields.includes('OB-C1:freshness'));}
});
test('cache depends on observations, decisions, model, rules, not run label or key insertion order',()=>{
 const p=packet(),key=cacheKey(p,versions);assert.equal(cacheKey({...p,run_id:'next'},versions),key);
 for(const k of Object.keys(versions))assert.notEqual(cacheKey(p,{...versions,[k]:'changed'}),key);
 const changed=packet();changed.candidates[0].observations[0].price=100;assert.notEqual(cacheKey(changed,versions),key);
 assert.throws(()=>cacheKey(p,{...versions,model_id:'unknown'}),/CACHE_VERSION_REQUIRED/);
});
test('cache misses corrupted, expired and wrong-key records without reading an alternate path',t=>{
 const key=hash('test'),result={decision:'hold'},cache=new PacketCache('/not-written');
 let stored=JSON.stringify({key,expires_at:'2026-09-10',result,result_hash:hash(result)}); t.mock.method(fs,'readFileSync',()=>stored);
 assert.deepEqual(cache.get(key,Date.parse(now)),result);assert.equal(cache.get(key,Date.parse('2026-09-11')),null);
 stored='{broken';assert.equal(cache.get(key),null);
 assert.throws(()=>cache.get('../auth'),/INVALID_CACHE_KEY/);
});
test('usage distinguishes measurement from estimates and distribution retains unknowns',()=>{
 assert.equal(usageRecord(null,'日本','語').kind,'estimated');assert.equal(usageRecord({input_tokens:2,output_tokens:1}).kind,'measured');
 const d=candidateDistribution([{candidate_id:'a',processes:['裁断'],materials:['綿'],target_price:500},{candidate_id:'b'}]);
 assert.equal(d.total,2);assert.equal(d.price_band.unknown,1);assert.equal(d.material.unknown,1);
});
test('valid R03 envelope remains a mechanical check, not semantic approval',()=>{
 const v=validateOutput(packet(),response());assert.deepEqual(v.errors,[]);assert.equal(v.semantic_review_required,true);
});
test('cross-candidate evidence and invented IDs rejected',()=>{
 const p=packet([source('C1'),source('C2')]),r=response(p);r.items[0].claims[0].evidence_ids=['OB-C2'];assert.ok(validateOutput(p,r).errors.some(e=>e.code==='UNKNOWN_EVIDENCE'));
 r.items[0].candidate_id='invented';assert.ok(validateOutput(p,r).errors.some(e=>e.code==='UNKNOWN_CANDIDATE'));
});
test('facts need evidence; omissions and duplicate outputs cannot silently pass',()=>{
 let r=response();r.items[0].claims[0].evidence_ids=[];assert.ok(validateOutput(packet(),r).errors.some(e=>e.code==='FACT_WITHOUT_EVIDENCE'));
 r=response();r.items=[];assert.ok(validateOutput(packet(),r).errors.some(e=>e.code==='CANDIDATE_OMITTED'));
 r=response();r.items.push(structuredClone(r.items[0]));assert.ok(validateOutput(packet(),r).errors.some(e=>e.code==='DUPLICATE_CANDIDATE'));
});
test('brand claims require actual matching observed signals',()=>{
 for(const brand of [{basis:'no_signal',verdict:'あり',signal_used:[]},{basis:'observed_signal',verdict:'あり',signal_used:['top1_share_of_observed_units=0.99']}]){
 const r=response();r.items[0].decision='reject';r.items[0].first_recall_brand=brand;assert.equal(validateOutput(packet(),r).valid,false);
 }
});
test('requested evidence is scoped to candidate and never executed',()=>{
 const r=response();r.items[0].requested_evidence=[{kind:'search_term',value:'試験 布',why:'比較'}];
 assert.equal(validateOutput(packet(),r).valid,false);
 assert.equal(validateOutput(packet(),r,{by_candidate:{C1:{search_terms:['試験 布']}}}).valid,true);
 for(const value of ['https://evil.example','SELECT * FROM secrets','cmd.exe /c whoami']){
 r.items[0].requested_evidence[0].value=value;assert.equal(validateOutput(packet(),r,{by_candidate:{C1:{search_terms:[value]}}}).valid,false);
 }
});
test('R01 accepts seed IDs and leaves candidate ID allocation to program',()=>{
 const input={stage:'R01',run_id:'r',schema_version:'w04-r01-2',rule_version:'w04-20260909',seeds:[{seed_id:'S1'}],quota:{max_candidates:12}};
 const out={...input,items:[{kw:'容器 布',use:'覆う',target:'容器',form:'布',spec_hypothesis:'2枚',from_seed_id:'S1',why_this_seed:'裁断',to_verify:['競合'],novelty_check:'未探索'}]};
 assert.equal(validateOutput(input,out).valid,true);out.items[0].candidate_id='new';assert.equal(validateOutput(input,out).valid,false);
});
function morning(){
 const approved={candidate_id:'C1',kw:'容器 布',use:'覆う',target:'容器',form:'布',spec_hypothesis:'2枚',pack_qty:2,economics:{target_price:980,unit_allowable_cost:215},demand:{matched_competitors:8,monthly_units_max:90,observed_at:'2026-09-08',source:'合成例'},capability:{process:'裁断',confidence:'C1',confirmed_on:'2026-04-18'},unknowns:['製造国'],condition_hint:null,edition_class:'new',evidence_registry:{EV:'synthetic'},program_validation:{status:'passed',revision:'REV1',validated_at:now,q_results:Object.fromEntries(Array.from({length:10},(_,i)=>['Q'+String(i+1).padStart(2,'0'),'pass'])),unresolved_blockers:[],evidence_refs:['EV']}};
 const input={stage:'R06',run_id:'r',schema_version:'w04-r06-2',rule_version:'w04-20260909',approved:[approved]};
 const output={stage:'R06',run_id:'r',schema_version:'w04-r06-2',rule_version:'w04-20260909',omitted:[],items:[{candidate_id:'C1',edition_class:'new',evidence_ids:['EV'],rank:1,rank_reason:'質問が明確',headline:'容器用の布2枚',why_now:'競合8件、月90点（2026-09-08・合成例）',why_makeable:'裁断の確認（2026-04-18）',own_relation:'確認済み',economics_line:'売価980円 / 1枚あたり215円で作る必要がある（入数2）',amc_message:'容器用の布です。競合8件・月90点（2026-09-08・合成例）。売価980円、1枚215円（御社の利益を含めて）。裁断工程で収まりそうでしょうか',open_items:['製造国が未取得'],decision_options:['相談したい','保留','見送る']}]};return {input,output};
}
test('R06 valid mechanical contract and changed numeric fact',()=>{
 const {input,output}=morning();assert.deepEqual(validateOutput(input,output).errors,[]);
 output.items[0].why_now='月100個';assert.ok(validateOutput(input,output).errors.some(e=>e.rule==='R06 B'));
});
test('R06 catches date invented after verification and hides no unknown',()=>{
 const {input,output}=morning();output.items[0].why_makeable='2026-05-18確認';output.items[0].open_items=[];
 const v=validateOutput(input,output);assert.ok(v.errors.some(e=>e.code==='DATE_CHANGED_OR_INVENTED'));assert.ok(v.errors.some(e=>e.code==='UNKNOWN_HIDDEN'));
});
test('API and gateway env are detected by name; secret values are not reported',()=>{
 assert.deepEqual(billingEnvironment({ANTHROPIC_API_KEY:'secret',OPENAI_BASE_URL:'secret',PATH:'normal'}),['ANTHROPIC_API_KEY','OPENAI_BASE_URL']);
 assert.deepEqual(configHazards({apiKeyHelper:'secret'}),['apiKeyHelper']);
});
function fakeCli(auth='claude') {let calls=[];return {calls,execute:async(_cmd,args)=>{calls.push(args);return {code:0,stdout:args[0]==='--version'?'test-cli':args[0]==='auth'?JSON.stringify({loggedIn:true,authMethod:'claude.ai',subscriptionType:'max'}):'Logged in using ChatGPT',stderr:''};}};}
test('billing route mismatch prevents even authentication process start',async()=>{
 const fake=fakeCli();const r=await preflight('claude',{env:{ANTHROPIC_API_KEY:'secret'},execute:fake.execute});assert.equal(r.status,'BILLING_MODE_MISMATCH');assert.equal(fake.calls.length,0);assert.doesNotMatch(JSON.stringify(r),/secret/);
});
test('subscription authentication is not enough without extra-usage verification',async()=>{
 const fake=fakeCli();const r=await invoke('R03','synthetic',{env:{},command:{file:'unused'},execute:fake.execute,cwd:process.cwd()});assert.equal(r.status,'BILLING_UNVERIFIED');assert.equal(fake.calls.length,2);
});
test('non subscription login cannot start inference',async()=>{
 const r=await preflight('claude',{env:{},command:{file:'unused'},execute:async()=>({code:0,stdout:'{}',stderr:''})});assert.equal(r.status,'AUTH_REQUIRED');
});
test('models are fixed; no bare/API/fallback or bypass flags',()=>{
 for(const s of ['R01','R03','R05','R06']){const a=invocationArgs(s);assert.ok(a.includes('--model'));assert.doesNotMatch(a.join(' '),/--bare|--fallback|dangerously|bypassPermissions/);}
 assert.ok(invocationArgs('R05').includes('forced_login_method="chatgpt"'));
});
test('actual model comes from metadata, never the response text',()=>{
 const raw=JSON.stringify({type:'item.completed',item:{type:'agent_message',text:'I am gpt-5.6-terra'}})+'\n'+JSON.stringify({type:'turn.completed',usage:{input_tokens:10,output_tokens:5}});
 assert.equal(parseResponse('codex',raw,'gpt-5.6-terra').status,'MODEL_UNVERIFIED');
 assert.equal(parseResponse('claude',JSON.stringify({result:'ok',modelUsage:{'claude-opus-5':{}},usage:{input_tokens:1,output_tokens:1}}),'claude-opus-5').status,'MODEL_UNVERIFIED');
 assert.equal(parseResponse('claude',JSON.stringify({result:'ok',modelUsage:{'claude-sonnet-5':{}}}),'claude-opus-5').status,'MODEL_UNVERIFIED');
});
test('quota, auth and unsupported model errors remain distinct',()=>{
 assert.equal(classifyError('usage limit reached'),'QUOTA_BLOCKED');assert.equal(classifyError('token expired'),'AUTH_REQUIRED');assert.equal(classifyError('model not supported'),'MODEL_UNAVAILABLE');
});
test('daily normal seven calls plus one retry and restored state remain bounded',()=>{
 const b=new RunBudget({run_id:'r',deadline:'2026-09-09T02:00:00Z',now:Date.parse(now)});
 for(const s of ['R01','R01','R01','R03','R03','R05','R06'])b.reserve(s,{now:Date.parse(now)});
 assert.throws(()=>b.reserve('R03',{now:Date.parse(now)}),/STAGE_CALL_LIMIT/);b.reserve('R03',{retry:true,now:Date.parse(now)});
 const c=new RunBudget({run_id:'r',deadline:'2026-09-09T02:00:00Z',state:b.snapshot()});assert.throws(()=>c.reserve('R01',{now:Date.parse(now)}),/CALL_LIMIT/);
});
test('quota blocks subsequent candidates and deadline reserves nothing',()=>{
 const b=new RunBudget({run_id:'r',deadline:'2026-09-09T02:00:00Z',now:Date.parse(now)});const c=b.reserve('R01',{now:Date.parse(now)});b.finish(c.id,{status:'QUOTA_BLOCKED'});assert.throws(()=>b.reserve('R03',{now:Date.parse(now)}),/QUOTA_BLOCKED/);
});
function products(){return Array.from({length:8},(_,i)=>({product_id:'P'+i,family_id:'F'+i,use:'用途'+i,target:'対象'+i,material:i%2?'布':'紙',form:i%2?'裁断':'包装',spec:'仕様'+i,sales_class:1,price:i===0?500:2000,launched_on:'2025-07-01',effective_at:'2024-01-01'}));}
const selection={seed:'reproducible',count:3,low_price_max:750};
test('seeded selection independent of input order and includes low-price and diverse strata',()=>{
 const rows=products(),chosen=sample(rows,selection);assert.deepEqual(sample([...rows].reverse(),selection),chosen);assert.ok(chosen.some(p=>p.price<=750));assert.equal(new Set(chosen.map(p=>p.material)).size,2);
});
test('missing semantics prevents seal; no automatic product-name inference',()=>{
 const rows=products();delete rows[0].use;assert.throws(()=>sample(rows,selection),/METADATA_REQUIRED/);
});
test('family release is its earliest variant and subtracting months clamps calendar day',()=>{
 assert.equal(threeMonthsBefore('2024-05-31'),'2024-02-29');
 const rows=products();rows.push({...rows[0],product_id:'old',launched_on:'2020-01-01'});assert.throws(()=>sample(rows,selection),/LOW_PRICE_STRATUM_EMPTY/);
});
test('sealed set excludes all answers, families, same-use products and future evidence',()=>{
 const rows=products();rows.push({...rows[0],product_id:'variant',family_id:'other'});
 const keys=crypto.generateKeyPairSync('rsa',{modulusLength:2048});
 const input={products:rows,selection,records:[{record_id:'future',product_ids:[],use:'x',target:'x',effective_at:'2026-01-01'},{record_id:'same-use',product_ids:[],use:'用途0',target:'対象0',effective_at:'2020-01-01'}],public_key_pem:keys.publicKey.export({type:'spki',format:'pem'})};
 const bundle=buildSealedSet(input);assert.ok(bundle.cases.every(c=>c.records.length===0));assert.ok(bundle.cases.every(c=>!c.products.some(p=>p.use==='用途0')));
 assert.equal(bundle.manifest.status,'sealed-not-scored');assert.ok(!JSON.stringify(bundle.sealed).includes('用途'));
 const s=bundle.sealed,key=crypto.privateDecrypt({key:keys.privateKey,oaepHash:'sha256',padding:crypto.constants.RSA_PKCS1_OAEP_PADDING},Buffer.from(s.key,'base64'));
 const decipher=crypto.createDecipheriv('aes-256-gcm',key,Buffer.from(s.iv,'base64'));decipher.setAuthTag(Buffer.from(s.tag,'base64'));
 const answers=JSON.parse(Buffer.concat([decipher.update(Buffer.from(s.ciphertext,'base64')),decipher.final()]));assert.equal(answers.answers.length,3);
 assert.equal(bundle.cases[0].historical_snapshot_verified,false);
});
test('R03 latest typed economics/signals/lookup survive the packet',()=>{
 const c=source();c.economics.evidence_id='EC1';c.economics.tax_basis='税込';c.brand_signals.evidence_id='BS1';c.lookup_evidence={evidence_id:'LK1',status:'partial'};
 const p=buildPacket(packet([c]),['C1'],now);assert.equal(p.candidates[0].economics.evidence_id,'EC1');assert.equal(p.candidates[0].lookup_evidence.evidence_id,'LK1');
 const r=response(p);r.items[0].first_recall_brand.signal_used=['BS1'];assert.equal(validateOutput(p,r).valid,true);
 r.items[0].q_findings.Q06={result:'pass',reason:'類似なし',evidence_ids:['LK1']};assert.ok(validateOutput(p,r).errors.some(e=>e.code==='LOOKUP_NOT_COMPLETE'));
});
test('malformed output arrays are validation failures, not exceptions',()=>{
 for(const field of ['claims','unknowns','requested_evidence']){const r=response();r.items[0][field]={};assert.equal(validateOutput(packet(),r).valid,false);}
 const r=response();r.items[0].first_recall_brand.signal_used={};assert.equal(validateOutput(packet(),r).valid,false);
});
test('R06 requires a passed revision, no blockers, all Qs and preserved edition class',()=>{
 for(const alter of [c=>delete c.program_validation,c=>c.program_validation.unresolved_blockers.push('cost'),c=>c.program_validation.q_results.Q07='short']){
 const {input,output}=morning();alter(input.approved[0]);assert.ok(validateOutput(input,output).errors.some(e=>e.code==='NOT_VALIDATED_FOR_PUBLICATION'));
 }
 const {input,output}=morning();output.items[0].edition_class='recheck';assert.ok(validateOutput(input,output).errors.some(e=>e.code==='EDITION_CLASS_CHANGED'));
 output.items=[];assert.ok(validateOutput(input,output).errors.some(e=>e.code==='SILENT_OMISSION'));
});
test('budget persisted before inference; failed persistence starts no model',async()=>{
 const fake=fakeCli();const budget=new RunBudget({run_id:'today',deadline:new Date(Date.now()+3600000).toISOString()});
 const result=await invoke('R03','synthetic',{env:{},command:{file:'unused'},execute:fake.execute,cwd:process.cwd(),budget,save_budget:async()=>{throw Error('disk full');},billing_attestation:{provider:'claude',additional_usage_disabled:true,checked_by:'test',checked_at:new Date().toISOString()}});
 assert.equal(result.status,'BUDGET_SAVE_FAILED');assert.equal(fake.calls.length,2);assert.equal(budget.snapshot().calls.length,1);
});
test('R03 assembled path validates cached output and rebinds the run without calling CLI',async()=>{
 const {evaluateR03}=require('./stage.cjs');const v={...versions,rule_version:'w04-20260909'};const p=packet();const cached=response(p);let put=false;
 const result=await evaluateR03({...p,run_id:'new-run'},{candidate_ids:['C1'],versions:v,prompt:'immutable prompt',now,cache:{get:()=>cached,put:()=>{put=true;}}});
 assert.equal(result.status,'OK');assert.equal(result.cache_hit,true);assert.equal(result.output.run_id,'new-run');assert.equal(put,false);
});
test('warehouse reader is readonly and missing semantic metadata returns counts only',()=>{
 const {readWarehouse}=require('./warehouse-input.cjs');let closed=false;
 class FakeDB {constructor(file,options){assert.deepEqual(options,{readonly:true,fileMustExist:true});}pragma(q){if(q==='query_only=ON')return;return ['product_id','商品コード','売上分類','標準売価','new_product_launch_date','updated_at'].map(name=>({name}));}prepare(){return {all:()=>[{product_id:1,code:'SECRET-CODE',price:500,launched_on:'2025-01-01'}]};}close(){closed=true;}}
 const result=readWarehouse(FakeDB,'not-opened');assert.equal(result.status,'METADATA_REQUIRED');assert.doesNotMatch(JSON.stringify(result),/SECRET/);assert.ok(closed);
});

test('Claude response model excludes auxiliary usage and init configuration',()=>{
 const events=[{type:'system',subtype:'init',model:'claude-sonnet-5'},{type:'assistant',parent_tool_use_id:null,message:{model:'claude-opus-5'}},{type:'assistant',parent_tool_use_id:'tool-1',message:{model:'claude-haiku-4-5'}},{type:'result',result:'ok',modelUsage:{'claude-opus-5':{},'claude-haiku-4-5':{}},usage:{input_tokens:1,output_tokens:2}}];
 const raw=events.map(e=>JSON.stringify(e)).join('\n');const r=parseResponse('claude',raw,'claude-opus-5');
 assert.equal(r.status,'OK');assert.equal(r.actual_model,'claude-opus-5');assert.equal(r.model_evidence,'assistant.message.model');assert.equal(r.usage_models.length,2);assert.equal(r.usage.output_tokens,2);
 assert.equal(parseResponse('claude',raw,'claude-sonnet-5').status,'MODEL_MISMATCH');
 assert.equal(parseResponse('claude',[events[0],events[3]].map(e=>JSON.stringify(e)).join('\n'),'claude-sonnet-5').status,'MODEL_UNVERIFIED');
 assert.ok(invocationArgs('R01').includes('stream-json'));assert.ok(invocationArgs('R01').includes('--verbose'));
});
test('incomplete, conflicting and malformed model streams do not pass',()=>{
 const a={type:'assistant',message:{model:'claude-opus-5'}},b={type:'assistant',message:{model:'claude-sonnet-5'}},r={type:'result',result:'ok'};
 const parse=events=>parseResponse('claude',events.map(e=>JSON.stringify(e)).join('\n'),'claude-opus-5');
 assert.equal(parse([a]).status,'INVALID_OUTPUT');assert.equal(parse([a,b,r]).status,'MODEL_UNVERIFIED');assert.equal(parse([null]).status,'INVALID_OUTPUT');
 assert.equal(parse([a,{type:'result',is_error:true,result:'usage limit reached'}]).status,'QUOTA_BLOCKED');
});

test('market lower-bound and separate demand date survive R03 packet',()=>{const row=source();Object.assign(row.observations[0],{monthly_units:100,monthly_units_kind:'amazon_bought_past_month_lower_bound',demand_observed_at:'2026-09-08T00:00:00Z',retrieved_at:'2026-09-09T00:00:00Z',price_kind:'NEW'});const r=buildPacket(packet([row]),['C1'],now).candidates[0].observations[0];assert.equal(r.monthly_units_kind,'amazon_bought_past_month_lower_bound');assert.equal(r.demand_observed_at,'2026-09-08T00:00:00Z');assert.equal(r.price_kind,'NEW');});
