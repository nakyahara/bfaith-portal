'use strict';
const {hash,requireValue:check}=require('./common.cjs');
const {executeSearchPlan}=require('./r02-search.cjs');
const ASIN=/^[A-Z0-9]{10}$/;
const SOURCE='https://keepa.com/api-docs/product-object.html';
function keepaDate(n){const ms=(n+21564000)*60000;return Number.isInteger(n)&&n>0&&Number.isFinite(ms)&&ms<=8640000000000000?new Date(ms).toISOString():null;}
function fresh(date,now){const n=Date.parse(date);return Number.isFinite(n)&&n<=now&&now-n<=7*86400000;}
function observation(p,retrieved_at){
 check(p&&ASIN.test(p.asin)&&p.domainId===5,'INVALID_PRODUCT');
 const now=Date.parse(retrieved_at);check(Number.isFinite(now),'INVALID_RETRIEVAL_DATE');
 const observed_at=keepaDate(p.lastUpdate),demand_observed_at=keepaDate(p.lastSoldUpdate),cur=p.stats?.current||[];
 const available=p.productType===0;
 const positive=n=>Number.isFinite(n)&&n>0;
 const price_kind=positive(cur[18])?'BUY_BOX_SHIPPING':positive(cur[1])?'NEW':null;
 const value=price_kind==='BUY_BOX_SHIPPING'?cur[18]:price_kind==='NEW'?cur[1]:null;
 const units=Number.isInteger(p.monthlySold)&&p.monthlySold>=0?p.monthlySold:null;
 const missing=[];
 if(!available)missing.push('product_type_unavailable');
 if(!fresh(observed_at,now))missing.push('product_freshness');
 if(!fresh(demand_observed_at,now))missing.push('demand_freshness');
 if(value===null)missing.push('price');if(units===null)missing.push('monthly_units');
 return {observation_id:'OB-'+hash({asin:p.asin,retrieved_at,lastUpdate:p.lastUpdate}).slice(0,24),asin:p.asin,parent_asin:ASIN.test(p.parentAsin||'')?p.parentAsin:null,
 source:'Keepa Product Request',observed_at,retrieved_at,product_type:p.productType,
 price:available&&fresh(observed_at,now)?value:null,price_kind,price_basis:{currency:'JPY',divisor:1,evidence_id:SOURCE},
 monthly_units:available&&fresh(demand_observed_at,now)?units:null,monthly_units_kind:'amazon_bought_past_month_lower_bound',demand_observed_at,
 brand:typeof p.brand==='string'?p.brand:null,title_excerpt:String(p.title||'').slice(0,1200),url:'https://www.amazon.co.jp/dp/'+p.asin,
 source_unit_count:p.unitCount?{unitValue:p.unitCount.unitValue??null,unitType:p.unitCount.unitType??null}:null,pack_qty:null,
 missing_fields:[...missing,'semantic_match_unverified','pack_qty_unverified']};
}
function collectionPlan({run_id,source_reference,keywords,refresh_asins=[],source_stage='representative_example_connection_test'}){
 check(typeof run_id==='string'&&run_id&&typeof source_reference==='string'&&source_reference,'SOURCE_REQUIRED');
 check(Array.isArray(keywords)&&keywords.length>0&&keywords.length<=3,'SEARCH_LIMIT');
 check(keywords.every(k=>typeof k==='string'&&k.trim()&&k.length<=120&&!/[\r\n;`]/.test(k))&&new Set(keywords).size===keywords.length,'INVALID_KEYWORDS');
 check(Array.isArray(refresh_asins)&&refresh_asins.length<=6&&refresh_asins.every(a=>ASIN.test(a)),'INVALID_REFRESH_ASINS');
 check(['R01','representative_example_connection_test'].includes(source_stage),'INVALID_SOURCE_STAGE');
 const entries=keywords.map((kw,i)=>({search_id:'KS-'+hash([run_id,kw]).slice(0,16),kw,from_seed_id:(source_stage==='R01'?'R01-':'MANUAL-')+(i+1)}));
 return {schema_version:'w05-collection-plan-v1',run_id,source_reference,refresh_asins:[...new Set(refresh_asins)],max_tokens:keywords.length*30+new Set(refresh_asins).size,
 search_plan:{schema_version:'r02-keyword-search-plan-v1',run_id,source_stage,max_tokens:keywords.length*10,entries}};
}
async function collect(plan,{keepaCall,save,assertIdle=async()=>{},now=()=>new Date().toISOString()}={}){
 check(typeof keepaCall==='function'&&typeof save==='function','COLLECTION_ADAPTER_REQUIRED');
 const validated=collectionPlan({run_id:plan?.run_id,source_reference:plan?.source_reference,keywords:plan?.search_plan?.entries?.map(e=>e.kw),refresh_asins:plan?.refresh_asins,source_stage:plan?.search_plan?.source_stage});
 check(hash(validated)===hash(plan),'INVALID_COLLECTION_PLAN');
 const record={schema_version:'w05-collection-v1',run_id:plan.run_id,plan,status:'running',started_at:now(),calls:[],search:null,observations:[],missing_asins:[]};
 let reserved=0;await save(record);
 const call=async(endpoint,params)=>{
   await assertIdle();const cost=endpoint==='/search'?10:params.asin.split(',').length;
   check(reserved+cost<=plan.max_tokens,'TOKEN_BUDGET_EXCEEDED');reserved+=cost;
   const entry={endpoint,params,reserved_tokens:cost,status:'reserved',started_at:now()};record.calls.push(entry);await save(record);
   // <60 seconds: the existing helper cannot start its 60-second retry wait.
   const j=await keepaCall(endpoint,{...params,domain:5},null,{deadlineMs:Date.now()+45000});
   check(j&&!j.error,'KEEPA_RESPONSE_ERROR');
   if(endpoint==='/search')check(Array.isArray(j.asinList)&&j.asinList.length<=20&&j.asinList.every(a=>ASIN.test(a)),'INVALID_SEARCH_RESPONSE');
   else check(Array.isArray(j.products),'INVALID_PRODUCT_RESPONSE');
   entry.status='received';entry.completed_at=now();entry.tokens_consumed=Number.isFinite(j.tokensConsumed)?j.tokensConsumed:null;entry.tokens_left=Number.isFinite(j.tokensLeft)?j.tokensLeft:null;
   if(endpoint==='/search')entry.asins=[...j.asinList];await save(record);return j;
 };
 try{
   record.search=await executeSearchPlan(plan.search_plan,{keepaCall:call,now});await save(record);
   const asins=[...new Set([...record.search.asins,...plan.refresh_asins])];
   if(asins.length){const j=await call('/product',{asin:asins.join(','),stats:30,history:0,update:1});const stamp=now();const seen=new Set();
     record.observations=j.products.map(p=>{check(asins.includes(p.asin)&&!seen.has(p.asin),'UNEXPECTED_PRODUCT');seen.add(p.asin);return observation(p,stamp);});
     record.missing_asins=asins.filter(a=>!seen.has(a));
   }
   record.results=record.search.results.map(r=>({search_id:r.search_id,kw:r.kw,observed_at:r.observed_at,source:r.source,coverage:'first_up_to_20_non_sponsored_results',market_complete:false,semantic_match:'unverified',
     observations:r.asins.map((asin,index)=>({asin,position:index+1,observation_id:record.observations.find(o=>o.asin===asin)?.observation_id??null}))}));
   record.status=record.missing_asins.length?'partial':'collected';record.completed_at=now();await save(record);return record;
 }catch(e){record.status='failed';record.error_code=e.code||'COLLECTION_FAILED';await save(record);throw e;}
}
module.exports={keepaDate,observation,collectionPlan,collect};
