'use strict';
const {hash,requireValue:check}=require('./common.cjs');
const policy=require('./kw-policy.json');
const ASIN=/^[A-Z0-9]{10}$/;
const normal=s=>String(s||'').normalize('NFKC').toLowerCase().replace(/[\s　]+/g,'').trim();
const keywordId=kw=>'KW-'+hash(normal(kw)).slice(0,24);
const fresh=(s,now)=>Number.isFinite(Date.parse(s))&&Date.parse(s)<=now&&now-Date.parse(s)<=7*86400000;
const safeText=(v,max=500)=>typeof v==='string'&&v.trim()&&v.length<=max;
function parseJson(response){
  const s=String(response).trim().replace(/^```(?:json)?\s*/,'').replace(/\s*```$/,'');
  try{return JSON.parse(s);}catch{throw Object.assign(new Error('AI_JSON_INVALID'),{code:'AI_JSON_INVALID'});}
}
function selectPool(rows,day,limit=70){
  // Historical titles may seed discovery; no stale price/sales is sent as current evidence.
  const unique=new Map();for(const r of rows)if(ASIN.test(r.asin)&&safeText(r.title,3000))unique.set(r.asin,r);
  const groups=new Map();for(const r of unique.values()){
    const k=r.categoryPath||String(r.rootCategory||'unknown');
    if(!groups.has(k))groups.set(k,[]);groups.get(k).push(r);
  }
  const ordered=[...groups.entries()].sort((a,b)=>hash([day,a[0]]).localeCompare(hash([day,b[0]])));
  for(const [,g]of ordered)g.sort((a,b)=>hash([day,a.asin]).localeCompare(hash([day,b.asin])));
  const result=[];for(let depth=0;result.length<limit;depth++){
    let any=false;for(const [,g]of ordered){if(g[depth]){const r=g[depth];result.push({asin:r.asin,title:r.title.slice(0,240),brand:r.brand||null,category:r.categoryPath||null,role:'discovery_seed_not_current_demand'});any=true;if(result.length===limit)break;}}
    if(!any)break;
  }return result;
}
function validateKeywords(output,pool){
  check(Array.isArray(output?.items)&&output.items.length<=3,'INVALID_KEYWORDS');
  const seen=new Set();return output.items.map(item=>{
    check(safeText(item.kw,80)&&!/[\r\n;`]|https?:|\$\(/.test(item.kw),'INVALID_KEYWORD');
    check(!/^(商品|用品|雑貨|グッズ|収納用品|キッチン用品)$/.test(item.kw),'GENERIC_KEYWORD');
    check(!seen.has(normal(item.kw)),'DUPLICATE_KEYWORD');seen.add(normal(item.kw));
    check(Array.isArray(item.seed_asins)&&item.seed_asins.length>0&&item.seed_asins.length<=5&&item.seed_asins.every(a=>pool.some(p=>p.asin===a)),'UNKNOWN_SEED_ASIN');
    for(const k of ['use','idea','reason'])check(safeText(item[k]),'INVALID_KEYWORD_DESCRIPTION');
    return {candidate_id:keywordId(item.kw),kw:item.kw.trim(),use:item.use,idea:item.idea,reason:item.reason,seed_asins:[...new Set(item.seed_asins)]};
  });
}
function assembleCandidates(keywords,market,ownNames=[],history=[],now=Date.now()){
  return keywords.map(k=>{
    const search=market.results?.find(r=>r.kw===k.kw);
    const ids=new Set(search?.observations.map(o=>o.asin)||[]);
    const evidence=(market.observations||[]).filter(o=>ids.has(o.asin)).map(o=>({...o,price:fresh(o.observed_at,now)?o.price:null,monthly_units:fresh(o.demand_observed_at,now)?o.monthly_units:null}));
    const tokens=k.kw.normalize('NFKC').split(/\s+/).filter(t=>t.length>1);
    const own_matches=ownNames.filter(n=>tokens.some(t=>normal(n).includes(normal(t)))).slice(0,8);
    const previous=history.filter(h=>h.candidate_id===k.candidate_id||normal(h.kw)===normal(k.kw));
    return {...k,evidence,search_verified:!!search,own_matches,own_lookup:'name_match_only',previous};
  });
}
function validateReviews(output,candidates){
  check(Array.isArray(output?.items)&&output.items.length===candidates.length,'REVIEW_COUNT_MISMATCH');
  const seen=new Set();return output.items.map(item=>{
    const c=candidates.find(c=>c.candidate_id===item.candidate_id);check(c&&!seen.has(item.candidate_id),'UNKNOWN_OR_DUPLICATE_CANDIDATE');seen.add(item.candidate_id);
    check(['retain','hold','exclude'].includes(item.decision),'INVALID_REVIEW_DECISION');
    check(Array.isArray(item.matched_asins)&&new Set(item.matched_asins).size===item.matched_asins.length&&item.matched_asins.every(a=>c.evidence.some(o=>o.asin===a)),'UNKNOWN_MATCHED_ASIN');
    check(Array.isArray(item.unknowns)&&item.unknowns.length<=12&&item.unknowns.every(x=>safeText(x)),'INVALID_UNKNOWNS');
    for(const k of ['match_reason','policy_reason','competition_note'])check(safeText(item[k],1000),'INVALID_REVIEW_TEXT');
    check(['none','regulated_medicine','confirmed_own_duplicate','confirmed_history_duplicate','different_use','brand_keyword'].includes(item.exclusion_code),'INVALID_EXCLUSION_CODE');
    if(item.decision==='exclude'){
      check(item.exclusion_code!=='none','EXCLUSION_WITHOUT_REASON');
      if(item.exclusion_code==='confirmed_own_duplicate')check(c.own_matches.length>0,'NO_OWN_EVIDENCE');
      if(item.exclusion_code==='confirmed_history_duplicate')check(c.previous.length>0,'NO_HISTORY_EVIDENCE');
    }
    return {...item};
  });
}
function finalize(candidates,reviews,{run_id,day,model_audit=[],warnings=[],now=new Date().toISOString()}){
  const items=candidates.map(c=>{
    const review=reviews.find(r=>r.candidate_id===c.candidate_id);check(review,'REVIEW_MISSING');
    // Missing own-product matches cannot support a claim of company capability or lineage.
    if(!c.own_matches.length&&/(?:自社|当社|弊社).*(?:近|系統|得意|実績|加工|製造)/.test(review.policy_reason)){
      review.policy_reason='用途や商品名で探す汎用品として検討する案です。自社品との関係は未確認です。';
      review.unknowns.push('AIが述べた自社品との関連は裏付けがなく、未確認として扱いました');
    }
    const matched=c.evidence.filter(o=>review.matched_asins.includes(o.asin));
    // The maximum is a single product's lower bound, NEVER summed across products/parents.
    const demand=matched.filter(o=>Number.isInteger(o.monthly_units)&&fresh(o.demand_observed_at,Date.parse(now))).sort((a,b)=>b.monthly_units-a.monthly_units)[0];
    const prior=c.previous.length>0||policy.known_examples.some(k=>normal(k)===normal(c.kw));
    const state=review.decision==='exclude'?'excluded':matched.length&&demand?.monthly_units>=policy.demand_lower_bound?'idea':'research';
    return {...c,review,edition:prior?'recheck':'new',state,search_volume:null,demand_asin:demand?.asin||null,demand_lower_bound:demand?.monthly_units??null,
      unknowns:[...new Set([...review.unknowns,'製造可否・細かな仕様・原価は未確認','実際のKW検索数は未確認','自社照合は名称による候補抽出。意味上の重複は未確認',...(state==='research'?['同用途商品の新鮮な購入観測が不足']:[])])]};
  });
  const newCount=items.filter(i=>i.state==='idea'&&i.edition==='new').length;
  return {schema_version:'kw-ideas-v1',policy_version:policy.version,run_id,day,generated_at:now,status:newCount>=2?'completed':'partial',new_count:newCount,target_count:3,
    items,warnings:[...warnings,...(newCount<2?['新規KW案が複数に達していません。根拠不足・再確認分を分けて表示しています']:[])],model_audit};
}
function validateEdition(value,now=Date.now()){
  if(value?.schema_version==='kw-discovery-v2')return require('./kw-discovery.cjs').validateDiscovery(value,now);
  check(value?.schema_version==='kw-ideas-v1'&&typeof value.policy_version==='string','INVALID_EDITION');
  check(/^[\w.-]{1,80}$/.test(value.run_id)&&/^\d{4}-\d{2}-\d{2}$/.test(value.day),'INVALID_EDITION_ID');
  check(Number.isFinite(Date.parse(value.generated_at))&&Date.parse(value.generated_at)<=now+300000,'INVALID_EDITION_DATE');
  check(['completed','partial','failed'].includes(value.status),'INVALID_EDITION_STATUS');
  check(Array.isArray(value.items)&&value.items.length<=12&&Array.isArray(value.warnings)&&value.warnings.every(t=>safeText(t,1000)),'INVALID_EDITION_ITEMS');
  const seen=new Set();for(const item of value.items){
    check(safeText(item.kw,80)&&item.candidate_id===keywordId(item.kw)&&!seen.has(item.candidate_id),'INVALID_CARD_ID');seen.add(item.candidate_id);
    check(['idea','research','excluded'].includes(item.state)&&['new','recheck'].includes(item.edition),'INVALID_CARD_STATE');
    check(safeText(item.idea)&&safeText(item.use)&&item.search_volume===null,'INVALID_CARD');
    check(Array.isArray(item.evidence)&&item.evidence.length<=20&&Array.isArray(item.unknowns)&&item.unknowns.every(t=>safeText(t)),'INVALID_CARD_EVIDENCE');
    check(Array.isArray(item.own_matches)&&item.own_matches.every(t=>safeText(t,1000))&&Array.isArray(item.previous),'INVALID_OWN_REFERENCE');
    validateReviews({items:[item.review]},[item]);
    for(const o of item.evidence){
      check(safeText(o.title_excerpt,3000),'INVALID_EVIDENCE_TITLE');
      check(ASIN.test(o.asin)&&o.url==='https://www.amazon.co.jp/dp/'+o.asin,'INVALID_EVIDENCE_URL');
      check(o.source==='Keepa Product Request','INVALID_EVIDENCE_SOURCE');
      check(o.price===null||(Number.isFinite(o.price)&&o.price>0),'INVALID_PRICE');
      check(o.monthly_units===null||(Number.isInteger(o.monthly_units)&&o.monthly_units>=0),'INVALID_DEMAND');
    }
    if(item.state==='idea'){
      const o=item.evidence.find(o=>o.asin===item.demand_asin);
      check(o&&item.review.matched_asins.includes(o.asin)&&o.monthly_units===item.demand_lower_bound&&o.monthly_units>=policy.demand_lower_bound&&fresh(o.demand_observed_at,Date.parse(value.generated_at)),'INVALID_CARD_DEMAND');
    }
  }
  check(value.new_count===value.items.filter(i=>i.state==='idea'&&i.edition==='new').length,'INVALID_NEW_COUNT');
  check(value.status!=='completed'||value.new_count>=2,'INVALID_COMPLETION');return value;
}
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function renderHtml(run){
  if(run.schema_version==='kw-discovery-v2')return require('./kw-discovery-render.cjs').render(run);
  validateEdition(run);return '<!doctype html><html lang="ja"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>検索KW案</title><style>body{font-family:system-ui;margin:24px auto;max-width:960px;padding:0 16px;background:#f5f7fa;color:#172536}article{background:white;border:1px solid #dbe2ea;border-radius:12px;padding:20px;margin:18px 0}h1{font-size:26px}h2{font-size:23px;color:#125b73}li{margin:8px 0}small{color:#536578}a{color:#126283} .warn{background:#fff4cf;padding:12px}</style><h1>検索KW案</h1><p>'+esc(run.day)+'／新規 '+run.new_count+'案</p><p>Keepaの観測から作った検討案です。価格・購入表示は競合商品の情報で、検索数や自社の売上予測ではありません。</p>'+run.warnings.map(w=>'<p class="warn">'+esc(w)+'</p>').join('')+run.items.map(i=>'<article><small>'+esc({idea:'検討案',research:'追加確認',excluded:'見送り候補'}[i.state])+'・'+esc(i.edition==='new'?'今回の候補':'再確認')+'</small><h2>'+esc(i.kw)+'</h2><p>'+esc(i.idea)+'</p><p>'+esc(i.review.policy_reason)+'</p><p>'+esc(i.review.competition_note)+'</p><ul>'+i.evidence.filter(o=>i.review.matched_asins.includes(o.asin)).slice(0,5).map(o=>'<li><a href="'+esc(o.url)+'" target="_blank" rel="noopener">'+esc(o.title_excerpt)+'</a><br>'+esc(o.price===null?'価格未確認':o.price+'円')+'／過去1か月購入表示 '+esc(o.monthly_units===null?'未確認':o.monthly_units+'以上')+'<br><small>価格観測 '+esc(o.observed_at)+'／購入観測 '+esc(o.demand_observed_at)+'</small></li>').join('')+'</ul><details><summary>未確認事項</summary><ul>'+i.unknowns.map(u=>'<li>'+esc(u)+'</li>').join('')+'</ul></details></article>').join('')+'</html>';
}
module.exports={policy,normal,keywordId,parseJson,selectPool,validateKeywords,assembleCandidates,validateReviews,finalize,validateEdition,renderHtml};
