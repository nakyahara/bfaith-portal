'use strict';
// W05: existing collector JSONL only. No HTTP, no refresh, no source-file mutation.
const fs = require('node:fs');
const { parseProducts } = require('../quality.cjs');
const { hash, requireValue: check, dateMs } = require('./common.cjs');
const DAY = 86400000;
function normal(value) { return String(value || '').normalize('NFKC').toLowerCase().replace(/[\s　]+/gu, ' ').trim(); }
function freshness(row, now, maxAgeDays) {
  const date = dateMs(row.observedAt);
  return !Number.isFinite(date) ? 'missing' : date > now ? 'future' : now-date > maxAgeDays*DAY ? 'stale' : 'fresh';
}
function createMarketIndex(jsonl, {now = Date.now(), maxAgeDays = 7} = {}) {
  check(Number.isFinite(now) && Number.isFinite(maxAgeDays) && maxAgeDays>0, 'INVALID_FRESHNESS_POLICY');
  const rows = parseProducts(jsonl);
  check(rows.every(r=>/^[A-Z0-9]{10}$/.test(r.asin)), 'INVALID_ASIN');
  const summary = {source:'existing-collector-jsonl',unique_asins:rows.length, fresh:0, stale:0, missing:0, future:0, fresh_with_demand:0, fresh_with_price:0, fresh_parent_unknown:0};
  for(const row of rows) {
    const state=freshness(row,now,maxAgeDays);summary[state]++;
    if(state==='fresh') {
      if(Number.isFinite(row.monthlySold) && row.monthlySold>=0)summary.fresh_with_demand++;
      if([row.priceBuyBox,row.priceNew].some(p=>Number.isFinite(p) && p>0))summary.fresh_with_price++;
      if(!/^[A-Z0-9]{10}$/.test(row.parentAsin || ''))summary.fresh_parent_unknown++;
    }
  }
  return {rows,summary,now,maxAgeDays,input_hash:hash(rows)};
}
function findMarketEvidence(index, {candidate_id,search_terms,limit=20,price_policy=null}) {
  check(typeof candidate_id==='string' && candidate_id.trim(), 'CANDIDATE_ID_REQUIRED');
  check(Array.isArray(search_terms) && search_terms.length>0 && search_terms.length<=12 && search_terms.every(t=>typeof t==='string' && normal(t)), 'INVALID_SEARCH_TERMS');
  check(Number.isInteger(limit) && limit>=1 && limit<=100,'INVALID_RESULT_LIMIT');
  if(price_policy)check(price_policy.currency==='JPY' && Number.isFinite(price_policy.divisor) && price_policy.divisor>0 && typeof price_policy.evidence_id==='string' && price_policy.evidence_id.trim(), 'PRICE_POLICY_UNVERIFIED');
  const termSets=search_terms.map(s=>normal(s).split(' '));
  // Literal title matching is a retrieval hint, never keyword rank or semantic equivalence.
  const matched=index.rows.filter(row=>termSets.some(terms=>terms.every(term=>normal(row.title).includes(term))));
  const fresh=matched.filter(row=>freshness(row,index.now,index.maxAgeDays)==='fresh')
    .sort((a,b)=>dateMs(b.observedAt)-dateMs(a.observedAt)||a.asin.localeCompare(b.asin));
  const observations=fresh.slice(0,limit).map(row=>{
    const rawPrice=[row.priceBuyBox,row.priceNew].find(v=>Number.isFinite(v) && v>0)??null;
    return {observation_id:'OB-'+hash({asin:row.asin,observedAt:row.observedAt,row}).slice(0,24),asin:row.asin,
      parent_asin:/^[A-Z0-9]{10}$/.test(row.parentAsin || '')?row.parentAsin:null,
      source:'Keepa (existing collector)',observed_at:row.observedAt,
      price:rawPrice!==null && price_policy?rawPrice/price_policy.divisor:null,
      price_basis:price_policy?{...price_policy}:null,
      monthly_units:Number.isFinite(row.monthlySold) && row.monthlySold>=0?row.monthlySold:null,
      brand:typeof row.brand==='string'?row.brand:null,origin_country:null,
      title_excerpt:String(row.title||'').slice(0,1200),url:'https://www.amazon.co.jp/dp/'+row.asin};
  });
  const missing_fields=['KW検索未接続','用途・形態一致未検証'];
  if(!price_policy)missing_fields.push('価格単位未確認');
  if(!observations.length)missing_fields.push('鮮度条件内の競合候補なし');
  if(observations.some(o=>o.monthly_units===null))missing_fields.push('購入観測欠測');
  if(observations.some(o=>o.price===null))missing_fields.push('価格欠測');
  return {candidate_id,search_terms:[...search_terms],retrieval_kind:'local_title_match',semantic_match:'unverified',
    search_market_verified:false,source_version:index.input_hash,
    coverage:{title_matches:matched.length,fresh_matches:fresh.length,returned:observations.length,truncated:fresh.length>limit,excluded_for_freshness:matched.length-fresh.length},
    observations,missing_fields};
}
module.exports={createMarketIndex,findMarketEvidence};
if(require.main===module) {
  let body='';process.stdin.setEncoding('utf8');process.stdin.on('data',part=>body+=part);
  process.stdin.on('end',()=>{try{
    const input=JSON.parse(body);check(typeof input.source_file==='string','SOURCE_FILE_REQUIRED');
    const index=createMarketIndex(fs.readFileSync(input.source_file,'utf8'),{now:input.now?dateMs(input.now):Date.now()});
    const results=(input.candidates||[]).map(c=>findMarketEvidence(index,c));
    // Summary by default; full observations require explicit local-only extraction.
    console.log(JSON.stringify({summary:index.summary,input_hash:index.input_hash,results:input.include_observations===true?results:results.map(({observations,...r})=>r)}));
  }catch(e){console.error(JSON.stringify({status:e.code||'MARKET_INPUT_INVALID'}));process.exitCode=1;}});
}
