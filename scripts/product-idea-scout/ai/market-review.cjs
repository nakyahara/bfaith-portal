'use strict';
const {hash,requireValue:check}=require('./common.cjs');
function reviewPack(o,claim){
 if(!claim)return {qty:null,status:'unknown',price_per_piece:null};
 check(Number.isInteger(claim.qty)&&claim.qty>0&&typeof claim.quote==='string'&&o.title_excerpt.includes(claim.quote),'PACK_EVIDENCE_INVALID');
 check(claim.quote.normalize('NFKC').includes(String(claim.qty)+'枚'),'PACK_QUANTITY_MISMATCH');
 const source=o.source_unit_count;
 const comparable=source&&['枚','Count','count'].includes(source.unitType)&&Number.isFinite(source.unitValue);
 const external=claim.external_evidence||[];
 check(Array.isArray(external)&&external.every(e=>typeof e.model==='string'&&e.model&&o.title_excerpt.includes(e.model)&&Number.isInteger(e.qty)&&e.qty>0&&/^https:\/\//.test(e.url||'')),'EXTERNAL_PACK_EVIDENCE_INVALID');
 const conflict=(comparable&&source.unitValue!==claim.qty)||external.some(e=>e.qty!==claim.qty);
 return {qty:conflict?null:claim.qty,title_claim:claim.qty,title_quote:claim.quote,source_unit_count:source,external_evidence:external,status:conflict?'conflict':'title_claim_only',
 price_per_piece:!conflict&&Number.isFinite(o.price)?o.price/claim.qty:null,price_kind:o.price_kind,comparison_basis:'conditional_on_title_pack_claim',listing_verified:false};
}
function groupParents(rows){
 const groups=new Map();const unknown=[];const seen=new Set();
 for(const o of rows){if(seen.has(o.asin))continue;seen.add(o.asin);if(!o.parent_asin){unknown.push(o.asin);continue;}const g=groups.get(o.parent_asin)||[];g.push(o.asin);groups.set(o.parent_asin,g);}
 return {known_parent_groups:[...groups].map(([parent_asin,asins])=>({parent_asin,asins})),parent_unknown_asins:unknown,unique_asins:seen.size,
 independent_competitor_count:null,market_share:null,note:'親不明は独立商品と確定しない。同じ親の別ASINは重複記録ではなくバリエーション。販売下限の合算・市場シェア算出なし。'};
}
function buildReview(record,annotations){
 check(record.schema_version==='w05-collection-v1'&&record.status==='collected','COLLECTION_REQUIRED');
 const rows=new Map(record.observations.map(o=>[o.asin,o]));check(rows.size===record.observations.length,'DUPLICATE_ASIN');
 const all=record.results.map(s=>{const notes=annotations[s.kw];check(Array.isArray(notes)&&notes.length===s.observations.length,'REVIEW_COVERAGE_REQUIRED');
 const items=s.observations.map((entry,i)=>{const o=rows.get(entry.asin),n=notes[i];check(o&&n.asin===o.asin&&['relevant_title','adjacent','hold'].includes(n.relevance)&&n.reason,'REVIEW_BINDING_INVALID');
 return {position:entry.position,...o,review:{relevance:n.relevance,reason:n.reason,scope:n.scope,basis:'saved_title_and_unit_metadata',independent_listing_verified:false},pack_review:reviewPack(o,n.pack),original_missing_fields:[...o.missing_fields]};});
 return {kw:s.kw,search_id:s.search_id,items,groups:groupParents(items),counts:Object.fromEntries(['relevant_title','adjacent','hold'].map(k=>[k,items.filter(o=>o.review.relevance===k).length])),missing_fields:['listing_verification','pack_claim_confirmation','scope_specific_market_coverage','manufacturing_evidence','economics','own_product_overlap','R05_model_verification'],decision:'hold_before_R03'};});
 return {schema_version:'w05-market-review-v1',source_run_id:record.run_id,source_hash:hash(record),reviewer:'Codex',review_basis:'saved_observations_and_linked_manufacturer_evidence',groups:groupParents(record.observations),results:all,
 supplemental:record.observations.filter(o=>!record.results.some(s=>s.observations.some(x=>x.asin===o.asin))).map(o=>({...o,review:{relevance:'relevant_title',scope:'調理用こし布・蒸し布',reason:'タイトルに調理用途と形態の記載。検索上位の根拠には含めない。',basis:'saved_title_and_unit_metadata'},pack_review:reviewPack(o,annotations._supplemental?.[o.asin]?.pack)})),ready_for_R03:false};
}
module.exports={reviewPack,groupParents,buildReview};
