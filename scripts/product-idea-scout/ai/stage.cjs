'use strict';
const {requireValue:check,dateMs,hash}=require('./common.cjs');
const {buildPacket,cacheKey}=require('./packet.cjs');
const {validateOutput}=require('./validate.cjs');
const {invoke,ROUTING}=require('./cli.cjs');
async function evaluateR03(input,{candidate_ids,versions,prompt,policy={},cache,now=new Date().toISOString(),...execution}) {
  check(typeof prompt==='string' && prompt.trim(),'PROMPT_REQUIRED');
  check(versions.model_id===ROUTING.R03.model && versions.effort===ROUTING.R03.effort && versions.rule_version===input.rule_version,'ROUTING_VERSION_MISMATCH');
  const packet=buildPacket(input,candidate_ids,now);
  const key=cacheKey(packet,{...versions,prompt_hash:hash(prompt),policy_hash:hash(policy)});
  const cached=cache?.get(key,Date.parse(now));
  if(cached) {
    const output={...cached,run_id:packet.run_id};
    const validation=validateOutput(packet,output,policy);
    if(validation.valid)return {status:'OK',cache_hit:true,output,validation};
  }
  const result=await invoke('R03',prompt+'\n\n<untrusted_packet>\n'+JSON.stringify(packet)+'\n</untrusted_packet>',execution);
  if(result.status!=='OK')return result;
  const validation=validateOutput(packet,result.response,policy);
  if(!validation.valid)return {...result,status:'VALIDATION_FAILED',validation};
  const output=JSON.parse(result.response);
  // Recheck minimum evidence lifetime. Missing dates/expiry never yield a persistent hit.
  const deadlines=packet.candidates.flatMap(c=>[
    ...c.observations.map(o=>dateMs(o.observed_at)+7*86400000),
    ...c.capability_refs.map(e=>dateMs(e.expires_at)),
    c.lookup_evidence?.status==='complete'?dateMs(c.lookup_evidence.checked_at)+48*3600000:NaN
  ]);
  if(cache && deadlines.length && deadlines.every(Number.isFinite) && Math.min(...deadlines)>Date.parse(now)) cache.put(key,output,new Date(Math.min(...deadlines)).toISOString(),Date.parse(now));
  return {...result,cache_hit:false,output,validation};
}
module.exports={evaluateR03};
