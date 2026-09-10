'use strict';
const path=require('node:path');const {spawnSync}=require('node:child_process');
const {run,write,load}=require('./kw-run.cjs');
const {latestJudgements}=require('./kw-learning.cjs');const {hash,requireValue:check}=require('./common.cjs');
function portalEndpoint(base='https://bfaith-portal.onrender.com'){
  const url=new URL(base);check(url.origin==='https://bfaith-portal.onrender.com'&&url.pathname==='/'&&!url.username&&!url.password&&!url.search&&!url.hash,'INVALID_PORTAL_URL');
  return url.origin+'/apps/product-scout/ingest/keywords';
}
async function publish(config,{fetchFn=fetch,runFn=run,env}={}){
  // Existing trusted helper reads the runtime secret; never log or pass it to the AI.
  env=env||require(config.keepa_module).loadEnv();check(env.MIRROR_SYNC_KEY,'SYNC_KEY_REQUIRED');
  const endpoint=portalEndpoint(env.PORTAL_URL);
  async function request(method,body,query=''){
    const response=await fetchFn(endpoint+query,{method,redirect:'error',signal:AbortSignal.timeout(20000),headers:{'x-sync-key':env.MIRROR_SYNC_KEY,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});
    check(response.ok,'PORTAL_HTTP_'+response.status);return response.json();
  }
  // A missing deployment/auth failure stops BEFORE paid or subscription inference.
  const feedbackFile=path.join(config.state_dir,'learning-feedback.json');
  const cached=load(feedbackFile,{cursor:0,judgements:[]});let cursor=cached.cursor;let events=[...cached.judgements];const until=Date.now()+120000;
  while(true){
    check(Date.now()<until,'FEEDBACK_SYNC_TIMEOUT');let page;
    try{page=await request('GET',null,'?since='+cursor);}catch(e){if(e.code==='PORTAL_HTTP_409'&&cursor>0){cursor=0;events=[];continue;}throw e;}
    check(Array.isArray(page.history),'INVALID_PORTAL_HISTORY');events.push(...page.history);
    const next=page.feedback_cursor??cursor;
    check(Number.isSafeInteger(next)&&next>=cursor&&(!page.feedback_has_more||next>cursor),'INVALID_FEEDBACK_CURSOR');cursor=next;
    if(!page.feedback_has_more)break;
  }
  const judgements=latestJudgements(events);write(feedbackFile,{cursor,judgements,synced_at:new Date().toISOString()});
  write(path.join(config.state_dir,'feedback.json'),judgements);
  const edition=await runFn(config);
  if(edition.schema_version==='kw-discovery-v2')check(edition.policy_version===require('./kw-policy.json').version&&edition.items.every(i=>i.screening?.decision==='propose'),'EDITION_REQUIRES_RESCREEN');
  const recoveries=load(path.join(config.state_dir,'pending-recovery.json'),[]);
  for(const id of recoveries){check(/^[\w.-]{1,80}$/.test(id),'INVALID_RUN_ID');const recovered=load(path.join(config.state_dir,'editions',id+'.json'),null);if(recovered?.schema_version==='kw-discovery-v2'&&(recovered.policy_version!==require('./kw-policy.json').version||!recovered.items.every(i=>i.screening?.decision==='propose')))continue;if(recovered){const receipt=await request('POST',recovered);check(receipt.body_hash===hash(recovered),'PUBLISH_HASH_MISMATCH');}}
  const sent=await request('POST',edition);
  const bodyHash=hash(edition); // common.hash is canonical, used by portal as well.
  check(sent.run_id===edition.run_id&&sent.body_hash===bodyHash,'PUBLISH_HASH_MISMATCH');
  const after=await request('GET');check(after.run_id===edition.run_id&&after.body_hash===bodyHash,'PUBLISH_READBACK_MISMATCH');
  write(path.join(config.state_dir,'last-published.json'),{run_id:edition.run_id,body_hash:bodyHash,published_at:new Date().toISOString(),status:edition.status,new_count:edition.new_count});
  write(path.join(config.state_dir,'pending-recovery.json'),[]);
  return {run_id:edition.run_id,status:edition.status,new_count:edition.new_count};
}
function ping(config,status){
  if(!config.ping_script||!path.isAbsolute(config.ping_script))return;
  spawnSync('powershell',['-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',config.ping_script,'-Id','product-kw-scout','-Status',status],{windowsHide:true,stdio:'ignore',timeout:20000});
}
module.exports={publish,portalEndpoint};
if(require.main===module){let input='';process.stdin.setEncoding('utf8');process.stdin.on('data',s=>input+=s);process.stdin.on('end',async()=>{
  let config;try{config=JSON.parse(input);const result=await publish(config);ping(config,result.status==='completed'?'ok':result.status==='failed'?'fail':'partial');console.log(JSON.stringify(result));}
  catch(e){if(config){write(path.join(config.state_dir,'last-publish-error.json'),{at:new Date().toISOString(),code:e.code||'PUBLISH_FAILED'});ping(config,'fail');}console.error(JSON.stringify({status:e.code||'PUBLISH_FAILED'}));process.exitCode=1;}
});}
