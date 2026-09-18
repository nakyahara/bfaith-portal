'use strict';
// 案が0件のまま公開された回を、保存済みのR01/R03応答から作り直す。
// AIもKeepaも呼ばない (保存した応答をそのまま再生する)。元の実行記録と巡回進捗は書き換えず、新しいrun IDで公開する。
const fs=require('node:fs'),path=require('node:path');
const {discover}=require('./kw-discovery.cjs');const {RunLedger}=require('./run-ledger.cjs');
const {catalogNames}=require('./kw-catalog.cjs');const {hash,requireValue:check}=require('./common.cjs');
const {write,load,collectorIdle}=require('./kw-run.cjs');const {renderHtml}=require('./kw-core.cjs');
const policy=require('./kw-policy.json');
function savedCalls(state_dir,source_run_id){
 const runs=path.join(state_dir,'runs');
 const batch=load(path.join(runs,source_run_id+'.batch-1-response.json'),null);
 const screen=load(path.join(runs,source_run_id+'.screen-1-response.json'),null);
 check(batch&&Array.isArray(batch.input_asins)&&batch.input_asins.length>0&&typeof batch.response==='string','SOURCE_BATCH_REQUIRED');
 check(screen&&typeof screen.response==='string','SOURCE_SCREEN_REQUIRED');
 // 2組以上のAI呼出がある回は、どの商品をどの組へ渡したかを再現できないので扱わない。
 check(!fs.existsSync(path.join(runs,source_run_id+'.batch-2-response.json')),'MULTI_BATCH_NOT_SUPPORTED');
 return {batch,screen};
}
async function recover(config,{collectorIdleFn=collectorIdle}={}){
 for(const key of ['source_file','own_file','state_dir'])check(typeof config[key]==='string'&&path.isAbsolute(config[key]),'CONFIG_PATH_REQUIRED');
 const source_run_id=config.source_run_id;check(/^[\w.-]{1,80}$/.test(source_run_id||''),'INVALID_SOURCE_RUN_ID');
 const run_id=config.run_id||source_run_id+'-recovered';check(/^[\w.-]{1,80}$/.test(run_id)&&run_id!==source_run_id,'INVALID_RUN_ID');
 const previous=load(path.join(config.state_dir,'editions',source_run_id+'.json'),null);
 check(previous&&Array.isArray(previous.items),'SOURCE_EDITION_REQUIRED');
 // 案が載っている回は作り直さない (同じ案を二度出さないため)。
 check(previous.items.length===0,'SOURCE_RUN_HAD_ITEMS');
 const day=previous.day;check(/^\d{4}-\d{2}-\d{2}$/.test(day),'INVALID_SOURCE_DAY');
 const {batch,screen}=savedCalls(config.state_dir,source_run_id);
 const lock=path.join(config.state_dir,'active.lock');let fd;
 try{fd=fs.openSync(lock,'wx');}catch(e){if(e.code==='EEXIST')throw Object.assign(new Error('KW_RUN_LOCKED'),{code:'KW_RUN_LOCKED'});throw e;}
 fs.writeFileSync(fd,JSON.stringify({pid:process.pid,run_id,started_at:new Date().toISOString()}));fs.closeSync(fd);
 let session;
 try{
  const output=path.join(config.state_dir,'editions',run_id+'.json');
  check(!fs.existsSync(output),'RECOVERED_EDITION_EXISTS');
  await collectorIdleFn();
  const asins=new Set(batch.input_asins);
  const rows=fs.readFileSync(config.source_file,'utf8').split(/\r?\n/).filter(Boolean).map(l=>JSON.parse(l)).filter(r=>asins.has(r.asin));
  check(rows.length===asins.size,'SOURCE_PRODUCTS_MISSING');
  const own=load(config.own_file,null);check(own&&Array.isArray(own.families),'OWN_REFERENCE_REQUIRED');
  const {ownNames,handledNames}=catalogNames(own);check(ownNames.length>0,'OWN_REFERENCE_EMPTY');
  const live=load(path.join(config.state_dir,'discovery-state.json'),{history:load(path.join(config.state_dir,'history.json'),[])});
  // 履歴だけ引き継ぎ、巡回進捗は触らない。同じ商品をもう一度材料にする回なので seen は空で始める。
  const state={history:[...(live.history||[])],scan:{cycle:1,seen_asins:[],policy_version:policy.version}};
  const judgements=load(path.join(config.state_dir,'learning-feedback.json'),{judgements:load(path.join(config.state_dir,'feedback.json'),[])}).judgements;
  const deadline=new Date(Date.now()+(config.run_minutes??80)*60000).toISOString();
  const ledger=new RunLedger(path.join(config.state_dir,'runs'));
  session=ledger.acquire({run_id,target_date:day,deadline,input_version:'kw-recovered-v1',budget_profile:'kw-screened-v3',model_plan:{R01:'replay',R03:'replay'},
   input_hash:hash({replay:source_run_id,rows:hash(rows),r01:hash(batch.response),r03:hash(screen.response)})});
  const saved={R01:{...(batch.metadata||{}),response:batch.response},R03:{...(screen.metadata||{}),response:screen.response}};
  const used={R01:0,R03:0};
  const invokeFn=async stage=>{const call=saved[stage];check(call&&used[stage]===0,'REPLAY_EXHAUSTED');used[stage]++;
   return {status:'OK',response:call.response,requested_model:call.requested_model||null,actual_model:call.actual_model||null,usage:{...(call.usage||{}),kind:'replayed'}};};
  const result=await discover({run_id,day,rows,ownNames,handledNames,judgements,state,session,
   execution:{cwd:config.cli_cwd,env:{},attestations:config.attestations||{}},
   saveState:async()=>{},
   saveStage:async(stage,value)=>write(path.join(config.state_dir,'runs',run_id+'.'+stage+'.json'),value),invokeFn});
  check(used.R01===1&&used.R03===1,'REPLAY_NOT_USED');
  write(output,result);write(path.join(config.state_dir,'recovered-'+run_id+'.html'),renderHtml(result));
  session.finish(result.status,result.stop_reason);session=null;
  return result;
 }catch(e){if(session)session.finish('failed',e.code||'RECOVER_FAILED');throw e;}
 finally{fs.unlinkSync(lock);}
}
// 公開できた案を次回の重複判定へ引き継ぐ。公開が終わってから呼ぶ。
function appendHistory(config,edition){
 const stateFile=path.join(config.state_dir,'discovery-state.json');const state=load(stateFile,null);
 if(!state||!Array.isArray(state.history))return 0;
 const known=new Set(state.history.map(i=>i.candidate_id));let added=0;
 for(const i of edition.items)if(!known.has(i.candidate_id)){
  state.history.push({candidate_id:i.candidate_id,kw:i.kw,use:i.use,category:i.category,screened:true,policy_version:policy.version});known.add(i.candidate_id);added++;}
 if(added)write(stateFile,state);
 return added;
}
module.exports={recover,appendHistory,savedCalls};
if(require.main===module){let body='';process.stdin.setEncoding('utf8');process.stdin.on('data',s=>body+=s);process.stdin.on('end',async()=>{
 let config;
 try{
  config=JSON.parse(body);
  if(config.publish===false){const edition=await recover(config);
   console.log(JSON.stringify({run_id:edition.run_id,status:edition.status,new_count:edition.new_count,published:false,items:edition.items.map(i=>i.kw)}));return;}
  let edition;
  const sent=await require('./kw-publish.cjs').publish(config,{runFn:async()=>{edition=await recover(config);return edition;}});
  const history_added=config.update_history===false?0:appendHistory(config,edition);
  console.log(JSON.stringify({...sent,published:true,history_added,items:edition.items.map(i=>i.kw)}));
 }catch(e){console.error(JSON.stringify({status:e.code||'RECOVER_FAILED',message:e.message}));process.exitCode=1;}
});}
