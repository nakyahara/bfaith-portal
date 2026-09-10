'use strict';
const fs=require('node:fs');const path=require('node:path');const {spawnSync}=require('node:child_process');
const {discover}=require('./kw-discovery.cjs');const {RunLedger}=require('./run-ledger.cjs');const {hash,requireValue:check}=require('./common.cjs');
const {validateEdition,renderHtml}=require('./kw-core.cjs');
function write(file,value){fs.mkdirSync(path.dirname(file),{recursive:true});const pending=file+'.writing';fs.writeFileSync(pending,typeof value==='string'?value:JSON.stringify(value,null,2)+'\n');fs.renameSync(pending,file);}
function load(file,fallback){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code==='ENOENT')return fallback;throw e;}}
function childEnvironment(){return Object.fromEntries(Object.entries(process.env).filter(([k])=>!/TOKEN|SECRET|KEY|PASSWORD|WEBHOOK/i.test(k)));}
function collectorIdle(){
  if(process.platform!=='win32')return;
  const script="$r=Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'node.exe' -and $_.CommandLine -match '(products|finder|own)\\.js' }; if($r){exit 3}";
  const r=spawnSync('powershell',['-NoProfile','-NonInteractive','-Command',script],{encoding:'utf8',windowsHide:true,timeout:20000});
  check(r.status===0,'COLLECTOR_BUSY');
}
async function run(config){
  for(const key of ['source_file','own_file','state_dir','cli_cwd','keepa_module'])check(typeof config[key]==='string'&&path.isAbsolute(config[key]),'CONFIG_PATH_REQUIRED');
  const day=config.day||new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Tokyo'}).format(new Date());
  const run_id=config.run_id||'kw-'+day;
  check(/^[\w.-]{1,80}$/.test(run_id),'INVALID_RUN_ID');
  fs.mkdirSync(config.state_dir,{recursive:true});
  const lock=path.join(config.state_dir,'active.lock');let fd;
  try{fd=fs.openSync(lock,'wx');}catch(e){if(e.code==='EEXIST')throw Object.assign(new Error('KW_RUN_LOCKED'),{code:'KW_RUN_LOCKED'});throw e;}
  fs.writeFileSync(fd,JSON.stringify({pid:process.pid,run_id,started_at:new Date().toISOString()}));fs.closeSync(fd);
  let session;
  try{
    const output=path.join(config.state_dir,'editions',run_id+'.json');
    const existing=load(output,null);if(existing){validateEdition(existing);return existing;}
    collectorIdle();
    const lines=fs.readFileSync(config.source_file,'utf8').split(/\r?\n/).filter(Boolean);const rows=lines.map(l=>JSON.parse(l));
    const own=load(config.own_file,null);check(own&&Array.isArray(own.families),'OWN_REFERENCE_REQUIRED');
    const ownNames=own.families.filter(f=>f.salesClass===1).map(f=>f.familyKey).filter(x=>typeof x==='string');
    check(ownNames.length>0,'OWN_REFERENCE_EMPTY');
    const historyFile=path.join(config.state_dir,'history.json');const stateFile=path.join(config.state_dir,'discovery-state.json');
    const state=load(stateFile,{history:load(historyFile,[]),scan:{cycle:1,seen_asins:[]}});
    // Recover validated output from an interrupted previous day without repeating AI calls.
    if(state.checkpoint?.run_id!==run_id&&state.checkpoint?.recoverable_edition){
      const recovered=state.checkpoint.recoverable_edition;const file=path.join(config.state_dir,'editions',recovered.run_id+'.json');
      if(!fs.existsSync(file)){validateEdition(recovered);write(file,recovered);const pending=load(path.join(config.state_dir,'pending-recovery.json'),[]);write(path.join(config.state_dir,'pending-recovery.json'),[...new Set([...pending,recovered.run_id])]);}
    }
    // Feedback copied from the portal is a cache, never a second decision authority.
    const feedback=load(path.join(config.state_dir,'learning-feedback.json'),{judgements:load(path.join(config.state_dir,'feedback.json'),[])}).judgements;
    const minutes=config.run_minutes??80;check(Number.isFinite(minutes)&&minutes>=12&&minutes<=80,'INVALID_RUN_MINUTES');
    const deadline=new Date(Date.now()+minutes*60000).toISOString();
    const ledger=new RunLedger(path.join(config.state_dir,'runs'));
    session=ledger.acquire({run_id,target_date:day,deadline,input_hash:hash({rows:hash(rows),history:state.history,feedback,scan:state.scan,policy:require('./kw-policy.json')}),input_version:'kw-screened-v3',budget_profile:'kw-screened-v3',model_plan:{R01:'claude-sonnet-5',R03:'claude-opus-5'}});
    const result=await discover({run_id,day,rows,ownNames,judgements:feedback,state,session,
      execution:{cwd:config.cli_cwd,env:childEnvironment(),attestations:config.attestations},
      saveState:async value=>write(stateFile,value),
      saveStage:async(stage,r)=>write(path.join(config.state_dir,'runs',run_id+'.'+stage+'.json'),r)});
    write(output,result);write(path.join(config.state_dir,'latest.json'),result);write(path.join(config.state_dir,'latest.html'),renderHtml(result));
    write(historyFile,state.history);
    session.finish(result.status,result.stop_reason);session=null;return result;
  }catch(e){if(session)session.finish('failed',e.code||'RUN_FAILED');write(path.join(config.state_dir,'last-error.json'),{run_id,at:new Date().toISOString(),code:e.code||'RUN_FAILED'});throw e;}
  finally{fs.unlinkSync(lock);}
}
module.exports={run,write,load,collectorIdle};
if(require.main===module){let body='';process.stdin.setEncoding('utf8');process.stdin.on('data',s=>body+=s);process.stdin.on('end',()=>run(JSON.parse(body)).then(r=>console.log(JSON.stringify({run_id:r.run_id,status:r.status,new_count:r.new_count,items:r.items.map(i=>({kw:i.kw,state:i.state}))}))).catch(e=>{console.error(JSON.stringify({status:e.code||'RUN_FAILED'}));process.exitCode=1;}));}
