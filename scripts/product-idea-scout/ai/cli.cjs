'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { requireValue: check } = require('./common.cjs');
const { usageRecord } = require('./packet.cjs');
const ROUTING = Object.freeze({R01:{provider:'claude',model:'claude-sonnet-5',effort:'low',calls:3},R03:{provider:'claude',model:'claude-opus-5',effort:'medium',calls:2},R05:{provider:'codex',model:'gpt-5.6-terra',effort:'medium',calls:1},R06:{provider:'claude',model:'claude-opus-5',effort:'medium',calls:1}});
const blockedEnv = /^(?:ANTHROPIC_(?:API_KEY|AUTH_TOKEN|BASE_URL|CUSTOM_HEADERS|DEFAULT_.*MODEL)|OPENAI_(?:API_KEY|BASE_URL|API_BASE)|CODEX_API_KEY|CLAUDE_CODE_(?:USE_.*|OAUTH_TOKEN|API_KEY_HELPER)|.*(?:GATEWAY|PROXY).*)$/i;
function billingEnvironment(env) { return Object.keys(env).filter(k=>env[k] && blockedEnv.test(k)); }
function configHazards(value, prefix='') {
  const found=[];
  if (!value || typeof value !== 'object') return found;
  for (const [key,v] of Object.entries(value)) {
    const p=prefix?prefix+'.'+key:key;
    if (/api.?key.?helper|base.?url|gateway|model.?provider|fallback.?model|auth.?token/i.test(key) && v) found.push(p);
    if (key === 'env' && v && typeof v === 'object') found.push(...billingEnvironment(v).map(k=>p+'.'+k));
    else if (v && typeof v === 'object') found.push(...configHazards(v,p));
  }
  return found;
}
function classifyError(value) {
  if (/usage.limit|rate.limit|quota|limit.reached|resets? at|try again after|429/i.test(value)) return 'QUOTA_BLOCKED';
  if (/not logged|log.?in required|authentication|unauthorized|token.*expired|401/i.test(value)) return 'AUTH_REQUIRED';
  if (/model.*(?:not found|unavailable|not supported|not exist|access)|invalid.model|unknown.model/i.test(value)) return 'MODEL_UNAVAILABLE';
  return 'CLI_FAILED';
}
function commandFor(provider, env=process.env) {
  const npm = env.APPDATA && path.join(env.APPDATA,'npm','node_modules');
  check(npm, 'CLI_PATH_REQUIRED');
  if(provider==='claude') { const native=path.join(npm,'@anthropic-ai','claude-code','bin','claude.exe'); if(fs.existsSync(native)) return {file:native,prefix:[]}; }
  const entry=provider==='claude'?path.join(npm,'@anthropic-ai','claude-code','cli.js'):path.join(npm,'@openai','codex','bin','codex.js');
  check(fs.existsSync(entry),'CLI_NOT_FOUND');
  return {file:process.execPath,prefix:[entry]};
}
function runProcess(command,args,{cwd,env,stdin='',timeoutMs=600000,maxBytes=2*1024*1024}={}) {
  return new Promise(resolve=>{
    let stdout='',stderr='',size=0,stopped=null,settled=false;
    const child=spawn(command.file,[...(command.prefix||[]),...args],{cwd,env,shell:false,windowsHide:true,stdio:['pipe','pipe','pipe']});
    function stop(reason) {
      if(stopped) return; stopped=reason;
      // Stop only the process tree started by this call, including the npm Codex native child.
      if(process.platform==='win32' && child.pid) {
        const killer=spawn('taskkill.exe',['/PID',String(child.pid),'/T','/F'],{windowsHide:true,stdio:'ignore'});
        killer.on('error',()=>child.kill());
      } else child.kill('SIGKILL');
    }
    const timer=setTimeout(()=>stop('TIMEOUT'),timeoutMs);
    const done=code=>{if(settled)return;settled=true;clearTimeout(timer);resolve({code,stdout,stderr,stopped});};
    for(const [name,stream] of [['stdout',child.stdout],['stderr',child.stderr]]) stream.on('data',b=>{size+=b.length;if(size>maxBytes){stop('OUTPUT_LIMIT');return;}if(name==='stdout')stdout+=b;else stderr+=b;});
    child.on('error',()=>{stopped='CLI_NOT_FOUND';done(-1);});child.on('close',done);
    child.stdin.on('error',()=>{}); child.stdin.end(stdin);
  });
}
async function preflight(provider, options={}) {
  check(['claude','codex'].includes(provider),'INVALID_PROVIDER');
  const env=options.env||process.env;
  const blocked=billingEnvironment(env);
  if(blocked.length) return {status:'BILLING_MODE_MISMATCH',blocked_names:blocked};
  // Only trusted, non-secret settings files; never .env, auth.json or credential stores.
  if(provider==='claude') {
    const settings=path.join(env.ProgramFiles||'C:/Program Files','ClaudeCode','managed-settings.json');
    if(fs.existsSync(settings)) {
      let cfg;try{cfg=JSON.parse(fs.readFileSync(settings,'utf8'));}catch{return {status:'CONFIG_UNVERIFIED'};}
      const hazards=configHazards(cfg);if(hazards.length)return {status:'BILLING_MODE_MISMATCH',blocked_names:hazards};
    }
  }
  let command;try{command=options.command||commandFor(provider,env);}catch(e){return {status:e.code};}
  const execute=options.execute||runProcess;
  const context={cwd:options.cwd,env,timeoutMs:15000};
  const version=await execute(command,['--version'],context);
  if(version.code!==0)return {status:version.stopped||'CLI_FAILED'};
  const auth=await execute(command,provider==='claude'?['auth','status']:['login','status'],context);
  let subscription=false;
  if(provider==='claude') {
    try {const a=JSON.parse(auth.stdout);subscription=a.loggedIn===true && a.authMethod==='claude.ai' && ['max','pro','team','enterprise'].includes(a.subscriptionType);}catch{}
  } else subscription=/Logged in using ChatGPT/i.test(auth.stdout+' '+auth.stderr);
  const status=auth.code===0 && subscription?'READY_FOR_BILLING_CHECK':'AUTH_REQUIRED';
  return {status,provider,cli_version:(version.stdout||version.stderr).trim(),subscription_authenticated:subscription};
}
function invocationArgs(stage) {
  const r=ROUTING[stage];check(r,'INVALID_STAGE');
  if(r.provider==='claude')return ['-p','--model',r.model,'--effort',r.effort,'--output-format','stream-json','--verbose','--no-session-persistence','--safe-mode','--restricted','--strict-mcp-config','--tools','','--disable-slash-commands','--no-chrome'];
  return ['exec','--ignore-user-config','--ephemeral','--skip-git-repo-check','--sandbox','read-only','--json','--model',r.model,
    '-c','forced_login_method="chatgpt"','-c','model_provider="openai"','-c','model_reasoning_effort="'+r.effort+'"',
    '-c','features.shell_tool=false','-c','features.multi_agent=false','-c','features.apps=false','-c','web_search="disabled"','-'];
}
function parseResponse(provider, raw, requestedModel, input='') {
  let events;
  try {
    try { events=[JSON.parse(raw)]; }
    catch { events=raw.split(/\r?\n/).filter(l=>l.trim()).map(l=>JSON.parse(l)); }
    if(!events.length || events.some(e=>!e || typeof e!=='object' || Array.isArray(e)))throw new Error();
  } catch {return {status:'INVALID_OUTPUT',actual_model:'unknown'};}
  const errors=events.filter(e=>e.is_error||e.type==='error'||e.type==='turn.failed');
  if(errors.length)return {status:classifyError(JSON.stringify(errors)),actual_model:'unknown'};
  let response='',usage=null,completed=false;
  const models=new Set(),usageModels=new Set();
  for(const e of events) {
    if(provider==='claude') {
      if(e.type==='result' || (events.length===1 && typeof e.result==='string')) {
        response=typeof e.result==='string'?e.result:'';usage=e.usage;completed=true;
        for(const m of Object.keys(e.modelUsage||{}))usageModels.add(m);
      }
      // Main assistant protocol metadata only. Init model is configuration;
      // modelUsage also includes auxiliary calls (observed: Haiku).
      if(e.type==='assistant' && e.parent_tool_use_id==null && typeof e.message?.model==='string' && e.message.model)models.add(e.message.model);
    } else {
      if(e.type==='item.completed' && e.item?.type==='agent_message')response+=e.item.text||'';
      if(e.type==='turn.completed'){usage=e.usage;completed=true;}
      // codex exec --json 0.150.1 exposes no response model metadata.
    }
  }
  const actual=models.size===1?[...models][0]:'unknown';
  return {status:!completed||!response?'INVALID_OUTPUT':actual==='unknown'?'MODEL_UNVERIFIED':actual!==requestedModel?'MODEL_MISMATCH':'OK',actual_model:actual,model_evidence:actual==='unknown'?null:'assistant.message.model',usage_models:[...usageModels].sort(),response,usage:usageRecord(usage,input,response)};
}

async function invoke(stage,prompt,options={}) {
  const route=ROUTING[stage];if(!route)return {status:'MODEL_UNAVAILABLE'};
  const ready=await preflight(route.provider,options);
  if(ready.status!=='READY_FOR_BILLING_CHECK')return ready;
  const attestation=options.billing_attestation;
  // A user's confirmed setting persists until revoked. Authentication and billing
  // environment are still checked on EVERY call; elapsed time is not a setting change.
  if(!attestation || attestation.provider!==route.provider || attestation.additional_usage_disabled!==true || attestation.revoked===true || !attestation.checked_by || !Number.isFinite(Date.parse(attestation.checked_at)) || Date.parse(attestation.checked_at)>Date.now())return {...ready,status:'BILLING_UNVERIFIED'};
  if(!options.cwd || !path.isAbsolute(options.cwd))return {status:'WORKDIR_REQUIRED'};
  if(!options.budget || typeof options.save_budget!=='function')return {status:'BUDGET_REQUIRED'};
  let reservation;
  try { reservation=options.budget.reserve(stage,{retry:options.retry===true}); await options.save_budget(options.budget.snapshot()); } catch(e) {return {status:e.code||'BUDGET_SAVE_FAILED'};}
  const execute=options.execute||runProcess;
  const result=await execute(options.command||commandFor(route.provider,options.env||process.env),invocationArgs(stage),{cwd:options.cwd,env:options.env||process.env,stdin:prompt,timeoutMs:600000});
  const completed = result.stopped || result.code!==0 ? {...ready,status:result.stopped||classifyError(result.stderr+' '+result.stdout),requested_model:route.model} : {...ready,...parseResponse(route.provider,result.stdout,route.model,prompt),requested_model:route.model,effort:route.effort};
  options.budget.finish(reservation.id,completed);
  try {await options.save_budget(options.budget.snapshot());}catch{return {...completed,status:'BUDGET_SAVE_FAILED'};}
  return completed;
}
module.exports={ROUTING,billingEnvironment,configHazards,classifyError,commandFor,runProcess,preflight,invocationArgs,parseResponse,invoke};
if(require.main===module) {
  // Read only probe: invoking a model is deliberately not available through an ambient CLI flag.
  (async()=>{for(const provider of ['claude','codex'])console.log(JSON.stringify(await preflight(provider,{cwd:__dirname})));})().catch(e=>{console.error(JSON.stringify({status:e.code||'PROBE_FAILED'}));process.exitCode=1;});
}
