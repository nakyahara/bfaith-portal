'use strict';
const path=require('node:path');const {preflight}=require('./cli.cjs');const {write}=require('./kw-run.cjs');const {portalEndpoint}=require('./kw-publish.cjs');
async function probe(config){
  const result={checked_at:new Date().toISOString(),providers:{}};
  for(const provider of ['claude','codex'])result.providers[provider]=(await preflight(provider,{cwd:config.cli_cwd})).status;
  result.ok=Object.values(result.providers).every(s=>s==='READY_FOR_BILLING_CHECK');
  if(config.check_portal){try{const env=require(config.keepa_module).loadEnv();const r=await fetch(portalEndpoint(env.PORTAL_URL),{redirect:'error',headers:{'x-sync-key':env.MIRROR_SYNC_KEY},signal:AbortSignal.timeout(20000)});result.portal=r.status;result.ok=result.ok&&r.ok;}catch{result.portal='UNREACHABLE';result.ok=false;}}
  write(path.join(config.state_dir,'task-preflight.json'),result);return result;
}
module.exports={probe};
if(require.main===module){let input='';process.stdin.setEncoding('utf8');process.stdin.on('data',s=>input+=s);process.stdin.on('end',()=>probe(JSON.parse(input)).then(r=>{console.log(JSON.stringify(r));process.exitCode=r.ok?0:1;}).catch(()=>{console.error('PREFLIGHT_FAILED');process.exitCode=1;}));}
