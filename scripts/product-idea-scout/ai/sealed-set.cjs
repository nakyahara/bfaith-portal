'use strict';
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const {hash,canonical,requireValue:check,dateMs}=require('./common.cjs');
function threeMonthsBefore(value) {
  const d=new Date(value); const day=d.getUTCDate();d.setUTCDate(1);d.setUTCMonth(d.getUTCMonth()-3);
  const end=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth()+1,0)).getUTCDate();d.setUTCDate(Math.min(day,end));return d.toISOString().slice(0,10);
}
function sample(products,{seed,count=6,low_price_max,minimum_launch='2024-01-01'}={}) {
  check(typeof seed==='string' && seed.length,'SEED_REQUIRED');
  check(Number.isInteger(count) && count>0,'INVALID_COUNT');
  check(Number.isFinite(low_price_max) && low_price_max>0,'LOW_PRICE_DEFINITION_REQUIRED');
  check(new Set(products.map(p=>p.product_id)).size===products.length,'DUPLICATE_PRODUCT');
  // Missing labels cannot be repaired from product-name keywords: that would make the held-out test unverifiable.
  for(const p of products) {
    check(['product_id','family_id','use','target','material','form','spec'].every(k=>typeof p[k]==='string' && p[k].trim()),'METADATA_REQUIRED');
    check(p.sales_class===1,'WRONG_SALES_CLASS');
    check(Number.isFinite(p.price) && p.price>0,'PRICE_REQUIRED');
  }
  const families=new Map();
  for(const p of products) {if(!families.has(p.family_id))families.set(p.family_id,[]);families.get(p.family_id).push(p);}
  const eligible=[];
  for(const members of families.values()) {
    if(members.some(p=>!Number.isFinite(dateMs(p.launched_on))))continue;
    const first=[...members].sort((a,b)=>a.launched_on.localeCompare(b.launched_on)||a.product_id.localeCompare(b.product_id))[0];
    if(first.launched_on>=minimum_launch)eligible.push(first);
  }
  const order=(a,b)=>hash([seed,a.product_id]).localeCompare(hash([seed,b.product_id]));
  const pool=eligible.sort(order),chosen=[];
  const low=pool.find(p=>p.price<=low_price_max);check(low,'LOW_PRICE_STRATUM_EMPTY');chosen.push(low);
  while(chosen.length<count) {
    const remaining=pool.filter(p=>!chosen.includes(p));check(remaining.length,'INSUFFICIENT_ELIGIBLE_FAMILIES');
    const usedMaterial=new Set(chosen.map(p=>p.material)),usedForm=new Set(chosen.map(p=>p.form));
    const novelty=p=>Number(!usedMaterial.has(p.material))+Number(!usedForm.has(p.form));
    remaining.sort((a,b)=>novelty(b)-novelty(a)||order(a,b));chosen.push(remaining[0]);
  }
  if(count>1)check(new Set(chosen.map(p=>p.material)).size>1 && new Set(chosen.map(p=>p.form)).size>1,'INSUFFICIENT_STRATA');
  return chosen;
}
function buildSealedSet(input) {
  const selected=sample(input.products,input.selection);
  check(Array.isArray(input.records),'REFERENCE_CATALOG_REQUIRED');
  for(const r of input.records) {
    check(typeof r.record_id==='string' && Array.isArray(r.product_ids) && typeof r.use==='string' && typeof r.target==='string','REFERENCE_MAPPING_REQUIRED');
    check(r.product_ids.every(id=>input.products.some(p=>p.product_id===id)),'UNKNOWN_REFERENCE_PRODUCT');
  }
  const cases=selected.map((answer,index)=>{
    const cutoff=threeMonthsBefore(answer.launched_on);
    // Exclude all selected answers from every case, not just the current answer.
    const excluded=new Set(input.products.filter(p=>selected.some(a=>p.family_id===a.family_id || (p.use===a.use && p.target===a.target))).map(p=>p.product_id));
    const products=input.products.filter(p=>!excluded.has(p.product_id) && (!p.effective_at || dateMs(p.effective_at)<=dateMs(cutoff)));
    const records=input.records.filter(r=>!r.product_ids.some(id=>excluded.has(id)) && !selected.some(a=>r.use===a.use && r.target===a.target) && (!r.effective_at || dateMs(r.effective_at)<=dateMs(cutoff)));
    // No original product metadata or answer IDs are included in the public manifest.
    return {case_id:'R'+String(index+1).padStart(2,'0'),cutoff,
      mode:'current-input-target-exclusion',model_training_leakage:'not_excluded',
      historical_snapshot_verified:false,products,records};
  });
  const plaintext=Buffer.from(canonical({selection:input.selection,answers:selected,cases:cases.map(c=>({case_id:c.case_id,cutoff:c.cutoff}))}));
  const publicKey=crypto.createPublicKey(input.public_key_pem);check(publicKey.asymmetricKeyType==='rsa','RSA_KEY_REQUIRED');
  check((publicKey.asymmetricKeyDetails?.modulusLength||0)>=2048,'RSA_KEY_TOO_SMALL');
  const key=crypto.randomBytes(32),iv=crypto.randomBytes(12);
  const cipher=crypto.createCipheriv('aes-256-gcm',key,iv);
  const encrypted=Buffer.concat([cipher.update(plaintext),cipher.final()]);
  const sealed={version:1,algorithm:'RSA-OAEP-SHA256+AES-256-GCM',key:crypto.publicEncrypt({key:publicKey,oaepHash:'sha256',padding:crypto.constants.RSA_PKCS1_OAEP_PADDING},key).toString('base64'),iv:iv.toString('base64'),tag:cipher.getAuthTag().toString('base64'),ciphertext:encrypted.toString('base64')};
  key.fill(0);plaintext.fill(0);
  return {cases,sealed,manifest:{version:1,count:cases.length,selection:input.selection,input_hash:hash({products:input.products,records:input.records}),cases_hash:hash(cases),seal_hash:hash(sealed),status:'sealed-not-scored'}};
}
function writeSealedSet(input) {
  const inside=(root,p)=>{const rel=path.relative(root,p);return rel==='' || (!rel.startsWith('..'+path.sep) && rel!=='..' && !path.isAbsolute(rel));};
  check(input.w04_directory && input.answer_directory && input.input_directory,'OUTPUT_PATHS_REQUIRED');
  const root=fs.realpathSync(input.w04_directory);
  // Existing parent directories only: resolve symlinks/junctions before writing anything.
  const answers=fs.realpathSync(input.answer_directory),inputs=fs.realpathSync(input.input_directory);
  check(!inside(root,answers),'ANSWER_INSIDE_W04');
  check(answers!==inputs,'ANSWER_INPUT_OVERLAP');
  const bundle=buildSealedSet(input);
  for(const [dir,name,value] of [[answers,'answers.sealed.json',bundle.sealed],[inputs,'cases.json',bundle.cases],[inputs,'manifest.json',bundle.manifest]]) {
    check(!fs.existsSync(path.join(dir,name)),'OUTPUT_ALREADY_EXISTS');
  }
  // Manifest last: an interrupted write is never reported as a completed evaluation set.
  fs.writeFileSync(path.join(answers,'answers.sealed.json'),JSON.stringify(bundle.sealed),{flag:'wx',mode:0o600});
  fs.writeFileSync(path.join(inputs,'cases.json'),JSON.stringify(bundle.cases),{flag:'wx',mode:0o600});
  fs.writeFileSync(path.join(inputs,'manifest.json'),JSON.stringify(bundle.manifest),{flag:'wx',mode:0o600});
  return bundle.manifest;
}
module.exports={threeMonthsBefore,sample,buildSealedSet,writeSealedSet};
if(require.main===module){let body='';process.stdin.setEncoding('utf8');process.stdin.on('data',s=>body+=s);process.stdin.on('end',()=>{try{console.log(JSON.stringify(writeSealedSet(JSON.parse(body))));}catch(e){console.error(JSON.stringify({status:e.code||'SEAL_FAILED'}));process.exitCode=1;}});}
