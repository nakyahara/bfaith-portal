'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const os=require('node:os');const path=require('node:path');
const {publish,portalEndpoint}=require('./kw-publish.cjs');const {hash}=require('./common.cjs');const {deadline}=require('../collection-window.cjs');
test('公開先・認証の失敗はAI呼出より先に止まる',async t=>{
  assert.throws(()=>portalEndpoint('https://evil.example'),/INVALID_PORTAL/);
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'kw-publish-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));let runs=0;
  await assert.rejects(publish({state_dir:dir},{env:{MIRROR_SYNC_KEY:'test'},fetchFn:async()=>({ok:false,status:404}),runFn:async()=>runs++}),/PORTAL_HTTP_404/);assert.equal(runs,0);
});
test('判断を受け取り、同じ結果を公開し、読み戻しの一致を確認する',async t=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'kw-publish-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const edition={run_id:'test',status:'partial',new_count:1};const judgement={candidate_id:'KW-test',kw:'用途語',use:'用途',category:'',decision:'adopt',reason:'いい',reason_codes:[],decided_at:'2026-09-10T00:00:00Z'};let count=0;
  const options={env:{MIRROR_SYNC_KEY:'test'},runFn:async()=>{assert.deepEqual(JSON.parse(fs.readFileSync(path.join(dir,'feedback.json'),'utf8')),[judgement]);return edition;},fetchFn:async(_url,opt)=>{
    assert.equal(opt.redirect,'error');count++;return {ok:true,json:async()=>count===1?{history:[judgement]}:{run_id:'test',body_hash:hash(edition)}};
  }};
  assert.equal((await publish({state_dir:dir},options)).new_count,1);assert.equal(count,3);assert.ok(fs.existsSync(path.join(dir,'last-published.json')));
  count=0;const good=options.fetchFn;options.fetchFn=async(...args)=>{const r=await good(...args);if(count===3)r.json=async()=>({run_id:'wrong'});return r;};
  await assert.rejects(publish({state_dir:dir},options),/PUBLISH_READBACK_MISMATCH/);
});
test('収集は04:15で終わり、再試行しても朝のKW枠へ延長しない',()=>{
  const start=Date.parse('2026-09-10T05:00:00Z');assert.equal(new Date(deadline(start,19*3600000,true)).toISOString(),'2026-09-10T19:15:00.000Z');
  const morning=Date.parse('2026-09-10T20:00:00Z');assert.equal(deadline(morning,19*3600000,true),morning);assert.equal(deadline(start,19*3600000,false),start+19*3600000);
});
