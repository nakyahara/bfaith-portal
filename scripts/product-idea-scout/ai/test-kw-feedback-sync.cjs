'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const os=require('node:os');
const {publish}=require('./kw-publish.cjs');const {hash}=require('./common.cjs');
const event=(event_seq,candidate_id,decision)=>({event_seq,candidate_id,kw:'用途'+candidate_id,use:'用途',category:'',decision,reason:'代表の理由',reason_codes:[],decided_at:'2026-09-10T00:00:00Z'});
test('判断をページで全件同期し、訂正を優先。次回は続きだけを取得する',async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'kw-feedback-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const value={run_id:'test',status:'completed',new_count:4};let runs=0;const cursors=[];
 const options={env:{MIRROR_SYNC_KEY:'test'},runFn:async()=>{runs++;const cache=JSON.parse(fs.readFileSync(path.join(dir,'learning-feedback.json'),'utf8'));assert.equal(cache.cursor,3);assert.equal(cache.judgements.length,2);assert.equal(cache.judgements.find(e=>e.candidate_id==='a').decision,'adopt');return value;},fetchFn:async(url,opt)=>{
  const u=new URL(url);if(opt.method==='POST'||!u.search)return {ok:true,json:async()=>({run_id:'test',body_hash:hash(value)})};
  const since=Number(u.searchParams.get('since'));cursors.push(since);const page=since===0?{history:[event(1,'a','reject'),event(2,'b','hold')],feedback_cursor:2,feedback_has_more:true}:since===2?{history:[event(3,'a','adopt')],feedback_cursor:3,feedback_has_more:false}:{history:[],feedback_cursor:3,feedback_has_more:false};return {ok:true,json:async()=>page};
 }};
 await publish({state_dir:dir},options);await publish({state_dir:dir},options);assert.deepEqual(cursors,[0,2,3]);assert.equal(runs,2);
});
test('同期途中の失敗では旧判断を消さず、AIを開始しない',async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'kw-feedback-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const file=path.join(dir,'learning-feedback.json');const saved={cursor:1,judgements:[event(1,'a','adopt')]};fs.writeFileSync(file,JSON.stringify(saved));let count=0,runs=0;
 await assert.rejects(publish({state_dir:dir},{env:{MIRROR_SYNC_KEY:'test'},runFn:async()=>runs++,fetchFn:async()=>++count===1?{ok:true,json:async()=>({history:[event(2,'b','reject')],feedback_cursor:2,feedback_has_more:true})}:{ok:false,status:503}}),/PORTAL_HTTP_503/);
 assert.deepEqual(JSON.parse(fs.readFileSync(file,'utf8')),saved);assert.equal(runs,0);
});
