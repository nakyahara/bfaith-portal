import {test} from 'node:test';import assert from 'node:assert/strict';import Database from 'better-sqlite3';import ejs from 'ejs';import {createRequire} from 'node:module';import {fileURLToPath} from 'node:url';
import {createKeywordTables,ingestKeywords,keywordQueue,keywordSyncState,recordKeywordDecision,keywordHistory,latestKeywords,REASONS} from './keywords.js';
const require=createRequire(import.meta.url);const {keywordId}=require('../../scripts/product-idea-scout/ai/kw-core.cjs');
function edition(run_id,offset=0,n=5){const now=new Date().toISOString();return {schema_version:'kw-discovery-v2',policy_version:require('../../scripts/product-idea-scout/ai/kw-policy.json').version,run_id,day:now.slice(0,10),generated_at:now,status:'completed',new_count:n,submitted_count:n,target_count:null,warnings:[],model_audit:[],stop_reason:'source_exhausted',coverage:{source_rows:n,unique_products:n,examined_this_run:n,seen_in_cycle:n,remaining_in_cycle:0,cycle:1},learning_audit:{judgement_count:0,example_ids:[]},items:Array.from({length:n},(_,i)=>{const kw='用途'+(i+offset)+' シート',asin='B'+String(i+offset).padStart(9,'0');return {candidate_id:keywordId(kw),kw,use:'土を受ける',idea:'土受け',reason:'用途が明確',state:'draft',edition:'new',search_volume:null,search_verified:false,evidence_scope:'source_products',own_matches:[],unknowns:[],learning_refs:[],seed_asins:[asin],screening:{candidate_id:keywordId(kw),policy_version:require('../../scripts/product-idea-scout/ai/kw-policy.json').version,decision:'propose',codes:[],reason:'室内で土を受ける用途',matched_asins:[asin],buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'植え替え時に室内の床へ土をこぼさないシート'},category:'園芸',evidence:[{asin,url:'https://www.amazon.co.jp/dp/'+asin,title_excerpt:'園芸シート',source:'Keepa (existing collector)',price:null,monthly_units:null,recorded_price:300,recorded_monthly_units:100,recorded_at:now}]};})};}
test('翌日の案が増えても未判定を保持。理由をワンクリックで保存し、判断の訂正を返す',async t=>{
 const db=new Database(':memory:');t.after(()=>db.close());createKeywordTables(db);const first=edition('first'),next=edition('next',5);ingestKeywords(first,db);ingestKeywords(next,db);
 assert.equal(keywordQueue({},db).total,10);assert.equal(keywordQueue({limit:3,page:2},db).items.length,3);
 const item=first.items[0];recordKeywordDecision({run_id:'first',candidate_id:item.candidate_id,decision:'reject',reason_codes:['too_similar'],decided_by:'human'},db);
 assert.equal(keywordQueue({},db).total,9);assert.equal(keywordHistory(db)[0].reason_codes[0],'too_similar');
 recordKeywordDecision({run_id:'first',candidate_id:item.candidate_id,decision:'adopt',comment:'用途を分ければ良い',decided_by:'human'},db);assert.equal(keywordHistory(db)[0].decision,'adopt');
 const page=keywordSyncState(db,{since:1});assert.equal(page.history.length,1);assert.equal(page.history[0].decision,'adopt');assert.equal(page.feedback_cursor,2);
 const html=await ejs.renderFile(fileURLToPath(new URL('./views/keyword-queue.ejs',import.meta.url)),{run:latestKeywords(db),today:'2099-01-01',queue:keywordQueue({},db),reasonLabels:REASONS});assert.ok(html.includes('未判定の案は翌日も残ります'));assert.ok(html.includes('用途1 シート'));assert.ok(html.includes('保存時の参考価格'));
});
test('500件を超える判断履歴を欠落なく返し、過去の判断時点の用途を保持する',t=>{
 const db=new Database(':memory:');t.after(()=>db.close());createKeywordTables(db);const run=edition('many',0,505);ingestKeywords(run,db);
 db.transaction(()=>{for(const i of run.items)recordKeywordDecision({run_id:run.run_id,candidate_id:i.candidate_id,decision:'adopt',reason_codes:['use_clear'],decided_by:'human'},db);})();
 const first=keywordSyncState(db),second=keywordSyncState(db,{since:first.feedback_cursor});assert.equal(first.history.length,500);assert.equal(second.history.length,5);assert.equal(first.feedback_has_more,true);assert.equal(second.feedback_has_more,false);
 const changed=edition('changed',0,1);changed.generated_at=new Date(Date.now()+1000).toISOString();changed.items[0].use='別用途に編集';ingestKeywords(changed,db);
 assert.equal(keywordHistory(db)[0].use,'土を受ける');assert.equal(keywordQueue({status:'adopt'},db).total,505);
});
test('旧テーブルの判断を失わず、新しいカード一覧と理由列へ移行できる',t=>{
 const db=new Database(':memory:');t.after(()=>db.close());db.exec('CREATE TABLE scout_keyword_decisions (decision_id TEXT PRIMARY KEY,run_id TEXT,candidate_id TEXT,decision TEXT,comment TEXT,decided_by TEXT,decided_at TEXT)');createKeywordTables(db);createKeywordTables(db);
 assert.ok(db.prepare('PRAGMA table_info(scout_keyword_decisions)').all().some(c=>c.name==='item_snapshot_json'));
 assert.throws(()=>recordKeywordDecision({decision:'reject',reason_codes:['forged'],decided_by:'human'},db),/理由の選択/);
});

test('見送りの採否と肯定・価格の理由を分けたまま保存し、学習へ返す',t=>{
 const db=new Database(':memory:');t.after(()=>db.close());createKeywordTables(db);const run=edition('reason-codes',0,1);ingestKeywords(run,db);
 const i=run.items[0];recordKeywordDecision({run_id:run.run_id,candidate_id:i.candidate_id,decision:'reject',reason_codes:['use_clear','price_competition','tooling_investment'],comment:'用途はよいが今回は見送る',decided_by:'tester'},db);
 const history=keywordSyncState(db).history;assert.equal(history[0].decision,'reject');assert.deepEqual(history[0].reason_codes,['use_clear','price_competition','tooling_investment']);
 const signal=require('../../scripts/product-idea-scout/ai/kw-learning.cjs').interpretJudgement(history[0]);assert.equal(signal.direction,'positive');assert.equal(signal.weight,2);assert.equal(keywordQueue({status:'reject'},db).total,1);
});
