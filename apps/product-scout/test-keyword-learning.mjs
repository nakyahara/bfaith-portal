import {test} from 'node:test';import assert from 'node:assert/strict';import Database from 'better-sqlite3';import ejs from 'ejs';import {createRequire} from 'node:module';import {fileURLToPath} from 'node:url';
import {createKeywordTables,ingestKeywords,keywordQueue,keywordSyncState,recordKeywordDecision,keywordHistory,latestKeywords,screenedOutKeywords,REASONS,REASON_GROUPS} from './keywords.js';
const require=createRequire(import.meta.url);const {keywordId}=require('../../scripts/product-idea-scout/ai/kw-core.cjs');
function edition(run_id,offset=0,n=5){const now=new Date().toISOString();return {schema_version:'kw-discovery-v2',policy_version:require('../../scripts/product-idea-scout/ai/kw-policy.json').version,run_id,day:now.slice(0,10),generated_at:now,status:'completed',new_count:n,submitted_count:n,target_count:null,warnings:[],model_audit:[],stop_reason:'source_exhausted',coverage:{source_rows:n,unique_products:n,examined_this_run:n,seen_in_cycle:n,remaining_in_cycle:0,cycle:1},learning_audit:{judgement_count:0,example_ids:[]},items:Array.from({length:n},(_,i)=>{const kw='用途'+(i+offset)+' シート',asin='B'+String(i+offset).padStart(9,'0');return {candidate_id:keywordId(kw),kw,use:'土を受ける',idea:'土受け',reason:'用途が明確',state:'draft',edition:'new',search_volume:null,search_verified:false,evidence_scope:'source_products',own_matches:[],unknowns:[],learning_refs:[],seed_asins:[asin],screening:{candidate_id:keywordId(kw),policy_version:require('../../scripts/product-idea-scout/ai/kw-policy.json').version,decision:'propose',codes:[],reason:'室内で土を受ける用途',matched_asins:[asin],buy_by:'generic',own_overlap:'different',commodity:'clear',opportunity:'植え替え時に室内の床へ土をこぼさないシート'},category:'園芸',evidence:[{asin,url:'https://www.amazon.co.jp/dp/'+asin,title_excerpt:'園芸シート',source:'Keepa (existing collector)',price:null,monthly_units:null,recorded_price:300,recorded_monthly_units:100,recorded_at:now}]};})};}
test('翌日の案が増えても未判定を保持。理由をワンクリックで保存し、判断の訂正を返す',async t=>{
 const db=new Database(':memory:');t.after(()=>db.close());createKeywordTables(db);const first=edition('first'),next=edition('next',5);ingestKeywords(first,db);ingestKeywords(next,db);
 assert.equal(keywordQueue({},db).total,10);assert.equal(keywordQueue({limit:3,page:2},db).items.length,3);
 const item=first.items[0];recordKeywordDecision({run_id:'first',candidate_id:item.candidate_id,decision:'reject',reason_codes:['too_similar'],decided_by:'human'},db);
 assert.equal(keywordQueue({},db).total,9);assert.equal(keywordHistory(db)[0].reason_codes[0],'too_similar');
 recordKeywordDecision({run_id:'first',candidate_id:item.candidate_id,decision:'adopt',comment:'用途を分ければ良い',decided_by:'human'},db);assert.equal(keywordHistory(db)[0].decision,'adopt');
 const page=keywordSyncState(db,{since:1});assert.equal(page.history.length,1);assert.equal(page.history[0].decision,'adopt');assert.equal(page.feedback_cursor,2);
 const html=await ejs.renderFile(fileURLToPath(new URL('./views/keyword-queue.ejs',import.meta.url)),{run:latestKeywords(db),today:'2099-01-01',queue:keywordQueue({},db),screened:screenedOutKeywords({},db),showScreened:false,reasonLabels:REASONS,reasonGroups:REASON_GROUPS});assert.ok(html.includes('未判定の案は翌日も残ります'));assert.ok(html.includes('用途1 シート'));assert.ok(html.includes('保存時の参考価格'));
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
test('AIが見送った案を新しい順に1件ずつ見せる。あとで提案した案・既出KWは出さない。古い回は用途なしでも出す (2026-10-06)',async t=>{
 const db=new Database(':memory:');t.after(()=>db.close());createKeywordTables(db);
 const old={candidate_id:keywordId('古い 見送り案'),kw:'古い 見送り案',decision:'defer',codes:['feedback_constraint'],reason:'価格競争の懸念が同じ',source_asins:['B000000099'],by:'R03'};
 const first={...edition('first'),generated_at:new Date(Date.now()-86400000).toISOString(),screened_out:[old,{candidate_id:keywordId('用途7 シート'),kw:'用途7 シート',decision:'defer',codes:['insufficient_market_evidence'],reason:'根拠が1件',by:'R03'},{candidate_id:keywordId('既出 KW'),kw:'既出 KW',decision:'exclude',codes:['already_seen'],reason:'既出',by:'program'}]};
 const fresh={candidate_id:keywordId('新しい 見送り案'),kw:'新しい 見送り案',decision:'exclude',codes:['own_duplicate'],reason:'自社品と同じ用途',by:'R03',use:'玄関の掃除',idea:'小さなほうき',idea_reason:'用途が明確',sources:[{asin:'B000000077',title:'元の商品 77'}]};
 const next={...edition('next',5),screened_out:[fresh,{...old,reason:'新しい回の理由'}]};
 ingestKeywords(first,db);ingestKeywords(next,db);
 const s=screenedOutKeywords({},db);
 assert.deepEqual(s.items.map(i=>i.kw),['新しい 見送り案','古い 見送り案'],'用途7 は翌日提案された・既出KW は出さない・同じ案は最新の1件');
 assert.equal(s.items[1].reason,'新しい回の理由');assert.deepEqual(s.items[1].sources,[{asin:'B000000099',title:''}]);assert.equal(s.items[0].use,'玄関の掃除');
 assert.deepEqual(s.code_counts,{own_duplicate:1,feedback_constraint:1});assert.equal(s.runs,2);
 const render=showScreened=>ejs.renderFile(fileURLToPath(new URL('./views/keyword-queue.ejs',import.meta.url)),{run:latestKeywords(db),today:'2099-01-01',queue:keywordQueue({},db),screened:s,showScreened,reasonLabels:REASONS,reasonGroups:REASON_GROUPS});
 const on=await render(true);assert.ok(on.includes('見送った理由'));assert.ok(on.includes('自社品・取扱品と同じ用途'));assert.ok(on.includes('元の商品 77'));assert.ok(on.includes('https://www.amazon.co.jp/dp/B000000099'));assert.ok(!on.includes('kw-btn--adopt'),'見るだけの一覧に判定ボタンがある');
 const off=await render(false);assert.ok(!off.includes('見送った理由'));assert.ok(off.includes('AIが見送った'));assert.ok(off.includes('kw-btn--adopt'));
});
