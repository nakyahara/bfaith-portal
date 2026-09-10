import {test} from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import ejs from 'ejs';
import {createRequire} from 'node:module';
import {createProductScoutTables} from './schema.js';
import {ingestKeywords,latestKeywords,keywordSyncState,keywordHistory,recordKeywordDecision} from './keywords.js';
const require=createRequire(import.meta.url);const core=require('../../scripts/product-idea-scout/ai/kw-core.cjs');
function sample(){
  const at=new Date().toISOString();const asin='B000000001';const kw='植え替えシート';
  const c={candidate_id:core.keywordId(kw),kw,use:'土を受ける',idea:'土を受けるシート',previous:[],own_matches:[],evidence:[{asin,title_excerpt:'園芸用シート',source:'Keepa Product Request',url:'https://www.amazon.co.jp/dp/'+asin,price:600,monthly_units:100,observed_at:at,demand_observed_at:at}]};
  const r={candidate_id:c.candidate_id,decision:'retain',exclusion_code:'none',matched_asins:[asin],match_reason:'用途が一致',policy_reason:'用途で探す商品',competition_note:'比較が必要',unknowns:[]};
  return core.finalize([c],[r],{run_id:'portal-test',day:at.slice(0,10),now:at});
}
test('KWを既存スカウトDBに取り込み、同一runの異なる内容は上書きしない',t=>{
  const db=new Database(':memory:');t.after(()=>db.close());createProductScoutTables(db);createProductScoutTables(db);
  const run=sample();const first=ingestKeywords(run,db);assert.deepEqual(ingestKeywords(run,db),first);assert.equal(latestKeywords(db).items.length,1);
  const changed=structuredClone(run);changed.items[0].idea='改変';assert.throws(()=>ingestKeywords(changed,db),e=>e.status===409);
  assert.equal(keywordSyncState(db).body_hash,first.body_hash);
});
test('判断は追記し、見送り理由と対象・ログインを必須にする。AIへ返すのは最新判断だけ',t=>{
  const db=new Database(':memory:');t.after(()=>db.close());createProductScoutTables(db);const run=sample();ingestKeywords(run,db);
  const decision={run_id:run.run_id,candidate_id:run.items[0].candidate_id,decision:'reject',decided_by:'test@example.com'};
  assert.throws(()=>recordKeywordDecision(decision,db),/見送り/);assert.throws(()=>recordKeywordDecision({...decision,decided_by:null},db),e=>e.status===401);
  assert.throws(()=>recordKeywordDecision({...decision,candidate_id:'wrong',comment:'用途違い'},db),e=>e.status===404);
  recordKeywordDecision({...decision,comment:'類似案が多い'},db);
  // Give the latter event an explicit later timestamp; UUID order is not event order.
  t.mock.timers.enable({apis:['Date'],now:Date.now()+1000});
  recordKeywordDecision({...decision,decision:'adopt',comment:'用途を再検討'},db);
  const state=keywordSyncState(db);assert.equal(state.history.length,2);assert.equal(keywordHistory(db).length,1);assert.equal(keywordHistory(db)[0].decision,'adopt');assert.ok(!JSON.stringify(state.history).includes('test@example.com'));
  assert.equal(db.prepare('SELECT count(*) n FROM scout_keyword_decisions').get().n,2);assert.equal(latestKeywords(db).items[0].last_decision.decision,'adopt');
  assert.throws(()=>db.prepare('DELETE FROM scout_keyword_decisions').run(),/append only/);
});
test('空・前日・判断済みのKW画面を描画し、商品名やコメントのHTMLを実行させない',async t=>{
  const db=new Database(':memory:');t.after(()=>db.close());createProductScoutTables(db);const run=sample();run.items[0].idea='<script>alert(1)</script>';ingestKeywords(run,db);
  const file=new URL('./views/keywords.ejs',import.meta.url);
  const html=await ejs.renderFile(file.pathname.replace(/^\/([A-Z]:)/,'$1'),{run:latestKeywords(db),today:'2099-01-01'});
  assert.ok(html.includes('&lt;script&gt;'));assert.ok(!html.includes('<script>alert'));assert.ok(html.includes('前回の案'));assert.ok(html.includes('検索回数や自社の売上予測ではありません'));
  assert.ok((await ejs.renderFile(file.pathname.replace(/^\/([A-Z]:)/,'$1'),{run:null,today:'2099-01-01'})).includes('まだKW案が届いていません'));
});
