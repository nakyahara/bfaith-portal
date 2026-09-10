import crypto from 'node:crypto';
import {createRequire} from 'node:module';
import {getMirrorDB} from '../warehouse-mirror/db.js';
const require=createRequire(import.meta.url);
const {hash}=require('../../scripts/product-idea-scout/ai/common.cjs');
const {validateEdition}=require('../../scripts/product-idea-scout/ai/kw-core.cjs');
export const {REASONS}=require('../../scripts/product-idea-scout/ai/kw-learning.cjs');
const fail=(message,status=400)=>{throw Object.assign(new Error(message),{status});};
export function createKeywordTables(db){
 const migrateCards=!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scout_keyword_cards'").get();
 db.exec(`CREATE TABLE IF NOT EXISTS scout_keyword_runs (run_id TEXT PRIMARY KEY,day TEXT NOT NULL,generated_at TEXT NOT NULL,body_hash TEXT NOT NULL,body_json TEXT NOT NULL,ingested_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS scout_keyword_decisions (decision_id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES scout_keyword_runs(run_id),candidate_id TEXT NOT NULL,decision TEXT NOT NULL CHECK(decision IN ('adopt','hold','reject')),comment TEXT NOT NULL,decided_by TEXT NOT NULL,decided_at TEXT NOT NULL,reason_codes_json TEXT NOT NULL DEFAULT '[]');
 CREATE TABLE IF NOT EXISTS scout_keyword_cards (candidate_id TEXT PRIMARY KEY,latest_run_id TEXT NOT NULL REFERENCES scout_keyword_runs(run_id),kw TEXT NOT NULL,item_json TEXT NOT NULL,first_seen_at TEXT NOT NULL,generated_at TEXT NOT NULL);
 CREATE INDEX IF NOT EXISTS idx_keyword_day ON scout_keyword_runs(day,generated_at);
 CREATE INDEX IF NOT EXISTS idx_keyword_decision_candidate ON scout_keyword_decisions(candidate_id);
 CREATE TRIGGER IF NOT EXISTS keyword_decisions_no_update BEFORE UPDATE ON scout_keyword_decisions BEGIN SELECT RAISE(ABORT,'append only'); END;
 CREATE TRIGGER IF NOT EXISTS keyword_decisions_no_delete BEFORE DELETE ON scout_keyword_decisions BEGIN SELECT RAISE(ABORT,'append only'); END;`);
 if(!db.prepare('PRAGMA table_info(scout_keyword_decisions)').all().some(c=>c.name==='reason_codes_json'))db.exec("ALTER TABLE scout_keyword_decisions ADD COLUMN reason_codes_json TEXT NOT NULL DEFAULT '[]'");
 if(!db.prepare('PRAGMA table_info(scout_keyword_decisions)').all().some(c=>c.name==='item_snapshot_json'))db.exec('ALTER TABLE scout_keyword_decisions ADD COLUMN item_snapshot_json TEXT');
 const insert=db.prepare('INSERT OR IGNORE INTO scout_keyword_cards VALUES (?,?,?,?,?,?)');
 if(migrateCards)for(const r of db.prepare('SELECT run_id,generated_at,body_json FROM scout_keyword_runs ORDER BY generated_at DESC').iterate())for(const i of JSON.parse(r.body_json).items)insert.run(i.candidate_id,r.run_id,i.kw,JSON.stringify(i),r.generated_at,r.generated_at);
}
export function ingestKeywords(value,handle){
 try{validateEdition(value);if(value.schema_version==='kw-discovery-v2'&&value.policy_version!==require('../../scripts/product-idea-scout/ai/kw-policy.json').version)fail('方針変更前の版です。再選別した案を送信してください');}catch(e){fail(e.code||'KW案の形式が不正です');}
 const db=handle||getMirrorDB();const body=JSON.stringify(value),bodyHash=hash(value);
 return db.transaction(()=>{
  const previous=db.prepare('SELECT body_hash FROM scout_keyword_runs WHERE run_id=?').get(value.run_id);
  if(previous&&previous.body_hash!==bodyHash)fail('同じ実行IDの内容を上書きできません',409);
  db.prepare('INSERT OR IGNORE INTO scout_keyword_runs VALUES (?,?,?,?,?,?)').run(value.run_id,value.day,value.generated_at,bodyHash,body,new Date().toISOString());
  const upsert=db.prepare(`INSERT INTO scout_keyword_cards VALUES (?,?,?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET latest_run_id=excluded.latest_run_id,kw=excluded.kw,item_json=excluded.item_json,generated_at=excluded.generated_at WHERE excluded.generated_at>=scout_keyword_cards.generated_at`);
  for(const i of value.items)upsert.run(i.candidate_id,value.run_id,i.kw,JSON.stringify(i),value.generated_at,value.generated_at);
  return {run_id:value.run_id,body_hash:bodyHash,new_count:value.new_count};
 })();
}
const LATEST=`SELECT d.* FROM scout_keyword_decisions d WHERE d.rowid=(SELECT max(d2.rowid) FROM scout_keyword_decisions d2 WHERE d2.candidate_id=d.candidate_id)`;
function withDecision(item,d){return {...item,last_decision:d?.decision?{decision:d.decision,comment:d.comment,decided_at:d.decided_at,reason_codes:JSON.parse(d.reason_codes_json||'[]')}:null};}
export function latestKeywords(handle){
 const db=handle||getMirrorDB(),row=db.prepare('SELECT body_json FROM scout_keyword_runs ORDER BY generated_at DESC,run_id DESC LIMIT 1').get();if(!row)return null;
 const run=JSON.parse(row.body_json),latest=new Map(db.prepare(LATEST).all().map(d=>[d.candidate_id,d]));return {...run,items:run.items.map(i=>withDecision(i,latest.get(i.candidate_id)))};
}
export function keywordQueue({status='undecided',page=1,limit=30}={},handle){
 const db=handle||getMirrorDB();status=['undecided','all','adopt','hold','reject'].includes(status)?status:'undecided';page=Math.max(1,Math.floor(Number(page))||1);limit=Math.min(100,Math.max(1,limit));
 const join=`FROM scout_keyword_cards c LEFT JOIN (${LATEST}) d ON d.candidate_id=c.candidate_id`;
 const where=status==='all'?'':status==='undecided'?'WHERE d.decision IS NULL':'WHERE d.decision=?',args=['all','undecided'].includes(status)?[]:[status];
 const total=db.prepare(`SELECT count(*) n ${join} ${where}`).get(...args).n;
 const rows=db.prepare(`SELECT c.item_json,c.latest_run_id,c.first_seen_at,d.decision,d.comment,d.decided_at,d.reason_codes_json ${join} ${where} ORDER BY c.first_seen_at,c.candidate_id LIMIT ? OFFSET ?`).all(...args,limit,(page-1)*limit);
 const counts={undecided:0,adopt:0,hold:0,reject:0};for(const r of db.prepare(`SELECT coalesce(d.decision,'undecided') status,count(*) n ${join} GROUP BY coalesce(d.decision,'undecided')`).all())counts[r.status]=r.n;
 return {items:rows.map(r=>({...withDecision(JSON.parse(r.item_json),r),run_id:r.latest_run_id,first_seen_at:r.first_seen_at})),page,limit,total,status,counts};
}
function eventRow(row){const i=JSON.parse(row.item_snapshot_json||row.item_json);return {event_seq:row.event_seq,candidate_id:row.candidate_id,kw:i.kw,use:i.use,category:i.category||'',decision:row.decision,reason:row.comment,reason_codes:JSON.parse(row.reason_codes_json||'[]'),decided_at:row.decided_at};}
export function keywordSyncState(handle,{since=0}={}){
 const db=handle||getMirrorDB();if(!Number.isSafeInteger(since)||since<0)fail('判断カーソルが不正です');
 const row=db.prepare('SELECT run_id,body_hash,body_json FROM scout_keyword_runs ORDER BY generated_at DESC,run_id DESC LIMIT 1').get(),max=db.prepare('SELECT coalesce(max(rowid),0) n FROM scout_keyword_decisions').get().n;
 if(since>max)fail('判断履歴の再同期が必要です',409);
 const history=db.prepare(`SELECT d.rowid event_seq,d.*,c.item_json FROM scout_keyword_decisions d JOIN scout_keyword_cards c ON c.candidate_id=d.candidate_id WHERE d.rowid>? ORDER BY d.rowid LIMIT 500`).all(since).map(eventRow),cursor=history.at(-1)?.event_seq??since;
 return {run_id:row?.run_id||null,body_hash:row?.body_hash||null,new_count:row?JSON.parse(row.body_json).new_count:0,history,feedback_cursor:cursor,feedback_has_more:cursor<max,feedback_total:max};
}
export function keywordHistory(handle){
 const db=handle||getMirrorDB();return db.prepare(`SELECT d.rowid event_seq,d.*,c.item_json FROM scout_keyword_decisions d JOIN scout_keyword_cards c ON c.candidate_id=d.candidate_id WHERE d.rowid=(SELECT max(d2.rowid) FROM scout_keyword_decisions d2 WHERE d2.candidate_id=d.candidate_id) ORDER BY d.rowid`).all().map(eventRow);
}
export function recordKeywordDecision({run_id,candidate_id,decision,comment='',reason_codes=[],decided_by},handle){
 if(!decided_by)fail('ログインが必要です',401);if(!['adopt','hold','reject'].includes(decision))fail('判断が不正です');
 if(!Array.isArray(reason_codes)||reason_codes.length>8||reason_codes.some(c=>!REASONS[c]))fail('理由の選択が不正です');
 if(typeof comment!=='string'||comment.length>1000||(decision==='reject'&&!comment.trim()&&!reason_codes.length))fail('見送りには理由を選ぶか記入してください');
 const db=handle||getMirrorDB(),row=db.prepare('SELECT body_json FROM scout_keyword_runs WHERE run_id=?').get(run_id);
 if(!row||!JSON.parse(row.body_json).items.some(i=>i.candidate_id===candidate_id))fail('対象のKW案がありません',404);
 const item=JSON.parse(row.body_json).items.find(i=>i.candidate_id===candidate_id);
 const snapshot=JSON.stringify({kw:item.kw,use:item.use,category:item.category||''});
 const id=crypto.randomUUID();db.prepare('INSERT INTO scout_keyword_decisions (decision_id,run_id,candidate_id,decision,comment,decided_by,decided_at,reason_codes_json,item_snapshot_json) VALUES (?,?,?,?,?,?,?,?,?)').run(id,run_id,candidate_id,decision,comment.trim(),decided_by,new Date().toISOString(),JSON.stringify([...new Set(reason_codes)]),snapshot);return {decision_id:id};
}
