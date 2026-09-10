'use strict';
const {hash,requireValue:check}=require('./common.cjs');
const REASONS=Object.freeze({use_clear:'用途がいい',demand_promising:'需要がありそう',policy_fit:'方針に合う',own_duplicate:'自社品と重複',too_similar:'似た案が多い',policy_mismatch:'方針に合わない',brand_dependent:'ブランド頼み',other:'その他'});
const normal=s=>String(s||'').normalize('NFKC').toLowerCase().replace(/\s+/g,'');
function latestJudgements(events=[]){
  const map=new Map();for(const e of events){
    check(e&&typeof e.candidate_id==='string'&&typeof e.kw==='string'&&['adopt','hold','reject'].includes(e.decision),'INVALID_JUDGEMENT');
    const previous=map.get(e.candidate_id);if(previous&&Number.isFinite(previous.event_seq)&&Number.isFinite(e.event_seq)&&previous.event_seq>e.event_seq)continue;
    map.set(e.candidate_id,{candidate_id:e.candidate_id,kw:e.kw,use:String(e.use||''),category:String(e.category||''),decision:e.decision,reason:String(e.reason||'').slice(0,1000),reason_codes:(e.reason_codes||[]).filter(c=>REASONS[c]),decided_at:e.decided_at,event_seq:e.event_seq});
  }return [...map.values()];
}
function overlap(a,b){
  const words=String(a||'').normalize('NFKC').toLowerCase().split(/[\s、・/]+/).filter(x=>x.length>=2);
  const hay=normal(b);return words.reduce((n,w)=>n+(hay.includes(w)?1:0),0);
}
function relevance(e,row){return overlap(e.kw,row.title)+(e.category&&e.category===row.categoryPath?1:0);}
function learningContext(judgements,pool,limit=48){
  const latest=latestJudgements(judgements);const counts={adopt:0,hold:0,reject:0};const reasons={};
  for(const e of latest){counts[e.decision]++;for(const code of e.reason_codes){reasons[code]??={adopt:0,hold:0,reject:0};reasons[code][e.decision]++;}}
  const chosen=[];const used=new Set();
  // Each outcome gets examples; one negative judgement never becomes a blanket ban.
  for(const decision of ['adopt','reject','hold']){
    const scored=latest.filter(e=>e.decision===decision).map(e=>({e,score:Math.max(0,...pool.map(p=>relevance(e,p)))})).sort((a,b)=>b.score-a.score||String(b.e.decided_at).localeCompare(String(a.e.decided_at)));
    for(const {e} of scored.slice(0,Math.floor(limit/3))){chosen.push(e);used.add(e.candidate_id);}
  }
  for(const e of [...latest].sort((a,b)=>String(b.decided_at).localeCompare(String(a.decided_at))))if(chosen.length<limit&&!used.has(e.candidate_id)){chosen.push(e);used.add(e.candidate_id);}
  return {version:hash(latest),judgement_count:latest.length,counts,reason_counts:reasons,examples:chosen,instruction:'代表の判断と理由を参考にする。判断なしを見送りとみなさない。保留は否定ではない。少数の例から素材・カテゴリ全体を禁止しない。似た用途の改善案と、判断例に似ていない新しい用途も出す。'};
}
function preferenceScore(row,judgements){
  let score=0;for(const e of judgements){const fit=relevance(e,row);if(fit)score+=fit*(e.decision==='adopt'?2:e.decision==='reject'?-1:0);}
  return score;
}
function preferenceScorer(judgements){
  const weights=new Map(),categories=new Map();
  for(const e of judgements){const weight=e.decision==='adopt'?2:e.decision==='reject'?-1:0;if(!weight)continue;
    if(e.category)categories.set(e.category,(categories.get(e.category)||0)+weight);
    for(const word of String(e.kw).normalize('NFKC').toLowerCase().split(/[\s、・/]+/).filter(w=>w.length>=2))weights.set(word,(weights.get(word)||0)+weight);
  }
  const root={children:new Map()};for(const [word,weight]of weights){if(!weight)continue;let node=root;for(const c of word){if(!node.children.has(c))node.children.set(c,{children:new Map()});node=node.children.get(c);}node.word=word;node.weight=weight;}
  return row=>{let score=categories.get(row.categoryPath)||0;const chars=Array.from(normal(row.title)),seen=new Set();
    for(let i=0;i<chars.length;i++){let node=root;for(let j=i;j<chars.length;j++){node=node.children.get(chars[j]);if(!node)break;if(node.word&&!seen.has(node.word)){seen.add(node.word);score+=node.weight;}}}return score;
  };
}
module.exports={REASONS,latestJudgements,learningContext,preferenceScore,preferenceScorer};
