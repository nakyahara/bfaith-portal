'use strict';
const {hash,requireValue:check}=require('./common.cjs');
const LEARNING_RULE_VERSION='reason-aware-v1';
const REASONS=Object.freeze({use_clear:'選定・用途がいい',demand_promising:'需要がありそう',policy_fit:'方針に合う',own_duplicate:'自社品と重複',already_proposed:'過去に提案済み',price_competition:'安価な競合が強い',tooling_investment:'金型・初期投資が重い',safety_responsibility:'安全面の責任が重い',too_similar:'似た案が多い',policy_mismatch:'方針に合わない',brand_dependent:'ブランド頼み',other:'その他'});
const LEARNING_INSTRUCTION='代表の今回の採否と、選定・用途の良さを分けて読む。「見送りだが選定は良い」は方向性への肯定を保つ。「いい」でも既存商品・過去提案なら新規採用例とは数えず、同用途の再提案を避ける。価格競争、初期投資、安全面の懸念は似た案の選別へ引き継ぐが、製造方法・金型の必要性・競合の製造国・商標を事実確認した記録とは扱わない。同じ問題を解消する具体的な違いが示せなければ再び推薦しない。製造先・工程・原価の未確認だけを理由に落とさない。判断なし・保留は否定ではない。少数の例から素材・カテゴリ全体を禁止しない。';
const normal=s=>String(s||'').normalize('NFKC').toLowerCase().replace(/\s+/g,'');
const words=s=>[...new Set(String(s||'').normalize('NFKC').toLowerCase().split(/[\s、・/]+/).filter(x=>x.length>=2))];
function latestJudgements(events=[]){
  const map=new Map();for(const e of events){
    check(e&&typeof e.candidate_id==='string'&&typeof e.kw==='string'&&['adopt','hold','reject'].includes(e.decision),'INVALID_JUDGEMENT');
    const previous=map.get(e.candidate_id);if(previous&&Number.isFinite(previous.event_seq)&&Number.isFinite(e.event_seq)&&previous.event_seq>e.event_seq)continue;
    map.set(e.candidate_id,{candidate_id:e.candidate_id,kw:e.kw,use:String(e.use||''),category:String(e.category||''),decision:e.decision,reason:String(e.reason||'').slice(0,1000),reason_codes:(e.reason_codes||[]).filter(c=>REASONS[c]),decided_at:e.decided_at,event_seq:e.event_seq});
  }return [...map.values()];
}
// Conservative, auditable hints. The human record remains unchanged; unrecognised text stays unknown.
function interpretJudgement(e){
  const reason=String(e.reason||'').normalize('NFKC'),codes=new Set(e.reason_codes||[]),evidence=[];
  function stated(code,pattern){
    if(codes.has(code)){evidence.push({signal:code,source:'reason_code',value:code});return true;}
    const match=reason.match(pattern);if(!match)return false;
    if(/^(?:ではない|でない|わけではない|とは思わない|とは言えない|していない|してない|しておらず)/.test(reason.slice(match.index+match[0].length)))return false;
    evidence.push({signal:code,source:'comment',value:match[0]});return true;
  }
  const positiveText=/(?:選定|方向性|用途)(?:自体)?(?:は|が)?(?:とても|かなり)?(?:いい|良い|よい)(?!とは|わけ|か(?:どうか|不明)|と思わな)/;
  const negativeText=/(?:選定|方向性|用途)(?:自体)?(?:は|が)?(?:よくない|良くない|悪い|合わない|違う|(?:いい|良い|よい)(?:とは思わない|とは言えない|わけではない))/;
  let positive=stated('use_clear',positiveText);
  for(const code of ['demand_promising','policy_fit'])if(codes.has(code)){positive=true;evidence.push({signal:'direction_positive',source:'reason_code',value:code});}
  const negative=negativeText.test(reason);
  if(negative)evidence.push({signal:'direction_negative',source:'comment',value:reason.match(negativeText)[0]});
  const blockers=[];
  const patterns={
    own_duplicate:/(?:すでに|既に)商品化(?:している|してる|済み)?|商品化済み|自社(?:商品|品)?(?:と|に)(?:重複|同じ)/,
    already_proposed:/過去(?:に|の)?(?:商品化案を送った|提案済み|提案した)|(?:以前|前)(?:に|も)提案(?:済み|した)|提案済み/,
    price_competition:/(?:競合|他社|中国)(?:[^。\n]{0,20})(?:価格が安い|価格が安すぎる|安い|やすい|強い)(?!とは|わけ|か(?:不明|どうか))/,
    tooling_investment:/金型(?:が必要(?!ない|ではない|か|かも)|を新規|を作る|代が高い)|(?:金型|初期投資|初期費用)(?:が|は)(?:重い|高い|大きい)/,
    safety_responsibility:/(?:責任|安全面のリスク)(?:が|は)(?:重い|大きい|高い)/,
  };
  for(const [code,pattern]of Object.entries(patterns))if(stated(code,pattern))blockers.push(code);
  for(const code of ['too_similar','policy_mismatch','brand_dependent'])if(codes.has(code)){blockers.push(code);evidence.push({signal:code,source:'reason_code',value:code});}
  const conflict=positive&&negative;
  const direction=conflict?'unknown':negative?'negative':positive?'positive':e.decision==='adopt'?'positive':'unknown';
  const known=blockers.includes('own_duplicate')||blockers.includes('already_proposed');
  const contextual=blockers.some(c=>['own_duplicate','already_proposed','price_competition','tooling_investment','safety_responsibility'].includes(c));
  const weight=conflict?0:direction==='positive'?2:direction==='negative'?-1:e.decision==='reject'&&!contextual?-1:0;
  return {rule_version:LEARNING_RULE_VERSION,direction,direction_source:positive||negative?'explicit':e.decision==='adopt'?'decision':'unknown',blockers,known_item:known,actionable_positive:e.decision==='adopt'&&!known&&!conflict&&direction==='positive',weight,conflict,evidence};
}
function overlap(a,b){const hay=normal(b);return words(a).reduce((n,w)=>n+(hay.includes(w)?1:0),0);}
function relevance(e,row){return overlap(e.kw,row.title)+(e.category&&e.category===row.categoryPath?1:0);}
function sameKeyword(a,b){
  const clean=s=>normal(s).replace(/[・、/]/g,'');
  if(clean(a)===clean(b))return true;
  const aa=words(a).sort(),bb=words(b).sort();return aa.length>1&&aa.length===bb.length&&aa.every((w,i)=>w===bb[i]);
}
function relatedHistory(item,history,limit=12){
  const map=new Map();for(const h of history){if(!h?.kw)continue;const key=h.candidate_id||normal(h.kw),p=map.get(key);
    if(p&&Number.isFinite(p.event_seq)&&(!Number.isFinite(h.event_seq)||p.event_seq>h.event_seq))continue;
    map.set(key,{...p,...h});
  }
  return [...map.values()].map(h=>({h,score:overlap(item.kw,h.kw+' '+(h.use||''))+overlap(h.kw,item.kw+' '+(item.use||''))+(sameKeyword(item.kw,h.kw)?10:0)})).filter(x=>x.score>0).sort((a,b)=>b.score-a.score||(Number(b.h.event_seq)||0)-(Number(a.h.event_seq)||0)).slice(0,limit).map(({h})=>({...h,...(h.decision?{interpretation:interpretJudgement(h)}:{})}));
}
function learningContext(judgements,pool,limit=48){
  const latest=latestJudgements(judgements).map(e=>({...e,interpretation:interpretJudgement(e)}));const counts={adopt:0,hold:0,reject:0},directions={positive:0,negative:0,unknown:0},constraints={},reasons={};let actionable=0;
  for(const e of latest){counts[e.decision]++;directions[e.interpretation.direction]++;if(e.interpretation.actionable_positive)actionable++;
    for(const c of e.interpretation.blockers)constraints[c]=(constraints[c]||0)+1;
    for(const code of e.reason_codes){reasons[code]??={adopt:0,hold:0,reject:0};reasons[code][e.decision]++;}}
  const chosen=[],used=new Set();
  for(const decision of ['adopt','reject','hold']){
    const scored=latest.filter(e=>e.decision===decision).map(e=>({e,score:Math.max(0,...pool.map(p=>relevance(e,p)))})).sort((a,b)=>b.score-a.score||String(b.e.decided_at).localeCompare(String(a.e.decided_at)));
    for(const {e}of scored.slice(0,Math.floor(limit/3))){chosen.push(e);used.add(e.candidate_id);}
  }
  for(const e of [...latest].sort((a,b)=>String(b.decided_at).localeCompare(String(a.decided_at))))if(chosen.length<limit&&!used.has(e.candidate_id)){chosen.push(e);used.add(e.candidate_id);}
  return {version:hash({rule_version:LEARNING_RULE_VERSION,latest}),rule_version:LEARNING_RULE_VERSION,judgement_count:latest.length,counts,reason_counts:reasons,direction_counts:directions,constraint_counts:constraints,actionable_positive_count:actionable,examples:chosen,instruction:LEARNING_INSTRUCTION};
}
function preferenceScore(row,judgements){
  return latestJudgements(judgements).reduce((score,e)=>score+overlap(e.kw,row.title)*interpretJudgement(e).weight,0);
}
function preferenceScorer(judgements){
  const weights=new Map();
  for(const e of latestJudgements(judgements)){const weight=interpretJudgement(e).weight;if(!weight)continue;
    for(const word of words(e.kw))weights.set(word,(weights.get(word)||0)+weight);
  }
  // No category-wide negative weight from one rejected product.
  const root={children:new Map()};for(const [word,weight]of weights){if(!weight)continue;let node=root;for(const c of word){if(!node.children.has(c))node.children.set(c,{children:new Map()});node=node.children.get(c);}node.word=word;node.weight=weight;}
  return row=>{let score=0;const chars=Array.from(normal(row.title)),seen=new Set();
    for(let i=0;i<chars.length;i++){let node=root;for(let j=i;j<chars.length;j++){node=node.children.get(chars[j]);if(!node)break;if(node.word&&!seen.has(node.word)){seen.add(node.word);score+=node.weight;}}}return score;
  };
}
module.exports={REASONS,LEARNING_RULE_VERSION,LEARNING_INSTRUCTION,latestJudgements,interpretJudgement,learningContext,preferenceScore,preferenceScorer,relatedHistory,sameKeyword};
