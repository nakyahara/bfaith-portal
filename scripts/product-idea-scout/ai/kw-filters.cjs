'use strict';
const policy=require('./kw-policy.json');
const {sameKeyword}=require('./kw-learning.cjs');
const norm=s=>String(s||'').normalize('NFKC').toLowerCase().replace(/[\s　・の]/g,'');
const result=(status,code,reason)=>({status,code,reason});
const PASS=result('pass','policy_eligible','対象・保存購入観測の入口条件を満たす');
function scopeGate(title,category=''){
 const t=String(title).normalize('NFKC'),c=String(category).normalize('NFKC');
 if(/第[123]類医薬品|指定第2類医薬品|要指導医薬品/.test(t)||/(?:^| > )医薬品(?: > |$)/.test(c))return result('exclude','medicine','医薬品は対象外');
 // Test the article itself, not words describing the appliance it cleans/protects.
 const passive=/ケース|カバー|収納袋|耐火袋|保護袋|洗浄剤|洗剤|クリーナー|掃除.?ブラシ|清掃.?ブラシ|掃除.?シート|補修シート|潤滑油|メンテナンスオイル/.test(t)&&!/充電ケース|発熱|電熱|自動開閉|電動ブラシ|電動クリーナー/.test(t);
 if(!passive&&/電池|電動|電気|電子|ソーラー|超音波|センサー|充電|USB|Bluetooth|ワイヤレス|コンセント|LED|モーター|ヒーター|育成ライト|水槽.{0,10}ライト|エア.?ポンプ|レーザーポインター|デジタルノギス|はんだごて|温度調整器|ブレーカー|露出.{0,6}スイッチ|配線|電源|外掛け.{0,8}フィルター|ケーブル/i.test(t))return result('exclude','electrical','電池・電気を使う製品または電気部品は対象外');
 const textileAccessory=/補修|洗濯|洗浄|収納|ハンガー|衣装ケース|型紙|生地|布地|晒し|さらし|蒸し布|マスクバンド/.test(t);
 if(!textileAccessory&&(/作業帽|ワーキングキャップ|アームカバー|アームスリーブ|アームウォーマー|エプロン|[TＴ]シャツ|シャツ|ズボン|パンツ|靴下|下着|ジャケット|パーカー|ワンピース|犬靴|犬用シューズ|犬服|犬の服|ドッグウェア/.test(t)||/ファッション > .*(?:服|シューズ)|服・アクセサリ > (?:服|ブーツ)/.test(c)))return result('exclude','apparel','アパレルは対象外');
 return PASS;
}
function sourceGate(row){
 const scope=scopeGate(row.title,row.categoryPath);if(scope.status!=='pass')return scope;
 const term=policy.filters.commodity_terms.find(k=>norm(row.title).includes(norm(k)));if(term)return result('exclude','known_commodity','既定NGの規格品・工具: '+term);
 if(!Number.isInteger(row.monthlySold)||row.monthlySold<policy.filters.monthly_units_min)return result('defer','demand_missing','月50点以上の購入観測を確認できない');
 const price=[row.priceBuyBox,row.priceNew].find(v=>Number.isFinite(v)&&v>0);
 if(!price)return result('defer','price_missing','保存価格がない');
 if(price>policy.filters.max_source_price)return result('defer','price_outside_baseline','既存探索の価格上限を超える');
 if(Number.isFinite(row.packageWeightG)&&row.packageWeightG>policy.filters.max_source_weight_g)return result('defer','oversize_source','既存探索の重量上限500gを超える');
 const tier=policy.filters.source_small_tier,dims=row.packageMm;
 if(!Array.isArray(dims)||dims.length!==3||!dims.every(n=>Number.isFinite(n)&&n>0)||!Number.isFinite(row.packageWeightG)||row.packageWeightG<=0)return result('defer','size_missing','小型配送に収まる根拠がない');
 const sorted=[...dims].sort((a,b)=>b-a);if(sorted[0]>tier.l||sorted[1]>tier.w||sorted[2]>tier.h||row.packageWeightG>tier.weightG)return result('defer','outside_small_size','既存の小型配送の目安250×180×20mm・250gを超える');
 const known=policy.filters.known_big_brands.find(b=>norm(b)===norm(row.brand)||norm(row.brand).includes(norm(b)));
 if(known)return result('defer','major_brand_source','既定の大手ブランド商品だけを根拠に推薦しない: '+known);
 return PASS;
}
const STOP=/^(?:無地|汎用|セット|交換用|小型|軽量|防水|日本製|無添加|お試し|用|向け|用品|シート|ケース|カバー|粉末|パウダー|オイル)$/;
function ownMatches(item,ownNames){
 const words=[...new Set(String(item.kw).normalize('NFKC').split(/[\s・]+/).filter(w=>w.length>=2&&!STOP.test(w)))];
 if(!words.length)return [];
 const matches=ownNames.map(name=>({name,matched:words.filter(w=>norm(name).includes(norm(w)))})).filter(x=>x.matched.length);
 const frequencies=new Map();for(const x of matches)for(const w of x.matched)frequencies.set(w,(frequencies.get(w)||0)+1);
 // Prefer a specific object over a generic shared word such as cleaner.
 return matches.map(x=>({...x,score:x.matched.reduce((s,w)=>s+Math.log1p(ownNames.length/frequencies.get(w)),0)})).sort((a,b)=>b.score-a.score).slice(0,12).map(x=>x.name);
}
function candidateGate(item,pool,history=[]){
 const scope=scopeGate([item.kw,item.idea].join(' '));if(scope.status!=='pass')return scope;
 const term=policy.filters.commodity_terms.find(k=>norm(item.kw+' '+item.idea).includes(norm(k)));if(term)return result('exclude','known_commodity','既定NGの規格品・工具: '+term);
 if(history.some(h=>sameKeyword(h.kw,item.kw)&&(h.screened===true||h.decision)))return result('exclude','already_seen','既出・判定済みの同じKW。既存カードと判断を保持する');
 const sources=item.seed_asins.map(a=>pool.find(r=>r.asin===a)).filter(Boolean);
 if(!sources.length)return result('defer','source_missing','元商品との対応がない');
 const gates=sources.map(sourceGate);if(!gates.some(g=>g.status==='pass'))return gates.find(g=>g.status==='exclude')||gates[0];
 return PASS;
}
function filterSources(rows){const eligible=[],records=[];const counts={pass:0,exclude:0,defer:0},reasons={};for(const r of rows){const g=sourceGate(r);counts[g.status]++;reasons[g.code]=(reasons[g.code]||0)+1;if(g.status==='pass')eligible.push(r);else records.push({asin:r.asin,...g});}return {eligible,records,counts,reasons};}
module.exports={scopeGate,sourceGate,candidateGate,filterSources,ownMatches};
