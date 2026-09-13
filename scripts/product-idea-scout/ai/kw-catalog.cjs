'use strict';
// Keep own-product evidence (class 1) separate from existing handled products (class 2).
// Names only: do not pass cost, sales, supplier codes, or other ledger fields to the models.
function catalogNames(value){
 const own=new Set(),handled=new Set();
 for(const f of value?.families||[]){
  if(f.salesClass!==1&&f.salesClass!==2)continue;
  const target=f.salesClass===1?own:handled;
  const names=[f.familyKey,...(f.products||[]).map(p=>p.name)];
  for(const name of names){if(typeof name!=='string'||!name.trim())continue;target.add(name.replace(/^AMC参考:\s*/,'').trim());}
 }
 return {ownNames:[...own],handledNames:[...handled]};
}
module.exports={catalogNames};
