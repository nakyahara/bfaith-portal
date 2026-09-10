'use strict';
const {requireValue:check}=require('./common.cjs');
function readWarehouse(Database,file,metadata=null) {
  const db=new Database(file,{readonly:true,fileMustExist:true});
  try {
    db.pragma('query_only=ON');
    const columns=db.pragma('table_info(m_products)').map(c=>c.name);
    const required=['product_id','商品コード','売上分類','標準売価','new_product_launch_date','updated_at'];
    check(required.every(c=>columns.includes(c)),'WAREHOUSE_SCHEMA_MISMATCH');
    const rows=db.prepare('SELECT product_id, 商品コード code, 標準売価 price, new_product_launch_date launched_on, updated_at effective_at FROM m_products WHERE 売上分類=1 ORDER BY product_id').all();
    const summary={source:'m_products',sales_class:1,total:rows.length,dated:rows.filter(r=>r.launched_on).length,recent:rows.filter(r=>r.launched_on>='2024-01-01').length,missing_semantic_columns:['family_id','use','target','material','form','spec'].filter(c=>!columns.includes(c))};
    if(!metadata)return {status:'METADATA_REQUIRED',summary};
    check(Array.isArray(metadata) && new Set(metadata.map(m=>String(m.product_id))).size===metadata.length,'INVALID_METADATA');
    const lookup=new Map(metadata.map(m=>[String(m.product_id),m]));
    const missing=rows.filter(r=>!lookup.has(String(r.product_id)) || !['family_id','use','target','material','form','spec'].every(k=>typeof lookup.get(String(r.product_id))[k]==='string' && lookup.get(String(r.product_id))[k].trim()));
    if(missing.length)return {status:'METADATA_REQUIRED',summary:{...summary,missing_metadata_count:missing.length}};
    const products=rows.map(r=>{const m=lookup.get(String(r.product_id));return {product_id:String(r.product_id),sales_class:1,price:r.price,launched_on:r.launched_on,effective_at:r.effective_at,...Object.fromEntries(['family_id','use','target','material','form','spec'].map(k=>[k,m[k]]))};});
    return {status:'READY',summary,products};
  } finally { db.close(); }
}
module.exports={readWarehouse};
if(require.main===module){let body='';process.stdin.setEncoding('utf8');process.stdin.on('data',s=>body+=s);process.stdin.on('end',()=>{try{const input=JSON.parse(body);const result=readWarehouse(require('better-sqlite3'),input.warehouse_db,input.metadata);console.log(JSON.stringify({status:result.status,summary:result.summary}));}catch(e){console.error(JSON.stringify({status:e.code||'WAREHOUSE_PROBE_FAILED'}));process.exitCode=1;}});}
