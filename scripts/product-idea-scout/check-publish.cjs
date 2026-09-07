// 既存ライブラリの認証で公開状態を確認。設定値や商品明細は表示しない。
const path=require('node:path');
const root=process.env.SCOUT_HOME;
if(!root) throw new Error('SCOUT_HOME is required');
const env=require(path.join(root,'lib','keepa')).loadEnv();
if(!env.MIRROR_SYNC_KEY) throw new Error('Publish authentication is not configured');
(async()=>{
  const base=(env.PORTAL_URL || 'https://bfaith-portal.onrender.com').replace(/\/+$/,'');
  const res=await fetch(base+'/apps/product-scout/ingest/status',{
    headers:{'x-sync-key':env.MIRROR_SYNC_KEY},signal:AbortSignal.timeout(30000),redirect:'error'
  });
  if(!res.ok) throw new Error('Publish status HTTP '+res.status);
  console.log(JSON.stringify(await res.json(),null,2));
})().catch(e=>{console.error(e.message);process.exitCode=1});
