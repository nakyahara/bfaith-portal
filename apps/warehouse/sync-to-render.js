/**
 * sync-to-render.js — ミニPCからRenderにミラーデータを送信
 *
 * 送信データ:
 *   - m_products（全件、約7,000件）
 *   - m_set_components（全件、約2,500件）
 *   - f_sales_by_product 月次集計（24ヶ月分）
 *   - f_sales_by_listing 月次集計（24ヶ月分）
 *   - f_sales_by_product 日次集計（直近90日分）
 *   - f_sales_by_listing 日次集計（直近90日分）
 *
 * daily-sync.js から呼び出す or 単体実行可能。
 */
import { getDB } from './db.js';

const RENDER_URL = process.env.RENDER_MIRROR_URL || 'https://bfaith-portal.onrender.com/apps/mirror';
const SYNC_KEY = process.env.MIRROR_SYNC_KEY || '';

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

export async function syncToRender() {
  const db = getDB();
  const ts = now();
  console.log('[Sync→Render] 開始...');

  // 日付計算
  const today = new Date();
  const months24ago = new Date(today);
  months24ago.setMonth(months24ago.getMonth() - 24);
  const months24agoStr = months24ago.toISOString().slice(0, 7); // YYYY-MM

  const days90ago = new Date(today);
  days90ago.setDate(days90ago.getDate() - 90);
  const days90agoStr = days90ago.toISOString().slice(0, 10); // YYYY-MM-DD

  // 1. products
  const products = db.prepare('SELECT * FROM m_products').all();
  console.log(`[Sync→Render]   products: ${products.length}件`);

  // 2. set_components
  const set_components = db.prepare('SELECT * FROM m_set_components').all();
  console.log(`[Sync→Render]   set_components: ${set_components.length}件`);

  // 3. sales_monthly（24ヶ月分、by_product + by_listing）
  const salesMonthlyProduct = db.prepare(`
    SELECT SUBSTR(日付, 1, 7) as 月, 商品コード, モール, MAX(商品名) as 商品名,
      SUM(数量) as 数量, SUM(直接販売数) as 直接販売数, SUM(セット経由数) as セット経由数,
      NULL as 売上金額, NULL as 注文数, 'by_product' as データ種別, '' as チャネル
    FROM f_sales_by_product
    WHERE SUBSTR(日付, 1, 7) >= ?
    GROUP BY 月, 商品コード, モール
  `).all(months24agoStr);

  const salesMonthlyListing = db.prepare(`
    SELECT 月, モール商品コード as 商品コード, モール, MAX(商品名) as 商品名,
      SUM(数量) as 数量, 0 as 直接販売数, 0 as セット経由数,
      SUM(売上金額) as 売上金額, SUM(注文数) as 注文数, 'by_listing' as データ種別, チャネル
    FROM f_sales_by_listing
    WHERE 月 >= ?
    GROUP BY 月, モール商品コード, モール, チャネル
  `).all(months24agoStr);

  const sales_monthly = [...salesMonthlyProduct, ...salesMonthlyListing];
  console.log(`[Sync→Render]   sales_monthly: ${sales_monthly.length}件 (product: ${salesMonthlyProduct.length}, listing: ${salesMonthlyListing.length})`);

  // 4. sales_daily（直近90日、by_product + by_listing）
  const salesDailyProduct = db.prepare(`
    SELECT 日付, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
      NULL as 売上金額, NULL as 注文数, 'by_product' as データ種別, '' as チャネル
    FROM f_sales_by_product
    WHERE 日付 >= ?
  `).all(days90agoStr);

  const salesDailyListing = db.prepare(`
    SELECT 日付, モール商品コード as 商品コード, モール, 商品名, 数量,
      0 as 直接販売数, 0 as セット経由数,
      売上金額, 注文数, 'by_listing' as データ種別, チャネル
    FROM f_sales_by_listing
    WHERE 日付 >= ?
  `).all(days90agoStr);

  const sales_daily = [...salesDailyProduct, ...salesDailyListing];
  console.log(`[Sync→Render]   sales_daily: ${sales_daily.length}件 (product: ${salesDailyProduct.length}, listing: ${salesDailyListing.length})`);

  // 送信
  const payload = {
    products,
    set_components,
    sales_monthly,
    sales_daily,
    meta: {
      source: 'minipc',
      synced_at: ts,
      products_count: products.length,
      sales_monthly_count: sales_monthly.length,
      sales_daily_count: sales_daily.length,
    }
  };

  const payloadJson = JSON.stringify(payload);
  console.log(`[Sync→Render]   payload size: ${(payloadJson.length / 1024 / 1024).toFixed(1)}MB`);

  const headers = { 'Content-Type': 'application/json' };
  if (SYNC_KEY) headers['x-sync-key'] = SYNC_KEY;

  try {
    const response = await fetch(`${RENDER_URL}/api/sync`, {
      method: 'POST',
      headers,
      body: payloadJson,
      signal: AbortSignal.timeout(120000), // 2分タイムアウト
    });

    if (!response.ok) {
      const err = await response.text();
      throw new Error(`HTTP ${response.status}: ${err}`);
    }

    const result = await response.json();
    console.log(`[Sync→Render] ✅ 完了:`, result.log?.join(', ') || 'OK');
    return { ok: true, ...result };
  } catch (e) {
    console.error(`[Sync→Render] ❌ 送信失敗:`, e.message);
    return { ok: false, error: e.message };
  }
}

// 単体実行
import { initDB } from './db.js';
const isMain = process.argv[1]?.includes('sync-to-render');
if (isMain) {
  await initDB();
  const result = await syncToRender();
  console.log('\n結果:', JSON.stringify(result, null, 2));
  process.exit(result.ok ? 0 : 1);
}
