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
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK || 'https://chat.googleapis.com/v1/spaces/AAQAL5zHy-w/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=yER7IJx_9CkKhYnzzre0WcWuqfgXc1oh8ldR35k01zE';

async function notify(text) {
  try {
    await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
  } catch (e) { console.error('[通知エラー]', e.message); }
}

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

  // 1. products（代表商品コードをraw_ne_productsからJOIN）
  const products = db.prepare(`
    SELECT p.*, n.代表商品コード
    FROM m_products p
    LEFT JOIN raw_ne_products n ON p.商品コード = n.商品コード COLLATE NOCASE
  `).all();
  console.log(`[Sync→Render]   products: ${products.length}件`);

  // 2. set_components
  const set_components = db.prepare('SELECT * FROM m_set_components').all();
  console.log(`[Sync→Render]   set_components: ${set_components.length}件`);

  // 2b. sku_map
  const sku_map = db.prepare('SELECT * FROM sku_map').all();
  console.log(`[Sync→Render]   sku_map: ${sku_map.length}件`);

  // 2c. amazon_sku_fees（手数料キャッシュ）
  let amazon_sku_fees = [];
  try {
    amazon_sku_fees = db.prepare('SELECT * FROM amazon_sku_fees').all();
    console.log(`[Sync→Render]   amazon_sku_fees: ${amazon_sku_fees.length}件`);
  } catch {
    console.log(`[Sync→Render]   amazon_sku_fees: テーブル未作成（スキップ）`);
  }

  // 2d. rakuten_sku_map（楽天AM/AL/W→NE商品コード マッピング）
  let rakuten_sku_map = [];
  try {
    rakuten_sku_map = db.prepare('SELECT rakuten_code, ne_code, source, updated_at FROM f_rakuten_sku_map').all();
    console.log(`[Sync→Render]   rakuten_sku_map: ${rakuten_sku_map.length}件`);
  } catch {
    console.log(`[Sync→Render]   rakuten_sku_map: テーブル未作成（スキップ）`);
  }

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

  // 分割送信（各パートを個別にPOST、8MB以下に収める）
  const headers = { 'Content-Type': 'application/json' };
  if (SYNC_KEY) headers['x-sync-key'] = SYNC_KEY;

  async function sendPart(data, label) {
    const json = JSON.stringify(data);
    const sizeMB = (json.length / 1024 / 1024).toFixed(1);
    console.log(`[Sync→Render]   送信: ${label} (${sizeMB}MB)`);
    const response = await fetch(`${RENDER_URL}/api/sync`, {
      method: 'POST', headers, body: json,
      signal: AbortSignal.timeout(120000),
    });
    if (!response.ok) {
      const err = await response.text().catch(() => '');
      throw new Error(`${label}: HTTP ${response.status} ${err.slice(0, 200)}`);
    }
    return response.json();
  }

  try {
    // Part 1: マスタデータ
    await sendPart({ products, set_components, sku_map, amazon_sku_fees, rakuten_sku_map }, 'マスタ');

    // Part 2: 月次集計（チャンク分割、9MB以下に収める）
    const monthlyChunkSize = 20000;
    for (let i = 0; i < sales_monthly.length; i += monthlyChunkSize) {
      const chunk = sales_monthly.slice(i, i + monthlyChunkSize);
      const isFirst = i === 0;
      await sendPart(
        { sales_monthly: chunk, meta: isFirst ? { clear_monthly: true } : undefined },
        `月次 ${i + 1}-${Math.min(i + monthlyChunkSize, sales_monthly.length)}`
      );
    }

    // Part 3: 日次集計（チャンク分割）
    const dailyChunkSize = 20000;
    for (let i = 0; i < sales_daily.length; i += dailyChunkSize) {
      const chunk = sales_daily.slice(i, i + dailyChunkSize);
      const isFirst = i === 0;
      await sendPart(
        { sales_daily: chunk, meta: isFirst ? { clear_daily: true } : undefined },
        `日次 ${i + 1}-${Math.min(i + dailyChunkSize, sales_daily.length)}`
      );
    }

    // Part 4: 最終メタデータ
    await sendPart({
      meta: { source: 'minipc', synced_at: ts, products_count: products.length,
        sales_monthly_count: sales_monthly.length, sales_daily_count: sales_daily.length }
    }, 'メタデータ');

    // Part 5: Render側のデータ件数を検証
    console.log('[Sync→Render]   検証中...');
    const statusRes = await fetch(`${RENDER_URL}/api/status`, { signal: AbortSignal.timeout(30000) });
    const status = await statusRes.json();

    const verify = {
      products: { sent: products.length, received: status.products_count || 0 },
      monthly: { sent: sales_monthly.length, received: status.sales_monthly_count || 0 },
      daily: { sent: sales_daily.length, received: status.sales_daily_count || 0 },
    };

    const allMatch = verify.products.sent === verify.products.received
      && verify.monthly.sent === verify.monthly.received
      && verify.daily.sent === verify.daily.received;

    if (allMatch) {
      console.log(`[Sync→Render] ✅ 検証OK — 全データ一致`);
      await notify(`✅ *Render同期完了*\n商品マスタ: ${verify.products.received}件\n月次集計: ${verify.monthly.received}件\n日次集計: ${verify.daily.received}件\n同期時刻: ${ts}`);
    } else {
      console.log(`[Sync→Render] ⚠️ 検証NG — データ不一致`);
      console.log(`  products: 送信${verify.products.sent} / 受信${verify.products.received}`);
      console.log(`  monthly: 送信${verify.monthly.sent} / 受信${verify.monthly.received}`);
      console.log(`  daily: 送信${verify.daily.sent} / 受信${verify.daily.received}`);
      await notify(`⚠️ *Render同期 データ不一致*\n商品: ${verify.products.sent}→${verify.products.received}\n月次: ${verify.monthly.sent}→${verify.monthly.received}\n日次: ${verify.daily.sent}→${verify.daily.received}`);
    }

    return { ok: true, verify };
  } catch (e) {
    console.error(`[Sync→Render] ❌ 送信失敗:`, e.message);
    await notify(`❌ *Render同期失敗*\n${e.message.slice(0, 200)}`);
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
