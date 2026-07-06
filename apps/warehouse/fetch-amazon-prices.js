/**
 * fetch-amazon-prices.js — Amazon カート(Buy Box)価格 日次スナップショット (amazon-dashboard PR-D)
 *
 * SP-API Product Pricing v0 で
 *   1. getPricing (ItemType=Sku, batch 20)          → 自分の出品価格 (landed, 税込)
 *   2. getCompetitivePricing (ItemType=Asin, batch 20) → カート価格 (CompetitivePriceId='1') + 自社保有か
 * を取得し fact_amazon_price_snapshot (日次×SKU) に UPSERT する。
 *
 * 対象 SKU: amazon_sku_fees キャッシュ (asin あり、fetch-amazon-fees.js が daily 差分更新している
 * アクティブ SKU 集合) を流用。レートは fees と同じ restore_rate=0.5 RPS 前提で 2.1s/batch。
 * 全 ~2,500 SKU で 2 パス ≈ 9 分想定。日次 1 回 (リアルタイム追従はプライスターの領域、表示・検知のみ)。
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-prices.js               (全対象)
 *   node apps/warehouse/fetch-amazon-prices.js --limit 40    (先頭Nのみ、初回動作確認用)
 *   node apps/warehouse/fetch-amazon-prices.js --dry-run     (対象列挙のみ、API呼ばない)
 */
import 'dotenv/config';
import SellingPartner from 'amazon-sp-api';
import { initDB, getDB } from './db.js';

let spClient = null;
function getClient() {
  if (!spClient) {
    spClient = new SellingPartner({
      region: 'fe',
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return spClient;
}

const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
const BATCH_SIZE = 20;           // Product Pricing v0 の items 上限
const BATCH_SLEEP_MS = 2100;     // restore_rate=0.5 RPS + 100ms 余裕 (fees と同じ)
const MAX_RETRIES = 3;

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const limitN = parseInt(getArg('--limit'), 10) || 0;
const isDryRun = args.includes('--dry-run');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function jstToday() { return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10); }

async function callWithRetry(sp, params, label) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await sp.callAPI(params);
    } catch (e) {
      const isThrottle = /QuotaExceeded|429|throttl/i.test(e.message || '');
      if (attempt === MAX_RETRIES) throw e;
      const wait = isThrottle ? BATCH_SLEEP_MS * 2 * attempt : 1500 * attempt;
      console.warn(`  ⚠ ${label} attempt ${attempt} failed (${(e.message || '').slice(0, 120)}), retry in ${wait}ms`);
      await sleep(wait);
    }
  }
}

// getPricing (Sku): 自分の出品の landed price (税込)。FBA/FBM 複数 offer は最安を採用
function extractMyPrice(product) {
  const offers = product?.Offers || [];
  let min = null;
  for (const o of offers) {
    const landed = o?.BuyingPrice?.LandedPrice?.Amount;
    if (typeof landed === 'number' && (min === null || landed < min)) min = landed;
  }
  return min;
}

// getCompetitivePricing (Asin): CompetitivePriceId='1' = New のカート価格
function extractBuybox(product) {
  const prices = product?.CompetitivePricing?.CompetitivePrices || [];
  const bb = prices.find(p => String(p.CompetitivePriceId) === '1' && (p.condition || 'New') === 'New')
          || prices.find(p => String(p.CompetitivePriceId) === '1');
  if (!bb) return { price: null, isMine: null };
  // belongsToRequester はモデル上 optional。未返却を「他社」に落とすと
  // buybox_lost が大量 false positive になるため、未知は NULL (Codex R1 High #1)
  return {
    price: bb?.Price?.LandedPrice?.Amount ?? bb?.Price?.ListingPrice?.Amount ?? null,
    isMine: bb?.belongsToRequester === true ? 1 : (bb?.belongsToRequester === false ? 0 : null),
  };
}

async function main() {
  await initDB();
  const db = getDB();

  db.exec(`CREATE TABLE IF NOT EXISTS fact_amazon_price_snapshot (
    date_jst        TEXT NOT NULL,
    seller_sku      TEXT NOT NULL,
    asin            TEXT NOT NULL,
    channel         TEXT,
    my_price        REAL,              -- 自分の landed price (税込)。offer 無し = NULL
    buybox_price    REAL,              -- カート価格 (landed、税込)。カート不在 = NULL
    buybox_is_mine  INTEGER,           -- 1=自社保有 / 0=他社 / NULL=カート不在
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (date_jst, seller_sku)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_faps_sku ON fact_amazon_price_snapshot(seller_sku)`);

  // 対象: fees キャッシュのアクティブ SKU (asin 必須)
  let targets = db.prepare(`
    SELECT seller_sku, asin, fulfillment_channel AS channel
    FROM amazon_sku_fees
    WHERE asin IS NOT NULL AND asin != ''
    ORDER BY seller_sku
  `).all();
  if (limitN > 0) targets = targets.slice(0, limitN);
  console.log(`[Prices] 対象 ${targets.length} SKU (distinct ASIN ${new Set(targets.map(t => t.asin)).size})`);
  if (targets.length === 0) { console.log('[Prices] 対象なし、終了'); return; }
  if (isDryRun) { console.log('[Prices] --dry-run のため API 呼び出しなし'); return; }

  const sp = getClient();
  const today = jstToday();
  const fetchedAt = new Date().toISOString();

  // 失敗 batch の SKU/ASIN は UPSERT 対象外にする (成功扱いで NULL 上書きすると
  // 再実行時に当日データを破壊するため、Codex R1 High #2)
  const failedSkus = new Set();
  const failedAsins = new Set();

  // ── パス1: 自分の価格 (Sku batch) ──
  const myPriceMap = new Map();
  let priceErrors = 0;
  for (let i = 0; i < targets.length; i += BATCH_SIZE) {
    const batch = targets.slice(i, i + BATCH_SIZE);
    try {
      const res = await callWithRetry(sp, {
        operation: 'getPricing',
        endpoint: 'productPricing',
        query: {
          MarketplaceId: MARKETPLACE_ID,
          Skus: batch.map(t => t.seller_sku),
          ItemType: 'Sku',
        },
      }, `getPricing[${i / BATCH_SIZE}]`);
      for (const r of (Array.isArray(res) ? res : [])) {
        const sku = r?.SellerSKU;
        if (!sku) continue;
        if (r.status === 'Success' || r.Product) myPriceMap.set(sku, extractMyPrice(r.Product));
      }
    } catch (e) {
      priceErrors++;
      for (const t of batch) failedSkus.add(t.seller_sku);
      console.error(`  ✗ getPricing batch ${i / BATCH_SIZE}: ${(e.message || '').slice(0, 150)}`);
    }
    if ((i / BATCH_SIZE) % 20 === 0) console.log(`  [my-price] ${Math.min(i + BATCH_SIZE, targets.length)}/${targets.length}`);
    await sleep(BATCH_SLEEP_MS);
  }

  // ── パス2: カート価格 (Asin batch、distinct) ──
  const asins = [...new Set(targets.map(t => t.asin))];
  const buyboxMap = new Map();
  let bbErrors = 0;
  for (let i = 0; i < asins.length; i += BATCH_SIZE) {
    const batch = asins.slice(i, i + BATCH_SIZE);
    try {
      const res = await callWithRetry(sp, {
        operation: 'getCompetitivePricing',
        endpoint: 'productPricing',
        query: {
          MarketplaceId: MARKETPLACE_ID,
          Asins: batch,
          ItemType: 'Asin',
        },
      }, `getCompetitivePricing[${i / BATCH_SIZE}]`);
      for (const r of (Array.isArray(res) ? res : [])) {
        const asin = r?.ASIN;
        if (!asin) continue;
        buyboxMap.set(asin, extractBuybox(r.Product));
      }
    } catch (e) {
      bbErrors++;
      for (const asin of batch) failedAsins.add(asin);
      console.error(`  ✗ getCompetitivePricing batch ${i / BATCH_SIZE}: ${(e.message || '').slice(0, 150)}`);
    }
    if ((i / BATCH_SIZE) % 20 === 0) console.log(`  [buybox] ${Math.min(i + BATCH_SIZE, asins.length)}/${asins.length}`);
    await sleep(BATCH_SLEEP_MS);
  }

  // ── UPSERT + 90日より古い snapshot を掃除 ──
  const upsert = db.prepare(`
    INSERT INTO fact_amazon_price_snapshot (date_jst, seller_sku, asin, channel, my_price, buybox_price, buybox_is_mine, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date_jst, seller_sku) DO UPDATE SET
      asin = excluded.asin, channel = excluded.channel,
      my_price = excluded.my_price, buybox_price = excluded.buybox_price,
      buybox_is_mine = excluded.buybox_is_mine, fetched_at = excluded.fetched_at
  `);
  let saved = 0;
  let skipped = 0;
  const tx = db.transaction(() => {
    for (const t of targets) {
      // どちらかのパスで batch 失敗した SKU は書かない (前回値を NULL で潰さない)
      if (failedSkus.has(t.seller_sku) || failedAsins.has(t.asin)) { skipped++; continue; }
      const bb = buyboxMap.get(t.asin) || { price: null, isMine: null };
      upsert.run(today, t.seller_sku, t.asin, t.channel || null,
        myPriceMap.get(t.seller_sku) ?? null, bb.price, bb.isMine, fetchedAt);
      saved++;
    }
    const pruneBefore = new Date(Date.now() + 9 * 3600 * 1000 - 90 * 86400000).toISOString().slice(0, 10);
    db.prepare(`DELETE FROM fact_amazon_price_snapshot WHERE date_jst < ?`).run(pruneBefore);
  });
  tx();

  const withBb = targets.filter(t => (buyboxMap.get(t.asin) || {}).price != null).length;
  const mineCnt = targets.filter(t => (buyboxMap.get(t.asin) || {}).isMine === 1).length;
  const totalBatches = Math.ceil(targets.length / BATCH_SIZE) + Math.ceil(asins.length / BATCH_SIZE);
  console.log(`[Prices] ✓ ${saved} SKU 保存 / ${skipped} SKU skip (batch失敗) (${today}) / カートあり ${withBb} / 自社カート保有 ${mineCnt} / batchエラー price=${priceErrors} bb=${bbErrors}/${totalBatches}`);
  // batch エラー率 10% 超は失敗扱い (通知に載せ、daily-sync の sync gate を止める)。
  // 少数の flaky batch は skip 済みなので成功扱いで sync に進めてよい
  if ((priceErrors + bbErrors) > totalBatches * 0.1) {
    console.error(`[Prices] FATAL: batch エラー率が閾値超過 (${priceErrors + bbErrors}/${totalBatches})`);
    process.exit(1);
  }
}

main().then(() => process.exit(0)).catch(e => {
  console.error(`[Prices] FATAL: ${e.message}`);
  process.exit(1);
});
