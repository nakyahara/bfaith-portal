/**
 * fetch-amazon-fees.js — Amazon SKU手数料一括取得バッチ
 *
 * SP-API getMyFeesEstimates でFBA/FBM手数料をバッチ取得し、
 * warehouse.db の amazon_sku_fees テーブルに保存する。
 *
 * 更新モード:
 *   --full       全アクティブSKUを対象（月1回想定）
 *   --recent N   直近N日で売れたSKUのみ（デフォルト30日、週1想定）
 *   --sku SKU    特定SKUのみ（新規SKU登場時）
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-fees.js --full
 *   node apps/warehouse/fetch-amazon-fees.js --recent 30
 *   node apps/warehouse/fetch-amazon-fees.js --sku SOME-SKU-001
 *
 * daily-sync.js からも呼び出し可能（週次/月次スケジュール）
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
const DELAY_BETWEEN_BATCHES_MS = 1000; // 429回避用（1件/秒ペース）
const MAX_RETRIES = 3;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

// ─── 対象SKU取得 ───
// SKU管理統合 Step 3c (2026-05-04): sku_map 起点を廃止し m_sku_master + raw_sp_orders 起点へ
// 3問題分解 (Codex 2-3周目指摘):
//   1. 候補列挙   : m_sku_master ∪ raw_sp_orders 由来の active SKU
//   2. ASIN取得    : raw_sp_orders.asin > amazon_sku_fees.asin (キャッシュ) > スキップ
//   3. 価格取得    : raw_sp_orders MAX(item_price/quantity) > amazon_sku_fees.price_used > スキップ

/**
 * 全アクティブSKU (m_sku_master + raw_sp_orders 由来)
 * - last_price > 0 のもののみ (SP-API に price 渡せないと推定APIが無意味)
 */
function getAllActiveSkus(db) {
  // orders_repr: SKU毎に「最新の有効注文1行」を代表として採用
  //   → asin/channel/price が同じ注文由来で一貫することを保証 (Codex Medium #1 対応)
  return db.prepare(`
    WITH active AS (
      SELECT seller_sku FROM m_sku_master
      UNION
      SELECT seller_sku FROM raw_sp_orders
      WHERE order_status NOT IN ('Cancelled') AND seller_sku IS NOT NULL AND seller_sku != ''
    ),
    orders_repr AS (
      SELECT seller_sku, asin, fulfillment_channel,
        item_price / NULLIF(quantity, 0) AS last_price
      FROM (
        SELECT seller_sku, asin, fulfillment_channel, item_price, quantity,
          ROW_NUMBER() OVER (PARTITION BY seller_sku ORDER BY purchase_date DESC, item_price DESC) AS rn
        FROM raw_sp_orders
        WHERE order_status NOT IN ('Cancelled') AND item_price > 0 AND quantity > 0
      ) WHERE rn = 1
    )
    SELECT a.seller_sku,
      COALESCE(o.asin, c.asin) AS asin,
      CASE
        WHEN o.fulfillment_channel = 'Amazon' THEN 'FBA'
        WHEN o.fulfillment_channel IS NOT NULL THEN 'FBM'
        WHEN c.fulfillment_channel IS NOT NULL THEN c.fulfillment_channel
        ELSE 'FBA'
      END AS channel,
      COALESCE(o.last_price, c.price_used, 0) AS last_price
    FROM active a
    LEFT JOIN orders_repr o ON a.seller_sku = o.seller_sku COLLATE NOCASE
    LEFT JOIN amazon_sku_fees c ON a.seller_sku = c.seller_sku COLLATE NOCASE
    WHERE COALESCE(o.last_price, c.price_used, 0) > 0
  `).all();
}

/**
 * 直近N日で売れたSKUのみ
 * - 過去N日に売上のあった SKU のみ列挙
 * - ASIN/価格は raw_sp_orders 優先、なければ amazon_sku_fees キャッシュ
 */
function getRecentSkus(db, days = 30) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  // orders_repr: 最新行を代表として採用 (Codex Medium #1 対応)
  return db.prepare(`
    WITH orders_repr AS (
      SELECT seller_sku, asin, fulfillment_channel,
        item_price / NULLIF(quantity, 0) AS last_price
      FROM (
        SELECT seller_sku, asin, fulfillment_channel, item_price, quantity,
          ROW_NUMBER() OVER (PARTITION BY seller_sku ORDER BY purchase_date DESC, item_price DESC) AS rn
        FROM raw_sp_orders
        WHERE order_status NOT IN ('Cancelled') AND purchase_date >= ?
          AND item_price > 0 AND quantity > 0
      ) WHERE rn = 1
    )
    SELECT o.seller_sku,
      COALESCE(o.asin, c.asin) AS asin,
      CASE WHEN o.fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END AS channel,
      o.last_price
    FROM orders_repr o
    LEFT JOIN amazon_sku_fees c ON o.seller_sku = c.seller_sku COLLATE NOCASE
  `).all(cutoffStr);
}

/**
 * 特定SKU
 */
function getSpecificSku(db, sku) {
  // 最新の注文行を代表として採用 (asin/channel/price 一貫性、Codex Medium #1 対応)
  const order = db.prepare(`
    SELECT seller_sku, asin, fulfillment_channel,
      item_price / NULLIF(quantity, 0) AS last_price
    FROM raw_sp_orders
    WHERE seller_sku = ? COLLATE NOCASE AND order_status NOT IN ('Cancelled')
      AND item_price > 0 AND quantity > 0
    ORDER BY purchase_date DESC, item_price DESC
    LIMIT 1
  `).get(sku);

  const masterRow = db.prepare(
    'SELECT seller_sku FROM m_sku_master WHERE seller_sku = ? COLLATE NOCASE'
  ).get(sku);

  // amazon_sku_fees キャッシュから fallback (ASIN・価格)
  const cache = db.prepare(
    'SELECT seller_sku, asin, fulfillment_channel, price_used FROM amazon_sku_fees WHERE seller_sku = ? COLLATE NOCASE'
  ).get(sku);

  // どこにも存在しなければ空
  if (!order && !masterRow && !cache) return [];

  // seller_sku は呼び出し時の入力をそのまま返す (Codex Low #2: 後方互換)
  return [{
    seller_sku: order?.seller_sku || cache?.seller_sku || sku,
    asin: order?.asin || cache?.asin || null,
    channel: order?.fulfillment_channel === 'Amazon' ? 'FBA'
      : order?.fulfillment_channel ? 'FBM'
      : cache?.fulfillment_channel || 'FBA',
    last_price: order?.last_price || cache?.price_used || 0,
  }];
}

// ─── SP-API 手数料取得 ───

/**
 * 単一ASIN指定で手数料取得（実績あるgetMyFeesEstimateForASINを使用）
 * profit-calculator/sp-api.js の getFees() と同じAPI
 */
async function fetchFeesSingle(item) {
  if (!item.asin) return null;

  const sp = getClient();

  const result = await sp.callAPI({
    operation: 'getMyFeesEstimateForASIN',
    endpoint: 'productFees',
    path: { Asin: item.asin },
    body: {
      FeesEstimateRequest: {
        MarketplaceId: MARKETPLACE_ID,
        IsAmazonFulfilled: item.channel === 'FBA',
        PriceToEstimateFees: {
          ListingPrice: { CurrencyCode: 'JPY', Amount: item.last_price || 1000 },
          Shipping: { CurrencyCode: 'JPY', Amount: 0 },
        },
        Identifier: `${item.channel}-${item.asin}-${Date.now()}`,
      },
    },
  });

  const r = result.FeesEstimateResult;
  if (r.Status !== 'Success') return null;

  const feeList = r.FeesEstimate.FeeDetailList;
  const referralFee = feeList.find(f => f.FeeType === 'ReferralFee')?.FeeAmount?.Amount || 0;
  return {
    referralFee,
    fbaFee: feeList.find(f => f.FeeType === 'FBAFees')?.FeeAmount?.Amount || 0,
    variableClosingFee: feeList.find(f => f.FeeType === 'VariableClosingFee')?.FeeAmount?.Amount || 0,
    perItemFee: feeList.find(f => f.FeeType === 'PerItemFee')?.FeeAmount?.Amount || 0,
    totalFee: r.FeesEstimate.TotalFeesEstimate.Amount,
    referralFeeRate: item.last_price > 0 ? referralFee / item.last_price : null,
  };
}

// ─── DB保存 ───

function saveFees(db, rows) {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO amazon_sku_fees
      (seller_sku, asin, fulfillment_channel, referral_fee, referral_fee_rate,
       fba_fee, variable_closing_fee, per_item_fee, total_fee, price_used, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const ts = now();
  const insertMany = db.transaction((rows) => {
    for (const r of rows) {
      stmt.run(
        r.seller_sku, r.asin, r.channel,
        r.referralFee, r.referralFeeRate,
        r.fbaFee, r.variableClosingFee, r.perItemFee, r.totalFee,
        r.price_used, ts
      );
    }
  });

  insertMany(rows);
  return rows.length;
}

// ─── メイン処理 ───

export async function fetchAmazonFees(mode = 'recent', param = 30) {
  const db = getDB();
  let targetSkus;

  switch (mode) {
    case 'full':
      targetSkus = getAllActiveSkus(db);
      break;
    case 'recent':
      targetSkus = getRecentSkus(db, param);
      break;
    case 'sku':
      targetSkus = getSpecificSku(db, param);
      break;
    default:
      throw new Error(`Unknown mode: ${mode}`);
  }

  // ASINがないSKUを除外
  const withAsin = targetSkus.filter(s => s.asin);
  const noAsin = targetSkus.filter(s => !s.asin);

  console.log(`[FetchFees] モード: ${mode}, 対象SKU: ${targetSkus.length}件 (ASIN有: ${withAsin.length}, ASIN無: ${noAsin.length})`);

  if (withAsin.length === 0) {
    console.log('[FetchFees] ASIN付きSKUなし。終了。');
    return { total: targetSkus.length, success: 0, failed: noAsin.length, errors: noAsin.map(s => ({ sku: s.seller_sku, error: 'No ASIN' })) };
  }

  // 1件ずつ取得（getMyFeesEstimateForASIN — 実績あり）
  const estimatedMin = Math.ceil(withAsin.length * DELAY_BETWEEN_BATCHES_MS / 60000);
  console.log(`[FetchFees] ${withAsin.length}件を1件ずつ取得開始（推定${estimatedMin}分）`);

  let totalSuccess = 0;
  let totalFailed = noAsin.length;
  const errors = noAsin.map(s => ({ sku: s.seller_sku, error: 'No ASIN' }));
  const saveBuffer = [];

  for (let i = 0; i < withAsin.length; i++) {
    const item = withAsin[i];
    const progress = `[${i + 1}/${withAsin.length}]`;

    for (let retry = 0; retry < MAX_RETRIES; retry++) {
      try {
        const fees = await fetchFeesSingle(item);
        if (fees) {
          saveBuffer.push({
            seller_sku: item.seller_sku,
            asin: item.asin,
            channel: item.channel,
            ...fees,
            price_used: item.last_price,
          });
          totalSuccess++;
        } else {
          totalFailed++;
          errors.push({ sku: item.seller_sku, error: 'API returned non-Success' });
        }
        break; // 成功
      } catch (e) {
        if (e.statusCode === 429 || e.code === 'QuotaExceeded') {
          const waitMs = DELAY_BETWEEN_BATCHES_MS * (retry + 2);
          console.log(`${progress} 429 Rate Limited → ${waitMs}ms 待機 (${retry + 1}/${MAX_RETRIES})`);
          await sleep(waitMs);
          continue;
        }
        if (retry < MAX_RETRIES - 1) {
          await sleep(DELAY_BETWEEN_BATCHES_MS);
          continue;
        }
        totalFailed++;
        errors.push({ sku: item.seller_sku, error: e.message });
      }
    }

    // 50件ごとにDB保存 + 進捗表示
    if (saveBuffer.length >= 50 || i === withAsin.length - 1) {
      if (saveBuffer.length > 0) {
        saveFees(db, saveBuffer);
        console.log(`${progress} ${totalSuccess}件成功 / ${totalFailed}件失敗（バッファ${saveBuffer.length}件保存）`);
        saveBuffer.length = 0;
      }
    }

    // API間隔（429回避）
    if (i < withAsin.length - 1) {
      await sleep(DELAY_BETWEEN_BATCHES_MS);
    }
  }

  const summary = {
    total: targetSkus.length,
    success: totalSuccess,
    failed: totalFailed,
    errors: errors.slice(0, 50), // 最大50件のエラーのみ保持
  };

  console.log(`[FetchFees] 完了: ${summary.success}件成功 / ${summary.failed}件失敗 / ${summary.total}件中`);
  return summary;
}

// ─── CLI実行 ───

const isMain = process.argv[1]?.includes('fetch-amazon-fees');
if (isMain) {
  await initDB();

  const args = process.argv.slice(2);
  let mode = 'recent';
  let param = 30;

  if (args.includes('--full')) {
    mode = 'full';
  } else if (args.includes('--recent')) {
    const idx = args.indexOf('--recent');
    param = parseInt(args[idx + 1]) || 30;
    mode = 'recent';
  } else if (args.includes('--sku')) {
    const idx = args.indexOf('--sku');
    param = args[idx + 1];
    if (!param) {
      console.error('--sku にはSKUを指定してください');
      process.exit(1);
    }
    mode = 'sku';
  }

  const result = await fetchAmazonFees(mode, param);
  console.log('\n結果:', JSON.stringify(result, null, 2));

  if (result.errors.length > 0) {
    console.log('\nエラー詳細:');
    for (const e of result.errors.slice(0, 10)) {
      console.log(`  ${e.sku}: ${e.error}`);
    }
  }

  process.exit(result.failed > 0 ? 1 : 0);
}
