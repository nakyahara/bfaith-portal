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
const BATCH_SIZE = 20; // SP-API getMyFeesEstimates の上限
const DELAY_BETWEEN_BATCHES_MS = 2000; // 429回避用
const MAX_RETRIES = 3;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

// ─── 対象SKU取得 ───

/**
 * 全アクティブSKU（sku_map + raw_sp_orders から取引実績あり）
 */
function getAllActiveSkus(db) {
  // sku_mapの全SKUに対して、raw_sp_ordersから最新の価格・チャネルを取得
  // COLLATE NOCASEを避けてインデックスを効かせる
  return db.prepare(`
    SELECT sm.seller_sku, sm.asin,
      COALESCE(sub.channel, 'FBA') as channel,
      COALESCE(sub.last_price, 0) as last_price
    FROM (SELECT DISTINCT seller_sku, asin FROM sku_map) sm
    LEFT JOIN (
      SELECT seller_sku,
        CASE WHEN fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END as channel,
        MAX(item_price / NULLIF(quantity, 0)) as last_price
      FROM raw_sp_orders
      WHERE order_status NOT IN ('Cancelled') AND item_price > 0 AND quantity > 0
      GROUP BY seller_sku
    ) sub ON sm.seller_sku = sub.seller_sku
    WHERE sub.last_price > 0
  `).all();
}

/**
 * 直近N日で売れたSKUのみ
 */
function getRecentSkus(db, days = 30) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  // 直近N日で注文があったSKUのみ取得（インデックス活用）
  return db.prepare(`
    SELECT sm.seller_sku, sm.asin,
      COALESCE(sub.channel, 'FBA') as channel,
      COALESCE(sub.last_price, 0) as last_price
    FROM (SELECT DISTINCT seller_sku, asin FROM sku_map) sm
    INNER JOIN (
      SELECT seller_sku,
        CASE WHEN fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END as channel,
        MAX(item_price / NULLIF(quantity, 0)) as last_price
      FROM raw_sp_orders
      WHERE order_status NOT IN ('Cancelled') AND purchase_date >= ? AND item_price > 0 AND quantity > 0
      GROUP BY seller_sku
    ) sub ON sm.seller_sku = sub.seller_sku
  `).all(cutoffStr);
}

/**
 * 特定SKU
 */
function getSpecificSku(db, sku) {
  // sku_mapから取得
  const smRow = db.prepare('SELECT seller_sku, asin FROM sku_map WHERE seller_sku = ? LIMIT 1').get(sku);

  // raw_sp_ordersから最新価格・チャネル
  const order = db.prepare(`
    SELECT seller_sku, asin,
      CASE WHEN fulfillment_channel = 'Amazon' THEN 'FBA' ELSE 'FBM' END as channel,
      MAX(item_price / NULLIF(quantity, 0)) as last_price
    FROM raw_sp_orders
    WHERE seller_sku = ? AND order_status NOT IN ('Cancelled')
    GROUP BY seller_sku
  `).get(sku);

  if (smRow) {
    return [{
      seller_sku: smRow.seller_sku,
      asin: smRow.asin || order?.asin,
      channel: order?.channel || 'FBA',
      last_price: order?.last_price || 0,
    }];
  }
  return order ? [order] : [];
}

// ─── SP-API バッチ手数料取得 ───

/**
 * バッチ単位で手数料を取得（最大20件/リクエスト）
 */
async function fetchFeesBatch(items) {
  const sp = getClient();

  const feesEstimateRequests = items.map(item => ({
    MarketplaceId: MARKETPLACE_ID,
    IdType: 'SellerSKU',
    SellerId: process.env.SP_API_SELLER_ID || 'A6HMLHKUUJC27',
    IdValue: item.seller_sku,
    IsAmazonFulfilled: item.channel === 'FBA',
    PriceToEstimateFees: {
      ListingPrice: {
        CurrencyCode: 'JPY',
        Amount: item.last_price || 1000, // 価格不明時はフォールバック
      },
      Shipping: { CurrencyCode: 'JPY', Amount: 0 },
    },
    Identifier: `${item.channel}-${item.seller_sku}`,
  }));

  const result = await sp.callAPI({
    operation: 'getMyFeesEstimates',
    endpoint: 'productFees',
    body: feesEstimateRequests,
  });

  return result;
}

/**
 * 単一SKUのフォールバック取得（ASIN指定）
 */
async function fetchFeesSingle(item) {
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
        Identifier: `${item.channel}-${item.asin}`,
      },
    },
  });

  const r = result.FeesEstimateResult;
  if (r.Status !== 'Success') return null;

  const feeList = r.FeesEstimate.FeeDetailList;
  return {
    referralFee: feeList.find(f => f.FeeType === 'ReferralFee')?.FeeAmount?.Amount || 0,
    fbaFee: feeList.find(f => f.FeeType === 'FBAFees')?.FeeAmount?.Amount || 0,
    variableClosingFee: feeList.find(f => f.FeeType === 'VariableClosingFee')?.FeeAmount?.Amount || 0,
    perItemFee: feeList.find(f => f.FeeType === 'PerItemFee')?.FeeAmount?.Amount || 0,
    totalFee: r.FeesEstimate.TotalFeesEstimate.Amount,
    referralFeeRate: feeList.find(f => f.FeeType === 'ReferralFee')?.FeePromotion?.Amount
      ? undefined
      : (feeList.find(f => f.FeeType === 'ReferralFee')?.FeeAmount?.Amount || 0) / (item.last_price || 1),
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

  console.log(`[FetchFees] モード: ${mode}, 対象SKU: ${targetSkus.length}件`);

  if (targetSkus.length === 0) {
    console.log('[FetchFees] 対象SKUなし。終了。');
    return { total: 0, success: 0, failed: 0, errors: [] };
  }

  // バッチに分割
  const batches = [];
  for (let i = 0; i < targetSkus.length; i += BATCH_SIZE) {
    batches.push(targetSkus.slice(i, i + BATCH_SIZE));
  }

  console.log(`[FetchFees] ${batches.length}バッチ（各${BATCH_SIZE}件）で取得開始`);

  let totalSuccess = 0;
  let totalFailed = 0;
  const errors = [];

  for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
    const batch = batches[batchIdx];
    const progress = `[${batchIdx + 1}/${batches.length}]`;

    for (let retry = 0; retry < MAX_RETRIES; retry++) {
      try {
        const results = await fetchFeesBatch(batch);

        // レスポンス解析
        const saveRows = [];
        for (let i = 0; i < results.length; i++) {
          const est = results[i];
          const item = batch[i];

          if (est.Status === 'Success' && est.FeesEstimateResult?.FeesEstimate) {
            const feeList = est.FeesEstimateResult.FeesEstimate.FeeDetailList || [];
            const referralFee = feeList.find(f => f.FeeType === 'ReferralFee')?.FeeAmount?.Amount || 0;
            saveRows.push({
              seller_sku: item.seller_sku,
              asin: item.asin,
              channel: item.channel,
              referralFee,
              referralFeeRate: item.last_price > 0 ? referralFee / item.last_price : null,
              fbaFee: feeList.find(f => f.FeeType === 'FBAFees')?.FeeAmount?.Amount || 0,
              variableClosingFee: feeList.find(f => f.FeeType === 'VariableClosingFee')?.FeeAmount?.Amount || 0,
              perItemFee: feeList.find(f => f.FeeType === 'PerItemFee')?.FeeAmount?.Amount || 0,
              totalFee: est.FeesEstimateResult.FeesEstimate.TotalFeesEstimate?.Amount || 0,
              price_used: item.last_price,
            });
          } else {
            // バッチ失敗時はASIN指定で個別リトライ
            if (item.asin) {
              try {
                const single = await fetchFeesSingle(item);
                if (single) {
                  saveRows.push({
                    seller_sku: item.seller_sku,
                    asin: item.asin,
                    channel: item.channel,
                    ...single,
                    referralFeeRate: single.referralFeeRate ?? (item.last_price > 0 ? single.referralFee / item.last_price : null),
                    price_used: item.last_price,
                  });
                } else {
                  totalFailed++;
                  errors.push({ sku: item.seller_sku, error: 'Single fetch returned null' });
                }
              } catch (e) {
                totalFailed++;
                errors.push({ sku: item.seller_sku, error: e.message });
              }
            } else {
              totalFailed++;
              const errMsg = est.FeesEstimateResult?.Error?.Message || 'Unknown error';
              errors.push({ sku: item.seller_sku, error: errMsg });
            }
          }
        }

        if (saveRows.length > 0) {
          saveFees(db, saveRows);
          totalSuccess += saveRows.length;
        }

        console.log(`${progress} ${saveRows.length}件保存 (失敗: ${batch.length - saveRows.length}件)`);
        break; // 成功したらリトライ不要

      } catch (e) {
        if (e.statusCode === 429 || e.code === 'QuotaExceeded') {
          const waitMs = DELAY_BETWEEN_BATCHES_MS * (retry + 2);
          console.log(`${progress} 429 Rate Limited → ${waitMs}ms 待機後リトライ (${retry + 1}/${MAX_RETRIES})`);
          await sleep(waitMs);
          continue;
        }
        if (retry < MAX_RETRIES - 1) {
          console.log(`${progress} エラー: ${e.message} → リトライ (${retry + 1}/${MAX_RETRIES})`);
          await sleep(DELAY_BETWEEN_BATCHES_MS);
          continue;
        }
        console.error(`${progress} 最終失敗:`, e.message);
        totalFailed += batch.length;
        for (const item of batch) {
          errors.push({ sku: item.seller_sku, error: e.message });
        }
      }
    }

    // バッチ間のウェイト
    if (batchIdx < batches.length - 1) {
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
