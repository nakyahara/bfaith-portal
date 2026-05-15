/**
 * fetch-amazon-fees.js — Amazon SKU 手数料一括取得バッチ (Phase 0 v2: batch API + TTL/差分更新)
 *
 * SP-API getMyFeesEstimates (バッチ最大20件/call、restore_rate=2 = 0.5 RPS) で
 * FBA/FBM 手数料を取得し、warehouse.db の amazon_sku_fees テーブルに保存する。
 *
 * v2 改修 (2026-05-15、Codex+Claude 議論結果):
 *   - 旧版は単発 API (1件1秒、1RPS) で 2,245 SKU = 37 分 → 30分タイムアウトで daily cron 落ち
 *   - v2 は batch API: 20件/call × 2sec sleep = 113 call × 2s = ~4分で完了 (理論値)
 *   - TTL/差分更新: 未キャッシュ + fetched_at > 7日 + asin/channel 変更 + 価格乖離 >20% のみ refresh
 *   - `amazon_sku_fees` は最新キャッシュ用途と割り切る (履歴は持たない、PK=seller_sku 1行)
 *
 * 更新モード:
 *   --full          全アクティブSKUを対象 (TTL/差分フィルタ後、月1 full sweep 用、推奨)
 *   --recent N      直近N日で売れたSKUのみ (デフォルト30、daily 差分更新用)
 *   --sku SKU       特定SKUのみ (新規SKU登場時、価格変更時)
 *   --force         TTL/差分フィルタを無効化、対象 SKU を強制再取得
 *
 * 設計判断 (Codex Round 1 with Claude):
 *   - amazon_sku_fees は estimate キャッシュ (近似値、速報粗利用)、Settlement 実額は別系統
 *   - daily cron は「差分のみ refresh」、monthly は full sweep、ad hoc は --sku/--force
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-fees.js --recent 30       (daily cron 既定)
 *   node apps/warehouse/fetch-amazon-fees.js --full --force    (月次 full refresh)
 *   node apps/warehouse/fetch-amazon-fees.js --sku SOME-SKU    (新規/特定 SKU)
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
const BATCH_SIZE = 20;                   // SP-API getMyFeesEstimates の batch 上限
const BATCH_SLEEP_MS = 2100;             // restore_rate=2 (0.5 RPS) + 100ms 余裕
const MAX_RETRIES = 3;
const TTL_HOURS = 7 * 24;                // キャッシュ有効期限 (Codex Round 1 #4: 整数切捨を避けるため hour 単位、7日 = 168時間)
const PRICE_DIFF_THRESHOLD = 0.20;       // 価格乖離 20% 超で refresh

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function now() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }

// ─── 対象SKU取得 (旧版と同一ロジック) ───

function getAllActiveSkus(db) {
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

function getRecentSkus(db, days = 30) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

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

function getSpecificSku(db, sku) {
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
  const cache = db.prepare(
    'SELECT seller_sku, asin, fulfillment_channel, price_used FROM amazon_sku_fees WHERE seller_sku = ? COLLATE NOCASE'
  ).get(sku);
  if (!order && !masterRow && !cache) return [];
  return [{
    seller_sku: order?.seller_sku || cache?.seller_sku || sku,
    asin: order?.asin || cache?.asin || null,
    channel: order?.fulfillment_channel === 'Amazon' ? 'FBA'
      : order?.fulfillment_channel ? 'FBM'
      : cache?.fulfillment_channel || 'FBA',
    last_price: order?.last_price || cache?.price_used || 0,
  }];
}

// ─── TTL/差分フィルタ (v2 新規) ───

/**
 * 対象 SKU から「実際に SP-API call が必要なもの」だけ抽出。
 * refresh 条件 (どれか 1 つでも該当):
 *   1. キャッシュ無し (新規 SKU)
 *   2. fetched_at > TTL_DAYS (デフォルト 7日)
 *   3. asin が変わった
 *   4. fulfillment_channel が変わった
 *   5. price_used と最新価格の乖離 > PRICE_DIFF_THRESHOLD (デフォルト 20%)
 */
function filterByTTLAndDiff(db, items) {
  if (items.length === 0) return { needRefresh: [], skipped: [], skipReasons: {} };

  // Codex Round 1 #4 反映: julianday の小数値 × 24 で hour 単位比較 (整数切捨で 7.9日が 7扱いされない)
  const cacheStmt = db.prepare(`
    SELECT seller_sku, asin, fulfillment_channel, price_used, fetched_at,
      (julianday('now') - julianday(fetched_at)) * 24.0 AS age_hours
    FROM amazon_sku_fees WHERE seller_sku = ? COLLATE NOCASE
  `);

  const needRefresh = [];
  const skipped = [];
  const skipReasons = { ttl_fresh: 0 };

  for (const item of items) {
    const cache = cacheStmt.get(item.seller_sku);
    if (!cache) {
      item.refresh_reason = 'not_cached';
      needRefresh.push(item);
      continue;
    }
    if (cache.age_hours >= TTL_HOURS) {
      item.refresh_reason = `ttl_${cache.age_hours.toFixed(1)}h`;
      needRefresh.push(item);
      continue;
    }
    if (item.asin && cache.asin && item.asin !== cache.asin) {
      item.refresh_reason = 'asin_changed';
      needRefresh.push(item);
      continue;
    }
    if (item.channel && cache.fulfillment_channel && item.channel !== cache.fulfillment_channel) {
      item.refresh_reason = 'channel_changed';
      needRefresh.push(item);
      continue;
    }
    if (item.last_price > 0 && cache.price_used > 0) {
      const diffRate = Math.abs(item.last_price - cache.price_used) / cache.price_used;
      if (diffRate > PRICE_DIFF_THRESHOLD) {
        item.refresh_reason = `price_diff_${(diffRate * 100).toFixed(0)}pct`;
        needRefresh.push(item);
        continue;
      }
    }
    skipped.push(item);
    skipReasons.ttl_fresh++;
  }

  return { needRefresh, skipped, skipReasons };
}

// ─── SP-API バッチ取得 (v2 メイン) ───

/**
 * getMyFeesEstimates (batch、最大20件/call) で 1 バッチ取得。
 * 各 item に Identifier を付与し、レスポンスを Identifier で seller_sku に紐付け。
 */
async function fetchFeesBatch(items) {
  if (items.length === 0) return { results: [], errors: [] };
  if (items.length > BATCH_SIZE) throw new Error(`batch size ${items.length} > ${BATCH_SIZE}`);

  const sp = getClient();

  // batch request body: 各 item を {FeesEstimateRequest, IdType, IdValue} 形式で
  // Identifier で response を seller_sku に紐付けるための一意キー
  const reqMap = new Map();
  const body = items.map((item, idx) => {
    const identifier = `${item.seller_sku}|${idx}|${Date.now()}`;
    reqMap.set(identifier, item);
    return {
      FeesEstimateRequest: {
        MarketplaceId: MARKETPLACE_ID,
        IsAmazonFulfilled: item.channel === 'FBA',
        PriceToEstimateFees: {
          ListingPrice: { CurrencyCode: 'JPY', Amount: item.last_price || 1000 },
          Shipping: { CurrencyCode: 'JPY', Amount: 0 },
        },
        Identifier: identifier,
      },
      IdType: 'ASIN',
      IdValue: item.asin,
    };
  });

  const response = await sp.callAPI({
    operation: 'getMyFeesEstimates',
    endpoint: 'productFees',
    body: body,
  });

  // response: 配列、各要素は { FeesEstimateIdentifier: {...}, FeesEstimate: {...} | undefined, Status: 'Success'|'ClientError', Error: {...} }
  const results = [];
  const errors = [];

  if (!Array.isArray(response)) {
    return { results: [], errors: [{ identifier: 'all', error: 'Response is not an array', raw: JSON.stringify(response).slice(0, 200) }] };
  }

  for (const r of response) {
    const identifier = r.FeesEstimateIdentifier?.SellerInputIdentifier
                    ?? r.FeesEstimateIdentifier?.IdValue
                    ?? null;
    const item = identifier ? reqMap.get(identifier) : null;
    if (!item) {
      errors.push({ identifier, error: 'Cannot match identifier to original SKU', raw: JSON.stringify(r).slice(0, 200) });
      continue;
    }
    if (r.Status !== 'Success' || !r.FeesEstimate) {
      errors.push({ sku: item.seller_sku, error: r.Status, errorMsg: r.Error?.Message || 'no estimate returned' });
      continue;
    }
    const feeList = r.FeesEstimate.FeeDetailList || [];
    const referralFee = feeList.find(f => f.FeeType === 'ReferralFee')?.FeeAmount?.Amount || 0;
    results.push({
      seller_sku: item.seller_sku,
      asin: item.asin,
      channel: item.channel,
      referralFee,
      fbaFee: feeList.find(f => f.FeeType === 'FBAFees')?.FeeAmount?.Amount || 0,
      variableClosingFee: feeList.find(f => f.FeeType === 'VariableClosingFee')?.FeeAmount?.Amount || 0,
      perItemFee: feeList.find(f => f.FeeType === 'PerItemFee')?.FeeAmount?.Amount || 0,
      totalFee: r.FeesEstimate.TotalFeesEstimate?.Amount || 0,
      referralFeeRate: item.last_price > 0 ? referralFee / item.last_price : null,
      price_used: item.last_price,
      refresh_reason: item.refresh_reason,
    });
  }

  return { results, errors };
}

// ─── DB保存 (旧版と同一) ───

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

export async function fetchAmazonFees(mode = 'recent', param = 30, options = {}) {
  const { force = false } = options;
  const db = getDB();
  let targetSkus;

  switch (mode) {
    case 'full':   targetSkus = getAllActiveSkus(db); break;
    case 'recent': targetSkus = getRecentSkus(db, param); break;
    case 'sku':    targetSkus = getSpecificSku(db, param); break;
    default: throw new Error(`Unknown mode: ${mode}`);
  }

  // ASIN がないSKUを除外
  const withAsin = targetSkus.filter(s => s.asin);
  const noAsin = targetSkus.filter(s => !s.asin);
  console.log(`[FetchFees v2] モード=${mode}, 対象SKU=${targetSkus.length}件 (ASIN有=${withAsin.length}, ASIN無=${noAsin.length})`);

  if (withAsin.length === 0) {
    console.log('[FetchFees v2] ASIN付きSKUなし。終了。');
    return { mode, total: targetSkus.length, refreshed: 0, skipped: 0, failed: noAsin.length, errors: noAsin.map(s => ({ sku: s.seller_sku, error: 'No ASIN' })) };
  }

  // TTL/差分フィルタ (--force でスキップ)
  let needRefresh, skipped, skipReasons;
  if (force) {
    needRefresh = withAsin.map(s => ({ ...s, refresh_reason: 'force' }));
    skipped = [];
    skipReasons = {};
    console.log(`[FetchFees v2] --force 指定: TTL/差分フィルタを無効化、全 ${withAsin.length} 件 refresh`);
  } else {
    ({ needRefresh, skipped, skipReasons } = filterByTTLAndDiff(db, withAsin));
    const reasonCnt = needRefresh.reduce((acc, x) => { acc[x.refresh_reason.replace(/\d+/g, 'N')] = (acc[x.refresh_reason.replace(/\d+/g, 'N')] || 0) + 1; return acc; }, {});
    console.log(`[FetchFees v2] TTL/差分フィルタ後: refresh必要=${needRefresh.length} / skip=${skipped.length} (TTL内${skipReasons.ttl_fresh})`);
    console.log(`[FetchFees v2]   refresh理由: ${JSON.stringify(reasonCnt)}`);
  }

  if (needRefresh.length === 0) {
    console.log('[FetchFees v2] refresh対象なし。終了。');
    return { mode, total: targetSkus.length, refreshed: 0, skipped: skipped.length, failed: noAsin.length, errors: noAsin.map(s => ({ sku: s.seller_sku, error: 'No ASIN' })) };
  }

  const numBatches = Math.ceil(needRefresh.length / BATCH_SIZE);
  const estMin = Math.ceil(numBatches * BATCH_SLEEP_MS / 60000);
  console.log(`[FetchFees v2] バッチ取得開始: ${numBatches}バッチ × ${BATCH_SIZE}件、間隔${BATCH_SLEEP_MS}ms (推定${estMin}分)`);

  let totalSuccess = 0;
  let totalFailed = noAsin.length;
  const allErrors = noAsin.map(s => ({ sku: s.seller_sku, error: 'No ASIN' }));

  for (let i = 0; i < needRefresh.length; i += BATCH_SIZE) {
    const batch = needRefresh.slice(i, i + BATCH_SIZE);
    const batchIdx = Math.floor(i / BATCH_SIZE) + 1;
    const progress = `[${batchIdx}/${numBatches}]`;

    let results = [], errors = [];
    // Codex Round 1 #3 反映: 429/QuotaExceeded は amazon-sp-api ライブラリが auto_request_throttled で
    // restore_rate ベースの自動リトライをやってる (SellingPartner.js _retryThrottledRequest) → 外側で
    // 429 を再リトライしない。それ以外のエラー (network 等) のみ linear backoff (BATCH_SLEEP_MS x retry) で MAX_RETRIES 回。
    for (let retry = 0; retry < MAX_RETRIES; retry++) {
      try {
        ({ results, errors } = await fetchFeesBatch(batch));
        break;
      } catch (e) {
        const isLastRetry = retry >= MAX_RETRIES - 1;
        if (isLastRetry) {
          // 最終失敗 → batch 全件 failed
          console.log(`${progress} Error after ${MAX_RETRIES} retries: ${e.message}`);
          for (const item of batch) {
            allErrors.push({ sku: item.seller_sku, error: e.message });
            totalFailed++;
          }
          results = []; errors = [];
          break;
        }
        const waitMs = BATCH_SLEEP_MS * (retry + 1);  // linear backoff (2.1s, 4.2s, ...)
        console.log(`${progress} Error: ${e.message} → retry (${retry + 1}/${MAX_RETRIES}, wait ${waitMs}ms)`);
        await sleep(waitMs);
      }
    }

    if (results.length > 0) saveFees(db, results);
    totalSuccess += results.length;
    totalFailed += errors.length;
    for (const e of errors) allErrors.push(e);

    console.log(`${progress} 成功 ${results.length} / 失敗 ${errors.length} (累計 成功${totalSuccess} / 失敗${totalFailed})`);

    if (i + BATCH_SIZE < needRefresh.length) {
      await sleep(BATCH_SLEEP_MS);
    }
  }

  const summary = {
    mode,
    total: targetSkus.length,
    refreshed: totalSuccess,
    skipped: skipped.length,
    failed: totalFailed,
    errors: allErrors.slice(0, 50),
  };

  console.log(`[FetchFees v2] 完了: refresh=${summary.refreshed} / skip=${summary.skipped} / failed=${summary.failed} / total=${summary.total}`);
  return summary;
}

// ─── CLI実行 ───

const isMain = process.argv[1]?.includes('fetch-amazon-fees');
if (isMain) {
  await initDB();

  const args = process.argv.slice(2);
  let mode = 'recent';
  let param = 30;
  const force = args.includes('--force');

  if (args.includes('--full')) {
    mode = 'full';
  } else if (args.includes('--recent')) {
    const idx = args.indexOf('--recent');
    param = parseInt(args[idx + 1]) || 30;
    mode = 'recent';
  } else if (args.includes('--sku')) {
    const idx = args.indexOf('--sku');
    param = args[idx + 1];
    if (!param) { console.error('--sku にはSKUを指定してください'); process.exit(1); }
    mode = 'sku';
  }

  const result = await fetchAmazonFees(mode, param, { force });
  console.log('\n結果:', JSON.stringify(result, null, 2));

  if (result.errors.length > 0) {
    console.log('\nエラー詳細 (先頭10件):');
    for (const e of result.errors.slice(0, 10)) {
      console.log(`  ${e.sku || e.identifier}: ${e.error}${e.errorMsg ? ` (${e.errorMsg})` : ''}`);
    }
  }

  process.exit(result.failed > 0 ? 1 : 0);
}
