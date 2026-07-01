/**
 * FBA納品プラン作成 — SP-API Fulfillment Inbound API v2024-03-20
 */
import SellingPartner from 'amazon-sp-api';

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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 納品プラン作成
 * @param {Object} sourceAddress - 送り元住所
 * @param {Array} items - [{msku, quantity, labelOwner, prepOwner, expiration?}]
 * @param {string} planName - プラン名（省略可）
 * @returns {Object} { inboundPlanId, operationId, status, problems }
 */
export async function createInboundPlan(sourceAddress, items, planName) {
  const sp = getClient();
  const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';

  const body = {
    sourceAddress,
    destinationMarketplaces: [marketplaceId],
    items: items.map(item => ({
      msku: item.msku,
      quantity: item.quantity,
      labelOwner: item.labelOwner || 'AMAZON',
      prepOwner: item.prepOwner || 'SELLER',
      ...(item.expiration ? { expiration: item.expiration } : {}),
    })),
  };
  if (planName) body.name = planName;

  console.log(`[Inbound] 納品プラン作成: ${items.length} SKU`);

  // createInboundPlan
  const createResult = await sp.callAPI({
    api_path: '/inbound/fba/2024-03-20/inboundPlans',
    method: 'POST',
    body,
  });

  const inboundPlanId = createResult.inboundPlanId;
  const operationId = createResult.operationId;
  console.log(`[Inbound] プランID: ${inboundPlanId}, オペレーションID: ${operationId}`);

  // ポーリング（最大3分）
  const result = await pollOperation(operationId);

  // エラー詳細をログに出力
  if (result.operationProblems && result.operationProblems.length > 0) {
    console.log('[Inbound] operationProblems:', JSON.stringify(result.operationProblems, null, 2));
  }

  // FAILEDの場合、プランのアイテム一覧を取得してエラーSKUを特定
  let planItems = [];
  if (result.operationStatus === 'FAILED' && inboundPlanId) {
    try {
      planItems = await listPlanItems(inboundPlanId);
      console.log(`[Inbound] プランアイテム取得: ${planItems.length}件`);
      if (planItems.length > 0) {
        console.log('[Inbound] planItems[0]:', JSON.stringify(planItems[0], null, 2));
      }
    } catch (e) {
      console.log('[Inbound] プランアイテム取得失敗（プランが存在しない可能性）:', e.message);
    }
  }

  return {
    inboundPlanId,
    operationId,
    status: result.operationStatus,
    problems: result.operationProblems || [],
    planItems,
  };
}

/**
 * プランのアイテム一覧を取得（エラー特定用）
 */
async function listPlanItems(inboundPlanId) {
  const sp = getClient();
  const allItems = [];
  let nextToken = null;

  do {
    const params = nextToken ? `?pageToken=${encodeURIComponent(nextToken)}` : '';
    const result = await sp.callAPI({
      api_path: `/inbound/fba/2024-03-20/inboundPlans/${inboundPlanId}/items${params}`,
      method: 'GET',
    });
    if (result.items) allItems.push(...result.items);
    nextToken = result.pagination?.token || null;
  } while (nextToken);

  return allItems;
}

/**
 * 納品プランのshipment一覧を取得
 */
export async function listShipments(inboundPlanId) {
  const sp = getClient();
  const allShipments = [];
  let nextToken = null;

  do {
    const params = nextToken ? `?pageToken=${encodeURIComponent(nextToken)}` : '';
    const result = await sp.callAPI({
      api_path: `/inbound/fba/2024-03-20/inboundPlans/${inboundPlanId}/shipments${params}`,
      method: 'GET',
    });
    if (result.shipments) allShipments.push(...result.shipments);
    nextToken = result.pagination?.token || null;
  } while (nextToken);

  return allShipments;
}

/**
 * shipmentのアイテム一覧を取得
 */
export async function listShipmentItems(inboundPlanId, shipmentId) {
  const sp = getClient();
  const allItems = [];
  let nextToken = null;

  do {
    const params = nextToken ? `?pageToken=${encodeURIComponent(nextToken)}` : '';
    const result = await sp.callAPI({
      api_path: `/inbound/fba/2024-03-20/inboundPlans/${inboundPlanId}/shipments/${shipmentId}/items${params}`,
      method: 'GET',
    });
    if (result.items) allItems.push(...result.items);
    nextToken = result.pagination?.token || null;
  } while (nextToken);

  return allItems;
}

/**
 * FBA Inbound Eligibility APIでASINの受入可否をチェック
 * @param {Array} asins - [{asin, msku}]
 * @returns {Array} 不適格アイテム [{asin, msku, reasons}]
 */
/**
 * 二分探索でエラーを起こすSKUを特定
 * アイテムを半分に分けてプラン作成 → 失敗した方をさらに分割 → 1件に絞り込む
 * @param {Object} sourceAddress
 * @param {Array} items - APIに送るアイテム配列 [{msku, quantity, labelOwner, prepOwner, ...}]
 * @returns {Array} エラーSKUのリスト
 */
export async function findErrorSkusByBinarySearch(sourceAddress, items) {
  console.log(`[BinarySearch] ${items.length}件から問題SKUを探索開始`);

  if (items.length <= 1) {
    return items.map(i => i.msku);
  }

  const mid = Math.ceil(items.length / 2);
  const firstHalf = items.slice(0, mid);
  const secondHalf = items.slice(mid);

  const errorSkus = [];

  for (const [label, batch] of [['前半', firstHalf], ['後半', secondHalf]]) {
    try {
      console.log(`[BinarySearch] ${label} ${batch.length}件を試行...`);
      const result = await createInboundPlan(sourceAddress, batch, `探索-${label}`);

      if (result.status === 'FAILED') {
        console.log(`[BinarySearch] ${label} → FAILED、さらに分割`);
        if (batch.length <= 1) {
          errorSkus.push(batch[0].msku);
        } else {
          const found = await findErrorSkusByBinarySearch(sourceAddress, batch);
          errorSkus.push(...found);
        }
      } else {
        console.log(`[BinarySearch] ${label} → SUCCESS（問題なし）`);
      }
    } catch (e) {
      // 例外（バリデーションエラー等）→ このバッチに問題がある
      console.log(`[BinarySearch] ${label} → 例外: ${e.message}`);
      if (batch.length <= 1) {
        errorSkus.push(batch[0].msku);
      } else {
        const found = await findErrorSkusByBinarySearch(sourceAddress, batch);
        errorSkus.push(...found);
      }
    }
  }

  return errorSkus;
}

export async function checkInboundEligibility(items) {
  const sp = getClient();
  const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
  const ineligible = [];

  console.log(`[Eligibility] ${items.length}件のASINをチェック開始...`);

  for (const item of items) {
    if (!item.asin) continue;
    try {
      const result = await sp.callAPI({
        api_path: '/fba/inbound/v1/eligibility/itemPreview',
        method: 'GET',
        query: {
          asin: item.asin,
          program: 'INBOUND',
          marketplaceIds: marketplaceId,
        },
      });
      if (result.isEligibleForProgram === false) {
        console.log(`[Eligibility] NG: ${item.asin} (${item.msku}) - ${JSON.stringify(result.ineligibilityReasonList)}`);
        ineligible.push({
          asin: item.asin,
          msku: item.msku,
          reasons: result.ineligibilityReasonList || [],
        });
      }
      await sleep(200);
    } catch (e) {
      console.log(`[Eligibility] ${item.asin} (${item.msku}) チェック失敗: ${e.message}`);
      // APIエラーでも不適格として記録（安全側）
      if (e.message && (e.message.includes('INELIGIBLE') || e.message.includes('dangerous'))) {
        ineligible.push({
          asin: item.asin,
          msku: item.msku,
          reasons: [{ code: 'API_ERROR', message: e.message }],
        });
      }
    }
  }

  console.log(`[Eligibility] 完了: ${ineligible.length}件が不適格`);
  return ineligible;
}

/**
 * ACTIVEな納品プランから、SKU別の準備中数量を集計
 * （7日以内に作成されたプランのみ対象 ※二重計上回避と「放置プラン」除外のための窓。
 *   恒久対策は「未出荷の箱数量のみ計上」だが shipment API が権限不足のため当面は日付窓で運用）
 * 期限管理商品もここで取得されるが、calculation-engine 側で「別期限を送るため除外」の例外処理あり
 * @returns {Object} { [msku]: quantity, ... }
 */
export async function fetchActiveInboundQuantities() {
  const sp = getClient();
  // 7日以内に作成されたプランのみ対象 (作成→出荷のリードタイムを跨ぐ谷間対策で 3日→7日。2026-06-30)
  const sevenDaysAgo = new Date(Date.now() - 7 * 86400000);

  // 1. ACTIVEプランを「作成日の新しい順」で取得し、7日より古いプランが出たら即停止。
  //   ※ ACTIVEは出荷・受領後も数ヶ月残る(2026-07時点で471件)。LAST_UPDATED順+全件取得だと
  //     49ページ/約24秒かかり、/refresh-inbound-working がタイムアウト/throttleで失敗→準備中が空に
  //     なる事故が出た(2026-07-01)。CREATION_TIME DESC なら7日以内(数件)は先頭に固まるので
  //     実質1ページで済み、高速・安定する。pageSize=30 (API最大)。
  let recentPlans = [];
  let nextToken = null;
  let pageCount = 0;
  let reachedOld = false;

  do {
    const params = new URLSearchParams();
    params.set('status', 'ACTIVE');
    params.set('pageSize', '30');
    params.set('sortBy', 'CREATION_TIME');
    params.set('sortOrder', 'DESC');
    if (nextToken) params.set('paginationToken', nextToken);

    const result = await sp.callAPI({
      api_path: `/inbound/fba/2024-03-20/inboundPlans?${params.toString()}`,
      method: 'GET',
    });

    for (const p of (result.inboundPlans || [])) {
      if (new Date(p.createdAt) >= sevenDaysAgo) recentPlans.push(p);
      else { reachedOld = true; } // CREATION_TIME DESC: 以降は全て7日より古い
    }
    // SP-API 応答のページングトークンは pagination.nextToken (.token ではない)。
    nextToken = result.pagination?.nextToken || null;
    pageCount++;
    if (reachedOld) break; // これ以上新しい(7日以内)プランは無いので打ち切り
    if (nextToken) await sleep(200);
  } while (nextToken && pageCount < 30);

  // サイレント欠落防止: 30ページ上限に当たったのに「7日より古い」に到達していない=取りこぼしの疑い。
  if (!reachedOld && nextToken && pageCount >= 30) {
    console.warn(`[Inbound] ⚠️ ページ上限(30)到達で打ち切り。7日以内プランを取りこぼした可能性 (recentPlans=${recentPlans.length})`);
  }
  console.log(`[Inbound] 取得ページ=${pageCount} / 7日以内のACTIVEプラン: ${recentPlans.length}件 (打ち切り=${reachedOld})`);

  // 3. 各プランのアイテムを取得してSKU別に集計
  const skuQtyMap = {};
  let totalItems = 0;

  for (const plan of recentPlans) {
    const items = [];
    let itemToken = null;

    do {
      const params = itemToken ? `?paginationToken=${encodeURIComponent(itemToken)}` : '';
      const result = await sp.callAPI({
        api_path: `/inbound/fba/2024-03-20/inboundPlans/${plan.inboundPlanId}/items${params}`,
        method: 'GET',
      });
      if (result.items) items.push(...result.items);
      itemToken = result.pagination?.nextToken || null;
    } while (itemToken);

    if (items.length === 0) continue;

    for (const item of items) {
      const msku = item.msku;
      const qty = item.quantity || 0;
      skuQtyMap[msku] = (skuQtyMap[msku] || 0) + qty;
    }
    totalItems += items.length;
    console.log(`[Inbound] ${plan.name || plan.inboundPlanId}: ${items.length}アイテム`);
  }

  console.log(`[Inbound] 準備中集計完了: ${Object.keys(skuQtyMap).length} SKU, ${totalItems}アイテム`);
  return skuQtyMap;
}

/**
 * オペレーションステータスをポーリング
 */
async function pollOperation(operationId) {
  const sp = getClient();

  for (let i = 0; i < 36; i++) {
    await sleep(5000);
    const status = await sp.callAPI({
      api_path: `/inbound/fba/2024-03-20/operations/${operationId}`,
      method: 'GET',
    });
    console.log(`[Inbound] ポーリング ${i + 1}: ${status.operationStatus}`);
    if (['SUCCESS', 'FAILED'].includes(status.operationStatus)) {
      return status;
    }
  }
  throw new Error('オペレーションがタイムアウトしました（3分超）');
}
