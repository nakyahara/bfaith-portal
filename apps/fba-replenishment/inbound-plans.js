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
      if (result && result.isEligibleForProgram === false) {
        console.log(`[Eligibility] NG: ${item.asin} (${item.msku}) - ${JSON.stringify(result.ineligibilityReasonList)}`);
        ineligible.push({
          asin: item.asin,
          msku: item.msku,
          reasons: result.ineligibilityReasonList || [],
        });
      }
      await sleep(1100); // レート制限: 1リクエスト/秒
    } catch (e) {
      const msg = e.message || '';
      // HTMLレスポンス（レート制限等）はスキップ
      if (msg.includes('<!DOCTYPE') || msg.includes('Unexpected token')) {
        console.log(`[Eligibility] ${item.asin} → レート制限、3秒待機...`);
        await sleep(3000);
        continue;
      }
      console.log(`[Eligibility] ${item.asin} (${item.msku}) チェック失敗: ${msg}`);
      if (msg.includes('INELIGIBLE') || msg.includes('dangerous')) {
        ineligible.push({
          asin: item.asin,
          msku: item.msku,
          reasons: [{ code: 'API_ERROR', message: msg }],
        });
      }
    }
  }

  console.log(`[Eligibility] 完了: ${ineligible.length}件が不適格`);
  return ineligible;
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
