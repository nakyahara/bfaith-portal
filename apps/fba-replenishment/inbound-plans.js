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

  return {
    inboundPlanId,
    operationId,
    status: result.operationStatus,
    problems: result.operationProblems || [],
  };
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
