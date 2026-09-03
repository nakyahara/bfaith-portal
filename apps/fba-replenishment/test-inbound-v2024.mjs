/**
 * SP-API Fulfillment Inbound v2024-03-20 — 「API 完結」の検証スクリプト (fba-box PR3 方向性 B)
 *
 * 目的: STA 画面を使わずに 納品プラン作成 → 梱包オプション → 梱包グループの商品 → 箱内容送信 (setPackingInformation)
 *       → 配置オプション取得 まで JP マーケットで通るかを、1 SKU × 1 個の使い捨てプランで確かめる。
 *       最後に必ず cancelInboundPlan で消す (準備中在庫の加算を残さない)。
 *
 * 実行 (miniPC, .env に SP_API_* がある場所で):
 *   node --env-file=.env apps/fba-replenishment/test-inbound-v2024.mjs --msku <SKU> [--qty 1] [--label SELLER|AMAZON] [--prep NONE|SELLER]
 *   --plan-only     … createInboundPlan → cancel だけ (最小)
 *   --no-cancel     … 最後にキャンセルしない (Seller Central で見え方を確認したいとき。必ず後で手動削除)
 *   --keep-going    … 途中の失敗で止まらず次のステップも試す
 *
 * 出力: 各ステップの結果 JSON を stdout に。失敗理由 (operationProblems / API エラー本文) をそのまま出す。
 * 送り元住所は fba-replenishment の設定 (Render 側 DB) ではなく引数/環境変数で渡す:
 *   INBOUND_SHIP_FROM_JSON='{"name":"...","addressLine1":"...","city":"...","stateOrProvinceCode":"...","postalCode":"...","countryCode":"JP","phoneNumber":"..."}'
 */
import SellingPartner from 'amazon-sp-api';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, arr) => {
  if (v.startsWith('--')) a.push([v.slice(2), arr[i + 1] && !arr[i + 1].startsWith('--') ? arr[i + 1] : true]);
  return a;
}, []));
const MSKU = args.msku;
const QTY = Number(args.qty || 1);
const LABEL_OWNER = args.label || 'SELLER';
const PREP_OWNER = args.prep || 'NONE';
const PLAN_ONLY = !!args['plan-only'];
const NO_CANCEL = !!args['no-cancel'];
const KEEP_GOING = !!args['keep-going'];
if (!MSKU) { console.error('usage: --msku <SKU> [--qty 1] [--label SELLER] [--prep NONE] [--plan-only] [--no-cancel] [--keep-going]'); process.exit(2); }

const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
const BASE = '/inbound/fba/2024-03-20';
let sourceAddress = null;
try { sourceAddress = JSON.parse(process.env.INBOUND_SHIP_FROM_JSON || ''); } catch { /* 下で検証 */ }
if (!sourceAddress?.name || !sourceAddress?.addressLine1 || !sourceAddress?.postalCode || !sourceAddress?.phoneNumber) {
  console.error('INBOUND_SHIP_FROM_JSON (name/addressLine1/city/stateOrProvinceCode/postalCode/countryCode/phoneNumber) を環境変数で渡してください');
  process.exit(2);
}

const sp = new SellingPartner({
  region: 'fe',
  refresh_token: process.env.SP_API_REFRESH_TOKEN,
  credentials: {
    SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
    SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
    AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (step, obj) => console.log(`\n### ${step}\n${JSON.stringify(obj, null, 2)}`);
const report = { msku: MSKU, qty: QTY, steps: {} };

async function call(step, req) {
  try {
    const r = await sp.callAPI(req);
    report.steps[step] = { ok: true, response: r };
    log(step, r);
    return r;
  } catch (e) {
    const detail = { ok: false, message: e.message, code: e.code, details: e.details || e.response?.data || null };
    report.steps[step] = detail;
    log(`${step} ✗`, detail);
    if (!KEEP_GOING) throw new Error(`${step} failed`);
    return null;
  }
}

/** operation の完了待ち (最大 3 分)。FAILED でも operationProblems を返す */
async function waitOperation(step, operationId) {
  if (!operationId) return null;
  for (let i = 0; i < 36; i++) {
    await sleep(5000);
    const st = await sp.callAPI({ api_path: `${BASE}/operations/${operationId}`, method: 'GET' });
    process.stdout.write(`  ${step} … ${st.operationStatus} (${i + 1})\r`);
    if (['SUCCESS', 'FAILED'].includes(st.operationStatus)) {
      console.log();
      report.steps[`${step}:operation`] = st;
      if (st.operationStatus === 'FAILED') log(`${step} operation FAILED`, st.operationProblems || st);
      return st;
    }
  }
  throw new Error(`${step}: operation timeout`);
}

let inboundPlanId = null;
try {
  // 1. プラン作成
  const create = await call('createInboundPlan', {
    api_path: `${BASE}/inboundPlans`, method: 'POST',
    body: {
      name: `fba-box検証 ${new Date().toISOString().slice(0, 16)}`,
      sourceAddress,
      destinationMarketplaces: [MARKETPLACE_ID],
      items: [{ msku: MSKU, quantity: QTY, labelOwner: LABEL_OWNER, prepOwner: PREP_OWNER }],
    },
  });
  inboundPlanId = create?.inboundPlanId || null;
  const op1 = await waitOperation('createInboundPlan', create?.operationId);
  if (op1?.operationStatus !== 'SUCCESS') throw new Error('createInboundPlan operation not SUCCESS');

  await call('getInboundPlan', { api_path: `${BASE}/inboundPlans/${inboundPlanId}`, method: 'GET' });
  await call('listInboundPlanItems', { api_path: `${BASE}/inboundPlans/${inboundPlanId}/items`, method: 'GET' });

  if (!PLAN_ONLY) {
    // 2. 梱包オプション
    const gen = await call('generatePackingOptions', { api_path: `${BASE}/inboundPlans/${inboundPlanId}/packingOptions`, method: 'POST' });
    await waitOperation('generatePackingOptions', gen?.operationId);
    const opts = await call('listPackingOptions', { api_path: `${BASE}/inboundPlans/${inboundPlanId}/packingOptions`, method: 'GET' });
    const option = opts?.packingOptions?.[0];
    if (!option) throw new Error('packingOptions が空');
    const groups = option.packingGroups || [];
    for (const gid of groups) {
      await call(`listPackingGroupItems:${gid}`, { api_path: `${BASE}/inboundPlans/${inboundPlanId}/packingGroups/${gid}/items`, method: 'GET' });
    }
    const conf = await call('confirmPackingOption', { api_path: `${BASE}/inboundPlans/${inboundPlanId}/packingOptions/${option.packingOptionId}/confirmation`, method: 'POST' });
    await waitOperation('confirmPackingOption', conf?.operationId);

    // 3. 箱内容 (これが fba-box の出力先になる)
    const setPack = await call('setPackingInformation', {
      api_path: `${BASE}/inboundPlans/${inboundPlanId}/packingInformation`, method: 'POST',
      body: {
        packageGroupings: groups.map((gid) => ({
          packingGroupId: gid,
          boxes: [{
            contentInformationSource: 'BOX_CONTENT_PROVIDED',
            quantity: 1,
            dimensions: { unitOfMeasurement: 'CM', length: 40, width: 30, height: 25 },
            weight: { unit: 'KG', value: 2.5 },
            items: [{ msku: MSKU, quantity: QTY, labelOwner: LABEL_OWNER, prepOwner: PREP_OWNER }],
          }],
        })),
      },
    });
    await waitOperation('setPackingInformation', setPack?.operationId);

    // 4. 配置オプション (確定はしない = 出荷は作らない)
    const genP = await call('generatePlacementOptions', { api_path: `${BASE}/inboundPlans/${inboundPlanId}/placementOptions`, method: 'POST' });
    await waitOperation('generatePlacementOptions', genP?.operationId);
    await call('listPlacementOptions', { api_path: `${BASE}/inboundPlans/${inboundPlanId}/placementOptions`, method: 'GET' });
    await call('getInboundPlan (after packing)', { api_path: `${BASE}/inboundPlans/${inboundPlanId}`, method: 'GET' });
  }
} catch (e) {
  console.error(`\n!! 中断: ${e.message}`);
  report.aborted = e.message;
} finally {
  if (inboundPlanId && !NO_CANCEL) {
    try {
      const c = await call('cancelInboundPlan', { api_path: `${BASE}/inboundPlans/${inboundPlanId}/cancellation`, method: 'PUT' });
      await waitOperation('cancelInboundPlan', c?.operationId);
    } catch (e) {
      console.error(`!! キャンセル失敗 — Seller Central で手動削除してください: ${inboundPlanId} (${e.message})`);
      report.cancelFailed = inboundPlanId;
    }
  } else if (inboundPlanId) {
    console.log(`\n(--no-cancel) プランを残しました: ${inboundPlanId} → https://sellercentral-japan.amazon.com/fba/sendtoamazon/confirm_content_step?wf=${inboundPlanId}`);
  }
  console.log('\n===== SUMMARY =====');
  console.log(JSON.stringify({
    inboundPlanId,
    aborted: report.aborted || null,
    steps: Object.fromEntries(Object.entries(report.steps).map(([k, v]) => [k, v.ok === false ? `✗ ${v.message}` : (v.operationStatus || 'ok')])),
  }, null, 2));
}
