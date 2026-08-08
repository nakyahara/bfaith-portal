/**
 * FBA納品への追跡番号投入 — SP-API Fulfillment Inbound v2024-03-20
 *
 * 2026-08-07 に本番アカウントで実測して確定した事実:
 *   - **shipment一覧の operation は存在しない**。shipments は `GET /inboundPlans/{id}` の
 *     レスポンス本体に入っている (`/inboundPlans/{id}/shipments` は Access denied になるが
 *     これは権限不足ではなく「そんなパスは無い」の意味)
 *   - boxId は `<納品番号>U<6桁連番>` だが **推測生成せず必ず listShipmentBoxes の返り値を使う**
 *   - trackingId は **ハイフン無しの数字**で送る
 *   - 送信ボディに `ltlTrackingDetail` は入れない (SPDのみ)
 *   - 🚨 **PUT は「期限内」でないと拒否される**。status ではなく期限が効く:
 *       READY_TO_SHIP + 期限外 ❌ / SHIPPED + 期限外 ❌ / SHIPPED + 期限内 ✅
 *     期限は `dates.readyToShipWindow.end` (出荷日23:59 JST) と
 *     `selectedDeliveryWindow.editableUntil` (翌日09:00 JST)。
 *     22:00 JST の定期実行は期限内に入るが、**失敗時のリトライ余裕は当日中しかない**
 *   - PUT は非同期。operationId を `getInboundOperationStatus` で SUCCESS まで追う
 */
import SellingPartner from 'amazon-sp-api';

const V = '/inbound/fba/2024-03-20';
const MARKETPLACE = () => process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';

// updateShipmentTrackingDetails の restore_rate は 0.5/秒。連打防止に最低間隔を置く。
const MIN_INTERVAL_MS = 1200;
let lastCallAt = 0;

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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function call(api_path, method = 'GET', body) {
  const wait = MIN_INTERVAL_MS - (Date.now() - lastCallAt);
  if (wait > 0) await sleep(wait);
  lastCallAt = Date.now();
  return getClient().callAPI(body ? { api_path, method, body } : { api_path, method });
}

export function missingEnv() {
  return ['SP_API_CLIENT_ID', 'SP_API_CLIENT_SECRET', 'SP_API_REFRESH_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    .filter((k) => !process.env[k]);
}

/** 期限を過ぎていないか。過ぎていたらAPIは受け付けない (画面からは入るのでリカバリは人手)。 */
export function checkDeadline(shipment, now = new Date()) {
  const rts = shipment?.dates?.readyToShipWindow?.end;
  const edit = shipment?.selectedDeliveryWindow?.editableUntil;
  const limits = [rts, edit].filter(Boolean).map((s) => ({ raw: s, t: Date.parse(s) })).filter((x) => !Number.isNaN(x.t));
  if (!limits.length) return { ok: true, limits: [], expired: [], note: '期限の情報が取れませんでした' };
  const expired = limits.filter((x) => x.t <= now.getTime());
  return {
    ok: expired.length === 0,
    limits: limits.map((x) => x.raw),
    expired: expired.map((x) => x.raw),
    note: expired.length
      ? 'この納品は編集期限を過ぎています。APIでは登録できません (Seller Central画面からは入力できます)'
      : null,
  };
}

/**
 * 追跡番号がまだ入っていない納品を探す。
 * ⭐無駄なAPI呼び出しを避けるため、プラン本体の shipments[].status を見てから詳細を取る。
 * @param {{statuses?: string[], maxPlans?: number}} [opts]
 * @returns {Promise<Array>} [{inboundPlanId, planName, shipmentId, shipmentConfirmationId, fcCode,
 *                             status, boxIds, hasTracking, deadline, raw}]
 */
export async function findOpenShipments(opts = {}) {
  const wantStatuses = opts.statuses ?? ['READY_TO_SHIP'];
  const maxPlans = opts.maxPlans ?? 20;
  const plans = await call(`${V}/inboundPlans?pageSize=${Math.min(maxPlans, 20)}&status=ACTIVE`);
  const out = [];
  for (const p of plans.inboundPlans ?? []) {
    let full;
    try { full = await call(`${V}/inboundPlans/${p.inboundPlanId}`); } catch { continue; }
    for (const s of full.shipments ?? []) {
      if (!wantStatuses.includes(s.status)) continue;
      const base = `${V}/inboundPlans/${p.inboundPlanId}/shipments/${s.shipmentId}`;
      const d = await call(base);
      const b = await call(`${base}/boxes?pageSize=100`);
      out.push({
        inboundPlanId: p.inboundPlanId,
        planName: p.name || '',
        shipmentId: s.shipmentId,
        shipmentConfirmationId: d.shipmentConfirmationId,
        fcCode: d.destination?.warehouseId ?? '',
        status: d.status,
        // ⭐boxIdは規則から生成せずAPIの返り値を正とする
        boxIds: (b.boxes ?? []).map((x) => x.boxId),
        hasTracking: Boolean(d.trackingDetails?.spdTrackingDetail?.spdTrackingItems?.length),
        deadline: checkDeadline(d),
      });
    }
  }
  return out;
}

/**
 * 追跡番号を投入する。
 * @param {{inboundPlanId: string, shipmentId: string}} target
 * @param {Array<{boxId: string, trackingId: string}>} items 箱の**全件**を渡すこと (部分更新のmergeは前提にしない)
 * @returns {Promise<{ok: boolean, operationId?: string, error?: string, verified?: object}>}
 */
export async function putTrackingDetails(target, items) {
  if (!items?.length) return { ok: false, error: '投入する項目がありません' };
  const bad = items.filter((i) => !i.boxId || !/^[0-9]{8,20}$/.test(String(i.trackingId ?? '')));
  if (bad.length) return { ok: false, error: `送信できない項目があります: ${JSON.stringify(bad.slice(0, 3))}` };

  const base = `${V}/inboundPlans/${target.inboundPlanId}/shipments/${target.shipmentId}`;
  // SPD のみ。過去レスポンスに出てくる空の ltlTrackingDetail はコピーしない
  const body = { trackingDetails: { spdTrackingDetail: { spdTrackingItems: items.map((i) => ({ boxId: i.boxId, trackingId: i.trackingId })) } } };

  let res;
  try {
    res = await call(`${base}/trackingDetails`, 'PUT', body);
  } catch (e) {
    const msg = e?.message ?? String(e);
    return {
      ok: false,
      error: msg,
      // 状態起因の400は再試行しても通らない。無限リトライしないことを呼び出し側へ伝える
      retryable: !/is not in a state where tracking details can be provided|BadRequest/i.test(msg),
    };
  }

  // ⭐202相当。operationId が返ったら SUCCESS まで追う (返り値だけで成功と判断しない)
  const operationId = res?.operationId;
  if (operationId) {
    for (let i = 0; i < 20; i++) {
      await sleep(3000);
      const op = await call(`${V}/operations/${operationId}`);
      if (op.operationStatus === 'SUCCESS') break;
      if (op.operationStatus === 'FAILED') {
        return { ok: false, operationId, error: `operation FAILED: ${JSON.stringify(op.operationProblems ?? {})}`, retryable: false };
      }
      if (i === 19) return { ok: false, operationId, error: 'operation が時間内に完了しませんでした', retryable: true };
    }
  }
  return { ok: true, operationId: operationId ?? null };
}

/**
 * 投入後の確認。
 * ⚠️ trackingDetails の反映には数時間かかるため、**null でも失敗とは限らない**。
 * 成否の判定には使わず、あくまで「見えたら一致しているか」を見る。
 */
export async function verifyTracking(target, items) {
  const base = `${V}/inboundPlans/${target.inboundPlanId}/shipments/${target.shipmentId}`;
  const d = await call(base);
  const got = d.trackingDetails?.spdTrackingDetail?.spdTrackingItems ?? [];
  if (!got.length) return { visible: false, matched: null, status: d.status, note: 'APIにはまだ反映されていません (反映に数時間かかることを確認済み)' };
  const a = got.map((x) => `${x.boxId}=${x.trackingId}`).sort().join(',');
  const b = items.map((x) => `${x.boxId}=${x.trackingId}`).sort().join(',');
  return { visible: true, matched: a === b, status: d.status, got };
}

export const _internal = { MARKETPLACE };
