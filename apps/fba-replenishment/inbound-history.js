/**
 * FBA納品実績の取込 — SP-API Fulfillment Inbound API v0
 *
 * 「いつ・何SKU・何個 のプランを作ったか」を日別に集計するための実績データを取り込む。
 *
 * なぜ v0 なのか (2026-08-05 実測):
 *   - v2024-03-20 の listInboundPlans は各 status でちょうど30件しか返らず、次ページの token も来ない
 *     → 直近3週間分しか遡れない
 *   - v2024-03-20 の /inboundPlans/{id}/shipments は "Access to requested resource is denied" (ロール不足)
 *   - v0 の /fba/inbound/v0/shipments は生きていて、2年分1,319件をページングで取得できた
 *   - v0 の items は QuantityShipped と QuantityReceived を両方返す → 未受領の追跡もここで完結する
 *
 * 作成日は v0 のレスポンスに無いので ShipmentName から抽出する (db.js の parseShipmentCreatedAt)。
 * Send to Amazon が付ける "FBA STA (2026/06/16 05:49)-TPY1" 形式で、実測 1318/1319 件が該当した。
 */
import SellingPartner from 'amazon-sp-api';
import {
  upsertInboundShipments,
  replaceInboundItems,
  getShipmentsNeedingItemSync,
  getInboundSyncStatus,
  flushInboundDb,
} from './db.js';

const ALL_STATUSES = [
  'WORKING', 'SHIPPED', 'RECEIVING', 'CANCELLED', 'DELETED',
  'CLOSED', 'ERROR', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN',
];

// SP-API の Fulfillment Inbound v0 は rate 2req/sec。余裕を見て 600ms 間隔。
const CALL_INTERVAL_MS = 600;
// 明細取得の途中でもディスクに落とす間隔 (途中で落ちても取り直しが少なく済む)
const FLUSH_EVERY = 50;

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

function marketplaceId() {
  return process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
}

/** スロットリング時だけ指数バックオフで粘る。それ以外のエラーは呼び出し側に投げる。 */
async function callWithRetry(apiPath, label, maxRetries = 4) {
  const sp = getClient();
  let waitMs = 2000;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res = await sp.callAPI({ api_path: apiPath, method: 'GET' });
      return res?.payload || res;
    } catch (e) {
      const msg = String(e?.message || e);
      const throttled = /throttl|quota|429|too many requests/i.test(msg);
      if (!throttled || attempt === maxRetries) throw e;
      console.log(`[InboundHistory] ${label} スロットル → ${waitMs / 1000}秒待機 (${attempt + 1}/${maxRetries})`);
      await sleep(waitMs);
      waitMs *= 2;
    }
  }
}

/**
 * シップメント一覧を取得 (LastUpdated の範囲で、全ページ)。
 * @param {{after: string, before: string, maxPages?: number, onProgress?: Function}} opts
 */
export async function fetchShipmentList(opts) {
  const { after, before, maxPages = 200, onProgress } = opts;
  const all = [];
  let token = null;
  let page = 0;

  do {
    const qs = token
      ? new URLSearchParams({ MarketplaceId: marketplaceId(), QueryType: 'NEXT_TOKEN', NextToken: token }).toString()
      : new URLSearchParams({
          MarketplaceId: marketplaceId(),
          QueryType: 'DATE_RANGE',
          LastUpdatedAfter: after,
          LastUpdatedBefore: before,
          ShipmentStatusList: ALL_STATUSES.join(','),
        }).toString();

    const payload = await callWithRetry(`/fba/inbound/v0/shipments?${qs}`, `getShipments p${page + 1}`);
    const data = payload?.ShipmentData || [];
    all.push(...data);
    token = payload?.NextToken || null;
    page += 1;
    if (onProgress) onProgress({ phase: 'list', page, total: all.length });
    await sleep(CALL_INTERVAL_MS);
  } while (token && page < maxPages);

  if (token) {
    console.log(`[InboundHistory] ⚠️ ページ上限 ${maxPages} に到達。まだ続きがある可能性`);
  }
  return all;
}

/** 1シップメントの明細を取得。 */
export async function fetchShipmentItems(shipmentId) {
  const qs = new URLSearchParams({ MarketplaceId: marketplaceId() }).toString();
  const payload = await callWithRetry(`/fba/inbound/v0/shipments/${shipmentId}/items?${qs}`, `items ${shipmentId}`);
  return payload?.ItemData || [];
}

/**
 * 納品実績を同期する。
 *
 * @param {object} opts
 * @param {boolean} [opts.full]        - 全期間を取り直す (初回バックフィル用)
 * @param {number}  [opts.sinceDays]   - 差分取得の遡り日数 (既定 14)
 * @param {boolean} [opts.allItems]    - 明細を全件取り直す (通常は変化しうるものだけ)
 * @param {number}  [opts.itemLimit]   - 明細取得の上限件数 (日次実行の暴走防止、0=無制限)
 * @param {Function} [opts.onProgress] - 進捗コールバック
 * @returns {Promise<object>} 取込結果サマリ
 */
export async function syncInboundHistory(opts = {}) {
  const {
    full = false,
    sinceDays = 14,
    allItems = false,
    itemLimit = 0,
    onProgress,
  } = opts;

  const startedAt = Date.now();
  const now = new Date();
  const before = new Date(now.getTime() + 60 * 60 * 1000).toISOString(); // 時刻ずれ対策で1時間先まで
  const after = full
    // v0 は作成日ではなく「更新日」で絞るため、古い作成日のものも更新があれば拾える。
    // 十分に古い起点にしておけば実質的な全件取得になる。
    ? '2023-01-01T00:00:00Z'
    : new Date(now.getTime() - sinceDays * 86400000).toISOString();

  console.log(`[InboundHistory] シップメント一覧を取得: ${after} 〜 ${before}`);
  const shipments = await fetchShipmentList({ after, before, onProgress });
  console.log(`[InboundHistory] ${shipments.length}件を取得`);

  const { inserted, updated } = upsertInboundShipments(shipments);
  flushInboundDb();
  console.log(`[InboundHistory] 新規 ${inserted}件 / 更新 ${updated}件`);

  // --- 明細 ---
  let targets = getShipmentsNeedingItemSync({ all: allItems });
  if (itemLimit > 0 && targets.length > itemLimit) {
    console.log(`[InboundHistory] 明細対象 ${targets.length}件 → 上限 ${itemLimit}件に制限 (残りは次回)`);
    targets = targets.slice(0, itemLimit);
  }
  console.log(`[InboundHistory] 明細を取得: ${targets.length}件`);

  let itemsSynced = 0;
  const errors = [];
  for (let i = 0; i < targets.length; i++) {
    const { shipment_id } = targets[i];
    try {
      const items = await fetchShipmentItems(shipment_id);
      replaceInboundItems(shipment_id, items);
      itemsSynced += 1;
    } catch (e) {
      errors.push({ shipment_id, message: String(e?.message || e) });
      console.log(`[InboundHistory] ❌ ${shipment_id}: ${e?.message || e}`);
    }
    if ((i + 1) % FLUSH_EVERY === 0) {
      flushInboundDb();
      console.log(`[InboundHistory] 明細 ${i + 1}/${targets.length}`);
    }
    if (onProgress) onProgress({ phase: 'items', done: i + 1, total: targets.length });
    await sleep(CALL_INTERVAL_MS);
  }
  flushInboundDb();

  const status = getInboundSyncStatus();
  const elapsedSec = Math.round((Date.now() - startedAt) / 1000);
  const result = {
    fetched: shipments.length,
    inserted,
    updated,
    items_synced: itemsSynced,
    items_failed: errors.length,
    errors: errors.slice(0, 10),
    elapsed_sec: elapsedSec,
    ...status,
  };
  console.log(`[InboundHistory] 完了 (${elapsedSec}秒): 明細 ${itemsSynced}件同期, 失敗 ${errors.length}件`);
  return result;
}
