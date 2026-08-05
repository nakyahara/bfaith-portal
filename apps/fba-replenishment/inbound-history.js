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
// 明細取得の途中でもディスクに落とす間隔。
// sql.js は保存のたびに DB 全体を書き直す (= その間イベントループが止まる) ので、
// 頻繁すぎるとジョブ状態の応答まで詰まる。落ちた分は items_synced_at が NULL のまま
// 残って次回取り直されるだけなので、間隔は広めでよい。
const FLUSH_EVERY = 100;

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

/**
 * 一時的な失敗 (スロットル / 5xx / 通信断) だけ指数バックオフで粘る。
 * それ以外 (認証・権限・不正パラメータ) は粘っても無駄なので即座に投げる。
 */
function retryableInfo(e) {
  const status = e?.statusCode ?? e?.status ?? e?.response?.status ?? null;
  const code = String(e?.code || '');
  const msg = String(e?.message || e);
  if (status === 429 || status === 408 || (status >= 500 && status < 600)) return { retryable: true, status };
  if (/throttl|quota|too many requests|rate exceeded/i.test(msg)) return { retryable: true, status: 429 };
  if (/ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|ECONNREFUSED|socket hang up|network/i.test(code + ' ' + msg)) {
    return { retryable: true, status: null };
  }
  return { retryable: false, status };
}

/** Retry-After (秒 or HTTP-date) を尊重する。無ければ null。 */
function retryAfterMs(e) {
  const h = e?.headers?.['retry-after'] ?? e?.response?.headers?.['retry-after'];
  if (!h) return null;
  const sec = Number(h);
  if (Number.isFinite(sec)) return Math.min(sec * 1000, 60000);
  const at = Date.parse(h);
  return Number.isFinite(at) ? Math.min(Math.max(at - Date.now(), 0), 60000) : null;
}

async function callWithRetry(apiPath, label, maxRetries = 4) {
  const sp = getClient();
  let waitMs = 2000;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res = await sp.callAPI({ api_path: apiPath, method: 'GET' });
      return res?.payload || res;
    } catch (e) {
      const { retryable } = retryableInfo(e);
      if (!retryable || attempt === maxRetries) throw e;
      // ジッタ: 複数プロセスが同時にリトライして再び詰まるのを避ける
      const wait = retryAfterMs(e) ?? Math.round(waitMs * (0.8 + Math.random() * 0.4));
      console.log(`[InboundHistory] ${label} 再試行 → ${Math.round(wait / 1000)}秒待機 (${attempt + 1}/${maxRetries}): ${e?.message || e}`);
      await sleep(wait);
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
    // 打ち切った結果をそのまま保存すると「取れた分が全部」に見えてしまう。
    // 不完全な取得は成功として扱わない。
    throw new Error(`シップメント一覧がページ上限 ${maxPages} に到達しました (取得済み ${all.length}件)。範囲を狭めて再実行してください`);
  }
  return all;
}

/**
 * 1シップメントの明細を取得。
 *
 * ⚠️ v0 のページングの癖 (2026-08-05 実測):
 *   - /shipments/{id}/items は ItemData と一緒に NextToken を必ず返す。
 *     が、その NextToken を**同じエンドポイントに渡しても同じ1ページ目が返る**。
 *     しかも token の値は毎回変わるので「token が同じなら終わり」という判定も効かず、
 *     素直にループを書くと同じ63件を延々と取り続ける。
 *   - 続きは別エンドポイント /fba/inbound/v0/shipmentItems?QueryType=NEXT_TOKEN でしか取れない。
 *     実測ではそちらが 0件 を返し、1ページ目の63件で完結していた。
 * つまり「NextToken がある = 続きがある」ではない。続きの有無は shipmentItems 側で確かめる。
 */
export async function fetchShipmentItems(shipmentId, maxPages = 20) {
  const mp = marketplaceId();
  const first = await callWithRetry(
    `/fba/inbound/v0/shipments/${shipmentId}/items?${new URLSearchParams({ MarketplaceId: mp }).toString()}`,
    `items ${shipmentId} p1`
  );
  const all = [...(first?.ItemData || [])];
  const seen = new Set(all.map(i => i.SellerSKU));
  let token = first?.NextToken || null;
  let page = 1;

  while (token && page < maxPages) {
    await sleep(CALL_INTERVAL_MS);
    const qs = new URLSearchParams({ MarketplaceId: mp, QueryType: 'NEXT_TOKEN', NextToken: token }).toString();
    const payload = await callWithRetry(`/fba/inbound/v0/shipmentItems?${qs}`, `items ${shipmentId} p${page + 1}`);
    // shipmentItems は他シップメントの行も返しうるので、対象のものだけ拾う
    const batch = (payload?.ItemData || []).filter(i => !i.ShipmentId || i.ShipmentId === shipmentId);
    if (batch.length === 0) break; // 続きなし = ここで完結

    // 同じページが返り続けるAPIの癖に対する安全弁。新しいSKUが1件も無ければ進んでいない。
    const fresh = batch.filter(i => !seen.has(i.SellerSKU));
    if (fresh.length === 0) break;
    for (const i of fresh) seen.add(i.SellerSKU);
    all.push(...fresh);

    token = payload?.NextToken || null;
    page += 1;
  }

  if (token && page >= maxPages) {
    throw new Error(`${shipmentId} の明細がページ上限 ${maxPages} に到達しました (取得済み ${all.length}行)`);
  }
  return all;
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
    historyDays = 730,
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
  // 明細は1シップメントあたり1〜2リクエスト。集計対象外の古いシップメントまで取ると
  // 初回が1時間コースになるので、遡る範囲を切る (既定2年)。
  const minCreatedDate = new Date(Date.now() + 9 * 3600 * 1000 - historyDays * 86400000)
    .toISOString().slice(0, 10);
  let targets = getShipmentsNeedingItemSync({ all: allItems, minCreatedDate });
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
