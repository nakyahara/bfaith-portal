/**
 * Yahoo!ショッピング受注データ取得（ミニPC側）
 *
 * VPSプロキシ経由でYahoo Shopping APIからデータを取得し、warehouse.dbに投入する。
 *
 * 使い方:
 *   node apps/warehouse/yahoo-orders.js [days]
 *   node apps/warehouse/yahoo-orders.js backfill [startDate] [endDate]
 *
 * デフォルト: 直近7日分
 */
import 'dotenv/config';
import { parseStringPromise } from 'xml2js';
import { initDB, getDB, updateSyncMeta } from './db.js';

// デフォルトは統合プロキシ 8080 (他12ファイルと同一)。旧 8081 は閉鎖済みの死番だった
const YAHOO_PROXY_URL = process.env.YAHOO_PROXY_URL || 'http://133.167.122.198:8080';
const YAHOO_PROXY_SECRET = process.env.YAHOO_PROXY_SECRET || process.env.AUPAY_PROXY_SECRET || '';

function now() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function formatDateYMD(date) {
  return date.toISOString().slice(0, 10).replace(/-/g, '');
}

// ─── テーブル確認 ───

function ensureTables() {
  const db = getDB();
  // テーブルはdb.jsのcreateTablesで作成済み。念のためインデックスだけ確認
  try {
    db.exec('CREATE INDEX IF NOT EXISTS idx_yh_orders_order ON raw_yahoo_orders(order_id)');
  } catch {}
}

// ─── VPSプロキシ呼び出し ───

async function proxyGet(path) {
  const url = `${YAHOO_PROXY_URL}${path}`;
  const res = await fetch(url, {
    headers: { 'X-Proxy-Secret': YAHOO_PROXY_SECRET },
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Proxy error ${res.status}: ${err}`);
  }
  return res;
}

async function proxyPost(path, body) {
  const url = `${YAHOO_PROXY_URL}${path}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'X-Proxy-Secret': YAHOO_PROXY_SECRET,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Proxy error ${res.status}: ${err}`);
  }
  return res.json();
}

// ─── orderList → 注文IDリスト取得 ───

async function getOrderIds(startDate, endDate) {
  console.log(`[Yahoo] orderList: ${startDate} → ${endDate}`);
  const res = await proxyGet(`/yahoo/orderList?startDate=${startDate}&endDate=${endDate}`);
  const xml = await res.text();

  const parsed = await parseStringPromise(xml, { explicitArray: false });
  // XML構造: Result > Status, Result > Search > TotalCount, Result > Search > OrderInfo[]
  const root = parsed?.Result || parsed?.ResultSet || parsed;

  // エラーチェック
  if (root?.Status && root.Status !== 'OK' && root.Status !== '0') {
    throw new Error(`orderList API error: ${root?.Message || JSON.stringify(root)}`);
  }

  // 注文ID抽出
  const orderIds = [];
  const search = root?.Search || root;
  let orderInfos = search?.OrderInfo || [];
  if (!Array.isArray(orderInfos)) orderInfos = [orderInfos];

  for (const oi of orderInfos) {
    if (oi?.OrderId) orderIds.push(oi.OrderId);
  }

  const totalCount = parseInt(search?.TotalCount || '0') || orderIds.length;

  console.log(`[Yahoo] orderList結果: ${orderIds.length}件 (Total: ${totalCount})`);
  return { orderIds, totalCount };
}

// ─── orderInfo → 注文詳細取得（バッチ） ───

async function getOrderDetails(orderIds) {
  if (!orderIds.length) return [];

  // VPSプロキシのバッチエンドポイントを使用（VPS側で1秒間隔を制御）
  // 大量の場合は50件ずつ分割
  const BATCH_SIZE = 50;
  const allResults = [];

  for (let i = 0; i < orderIds.length; i += BATCH_SIZE) {
    const batch = orderIds.slice(i, i + BATCH_SIZE);
    console.log(`[Yahoo] orderInfo: ${i + 1}-${i + batch.length} / ${orderIds.length}`);

    const data = await proxyPost('/yahoo/orderInfo', { orderIds: batch });

    for (const r of (data.results || [])) {
      try {
        const parsed = await parseStringPromise(r.xml, { explicitArray: false });
        allResults.push({ orderId: r.orderId, data: parsed });
      } catch (e) {
        console.log(`[Yahoo] XML parse error for ${r.orderId}: ${e.message}`);
      }
    }

    // バッチ間の待ち（VPS側でも1秒間隔あるが、念のため）
    if (i + BATCH_SIZE < orderIds.length) await sleep(2000);
  }

  return allResults;
}

// ─── DB投入 ───

function insertOrders(db, orders, batchId, windowStart, windowEnd) {
  const ts = now();

  const insertLog = db.prepare(`
    INSERT INTO raw_yahoo_orders_log (
      batch_id, source_window_start, source_window_end,
      order_id, order_time, last_update_time, order_status, pay_status, ship_status,
      total_price, pay_charge, ship_charge, discount, use_point,
      line_id, item_id, title, sub_code,
      unit_price, original_price, quantity, item_tax_ratio, coupon_discount,
      ingested_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const deleteCurrentOrder = db.prepare('DELETE FROM raw_yahoo_orders WHERE order_id = ?');
  const insertCurrent = db.prepare(`
    INSERT INTO raw_yahoo_orders (
      order_id, order_time, last_update_time, order_status, pay_status, ship_status,
      total_price, pay_charge, ship_charge, discount, use_point,
      line_id, item_id, title, sub_code,
      unit_price, original_price, quantity, item_tax_ratio, coupon_discount,
      synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  let logCount = 0, currentCount = 0;
  // Phase 1.1.1 fail-closed カウンタ (5/8-10 proxy regression 事故対応、Codex 推奨)
  let skippedInvalid = 0;
  let apiError = 0;

  const tx = db.transaction(() => {
    for (const { orderId, data } of orders) {
      // <Error> ルート検知 (proxy or Yahoo API がエラー XML 返した場合、5/8-10 事故の主因)
      const errorNode = data?.Error || data?.ResultSet?.Error || data?.Result?.Error;
      if (errorNode) {
        console.log(`[Yahoo] orderInfo API error for ${orderId}: code=${errorNode?.Code || '?'} msg=${errorNode?.Message || '?'}`);
        apiError++;
        continue;
      }

      const resultSet = data?.ResultSet || data?.result || data;
      const result = resultSet?.Result || {};

      // Status エラーチェック (既存)
      if (result?.Status && result.Status !== '0' && result.Status !== 'OK') {
        console.log(`[Yahoo] orderInfo Status error for ${orderId}: ${result?.Message || 'unknown'}`);
        apiError++;
        continue;
      }

      const orderInfo = result?.OrderInfo || resultSet?.OrderInfo || resultSet?.Order || resultSet;
      if (!orderInfo || typeof orderInfo !== 'object') {
        console.log(`[Yahoo] skip ${orderId}: OrderInfo missing or invalid`);
        skippedInvalid++;
        continue;
      }

      // 注文レベルの情報
      const orderTime = orderInfo.OrderTime || '';
      const lastUpdateTime = orderInfo.LastUpdateTime || '';
      const orderStatus = orderInfo.OrderStatus || '';
      const payStatus = orderInfo.Pay?.PayStatus || orderInfo.PayStatus || '';
      const shipStatus = orderInfo.Ship?.ShipStatus || orderInfo.ShipStatus || '';

      // 注文ヘッダ必須 field validation (Codex fail-closed 案、SubCode は任意)
      if (!orderTime || !orderStatus) {
        console.log(`[Yahoo] skip ${orderId}: order_time or status empty (order_time='${orderTime}' status='${orderStatus}')`);
        skippedInvalid++;
        continue;
      }

      const detail = orderInfo.Detail || {};
      const totalPrice = parseFloat(detail.TotalPrice || orderInfo.TotalPrice) || 0;
      const payCharge = parseFloat(detail.PayCharge || orderInfo.PayCharge) || 0;
      const shipCharge = parseFloat(detail.ShipCharge || orderInfo.ShipCharge) || 0;
      const discount = parseFloat(detail.Discount || orderInfo.Discount) || 0;
      const usePoint = parseFloat(detail.UsePoint || orderInfo.UsePoint) || 0;

      // 商品明細
      let items = orderInfo.Item || orderInfo.Items?.Item || [];
      if (!Array.isArray(items)) items = [items];
      if (!items.length || !items[0]) {
        // 明細なしは異常 (Codex fail-closed 案: 注文 skip)
        console.log(`[Yahoo] skip ${orderId}: items empty`);
        skippedInvalid++;
        continue;
      }

      // 明細必須 field validation (item_id / quantity / unit_price のいずれか欠落で注文 skip)
      let itemValid = true;
      for (const item of items) {
        const _itemId = item.ItemId || '';
        const _qty = parseInt(item.Quantity) || 0;
        const _price = parseFloat(item.UnitPrice) || 0;
        if (!_itemId || _qty <= 0 || _price <= 0) {
          console.log(`[Yahoo] skip ${orderId}: item field empty (item_id='${_itemId}' qty=${_qty} price=${_price})`);
          itemValid = false;
          break;
        }
      }
      if (!itemValid) {
        skippedInvalid++;
        continue;
      }

      // current: 注文ID単位でDELETE→INSERT
      deleteCurrentOrder.run(orderId);

      for (const item of items) {
        const lineId = parseInt(item.LineId) || 0;
        const itemId = (item.ItemId || '').toLowerCase();
        const title = item.Title || '';
        const subCode = item.SubCode || '';
        const unitPrice = parseFloat(item.UnitPrice) || 0;
        const originalPrice = parseFloat(item.OriginalPrice) || 0;
        const quantity = parseInt(item.Quantity) || 0;
        const itemTaxRatio = parseFloat(item.ItemTaxRatio) || 0;
        const couponDiscount = parseFloat(item.CouponDiscount) || 0;

        // log (append-only)
        insertLog.run(
          batchId, windowStart, windowEnd,
          orderId, orderTime, lastUpdateTime, orderStatus, payStatus, shipStatus,
          totalPrice, payCharge, shipCharge, discount, usePoint,
          lineId, itemId, title, subCode,
          unitPrice, originalPrice, quantity, itemTaxRatio, couponDiscount,
          ts
        );
        logCount++;

        // current
        insertCurrent.run(
          orderId, orderTime, lastUpdateTime, orderStatus, payStatus, shipStatus,
          totalPrice, payCharge, shipCharge, discount, usePoint,
          lineId, itemId, title, subCode,
          unitPrice, originalPrice, quantity, itemTaxRatio, couponDiscount,
          ts
        );
        currentCount++;
      }
    }
  });
  tx();

  return { logCount, currentCount, skippedInvalid, apiError };
}

// fail-closed 判定 (Codex 推奨、5/8-10 事故再発防止)
//   - fetched > 0 AND inserted = 0 → FATAL (proxy / API 障害の可能性、cron exit 1)
//   - api_error_rate > 50% → FATAL (Yahoo API 系統的 reject、cron exit 1)
//   - skip_ratio (skipped_invalid + api_error の合計 / fetched) > 5% → FATAL (Phase 1.3: 系統的問題の可能性)
//   - skipped_invalid > 0 (5% 以下) → warning (個別 record の一時的不完全レスポンス、翌日 cron で補完想定)
const SKIP_RATIO_FATAL_THRESHOLD = 0.05;
function evaluateFetchResult(label, fetched, currentCount, skippedInvalid, apiError) {
  const skipRatio = fetched > 0 ? (skippedInvalid + apiError) / fetched : 0;
  const skipRatioPct = (skipRatio * 100).toFixed(1);
  console.log(`[Yahoo] ${label}: fetched=${fetched} inserted=${currentCount} skipped_invalid=${skippedInvalid} api_error=${apiError} (skip_ratio=${skipRatioPct}%)`);
  if (fetched > 0 && currentCount === 0) {
    console.error(`[Yahoo] FATAL: fetched=${fetched} だが inserted=0、proxy or Yahoo API 障害の可能性 (5/8-10 同型事故防止)`);
    return 'fatal';
  }
  if (fetched > 0 && apiError / fetched > 0.5) {
    console.error(`[Yahoo] FATAL: api_error_rate=${(apiError / fetched * 100).toFixed(1)}% (50% 超)`);
    return 'fatal';
  }
  if (fetched > 0 && skipRatio > SKIP_RATIO_FATAL_THRESHOLD) {
    console.error(`[Yahoo] FATAL: skip_ratio=${skipRatioPct}% (${(SKIP_RATIO_FATAL_THRESHOLD * 100)}% 超、系統的問題の可能性)`);
    return 'fatal';
  }
  if (skippedInvalid > 0 || apiError > 0) {
    console.log(`[Yahoo] ⚠️  semantic warning: skipped_invalid=${skippedInvalid} api_error=${apiError} (skip_ratio=${skipRatioPct}%、翌日 cron で補完想定)`);
    return 'warning';
  }
  return 'ok';
}

// ─── メイン：日次取得 ───

async function fetchYahoo(days = 7) {
  // 設定不備・プロキシ死・トークン消滅を exit 0 で握りつぶすと「✅ 0件」が無限継続する
  // (期限警告も同一プロキシ依存で同時に沈黙する) ため、3経路とも fail として daily-sync に ❌ を出させる
  if (!YAHOO_PROXY_SECRET) {
    console.error('[Yahoo] FATAL: YAHOO_PROXY_SECRET（またはAUPAY_PROXY_SECRET）が未設定');
    process.exit(1);
  }

  console.log(`[Yahoo] 受注取得開始（直近${days}日）`);

  // ヘルスチェック
  try {
    const healthRes = await proxyGet('/yahoo/health');
    const health = await healthRes.json();
    if (!health.hasTokens) {
      console.error('[Yahoo] FATAL: トークン未初期化。VPSで /yahoo/token/init を実行してください');
      process.exit(1);
    }
  } catch (e) {
    console.error(`[Yahoo] FATAL: VPSプロキシ接続失敗: ${e.message}`);
    process.exit(1);
  }

  const db = getDB();
  ensureTables();

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);

  const startStr = formatDateYMD(startDate);
  const endStr = formatDateYMD(endDate);
  const batchId = `yahoo_${startStr}_${endStr}_${Date.now()}`;

  // Step 1: orderList で注文IDリスト取得
  const { orderIds } = await getOrderIds(startStr, endStr);

  if (!orderIds.length) {
    console.log('[Yahoo] 該当注文なし');
    updateSyncMeta('yahoo_last_sync', now());
    return 0;
  }

  // Step 2: orderInfo で詳細取得
  const orders = await getOrderDetails(orderIds);

  // Step 3: DB投入 (fail-closed 判定込み)
  const { logCount, currentCount, skippedInvalid, apiError } = insertOrders(db, orders, batchId, startStr, endStr);

  updateSyncMeta('yahoo_last_sync', now());
  console.log(`[Yahoo] 受注取得完了: log=${logCount}件, current=${currentCount}件 (注文${orderIds.length}件)`);

  const verdict = evaluateFetchResult('fetchYahoo', orderIds.length, currentCount, skippedInvalid, apiError);
  if (verdict === 'fatal') {
    process.exit(1);  // daily-sync runScript で fail として扱われる
  }
  return logCount;
}

// ─── バックフィル ───

async function backfill(startDateStr, endDateStr) {
  console.log(`[Yahoo] バックフィル: ${startDateStr} → ${endDateStr}`);

  const db = getDB();
  ensureTables();

  // 日単位で遡って取得（1日ずつ、進捗をsync_metaに記録）
  const end = new Date(endDateStr.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'));
  const start = new Date(startDateStr.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'));

  let totalLog = 0;
  let totalFetched = 0;
  let totalCurrent = 0;
  let totalSkipped = 0;
  let totalApiError = 0;
  let current = new Date(end);

  while (current >= start) {
    const dayEnd = new Date(current);
    const dayStart = new Date(current);
    dayStart.setDate(dayStart.getDate() - 6); // 7日分ずつ
    if (dayStart < start) dayStart.setTime(start.getTime());

    const dayStartStr = formatDateYMD(dayStart);
    const dayEndStr = formatDateYMD(dayEnd);
    const batchId = `yahoo_backfill_${dayStartStr}_${dayEndStr}_${Date.now()}`;

    try {
      const { orderIds } = await getOrderIds(dayStartStr, dayEndStr);
      if (orderIds.length) {
        const orders = await getOrderDetails(orderIds);
        const { logCount, currentCount, skippedInvalid, apiError } = insertOrders(db, orders, batchId, dayStartStr, dayEndStr);
        totalLog += logCount;
        totalFetched += orderIds.length;
        totalCurrent += currentCount;
        totalSkipped += skippedInvalid;
        totalApiError += apiError;
        console.log(`[Yahoo] バックフィル ${dayStartStr}-${dayEndStr}: ${logCount}件 (累計${totalLog})`);
      } else {
        console.log(`[Yahoo] バックフィル ${dayStartStr}-${dayEndStr}: 0件`);
      }

      // 進捗記録
      updateSyncMeta('yahoo_backfill_progress', dayStartStr);
    } catch (e) {
      console.log(`[Yahoo] バックフィル エラー ${dayStartStr}: ${e.message}`);
      console.log('[Yahoo] 途中再開可能: sync_meta.yahoo_backfill_progress を確認');
      break;
    }

    current.setDate(current.getDate() - 7);
    await sleep(3000); // バッチ間の待ち
  }

  console.log(`[Yahoo] バックフィル完了: ${totalLog}件`);

  const verdict = evaluateFetchResult('backfill', totalFetched, totalCurrent, totalSkipped, totalApiError);
  if (verdict === 'fatal') {
    process.exit(1);
  }
  return totalLog;
}

// ─── エントリポイント ───

async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || '7';

  await initDB();
  ensureTables();

  if (command === 'backfill') {
    const startDate = args[1] || formatDateYMD(new Date(Date.now() - 90 * 86400000));
    const endDate = args[2] || formatDateYMD(new Date());
    await backfill(startDate, endDate);
  } else {
    const days = parseInt(command) || 7;
    await fetchYahoo(days);
  }
}

// mall-orders.jsから呼び出せるようにexport
export { fetchYahoo, backfill };

main().catch(e => {
  console.error('[Yahoo] エラー:', e.message);
  process.exit(1);
});
