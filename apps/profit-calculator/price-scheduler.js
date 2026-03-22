/**
 * 価格改定ワーカー — SQS通知ベース（プライスター方式）
 *
 * フロー:
 *   1. AWS SQSからANY_OFFER_CHANGED通知をロングポーリング
 *   2. 通知ペイロードからASIN・競合価格・BuyBox価格を抽出
 *   3. DBから該当商品を検索（ASINで照合）
 *   4. price-engine で新価格を計算
 *   5. 価格変更が必要なら updatePrice で更新
 *   6. price_history に記録
 */
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { updatePrice } from './sp-api.js';
import { calculateNewPrice, parseNotificationPayload } from './price-engine.js';
import { initDb, updateProductPriceInfo, savePriceHistory } from './db.js';

const QUEUE_URL = process.env.AWS_SQS_QUEUE_URL;
const REGION = process.env.AWS_SQS_REGION || 'ap-northeast-1';

let sqsClient = null;
let isRunning = false;
let shouldStop = false;
let dbInitialized = false;

// 処理済み通知IDの重複排除キャッシュ（直近1000件）
const processedIds = new Set();
const MAX_CACHE_SIZE = 1000;

// 統計情報
const stats = {
  received: 0,
  processed: 0,
  updated: 0,
  skipped: 0,
  errors: 0,
  lastProcessedAt: null,
};

function now() {
  return new Date().toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' });
}

/**
 * DB内の商品をASINで検索（キャッシュ付き）
 */
let productCache = null;
let cacheExpiry = 0;

async function getProductByAsin(asin) {
  // 5分間キャッシュ
  if (!productCache || Date.now() > cacheExpiry) {
    const { getTrackingProducts } = await import('./db.js');
    const products = getTrackingProducts();
    productCache = new Map();
    for (const p of products) {
      if (p.asin) {
        // 同じASINで複数SKUある場合もあるのでArrayで保持
        if (!productCache.has(p.asin)) {
          productCache.set(p.asin, []);
        }
        productCache.get(p.asin).push(p);
      }
    }
    cacheExpiry = Date.now() + 5 * 60 * 1000;
    console.log(`[PriceWorker] 商品キャッシュ更新: ${productCache.size} ASINs`);
  }
  return productCache.get(asin) || [];
}

/** キャッシュを強制リフレッシュ */
export function refreshProductCache() {
  productCache = null;
  cacheExpiry = 0;
}

/**
 * 1件の通知を処理
 */
async function processNotification(notification) {
  const offersData = parseNotificationPayload(notification);
  if (!offersData) {
    console.warn('[PriceWorker] 通知ペイロード解析失敗');
    return;
  }

  const products = await getProductByAsin(offersData.asin);
  if (products.length === 0) {
    // 追従対象でない商品 → スキップ
    stats.skipped++;
    return;
  }

  for (const product of products) {
    try {
      const { newPrice, reason, competitorPrice, buyBoxPrice } = calculateNewPrice(product, offersData);

      // チェック日時を更新
      const updateData = {
        last_checked_at: now(),
        competitor_price: competitorPrice,
      };

      if (newPrice !== null) {
        // 価格更新実行
        const result = await updatePrice({ sku: product.sku, price: newPrice });

        if (result.status === 'ACCEPTED') {
          updateData.selling_price = newPrice;
          updateData.last_price_changed_at = now();

          savePriceHistory({
            productId: product.id,
            asin: product.asin,
            sku: product.sku,
            oldPrice: product.selling_price,
            newPrice,
            reason,
            mode: product.price_tracking,
            competitorPrice,
            buyBoxPrice,
          });

          stats.updated++;
          console.log(`[PriceWorker] ${product.asin} (${product.sku}): ¥${product.selling_price} → ¥${newPrice} [${reason}]`);

          // キャッシュ内の価格も更新
          product.selling_price = newPrice;
        } else {
          console.error(`[PriceWorker] ${product.asin} 価格更新失敗: ${result.status}`, result.issues);
          stats.errors++;
        }
      }

      updateProductPriceInfo(product.id, updateData);
      stats.processed++;

    } catch (err) {
      console.error(`[PriceWorker] ${product.asin} 処理エラー:`, err.message);
      stats.errors++;
    }
  }
}

/**
 * SQSポーリングループ（メイン）
 */
async function pollLoop() {
  while (!shouldStop) {
    try {
      const { Messages } = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: QUEUE_URL,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 20,       // ロングポーリング
          VisibilityTimeout: 60,
        })
      );

      if (!Messages || Messages.length === 0) continue;

      stats.received += Messages.length;

      for (const message of Messages) {
        try {
          // メッセージ解析（SQSラッパーを剥がす）
          let payload = JSON.parse(message.Body);
          if (typeof payload.Message === 'string') {
            payload = JSON.parse(payload.Message);
          }

          // 重複排除
          const notificationId = payload.NotificationMetadata?.NotificationId;
          if (notificationId && processedIds.has(notificationId)) {
            // 重複 → 削除のみ
            await sqsClient.send(new DeleteMessageCommand({
              QueueUrl: QUEUE_URL,
              ReceiptHandle: message.ReceiptHandle,
            }));
            continue;
          }

          // 通知処理
          if (payload.NotificationType === 'ANY_OFFER_CHANGED') {
            await processNotification(payload);
          }

          // 重複排除キャッシュに追加
          if (notificationId) {
            processedIds.add(notificationId);
            if (processedIds.size > MAX_CACHE_SIZE) {
              const first = processedIds.values().next().value;
              processedIds.delete(first);
            }
          }

          // 処理完了 → メッセージ削除
          await sqsClient.send(new DeleteMessageCommand({
            QueueUrl: QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          }));

          stats.lastProcessedAt = now();

        } catch (err) {
          console.error('[PriceWorker] メッセージ処理エラー:', err.message);
          stats.errors++;
          // 削除しない → VisibilityTimeout後に再配信
        }
      }

    } catch (err) {
      if (shouldStop) break;
      console.error('[PriceWorker] SQS受信エラー:', err.message);
      // 接続エラー時は少し待ってリトライ
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

/**
 * ワーカー開始
 */
export function startPriceWorker() {
  if (!QUEUE_URL) {
    console.log('[PriceWorker] AWS_SQS_QUEUE_URL が未設定 → 価格改定ワーカーは無効');
    return;
  }

  if (isRunning) {
    console.log('[PriceWorker] 既に起動中');
    return;
  }

  sqsClient = new SQSClient({
    region: REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });

  isRunning = true;
  shouldStop = false;

  // DB初期化してからポーリング開始
  initDb().then(() => {
    dbInitialized = true;
    console.log(`[PriceWorker] SQSポーリング開始 (${QUEUE_URL})`);
    pollLoop().then(() => {
      isRunning = false;
      console.log('[PriceWorker] ポーリング終了');
    });
  }).catch(err => {
    console.error('[PriceWorker] DB初期化失敗:', err.message);
    isRunning = false;
  });
}

/**
 * ワーカー停止
 */
export function stopPriceWorker() {
  if (!isRunning) return;
  shouldStop = true;
  console.log('[PriceWorker] 停止リクエスト送信');
}

/**
 * ステータス取得
 */
export function getWorkerStatus() {
  return {
    running: isRunning,
    enabled: !!QUEUE_URL,
    stats: { ...stats },
  };
}
