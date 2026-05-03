/**
 * FBA収益性分析ツール — ルーター
 *
 * 全FBA在庫の利益率を一覧表示し、低利益率商品を炙り出す
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();

// --- ミニPC接続（SP-API実行用） ---
const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';

// 起動時の env fail-fast: 必須envが空ならログ警告（CF Accessは公開エンドポイントなら任意）
const REQUIRED_ENV = ['CF_ACCESS_CLIENT_ID', 'CF_ACCESS_CLIENT_SECRET', 'WAREHOUSE_SERVICE_TOKEN'];
const missingEnv = REQUIRED_ENV.filter(k => !process.env[k]);
if (missingEnv.length > 0) {
  console.error(`[FBA-Profit] 必須環境変数が未設定: ${missingEnv.join(', ')} — /api/listings と /api/fees は動作しません`);
}

function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}

class MiniPCError extends Error {
  constructor({ message, userMessage, statusCode, errorCode, requestId, upstreamRequestId } = {}) {
    super(message || 'MiniPCError');
    this.name = 'MiniPCError';
    this.userMessage = userMessage || 'バックエンドエラーが発生しました';
    this.statusCode = Number.isInteger(statusCode) ? statusCode : 502;
    this.errorCode = errorCode || 'UNKNOWN';
    this.requestId = requestId || null;
    this.upstreamRequestId = upstreamRequestId || null;
  }
}

// retry: 総試行回数（再試行回数ではない）。省略時は3回試行。retry:1 は再試行なしで1回のみ試行。
async function callResearchAPI(path, { timeout = 60000, retry } = {}) {
  if (missingEnv.length > 0) {
    throw new MiniPCError({
      message: `Missing env: ${missingEnv.join(',')}`,
      userMessage: 'バックエンド設定エラー（管理者に連絡してください）',
      statusCode: 503,
      errorCode: 'CONFIG_ERROR',
    });
  }
  const url = `${WAREHOUSE_URL}/service-api/research${path}`;
  const requestId = `r-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const headers = { ...getServiceHeaders(), 'x-request-id': requestId };
  const maxAttempts = retry ?? 3;

  let lastError;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'GET',
        headers,
        redirect: 'manual',
        signal: AbortSignal.timeout(timeout),
      });
      const ct = res.headers.get('content-type') || '';

      if (res.status === 302 || res.status === 303) {
        throw new MiniPCError({
          message: `CF Access redirect ${res.status} → ${res.headers.get('location') || ''} req=${requestId}`,
          userMessage: 'バックエンド認証エラー（管理者に連絡してください）',
          statusCode: 502,
          errorCode: 'CF_ACCESS_ERROR',
          requestId,
        });
      }
      // 401/403: CF Access 一過性失敗の可能性があるため初回のみリトライ、2回目以降は hard fail
      if (res.status === 401 || res.status === 403) {
        if (attempt === 1 && maxAttempts > 1) {
          lastError = new MiniPCError({
            message: `認証失敗 HTTP ${res.status} (初回試行) req=${requestId}`,
            userMessage: 'バックエンド認証エラー（管理者に連絡してください）',
            statusCode: 502,
            errorCode: 'AUTH_FAILED',
            requestId,
          });
          await new Promise(r => setTimeout(r, 500 + Math.random() * 300));
          continue;
        }
        throw new MiniPCError({
          message: `認証失敗 HTTP ${res.status} req=${requestId}`,
          userMessage: 'バックエンド認証エラー（管理者に連絡してください）',
          statusCode: 502,
          errorCode: 'AUTH_FAILED',
          requestId,
        });
      }

      // 4xx/5xx でも JSON なら構造化エラーをパース
      let upstreamJson = null;
      if (ct.includes('application/json')) {
        upstreamJson = await res.json().catch(() => null);
      }

      // リトライ対象: 429 / 502 / 503 / 504
      if ([429, 502, 503, 504].includes(res.status)) {
        const upstreamReqId = upstreamJson?.requestId;
        const upstreamMsg = upstreamJson?.message || upstreamJson?.error || `HTTP ${res.status}`;
        lastError = new MiniPCError({
          message: `upstream ${res.status}: ${upstreamMsg} req=${requestId} upstream=${upstreamReqId || 'n/a'}`,
          userMessage: 'バックエンド一時障害（時間をおいて再試行してください）',
          statusCode: 502,
          errorCode: 'UPSTREAM_UNAVAILABLE',
          requestId,
          upstreamRequestId: upstreamReqId,
        });
        if (attempt < maxAttempts) {
          await new Promise(r => setTimeout(r, Math.min(500 * 2 ** (attempt - 1), 4000) + Math.random() * 300));
          continue;
        }
        throw lastError;
      }

      if (!res.ok) {
        const upstreamReqId = upstreamJson?.requestId;
        const upstreamMsg = upstreamJson?.message || upstreamJson?.error || (await res.text().catch(() => '')).slice(0, 200);
        throw new MiniPCError({
          message: `ミニPC HTTP ${res.status}: ${upstreamMsg} req=${requestId} upstream=${upstreamReqId || 'n/a'}`,
          userMessage: 'バックエンド処理エラー（時間をおいて再試行してください）',
          statusCode: 502,
          errorCode: 'UPSTREAM_ERROR',
          requestId,
          upstreamRequestId: upstreamReqId,
        });
      }

      if (!ct.includes('application/json')) {
        const txt = await res.text().catch(() => '');
        throw new MiniPCError({
          message: `レスポンス形式異常 (ct=${ct || 'none'}): ${txt.slice(0, 200)} req=${requestId}`,
          userMessage: 'バックエンド応答形式エラー',
          statusCode: 502,
          errorCode: 'INVALID_RESPONSE',
          requestId,
        });
      }

      // 2xx + JSON: shape検証
      const json = upstreamJson;
      if (!json || typeof json !== 'object') {
        throw new MiniPCError({
          message: `JSONパース失敗 req=${requestId}`,
          userMessage: 'バックエンド応答形式エラー',
          statusCode: 502,
          errorCode: 'INVALID_RESPONSE',
          requestId,
        });
      }
      if (json.ok !== true) {
        throw new MiniPCError({
          message: `envelope error: ${json.error || ''}: ${json.message || ''} req=${requestId} upstream=${json.requestId || 'n/a'}`,
          userMessage: 'バックエンド処理エラー',
          statusCode: 502,
          errorCode: json.error || 'UPSTREAM_ENVELOPE_ERROR',
          requestId,
          upstreamRequestId: json.requestId,
        });
      }
      if (json.result === undefined || json.result === null) {
        throw new MiniPCError({
          message: `envelope.result が空 req=${requestId} upstream=${json.requestId || 'n/a'}`,
          userMessage: 'バックエンド応答形式エラー',
          statusCode: 502,
          errorCode: 'EMPTY_RESULT',
          requestId,
          upstreamRequestId: json.requestId,
        });
      }
      return json.result;
    } catch (e) {
      // MiniPCError 以外のネットワーク例外をリトライ判定
      if (!(e instanceof MiniPCError)) {
        const msg = e?.message || String(e);
        const isRetryable = e?.name === 'TimeoutError' || /aborted|timeout|ECONNREFUSED|ECONNRESET|ENOTFOUND|fetch failed/i.test(msg);
        if (isRetryable && attempt < maxAttempts) {
          lastError = e;
          await new Promise(r => setTimeout(r, Math.min(500 * 2 ** (attempt - 1), 4000) + Math.random() * 300));
          continue;
        }
        throw new MiniPCError({
          message: `ネットワークエラー: ${msg} req=${requestId}`,
          userMessage: 'バックエンド通信エラー（時間をおいて再試行してください）',
          statusCode: 502,
          errorCode: 'NETWORK_ERROR',
          requestId,
        });
      }
      throw e;
    }
  }
  throw lastError || new MiniPCError({
    message: 'callResearchAPI: unknown error',
    userMessage: 'バックエンド通信エラー',
    statusCode: 502,
    errorCode: 'UNKNOWN',
    requestId,
  });
}

// クライアント向けエラーレスポンスを返す（ログ詳細はサーバー側のみ）
function sendError(res, e, context) {
  const isMiniPC = e instanceof MiniPCError;
  const statusCode = isMiniPC ? e.statusCode : 500;
  const userMessage = isMiniPC ? e.userMessage : 'サーバーエラーが発生しました';
  const errorCode = isMiniPC ? e.errorCode : 'INTERNAL_ERROR';
  const reqId = isMiniPC ? e.requestId : null;
  const upstreamReqId = isMiniPC ? e.upstreamRequestId : null;

  console.error(
    `[FBA-Profit] ${context} error: ${e.message}` +
      (reqId ? ` req=${reqId}` : '') +
      (upstreamReqId ? ` upstream=${upstreamReqId}` : '')
  );
  if (!isMiniPC && e.stack) console.error(e.stack);

  res.status(statusCode).json({ error: userMessage, errorCode });
}

// ===== メイン画面 =====
router.get('/', (req, res) => {
  res.render('fba-profitability', {
    title: 'FBA収益性分析',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ===== API: FBA出品一覧取得 + 原価突合 =====
router.post('/api/listings', async (req, res) => {
  try {
    console.log('[FBA-Profit] 出品レポート取得開始...');
    // レポート作成→ポーリング(最大5分)→ダウンロードはミニPC側で実行。Render側は最大6分待機
    // ※ ジョブ化していないため Render の HTTP接続を5-6分専有する。デプロイ/再起動時は中断される
    const report = await callResearchAPI('/active-listings-report', { timeout: 360000, retry: 1 });
    if (!Array.isArray(report?.listings)) {
      throw new MiniPCError({
        message: `report.listings が配列でない: ${typeof report?.listings}`,
        userMessage: 'バックエンド応答形式エラー',
        statusCode: 502,
        errorCode: 'INVALID_REPORT_SHAPE',
      });
    }
    console.log(`[FBA-Profit] 全出品: ${report.totalCount}件`);

    // FBA出品のみフィルタ（日本語ヘッダー「フルフィルメント・チャンネル」にも対応）
    const fbaListings = report.listings.filter(r => {
      const fc = (
        r['fulfillment-channel'] || r['fulfillment channel'] ||
        r['フルフィルメント・チャンネル'] || r['フルフィルメントチャンネル'] || ''
      ).toLowerCase();
      return fc.includes('amazon') || fc === 'afn' || fc.includes('fba') ||
             fc.includes('default') || fc === 'amazon_na' || fc === 'amazon_jp';
    });
    console.log(`[FBA-Profit] FBA出品: ${fbaListings.length}件`);

    // warehouse-mirror.db から原価データ取得
    let costMap = new Map();
    try {
      const db = getMirrorDB();

      // SKU解決マップ: seller_sku → ne_code
      // 既定: mirror_sku_resolved (master優先 + sku_map fallback)
      // env WAREHOUSE_SKU_SOURCE=legacy で旧 mirror_sku_map 直参照に戻せる
      const useLegacySku = process.env.WAREHOUSE_SKU_SOURCE === 'legacy';
      const skuMappings = useLegacySku
        ? db.prepare('SELECT seller_sku, ne_code, 数量 FROM mirror_sku_map').all()
        : db.prepare('SELECT seller_sku, ne_code, quantity AS 数量 FROM mirror_sku_resolved').all();
      const skuToNe = new Map();
      for (const m of skuMappings) {
        if (!skuToNe.has(m.seller_sku?.toLowerCase())) {
          skuToNe.set(m.seller_sku?.toLowerCase(), []);
        }
        // 数量検証: NULL→1扱い、0/負数/非整数→null (invalid)
        const rawQty = m.数量;
        const validQty = (rawQty == null) ? 1
          : (Number.isFinite(rawQty) && rawQty > 0) ? rawQty : null;
        skuToNe.get(m.seller_sku?.toLowerCase()).push({ ne_code: m.ne_code, qty: validQty, rawQty });
      }

      // mirror_products: 商品コード → 原価, 消費税率
      const products = db.prepare('SELECT 商品コード, 原価, 原価ソース, 原価状態, 消費税率 FROM mirror_products').all();
      const productMap = new Map();
      for (const p of products) {
        productMap.set(p.商品コード?.toLowerCase(), p);
      }

      // FBA SKU ごとに原価を計算
      for (const listing of fbaListings) {
        const sku = (listing['seller-sku'] || listing['seller sku'] || listing['出品者SKU'] || listing['sku'] || '').trim();
        if (!sku) continue;

        const neEntries = skuToNe.get(sku.toLowerCase());
        if (neEntries && neEntries.length > 0) {
          // 数量invalid な構成品があれば cost計算スキップ
          const hasInvalidQty = neEntries.some(e => e.qty === null);
          if (hasInvalidQty) {
            // costMap には入れない (UI 上で原価未解決として警告)
            continue;
          }
          // セット商品: 構成品の原価 × 数量の合計
          let totalCost = 0;
          let taxRate = 10;
          let allFound = true;
          let costSource = '';

          for (const entry of neEntries) {
            const prod = productMap.get(entry.ne_code?.toLowerCase());
            if (prod && prod.原価 != null) {
              totalCost += prod.原価 * entry.qty;
              taxRate = prod.消費税率 ?? 10;
              costSource = prod.原価ソース || '';
            } else {
              allFound = false;
            }
          }

          // Codex指摘D: totalCost > 0 制約を外す (正当な0円原価SKUを落とさない)
          if (allFound) {
            costMap.set(sku.toLowerCase(), {
              cost: totalCost,
              taxRate,
              costSource,
              neCode: neEntries.map(e => e.ne_code).join(', '),
            });
          }
        } else {
          // SKU = NE商品コードの場合もある
          const prod = productMap.get(sku.toLowerCase());
          if (prod && prod.原価 != null) {
            costMap.set(sku.toLowerCase(), {
              cost: prod.原価,
              taxRate: prod.消費税率 ?? 10,
              costSource: prod.原価ソース || '',
              neCode: sku,
            });
          }
        }
      }
      console.log(`[FBA-Profit] 原価マッチ: ${costMap.size}/${fbaListings.length}件`);
    } catch (e) {
      console.error('[FBA-Profit] warehouse.db アクセスエラー:', e.message);
    }

    // レスポンス組み立て（日本語ヘッダー対応）
    const items = fbaListings.map(listing => {
      const sku = (listing['seller-sku'] || listing['seller sku'] || listing['出品者SKU'] || listing['sku'] || '').trim();
      const asin = (listing['asin1'] || listing['asin'] || listing['商品ID'] || '').trim();
      const price = parseFloat(listing['price'] || listing['your-price'] || listing['価格'] || '0') || 0;
      const productName = listing['item-name'] || listing['item name'] || listing['product-name'] || listing['商品名'] || '';
      const quantity = parseInt(listing['quantity'] || listing['afn-fulfillable-quantity'] || listing['在庫数'] || listing['数量'] || '0') || 0;

      const costData = costMap.get(sku.toLowerCase());

      return {
        sku,
        asin,
        productName,
        price,
        quantity,
        cost: costData?.cost ?? null,
        taxRate: costData?.taxRate ?? null,
        costSource: costData?.costSource ?? null,
        neCode: costData?.neCode ?? null,
      };
    }).filter(item => item.asin); // ASINなしは除外

    res.json({ success: true, items, total: items.length });
  } catch (e) {
    sendError(res, e, '/api/listings');
  }
});

// ===== API: 手数料バッチ取得 =====
// フロントエンドから少しずつ呼ぶ（SP-APIレート制限対策）
router.post('/api/fees', async (req, res) => {
  const { items } = req.body; // [{ asin, price }]
  if (!items || !Array.isArray(items)) {
    return res.status(400).json({ error: 'items配列が必要です' });
  }

  const results = [];
  for (const item of items) {
    try {
      const qs = `?asin=${encodeURIComponent(item.asin)}&price=${encodeURIComponent(item.price)}&isFba=true`;
      // retry: 1 で1件あたりの最悪待ち時間を 30秒に抑える（5件バッチで最悪2.5分）
      const fees = await callResearchAPI(`/fees${qs}`, { timeout: 30000, retry: 1 });
      results.push({
        asin: item.asin,
        success: true,
        ...fees,
      });
    } catch (e) {
      const isMiniPC = e instanceof MiniPCError;
      const reqId = isMiniPC ? e.requestId : null;
      const upstreamReqId = isMiniPC ? e.upstreamRequestId : null;
      console.error(
        `[FBA-Profit] /api/fees error (${item.asin}): ${e.message}` +
          (reqId ? ` req=${reqId}` : '') +
          (upstreamReqId ? ` upstream=${upstreamReqId}` : '')
      );
      results.push({
        asin: item.asin,
        success: false,
        error: isMiniPC ? e.userMessage : '手数料取得エラー',
        errorCode: isMiniPC ? e.errorCode : 'INTERNAL_ERROR',
      });
    }
    // レート制限対策: 1リクエストごとに少し待つ
    if (items.length > 1) {
      await new Promise(r => setTimeout(r, 200));
    }
  }

  res.json({ results });
});

// ===== API: 原価手動更新 =====
router.post('/api/update-cost', (req, res) => {
  const { sku, cost, taxRate } = req.body;
  if (!sku || cost === undefined) {
    return res.status(400).json({ error: 'sku と cost は必須です' });
  }

  try {
    const db = getMirrorDB();
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

    // mirror_products に即時反映
    db.prepare(
      "UPDATE mirror_products SET 原価 = ?, 原価ソース = '例外', 原価状態 = 'OVERRIDDEN', updated_at = ? WHERE 商品コード = ?"
    ).run(parseFloat(cost), now, sku);

    res.json({ ok: true, sku, cost: parseFloat(cost) });
  } catch (e) {
    sendError(res, e, '/api/update-cost');
  }
});

export default router;
