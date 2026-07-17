/**
 * 楽天RMS APIサービス
 * /service-api/rakuten-rms/* にマウント
 *
 * 楽天RMS APIのプロキシ。APIキー（serviceSecret/licenseKey）はミニPC側で管理。
 * LINEギフト同期・メルカリ同期の両方から利用される。
 *
 * レート制御は rakuten-client.js の rakutenRequest() に一元化されている
 * (プロセス内グローバル直列キュー + 429/Retry-After 尊重 + 5xx 自動リトライ)。
 *
 * rateLimitMiddleware('rakuten') は HTTP route レベルでの過負荷保護
 * (同時実行数 1 + queue 5 超で早期 429) として残してある。
 */
import { Router } from 'express';
import { rateLimitMiddleware } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';
import { rakutenRequest } from './rakuten-client.js';

const router = Router();

const DETAILS_BULK_MAX = 200;

// /items/all-codes は 100ページ × 1.1秒 = ~110秒かかるので 5分キャッシュ。
// 進行中の取得は in-flight promise を共有して同時実行重複を防ぐ。
// 強制再取得は ?refresh=1 で可能。
const ALL_CODES_CACHE_TTL_MS = 5 * 60 * 1000;
let allCodesCache = null; // { mapping, fetchedAt }
let allCodesInflight = null; // Promise<{mapping}>

async function fetchAllRakutenItemCodes() {
  const mapping = {};
  let cursorMark = '*';

  for (let page = 0; page < 100; page++) {
    const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=100`;
    const result = await rakutenRequest({ path: apiPath });

    if (result.status !== 200) {
      const err = extractRmsError(result.data);
      throw Object.assign(new Error(`HTTP ${result.status}${err.message ? `: ${err.message}` : ''}`), { rmsStatus: result.status });
    }

    const items = result.data.results || result.data.items || [];
    for (const r of items) {
      const item = r.item || r;
      if (item.manageNumber) {
        mapping[item.itemNumber || item.manageNumber] = item.manageNumber;
      }
    }

    if (!result.data.nextCursorMark || items.length === 0) break;
    cursorMark = result.data.nextCursorMark;
  }

  return mapping;
}

// 楽天 RMS のレスポンス本文からエラーコード / メッセージを安全に抽出 (型固定)
function extractRmsError(data) {
  if (data == null) return { errorCode: null, message: null };
  if (typeof data === 'string') return { errorCode: null, message: data.slice(0, 500) };
  if (typeof data === 'object') {
    // MessageModelList[].messageCode / message を最優先で拾う
    const list = Array.isArray(data.MessageModelList) ? data.MessageModelList : null;
    if (list && list.length > 0) {
      const err = list.find(m => m && m.messageType === 'ERROR') || list[0];
      return {
        errorCode: err && typeof err.messageCode === 'string' ? err.messageCode : null,
        message: err && typeof err.message === 'string' ? err.message : null,
      };
    }
    return {
      errorCode: typeof data.code === 'string' ? data.code : null,
      message: typeof data.message === 'string' ? data.message : null,
    };
  }
  return { errorCode: null, message: null };
}

// ==========================================
// 商品検索（LINEギフト同期で使用）
// ==========================================

router.get('/items/search', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const cursorMark = req.query.cursorMark || '*';
    const hits = req.query.hits || '100';
    const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=${hits}`;
    const result = await rakutenRequest({ path: apiPath });
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 全商品コード取得（メルカリ同期で使用）
// ==========================================

router.get('/items/all-codes', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const forceRefresh = req.query.refresh === '1';
    const now = Date.now();

    // キャッシュ有効ならそのまま返す
    if (!forceRefresh && allCodesCache && (now - allCodesCache.fetchedAt) < ALL_CODES_CACHE_TTL_MS) {
      const ageSec = Math.round((now - allCodesCache.fetchedAt) / 1000);
      return okResponse(res, {
        mapping: allCodesCache.mapping,
        count: Object.keys(allCodesCache.mapping).length,
        cached: true,
        ageSec,
      });
    }

    // 既に取得中なら同じ promise を待つ (同時実行による重複叩き防止)
    if (!allCodesInflight) {
      allCodesInflight = fetchAllRakutenItemCodes()
        .then((mapping) => {
          allCodesCache = { mapping, fetchedAt: Date.now() };
          return mapping;
        })
        .finally(() => { allCodesInflight = null; });
    }

    const mapping = await allCodesInflight;
    okResponse(res, { mapping, count: Object.keys(mapping).length, cached: false });
  } catch (e) {
    const status = e && e.rmsStatus ? e.rmsStatus : 502;
    errorResponse(res, { status, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 全SKU取得（粗利分析の sku_map 構築用）
// ==========================================
// 各商品のvariantsまで展開し、AM/AL/W 3コードをSKU粒度で返す。
// AM = merchantDefinedSkuId（システム連携用SKU番号）
// AL = variants のキー（SKU管理番号）
// W  = item.itemNumber（商品番号）

router.get('/items/all-skus', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const skus = [];
    let cursorMark = '*';
    let pageCount = 0;

    for (let page = 0; page < 100; page++) {
      const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=100`;
      const result = await rakutenRequest({ path: apiPath });

      if (result.status !== 200) {
        return errorResponse(res, { status: result.status, error: 'RMS_API_ERROR', message: `HTTP ${result.status}`, requestId: req.requestId });
      }

      const items = result.data.results || result.data.items || [];
      for (const r of items) {
        const item = r.item || r;
        const itemNumber = item.itemNumber || '';
        const manageNumber = item.manageNumber || '';
        const variants = item.variants || {};
        const variantKeys = Object.keys(variants);

        if (variantKeys.length === 0) {
          skus.push({
            itemNumber,
            manageNumber,
            skuManageNumber: manageNumber,
            systemSkuNumber: '',
          });
        } else {
          for (const key of variantKeys) {
            const v = variants[key] || {};
            skus.push({
              itemNumber,
              manageNumber,
              skuManageNumber: key,
              systemSkuNumber: v.merchantDefinedSkuId || '',
            });
          }
        }
      }

      pageCount++;
      if (!result.data.nextCursorMark || items.length === 0) break;
      cursorMark = result.data.nextCursorMark;
    }

    okResponse(res, { skus, count: skus.length, pages: pageCount });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 商品詳細取得（メルカリ同期で使用）
// ==========================================

router.get('/items/detail/:manageNumber', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const apiPath = `/es/2.0/items/manage-numbers/${encodeURIComponent(req.params.manageNumber)}`;
    const result = await rakutenRequest({ path: apiPath });
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 複数商品詳細（バルク）
// 個別失敗を握り潰さず failedCodes で明示返却。status: 'ok' | 'partial'
router.post('/items/details-bulk', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const { itemCodes } = req.body;
    if (!itemCodes || !Array.isArray(itemCodes)) {
      return errorResponse(res, { status: 400, error: 'VALIDATION', message: 'itemCodes required', requestId: req.requestId });
    }
    if (itemCodes.length > DETAILS_BULK_MAX) {
      return errorResponse(res, {
        status: 400,
        error: 'VALIDATION',
        message: `itemCodes too many (${itemCodes.length} > ${DETAILS_BULK_MAX})`,
        requestId: req.requestId,
      });
    }

    const results = [];
    const failedCodes = [];

    // 固定スキーマ: { code: string, status: number|null, errorCode: string|null, message: string|null }
    for (const code of itemCodes) {
      try {
        const apiPath = `/es/2.0/items/manage-numbers/${encodeURIComponent(code)}`;
        const result = await rakutenRequest({ path: apiPath });
        if (result.status === 200 && result.data) {
          results.push(result.data);
        } else {
          const errInfo = extractRmsError(result.data);
          failedCodes.push({
            code,
            status: result.status,
            errorCode: errInfo.errorCode,
            message: errInfo.message,
          });
        }
      } catch (e) {
        failedCodes.push({
          code,
          status: null,
          errorCode: null,
          message: String(e && e.message ? e.message : e),
        });
      }
    }

    okResponse(res, {
      items: results,
      count: results.length,
      failedCodes,
      failedCount: failedCodes.length,
      status: failedCodes.length === 0 ? 'ok' : 'partial',
    });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 問い合わせ管理API (inquirymng-api) — inquiry-hub 受信同期で使用
// ==========================================
// 設計原則: Render に楽天キーを置かない (rakuten-rms-proxy.js と同じ)。
// inquiry-hub (Render) はこの passthrough を Cloudflare Tunnel + サービストークン経由で叩く。
// ⚠️ read-only のみ。返信・既読化・完了化などの変更系 passthrough は意図的に作らない
//    (送信系は Step 4 で outbox worker と一体で設計する。無条件 passthrough は事故のもと)

router.get('/inquiries', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    // パラメータは許可リスト方式 (任意パス注入をさせない)
    const params = new URLSearchParams();
    for (const k of ['fromDate', 'toDate', 'limit', 'page', 'noMerchantReply']) {
      if (req.query[k] != null) params.set(k, String(req.query[k]));
    }
    const result = await rakutenRequest({ path: `/es/1.0/inquirymng-api/inquiries?${params.toString()}` });
    // RMS のレスポンスをステータスごと素通し (アダプター側が形を検証する)
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// ステータス
// ==========================================

router.get('/status', (req, res) => {
  okResponse(res, {
    hasCredentials: !!(process.env.RAKUTEN_SERVICE_SECRET && process.env.RAKUTEN_LICENSE_KEY),
  });
});

export default router;
