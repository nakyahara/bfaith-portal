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

// 問い合わせ返信 passthrough (inquiry-hub Step 4。変更系はこの1本のみ・厳重ガード)
// - 呼び出し元は inquiry-hub の outbox worker のみ (Render→CF Tunnel→サービストークン認証の内側)
// - inquiryNumber は実測形式 (shopId-日付-連番英字) の厳格検証。任意パス・任意ペイロードは通さない
// - 既読化/完了化/添付アップロードの passthrough は作らない (必要になった時点で個別設計)
router.post('/inquiry-reply', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const inquiryNumber = String(req.body?.inquiryNumber || '').trim();
    const message = String(req.body?.message || '');
    if (!/^\d{1,10}-\d{8}-\d{1,12}[a-z]?$/i.test(inquiryNumber)) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'inquiryNumber が実測形式ではありません', requestId: req.requestId });
    }
    if (!message.trim()) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'message が空です', requestId: req.requestId });
    }
    if (message.length > 10000) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'message が長すぎます (10000文字まで)', requestId: req.requestId });
    }
    // ⚠️ maxAttempts:1 必須 — rakutenRequest の既定は 429/5xx を自動リトライ (計4回) するが、
    // 送信系でのリトライは二重送信になり得る (5xx=送信された可能性が残る)。結果不明の扱いは
    // inquiry-hub の outbox (unknown→人手解決) に委ねる
    const result = await rakutenRequest({
      path: '/es/1.0/inquirymng-api/inquiry/reply',
      method: 'POST',
      body: { inquiryNumber, message },
      maxAttempts: 1,
    });
    console.log(`[rakuten-rms] inquiry-reply ${inquiryNumber} -> HTTP ${result.status}`);
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// ステータス
// ==========================================

// ==========================================
// 書込系 (product-hub P3 楽天自動出品、2026-07-26 権限smoke実証済)
//   - env RAKUTEN_RMS_WRITE_ENABLED が ON のときだけ有効 (fail-closed)
//   - 商品の新規作成は **必ず倉庫指定 (hideItem=true) に強制** — 公開は人が RMS 画面で行う
//   - 既存商品の上書きは拒否 (稼働中の商品ページを事故で潰さない)
//   - 削除は非公開 (hideItem=true) の商品だけ許可
// ==========================================

// 書込は manageNumber 単位で直列化する (High-1: GET確認→PUT の間に同じ番号への
// 別リクエストが入る競合を防ぐ)。RMS 側に条件付き作成が無いため、RMS画面など
// 外部からの同時作成は API 仕様上防げない — その場合は直後の内容確認で人が気づく前提。
const writeLocks = new Map(); // key -> Promise (チェーン末尾)
async function withManageNumberLock(mn, fn) {
  const prev = writeLocks.get(mn) || Promise.resolve();
  let release;
  const cur = new Promise((r) => { release = r; });
  const tail = prev.then(() => cur);
  writeLocks.set(mn, tail);
  await prev;
  try {
    return await fn();
  } finally {
    release();
    // 自分が末尾のままなら削除 (Codex low: cur と比較すると一致せず Map が増え続ける)
    if (writeLocks.get(mn) === tail) writeLocks.delete(mn);
  }
}

const WRITE_ON = new Set(['1', 'true', 'on', 'yes']);
function writeEnabled() {
  return WRITE_ON.has(String(process.env.RAKUTEN_RMS_WRITE_ENABLED ?? '').trim().toLowerCase());
}
function requireWrite(req, res, next) {
  if (!writeEnabled()) {
    return errorResponse(res, {
      status: 503, error: 'RMS_WRITE_DISABLED',
      message: 'RAKUTEN_RMS_WRITE_ENABLED が未設定のため書込は無効です (fail-closed)',
      requestId: req.requestId,
    });
  }
  next();
}

// 楽天の商品管理番号形式 (小文字英数とハイフン)。パス組み立てに使うので厳格に
const MANAGE_NUMBER_RE = /^[a-z0-9][a-z0-9\-]{0,31}$/;

// 単一商品 GET (書込前後の確認用。読み取りなので write gate 不要)
router.get('/items/manage-numbers/:manageNumber', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    const result = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 新規作成 (倉庫指定固定)。既存商品なら 409 で拒否
router.put('/items/manage-numbers/:manageNumber', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    const payload = req.body && typeof req.body === 'object' ? { ...req.body } : null;
    if (!payload || !payload.title) {
      return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'item payload (title 必須) が必要です', requestId: req.requestId });
    }

    // 確認→PUT を manageNumber 単位のロック内で行う (同番号への並行リクエストを直列化)
    const result = await withManageNumberLock(mn, async () => {
      // 稼働中ページの上書き防止: 既に存在するなら作らない
      const existing = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });
      if (existing.status === 200) return { block: 409 };
      if (existing.status !== 404) return { block: 502, detail: existing.status };

      // 公開状態はサーバー側で強制 (P3 スコープ: 非公開登録のみ。公開は人が RMS で)
      payload.hideItem = true;

      // 変更系は自動リトライしない (High-2: timeout 後の再送は既存確認を通らず
      // 上書きになりうる)。結果不明なら呼び出し側が GET で照合する
      return rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}`, method: 'PUT', body: payload, timeoutMs: 90_000, maxAttempts: 1 });
    });
    if (result.block === 409) {
      return errorResponse(res, {
        status: 409, error: 'ITEM_ALREADY_EXISTS',
        message: `商品管理番号 ${mn} は楽天に既に存在します。既存ページの上書きはこの API では行いません`,
        requestId: req.requestId,
      });
    }
    if (result.block === 502) {
      return errorResponse(res, { status: 502, error: 'RMS_PRECHECK_FAILED', message: `既存確認に失敗 (HTTP ${result.detail})`, requestId: req.requestId });
    }
    console.log(`[rakuten-rms] item create mn=${mn} status=${result.status}`);
    res.status(result.status).json(result.data ?? { ok: result.status < 300 });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 公開/非公開の切り替え (product-hub の「公開」ボタン用。2026-07-27 仕様)。
// GET した item の hideItem だけを反転して PUT で書き戻す (RMS 2.0 に部分更新が無いため)。
// ⚠️ Render 側 (rakuten-listing.js setItemVisibility) が「アプリから登録した商品のみ」に
//    制限している。この route 単体では任意の商品を切り替えられるので、呼び出しは service token 前提。
router.post('/items/manage-numbers/:manageNumber/visibility', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    if (typeof req.body?.hide !== 'boolean') {
      return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'hide (boolean) が必要です', requestId: req.requestId });
    }
    const hide = req.body.hide;
    const result = await withManageNumberLock(mn, async () => {
      const existing = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });
      if (existing.status === 404) return { block: 404 };
      if (existing.status !== 200) return { block: 502, detail: existing.status };
      const item = existing.data?.item || existing.data;
      if (!item || typeof item !== 'object') return { block: 502, detail: 'empty_item' };
      if (item.hideItem === hide) return { noop: true };
      // GET の形をそのまま PUT に戻す (smoke 実証済みの往復)。読み取り専用メタだけ落とす
      const body = { ...item, hideItem: hide };
      delete body.manageNumber;
      delete body.created;
      delete body.updated;
      // 変更系は自動リトライしない (PUT route と同じ理由)
      return rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}`, method: 'PUT', body, timeoutMs: 90_000, maxAttempts: 1 });
    });
    if (result.block === 404) {
      return errorResponse(res, { status: 404, error: 'ITEM_NOT_FOUND', message: `${mn} は楽天に存在しません`, requestId: req.requestId });
    }
    if (result.block) {
      return errorResponse(res, { status: 502, error: 'RMS_PRECHECK_FAILED', message: `商品の取得に失敗 (${result.detail})`, requestId: req.requestId });
    }
    if (result.noop) {
      console.log(`[rakuten-rms] visibility noop mn=${mn} hide=${hide}`);
      return res.status(200).json({ ok: true, hidden: hide, noop: true });
    }
    console.log(`[rakuten-rms] visibility mn=${mn} hide=${hide} status=${result.status}`);
    if (result.status === 200 || result.status === 201 || result.status === 204) {
      return res.status(200).json({ ok: true, hidden: hide });
    }
    res.status(result.status).json(result.data ?? { ok: false });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 削除 (非公開の商品のみ)。テスト登録の掃除用
router.delete('/items/manage-numbers/:manageNumber', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    // High-3: 「非公開確認→DELETE」の間に公開される競合は API 仕様上防げない。
    // そこで削除はテスト用の管理番号 (zz- で始まる) に**構造的に限定**する。
    // 本番商品は zz- で始まらないので、この route から消えることはない
    if (!mn.startsWith('zz-')) {
      return errorResponse(res, {
        status: 403, error: 'DELETE_NOT_ALLOWED',
        message: '削除できるのはテスト用商品 (zz- で始まる管理番号) だけです',
        requestId: req.requestId,
      });
    }
    const result = await withManageNumberLock(mn, async () => {
      const existing = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });
      if (existing.status === 404) return { block: 404 };
      const item = existing.data?.item || existing.data;
      if (item?.hideItem !== true) return { block: 403 };
      return rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}`, method: 'DELETE', maxAttempts: 1 });
    });
    if (result.block === 404) {
      return errorResponse(res, { status: 404, error: 'ITEM_NOT_FOUND', message: `${mn} は存在しません`, requestId: req.requestId });
    }
    if (result.block === 403) {
      return errorResponse(res, {
        status: 403, error: 'ITEM_IS_PUBLIC',
        message: `${mn} は公開中のため削除しません (非公開の商品だけ削除できます)`,
        requestId: req.requestId,
      });
    }
    console.log(`[rakuten-rms] item delete mn=${mn} status=${result.status}`);
    res.status(result.status === 204 ? 200 : result.status).json({ ok: result.status === 204, status: result.status });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// R-Cabinet (画像置き場)。XML API 1.0
//   folder-ensure: directoryName で検索し、無ければ作成 (冪等)
//   upload: base64 画像を multipart で insert → FileUrl から item 用 location を導出
// ==========================================

const CABINET_UPLOAD_MAX_BYTES = 2 * 1024 * 1024; // R-Cabinet の上限 2MB (デコード後)

function xmlEscape(s) {
  return String(s).replace(/[<>&'"]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', "'": '&apos;', '"': '&quot;' }[c]));
}

// 1.0 XML response から result ブロックを安全に取り出す (xml2js explicitArray:false 前提)
async function parseCabinetXml(text) {
  const { parseStringPromise } = await import('xml2js');
  return parseStringPromise(String(text || ''), { explicitArray: false });
}

router.post('/cabinet/folder-ensure', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const directoryName = String(req.body?.directoryName || '').trim();
    // 同じ directoryName の folder-ensure は直列化 (同時要求の二重作成防止)
    return await withManageNumberLock(`cabinet-dir:${directoryName}`, async () => {
    const folderName = String(req.body?.folderName || directoryName).trim();
    // directoryName は URL パスになる。楽天仕様 (半角英数字) に限定
    if (!/^[a-z0-9][a-z0-9\-]{0,19}$/.test(directoryName)) {
      return errorResponse(res, { status: 400, error: 'INVALID_DIRECTORY', message: 'directoryName は半角英数小文字とハイフンで指定してください', requestId: req.requestId });
    }

    // 既存フォルダを探す (最大10ページ = 1000フォルダまで走査)。
    // 検索失敗や走査上限到達は「存在しない」とみなさず fail-closed で返す
    // (Codex medium: 見落として同名フォルダを二重作成しない)
    let searched = 0;
    let totalFolders = null;
    for (let offset = 1; offset <= 10; offset++) {
      const r = await rakutenRequest({ path: `/es/1.0/cabinet/folders/search?offset=${offset}&limit=100` });
      if (r.status !== 200) {
        return errorResponse(res, { status: 502, error: 'CABINET_SEARCH_FAILED', message: `folder search 失敗 (HTTP ${r.status})`, requestId: req.requestId });
      }
      const parsed = await parseCabinetXml(r.data);
      const resBlock = parsed?.result?.cabinetFoldersSearchResult;
      let folders = resBlock?.folders?.folder || [];
      if (!Array.isArray(folders)) folders = [folders];
      const hit = folders.find((f) => String(f?.DirectoryName || '').trim() === directoryName);
      if (hit) {
        return okResponse(res, { folderId: Number(hit.FolderId), directoryName, existed: true });
      }
      searched += folders.length;
      totalFolders = Number(resBlock?.folderAllCount || 0);
      if (searched >= totalFolders || folders.length === 0) break;
    }
    if (totalFolders !== null && searched < totalFolders) {
      return errorResponse(res, {
        status: 502, error: 'CABINET_TOO_MANY_FOLDERS',
        message: `フォルダ数が多く全走査できません (${searched}/${totalFolders})。手動でフォルダIDを確認してください`,
        requestId: req.requestId,
      });
    }

    // 無ければ作成
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<request><folderInsertRequest><folder>
<folderName>${xmlEscape(folderName)}</folderName>
<directoryName>${xmlEscape(directoryName)}</directoryName>
<upperFolderId>0</upperFolderId>
</folder></folderInsertRequest></request>`;
    const ins = await rakutenRequest({
      path: '/es/1.0/cabinet/folder/insert', method: 'POST',
      rawBody: xml, headers: { 'Content-Type': 'text/xml; charset=UTF-8' },
      maxAttempts: 1, // 変更系は自動リトライしない (二重作成防止)
    });
    const parsed = await parseCabinetXml(ins.data);
    const result = parsed?.result?.cabinetFolderInsertResult;
    const code = String(result?.resultCode ?? '');
    if (ins.status !== 200 || code !== '0') {
      return errorResponse(res, {
        status: 502, error: 'CABINET_FOLDER_INSERT_FAILED',
        message: `folder insert 失敗 (HTTP ${ins.status} / resultCode ${code || 'なし'})`,
        requestId: req.requestId,
      });
    }
    console.log(`[rakuten-rms] cabinet folder created dir=${directoryName} id=${result.FolderId}`);
    okResponse(res, { folderId: Number(result.FolderId), directoryName, existed: false });
    });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

router.post('/cabinet/upload', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const folderId = Number(req.body?.folderId);
    const filePath = String(req.body?.filePath || '').trim();
    const fileName = String(req.body?.fileName || filePath).trim();
    const fileBase64 = String(req.body?.fileBase64 || '');
    if (!Number.isInteger(folderId) || folderId <= 0) {
      return errorResponse(res, { status: 400, error: 'INVALID_FOLDER_ID', message: 'folderId が必要です', requestId: req.requestId });
    }
    // filePath は画像URLの一部になる。この route は JPEG 専用 (Content-Type と整合させる)
    if (!/^[a-z0-9][a-z0-9\-]{0,30}\.jpg$/.test(filePath)) {
      return errorResponse(res, { status: 400, error: 'INVALID_FILE_PATH', message: 'filePath は英数小文字とハイフン + .jpg で指定してください', requestId: req.requestId });
    }
    let buf;
    try {
      buf = Buffer.from(fileBase64, 'base64');
    } catch (_) {
      buf = null;
    }
    if (!buf || buf.length === 0) {
      return errorResponse(res, { status: 400, error: 'INVALID_FILE', message: 'fileBase64 が空か不正です', requestId: req.requestId });
    }
    // JPEG マジックバイト (FF D8) を検証 (Codex medium: 不正 base64 は黙って壊れた画像になる)
    if (buf[0] !== 0xFF || buf[1] !== 0xD8) {
      return errorResponse(res, { status: 400, error: 'NOT_JPEG', message: 'JPEG ではありません (先頭バイト不一致)', requestId: req.requestId });
    }
    if (buf.length > CABINET_UPLOAD_MAX_BYTES) {
      return errorResponse(res, { status: 400, error: 'FILE_TOO_LARGE', message: `画像は2MB以下にしてください (${Math.round(buf.length / 1024)}KB)`, requestId: req.requestId });
    }

    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<request><fileInsertRequest><file>
<fileName>${xmlEscape(fileName)}</fileName>
<folderId>${folderId}</folderId>
<filePath>${xmlEscape(filePath)}</filePath>
<overWrite>true</overWrite>
</file></fileInsertRequest></request>`;
    const fd = new FormData();
    fd.append('xml', xml);
    fd.append('file', new Blob([buf], { type: 'image/jpeg' }), filePath);
    const ins = await rakutenRequest({
      path: '/es/1.0/cabinet/file/insert', method: 'POST', formData: fd, timeoutMs: 90_000,
      maxAttempts: 1, // overWrite=true で再実行は安全だが、自動再送はしない (呼び出し側の再操作に任せる)
    });
    const parsed = await parseCabinetXml(ins.data);
    const result = parsed?.result?.cabinetFileInsertResult;
    const code = String(result?.resultCode ?? '');
    if (ins.status !== 200 || code !== '0') {
      return errorResponse(res, {
        status: 502, error: 'CABINET_UPLOAD_FAILED',
        message: `file insert 失敗 (HTTP ${ins.status} / resultCode ${code || 'なし'})`,
        requestId: req.requestId,
      });
    }
    const fileId = Number(result.FileId);

    // FileUrl を引いて item 用の location (/dir/file.jpg) を導出する
    let fileUrl = null;
    let location = null;
    const search = await rakutenRequest({ path: `/es/1.0/cabinet/files/search?fileId=${fileId}` });
    if (search.status === 200) {
      const sp = await parseCabinetXml(search.data);
      let files = sp?.result?.cabinetFilesSearchResult?.files?.file || [];
      if (!Array.isArray(files)) files = [files];
      fileUrl = files[0]?.FileUrl || null;
      if (fileUrl) {
        const m = String(fileUrl).match(/\/cabinet(\/.+)$/);
        location = m ? m[1] : null;
      }
    }
    console.log(`[rakuten-rms] cabinet upload fileId=${fileId} path=${filePath} location=${location}`);
    okResponse(res, { fileId, fileUrl, location });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

router.get('/status', (req, res) => {
  okResponse(res, {
    hasCredentials: !!(process.env.RAKUTEN_SERVICE_SECRET && process.env.RAKUTEN_LICENSE_KEY),
  });
});

export default router;
