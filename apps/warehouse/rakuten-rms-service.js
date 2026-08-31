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
import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';
import {
  planPriceUpdate, getPriceOpsDb, receiveOperation, completeOperation, getOperation, replayOf,
} from './rakuten-price-ops.js';
import express, { Router } from 'express';
import { rateLimitMiddleware } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';
import { rakutenRequest, describeRmsError, rmsErrorSuffix, RMS_AUTH_HINT } from './rakuten-client.js';

const router = Router();

// ─── アプリ起点で作成した商品の台帳 (visibility の対象制限) ───
// Codex R1 High-2 / R2 High: service token を持てば任意の商品管理番号を公開/非公開に
// できるのは危険。この service の PUT (新規作成) が作った管理番号だけを台帳に記録し、
// visibility は**台帳にある商品のみ**に構造的に制限する (zz- の無条件例外は R2 で撤廃 —
// zz- テスト商品も PUT で作れば台帳に載るので例外は不要)。
// 永続層は SQLite (WAL・原子的 upsert)。JSON ファイル書き戻しは複数プロセスで後勝ち消失が
// あり得るため不採用 (R2 Medium)。台帳導入以前にこの route で作られた本番商品は無い。
const RMS_DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
let registryDb = null;
function getRegistryDb() {
  if (registryDb) return registryDb;
  fs.mkdirSync(RMS_DATA_DIR, { recursive: true });
  registryDb = new Database(path.join(RMS_DATA_DIR, 'rms-app-items.db'));
  registryDb.pragma('journal_mode = WAL');
  registryDb.pragma('busy_timeout = 5000');
  registryDb.exec(`CREATE TABLE IF NOT EXISTS app_created_items (
    manage_number TEXT PRIMARY KEY,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  )`);
  return registryDb;
}
function recordCreatedItem(mn) {
  try {
    getRegistryDb().prepare('INSERT OR IGNORE INTO app_created_items (manage_number) VALUES (?)').run(mn);
  } catch (e) {
    // 記録に失敗しても RMS 側の作成は成功している。その商品の公開切替が拒否されるだけ (安全側)
    console.error('[rakuten-rms] app_created_items 台帳の保存に失敗:', e.message);
  }
}
function isAppCreatedItem(mn) {
  return !!getRegistryDb().prepare('SELECT 1 FROM app_created_items WHERE manage_number = ?').get(mn);
}

const DETAILS_BULK_MAX = 200;

// 問い合わせ添付の受信上限 (inquiry-hub 側の上限とは独立に、この中継自身も自衛する)
const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;

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
      const hint = (result.status === 401 || result.status === 403) ? RMS_AUTH_HINT : '';
      throw Object.assign(new Error(`HTTP ${result.status}${rmsErrorSuffix(result.data)}${hint}`), { rmsStatus: result.status });
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

// 楽天 RMS のレスポンス本文からエラーコード / メッセージを安全に抽出 (型固定)。
// 形の網羅 (errors[] / MessageModelList[] / {code,message}) は rakuten-client.js に集約した。
// **errors[] を拾えないと 401 GA0001 'Un-Authorised' が空欄になる** (2026-08-30 のライセンス失効で実害)
function extractRmsError(data) {
  const { code, message } = describeRmsError(data);
  return { errorCode: code, message };
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
        // 401 は errors[] 形式で理由が来る (GA0001 Un-Authorised = ライセンスキー失効)
        const hint = (result.status === 401 || result.status === 403) ? RMS_AUTH_HINT : '';
        return errorResponse(res, {
          status: result.status, error: 'RMS_API_ERROR',
          message: `HTTP ${result.status}${rmsErrorSuffix(result.data)}${hint}`, requestId: req.requestId,
        });
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

// 添付ファイル取得 passthrough (inquiry-hub 添付表示。2026-08-02)
// - read-only。RMS の GET /attachment?path=&label= をバイナリのまま素通しする
// - path は RMS が発行した取得キーをそのまま渡す (任意パス注入にはならない。path/label 以外は通さない)
// - 呼び出し元は inquiry-hub の添付配信ルートのみ (Render→CF Tunnel→サービストークン認証の内側)
router.get('/inquiry-attachment', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const attPath = String(req.query.path || '');
    const label = String(req.query.label || '');
    if (!attPath) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'path が必要です', requestId: req.requestId });
    }
    if (attPath.length > 500 || label.length > 300) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'path / label が長すぎます', requestId: req.requestId });
    }
    const params = new URLSearchParams({ path: attPath, label });
    let result;
    try {
      result = await rakutenRequest({
        path: `/es/1.0/inquirymng-api/attachment?${params.toString()}`,
        responseType: 'buffer',
        maxBytes: MAX_ATTACHMENT_BYTES,   // 呼び出し元 (Render) とは独立に自分でも上限を持つ
      });
    } catch (e) {
      if (e?.tooLarge) {
        return errorResponse(res, { status: 413, error: 'ATTACHMENT_TOO_LARGE', message: e.message, requestId: req.requestId });
      }
      throw e;
    }
    if (result.status !== 200 || !Buffer.isBuffer(result.data)) {
      const detail = Buffer.isBuffer(result.data) ? '(binary)' : String(JSON.stringify(result.data ?? '')).slice(0, 200);
      return errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: `添付取得に失敗 (HTTP ${result.status}) ${detail}`, requestId: req.requestId });
    }
    console.log(`[rakuten-rms] inquiry-attachment ${attPath.slice(0, 24)}… -> ${result.data.length} bytes`);
    res.setHeader('Content-Type', result.contentType || 'application/octet-stream');
    res.setHeader('Content-Length', String(result.data.length));
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.status(200).end(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 返信添付アップロード passthrough (2026-08-20 返信添付。中原さん指示: 楽天は添付対応)
// - R-Messe の POST /attachment (multipart, フィールド名 'file') へ転送し {result:{label,path}} を返す。
//   返信本体は /inquiry-reply が別途送る (このrouteだけでは顧客に何も届かない = リトライ安全)
// - inquiry-hub 側で中身バイト検証済みだが、形式allow-listとサイズ上限は自分でも持つ (多層防御)
const REPLY_ATTACHMENT_MAX_BYTES = 5 * 1024 * 1024;
const REPLY_ATTACHMENT_TYPES = /^(application\/pdf|image\/(png|jpeg|gif|webp|bmp))$/;
router.post('/inquiry-attachment-upload', rateLimitMiddleware('rakuten'),
  express.raw({ type: 'application/octet-stream', limit: REPLY_ATTACHMENT_MAX_BYTES + 1024 * 1024 }),
  async (req, res) => {
    try {
      const buf = req.body;
      if (!Buffer.isBuffer(buf) || buf.length === 0) {
        return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'ファイルが空です (Content-Type: application/octet-stream で送ってください)', requestId: req.requestId });
      }
      if (buf.length > REPLY_ATTACHMENT_MAX_BYTES) {
        return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: `ファイルが大きすぎます (上限${Math.round(REPLY_ATTACHMENT_MAX_BYTES / 1048576)}MB)`, requestId: req.requestId });
      }
      const contentType = String(req.headers['x-file-content-type'] || '');
      if (!REPLY_ATTACHMENT_TYPES.test(contentType)) {
        return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: `この形式は添付できません (${contentType || '未指定'})`, requestId: req.requestId });
      }
      let fileName = 'attachment';
      try { fileName = decodeURIComponent(String(req.headers['x-file-name'] || '')).slice(0, 200) || 'attachment'; } catch { /* 不正エンコードは既定名 */ }
      const fd = new FormData();
      fd.append('file', new Blob([buf], { type: contentType }), fileName);
      // アップロードは顧客可視の副作用が無いため rakutenRequest の既定リトライのままでよい
      const result = await rakutenRequest({ path: '/es/1.0/inquirymng-api/attachment', method: 'POST', formData: fd });
      // 非2xx は理由も残す (401 GA0001 = ライセンスキー失効。status だけだと原因が追えない)
      console.log(`[rakuten-rms] inquiry-attachment-upload ${fileName} ${buf.length}bytes -> HTTP ${result.status}${result.status >= 300 ? rmsErrorSuffix(result.data) : ''}`);
      res.status(result.status).json(result.data);
    } catch (e) {
      errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
    }
  });

// 問い合わせ返信 passthrough (inquiry-hub Step 4。変更系はこの1本のみ・厳重ガード)
// - 呼び出し元は inquiry-hub の outbox worker のみ (Render→CF Tunnel→サービストークン認証の内側)
// - inquiryNumber は実測形式 (shopId-日付-連番英字) の厳格検証。任意パス・任意ペイロードは通さない
// - 既読化/完了化の passthrough は作らない (必要になった時点で個別設計)。
//   添付は上の /inquiry-attachment-upload が発行した {label, path} のみ受ける
router.post('/inquiry-reply', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const inquiryNumber = String(req.body?.inquiryNumber || '').trim();
    const message = String(req.body?.message || '');
    if (!/^\d{1,10}-\d{8}-\d{1,12}[a-z]?$/i.test(inquiryNumber)) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'inquiryNumber が実測形式ではありません', requestId: req.requestId });
    }
    // shopId は返信APIの必須パラメータ (2026-08-17 初実送信の実測: 欠落だと IE001 bad parameter)。
    // inquiryNumber の先頭セグメント = shopId。呼び出し元が未送信でもここで導出して補完する
    // (Render/miniPC のデプロイ順序に依存させない)。指定があれば導出値との一致を要求 (店舗混入防止)
    const derivedShopId = Number(inquiryNumber.split('-')[0]);
    const shopId = req.body?.shopId != null ? Number(req.body.shopId) : derivedShopId;
    if (!Number.isInteger(shopId) || shopId !== derivedShopId) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'shopId が inquiryNumber と一致しません', requestId: req.requestId });
    }
    if (!message.trim()) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'message が空です', requestId: req.requestId });
    }
    if (message.length > 10000) {
      return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'message が長すぎます (10000文字まで)', requestId: req.requestId });
    }
    // 返信添付 (2026-08-20): /inquiry-attachment-upload が発行した {label, path} のみ・3つまで。
    // 任意構造は通さない (label/path以外のキーは落とす)
    let attachments;
    if (req.body?.attachments != null) {
      const arr = req.body.attachments;
      const valid = Array.isArray(arr) && arr.length <= 3 && arr.every(a => a
        && typeof a.label === 'string' && a.label.length > 0 && a.label.length <= 300
        && typeof a.path === 'string' && a.path.length > 0 && a.path.length <= 500);
      if (!valid) {
        return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: 'attachments が不正です ([{label, path}] を3つまで)', requestId: req.requestId });
      }
      attachments = arr.map(a => ({ label: a.label, path: a.path }));
    }
    // ⚠️ maxAttempts:1 必須 — rakutenRequest の既定は 429/5xx を自動リトライ (計4回) するが、
    // 送信系でのリトライは二重送信になり得る (5xx=送信された可能性が残る)。結果不明の扱いは
    // inquiry-hub の outbox (unknown→人手解決) に委ねる
    const result = await rakutenRequest({
      path: '/es/1.0/inquirymng-api/inquiry/reply',
      method: 'POST',
      body: { inquiryNumber, shopId, message, ...(attachments?.length ? { attachments } : {}) },
      maxAttempts: 1,
    });
    // 非2xx は理由も残す (401 GA0001 = ライセンスキー失効。status だけだと原因が追えない)
    console.log(`[rakuten-rms] inquiry-reply ${inquiryNumber} -> HTTP ${result.status}${result.status >= 300 ? rmsErrorSuffix(result.data) : ''}`);
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
//   - 商品の新規作成の公開状態は payload の hideItem に従う (未指定なら倉庫指定=true に倒す fail-safe)。
//     2026-08-05 中原さん指示で公開登録に変更 (それ以前は hideItem=true を強制していた)
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

// SKU画像だけを variants[sku].images へ PATCH する限定ルート (2026-08-07 中原さん指示)。
// PATCH は per-SKU マージ (zz- テスト商品で実測: 他SKU・価格・selectorValues は無傷) のため、
// **台帳ガードは適用しない** — 対象は手動登録済みのバリエーションページが主で、
// サーバー側で body を「images 1枚のみ」の形に作り直すので他フィールドには構造的に触れない
router.patch('/items/manage-numbers/:manageNumber/sku-images', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    const src = req.body?.variants;
    const keys = src && typeof src === 'object' && !Array.isArray(src) ? Object.keys(src) : [];
    if (keys.length === 0 || keys.length > 40) {
      return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'variants は 1〜40 SKU で指定してください', requestId: req.requestId });
    }
    const variants = Object.create(null); // __proto__ 等の特殊キーで代入が化けないよう素のマップにする (Codex Low-7)
    for (const key of keys) {
      const k = String(key);
      // SKU管理番号 (RMS側の実キー)。前後空白は呼び出し側の正規化ミスなので黙って直さず拒否する
      const hasCtl = Array.from(k).some((ch) => ch.charCodeAt(0) < 32 || ch.charCodeAt(0) === 127);
      if (!k || k !== k.trim() || k.length > 96 || hasCtl) {
        return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: `SKU番号の形式が不正です: ${String(key).slice(0, 40)}`, requestId: req.requestId });
      }
      const imgs = src[key]?.images;
      const loc = Array.isArray(imgs) && imgs.length === 1 ? String(imgs[0]?.location || '') : '';
      // R-Cabinet の location 形式に限定 (Codex Medium-5): /dir/…/file.<画像拡張子>
      if (!/^\/(?:[A-Za-z0-9_\-]+\/)*[A-Za-z0-9_\-]+\.(?:jpg|jpeg|gif|png)$/.test(loc) || loc.length > 200) {
        return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: `images は {type:'CABINET', location:'/dir/file.jpg'} を1枚だけ指定してください (SKU: ${k})`, requestId: req.requestId });
      }
      variants[k] = { images: [{ type: 'CABINET', location: loc }] };
    }
    const result = await withManageNumberLock(mn, async () => {
      // 指定 SKU がその商品に実在するかを同一ロック内で検証してから送る (Codex Medium-4:
      // このルートは書き込み境界なので、呼び出し側の GET 照合には依存しない)
      const cur = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });
      if (cur.status !== 200) {
        return { status: cur.status === 404 ? 404 : 502, data: { message: `商品の取得に失敗 (HTTP ${cur.status})` } };
      }
      const existing = new Set(Object.keys(cur.data?.variants || {}));
      const unknown = Object.keys(variants).filter((k) => !existing.has(k));
      if (unknown.length > 0) {
        return { status: 400, data: { message: `この商品に存在しないSKUです: ${unknown.slice(0, 10).join(', ')}` } };
      }
      return rakutenRequest({
        path: `/es/2.0/items/manage-numbers/${mn}`, method: 'PATCH',
        body: { variants },
        maxAttempts: 1, // 変更系は自動リトライしない
      });
    });
    if (result.status < 200 || result.status >= 300) {
      const err = extractRmsError(result.data);
      return errorResponse(res, {
        // 事前検証由来 (存在しないSKU/商品なし) は 400/404 のまま返す
        status: result.status === 400 || result.status === 404 ? result.status : 502,
        error: 'RMS_API_ERROR',
        message: `SKU画像の反映に失敗 (HTTP ${result.status}${err.message ? ` / ${err.message}` : ''})`,
        requestId: req.requestId,
      });
    }
    console.log(`[rakuten-rms] sku-images patched mn=${mn} skus=${Object.keys(variants).length}`);
    okResponse(res, { patched: Object.keys(variants).length });
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

      // 公開状態は payload の hideItem に従う (2026-08-05〜 公開登録)。
      // boolean false のときだけ公開 — 未指定・不正値は倉庫指定 (非公開) に倒す fail-safe
      payload.hideItem = payload.hideItem === false ? false : true;

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
    if (result.status === 200 || result.status === 201) recordCreatedItem(mn); // visibility の対象台帳
    res.status(result.status).json(result.data ?? { ok: result.status < 300 });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 公開/非公開の切り替え (product-hub の「公開」ボタン用。2026-07-27 仕様)。
// PATCH (部分更新・マージ形式) で hideItem だけを送る — 全文 GET→PUT 往復だと
// GET→PUT 間の RMS 画面・他プロセスの変更を古い値で巻き戻すため (Codex R1 High-1)。
// 対象は「この service の PUT で作った商品 (台帳) + zz- テスト商品」に構造的に限定 (High-2)。
router.post('/items/manage-numbers/:manageNumber/visibility', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    if (typeof req.body?.hide !== 'boolean') {
      return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'hide (boolean) が必要です', requestId: req.requestId });
    }
    // 台帳照合のみ (zz- 例外なし — R2 High)。zz- テスト商品も PUT 経由なら台帳に載る
    if (!isAppCreatedItem(mn)) {
      return errorResponse(res, {
        status: 403, error: 'VISIBILITY_NOT_ALLOWED',
        message: `${mn} はこの service から登録した商品ではないため公開切替できません (既存商品は RMS 画面で操作)`,
        requestId: req.requestId,
      });
    }
    const hide = req.body.hide;
    const result = await withManageNumberLock(mn, async () => {
      const existing = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });
      if (existing.status === 404) return { block: 404 };
      if (existing.status !== 200) return { block: 502, detail: existing.status };
      const item = existing.data?.item || existing.data;
      if (!item || typeof item !== 'object') return { block: 502, detail: 'empty_item' };
      if (item.hideItem === hide) return { noop: true };
      // 変更系は自動リトライしない (PUT route と同じ理由)
      return rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}`, method: 'PATCH', body: { hideItem: hide }, timeoutMs: 90_000, maxAttempts: 1 });
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

// ==========================================
// 価格更新 (価格一括改定ツール M2)
//   PATCH /items/manage-numbers/:mn/prices
//   body: { operation_id, run_id?, expected: {sku:円}, prices: {sku:円} }
//
// 🚨ここが唯一「楽天の売価を書き換える」経路。守っていること:
//   ・RAKUTEN_RMS_WRITE_ENABLED が無ければ 503 (fail-closed)
//   ・operation_id で冪等。同じ ID の再送は実行せず前回結果を返す。
//     結果が残っていない受領済み ID は unknown を返して**実行しない** (二重更新を作らない)
//   ・商品ロックの中で GET → SKU実在確認 → expected と整数円で完全一致 → PATCH
//     (M0実測: 存在しない SKU を送ると新規SKU作成になる / GET の価格は文字列)
//   ・自動リトライしない (maxAttempts:1)。成功は 204 のみ
//   ・受領台帳 (rakuten-price-ops.db) に受領と結果を残す (要件 F6 の miniPC 側ログ)
// ==========================================
router.patch('/items/manage-numbers/:manageNumber/prices', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  const mn = String(req.params.manageNumber || '').trim().toLowerCase();
  const operationId = String(req.body?.operation_id || '').trim();
  const runId = req.body?.run_id ? String(req.body.run_id).trim() : null;
  const expected = req.body?.expected;
  const prices = req.body?.prices;

  if (!MANAGE_NUMBER_RE.test(mn)) {
    return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
  }
  if (!/^[A-Za-z0-9._-]{8,80}$/.test(operationId)) {
    return errorResponse(res, { status: 400, error: 'INVALID_OPERATION_ID', message: 'operation_id (8〜80文字の英数.-_) が必要です', requestId: req.requestId });
  }
  if (!prices || typeof prices !== 'object' || Array.isArray(prices)
      || !expected || typeof expected !== 'object' || Array.isArray(expected)) {
    return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'expected と prices (どちらもオブジェクト) が必要です', requestId: req.requestId });
  }

  let opsDb;
  try {
    opsDb = getPriceOpsDb();
  } catch (e) {
    // 台帳に書けない = 冪等を保証できない。書き込みはしない (fail-closed)
    return errorResponse(res, { status: 503, error: 'OPS_LEDGER_UNAVAILABLE', message: `受領台帳を開けません: ${e.message}`, requestId: req.requestId });
  }

  const received = receiveOperation(opsDb, { operationId, runId, manageNumber: mn, request: { expected, prices } });
  if (received.reused) {
    // 同じ operation_id で別の依頼が来た。前回結果を返すと「更新していないのに成功」になる
    console.error(`[rakuten-rms] price operation_id 使い回し op=${operationId} mn=${mn}`);
    return errorResponse(res, {
      status: 409, error: 'OPERATION_ID_REUSED',
      message: 'この operation_id は別の依頼で使われています。新しい ID で送り直してください',
      requestId: req.requestId,
    });
  }
  if (!received.fresh) {
    const r = replayOf(received.row);
    // ★状態ごとに初回と同じ扱いにする。conflict を 200/ok:true で返すと、
    //   呼び出し側が「適用済み」と誤って確定する
    const status = { applied: 200, noop: 200, conflict: 409, failed: 502, unknown: 409 }[r.state] ?? 409;
    console.log(`[rakuten-rms] price replay op=${operationId} state=${r.state} status=${status}`);
    return res.status(status).json({
      ok: r.state === 'applied' || r.state === 'noop',
      replay: true, state: r.state, result: r.result,
    });
  }

  try {
    const outcome = await withManageNumberLock(mn, async () => {
      const existing = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });
      if (existing.status === 404) return { block: 404 };
      if (existing.status !== 200) return { block: 502, detail: existing.status };
      const item = existing.data?.item || existing.data;
      const plan = planPriceUpdate(item, { expected, prices });
      if (!plan.ok) return { plan };
      if (plan.noop) return { plan };
      // 変更系は自動リトライしない (途中で通っていた場合に二重更新になるため)
      const result = await rakutenRequest({
        path: `/es/2.0/items/manage-numbers/${mn}`, method: 'PATCH',
        body: plan.patch, timeoutMs: 90_000, maxAttempts: 1,
      });
      return { plan, result };
    });

    if (outcome.block === 404) {
      completeOperation(opsDb, operationId, 'failed', { error: 'ITEM_NOT_FOUND' });
      return errorResponse(res, { status: 404, error: 'ITEM_NOT_FOUND', message: `${mn} は楽天に存在しません`, requestId: req.requestId });
    }
    if (outcome.block) {
      completeOperation(opsDb, operationId, 'failed', { error: 'RMS_PRECHECK_FAILED', detail: outcome.detail });
      return errorResponse(res, { status: 502, error: 'RMS_PRECHECK_FAILED', message: `商品の取得に失敗 (${outcome.detail})`, requestId: req.requestId });
    }
    const { plan, result } = outcome;
    if (!plan.ok) {
      const state = plan.code === 'CONFLICT' ? 'conflict' : 'failed';
      completeOperation(opsDb, operationId, state, { code: plan.code, message: plan.message, detail: plan.detail });
      console.log(`[rakuten-rms] price ${state} mn=${mn} op=${operationId} code=${plan.code}`);
      return errorResponse(res, {
        status: plan.code === 'CONFLICT' ? 409 : 400,
        error: plan.code, message: plan.message, requestId: req.requestId, detail: plan.detail,
      });
    }
    if (plan.noop) {
      completeOperation(opsDb, operationId, 'noop', { reason: plan.reason });
      console.log(`[rakuten-rms] price noop mn=${mn} op=${operationId}`);
      return res.status(200).json({ ok: true, state: 'noop', reason: plan.reason });
    }
    // 🚨M0実測: 成功は 204。200/201 を成功にしない
    if (result.status === 204) {
      completeOperation(opsDb, operationId, 'applied', { applied: plan.applied });
      console.log(`[rakuten-rms] price applied mn=${mn} op=${operationId} skus=${Object.keys(plan.applied).join(',')}`);
      return res.status(200).json({ ok: true, state: 'applied', applied: plan.applied });
    }
    completeOperation(opsDb, operationId, 'failed', { status: result.status, body: result.data ?? null });
    console.error(`[rakuten-rms] price FAILED mn=${mn} op=${operationId} status=${result.status}`);
    return errorResponse(res, {
      status: 502, error: 'RMS_PRICE_UPDATE_FAILED',
      message: `楽天が 204 以外を返しました (${result.status})`, requestId: req.requestId,
    });
  } catch (e) {
    // 送信の途中で落ちた可能性がある → 結果は書かない (unknown のまま残し、再送は実行させない)
    console.error(`[rakuten-rms] price ERROR mn=${mn} op=${operationId}: ${e.message}`);
    return errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 受領照会 (Render 側が unknown を解決するために使う)。read-only
router.get('/price-ops/:operationId', (req, res) => {
  try {
    const row = getOperation(getPriceOpsDb(), String(req.params.operationId || '').trim());
    if (!row) return res.status(404).json({ ok: false, error: 'NOT_FOUND' });
    res.json({
      ok: true,
      operation_id: row.operation_id, run_id: row.run_id, manage_number: row.manage_number,
      received_at: row.received_at, state: row.result_state, completed_at: row.completed_at,
      request: JSON.parse(row.request_json || 'null'), result: JSON.parse(row.result_json || 'null'),
    });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'OPS_LOOKUP_FAILED', message: e.message, requestId: req.requestId });
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
    // 一覧は folders/get (2026-08-05 実測: folders/search は 405 NotApiFunction で存在しない。
    // 応答は cabinetFoldersGetResult、識別は DirectoryName ではなく FolderPath = "/<directoryName>")
    // 取得失敗や走査上限到達は「存在しない」とみなさず fail-closed で返す
    // (Codex medium: 見落として同名フォルダを二重作成しない)
    let searched = 0;
    let totalFolders = null;
    for (let offset = 1; offset <= 10; offset++) {
      const r = await rakutenRequest({ path: `/es/1.0/cabinet/folders/get?offset=${offset}&limit=100` });
      if (r.status !== 200) {
        return errorResponse(res, { status: 502, error: 'CABINET_SEARCH_FAILED', message: `folders/get 失敗 (HTTP ${r.status})`, requestId: req.requestId });
      }
      const parsed = await parseCabinetXml(r.data);
      const resBlock = parsed?.result?.cabinetFoldersGetResult;
      let folders = resBlock?.folders?.folder || [];
      if (!Array.isArray(folders)) folders = [folders];
      const hit = folders.find((f) => String(f?.FolderPath || '').trim() === `/${directoryName}`);
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

    // 無ければ作成。トップ直下は upperFolderId を送らない
    // (2026-08-05 実測: <upperFolderId>0</upperFolderId> は 3001 Input Parameter Error)
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<request><folderInsertRequest><folder>
<folderName>${xmlEscape(folderName)}</folderName>
<directoryName>${xmlEscape(directoryName)}</directoryName>
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

// ==========================================
// ジャンル属性辞書 (product-hub P3: 必須属性の自動フォーム / カタログID自動付与の判定)
//   GET /es/2.0/navigation/genres/{genreId}/attributes (2026-07-28 プローブで実証)
//   応答: genre.nameJa / nameJaPath / attributes[] (rmsMandatoryFlg / rmsInputMethod 等)
//   ⚠️ SELECTIVE 属性の選択肢一覧はこの API では取れない (単一属性 endpoint でも同じ)
//   読み取り専用なので write gate は不要。辞書はめったに変わらないので 24h キャッシュ
// ==========================================

const GENRE_ATTR_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const GENRE_ATTR_NEGATIVE_TTL_MS = 60 * 60 * 1000; // 404 (存在しない/非末端ジャンル) は 1h
const GENRE_ATTR_CACHE_MAX = 500; // 店で使うジャンル数は高々数十。暴走上限
const genreAttrCache = new Map(); // genreId -> { fetchedAt, status, data }

router.get('/genres/:genreId/attributes', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const genreId = String(req.params.genreId || '').trim();
    if (!/^\d{1,12}$/.test(genreId)) {
      return errorResponse(res, { status: 400, error: 'INVALID_GENRE_ID', message: 'ジャンルIDは数字で指定してください', requestId: req.requestId });
    }
    const force = req.query.refresh === '1';
    const cached = genreAttrCache.get(genreId);
    const ttl = cached?.status === 200 ? GENRE_ATTR_CACHE_TTL_MS : GENRE_ATTR_NEGATIVE_TTL_MS;
    if (!force && cached && (Date.now() - cached.fetchedAt) < ttl) {
      return res.status(cached.status).json(cached.data);
    }
    const r = await rakutenRequest({ path: `/es/2.0/navigation/genres/${genreId}/attributes` });
    // 200/404 だけキャッシュ (5xx や 429 は次回また取りに行く)
    if (r.status === 200 || r.status === 404) {
      if (genreAttrCache.size >= GENRE_ATTR_CACHE_MAX) genreAttrCache.clear();
      genreAttrCache.set(genreId, { fetchedAt: Date.now(), status: r.status, data: r.data });
    }
    res.status(r.status).json(r.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 店舗内カテゴリ (お店の棚) の取得 — Category API 2.0
//   2026-08-02 プローブで実証したパス (楽天ジャンルとは全くの別物):
//     GET /es/2.0/categories/shop-category-set-lists                        → { categorySetKeyList: ["0"] }
//     GET /es/2.0/categories/shop-category-trees/category-set-ids/{setId}   → { rootNode: { children: [...] } }
//   ⚠️ 誤りやすいパス: /es/2.0/categories/category-sets や .../shop-categories は 404/405。
//      shop-categories は POST (カテゴリ作成) 専用で、一覧取得には使えない。
//   読み取り専用なので write gate 不要。棚の構成はめったに変わらないので 24h キャッシュ
// ==========================================

const SHOP_CAT_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
let shopCatCache = null; // { fetchedAt, data }

router.get('/shop-categories/tree', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const force = req.query.refresh === '1';
    if (!force && shopCatCache && (Date.now() - shopCatCache.fetchedAt) < SHOP_CAT_CACHE_TTL_MS) {
      return okResponse(res, { ...shopCatCache.data, cached: true });
    }
    const listRes = await rakutenRequest({ path: '/es/2.0/categories/shop-category-set-lists' });
    if (listRes.status !== 200) {
      return errorResponse(res, {
        status: 502, error: 'RMS_API_ERROR',
        message: `カテゴリセット一覧の取得に失敗しました (status ${listRes.status})`,
        requestId: req.requestId,
      });
    }
    // メガプランは複数セットを持てる。通常店は "0" の 1 つだけ
    const setIds = Array.isArray(listRes.data?.categorySetKeyList) ? listRes.data.categorySetKeyList : [];
    if (setIds.length === 0) {
      return errorResponse(res, {
        status: 502, error: 'RMS_API_ERROR',
        message: 'カテゴリセットが1つも返りませんでした', requestId: req.requestId,
      });
    }
    const trees = [];
    for (const setId of setIds) {
      if (!/^\d{1,10}$/.test(String(setId))) {
        // 想定外の ID 形式を黙って捨てると「一部が欠けたまま全置き換え」になるので中断する
        return errorResponse(res, {
          status: 502, error: 'RMS_API_ERROR',
          message: `カテゴリセットIDの形式が想定外です (${String(setId).slice(0, 20)})`,
          requestId: req.requestId,
        });
      }
      const r = await rakutenRequest({
        path: `/es/2.0/categories/shop-category-trees/category-set-ids/${encodeURIComponent(setId)}?categoryfields=TITLE`,
      });
      if (r.status !== 200) {
        return errorResponse(res, {
          status: 502, error: 'RMS_API_ERROR',
          message: `カテゴリツリー (セット ${setId}) の取得に失敗しました (status ${r.status})`,
          requestId: req.requestId,
        });
      }
      // 200 でも中身が壊れていたら中断 (Codex R1: 部分データで全置き換えするとマスタが欠ける)
      const rootNode = r.data?.rootNode;
      if (!rootNode || !Array.isArray(rootNode.children)) {
        return errorResponse(res, {
          status: 502, error: 'RMS_API_ERROR',
          message: `カテゴリツリー (セット ${setId}) の応答形が想定外です (rootNode.children が無い)`,
          requestId: req.requestId,
        });
      }
      trees.push({ categorySetId: String(setId), rootNode });
    }
    const data = { categorySetIds: setIds.map(String), trees };
    shopCatCache = { fetchedAt: Date.now(), data };
    okResponse(res, { ...data, cached: false });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 商品 ⇔ 店舗内カテゴリ の割り当て (item-mappings、2026-08-02)
//   GET  /es/2.0/categories/item-mappings/manage-numbers/{mn} → {"categoryIds":["1270"], created, updated}
//   PUT  同 body {categoryIds:[...], mainPluralCategoryId?} → 登録/変更 (⚠️POST ではなく PUT)
//   複数カテゴリのときは mainPluralCategoryId (メインに据える棚) が必須で、categoryIds に含める必要がある。
//   ⚠️商品 API (items) には店舗内カテゴリのフィールドが無いので、割り当てはこの API が唯一の経路
// ==========================================

router.get('/item-mappings/manage-numbers/:manageNumber', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    const r = await rakutenRequest({ path: `/es/2.0/categories/item-mappings/manage-numbers/${mn}` });
    res.status(r.status).json(r.data ?? { ok: r.status < 300 });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

router.put('/item-mappings/manage-numbers/:manageNumber', requireWrite, rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mn = String(req.params.manageNumber || '').trim().toLowerCase();
    if (!MANAGE_NUMBER_RE.test(mn)) {
      return errorResponse(res, { status: 400, error: 'INVALID_MANAGE_NUMBER', message: '商品管理番号の形式が不正です', requestId: req.requestId });
    }
    // 入力検証を先に行う (台帳ガードが先だと、入力ミスまで 403 に見えて原因が分からない)
    const ids = Array.isArray(req.body?.categoryIds) ? req.body.categoryIds.map(String) : null;
    if (!ids || ids.length === 0) {
      return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'categoryIds (1件以上) が必要です', requestId: req.requestId });
    }
    if (ids.some((id) => !/^\d{1,12}$/.test(id))) {
      return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'categoryIds は数字で指定してください', requestId: req.requestId });
    }
    if (new Set(ids).size !== ids.length) {
      return errorResponse(res, { status: 400, error: 'INVALID_PAYLOAD', message: 'categoryIds が重複しています', requestId: req.requestId });
    }
    const body = { categoryIds: ids };
    // mainPluralCategoryId は「PLURAL 形式 (1ページ複数商品形式) カテゴリ」のメインページ指定専用。
    // 通常カテゴリ (LIST/GALLERY) の ID を渡すと IE0128 で拒否される (2026-08-05 実測) ため
    // 任意入力とし、指定時のみ categoryIds に含まれることを検証して透過する
    if (req.body?.mainPluralCategoryId != null && String(req.body.mainPluralCategoryId).trim() !== '') {
      const main = String(req.body.mainPluralCategoryId).trim();
      if (!ids.includes(main)) {
        return errorResponse(res, {
          status: 400, error: 'INVALID_PAYLOAD',
          message: 'mainPluralCategoryId は categoryIds に含まれる値で指定してください',
          requestId: req.requestId,
        });
      }
      body.mainPluralCategoryId = main;
    }
    // 既存商品の棚を勝手に付け替えないよう、対象はこの service で登録した商品に限る
    // (items の PUT / visibility と同じ台帳ガード)
    if (!isAppCreatedItem(mn)) {
      return errorResponse(res, {
        status: 403, error: 'MAPPING_NOT_ALLOWED',
        message: `${mn} はこの service から登録した商品ではないため店舗内カテゴリを変更できません (既存商品は RMS 画面で操作)`,
        requestId: req.requestId,
      });
    }
    // 変更系は自動リトライしない (items PUT と同じ理由。結果不明なら呼び出し側が GET で照合)
    const result = await withManageNumberLock(mn, async () => rakutenRequest({
      path: `/es/2.0/categories/item-mappings/manage-numbers/${mn}`,
      method: 'PUT', body, timeoutMs: 60_000, maxAttempts: 1,
    }));
    console.log(`[rakuten-rms] item-mapping mn=${mn} ids=${ids.join(',')} status=${result.status}`);
    res.status(result.status).json(result.data ?? { ok: result.status < 300 });
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
