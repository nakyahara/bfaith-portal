/**
 * remote-sources.js — Render から miniPC の素材 (商品マスタ・楽天RMS) を読むクライアント。
 *
 * なぜ要るか (2026-08-09):
 *   拡張の接続先を miniPC (wh.bfaith-wh.uk) にしたら Cloudflare Access に弾かれた
 *   (SWのfetchにはCFのログインクッキーが乗らない・cloudflareaccess.com へのリダイレクトは
 *    host_permissions 外でCORSブロック)。Codexと協議して (A)=拡張はRenderへ向け、
 *   Render が不足する素材を miniPC の /service-api/ から引く形にした。
 *
 * 設計 (Codexの (A) 必須対策に沿う):
 *   - 認証は既存の service-api パターン (CF Access サービストークン + SERVICE_TOKEN Bearer)。
 *     fba-profitability と同じ env (CF_ACCESS_CLIENT_ID/SECRET, WAREHOUSE_SERVICE_TOKEN)
 *   - 読み取り専用の GET だけ。任意URLを渡せる汎用プロキシにはしない (固定の2操作のみ)
 *   - タイムアウトを必ず切る (無制限に待たない)
 *   - miniPC のエラーやHTMLをそのまま上へ流さない。固定の形に整えて返す
 *   - x-request-id を付けて、Render と miniPC のログを突き合わせられるようにする
 */

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
const PRODUCTS_TIMEOUT_MS = 30_000;
const RMS_TIMEOUT_MS = 45_000; // miniPC側の楽天レート制御 (1.1s/req直列) の待ちを見込む

export function remoteConfigured() {
  return !!(process.env.CF_ACCESS_CLIENT_ID
    && process.env.CF_ACCESS_CLIENT_SECRET
    && process.env.WAREHOUSE_SERVICE_TOKEN);
}

function headers(requestId) {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    Authorization: `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'x-request-id': requestId,
  };
}

function newRequestId() {
  return `ss-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

async function callServiceApi(path, timeoutMs) {
  if (!remoteConfigured()) {
    throw new Error('miniPCへの接続情報 (CF_ACCESS_CLIENT_ID / CF_ACCESS_CLIENT_SECRET / WAREHOUSE_SERVICE_TOKEN) が未設定です');
  }
  const requestId = newRequestId();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${WAREHOUSE_URL}${path}`, {
      headers: headers(requestId),
      signal: controller.signal,
    });
    const ct = res.headers.get('content-type') || '';
    if (!ct.includes('json')) {
      // Cloudflare のエラーページ等のHTMLを上へ流さない
      throw new Error(`miniPCの応答がJSONではありません (HTTP ${res.status}, req=${requestId})`);
    }
    const json = await res.json();
    if (!res.ok) {
      const code = json?.error || json?.message || `HTTP ${res.status}`;
      throw new Error(`miniPCがエラーを返しました (${String(code).slice(0, 120)}, req=${requestId})`);
    }
    return json;
  } catch (e) {
    if (e?.name === 'AbortError') throw new Error(`miniPCへの接続がタイムアウトしました (req=${requestId})`);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * 商品マスタの応答を検証して整える (テスト可能な純関数)。
 * 形が想定と違えば投げる — 欠けた素材で展開して誤った結果を出すより止まる方がよい。
 */
export function parseProductsResponse(json) {
  if (!json || json.ok !== true || !Array.isArray(json.products)) {
    throw new Error('商品マスタの応答の形が想定と違います');
  }
  if (!json.products.length) {
    throw new Error('商品マスタが空で返ってきました');
  }
  const out = [];
  for (const p of json.products) {
    const code = String(p?.code || '').trim();
    if (!code) continue;
    out.push({
      code,
      name: String(p?.name || ''),
      available: Number.isFinite(Number(p?.available)) ? Number(p.available) : 0,
      status: String(p?.status || ''),
    });
  }
  if (!out.length) throw new Error('商品マスタに有効な行がありません');
  return out;
}

/** RMS商品応答から customizationOptions を取り出す (テスト可能な純関数) */
export function parseRmsItemResponse(json) {
  if (!json || typeof json !== 'object') {
    throw new Error('RMS応答の形が想定と違います');
  }
  // miniPC の rakuten-rms-service は RMS のレスポンスをそのまま返す。
  // customizationOptions の無い商品 (選べるセットでない) は空配列として扱う
  if (Array.isArray(json.customizationOptions)) return json.customizationOptions;
  if (json.manageNumber || json.itemNumber || json.title) return [];
  throw new Error('RMS応答に商品情報がありません');
}

/** miniPC から NE商品マスタ (商品コード/商品名/利用可能在庫/取扱区分) を取る */
export async function fetchRemoteProducts() {
  const json = await callServiceApi('/service-api/select-set/products', PRODUCTS_TIMEOUT_MS);
  return parseProductsResponse(json);
}

/** miniPC 経由で楽天RMSの選択肢定義 (customizationOptions) を取る */
export async function fetchRemoteRmsOptions(setCode) {
  const mn = String(setCode || '').trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9-]{0,49}$/.test(mn)) {
    throw new Error('セット商品コードの形式が不正です');
  }
  const json = await callServiceApi(
    `/service-api/rakuten-rms/items/manage-numbers/${encodeURIComponent(mn)}`,
    RMS_TIMEOUT_MS,
  );
  return parseRmsItemResponse(json);
}
