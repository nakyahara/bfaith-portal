/**
 * qoo10-apply.js — Qoo10 の商品価格クライアント (価格一括改定 M5)
 *
 * ★Qoo10 も「商品」に1つの価格しか持たない (SellPrice)。オプション (色など) は
 *   商品価格への差額で、別系の API (ItemsOptions) が持つ。このツールは触らない。
 *   → 送り先のキーは **Qoo10 商品番号 (ItemNo)**。Yahoo / au PAY と同じ扱い。
 *
 * 経路: Render → miniPC service-api (/service-api/qoo10/*) → api.qoo10.jp
 *   商品系 API の鍵 QOO10_CERT_KEY は miniPC にしか無い。変更系 API は miniPC 経由が家ルール
 *   ([[feedback_render_vs_minipc_api_placement]])。Render は鍵を持たない。
 *
 * 実際の判定 (楽観ロック・省略の罠よけ・送信後の照合) は miniPC 側
 * (apps/warehouse/qoo10-price-service.js) が持つ。ここは楽天のクライアントと同じ薄さ。
 */

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    const err = new Error(`${name} が未設定です。Qoo10 は送信も取得もしません (fail-closed)`);
    err.statusCode = 503;
    throw err;
  }
  return String(v).trim();
}

function warehouseUrl() {
  return requireEnv('WAREHOUSE_URL').replace(/\/+$/, '');
}

function serviceHeaders() {
  return {
    'CF-Access-Client-Id': requireEnv('CF_ACCESS_CLIENT_ID'),
    'CF-Access-Client-Secret': requireEnv('CF_ACCESS_CLIENT_SECRET'),
    Authorization: `Bearer ${requireEnv('WAREHOUSE_SERVICE_TOKEN')}`,
    'Content-Type': 'application/json',
  };
}

/**
 * Qoo10 の商品を1件読む (miniPC 経由)。
 * @param {string} itemNo Qoo10 商品番号 (9〜10桁)
 * @returns {Promise<{ok:boolean, item?:object, error?:string, message?:string}>}
 */
export async function fetchQoo10ItemDetail(itemNo, deps = {}) {
  const fetchImpl = deps.fetchImpl || fetch;
  const url = `${warehouseUrl()}/service-api/qoo10/items/${encodeURIComponent(String(itemNo).trim())}`;
  const res = await fetchImpl(url, { headers: serviceHeaders(), signal: AbortSignal.timeout(30_000) });
  let body = null;
  try { body = await res.json(); } catch { /* JSON でない応答 */ }
  if (!res.ok || !body?.ok) {
    return { ok: false, error: body?.error || `HTTP_${res.status}`, message: body?.message || `miniPC が ${res.status} を返しました` };
  }
  return { ok: true, item: body.item };
}

/**
 * Qoo10 用のクライアント。execute.js からは楽天・Yahoo・au PAY と同じ形で呼ばれる。
 * @param {object} deps テスト用の差し替え ({ getDetail / patch })
 */
export function makeQoo10Client(deps = {}) {
  const getDetail = deps.getDetail || fetchQoo10ItemDetail;
  const patch = deps.patch || defaultPatch;

  return {
    /**
     * 照合のための再取得。楽天と同じ形にそろえる。
     * ★Qoo10 は商品に1つの価格なので、variants のキーは商品番号そのもの。
     *   要求した番号でも引けるようにする (大小の概念は無いが、契約として揃えておく)
     */
    async fetchItemDetail(itemNo) {
      const got = await getDetail(itemNo);
      const d = got?.item;
      // ★整数円でなければ「取れない」扱い (miniPC 側の整形を信頼しきらない)
      if (!got?.ok || !d || !Number.isInteger(d.sellPrice)) {
        return { item: null, status: 'not_found' };
      }
      const price = { standardPrice: String(d.sellPrice) };
      const variants = { [d.itemNo]: price };
      if (String(itemNo).trim() !== String(d.itemNo)) variants[String(itemNo).trim()] = price;
      return { item: { manageNumber: d.itemNo, variants }, status: 'found' };
    },

    /**
     * 価格を送る。miniPC が楽天と同じ形 (state: applied/noop/conflict/failed) で返すので、
     * そのまま execute.js の classify() に渡せる。
     * ★リトライしない。応答が返らなかった時に再送すると二重更新になる
     */
    async patchItemPrices(itemNo, { operationId, runId, expected, prices }) {
      return patch(itemNo, { operationId, runId, expected, prices });
    },
  };
}

/** miniPC の PATCH /service-api/qoo10/items/:itemNo/price を叩く */
async function defaultPatch(itemNo, { operationId, runId, expected, prices }) {
  const url = `${warehouseUrl()}/service-api/qoo10/items/${encodeURIComponent(String(itemNo).trim())}/price`;
  const res = await fetch(url, {
    method: 'PATCH',
    headers: serviceHeaders(),
    body: JSON.stringify({ operation_id: operationId, run_id: runId ?? null, expected, prices }),
    signal: AbortSignal.timeout(120_000),
  });
  let body = null;
  try { body = await res.json(); } catch { /* JSON でない応答 */ }
  return { status: res.status, body };
}
