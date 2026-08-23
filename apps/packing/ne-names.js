/**
 * NE商品マスタ名の解決 (梱包画面の表示用 — 中原さん指示 2026-08-22)。
 *
 * 源泉 = warehouse.db raw_ne_products (NEから毎日取込む社内商品マスタ)。
 * 納品書CSVの印字商品名はセット品受注でモールのSEO長文になるため、
 * セットに関係なく必ず「単品の社内商品名」を出す。
 * 取得は warehouse service-api (localhost) 経由 — DB直開きはしない (単一入口方針)。
 *
 * fail-soft: API不達・未設定でも空を返し、呼び出し側はCSVの商品名列へフォールバック。
 * メモリキャッシュ30分 (マスタ改名は当日中に追従すれば十分)。
 */

const TTL_MS = 30 * 60 * 1000;
const NEG_TTL_MS = 5 * 60 * 1000;   // 見つからなかったSKUの再問い合わせ抑制 (新商品登録は5分で追従)
const ERR_BACKOFF_MS = 60 * 1000;   // API失敗直後は問い合わせ自体を休む (warehouse停止中に毎ページ待たせない)
const MAX_PER_CALL = 1000;          // サーバー側上限と同値。超過分が黙って切られ「存在しない」扱いになるのを防ぐ
const TIMEOUT_MS = 1500;            // localhost前提。作業画面を待たせない
const _cache = new Map();           // skuLower → { name: string|null, at: number }
let _errUntil = 0;

function apiConfig() {
  const base = String(process.env.WAREHOUSE_URL || '').trim().replace(/\/+$/, '');
  const token = process.env.WAREHOUSE_SERVICE_TOKEN;
  if (!base || !token) return null;
  const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
  if (process.env.CF_ACCESS_CLIENT_ID && process.env.CF_ACCESS_CLIENT_SECRET) {
    headers['CF-Access-Client-Id'] = process.env.CF_ACCESS_CLIENT_ID;
    headers['CF-Access-Client-Secret'] = process.env.CF_ACCESS_CLIENT_SECRET;
  }
  return { base, headers };
}

/**
 * 商品コード→NEマスタ商品名の一括解決 (キャッシュつき)。
 * @returns Map<sku, name> — 解決できたものだけ (呼び出し側でフォールバック)
 * @param fetchFn テスト注入用
 */
export async function neNamesFor(skus, fetchFn = fetch) {
  const now = Date.now();
  const result = new Map();
  const missing = [];
  const stale = new Map();   // 期限切れの正キャッシュ — API不調時はこれで表示を守る (名前は古くても短い)
  for (const sku of new Set((skus || []).map((s) => String(s ?? '').trim()).filter(Boolean))) {
    const c = _cache.get(sku.toLowerCase());
    if (c && now - c.at < (c.name ? TTL_MS : NEG_TTL_MS)) {
      if (c.name) result.set(sku, c.name);
    } else {
      if (c?.name) stale.set(sku, c.name);
      missing.push(sku);
    }
  }
  if (missing.length === 0) return result;
  const cfg = apiConfig();
  if (!cfg || now < _errUntil) {
    for (const [k, v] of stale) result.set(k, v);
    return result;
  }
  try {
    for (let i = 0; i < missing.length; i += MAX_PER_CALL) {
      const chunk = missing.slice(i, i + MAX_PER_CALL);
      const res = await fetchFn(`${cfg.base}/service-api/ne-products/names`, {
        method: 'POST',
        headers: cfg.headers,
        body: JSON.stringify({ codes: chunk }),
        signal: AbortSignal.timeout(TIMEOUT_MS),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json();
      const names = body?.names || {};
      for (const sku of chunk) {
        // own-property限定+文字列検証: 異常応答 (prototype名と同名SKU・非文字列) でキャッシュを汚さない
        const raw = Object.hasOwn(names, sku) ? names[sku] : null;
        const name = typeof raw === 'string' && raw.trim() ? raw.trim() : null;
        _cache.set(sku.toLowerCase(), { name, at: now });
        if (name) result.set(sku, name);
      }
    }
  } catch (e) {
    _errUntil = now + ERR_BACKOFF_MS;
    for (const [k, v] of stale) if (!result.has(k)) result.set(k, v);
    console.warn(`[packing] NEマスタ名の取得失敗 (CSV名へフォールバック): ${e.message}`);
  }
  return result;
}

/** テスト用: キャッシュ初期化。 */
export function _clearNeNameCache() {
  _cache.clear();
  _errUntil = 0;
}

/** テスト用: キャッシュ本体 (TTL切れの再現に使う)。 */
export function _cacheForTest() {
  return _cache;
}
