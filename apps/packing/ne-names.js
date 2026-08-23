/**
 * NE商品マスタ名の解決 (梱包画面の表示用 — 中原さん指示 2026-08-22)。
 *
 * 源泉 = warehouse.db raw_ne_products (NEから毎日取込む社内商品マスタ)。
 * 納品書CSVの印字商品名はセット品受注でモールのSEO長文になるため、
 * 🚨必ず「単品の社内商品名」を出す。セット商品の名前は絶対に使わない (2026-08-23 絶対ルール —
 * セット親は warehouse 側で構成単品へ展開され、セット自身の名前はこのAPIからは来ない)。
 * 取得は warehouse service-api (localhost) 経由 — DB直開きはしない (単一入口方針)。
 *
 * fail-soft: API不達・未設定でも空を返し、呼び出し側はCSVの「商品名」列 (=NE単品名) へフォールバック。
 * セットだが展開に失敗したコード (unresolved) は isSet で返し、呼び出し側はCSV名へ戻さず未解決表示。
 * メモリキャッシュ30分 (マスタ改名は当日中に追従すれば十分)。
 */

const TTL_MS = 30 * 60 * 1000;
const NEG_TTL_MS = 5 * 60 * 1000;   // 見つからなかったSKUの再問い合わせ抑制 (新商品登録は5分で追従)
const ERR_BACKOFF_MS = 60 * 1000;   // API失敗直後は問い合わせ自体を休む (warehouse停止中に毎ページ待たせない)
const MAX_PER_CALL = 1000;          // サーバー側上限と同値。超過分が黙って切られ「存在しない」扱いになるのを防ぐ
const TIMEOUT_MS = 1500;            // localhost前提。作業画面を待たせない
const _cache = new Map();           // skuLower → { name: string|null, comps, isSet, at: number }
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

/** 応答オブジェクトを lower キーの Map に (own-property のみ → prototype名と同名SKUで誤解決しない)。 */
function lowerKeyed(obj) {
  const m = new Map();
  if (obj && typeof obj === 'object') {
    for (const k of Object.keys(obj)) m.set(String(k).trim().toLowerCase(), obj[k]);
  }
  return m;
}

/** キャッシュ行 → 呼び出し側へ返す形。名前も未解決フラグも無ければ null (= Map に入れない)。 */
function entryOf(c) {
  if (c.name) return { name: c.name, comps: c.comps || null, isSet: false };
  if (c.isSet) return { name: null, comps: null, isSet: true };
  return null;
}

/**
 * 商品コード→NEマスタの**単品**商品名の一括解決 (キャッシュつき)。
 * 大小文字違いは同じ商品として扱い (warehouse も NOCASE)、呼び出し側の表記ごとに結果を返す。
 * @returns Map<sku, {name, comps, isSet}>
 *   - name あり: 単品名。comps = セット展開したときだけ [{sku, name, qty}] (それ以外 null)
 *   - name null + isSet true: セットだが展開失敗 → 呼び出し側は CSV名へ戻さず「商品名未解決」を表示
 *   - Map に無い: 未登録の単品 or API不達 → 呼び出し側は CSVの商品名列 (単品名) へフォールバック
 * @param fetchFn テスト注入用
 */
export async function neNamesFor(skus, fetchFn = fetch) {
  const now = Date.now();
  const result = new Map();
  const wanted = new Map();   // lower → Set<呼び出し側の表記>
  for (const s of skus || []) {
    const sku = String(s ?? '').trim();
    if (!sku) continue;
    const k = sku.toLowerCase();
    if (!wanted.has(k)) wanted.set(k, new Set());
    wanted.get(k).add(sku);
  }
  const put = (k, entry) => { if (entry) for (const sp of wanted.get(k)) result.set(sp, entry); };

  const missing = [];         // lower keys
  const stale = new Map();    // lower → entry (期限切れの正キャッシュ — API不調時はこれで表示を守る)
  for (const [k] of wanted) {
    const c = _cache.get(k);
    if (c && now - c.at < (c.name ? TTL_MS : NEG_TTL_MS)) {
      put(k, entryOf(c));
    } else {
      if (c?.name) stale.set(k, entryOf(c));
      missing.push(k);
    }
  }
  if (missing.length === 0) return result;
  const cfg = apiConfig();
  if (!cfg || now < _errUntil) {
    for (const [k, e] of stale) put(k, e);
    return result;
  }
  try {
    for (let i = 0; i < missing.length; i += MAX_PER_CALL) {
      const chunk = missing.slice(i, i + MAX_PER_CALL);
      const codes = chunk.map((k) => [...wanted.get(k)][0]);   // 送るのは最初の表記 (応答は lower で照合)
      const res = await fetchFn(`${cfg.base}/service-api/ne-products/names`, {
        method: 'POST',
        headers: cfg.headers,
        body: JSON.stringify({ codes }),
        signal: AbortSignal.timeout(TIMEOUT_MS),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json();
      const names = lowerKeyed(body?.names);
      const compsAll = lowerKeyed(body?.components);
      // セットだが展開に失敗したコード → CSV名 (セット名の可能性) へ戻さず「未解決」として扱う
      const unresolved = new Set((Array.isArray(body?.unresolved) ? body.unresolved : [])
        .map((s) => String(s ?? '').trim().toLowerCase()));
      for (const k of chunk) {
        const raw = names.get(k);
        const name = typeof raw === 'string' && raw.trim() ? raw.trim() : null;
        // セット展開したときだけ構成 [{sku, name, qty}] が付く (名前は単品名のみ。セット名は来ない)。
        // 1件でも壊れていたら構成ごと捨てる (名前は残す)
        const rawComps = name && Array.isArray(compsAll.get(k)) ? compsAll.get(k) : null;
        let comps = null;
        if (rawComps && rawComps.length > 0) {
          const ok = rawComps.every((c) => c && typeof c.sku === 'string' && c.sku.trim()
            && typeof c.name === 'string' && c.name.trim() && Number.isSafeInteger(c.qty) && c.qty > 0);
          if (ok) comps = rawComps.map((c) => ({ sku: c.sku.trim(), name: c.name.trim(), qty: c.qty }));
        }
        const isSet = !name && unresolved.has(k);
        const entry = { name, comps, isSet, at: now };
        _cache.set(k, entry);
        put(k, entryOf(entry));
      }
    }
  } catch (e) {
    _errUntil = now + ERR_BACKOFF_MS;
    for (const [k, e] of stale) {
      if (![...wanted.get(k)].some((sp) => result.has(sp))) put(k, e);
    }
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
