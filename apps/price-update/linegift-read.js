/**
 * linegift-read.js — LINEギフトの現在売価を読むだけのクライアント (価格一括改定)
 *
 * ★**読み取り専用**。送信 (apply) の関数はこのファイルに存在しない。
 *   LINEギフトの更新は `PATCH /items/{item_id}` = 商品まるごと更新しか無く、
 *   Yahoo `editItem` と同じ「送らなかった項目が消える」危険がある。さらに GET が返す画像は
 *   `id`+`url` なのに PATCH は `temporary_uuid` を要求するため、**読んだ画像を送り返せない**。
 *   部分更新かどうかを実測するまで書き込みは作らない。
 *   → mall-capabilities.js でも linegift は updatable:false / executable:false。
 *
 * 経路: Render → miniPC service-api (/service-api/linegift/*) → gift-shop-cms.line.biz
 *   `LINEGIFT_ACCESS_TOKEN` は miniPC にしか無い。楽天 (details-bulk) / Qoo10 と同じ形。
 *
 * 🚨**「引き当てできない」を「出品していない」に変換しない**。
 *   LINEギフトの商品APIは数値の item_id でしか引けず、その手がかりは受注実績から取っている。
 *   売れたことが無い商品は手がかりが無いだけで、出品はされているかもしれない。
 *   ([[feedback_existence_check_needs_authoritative_source]] — 楽天で 407件中135件を誤判定した)
 */

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    const err = new Error(`${name} が未設定です。LINEギフトの価格は取得しません (fail-closed)`);
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
 * 出品コード (NEコード) で LINEギフトの現在売価を読む (miniPC 経由)。
 *
 * @param {string} code 出品コード = LINEギフトの variation.code
 * @returns {Promise<{ok:boolean, price?:number|null, error?:string, message?:string}>}
 */
export async function fetchLinegiftItemByCode(code, deps = {}) {
  const fetchImpl = deps.fetchImpl || fetch;
  const url = `${warehouseUrl()}/service-api/linegift/items/by-code/${encodeURIComponent(String(code).trim())}/price`;
  const res = await fetchImpl(url, { headers: serviceHeaders(), signal: AbortSignal.timeout(30_000) });
  let body = null;
  try { body = await res.json(); } catch { /* JSON でない応答 */ }
  if (!res.ok || !body?.ok) {
    return {
      ok: false,
      error: body?.error || `HTTP_${res.status}`,
      message: body?.message || `miniPC が ${res.status} を返しました`,
    };
  }
  return { ok: true, ...body };
}

/** miniPC が返すエラーを、画面に出す日本語にする。★「未出品」と言い切らない */
export function reasonOf(got) {
  const map = {
    ITEM_ID_UNKNOWN: 'LINEギフトの商品IDが分かりません (受注実績が無い商品。出品の有無は判定していません)',
    ITEM_NOT_FOUND: 'この商品IDが LINEギフトに見つかりません',
    CODE_MISMATCH: '別の商品が返りました (取り違え防止のため確定しません)',
  };
  return map[got?.error] || got?.message || 'LINEギフトから取得できません';
}

/**
 * 出品コードごとに現在売価を引く。楽天・Yahoo・au PAY・Qoo10 の fetch*Prices と同じ形で返す。
 *
 * ★確定させない条件 (どれも「嘘の値を確定させない」ため):
 *   - 引き当てできない / 別商品が返った / 通信に失敗した
 *   - 販売中 (status='sale') でない — 値付けしても客に見えない
 *   - セール価格が入っている — 表示している通常価格と実売価がずれる
 *   - price が整数円で読めない
 *
 * @param {Array<{key:string, code:string}>} targets key = 行キー / code = 出品コード
 * @param {object} [deps] テスト用の差し替え ({ fetchLinegiftItemByCode, gapMs })
 * @returns {Promise<Map<string, {price:number|null, itemCode:string, found:boolean, reason:string|null, itemName:string|null}>>}
 */
export async function fetchLinegiftPrices(targets, deps = {}) {
  const out = new Map();
  const list = (targets || [])
    .map((t) => ({ key: String(t.key || ''), code: String(t.code || '').trim() }))
    .filter((t) => t.key && t.code);
  if (list.length === 0) return out;

  const fetchOne = deps.fetchLinegiftItemByCode || fetchLinegiftItemByCode;
  // LINEギフト側のレート制限は miniPC のセマフォ (同時1) が持つ。ここは間隔だけ空ける
  const gapMs = Number.isInteger(deps.gapMs) ? deps.gapMs : 400;
  const cache = new Map();

  for (const t of list) {
    const cacheKey = t.code.toLowerCase();
    if (!cache.has(cacheKey)) {
      try { cache.set(cacheKey, await fetchOne(t.code)); }
      catch (e) { cache.set(cacheKey, { ok: false, message: e.message }); }
      if (gapMs > 0) await new Promise((r) => setTimeout(r, gapMs));
    }
    const got = cache.get(cacheKey);
    const base = { price: null, skuCode: null, itemCode: t.code, itemName: null };

    if (!got?.ok) {
      out.set(t.key, { ...base, found: false, reason: reasonOf(got) });
      continue;
    }
    const itemName = got.itemName ?? null;
    if (got.status !== 'sale') {
      out.set(t.key, { ...base, itemName, found: false,
        reason: `販売中の商品ではありません (状態 ${got.status ?? '不明'})。値付けしても客に見えないため対象外です` });
      continue;
    }
    // ★セール中は通常価格と実売価がずれる。読めているうちに弾く (Yahoo と同じ扱い)
    if (got.hasSale) {
      out.set(t.key, { ...base, itemName, found: false,
        reason: `セール価格が設定されています (${got.salePrice ?? '値は取得できず'})。実売価がずれるため対象外です` });
      continue;
    }
    if (!Number.isInteger(got.price) || got.price < 1) {
      out.set(t.key, { ...base, itemName, found: false,
        reason: `価格を整数円として読めません (${got.price ?? 'なし'})` });
      continue;
    }
    out.set(t.key, { price: got.price, skuCode: null, itemCode: got.code || t.code, itemName,
      webUrl: got.webUrl || null, found: true, reason: null });
  }
  return out;
}
