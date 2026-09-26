/**
 * product-hub → miniPC の service-api「Amazon カタログ」から商品タイトルを取るクライアント (SP広告KW PR3c おまかせ・2026-09-26)
 *
 * 口 = miniPC の GET /service-api/research/product/:asin (SP-API Catalog Items。応答 = { ok, result: { asin, itemName, image, ... } })。
 * fba-box (apps/fba-box/images.js) と同じ口・同じ包み方。🚨 SP-API の鍵は miniPC にしか無いので Render からは service-api 経由だけ。
 * 中原さん「いつもチャッピーに聞く時は Amazon のタイトルだけ渡してる」→ おまかせの種の材料の中心。
 * 取れなくても受付は止めない (タイトル無しで商品名・楽天タイトルから種を作る)。throw しない
 */
import { ASIN_RE } from '../../../lib/asin.js';

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
export const TITLE_MAX = 300;

export function catalogConfigured() {
  return !!process.env.WAREHOUSE_SERVICE_TOKEN;
}

function serviceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    Authorization: `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    Accept: 'application/json',
  };
}

const defaultFetcher = async (asin, timeoutMs) => {
  const res = await fetch(`${WAREHOUSE_URL}/service-api/research/product/${encodeURIComponent(asin)}`, {
    headers: serviceHeaders(), redirect: 'manual', signal: AbortSignal.timeout(timeoutMs),
  });
  let j = null;
  try { j = await res.json(); } catch (_) { /* JSON でない */ }
  if (!res.ok) { const e = new Error(`miniPC が応答できません (HTTP ${res.status})`); e.code = 'unreachable'; throw e; }
  return j;
};
let fetcher = defaultFetcher;
export function _setCatalogFetcher(fn) { fetcher = fn || defaultFetcher; }

/**
 * ASIN の Amazon 商品タイトル。
 * @returns {Promise<{ok:true, title:string}|{ok:false, code:string, message:string}>}
 */
export async function fetchAmazonTitle(asin, { timeoutMs = 15_000 } = {}) {
  const a = String(asin || '').trim().toUpperCase();
  if (!ASIN_RE.test(a)) return { ok: false, code: 'no_asin', message: 'ASIN がありません' };
  if (!catalogConfigured()) return { ok: false, code: 'not_configured', message: 'Render の WAREHOUSE_SERVICE_TOKEN が未設定です' };
  try {
    const j = await fetcher(a, timeoutMs);
    const p = j?.result ?? j;
    const title = typeof p?.itemName === 'string' ? p.itemName.replace(/[\x00-\x1f\x7f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, TITLE_MAX) : '';
    if (!title) return { ok: false, code: 'no_title', message: 'カタログにタイトルがありません' };
    return { ok: true, title };
  } catch (e) {
    const timeout = e && (e.name === 'TimeoutError' || e.name === 'AbortError');
    return { ok: false, code: e.code || (timeout ? 'timeout' : 'unreachable'), message: e.message || String(e) };
  }
}
