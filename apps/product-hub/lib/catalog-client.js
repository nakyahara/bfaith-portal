/**
 * product-hub → miniPC の service-api「Amazon カタログ」から商品ページの情報を取るクライアント (SP広告KW PR3c おまかせ・2026-09-26)
 *
 * 口 = miniPC の GET /service-api/research/product/:asin (SP-API Catalog Items。応答 = { ok, result: { asin, itemName, brand, category, bulletPoints, description, ... } })。
 * fba-box (apps/fba-box/images.js) と同じ口・同じ包み方。🚨 SP-API の鍵は miniPC にしか無いので Render からは service-api 経由だけ。
 * 中原さん「いつもチャッピーに聞く時は Amazon のタイトルだけ渡してる」「Amazon のリンク先の情報も考慮して」→ おまかせの種と最終案の材料。
 * 取れなくても受付は止めない (無しで商品名・楽天タイトルから種を作る)。throw しない
 */
import { ASIN_RE } from '../../../lib/asin.js';

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
export const TITLE_MAX = 300;
export const BULLETS_MAX = 10;
export const BULLET_MAX = 300;
export const DESCRIPTION_MAX = 1500;

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

const clean = (v, max) => (typeof v === 'string' ? v.replace(/[\x00-\x1f\x7f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, max) : '');

/**
 * ASIN の Amazon 商品ページの情報 (タイトル・ブランド・カテゴリ・箇条書き・説明)。タイトルが無ければ取れなかった扱い。
 * @returns {Promise<{ok:true, title, brand, category, bullets:string[], description}|{ok:false, code:string, message:string}>}
 */
export async function fetchAmazonCatalog(asin, { timeoutMs = 15_000 } = {}) {
  const a = String(asin || '').trim().toUpperCase();
  if (!ASIN_RE.test(a)) return { ok: false, code: 'no_asin', message: 'ASIN がありません' };
  if (!catalogConfigured()) return { ok: false, code: 'not_configured', message: 'Render の WAREHOUSE_SERVICE_TOKEN が未設定です' };
  try {
    const j = await fetcher(a, timeoutMs);
    const p = j?.result ?? j;
    const title = clean(p?.itemName, TITLE_MAX);
    if (!title) return { ok: false, code: 'no_title', message: 'カタログにタイトルがありません' };
    const bullets = (Array.isArray(p?.bulletPoints) ? p.bulletPoints : []).map((b) => clean(b, BULLET_MAX)).filter(Boolean).slice(0, BULLETS_MAX);
    return { ok: true, title, brand: clean(p?.brand, 100) || null, category: clean(p?.category, 100) || null, bullets, description: clean(p?.description, DESCRIPTION_MAX) || null };
  } catch (e) {
    const timeout = e && (e.name === 'TimeoutError' || e.name === 'AbortError');
    return { ok: false, code: e.code || (timeout ? 'timeout' : 'unreachable'), message: e.message || String(e) };
  }
}

/** タイトルだけ (旧い呼び方)。@returns {Promise<{ok:true, title}|{ok:false, code, message}>} */
export async function fetchAmazonTitle(asin, opts = {}) {
  const r = await fetchAmazonCatalog(asin, opts);
  return r.ok ? { ok: true, title: r.title } : r;
}
