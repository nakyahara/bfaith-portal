/**
 * 商品別 想定利益 — 楽天 RMS 呼び出し (miniPC 内で直接叩く)
 *
 * 🚨 Render 用の rakuten-rms-proxy.js (Cloudflare Tunnel 経由) ではなく、
 *    miniPC 内で動くこのバッチは warehouse の rakuten-client.js を直接使う。
 *    レート制御 (MIN_GAP 1.1 秒の直列キュー) は rakutenRequest() に一元化されているので、
 *    価格一括改定ツールと同じ枠を共有できる (§15-10)。
 */
import { rakutenRequest, rmsErrorSuffix, RMS_AUTH_HINT } from '../warehouse/rakuten-client.js';

/**
 * items/search を 1 ページ取る。
 * @param {string} cursorMark
 * @param {number} hits
 * @returns {Promise<{results?:Array, items?:Array, nextCursorMark?:string}>}
 */
export async function searchItemsPage(cursorMark = '*', hits = 100) {
  const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=${hits}`;
  const result = await rakutenRequest({ path: apiPath });
  if (result.status !== 200) {
    // 401/403 = ライセンスキー失効の可能性 (90日で切れる)
    const hint = (result.status === 401 || result.status === 403) ? RMS_AUTH_HINT : '';
    throw new Error(`RMS items/search HTTP ${result.status}${rmsErrorSuffix(result.data)}${hint}`);
  }
  return result.data;
}

/** fetch-listings.js のデフォルト実装として使う */
export async function callRakutenProxy(pathWithQuery) {
  const m = /cursorMark=([^&]*)/.exec(pathWithQuery);
  const cursorMark = m ? decodeURIComponent(m[1]) : '*';
  return searchItemsPage(cursorMark);
}
