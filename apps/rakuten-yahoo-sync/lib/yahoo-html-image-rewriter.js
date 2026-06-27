/**
 * 楽天 HTML 内の <img src="楽天URL"> を Yahoo CDN URL に置換 + sanitize.
 *
 * 設計 (Codex Phase E image-html R1-R3 stop):
 *   - urlMap (楽天URL → Yahoo URL) で置換
 *   - urlMap に無い楽天 img があれば throw (fail-closed)
 *   - 置換後 html を sanitizeProductHtml に通す (allowImgFromHosts に urlMap の Yahoo host)
 *   - sanitize は <img src/width/height/alt/border> のみ残す
 *
 * 失敗時の fallback (img → <br>) は廃止。 全て fail-closed (R1 H-1)。
 */
import * as cheerio from 'cheerio';

import { normalizeRakutenImageUrl } from './image-uploader.js';
import { sanitizeProductHtml } from './html-sanitize.js';

export class YahooHtmlRewriteError extends Error {
  constructor(message, { rakutenUrl = null } = {}) {
    super(message);
    this.name = 'YahooHtmlRewriteError';
    this.rakutenUrl = rakutenUrl;
  }
}

/**
 * @param {string} rakutenHtml 楽天 raw HTML (salesDescription / productDescription.sp)
 * @param {Map<string,string>|object} urlMap 楽天URL (normalize 済) → Yahoo URL
 * @param {object} opts
 * @param {string[]} opts.allowedYahooHosts hostname (lower-case) の許可リスト
 * @param {string[]} [opts.excludedRakutenUrls=[]] customPatterns / builtin (coupon/review) 等で
 *   意図的に除外された楽天 URL の list。 これに該当する <img> は silent に削除する
 *   (urlMap に無い throw とは区別、 Codex Phase E image-html R4 H-1)。
 * @returns {string} 置換 + sanitize 済 HTML
 */
export function rewriteAndSanitizeYahooHtml(rakutenHtml, urlMap, { allowedYahooHosts, excludedRakutenUrls = [] } = {}) {
  if (rakutenHtml == null) return '';
  const s = String(rakutenHtml);
  if (s.trim() === '') return '';
  // Codex R7 medium: HTML に <img> が無いケース (画像 0 件商品 + br/table のみ) では
  // allowedYahooHosts が空でも sanitize 経由で素通す (rewrite 不要)。
  const hasImg = /<img\b/i.test(s);
  if (!hasImg) {
    return sanitizeProductHtml(s); // img 無し → 通常 sanitize (img 削除モード) で問題なし
  }
  if (!allowedYahooHosts || !Array.isArray(allowedYahooHosts) || allowedYahooHosts.length === 0) {
    throw new YahooHtmlRewriteError('allowedYahooHosts is required (non-empty array) when HTML contains <img>');
  }

  const map = urlMap instanceof Map ? urlMap : new Map(Object.entries(urlMap || {}));
  const excludedSet = new Set(excludedRakutenUrls);

  // 1. cheerio で <img> を全部走査して src を置換 (urlMap に無ければ throw)
  const $ = cheerio.load(`<root>${s}</root>`, { decodeEntities: true });
  $('img').each((_, el) => {
    const $el = $(el);
    const rawSrc = $el.attr('src');
    if (!rawSrc) {
      $el.remove();
      return;
    }
    const normalized = normalizeRakutenImageUrl(rawSrc);
    if (!normalized) {
      throw new YahooHtmlRewriteError(`cannot normalize img src: ${rawSrc}`, { rakutenUrl: rawSrc });
    }
    // R4 H-1: 意図的除外 (coupon / review / customPatterns) は silent 削除
    if (excludedSet.has(normalized)) {
      $el.remove();
      return;
    }
    // Yahoo URL を既に入れてある場合 (再 publish 等) も pass through
    if (allowedYahooHosts.some((h) => normalized.includes(`://${h.toLowerCase()}`))) {
      $el.attr('src', normalized);
      return;
    }
    const yahooUrl = map.get(normalized);
    if (!yahooUrl) {
      throw new YahooHtmlRewriteError(
        `no Yahoo URL mapped for rakuten image: ${normalized}`,
        { rakutenUrl: normalized }
      );
    }
    $el.attr('src', yahooUrl);
  });

  const rewritten = $('root').html() || '';

  // 2. sanitize (img を許可、 Yahoo host のみ)
  return sanitizeProductHtml(rewritten, { allowImgFromHosts: allowedYahooHosts });
}
