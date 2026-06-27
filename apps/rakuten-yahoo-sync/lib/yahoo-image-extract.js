/**
 * 楽天 HTML (salesDescription / productDescription.sp / .pc) から <img src="..."> を抽出し、
 * normalizeRakutenImageUrl で完全 URL 化する helper.
 *
 * 2026-06-27 1 SKU smoke で additional1 / sp_additional から画像が全部消えてた原因:
 *   楽天 HTML の <img src> を Yahoo に送る前に sanitize で削除していた。
 *   本ファイルは画像 URL を抽出して uploader に渡し、 Yahoo CDN URL に置換するための前処理。
 */
import * as cheerio from 'cheerio';
import { normalizeRakutenImageUrl } from './image-uploader.js';

/**
 * HTML 内の <img src> を全部抽出し、 楽天 URL に正規化したもののみ返す。
 *   - protocol-relative や相対パスは normalizeRakutenImageUrl で https://image.rakuten.co.jp/... に
 *   - 同一 URL は順序保持で 1 度のみ返す (重複削除)
 *   - 楽天 cabinet URL でないもの (https://example.com/foo.jpg 等) も normalize 通過すれば残す
 *     (caller 側で楽天かどうか判定する)
 *
 * @param {string} html
 * @returns {string[]} 正規化された絶対 URL のリスト (重複なし)
 */
export function extractRakutenImageUrls(html) {
  if (!html || typeof html !== 'string') return [];
  const $ = cheerio.load(`<root>${html}</root>`, { decodeEntities: true });
  const seen = new Set();
  const out = [];
  $('img').each((_, el) => {
    const src = $(el).attr('src');
    if (!src) return;
    const normalized = normalizeRakutenImageUrl(src);
    if (!normalized) return;
    if (!seen.has(normalized)) {
      seen.add(normalized);
      out.push(normalized);
    }
  });
  return out;
}
