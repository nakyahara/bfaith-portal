/**
 * Yahoo Shopping editItem テキスト系 field の単位計算 + truncate + HTML→text + explanation builder.
 *
 * Yahoo の文字数単位 (公式呼称: 半角換算 N 文字以内):
 *   半角 (ASCII) 1 文字 = 1 unit
 *   全角        1 文字 = 2 unit
 *
 * editItem field 仕様 (公式 https://developer.yahoo.co.jp/webapi/shopping/editItem.html):
 *   explanation:   全角 500  (= 1000 units)、 HTML 不可
 *   caption:       全角 5000 (= 10000 units)、 HTML 可
 *   additional1:   全角 5000 (= 10000 units)、 HTML 可
 *   sp_additional: 全角 5000 (= 10000 units)、 HTML 可 + 総容量 2000KB
 *   headline:      全角  30  (=   60 units)、 HTML 不可
 *
 * 2026-06-27 1 SKU smoke で editItem HTTP 400 (Code it-01033) → 本ファイル新規:
 *   旧実装は productDescription.pc を HTML のまま explanation に入れていた。
 *   Yahoo の explanation は HTML 不可 + 500 字以内なので、 plain text 化 + truncate。
 */
import * as cheerio from 'cheerio';

const ASCII_RE = /^[\x00-\x7F]$/;

export function yahooTextUnits(s) {
  let n = 0;
  for (const ch of String(s)) n += ASCII_RE.test(ch) ? 1 : 2;
  return n;
}

/**
 * grapheme cluster 単位で N units 以内に切る。
 *   - 絵文字や合字を途中で切らない (Intl.Segmenter)
 *   - units 超過したら直前の grapheme で停止
 */
export function truncateYahooTextUnits(s, maxUnits) {
  if (s == null) return '';
  const src = String(s);
  if (!src) return '';
  let out = '';
  let units = 0;
  const seg = new Intl.Segmenter('ja', { granularity: 'grapheme' });
  for (const { segment } of seg.segment(src)) {
    const u = yahooTextUnits(segment);
    if (units + u > maxUnits) break;
    out += segment;
    units += u;
  }
  return out;
}

/**
 * HTML → plain text 変換 (cheerio ベース):
 *   - <br> を改行に
 *   - <tr>/<p>/<li>/<div>/<h*> 末尾に改行を入れる
 *   - <script>/<style> は完全に削除 (text 汚染防止)
 *   - 連続 whitespace を 1 個に、 連続改行は 2 個までに圧縮
 */
export function htmlToPlainText(html) {
  if (!html) return '';
  const $ = cheerio.load(`<root>${String(html)}</root>`, { decodeEntities: true });
  $('script, style').remove();
  $('br').replaceWith('\n');
  $('tr, p, li, div, h1, h2, h3, h4, h5, h6').each((_, el) => {
    $(el).append('\n');
  });
  // td/th は空白で区切る (テーブル各セルが続けて読めるように)
  $('td, th').each((_, el) => {
    $(el).append(' ');
  });
  const txt = $('root').text();
  return txt
    .replace(/\r\n?/g, '\n')
    .replace(/[ \t　]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/**
 * Yahoo explanation 専用 builder.
 *   headline → itemName → 楽天 PC description 先頭 (HTML→text) の順で結合し、
 *   1000 units (全角 500) で truncate。
 *
 * - headline が長い場合に body が完全に消えないように、 itemName は 200 units で個別 truncate (Codex R2)
 * - 重複は除外
 */
export function buildYahooExplanation({ headline = '', itemName = '', html = '' } = {}) {
  const parts = [];
  // Codex R3 H-1: headline は Notion 由来で `<b>` 等の HTML が混入し得るので、
  //   explanation (HTML 不可) に入れる前に htmlToPlainText で plain 化する。
  const headlineStr = htmlToPlainText(String(headline || '')).trim();
  if (headlineStr) parts.push(headlineStr);
  // itemName も同様に念のため plain 化 (商品名に HTML が入る可能性は低いが二重防御)
  const itemNameRaw = htmlToPlainText(String(itemName || '')).trim();
  const itemNameStr = truncateYahooTextUnits(itemNameRaw, 200);
  if (itemNameStr && !parts.includes(itemNameStr)) parts.push(itemNameStr);
  const body = htmlToPlainText(html);
  if (body) parts.push(body);
  const joined = parts.join('\n').trim();
  return truncateYahooTextUnits(joined, 1000);
}

/**
 * Yahoo headline 専用 builder (HTML 不可、 全角 30 以内):
 *   - htmlToPlainText で plain 化 (Codex R3 H-1)
 *   - 60 units (= 全角 30) で truncate
 */
export function buildYahooHeadline(headline) {
  const plain = htmlToPlainText(String(headline || '')).trim();
  return truncateYahooTextUnits(plain, 60);
}
