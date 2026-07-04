/**
 * Yahoo 用 HTML サニタイズ (caption / explanation / additional1 / sp_additional)。
 * ローカル RYS src/lib/html-sanitize.js のロジックを cheerio (bfaith-portal 既存 dep) で再実装。
 *
 * 設計原則 (Phase 0 Q33 + Codex Phase C 確定):
 *   - 許可タグのみ残す: a/b/strong/i/em/u/br/p/div/span/ul/ol/li/table系/h1-6/hr
 *   - img タグ全廃 (要素ごと削除) ← default
 *   - 例外: allowImgFromHosts オプション指定時のみ、 src が指定 host で始まる img を残す
 *   - style / class 属性全廃
 *   - Yahoo 禁止 <font>/<center> はタグ除去 (中身は保持)
 *   - a タグは href のみ許可、 schemes は http/https のみ
 *
 * 楽天 HTML を Yahoo の caption/explanation/additional1/sp_additional に入れる前に必ず通す。
 */

import * as cheerio from 'cheerio';

import { classifyRakutenHref } from './rakuten-link-sanitizer.js';

const ALLOWED_TAGS = new Set([
  'a', 'b', 'strong', 'i', 'em', 'u', 'br', 'p', 'div', 'span',
  'ul', 'ol', 'li', 'table', 'thead', 'tbody', 'tr', 'th', 'td',
  'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'hr',
]);

// Yahoo 禁止だが strip-only (中身は残す)
const STRIP_BUT_KEEP_CONTENT = new Set(['font', 'center']);

// 完全削除 (要素ごと、 中身も) — img はオプションで上書き可
const REMOVE_COMPLETELY_BASE = new Set(['script', 'style', 'iframe', 'object', 'embed']);

// img の許可属性 (allowImgFromHosts 経由で残す場合のみ)
const IMG_ALLOWED_ATTRS = new Set(['src', 'width', 'height', 'alt', 'border']);

export const FORBIDDEN_BY_YAHOO = ['font', 'center'];

function isSafeHref(href) {
  if (!href) return false;
  const v = String(href).trim().toLowerCase();
  return v.startsWith('http://') || v.startsWith('https://');
}

function isImgFromAllowedHost(src, allowedHostsLower) {
  if (!src || !allowedHostsLower?.size) return false;
  try {
    const u = new URL(src);
    if (u.protocol !== 'https:') return false;
    return allowedHostsLower.has(u.hostname.toLowerCase());
  } catch (_) {
    return false;
  }
}

/**
 * @param {string|null} html
 * @param {object} [opts]
 * @param {string[]} [opts.allowImgFromHosts=[]]
 *   指定時のみ <img src="https://{host}/..."> を残す (Codex Phase E image-html R3 medium:
 *   hostname は lower-case で比較)。 空配列なら従来通り img を完全削除。
 */
export function sanitizeProductHtml(html, opts = {}) {
  if (html == null) return '';
  const s = String(html);
  if (s.trim() === '') return '';
  const allowedHostsLower = new Set(
    (opts.allowImgFromHosts || []).map((h) => String(h).trim().toLowerCase()).filter(Boolean)
  );
  const allowImg = allowedHostsLower.size > 0;

  const $ = cheerio.load(`<div id="__rys_root__">${s}</div>`, null, false);
  const root = $('#__rys_root__');

  // 完全削除 (要素ごと): script/style/iframe/object/embed は常時。 img は allowImg=false のときのみ。
  REMOVE_COMPLETELY_BASE.forEach((tag) => root.find(tag).remove());
  if (!allowImg) {
    root.find('img').remove();
  } else {
    // allowImg のとき: src が allow 外の img も削除
    root.find('img').each((_, el) => {
      const src = el.attribs?.src;
      if (!isImgFromAllowedHost(src, allowedHostsLower)) {
        $(el).remove();
      }
    });
  }

  // 楽天リンク根絶 (R16 2026-07-04 中原さん指示・重大):
  //   Yahoo!ショッピング規約上、他モール (楽天) への誘導リンクは違反。
  //   - 自店商品リンク item.rakuten.co.jp/{shop}/{code}/ → Yahoo ストア URL に書換
  //   - その他の楽天系リンク (他店/検索/CDN/rakuten.ne.jp 等) → <a> を unwrap (リンク除去)
  //   YAHOO_SELLER_ID 未設定でも楽天リンクは絶対に残さない (書換不能なら unwrap)。
  root.find('a').each((_, el) => {
    const href = el.attribs?.href;
    if (!href) return;
    const c = classifyRakutenHref(href);
    if (c.kind === 'rewrite') {
      $(el).attr('href', c.yahooUrl);
    } else if (c.kind === 'remove') {
      const $el = $(el);
      $el.replaceWith($el.contents());
    }
  });

  // strip-only (中身を残してタグだけ unwrap)
  STRIP_BUT_KEEP_CONTENT.forEach((tag) => {
    root.find(tag).each((_, el) => {
      const $el = $(el);
      $el.replaceWith($el.contents());
    });
  });

  // 許可外タグを unwrap (中身は残す) — img が allowImg=true のときは ALLOWED として扱う
  root.find('*').each((_, el) => {
    const tagName = el.tagName?.toLowerCase();
    if (!tagName) return;
    if (tagName === 'img' && allowImg) return;
    if (!ALLOWED_TAGS.has(tagName)) {
      const $el = $(el);
      $el.replaceWith($el.contents());
    }
  });

  // 全属性を整理:
  //   - img: allowImg=true なら src/width/height/alt/border のみ残す
  //   - a: href (safe scheme) のみ残す
  //   - その他: 全属性削除 (style/class/onclick 等を完全に剥がす)
  root.find('*').each((_, el) => {
    const tagName = el.tagName?.toLowerCase();
    const $el = $(el);
    const attribs = el.attribs || {};
    for (const name of Object.keys(attribs)) {
      if (tagName === 'img' && allowImg && IMG_ALLOWED_ATTRS.has(name.toLowerCase())) continue;
      if (tagName === 'a' && name === 'href' && isSafeHref(attribs.href)) continue;
      $el.removeAttr(name);
    }
  });

  return root.html() || '';
}
