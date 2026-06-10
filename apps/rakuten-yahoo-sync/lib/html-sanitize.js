/**
 * Yahoo 用 HTML サニタイズ (caption / explanation / additional1 / sp_additional)。
 * ローカル RYS src/lib/html-sanitize.js のロジックを cheerio (bfaith-portal 既存 dep) で再実装。
 *
 * 設計原則 (Phase 0 Q33 + Codex Phase C 確定):
 *   - 許可タグのみ残す: a/b/strong/i/em/u/br/p/div/span/ul/ol/li/table系/h1-6/hr
 *   - img タグ全廃 (要素ごと削除)
 *   - style / class 属性全廃
 *   - Yahoo 禁止 <font>/<center> はタグ除去 (中身は保持)
 *   - a タグは href のみ許可、 schemes は http/https のみ
 *
 * 楽天 HTML を Yahoo の caption/explanation/additional1/sp_additional に入れる前に必ず通す。
 */

import * as cheerio from 'cheerio';

const ALLOWED_TAGS = new Set([
  'a', 'b', 'strong', 'i', 'em', 'u', 'br', 'p', 'div', 'span',
  'ul', 'ol', 'li', 'table', 'thead', 'tbody', 'tr', 'th', 'td',
  'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'hr',
]);

// Yahoo 禁止だが strip-only (中身は残す)
const STRIP_BUT_KEEP_CONTENT = new Set(['font', 'center']);

// 完全削除 (要素ごと、 中身も)
const REMOVE_COMPLETELY = new Set(['img', 'script', 'style', 'iframe', 'object', 'embed']);

export const FORBIDDEN_BY_YAHOO = ['font', 'center'];

function isSafeHref(href) {
  if (!href) return false;
  const v = String(href).trim().toLowerCase();
  return v.startsWith('http://') || v.startsWith('https://');
}

export function sanitizeProductHtml(html) {
  if (html == null) return '';
  const s = String(html);
  if (s.trim() === '') return '';
  const $ = cheerio.load(`<div id="__rys_root__">${s}</div>`, null, false);
  const root = $('#__rys_root__');

  // 完全削除 (要素ごと)
  REMOVE_COMPLETELY.forEach((tag) => root.find(tag).remove());

  // strip-only (中身を残してタグだけ unwrap)
  STRIP_BUT_KEEP_CONTENT.forEach((tag) => {
    root.find(tag).each((_, el) => {
      const $el = $(el);
      $el.replaceWith($el.contents());
    });
  });

  // 許可外タグを unwrap (中身は残す)
  root.find('*').each((_, el) => {
    const tagName = el.tagName?.toLowerCase();
    if (!tagName) return;
    if (!ALLOWED_TAGS.has(tagName)) {
      const $el = $(el);
      $el.replaceWith($el.contents());
    }
  });

  // 全属性を整理: style/class 除去、 a は href (safe scheme) のみ
  root.find('*').each((_, el) => {
    const tagName = el.tagName?.toLowerCase();
    const $el = $(el);
    const attribs = el.attribs || {};
    for (const name of Object.keys(attribs)) {
      if (tagName === 'a' && name === 'href' && isSafeHref(attribs.href)) continue;
      $el.removeAttr(name);
    }
  });

  return root.html() || '';
}
