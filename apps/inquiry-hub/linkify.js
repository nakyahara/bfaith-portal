/**
 * inquiry-hub 本文中のURLをリンクにする (2026-08-26 中原さん要望
 * 「本文のリンクがテキストのままなのでリンクになるようにしてほしい」)。
 *
 * 例: メルカリShopsの問い合わせ通知に入っている
 *     https://mercari-shops.com/seller/shops/…/inquiries/… を1クリックで開けるようにする。
 *
 * ■ 安全性
 *   - http/https のみをリンクにする → javascript: 等のスキームは構造的に入らない
 *   - エスケープ後の文字列に正規表現を掛けない。生テキストを URL/非URL に切り分け、
 *     それぞれを HTML エスケープしてから <a> を組み立てる
 *     (エスケープ後だと URL の & が &amp; になっていて URL の切り出しが壊れる)
 */

/** HTMLエスケープ (router.js の he と同じ規則。この関数だけで完結させるため再掲) */
const esc = s => String(s == null ? '' : s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

/** 本文中のURL。空白・山括弧・引用符・全角括弧は URL に含めない
 * (「(https://…)」「【https://…】」のような囲みを URL に巻き込まないため) */
export const LINK_RE = /https?:\/\/[^\s<>"'`|\\^{}（）【】「」『』]+/g;

/** URLの末尾に付いた句読点・閉じ括弧はリンクから外す (「…をご確認ください。」「(https://x)」対策) */
export const trimUrlTail = url => url.replace(/[.,;:!?、。）)\]】」』]+$/, '');

/**
 * プレーンテキストを HTML エスケープしつつ URL を <a> に変換して返す。
 * 改行の <br> 化は呼び元で行う (この関数は改行をそのまま残す)。
 */
export function linkifyText(text) {
  const src = String(text == null ? '' : text);
  let out = '', last = 0;
  for (const m of src.matchAll(LINK_RE)) {
    const url = trimUrlTail(m[0]);
    if (!url) continue;
    out += esc(src.slice(last, m.index));
    out += `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(url)}</a>`;
    last = m.index + url.length;
  }
  return out + esc(src.slice(last));
}

/**
 * 折りたたみ (「… 続きを表示」) の境目が URL の途中に落ちると、リンクが畳みの前後で
 * 2つに割れてどちらも壊れたURLになる → URLの手前で切る (URLが先頭から始まる場合は直後で切る)。
 * @param {string} text 全文
 * @param {number} cut 元の切り位置 (文字数)
 * @returns {number} 補正後の切り位置
 */
export function urlSafeCut(text, cut) {
  for (const m of String(text).matchAll(LINK_RE)) {
    const start = m.index, end = m.index + m[0].length;
    if (start < cut && end > cut) return start > 0 ? start : end;
  }
  return cut;
}
