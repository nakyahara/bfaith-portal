/**
 * Amazon ASIN の形式検証と正規化 (共通)。2026-09-23 SP広告KW PR2-C で新設。
 * 2026-09-23 完全一致の判定 (/^[A-Z0-9]{10}$/) は apps/ の 5 ファイルをこの ASIN_RE に寄せた。
 * 🚨 寄せていないもの = URL から抜き出す正規表現 (パスの形がそれぞれ違う・大文字小文字の扱いも違う)・
 *    CommonJS の scripts/product-idea-scout/ai/*.cjs・tools/aba-chrome-extension (単体で読み込まれる)。
 *    ASIN_RE に g / y フラグを付けないこと (.test() が lastIndex を持ち、呼び手の判定が交互に変わる)
 */
export const ASIN_RE = /^[A-Z0-9]{10}$/;

/** 1 つの ASIN を正規化 (前後の空白を落として大文字に)。形式が違えば null */
export function normalizeAsin(raw) {
  const s = String(raw ?? '').trim().toUpperCase();
  return ASIN_RE.test(s) ? s : null;
}

/**
 * 人が貼った文字列 (カンマ・空白・改行区切り、URL 混じりも可) から ASIN を取り出す。
 * @returns {{asins: string[], invalid: string[]}} asins = 正規化・重複除去済み (出た順) / invalid = 形式が違った断片
 */
export function parseAsinList(raw, { max = 50 } = {}) {
  const asins = [], invalid = [], seen = new Set();
  for (const piece of String(raw ?? '').split(/[\s,、;]+/)) {
    if (!piece) continue;
    // Amazon の URL なら /dp/XXXXXXXXXX か /gp/product/XXXXXXXXXX を拾う
    const m = piece.match(/\/(?:dp|gp\/product)\/([A-Za-z0-9]{10})(?:[/?#]|$)/);
    const asin = normalizeAsin(m ? m[1] : piece);
    if (!asin) { invalid.push(piece); continue; }
    if (seen.has(asin)) continue;
    seen.add(asin);
    if (asins.length < max) asins.push(asin);
  }
  return { asins, invalid };
}
