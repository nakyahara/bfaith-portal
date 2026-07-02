// カテゴリ lexical マッチ用のテキスト正規化 + 文字バイグラム類似度。
// 日本語は分かち書きが無いので、形態素解析に依存せず文字バイグラムで照合する。
// ローカル RYS (Downloads/RakutenYahooSync) Phase B 実装 (178 tests pass) の ESM 移植。

// NFKC + 小文字化 + 記号/区切りを空白へ。英数・ひらがな・カタカナ・漢字・長音は残す。
export function normalizeForMatch(s) {
  if (s === undefined || s === null) return '';
  let t = String(s).normalize('NFKC').toLowerCase();
  // 残す: 0-9 a-z / ひらがな・カタカナ (぀-ヿ) / CJK (㐀-鿿)。
  // ただし中黒 ・ は区切り扱い、長音 ー は語の一部なので残す。
  t = t.replace(/[^0-9a-z぀-ヺー-ヿ㐀-鿿]+/gu, ' ');
  return t.replace(/\s+/g, ' ').trim();
}

// 正規化済み文字列 → 文字バイグラム集合 (空白除去後、隣接2文字)。
// 2文字未満は、その文字自体を1要素として返す (短語の取りこぼし防止)。
export function charBigrams(normalized) {
  const c = normalized.replace(/\s+/g, '');
  if (c.length === 0) return new Set();
  if (c.length === 1) return new Set([c]);
  const out = new Set();
  for (let i = 0; i < c.length - 1; i++) out.add(c.slice(i, i + 2));
  return out;
}

// 2集合の Jaccard 係数 (0..1)。
export function jaccard(a, b) {
  if (a.size === 0 || b.size === 0) return 0;
  let inter = 0;
  const [small, large] = a.size <= b.size ? [a, b] : [b, a];
  for (const x of small) if (large.has(x)) inter += 1;
  return inter / (a.size + b.size - inter);
}

// 正規化済み needle が haystack に部分文字列として含まれるか (空白除去後)。
export function containsNormalized(haystackNorm, needleNorm) {
  const h = haystackNorm.replace(/\s+/g, '');
  const n = needleNorm.replace(/\s+/g, '');
  if (n.length === 0) return false;
  return h.includes(n);
}
