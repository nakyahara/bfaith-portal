/**
 * 一括リサーチの概算保管コスト (1個あたり・月額の簡易推定)
 *
 * 受け取るのは miniPC の /service-api/research/product/:asin の dimensions
 * (sp-api.js の normalizePackageDimensions が作る形):
 *   { length: "30.5", width: "20.0", height: "10.2", weight: "0.45" }  ← cm / kg の文字列、分からない値は '-'
 *
 * ⚠ 以前は lengthCm / widthCm / heightCm という無いキーを読んでいて、いつも 0 → 「不明」の 50 円になっていた
 */

/** 3 辺合計 (cm) の区切りと金額 (円)。区切りは「以下」 */
export const STORAGE_TIERS = [
  { maxSumCm: 60, cost: 30, label: '小型' },
  { maxSumCm: 100, cost: 80, label: '標準' },
  { maxSumCm: Infinity, cost: 250, label: '大型' },
];
/** 寸法が分からないときの金額 (円) */
export const STORAGE_UNKNOWN_COST = 50;

export function estimateStorageCost(dimensions) {
  if (!dimensions) return STORAGE_UNKNOWN_COST;
  const side = (v) => {
    const n = parseFloat(v);
    return Number.isFinite(n) && n > 0 ? n : 0;
  };
  const l = side(dimensions.length);
  const w = side(dimensions.width);
  const h = side(dimensions.height);
  // 3 辺のどれかが分からないと大きさを決められないので「不明」扱い
  if (!l || !w || !h) return STORAGE_UNKNOWN_COST;
  const total = l + w + h;
  return STORAGE_TIERS.find((t) => total <= t.maxSumCm).cost;
}
