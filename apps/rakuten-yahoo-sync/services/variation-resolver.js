/**
 * 楽天 RMS getItem の variants と Notion 「バリエーション有無」 から
 * Yahoo の (condition, auc 送信可否, subcodes) を決定する resolver。
 * ローカル RYS src/services/variation-resolver.js の ES module 翻訳 (Phase B2 R1-R2 完成版)。
 *
 * 設計原則 (Phase 0 §「商品タイプ分岐」 + Q12/Q34 + Codex B2 R1 M-1/M-2/L-1):
 *   - SoT: 楽天 variants.length >= 2 が真 (Notion はクロスチェック専用)
 *   - 通常品 (variants < 2): condition=2 / auc_* 送る / subcodes=null
 *   - バリ品 (variants >= 2): condition=1 / auc_* 送らない / subcodes=[merchantDefinedSkuId, ...]
 *     ⚠️ R10 (2026-07-03 中原さん指示): Yahoo バリエーションコード = 楽天「システム連携用SKU番号」
 *     (= merchantDefinedSkuId) **そのまま**。 システム連携用SKU番号は既に「商品番号+項目選択肢」
 *     形式なので、 旧実装の `${itemNumber}-${skuId}` は商品番号が二重になっていた
 *     (例: aromamist20-aromamist20-am)。
 *   - subcode は半角英数+ハイフンのみ (Yahoo 仕様、 Codex R5 確定)
 *   - subcode 重複検知 (M-2)
 *   - Notion 想定外値は unknown_notion_variation_value conflict として返す (L-1)
 */

const SUBCODE_REGEX = /^[A-Za-z0-9-]+$/;

export function normalizeVariants(rakutenItem) {
  if (!rakutenItem) return [];
  const v = rakutenItem.variants;
  if (!v) return [];
  if (Array.isArray(v)) return v;
  if (typeof v === 'object') return Object.values(v);
  return [];
}

export function resolveVariation({ rakutenItem, notionHasVariation }) {
  const variants = normalizeVariants(rakutenItem);
  const variantCount = variants.length;
  const isVariation = variantCount >= 2;
  const condition = isVariation ? 1 : 2;
  const sendAucFields = !isVariation;

  let subcodes = null;
  const subcodeErrors = [];
  if (isVariation) {
    // R10: subcode = merchantDefinedSkuId (楽天「システム連携用SKU番号」) そのまま。
    //   既に「商品番号+項目選択肢」形式のため itemNumber prefix は付けない (二重防止)。
    subcodes = [];
    for (const v of variants) {
      const skuId = v?.merchantDefinedSkuId ? String(v.merchantDefinedSkuId).trim() : '';
      if (!skuId) {
        subcodeErrors.push('variant_sku_id_empty');
        continue;
      }
      if (!SUBCODE_REGEX.test(skuId)) {
        subcodeErrors.push(`variant_sku_id_invalid_format: ${skuId}`);
        continue;
      }
      subcodes.push(skuId);
    }
    if (subcodes.length === 0) subcodes = null;
    // M-2: 重複検知
    if (subcodes && subcodes.length > 0) {
      const seen = new Set();
      const dups = new Set();
      for (const s of subcodes) {
        if (seen.has(s)) dups.add(s);
        seen.add(s);
      }
      if (dups.size > 0) {
        subcodeErrors.push(`variation_subcode_duplicate:${[...dups].join(',')}`);
      }
    }
  }

  // Notion クロスチェック
  let conflict = null;
  if (notionHasVariation === 'バリエーション登録あり' && !isVariation) {
    conflict = 'notion_says_variation_but_rakuten_is_single';
  } else if (notionHasVariation === 'バリエーションなし' && isVariation) {
    conflict = 'notion_says_single_but_rakuten_is_variation';
  } else if (
    notionHasVariation !== null && notionHasVariation !== undefined
    && notionHasVariation !== 'バリエーション登録あり' && notionHasVariation !== 'バリエーションなし'
  ) {
    // L-1: 想定外 Notion 値の可視化
    conflict = `unknown_notion_variation_value:${notionHasVariation}`;
  }

  return {
    isVariation,
    variantCount,
    condition,
    sendAucFields,
    subcodes,
    conflict,
    subcodeErrors,
  };
}

export { SUBCODE_REGEX };
