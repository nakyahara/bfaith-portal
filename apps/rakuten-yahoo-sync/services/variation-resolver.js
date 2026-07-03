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
// Yahoo editItem: オプション項目/値に 半角スペース と 半角記号 |;:&=#"\ は使用不可 (公式 spec)。
// 半角スペースは全角に置換して救済、 他の禁止文字は fail-closed。
const OPTION_FORBIDDEN = /[|;:&=#"\\]/;

export function normalizeVariants(rakutenItem) {
  if (!rakutenItem) return [];
  const v = rakutenItem.variants;
  if (!v) return [];
  if (Array.isArray(v)) return v;
  if (typeof v === 'object') return Object.values(v);
  return [];
}

/** 楽天 RMS 2.0 の variantSelectors を [{key, name}] に正規化 (単一 object も許容)。 */
export function normalizeSelectors(rakutenItem) {
  const s = rakutenItem?.variantSelectors;
  const list = Array.isArray(s) ? s : (s && typeof s === 'object' ? [s] : []);
  return list
    .map((sel) => ({ key: sel?.key != null ? String(sel.key) : null, name: sel?.displayName || sel?.key || null }))
    .filter((sel) => sel.key && sel.name);
}

function sanitizeOptionText(raw) {
  // 半角スペース → 全角スペース (Yahoo は半角スペース不可)
  return String(raw).trim().replace(/ /g, '　');
}

/**
 * 再設計 R11 (2026-07-03 中原さん要望「エラーにならない方法を。楽天から取得できるはず」):
 * Yahoo editItem のバリエーション 3 フィールドを楽天 RMS データだけから自動構築する。
 *   - options:  `軸名#選択肢1,選択肢2|軸名2#...` (公式 spec: 名前と値の区切り #、値間 ,、軸間 |)
 *   - subcodes: `軸名:選択肢=サブコード|...` (公式例: 色:ネイビー#サイズ:S=subcodetest-1|...)
 *     サブコード = merchantDefinedSkuId (楽天「システム連携用SKU番号」) そのまま
 *   - subcode_param は Phase では送らない (価格は item price を継承。 Notion 売価 SoT と矛盾させない)
 *
 * 旧 variation_spec_mapping (手動対応表) はこの自動構築で不要になった。
 *
 * fail-closed 条件 (errors に理由を積む):
 *   - 軸が取れない / 選択肢値が欠落 / 禁止文字 / サブコード不正
 *   - 多軸 (2軸以上) で組み合わせが不完全 (Yahoo は指定オプションの全組み合わせに
 *     サブコード必須のため、 楽天側が疎な組み合わせだと登録できない)
 *
 * @returns {{ ok: boolean, options: string|null, subcodes: string|null, errors: string[] }}
 */
export function buildVariationOptionFields(rakutenItem) {
  const errors = [];
  const selectors = normalizeSelectors(rakutenItem);
  if (selectors.length === 0) {
    return { ok: false, options: null, subcodes: null, errors: ['variation_axes_missing'] };
  }

  // 軸名 sanitize + 禁止文字チェック
  const axes = [];
  for (const sel of selectors) {
    const name = sanitizeOptionText(sel.name);
    if (OPTION_FORBIDDEN.test(name)) {
      errors.push(`variation_option_invalid_char:axis=${name}`);
      continue;
    }
    axes.push({ key: sel.key, name });
  }
  if (axes.length === 0) {
    return { ok: false, options: null, subcodes: null, errors: errors.length ? errors : ['variation_axes_missing'] };
  }

  // 各 variant の (軸 → 選択肢値) と subcode を抽出
  const variants = normalizeVariants(rakutenItem);
  const rows = []; // { subcode, values: { [axisKey]: sanitizedValue } }
  for (const v of variants) {
    const subcode = v?.merchantDefinedSkuId ? String(v.merchantDefinedSkuId).trim() : '';
    if (!subcode) { errors.push('variant_sku_id_empty'); continue; }
    if (!SUBCODE_REGEX.test(subcode)) { errors.push(`variant_sku_id_invalid_format: ${subcode}`); continue; }
    const values = {};
    let rowOk = true;
    for (const axis of axes) {
      const rawVal = v?.selectorValues?.[axis.key];
      if (rawVal == null || String(rawVal).trim() === '') {
        errors.push(`variation_option_value_missing:${subcode}/${axis.name}`);
        rowOk = false;
        break;
      }
      const val = sanitizeOptionText(rawVal);
      if (OPTION_FORBIDDEN.test(val)) {
        errors.push(`variation_option_invalid_char:${subcode}/${val}`);
        rowOk = false;
        break;
      }
      values[axis.key] = val;
    }
    if (rowOk) rows.push({ subcode, values });
  }
  if (rows.length === 0) {
    return { ok: false, options: null, subcodes: null, errors: errors.length ? errors : ['variation_axes_missing'] };
  }
  if (errors.length > 0) {
    // 一部 variant が欠けたまま出すと Yahoo 側で組み合わせ不足エラーになるため fail-closed
    return { ok: false, options: null, subcodes: null, errors };
  }

  // 軸ごとの選択肢一覧 (variantSelectors の定義順を優先し、 実際に使われている値のみ)
  const usedByAxis = new Map(); // axisKey → ordered unique values
  for (const axis of axes) {
    const declared = (Array.isArray(rakutenItem?.variantSelectors)
      ? rakutenItem.variantSelectors
      : [rakutenItem?.variantSelectors]).find((s) => s?.key === axis.key);
    const declaredOrder = (declared?.values || []).map((x) => sanitizeOptionText(x?.displayValue ?? '')).filter(Boolean);
    const used = new Set(rows.map((r) => r.values[axis.key]));
    const ordered = [...declaredOrder.filter((v) => used.has(v)), ...[...used].filter((v) => !declaredOrder.includes(v))];
    usedByAxis.set(axis.key, ordered);
  }

  // 組み合わせの重複検知 (Codex R11-R1 High: 重複があると件数比較だけでは歯抜けを見逃す。
  //   同一組み合わせが複数 SKU にあると Yahoo 側でも不正なので全軸数で fail-closed)
  const comboKeys = new Set();
  const dupCombos = new Set();
  for (const r of rows) {
    const key = axes.map((a) => r.values[a.key]).join('#');
    if (comboKeys.has(key)) dupCombos.add(key);
    comboKeys.add(key);
  }
  if (dupCombos.size > 0) {
    return {
      ok: false, options: null, subcodes: null,
      errors: [`variation_combo_duplicate:${[...dupCombos].join(',')}`],
    };
  }

  // 多軸: Yahoo は「指定オプションの全組み合わせにサブコード必須」→ 疎な組み合わせは登録不可
  //   (ユニーク組み合わせ数で判定)
  if (axes.length >= 2) {
    const comboCount = axes.reduce((acc, a) => acc * usedByAxis.get(a.key).length, 1);
    if (comboKeys.size !== comboCount) {
      return {
        ok: false, options: null, subcodes: null,
        errors: [`variation_combos_incomplete:${comboKeys.size}/${comboCount}`],
      };
    }
  }

  const options = axes.map((a) => `${a.name}#${usedByAxis.get(a.key).join(',')}`).join('|');
  const subcodes = rows
    .map((r) => axes.map((a) => `${a.name}:${r.values[a.key]}`).join('#') + '=' + r.subcode)
    .join('|');
  return { ok: true, options, subcodes, errors: [] };
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

  // R11: options + subcodes (Yahoo 正式書式の文字列) を楽天データから自動構築
  const optionFields = isVariation ? buildVariationOptionFields(rakutenItem) : null;

  return {
    isVariation,
    variantCount,
    condition,
    sendAucFields,
    subcodes,        // 後方互換: サブコード id の配列 (表示/検証用)
    optionFields,    // R11: { ok, options, subcodes(正式書式文字列), errors } — editItem 用
    conflict,
    subcodeErrors,
  };
}

export { SUBCODE_REGEX };
