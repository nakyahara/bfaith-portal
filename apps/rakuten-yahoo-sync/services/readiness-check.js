/**
 * readiness-check: publish 試行前に display=1 昇格して良いかを 11 項目で判定。
 * ローカル RYS src/services/readiness-check.js の ES module 翻訳 (Phase B2 R1-R2 完成版)。
 *
 * 設計原則:
 *   - 1 つでも fail なら display=0 維持 + reasons 配列を返す
 *   - pure function: DB / 楽天 API には触れない、 caller (publish-pipeline) が事前解決して渡す
 *   - 結果の DB persist は persistJobReadiness() で別関数 (任意呼び出し)
 *
 * 11 項目 (通常品 10 + バリ品 +1):
 *   name / product_category / path / price / postage_set / delivery / taxrate_type /
 *   image preflight / variation 整合 / lead_time preflight / variation_spec (バリ品のみ)
 */

const TAX_RATE_MAP = {
  '10': 0.1,
  '10%': 0.1,
  '8': 0.08,
};

const NAME_MAX_LENGTH = 65;

export function evaluateTaxRate(notionTaxRate, rakutenTaxRate) {
  if (notionTaxRate === null || notionTaxRate === undefined) {
    return { ok: false, reason: 'notion_tax_rate_missing' };
  }
  if (notionTaxRate === '#REF!') {
    return { ok: false, reason: 'notion_tax_rate_ref_error' };
  }
  const notionNumeric = TAX_RATE_MAP[notionTaxRate];
  if (notionNumeric === undefined) {
    return { ok: false, reason: `notion_tax_rate_unknown_value:${notionTaxRate}` };
  }
  if (rakutenTaxRate === null || rakutenTaxRate === undefined) {
    return { ok: false, reason: 'rakuten_tax_rate_missing' };
  }
  const r = Number(rakutenTaxRate);
  if (!Number.isFinite(r)) {
    return { ok: false, reason: `rakuten_tax_rate_invalid:${rakutenTaxRate}` };
  }
  if (Math.abs(notionNumeric - r) > 1e-6) {
    return { ok: false, reason: `tax_rate_mismatch:notion=${notionNumeric},rakuten=${r}` };
  }
  return { ok: true, value: notionNumeric };
}

/**
 * variation_spec_mapping が axis (with category) を解決できるか。
 *   Codex B2 R1 M-1: yahoo_product_category 込みで combo lookup (別カテゴリ流用 防止)
 */
export function evaluateVariationSpec(db, requiredAxes, yahooProductCategory) {
  if (!requiredAxes || requiredAxes.length === 0) return { ok: true, missing: [] };
  if (yahooProductCategory === null || yahooProductCategory === undefined) {
    return { ok: false, missing: requiredAxes.slice(), categoryUnknown: true };
  }
  // Codex E-3 R1 M-1: sqlite_master で明示確認 (catch-all は操作エラーを mask するため避ける)
  const hasTable = !!db.prepare(
    `SELECT 1 FROM sqlite_master WHERE type='table' AND name='variation_spec_mapping'`
  ).get();
  if (!hasTable) return { ok: false, missing: requiredAxes.slice(), tableMissing: true };
  const stmt = db.prepare(
    `SELECT 1 FROM variation_spec_mapping WHERE axis_label = ? AND yahoo_product_category = ? LIMIT 1`
  );
  const missing = [];
  for (const axis of requiredAxes) {
    const hit = stmt.get(axis, yahooProductCategory);
    if (!hit) missing.push(axis);
  }
  return { ok: missing.length === 0, missing };
}

export function extractRequiredAxes(rakutenItem) {
  if (!rakutenItem) return [];
  const variants = rakutenItem.variants;
  const list = Array.isArray(variants)
    ? variants
    : (variants && typeof variants === 'object' ? Object.values(variants) : []);
  const axes = new Set();
  for (const v of list) {
    if (!v) continue;
    if (Array.isArray(v.axes)) {
      for (const a of v.axes) if (a?.name) axes.add(String(a.name));
    } else {
      for (const key of ['axis1Name', 'axis2Name', 'axis3Name']) {
        if (v[key]) axes.add(String(v[key]));
      }
    }
  }
  return [...axes];
}

/**
 * readiness-check のエントリポイント (pure function)。
 */
export function evaluateReadiness(input) {
  const reasons = [];

  // #1 name (Notion yahoo_title 強制、 65 字以下)
  const yt = input.notionOverride?.yahoo_title;
  if (!yt) {
    reasons.push('notion_title_missing');
  } else if (yt.length > NAME_MAX_LENGTH) {
    reasons.push(`notion_title_too_long:${yt.length}>${NAME_MAX_LENGTH}`);
  }

  // #2 product_category
  if (!input.productCategory) reasons.push('product_category_unresolved');
  // #3 path
  if (!input.path) reasons.push('path_unresolved');

  // #4 price
  if (input.resolvedPrice === null || input.resolvedPrice === undefined || !(input.resolvedPrice > 0)) {
    reasons.push('price_invalid_or_zero');
  }

  // #5/#6 delivery / postage_set
  if (!input.deliveryRow) {
    reasons.push('delivery_mapping_unresolved');
  } else {
    const d = input.deliveryRow.yahoo_delivery;
    if (![0, 1, 2, 3].includes(d)) reasons.push(`delivery_value_invalid:${d}`);
    const ps = input.deliveryRow.yahoo_postage_set;
    if (!(ps >= 1 && ps <= 20)) reasons.push(`postage_set_out_of_range:${ps}`);
  }

  // #7 taxrate_type
  const taxRes = evaluateTaxRate(input.notionOverride?.notion_tax_rate, input.rakutenTaxRate);
  if (!taxRes.ok) reasons.push(taxRes.reason);

  // #8 画像 preflight
  if (!input.imagePreflight) {
    reasons.push('image_preflight_not_run');
  } else if (!input.imagePreflight.ok) {
    reasons.push(`image_upload_failed:${input.imagePreflight.error || 'unknown'}`);
  }

  // #9 variation 整合性
  const vr = input.variationResult;
  if (!vr) {
    reasons.push('variation_result_missing');
  } else {
    if (vr.conflict) reasons.push(`variation_conflict:${vr.conflict}`);
    if (vr.subcodeErrors && vr.subcodeErrors.length > 0) {
      reasons.push(`variation_subcode_errors:${vr.subcodeErrors.join('|')}`);
    }
  }

  // #10 lead_time preflight
  if (!input.leadTimePreflight) {
    reasons.push('lead_time_preflight_not_run');
  } else if (!input.leadTimePreflight.ok) {
    reasons.push(`lead_time_invalid:${input.leadTimePreflight.error || 'unknown'}`);
  }

  // #11 variation_spec_mapping (バリ品のみ)
  if (vr?.isVariation) {
    if (!input.db) {
      reasons.push('variation_spec_db_missing');
    } else {
      const axes = extractRequiredAxes(input.rakutenItem);
      // Codex E-3 R1 H-1: バリ品なのに axes が空 (RMS shape 違い or rakutenItem 欠落) は fail-closed
      if (axes.length === 0) {
        reasons.push('variation_axes_missing');
      } else {
        const specRes = evaluateVariationSpec(input.db, axes, input.yahooProductCategoryId);
        if (specRes.categoryUnknown) {
          reasons.push('variation_spec_category_unknown');
        } else if (specRes.tableMissing) {
          reasons.push('variation_spec_table_missing');
        } else if (!specRes.ok) {
          reasons.push(`variation_spec_unresolved:${specRes.missing.join(',')}`);
        }
      }
    }
  }

  return {
    status: reasons.length === 0 ? 'ok' : 'blocked',
    reasons,
  };
}

/**
 * jobs 行に readiness 結果を persist (cross-column CHECK 制約遵守)。
 */
export function persistJobReadiness(db, itemCode, result) {
  if (!itemCode) throw new Error('persistJobReadiness: itemCode required');
  if (!result || !['pending', 'ok', 'blocked'].includes(result.status)) {
    throw new Error(`persistJobReadiness: invalid status "${result?.status}"`);
  }
  // Codex E-3 R1 L-1: jobs CHECK 制約 (blocked → reasons 配列 1 個以上必須) を perimeter で保証
  if (result.status === 'blocked' && (!Array.isArray(result.reasons) || result.reasons.length === 0)) {
    throw new Error('persistJobReadiness: blocked status requires at least one reason');
  }
  const reasonsJson = result.status === 'blocked' ? JSON.stringify(result.reasons) : null;
  db.prepare(`
    UPDATE jobs
       SET readiness_status         = ?,
           readiness_blocked_reasons = ?,
           last_readiness_at         = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
     WHERE item_code = ?
  `).run(result.status, reasonsJson, itemCode);
}

export { TAX_RATE_MAP, NAME_MAX_LENGTH };
