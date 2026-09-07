function invalid(message) { throw Object.assign(new Error(message), { status: 400 }); }
function date(value, name) {
  if (typeof value !== 'string' || !/(?:Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))
      || Date.parse(value) > Date.now() + 300000) invalid(`${name} はタイムゾーン付きの正しい日時が必要です`);
}
function objects(rows, name) {
  if (!Array.isArray(rows) || rows.some(row => !row || typeof row !== 'object' || Array.isArray(row))) invalid(`${name} の各要素はオブジェクトが必要です`);
}
function numbers(row, keys) {
  for (const key of keys) if (row[key] != null && (!Number.isFinite(row[key]) || row[key] < 0)) invalid(`${key} が不正です`);
}
export function validateSnapshot(payload) {
  if (!payload) invalid('データがありません');
  date(payload.generatedAt, 'generatedAt');
  if (payload.lastProgressAt != null) date(payload.lastProgressAt, 'lastProgressAt');
  objects(payload.concepts, 'concepts');
  if (payload.concepts.length > 20000) invalid('テーマが多すぎます');
  if (payload.collection != null) objects(payload.collection, 'collection');
  const seen = new Set();
  for (const c of payload.concepts) {
    if (![c.concept, c.categoryPath, c.form].every(x => typeof x === 'string' && x.trim())) invalid('concept、categoryPath、form が必要です');
    const key = JSON.stringify([c.categoryPath, c.form]);
    if (seen.has(key)) invalid('同じテーマが重複しています');
    seen.add(key);
    if (!['pass', 'unknown', 'fail'].includes(c.hardGate)) invalid('hardGate が不正です');
    if (c.examples != null) objects(c.examples, 'examples');
    numbers(c, ['productCount', 'totalMonthlySold', 'medianPrice', 'medianReferralFeePct', 'unknownSizeCount']);
    if ((payload.algorithmVersion || 1) >= 2 && c.hardGate === 'pass') {
      if (c.gates?.amc !== 'pass' || c.gates?.size !== 'pass' || c.gates?.freshness !== 'pass'
          || !c.examples?.length || !c.quality || c.quality.unknownObservationCount !== 0
          || c.quality.staleObservationCount !== 0) invalid('通過テーマの形態・寸法・鮮度の根拠が不足しています');
      date(c.quality.observedFrom, 'observedFrom');
      if (Date.now() - Date.parse(c.quality.observedFrom) > 30 * 86400000) invalid('通過テーマの商品観測が古すぎます');
    }
  }
  const categories = new Set();
  for (const c of payload.collection || []) {
    if (c.rootCategory == null || categories.has(String(c.rootCategory))) invalid('カテゴリが不正または重複しています');
    categories.add(String(c.rootCategory));
    numbers(c, ['asinTarget', 'fetched', 'remaining', 'estimatedMissing']);
  }
}
export function validateOwnPayload(payload) {
  if (!payload) invalid('データがありません');
  objects(payload.families, 'families');
  if (payload.families.length > 20000) invalid('ファミリーが多すぎます');
  if (payload.generatedAt != null) date(payload.generatedAt, 'generatedAt');
  if (payload.sourceGeneratedAt != null) date(payload.sourceGeneratedAt, 'sourceGeneratedAt');
  if (payload.sourceUpdatedAt != null) {
    date(payload.sourceUpdatedAt, 'sourceUpdatedAt');
    if (Date.parse(payload.sourceUpdatedAt) > Date.parse(payload.generatedAt)) invalid('商品DBの更新日時が生成日時より未来です');
  }
  if ((payload.algorithmVersion || 1) >= 2) {
    date(payload.generatedAt, 'generatedAt');
    date(payload.sourceGeneratedAt, 'sourceGeneratedAt');
    if (Date.parse(payload.sourceGeneratedAt) > Date.parse(payload.generatedAt)) invalid('元データの日時が生成日時より未来です');
  }
  const seen = new Set();
  for (const f of payload.families) {
    if (typeof f.familyKey !== 'string' || !f.familyKey.trim() || seen.has(f.familyKey)) invalid('familyKey が不正または重複しています');
    seen.add(f.familyKey);
    if (!['active', 'withdrawn', 'shrinking'].includes(f.outcome)) invalid('outcome が不正です');
    if ((payload.algorithmVersion || 1) >= 2 && ![1, 2].includes(f.salesClass)) invalid('売上分類1または2が必要です');
    numbers(f, ['skuCount', 'asinCount', 'qty180', 'qtyAll', 'activeSkus', 'discontinuedSkus']);
    if (f.products != null) objects(f.products, 'products');
  }
}
