'use strict';

// 商品形態と製造実績は別。これは工程候補の分類であり、AMCの製造能力の証明ではない。
const FORMS = [
  { key: 'sheet', label: 'シート裁断', amc: true, re: /シート|テープ|シール|フィルム|不織布|ワイパー|クロス|パッド(?!ル)|台紙|ステッカー|障子紙|網戸|晒し|さらし|蒸し布|手(?:拭|ぬぐ|拭ぐ)い/ },
  { key: 'liquid', label: '液体・クリーナー充填', amc: true, re: /液体|スプレー|ミスト|クリーナー|洗剤|ローション|ジェル|原液|溶液|除光|コーティング剤|補修液|消臭剤|除菌剤|シャンプー|リンス/ },
  { key: 'oil', label: 'オイル充填', amc: true, re: /オイル|油|ワックス|グリス|潤滑|アロマ|精油/ },
  { key: 'powder', label: '粉末充填', amc: true, re: /粉末|パウダー|顆粒|粒剤|重曹|クエン酸|砂(?!糖)/ },
  { key: 'simple', label: '紙・木・金属の単純加工', amc: true, re: /木材|木片|竹材|紙製|段ボール|ダンボール/ },
];
const FORM_OTHER = { key: 'other', label: 'その他 (要判定)', amc: null, reason: '商品名から工程を確定できない' };
function detectForm(p) {
  const title = String(p.title || '').normalize('NFKC');
  const category = String(p.categoryPath || '').normalize('NFKC');
  if (/第[123]類医薬品|指定第2類医薬品|要指導医薬品/.test(title) || /(?:^| > )医薬品(?:・| > |$)/.test(category)) {
    return { key: 'medicine', label: '医薬品 (対象外)', amc: false, reason: '医薬品を企画対象から除外' };
  }
  if (/グミ|カプセル|錠剤|ソフトジェル|タブレット|[0-9]+粒/.test(title)) {
    return { key: 'formed', label: '成形・カプセル等 (要確認)', amc: null, reason: '充填だけでは形態を説明できない' };
  }
  if (/ブラシ|ホルダー|フック|ケース|スタンド|トレー|ドリル|くし|櫛|ディフューザー|電動|電池|充電|USB|モーター|LED|センサー/.test(title)) {
    return { key: 'assembled', label: '成形・組立等 (要確認)', amc: null, reason: '成形・組立・植毛など必要工程を確認する' };
  }
  // 上位カテゴリに「オイル」「サプリメント」があっても現物の形態には使わない。
  for (const f of FORMS) if (f.re.test(title)) return f;
  return FORM_OTHER;
}
function isSmall(p, tier) {
  const dims = p.packageMm;
  if (!Array.isArray(dims) || dims.length !== 3 || !dims.every(v => Number.isFinite(v) && v > 0)
      || !Number.isFinite(p.packageWeightG) || p.packageWeightG <= 0) return null;
  const sorted = [...dims].sort((a,b) => b-a);
  return sorted[0] <= tier.l && sorted[1] <= tier.w && sorted[2] <= tier.h && p.packageWeightG <= tier.weightG;
}
function parseProducts(text) {
  const rows = new Map();
  for (const [i, line] of text.split(/\r?\n/).entries()) {
    if (!line.trim()) continue;
    let row;
    try { row = JSON.parse(line); } catch { throw new Error(`products.jsonl ${i + 1}行目のJSONが壊れています`); }
    if (!row || typeof row.asin !== 'string' || !row.asin) throw new Error(`products.jsonl ${i + 1}行目のASINがありません`);
    const previous = rows.get(row.asin);
    const previousTime = Date.parse(previous?.observedAt);
    const time = Date.parse(row.observedAt);
    if (!previous || !Number.isFinite(previousTime) || (Number.isFinite(time) && time >= previousTime)) rows.set(row.asin, row);
  }
  return [...rows.values()];
}
function evidence(items, now = Date.now()) {
  const timestamps = items.map(p => Date.parse(p.observedAt));
  const valid = timestamps.filter(t => Number.isFinite(t) && t <= now + 300000);
  const unknown = timestamps.length - valid.length;
  const stale = valid.filter(t => now - t > 30 * 86400000).length;
  return {
    observedFrom: valid.length ? new Date(Math.min(...valid)).toISOString() : null,
    observedTo: valid.length ? new Date(Math.max(...valid)).toISOString() : null,
    unknownObservationCount: unknown, staleObservationCount: stale,
    freshness: unknown || stale ? 'unknown' : 'pass',
    parentUnknownCount: items.filter(p => !p.parentAsin).length,
  };
}
function purchaseSignal(items) {
  const groups = new Map();
  for (const p of items) {
    const key = p.parentAsin || p.asin;
    groups.set(key, Math.max(groups.get(key) || 0, Number.isFinite(p.monthlySold) && p.monthlySold >= 0 ? p.monthlySold : 0));
  }
  return [...groups.values()].reduce((a,b) => a+b, 0);
}
function refreshTargets(products, targets, limit, now = Date.now()) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) throw new Error('refresh-limit は1〜1000件で指定してください');
  const allowed = new Set(targets);
  return products.filter(p => allowed.has(p.asin) && evidence([p], now).freshness !== 'pass')
    .sort((a,b) => (Date.parse(a.observedAt) || 0) - (Date.parse(b.observedAt) || 0))
    .slice(0, limit).map(p => p.asin);
}
module.exports = { FORMS, FORM_OTHER, detectForm, isSmall, parseProducts, evidence, refreshTargets, purchaseSignal };
