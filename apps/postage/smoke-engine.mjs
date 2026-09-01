/**
 * 判定エンジンの単体テスト (DB 不要)。
 *   node apps/postage/smoke-engine.mjs
 *
 * 料金の境界は 1g・1mm ずれると別料金になる。ここが緩いと、紙に間違った金額が出る。
 */
import { judge, LIMITS } from './engine.js';

let pass = 0, fail = 0;
function t(name, fn) {
  try { fn(); pass++; console.log(`  ok   ${name}`); }
  catch (e) { fail++; console.log(`  FAIL ${name}\n       ${e.message}`); }
}
function eq(actual, expected, what) {
  const a = JSON.stringify(actual), b = JSON.stringify(expected);
  if (a !== b) throw new Error(`${what || ''} expected ${b}, got ${a}`);
}

const BANDS = [
  { mail_type: 'teikei',    band_code: 'teikei_50',      display_name: '定形 50g以内',          max_weight_g: 50,   amount_yen: 110 },
  { mail_type: 'kikakunai', band_code: 'kikakunai_50',   display_name: '規格内 50g以内',        max_weight_g: 50,   amount_yen: 140 },
  { mail_type: 'kikakunai', band_code: 'kikakunai_100',  display_name: '規格内 100g以内',       max_weight_g: 100,  amount_yen: 180 },
  { mail_type: 'kikakunai', band_code: 'kikakunai_1000', display_name: '規格内 1kg以内',        max_weight_g: 1000, amount_yen: 750 },
  { mail_type: 'kikakugai', band_code: 'kikakugai_50',   display_name: '規格外 50g以内',        max_weight_g: 50,   amount_yen: 260 },
  { mail_type: 'kikakugai', band_code: 'kikakugai_100',  display_name: '規格外 100g以内',       max_weight_g: 100,  amount_yen: 290 },
  { mail_type: 'kikakugai', band_code: 'kikakugai_4000', display_name: '規格外 4kg以内',        max_weight_g: 4000, amount_yen: 1750 },
];

const MATERIALS = new Map([
  // 長形3号 = 定形サイズそのもの
  ['chabuto',    { display_name: '茶封筒', tare_weight_g: 5,  outer_length_mm: 235, outer_width_mm: 120 }],
  // 外寸未測定 (実運用の初期状態)
  ['shiropuchi', { display_name: '白プチ', tare_weight_g: 10, outer_length_mm: null, outer_width_mm: null }],
  // 規格内に収まる大きさ
  ['shirobi',    { display_name: '白ビ袋', tare_weight_g: 11, outer_length_mm: 320, outer_width_mm: 240 }],
  // 規格外になる大きさ
  ['ookibako',   { display_name: '大箱',   tare_weight_g: 50, outer_length_mm: 450, outer_width_mm: 300 }],
]);

function ctx(over = {}) {
  return {
    skus: over.skus, materials: MATERIALS, bands: BANDS,
    overheadG: 0.5, boundaryMarginG: 5, thicknessMarginMm: 1, ...over,
  };
}
const sku = (o) => new Map(Object.entries(o));
const ship = (...lines) => ({ lines: lines.map(([sku_code, qty]) => ({ sku_code, qty })) });

console.log('■ 確定するケース');

t('茶封筒・薄い・軽い → 定形110円', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 15, thickness_mm: 1, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'confirmed'); eq(r.mailType, 'teikei'); eq(r.amountYen, 110);
  eq(r.weightG, 20.5, '15 + 5 + 0.5');
});

t('茶封筒・厚い(20mm) → 規格内140円', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 15, thickness_mm: 20, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'confirmed'); eq(r.mailType, 'kikakunai'); eq(r.amountYen, 140);
});

t('茶封筒・薄いが 50g超 → 定形にならず規格内180円', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 60, thickness_mm: 1, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'confirmed'); eq(r.mailType, 'kikakunai'); eq(r.amountYen, 180);
});

t('数量複数 → 重さも厚みも数量ぶん積む', () => {
  const r = judge(ship(['a', 3]), ctx({ skus: sku({ a: { unit_weight_g: 10, thickness_mm: 5, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'confirmed'); eq(r.weightG, 35.5, '10*3 + 5 + 0.5');
  eq(r.thicknessMm, 15); eq(r.mailType, 'kikakunai', '15mm は定形10mmを超える');
});

t('大きい資材 → 規格外', () => {
  // 20 + 50 + 0.5 = 70.5g → 規格外 100g帯 = 290円
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 20, thickness_mm: 5, default_material_code: 'ookibako' } }) }));
  eq(r.status, 'confirmed'); eq(r.mailType, 'kikakugai'); eq(r.amountYen, 290); eq(r.weightG, 70.5);
});

t('該当帯が無く上の帯しかないときは上の帯を使う (安い側に丸めない)', () => {
  const sparse = BANDS.filter((b) => b.band_code !== 'kikakugai_100');
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 20, thickness_mm: 5, default_material_code: 'ookibako' } }), bands: sparse }));
  eq(r.status, 'confirmed'); eq(r.amountYen, 1750, '50g帯には入らないので次の帯');
});

console.log('\n■ 境界 — 確定させてはいけないケース');

t('重量が境界5g以内 → near_weight_boundary', () => {
  // 40 + 5 + 0.5 = 45.5g。50g まで 4.5g しかない
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 40, thickness_mm: 20, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'near_weight_boundary');
});

t('境界まで 5g ちょうど → まだ不明 (以内は含む)', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 39.5, thickness_mm: 20, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'near_weight_boundary');
});

t('境界まで 5.1g → 確定してよい', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 39.4, thickness_mm: 20, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'confirmed'); eq(r.weightG, 44.9);
});

t('厚みが定形の上限10mmぎりぎり → near_thickness_boundary', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 10, thickness_mm: 9.5, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'near_thickness_boundary');
});

t('厚みが10mmをわずかに超える → 規格内と言い切らず不明', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 10, thickness_mm: 10.5, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'near_thickness_boundary');
});

t('数量複数のときは厚みの安全幅が倍 (1mm→2mm)', () => {
  // 単品なら 8mm は 10mm から 2mm 離れていて確定できるが、2個 = 合計 8mm でも重なり方が読めない
  const single = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 5, thickness_mm: 8, default_material_code: 'chabuto' } }) }));
  eq(single.status, 'confirmed'); eq(single.mailType, 'teikei');
  const dbl = judge(ship(['a', 2]), ctx({ skus: sku({ a: { unit_weight_g: 5, thickness_mm: 4, default_material_code: 'chabuto' } }) }));
  eq(dbl.status, 'unknown'); eq(dbl.reason, 'near_thickness_boundary', '合計8mm・上限10mm・安全幅2mm');
});

console.log('\n■ マスタ不足 — 埋めれば直るケース');

t('SKUがマスタに無い → missing_sku', () => {
  const r = judge(ship(['zzz', 1]), ctx({ skus: sku({}) }));
  eq(r.status, 'unknown'); eq(r.reason, 'missing_sku');
});

t('重さ未登録 → missing_weight', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { thickness_mm: 1, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'missing_weight');
});

t('厚み未登録 → missing_thickness', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 15, default_material_code: 'chabuto' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'missing_thickness');
});

t('資材の外寸が未測定 → missing_dims (重量は出せていても確定しない)', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 30, thickness_mm: 5, default_material_code: 'shiropuchi' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'missing_dims');
  eq(r.weightG, 40.5, '重量そのものは計算できている');
});

t('資材が決まらない → missing_material', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 15, thickness_mm: 1 } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'missing_material');
});

t('明細で資材が食い違う → material_conflict', () => {
  const r = judge(ship(['a', 1], ['b', 1]), ctx({
    skus: sku({
      a: { unit_weight_g: 5, thickness_mm: 1, default_material_code: 'chabuto' },
      b: { unit_weight_g: 5, thickness_mm: 1, default_material_code: 'shirobi' },
    }),
  }));
  eq(r.status, 'unknown'); eq(r.reason, 'material_conflict');
});

console.log('\n■ 郵便で出せないケース');

t('4kg超 → over_maximum (最大料金に丸めない)', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 5000, thickness_mm: 5, default_material_code: 'ookibako' } }) }));
  eq(r.status, 'unknown'); eq(r.reason, 'over_maximum');
});

t('3辺合計が90cm超 → over_maximum', () => {
  const mats = new Map(MATERIALS);
  mats.set('nagai', { display_name: '長物', tare_weight_g: 100, outer_length_mm: 590, outer_width_mm: 300 });
  const r = judge(ship(['a', 1]), ctx({
    skus: sku({ a: { unit_weight_g: 100, thickness_mm: 100, default_material_code: 'nagai' } }), materials: mats,
  }));
  eq(r.status, 'unknown'); eq(r.reason, 'over_maximum', '590+300+100=990 > 900');
});

console.log('\n■ 入力の頑健さ');

t('明細ゼロ → no_lines', () => { eq(judge({ lines: [] }, ctx({ skus: sku({}) })).reason, 'no_lines'); });
t('数量0 → no_lines', () => { eq(judge(ship(['a', 0]), ctx({ skus: sku({ a: { unit_weight_g: 1 } }) })).reason, 'no_lines'); });
t('外寸の長短を逆に入れても同じ結果', () => {
  const mats = new Map(MATERIALS);
  mats.set('rev', { display_name: '逆', tare_weight_g: 5, outer_length_mm: 120, outer_width_mm: 235 });
  const r = judge(ship(['a', 1]), ctx({
    skus: sku({ a: { unit_weight_g: 15, thickness_mm: 1, default_material_code: 'rev' } }), materials: mats,
  }));
  eq(r.status, 'confirmed'); eq(r.mailType, 'teikei');
});
t('料金表が無い → no_tariff', () => {
  const r = judge(ship(['a', 1]), ctx({ skus: sku({ a: { unit_weight_g: 15, thickness_mm: 1, default_material_code: 'chabuto' } }), bands: [] }));
  eq(r.status, 'unknown'); eq(r.reason, 'no_tariff');
});

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed, ${fail} failed`);
process.exitCode = fail === 0 ? 0 : 1;
