/**
 * test-easyship.mjs — Amazon Easy Ship の料金表と、梱包サイズマスターの照会
 *
 * 🚨 料金表は **全セルを固定する**。写し間違えると全 FBM の送料が静かにずれるので、
 *    表を直すときは必ずこの試験も直る形にする (差分がレビューに出る)。
 *
 * 実行: node apps/expected-profit/test-easyship.mjs
 */
import assert from 'node:assert/strict';
import {
  EASYSHIP_RATES, EASYSHIP_REGIONS, EASYSHIP_DEFAULT_REGION, EASYSHIP_RATE_VERSION,
  normalizeEasyshipSize, easyshipFeeInclTax,
} from './easyship-rates.js';
import { fetchEasyshipSizes, easyshipBaseUrl } from './easyship-lookup.js';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

console.log('料金表 (関西発・税込。中原さんからもらった表そのまま)');

// 表の並び: 北海道 東北 関東 信越 北陸 中部 関西 中国 四国 九州 沖縄
const TABLE = {
  MAIL:     [185, 185, 185, 185, 185, 185, 185, 185, 185, 185, 185],
  SIZE_50:  [425, 425, 425, 425, 425, 425, 425, 425, 425, 425, 425],
  SIZE_60:  [779, 592, 430, 536, 536, 510, 430, 510, 510, 536, 913],
  SIZE_80:  [916, 668, 535, 588, 588, 588, 509, 588, 588, 588, 1044],
  SIZE_100: [1202, 935, 748, 748, 748, 748, 668, 748, 748, 748, 1470],
  SIZE_120: [1270, 1016, 728, 728, 728, 728, 647, 728, 728, 728, 1733],
  SIZE_140: [1617, 1294, 924, 924, 924, 924, 785, 924, 924, 924, 2079],
  SIZE_160: [1848, 1432, 1098, 1098, 1098, 1098, 924, 1098, 1098, 1098, 2426],
};

t('[!] 料金表の全セルが表のとおり', () => {
  assert.deepEqual(Object.keys(EASYSHIP_RATES), Object.keys(TABLE), 'サイズ区分の並びが違う');
  for (const [size, row] of Object.entries(TABLE)) {
    EASYSHIP_REGIONS.forEach((region, i) => {
      assert.equal(EASYSHIP_RATES[size].byRegion[region], row[i],
        `${size} の ${region} が違う (期待 ${row[i]})`);
    });
  }
});

t('地域列は表の並びのまま 11 列', () => {
  assert.deepEqual(EASYSHIP_REGIONS,
    ['北海道', '東北', '関東', '信越', '北陸', '中部', '関西', '中国', '四国', '九州', '沖縄']);
});

t('[!] メールサイズとサイズ50 は全国一律', () => {
  for (const size of ['MAIL', 'SIZE_50']) {
    const vals = new Set(Object.values(EASYSHIP_RATES[size].byRegion));
    assert.equal(vals.size, 1, `${size} が地域で違う`);
  }
});

t('[!] 標準シナリオの宛先は関東 (中原さん 2026-09-09)', () => {
  assert.equal(EASYSHIP_DEFAULT_REGION, '関東');
  assert.equal(easyshipFeeInclTax('SIZE_60').feeInclTax, 430);
  // 地域を変えれば金額も変わる (既定が効いているだけの試験にしない)
  assert.equal(easyshipFeeInclTax('SIZE_60', '沖縄').feeInclTax, 913);
});

t('料金表の版を持っている (変えたら行から追える)', () => {
  assert.ok(EASYSHIP_RATE_VERSION && EASYSHIP_RATE_VERSION.length > 3);
});

console.log('\nサイズ区分の読み取り (梱包サイズマスターは自由入力)');

t('[!] 書き方の揺れを吸収する', () => {
  for (const v of ['SIZE_60', 'size_60', 'size60', '60', 'サイズ60', '60サイズ']) {
    assert.equal(normalizeEasyshipSize(v), 'SIZE_60', `${v} が読めていない`);
  }
  // Easy Ship 画面の表示名そのまま (寸法つき)
  assert.equal(normalizeEasyshipSize('60サイズ (26 cm x 19 cm x 11 cm)'), 'SIZE_60');
  assert.equal(normalizeEasyshipSize('100サイズ (40 cm x 30 cm x 30 cm)'), 'SIZE_100');
});

t('[!] 寸法の数字をサイズと読み違えない', () => {
  // 「26 cm」を SIZE_26 と読んで落ちる、または近いサイズに寄せる、をしないこと
  assert.equal(normalizeEasyshipSize('60サイズ (26 cm x 19 cm x 11 cm)'), 'SIZE_60');
});

t('メールサイズを読める', () => {
  for (const v of ['MAIL', 'メールサイズ', 'メール便', 'メールサイズ (34 cm x 25 cm x 3.5 cm)']) {
    assert.equal(normalizeEasyshipSize(v), 'MAIL', `${v} が読めていない`);
  }
});

t('[!] 決められないものは近いサイズに寄せず null', () => {
  for (const v of ['', null, undefined, 'SIZE_70', '70', 'よくわからない区分', 'SIZE_XL']) {
    assert.equal(normalizeEasyshipSize(v), null, `${JSON.stringify(v)} を勝手に解決している`);
  }
  const r = easyshipFeeInclTax('SIZE_70');
  assert.equal(r.ok, false);
  assert.equal(r.reason, 'easyship_size_unmapped');
});

t('知らない地域は「分からない」にする (0 円にしない)', () => {
  const r = easyshipFeeInclTax('SIZE_60', '海外');
  assert.equal(r.ok, false);
  assert.equal(r.reason, 'easyship_region_unknown');
});

console.log('\n梱包サイズマスターの照会 (ポータルの ext-api)');

await ta('[!] found / inactive / notFound を 3 つの状態として持つ', async () => {
  const r = await fetchEasyshipSizes(['a', 'b', 'c'], {
    fetchBulk: async () => ({
      found: [{ sku: 'A', packageSizeCode: 'SIZE_60', packageSizeLabel: '60サイズ' }],
      inactive: ['b'],
      notFound: ['c'],
    }),
  });
  assert.equal(r.ok, true);
  assert.equal(r.map.get('a').status, 'easyship');
  assert.equal(r.map.get('a').sizeCode, 'SIZE_60');
  assert.equal(r.map.get('b').status, 'inactive');
  assert.equal(r.map.get('c').status, 'not_registered');
  assert.deepEqual(r.counts, { easyship: 1, inactive: 1, not_registered: 1 });
});

await ta('[!] 200 件ずつに分けて聞く (ext-api の上限)', async () => {
  const sizes = [];
  const skus = Array.from({ length: 450 }, (_, i) => `sku${i}`);
  await fetchEasyshipSizes(skus, {
    fetchBulk: async (part) => { sizes.push(part.length); return { found: [], inactive: [], notFound: part }; },
  });
  assert.deepEqual(sizes, [200, 200, 50]);
});

await ta('[!] 途中で失敗したら、取れた分だけで判定しない', async () => {
  let call = 0;
  const skus = Array.from({ length: 250 }, (_, i) => `sku${i}`);
  const r = await fetchEasyshipSizes(skus, {
    fetchBulk: async (part) => {
      call++;
      if (call === 2) throw new Error('HTTP 502');
      return { found: part.map((s) => ({ sku: s, packageSizeCode: 'SIZE_60' })), inactive: [], notFound: [] };
    },
  });
  assert.equal(r.ok, false, '一部でも失敗したら ok にしない');
  assert.equal(r.map.size, 0, '取れた分を残すと「登録が無い」と混ざる');
  assert.match(r.error, /502/);
});

await ta('SKU の大文字小文字を吸収する', async () => {
  const r = await fetchEasyshipSizes(['ABC-1'], {
    fetchBulk: async () => ({ found: [{ sku: 'abc-1', packageSizeCode: 'SIZE_80' }], inactive: [], notFound: [] }),
  });
  assert.equal(r.map.get('abc-1').sizeCode, 'SIZE_80');
});

await ta('聞く相手がいなければ何も聞かない (空の一覧)', async () => {
  let called = false;
  const r = await fetchEasyshipSizes([], { fetchBulk: async () => { called = true; return {}; } });
  assert.equal(called, false);
  assert.equal(r.ok, true);
});

t('[!] https 以外のポータルには聞きに行かない (トークンを平文で出さない)', () => {
  assert.equal(easyshipBaseUrl({ RENDER_MIRROR_URL: 'http://example.com/apps/mirror' }), '');
  assert.equal(easyshipBaseUrl({ RENDER_MIRROR_URL: 'https://example.com/apps/mirror' }), 'https://example.com');
  assert.equal(easyshipBaseUrl({}), '');
});

console.log(`\n${passed} 件 PASS`);
