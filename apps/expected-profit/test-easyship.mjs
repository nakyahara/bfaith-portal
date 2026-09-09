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
  normalizeEasyshipSize, easyshipFeeInclTax, resolveEasyshipSize,
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

console.log('');
console.log('コードと表示名のどちらを見るか (Codex P2)');

t('[!] 片方しか読めなくても、読める方で計算する', () => {
  // 🚨 コードは人が付ける自由入力なので古い書き方が残りうる。表示名だけのこともある。
  //    片方しか見ないと、ちゃんと登録してある出品を「サイズが読めない」で落とす
  assert.equal(resolveEasyshipSize({ sizeCode: 'LEGACY-A', sizeLabel: '60サイズ (26 cm x 19 cm x 11 cm)' }), 'SIZE_60');
  assert.equal(resolveEasyshipSize({ sizeCode: '', sizeLabel: '80サイズ' }), 'SIZE_80');
  assert.equal(resolveEasyshipSize({ sizeCode: 'SIZE_100', sizeLabel: null }), 'SIZE_100');
  assert.equal(resolveEasyshipSize({ sizeCode: 'SIZE_140', sizeLabel: '独自の名前' }), 'SIZE_140');
});

t('[!] コードと表示名が食い違ったら決めない (片方を勝たせない)', () => {
  // どちらが正しいか分からないまま採ると、違うサイズの送料で利益を出すことになる
  assert.equal(resolveEasyshipSize({ sizeCode: 'SIZE_60', sizeLabel: '80サイズ' }), null);
  // 同じサイズを指していれば問題ない
  assert.equal(resolveEasyshipSize({ sizeCode: 'SIZE_60', sizeLabel: '60サイズ (26 cm x 19 cm x 11 cm)' }), 'SIZE_60');
});

t('どちらも読めなければ null', () => {
  assert.equal(resolveEasyshipSize({ sizeCode: 'LEGACY-A', sizeLabel: '特大' }), null);
  assert.equal(resolveEasyshipSize({}), null);
  assert.equal(resolveEasyshipSize(), null);
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

console.log('');
console.log('夜間バッチの期限 (Codex P2: 往復の数だけ期限を越えない)');

await ta('[!] 期限を過ぎたら往復をやめる', async () => {
  // 🚨 3,400 SKU = 18 往復。1 往復 30 秒待つと 9 分ぶん期限を越えうる
  let calls = 0;
  const past = new Date(Date.now() - 1000);
  const r = await fetchEasyshipSizes(['a', 'b'], {
    deadline: past,
    fetchBulk: async () => { calls++; return { found: [], inactive: [], notFound: [] }; },
  });
  assert.equal(calls, 0, '期限を過ぎているのに聞きに行った');
  assert.equal(r.ok, false);
  assert.match(r.error, /期限/);
});

await ta('[!] 期限で打ち切っても「自己配送」に倒さない', async () => {
  const r = await fetchEasyshipSizes(['a'], { deadline: new Date(Date.now() - 1000), fetchBulk: async () => ({}) });
  assert.equal(r.ok, false, '打ち切りを ok にすると全 FBM が自社の送料で計算される');
  assert.equal(r.map.size, 0);
});

await ta('期限がまだ先なら、これまでどおり全部聞く', async () => {
  let calls = 0;
  const skus = Array.from({ length: 250 }, (_, i) => `sku${i}`);
  const r = await fetchEasyshipSizes(skus, {
    deadline: new Date(Date.now() + 60_000),
    fetchBulk: async (part) => { calls++; return { found: [], inactive: [], notFound: part }; },
  });
  assert.equal(calls, 2);
  assert.equal(r.ok, true);
});

await ta('[!] 残りが 1 秒未満でも、その残り時間を超えて待たない (Codex P2)', async () => {
  // 🚨 下限を 1 秒に切り上げると、残り 0.2 秒でも 1 秒待てることになり期限を越える
  let budget = null;
  await fetchEasyshipSizes(['a'], {
    deadline: new Date(Date.now() + 200),
    fetchBulk: async () => { budget = Date.now(); return { found: [], inactive: [], notFound: ['a'] }; },
  });
  assert.ok(budget != null, '期限がまだ来ていないのに聞きに行っていない');
  // 実際の timeout は内部なので、境界の計算そのものを別に固定する
  const { __remainingMsForTest } = await import('./easyship-lookup.js');
  assert.ok(__remainingMsForTest(new Date(Date.now() + 200)) <= 200, '残り時間より長く待とうとしている');
  assert.ok(__remainingMsForTest(new Date(Date.now() + 200)) >= 1, '0 以下は AbortSignal が受けない');
  assert.equal(__remainingMsForTest(null), 30_000, '期限が無ければ既定のまま');
});

await ta('期限を渡さない呼び出しは、これまでどおり動く', async () => {
  const r = await fetchEasyshipSizes(['a'], { fetchBulk: async (p) => ({ found: [], inactive: [], notFound: p }) });
  assert.equal(r.ok, true);
});

console.log('');
console.log('聞く相手の集め方 (Codex P1: 引き継いだ出品を聞き漏らさない)');

const { default: Database } = await import('better-sqlite3');
const { createExpectedProfitSchema } = await import('./db.js');
const { loadEasyshipTargetSkus } = await import('./easyship-lookup.js');

function seedRuns(db, runs) {
  for (const r of runs) {
    db.prepare(`INSERT INTO price_fetch_run
      (run_id, mall, started_at, status, listing_enum_status) VALUES (?, 'amazon', ?, ?, ?)`)
      .run(r.id, r.at, r.status, r.enumStatus);
    for (const [key, ff] of r.listings) {
      db.prepare(`INSERT INTO mall_price_snapshot
        (run_id, mall, shop_id, mall_item_key, fulfillment, fetch_status, resolve_status, valid_until, source, fetched_at)
        VALUES (?, 'amazon', 'S1', ?, ?, 'ok', 'ok', '2099-01-01T00:00:00Z', 'test', ?)`)
        .run(r.id, key, ff, r.at);
    }
  }
}

t('[!] 列挙が partial の夜は、前回の完全な実行から引き継ぐぶんも聞く', () => {
  // 🚨 聞き漏らすと、Easy Ship の出品が「登録が無い」= 自己配送 に化ける (Codex P1)
  const db = new Database(':memory:');
  createExpectedProfitSchema(db);
  seedRuns(db, [
    { id: 'r1', at: '2026-09-08T14:00:00Z', status: 'ok', enumStatus: 'ok',
      listings: [['old-fbm', 'FBM'], ['old-fba', 'FBA']] },
    { id: 'r2', at: '2026-09-09T14:00:00Z', status: 'partial', enumStatus: 'partial',
      listings: [['new-fbm', 'FBM']] },
  ]);
  assert.deepEqual(loadEasyshipTargetSkus(db).sort(), ['new-fbm', 'old-fbm'],
    '引き継ぎ元の自社出荷を聞き漏らしている');
  db.close();
});

t('FBA と FBM を混ぜない (FBA には聞く必要がない)', () => {
  const db = new Database(':memory:');
  createExpectedProfitSchema(db);
  seedRuns(db, [
    { id: 'r1', at: '2026-09-09T14:00:00Z', status: 'ok', enumStatus: 'ok',
      listings: [['fbm-1', 'FBM'], ['fba-1', 'FBA']] },
  ]);
  assert.deepEqual(loadEasyshipTargetSkus(db), ['fbm-1']);
  db.close();
});

t('実行が1つも無ければ空 (聞きに行かない)', () => {
  const db = new Database(':memory:');
  createExpectedProfitSchema(db);
  assert.deepEqual(loadEasyshipTargetSkus(db), []);
  db.close();
});

console.log(`\n${passed} 件 PASS`);
