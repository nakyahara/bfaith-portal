/**
 * packing — NE商品マスタ名の解決 (ne-names.js) のテスト。
 *
 * 検証の要点:
 *   - 表示名の正 = NE商品マスタ (warehouse service-api経由)。呼べたSKUだけ返す
 *   - fail-soft: env未設定・API不達・HTTPエラーでも空を返して画面は落とさない (CSV名フォールバック)
 *   - キャッシュ: 2回目は同SKUをAPIに聞かない。見つからなかったSKUも負キャッシュで連打しない
 */
import assert from 'node:assert/strict';

process.env.WAREHOUSE_URL = 'http://wh.test';
process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';
delete process.env.CF_ACCESS_CLIENT_ID;
delete process.env.CF_ACCESS_CLIENT_SECRET;

const { neNamesFor, _clearNeNameCache } = await import('../ne-names.js');

let passed = 0;
async function t(name, fn) { await fn(); passed++; console.log(`  ok: ${name}`); }

const calls = [];
function fakeFetch(names, { ok = true, status = 200 } = {}) {
  return async (url, opts) => {
    const body = JSON.parse(opts.body);
    calls.push({ url, codes: body.codes, headers: opts.headers });
    return { ok, status, json: async () => ({ ok: true, names }) };
  };
}

await t('解決できたSKUだけ返す (重複・空白は正規化)', async () => {
  _clearNeNameCache(); calls.length = 0;
  const m = await neNamesFor(['AAA-1', ' AAA-1 ', 'BBB-2', '', null],
    fakeFetch({ 'AAA-1': 'ホワイトセージ 50ml_白プチ' }));
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].codes, ['AAA-1', 'BBB-2']);
  assert.equal(calls[0].url, 'http://wh.test/service-api/ne-products/names');
  assert.equal(calls[0].headers.Authorization, 'Bearer test-token');
  assert.equal(m.get('AAA-1'), 'ホワイトセージ 50ml_白プチ');
  assert.equal(m.has('BBB-2'), false);
});

await t('2回目はキャッシュから返し、APIを呼ばない (未発見SKUの負キャッシュ込み)', async () => {
  calls.length = 0;
  const m = await neNamesFor(['AAA-1', 'BBB-2'], fakeFetch({}));
  assert.equal(calls.length, 0);
  assert.equal(m.get('AAA-1'), 'ホワイトセージ 50ml_白プチ');
});

await t('API不達 (throw) でも空で返す', async () => {
  _clearNeNameCache();
  const m = await neNamesFor(['CCC-3'], async () => { throw new Error('ECONNREFUSED'); });
  assert.equal(m.size, 0);
});

await t('HTTPエラーでも空で返す + 直後はバックオフでAPIを呼ばない (毎ページ3秒待たせない)', async () => {
  _clearNeNameCache(); calls.length = 0;
  let m = await neNamesFor(['DDD-4'], fakeFetch({}, { ok: false, status: 502 }));
  assert.equal(m.size, 0);
  m = await neNamesFor(['DDD-4'], fakeFetch({ 'DDD-4': 'x' }));
  assert.equal(calls.length, 1);   // バックオフ中は問い合わせ自体を休む
  assert.equal(m.size, 0);
});

await t('バックオフ明け (リセット後) は再試行して復活する', async () => {
  _clearNeNameCache(); calls.length = 0;
  const m = await neNamesFor(['DDD-4'], fakeFetch({ 'DDD-4': '復活した名前' }));
  assert.equal(calls.length, 1);
  assert.equal(m.get('DDD-4'), '復活した名前');
});

await t('1000件超はクライアント側で分割して送る (超過分が黙って負キャッシュされない)', async () => {
  _clearNeNameCache(); calls.length = 0;
  const many = Array.from({ length: 1001 }, (_, i) => `SKU-${i}`);
  const m = await neNamesFor(many, fakeFetch({ 'SKU-1000': '1001件目の商品' }));
  assert.equal(calls.length, 2);
  assert.equal(calls[0].codes.length, 1000);
  assert.deepEqual(calls[1].codes, ['SKU-1000']);
  assert.equal(m.get('SKU-1000'), '1001件目の商品');
});

await t('2チャンク目が失敗 → 1チャンク目は正キャッシュ・失敗分は負キャッシュされずstaleで補う', async () => {
  _clearNeNameCache(); calls.length = 0;
  const many = Array.from({ length: 1001 }, (_, i) => `SKU-${i}`);
  // 事前に SKU-1000 (2チャンク目) の正キャッシュを作り、TTL切れにしておく
  await neNamesFor(['SKU-1000'], fakeFetch({ 'SKU-1000': '以前の名前' }));
  const { _cacheForTest } = await import('../ne-names.js');
  _cacheForTest().get('sku-1000').at = Date.now() - (31 * 60 * 1000);
  calls.length = 0;
  let n = 0;
  const m = await neNamesFor(many, async (url, opts) => {
    n++;
    const body = JSON.parse(opts.body);
    calls.push({ url, codes: body.codes });
    if (n === 2) return { ok: false, status: 500, json: async () => ({}) };
    return { ok: true, status: 200, json: async () => ({ ok: true, names: { 'SKU-0': '1チャンク目の商品' } }) };
  });
  assert.equal(calls.length, 2);
  assert.equal(m.get('SKU-0'), '1チャンク目の商品');   // 1チャンク目は反映
  assert.equal(m.get('SKU-1000'), '以前の名前');         // 失敗チャンクはstaleで補う
  assert.equal(_cacheForTest().has('sku-999'), true);      // 1チャンク目の未発見は負キャッシュ (正常)
  assert.equal(_cacheForTest().get('sku-1000').name, '以前の名前');   // 失敗チャンクは上書きされない
  // バックオフ中の再呼び出しでもstaleが返る
  calls.length = 0;
  const m2 = await neNamesFor(['SKU-1000'], fakeFetch({ 'SKU-1000': 'x' }));
  assert.equal(calls.length, 0);
  assert.equal(m2.get('SKU-1000'), '以前の名前');
});

await t('異常応答 (prototype名SKU・非文字列・空白名) はキャッシュを汚さない', async () => {
  _clearNeNameCache(); calls.length = 0;
  const m = await neNamesFor(['toString', 'GGG-7', 'HHH-8'],
    fakeFetch({ 'GGG-7': 123, 'HHH-8': '   ' }));
  assert.equal(m.size, 0);
});

await t('API不調時は期限切れの正キャッシュ (stale) で表示を守る', async () => {
  _clearNeNameCache(); calls.length = 0;
  await neNamesFor(['FFF-6'], fakeFetch({ 'FFF-6': '古いが正しい名前' }));
  // TTL切れを手で再現 (実時間を進める代わりにキャッシュ時刻を巻き戻す)
  const { _cacheForTest } = await import('../ne-names.js');
  _cacheForTest().get('fff-6').at = Date.now() - (31 * 60 * 1000);
  const m = await neNamesFor(['FFF-6'], async () => { throw new Error('down'); });
  assert.equal(m.get('FFF-6'), '古いが正しい名前');
});

await t('env未設定なら API を呼ばず空 (fail-soft)', async () => {
  _clearNeNameCache(); calls.length = 0;
  const savedUrl = process.env.WAREHOUSE_URL;
  delete process.env.WAREHOUSE_URL;
  const m = await neNamesFor(['EEE-5'], fakeFetch({ 'EEE-5': 'x' }));
  process.env.WAREHOUSE_URL = savedUrl;
  assert.equal(calls.length, 0);
  assert.equal(m.size, 0);
});

console.log(`test-ne-names: ${passed} 件 pass`);
