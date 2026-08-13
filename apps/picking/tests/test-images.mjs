/**
 * picking PR3.5 — 楽天白抜き画像の解決チェーンとキャッシュのテスト。
 * mirror索引とRMS呼び出しは注入 (deps) で差し替え、実APIは叩かない。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));
delete process.env.RAKUTEN_SHOP_SLUG;

const { normalizeImageUrl, resolveManageNumber, extractImageUrls, ensureImagesFor, getImageMap } =
  await import('../images.js');
const { initPickingDB, getDB } = await import('../db.js');

initPickingDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

// ─── 純関数 ───

t('normalizeImageUrl: 完全URL/プロトコル相対/cabinet相対/不正値/ドメインallowlist', () => {
  assert.equal(normalizeImageUrl('https://image.rakuten.co.jp/b/a.jpg'), 'https://image.rakuten.co.jp/b/a.jpg');
  assert.equal(normalizeImageUrl('https://shop.r10s.jp/x.jpg'), 'https://shop.r10s.jp/x.jpg');
  assert.equal(normalizeImageUrl('//image.rakuten.co.jp/x.jpg'), 'https://image.rakuten.co.jp/x.jpg');
  assert.equal(normalizeImageUrl('/img/white_00.jpg'), 'https://image.rakuten.co.jp/b-faith/cabinet/img/white_00.jpg');
  assert.equal(normalizeImageUrl(''), null);
  assert.equal(normalizeImageUrl(null), null);
  assert.equal(normalizeImageUrl('relative.jpg'), null);
  assert.equal(normalizeImageUrl('https://evil.example.com/a.jpg'), null, '楽天ドメイン以外は弾く');
  assert.equal(normalizeImageUrl('https://evilrakuten.co.jp/a.jpg'), null, 'サフィックス偽装は弾く');
});

t('resolveManageNumber: 直接一致とハイフン削りfallback (最大3段)', () => {
  const nums = new Map([['whitesage10', 'whitesage10'], ['0726-000629', '0726-000629']]);
  assert.equal(resolveManageNumber(nums, 'whitesage10'), 'whitesage10');
  assert.equal(resolveManageNumber(nums, 'WhiteSage10'), 'whitesage10');       // 大文字小文字を吸収
  assert.equal(resolveManageNumber(nums, '0726-000629-bk'), '0726-000629');   // SKU→親
  assert.equal(resolveManageNumber(nums, 'unknown-code'), null);
  assert.equal(resolveManageNumber(nums, 'a-b-c-d-e'), null);                 // 3段削っても無ければnull
});

t('extractImageUrls: 白抜き優先・無ければimages[0]', () => {
  const both = extractImageUrls({
    whiteBgImage: { location: '/white_00.jpg' },
    images: [{ location: '/top.jpg' }],
  });
  assert.ok(both.whiteBgUrl.endsWith('/cabinet/white_00.jpg'));
  assert.ok(both.topUrl.endsWith('/cabinet/top.jpg'));
  const topOnly = extractImageUrls({ images: [{ location: '/top.jpg' }] });
  assert.equal(topOnly.whiteBgUrl, null);
  assert.ok(topOnly.topUrl);
  const none = extractImageUrls({});
  assert.equal(none.whiteBgUrl, null);
  assert.equal(none.topUrl, null);
});

// ─── 解決+キャッシュ ───

function fakeDeps({ items = [], failed = [] } = {}) {
  const calls = [];
  return {
    calls,
    loadMaps: () => ({
      rakutenByNe: new Map([['csvsku-a', 'rk-item-a-red']]),   // 変換テーブル経由のSKU
      itemNumbers: new Map([
        ['rk-item-a', 'rk-item-a'],          // ハイフン削りで届く親
        ['whitesage10', 'whitesage10'],      // ne_code直接一致 (product-hub出品分)
      ]),
    }),
    fetchDetails: async (mns) => { calls.push(mns); return { items, failed }; },
  };
}

// 以下の3ケースは async のため t() を使わず直接実行し、末尾で件数に加算する
{
  const deps = fakeDeps({
    items: [
      { manageNumber: 'rk-item-a', whiteBgImage: { location: '/a_00.jpg' }, images: [{ location: '/a_top.jpg' }] },
      { manageNumber: 'whitesage10', images: [{ location: '/ws_top.jpg' }] },   // 白抜き未登録→1枚目
    ],
  });
  const stats = await ensureImagesFor(['csvsku-a', 'WhiteSage10', 'no-such-sku'], deps);
  assert.equal(stats.ok, 2);
  assert.equal(stats.notFound, 1);
  assert.deepEqual(deps.calls, [['rk-item-a', 'whitesage10']]);
  const map = getImageMap(['csvsku-a', 'whitesage10', 'no-such-sku']);
  assert.ok(map.get('csvsku-a').url.endsWith('/cabinet/a_00.jpg'), '白抜きが優先');
  assert.ok(map.get('whitesage10').url.endsWith('/cabinet/ws_top.jpg'), 'フォールバックで1枚目');
  assert.equal(map.get('no-such-sku').url, null);
  assert.equal(map.get('no-such-sku').status, 'not_found');
  console.log('  ok: ensureImagesFor: 白抜き取得→キャッシュok・変換テーブル+ハイフン削りの連鎖 (async)');
}

{
  // キャッシュ済み (ok) は再取得しない / not_found は当日中は再試行しない
  const deps = fakeDeps({ items: [] });
  const stats = await ensureImagesFor(['csvsku-a', 'no-such-sku'], deps);
  assert.equal(stats.fetched, 0, '全てキャッシュ済みでRMSを呼ばない');
  assert.equal(deps.calls.length, 0);
  console.log('  ok: ensureImagesFor: 解決済み/当日not_foundは再取得しない (async)');
}

{
  // RMS個別失敗は error でキャッシュ (翌日再試行対象)
  const deps = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['failsku', 'failsku']]) }),
    fetchDetails: async () => ({ items: [], failed: [{ manageNumber: 'failsku', reason: 'rms 500' }] }),
  };
  const stats = await ensureImagesFor(['failsku'], deps);
  assert.equal(stats.errors, 1);
  const row = getDB().prepare("SELECT status FROM pk_product_images WHERE ne_code='failsku'").get();
  assert.equal(row.status, 'error');
  console.log('  ok: ensureImagesFor: RMS個別失敗はerrorでキャッシュ (async)');
}

{
  // RMS呼び出し全体の失敗 (キュー上限等) も error 記録 → 画面を開くたびの連打にならない
  const deps = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['qsku', 'qsku']]) }),
    fetchDetails: async () => { throw new Error('Too many pending requests for rakuten'); },
  };
  const stats = await ensureImagesFor(['qsku'], deps);
  assert.equal(stats.errors, 1);
  const row = getDB().prepare("SELECT status FROM pk_product_images WHERE ne_code='qsku'").get();
  assert.equal(row.status, 'error');
  // 同日の再呼び出しは fetch 自体走らない
  let called = 0;
  await ensureImagesFor(['qsku'], { ...deps, fetchDetails: async () => { called++; return { items: [], failed: [] }; } });
  assert.equal(called, 0);
  console.log('  ok: ensureImagesFor: 一括失敗もerror記録し当日は再試行しない (async)');
}

console.log(`\ntest-images: ${passed + 3} 件 pass`);
