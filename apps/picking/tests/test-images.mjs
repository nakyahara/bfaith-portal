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
  // RMS呼び出し全体の失敗 (キュー上限等): 429はバックオフ付きで4回まで粘り、
  // それでもダメなら error 記録 → 画面を開くたびの連打にならない
  let attempts = 0;
  const sleeps = [];
  const deps = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['qsku', 'qsku']]) }),
    fetchDetails: async () => { attempts++; throw new Error('Too many pending requests for rakuten'); },
    sleep: async (ms) => { sleeps.push(ms); },
  };
  const stats = await ensureImagesFor(['qsku'], deps);
  assert.equal(attempts, 4, 'キュー混雑は4回まで再試行');
  assert.equal(sleeps.length, 3, '再試行の間に3回待つ');
  assert.ok(sleeps.every((ms) => ms >= 45_000), 'バックオフは45秒以上');
  assert.equal(stats.errors, 1);
  const row = getDB().prepare("SELECT status FROM pk_product_images WHERE ne_code='qsku'").get();
  assert.equal(row.status, 'error');
  // 直後 (30分TTL内) の再呼び出しは fetch 自体走らない
  let called = 0;
  await ensureImagesFor(['qsku'], { ...deps, fetchDetails: async () => { called++; return { items: [], failed: [] }; } });
  assert.equal(called, 0);
  console.log('  ok: ensureImagesFor: キュー混雑はバックオフ再試行→error記録・30分内は再試行しない (async)');
}

{
  // キュー混雑が途中で解消したら成功する / 混雑以外のエラーは即error記録 (リトライしない)
  let n = 0;
  const deps = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['rsku', 'rsku']]) }),
    fetchDetails: async () => {
      n++;
      if (n < 3) throw new Error('[rakuten-rms-proxy] RATE_LIMIT_QUEUE_FULL');
      return { items: [{ manageNumber: 'rsku', whiteBgImage: { location: '/r_00.jpg' } }], failed: [] };
    },
    sleep: async () => {},
  };
  const stats = await ensureImagesFor(['rsku'], deps);
  assert.equal(n, 3, '2回混雑→3回目で成功');
  assert.equal(stats.ok, 1);
  let m = 0;
  const deps2 = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['xsku', 'xsku']]) }),
    fetchDetails: async () => { m++; throw new Error('ECONNREFUSED'); },
    sleep: async () => { throw new Error('混雑以外で待ってはいけない'); },
  };
  const stats2 = await ensureImagesFor(['xsku'], deps2);
  assert.equal(m, 1, '混雑以外は再試行しない');
  assert.equal(stats2.errors, 1);
  console.log('  ok: fetchWithQueueRetry: 混雑解消で成功・他エラーは即error (async)');
}

{
  // 混雑判定は構造優先: statusCode=500は本文が混雑文言でも再試行しない /
  // statusCode=429はメッセージが "HTTP 429" でも再試行する
  let a = 0;
  const e500 = Object.assign(new Error('Too many pending requests for rakuten'), { statusCode: 500 });
  const deps500 = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['s500', 's500']]) }),
    fetchDetails: async () => { a++; throw e500; },
    sleep: async () => { throw new Error('500で待ってはいけない'); },
  };
  const st1 = await ensureImagesFor(['s500'], deps500);
  assert.equal(a, 1, 'statusCode=500は文言が混雑でも即error');
  assert.equal(st1.errors, 1);
  let b = 0;
  const e429 = Object.assign(new Error('HTTP 429'), { statusCode: 429 });
  const deps429 = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['s429', 's429']]) }),
    fetchDetails: async () => { b++; throw e429; },
    sleep: async () => {},
  };
  await ensureImagesFor(['s429'], deps429);
  assert.equal(b, 4, 'statusCode=429は文言に関わらず再試行');
  // body.error は構造化マーカー: statusCodeが500でも混雑として再試行する
  let c = 0;
  const eBody = Object.assign(new Error('proxy relay error'), { statusCode: 500, body: { error: 'RATE_LIMIT_QUEUE_FULL' } });
  const depsBody = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['sbody', 'sbody']]) }),
    fetchDetails: async () => { c++; throw eBody; },
    sleep: async () => {},
  };
  await ensureImagesFor(['sbody'], depsBody);
  assert.equal(c, 4, 'body.error=RATE_LIMIT_QUEUE_FULLはstatusCodeに関わらず再試行');
  console.log('  ok: isQueueFull: 構造 (statusCode/body) 優先で判定 (async)');
}

{
  // 60件=2チャンク: 前半チャンク成功・後半チャンクだけ混雑 → 成功済みチャンクは再送しない
  const skus = Array.from({ length: 60 }, (_, i) => `blk${String(i).padStart(2, '0')}`);
  const itemNumbers = new Map(skus.map((s) => [s, s]));
  const calls = [];
  const deps = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers }),
    fetchDetails: async (mns) => {
      calls.push(mns.length);
      if (mns.includes('blk50')) throw Object.assign(new Error('queue full'), { statusCode: 429 });
      return { items: mns.map((mn) => ({ manageNumber: mn, whiteBgImage: { location: `/w_${mn}.jpg` } })), failed: [] };
    },
    sleep: async () => {},
  };
  const stats = await ensureImagesFor(skus, deps);
  assert.equal(stats.ok, 50, '成功チャンクの50件は解決');
  assert.equal(stats.errors, 10, '混雑チャンクの10件だけerror');
  assert.deepEqual(calls, [50, 10, 10, 10, 10], '成功済みチャンクは再送されない');
  const row = getDB().prepare("SELECT status FROM pk_product_images WHERE ne_code='blk55'").get();
  assert.equal(row.status, 'error');
  const okRow = getDB().prepare("SELECT status FROM pk_product_images WHERE ne_code='blk10'").get();
  assert.equal(okRow.status, 'ok');
  console.log('  ok: ensureImagesFor: チャンク単位リトライ・部分失敗は該当SKUのみerror (async)');
}

{
  // 同一SKU集合がキューにいる間は積み直さない (作業画面の連続オープン対策)
  const { queueEnsureImages } = await import('../images.js');
  let runs = 0;
  const deps = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['dupsku', 'dupsku']]) }),
    fetchDetails: async () => { runs++; return { items: [], failed: [] }; },
  };
  queueEnsureImages(['dupsku'], 'a', deps);
  queueEnsureImages(['dupsku'], 'b', deps);   // 同一集合 → skip
  await queueEnsureImages(['DupSku '], 'c', deps);   // 正規化後も同一 → skip
  assert.equal(runs, 1, '同一SKU集合はキュー内で1回だけ実行');
  const deps2 = {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['othersku', 'othersku']]) }),
    fetchDetails: async () => { runs++; return { items: [], failed: [] }; },
  };
  await queueEnsureImages(['othersku'], 'd', deps2);   // 別集合は通る (キーが掃除されている)
  assert.equal(runs, 2);
  console.log('  ok: queueEnsureImages: 同一SKU集合の重複enqueueを排除 (async)');
}

console.log(`\ntest-images: ${passed + 8} 件 pass`);
