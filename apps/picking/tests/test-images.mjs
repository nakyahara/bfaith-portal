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

t('extractImageUrls: バリエーション画像はSKUに一致したものだけ・無ければ白抜き', () => {
  const item = {
    manageNumber: 'ganeshisc20',
    whiteBgImage: { location: '/ganeshisc20_00.jpg' },
    images: [{ location: '/ganeshisc20_top.jpg' }],
    variants: {
      'ganeshisc20-no04': { merchantDefinedSkuId: 'ganeshisc20-no04', images: [{ location: '/ganeshisc20-no04_00.jpg' }] },
      'ganeshisc20-no08': { merchantDefinedSkuId: 'ganeshisc20-no08', images: [{ location: '/ganeshisc20-no08_00.jpg' }] },
      'ganeshisc20-noimg': { merchantDefinedSkuId: 'ganeshisc20-noimg', images: [] },
    },
  };
  assert.ok(extractImageUrls(item, ['GANESHISC20-NO08']).variantUrl.endsWith('/ganeshisc20-no08_00.jpg'), '大文字小文字を吸収してそのSKUの画像');
  assert.ok(extractImageUrls(item, ['ganeshisc20-no04']).variantUrl.endsWith('/ganeshisc20-no04_00.jpg'));
  assert.equal(extractImageUrls(item, ['ganeshisc20-noimg']).variantUrl, null, 'SKU画像が未登録なら null (白抜きへ)');
  assert.equal(extractImageUrls(item, ['other']).variantUrl, null);
  assert.ok(extractImageUrls(item, ['other']).whiteBgUrl.endsWith('/ganeshisc20_00.jpg'));
  assert.equal(extractImageUrls(item).variantUrl, null, 'codes 省略でも壊れない');
});

t('resolveManageNumber: 直接一致とハイフン削りfallback (最大3段)', () => {
  const nums = new Map([['whitesage10', 'whitesage10'], ['0726-000629', '0726-000629']]);
  assert.equal(resolveManageNumber(nums, 'whitesage10'), 'whitesage10');
  assert.equal(resolveManageNumber(nums, 'WhiteSage10'), 'whitesage10');       // 大文字小文字を吸収
  assert.equal(resolveManageNumber(nums, '0726-000629-bk'), '0726-000629');   // SKU→親
  assert.equal(resolveManageNumber(nums, 'unknown-code'), null);
  assert.equal(resolveManageNumber(nums, 'a-b-c-d-e'), null);                 // 3段削っても無ければnull
});

t('resolveManageNumber: 候補配列を順に試す (楽天 W/AM/AL の別名 — 2026-09-01)', () => {
  const nums = new Map([['waterbowl-m', 'waterbowl-m'], ['kofunneil', 'kofunneil']]);
  // AL (連番) を先に渡しても、後続の AM から解決できる
  assert.equal(resolveManageNumber(nums, ['394', 'waterbowl-m-wh']), 'waterbowl-m');
  assert.equal(resolveManageNumber(nums, ['0776', 'kofunneil-0776']), 'kofunneil');
  // 1つも当たらなければ null
  assert.equal(resolveManageNumber(nums, ['394', '0776']), null);
  // 文字列渡し (従来の呼び出し) も引き続き動く
  assert.equal(resolveManageNumber(nums, 'waterbowl-m-wh'), 'waterbowl-m');
  assert.equal(resolveManageNumber(nums, []), null);
  assert.equal(resolveManageNumber(nums, [null, undefined, '']), null);
});

t('extractImageUrls: 役割ごとに照合する (W で兄弟SKUの画像を掴まない — Codex 2026-09-01 High)', () => {
  // 合皮補修シート型: 商品番号 W=0726-001802 を全12色が共有。variants のキーは AL の連番
  const item = {
    manageNumber: '0726-001802',
    whiteBgImage: { location: '/sheet_00.jpg' },
    variants: {
      // 先頭 variant の merchantDefinedSkuId が W と同値 = 旧実装ならこれが先に当たった
      '360': { merchantDefinedSkuId: '0726-001802', images: [{ location: '/sheet_black.jpg' }] },
      '361': { merchantDefinedSkuId: '0726-001802-ow', images: [{ location: '/sheet_white.jpg' }] },
    },
  };
  // 対象は白 (AL=361 / AM=0726-001802-ow)。W は渡さない
  const white = extractImageUrls(item, { variantIds: ['361'], merchantIds: ['0726-001802-ow'], any: ['0726-001802-ow'] });
  assert.ok(white.variantUrl.endsWith('/sheet_white.jpg'), '自分の色の写真を取る');
  // AL だけ分かっている場合も同じ
  assert.ok(extractImageUrls(item, { variantIds: ['361'], merchantIds: [], any: [] }).variantUrl.endsWith('/sheet_white.jpg'));
  // AM だけでも取れる
  assert.ok(extractImageUrls(item, { variantIds: [], merchantIds: ['0726-001802-ow'], any: [] }).variantUrl.endsWith('/sheet_white.jpg'));
  // 一致しなければ null (白抜きへフォールバック)
  const none = extractImageUrls(item, { variantIds: ['999'], merchantIds: [], any: [] });
  assert.equal(none.variantUrl, null);
  assert.ok(none.whiteBgUrl.endsWith('/sheet_00.jpg'));
  // 配列渡し (従来の呼び出し) は役割不明として両方に当てる
  assert.ok(extractImageUrls(item, ['361']).variantUrl.endsWith('/sheet_white.jpg'));
  assert.ok(extractImageUrls(item, ['0726-001802-ow']).variantUrl.endsWith('/sheet_white.jpg'));
});

t('extractImageUrls: 候補順に走査する (variants の並び順に引きずられない)', () => {
  const item = {
    variants: {
      'zz-first': { merchantDefinedSkuId: 'sku-b', images: [{ location: '/b.jpg' }] },
      'sku-a': { merchantDefinedSkuId: 'zz-other', images: [{ location: '/a.jpg' }] },
    },
  };
  // AL='sku-a' を優先 (列挙順では zz-first が先)
  assert.ok(extractImageUrls(item, { variantIds: ['sku-a'], merchantIds: ['sku-b'], any: [] })
    .variantUrl.endsWith('/a.jpg'));
});

{
  // 兄弟SKU 2つが同じ商品ページ (W=share-item) を共有し、AL/AM は未登録。
  // W を照合に使うと、先頭 variant (別の色) の画像を掴んでしまう
  const item = {
    manageNumber: 'share-item',
    whiteBgImage: { location: '/share_00.jpg' },
    variants: {
      'v-red': { merchantDefinedSkuId: 'share-item', images: [{ location: '/red.jpg' }] },
      'v-blue': { merchantDefinedSkuId: 'share-item', images: [{ location: '/blue.jpg' }] },
    },
  };
  const stats = await ensureImagesFor(['share-blue'], {
    loadMaps: () => ({
      // 新形式の索引で W だけ (AL/AM なし)
      rakutenByNe: new Map([['share-blue', { all: ['share-item'], variantIds: [], merchantIds: [] }]]),
      itemNumbers: new Map([['share-item', 'share-item']]),
    }),
    fetchDetails: async () => ({ items: [item], failed: [] }),
  });
  assert.equal(stats.ok, 1);
  const row = getDB().prepare('SELECT * FROM pk_product_images WHERE ne_code=?').get('share-blue');
  assert.equal(row.manage_number, 'share-item', 'W からは商品管理番号を解決してよい');
  assert.equal(row.variant_image_url, null, 'W では variant を確定させない (別の色を掴まない)');
  assert.ok(row.white_bg_url.endsWith('/share_00.jpg'), '商品共通の写真にフォールバック');
  passed++;
  console.log('  ok: W だけ登録された商品でも W を variants 照合に使わない (Codex R2 High) (async)');
}

{
  const item = {
    manageNumber: 'share2',
    whiteBgImage: { location: '/share2_00.jpg' },
    variants: {
      'v1': { merchantDefinedSkuId: 'share2', images: [{ location: '/first.jpg' }] },
      'v2': { merchantDefinedSkuId: 'share2-b', images: [{ location: '/second.jpg' }] },
    },
  };
  // ne_code 自体が W と同値 ('share2')。any に入れると先頭 variant を掴んでしまう
  const stats = await ensureImagesFor(['share2'], {
    loadMaps: () => ({
      rakutenByNe: new Map([['share2', { all: ['share2'], variantIds: [], merchantIds: [] }]]),
      itemNumbers: new Map([['share2', 'share2']]),
    }),
    fetchDetails: async () => ({ items: [item], failed: [] }),
  });
  assert.equal(stats.ok, 1);
  const row = getDB().prepare('SELECT * FROM pk_product_images WHERE ne_code=?').get('share2');
  assert.equal(row.variant_image_url, null, 'ne_code=W では variant を確定させない');
  assert.ok(row.white_bg_url.endsWith('/share2_00.jpg'));
  passed++;
  console.log('  ok: ne_code が W と同値でも variants 照合に使わない (Codex R3 High) (async)');
}

{
  // mirror_rakuten_item_daily は「その日に動いた商品」だけなので、売れなかった商品は引けない。
  // 実測: 「楽天に無い」244件のうち135件が実在し、全部に画像があった
  const calls = [];
  const stats = await ensureImagesFor(['mamabutter-bs'], {
    loadMaps: () => ({
      // AL しか登録がなく、item_daily にも載っていない = 旧実装では not_found だった
      rakutenByNe: new Map([['mamabutter-bs', { all: ['mamabutter-bs'], variantIds: ['mamabutter-bs'], merchantIds: [] }]]),
      itemNumbers: new Map([['other-item', 'other-item']]),
    }),
    fetchDetails: async (mns) => {
      calls.push(mns);
      return {
        items: mns.some((m) => m === 'mamabutter-bs')
          ? [{ manageNumber: 'mamabutter-bs', whiteBgImage: { location: '/mamabutter_00.jpg' } }]
          : [],
        failed: [],
      };
    },
  });
  assert.equal(stats.ok, 1, '画像が取れる');
  assert.ok(calls[0].includes('mamabutter-bs'), '候補を RMS へ投げている');
  const row = getDB().prepare('SELECT * FROM pk_product_images WHERE ne_code=?').get('mamabutter-bs');
  assert.equal(row.manage_number, 'mamabutter-bs', '当たった管理番号を保存する (次回はこれを使う)');
  assert.ok(String(row.white_bg_url).endsWith('/mamabutter_00.jpg'), `white_bg_url=${row.white_bg_url} status=${row.status} mn=${row.manage_number}`);
  passed++;
  console.log('  ok: mirror に無い商品でも楽天へ直接照会して見つける (2026-09-01 ママバター事件) (async)');
}

{
  const stats = await ensureImagesFor(['nowhere-sku'], {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map() }),
    // RMS は投げた全コードについて items か failed のどちらかを返す (実測)
    fetchDetails: async (mns) => ({ items: [], failed: mns.map((m) => ({ manageNumber: m, reason: `Not found for inputs; manageNumber=${m}` })) }),
  });
  assert.equal(stats.notFound, 1);
  const row = getDB().prepare('SELECT * FROM pk_product_images WHERE ne_code=?').get('nowhere-sku');
  assert.equal(row.status, 'not_found');
  passed++;
  console.log('  ok: 楽天に本当に無い商品は not_found のまま (候補を全部試しても当たらない) (async)');
}

{
  // 🚨 障害 (500/レート制限/欠落応答) を「楽天に無い」と断定しない — 今回の事故と同じ性質 (Codex R1 High)
  const stats = await ensureImagesFor(['flaky-sku'], {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map() }),
    fetchDetails: async (mns) => ({
      items: [],
      failed: mns.map((m, i) => ({ manageNumber: m, reason: i === 0 ? 'Not found' : 'HTTP 500 upstream error' })),
    }),
  });
  assert.equal(stats.notFound, 0, '理由不明が混ざれば不存在を確定しない');
  assert.equal(stats.errors, 1);
  assert.equal(getDB().prepare('SELECT status FROM pk_product_images WHERE ne_code=?').get('flaky-sku').status, 'error');
  passed++;
  console.log('  ok: 障害応答を not_found にしない (30分後に再試行する) (async)');
}

{
  // 応答自体が返ってこない (チャンク例外) 場合も、probe 中の SKU を not_found にしない
  const stats = await ensureImagesFor(['chunkfail-sku'], {
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map() }),
    fetchDetails: async () => { throw new Error('network down'); },
  });
  assert.equal(stats.notFound, 0);
  const cf = getDB().prepare('SELECT status, manage_number FROM pk_product_images WHERE ne_code=?').get('chunkfail-sku');
  assert.equal(cf.status, 'error', 'probe 候補もチャンクに紐づいている');
  assert.equal(cf.manage_number, null,
    '未確認の候補を管理番号として保存しない (次回キャッシュ優先で誤った番号に固定される — Codex R2 High)');
  passed++;
  console.log('  ok: チャンク失敗時も probe 中の商品を not_found にしない (async)');
}

{
  const calls = [];
  await ensureImagesFor(['mamabutter-bs'], {
    force: true,
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map() }),   // mirror から完全に消えた状態
    fetchDetails: async (mns) => {
      calls.push(mns);
      return { items: [{ manageNumber: 'mamabutter-bs', whiteBgImage: { location: '/mamabutter_01.jpg' } }], failed: [] };
    },
  });
  assert.deepEqual(calls[0], ['mamabutter-bs'], 'キャッシュの管理番号だけを問い合わせる (候補を撒き直さない)');
  passed++;
  console.log('  ok: 一度当たった管理番号はキャッシュから使う (mirror から消えても再照会しない) (async)');
}

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
    // RMS は投げた全コードについて items か failed のどちらかを返す (実測)。
    // 明示的な failed が無いと「答えが無い」= 障害扱い (error) になるので、テストでも実態に合わせる
    fetchDetails: async (mns) => {
      calls.push(mns);
      const hit = new Set(items.map((it) => String(it.manageNumber || it.itemNumber).toLowerCase()));
      const auto = mns.filter((m) => !hit.has(String(m).toLowerCase()))
        .map((m) => ({ manageNumber: m, reason: `Not found for inputs; manageNumber=${m}` }));
      return { items, failed: failed.length > 0 ? failed : auto };
    },
  };
}

// 以下の3ケースは async のため t() を使わず直接実行し、末尾で件数に加算する
{
  const deps = fakeDeps({
    items: [
      { manageNumber: 'rk-item-a', whiteBgImage: { location: '/a_00.jpg' }, images: [{ location: '/a_top.jpg' }],
        variants: { 'rk-item-a-red': { merchantDefinedSkuId: 'rk-item-a-red', images: [{ location: '/a_red_00.jpg' }] },
                    'rk-item-a-blue': { merchantDefinedSkuId: 'rk-item-a-blue', images: [{ location: '/a_blue_00.jpg' }] } } },
      { manageNumber: 'whitesage10', images: [{ location: '/ws_top.jpg' }] },   // 白抜き未登録→1枚目
    ],
  });
  const stats = await ensureImagesFor(['csvsku-a', 'WhiteSage10', 'no-such-sku'], deps);
  assert.equal(stats.ok, 2);
  assert.equal(stats.notFound, 1);
  assert.equal(stats.variant, 1);
  // mirror で管理番号を引けなかった SKU は、候補を RMS に投げて存在を確かめる (2026-09-01)。
  // 'no' のような短い断片は投げない (PROBE_MIN_LEN)
  assert.deepEqual(deps.calls, [['rk-item-a', 'whitesage10', 'no-such-sku', 'no-such']]);
  const map = getImageMap(['csvsku-a', 'whitesage10', 'no-such-sku']);
  assert.ok(map.get('csvsku-a').url.endsWith('/cabinet/a_red_00.jpg'), '変換テーブルの楽天SKUコードで variants を引き、そのSKUの画像 (白抜きより優先)');
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
