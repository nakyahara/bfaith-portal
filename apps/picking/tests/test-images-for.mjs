import { temporaryTestRoot } from '../../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * imagesFor (一覧の画面用の読み口・マスタの入力の一覧 2026-10-17) のテスト。
 * 確かめること:
 *   - キャッシュ済み = URL をそのまま返す (楽天を呼ばない)・選ぶ順はバリエーション画像 → 白抜き → 1枚目
 *   - キャッシュに無い = 「取得中」で返し、キューに 1 回だけ積む (続けて呼んでも積み増さない)・解決後は URL
 *   - not_found / error = 時期 (isDue) が来るまで積まない。来たら積む
 *   - 1 回に積むのは queueMax 件まで・同じ lane の待ちジョブは 1 つまで・canQueue=false なら積まない
 *   - 楽天の画像ドメイン以外の URL は返さない (キャッシュが汚れていても)・http は https に直す
 * mirror 索引と RMS 呼び出しは deps で差し替え、実 API は叩かない。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-imgfor-'));
delete process.env.RAKUTEN_SHOP_SLUG;

const { imagesFor, queueEnsureImages } = await import('../images.js');
const { initPickingDB, getDB, utcNow } = await import('../db.js');
initPickingDB();
const db = getDB();

let passed = 0;
async function t(name, fn) { await fn(); passed++; console.log(`  ok: ${name}`); }

const put = (ne, o) => db.prepare(`INSERT OR REPLACE INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, variant_image_url, status, fetched_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)`).run(ne, o.mn ?? null, o.white ?? null, o.top ?? null, o.variant ?? null, o.status ?? 'ok', o.at ?? utcNow());
const ago = (ms) => new Date(Date.now() - ms).toISOString().slice(0, 19) + 'Z';

/** RMS の差し替え: 呼ばれた管理番号を記録し、ITEMS にあれば返す (無ければ明示的に「無い」) */
const calls = [];
const ITEMS = new Map();
const deps = {
  loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([...ITEMS.keys()].map((k) => [k, k])) }),
  fetchDetails: async (mns) => {
    calls.push([...mns]);
    return {
      items: mns.filter((m) => ITEMS.has(m)).map((m) => ITEMS.get(m)),
      failed: mns.filter((m) => !ITEMS.has(m)).map((m) => ({ manageNumber: m, reason: `Not found for inputs; manageNumber=${m}` })),
    };
  },
};
/** キューが空くまで待つ (queueEnsureImages は直列キューの末尾を返す) */
const drain = () => queueEnsureImages([], '', deps);

await t('キャッシュ済み = URL を返す・楽天を呼ばない・バリエーション画像が先 (別の色の写真を出さない)', async () => {
  put('v-red', { variant: 'https://image.rakuten.co.jp/b-faith/cabinet/red.jpg', white: 'https://image.rakuten.co.jp/b-faith/cabinet/common.jpg' });
  put('w-only', { white: 'https://image.rakuten.co.jp/b-faith/cabinet/w.jpg', top: 'https://image.rakuten.co.jp/b-faith/cabinet/t.jpg' });
  put('t-only', { top: 'https://tshop.r10s.jp/b-faith/cabinet/t2.jpg' });
  const m = imagesFor(['V-RED', ' w-only ', 't-only'], { deps, lane: 'test' });
  assert.deepEqual(m.get('v-red'), { url: 'https://image.rakuten.co.jp/b-faith/cabinet/red.jpg', state: 'ok' }, '大文字小文字・空白は吸収・バリエーション画像');
  assert.equal(m.get('w-only').url, 'https://image.rakuten.co.jp/b-faith/cabinet/w.jpg');
  assert.equal(m.get('t-only').url, 'https://tshop.r10s.jp/b-faith/cabinet/t2.jpg');
  await drain();
  assert.equal(calls.length, 0, '新しいキャッシュだけなら楽天を呼ばない');
});

await t('楽天の画像ドメイン以外は返さない・http は https に直す', async () => {
  put('evil', { variant: 'https://evil.example.com/a.jpg', white: 'javascript:alert(1)', top: 'https://evilrakuten.co.jp/x.jpg' });
  put('plain-http', { white: 'http://image.rakuten.co.jp/b-faith/cabinet/h.jpg' });
  put('evil-fallback', { variant: 'https://evil.example.com/a.jpg', white: 'https://image.rakuten.co.jp/b-faith/cabinet/ok.jpg' });
  const m = imagesFor(['evil', 'plain-http', 'evil-fallback'], { deps, canQueue: false });
  assert.deepEqual(m.get('evil'), { url: null, state: 'not_found' }, 'ドメイン外しか無い = 画像なしと同じ');
  assert.equal(m.get('plain-http').url, 'https://image.rakuten.co.jp/b-faith/cabinet/h.jpg');
  assert.equal(m.get('evil-fallback').url, 'https://image.rakuten.co.jp/b-faith/cabinet/ok.jpg', 'ドメイン外は飛ばして次の候補');
});

await t('キャッシュに無い = 取得中で返す・キューに 1 回だけ積む・解決したら URL', async () => {
  calls.length = 0;
  ITEMS.set('new-a', { manageNumber: 'new-a', whiteBgImage: { location: '/new-a.jpg' } });
  const m1 = imagesFor(['new-a', 'new-gone'], { deps, lane: 'test', label: 'test' });
  assert.deepEqual(m1.get('new-a'), { url: null, state: 'pending' });
  assert.deepEqual(m1.get('new-gone'), { url: null, state: 'pending' });
  // 解決が終わる前にもう一度開いた = 取得中のまま・積み増さない
  const m2 = imagesFor(['new-a', 'new-gone'], { deps, lane: 'test', label: 'test' });
  assert.equal(m2.get('new-a').state, 'pending');
  await drain();
  assert.equal(calls.length, 1, `楽天は 1 回だけ (${JSON.stringify(calls)})`);
  const m3 = imagesFor(['new-a', 'new-gone'], { deps, lane: 'test' });
  assert.deepEqual(m3.get('new-a'), { url: 'https://image.rakuten.co.jp/b-faith/cabinet/new-a.jpg', state: 'ok' });
  assert.deepEqual(m3.get('new-gone'), { url: null, state: 'not_found' }, '楽天に無い = 画像なし');
  await drain();
  assert.equal(calls.length, 1, 'not_found は当日中は取り直さない');
});

await t('not_found / error は時期が来るまで積まない・来たら積む (ok は 7 日)', async () => {
  calls.length = 0;
  put('err-fresh', { status: 'error', at: ago(5 * 60e3) });
  put('err-old', { status: 'error', at: ago(31 * 60e3) });
  put('nf-yesterday', { status: 'not_found', at: ago(36 * 3600e3) });
  put('ok-old', { white: 'https://image.rakuten.co.jp/b-faith/cabinet/old.jpg', at: ago(8 * 86400e3) });
  const m = imagesFor(['err-fresh', 'err-old', 'nf-yesterday', 'ok-old'], { deps, lane: 'test' });
  assert.equal(m.get('err-fresh').state, 'error', '30 分たっていない error = 積まない');
  assert.equal(m.get('err-old').state, 'pending');
  assert.equal(m.get('nf-yesterday').state, 'pending', '前の日の not_found = 取り直す');
  assert.deepEqual(m.get('ok-old'), { url: 'https://image.rakuten.co.jp/b-faith/cabinet/old.jpg', state: 'ok' }, '古い画像は出したまま裏で取り直す');
  await drain();
  assert.equal(calls.length, 1);
  assert.equal(db.prepare('SELECT status FROM pk_product_images WHERE ne_code = ?').get('err-fresh').status, 'error', '積まなかった行はそのまま');
});

await t('notFoundRetryMs: 「楽天に無い」を取り直すまでの間を長くできる (マスタの一覧 = 7 日)', async () => {
  calls.length = 0;
  put('nf-2days', { status: 'not_found', at: ago(2 * 86400e3) });
  put('nf-8days', { status: 'not_found', at: ago(8 * 86400e3) });
  const m = imagesFor(['nf-2days', 'nf-8days'], { deps, lane: 'nf', notFoundRetryMs: 7 * 86400e3 });
  assert.equal(m.get('nf-2days').state, 'not_found', '2 日前の「無い」= まだ聞き直さない');
  assert.equal(m.get('nf-8days').state, 'pending', '8 日前 = 聞き直す');
  await drain();
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], ['nf-8days']);
  // 指定しない = ピッキングと同じ (JST の翌日)
  const m2 = imagesFor(['nf-2days'], { deps, lane: 'nf' });
  assert.equal(m2.get('nf-2days').state, 'pending');
  await drain();
});

await t('1 回に積むのは queueMax 件まで (画像の無い物が先)・同じ lane は 1 つまで・canQueue=false は積まない', async () => {
  calls.length = 0;
  put('ok-old2', { white: 'https://image.rakuten.co.jp/b-faith/cabinet/old2.jpg', at: ago(8 * 86400e3) });
  const codes = ['ok-old2', ...Array.from({ length: 8 }, (_, i) => `cap-${i}`)];
  const m = imagesFor(codes, { deps, lane: 'cap', queueMax: 5 });
  const pending = codes.filter((c) => m.get(c).state === 'pending');
  assert.deepEqual(pending, ['cap-0', 'cap-1', 'cap-2', 'cap-3', 'cap-4'], '上限まで・画像の無い物から');
  assert.equal(m.get('cap-5').state, 'uncached', '上限を超えた分 = まだ取っていない (次に開いたとき)');
  assert.equal(m.get('ok-old2').state, 'ok');
  // 同じ lane の待ちジョブがある間は積まない (一覧を何度開いても積み増さない)
  const m2 = imagesFor(codes, { deps, lane: 'cap', queueMax: 5 });
  assert.equal(m2.get('cap-0').state, 'pending', '積んだ物は取得中');
  assert.equal(m2.get('cap-6').state, 'uncached', '待ちジョブがある間は新しく積まない');
  await drain();
  assert.equal(calls.length, 1, `楽天は 1 回 (${calls.length})`);
  assert.equal(calls[0].length, 5);
  // canQueue=false (楽天につながらない環境) = 積まない
  calls.length = 0;
  const m3 = imagesFor(['nq-1'], { deps, lane: 'cap', canQueue: false });
  assert.equal(m3.get('nq-1').state, 'uncached');
  // queueMax=0 (列を出していない人) = 積まない
  const m4 = imagesFor(['nq-2'], { deps, lane: 'cap', queueMax: 0 });
  assert.equal(m4.get('nq-2').state, 'uncached');
  await drain();
  assert.equal(calls.length, 0);
  // 空きができたら残りを積む
  const m5 = imagesFor(codes, { deps, lane: 'cap', queueMax: 5 });
  assert.deepEqual(codes.filter((c) => m5.get(c).state === 'pending'), ['cap-5', 'cap-6', 'cap-7']);
  assert.equal(m5.get('ok-old2').state, 'ok', '古い画像は出したまま');
  await drain();
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].length, 4, '残りの 3 件 + 古い画像の 1 件 (空きがあるので取り直す)');
});

await t('maxCodes: 楽天に投げるコード (probe の候補を含む) を予算に収める・外した SKU は台帳に書かない (次の回へ)', async () => {
  calls.length = 0;
  // 変換表にも mirror にも無い = 候補を投げて確かめる。'mc-a-b-c' 型 = 候補 3 つ (mc-a-b-c / mc-a-b / mc-a は 4 字以上)
  const codes = Array.from({ length: 6 }, (_, i) => `mc${i}x-aa-bb`);
  const m = imagesFor(codes, { deps, lane: 'mc', maxCodes: 7 });
  assert.equal(codes.filter((c) => m.get(c).state === 'pending').length, 6, '画面には全部「取得中」');
  await drain();
  assert.equal(calls.length, 1);
  assert.ok(calls[0].length <= 7, `予算 7 に収める (${calls[0].length})`);
  const written = codes.filter((c) => db.prepare('SELECT 1 FROM pk_product_images WHERE ne_code = ?').get(c));
  assert.equal(written.length, 2, `3 候補 × 2 SKU = 6 ≦ 7 (${written})`);
  // 外した SKU = まだ取っていない (次に開いたときに積む)
  const m2 = imagesFor(codes, { deps, lane: 'mc', maxCodes: 7 });
  assert.equal(codes.filter((c) => m2.get(c).state === 'pending').length, 4);
  assert.equal(codes.filter((c) => m2.get(c).state === 'not_found').length, 2);
  await drain();
  // 予算より大きい 1 SKU でも止まらない (最初の 1 つは必ず解決する)
  calls.length = 0;
  imagesFor(['big0x-aa-bb'], { deps, lane: 'mc', maxCodes: 1 });
  await drain();
  assert.equal(calls.length, 1);
  assert.ok(db.prepare('SELECT 1 FROM pk_product_images WHERE ne_code = ?').get('big0x-aa-bb'));
});

await t('空・null の並びでも壊れない', async () => {
  assert.equal(imagesFor([]).size, 0);
  assert.equal(imagesFor(null).size, 0);
  assert.equal(imagesFor(['', null, '  ']).size, 0);
});

console.log(`\nimagesFor: ${passed} 件 ok`);
