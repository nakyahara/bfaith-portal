import { temporaryTestRoot } from '../../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-guide.mjs — 梱包指示 (入れ方の写真・注意点) の写しと、梱包の画面の出し方 (中原さん 2026-10-23・正 = Render の Company DB の 0101)
 *
 * 見たいこと:
 *   ① 写し (guide-sync.js): Render の配りの口 (ここでは試しのサーバー) から一覧と写真を取り、sha256 を確かめて SQLite とファイルに入れる。
 *      写真の中身が一覧と違う・つながらない = 前の写しのまま (梱包を止めない)・鍵が無い = 取りに行かない
 *   ② 梱包の画面: 指示がある商品の伝票だけ橙の枠 (注意点・番号つきの写真・一言)・明細に「梱包指示あり」・写真を押すと大きく見る (前 / 次 / 閉じる)・
 *      セットを展開した構成品の指示も出る・写しが古い = 小さく知らせる・写真の配信 (sha256 の名前だけ)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'guide-'));
process.env.DATA_DIR = tmp;
process.env.PACKING_ENABLED = '1';
delete process.env.PACKING_GUIDE_KEY;

const express = (await import('express')).default;
const { default: sharp } = await import('sharp');
const { initPackingDB, getDB, utcNow } = await import('../db.js');
const GS = await import('../guide-sync.js');
const router = (await import('../router.js')).default;

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('梱包指示の写しと梱包の画面');

// ─── 試しの Render (配りの口) ───
const jpeg = (color) => sharp({ create: { width: 64, height: 48, channels: 3, background: color } }).jpeg().toBuffer();
const PHOTOS = new Map([['11', await jpeg({ r: 200, g: 40, b: 40 })], ['12', await jpeg({ r: 40, g: 200, b: 40 })], ['21', await jpeg({ r: 40, g: 40, b: 200 })]]);
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
let manifest = {
  ok: true,
  guides: [
    { code: 'SKU1', sku_id: '1', name: 'ガラスの花瓶', note: 'ワレモノ。\nプチプチ 2 重で', event_id: '5', updated_at: '2026-10-23T01:00:00Z', updated_by: 'naka@test',
      photos: [{ photo_id: '11', sha256: sha(PHOTOS.get('11')), position: 1, caption: '底に緩衝材', width: 64, height: 48 },
        { photo_id: '12', sha256: sha(PHOTOS.get('12')), position: 2, caption: '立てて入れる', width: 64, height: 48 }] },
    { code: 'comp-b', sku_id: '2', name: '構成品 B', note: '説明書を一番上に', event_id: '6', photos: [] },
  ],
};
let photoOverride = null;
const fake = express();
let calls = 0;
fake.post('/service-api/ne-products/names', express.json(), (req, res) => res.json({ names: { 'set-x': 'セット X の単品名' }, components: { 'set-x': [{ sku: 'comp-a', name: '構成品 A', qty: 1 }, { sku: 'comp-b', name: '構成品 B', qty: 1 }] } }));
fake.use((req, res, next) => { calls++; if (req.headers['x-packing-guide-key'] !== 'k1') return res.status(401).json({ ok: false, error: 'bad key' }); next(); });
fake.get('/api/manifest', (req, res) => res.json(manifest));
fake.get('/api/photos/:id', (req, res) => { const b = photoOverride?.[req.params.id] || PHOTOS.get(req.params.id); if (!b) return res.status(404).end(); res.type('image/jpeg').send(b); });
const fakeServer = http.createServer(fake);
await new Promise((r) => fakeServer.listen(0, '127.0.0.1', r));
process.env.PACKING_GUIDE_URL = `http://127.0.0.1:${fakeServer.address().port}/api`;
// 商品名とセットの展開 (warehouse の service-api の代わり = 同じ試しのサーバー)
process.env.WAREHOUSE_URL = `http://127.0.0.1:${fakeServer.address().port}`;
process.env.WAREHOUSE_SERVICE_TOKEN = 't';

// ─── 梱包のバッチ (伝票 1 = 指示のある商品・伝票 2 = 無い商品・伝票 3 = セット (構成品 B に指示)) ───
initPackingDB();
const db = getDB();
const now = utcNow();
db.prepare(`INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count,
  total_qty, match_status, status, worker, csv_sha256, imported_by, created_at, updated_at)
  VALUES (1,'TB1','出荷_02',?,3,3,3,'ok','packing','三宅晴菜','x','t',?,?)`).run(now.slice(0, 10), now, now);
for (const [i, sku] of [[1, 'sku1'], [2, 'sku2'], [3, 'set-x']]) {
  db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status, delivery_method)
    VALUES (1,?,?,?,?,?,'pending','箱 陸便 元払い 営業所止めなし')`).run(i, `15389${50 + i}`, `SP${i}`, `お客さま${i}`, `503-000-${i}`);
  db.prepare(`INSERT INTO pk_pack_lines (slip_id, sku, product_name, qty) SELECT id, ?, ?, 1 FROM pk_pack_slips WHERE batch_id=1 AND seq=?`).run(sku, `商品${i}`, i);
}

await ta('[G1] 鍵が無い = 取りに行かない (梱包指示は出ない)・取得の様子 = enabled false', async () => {
  const r = await GS.pullGuides();
  assert.deepEqual(r, { ok: false, skipped: 'disabled' });
  assert.deepEqual(GS.guideSyncStatus(), { enabled: false });
  assert.equal(calls, 0);
  assert.equal(GS.startGuidePoller(), false);
});

process.env.PACKING_GUIDE_KEY = 'k1';

await ta('[G2] 取る: 一覧と写真 (sha256 を確かめて <sha>.jpg)・SQLite に丸ごと・引く (コードの大小・空白は同じに見る)', async () => {
  const r = await GS.pullGuides();
  assert.deepEqual([r.ok, r.count, r.fetched], [true, 2, 2], JSON.stringify(r));
  for (const id of ['11', '12']) assert.ok(fs.existsSync(GS.guidePhotoFile(sha(PHOTOS.get(id)))), id);
  const g = GS.guidesFor([' sku1 ', 'SKU2', 'COMP-B']);
  assert.deepEqual([...g.keys()].sort(), ['comp-b', 'sku1']);
  const s1 = g.get('sku1');
  assert.equal(s1.note, 'ワレモノ。\nプチプチ 2 重で');
  assert.deepEqual(s1.photos.map((p) => [p.position, p.caption, p.url]), [[1, '底に緩衝材', `/apps/packing/guide-photos/${sha(PHOTOS.get('11'))}.jpg`], [2, '立てて入れる', `/apps/packing/guide-photos/${sha(PHOTOS.get('12'))}.jpg`]]);
  const st = GS.guideSyncStatus();
  assert.deepEqual([st.enabled, st.stale, st.count, st.error], [true, false, 2, null]);
  // 2 回目 = 写真は取り直さない
  const r2 = await GS.pullGuides();
  assert.deepEqual([r2.ok, r2.fetched], [true, 0]);
});

await ta('[G3] 写真の中身が一覧と違う・つながらない・鍵が違う・形が違う = 前の写しのまま (誤りを残す)', async () => {
  const before = JSON.stringify([...GS.guidesFor(['sku1', 'comp-b'])]);
  const keep = manifest;
  manifest = { ...keep, guides: [...keep.guides, { code: 'sku9', sku_id: '9', name: '新しい', note: '', event_id: '9', photos: [{ photo_id: '21', sha256: 'f'.repeat(64), position: 1, caption: '', width: 1, height: 1 }] }] };
  let r = await GS.pullGuides();
  assert.equal(r.ok, false);
  assert.match(r.error, /中身が一覧と違います/);
  assert.equal(JSON.stringify([...GS.guidesFor(['sku1', 'comp-b'])]), before);
  assert.equal(GS.guidesFor(['sku9']).size, 0, '半端な版は出さない');
  assert.match(GS.guideSyncStatus().error, /中身が一覧と違います/);
  manifest = { ok: true, guides: [{ code: 'a', photos: [] }, { code: 'A', photos: [] }] };
  r = await GS.pullGuides();
  assert.match(r.error, /重なって/);
  manifest = keep;
  process.env.PACKING_GUIDE_KEY = 'wrong';
  r = await GS.pullGuides();
  assert.match(r.error, /HTTP 401/);
  process.env.PACKING_GUIDE_KEY = 'k1';
  const port = process.env.PACKING_GUIDE_URL;
  process.env.PACKING_GUIDE_URL = 'http://example.com/api';
  r = await GS.pullGuides();
  assert.match(r.error, /https ではありません/);
  process.env.PACKING_GUIDE_URL = port;
  assert.equal(JSON.stringify([...GS.guidesFor(['sku1', 'comp-b'])]), before);
  assert.equal((await GS.pullGuides()).ok, true);
  assert.equal(GS.guideSyncStatus().error, null);
});

// ─── 梱包の画面 (本物の router・実ブラウザ) ───
const app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'views'));
app.use(express.json());
app.use((req, res, next) => { req.session = { email: 't@x', role: 'admin', allowedApps: '*' }; next(); });
app.use('/apps/packing', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 820, height: 1100 } });
const open = async () => {
  await page.goto(`${base}/apps/packing/work/1`, { waitUntil: 'networkidle' });
  await page.waitForFunction(() => (document.getElementById('slipNo')?.textContent || '').trim() !== '—', null, { timeout: 15000 });
  const w = page.locator('#workerOverlay');
  if (await w.isVisible()) { await page.evaluate(() => window.begin('三宅晴菜')); await page.waitForTimeout(300); }
};
const goSlip = async (seq) => { await page.evaluate((q) => { viewSeq = q; render(); }, seq); };

await ta('[V1] 指示のある伝票: 橙の枠 (商品名・注意点・番号つきの写真・一言)・明細に「梱包指示あり」・古くない = 知らせない', async () => {
  await open();
  await goSlip(1);
  const box = page.locator('.guide-box');
  assert.equal(await box.count(), 1);
  const text = await box.innerText();
  assert.match(text, /梱包指示/);
  assert.match(text, /ガラスの花瓶/);
  assert.match(text, /ワレモノ。\nプチプチ 2 重で/);
  assert.match(text, /底に緩衝材/);
  assert.doesNotMatch(text, /分前に取った/);
  assert.equal(await page.locator('.guide-ph').count(), 2);
  assert.equal(await page.locator('.guide-tag').count(), 1);
  // 写真が実際に出ている (読み込めた)
  await page.waitForFunction(() => [...document.querySelectorAll('.guide-ph img')].every((i) => i.complete && i.naturalWidth > 0), null, { timeout: 10000 });
});

await ta('[V2] 写真を押すと大きく見る (番号・一言・前 / 次・閉じる)・開いている間はめくらない', async () => {
  await page.locator('.guide-ph').nth(1).click();
  assert.equal(await page.locator('#guideOverlay').isVisible(), true);
  assert.equal(await page.locator('#guideBigCap').innerText(), '立てて入れる');
  assert.equal(await page.locator('#guideBigNo').innerText(), '2');
  assert.equal(await page.locator('#guideBigNext').isDisabled(), true);
  await page.locator('#guideBigPrev').click();
  assert.equal(await page.locator('#guideBigCap').innerText(), '底に緩衝材');
  assert.equal(await page.locator('#guideBigPrev').isDisabled(), true);
  await page.locator('#guideBigClose').click();
  assert.equal(await page.locator('#guideOverlay').isVisible(), false);
  assert.equal((await page.locator('#slipNo').innerText()).trim(), '1538951', '伝票は変わらない');
});

await ta('[V3] 指示の無い伝票 = 枠を出さない・セットの伝票 = 構成品の指示を出す', async () => {
  await goSlip(2);
  assert.equal(await page.locator('.guide-box').count(), 0);
  assert.equal(await page.locator('.guide-tag').count(), 0);
  await goSlip(3);
  assert.equal(await page.locator('.guide-box').count(), 1);
  assert.match(await page.locator('.guide-box').innerText(), /説明書を一番上に/);
  assert.match(await page.locator('.guide-box').innerText(), /構成品 B/);
});

await ta('[V4] 写しが古い (30 分を超えた) = 枠の下に小さく知らせる・写真の配信 (sha256 の名前だけ・無い = 404)', async () => {
  db.prepare(`UPDATE pk_pack_meta SET value = ? WHERE key = 'guide_last_ok_at'`).run(new Date(Date.now() - 95 * 60000).toISOString().slice(0, 19) + 'Z');
  await open();
  await goSlip(1);
  assert.match(await page.locator('.guide-box').innerText(), /95 分前に取ったもの/);
  const ok = await fetch(`${base}/apps/packing/guide-photos/${sha(PHOTOS.get('11'))}.jpg`);
  assert.equal(ok.status, 200);
  assert.equal(ok.headers.get('content-type'), 'image/jpeg');
  assert.equal(sha(Buffer.from(await ok.arrayBuffer())), sha(PHOTOS.get('11')));
  assert.equal((await fetch(`${base}/apps/packing/guide-photos/${'0'.repeat(64)}.jpg`)).status, 404);
  assert.equal((await fetch(`${base}/apps/packing/guide-photos/..%2Fpicking.db`)).status, 404);
});

await browser.close();
server.close();
fakeServer.close();
console.log(`${passed} passed`);
process.exit(process.exitCode || 0);
