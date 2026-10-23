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
// 2 段目: 撮った写真を足す (本物は Company DB の ops.add_sku_packing_photo。ここは同じ答えの形を真似る)
const posted = [];          // 届いた順 { request_id, code, caption, actor }
let postMode = 'ok';        // ok / down (500) / full (409 photos_full)
const seenReq = new Map();  // request_id → 答え (押し直し = 前の答え)
let nextPhotoId = 100;
fake.post('/api/photos', express.json({ limit: '20mb' }), (req, res) => {
  const b = req.body;
  posted.push({ request_id: b.request_id, code: b.code, caption: b.caption, actor: b.actor });
  if (postMode === 'down') return res.status(500).json({ ok: false, error: 'Render が落ちている' });
  if (seenReq.has(b.request_id)) return res.json({ ...seenReq.get(b.request_id), replay: true });
  if (postMode === 'full') return res.status(409).json({ ok: false, reason: 'photos_full', error: '写真はもう 3 枚あります。どれかを外してから足してください' });
  const buf = Buffer.from(b.image, 'base64');
  const id = String(nextPhotoId++);
  PHOTOS.set(id, buf);
  let g = manifest.guides.find((x) => x.code.toLowerCase() === String(b.code).toLowerCase());
  if (!g) { g = { code: b.code, sku_id: id, name: `撮った商品 ${b.code}`, note: '', event_id: id, photos: [] }; manifest.guides.push(g); }
  g.photos.push({ photo_id: id, sha256: sha(buf), position: g.photos.length + 1, caption: b.caption || '', width: 10, height: 10 });
  const ans = { ok: true, code: b.code, photo_id: id, position: g.photos.length, replay: false };
  seenReq.set(b.request_id, ans);
  res.json(ans);
});
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

await ta('[G4] 鍵を外した (取りに行かない) = 写しがあっても出さない (古さの知らせなしに古い指示を出し続けない)', async () => {
  assert.equal(GS.guidesFor(['sku1']).size, 1);
  delete process.env.PACKING_GUIDE_KEY;
  try {
    assert.equal(GS.guidesFor(['sku1']).size, 0);
    assert.deepEqual(GS.guideSyncStatus(), { enabled: false });
  } finally { process.env.PACKING_GUIDE_KEY = 'k1'; }
  assert.equal(GS.guidesFor(['sku1']).size, 1);
});

await ta('[G5] 誤りを残せない (SQLite に書けない) ときも投げない = ポーラーから standalone を落とさない', async () => {
  db.exec(`CREATE TRIGGER t_meta_full_i BEFORE INSERT ON pk_pack_meta BEGIN SELECT RAISE(FAIL, 'database or disk is full'); END;
           CREATE TRIGGER t_meta_full_u BEFORE UPDATE ON pk_pack_meta BEGIN SELECT RAISE(FAIL, 'database or disk is full'); END;`);
  const url = process.env.PACKING_GUIDE_URL;
  try {
    process.env.PACKING_GUIDE_URL = 'http://example.com/api';   // 取れない → 誤りを残そうとして書けない
    const r = await GS.pullGuides();
    assert.equal(r.ok, false);
    assert.match(r.error, /https ではありません/);
    process.env.PACKING_GUIDE_URL = url;
    const r2 = await GS.pullGuides();   // 取れても「取れた時刻」を書けない = 誤りとして返す (投げない)
    assert.equal(r2.ok, false);
  } finally {
    process.env.PACKING_GUIDE_URL = url;
    db.exec('DROP TRIGGER t_meta_full_i; DROP TRIGGER t_meta_full_u;');
  }
  assert.equal((await GS.pullGuides()).ok, true);
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

// ─── 2 段目: 撮って登録 ───
// 作業者の名簿 (picking の表・同じ picking.db)
db.exec(`CREATE TABLE IF NOT EXISTS pk_workers (code TEXT PRIMARY KEY, name TEXT NOT NULL, sort INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1)`);
db.prepare(`INSERT OR IGNORE INTO pk_workers (code, name) VALUES ('w01', '三宅晴菜')`).run();
const shot = await sharp({ create: { width: 2000, height: 1500, channels: 3, background: { r: 250, g: 180, b: 20 } } }).jpeg().toBuffer();
const upload = async (fields, file = shot) => {
  const fd = new FormData();
  for (const [k, v] of Object.entries(fields)) fd.append(k, v);
  if (file) fd.append('photo', new Blob([file], { type: 'image/jpeg' }), 'g.jpg');
  const r = await fetch(`${base}/apps/packing/api/guide-photos`, { method: 'POST', body: fd });
  return { status: r.status, j: await r.json().catch(() => ({})) };
};
const upRow = (op) => db.prepare('SELECT * FROM pk_pack_guide_uploads WHERE op_id = ?').get(op);

await ta('[U1] 撮った写真を登録: 送り待ちに残してすぐ送る (sent・何枚目)・写しを取り直して出る・押し直し (同じ op_id) = 2 枚にならない・記録の人 = 作業者', async () => {
  const r = await upload({ op_id: 'op-u1-aaaaaaa', sku: 'sku2', caption: '底に段ボール', worker_name: '三宅晴菜', batch_id: '1' });
  assert.equal(r.status, 200, JSON.stringify(r.j));
  assert.deepEqual([r.j.ok, r.j.status, r.j.position], [true, 'sent', 1]);
  const row = upRow('op-u1-aaaaaaa');
  assert.equal(row.status, 'sent');
  assert.ok(fs.existsSync(path.join(GS.GUIDE_UPLOAD_DIR, row.file)), '撮った写真のファイルは残る');
  assert.deepEqual(posted.at(-1), { request_id: row.request_id, code: 'sku2', caption: '底に段ボール', actor: '三宅晴菜' });
  assert.equal(GS.guidesFor(['sku2']).get('sku2').photos[0].caption, '底に段ボール', '送れたら写しを取り直す');
  const n = posted.length;
  const again = await upload({ op_id: 'op-u1-aaaaaaa', sku: 'sku2', caption: '底に段ボール', worker_name: '三宅晴菜', batch_id: '1' });
  assert.deepEqual([again.j.status, again.j.position], ['sent', 1]);
  assert.equal(posted.length, n, '送れた行は送り直さない');
});

await ta('[U2] つながらない = 送り待ち (pending・間をあけて送り直す・同じ request_id)・断られた = failed (送り直さない・理由)・形の誤り = 400', async () => {
  postMode = 'down';
  const r = await upload({ op_id: 'op-u2-bbbbbbb', sku: 'sku2', caption: '', worker_name: '三宅晴菜', batch_id: '1' });
  assert.deepEqual([r.status, r.j.ok, r.j.status], [200, true, 'pending']);
  let row = upRow('op-u2-bbbbbbb');
  assert.deepEqual([row.status, row.attempts], ['pending', 1]);
  assert.match(row.last_error, /HTTP 500/);
  assert.ok(Date.parse(row.next_at) > Date.now(), '間をあける');
  assert.deepEqual(await GS.sendGuideUploads(), { sent: 0, failed: 0, pending: 0 }, '時刻の前は送らない');
  postMode = 'ok';
  db.prepare(`UPDATE pk_pack_guide_uploads SET next_at = ? WHERE op_id = ?`).run('2000-01-01T00:00:00Z', 'op-u2-bbbbbbb');
  const reqIds = posted.filter((p) => p.request_id === row.request_id).length;
  assert.deepEqual(await GS.sendGuideUploads(), { sent: 1, failed: 0, pending: 0 });
  assert.equal(posted.filter((p) => p.request_id === row.request_id).length, reqIds + 1, '同じ request_id で送り直す');
  assert.equal(upRow('op-u2-bbbbbbb').position, 2);
  // 断られた (もう 3 枚ある) = failed
  postMode = 'full';
  const f = await upload({ op_id: 'op-u2-ccccccc', sku: 'sku2', caption: '', worker_name: '三宅晴菜', batch_id: '1' });
  assert.deepEqual([f.j.ok, f.j.status], [false, 'failed']);
  assert.match(f.j.error, /3 枚/);
  assert.equal(upRow('op-u2-ccccccc').status, 'failed');
  assert.deepEqual(await GS.sendGuideUploads(), { sent: 0, failed: 0, pending: 0 }, '断られたものは送り直さない');
  assert.match(GS.guideUploadsFor(['sku2']).sku2.failed.at(-1).error, /3 枚/);
  postMode = 'ok';
  // 形の誤り
  assert.equal((await upload({ op_id: 'op-u2-ddddddd', sku: 'sku2', worker_name: '知らない人', batch_id: '1' })).status, 400);
  assert.equal((await upload({ op_id: 'x', sku: 'sku2', worker_name: '三宅晴菜' })).status, 400);
  assert.equal((await upload({ op_id: 'op-u2-eeeeeee', sku: '', worker_name: '三宅晴菜' })).status, 400);
  assert.equal((await upload({ op_id: 'op-u2-fffffff', sku: 'sku2', worker_name: '三宅晴菜' }, Buffer.from('<svg></svg>'.padEnd(64, ' ')))).status, 400);
  assert.equal((await upload({ op_id: 'op-u2-ggggggg', sku: 'sku2', worker_name: '三宅晴菜', caption: 'あ'.repeat(101) })).status, 400);
  assert.equal(upRow('op-u2-ddddddd'), undefined, '断ったら送り待ちに残さない');
});

await ta('[V6] 開きっぱなしの画面: 古さは「今」から数え直す (開いたときは新しくても、時間がたてば知らせる)・取り直しで新しい指示が出る', async () => {
  await GS.pullGuides();
  await open();
  await goSlip(1);
  assert.doesNotMatch(await page.locator('.guide-box').first().innerText(), /分前に取った/);
  // 開いてから 2 時間たった (端末の時計を進める代わりに、取れた時刻を戻して描き直す)
  await page.evaluate(() => { STATE.guideSync.okAt = new Date(Date.now() - 120 * 60000).toISOString(); render(); });
  assert.match(await page.locator('.guide-box').first().innerText(), /120 分前に取ったもの/);
  // 商品の画面で注意点が変わった → 写しを取り直す → 開いたままの画面も取り直しで出る
  manifest.guides.find((g) => g.code === 'SKU1').note = '新しい注意点';
  await GS.pullGuides();
  await page.evaluate(() => refreshGuides({ force: false }));
  await page.waitForFunction(() => /新しい注意点/.test(document.querySelector('.guide-box')?.innerText || ''), null, { timeout: 10000 });
  assert.doesNotMatch(await page.locator('.guide-box').first().innerText(), /分前に取った/, '取り直した = 新しい');
});

await ta('[V5] 画面で撮って登録: 明細の「📷 梱包写真を登録」→ カメラ → 確かめる (一言) → 登録 → 指示の枠に出る・セットの明細 = 構成品を選ぶ', async () => {
  await open();
  await goSlip(2);
  assert.equal(await page.locator('.guide-cam').count(), 1);
  const shotFile = path.join(tmp, 'shot.jpg');
  fs.writeFileSync(shotFile, await sharp({ create: { width: 1200, height: 900, channels: 3, background: { r: 20, g: 120, b: 250 } } }).jpeg().toBuffer());
  const n = posted.length;
  await page.locator('.guide-cam').click();
  await page.setInputFiles('#guideCamInput', shotFile);
  await page.waitForSelector('#guideCamOverlay', { state: 'visible' });
  await page.fill('#guideCamCaption', '箱の向きに注意');
  await page.locator('#guideCamOk').click();
  await page.waitForFunction(() => document.getElementById('guideCamOverlay').style.display === 'none', null, { timeout: 15000 });
  await page.waitForFunction(() => /箱の向きに注意/.test(document.querySelector('.guide-box')?.innerText || ''), null, { timeout: 15000 });
  assert.equal(posted.length, n + 1);
  assert.equal(posted.at(-1).caption, '箱の向きに注意');
  assert.match(await page.locator('.guide-box').innerText(), /箱の向きに注意/);
  // セットの明細 = どの構成品か選ぶ
  await goSlip(3);
  await page.locator('.guide-cam').click();
  assert.equal(await page.locator('#guidePickOverlay').isVisible(), true);
  assert.equal(await page.locator('.gpick-opt').count(), 2);
  await page.locator('#guidePickCancel').click();
  assert.equal(await page.locator('#guidePickOverlay').isVisible(), false);
});

await browser.close();
server.close();
fakeServer.close();
console.log(`${passed} passed`);
process.exit(process.exitCode || 0);
