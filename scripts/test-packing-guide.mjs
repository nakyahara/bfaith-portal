/**
 * test-packing-guide.mjs — 梱包指示 = 入れ方の写真 (3 枚まで・順番と一言) と注意点の文 (中原さん 2026-10-23・Company DB の 0101・lib/packing-guide.mjs)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 全部の migration を流した新しい DB)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   D 0101 の形: 表・関数 (security definer・search_path・public の実行権なし)・守り (関数の外では書けない・記録は追記だけ・写真の中身は変えない・消さない・TRUNCATE できない)・
 *     画面のロール master_edit = 3 つの表の select と 2 つの関数の実行だけ (中の部品・守りは持ち主だけ)
 *   W 書く: 写真を足す (後ろへ・3 枚まで・押し直し = 前の答え・JPEG だけ) / 保存 (注意点・並べ替え・一言・外す・版が違う = version_conflict・ほかの商品の写真 = unknown_photo・
 *     何も変えない = 記録なし・空の指示は行を作らない)・登録をやめた商品・形の誤り
 *   P 写真の作り直し (sharp): 向きを直す・長辺 1280px・JPEG・メタデータを捨てる・読めない = 400
 *   S 商品の画面: 「梱包指示」の節・写真を足す (名簿の人だけ・Origin・大きい本文の道)・保存・写真を出す (この商品の写真だけ)・セット = 節を出さない
 *   X 配り (梱包アプリの写し): 指示のある単品だけ・写真の番号と sha256・鍵 (x-packing-guide-key) が無い / 違う = 401・設定が無い = 503
 *   R ロールの流し直しの前: 節を出さない・書き込みは 503
 *   B バックアップ: bytea の列のある表は小分けに読む・入れる (中身はそのまま戻る)
 * 使い方: node scripts/test-packing-guide.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pgd-'));

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const PG = await import('../lib/packing-guide.mjs');
const ROLES = await import('./company-db/create-master-edit-roles.mjs');
const { skuOf } = await import('./fixtures/master-variation-db.mjs');
// セット (梱包指示を持たない) を 1 つ足しておく
const T = await setupVariationDb({ extraSkus: [skuOf('pgset1', '試しのセット', { kind: 'set' })] });
const { pg, db, q, one, asEditor } = T;
const { default: sharp } = await import('sharp');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

// ─── 本物の router (商品の画面) を HTTP 越しに ───
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/pgd';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/pgd';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const MR = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
MR.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
MR.__setShippingRatesProvider(async () => new Map());
MR.__setPhDraftLookup(async () => null);
const app = express();
app.set('view engine', 'ejs');
const SESS = { editor: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] },
  viewer: { authenticated: true, email: 'see@test', displayName: '見る人', role: 'user', allowedApps: ['master-edit'] } };
app.use((req, res, next) => { req.session = SESS[req.headers['x-test-session'] || 'editor']; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'editor', origin = true, raw = false } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(ORIGIN + '/apps/master-edit' + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(60e3) });
  if (raw) return { status: r.status, type: r.headers.get('content-type'), cache: r.headers.get('cache-control'), buf: Buffer.from(await r.arrayBuffer()) };
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}

const uuid = () => crypto.randomUUID();
const skuIdOf = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;
/** 試しの写真 (色の付いた JPEG・w×h) */
const jpeg = (w = 64, h = 48, color = { r: 200, g: 80, b: 40 }) => sharp({ create: { width: w, height: h, channels: 3, background: color } }).jpeg().toBuffer();
const photoOf = async (w, h, color) => PG.normalizePhoto(await jpeg(w, h, color));
const eventsOf = async (code) => q(`select e.action, e.via, e.actor_id, e.from_value as f, e.to_value as t from ops.sku_packing_guide_events e join core.skus k on k.sku_id = e.sku_id
  where k.code = $1 order by e.event_id`, [code]);
const add = (code, skuId, photo, x = {}) => asEditor(() => PG.addPackingPhoto(db, { requestId: uuid(), actor: 'naka@test', via: 'sku_page', skuId, photo, ...x }));
const save = (skuId, x) => asEditor(() => PG.savePackingGuide(db, { requestId: uuid(), actor: 'naka@test', via: 'sku_page', skuId, ...x }));

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('梱包指示 (0101)');

await ta('[D1] 0101 の形: 表・関数 (security definer・search_path 固定・public の実行権なし)・画面のロールの権限 (読む・2 つの関数だけ)・ほかのロールは読めない', async () => {
  for (const t of PG.GUIDE_TABLES) assert.ok((await one('select to_regclass($1) is not null as ok', [t])).ok, t);
  for (const name of ['add_sku_packing_photo', 'set_sku_packing_guide']) {
    const f = await one(`select p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p
      join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname = $1`, [name]);
    assert.deepEqual([f.d, f.pub], [true, false], name);
    assert.match(f.c, /search_path=pg_catalog, ?pg_temp/, name);
  }
  for (const t of ROLES.PACKING_GUIDE_SELECT) {
    assert.equal((await one(`select has_table_privilege('master_edit', $1, 'select') as s`, [t])).s, true, t);
    for (const p of ['insert', 'update', 'delete', 'truncate']) assert.equal((await one(`select has_table_privilege('master_edit', $1, $2) as s`, [t, p])).s, false, `${t} ${p}`);
    for (const r of ['master_ops', 'master_observer', 'master_gate']) assert.equal((await one(`select has_table_privilege($1, $2, 'select') as s`, [r, t])).s, false, `${r} が ${t} を読める`);
  }
  for (const fn of ROLES.PACKING_GUIDE_EDIT_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [fn])).ok, true, fn);
  for (const fn of ROLES.PACKING_GUIDE_OWNER_ONLY_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [fn])).ok, false, fn);
  assert.deepEqual(Object.values(PG.GUIDE_FUNCTIONS), [...ROLES.PACKING_GUIDE_EDIT_FUNCTIONS]);
  assert.deepEqual(PG.GUIDE_TABLES, [...ROLES.PACKING_GUIDE_SELECT]);
  assert.equal(await asEditor(() => PG.packingGuideReady(db)), true);
});

await ta('[W1] 写真を足す: 後ろへ・3 枚まで (4 枚目 = photos_full)・押し直し = 前の答え・記録 (add_photo・前後の指示)', async () => {
  const sku = await skuIdOf('s001');
  const p1 = await photoOf(64, 48), p2 = await photoOf(48, 64, { r: 10, g: 200, b: 10 }), p3 = await photoOf(30, 30, { r: 10, g: 10, b: 200 });
  const a1 = await add('s001', sku, p1, { caption: '底に緩衝材' });
  assert.deepEqual([a1.ok, a1.position, a1.replay, a1.sha256], [true, 1, false, p1.sha256]);
  const rid = uuid();
  const a2 = await asEditor(() => PG.addPackingPhoto(db, { requestId: rid, actor: 'packing:山田', via: 'packing', skuId: sku, photo: p2 }));
  assert.equal(a2.position, 2);
  const again = await asEditor(() => PG.addPackingPhoto(db, { requestId: rid, actor: 'packing:山田', via: 'packing', skuId: sku, photo: p2 }));
  assert.deepEqual([again.replay, again.photo_id, again.position, again.event_id], [true, a2.photo_id, 2, a2.event_id], '押し直し = 前の答え (2 枚目を作らない)');
  await add('s001', sku, p3);
  await assert.rejects(() => add('s001', sku, p1), (e) => e.status === 409 && e.reason === 'photos_full');
  const g = await asEditor(() => PG.readPackingGuide(db, sku));
  assert.deepEqual(g.photos.map((p) => [p.position, p.caption, p.width, p.height]), [[1, '底に緩衝材', 64, 48], [2, '', 48, 64], [3, '', 30, 30]]);
  assert.equal(g.note, '');
  assert.equal(g.updated_via, 'sku_page');
  const ev = await eventsOf('s001');
  assert.deepEqual(ev.map((e) => [e.action, e.via, e.actor_id]), [['add_photo', 'sku_page', 'naka@test'], ['add_photo', 'packing', 'packing:山田'], ['add_photo', 'sku_page', 'naka@test']]);
  assert.equal(ev[0].f, null);
  assert.equal(ev[1].t.photos.length, 2);
  assert.equal(String(g.event_id), String((await one(`select max(event_id)::text as e from ops.sku_packing_guide_events where sku_id = $1`, [sku])).e));
  // 同じ request_id をほかの操作に = 断る
  await assert.rejects(() => asEditor(() => PG.savePackingGuide(db, { requestId: rid, actor: 'naka@test', via: 'sku_page', skuId: sku, seenEventId: g.event_id, note: '', photos: [] })),
    (e) => e.status === 409 && e.reason === 'request_reused');
});

await ta('[W2] 保存: 注意点・並べ替え・一言・外す (行は残る)・版が違う = version_conflict・ほかの商品の写真 = unknown_photo・変えない = 記録なし', async () => {
  const sku = await skuIdOf('s001');
  let g = await asEditor(() => PG.readPackingGuide(db, sku));
  const [x1, x2, x3] = g.photos;
  const before = (await eventsOf('s001')).length;
  const r = await save(sku, { seenEventId: g.event_id, note: '割れ物。\n天地無用で入れる', photos: [{ photo_id: x3.photo_id, caption: '最後に上から' }, { photo_id: x1.photo_id, caption: '底に緩衝材' }] });
  assert.deepEqual([r.ok, r.changed, r.replay], [true, true, false]);
  g = await asEditor(() => PG.readPackingGuide(db, sku));
  assert.equal(g.note, '割れ物。\n天地無用で入れる');
  assert.deepEqual(g.photos.map((p) => [p.photo_id, p.position, p.caption]), [[x3.photo_id, 1, '最後に上から'], [x1.photo_id, 2, '底に緩衝材']]);
  const gone = await one('select position, removed_by, removed_at is not null as removed from core.sku_packing_photos where photo_id = $1', [x2.photo_id]);
  assert.deepEqual([gone.position, gone.removed_by, gone.removed], [null, 'naka@test', true], '外した写真の行は残る');
  assert.equal((await asEditor(() => PG.readPackingPhoto(db, x2.photo_id))).sha256, x2.sha256, '外した写真も読める (前の版)');
  const ev = await eventsOf('s001');
  assert.equal(ev.length, before + 1);
  assert.equal(ev.at(-1).action, 'set_guide');
  assert.equal(ev.at(-1).f.photos.length, 3);
  assert.equal(ev.at(-1).t.note, '割れ物。\n天地無用で入れる');
  // 古い版で保存 = version_conflict (何も変わらない)
  await assert.rejects(() => save(sku, { seenEventId: x1 && String(Number(g.event_id) - 1), note: 'x', photos: [] }), (e) => e.status === 409 && e.reason === 'version_conflict');
  await assert.rejects(() => save(sku, { seenEventId: null, note: 'x', photos: [] }), (e) => e.reason === 'version_conflict');
  // 外した写真・ほかの商品の写真を並べる = unknown_photo
  await assert.rejects(() => save(sku, { seenEventId: g.event_id, note: 'x', photos: [{ photo_id: x2.photo_id }] }), (e) => e.status === 409 && e.reason === 'unknown_photo');
  const other = await skuIdOf('s002');
  const o = await add('s002', other, await photoOf(20, 20));
  await assert.rejects(() => save(sku, { seenEventId: g.event_id, note: 'x', photos: [{ photo_id: o.photo_id }] }), (e) => e.reason === 'unknown_photo');
  assert.equal((await asEditor(() => PG.readPackingGuide(db, sku))).note, '割れ物。\n天地無用で入れる', '断ったら何も変えない');
  // 何も変えない = 記録なし・changed false
  const same = await save(sku, { seenEventId: g.event_id, note: '割れ物。\r\n天地無用で入れる', photos: g.photos.map((p) => ({ photo_id: p.photo_id, caption: p.caption })) });
  assert.deepEqual([same.changed, same.event_id], [false, g.event_id]);
  assert.equal((await eventsOf('s001')).length, before + 1);
  // 写真を全部外して 1 枚足す = 位置 1 から
  await save(sku, { seenEventId: g.event_id, note: '', photos: [] });
  const a = await add('s001', sku, await photoOf(10, 10));
  assert.equal(a.position, 1);
  g = await asEditor(() => PG.readPackingGuide(db, sku));
  assert.equal(g.note, '');
  assert.equal(g.photos.length, 1);
});

await ta('[W3] 空の指示は行を作らない・形の誤り (注意点 2001 字・制御文字・一言 101 字・4 つの並び・同じ写真 2 回・JPEG でない)・登録をやめた商品', async () => {
  const sku = await skuIdOf('s003');
  const r = await save(sku, { seenEventId: null, note: '   \n ', photos: [] });
  assert.equal(r.changed, false);
  assert.equal((await one('select count(*)::int as n from core.sku_packing_guides where sku_id = $1', [sku])).n, 0);
  assert.equal((await eventsOf('s003')).length, 0);
  const call = (note, photos) => asEditor(() => pg.query(`select ops.set_sku_packing_guide($1::uuid, 'naka@test', 'sku_page', $2::bigint, null, $3, $4::jsonb)`, [uuid(), sku, note, JSON.stringify(photos)]));
  await assert.rejects(() => call('あ'.repeat(2001), []), /invalid_input/);
  await assert.rejects(() => call('a\u0007b', []), /invalid_input/);
  await assert.rejects(() => call('', [{ photo_id: '1' }, { photo_id: '2' }, { photo_id: '3' }, { photo_id: '4' }]), /invalid_input/);
  await assert.rejects(() => call('', [{ photo_id: '1' }, { photo_id: '1' }]), /invalid_input/);
  await assert.rejects(() => call('', [{ photo_id: 'x' }]), /invalid_input/);
  await assert.rejects(() => call('', [{ photo_id: '1', caption: 'あ'.repeat(101) }]), /invalid_input/);
  await assert.rejects(() => asEditor(() => pg.query(`select ops.set_sku_packing_guide($1::uuid, 'naka@test', 'mail', $2::bigint, null, '', '[]'::jsonb)`, [uuid(), sku])), /invalid_input/);
  await assert.rejects(() => asEditor(() => pg.query(`select ops.set_sku_packing_guide($1::uuid, '', 'sku_page', $2::bigint, null, '', '[]'::jsonb)`, [uuid(), sku])), /invalid_input/);
  const png = await sharp({ create: { width: 20, height: 20, channels: 3, background: { r: 1, g: 2, b: 3 } } }).png().toBuffer();
  await assert.rejects(() => asEditor(() => pg.query(`select ops.add_sku_packing_photo($1::uuid, 'naka@test', 'sku_page', $2::bigint, $3::bytea, 20, 20, '')`, [uuid(), sku, png])), /invalid_input: 写真は JPEG/);
  { const j = await jpeg(); await assert.rejects(() => asEditor(() => pg.query(`select ops.add_sku_packing_photo($1::uuid, 'naka@test', 'sku_page', $2::bigint, $3::bytea, 0, 20, '')`, [uuid(), sku, j])), /invalid_input/); }
  { const j = await jpeg(); await assert.rejects(() => asEditor(() => pg.query(`select ops.add_sku_packing_photo($1::uuid, 'naka@test', 'sku_page', 999999999, $2::bytea, 20, 20, '')`, [uuid(), j])), /invalid_input: SKU/); }
  // 登録をやめた商品 = cancelled (状態を持ち主で変える = 試験だけ)
  const s2 = await skuIdOf('s002');
  const st = await one('select state from ops.master_registrations where sku_id = $1', [s2]);
  await pg.query(`alter table ops.master_registrations disable trigger user`);
  try {
    await pg.query(`update ops.master_registrations set state = 'cancelled' where sku_id = $1`, [s2]);
    await assert.rejects(() => add('s002', s2, { image: Buffer.alloc(0) }), () => true);   // 形の誤り (空)
    const p12 = await photoOf(12, 12);
    await assert.rejects(() => add('s002', s2, p12), (e) => e.status === 409 && e.reason === 'cancelled');
    assert.equal((await PG.listGuidesForExport(db)).some((x) => x.code === 's002'), false, '登録をやめた商品は配らない');
  } finally {
    await pg.query(`update ops.master_registrations set state = $2 where sku_id = $1`, [s2, st.state]);
    await pg.query(`alter table ops.master_registrations enable trigger user`);
  }
});

await ta('[D2] 守り: 関数の外では書けない (持ち主でも)・記録は追記だけ・写真の中身は変えない・消さない・外した写真は戻さない・TRUNCATE できない', async () => {
  const sku = await skuIdOf('s001');
  const pid = (await one('select photo_id::text as id from core.sku_packing_photos where sku_id = $1 and position is not null limit 1', [sku])).id;
  const s3 = await skuIdOf('s003');
  await assert.rejects(() => pg.query(`insert into core.sku_packing_guides (sku_id, note, updated_by, updated_via) values ($1, 'x', 'a', 'sku_page')`, [s3]), /の中でだけ書く/);
  await assert.rejects(() => pg.query(`update core.sku_packing_photos set caption = 'x' where photo_id = $1`, [pid]), /の中でだけ書く/);
  await assert.rejects(() => pg.query(`update core.sku_packing_photos set sha256 = repeat('0', 64) where photo_id = $1`, [pid]), /写真の中身は変えない/);
  await assert.rejects(() => pg.query(`delete from core.sku_packing_photos where photo_id = $1`, [pid]), /消さない/);
  await assert.rejects(() => pg.query(`update ops.sku_packing_guide_events set actor_id = 'x'`), /追記だけ/);
  await assert.rejects(() => pg.query(`delete from ops.sku_packing_guide_events`), /追記だけ/);
  const removed = (await one('select photo_id::text as id from core.sku_packing_photos where position is null limit 1')).id;
  await pg.query(`select set_config('ops.packing_guide_protocol', '1', false)`);
  try {
    await assert.rejects(() => pg.query(`update core.sku_packing_photos set position = 3, removed_at = null, removed_by = null where photo_id = $1`, [removed]), /外した写真は戻さない/);
  } finally { await pg.query(`select set_config('ops.packing_guide_protocol', '', false)`); }
  for (const t of PG.GUIDE_TABLES) await assert.rejects(() => pg.query(`truncate ${t}`), /./, t);
  await assert.rejects(() => asEditor(() => pg.query(`insert into core.sku_packing_guides (sku_id, note, updated_by, updated_via) values ($1, 'x', 'a', 'sku_page')`, [sku])), /permission denied/);
});

await ta('[P1] 写真の作り直し: 向きを直す (EXIF)・長辺 1280px・JPEG・メタデータを捨てる・読めない = 400・大きすぎる = 400', async () => {
  const big = await sharp({ create: { width: 3000, height: 2000, channels: 3, background: { r: 9, g: 9, b: 9 } } }).jpeg().withMetadata({ orientation: 6, exif: { IFD0: { Copyright: 'gps-test' } } }).toBuffer();
  const p = await PG.normalizePhoto(big);
  assert.deepEqual([p.width, p.height], [853, 1280], '向き 6 = 90 度回して縦長・長辺 1280');
  const meta = await sharp(p.image).metadata();
  assert.equal(meta.format, 'jpeg');
  assert.equal(meta.exif, undefined, 'EXIF を捨てる');
  assert.equal(meta.orientation, undefined);
  assert.equal(p.sha256, crypto.createHash('sha256').update(p.image).digest('hex'));
  const small = await PG.normalizePhoto(await jpeg(40, 30));
  assert.deepEqual([small.width, small.height], [40, 30], '大きくはしない');
  const png = await PG.normalizePhoto(await sharp({ create: { width: 50, height: 50, channels: 4, background: { r: 0, g: 0, b: 0, alpha: 0.5 } } }).png().toBuffer());
  assert.equal((await sharp(png.image).metadata()).format, 'jpeg', 'PNG も JPEG に');
  await assert.rejects(() => PG.normalizePhoto(Buffer.from('not an image at all')), (e) => e.status === 400 && e.reason === 'bad_image');
  await assert.rejects(() => PG.normalizePhoto(Buffer.alloc(PG.UPLOAD_MAX_BYTES + 1)), (e) => e.status === 400 && e.reason === 'too_large');
  assert.equal(PG.decodeImageBase64('data:image/jpeg;base64,' + Buffer.from('abc').toString('base64')).toString(), 'abc');
  assert.throws(() => PG.decodeImageBase64('!!!'), (e) => e.status === 400);
  assert.throws(() => PG.guideSaveIn({ request_id: uuid(), photos: [{ photo_id: '1' }, { photo_id: '1' }] }), (e) => e.status === 400);
  assert.throws(() => PG.guideSaveIn({ request_id: 'x', photos: [] }), (e) => e.status === 400);
  assert.deepEqual(PG.guideSaveIn({ request_id: 'A0000000-0000-4000-8000-000000000000', seen_event_id: '', note: ' \n', photos: [{ photo_id: 5, caption: ' a\nb ' }] }),
    { requestId: 'a0000000-0000-4000-8000-000000000000', seenEventId: null, note: '', photos: [{ photo_id: '5', caption: 'a b' }] });
});

await ta('[X1] 配りの一覧: 指示のある単品だけ (注意点か今の写真)・写真の番号と sha256 と並び・中身は入れない', async () => {
  const list = await asEditor(() => PG.listGuidesForExport(db));
  const codes = list.map((x) => x.code);
  assert.ok(codes.includes('s001') && codes.includes('s002'), codes.join(','));
  assert.equal(codes.includes('s003'), false, '何も無い商品は出さない');
  const s1 = list.find((x) => x.code === 's001');
  assert.equal(s1.photos.length, 1);
  assert.deepEqual(Object.keys(s1.photos[0]).sort(), ['caption', 'height', 'photo_id', 'position', 'sha256', 'width']);
  assert.ok(s1.event_id);
});

await ta('[B1] バックアップ: 写真の表 (bytea) は小分けに読む・戻すと写真の中身 (sha256)・並び・一言・外した写真がそのまま戻る', async () => {
  const BK = await import('../apps/company-db/backup/dump.mjs');
  const before = await q(`select photo_id::text as id, sku_id::text as sku, position, caption, sha256, encode(sha256(image), 'hex') as h from core.sku_packing_photos order by photo_id`);
  assert.ok(before.length >= 3 && before.some((p) => p.position == null), '外した写真も含む');
  const fetches = [];
  const spy = { ...db, query: (sql, p) => { if (/fetch forward/.test(sql)) fetches.push(sql); return db.query(sql, p); }, exec: (sql) => db.exec(sql) };
  const lines = [];
  await BK.dumpCompanyDb(spy, (l) => lines.push(l), { log: () => {} });
  assert.ok(fetches.some((s) => /fetch forward 8 /.test(s)), '写真の表は 8 行ずつ');
  assert.ok(fetches.some((s) => /fetch forward 5000 /.test(s)), 'ほかの表は今までどおり');
  // 戻す前に変える (並び・一言・写真を 1 枚足す) → 戻すと前のまま
  const sku = await skuIdOf('s001');
  const g = await asEditor(() => PG.readPackingGuide(db, sku));
  await save(sku, { seenEventId: g.event_id, note: '戻す前の文', photos: g.photos.map((p) => ({ photo_id: p.photo_id, caption: '変えた' })) });
  const p33 = await photoOf(33, 33);
  await add('s001', sku, p33);
  await BK.restoreCompanyDb(db, lines.join('\n') + '\n', { log: () => {} });
  const after = await q(`select photo_id::text as id, sku_id::text as sku, position, caption, sha256, encode(sha256(image), 'hex') as h from core.sku_packing_photos order by photo_id`);
  assert.deepEqual(after, before);
  for (const p of after) assert.equal(p.h, p.sha256, `写真 ${p.id} の中身が sha256 と同じ`);
  assert.equal((await asEditor(() => PG.readPackingGuide(db, sku))).note, g.note);
});

const pgdData = (html) => JSON.parse((/<script type="application\/json" id="me-pgd">([\s\S]*?)<\/script>/.exec(html) || [])[1] || 'null');

await ta('[S1] 商品の画面: 単品に「梱包指示」の節 (写真・一言・注意点・書ける人だけ動きの JS)・見る人 = 見るだけ・セット = 節を出さない', async () => {
  const e = await call('GET', '/sku/s001');
  assert.equal(e.status, 200);
  assert.match(e.text, /id="p-pgd"/);
  assert.match(e.text, /me-packing-guide\.js/);
  const d = pgdData(e.text);
  assert.equal(d.canWrite, true);
  assert.equal(d.max, 3);
  const s1 = await skuIdOf('s001');
  const g = await asEditor(() => PG.readPackingGuide(db, s1));
  assert.equal(d.guide.event_id, g.event_id);
  assert.equal(d.guide.photos.length, g.photos.length);
  assert.match(d.guide.photos[0].url, /^\/sku\/s001\/packing-photo\/\d+$/);
  const v = await call('GET', '/sku/s001', { session: 'viewer' });
  assert.match(v.text, /id="p-pgd"/);
  assert.doesNotMatch(v.text, /me-packing-guide\.js/);
  assert.match(v.text, /見るだけです/);
  assert.doesNotMatch(v.text, /id="pgd-save"/);
  const s = await call('GET', '/sku/pgset1');
  assert.equal(s.status, 200);
  assert.doesNotMatch(s.text, /id="p-pgd"/);
});

await ta('[S2] 写真を足す (HTTP): 作り直して保存・写真を出す (JPEG・この商品の写真だけ)・見る人 = 403・Origin 無し = 403・形の誤り = 400・セット = 400', async () => {
  const big = await sharp({ create: { width: 2400, height: 1800, channels: 3, background: { r: 30, g: 120, b: 220 } } }).jpeg({ quality: 95 }).toBuffer();
  const body = { request_id: uuid(), image: 'data:image/jpeg;base64,' + big.toString('base64'), caption: '箱に立てて入れる' };
  const r = await call('POST', '/api/sku/s003/packing-guide/photo', { body });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.ok, r.j.position], [true, 1]);
  const ph = r.j.guide.photos[0];
  assert.deepEqual([ph.width, ph.height, ph.caption], [1280, 960, '箱に立てて入れる']);
  const img = await call('GET', ph.url, { raw: true });
  assert.equal(img.status, 200);
  assert.equal(img.type, 'image/jpeg');
  assert.match(img.cache, /private/);
  assert.equal(crypto.createHash('sha256').update(img.buf).digest('hex'), ph.sha256);
  assert.equal((await call('GET', ph.url.replace('/s003/', '/s001/'), { raw: true })).status, 404, 'ほかの商品のコードでは出さない');
  assert.equal((await call('GET', '/sku/s003/packing-photo/abc', { raw: true })).status, 404);
  const again = await call('POST', '/api/sku/s003/packing-guide/photo', { body });
  assert.deepEqual([again.j.replay, again.j.photo_id], [true, r.j.photo_id], '押し直し = 前の答え');
  assert.equal((await call('POST', '/api/sku/s003/packing-guide/photo', { body: { ...body, request_id: uuid() }, session: 'viewer' })).status, 403);
  assert.equal((await call('POST', '/api/sku/s003/packing-guide/photo', { body: { ...body, request_id: uuid() }, origin: false })).status, 403);
  const bad = await call('POST', '/api/sku/s003/packing-guide/photo', { body: { request_id: uuid(), image: Buffer.from('hello').toString('base64') } });
  assert.deepEqual([bad.status, bad.j.reason], [400, 'bad_image']);
  const set = await call('POST', '/api/sku/pgset1/packing-guide/photo', { body: { ...body, request_id: uuid() } });
  assert.deepEqual([set.status, set.j.reason], [400, 'not_single']);
  assert.equal((await call('POST', '/api/sku/nope-code/packing-guide/photo', { body: { ...body, request_id: uuid() } })).status, 404);
  const g = await call('GET', '/api/sku/s003/packing-guide');
  assert.deepEqual([g.status, g.j.guide.photos.length], [200, 1]);
  // 大きい本文 (17 MB まで) は写真の道だけ = 保存の道は今までどおり 256kb
  const huge = await call('POST', '/api/sku/s003/packing-guide', { body: { request_id: uuid(), seen_event_id: g.j.guide.event_id, note: 'x'.repeat(300 * 1024), photos: [] } });
  assert.equal(huge.status, 413);
});

await ta('[S3] 保存 (HTTP): 注意点・一言・外す・ほかの人が変えた後 = 409・形の誤り = 400・見る人 = 403・記録の人 = セッションのメール', async () => {
  const g = (await call('GET', '/api/sku/s003/packing-guide')).j.guide;
  const r = await call('POST', '/api/sku/s003/packing-guide', { body: { request_id: uuid(), seen_event_id: g.event_id, note: 'ワレモノ。立てて入れる', photos: [{ photo_id: g.photos[0].photo_id, caption: '立てる' }] } });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.changed, r.j.guide.note, r.j.guide.photos[0].caption, r.j.guide.updated_by], [true, 'ワレモノ。立てて入れる', '立てる', 'naka@test']);
  const old = await call('POST', '/api/sku/s003/packing-guide', { body: { request_id: uuid(), seen_event_id: g.event_id, note: '古い画面から', photos: [] } });
  assert.deepEqual([old.status, old.j.reason], [409, 'version_conflict']);
  const bad = await call('POST', '/api/sku/s003/packing-guide', { body: { request_id: 'x', seen_event_id: r.j.event_id, note: '', photos: [] } });
  assert.equal(bad.status, 400);
  assert.equal((await call('POST', '/api/sku/s003/packing-guide', { body: { request_id: uuid(), seen_event_id: r.j.event_id, note: '', photos: [] }, session: 'viewer' })).status, 403);
  const ev = await eventsOf('s003');
  assert.deepEqual(ev.map((e) => [e.action, e.via, e.actor_id]), [['add_photo', 'sku_page', 'naka@test'], ['set_guide', 'sku_page', 'naka@test']]);
});

await ta('[R1] ロールの流し直しの前 (master_edit に関数の実行権が無い): 節を出さない・書き込みは 503 db_not_ready・画面は止めない', async () => {
  for (const f of ROLES.PACKING_GUIDE_EDIT_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
  try {
    const e = await call('GET', '/sku/s001');
    assert.equal(e.status, 200);
    assert.doesNotMatch(e.text, /id="p-pgd"/);
    const w = await call('POST', '/api/sku/s001/packing-guide', { body: { request_id: uuid(), seen_event_id: null, note: 'x', photos: [] } });
    assert.deepEqual([w.status, w.j.reason], [503, 'db_not_ready']);
  } finally {
    for (const f of ROLES.PACKING_GUIDE_EDIT_FUNCTIONS) await pg.query(`grant execute on function ${f} to master_edit`);
  }
});

await ta('[X2] 配りの口 (Render → miniPC): 鍵が無い設定 = 503・鍵が違う = 401・一覧 (指示のある単品)・写真の中身 (sha256 と同じ)・無い写真 = 404・DB の準備の前 = 503', async () => {
  const AR = await import('../apps/packing-guide/api-router.mjs');
  AR.__setPgClientFactory(async () => {
    let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
    await pg.query('set role master_edit');
    return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
  });
  const a2 = express();
  a2.use('/apps/packing-guide-api', AR.default);
  const s2 = http.createServer(a2);
  await new Promise((r) => s2.listen(0, '127.0.0.1', r));
  const B = `http://127.0.0.1:${s2.address().port}/apps/packing-guide-api`;
  const get = (p, key) => fetch(B + p, { headers: key ? { 'x-packing-guide-key': key } : {} });
  try {
    delete process.env.PACKING_GUIDE_KEY;
    assert.equal((await get('/manifest', 'k')).status, 503);
    process.env.PACKING_GUIDE_KEY = 'secret-1';
    assert.equal((await get('/manifest')).status, 401);
    assert.equal((await get('/manifest', 'secret-2')).status, 401);
    const m = await get('/manifest', 'secret-1');
    assert.equal(m.status, 200);
    assert.equal(m.headers.get('cache-control'), 'no-store');
    const body = await m.json();
    assert.equal(body.ok, true);
    const s3 = body.guides.find((g) => g.code === 's003');
    assert.equal(s3.note, 'ワレモノ。立てて入れる');
    assert.equal(s3.photos.length, 1);
    const ph = await get(`/photos/${s3.photos[0].photo_id}`, 'secret-1');
    assert.equal(ph.status, 200);
    assert.equal(ph.headers.get('content-type'), 'image/jpeg');
    assert.equal(crypto.createHash('sha256').update(Buffer.from(await ph.arrayBuffer())).digest('hex'), s3.photos[0].sha256);
    assert.equal((await get('/photos/999999999', 'secret-1')).status, 404);
    assert.equal((await get('/photos/x', 'secret-1')).status, 404);
    for (const f of ROLES.PACKING_GUIDE_EDIT_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
    try { assert.equal((await get('/manifest', 'secret-1')).status, 503); } finally {
      for (const f of ROLES.PACKING_GUIDE_EDIT_FUNCTIONS) await pg.query(`grant execute on function ${f} to master_edit`);
    }
  } finally { s2.close(); delete process.env.PACKING_GUIDE_KEY; }
});

await ta('[X3] 配りの口の写真を足す (梱包の画面で撮った・2 段目): 作り直して保存 (via packing・人 = packing:<作業者>)・押し直し = 前の答え・断り (3 枚・セット・無いコード・読めない写真・人なし)・鍵', async () => {
  const AR = await import('../apps/packing-guide/api-router.mjs');
  const a3 = express();
  a3.use('/apps/packing-guide-api', AR.default);
  const s3 = http.createServer(a3);
  await new Promise((r) => s3.listen(0, '127.0.0.1', r));
  const B = `http://127.0.0.1:${s3.address().port}/apps/packing-guide-api`;
  const post = async (body, key = 'secret-1') => {
    const r = await fetch(B + '/photos', { method: 'POST', headers: { 'Content-Type': 'application/json', ...(key ? { 'x-packing-guide-key': key } : {}) }, body: JSON.stringify(body) });
    return { status: r.status, j: await r.json().catch(() => ({})) };
  };
  const img = (await sharp({ create: { width: 1800, height: 2400, channels: 3, background: { r: 5, g: 90, b: 60 } } }).jpeg().toBuffer()).toString('base64');
  const s2id = await skuIdOf('s002');
  try {
    process.env.PACKING_GUIDE_KEY = 'secret-1';
    assert.equal((await post({}, null)).status, 401);
    const before = (await asEditor(() => PG.readPackingGuide(db, s2id))).photos.length;
    const rid = uuid();
    const r = await post({ request_id: rid, code: 'S002', image: img, caption: '立てて入れる', actor: '三宅晴菜' });
    assert.equal(r.status, 200, JSON.stringify(r.j));
    assert.deepEqual([r.j.ok, r.j.code, r.j.position, r.j.replay], [true, 's002', before + 1, false]);
    const ph = (await asEditor(() => PG.readPackingGuide(db, s2id))).photos.at(-1);
    assert.deepEqual([ph.caption, ph.width, ph.height, ph.added_via, ph.added_by], ['立てて入れる', 960, 1280, 'packing', 'packing:三宅晴菜']);
    const again = await post({ request_id: rid, code: 'S002', image: img, caption: '立てて入れる', actor: '三宅晴菜' });
    assert.deepEqual([again.j.replay, again.j.photo_id], [true, r.j.photo_id]);
    while ((await asEditor(() => PG.readPackingGuide(db, s2id))).photos.length < 3) await post({ request_id: uuid(), code: 's002', image: img, actor: '三宅晴菜' });
    const full = await post({ request_id: uuid(), code: 's002', image: img, actor: '三宅晴菜' });
    assert.deepEqual([full.status, full.j.reason], [409, 'photos_full']);
    assert.deepEqual([(await post({ request_id: uuid(), code: 'pgset1', image: img, actor: 'a' })).j.reason], ['not_single']);
    assert.deepEqual([(await post({ request_id: uuid(), code: 'nope', image: img, actor: 'a' })).status], [404]);
    const badImg = await post({ request_id: uuid(), code: 's003', image: Buffer.from('not image').toString('base64'), actor: 'a' });
    assert.deepEqual([badImg.status, badImg.j.reason], [400, 'bad_image']);
    assert.equal((await post({ request_id: uuid(), code: 's003', image: img, actor: '  ' })).status, 400);
    assert.equal((await post({ request_id: 'x', code: 's003', image: img, actor: 'a' })).status, 400);
  } finally { s3.close(); delete process.env.PACKING_GUIDE_KEY; }
});

server.close();
console.log(`${passed} passed`);
clearTimeout(killer);
process.exit(process.exitCode || 0);
