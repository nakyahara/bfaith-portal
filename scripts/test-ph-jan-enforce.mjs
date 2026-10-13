/**
 * test-ph-jan-enforce.mjs — 楽天に送る JAN を「商品マスタ (Company DB) から選んだ物」だけにする (Company DB構想 21 §5 B2 の段 3〜5)
 *
 * Company DB = PGlite (最新の migration まで・本物の SQL)。product-hub = 一時の DATA_DIR の SQLite (本物の router)。楽天 (miniPC) は fetch を差し替えて数える。
 * 固定する契約:
 *   S 段 3 (選ぶ): /jan-check は 1 回の読みで比べる印と「選ぶ物」(単位ごとの状態・その商品の有効な JAN と行番号) を返す・
 *     /jan-select = Company DB で「今も有効・この商品の JAN・値が同じ」を確かめてから JAN の値 + 結んだ印を 1 つの取引で書く・なしの理由は外す・
 *     違う行 / 外された行 / 値が違う / セット / 商品が無い / 単位でない / 読めない / Origin が無い = 何も書かない・
 *     まとまり = 子の SKU ごと (ほかの SKU と同じ JAN = 断る・大文字小文字だけ違う行は 1 つに)・
 *     手の道 (基本情報・SKU 表) で JAN を変えた = 結んだ印を外す (同じ値なら残す)・止める設定のとき手の道で JAN を変えるのは断る (空にするのは通す)
 *   G 段 4 (止める): 既定 (設定なし) = 何も止めない (印だけ)・設定 (PH_JAN_ENFORCE=1) = 結ばれていない / 要確認 / 両方 / どちらも無い / 行が重なる を
 *     詳細画面の登録とボードからの出品で、楽天に触る前に止める (理由つき)・JAN を選んである / なしの理由だけ = 止めない・
 *     管理画面に設定の今と「今止めたら止まる数」
 *   P 段 5 (送る直前の 1 か所): 既定 = Company DB を読まず jan_code を今までどおり送る・
 *     設定 = 結んだ JAN を Company DB で確かめ直し、送るのは Company DB の値 (全角で打った値でも送るのは Company DB の値)・
 *     外された / 読めない / 確かめている間に SQLite が変わった = 送らない・
 *     articleNumber は「JAN 1 つ」か「なしの理由」のどちらか 1 つだけ (checkArticleNumbers)・まとまりは SKU ごと・
 *     設定があるのに確かめた結果を渡さない payload = 止める・プレビューも同じ理由
 * 使い方: node scripts/test-ph-jan-enforce.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phjan3-data-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;
delete process.env.COMPANY_DB_URL;
delete process.env.COMPANY_DB_WATCH_URL;
delete process.env.COMPANY_DB_SCREEN_READ_URL;
delete process.env.PH_JAN_ENFORCE;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const LG = await import('../lib/master-legacy-gate.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const J = await import('../apps/product-hub/services/cdb-jan-compare.mjs');
const LJ = await import('../apps/product-hub/services/listing-jan.mjs');
const RL = await import('../apps/product-hub/services/rakuten-listing.js');
const BL = await import('../apps/product-hub/services/board-listing.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const ean = (d12) => { const s = String(d12); let t = 0; for (let i = 0; i < 12; i++) t += Number(s[i]) * (i % 2 ? 3 : 1); return s + ((10 - (t % 10)) % 10); };
const JAN = Object.fromEntries(['s1', 's2a', 's2b', 'vr', 'vb', 'vw', 'x1', 'z1', 'm1'].map((k, i) => [k, ean(`4910000${String(300 + i).padStart(5, '0')}`)]));
const fullWidth = (s) => String(s).replace(/[0-9]/g, (d) => String.fromCharCode(d.charCodeAt(0) + 0xFEE0));
const enforce = (on) => { if (on) process.env.PH_JAN_ENFORCE = '1'; else delete process.env.PH_JAN_ENFORCE; };

// ─── Company DB ───
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const cdb = pgliteAdapter(pg);
await applyMigrations(cdb, { log: quiet });
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
const loadR = await runInitialLoad(cdb, {
  skus: [sku('s001', '単品 1 つ'), sku('s002', '単品 2 つ'), sku('s003', 'JAN 無し'), sku('v-red', '赤'), sku('v-blu', '青'), sku('v-wht', '白'), sku('m001', '手で打つ'),
    sku('set001', 'セット', { kind: 'set', cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE' } })],
  variationGroups: [], setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 2, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_phjan3' },
}, { log: quiet, runId: 'load_phjan3', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(loadR.ok, true, loadR.error);
const pidOf = async (code) => (await pg.query('select product_id::text as id from core.skus where code = $1', [code])).rows[0].id;
async function addJan(code, jan) {
  return Number((await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
    values (1, 'product', $1, 'jan', 'jan', $2, 'manual', 'human', 'phjan3-test') returning external_id_row::text as id`, [await pidOf(code), jan])).rows[0].id);
}
const closeJan = (row) => pg.query('update core.external_ids set valid_to = now() where external_id_row = $1', [row]);
const ROW = {
  s1: await addJan('s001', JAN.s1), s2a: await addJan('s002', JAN.s2a), s2b: await addJan('s002', JAN.s2b),
  vr: await addJan('v-red', JAN.vr), vb: await addJan('v-blu', JAN.vb), vw: await addJan('v-wht', JAN.vw), m1: await addJan('m001', JAN.m1),
};

// 読み手 = 本物の SQL (PGlite)。読めない・読む間に何かする・数える を切り替える
let readable = true;
let reads = 0;
let duringRead = null;
J.__setCdbJanReader(async (items) => {
  reads++;
  if (duringRead) { const f = duringRead; duringRead = null; await f(); }
  if (!readable) return { ok: false, reason: 'テストで読めない' };
  return { ok: true, rows: await J.queryCdbJans(pg, 1, items) };
});

// ─── 楽天 (miniPC) = fetch を数える ───
process.env.WAREHOUSE_URL = 'http://warehouse.invalid';
process.env.CF_ACCESS_CLIENT_ID = 'x'; process.env.CF_ACCESS_CLIENT_SECRET = 'x'; process.env.WAREHOUSE_SERVICE_TOKEN = 'x';
const realFetch = globalThis.fetch;
const puts = [];
const rmsCalls = [];
globalThis.fetch = async (url, opts = {}) => {
  if (String(url).startsWith('http://warehouse.invalid')) {
    const p = String(url).replace('http://warehouse.invalid', '');
    rmsCalls.push(`${opts.method || 'GET'} ${p}`);
    if ((opts.method || 'GET') === 'PUT' && /\/items\/manage-numbers\//.test(p)) {
      puts.push({ path: p, body: JSON.parse(opts.body) });
      return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }
    return new Response(JSON.stringify({ message: 'not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
  }
  return realFetch(url, opts);
};

// ─── product-hub ───
const ph = PHDB.getDB();
let mirrorPid = 910000;
const mirror = (code, repCode) => ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', ?, '2026-10-13T00:00:00Z')`).run(++mirrorPid, code, repCode);
for (const c of ['s001', 's002', 's003', 'set001', 'nosku1', 'm001']) mirror(c, null);
mirror('v-red', 'grpv'); mirror('v-blu', 'grpv'); mirror('v-wht', 'grpv');
const draft = (code, jan) => {
  ph.prepare('INSERT INTO product_drafts (ne_code, name, jan_code, price) VALUES (?, ?, ?, 1980)').run(code, `下書き ${code}`, jan);
  return ph.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get(code);
};
const dS1 = draft('s001', null);
const dS2 = draft('s002', JAN.s2b);   // 手で打った (結ばれていない)
const dS3 = draft('s003', null);
const dSet = draft('set001', null);
const dNo = draft('nosku1', JAN.z1);
const dM = draft('m001', JAN.m1);     // 手で打った・商品マスタと同じ値
const dV = draft('grpv', null);
const TAX = { tax: { mode: 'legacy' } };
const LEGACY = { mode: 'legacy' };
const fingerprint = () => Object.fromEntries(['product_drafts', 'draft_sku_jans', 'draft_events', 'draft_rakuten', 'draft_sku_catalog_exemptions'].map((t) => [t, crypto.createHash('sha256').update(JSON.stringify(ph.prepare(`SELECT * FROM ${t} ORDER BY rowid`).all())).digest('hex')]));
const linkOf = (id) => ph.prepare('SELECT jan_code, jan_cdb_row, jan_cdb_value, jan_link_state, jan_link_reason, jan_linked_by FROM product_drafts WHERE id = ?').get(id);
const skuLinkOf = (id, code) => ph.prepare('SELECT jan_code, jan_cdb_row, jan_cdb_value, jan_link_state, jan_linked_by FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').get(id, code);
const setLink = (id, row, value, state = 'linked', reason = null) => ph.prepare('UPDATE product_drafts SET jan_cdb_row = ?, jan_cdb_value = ?, jan_link_state = ?, jan_link_reason = ? WHERE id = ?').run(row, value, state, reason, id);
const setReason = (id, reason) => ph.prepare(`INSERT INTO draft_rakuten (draft_id, catalog_id_exemption_reason) VALUES (?, ?) ON CONFLICT(draft_id) DO UPDATE SET catalog_id_exemption_reason = excluded.catalog_id_exemption_reason`).run(id, reason);

// 出品できる形にする (画像・ジャンル・タイトル・配送・画像の工程)。まとまりは項目名・選択肢も
function makeListable(d, { variation = null } = {}) {
  ph.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, shipping_method_group, variant_selector_name) VALUES (?, '205761', '[{"name":"ブランド名","values":["ノーブランド品"]}]', '4', ?)
    ON CONFLICT(draft_id) DO UPDATE SET genre_id = '205761', attributes_json = excluded.attributes_json, shipping_method_group = '4', variant_selector_name = excluded.variant_selector_name`).run(d.id, variation ? 'カラー' : null);
  ph.prepare('INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, ?)').run(d.id, `gf-${d.id}`);
  ph.prepare('INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, ?, ?)').run(d.id, `gf-${d.id}`, `/app-newitems/${d.ne_code}-1.jpg`);
  ph.prepare(`INSERT INTO draft_ai_outputs (draft_id, kind, content) VALUES (?, 'rakuten_title', '楽天用タイトル')`).run(d.id);
  for (const [code, v] of variation || []) ph.prepare('INSERT INTO draft_sku_selector_values (draft_id, sku_code, value) VALUES (?, ?, ?)').run(d.id, code, v);
  RL.buildItemPayload(ph, d.id, TAX);   // 工程の行を作る
  ph.prepare(`UPDATE draft_step_progress SET state = 'done', done_by = 't' WHERE draft_id = ? AND step_code IN (SELECT code FROM ph_steps WHERE track = 'image')`).run(d.id);
}
const resetListed = (id) => ph.prepare('UPDATE draft_rakuten SET registered_at = NULL, published_at = NULL, listing_outcome = NULL, last_error = NULL WHERE draft_id = ?').run(id);

// ─── router ───
LG.__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open', changed_at: '2030-01-01', changed_by: 'x' }));
const app = express();
app.set('view engine', 'ejs');
app.use(express.json());
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'admin' ? { authenticated: true, email: 'boss@test', displayName: '管理者', role: 'admin', allowedApps: ['product-hub'] }
    : { authenticated: true, email: 'staff@test', displayName: 'スタッフ', role: 'user', allowedApps: ['product-hub'] };
  next();
});
const PHR = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', PHR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'user', origin = ORIGIN } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (origin) headers.Origin = origin;
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const r = await realFetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30000) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
const pick = (id, code, row, value, opts = {}) => call('POST', `/apps/product-hub/api/drafts/${id}/jan-select`, { body: { code, row, value }, ...opts });
const janCheck = (id) => call('GET', `/apps/product-hub/api/drafts/${id}/jan-check`);
const pickOf = (j, code) => j.picks.units.find((u) => u.code.toLowerCase() === code.toLowerCase());

try {
  // ═══ S 段 3: 選ぶ ═══
  console.log('S 選ぶ (段 3)');
  await ta('[S1] /jan-check = 1 回の読みで比べる印と選ぶ物: 有効な JAN と行番号・結ばれていない・JAN 無し・セット・商品マスタに無い・既定は止めない', async () => {
    reads = 0;
    const r2 = await janCheck(dS2.id);
    assert.equal(r2.status, 200);
    assert.equal(reads, 1, '2 回読んだ');
    assert.equal(r2.j.enforce, false);
    const u2 = pickOf(r2.j, 's002');
    assert.deepEqual(u2.cdb.jans, [{ row: ROW.s2a, value: JAN.s2a }, { row: ROW.s2b, value: JAN.s2b }].sort((a, b) => a.value.localeCompare(b.value)));
    assert.equal(u2.state, 'not_linked');
    assert.equal(u2.linkedRow, null);
    assert.equal(u2.masterEditUrl, '/apps/master-edit/sku/s002');
    assert.equal(r2.j.units[0].state, 'match', 'B1 の印も同じ答えに入る');
    const u3 = pickOf((await janCheck(dS3.id)).j, 's003');
    assert.deepEqual([u3.state, u3.cdb.jans], ['neither', []]);
    const us = pickOf((await janCheck(dSet.id)).j, 'set001');
    assert.equal(us.cdb.noJanKind, 'set');
    const un = pickOf((await janCheck(dNo.id)).j, 'nosku1');
    assert.equal(un.cdb.skuMissing, true);
    readable = false;
    try {
      const off = await janCheck(dS2.id);
      assert.equal(off.j.readable, false);
      assert.equal(off.j.picks.readable, false);
    } finally { readable = true; }
  });

  await ta('[S2] 単品で選ぶ = Company DB で確かめて JAN の値 + 結んだ印 (行番号・値・だれ) を書く・なしの理由は外す・履歴・選んだ後の印は「選んである」', async () => {
    setReason(dS1.id, 5);
    const r = await pick(dS1.id, 's001', String(ROW.s1), JAN.s1);
    assert.equal(r.status, 200, r.text);
    assert.deepEqual([r.j.jan, r.j.row, r.j.clearedReason], [JAN.s1, ROW.s1, true]);
    assert.deepEqual(linkOf(dS1.id), { jan_code: JAN.s1, jan_cdb_row: ROW.s1, jan_cdb_value: JAN.s1, jan_link_state: 'linked', jan_link_reason: null, jan_linked_by: 'staff@test' });
    assert.equal(ph.prepare('SELECT catalog_id_exemption_reason AS r FROM draft_rakuten WHERE draft_id = ?').get(dS1.id).r, null);
    const ev = ph.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'jan_selected'`).get(dS1.id);
    assert.match(ev.detail, new RegExp(`s001 = ${JAN.s1} \\(商品マスタの JAN を選んだ・行 ${ROW.s1}\\) / カタログIDなしの理由を外した`));
    const u = pickOf((await janCheck(dS1.id)).j, 's001');
    assert.deepEqual([u.state, u.linkedRow], ['ok_jan', ROW.s1]);
  });

  await ta('[S3] 選べない = 何も書かない: 別の商品の行・外された行・値が違う・セット・商品マスタに無い・単位でない・読めない・Origin 無し・JSON でない・形が違う', async () => {
    const before = fingerprint();
    const cases = [
      [() => pick(dS2.id, 's002', String(ROW.s1), JAN.s1), 409, /今の商品マスタの s002 の有効な JAN にありません/],
      [() => pick(dS2.id, 's002', String(ROW.s2a), JAN.s2b), 409, /有効な JAN にありません/],
      [() => pick(dSet.id, 'set001', String(ROW.s1), JAN.s1), 409, /セット・例外の商品なので JAN を持てません/],
      [() => pick(dNo.id, 'nosku1', String(ROW.s1), JAN.s1), 409, /商品マスタにありません/],
      [() => pick(dS2.id, 'v-red', String(ROW.vr), JAN.vr), 409, /出品の単位ではありません/],
      [() => pick(dS2.id, 's002', 'abc', JAN.s2a), 400, /code・row・value/],
      [() => pick(dS2.id, 's002', String(ROW.s2a), '123'), 400, /形式/],
      [() => pick(dS2.id, 's002', String(ROW.s2a), JAN.s2a, { origin: null }), 403, /Origin/],
      [() => pick(dS2.id, 's002', String(ROW.s2a), JAN.s2a, { origin: 'http://evil.example' }), 403, /Origin/],
    ];
    for (const [f, status, re] of cases) {
      const r = await f();
      assert.equal(r.status, status, r.text);
      assert.match(r.j.error, re);
    }
    const nj = await realFetch(`${ORIGIN}/apps/product-hub/api/drafts/${dS2.id}/jan-select`, { method: 'POST', headers: { Origin: ORIGIN, 'Content-Type': 'text/plain', 'x-test-session': 'user' }, body: 'x' });
    assert.equal(nj.status, 415);
    readable = false;
    try {
      const r = await pick(dS2.id, 's002', String(ROW.s2a), JAN.s2a);
      assert.equal(r.status, 503); assert.match(r.j.error, /読めないので選べません/);
    } finally { readable = true; }
    // 外された行 (選んだ後に商品マスタで外れた)
    const tmp = await addJan('s003', JAN.x1);
    await closeJan(tmp);
    const r = await pick(dS3.id, 's003', String(tmp), JAN.x1);
    assert.equal(r.status, 409, r.text);
    assert.deepEqual(fingerprint(), before);
  });

  await ta('[S4] まとまり = 子の SKU ごとに選ぶ・SKU のなしの理由を外す・ほかの SKU と同じ JAN = 断る・大文字小文字だけ違う行は 1 つに', async () => {
    ph.prepare('INSERT INTO draft_sku_catalog_exemptions (draft_id, sku_code, reason) VALUES (?, ?, 3)').run(dV.id, 'v-red');
    ph.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)').run(dV.id, 'v-blu', JAN.vr);   // 青に赤の JAN を打ち間違えた
    const dupR = await pick(dV.id, 'v-red', String(ROW.vr), JAN.vr);
    assert.equal(dupR.status, 409); assert.match(dupR.j.error, /別の SKU \(v-blu\) で使われています/);
    ph.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').run(dV.id, 'v-blu');
    const r = await pick(dV.id, 'V-RED', String(ROW.vr), JAN.vr);
    assert.equal(r.status, 200, r.text);
    assert.deepEqual(skuLinkOf(dV.id, 'v-red'), { jan_code: JAN.vr, jan_cdb_row: ROW.vr, jan_cdb_value: JAN.vr, jan_link_state: 'linked', jan_linked_by: 'staff@test' });
    assert.equal(ph.prepare('SELECT COUNT(*) AS n FROM draft_sku_catalog_exemptions WHERE draft_id = ? AND sku_code = ?').get(dV.id, 'v-red').n, 0);
    // 大文字小文字だけ違う行が 2 つ (古いデータ) = 選んだ行だけ残す
    ph.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?), (?, ?, ?)').run(dV.id, 'V-WHT', JAN.x1, dV.id, 'v-wht', JAN.z1);
    assert.equal(pickOf((await janCheck(dV.id)).j, 'v-wht').state, 'dup');
    const w = await pick(dV.id, 'v-wht', String(ROW.vw), JAN.vw);
    assert.equal(w.status, 200, w.text);
    assert.deepEqual(ph.prepare('SELECT sku_code, jan_code FROM draft_sku_jans WHERE draft_id = ? AND LOWER(sku_code) = ?').all(dV.id, 'v-wht'), [{ sku_code: 'v-wht', jan_code: JAN.vw }]);
    const units = (await janCheck(dV.id)).j.picks.units.map((u) => [u.code, u.state]);
    assert.deepEqual(units, [['v-blu', 'neither'], ['v-red', 'ok_jan'], ['v-wht', 'ok_jan']]);
  });

  await ta('[S5] 手の道 (基本情報・SKU 表) で JAN を変えた = 結んだ印を外す・同じ値なら残す / 止める設定のとき = 手で変えるのは断る (空は通す)', async () => {
    // 単品: 同じ値 = 残す
    let r = await call('POST', `/apps/product-hub/api/drafts/${dS1.id}`, { body: { jan_code: JAN.s1 } });
    assert.equal(r.status, 200, r.text);
    assert.equal(linkOf(dS1.id).jan_link_state, 'linked');
    // 違う値 = 外す
    r = await call('POST', `/apps/product-hub/api/drafts/${dS1.id}`, { body: { jan_code: JAN.x1 } });
    assert.equal(r.status, 200, r.text);
    assert.deepEqual(linkOf(dS1.id), { jan_code: JAN.x1, jan_cdb_row: null, jan_cdb_value: null, jan_link_state: null, jan_link_reason: null, jan_linked_by: null });
    // SKU 表: 違う値 = 外す
    r = await call('POST', `/apps/product-hub/api/drafts/${dV.id}/sku-jans`, { body: { ne_code: 'v-red', jan_code: JAN.vr } });
    assert.equal(r.status, 200, r.text);
    assert.equal(skuLinkOf(dV.id, 'v-red').jan_link_state, 'linked', '同じ値で外した');
    r = await call('POST', `/apps/product-hub/api/drafts/${dV.id}/sku-jans`, { body: { ne_code: 'v-red', jan_code: JAN.x1 } });
    assert.equal(r.status, 200, r.text);
    assert.deepEqual(skuLinkOf(dV.id, 'v-red'), { jan_code: JAN.x1, jan_cdb_row: null, jan_cdb_value: null, jan_link_state: null, jan_linked_by: null });
    // 止める設定 = 手で変えるのは断る (何も変えない)・空にするのは通す
    enforce(true);
    try {
      const before = fingerprint();
      r = await call('POST', `/apps/product-hub/api/drafts/${dS1.id}`, { body: { jan_code: JAN.s1 } });
      assert.equal(r.status, 400); assert.match(r.j.error, /商品マスタ \(Company DB\) の JAN から選んでください/);
      r = await call('POST', `/apps/product-hub/api/drafts/${dV.id}/sku-jans`, { body: { ne_code: 'v-red', jan_code: JAN.vr } });
      assert.equal(r.status, 400); assert.match(r.j.error, /商品マスタ \(Company DB\) の JAN から選んでください/);
      assert.deepEqual(fingerprint(), before);
      r = await call('POST', `/apps/product-hub/api/drafts/${dS1.id}`, { body: { jan_code: '' } });
      assert.equal(r.status, 200, r.text);
      r = await call('POST', `/apps/product-hub/api/drafts/${dV.id}/sku-jans`, { body: { ne_code: 'v-red', jan_code: '' } });
      assert.equal(r.status, 200, r.text);
      // 選ぶ道は通る
      r = await pick(dS1.id, 's001', String(ROW.s1), JAN.s1);
      assert.equal(r.status, 200, r.text);
      r = await pick(dV.id, 'v-red', String(ROW.vr), JAN.vr);
      assert.equal(r.status, 200, r.text);
      // 今と同じ値を送る (画面の保存の列) = 通す
      r = await call('POST', `/apps/product-hub/api/drafts/${dS1.id}`, { body: { jan_code: JAN.s1, name: '下書き s001' } });
      assert.equal(r.status, 200, r.text);
      assert.equal(linkOf(dS1.id).jan_link_state, 'linked');
    } finally { enforce(false); }
  });

  // ═══ G 段 4: 止める ═══
  console.log('G 出品の前に止める (段 4)');
  for (const d of [dS1, dS2, dS3, dM]) makeListable(d);
  makeListable(dV, { variation: [['v-red', '赤'], ['v-blu', '青'], ['v-wht', '白']] });

  await ta('[G1] 既定 (設定なし) = 何も止めない: 門は null・登録の前の判定は JAN を見ない・プレビューに「設定を入れたら止まる」の知らせ', async () => {
    for (const d of [dS1, dS2, dS3, dSet, dNo, dV]) assert.equal(LJ.janListingBlock(ph, d.id), null);
    assert.doesNotThrow(() => BL.assertRakutenListable(ph, dS2));
    const r = await call('GET', `/apps/product-hub/api/drafts/${dS2.id}/rakuten/preview`);
    assert.equal(r.j.ok, true, JSON.stringify(r.j.reasons));
    assert.ok(r.j.warnings.some((w) => /^カタログID \(JAN\) 商品マスタに結ばれていない: 商品コード s002 .*今は止めません。止める設定 PH_JAN_ENFORCE=1 を入れると出品が止まります/.test(w)), JSON.stringify(r.j.warnings));
    assert.equal(r.j.payload.variants.s002.articleNumber.value, JAN.s2b, '既定は今の jan_code を送る');
  });

  await ta('[G2] 設定 = 結ばれていない / 要確認 / 両方 / どちらも無い / 行が重なる を、詳細の登録とボードからの出品で楽天に触る前に止める (理由つき)', async () => {
    enforce(true);
    try {
      const expect = (d, re) => {
        const m = LJ.janListingBlock(ph, d.id);
        assert.match(String(m), re, `${d.ne_code}: ${m}`);
        assert.throws(() => BL.assertRakutenListable(ph, d), re);
      };
      expect(dS2, /商品マスタに結ばれていない: 商品コード s002 — .*直し方: カテゴリ・属性タブの「カタログID」で、商品マスタの JAN から選ぶ/);
      expect(dS3, /JAN も「なしの理由」も無い: 商品コード s003/);
      expect(dM, /商品マスタに結ばれていない: 商品コード m001/);   // 値は商品マスタと同じでも、選んでいない
      setReason(dS1.id, 3);
      expect(dS1, /JAN と「なしの理由」の両方: 商品コード s001/);
      setReason(dS1.id, null);
      setLink(dS2.id, null, null, 'needs_review', 'cdb_other_jan');
      expect(dS2, /要確認: 商品コード s002 — .*商品マスタにはこの商品の別の JAN がある/);
      expect(dV, /JAN も「なしの理由」も無い: SKU「v-blu」/);
      // 止めない: 選んである (s001)・なしの理由だけ (s003 に理由)
      assert.equal(LJ.janListingBlock(ph, dS1.id), null);
      setReason(dS3.id, 3);
      assert.equal(LJ.janListingBlock(ph, dS3.id), null);
      setReason(dS3.id, null);
      // router: 詳細の登録・ボードからの出品とも 400 で、楽天 (miniPC) に 1 回も触らない
      rmsCalls.length = 0;
      ph.prepare(`UPDATE draft_step_progress SET state = 'done' WHERE draft_id = ? AND step_code IN (SELECT code FROM ph_steps WHERE track = 'main' AND code <> 'listing')`).run(dS3.id);
      let r = await call('POST', `/apps/product-hub/api/drafts/${dS3.id}/rakuten/register`, { body: { confirm: true } });
      assert.equal(r.status, 400, r.text); assert.match(r.j.error, /JAN も「なしの理由」も無い/);
      r = await call('POST', `/apps/product-hub/api/drafts/${dS3.id}/rakuten/list-from-board`, { body: { confirm: true } });
      assert.equal(r.status, 400, r.text); assert.match(r.j.error, /JAN も「なしの理由」も無い/);
      assert.deepEqual(rmsCalls, []);
      // 行が重なる (大文字小文字だけ違う SKU の行が 2 つ)
      ph.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)').run(dV.id, 'V-RED', JAN.z1);
      expect(dV, /SKU の JAN の行が 2 つ: SKU「v-red」/);
      ph.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').run(dV.id, 'V-RED');
    } finally { enforce(false); setLink(dS2.id, null, null, null, null); }
  });

  await ta('[G3] 管理画面 (jan-link) に止める設定の今と「今止めたら止まる」数・入れ方', async () => {
    let r = await call('GET', '/apps/product-hub/jan-link', { session: 'admin' });
    assert.equal(r.status, 200);
    assert.ok(r.text.includes('出品の前に止める設定') && r.text.includes('止めない (印だけ・今までどおり)') && r.text.includes('PH_JAN_ENFORCE=1'), '設定の説明が無い');
    const f = LJ.enforceForecast(ph);
    assert.ok(f.stopDrafts >= 3 && f.byProblem.not_linked >= 2 && f.byProblem.neither >= 1, JSON.stringify(f));
    assert.ok(r.text.includes(`<div class="n">${f.stopDrafts}</div><div class="l">今止めたら止まる商品</div>`));
    enforce(true);
    try {
      r = await call('GET', '/apps/product-hub/jan-link', { session: 'admin' });
      assert.ok(r.text.includes('止める (<code>PH_JAN_ENFORCE=1</code>)'));
    } finally { enforce(false); }
    for (const m of [...r.text.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)]) { if (/application\/json/.test(m[1])) JSON.parse(m[2]); else if (!/\bsrc=/.test(m[1])) new vm.Script(m[2]); }
  });

  // ═══ P 段 5: 送る直前の 1 か所 ═══
  console.log('P 送る直前の 1 か所 (段 5)');
  await ta('[P1] 既定 = Company DB を読まずに今までどおり jan_code を送る (手で打った値もそのまま)', async () => {
    reads = 0; puts.length = 0;
    resetListed(dS2.id);
    const r = await RL.registerItem(dS2.id, { actor: 't' });
    assert.equal(r.ok, true, JSON.stringify(r));
    assert.equal(reads, 0, '既定で Company DB を読んだ');
    assert.equal(puts.length, 1);
    assert.deepEqual(puts[0].body.variants.s002.articleNumber, { value: JAN.s2b });
    resetListed(dS2.id);
  });

  await ta('[P2] 設定 = 結んだ JAN を Company DB で確かめ直し、送るのは Company DB の値 (全角で打った値でも)・なしの理由は理由だけ', async () => {
    enforce(true);
    try {
      ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(fullWidth(JAN.s1), dS1.id);   // 移す (jan-link) で結んだ古い値 = 全角
      setLink(dS1.id, ROW.s1, JAN.s1);
      reads = 0; puts.length = 0;
      resetListed(dS1.id);
      let r = await RL.registerItem(dS1.id, { actor: 't' });
      assert.equal(r.ok, true, JSON.stringify(r));
      assert.equal(reads, 1);
      assert.deepEqual(puts[0].body.variants.s001.articleNumber, { value: JAN.s1 });
      resetListed(dS1.id);
      // なしの理由だけ
      setReason(dS3.id, 3);
      puts.length = 0;
      r = await RL.registerItem(dS3.id, { actor: 't' });
      assert.equal(r.ok, true, JSON.stringify(r));
      assert.deepEqual(puts[0].body.variants.s003.articleNumber, { exemptionReason: 3 });
      resetListed(dS3.id);
    } finally { enforce(false); }
  });

  await ta('[P3] 設定 = 商品マスタで外された / 読めない / 結ばれていない = 送らない (理由つき・PUT 0 回)', async () => {
    enforce(true);
    try {
      puts.length = 0;
      readable = false;
      let r = await RL.registerItem(dS1.id, { actor: 't' });
      readable = true;
      assert.equal(r.ok, false); assert.ok(r.reasons.some((x) => /確かめられない: 商品コード s001 — 商品マスタ \(Company DB\) を読めない/.test(x)), JSON.stringify(r.reasons));
      // 商品マスタで外された (行を閉じて、同じ値を別の行で付け直した = 行番号が違う)
      await closeJan(ROW.s1);
      const again = await addJan('s001', JAN.s1);
      r = await RL.registerItem(dS1.id, { actor: 't' });
      assert.equal(r.ok, false); assert.ok(r.reasons.some((x) => /商品マスタで変わった: 商品コード s001 — .*有効な JAN に無い/.test(x)), JSON.stringify(r.reasons));
      // 選び直せば出せる
      const p = await pick(dS1.id, 's001', String(again), JAN.s1);
      assert.equal(p.status, 200, p.text);
      ROW.s1 = again;
      r = await RL.registerItem(dS2.id, { actor: 't' });
      assert.equal(r.ok, false); assert.ok(r.reasons.some((x) => /商品マスタに結ばれていない: 商品コード s002/.test(x)));
      assert.equal(puts.length, 0, '止めたのに送った');
    } finally { enforce(false); readable = true; }
  });

  await ta('[P4] 設定 = まとまりは SKU ごと: 選んだ SKU は Company DB の JAN・SKU の理由・ページ共通の理由 (どちらか 1 つ)', async () => {
    enforce(true);
    try {
      ph.prepare('INSERT INTO draft_sku_catalog_exemptions (draft_id, sku_code, reason) VALUES (?, ?, 3) ON CONFLICT(draft_id, sku_code) DO UPDATE SET reason = 3').run(dV.id, 'v-blu');
      ph.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').run(dV.id, 'v-wht');
      ph.prepare('UPDATE draft_rakuten SET catalog_id_exemption_reason = 5 WHERE draft_id = ?').run(dV.id);
      puts.length = 0;
      resetListed(dV.id);
      const r = await RL.registerItem(dV.id, { actor: 't' });
      assert.equal(r.ok, true, JSON.stringify(r));
      const v = puts[0].body.variants;
      assert.deepEqual([v['v-red'].articleNumber, v['v-blu'].articleNumber, v['v-wht'].articleNumber], [{ value: JAN.vr }, { exemptionReason: 3 }, { exemptionReason: 5 }]);
      resetListed(dV.id);
    } finally { enforce(false); }
  });

  await ta('[P5] 🚨 設定 = Company DB を読んでいる間に SQLite の JAN が変わった = 送らない (送る直前に照らす)', async () => {
    enforce(true);
    try {
      puts.length = 0;
      duringRead = async () => { ph.prepare('UPDATE draft_rakuten SET catalog_id_exemption_reason = 2 WHERE draft_id = ?').run(dS1.id); };
      setReason(dS1.id, null);
      const r = await RL.registerItem(dS1.id, { actor: 't' });
      assert.equal(r.ok, false);
      assert.ok(r.reasons.some((x) => /確かめている間に JAN が変わりました/.test(x)), JSON.stringify(r.reasons));
      assert.equal(puts.length, 0);
      setReason(dS1.id, null);
    } finally { enforce(false); duringRead = null; }
  });

  await ta('[P6] 送る直前の決まり (checkArticleNumbers): どちらか 1 つだけ・確かめた値と同じ・SKU が同じ・カタログID の属性も同じ', () => {
    const g = { enforce: true, ok: true, reasons: [], snapshot: null, units: [{ code: 'a1', decision: { ok: true, send: { value: '4900000000000' } } }, { code: 'a2', decision: { ok: true, send: { reason: 5 } } }] };
    const pl = (a1, a2, extra = {}) => ({ variants: { a1: { articleNumber: a1, ...extra }, a2: { articleNumber: a2 } } });
    assert.deepEqual(LJ.checkArticleNumbers(pl({ value: '4900000000000' }, { exemptionReason: 5 }), g), []);
    assert.deepEqual(LJ.checkArticleNumbers(pl({ value: 'x' }, { exemptionReason: 5 }), null), [], '既定は形だけ見る');
    const bad = (p, re, gate = g) => assert.ok(LJ.checkArticleNumbers(p, gate).some((x) => re.test(x)), JSON.stringify(LJ.checkArticleNumbers(p, gate)));
    bad(pl({ value: '4900000000000', exemptionReason: 5 }, { exemptionReason: 5 }), /どちらか 1 つだけ/, null);
    bad(pl({}, { exemptionReason: 5 }), /どちらか 1 つだけ/, null);
    bad(pl({ exemptionReason: 0 }, { exemptionReason: 5 }), /どちらか 1 つだけ/, null);
    bad(pl({ value: '4900000000017' }, { exemptionReason: 5 }), /確かめた商品マスタの値と違います/);
    bad(pl({ exemptionReason: 5 }, { exemptionReason: 5 }), /確かめた商品マスタの値と違います/);
    bad(pl({ value: '4900000000000' }, { exemptionReason: 3 }), /確かめた商品マスタの値と違います/);
    bad(pl({ value: '4900000000000' }, { exemptionReason: 5 }, { attributes: [{ name: 'カタログID', values: ['4900000000017'] }] }), /属性「カタログID」/);
    bad({ variants: { a1: { articleNumber: { value: '4900000000000' } } } }, /SKU が合いません/);
    bad({ variants: {} }, /送る SKU がありません/, null);
    bad(pl({ value: '4900000000000' }, { exemptionReason: 5 }), /止めた理由/, { ...g, ok: false, reasons: ['止めた理由'] });
  });

  await ta('[P7] 設定があるのに確かめた結果を渡さない payload = 止める・プレビューは登録と同じ理由と同じ値', async () => {
    enforce(true);
    try {
      const b = RL.buildItemPayload(ph, dS1.id, TAX);
      assert.equal(b.ok, false);
      assert.ok(b.reasons.includes('カタログID (JAN) を商品マスタ (Company DB) で確かめていません (出品を止めました)'));
      let r = await call('GET', `/apps/product-hub/api/drafts/${dS1.id}/rakuten/preview`);
      assert.equal(r.j.ok, true, JSON.stringify(r.j.reasons));
      assert.deepEqual(r.j.payload.variants.s001.articleNumber, { value: JAN.s1 });
      assert.ok(!(r.j.warnings || []).some((w) => /今は止めません/.test(w)), '設定があるのに「今は止めません」');
      r = await call('GET', `/apps/product-hub/api/drafts/${dS2.id}/rakuten/preview`);
      assert.equal(r.j.ok, false);
      assert.ok(r.j.reasons.some((x) => /商品マスタに結ばれていない: 商品コード s002/.test(x)), JSON.stringify(r.j.reasons));
    } finally { enforce(false); }
  });

  await ta('[P8] ソース: 登録は「確かめる → payload → 送る直前の決まり → PUT」の順・決まりと PUT の間に await を挟まない・送る道は registerItem だけ', () => {
    const src = fs.readFileSync(new URL('../apps/product-hub/services/rakuten-listing.js', import.meta.url), 'utf8');
    const at = src.indexOf('export async function registerItem(');
    const body = src.slice(at, src.indexOf('\nexport ', at + 10));
    const iGate = body.indexOf('const janGate = await resolveListingJans(db, draftId);');
    const iBuild = body.indexOf('const built = buildItemPayload(db, draftId, { tax, jan: janGate });');
    const iCheck = body.indexOf('const articleProblems = checkArticleNumbers(built.payload, janGate, { db, draftId });');
    const iPut = body.indexOf("r = await callWarehouse(itemPath, { method: 'PUT', body: built.payload });");
    assert.ok(iGate > 0 && iGate < iBuild && iBuild < iCheck && iCheck < iPut, `順番 ${[iGate, iBuild, iCheck, iPut]}`);
    assert.ok(!/\bawait\b/.test(body.slice(iCheck, iPut).replace(/\/\/[^\n]*/g, '').replace("r = await callWarehouse(itemPath, { method: 'PUT'", '')), '決まりと PUT の間に await');
    assert.equal((src.match(/method: 'PUT', body: built\.payload/g) || []).length, 1);
  });
} finally {
  enforce(false);
  server.close();
  globalThis.fetch = realFetch;
  try { await pg.close(); } catch { /* */ }
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows はハンドルが残ることがある */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
process.exit(process.exitCode || 0);
