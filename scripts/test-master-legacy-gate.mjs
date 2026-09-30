/**
 * test-master-legacy-gate.mjs — マスタの古い入口の門 (lib/master-legacy-gate.mjs・config/master-legacy-entries.mjs) の試験
 * (Company DB構想 14 §5・§9 v2 M2・§10 契約 v3 H1)
 *
 *   A. 門の部品: 段階ごとの可否・読めない = 閉じる・前に読めた値の使い回し (5 分・段階が変わったのを見たら使わない)・画面の 30 秒・答えの形・帯
 *   B. Company DB (PGlite・0050 まで流す): 段階を 1 段ずつ進めて門が閉じる / 見張りのロール watcher でも段階を読める / ack の関数が無い・ある
 *   C. 入口ごと (本物の router を HTTP で): legacy_open = 今までどおり書ける / frozen・company_owner・new_open = 410 で何も書かない /
 *      読めない = 503 で何も書かない / 画面は帯と書く部品を隠す。miniPC の /register (全部の API)・会計アプリ 5 つ・fba-profitability・
 *      profit-calculator・product-hub の税率
 *   D. CLI (子プロセス): 閉じた mode は DB に触らず終了コード 3 / 止めない mode は動く / 読めない = 3
 *
 * 使い方: node scripts/test-master-legacy-gate.mjs (一時の DATA_DIR・PGlite だけ。本番の DB・API にはつながない)
 */
import { temporaryTestDataDir } from './test-temp-dir.mjs';
const DATA_DIR = await temporaryTestDataDir(import.meta.url, 'mlg-');

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import express from 'express';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
// 本番の接続先は子プロセスにも渡さない (この試験は PGlite と偽の読み方だけ)
for (const k of ['COMPANY_DB_URL', 'COMPANY_DB_WATCH_URL', 'COMPANY_DB_WATCH_WRITER_URL']) delete process.env[k];
process.env.WAREHOUSE_API_KEY = '';
delete process.env.RENDER;

const G = await import('../lib/master-legacy-gate.mjs');
const E = await import('../config/master-legacy-entries.mjs');
const { readCutoverPhase, CUTOVER_PHASES } = await import('../lib/master-cutover.mjs');

let passed = 0;
async function t(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const CLOSED = ['frozen', 'company_owner', 'new_open'];
const phaseReader = (phase) => async () => (phase === 'unreadable' ? { readable: false, phase: null, error: '試験: 読めない' } : { readable: true, phase });
const setPhase = (phase) => G.__setLegacyPhaseReader(phaseReader(phase));

// ═══ A. 門の部品 ═══
console.log('── A. 門の部品 ──');
await t('legacy_open だけ書ける・frozen / company_owner / new_open は書けない (持ち主表とは別の門)', async () => {
  for (const p of CUTOVER_PHASES) {
    setPhase(p);
    const s = await G.checkLegacyGate();
    assert.equal(s.writable, p === 'legacy_open', p);
    assert.equal(s.readable, true);
    assert.equal(s.source, 'db');
  }
});
await t('読めない = 書けない (前に読めた値が無い)・知らない段階 = 読めない', async () => {
  setPhase('unreadable');
  let s = await G.checkLegacyGate();
  assert.equal(s.writable, false); assert.equal(s.readable, false); assert.equal(s.source, 'unreadable');
  G.__setLegacyPhaseReader(async () => ({ readable: true, phase: 'opened_by_typo' }));
  s = await G.checkLegacyGate();
  assert.equal(s.writable, false); assert.equal(s.source, 'unreadable');
  G.__setLegacyPhaseReader(async () => { throw new Error('つながらない'); });
  s = await G.checkLegacyGate();
  assert.equal(s.writable, false);
});
await t('前に読めた legacy_open は 5 分だけ使う (途切れで日々の作業を止めない)・5 分を過ぎたら閉じる・上限は 10 分', async () => {
  let now = Date.parse('2026-10-01T00:00:00Z');
  G.__setLegacyClock(() => now);
  let mode = 'legacy_open';
  G.__setLegacyPhaseReader(async () => (mode === 'down' ? { readable: false, phase: null, error: 'down' } : { readable: true, phase: mode }));
  assert.equal((await G.checkLegacyGate()).writable, true);
  mode = 'down'; now += 4 * 60 * 1000;
  let s = await G.checkLegacyGate();
  assert.equal(s.writable, true); assert.equal(s.source, 'last_good'); assert.equal(s.readable, false);
  now += 2 * 60 * 1000;   // 最後に読めてから 6 分
  s = await G.checkLegacyGate();
  assert.equal(s.writable, false); assert.equal(s.source, 'unreadable');
  // 使い回しの長さは 10 分より長くできない
  mode = 'legacy_open'; await G.checkLegacyGate();
  mode = 'down'; now += 11 * 60 * 1000;
  assert.equal((await G.checkLegacyGate({ fallbackMs: 60 * 60 * 1000 })).writable, false);
  G.__setLegacyClock(null);
});
await t('frozen を一度読んだら、その後に途切れても前の legacy_open は使わない (段階が変わったのをまたいで使い回さない)', async () => {
  let mode = 'legacy_open';
  G.__setLegacyPhaseReader(async () => (mode === 'down' ? { readable: false, phase: null, error: 'down' } : { readable: true, phase: mode }));
  assert.equal((await G.checkLegacyGate()).writable, true);
  mode = 'frozen';
  assert.equal((await G.checkLegacyGate()).writable, false);
  mode = 'down';
  const s = await G.checkLegacyGate();
  assert.equal(s.writable, false); assert.equal(s.source, 'unreadable');
});
await t('書き込みは毎回読む・画面は 30 秒だけ前の結果を使う', async () => {
  let calls = 0, now = Date.parse('2026-10-01T01:00:00Z');
  G.__setLegacyClock(() => now);
  G.__setLegacyPhaseReader(async () => { calls++; return { readable: true, phase: 'legacy_open' }; });
  await G.checkLegacyGate(); await G.checkLegacyGate();
  assert.equal(calls, 2, '書き込みは毎回');
  await G.checkLegacyGate({ purpose: 'screen' });
  assert.equal(calls, 2, '画面は直前の結果 (30 秒以内)');
  now += 31 * 1000;
  await G.checkLegacyGate({ purpose: 'screen' });
  assert.equal(calls, 3, '30 秒を過ぎたら読む');
  G.__setLegacyClock(null);
});
await t('同時に来た要求は 1 回の読みを分け合う (照会用ロールの接続の数を増やさない)・終わったら次はまた読む', async () => {
  let calls = 0, release;
  const gate = new Promise((r) => { release = r; });
  G.__setLegacyPhaseReader(async () => { calls++; await gate; return { readable: true, phase: 'frozen' }; });
  const both = Promise.all([G.checkLegacyGate(), G.checkLegacyGate(), G.checkLegacyGate({ purpose: 'screen' })]);
  release();
  const rs = await both;
  assert.equal(calls, 1);
  assert.ok(rs.every((s) => s.writable === false && s.phase === 'frozen'));
  await G.checkLegacyGate();
  assert.equal(calls, 2);
});
await t('断る答え = 閉じた 410 {error:master_frozen, message, url} / 読めない 503 {error:master_phase_unreadable}', async () => {
  const [s1, b1] = G.refusal({ writable: false, readable: true, phase: 'frozen' }, { id: 'x' });
  assert.equal(s1, 410); assert.equal(b1.error, 'master_frozen'); assert.equal(b1.url, E.MASTER_EDIT_URL); assert.ok(b1.message.includes('新しい画面'));
  const [s2, b2] = G.refusal({ writable: false, readable: false, phase: null }, { id: 'x' });
  assert.equal(s2, 503); assert.equal(b2.error, 'master_phase_unreadable'); assert.equal(b2.url, E.MASTER_EDIT_URL);
});
await t('道の形 (Express 4 と同じ: 大文字小文字を区別しない・末尾の / は任意・:param は 1 段だけ)', async () => {
  const re = G.pathPattern('/api/shipping/:sku');
  assert.ok(re.test('/api/shipping/abc')); assert.ok(re.test('/API/Shipping/abc/')); assert.ok(!re.test('/api/shipping/abc/x')); assert.ok(!re.test('/api/shipping'));
  assert.ok(G.pathPattern('/').test('/')); assert.ok(!G.pathPattern('/').test('/x'));
  assert.ok(G.pathPattern('/api/drafts/:id').test('/api/drafts/5')); assert.ok(!G.pathPattern('/api/drafts/:id').test('/api/drafts/5/yahoo'));
});
await t('帯: 閉じていなければ空 (今までどおり)・閉じたら文言・新しい画面への道・隠す部品・文字を逃がす', async () => {
  assert.equal(G.legacyBannerHtml(G.screenInfo({ writable: true, readable: true, phase: 'legacy_open' })), '');
  assert.equal(G.legacyBannerHtml(null), '');
  const h = G.legacyBannerHtml(G.screenInfo({ writable: false, readable: true, phase: 'frozen' }), { hideSelectors: ['#a', '.b'] });
  assert.ok(h.includes('マスタは新しい画面で直します ↗')); assert.ok(h.includes(E.MASTER_EDIT_URL)); assert.ok(h.includes('#a,.b{display:none !important}'));
  const h2 = G.legacyBannerHtml(G.screenInfo({ writable: false, readable: false, phase: null }));
  assert.ok(h2.includes('読めない'));
});
await t('一覧: app ごとの入口がある・知らない app の門は起動時に落ちる・CLI の入口を file と mode で引ける', async () => {
  for (const app of ['warehouse', 'aupay-accounting', 'yahoo-accounting', 'mercari-accounting', 'linegift-accounting', 'qoo10-accounting', 'fba-profitability', 'product-hub', 'profit-calculator']) {
    assert.ok(E.entriesForApp(app).length > 0, app);
  }
  assert.throws(() => G.masterLegacyGate('no-such-app'));
  assert.equal(E.cliEntry('apps/warehouse/csv-import.js', 'product_shipping').id, 'cli:csv-import.js product_shipping');
  assert.equal(E.cliEntry('apps/warehouse/csv-import.js', 'orders'), null);
  assert.equal(E.cliEntry('apps/warehouse/import-sku-master.js', 'anything').id, 'cli:import-sku-master.js');
});
await t('CLI の門: legacy_open = 通す / 閉じた・読めない = 理由を出して終了コード 3 (process.exit は呼ばない)', async () => {
  const logs = [];
  const saved = process.exitCode;
  assert.equal(await G.legacyCliGate('cli:import-sales-class.js', { read: phaseReader('legacy_open'), log: (m) => logs.push(m) }), true);
  for (const p of [...CLOSED, 'unreadable']) {
    process.exitCode = undefined;
    assert.equal(await G.legacyCliGate('cli:import-sales-class.js', { read: phaseReader(p), log: (m) => logs.push(m) }), false, p);
    assert.equal(process.exitCode, G.CLI_EXIT_CODE);
  }
  process.exitCode = saved;
  assert.ok(logs.some((m) => m.includes('マスタは新しい画面で直します') && m.includes(E.MASTER_EDIT_URL)));
  assert.ok(logs.some((m) => m.includes('読めない')));
  await assert.rejects(() => G.legacyCliGate('no-such-entry', { read: phaseReader('legacy_open') }));
});

// ═══ B. Company DB (PGlite) ═══
console.log('── B. Company DB (PGlite) ──');
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const cdb = pgliteAdapter(pg);
await applyMigrations(cdb, { log: () => {} });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });

await t('Company DB の段階を 1 段ずつ進めると、門は frozen から閉じる (持ち主表はまだ全部 load のまま)', async () => {
  const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
  assert.ok(Object.values(MASTER_OWNERSHIP).every((v) => v === 'load'), '持ち主表は load のまま');
  G.__setLegacyPhaseReader(() => readCutoverPhase(cdb));
  assert.equal((await G.checkLegacyGate()).writable, true, 'legacy_open');
  for (const to of CLOSED) {
    await cdb.query('select ops.set_master_cutover_phase($1, $2, $3)', [to, 'test@b-faith.biz', '試験']);
    const s = await G.checkLegacyGate();
    assert.equal(s.phase, to); assert.equal(s.writable, false, to);
  }
});
await t('miniPC の読み方 = 見張りの照会用ロール watcher で段階を読める (新しい秘密・権限を足さない)', async () => {
  await pg.query('set role watcher');
  try {
    const s = await readCutoverPhase(cdb);
    assert.equal(s.readable, true, s.error); assert.equal(s.phase, 'new_open');
  } finally { await pg.query('set role deploy'); }
});
await t('表が無い (0050 の前の DB) = 読めない = 閉じる', async () => {
  const pg2 = new PGlite();
  G.__setLegacyPhaseReader(() => readCutoverPhase(pgliteAdapter(pg2)));
  const s = await G.checkLegacyGate();
  assert.equal(s.writable, false); assert.equal(s.readable, false);
  await pg2.close();
});
await t('ack: 関数がまだ無い = no_function (何もしない) / 接続先が無い = not_configured / 関数があれば版・持ち主表の指紋・段階を書く', async () => {
  const connect = async () => ({ db: cdb, close: async () => {} });
  assert.equal((await G.ackLegacyGates({ host: 'render', env: {} })).state, 'not_configured');
  assert.equal((await G.ackLegacyGates({ host: null, connect })).state, 'not_configured');
  assert.equal((await G.ackLegacyGates({ host: 'render', connect })).state, 'no_function');
  // ⑤-1 の直しで入る表と関数の形 (契約: host・build_id・owner_hash・legacy_gates_version・phase_seen・acked_at) をここだけで仮に作る
  await cdb.exec(`create table ops.test_master_legacy_gate_acks (host text primary key, build_id text, owner_hash text not null, legacy_gates_version integer not null, phase_seen text, acked_at timestamptz not null default now());
    create function ops.ack_master_legacy_gate(p_host text, p_build_id text, p_owner_hash text, p_version integer, p_phase_seen text) returns void language plpgsql as $$
    begin
      insert into ops.test_master_legacy_gate_acks (host, build_id, owner_hash, legacy_gates_version, phase_seen) values (p_host, p_build_id, p_owner_hash, p_version, p_phase_seen)
        on conflict (host) do update set build_id = excluded.build_id, owner_hash = excluded.owner_hash, legacy_gates_version = excluded.legacy_gates_version, phase_seen = excluded.phase_seen, acked_at = now();
    end $$;`);
  const r = await G.ackLegacyGates({ host: 'minipc', connect });
  assert.equal(r.state, 'acked', r.detail);
  const row = (await cdb.query('select * from ops.test_master_legacy_gate_acks where host = $1', ['minipc'])).rows[0];
  const fp = await G.legacyGateFingerprint();
  assert.equal(row.legacy_gates_version, E.LEGACY_GATES_VERSION); assert.equal(row.owner_hash, fp.owner_hash); assert.equal(row.phase_seen, 'new_open');
  await cdb.exec('drop function ops.ack_master_legacy_gate(text, text, text, integer, text); drop table ops.test_master_legacy_gate_acks;');
});

// ═══ C. 入口ごと (本物の router を HTTP で) ═══
console.log('── C. 入口ごと (HTTP) ──');
const { initDB, getDB } = await import('../apps/warehouse/db.js');
await initDB();
const whdb = getDB();
whdb.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 原価, 消費税率) VALUES (?, ?, ?, ?)').run('ne-aaa', 'NE-A', 100, 10);
whdb.prepare("INSERT OR REPLACE INTO shipping_rates (shipping_code, 小分類区分名称, 配送関係費合計) VALUES ('S01', 'ゆうパケット', 300)").run();
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const mdb = getMirrorDB();
mdb.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価状態, 消費税率, 売上分類, 原価, updated_at) VALUES ('mp-1', 'M1', '単品', 'OK', 0.1, 3, 500, '2026-10-01 00:00:00')`).run();

const whMod = await import('../apps/warehouse/router.js');
for (let i = 0; i < 200 && !whMod.isWarehouseDbReady(); i++) await new Promise((r) => setTimeout(r, 20));
const app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(ROOT, 'views'));
app.use(express.json());
app.use((req, res, next) => { req.session = { email: 'test@b-faith.biz', role: 'admin' }; next(); });
app.use('/apps/warehouse', whMod.default);
const ACC = ['aupay', 'yahoo', 'mercari', 'linegift', 'qoo10'];
for (const a of ACC) app.use(`/apps/${a}-accounting`, (await import(`../apps/${a}-accounting/router.js`)).default);
app.use('/apps/fba-profitability', (await import('../apps/fba-profitability/router.js')).default);
app.use('/apps/profit-calculator', (await import('../apps/profit-calculator/router.js')).default);
const phMod = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', phMod.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;

async function call(method, p, body, { csv = null } = {}) {
  const opts = { method, headers: {} };
  if (csv != null) {
    const fd = new FormData();
    fd.append('file', new Blob([csv], { type: 'text/csv' }), 'x.csv');
    opts.body = fd;
  } else if (body !== undefined) {
    opts.headers['content-type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(base + p, opts);
  const text = await res.text();
  let json = null; try { json = JSON.parse(text); } catch { /* 画面 */ }
  return { status: res.status, json, text };
}

// miniPC の /register (warehouse の router): 一覧の全部の API に要求の見本を持つ (一覧に足したら、ここにも足さないと落ちる)
const WH_SAMPLES = {
  'warehouse:POST /api/shipping': ['POST', '/api/shipping', { sku: 'ne-aaa', shipping_code: 'S01', ship_method: 'ゆうパケット', ship_cost: 300 }],
  'warehouse:POST /api/genka': ['POST', '/api/genka', { sku: 'ne-aaa', genka: 120 }],
  'warehouse:POST /api/csv/shipping': ['POST', '/api/csv/shipping', undefined, '商品コード,送料コード\nne-aaa,S01\n'],
  'warehouse:POST /api/csv/genka': ['POST', '/api/csv/genka', undefined, 'ne-aaa,150\n'],
  'warehouse:POST /api/csv/m-sku-master': ['POST', '/api/csv/m-sku-master', undefined, 'sku,asin,商品名,NE商品コード,数量\nsku-csv,B000,CSVの品,ne-aaa,1\n'],
  'warehouse:DELETE /api/shipping/:sku': ['DELETE', '/api/shipping/ne-aaa'],
  'warehouse:DELETE /api/genka/:sku': ['DELETE', '/api/genka/ne-aaa'],
  'warehouse:DELETE /api/sales_class/:sku': ['DELETE', '/api/sales_class/ne-aaa'],
  'warehouse:DELETE /api/tax_rate/:sku': ['DELETE', '/api/tax_rate/ne-aaa'],
  'warehouse:POST /api/sales_class': ['POST', '/api/sales_class', { sku: 'ne-aaa', sales_class: 1 }],
  'warehouse:POST /api/csv/sales_class': ['POST', '/api/csv/sales_class', undefined, 'ne-aaa,2\n'],
  'warehouse:POST /api/reorder_setting': ['POST', '/api/reorder_setting', { sku: 'ne-aaa', 推奨保有月数: 3 }],
  'warehouse:DELETE /api/reorder_setting/:sku': ['DELETE', '/api/reorder_setting/ne-aaa'],
  'warehouse:POST /api/csv/reorder_setting': ['POST', '/api/csv/reorder_setting', undefined, 'ne-aaa,2\n'],
  'warehouse:POST /api/tax_rate': ['POST', '/api/tax_rate', { sku: 'ne-aaa', tax_rate: '0.1' }],
  'warehouse:POST /api/csv/tax_rate': ['POST', '/api/csv/tax_rate', undefined, 'ne-aaa,0.08\n'],
  'warehouse:POST /api/m-sku-master': ['POST', '/api/m-sku-master', { seller_sku: 'sku-x', 商品名: 'X', components: [{ ne_code: 'ne-aaa', 数量: 1 }] }],
  'warehouse:PUT /api/m-sku-master/:sku': ['PUT', '/api/m-sku-master/sku-x', { 商品名: 'X2', components: [{ ne_code: 'ne-aaa', 数量: 2 }] }],
  'warehouse:DELETE /api/m-sku-master/:sku': ['DELETE', '/api/m-sku-master/sku-x'],
};
const WH_TABLES = ['product_shipping', 'exception_genka', 'product_sales_class', 'product_tax_rate', 'm_reorder_setting', 'm_sku_master', 'm_sku_components', 'm_products', 'audit_log'];
const whSnap = () => JSON.stringify(WH_TABLES.map((tb) => whdb.prepare(`SELECT * FROM ${tb} ORDER BY rowid`).all()));
const whRoutes = E.entriesForApp('warehouse').filter((e) => e.kind === 'route');
const whReq = (id) => { const [m, p, b, csv] = WH_SAMPLES[id]; return call(m, `/apps/warehouse${p}`, b, { csv }); };

await t('miniPC /register: 一覧の全部の API に試験の見本がある', async () => {
  const noSample = whRoutes.filter((e) => !WH_SAMPLES[e.id]).map((e) => e.id);
  assert.deepEqual(noSample, []);
  assert.equal(whRoutes.length, 19);
});
for (const phase of [...CLOSED, 'unreadable']) {
  await t(`miniPC /register: ${phase} = 全部の API が ${phase === 'unreadable' ? 503 : 410} で、上書き表・m_products・SKU マスタに何も書かない`, async () => {
    setPhase(phase);
    for (const e of whRoutes) {
      const before = whSnap();
      const r = await whReq(e.id);
      assert.equal(r.status, phase === 'unreadable' ? 503 : 410, `${e.id}: ${r.status} ${r.text.slice(0, 200)}`);
      assert.equal(r.json.error, phase === 'unreadable' ? 'master_phase_unreadable' : 'master_frozen', e.id);
      assert.equal(r.json.url, E.MASTER_EDIT_URL, e.id);
      assert.equal(whSnap(), before, `${e.id} が書いた`);
    }
  });
}
await t('miniPC /register: legacy_open = 今までどおり書ける (登録 → CSV → 削除 → SKU マスタ)', async () => {
  setPhase('legacy_open');
  const results = {};
  for (const id of ['warehouse:POST /api/shipping', 'warehouse:POST /api/genka', 'warehouse:POST /api/sales_class', 'warehouse:POST /api/tax_rate', 'warehouse:POST /api/reorder_setting',
    'warehouse:POST /api/csv/genka', 'warehouse:POST /api/csv/shipping', 'warehouse:POST /api/csv/sales_class', 'warehouse:POST /api/csv/tax_rate', 'warehouse:POST /api/csv/reorder_setting',
    'warehouse:POST /api/m-sku-master', 'warehouse:PUT /api/m-sku-master/:sku', 'warehouse:POST /api/csv/m-sku-master']) {
    const r = await whReq(id);
    results[id] = r;
    assert.equal(r.status < 300, true, `${id}: ${r.status} ${r.text.slice(0, 200)}`);
  }
  assert.equal(whdb.prepare("SELECT ship_cost FROM product_shipping WHERE sku = 'ne-aaa'").get().ship_cost, 300);
  assert.equal(whdb.prepare("SELECT genka FROM exception_genka WHERE sku = 'ne-aaa'").get().genka, 150);
  assert.equal(whdb.prepare("SELECT sales_class FROM product_sales_class WHERE sku = 'ne-aaa'").get().sales_class, 2);
  assert.equal(whdb.prepare("SELECT tax_rate FROM product_tax_rate WHERE sku = 'ne-aaa'").get().tax_rate, 0.08);
  assert.equal(whdb.prepare("SELECT 推奨保有月数 AS m FROM m_reorder_setting WHERE sku = 'ne-aaa'").get().m, 2);
  assert.equal(whdb.prepare("SELECT 商品名 FROM m_sku_master WHERE seller_sku = 'sku-x'").get().商品名, 'X2');
  assert.ok(whdb.prepare("SELECT 1 FROM m_sku_master WHERE seller_sku = 'sku-csv'").get());
  for (const id of ['warehouse:DELETE /api/shipping/:sku', 'warehouse:DELETE /api/genka/:sku', 'warehouse:DELETE /api/sales_class/:sku', 'warehouse:DELETE /api/tax_rate/:sku',
    'warehouse:DELETE /api/reorder_setting/:sku', 'warehouse:DELETE /api/m-sku-master/:sku']) {
    const r = await whReq(id);
    assert.equal(r.status, 200, `${id}: ${r.status} ${r.text.slice(0, 200)}`);
  }
  assert.equal(whdb.prepare("SELECT count(*) AS c FROM product_shipping WHERE sku = 'ne-aaa'").get().c, 0);
  assert.equal(whdb.prepare("SELECT count(*) AS c FROM m_sku_master WHERE seller_sku = 'sku-x'").get().c, 0);
});
await t('miniPC /register: 一覧に無い要求 (読むだけの GET) は段階を読まずに通る', async () => {
  let calls = 0;
  G.__setLegacyPhaseReader(async () => { calls++; return { readable: false, phase: null, error: 'x' }; });
  const r = await call('GET', '/apps/warehouse/api/shipping/list');
  assert.equal(r.status, 200); assert.equal(calls, 0);
});
await t('miniPC の画面 (/register と /): 閉じたら帯と書く部品を隠す・legacy_open は今までどおり (帯なし)', async () => {
  setPhase('frozen'); G.__resetLegacyGate();
  let r = await call('GET', '/apps/warehouse/register');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('マスタは新しい画面で直します ↗') && r.text.includes(E.MASTER_EDIT_URL));
  assert.ok(r.text.includes('#csv-card') && r.text.includes('[data-act^="reg-"]') && r.text.includes('#sku-modal-submit'));
  assert.ok(r.text.includes('id="csv-card"'), 'CSV の取込の枠に id がある (隠す)');
  r = await call('GET', '/apps/warehouse/');
  assert.ok(r.text.includes('マスタは新しい画面で直します ↗') && r.text.includes('[data-action="delete"]'));
  setPhase('legacy_open'); G.__resetLegacyGate();
  r = await call('GET', '/apps/warehouse/register');
  assert.ok(!r.text.includes('master-legacy-banner'));
  r = await call('GET', '/apps/warehouse/');
  assert.ok(!r.text.includes('master-legacy-banner'));
});
await t('読み戻し GET /apps/warehouse/api/master-legacy-gate = この環境の段階・書けるか・門の版・持ち主表の指紋', async () => {
  setPhase('frozen');
  const r = await call('GET', '/apps/warehouse/api/master-legacy-gate');
  assert.equal(r.status, 200);
  assert.equal(r.json.phase, 'frozen'); assert.equal(r.json.writable, false); assert.equal(r.json.host, 'minipc');
  assert.equal(r.json.legacy_gates_version, E.LEGACY_GATES_VERSION);
  assert.match(r.json.owner_hash, /^[0-9a-f]{16}$/);
});

// 会計アプリ 5 つ
const mirrorRow = () => JSON.stringify(mdb.prepare("SELECT 消費税率, 売上分類, 原価, 原価ソース, 原価状態 FROM mirror_products WHERE 商品コード = 'mp-1'").get());
for (const a of ACC) {
  await t(`${a}-accounting: POST /register は閉じたら 410 / 読めない 503 で mirror_products を変えない・legacy_open は今までどおり・画面は帯`, async () => {
    for (const phase of [...CLOSED, 'unreadable']) {
      setPhase(phase);
      const before = mirrorRow();
      const r = await call('POST', `/apps/${a}-accounting/register`, { items: [{ code: 'mp-1', taxRate: 8, segment: 1 }] });
      assert.equal(r.status, phase === 'unreadable' ? 503 : 410, `${phase} ${r.status} ${r.text.slice(0, 200)}`);
      assert.equal(mirrorRow(), before);
    }
    setPhase('frozen'); G.__resetLegacyGate();
    let page = await call('GET', `/apps/${a}-accounting/`);
    assert.ok(page.text.includes('マスタは新しい画面で直します ↗') && page.text.includes('#registerBtn,.reg-sel{display:none !important}'));
    setPhase('legacy_open'); G.__resetLegacyGate();
    page = await call('GET', `/apps/${a}-accounting/`);
    assert.ok(!page.text.includes('master-legacy-banner'));
    const r = await call('POST', `/apps/${a}-accounting/register`, { items: [{ code: 'mp-1', taxRate: 8, segment: 1 }] });
    assert.equal(r.status, 200); assert.equal(r.json.updatedTax, 1); assert.equal(r.json.updatedSeg, 1);
    mdb.prepare("UPDATE mirror_products SET 消費税率 = 0.1, 売上分類 = 3 WHERE 商品コード = 'mp-1'").run();
  });
}

// fba-profitability
await t('fba-profitability: 原価の手入力は閉じたら 410 / 読めない 503 で mirror_products を変えない・legacy_open は今までどおり・画面は帯と原価の部品を隠す', async () => {
  for (const phase of [...CLOSED, 'unreadable']) {
    setPhase(phase);
    const before = mirrorRow();
    const r = await call('POST', '/apps/fba-profitability/api/update-cost', { sku: 'mp-1', cost: 999 });
    assert.equal(r.status, phase === 'unreadable' ? 503 : 410, phase);
    assert.equal(mirrorRow(), before);
  }
  setPhase('frozen'); G.__resetLegacyGate();
  let page = await call('GET', '/apps/fba-profitability/');
  assert.equal(page.status, 200, page.text.slice(0, 300));
  assert.ok(page.text.includes('マスタは新しい画面で直します ↗') && page.text.includes('button[onclick^="openCostModal"]'));
  setPhase('legacy_open'); G.__resetLegacyGate();
  page = await call('GET', '/apps/fba-profitability/');
  assert.ok(!page.text.includes('master-legacy-banner'));
  const r = await call('POST', '/apps/fba-profitability/api/update-cost', { sku: 'mp-1', cost: 999 });
  assert.equal(r.status, 200);
  assert.equal(mdb.prepare("SELECT 原価 FROM mirror_products WHERE 商品コード = 'mp-1'").get().原価, 999);
});

// profit-calculator
const suppliersFile = path.join(DATA_DIR, 'suppliers.json');
const supSnap = () => (fs.existsSync(suppliersFile) ? fs.readFileSync(suppliersFile, 'utf8') : null);
await t('profit-calculator: NE 用 CSV・仕入れ先の追加・削除は閉じたら 410 / 読めない 503 (suppliers.json を変えない・CSV を作らない)・legacy_open は今までどおり', async () => {
  for (const phase of [...CLOSED, 'unreadable']) {
    setPhase(phase);
    const before = supSnap();
    const want = phase === 'unreadable' ? 503 : 410;
    assert.equal((await call('POST', '/apps/profit-calculator/api/suppliers', { code: '7777', name: '試験の仕入先' })).status, want, phase);
    assert.equal((await call('DELETE', '/apps/profit-calculator/api/suppliers', { code: '0001' })).status, want, phase);
    const csv = await call('GET', '/apps/profit-calculator/api/products/csv/ne?type=single');
    assert.equal(csv.status, want); assert.equal(csv.json.url, E.MASTER_EDIT_URL);
    assert.equal(supSnap(), before);
  }
  setPhase('legacy_open');
  const r = await call('POST', '/apps/profit-calculator/api/suppliers', { code: '7777', name: '試験の仕入先' });
  assert.equal(r.status, 200);
  assert.ok(JSON.parse(supSnap()).some((s) => s.code === '7777'));
  assert.equal((await call('DELETE', '/apps/profit-calculator/api/suppliers', { code: '7777' })).status, 200);
  assert.ok(!JSON.parse(supSnap()).some((s) => s.code === '7777'));
});
await t('profit-calculator の画面: 閉じたら帯と部品を隠す (仕入れ先マスタ・NE 用 CSV・仕入れ先の追加)・legacy_open はファイルのまま', async () => {
  const pages = { '/suppliers': '.add-form', '/products': '[onclick^="exportNeCsv"]', '/': '[onclick^="saveNewSupplier"]', '/research': '[onclick^="addNewSupplier"]' };
  setPhase('frozen'); G.__resetLegacyGate();
  for (const [p, sel] of Object.entries(pages)) {
    const r = await call('GET', `/apps/profit-calculator${p}`);
    assert.equal(r.status, 200, p);
    assert.ok(r.text.includes('マスタは新しい画面で直します ↗') && r.text.includes(sel), p);
  }
  setPhase('legacy_open'); G.__resetLegacyGate();
  for (const p of Object.keys(pages)) {
    const r = await call('GET', `/apps/profit-calculator${p}`);
    const file = { '/suppliers': 'suppliers.html', '/products': 'products.html', '/': 'index.html', '/research': 'list.html' }[p];
    assert.equal(r.text, fs.readFileSync(path.join(ROOT, 'apps/profit-calculator', file), 'utf8'), `${p} は今までどおりのファイル`);
  }
});

// product-hub の税率
const phdb = (await import('../apps/product-hub/db.js')).getDB();
const draftId = Number(phdb.prepare("INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('ph-tax-1', '税率の試験', 'test')").run().lastInsertRowid);
phdb.prepare("INSERT INTO draft_yahoo (draft_id, tax_rate) VALUES (?, '10%')").run(draftId);
const phTax = () => phdb.prepare('SELECT tax_rate FROM draft_yahoo WHERE draft_id = ?').get(draftId)?.tax_rate;
await t('product-hub: 税率を送る保存は閉じたら 410 / 読めない 503 (draft_yahoo を変えない)・税率を送らない保存は通る・legacy_open は今までどおり', async () => {
  for (const phase of [...CLOSED, 'unreadable']) {
    setPhase(phase);
    const want = phase === 'unreadable' ? 503 : 410;
    let r = await call('POST', `/apps/product-hub/api/drafts/${draftId}`, { tax_rate: '8%' });
    assert.equal(r.status, want, `${phase} basic ${r.text.slice(0, 200)}`);
    r = await call('POST', `/apps/product-hub/api/drafts/${draftId}/yahoo`, { tax_rate: '8%', yahoo_path: 'x' });
    assert.equal(r.status, want, `${phase} yahoo`);
    assert.equal(phTax(), '10%');
    r = await call('POST', `/apps/product-hub/api/drafts/${draftId}`, { memo: `メモ ${phase}` });
    assert.equal(r.status, 200, `${phase} 税率なしの保存 ${r.text.slice(0, 200)}`);
    r = await call('POST', `/apps/product-hub/api/drafts/${draftId}/yahoo`, { yahoo_path: `p-${phase}` });
    assert.equal(r.status, 200, `${phase} 税率なしの Yahoo 保存`);
    assert.equal(phTax(), '10%');
  }
  setPhase('legacy_open');
  assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}`, { tax_rate: '8%' })).status, 200);
  assert.equal(phTax(), '8%');
  assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}/yahoo`, { tax_rate: '10%' })).status, 200);
  assert.equal(phTax(), '10%');
});
await t('product-hub の詳細画面: 閉じたら税率の手入力の欄 (id="y-tax") を出さず Company DB の税率を見せる・legacy_open は今までどおり', async () => {
  const { __setCdbTaxReader } = await import('../apps/product-hub/services/cdb-tax-rate.mjs');
  let asked = null;
  __setCdbTaxReader(async (code) => { asked = code; return { ok: true, label: '8%', tax_class: 'REDUCED_8' }; });
  setPhase('frozen'); G.__resetLegacyGate();
  let r = await call('GET', `/apps/product-hub/detail/${draftId}`);
  assert.equal(r.status, 200, r.text.slice(0, 300));
  assert.ok(!r.text.includes('id="y-tax"'), '手入力の欄が無い');
  assert.ok(/id="y-tax-cdb" value="8%"/.test(r.text), 'Company DB の税率を見せる');
  assert.equal(asked, 'ph-tax-1');
  assert.ok(r.text.includes('マスタは新しい画面で直します ↗'));
  __setCdbTaxReader(async () => ({ ok: false, reason: 'Company DB を読めない (試験)' }));
  G.__resetLegacyGate();
  r = await call('GET', `/apps/product-hub/detail/${draftId}`);
  assert.ok(r.text.includes('Company DB を読めない (試験)'));
  setPhase('legacy_open'); G.__resetLegacyGate();
  asked = null;
  __setCdbTaxReader(async (code) => { asked = code; return { ok: true, label: 'x' }; });
  r = await call('GET', `/apps/product-hub/detail/${draftId}`);
  assert.ok(r.text.includes('id="y-tax"') && !r.text.includes('y-tax-cdb'));
  assert.equal(asked, null, '閉じる前は Company DB を読まない');
  __setCdbTaxReader(null);
});
server.close();

// ═══ D. CLI (子プロセス) ═══
console.log('── D. CLI (子プロセス) ──');
const PRELOAD = pathToFileURL(path.join(ROOT, 'scripts/test-master-legacy-gate-preload.mjs')).href;
const cliDir = path.join(DATA_DIR, 'cli');
fs.mkdirSync(path.join(cliDir, 'import'), { recursive: true });
const baseEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^COMPANY_DB_/.test(k)));
function runCli(args, { phase = null, env = {}, cwd = ROOT } = {}) {
  const r = spawnSync(process.execPath, [...(phase ? ['--import', PRELOAD] : []), ...args], {
    cwd, encoding: 'utf8', timeout: 60000, windowsHide: true,
    env: { ...baseEnv, DATA_DIR: cliDir, ...(phase ? { TEST_LEGACY_PHASE: phase } : {}), ...env },
  });
  return { code: r.status, out: `${r.stdout}\n${r.stderr}` };
}
const whFile = path.join(cliDir, 'warehouse.db');
const Database = (await import('better-sqlite3')).default;
const countIn = (table) => { const d = new Database(whFile, { readonly: true }); try { return d.prepare(`SELECT count(*) AS c FROM ${table}`).get().c; } finally { d.close(); } };
const shipCsv = path.join(cliDir, 'ship.csv');
fs.writeFileSync(shipCsv, 'sku,name,code,method,cost,note\nne-aaa,A,S01,ゆうパケット,300,\nne-bbb,B,S01,ゆうパケット,300,\n');
const CSV_IMPORT = path.join(ROOT, 'apps/warehouse/csv-import.js');

await t('csv-import.js product_shipping: 閉じたら DB を開かずに終了コード 3・legacy_open は今までどおり・閉じた後の全部消して入れ直しは起きない', async () => {
  let r = runCli([CSV_IMPORT, 'product_shipping', shipCsv], { phase: 'frozen' });
  assert.equal(r.code, 3, r.out); assert.ok(r.out.includes('マスタは新しい画面で直します'), r.out);
  assert.ok(!fs.existsSync(whFile), 'DB を作っていない = 触っていない');
  r = runCli([CSV_IMPORT, 'product_shipping', shipCsv], { phase: 'legacy_open' });
  assert.equal(r.code, 0, r.out);
  assert.equal(countIn('product_shipping'), 2);
  fs.writeFileSync(path.join(cliDir, 'empty.csv'), 'sku,name\n');
  for (const p of [...CLOSED, 'unreadable']) {
    r = runCli([CSV_IMPORT, 'product_shipping', path.join(cliDir, 'empty.csv')], { phase: p });
    assert.equal(r.code, 3, `${p}: ${r.out}`);
    assert.equal(countIn('product_shipping'), 2, `${p}: 全部消されていない`);
  }
});
await t('csv-import.js exception_genka: 閉じたら終了コード 3・legacy_open は入る', async () => {
  const g = path.join(cliDir, 'genka.csv');
  fs.writeFileSync(g, 'sku,genka,name\nne-aaa,120,A\n');
  assert.equal(runCli([CSV_IMPORT, 'exception_genka', g], { phase: 'company_owner' }).code, 3);
  assert.equal(countIn('exception_genka'), 0);
  assert.equal(runCli([CSV_IMPORT, 'exception_genka', g], { phase: 'legacy_open' }).code, 0);
  assert.equal(countIn('exception_genka'), 1);
});
await t('csv-import.js の止めない mode (送料の表) は閉じた後も動く (ファイル単位ではなく mode 単位)', async () => {
  const rates = path.join(cliDir, 'rates.csv');
  const cols = Array.from({ length: 18 }, () => '');
  const row = [...cols]; row[1] = '小型'; row[2] = '日本郵便'; row[3] = 'S09'; row[4] = 'ゆうパケット'; row[16] = '310';
  fs.writeFileSync(rates, `${cols.map((_, i) => `c${i}`).join(',')}\n${row.join(',')}\n`);
  const r = runCli([CSV_IMPORT, 'shipping_rates', rates], { phase: 'new_open' });
  assert.equal(r.code, 0, r.out);
  assert.ok(!r.out.includes('マスタは新しい画面で直します'));
});
await t('CLI: 段階を読めない (Company DB につながらない) = 終了コード 3 (fail-closed)', async () => {
  const r = runCli([CSV_IMPORT, 'product_shipping', shipCsv], { env: { COMPANY_DB_WATCH_URL: 'postgres://gate-test@127.0.0.1:1/none' } });
  assert.equal(r.code, 3, r.out); assert.ok(r.out.includes('読めない'), r.out);
  assert.equal(countIn('product_shipping'), 2);
});
await t('import-sales-class.js: 閉じたら終了コード 3 で product_sales_class を変えない・legacy_open は入る', async () => {
  const cwd = path.join(DATA_DIR, 'isc');
  fs.mkdirSync(path.join(cwd, 'data', 'import'), { recursive: true });
  fs.copyFileSync(whFile, path.join(cwd, 'data', 'warehouse.db'));
  fs.writeFileSync(path.join(cwd, 'data', 'import', 'sales_class.csv'), 'sku,name,a,b,class\nne-aaa,A,,,2\n');
  const S = path.join(ROOT, 'apps/warehouse/import-sales-class.js');
  const cnt = () => { const d = new Database(path.join(cwd, 'data', 'warehouse.db'), { readonly: true }); try { return d.prepare('SELECT count(*) AS c FROM product_sales_class').get().c; } finally { d.close(); } };
  for (const p of [...CLOSED, 'unreadable']) {
    const r = runCli([S], { phase: p, cwd });
    assert.equal(r.code, 3, `${p}: ${r.out}`);
    assert.equal(cnt(), 0);
  }
  const r = runCli([S], { phase: 'legacy_open', cwd });
  assert.equal(r.code, 0, r.out); assert.equal(cnt(), 1);
});
await t('import-sku-master.js: 閉じたら --dry-run も終了コード 3・legacy_open は入る', async () => {
  const csv = path.join(cliDir, 'skumaster.csv');
  fs.writeFileSync(csv, 'sku,asin,商品名,NE商品コード,数量\nsku-cli,B000,CLIの品,ne-aaa,1\n');
  const S = path.join(ROOT, 'apps/warehouse/import-sku-master.js');
  for (const args of [[csv, '--encoding=utf-8'], [csv, '--encoding=utf-8', '--dry-run']]) {
    const r = runCli([S, ...args], { phase: 'frozen' });
    assert.equal(r.code, 3, r.out);
  }
  assert.equal(countIn('m_sku_master'), 0);
  const r = runCli([S, csv, '--encoding=utf-8'], { phase: 'legacy_open' });
  assert.equal(r.code, 0, r.out); assert.equal(countIn('m_sku_master'), 1);
});
await t('migrate-reorder-setting-initial.js: 閉じたら終了コード 3・legacy_open は入る', async () => {
  const csv = path.join(cliDir, 'pml.csv');
  fs.writeFileSync(csv, '商品コード,推奨保有在庫\nne-aaa,2.5\n');
  const S = path.join(ROOT, 'apps/warehouse/migrate-reorder-setting-initial.js');
  const r1 = runCli([S, `--csv=${csv}`], { phase: 'new_open' });
  assert.equal(r1.code, 3, r1.out);
  assert.equal(countIn('m_reorder_setting'), 0);
  const r2 = runCli([S, `--csv=${csv}`], { phase: 'legacy_open' });
  assert.equal(r2.code, 0, r2.out); assert.equal(countIn('m_reorder_setting'), 1);
});

G.__setLegacyPhaseReader(null);
await pg.close();
console.log(process.exitCode ? `\n❌ 失敗あり (${passed} 件 OK)` : `\n✅ ${passed} 件 OK`);
