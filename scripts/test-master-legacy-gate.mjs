/**
 * test-master-legacy-gate.mjs — マスタの古い入口の門 (lib/master-legacy-gate.mjs・config/master-legacy-entries.mjs) の試験
 * (Company DB構想 14 §5・§9 v2 M2・§10 契約 v3 H1・PR #1565 Codex R1)
 *
 *   A. 門の部品: 段階ごとの可否・読めない = すぐ閉じる (前に読めた値を使わない)・書き込みは毎回読む・画面だけ 30 秒・接続先が無い / つながらない / 返事が無い
 *      (再試行 1 回)・答えの形・道の形と when・帯・drain (書きかけを数える)・route_part・manifest・build の番号・門の記録 (書く前の確かめ・書く中身・5 分おき)
 *   B. Company DB (PGlite・0050 まで流す): 段階を進めると門が閉じる / 見張りのロール watcher でも段階を読める / 0050 の前の DB = 閉じる
 *   C. 入口ごと (本物の router を HTTP で): legacy_open = 今までどおり / frozen・company_owner・new_open = 410 で何も書かない / 読めない = 503 で何も書かない /
 *      画面は帯。miniPC の /register (全部の API)・CSV を受け取っている間に閉じた・会計アプリ 5 つ・fba-profitability・profit-calculator・
 *      product-hub (税率・Notion の取込・古い新商品の作り方・自動取込・代表コードの税率・出品を止める)・発注アプリの仕入先・売れ筋共有の表示名
 *   D. CLI (子プロセス): 閉じた mode は引数・ファイルの検査より前に終了コード 3 / 書く直前に閉じたら書かない / 止めない 5 つの mode は動く / 読めない = 3
 *
 * 使い方: node scripts/test-master-legacy-gate.mjs (一時の DATA_DIR・PGlite・127.0.0.1 の偽のサーバーだけ。本番の DB・API にはつながない)
 */
import { temporaryTestDataDir } from './test-temp-dir.mjs';
const DATA_DIR = await temporaryTestDataDir(import.meta.url, 'mlg-');

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import net from 'node:net';
import path from 'node:path';
import http from 'node:http';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import express from 'express';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
// 本番の接続先は子プロセスにも渡さない (この試験は PGlite・偽の読み方・127.0.0.1 だけ)
for (const k of ['COMPANY_DB_URL', 'COMPANY_DB_WATCH_URL', 'COMPANY_DB_WATCH_WRITER_URL', 'RENDER_GIT_COMMIT']) delete process.env[k];
process.env.WAREHOUSE_API_KEY = '';
delete process.env.RENDER;

const G = await import('../lib/master-legacy-gate.mjs');
const E = await import('../config/master-legacy-entries.mjs');
const { readCutoverPhase, CUTOVER_PHASES, ownershipHash } = await import('../lib/master-cutover.mjs');

let passed = 0;
async function t(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const CLOSED = ['frozen', 'company_owner', 'new_open'];
const phaseReader = (phase) => async () => (phase === 'unreadable' ? { readable: false, phase: null, error: '試験: 読めない' } : { readable: true, phase });
const setPhase = (phase) => G.__setLegacyPhaseReader(phaseReader(phase));
const quiet = async (fn) => { const w = console.warn, l = console.log, e = console.error; console.warn = console.log = console.error = () => {}; try { return await fn(); } finally { console.warn = w; console.log = l; console.error = e; } };

// ═══ A. 門の部品 ═══
console.log('── A. 門の部品 ──');
await t('legacy_open だけ書ける・frozen / company_owner / new_open は書けない (持ち主表とは別の門)', async () => {
  for (const p of CUTOVER_PHASES) {
    setPhase(p);
    const s = await G.checkLegacyGate();
    assert.equal(s.writable, p === 'legacy_open', p); assert.equal(s.readable, true); assert.equal(s.source, 'db');
  }
});
await t('読めない・知らない段階・例外 = 書けない', async () => {
  await quiet(async () => {
    setPhase('unreadable');
    let s = await G.checkLegacyGate();
    assert.deepEqual([s.writable, s.readable, s.source], [false, false, 'unreadable']);
    G.__setLegacyPhaseReader(async () => ({ readable: true, phase: 'opened_by_typo' }));
    assert.equal((await G.checkLegacyGate()).writable, false);
    G.__setLegacyPhaseReader(async () => { throw new Error('つながらない'); });
    assert.equal((await G.checkLegacyGate()).writable, false);
  });
});
await t('🚨 R1 H1: 直前に legacy_open を読めても、次に読めなければすぐ閉じる (前に読めた値で通さない)', async () => {
  let mode = 'legacy_open';
  G.__setLegacyPhaseReader(async () => (mode === 'down' ? { readable: false, phase: null, error: 'down' } : { readable: true, phase: mode }));
  assert.equal((await G.checkLegacyGate()).writable, true);
  mode = 'down';
  const s = await quiet(() => G.checkLegacyGate());
  assert.deepEqual([s.writable, s.readable, s.source], [false, false, 'unreadable']);
});
await t('書き込みは毎回読む (同時でも別々に読む)・画面だけ 30 秒前の結果 (読めない結果も) を使う', async () => {
  let calls = 0, now = Date.parse('2026-10-01T01:00:00Z');
  G.__setLegacyClock(() => now);
  G.__setLegacyPhaseReader(async () => { calls++; return { readable: true, phase: 'legacy_open' }; });
  await Promise.all([G.checkLegacyGate(), G.checkLegacyGate(), G.checkLegacyGate()]);
  assert.equal(calls, 3, '書き込みは毎回');
  await G.checkLegacyGate({ purpose: 'screen' });
  assert.equal(calls, 3, '画面は直前の結果 (30 秒以内)');
  now += 31 * 1000;
  await G.checkLegacyGate({ purpose: 'screen' });
  assert.equal(calls, 4, '30 秒を過ぎたら読む');
  G.__setLegacyClock(null);
});
await t('M6: 本物の読み方 = 接続先が無い → すぐ閉じる / つながらない → 1 回読み直して閉じる (回数・遅さを数える)', async () => {
  G.__setLegacyPhaseReader(null);
  const before = G.legacyGateStats();
  let s = await quiet(() => G.checkLegacyGate());
  assert.equal(s.writable, false); assert.match(s.error, /接続先が無い/);
  process.env.COMPANY_DB_WATCH_URL = 'postgres://gate-test@127.0.0.1:1/none';
  try {
    s = await quiet(() => G.checkLegacyGate());
    assert.equal(s.writable, false); assert.equal(s.readable, false);
    const after = G.legacyGateStats();
    assert.ok(after.retries >= before.retries + 1, '読み直した');
    assert.ok(after.reads_failed >= before.reads_failed + 2);
    assert.ok(after.last_latency_ms != null);
  } finally { delete process.env.COMPANY_DB_WATCH_URL; await G.closeLegacyGatePool(); }
});
await t('M6: 返事の無い Company DB (接続の打ち切り 3 秒) = 待ちすぎずに閉じる (再試行込みで 10 秒以内)', async () => {
  const sockets = [];
  const silent = net.createServer((sock) => { sockets.push(sock); /* 何も返さない */ });
  await new Promise((r) => silent.listen(0, '127.0.0.1', r));
  process.env.COMPANY_DB_WATCH_URL = `postgres://gate-test@127.0.0.1:${silent.address().port}/none`;
  G.__setLegacyPhaseReader(null);
  const t0 = Date.now();
  try {
    const s = await quiet(() => G.checkLegacyGate());
    const ms = Date.now() - t0;
    assert.equal(s.writable, false);
    assert.ok(ms >= 2500 && ms < 10000, `${ms}ms`);
  } finally {
    delete process.env.COMPANY_DB_WATCH_URL;
    await G.closeLegacyGatePool();
    for (const s of sockets) s.destroy();
    await new Promise((r) => silent.close(r));
  }
});
await t('断る答え = 閉じた 410 {error:master_frozen, message, url} / 読めない 503 {error:master_phase_unreadable}', async () => {
  const [s1, b1] = G.refusal({ writable: false, readable: true, phase: 'frozen' }, { id: 'x' });
  assert.equal(s1, 410); assert.equal(b1.error, 'master_frozen'); assert.equal(b1.url, E.MASTER_EDIT_URL); assert.ok(b1.message.includes('新しい画面'));
  const [s2, b2] = G.refusal({ writable: false, readable: false, phase: null }, { id: 'x' });
  assert.equal(s2, 503); assert.equal(b2.error, 'master_phase_unreadable');
});
await t('道の形 (Express 4 と同じ: 大文字小文字を区別しない・末尾の / は任意・:param は 1 段で名前つき)', async () => {
  const re = G.pathPattern('/api/shipping/:sku');
  assert.ok(re.test('/api/shipping/abc')); assert.ok(re.test('/API/Shipping/abc/')); assert.ok(!re.test('/api/shipping/abc/x')); assert.ok(!re.test('/api/shipping'));
  assert.equal(G.pathPattern('/api/masters/:kind/:id').exec('/api/masters/suppliers/0001').groups.kind, 'suppliers');
  assert.ok(G.pathPattern('/').test('/')); assert.ok(!G.pathPattern('/').test('/x'));
});
await t('帯: 閉じていなければ空 (今までどおり)・閉じたら文言・新しい画面への道・隠す部品', async () => {
  assert.equal(G.legacyBannerHtml(G.screenInfo({ writable: true, readable: true, phase: 'legacy_open' })), '');
  const h = G.legacyBannerHtml(G.screenInfo({ writable: false, readable: true, phase: 'frozen' }), { hideSelectors: ['#a', '.b'] });
  assert.ok(h.includes('マスタは新しい画面で直します ↗') && h.includes(E.MASTER_EDIT_URL) && h.includes('#a,.b{display:none !important}'));
  assert.ok(G.legacyBannerHtml(G.screenInfo({ writable: false, readable: false, phase: null })).includes('読めない'));
});
await t('一覧: app ごとの入口・知らない app の門は起動時に落ちる・CLI の入口を file と mode で引ける・閉じない口の種類', async () => {
  for (const app of ['warehouse', 'aupay-accounting', 'yahoo-accounting', 'mercari-accounting', 'linegift-accounting', 'qoo10-accounting', 'fba-profitability', 'product-hub', 'profit-calculator', 'purchase-orders', 'supplier-sales']) {
    assert.ok(E.entriesForApp(app).length > 0, app);
  }
  assert.throws(() => G.masterLegacyGate('no-such-app'));
  assert.equal(E.cliEntry('apps/warehouse/csv-import.js', 'product_shipping').id, 'cli:csv-import.js product_shipping');
  assert.equal(E.cliEntry('apps/warehouse/csv-import.js', 'orders'), null);
  assert.ok(E.LEGACY_EXEMPT.every((e) => ['replication', 'already_closed', 'manual'].includes(e.kind)));
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
  assert.ok(logs.some((m) => m.includes('マスタは新しい画面で直します') && m.includes(E.MASTER_EDIT_URL)) && logs.some((m) => m.includes('読めない')));
});
await t('drain (R1 H3): 門を通った書き込みを終わるまで数える (件数・いちばん古い開始)。閉じた要求は数えない', async () => {
  setPhase('legacy_open');
  let release;
  const hold = new Promise((r) => { release = r; });
  const app = express();
  const r = express.Router();
  r.use(G.masterLegacyGate('warehouse'));
  r.post('/api/shipping', async (req, res) => { await hold; res.json({ ok: true }); });
  app.use('/', r);
  const srv = http.createServer(app);
  await new Promise((ok) => srv.listen(0, '127.0.0.1', ok));
  const url = `http://127.0.0.1:${srv.address().port}/api/shipping`;
  try {
    assert.equal(G.legacyInflight().count, 0);
    const p = fetch(url, { method: 'POST' });
    for (let i = 0; i < 50 && G.legacyInflight().count === 0; i++) await new Promise((ok) => setTimeout(ok, 10));
    const mid = G.legacyInflight();
    assert.equal(mid.count, 1); assert.ok(mid.oldest_started_at); assert.equal(mid.by_entry['warehouse:POST /api/shipping'], 1);
    release();
    assert.equal((await p).status, 200);
    for (let i = 0; i < 50 && G.legacyInflight().count > 0; i++) await new Promise((ok) => setTimeout(ok, 10));
    assert.equal(G.legacyInflight().count, 0);
    setPhase('frozen');
    assert.equal((await quiet(() => fetch(url, { method: 'POST' }))).status, 410);
    assert.equal(G.legacyInflight().count, 0);
  } finally { await new Promise((ok) => srv.close(ok)); }
});
await t('route_part: 断らずに res.locals.masterLegacyWrite (毎回読んだ状態) を渡す = handler がマスタの部分だけ書かない', async () => {
  const app = express();
  const r = express.Router();
  r.use(express.json());
  r.use(G.masterLegacyGate('product-hub'));
  r.post('/api/drafts/:id/set-drafts', (req, res) => res.json({ w: res.locals.masterLegacyWrite?.writable ?? null }));
  app.use('/', r);
  const srv = http.createServer(app);
  await new Promise((ok) => srv.listen(0, '127.0.0.1', ok));
  try {
    const post = async () => (await (await fetch(`http://127.0.0.1:${srv.address().port}/api/drafts/1/set-drafts`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' })).json()).w;
    setPhase('legacy_open'); assert.equal(await post(), true);
    setPhase('frozen'); assert.equal(await post(), false);
    setPhase('unreadable'); assert.equal(await quiet(post), false);
  } finally { await new Promise((ok) => srv.close(ok)); }
});
await t('manifest: 形を決めた JSON の sha256 (同じ一覧 = 同じ値・一覧が変われば変わる)・手の入口の id を含む', async () => {
  const m = G.legacyManifest();
  assert.equal(G.manifestHash(), G.manifestHash(G.legacyManifest()));
  assert.match(G.manifestHash(), /^[0-9a-f]{64}$/);
  assert.deepEqual(m.manual_entry_ids, E.LEGACY_EXEMPT.filter((e) => e.kind === 'manual').map((e) => e.id));
  assert.ok(m.manual_entry_ids.includes('ne:商品画面'));
  assert.equal(m.entries.length, E.LEGACY_ENTRIES.length);
  assert.notEqual(G.manifestHash({ ...m, entries: m.entries.slice(1) }), G.manifestHash(m));
  // キーの並びが違っても同じ値 (どの PC でも同じ)
  const reordered = { manual_entry_ids: m.manual_entry_ids, exempt: m.exempt, entries: m.entries, schema: m.schema };
  assert.equal(G.manifestHash(reordered), G.manifestHash(m));
});
await t('build の番号: Render = RENDER_GIT_COMMIT / miniPC = リポジトリの git HEAD / 分からない = null (記録を書かない)', async () => {
  assert.equal(G.resolveBuildId({ env: { RENDER_GIT_COMMIT: 'ABCDEF1234567' }, fresh: true }), 'abcdef1234567');
  const head = G.resolveBuildId({ env: {}, fresh: true });
  assert.match(head, /^[0-9a-f]{40}$/);
  const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'mlg-nogit-'));
  assert.equal(G.resolveBuildId({ env: {}, repoDir: empty, fresh: true }), null);
});
await t('門の記録 (R1 H2・M6): 書く前に確かめる (場所・build の番号・段階を読める・書く接続先・書き手の関数)・書く中身 (manifest・書きかけ・持ち主表)', async () => {
  const noGit = fs.mkdtempSync(path.join(os.tmpdir(), 'mlg-nogit-'));
  const calls = [];
  const fakeDb = (hasFn) => async () => ({ db: { query: async (sql, params) => { calls.push({ sql, params }); if (/to_regprocedure/.test(sql)) return { rows: [{ ok: hasFn }] }; return { rows: [{ r: 'ack-1' }] }; } }, close: async () => {} });
  await quiet(async () => {
    setPhase('legacy_open');
    assert.equal((await G.ackLegacyGates({ host: 'laptop', connect: fakeDb(true) })).state, 'precheck_failed');
    let r = await G.ackLegacyGates({ host: 'minipc', connect: fakeDb(true), repoDir: noGit, env: {} });
    assert.equal(r.state, 'precheck_failed'); assert.match(r.detail, /build の番号/);
    setPhase('unreadable');
    r = await G.ackLegacyGates({ host: 'minipc', connect: fakeDb(true) });
    assert.equal(r.state, 'precheck_failed'); assert.match(r.detail, /段階を読めない/);
    setPhase('legacy_open');
    r = await G.ackLegacyGates({ host: 'minipc', env: {} });
    assert.equal(r.state, 'precheck_failed'); assert.match(r.detail, /接続先が無い/);
    r = await G.ackLegacyGates({ host: 'minipc', connect: fakeDb(false) });
    assert.equal(r.state, 'no_function');
  });
  calls.length = 0;
  const end = G.beginLegacyWrite('warehouse:POST /api/shipping');
  try {
    const r = await G.ackLegacyGates({ host: 'render', connect: fakeDb(true), env: { RENDER_GIT_COMMIT: 'a'.repeat(40), RENDER_INSTANCE_ID: 'srv-1' } });
    assert.equal(r.state, 'acked'); assert.equal(r.ack_id, 'ack-1');
  } finally { end(); }
  const call = calls.find((c) => c.sql === G.ACK_WRITER.call);
  const p = JSON.parse(call.params[0]);
  const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
  assert.equal(p.host, 'render'); assert.equal(p.build_id, 'a'.repeat(40)); assert.match(p.instance_id, /^srv-1:\d+:[0-9a-f]{8}$/);
  assert.equal(p.manifest_hash, G.manifestHash()); assert.deepEqual(p.manifest.manual_entry_ids, G.legacyManifest().manual_entry_ids);
  assert.equal(p.owner_hash, ownershipHash(MASTER_OWNERSHIP)); assert.equal(p.phase_seen, 'legacy_open');
  assert.equal(p.inflight_count, 1); assert.ok(p.oldest_inflight_at);
  assert.equal(G.legacyAckState().state, 'acked');
});
await t('門の記録は 5 分おき (要求が来たついで)・同時に 2 本走らせない・読み戻しは今すぐ書き直す', async () => {
  let now = Date.parse('2026-10-01T02:00:00Z');
  G.__setLegacyClock(() => now);
  G.__resetLegacyAck();
  setPhase('unreadable');   // 確かめで止まる = 速い
  await quiet(async () => {
    const p1 = G.maybeRefreshLegacyAck({ host: 'minipc' });
    assert.ok(p1);
    assert.equal(G.maybeRefreshLegacyAck({ host: 'minipc' }), p1, '走っている間は同じもの');
    await p1;
    assert.equal(G.maybeRefreshLegacyAck({ host: 'minipc' }), null, '5 分以内は書き直さない');
    now += 5 * 60 * 1000 + 1;
    const p2 = G.maybeRefreshLegacyAck({ host: 'minipc' });
    assert.ok(p2); await p2;
    const p3 = G.maybeRefreshLegacyAck({ host: 'minipc', force: true });
    assert.ok(p3); await p3;
    // app.use の heartbeat
    now += 5 * 60 * 1000 + 1;
    let nexted = false;
    G.legacyAckHeartbeat('minipc')({}, {}, () => { nexted = true; });
    assert.ok(nexted);
    await new Promise((ok) => setTimeout(ok, 50));
    assert.equal(G.legacyAckState().state, 'precheck_failed');
  });
  G.__setLegacyClock(null);
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
/** 試験だけ: 段階を直接変える (段階を進める関数の門 = 記録と証拠は ⑤-1 の試験が見る。ここは古い入口の門の側だけを見る) */
async function forcePhase(phase) {
  await pg.query(`select set_config('ops.cutover_protocol', '1', false)`);
  await pg.query(`update ops.master_cutover_state set phase = $1, owner_hash = $2 where id = 1`, [phase, ['company_owner', 'new_open'].includes(phase) ? 'a'.repeat(64) : null]);
  await pg.query(`select set_config('ops.cutover_protocol', '', false)`);
}
await t('Company DB の段階が frozen 以降になると門が閉じる (持ち主表はまだ全部 load のまま)', async () => {
  const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
  assert.ok(Object.values(MASTER_OWNERSHIP).every((v) => v === 'load'));
  G.__setLegacyPhaseReader(() => readCutoverPhase(cdb));
  assert.equal((await G.checkLegacyGate()).writable, true, 'legacy_open');
  for (const to of CLOSED) {
    await forcePhase(to);
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
  const s = await quiet(() => G.checkLegacyGate());
  assert.equal(s.writable, false); assert.equal(s.readable, false);
  await pg2.close();
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
const insMirror = mdb.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価状態, 消費税率, 売上分類, 原価, 代表商品コード, updated_at) VALUES (?, ?, '単品', 'OK', 0.1, 3, 500, ?, '2026-10-01 00:00:00')`);
insMirror.run('mp-1', 'M1', null);
insMirror.run('ph-rep-a', '代表の子 A', 'ph-rep');
insMirror.run('ph-rep-b', '代表の子 B', 'ph-rep');

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
app.use('/apps/purchase-orders', (await import('../apps/purchase-orders/router.js')).default);
app.use('/apps/supplier-sales', (await import('../apps/supplier-sales/router.js')).default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;

async function call(method, p, body, { csv = null, files = null } = {}) {
  const opts = { method, headers: {} };
  if (csv != null || files) {
    const fd = new FormData();
    if (csv != null) fd.append('file', new Blob([csv], { type: 'text/csv' }), 'x.csv');
    for (const [name, content] of files || []) fd.append('files', new Blob([content], { type: 'text/csv' }), name);
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
const isGateRefusal = (r) => r.json && ['master_frozen', 'master_phase_unreadable'].includes(r.json.error);

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
const uploadDir = path.join(ROOT, 'data', 'import');
const uploadsNow = () => (fs.existsSync(uploadDir) ? fs.readdirSync(uploadDir).length : 0);

await t('miniPC /register: 一覧の全部の API に試験の見本がある', async () => {
  assert.deepEqual(whRoutes.filter((e) => !WH_SAMPLES[e.id]).map((e) => e.id), []);
  assert.equal(whRoutes.length, 19);
});
for (const phase of [...CLOSED, 'unreadable']) {
  await t(`miniPC /register: ${phase} = 全部の API が ${phase === 'unreadable' ? 503 : 410} で、上書き表・m_products・SKU マスタに何も書かない`, async () => {
    setPhase(phase);
    await quiet(async () => {
      for (const e of whRoutes) {
        const before = whSnap();
        const r = await whReq(e.id);
        assert.equal(r.status, phase === 'unreadable' ? 503 : 410, `${e.id}: ${r.status} ${r.text.slice(0, 200)}`);
        assert.equal(r.json.error, phase === 'unreadable' ? 'master_phase_unreadable' : 'master_frozen', e.id);
        assert.equal(r.json.url, E.MASTER_EDIT_URL, e.id);
        assert.equal(whSnap(), before, `${e.id} が書いた`);
      }
    });
  });
}
await t('🚨 R1 H3: CSV を受け取っている間に frozen になった = 書く直前にもう一度読んで 410・何も書かない・受け取ったファイルも消す', async () => {
  for (const id of whRoutes.filter((e) => e.recheck).map((e) => e.id)) {
    let n = 0;
    G.__setLegacyPhaseReader(async () => ({ readable: true, phase: n++ === 0 ? 'legacy_open' : 'frozen' }));   // 1 回目 (門) は legacy_open・2 回目 (書く直前) は frozen
    const before = whSnap();
    const files0 = uploadsNow();
    const r = await quiet(() => whReq(id));
    assert.equal(r.status, 410, `${id}: ${r.status} ${r.text.slice(0, 200)}`);
    assert.equal(n, 2, `${id}: 2 回読んだ`);
    assert.equal(whSnap(), before, `${id} が書いた`);
    assert.equal(uploadsNow(), files0, `${id}: 受け取ったファイルが残った`);
  }
});
await t('miniPC /register: legacy_open = 今までどおり書ける (登録 → CSV → 削除 → SKU マスタ)', async () => {
  setPhase('legacy_open');
  for (const id of ['warehouse:POST /api/shipping', 'warehouse:POST /api/genka', 'warehouse:POST /api/sales_class', 'warehouse:POST /api/tax_rate', 'warehouse:POST /api/reorder_setting',
    'warehouse:POST /api/csv/genka', 'warehouse:POST /api/csv/shipping', 'warehouse:POST /api/csv/sales_class', 'warehouse:POST /api/csv/tax_rate', 'warehouse:POST /api/csv/reorder_setting',
    'warehouse:POST /api/m-sku-master', 'warehouse:PUT /api/m-sku-master/:sku', 'warehouse:POST /api/csv/m-sku-master']) {
    const r = await whReq(id);
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
  assert.equal(G.legacyInflight().count, 0, '全部終わった = 書きかけ 0');
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
  assert.ok(r.text.includes('マスタは新しい画面で直します ↗') && r.text.includes('#csv-card') && r.text.includes('[data-act^="reg-"]') && r.text.includes('id="csv-card"'));
  r = await call('GET', '/apps/warehouse/');
  assert.ok(r.text.includes('マスタは新しい画面で直します ↗') && r.text.includes('[data-action="delete"]'));
  setPhase('legacy_open'); G.__resetLegacyGate();
  assert.ok(!(await call('GET', '/apps/warehouse/register')).text.includes('master-legacy-banner'));
  assert.ok(!(await call('GET', '/apps/warehouse/')).text.includes('master-legacy-banner'));
});
await t('読み戻し GET /apps/warehouse/api/master-legacy-gate = 段階・書けるか・manifest・持ち主表・build・書きかけ・数・門の記録', async () => {
  setPhase('frozen');
  const r = await quiet(() => call('GET', '/apps/warehouse/api/master-legacy-gate'));
  assert.equal(r.status, 200);
  const s = r.json;
  assert.deepEqual([s.phase, s.writable, s.host], ['frozen', false, 'minipc']);
  assert.equal(s.manifest_hash, G.manifestHash()); assert.match(s.owner_hash, /^[0-9a-f]{64}$/); assert.match(s.build_id, /^[0-9a-f]{40}$/);
  assert.equal(s.inflight.count, 0); assert.ok('oldest_started_at' in s.inflight);
  assert.ok(s.stats.reads_ok > 0 && 'refused_410' in s.stats && 'last_latency_ms' in s.stats);
  assert.ok(s.ack && s.ack.state);
});

// 会計アプリ 5 つ
const mirrorRow = () => JSON.stringify(mdb.prepare("SELECT 消費税率, 売上分類, 原価, 原価ソース, 原価状態 FROM mirror_products WHERE 商品コード = 'mp-1'").get());
for (const a of ACC) {
  await t(`${a}-accounting: POST /register は閉じたら 410 / 読めない 503 で mirror_products を変えない・legacy_open は今までどおり・画面は帯`, async () => {
    await quiet(async () => {
      for (const phase of [...CLOSED, 'unreadable']) {
        setPhase(phase);
        const before = mirrorRow();
        const r = await call('POST', `/apps/${a}-accounting/register`, { items: [{ code: 'mp-1', taxRate: 8, segment: 1 }] });
        assert.equal(r.status, phase === 'unreadable' ? 503 : 410, `${phase} ${r.status}`);
        assert.equal(mirrorRow(), before);
      }
    });
    setPhase('frozen'); G.__resetLegacyGate();
    assert.ok((await call('GET', `/apps/${a}-accounting/`)).text.includes('#registerBtn,.reg-sel{display:none !important}'));
    setPhase('legacy_open'); G.__resetLegacyGate();
    assert.ok(!(await call('GET', `/apps/${a}-accounting/`)).text.includes('master-legacy-banner'));
    const r = await call('POST', `/apps/${a}-accounting/register`, { items: [{ code: 'mp-1', taxRate: 8, segment: 1 }] });
    assert.equal(r.status, 200); assert.equal(r.json.updatedTax, 1);
    mdb.prepare("UPDATE mirror_products SET 消費税率 = 0.1, 売上分類 = 3 WHERE 商品コード = 'mp-1'").run();
  });
}

// fba-profitability
await t('fba-profitability: 原価の手入力は閉じたら 410 / 読めない 503 (mirror_products を変えない)・legacy_open は今までどおり・画面は帯', async () => {
  await quiet(async () => {
    for (const phase of [...CLOSED, 'unreadable']) {
      setPhase(phase);
      const before = mirrorRow();
      assert.equal((await call('POST', '/apps/fba-profitability/api/update-cost', { sku: 'mp-1', cost: 999 })).status, phase === 'unreadable' ? 503 : 410);
      assert.equal(mirrorRow(), before);
    }
  });
  setPhase('frozen'); G.__resetLegacyGate();
  assert.ok((await call('GET', '/apps/fba-profitability/')).text.includes('button[onclick^="openCostModal"]'));
  setPhase('legacy_open'); G.__resetLegacyGate();
  assert.ok(!(await call('GET', '/apps/fba-profitability/')).text.includes('master-legacy-banner'));
  assert.equal((await call('POST', '/apps/fba-profitability/api/update-cost', { sku: 'mp-1', cost: 999 })).status, 200);
  mdb.prepare("UPDATE mirror_products SET 原価 = 500, 原価ソース = NULL, 原価状態 = 'OK' WHERE 商品コード = 'mp-1'").run();
});

// profit-calculator
const suppliersFile = path.join(DATA_DIR, 'suppliers.json');
const supSnap = () => (fs.existsSync(suppliersFile) ? fs.readFileSync(suppliersFile, 'utf8') : null);
await t('profit-calculator: NE 用 CSV・仕入れ先の追加・削除は閉じたら 410 / 読めない 503 (suppliers.json を変えない)・legacy_open は今までどおり・画面は帯', async () => {
  await quiet(async () => {
    for (const phase of [...CLOSED, 'unreadable']) {
      setPhase(phase);
      const before = supSnap();
      const want = phase === 'unreadable' ? 503 : 410;
      assert.equal((await call('POST', '/apps/profit-calculator/api/suppliers', { code: '7777', name: '試験の仕入先' })).status, want);
      assert.equal((await call('DELETE', '/apps/profit-calculator/api/suppliers', { code: '0001' })).status, want);
      assert.equal((await call('GET', '/apps/profit-calculator/api/products/csv/ne?type=single')).status, want);
      assert.equal(supSnap(), before);
    }
  });
  setPhase('frozen'); G.__resetLegacyGate();
  for (const [p, sel] of Object.entries({ '/suppliers': '.add-form', '/products': '[onclick^="exportNeCsv"]', '/': '[onclick^="saveNewSupplier"]', '/research': '[onclick^="addNewSupplier"]' })) {
    assert.ok((await call('GET', `/apps/profit-calculator${p}`)).text.includes(sel), p);
  }
  setPhase('legacy_open'); G.__resetLegacyGate();
  assert.equal((await call('GET', '/apps/profit-calculator/suppliers')).text, fs.readFileSync(path.join(ROOT, 'apps/profit-calculator/suppliers.html'), 'utf8'));
  assert.equal((await call('POST', '/apps/profit-calculator/api/suppliers', { code: '7777', name: '試験の仕入先' })).status, 200);
  assert.ok(JSON.parse(supSnap()).some((s) => s.code === '7777'));
  assert.equal((await call('DELETE', '/apps/profit-calculator/api/suppliers', { code: '7777' })).status, 200);
});

// product-hub
const phdb = (await import('../apps/product-hub/db.js')).getDB();
const draftId = Number(phdb.prepare("INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('mp-1', '税率の試験', 'test')").run().lastInsertRowid);
phdb.prepare("INSERT INTO draft_yahoo (draft_id, tax_rate) VALUES (?, '10%')").run(draftId);
const repDraftId = Number(phdb.prepare("INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('ph-rep', '代表コードの試験', 'test')").run().lastInsertRowid);
phdb.prepare("INSERT INTO draft_yahoo (draft_id, tax_rate) VALUES (?, '10%')").run(repDraftId);
const phTax = (id = draftId) => phdb.prepare('SELECT tax_rate FROM draft_yahoo WHERE draft_id = ?').get(id)?.tax_rate;
const nDrafts = () => phdb.prepare('SELECT count(*) AS c FROM product_drafts').get().c;
const { __setCdbTaxReader, resolveCdbDraftTax } = await import('../apps/product-hub/services/cdb-tax-rate.mjs');
const cdbRates = (rates) => async (codes) => ({ ok: true, rows: codes.map((c) => (c in rates ? { code: c, found: rates[c] !== 'missing', tax_rate: rates[c] === 'missing' || rates[c] == null ? null : rates[c], tax_class: rates[c] == null ? 'UNKNOWN' : null } : { code: c, found: false, tax_rate: null })) });

await t('product-hub: 税率を送る保存は閉じたら 410 / 読めない 503 (draft_yahoo を変えない)・税率を送らない保存は通る・legacy_open は今までどおり', async () => {
  await quiet(async () => {
    for (const phase of [...CLOSED, 'unreadable']) {
      setPhase(phase);
      const want = phase === 'unreadable' ? 503 : 410;
      assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}`, { tax_rate: '8%' })).status, want);
      assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}/yahoo`, { tax_rate: '8%', yahoo_path: 'x' })).status, want);
      assert.equal(phTax(), '10%');
      assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}`, { memo: `メモ ${phase}` })).status, 200);
      assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}/yahoo`, { yahoo_path: `p-${phase}` })).status, 200);
    }
  });
  setPhase('legacy_open');
  assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}`, { tax_rate: '8%' })).status, 200);
  assert.equal(phTax(), '8%');
  assert.equal((await call('POST', `/apps/product-hub/api/drafts/${draftId}/yahoo`, { tax_rate: '10%' })).status, 200);
  assert.equal(phTax(), '10%');
});
await t('product-hub (R1 H4): Notion の取込 2 つは閉じたら 410 (税率を書かない)・legacy_open は門を通る', async () => {
  for (const p of ['/api/notion-import', '/api/notion-import-by-status']) {
    setPhase('frozen');
    const r = await quiet(() => call('POST', `/apps/product-hub${p}`, { codes: 'mp-1', status: 'x' }));
    assert.equal(r.status, 410, p); assert.equal(phTax(), '10%');
    setPhase('legacy_open');
    const r2 = await quiet(() => call('POST', `/apps/product-hub${p}`, {}));   // 中身が空 = 取込の前に 400 など (Notion にはつながない)
    assert.ok(!isGateRefusal(r2), `${p}: ${r2.status} ${r2.text.slice(0, 120)}`);
  }
});
await t('product-hub (⑤-2a M5): 古い新商品の作り方 (POST /api/drafts・NE のコードから登録・自動取込を手で回す) は閉じたら 410・下書きを作らない / 古い /new の画面は帯', async () => {
  for (const phase of [...CLOSED, 'unreadable']) {
    setPhase(phase);
    const n0 = nDrafts();
    await quiet(async () => {
      assert.equal((await call('POST', '/apps/product-hub/api/drafts', { ne_code: 'new-after-frozen', name: '閉じた後の新商品' })).status, phase === 'unreadable' ? 503 : 410);
      assert.equal((await call('POST', '/apps/product-hub/api/register-codes', { codes: 'new-after-frozen' })).status, phase === 'unreadable' ? 503 : 410);
      assert.equal((await call('POST', '/apps/product-hub/api/intake/run', {})).status, phase === 'unreadable' ? 503 : 410);
    });
    assert.equal(nDrafts(), n0);
  }
  G.__resetLegacyGate(); setPhase('frozen');
  const page = await call('GET', '/apps/product-hub/new');
  assert.ok(page.text.includes('マスタは新しい画面で直します ↗') && page.text.includes('#create-btn{display:none !important}'));
  setPhase('legacy_open'); G.__resetLegacyGate();
  assert.ok(!(await call('GET', '/apps/product-hub/new')).text.includes('master-legacy-banner'));
  const r = await quiet(() => call('POST', '/apps/product-hub/api/drafts', { ne_code: 'new-before-frozen', name: '切替前の新商品' }));
  assert.ok(!isGateRefusal(r), `${r.status} ${r.text.slice(0, 160)}`);
});
await t('product-hub (⑤-2a M5): 自動取込 (cron) は閉じたら丸ごと止める (ログを残す・下書きを作らない)・legacy_open は今までどおり動く', async () => {
  const { runProductHubIntake } = await import('../apps/product-hub/intake-cron.js');
  const lines = [];
  const l = console.log, w = console.warn, er = console.error;
  console.log = console.warn = console.error = (...a) => lines.push(a.join(' '));
  try {
    setPhase('frozen');
    const n0 = nDrafts();
    await runProductHubIntake();
    assert.equal(nDrafts(), n0);
    assert.ok(lines.some((x) => x.includes('intake skipped') && x.includes('frozen')), lines.join('\n'));
    lines.length = 0;
    setPhase('legacy_open');
    await runProductHubIntake();
    assert.ok(!lines.some((x) => x.includes('古い新商品の取込は閉じている')), lines.join('\n'));
  } finally { console.log = l; console.warn = w; console.error = er; }
});
await t('product-hub (R1 H5): 代表コードの税率は構成の SKU から (Company DB)。そろえば決まる・混ざる / 無い / 未解決 / 読めない = 決められない', async () => {
  const draft = phdb.prepare('SELECT * FROM product_drafts WHERE id = ?').get(repDraftId);
  let asked = null;
  __setCdbTaxReader(async (codes) => { asked = codes; return cdbRates({ 'ph-rep-a': 0.08, 'ph-rep-b': 0.08 })(codes); });
  let r = await resolveCdbDraftTax(phdb, draft);
  assert.deepEqual([r.ok, r.label, r.percent], [true, '8%', 8]);
  assert.deepEqual([...asked].sort(), ['ph-rep-a', 'ph-rep-b'], '代表コードそのものではなく構成の SKU を読む');
  __setCdbTaxReader(cdbRates({ 'ph-rep-a': 0.08, 'ph-rep-b': 0.1 }));
  r = await resolveCdbDraftTax(phdb, draft); assert.equal(r.ok, false); assert.match(r.reason, /混ざっている/);
  __setCdbTaxReader(cdbRates({ 'ph-rep-a': 0.08 }));
  r = await resolveCdbDraftTax(phdb, draft); assert.equal(r.ok, false); assert.match(r.reason, /Company DB に無い/);
  __setCdbTaxReader(cdbRates({ 'ph-rep-a': 0.08, 'ph-rep-b': null }));
  r = await resolveCdbDraftTax(phdb, draft); assert.equal(r.ok, false); assert.match(r.reason, /決まっていない/);
  __setCdbTaxReader(async () => ({ ok: false, reason: 'Company DB を読めない (試験)' }));
  r = await resolveCdbDraftTax(phdb, draft); assert.equal(r.ok, false); assert.match(r.reason, /読めない/);
  __setCdbTaxReader(null);
});
await t('product-hub (R1 H5): 楽天の出品のプレビューは、閉じた後に Company DB の税率を決められない = 止める (理由つき)・決まれば止めない・legacy_open は今までどおり', async () => {
  const taxBlocked = (r) => (r.json?.reasons || []).some((x) => x.includes('税率を Company DB から決められない'));
  setPhase('frozen');
  __setCdbTaxReader(async () => ({ ok: false, reason: 'Company DB を読めない (試験)' }));
  let r = await quiet(() => call('GET', `/apps/product-hub/api/drafts/${repDraftId}/rakuten/preview`));
  assert.equal(r.json.ok, false); assert.ok(taxBlocked(r), JSON.stringify(r.json));
  __setCdbTaxReader(cdbRates({ 'ph-rep-a': 0.08, 'ph-rep-b': 0.08 }));
  r = await quiet(() => call('GET', `/apps/product-hub/api/drafts/${repDraftId}/rakuten/preview`));
  assert.ok(!taxBlocked(r), JSON.stringify(r.json));
  setPhase('unreadable');
  __setCdbTaxReader(async () => ({ ok: false, reason: 'Company DB を読めない (試験)' }));
  r = await quiet(() => call('GET', `/apps/product-hub/api/drafts/${repDraftId}/rakuten/preview`));
  assert.ok(taxBlocked(r), '段階が読めない = Company DB の税率だけ = 読めなければ止める');
  setPhase('legacy_open');
  r = await quiet(() => call('GET', `/apps/product-hub/api/drafts/${repDraftId}/rakuten/preview`));
  assert.ok(!taxBlocked(r) && !(r.json?.reasons || []).some((x) => x.includes('税率の決め方')), JSON.stringify(r.json));
  __setCdbTaxReader(null);
});
await t('product-hub の詳細画面: 閉じたら手入力の欄 (id="y-tax") を出さず Company DB の税率 (代表コードは構成の SKU) を見せ、利益の試算もその税率・legacy_open は今までどおり', async () => {
  __setCdbTaxReader(cdbRates({ 'ph-rep-a': 0.08, 'ph-rep-b': 0.08 }));
  setPhase('frozen'); G.__resetLegacyGate();
  let r = await call('GET', `/apps/product-hub/detail/${repDraftId}`);
  assert.equal(r.status, 200, r.text.slice(0, 300));
  assert.ok(!r.text.includes('id="y-tax"'));
  assert.ok(/id="y-tax-cdb" value="8%"/.test(r.text));
  assert.ok(/data-tax="8"/.test(r.text), '利益の試算は Company DB の 8% (draft_yahoo は 10%)');
  __setCdbTaxReader(cdbRates({ 'ph-rep-a': 0.08, 'ph-rep-b': 0.1 }));
  G.__resetLegacyGate();
  r = await call('GET', `/apps/product-hub/detail/${repDraftId}`);
  assert.ok(r.text.includes('混ざっている') && /data-tax=""/.test(r.text), '決められない = 試算しない');
  setPhase('legacy_open'); G.__resetLegacyGate();
  let asked = false;
  __setCdbTaxReader(async (codes) => { asked = true; return cdbRates({})(codes); });
  r = await call('GET', `/apps/product-hub/detail/${repDraftId}`);
  assert.ok(r.text.includes('id="y-tax"') && !r.text.includes('y-tax-cdb') && /data-tax="10"/.test(r.text));
  assert.equal(asked, false, '閉じる前は Company DB を読まない');
  __setCdbTaxReader(null);
});

// 発注アプリの仕入先・売れ筋共有の表示名 (R1 H4)
await t('発注アプリ: 仕入先 (kind = suppliers) の追加・削除・CSV・宛先の CSV・一括取込は閉じたら 410 / 読めない 503・仕入先でないマスタ (発注条件) は止めない・legacy_open は今までどおり', async () => {
  const { getDB: poDB } = await import('../apps/purchase-orders/db.js');
  const sup = () => JSON.stringify(poDB().prepare('SELECT * FROM po_suppliers ORDER BY supplier_code').all());
  await quiet(async () => {
    for (const phase of [...CLOSED, 'unreadable']) {
      setPhase(phase);
      const want = phase === 'unreadable' ? 503 : 410;
      const before = sup();
      assert.equal((await call('POST', '/apps/purchase-orders/api/masters/suppliers', { supplier_code: '0777', name: '閉じた後の仕入先' })).status, want);
      assert.equal((await call('DELETE', '/apps/purchase-orders/api/masters/suppliers/0777')).status, want);
      assert.equal((await call('POST', '/apps/purchase-orders/api/masters/suppliers/csv', undefined, { csv: 'supplier_code,name\n0778,x\n' })).status, want);
      assert.equal((await call('POST', '/apps/purchase-orders/api/email/recipients/csv', undefined, { csv: 'supplier_code,email_to\n0001,a@example.com\n' })).status, want);
      assert.equal((await call('POST', '/apps/purchase-orders/api/import', undefined, { files: [['suppliers.csv', 'x\n']] })).status, want);
      assert.equal(sup(), before);
      const cond = await call('POST', '/apps/purchase-orders/api/masters/conditions', {});
      assert.ok(!isGateRefusal(cond), `発注条件は止めない: ${cond.status}`);
    }
  });
  setPhase('legacy_open');
  const r = await call('POST', '/apps/purchase-orders/api/masters/suppliers', { supplier_code: '0777', name: '切替前の仕入先' });
  assert.ok(!isGateRefusal(r) && r.status < 300, `${r.status} ${r.text.slice(0, 160)}`);
  assert.ok(sup().includes('切替前の仕入先'));
});
await t('売れ筋共有: 仕入先の表示名は閉じたら 410 / 読めない 503・legacy_open は今までどおり', async () => {
  const shown = () => mdb.prepare("SELECT 表示名 FROM supplier_share_master WHERE 仕入先コード = '0888'").get()?.表示名 ?? null;
  await quiet(async () => {
    for (const phase of [...CLOSED, 'unreadable']) {
      setPhase(phase);
      assert.equal((await call('POST', '/apps/supplier-sales/api/supplier-name', { code: '0888', name: '閉じた後' })).status, phase === 'unreadable' ? 503 : 410);
      assert.equal(shown(), null);
    }
  });
  setPhase('legacy_open');
  assert.equal((await call('POST', '/apps/supplier-sales/api/supplier-name', { code: '0888', name: '切替前' })).status, 200);
  assert.equal(shown(), '切替前');
});
server.close();

// ═══ D. CLI (子プロセス) ═══
console.log('── D. CLI (子プロセス) ──');
const PRELOAD = pathToFileURL(path.join(ROOT, 'scripts/test-master-legacy-gate-preload.mjs')).href;
const cliDir = path.join(DATA_DIR, 'cli');
fs.mkdirSync(cliDir, { recursive: true });
const baseEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^COMPANY_DB_/.test(k)));
function runCli(args, { phase = null, env = {}, cwd = ROOT } = {}) {
  const r = spawnSync(process.execPath, [...(phase ? ['--import', PRELOAD] : []), ...args], {
    cwd, encoding: 'utf8', timeout: 60000, windowsHide: true,
    env: { ...baseEnv, DATA_DIR: cliDir, ...(phase ? { TEST_LEGACY_PHASE: phase } : {}), ...env },
  });
  if (r.error) throw r.error;
  return { code: r.status, out: `${r.stdout}\n${r.stderr}` };
}
const whFile = path.join(cliDir, 'warehouse.db');
const Database = (await import('better-sqlite3')).default;
const countIn = (table, file = whFile) => { const d = new Database(file, { readonly: true }); try { return d.prepare(`SELECT count(*) AS c FROM ${table}`).get().c; } finally { d.close(); } };
const shipCsv = path.join(cliDir, 'ship.csv');
fs.writeFileSync(shipCsv, 'sku,name,code,method,cost,note\nne-aaa,A,S01,ゆうパケット,300,\nne-bbb,B,S01,ゆうパケット,300,\n');
const CSV_IMPORT = path.join(ROOT, 'apps/warehouse/csv-import.js');
const missingFile = path.join(cliDir, 'no-such-file.csv');

await t('csv-import.js product_shipping: 閉じたら DB を開かずに終了コード 3・legacy_open は今までどおり・閉じた後の全部消して入れ直しは起きない', async () => {
  let r = runCli([CSV_IMPORT, 'product_shipping', shipCsv], { phase: 'frozen' });
  assert.equal(r.code, 3, r.out); assert.ok(r.out.includes('マスタは新しい画面で直します'));
  assert.ok(!fs.existsSync(whFile), 'DB を作っていない = 触っていない');
  r = runCli([CSV_IMPORT, 'product_shipping', shipCsv], { phase: 'legacy_open' });
  assert.equal(r.code, 0, r.out); assert.equal(countIn('product_shipping'), 2);
  fs.writeFileSync(path.join(cliDir, 'empty.csv'), 'sku,name\n');
  for (const p of [...CLOSED, 'unreadable']) {
    r = runCli([CSV_IMPORT, 'product_shipping', path.join(cliDir, 'empty.csv')], { phase: p });
    assert.equal(r.code, 3, `${p}: ${r.out}`); assert.equal(countIn('product_shipping'), 2);
  }
});
await t('🚨 R1 H3: CLI の取込の途中 (DB を開いた後・書く前) に frozen になった = 書く直前にもう一度読んで終了コード 3・全部消して入れ直さない', async () => {
  const r = runCli([CSV_IMPORT, 'product_shipping', path.join(cliDir, 'empty.csv')], { phase: 'legacy_open_then_frozen' });
  assert.equal(r.code, 3, r.out); assert.equal(countIn('product_shipping'), 2);
});
await t('L7: 閉じた CLI は引数・ファイルの検査より前に終了コード 3 (ファイルが無い・引数が無いときも)', async () => {
  assert.equal(runCli([CSV_IMPORT, 'product_shipping', missingFile], { phase: 'frozen' }).code, 3);
  assert.equal(runCli([CSV_IMPORT, 'exception_genka'], { phase: 'frozen' }).code, 3);
  assert.equal(runCli([path.join(ROOT, 'apps/warehouse/import-sku-master.js')], { phase: 'frozen' }).code, 3);
  assert.equal(runCli([path.join(ROOT, 'apps/warehouse/migrate-reorder-setting-initial.js'), `--csv=${missingFile}`], { phase: 'frozen' }).code, 3);
  assert.equal(runCli([path.join(ROOT, 'apps/warehouse/migrate-reorder-setting-initial.js')], { phase: 'frozen' }).code, 3);
  // 切替前は今までどおり (ファイルが無い = 2)
  assert.equal(runCli([path.join(ROOT, 'apps/warehouse/migrate-reorder-setting-initial.js'), `--csv=${missingFile}`], { phase: 'legacy_open' }).code, 2);
});
await t('csv-import.js exception_genka: 閉じたら終了コード 3・legacy_open は入る', async () => {
  const g = path.join(cliDir, 'genka.csv');
  fs.writeFileSync(g, 'sku,genka,name\nne-aaa,120,A\n');
  assert.equal(runCli([CSV_IMPORT, 'exception_genka', g], { phase: 'company_owner' }).code, 3);
  assert.equal(countIn('exception_genka'), 0);
  assert.equal(runCli([CSV_IMPORT, 'exception_genka', g], { phase: 'legacy_open' }).code, 0);
  assert.equal(countIn('exception_genka'), 1);
});
await t('csv-import.js の止めない 5 つの mode (NE の商品・セット・受注・ロジザード・送料の表) は閉じた後も動く (ファイル単位ではなく mode 単位)', async () => {
  const w = (name, lines) => { const p = path.join(cliDir, name); fs.writeFileSync(p, lines.join('\r\n'), 'utf8'); return p; };
  const rates = Array.from({ length: 18 }, () => ''); rates[1] = '小型'; rates[2] = '日本郵便'; rates[3] = 'S09'; rates[4] = 'ゆうパケット'; rates[16] = '310';
  const LZ_HEADER = '在庫日,倉庫ID,倉庫名,ブロックID,ブロック略称,ロケ,商品ID,バーコード,商品名,有効期限,入荷日,品質区分ID,品質区分名,在庫数(引当数を含む),引当数,ロケ引当条件,ロケ業務区分,取置取引先ID,取置取引先名,棚卸状況,検索名称,検索名称２,大分類,中分類,小分類,商品予備項目００１,商品予備項目００２,商品予備項目００３,商品予備項目００４,商品予備項目００５,商品予備項目００６,商品予備項目００７,商品予備項目００８,商品予備項目００９,商品予備項目０１０,最終入荷日,最終出荷日,ブロック引当順';
  const LZ_ROW = '"20260815","1","B-Faith","18","R1FA","001-001-01","hakkaspray100","X0014Q5RST","ハッカ油スプレー","20280115","","1","良品","200","0","指定","卸","","","","ハッカ油スプレー","","","","","","","","","","","","","","","20260807","20260814","2"';
  const cases = [
    ['products', w('p.csv', [Array.from({ length: 18 }, (_, i) => `c${i}`).join(','), 'G0001,商品1,0001,100,200,取扱中,,,,0,,,,0,0,,0.1,0']), 'raw_ne_products'],
    ['sets', w('s.csv', [Array.from({ length: 7 }, (_, i) => `c${i}`).join(','), 'SET2,セット2,1000,G0001,1,0,']), 'raw_ne_set_products'],
    ['orders', w('o.csv', [Array.from({ length: 18 }, (_, i) => `c${i}`).join(','), ['D1', '2026-10-01', '', '', '', '', '', '', '', '1', '', '', 'g0001', '商品1', '', '1', '1', '100'].join(',')]), 'raw_ne_orders'],
    ['logizard', w('l.csv', [LZ_HEADER, LZ_ROW]), 'raw_lz_inventory'],
    ['shipping_rates', w('r.csv', [Array.from({ length: 18 }, (_, i) => `c${i}`).join(','), rates.join(',')]), 'shipping_rates'],
  ];
  for (const [mode, file, table] of cases) {
    const r = runCli([CSV_IMPORT, mode, file], { phase: 'new_open', env: { LZ_IMPORT_MIN_ROWS: '1' } });
    assert.equal(r.code, 0, `${mode}: ${r.out.slice(-600)}`);
    assert.ok(!r.out.includes('マスタは新しい画面で直します'), mode);
    assert.ok(countIn(table) >= 1, `${mode}: ${table} に入った`);
  }
});
await t('CLI: 段階を読めない (Company DB につながらない) = 終了コード 3 (fail-closed)', async () => {
  const r = runCli([CSV_IMPORT, 'product_shipping', shipCsv], { env: { COMPANY_DB_WATCH_URL: 'postgres://gate-test@127.0.0.1:1/none' } });
  assert.equal(r.code, 3, r.out); assert.ok(r.out.includes('読めない'));
  assert.equal(countIn('product_shipping'), 2);
});
await t('import-sales-class.js: 閉じたら終了コード 3 で product_sales_class を変えない・書く直前に閉じても書かない・legacy_open は入る', async () => {
  const cwd = path.join(DATA_DIR, 'isc');
  fs.mkdirSync(path.join(cwd, 'data', 'import'), { recursive: true });
  fs.copyFileSync(whFile, path.join(cwd, 'data', 'warehouse.db'));
  fs.writeFileSync(path.join(cwd, 'data', 'import', 'sales_class.csv'), 'sku,name,a,b,class\nne-aaa,A,,,2\n');
  const S = path.join(ROOT, 'apps/warehouse/import-sales-class.js');
  const cnt = () => countIn('product_sales_class', path.join(cwd, 'data', 'warehouse.db'));
  for (const p of [...CLOSED, 'unreadable', 'legacy_open_then_frozen']) {
    const r = runCli([S], { phase: p, cwd });
    assert.equal(r.code, 3, `${p}: ${r.out}`); assert.equal(cnt(), 0);
  }
  const r = runCli([S], { phase: 'legacy_open', cwd });
  assert.equal(r.code, 0, r.out); assert.equal(cnt(), 1);
});
await t('import-sku-master.js: 閉じたら --dry-run も終了コード 3・書く直前に閉じても書かない・legacy_open は入る', async () => {
  const csv = path.join(cliDir, 'skumaster.csv');
  fs.writeFileSync(csv, 'sku,asin,商品名,NE商品コード,数量\nsku-cli,B000,CLIの品,ne-aaa,1\n');
  const S = path.join(ROOT, 'apps/warehouse/import-sku-master.js');
  for (const args of [[csv, '--encoding=utf-8'], [csv, '--encoding=utf-8', '--dry-run']]) assert.equal(runCli([S, ...args], { phase: 'frozen' }).code, 3);
  assert.equal(runCli([S, csv, '--encoding=utf-8'], { phase: 'legacy_open_then_frozen' }).code, 3);
  assert.equal(countIn('m_sku_master'), 0);
  const r = runCli([S, csv, '--encoding=utf-8'], { phase: 'legacy_open' });
  assert.equal(r.code, 0, r.out); assert.equal(countIn('m_sku_master'), 1);
});
await t('migrate-reorder-setting-initial.js: 閉じたら終了コード 3・書く直前に閉じても書かない・legacy_open は入る', async () => {
  const csv = path.join(cliDir, 'pml.csv');
  fs.writeFileSync(csv, '商品コード,推奨保有在庫\nne-aaa,2.5\n');
  const S = path.join(ROOT, 'apps/warehouse/migrate-reorder-setting-initial.js');
  assert.equal(runCli([S, `--csv=${csv}`], { phase: 'new_open' }).code, 3);
  assert.equal(runCli([S, `--csv=${csv}`], { phase: 'legacy_open_then_frozen' }).code, 3);
  assert.equal(countIn('m_reorder_setting'), 0);
  const r = runCli([S, `--csv=${csv}`], { phase: 'legacy_open' });
  assert.equal(r.code, 0, r.out); assert.equal(countIn('m_reorder_setting'), 1);
});

G.__setLegacyPhaseReader(null);
await G.closeLegacyGatePool();
await pg.close();
console.log(process.exitCode ? `\n❌ 失敗あり (${passed} 件 OK)` : `\n✅ ${passed} 件 OK`);
