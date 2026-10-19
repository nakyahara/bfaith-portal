/**
 * test-ne-update-ui.mjs — 画面「NEへ送る」(0093・AI_reference CompanyDB構想/25 の PR-3・10/19 中原さん「わかりにくい」→ 一覧とボタン 1 つ)
 *
 * 固定する契約 (本物の router・PGlite):
 *   [S1] 画面: 見出し「NEへ送る」・左の列に「NEへ送る」・一覧 = 商品・欄・NE の今の値 → 自社の値・止めている欄はふつうの言葉の 1 行・
 *        画面に出さない言葉 (ファイル番号 #・予約・鍵・状態の名前 made / issued / queued / send / blocked など) が無い
 *   [S2] ボタン: 送り手 (miniPC) が動いていない = 「CSV を作る (NE にアップする用)」/ 動いていて上げてよい = 「今すぐ NE へ送る」/ 名簿 (MASTER_EDITORS) に無い = ボタンなし
 *   [S3] API: 名簿に無い = 403・今すぐ = 頼みを積む (同じ request_id = 前の答え)・CSV を作る → 受け取る (POST・sha256・UTF-8)・GET では受け取れない・
 *        試しの記録 = MASTER_DECISION_APPROVERS の人だけ
 *   [S4] 商品の画面: 「NE とちがう所が N つあります → NEへ送る」の 1 行 / 差が無い = 「NE と同じ」/ 保存の後の「NE でやること」が「NEへ送る」を言う
 *   [S5] 0093 の前 (関数が無い) = 画面は「まだ使えません」・商品の画面の 1 行は出さない
 *   [U1] 画面の JS (Chrome): CSV を作る → ダウンロード (sha256 を照らして保存) / 今すぐ NE へ送る → 「送るように頼みました」
 * 使い方: node scripts/test-ne-update-ui.mjs (MASTER_EDIT_UI_SKIP=1 で画面の JS を飛ばす)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import * as W from '../lib/master-write.mjs';

const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'neupd-ui-'));
process.env.DATA_DIR = DATA_TMP;
const watchdog = setTimeout(() => { console.error('  NG  試験が 600 秒で終わらない (止める)'); process.exit(1); }, 600000);
watchdog.unref();
let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const quiet = () => {};
const jsonOf = (r) => (typeof r === 'string' ? JSON.parse(r) : r);

// ═══════════ DB ═══════════
async function makeDb({ upTo = null } = {}) {
  const pg = new PGlite();
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet, ...(upTo ? { to: upTo } : {}) });
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  return { pg, db };
}
const { pg, db } = await makeDb();
const q = async (sql, p) => (await pg.query(sql, p)).rows;
const one = async (sql, p) => (await q(sql, p))[0];
const single = (code) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
const CODES = ['a01', 'a02', 'a03'];
const PLAN = { skus: CODES.map(single), setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [], variationGroups: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: CODES.map((c) => ({ supplierCode: '0001', skuCode: c })), primarySuppliers: CODES.map((c) => ({ skuCode: c, supplierCode: '0001' })),
  reorder: { available: false, runId: null, reason: '試験' } };
assert.equal((await runInitialLoad(db, PLAN, { log: quiet, runId: 'neupd_ui_1', host: 'test', now: new Date(Date.now() - 5 * 86400e3) })).ok, true);
const OWN = { ...Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])), 'skus.name': 'company', 'products.name': 'company', 'skus.handling': 'company', 'products.status': 'company',
  'skus.tax_rate': 'company', 'skus.tax_class': 'company', 'skus.standard_price': 'company', sku_costs: 'company', 'supplier_skus.is_primary': 'company', 'skus.sku_kind': 'company' };
await forceNewOpen(db, OWN);
const iso = (ms) => new Date(ms).toISOString();
/** 今朝の NE (全部 Company DB の今の値 = 差なし) */
async function seedNe() {
  const start = Date.now();
  const run = `mc_${iso(start + 2000).replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`;
  await q('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)', [run, iso(start + 2000)]);
  await q('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: CODES.map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
  const entries = CODES.flatMap((c) => Object.entries({ name: `商品 ${c}`, handling: 'active', tax_rate: 0.1, standard_price_jpy: 1000, cost: 100, primary_supplier: ['0001'] })
    .map(([col, value]) => ({ code_norm: c, col, ne_kind: 'single', state: 'ok', value })));
  const side = (s, n, extra) => ({ started_at: iso(s), finished_at: iso(s + 500), complete_rev: 1, fetched_rows: n, write_attempts: n, stored_rows: n, dropped_no_code: 0, dropped_missing_fields: 0, raw_rows: n, ...extra });
  await q('select ops.record_ne_values($1::jsonb)', [JSON.stringify({ compare_run_id: run, norm_version: 1,
    fetch: { fingerprint: 'f'.repeat(64), products: side(start, CODES.length, { codes: CODES.length, set_overlap: 0 }), sets: side(start + 1, 0, { codes: 0 }) }, entries })]);
}
await seedNe();
const sid = async (code) => String((await one('select sku_id from core.skus where code = $1', [code])).sku_id);
async function save(code, values) {
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  const cur = await W.readCurrent(db, await sid(code), today);
  const ev = (await one('select coalesce(max(event_id), 0)::text as id from events.master_change_events')).id;
  await pg.query('set role master_edit');
  try { return await W.saveSku(db, { actor: 'naka@test', requestId: crypto.randomUUID(), code, reason: '試験', seen: { token: W.editTokenOf(cur), event_id: ev }, values }, { open: true }); }
  finally { await pg.query('set role deploy'); }
}
const saved = await save('a01', { cost: { jpy: '150', reason: '試験' } });
await save('a02', { name: 'A02' });   // 名前 = 商品コード = 送らない (止めている欄)

// ═══════════ 本物の router ═══════════
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { default: router, __setPgClientFactory, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/neupd';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/neupd';
process.env.MASTER_EDITORS = 'naka@test,edit@test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let CUR = { pg };
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  const P = CUR.pg;
  await P.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => P.query(t, p), end: async () => { await P.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
let EMAIL = 'naka@test';
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: EMAIL, displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const B = `${ORIGIN}/apps/master-edit`;
const api = (p, body) => fetch(`${B}${p}`, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: ORIGIN }, body: JSON.stringify(body || {}) });
const apiJ = async (p, body) => { const r = await api(p, body); return { status: r.status, j: await r.json() }; };
const page = async (p) => (await fetch(`${B}${p}`)).text();
/** 画面の本文 (head・script を除く) */
const bodyText = (h) => h.replace(/<script[\s\S]*?<\/script>/g, '').replace(/<style[\s\S]*?<\/style>/g, '').replace(/<[^>]+>/g, ' ');

console.log('[S] 画面');
await ta('[S1] 画面: 見出し・左の列・一覧 (商品・欄・NE の今の値 → 自社の値)・止めている欄はふつうの言葉の 1 行・出さない言葉が無い', async () => {
  const h = await page('/ne-update');
  assert.match(h, /<h1>NEへ送る<\/h1>/);
  assert.match(h, /aria-current="page"[^>]*><svg[^>]*><use href="#i-up"\/><\/svg><span class="lbl">NEへ送る<\/span>/);
  assert.match(h, /id="nu-rows"/);
  const t = bodyText(h).replace(/\s+/g, ' ');
  assert.match(t, /a01 商品 a01 原価 100 円 → 150 円/);
  assert.match(t, /a02 .* 名前 商品 a02 → A02 名前が商品コードと同じなので送りません/);
  for (const w of ['#1', 'ファイル #', '予約', '鍵', 'made', 'issued', 'queued', 'blocked', 'sending', 'reserved', 'que_id', 'export']) assert.ok(!t.includes(w), `画面に「${w}」`);
  assert.match(h, /比べた NE の値 = 今朝 \d{2}:\d{2} の取り込み/);
});
await ta('[S2] ボタン: いつも「CSV を作る (NE にアップする用)」/ API の口が開いている = 「今すぐ NE へ送る」も / 名簿に無い = ボタンなし', async () => {
  let h = await page('/ne-update');
  assert.match(h, /id="nu-csv"[^>]*>.*CSV を作る \(NE にアップする用\)/);
  assert.doesNotMatch(h, /id="nu-send"/);
  await q('select ops.ne_upd_worker_seen($1, true, true, $2)', ['minipc', 'v']);
  h = await page('/ne-update');
  assert.match(h, /id="nu-csv"/);
  assert.match(h, /id="nu-send"[^>]*>.*今すぐ NE へ送る/);
  assert.match(h, /毎朝、自動でも NE に送っています/);
  EMAIL = 'other@test';
  h = await page('/ne-update');
  assert.doesNotMatch(h, /id="nu-send"|id="nu-csv"/);
  assert.match(h, /保存できるのは名簿の人だけです/);
  EMAIL = 'naka@test';
  await q("select ops.ne_upd_worker_seen($1, false, false, 'v')", ['minipc']);
});
await ta('[S3] API: 名簿に無い = 403・今すぐ = 頼みを積む・CSV を作る → 受け取る (POST・sha256)・GET では受け取れない・試しの記録 = 判断の名簿の人だけ', async () => {
  EMAIL = 'other@test';
  assert.equal((await api('/api/ne-update/send-now', { request_id: crypto.randomUUID() })).status, 403);
  assert.equal((await api('/api/ne-update/csv', { request_id: crypto.randomUUID() })).status, 403);
  EMAIL = 'naka@test';
  assert.equal((await apiJ('/api/ne-update/send-now', { request_id: 'x' })).status, 400);
  const rid = crypto.randomUUID();
  const s1 = await apiJ('/api/ne-update/send-now', { request_id: rid });
  assert.equal(s1.status, 200, JSON.stringify(s1.j));
  assert.equal(s1.j.state, 'waiting');
  const s2 = await apiJ('/api/ne-update/send-now', { request_id: rid });
  assert.equal(s2.j.replayed, true);
  const c = await apiJ('/api/ne-update/csv', { request_id: crypto.randomUUID() });
  assert.equal(c.status, 200, JSON.stringify(c.j));
  assert.equal(c.j.state, 'built');
  assert.equal(c.j.files.length, 1);
  const id = c.j.files[0].file_id;
  assert.equal((await fetch(`${B}/api/ne-update/files/${id}/download`)).status, 404, 'GET では受け取れない');
  const d = await api(`/api/ne-update/files/${id}/download`, {});
  assert.equal(d.status, 200);
  assert.equal(d.headers.get('content-type'), 'text/csv; charset=utf-8');
  const buf = Buffer.from(await d.arrayBuffer());
  assert.equal(buf.toString('utf8'), 'syohin_code,genka_tnk\r\na01,150\r\n');
  assert.equal(d.headers.get('x-content-sha256'), crypto.createHash('sha256').update(buf).digest('hex'));
  // 受け取った = 送っている途中 (画面の一言)
  const after = await page('/ne-update');
  assert.match(bodyText(after), /この欄は今送っている途中です \(NE に取り込めば、明日の朝に一覧から消えます\)/);
  assert.match(after, new RegExp(`data-dl="${id}"`), '今日作った CSV をもう一度ダウンロードできる');
  assert.doesNotMatch(bodyText(after), /#\d|ファイル #|ne-update-products/, 'ファイル番号・ファイル名は本文に出さない');
  EMAIL = 'edit@test';
  assert.equal((await api('/api/ne-update/verified', { kind: 'products', col: 'cost', result: 'ok' })).status, 403);
  EMAIL = 'naka@test';
  const v = await apiJ('/api/ne-update/verified', { kind: 'products', col: 'cost', result: 'ng', note: '試験' });
  assert.equal(v.status, 200, JSON.stringify(v.j));
  assert.equal((await apiJ('/api/ne-update/verified', { kind: 'products', col: 'stock', result: 'ok' })).status, 400);
});
await ta('[S4] 商品の画面: 「NE とちがう所が N つあります → NEへ送る」/ 差が無い = 「NE と同じ」・保存の後の「NE でやること」', async () => {
  const h = await page('/sku/a02');
  assert.match(h, /<a href="\/apps\/master-edit\/ne-update">NE とちがう所が 1 つあります → NEへ送る<\/a>/);
  assert.match(await page('/sku/a03'), /id="ne-diff"><span class="b ok">.*NE と同じ/);
  assert.ok(saved.ne_steps.some((s) => /「NEへ送る」の画面で CSV を作って取り込みます/.test(s)), JSON.stringify(saved.ne_steps));
  assert.match(await page('/manual'), /id="ne-update"/);
});
await ta('[S5] 0093 の前 (関数が無い) = 画面は「まだ使えません」・商品の画面の 1 行は出さない', async () => {
  const before = (fs.readdirSync(new URL('../db/company/migrations/', import.meta.url)).find((f) => /_ne_update_send\.sql$/.test(f)) || '0093').slice(0, 4);
  const old = await makeDb({ upTo: String(Number(before) - 1).padStart(4, '0') });
  assert.equal((await runInitialLoad(old.db, PLAN, { log: quiet, runId: 'neupd_ui_old', host: 'test', now: new Date(Date.now() - 5 * 86400e3) })).ok, true);
  CUR = { pg: old.pg };
  try {
    const h = await page('/ne-update');
    assert.match(h, /まだ使えません: Company DB にまだこの仕組みが入っていません/);
    assert.doesNotMatch(h, /id="nu-csv"|id="nu-send"/);
    assert.match(await page('/sku/a01'), /id="ne-diff"><span class="hint">—/);
  } finally { CUR = { pg }; await old.pg.close(); }
});

console.log('[U] 画面の JS');
if (process.env.MASTER_EDIT_UI_SKIP === '1') console.log('  ⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の JS は飛ばす');
else {
  let browser;
  try { const { chromium } = await import('playwright'); browser = await chromium.launch(); }
  catch (e) { console.error(`  NG  画面の JS の試験を動かせない (Playwright / Chromium): ${e.message}`); process.exitCode = 1; }
  if (browser) {
    await ta('[U1] CSV を作る → ダウンロード (sha256 を照らして保存) / 今すぐ NE へ送る → 「送るように頼みました」', async () => {
      await save('a03', { standard_price: '1300' });
      const ctx = await browser.newContext({ viewport: { width: 1280, height: 860 }, locale: 'ja-JP', acceptDownloads: true });
      const p = await ctx.newPage();
      const errors = []; p.on('pageerror', (e) => errors.push(e.message));
      p.on('console', (m) => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errors.push(m.text()); });   // 4xx は下の response で (favicon.png はこの試験の app に無い)
      p.on('response', (r) => { if (r.status() >= 400 && !/favicon/.test(r.url())) errors.push(`${r.status()} ${r.url()}`); });
      try {
        await p.goto(`${B}/ne-update`);
        const [dl] = await Promise.all([p.waitForEvent('download', { timeout: 15000 }), p.click('#nu-csv')]);
        assert.equal(fs.readFileSync(await dl.path(), 'utf8'), 'syohin_code,baika_tnk\r\na03,1300\r\n');
        await p.waitForFunction(() => /NE に取り込んでください|保存しました/.test(document.querySelector('#nu-msg')?.textContent || ''), null, { timeout: 15000 });
        assert.equal(await p.locator('#nu-csv').isDisabled(), false, 'CSV を作った後も次を作れる');
        await q('select ops.ne_upd_worker_seen($1, true, false, $2)', ['minipc', 'v']);
        await save('a03', { standard_price: '1400' });
        await p.goto(`${B}/ne-update`);
        if (process.env.NEUPD_SHOT) await p.screenshot({ path: process.env.NEUPD_SHOT, fullPage: true });   // 見た目を人が見るとき (試験の判定には使わない)
        await p.click('#nu-send');
        await p.waitForFunction(() => /NE へ送る頼みを受け付けました/.test(document.querySelector('#nu-msg')?.textContent || ''), null, { timeout: 15000 });
        assert.deepEqual(errors, []);
      } finally { await ctx.close(); }
    });
    await browser.close();
  }
}

server.close();
await pg.close();
try { fs.rmSync(DATA_TMP, { recursive: true, force: true }); } catch { /* */ }
console.log(`${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
