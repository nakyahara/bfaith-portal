/**
 * test-ph-jan-compare.mjs — product-hub の JAN と Company DB の JAN を比べる印 (Company DB構想 21 §5 B1・読むだけ)
 *
 * Company DB = PGlite (最新の migration まで・持ち主のロール deploy・読むだけのロール watcher)。product-hub = 一時の DATA_DIR の SQLite (本物の router)。
 * 固定する契約:
 *   J 印の決め方 (純粋な関数): 一致 (複数のうちの 1 つも) / 違う / Company DB に無い (JAN が 0 個・セット) / 商品コードが無い / product-hub は空 / どちらにも無い /
 *     読めない (読めない・答えに無い・形が違う)・ほかの商品の JAN なら holder・まとめ (一番重い印と数)・注意の文 (warn と読めないだけ・読めないは 1 行)
 *   P 本物の SQL (PGlite): 有効な JAN だけ (外した = valid_to は数えない)・1 商品に複数・全角の数字も同じ決まりで正規化・ほかの商品の JAN・セット・無い商品コード・
 *     読むだけのロール (watcher) でも読める・🚨 読むだけの取引 (begin transaction read only → rollback) = 中で書けない・失敗しても rollback
 *   U 比べる単位: 単品 = 出品の商品コード ↔ product_drafts.jan_code / まとまり = 子の SKU ごと ↔ draft_sku_jans (ページ代表の jan_code は比べない)・外した子は比べない
 *   R router: GET /api/drafts/:id/jan-check・GET /api/board/jan-check (warn のカードだけ)・楽天のプレビューの warnings に足す (payload は同じ・止めない)・
 *     読めない = 200 で「読めない」・🚨 Company DB にも product-hub の SQLite にも何も書かない (行の数とハッシュが同じ)
 *   H 画面: 詳細 (単品 / まとまり) に印の置き場・見かたの説明・ボードに状態の欄・画面の JS が読める (vm)
 * 使い方: node scripts/test-ph-jan-compare.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phjan-data-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;
delete process.env.COMPANY_DB_URL;
delete process.env.COMPANY_DB_WATCH_URL;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const LG = await import('../lib/master-legacy-gate.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const J = await import('../apps/product-hub/services/cdb-jan-compare.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

/** EAN-13 のチェック数字を付ける (12 桁 → 13 桁) */
const ean = (d12) => { const s = String(d12); let t = 0; for (let i = 0; i < 12; i++) t += Number(s[i]) * (i % 2 ? 3 : 1); return s + ((10 - (t % 10)) % 10); };
const JAN = Object.fromEntries(['a1', 'a2x', 'a2y', 'a3', 'a5', 'a6', 'a9', 'g1', 'g2', 'gx', 's1', 'z1', 'other'].map((k, i) => [k, ean(`4900000${String(100 + i).padStart(5, '0')}`)]));
const toFull = (s) => s.replace(/[0-9]/g, (c) => String.fromCharCode(c.charCodeAt(0) + 0xfee0));

// ─── J 印の決め方 (純粋な関数) ───
console.log('J 印の決め方');
const rd = (rows) => ({ ok: true, byPair: new Map(rows.map(([code, jan, row]) => [`${code.toLowerCase()}\u0000${jan}`, row])) });
const R0 = (x = {}) => ({ sku_id: '1', sku_kind: 'single', product_id: '10', jans: [], jan_norm: null, jan_holder: null, ...x });
const J2 = (...vs) => vs.map((v, i) => ({ row: String(i + 1), value: v, norm: v }));
await ta('[J1] 一致 / 複数のうちの 1 つ (数と全部の JAN を添える)', () => {
  const a = J.judgeJanUnit({ code: 'A', phJan: '111' }, rd([['A', '111', R0({ jans: J2('111'), jan_norm: '111' })]]));
  assert.deepEqual([a.state, a.severity, a.label, a.detail, a.cdbCount], ['match', 'ok', '一致', '', 1]);
  const b = J.judgeJanUnit({ code: 'A', phJan: '222' }, rd([['A', '222', R0({ jans: J2('111', '222'), jan_norm: '222' })]]));
  assert.equal(b.state, 'match');
  assert.match(b.detail, /JAN が 2 個 \(111・222\) — そのうちの 1 つ/);
});
await ta('[J2] 違う / Company DB に無い / ほかの商品の JAN (holder)', () => {
  const a = J.judgeJanUnit({ code: 'A', phJan: '999' }, rd([['A', '999', R0({ jans: J2('111'), jan_norm: '999', jan_holder: 'B01' })]]));
  assert.deepEqual([a.state, a.severity, a.holder], ['differs', 'warn', 'B01']);
  assert.match(a.detail, /Company DB の JAN = 111。この JAN は Company DB ではほかの商品 \(B01\) の JAN/);
  const b = J.judgeJanUnit({ code: 'A', phJan: '999' }, rd([['A', '999', R0({ jan_norm: '999' })]]));
  assert.deepEqual([b.state, b.severity, b.label], ['not_in_cdb', 'warn', 'Company DB に無い']);
  assert.match(b.detail, /JAN が 1 つも無い/);
});
await ta('[J3] セット・例外 (product 無し) は JAN を持たない = Company DB に無い / product-hub も空なら印なし', () => {
  const a = J.judgeJanUnit({ code: 'S', phJan: '999' }, rd([['S', '999', R0({ sku_kind: 'set', product_id: null, jan_norm: '999' })]]));
  assert.deepEqual([a.state, a.detail], ['not_in_cdb', 'Company DB ではセットは JAN を持たない']);
  const b = J.judgeJanUnit({ code: 'S', phJan: '' }, rd([['S', '', R0({ sku_kind: 'set', product_id: null })]]));
  assert.equal(b.state, 'none');
});
await ta('[J4] 商品コードが Company DB に無い (JAN があれば warn・無ければ印なし) / product-hub は空 (1 個・複数) / どちらにも無い', () => {
  assert.equal(J.judgeJanUnit({ code: 'Z', phJan: '9' }, rd([['Z', '9', R0({ sku_id: null, product_id: null, sku_kind: null })]])).state, 'sku_missing');
  assert.equal(J.judgeJanUnit({ code: 'Z', phJan: '' }, rd([['Z', '', R0({ sku_id: null, product_id: null, sku_kind: null })]])).state, 'none');
  const e1 = J.judgeJanUnit({ code: 'A', phJan: '' }, rd([['A', '', R0({ jans: J2('111') })]]));
  assert.deepEqual([e1.state, e1.severity, e1.detail], ['ph_empty', 'info', 'Company DB の JAN = 111']);
  assert.match(J.judgeJanUnit({ code: 'A', phJan: '' }, rd([['A', '', R0({ jans: J2('111', '222') })]])).detail, /JAN が 2 個/);
  assert.equal(J.judgeJanUnit({ code: 'A', phJan: '' }, rd([['A', '', R0()]])).state, 'none');
});
await ta('[J5] 読めない: 読めなかった・答えに無い・形が違う (jans が文字の JSON なら読む)', () => {
  const a = J.judgeJanUnit({ code: 'A', phJan: '1' }, { ok: false, reason: '接続先が無い' });
  assert.deepEqual([a.state, a.severity, a.detail], ['unreadable', 'muted', '接続先が無い']);
  assert.equal(J.judgeJanUnit({ code: 'A', phJan: '1' }, rd([])).state, 'unreadable');
  assert.equal(J.judgeJanUnit({ code: 'A', phJan: '1' }, rd([['A', '1', R0({ jans: 'x{' })]])).state, 'unreadable');
  assert.equal(J.judgeJanUnit({ code: 'A', phJan: '1' }, rd([['A', '1', R0({ jans: JSON.stringify(J2('1')), jan_norm: '1' })]])).state, 'match');
});
await ta('[J6] まとめ = 一番重い印と数・注意の文は warn と読めないだけ (読めないは 1 行)', () => {
  const u = (state) => ({ state, severity: J.JAN_STATE_SEVERITY[state], code: 'c', phJan: '1', label: J.JAN_STATE_LABELS[state], detail: '' });
  const s = J.summarizeJanUnits([u('match'), u('differs'), u('ph_empty'), u('differs')]);
  assert.deepEqual([s.state, s.severity, s.label, s.counts.differs], ['differs', 'warn', 'Company DB と違う (2 SKU)', 2]);
  assert.deepEqual(J.summarizeJanUnits([]).state, 'none');
  assert.equal(J.summarizeJanUnits([u('match')]).label, '一致');
  assert.equal(J.janWarningsOf({ ok: true, units: [u('match'), u('ph_empty'), u('none')] }).length, 0);
  const w = J.janWarningsOf({ ok: true, units: [u('differs'), u('not_in_cdb'), u('match')] });
  assert.equal(w.length, 2);
  assert.ok(w.every((x) => /出品は止めません/.test(x) && /master-edit/.test(x)));
  assert.equal(J.janWarningsOf({ ok: true, readable: false, reason: 'x', units: [u('unreadable'), u('unreadable')] }).length, 1);
  assert.equal(J.janWarningsOf({ ok: false, reason: '6 秒で読めなかった', units: [] }).length, 1);
});

// ─── P 本物の SQL (PGlite) ───
console.log('P 本物の SQL');
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
const plan = {
  skus: [sku('a001', '一致'), sku('a002', '複数'), sku('a003', '違う'), sku('a004', 'JAN 無し'), sku('a005', 'ほかの持ち主'), sku('a006', '外した JAN'),
    sku('g-red', '色 赤'), sku('g-blue', '色 青'), sku('g-wht', '色 白'), sku('set001', 'セット', { kind: 'set', cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE' } })],
  variationGroups: [],
  setComponents: [{ parentCode: 'set001', childCode: 'a001', qty: 2, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_phjan' },
};
const loadR = await runInitialLoad(db, plan, { log: quiet, runId: 'load_phjan', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(loadR.ok, true, loadR.error);
const pidOf = async (code) => (await pg.query('select product_id::text as id from core.skus where code = $1', [code])).rows[0].id;
async function addJan(code, jan, { closed = false } = {}) {
  const r = await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
    values (1, 'product', $1, 'jan', 'jan', $2, 'manual', 'human', 'phjan-test') returning external_id_row::text as id`, [await pidOf(code), jan]);
  if (closed) await pg.query('update core.external_ids set valid_to = now() where external_id_row = $1', [r.rows[0].id]);
}
await addJan('a001', JAN.a1);
await addJan('a002', JAN.a2x); await addJan('a002', JAN.a2y);
await addJan('a003', JAN.a3);
await addJan('a005', JAN.a5);
await addJan('a006', JAN.a6, { closed: true });
await addJan('g-red', JAN.g1);
await addJan('g-blue', JAN.g2);
const cdbFingerprint = async () => (await pg.query(`select count(*)::int as n, md5(coalesce(string_agg(external_id_row::text || ':' || external_value || ':' || coalesce(valid_to::text, '-'), ',' order by external_id_row), '')) as h from core.external_ids`)).rows[0];
const cdbBefore = await cdbFingerprint();

/** PGlite を「読む口」に: 取引の文を記録する (読むだけの取引で読んでいるかを確かめる) */
const stmts = [];
const pgClient = { query: (sql, params) => { stmts.push(String(sql).split('\n')[0].trim()); return pg.query(sql, params); } };
const readWith = (client) => async (items) => ({ ok: true, rows: await J.queryCdbJans(client, 1, items) });
J.__setCdbJanReader(readWith(pgClient));
const unit = async (code, phJan) => J.judgeJanUnit({ code, phJan }, await J.readCdbJans([{ code, jan: phJan }]));

await ta('[P1] 一致・複数のうちの 1 つ・違う・JAN 無し + ほかの持ち主・外した JAN は数えない・product-hub は空', async () => {
  const a = await unit('a001', JAN.a1);
  assert.deepEqual([a.state, a.cdbJans, a.holder], ['match', [JAN.a1], null], '自分の商品の JAN を「ほかの商品」とした');
  const b = await unit('a002', JAN.a2y);
  assert.deepEqual([b.state, b.cdbCount, b.cdbJans], ['match', 2, [JAN.a2x, JAN.a2y].sort()]);
  const c = await unit('a003', JAN.a9);
  assert.deepEqual([c.state, c.holder], ['differs', null]);
  const d = await unit('a004', JAN.a5);
  assert.deepEqual([d.state, d.holder], ['not_in_cdb', 'a005']);
  const e = await unit('a006', JAN.a6);
  assert.deepEqual([e.state, e.cdbCount, e.holder], ['not_in_cdb', 0, null], '外した JAN を有効として数えた');
  const f = await unit('a005', '');
  assert.deepEqual([f.state, f.detail], ['ph_empty', `Company DB の JAN = ${JAN.a5}`]);
  const g = await unit('a003', JAN.a1);
  assert.deepEqual([g.state, g.holder], ['differs', 'a001']);
});
await ta('[P2] 全角の数字・前後の空白も Company DB と同じ決まり (core.norm_code) で比べる・商品コードの大文字小文字', async () => {
  assert.equal((await unit('a001', toFull(JAN.a1))).state, 'match');
  assert.equal((await unit('A001', JAN.a1)).state, 'match');
});
await ta('[P3] セット (product 無し) = Company DB に無い・Company DB に無い商品コード = 商品コードが無い', async () => {
  const s = await unit('set001', JAN.s1);
  assert.deepEqual([s.state, s.detail], ['not_in_cdb', 'Company DB ではセットは JAN を持たない']);
  assert.equal((await unit('nope-999', JAN.z1)).state, 'sku_missing');
});
await ta('[P4] 🚨 読むだけの取引: begin transaction read only → 読む → rollback・中で書くと断られる・失敗しても rollback', async () => {
  stmts.length = 0;
  await J.readCdbJans([{ code: 'a001', jan: JAN.a1 }, { code: 'a002', jan: '' }, { code: 'a001', jan: JAN.a1 }]);
  assert.deepEqual([stmts[0], stmts.at(-1), stmts.length], ['begin transaction read only', 'rollback', 3]);
  // 読むだけの取引の中で書こうとすると PostgreSQL が断る (= この取引からは書けない)
  await pg.query('begin transaction read only');
  await assert.rejects(pg.query(`update core.skus set name = name where code = 'a001'`), /read-only/);
  await pg.query('rollback');
  // SQL が失敗しても rollback する (取引を開いたままにしない)
  stmts.length = 0;
  const bad = { query: (sql, params) => { stmts.push(String(sql).split('\n')[0].trim()); if (/^select x\.ord/.test(String(sql).trim())) return Promise.reject(new Error('boom')); return pg.query(sql, params); } };
  J.__setCdbJanReader(readWith(bad));
  const r = await J.readCdbJans([{ code: 'a001', jan: JAN.a1 }]);
  assert.deepEqual([r.ok, /boom/.test(r.reason)], [false, true]);
  assert.deepEqual([stmts[0], stmts.at(-1)], ['begin transaction read only', 'rollback']);
  assert.equal((await pg.query(`select count(*)::int as n from pg_stat_activity where state like 'idle in transaction%'`).catch(() => ({ rows: [{ n: 0 }] }))).rows[0].n, 0);
  J.__setCdbJanReader(readWith(pgClient));
});
await ta('[P5] 読むだけのロール (watcher) でも読める (COMPANY_DB_WATCH_URL のとき)', async () => {
  await pg.query('set role watcher');
  try {
    const rows = await J.queryCdbJans(pg, 1, [{ code: 'a002', jan: JAN.a2x }, { code: 'a004', jan: JAN.a5 }]);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].jans.length, 2);
    assert.equal(rows[1].jan_holder, 'a005');
  } finally { await pg.query('set role deploy'); }
});
await ta('[P6] 接続先が無い = 読めない (接続しない)・組が多すぎる = 読めない・空 = 読まない', async () => {
  J.__setCdbJanReader(null);
  const r = await J.readCdbJans([{ code: 'a001', jan: '1' }], { env: {} });
  assert.deepEqual(r, { ok: false, reason: 'Company DB の接続先が無い' });
  J.__setCdbJanReader(readWith(pgClient));
  const many = Array.from({ length: J.JAN_READ_MAX_ITEMS + 1 }, (_, i) => ({ code: `c${i}`, jan: '' }));
  assert.equal((await J.readCdbJans(many)).ok, false);
  stmts.length = 0;
  assert.deepEqual([(await J.readCdbJans([])).ok, stmts.length], [true, 0]);
});

// ─── U 比べる単位 (product-hub の SQLite) ───
console.log('U 比べる単位');
const ph = PHDB.getDB();
let mirrorPid = 900000;
const mirror = (code, repCode) => ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', ?, '2026-10-13T00:00:00Z')`).run(++mirrorPid, code, repCode);
for (const c of ['a001', 'a002', 'a003', 'a004', 'set001']) mirror(c, null);
mirror('g-red', 'grpx'); mirror('g-blue', 'grpx'); mirror('g-wht', 'grpx');
const draft = (code, jan) => {
  ph.prepare('INSERT INTO product_drafts (ne_code, name, jan_code) VALUES (?, ?, ?)').run(code, `下書き ${code}`, jan);
  return ph.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get(code);
};
const dA1 = draft('a001', JAN.a1);
const dA3 = draft('a003', JAN.a9);
const dA4 = draft('a004', null);
const dSet = draft('set001', JAN.s1);
const dG = draft('grpx', JAN.gx);   // ページ代表の JAN (楽天に送らない = 比べない)
const sj = ph.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)');
sj.run(dG.id, 'g-red', JAN.g1); sj.run(dG.id, 'g-blue', JAN.a3);   // 赤 = 一致 / 青 = ほかの商品 (a003) の JAN / 白 = 空 (Company DB にも無い)
sj.run(dG.id, 'g-gone', JAN.z1);   // まとまりに居ない子の行 (楽天に送らない = 比べない)
const phFingerprint = () => {
  const t = ['product_drafts', 'draft_sku_jans', 'draft_events', 'draft_rakuten', 'draft_step_progress'];
  return Object.fromEntries(t.map((n) => [n, crypto.createHash('sha256').update(JSON.stringify(ph.prepare(`SELECT * FROM ${n} ORDER BY rowid`).all())).digest('hex')]));
};

await ta('[U1] 単品 = 出品の商品コード ↔ product_drafts.jan_code・まとまり = 子の SKU ごと ↔ draft_sku_jans (ページ代表・居ない子は比べない)', () => {
  assert.deepEqual(J.janUnitsForDraft(ph, dA1), { mode: 'single', units: [{ code: 'a001', phJan: JAN.a1 }] });
  const g = J.janUnitsForDraft(ph, dG);
  assert.equal(g.mode, 'variation');
  assert.deepEqual(g.units, [{ code: 'g-blue', phJan: JAN.a3 }, { code: 'g-red', phJan: JAN.g1 }, { code: 'g-wht', phJan: '' }]);
});
await ta('[U2] まとまりの子を外した (variation/exclude の行) = その子は比べない', () => {
  ph.prepare(`INSERT INTO draft_variation_exclusions (draft_id, ne_code) VALUES (?, 'g-wht')`).run(dG.id);
  try {
    assert.deepEqual(J.janUnitsForDraft(ph, dG).units.map((u) => u.code), ['g-blue', 'g-red']);
  } finally { ph.prepare('DELETE FROM draft_variation_exclusions WHERE draft_id = ?').run(dG.id); }
});
await ta('[U3] ドラフトを比べる: まとまり = 子ごとの印とまとめ', async () => {
  const r = await J.compareDraftJans(ph, dG);
  assert.deepEqual(r.units.map((u) => [u.code, u.state]), [['g-blue', 'differs'], ['g-red', 'match'], ['g-wht', 'none']]);
  assert.equal(r.units[0].holder, 'a003');
  assert.deepEqual([r.summary.state, r.summary.label], ['differs', 'Company DB と違う (1 SKU)']);
});

// ─── R router ───
console.log('R router');
LG.__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open', changed_at: '2030-01-01', changed_by: 'x' }));
const app = express();
app.set('view engine', 'ejs');
app.use(express.json());
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['product-hub'] }; next(); });
const PHR = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', PHR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url) {
  const r = await fetch(ORIGIN + url, { method, headers: { Accept: 'application/json', Origin: ORIGIN }, signal: AbortSignal.timeout(30000) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
function checkScripts(html) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  assert.equal(all.length, [...html.matchAll(/<script\b/gi)].length, 'script の開きと閉じの数が合わない');
  for (const m of all) {
    if (/type="application\/json"/.test(m[1])) { JSON.parse(m[2]); continue; }
    if (/\bsrc=/.test(m[1])) continue;
    new vm.Script(m[2]);
    assert.ok(!/<%|%>/.test(m[2]), 'EJS のタグが JS に残っている');
  }
}
const phBefore = phFingerprint();

try {
  await ta('[R1] GET /api/drafts/:id/jan-check = 単品 (一致 / 違う / Company DB に無い = セット / product-hub は空でも Company DB にも無い = 印なし)', async () => {
    const a = await call('GET', `/apps/product-hub/api/drafts/${dA1.id}/jan-check`);
    assert.equal(a.status, 200);
    assert.deepEqual([a.j.ok, a.j.readable, a.j.mode, a.j.units[0].state, a.j.summary.state], [true, true, 'single', 'match', 'match']);
    assert.equal((await call('GET', `/apps/product-hub/api/drafts/${dA3.id}/jan-check`)).j.units[0].state, 'differs');
    assert.equal((await call('GET', `/apps/product-hub/api/drafts/${dSet.id}/jan-check`)).j.units[0].state, 'not_in_cdb');
    assert.equal((await call('GET', `/apps/product-hub/api/drafts/${dA4.id}/jan-check`)).j.units[0].state, 'none');
    assert.equal((await call('GET', '/apps/product-hub/api/drafts/999999/jan-check')).status, 404);
  });
  await ta('[R2] GET /api/drafts/:id/jan-check = まとまり (子の SKU ごと)', async () => {
    const r = await call('GET', `/apps/product-hub/api/drafts/${dG.id}/jan-check`);
    assert.deepEqual([r.j.mode, r.j.units.map((u) => u.state)], ['variation', ['differs', 'match', 'none']]);
  });
  await ta('[R3] GET /api/board/jan-check = product-hub に JAN があるカードだけ比べ、違う / 無いのカードだけ返す', async () => {
    // 読む組は product-hub に JAN がある単位だけ (空の子 g-wht・JAN の無い a004・ページ代表の JAN は読まない)
    const asked = [];
    J.__setCdbJanReader(async (items) => { asked.push(...items.map((i) => `${i.code}=${i.jan}`)); return readWith(pgClient)(items); });
    let r;
    try { r = await call('GET', '/apps/product-hub/api/board/jan-check'); } finally { J.__setCdbJanReader(readWith(pgClient)); }
    assert.deepEqual(asked.sort(), [`a001=${JAN.a1}`, `a003=${JAN.a9}`, `g-blue=${JAN.a3}`, `g-red=${JAN.g1}`, `set001=${JAN.s1}`].sort());
    assert.deepEqual([r.status, r.j.ok, r.j.readable, r.j.checked], [200, true, true, 4]);   // a001・a003・set001・grpx (a004 は JAN 無し)
    assert.deepEqual(Object.keys(r.j.byDraft).map(Number).sort((x, y) => x - y), [dA3.id, dSet.id, dG.id].sort((x, y) => x - y));
    assert.deepEqual(r.j.byDraft[dG.id].units.map((u) => u.code), ['g-blue']);
    assert.equal(r.j.byDraft[dA3.id].label, 'Company DB と違う');
  });
  await ta('[R4] 楽天のプレビュー: JAN の注意を warnings に足す (payload の可否は変えない)・一致なら足さない', async () => {
    const bad = await call('GET', `/apps/product-hub/api/drafts/${dA3.id}/rakuten/preview`);
    assert.equal(bad.status, 200);
    assert.ok((bad.j.warnings || []).some((w) => /JAN Company DB と違う: a003/.test(w)), JSON.stringify(bad.j.warnings));
    const good = await call('GET', `/apps/product-hub/api/drafts/${dA1.id}/rakuten/preview`);
    assert.ok(!(good.j.warnings || []).some((w) => /JAN/.test(w)), JSON.stringify(good.j.warnings));
    // 印を足しても止める理由 (reasons) は同じ = 印は知らせだけ
    J.__setCdbJanReader(async () => ({ ok: false, reason: 'テストで読めない' }));
    const off = await call('GET', `/apps/product-hub/api/drafts/${dA3.id}/rakuten/preview`);
    J.__setCdbJanReader(readWith(pgClient));
    assert.deepEqual(off.j.reasons, bad.j.reasons);
    assert.equal(off.j.ok, bad.j.ok);
    assert.ok(off.j.warnings.some((w) => /JAN を Company DB と比べられませんでした \(テストで読めない\)/.test(w)));
  });
  await ta('[R5] 読めない = 200 で「読めない」(詳細・ボード)・読み手が例外を投げても 200', async () => {
    J.__setCdbJanReader(async () => { throw new Error('down'); });
    try {
      const a = await call('GET', `/apps/product-hub/api/drafts/${dA1.id}/jan-check`);
      assert.deepEqual([a.status, a.j.readable, a.j.units[0].state], [200, false, 'unreadable']);
      const b = await call('GET', '/apps/product-hub/api/board/jan-check');
      assert.deepEqual([b.status, b.j.readable, b.j.byDraft], [200, false, {}]);
      assert.match(b.j.reason, /down/);
    } finally { J.__setCdbJanReader(readWith(pgClient)); }
  });
  await ta('[R6] 時間切れ = 読めない (待つ上限つき)', async () => {
    const r = await J.compareDraftJansWithin(ph, dA1, { waitMs: 50, env: {} });
    assert.equal(r.ok, true);   // 読み手が速いとき
    J.__setCdbJanReader(() => new Promise((resolve) => setTimeout(() => resolve({ ok: true, rows: [] }), 500)));
    try {
      const slow = await J.compareDraftJansWithin(ph, dA1, { waitMs: 50 });
      assert.deepEqual([slow.ok, /秒で読めなかった/.test(slow.reason)], [false, true]);
      assert.equal(J.janWarningsOf(slow).length, 1);
    } finally { J.__setCdbJanReader(readWith(pgClient)); }
  });
  await ta('[H1] 詳細画面 (単品・まとまり): 印の置き場 (子ごと)・見かたの説明・画面の JS が読める', async () => {
    const a = await call('GET', `/apps/product-hub/detail/${dA1.id}`);
    assert.equal(a.status, 200, a.text.slice(0, 300));
    assert.match(a.text, /id="jan-cdb-summary"/);
    assert.match(a.text, /JAN の印の見かた/);
    assert.match(a.text, /<div class="jan-cdb-mark" data-code="a001"><\/div>/);
    checkScripts(a.text);
    const g = await call('GET', `/apps/product-hub/detail/${dG.id}`);
    assert.equal(g.status, 200);
    for (const c of ['g-red', 'g-blue', 'g-wht']) assert.match(g.text, new RegExp(`<div class="jan-cdb-mark" data-code="${c}"></div>`));
    checkScripts(g.text);
  });
  await ta('[H2] ボード: JAN の状態の欄・画面の JS が読める', async () => {
    const b = await call('GET', '/apps/product-hub/board');
    assert.equal(b.status, 200, b.text.slice(0, 300));
    assert.match(b.text, /id="kb-jan-status"/);
    assert.match(b.text, /\/apps\/product-hub\/api\/board\/jan-check/);
    checkScripts(b.text);
  });
  await ta('[R7] 🚨 何も書かない: Company DB の JAN の行も product-hub の表も、比べる前と同じ', async () => {
    assert.deepEqual(await cdbFingerprint(), cdbBefore);
    const now = phFingerprint();
    // 詳細画面を開くと工程の行を作る (表示の自己修復・この PR の前から) = 工程の表は比べない
    delete now.draft_step_progress; const before = { ...phBefore }; delete before.draft_step_progress;
    assert.deepEqual(now, before);
  });
  await ta('[R8] ソース: 比べる部品は INSERT / UPDATE / DELETE を書かない・router の 2 つの入口は GET', () => {
    const src = fs.readFileSync(new URL('../apps/product-hub/services/cdb-jan-compare.mjs', import.meta.url), 'utf8');
    assert.ok(!/\b(insert\s+into|update\s+\w+(\.\w+)?\s+set|delete\s+from)\b/i.test(src), '比べる部品に書く SQL がある');
    const r = fs.readFileSync(new URL('../apps/product-hub/router.js', import.meta.url), 'utf8');
    assert.match(r, /router\.get\('\/api\/drafts\/:id\/jan-check'/);
    assert.match(r, /router\.get\('\/api\/board\/jan-check'/);
  });
} finally {
  server.close();
  try { await pg.close(); } catch { /* */ }
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows はハンドルが残ることがある */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
process.exit(process.exitCode || 0);
