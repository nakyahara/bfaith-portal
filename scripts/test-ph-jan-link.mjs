/**
 * test-ph-jan-link.mjs — product-hub の今ある JAN を商品マスタ (Company DB) の JAN に結ぶ (Company DB構想 21 §5 B2 の段 1・2)
 *
 * Company DB = PGlite (最新の migration まで)。product-hub = 一時の DATA_DIR の SQLite (本物の router)。
 * 固定する契約:
 *   M SQLite の migration: product_drafts・draft_sku_jans に jan_* の列 (空でよい・今ある行は空のまま)・記録の表・2 回流しても同じ
 *   P 計画 (「数える」): 同じ商品・同じ JAN・今も有効 で 1 つ = 結ぶ / 0 件 = 要確認 (理由: 別の JAN・JAN 無し・セット・商品コード無し) /
 *     まとまりは子の SKU ごと (JAN の行が無い子・まとまりに居ない子・ページ代表は対象外)・空 = 何もしない・数えるだけでは何も書かない
 *   A 「移す」: 数えたのと同じ数え方 (書いた数 = 数えた数)・外部 ID の行番号と値・だれがいつ・商品の履歴・
 *     二度押し = 409 (何も変えない)・同時に押す = 1 つだけ・途中で失敗 = 全部 戻る・数えた後に変わった = 409・
 *     Company DB を読めない = 何も書かない・JAN が空になった = 結びを外す・商品マスタで JAN が外れた = 要確認に変える
 *     商品マスタを読むのが混んでいる (B1 の同時 2 本) = 何も書かない・期限は要求の入口から (B1 と同じ)
 *   P3 対象を集めるのは B1 の一括の関数 (janUnitsForDrafts) = 1 件ずつと同じ単位 (大文字の子のコードも)・直し方は B1 の JAN_FIX と同じ言葉
 *   R1 (Codex #1707): 数えた後に商品コード / まとまりの所属が変わった = 取引の中の印と UPDATE の条件の両方で断る・成功の記録は更新と同じ取引・
 *     NULL の JAN の行も外せる・SKU の大文字小文字 (片方だけ = 同じ子・2 つ = 要確認)・計画の印は見せた全部・CSRF (Origin + JSON)・cdb_multiple・古い形の SQLite から ALTER
 *   R2 (Codex #1707): 重なる 2 行は JAN が空でも要確認 (片方が空・両方が空・前に結んだ空の行)・cdb_multiple を本物の道で (試験の DB で一意の索引を一時に外す)
 *   G 管理者だけ (画面・数える・移す)
 *   H 画面: 管理者の画面・担当者・工程の設定から入れる・画面の JS が読める
 *   B 楽天の出品は今の列 (jan_code) のまま = 移しても payload は同じ
 * 使い方: node scripts/test-ph-jan-link.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';
import Database from 'better-sqlite3';

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phjan-data-link-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;
delete process.env.COMPANY_DB_URL;
delete process.env.COMPANY_DB_WATCH_URL;
delete process.env.COMPANY_DB_SCREEN_READ_URL;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const LG = await import('../lib/master-legacy-gate.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const J = await import('../apps/product-hub/services/cdb-jan-compare.mjs');
const L = await import('../apps/product-hub/services/jan-link.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const ean = (d12) => { const s = String(d12); let t = 0; for (let i = 0; i < 12; i++) t += Number(s[i]) * (i % 2 ? 3 : 1); return s + ((10 - (t % 10)) % 10); };
const JAN = Object.fromEntries(['a1', 'a3', 'a3x', 'a4x', 's1', 'z1', 'r1', 'b1', 'b2', 'bx', 'gp'].map((k, i) => [k, ean(`4900000${String(200 + i).padStart(5, '0')}`)]));

// ─── Company DB ───
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
const plan0 = {
  skus: [sku('a001', '結べる'), sku('a003', '別の JAN'), sku('a004', 'JAN 無し'), sku('b-red', '色 赤'), sku('b-blu', '色 青'), sku('b-wht', '色 白'),
    sku('set001', 'セット', { kind: 'set', cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE' } })],
  variationGroups: [], setComponents: [{ parentCode: 'set001', childCode: 'a001', qty: 2, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_phjanl' },
};
const loadR = await runInitialLoad(db, plan0, { log: quiet, runId: 'load_phjanl', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(loadR.ok, true, loadR.error);
const pidOf = async (code) => (await pg.query('select product_id::text as id from core.skus where code = $1', [code])).rows[0].id;
async function addJan(code, jan) {
  return Number((await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
    values (1, 'product', $1, 'jan', 'jan', $2, 'manual', 'human', 'phjan-test') returning external_id_row::text as id`, [await pidOf(code), jan])).rows[0].id);
}
const rowA1 = await addJan('a001', JAN.a1);
await addJan('a003', JAN.a3x);
const rowB1 = await addJan('b-red', JAN.b1);
await addJan('b-blu', JAN.b2);

// 読み手 = 本物の SQL (PGlite)。遅らせる・読めないを切り替えられる
let slowMs = 0;
let readable = true;
J.__setCdbJanReader(async (items) => {
  if (slowMs) await new Promise((r) => setTimeout(r, slowMs));
  if (!readable) return { ok: false, reason: 'テストで読めない' };
  return { ok: true, rows: await J.queryCdbJans(pg, 1, items) };
});

// ─── product-hub ───
const ph = PHDB.getDB();
let mirrorPid = 900000;
const mirror = (code, repCode) => ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', ?, '2026-10-13T00:00:00Z')`).run(++mirrorPid, code, repCode);
for (const c of ['a001', 'a003', 'a004', 'set001', 'nope1', 'empty1']) mirror(c, null);
mirror('b-red', 'grpb'); mirror('b-blu', 'grpb'); mirror('b-wht', 'grpb');
const draft = (code, jan) => {
  ph.prepare('INSERT INTO product_drafts (ne_code, name, jan_code) VALUES (?, ?, ?)').run(code, `下書き ${code}`, jan);
  return ph.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get(code);
};
const dA1 = draft('a001', JAN.a1);      // 結べる
const dA3 = draft('a003', JAN.a3);      // 商品マスタには別の JAN = 要確認 (cdb_other_jan)
const dA4 = draft('a004', JAN.a4x);     // 商品マスタに JAN 無し = 要確認 (cdb_no_jan)
const dSet = draft('set001', JAN.s1);   // セット = 要確認 (cdb_set)
const dNo = draft('nope1', JAN.z1);     // 商品マスタに商品コード無し = 要確認 (cdb_no_sku)
const dEmpty = draft('empty1', null);   // JAN 空 = 何もしない
const dG = draft('grpb', JAN.gp);       // まとまり (ページ代表の JAN は対象外)
const sj = ph.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)');
sj.run(dG.id, 'b-red', JAN.b1);    // 結べる
sj.run(dG.id, 'b-blu', JAN.bx);    // 別の JAN = 要確認
sj.run(dG.id, 'gone-1', JAN.r1);   // まとまりに居ない子の行 = 対象外
// b-wht は JAN の行が無い = 対象外

const draftState = (id) => ph.prepare('SELECT jan_cdb_row, jan_cdb_value, jan_link_state, jan_link_reason, jan_linked_at, jan_linked_by FROM product_drafts WHERE id = ?').get(id);
const skuState = (id, code) => ph.prepare('SELECT jan_cdb_row, jan_cdb_value, jan_link_state, jan_link_reason, jan_linked_by FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').get(id, code);
const fingerprint = () => Object.fromEntries(['product_drafts', 'draft_sku_jans', 'draft_events'].map((t) => [t, crypto.createHash('sha256').update(JSON.stringify(ph.prepare(`SELECT * FROM ${t} ORDER BY rowid`).all())).digest('hex')]));

// ─── M migration ───
console.log('M SQLite の migration');
await ta('[M1] jan_* の列 (product_drafts・draft_sku_jans) と記録の表がある・今ある行は空・2 回流しても同じ', () => {
  for (const t of ['product_drafts', 'draft_sku_jans']) {
    const cols = ph.prepare(`PRAGMA table_info(${t})`).all();
    for (const c of ['jan_cdb_row', 'jan_cdb_value', 'jan_link_state', 'jan_link_reason', 'jan_linked_at', 'jan_linked_by']) {
      const col = cols.find((x) => x.name === c);
      assert.ok(col, `${t}.${c} が無い`);
      assert.equal(col.notnull, 0, `${t}.${c} が NOT NULL`);
    }
  }
  assert.deepEqual(draftState(dA1.id), { jan_cdb_row: null, jan_cdb_value: null, jan_link_state: null, jan_link_reason: null, jan_linked_at: null, jan_linked_by: null });
  assert.throws(() => ph.prepare(`UPDATE product_drafts SET jan_link_state = 'x' WHERE id = ?`).run(dA1.id), /CHECK/);
  const before = ph.prepare(`SELECT sql FROM sqlite_master WHERE name IN ('product_drafts', 'draft_sku_jans', 'ph_jan_link_runs') ORDER BY name`).all();
  PHDB.initProductHubDB();
  assert.deepEqual(ph.prepare(`SELECT sql FROM sqlite_master WHERE name IN ('product_drafts', 'draft_sku_jans', 'ph_jan_link_runs') ORDER BY name`).all(), before);
});

await ta('[M2] 古い形の SQLite (jan_* の列も記録の表も無い・行はある) から起動 = 列を足す・今ある行はそのまま (jan_* は空)', () => {
  const before = {
    drafts: ph.prepare('SELECT id, ne_code, name, jan_code FROM product_drafts ORDER BY id').all(),
    skus: ph.prepare('SELECT draft_id, sku_code, jan_code FROM draft_sku_jans ORDER BY draft_id, sku_code').all(),
  };
  assert.ok(before.drafts.length >= 7 && before.skus.length >= 3, '行が無い (試験の前提)');
  for (const t of ['product_drafts', 'draft_sku_jans']) for (const c of ['jan_cdb_row', 'jan_cdb_value', 'jan_link_state', 'jan_link_reason', 'jan_linked_at', 'jan_linked_by']) ph.exec(`ALTER TABLE ${t} DROP COLUMN ${c}`);
  ph.exec('DROP TABLE ph_jan_link_runs');
  assert.ok(!ph.prepare('PRAGMA table_info(product_drafts)').all().some((c) => c.name.startsWith('jan_cdb')), '古い形にできていない');
  PHDB._resetInitForTest();
  PHDB.getDB();
  for (const t of ['product_drafts', 'draft_sku_jans']) {
    const cols = ph.prepare(`PRAGMA table_info(${t})`).all().map((c) => c.name);
    for (const c of ['jan_cdb_row', 'jan_cdb_value', 'jan_link_state', 'jan_link_reason', 'jan_linked_at', 'jan_linked_by']) assert.ok(cols.includes(c), `${t}.${c} を足していない`);
  }
  assert.deepEqual(ph.prepare('SELECT id, ne_code, name, jan_code FROM product_drafts ORDER BY id').all(), before.drafts);
  assert.deepEqual(ph.prepare('SELECT draft_id, sku_code, jan_code FROM draft_sku_jans ORDER BY draft_id, sku_code').all(), before.skus);
  assert.equal(ph.prepare('SELECT COUNT(*) AS n FROM product_drafts WHERE jan_link_state IS NOT NULL OR jan_cdb_row IS NOT NULL').get().n, 0);
  assert.throws(() => ph.prepare(`UPDATE draft_sku_jans SET jan_link_state = 'x'`).run(), /CHECK/);
  assert.equal(ph.prepare(`SELECT COUNT(*) AS n FROM sqlite_master WHERE name = 'ph_jan_link_runs'`).get().n, 1);
});
await ta('[L7a] Company DB に同じ JAN が 2 つ以上 (cdb_multiple) = 選ばずに要確認', () => {
  const read = { ok: true, byPair: new Map([['a001\u0000111', { sku_id: '1', sku_kind: 'single', product_id: '10', jan_norm: '111', jan_holder: null,
    jans: [{ row: '5', value: '111', norm: '111' }, { row: '6', value: ' 111', norm: '111' }] }]]) };
  const t = L.decideJanLink({ code: 'a001', phJan: '111', dup: false }, read);
  assert.deepEqual(t, { state: 'needs_review', row: null, value: null, reason: 'cdb_multiple' });
  assert.ok(L.JAN_LINK_REASONS.cdb_multiple && L.JAN_LINK_FIX.cdb_multiple);
});

// ─── P 計画 ───
console.log('P 計画 (数える)');
const byKey = (plan) => Object.fromEntries(plan.items.map((i) => [`${i.draftId}:${i.skuKey || '-'}`, i]));
await ta('[P1] 結べる / 要確認 (理由ごと) / 空 / まとまりは子ごと・対象外 (JAN の行が無い子・居ない子・ページ代表)', async () => {
  const plan = await L.buildJanLinkPlan(ph);
  assert.equal(plan.ok, true, plan.reason);
  const k = byKey(plan);
  assert.deepEqual([k[`${dA1.id}:-`].action, k[`${dA1.id}:-`].target], ['link', { state: 'linked', row: rowA1, value: JAN.a1, reason: null }]);
  assert.deepEqual([k[`${dA3.id}:-`].action, k[`${dA3.id}:-`].target.reason], ['review', 'cdb_other_jan']);
  assert.equal(k[`${dA4.id}:-`].target.reason, 'cdb_no_jan');
  assert.equal(k[`${dSet.id}:-`].target.reason, 'cdb_set');
  assert.equal(k[`${dNo.id}:-`].target.reason, 'cdb_no_sku');
  assert.equal(k[`${dEmpty.id}:-`].action, 'empty');
  assert.deepEqual([k[`${dG.id}:b-red`].action, k[`${dG.id}:b-red`].target.row], ['link', rowB1]);
  assert.deepEqual([k[`${dG.id}:b-blu`].action, k[`${dG.id}:b-blu`].target.reason], ['review', 'cdb_other_jan']);
  assert.ok(!k[`${dG.id}:-`] && !k[`${dG.id}:gone-1`] && !k[`${dG.id}:b-wht`], '対象外の行が計画に入った');
  assert.deepEqual([plan.counts.link, plan.counts.review, plan.counts.clear, plan.counts.same, plan.counts.empty],
    [2, 5, 0, 0, 1]);
  assert.deepEqual(plan.counts.reviewByReason, { cdb_other_jan: 2, cdb_no_jan: 1, cdb_set: 1, cdb_no_sku: 1 });
  assert.match(plan.hash, /^[0-9a-f]{64}$/);
});
await ta('[P3] 対象を集めるのは B1 の一括の関数 (janUnitsForDrafts) = 1 件ずつ (janUnitsForDraft) と同じ単位 (大文字の子のコードも)', () => {
  // 写しの子のコードが大文字 (product-hub の SKU 別 JAN は小文字で持つ) のまとまりを一時に足す
  mirror('U-RED', 'grpu'); const pidA = mirrorPid; mirror('U-BLU', 'grpu'); const pidB = mirrorPid;
  const du = draft('grpu', null);
  sj.run(du.id, 'u-red', JAN.r1);
  try {
    const got = L.collectJanLinkUnits(ph).filter((u) => u.draftId === du.id).map((u) => [u.skuKey, u.code, u.phJan]);
    assert.deepEqual(got, [['u-red', 'U-RED', JAN.r1]], '大文字の子の JAN の行を見落とした');
    checkAll();
  } finally {
    ph.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ?').run(du.id);
    ph.prepare('DELETE FROM product_drafts WHERE id = ?').run(du.id);
    for (const pid of [pidA, pidB]) ph.prepare('DELETE FROM mirror_products WHERE product_id = ?').run(pid);
  }
});
function checkAll() {
  const units = L.collectJanLinkUnits(ph);
  const drafts = ph.prepare('SELECT * FROM product_drafts ORDER BY id').all();
  const expect = [];
  for (const d of drafts) {
    const { mode, units: us } = J.janUnitsForDraft(ph, d);
    if (mode === 'variation') {
      for (const u of us) { const r = ph.prepare('SELECT * FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').get(d.id, u.code.toLowerCase()); if (r) expect.push([d.id, r.sku_code, u.code, r.jan_code]); }
    } else if (us.length) expect.push([d.id, null, us[0].code, String(d.jan_code ?? '').trim()]);
  }
  assert.deepEqual(units.map((u) => [u.draftId, u.skuKey, u.code, u.phJan]), expect);
  const src = fs.readFileSync(new URL('../apps/product-hub/services/jan-link.mjs', import.meta.url), 'utf8');
  assert.ok(/janUnitsForDrafts\(db, drafts\)/.test(src) && !/janUnitsForDraft\(db, d\)/.test(src), '1 件ずつ判定している');
}
await ta('[P2] 同じ中身なら同じ印 (hash)・数えるだけでは何も書かない', async () => {
  const before = fingerprint();
  const [a, b] = [await L.buildJanLinkPlan(ph), await L.buildJanLinkPlan(ph)];
  assert.equal(a.hash, b.hash);
  assert.deepEqual(fingerprint(), before);
});

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
async function call(method, url, { body, session = 'admin' } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session, Origin: ORIGIN };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30000) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
const preview = (session) => call('POST', '/apps/product-hub/api/jan-link/preview', { body: {}, session });
const applyReq = (hash, session) => call('POST', '/apps/product-hub/api/jan-link/apply', { body: { confirm: true, hash }, session });
const runs = () => ph.prepare('SELECT kind, outcome, actor, plan_hash, counts_json, error FROM ph_jan_link_runs ORDER BY id').all();

try {
  console.log('G 管理者だけ');
  await ta('[G1] 管理者でない = 画面・数える・移す が 403 (何も書かない・記録も残さない)', async () => {
    const before = fingerprint();
    const n0 = runs().length;
    assert.equal((await call('GET', '/apps/product-hub/jan-link', { session: 'user' })).status, 403);
    assert.equal((await preview('user')).status, 403);
    assert.equal((await applyReq('none', 'user')).status, 403);
    assert.deepEqual(fingerprint(), before);
    assert.equal(runs().length, n0);
  });

  console.log('A 移す');
  let firstHash;
  let firstCounts;
  await ta('[A0] confirm が無い / hash の形が違う = 400 (何も書かない)', async () => {
    const before = fingerprint();
    assert.equal((await call('POST', '/apps/product-hub/api/jan-link/apply', { body: { hash: 'none' } })).status, 400);
    assert.equal((await applyReq('abc')).status, 400);
    assert.deepEqual(fingerprint(), before);
  });
  await ta('[A1] 数える = 書かない・記録 (だれ・印・数)', async () => {
    const before = fingerprint();
    const r = await preview();
    assert.equal(r.status, 200);
    assert.deepEqual([r.j.counts.link, r.j.counts.review], [2, 5]);
    assert.equal(r.j.review.length, 5);
    assert.ok(r.j.review.every((x) => x.reasonText && x.fixText), '理由か直し方が無い');
    // 直し方は B1 の印の直し方 (JAN_FIX) と同じ言葉
    assert.equal(r.j.review.find((x) => x.reason === 'cdb_other_jan').fixText, J.JAN_FIX.differs);
    assert.equal(r.j.review.find((x) => x.reason === 'cdb_set').fixText, J.JAN_FIX.not_in_cdb_set);
    firstHash = r.j.hash; firstCounts = r.j.counts;
    assert.deepEqual(fingerprint(), before);
    const last = runs().at(-1);
    assert.deepEqual([last.kind, last.outcome, last.actor, last.plan_hash], ['preview', 'ok', 'boss@test', firstHash]);
  });
  await ta('[A2] 移す = 数えたのと同じ数を書く・外部 ID の行番号と値・だれがいつ・商品の履歴', async () => {
    const r = await applyReq(firstHash);
    assert.equal(r.status, 200, JSON.stringify(r.j));
    assert.deepEqual(r.j.written, { link: firstCounts.link, review: firstCounts.review, clear: firstCounts.clear });
    const a = draftState(dA1.id);
    assert.deepEqual([a.jan_cdb_row, a.jan_cdb_value, a.jan_link_state, a.jan_link_reason, a.jan_linked_by], [rowA1, JAN.a1, 'linked', null, 'boss@test']);
    assert.match(a.jan_linked_at, /^\d{4}-\d{2}-\d{2}T/);
    assert.deepEqual([draftState(dA3.id).jan_link_state, draftState(dA3.id).jan_link_reason, draftState(dA3.id).jan_cdb_row], ['needs_review', 'cdb_other_jan', null]);
    assert.deepEqual([skuState(dG.id, 'b-red').jan_cdb_row, skuState(dG.id, 'b-red').jan_link_state], [rowB1, 'linked']);
    assert.equal(skuState(dG.id, 'b-blu').jan_link_state, 'needs_review');
    assert.equal(skuState(dG.id, 'gone-1').jan_link_state, null, '対象外の行を書いた');
    assert.equal(draftState(dG.id).jan_link_state, null, 'ページ代表の JAN を書いた');
    assert.equal(draftState(dEmpty.id).jan_link_state, null);
    // JAN そのもの (楽天に送る今の列) は変えない
    assert.equal(ph.prepare('SELECT jan_code FROM product_drafts WHERE id = ?').get(dA3.id).jan_code, JAN.a3);
    const ev = ph.prepare(`SELECT draft_id, actor, detail FROM draft_events WHERE event = 'jan_linked' ORDER BY draft_id`).all();
    assert.deepEqual(ev.map((e) => e.draft_id).sort((x, y) => x - y), [dA1.id, dA3.id, dA4.id, dSet.id, dNo.id, dG.id].sort((x, y) => x - y));
    assert.ok(ev.every((e) => e.actor === 'boss@test'));
    const last = runs().at(-1);
    assert.deepEqual([last.kind, last.outcome, last.actor], ['apply', 'ok', 'boss@test']);
    assert.deepEqual(JSON.parse(last.counts_json).written, r.j.written);
  });
  await ta('[A3] 二度押し (同じ印) = 409・何も変えない・数え直すと「もう済み」', async () => {
    const before = fingerprint();
    const r = await applyReq(firstHash);
    assert.equal(r.status, 409);
    assert.match(r.j.error, /数えた後に内容が変わりました/);
    assert.deepEqual(fingerprint(), before);
    assert.equal(runs().at(-1).outcome, 'refused');
    const p = await preview();
    assert.deepEqual([p.j.counts.link, p.j.counts.review, p.j.counts.same], [0, 0, 7]);
    assert.match(p.j.hash, /^[0-9a-f]{64}$/);
    // 書く物が無いときの「移す」= 何も書かずに ok
    const n = await applyReq(p.j.hash);
    assert.deepEqual([n.status, n.j.written], [200, { link: 0, review: 0, clear: 0 }]);
    assert.deepEqual(fingerprint(), before);
  });
  await ta('[A4] JAN を直した後 (数えた後) に移す = 409・書かない / 数え直すと直した分だけ書く (要確認 → 結べる・空 → 外す)', async () => {
    // 数える
    ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(JAN.a3x, dA3.id);   // 商品マスタと同じ JAN に直した
    const p = await preview();
    assert.deepEqual([p.j.counts.link, p.j.counts.review], [1, 0]);
    // 数えた後に別の人が JAN を空にした
    ph.prepare('UPDATE product_drafts SET jan_code = NULL WHERE id = ?').run(dA1.id);
    const before = fingerprint();
    const r = await applyReq(p.j.hash);
    assert.equal(r.status, 409);
    assert.deepEqual(fingerprint(), before);
    assert.ok(r.j.counts, '今の中身を返していない');
    const p2 = await preview();
    assert.deepEqual([p2.j.counts.link, p2.j.counts.clear], [1, 1]);
    const r2 = await applyReq(p2.j.hash);
    assert.deepEqual([r2.status, r2.j.written], [200, { link: 1, review: 0, clear: 1 }]);
    assert.equal(draftState(dA3.id).jan_link_state, 'linked');
    assert.deepEqual(draftState(dA1.id), { jan_cdb_row: null, jan_cdb_value: null, jan_link_state: null, jan_link_reason: null, jan_linked_at: null, jan_linked_by: null });
    ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(JAN.a1, dA1.id);
  });
  await ta('[A5] 商品マスタで JAN が外れた (valid_to) = 結んだ行は要確認に変わる', async () => {
    await pg.query(`update core.external_ids set valid_to = now() where external_id_row = $1`, [rowB1]);
    const p = await preview();
    const item = p.j.review.find((x) => x.code === 'b-red');
    assert.ok(item, JSON.stringify(p.j.review));
    assert.equal(item.reason, 'cdb_no_jan');
    const r = await applyReq(p.j.hash);
    assert.equal(r.status, 200);
    assert.deepEqual([skuState(dG.id, 'b-red').jan_link_state, skuState(dG.id, 'b-red').jan_cdb_row], ['needs_review', null]);
  });
  await ta('[A6] 同時に押す = 1 つだけが書く (もう 1 つは 409・ほかの人が移している途中)', async () => {
    // 結ぶ物を 1 つ作る (結びを消した = まだの形)
    ph.prepare('UPDATE product_drafts SET jan_cdb_row = NULL, jan_cdb_value = NULL, jan_link_state = NULL, jan_link_reason = NULL, jan_linked_at = NULL, jan_linked_by = NULL WHERE id = ?').run(dA1.id);
    const evBefore = ph.prepare(`SELECT COUNT(*) AS n FROM draft_events WHERE event = 'jan_linked' AND draft_id = ?`).get(dA1.id).n;
    const p = await preview();
    assert.equal(p.j.counts.link, 1);
    slowMs = 300;
    let rs;
    try { rs = await Promise.all([applyReq(p.j.hash), applyReq(p.j.hash)]); } finally { slowMs = 0; }
    assert.deepEqual(rs.map((r) => r.status).sort(), [200, 409]);
    assert.ok(rs.some((r) => r.status === 409 && /ほかの人が移している途中/.test(r.j.error)));
    assert.equal(ph.prepare(`SELECT COUNT(*) AS n FROM draft_events WHERE event = 'jan_linked' AND draft_id = ?`).get(dA1.id).n, evBefore + 1, '2 回書いた');
    assert.equal(draftState(dA1.id).jan_link_state, 'linked');
  });
  await ta('[A7] 途中で失敗 = 全部 戻る (1 件目を書いた後に失敗しても、1 件も残らない)', async () => {
    ph.prepare('UPDATE product_drafts SET jan_code = NULL WHERE id = ?').run(dA4.id);   // dA4 の JAN を空に = 外す
    ph.prepare('UPDATE draft_sku_jans SET jan_code = ? WHERE draft_id = ? AND sku_code = ?').run(JAN.b2, dG.id, 'b-blu');   // 結べるに
    const plan = await L.buildJanLinkPlan(ph);
    assert.deepEqual(plan.items.filter((i) => ['link', 'review', 'clear'].includes(i.action)).map((i) => i.action).sort(), ['clear', 'link']);
    const before = fingerprint();
    assert.throws(() => L.applyJanLinkPlan(ph, plan, { actor: 'boss@test', failAfter: 1 }), /途中で失敗/);
    assert.deepEqual(fingerprint(), before, '途中で失敗したのに書いた行が残った');
    // 取引の中で今の値が数えたときと違う = 何も書かない
    ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(JAN.a4x, dA4.id);
    const before2 = fingerprint();
    assert.throws(() => L.applyJanLinkPlan(ph, plan, { actor: 'boss@test' }), L.JanLinkConflict);
    assert.deepEqual(fingerprint(), before2);
    // 書かない行 (もう済み) でも、数えた後に変わっていれば取引の中で気づいて何も書かない (数えた中身と今がずれたまま書かない)
    const plan2 = await L.buildJanLinkPlan(ph);
    assert.ok(plan2.items.some((i) => i.action !== 'same' && i.action !== 'empty'), '書く物が無い (試験の前提)');
    const keep = ph.prepare('SELECT jan_code FROM product_drafts WHERE id = ?').get(dA1.id).jan_code;
    ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(JAN.a3, dA1.id);
    const before3 = fingerprint();
    try {
      assert.throws(() => L.applyJanLinkPlan(ph, plan2, { actor: 'boss@test' }), L.JanLinkConflict);
      assert.deepEqual(fingerprint(), before3);
    } finally { ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(keep, dA1.id); }
    // だれが押したかが無い = 書かない
    assert.throws(() => L.applyJanLinkPlan(ph, plan, { actor: '' }), /だれが押したか/);
  });
  await ta('[A8] 商品マスタを読めない = 数える・移す とも 503・何も書かない・記録は失敗', async () => {
    const p = await preview();
    readable = false;
    try {
      const before = fingerprint();
      const a = await preview();
      assert.deepEqual([a.status, /読めないので数えられません/.test(a.j.error)], [503, true]);
      const b = await applyReq(p.j.hash);
      assert.deepEqual([b.status, /読めないので移せません/.test(b.j.error)], [503, true]);
      assert.deepEqual(fingerprint(), before);
      assert.deepEqual(runs().slice(-2).map((r) => [r.kind, r.outcome]), [['preview', 'failed'], ['apply', 'failed']]);
    } finally { readable = true; }
  });
  await ta('[A9] 「数える」と「移す」は同じ数え方: 数えた結べる / 要確認 / 外す の数 = 書いた数 (いくつかの形で)', async () => {
    for (const change of [
      () => ph.prepare('UPDATE draft_sku_jans SET jan_code = ? WHERE draft_id = ? AND sku_code = ?').run(JAN.bx, dG.id, 'b-blu'),
      () => { ph.prepare('UPDATE product_drafts SET jan_code = NULL WHERE id = ?').run(dSet.id); ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(JAN.a1, dA4.id); },
    ]) {
      change();
      const p = await preview();
      const r = await applyReq(p.j.hash);
      assert.equal(r.status, 200, JSON.stringify(r.j));
      assert.deepEqual(r.j.written, { link: p.j.counts.link, review: p.j.counts.review, clear: p.j.counts.clear });
      const again = await preview();
      assert.equal(again.j.counts.link + again.j.counts.review + again.j.counts.clear, 0, '書いた後に数え直しても書く物が残った');
    }
  });

  await ta('[R1-H1] 🚨 数えた後 (取引の前) に商品コードが変わった = 取引の中で気づいて何も書かない・印を素通りしても UPDATE が商品コードで断る', async () => {
    ph.prepare('UPDATE product_drafts SET jan_cdb_row = NULL, jan_cdb_value = NULL, jan_link_state = NULL, jan_link_reason = NULL, jan_linked_at = NULL, jan_linked_by = NULL WHERE id = ?').run(dA1.id);
    const plan = await L.buildJanLinkPlan(ph);
    assert.ok(plan.items.some((i) => i.draftId === dA1.id && i.action === 'link'), '試験の前提 (a001 を結ぶ計画)');
    ph.prepare(`UPDATE product_drafts SET ne_code = 'a001-new' WHERE id = ?`).run(dA1.id);
    try {
      const before = fingerprint();
      assert.throws(() => L.applyJanLinkPlan(ph, plan, { actor: 'boss@test' }), (e) => e instanceof L.JanLinkConflict && /数えた後に product-hub の JAN が変わりました/.test(e.message), '取引の中の印で気づかない');
      assert.deepEqual(fingerprint(), before);
      // 取引の中の印を素通りしたとしても (印だけ今に合わせる)、UPDATE の条件が商品コードで断る
      const forged = { ...plan, snapshot: (await L.buildJanLinkPlan(ph)).snapshot };
      assert.throws(() => L.applyJanLinkPlan(ph, forged, { actor: 'boss@test' }), (e) => e instanceof L.JanLinkConflict && /書けなかった行があります/.test(e.message), 'UPDATE が商品コードで断らない');
      assert.deepEqual(fingerprint(), before);
      // まとまりの所属が変わった (子が 1 つ増えた) も印が変わる
      const g0 = await L.buildJanLinkPlan(ph);
      mirror('b-new', 'grpb'); const pidNew = mirrorPid;
      try { assert.notEqual((await L.buildJanLinkPlan(ph)).snapshot, g0.snapshot, 'まとまりの所属の変化に気づかない'); }
      finally { ph.prepare('DELETE FROM mirror_products WHERE product_id = ?').run(pidNew); }
    } finally { ph.prepare(`UPDATE product_drafts SET ne_code = 'a001' WHERE id = ?`).run(dA1.id); }
  });
  await ta('[R1-H2] 🚨 成功の記録は JAN の更新と同じ取引: 記録に失敗 = 更新も戻る (500「全部 元に戻しました」が本当)・成功なら更新と記録がそろう', async () => {
    const p = await preview();
    assert.ok(p.j.counts.link + p.j.counts.review + p.j.counts.clear >= 1, '試験の前提 (書く物がある)');
    const before = fingerprint();
    const runsBefore = runs().length;
    ph.exec('ALTER TABLE ph_jan_link_runs RENAME TO ph_jan_link_runs_hidden');
    let r;
    try { r = await applyReq(p.j.hash); } finally { ph.exec('ALTER TABLE ph_jan_link_runs_hidden RENAME TO ph_jan_link_runs'); }
    assert.equal(r.status, 500);
    assert.match(r.j.error, /全部 元に戻しました/);
    assert.deepEqual(fingerprint(), before, '記録に失敗したのに JAN の更新が残った');
    assert.equal(runs().length, runsBefore);
    // 成功: 更新と記録がそろう
    const ok = await applyReq((await preview()).j.hash);
    assert.equal(ok.status, 200);
    const last = runs().at(-1);
    assert.deepEqual([last.kind, last.outcome, JSON.parse(last.counts_json).written], ['apply', 'ok', ok.j.written]);
    assert.equal(draftState(dA1.id).jan_link_state, 'linked');
  });
  await ta('[R1-M4] SKU コードの大文字小文字: 片方だけ大文字 = 同じ子として結ぶ / 大文字小文字だけ違う行が 2 つ = 選ばずに両方 要確認', async () => {
    // b-red の行を大文字に (同じ子として扱う)
    ph.prepare(`UPDATE draft_sku_jans SET sku_code = 'B-RED', jan_cdb_row = NULL, jan_cdb_value = NULL, jan_link_state = NULL, jan_link_reason = NULL WHERE draft_id = ? AND sku_code = 'b-red'`).run(dG.id);
    await pg.query('update core.external_ids set valid_to = null where external_id_row = $1', [rowB1]).catch(() => {});
    const added = [];
    try {
      const plan = await L.buildJanLinkPlan(ph);
      const red = plan.items.find((i) => i.draftId === dG.id && i.code === 'b-red');
      assert.ok(red, '大文字の行を見落とした');
      assert.equal(red.skuKey, 'B-RED');
      // 大文字小文字だけ違う行が 2 つ (b-blu と B-BLU)
      ph.prepare(`INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, 'B-BLU', ?)`).run(dG.id, JAN.b2); added.push('B-BLU');
      const plan2 = await L.buildJanLinkPlan(ph);
      const blus = plan2.items.filter((i) => i.draftId === dG.id && i.code === 'b-blu');
      assert.deepEqual(blus.map((i) => [i.skuKey, i.target.state, i.target.reason]).sort(), [['B-BLU', 'needs_review', 'ph_sku_dup'], ['b-blu', 'needs_review', 'ph_sku_dup']]);
      const r = await applyReq((await preview()).j.hash);
      assert.equal(r.status, 200, JSON.stringify(r.j));
      assert.deepEqual([skuState(dG.id, 'B-BLU').jan_link_reason, skuState(dG.id, 'b-blu').jan_link_reason], ['ph_sku_dup', 'ph_sku_dup']);
    } finally {
      for (const k of added) ph.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').run(dG.id, k);
      ph.prepare(`UPDATE draft_sku_jans SET sku_code = 'b-red' WHERE draft_id = ? AND sku_code = 'B-RED'`).run(dG.id);
    }
  });
  await ta('[R1-M5] 計画の印は見せた計画の全部: 書かない単位 (JAN が空) が増えただけでも印が変わる = 移すは 409', async () => {
    const p = await preview();
    const d = draft('empty2', null);
    try {
      const r = await applyReq(p.j.hash);
      assert.equal(r.status, 409, '数えた後に件数が変わったのに書いた');
      assert.equal(r.j.counts.empty, p.j.counts.empty + 1);
    } finally { ph.prepare('DELETE FROM product_drafts WHERE id = ?').run(d.id); }
  });
  await ta('[R1-M6] CSRF: Origin が無い / 違う = 403・JSON でない = 415 (数える・移す とも・何も書かない・記録も書かない)', async () => {
    const before = fingerprint();
    const n0 = runs().length;
    const raw = (url, headers, body) => fetch(ORIGIN + url, { method: 'POST', headers: { 'x-test-session': 'admin', ...headers }, body, signal: AbortSignal.timeout(30000) }).then((r) => r.status);
    for (const url of ['/apps/product-hub/api/jan-link/preview', '/apps/product-hub/api/jan-link/apply']) {
      assert.equal(await raw(url, { 'Content-Type': 'application/json' }, '{}'), 403, `${url} Origin なし`);
      assert.equal(await raw(url, { 'Content-Type': 'application/json', Origin: 'https://evil.example' }, '{}'), 403, `${url} 違う Origin`);
      assert.equal(await raw(url, { 'Content-Type': 'text/plain', Origin: ORIGIN }, 'confirm=true'), 415, `${url} JSON でない`);
    }
    assert.deepEqual(fingerprint(), before);
    assert.equal(runs().length, n0);
  });
  await ta('[R1-M3] SKU 別の JAN が NULL の行 (古い形の表) も「外す」で書ける (TRIM(COALESCE))', () => {
    // NOT NULL の無い古い形の表を持つ別の SQLite (メモリ) で、計画を作って書く (Company DB は読まない = JAN が空の単位だけ)
    const mem = new Database(':memory:');
    mem.exec(`CREATE TABLE product_drafts (id INTEGER PRIMARY KEY, ne_code TEXT, name TEXT, jan_code TEXT, jan_cdb_row INTEGER, jan_cdb_value TEXT, jan_link_state TEXT, jan_link_reason TEXT, jan_linked_at TEXT, jan_linked_by TEXT);
      CREATE TABLE draft_sku_jans (draft_id INTEGER, sku_code TEXT, jan_code TEXT, jan_cdb_row INTEGER, jan_cdb_value TEXT, jan_link_state TEXT, jan_link_reason TEXT, jan_linked_at TEXT, jan_linked_by TEXT);
      CREATE TABLE mirror_products (product_id INTEGER, 商品コード TEXT, 代表商品コード TEXT);
      CREATE TABLE draft_variation_exclusions (draft_id INTEGER, ne_code TEXT);
      CREATE TABLE draft_events (id INTEGER PRIMARY KEY, draft_id INTEGER, event TEXT, detail TEXT, actor TEXT);
      INSERT INTO product_drafts (id, ne_code, name) VALUES (1, 'gx', 'まとまり');
      INSERT INTO mirror_products VALUES (1, 'x1', 'gx'), (2, 'x2', 'gx');
      INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code, jan_cdb_row, jan_cdb_value, jan_link_state) VALUES (1, 'x1', NULL, 9, '4900000000000', 'linked');`);
    return L.buildJanLinkPlan(mem).then((plan) => {
      assert.equal(plan.ok, true, plan.reason);
      assert.deepEqual(plan.items.map((i) => [i.code, i.phJan, i.action]), [['x1', '', 'clear']]);
      const r = L.applyJanLinkPlan(mem, plan, { actor: 'boss@test' });
      assert.deepEqual(r.written, { link: 0, review: 0, clear: 1 });
      assert.deepEqual(mem.prepare('SELECT jan_cdb_row, jan_link_state FROM draft_sku_jans').get(), { jan_cdb_row: null, jan_link_state: null });
      mem.close();
    });
  });
  await ta('[A10] 商品マスタを読むのが混んでいる (B1 の同時 2 本の上限) = 数える・移す とも 503「混んでいる」・何も書かない', async () => {
    const p = await preview();
    const gates = [];
    J.__setCdbJanReader(() => new Promise((resolve) => gates.push(() => resolve({ ok: true, rows: [] }))));
    const hold = [J.readCdbJans([{ code: 'a001', jan: '1' }]), J.readCdbJans([{ code: 'a001', jan: '2' }])];
    try {
      await new Promise((r) => setTimeout(r, 10));
      assert.equal(J.__janReadInFlight(), 2);
      const before = fingerprint();
      const a = await preview();
      assert.deepEqual([a.status, /混んでいる/.test(a.j.error)], [503, true], JSON.stringify(a.j));
      const b = await applyReq(p.j.hash);
      assert.deepEqual([b.status, /混んでいる/.test(b.j.error)], [503, true], JSON.stringify(b.j));
      assert.deepEqual(fingerprint(), before);
    } finally {
      gates.splice(0).forEach((g) => g());
      await Promise.all(hold);
      J.__setCdbJanReader(async (items) => {
        if (slowMs) await new Promise((r) => setTimeout(r, slowMs));
        if (!readable) return { ok: false, reason: 'テストで読めない' };
        return { ok: true, rows: await J.queryCdbJans(pg, 1, items) };
      });
    }
  });
  await ta('[A11] 期限は要求の入口から (数える・移す とも入口で決めて計画まで渡す)', async () => {
    const src = fs.readFileSync(new URL('../apps/product-hub/router.js', import.meta.url), 'utf8');
    for (const route of ["router.post('/api/jan-link/preview', janLinkCsrfGuard, async (req, res) => {", "router.post('/api/jan-link/apply', janLinkCsrfGuard, async (req, res) => {"]) {
      const at = src.indexOf(route);
      assert.ok(at >= 0, route);
      assert.match(src.slice(at + route.length, at + route.length + 200), /^\s*const deadlineAt = Date\.now\(\) \+ JAN_READ_DEADLINE_MS;/, `${route} の入口で期限を決めていない`);
    }
    assert.equal((src.match(/buildJanLinkPlan\(db, \{ deadlineAt \}\)/g) || []).length, 2, '計画に期限を渡していない');
    // 期限が過ぎていれば読みに行かない = 計画なし (何も書かない)
    const plan = await L.buildJanLinkPlan(ph, { deadlineAt: Date.now() - 1 });
    assert.deepEqual([plan.ok, /期限/.test(plan.reason)], [false, true]);
  });

  await ta('[R2-M1] 大文字小文字だけ違う SKU の 2 行は、JAN が空でも要確認 (片方が空・両方が空・前に結んだ空の行 = 外すでなく要確認)', async () => {
    const keep = ph.prepare(`SELECT * FROM draft_sku_jans WHERE draft_id = ? AND sku_code = 'b-blu'`).get(dG.id);
    const reset = () => {
      ph.prepare(`DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code IN ('b-blu', 'B-BLU')`).run(dG.id);
      ph.prepare(`INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code, jan_cdb_row, jan_cdb_value, jan_link_state, jan_link_reason, jan_linked_at, jan_linked_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(dG.id, keep.sku_code, keep.jan_code, keep.jan_cdb_row, keep.jan_cdb_value, keep.jan_link_state, keep.jan_link_reason, keep.jan_linked_at, keep.jan_linked_by);
    };
    const blus = (plan) => plan.items.filter((i) => i.draftId === dG.id && i.code === 'b-blu').map((i) => [i.skuKey, i.phJan, i.action, i.target.state, i.target.reason]).sort();
    try {
      // 片方が空
      ph.prepare(`DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code IN ('b-blu', 'B-BLU')`).run(dG.id);
      ph.prepare(`INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, 'b-blu', ?), (?, 'B-BLU', '')`).run(dG.id, JAN.b2, dG.id);
      assert.deepEqual(blus(await L.buildJanLinkPlan(ph)), [['B-BLU', '', 'review', 'needs_review', 'ph_sku_dup'], ['b-blu', JAN.b2, 'review', 'needs_review', 'ph_sku_dup']]);
      // 両方が空 (同じ空の JAN は 1 つのページに 1 つ = 片方は空白だけ)
      ph.prepare(`UPDATE draft_sku_jans SET jan_code = ' ' WHERE draft_id = ? AND sku_code = 'b-blu'`).run(dG.id);
      assert.deepEqual(blus(await L.buildJanLinkPlan(ph)), [['B-BLU', '', 'review', 'needs_review', 'ph_sku_dup'], ['b-blu', '', 'review', 'needs_review', 'ph_sku_dup']]);
      // 前に結んだ空の行 = 結びを外すのでなく要確認にする
      ph.prepare(`UPDATE draft_sku_jans SET jan_cdb_row = 77, jan_cdb_value = ?, jan_link_state = 'linked' WHERE draft_id = ? AND sku_code = 'b-blu'`).run(JAN.b2, dG.id);
      const plan = await L.buildJanLinkPlan(ph);
      assert.deepEqual(blus(plan).map((x) => x.slice(0, 3)), [['B-BLU', '', 'review'], ['b-blu', '', 'review']]);
      assert.ok(!plan.items.some((i) => i.draftId === dG.id && i.code === 'b-blu' && i.action === 'clear'), '重なる空の行の結びを外した');
      const r = await applyReq((await preview()).j.hash);
      assert.equal(r.status, 200, JSON.stringify(r.j));
      assert.deepEqual([skuState(dG.id, 'b-blu').jan_link_state, skuState(dG.id, 'b-blu').jan_link_reason, skuState(dG.id, 'b-blu').jan_cdb_row], ['needs_review', 'ph_sku_dup', null]);
      assert.deepEqual([skuState(dG.id, 'B-BLU').jan_link_state, skuState(dG.id, 'B-BLU').jan_link_reason], ['needs_review', 'ph_sku_dup']);
      // もう要確認 = 書き直さない (same)
      assert.deepEqual(blus(await L.buildJanLinkPlan(ph)).map((x) => x[2]), ['same', 'same']);
    } finally { reset(); }
  });
  await ta('[R2-L2] Company DB に同じ JAN が 2 行 (一意の索引を試験の DB で一時に外して作る) = 本物の道 (queryCdbJans → 計画 → 数える / 移す) で要確認 cdb_multiple', async () => {
    const keepJan = ph.prepare('SELECT jan_code FROM product_drafts WHERE id = ?').get(dA4.id).jan_code;
    ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(JAN.a4x, dA4.id);
    await pg.query('drop index core.ux_external_ids_active');
    const rows = [];
    try {
      rows.push(await addJan('a004', JAN.a4x), await addJan('a004', JAN.a4x));
      const raw = await J.queryCdbJans(pg, 1, [{ code: 'a004', jan: JAN.a4x }]);
      assert.equal(raw[0].jans.length, 2, '重なる JAN を作れていない (試験の前提)');
      const p = await preview();
      const item = p.j.review.find((x) => x.code === 'a004');
      assert.ok(item, JSON.stringify(p.j.review));
      assert.deepEqual([item.reason, item.fixText], ['cdb_multiple', L.JAN_LINK_FIX.cdb_multiple]);
      const r = await applyReq(p.j.hash);
      assert.equal(r.status, 200, JSON.stringify(r.j));
      assert.deepEqual([draftState(dA4.id).jan_link_state, draftState(dA4.id).jan_link_reason, draftState(dA4.id).jan_cdb_row], ['needs_review', 'cdb_multiple', null]);
    } finally {
      // 重なる行は外し (JAN の行は消せない = valid_to)、一意の索引を戻す
      for (const id of rows) await pg.query('update core.external_ids set valid_to = now() where external_id_row = $1', [id]);
      await pg.query('create unique index ux_external_ids_active on core.external_ids (system, id_kind, external_norm) where valid_to is null');
      ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run(keepJan, dA4.id);
    }
  });

  console.log('H 画面');
  await ta('[H1] 管理者の画面 (つかいかた・数える・移す・記録)・担当者・工程の設定から入れる・画面の JS が読める', async () => {
    const r = await call('GET', '/apps/product-hub/jan-link');
    assert.equal(r.status, 200, r.text.slice(0, 300));
    for (const s of ['これは何のための作業か', 'id="jl-preview"', 'id="jl-apply"', 'id="jl-runs"', 'boss@test']) assert.ok(r.text.includes(s), s);
    const all = [...r.text.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
    for (const m of all) { if (/application\/json/.test(m[1])) JSON.parse(m[2]); else if (!/\bsrc=/.test(m[1])) new vm.Script(m[2]); }
    const s = await call('GET', '/apps/product-hub/staff');
    assert.ok(s.text.includes('href="/apps/product-hub/jan-link"'));
    const su = await call('GET', '/apps/product-hub/staff', { session: 'user' });
    assert.ok(!su.text.includes('href="/apps/product-hub/jan-link"') && su.text.includes('使えるのは管理者だけです'));
    const u = await call('GET', '/apps/product-hub/jan-link', { session: 'user' });
    assert.ok(u.text.includes('この画面を使えるのは管理者だけです') && !u.text.includes('id="jl-preview"'));
  });

  console.log('B 楽天の出品は今の列のまま');
  await ta('[B1] 移した後も楽天の payload は今の jan_code (段 3 まで切り替えない)', async () => {
    const r = await call('GET', `/apps/product-hub/api/drafts/${dA3.id}/rakuten/preview`);
    const t = JSON.stringify(r.j);
    assert.ok(!t.includes('jan_cdb') && !t.includes('jan_link'), '印の列を出品に使っている');
    const src = fs.readFileSync(new URL('../apps/product-hub/services/rakuten-listing.js', import.meta.url), 'utf8');
    assert.ok(!/jan_cdb_|jan_link_/.test(src), 'rakuten-listing.js が印の列を読んでいる (段 3 の前)');
  });
} finally {
  server.close();
  try { await pg.close(); } catch { /* */ }
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows はハンドルが残ることがある */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
process.exit(process.exitCode || 0);
