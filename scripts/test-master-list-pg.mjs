/**
 * test-master-list-pg.mjs — 商品・セットの一覧の並び (10/8 PR1) を本物の PostgreSQL で: 本番と同じくらいの数 (7,388 件) で全部の列の並びと速さ
 *
 *   1 並べられる全部の列 × 昇順 / 降順 (在庫はロジザードの写しの見本で): 絞った全件の並び = 一覧に出す値で手で並べた並び (空は最後・同じ値はコード順)・
 *     1 ページ目 (100 件) も同じ。読むのは画面のロール master_edit (本番の画面と同じ権限)
 *   2 速さ: 1 ページ目を読む時間 (5 回の中央値) を、今のコード (origin/master の read.mjs の写し = 引数で渡す) のコード順・登録日の新しい順・利益の少ない順と比べる。
 *     どの並びも 1 回 1 秒より短い (本番の 20 秒の statement_timeout から遠い)・コード順は今と大きく変わらない
 *   3 EXPLAIN: 並びの読み (① の SQL) は全部の行を 1 回読んで並べる形 (索引が要らない = SKU の数 7,400 件は並べても数 ms)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:5xxxx/postgres node scripts/test-master-list-pg.mjs [前の read.mjs の写し (比べる・無ければ比べない)]
 *   (この PC では C:/tmp/pg-embed の run-*.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)。localhost 以外の URL は拒む (本番を渡さない)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
const W2 = await import('./fixtures/master-widen-pr1.mjs');

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (本物の PostgreSQL の並びの試験は飛ばす。PGlite の試験は scripts/test-master-list.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const R = await import('../apps/master-edit/read.mjs');
const LC = await import('../apps/master-edit/list-columns.mjs');
const { expectedOrder, distinctValues, nullCount } = await import('./fixtures/master-list-order.mjs');
const OLD_PATH = process.argv[2] || process.env.MASTER_LIST_OLD_READ || '';
const OLD = OLD_PATH ? await import(pathToFileURL(path.resolve(OLD_PATH)).href) : null;

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const NOW = new Date('2030-01-10T03:00:00Z');

const PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const dbName = `cdb_mlist_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const O = await openPgClient(u.toString());
let E = null;
try {
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: { master_edit: PW, master_gate_render: PW + 'r', master_gate_minipc: PW + 'm', master_ops: PW + 'o', master_observer: PW + 'v' } });
  await W2.useReal0058(O);
  // ── 本番と同じくらいの数: 単品 5,063・セット 2,236・例外 89 = 7,388 件・中止 1,569・仕入先 120 ──
  const rnd = (() => { let s = 20261008; return () => { s = (s * 1103515245 + 12345) % 2147483648; return s / 2147483648; }; })();
  const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
  const WORDS = ['はちみつ', 'ハチミツ', 'エプロン', '石鹸', 'タオル', 'abc', 'ABC', 'ｶﾀｶﾅ', '国産', '有機', 'クロレラ', 'ごぼう茶', 'ギフト', '保存袋', 'zeta', '0番'];
  const skus = []; const comps = []; const supSkus = []; const prim = [];
  const suppliers = Array.from({ length: 120 }, (_, i) => ({ code: String(i + 1).padStart(4, '0'), name: `仕入先 ${i + 1}` }));
  const single = (i) => `s${String(i).padStart(5, '0')}`;
  for (let i = 0; i < 5063; i++) {
    const code = single(i);
    const noCost = rnd() < 0.02; const noTax = rnd() < 0.01; const noPrice = rnd() < 0.02;
    skus.push({ code, name: `${pick(WORDS)} ${pick(WORDS)} ${Math.floor(rnd() * 500)}`, kind: 'single', taxRate: noTax ? null : pick([0.08, 0.1]), taxClass: null,
      handling: rnd() < 0.2 ? 'discontinued' : 'active', salesClass: rnd() < 0.01 ? null : 1 + Math.floor(rnd() * 4),
      cost: noCost ? null : { jpy: 50 + Math.floor(rnd() * 3000), source: 'ne', status: 'COMPLETE' },
      standardPriceJpy: noPrice ? null : 100 * (5 + Math.floor(rnd() * 60)), shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: rnd() < 0.05 ? null : pick([210, 380, 520]), reorderMonths: 2,
      ...(rnd() < 0.9 ? { registeredOn: `20${String(18 + Math.floor(rnd() * 9)).padStart(2, '0')}-${String(1 + Math.floor(rnd() * 12)).padStart(2, '0')}-${String(1 + Math.floor(rnd() * 28)).padStart(2, '0')}` } : {}) });
    if (rnd() < 0.95) { const sc = pick(suppliers).code; supSkus.push({ supplierCode: sc, skuCode: code }); prim.push({ skuCode: code, supplierCode: sc }); }
  }
  for (let i = 0; i < 2236; i++) {
    const code = `t${String(i).padStart(5, '0')}`;
    skus.push({ code, name: `${pick(WORDS)} セット ${i}`, kind: 'set', taxRate: pick([0.08, 0.1]), taxClass: null, handling: rnd() < 0.15 ? 'discontinued' : 'active', salesClass: null,
      cost: rnd() < 0.03 ? null : { jpy: 300 + Math.floor(rnd() * 5000), source: 'set_calc', status: 'COMPLETE' }, standardPriceJpy: 100 * (10 + Math.floor(rnd() * 90)), shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
    const n = rnd() < 0.02 ? 0 : 1 + Math.floor(rnd() * 3); const used = new Set();   // 2% は構成が無い (作れる数が空 = 在庫の並びで最後)
    for (let k = 0; k < n; k++) { const c = single(Math.floor(rnd() * 5063)); if (used.has(c)) continue; used.add(c); comps.push({ parentCode: code, childCode: c, qty: 1 + Math.floor(rnd() * 3), source: 'ne' }); }
  }
  for (let i = 0; i < 89; i++) {
    skus.push({ code: `x${String(i).padStart(4, '0')}`, name: `例外 ${i}`, kind: 'exception', taxRate: 0.1, taxClass: null, handling: 'active', salesClass: null,
      cost: { jpy: 500 + i, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: rnd() < 0.3 ? null : 2000 + i, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
  }
  const t0 = Date.now();
  const r0 = await runInitialLoad(dbO, { skus, variationGroups: [], setComponents: comps, listings: [], observations: [], physicals: [], compliance: [], workers: [], suppliers, supplierSkus: supSkus, primarySuppliers: prim,
    reorder: { available: true, runId: 'pml_mlist' } }, { log: () => {}, runId: 'load_mlist', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  // 原価の履歴 (本番は 73,770 行 = SKU ごとに 10 くらい)。今の行より前の 9 か月分を足す (並びの読みが原価の表を全部読む形でも速いか)
  const minFrom = (await O.query('select min(valid_from)::text as d from core.sku_costs')).rows[0].d;
  let hist = 0;
  try {
    const r = await O.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to, created_by_type)
      select 1, c.sku_id, c.cost_jpy + k, c.cost_source, c.cost_status, ($1::date - (k + 1) * 30), ($1::date - k * 30 - 1), 'system'
        from core.sku_costs c cross join generate_series(0, 8) k where c.valid_to is null`, [minFrom]);
    hist = r.rowCount;
  } catch (e) { console.log(`   (原価の履歴を足せなかった = 1 行ずつのまま: ${e.message})`); }
  await O.query('analyze');
  const nSku = Number((await O.query('select count(*)::int as n from core.skus')).rows[0].n);
  const nCost = Number((await O.query('select count(*)::int as n from core.sku_costs')).rows[0].n);
  console.log(`   見本: SKU ${nSku.toLocaleString()} 件・原価の行 ${nCost.toLocaleString()} (履歴 +${hist.toLocaleString()})・作る ${Date.now() - t0} ms`);
  assert.equal(nSku, 7388);
  // 画面のロールで読む (本番の画面と同じ権限・設定)
  const eu = new URL(u.toString()); eu.username = 'master_edit'; eu.password = PW;
  E = await openPgClient(eu.toString());
  await E.query(`set statement_timeout = '20s'`);
  const dbE = pgAdapter(E);
  // ロジザードの写しの見本 (単品の 70% に在庫・例外にも)
  const stockMap = new Map();
  for (const s of skus) if (s.kind !== 'set' && rnd() < 0.7) stockMap.set(s.code, Math.floor(rnd() * 200));
  const extras = { stock: { ok: true, asOf: NOW.toISOString(), stale: false, map: stockMap } };
  const all = (await R.listSkus(dbE, {}, { now: NOW, extras, mode: 'all' })).rows;
  assert.equal(all.length, 7388);

  await ta('[1] 7,388 件で 並べられる全部の列 × 昇順 / 降順 = 一覧に出す値で並べた並び (全部のコード・1 ページ目)', async () => {
    for (const col of LC.SORTABLE) {
      assert.ok(distinctValues(all, col) >= 2, `${col}: 違う値が 2 つ以上`);
      if (!['code', 'name', 'kind', 'state'].includes(col)) assert.ok(nullCount(all, col) >= 1, `${col}: 空の行がある`);
      for (const dir of ['asc', 'desc']) {
        const f = { sort: col === 'code' ? '' : col, dir: dir === 'desc' ? 'desc' : '' };
        const want = expectedOrder(all, col, dir);
        const codes = await R.listSkus(dbE, f, { now: NOW, extras, mode: 'codes' });
        assert.deepEqual(codes.codes, want, `${col} ${dir}: 全部のコード`);
        const page = await R.listSkus(dbE, f, { now: NOW, extras });
        assert.deepEqual(page.rows.map((x) => x.code), want.slice(0, 100), `${col} ${dir}: 1 ページ目`);
      }
    }
    // 古い URL の並び = 新しい形の並び
    for (const [old, sort, dir] of [['reg_desc', 'reg', 'desc'], ['kind', 'kind', ''], ['profit_asc', 'profit', ''], ['rate_asc', 'rate', '']]) {
      const a = (await R.listSkus(dbE, { sort: old }, { now: NOW, extras, mode: 'codes' })).codes;
      assert.deepEqual(a, expectedOrder(all, sort, dir || 'asc'), old);
    }
  });

  await ta('[2] 速さ: 1 ページ目を読む時間 (中央値) = どの並びも 1 秒より短い・コード順は今 (origin/master) と大きく変わらない', async () => {
    const med = async (fn, n = 5) => { await fn(); const ts = []; for (let i = 0; i < n; i++) { const t = performance.now(); await fn(); ts.push(performance.now() - t); } ts.sort((a, b) => a - b); return ts[Math.floor(n / 2)]; };
    const rows = [];
    const base = await med(() => R.listSkus(dbE, {}, { now: NOW, extras }));
    rows.push(['新 コード順', base]);
    if (OLD) {
      const old = await med(() => OLD.listSkus(dbE, {}, { now: NOW, extras }));
      rows.push(['今 (master) コード順', old]);
      rows.push(['今 (master) 登録日の新しい順', await med(() => OLD.listSkus(dbE, { sort: 'reg_desc' }, { now: NOW, extras }))]);
      rows.push(['今 (master) 利益の少ない順', await med(() => OLD.listSkus(dbE, { sort: 'profit_asc' }, { now: NOW, extras }))]);
      assert.ok(base < old * 1.5 + 30, `コード順が今より大きく遅くない (新 ${base.toFixed(1)} ms / 今 ${old.toFixed(1)} ms)`);
    }
    for (const col of LC.SORTABLE.filter((c) => c !== 'code')) {
      for (const dir of ['', 'desc']) rows.push([`新 ${LC.COLUMN_BY_ID[col].label} ${dir || 'asc'}`, await med(() => R.listSkus(dbE, { sort: col, dir }, { now: NOW, extras }))]);
    }
    for (const [k, ms] of rows) console.log(`      ${k.padEnd(28, '　')} ${ms.toFixed(1).padStart(7)} ms`);
    for (const [k, ms] of rows) assert.ok(ms < 1000, `${k}: ${ms.toFixed(1)} ms`);
    const worst = rows.filter(([k]) => k.startsWith('新')).reduce((a, b) => (b[1] > a[1] ? b : a));
    console.log(`      いちばん遅い並び = ${worst[0]} ${worst[1].toFixed(1)} ms`);
  });

  await ta('[3] EXPLAIN: 並びの読みは全部の行を 1 回読んで並べる (索引が要らない)・原価・仕入先も 1 回の集合', async () => {
    const sqls = [];
    const spy = { query: (t, p) => { sqls.push([t, p]); return dbE.query(t, p); } };
    for (const sort of ['name', 'cost', 'sup', 'price', 'reg']) {
      sqls.length = 0;
      await R.listSkus(spy, { sort, dir: 'desc' }, { now: NOW, extras });
      const [t, p] = sqls.find(([x]) => /order by/.test(x) && /from core\.skus s/.test(x) && !/= any\(\$1::bigint/.test(x));
      const plan = (await E.query(`explain (analyze, format json) ${t}`, p)).rows[0]['QUERY PLAN'][0];
      const ms = plan['Execution Time'];
      const flat = JSON.stringify(plan.Plan);
      const nodes = [...flat.matchAll(/"Node Type":"([^"]+)"/g)].map((m) => m[1]);
      console.log(`      ${sort.padEnd(6)} desc: ${ms.toFixed(1)} ms · ${[...new Set(nodes)].join(' / ')}`);
      assert.ok(ms < 300, `${sort}: ${ms} ms`);
      assert.ok(!/Nested Loop/.test(flat) || !/"Relation Name":"sku_costs"[^}]*"Loops":[1-9][0-9]{2,}/.test(flat), `${sort}: 原価の表を SKU ごとに読まない`);
    }
  });
} finally {
  try { if (E) await E.end(); } catch { /* */ }
  try { await O.end(); } catch { /* */ }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
