/**
 * test-master-ne-delete.mjs — NE にもう登録した商品を「NE から消す」(0072・中原さんの頼み 10/11 の場面 c・決定 10/12 Q1 a / Q2 a / Q3 a / Q4 b) を PGlite で確かめる
 *
 * 固定する契約:
 *   [M]  migration の写し: ops.sku_gone_from_ne_check・ops.cancel_sku_gone_from_ne・ops.guard_sku_ref_not_cancelled (0070)・ops.master_write_allowed (0071) は、
 *        前の最新の版から写して「🆕 0072」の行だけ足した (印の行を外すと前の版と 1 字も違わない)・約束の操作の CHECK = 0071 の全部 + 3 つ・
 *        security definer の関数は search_path = pg_catalog, pg_temp・public の実行権なし・一時の表なし
 *   [F]  CSV のバイト列: 消す CSV = 見出し「コード」の Shift_JIS (83 52 81 5B 83 68)・CRLF・最後の行も CRLF・NE の元の書き方 (DB = JS) /
 *        取扱中止 = apps/master-decisions/ne-csv.mjs の buildCsv(products:handling) と同じバイト (DB = JS)・アプリの在庫の鍵 (DB = JS)
 *   [C]  確かめ (ops.ne_del_check): ok の形 (NE の元の書き方・在庫)・理由の 1 つ 1 つ (NE に無い・書き方・台帳が古い・available でない・セットの中身 (同じ依頼なら ok + セットを先に)・
 *        出品の構成・ほかの親・在庫 (NE の日次・ロジザードの日次・アプリの値・読めない)・セット自身の構成の依頼)
 *   [B]  作る (ops.ne_del_build): セット → 単品の 2 つのファイル・品目 live・保存の記録・押し直し = 前の答え・同じ番号で違う中身 = request_id_reused・1 件でも断られると何も書かない・
 *        依頼の途中は 2 つ目の依頼にできない
 *   [I]  依頼の途中の SKU への新しい参照 (出品の構成・構成の依頼・構成の子) = sku_ne_deleting で断る・やめたセット (親) への構成 = sku_cancelled
 *   [D]  受け取り (ops.ne_del_download): 作った日だけ・中の品目が全部 live のときだけ・初めてのダウンロードと回数を残す / [W] 取り下げ
 *   [G]  セットの「NE から消えた → やめる」(決定 Q4 b): 3 日続けて無いセット = やめられる・構成を外す・依頼の品目 = gone・中身の単品がやめられるようになる・
 *        夜間ロードは NE の古い行からやめたセットの構成を戻さない
 *   [R]  断られたとき (決定 Q2 a): ダウンロードから 3 日たっても NE にある単品 = 取扱中止の CSV (ne-csv.mjs と同じバイト)・消す品目は handled / 3 日たっていない = 断る
 *   [L]  夜間ロード: 依頼の途中の子・出品の構成は飛ばして止まらない (今の行は残す)
 *   [O]  表の守り (関数の外の書き込み・消す・戻す) と権限 (master_edit = 関数だけ・watcher = 確かめと一覧だけ)
 *   [K]  アプリの在庫 (ne-delete-stock.mjs): 値の出どころ (NE = 総在庫数 − FBA ほか・ロジザードの今の在庫と引当を別々に)・読めない = unreadable (0 にしない) / [G4] CLI でセットをやめたときの変更の記録
 *   [S]  画面 (本物の router): 注文残の数は発注アプリの権限がある人だけ (S4)・ 「NE から消す」の画面・確かめる・作る・受け取る (POST・sha256・Shift_JIS)・名簿の人だけ・商品の画面の箱 / [U] 画面の JS (Playwright)
 * 使い方: node scripts/test-master-ne-delete.mjs (MASTER_EDIT_UI_SKIP=1 で画面の JS を飛ばす)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles, NE_DEL_FUNCTIONS, NE_DEL_OWNER_ONLY_FUNCTIONS } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import * as ND from '../lib/master-ne-delete.mjs';
import { cancelSkuGoneFromNe } from '../lib/master-gone.mjs';
import { buildCsv, specOf as neCsvSpecOf } from '../apps/master-decisions/ne-csv.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIG = path.join(ROOT, 'db', 'company', 'migrations');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'nedel-'));
process.env.DATA_DIR = DATA_TMP;
const watchdog = setTimeout(() => { console.error('  NG  試験が 900 秒で終わらない (止める)'); process.exit(1); }, Number(process.env.NEDEL_TEST_MS || 900000));
watchdog.unref();

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const quiet = () => {};
const hex = (c) => c.repeat(64);

// ═══════════ [M] migration の写し (文字で) ═══════════
const migFiles = fs.readdirSync(MIG).filter((f) => /^\d{4}_.+\.sql$/.test(f)).sort();
const readMig = (f) => fs.readFileSync(path.join(MIG, f), 'utf8').replace(/\r\n/g, '\n');
function fnText(text, name) {
  const re = new RegExp(`create (?:or replace )?function ${name.replace(/\./g, '\\.')}\\(`);
  const i = text.search(re);
  if (i < 0) return null;
  const k = text.indexOf(' as $$', i);
  const j = text.indexOf('$$;', k + 6);
  return text.slice(i, j + 3).replace(/^create or replace function/, 'create function');
}
function strip0072(t) {
  const out = [];
  let skip = false;
  for (const l of t.split('\n')) {
    if (/-- 🆕 0072 ここから/.test(l)) { skip = true; continue; }
    if (/-- 🆕 0072 ここまで/.test(l)) { skip = false; continue; }
    if (skip || /🆕 0072/.test(l)) continue;
    out.push(l);
  }
  return out.join('\n');
}
const lastDefBefore = (name, before = '0072') => migFiles.filter((f) => f < before && fnText(readMig(f), name)).pop();
const F72 = migFiles.find((f) => f.startsWith('0072_'));
const M72 = readMig(F72);

console.log('[M] migration の写し');
for (const [name, expectFrom] of [['ops.sku_gone_from_ne_check', '0070'], ['ops.cancel_sku_gone_from_ne', '0070'], ['ops.guard_sku_ref_not_cancelled', '0070'], ['ops.master_write_allowed', '0071']]) {
  await ta(`[M1] ${name} = ${expectFrom} の版 (前の最新) から写して 🆕 0072 の行だけ足した`, async () => {
    const src = lastDefBefore(name);
    assert.ok(src && src.startsWith(expectFrom), `前の最新の版が ${src} (期待 ${expectFrom})`);
    const before = fnText(readMig(src), name);
    const now = fnText(M72, name);
    assert.ok(now, '0072 に無い');
    assert.notEqual(now, before, '何も足していない');
    assert.equal(strip0072(now), before, '印の行を外すと前の版と違う (写し間違い・印の無い変更)');
    for (const later of migFiles.filter((x) => x > F72)) assert.equal(fnText(readMig(later), name), null, `${later} も ${name} を作り直している = この試験の元を見直す`);
  });
}
await ta('[M2] 約束の操作の CHECK = 0071 の全部 + ne_del_build・ne_del_withdraw・ne_handling_build (どちらの表も)', async () => {
  const ops = (text, tbl) => {
    const m = new RegExp(`alter table ops\\.${tbl} add constraint ck_\\w+ check \\(operation in \\(([^;]*?)\\)\\);`).exec(text);
    return m ? [...m[1].matchAll(/'([a-z_]+)'/g)].map((x) => x[1]) : null;
  };
  const src = readMig(migFiles.find((f) => f.startsWith('0071_')));
  for (const tbl of ['master_write_sessions', 'master_edit_requests']) {
    assert.deepEqual(ops(M72, tbl), [...ops(src, tbl), 'ne_del_build', 'ne_del_withdraw', 'ne_handling_build'], tbl);
  }
});
await ta('[M3] security definer の関数 = search_path = pg_catalog, pg_temp・public の実行権を外す・一時の表なし', async () => {
  assert.doesNotMatch(M72, /\b(create\s+(temp|temporary)\s+table|on commit drop)\b/i);
  const defs = [...M72.matchAll(/create (?:or replace )?function ([a-z_.]+)\(([^)]*)\)[^$]*?security definer[^$]*?as \$\$/g)];
  assert.ok(defs.length >= 12, `security definer の関数が ${defs.length}`);
  for (const d of defs) {
    const head = d[0];
    assert.match(head, /set search_path = pg_catalog, pg_temp/, `${d[1]} の search_path`);
    assert.ok(M72.includes(`revoke all on function ${d[1]}(`), `${d[1]} の public の実行権を外していない`);
  }
});

// ═══════════ [F] CSV のバイト列 (JS) ═══════════
console.log('[F] CSV のバイト列');
await ta('[F1] 消す CSV (JS) = 見出し「コード」の Shift_JIS 83 52 81 5B 83 68 0D 0A・コードごとに CRLF・最後の行も CRLF・Shift_JIS で読むと「コード」', async () => {
  const b = ND.delCsvBytes(['AbC-01', 'x_2']);
  assert.equal(b.subarray(0, 8).toString('hex'), '8352815b83680d0a');
  assert.equal(b.toString('hex'), '8352815b83680d0a' + Buffer.from('AbC-01\r\nx_2\r\n', 'ascii').toString('hex'));
  assert.equal(new TextDecoder('shift_jis').decode(b), 'コード\r\nAbC-01\r\nx_2\r\n');
  assert.equal(b.toString('latin1').split('\r\n').length, 4);   // 見出し + 2 行 + 最後の空
  assert.doesNotMatch(b.toString('latin1').replace(/\r\n/g, ''), /[\r\n]/, 'CR / LF が単独で入っている');
  for (const bad of ['', 'a,b', 'ｱ', 'a b', 'x'.repeat(31)]) assert.throws(() => ND.delCsvBytes([bad]), /形が違う/, bad);
});
await ta('[F2] 取扱中止の CSV (JS) = ne-csv.mjs の buildCsv(products:handling) と同じ (見出し syohin_code,toriatukai_kbn・値 1・UTF-8・CRLF)', async () => {
  const want = buildCsv(neCsvSpecOf('products', 'handling'), [{ ne_code: 'Ab-1', cell: '1' }, { ne_code: 'zz', cell: '1' }]).bytes;
  assert.equal(ND.handlingCsvBytes(['Ab-1', 'zz']).toString('hex'), want.toString('hex'));
  assert.equal(want.toString('utf8'), 'syohin_code,toriatukai_kbn\r\nAb-1,1\r\nzz,1\r\n');
});

// ═══════════ DB の用意 ═══════════
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const q = async (sql, p) => (await pg.query(sql, p)).rows;
const one = async (sql, p) => (await q(sql, p))[0];
async function as(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
const errOf = async (fn) => { try { await fn(); } catch (e) { return e; } return null; };

await ta('[F3] DB のバイト列 = JS (消す CSV・取扱中止の CSV)・アプリの在庫の鍵 = JS', async () => {
  const codes = ['AbC-01', 'x_2', 'Z9'];
  const d = (await one('select ops.ne_del_csv_bytes($1::text[]) as b', [codes])).b;
  assert.equal(Buffer.from(d).toString('hex'), ND.delCsvBytes(codes).toString('hex'));
  const h = (await one('select ops.ne_handling_csv_bytes($1::text[]) as b', [codes])).b;
  assert.equal(Buffer.from(h).toString('hex'), ND.handlingCsvBytes(codes).toString('hex'));
  const keys = Object.keys((await one('select ops.ne_del_app_stock_keys() as k')).k).sort();
  assert.deepEqual(keys, [...ND.APP_STOCK_KEYS].sort());
});

const single = (code) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
const setSku = (code) => ({ ...single(code), kind: 'set', cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } });
const SINGLES = ['n01', 'n02', 'n03', 'n04', 'n05', 'n06', 'n07', 'n08', 'n09', 'n10', 'n11', 'n12', 'n13', 'c01', 'c02', 'c03', 'c04', 'c05', 'k01', 'NdUp'];
const rakuten = (code, comps) => ({ mall: 'rakuten', shopCode: 'main', listingCode: code, status: 'active', components: comps.map((c) => ({ code: c, qty: 1, resolution: 'exact', evidence: { source: 'rakuten_sku_map' } })) });
const PLAN = {
  skus: [...SINGLES.map(single), setSku('set01'), setSku('set02'), setSku('set03'), setSku('set04')],
  variationGroups: [{ code: 'n06', name: 'まとまり n06', childCodes: ['k01'], status: 'active' }],   // n06 = k01 の代表 (親)
  setComponents: [{ parentCode: 'set01', childCode: 'c01', qty: 1, source: 'ne' }, { parentCode: 'set01', childCode: 'c02', qty: 2, source: 'ne' },
    { parentCode: 'set02', childCode: 'c03', qty: 1, source: 'ne' }, { parentCode: 'set03', childCode: 'c04', qty: 1, source: 'ne' }, { parentCode: 'set04', childCode: 'c05', qty: 1, source: 'ne' }],
  listings: [rakuten('r-n05', ['n05'])], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], reorder: { available: false, runId: null, reason: '試験' },
};
const lr = await runInitialLoad(db, PLAN, { log: quiet, runId: 'nedel_load_1', host: 'test', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', 'skus.sku_kind': 'company', 'listing_components.amazon': 'company' };   // 10/9 の本番の形
await forceNewOpen(db, BASE);
const sid = async (code) => String((await one('select sku_id from core.skus where code = $1', [code])).sku_id);
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))?.state ?? null;
const jstToday = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
const jstDay = (d) => new Date(Date.parse(`${jstToday()}T00:00:00Z`) + d * 86400e3).toISOString().slice(0, 10);

// NE のコードの台帳 (0041)。observed_at = 今 (36 時間の中)・回ごとに新しく
let ncN = 0;
async function neCodes(codes, { collided = [], observedAt = null } = {}) {
  ncN++;
  const at = observedAt || new Date(Date.now() - 60e3 + ncN * 10);
  const run = `mc_${at.toISOString().replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`;
  await q('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)', [run, at.toISOString()]);
  const entries = codes.map((c) => (collided.includes(c)
    ? { code_norm: c.toLowerCase(), kind: 'product', state: 'collided', ne_code: null, spellings: [c, c.toUpperCase()] }
    : { code_norm: c.toLowerCase(), kind: 'product', state: 'ok', ne_code: c, spellings: [c] }));
  await q('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries })]);
  return run;
}
const ALL_CODES = [...SINGLES, 'set01', 'set02', 'set03', 'set04'];
await neCodes(ALL_CODES.filter((c) => c !== 'n02'), { collided: ['n08'] });

// 在庫の日次 (0011)。rows = { code: qty } (sku_id は code_norm で当てる)。alloc = { code: 引当 }
let irN = 0;
async function stockDay(source, day, rows, alloc = {}) {
  const run = `nedel_${source}_${++irN}`;
  const scope = source === 'ne' ? 'main' : 'wh1';
  await q(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, complete) values ($1, $2, 'stock_daily', $3, now(), 'success', true)`, [run, source, scope]);
  await q(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, captured_at) values ($1, $2, $3, 1, 'building', $4, now())`, [day, source, scope, run]);
  for (const [code, qty] of Object.entries(rows)) {
    await q(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, allocated_qty, captured_at, ingest_run_id)
      values ($1, $2, $3, $4, 1, (select sku_id from core.skus where code_norm = core.norm_code($4)), $5, $6, now(), $7)`, [day, source, scope, code, qty, alloc[code] ?? 0, run]);
  }
  await q(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1 and source = $2 and scope_key = $3`, [day, source, scope]);
}
const neRows = Object.fromEntries(SINGLES.filter((c) => c !== 'n04').map((c) => [c, c === 'n03' ? 2 : 0]));
await stockDay('ne', jstToday(), neRows);
await stockDay('logizard', jstDay(-1), { n07: 1, n01: 0 });

/** アプリの在庫 (全部 0) */
const zero = () => ({ ne_own: 0, ne_allocated: 0, ne_on_order: 0, fba: 0, logizard: 0, logizard_allocated: 0, po_backorder: 0, sources: { test: true } });
const zeroReader = async (skus) => new Map(skus.map((s) => [s.code_norm, zero()]));
const check = async (code, { batch = null, app = undefined } = {}) => {
  const id = await sid(code);
  const b = batch ? await Promise.all(batch.map(sid)) : [id];
  return (await one('select ops.ne_del_check($1::bigint, $2::bigint[], $3::jsonb) as r', [id, b, app === undefined ? JSON.stringify(zero()) : (app === null ? null : JSON.stringify(app))])).r;
};
const reasons = (c) => c.problems.map((p) => p.reason).sort();

// 完全な NE の取得の続けて無い日数 (0070) = 記録の回を置いて本物の ops._record_ne_absence を流す (test-master-gone.mjs と同じ作り)
async function obsWithout(gone = []) {
  const rows = await q(`select k.code_norm, k.sku_kind from core.skus k where k.company_id = 1 and k.sku_kind in ('single', 'set') order by k.code_norm`);
  return { format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' }, trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' },
    rows: rows.filter((r) => !gone.includes(r.code_norm)).map((r) => (r.sku_kind === 'set' ? [r.code_norm, 'set', null, null, null] : [r.code_norm, 'single', 'ok', null, null])) };
}
let runN = 0;
const runId = () => `mc_20301012T${String(++runN).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;
let tick = 0;
function fetchAt(day) {
  tick++;
  if (day === 0) return new Date(Math.max(Date.now() - 20 * 60e3, Date.parse(`${jstToday()}T00:01:00+09:00`)) + tick * 1000);
  return new Date(Date.parse(`${jstToday()}T12:00:00+09:00`) + day * 86400e3 + tick * 1000);
}
async function feed(gone, { day = 0 } = {}) {
  const when = fetchAt(day).toISOString();
  const obs = await obsWithout(gone);
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.parent_gate_protocol', '1', true)");
    const id = (await pg.query(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
        evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by)
      values (1, $1, $2, repeat('1', 64), $3, $3, 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`,
      [runId(), `ne_${crypto.randomBytes(4).toString('hex')}`, when])).rows[0].result_id;
    await pg.query('select ops._record_ne_absence($1, $2::jsonb)', [id, JSON.stringify(obs)]);
    await pg.query('commit');
    return String(id);
  } catch (e) { await pg.query('rollback'); throw e; }
}
await feed([], { day: -3 });

// ═══════════ [C] 確かめ ═══════════
console.log('[C] 確かめ');
await ta('[C1] ok の形: NE の元の書き方 (大文字も)・在庫 (Company DB の日次 + アプリの値)・セットは在庫なし', async () => {
  assert.equal(await regOf('n01'), 'available');
  const c = await check('n01');
  assert.deepEqual(c.problems, [], JSON.stringify(c.problems));
  assert.equal(c.ok, true); assert.equal(c.ne_code, 'n01'); assert.equal(c.sku_kind, 'single');
  assert.equal(c.stock.db.ne_day, jstToday()); assert.equal(c.stock.db.ne_qty, 0); assert.equal(c.stock.db.logizard_day, jstDay(-1)); assert.equal(c.stock.app.ne_own, 0);
  const u = await check('NdUp');
  assert.equal(u.ok, true, JSON.stringify(u.problems)); assert.equal(u.ne_code, 'NdUp');
  const s = await check('set01', { app: null });
  assert.equal(s.ok, true, JSON.stringify(s.problems)); assert.equal(s.stock, null);
});
await ta('[C2] 理由: NE に無い・書き方が決まらない・出品の構成・ほかの親・在庫 (NE の日次 2 個 / 行が無い / ロジザード 1 個)', async () => {
  assert.deepEqual(reasons(await check('n02')), ['not_in_ne']);
  assert.deepEqual(reasons(await check('n08')), ['ne_code_bad']);
  assert.deepEqual(reasons(await check('n05')), ['listing_component']);
  assert.match((await check('n05')).problems[0].text, /rakuten:r-n05/);
  assert.deepEqual(reasons(await check('n06')), ['parent_of_others']);
  assert.deepEqual(reasons(await check('n03')), ['stock_ne']);
  assert.match((await check('n03')).problems[0].text, /2 個/);
  assert.deepEqual(reasons(await check('n04')), ['stock_ne_unknown']);
  assert.deepEqual(reasons(await check('n07')), ['stock_logizard']);
});
await ta('[C3] アプリの在庫: 1 つでも 0 でない = stock_app (どれか言う)・鍵が無い / 形が違う / 読めない / 無い = stock_app_unknown', async () => {
  const nz = await check('n09', { app: { ...zero(), fba: 3, po_backorder: 1 } });
  assert.deepEqual(reasons(nz), ['stock_app']); assert.match(nz.problems[0].text, /FBA の在庫 3/); assert.match(nz.problems[0].text, /発注アプリの注文残 1/);
  // 🆕 #1690 Codex R1 High 1: ロジザードの今の引当 (在庫 0・引当 5) = 断る / 引当の鍵が無い = 読めない
  const al = await check('n09', { app: { ...zero(), logizard_allocated: 5 } });
  assert.deepEqual(reasons(al), ['stock_app']); assert.match(al.problems[0].text, /ロジザードの引当 \(今\) 5/);
  const noAl = { ...zero() }; delete noAl.logizard_allocated;
  assert.deepEqual(reasons(await check('n09', { app: noAl })), ['stock_app_unknown']);
  assert.deepEqual(reasons(await check('n09', { app: { ...zero(), logizard_allocated: null } })), ['stock_app_unknown']);
  const miss = { ...zero() }; delete miss.ne_allocated;
  assert.deepEqual(reasons(await check('n09', { app: miss })), ['stock_app_unknown']);
  assert.deepEqual(reasons(await check('n09', { app: { ...zero(), ne_own: -1 } })), ['stock_app_unknown']);
  assert.deepEqual(reasons(await check('n09', { app: { ...zero(), ne_own: '0' } })), ['stock_app_unknown']);
  assert.deepEqual(reasons(await check('n09', { app: { ...zero(), ne_own: 1.5 } })), ['stock_app_unknown']);
  const ur = await check('n09', { app: { unreadable: '商品管理リストが古い' } });
  assert.deepEqual(reasons(ur), ['stock_app_unknown']); assert.match(ur.problems[0].text, /商品管理リストが古い/);
  assert.deepEqual(reasons(await check('n09', { app: null })), ['stock_app_unknown']);
});
await ta('[C4] セットの中身: 単品だけ = set_component (セットを先に) / 同じ依頼にセットがある = ok + 警告 set_first / セットは在庫を見ない', async () => {
  const alone = await check('c01');
  assert.deepEqual(reasons(alone), ['set_component']); assert.match(alone.problems[0].text, /set01/);
  const both = await check('c01', { batch: ['c01', 'set01'] });
  assert.equal(both.ok, true, JSON.stringify(both.problems));
  assert.deepEqual(both.warnings.map((w) => w.reason), ['set_first']);
});
await ta('[C5] (取引の中で変えて巻き戻す) available でない・台帳が古い・セット自身の開いている構成の依頼・同じ代表の子を全部 = 警告 page_info', async () => {
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.registration_protocol', '1', true)");
    await q("update ops.master_registrations set state = 'quarantined' where sku_id = $1", [await sid('n12')]);
    assert.ok(reasons(await check('n12')).includes('not_available'));
    await q("update ops.master_ne_code_mark set observed_at = now() - interval '40 hours' where id = 1");
    assert.ok(reasons(await check('n13')).includes('ne_codes_old'));
    await q(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id) values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`,
      [await sid('set01'), JSON.stringify([{ sku_id: Number(await sid('n13')), code: 'n13', qty: 1, sort: 1 }]), hex('9')]);
    const s = await check('set01', { app: null });
    assert.ok(reasons(s).includes('own_component_request'), JSON.stringify(s.problems));
    assert.ok(reasons(await check('n13')).includes('component_request'));
  } finally { await pg.query('rollback'); }
  const k = await check('k01');
  assert.equal(k.ok, true, JSON.stringify(k.problems));
  assert.deepEqual(k.warnings.map((w) => w.reason), ['page_info']);
});

// ═══════════ [B] 作る ═══════════
console.log('[B] 作る');
/** 画面のロール (master_edit) の db (lib に渡す) */
const asEdit = { query: async (t, p) => { await pg.query('set role master_edit'); try { return await pg.query(t, p); } finally { await pg.query('set role deploy'); } } };
// lib は取引の中で何度も query を呼ぶ = 取引の間ずっと master_edit (set role は取引の中でも効く・PGlite は 1 つの接続)
const editDb = { query: (t, p) => pg.query(t, p) };
async function asEditTx(fn) { await pg.query('set role master_edit'); try { return await fn(editDb); } finally { await pg.query('set role deploy'); } }
const counts = async () => one('select (select count(*)::int from ops.ne_del_exports) as e, (select count(*)::int from ops.ne_del_items) as i');
let B1;
await ta('[B1] 画面のロールで作る: セット → 単品の 2 つのファイル (NE の元の書き方・コードの順)・バイト列 = JS・品目 live・保存の記録 done (db_user = master_edit)', async () => {
  const rid = crypto.randomUUID();
  B1 = await asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n01', 'set01', 'c01', 'c02', 'NdUp'], reason: '販売をやめた', requestId: rid, confirm: true },
    { open: true, stockReader: zeroReader }));
  assert.equal(B1.ok, true);
  assert.deepEqual(B1.exports.map((e) => [e.kind, e.codes]), [['sets', ['set01']], ['products', ['NdUp', 'c01', 'c02', 'n01']]]);
  for (const e of B1.exports) {
    const r = await one('select file_bytes, sha256, header, encoding, built_by, built_day::text as d, db_user from ops.ne_del_exports where export_id = $1', [e.export_id]);
    assert.equal(Buffer.from(r.file_bytes).toString('hex'), ND.delCsvBytes(e.codes).toString('hex'));
    assert.equal(r.sha256, ND.sha256Hex(Buffer.from(r.file_bytes))); assert.equal(r.sha256, e.sha256);
    assert.deepEqual([r.header, r.encoding, r.built_by, r.d, r.db_user], ['コード', 'sjis', 'naka@test', jstToday(), 'master_edit']);
  }
  const items = await q("select k.code, i.state, i.action, i.ne_code, i.evidence from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id order by k.code");
  assert.deepEqual(items.map((x) => [x.code, x.state, x.action]), [['NdUp', 'live', 'delete'], ['c01', 'live', 'delete'], ['c02', 'live', 'delete'], ['n01', 'live', 'delete'], ['set01', 'live', 'delete']]);
  assert.equal(items.find((x) => x.code === 'n01').evidence.stock.app.sources.test, true);   // アプリの在庫の値を記録に残す
  assert.equal(items.find((x) => x.code === 'n01').evidence.stock.db.ne_qty, 0);
  const req = await one("select operation, status, actor_id, target_code from ops.master_edit_requests where request_id = $1", [rid]);
  assert.deepEqual([req.operation, req.status, req.actor_id], ['ne_del_build', 'done', 'naka@test']);
  assert.equal((await one('select db_user from ops.master_write_sessions where request_id = $1', [rid])).db_user, 'master_edit');
  // 押し直し = 前の答え・同じ番号で違う理由 = request_id_reused
  const again = await asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n01', 'set01', 'c01', 'c02', 'NdUp'], reason: '販売をやめた', requestId: rid, confirm: true }, { open: true, stockReader: zeroReader }));
  assert.equal(again.replayed, true); assert.deepEqual(again.exports, B1.exports);
  // 🆕 #1690 Codex R2 Medium 1: 応答が失われて押し直す = 在庫を読み直す (読んだ時刻・値の証跡が変わる) = それでも前の答え (request_id_reused にしない)
  const movingReader = async (skus) => new Map(skus.map((s) => [s.code_norm, { ...zero(), sources: { read_at: new Date().toISOString(), nonce: crypto.randomUUID() } }]));
  const again2 = await asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n01', 'set01', 'c01', 'c02', 'NdUp'], reason: '販売をやめた', requestId: rid, confirm: true }, { open: true, stockReader: movingReader }));
  assert.equal(again2.replayed, true); assert.deepEqual(again2.exports, B1.exports);
  const e2 = await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n01', 'set01', 'c01', 'c02', 'NdUp'], reason: '別の理由', requestId: rid, confirm: true }, { open: true, stockReader: zeroReader })));
  assert.equal(e2?.reason, 'request_id_reused');
});
await ta('[B2] 1 件でも断られる = 何も書かない (409 not_deletable・理由を全部)・依頼の途中 = live_request・無いコード = 404・印なし / 理由なし = 400・切替前 = 409', async () => {
  const before = await counts();
  const e = await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n09', 'n03'], reason: '販売をやめた', confirm: true }, { open: true, stockReader: zeroReader })));
  assert.equal(e?.status, 409); assert.equal(e?.reason, 'not_deletable'); assert.match(e.message, /n03 \(NE の在庫が 2 個/); assert.doesNotMatch(e.message, /n09 \(/);
  const e2 = await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n01'], reason: '2 回目', confirm: true }, { open: true, stockReader: zeroReader })));
  assert.equal(e2?.reason, 'not_deletable'); assert.match(e2.message, /依頼の途中/);
  const e3 = await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['zz-none'], reason: 'x', confirm: true }, { open: true, stockReader: zeroReader })));
  assert.equal(e3?.status, 404);
  assert.equal((await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n09'], reason: 'x' }, { open: true, stockReader: zeroReader }))))?.status, 400);
  assert.equal((await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n09'], reason: ' ', confirm: true }, { open: true, stockReader: zeroReader }))))?.status, 400);
  assert.equal((await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n09'], reason: 'x', confirm: true }, { open: false, stockReader: zeroReader }))))?.reason, 'before_cutover');
  // 読み手が無い・落ちた = 全部「読めない」(DB が断る)
  const e4 = await errOf(async () => asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n09'], reason: 'x', confirm: true }, { open: true, stockReader: async () => { throw new Error('mirror 無し'); } })));
  assert.equal(e4?.reason, 'not_deletable'); assert.match(e4.message, /mirror 無し/);
  assert.deepEqual(await counts(), before);
});

await ta('[B3] セットだけの依頼 = セットのファイル 1 つ (在庫は見ない)', async () => {
  const r = await asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['set03'], reason: 'セットをやめる', confirm: true }, { open: true, stockReader: null }));
  assert.deepEqual(r.exports.map((e) => [e.kind, e.codes]), [['sets', ['set03']]]);
});

// ═══════════ [I] 参照の守り ═══════════
console.log('[I] 参照の守り');
await ta('[I1] 依頼の途中の SKU = 新しい出品の構成・構成の依頼・構成の子 に入れられない (sku_ne_deleting・どの書き手も)', async () => {
  const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', 'main', 'r-i1', 'active') returning listing_id`)).listing_id;
  const e1 = await errOf(async () => q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('n01')]));
  assert.match(e1?.message ?? '', /sku_ne_deleting/); assert.equal(e1.code, '23514');
  const e2 = await errOf(async () => q(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id) values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`,
    [await sid('set02'), JSON.stringify([{ sku_id: Number(await sid('n01')), code: 'n01', qty: 1, sort: 1 }]), hex('8')]));
  assert.match(e2?.message ?? '', /sku_ne_deleting/);
  const e3 = await errOf(async () => q(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id) values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`,
    [await sid('set01'), JSON.stringify([{ sku_id: Number(await sid('n13')), code: 'n13', qty: 1, sort: 1 }]), hex('7')]));
  assert.match(e3?.message ?? '', /sku_ne_deleting/, '依頼のセット自身 (set01) も見る');
  const e4 = await errOf(async () => q(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'manual')`, [await sid('set02'), await sid('n01')]));
  assert.match(e4?.message ?? '', /sku_ne_deleting/);
  // 関係ない商品はいつもどおり
  await q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('n13')]);
  await q('delete from core.listing_components where listing_id = $1', [lid]);
});

// ═══════════ [D] 受け取り / [W] 取り下げ ═══════════
console.log('[D] 受け取り・[W] 取り下げ');
await ta('[D1] 受け取る: バイト列 = 記録・初めてのダウンロード (人・時刻) と回数・2 回目は回数だけ・ファイル名', async () => {
  const sets = B1.exports.find((e) => e.kind === 'sets');
  const f = await asEditTx((d) => ND.downloadNeDelete(d, { actor: 'naka@test', exportId: sets.export_id }));
  assert.equal(f.bytes.toString('hex'), ND.delCsvBytes(['set01']).toString('hex'));
  assert.match(f.file_name, new RegExp(`^ne-delete-sets-${jstToday().replace(/-/g, '')}-${sets.export_id}\\.csv$`));
  const r1 = await one('select first_downloaded_by, download_count, first_downloaded_at from ops.ne_del_exports where export_id = $1', [sets.export_id]);
  assert.deepEqual([r1.first_downloaded_by, r1.download_count], ['naka@test', 1]);
  await asEditTx((d) => ND.downloadNeDelete(d, { actor: 'other@test', exportId: sets.export_id }));
  const r2 = await one('select first_downloaded_by, download_count, first_downloaded_at from ops.ne_del_exports where export_id = $1', [sets.export_id]);
  assert.deepEqual([r2.first_downloaded_by, r2.download_count, String(r2.first_downloaded_at)], ['naka@test', 2, String(r1.first_downloaded_at)]);
});
await ta('[D2] 作った日でない: 消す CSV = file_old (渡さない) / 取扱中止の CSV = 後の日も同じバイト列 (sha256) で渡す', async () => {
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.ne_del_protocol', '1', true)");
    const b = ND.delCsvBytes(['n13']);
    const id = (await one(`insert into ops.ne_del_exports (kind, schema_version, encoding, header, file_bytes, sha256, row_count, request_id, reason, built_by, built_day, db_user)
      values ('products', 'ne-del-products-v1', 'sjis', 'コード', $1, $2, 1, gen_random_uuid(), '古い', 't', $3, 't') returning export_id`, [b, ND.sha256Hex(b), jstDay(-1)])).export_id;
    await pg.query('savepoint d2');
    const e = await errOf(async () => one('select ops.ne_del_download($1, $2::bigint) as r', ['naka@test', id]));
    assert.match(e?.message ?? '', /^file_old:/);
    await pg.query('rollback to savepoint d2');
    // 🆕 #1690 Codex R1 Medium 3: 取扱中止の CSV は作った時に元の品目を handled にする = 後の日も受け取れる
    const hb = ND.handlingCsvBytes(['n13']);
    const hid = (await one(`insert into ops.ne_del_exports (kind, schema_version, encoding, header, file_bytes, sha256, row_count, request_id, reason, built_by, built_day, db_user)
      values ('handling', 'ne-handling-v1', 'utf8', 'syohin_code,toriatukai_kbn', $1, $2, 1, gen_random_uuid(), '前の日', 't', $3, 't') returning export_id`, [hb, ND.sha256Hex(hb), jstDay(-5)])).export_id;
    const r = (await one('select ops.ne_del_download($1, $2::bigint) as r', ['naka@test', hid])).r;
    assert.equal(r.sha256, ND.sha256Hex(hb)); assert.equal(Buffer.from(r.bytes_b64, 'base64').toString('hex'), hb.toString('hex'));
  } finally { await pg.query('rollback'); }
});
await ta('[W1] 取り下げ: live → withdrawn (理由・押し直し = 前の答え)・同じ品目をもう一度 = not_live・中の品目が変わったファイル = file_changed・取り下げた SKU は参照に使える', async () => {
  const item = (await one("select i.item_id from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = 'NdUp' and i.state = 'live'")).item_id;
  const rid = crypto.randomUUID();
  const r = await asEditTx((d) => ND.withdrawNeDelete(d, { actor: 'naka@test', itemId: item, reason: 'まだ使う', requestId: rid }, { open: true }));
  assert.deepEqual([r.ok, r.state, r.code], [true, 'withdrawn', 'NdUp']);
  assert.equal((await asEditTx((d) => ND.withdrawNeDelete(d, { actor: 'naka@test', itemId: item, reason: 'まだ使う', requestId: rid }, { open: true }))).replayed, true);
  assert.equal((await errOf(async () => asEditTx((d) => ND.withdrawNeDelete(d, { actor: 'naka@test', itemId: item, reason: 'まだ使う', requestId: crypto.randomUUID() }, { open: true }))))?.reason, 'not_live');
  const prod = B1.exports.find((e) => e.kind === 'products');
  const e = await errOf(async () => asEditTx((d) => ND.downloadNeDelete(d, { actor: 'naka@test', exportId: prod.export_id })));
  assert.equal(e?.reason, 'file_changed'); assert.match(e.message, /NdUp withdrawn/);
  const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', 'main', 'r-w1', 'active') returning listing_id`)).listing_id;
  await q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('NdUp')]);
  await q('delete from core.listing_components where listing_id = $1', [lid]);
});

// ═══════════ [G] セットの「NE から消えた → やめる」 ═══════════
console.log('[G] セットの「NE から消えた → やめる」');
const goneCheck = async (code) => (await one('select ops.sku_gone_from_ne_check($1::bigint) as r', [await sid(code)])).r;
const cancelGone = (code, reason = 'NE から消す CSV で消した') => asEditTx(async (d) => cancelSkuGoneFromNe(d, { actor: 'naka@test', code, reason, requestId: crypto.randomUUID(), confirm: true }, { open: true }));
await ta('[G1] セット・中身が 3 日続けて NE に無い: 中身の単品は「セットの構成品」で止まる → セットをやめる (構成を外す・依頼の品目 gone) → 単品もやめられる', async () => {
  for (const day of [-2, -1, 0]) await feed(['set01', 'c01', 'c02', 'n01', 'set03', 'set04'], { day });
  await neCodes(ALL_CODES.filter((c) => !['n02', 'set01', 'c01', 'c02', 'n01', 'set03', 'set04'].includes(c)), { collided: ['n08'] });   // 今朝の照合の NE のコードの台帳にも無い
  const s = await goneCheck('set01');
  assert.equal(s.ok, true, JSON.stringify(s.problems)); assert.equal(s.sku_kind, 'set');
  assert.deepEqual((await goneCheck('c01')).problems.map((p) => p.reason), ['set_component']);
  const r = await cancelGone('set01');
  assert.equal(r.state, 'cancelled');
  assert.deepEqual(r.components_removed.map((x) => [x.code, x.qty]), [['c01', 1], ['c02', 2]]);
  assert.equal(r.ne_del_items.length, 1);
  assert.equal(await regOf('set01'), 'cancelled');
  assert.equal((await one('select count(*)::int as n from core.sku_components where parent_sku_id = $1', [await sid('set01')])).n, 0);
  assert.equal((await one("select state from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = 'set01'")).state, 'gone');
  // 外した構成は変更の記録に残る (0026・画面のロール = 約束の人)
  const ev = await q("select operation, actor_id from events.master_change_events where entity_type = 'sku_component' and operation = 'DELETE' and (entity_key ->> 'parent_sku_id')::bigint = $1", [await sid('set01')]);
  assert.equal(ev.length, 2); assert.ok(ev.every((x) => x.actor_id === 'naka@test'));
  assert.equal((await goneCheck('c01')).ok, true);
  for (const c of ['c01', 'c02', 'n01']) assert.equal((await cancelGone(c)).state, 'cancelled', c);
  assert.equal((await one("select count(*)::int as n from ops.ne_del_items where state = 'gone'")).n, 4);
});
await ta('[G2] やめたセット (親) に構成は作れない (sku_cancelled)・夜間ロードは NE の古い行からやめたセットの構成を戻さない (parent_cancelled で飛ばす・止まらない)', async () => {
  const e = await errOf(async () => q(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'manual')`, [await sid('set01'), await sid('n13')]));
  assert.match(e?.message ?? '', /sku_cancelled: やめた \(cancelled\) セット set01/);
  const r = await runInitialLoad(db, PLAN, { log: quiet, runId: 'nedel_load_2', host: 'test' });
  assert.equal(r.ok, true, r.error);
  assert.equal((await one('select count(*)::int as n from core.sku_components where parent_sku_id = $1', [await sid('set01')])).n, 0);
  const sk = r.unresolved?.set_components || [];
  assert.ok(sk.some((x) => x.reason_code === 'parent_cancelled' && x.parent === 'set01'), JSON.stringify({ sk, sec: r.sections?.set_components, owned: r.company_owned }).slice(0, 1500));
  assert.equal(await regOf('set01'), 'cancelled');
});
await ta('[G3] セット自身の開いている構成の依頼 = やめられない (own_component_request)', async () => {
  await pg.query('begin');
  try {
    // 守り (依頼のセット自身が依頼の途中 = sku_ne_deleting) は持ち主の印で外して置く (試験だけ・取引ごと巻き戻す)
    await q('alter table ops.sku_component_requests disable trigger trg_sku_component_requests_not_cancelled');
    await q(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id) values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`,
      [await sid('set03'), JSON.stringify([{ sku_id: Number(await sid('c04')), code: 'c04', qty: 1, sort: 1 }]), hex('6')]);
    assert.deepEqual((await goneCheck('set03')).problems.map((p) => p.reason), ['own_component_request']);
  } finally { await pg.query('rollback'); }
  assert.equal((await goneCheck('set03')).ok, true);
});

await ta('[G4] 画面のロールでない呼び手 (CLI = master_ops・持ち主) がセットをやめる: 外した構成の変更の記録 = 呼んだ人・request_id・理由 / 呼び手の取引の設定は元に戻す', async () => {
  const rid = crypto.randomUUID();
  await pg.query('begin');
  try {
    await pg.query("select set_config('core.actor_id', 'before@test', true), set_config('core.reason', '前の理由', true)");
    const r = (await pg.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint) as r', [rid, 'cli@test', 'CLI でやめた', await sid('set04')])).rows[0].r;
    assert.deepEqual(r.components_removed.map((x) => x.code), ['c05']);
    assert.deepEqual(await one("select current_setting('core.actor_id', true) as a, current_setting('core.reason', true) as b"), { a: 'before@test', b: '前の理由' });
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
  const ev = await one("select actor_type, actor_id, request_id, reason_text, source_system from events.master_change_events where entity_type = 'sku_component' and operation = 'DELETE' and (entity_key ->> 'parent_sku_id')::bigint = $1", [await sid('set04')]);
  assert.deepEqual(ev, { actor_type: 'human', actor_id: 'cli@test', request_id: rid, reason_text: 'CLI でやめた', source_system: 'sku_gone_cancel' });
});

// ═══════════ [L] 夜間ロード (依頼の途中) ═══════════
console.log('[L] 夜間ロード');
await ta('[L1] 依頼の途中の子 (set02 の c03)・出品の構成 (n10) = ロードは飛ばして止まらない (今の行は残す)', async () => {
  await feed(['set03', 'set04'], { day: 0 });   // 一番新しい取得 = set03 のほかは全部ある (set02 も)
  await neCodes(ALL_CODES.filter((c) => !['n02', 'set01', 'c01', 'c02', 'n01', 'set03', 'set04'].includes(c)), { collided: ['n08'] });
  const r0 = await asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['set02', 'c03', 'n10'], reason: '試験', confirm: true }, { open: true, stockReader: zeroReader }));
  assert.equal(r0.ok, true);
  const PLAN2 = { ...PLAN, listings: [...PLAN.listings, rakuten('r-n10', ['n10'])] };
  const r = await runInitialLoad(db, PLAN2, { log: quiet, runId: 'nedel_load_3', host: 'test' });
  assert.equal(r.ok, true, r.error);
  assert.equal((await one('select count(*)::int as n from core.sku_components where parent_sku_id = $1 and child_sku_id = $2', [await sid('set02'), await sid('c03')])).n, 1, '今の構成の行は残す');
  assert.ok((r.unresolved?.set_components || []).some((x) => x.reason_code === 'child_ne_deleting' && x.child === 'c03'));
  assert.equal((await one(`select count(*)::int as n from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id where l.listing_code = 'r-n10'`)).n, 0);
});

// ═══════════ [R] 断られたとき = 取扱中止の CSV ═══════════
console.log('[R] 断られたとき');
const itemOf = async (code) => (await one("select i.item_id::text as id, i.export_id::text as e from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = $1 and i.state = 'live' and i.action = 'delete'", [code]));
async function backdateDownload(exportId, days) {
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.ne_del_protocol', '1', true)");
    await q(`update ops.ne_del_exports set first_downloaded_at = now() - make_interval(days => $2), first_downloaded_by = 'naka@test', download_count = 1 where export_id = $1 and first_downloaded_at is null`, [exportId, days]);
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
}
await ta('[R1] ダウンロードから 3 日たっても NE にある単品 = 断られたかも → 取扱中止の CSV (ne-csv.mjs と同じバイト)・消す品目 handled・取扱中止の品目 recorded・参照の守りが外れる', async () => {
  const n10 = await itemOf('n10');
  const tooSoon = (await one('select ops.ne_del_refused_check($1::bigint) as r', [n10.id])).r;
  assert.ok(tooSoon.problems.some((p) => p.reason === 'not_downloaded'));
  await backdateDownload(n10.e, 4);
  await feed(['set03', 'set04'], { day: 0 });   // ダウンロードの後の完全な取得にまだある
  const rc = (await one('select ops.ne_del_refused_check($1::bigint) as r', [n10.id])).r;
  assert.equal(rc.ok, true, JSON.stringify(rc.problems));
  const set02 = await itemOf('set02');
  assert.ok((await one('select ops.ne_del_refused_check($1::bigint) as r', [set02.id])).r.problems.some((p) => p.reason === 'not_single'));
  const ov = await ND.readNeDelOverview(asEdit, null);
  assert.equal(ov.items.find((i) => i.item_id === n10.id).status.status, 'refused');
  const rid = crypto.randomUUID();
  const h = await asEditTx((d) => ND.buildHandling(d, { actor: 'naka@test', itemIds: [n10.id], reason: 'NE が断った', requestId: rid, confirm: true }, { open: true }));
  assert.deepEqual(h.export.codes, ['n10']);
  const f = await asEditTx((d) => ND.downloadNeDelete(d, { actor: 'naka@test', exportId: h.export.export_id }));
  assert.equal(f.bytes.toString('hex'), buildCsv(neCsvSpecOf('products', 'handling'), [{ ne_code: 'n10', cell: '1' }]).bytes.toString('hex'));
  assert.match(f.file_name, /^ne-handling-stop-/);
  assert.equal((await one('select state from ops.ne_del_items where item_id = $1', [n10.id])).state, 'handled');
  assert.deepEqual(await one("select action, state, replaces_item_id::text as r from ops.ne_del_items where export_id = $1", [h.export.export_id]), { action: 'handling', state: 'recorded', r: n10.id });
  assert.equal((await asEditTx((d) => ND.buildHandling(d, { actor: 'naka@test', itemIds: [n10.id], reason: 'NE が断った', requestId: rid, confirm: true }, { open: true }))).replayed, true);
  const e = await errOf(async () => asEditTx((d) => ND.buildHandling(d, { actor: 'naka@test', itemIds: [n10.id], reason: 'NE が断った', requestId: crypto.randomUUID(), confirm: true }, { open: true })));
  assert.equal(e?.reason, 'not_refused');
  const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', 'main', 'r-r1', 'active') returning listing_id`)).listing_id;
  await q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('n10')]);
});
await ta('[R2] 3 日たっていない = too_soon・ダウンロードの後の完全な取得がまだ = fetch_before_download (どちらも作らない)', async () => {
  await asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['n12'], reason: '試験', confirm: true }, { open: true, stockReader: zeroReader }));
  const n12 = await itemOf('n12');
  await asEditTx((d) => ND.downloadNeDelete(d, { actor: 'naka@test', exportId: n12.e }));
  const p = (await one('select ops.ne_del_refused_check($1::bigint) as r', [n12.id])).r.problems.map((x) => x.reason);
  assert.ok(p.includes('too_soon') && p.includes('fetch_before_download'), JSON.stringify(p));
  const before = await counts();
  assert.equal((await errOf(async () => asEditTx((d) => ND.buildHandling(d, { actor: 'naka@test', itemIds: [n12.id], reason: 'x', confirm: true }, { open: true }))))?.reason, 'not_refused');
  assert.deepEqual(await counts(), before);
});

await ta('[R3] 日をまたいで 3 日たっても、一番新しい完全な取得が 3 日の待機の期限 (ダウンロードした日 + 3 日の 0 時) より前 = fetch_too_soon (断られたかもと言わない)', async () => {
  await asEditTx((d) => ND.buildNeDelete(d, { actor: 'naka@test', codes: ['k01'], reason: '試験', confirm: true }, { open: true, stockReader: zeroReader }));
  const k01 = await itemOf('k01');
  // 3 日前の夜 (JST 23:00) にダウンロード = 期限は今日の 0 時
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.ne_del_protocol', '1', true)");
    await q(`update ops.ne_del_exports set first_downloaded_at = ($2::date + time '23:00') at time zone 'Asia/Tokyo', first_downloaded_by = 'naka@test', download_count = 1 where export_id = $1`, [k01.e, jstDay(-3)]);
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
  // 一番新しい取得を、ダウンロードの後・期限の前 (昨日の取得) に置き換える (取引の中だけ)
  await pg.query('begin');
  try {
    const y = await one(`select result_id, ne_generation_id, products_complete_at, setproducts_complete_at from ops.master_parent_gate_results
       where (least(products_complete_at, setproducts_complete_at) at time zone 'Asia/Tokyo')::date = $1::date order by result_id desc limit 1`, [jstDay(-1)]);
    assert.ok(y, '昨日の取得が無い');
    await pg.query("select set_config('ops.ne_absence_protocol', '1', true)");
    await q('update ops.master_ne_absence_mark set result_id = $1, ne_generation_id = $2, products_complete_at = $3, setproducts_complete_at = $4, fetch_day = $5::date where id = 1',
      [y.result_id, y.ne_generation_id, y.products_complete_at, y.setproducts_complete_at, jstDay(-1)]);
    const p = (await one('select ops.ne_del_refused_check($1::bigint) as r', [k01.id])).r.problems.map((x) => x.reason);
    assert.deepEqual(p, ['fetch_too_soon']);
    assert.equal((await one('select ops.ne_del_item_status($1::bigint) as r', [k01.id])).r.status, 'waiting');
  } finally { await pg.query('rollback'); }
  // 期限の後 (今日) の取得 = 断られたかも
  const ok = (await one('select ops.ne_del_refused_check($1::bigint) as r', [k01.id])).r;
  assert.equal(ok.ok, true, JSON.stringify(ok.problems));
});

// ═══════════ [O] 表の守りと権限 ═══════════
console.log('[O] 表の守りと権限');
await ta('[O1] 表は関数の外で書けない (持ち主の手も)・消せない・品目の状態は戻せない・ファイルの中身は変えられない', async () => {
  const id = (await one('select item_id from ops.ne_del_items order by item_id limit 1')).item_id;
  assert.match((await errOf(async () => q("update ops.ne_del_items set state = 'live' where item_id = $1", [id])))?.message ?? '', /ne_del_write/);
  assert.match((await errOf(async () => q('delete from ops.ne_del_items where item_id = $1', [id])))?.message ?? '', /消さない/);
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.ne_del_protocol', '1', true)");
    assert.match((await errOf(async () => q("update ops.ne_del_items set state = 'live' where item_id = $1 and state <> 'live'", [(await one("select item_id from ops.ne_del_items where state = 'gone' limit 1")).item_id])))?.message ?? '', /一方向/);
  } finally { await pg.query('rollback'); }
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.ne_del_protocol', '1', true)");
    assert.match((await errOf(async () => q("update ops.ne_del_exports set reason = 'x' where export_id = (select min(export_id) from ops.ne_del_exports)")))?.message ?? '', /中身は変えない/);
  } finally { await pg.query('rollback'); }
  assert.match((await errOf(async () => q('truncate ops.ne_del_items')))?.message ?? '', /./);
});
await ta('[O2] 権限: master_edit = 9 つの関数だけ (表は読めない・部品は呼べない)・watcher = 確かめと一覧 (作る・受け取るは 42501)', async () => {
  for (const f of NE_DEL_FUNCTIONS) assert.equal((await one(`select has_function_privilege('master_edit', $1::regprocedure, 'execute') as x`, [f])).x, true, f);
  for (const f of NE_DEL_OWNER_ONLY_FUNCTIONS) {
    assert.equal((await one(`select has_function_privilege('master_edit', $1::regprocedure, 'execute') as x`, [f])).x, false, f);
    assert.equal((await one(`select has_function_privilege('public', $1::regprocedure, 'execute') as x`, [f])).x, false, f);
  }
  assert.equal((await one(`select has_table_privilege('master_edit', 'ops.ne_del_exports', 'select') as x`)).x, false);
  const w = await as('watcher', async () => (await pg.query('select ops.ne_del_overview(null) as r')).rows[0].r);
  assert.ok(w.items.length > 0);
  await as('watcher', async () => (await pg.query('select ops.ne_del_check($1::bigint, null, null) as r', [await sid('n13')])).rows[0].r);
  const e = await errOf(async () => as('watcher', () => pg.query('select ops.ne_del_download($1, 1)', ['w@test'])));
  assert.equal(e?.code, '42501');
  // 🆕 #1690 Codex R1 Medium 1: watcher は file_bytes を直に読めない (ほかの列は読める)
  assert.equal((await errOf(async () => as('watcher', () => pg.query('select file_bytes from ops.ne_del_exports limit 1'))))?.code, '42501');
  assert.ok((await as('watcher', () => pg.query('select export_id, sha256 from ops.ne_del_exports'))).rows.length > 0);
});

// ═══════════ [K] アプリの在庫 (apps/master-edit/ne-delete-stock.mjs) ═══════════
console.log('[K] アプリの在庫');
const { readNeDeleteStock, readLogizardForDelete } = await import('../apps/master-edit/ne-delete-stock.mjs');
const pmlOk = (rows) => ({ ok: true, runId: 'run1', neSyncedAt: '2030-01-01T00:00:00Z', map: new Map(Object.entries(rows)) });
const lzOk = (rows, stale = false) => ({ ok: true, asOf: '2030-01-01T00:00:00Z', stale, map: new Map(Object.entries(rows)) });
const boOk = (rows) => ({ ok: true, map: new Map(Object.entries(rows)) });
const SK = [{ code: 'Ab-1', code_norm: 'ab-1' }, { code: 'b2', code_norm: 'b2' }];
await ta('[K1] 値: NE の在庫 = 総在庫数 − FBA・引当・NE の発注残・FBA・ロジザード (code_norm)・注文残 (発注アプリの鍵) / 出どころを残す', async () => {
  const m = await readNeDeleteStock(SK, { readers: {
    pml: async () => pmlOk({ 'ab-1': { total: 5, fba: 2, allocated: 1, onOrder: 4 }, b2: { total: 0, fba: 0, allocated: 0, onOrder: 0 } }),
    logizard: async () => lzOk({ 'ab-1': { qty: 7, allocated: 2 } }), backorders: () => boOk({ 'ab-1': 3 }) } });
  const a = m.get('ab-1');
  assert.deepEqual([a.ne_own, a.ne_allocated, a.ne_on_order, a.fba, a.logizard, a.logizard_allocated, a.po_backorder], [3, 1, 4, 2, 7, 2, 3]);
  assert.equal(a.sources.pml_run, 'run1');
  const b = m.get('b2');
  assert.deepEqual([b.ne_own, b.ne_allocated, b.ne_on_order, b.fba, b.logizard, b.logizard_allocated, b.po_backorder], [0, 0, 0, 0, 0, 0, 0]);
});
await ta('[K2] 読めない = unreadable (0 にしない): 商品管理リストが読めない / 商品が無い / 値が整数でない / ロジザードの写しが古い・読めない / 注文残が読めない', async () => {
  const zeroRow = { total: 0, fba: 0, allocated: 0, onOrder: 0 };
  const run = async (r) => (await readNeDeleteStock([SK[1]], { readers: { pml: async () => pmlOk({ b2: zeroRow }), logizard: async () => lzOk({}), backorders: () => boOk({}), ...r } })).get('b2');
  assert.match((await run({ pml: async () => ({ ok: false, error: '古い' }) })).unreadable, /古い/);
  assert.match((await run({ pml: async () => pmlOk({}) })).unreadable, /商品管理リストにこの商品が無い/);
  assert.match((await run({ pml: async () => pmlOk({ b2: { ...zeroRow, allocated: null } }) })).unreadable, /読めない/);
  assert.match((await run({ pml: async () => pmlOk({ b2: { ...zeroRow, fba: 3 } }) })).unreadable, /総在庫数 − FBA在庫数/);
  assert.match((await run({ logizard: async () => lzOk({}, true) })).unreadable, /古い/);
  assert.match((await run({ logizard: async () => ({ ok: false, error: 'ロジザード無し' }) })).unreadable, /ロジザード無し/);
  assert.match((await run({ logizard: async () => lzOk({ b2: { bad: 'ロジザードの在庫数・引当数が読めない行' } }) })).unreadable, /読めない行/);
  assert.match((await run({ backorders: () => boOk({ b2: 1.5 }) })).unreadable, /注文残の値が読めない/);
  // 🆕 #1690 Codex R2 Medium 2: 鍵があって読めない値 (null・NaN・空・負) = 読めない (0 にしない)・鍵が無い = 0
  for (const v of [null, NaN, '', -1, 'x', undefined]) assert.match((await run({ backorders: () => boOk({ b2: v }) })).unreadable ?? '', /注文残の値が読めない/, String(v));
  assert.equal((await run({ backorders: () => boOk({ b2: '0' }) })).po_backorder, 0);
  assert.equal((await run({ backorders: () => boOk({ b2: 4 }) })).po_backorder, 4);
  assert.equal((await run({ backorders: () => boOk({ other: 4 }) })).po_backorder, 0);
  assert.match((await run({ backorders: () => ({ ok: false, error: '台帳無し' }) })).unreadable, /台帳無し/);
  assert.equal((await run({})).unreadable, undefined);
});

await ta('[K3] ロジザードの今の在庫と引当 (削除だけの読み方): 商品ID ごとに在庫数と引当数を足す・1 行でも整数でない (空・小数・負・文字・NaN) = その商品は読めない (0 にしない)・写しが無い / 古い', async () => {
  const at = new Date(Date.now() - 10 * 60e3).toISOString();
  const rows = [{ 商品ID: 'AB-1', 在庫数: 0, 引当数: 5 }, { 商品ID: 'ab-1', 在庫数: '2', 引当数: 0 }, { 商品ID: 'c3', 在庫数: 0, 引当数: 0 },
    { 商品ID: 'bad1', 在庫数: NaN, 引当数: 0 }, { 商品ID: 'bad2', 在庫数: 1.5, 引当数: 0 }, { 商品ID: 'bad3', 在庫数: '', 引当数: 0 }, { 商品ID: 'bad4', 在庫数: 0, 引当数: null },
    { 商品ID: 'bad5', 在庫数: -1, 引当数: 0 }, { 商品ID: 'bad6', 在庫数: 'x', 引当数: 0 }, { 商品ID: 'bad6', 在庫数: 3, 引当数: 0 }];
  const r = await readLogizardForDelete({ reader: async () => ({ capturedAt: at, rows }) });
  assert.equal(r.ok, true); assert.equal(r.stale, false);
  assert.deepEqual(r.map.get('ab-1'), { qty: 2, allocated: 5 });
  assert.deepEqual(r.map.get('c3'), { qty: 0, allocated: 0 });
  for (const k of ['bad1', 'bad2', 'bad3', 'bad4', 'bad5', 'bad6']) assert.ok(r.map.get(k)?.bad, k);
  const s = await readNeDeleteStock([{ code: 'AB-1', code_norm: 'ab-1' }, { code: 'bad1', code_norm: 'bad1' }, { code: 'none', code_norm: 'none' }],
    { readers: { pml: async () => pmlOk({ 'ab-1': { total: 0, fba: 0, allocated: 0, onOrder: 0 }, bad1: { total: 0, fba: 0, allocated: 0, onOrder: 0 }, none: { total: 0, fba: 0, allocated: 0, onOrder: 0 } }),
      logizard: async () => r, backorders: () => boOk({}) } });
  assert.deepEqual([s.get('ab-1').logizard, s.get('ab-1').logizard_allocated], [2, 5]);   // 在庫 2・引当 5 = DB が断る
  assert.ok(s.get('bad1').unreadable);
  assert.deepEqual([s.get('none').logizard, s.get('none').logizard_allocated], [0, 0]);   // 写しに行が無い = 0
  assert.equal((await readLogizardForDelete({ reader: async () => null })).ok, false);
  assert.equal((await readLogizardForDelete({ reader: async () => { throw new Error('壊れた'); } })).ok, false);
  assert.equal((await readLogizardForDelete({ reader: async () => ({ capturedAt: '2020-01-01T00:00:00Z', rows }) })).stale, true);
});

// ═══════════ [S] 画面 (本物の router) ═══════════
console.log('[S] 画面');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { default: router, __setPgClientFactory, __setShippingRatesProvider, __setNeDelStockReader } = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/nedel';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/nedel';
process.env.MASTER_EDITORS = 'naka@test,edit@test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
__setNeDelStockReader(zeroReader);
let EMAIL = 'naka@test';
let APPS = ['master-edit'];   // 発注アプリの権限 (purchase-orders) が無い = 注文残の数を見せない
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: EMAIL, displayName: '中原', role: 'user', allowedApps: APPS }; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const B = `${ORIGIN}/apps/master-edit`;
const api = async (p, body) => { const r = await fetch(`${B}${p}`, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: ORIGIN }, body: JSON.stringify(body || {}) }); return r; };
const apiJ = async (p, body) => { const r = await api(p, body); return { status: r.status, j: await r.json() }; };
await ta('[S1] 「NE から消す」の画面: 流れ・入れる欄・依頼の途中 (いまの様子)・ファイル・終わった依頼 / 名簿に無い人 = 見るだけ', async () => {
  EMAIL = 'naka@test';
  const r = await fetch(`${B}/ne-delete?codes=n13`);
  assert.equal(r.status, 200);
  const h = await r.text();
  assert.match(h, /<h1>NE から消す<\/h1>/); assert.match(h, /id="nd-codes"[^>]*>n13</); assert.match(h, /data-can="1"/);
  assert.match(h, /id="nd-live"/); assert.match(h, /c03/); assert.match(h, /終わった依頼/); assert.match(h, /me-nedel\.js/);
  assert.match(h, /aria-current="page"[^>]*>.*NE から消す|NE から消す<\/span><\/a>/);
  EMAIL = 'edit@test';
  const h2 = await (await fetch(`${B}/ne-delete`)).text();
  assert.match(h2, /data-can=""/); assert.match(h2, /MASTER_DECISION_APPROVERS|名簿/);
  EMAIL = 'naka@test';
});
await ta('[S2] API: 名簿に無い = 403・確かめ (読むだけ)・作る (409 / 200)・受け取る = POST・Shift_JIS・sha256・GET は無い', async () => {
  EMAIL = 'edit@test';
  assert.equal((await api('/api/ne-delete/check', { codes: ['n13'] })).status, 403);
  assert.equal((await api('/api/ne-delete/build', { codes: ['n13'], reason: 'x', confirm: true })).status, 403);
  EMAIL = 'naka@test';
  const c = await apiJ('/api/ne-delete/check', { codes: ['n13', 'n03', 'zz-none'] });
  assert.equal(c.status, 200); assert.equal(c.j.ok, true); assert.equal(c.j.all_ok, false); assert.deepEqual(c.j.missing, ['zz-none']);
  assert.deepEqual(c.j.rows.map((x) => [x.code, x.ok]).sort((a, b) => (a[0] < b[0] ? -1 : 1)), [['n03', false], ['n13', true]]);
  assert.equal(c.j.rows.find((x) => x.code === 'n03').problems[0].word, 'NE に在庫がある');
  const ng = await apiJ('/api/ne-delete/build', { codes: ['n03'], reason: '販売をやめた', confirm: true, request_id: crypto.randomUUID() });
  assert.equal(ng.status, 409); assert.equal(ng.j.reason, 'not_deletable');
  const ok = await apiJ('/api/ne-delete/build', { codes: ['n13'], reason: '販売をやめた', confirm: true, request_id: crypto.randomUUID() });
  assert.equal(ok.status, 200, JSON.stringify(ok.j)); assert.equal(ok.j.exports[0].kind, 'products');
  const id = ok.j.exports[0].export_id;
  assert.equal((await fetch(`${B}/api/ne-delete/exports/${id}/download`)).status, 404, 'GET では受け取れない');
  const d = await api(`/api/ne-delete/exports/${id}/download`, {});
  assert.equal(d.status, 200);
  assert.equal(d.headers.get('content-type'), 'text/csv; charset=Shift_JIS');
  const buf = Buffer.from(await d.arrayBuffer());
  assert.equal(buf.toString('hex'), ND.delCsvBytes(['n13']).toString('hex'));
  assert.equal(d.headers.get('x-content-sha256'), ND.sha256Hex(buf));
  assert.match(d.headers.get('content-disposition'), /attachment; filename="ne-delete-products-\d{8}-\d+\.csv"/);
  EMAIL = 'edit@test';
  assert.equal((await api(`/api/ne-delete/exports/${id}/download`, {})).status, 403);
  EMAIL = 'naka@test';
});
await ta('[S3] 商品の画面: 依頼の途中 = 「NE から消す」の箱 (いまの様子・画面へ) / ふつうの商品 = 「NE から消せるか確かめる」(名簿の人だけ) / セットの「NE から消えた (やめる)」', async () => {
  const h = await (await fetch(`${B}/sku/n13`)).text();
  assert.match(h, /id="nedel-box"/); assert.match(h, /依頼の途中/);
  const h2 = await (await fetch(`${B}/sku/n09`)).text();
  assert.match(h2, /id="nedel-box"/); assert.match(h2, /ne-delete\?codes=n09/);
  EMAIL = 'edit@test';
  assert.doesNotMatch(await (await fetch(`${B}/sku/n09`)).text(), /id="nedel-box"/);
  EMAIL = 'naka@test';
  const hs = await (await fetch(`${B}/sku/set03`)).text();
  assert.match(hs, /id="gone-box"/); assert.match(hs, /セットは、やめると構成/); assert.match(hs, /NE から消えました。Company DB でも/);
});

await ta('[S4] 注文残の数は発注アプリの権限がある人だけ: 確かめの答え (在庫・理由)・作るの断りの理由に数が出る / 権限が無い = 数を外して「注文残あり」(確かめ自体は同じく断る)', async () => {
  const poReader = async (skus) => new Map(skus.map((s) => [s.code_norm, { ...zero(), po_backorder: s.code_norm === 'n09' ? 7 : 0 }]));
  __setNeDelStockReader(poReader);
  try {
    EMAIL = 'naka@test';
    APPS = ['master-edit'];
    const c = await apiJ('/api/ne-delete/check', { codes: ['n09'] });
    assert.equal(c.status, 200); assert.equal(c.j.all_ok, false);
    const row = c.j.rows[0];
    assert.deepEqual(row.problems.map((p) => p.reason), ['stock_app']);
    assert.match(row.problems[0].text, /発注アプリの注文残あり/);
    assert.equal(row.stock.app.po_backorder, null); assert.equal(row.stock.app.po_backorder_hidden, true);
    assert.equal(c.j.app_stock[row.sku_id].po_backorder, null);
    assert.doesNotMatch(JSON.stringify(c.j), /注文残 7|"po_backorder":7/);
    const b = await apiJ('/api/ne-delete/build', { codes: ['n09'], reason: '販売をやめた', confirm: true, request_id: crypto.randomUUID() });
    assert.equal(b.status, 409); assert.equal(b.j.reason, 'not_deletable');
    assert.match(b.j.error, /発注アプリの注文残あり/); assert.doesNotMatch(b.j.error, /注文残 7/);
    APPS = ['master-edit', 'purchase-orders'];
    const c2 = await apiJ('/api/ne-delete/check', { codes: ['n09'] });
    assert.match(c2.j.rows[0].problems[0].text, /発注アプリの注文残 7/);
    assert.equal(c2.j.rows[0].stock.app.po_backorder, 7);
    const b2 = await apiJ('/api/ne-delete/build', { codes: ['n09'], reason: '販売をやめた', confirm: true, request_id: crypto.randomUUID() });
    assert.equal(b2.status, 409); assert.match(b2.j.error, /発注アプリの注文残 7/);
  } finally { __setNeDelStockReader(zeroReader); APPS = ['master-edit']; }
  assert.equal((await one("select count(*)::int as n from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = 'n09'")).n, 0);
});

// ═══════════ [U] 画面の JS ═══════════
console.log('[U] 画面の JS');
if (process.env.MASTER_EDIT_UI_SKIP === '1') console.log('  ⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の JS は飛ばす');
else {
  let browser;
  try { const { chromium } = await import('playwright'); browser = await chromium.launch(); }
  catch (e) { console.error(`  NG  画面の JS の試験を動かせない (Playwright / Chromium): ${e.message}`); process.exitCode = 1; }
  if (browser) {
    await ta('[U1] ?codes= で開く = すぐ確かめる → 止まる理由 / 消せる → 理由 → 確かめの窓 → 作る → ダウンロード (sha256 を照らして保存)', async () => {
      const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: 'ja-JP', acceptDownloads: true });
      const p = await ctx.newPage();
      const errors = []; p.on('pageerror', (e) => errors.push(e.message));
      p.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
      try {
        await p.goto(`${B}/ne-delete?codes=n03`);
        await p.waitForSelector('#nd-table tbody tr', { timeout: 15000 }).catch(async (e) => { throw new Error(`${e.message} / errors=${JSON.stringify(errors)} / msg=${await p.locator('#nd-msg').textContent()}`); });
        assert.match(await p.locator('#nd-table tbody').textContent(), /NE に在庫がある/);
        assert.equal(await p.locator('#nd-go-box').isHidden(), true);
        await p.fill('#nd-codes', 'n11');
        await p.click('#nd-check');
        await p.waitForFunction(() => /消せます/.test(document.querySelector('#nd-table tbody')?.textContent || ''), null, { timeout: 15000 });
        assert.equal(await p.locator('#nd-build').isDisabled(), true);
        await p.fill('#nd-reason', '販売をやめた (試験)');
        assert.equal(await p.locator('#nd-build').isDisabled(), false);
        await p.click('#nd-build');
        assert.equal(await p.locator('#nd-bg').evaluate((el) => el.classList.contains('on')), true);
        await p.click('#nd-bg-ok');
        await p.waitForSelector('#nd-built .filecard button', { timeout: 15000 });
        const [dl] = await Promise.all([p.waitForEvent('download', { timeout: 15000 }), p.click('#nd-built .filecard button')]);
        const file = await dl.path();
        assert.equal(fs.readFileSync(file).toString('hex'), ND.delCsvBytes(['n11']).toString('hex'));
        assert.equal((await one("select count(*)::int as n from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = 'n11' and i.state = 'live'")).n, 1);
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
