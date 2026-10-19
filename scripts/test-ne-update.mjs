/**
 * test-ne-update.mjs — NE へ送る (migration 0093・AI_reference CompanyDB構想/25 の PR-2・2026-10-19 の方針 = 編集は自由・自社 ≠ 今朝の NE の欄だけを CSV にして NE の API で上げる)
 *
 * 固定する契約:
 *   [M] migration: security definer の関数 = search_path = pg_catalog, pg_temp・public の実行権なし・一時の表なし・既にある表と関数を作り直さない
 *   [F] CSV: 1 欄のファイル = apps/master-decisions/ne-csv.mjs の buildCsv と同じバイト (見出し・UTF-8・CRLF・引用符)・セルの決まり (DB = JS の COLUMNS の cell)・列の表 (見出し・持ち主のキー = SINGLE_FIELDS / SET_FIELDS)
 *   [U] 差の一覧 (ops.ne_upd_units):
 *       U1 ポータルで直した (saveSku) 原価・売価・名前が今朝の NE と違う = send (セル・NE の書き方)・夜間ロードの値の差は出さない・同じ値に戻した (A→B→A) は出さない
 *       U2 マスタの判断: 変更の後の「差を残す」= decided・fix_ne の承認の目標が違う = decision_other_value / 同じ = send・変更より前の判断は見ない
 *       U3 NE から消す依頼の途中 = deleting・新商品の途中 (draft) = new_product・書けない値 (名前 = コード) = name_is_code・持ち主が company でない欄は出さない
 *       U4 コードの書き方: NE = 小文字・ロジザード = 大文字 → ロジザードの書き方 (spelling_fix)・NE が 2 つ (collided) + ロジザード 1 つ → ロジザードの書き方 /
 *          ロジザードに無い = ne_collided・ロジザードが 2 つ = lz_many・セットは NE の書き方のまま
 *       U5 書き方だけの行 (spelling): 値の差が無い単品で NE の書き方 ≠ ロジザード → 原価の欄を NE と同じ値で 1 つ
 *       U6 今朝の NE の取得でない (昨日の取得) = ne_not_today・段階が new_open でない = not_open
 *   [B] 作る (ops.ne_upd_build): 単品とセットは別のファイル・確かめていない欄は空欄にしない (組ごとのファイル)・確かめた欄 (ops.ne_upd_verified) は空欄で 1 ファイル・
 *       送った後は「送っている途中」(もう一度作っても入らない)・値をまた直した = 新しい値で入る・自動は同じ今朝の回で 1 回 (前の答え)・
 *       100 行を超える = held・前のファイルを NE が受け付け中 = 自動は held / 今すぐは busy (頼みは残る)・結果 (ok / failed) と回の状態・
 *       翌朝: NE が同じになった = 一覧から消える / まだ違う = もう一度 send / 2 回 NE が受け付けたのにまだ違う = stuck
 *   [D] 画面: 今すぐの頼み (同じ request_id = 前の答え・待っている頼みがあれば 1 つ)・CSV で受け取る (作った日だけ・最初のダウンロードを残す)・実機の試しの記録
 *   [W] 送り手 (scripts/company-db/ne-update-send.mjs): 止めてある (NE_UPDATE_UPLOAD が無い) = 上げない・差の数だけ / 上げる (NE の API は試験の偽物)・
 *       受付の結果を見直す・NE が断った = ❌・まとめの 1 行
 *   [P] 権限: master_edit = 画面の 6 つの関数だけ (作る・結果は不可)・watch_writer = 送り手の 5 つだけ (今すぐの頼み・受け取りは不可)・表はだれも直接書けない (持ち主でも印が無ければ断る)
 * 🚨 実 PostgreSQL の 2 接続での待ち合い (作る × record_ne_values・作る × 保存) は test-ne-update-pg.mjs (TEST_PG_URL があるときだけ)
 * 使い方: node scripts/test-ne-update.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { buildCsv, specOf, COLUMNS } from '../apps/master-decisions/ne-csv.mjs';
import { SINGLE_FIELDS, SET_FIELDS } from '../lib/master-write.mjs';
import * as W from '../lib/master-write.mjs';
import * as S from './company-db/ne-update-send.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIG = path.join(ROOT, 'db', 'company', 'migrations');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const watchdog = setTimeout(() => { console.error('  NG  試験が 600 秒で終わらない (止める)'); process.exit(1); }, 600000);
watchdog.unref();

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const quiet = () => {};
const MIGF = fs.readdirSync(MIG).find((f) => /^\d{4}_ne_update_send\.sql$/.test(f));
const SQL = fs.readFileSync(path.join(MIG, MIGF), 'utf8').replace(/\r\n/g, '\n');

// ═══════════ [M] migration ═══════════
console.log('[M] migration');
await ta('[M1] security definer の関数 = search_path = pg_catalog, pg_temp・public の実行権を外す・一時の表なし・既にある関数を作り直さない', async () => {
  assert.doesNotMatch(SQL, /\b(create\s+(temp|temporary)\s+table|on commit drop)\b/i);
  assert.doesNotMatch(SQL, /create or replace function/i, '既にある関数を作り直さない (足すだけ)');
  const defs = [...SQL.matchAll(/create function ([a-z_.]+)\(([^)]*)\)[^$]*?as \$\$/g)];
  assert.ok(defs.length >= 15, `関数が ${defs.length}`);
  for (const d of defs) {
    if (/security definer/.test(d[0])) assert.match(d[0], /set search_path = pg_catalog, pg_temp/, `${d[1]} の search_path`);
    assert.ok(SQL.includes(`revoke all on function ${d[1]}(`), `${d[1]} の public の実行権を外していない`);
  }
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
const jsonOf = (r) => (typeof r === 'string' ? JSON.parse(r) : r);

// ═══════════ [F] CSV の形 ═══════════
console.log('[F] CSV の形');
await ta('[F1] 列の表 = ne-csv.mjs の COLUMNS (見出し・種類・欄)・持ち主のキー = lib/master-write.mjs の SINGLE_FIELDS / SET_FIELDS', async () => {
  const cols = await q('select kind, col, header, keys from ops.ne_upd_cols() order by kind, ord');
  for (const c of cols) {
    const spec = specOf(c.kind, c.col);
    assert.ok(spec, `${c.kind}:${c.col} が ne-csv.mjs に無い`);
    assert.equal(c.header, spec.ne);
    const F = Object.values(c.kind === 'sets' ? SET_FIELDS : SINGLE_FIELDS).find((f) => f.csvCol === c.col);
    assert.ok(F, `${c.kind}:${c.col} の画面の欄`);
    assert.deepEqual([...c.keys].sort(), [...F.keys].sort(), `${c.kind}:${c.col} の持ち主のキー`);
  }
  // 代表 (parent) は入れない (商品の画面で変えない)・在庫の列も無い
  assert.deepEqual(Object.keys(COLUMNS).filter((k) => !cols.some((c) => `${c.kind}:${c.col}` === k)), ['products:parent']);
});
await ta('[F2] セルの決まり (DB) = ne-csv.mjs の COLUMNS の cell (JS)', async () => {
  const cases = [['name', '商品 A'], ['name', ''], ['name', 'empty'], ['name', 'a\tb'], ['name', '😀'], ['name', 'x'.repeat(256)], ['name', 'あ'.repeat(255)], ['name', 'Ab-1'],
    ['handling', 'active'], ['handling', 'discontinued'], ['handling', 'unknown'], ['tax_rate', 0.1], ['tax_rate', 0.08], ['tax_rate', 0.05],
    ['cost', 1], ['cost', 999999999], ['cost', 0], ['cost', 1000000000], ['cost', 12.5], ['standard_price_jpy', 1200], ['primary_supplier', ['0002']], ['primary_supplier', ['12']],
    ['primary_supplier', []], ['primary_supplier', ['0001', '0002']]];
  for (const [col, v] of cases) {
    const db1 = jsonOf((await one('select ops.ne_upd_cell($1, $2::jsonb, $3) as r', [col, JSON.stringify(v), 'ab-1'])).r);
    const spec = specOf('products', col);
    const jsV = col === 'primary_supplier' ? (Array.isArray(v) && v.length === 1 ? v[0] : null) : v;
    let js = spec.cell(jsV);
    if (col === 'name' && js.ok && /^ab-1$/i.test(v)) js = { ok: false };   // 名前 = コード (judge が別に見る)
    if (col === 'primary_supplier' && (!Array.isArray(v) || v.length !== 1)) js = { ok: false };
    assert.equal(db1.ok, js.ok, `${col} ${JSON.stringify(v)}: DB ${JSON.stringify(db1)} / JS ${JSON.stringify(js)}`);
    if (js.ok) assert.equal(db1.cell, js.cell, `${col} ${JSON.stringify(v)}`);
  }
});

// ═══════════ 商品と今朝の NE ═══════════
const single = (code, over = {}) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...over });
const setSku = (code) => ({ ...single(code), kind: 'set', cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' }, standardPriceJpy: 3000 });
const SINGLES = ['a01', 'a02', 'a03', 'a04', 'a05', 'a06', 'a07', 'a08', 'a09', 'a10', 'u60e-10', 'u60e-05', 'k01', 'k02', 'k03', 'x01', 'x02', 'x03', 'q01', 'q02', 'q03', 'q04', 'q05', 'h01'];
const SETS = ['s01', 's02'];
const PLAN = {
  skus: [...SINGLES.map((c) => single(c)), ...SETS.map(setSku)],
  setComponents: [{ parentCode: 's01', childCode: 'k01', qty: 1, source: 'ne' }, { parentCode: 's02', childCode: 'k02', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [], variationGroups: [],
  suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }],
  supplierSkus: SINGLES.map((c) => ({ supplierCode: '0001', skuCode: c })), primarySuppliers: SINGLES.map((c) => ({ skuCode: c, supplierCode: '0001' })),
  reorder: { available: false, runId: null, reason: '試験' },
};
const lr = await runInitialLoad(db, PLAN, { log: quiet, runId: 'neupd_load_1', host: 'test', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const OWN = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', 'skus.handling': 'company', 'products.status': 'company', 'skus.tax_rate': 'company',
  'skus.tax_class': 'company', 'skus.standard_price': 'company', sku_costs: 'company', 'supplier_skus.is_primary': 'company', 'skus.sku_kind': 'company' };
await forceNewOpen(db, OWN);
const sid = async (code) => String((await one('select sku_id from core.skus where code_norm = core.norm_code($1)', [code])).sku_id);

/** Company DB の今の値 (D2) を JS で (試験の NE の値の元) */
async function cdbNow() {
  const rows = await q(`select k.code_norm, k.sku_kind, k.name, k.handling, k.tax_rate::float8 as tax_rate, k.standard_price_jpy::float8 as price,
      (select c.cost_jpy::float8 from core.sku_costs c where c.sku_id = k.sku_id and c.valid_to is null) as cost,
      (select coalesce(json_agg(s.code_norm order by s.code_norm), '[]') from core.supplier_skus x join core.suppliers s on s.supplier_id = x.supplier_id where x.sku_id = k.sku_id and x.is_primary) as sup
    from core.skus k where k.company_id = 1 and k.sku_kind in ('single', 'set') order by k.code_norm`);
  const yen = (v) => (v == null || Number(v) === 0 ? null : Number(v));
  return rows.map((r) => ({ code: r.code_norm, kind: r.sku_kind, cols: r.sku_kind === 'single'
    ? { name: (r.name || '').trim() || null, handling: r.handling || 'unknown', tax_rate: r.tax_rate, standard_price_jpy: yen(r.price), cost: yen(r.cost), primary_supplier: jsonOf(r.sup) }
    : { name: (r.name || '').trim() || null, standard_price_jpy: yen(r.price) } }));
}
let runN = 0;
const runIdAt = (ms) => `mc_${new Date(ms).toISOString().replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`;
const iso = (ms) => new Date(ms).toISOString();
let lastStart = 0;
const NE_STATE = new Map();
/**
 * 今朝の NE (照合の回 + NE のコードの書き方 + NE の値)。ne = { code: { col: 値 | { unknown: true } } } = Company DB と違う値 / spell = { code: 書き方 | [書き方…] (collided) }
 * startMs = 取得の始め (既定 = 今)
 */
async function seedNe({ ne = {}, spell = {}, startMs = null } = {}) {
  runN++;
  const start = Math.max(startMs ?? Date.now() + 5, lastStart + 10);   // 既定 = 今 (前に作ったファイルより後に始まった取得 = 翌朝)
  lastStart = start;
  const observed = start + 2000;
  const run = runIdAt(observed);
  await q('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)', [run, iso(observed)]);
  // NE の値 = 前の回の NE の値 (初めての商品は Company DB の今の値) + ne の上書き (上書きは次の回にも残る = NE がそうなった)
  const cur = (await cdbNow()).map((r) => {
    const prev = NE_STATE.get(r.code);
    const cols = { ...(prev || r.cols) };
    for (const [col, v] of Object.entries(ne[r.code] || {})) cols[col] = v;
    NE_STATE.set(r.code, cols);
    return { ...r, cols };
  });
  const codeEntries = cur.map((r) => {
    const s = spell[r.code];
    if (Array.isArray(s)) return { code_norm: r.code, kind: 'product', state: 'collided', ne_code: null, spellings: s };
    return { code_norm: r.code, kind: 'product', state: 'ok', ne_code: s || r.code, spellings: [s || r.code] };
  });
  await q('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: codeEntries })]);
  const entries = [];
  for (const r of cur) {
    for (const [col, v] of Object.entries(r.cols)) {
      const o = ne[r.code] && Object.hasOwn(ne[r.code], col) ? ne[r.code][col] : v;
      if (o && typeof o === 'object' && !Array.isArray(o) && o.unknown) entries.push({ code_norm: r.code, col, ne_kind: r.kind, state: 'unknown', reason: 'raw_unknown' });
      else entries.push({ code_norm: r.code, col, ne_kind: r.kind, state: 'ok', value: o });
    }
  }
  const nS = cur.filter((r) => r.kind === 'single').length, nT = cur.filter((r) => r.kind === 'set').length;
  const side = (s, n, extra) => ({ started_at: iso(s), finished_at: iso(s + 500), complete_rev: runN, fetched_rows: n, write_attempts: n, stored_rows: n, dropped_no_code: 0,
    dropped_missing_fields: 0, raw_rows: n, ...extra });
  const p = { compare_run_id: run, norm_version: 1, fetch: { fingerprint: 'f'.repeat(64), products: side(start, nS + nT, { codes: nS + nT, set_overlap: nT }), sets: side(start + 1, nT, { codes: nT }) }, entries };
  await q('select ops.record_ne_values($1::jsonb)', [JSON.stringify(p)]);
  return run;
}
let lzN = 0;
/** ロジザードの書き方 (ids = ロジザードの商品 ID の全部)。既定 = 単品のコードそのまま */
async function seedLz(over = {}, { hoursAgo = 1 } = {}) {
  lzN++;
  const exp = Date.now() - hoursAgo * 3600e3 + lzN * 10;
  const ids = [];
  for (const c of SINGLES) { const o = over[c]; if (o === null) continue; if (Array.isArray(o)) ids.push(...o); else ids.push(o || c); }
  const run = `lzd_${new Date(exp).toISOString().replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`;
  const jst = new Date(exp + 9 * 3600e3).toISOString().slice(0, 10);
  await q('select ops.record_lz_codes($1::jsonb)', [JSON.stringify({ lz_daily_run_id: run, export: { exported_at: iso(exp), stamp_at: iso(exp + 60000), stamp_date: jst, csv_sha256: crypto.randomBytes(32).toString('hex'), rows: ids.length }, ids })]);
}
/** 画面と同じ保存 (master_edit のロール・saveSku) */
async function save(code, values) {
  const id = await sid(code);
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  const cur = await W.readCurrent(db, id, today);
  const ev = (await one('select coalesce(max(event_id), 0)::text as id from events.master_change_events')).id;
  return as('master_edit', () => W.saveSku(db, { actor: 'naka@test', requestId: crypto.randomUUID(), code, reason: '試験', seen: { token: W.editTokenOf(cur), event_id: ev }, values }, { open: true }));
}
/** 夜間ロードの書き方で直接書く (source_system = company_db_load = わざと残した差) */
async function loadWrite(sql, p) {
  await pg.query('begin');
  try {
    await pg.query("select set_config('core.actor_type', 'system', true), set_config('core.source_system', 'company_db_load', true)");
    await pg.query(sql, p);
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
}
const units = async (codes = null, entry = 'bulk') => jsonOf((await one('select ops.ne_upd_units($1::bigint[], $2) as r', [codes ? await Promise.all(codes.map(sid)) : null, entry])).r);
const unitOf = (u, code, col) => u.units.find((x) => x.code_norm === code && x.col === col);
/** 作る (既定 = API で上げるファイル = 送り手が claimed)。upload = false = 人が受け取る CSV */
const build = async (how, upload = true) => jsonOf((await one('select ops.ne_upd_build($1, $2, $3) as r', [how, 'minipc-test', upload])).r);
/** 上げた結果。st = 2 (ok) / -1 (failed) / 1 / null (queued) */
const fileResult = (id, que, st, msg = null) => one('select ops.ne_upd_file_result($1, $2, $3, $4, $5) as r',
  [id, que, st === 2 ? 'ok' : st === -1 ? 'failed' : 'queued', st, msg]);
const csvOf = (f) => Buffer.from(f.bytes_b64, 'base64').toString('utf8');
/** 前の回のファイルを全部「NE が受け付けた」にする (次の試験の邪魔をしない) */
async function settleAll(status = 2) {
  for (const f of await q(`select f.file_id::text as id, coalesce(f.que_id, '1') as que from ops.ne_upd_files f join ops.ne_upd_sends s on s.send_id = f.send_id
     where f.claimed_by is not null and f.state in ('made', 'queued')`)) await fileResult(f.id, f.que, status);
}

await seedLz({ 'u60e-10': 'U60E-10', 'u60e-05': 'U60E-05', 'k03': ['K03', 'k03'] });
// 最初の回 = 昨日始まった取得 (今朝の取得でない = 何も送れない)。次の回から今日
await seedNe({ startMs: Date.now() - 86400e3 - 60000 });
const NOT_TODAY = await units();
await seedNe();

// ═══════════ [U] 差の一覧 ═══════════
console.log('[U] 差の一覧');
await ta('[U1] ポータルで直した原価・売価・名前 = send (セル・NE の書き方)・夜間ロードの値の差は出さない・A→B→A は出さない', async () => {
  await save('a01', { cost: { jpy: '150', reason: '試験' } });
  await save('a02', { standard_price: '1300' });
  await save('a03', { name: '商品 a03 改' });
  await save('a04', { standard_price: '1400' });
  await save('a04', { standard_price: '1000' });   // 今朝の NE と同じに戻した
  await loadWrite("update core.skus set standard_price_jpy = 1500 where code = 'a05'");   // 夜間ロードの値 (わざと残した差)
  const u = await units();
  assert.equal(u.ok, true, JSON.stringify(u.problems));
  const a1 = unitOf(u, 'a01', 'cost');
  assert.deepEqual([a1.status, a1.cell, a1.ne_code, a1.ne_value, a1.cdb_value, a1.role, a1.changed_by], ['send', '150', 'a01', 100, 150, 'change', 'naka@test']);
  assert.equal(unitOf(u, 'a02', 'standard_price_jpy').cell, '1300');
  assert.equal(unitOf(u, 'a03', 'name').cell, '商品 a03 改');
  assert.equal(unitOf(u, 'a04', 'standard_price_jpy'), undefined, '今朝の NE と同じ = 出さない');
  assert.equal(unitOf(u, 'a05', 'standard_price_jpy'), undefined, '夜間ロードの値 = 出さない');
  assert.equal(u.units.filter((x) => x.code_norm === 'a01').length, 1, '変えていない欄は出さない');
});
await ta('[U2] マスタの判断: 変更の後の「差を残す」= decided・fix_ne の目標が違う = decision_other_value / 同じ = send・変更より前の判断は見ない', async () => {
  const cand = async (code, col) => {
    const fp = crypto.createHash('sha256').update(`${code}|${col}|${Math.random()}`).digest('hex');
    await q(`insert into ops.master_decision_candidates (fingerprint, subject_key, code_norm, col, cls, reason_kind, semantic, print, resolutions, first_seen_run, first_seen_at, last_seen_run, last_seen_at)
      values ($1, $2, $3, $4, 'diff', 'company_owned', 'x', '{}'::jsonb, '["accept_difference", "fix_ne", "fix_cdb", "fix_input", "spec"]'::jsonb, 'r', now(), 'r', now())`, [fp, `sku:${code}`, code, col]);
    return fp;
  };
  const decide = (fp, kind, resolution, value) => q(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, $2, $3, $4::jsonb, 'user', 'x@test')`,
    [fp, kind, resolution, resolution && ['fix_ne', 'fix_cdb'].includes(resolution) ? JSON.stringify({ value }) : null]);
  const old = await cand('a06', 'standard_price_jpy');
  await decide(old, 'approved', 'accept_difference');   // 変更の前の判断 = 見ない
  await save('a06', { standard_price: '1600' });
  await save('a07', { standard_price: '1700' });
  await save('a08', { standard_price: '1800' });
  await save('a09', { standard_price: '1900' });
  await decide(await cand('a07', 'standard_price_jpy'), 'approved', 'accept_difference');
  await decide(await cand('a08', 'standard_price_jpy'), 'approved', 'fix_ne', 9999);
  await decide(await cand('a09', 'standard_price_jpy'), 'approved', 'fix_ne', 1900);
  const u = await units();
  assert.equal(unitOf(u, 'a06', 'standard_price_jpy').status, 'send');
  assert.deepEqual([unitOf(u, 'a07', 'standard_price_jpy').status, unitOf(u, 'a07', 'standard_price_jpy').why], ['blocked', 'decided']);
  assert.equal(unitOf(u, 'a08', 'standard_price_jpy').why, 'decision_other_value');
  assert.equal(unitOf(u, 'a09', 'standard_price_jpy').status, 'send');
});
await ta('[U3] NE から消す依頼の途中 = deleting・新商品の途中 (draft) = new_product・名前 = コード = name_is_code・持ち主が company でない欄は出さない', async () => {
  await save('x01', { standard_price: '1100' });
  await save('x02', { standard_price: '1200' });
  await save('x03', { name: 'X03' });
  await pg.query('begin');
  await pg.query("select set_config('ops.ne_del_protocol', '1', true)");
  const bytes = Buffer.concat([Buffer.from('8352815b83680d0a', 'hex'), Buffer.from('x01\r\n')]);
  const ex = (await pg.query(`insert into ops.ne_del_exports (kind, schema_version, encoding, header, file_bytes, sha256, row_count, request_id, reason, built_by, built_day, db_user)
    values ('products', 'ne-del-products-v1', 'sjis', 'コード', $1, encode(sha256($1), 'hex'), 1, gen_random_uuid(), '試験', 'x@test', current_date, 'deploy') returning export_id`, [bytes])).rows[0].export_id;
  await pg.query(`insert into ops.ne_del_items (export_id, sku_id, code_norm, ne_code, sku_kind, action, state, evidence, state_changed_by) values ($1, $2, 'x01', 'x01', 'single', 'delete', 'live', '{}', 'x@test')`, [ex, await sid('x01')]);
  await pg.query("select set_config('ops.registration_protocol', '1', true)");
  await pg.query("update ops.master_registrations set state = 'draft' where sku_id = $1", [await sid('x02')]);
  await pg.query('commit');
  const u = await units();
  assert.equal(unitOf(u, 'x01', 'standard_price_jpy').why, 'deleting');
  assert.equal(unitOf(u, 'x02', 'standard_price_jpy').why, 'new_product');
  assert.equal(unitOf(u, 'x03', 'name').why, 'name_is_code');
  assert.equal(unitOf(u, 'x03', 'name').cell, null);
  // 取扱中止の CSV (0072) を人の取扱の直しより後に作った = 取扱は送らない (NE の取扱中止を戻さない・設計 25 R3 H2)
  await save('h01', { handling: 'discontinued' });
  await save('h01', { standard_price: '1001' });
  await pg.query('begin');
  await pg.query("select set_config('ops.ne_del_protocol', '1', true)");
  const del = (await pg.query(`insert into ops.ne_del_items (export_id, sku_id, code_norm, ne_code, sku_kind, action, state, evidence, state_changed_by)
    values ($1, $2, 'h01', 'h01', 'single', 'delete', 'live', '{}', 'x@test') returning item_id`, [ex, await sid('h01')])).rows[0].item_id;
  const hb = Buffer.from('syohin_code,toriatukai_kbn\r\nh01,1\r\n');
  const hx = (await pg.query(`insert into ops.ne_del_exports (kind, schema_version, encoding, header, file_bytes, sha256, row_count, request_id, reason, built_by, built_day, db_user)
    values ('handling', 'ne-handling-v1', 'utf8', 'syohin_code,toriatukai_kbn', $1, encode(sha256($1), 'hex'), 1, gen_random_uuid(), '試験', 'x@test', current_date, 'deploy') returning export_id`, [hb])).rows[0].export_id;
  await pg.query(`insert into ops.ne_del_items (export_id, sku_id, code_norm, ne_code, sku_kind, action, state, replaces_item_id, evidence, state_changed_by)
    values ($1, $2, 'h01', 'h01', 'single', 'handling', 'recorded', $3, '{}', 'x@test')`, [hx, await sid('h01'), del]);
  await pg.query("update ops.ne_del_items set state = 'handled' where item_id = $1", [del]);
  await pg.query('commit');
  const uh = await units();
  assert.equal(unitOf(uh, 'h01', 'handling').why, 'handling_csv');
  assert.equal(unitOf(uh, 'h01', 'standard_price_jpy').status, 'send', 'ほかの欄は送る');
  // 原価の持ち主が load の DB = 原価は出さない (保存も通らないが、記録が残っている場合も)
  const u2 = jsonOf((await one(`select ops.ne_upd_units(null, 'bulk') as r`)).r);
  assert.ok(unitOf(u2, 'a01', 'cost'));
});
await ta('[U4] コードの書き方: NE 小文字・ロジザード大文字 = ロジザード (spelling_fix)・collided + ロジザード 1 つ = ロジザード・ロジザードに無い = ne_collided・ロジザード 2 つ = lz_many・セットは NE のまま', async () => {
  await save('u60e-10', { cost: { jpy: '110', reason: '試験' } });
  await save('q01', { cost: { jpy: '111', reason: '試験' } });
  await save('k03', { cost: { jpy: '113', reason: '試験' } });
  await save('s01', { standard_price: '3100' });
  await seedLz({ 'u60e-10': 'U60E-10', 'u60e-05': 'U60E-05', 'k03': ['K03', 'k03'], q01: null });
  await seedNe({ ne: {}, spell: { 'u60e-05': ['U60E-05', 'u60e-05'], q01: ['Q01', 'q01'] } });
  // NE の実物で「大文字・小文字の直しは今ある商品を書き換える」を確かめる前 = NE の今の書き方で送る・書き方が 2 つ = 送らない・書き方だけの行は作らない
  const u0 = await units();
  assert.deepEqual([unitOf(u0, 'u60e-10', 'cost').ne_code, unitOf(u0, 'u60e-10', 'cost').spelling_fix], ['u60e-10', false]);
  assert.equal(unitOf(u0, 'q01', 'cost').why, 'case_unverified');
  assert.equal(unitOf(u0, 'k03', 'cost').status, 'send', 'ロジザードの書き方を使わない = ロジザードが 2 つでも送る');
  assert.equal(u0.units.filter((x) => x.code_norm === 'u60e-05').length, 0, '書き方だけの行は作らない');
  await q("select ops.ne_upd_record_verified('naka@test', 'products', 'code_case', 'bulk', 'ok', null, '10/17 の u60e の CSV で今ある商品の書き方が変わった')");
  // 入口ごと: 人の取り込み (bulk) で確かめても API (api) では NE の今の書き方のまま
  assert.equal(unitOf(await units(null, 'api'), 'u60e-10', 'cost').ne_code, 'u60e-10');
  await q("select ops.ne_upd_record_verified('naka@test', 'products', 'code_case', 'api', 'ok', null, '試験')");
  const u = await units();
  const x = unitOf(u, 'u60e-10', 'cost');
  assert.deepEqual([x.status, x.ne_code, x.spelling_fix, x.ne_spelling, x.lz_spelling], ['send', 'U60E-10', true, 'u60e-10', 'U60E-10']);
  assert.equal(unitOf(u, 'q01', 'cost').why, 'ne_collided');
  assert.equal(unitOf(u, 'k03', 'cost').why, 'lz_many');
  const s = unitOf(u, 's01', 'standard_price_jpy');
  assert.deepEqual([s.status, s.ne_code, s.kind], ['send', 's01', 'sets']);
});
await ta('[U5] 書き方だけの行: 値の差が無い単品で NE の書き方 (collided) → ロジザードの書き方を原価の欄 (NE と同じ値) で 1 つ', async () => {
  const u = await units();
  const c = u.units.filter((x) => x.code_norm === 'u60e-05');
  assert.equal(c.length, 1, JSON.stringify(c));
  assert.deepEqual([c[0].role, c[0].col, c[0].cell, c[0].ne_code, c[0].status, c[0].spelling_fix], ['spelling', 'cost', '100', 'U60E-05', 'send', true]);
  // NE の書き方が ok でロジザードと同じ単品には無い
  assert.equal(u.units.filter((x) => x.code_norm === 'a10').length, 0);
});
await ta('[U6] 今朝の取得でない = ne_not_today・段階が new_open でない = 全部止める (not_open)', async () => {
  // 最初の回 (昨日始まった取得) の一覧 = ne_not_today で送れない
  assert.equal(NOT_TODAY.ok, false);
  assert.ok(NOT_TODAY.problems.some((p) => p.reason === 'ne_not_today'), JSON.stringify(NOT_TODAY.problems));
  assert.equal((await units()).ok, true, '今の回は今日');
  // 段階を frozen に (試験の印で) → not_open
  await pg.query('begin');
  await pg.query("select set_config('ops.cutover_protocol', '1', true)");
  const ph = await one('select phase, owner_hash from ops.master_cutover_state where id = 1');
  await pg.query("update ops.master_cutover_state set phase = 'company_owner' where id = 1");
  await pg.query('commit');
  try {
    const u2 = await units();
    assert.equal(u2.ok, false);
    assert.ok(u2.problems.some((p) => p.reason === 'not_open'));
    assert.ok(u2.units.every((x) => x.status === 'blocked' && x.why === 'not_open'));
  } finally {
    await pg.query('begin');
    await pg.query("select set_config('ops.cutover_protocol', '1', true)");
    await pg.query('update ops.master_cutover_state set phase = $1, owner_hash = $2 where id = 1', [ph.phase, ph.owner_hash]);
    await pg.query('commit');
  }
});

// ═══════════ [B] 作る ═══════════
console.log('[B] 作る');
await ta('[B1] 単品とセットは別のファイル・確かめていない欄は空欄にしない (欄の組ごとのファイル)・バイト列 = ne-csv.mjs の buildCsv (1 欄のとき)', async () => {
  const b = await build('auto');
  assert.equal(b.state, 'built', JSON.stringify(b));
  const files = b.files;
  const heads = files.map((f) => `${f.kind}|${f.header}`).sort();
  assert.deepEqual(heads, ['products|syohin_code,baika_tnk', 'products|syohin_code,genka_tnk', 'products|syohin_code,syohin_name', 'sets|set_syohin_code,set_baika_tnk']);
  const cost = files.find((f) => f.header === 'syohin_code,genka_tnk');
  const want = buildCsv(specOf('products', 'cost'), [{ ne_code: 'a01', cell: '150' }, { ne_code: 'U60E-05', cell: '100' }, { ne_code: 'U60E-10', cell: '110' }]);   // コードの小文字の順
  assert.equal(csvOf(cost), want.bytes.toString('utf8'));
  assert.equal(cost.sha256, want.sha256);
  assert.equal(csvOf(files.find((f) => f.kind === 'sets')), 'set_syohin_code,set_baika_tnk\r\ns01,3100\r\n');
  assert.match(csvOf(files.find((f) => f.header === 'syohin_code,baika_tnk')), /^syohin_code,baika_tnk\r\na02,1300\r\na06,1600\r\na09,1900\r\nh01,1001\r\n$/);
  // 送った後 = 送っている途中 (もう一度作っても入らない)
  const u = await units();
  assert.equal(unitOf(u, 'a01', 'cost').status, 'sending');
  assert.equal(unitOf(u, 'u60e-05', 'cost').status, 'sending');
  for (const f of files) { await fileResult(f.file_id, String(100 + Number(f.file_id)), null); }
  assert.equal((await one('select state from ops.ne_upd_sends where send_id = $1', [b.send_id])).state, 'sent', 'NE に上げた = 送った (キューは聞かない = 翌朝の照合で確かめる)');
  assert.equal(unitOf(await units(), 'a01', 'cost').status, 'sending');
  for (const f of files) await fileResult(f.file_id, String(100 + Number(f.file_id)), 2);
  assert.equal((await one('select state from ops.ne_upd_sends where send_id = $1', [b.send_id])).state, 'sent');
});
await ta('[B2] 自動は同じ今朝の回で 1 回 (前の答え)・今すぐの頼みが無い = nothing・値をまた直した = 新しい値で入る', async () => {
  const again = await build('auto');
  assert.equal(again.replayed, true);
  assert.equal((await build('now')).state, 'nothing');
  await save('a01', { cost: { jpy: '160', reason: '試験' } });
  const u = await units();
  assert.deepEqual([unitOf(u, 'a01', 'cost').status, unitOf(u, 'a01', 'cost').cell], ['send', '160']);
  const r = jsonOf((await one('select ops.ne_upd_request_now($1, $2) as r', [crypto.randomUUID(), 'naka@test'])).r);
  const b = await build('now');
  assert.equal(b.send_id, r.send_id);
  assert.deepEqual(b.files.map((f) => csvOf(f)), ['syohin_code,genka_tnk\r\na01,160\r\n']);
  await settleAll(2);
});
await ta('[B3] 確かめた欄 (空欄 = 変えない が ok) は空欄にして 1 ファイル', async () => {
  // 空欄の確かめは入口ごと: 画面の一括登録 (bulk) だけ ok では API のファイルは分けたまま
  for (const col of ['standard_price_jpy', 'cost', 'name']) await q('select ops.ne_upd_record_verified($1, $2, $3, $4, $5, null, null)', ['naka@test', 'products', col, 'bulk', 'ok']);
  const u0 = await units();
  assert.deepEqual(u0.blank_ok.bulk.products, ['cost', 'name', 'standard_price_jpy']);
  assert.deepEqual(u0.blank_ok.api.products, []);
  for (const col of ['standard_price_jpy', 'cost', 'name']) await q('select ops.ne_upd_record_verified($1, $2, $3, $4, $5, null, null)', ['naka@test', 'products', col, 'api', 'ok']);
  await save('q02', { cost: { jpy: '122', reason: '試験' } });
  await save('q03', { standard_price: '1333' });
  await save('q04', { cost: { jpy: '144', reason: '試験' }, standard_price: '1444' });
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  const b = await build('now');
  const p = b.files.filter((f) => f.kind === 'products');
  assert.equal(p.length, 1, p.map((f) => f.header).join(' / '));
  assert.equal(csvOf(p[0]), 'syohin_code,baika_tnk,genka_tnk\r\nq02,,122\r\nq03,1333,\r\nq04,1444,144\r\n');
  await settleAll(2);
  await q("select ops.ne_upd_record_verified('naka@test', 'products', c, e, 'ng', null, '戻す') from unnest(array['standard_price_jpy', 'cost', 'name']) c, unnest(array['bulk', 'api']) e");
});
await ta('[B4] 自動の上限 (100 行) を超える = held (何も作らない)・前のファイルを NE が受け付け中 = 自動は held / 今すぐは busy (頼みは残る)', async () => {
  // 101 行の差 (NE の値をずらす = 人が直したものにするため、保存の記録を作る)
  const many = Array.from({ length: 101 }, (_, i) => `m${String(i).padStart(3, '0')}`);
  await runInitialLoad(db, { ...PLAN, skus: [...PLAN.skus, ...many.map((c) => single(c))], supplierSkus: [], primarySuppliers: [] }, { log: quiet, runId: 'neupd_load_2', host: 'test', now: new Date(Date.now() - 4 * 86400e3) });
  await pg.query('begin');
  await pg.query("select set_config('core.actor_type', 'human', true), set_config('core.actor_id', 'naka@test', true), set_config('core.source_system', 'portal_master_edit', true)");
  await pg.query("update core.skus set standard_price_jpy = 2222 where code like 'm%'");
  await pg.query('commit');
  await seedNe({ ne: Object.fromEntries(many.map((c) => [c, { standard_price_jpy: 1000 }])), spell: { 'u60e-05': ['U60E-05', 'u60e-05'], q01: ['Q01', 'q01'] } });
  const b = await build('auto');
  assert.equal(b.state, 'held');
  assert.match(b.reason, /毎朝の上限 \(100 欄\)/);
  assert.equal((await one('select count(*)::int as n from ops.ne_upd_files where send_id = $1', [b.send_id])).n, 0);
  // 今すぐ = 上限なし (1 ファイル 1,000 行まで)
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  const n = await build('now');
  assert.equal(n.state, 'built');
  assert.ok(n.rows >= 101, String(n.rows));
  // 受け付け中 = 今すぐは busy (頼みは残る)
  await save('a10', { standard_price: '1010' });
  const req = jsonOf((await one('select ops.ne_upd_request_now($1, $2) as r', [crypto.randomUUID(), 'naka@test'])).r);
  for (const f of n.files) await fileResult(f.file_id, String(700 + Number(f.file_id)), 1);
  assert.equal((await build('now')).state, 'busy');
  assert.equal((await one('select state from ops.ne_upd_sends where send_id = $1', [req.send_id])).state, 'waiting');
  // 同じ頼みを 2 回押しても 1 つ
  const req2 = jsonOf((await one('select ops.ne_upd_request_now($1, $2) as r', [crypto.randomUUID(), 'naka@test'])).r);
  assert.equal(req2.send_id, req.send_id);
  assert.equal(req2.joined, true);
  await settleAll(2);
  const b2 = await build('now');
  assert.equal(b2.state, 'built');
  assert.deepEqual(b2.files.map(csvOf), ['syohin_code,baika_tnk\r\na10,1010\r\n']);
  await settleAll(-1);
  assert.equal((await one('select state from ops.ne_upd_sends where send_id = $1', [b2.send_id])).state, 'failed');
  // NE が断った = 送っている途中ではない (もう一度送れる)
  assert.equal(unitOf(await units(), 'a10', 'standard_price_jpy').status, 'send');
});
await ta('[B5] 翌朝: NE が同じになった = 一覧から消える / まだ違う = もう一度 send / 2 回 NE が受け付けたのにまだ違う = stuck (自動では送らない)', async () => {
  // k02 (1222) を送って NE が受け付けた → 翌朝: a01 (160) は NE が同じになった = 消える / k02 は NE が 1000 のまま (1 回目) = send
  const SP = { 'u60e-05': ['U60E-05', 'u60e-05'], q01: ['Q01', 'q01'] };
  await save('k02', { standard_price: '1222' });
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  assert.ok((await build('now')).files.some((f) => csvOf(f).includes('\r\nk02,1222\r\n')));
  await settleAll(2);
  await seedNe({ ne: { a01: { cost: 160 } }, spell: SP });
  let u = await units();
  assert.equal(unitOf(u, 'a01', 'cost'), undefined, 'NE と同じ = 一覧に無い');
  assert.deepEqual([unitOf(u, 'k02', 'standard_price_jpy').status, unitOf(u, 'k02', 'standard_price_jpy').sent_count], ['send', 1]);
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  await build('now');
  await settleAll(2);
  await seedNe({ spell: SP });
  u = await units();
  assert.deepEqual([unitOf(u, 'k02', 'standard_price_jpy').status, unitOf(u, 'k02', 'standard_price_jpy').sent_count], ['stuck', 2]);
  const b = await build('auto');
  assert.ok(!(b.files || []).some((f) => /k02,/.test(csvOf(f))), 'stuck は自動で送らない');
  await settleAll(2);
});

// ═══════════ [D] 画面 ═══════════
console.log('[D] 画面');
await ta('[D1] CSV で受け取る: 作る (同じ request_id = 前の答え)・作った日だけ渡す・最初のダウンロードを残す・NE に上げるファイルは渡さない', async () => {
  await save('q05', { standard_price: '1555' });
  const id = crypto.randomUUID();
  const m = jsonOf((await one('select ops.ne_upd_make_download($1, $2) as r', [id, 'naka@test'])).r);
  assert.equal(m.state, 'built', JSON.stringify(m));
  assert.ok(m.files.every((f) => !f.bytes_b64), 'バイト列はここでは返さない');
  const again = jsonOf((await one('select ops.ne_upd_make_download($1, $2) as r', [id, 'naka@test'])).r);
  assert.equal(again.replayed, true);
  assert.equal(again.send_id, m.send_id);
  const f = m.files.find((x) => x.kind === 'products');
  const d = jsonOf((await one('select ops.ne_upd_download($1, $2) as r', ['naka@test', f.file_id])).r);
  assert.match(Buffer.from(d.bytes_b64, 'base64').toString('utf8'), /q05,1555\r\n/);
  assert.match(d.file_name, /^ne-update-products-\d{8}-\d+\.csv$/);
  const row = await one('select downloaded_by, download_count from ops.ne_upd_files where file_id = $1', [f.file_id]);
  assert.deepEqual([row.downloaded_by, row.download_count], ['naka@test', 1]);
  assert.equal(unitOf(await units(), 'q05', 'standard_price_jpy').status, 'sending', '受け取った = 送っている途中');
  const up = await one("select f.file_id::text as id from ops.ne_upd_files f where f.claimed_by is not null limit 1");
  assert.match(String((await errOf(() => q('select ops.ne_upd_download($1, $2)', ['naka@test', up.id])))?.message), /not_download/);
  // 違う人の同じ request_id = 断る
  assert.match(String((await errOf(() => q('select ops.ne_upd_make_download($1, $2)', [id, 'other@test'])))?.message), /request_id_reused/);
  // 同じ差で 2 つ目の CSV を作ってから 1 つ目を受け取った = 2 つ目は渡さない (同じ欄を 2 回出さない)
  await save('q04', { standard_price: '1499' });
  const m1 = jsonOf((await one('select ops.ne_upd_make_download($1, $2) as r', [crypto.randomUUID(), 'naka@test'])).r);
  const m2 = jsonOf((await one('select ops.ne_upd_make_download($1, $2) as r', [crypto.randomUUID(), 'naka@test'])).r);
  const f1 = m1.files.find((x) => x.kind === 'products'), f2 = m2.files.find((x) => x.kind === 'products');
  await q('select ops.ne_upd_download($1, $2)', ['naka@test', f1.file_id]);
  assert.match(String((await errOf(() => q('select ops.ne_upd_download($1, $2)', ['naka@test', f2.file_id])))?.message), /file_stale/);
  await q('select ops.ne_upd_download($1, $2)', ['naka@test', f1.file_id]);   // 同じファイルのもう一度 = 渡す
  // 作った後に値を直した = 渡さない
  await save('q04', { standard_price: '1498' });
  const m3 = jsonOf((await one('select ops.ne_upd_make_download($1, $2) as r', [crypto.randomUUID(), 'naka@test'])).r);
  await save('q04', { standard_price: '1497' });
  assert.match(String((await errOf(() => q('select ops.ne_upd_download($1, $2)', ['naka@test', m3.files.find((x) => x.kind === 'products').file_id])))?.message), /file_stale/);
  assert.match(String((await errOf(() => q('select ops.ne_upd_download($1, $2)', ['naka@test', f1.file_id])))?.message), /file_stale/, '前に受け取ったファイルも値が変わったら渡さない');
});
await ta('[D2] 画面の一覧 (ops.ne_upd_overview): 差・回 (ファイル・バイト列なし)・送り手の様子・試しの記録 / 商品の画面 (ops.ne_upd_sku)', async () => {
  await q('select ops.ne_upd_worker_seen($1, true, false, $2)', ['minipc-test', 'v']);
  const o = jsonOf((await one('select ops.ne_upd_overview() as r')).r);
  assert.equal(o.worker.upload_ready, true);
  assert.ok(o.sends.length >= 5);
  assert.ok(o.sends.every((s) => s.files.every((f) => !('bytes_b64' in f))));
  assert.ok(o.verified.length >= 6);
  const s = jsonOf((await one('select ops.ne_upd_sku($1) as r', [await sid('q05')])).r);
  assert.deepEqual(s.units.map((x) => [x.code_norm, x.col, x.status]), [['q05', 'standard_price_jpy', 'sending']]);
});

// ═══════════ [W] 送り手 ═══════════
console.log('[W] 送り手');
function fakeNe({ reject = false, cut = false, status = 2 } = {}) {
  const ups = [];
  const asks = [];
  return {
    ups, asks,
    async upload(kind, text) {
      if (reject) throw new S.NeRejected('NE API エラー: アクセス情報[xxx]の許可が必要です');
      if (cut) throw new TypeError('fetch failed');
      ups.push({ kind, text }); return { que_id: String(9000 + ups.length) };
    },
    async queStatus(id) { asks.push(id); return { status, message: status === -1 ? '1件失敗' : '処理完了' }; },
  };
}
await ta('[W1] 毎朝 (API の口は既定で止めてある) = 人が受け取る CSV を作るだけ・まとめに差の数と内訳 / 今すぐの頼みは拾わない / API は呼ばない', async () => {
  await save('a03', { name: '商品 a03 改 2' });
  // 新しい朝 (B4 の 101 欄は NE が同じになった)
  await seedNe({ ne: Object.fromEntries(Array.from({ length: 101 }, (_, i) => [`m${String(i).padStart(3, '0')}`, { standard_price_jpy: 2222 }])), spell: { 'u60e-05': ['U60E-05', 'u60e-05'], q01: ['Q01', 'q01'] } });
  const ne = fakeNe();
  const r = await S.runNeUpdateSend({ db, ne, mode: 'daily', env: {}, host: 'minipc-test', sleep: async () => {} });
  assert.equal(r.code, 0, r.line);
  assert.match(r.line, /^✅ NEへ送る: NE とちがう所 \d+ 欄 \(.*名前.*\) → ポータルの「NEへ送る」で CSV を作って NE に取り込んでください$/);
  assert.doesNotMatch(r.line, /API|回数/);
  const f = await one('select count(*)::int as n, count(*) filter (where claimed_by is null)::int as unclaimed from ops.ne_upd_files where send_id = $1', [r.sends[0].send_id]);
  assert.ok(f.n >= 1 && f.n === f.unclaimed, JSON.stringify(f));
  assert.equal(unitOf(await units(), 'a03', 'name').status, 'send', '作っただけ (だれも受け取っていない) = まだ送っていない');
  const again = await S.runNeUpdateSend({ db, ne, mode: 'daily', env: {}, host: 'minipc-test', sleep: async () => {} });
  assert.match(again.line, /今朝の分はもう作ってあります/);
  const p = await S.runNeUpdateSend({ db, ne, mode: 'pending', env: {}, host: 'minipc-test', sleep: async () => {} });
  assert.match(p.line, /API の口は止めてあります/);
  assert.deepEqual([ne.ups.length, ne.asks.length], [0, 0]);
});
await ta('[W2] 今すぐ: 頼みを拾って上げる (単品 = master_goods・セット = setgoods)・受付の結果を残す・まとめの 1 行', async () => {
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  const ne = fakeNe();
  const r = await S.runNeUpdateSend({ db, ne, mode: 'pending', env: { NE_UPDATE_UPLOAD: '1' }, host: 'minipc-test', sleep: async () => {}, waitMs: 0 });
  assert.equal(r.code, 0, r.line);
  assert.ok(ne.ups.some((x) => x.kind === 'products' && /a03,商品 a03 改 2\r\n/.test(x.text)), JSON.stringify(ne.ups));
  assert.match(r.line, /^✅ NEへ送る \(今すぐ\): \d+ 欄 \(.*名前 1.*\) を NE に上げました \(翌朝の照合/);
  assert.equal(ne.asks.length, 0, 'キューの結果は聞かない (既定)');
  assert.equal(unitOf(await units(), 'a03', 'name').status, 'sending');
  assert.equal((await one("select count(*)::int as n from ops.ne_upd_files where claimed_by is not null and state = 'made'")).n, 0);
  // キューを聞く設定 = 1 ファイル 1 回だけ
  await save('a03', { name: '商品 a03 改 2b' });
  await settleAll(2);
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  const ne2 = fakeNe();
  const r2 = await S.runNeUpdateSend({ db, ne: ne2, mode: 'pending', env: { NE_UPDATE_UPLOAD: '1', NE_UPDATE_QUE_CHECK: '1' }, host: 'minipc-test', sleep: async () => {}, waitMs: 0 });
  assert.equal(r2.code, 0, r2.line);
  assert.equal(ne2.asks.length, ne2.ups.length);
  assert.equal((await one("select state from ops.ne_upd_files where que_id = $1", [ne2.asks[0]])).state, 'ok');
  // 答えが「処理中」= 聞いた印だけ残す → 次の回は聞かない (1 ファイル 1 回)
  await save('a03', { name: '商品 a03 改 2c' });
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  const ne3 = fakeNe({ status: 1 });
  await S.runNeUpdateSend({ db, ne: ne3, mode: 'pending', env: { NE_UPDATE_UPLOAD: '1', NE_UPDATE_QUE_CHECK: '1' }, host: 'minipc-test', sleep: async () => {}, waitMs: 0 });
  assert.equal(ne3.asks.length, ne3.ups.length);
  assert.ok((await one('select que_checked_at from ops.ne_upd_files where que_id = $1', [ne3.asks[0]])).que_checked_at);
  await S.runNeUpdateSend({ db, ne: ne3, mode: 'pending', env: { NE_UPDATE_UPLOAD: '1', NE_UPDATE_QUE_CHECK: '1' }, host: 'minipc-test', sleep: async () => {}, waitMs: 0 });
  assert.equal(ne3.asks.length, ne3.ups.length, '2 回目は聞かない');
  await settleAll(2);
});
await ta('[W3] NE がはっきり断った = ❌・failed (もう一度送れる) / 通信が切れた = ⚠️ unknown (送り直さない = 送っている途中のまま・翌朝の照合を待つ)', async () => {
  await save('a03', { name: '商品 a03 改 3' });
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  const r = await S.runNeUpdateSend({ db, ne: fakeNe({ reject: true }), mode: 'pending', env: { NE_UPDATE_UPLOAD: '1' }, host: 'minipc-test', sleep: async () => {}, waitMs: 0 });
  assert.equal(r.code, 1);
  assert.match(r.line, /^❌ NEへ送る \(今すぐ\): .*アクセス情報/);
  assert.equal(unitOf(await units(), 'a03', 'name').status, 'send');
  await q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']);
  const r2 = await S.runNeUpdateSend({ db, ne: fakeNe({ cut: true }), mode: 'pending', env: { NE_UPDATE_UPLOAD: '1' }, host: 'minipc-test', sleep: async () => {}, waitMs: 0 });
  assert.equal(r2.code, 0, r2.line);
  assert.match(r2.line, /^⚠️ NEへ送る \(今すぐ\): .*届いたか分かりません/);
  assert.equal(unitOf(await units(), 'a03', 'name').status, 'sending', '受付が分からない = 自動では送り直さない');
  // 前の答えは同じファイルを 2 回上げない (バイト列を渡さない)
  const rp = await build('auto');
  assert.ok(rp.replayed && (rp.files || []).every((x) => !x.bytes_b64), JSON.stringify(rp));
});

// ═══════════ [P] 権限 ═══════════
console.log('[P] 権限');
await ta('[P1] master_edit = 画面の関数だけ (作る・結果・生存は不可)・watch_writer = 送り手の関数だけ (今すぐの頼み・受け取りは不可)・表はだれも直接書けない', async () => {
  const can = async (role, fn) => (await one(`select has_function_privilege($1, $2, 'execute') as ok`, [role, fn])).ok;
  for (const f of ['ops.ne_upd_overview()', 'ops.ne_upd_sku(bigint)', 'ops.ne_upd_request_now(uuid, text)', 'ops.ne_upd_make_download(uuid, text)', 'ops.ne_upd_download(text, bigint)',
    'ops.ne_upd_record_verified(text, text, text, text, text, text, text)']) assert.equal(await can('master_edit', f), true, f);
  for (const f of ['ops.ne_upd_build(text, text, boolean)', 'ops.ne_upd_file_result(bigint, text, text, integer, text)', 'ops.ne_upd_queued_files()', 'ops.ne_upd_worker_seen(text, boolean, boolean, text)',
    'ops.ne_upd_units(bigint[], text)', 'ops._ne_upd_fill(bigint, boolean, text, boolean)']) assert.equal(await can('master_edit', f), false, f);
  for (const f of ['ops.ne_upd_build(text, text, boolean)', 'ops.ne_upd_file_result(bigint, text, text, integer, text)', 'ops.ne_upd_queued_files()', 'ops.ne_upd_worker_seen(text, boolean, boolean, text)',
    'ops.ne_upd_overview()']) assert.equal(await can('watch_writer', f), true, f);
  for (const f of ['ops.ne_upd_request_now(uuid, text)', 'ops.ne_upd_make_download(uuid, text)', 'ops.ne_upd_download(text, bigint)']) assert.equal(await can('watch_writer', f), false, f);
  for (const role of ['master_edit', 'watch_writer', 'watcher']) {
    for (const t of ['ops.ne_upd_sends', 'ops.ne_upd_files', 'ops.ne_upd_rows', 'ops.ne_upd_verified', 'ops.ne_upd_workers']) {
      // watcher = 見張りの読むだけのロール (ops の全部の表の select = 0040 の CSV の表と同じ)。書けるロールは無い
      if (role !== 'watcher') assert.equal((await one(`select has_table_privilege($1, $2, 'select') as ok`, [role, t])).ok, false, `${role} ${t} select`);
      assert.equal((await one(`select has_table_privilege($1, $2, 'insert') or has_table_privilege($1, $2, 'update') or has_table_privilege($1, $2, 'delete') as ok`, [role, t])).ok, false, `${role} ${t}`);
    }
  }
  // 持ち主のロールでも印が無ければ書けない・消せない
  assert.match(String((await errOf(() => q("insert into ops.ne_upd_sends (how, request_id, requested_by, state) values ('now', gen_random_uuid(), 'x', 'waiting')")))?.message), /ne_upd_write/);
  assert.match(String((await errOf(() => q('update ops.ne_upd_files set state = state')))?.message), /ne_upd_write/);
  assert.match(String((await errOf(() => q('delete from ops.ne_upd_rows')))?.message), /ne_upd_write/);
  // 関数の中でも行は書き換えない
  await pg.query('begin');
  await pg.query("select set_config('ops.ne_upd_protocol', '1', true)");
  const e1 = await errOf(() => pg.query("update ops.ne_upd_rows set cell = 'x'"));
  await pg.query('rollback');
  assert.match(String(e1?.message), /行は書き換えない/);
  await pg.query('begin');
  await pg.query("select set_config('ops.ne_upd_protocol', '1', true)");
  const e2 = await errOf(() => pg.query("update ops.ne_upd_files set header = 'x'"));
  await pg.query('rollback');
  assert.match(String(e2?.message), /中身は書き換えない/);
});
await ta('[P2] 画面のロールで呼べる (overview・頼み)・送り手のロールで作れる', async () => {
  const o = await as('master_edit', async () => jsonOf((await one('select ops.ne_upd_overview() as r')).r));
  assert.ok(Array.isArray(o.units));
  await as('master_edit', () => q('select ops.ne_upd_request_now($1, $2)', [crypto.randomUUID(), 'naka@test']));
  const b = await as('watch_writer', async () => jsonOf((await one('select ops.ne_upd_build($1, $2, true) as r', ['now', 'minipc-test'])).r));
  assert.ok(['built', 'empty'].includes(b.state), JSON.stringify(b));
  await settleAll(2);
});
await ta('[P3] ロールが先・0093 が後 (本番の当て方) でも同じ実行権 (migration の中で付ける = ロールの流し直しを待たない)', async () => {
  const pg2 = new PGlite();
  await pg2.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg2.query(`alter database ${(await pg2.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg2.query('set role deploy');
  const db2 = pgliteAdapter(pg2);
  await applyMigrations(db2, { log: quiet, to: String(Number(MIGF.slice(0, 4)) - 1).padStart(4, '0') });
  await createRoles(pg2, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg2, {});
  await applyMigrations(db2, { log: quiet });
  const can = async (role, fn) => (await pg2.query(`select has_function_privilege($1, $2, 'execute') as ok`, [role, fn])).rows[0].ok;
  assert.equal(await can('master_edit', 'ops.ne_upd_overview()'), true);
  assert.equal(await can('master_edit', 'ops.ne_upd_request_now(uuid, text)'), true);
  assert.equal(await can('master_edit', 'ops.ne_upd_build(text, text, boolean)'), false);
  assert.equal(await can('watch_writer', 'ops.ne_upd_build(text, text, boolean)'), true);
  assert.equal(await can('watch_writer', 'ops.ne_upd_download(text, bigint)'), false);
  assert.equal(await can('watcher', 'ops.ne_upd_build(text, text, boolean)'), false);
  assert.equal(await can('master_edit', 'ops.ne_upd_units(bigint[], text)'), false);
  await pg2.close();
});

console.log(`\n${passed} passed${process.exitCode ? ' (失敗あり)' : ''}`);
