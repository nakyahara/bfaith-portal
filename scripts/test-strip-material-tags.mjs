/**
 * test-strip-material-tags.mjs — 一回きり: 商品名の末尾の資材の印を外す (scripts/company-db/strip-material-tags.mjs・10/23 中原さん)
 *
 * Company DB = PGlite (段階 new_open・持ち主 = 全部 company)。資材の登録 = 試験の SQLite (郵便料金の pm_skus・梱包アプリの pk_pack_material_rules)
 * 固定する契約:
 *   P 印を外す形: 末尾の印 1 つだけ (前後の空白も除く)・印でない末尾 (_Type-C・末尾の空白の後) は外さない・引数の検査
 *   D dry-run: 読むだけ (変更の記録・保存の記録が増えない)・外す / 外さない (郵便料金の登録が無い・違う・梱包アプリの登録が無い・セット・印が 2 つ・
 *     短い・コードと同じ・同じ名前になる)・ハッシュは同じ入力で同じ・--gate postage で変わる・watcher のロールで読める
 *   A apply: 画面のロール master_edit でないと断る・ハッシュが違うと断る (何も書かない)・保存は画面と同じ (saveSku = 変更の記録の人・理由・
 *     source_system portal_master_edit・db_user master_edit・商品の行の名前も同じ値)・--limit と途中で止まった後に同じハッシュで続きから
 *     (済んだものは書かない)・全部済んだ後の apply = 何も書かない・前に失敗した回の request_id は使わない (次の回)
 *   N 「NEへ送る」(0094 の ops.ne_upd_units): 外した名前は人の直しとして send に出る・外さない商品は出ない
 * 使い方: node scripts/test-strip-material-tags.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
const OG = await import('../lib/master-owner-gate.mjs');
const { OWNED_COLUMNS } = await import('../config/master-ownership.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { forceNewOpen } = await import('./fixtures/master-widen.mjs');
const W = await import('../lib/master-write.mjs');
const T = await import('./company-db/strip-material-tags.mjs');
const Database = createRequire(import.meta.url)('better-sqlite3');

const watchdog = setTimeout(() => { console.error('  NG  試験が 600 秒で終わらない (止める)'); process.exit(1); }, 600000);
watchdog.unref();
let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const ALL = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const ACTOR = 'naka@test';
const REASON = T.DEFAULT_REASON;

// ═══════════ P 印を外す形・引数 ═══════════
console.log('[P] 印を外す形・引数');
await ta('[P1] 末尾の印 1 つだけを外す (前後の空白も)・印でない末尾は外さない', async () => {
  assert.deepEqual(T.stripTag('ローズマリーシネオールオイル【10ml】_梱機プ'), { tag: '梱機プ', to: 'ローズマリーシネオールオイル【10ml】' });
  assert.deepEqual(T.stripTag('ハッカ油 【200ml】 _パフ箱'), { tag: 'パフ箱', to: 'ハッカ油 【200ml】' });
  assert.deepEqual(T.stripTag('ひば油 50ml_K-44'), { tag: 'K-44', to: 'ひば油 50ml' });
  assert.deepEqual(T.stripTag('アロマ_白プチ_梱機プ'), { tag: '梱機プ', to: 'アロマ_白プチ' });   // 1 つだけ (2 つ重なりは計画で外さない)
  assert.equal(T.stripTag('ケーブル_Type-C'), null);
  assert.equal(T.stripTag('オイル_梱機プ '), null);
  assert.equal(T.stripTag('梱機プの説明'), null);
  assert.equal(T.stripTag(null), null);
  assert.notEqual(T.stripRequestId('1', 'a_白プチ', 'a', 0), T.stripRequestId('1', 'a_白プチ', 'a', 1));
  assert.match(T.stripRequestId('1', 'a_白プチ', 'a', 0), /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
});
await ta('[P2] 引数: --actor・資材の DB は要る・apply は --expect-hash と --yes・--limit は apply だけ・--gate は both / postage', async () => {
  const base = ['--actor', 'Naka@Test', '--postage-db', 'p.db', '--packing-db', 'k.db'];
  const o = T.parseStripArgs(base);
  assert.equal(o.actor, 'naka@test'); assert.equal(o.apply, false); assert.equal(o.gate, 'both'); assert.equal(o.reason, T.DEFAULT_REASON);
  assert.throws(() => T.parseStripArgs(['--postage-db', 'p.db', '--packing-db', 'k.db']), /--actor/);
  assert.throws(() => T.parseStripArgs(['--actor', 'a@b', '--packing-db', 'k.db']), /--postage-db/);
  assert.throws(() => T.parseStripArgs(['--actor', 'a@b', '--postage-db', 'p.db']), /--packing-db/);
  assert.equal(T.parseStripArgs(['--actor', 'a@b', '--postage-db', 'p.db', '--gate', 'postage']).gate, 'postage');
  assert.equal(T.parseStripArgs([...base, '--gate', 'none']).gate, 'none');   // 🆕 10/23 中原さん
  assert.throws(() => T.parseStripArgs([...base, '--gate', 'all']), /--gate/);
  assert.throws(() => T.parseStripArgs([...base, '--apply', '--yes']), /--expect-hash/);
  assert.throws(() => T.parseStripArgs([...base, '--apply', '--expect-hash', 'a'.repeat(64)]), /--yes/);
  assert.throws(() => T.parseStripArgs([...base, '--limit', '3']), /--limit は --apply/);
  assert.equal(T.parseStripArgs([...base, '--apply', '--expect-hash', 'a'.repeat(64), '--yes', '--limit', '3', '--chunk', '2']).limit, 3);
  assert.throws(() => T.parseStripArgs([...base, '--chunk', '201']), /--chunk/);
  assert.throws(() => T.parseStripArgs([...base, '--reason', 'あ'.repeat(201)]), /--reason/);
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
const jsonOf = (r) => (typeof r === 'string' ? JSON.parse(r) : r);

const single = (code, name) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
const NAMES = {
  t01: 'オイルA【10ml】_梱機プ',        // 郵便料金 白プチ + 梱包 = 外す
  t02: 'エプロン 110cm_長3封',          // 郵便料金の登録なし = 外さない
  t03: 'ジャージ補修シート_白ビ袋',      // 郵便料金 = 茶封筒 (印は白ビ袋) = 外さない
  t04: 'ひば油 【50ml】 _K-44',          // 郵便料金は引かない印 + 梱包 = 外す
  t05: 'ハニー 20ml_ネコ段',            // 梱包の登録が 2 品の伝票だけ = 外さない (--gate postage なら外す)
  t06: 'アロマ_白プチ_梱機プ',          // 印が 2 つ
  t07: 'あ_パフ箱',                     // 外すと 1 字
  t08: 'オイルB_梱機プ',                // t09 と同じ名前になる
  t09: 'オイルB_白プチ',
  t10: 'ふつうの名前',                  // 印なし
  'ab-1': 'ab-1_K-50',                  // 外すとコードと同じ
  t11: 'ピンセット_白プチ',             // 郵便料金 白プチ・梱包の登録なし = 外さない (--gate postage なら外す)
  t12: 'オイルC',                       // 印なし (同じ名前の相手)
  t13: 'オイルC_K-44',                  // 外すと t12 と同じ名前
  t14: 'オイルC＿K-44_K-50',            // 外すと「オイルC＿K-44」= NFKC で t13 の今の名前 (t13 は外さない = 今の名前のまま) と同じ
  m01: 'まとめ 1_K-60', m02: 'まとめ 2_K-60', m03: 'まとめ 3_K-60', m04: 'まとめ 4_K-60', m05: 'まとめ 5_K-60',
};
const SINGLES = Object.entries(NAMES).map(([c, n]) => single(c, n));
const lr = await runInitialLoad(db, {
  skus: [...SINGLES, { ...single('s01', 'セット 2 個_梱機プ'), kind: 'set', cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } }],
  setComponents: [{ parentCode: 's01', childCode: 't10', qty: 2, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [], variationGroups: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: SINGLES.map((s) => ({ supplierCode: '0001', skuCode: s.code })),
  primarySuppliers: SINGLES.map((s) => ({ skuCode: s.code, supplierCode: '0001' })), reorder: { available: false, runId: null, reason: '試験' },
}, { log: quiet, runId: 'strip_load_1', host: 'test', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
await forceNewOpen(db, ALL);

// 資材の登録 (試験の SQLite)
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'strip-tags-test-'));
const POSTAGE = path.join(TMP, 'postage.db');
const PACKING = path.join(TMP, 'picking.db');
{
  const p = new Database(POSTAGE);
  p.exec(`create table pm_skus (sku_code text primary key, display_name text, default_material_code text, material_source text, updated_at text)`);
  const ins = p.prepare('insert into pm_skus values (?, ?, ?, ?, ?)');
  for (const [c, m, s] of [['T01', 'shiropuchi', 'explicit'], ['t03', 'chabuto', 'explicit'], ['t06', 'shiropuchi', 'name_suffix'], ['t08', 'shiropuchi', 'explicit'], ['t09', 'shiropuchi', 'explicit'], ['t11', 'shiropuchi', 'explicit'], ['t02', null, null]]) {
    ins.run(c, NAMES[c.toLowerCase()], m, s, '2026-09-05T00:00:00Z');
  }
  p.close();
  const k = new Database(PACKING);
  k.exec(`create table pk_pack_material_rules (id integer primary key, combo_key text, combo_detail text, delivery_code text, material_code text, status text, updated_at text)`);
  const ins2 = k.prepare("insert into pk_pack_material_rules (combo_key, combo_detail, delivery_code, material_code, status, updated_at) values (?, '[]', ?, ?, ?, '2026-10-22T00:00:00Z')");
  for (const c of ['t01', 't02', 't03', 't04', 't06', 't07', 't08', 't09', 'ab-1', 't13', 't14', 'm01', 'm02', 'm03', 'm04', 'm05', 's01']) ins2.run(`v1|${c}*1`, 'aes_mail', 'shiro_puchi_env', 'active');
  ins2.run('v1|t05*1|t10*1', 'aes_mail', 'nekopos_box', 'active');   // 2 品の伝票 = この商品だけの登録ではない
  ins2.run('v1|t05*2', 'aes_mail', 'nekopos_box', 'disabled');       // 取り消した登録
  k.close();
}
const EV = T.readMaterialEvidence({ postageDb: POSTAGE, packingDb: PACKING });
const nameOf = async (code) => (await one('select s.name, p.name as pname from core.skus s left join core.products p on p.product_id = s.product_id where s.code = $1', [code]));
const nEvents = async () => Number((await one('select count(*)::int as n from events.master_change_events')).n);
const nReq = async () => Number((await one('select count(*)::int as n from ops.master_edit_requests')).n);
const sid = async (code) => String((await one('select sku_id from core.skus where code = $1', [code])).sku_id);
const dry = (role = 'watcher', opts = {}) => as(role, () => T.runStrip(db, { actor: ACTOR, reason: REASON, gate: 'both', evidence: EV, show: 5, ...opts }, { log: quiet }));
const apply = (hash, opts = {}, hooks = {}) => as(opts.role || 'master_edit', () => T.runStrip(db, { actor: ACTOR, reason: REASON, gate: 'both', evidence: EV, apply: true, expectHash: hash, chunk: 2, open: true, ...opts },
  { log: quiet, now: hooks.now ?? undefined, save: hooks.save ?? null }));

// 今朝の NE (照合の回 + NE のコード + NE の値) = 今の Company DB の値 (名前は印つき)
async function seedNe() {
  const startMs = Date.now() - 60000;
  const run = `mc_${new Date(startMs + 2000).toISOString().replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`;
  await q('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)', [run, new Date(startMs + 2000).toISOString()]);
  const rows = await q(`select k.code_norm, k.sku_kind, k.name, k.handling, k.tax_rate::float8 as tax_rate, k.standard_price_jpy::float8 as price,
      (select c.cost_jpy::float8 from core.sku_costs c where c.sku_id = k.sku_id and c.valid_to is null) as cost,
      (select coalesce(json_agg(s.code_norm order by s.code_norm), '[]') from core.supplier_skus x join core.suppliers s on s.supplier_id = x.supplier_id where x.sku_id = k.sku_id and x.is_primary) as sup
    from core.skus k where k.company_id = 1 and k.sku_kind in ('single', 'set') order by k.code_norm`);
  await q('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: rows.map((r) => ({ code_norm: r.code_norm, kind: 'product', state: 'ok', ne_code: r.code_norm, spellings: [r.code_norm] })) })]);
  const entries = [];
  for (const r of rows) {
    const cols = r.sku_kind === 'single'
      ? { name: r.name.trim(), handling: r.handling || 'unknown', tax_rate: r.tax_rate, standard_price_jpy: r.price, cost: r.cost, primary_supplier: jsonOf(r.sup) }
      : { name: r.name.trim(), standard_price_jpy: r.price };
    for (const [col, v] of Object.entries(cols)) entries.push({ code_norm: r.code_norm, col, ne_kind: r.sku_kind, state: 'ok', value: v });
  }
  const nS = rows.filter((r) => r.sku_kind === 'single').length, nT = rows.length - nS;
  const side = (s, n, extra) => ({ started_at: new Date(s).toISOString(), finished_at: new Date(s + 500).toISOString(), complete_rev: 1, fetched_rows: n, write_attempts: n, stored_rows: n,
    dropped_no_code: 0, dropped_missing_fields: 0, raw_rows: n, ...extra });
  await q('select ops.record_ne_values($1::jsonb)', [JSON.stringify({ compare_run_id: run, norm_version: 1,
    fetch: { fingerprint: 'f'.repeat(64), products: side(startMs, nS + nT, { codes: nS + nT, set_overlap: nT }), sets: side(startMs + 1, nT, { codes: nT }) }, entries })]);
}
await seedNe();
const units = async () => jsonOf((await one('select ops.ne_upd_units(null, $1) as r', ['bulk'])).r);

// ═══════════ D dry-run ═══════════
console.log('[D] dry-run');
let H = null;
await ta('[D1] 読むだけ (記録が増えない)・外す / 外さない の理由・ハッシュは同じ入力で同じ・watcher のロールで読める', async () => {
  const e0 = await nEvents(), r0 = await nReq();
  const r = await dry();
  assert.equal(r.exitCode, 0);
  const p = r.plan;
  assert.deepEqual(p.pending.map((x) => x.code), ['t01', 't04', 'm01', 'm02', 'm03', 'm04', 'm05']);
  assert.deepEqual(p.pending.slice(0, 2).map((x) => x.to), ['オイルA【10ml】', 'ひば油 【50ml】']);
  const why = Object.fromEntries(p.skipped.map((s) => [s.code, s.why]));
  assert.deepEqual(why, { t02: 'no_postage_material', t03: 'postage_mismatch', t05: 'no_packing_rule', t06: 'double_tag', t07: 'name_short',
    t08: 'duplicate_name', t09: 'duplicate_name', 'ab-1': 'name_invalid', t11: 'no_packing_rule', t13: 'duplicate_name', t14: 'duplicate_name', s01: 'kind_set' });
  assert.equal(p.counts.tagged_non_cancelled, 19);
  assert.equal(p.counts.strippable, 7);
  assert.equal(p.counts.skipped_no_material, 3); assert.equal(p.counts.skipped_mismatch, 1); assert.equal(p.counts.skipped_other, 8);
  assert.equal(p.counts.if_gate_postage, 8, 't11 だけ (t05 = 郵便料金が見ない印は梱包の登録が要る)');
  assert.deepEqual(p.ne, { product_rows: 7, files: 1, auto_limit: 100, file_rows: 1000 });
  assert.equal(await nEvents(), e0); assert.equal(await nReq(), r0);
  assert.equal((await dry('watcher')).plan.hash, p.hash);
  assert.equal((await dry('master_edit')).plan.hash, p.hash, '読むロールによらない');
  assert.notEqual((await dry('watcher', { actor: 'other@test' })).plan.hash, p.hash, '人はハッシュに入る');
  H = p.hash;
  // 外さない一覧の CSV
  const csv = T.skippedCsv(p);
  assert.ok(csv.startsWith('﻿コード,名前,印,'));
  assert.match(csv, /\r\nt02,エプロン 110cm_長3封,長3封,no_postage_material,/);
});
await ta('[D2] 資材の登録の読み方: 郵便料金のコードは大文字小文字をそろえる・梱包はこの商品だけの伝票の有効な登録だけ', async () => {
  assert.equal(EV.postage.get('t01').material, 'shiropuchi');
  assert.equal(EV.postage.get('t02').material, null);
  assert.equal(EV.packing.has('t05'), false);
  assert.deepEqual(EV.packing.get('t01'), [{ delivery: 'aes_mail', material: 'shiro_puchi_env', qty: 1 }]);
  assert.equal(EV.meta.packing.active_rules, 18);
  assert.equal(T.packingSkuKey(' AB-1 '), 'ab-1');
  assert.throws(() => T.readMaterialEvidence({ postageDb: path.join(TMP, 'nai.db') }), /ファイルが無い/);
});
await ta('[D3] --gate postage = 郵便料金が印から資材を引く印だけ梱包アプリを見ない (t11 は外す・t05 の _ネコ段 は梱包の登録が要る)・ハッシュが変わる', async () => {
  const r = await dry('watcher', { gate: 'postage' });
  assert.ok(r.plan.pending.some((x) => x.code === 't11'));
  assert.equal(r.plan.skipped.find((x) => x.code === 't05').why, 'no_packing_rule');
  assert.equal(r.plan.pending.length, 8);
  // 梱包アプリの DB が無いとき (--gate postage) = 郵便料金が見ない印は外さない
  const r2 = await dry('watcher', { gate: 'postage', evidence: T.readMaterialEvidence({ postageDb: POSTAGE }) });
  assert.deepEqual(r2.plan.pending.map((x) => x.code), ['t01', 't11']);
  assert.notEqual(r.plan.hash, H);
});

// ═══════════ A apply ═══════════
console.log('[A] apply');
await ta('[A1] 保存を開くスイッチ (MASTER_EDIT_OPEN) が無い・画面のロール master_edit でない (DB の持ち主) = 断る・ハッシュが違う = 断る (何も書かない)', async () => {
  const e0 = await nEvents();
  const prevOpen = process.env.MASTER_EDIT_OPEN;
  delete process.env.MASTER_EDIT_OPEN;
  try {
    const r0 = await apply(H, { open: undefined });
    assert.equal(r0.exitCode, 2); assert.equal(r0.result.refused, 'not_open');
  } finally { if (prevOpen != null) process.env.MASTER_EDIT_OPEN = prevOpen; }
  const r1 = await apply(H, { role: 'deploy' });
  assert.equal(r1.exitCode, 2); assert.equal(r1.result.refused, 'role');
  const r2 = await apply('f'.repeat(64));
  assert.equal(r2.exitCode, 1); assert.equal(r2.result.refused, 'hash');
  assert.equal(await nEvents(), e0);
  assert.equal((await nameOf('t01')).name, NAMES.t01);
});
await ta('[A2] --limit 2 (2 件ずつ) = 画面と同じ保存: 名前と商品の行の名前・変更の記録 (人・理由・portal_master_edit・master_edit)・request_id', async () => {
  const r = await apply(H, { limit: 2 });
  assert.equal(r.exitCode, 0, JSON.stringify(r.result.failed));
  assert.deepEqual(r.result.wrote.map((x) => x.code), ['t01', 't04']);
  assert.deepEqual(await nameOf('t01'), { name: 'オイルA【10ml】', pname: 'オイルA【10ml】' });
  assert.deepEqual(await nameOf('t04'), { name: 'ひば油 【50ml】', pname: 'ひば油 【50ml】' });
  assert.equal((await nameOf('m01')).name, NAMES.m01, '--limit の外は書かない');
  const id = await sid('t01');
  const rid = T.stripRequestId(id, NAMES.t01, 'オイルA【10ml】', 0);
  assert.equal(r.result.wrote[0].request_id, rid);
  const ev = await q(`select entity_type, attribute, actor_type, actor_id, source_system, db_user, reason_text from events.master_change_events where request_id = $1 order by event_id`, [rid]);
  assert.deepEqual([...new Set(ev.map((e) => `${e.entity_type}.${e.attribute}`))].sort(), ['product.name', 'sku.name']);
  for (const e of ev) assert.deepEqual([e.actor_type, e.actor_id, e.source_system, e.db_user, e.reason_text], ['human', ACTOR, 'portal_master_edit', 'master_edit', REASON]);
  const req = await one('select status, operation, actor_id from ops.master_edit_requests where request_id = $1', [rid]);
  assert.deepEqual(req, { status: 'done', operation: 'sku_edit', actor_id: ACTOR });
});
await ta('[A3] 済んだものも計画に入る = 同じハッシュ・途中で止まる (つながらない) → 書けた分は残る', async () => {
  const again = await dry();
  assert.equal(again.plan.hash, H, '済んだ 2 件を入れて同じハッシュ');
  assert.deepEqual(again.plan.done.map((x) => x.code), ['t01', 't04']);
  assert.equal(again.plan.pending.length, 5);
  let n = 0;
  const save = async (...a) => { n++; if (n === 2) throw Object.assign(new Error('Connection terminated unexpectedly'), { code: 'ECONNRESET' }); return W.saveSku(...a); };
  const r = await apply(H, {}, { save });
  assert.equal(r.exitCode, 1);
  assert.deepEqual(r.result.wrote.map((x) => x.code), ['m01']);
  assert.equal(r.result.stopped.code, 'm02');
  assert.equal((await nameOf('m01')).name, 'まとめ 1');
  assert.equal((await nameOf('m02')).name, NAMES.m02);
});
await ta('[A4] 同じハッシュでもう一度 = 残りだけ書く (前に失敗した回の request_id は使わない)・済んだものは書き直さない', async () => {
  // m03 の 0 回目の失敗の記録 (例: 夜間の取り込み中) が残っている = 1 回目の番号で書く
  const m03 = await sid('m03');
  await q(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, error, started_at)
    values ($1, 1, 'sku_edit', 'm03', $2, $3, repeat('0', 64), 'failed', '{"status":409,"reason":"nightly_load"}'::jsonb, now())`,
  [T.stripRequestId(m03, NAMES.m03, 'まとめ 3', 0), m03, ACTOR]);
  const before = Number((await one(`select count(*)::int as n from events.master_change_events where entity_type = 'sku' and attribute = 'name' and entity_id = $1`, [await sid('t01')])).n);
  const r = await apply(H);
  assert.equal(r.exitCode, 0, JSON.stringify(r.result));
  assert.deepEqual(r.result.wrote.map((x) => x.code), ['m02', 'm03', 'm04', 'm05']);
  assert.equal(r.result.wrote.find((x) => x.code === 'm03').request_id, T.stripRequestId(m03, NAMES.m03, 'まとめ 3', 1));
  for (const c of ['m02', 'm03', 'm04', 'm05']) assert.equal((await nameOf(c)).name, NAMES[c].replace('_K-60', ''));
  assert.equal(Number((await one(`select count(*)::int as n from events.master_change_events where entity_type = 'sku' and attribute = 'name' and entity_id = $1`, [await sid('t01')])).n), before);
  // 外さない商品はそのまま
  for (const c of ['t02', 't03', 't05', 't06', 't07', 't08', 't09', 'ab-1', 't11', 't13', 't14', 's01']) assert.equal((await nameOf(c)).name, c === 's01' ? 'セット 2 個_梱機プ' : NAMES[c], c);
});
await ta('[A5] 全部済んだ後 = 同じハッシュで何も書かない (やり直しても同じ)', async () => {
  const p = (await dry()).plan;
  assert.equal(p.hash, H); assert.equal(p.pending.length, 0); assert.equal(p.done.length, 7);
  const e0 = await nEvents();
  const r = await apply(H);
  assert.equal(r.exitCode, 0); assert.equal(r.result.wrote.length, 0);
  assert.equal(await nEvents(), e0);
});
await ta('[A6] ほかの人が後から名前を直した済みの商品は計画から外れる (ハッシュが変わる = 前のハッシュの apply は断る)', async () => {
  const id = await sid('t01');
  const cur = await W.readCurrent(db, id, W.jstDate());
  await as('master_edit', () => W.saveSku(db, { actor: 'other@test', requestId: crypto.randomUUID(), code: 't01', reason: 'ほかの人', seen: { token: W.editTokenOf(cur) }, values: { name: 'オイルA 10ml' } }, { open: true }));
  const p = (await dry()).plan;
  assert.notEqual(p.hash, H);
  assert.equal(p.done.length, 6);
  assert.equal((await apply(H)).result.refused, 'hash');
});
await ta('[A7] 印を付け直された商品はまた計画に入る・保存の直前にほかの人が直した = 1 回だけ読み直して、名前が計画と違えば飛ばす (書かない)', async () => {
  const save1 = async (code, name) => {
    const cur = await W.readCurrent(db, await sid(code), W.jstDate());
    return W.saveSku(db, { actor: 'other@test', requestId: crypto.randomUUID(), code, reason: 'ほかの人', seen: { token: W.editTokenOf(cur) }, values: { name } }, { open: true });
  };
  await as('master_edit', () => save1('m05', 'まとめ 5_K-60'));
  const p = (await dry()).plan;
  assert.deepEqual(p.pending.map((x) => x.code), ['m05']);
  let n = 0;
  const save = async (...a) => { if (n++ === 0) await save1('m05', 'まとめ 5 改_K-60'); return W.saveSku(...a); };
  const r = await apply(p.hash, {}, { save });
  assert.equal(r.exitCode, 1);
  assert.deepEqual(r.result.skipped.map((x) => [x.code, x.why]), [['m05', 'changed_since_plan']]);
  assert.equal((await nameOf('m05')).name, 'まとめ 5 改_K-60');
  const m05 = await sid('m05');
  // 0 回目 = 前に済んだ番号 (使わない)・1 回目 = ほかの人の直しで version_conflict の記録
  assert.equal((await one(`select status from ops.master_edit_requests where request_id = $1`, [T.stripRequestId(m05, 'まとめ 5_K-60', 'まとめ 5', 0)])).status, 'done');
  assert.equal((await one(`select status, error ->> 'reason' as why from ops.master_edit_requests where request_id = $1`, [T.stripRequestId(m05, 'まとめ 5_K-60', 'まとめ 5', 1)])).why, 'version_conflict');
});
await ta('[A8] 外した後に別の印を付けられ、もう一度外した商品 = 済んだものは最後に外したときの前の名前 (ハッシュは書く前と同じ = 同じハッシュで続けられる)', async () => {
  const cur = await W.readCurrent(db, await sid('m04'), W.jstDate());
  await as('master_edit', () => W.saveSku(db, { actor: 'other@test', requestId: crypto.randomUUID(), code: 'm04', reason: 'ほかの人', seen: { token: W.editTokenOf(cur) }, values: { name: 'まとめ 4_K-50' } }, { open: true }));
  const p0 = (await dry()).plan;
  const it = p0.pending.find((x) => x.code === 'm04');
  assert.deepEqual([it.from, it.to], ['まとめ 4_K-50', 'まとめ 4']);
  const r = await apply(p0.hash, { limit: 1 });   // m05 (A7 で「まとめ 5 改_K-60」) も外す計画に入っている = 1 件だけ
  assert.deepEqual(r.result.wrote.map((x) => x.code), ['m04']);
  assert.equal((await nameOf('m04')).name, 'まとめ 4');
  const p1 = (await dry()).plan;
  const d = p1.done.find((x) => x.code === 'm04');
  assert.equal(d.from, 'まとめ 4_K-50', '前の記録 (まとめ 4_K-60 → まとめ 4) ではなく最後の記録');
  assert.equal(p1.hash, p0.hash);
});

// ═══════════ N NE へ送る ═══════════
console.log('[N] NE へ送る');
await ta('[N1] 外した名前は「NEへ送る」の send (人の直し)・外さない商品の名前は出ない', async () => {
  const u = await units();
  assert.equal(u.ok, true, JSON.stringify(u.problems));
  const nameUnit = (code) => u.units.find((x) => x.code_norm === code && x.col === 'name');
  for (const c of ['t04', 'm01', 'm02', 'm03', 'm04', 'm05']) {
    const x = nameUnit(c);
    assert.ok(x, `${c} の名前の単位`);
    assert.equal(x.status, 'send', `${c}: ${JSON.stringify(x)}`);
  }
  for (const c of ['t02', 't03', 't05', 's01']) assert.equal(nameUnit(c), undefined, c);
});

await ta('[G1] 🆕 --gate none (中原さん 10/23): 資材の DB を見ずに全部外す・印の重なりも全部・同じ名前になっても外す・短い / コードと同じは外さない・済んだ後は何もしない', async () => {
  assert.equal(T.stripAllTags('アロマ_白プチ_梱機プ').to, 'アロマ');
  assert.deepEqual(T.stripAllTags('アロマ_白プチ_梱機プ').tags, ['梱機プ', '白プチ']);
  assert.equal(T.stripAllTags('ふつうの名前'), null);
  assert.equal(T.parseStripArgs(['--actor', ACTOR, '--gate', 'none']).gate, 'none', '資材の DB の引数は要らない');
  const NONE = { postage: new Map(), packing: null, meta: { gate: 'none' } };
  const d = await dry('watcher', { gate: 'none', evidence: NONE });
  const to = Object.fromEntries(d.plan.pending.map((p) => [p.code, p.to]));
  assert.equal(to.t02, 'エプロン 110cm', '郵便料金の登録が無くても外す');
  assert.equal(to.t05, 'ハニー 20ml', '梱包の登録が無くても外す');
  assert.equal(to.t06, 'アロマ', '印の重なりは全部外す');
  assert.equal(to.t08, 'オイルB', '同じ名前になっても外す');
  assert.equal(to.t09, 'オイルB');
  const skip = Object.fromEntries(d.plan.skipped.map((s) => [s.code, s.why]));
  assert.equal(skip.t07, 'name_short');
  assert.equal(skip['ab-1'], 'name_invalid');
  assert.ok(!('t10' in to) && !('t12' in to), '印の無い商品は計画に入らない');
  const r = await apply(d.plan.hash, { gate: 'none', evidence: NONE, chunk: 50 });
  assert.equal(r.exitCode, 0, JSON.stringify(r.result && { stopped: r.result.stopped, failed: r.result.failed, skipped: r.result.skipped, refused: r.result.refused }));
  for (const [c, n] of Object.entries({ t02: 'エプロン 110cm', t06: 'アロマ', t08: 'オイルB', t09: 'オイルB' })) {
    const x = await nameOf(c);
    assert.equal(x.name, n, c);
    assert.equal(x.pname, n, `${c} の商品の名前もそろう`);
  }
  const again = await dry('watcher', { gate: 'none', evidence: NONE });
  assert.equal(again.plan.pending.length, 0, '全部済んだ後は何も書かない');
});

fs.rmSync(TMP, { recursive: true, force: true });
console.log(`\n${passed} 件 ok`);
