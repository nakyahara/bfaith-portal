/**
 * test-master-gone-day.mjs — 「NE から消えた → やめる」の日数の許可 (0076・中原さんの決定 10/13 夜 B) を PGlite で確かめる
 *
 * 固定する契約:
 *   [M]  ops.sku_gone_from_ne_check = 0072 の版 (前の最新) から写して 🆕 0076 の行だけ足した・後の migration が作り直していない・
 *        security definer の関数は search_path = pg_catalog, pg_temp・public の実行権なし・一時の表なし
 *   [A]  許可が無い = 3 日 (1 日では absent_short) / 許可 (1 日) がある = 1 日でやめられる (答え・根拠に day_override・登録の状態の履歴にも) /
 *        期限切れの許可 = 効かない (3 日に戻る) / 同じ SKU は期限の中の一番新しい許可が効く
 *   [B]  ほかの止まる理由は許可があっても止まる (出品の構成・セットの構成品)
 *   [P]  CLI (master-gone-from-ne.mjs) の計画のハッシュに許可の事実が入る = dry-run の後に許可が変わると --apply は 1 件も変えない / 同じなら通る
 *   [W]  許可を書けるのは master_ops の関数だけ (master_edit・watcher = 42501・表に直接 = 42501・持ち主の手も印が無ければ断る・変える / 消す = 断る)・
 *        1 つでも足せないコード = 何も書かない・同じ request_id = 前の答え・違う中身 = request_id_reused
 *   [K]  許可を足す CLI (master-gone-day-override.mjs): 引数・確かめ (読むだけ・許可を足したらやめてよいか)・足す (だれ・理由・日数・期限・DB のロールが残る)
 * 使い方: node scripts/test-master-gone-day.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles, GONE_DAY_OVERRIDE_FUNCTIONS, GONE_DAY_OVERRIDE_OWNER_ONLY_FUNCTIONS } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { cancelSkuGoneFromNe, gonePlanHash } from '../lib/master-gone.mjs';
import { runGone } from './company-db/master-gone-from-ne.mjs';
import { parseDayArgs, runDayOverride, okWithDays } from './company-db/master-gone-day-override.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIG = path.join(ROOT, 'db', 'company', 'migrations');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'goneday-'));
process.env.DATA_DIR = DATA_TMP;
const watchdog = setTimeout(() => { console.error('  NG  試験が 600 秒で終わらない (止める)'); process.exit(1); }, 600000);
watchdog.unref();

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const quiet = () => {};

// ═══════════ [M] migration の写し ═══════════
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
function strip0076(t) {
  const out = [];
  let skip = false;
  for (const l of t.split('\n')) {
    if (/-- 🆕 0076 ここから/.test(l)) { skip = true; continue; }
    if (/-- 🆕 0076 ここまで/.test(l)) { skip = false; continue; }
    if (skip || /🆕 0076/.test(l)) continue;
    out.push(l);
  }
  return out.join('\n');
}
const F76 = migFiles.find((f) => f.startsWith('0076_'));
const M76 = readMig(F76);
console.log('[M] migration の写し');
await ta('[M1] ops.sku_gone_from_ne_check = 0072 の版 (前の最新) から写して 🆕 0076 の行だけ足した・後の migration が作り直していない', async () => {
  const name = 'ops.sku_gone_from_ne_check';
  const src = migFiles.filter((f) => f < '0076' && fnText(readMig(f), name)).pop();
  assert.ok(src && src.startsWith('0072'), `前の最新の版が ${src}`);
  const before = fnText(readMig(src), name);
  const now = fnText(M76, name);
  assert.notEqual(now, before);
  assert.equal(strip0076(now), before, '印の行を外すと前の版と違う');
  for (const later of migFiles.filter((x) => x > F76)) assert.equal(fnText(readMig(later), name), null, `${later} も作り直している`);
});
await ta('[M2] security definer の関数 = search_path = pg_catalog, pg_temp・public の実行権を外す・一時の表なし', async () => {
  assert.doesNotMatch(M76, /\b(create\s+(temp|temporary)\s+table|on commit drop)\b/i);
  const defs = [...M76.matchAll(/create (?:or replace )?function ([a-z_.]+)\(([^)]*)\)[^$]*?security definer[^$]*?as \$\$/g)];
  assert.equal(defs.length, 3);
  for (const d of defs) {
    assert.match(d[0], /set search_path = pg_catalog, pg_temp/, d[1]);
    assert.ok(M76.includes(`revoke all on function ${d[1]}(`), `${d[1]} の public の実行権`);
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
const opsDb = { query: async (t, p) => { await pg.query('set role master_ops'); try { return await pg.query(t, p); } finally { await pg.query('set role deploy'); } } };
// 取引をまたぐ lib (begin … commit) は、取引の間ずっと master_ops にする
async function asOps(fn) { await pg.query('set role master_ops'); try { return await fn({ query: (t, p) => pg.query(t, p) }); } finally { await pg.query('set role deploy'); } }

const single = (code) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
const SINGLES = ['d01', 'd02', 'd03', 'd04', 'd05', 'd06', 'd07', 'd08', 'c01', 'b01', 'b02', 'b03', 'b04', 'b05', 'b06', 'b07'];
const PLAN = {
  skus: [...SINGLES.map(single), { ...single('dset'), kind: 'set', cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } }, { ...single('bset'), kind: 'set', cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } }],
  variationGroups: [], setComponents: [{ parentCode: 'dset', childCode: 'c01', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], reorder: { available: false, runId: null, reason: '試験' },
};
const lr = await runInitialLoad(db, PLAN, { log: quiet, runId: 'goneday_load_1', host: 'test', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
await forceNewOpen(db, Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company'])));
const sid = async (code) => String((await one('select sku_id from core.skus where code = $1', [code])).sku_id);
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))?.state ?? null;
{
  const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', 'main', 'r-d04', 'active') returning listing_id`)).listing_id;
  await q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('d04')]);
}

// 完全な NE の取得に無い日数 (0070) = 記録の回を置いて本物の ops._record_ne_absence を流す (test-master-gone.mjs と同じ作り)。今日の取得 1 回だけ = 1 / 3 日
const jstToday = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
async function obsWithout(gone = []) {
  const rows = await q(`select k.code_norm, k.sku_kind from core.skus k where k.company_id = 1 and k.sku_kind in ('single', 'set') order by k.code_norm`);
  return { format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' }, trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' },
    rows: rows.filter((r) => !gone.includes(r.code_norm)).map((r) => (r.sku_kind === 'set' ? [r.code_norm, 'set', null, null, null] : [r.code_norm, 'single', 'ok', null, null])) };
}
let runN = 0;
async function feed(gone, when) {
  const obs = await obsWithout(gone);
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.parent_gate_protocol', '1', true)");
    const id = (await pg.query(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
        evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by)
      values (1, $1, $2, repeat('1', 64), $3, $3, 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`,
      [`mc_20301013T${String(++runN).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`, `ne_${crypto.randomBytes(4).toString('hex')}`, when.toISOString()])).rows[0].result_id;
    await pg.query('select ops._record_ne_absence($1, $2::jsonb)', [id, JSON.stringify(obs)]);
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
}
const GONE = ['d01', 'd02', 'd03', 'd04', 'd05', 'd06', 'd07', 'd08', 'c01', 'b02', 'b03', 'b04', 'b05', 'b06', 'b07', 'bset'];   // b01 = 一番新しい取得にある
await feed(GONE, new Date(Math.max(Date.now() - 20 * 60e3, Date.parse(`${jstToday()}T00:01:00+09:00`))));
const check = async (code) => (await one('select ops.sku_gone_from_ne_check($1::bigint) as r', [await sid(code)])).r;
const reasons = (c) => c.problems.map((p) => p.reason).sort();
/** 許可を足す (master_ops・本物の関数) */
const addOv = (codes, days, { valid = 7, rid = crypto.randomUUID(), actor = 'naka@test', reason = '中原さんの決定 B (1 日前倒し)' } = {}) =>
  opsDb.query('select ops.add_sku_gone_day_overrides($1::uuid, $2, $3, $4::text[], $5, $6) as r', [rid, actor, reason, codes, days, valid]).then((r) => r.rows[0].r);

// ═══════════ [A] 許可の日数 ═══════════
console.log('[A] 許可の日数');
await ta('[A1] 許可が無い = 3 日 (1 日では「日数が足りない」・ほかの止まる理由は無い)', async () => {
  const c = await check('d01');
  assert.deepEqual(reasons(c), ['absent_short']); assert.equal(c.streak, 1); assert.equal(c.required_runs, 3); assert.equal(c.day_override, undefined);
});
let OV1;
await ta('[A2] 許可 (1 日) がある = 1 日でやめられる・答えと根拠に day_override・やめると登録の状態の履歴にも許可が残る', async () => {
  OV1 = await addOv(['d01', 'd02'], 1);
  assert.equal(OV1.overrides.length, 2);
  const c = await check('d01');
  assert.equal(c.ok, true, JSON.stringify(c.problems)); assert.equal(c.required_runs, 1);
  assert.equal(c.day_override.required_days, 1); assert.equal(c.day_override.normal_days, 3); assert.equal(c.day_override.actor_id, 'naka@test');
  assert.equal(c.evidence.required_days, 1); assert.equal(c.evidence.day_override.override_id, OV1.overrides.find((o) => o.code === 'd01').override_id);
  const r = await asOps((d) => cancelSkuGoneFromNe(d, { actor: 'naka@test', code: null, skuId: c.sku_id, reason: 'NE で消えた (決定 B)', requestId: crypto.randomUUID() }, { open: true, requireConfirm: false }));
  assert.equal(r.state, 'cancelled'); assert.equal(r.evidence.day_override.required_days, 1);
  const ev = await one(`select evidence from ops.master_registration_events where sku_id = $1 and to_state = 'cancelled'`, [c.sku_id]);
  assert.equal(ev.evidence.day_override.override_id, c.day_override.override_id);
  assert.equal(await regOf('d01'), 'cancelled');
  // 許可の無い SKU は 3 日のまま
  assert.deepEqual(reasons(await check('d03')), ['absent_short']);
});
await ta('[A3] 期限切れの許可 = 効かない (3 日に戻る)・同じ SKU は一番新しい許可が効く (それが期限切れなら許可なし = 古い許可はよみがえらない)', async () => {
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.gone_day_override_protocol', '1', true)");
    await q(`insert into ops.sku_gone_from_ne_day_overrides (sku_id, required_days, actor_id, reason, request_id, db_user, created_at, expires_at)
      values ($1, 1, 'naka@test', '古い許可', gen_random_uuid(), 't', now() - interval '3 days', now() - interval '1 day')`, [await sid('d03')]);
    await pg.query("select set_config('ops.gone_day_override_protocol', '', true)");
    const c = await check('d03');
    assert.deepEqual(reasons(c), ['absent_short']); assert.equal(c.required_runs, 3); assert.equal(c.day_override, undefined);
  } finally { await pg.query('rollback'); }
  // 🆕 #1709 Codex R1 Low 1: 古い 1 日 (期限の中) + 新しい 3 日 (期限切れ) = 新しい方を選んでから期限を見る = 許可なし (3 日)・古い 1 日はよみがえらない
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.gone_day_override_protocol', '1', true)");
    await q(`insert into ops.sku_gone_from_ne_day_overrides (sku_id, required_days, actor_id, reason, request_id, db_user, created_at, expires_at)
      values ($1, 1, 'naka@test', '古い 1 日 (期限の中)', gen_random_uuid(), 't', now() - interval '2 days', now() + interval '5 days'),
             ($1, 3, 'naka@test', '新しい 3 日 (期限切れ)', gen_random_uuid(), 't', now() - interval '1 day', now() - interval '1 hour')`, [await sid('d03')]);
    await pg.query("select set_config('ops.gone_day_override_protocol', '', true)");
    const c = await check('d03');
    assert.deepEqual(reasons(c), ['absent_short']); assert.equal(c.required_runs, 3); assert.equal(c.day_override, undefined);
  } finally { await pg.query('rollback'); }
  await addOv(['d02'], 3);   // d02 に新しい許可 (3 日) = 前の 1 日より新しい
  const c2 = await check('d02');
  assert.deepEqual(reasons(c2), ['absent_short']); assert.equal(c2.required_runs, 3); assert.equal(c2.day_override.required_days, 3);
});

// ═══════════ [B] ほかの止まる理由 ═══════════
console.log('[B] ほかの止まる理由');
await ta('[B1] 許可があっても、出品の構成・セットの構成品は止まる (日数の理由だけが消える)', async () => {
  await addOv(['d04', 'c01'], 1);
  assert.deepEqual(reasons(await check('d04')), ['listing_component']);
  assert.deepEqual(reasons(await check('c01')), ['set_component']);
  const e2 = await errOf(async () => opsDb.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint)', [crypto.randomUUID(), 'naka@test', 'x', await sid('d04')]));
  assert.match(e2?.message ?? '', /not_gone_from_ne: .*販路の出品の構成/);
  assert.equal(await regOf('d04'), 'available');
});

await ta('[B2] 許可 (1 日) があっても止まる理由 (表): in_ne_latest・in_ne_codes・fetch_old・live_file・listing_component_deleted・parent_of_others・own_component_request (それぞれ取引の中で作って巻き戻す)', async () => {
  await addOv(['b01', 'b02', 'b03', 'b04', 'b05', 'b06', 'b07', 'bset'], 1);
  const TABLE = [
    ['in_ne_latest', 'b01', async () => {}],
    ['in_ne_codes', 'b02', async () => q(`insert into ops.master_ne_codes (code_norm, kind, state, ne_code, spellings) values ('b02', 'product', 'ok', 'b02', '["b02"]'::jsonb)`)],
    ['fetch_old', 'b03', async () => {
      await pg.query("select set_config('ops.parent_gate_protocol', '1', true)");
      const old = (await one(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
          evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by)
        values (1, 'mc_old_x', 'ne_old', repeat('1', 64), now() - interval '40 hours', now() - interval '1 hour', 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`)).result_id;
      await pg.query("select set_config('ops.ne_absence_protocol', '1', true)");
      await q('update ops.master_ne_absence_mark set result_id = $1', [old]);
      await q('update ops.master_ne_absence set last_absent_result_id = $1', [old]);
    }],
    ['live_file', 'b04', async () => {
      await q('alter table ops.ne_reg_export_items disable trigger trg_ne_reg_items_parent_gate');   // 0068 の門はこの試験の外 (取引ごと戻す)
      await q(`with e as (insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
          values ('products', 'ne-reg-single-v2', 'syohin_code', 'utf8', false, 1, 1, repeat('a', 64), repeat('b', 64), repeat('c', 64), '\\x00', gen_random_uuid(), 'x', current_date, 't') returning export_id)
        insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by, state)
        select e.export_id, k.sku_id, k.code_norm, k.code, k.sku_kind, repeat('e', 64), '{}'::jsonb, repeat('f', 64), 1, 1, 't', 'built' from e, core.skus k where k.code = 'b04'`);
    }],
    ['listing_component_deleted', 'b05', async () => {
      const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', 'main', 'r-b05', 'deleted') returning listing_id`)).listing_id;
      await q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('b05')]);
    }],
    ['parent_of_others', 'b06', async () => {
      await pg.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
      await q(`update core.products set parent_product_id = (select product_id from core.skus where code = 'b06'), parent_set_by = 'manual' where product_id = (select product_id from core.skus where code = 'd07')`);
    }],
    ['own_component_request', 'bset', async () => q(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id) values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`,
      [await sid('bset'), JSON.stringify([{ sku_id: Number(await sid('b07')), code: 'b07', qty: 1, sort: 1 }]), '9'.repeat(64)])],
  ];
  for (const [want, code, setup] of TABLE) {
    await pg.query('begin');
    try {
      await setup();
      const c = await check(code);
      assert.equal(c.required_runs, 1, code); assert.ok(c.day_override, code);
      assert.deepEqual(reasons(c), [want], `${code}: ${JSON.stringify(c.problems)}`);
      const e = await errOf(async () => pg.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint)', [crypto.randomUUID(), 'naka@test', 'x', await sid(code)]));
      assert.match(e?.message ?? '', /not_gone_from_ne/, code);
    } finally { await pg.query('rollback'); }
  }
  // 止まる理由が無いもの (表の後) = 許可 1 日でやめてよい
  for (const code of ['b02', 'b03', 'b04', 'b05', 'b06', 'bset']) assert.equal((await check(code)).ok, true, code);
});

// ═══════════ [P] 計画のハッシュ ═══════════
console.log('[P] 計画のハッシュ');
await ta('[P1] CLI: dry-run の計画のハッシュに許可が入る = dry-run の後に許可が変わる (新しい許可) と --apply は 1 件も変えない / 許可が同じなら通る', async () => {
  await addOv(['d05', 'd06'], 1);
  const dry = await runGone(opsDb, { apply: false, codes: ['d05', 'd06'] }, { log: quiet });
  assert.equal(dry.checks.filter((r) => r.ok).length, 2);
  assert.ok(dry.checks.every((r) => r.evidence.day_override));
  await addOv(['d06'], 1, { reason: '同じ日数で許可を足し直した' });   // 日数は同じでも許可の事実が変わる
  const ng = await asOps((d) => runGone(d, { apply: true, codes: ['d05', 'd06'], expectHash: dry.planHash, actor: 'naka@test', reason: 'NE で消えた (決定 B)' }, { log: quiet }));
  assert.equal(ng.exitCode, 1); assert.equal(ng.error.reason, 'plan_mismatch');
  assert.equal(await regOf('d05'), 'available'); assert.equal(await regOf('d06'), 'available');
  const dry2 = await runGone(opsDb, { apply: false, codes: ['d05', 'd06'] }, { log: quiet });
  assert.notEqual(dry2.planHash, dry.planHash);
  const ok = await asOps((d) => runGone(d, { apply: true, codes: ['d05', 'd06'], expectHash: dry2.planHash, actor: 'naka@test', reason: 'NE で消えた (決定 B)' }, { log: quiet }));
  assert.equal(ok.exitCode, 0, JSON.stringify(ok.error));
  assert.equal(await regOf('d05'), 'cancelled'); assert.equal(await regOf('d06'), 'cancelled');
  // 許可が無い時のハッシュとも違う (許可の事実が入っている)
  assert.notEqual(gonePlanHash(dry2.checks), gonePlanHash(dry2.checks.map((r) => ({ ...r, evidence: { ...r.evidence, day_override: undefined } }))));
});

// ═══════════ [W] 許可を書ける人と形 ═══════════
console.log('[W] 許可を書ける人');
await ta('[W1] 書けるのは master_ops の関数だけ: master_edit・watcher = 42501・表に直接 = 42501・持ち主の手も印が無ければ断る・変える / 消す / TRUNCATE = 断る', async () => {
  const sig = GONE_DAY_OVERRIDE_FUNCTIONS[0];
  for (const role of ['master_edit', 'watcher', 'watch_writer', 'public']) assert.equal((await one(`select has_function_privilege($1, $2::regprocedure, 'execute') as x`, [role, sig])).x, false, role);
  assert.equal((await one(`select has_function_privilege('master_ops', $1::regprocedure, 'execute') as x`, [sig])).x, true);
  for (const f of GONE_DAY_OVERRIDE_OWNER_ONLY_FUNCTIONS) for (const role of ['master_ops', 'master_edit', 'public']) assert.equal((await one(`select has_function_privilege($1, $2::regprocedure, 'execute') as x`, [role, f])).x, false, `${role} ${f}`);
  const e1 = await errOf(() => as('master_edit', () => pg.query('select ops.add_sku_gone_day_overrides(gen_random_uuid(), $1, $2, $3::text[], 1, 7)', ['naka@test', 'x', ['d07']])));
  assert.equal(e1?.code, '42501');
  const e2 = await errOf(async () => opsDb.query(`insert into ops.sku_gone_from_ne_day_overrides (sku_id, required_days, actor_id, reason, request_id, db_user, expires_at) values ($1, 1, 'a@b', 'x', gen_random_uuid(), 't', now() + interval '1 day')`, [await sid('d07')]));
  assert.equal(e2?.code, '42501');
  const e3 = await errOf(async () => q(`insert into ops.sku_gone_from_ne_day_overrides (sku_id, required_days, actor_id, reason, request_id, db_user, expires_at) values ($1, 1, 'a@b', 'x', gen_random_uuid(), 't', now() + interval '1 day')`, [await sid('d07')]));
  assert.match(e3?.message ?? '', /gone_day_override/);
  assert.match((await errOf(() => q('update ops.sku_gone_from_ne_day_overrides set required_days = 1')))?.message ?? '', /追記だけ/);
  assert.match((await errOf(() => q('delete from ops.sku_gone_from_ne_day_overrides')))?.message ?? '', /追記だけ/);
  assert.ok(await errOf(() => q('truncate ops.sku_gone_from_ne_day_overrides')));
});
await ta('[W2] 形: 1 つでも足せないコード (無い・やめた・例外) = 何も書かない・日数 1〜3・期限 1〜14 日・同じ SKU 2 回・人と理由 / 同じ request_id = 前の答え・違う中身 = request_id_reused', async () => {
  const n0 = (await one('select count(*)::int as n from ops.sku_gone_from_ne_day_overrides')).n;
  for (const [codes, days, valid, re] of [[['d07', 'zz-none'], 1, 7, /zz-none \(Company DB に無い\)/], [['d07', 'd01'], 1, 7, /d01 \(登録の状態が cancelled\)/], [['d07'], 0, 7, /日数は 1〜3/],
    [['d07'], 4, 7, /日数は 1〜3/], [['d07'], 1, 15, /有効な日数/], [['d07', 'D07'], 1, 7, /同じコード/]]) {
    const e = await errOf(() => addOv(codes, days, { valid }));
    assert.match(e?.message ?? '', re, JSON.stringify(codes));
  }
  assert.match((await errOf(() => addOv(['d07'], 1, { actor: 'naka' })))?.message ?? '', /人 \(メール\)/);
  assert.match((await errOf(() => addOv(['d07'], 1, { reason: ' ' })))?.message ?? '', /理由/);
  assert.equal((await one('select count(*)::int as n from ops.sku_gone_from_ne_day_overrides')).n, n0);
  const rid = crypto.randomUUID();
  const a = await addOv(['d07', 'd08'], 1, { rid });
  const b = await addOv(['d08', 'd07'], 1, { rid });
  assert.equal(b.replayed, true); assert.deepEqual(b.overrides, a.overrides);
  assert.match((await errOf(() => addOv(['d07', 'd08'], 2, { rid })))?.message ?? '', /request_id_reused/);
  assert.match((await errOf(() => addOv(['d07'], 1, { rid })))?.message ?? '', /request_id_reused/);
  const row = await one('select actor_id, reason, required_days, db_user, expires_at - created_at as v from ops.sku_gone_from_ne_day_overrides where request_id = $1 limit 1', [rid]);
  assert.deepEqual([row.actor_id, row.required_days, row.db_user], ['naka@test', 1, 'master_ops']);
});

// ═══════════ [K] 許可を足す CLI ═══════════
console.log('[K] 許可を足す CLI');
await ta('[K1] 引数: コードが要る・--apply は --actor・--reason・--yes が要る・日数 1〜3・期限 1〜14', async () => {
  assert.throws(() => parseDayArgs([]), /コード/);
  assert.throws(() => parseDayArgs(['--codes', 'a', '--apply', '--reason', 'x', '--yes']), /--actor/);
  assert.throws(() => parseDayArgs(['--codes', 'a', '--apply', '--actor', 'a@b', '--yes']), /--reason/);
  assert.throws(() => parseDayArgs(['--codes', 'a', '--apply', '--actor', 'a@b', '--reason', 'x']), /--yes/);
  assert.throws(() => parseDayArgs(['--codes', 'a', '--days', '0']), /1〜3/);
  assert.throws(() => parseDayArgs(['--codes', 'a', '--valid-days', '30']), /1〜14/);
  // 🆕 #1709 Codex R1 M1: --apply は --request-id (確かめが出した UUID) が要る
  assert.throws(() => parseDayArgs(['--codes', 'a,b', '--days', '1', '--apply', '--actor', 'a@b', '--reason', '決定 B', '--yes']), /--request-id/);
  assert.throws(() => parseDayArgs(['--codes', 'a', '--request-id', 'x']), /UUID/);
  const rid = crypto.randomUUID();
  const o = parseDayArgs(['--codes', 'a,b', '--days', '1', '--apply', '--request-id', rid.toUpperCase(), '--actor', 'a@b', '--reason', '決定 B', '--yes']);
  assert.deepEqual([o.codes, o.days, o.validDays, o.apply, o.requestId], [['a', 'b'], 1, 7, true, rid]);
  assert.equal(okWithDays({ sku_id: '1', ok: false, problems: [{ reason: 'absent_short' }], streak: 1 }, 1), true);
  assert.equal(okWithDays({ sku_id: '1', ok: false, problems: [{ reason: 'absent_short' }, { reason: 'listing_component' }], streak: 1 }, 1), false);
  // 🆕 #1709 Codex R1 M2: 今の r.ok を信じない (今の 1 日の許可で ok・1 日無い SKU に --days 3 = やめない)
  assert.equal(okWithDays({ sku_id: '1', ok: true, problems: [], streak: 1 }, 3), false);
  assert.equal(okWithDays({ sku_id: '1', ok: true, problems: [], streak: 3 }, 3), true);
  assert.equal(okWithDays({ sku_id: null, ok: false, problems: [{ reason: 'no_sku' }] }, 1), false);
});
await ta('[K2] 確かめ (既定) = 何も書かない・許可を足したらやめてよいか / --apply = 足す (master_ops・だれ・理由・期限)', async () => {
  const n0 = (await one('select count(*)::int as n from ops.sku_gone_from_ne_day_overrides')).n;
  const lines = [];
  const file = path.join(DATA_TMP, 'codes.txt');
  fs.writeFileSync(file, 'd03\n# コメント\nd04\n');
  const dry = await runDayOverride(opsDb, parseDayArgs(['--codes-file', file, '--days', '1']), { log: (l) => lines.push(l) });
  assert.equal(dry.mode, 'dry-run');
  assert.equal((await one('select count(*)::int as n from ops.sku_gone_from_ne_day_overrides')).n, n0);
  assert.ok(lines.some((l) => /✅ d03/.test(l)) && lines.some((l) => /⛔ d04/.test(l)), lines.join('\n'));
  // 今の許可 (b02 = 1 日) と違う --days 3 = 1/3 日なので やめない と出す (今の ok を信じない)
  const l3 = [];
  await runDayOverride(opsDb, parseDayArgs(['--codes', 'b02', '--days', '3']), { log: (l) => l3.push(l) });
  assert.ok(l3.some((l) => /⛔ b02/.test(l)) && l3.some((l) => /今の許可 1 日/.test(l)), l3.join('\n'));
  const rid = lines.map((l) => /--request-id ([0-9a-f-]{36})/.exec(l)).find(Boolean)?.[1];
  assert.ok(rid && rid === dry.requestId, '確かめが --apply に使う request_id を出す');
  const args = ['--codes', 'd03', '--days', '1', '--valid-days', '3', '--apply', '--request-id', rid, '--actor', 'Naka@Test', '--reason', '決定 B', '--yes'];
  const r = await runDayOverride(opsDb, parseDayArgs(args), { log: quiet });
  assert.equal(r.exitCode, 0); assert.equal(r.added.overrides[0].code, 'd03');
  // 🆕 #1709 Codex R1 M1: 同じコマンドの押し直し (接続が切れて答えが分からなかった) = 前の答え・許可は増えない・期限も延びない
  const nBefore = (await one('select count(*)::int as n, max(expires_at)::text as e from ops.sku_gone_from_ne_day_overrides'));
  const again = await runDayOverride(opsDb, parseDayArgs(args), { log: quiet });
  assert.equal(again.exitCode, 0); assert.equal(again.added.replayed, true); assert.deepEqual(again.added.overrides, r.added.overrides);
  assert.deepEqual(await one('select count(*)::int as n, max(expires_at)::text as e from ops.sku_gone_from_ne_day_overrides'), nBefore);
  const row = await one(`select o.actor_id, o.reason, o.required_days, o.db_user, extract(epoch from o.expires_at - o.created_at)::int as v from ops.sku_gone_from_ne_day_overrides o join core.skus k on k.sku_id = o.sku_id where k.code = 'd03'`);
  assert.deepEqual(row, { actor_id: 'naka@test', reason: '決定 B', required_days: 1, db_user: 'master_ops', v: 3 * 86400 });
  assert.equal((await check('d03')).ok, true);
  const bad = await runDayOverride(opsDb, parseDayArgs(['--codes', 'zz-none', '--apply', '--request-id', crypto.randomUUID(), '--actor', 'a@b', '--reason', 'x', '--yes']), { log: quiet });
  assert.equal(bad.exitCode, 1);
});

await pg.close();
try { fs.rmSync(DATA_TMP, { recursive: true, force: true }); } catch { /* */ }
console.log(`${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
