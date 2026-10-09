/**
 * test-master-parent-gate.mjs — 代表 (親) の数えと門 (0068・AI_reference CompanyDB構想/20 v7 §②・§⑥・§⑩ PR-6) を PGlite で確かめる
 *
 * 固定する契約:
 *   [P0] 0068: 広げてよいキーに products.parent・関数の権限の形 (security definer・search_path・public の実行権なし)
 *   [P1] 生の数え ops.parent_raw_gate = 6 つ (mismatch / incomparable / ambiguous / missing / two_level / loop)・1 つの単品は 1 つの数えだけ・
 *        判定表 (登録の状態 × 品目の状態): partial・quarantined・ne_confirmed・available (backfill) = 数える / draft・built・superseded・issued・import_declared・failed・cancelled = 外す /
 *        セット・例外 = 外す・外した数を証跡に・一覧 (drift-list) の理由
 *   [P2] 観測の形の確かめ (形が違えば全部断る・同じコードが 2 行 = 比べられない)
 *   [P3] 構造の数え (全部の商品の 2 段・循環 = widen の判定がその場で数える)
 *   [P4] 照合 ② の記録 ops.record_parent_gate: 形・今日の取得だけ・未来の時刻・封 (結果の JSON の sha256) が要る・同じ回は 1 回・DB が数える (呼び手の数は受けない)・
 *        表は関数でだけ書く・追記だけ
 *   [P5] 門 (品目の表の trigger): 持ち主 load の間 = 閉じない (今の単品の登録を止めない) / company = 数えが 0 でない・記録が無い・別の回・今日でない = 新しい CSV を作らない・配らない /
 *        通すもの = 作り直し (前の品目が failed)・申告・照合・使わない・failed / 0 の今日の同じ回 = 開く
 *   [P6] 判断の画面の「差を残す」は代表の候補に使えない (前からの候補も DB が断る)・ほかの列は今までどおり
 *   [P7] JS: 観測 (nModelOf → parentObservations)・選べる解決・承認の指紋の意味の版・朝の要約 (先頭の ⚠️・load = 知らせだけ・company = 閉じた・記録できない)
 *   [P8] 照合の実行口 (runCompare) が封の後に記録する・recordParentGate の状態 (ok / not_applied / not_configured / failed / no_fetch)
 *   [P9] drift-list (読むだけ): 6 つの数えと一覧・引数・数えられないときは止まる
 *   [P10] 関数の作り直しは 0059 の版との差が決めた所だけ (ops._widen_judge・ops.master_widen_allowed_keys)・後の migration が作り直していない
 * 使い方: node scripts/test-master-parent-gate.mjs   (widen の判定の全部の道・ロールは scripts/test-master-parent-gate-pg.mjs)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { forceNewOpen, seedParentGate, hex, ZERO_GATE } from './fixtures/master-widen.mjs';
import { seedActiveEpoch } from './fixtures/master-epoch.mjs';
import { nModelOf, resolutionsFor, decisionPrint, PARENT_SEMANTIC_SUFFIX } from '../apps/company-db/master-compare/compare-ne.mjs';
import { parentObservations, recordParentGate, parentTrouble, parentNote, readParentCounts, repSpellingsOf, PARENT_COUNT_KEYS } from '../apps/company-db/master-compare/parent-gate.mjs';
import { summaryLine, runCompare } from '../apps/company-db/master-compare/run.mjs';
import { driftList, formatDriftList, parseArgs } from '../apps/company-db/master-compare/drift-list.mjs';
import { jstDateStr } from '../lib/jst-date.js';

let passed = 0;
async function ta(name, fn) { const t = Date.now(); try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company' };   // products.parent = load (今の本番の形)
const COMPANY = { ...BASE, 'products.parent': 'company' };
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const setOf = (code) => ({ code, name: code, kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } });
const SINGLES = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12', 'm13', 'm14', 'm15', 'm16',
  'd01', 'd02', 'd03', 'd04', 'd05', 'd06', 'd07', 'd08', 'd09', 'd10', 'd11'];
const plan = (codes) => ({ skus: [...codes.map(single), setOf('s01'), { ...single('x01'), kind: 'exception' }], variationGroups: [],
  setComponents: [{ parentCode: 's01', childCode: codes[0], qty: 1, source: 'ne' }], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] });

/** 1 つの DB (0068 まで・本物の初期ロード・段階 new_open・products.parent = load) */
async function setupDb(codes) {
  const pg = new PGlite();
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  const r = await runInitialLoad(db, plan(codes), { log: quiet, runId: `pg_load_${crypto.randomBytes(3).toString('hex')}`, host: 'test' });
  assert.equal(r.ok, true, r.error);
  await forceNewOpen(db, BASE);
  const q = async (sql, p) => (await db.query(sql, p)).rows;
  const one = async (sql, p) => (await q(sql, p))[0];
  const tx = async (fn, sets = []) => {
    await db.query('begin');
    try { for (const s of sets) await db.query(s); const v = await fn(); await db.query('commit'); return v; } catch (e) { await db.query('rollback'); throw e; }
  };
  const PARENT_TX = ["select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())"];
  const pidOf = async (code) => (await one('select product_id from core.skus where code = $1', [code])).product_id;
  const tag = (display) => tx(async () => (await one('insert into core.products (company_id, display_code, name) values (1, $1, $1) returning product_id', [display])).product_id);
  const setParent = (childPid, parentPid) => tx(() => db.query("update core.products set parent_product_id = $2, parent_set_by = 'load' where product_id = $1", [childPid, parentPid]), PARENT_TX);
  const setReg = (code, state) => tx(() => db.query(`update ops.master_registrations set state = $2, state_changed_by = 't', state_changed_at = now()
    where sku_id = (select sku_id from core.skus where code = $1)`, [code, state]), ["select set_config('ops.registration_protocol', '1', true)"]);
  const mkItem = async (code) => {
    const e = (await one(`insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
      values ('products', 'ne-reg-single-v2', 'syohin_code', 'utf8', false, 1, 1, repeat('a', 64), repeat('b', 64), repeat('c', 64), '\\x00', gen_random_uuid(), 'x', current_date, 't') returning export_id`)).export_id;
    const i = (await one(`insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by)
      select $1, k.sku_id, k.code_norm, k.code, k.sku_kind, repeat('e', 64), '{}'::jsonb, repeat('f', 64), 1, 1, 't' from core.skus k where k.code = $2 returning item_id`, [e, code])).item_id;
    return { exportId: e, itemId: i };
  };
  const step = async (it, to) => {
    if (to === 'import_declared') {
      const a = (await one("insert into ops.ne_reg_attempts (export_id, sha256, declared_by, result) values ($1, repeat('c', 64), 't', 'ok') returning attempt_id", [it.exportId])).attempt_id;
      return db.query("update ops.ne_reg_export_items set state = 'import_declared', attempt_id = $2 where item_id = $1", [it.itemId, a]);
    }
    const extra = { verified: ", verified_run = 'r', verified_at = now()", failed: ", failed_reason = 'not_in_ne'", superseded: ", superseded_reason = 'x', superseded_correction = 'y'" }[to] || '';
    return db.query(`update ops.ne_reg_export_items set state = $2${extra} where item_id = $1`, [it.itemId, to]);
  };
  const itemPath = async (code, states) => { const it = await mkItem(code); for (const s of states) await step(it, s); return it; };
  return { pg, db, q, one, tx, pidOf, tag, setParent, setReg, mkItem, step, itemPath };
}
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
const obsOf = (rows, { untrusted = [], complete = true, repSpellings = { state: 'ok' } } = {}) => ({ format: 'parent-obs-v1', complete, untrusted, rep_spellings: repSpellings, rows });
const S = (code, rep = null, raw = rep) => [code, 'single', 'ok', rep, rep == null ? null : raw];
const SET = (code) => [code, 'set', null, null, null];
const gate = async (E, obs, detail = true) => (await E.one('select ops.parent_raw_gate(1, $1::jsonb, $2) as r', [JSON.stringify(obs), detail])).r;
const nowRfc = (ms = -60000) => new Date(Date.now() + ms).toISOString();
const FETCH = (at = nowRfc()) => ({ generation_id: `ne_test_${crypto.randomBytes(3).toString('hex')}`, raw_hash: hex('1'), products_complete_at: at, setproducts_complete_at: at });
const runId = () => `mc_${new Date().toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;

const E = await setupDb(SINGLES);
// ── 代表の形 (持ち主 load の間に夜間ロードが付けた形を直接置く) ──
const T = {};
for (const d of ['grpA', 'grpB', 'grpC', 'grpD', 'grpE', 'dupg', 'DUPG', 'L1', 'L2']) T[d] = await E.tag(d);
await E.setParent(await E.pidOf('m01'), T.grpA);            // NE grpA = 一致
await E.setParent(await E.pidOf('m02'), T.grpA);            // NE grpB = 違う
await E.setParent(await E.pidOf('m04'), T.grpA);            // NE 親なし = 違う
await E.setParent(await E.pidOf('m10'), T.dupg);            // NE dupg = 社内に dupg / DUPG の 2 つ = 曖昧
await E.setParent(await E.pidOf('m11'), T.grpC);            // NE grpC と GRPC の 2 つの書き方 = 曖昧
await E.setParent(await E.pidOf('m12'), T.grpC);
await E.setParent(T.grpD, T.grpE);                           // 名札が親を持つ
await E.setParent(await E.pidOf('m13'), T.grpD);            // = m13 は 2 段
await E.setParent(T.L1, T.L2); await E.setParent(T.L2, T.L1); // 循環
await E.setParent(await E.pidOf('m14'), T.L1);              // = m14 は循環
await E.setParent(await E.pidOf('m15'), T.grpA);            // m15 は親を持ち、子 (m16) も持つ = 2 段
await E.setParent(await E.pidOf('m16'), await E.pidOf('m15'));
for (const c of ['d01', 'd02', 'd03', 'd04', 'd05', 'd06', 'd07', 'd08', 'd09', 'd10', 'd11']) await E.setParent(await E.pidOf(c), T.grpB);   // NE は grpA = 数える行は「違う」
// ── 登録の状態 × 品目の状態 (判定表) ──
await E.setReg('d01', 'draft');                                                                // 品目なし = 外す (draft)
await E.setReg('d02', 'draft'); await E.itemPath('d02', []);                                   // built = 外す (draft)
await E.setReg('d03', 'draft'); await E.itemPath('d03', ['issued']);                           // issued = 外す (issued)
await E.setReg('d04', 'ne_pending'); await E.itemPath('d04', ['issued', 'import_declared']);   // import_declared = 外す (issued)
await E.setReg('d05', 'ne_pending'); await E.itemPath('d05', ['issued', 'failed']);            // failed = 外す (failed・復旧中)
await E.setReg('d06', 'draft'); await E.itemPath('d06', ['issued', 'import_declared', 'partial']);   // partial = 数える (どの登録の状態でも)
await E.setReg('d07', 'cancelled');                                                            // やめた = 外す
await E.setReg('d08', 'quarantined');                                                          // NE で直接作られた = 数える
await E.setReg('d09', 'draft'); await E.itemPath('d09', ['superseded']);                       // superseded = 外す (draft)
await E.setReg('d10', 'ne_confirmed');                                                         // NE 確認済み = 数える
await E.setReg('d11', 'draft'); await E.itemPath('d11', ['issued', 'import_declared', 'verified']);   // draft なのに verified (起きないはずの形) = 数える
const NE_ROWS = [S('m01', 'grpa', 'grpA'), S('m02', 'grpb', 'grpB'), S('m03'), S('m04'), S('m05', 'grpa', 'grpA'), ['m06', 'single', 'unknown', null, null],
  /* m07 は NE に無い */ S('m08', 'grpa', 'grpA'), SET('m09'), S('m10', 'dupg', 'dupg'), S('m11', 'grpc', 'grpC'), S('m12', 'grpc', 'GRPC'),
  S('m13', 'grpd', 'grpD'), S('m14', 'l1', 'L1'), S('m15', 'grpa', 'grpA'), S('m16', 'm15', 'm15'),
  ...['d01', 'd02', 'd03', 'd04', 'd05', 'd06', 'd07', 'd08', 'd09', 'd10', 'd11'].map((c) => S(c, 'grpa', 'grpA')), SET('s01')];
const NE1 = obsOf(NE_ROWS, { untrusted: ['m08'] });

await ta('[P0] 0068: 広げてよいキーに products.parent・関数の権限の形 (security definer・search_path・public の実行権なし)', async () => {
  assert.deepEqual((await E.one('select ops.master_widen_allowed_keys() as k')).k, ['listing_components.amazon', 'products.parent', 'skus.sku_kind']);
  const fns = await E.q(`select p.proname, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname in ('parent_raw_gate', 'record_parent_gate', 'parent_gate_state', '_parent_gate_problems',
      'parent_gate_enforced', 'parent_structure_counts', '_parent_loop_products', 'guard_ne_reg_parent_gate', 'master_decision_events_parent_no_accept', 'guard_master_parent_gate_results', '_widen_judge') order by 1`);
  assert.equal(fns.length, 11);
  for (const f of fns) { assert.equal(f.pub, false, f.proname); assert.equal(f.c, 'search_path=pg_catalog, pg_temp', f.proname); }
  assert.deepEqual(fns.filter((f) => f.d).map((f) => f.proname).sort(), ['guard_ne_reg_parent_gate', 'parent_gate_enforced', 'parent_gate_state', 'parent_raw_gate', 'record_parent_gate']);
});

await ta('[P1] 生の数え: 6 つの数え・1 つの単品は 1 つだけ・判定表 (登録の状態 × 品目の状態)・外した数・一覧の理由', async () => {
  const r = await gate(E, NE1);
  assert.equal(r.format, 'parent-raw-v1');
  const byCode = Object.fromEntries(r.items.map((x) => [x.code, [x.class, x.reason]]));
  assert.deepEqual(byCode, {
    m02: ['parent_mismatch', null], m04: ['parent_mismatch', null],
    m05: ['parent_missing', null],
    m06: ['parent_incomparable', 'ne_rep_unknown'], m07: ['parent_incomparable', 'not_in_ne'], m08: ['parent_incomparable', 'ne_untrusted'], m09: ['parent_incomparable', 'ne_is_set'],
    m10: ['parent_ambiguous', 'cdb_candidates_2'], m11: ['parent_ambiguous', 'ne_rep_spellings'], m12: ['parent_ambiguous', 'ne_rep_spellings'],
    m13: ['parent_two_level', null], m15: ['parent_two_level', null], m16: ['parent_two_level', null],
    m14: ['parent_loop', null],
    d06: ['parent_mismatch', null], d08: ['parent_mismatch', null], d10: ['parent_mismatch', null], d11: ['parent_mismatch', null],
  });
  assert.deepEqual(r.counts, { parent_mismatch: 6, parent_incomparable: 4, parent_ambiguous: 3, parent_missing: 1, parent_two_level: 3, parent_loop: 1 });
  // 数える = 単品 27 − 外す 7 (d01 d02 d09 = draft・d03 d04 = issued・d05 = failed・d07 = cancelled) = 20 (m01・m03 は一致)
  assert.equal(r.counted, 20);
  assert.deepEqual(r.excluded, { set: 1, exception: 1, draft: 3, issued: 2, failed: 1, cancelled: 1 });
  assert.deepEqual(r.obs, { rows: NE_ROWS.length, singles: NE_ROWS.length - 2, sets: 2, untrusted: 1, complete: true });
  assert.deepEqual(r.samples.parent_mismatch, ['d06', 'd08', 'd10', 'd11', 'm02', 'm04']);
  const it = r.items.find((x) => x.code === 'd06');
  assert.deepEqual([it.ne_parent, it.cdb_parent, it.reg_state, it.item_state], ['grpA', 'grpB', 'draft', 'partial']);
  // 一覧を出さない (照合の記録) = items が無い・数えは同じ
  const r2 = await gate(E, NE1, false);
  assert.equal(r2.items, null); assert.deepEqual(r2.counts, r.counts);
  // 行が落ちた取得 (complete = false) = NE に無い単品の理由が変わるだけ (数えは同じ)
  const r3 = await gate(E, { ...NE1, complete: false });
  assert.equal(r3.items.find((x) => x.code === 'm07').reason, 'not_in_ne_rows_dropped'); assert.deepEqual(r3.counts, r.counts);
  // 承認の台帳を読まない = 差を残す承認があっても数えは減らない (台帳の表を読む文が無い)
  const src = (await E.one("select prosrc from pg_proc where proname = 'parent_raw_gate'")).prosrc;
  assert.ok(!/master_decision/.test(src), '生の数えは判断の台帳を読まない');
});

await ta('[P2] 観測の形の確かめ (形が違えば全部断る・22023)・同じコードが 2 行 = 比べられない・会社 1 だけ', async () => {
  const bad = [
    { ...NE1, format: 'x' }, { ...NE1, rows: {} }, { ...NE1, untrusted: 'm08' }, { format: 'parent-obs-v1', untrusted: [], rows: [] },
    obsOf([['m01', 'single', 'ok', 'grpa']]), obsOf(['m01']), obsOf([[1, 'single', 'ok', null, null]]), obsOf([['', 'single', 'ok', null, null]]),
    obsOf([['m01', 'exception', 'ok', null, null]]), obsOf([['m01', 'set', 'ok', null, null]]), obsOf([['m01', 'single', 'maybe', null, null]]),
    obsOf([['m01', 'single', 'unknown', 'grpa', 'grpA']]), obsOf([['m01', 'single', 'ok', 'grpa', null]]), obsOf([['m01', 'single', 'ok', 'x'.repeat(201), 'x']]),
    obsOf([['m01', 'single', 'ok', '', 'grpA']]), obsOf([['m01', 'single', 'ok', 'grpa', '']]), obsOf([S('m01')], { untrusted: [1] }), { ...NE1, rep_collided: 'grpa' }, { ...NE1, rep_collided: [1] }, { ...NE1, rep_collided: [''] },
  ];
  for (const o of bad) {
    const e = await errOf(gate(E, o));
    assert.ok(e && e.code === '22023' && /invalid_input/.test(e.message), `${JSON.stringify(o).slice(0, 120)}: ${e?.code} ${e?.message}`);
  }
  const e2 = await errOf(E.one(`select ops.parent_raw_gate(2, $1::jsonb) as r`, [JSON.stringify(NE1)]));
  assert.match(e2?.message ?? '', /invalid_input: 会社 2/);
  // 同じコードが 2 行 (取得の重なり) = 比べられない (どちらの行かを決めない)
  const r = await gate(E, obsOf([...NE_ROWS, S('m01', 'grpa', 'grpA')], { untrusted: ['m08'] }));
  assert.deepEqual(r.items.find((x) => x.code === 'm01'), { code: 'm01', class: 'parent_incomparable', reason: 'ne_untrusted', ne_parent: 'grpA', cdb_parent: 'grpA', reg_state: 'available', item_state: null });
});

await ta('[P3] 構造の数え (全部の商品・登録の状態によらない): 2 段 = 親を持ち、親も親を持つ・自分も子を持つ / 循環 = 辿ると戻る', async () => {
  // 2 段: grpD (親 grpE・子 m13)・m13 (親 grpD に親)・m15 (親 grpA・子 m16)・m16 (親 m15 に親)・L1 L2 (循環の 2 つも親が親を持つ = 2 段にも数える) / 循環: L1・L2・m14
  assert.deepEqual((await E.one('select ops.parent_structure_counts(1) as r')).r, { two_level: 7, loop: 3 });
});

let RUN1;
await ta('[P4] 照合 ② の記録: 形・今日の取得だけ・未来の時刻・封が要る・同じ回は 1 回・DB が数える・表は関数でだけ書く・追記だけ', async () => {
  RUN1 = runId();
  const rec = (run, fetch, mat, ev, obs) => E.one('select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb) as r', [run, JSON.stringify(fetch), mat, ev, JSON.stringify(obs)]);
  const yesterday = new Date(Date.now() - 36 * 3600000).toISOString();
  for (const [args, re] of [
    [['bad run!', FETCH(), 'mat_1', hex('d'), NE1], /invalid_input: 照合の回/],
    [[RUN1, { ...FETCH(), raw_hash: 'x' }, 'mat_1', hex('d'), NE1], /invalid_input: NE の取得/],
    [[RUN1, { ...FETCH(), products_complete_at: '2030-01-01 00:00:00' }, 'mat_1', hex('d'), NE1], /invalid_input: 取得の完了の時刻/],
    [[RUN1, FETCH(yesterday), 'mat_1', hex('d'), NE1], /stale_fetch/],
    [[RUN1, FETCH(nowRfc(3600000)), 'mat_1', hex('d'), NE1], /今より後/],
    [[RUN1, FETCH(), null, hex('d'), NE1], /材料の世代/],
    [[RUN1, FETCH(), 'mat_1', null, NE1], /封をした回だけ/],
    [[RUN1, FETCH(), 'mat_1', hex('d'), { ...NE1, format: 'v0' }], /NE の観測は/],
  ]) await assert.rejects(rec(...args), re);
  assert.equal((await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n, 0, '断った呼び出しは何も残さない');
  const f = FETCH();
  const r = (await rec(RUN1, f, 'mat_1', hex('d'), NE1)).r;
  assert.deepEqual(r.counts, (await gate(E, NE1, false)).counts, 'DB が自分で数えた数');
  assert.equal(r.owner, 'load'); assert.equal(r.gate.enforced, false); assert.equal(r.gate.open, true);
  const row = await E.one('select * from ops.master_parent_gate_results where compare_run_id = $1', [RUN1]);
  assert.deepEqual([row.ne_generation_id, row.ne_raw_hash, row.material_generation_id, row.evidence_sha256, row.owner_at_record, Number(row.counted)], [f.generation_id, hex('1'), 'mat_1', hex('d'), 'load', 20]);
  assert.match(row.obs_hash, /^[0-9a-f]{64}$/);
  await assert.rejects(rec(RUN1, FETCH(), 'mat_1', hex('d'), NE1), /run_reused/);
  // 表は関数でだけ・追記だけ
  await assert.rejects(E.q(`insert into ops.master_parent_gate_results (compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
    evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by) values ('x', 'g', $1, now(), now(), 'm', $1, $1, '{}', 0, '{}', '{}', 'load', 't')`, [hex('a')]),
    /record_parent_gate でだけ書く/);
  await assert.rejects(E.q("update ops.master_parent_gate_results set counted = 0"), /append-only/);
  await assert.rejects(E.q('delete from ops.master_parent_gate_results'), /append-only/);
});

await ta('[P5] 門: 持ち主 load の間は閉じない (ずれがあっても単品の CSV を作る・配る) / company = 数え 0 でない・記録なし・別の回・今日でない = 作らない・配らない / 作り直し・申告・照合・使わないは通す / 0 の今日の同じ回 = 開く', async () => {
  const G = await setupDb(['g01', 'g02', 'g03', 'g04', 'g05', 'g06']);
  const tg = await G.tag('grpZ');
  await G.setParent(await G.pidOf('g01'), tg);
  for (const c of ['g02', 'g03', 'g04', 'g05', 'g06']) await G.setReg(c, 'draft');
  const closedRe = /parent_gate_closed: 代表 \(親\) の数えの門が閉じている/;
  const DRIFT = obsOf([S('g01'), S('g02'), S('g03'), S('g04'), S('g05'), S('g06')]);   // NE は g01 に親なし = 違う 1
  // ① load: 記録が無くても・ずれがあっても閉じない (今の単品の登録を止めない)
  const st0 = (await G.one('select ops.parent_gate_state() as s')).s;
  assert.deepEqual([st0.enforced, st0.open], [false, true]); assert.match(st0.problems[0], /no_parent_gate_result/);
  const a = await G.mkItem('g02'); await G.step(a, 'issued');
  const runL = runId();
  await G.q('select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb)', [runL, JSON.stringify(FETCH()), 'mat_1', hex('d'), JSON.stringify(DRIFT)]);
  const b = await G.mkItem('g03'); await G.step(b, 'issued');
  // ② company にする (本番 = widen)。記録はずれ 1・新商品の許可の回と違う = 閉じる
  await seedActiveEpoch(G.db, COMPANY);
  const st1 = (await G.one('select ops.parent_gate_state() as s')).s;
  assert.deepEqual([st1.enforced, st1.open], [true, false]);
  assert.ok(st1.problems.some((p) => /parent_gate_other_run/.test(p)) && st1.problems.some((p) => /parent_raw: 代表のずれが 0 でない \(parent_mismatch 1/.test(p)), JSON.stringify(st1.problems));
  assert.match((await errOf(G.mkItem('g04')))?.message ?? '', closedRe, '作る (built の品目) は閉じる');
  const c = await G.mkItem('g05').catch((e) => e);
  assert.match(c.message, closedRe);
  // 配る (built → issued) も閉じる: load の間に作った built を置く (持ち主を一時 load に戻して作る = 本番の「配る前に門が閉じた」の形)
  await seedActiveEpoch(G.db, BASE);
  const d = await G.mkItem('g05');
  await seedActiveEpoch(G.db, COMPANY);
  assert.match((await errOf(G.step(d, 'issued')))?.message ?? '', closedRe, '配る (built → issued) は閉じる');
  // 通すもの: 配ったファイルの申告・照合 (partial / verified)・取り込めなかった (failed)・使わない (superseded)
  await G.step(a, 'import_declared'); await G.step(a, 'partial'); await G.step(a, 'failed');
  await G.step(d, 'superseded');
  await G.step(b, 'import_declared'); await G.step(b, 'verified');
  // 作り直し = 前の品目 (使わないを除く) が failed の商品だけのファイル = 作る・配る (門が閉じていても)
  const redo = await G.mkItem('g02');
  await G.step(redo, 'issued');
  // superseded を挟んでも作り直し (前の品目の最後の「使わないでない」= failed)
  await G.step(redo, 'failed');
  const redo2 = await G.mkItem('g02'); await G.step(redo2, 'superseded');
  const redo3 = await G.mkItem('g02'); await G.step(redo3, 'issued');
  // 前の品目が verified = 作り直しでない (一般) = 閉じる (使わないにした品目の後でも)
  assert.match((await errOf(G.mkItem('g03')))?.message ?? '', closedRe);
  // ③ 開く: 今日の同じ回 (新商品の許可の回) の記録で数えが 0
  const runO = runId();
  await G.q('select ops.close_new_entry_for_compare($1)', [runO]);
  await G.q('select ops.record_new_entry_gate($1, $2, $2, $3::jsonb)', [runO, nowRfc(), JSON.stringify(ZERO_GATE)]);
  assert.match((await G.one('select ops.parent_gate_state() as s')).s.problems.join(' '), /parent_gate_other_run/, '新しい許可の回の数えがまだ = 閉じたまま');
  const z = await seedParentGate(G.db, { runId: runO });   // NE = Company DB (数え 0)
  assert.deepEqual(Object.values(z.counts), [0, 0, 0, 0, 0, 0]);
  assert.deepEqual([z.gate.enforced, z.gate.open, z.gate.problems], [true, true, []]);
  const e = await G.mkItem('g06'); await G.step(e, 'issued');
  // ④ 別の回の新商品のゲートの結果が後から入った (今朝の照合で代表を数えられなかった) = 閉じる
  const runX = runId();
  await G.q('select ops.close_new_entry_for_compare($1)', [runX]);
  await G.q('select ops.record_new_entry_gate($1, $2, $2, $3::jsonb)', [runX, nowRfc(), JSON.stringify(ZERO_GATE)]);
  assert.match((await errOf(G.mkItem('g04')))?.message ?? '', /parent_gate_other_run/);
  // ⑤ 今日でない記録 (同じ回でも) = 閉じる (試験だけ: 印を立てて昨日の行を直接置く)
  await G.tx(() => G.q(`insert into ops.master_parent_gate_results (compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
      evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by, created_at)
    select $1 || '_old', ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id, evidence_sha256, obs_hash, counts, counted, excluded, samples,
      owner_at_record, recorded_by, now() - interval '2 days' from ops.master_parent_gate_results where compare_run_id = $2`, [runX, runO]), ["select set_config('ops.parent_gate_protocol', '1', true)"]);
  const st5 = (await G.one('select ops.parent_gate_state() as s')).s;
  assert.ok(st5.problems.some((p) => /parent_gate_not_today/.test(p)), JSON.stringify(st5.problems));
  // 試みを壊していない: 前の品目の状態
  assert.deepEqual((await G.q("select k.code, i.state from ops.ne_reg_export_items i join core.skus k on k.sku_id = i.sku_id order by i.item_id")).map((x) => `${x.code}:${x.state}`),
    ['g02:failed', 'g03:verified', 'g05:superseded', 'g02:failed', 'g02:superseded', 'g02:issued', 'g06:issued']);
  await G.pg.close();
});

await ta('[P6] 判断の画面: 代表 (親) の候補は「差を残す」で承認できない (前からの候補 = 選べる解決に accept_difference が入ったままでも DB が断る)・直すは承認できる・ほかの列の「差を残す」は今までどおり', async () => {
  const cand = async (fp, col) => E.q(`insert into ops.master_decision_candidates (fingerprint, subject_key, code_norm, col, child, cls, reason_kind, semantic, print, resolutions, proposal,
      first_seen_run, first_seen_at, last_seen_run, last_seen_at, seen_count)
    values ($1, $2, 'm02', $3, null, 'held_by_load', 'held_by_load', 'held_by_load@1', '{}'::jsonb, '["fix_input", "accept_difference", "fix_ne"]'::jsonb, '{"op": "decide"}'::jsonb,
      'mc_20301010T000000000Z_aaaaaa', now(), 'mc_20301010T000000000Z_aaaaaa', now(), 1)`, [fp, `${col === 'parent' ? 'parent' : 'value'}:m02`, col]);
  const approve = (fp, res) => E.q(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', $2, $3::jsonb, 'user', 't@example.com')`,
    [fp, res, res === 'fix_ne' ? JSON.stringify({ subject_key: 'parent:m02', col: 'parent', value: 'grpa' }) : null]);
  await cand(hex('7'), 'parent'); await cand(hex('8'), 'name');
  const e = await errOf(approve(hex('7'), 'accept_difference'));
  assert.equal(e?.code, '23514'); assert.match(e.message, /parent_no_accept/);
  await approve(hex('7'), 'fix_ne');
  await approve(hex('8'), 'accept_difference');
  assert.equal((await E.one("select count(*)::int as n from ops.master_decision_events where fingerprint in ($1, $2)", [hex('7'), hex('8')])).n, 2);
});

await ta('[P7] JS: 観測 (nModelOf → parentObservations)・選べる解決・意味の版・朝の要約 (先頭の ⚠️・load = 知らせだけ・company = 閉じた・記録できない)', async () => {
  const p = (code, rep, repSrc = undefined) => ({ code, name: code, supplier: '0001', handling: '取扱中', cost_src: '100', price_src: '200', tax_src: '0.1', rep, rep_src: repSrc === undefined ? JSON.stringify(rep ?? '') : repSrc });
  const ne = { products: [p('a01', 'GrpA'), p('a02', 'a02'), p('a03', ''), p('a04', null, null), p('X1', null), p('x1', null), p('s01', null), p('a05', ' grpa ')],
    sets: [{ parent: 's01', name: 's', child: 'a01', price_src: '1', qty_src: '1' }] };
  const { m, collided } = nModelOf(ne);
  const obs = parentObservations(m, { untrusted: [...collided], complete: true });
  assert.deepEqual(obs, { format: 'parent-obs-v1', complete: true, untrusted: ['x1'], rep_collided: [], rep_spellings: { state: 'unavailable', reason: 'not_read' }, rows: [
    ['a01', 'single', 'ok', 'grpa', 'GrpA'], ['a02', 'single', 'ok', null, null], ['a03', 'single', 'ok', null, null], ['a04', 'single', 'unknown', null, null],
    ['a05', 'single', 'ok', 'grpa', 'grpa'], ['s01', 'set', null, null, null], ['x1', 'single', 'ok', null, null]] });
  // 長すぎるコード・代表 = 送らない / 比べられない
  const big = parentObservations(new Map([['y'.repeat(201), { kind: 'single', cols: { parent: { raw: 'value', validity: 'ok', value: null } } }],
    ['z1', { kind: 'single', cols: { parent: { raw: 'value', validity: 'ok', value: 'w'.repeat(201) } }, repRaw: 'W' }]]));
  assert.deepEqual(big.rows, [['z1', 'single', 'unknown', null, null]]);
  // 選べる解決: 代表は差を残すを外す・ほかの列は今までどおり
  assert.deepEqual(resolutionsFor({ cls: 'held_by_load', reasonKind: 'held_by_load', col: 'parent' }), ['fix_input']);
  assert.deepEqual(resolutionsFor({ cls: 'rule', reasonKind: 'parent_manual', col: 'parent' }), ['fix_ne', 'fix_cdb']);
  assert.deepEqual(resolutionsFor({ cls: 'unexplained', reasonKind: 'none', col: 'parent' }), ['fix_ne']);
  assert.deepEqual(resolutionsFor({ cls: 'held_by_load', reasonKind: 'held_by_load', col: 'name' }), ['fix_input', 'accept_difference']);
  // 意味の版: 代表の列だけ別の印 (前の候補と別の指紋)
  const base = { norm: 'a01', kind: 'single', problem: 'parent', owner: 'load', reasonKind: 'held_by_load', reason: { reason_code: 'x' }, n_state: 'value', n: 'g', c: 'h', proposal: { op: 'decide' } };
  assert.equal(decisionPrint({ ...base, col: 'parent' }).semantic, `held_by_load@1${PARENT_SEMANTIC_SUFFIX}`);
  assert.equal(decisionPrint({ ...base, col: 'name' }).semantic, 'held_by_load@1');
  // 朝の要約
  const counts = (o = {}) => ({ parent_mismatch: 0, parent_incomparable: 0, parent_ambiguous: 0, parent_missing: 0, parent_two_level: 0, parent_loop: 0, ...o });
  const R = (pg, neExtra = {}) => ({ verdict: 'pass', load: { ingest_run_id: 'L1' }, counts: { compared: {} }, ne: { verdict: 'pass', counts: { items: 0, held: 0 }, parent_gate: pg, ...neExtra } });
  const ok0 = { state: 'ok', owner: 'load', counts: counts(), samples: {} };
  assert.match(summaryLine(R(ok0)), /^✅ マスタ照合 ①.* \/ ✅ ②: NE との差 0$/);
  const driftL = { state: 'ok', owner: 'load', counts: counts({ parent_mismatch: 2, parent_loop: 1 }), samples: { parent_mismatch: ['b1', 'b2'], parent_loop: ['c1'] } };
  assert.equal(parentTrouble({ parent_gate: driftL }), '代表 (親) が NE とずれた単品 3 件 (NE と違う 2・循環 1: b1, b2, c1) → 知らせだけ (代表の持ち主は夜間ロード = CSV は閉じない・広げる前に 0 にする)・一覧 = drift-list.mjs');
  assert.match(summaryLine(R(driftL)), /^⚠️ 代表 \(親\) が NE とずれた単品 3 件 .* \/ ✅ マスタ照合 ①/, '✅ で始まる朝は先頭に ⚠️ (isWarnSummary)');
  // load: もう ⚠️ で始まる朝は後ろに足す (重い知らせを押し下げない)
  const blocked = { ...R(driftL), ne: { verdict: 'blocked', blocked_reason: 'stale_ne', parent_gate: driftL } };
  const lb = summaryLine(blocked);
  assert.match(lb, /^⚠️ ②: 判定できない \(stale_ne\) .* \/ ⚠️ 代表 \(親\) が NE とずれた単品 3 件/);
  // company: ずれ = 新しい CSV を閉じた (一番先頭)・記録できない = 閉じたまま
  const driftC = { ...driftL, owner: 'company' };
  assert.match(summaryLine({ ...blocked, ne: { ...blocked.ne, parent_gate: driftC } }), /^⚠️ 代表 \(親\) が NE とずれた単品 3 件 .* → 新しい NE 登録の CSV \(作る・配る\) は閉じた .* \/ ⚠️ ②: 判定できない/);
  const failC = { state: 'failed', owner: 'company', error: 'writer down', counts: counts() };
  assert.match(summaryLine(R(failC)), /^⚠️ 代表 \(親\) の数えを記録できない \(書けない: writer down\) → 新しい NE 登録の CSV \(作る・配る\) は閉じた/);
  assert.equal(parentNote({ parent_gate: failC }), null);
  // load: 記録できない = ℹ️ (後ろ・CSV は閉じない)・書く接続が無い朝で数え 0 = 黙る・関数が無い (0068 の前) = 黙る
  const failL = { state: 'failed', owner: 'load', error: 'writer down', counts: counts() };
  assert.equal(parentTrouble({ parent_gate: failL }), null);
  assert.match(summaryLine(R(failL)), /^✅ マスタ照合 ①.* \/ ✅ ②: NE との差 0 \/ ℹ️ 代表 \(親\) の数えを記録できない \(書けない: writer down・持ち主は夜間ロード = CSV は閉じない\)$/);
  assert.match(summaryLine(R({ state: 'failed', owner: 'load', error: 'x', counts: counts({ parent_missing: 1 }), samples: {} })), /^⚠️ 代表 \(親\) が NE とずれた単品 1 件 \(社内に親が無い 1\)/);
  for (const pg of [{ state: 'not_configured', owner: 'load', counts: counts() }, { state: 'not_applied' }, { state: 'no_obs' }, undefined]) {
    assert.match(summaryLine(R(pg)), /^✅ マスタ照合 ①.* \/ ✅ ②: NE との差 0$/, JSON.stringify(pg));
  }
  assert.match(summaryLine(R({ state: 'not_configured', owner: 'company', counts: counts() })), /^⚠️ 代表 \(親\) の数えを記録できない \(書く接続が無い/);
  // 門の状態だけが company を知っている (読み直しの答え)
  assert.match(summaryLine(R({ state: 'failed', owner: undefined, gate: { enforced: true }, error: 'e', counts: null })), /^⚠️ 代表 \(親\) の数えを記録できない \(書けない: e\) → 新しい NE 登録の CSV/);
  // 新商品の入口を閉じられない朝はそれが一番先頭 (今までどおり)
  assert.match(summaryLine(R(driftC, { gate_close: { state: 'failed', error: 'x' } })), /^⚠️ 新商品の入口を閉じられない .* \/ ⚠️ 代表 \(親\) が NE とずれた/);
});

await ta('[P8] 照合の実行口: 封 (結果の JSON の sha256) の後に DB に数えさせて記録する・recordParentGate の状態 (ok / not_applied / not_configured / failed / no_fetch)', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'vg6-pgate-'));
  try {
    const obs = NE1;
    const parentObs = { obs, fetch: FETCH(), material_generation_id: 'mat_run' };
    const neStub = () => ({ result: { format: 'mc-ne-v1', verdict: 'pass', counts: { items: 0, held: 0 } }, pendingEntries: null, decisionsDone: [], baselineWrites: [], neCodes: null, regObs: null, parentObs });
    const writes = [];
    const r = await runCompare({ db: E.db, writerDb: E.db, dataDir: tmp, asOf: jstDateStr(new Date()), compare: async () => ({ verdict: 'pass', counts: { compared: {} }, load: { ingest_run_id: 'L1' } }),
      neCompare: neStub, oldCompare: null, write: (d, n, x) => { writes.push(x); return true; } });
    const pgr = r.result.ne.parent_gate;
    assert.equal(pgr.state, 'ok', JSON.stringify(pgr));
    const row = await E.one('select * from ops.master_parent_gate_results where compare_run_id = $1', [r.result.compare_run_id]);
    assert.equal(row.evidence_sha256, r.evidence.sha256, '結果の JSON の sha256 = 封');
    assert.equal(row.material_generation_id, 'mat_run');
    assert.deepEqual(pgr.counts, (await gate(E, obs, false)).counts);
    assert.match(r.line, /^⚠️ 代表 \(親\) が NE とずれた単品 18 件 .*知らせだけ/);
    // blocked の回は記録しない
    const r2 = await runCompare({ db: E.db, writerDb: E.db, dataDir: tmp, asOf: jstDateStr(new Date()), compare: async () => ({ verdict: 'pass', counts: { compared: {} }, load: { ingest_run_id: 'L1' } }),
      neCompare: () => ({ ...neStub(), result: { format: 'mc-ne-v1', verdict: 'blocked', blocked_reason: 'stale_ne' } }), oldCompare: null, write: () => true });
    assert.equal(r2.result.ne.parent_gate, undefined);
    // recordParentGate の状態
    const fake = (ok) => ({ query: async (sql) => (/to_regprocedure/.test(sql) ? { rows: [{ ok }] } : (() => { throw new Error('呼ばない'); })()) });
    assert.deepEqual(await recordParentGate(async () => fake(false), { compareRunId: runId(), parentObs, evidenceSha256: hex('e') }), { state: 'not_applied' });
    assert.deepEqual(await recordParentGate(null, { compareRunId: runId(), parentObs, evidenceSha256: hex('e'), readDb: fake(false) }), { state: 'not_applied' });
    assert.deepEqual(await recordParentGate(null, { compareRunId: runId(), parentObs, evidenceSha256: hex('e') }), { state: 'not_configured' });
    const nc = await recordParentGate(null, { compareRunId: runId(), parentObs, evidenceSha256: hex('e'), readDb: E.db });
    assert.deepEqual([nc.state, nc.owner, nc.counts], ['not_configured', 'load', pgr.counts], '書く接続が無い朝 = 読むだけで数える (記録はしない)');
    const fl = await recordParentGate(async () => E.db, { compareRunId: runId(), parentObs: { ...parentObs, fetch: FETCH(nowRfc(3600000)) }, evidenceSha256: hex('e'), readDb: E.db });
    assert.deepEqual([fl.state, fl.stage, /今より後/.test(fl.error), fl.counts], ['failed', 'record', true, pgr.counts]);
    const dn = await recordParentGate(async () => { throw new Error('ECONNREFUSED'); }, { compareRunId: runId(), parentObs, evidenceSha256: hex('e') });
    assert.deepEqual([dn.state, dn.stage], ['failed', 'presence']);
    assert.equal((await recordParentGate(async () => E.db, { compareRunId: runId(), parentObs: { ...parentObs, material_generation_id: null }, evidenceSha256: hex('e') })).state, 'no_fetch');
    assert.deepEqual(await recordParentGate(async () => E.db, { compareRunId: runId(), parentObs: null }), { state: 'no_obs' });
    // 読むだけの数え (watcher の取引) は何も書かない
    const n0 = (await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n;
    const rc = await readParentCounts(E.db, obs, { detail: true });
    assert.equal(rc.items.length, 18); assert.equal((await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n, n0);
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
});

await ta('[P9] drift-list (読むだけ): 照合と同じ読み方の NE で 6 つの数えと一覧・引数・数えられないときは止まる', async () => {
  const integ = { ne_api_products_integrity: JSON.stringify({ dup_codes: ['m03'], dropped_no_code: 0 }),
    ne_api_setproducts_integrity: JSON.stringify({ parent_conflicts: [], pair_dups: [], dropped_missing_key: 0, dropped_missing_parent: 0, missing_child_parents: [] }) };
  const prod = (code, rep) => ({ code, name: code, supplier: '0001', handling: '取扱中', cost_src: '100', price_src: '200', tax_src: '0.1', rep, rep_src: JSON.stringify(rep ?? '') });
  const ne = { hasSrc: true, meta: { ne_api_products_complete_at: '2030-01-01 00:00:00', ne_api_setproducts_complete_at: '2030-01-01 00:00:00', ...integ },
    products: [prod('m01', 'grpA'), prod('m02', 'grpA'), prod('m03', null), prod('d10', 'grpB')], sets: [], spellings: { ok: true, rows: [] } };
  const n0 = (await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n;
  const r = await driftList({ ne, db: E.db });
  const cls = Object.fromEntries(r.items.map((x) => [x.code, `${x.class}${x.reason ? `:${x.reason}` : ''}`]));
  assert.equal(cls.m01, undefined, 'm01 は一致');
  assert.equal(cls.d10, undefined, 'd10 は NE grpB = 社内 grpB = 一致');
  assert.equal(cls.m02, undefined, 'm02 は NE grpA = 社内 grpA = 一致 (この取得では)');
  assert.equal(cls.m03, 'parent_incomparable:ne_untrusted', '取込の整合で保持した商品 = 比べられない');
  assert.equal(cls.m05, 'parent_incomparable:not_in_ne');
  assert.equal(r.counted, 20); assert.equal((await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n, n0, '何も書かない');
  const lines = formatDriftList(r, { cls: 'parent_incomparable', limit: 2 });
  assert.match(lines[0], /^代表 \(親\) のずれ \d+ 件 \(数えた単品 20・NE の取得 2030-01-01 00:00:00 UTC\)$/);
  assert.match(lines[3], /門: 持ち主 load \(知らせだけ\)/);
  assert.match(lines[4], /^ {2}parent_incomparable +\S+ +NE の代表/);
  assert.match(lines.at(-1), /… ほか \d+ 件/);
  assert.deepEqual(parseArgs(['--class', 'parent_loop', '--limit', '5', '--json', '--data-dir', 'D']), { dataDir: 'D', json: true, cls: 'parent_loop', limit: 5 });
  assert.throws(() => parseArgs(['--class', 'x']), /--class/); assert.throws(() => parseArgs(['--limit', '0']), /--limit/); assert.throws(() => parseArgs(['--x']), /知らない引数/);
  await assert.rejects(driftList({ ne: { error: 'no_warehouse_db' }, db: E.db }), /NE の取得を読めない/);
  await assert.rejects(driftList({ ne: { ...ne, meta: { ...ne.meta, ne_api_products_complete_at: null } }, db: E.db }), /完了の印が無い/);
  await assert.rejects(driftList({ ne: { ...ne, meta: { ...ne.meta, ne_api_products_integrity: 'x' } }, db: E.db }), /取込の整合/);
  await assert.rejects(driftList({ ne: { ...ne, hasSrc: false }, db: E.db }), /古い/);
  await assert.rejects(driftList({ ne, db: { query: async () => ({ rows: [{ ok: false }] }) } }), /0068 の前/);
});

await ta('[P10] 関数の作り直し: 0059 の版との差は決めた所だけ (ops._widen_judge = products.parent の判定 9 を足しただけ・ops.master_widen_allowed_keys = 1 つ足しただけ)・後の migration が作り直していない', async () => {
  const DIR = path.join(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '..', 'db', 'company', 'migrations');
  const files = fs.readdirSync(DIR).filter((x) => /^\d{4}_.*\.sql$/.test(x)).sort();
  const F = files.find((x) => /^\d{4}_parent_raw_gate\.sql$/.test(x));
  assert.ok(F, '0068 の file');
  const defIn = (file, name) => {
    const t = fs.readFileSync(path.join(DIR, file), 'utf8').replace(/\r\n/g, '\n');
    const re = new RegExp(`\\ncreate (or replace )?function ${name.replace('.', '\\.')}\\(`, 'g');
    let m; let last = null; while ((m = re.exec(t))) last = m.index + 1;
    if (last == null) return null;
    const endBody = t.indexOf('\n$$;\n', last), endPl = t.indexOf('\nend $$;\n', last);
    const end = endPl >= 0 && (endBody < 0 || endPl < endBody) ? endPl + 9 : endBody + 5;
    return t.slice(last, end).replace(/^create (or replace )?function /, 'create function ');
  };
  const diff = (a, b) => {
    const A = a.split('\n'), B = b.split('\n'), n = A.length, mm = B.length;
    const L = Array.from({ length: n + 1 }, () => new Int32Array(mm + 1));
    for (let i = n - 1; i >= 0; i--) for (let j = mm - 1; j >= 0; j--) L[i][j] = A[i] === B[j] ? L[i + 1][j + 1] + 1 : Math.max(L[i + 1][j], L[i][j + 1]);
    const removed = [], added = []; let i = 0, j = 0;
    while (i < n && j < mm) { if (A[i] === B[j]) { i++; j++; } else if (L[i + 1][j] >= L[i][j + 1]) removed.push(A[i++]); else added.push(B[j++]); }
    while (i < n) removed.push(A[i++]); while (j < mm) added.push(B[j++]);
    return { removed: removed.map((x) => x.trim()), added: added.map((x) => x.trim()) };
  };
  for (const name of ['ops._widen_judge', 'ops.master_widen_allowed_keys']) {
    const prev = files.filter((x) => x < F && defIn(x, name)).pop();
    assert.equal(prev, '0059_master_widen_amazon.sql', `${name} の元にした版 (0068 より前の最後の定義) が変わった = 0068 の関数を、新しい版から写し直す`);
    const d = diff(defIn(prev, name), defIn(F, name));
    if (name === 'ops.master_widen_allowed_keys') {
      assert.deepEqual(d, { removed: ["select array['listing_components.amazon', 'skus.sku_kind']::text[]"], added: ["select array['listing_components.amazon', 'products.parent', 'skus.sku_kind']::text[]"] });
    } else {
      assert.deepEqual(d.removed, [], `${name}: 0059 から消した行は無い`);
      // 足した行 = 変数 5 つ・v_parent_added の代入・判定 9 の塊 (if v_parent_added then … end if;) だけ
      const body = defIn(F, name);
      const s9 = body.indexOf('  -- 9. 🆕 0068');
      const e9 = body.indexOf('\n  return jsonb_build_object(', s9);
      assert.ok(s9 > 0 && e9 > s9);
      const block = body.slice(s9, e9).split('\n').map((x) => x.trim()).filter((x) => x !== '');
      const extra = d.added.filter((x) => x !== '');
      const rest = [...extra]; for (const l of block) { const k = rest.indexOf(l); if (k >= 0) rest.splice(k, 1); }
      assert.deepEqual(rest, ['v_parent_added boolean;   -- 🆕 0068: products.parent を足す試み = 照合 ② の代表の生の数え 0・構造の数え 0 を見る',
        'pgr        ops.master_parent_gate_results;', 'v_struct   jsonb;', 'v_mgen     text;', 'v_bad      text;', "v_parent_added := 'products.parent' = any(a.added_keys);"], `${name}: 決めた所以外の差`);
    }
    for (const later of files.filter((x) => x > F)) assert.equal(defIn(later, name), null, `${later} も ${name} を作り直している = この試験の元を見直す`);
  }
});

await ta('[P11] (#1676 Codex R1 High) 本番の保存の形 = 代表は小文字 (代表商品コード)・元の書き方は 代表商品コード_src (JSON): GRPA と grpA は書き方の衝突 = parent_ambiguous / NE のコードの元の書き方 (raw_ne_code_spellings の代表の名前空間) の衝突も同じ', async () => {
  const integ = { ne_api_products_integrity: JSON.stringify({ dup_codes: [], dropped_no_code: 0 }),
    ne_api_setproducts_integrity: JSON.stringify({ parent_conflicts: [], pair_dups: [], dropped_missing_key: 0, dropped_missing_parent: 0, missing_child_parents: [] }) };
  // 本番の ne-api.js の保存 = 代表商品コード は toLowerCase・代表商品コード_src = JSON.stringify(元の値)
  const stored = (code, original) => ({ code, name: code, supplier: '0001', handling: '取扱中', cost_src: '100', price_src: '200', tax_src: '0.1',
    rep: original == null ? '' : String(original).toLowerCase(), rep_src: JSON.stringify(original ?? '') });
  const neOf = (products, spellings) => ({ hasSrc: true, meta: { ne_api_products_complete_at: '2030-01-01 00:00:00', ne_api_setproducts_complete_at: '2030-01-01 00:00:00', ...integ },
    products, sets: [], spellings: spellings || { ok: true, rows: [] } });
  // m01・m02 は社内の親 grpA (一致の形)。NE の代表の元の書き方が grpA と GRPA = 衝突 = 曖昧 (どちらの名札か決められない)
  const r = await driftList({ ne: neOf([stored('m01', 'grpA'), stored('m02', 'GRPA')]), db: E.db });
  const cls = Object.fromEntries(r.items.map((x) => [x.code, `${x.class}:${x.reason}`]));
  assert.equal(cls.m01, 'parent_ambiguous:ne_rep_spellings'); assert.equal(cls.m02, 'parent_ambiguous:ne_rep_spellings');
  assert.deepEqual(r.items.filter((x) => ['m01', 'm02'].includes(x.code)).map((x) => x.ne_parent).sort(), ['GRPA', 'grpA'], '一覧の NE の代表 = 元の書き方');
  // 同じ書き方 = 衝突しない (今までどおり一致)
  const r2 = await driftList({ ne: neOf([stored('m01', 'grpA'), stored('m02', 'grpA')]), db: E.db });
  assert.equal(r2.items.find((x) => ['m01', 'm02'].includes(x.code)), undefined);
  // 照合が読む NE の元の書き方 (代表の名前空間) に 2 つ以上の書き方 (商品コードが空で落とした行の代表など) = 衝突 = 曖昧
  const sp = { ok: true, rows: [{ kind: 'rep', code_norm: 'grpa', spellings: JSON.stringify(['GRPA', 'grpA']) }, { kind: 'single', code_norm: 'm01', spellings: JSON.stringify(['m01']) }] };
  const r3 = await driftList({ ne: neOf([stored('m01', 'grpA'), stored('m02', 'grpA')], sp), db: E.db });
  assert.equal(Object.fromEntries(r3.items.map((x) => [x.code, x.reason])).m01, 'ne_rep_spellings');
  // 元の値 (_src) が読めない古い行は小文字の値を使う (今までどおり)
  const { m } = nModelOf({ products: [{ ...stored('z9', 'Q1'), rep_src: 'x{' }], sets: [] });
  assert.equal(m.get('z9').repRaw, 'q1');
});

await ta('[P12] (#1676 Codex R1 Medium) 持ち主が company で記録も重い数えの読み直しも落ちた朝 = 門の状態だけは別に読む → ⚠️ 閉じた / 門の状態も読めない = 持ち主が分からない = company と同じ ⚠️ (「持ち主は夜間ロード」と言わない)', async () => {
  const parentObs = { obs: NE1, fetch: FETCH(), material_generation_id: 'mat_x' };
  const writer = async () => ({ query: async (sql) => { if (/to_regprocedure/.test(sql)) return { rows: [{ ok: true }] }; throw new Error('canceling statement due to statement timeout'); } });
  const readDb = (stateOk) => ({ query: async (sql) => {
    if (/^(begin|rollback)/.test(sql)) return { rows: [] };
    if (/parent_raw_gate/.test(sql)) throw new Error('canceling statement due to statement timeout');
    if (/parent_gate_state/.test(sql)) { if (!stateOk) throw new Error('connection lost'); return { rows: [{ g: { enforced: true, open: false, problems: ['x'] } }] }; }
    return { rows: [{ ok: true }] };
  } });
  const a = await recordParentGate(writer, { compareRunId: runId(), parentObs, evidenceSha256: hex('e'), readDb: readDb(true) });
  assert.deepEqual([a.state, a.owner, a.gate?.enforced], ['failed', 'company', true]);
  assert.match(a.read_error, /statement timeout/);
  assert.match(parentTrouble({ parent_gate: a }), /代表 \(親\) の数えを記録できない .*→ 新しい NE 登録の CSV \(作る・配る\) は閉じた/);
  assert.equal(parentNote({ parent_gate: a }), null);
  const b = await recordParentGate(writer, { compareRunId: runId(), parentObs, evidenceSha256: hex('e'), readDb: readDb(false) });
  assert.deepEqual([b.state, b.owner, b.gate], ['failed', undefined, undefined]);
  assert.match(parentTrouble({ parent_gate: b }), /代表 \(親\) の数えを記録できない .*持ち主が分からない.*→ 新しい NE 登録の CSV \(作る・配る\) は閉じた/);
  assert.equal(parentNote({ parent_gate: b }), null, '持ち主が分からない朝に「持ち主は夜間ロード」と言わない');
  // 書く接続が無く読み直しもできない朝も同じ (持ち主が分からない = ⚠️)
  assert.match(parentTrouble({ parent_gate: { state: 'not_configured' } }), /持ち主が分からない/);
});

await ta('[P13] (#1676 Codex R2 High) 書き方の台帳 (raw_ne_code_spellings) を読めない回 = 代表の数えを記録しない (SQL も断る)・drift-list も止まる / 商品コードが空で落ちた行にだけ別の書き方がある形: 台帳を読めた回 = 曖昧・読めない回 = 0 件の記録を作らない = 門は閉じたまま / 照合そのものは止めない (load = ℹ️・company = ⚠️)', async () => {
  const integ = { ne_api_products_integrity: JSON.stringify({ dup_codes: [], dropped_no_code: 1 }),
    ne_api_setproducts_integrity: JSON.stringify({ parent_conflicts: [], pair_dups: [], dropped_missing_key: 0, dropped_missing_parent: 0, missing_child_parents: [] }) };
  const stored = (code, original) => ({ code, name: code, supplier: '0001', handling: '取扱中', cost_src: '100', price_src: '200', tax_src: '0.1',
    rep: String(original).toLowerCase(), rep_src: JSON.stringify(original) });
  // 保存した行は m01 → grpA と m02 → grpA だけ。商品コードが空で落ちた行の代表 GRPA は書き方の台帳 (代表の名前空間) にだけある
  const neOf = (spellings) => ({ hasSrc: true, meta: { ne_api_products_complete_at: '2030-01-01 00:00:00', ne_api_setproducts_complete_at: '2030-01-01 00:00:00', ...integ },
    products: [stored('m01', 'grpA'), stored('m02', 'grpA')], sets: [], spellings });
  const collected = { ok: true, rows: [{ kind: 'rep', code_norm: 'grpa', spellings: JSON.stringify(['GRPA', 'grpA']) }] };
  const ok = await driftList({ ne: neOf(collected), db: E.db });
  assert.equal(Object.fromEntries(ok.items.map((x) => [x.code, x.reason])).m01, 'ne_rep_spellings', '台帳を読めた回 = 曖昧');
  for (const sp of [{ ok: false, reason: 'not_collected' }, { ok: false, reason: 'rows_mismatch' }, { ok: false, reason: 'unknown_version' }, undefined]) {
    await assert.rejects(driftList({ ne: neOf(sp), db: E.db }), /NE のコードの元の書き方 \(代表\) を読めない/, JSON.stringify(sp));
  }
  // 照合 ② の観測 (compare-ne と同じ部品): 台帳を読めない = 信頼の状態 unavailable + 理由
  const { m } = nModelOf(neOf(undefined));
  const bad = parentObservations(m, { repSpellings: repSpellingsOf({ ok: false, reason: 'not_collected' }) });
  assert.deepEqual([bad.rep_spellings, bad.rep_collided], [{ state: 'unavailable', reason: 'not_collected' }, []]);
  const good = parentObservations(m, { repSpellings: repSpellingsOf({ ok: true, entries: [{ kind: 'rep', state: 'collided', code_norm: 'grpa' }, { kind: 'product', state: 'collided', code_norm: 'm09' }] }) });
  assert.deepEqual([good.rep_spellings, good.rep_collided], [{ state: 'ok' }, ['grpa']]);
  // SQL: 生の数えは台帳の状態を持つ観測だけ受ける・記録は「読めた」回だけ (読めない回は 0 件の記録を作らない)
  await assert.rejects(gate(E, { ...NE1, rep_spellings: undefined }), /invalid_input: rep_spellings/);
  await assert.rejects(gate(E, { ...NE1, rep_spellings: { state: 'maybe' } }), /invalid_input: rep_spellings/);
  const n0 = (await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n;
  await assert.rejects(E.q('select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb)', [runId(), JSON.stringify(FETCH()), 'mat_x', hex('d'), JSON.stringify(bad)]),
    /spellings_unavailable: NE のコードの元の書き方 \(代表\) を読めない回 \(not_collected\)/);
  // 照合の部品 (recordParentGate) は記録を呼ばない = 状態 no_spellings・読むだけの数え (持ち主) は出す
  const r = await recordParentGate(async () => E.db, { compareRunId: runId(), parentObs: { obs: bad, fetch: FETCH(), material_generation_id: 'mat_x' }, evidenceSha256: hex('e'), readDb: E.db });
  assert.deepEqual([r.state, r.reason, r.owner], ['no_spellings', 'not_collected', 'load']);
  assert.equal((await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n, n0, '記録は作らない');
  // 朝の要約: load = 照合は止めない・ℹ️ (CSV は閉じない) / company = ⚠️ 閉じたまま
  assert.match(parentNote({ parent_gate: r }), /^ℹ️ 代表 \(親\) の数えを記録できない \(NE のコードの元の書き方 \(代表\) を読めない: not_collected・持ち主は夜間ロード = CSV は閉じない\)$/);
  assert.match(parentTrouble({ parent_gate: { ...r, owner: 'company' } }), /代表 \(親\) の数えを記録できない \(NE のコードの元の書き方 \(代表\) を読めない: not_collected\).* → 新しい NE 登録の CSV \(作る・配る\) は閉じた/);
  // 照合の実行口: 台帳を読めない回も照合は最後まで走る (証跡・全件 JSON・要約)・記録は作らない
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'vg6-pgate-sp-'));
  try {
    const neStub = () => ({ result: { format: 'mc-ne-v1', verdict: 'pass', counts: { items: 0, held: 0 } }, pendingEntries: null, decisionsDone: [], baselineWrites: [], neCodes: null, regObs: null,
      parentObs: { obs: bad, fetch: FETCH(), material_generation_id: 'mat_x' } });
    const x = await runCompare({ db: E.db, writerDb: E.db, dataDir: tmp, asOf: jstDateStr(new Date()), compare: async () => ({ verdict: 'pass', counts: { compared: {} }, load: { ingest_run_id: 'L1' } }),
      neCompare: neStub, oldCompare: null, write: () => true });
    assert.equal(x.evidence.state, 'complete');
    assert.equal(x.result.ne.parent_gate.state, 'no_spellings');
    assert.match(x.line, /^⚠️ 代表 \(親\) が NE とずれた単品 .* \/ ✅ マスタ照合 ①.* \/ ℹ️ 代表 \(親\) の数えを記録できない \(NE のコードの元の書き方 \(代表\) を読めない: not_collected/);
    assert.equal((await E.one('select count(*)::int as n from ops.master_parent_gate_results')).n, n0);
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
  // company の DB: 台帳を読めない朝 = 一番新しい記録が作られない = 今の許可の回と違う = 新しい CSV は閉じたまま
  const G = await setupDb(['k01', 'k02']);
  await G.setReg('k02', 'draft');
  await seedActiveEpoch(G.db, COMPANY);
  const run = runId();
  await G.q('select ops.close_new_entry_for_compare($1)', [run]);
  await G.q('select ops.record_new_entry_gate($1, $2, $2, $3::jsonb)', [run, nowRfc(), JSON.stringify(ZERO_GATE)]);
  const gr = await recordParentGate(async () => G.db, { compareRunId: run, parentObs: { obs: { ...bad, rows: [S('k01'), S('k02')] }, fetch: FETCH(), material_generation_id: 'mat_x' }, evidenceSha256: hex('e'), readDb: G.db });
  assert.deepEqual([gr.state, gr.owner, gr.gate.open], ['no_spellings', 'company', false]);
  assert.match((await errOf(G.mkItem('k02')))?.message ?? '', /parent_gate_closed: .*no_parent_gate_result/);
  await G.pg.close();
});

await E.pg.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
