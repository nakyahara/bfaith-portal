/**
 * test-master-edit.mjs — マスタの入力 (apps/master-edit・lib/master-write.mjs・lib/master-cutover.mjs・0050。Company DB構想 14 §6 ⑤-1 / Codex ⑤-R0・R1・PR #1563 R1)
 *
 * Company DB = PGlite (Render と同じ条件の持ち主のロール deploy で migration)。保存は画面だけのロール master_edit で流す (権限が足りているかも確かめる)。
 * 本物の router を HTTP 越しにも通す (セッションは x-test-session で模擬)
 * 固定する契約:
 *   1 切替の段階: 一方向・1 段ずつ。進めるには証拠 (drain・手の入口を止めた一覧 / 持ち主表のハッシュ) と、全部の場所 (render・minipc) の門の記録が要る
 *     (⑤-1 では誰も門の記録を書かない = 進められない)。直接は書けない・記録が残る・読めない = 閉じている
 *   2 保存を開く = 段階 new_open **かつ** 持ち主表のハッシュが段階の記録と同じ **かつ** 列が 'company' **かつ** MASTER_EDIT_OPEN。欠ければ 409 切替前・何も書かない
 *     (変わる項目が無い保存も 409 = done を残さない)。構成の依頼も持ち主 sku_components が 'company' のときだけ
 *   3 単品の保存: 変わった列だけ・商品の行もそろう・代表 (親) は manual・代表の仕入先・変更の記録の actor / source / request_id / reason
 *   4 保存した値は夜間ロードを 2 回流しても残る (持ち主 company)。持ち主を load に戻すと夜間ロードが戻す
 *   5 同じ request_id: 同じ中身 = 前の結果 / 違う中身・違う人・違う SKU = 409 / 失敗 = 同じ誤り。記録は保存と同じ取引・追記だけ
 *   6 編集の印: 読んだ行の全部と「行が無いこと」(phantom) のどれかが変わった = 409 とその間の変更・何も書かない
 *   7 入力の検証 / 新しい商品コードの形 (⑤-2 用)
 *   8 単品の税率・取扱・原価を変えると、含むセットの導く値を同じ取引で計算し直す
 *   9 原価は今日だけ (東京の日付・取引の初めに 1 回)・期間を重ねない・重なりは DB が拒む (この画面と昇格だけ)
 *  10 NE に取り込む CSV が出ている列は変えない (409)
 *  11 セットの構成 = 依頼だけ。NE の観測 (DB に残した完全な回・依頼より後) が構成品・数量・並び・行の数まで同じときだけ上げる (同じ取引で構成・導く値・依頼・食い違い)。
 *     違えば食い違い (mismatch / stale / unrequested_diff) を残す。偽の観測・完全でない回・依頼より前の観測は上げない。観測を書く関数は画面のロールでは動かない
 *  12 セットの導く値: 今の構成と依頼の構成の両方で確かめる (上書きを外すのは両方が導けるときだけ)・例外原価をやめる = 今日から合計
 *  13 上げる処理と単品の保存の順番 (どちらが先でもセットの値が新しい単品の値になる・後の保存は含むセットが増えたことに気づく)
 *  14 DB のロール: master_edit = 画面の読み書きだけ (段階を進める・観測を書く・構成を上げる・門の記録は不可) / master_ops = 段階を進める関数だけ
 *  15 画面: 一覧・単品・セット・変更の記録・つかいかた・404 の描画と画面の JS / 名簿・Origin・Content-Type / 書き込み用の接続が無い = 見るだけ /
 *     Company DB が無い・届かない = 帯と 503 / 保存を開いていない = 帯・欄と保存のボタンが閉じている / server.js は Render だけ
 * 使い方: node scripts/test-master-edit.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import vm from 'node:vm';
import express from 'express';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const R = await import('../apps/master-edit/read.mjs');
const { default: router, __setPgClientFactory, __setClock, __setOwnership, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const rejectsWith = async (p, status, reason) => {
  try { await p; } catch (e) {
    assert.ok(e instanceof W.MasterWriteError, `MasterWriteError でない: ${e && e.stack}`);
    assert.equal(e.status, status, `${e.reason}: ${e.message}`);
    if (reason) assert.equal(e.reason, reason, e.message);
    return e;
  }
  assert.fail(`${status} ${reason || ''} にならなかった`);
};
const pgCode = async (p) => { try { await p; return 'ok'; } catch (e) { return e.code || e.message; } };

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const withOwn = (over) => ({ ...MASTER_OWNERSHIP, ...over });
const LOAD_NOW = new Date('2030-01-05T03:00:00Z');   // 夜間ロードの日 (東京 2030-01-05)
const NOW = new Date('2030-01-10T03:00:00Z');        // 画面の今日 (東京 2030-01-10)
const TODAY = '2030-01-10';
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210.4 }], ['S02', { method: '宅急便', cost: 520 }]]);
/** ⑤-3 の古い入口の一覧 (manifest) の形の例。kind = manual = コードでは閉じられない入口 */
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }, { id: 'gas.logizard_sheet', kind: 'manual' }] };
const MANUAL_STOPPED = [{ id: 'gas.logizard_sheet', by: 'naka@test', at: '2030-01-09T10:00:00+09:00' }, { id: 'ne.product_screen', by: 'naka@test', at: '2030-01-09T10:00:00+09:00' }];
const DRAIN = { done: true, checked_by: 'naka@test', checked_at: '2030-01-09T10:00:00+09:00' };
const BUILDS = { render: ['r1'], minipc: ['m1'] };
const LEGACY_HASH = C.ownershipHash(MASTER_OWNERSHIP);

/** 夜間ロードの材料 (sources.mjs が作る形) */
function makePlan() {
  const sku = (code, name, kind, taxRate, salesClass, cost, extra = {}) => ({
    code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : taxRate === 0.1 ? 'STANDARD_10' : null, handling: 'active', salesClass,
    cost: cost == null ? null : { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' },
    standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...extra,
  });
  return {
    skus: [
      sku('s001', '単品 1', 'single', 0.1, 3, 100, { representativeCode: 'grp1', representativeState: 'value' }),
      sku('s002', '単品 2', 'single', 0.08, 2, 200),
      sku('s003', '単品 3', 'single', 0.1, 1, 50, { representativeCode: 'grp1', representativeState: 'value' }),
      sku('s004', '単品 4 (分類・原価なし)', 'single', 0.1, null, null),
      sku('s005', '単品 5 (税率なし)', 'single', null, 3, 80),
      sku('s006', '単品 6 (分類・原価なし)', 'single', 0.1, null, null),
      sku('set001', 'セット 1', 'set', 0.08, null, 400, { taxClass: 'MIXED' }),
      sku('set004', 'セット 4 (導けない)', 'set', 0.1, null, null),
      sku('set005', 'セット 5 (税率が決まらない)', 'set', null, null, 180),
      sku('set006', 'セット 6 (今の構成は導けない)', 'set', 0.1, null, null),
    ],
    variationGroups: [{ code: 'grp1', name: '名札', childCodes: ['s001', 's003'], status: 'active' }],
    setComponents: [
      { parentCode: 'set001', childCode: 's001', qty: 2, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' },
      { parentCode: 'set004', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set004', childCode: 's004', qty: 1, source: 'ne' },
      { parentCode: 'set005', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set005', childCode: 's005', qty: 1, source: 'ne' },
      { parentCode: 'set006', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set006', childCode: 's006', qty: 1, source: 'ne' },
    ],
    listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC', orderMethod: 'fax', leadTimeDays: 10 }, { code: '0002', name: 'ビーフリー', orderMethod: 'email', leadTimeDays: 5 }, { code: '0003', name: '止めた仕入先' }],
    supplierSkus: [{ supplierCode: '0001', skuCode: 's001', vendorCode: 'AMC-001' }],
    primarySuppliers: [{ skuCode: 's001', supplierCode: '0001' }],
    reorder: { available: true, runId: 'pml_test' },
  };
}

/** 1 つの Company DB (PGlite) を作る: 持ち主のロール deploy で migration・見張りと画面のロール・夜間ロード */
async function setupDb() {
  const pg = new PGlite();
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: 'load_setup', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  await pg.query("update core.suppliers set active = false where code = '0003'");
  return { pg, db };
}
/** ロールを切り替えて動かす (試験は 1 つの接続 = SET ROLE。終わったら持ち主のロール deploy に戻す) */
async function asRole(E, role, fn) {
  await E.pg.query(`set role ${role}`);
  try { return await fn(); } finally { await E.pg.query('set role deploy'); }
}
/** 画面だけのロールで動かす (保存は必ずこれ = 権限が足りているかも確かめる) */
const asEditor = (E, fn) => asRole(E, 'master_edit', fn);
/** ⑤-3 の門が書く記録 (ロール master_gate・DB の関数だけ) */
const gateAck = (E, host, instanceId, buildId, ownership, phaseSeen, inflight = 0) => asRole(E, 'master_gate', () => C.recordLegacyGateAck(E.db,
  { host, instanceId, buildId, manifest: MANIFEST, ownership, phaseSeen, inflightCount: inflight, oldestInflightAt: inflight ? new Date().toISOString() : null }));
/** 段階を進める (運用のロール master_ops) */
const advance = (E, to, evidence, note = null) => asRole(E, 'master_ops', () => C.advanceCutoverPhase(E.db, { to, actor: 'naka@test', evidence, note }));
/** 試験だけの切替: 門の記録を足しながら、証拠つきで new_open まで進める (本番の関数そのまま・門は弱めない)。プロセス r-a (render)・m-a (minipc) */
async function openCutover(E, ownership) {
  const h = C.ownershipHash(ownership);
  const mh = await C.manifestHashOf(E.db, MANIFEST);
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, MASTER_OWNERSHIP, 'legacy_open');
  await advance(E, 'frozen', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: MANUAL_STOPPED, drain: DRAIN });
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, ownership, 'frozen');
  await advance(E, 'company_owner', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, ownership, 'company_owner');
  await advance(E, 'new_open', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
}

const E0 = await setupDb();
const { pg, db } = E0;
const q = async (sql, params) => (await db.query(sql, params)).rows;
const load = async (ownership) => {
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: `load_${crypto.randomBytes(3).toString('hex')}`, ownership, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  return r;
};

const skuId = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id;
const cur = async (code) => W.readCurrent(db, await skuId(code), TODAY);
const tokenOf = async (code) => W.editTokenOf(await cur(code));
const lastEvent = async () => (await q('select coalesce(max(event_id), 0)::text as id from events.master_change_events'))[0].id;
const nEvents = async () => Number((await q('select count(*)::int as n from events.master_change_events'))[0].n);
const uuid = () => crypto.randomUUID();
/** その DB の今の編集の印 (無い SKU = 形だけの印) */
async function tokenIn2(E, code) {
  const id = (await E.db.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0]?.id;
  return id ? W.editTokenOf(await W.readCurrent(E.db, id, TODAY)) : 'a'.repeat(64);
}
/** 画面と同じ形で保存 (seen は今の値から)・画面だけのロールで */
async function save(code, values, { E = E0, ownership = ALL_COMPANY, open = true, requestId = uuid(), reason = 'テスト', actor = 'Naka@Test', token, eventId, shippingRates = RATES, now = NOW } = {}) {
  const seen = { token: token ?? await tokenIn2(E, code), event_id: eventId ?? await lastEvent() };
  return asEditor(E, () => W.saveSku(E.db, { actor, requestId, code, reason, seen, values }, { ownership, open, now, shippingRates }));
}
const skuRow = async (code) => (await q(`select s.name, s.tax_rate::float8 as tax_rate, s.tax_class, s.handling, s.standard_price_jpy::int as price, s.shipping_code, s.shipping_method,
    s.shipping_cost_jpy::int as ship, s.reorder_months::float8 as months, s.set_sales_class_override as override, s.handling_own,
    p.name as pname, p.sales_class, p.status, pp.display_code as parent, p.parent_set_by
  from core.skus s left join core.products p on p.product_id = s.product_id left join core.products pp on pp.product_id = p.parent_product_id where s.code = $1`, [code]))[0];
const costsOf = async (code) => (await q(`select c.cost_jpy::int as jpy, c.cost_source as src, c.cost_status as st, c.valid_from::text as f, c.valid_to::text as t
  from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = $1 order by c.valid_from, c.sku_cost_id`, [code]));
const compsOf = async (code) => (await q(`select k.code, c.qty, c.sort_order as so, c.source from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
  where c.parent_sku_id = (select sku_id from core.skus where code = $1) order by c.sort_order, k.code_norm`, [code])).map((r) => [r.code, r.qty, r.so, r.source]);
const primaryOf = async (code) => (await q(`select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id join core.skus k on k.sku_id = x.sku_id
  where k.code = $1 and x.is_primary`, [code])).map((r) => r.code);
const reqRow = async (id) => (await q('select status, result, error, sku_id::text as sku_id, operation, target_code from ops.master_edit_requests where request_id = $1', [id]))[0];
let runSeq = 0;
/** NE のセットの構成の観測を 1 回分書く (観測のロール master_observer・DB の関数)。戻り値 = set_code → observation_id */
async function observe(sets, { complete = true, at = new Date(Date.now() + 60000).toISOString() } = {}) {
  const runId = `ne_run_${++runSeq}`;
  const payload = { run_id: runId, observed_at: at, complete, sets, ...(complete ? { requested: sets.length, fetched: sets.length, raw_hash: 'c'.repeat(64), source_generation: `gen_${runSeq}` } : {}) };
  await asRole(E0, 'master_observer', () => W.recordNeSetObservations(db, payload));
  const rows = await q('select o.observation_id::text as id, k.code from ops.ne_set_observations o join core.skus k on k.sku_id = o.set_sku_id where o.run_id = $1', [runId]);
  return Object.fromEntries(rows.map((r) => [r.code, r.id]));
}
/** 観測の関数を通さずに入れる (持ち主のロールだけ・試験で「依頼から 8 日後」の観測を作るため) */
async function observeRaw(setCode, rows, at) {
  const runId = `ne_raw_${++runSeq}`;
  await pg.query(`insert into ops.ne_set_observation_runs (run_id, observed_at, complete, requested_count, fetched_count, saved_count, skipped_count, raw_hash, source_generation, content_hash)
    values ($1, $2, true, 1, 1, 1, 0, repeat('d', 64), 'raw', md5($1))`, [runId, at]);
  const resolved = [];
  for (const r of rows) resolved.push({ sku_id: Number((await q('select sku_id from core.skus where code_norm = core.norm_code($1)', [r.code]))[0].sku_id), code: r.code, qty: r.qty, sort: r.sort });
  return (await q(`insert into ops.ne_set_observations (run_id, set_sku_id, rows) select $1, sku_id, $3::jsonb from core.skus where code = $2 returning observation_id::text as id`, [runId, setCode, JSON.stringify(resolved)]))[0].id;
}
const promote = (id, opts = {}) => W.promoteComponentRequest(db, id, { ownership: ALL_COMPANY, now: NOW, ...opts });


console.log('切替の段階と門');

await ta('[1] 門の記録の関数: 時刻はサーバー・見た段階が今と違えば拒む・manifest の形・manifest はハッシュごとに 1 つ・画面のロールは書けない・追記だけ', async () => {
  const before = Date.now();
  const r = await gateAck(E0, 'render', 'r-a', 'r1', MASTER_OWNERSHIP, 'legacy_open');
  assert.equal(r.manifest_hash, await C.manifestHashOf(db, MANIFEST));
  assert.ok(Date.parse(r.acked_at) >= before - 5000, r.acked_at);
  await assert.rejects(() => gateAck(E0, 'render', 'r-a', 'r1', MASTER_OWNERSHIP, 'frozen'), /stale_phase/);
  await assert.rejects(() => asRole(E0, 'master_gate', () => C.recordLegacyGateAck(db, { host: 'render', instanceId: 'x', buildId: 'b', manifest: { entries: [] }, ownership: MASTER_OWNERSHIP, phaseSeen: 'legacy_open' })), /invalid_manifest/);
  await assert.rejects(() => asRole(E0, 'master_gate', () => C.recordLegacyGateAck(db, { host: 'render', instanceId: 'x', buildId: 'b', manifest: { entries: [{ id: 'a', kind: 'code' }, { id: 'a', kind: 'manual' }] }, ownership: MASTER_OWNERSHIP, phaseSeen: 'legacy_open' })), /重なって/);
  await assert.rejects(() => asRole(E0, 'master_gate', () => C.recordLegacyGateAck(db, { host: 'aws', instanceId: 'x', buildId: 'b', manifest: MANIFEST, ownership: MASTER_OWNERSHIP, phaseSeen: 'legacy_open' })), /check/);
  await gateAck(E0, 'minipc', 'm-a', 'm1', MASTER_OWNERSHIP, 'legacy_open');
  assert.equal(Number((await q('select count(*)::int as n from ops.master_legacy_manifests'))[0].n), 1);
  assert.equal(await pgCode(asRole(E0, 'master_edit', () => pg.query("select ops.record_legacy_gate_ack('render', 'x', 'b', '{}'::jsonb, repeat('a', 64), 'legacy_open', 0, null)"))), '42501');
  await assert.rejects(() => pg.query('delete from ops.master_legacy_gate_acks'), /append-only/);
  await assert.rejects(() => pg.query('delete from ops.master_legacy_manifests'), /append-only/);
});

await ta('[1] legacy_open → frozen の門: 証拠の形・manifest・手の入口の集合・場所ごとの新しい記録・予定の build・持ち主表・差し込み口。どれか外れたら拒む (段階はそのまま)', async () => {
  const mh = await C.manifestHashOf(db, MANIFEST);
  const ev = { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: MANUAL_STOPPED, drain: DRAIN };
  /** 一時の記録を足してから試す (取引ごと巻き戻す = 足した記録も残らない) */
  const refuse = async (evidence, re, setup = []) => {
    await pg.query('begin');
    try {
      for (const [sql, params] of setup) await pg.query(sql, params);
      await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['frozen', 'naka@test', JSON.stringify(evidence)]), re);
    } finally { await pg.query('rollback'); }
  };
  const rawAck = (host, inst, build, { manifest = mh, owner = LEGACY_HASH, phase = 'legacy_open', ago = '0 minutes', inflight = 0 } = {}) =>
    [`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, oldest_inflight_at, acked_at)
      values ($1, $2, $3, $4, $5, $6, $7, case when $7 > 0 then now() end, clock_timestamp() - $8::interval)`, [host, inst, build, manifest, owner, phase, inflight, ago]];
  await assert.rejects(() => pg.query(`select ops.set_master_cutover_phase('frozen', 'naka@test', null)`), /evidence_required/);
  await refuse({ ...ev, manifest_hash: 'a'.repeat(64) }, /manifest_hash が記録された/);
  await refuse({ ...ev, owner_hash: 'x' }, /owner_hash/);
  await refuse({ ...ev, expected_builds: { render: ['r1'] } }, /expected_builds.minipc/);
  await refuse({ ...ev, drain: { ...DRAIN, done: false } }, /drain/);
  await refuse({ ...ev, drain: { ...DRAIN, checked_at: 'きのう' } }, /drain/);
  await refuse({ ...ev, manual_entries_stopped: MANUAL_STOPPED.slice(0, 1) }, /手の入口/);                                           // 足りない
  await refuse({ ...ev, manual_entries_stopped: [...MANUAL_STOPPED, { id: 'x.extra', by: 'a', at: '2030-01-09' }] }, /手の入口/);      // 多い
  await refuse({ ...ev, manual_entries_stopped: [MANUAL_STOPPED[0], MANUAL_STOPPED[0]] }, /重なって/);
  await refuse({ ...ev, manual_entries_stopped: [{ id: 'ne.product_screen' }] }, /id・by・at/);
  // 記録: 場所が無い (minipc が古い = 15 分より前) / 予定に無い build のプロセスが動いている / manifest が違う / 持ち主表が違う / 見た段階が違う
  await pg.query("insert into ops.master_legacy_manifests (manifest_hash, entries) values (repeat('e', 64), '{\"entries\":[{\"id\":\"z\",\"kind\":\"code\"}]}')");
  await refuse(ev, /予定に無い build rX/, [rawAck('render', 'r-z', 'rX')]);
  await refuse(ev, /古い入口の一覧が違う/, [rawAck('minipc', 'm-z', 'm1', { manifest: 'e'.repeat(64) })]);
  await refuse(ev, /持ち主表のハッシュが違う/, [rawAck('minipc', 'm-z', 'm1', { owner: 'f'.repeat(64) })]);
  await refuse(ev, /見た段階が frozen/, [rawAck('minipc', 'm-z', 'm1', { phase: 'frozen' })]);
  // 差し込み口 (後の migration の「まだの項目」) を置き換えると止まる
  await refuse(ev, /prereq_failed: 構成の写しの作り直しがまだ/, [[`create or replace function ops.master_cutover_prereq_problems(p_from text, p_to text) returns text[]
    language plpgsql stable security definer set search_path = pg_catalog, ops, pg_temp as $$ begin return array['構成の写しの作り直しがまだ']; end $$`, []]]);
  assert.deepEqual((await q('select ops.master_cutover_prereq_problems($1, $2) as p', ['legacy_open', 'frozen']))[0].p, []);   // 巻き戻した = 既定のまま
  assert.equal((await C.readCutoverPhase(db)).phase, 'legacy_open');
  // 飛ばす・戻す・知らない段階・直接の UPDATE / DELETE・読めない = 閉じている
  await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['company_owner', 'naka@test', JSON.stringify(ev)]), /one_way/);
  await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['open', 'naka@test', JSON.stringify(ev)]), /知らない段階/);
  await assert.rejects(() => pg.query("update ops.master_cutover_state set phase = 'new_open'"), /set_master_cutover_phase/);
  await assert.rejects(() => pg.query('delete from ops.master_cutover_state'), /消さない/);
  const s = await C.readCutoverPhase(db);
  assert.deepEqual([s.readable, s.phase, s.owner_hash], [true, 'legacy_open', null]);
  assert.equal(C.newEntryWritable(s), false); assert.equal(C.legacyWritable(s), true);
  const broken = await C.readCutoverPhase({ query: async () => { throw new Error('connection terminated'); } });
  assert.deepEqual([broken.readable, broken.phase, C.newEntryWritable(broken), C.legacyWritable(broken)], [false, null, false, false]);
});


await ta('[2] 段階が new_open でない = 持ち主 company + MASTER_EDIT_OPEN でも 409 切替前・何も書かない。変わる項目が無い保存も 409 (done を残さない)', async () => {
  const before = await nEvents();
  const id = uuid();
  const e = await rejectsWith(save('s001', { name: '直した名前' }, { requestId: id }), 409, 'before_cutover');
  assert.equal(e.extra.phase, 'legacy_open');
  assert.match(e.message, /切替の段階が legacy_open/);
  assert.equal((await skuRow('s001')).name, '単品 1');
  assert.equal(await nEvents(), before);
  const r = await reqRow(id);
  assert.equal(r.status, 'failed'); assert.equal(r.error.reason, 'before_cutover'); assert.equal(r.target_code, 's001'); assert.ok(r.sku_id);
  const noop = uuid();
  await rejectsWith(save('s001', { name: '単品 1' }, { requestId: noop }), 409, 'before_cutover');
  assert.equal((await reqRow(noop)).status, 'failed');
  assert.equal(Number((await q("select count(*)::int as n from ops.master_edit_requests where status = 'done'"))[0].n), 0);
});

await ta('[2] 場所が足りない・古い記録だけ = 拒む → そろえば frozen。company_owner / new_open は段階に入った後の記録・書きかけ 0・持ち主表のハッシュが要る。保存は記録と同じ持ち主表だけ', async () => {
  const mh = await C.manifestHashOf(db, MANIFEST);
  const h = C.ownershipHash(ALL_COMPANY);
  const ev = { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: MANUAL_STOPPED, drain: DRAIN };
  // 場所が足りない / 古い記録だけ: 別の DB で (この DB は [1] で両方の場所の記録がある)
  const E2 = await setupDb();
  await gateAck(E2, 'render', 'r-a', 'r1', MASTER_OWNERSHIP, 'legacy_open');
  await assert.rejects(() => advance(E2, 'frozen', ev), /minipc: 15 分以内の記録が無い/);
  await E2.pg.query(`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, acked_at)
    values ('minipc', 'm-a', 'm1', $1, $2, 'legacy_open', 0, clock_timestamp() - interval '20 minutes')`, [mh, LEGACY_HASH]);
  await assert.rejects(() => advance(E2, 'frozen', ev), /minipc: 15 分以内の記録が無い/);
  await E2.pg.close();
  // この DB で進める
  const r1 = await advance(E0, 'frozen', ev, '試験');
  assert.deepEqual(r1.acks.map((a) => [a.host, a.instance_id, a.build_id]), [['minipc', 'm-a', 'm1'], ['render', 'r-a', 'r1']]);
  const ev2 = { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h };
  // frozen の前の記録 (legacy_open を見た) だけ = 拒む
  await assert.rejects(() => advance(E0, 'company_owner', ev2), /見た段階が legacy_open/);
  await gateAck(E0, 'render', 'r-a', 'r1', ALL_COMPANY, 'frozen');
  await gateAck(E0, 'minipc', 'm-a', 'm1', ALL_COMPANY, 'frozen', 2);   // minipc に書きかけが 2 件
  await assert.rejects(() => advance(E0, 'company_owner', ev2), /m-a: 書きかけが 2 件/);
  await gateAck(E0, 'minipc', 'm-a', 'm1', ALL_COMPANY, 'frozen');
  // 段階に入る前の記録 (時刻を frozen の前にした別のプロセス) = 拒む
  await pg.query('begin');
  await pg.query(`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, acked_at)
    select 'render', 'r-old', 'r1', $1, $2, 'frozen', 0, changed_at - interval '1 second' from ops.master_cutover_state`, [mh, h]);
  await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['company_owner', 'naka@test', JSON.stringify(ev2)]), /r-old: 記録が今の段階に入る前/);
  await pg.query('rollback');
  await assert.rejects(() => advance(E0, 'company_owner', { ...ev2, owner_hash: 'x' }), /owner_hash/);
  await advance(E0, 'company_owner', ev2);
  await gateAck(E0, 'render', 'r-a', 'r1', ALL_COMPANY, 'company_owner'); await gateAck(E0, 'minipc', 'm-a', 'm1', ALL_COMPANY, 'company_owner');
  await assert.rejects(() => advance(E0, 'new_open', { ...ev2, owner_hash: 'a'.repeat(64) }), /company_owner のときと違う/);
  await advance(E0, 'new_open', ev2);
  const ev3 = await q('select from_phase, to_phase, evidence, jsonb_array_length(acks) as n from ops.master_cutover_events order by event_id');
  assert.deepEqual(ev3.map((x) => [x.from_phase, x.to_phase, x.n]), [['legacy_open', 'frozen', 2], ['frozen', 'company_owner', 2], ['company_owner', 'new_open', 2]]);
  assert.deepEqual(ev3[0].evidence, ev);
  await assert.rejects(() => advance(E0, 'legacy_open', ev2), /one_way/);
  await assert.rejects(() => pg.query('delete from ops.master_cutover_events'), /append-only/);
  const s = await C.readCutoverPhase(db);
  assert.deepEqual([s.phase, s.owner_hash], ['new_open', h]);
  assert.equal(C.newEntryWritable(s, ALL_COMPANY), true);
  assert.equal(C.newEntryWritable(s, MASTER_OWNERSHIP), false);
  // 動いているコードの持ち主表が記録と違う = 保存しない (今の本番の持ち主表 = 全部 load のまま動かした)
  const e = await rejectsWith(save('s001', { name: '直した名前' }, { ownership: MASTER_OWNERSHIP }), 409, 'before_cutover');
  assert.match(e.message, /持ち主表が切替のときの記録と違う/);
  const e2 = await rejectsWith(save('s001', { name: '直した名前' }, { open: false }), 409, 'before_cutover');
  assert.match(e2.message, /保存はまだ開いていません/);
  assert.equal((await skuRow('s001')).name, '単品 1');
});


await ta('[2] 一部だけ company で切り替えた DB: 名前 (company) + 税率 (load) = 全部断る・名前だけは通る。構成の依頼も sku_components が load なら 409', async () => {
  const own = withOwn({ 'skus.name': 'company', 'products.name': 'company', 'skus.shipping': 'company' });
  const E1 = await setupDb();
  await openCutover(E1, own);
  const tok = (code) => tokenIn2(E1, code);
  const e = await rejectsWith(save('s002', { name: '名前 2 改', tax_rate: '10' }, { E: E1, ownership: own, token: await tok('s002') }), 409, 'before_cutover');
  assert.deepEqual(e.extra.fields, ['税率']);
  const r = await save('s002', { name: '名前 2 改', tax_rate: '8' }, { E: E1, ownership: own, token: await tok('s002') });
  assert.deepEqual(r.changed.map((c) => c.field), ['name']);
  const e3 = await rejectsWith(save('set001', { components: [{ code: 's001', qty: 1 }] }, { E: E1, ownership: own, token: await tok('set001') }), 409, 'before_cutover');
  assert.deepEqual([e3.extra.fields, e3.extra.load_keys], [['構成の依頼'], ['sku_components']]);
  assert.equal(Number((await E1.db.query('select count(*)::int as n from ops.sku_component_requests')).rows[0].n), 0);
  await E1.pg.close();
});

console.log('\n単品の保存');

await ta('[3] 単品: 名前・取扱・売価・税率・分類・送料・月数・代表の仕入先・代表 (親) を 1 回で。変わった列だけ・商品の行もそろう (画面のロールで)', async () => {
  const id = uuid();
  const r = await save('s003', {
    name: '単品 3 改', handling: 'active', standard_price: '1,280', tax_rate: '8', sales_class: '2', shipping_code: 'S01', reorder_months: '1.5',
    primary_supplier: '2', parent_code: 's001',
  }, { requestId: id, reason: '棚卸で見直し' });
  assert.deepEqual(r.changed.map((c) => c.field).sort(), ['name', 'parent_code', 'primary_supplier', 'reorder_months', 'sales_class', 'shipping_code', 'standard_price', 'tax_rate'].sort());
  assert.deepEqual(await skuRow('s003'), {
    name: '単品 3 改', tax_rate: 0.08, tax_class: 'REDUCED_8', handling: 'active', price: 1280, shipping_code: 'S01', shipping_method: 'ゆうパケット', ship: 210, months: 1.5,
    override: null, handling_own: null, pname: '単品 3 改', sales_class: 2, status: 'active', parent: 's001', parent_set_by: 'manual',
  });
  assert.deepEqual(await primaryOf('s003'), ['0002']);
  assert.equal((await reqRow(id)).status, 'done');
  assert.deepEqual((await reqRow(id)).result.changed.length, 8);
  assert.ok(r.ne_steps.some((s) => /翌朝の照合/.test(s)));
});

await ta('[3] 変更の記録: トリガーが actor = human・メール・portal_master_edit・request_id・理由を残す。設定は取引の外に漏れない', async () => {
  const id = uuid();
  await save('s002', { name: '名前 2 改 2', handling: 'discontinued' }, { requestId: id, reason: '取扱をやめた', actor: 'Other@Test' });
  const ev = await q('select entity_type, attribute, actor_type, actor_id, source_system, reason_text, db_user from events.master_change_events where request_id = $1 order by event_id', [id]);
  assert.ok(ev.length >= 4, JSON.stringify(ev));
  for (const e of ev) assert.deepEqual([e.actor_type, e.actor_id, e.source_system, e.reason_text, e.db_user], ['human', 'other@test', 'portal_master_edit', '取扱をやめた', 'master_edit']);
  assert.deepEqual(ev.filter((e) => e.entity_type === 'product').map((e) => e.attribute).sort(), ['name', 'status']);
  const skuEv = await q("select attribute from events.master_change_events where request_id = $1 and entity_type = 'sku' and entity_id = $2", [id, await skuId('s002')]);
  assert.deepEqual(skuEv.map((e) => e.attribute).sort(), ['handling', 'name']);
  assert.equal((await q("select current_setting('core.actor_type', true) as a"))[0].a || '', '');
});

await ta('[4] 保存した値は夜間ロード (持ち主 company) を 2 回流しても残る。load に戻すと夜間ロードが戻す', async () => {
  const snap = async () => ({ s003: await skuRow('s003'), s002: await skuRow('s002'), prim: await primaryOf('s003') });
  const before = await snap();
  await load(ALL_COMPANY); await load(ALL_COMPANY);
  assert.deepEqual(await snap(), before);
  await load(MASTER_OWNERSHIP);
  assert.equal((await skuRow('s003')).name, '単品 3');
  assert.equal((await skuRow('s003')).tax_rate, 0.1);
  assert.equal((await skuRow('s003')).parent, 's001');   // 人が決めた親 (manual) は load でも触らない (0036)
  await save('s003', { name: '単品 3 改', tax_rate: '8' });   // 以降の試験の前提
});

console.log('\n同じ request_id');

await ta('[5] 同じ中身 = 前の結果 (記録は増えない) / 違う中身・違う人・違う SKU = 409 / 失敗 = 同じ誤り / 記録は追記だけ', async () => {
  const id = uuid();
  const token = await tokenOf('s004');
  const eventId = await lastEvent();
  const first = await save('s004', { sales_class: '3' }, { requestId: id, token, eventId });
  const n = await nEvents();
  const again = await save('s004', { sales_class: '3' }, { requestId: id, token, eventId });
  assert.equal(again.replayed, true);
  assert.deepEqual(again.changed, first.changed);
  assert.equal(await nEvents(), n);
  await rejectsWith(save('s004', { sales_class: '2' }, { requestId: id, token, eventId }), 409, 'request_id_reused');
  await rejectsWith(save('s004', { sales_class: '3' }, { requestId: id, token, eventId, actor: 'other@test' }), 409, 'request_id_reused');
  await rejectsWith(save('s002', { sales_class: '3' }, { requestId: id }), 409, 'request_id_reused');   // 違う SKU へ同じ番号
  const bad = uuid();
  const t2 = await tokenOf('s004'); const e2 = await lastEvent();
  await rejectsWith(save('s004', { sales_class: '1' }, { requestId: bad, token: t2, eventId: e2, open: false }), 409, 'before_cutover');
  const e = await rejectsWith(save('s004', { sales_class: '1' }, { requestId: bad, token: t2, eventId: e2, open: false }), 409, 'before_cutover');
  assert.equal(e.extra.replayed, true);
  assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [bad]))[0].n), 1);
  assert.deepEqual((await q('select distinct status from ops.master_edit_requests order by 1')).map((x) => x.status), ['done', 'failed']);
  await assert.rejects(() => pg.query(`update ops.master_edit_requests set status = 'failed' where request_id = $1`, [id]), /append-only/);
  await assert.rejects(() => pg.query('delete from ops.master_edit_requests where request_id = $1', [id]), /append-only/);
});

console.log('\n編集の印 (楽観ロック)');

await ta('[6] 画面を開いた後に別の保存が SKU を変えた = 409 とその間の変更・何も書かない', async () => {
  const token = await tokenOf('s004');
  const since = await lastEvent();
  await save('s004', { standard_price: '1500' }, { actor: 'other@test' });
  const n = await nEvents();
  const e = await rejectsWith(save('s004', { name: '上書き' }, { token, eventId: since, reason: null }), 409, 'version_conflict');
  assert.ok(e.extra.events.some((x) => x.attribute === 'standard_price_jpy' && x.actor_id === 'other@test'), JSON.stringify(e.extra.events));
  assert.equal(await nEvents(), n);
  assert.equal((await skuRow('s004')).name, '単品 4 (分類・原価なし)');
});

await ta('[6] 行が増えた・変わった (仕入先ごとの商品・JAN・商品の行・構成品の値・含むセット) でも編集の印は変わる', async () => {
  const s001 = await skuId('s001');
  const pid = (await q('select product_id::text as id from core.skus where code = $1', ['s001']))[0].id;
  const steps = [
    ["update core.supplier_skus set vendor_code = 'AMC-XXX' where sku_id = $1", [s001]],
    ["insert into core.supplier_skus (company_id, supplier_id, sku_id) select 1, supplier_id, $1 from core.suppliers where code = '0002'", [s001]],   // 行が増えた (phantom)
    ["insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4900000000001', 'manual', 'human')", [pid]],
    ['update core.products set sales_class = 1 where product_id = $1', [pid]],
  ];
  for (const [sql, params] of steps) {
    const t0 = await tokenOf('s001');
    await pg.query(sql, params);
    assert.notEqual(await tokenOf('s001'), t0, sql);
    await rejectsWith(save('s001', { name: '変える' }, { token: t0 }), 409, 'version_conflict');
  }
  await pg.query('update core.products set sales_class = 3 where product_id = $1', [pid]);
  const t1 = await tokenOf('set001');
  await pg.query("update core.skus set standard_price_jpy = 1001 where code = 's002'");
  assert.notEqual(await tokenOf('set001'), t1);
  const t2 = await tokenOf('s002');
  await pg.query("update core.skus set standard_price_jpy = 5000 where code = 'set001'");
  assert.notEqual(await tokenOf('s002'), t2);
});

console.log('\n入力の検証');

await ta('[7] 形の誤り (400): 名前・売価・税率・分類・月数・原価・構成・知らない項目・種類に無い項目・番号・編集の印', async () => {
  const b = async (code, values, re) => { const e = await rejectsWith(save(code, values), 400); if (re) assert.match(e.message, re); };
  await b('s001', { name: '' }, /空/);
  await b('s001', { name: 'EMPTY' }, /empty/);
  await b('s001', { name: 'a\nb' }, /改行/);
  await b('s001', { name: 'あ'.repeat(256) }, /255/);
  await b('s001', { standard_price: '0' }, /1〜/);
  await b('s001', { standard_price: '12.5' }, /整数/);
  await b('s001', { tax_rate: '5' }, /8% か 10%/);
  await b('s001', { tax_rate: '' }, /空にできません/);
  await b('s001', { sales_class: '5' }, /1〜4/);
  await b('s001', { reorder_months: '61' }, /0〜60/);
  await b('s001', { reorder_months: '1.25' }, /小数/);
  await b('s001', { cost: { jpy: '-1', reason: 'x' } }, /0〜/);
  await b('s001', { cost: { jpy: '100' } }, /理由/);
  await b('s001', { handling: 'paused' }, /取扱/);
  await b('s001', { primary_supplier: '0003' }, /取引停止/);
  await b('s001', { primary_supplier: '0099' }, /ありません/);
  await b('s001', { parent_code: 's001' }, /自分自身/);
  await b('s001', { parent_code: 'set001' }, /単品か代表の名札/);
  await b('s001', { parent_code: 'nope' }, /見つかりません/);
  await b('s001', { shipping_code: 'S99' }, /送料の表にありません/);
  await b('s001', { components: [{ code: 's002', qty: 1 }] }, /単品では直せません/);
  await b('set001', { tax_rate: '10' }, /セットでは直せません/);
  await b('set001', { components: [] }, /1〜20/);
  await b('set001', { components: Array.from({ length: 21 }, (_, i) => ({ code: `x${i}`, qty: 1 })) }, /1〜20/);
  await b('set001', { components: [{ code: 's001', qty: 0 }] }, /1〜999/);
  await b('set001', { components: [{ code: 's001', qty: 1000 }] }, /1〜999/);
  await b('set001', { components: [{ code: 's001', qty: 1 }, { code: 'S001', qty: 2 }] }, /2 回/);
  await b('set001', { components: [{ code: 'set001', qty: 1 }] }, /セット自身/);
  await b('set001', { components: [{ code: 'set004', qty: 1 }] }, /入れ子/);
  await b('set001', { components: [{ code: 'nope', qty: 1 }] }, /ありません/);
  await b('s001', { color: 'red' }, /知らない項目/);
  await rejectsWith(W.saveSku(db, { actor: 'naka@test', requestId: 'x', code: 's001', seen: { token: 'a'.repeat(64) }, values: {} }, { open: true, ownership: ALL_COMPANY }), 400);
  await rejectsWith(W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 's001', seen: {}, values: {} }, { open: true, ownership: ALL_COMPANY }), 400);
  await rejectsWith(W.saveSku(db, { actor: '', requestId: uuid(), code: 's001', seen: { token: 'a'.repeat(64) }, values: {} }, { open: true, ownership: ALL_COMPANY }), 400);
  await rejectsWith(save('nope', { name: 'x' }, { token: 'a'.repeat(64) }), 404, 'not_found');
  const e = await rejectsWith(save('s001', { shipping_code: 'S01' }, { shippingRates: null }), 503, 'shipping_rates_unavailable');
  assert.equal(e.extra.field, 'shipping_code');
});

await ta('[7] 新しい商品コードの形 (⑤-2 で使う): 小文字の英数字・- _・30 字まで・大文字は禁止・前後の空白・SET- で始まらない', () => {
  for (const ok of ['abc-01', 'a_b', '0', 'x'.repeat(30)]) assert.deepEqual(W.validateNewSkuCode(ok), { ok: true, code: ok, message: null }, ok);
  const bad = (v, re) => { const r = W.validateNewSkuCode(v); assert.equal(r.ok, false, String(v)); assert.match(r.message, re); };
  bad('', /入れて/); bad(null, /入れて/); bad(' abc', /空白/); bad('Abc', /大文字/); bad('ABC-01', /大文字/);
  bad('abc 01', /使える文字/); bad('ａｂｃ', /使える文字/); bad('x'.repeat(31), /30 字/); bad('abc.01', /使える文字/); bad('set-abc', /set-/);
});

console.log('\nセットの導く値');

await ta('[8] 単品の税率を変えると、含むセットの税率・税区分を同じ取引で (記録も同じ request_id)。NE でやることに出る', async () => {
  assert.deepEqual([(await skuRow('set001')).tax_rate, (await skuRow('set001')).tax_class], [0.08, 'MIXED']);
  const id = uuid();
  const r = await save('s002', { tax_rate: '10' }, { requestId: id });
  assert.deepEqual(r.derived.map((d) => [d.code, d.col, d.to]), [['set001', 'tax_rate', { rate: 0.1, class: 'STANDARD_10' }]]);
  assert.deepEqual([(await skuRow('set001')).tax_rate, (await skuRow('set001')).tax_class], [0.1, 'STANDARD_10']);
  const ev = await q("select e.attribute from events.master_change_events e where e.request_id = $1 and e.entity_type = 'sku' and e.entity_id = (select sku_id from core.skus where code = 'set001')", [id]);
  assert.deepEqual(ev.map((e) => e.attribute).sort(), ['tax_class', 'tax_rate']);
  assert.ok(r.ne_steps.some((s) => /set001 の税率/.test(s)));
});

await ta('[8] 単品を中止にすると含むセットも中止 / 取扱中に戻しても「セット自身の取扱」が決まっていないセットは中止のまま (気をつけること)', async () => {
  let r = await save('s001', { handling: 'discontinued' });
  assert.deepEqual(r.derived.filter((d) => d.col === 'handling').map((d) => d.code).sort(), ['set001', 'set004', 'set005', 'set006']);
  assert.equal((await skuRow('set001')).handling, 'discontinued');
  r = await save('s001', { handling: 'active' });
  assert.equal((await skuRow('set001')).handling, 'discontinued');
  assert.ok(r.warnings.some((w) => /セット set001 は「セット自身の取扱」が決まっていない/.test(w)), JSON.stringify(r.warnings));
  r = await save('set001', { handling_own: 'active' });
  assert.deepEqual(r.derived.map((d) => [d.col, d.to]), [['handling', 'active']]);
  assert.equal((await skuRow('set001')).handling, 'active');
});

console.log('\n原価');

await ta('[9] 単品の原価: 今日から (今の行は昨日で閉じる)・含むセットの合計も今日から・過去の行は変えない', async () => {
  assert.deepEqual((await costsOf('s001')).map((c) => [c.jpy, c.f, c.t]), [[100, '2030-01-05', null]]);
  const r = await save('s001', { cost: { jpy: '130', reason: '値上げ' } });
  assert.deepEqual((await costsOf('s001')).map((c) => [c.jpy, c.src, c.st, c.f, c.t]), [[100, 'ne', 'COMPLETE', '2030-01-05', '2030-01-09'], [130, 'manual', 'COMPLETE', TODAY, null]]);
  // set001 = s001×2 + s002×1 = 260 + 200 / set005 = s001 + s005 = 130 + 80 / set004・set006 は原価の無い構成品がある (行も無いので何もしない)
  assert.deepEqual(r.derived.filter((d) => d.col === 'cost').map((d) => [d.code, d.from, d.to]), [['set001', 400, 460], ['set005', 180, 210]]);
  assert.deepEqual((await costsOf('set001')).map((c) => [c.jpy, c.src, c.f, c.t]), [[400, 'set_calc', '2030-01-05', '2030-01-09'], [460, 'set_calc', TODAY, null]]);
});

await ta('[9] 今日 2 回目は今日の行を入れ替える (期間を重ねない)。同じ値なら変わりなし。先の日付の行があれば入れない', async () => {
  await save('s001', { cost: { jpy: '120', reason: '打ち間違い' } });
  assert.deepEqual((await costsOf('s001')).map((c) => [c.jpy, c.f, c.t]), [[100, '2030-01-05', '2030-01-09'], [120, TODAY, null]]);
  assert.deepEqual((await costsOf('set001')).map((c) => [c.jpy, c.f, c.t]), [[400, '2030-01-05', '2030-01-09'], [440, TODAY, null]]);
  assert.equal((await save('s001', { cost: { jpy: '120', reason: '同じ' } })).no_change, true);
  const overlap = await q(`select count(*)::int as n from core.sku_costs a join core.sku_costs b on a.sku_id = b.sku_id and a.sku_cost_id < b.sku_cost_id
    and a.valid_from <= coalesce(b.valid_to, 'infinity') and b.valid_from <= coalesce(a.valid_to, 'infinity')`);
  assert.equal(overlap[0].n, 0);
  await pg.query("update core.sku_costs set valid_to = '2030-01-14' where valid_to is null and sku_id = (select sku_id from core.skus where code = 's002')");
  await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 210, 'ne', 'COMPLETE', '2030-01-15' from core.skus where code = 's002'");
  await rejectsWith(save('s002', { cost: { jpy: '205', reason: 'x' } }), 400, 'cost_future');
  await pg.query("delete from core.sku_costs where valid_from = '2030-01-15'");
  await pg.query("update core.sku_costs set valid_to = null where valid_to = '2030-01-14'");
});

await ta('[9] 期間の重なりは DB が拒む (この画面・昇格の書き込み)。夜間ロード・ほかの書き手は見ない (今の動きを止めない = ⑥ の前提)', async () => {
  const sid = await skuId('s003');
  const asPortal = async (sql) => {
    await pg.query('begin');
    try { await pg.query("select set_config('core.source_system', 'portal_master_edit', true)"); await pg.query(sql, [sid]); await pg.query('commit'); }
    catch (e) { await pg.query('rollback'); throw e; }
  };
  await assert.rejects(() => asPortal("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 1, 'manual', 'COMPLETE', '2030-01-06', '2030-01-07')"), /sku_cost_overlap/);
  await asPortal("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 1, 'manual', 'COMPLETE', '2029-12-01', '2030-01-04')");
  await assert.rejects(() => asPortal("update core.sku_costs set valid_to = '2030-01-05' where sku_id = $1 and valid_from = '2029-12-01'"), /sku_cost_overlap/);
  await pg.query("delete from core.sku_costs where sku_id = $1 and valid_from = '2029-12-01'", [sid]);
  for (const src of ['company_db_load', '']) {
    await pg.query('begin');
    await pg.query("select set_config('core.source_system', $1, true)", [src]);
    await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 1, 'ne', 'COMPLETE', '2030-01-06', '2030-01-06')", [sid]);
    await pg.query('rollback');
  }
});

await ta('[9] 「今日」は東京の日付 (UTC 14:59:59 と 15:00 の境目・DB の Asia/Tokyo と同じ答え)', async () => {
  for (const [iso, want] of [['2030-01-10T14:59:59Z', '2030-01-10'], ['2030-01-10T15:00:00Z', '2030-01-11'], ['2030-12-31T15:00:00Z', '2031-01-01'], ['2030-01-10T00:00:00Z', '2030-01-10']]) {
    assert.equal(W.jstDate(new Date(iso)), want, iso);
    assert.equal((await q(`select (($1::timestamptz) at time zone 'Asia/Tokyo')::date::text as d`, [iso]))[0].d, want, `DB ${iso}`);
  }
  await save('s005', { cost: { jpy: '81', reason: '日の境目' } }, { now: new Date('2030-01-10T15:00:00Z') });
  assert.deepEqual((await costsOf('s005')).map((c) => [c.jpy, c.f, c.t]), [[80, '2030-01-05', '2030-01-10'], [81, '2030-01-11', null]]);
});

console.log('\nNE に取り込む CSV');

await ta('[10] CSV が出ている列は変えない (作った・確かめた / 申告して確かめ待ち)。CSV に無い列・void・確かめ済みは変えられる', async () => {
  const mk = async (state) => {
    const extra = state === 'declared' ? ', checked_at, checked_run, declared_at, declared_by' : '';
    const vals = state === 'declared' ? ", now(), 'mc_20300101T000000000Z_abcdef', now(), 'x@test'" : '';
    return (await q(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by, state${extra})
      values ('products', 'name', 'syohin_name', 'v1', 'utf8', true, 1, repeat('a', 64), decode('00', 'hex'), 'mc_20300101T000000000Z_abcdef', 'x@test', '${state}'${vals}) returning export_id::text as id`))[0].id;
  };
  const row = (id, code, col) => pg.query(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell, cdb_version, evidence)
    values ($1, 'to_ne', $2, $3, $2, '{"value":"a"}', 'a', 1, '{}')`, [id, code, col]);
  const made = await mk('made');
  await row(made, 's003', 'name');
  const monthsBefore = (await skuRow('s003')).months;
  let e = await rejectsWith(save('s003', { name: '直したい', reorder_months: '3' }), 409, 'csv_issued');
  assert.match(e.message, new RegExp(`#${made}`));
  assert.equal((await skuRow('s003')).months, monthsBefore);
  await save('s003', { reorder_months: '3' });
  await pg.query("update ops.ne_csv_exports set state = 'void', void_at = now(), void_by = 'x@test', void_reason = 'by_user' where export_id = $1", [made]);
  await pg.query("update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = 'void' where export_id = $1", [made]);
  await save('s003', { name: '単品 3 改 2' });
  const declared = await mk('declared');
  await row(declared, 's003', 'tax_rate');
  e = await rejectsWith(save('s003', { tax_rate: '10' }), 409, 'csv_issued');
  assert.deepEqual(e.extra.exports.map((x) => [x.col, x.state]), [['tax_rate', 'declared']]);
  await pg.query("update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = 'confirmed' where export_id = $1", [declared]);
  await save('s003', { tax_rate: '10' });
  assert.equal((await skuRow('s003')).tax_rate, 0.1);
});

console.log('\nセット: 構成の依頼・NE の観測・上げる・食い違い');

await ta('[11] 構成を変える = 依頼だけ (core は変えない)。NE でやること・印が変わる・置き換え・今の構成に戻す = 取り下げ・並べ替えも依頼', async () => {
  const before = await compsOf('set001');
  const t0 = await tokenOf('set001');
  const r = await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 2 }] }, { reason: '中身を変える' });
  assert.equal(r.changed[0].field, 'components');
  assert.ok(r.ne_steps.some((s) => /s002 を外して/.test(s)), JSON.stringify(r.ne_steps));
  assert.ok(r.ne_steps.some((s) => /s003×2 を足す/.test(s) && /s001 の数量を 2→1/.test(s)), JSON.stringify(r.ne_steps));
  assert.deepEqual(await compsOf('set001'), before);
  const open = await q("select rows, base_rows, status, requested_by, reason from ops.sku_component_requests where status = 'open'");
  assert.equal(open.length, 1);
  assert.deepEqual(open[0].rows.map((x) => [x.code, x.qty, x.sort]), [['s001', 1, 1], ['s003', 2, 2]]);
  assert.deepEqual(open[0].base_rows.map((x) => [x.code, x.qty]), [['s001', 2], ['s002', 1]]);
  assert.deepEqual([open[0].requested_by, open[0].reason], ['naka@test', '中身を変える']);
  assert.notEqual(await tokenOf('set001'), t0);
  await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 3 }] });
  assert.deepEqual((await q('select status, close_reason from ops.sku_component_requests order by component_request_id')).map((x) => [x.status, x.close_reason]), [['cancelled', 'superseded'], ['open', null]]);
  const w = await save('set001', { components: [{ code: 's001', qty: 2 }, { code: 's002', qty: 1 }] });
  assert.ok(w.ne_steps.some((s) => /取り下げ/.test(s)));
  assert.deepEqual((await q('select status, close_reason from ops.sku_component_requests order by component_request_id')).map((x) => [x.status, x.close_reason]), [['cancelled', 'superseded'], ['cancelled', 'withdrawn']]);
  const re = await save('set001', { components: [{ code: 's002', qty: 1 }, { code: 's001', qty: 2 }] });
  assert.ok(re.ne_steps.some((s) => /並びを s002 → s001/.test(s)), JSON.stringify(re.ne_steps));
  await save('set001', { components: [{ code: 's001', qty: 2 }, { code: 's002', qty: 1 }] });
  await assert.rejects(() => pg.query(`update ops.sku_component_requests set rows = '[{"sku_id":1,"qty":1,"sort":1}]'::jsonb`), /書き換えない|閉じた依頼/);
  await assert.rejects(() => pg.query('delete from ops.sku_component_requests'), /消さない/);
});

await ta('[11] NE の観測を書く: 完全な回は残せないセット・構成品・重なり・数の違い・原本のハッシュが無いと拒む / 完全でない回は飛ばして数える / 時刻の範囲 / 再送 / 観測のロールだけ', async () => {
  const at = new Date(Date.now() + 60000).toISOString();
  const full = (sets, extra = {}) => ({ run_id: `ne_w_${++runSeq}`, observed_at: at, complete: true, requested: sets.length, fetched: sets.length, raw_hash: 'c'.repeat(64), source_generation: 'g', sets, ...extra });
  const w = (p) => asRole(E0, 'master_observer', () => W.recordNeSetObservations(db, p));
  const ok = [{ set_code: 'SET001', rows: [{ code: 's001', qty: 2, sort: 1 }] }];
  await assert.rejects(() => w(full([...ok, { set_code: 'nope', rows: [] }])), /残せないセット.*知らないセット/);
  await assert.rejects(() => w(full([...ok, { set_code: 's001', rows: [] }])), /知らないセット・セットでない s001/);
  await assert.rejects(() => w(full([...ok, { set_code: 'set001', rows: [] }])), /同じセットが 2 回/);
  await assert.rejects(() => w(full([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 'zzz', qty: 1, sort: 2 }] }])), /知らない構成品/);
  await assert.rejects(() => w(full([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 's002', qty: 1, sort: 1 }] }])), /並びが重なる/);
  await assert.rejects(() => w(full([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 'S001', qty: 1, sort: 2 }] }])), /構成品・並びが重なる/);
  await assert.rejects(() => w(full(ok, { requested: 2 })), /requested = fetched/);
  await assert.rejects(() => w(full(ok, { raw_hash: null })), /raw_hash/);
  await assert.rejects(() => w(full(ok, { observed_at: new Date(Date.now() + 10 * 60000).toISOString() })), /未来/);
  await assert.rejects(() => w(full(ok, { observed_at: new Date(Date.now() - 40 * 3600000).toISOString() })), /古すぎる/);
  await assert.rejects(() => w(full(ok, { run_id: 'bad run' })), /run_id/);
  const p = full(ok);
  assert.deepEqual(await w(p), { state: 'written', run_id: p.run_id, sets: 1, skipped: 0 });
  assert.equal((await w(p)).state, 'unchanged');
  await assert.rejects(() => w({ ...p, source_generation: 'g2' }), /run_conflict/);
  const run = (await q('select complete, requested_count, fetched_count, saved_count, skipped_count, raw_hash, source_generation from ops.ne_set_observation_runs where run_id = $1', [p.run_id]))[0];
  assert.deepEqual(run, { complete: true, requested_count: 1, fetched_count: 1, saved_count: 1, skipped_count: 0, raw_hash: 'c'.repeat(64), source_generation: 'g' });
  // 完全でない回: 残せないものは飛ばして数える (上げる根拠には使わない)
  const inc = await w({ run_id: `ne_w_${++runSeq}`, observed_at: at, complete: false, sets: [...ok, { set_code: 'nope', rows: [] }, { set_code: 's001', rows: [] }, { set_code: 'set004', rows: [{ code: 'zzz', qty: 1, sort: 1 }] }] });
  assert.deepEqual([inc.sets, inc.skipped], [2, 2]);
  // 画面のロール・持ち主でも観測の表へ直接は書けない (持ち主は試験の作り方として書ける = 画面のロールだけ確かめる)
  assert.equal(await pgCode(asRole(E0, 'master_edit', () => pg.query("select ops.record_ne_set_observations('{}'::jsonb)"))), '42501');
  await assert.rejects(() => pg.query('delete from ops.ne_set_observations'), /append-only/);
  await assert.rejects(() => pg.query("update ops.ne_set_observation_runs set complete = false"), /append-only/);
});


await ta('[11] 上げない: 偽の番号・完全でない回・依頼より前の観測・並び / 行の数 / 数量が違う (食い違いを残す)・保存が開いていない', async () => {
  await save('set001', { components: [{ code: 's003', qty: 2 }, { code: 's001', qty: 1 }] }, { reason: '入れ替え' });
  const reqAt = Date.parse((await q("select created_at::text as t from ops.sku_component_requests where status = 'open'"))[0].t);
  const before = await compsOf('set001');
  const good = [{ code: 's003', qty: 2, sort: 10 }, { code: 'S001', qty: 1, sort: 20 }];
  for (const fake of ['999999', 'abc', null, { set_code: 'set001', complete: true, rows: good }]) assert.equal((await promote(fake)).reason, 'no_observation', JSON.stringify(fake));
  assert.equal((await promote((await observe([{ set_code: 'set001', rows: good }], { complete: false })).set001)).reason, 'incomplete_observation');
  assert.equal((await promote((await observe([{ set_code: 'set001', rows: good }], { at: new Date(reqAt - 3600000).toISOString() })).set001)).reason, 'stale_observation');
  assert.equal((await promote((await observe([{ set_code: 'set001', rows: good }])).set001, { ownership: MASTER_OWNERSHIP })).reason, 'before_cutover');
  const tries = [
    [[good[1], good[0]].map((r, i) => ({ ...r, sort: i + 1 })), 'mismatch'],   // 並びが違う
    [[...good, { code: 's002', qty: 1, sort: 30 }], 'mismatch'],                 // 行が多い
    [[good[0]], 'mismatch'],                                                      // 行が足りない
    [[{ ...good[0], qty: 3 }, good[1]], 'mismatch'],                              // 数量が違う
  ];
  for (const [rows, why] of tries) {
    const r = await promote((await observe([{ set_code: 'set001', rows }])).set001);
    assert.deepEqual([r.promoted, r.reason], [false, why], JSON.stringify(rows));
  }
  assert.deepEqual(await compsOf('set001'), before);
  // 食い違いは開いているのが 1 つ (中身が変われば前のを閉じて新しく)
  const br = await q("select kind, status, close_reason from ops.sku_component_breaches where set_sku_id = (select sku_id from core.skus where code = 'set001') order by breach_id");
  assert.deepEqual(br.filter((b) => b.status === 'open').map((b) => b.kind), ['mismatch']);
  assert.equal(br.filter((b) => b.close_reason === 'superseded').length, tries.length - 1);
  // 同じ中身の食い違いをもう一度 = 増やさない
  const n = br.length;
  await promote((await observe([{ set_code: 'set001', rows: tries.at(-1)[0] }])).set001);
  assert.equal(Number((await q('select count(*)::int as n from ops.sku_component_breaches'))[0].n), n);
  // 依頼から 7 日より後の観測でも違う = stale (mismatch は閉じる)
  const st = await promote(await observeRaw('set001', [good[0]], new Date(reqAt + 8 * 86400000).toISOString()));
  assert.equal(st.reason, 'stale');
  assert.deepEqual((await q("select kind from ops.sku_component_breaches where status = 'open'")).map((b) => b.kind), ['stale']);
  // 画面にも出る
  const page = await R.readSkuPage(db, 'set001', { now: NOW, ownership: ALL_COMPANY, open: true });
  assert.deepEqual(page.breaches.map((b) => b.kind), ['stale']);
  await assert.rejects(() => pg.query("update ops.sku_component_breaches set kind = 'mismatch' where status = 'open'"), /書き換えない/);
  await assert.rejects(() => pg.query("update ops.sku_component_breaches set closed_by = 'x' where status = 'closed'"), /閉じた食い違いは変えない/);
  await assert.rejects(() => pg.query('delete from ops.sku_component_breaches'), /消さない/);
});

await ta('[11] 上げる: 観測が完全・依頼より後・構成品 / 数量 / 並び / 行の数まで同じ。同じ取引で構成・導く値・依頼 (applied)・食い違い (resolved)', async () => {
  const good = [{ code: 's003', qty: 2, sort: 10 }, { code: 'S001', qty: 1, sort: 20 }];
  const obsId = (await observe([{ set_code: 'set001', rows: good }], { at: new Date(Date.now() + 120000).toISOString() })).set001;
  const r = await promote(obsId);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.deepEqual(await compsOf('set001'), [['s003', 2, 1, 'ne'], ['s001', 1, 2, 'ne']]);
  const req = (await q("select status, close_reason, closed_by, applied_observation_id::text as oid from ops.sku_component_requests where set_sku_id = (select sku_id from core.skus where code = 'set001') order by component_request_id desc limit 1"))[0];
  assert.deepEqual(req, { status: 'applied', close_reason: 'matched', closed_by: 'system', oid: obsId });
  assert.deepEqual((await q("select distinct status, close_reason from ops.sku_component_breaches where set_sku_id = (select sku_id from core.skus where code = 'set001') and close_reason <> 'superseded'")),
    [{ status: 'closed', close_reason: 'resolved' }]);
  // 導く値も同じ取引で (s003 50×2 + s001 120 = 220・税率 s003 10% (直した)・s001 10%)
  assert.ok(r.derived.some((d) => d.col === 'cost' && d.to === 220), JSON.stringify(r.derived));
  assert.equal((await costsOf('set001')).at(-1).jpy, 220);
  const ev = await q("select distinct actor_type, source_system, run_id from events.master_change_events where source_system = 'ne_observation'");
  assert.deepEqual(ev.map((x) => [x.actor_type, x.source_system]), [['system', 'ne_observation']]);
  assert.equal((await promote(obsId)).reason, 'no_open_request');
});

await ta('[11] 依頼が無いのに NE の構成が今の構成と違う = unrequested_diff を残す → NE が今の構成に戻った観測で resolved', async () => {
  let r = await promote((await observe([{ set_code: 'set005', rows: [{ code: 's001', qty: 1, sort: 1 }] }])).set005);
  assert.deepEqual([r.promoted, r.reason], [false, 'unrequested_diff']);
  assert.deepEqual((await q("select kind, component_request_id from ops.sku_component_breaches where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set005')")),
    [{ kind: 'unrequested_diff', component_request_id: null }]);
  r = await promote((await observe([{ set_code: 'set005', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 's005', qty: 1, sort: 2 }] }])).set005);
  assert.deepEqual([r.reason, r.closed_breaches], ['no_open_request', 1]);
  assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_breaches where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set005')"))[0].n), 0);
});

await ta('[11] 依頼の後に「依頼にだけある構成品」の原価が 0 になった = NE の構成が依頼どおりでも上げない (core は変えない・underivable を残す) → 直すと上げる', async () => {
  await save('set001', { components: [{ code: 's003', qty: 2 }, { code: 's001', qty: 1 }, { code: 's002', qty: 1 }] }, { reason: 's002 を足す' });
  const s2 = await save('s002', { cost: { jpy: '0', reason: '仕入先が無償に' } });   // s002 はまだどのセットにも入っていない = セットは計算し直さない
  assert.deepEqual(s2.derived, []);
  const before = { comps: await compsOf('set001'), cost: (await costsOf('set001')).at(-1) };
  const rows = [{ code: 's003', qty: 2, sort: 1 }, { code: 's001', qty: 1, sort: 2 }, { code: 's002', qty: 1, sort: 3 }];
  let r = await promote((await observe([{ set_code: 'set001', rows }], { at: new Date(Date.now() + 90000).toISOString() })).set001);
  assert.deepEqual([r.promoted, r.reason], [false, 'underivable']);
  assert.match(r.blockers.join(' '), /s002 の原価/);
  assert.deepEqual({ comps: await compsOf('set001'), cost: (await costsOf('set001')).at(-1) }, before);
  assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_requests where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set001')"))[0].n), 1);
  const br = await q("select kind, details from ops.sku_component_breaches where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set001')");
  assert.deepEqual(br.map((b) => b.kind), ['underivable']);
  assert.ok(br[0].details.blockers.length > 0);
  await save('s002', { cost: { jpy: '200', reason: '戻した' } });
  r = await promote((await observe([{ set_code: 'set001', rows }], { at: new Date(Date.now() + 100000).toISOString() })).set001);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.deepEqual(await compsOf('set001'), [['s003', 2, 1, 'ne'], ['s001', 1, 2, 'ne'], ['s002', 1, 3, 'ne']]);
  assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_breaches where status = 'open'"))[0].n), 0);
});

await ta('[12] 導く値が決まらないセットは保存しない: 分類 → 上書きで・原価 → 例外原価で通る。税率が決まらない (上書きなし) は通らない', async () => {
  await pg.query("update core.products set sales_class = null where product_id = (select product_id from core.skus where code = 's004')");   // 構成品の分類が未入力
  let e = await rejectsWith(save('set004', { name: 'セット 4 改' }), 400, 'set_underivable');
  assert.equal(e.extra.blockers.length, 2);
  assert.match(e.extra.blockers.join(' '), /売上分類/); assert.match(e.extra.blockers.join(' '), /s004 の原価/);
  e = await rejectsWith(save('set004', { name: 'セット 4 改', set_sales_class_override: '3' }), 400, 'set_underivable');
  assert.equal(e.extra.blockers.length, 1);
  const r = await save('set004', { name: 'セット 4 改', set_sales_class_override: '3', exception_cost: { jpy: '999', reason: '仕入先の見積' } });
  assert.deepEqual(r.changed.map((c) => c.field).sort(), ['exception_cost', 'name', 'set_sales_class_override']);
  assert.deepEqual([(await skuRow('set004')).name, (await skuRow('set004')).override], ['セット 4 改', 3]);
  assert.deepEqual((await costsOf('set004')).map((c) => [c.jpy, c.src, c.st, c.f, c.t]), [[999, 'manual', 'OVERRIDDEN', TODAY, null]]);
  e = await rejectsWith(save('set005', { name: 'セット 5 改' }), 400, 'set_underivable');
  assert.deepEqual(e.extra.blockers.length, 1);
  assert.match(e.extra.blockers[0], /s005 の税率が未入力/);
  await pg.query("update core.products set sales_class = 1 where product_id = (select product_id from core.skus where code = 's004')");
  e = await rejectsWith(save('set004', { set_sales_class_override: '2' }), 400);
  assert.match(e.message, /導けるので、上書きはできません/);
});

await ta('[12] 今の構成は導けない + 依頼の構成は導ける: 上書き・例外原価を外すのは拒む (今の構成の値を壊さない)。依頼の構成が導けない依頼も拒む', async () => {
  let r = await save('set006', { set_sales_class_override: '3', exception_cost: { jpy: '500', reason: '見積' } });
  assert.deepEqual(r.changed.map((c) => c.field).sort(), ['exception_cost', 'set_sales_class_override']);
  r = await save('set006', { components: [{ code: 's001', qty: 1 }, { code: 's002', qty: 1 }] });   // 依頼の構成は上書きなしでも導ける
  assert.equal(r.changed[0].field, 'components');
  const before = { costs: await costsOf('set006'), row: await skuRow('set006') };
  let e = await rejectsWith(save('set006', { exception_cost: { clear: true, reason: 'やめる' } }), 400, 'set_underivable');
  assert.match(e.extra.blockers.join(' '), /今の構成: .*s006 の原価/);
  assert.ok(!e.extra.blockers.some((b) => /依頼の構成/.test(b)), JSON.stringify(e.extra.blockers));
  e = await rejectsWith(save('set006', { set_sales_class_override: '' }), 400, 'set_underivable');
  assert.match(e.extra.blockers.join(' '), /今の構成: .*売上分類/);
  assert.deepEqual({ costs: await costsOf('set006'), row: await skuRow('set006') }, before);
  // 逆: 今の構成は導ける・依頼の構成が導けない (s006 = 原価・分類なし) = 依頼を受けない
  e = await rejectsWith(save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's006', qty: 1 }] }), 400, 'set_underivable');
  assert.match(e.extra.blockers.join(' '), /依頼の構成: /);
});

await ta('[12] 例外原価をやめる = 今日の例外の行を消して、今日から構成品の合計 (合計できなければ保存しない)', async () => {
  const e = await rejectsWith(save('set004', { exception_cost: { clear: true, reason: 'やめる' } }), 400, 'set_underivable');
  assert.match(e.message, /原価/);
  await save('s004', { cost: { jpy: '70', reason: '入れた' } });
  const r = await save('set004', { exception_cost: { clear: true, reason: 'やめる' } });
  assert.deepEqual(r.derived.map((d) => [d.col, d.to]), [['cost', 190]]);   // s001 120 + s004 70
  assert.deepEqual((await costsOf('set004')).map((c) => [c.jpy, c.src, c.f, c.t]), [[190, 'set_calc', TODAY, null]]);
});

console.log('\n上げる処理と単品の保存の順番');

await ta('[13] 単品の保存 → 上げる: 上げたセットは新しい単品の値で計算する / 上げる → 単品の保存: 上げる前の画面は 409・新しい画面の保存は増えたセットも計算し直す', async () => {
  // set004 に s003 を足す依頼。s003 の原価を 50 → 55 に直してから上げる
  await save('set004', { components: [{ code: 's001', qty: 1 }, { code: 's004', qty: 1 }, { code: 's003', qty: 1 }] });
  await save('s003', { cost: { jpy: '55', reason: '上げる前に直した' } });
  const before = await tokenOf('s003');
  const r = await promote((await observe([{ set_code: 'set004', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 's004', qty: 1, sort: 2 }, { code: 's003', qty: 1, sort: 3 }] }], { at: new Date(Date.now() + 180000).toISOString() })).set004);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.equal((await costsOf('set004')).at(-1).jpy, 120 + 70 + 55);   // s001 + s004 + 直した s003
  // 上げた後: s003 を含むセットが増えた = 上げる前に開いた画面の保存は 409
  await rejectsWith(save('s003', { cost: { jpy: '56', reason: '古い画面' } }, { token: before }), 409, 'version_conflict');
  const s2 = await save('s003', { cost: { jpy: '56', reason: '新しい画面' } });
  assert.ok(s2.derived.some((d) => d.code === 'set004' && d.col === 'cost' && d.to === 120 + 70 + 56), JSON.stringify(s2.derived));
});

console.log('\nDB のロール');

await ta('[14] master_edit = 画面の読み書きだけ (記録の偽造・過去の原価・版・キーの列も不可) / master_ops = 段階を進める関数 / master_observer = 観測 / master_gate = 門の記録', async () => {
  const as = async (role, sql, params = []) => {
    await pg.query(`set role ${role}`);
    try { return await pgCode(pg.query(sql, params)); } finally { await pg.query('set role deploy'); }
  };
  const deny = [
    [`select ops.set_master_cutover_phase('frozen', 'x', '{}'::jsonb)`, '42501'],
    [`select ops.record_ne_set_observations('{}'::jsonb)`, '42501'],
    ['insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, 1, 2, 1, \'manual\')', '42501'],
    ['delete from core.sku_components where false', '42501'],
    ["update ops.master_cutover_state set note = 'x'", '42501'],
    ["insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count) select 'render', 'x', 'x', manifest_hash, repeat('a', 64), 'new_open', 0 from ops.master_legacy_manifests limit 1", '42501'],
    ["select ops.record_legacy_gate_ack('render', 'x', 'b', '{}'::jsonb, repeat('a', 64), 'new_open', 0, null)", '42501'],
    ["insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_key, new_value, actor_type, source_system) values (1, gen_random_uuid(), 'INSERT', 'sku', '{}', '{}', 'human', 'fake')", '42501'],
    ['update core.skus set version = version where false', '42501'],
    ["insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code) values (1, 1, 1, 'x')", '42501'],
    ["update core.suppliers set active = false where false", '42501'],
    ['update core.suppliers set version = version where false', '42501'],
    ['select 1 from core.suppliers for share', '42501'],
    ["insert into ops.ne_set_observation_runs (run_id, observed_at, complete, saved_count, skipped_count, content_hash) values ('x', now(), false, 0, 0, repeat('a', 32))", '42501'],
    ['update core.skus set code = code where false', '42501'],
    ['delete from core.skus where false', '42501'],
    ['select 1 from core.orders limit 1', '42501'],
    ['update ops.sku_component_requests set rows = rows where false', '42501'],
  ];
  for (const [sql, want] of deny) assert.equal(await as('master_edit', sql), want, sql);
  for (const sql of ['select 1 from core.skus limit 1', 'select 1 from ops.master_cutover_state', "update core.skus set name = name where false", 'select 1 from ops.sku_component_breaches limit 1']) assert.equal(await as('master_edit', sql), 'ok', sql);
  // 原価: 今日より前に始まった行は消せない・閉じた行は変えられない (画面のロールでも)
  await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) select 1, sku_id, 10, 'ne', 'COMPLETE', '2026-01-01', '2026-01-31' from core.skus where code = 's006'");
  assert.equal(await as('master_edit', "delete from core.sku_costs where valid_from = '2026-01-01'"), '42501');
  assert.equal(await as('master_edit', "update core.sku_costs set valid_to = '2026-02-28' where valid_from = '2026-01-01'"), '42501');
  assert.equal(await as('master_edit', "insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 11, 'manual', 'COMPLETE', '2026-01-10' from core.skus where code = 's006'"), '23P01');   // 画面のロールは source を偽っても重なりを拒む
  await pg.query("delete from core.sku_costs where valid_from = '2026-01-01'");
  // master_observer / master_gate: それぞれの関数だけ
  assert.equal(await as('master_observer', 'select 1 from core.skus limit 1'), '42501');
  assert.equal(await as('master_observer', `select ops.set_master_cutover_phase('frozen', 'x', '{}'::jsonb)`), '42501');
  assert.equal(await as('master_gate', `select ops.record_ne_set_observations('{}'::jsonb)`), '42501');
  assert.equal(await as('master_gate', 'select phase from ops.master_cutover_state'), 'ok');
  // master_ops: 関数は動く (段階はもう new_open = one_way で止まる = 権限では拒まれない)・表は直接書けない・商品は読めない
  assert.equal(await as('master_ops', `select ops.set_master_cutover_phase('frozen', 'x', '{}'::jsonb)`), 'P0001');
  assert.equal(await as('master_ops', "update ops.master_cutover_state set note = 'x'"), '42501');
  assert.equal(await as('master_ops', 'select 1 from core.skus limit 1'), '42501');
  assert.equal(await as('master_ops', 'select phase from ops.master_cutover_state'), 'ok');
  const roles = await q("select rolname, rolsuper, rolcreaterole, rolinherit, rolcanlogin from pg_roles where rolname like 'master\\_%' order by 1");
  assert.deepEqual(roles.map((r) => [r.rolname, r.rolsuper, r.rolcreaterole, r.rolinherit, r.rolcanlogin]),
    ['master_edit', 'master_gate', 'master_observer', 'master_ops'].map((n) => [n, false, false, false, true]));
  // 変更の記録は画面のロールで保存しても db_user = master_edit (security definer の関数でも呼び手を残す)
  assert.equal(Number((await q("select count(*)::int as n from events.master_change_events where source_system = 'portal_master_edit' and request_id in (select request_id::text from ops.master_edit_requests) and db_user <> 'master_edit'"))[0].n), 0);
  assert.ok(Number((await q("select count(*)::int as n from events.master_change_events where db_user = 'master_edit'"))[0].n) > 10);
});

console.log('\n画面 (router)');

process.env.COMPANY_DB_URL = 'postgres://owner@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test, other@test';
process.env.MASTER_EDIT_OPEN = '1';
let factoryMode = 'ok';
const opened = [];
__setPgClientFactory(async (url) => {
  if (factoryMode === 'down') throw new Error('connect ECONNREFUSED');
  const role = /master_edit@/.test(url) ? 'master_edit' : 'deploy';
  opened.push(role);
  await pg.query(`set role ${role}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
__setClock(() => NOW.getTime());
__setOwnership(ALL_COMPANY);
__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'editor' ? { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }
    : s === 'admin' ? { authenticated: true, email: 'admin@test', role: 'admin', allowedApps: '*' } : null;
  next();
});
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-edit`;
async function call(method, url, { body, session = 'editor', origin = true, ctype = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined && ctype) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
/** 画面の JS が文法として読めること (描画の試験は通っても、画面の JS が壊れていることがある) と、EJS の出力が JS の中に混ざっていないこと */
function checkScripts(html, expected) {
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((x) => x[1]);
  assert.equal(scripts.length, expected, `<script> の数 ${scripts.length}`);
  for (const s of scripts) { new vm.Script(s); assert.ok(!/<%|%>/.test(s), 'EJS のタグが JS に残っている'); }
  return scripts;
}
const tokenIn = (html) => /data-token="([0-9a-f]{64})"/.exec(html)?.[1];
const eventIn = (html) => /data-event-id="(\d+)"/.exec(html)?.[1];

await ta('[15] 一覧: 描画・検索・区分・状態・未入力 (売上分類はセットを導いてから)・導いた値の * ・末尾の /・つかいかた (画面のロールで読む)', async () => {
  opened.length = 0;
  let r = await call('GET', '/');
  assert.equal(r.status, 200); assert.match(r.text, /マスタの入力/);
  assert.deepEqual(opened, ['master_edit']);
  checkScripts(r.text, 0);
  assert.match(r.text, /href="sku\/set001"/);
  assert.match(r.text, /10\*/);
  assert.ok(!/いまは保存できません/.test(r.text));
  r = await call('GET', '/?q=S00&kind=single');
  assert.ok(r.text.includes('sku/s001') && !r.text.includes('sku/set001'));
  r = await call('GET', '/?q=' + encodeURIComponent('セット 5'));
  assert.ok(r.text.includes('sku/set005') && !r.text.includes('sku/s001"'));
  await pg.query("update core.products set sales_class = null where product_id = (select product_id from core.skus where code = 's003')");
  assert.deepEqual((await R.listSkus(db, { missing: 'sales' }, { now: NOW })).rows.map((x) => x.code), ['s003', 's006', 'set001']);
  await pg.query("update core.products set sales_class = 1 where product_id = (select product_id from core.skus where code = 's003')");
  assert.deepEqual((await R.listSkus(db, { kind: 'set', state: 'available' }, { now: NOW })).rows.map((x) => [x.code, x.tax_derived]), [['set001', true]]);
  assert.deepEqual((await R.listSkus(db, { kind: 'set', state: 'discontinued' }, { now: NOW })).rows.map((x) => x.code), ['set004', 'set005', 'set006']);
  const bare = await fetch(`${ORIGIN}/apps/master-edit`, { headers: { 'x-test-session': 'editor' }, redirect: 'manual' });
  assert.equal(bare.status, 301); assert.equal(bare.headers.get('location'), '/apps/master-edit/');
  const m = await call('GET', '/manual');
  assert.equal(m.status, 200);
  for (const word of ['保存', '+ 構成品', '表示し直す', '画面を開き直す', '例外原価をやめる (構成品の合計に戻す)', 'NEとの差あり', '未入力', '切替前', 'NE でやること']) assert.ok(m.text.includes(word), `つかいかたに「${word}」が無い`);
});

await ta('[15] 単品・セットの画面: 描画・画面の JS・編集の印・導く値・食い違い・JAN とロジザードは単品だけ・404', async () => {
  let r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200);
  const scripts = checkScripts(r.text, 1);
  for (const api of ["'/api/sku/'", "'/api/lookup?code='"]) assert.ok(scripts[0].includes(api), `画面が ${api} を呼んでいない`);
  assert.equal(tokenIn(r.text), await tokenOf('s001'));
  assert.match(r.text, /data-can-save="1"/);
  assert.match(r.text, /<label class="k">JAN<\/label>/); assert.match(r.text, /ロジザードが正/);
  await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 2 }] });
  await promote((await observe([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }] }], { at: new Date(Date.now() + 240000).toISOString() })).set001);
  r = await call('GET', '/sku/set001');
  checkScripts(r.text, 1);
  assert.ok(!/<label class="k">JAN<\/label>/.test(r.text) && !/ロジザードが正/.test(r.text), 'セットに JAN・ロジザードの欄を出さない');
  assert.match(r.text, /導く値 \(今の構成/); assert.match(r.text, /NE でやること \(構成の依頼\)/); assert.match(r.text, /導く値 \(依頼の構成\)/);
  assert.match(r.text, /NE でやること \(食い違い: NE の構成が依頼と違う\)/);
  const hist = await call('GET', '/sku/set001/history');
  assert.equal(hist.status, 200); assert.match(hist.text, /構成の依頼/); assert.match(hist.text, /マスタの入力/);
  assert.equal((await call('GET', '/sku/nope')).status, 404);
  assert.equal((await call('GET', '/sku/nope/history')).status, 404);
  const lk = await call('GET', '/api/lookup?code=S002');
  assert.deepEqual([lk.status, lk.j.item.code, lk.j.item.kind], [200, 's002', 'single']);
  assert.equal((await call('GET', '/api/lookup?code=nope')).status, 404);
});

await ta('[15] 保存の API: 画面と同じ形で通る (画面のロールで書く)・名簿・Origin・Content-Type・押し直し・印の違い', async () => {
  const page = await call('GET', '/sku/s002');
  const body = { request_id: uuid(), reason: '画面から', seen: { token: tokenIn(page.text), event_id: eventIn(page.text) }, values: { name: '画面から直した', reorder_months: '2' } };
  assert.equal((await call('POST', '/api/sku/s002', { body, session: 'admin' })).status, 403);
  const keep = process.env.MASTER_EDITORS;
  process.env.MASTER_EDITORS = ' , ';
  const none = await call('POST', '/api/sku/s002', { body });
  assert.equal(none.status, 403); assert.match(none.j.error, /誰も保存できません/);
  process.env.MASTER_EDITORS = keep;
  assert.equal((await call('POST', '/api/sku/s002', { body, origin: false })).j.error, 'origin_mismatch');
  assert.equal((await call('POST', '/api/sku/s002', { body, ctype: false })).status, 415);
  opened.length = 0;
  const ok = await call('POST', '/api/sku/s002', { body });
  assert.equal(ok.status, 200, ok.text);
  assert.deepEqual(opened, ['master_edit']);
  assert.deepEqual(ok.j.changed.map((c) => c.field), ['name']);
  assert.equal((await skuRow('s002')).name, '画面から直した');
  assert.equal((await call('POST', '/api/sku/s002', { body })).j.replayed, true);
  const conflict = await call('POST', '/api/sku/s002', { body: { ...body, request_id: uuid(), values: { name: 'もう一度' } } });
  assert.deepEqual([conflict.status, conflict.j.reason], [409, 'version_conflict']);
  assert.ok(Array.isArray(conflict.j.events) && conflict.j.events.length > 0);
  assert.equal((await call('POST', '/api/sku/s002', { body: { ...body, request_id: uuid(), values: { tax_rate: '5' } } })).status, 400);
});

await ta('[15] 保存を開いていない (MASTER_EDIT_OPEN なし / 持ち主表が記録と違う) = 帯・欄と保存のボタンが閉じる・API は 409', async () => {
  delete process.env.MASTER_EDIT_OPEN;
  let r = await call('GET', '/sku/s001');
  assert.match(r.text, /いまは保存できません \(保存を開くスイッチ/);
  assert.match(r.text, /data-can-save="0"/); assert.match(r.text, /id="save" disabled/);
  assert.match(r.text, /data-field="name" value="[^"]*" size="60" disabled/);
  assert.match(r.text, /<span class="tag">切替前<\/span>/);
  const body = { request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) }, values: { name: 'x' } };
  assert.deepEqual([(await call('POST', '/api/sku/s001', { body })).status], [409]);
  process.env.MASTER_EDIT_OPEN = '1';
  __setOwnership(null);   // 本番の持ち主表 (全部 load) = 切替のときの記録と違う
  r = await call('GET', '/sku/s001');
  assert.match(r.text, /いまは保存できません \(持ち主表が切替のときの記録と違う\)/); assert.match(r.text, /data-can-save="0"/);
  const res2 = await call('POST', '/api/sku/s001', { body: { ...body, request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) } } });
  assert.deepEqual([res2.status, res2.j.reason], [409, 'before_cutover']);
  __setOwnership(ALL_COMPANY);
  assert.ok(!/いまは保存できません/.test((await call('GET', '/sku/s001')).text));
});

await ta('[15] 書き込み用の接続 (COMPANY_DB_MASTER_EDIT_URL) が無い = 持ち主のロールで読むだけ・保存のボタンなし・API は 503', async () => {
  const keep = process.env.COMPANY_DB_MASTER_EDIT_URL;
  delete process.env.COMPANY_DB_MASTER_EDIT_URL;
  opened.length = 0;
  const r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200); assert.deepEqual(opened, ['deploy']);
  assert.match(r.text, /いまは保存できません \(書き込み用の接続/); assert.match(r.text, /data-can-save="0"/);
  const res = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tokenIn(r.text) }, values: { name: 'x' } } });
  assert.deepEqual([res.status, res.j.reason], [503, 'no_write_role']);
  process.env.COMPANY_DB_MASTER_EDIT_URL = keep;
});

await ta('[15] Company DB が無い・届かない = 画面は帯 (保存のボタンなし)・API は 503', async () => {
  factoryMode = 'down';
  let r = await call('GET', '/');
  assert.equal(r.status, 200); assert.match(r.text, /Company DB につながりません/);
  r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200); assert.match(r.text, /Company DB につながりません/); assert.ok(!/id="save"/.test(r.text));
  const res = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: 'a'.repeat(64) }, values: { name: 'x' } } });
  assert.deepEqual([res.status, res.j.reason], [503, 'db_unreachable']);
  factoryMode = 'ok';
  const keep = [process.env.COMPANY_DB_URL, process.env.COMPANY_DB_MASTER_EDIT_URL];
  delete process.env.COMPANY_DB_URL; delete process.env.COMPANY_DB_MASTER_EDIT_URL;
  r = await call('GET', '/');
  assert.match(r.text, /COMPANY_DB_URL/);
  assert.equal((await call('GET', '/api/lookup?code=s001')).status, 503);
  [process.env.COMPANY_DB_URL, process.env.COMPANY_DB_MASTER_EDIT_URL] = keep;
});

await ta('[15] server.js: Render だけ (env + PORTAL_VARIANT)・requireAppAccess・共通の JSON parser を通さない・アプリ一覧に載る', async () => {
  const s = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  assert.match(s, /if \(process\.env\.MASTER_EDIT_ENABLED === '1' && PORTAL_VARIANT === 'render'\) \{\r?\n\s+app\.use\('\/apps\/master-edit', requireAppAccess\('master-edit'\), masterEditRouter\);/);
  assert.equal((s.match(/masterEditRouter/g) || []).length, 2);
  const skip = s.indexOf("if (normalizedPath.toLowerCase().startsWith('/apps/master-edit')) return next();");
  assert.ok(skip > 0 && skip < s.indexOf('return globalJsonParser(req, res, next);'), '共通の JSON parser の除外に master-edit が無い');
  const { apps } = await import('../lib/portal-apps.js');
  assert.equal(apps.find((a) => a.id === 'master-edit')?.path, '/apps/master-edit/');
});

server.close();
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
