/**
 * test-master-edit.mjs — マスタの入力 (apps/master-edit・lib/master-write.mjs・lib/master-cutover.mjs・0050。Company DB構想 14 §6 ⑤-1 / Codex ⑤-R0・R1)
 *
 * Company DB = PGlite (Render と同じ条件の持ち主のロール deploy で migration)。本物の router を HTTP 越しにも通す (セッションは x-test-session で模擬)
 * 固定する契約:
 *   1 切替の段階: legacy_open → frozen → company_owner → new_open の一方向・1 段ずつ (飛ばさない・戻さない・直接は書けない・記録が残る)・読めない = 閉じている
 *   2 保存を開く = 段階 new_open **かつ** 持ち主表の列が 'company' **かつ** MASTER_EDIT_OPEN (opts.open)。どれかが欠ければ 409 切替前・何も書かない (失敗の記録は残る)
 *   3 単品の保存: 変わった列だけ・商品の行 (名前・取扱) もそろう・代表 (親) は manual・代表の仕入先の付け替え・変更の記録の actor / source / request_id / reason
 *   4 保存した値は夜間ロードを 2 回流しても残る (持ち主 company)。持ち主を load に戻すと夜間ロードが戻す (= 守っているのは持ち主表)
 *   5 同じ request_id: 同じ中身 = 前の結果 (記録は増えない) / 違う中身・違う人 = 409 / 失敗 = 同じ誤り。記録は保存と同じ取引 (処理中の行は作らない)・追記だけ
 *   6 編集の印: SKU・商品・仕入先ごとの商品・原価・JAN・構成品・構成の依頼・含むセットのどれかの行が変わった / 増えた = 409 とその間の変更・何も書かない
 *   7 入力の検証 (名前・税率・売価・分類・月数・原価・構成の規則・種類に無い項目・番号・編集の印)
 *   8 単品の税率・取扱・原価を変えると、含むセットの導く値を同じ取引で計算し直す (記録も同じ request_id)
 *   9 原価は今日だけ (東京の日付・取引の初めに 1 回): 今の行を昨日で閉じる・今日 2 回目は入れ替え・先の日付の行があれば入れない。期間の重なりは DB が拒む (夜間ロードは見ない)
 *  10 NE に取り込む CSV が出ている列 (作った・確かめた / 申告して確かめ待ち) は変えない (409)。void・確かめ済み・CSV に無い列は変えられる
 *  11 セットの構成 = 依頼だけ (core.sku_components は書かない)。置き換え・取り下げ (構成品・数量・並びまで同じとき)・並べ替えも依頼。
 *     依頼を上げる = NE の観測が完全・依頼より後・構成品 / 数量 / 並び / 行の数まで同じときだけ (同じ取引で構成を上げ・導く値を計算し直し・依頼を閉じる)
 *  12 セットの導く値が決まらない = 保存しない (上書き・例外原価で通る・税率は通らない)・導けるのに上書き = 400・例外原価をやめる = 今日から合計
 *  13 画面: 一覧・単品・セット・変更の記録・つかいかた・404 の描画と画面の JS / 名簿・Origin・Content-Type / Company DB が無い・届かない = 帯と 503 /
 *     保存を開いていない = 切替前の帯と欄が閉じている / server.js は Render だけ・共通の JSON parser を通さない
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

// ── Company DB (Render と同じ = 持ち主のロールで) ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
const q = async (sql, params) => (await db.query(sql, params)).rows;

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const withOwn = (over) => ({ ...MASTER_OWNERSHIP, ...over });
const LOAD_NOW = new Date('2030-01-05T03:00:00Z');   // 夜間ロードの日 (東京 2030-01-05)
const NOW = new Date('2030-01-10T03:00:00Z');        // 画面の今日 (東京 2030-01-10)
const TODAY = '2030-01-10';
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210.4 }], ['S02', { method: '宅急便', cost: 520 }]]);

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
      sku('set001', 'セット 1', 'set', 0.08, null, 400, { taxClass: 'MIXED' }),
      sku('set004', 'セット 4 (導けない)', 'set', 0.1, null, null),
      sku('set005', 'セット 5 (税率が決まらない)', 'set', null, null, 180),
    ],
    variationGroups: [{ code: 'grp1', name: '名札', childCodes: ['s001', 's003'], status: 'active' }],
    setComponents: [
      { parentCode: 'set001', childCode: 's001', qty: 2, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' },
      { parentCode: 'set004', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set004', childCode: 's004', qty: 1, source: 'ne' },
      { parentCode: 'set005', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set005', childCode: 's005', qty: 1, source: 'ne' },
    ],
    listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC', orderMethod: 'fax', leadTimeDays: 10 }, { code: '0002', name: 'ビーフリー', orderMethod: 'email', leadTimeDays: 5 }, { code: '0003', name: '止めた仕入先' }],
    supplierSkus: [{ supplierCode: '0001', skuCode: 's001', vendorCode: 'AMC-001' }],
    primarySuppliers: [{ skuCode: 's001', supplierCode: '0001' }],
    reorder: { available: true, runId: 'pml_test' },
  };
}
const load = async (ownership) => {
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: `load_${crypto.randomBytes(3).toString('hex')}`, ownership, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  return r;
};
await load(MASTER_OWNERSHIP);
await pg.query("update core.suppliers set active = false where code = '0003'");

const skuId = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id;
const cur = async (code) => W.readCurrent(db, await skuId(code), TODAY);
const tokenOf = async (code) => W.editTokenOf(await cur(code));
const lastEvent = async () => (await q('select coalesce(max(event_id), 0)::text as id from events.master_change_events'))[0].id;
const nEvents = async () => Number((await q('select count(*)::int as n from events.master_change_events'))[0].n);
const uuid = () => crypto.randomUUID();
/** 画面と同じ形で保存 (seen は今の値から) */
async function save(code, values, { ownership = ALL_COMPANY, open = true, requestId = uuid(), reason = 'テスト', actor = 'Naka@Test', token, eventId, shippingRates = RATES, now = NOW } = {}) {
  const seen = { token: token ?? await tokenOf(code), event_id: eventId ?? await lastEvent() };
  return W.saveSku(db, { actor, requestId, code, reason, seen, values }, { ownership, open, now, shippingRates });
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

console.log('切替の段階');

await ta('[1] 最初は legacy_open。1 段ずつ一方向 (飛ばす・戻す・知らない段階・人なし = 拒む)・直接の UPDATE は拒む・読めない = 閉じている', async () => {
  let s = await C.readCutoverPhase(db);
  assert.deepEqual([s.readable, s.phase], [true, 'legacy_open']);
  assert.equal(C.newEntryWritable(s), false); assert.equal(C.legacyWritable(s), true);
  await assert.rejects(() => C.advanceCutoverPhase(db, { to: 'company_owner', actor: 'naka@test' }), /one_way/);
  await assert.rejects(() => pg.query("select ops.set_master_cutover_phase('frozen', '')"), /actor/);
  await assert.rejects(() => C.advanceCutoverPhase(db, { to: 'open', actor: 'naka@test' }), /知らない段階/);
  await assert.rejects(() => pg.query("update ops.master_cutover_state set phase = 'new_open'"), /set_master_cutover_phase/);
  await assert.rejects(() => pg.query('delete from ops.master_cutover_state'), /消さない/);
  // 読めない = 閉じている (新しい画面も古い入口も書けない側)
  const broken = { query: async () => { throw new Error('connection terminated'); } };
  s = await C.readCutoverPhase(broken);
  assert.deepEqual([s.readable, s.phase], [false, null]);
  assert.equal(C.newEntryWritable(s), false); assert.equal(C.legacyWritable(s), false);
  assert.equal(C.newEntryWritable({ readable: true, phase: 'bogus' }), false);
});

await ta('[2] 段階が new_open でない = 持ち主 company + MASTER_EDIT_OPEN でも 409 切替前・何も書かない・失敗の記録が残る', async () => {
  const before = await nEvents();
  const id = uuid();
  const e = await rejectsWith(save('s001', { name: '直した名前' }, { requestId: id }), 409, 'before_cutover');
  assert.equal(e.extra.phase, 'legacy_open');
  assert.match(e.message, /切替の段階が legacy_open/);
  assert.equal((await skuRow('s001')).name, '単品 1');
  assert.equal(await nEvents(), before);
  const r = await reqRow(id);
  assert.equal(r.status, 'failed'); assert.equal(r.error.reason, 'before_cutover'); assert.equal(r.target_code, 's001'); assert.ok(r.sku_id);
  // 段階を進める (切替日の手順の順)。記録が残る
  for (const to of ['frozen', 'company_owner', 'new_open']) await C.advanceCutoverPhase(db, { to, actor: 'naka@test', note: `試験: ${to}` });
  assert.deepEqual((await q('select from_phase, to_phase, actor from ops.master_cutover_events order by event_id')).map((x) => [x.from_phase, x.to_phase, x.actor]),
    [['legacy_open', 'frozen', 'naka@test'], ['frozen', 'company_owner', 'naka@test'], ['company_owner', 'new_open', 'naka@test']]);
  await assert.rejects(() => C.advanceCutoverPhase(db, { to: 'legacy_open', actor: 'naka@test' }), /one_way/);
  assert.equal(C.newEntryWritable(await C.readCutoverPhase(db)), true);
  await assert.rejects(() => pg.query('delete from ops.master_cutover_events'), /append-only/);
});

await ta('[2] 段階 new_open でも、持ち主が load (今の本番の持ち主表) = 409 / MASTER_EDIT_OPEN が無い = 409', async () => {
  let e = await rejectsWith(save('s001', { name: '直した名前' }, { ownership: MASTER_OWNERSHIP }), 409, 'before_cutover');
  assert.deepEqual(e.extra.fields, ['名前']);
  assert.deepEqual(e.extra.load_keys.sort(), ['products.name', 'skus.name']);
  e = await rejectsWith(save('s001', { name: '直した名前' }, { open: false }), 409, 'before_cutover');
  assert.equal(e.extra.open, false); assert.match(e.message, /保存はまだ開いていません/);
  assert.equal((await skuRow('s001')).name, '単品 1');
});

await ta('[2] 一部だけ company: 名前 (company) + 税率 (load) を一緒に保存 = 全部断る。名前だけなら通る。変わる項目が無い = 変わりなし', async () => {
  const own = withOwn({ 'skus.name': 'company', 'products.name': 'company' });
  const e = await rejectsWith(save('s002', { name: '名前 2 改', tax_rate: '10' }, { ownership: own }), 409, 'before_cutover');
  assert.deepEqual(e.extra.fields, ['税率']);
  assert.equal((await skuRow('s002')).name, '単品 2');
  const r = await save('s002', { name: '名前 2 改', tax_rate: '8' }, { ownership: own });   // 税率は今と同じ = 変わらない列は見ない
  assert.deepEqual(r.changed.map((c) => c.field), ['name']);
  assert.equal((await skuRow('s002')).name, '名前 2 改');
  const before = await nEvents();
  assert.equal((await save('s001', { name: '単品 1', tax_rate: '0.1' }, { ownership: MASTER_OWNERSHIP })).no_change, true);
  assert.equal(await nEvents(), before);
});

console.log('\n単品の保存');

await ta('[3] 単品: 名前・取扱・売価・税率・分類・送料・月数・代表の仕入先・代表 (親) を 1 回で。変わった列だけ・商品の行もそろう', async () => {
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
  const ev = await q('select entity_type, attribute, actor_type, actor_id, source_system, reason_text from events.master_change_events where request_id = $1 order by event_id', [id]);
  assert.ok(ev.length >= 4, JSON.stringify(ev));
  for (const e of ev) assert.deepEqual([e.actor_type, e.actor_id, e.source_system, e.reason_text], ['human', 'other@test', 'portal_master_edit', '取扱をやめた']);
  assert.deepEqual(ev.filter((e) => e.entity_type === 'product').map((e) => e.attribute).sort(), ['name', 'status']);
  const s002 = await skuId('s002');
  const skuEv = await q("select attribute from events.master_change_events where request_id = $1 and entity_type = 'sku' and entity_id = $2", [id, s002]);
  assert.deepEqual(skuEv.map((e) => e.attribute).sort(), ['handling', 'name']);
  assert.equal((await q("select current_setting('core.actor_type', true) as a"))[0].a || '', '');
});

await ta('[4] 保存した値は夜間ロード (持ち主 company) を 2 回流しても残る。load に戻すと夜間ロードが戻す', async () => {
  const snap = async () => ({ s003: await skuRow('s003'), s002: await skuRow('s002'), prim: await primaryOf('s003') });
  const before = await snap();
  await load(ALL_COMPANY); await load(ALL_COMPANY);
  assert.deepEqual(await snap(), before);
  await load(MASTER_OWNERSHIP);   // 持ち主 load の夜間ロードは NE の値に戻す (守っているのは持ち主表)
  assert.equal((await skuRow('s003')).name, '単品 3');
  assert.equal((await skuRow('s003')).tax_rate, 0.1);
  assert.equal((await skuRow('s003')).parent, 's001');   // 人が決めた親 (manual) は load でも触らない (0036)
  await save('s003', { name: '単品 3 改', tax_rate: '8' });   // 以降の試験の前提
});

console.log('\n同じ request_id');

await ta('[5] 同じ中身 = 前の結果 (記録は増えない) / 違う中身・違う人 = 409 / 失敗 = 同じ誤り / 記録は追記だけ', async () => {
  const id = uuid();
  const token = await tokenOf('s004');
  const eventId = await lastEvent();   // 画面は同じ本文を送り直す (編集の印・見た記録の番号も同じ)
  const first = await save('s004', { sales_class: '3' }, { requestId: id, token, eventId });
  const n = await nEvents();
  const again = await save('s004', { sales_class: '3' }, { requestId: id, token, eventId });
  assert.equal(again.replayed, true);
  assert.deepEqual(again.changed, first.changed);
  assert.equal(await nEvents(), n);
  await rejectsWith(save('s004', { sales_class: '2' }, { requestId: id, token, eventId }), 409, 'request_id_reused');
  await rejectsWith(save('s004', { sales_class: '3' }, { requestId: id, token, eventId, actor: 'other@test' }), 409, 'request_id_reused');
  const bad = uuid();
  const t2 = await tokenOf('s004'); const e2 = await lastEvent();
  await rejectsWith(save('s004', { sales_class: '1' }, { requestId: bad, token: t2, eventId: e2, ownership: MASTER_OWNERSHIP }), 409, 'before_cutover');
  const e = await rejectsWith(save('s004', { sales_class: '1' }, { requestId: bad, token: t2, eventId: e2, ownership: MASTER_OWNERSHIP }), 409, 'before_cutover');
  assert.equal(e.extra.replayed, true);
  assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [bad]))[0].n), 1);
  // 処理中の行は作らない (done / failed だけ)・追記だけ
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
  const e = await rejectsWith(W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 's004', reason: null, seen: { token, event_id: since }, values: { name: '上書き' } },
    { ownership: ALL_COMPANY, open: true, now: NOW }), 409, 'version_conflict');
  assert.ok(e.extra.events.some((x) => x.attribute === 'standard_price_jpy' && x.actor_id === 'other@test'), JSON.stringify(e.extra.events));
  assert.equal(await nEvents(), n);
  assert.equal((await skuRow('s004')).name, '単品 4 (分類・原価なし)');
});

await ta('[6] 行が増えた・変わった (仕入先ごとの商品・原価の行・JAN・商品の行・構成品の税率・含むセット) でも編集の印は変わる', async () => {
  const s001 = await skuId('s001');
  const pid = (await q('select product_id::text as id from core.skus where code = $1', ['s001']))[0].id;
  const steps = [
    ["update core.supplier_skus set vendor_code = 'AMC-XXX' where sku_id = $1", [s001]],
    ["insert into core.supplier_skus (company_id, supplier_id, sku_id) select 1, supplier_id, $1 from core.suppliers where code = '0002'", [s001]],   // 行が増えた (phantom)
    ["insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4900000000001', 'manual', 'human')", [pid]],
    ["update core.products set sales_class = 1 where product_id = $1", [pid]],
  ];
  for (const [sql, params] of steps) {
    const t0 = await tokenOf('s001');
    await pg.query(sql, params);
    assert.notEqual(await tokenOf('s001'), t0, sql);
    await rejectsWith(save('s001', { name: '変える' }, { token: t0 }), 409, 'version_conflict');
  }
  await pg.query('update core.products set sales_class = 3 where product_id = $1', [pid]);
  // 構成品の値が変わった = セットの印が変わる / 含むセットが変わった = 単品の印が変わる
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
  assert.deepEqual(r.derived.filter((d) => d.col === 'handling').map((d) => d.code).sort(), ['set001', 'set004', 'set005']);
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
  // set001 = s001×2 + s002×1 = 260 + 200 / set005 = s001 + s005 = 130 + 80 / set004 は s004 に原価が無い = 合計できない (行も無いので何もしない)
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

await ta('[9] 期間の重なりは DB が拒む (この画面・昇格の書き込み)。夜間ロード・ほかの書き手は見ない (今の動きを止めない)', async () => {
  const sid = await skuId('s003');
  const asPortal = async (sql) => {
    await pg.query('begin');
    try { await pg.query("select set_config('core.source_system', 'portal_master_edit', true)"); await pg.query(sql, [sid]); await pg.query('commit'); }
    catch (e) { await pg.query('rollback'); throw e; }
  };
  await assert.rejects(() => asPortal("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 1, 'manual', 'COMPLETE', '2030-01-06', '2030-01-07')"), /sku_cost_overlap/);
  // 重ならない前の期間は入る (両端を含む: 前の行の終わり 1/4 と今の行の始まり 1/5 は重ならない)
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
  // 日をまたいだ保存: 東京の 1/11 00:00 (UTC 1/10 15:00) の保存は 1/11 から
  await save('s005', { cost: { jpy: '81', reason: '日の境目' } }, { now: new Date('2030-01-10T15:00:00Z') });
  assert.deepEqual((await costsOf('s005')).map((c) => [c.jpy, c.f, c.t]), [[80, '2030-01-05', '2030-01-10'], [81, '2030-01-11', null]]);
});

console.log('\nNE に取り込む CSV');

await ta('[10] CSV が出ている列は変えない (作った・確かめた / 申告して確かめ待ち)。CSV に無い列・void・確かめ済みは変えられる', async () => {
  const mk = async (state) => {
    const extra = state === 'declared' ? ', checked_at, checked_run, declared_at, declared_by' : state === 'void' ? ', void_at, void_by' : '';
    const vals = state === 'declared' ? ", now(), 'mc_20300101T000000000Z_abcdef', now(), 'x@test'" : state === 'void' ? ", now(), 'x@test'" : '';
    return (await q(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by, state${extra})
      values ('products', 'name', 'syohin_name', 'v1', 'utf8', true, 1, repeat('a', 64), decode('00', 'hex'), 'mc_20300101T000000000Z_abcdef', 'x@test', '${state}'${vals}) returning export_id::text as id`))[0].id;
  };
  const row = (id, code, col, reserved = true) => pg.query(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell, cdb_version, evidence, reserved, released_at, release_reason)
    values ($1, 'to_ne', $2, $3, $2, '{"value":"a"}', 'a', 1, '{}', $4, ${reserved ? 'null' : 'now()'}, ${reserved ? 'null' : "'confirmed'"})`, [id, code, col, reserved]);
  const made = await mk('made');
  await row(made, 's003', 'name');
  const monthsBefore = (await skuRow('s003')).months;
  let e = await rejectsWith(save('s003', { name: '直したい', reorder_months: '3' }), 409, 'csv_issued');
  assert.match(e.message, new RegExp(`#${made}`));
  assert.equal((await skuRow('s003')).months, monthsBefore);
  await save('s003', { reorder_months: '3' });   // CSV に無い列は変えられる
  await pg.query("update ops.ne_csv_exports set state = 'void', void_at = now(), void_by = 'x@test', void_reason = 'by_user' where export_id = $1", [made]);
  await pg.query('update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = $2 where export_id = $1', [made, 'void']);
  await save('s003', { name: '単品 3 改 2' });   // 人が「使わない」にした = 変えられる
  const declared = await mk('declared');
  await row(declared, 's003', 'tax_rate', true);
  e = await rejectsWith(save('s003', { tax_rate: '10' }), 409, 'csv_issued');
  assert.deepEqual(e.extra.exports.map((x) => [x.col, x.state]), [['tax_rate', 'declared']]);
  await pg.query("update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = 'confirmed' where export_id = $1", [declared]);
  await save('s003', { tax_rate: '10' });   // 確かめが終わった = 変えられる
  assert.equal((await skuRow('s003')).tax_rate, 0.1);
});

console.log('\nセット: 構成の依頼・上げる・導けない・例外原価');

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
  assert.deepEqual((await cur('set001')).component_request.rows.map((x) => x.code), ['s001', 's003']);
  await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 3 }] });
  assert.deepEqual((await q('select status, close_reason from ops.sku_component_requests order by component_request_id')).map((x) => [x.status, x.close_reason]), [['cancelled', 'superseded'], ['open', null]]);
  // 今の構成と同じ (構成品・数量・並び) に戻す = 取り下げ
  const w = await save('set001', { components: [{ code: 's001', qty: 2 }, { code: 's002', qty: 1 }] });
  assert.ok(w.ne_steps.some((s) => /取り下げ/.test(s)));
  assert.deepEqual((await q('select status, close_reason from ops.sku_component_requests order by component_request_id')).map((x) => [x.status, x.close_reason]), [['cancelled', 'superseded'], ['cancelled', 'withdrawn']]);
  // 並びだけ違う = 依頼 (並びも構成のうち)
  const re = await save('set001', { components: [{ code: 's002', qty: 1 }, { code: 's001', qty: 2 }] });
  assert.ok(re.ne_steps.some((s) => /並びを s002 → s001/.test(s)), JSON.stringify(re.ne_steps));
  await save('set001', { components: [{ code: 's001', qty: 2 }, { code: 's002', qty: 1 }] });   // 取り下げて元に
  await assert.rejects(() => pg.query(`update ops.sku_component_requests set rows = '[{"sku_id":1,"qty":1,"sort":1}]'::jsonb`), /書き換えない|閉じた依頼/);
  await assert.rejects(() => pg.query('delete from ops.sku_component_requests'), /消さない/);
});

await ta('[11] 依頼を上げる: NE の観測が完全・依頼より後・構成品 / 数量 / 並び / 行の数まで同じときだけ。同じ取引で構成・導く値・依頼を', async () => {
  await save('set001', { components: [{ code: 's003', qty: 2 }, { code: 's001', qty: 1 }] }, { reason: '入れ替え' });
  const reqAt = Date.parse((await q("select created_at::text as t from ops.sku_component_requests where status = 'open'"))[0].t);
  const after = new Date(Date.now() + 60000).toISOString();
  const ok = { set_code: 'set001', complete: true, observed_at: after, run_id: 'ne_run_1', rows: [{ code: 's003', qty: 2, sort: 10 }, { code: 'S001', qty: 1, sort: 20 }] };
  const before = await compsOf('set001');
  const tries = [
    [{ ...ok, complete: false }, 'incomplete_observation'],
    [{ ...ok, observed_at: new Date(reqAt - 1000).toISOString() }, 'stale_observation'],
    [{ ...ok, rows: [ok.rows[1], ok.rows[0]].map((r, i) => ({ ...r, sort: i + 1 })) }, 'mismatch'],   // 並びが違う
    [{ ...ok, rows: [...ok.rows, { code: 's002', qty: 1, sort: 30 }] }, 'mismatch'],                     // 行が多い
    [{ ...ok, rows: [ok.rows[0]] }, 'mismatch'],                                                          // 行が足りない
    [{ ...ok, rows: [{ ...ok.rows[0], qty: 3 }, ok.rows[1]] }, 'mismatch'],                               // 数量が違う
    [{ ...ok, rows: [ok.rows[0], { ...ok.rows[1], sort: 10 }] }, 'mismatch'],                             // 並びが決められない
    [{ ...ok, rows: [ok.rows[0], { code: 'nope', qty: 1, sort: 20 }] }, 'unknown_component'],
    [{ ...ok, set_code: 's001' }, 'not_a_set'],
  ];
  for (const [obs, why] of tries) {
    const r = await W.promoteComponentRequest(db, obs, { ownership: ALL_COMPANY, now: NOW });
    assert.deepEqual([r.promoted, r.reason], [false, why], JSON.stringify(obs));
  }
  assert.equal((await W.promoteComponentRequest(db, ok, { ownership: MASTER_OWNERSHIP, now: NOW })).reason, 'before_cutover');
  assert.deepEqual(await compsOf('set001'), before);
  assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_requests where status = 'open'"))[0].n), 1);
  // 同じ = 上げる
  const r = await W.promoteComponentRequest(db, ok, { ownership: ALL_COMPANY, now: NOW });
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.deepEqual(await compsOf('set001'), [['s003', 2, 1, 'ne'], ['s001', 1, 2, 'ne']]);
  assert.deepEqual((await q("select status, close_reason, closed_by, applied_run from ops.sku_component_requests where set_sku_id = (select sku_id from core.skus where code = 'set001') order by component_request_id desc limit 1"))[0],
    { status: 'applied', close_reason: 'matched', closed_by: 'system', applied_run: 'ne_run_1' });
  // 導く値も同じ取引で (s003 税 10%・s001 10% / 原価 s003 50×2 + s001 120 = 220)
  assert.ok(r.derived.some((d) => d.col === 'cost' && d.to === 220), JSON.stringify(r.derived));
  assert.equal((await costsOf('set001')).at(-1).jpy, 220);
  const ev = await q("select distinct actor_type, source_system, run_id from events.master_change_events where source_system = 'ne_observation'");
  assert.deepEqual(ev, [{ actor_type: 'system', source_system: 'ne_observation', run_id: 'ne_run_1' }]);
  // もう開いている依頼は無い = もう一度は上げない
  assert.equal((await W.promoteComponentRequest(db, ok, { ownership: ALL_COMPANY, now: NOW })).reason, 'no_open_request');
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

await ta('[12] 例外原価をやめる = 今日の例外の行を消して、今日から構成品の合計 (合計できなければ保存しない)', async () => {
  const e = await rejectsWith(save('set004', { exception_cost: { clear: true, reason: 'やめる' } }), 400, 'set_underivable');
  assert.match(e.message, /原価/);
  await save('s004', { cost: { jpy: '70', reason: '入れた' } });
  const r = await save('set004', { exception_cost: { clear: true, reason: 'やめる' } });
  assert.deepEqual(r.derived.map((d) => [d.col, d.to]), [['cost', 190]]);   // s001 120 + s004 70
  assert.deepEqual((await costsOf('set004')).map((c) => [c.jpy, c.src, c.f, c.t]), [[190, 'set_calc', TODAY, null]]);
});

console.log('\n画面 (router)');

process.env.COMPANY_DB_URL = 'postgres://test@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test, other@test';
process.env.MASTER_EDIT_OPEN = '1';
let factoryMode = 'ok';
__setPgClientFactory(async () => {
  if (factoryMode === 'down') throw new Error('connect ECONNREFUSED');
  return { query: (t, p) => pg.query(t, p), end: async () => {}, on: () => {} };
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

await ta('[13] 一覧: 描画・検索 (コード・名前)・区分・状態・未入力 (売上分類はセットを導いてから)・導いた値の * ・末尾の /・つかいかた', async () => {
  let r = await call('GET', '/');
  assert.equal(r.status, 200); assert.match(r.text, /マスタの入力/);
  checkScripts(r.text, 0);
  assert.match(r.text, /href="sku\/set001"/);
  assert.match(r.text, /10\*/);   // セットの税率に *
  assert.ok(!/切替前です/.test(r.text));
  r = await call('GET', '/?q=S00&kind=single');
  assert.ok(r.text.includes('sku/s001') && !r.text.includes('sku/set001'));
  r = await call('GET', '/?q=' + encodeURIComponent('セット 5'));
  assert.ok(r.text.includes('sku/set005') && !r.text.includes('sku/s001"'));
  await pg.query("update core.products set sales_class = null where product_id = (select product_id from core.skus where code = 's003')");
  assert.deepEqual((await R.listSkus(db, { missing: 'sales' }, { now: NOW })).rows.map((x) => x.code), ['s003', 'set001']);
  await pg.query("update core.products set sales_class = 1 where product_id = (select product_id from core.skus where code = 's003')");
  assert.deepEqual((await R.listSkus(db, { missing: 'sales' }, { now: NOW })).rows.map((x) => x.code), []);
  // set004・set005 は s001 を一度中止にしたとき一緒に中止になり、セット自身の取扱が決まっていないので中止のまま ([8])
  assert.deepEqual((await R.listSkus(db, { kind: 'set', state: 'available' }, { now: NOW })).rows.map((x) => [x.code, x.tax_derived]), [['set001', true]]);
  assert.deepEqual((await R.listSkus(db, { kind: 'set', state: 'discontinued' }, { now: NOW })).rows.map((x) => x.code), ['set004', 'set005']);
  const bare = await fetch(`${ORIGIN}/apps/master-edit`, { headers: { 'x-test-session': 'editor' }, redirect: 'manual' });
  assert.equal(bare.status, 301); assert.equal(bare.headers.get('location'), '/apps/master-edit/');
  const m = await call('GET', '/manual');
  assert.equal(m.status, 200);
  for (const word of ['保存', '+ 構成品', '表示し直す', '画面を開き直す', '例外原価をやめる (構成品の合計に戻す)', 'NEとの差あり', '未入力', '切替前', 'NE でやること']) assert.ok(m.text.includes(word), `つかいかたに「${word}」が無い`);
});

await ta('[13] 単品・セットの画面: 描画・画面の JS・編集の印・導く値 (今の構成と依頼の構成)・JAN とロジザードは単品だけ・404', async () => {
  let r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200);
  const scripts = checkScripts(r.text, 1);
  for (const api of ["'/api/sku/'", "'/api/lookup?code='"]) assert.ok(scripts[0].includes(api), `画面が ${api} を呼んでいない`);
  assert.equal(tokenIn(r.text), await tokenOf('s001'));
  assert.match(r.text, /data-can-save="1"/);
  assert.match(r.text, /<label class="k">JAN<\/label>/); assert.match(r.text, /ロジザードが正/);
  const pageSet = await call('GET', '/sku/set001');
  checkScripts(pageSet.text, 1);
  assert.ok(!/<label class="k">JAN<\/label>/.test(pageSet.text) && !/ロジザードが正/.test(pageSet.text), 'セットに JAN・ロジザードの欄を出さない');
  assert.match(pageSet.text, /導く値 \(今の構成/);
  await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's002', qty: 1 }] });
  r = await call('GET', '/sku/set001');
  assert.match(r.text, /NE でやること \(構成の依頼\)/); assert.match(r.text, /導く値 \(依頼の構成\)/);
  const hist = await call('GET', '/sku/set001/history');
  assert.equal(hist.status, 200); assert.match(hist.text, /構成の依頼/); assert.match(hist.text, /マスタの入力/);
  assert.equal((await call('GET', '/sku/nope')).status, 404);
  assert.equal((await call('GET', '/sku/nope/history')).status, 404);
  const lk = await call('GET', '/api/lookup?code=S002');
  assert.deepEqual([lk.status, lk.j.item.code, lk.j.item.kind], [200, 's002', 'single']);
  assert.equal((await call('GET', '/api/lookup?code=nope')).status, 404);
});

await ta('[13] 保存の API: 画面と同じ形で通る・名簿 (名簿に無い admin も不可・空なら誰も不可)・Origin・Content-Type・押し直し・印の違い', async () => {
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
  const ok = await call('POST', '/api/sku/s002', { body });
  assert.equal(ok.status, 200, ok.text);
  assert.deepEqual(ok.j.changed.map((c) => c.field), ['name']);
  assert.equal((await skuRow('s002')).name, '画面から直した');
  assert.equal((await call('POST', '/api/sku/s002', { body })).j.replayed, true);
  const conflict = await call('POST', '/api/sku/s002', { body: { ...body, request_id: uuid(), values: { name: 'もう一度' } } });
  assert.deepEqual([conflict.status, conflict.j.reason], [409, 'version_conflict']);
  assert.ok(Array.isArray(conflict.j.events) && conflict.j.events.length > 0);
  assert.equal((await call('POST', '/api/sku/s002', { body: { ...body, request_id: uuid(), values: { tax_rate: '5' } } })).status, 400);
});

await ta('[13] 保存を開いていない (MASTER_EDIT_OPEN なし / 持ち主が load) = 切替前の帯・欄は閉じる・API は 409', async () => {
  delete process.env.MASTER_EDIT_OPEN;
  let r = await call('GET', '/sku/s001');
  assert.match(r.text, /切替前です/);
  assert.match(r.text, /data-field="name" value="[^"]*" size="60" disabled/);
  assert.match(r.text, /<span class="tag">切替前<\/span>/);
  const body = { request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) }, values: { name: 'x' } };
  const res = await call('POST', '/api/sku/s001', { body });
  assert.deepEqual([res.status, res.j.reason], [409, 'before_cutover']);
  process.env.MASTER_EDIT_OPEN = '1';
  __setOwnership(null);   // 本番の持ち主表 (全部 load)
  r = await call('GET', '/sku/s001');
  assert.match(r.text, /切替前です/); assert.match(r.text, /size="60" disabled/);
  const res2 = await call('POST', '/api/sku/s001', { body: { ...body, request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) } } });
  assert.deepEqual([res2.status, res2.j.reason], [409, 'before_cutover']);
  __setOwnership(ALL_COMPANY);
  assert.ok(!/切替前です/.test((await call('GET', '/sku/s001')).text));
});

await ta('[13] Company DB が無い・届かない = 画面は帯 (保存のボタンなし)・API は 503', async () => {
  factoryMode = 'down';
  let r = await call('GET', '/');
  assert.equal(r.status, 200); assert.match(r.text, /Company DB につながりません/);
  r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200); assert.match(r.text, /Company DB につながりません/); assert.ok(!/id="save"/.test(r.text));
  const res = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: 'a'.repeat(64) }, values: { name: 'x' } } });
  assert.deepEqual([res.status, res.j.reason], [503, 'db_unreachable']);
  factoryMode = 'ok';
  const url = process.env.COMPANY_DB_URL; delete process.env.COMPANY_DB_URL;
  r = await call('GET', '/');
  assert.match(r.text, /COMPANY_DB_URL/);
  assert.equal((await call('GET', '/api/lookup?code=s001')).status, 503);
  process.env.COMPANY_DB_URL = url;
});

await ta('[13] server.js: Render だけ (env + PORTAL_VARIANT)・requireAppAccess・共通の JSON parser を通さない・アプリ一覧に載る', async () => {
  const s = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  assert.match(s, /if \(process\.env\.MASTER_EDIT_ENABLED === '1' && PORTAL_VARIANT === 'render'\) \{\r?\n\s+app\.use\('\/apps\/master-edit', requireAppAccess\('master-edit'\), masterEditRouter\);/);
  assert.equal((s.match(/masterEditRouter/g) || []).length, 2);   // import と mount だけ
  const skip = s.indexOf("if (normalizedPath.toLowerCase().startsWith('/apps/master-edit')) return next();");
  assert.ok(skip > 0 && skip < s.indexOf('return globalJsonParser(req, res, next);'), '共通の JSON parser の除外に master-edit が無い');
  const { apps } = await import('../lib/portal-apps.js');
  assert.equal(apps.find((a) => a.id === 'master-edit')?.path, '/apps/master-edit/');
});

server.close();
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
