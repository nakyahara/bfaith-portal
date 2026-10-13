/**
 * test-order-settings-outbox.mjs — 新商品の登録の「発注の設定の知らせ」(0075・lib/order-settings-outbox.mjs・AI_reference CompanyDB構想/21 v4 の C1)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 切替の後・products.parent も company・画面のロール master_edit の本物の権限)。
 * 発注アプリ = 一時の DATA_DIR の warehouse-mirror.db (本番の DB に触らない)。
 * 固定する契約:
 *   K 形と hash: 知らせの中身の形の決まり = JS (orderPayloadShapeProblem) と DB (ops.order_settings_payload_problem) が同じ答え・hash = DB の ops.js_stable_sha256
 *   E 積む: 単品・まとまりの登録と同じ取引 (要求のハッシュに入る = 違う発注の設定の押し直しは 409)・登録が巻き戻れば知らせも無い・
 *     取引の外 / ほかの人 / ほかの商品 / 形の違う中身 / 画面のロールの直接の insert・update = 断る・セットは無い・知らせは変えない / 消さない
 *   D 届ける: 発注アプリに書く (届けの ID で 1 回だけ = 結果を書けずに 2 回届けても 1 回分)・借りの間はほかが取らない・失敗 = 自動は 5 回まで / 夜間の段と人は続ける・
 *     ほかの画面が先に作っていた = 要確認 (上書きしない)・登録をやめた下書き = 届けずに閉じる・中身が書き換わった = 届けない・人が閉じる (failed / conflict だけ)
 *   N 夜間ロードの後ろの段: 届け直す・24 時間を超えた知らせ = 段の失敗 (コードを note に)・閉じたら戻る・Render の外 / 0075 の前 = 飛ばす
 *   G 権限: 画面のロールは表を読むだけ + 4 つの関数・PUBLIC は無し・見張りは読める・重い入口の棚卸し (heavy-entry-manifest) に漏れが無い
 *   R 画面 (master-edit): まとまりの登録の発注の設定 = 子ごとの知らせ → 届く / 届かない子の数と理由
 *   P 発注アプリの画面: 届いていない商品に「発注の設定がまだ届いていない」の印 (すすめる数は止めない = 決定 2 = b)・全部の画面の上の帯・読めなくても画面は出る
 * 使い方: node scripts/test-order-settings-outbox.mjs (DB もネットも使わない。PGlite と一時の SQLite)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const killer = setTimeout(() => { console.error('時間切れ (600 秒)'); process.exit(3); }, 600e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = await temporaryTestDataDir(import.meta.url, 'posync-oso-');
delete process.env.MASTER_EDIT_OPEN;
delete process.env.RENDER;

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const T = await setupVariationDb();
const { pg, db, one, asEditor } = T;
const R = await import('../lib/master-register.mjs');
const V = await import('../lib/master-variation.mjs');
const X = await import('../lib/order-settings-outbox.mjs');
const MEO = await import('../apps/master-edit/order-settings.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const POD = await import('../apps/purchase-orders/db.js');
const POS = await import('../apps/purchase-orders/order-settings.js');
const podb = POD.getDB();
{
  const t0 = new Date().toISOString();
  podb.prepare('INSERT INTO po_order_conditions (condition_id, supplier_code, display_name, condition_type, condition_value, unit, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)')
    .run('amc-30000', '1', 'AMC 3 万円以上', '金額', 30000, '円', t0, t0);
  podb.prepare('INSERT INTO po_material_groups (group_id, name, min_order_qty, unit, created_at, updated_at) VALUES (?,?,?,?,?,?)').run('oil', 'オイル', 1000, 'ml', t0, t0);
}

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e && e.stack || e}`); process.exitCode = 1; } }
const uuid = () => crypto.randomUUID();
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }], ['S02', { method: '宅急便', cost: 520 }]]);
const VALUES = Object.freeze({ standard_price: '1500', tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0', shipping_code: 'S01' });
const baseOf = (patch = { order_lot: '12', condition_id: 'amc-30000' }, extra = {}) => MEO.orderBaseOf({ order: { patch, newCondition: extra.newCondition ?? null, newMaterial: extra.newMaterial ?? null }, supplierCode: '0001' });
const regSingle = (code, { order = baseOf(), rid = uuid(), actor = 'naka@test', beforeCommit = undefined, kind = 'single' } = {}) => asEditor(() => R.registerNewSku(db, {
  actor, requestId: rid, kind, code, values: { name: `発注つき ${code}`, ...VALUES }, card: { create: false }, order,
}, { open: true, shippingRates: RATES, orderOutbox: true, beforeCommit }));
const rowsOf = (sql, p) => pg.query(sql, p).then((r) => r.rows);
const outboxOf = async (code) => one(`select o.event_id::text as event_id, o.status, o.attempts, o.registration_kind, o.registration_request_id::text as reg, o.delivery_request_id::text as del,
    o.payload, o.payload_hash, o.created_by, o.last_error, o.result, o.lease_owner from ops.order_settings_outbox o join core.skus s on s.sku_id = o.sku_id where s.code_norm = lower($1)`, [code]);
const attrsOf = (code) => podb.prepare('SELECT * FROM po_product_attrs WHERE product_key = ?').get(String(code).toLowerCase()) || null;
const auditN = (code) => podb.prepare('SELECT COUNT(*) AS n FROM po_audit_log WHERE resource = ?').get(`attrs:${String(code).toLowerCase()}`).n;
/** 持ち主のロールで守りの trigger を止めて書く (試験だけ = 時刻を古くする・中身を書き換える) */
const ownerForce = async (sql, p) => { await pg.query('alter table ops.order_settings_outbox disable trigger trg_order_settings_outbox_guard'); try { await pg.query(sql, p); } finally { await pg.query('alter table ops.order_settings_outbox enable trigger trg_order_settings_outbox_guard'); } };
const deliver = (opts = {}) => asEditor(() => X.runOrderSettingsOutbox(db, opts));

console.log('形と hash');
await ta('[K1] 知らせの中身の形 = JS と DB が同じ答え (正しい形・キーの過不足・版・数の値・入れ子)・hash = DB の ops.js_stable_sha256 (日本語・null・入れ子)', async () => {
  const ok = X.orderPayloadOf('kp-1', baseOf({ order_lot: '12', case_lot: null, capacity_per_unit: '1.5' }, { newCondition: { condition_id: 'c-new', display_name: '新しい条件', condition_type: '金額', unit: '円', condition_value: '30000' },
    newMaterial: { group_id: 'honey', name: 'はちみつ', min_order_qty: '20', unit: 'kg' } }));
  assert.equal(ok.patch.capacity_per_unit, '1.5'); assert.equal(ok.new_condition.maker_name, null);
  const samples = [ok, { ...ok, extra: 1 }, (({ patch, ...o }) => o)(ok), { ...ok, schema: 'po-settings-v2' }, { ...ok, code: '' }, { ...ok, supplier_code: 1 },
    { ...ok, patch: { bogus: '1' } }, { ...ok, patch: { order_lot: 12 } }, { ...ok, patch: [] }, { ...ok, new_condition: { condition_id: 'x' } },
    { ...ok, new_condition: { ...ok.new_condition, condition_value: 30000 } }, { ...ok, new_condition: 'x' }, { ...ok, new_material: null, new_condition: null },
    { ...ok, new_material: [] }, { ...ok, new_material: { ...ok.new_material, unit: true } }, [], 'x'];
  const want = [null, 'payload_keys', 'payload_keys', 'schema', 'code', 'supplier_code', 'patch_key', 'patch_value', 'patch', 'new_condition_keys', 'new_condition_value', 'new_condition', null,
    'new_material', 'new_material_value', 'payload_not_object', 'payload_not_object'];
  for (const [i, p] of samples.entries()) {
    const js = X.orderPayloadShapeProblem(p);
    const dbp = (await one('select ops.order_settings_payload_problem($1::jsonb) as p', [JSON.stringify(p)])).p;
    assert.equal(js, want[i], `JS ${i}: ${JSON.stringify(p).slice(0, 80)}`);
    assert.equal(dbp, js, `DB と JS が違う ${i}`);
  }
  for (const p of [ok, { ...ok, new_condition: null, new_material: null, patch: {} }, X.orderPayloadOf('ﾃｽﾄ-日本', baseOf())]) {
    assert.equal(X.orderPayloadHash(p), (await one('select ops.js_stable_sha256($1::jsonb) as h', [JSON.stringify(p)])).h);
  }
});

console.log('\n積む (登録と同じ取引)');
let E1 = null;
await ta('[E1] 単品の登録 + 発注の設定 = 同じ取引で知らせ 1 つ (登録の ID = 届けの ID・中身・hash は DB)・押し直し = 前の答え (知らせは増えない)・違う発注の設定 = 409・発注の設定なし = 知らせなし', async () => {
  const rid = uuid();
  const base = baseOf({ order_lot: '１２', condition_id: 'amc-30000', capacity_per_unit: '300' });
  const r = await regSingle('posync-s1', { rid, order: base });
  assert.equal(r.state, 'draft');
  const o = await outboxOf('posync-s1');
  assert.deepEqual([o.status, o.attempts, o.registration_kind, o.reg, o.del, o.created_by], ['pending', 0, 'single', rid, rid, 'naka@test']);
  assert.deepEqual(o.payload, X.orderPayloadOf('posync-s1', base));
  assert.equal(o.payload.patch.order_lot, '12', '全角の数字を半角の文字に');
  assert.equal(o.payload_hash, X.orderPayloadHash(o.payload));
  const again = await regSingle('posync-s1', { rid, order: base });
  assert.equal(again.replayed, true);
  assert.equal((await one(`select count(*)::int as n from ops.order_settings_outbox where registration_request_id = $1::uuid`, [rid])).n, 1);
  const e = await errOf(regSingle('posync-s1', { rid, order: baseOf({ order_lot: '48' }) }));
  assert.equal(e && e.reason, 'request_id_reused', e && e.message);
  const rid0 = uuid();
  await regSingle('posync-s0', { order: null, rid: rid0 });
  assert.equal(await outboxOf('posync-s0'), undefined);
  E1 = { rid, code: 'posync-s1', rid0 };
});

await ta('[E2] 登録が巻き戻る (commit の前に落ちる) = 知らせも無い (商品も無い)', async () => {
  const e = await errOf(regSingle('posync-rb', { beforeCommit: async () => { throw new Error('commit の前に落ちた'); } }));
  assert.match(String(e && e.message), /commit の前に落ちた/);
  assert.equal((await one(`select count(*)::int as n from core.skus where code_norm = 'posync-rb'`)).n, 0);
  assert.equal((await one(`select count(*)::int as n from ops.order_settings_outbox where code = 'posync-rb'`)).n, 0);
});

await ta('[E3] 断る: 取引の外 (もう commit した登録)・ほかの人・ほかの商品・2 件・形の違う中身・画面のロールの直接の insert / update・持ち主でも中身の書き換え / 消す・セットの発注の設定', async () => {
  const enq = (rid, actor, items) => pg.query('select ops.enqueue_order_settings($1::uuid, $2, $3::jsonb) as r', [rid, actor, JSON.stringify(items)]);
  const item = (code, base = baseOf()) => ({ code, payload: X.orderPayloadOf(code, base) });
  // 取引の外 = もう commit した登録 (発注の設定なしの posync-s0 = 知らせがまだ無い SKU) に、後から別の取引で積む = この取引で書いた登録でない (xmin)
  let e = await asEditor(async () => { await pg.query('begin'); try { return await errOf(enq(E1.rid0, 'naka@test', [item('posync-s0')])); } finally { await pg.query('rollback'); } });
  assert.match(String(e && e.message), /^not_in_registration: 発注の設定の知らせは、同じ人の登録 \(sku_create\) と同じ取引の中だけ/);
  // 登録の取引の中 (commit の前 = beforeCommit) で、ほかの人・ほかの商品・2 件・形の違う中身 (それぞれ savepoint で戻す)。最後に落として全部巻き戻す
  const rid = uuid();
  const seen = {};
  const sp = async (k, fn) => { await pg.query('savepoint t'); seen[k] = await errOf(fn()); await pg.query('rollback to savepoint t'); };
  const boom = await errOf(regSingle('posync-e3', { rid, order: null, beforeCommit: async () => {
    await sp('actor', () => enq(rid, 'other@test', [item('posync-e3')]));
    await sp('other_sku', () => enq(rid, 'naka@test', [item('s001')]));
    await sp('two', () => enq(rid, 'naka@test', [item('posync-e3'), item('s001')]));
    await sp('shape', () => enq(rid, 'naka@test', [{ code: 'posync-e3', payload: { ...X.orderPayloadOf('posync-e3', baseOf()), patch: { order_lot: 12 } } }]));
    await sp('code_mismatch', () => enq(rid, 'naka@test', [{ code: 'posync-e3', payload: X.orderPayloadOf('posync-zz', baseOf()) }]));
    await sp('direct', () => pg.query(`insert into ops.order_settings_outbox (sku_id, code, registration_kind, registration_request_id, delivery_request_id, schema_version, payload, payload_hash, created_by)
      select sku_id, code, 'single', $1::uuid, $1::uuid, 'po-settings-v1', '{}'::jsonb, repeat('0', 64), 'naka@test' from core.skus where code = 'posync-e3'`, [rid]));
    seen.ok = await enq(rid, 'naka@test', [item('posync-e3')]).then((r) => r.rows[0].r, (x) => x);
    throw new Error('最後に巻き戻す');
  } }));
  assert.match(String(boom && boom.message), /最後に巻き戻す/);
  assert.match(String(seen.actor && seen.actor.message), /^not_in_registration/);
  assert.match(String(seen.other_sku && seen.other_sku.message), /^not_in_registration/);
  assert.match(String(seen.two && seen.two.message), /^invalid_input: 単品の登録の知らせは 1 件/);
  assert.match(String(seen.shape && seen.shape.message), /^invalid_input: .*patch_value/);
  assert.match(String(seen.code_mismatch && seen.code_mismatch.message), /^invalid_input: 知らせの中身のコード/);
  assert.equal(seen.direct && seen.direct.code, '42501', '画面のロールが知らせを直接書けた');
  assert.ok(Array.isArray(seen.ok) && seen.ok.length === 1, `同じ取引の正しい知らせは積める: ${JSON.stringify(seen.ok)}`);
  assert.equal((await one(`select count(*)::int as n from ops.order_settings_outbox where code = 'posync-e3'`)).n, 0, '巻き戻した登録の知らせが残った');
  // 画面のロールは状態の列も書けない
  e = await asEditor(() => errOf(pg.query(`update ops.order_settings_outbox set status = 'done' where code = 'posync-s1'`)));
  assert.equal(e && e.code, '42501');
  // 持ち主でも: 中身は変えない・消さない・空にしない
  assert.match(String((await errOf(pg.query(`update ops.order_settings_outbox set payload = '{}'::jsonb where code = 'posync-s1'`)))?.message), /中身/);
  assert.match(String((await errOf(pg.query(`delete from ops.order_settings_outbox where code = 'posync-s1'`)))?.message), /消さない/);
  assert.ok(await errOf(pg.query('truncate ops.order_settings_outbox')));
  // セットには発注の設定が無い (lib でも断る)
  e = await errOf(asEditor(() => R.registerNewSku(db, { actor: 'naka@test', requestId: uuid(), kind: 'set', code: 'posync-set', values: { name: 'x', standard_price: '1', reorder_months: '1', components: [{ code: 's001', qty: 1 }] }, card: { create: false }, order: baseOf() },
    { open: true, shippingRates: RATES, orderOutbox: true })));
  assert.equal(e && e.reason, 'order_settings_set');
});

let VB = null;
await ta('[E4] まとまりの登録 + 発注の設定 = 子ごとに 1 つ (同じ取引・届けの ID = sub_request_id(登録の ID, order:子) = JS と DB と今までの ②)・違う発注の設定の押し直し = 409', async () => {
  const rid = uuid();
  const base = baseOf({ order_lot: '6' }, { newMaterial: { group_id: 'wool', name: 'ウール', min_order_qty: '5', unit: 'kg' } });
  const input = {
    group: { mode: 'new', code: 'posync-g', name: '発注つきのまとまり' }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-WH', name: 'ホワイト' }, { axis: 1, code: '-BK', name: 'ブラック' }],
    children: [{ code: 'posync-g-WH', choices: { 1: '-WH' }, name: 'まとまり【ホワイト】' }, { code: 'posync-g-BK', choices: { 1: '-BK' }, name: 'まとまり【ブラック】' }],
    values: { ...VALUES }, card: {}, order: base,
  };
  const r = await asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: rid, ...input }, { open: true, shippingRates: RATES, orderOutbox: true }));
  assert.equal(r.children.length, 2);
  for (const code of ['posync-g-WH', 'posync-g-BK']) {
    const o = await outboxOf(code);
    const norm = code.toLowerCase();
    assert.deepEqual([o.status, o.registration_kind, o.reg], ['pending', 'variation', rid]);
    assert.equal(o.del, V.subRequestId(rid, `order:${norm}`));
    assert.equal(o.del, (await one('select ops.variation_sub_request_id($1::uuid, $2)::text as d', [rid, `order:${norm}`])).d);
    assert.deepEqual(o.payload, X.orderPayloadOf(code, base));
  }
  const again = await asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: rid, ...input }, { open: true, shippingRates: RATES, orderOutbox: true }));
  assert.equal(again.replayed, true);
  const e = await errOf(asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: rid, ...input, order: baseOf({ order_lot: '7' }) }, { open: true, shippingRates: RATES, orderOutbox: true })));
  assert.equal(e && e.reason, 'request_id_reused');
  assert.equal((await one(`select count(*)::int as n from ops.order_settings_outbox where registration_request_id = $1::uuid`, [rid])).n, 2);
  VB = { rid };
});

console.log('\n届ける (発注アプリへ)');
await ta('[D1] 届ける = 発注アプリに書く (via master-edit:new・記録 1 つ)・まとまりの子 (新しい原料グループは 1 回だけ作る)・届けた知らせは閉じる', async () => {
  const r = await deliver({ mode: 'auto', registrationRequestId: E1.rid });
  assert.deepEqual(r.map((x) => [x.code, x.status, x.recorded, x.result.outcome]), [['posync-s1', 'done', true, 'written']]);
  const a = attrsOf('posync-s1');
  assert.deepEqual([a.order_lot, a.condition_id, a.capacity_per_unit, a.created_via], [12, 'amc-30000', 300, 'master-edit:new']);
  assert.equal(auditN('posync-s1'), 1);
  assert.ok(podb.prepare('SELECT 1 FROM po_order_settings_requests WHERE request_id = ?').get(E1.rid));
  const o = await outboxOf('posync-s1');
  assert.deepEqual([o.status, o.attempts, o.lease_owner, o.last_error], ['done', 1, null, null]);
  const v = await deliver({ mode: 'auto', registrationRequestId: VB.rid });
  assert.deepEqual(v.map((x) => x.status), ['done', 'done']);
  assert.deepEqual([attrsOf('posync-g-WH').material_group_id, attrsOf('posync-g-BK').material_group_id, attrsOf('posync-g-BK').order_lot], ['wool', 'wool', 6]);
  assert.equal(podb.prepare("SELECT COUNT(*) AS n FROM po_material_groups WHERE group_id = 'wool'").get().n, 1);
  assert.deepEqual(await deliver({ mode: 'manual', registrationRequestId: E1.rid }), [], '届いた知らせをもう一度借りた');
});

await ta('[D2] 2 回届けても 1 回分: 書いた後に結果を書けずに借りが切れた = 次の回は「前の結果」(replayed)・記録は増えない・前の借り手の結果は書けない・借りの間はほかが取らない', async () => {
  await regSingle('posync-d2', { order: baseOf({ order_lot: '3' }) });
  const ev = (await outboxOf('posync-d2')).event_id;
  const c1 = await asEditor(() => rowsOf(`select * from ops.claim_order_settings_events('ghost', 'auto', null, $1::uuid, 1, 60, 5)`, [ev]));
  assert.equal(c1.length, 1);
  assert.deepEqual(await deliver({ mode: 'auto', eventId: ev }), [], '借りの間にほかが取った');
  POS.writeRegistrationOrderSettings(X.deliveryInputOf({ ...c1[0], delivery_request_id: String(c1[0].delivery_request_id), payload: c1[0].payload }));   // 書いた (結果は書けなかった)
  assert.equal(attrsOf('posync-d2').order_lot, 3);
  await pg.query(`update ops.order_settings_outbox set leased_until = now() - interval '1 second' where event_id = $1::uuid`, [ev]);   // 借りが切れた
  const r = await deliver({ mode: 'auto', eventId: ev });
  assert.deepEqual([r[0].status, r[0].result.outcome, r[0].result.replayed], ['done', 'replayed', true]);
  assert.equal(auditN('posync-d2'), 1, '2 回書いた');
  assert.equal((await asEditor(() => one(`select ops.finish_order_settings_event($1::uuid, 'ghost', 'failed', null, 'x') as ok`, [ev]))).ok, false, '前の借り手が結果を書けた');
  assert.equal((await outboxOf('posync-d2')).status, 'done');
});

await ta('[D3] 失敗 = failed (理由・回数)・自動は 5 回まで・夜間の段 (sweep) と人 (manual) は続ける・ほかの画面が先に作っていた = 要確認 (上書きしない・自動 / 夜間は試さない・人は試す)・閉じる (理由・failed / conflict だけ)', async () => {
  await regSingle('posync-d3', { order: baseOf({ order_lot: '5' }) });
  const ev = (await outboxOf('posync-d3')).event_id;
  const boom = async () => { throw new Error('SQLite が書けない'); };
  for (let i = 1; i <= 5; i++) {
    const r = await deliver({ mode: 'auto', eventId: ev, apply: boom });
    assert.deepEqual([r.length, r[0] && r[0].status], [1, 'failed'], `${i} 回目`);
  }
  let o = await outboxOf('posync-d3');
  assert.deepEqual([o.status, o.attempts, o.last_error], ['failed', 5, 'SQLite が書けない']);
  assert.deepEqual(await deliver({ mode: 'auto', eventId: ev, apply: boom }), [], '自動が 5 回を超えて試した');
  assert.equal((await deliver({ mode: 'sweep', eventId: ev, apply: boom })).length, 1, '夜間の段が試さない');
  // 届け待ちは閉じない・失敗は閉じられる (理由が要る)
  await regSingle('posync-d3p', { order: baseOf({ order_lot: '5' }) });
  const evp = (await outboxOf('posync-d3p')).event_id;
  assert.equal(await asEditor(() => X.dismissOrderSettingsEvent(db, { eventId: evp, actor: 'naka@test', reason: '試験' })), false, '届け待ちを閉じた');
  assert.match(String((await errOf(asEditor(() => X.dismissOrderSettingsEvent(db, { eventId: ev, actor: 'naka@test', reason: ' ' }))))?.message), /閉じる理由/);
  // ほかの画面が先に作った → 人の「もう一度」= 要確認 (上書きしない)
  POS.writeOrderSettings({ code: 'posync-d3', patch: { order_lot: 9 }, seen: { updated_at: null }, actor: 'other@test', via: 'po-admin' });
  const r = await deliver({ mode: 'manual', eventId: ev });
  assert.deepEqual([r[0].status, r[0].result.reason, attrsOf('posync-d3').order_lot], ['conflict', 'stale', 9]);
  assert.match(r[0].error, /もう作られていました/);
  assert.deepEqual(await deliver({ mode: 'auto', eventId: ev }), []);
  assert.deepEqual(await deliver({ mode: 'sweep', eventId: ev }), []);
  assert.equal((await deliver({ mode: 'manual', eventId: ev })).length, 1, '人の「もう一度」が要確認を試さない');
  assert.equal(await asEditor(() => X.dismissOrderSettingsEvent(db, { eventId: ev, actor: 'naka@test', reason: '発注アプリの 9 が正しい' })), true);
  o = await outboxOf('posync-d3');
  assert.equal(o.status, 'dismissed');
  assert.equal(await asEditor(() => X.dismissOrderSettingsEvent(db, { eventId: ev, actor: 'naka@test', reason: 'もう一度' })), false);
  assert.deepEqual(await deliver({ mode: 'manual', eventId: ev }), [], '閉じた知らせを借りた');
  // 借りの最中は閉じない
  await regSingle('posync-d3l', { order: baseOf({ order_lot: '5' }) });
  const evl = (await outboxOf('posync-d3l')).event_id;
  await deliver({ mode: 'auto', eventId: evl, apply: boom });
  await asEditor(() => rowsOf(`select 1 from ops.claim_order_settings_events('holder', 'manual', null, $1::uuid, 1, 60, 5)`, [evl]));
  assert.equal(await asEditor(() => X.dismissOrderSettingsEvent(db, { eventId: evl, actor: 'naka@test', reason: '試験' })), false, '借りの最中に閉じた');
  await pg.query(`update ops.order_settings_outbox set leased_until = now() - interval '1 second' where event_id = $1::uuid`, [evl]);
  await deliver({ mode: 'manual', eventId: evl });
  // 届け待ち (posync-d3p) は後の試験で届ける
});

await ta('[D4] 登録をやめた下書き (0071) = 届けずに閉じる (発注アプリに書かない)・中身が書き換わった (hash が合わない) = 届けない (failed)', async () => {
  await regSingle('posync-d4', { order: baseOf({ order_lot: '8' }) });
  const { cancelSkuDraft } = await import('../lib/master-draft-cancel.mjs');
  await asEditor(() => cancelSkuDraft(db, { actor: 'naka@test', code: 'posync-d4', reason: '入れ間違い', requestId: uuid(), confirm: true }, { open: true, requireConfirm: true }));
  const r = await deliver({ mode: 'auto', eventId: (await outboxOf('posync-d4')).event_id });
  assert.deepEqual([r[0].status, r[0].result.outcome], ['done', 'skipped_cancelled']);
  assert.equal(attrsOf('posync-d4'), null, 'やめた下書きの発注の設定を書いた');
  await regSingle('posync-d5', { order: baseOf({ order_lot: '8' }) });
  await ownerForce(`update ops.order_settings_outbox set payload = jsonb_set(payload, '{patch,order_lot}', '"800"') where code = 'posync-d5'`);
  const t = await deliver({ mode: 'auto', eventId: (await outboxOf('posync-d5')).event_id });
  assert.deepEqual([t[0].status], ['failed']);
  assert.match(t[0].error, /^payload_hash_mismatch/);
  assert.equal(attrsOf('posync-d5'), null);
  await asEditor(async () => X.dismissOrderSettingsEvent(db, { eventId: (await outboxOf('posync-d5')).event_id, actor: 'naka@test', reason: '試験の書き換え' }));
});

console.log('\n夜間ロードの後ろの段');
const pgConnect = async () => ({ query: (t, p) => pg.query(t, p), exec: (t) => pg.exec(t) });
await ta('[N1] 夜間の段: 届け待ちを届ける (回数の上限なし)・24 時間を超えた届いていない知らせ = 段の失敗 (コードを note に)・閉じれば戻る・Render の外 = 飛ばす・URL が無い = 失敗', async () => {
  X.__setOrderDeliveryEnabledForTest(false);
  assert.deepEqual(await X.runOrderSettingsNightlyStep({ url: 'x', connect: pgConnect }), { ok: true, skipped: true, note: 'Render の外なので届けない' });
  X.__setOrderDeliveryEnabledForTest(true);
  assert.equal((await X.runOrderSettingsNightlyStep({ url: '', connect: pgConnect })).ok, false);
  const s1 = await X.runOrderSettingsNightlyStep({ url: 'x', connect: pgConnect });
  assert.equal(s1.ok, true, JSON.stringify(s1));
  assert.ok(s1.delivered >= 1, JSON.stringify(s1));
  assert.equal((await outboxOf('posync-d3p')).status, 'done');
  assert.equal(attrsOf('posync-d3p').order_lot, 5);
  // 24 時間を超えて届かない知らせ (書けない) = 段の失敗
  await regSingle('posync-n1', { order: baseOf({ order_lot: '2' }) });
  await ownerForce(`update ops.order_settings_outbox set created_at = now() - interval '25 hours' where code = 'posync-n1'`);
  const s2 = await X.runOrderSettingsNightlyStep({ url: 'x', connect: pgConnect, apply: async () => { throw new Error('書けない'); } });
  assert.equal(s2.ok, false);
  assert.match(s2.note, /24時間超 1: posync-n1/);
  assert.equal(s2.overdue, 1);
  assert.equal((await outboxOf('posync-n1')).status, 'failed');
  // 見張りの view も同じ数
  assert.deepEqual(await one(`select n, overdue from ops.v_order_settings_outbox_open where status = 'failed'`), { n: 1, overdue: 1 });
  await asEditor(async () => X.dismissOrderSettingsEvent(db, { eventId: (await outboxOf('posync-n1')).event_id, actor: 'naka@test', reason: '商品の画面で入れた' }));
  const s3 = await X.runOrderSettingsNightlyStep({ url: 'x', connect: pgConnect });
  assert.deepEqual([s3.ok, s3.overdue], [true, 0], JSON.stringify(s3));
});

await ta('[N2] 夜間ロード (apps/company-db/nightly.mjs) の既定の後ろの段に入っている (新しい定期実行は作らない)・台帳の夜間ロードの項目に段と手順', async () => {
  const N = await import('../apps/company-db/nightly.mjs');
  assert.deepEqual(N.DEFAULT_STEPS.map((s) => s.id), ['po-settings-outbox']);
  const { JOBS_REGISTRY: list, validateRegistry } = await import('../config/jobs-registry.mjs');
  const vr = validateRegistry();
  assert.ok(!vr || vr === true || (Array.isArray(vr) ? vr.length === 0 : vr.ok !== false), `台帳の確かめ: ${JSON.stringify(vr).slice(0, 300)}`);
  const job = list.find((j) => j && j.id === 'company-db-nightly-load');
  assert.ok(job, '台帳に company-db-nightly-load が無い');
  assert.match(job.purpose, /発注の設定の知らせ/); assert.match(job.runbook, /発注の設定の届け待ち/);
  assert.equal(list.filter((j) => j && /order-settings|po-settings/.test(String(j.id))).length, 0, '新しい定期実行を台帳に足した');
});

console.log('\n権限');
await ta('[G1] 画面のロール = 表 (と view) を読むだけ + 4 つの関数・PUBLIC は無し・形の確かめの部品は持ち主だけ・見張りは読める・重い入口の棚卸しに漏れが無い', async () => {
  const fn = async (role, sig) => (await one(`select has_function_privilege($1, $2::regprocedure, 'execute') as ok`, [role, sig])).ok;
  for (const sig of ['ops.enqueue_order_settings(uuid, text, jsonb)', 'ops.claim_order_settings_events(text, text, uuid, uuid, integer, integer, integer)',
    'ops.finish_order_settings_event(uuid, text, text, jsonb, text)', 'ops.dismiss_order_settings_event(uuid, text, text)']) {
    assert.equal(await fn('master_edit', sig), true, sig);
    assert.equal(await fn('public', sig), false, `PUBLIC ${sig}`);
    assert.equal(await fn('watcher', sig), false, `watcher ${sig}`);
  }
  assert.equal(await fn('master_edit', 'ops.order_settings_payload_problem(jsonb)'), false);
  assert.equal(await fn('public', 'ops.order_settings_payload_problem(jsonb)'), false);
  const tb = async (role, t, p) => (await one(`select has_table_privilege($1, $2, $3) as ok`, [role, t, p])).ok;
  assert.deepEqual([await tb('master_edit', 'ops.order_settings_outbox', 'select'), await tb('master_edit', 'ops.order_settings_outbox', 'insert'), await tb('master_edit', 'ops.order_settings_outbox', 'update'),
    await tb('master_edit', 'ops.order_settings_outbox', 'delete'), await tb('master_edit', 'ops.v_order_settings_outbox_open', 'select')], [true, false, false, false, true]);
  assert.deepEqual([await tb('watcher', 'ops.order_settings_outbox', 'select'), await tb('watcher', 'ops.v_order_settings_outbox_open', 'select')], [true, true]);
  const { heavyEntryFindings } = await import('./company-db/heavy-entry-manifest.mjs');
  assert.deepEqual(await heavyEntryFindings(db), []);
  // security definer の関数 = search_path 固定
  const cfg = await rowsOf(`select p.proname, p.prosecdef, array_to_string(p.proconfig, ',') as c from pg_proc p join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'ops' and p.proname in ('enqueue_order_settings', 'claim_order_settings_events', 'finish_order_settings_event', 'dismiss_order_settings_event') order by 1`);
  assert.deepEqual(cfg.map((r) => [r.proname, r.prosecdef, r.c]), [['claim_order_settings_events', true, 'search_path=pg_catalog, pg_temp'], ['dismiss_order_settings_event', true, 'search_path=pg_catalog, pg_temp'],
    ['enqueue_order_settings', true, 'search_path=pg_catalog, pg_temp'], ['finish_order_settings_event', true, 'search_path=pg_catalog, pg_temp']]);
});

// ── 画面 (master-edit のまとまりの登録・発注アプリ) ──
console.log('\n画面');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/posync';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/posync';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let chain = Promise.resolve();
const roleClient = async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
};
const MR = await import('../apps/master-edit/router.mjs');
MR.__setPgClientFactory(roleClient);
MR.__setShippingRatesProvider(async () => RATES);
MR.__setPhDraftLookup(async () => null);
MR.__setGroupCardApplier(async () => ({ outcome: 'created', draft_id: 1 }));
(await import('../lib/product-hub-outbox.mjs')).__setCompanyDbClientFactory(roleClient);
(await import('../lib/master-legacy-gate.mjs')).__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open' }));
const POR = (await import('../apps/purchase-orders/router.js')).default;
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] }; next(); });
app.use('/apps/master-edit', MR.default);
app.use('/apps/purchase-orders', express.json({ limit: '1mb' }), POR);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const call = async (method, url, body) => {
  const r = await fetch(ORIGIN + url, { method, headers: { Accept: 'application/json', Origin: ORIGIN, ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}) }, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await r.text(); let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, text, j };
};

await ta('[R1] まとまりの登録 (画面の API) + 発注の設定 = 子ごとに届く (written)・届かない子 = 数と理由 (pending)・押し直しで届く・画面の JS の文', async () => {
  const body = (code, rid = uuid()) => ({ request_id: rid, group: { mode: 'new', code, name: 'まとまり (画面)' }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-A', name: 'A' }, { axis: 1, code: '-B', name: 'B' }],
    children: [{ code: `${code}-A`, choices: { 1: '-A' }, name: `${code}【A】` }, { code: `${code}-B`, choices: { 1: '-B' }, name: `${code}【B】` }],
    values: { ...VALUES }, card: {}, order_settings: { order_lot: '24', condition_id: 'amc-30000' } });
  const r = await call('POST', '/apps/master-edit/api/new/variation', body('posync-r1'));
  assert.equal(r.status, 200, r.text);
  assert.deepEqual(r.j.order_settings, { ok: true, written: 2, pending: 0, failed: [] });
  assert.deepEqual([attrsOf('posync-r1-A').order_lot, attrsOf('posync-r1-B').condition_id], [24, 'amc-30000']);
  // 届かない (書けない) = 登録はできている・子ごとの知らせは failed で残る → 押し直しで届く
  MR.__setOrderWriter(() => { throw new Error('SQLite に書けない'); });
  const b2 = body('posync-r2');
  let r2;
  try { r2 = await call('POST', '/apps/master-edit/api/new/variation', b2); } finally { MR.__setOrderWriter(null); }
  assert.equal(r2.status, 200, r2.text);
  assert.deepEqual([r2.j.order_settings.ok, r2.j.order_settings.written, r2.j.order_settings.pending, r2.j.order_settings.failed.map((f) => [f.code, f.status])],
    [false, 0, 2, [['posync-r2-A', 'failed'], ['posync-r2-B', 'failed']]]);
  assert.match(r2.j.order_settings.failed[0].error, /SQLite に書けない/);
  assert.equal(attrsOf('posync-r2-A'), null);
  const r3 = await call('POST', '/apps/master-edit/api/new/variation', b2);
  assert.deepEqual([r3.status, r3.j.replayed, r3.j.order_settings.ok, r3.j.order_settings.written], [200, true, true, 2], r3.text);
  assert.equal(attrsOf('posync-r2-B').order_lot, 24);
  const js = await (await fetch(`${ORIGIN}/apps/master-edit/public/me-variation.js`)).text();
  assert.match(js, /まだ発注アプリに届いていません/);
});

await ta('[P1] 発注アプリ: 届いていない新商品に「発注の設定がまだ届いていない」の印 (すすめる数は今の設定で出す = 止めない)・全部の画面の上の帯・API の件数・Company DB が読めなくても画面は出る', async () => {
  // 届いていない知らせ (Render の外で登録 = 届けない) と、商品管理リストの行 (要発注)
  X.__setOrderDeliveryEnabledForTest(false);
  await regSingle('posync-p1', { order: baseOf({ order_lot: '10' }) });
  X.__setOrderDeliveryEnabledForTest(true);
  const T0 = new Date(Date.now() - 86400e3).toISOString();
  podb.prepare(`INSERT OR REPLACE INTO mirror_pml_published (id, run_id, status, as_of_date, synced_at) VALUES (1, 'run_p', 'ok', ?, ?)`).run(T0.slice(0, 10), T0);
  const ins = podb.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 商品区分, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数, 売価, 原価)
    VALUES ('run_p', ?, ?, '0001', '取扱中', '', 0, 0, 10, 100, ?, 1.5, 500, 200)`);
  ins.run('posync-p1', '届いていない新商品', 6);
  ins.run('s001', '前からある商品', 6);
  // 画面を開く = 先に届け直す (60 秒に 1 回) → 印を見たいので届け直しを止めた状態で読む
  X.__setOrderDeliveryEnabledForTest(false);
  X.__resetOrderOutboxCacheForTest();
  const page = await call('GET', '/apps/purchase-orders/supplier/1');
  assert.equal(page.status, 200, page.text.slice(0, 300));
  const D = JSON.parse(/var D = (\{[\s\S]*?\});\nvar CART/.exec(page.text.replace(/\r\n/g, '\n'))[1]);
  const all = [...D.targets, ...D.candidates, ...D.horikoshi];
  const p1 = all.find((p) => p.code === 'posync-p1'); const s1 = all.find((p) => p.code === 's001');
  assert.ok(p1 && s1, '商品管理リストの行が出ない');
  assert.deepEqual([p1.poPending, s1.poPending], [true, undefined]);
  assert.ok(p1.recQty > 0, 'すすめる数を止めた (決定 2 = b は止めない)');
  assert.deepEqual([D.poPending.ok, D.poPending.open >= 1], [true, true]);
  assert.match(page.text, /function poPendBadge\(p\)[\s\S]{0,400}発注の設定がまだ届いていない/);
  assert.match(page.text, /poPendBadge\(p\) \+ addedBadge\(p\)/);
  assert.match(page.text, /id="poOutbox" hidden/);
  assert.match(page.text, /\/apps\/purchase-orders\/api\/order-settings-outbox/);
  const api = await call('GET', '/apps/purchase-orders/api/order-settings-outbox');
  assert.deepEqual([api.status, api.j.ok, api.j.ready, api.j.items.some((i) => i.code === 'posync-p1')], [200, true, true, true]);
  // ほかの画面にも帯
  assert.match((await call('GET', '/apps/purchase-orders/orders')).text, /id="poOutbox" hidden/);
  // Company DB が読めない = 印も帯も無しで画面は出る
  const PHO = await import('../lib/product-hub-outbox.mjs');
  PHO.__setCompanyDbClientFactory(async () => { throw new Error('connect ECONNREFUSED'); });
  X.__resetOrderOutboxCacheForTest();
  try {
    const down = await call('GET', '/apps/purchase-orders/supplier/1');
    assert.equal(down.status, 200);
    const D2 = JSON.parse(/var D = (\{[\s\S]*?\});\nvar CART/.exec(down.text.replace(/\r\n/g, '\n'))[1]);
    assert.equal([...D2.targets, ...D2.candidates].find((p) => p.code === 'posync-p1').poPending, undefined);
    assert.equal(D2.poPending.ok, false);
    assert.equal((await call('GET', '/apps/purchase-orders/api/order-settings-outbox')).j.ok, false);
  } finally { PHO.__setCompanyDbClientFactory(roleClient); }
  // 届け直し (Render の中) = 開いたら届く → 印は消える
  X.__setOrderDeliveryEnabledForTest(true);
  X.__resetOrderOutboxCacheForTest();
  const after = await call('GET', '/apps/purchase-orders/supplier/1');
  const D3 = JSON.parse(/var D = (\{[\s\S]*?\});\nvar CART/.exec(after.text.replace(/\r\n/g, '\n'))[1]);
  assert.equal([...D3.targets, ...D3.candidates].find((p) => p.code === 'posync-p1').poPending, undefined);
  assert.equal(attrsOf('posync-p1').order_lot, 10);
});

server.close();
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.log('NG があります');
setTimeout(() => process.exit(process.exitCode || 0), 50).unref();
