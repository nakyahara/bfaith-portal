/**
 * test-master-variation-batch.mjs — 色違い・サイズ違いのまとまりの登録の lib (lib/master-variation.mjs・CompanyDB構想/20 v7 §⑩ の PR-7)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs・products.parent も company)。書き込みは画面のロール master_edit。
 * 固定する契約:
 *   K 決まり: 子の request_id の作り方 = DB (ops.variation_sub_request_id)・子の数の上限 120 = DB (ops.variation_max_children)・入力の形 (DB を読む前に断る)
 *   G 門: products.parent の DB の active が load = 409 parent_not_company (まとまりは何も書かない)・単品で書く列の門・MASTER_EDIT_OPEN
 *   B まとめての登録 (1 取引): 新しいまとまり (2 軸)・子の名前 / 売価 / 原価 / JAN・共通の欄・選択肢・親・まとまりの知らせの共通の欄 (公式 URL・送料)・記録 (open・子・JAN・close)
 *   R 押し直し: 同じ request_id = 前の答え・違う中身 / 人 = 409・失敗の記録 = 同じ誤り (replayed)
 *   F 1 つでも断られたら全部巻き戻す: 子のコードがもうある (code_taken・どの子か)・JAN がほかの商品・まとまりのコード (group_exists)
 *   A 今あるまとまりに足す: 記録のあるまとまり (足す色・前回作らなかった組み合わせ・今ある組み合わせ = choice_exists・今ある文字 = option_exists)・
 *     NE で作ったまとまり (最初の 1 回 = 前からある子の選択肢も記録・子は増えない)・単品が代表のまとまり
 *   S 探す・読む・確かめる: コード・名前・子の商品コードで探す・まとまりでない単品は出さない・今ある子の値・確かめの答え (重なり・product-hub の下書き・JAN)
 *   E 商品の画面の保存 (saveSku) に代表の欄は無い = parent_code は 400 parent_not_editable
 *   C NE 登録の CSV: まとまりの子は回ごとに並ぶ・回ごとに 1 ファイル (代表商品コード = まとまりのコード・JAN = empty)・2 つの回がどちらもまだ = 回ごとには作れない (まとめて 1 ファイル)
 * 使い方: node scripts/test-master-variation-batch.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vg7-batch-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { setupVariationDb, jan13, NOW_MS } = await import('./fixtures/master-variation-db.mjs');
const V = await import('../lib/master-variation.mjs');
const W = await import('../lib/master-write.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const O = await import('../lib/product-hub-outbox.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const uuid = () => crypto.randomUUID();
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
async function rejects(p, status, reason) {
  const e = await errOf(p);
  assert.ok(e, `断らなかった (${reason})`);
  assert.equal(e.reason, reason, `${e.reason}: ${e.message}`);
  assert.equal(e.status, status, e.message);
  return e;
}

const T = await setupVariationDb();
const { pg, db, q, one, asEditor } = T;
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }], ['S02', { method: '宅急便', cost: 520 }]]);
const reg = (input, opts = {}) => asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: uuid(), ...input }, { open: true, shippingRates: RATES, ...opts }));
const VALUES = Object.freeze({ standard_price: '12800', cost: { jpy: '5200' }, tax_rate: '0.1', sales_class: '1', primary_supplier: '0001', reorder_months: '3', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'S02' });
const counts = async () => one(`select (select count(*) from core.products)::int as products, (select count(*) from core.skus)::int as skus,
  (select count(*) from core.variation_options)::int as options, (select count(*) from core.sku_variation_choices)::int as choices, (select count(*) from ops.product_hub_outbox)::int as outbox,
  (select count(*) from ops.variation_batches)::int as batches, (select count(*) from core.external_ids where system = 'jan')::int as jans`);
const kidName = (base, h, v, jan) => `${base}【${h}】${v ? `【${v}】` : ''}${jan ? `【${jan}】` : ''}`;
const JAN_A = jan13('458012345001');
const JAN_B = jan13('458012345002');

/** 子ども袴 (2 軸) = 4 色 × 4 サイズから、エンジ × 90 を作らない = 15 子 (見本 ①) */
function hakamaInput(code = 'hakama', extra = {}) {
  const H = [['ホワイト', '-WH'], ['ブラック', '-BK'], ['ネイビー', '-NV'], ['エンジ', '-EN']];
  const Vs = [['90cm', '-90'], ['100cm', '-100'], ['110cm', '-110'], ['120cm', '-120']];
  const children = [];
  for (const [hn, hc] of H) for (const [vn, vc] of Vs) {
    if (hc === '-EN' && vc === '-90') continue;
    const jan = code !== 'hakama' ? '' : hc === '-WH' && vc === '-90' ? JAN_A : hc === '-WH' && vc === '-100' ? JAN_B : '';
    children.push({ code: `${code}${hc}${vc}`, choices: { 1: hc, 2: vc }, name: kidName('子ども袴 2点セット', hn, vn, jan), price: hc === '-BK' && vc === '-120' ? '13800' : '', cost: '', jan });
  }
  children.find((k) => k.code === `${code}-NV-110`).name = '子ども袴 2点セット【ネイビー】【110cm】限定柄';
  return {
    group: { mode: 'new', code, name: '子ども袴 2点セット' }, axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
    options: [...H.map(([n, c]) => ({ axis: 1, code: c, name: n })), ...Vs.map(([n, c]) => ({ axis: 2, code: c, name: n }))],
    children, values: { ...VALUES }, card: { official_url: 'https://example.com/hakama', amazon_url: '', asin: '' }, ...extra,
  };
}

console.log('決まり');
await ta('[K1] 子・閉じる・JAN の request_id の作り方 = DB (ops.variation_sub_request_id)・子の数の上限 120 = DB (ops.variation_max_children)', async () => {
  const rid = uuid();
  for (const tag of ['child:hakama-wh-90', 'close', 'jan:hakama-wh-90']) {
    assert.equal(V.subRequestId(rid, tag), (await one('select ops.variation_sub_request_id($1::uuid, $2)::text as r', [rid, tag])).r);
  }
  assert.equal(V.VARIATION_MAX_CHILDREN, (await one('select ops.variation_max_children() as n')).n);
  assert.equal(V.VARIATION_MAX_CHILDREN, 120);
  assert.deepEqual(V.VARIATION_KEYS.slice(-1), ['products.parent']);
});

await ta('[K2] 入力の形 (DB を読む前・何も記録しない): まとまり・軸・選択肢 (「-」から・重なり)・子 (コード = まとまり + 文字・重なり・JAN の形と重なり・名前の長さ)・121 件・共通の欄の必須', async () => {
  const P = (patch) => V.parseVariationRequest({ actor: 'naka@test', requestId: uuid(), ...hakamaInput(), ...patch });
  const bad = (patch, reason, field) => { let e = null; try { P(patch); } catch (x) { e = x; } assert.ok(e, `断らなかった ${reason}`); assert.equal(e.reason, reason, e.message); if (field) assert.equal(e.extra.field, field, e.message); return e; };
  assert.equal(P({}).children.length, 15);
  bad({ group: { mode: 'new', code: 'set-x', name: 'x' } }, 'invalid_input', 'group.code');
  bad({ group: { mode: 'new', code: 'ok', name: '' } }, 'invalid_input');
  bad({ group: { mode: 'add', product_id: 'abc' } }, 'invalid_input', 'group');
  bad({ axes: null }, 'invalid_input', 'axes.1');
  bad({ axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'ｶﾗｰ' }] }, 'invalid_input', 'axes.2');
  bad({ options: [{ axis: 1, code: 'WH', name: '白' }] }, 'option_code_shape', 'options.1');
  bad({ options: [{ axis: 1, code: '-WH', name: '白' }, { axis: 1, code: '-wh', name: '白2' }] }, 'option_exists');
  bad({ options: [{ axis: 1, code: '-A', name: 'Ａ' }, { axis: 1, code: '-B', name: 'A' }] }, 'option_name_exists');
  const base = hakamaInput();
  const e1 = bad({ children: [{ ...base.children[0], code: 'hakama-wh-90' }] }, 'child_code_not_group_plus_choices');
  assert.equal(e1.extra.code, 'hakama-wh-90');
  bad({ children: [base.children[0], { ...base.children[0] }] }, 'child_dup');
  bad({ children: [{ ...base.children[0], jan: '4580123450011' }] }, 'jan_shape');
  bad({ children: [{ ...base.children[0], jan: JAN_A }, { ...base.children[1], jan: JAN_A }] }, 'jan_dup');
  bad({ children: [{ ...base.children[0], name: 'あ'.repeat(256) }] }, 'invalid_input');
  bad({ children: [{ ...base.children[0], name: 'empty' }] }, 'invalid_input');
  bad({ children: [{ ...base.children[0], price: '0' }] }, 'invalid_input');
  const many = Array.from({ length: 121 }, (_, i) => ({ code: `m-C${i}`, choices: { 1: `-C${i}` }, name: `色 ${i}` }));
  bad({ group: { mode: 'new', code: 'm', name: 'x' }, axes: [{ axis: 1, name: '色' }], options: many.map((k, i) => ({ axis: 1, code: `-C${i}`, name: `色 ${i}` })), children: many }, 'too_many', 'children');
  for (const k of ['standard_price', 'tax_rate', 'sales_class', 'primary_supplier', 'reorder_months', 'expiry_managed']) {
    const v = { ...VALUES }; delete v[k];
    bad({ values: v }, 'invalid_input', k);
  }
  bad({ values: { ...VALUES, name: 'x' } }, 'invalid_input');
  bad({ card: { yahoo: { price: 1 } } }, 'invalid_input', 'card');
});

console.log('\n門');
await ta('[G1] products.parent の DB の active が load = 409 parent_not_company (まとまり・子・選択肢・知らせは何も書かない)・MASTER_EDIT_OPEN が無い = 409 before_cutover', async () => {
  const before = await counts();
  await T.W2.setActiveOwnershipInDb(pg, { ...T.ALL_COMPANY, 'products.parent': 'load' });
  try {
    const e = await rejects(reg(hakamaInput('GateP')), 409, 'parent_not_company');
    assert.match(e.message, /まだ登録できません/);
  } finally { await T.W2.setActiveOwnershipInDb(pg, T.ALL_COMPANY); }
  await rejects(reg(hakamaInput('GateO'), { open: false }), 409, 'before_cutover');
  assert.deepEqual(await counts(), before);
});

console.log('\nまとめての登録');
let HAKAMA = null;
await ta('[B1] 新しいまとまり (2 軸・15 子): 札・軸・選択肢・子 (名前は画面のまま・売価の上書き・JAN)・共通の欄 (原価・税・分類・仕入先・送料・月数・ロジザード)・親・知らせ (共通の欄 = 公式 URL・送料)・記録', async () => {
  const before = await counts();
  const input = hakamaInput();
  const r = await reg(input);
  HAKAMA = r.group_product_id;
  assert.equal(r.ok, true); assert.equal(r.group_code, 'hakama'); assert.equal(r.group_created, true); assert.equal(r.revision, 1);
  assert.equal(r.children.length, 15);
  const kids = await q(`select s.code, s.name, s.standard_price_jpy::int as price, s.tax_rate::float8 as tax, s.shipping_code, s.reorder_months::float8 as months, p.sales_class, p.expiry_managed, p.inbound_date_managed,
      p.parent_product_id::text as par, r.state, (select x.cost_jpy::int from core.sku_costs x where x.sku_id = s.sku_id and x.valid_to is null) as cost,
      (select su.code from core.supplier_skus ss join core.suppliers su on su.supplier_id = ss.supplier_id where ss.sku_id = s.sku_id and ss.is_primary) as sup,
      (select string_agg(e.external_value, ',') from core.external_ids e where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.valid_to is null) as jan
     from core.skus s join core.products p on p.product_id = s.product_id join ops.master_registrations r on r.sku_id = s.sku_id where p.parent_product_id = $1 order by s.code`, [HAKAMA]);
  assert.equal(kids.length, 15);
  const by = Object.fromEntries(kids.map((k) => [k.code, k]));
  assert.deepEqual(by['hakama-WH-90'], { code: 'hakama-WH-90', name: `子ども袴 2点セット【ホワイト】【90cm】【${JAN_A}】`, price: 12800, tax: 0.1, shipping_code: 'S02', months: 3, sales_class: 1,
    expiry_managed: false, inbound_date_managed: false, par: HAKAMA, state: 'draft', cost: 5200, sup: '0001', jan: JAN_A });
  assert.equal(by['hakama-BK-120'].price, 13800);
  assert.equal(by['hakama-NV-110'].name, '子ども袴 2点セット【ネイビー】【110cm】限定柄');
  assert.equal(by['hakama-EN-100'].jan, null);
  assert.ok(!by['hakama-EN-90']);
  assert.deepEqual(await q('select axis, code, name from core.variation_options where group_product_id = $1 order by axis, sort', [HAKAMA]),
    [...['-WH', '-BK', '-NV', '-EN'].map((c, i) => ({ axis: 1, code: c, name: ['ホワイト', 'ブラック', 'ネイビー', 'エンジ'][i] })), ...['-90', '-100', '-110', '-120'].map((c) => ({ axis: 2, code: c, name: `${c.slice(1)}cm` }))]);
  // まとまりの知らせ = 1 件 (revision 1)・共通の欄 = 公式 URL と送料 (子ごとのカードの知らせは無い)
  const ob = await q('select revision, payload from ops.product_hub_outbox where group_product_id = $1', [HAKAMA]);
  assert.equal(ob.length, 1);
  assert.equal(O.groupSnapshotShapeProblem(ob[0].payload), null);
  assert.deepEqual(ob[0].payload.common, { shipping: { code: 'S02', method: '宅急便', cost_jpy: 520 }, amazon_url: null, asin: null, official_url: 'https://example.com/hakama', reference_urls: [], yahoo: null });
  assert.equal(ob[0].payload.children.length, 15);
  // 記録: open (done) + 子の登録 15 + JAN 2 + close (done)
  const rq = await q(`select operation, count(*)::int as n from ops.master_edit_requests where actor_id = 'naka@test' and started_at >= (select min(created_at) from ops.variation_batches where request_id = $1::uuid) - interval '1 minute' and request_id in
      (select $1::uuid union all select ops.variation_sub_request_id($1::uuid, 'close') union all select ops.variation_sub_request_id($1::uuid, 'child:' || lower(c)) from unnest($2::text[]) c
       union all select ops.variation_sub_request_id($1::uuid, 'jan:' || lower(c)) from unnest($2::text[]) c) group by 1 order by 1`, [r.request_id, input.children.map((k) => k.code)]);
  assert.deepEqual(rq, [{ operation: 'jan_edit', n: 2 }, { operation: 'sku_create', n: 15 }, { operation: 'variation_batch_close', n: 1 }, { operation: 'variation_batch_open', n: 1 }]);
  const after = await counts();
  assert.deepEqual([after.products - before.products, after.skus - before.skus, after.options - before.options, after.choices - before.choices, after.outbox - before.outbox, after.jans - before.jans], [16, 15, 8, 15, 1, 2]);
});

await ta('[R1] 同じ request_id = 前の答え (replayed・何も足さない)・違う中身 / 違う人 = 409 request_id_reused', async () => {
  const rid = uuid();
  const input = { ...hakamaInput('Rp1'), requestId: rid };
  const a = await reg(input);
  const before = await counts();
  const b = await reg(input);
  assert.equal(b.replayed, true);
  assert.equal(b.group_product_id, a.group_product_id);
  assert.deepEqual(await counts(), before);
  await rejects(reg({ ...input, children: input.children.slice(1) }), 409, 'request_id_reused');
  await rejects(reg({ ...input, actor: 'other@test' }), 409, 'request_id_reused');
  assert.deepEqual(await counts(), before);
});

await ta('[R2] 0069 (#1679 Codex R1 Medium 1): 同じ request_id で名前・売価・原価・JAN・カードの欄・理由だけを変えた押し直し = 409 request_id_reused (前の答えにしない・何も書かない)・要求の全部のハッシュを同じ取引で残す', async () => {
  const rid = uuid();
  const input = { ...hakamaInput('Rp2'), requestId: rid, reason: '最初の理由' };
  await reg(input);
  const rec = await one('select request_hash, actor_id from ops.variation_batch_requests where request_id = $1', [rid]);
  assert.deepEqual(rec, { request_hash: V.variationPayloadHashOf(V.parseVariationRequest({ actor: 'naka@test', ...input })), actor_id: 'naka@test' });
  const before = await counts();
  const kid0 = input.children[0];
  const variants = [
    ['名前', { children: [{ ...kid0, name: `${kid0.name} 改` }, ...input.children.slice(1)] }],
    ['売価', { children: [{ ...kid0, price: '19800' }, ...input.children.slice(1)] }],
    ['原価', { children: [{ ...kid0, cost: '777' }, ...input.children.slice(1)] }],
    ['JAN', { children: [{ ...kid0, jan: jan13('458012399990') }, ...input.children.slice(1)] }],
    ['カード', { card: { official_url: 'https://example.com/other' } }],
    ['理由', { reason: '違う理由' }],
    ['共通の売価', { values: { ...VALUES, standard_price: '9999' } }],
  ];
  for (const [what, patch] of variants) {
    const e = await rejects(reg({ ...input, ...patch }), 409, 'request_id_reused');
    assert.match(e.message, /違う中身/, what);
  }
  assert.deepEqual(await counts(), before);
  // 同じ中身 = 前の答え
  const again = await reg(input);
  assert.equal(again.replayed, true);
  // 記録の関数: この取引で開いたまとめての登録が無い = no_open_batch (画面のロールが直接呼んでも書けない)
  const e2 = await errOf(asEditor(() => pg.query('select ops.variation_batch_record_request($1::uuid, $2, $3)', [rid, 'naka@test', 'a'.repeat(64)])));
  assert.match(String(e2?.message), /^no_open_batch/);
  const e3 = await errOf(asEditor(() => pg.query(`insert into ops.variation_batch_requests (request_id, actor_id, request_hash) values ($1, 'x', $2)`, [uuid(), 'a'.repeat(64)])));
  assert.ok(e3, '画面のロールは表に直接書けない');
});

await ta('[R3] 0069: 成功した後の同じ要求の押し直しは、門・product-hub の下書き・仕入先を見ずに残した答え (自分の保存で作られたカード・閉じた門でも失敗しない)', async () => {
  const rid = uuid();
  const input = { ...hakamaInput('Rp3'), requestId: rid };
  const a = await reg(input, { phDraftExists: async () => null });
  // 自分の保存で product-hub にカードができた (同じ管理番号) = 新しい要求なら止まるが、押し直しは前の答え
  const b = await reg(input, { phDraftExists: async (n) => (n === 'rp3' ? 99 : null) });
  assert.deepEqual([b.replayed, b.group_product_id, b.children.length], [true, a.group_product_id, 15]);
  await rejects(reg({ ...input, requestId: uuid(), group: { mode: 'new', code: 'Rp3x', name: 'x' }, children: hakamaInput('Rp3x').children }, { phDraftExists: async () => 99 }), 409, 'ph_draft_exists');
  // 門が閉じた後 (MASTER_EDIT_OPEN なし・products.parent が load)
  assert.equal((await reg(input, { open: false })).replayed, true);
  await T.W2.setActiveOwnershipInDb(pg, { ...T.ALL_COMPANY, 'products.parent': 'load' });
  try { assert.equal((await reg(input)).replayed, true); } finally { await T.W2.setActiveOwnershipInDb(pg, T.ALL_COMPANY); }
});

await ta('[F1] 1 つでも断られたら全部巻き戻す: 子のコードがもうある (code_taken・どの子か)・同じ request_id の押し直し = 同じ誤り (replayed)・JAN がほかの商品 (jan_taken)・まとまりのコードがある (group_exists)', async () => {
  // 先に単品 Zt-WH-90 を登録しておく (同じコードの子を作れない)
  await asEditor(() => pg.query('select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r', [uuid(), 'naka@test', null, T.OWN, 'e'.repeat(64), JSON.stringify({
    kind: 'single', code: 'Zt-WH-90', started_at: null, product: { name: '前からある', sales_class: 3, expiry_managed: false, inbound_date_managed: null },
    sku: { name: '前からある', tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', standard_price_jpy: 1000, shipping_code: null, shipping_method: null, shipping_cost_jpy: null, reorder_months: 1, set_sales_class_override: null, handling_own: null },
    supplier_id: T.SUP1, cost: null, component_request: null, card: null })]));
  const before = await counts();
  const rid = uuid();
  const input = { ...hakamaInput('Zt'), requestId: rid };
  const e = await rejects(reg(input), 409, 'code_taken');
  assert.equal(e.extra.code, 'Zt-WH-90');
  assert.equal(e.extra.field, 'children');
  assert.deepEqual(await counts(), before);
  const fail = await one('select operation, status, error from ops.master_edit_requests where request_id = $1', [rid]);
  assert.deepEqual([fail.operation, fail.status, fail.error.reason], ['variation_batch_open', 'failed', 'code_taken']);
  const e2 = await rejects(reg(input), 409, 'code_taken');
  assert.equal(e2.extra.replayed, true);
  await rejects(reg({ ...input, reason: '違う中身' }), 409, 'request_id_reused');
  // JAN がほかの商品の有効な JAN (hakama-WH-90 の JAN_A)
  const j = hakamaInput('Zj');
  j.children = j.children.map((k) => ({ ...k, jan: k.code === 'Zj-BK-90' ? JAN_A : '' }));
  const ej = await rejects(reg(j), 409, 'jan_taken');
  assert.equal(ej.extra.code, 'Zj-BK-90');
  // まとまりのコードが今あるまとまり (大文字小文字を問わず)
  const eg = await rejects(reg(hakamaInput('HAKAMA')), 409, 'group_exists');
  assert.equal(eg.extra.field, 'group.code');
  await rejects(reg(hakamaInput('WS100')), 409, 'group_exists');
  assert.deepEqual(await counts(), before);
});

console.log('\n今あるまとまりに足す');
await ta('[A1] 記録のあるまとまり (hakama): 足す色 (ベージュ) × 今のサイズ・前回作らなかった組み合わせ (エンジ × 90) を作る・今ある組み合わせ = choice_exists・今ある文字 = option_exists・軸は送らない', async () => {
  const kids = [...['-90', '-100', '-110', '-120'].map((vc) => ({ code: `hakama-BE${vc}`, choices: { 1: '-BE', 2: vc }, name: `子ども袴 2点セット【ベージュ】【${vc.slice(1)}cm】` })),
    { code: 'hakama-EN-90', choices: { 1: '-EN', 2: '-90' }, name: '子ども袴 2点セット【エンジ】【90cm】' }];
  const r = await reg({ group: { mode: 'add', product_id: HAKAMA }, options: [{ axis: 1, code: '-BE', name: 'ベージュ' }], children: kids, values: { ...VALUES, standard_price: '12800' } });
  assert.equal(r.group_created, false); assert.equal(r.revision, 2); assert.equal(r.children.length, 5);
  assert.equal((await one('select count(*)::int as n from core.products where parent_product_id = $1', [HAKAMA])).n, 20);
  await rejects(reg({ group: { mode: 'add', product_id: HAKAMA }, options: [], children: [{ code: 'hakama-WH-90', choices: { 1: '-WH', 2: '-90' }, name: 'x' }], values: VALUES }), 409, 'choice_exists');
  await rejects(reg({ group: { mode: 'add', product_id: HAKAMA }, options: [{ axis: 1, code: '-wh', name: '白' }], children: [{ code: 'hakama-wh-90', choices: { 1: '-wh', 2: '-90' }, name: 'x' }], values: VALUES }), 409, 'option_exists');
  // 記録のあるまとまりに違う軸を送る = axes_fixed (画面は送らない)
  await rejects(reg({ group: { mode: 'add', product_id: HAKAMA }, axes: [{ axis: 1, name: '色' }, { axis: 2, name: 'サイズ' }], options: [{ axis: 1, code: '-GR', name: 'グリーン' }],
    children: [{ code: 'hakama-GR-90', choices: { 1: '-GR', 2: '-90' }, name: 'x' }], values: VALUES }), 409, 'axes_fixed');
});

await ta('[A2] NE で作ったまとまり (ws100・軸の記録なし) の最初の 1 回: 前からある子の選択肢 (-BR ブラウン・-NV ネイビー・-GY グレー) も記録・足すのはワインレッドだけ・前からある子は増えない / 2 回目は記録のあるまとまり', async () => {
  const gid = (await one(`select product_id::text as id from core.products where display_code = 'ws100'`)).id;
  const g0 = await V.readVariationGroup(db, gid);
  assert.deepEqual([g0.kind, g0.axes, g0.children.map((k) => k.code)], ['tag', [], ['ws100-BR', 'ws100-GY', 'ws100-NV']]);
  const r = await reg({ group: { mode: 'add', product_id: gid }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-BR', name: 'ブラウン' }, { axis: 1, code: '-NV', name: 'ネイビー' }, { axis: 1, code: '-GY', name: 'グレー' }, { axis: 1, code: '-WR', name: 'ワインレッド' }],
    children: [{ code: 'ws100-WR', choices: { 1: '-WR' }, name: 'ウールストール【ワインレッド】' }], values: { ...VALUES, standard_price: '2980' } });
  assert.equal(r.children.length, 1);
  const g1 = await V.readVariationGroup(db, gid);
  assert.deepEqual(g1.axes, [{ axis: 1, name: 'カラー' }]);
  assert.deepEqual(g1.options.map((o) => o.code), ['-BR', '-NV', '-GY', '-WR']);
  assert.deepEqual(g1.children.map((k) => [k.code, k.choices]), [['ws100-BR', null], ['ws100-GY', null], ['ws100-NV', null], ['ws100-WR', { 1: '-WR' }]]);
  // 予約 = load (前からある札)
  assert.equal((await one('select source from ops.variation_group_codes where group_product_id = $1', [gid])).source, 'load');
  // 2 回目: 軸を送らない・前からある文字は使えない
  await rejects(reg({ group: { mode: 'add', product_id: gid }, options: [{ axis: 1, code: '-BR', name: 'ブラウン 2' }], children: [{ code: 'ws100-BR', choices: { 1: '-BR' }, name: 'x' }], values: VALUES }), 409, 'option_exists');
});

await ta('[A3] 単品が代表のまとまり (towel-gift・子 2): 最初の 1 回 = 前からある子の文字 (-2p・-3p) を記録して -5p を足す', async () => {
  const gid = (await one(`select product_id::text as id from core.skus where code = 'towel-gift'`)).id;
  const g0 = await V.readVariationGroup(db, gid);
  assert.deepEqual([g0.kind, g0.code, g0.children.length], ['single', 'towel-gift', 2]);
  const r = await reg({ group: { mode: 'add', product_id: gid }, axes: [{ axis: 1, name: '入数' }],
    options: [{ axis: 1, code: '-2p', name: '2 枚' }, { axis: 1, code: '-3p', name: '3 枚' }, { axis: 1, code: '-5p', name: '5 枚' }],
    children: [{ code: 'towel-gift-5p', choices: { 1: '-5p' }, name: '今治タオル ギフト【5 枚】' }], values: { ...VALUES, standard_price: '5500' } });
  assert.equal(r.group_code, 'towel-gift');
  assert.equal((await one(`select p.parent_product_id::text as par from core.skus s join core.products p on p.product_id = s.product_id where s.code = 'towel-gift-5p'`)).par, gid);
});

console.log('\n探す・読む・確かめる');
await ta('[S1] 探す: コードの前の方・名前の一部・子の商品コード (そのまとまりを出す)・まとまりでない単品は出さない・空 = まとまりを全部 (子の多い順)', async () => {
  const s1 = await V.searchVariationGroups(db, 'ws1');
  assert.deepEqual(s1.map((x) => [x.code, x.kind, x.recorded, x.children]), [['ws100', 'tag', true, 4]]);
  assert.deepEqual((await V.searchVariationGroups(db, 'ストール')).map((x) => x.code), ['ws100']);
  assert.deepEqual((await V.searchVariationGroups(db, 'hakama-BK')).map((x) => x.code), ['hakama']);
  assert.deepEqual(await V.searchVariationGroups(db, 's001'), []);
  const all = await V.searchVariationGroups(db, '');
  assert.ok(['hakama', 'ws100', 'towel-gift', 'Rp1'].every((c) => all.some((x) => x.code === c)), JSON.stringify(all.map((x) => x.code)));
  assert.equal(all[0].code, 'hakama');   // 子の多い順
  assert.deepEqual(await V.searchVariationGroups(db, 'x'.repeat(61)), []);
});

await ta('[S2] 読む: 今ある子の値 (共通の欄の形)・まとまりでない (子の無い単品・子・無い番号) = null', async () => {
  const g = await V.readVariationGroup(db, (await one(`select product_id::text as id from core.products where display_code = 'ws100'`)).id);
  const gy = g.children.find((k) => k.code === 'ws100-GY');
  assert.deepEqual(gy.values, { price: '3280', cost: '1100', tax: '0.1', sales: '3', supplier: '0034', months: '2', expiry: '0', inbound: '', ship: 'S01' });
  assert.equal(g.children.find((k) => k.code === 'ws100-WR').values.expiry, '0');
  assert.equal(await V.readVariationGroup(db, (await one(`select product_id::text as id from core.skus where code = 's001'`)).id), null);
  assert.equal(await V.readVariationGroup(db, (await one(`select product_id::text as id from core.skus where code = 'ws100-BR'`)).id), null);
  assert.equal(await V.readVariationGroup(db, '999999999'), null);
  assert.equal(await V.readVariationGroup(db, 'abc'), null);
});

await ta('[S3] 確かめる: まとまりのコード (形・もうある = その番号・商品のコード・product-hub の下書き)・子のコード (Company DB にある・形)・JAN (持っている商品)', async () => {
  const hk = await V.checkVariationCodes(db, { group_code: 'HAKAMA', codes: ['hakama-WH-90', 'new-ok-1', 'bad code'], jans: [JAN_A, jan13('458099999999'), 'x'] });
  assert.equal(hk.group.problem, 'group_exists');
  assert.equal(hk.group.product_id, HAKAMA);
  assert.deepEqual(Object.keys(hk.codes).sort(), ['bad code', 'hakama-WH-90']);
  assert.equal(hk.codes['hakama-WH-90'].problem, 'code_taken');
  assert.deepEqual(hk.jans, { [JAN_A]: 'hakama-WH-90' });
  assert.equal((await V.checkVariationCodes(db, { group_code: 's001' })).group.problem, 'code_taken');
  assert.equal((await V.checkVariationCodes(db, { group_code: 'set-1' })).group.problem, 'code_shape');
  const ok = await V.checkVariationCodes(db, { group_code: 'fresh-group' }, { phDraftExists: async () => null });
  assert.deepEqual([ok.group.problem, ok.group.message], [null, null]);
  const ph = await V.checkVariationCodes(db, { group_code: 'fresh-group' }, { phDraftExists: async (n) => (n === 'fresh-group' ? 7 : null) });
  assert.equal(ph.group.problem, 'ph_draft_exists');
  assert.match(ph.group.message, /#7/);
  // 登録でも同じ (product-hub の下書きと同じ管理番号 = 止める・何も書かない)
  const before = await counts();
  await rejects(reg(hakamaInput('fresh-group'), { phDraftExists: async () => 7 }), 409, 'ph_draft_exists');
  assert.deepEqual(await counts(), before);
});

console.log('\n商品の画面の保存');
await ta('[E1] 商品の画面の保存 (saveSku) に代表の欄は無い: parent_code = 400 parent_not_editable (何も書かない)・欄の定義にも無い', async () => {
  const id = (await one(`select sku_id::text as id from core.skus where code = 's001'`)).id;
  const token = W.editTokenOf(await W.readCurrent(db, id, W.jstDate(new Date())));
  const e = await errOf(asEditor(() => W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 's001', seen: { token }, values: { parent_code: 'hakama' } }, { open: true })));
  assert.deepEqual([e.status, e.reason, e.extra.field], [400, 'parent_not_editable', 'parent_code']);
  assert.equal((await one(`select p.parent_product_id from core.skus s join core.products p on p.product_id = s.product_id where s.code = 's001'`)).parent_product_id, null);
  assert.ok(!Object.hasOwn(W.SINGLE_FIELDS, 'parent_code'));
  assert.ok(!W.REG_CSV_FIELDS.single.includes('parent_code'));
  // ほかの欄は今までどおり保存できる
  const r = await asEditor(() => W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 's001', seen: { token }, values: { reorder_months: '4' } }, { open: true }));
  assert.equal(r.ok, true);
});

console.log('\nNE 登録の CSV');
await ta('[C1] まとまりの子は回ごとに並ぶ (ふつうの候補には出さない)・回ごとに 1 ファイル (ne-reg-variation-v1・代表商品コード = まとまりのコード・JAN = empty)・止まる子が居る回は作れない', async () => {
  const s0 = await asEditor(() => G.regSummary(db, { nowMs: NOW_MS }));
  assert.ok(!s0.candidates.some((c) => /^hakama-/.test(c.code)), 'まとまりの子はふつうの候補に出さない');
  const hk = s0.variation.find((g) => g.group_code === 'hakama');
  assert.deepEqual(hk.rounds.map((r) => [r.no, r.kids.length, r.pending]), [[1, 15, 15], [2, 5, 5]]);
  assert.equal(hk.pending_rounds, 2);
  // 2 つの回がどちらもまだ = 回ごとには作れない (DB の決まり = まとまりの NE 登録待ちの子は全部 1 つのファイル)
  const e = await errOf(asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', variation: true, codes: hk.rounds[0].codes, requestId: uuid() }, { open: true, nowMs: NOW_MS })));
  assert.match(String(e && e.message), /variation_incomplete|まとまりで 1 ファイル/);
  // まとめて 1 ファイル = 作れる
  const all = hk.rounds.flatMap((r) => r.codes);
  const b = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', variation: true, codes: all, requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  assert.equal(b.export.schema_version, 'ne-reg-variation-v1');
  const rows = await q('select r.cells from ops.ne_reg_export_rows r where r.export_id = $1 order by r.row_no', [b.export.export_id]);
  assert.equal(rows.length, 20);
  assert.ok(rows.every((x) => x.cells[7] === 'hakama' && x.cells[8] === 'empty'), JSON.stringify(rows[0]));
  // 回ごとに 1 ファイル: 新しいまとまり (1 回目を保存 → 1 回目のファイル → 2 回目を保存 → 2 回目のファイル)
  const one1 = { group: { mode: 'new', code: 'roomwear', name: 'ルームウェア' }, axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-WH', name: 'ホワイト' }],
    children: [{ code: 'roomwear-WH', choices: { 1: '-WH' }, name: 'ルームウェア【ホワイト】' }], values: VALUES };
  const r1 = await reg(one1);
  const s1 = await asEditor(() => G.regSummary(db, { nowMs: NOW_MS }));
  const rw1 = s1.variation.find((g) => g.group_code === 'roomwear');
  assert.deepEqual([rw1.pending_rounds, rw1.rounds[0].buildable, rw1.rounds[0].codes], [1, true, ['roomwear-WH']]);
  const f1 = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', variation: true, codes: rw1.rounds[0].codes, requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  await reg({ group: { mode: 'add', product_id: r1.group_product_id }, options: [{ axis: 1, code: '-BK', name: 'ブラック' }], children: [{ code: 'roomwear-BK', choices: { 1: '-BK' }, name: 'ルームウェア【ブラック】' }], values: VALUES });
  const s2 = await asEditor(() => G.regSummary(db, { nowMs: NOW_MS }));
  const rw2 = s2.variation.find((g) => g.group_code === 'roomwear');
  assert.deepEqual(rw2.rounds.map((r) => [r.no, r.pending]), [[1, 0], [2, 1]]);
  assert.equal(rw2.pending_rounds, 1);
  const f2 = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', variation: true, codes: rw2.rounds[1].codes, requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  assert.notEqual(f1.export.export_id, f2.export.export_id);
  assert.deepEqual([f1.export.item_count, f2.export.item_count], [1, 1]);
  // 単品の版にはまとまりの子を入れない (DB)
  const r3 = await reg({ group: { mode: 'add', product_id: r1.group_product_id }, options: [{ axis: 1, code: '-NV', name: 'ネイビー' }], children: [{ code: 'roomwear-NV', choices: { 1: '-NV' }, name: 'ルームウェア【ネイビー】' }], values: VALUES });
  assert.equal(r3.ok, true);
  const e3 = await errOf(asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', codes: ['roomwear-NV'], requestId: uuid() }, { open: true, nowMs: NOW_MS })));
  assert.match(String(e3 && e3.message), /variation_file_required|ne-reg-variation-v1/);
  // 止まる子 (原価が無い) が居る回 = 作れない (画面のボタンは押せない・どの子の何かを出す)
  const { cost: _c, ...noCost } = VALUES;
  await reg({ group: { mode: 'new', code: 'nocost', name: '原価なし' }, axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-A', name: 'A' }, { axis: 1, code: '-B', name: 'B' }],
    children: [{ code: 'nocost-A', choices: { 1: '-A' }, name: '原価なし【A】', cost: '300' }, { code: 'nocost-B', choices: { 1: '-B' }, name: '原価なし【B】' }], values: noCost });
  const s4 = await asEditor(() => G.regSummary(db, { nowMs: NOW_MS }));
  const nc = s4.variation.find((g) => g.group_code === 'nocost');
  assert.equal(nc.rounds[0].buildable, false);
  assert.deepEqual(nc.rounds[0].kids.map((k) => [k.code, k.blockers.some((b) => /原価が無い/.test(b))]), [['nocost-A', false], ['nocost-B', true]]);
});

console.log(`\n${passed} ok`);
