#!/usr/bin/env node
/**
 * test-company-db-finance.mjs — 0012 (Amazon 財務の受け皿) の受入試験 (Company DB 構想 08 §4.4 / §4.7。F2 の DDL 部分)
 *
 * PGlite で 0001〜0012 を流し、DDL の「歯止め」を実際の操作で確かめる:
 *   policy の期間は重ならない (同じ 会社 × モール × scope) / 版の境目 (11/1) は隣接なら可 / scope が違えば可 / update でも検査 / period_to > from /
 *   policy の欠落 (finance_policy_gaps) を検出し、assert は例外 / 未採用の行は v_order_finance_uncovered に出る /
 *   集約は JPY だけ・net は 19 列の合計 (符号は決済レポートのまま)・source_lines > 0・会社違いの SKU / 出品は付かない /
 *   apply_order_finance_batch = 注文の明細集合を丸ごと置換 (古い世代は拒む・同じ内容は世代だけ進む・同じ世代で内容違いは例外・空の集合でも受領状態が残る) /
 *   注文の累計 view は policy が指す source の行だけ (旧と V2 の両方が入っていても二重にならない)・全内訳を出す・'-' の費用行は入らない /
 *   finance_daily は旧表と同じ列 (絶対値の列は負を拒む)・listing / sku / seller_sku のどれが null でも主キーが組める・JPY だけ
 * 🚨 policy の重複検査・受領行の for update の 2 接続の並行は PGlite では書けない → 本番で手で確かめる (08 §7.6)
 * 実行: node scripts/test-company-db-finance.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`); return threw; };
const quiet = () => {};

const pg = new PGlite();
const db = pgliteAdapter(pg);
const applied = await applyMigrations(db, { log: quiet });
assert.ok(applied.applied.includes('0012'), '0012 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
const co = (await one(`select company_id from core.companies order by company_id limit 1`)).company_id;
await pg.query(`insert into core.products (company_id, name) values ($1, '見本A')`, [co]);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'sku-a', name from core.products`);
const sku = (await one(`select sku_id from core.skus where code = 'sku-a'`)).sku_id;
const listing = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values ($1, 'amazon', 'main@A1VC38T7YXB528', 'pr_SKU-A', 'active') returning listing_id`, [co])).listing_id;
const other = (await one(`select max(company_id)::smallint + 1 as c from core.companies`)).c;
await pg.query(`insert into core.companies (company_id, name, kind) values ($1, 'other', 'subsidiary')`, [other]);
await pg.query(`insert into core.products (company_id, name) values ($1, '他社品')`, [other]);
const skuOther = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'other-1', name from core.products where company_id = $1 returning sku_id`, [other])).sku_id;
const listingOther = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values ($1, 'amazon', 'other@X', 'pr_OTHER-1', 'active') returning listing_id`, [other])).listing_id;

const policy = (mall, scope, from, to, source) => pg.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, period_to, source) values ($1, $2, $3, $4, $5, $6)`, [co, mall, scope, from, to, source]);
/** 注文の明細集合を 1 世代ぶん適用 (関数経由) */
const apply = (order, seq, checksum, rows, { scope = 'jp', company = co, mall = 'amazon', version = 'v1' } = {}) =>
  one(`select core.apply_order_finance_batch($1::smallint, $2, $3, $4, $5::bigint, $6, $7, $8::jsonb) as r`, [company, mall, scope, order, seq, checksum, version, JSON.stringify(rows)]).then((r) => r.r);
const line = (date, source, x = {}) => ({ economic_date_jst: date, seller_sku: 'pr_SKU-A', source, source_lines: 1, source_updated_at: `${date}T00:00:00Z`, content_hash: 'h', ...x });
const gaps = (from, to, scope = 'jp') => pg.query(`select gap_from::text f, gap_to::text t from core.finance_policy_gaps($1::smallint, 'amazon', $2, $3::date, $4::date)`, [co, scope, from, to]).then((r) => r.rows.map((g) => `${g.f}..${g.t}`));

console.log('0012: 表と view');
await t('表・view・関数・trigger がある', async () => {
  const tables = (await pg.query(`select table_schema || '.' || table_name as t from information_schema.tables where table_schema in ('core','mart') and table_type = 'BASE TABLE' and table_name in ('finance_source_policy','order_finance_receipts','order_finance_daily','finance_daily') order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(tables, ['core.finance_source_policy', 'core.order_finance_daily', 'core.order_finance_receipts', 'mart.finance_daily']);
  const views = (await pg.query(`select table_name as t from information_schema.views where table_schema = 'mart' and (table_name like 'v_order_finance%' or table_name = 'v_finance_daily_legacy') order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(views, ['v_finance_daily_legacy', 'v_order_finance_summary', 'v_order_finance_uncovered']);
  assert.equal(await num(`select count(*) as n from pg_proc where proname in ('finance_policy_gaps','assert_finance_policy_covered','apply_order_finance_batch','check_finance_policy_overlap')`), 4);
  assert.equal(await num(`select count(*) as n from pg_trigger where tgname = 'trg_finance_source_policy_overlap'`), 1);
});

console.log('policy (採用する取得元)');
await t('🚨 期間 [from, to) は同じ 会社 × モール × scope で重ならない。版の境目 (10/31 まで v1 → 11/1 から V2) は隣接なので可、scope が違えば可', async () => {
  await policy('amazon', 'jp', '2025-01-01', '2026-11-01', 'amazon_settlement_flat_v1');
  await policy('amazon', 'jp', '2026-11-01', null, 'amazon_settlement_flat_v2');
  await rejects(() => policy('amazon', 'jp', '2026-10-01', '2026-12-01', 'amazon_finances_api'), /overlaps/);
  await rejects(() => policy('amazon', 'jp', '2024-01-01', '2025-01-02', 'amazon_finances_api'), /overlaps/);   // 端が 1 日でも重なる
  await rejects(() => policy('amazon', 'jp', '2027-01-01', null, 'amazon_finances_api'), /overlaps/);           // 開いた期間 (to = null) とも重なる
  await policy('amazon', 'us', '2026-10-01', null, 'amazon_finances_api');
  await policy('rakuten', 'jp', '2025-01-01', null, 'mall_finance_daily_v1');
  assert.equal(await num(`select count(*) as n from core.finance_source_policy`), 4);
});
await t('update でも重複を検査する (境目を動かす順は「縮める → 伸ばす」)。period_to > from。source は決まった値だけ', async () => {
  await rejects(() => pg.query(`update core.finance_source_policy set period_from = date '2026-10-01' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`), /overlaps/);
  await pg.query(`update core.finance_source_policy set period_to = date '2026-10-01' where source = 'amazon_settlement_flat_v1' and scope_key = 'jp'`);
  await pg.query(`update core.finance_source_policy set period_from = date '2026-10-01' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`);
  await pg.query(`update core.finance_source_policy set period_from = date '2026-11-01' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`);
  await pg.query(`update core.finance_source_policy set period_to = date '2026-11-01' where source = 'amazon_settlement_flat_v1' and scope_key = 'jp'`);
  await rejects(() => policy('amazon', 'jp2', '2026-01-01', '2026-01-01', 'amazon_settlement_flat_v1'), /ck_finance_source_policy_period/);
  await rejects(() => policy('amazon', 'jp2', '2026-01-01', null, 'settlement'), /finance_source_policy_source_check|violates check/);
});
await t('🚨 policy の欠落: gaps は無い区間を返す (前・間・後・開いた期間・全部無し)。assert は例外、揃っていれば通る', async () => {
  assert.deepEqual(await gaps('2026-01-01', '2026-12-31'), []);                                   // jp は 2025-01-01〜 v1、11/1〜 V2 (開いた期間)
  assert.deepEqual(await gaps('2024-12-01', '2025-01-10'), ['2024-12-01..2025-01-01']);            // 前に無い
  assert.deepEqual(await gaps('2026-09-01', '2026-12-01', 'us'), ['2026-09-01..2026-10-01']);      // us は 10/1〜 だけ
  assert.deepEqual(await gaps('2026-01-01', '2026-02-01', 'none'), ['2026-01-01..2026-02-01']);    // 全部無し
  await policy('amazon', 'gap', '2026-01-01', '2026-02-01', 'amazon_settlement_flat_v1');
  await policy('amazon', 'gap', '2026-03-01', '2026-04-01', 'amazon_settlement_flat_v2');
  assert.deepEqual(await gaps('2025-12-15', '2026-05-01', 'gap'), ['2025-12-15..2026-01-01', '2026-02-01..2026-03-01', '2026-04-01..2026-05-01']);   // 間にも無い
  assert.deepEqual(await gaps('2026-01-10', '2026-01-20', 'gap'), []);
  assert.deepEqual(await gaps('2026-02-01', '2026-01-01', 'gap'), []);                               // 空の窓
  await rejects(() => pg.query(`select core.assert_finance_policy_covered($1::smallint, 'amazon', 'gap', date '2026-01-01', date '2026-03-15')`, [co]), /gap \[2026-02-01, 2026-03-01\)/);
  await pg.query(`select core.assert_finance_policy_covered($1::smallint, 'amazon', 'jp', date '2025-01-01', date '2027-12-31')`, [co]);
});

console.log('apply_order_finance_batch (§4.7 の契約) と order_finance_daily');
await t('🚨 適用: 受領状態 + 明細行。古い世代は stale (何も変えない)。同じ内容は世代だけ進む (same、行は変わらない)。同じ世代で内容が違えば例外', async () => {
  const rows1 = [line('2026-10-31', 'amazon_settlement_flat_v1', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000, sales_tax_jpy: 100, commission_jpy: -150, fba_fulfillment_jpy: -300 })];
  assert.equal(await apply('503-1', 10, 'c1', rows1), 'applied');
  const rc = await one(`select received_batch_seq, set_checksum, lines from core.order_finance_receipts where mall_order_no = '503-1'`);
  assert.equal(Number(rc.received_batch_seq), 10); assert.equal(rc.set_checksum, 'c1'); assert.equal(rc.lines, 1);
  const r = await one(`select net_jpy, listing_id, received_batch_seq, built_at from core.order_finance_daily where mall_order_no = '503-1'`);
  assert.equal(Number(r.net_jpy), 650); assert.equal(r.listing_id, listing); assert.equal(Number(r.received_batch_seq), 10);   // net は自動計算、listing は seller_sku から
  assert.equal(await apply('503-1', 9, 'c0', [line('2026-10-31', 'amazon_settlement_flat_v1', { sales_principal_jpy: 1 })]), 'stale');
  assert.equal(Number((await one(`select net_jpy from core.order_finance_daily where mall_order_no = '503-1'`)).net_jpy), 650);
  assert.equal(await apply('503-1', 11, 'c1', rows1), 'same');
  const r2 = await one(`select net_jpy, received_batch_seq, built_at from core.order_finance_daily where mall_order_no = '503-1'`);
  assert.equal(Number(r2.received_batch_seq), 11); assert.equal(String(r2.built_at), String(r.built_at));   // 世代だけ進む
  assert.equal(Number((await one(`select received_batch_seq from core.order_finance_receipts where mall_order_no = '503-1'`)).received_batch_seq), 11);
  await rejects(() => apply('503-1', 11, 'c9', [line('2026-10-31', 'amazon_settlement_flat_v1', { sales_principal_jpy: 1 })]), /already applied with a different checksum/);
});
await t('🚨 新しい世代は明細集合を丸ごと置換 (消えた計上日の行は消える)。空の集合でも受領状態 (世代) は残り、その後の古い世代は拒まれる', async () => {
  const rows2 = [
    line('2026-10-31', 'amazon_settlement_flat_v1', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000, sales_tax_jpy: 100, commission_jpy: -150, fba_fulfillment_jpy: -300 }),
    line('2026-11-02', 'amazon_settlement_flat_v2', { units_refunded_customer: 1, units_net_sold: -1, refund_principal_jpy: -200, sales_giftwrap_jpy: 30 }),
    line('2026-11-02', 'amazon_settlement_flat_v1', { refund_principal_jpy: -999 }),   // 11 月は V2 が採用 → 累計に入らない (R3 #6 の筋書き)
  ];
  assert.equal(await apply('503-1', 12, 'c2', rows2), 'applied');
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '503-1'`), 3);
  assert.equal(await apply('503-2', 5, 'c5', [line('2026-10-20', 'amazon_settlement_flat_v1', { sales_principal_jpy: 500 })]), 'applied');
  assert.equal(await apply('503-2', 6, 'empty', []), 'applied');   // 明細が全部消えた
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '503-2'`), 0);
  const rc = await one(`select received_batch_seq, lines from core.order_finance_receipts where mall_order_no = '503-2'`);
  assert.equal(Number(rc.received_batch_seq), 6); assert.equal(rc.lines, 0);
  assert.equal(await apply('503-2', 5, 'c5', [line('2026-10-20', 'amazon_settlement_flat_v1', { sales_principal_jpy: 500 })]), 'stale');   // 遅れて届いた古い世代は復活しない
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '503-2'`), 0);
});
await t('apply の入力検査: 配列でない / 世代 0 / 違う注文番号の行 / net の検算違い (CHECK) / source_lines 0 / 決まっていない source → 例外で何も変わらない', async () => {
  const before = await num(`select count(*) as n from core.order_finance_daily`);
  await rejects(() => one(`select core.apply_order_finance_batch($1::smallint, 'amazon', 'jp', 'x-1', 1, 'c', 'v1', '{}'::jsonb)`, [co]), /json array/);
  await rejects(() => apply('x-1', 0, 'c', []), /positive/);
  await rejects(() => apply('x-1', 1, 'c', [line('2026-10-01', 'amazon_settlement_flat_v1', { mall_order_no: 'x-2' })]), /different mall_order_no/);
  await rejects(() => apply('x-1', 1, 'c', [line('2026-10-01', 'amazon_settlement_flat_v1', { sales_principal_jpy: 100, net_jpy: 99 })]), /ck_order_finance_daily_net/);
  await rejects(() => apply('x-1', 1, 'c', [line('2026-10-01', 'amazon_settlement_flat_v1', { source_lines: 0 })]), /source_lines/);
  await rejects(() => apply('x-1', 1, 'c', [line('2026-10-01', 'settlement')]), /source_check|violates check/);
  assert.equal(await num(`select count(*) as n from core.order_finance_daily`), before);
  assert.equal(await num(`select count(*) as n from core.order_finance_receipts where mall_order_no = 'x-1'`), 0);   // 受領行も残らない (同じ取引で巻き戻る)
});
await t('JPY だけ。受領状態の無い注文の行は入らない (FK)。会社違いの SKU / 出品は付かない (複合 FK)。同じ会社の SKU・出品は付く', async () => {
  const cols = 'company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash';
  await rejects(() => pg.query(`insert into core.order_finance_daily (${cols}, currency) values ($1, 'amazon', 'jp', '503-1', current_date, 'X', 'amazon_settlement_flat_v1', 1, 12, now(), 'v1', 'h', 'USD')`, [co]), /currency/);
  await rejects(() => pg.query(`insert into core.order_finance_daily (${cols}) values ($1, 'amazon', 'jp', 'no-receipt', current_date, 'X', 'amazon_settlement_flat_v1', 1, 1, now(), 'v1', 'h')`, [co]), /foreign key|violates/i);
  await rejects(() => pg.query(`insert into core.order_finance_daily (${cols}, sku_id) values ($1, 'amazon', 'jp', '503-1', date '2026-12-01', 'X', 'amazon_settlement_flat_v1', 1, 12, now(), 'v1', 'h', $2)`, [co, skuOther]), /foreign key|violates/i);
  await rejects(() => pg.query(`insert into core.order_finance_daily (${cols}, listing_id) values ($1, 'amazon', 'jp', '503-1', date '2026-12-01', 'X', 'amazon_settlement_flat_v1', 1, 12, now(), 'v1', 'h', $2)`, [co, listingOther]), /foreign key|violates/i);
  await pg.query(`insert into core.order_finance_daily (${cols}, sku_id, listing_id) values ($1, 'amazon', 'jp', '503-1', date '2026-12-01', 'X', 'amazon_settlement_flat_v1', 1, 12, now(), 'v1', 'h', $2, $3)`, [co, sku, listing]);
  await pg.query(`delete from core.order_finance_daily where mall_order_no = '503-1' and seller_sku = 'X'`);
});

console.log('mart.v_order_finance_summary / v_order_finance_uncovered');
await t('🚨 累計は policy が指す source の行だけ: 10/31 は v1、11/2 は V2 (v1 の 11/2 の行は入らない)。全内訳が出て、内訳の合計 = net。ギフト代も出る', async () => {
  const s = await one(`select *, first_economic_date_jst::text as f, last_economic_date_jst::text as l from mart.v_order_finance_summary where mall_order_no = '503-1'`);
  assert.equal(Number(s.lines), 2); assert.equal(Number(s.units_net_sold), 0); assert.equal(Number(s.units_ordered), 1); assert.equal(Number(s.units_refunded_customer), 1);
  assert.equal(Number(s.sales_principal_jpy), 1000); assert.equal(Number(s.sales_giftwrap_jpy), 30); assert.equal(Number(s.refund_principal_jpy), -200); assert.equal(Number(s.commission_jpy), -150);
  assert.equal(Number(s.net_jpy), 480); assert.equal(s.f, '2026-10-31'); assert.equal(s.l, '2026-11-02');
  const parts = ['sales_principal_jpy', 'sales_shipping_jpy', 'sales_giftwrap_jpy', 'sales_tax_jpy', 'commission_jpy', 'fba_fulfillment_jpy', 'fba_storage_jpy', 'closing_fee_jpy', 'shipping_chargeback_jpy', 'giftwrap_chargeback_jpy', 'promotion_jpy', 'warehouse_damage_jpy', 'warehouse_lost_jpy', 'safe_t_jpy', 'refund_principal_jpy', 'reversal_reimbursement_jpy', 'misc_fee_jpy', 'other_fee_jpy', 'other_amount_jpy'];
  assert.equal(parts.reduce((a, k) => a + Number(s[k]), 0), Number(s.net_jpy));
  const types = (await pg.query(`select column_name, data_type from information_schema.columns where table_schema = 'mart' and table_name = 'v_order_finance_summary' and column_name like '%_jpy'`)).rows;
  assert.equal(types.length, 20); assert.ok(types.every((c) => c.data_type === 'bigint'), JSON.stringify(types));
});
await t('🚨 policy が無い計上日の行は累計に入らず、v_order_finance_uncovered に出る (黙って落ちない)。policy を足すと消える', async () => {
  await policy('amazon', 'late', '2026-01-01', '2026-11-01', 'amazon_settlement_flat_v1');
  await apply('L-1', 1, 'c', [line('2026-10-31', 'amazon_settlement_flat_v1', { sales_principal_jpy: 300 }), line('2026-11-01', 'amazon_settlement_flat_v1', { sales_principal_jpy: 500 })], { scope: 'late' });
  assert.equal(Number((await one(`select net_jpy from mart.v_order_finance_summary where mall_order_no = 'L-1'`)).net_jpy), 300);
  const u = (await pg.query(`select mall_order_no, economic_date_jst::text d, net_jpy from mart.v_order_finance_uncovered where scope_key = 'late'`)).rows;
  assert.equal(u.length, 1); assert.equal(u[0].d, '2026-11-01'); assert.equal(Number(u[0].net_jpy), 500);
  await rejects(() => pg.query(`select core.assert_finance_policy_covered($1::smallint, 'amazon', 'late', date '2026-10-01', date '2026-12-01')`, [co]), /gap \[2026-11-01, 2026-12-01\)/);
  await policy('amazon', 'late', '2026-11-01', null, 'amazon_settlement_flat_v1');
  assert.equal(await num(`select count(*) as n from mart.v_order_finance_uncovered where scope_key = 'late'`), 0);
  assert.equal(Number((await one(`select net_jpy from mart.v_order_finance_summary where mall_order_no = 'L-1'`)).net_jpy), 800);
});
await t("注文に紐付かない費用 (mall_order_no = '-') は累計 view に出ない。採用されていない source の行は uncovered には出ない (期間の policy はある)", async () => {
  assert.equal(await apply('-', 1, 'fee', [line('2026-10-15', 'amazon_settlement_flat_v1', { seller_sku: '-', fba_storage_jpy: -5000 })]), 'applied');
  assert.equal(await num(`select count(*) as n from mart.v_order_finance_summary where mall_order_no = '-'`), 0);
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '-'`), 1);
  assert.equal(await num(`select count(*) as n from mart.v_order_finance_uncovered where mall_order_no = '503-1'`), 0);   // v1 の 11/2 は「未採用」だが期間の policy はある
});

console.log('mart.finance_daily (日次集計。旧表と同じ列・符号規約)');
await t('旧表 f_amazon_finance_sku_daily_v1 の数量 5 列・金額 19 列が同じ名前である。絶対値の列に負は入らない', async () => {
  const oldCols = ['units_ordered', 'units_refunded_customer', 'units_marketplace_guarantee', 'units_a_to_z_refund', 'units_net_sold',
    'sales_principal_jpy', 'sales_shipping_jpy', 'sales_giftwrap_jpy', 'sales_tax_jpy', 'commission_jpy', 'fba_fulfillment_jpy', 'fba_storage_jpy', 'closing_fee_jpy',
    'shipping_chargeback_jpy', 'giftwrap_chargeback_jpy', 'promotion_jpy', 'warehouse_damage_jpy', 'warehouse_lost_jpy', 'safe_t_jpy', 'refund_principal_jpy',
    'reversal_reimbursement_jpy', 'misc_fee_jpy', 'other_fee_jpy', 'other_amount_jpy'];
  const have = new Set((await pg.query(`select column_name from information_schema.columns where table_schema = 'mart' and table_name = 'finance_daily'`)).rows.map((r) => r.column_name));
  assert.deepEqual(oldCols.filter((c) => !have.has(c)), []);
  const haveOrder = new Set((await pg.query(`select column_name from information_schema.columns where table_schema = 'core' and table_name = 'order_finance_daily'`)).rows.map((r) => r.column_name));
  assert.deepEqual(oldCols.filter((c) => !haveOrder.has(c)), []);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, fba_storage_jpy) values ('r0', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', -1)`, [co]), /ck_finance_daily_abs/);
  await pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, commission_jpy, warehouse_damage_jpy) values ('r0', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', -150, -20)`, [co]);   // commission は正味 (返還だけの日は負)、補填は符号そのまま
});
await t('🚨 旧互換 (legacy_*): 明細単位の ABS 合計を別に持つ。保管料 −100 + 訂正 +40 → 符号つき −60・旧互換 140。返還だけの日の commission は負。legacy_* に負は入らない', async () => {
  await policy('amazon', 'lg', '2026-01-01', null, 'amazon_settlement_flat_v1');
  const rows = [
    line('2026-10-05', 'amazon_settlement_flat_v1', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000, fba_storage_jpy: -60, commission_jpy: -150, promotion_jpy: -80, other_fee_jpy: 5,
      legacy_fba_storage_jpy: 140, legacy_commission_gross_jpy: 150, legacy_promotion_jpy: 80, legacy_other_fee_jpy: 25, legacy_complete: true }),
    line('2026-10-06', 'amazon_settlement_flat_v1', { units_refunded_customer: 1, units_net_sold: -1, refund_principal_jpy: -1000, commission_jpy: 150,
      legacy_refund_principal_customer_jpy: 1000, legacy_refund_commission_jpy: 150, legacy_complete: true }),   // 返還だけの日
  ];
  assert.equal(await apply('LG-1', 1, 'c', rows, { scope: 'lg' }), 'applied');
  const d5 = await one(`select * from mart.v_finance_daily_legacy where scope_key = 'lg' and economic_date_jst = date '2026-10-05'`);
  assert.equal(Number(d5.fba_storage_jpy), 140); assert.equal(Number(d5.commission_jpy), 150); assert.equal(Number(d5.promotion_jpy), 80); assert.equal(Number(d5.other_fee_jpy), 25);
  assert.equal(Number(d5.sales_principal_jpy), 1000); assert.equal(Number(d5.closing_fee_jpy), 0); assert.equal(Number(d5.units_net_sold), 1); assert.equal(Number(d5.source_row_count), 1);
  const d6 = await one(`select commission_jpy, refund_principal_jpy, units_refunded_customer, units_net_sold from mart.v_finance_daily_legacy where scope_key = 'lg' and economic_date_jst = date '2026-10-06'`);
  assert.equal(Number(d6.commission_jpy), -150); assert.equal(Number(d6.refund_principal_jpy), 1000); assert.equal(Number(d6.units_refunded_customer), 1); assert.equal(Number(d6.units_net_sold), -1);
  // 符号つきの真の値はそのまま (net = 合計)
  const s = await one(`select fba_storage_jpy, commission_jpy, net_jpy from mart.v_order_finance_summary where mall_order_no = 'LG-1'`);
  assert.equal(Number(s.fba_storage_jpy), -60); assert.equal(Number(s.commission_jpy), 0); assert.equal(Number(s.net_jpy), 1000 - 60 - 150 - 80 + 5 - 1000 + 150);
  // legacy 列は旧表 (v_finance_daily_legacy) と同じ名前で finance_daily に写せる (旧表の 24 列すべて)
  const legacyCols = (await pg.query(`select column_name from information_schema.columns where table_schema = 'mart' and table_name = 'v_finance_daily_legacy'`)).rows.map((r) => r.column_name);
  const fdCols = new Set((await pg.query(`select column_name from information_schema.columns where table_schema = 'mart' and table_name = 'finance_daily'`)).rows.map((r) => r.column_name));
  assert.deepEqual(legacyCols.filter((c) => !['order_rows', 'legacy_incomplete_rows'].includes(c) && !fdCols.has(c)), []);
  await rejects(() => apply('LG-2', 1, 'c', [line('2026-10-05', 'amazon_settlement_flat_v1', { legacy_fba_storage_jpy: -1 })], { scope: 'lg' }), /legacy_fba_storage_jpy/);
  // policy が指さない source の行は旧互換の日次にも入らない
  await apply('LG-3', 1, 'c', [line('2026-10-05', 'amazon_settlement_flat_v2', { legacy_fba_storage_jpy: 999, legacy_complete: true })], { scope: 'lg' });
  assert.equal(Number((await one(`select fba_storage_jpy from mart.v_finance_daily_legacy where scope_key = 'lg' and economic_date_jst = date '2026-10-05'`)).fba_storage_jpy), 140);
});
await t('🚨 旧 SQL の式を期待値に: 返品数量は「日 × SKU の返金額 ÷ 月の Order 単価」を丸める (注文ごとに丸めない)。SKU 無しの行は旧互換に入らない。source_row_count = Σ source_lines', async () => {
  await policy('amazon', 'old', '2026-01-01', null, 'amazon_settlement_flat_v1');
  const L = (date, x) => line(date, 'amazon_settlement_flat_v1', { legacy_complete: true, ...x });
  // 10 月の Order: 3 注文で 単価 1,000 円 × 各 1 個 (月の単価 = trunc(3000 * 1e6 / 3) = 1,000,000 micro)
  await apply('O-1', 1, 'c', [L('2026-10-01', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000 })], { scope: 'old' });
  await apply('O-2', 1, 'c', [L('2026-10-02', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000 })], { scope: 'old' });
  await apply('O-3', 1, 'c', [L('2026-10-02', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000 })], { scope: 'old' });
  // 10/10: 別注文に 400 円ずつの返金 (customer) + 別注文に 600 円の A-to-z。旧: ROUND(800/1000)=1、ROUND(600/1000)=1。注文ごとなら 0+0+1
  await apply('O-1', 2, 'c2', [L('2026-10-01', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000 }), L('2026-10-10', { refund_principal_jpy: -400, legacy_refund_principal_customer_jpy: 400, source_lines: 2 })], { scope: 'old' });
  await apply('O-2', 2, 'c2', [L('2026-10-02', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000 }), L('2026-10-10', { refund_principal_jpy: -400, legacy_refund_principal_customer_jpy: 400 })], { scope: 'old' });
  await apply('O-3', 2, 'c2', [L('2026-10-02', { units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000 }), L('2026-10-10', { refund_principal_jpy: -600, legacy_refund_principal_atoz_jpy: 600 })], { scope: 'old' });
  // 旧 build と同じ式を JS で (unit_price_micro = trunc(Σ principal_micro / Σ qty)、units = ROUND(refund_micro / unit_price_micro))
  const unitPriceMicro = Math.trunc((3000 * 1e6) / 3);
  const expectCustomer = Math.round((800 * 1e6) / unitPriceMicro), expectAtoz = Math.round((600 * 1e6) / unitPriceMicro);
  assert.equal(expectCustomer, 1); assert.equal(expectAtoz, 1);
  const d = await one(`select * from mart.v_finance_daily_legacy where scope_key = 'old' and economic_date_jst = date '2026-10-10'`);
  assert.equal(Number(d.units_refunded_customer), expectCustomer); assert.equal(Number(d.units_a_to_z_refund), expectAtoz); assert.equal(Number(d.units_marketplace_guarantee), 0);
  assert.equal(Number(d.units_net_sold), 0 - expectCustomer - expectAtoz); assert.equal(Number(d.refund_principal_jpy), 1400); assert.equal(Number(d.source_row_count), 4);   // 2 + 1 + 1
  const d2 = await one(`select units_ordered, units_net_sold, sales_principal_jpy, source_row_count, order_rows from mart.v_finance_daily_legacy where scope_key = 'old' and economic_date_jst = date '2026-10-02'`);
  assert.equal(Number(d2.units_ordered), 2); assert.equal(Number(d2.units_net_sold), 2); assert.equal(Number(d2.sales_principal_jpy), 2000); assert.equal(Number(d2.order_rows), 2);
  // 単価が無い月 (Order が無い) の返金 → 旧は NULL → 0
  await apply('O-9', 1, 'c', [L('2026-11-05', { refund_principal_jpy: -500, legacy_refund_principal_customer_jpy: 500 })], { scope: 'old' });
  assert.equal(Number((await one(`select units_refunded_customer from mart.v_finance_daily_legacy where scope_key = 'old' and economic_date_jst = date '2026-11-05'`)).units_refunded_customer), 0);
  // SKU 無しの費用行は core には残るが旧互換の日次には出ない
  await apply('-', 1, 'fee-old', [L('2026-10-10', { seller_sku: '-', fba_storage_jpy: -5000, legacy_fba_storage_jpy: 5000 })], { scope: 'old' });
  assert.equal(await num(`select count(*) as n from mart.v_finance_daily_legacy where scope_key = 'old' and seller_sku = '-'`), 0);
  assert.equal(Number((await one(`select fba_storage_jpy from mart.v_finance_daily_legacy where scope_key = 'old' and economic_date_jst = date '2026-10-10'`)).fba_storage_jpy), 0);
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where scope_key = 'old' and mall_order_no = '-'`), 1);
});
await t('🚨 legacy_* の未提供は 0 と区別する: legacy_complete=false の行があると legacy_incomplete_rows > 0 で、assert_legacy_complete は例外。apply で JPY 以外の currency は例外', async () => {
  await apply('O-8', 1, 'c', [line('2026-10-20', 'amazon_settlement_flat_v1', { fba_storage_jpy: -100 })], { scope: 'old' });   // legacy 無し (V2 に切り替えた後に省略した想定)
  const d = await one(`select fba_storage_jpy, legacy_incomplete_rows from mart.v_finance_daily_legacy where scope_key = 'old' and economic_date_jst = date '2026-10-20'`);
  assert.equal(Number(d.fba_storage_jpy), 0); assert.equal(Number(d.legacy_incomplete_rows), 1);   // 0 に見えるが「未提供」と分かる
  await rejects(() => pg.query(`select core.assert_legacy_complete($1::smallint, 'amazon', 'old', date '2026-10-01', date '2026-11-01')`, [co]), /1 adopted rows .*legacy_complete = false/);
  await pg.query(`select core.assert_legacy_complete($1::smallint, 'amazon', 'old', date '2026-11-01', date '2026-12-01')`, [co]);   // 11 月は揃っている
  await pg.query(`select core.assert_legacy_complete($1::smallint, 'amazon', 'lg', date '2026-10-01', date '2026-11-01')`, [co]);
  await rejects(() => apply('O-7', 1, 'c', [line('2026-10-21', 'amazon_settlement_flat_v1', { currency: 'USD', sales_principal_jpy: 100 })], { scope: 'old' }), /non-JPY/);
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O-7'`), 0);
});
await t('listing / sku / seller_sku のどれが null でも主キーが組める (grain_key)。同じ run に同じ粒度は 2 行入らない。seller_sku の「無し」は null だけ。JPY だけ。会社違いの SKU / 出品は付かない', async () => {
  await pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1')`, [co]);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1')`, [co]), /duplicate key/);
  await pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, sku_id, listing_id, seller_sku) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', $2, $3, 'pr_SKU-A')`, [co, sku, listing]);
  await pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, seller_sku) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', 'pr_SKU-A')`, [co]);
  const g = (await pg.query(`select grain_key from mart.finance_daily where run_id = 'r1' order by grain_key`)).rows.map((r) => r.grain_key);
  assert.deepEqual(g, ['-|-|-', '-|-|pr_SKU-A', `${listing}|${sku}|pr_SKU-A`]);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, seller_sku) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', '-')`, [co]), /finance_daily_seller_sku_check|violates check/);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, currency) values ('r2', $1, current_date, 'amazon', 'us', 'amazon_finances_api', 'USD')`, [co]), /currency/);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, sku_id) values ('r3', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', $2)`, [co, skuOther]), /foreign key|violates/i);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, listing_id) values ('r3', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', $2)`, [co, listingOther]), /foreign key|violates/i);
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
