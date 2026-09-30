#!/usr/bin/env node
/**
 * test-company-db-finance.mjs — Amazon 財務の受け口 (0012 → 0043 で作り直し・F2b-1) の受入試験
 *
 * 設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』v5.1。PGlite で 0001〜0043 を流して確かめる:
 *   0043 の安全の手順 (0012 の表が空でなければ止まる) / 旧互換 (legacy_*・v_finance_daily_legacy・assert_legacy_complete) が無い / policy (amazon / jp = unified 1 行) /
 *   policy の重なり・欠落 (0012 の検査は残る) / apply = 集合の丸ごと置換 (古い世代は stale・同じ内容 + 同じ版は same・版が違えば置換・同じ世代で内容違いは例外・空の集合) /
 *   入力の歯止め (整数でない金額・未知の line_kind・疑似注文と計上日・SKU と種類・SKU の行の手数料の材料・net = 20 列 + unmapped) /
 *   mart.v_finance_daily = SQLite の日次の財務と同じ値 (SQLite 側の試験 test-finance-promotion-tax.js と同じ場面・手で計算した期待値) /
 *   月の手数料の view / uncovered (policy 無し・source 違い) / 注文の累計は疑似注文を入れない /
 *   指紋 (checksum) の決め (並べ方・列の順・NFC・安全な整数・空の集合) / 受け口の検証 (送り手の申告と内容の指紋が違えば 400)
 * 🚨 policy の重複検査・受領行の for update の 2 接続の並行は PGlite では書けない → 本番で手で確かめる (08 §7.6)
 * 実行: node scripts/test-company-db-finance.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { validateFinanceRows, orderFinanceChecksum, financeRowsFormat, versionHasClass, CONTENT_COLUMNS, LEGACY_CONTENT_COLUMNS, CLASS_COLUMNS, pseudoOrderNo } from '../apps/company-db/finance/order-finance-checksum.mjs';
import { validateFinanceChunk, ingestOrderFinanceChunk } from '../apps/company-db/ingest/order-finance.mjs';
import Database from 'better-sqlite3';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`); return threw; };
const quiet = () => {};

const pg = new PGlite();
const applied = await applyMigrations(pgliteAdapter(pg), { log: quiet });
assert.ok(applied.applied.includes('0043'), '0043 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
const co = 1;
await pg.query(`insert into core.products (company_id, name) values (1, '見本B')`);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'sku-b', name from core.products`);
const listing = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', 'sku-b', 'active') returning listing_id`)).listing_id;

const U = 'amazon_settlement_unified';
/** 行 (無い整数列は 0) */
const row = (date, sku, x = {}) => ({ economic_date_jst: date, seller_sku: sku, line_kind: sku === '-' ? 'unknown' : 'sku', source: U, source_lines: 1, source_updated_at: `${date}T00:00:00Z`, content_hash: 'h', ...x });
/** 受け口と同じ手順: 形を確かめ → 指紋を計算 → 関数へ */
//   0047: 旧い形 (4 列の鍵が無い) = 版 't1'・4 列を外して渡す / 今の形 = 版 amazon_finance_v2 (版と行の形は結び付いている)
const V2 = 'amazon_finance_v2';
const apply = async (order, seq, rows, { version = null, mall = 'amazon', scope = 'jp' } = {}) => {
  const legacy = financeRowsFormat(rows) !== 'v2';
  const content = validateFinanceRows(order, rows);
  const checksum = orderFinanceChecksum(content, { legacy });
  const lines = content.map((c, i) => { const o = { ...c, source_updated_at: rows[i].source_updated_at, content_hash: rows[i].content_hash }; if (legacy) for (const k of CLASS_COLUMNS) delete o[k]; return o; });
  return (await one(`select core.apply_order_finance_batch($1::smallint, $2, $3, $4, $5::bigint, $6, $7, $8::jsonb) as r`, [co, mall, scope, order, seq, checksum, version ?? (legacy ? 't1' : V2), JSON.stringify(lines)])).r;
};
/** 形の検査を通さずに関数へ直接 (SQL の歯止めを確かめる)。4 列の鍵があれば版は amazon_finance_v2 */
const applyRaw = (order, seq, rows, checksum = 'a'.repeat(64), version = null) =>
  one(`select core.apply_order_finance_batch($1::smallint, 'amazon', 'jp', $2, $3::bigint, $4, $5, $6::jsonb) as r`,
    [co, order, seq, checksum, version ?? (financeRowsFormat(rows) === 'v2' ? V2 : 't1'), JSON.stringify(rows)]);

console.log('0043: 作り直しと安全の手順');
await t('表・view・関数がそろい、旧互換 (legacy_*・v_finance_daily_legacy・assert_legacy_complete) は無い', async () => {
  const views = (await pg.query(`select table_name as t from information_schema.views where table_schema = 'mart' and table_name like '%finance%' order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(views, ['v_finance_account_fees_monthly', 'v_finance_daily', 'v_order_finance_summary', 'v_order_finance_uncovered']);
  assert.equal(await num(`select count(*) as n from information_schema.columns where table_schema = 'core' and table_name = 'order_finance_daily' and column_name like 'legacy%'`), 0);
  assert.equal(await num(`select count(*) as n from pg_proc where proname = 'assert_legacy_complete'`), 0);
  assert.equal(await num(`select count(*) as n from pg_proc where proname in ('apply_order_finance_batch','finance_policy_gaps','assert_finance_policy_covered','check_finance_policy_overlap')`), 4);
  const cols = (await pg.query(`select column_name as c from information_schema.columns where table_schema = 'core' and table_name = 'order_finance_daily'`)).rows.map((r) => r.c);
  for (const c of ['line_kind', 'points_jpy', 'unmapped_jpy', 'promotion_tax_jpy', 'refund_principal_customer_jpy', 'refund_principal_atoz_jpy', 'account_fee_amount_jpy']) assert.ok(cols.includes(c), `列 ${c} が無い`);
});
await t('policy = amazon / jp に amazon_settlement_unified [2026-01-01, 無期限) の 1 行', async () => {
  const p = (await pg.query(`select company_id, mall, scope_key, period_from::text f, period_to, source from core.finance_source_policy`)).rows;
  assert.deepEqual(p, [{ company_id: 1, mall: 'amazon', scope_key: 'jp', f: '2026-01-01', period_to: null, source: U }]);
});
await t('🚨 0012 の表が空でなければ 0043 は止まる (何も変えない)', async () => {
  const pg2 = new PGlite();
  await applyMigrations(pgliteAdapter(pg2), { log: quiet, to: '0042' });
  await pg2.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, source) values (1, 'amazon', 'jp', '2026-01-01', 'amazon_settlement_flat_v1')`);
  await rejects(() => applyMigrations(pgliteAdapter(pg2), { log: quiet }), /0012 の表が空ではない/);
  assert.equal(Number((await pg2.query(`select count(*) as n from ops.schema_migrations where version = '0043'`)).rows[0].n), 0);
  assert.equal(Number((await pg2.query(`select count(*) as n from information_schema.views where table_schema = 'mart' and table_name = 'v_finance_daily_legacy'`)).rows[0].n), 1);   // 旧のまま
  await pg2.close();
});

console.log('policy (0012 の検査は残る)');
await t('期間の重なりは拒む・欠落は gaps に出る・source は決まった値だけ (unified を含む)', async () => {
  await rejects(() => pg.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, source) values (1, 'amazon', 'jp', '2027-01-01', 'amazon_settlement_flat_v2')`), /overlaps/);
  await rejects(() => pg.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, source) values (1, 'amazon', 'x', '2026-01-01', 'settlement')`), /ck_finance_source_policy_source/);
  const g = (await pg.query(`select gap_from::text f, gap_to::text t from core.finance_policy_gaps(1::smallint, 'amazon', 'jp', date '2025-12-01', date '2026-02-01')`)).rows;
  assert.deepEqual(g, [{ f: '2025-12-01', t: '2026-01-01' }]);
});

console.log('apply_order_finance_batch (集合の丸ごと置換)');
await t('🚨 applied → 古い世代は stale → 同じ内容 + 同じ版は same (世代だけ) → 同じ世代で内容違いは例外 → 版が違えば置換', async () => {
  const r1 = [row('2026-09-05', 'sku-b', { units_ordered: 1, sales_principal_jpy: 1000, commission_jpy: -110 })];
  assert.equal(await apply('503-1', 10, r1), 'applied');
  const f = await one(`select net_jpy, listing_id, received_batch_seq, built_at from core.order_finance_daily where mall_order_no = '503-1'`);
  assert.equal(Number(f.net_jpy), 890); assert.equal(f.listing_id, listing);
  assert.equal(await apply('503-1', 9, [row('2026-09-05', 'sku-b', { sales_principal_jpy: 1 })]), 'stale');
  assert.equal(await apply('503-1', 11, r1), 'same');
  const f2 = await one(`select received_batch_seq, built_at from core.order_finance_daily where mall_order_no = '503-1'`);
  assert.equal(Number(f2.received_batch_seq), 11); assert.equal(String(f2.built_at), String(f.built_at));
  await rejects(() => apply('503-1', 11, [row('2026-09-05', 'sku-b', { sales_principal_jpy: 2 })]), /already applied with a different content or version/);
  assert.equal(await apply('503-1', 12, r1, { version: 't2' }), 'applied');   // 同じ内容でも変換の版が違えば置き換える
  assert.equal((await one(`select transform_version v from core.order_finance_receipts where mall_order_no = '503-1'`)).v, 't2');
});
await t('新しい世代は集合を丸ごと置換 (消えた計上日は消える)・空の集合でも受領状態は残り、遅れた古い世代は戻らない', async () => {
  assert.equal(await apply('503-2', 5, [row('2026-09-05', 'sku-b', { sales_principal_jpy: 500 }), row('2026-09-10', 'sku-b', { refund_principal_jpy: -500 })]), 'applied');
  assert.equal(await apply('503-2', 6, [row('2026-09-10', 'sku-b', { refund_principal_jpy: -500 })]), 'applied');
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '503-2'`), 1);
  assert.equal(await apply('503-2', 7, []), 'applied');
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '503-2'`), 0);
  assert.equal((await one(`select lines from core.order_finance_receipts where mall_order_no = '503-2'`)).lines, 0);
  assert.equal(await apply('503-2', 6, [row('2026-09-10', 'sku-b', { refund_principal_jpy: -500 })]), 'stale');
});
await t('SQL の歯止め: 整数でない金額・未知の line_kind・疑似注文と計上日の違い・SKU と種類の食い違い・SKU の行の手数料の材料・指紋の形 → 例外で何も変わらない', async () => {
  const before = await num(`select count(*) as n from core.order_finance_daily`);
  await rejects(() => applyRaw('x-1', 1, [row('2026-09-05', 'sku-b', { sales_principal_jpy: 1.5 })]), /bigint|invalid input syntax/);
  await rejects(() => applyRaw('x-2', 1, [row('2026-09-05', '-', { line_kind: 'mystery' })]), /line_kind_check|violates check/);
  await rejects(() => applyRaw('-:2026-09-05', 1, [row('2026-09-06', '-', { line_kind: 'storage' })]), /ck_order_finance_daily_pseudo/);
  await rejects(() => applyRaw('x-3', 1, [row('2026-09-05', 'sku-b', { line_kind: 'storage' })]), /ck_order_finance_daily_kind/);
  await rejects(() => applyRaw('x-4', 1, [row('2026-09-05', 'sku-b', { account_fee_amount_jpy: -1 })]), /ck_order_finance_daily_account_fee/);
  await rejects(() => applyRaw('x-5', 1, [row('2026-09-05', 'sku-b')], 'not-a-hash'), /sha256/);
  await rejects(() => applyRaw('x-6', 1, [row('2026-09-05', 'sku-b', { currency: 'USD' })]), /non-JPY/);
  assert.equal(await num(`select count(*) as n from core.order_finance_daily`), before);
});
await t('net = 20 金額列 + unmapped (内訳 3 列と手数料の材料は足さない)', async () => {
  await apply('503-3', 1, [row('2026-09-06', 'sku-b', { sales_principal_jpy: 1000, points_jpy: -20, unmapped_jpy: -7, promotion_jpy: -50, promotion_tax_jpy: -5, refund_principal_jpy: -100, refund_principal_customer_jpy: -100 })]);
  assert.equal(Number((await one(`select net_jpy from core.order_finance_daily where mall_order_no = '503-3'`)).net_jpy), 1000 - 20 - 7 - 50 - 100);
  await apply('503-3', 2, []);
});

console.log('mart.v_finance_daily (SQLite の日次の財務と同じ値・test-finance-promotion-tax.js と同じ場面)');
// SKU-Q (注文 O2・SQLite の試験の SKU-B と同じ): 5 日に 2 個 (本体 2,000・税 200・送料 300・手数料 −220・FBA −660・送料のチャージバック −330… ではなく −300・値引き −330 (税 −30))
//   10 日に返品 (本体 −1,000・送料の返金 −300・返品の手数料 +50 = 返金 −1,250 (うち本体 customer −1,000)・手数料 +110 −22 = +88・チャージバック +300・値引き +330 (税 +30))
//   12 日にカードの支払い取り消し (本体 −1,000 = 返金 −1,000・customer には入れない = 返品数に入れない・手数料 +88)
// SKU-C: 5 日に 1 個 1,000・ポイント −30 / 注文番号の無い SKU の行: 7 日 倉庫の破損の補てん +500 (疑似注文 -:2026-09-07)
await t('返品・カードの支払い取り消し・ポイント・値引きの税・補てん (注文番号なし) の日次 = 手で計算した値', async () => {
  await apply('O2', 1, [
    row('2026-09-05', 'sku-q', { units_ordered: 2, sales_principal_jpy: 2000, sales_tax_jpy: 200, sales_shipping_jpy: 300, commission_jpy: -220, fba_fulfillment_jpy: -660, shipping_chargeback_jpy: -300, promotion_jpy: -330, promotion_tax_jpy: -30, source_lines: 9 }),
    row('2026-09-10', 'sku-q', { refund_principal_jpy: -1250, refund_principal_customer_jpy: -1000, sales_tax_jpy: -100, commission_jpy: 88, shipping_chargeback_jpy: 300, promotion_jpy: 330, promotion_tax_jpy: 30, source_lines: 9 }),
    row('2026-09-12', 'sku-q', { refund_principal_jpy: -1000, sales_tax_jpy: -100, commission_jpy: 88, source_lines: 4 }),
  ]);
  await apply('O3', 1, [row('2026-09-05', 'sku-c', { units_ordered: 1, sales_principal_jpy: 1000, points_jpy: -30, source_lines: 3 })]);
  await apply(pseudoOrderNo('2026-09-07'), 1, [row('2026-09-07', 'sku-b', { warehouse_damage_jpy: 500 })]);
  const d = async (date, sku) => one(`select * from mart.v_finance_daily where economic_date_jst = $1 and seller_sku = $2`, [date, sku]);
  const b5 = await d('2026-09-05', 'sku-q'), b10 = await d('2026-09-10', 'sku-q'), b12 = await d('2026-09-12', 'sku-q'), c5 = await d('2026-09-05', 'sku-c'), b7 = await d('2026-09-07', 'sku-b');
  const n = (x) => Number(x);
  assert.deepEqual([b5.units_ordered, n(b5.commission_jpy), n(b5.fba_fulfillment_jpy), n(b5.shipping_chargeback_jpy), n(b5.promotion_jpy), n(b5.promotion_tax_jpy), n(b5.profit_before_cogs_jpy)], [2, 220, 660, 300, 330, 30, 2300 - 220 - 660 - 300 - 330]);
  assert.deepEqual([n(b10.commission_jpy), n(b10.shipping_chargeback_jpy), n(b10.promotion_jpy), n(b10.promotion_tax_jpy), n(b10.refund_principal_jpy), b10.units_refunded_customer, b10.units_net_sold, n(b10.profit_before_cogs_jpy)],
    [-88, -300, -330, -30, 1250, 1, -1, 88 + 300 + 330 - 1250]);   // 単価 = 2,000 ÷ 2 = 1,000 → 本体の返金 1,000 ÷ 1,000 = 1 個
  assert.deepEqual([n(b12.refund_principal_jpy), b12.units_refunded_customer, n(b12.commission_jpy), n(b12.profit_before_cogs_jpy)], [1000, 0, -88, 88 - 1000]);   // 支払い取り消しは返品数に入れない
  assert.deepEqual([n(c5.points_jpy), n(c5.profit_before_cogs_jpy)], [30, 1000 - 30]);
  assert.deepEqual([n(b7.warehouse_damage_jpy), n(b7.profit_before_cogs_jpy)], [500, 500]);   // 注文番号の無い SKU の行も日次に入る
  assert.equal(Number(b5.closing_fee_jpy), 0); assert.equal(b5.units_marketplace_guarantee, 0);   // build の固定値
  // SQLite の試験の B の合計と同じ: 原価を除いた利益 = 790 − 532 − 912 = −654 (SQLite は原価 400 を引いて −1,054)
  assert.equal(await num(`select sum(profit_before_cogs_jpy) as n from mart.v_finance_daily where seller_sku = 'sku-q' and economic_date_jst in ('2026-09-05','2026-09-10','2026-09-12')`), -654);
});
await t('0044 mart.finance_daily_range = mart.v_finance_daily (同じ期間で全列一致・期間の途中の日だけでも単価は月の全部の行・月をまたぐ・別の scope は入らない)', async () => {
  await apply('O-AUG', 1, [row('2026-08-31', 'sku-q', { units_ordered: 1, sales_principal_jpy: 3000, source_lines: 2 })]);   // 8 月は別の単価 (月をまたぐ期間の確かめ)
  const cmp = async (from, to) => {
    const v = (await pg.query(`select * from mart.v_finance_daily where company_id = 1 and mall = 'amazon' and scope_key = 'jp' and economic_date_jst between $1::date and $2::date order by economic_date_jst, seller_sku`, [from, to])).rows;
    const f = (await pg.query(`select * from mart.finance_daily_range(1::smallint, 'amazon', 'jp', $1::date, $2::date) order by economic_date_jst, seller_sku`, [from, to])).rows;
    assert.deepEqual(f, v, `${from}〜${to}`);
    return f;
  };
  assert.ok((await cmp('2026-09-01', '2026-09-30')).length >= 5);
  const only10 = await cmp('2026-09-10', '2026-09-10');   // 返品の日だけ = 単価は 9 月の全部の行 (5 日の売上) から → 1 個
  assert.equal(only10.find((r) => r.seller_sku === 'sku-q').units_refunded_customer, 1);
  const cross = await cmp('2026-08-15', '2026-10-15');
  assert.ok(cross.some((r) => r.economic_date_jst.toISOString ? r.economic_date_jst.toISOString().startsWith('2026-08-31') : String(r.economic_date_jst).startsWith('2026-08-31')));
  assert.equal(await num(`select count(*) as n from mart.finance_daily_range(1::smallint, 'amazon', 'us', '2026-09-01', '2026-09-30')`), 0);
  // 0045: 関数の中だけ nested loop を使わない (本番で見込みが外れて 1,786 万回の比較 = 33 秒・120 秒で打ち切りだった)
  const cfg = (await one(`select array_to_string(proconfig, ',') as c from pg_proc where proname = 'finance_daily_range'`)).c;
  assert.match(String(cfg), /enable_nestloop=off/);
  const src = fs.readFileSync(new URL('../apps/company-db/router.mjs', import.meta.url), 'utf8');
  assert.match(src, /from mart\.finance_daily_range\(1::smallint, \$1, \$2, \$3::date, \$4::date\)/);   // 受け口の /daily は関数を使う
  await apply('O-AUG', 2, []);
});
await t('単価の丸め = ROUND (0.5 は 0 から遠い方)・Order の数量 0 の月は返品数 0', async () => {
  // 3 個で 2 円 → 単価 666,666.67 micro → ROUND = 666,667 (trunc なら 666,666)。本体の返金 1 円 → 1,000,000 ÷ 666,667 = 1.4999… → 1 個 (trunc の単価だと 1.5000… → 2 個)
  await apply('R1', 1, [row('2026-08-03', 'sku-r', { units_ordered: 3, sales_principal_jpy: 2 }), row('2026-08-20', 'sku-r', { refund_principal_jpy: -1, refund_principal_customer_jpy: -1 })]);
  assert.equal((await one(`select units_refunded_customer u from mart.v_finance_daily where seller_sku = 'sku-r' and economic_date_jst = '2026-08-20'`)).u, 1);
  await apply('R2', 1, [row('2026-07-20', 'sku-z', { refund_principal_jpy: -500, refund_principal_customer_jpy: -500 })]);   // その月に売上が無い
  assert.equal((await one(`select units_refunded_customer u from mart.v_finance_daily where seller_sku = 'sku-z'`)).u, 0);
});

console.log('月の手数料・uncovered・注文の累計');
await t('月の手数料 = 種類ごとの Σ(other_amount + item_related_fee)。本物の注文の Easy Ship も入る・手数料に入れない種類は入らない', async () => {
  await apply(pseudoOrderNo('2026-09-03'), 1, [
    row('2026-09-03', '-', { line_kind: 'storage', fba_storage_jpy: -300, account_fee_amount_jpy: -300 }),
    row('2026-09-03', '-', { line_kind: 'subscription', other_amount_jpy: -4900, account_fee_amount_jpy: -4900 }),
    row('2026-09-03', '-', { line_kind: 'not_account_fee', other_amount_jpy: -1000, account_fee_amount_jpy: -1000 }),
  ]);
  await apply('ES-1', 1, [row('2026-09-04', '-', { line_kind: 'easy_ship', other_fee_jpy: -440, other_amount_jpy: -100, account_fee_amount_jpy: -540 })]);   // 新しい月 = item_related_fee (-440) + 古い月の列 (-100)
  const m = Object.fromEntries((await pg.query(`select fee_type, amount_jpy from mart.v_finance_account_fees_monthly where month_start_jst = '2026-09-01'`)).rows.map((r) => [r.fee_type, Number(r.amount_jpy)]));
  assert.deepEqual(m, { easy_ship: -540, storage: -300, subscription: -4900 });
});
await t('🚨 uncovered = policy の無い日 (no_policy) と source の違う日 (source_mismatch)。どちらも日次に入らない', async () => {
  await apply('U1', 1, [row('2025-12-31', 'sku-u', { sales_principal_jpy: 100 }), row('2026-09-08', 'sku-u', { sales_principal_jpy: 200, source: 'amazon_settlement_flat_v1' })]);
  const u = (await pg.query(`select economic_date_jst::text d, reason from mart.v_order_finance_uncovered where mall_order_no = 'U1' order by 1`)).rows;
  assert.deepEqual(u, [{ d: '2025-12-31', reason: 'no_policy' }, { d: '2026-09-08', reason: 'source_mismatch' }]);
  assert.equal(await num(`select count(*) as n from mart.v_finance_daily where seller_sku = 'sku-u'`), 0);
});
await t('注文の累計は本物の注文だけ (疑似注文 -: は入らない)', async () => {
  assert.equal(await num(`select count(*) as n from mart.v_order_finance_summary where mall_order_no like '-%'`), 0);
  const o2 = await one(`select units_ordered, net_jpy from mart.v_order_finance_summary where mall_order_no = 'O2'`);
  assert.equal(o2.units_ordered, 2);
});

console.log('指紋 (checksum) の決めと受け口の検証');
await t('指紋 = 並べ方 (UTF-8 のバイト順)・列の順・空の集合 = [] の sha256。行の順が違っても同じ・列を 1 つ変えると変わる', async () => {
  const rs = validateFinanceRows('A1', [row('2026-09-02', 'b', { sales_principal_jpy: 5 }), row('2026-09-01', 'a', { units_ordered: 1 })]);
  const expectJson = JSON.stringify([
    CONTENT_COLUMNS.map((c) => ({ economic_date_jst: '2026-09-01', seller_sku: 'a', line_kind: 'sku', source: U, units_ordered: 1, source_lines: 1 }[c] ?? 0)),
    CONTENT_COLUMNS.map((c) => ({ economic_date_jst: '2026-09-02', seller_sku: 'b', line_kind: 'sku', source: U, sales_principal_jpy: 5, source_lines: 1 }[c] ?? 0)),
  ]);
  assert.equal(orderFinanceChecksum(rs), crypto.createHash('sha256').update(Buffer.from(expectJson, 'utf8')).digest('hex'));
  assert.equal(orderFinanceChecksum([...rs].reverse()), orderFinanceChecksum(rs));
  assert.equal(orderFinanceChecksum([]), crypto.createHash('sha256').update('[]').digest('hex'));
  const base = orderFinanceChecksum(rs);
  for (const c of CONTENT_COLUMNS.filter((c) => !['economic_date_jst', 'seller_sku', 'line_kind', 'source'].includes(c))) {
    const x = rs.map((r, i) => (i === 0 ? { ...r, [c]: r[c] + 1 } : r));
    assert.notEqual(orderFinanceChecksum(x), base, `列 ${c} を変えても指紋が変わらない`);
  }
});
await t('形の検査: 安全でない整数 (上限外・足すと超える)・NFC でない SKU・疑似注文の日付違い・行の鍵の重複 → 整形できない', async () => {
  assert.throws(() => validateFinanceRows('A1', [row('2026-09-01', 'a', { sales_principal_jpy: Number.MAX_SAFE_INTEGER + 2 })]), /safe integer/);
  assert.throws(() => validateFinanceRows('A1', [row('2026-09-01', 'a', { sales_principal_jpy: Number.MAX_SAFE_INTEGER, commission_jpy: 10 })]), /net is not a safe integer/);
  assert.doesNotThrow(() => validateFinanceRows('A1', [row('2026-09-01', 'a', { sales_principal_jpy: Number.MAX_SAFE_INTEGER })]));
  assert.throws(() => validateFinanceRows('A1', [row('2026-09-01', 'é')]), /NFC/);   // é を 2 文字で
  assert.throws(() => validateFinanceRows('-:2026-09-01', [row('2026-09-02', '-', { line_kind: 'storage' })]), /pseudo order/);
  assert.throws(() => validateFinanceRows('A1', [row('2026-09-01', 'a'), row('2026-09-01', 'a')]), /duplicate row key/);
  assert.throws(() => validateFinanceRows('A1', [row('2026-02-30', 'a')]), /not a date/);
});
await t('受け口: 送り手の申告の指紋が内容と違えば 400・合っていれば通る', async () => {
  const rows = [row('2026-09-01', 'a', { sales_principal_jpy: 5 })];   // 旧い形 (0047 の 4 列の鍵が無い) = 指紋は旧い形の列で
  const good = orderFinanceChecksum(validateFinanceRows('A1', rows), { legacy: true });
  const body = (cs) => ({ run_id: 'ship_202609291200000_abcdef', batch_seq: 1, chunk_index: 0, last: true, transform_version: 't1',
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'A1', header: { transform_version: 't1', set_checksum: cs }, lines: rows }] });
  assert.equal(validateFinanceChunk(body(good)).rows[0].set_checksum, good);
  assert.throws(() => validateFinanceChunk(body('0'.repeat(64))), /set_checksum differs/);
  assert.throws(() => validateFinanceChunk({ ...body(good), rows: [{ ...body(good).rows[0], mall_order_no: '-:2026-9-1' }] }), /mall_order_no has a bad form/);
});

console.log('#1533 Codex R1 の直し');
await t('🚨 丸め = 本物の SQLite の CAST(ROUND(a * 1.0 / b) AS INTEGER) と同じ答え (Codex の例 + 0.5 の境目の近く + 乱数 3,000 組)', async () => {
  const sq = new Database(':memory:');
  const sqRound = sq.prepare('SELECT CAST(ROUND(? * 1.0 / ?) AS INTEGER) AS r');
  const pairs = [[4000002001000000, 2000001], [1000000000, 2000000000], [1000000000, 2000000001], [2000000, 3], [1000000, 666667], [-1000000, 3], [5, 10], [15, 10], [-15, 10]];
  let seed = 12345; const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
  for (let i = 0; i < 3000; i++) {
    const b = 1 + Math.floor(rnd() * 5000000);
    const k = Math.floor(rnd() * 3000000000);
    const a = k * b + Math.floor(b / 2) + Math.floor(rnd() * 3) - 1;   // 0.5 の境目の近く
    if (Number.isSafeInteger(a)) pairs.push([rnd() < 0.2 ? -a : a, b]);
  }
  const bad = [];
  for (const [a, b] of pairs) {
    const want = Number(sqRound.get(BigInt(a), BigInt(b)).r);
    const got = Number((await one(`select core.sqlite_round($1::bigint::float8 / $2::bigint::float8) as r`, [String(a), String(b)])).r);
    if (want !== got) bad.push([a, b, want, got]);
  }
  sq.close();
  assert.equal(bad.length, 0, `SQLite と違う: ${JSON.stringify(bad.slice(0, 5))}`);
  // view でも: 本体 4,000,002,001 円・2,000,001 個 → 単価 2,000,000,000 micro (SQLite) → 本体の返金 1,000 円は 1 個
  await apply('RB', 1, [row('2026-06-03', 'sku-rb', { units_ordered: 2000001, sales_principal_jpy: 4000002001 }), row('2026-06-20', 'sku-rb', { refund_principal_jpy: -1000, refund_principal_customer_jpy: -1000 })]);
  assert.equal((await one(`select units_refunded_customer u from mart.v_finance_daily where seller_sku = 'sku-rb' and economic_date_jst = '2026-06-20'`)).u, 1);
});
await t('🚨 通貨: JPY 以外の行は形の確かめで拒む (受け口の 400・HTTP の道で USD が円として入らない)', async () => {
  assert.throws(() => validateFinanceRows('C1', [row('2026-09-01', 'a', { currency: 'USD', sales_principal_jpy: 100 })]), /currency must be JPY/);
  const rows = [row('2026-09-01', 'a', { currency: 'USD', sales_principal_jpy: 100 })];
  const body = { run_id: 'ship_202609291200000_abcdef', batch_seq: 1, chunk_index: 0, last: true, transform_version: 't1',
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'C1', header: { transform_version: 't1', set_checksum: '0'.repeat(64) }, lines: rows }] };
  assert.throws(() => validateFinanceChunk(body), /currency must be JPY/);
  assert.doesNotThrow(() => validateFinanceRows('C1', [row('2026-09-01', 'a', { currency: 'JPY' })]));
});
await t('受け口の道 (validateFinanceChunk → ingestOrderFinanceChunk) で入る・同じ chunk の再送は同じ結果', async () => {
  const rows = [row('2026-09-01', 'sku-http', { units_ordered: 1, sales_principal_jpy: 300 })];
  const cs = orderFinanceChecksum(validateFinanceRows('H1', rows), { legacy: true });   // 旧い形
  const body = { run_id: 'ship_202609291300000_abcdef', batch_seq: 3, chunk_index: 0, last: true, transform_version: 't1',
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'H1', header: { transform_version: 't1', set_checksum: cs }, lines: rows }] };
  const r = await ingestOrderFinanceChunk(pgliteAdapter(pg), { ...validateFinanceChunk(body), host: 'test' });
  assert.equal(r.applied, 1, JSON.stringify(r));
  const again = await ingestOrderFinanceChunk(pgliteAdapter(pg), { ...validateFinanceChunk(body), host: 'test' });   // 同じ chunk の再送 (#1533 Codex R2)
  assert.ok(again.replay === true, JSON.stringify(again));
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = 'H1'`), 1);
  assert.equal(Number((await one(`select sales_principal_jpy s from core.order_finance_daily where mall_order_no = 'H1'`)).s), 300);
  assert.equal((await one(`select currency from core.order_finance_daily where mall_order_no = 'H1'`)).currency, 'JPY');
});
await t('🚨 server.js は order-finance を 事前の鍵の検査 と 共通の 10MB parser の素通り の両方に入れている (router 側の 12MB・圧縮なしが効く。#1533 Codex R2)', async () => {
  const fs = await import('node:fs'), path = await import('node:path'), { fileURLToPath } = await import('node:url');
  const srv = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'server.js'), 'utf8');
  assert.match(srv, /app\.use\(\[[^\]]*'\/apps\/company-db\/sync\/order-finance'[^\]]*\], companyDbRequireSyncKey\);/);
  assert.match(srv, /normalizedPath\.toLowerCase\(\)\.startsWith\('\/apps\/company-db\/sync\/order-finance'\)\) return next\(\);/);
});
await t('足し算の途中で安全な整数を超えたら拒む (最後だけ範囲に戻っても)', async () => {
  assert.throws(() => validateFinanceRows('A1', [row('2026-09-01', 'a', { sales_principal_jpy: Number.MAX_SAFE_INTEGER, sales_shipping_jpy: 2, sales_giftwrap_jpy: -2 })]), /net is not a safe integer \(at sales_shipping_jpy\)/);
});
await t('指紋は固定の値 (日本語の SKU = UTF-8 のバイト順で a < あ < b)', async () => {
  const rs = validateFinanceRows('A1', [row('2026-09-02', 'b', { sales_principal_jpy: 5 }), row('2026-09-01', 'あ', { units_ordered: 1 }), row('2026-09-01', 'a', { commission_jpy: -3 })]);
  const json = '[["2026-09-01","a","sku","amazon_settlement_unified",0,0,0,0,0,-3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],["2026-09-01","あ","sku","amazon_settlement_unified",1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],["2026-09-02","b","sku","amazon_settlement_unified",0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]]';
  assert.equal(crypto.createHash('sha256').update(Buffer.from(json, 'utf8')).digest('hex'), 'da0531f5f6d092cdc3cc473c2be393713813f923f10fe373d6603aa456bb81b1');
  // 旧い形 (0043〜0046 の取り決め) の値は 0047 の後も変わらない = 古い送り手の申告と合う
  assert.equal(orderFinanceChecksum(rs, { legacy: true }), 'da0531f5f6d092cdc3cc473c2be393713813f923f10fe373d6603aa456bb81b1');   // 列の順・並べ方・JSON の書き方を変えたら落ちる (送り手と受け口の取り決め)
  // 今の形 (0047) = 各行の後ろに 4 列 (分けられない部品の数・符号つき・絶対値・unmapped の部品の数)
  const json2 = '[["2026-09-01","a","sku","amazon_settlement_unified",0,0,0,0,0,-3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0],["2026-09-01","あ","sku","amazon_settlement_unified",1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0],["2026-09-02","b","sku","amazon_settlement_unified",0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0]]';
  const want2 = crypto.createHash('sha256').update(Buffer.from(json2, 'utf8')).digest('hex');
  assert.equal(want2, 'ef7750d06729d79ec9439656a7965b8e54442dbff988c29dac478a4e45ae12c4');
  assert.equal(orderFinanceChecksum(rs), 'ef7750d06729d79ec9439656a7965b8e54442dbff988c29dac478a4e45ae12c4');
});
await t('🚨 0043 は受領状態・公開の表のどれかに 1 行でもあれば止まる', async () => {
  for (const setup of [
    `insert into core.order_finance_receipts (company_id, mall, scope_key, mall_order_no, received_batch_seq, set_checksum, lines) values (1, 'amazon', 'jp', 'x', 1, 'c', 0)`,
    `insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source) values ('r', 1, '2026-09-01', 'amazon', 'jp', 's')`,
  ]) {
    const pg3 = new PGlite();
    await applyMigrations(pgliteAdapter(pg3), { log: quiet, to: '0042' });
    await pg3.query(setup);
    await rejects(() => applyMigrations(pgliteAdapter(pg3), { log: quiet }), /0012 の表が空ではない/);
    await pg3.close();
  }
});

console.log('0047 分けられない部品の 4 列・mart.finance_daily_sku_range (D7b-1a)');
/** 今の形 (0047) の 4 列 = 部品の数・符号つき・絶対値・unmapped の部品の数 */
const C4 = (c = 0, m = 0, a = 0, u = 0) => ({ unclassified_component_count: c, unclassified_mapped_jpy: m, unclassified_abs_jpy: a, unmapped_component_count: u });
await t('🚨 形の確かめ: 4 列は全部あるか全部無いか (行の中・行の間)・今の形だけ unmapped の金額には部品が要る・SKU の行は 分けられない列の和 = 符号つき・絶対値はその列の絶対値の和以上・どの形でも数 0 ⇔ 絶対値 0', async () => {
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', 'a', { unclassified_component_count: 1 })]), /all together/);
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', 'a', C4()), row('2026-09-02', 'a')]), /mix the new form/);
  assert.equal(financeRowsFormat([]), 'empty'); assert.equal(financeRowsFormat([row('2026-09-01', 'a')]), 'legacy'); assert.equal(financeRowsFormat([row('2026-09-01', 'a', C4())]), 'v2');
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', 'a', { unmapped_jpy: -7, ...C4() })]), /unmapped_component_count is 0/);
  assert.doesNotThrow(() => validateFinanceRows('V1', [row('2026-09-01', 'a', { unmapped_jpy: -7 })]));   // 旧い形 = 数を持たない (既存の行と同じ)
  assert.doesNotThrow(() => validateFinanceRows('V1', [row('2026-09-01', 'a', { unmapped_jpy: 0, ...C4(0, 0, 0, 2) })]));   // +5 / −5 の相殺 = 部品 2
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', 'a', { other_amount_jpy: 100, ...C4() })]), /must equal/);   // 分けられない金額があるのに数 0
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', 'a', { other_amount_jpy: 100, misc_fee_jpy: -100, ...C4(1, 0, 150) })]), /must be >=/);   // 列の打ち消しを絶対値で隠さない
  assert.doesNotThrow(() => validateFinanceRows('V1', [row('2026-09-01', 'a', { other_amount_jpy: 100, misc_fee_jpy: -100, ...C4(2, 0, 200) })]));
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', '-', { line_kind: 'storage', ...C4(0, 0, 5) })]), /iff/);
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', '-', { line_kind: 'storage', ...C4(1, 9, 5) })]), /<= unclassified_abs_jpy/);
  assert.throws(() => validateFinanceRows('V1', [row('2026-09-01', '-', { line_kind: 'storage', ...C4(0, 0, 0, -1) })]), />= 0/);
  assert.doesNotThrow(() => validateFinanceRows('V1', [row('2026-09-01', '-', { line_kind: 'storage', misc_fee_jpy: 3, fba_storage_jpy: -300, account_fee_amount_jpy: -300, ...C4(1, 3, 3) })]));   // 手数料の行は列の和の決めなし
});
await t('🚨 SQL の歯止め (0047 の CHECK): 数 0 で絶対値あり・|符号つき| > 絶対値・負の数 → 例外', async () => {
  await rejects(() => applyRaw('ck-1', 1, [row('2026-09-05', 'sku-b', C4(0, 0, 5))]), /ck_order_finance_daily_unclassified/);
  await rejects(() => applyRaw('ck-2', 1, [row('2026-09-05', '-', { line_kind: 'storage', misc_fee_jpy: -9, ...C4(1, -9, 5) })]), /ck_order_finance_daily_unclassified/);
  await rejects(() => applyRaw('ck-3', 1, [row('2026-09-05', 'sku-b', C4(0, 0, 0, -1))]), /ck_order_finance_daily_unmapped_count/);
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no like 'ck-%'`), 0);
});
await t('受け口: 旧い形 = 旧い列の指紋・正規化した行に 4 列を出さない (古い送り手の受領記録の指紋と同じ) / 今の形 = 4 列つきの指紋 (旧い列の指紋は 400)', async () => {
  const body = (lines, cs, v = 't1') => ({ run_id: 'ship_202609301200000_abcdef', batch_seq: 1, chunk_index: 0, last: true, transform_version: v,
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'F1', header: { transform_version: v, set_checksum: cs }, lines }] });
  const old = [row('2026-09-01', 'a', { sales_principal_jpy: 5 })];
  const vOld = validateFinanceChunk(body(old, orderFinanceChecksum(validateFinanceRows('F1', old), { legacy: true }))).rows[0];
  for (const c of CLASS_COLUMNS) assert.ok(!Object.hasOwn(vOld.lines[0], c), `旧い形の正規化した行に ${c} がある`);
  assert.deepEqual(Object.keys(vOld.lines[0]), [...LEGACY_CONTENT_COLUMNS, 'source_updated_at', 'content_hash']);   // 0046 までの受け口と同じ形 = 受領記録の指紋が同じ
  const neu = [row('2026-09-01', 'a', { sales_principal_jpy: 5, other_amount_jpy: 0, ...C4(2, 0, 200) })];
  const vNew = validateFinanceChunk(body(neu, orderFinanceChecksum(validateFinanceRows('F1', neu)), V2)).rows[0];
  assert.deepEqual([vNew.lines[0].unclassified_component_count, vNew.lines[0].unclassified_abs_jpy], [2, 200]);
  assert.throws(() => validateFinanceChunk(body(neu, orderFinanceChecksum(validateFinanceRows('F1', neu), { legacy: true }), V2)), /set_checksum differs/);
  // router: NOT_MIGRATED / DOWNGRADE は 409
  const src = fs.readFileSync(new URL('../apps/company-db/router.mjs', import.meta.url), 'utf8');
  assert.match(src, /e\.code === 'NOT_MIGRATED' \|\| e\.code === 'DOWNGRADE'\) \? 409/);
});
await t('🚨 版と行の形 (#1554 Codex R1 High): v2 の版で 4 列が無い・null・一部 = 400 / 旧い版で 4 列あり = 400 / 墓石 (空の集合) はどちらでも通る / JS と SQL の版の規則が同じ', async () => {
  const body = (lines, v) => ({ run_id: 'ship_202609301200000_abcdef', batch_seq: 1, chunk_index: 0, last: true, transform_version: v,
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'G1', header: { transform_version: v, set_checksum: orderFinanceChecksum(validateFinanceRows('G1', lines), { legacy: financeRowsFormat(lines) !== 'v2' }) }, lines }] });
  const old = [row('2026-09-01', 'a', { sales_principal_jpy: 5 })];
  assert.throws(() => validateFinanceChunk(body(old, V2)), /needs unclassified_component_count/);
  assert.throws(() => validateFinanceChunk(body(old, 'amazon_finance_v3_x')), /needs/);
  const nul = [row('2026-09-01', 'a', { sales_principal_jpy: 5, ...C4(), unclassified_abs_jpy: null })];
  assert.throws(() => validateFinanceChunk(body(nul, V2)), /must not be null/);
  assert.throws(() => validateFinanceChunk(body([row('2026-09-01', 'a', { unclassified_component_count: 0 })], V2)), /all together/);
  const neu = [row('2026-09-01', 'a', { sales_principal_jpy: 5, ...C4() })];
  assert.throws(() => validateFinanceChunk(body(neu, 'amazon_finance_v1')), /old version but the rows carry/);
  assert.doesNotThrow(() => validateFinanceChunk(body([], V2)));
  assert.doesNotThrow(() => validateFinanceChunk(body([], 't1')));
  // SQL の歯止め (受け口の JS を通らない道でも)
  await rejects(() => applyRaw('vf-1', 1, [row('2026-09-05', 'sku-b')], 'a'.repeat(64), V2), /version_form/);
  await rejects(() => applyRaw('vf-2', 1, [row('2026-09-05', 'sku-b', { ...C4(), unclassified_mapped_jpy: null })], 'a'.repeat(64), V2), /version_form/);
  await rejects(() => applyRaw('vf-3', 1, [row('2026-09-05', 'sku-b', C4())], 'a'.repeat(64), 't1'), /version_form/);
  for (const v of ['amazon_finance_v1', 'amazon_finance_v2', 'amazon_finance_v10', 'amazon_finance_v2_test', 'amazon_finance_v1_test', 't1', '', 'xamazon_finance_v2', 'amazon_finance_v2-x', null]) {
    const sql = (await one(`select core.finance_version_has_class($1) as b`, [v])).b;
    assert.equal(sql, versionHasClass(v), `版 ${v}: SQL ${sql} / JS ${versionHasClass(v)}`);
  }
  assert.deepEqual(['amazon_finance_v1', 'amazon_finance_v2', 'amazon_finance_v2_test', 't1'].map(versionHasClass), [false, true, true, false]);
});
await t('🚨 旧い版への戻しを拒む (#1554 Codex R1 High): v2 で受けた注文を より新しい世代の旧い版 (4 列なし) で送っても 4 列は残る (受け口 = chunk ごと 409 DOWNGRADE / SQL = 例外)・墓石も・v2 の世代の続きは通る', async () => {
  const good = [row('2026-09-02', 'sku-dg', { sales_principal_jpy: 100, other_amount_jpy: 7, ...C4(1, 7, 7, 0) })];
  const send = (no, lines, seq, v, run) => ingestOrderFinanceChunk(pgliteAdapter(pg), { ...validateFinanceChunk({ run_id: run, batch_seq: seq, chunk_index: 0, last: true, transform_version: v,
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: no, header: { transform_version: v, set_checksum: orderFinanceChecksum(validateFinanceRows(no, lines), { legacy: financeRowsFormat(lines) !== 'v2' }) }, lines }] }), host: 'test' });
  assert.equal((await send('DG1', good, 10, V2, 'ship_202609301400000_dddddd')).applied, 1);
  const e = await rejects(() => send('DG1', [row('2026-09-02', 'sku-dg', { sales_principal_jpy: 100, other_amount_jpy: 7 })], 11, 'amazon_finance_v1', 'ship_202609301400001_dddddd'), /downgrade/);
  assert.equal(e.code, 'DOWNGRADE');
  await rejects(() => send('DG1', [], 12, 'amazon_finance_v1', 'ship_202609301400002_dddddd'), /downgrade/);   // 墓石で消すのも旧い版では拒む
  const kept = await one(`select unclassified_component_count c, unclassified_abs_jpy::int a, received_batch_seq::int s from core.order_finance_daily where mall_order_no = 'DG1'`);
  assert.deepEqual([kept.c, kept.a, kept.s], [1, 7, 10]);
  assert.equal((await one(`select transform_version v from core.order_finance_receipts where mall_order_no = 'DG1'`)).v, V2);
  // SQL の関数を直接 (JS の検査を通らない道・同時の書き込みの保険)
  await rejects(() => apply('DG1', 13, [row('2026-09-02', 'sku-dg', { sales_principal_jpy: 100, other_amount_jpy: 7 })], { version: 'amazon_finance_v1' }), /downgrade/);
  assert.equal((await one(`select unclassified_component_count c from core.order_finance_daily where mall_order_no = 'DG1'`)).c, 1);
  // 今の形の版の続き (より新しい v2 の世代・v3) は通る
  assert.equal((await send('DG1', good, 14, V2, 'ship_202609301400004_dddddd')).same, 1);
  assert.equal((await send('DG1', good, 15, 'amazon_finance_v3', 'ship_202609301400005_dddddd')).applied, 1);
  // 旧い版の注文 (受領記録が旧い版) は旧い版のまま送れる (downgrade ではない)
  assert.equal((await send('DG2', [row('2026-09-02', 'sku-dg2', { sales_principal_jpy: 1 })], 16, 't1', 'ship_202609301400006_dddddd')).applied, 1);
  assert.equal((await send('DG2', [row('2026-09-02', 'sku-dg2', { sales_principal_jpy: 2 })], 17, 't1', 'ship_202609301400007_dddddd')).applied, 1);
  await apply('DG1', 16, [], { version: 'amazon_finance_v3' }); await apply('DG2', 18, []);
});
await t('🚨 競合 (#1554 Codex R2 Medium 1): 事前の照会の後・取引の前に別の送信が受領記録を v2 にしても、旧い版の chunk は全体が 409 DOWNGRADE・ほかの行も入らない (行の failed に吸収しない)', async () => {
  const base = pgliteAdapter(pg);
  let raced = false;
  const racy = {
    ...base,
    query: async (sql, params) => {
      const r = await base.query(sql, params);
      // 受け口の事前の照会 (受領記録の版) の直後に、別の送り手が CR2 を v2 で送った
      if (!raced && /from core\.order_finance_receipts/.test(sql) && /any\(\$4::text\[\]\)/.test(sql)) {
        raced = true;
        assert.equal(await apply('CR2', 50, [row('2026-09-03', 'sku-cr2', { sales_principal_jpy: 10, ...C4() })]), 'applied');
      }
      return r;
    },
  };
  const lines1 = [row('2026-09-03', 'sku-cr1', { sales_principal_jpy: 1 })], lines2 = [row('2026-09-03', 'sku-cr2', { sales_principal_jpy: 2 })];
  const cs = (no, l) => orderFinanceChecksum(validateFinanceRows(no, l), { legacy: true });
  const body = { run_id: 'ship_202609301600000_ffffff', batch_seq: 51, chunk_index: 0, last: true, transform_version: 't1',
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'CR1', header: { transform_version: 't1', set_checksum: cs('CR1', lines1) }, lines: lines1 },
      { mall: 'amazon', scope_key: 'jp', mall_order_no: 'CR2', header: { transform_version: 't1', set_checksum: cs('CR2', lines2) }, lines: lines2 }] };
  const e = await rejects(() => ingestOrderFinanceChunk(racy, { ...validateFinanceChunk(body), host: 'test' }), /downgrade/);
  assert.ok(raced, '競合を作れていない');
  assert.equal(e.code, 'DOWNGRADE');
  assert.equal(await num(`select count(*) as n from core.order_finance_receipts where mall_order_no = 'CR1'`), 0);   // 先の行 (CR1) も入っていない = chunk 全体を rollback
  assert.equal(await num(`select count(*) as n from ops.ingest_chunks where ingest_run_id = 'ship_202609301600000_ffffff'`), 0);
  assert.equal((await one(`select transform_version v from core.order_finance_receipts where mall_order_no = 'CR2'`)).v, V2);
  // ほかのモールの受け口の挙動は変えない = 財務でも downgrade 以外の行の例外は今までどおり行の failed
  const bad = { ...body, run_id: 'ship_202609301600001_ffffff', batch_seq: 52, rows: [body.rows[0]] };
  const r = await ingestOrderFinanceChunk({ ...base, query: async (sql, p) => { if (/apply_order_finance_batch/.test(sql)) throw new Error('boom'); return base.query(sql, p); } }, { ...validateFinanceChunk(bad), host: 'test' });
  assert.deepEqual([r.applied, r.failed.length], [0, 1]);
  await apply('CR2', 53, [], { version: V2 });
});
await t('🚨 SKU の無い行の等式 (#1554 Codex R2 Medium 2): 月の手数料の行で分類の漏れを 4 列 0 と偽る (misc_fee +3・net −97) = JS も SQL も拒む / not_account_fee・unknown の行は分けられない部品を持たない', async () => {
  const fake = row('2026-09-04', '-', { line_kind: 'storage', fba_storage_jpy: -100, account_fee_amount_jpy: -100, misc_fee_jpy: 3, ...C4() });
  assert.throws(() => validateFinanceRows('-:2026-09-04', [fake]), /on an account fee row net \(-97\) must equal .* \(-100\)/);
  await rejects(() => applyRaw('-:2026-09-04', 1, [fake]), /ck_order_finance_daily_class_form/);
  const honest = { ...fake, ...C4(1, 3, 3) };
  assert.doesNotThrow(() => validateFinanceRows('-:2026-09-04', [honest]));
  const na = row('2026-09-04', '-', { line_kind: 'not_account_fee', other_amount_jpy: -5, account_fee_amount_jpy: -5, ...C4(1, -5, 5) });
  assert.throws(() => validateFinanceRows('-:2026-09-04', [na]), /must not carry unclassified components/);
  await rejects(() => applyRaw('-:2026-09-04', 1, [na]), /ck_order_finance_daily_class_form/);
  assert.doesNotThrow(() => validateFinanceRows('-:2026-09-04', [{ ...na, ...C4() }]));
  // SQL の等式 (SKU の行・unmapped の部品) も JS と同じ
  await rejects(() => applyRaw('sq-1', 1, [row('2026-09-04', 'sku-sq', { other_amount_jpy: 9, ...C4() })]), /ck_order_finance_daily_class_form/);
  await rejects(() => applyRaw('sq-2', 1, [row('2026-09-04', 'sku-sq', { unmapped_jpy: 9, ...C4() })]), /ck_order_finance_daily_class_form/);
  await rejects(() => applyRaw('sq-3', 1, [row('2026-09-04', 'sku-sq', { other_amount_jpy: 9, misc_fee_jpy: -9, ...C4(2, 0, 9) })]), /ck_order_finance_daily_class_form/);
  assert.equal(await num(`select count(*) as n from core.order_finance_receipts where mall_order_no in ('-:2026-09-04', 'sq-1', 'sq-2', 'sq-3')`), 0);
  // 旧い形の行 (旧い版) は対象の外 (既存の行と同じ = 数を持たない)
  assert.equal(await apply('-:2026-09-04', 1, [row('2026-09-04', '-', { line_kind: 'storage', fba_storage_jpy: -100, account_fee_amount_jpy: -100, misc_fee_jpy: 3 })]), 'applied');
  await apply('-:2026-09-04', 2, []);
});
await t('🚨 0047 の適用前: 旧い形は受ける・今の形は NOT_MIGRATED (4 列が黙って落ちない) → 0047 の後: 既存の行は 0・今の形が入る', async () => {
  const pg2 = new PGlite();
  const db2 = pgliteAdapter(pg2);
  await applyMigrations(db2, { log: quiet, to: '0046' });
  const send = async (no, lines, seq, run) => {
    const legacy = financeRowsFormat(lines) === 'legacy';
    const v = legacy ? 't1' : V2;
    const body = { run_id: run, batch_seq: seq, chunk_index: 0, last: true, transform_version: v,
      rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: no, header: { transform_version: v, set_checksum: orderFinanceChecksum(validateFinanceRows(no, lines), { legacy }) }, lines }] };
    return ingestOrderFinanceChunk(db2, { ...validateFinanceChunk(body), host: 'test' });
  };
  const r1 = await send('P1', [row('2026-09-01', 'sku-p1', { sales_principal_jpy: 100, unmapped_jpy: -3 })], 1, 'ship_202609301300000_aaaaaa');
  assert.equal(r1.applied, 1);
  const e = await rejects(() => send('P2', [row('2026-09-01', 'sku-p2', { sales_principal_jpy: 100, ...C4() })], 2, 'ship_202609301300001_aaaaaa'), /not_migrated/);
  assert.equal(e.code, 'NOT_MIGRATED');
  assert.equal(Number((await pg2.query(`select count(*) as n from core.order_finance_receipts where mall_order_no = 'P2'`)).rows[0].n), 0);
  await applyMigrations(db2, { log: quiet });
  const p1 = (await pg2.query(`select unclassified_component_count c, unclassified_mapped_jpy m, unclassified_abs_jpy a, unmapped_component_count u, unmapped_jpy uj from core.order_finance_daily where mall_order_no = 'P1'`)).rows[0];
  assert.deepEqual([p1.c, Number(p1.m), Number(p1.a), p1.u, Number(p1.uj)], [0, 0, 0, 0, -3]);   // 既存の行は 0 (送り直すまで = D7b-1b の coverage は送り直すまで complete にならない)
  const r2 = await send('P2', [row('2026-09-01', 'sku-p2', { sales_principal_jpy: 100, other_amount_jpy: 0, unmapped_jpy: -3, ...C4(2, 0, 200, 1) })], 3, 'ship_202609301300002_aaaaaa');
  assert.equal(r2.applied, 1);
  const p2 = (await pg2.query(`select unclassified_component_count c, unclassified_abs_jpy a, unmapped_component_count u from core.order_finance_daily where mall_order_no = 'P2'`)).rows[0];
  assert.deepEqual([p2.c, Number(p2.a), p2.u], [2, 200, 1]);
  await pg2.close();
});

// ── mart.finance_daily_sku_range ──
// 2026-06 (もう終わった月 = estimated_monthly_unit_price) の場面:
//   'sku-n1' (正規化) = 受け取った seller SKU 'SKU-N1' (N1) / 'sku-n1' (N2) / 'Sku-N1' (N3・出品を作る前に受け取った = 未解決)
//     6/5: N1 2 個 2,000 円・手数料 −200・closing_fee −30・other_amount の +100 / −100 (部品 2・絶対値 200)
//          N2 1 個 1,000 円・misc_fee +5 (部品 1)・unmapped −7 (部品 1) / N3 1 個 500 円
//     6/10: N1 の返品 本体 −1,000 (customer) → 'SKU-N1' の 6 月の単価 1,000 → 1 個
//   'sku-m' = 'SKU-M' (6/2 1 個 1,000・6/15 返品 −1,000 = 1 個) と 'sku-m' (6/15 A-to-z −300・6 月の売上なし = 単価なし) → 子は unit_price_missing
//   'sku-zz' = 6/12 返品 −500・売上なし → unit_price_missing / 'sku-f7' = 6/3 3 個 2 円・6/20 返品 −1 円 → 1,000,000 ÷ 666,667 = 1.4999992… → 1 個・丸める前 1.499999
// 2099-03 (まだ終わっていない月) = 'sku-p' 3/1 1 個 800・3/2 返品 −800 → estimated_partial_month_unit_price
const skuRange = async (from, to) => (await pg.query(`select * from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', $1::date, $2::date)`, [from, to])).rows;
const at = (rows, date, norm) => rows.find((r) => (r.economic_date_jst.toISOString ? r.economic_date_jst.toISOString().slice(0, 10) : String(r.economic_date_jst)) === date && r.seller_sku_norm === norm);
let listingN1 = null;
await t('🚨 手で計算: 正規化 SKU にまとめる・受け取りの出品 (解決済みと未解決が同じ日・同じ SKU に混ざる)・closing_fee を引く・net / unmapped・分けられない部品は相殺しても数で残る', async () => {
  await apply('N3', 1, [row('2026-06-05', 'Sku-N1', { units_ordered: 1, sales_principal_jpy: 500 })]);   // 出品を作る前 = 未解決
  listingN1 = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', 'sku-n1', 'active') returning listing_id`)).listing_id);
  await apply('N1', 1, [
    row('2026-06-05', 'SKU-N1', { units_ordered: 2, sales_principal_jpy: 2000, commission_jpy: -200, closing_fee_jpy: -30, other_amount_jpy: 0, ...C4(2, 0, 200, 0) }),
    row('2026-06-10', 'SKU-N1', { refund_principal_jpy: -1000, refund_principal_customer_jpy: -1000, ...C4() }),
  ]);
  await apply('N2', 1, [row('2026-06-05', 'sku-n1', { units_ordered: 1, sales_principal_jpy: 1000, misc_fee_jpy: 5, unmapped_jpy: -7, ...C4(1, 5, 5, 1) })]);
  const rows = await skuRange('2026-06-01', '2026-06-30');
  const n5 = at(rows, '2026-06-05', 'sku-n1');
  assert.ok(n5, JSON.stringify(rows.map((r) => [r.economic_date_jst, r.seller_sku_norm])));
  assert.equal(rows.filter((r) => r.seller_sku_norm === 'sku-n1' && at([r], '2026-06-05', 'sku-n1')).length, 1);   // 3 つの表記が 1 行に
  assert.deepEqual(n5.received_listing_ids.map(Number), [listingN1]);
  assert.equal(n5.received_listing_unresolved_count, 1);
  const n = (x) => Number(x);
  assert.deepEqual([n5.units_ordered, n(n5.sales_principal_jpy), n(n5.commission_jpy), n(n5.closing_fee_jpy), n(n5.misc_fee_jpy), n(n5.other_amount_jpy)], [4, 3500, 200, 30, 5, 0]);
  assert.equal(n(n5.profit_before_cogs_jpy), 3500 - 200 - 30);   // §3.5b: closing_fee も引く
  assert.deepEqual([n(n5.net_jpy), n(n5.unmapped_jpy)], [(2000 - 200 - 30) + (1000 + 5 - 7) + 500, -7]);
  assert.deepEqual([n5.unclassified_component_count, n(n5.unclassified_mapped_jpy), n(n5.unclassified_abs_jpy), n5.unmapped_component_count], [3, 5, 205, 1]);   // +100 / −100 は金額 0 でも 2 つ
  assert.deepEqual([n5.order_rows, n5.source_lines, n5.refund_units_status, Number(n5.units_refunded_customer_unrounded), n(n5.refund_unestimated_jpy)], [3, 3, 'no_refund', 0, 0]);
  const n10 = at(rows, '2026-06-10', 'sku-n1');
  assert.deepEqual([n10.units_refunded_customer, n10.units_net_sold, n(n10.refund_principal_jpy), n(n10.profit_before_cogs_jpy), n10.refund_units_status, String(n10.units_refunded_customer_unrounded)],
    [1, -1, 1000, -1000, 'estimated_monthly_unit_price', '1.000000']);
  assert.deepEqual([n10.received_listing_ids.map(Number), n10.received_listing_unresolved_count], [[listingN1], 0]);
});
await t('🚨 返品の状態の 4 つ: no_refund / estimated_monthly_unit_price / unit_price_missing (子にまとめると弱い方・推定できない額) / estimated_partial_month_unit_price (月が終わっていない)・丸める前の返品数', async () => {
  await apply('M1', 1, [row('2026-06-02', 'SKU-M', { units_ordered: 1, sales_principal_jpy: 1000 }), row('2026-06-15', 'SKU-M', { refund_principal_jpy: -1000, refund_principal_customer_jpy: -1000 })]);
  await apply('M2', 1, [row('2026-06-15', 'sku-m', { refund_principal_jpy: -300, refund_principal_atoz_jpy: -300 })]);
  await apply('Z1', 1, [row('2026-06-12', 'sku-zz', { refund_principal_jpy: -500, refund_principal_customer_jpy: -500 })]);
  await apply('F7', 1, [row('2026-06-03', 'sku-f7', { units_ordered: 3, sales_principal_jpy: 2 }), row('2026-06-20', 'sku-f7', { refund_principal_jpy: -1, refund_principal_customer_jpy: -1 })]);
  await apply('PP', 1, [row('2099-03-01', 'sku-p', { units_ordered: 1, sales_principal_jpy: 800 }), row('2099-03-02', 'sku-p', { refund_principal_jpy: -800, refund_principal_customer_jpy: -800 })]);
  const rows = await skuRange('2026-06-01', '2026-06-30');
  const m15 = at(rows, '2026-06-15', 'sku-m');
  assert.deepEqual([m15.refund_units_status, m15.units_refunded_customer, m15.units_a_to_z_refund, m15.units_refunded_customer_unrounded, m15.units_a_to_z_refund_unrounded, Number(m15.refund_unestimated_jpy), Number(m15.refund_principal_jpy)],
    ['unit_price_missing', 1, 0, null, null, 300, 1300]);   // 'SKU-M' は 1 個と推定できるが 'sku-m' の A-to-z 300 円は単価が無い = 子は unit_price_missing
  assert.equal(at(rows, '2026-06-02', 'sku-m').refund_units_status, 'no_refund');
  const z = at(rows, '2026-06-12', 'sku-zz');
  assert.deepEqual([z.refund_units_status, z.units_refunded_customer, Number(z.refund_unestimated_jpy)], ['unit_price_missing', 0, 500]);
  const f7 = at(rows, '2026-06-20', 'sku-f7');
  assert.deepEqual([f7.refund_units_status, f7.units_refunded_customer, String(f7.units_refunded_customer_unrounded)], ['estimated_monthly_unit_price', 1, '1.499999']);
  const p = at(await skuRange('2099-03-01', '2099-03-31'), '2099-03-02', 'sku-p');
  assert.deepEqual([p.refund_units_status, p.units_refunded_customer, String(p.units_refunded_customer_unrounded)], ['estimated_partial_month_unit_price', 1, '1.000000']);
  const all4 = new Set([...rows, p].map((r) => r.refund_units_status));
  for (const s of ['no_refund', 'estimated_monthly_unit_price', 'unit_price_missing', 'estimated_partial_month_unit_price']) assert.ok(all4.has(s), s);
});
await t('🚨 今の mart.finance_daily_range と同じ期間の金額・数量の合計が一致 (粒度だけ違う。closing_fee は新しい関数だけが持ち、その分だけ利益が小さい)', async () => {
  const cols = ['units_ordered', 'units_refunded_customer', 'units_marketplace_guarantee', 'units_a_to_z_refund', 'units_net_sold', 'sales_principal_jpy', 'sales_shipping_jpy', 'sales_giftwrap_jpy', 'sales_tax_jpy',
    'commission_jpy', 'fba_fulfillment_jpy', 'fba_storage_jpy', 'shipping_chargeback_jpy', 'giftwrap_chargeback_jpy', 'promotion_jpy', 'promotion_tax_jpy', 'points_jpy',
    'warehouse_damage_jpy', 'warehouse_lost_jpy', 'safe_t_jpy', 'refund_principal_jpy', 'reversal_reimbursement_jpy', 'misc_fee_jpy', 'other_fee_jpy', 'other_amount_jpy', 'source_lines', 'order_rows'];
  for (const [from, to] of [['2026-06-01', '2026-06-30'], ['2026-06-10', '2026-06-15'], ['2026-01-01', '2026-12-31'], ['2099-03-01', '2099-03-31']]) {
    const q = (fn) => one(`select ${[...cols, 'closing_fee_jpy', 'profit_before_cogs_jpy'].map((c) => `coalesce(sum(${c}), 0)::bigint::text as ${c}`).join(', ')} from mart.${fn}(1::smallint, 'amazon', 'jp', $1::date, $2::date)`, [from, to]);
    const a = await q('finance_daily_range'), b = await q('finance_daily_sku_range');
    for (const c of cols) assert.equal(b[c], a[c], `${from}〜${to} ${c}`);
    assert.equal(a.closing_fee_jpy, '0');   // 0045 は固定の 0
    assert.equal(Number(b.profit_before_cogs_jpy) + Number(b.closing_fee_jpy), Number(a.profit_before_cogs_jpy), `${from}〜${to} profit`);
  }
  const b6 = await one(`select sum(closing_fee_jpy)::int c from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', '2026-06-01', '2026-06-30')`);
  assert.equal(b6.c, 30);
});
await t('契約: from <= to・最大 400 日 (両端を含む)・期間の月だけを読む・nested loop を使わない・別の scope は 0 行', async () => {
  await rejects(() => skuRange('2026-06-02', '2026-06-01'), /invalid_input: from/);
  assert.ok(Array.isArray(await skuRange('2026-01-01', '2027-02-04')));   // 400 日
  await rejects(() => skuRange('2026-01-01', '2027-02-05'), /400 日/);
  await rejects(() => pg.query(`select * from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', null, '2026-06-01'::date)`), /invalid_input/);
  const cfg = (await one(`select array_to_string(proconfig, ',') as c from pg_proc where proname = 'finance_daily_sku_range'`)).c;
  assert.match(String(cfg), /enable_nestloop=off/);
  assert.equal(await num(`select count(*) as n from mart.finance_daily_sku_range(1::smallint, 'amazon', 'us', '2026-06-01', '2026-06-30')`), 0);
  // 期間の途中の日だけでも単価は月の全部の行 (6/10 だけ → 6/5 の売上から 1 個)
  const only = at(await skuRange('2026-06-10', '2026-06-10'), '2026-06-10', 'sku-n1');
  assert.deepEqual([only.units_refunded_customer, only.refund_units_status], [1, 'estimated_monthly_unit_price']);
  // 今の関数の戻りの型は変えない (R20 M4)
  const outs = (await one(`select pg_get_function_result(p.oid) as r from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'mart' and p.proname = 'finance_daily_range'`)).r;
  assert.ok(!/seller_sku_norm|unclassified|net_jpy/.test(outs), outs);
});

console.log(`\n${ok} 件 PASS${ng ? ` / ${ng} 件 NG` : ''}`);
process.exit(ng ? 1 : 0);
