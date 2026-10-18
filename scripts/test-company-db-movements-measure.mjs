/**
 * test-company-db-movements-measure.mjs — PR-0 の測るスクリプト (scripts/company-db/movements-measure.mjs) の試験
 *   区間の計算 (純粋な関数) と、PGlite の上で全部の問い合わせが通ること (読み取り専用の取引の中で) を確かめる。
 *
 * 使い方: node scripts/test-company-db-movements-measure.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { captureLogizardInventory, closeStockDays } from '../apps/company-db/inventory/logizard.mjs';
import { intervalsOf, statsOf, sizeBucket, measure, summaryJa } from './company-db/movements-measure.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 6).join('\n      ')); } };
const quiet = () => {};

console.log('区間の計算');
await t('出荷を取得時刻で切る (time) と出荷確定日で切る (date)。合わない分 = 在庫の増減 + 出荷', () => {
  const days = [{ day: '2026-09-01', gen: '2026-09-01T09:00:00Z' }, { day: '2026-09-02', gen: '2026-09-02T09:00:00Z' }, { day: '2026-09-03', gen: '2026-09-03T09:00:00Z' }];
  const stock = new Map([['2026-09-01', 10], ['2026-09-02', 7], ['2026-09-03', 7]]);
  // 9/02 18:30 JST に 3 個を確定 (取得 18:00 JST の後) = 在庫は 9/02 に減っているのに、出荷は 9/03 の区間に入る
  const ships = [{ at: '2026-09-02T09:30:00Z', date: '2026-09-02', qty: 3 }];
  const iv = intervalsOf(days, stock, ships);
  assert.deepEqual(iv.map((x) => [x.to, x.delta, x.shippedTime, x.unTime, x.shippedDate, x.unDate]), [['2026-09-02', -3, 0, -3, 3, 0], ['2026-09-03', 0, 3, 3, 0, 0]]);
  const s = statsOf(iv.map((x) => x.unTime));
  assert.deepEqual([s.intervals, s.nonzero, s.pos, s.neg, s.pairs, s.pairQty, s.nonzeroAfterPairs, s.oppositeNext], [2, 2, 1, 1, 1, 3, 0, 1]);
});
await t('complete の日に行が無い = 0。取得時刻の無い日は time を null (date だけ数える)', () => {
  const iv = intervalsOf([{ day: '2026-09-01', gen: null }, { day: '2026-09-03', gen: '2026-09-03T09:00:00Z' }], new Map([['2026-09-01', 4]]), []);
  assert.deepEqual(iv.map((x) => [x.gapDays, x.delta, x.unTime, x.unDate]), [[2, -4, null, -4]]);
  assert.equal(statsOf([null, -4]).intervals, 1);
});
await t('大きさの段', () => { assert.deepEqual([0, 1, -2, 5, 6, 20, 21, 100, 101].map(sizeBucket), ['0', '1', '2-5', '2-5', '6-20', '6-20', '21-100', '21-100', '101+']); });

console.log('PGlite で問い合わせが全部通る');
const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
await pg.query(`insert into core.products (company_id, name) values (1, '見本A'), (1, '見本B')`);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select 1, product_id, 'single', case name when '見本A' then 'AAA-1' else 'BBB-2' end, name from core.products`);
const skuA = (await one(`select sku_id from core.skus where code = 'AAA-1'`)).sku_id;
const row = (id, qty) => ({ '商品ID': id, '商品名': id, 'バーコード': '4900000000001', 'ブロック略称': 'P3FA', 'ロケ': '001', '品質区分名': '良品', '有効期限': '', '入荷日': '2026/08/01', '在庫数': qty, '引当数': 0, 'ロケ業務区分': 'ピック', '最終入荷日': '2026/08/01', '最終出荷日': null, '在庫日': '2026/09/01' });
for (const [d, a] of [['2026-09-05', 10], ['2026-09-06', 7], ['2026-09-07', 7], ['2026-09-08', 12]]) {
  await captureLogizardInventory(db, { rows: [row('AAA-1', a), row('BBB-2', 5)], capturedAt: `${d}T09:00:00.000Z`, log: quiet });
}
await closeStockDays(db, { todayJst: '2026-09-09', log: quiet });
let seq = 0;
const ship = async (slip, at, qty, shop = '1') => {
  const s = await one(`insert into core.shipments (company_id, ne_slip_no, shop_code, status, shipped_at, ship_date_jst, order_date_jst, received_batch_seq, source_updated_at, transform_version, content_hash)
    values (1, $1, $2, 'shipped', $3::timestamptz, ($3::timestamptz at time zone 'Asia/Tokyo')::date, ($3::timestamptz at time zone 'Asia/Tokyo')::date, $4, now(), 't', 'h') returning shipment_id`, [slip, shop, at, ++seq]);
  await pg.query(`insert into core.shipment_lines (company_id, shipment_id, line_no, sku_id, qty, received_batch_seq) values (1, $1, '1', $2, $3, $4)`, [s.shipment_id, skuA, qty, seq]);
};
await ship('S1', '2026-09-06T09:30:00Z', 3);           // 取得 (18:00 JST) の後に確定 = 9/06 −3 / 9/07 +3 の組
await ship('S2', '2026-09-08T03:00:00Z', 2, '15');     // FBA納品
await t('measure: 表ごとの数・取得時刻・見本・組・速さが出る (読み取り専用の取引で)', async () => {
  await pg.query('begin read only');
  let r;
  try { r = await measure(db, { days: 7, sample: 5, now: new Date('2026-09-09T01:00:00Z') }); } finally { await pg.query('rollback'); }
  assert.equal(r.sources.shipments.rows, 2);
  assert.equal(r.capture.logizard.complete, 4);
  assert.deepEqual(r.capture.logizard.gen_hour_jst, { 18: 4 });
  assert.equal(r.sample.skus, 1);
  // 9/06: −3 (出荷 0) → 合わない −3 / 9/07: 0 (出荷 3) → +3 / 9/08: +5 (出荷 2) → +7 (仕入のはず)
  const lt = r.unexplained.logizard_time;
  assert.deepEqual([lt.intervals, lt.nonzero, lt.pairs, lt.posSum, lt.negSum, lt.nonzeroAfterPairs], [3, 3, 1, 10, -3, 1]);
  const ld = r.unexplained.logizard_date;
  assert.deepEqual([ld.nonzero, ld.posSum], [1, 7]);
  assert.equal(r.unexplained.ne_time.intervals, 0, 'NE の日次は無い');
  assert.ok((r.shipments_by_shop.find((x) => x.shop_code === '15') || {}).slips === 1);
  assert.ok(Number.isFinite(r.speed.shipments_13m.ms));
  assert.match(summaryJa(r), /logizard_time: 区間 3 のうち合わない 3/);
});

console.log(`\n${ok} ok / ${ng} NG`);
if (ng) process.exit(1);
