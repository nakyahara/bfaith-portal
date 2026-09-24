/**
 * FBA 補充: 倉庫在庫の配分 (自社出荷ぶんを残す = FBA と自社を同じ日数分に / 同じ NE 商品の取り合いを止める)
 *   node scripts/test-fba-self-reserve.mjs
 * 一時 DB (DATA_DIR = OS の一時フォルダ) を使うので本番データに触らない。
 */
import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import iconv from 'iconv-lite';
import vm from 'node:vm';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-self-reserve-'));
process.env.FBA_SKU_MAPPING_SOURCE = 'sheet';

const { allocateWarehouse, equalDays, findPendingSlips, shipmentSinceJstDate } = await import('../apps/fba-replenishment/self-reserve.js');

let pass = 0;
const t = async (name, fn) => { await fn(); pass++; console.log(`  ok  ${name}`); };
const norm = (v) => String(v ?? '').trim().toLowerCase();

// item を作る (配分に要る項目だけ)
const item = (sku, o = {}) => ({
  amazon_sku: sku, urgency_score: 50, units_sold_30d: 60, effective_fba_stock: 0, daily_sales: 2,
  adjusted_qty: 0, recommended_qty: 0, is_expiry_managed: false, alerts: [],
  _units: [{ code: 'x', qty: 1 }], _expiry: [], ...o,
  ...(o.qty !== undefined ? { adjusted_qty: o.qty, recommended_qty: o.qty } : {}),
});
const run = (items, { W = {}, pending = {}, self = {}, excluded = [], minDays = 7 } = {}) => allocateWarehouse(items, {
  warehouseOf: (c) => W[c] ?? 0,
  pendingOf: (c) => pending[c] ?? 0,
  selfDailyOf: (c) => (c in self ? self[c] : null),
  excluded: new Set(excluded.map(norm)), norm, minShipmentDays: minDays,
});

console.log('--- 同じ日数にそろえる ---');
await t('FBA と自社が同じ日数分になるところまでしか FBA に送らない', () => {
  // 倉庫 100 / FBA 日販 2・FBA 在庫 0 / 自社日販 1 → 2T + T = 100 → T = 33.3 日 → FBA へ 66 個
  const a = item('a', { qty: 90 });
  run([a], { W: { x: 100 }, self: { x: 1 } });
  assert.equal(a.adjusted_qty, 66);
  assert.equal(a.recommended_qty, 66);
  assert.equal(a.allocation.self_cut, 24);
  assert.equal(a.non_fba_reserve, 24);
  assert.equal(a.allocation.units[0].equal_days, 33.3);
  assert.ok(a.alerts.some((x) => x.type === 'self_reserve'));
});
await t('推奨数は削るだけ。上限より少なければそのまま', () => {
  const a = item('a', { qty: 30 });
  run([a], { W: { x: 100 }, self: { x: 1 } });
  assert.equal(a.adjusted_qty, 30);
  assert.equal(a.allocation.self_cut, 0);
  assert.equal(a.qty_before_allocation, undefined);
});
await t('自社日販が分からない (null) / 0 なら自社ぶんの上限はかけない', () => {
  const a = item('a', { qty: 90 }); run([a], { W: { x: 100 } });
  assert.equal(a.adjusted_qty, 90);
  const b = item('b', { qty: 90 }); run([b], { W: { x: 100 }, self: { x: 0 } });
  assert.equal(b.adjusted_qty, 90);
});
await t('🚨 売れないセットの FBA 在庫が、売れる単品の補充を止めない (Codex High 1)', () => {
  // 単品: FBA 在庫 0・日販 10 / セット (同じ構成品 1 個): 構成品換算の FBA 在庫 1000・販売 0 / 倉庫 100・自社日販 1
  // 構成品でまとめて F を足すと A = 0 になる。SKU ごとの不足で見ると 10T + T = 100 → T = 9.09 → 単品へ 90
  const single = item('single', { qty: 100, units_sold_30d: 300, daily_sales: 10, urgency_score: 90 });
  const set = item('set', { qty: 0, units_sold_30d: 0, effective_fba_stock: 1000, daily_sales: 0 });
  run([single, set], { W: { x: 100 }, self: { x: 1 } });
  assert.equal(single.adjusted_qty, 90);
});
await t('どの SKU も FBA 在庫が十分なら、そろう日数 = 倉庫 ÷ 自社日販', () => {
  assert.equal(equalDays({ W: 100, rS: 2, members: [{ q: 1, rF: 1, F: 500 }] }), 50);
  assert.equal(equalDays({ W: 100, rS: null, members: [] }), Infinity);
  assert.equal(equalDays({ W: 0, rS: 1, members: [{ q: 1, rF: 1, F: 0 }] }), 0);
});

console.log('--- 同じ NE 商品の取り合い ---');
await t('緊急度の高い SKU から倉庫在庫を配り、合計が倉庫在庫を超えない', () => {
  const a = item('a', { qty: 80, urgency_score: 90, daily_sales: 1 });
  const b = item('b', { qty: 80, urgency_score: 10, daily_sales: 1 });
  run([b, a], { W: { x: 100 } });
  assert.equal(a.adjusted_qty, 80);
  assert.equal(b.adjusted_qty, 20);
  assert.equal(b.allocation.shared_cut, 60);
  assert.equal(b.allocation.self_cut, 0);
  assert.equal(b.non_fba_reserve, 0);        // 他 SKU に配ったぶんは「自社に残す」に入れない (Codex Medium 7)
});
await t('自社日販が取れなくても、取り合いの歯止めは常にかかる (Codex High 3)', () => {
  const a = item('a', { qty: 80, urgency_score: 90, daily_sales: 1 });
  const b = item('b', { qty: 80, urgency_score: 10, daily_sales: 1 });
  run([a, b], { W: { x: 100 }, self: {} });
  assert.equal(a.adjusted_qty + b.adjusted_qty, 100);
});
await t('緊急度が同じなら SKU 順 (結果が並び順で変わらない)', () => {
  const mk = () => [item('zz', { qty: 80, daily_sales: 1 }), item('aa', { qty: 80, daily_sales: 1 })];
  const [z1, a1] = mk(); run([z1, a1], { W: { x: 100 } });
  const [z2, a2] = mk(); run([a2, z2], { W: { x: 100 } });
  assert.equal(a1.adjusted_qty, 80); assert.equal(a2.adjusted_qty, 80);
  assert.equal(z1.adjusted_qty, 20); assert.equal(z2.adjusted_qty, 20);
});
await t('🚨 恒久除外の SKU は在庫を食わない (Codex High 3)', () => {
  const ex = item('EXCL', { qty: 80, urgency_score: 99, daily_sales: 1 });
  const b = item('b', { qty: 50, urgency_score: 10, daily_sales: 1 });
  run([ex, b], { W: { x: 100 }, excluded: ['excl'] });
  assert.equal(b.adjusted_qty, 50);
  assert.equal(ex.allocation, undefined);
});
await t('セット (1 個あたり構成品 2 個) は構成品の個数で倉庫在庫を引く', () => {
  const s = item('set2', { qty: 80, daily_sales: 1, _units: [{ code: 'x', qty: 2 }] });
  run([s], { W: { x: 100 } });
  assert.equal(s.adjusted_qty, 50);
});
await t('🚨 期限ごとの在庫も SKU どうしで取り合わない (Codex High 6)', () => {
  const exp = [{ code: 'x', expiry: '2027-01-01', total: 50 }];
  const a = item('a', { qty: 40, urgency_score: 90, daily_sales: 1, is_expiry_managed: true, _expiry: exp });
  const b = item('b', { qty: 40, urgency_score: 10, daily_sales: 1, is_expiry_managed: true, _expiry: exp });
  run([a, b], { W: { x: 1000 } });
  assert.equal(a.adjusted_qty, 40);
  assert.equal(b.adjusted_qty, 10);
});
await t('🚨 出荷待ちの FBA 伝票は、期限ごとの在庫 (最古ロット) からも引く (Codex R1 High 3)', () => {
  // 倉庫 100 = 先の期限 50 + 後の期限 50。先の期限から出荷待ち 40 → 同じ期限で送れるのは 10
  const a = item('a', { qty: 50, daily_sales: 1, is_expiry_managed: true, _expiry: [{ code: 'x', expiry: '2027-01-01', total: 50 }] });
  run([a], { W: { x: 100 }, pending: { x: 40 } });
  assert.equal(a.adjusted_qty, 10);
});
await t('出荷待ちの FBA 伝票ぶんは倉庫在庫から引く', () => {
  const a = item('a', { qty: 90, daily_sales: 1 });
  run([a], { W: { x: 100 }, pending: { x: 60 } });
  assert.equal(a.adjusted_qty, 40);
  assert.equal(a.allocation.units[0].pending_fba_slips, 60);
  assert.equal(a.allocation.units[0].free, 40);
});

console.log('--- 0 と最低出荷日数 ---');
await t('🚨 0 にした行は adjusted_qty も recommended_qty も 0 (元の推奨数が生き返らない。Codex High 5)', () => {
  const a = item('a', { qty: 50, daily_sales: 1, effective_fba_stock: 500 });   // FBA 在庫が十分 → 自社ぶんで 0
  run([a], { W: { x: 100 }, self: { x: 1 } });
  assert.equal(a.adjusted_qty, 0);
  assert.equal(a.recommended_qty, 0);
  assert.equal(a.allocation.after, 0);
});
await t('減らした結果が最低出荷日数に満たなければ 0 (期限商品は除く)', () => {
  const a = item('a', { qty: 30, daily_sales: 1, effective_fba_stock: 10 });
  run([a], { W: { x: 3 } });
  assert.equal(a.adjusted_qty, 0);
  assert.equal(a.allocation.min_days_cut, 3);
  assert.equal(a.skipped_min_days, true);
  const b = item('b', { qty: 30, daily_sales: 1, effective_fba_stock: 10, is_expiry_managed: true });
  run([b], { W: { x: 3 } });
  assert.equal(b.adjusted_qty, 3);
});

console.log('--- 出荷待ちの FBA 伝票 ---');
const csvOf = (lines, orderNo) => {
  const head = ['店舗伝票番号', '商品コード', '受注数量'];
  return iconv.encode([head.join(','), ...lines.map(([c, q]) => `${orderNo},${c},${q}`)].join('\r\n'), 'Shift_JIS');
};
const H = 3600e3;
const now = Date.parse('2026-09-24T12:00:00+09:00');
const ex = (id, createdMs, lines, skus, orderNo = `FBA${id}`) => ({ id, filename: `f${id}.csv`, created_at: String(id), createdMs, file_data: csvOf(lines, orderNo), sku_list: JSON.stringify(skus) });
await t('Amazon の納品に出た伝票は引かず、出ていない伝票だけ構成品ごとに足す', () => {
  const r = findPendingSlips({
    exports: [
      ex(1, now - 70 * H, [['aa', 10], ['bb', 5]], ['SKU-A', 'SKU-B']),   // 出た (倉庫 CSV の前に納品あり)
      ex(2, now - 40 * H, [['aa', 7]], ['SKU-A2']),                     // まだ
      ex(3, now - 1 * H, [['cc', 3]], ['SKU-C']),                       // 倉庫 CSV より後に出力 → まだ
    ],
    shipments: [{ atMs: now - 50 * H, skus: new Set(['sku-a', 'sku-b']) }],
    warehouseUploadedMs: now - 3 * H, inboundLastSyncMs: now - 6 * H, nowMs: now, lookbackDays: 10,
  });
  assert.equal(r.status, 'ok');
  assert.deepEqual([...r.byCode].sort(), [['aa', 7], ['cc', 3]]);
  assert.equal(r.slips.length, 2);
});
await t('倉庫 CSV の取り込みより後にできた納品は「出た」に数えない (CSV にはまだ在庫が残っている)', () => {
  const r = findPendingSlips({
    exports: [ex(1, now - 70 * H, [['aa', 10]], ['SKU-A'])],
    shipments: [{ atMs: now - 1 * H, skus: new Set(['sku-a']) }],
    warehouseUploadedMs: now - 3 * H, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(r.byCode.get('aa'), 10);
});
await t('同じ伝票 (店舗伝票番号) は 1 回と数える / 見る期間より古い出力は見ない', () => {
  const r = findPendingSlips({
    exports: [
      ex(1, now - 20 * H, [['aa', 10]], ['SKU-A'], 'FBA202609230100'),
      ex(2, now - 19 * H, [['aa', 10]], ['SKU-A'], 'FBA202609230100'),
      ex(3, now - 15 * 24 * H, [['zz', 99]], ['SKU-Z']),
    ],
    shipments: [], warehouseUploadedMs: now, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(r.byCode.get('aa'), 10);
  assert.equal(r.byCode.has('zz'), false);
});
await t('🚨 別の伝票なら中身が同じでも足す。片方だけ出ていれば、出ていない方だけ引く (Codex R1 High 2)', () => {
  const both = findPendingSlips({
    exports: [ex(1, now - 50 * H, [['aa', 10]], ['SKU-A']), ex(2, now - 20 * H, [['aa', 10]], ['SKU-A'])],
    shipments: [], warehouseUploadedMs: now, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(both.byCode.get('aa'), 20);
  const one = findPendingSlips({
    exports: [ex(1, now - 50 * H, [['aa', 10]], ['SKU-A']), ex(2, now - 20 * H, [['aa', 10]], ['SKU-A'])],
    shipments: [{ atMs: now - 45 * H, skus: new Set(['sku-a']) }],   // 1 つ目の伝票の納品 (2 つ目の 25 時間前)
    warehouseUploadedMs: now - 1 * H, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(one.byCode.get('aa'), 10);
  assert.deepEqual(one.slips.map((x) => x.order_no), ['FBA2']);
});
await t('🚨 画面の手順 (Amazon のプランを確定 → NE CSV 出力) だと納品が少し先にできる → 12 時間前までは同じ伝票の納品 (Codex R1 High 1)', () => {
  const r = findPendingSlips({
    exports: [ex(1, now - 30 * H, [['aa', 10]], ['SKU-A'])],
    shipments: [{ atMs: now - 32 * H, skus: new Set(['sku-a']) }],   // 伝票の 2 時間前にできた納品
    warehouseUploadedMs: now - 2 * H, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(r.byCode.size, 0);
  const prevDay = findPendingSlips({
    exports: [ex(1, now - 30 * H, [['aa', 10]], ['SKU-A'])],
    shipments: [{ atMs: now - 55 * H, skus: new Set(['sku-a']) }],   // 前日の便 (25 時間前) は拾わない
    warehouseUploadedMs: now - 2 * H, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(prevDay.byCode.get('aa'), 10);
});
await t('🚨 1 つの納品で 2 つの伝票を「出た」にしない (Codex R2 High 1)', () => {
  // 伝票 A (10 時間前) → A の納品 (8 時間前) → 伝票 B (2 時間前、同じ SKU・まだ出ていない)
  const r = findPendingSlips({
    exports: [ex(1, now - 10 * H, [['aa', 10]], ['SKU-A']), ex(2, now - 2 * H, [['aa', 25]], ['SKU-A'])],
    shipments: [{ atMs: now - 8 * H, skus: new Set(['sku-a']) }],
    warehouseUploadedMs: now - 1 * H, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.deepEqual(r.slips.map((x) => x.order_no), ['FBA2']);
  assert.equal(r.byCode.get('aa'), 25);
});
await t('ふだんの順 (伝票 → 約 2 日後に納品) で、次の日の伝票に納品を取られない', () => {
  const r = findPendingSlips({
    exports: [ex(1, now - 48 * H, [['aa', 10]], ['SKU-A', 'SKU-B']), ex(2, now - 24 * H, [['cc', 5]], ['SKU-C', 'SKU-B'])],
    shipments: [{ atMs: now - 3 * H, skus: new Set(['sku-a', 'sku-b']) }],   // 伝票 1 の納品 (伝票 2 とも SKU-B が重なる)
    warehouseUploadedMs: now - 1 * H, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.deepEqual(r.slips.map((x) => x.order_no), ['FBA2']);
});
await t('🚨 同じ伝票番号で中身が違う出力 (同じ分に出し直し) は、商品ごとに多い方を採る (Codex R2 High 2)', () => {
  const r = findPendingSlips({
    exports: [
      ex(1, now - 5 * H, [['aa', 10], ['bb', 4]], ['SKU-A'], 'FBA202609240300'),
      ex(2, now - 5 * H + 20e3, [['aa', 30]], ['SKU-A'], 'FBA202609240300'),
    ],
    shipments: [], warehouseUploadedMs: now, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(r.slips.length, 1);
  assert.equal(r.byCode.get('aa'), 30);
  assert.equal(r.byCode.get('bb'), 4);
});
await t('Amazon の納品を DB から取る下限は、伝票の 12 時間前までさかのぼる (日付の境目。Codex R2 Medium 3)', () => {
  // 9/24 06:00 JST から 10 日前 = 9/14 06:00 → 12 時間前 = 9/13 18:00 → 9/13 から取る
  assert.equal(shipmentSinceJstDate(Date.parse('2026-09-24T06:00:00+09:00'), 10), '2026-09-13');
  assert.equal(shipmentSinceJstDate(Date.parse('2026-09-24T20:00:00+09:00'), 10), '2026-09-14');
});
await t('納品実績が 2 日以上古い / 倉庫在庫が無いときは状態で知らせる', () => {
  const base = { exports: [], shipments: [], nowMs: now, lookbackDays: 10 };
  assert.equal(findPendingSlips({ ...base, warehouseUploadedMs: now, inboundLastSyncMs: now - 3 * 24 * H }).status, 'inbound_stale');
  assert.equal(findPendingSlips({ ...base, warehouseUploadedMs: null, inboundLastSyncMs: now }).status, 'no_warehouse');
});

console.log('--- 計算エンジンを通す ---');
const db = await import('../apps/fba-replenishment/db.js');
await db.initDb();
db.upsertSkuMappings([
  { amazon_sku: 'ONE', product_name: '単品', ne_code: 'shared', logizard_code: 'shared' },
  { amazon_sku: 'PACK2', product_name: '2個セット', ne_code: 'shared', logizard_code: 'shared', is_set: true, set_components: [{ ne_code: 'shared', qty: 2 }] },
]);
db.saveRestockLatest([
  { amazon_sku: 'ONE', product_name: '単品', fba_available: 0, units_sold_30d: 300, amazon_recommended_qty: null },
  { amazon_sku: 'PACK2', product_name: '2個セット', fba_available: 0, units_sold_30d: 90, amazon_recommended_qty: null },
]);
db.replaceWarehouseInventory([
  { logizard_code: 'shared', product_name: '共有', location: 'A-01', quantity: 400, reserved: 0, available_qty: 400, expiry_date: '', block_alloc_order: 1 },
]);
const { generateRecommendations } = await import('../apps/fba-replenishment/calculation-engine.js');
const engine = (opts) => generateRecommendations(true, {}, { excluded: [], pendingSlips: { status: 'ok', slips: [], byCode: new Map() }, ...opts });

await t('上限なし (自社日販が取れない) でも、単品とセットの合計は倉庫在庫 400 個を超えない', () => {
  const r = engine({ selfShipSales: { status: 'unavailable', map: null, error: 'test' } });
  const one = r.items.find((i) => i.amazon_sku === 'ONE'), pack = r.items.find((i) => i.amazon_sku === 'PACK2');
  assert.ok(one.adjusted_qty + pack.adjusted_qty * 2 <= 400, `${one.adjusted_qty} + ${pack.adjusted_qty}×2`);
  assert.equal(r.data_quality.allocation.self_sales.used, false);
  assert.equal(r.data_quality.allocation.self_sales.status, 'unavailable');
  assert.equal(one._units, undefined);             // 材料は応答に出さない
});
await t('自社日販があると自社ぶんを残し、計算過程 (Step11) と要約が出る', () => {
  const r = engine({ selfShipSales: { status: 'ok', as_of: '2026-09-23', age_days: 1, map: new Map([['shared', 300]]), invalid: [] } });
  const one = r.items.find((i) => i.amazon_sku === 'ONE'), pack = r.items.find((i) => i.amazon_sku === 'PACK2');
  const used = one.adjusted_qty + pack.adjusted_qty * 2;
  // FBA 日販 = 10 + 3×2 = 16 / 自社 10 → そろう日数 T = 400 / 26 = 15.4 → FBA に回るのは約 246 以下
  assert.ok(used <= 247, `FBA に ${used}`);
  assert.ok(400 - used >= 150, `倉庫に ${400 - used} 残る`);
  assert.ok(one.calc_steps.some((s) => s.startsWith('[Step11]')));
  assert.equal(r.data_quality.allocation.self_sales.used, true);
  assert.ok(r.data_quality.allocation.cut.units_self > 0);
  assert.equal(r.recommended_units, r.items.filter((i) => i.recommended_qty > 0).reduce((s, i) => s + i.recommended_qty, 0));
});
await t('設定 self_reserve_mode=off で自社ぶんの上限だけ止まる', () => {
  db.updateSetting('self_reserve_mode', 'off');
  const r = engine({ selfShipSales: { status: 'ok', map: new Map([['shared', 300]]), invalid: [] } });
  assert.equal(r.data_quality.allocation.mode, 'off');
  assert.equal(r.data_quality.allocation.cut.units_self, 0);
  const one = r.items.find((i) => i.amazon_sku === 'ONE'), pack = r.items.find((i) => i.amazon_sku === 'PACK2');
  assert.ok(one.adjusted_qty + pack.adjusted_qty * 2 <= 400);
  db.updateSetting('self_reserve_mode', 'equal_days');
});
await t('商品管理リストに行が無い構成品は「分からない」として上限をかけず、一覧に出す', () => {
  const r = engine({ selfShipSales: { status: 'ok', map: new Map([['other', 5]]), invalid: [] } });
  assert.deepEqual(r.data_quality.allocation.self_sales.missing_codes, ['shared']);
  assert.equal(r.data_quality.allocation.cut.units_self, 0);
});
await t('出荷待ちの FBA 伝票は計算エンジンでも倉庫在庫から引かれる', () => {
  const r = engine({ selfShipSales: { status: 'unavailable', map: null }, pendingSlips: { status: 'ok', slips: [{ qty: 300 }], byCode: new Map([['shared', 300]]) } });
  const one = r.items.find((i) => i.amazon_sku === 'ONE'), pack = r.items.find((i) => i.amazon_sku === 'PACK2');
  assert.ok(one.adjusted_qty + pack.adjusted_qty * 2 <= 100);
  assert.equal(r.data_quality.allocation.pending_slips.units, 300);
});

console.log('--- 自社日販 (商品管理リスト) の取り出し ---');
const mirror = await import('../apps/warehouse-mirror/db.js');
mirror.initMirrorDB();
const mdb = mirror.getMirrorDB();
const todayJst = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
const daysAgo = (n) => new Date(Date.parse(todayJst) - n * 86400000).toISOString().slice(0, 10);
const publish = ({ status, velocityAsOf, rows }) => {
  mdb.prepare('DELETE FROM mirror_pml_snapshot_rows').run();
  mdb.prepare('DELETE FROM mirror_pml_published').run();
  const ins = mdb.prepare('INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 販売数30日_FBA以外) VALUES (?, ?, ?)');
  for (const [c, n] of rows) ins.run('run1', c, n);
  mdb.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, src_velocity_as_of, row_count, synced_at)
    VALUES (1, 'run1', ?, ?, ?, ?, datetime('now'))`).run(status, velocityAsOf, velocityAsOf, rows.length);
};
await t('構成品ごとに返す。正常な 0 は 0、値がおかしい行は「分からない」(map に入れない)', () => {
  publish({ status: 'ok', velocityAsOf: daysAgo(1), rows: [['AA', 30], ['bb', 0], ['cc', null], ['dd', -3]] });
  const r = db.getSelfShipSalesByCode({ maxAgeDays: 7 });
  assert.equal(r.status, 'ok');
  assert.equal(r.map.get('aa'), 30);
  assert.equal(r.map.get('bb'), 0);
  assert.equal(r.map.has('cc'), false);
  assert.deepEqual(r.invalid.sort(), ['cc', 'dd']);
  assert.equal(r.age_days, 1);
});
await t('🚨 partial (FBA 在庫だけ古い) でも自社日販は使う / failed は使わない (Codex R1 Medium 4)', () => {
  publish({ status: 'partial', velocityAsOf: daysAgo(1), rows: [['aa', 30]] });
  assert.equal(db.getSelfShipSalesByCode().status, 'ok');
  publish({ status: 'failed', velocityAsOf: daysAgo(1), rows: [['aa', 30]] });
  const f = db.getSelfShipSalesByCode();
  assert.equal(f.status, 'unavailable');
  assert.equal(f.map, null);
});
await t('販売データの日付が古ければ stale (エンジンは自社ぶんの上限をかけない)', () => {
  publish({ status: 'ok', velocityAsOf: daysAgo(9), rows: [['aa', 30]] });
  assert.equal(db.getSelfShipSalesByCode({ maxAgeDays: 7 }).status, 'stale');
  const r = generateRecommendations(false, {}, { excluded: [], pendingSlips: { status: 'ok', slips: [], byCode: new Map() } });
  assert.equal(r.data_quality.allocation.self_sales.used, false);
  assert.equal(r.data_quality.allocation.self_sales.status, 'stale');
});

console.log('--- 画面 ---');
await t('FBA 補充の画面のスクリプトが構文として通る (配分の列・注意書きを足したため)', () => {
  const html = fs.readFileSync(new URL('../views/fba-replenishment.ejs', import.meta.url), 'utf8');
  const scripts = [...html.matchAll(/<script(?![^>]*src)[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1].replace(/<%[\s\S]*?%>/g, '0'));
  assert.ok(scripts.length > 0);
  for (const code of scripts) new vm.Script(`(function () {
${code}
})`);   // 構文だけ確かめる (実行はしない)。構文エラーなら throw
  assert.ok(html.includes('${allocCells(r)}'));
});

console.log(`\n${pass} 件 PASS`);
