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

const { allocateWarehouse, equalDays, findPendingSlips } = await import('../apps/fba-replenishment/self-reserve.js');

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
const csvOf = (lines) => {
  const head = ['店舗伝票番号', '商品コード', '受注数量'];
  return iconv.encode([head.join(','), ...lines.map(([c, q]) => `FBA1,${c},${q}`)].join('\r\n'), 'Shift_JIS');
};
const H = 3600e3;
const now = Date.parse('2026-09-24T12:00:00+09:00');
const ex = (id, createdMs, lines, skus) => ({ id, filename: `f${id}.csv`, created_at: String(id), createdMs, file_data: csvOf(lines), sku_list: JSON.stringify(skus) });
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
await t('同じ中身の再ダウンロードは 1 回と数える / 見る期間より古い出力は見ない', () => {
  const r = findPendingSlips({
    exports: [
      ex(1, now - 20 * H, [['aa', 10]], ['SKU-A']),
      ex(2, now - 19 * H, [['aa', 10]], ['SKU-A']),
      ex(3, now - 15 * 24 * H, [['zz', 99]], ['SKU-Z']),
    ],
    shipments: [], warehouseUploadedMs: now, inboundLastSyncMs: now, nowMs: now, lookbackDays: 10,
  });
  assert.equal(r.byCode.get('aa'), 10);
  assert.equal(r.byCode.has('zz'), false);
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
