#!/usr/bin/env node
/**
 * 米国FBA在庫補充 PR2 (日本優先の配分・表示だけ) の試験。DB・SP-API・miniPC に行かない。
 *   node scripts/test-fba-us-allocation.mjs
 * 受け入れ条件 = Codex PR2 設計レビューの数値例 (設計方針 §9.5)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-us-alloc-'));
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);
const { computeUsAllocation, parseComponents, US_TARGET_DAYS, US_REORDER_DAYS, SELF_PROTECT_DAYS } = await imp('apps/fba-replenishment-us/allocation.js');

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}\n     ${e.stack.split('\n').slice(0, 5).join('\n     ')}`); }
}

const now = new Date('2026-09-25T03:00:00Z');
// 全部新しい・全部そろっている入力 (1 つずつ壊して試す)
const base = (over = {}) => ({
  now,
  usRows: [],
  usRestockFetchedAt: '2026-09-24T22:05:00Z',
  jpRestock: [],
  jpTargetDaysOf: () => 60,
  jpMappings: [],
  jpExcluded: new Set(),
  warehouse: [],
  selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map() },
  pending: { status: 'ok', byCode: new Map() },
  freshness: { jpRestockSourceAt: '2026-09-24 22:00:00', jpRestockSourceMissing: 0, warehouseUploadedAt: '2026-09-25 09:00:00' },
  ...over,
});
const usRow = (sku, sold, onHand, comps, route = 'master') => ({ sku, sold_30d_restock: sold, on_hand: onHand, mapping: { route, components: comps.map(([ne_code, qty]) => ({ ne_code, qty })) } });
const jpRow = (sku, sold, avail, over = {}) => ({ amazon_sku: sku, units_sold_30d: sold, fba_available: avail, fba_inbound_shipped: 0, fba_inbound_received: 0, fba_inbound_working: 0, ...over });
const map = (sku, comps) => ({ amazon_sku: sku, ne_code: comps[0][0], set_components: JSON.stringify(comps.map(([ne_code, qty]) => ({ ne_code, qty }))) });
const wh = (code, avail, expiry = null) => ({ logizard_code: code, warehouse_available: avail, earliest_expiry: expiry });
const selfOf = (entries) => ({ status: 'ok', as_of: '2026-09-24', map: new Map(entries) });

console.log('① 構成の読み方');
await t('構成: JSON 文字列・配列・単品 / 同じ構成品の重複行は合算 / 数量は正の整数だけ・不正は 1 個に補わない (Codex H3)', async () => {
  assert.deepEqual(parseComponents('[{"ne_code":"C","qty":20},{"ne_code":"c ","qty":20}]'), [{ code: 'c', qty: 40 }]);
  assert.deepEqual(parseComponents(null, 'Single'), [{ code: 'single', qty: 1 }]);
  assert.deepEqual(parseComponents([], 'x'), [{ code: 'x', qty: 1 }]);
  for (const bad of ['{broken', '[{"ne_code":"c"}]', '[{"ne_code":"c","qty":0}]', '[{"ne_code":"c","qty":1.5}]', '[{"ne_code":"","qty":1}]', '{"a":1}']) {
    assert.equal(parseComponents(bad, 'fallback'), null, bad);
  }
  assert.equal(parseComponents(null, null), null);
});

console.log('② 日本に残す数');
await t('🚨 Codex H2 の数値例: 倉庫 100・日本の目標 60 日・日販 1・販売可能 0・準備中 60・伝票 0 → 準備中は日本の在庫に足さない = 不足 60 を残す → 米国に回せる数 40 (現案は 100 だった)', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])],
    jpRestock: [jpRow('jp-a', 30, 0, { fba_inbound_working: 60 })],
    jpMappings: [map('jp-a', [['c', 1]])],
    warehouse: [wh('c', 100)],
    selfShip: selfOf([['c', 0]]),
  }));
  const c = r.codes[0];
  assert.deepEqual([c.warehouse, c.jp_fba_short, c.jp_self, c.pool], [100, 60, 0, 40]);
  assert.equal(r.reference, false);
});
await t('日本 FBA の不足 = SKU ごとの目標日数 (jpTargetDaysOf) × 日販 − (販売可能+輸送中+受領中) を構成数倍して足す / 自社出荷は 60 日分 / 出荷待ち伝票を引く', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['cardstand-r', 20]])],
    jpRestock: [jpRow('B010100720510', 106, 99), jpRow('pr_x', 11, 63, { fba_inbound_shipped: 5, fba_inbound_received: 2 })],
    jpTargetDaysOf: (row) => (row.amazon_sku === 'B010100720510' ? 40 : 180),
    jpMappings: [map('b010100720510', [['cardstand-r', 40]]), map('PR_X', [['cardstand-r', 20]])],
    warehouse: [wh('Cardstand-R', 20000)],
    selfShip: selfOf([['cardstand-r', 300]]),
    pending: { status: 'ok', byCode: new Map([['cardstand-r', 800]]) },
  }));
  const c = r.codes[0];
  // B010: ceil(40*106/30 - 99) = ceil(141.33-99)=43 → ×40 = 1720 / pr_x: ceil(180*11/30 - 70) = ceil(66-70) → 0
  assert.deepEqual([c.jp_fba_short, c.jp_self, c.jp_pending, c.pool], [1720, SELF_PROTECT_DAYS * 10, 800, 20000 - 800 - 1720 - 600]);
  assert.deepEqual(c.jp_skus.map((s) => [s.sku, s.target_days, s.supply, s.short, s.qty_per]), [['B010100720510', 40, 99, 43, 40], ['pr_x', 180, 70, 0, 20]]);
});
await t('🚨 Codex H1: 構成が分からない日本 SKU (販売か在庫あり) は黙って外さず件数と一覧を返す / 動きの無い SKU は数えない / 構成が食い違う SKU も同じ', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])],
    jpRestock: [jpRow('no-map', 5, 0), jpRow('quiet', 0, 0), jpRow('conf', 1, 1)],
    jpMappings: [map('conf', [['c', 1]]), map('CONF', [['d', 1]])],
    warehouse: [wh('c', 10)],
    selfShip: selfOf([['c', 0]]),
  }));
  assert.equal(r.unattributed_jp_count, 2);
  assert.deepEqual(r.unattributed_jp.map((u) => [u.sku, u.why, u.blocked]), [['no-map', '構成が無い', false], ['conf', '構成が食い違う', true]]);
  // 米国の構成品に効きうるもの (判定不能にした) と、影響先が分からないもの (計算に入っていない) を分けて出す (R2 Low)
  assert.ok(r.notes.some((n) => /1 件は米国と同じ構成品を使っている可能性があるので、その構成品を「判定できない」に/.test(n)));
  assert.ok(r.notes.some((n) => /日本の SKU 1 件は構成が分からず、どの構成品を使うかも分からない/.test(n)));
});
await t('日本 SKU の 30日販売が取れない / FBA 在庫が取れない / 目標日数が分からない → その構成品は判定不能 (0 にしない)・恒久除外 SKU は不足 0', async () => {
  const mk = (jr, target = () => 60) => computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])], jpRestock: [jr], jpTargetDaysOf: target,
    jpMappings: [map('jp', [['c', 1]])], warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]),
  }));
  assert.match(mk(jpRow('jp', null, 5)).codes[0].unknown[0], /30日販売が取れていない/);
  assert.match(mk(jpRow('jp', 5, null)).codes[0].unknown[0], /FBA 在庫が取れていない/);
  assert.match(mk(jpRow('jp', 5, 1), () => null).codes[0].unknown[0], /目標日数が分からない/);
  const u = mk(jpRow('jp', null, 5));
  assert.deepEqual([u.codes[0].pool, u.us[0].status, u.us[0].give], [null, 'unknown', null]);
  const ex = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])], jpRestock: [jpRow('jp', 300, 0)], jpExcluded: new Set(['jp']),
    jpMappings: [map('jp', [['c', 1]])], warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]),
  }));
  assert.deepEqual([ex.codes[0].jp_fba_short, ex.codes[0].pool, ex.codes[0].jp_skus[0].excluded], [0, 100, true]);
});
await t('構成品ごとの判定不能: 自社販売に行が無い / 自社販売を読めない / 出荷待ちを数えられない / 期限管理品 / 倉庫 CSV に同じコードが 2 行 / 倉庫 CSV に行が無い = 0 (在庫なし)', async () => {
  const one = (over) => computeUsAllocation(base({ usRows: [usRow('us-a', 30, 0, [['c', 1]])], warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]), ...over })).codes[0];
  assert.match(one({ selfShip: selfOf([]) }).unknown[0], /自社出荷の販売に行が無い/);
  assert.match(one({ selfShip: { status: 'unavailable', map: null, error: 'x' } }).unknown[0], /自社出荷の販売を読めない/);
  assert.match(one({ pending: { status: 'error', byCode: null } }).unknown[0], /出荷待ち伝票を数えられない/);
  assert.match(one({ warehouse: [wh('c', 100, '2027-01-31')] }).unknown[0], /期限管理品/);
  assert.match(one({ warehouse: [wh('c', 100), wh('C ', 5)] }).unknown[0], /2 行/);
  const none = one({ warehouse: [] });
  assert.deepEqual([none.warehouse, none.pool, none.unknown], [0, 0, []]);
});

console.log('③ 米国の推奨と取り合い');
await t('🚨 Codex H3 の数値例: セットの構成が c×20 + c×20 (= c×40)・pool 60 → 1 セットだけ (2 行を別々に見ると 3 セットで 120 個使っていた)', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('us-set', 30, 0, [['c', 20], ['C', 20]])],
    warehouse: [wh('c', 60)], selfShip: selfOf([['c', 0]]),
  }));
  assert.deepEqual([r.us[0].need, r.us[0].give, r.us[0].consumption[0].qty, r.us[0].consumption[0].remain_after], [90, 1, 40, 20]);
  assert.equal(r.us[0].status, 'reco');
  assert.match(r.us[0].reason, /c が足りない \(必要 90 → 1\)/);
});
await t('同じ構成品を使う米国 SKU は取り合う: 在庫日数の少ない順 (丸めない値) に 90 日分まで・配った順と残りを返す (cardstand-r-20 / -40)', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('cardstand-r-40', 44, 1, [['cardstand-r', 40]]), usRow('cardstand-r-20', 33, 0, [['cardstand-r', 20]])],
    warehouse: [wh('cardstand-r', 3000)], selfShip: selfOf([['cardstand-r', 0]]),
  }));
  // -20: 在庫 0 日 → need (90*33 - 0)/30 = 99 ちょうど (浮動小数で 100 にしない) → 99×20 = 1980 使う (残 1020)。
  // -40: 在庫 1/(44/30)=0.68 日 → need ceil((3960-30)/30) = 131 → floor(1020/40)=25
  assert.deepEqual(r.us.map((x) => [x.sku, x.order, x.need, x.give]), [['cardstand-r-20', 1, 99, 99], ['cardstand-r-40', 2, 131, 25]]);
  assert.deepEqual([r.codes[0].pool, r.codes[0].pool_after], [3000, 20]);
  assert.equal(r.us[1].status, 'reco');
});
await t('米国: 売れていない → 0 / 在庫日数 45 日以上 → 0 / 30日販売 (RESTOCK) が無い → 判定不能 (PLANNING に落とさない) / 在庫が分からない → 判定不能 / 結びつかない SKU → 判定不能', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('no-sale', 0, 5, [['c', 1]]), usRow('enough', 30, 45, [['c', 1]]), usRow('no-sold', null, 5, [['c', 1]]), usRow('no-stock', 30, null, [['c', 1]]), usRow('unmapped', 30, 0, [], 'none')],
    warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]),
  }));
  const by = Object.fromEntries(r.us.map((x) => [x.sku, x]));
  assert.deepEqual([by['no-sale'].status, by['no-sale'].give], ['zero', 0]);
  assert.deepEqual([by.enough.status, /まだ足りている \(在庫 45 日分/.test(by.enough.reason)], ['zero', true]);
  assert.match(by['no-sold'].reason, /RESTOCK の 30日販売が無い/);
  assert.match(by['no-stock'].reason, /在庫.*取れていない/);
  assert.deepEqual([by.unmapped.status, by.unmapped.reason], ['unknown', '自社の商品コードに結びつかない']);
  assert.equal(r.codes[0].pool_after, 100, '判定不能・0 の SKU が在庫を使っている');
});
await t('必要数は負にならない・pool が 0 でも負の配分をしない (Codex M5)', async () => {
  const r = computeUsAllocation(base({ usRows: [usRow('a', 30, 44, [['c', 1]])], warehouse: [wh('c', 0)], selfShip: selfOf([['c', 0]]) }));
  assert.deepEqual([r.us[0].need, r.us[0].give, r.us[0].status, r.codes[0].pool_after], [46, 0, 'short', 0]);
  assert.equal(US_TARGET_DAYS > US_REORDER_DAYS, true);
});

console.log('③-2 Codex PR2 R1 の再現例');
await t('🚨 High 1: 日本 SKU の構成が食い違う (jp → c×1 / JP → d×1)・販売 30 → c の pool は判定不能 (予約 0 で米国に 90 個を出さない)', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])],
    jpRestock: [jpRow('jp', 30, 0)],
    jpMappings: [map('jp', [['c', 1]]), map('JP', [['d', 1]])],
    warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]),
  }));
  assert.deepEqual([r.codes[0].pool, r.us[0].status, r.us[0].give], [null, 'unknown', null]);
  assert.match(r.codes[0].unknown[0], /日本 SKU jp の構成が食い違う/);
  // 同じ構成の重複は食い違いではない (害が無い)
  const same = computeUsAllocation(base({ usRows: [usRow('us-a', 30, 0, [['c', 1]])], jpRestock: [jpRow('jp', 30, 0)], jpMappings: [map('jp', [['c', 1]]), map('JP ', [['C', 1]])], warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]) }));
  assert.deepEqual([same.codes[0].jp_fba_short, same.codes[0].pool], [60, 40]);
});
await t('🚨 Medium 2: セット品 (is_set) なのに構成が空 → 単品 1 個に補わず、代表 ne_code の構成品を判定不能 / 単品の空構成は 1 個のまま', async () => {
  assert.equal(parseComponents('[]', 'c', true), null);
  assert.deepEqual(parseComponents('[]', 'c', false), [{ code: 'c', qty: 1 }]);
  const r = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])],
    jpRestock: [jpRow('jp-set', 30, 0)],
    jpMappings: [{ amazon_sku: 'jp-set', ne_code: 'c', is_set: 1, set_components: '[]' }],
    warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]),
  }));
  assert.deepEqual([r.codes[0].pool, r.us[0].status], [null, 'unknown']);
  assert.match(r.codes[0].unknown[0], /日本 SKU jp-set の構成が不正/);
});
await t('🚨 High 2: 生の米国レポート → 画面用の組み立て → 配分。RESTOCK に同じ SKU が 2 行 (us 在庫 0 / US 在庫 100) → 判定不能 (1 行目で 90 個を出さない)', async () => {
  const { buildUsInventoryView } = await imp('apps/fba-replenishment-us/us-view.js');
  const raw = (sku, avail) => ({ 'Merchant SKU': sku, Available: String(avail), Working: '0', Shipped: '0', Receiving: '0', 'FC Transfer': '0', 'FC Processing': '0', 'Customer Order': '0', Unfulfillable: '0', 'Units Sold Last 30 Days': '30', FNSKU: 'X', 'Recommended replenishment qty': '0' });
  const payload = { last_attempt: null, file_errors: [], save_failure: null, latest: { business_date: '2026-09-25', reports: { restock: { ok: true, fetched_at: '2026-09-24T22:05:00Z', rows: [raw('us', 0), raw('US', 100)] }, planning: { ok: false, rows: null } } } };
  const view = buildUsInventoryView(payload, { now, resolveSkus: (skus) => new Map(skus.map((s) => [s, { route: 'master', components: [{ ne_code: 'c', qty: 1 }] }])) });
  assert.deepEqual(view.dup_keys.restock, ['us']);
  const r = computeUsAllocation(base({ usRows: view.rows, usRestockFetchedAt: view.restock_fetched_at, usLastAttempt: view.last_attempt, usSaveFailure: view.save_failure, usDupKeys: view.dup_keys, warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]) }));
  assert.deepEqual([r.us[0].status, r.us[0].give, r.codes[0].pool_after], ['unknown', null, 100]);
  assert.match(r.us[0].reason, /同じ SKU が 2 行/);
});
await t('🚨 Medium 1: 米国の最新の取得で RESTOCK が失敗 (前の回は 36h 以内) / 保存に失敗 → 参考 / PLANNING だけの失敗は参考にしない', async () => {
  const codes = (over) => computeUsAllocation(base(over)).gates.map((g) => g.code);
  assert.deepEqual(codes({ usLastAttempt: { business_date: '2026-09-25', attempted_at: '2026-09-25T02:00:00Z', restock_ok: false, planning_ok: false, error: 'US 403' } }), ['us_restock_last_failed']);
  assert.deepEqual(codes({ usLastAttempt: { business_date: '2026-09-25', attempted_at: '2026-09-24T21:00:00Z', restock_ok: false, error: 'old' } }), [], '取れた回より前の失敗で参考にしている');
  assert.deepEqual(codes({ usLastAttempt: { business_date: '2026-09-25', attempted_at: '2026-09-25T02:00:00Z', restock_ok: true, planning_ok: false } }), []);
  assert.deepEqual(codes({ usLastAttempt: { business_date: '2026-09-25', attempted_at: '2026-09-24T22:00:00Z', restock_ok: true, planning_ok: true, save_error: 'EPERM' } }), ['us_save_failed']);
  assert.deepEqual(codes({ usSaveFailure: { at: 'x', error: 'EPERM' } }), ['us_save_failed']);
});

await t('🚨 R2 High: 構成が不正 (セットの空構成) + 30日販売が取れていない + 在庫 0 → 「動きなし」にしない = c を判定不能 (米国に 90 個出さない)', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])],
    jpRestock: [jpRow('jp-set', null, 0)],
    jpMappings: [{ amazon_sku: 'jp-set', ne_code: 'c', is_set: 1, set_components: '[]' }],
    warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]),
  }));
  assert.deepEqual([r.codes[0].pool, r.us[0].status, r.us[0].give], [null, 'unknown', null]);
  // 本当に動きが無い (販売 0・在庫 0 と取れている) なら止めない
  const idle = computeUsAllocation(base({ usRows: [usRow('us-a', 30, 0, [['c', 1]])], jpRestock: [jpRow('jp-set', 0, 0)], jpMappings: [{ amazon_sku: 'jp-set', ne_code: 'c', is_set: 1, set_components: '[]' }], warehouse: [wh('c', 100)], selfShip: selfOf([['c', 0]]) }));
  assert.deepEqual([idle.codes[0].pool, idle.unattributed_jp_count], [100, 0]);
});
await t('🚨 R2 Medium: 恒久除外の日本 SKU は構成が不正・食い違いでも配分を止めない (日本に送らない = 不足 0)。出荷待ち伝票・自社出荷は引いたまま', async () => {
  const r = computeUsAllocation(base({
    usRows: [usRow('us-a', 30, 0, [['c', 1]])],
    jpRestock: [jpRow('jp-set', 30, 0), jpRow('jp-conf', 30, 0)],
    jpExcluded: new Set(['jp-set', 'jp-conf']),
    jpMappings: [{ amazon_sku: 'jp-set', ne_code: 'c', is_set: 1, set_components: '[]' }, map('jp-conf', [['c', 1]]), map('JP-CONF', [['c', 2]])],
    warehouse: [wh('c', 100)], selfShip: selfOf([['c', 30]]),
    pending: { status: 'ok', byCode: new Map([['c', 10]]) },
  }));
  assert.deepEqual([r.codes[0].unknown, r.codes[0].jp_fba_short, r.codes[0].pool, r.us[0].status], [[], 0, 100 - 10 - 60, 'reco']);
});

console.log('④ 関所 (参考扱い)');
await t('新しくそろっていれば参考にしない / 米国 RESTOCK・日本 RESTOCK (時刻なし・欠け)・倉庫 CSV が 36 時間より古い・自社販売が ok でない or 日付なし・出荷待ちが ok でない は参考扱い + 理由', async () => {
  assert.deepEqual(computeUsAllocation(base()).gates, []);
  const codes = (over) => computeUsAllocation(base(over)).gates.map((g) => g.code);
  assert.deepEqual(codes({ usRestockFetchedAt: '2026-09-23T10:00:00Z' }), ['us_restock_stale']);
  assert.deepEqual(codes({ usRestockFetchedAt: null }), ['us_restock_stale']);
  assert.deepEqual(codes({ freshness: { jpRestockSourceAt: '2026-09-24 22:00:00', jpRestockSourceMissing: 3, warehouseUploadedAt: '2026-09-25 09:00:00' } }), ['jp_restock_stale']);
  assert.deepEqual(codes({ freshness: { jpRestockSourceAt: null, jpRestockSourceMissing: 0, warehouseUploadedAt: '2026-09-25 09:00:00' } }), ['jp_restock_stale']);
  assert.deepEqual(codes({ freshness: { jpRestockSourceAt: '2026-09-24 22:00:00', jpRestockSourceMissing: 0, warehouseUploadedAt: '2026-09-22 09:00:00' } }), ['warehouse_stale']);
  assert.deepEqual(codes({ selfShip: { status: 'ok', as_of: null, map: new Map() } }), ['self_sales_not_ok']);
  assert.deepEqual(codes({ selfShip: { status: 'stale', as_of: '2026-09-10', map: new Map() } }), ['self_sales_not_ok']);
  assert.deepEqual(codes({ pending: { status: 'inbound_stale', byCode: new Map() } }), ['pending_slips_not_ok']);
  const r = computeUsAllocation(base({ pending: { status: 'inbound_stale', byCode: new Map() } }));
  assert.equal(r.reference, true);
  assert.ok(r.notes.some((n) => /直近 10 日/.test(n)) && r.notes.some((n) => /準備中/.test(n)), '常に出す注意がない');
});

console.log('⑤ 日本の DB の読み方');
await t('🚨 日本の DB が initDb 前なら計算しない (JP_DB_NOT_READY)・米国側から initDb を呼ばない / calcTargetDays は日本の計算と同じ関数を export', async () => {
  const { loadJpInputs } = await imp('apps/fba-replenishment-us/router.js');
  await assert.rejects(loadJpInputs(), (e) => e.code === 'JP_DB_NOT_READY');
  const jpDb = await imp('apps/fba-replenishment/db.js');
  assert.equal(jpDb.isFbaDbReady(), false);
  const src = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment-us', 'router.js'), 'utf8');
  assert.ok(!/initDb\s*\(/.test(src.replace(/\/\/.*$|\/\*[\s\S]*?\*\/|\*.*$/gm, '')), '米国の router が initDb() を呼んでいる');
  const eng = await imp('apps/fba-replenishment/calculation-engine.js');
  assert.equal(eng.calcTargetDays(150, 100, {}, {}), 40, '高回転・小型 = 40 日 (日本の既定)');
  assert.equal(eng.calcTargetDays(5, 100, {}, {}), 180, '低回転・小型 = 180 日 (日本の既定)');
});
await t('🚨 実際の入口: 日本の DB (一時フォルダ) に SKU 対応・RESTOCK・PLANNING・倉庫 CSV を入れ、loadJpInputs → computeUsAllocation。目標日数は日本の計算と同じ (高回転・小型 40 日)', async () => {
  const jpDb = await imp('apps/fba-replenishment/db.js');
  await jpDb.initDb();
  assert.equal(jpDb.isFbaDbReady(), true);
  jpDb.upsertSkuMappings([
    { amazon_sku: 'B010100720510', ne_code: 'cardstand-r', is_set: true, set_components: [{ ne_code: 'cardstand-r', qty: 40 }], per_unit_volume: 300 },
    { amazon_sku: 'jp-set-empty', ne_code: 'cardstand-w', is_set: true, set_components: [] },
  ]);
  const src = '2026-09-24 22:00:00';
  jpDb.saveRestockLatest([
    { amazon_sku: 'B010100720510', fba_available: 99, fba_inbound_working: 500, units_sold_30d: 106, source_fetched_at: src },
    { amazon_sku: 'jp-set-empty', fba_available: 1, units_sold_30d: 3, source_fetched_at: src },
  ]);
  jpDb.replaceWarehouseInventory([{ logizard_code: 'Cardstand-R', location: 'P-01', quantity: 9000, available_qty: 9000 }, { logizard_code: 'cardstand-w', location: 'P-02', quantity: 100, available_qty: 100 }]);
  const { loadJpInputs } = await imp('apps/fba-replenishment-us/router.js');
  const inputs = await loadJpInputs();
  assert.equal(inputs.jpTargetDaysOf(inputs.jpRestock.find((x) => x.amazon_sku === 'B010100720510')), 40);
  const r = computeUsAllocation({ ...base(), ...inputs, now,
    usRows: [usRow('cardstand-r-40', 44, 0, [['cardstand-r', 40]]), usRow('cardstand-w-10', 2, 0, [['cardstand-w', 10]])] });
  const cr = r.codes.find((c) => c.code === 'cardstand-r');
  // 準備中 500 は足さない: ceil((40*106 - 30*99)/30) = ceil(42.33) = 43 → ×40 = 1720
  assert.deepEqual([cr.warehouse, cr.jp_fba_short], [9000, 1720]);
  assert.match(r.codes.find((c) => c.code === 'cardstand-w').unknown.join(), /jp-set-empty の構成が不正/);
  assert.ok(r.gates.some((g) => g.code === 'self_sales_not_ok'), '商品管理リストを読めないのに参考になっていない');
});

console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
