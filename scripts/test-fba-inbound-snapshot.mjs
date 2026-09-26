/**
 * test-fba-inbound-snapshot.mjs — 納品プラン・出荷便の状態のスナップショット (FBA 補充 B1) の受入試験
 *
 * 偽の SP-API (v2024 の一覧・プラン・便・品目 / v0 の出荷便・明細) を相手に、
 *   取り方 (全ページ・追跡中・新しいプラン・空プランの取り直し・時間の上限・知らない状態)・
 *   SKU ごとの内訳・レポートとの照合・2 回の差・毎朝の組み込み (失敗しても止めない) を見る。
 * 使い方: node scripts/test-fba-inbound-snapshot.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  takeInboundSnapshot, summarizeSnapshot, checkAgainstReport, diffSnapshots, nextTracked, nextPlanCache, verifyAfterMs,
} from '../apps/fba-replenishment/inbound-snapshot.js';

let passed = 0;
async function t(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack}`); process.exitCode = 1; } }

const NOW = Date.parse('2026-09-26T22:30:00Z');   // 9/27 07:30 JST
const day = (n) => new Date(NOW - n * 86400e3).toISOString();

/** 偽の Amazon。world を書き換えると次の呼び出しから反映される */
function fakeAmazon(world) {
  const calls = [];
  const call = async (p) => {
    calls.push(p);
    const u = new URL(`https://x${p}`);
    if (world.fail && world.fail(p)) throw new Error('500 boom');
    if (u.pathname === '/inbound/fba/2024-03-20/inboundPlans') {
      const st = u.searchParams.get('status');
      const all = world.plans.filter((x) => x.status === st).sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
      const page = Number(u.searchParams.get('paginationToken') || 0);
      const size = world.pageSize || 30;
      const slice = all.slice(page * size, page * size + size);
      return { inboundPlans: slice.map((x) => ({ inboundPlanId: x.id, name: x.name, status: x.status, createdAt: x.createdAt, lastUpdatedAt: x.lastUpdatedAt })),
        pagination: (page + 1) * size < all.length ? { nextToken: String(page + 1) } : {} };
    }
    let m = u.pathname.match(/^\/inbound\/fba\/2024-03-20\/inboundPlans\/([^/]+)$/);
    if (m) { const x = world.plans.find((y) => y.id === m[1]); return { ...x, shipments: (x.shipments || []).map((s) => ({ shipmentId: s.id, status: s.status })) }; }
    m = u.pathname.match(/^\/inbound\/fba\/2024-03-20\/inboundPlans\/([^/]+)\/items$/);
    if (m) { const x = world.plans.find((y) => y.id === m[1]); return { items: Object.entries(x.items || {}).map(([msku, quantity]) => ({ msku, quantity })) }; }
    m = u.pathname.match(/^\/inbound\/fba\/2024-03-20\/inboundPlans\/([^/]+)\/shipments\/([^/]+)$/);
    if (m) { const s = world.plans.find((y) => y.id === m[1]).shipments.find((z) => z.id === m[2]); return { shipmentId: s.id, status: s.status, shipmentConfirmationId: s.conf }; }
    m = u.pathname.match(/^\/inbound\/fba\/2024-03-20\/inboundPlans\/([^/]+)\/shipments\/([^/]+)\/items$/);
    if (m) { const s = world.plans.find((y) => y.id === m[1]).shipments.find((z) => z.id === m[2]); return { items: Object.entries(s.items).map(([msku, quantity]) => ({ msku, quantity })) }; }
    if (u.pathname === '/fba/inbound/v0/shipments') {
      const want = (u.searchParams.get('ShipmentStatusList') || '').split(',');
      return { ShipmentData: world.v0.filter((s) => want.includes(s.status)).map((s) => ({ ShipmentId: s.id, ShipmentName: s.id, ShipmentStatus: s.status })) };
    }
    m = u.pathname.match(/^\/fba\/inbound\/v0\/shipments\/([^/]+)\/items$/);
    if (m) {
      const s = world.v0.find((z) => z.id === m[1]);
      const rows = Object.entries(s.items).map(([k, v]) => ({ SellerSKU: k, QuantityShipped: v[0], QuantityReceived: v[1] }));
      if (s.page2) return { ItemData: rows, NextToken: `tok-${s.id}` };
      return { ItemData: rows, NextToken: 'always-there' };   // v0 は続きが無くても NextToken を返す癖
    }
    if (u.pathname === '/fba/inbound/v0/shipmentItems') {
      const tok = u.searchParams.get('NextToken');
      const s = world.v0.find((z) => `tok-${z.id}` === tok);
      if (!s) return { ItemData: [] };
      return { ItemData: Object.entries(s.page2).map(([k, v]) => ({ ShipmentId: s.id, SellerSKU: k, QuantityShipped: v[0], QuantityReceived: v[1] })) };
    }
    throw new Error(`知らない呼び出し ${p}`);
  };
  return { call, calls };
}

/** 9/26 の実データに似せた世界 */
const baseWorld = () => ({
  plans: [
    { id: 'wfA', name: '9/28 納品プラン①', status: 'ACTIVE', createdAt: day(2), lastUpdatedAt: day(2), items: { 'SKU-1': 30, 'sku-2': 20 }, shipments: [] },
    { id: 'wfB', name: '9/25納品①', status: 'ACTIVE', createdAt: day(3), lastUpdatedAt: day(1), items: { 'sku-3': 50 },
      shipments: [{ id: 'shB1', status: 'READY_TO_SHIP', conf: 'FBA15B1', items: { 'sku-3': 50 } }] },
    { id: 'wfC', name: '9/24 納品①', status: 'SHIPPED', createdAt: day(4), lastUpdatedAt: day(1), items: { 'sku-4': 100, 'sku-5': 10 },
      shipments: [{ id: 'shC1', status: 'RECEIVING', conf: 'FBA15C1', items: { 'sku-4': 100 } }, { id: 'shC2', status: 'SHIPPED', conf: 'FBA15C2', items: { 'sku-5': 10 } }] },
    { id: 'wfV', name: '', status: 'VOIDED', createdAt: day(2), lastUpdatedAt: day(2), items: {}, shipments: [] },
    { id: 'wfOld', name: '', status: 'ACTIVE', createdAt: day(900), lastUpdatedAt: day(900), items: {}, shipments: [] },
    { id: 'wfOldShipped', name: 'old', status: 'SHIPPED', createdAt: day(60), lastUpdatedAt: day(50), items: { 'sku-9': 5 }, shipments: [] },
  ],
  v0: [
    { id: 'FBA15B1', status: 'READY_TO_SHIP', items: { 'sku-3': [50, 0] } },
    { id: 'FBA15C1', status: 'RECEIVING', items: { 'sku-4': [100, 60] } },
    { id: 'FBA15C2', status: 'SHIPPED', items: { 'sku-5': [10, 0] } },
    { id: 'FBA15OLD', status: 'RECEIVING', items: { 'sku-8': [7, 2] } },
  ],
});
const opts = (world, extra = {}) => ({ call: fakeAmazon(world).call, nowMs: NOW, sleep: async () => {}, paceMs: 0, ...extra });

console.log('取り方');
await t('ACTIVE は年齢で切らずに全部、SHIPPED/VOIDED は 14 日以内に作ったもの。便は getShipment で FBA15 ID と状態、品目は便ごと', async () => {
  const s = await takeInboundSnapshot(opts(baseWorld()));
  assert.equal(s.complete, true, s.errors.join());
  assert.deepEqual(s.plans.map((p) => p.id).sort(), ['wfA', 'wfB', 'wfC', 'wfOld', 'wfV'], '60 日前の SHIPPED は (追跡していなければ) 探さない');
  const c = s.plans.find((p) => p.id === 'wfC');
  assert.deepEqual(c.shipments.map((x) => [x.confirmationId, x.status, x.items]), [['FBA15C1', 'RECEIVING', { 'sku-4': 100 }], ['FBA15C2', 'SHIPPED', { 'sku-5': 10 }]]);
  assert.deepEqual(s.plans.find((p) => p.id === 'wfA').items, { 'sku-1': 30, 'sku-2': 20 }, 'SKU は小文字にそろえる');
  assert.equal(s.v0.length, 4);
});

await t('SKU ごとの内訳: 配置未確定 / 出荷前の便 / v0 の送った数−受領した数 (出荷前の v0 便は輸送中に入れない)', async () => {
  const s = await takeInboundSnapshot(opts(baseWorld()));
  const { bySku, mixedPlans, unlinkedV0 } = summarizeSnapshot(s);
  assert.deepEqual(bySku['sku-1'], { unconfirmed: 30, unshipped: 0, v0Open: 0 });
  assert.deepEqual(bySku['sku-3'], { unconfirmed: 0, unshipped: 50, v0Open: 0 }, '配置確定済み = レポートの準備中側。輸送中には数えない');
  assert.deepEqual(bySku['sku-4'], { unconfirmed: 0, unshipped: 0, v0Open: 40 });
  assert.deepEqual(bySku['sku-5'].v0Open, 10);
  assert.deepEqual(mixedPlans, []);
  assert.deepEqual(unlinkedV0, ['FBA15OLD'], 'v2024 のどの便にも結べない v0 便は黙って捨てずに出す');
});

await t('一部だけ出荷したプランを見分ける (ACTIVE なのに出荷済みの便 / SHIPPED なのに出荷前の便)', async () => {
  const w = baseWorld();
  w.plans.find((p) => p.id === 'wfB').shipments.push({ id: 'shB2', status: 'SHIPPED', conf: 'FBA15B2', items: { 'sku-3': 1 } });
  w.plans.find((p) => p.id === 'wfC').shipments.push({ id: 'shC3', status: 'READY_TO_SHIP', conf: 'FBA15C3', items: { 'sku-6': 4 } });
  const { mixedPlans } = summarizeSnapshot(await takeInboundSnapshot(opts(w)));
  assert.deepEqual(mixedPlans.sort(), ['wfB', 'wfC']);
});

await t('レポートとの照合: 準備中 = 出荷前の便、輸送中+受領中 = v0。合わない SKU を出す', async () => {
  const s = await takeInboundSnapshot(opts(baseWorld()));
  const rows = [
    { amazon_sku: 'SKU-3', fba_inbound_working: 50, fba_inbound_shipped: 0, fba_inbound_received: 0 },
    { amazon_sku: 'sku-4', fba_inbound_working: 0, fba_inbound_shipped: 0, fba_inbound_received: 40 },
    { amazon_sku: 'sku-5', fba_inbound_working: 0, fba_inbound_shipped: 8, fba_inbound_received: 0 },   // 2 個足りない
    { amazon_sku: 'sku-8', fba_inbound_working: 0, fba_inbound_shipped: 5, fba_inbound_received: 0 },
    { amazon_sku: 'sku-7', fba_inbound_working: 12, fba_inbound_shipped: null, fba_inbound_received: null },   // こちらに無い準備中
  ];
  const c = checkAgainstReport(summarizeSnapshot(s), rows);
  assert.deepEqual(c.workingMismatch, [{ sku: 'sku-7', api: 0, report: 12 }]);
  assert.deepEqual(c.openMismatch, [{ sku: 'sku-5', api: 10, report: 8 }]);
});

await t('🚨 知らない状態・ページ上限・取得失敗 = complete=false (取れた分を全部に見せない)', async () => {
  const w1 = baseWorld(); w1.plans.find((p) => p.id === 'wfB').shipments[0].status = 'UNCONFIRMED';
  const s1 = await takeInboundSnapshot(opts(w1));
  assert.equal(s1.complete, false); assert.match(s1.unknownStates.join(), /UNCONFIRMED/);
  const w2 = baseWorld(); w2.pageSize = 1;
  const s2 = await takeInboundSnapshot(opts(w2, { pageCap: 2 }));
  assert.equal(s2.complete, false); assert.match(s2.errors.join(), /ACTIVE の一覧がページ上限/);
  const w3 = baseWorld(); w3.fail = (p) => p.endsWith('/wfA/items');
  const s3 = await takeInboundSnapshot(opts(w3));
  assert.equal(s3.complete, false); assert.deepEqual(s3.failedIds, ['wfA']);
});

await t('空の古い ACTIVE は前回と同じなら取り直さない (日をずらして確かめ直す)。品目のあるプランは毎回取る', async () => {
  const w = baseWorld();
  const s1 = await takeInboundSnapshot(opts(w));
  const cache = { plans: nextPlanCache(s1, {}), tracked: nextTracked(s1, []) };
  const amz = fakeAmazon(w);
  const s2 = await takeInboundSnapshot({ ...opts(w), call: amz.call, cache, nowMs: NOW + 3600e3 });
  assert.equal(s2.plans.find((p) => p.id === 'wfOld').reused, true);
  assert.ok(!amz.calls.some((c) => c.includes('/wfOld')), '空プランの中身を取りにいかない');
  assert.ok(amz.calls.some((c) => c.includes('/wfA/items')), '品目のあるプランは取る');
  const later = NOW + verifyAfterMs('wfOld', 7) + 60e3;
  const amz2 = fakeAmazon(w);
  await takeInboundSnapshot({ ...opts(w), call: amz2.call, cache, nowMs: later });
  assert.ok(amz2.calls.some((c) => c.includes('/wfOld')), '間隔が来たら確かめ直す');
  const spread = new Set(Array.from({ length: 200 }, (_, i) => verifyAfterMs(`wf${i}`, 7)));
  assert.ok(spread.size >= 6, '確かめ直す日がプランごとにばらける');
});

await t('時間の上限: 大事なプラン (品目あり・新しい・追跡中) は必ず取り、空の古いプランだけ翌日へ回す (complete=false)', async () => {
  const w = baseWorld();
  for (let i = 0; i < 5; i++) w.plans.push({ id: `wfE${i}`, name: '', status: 'ACTIVE', createdAt: day(400 + i), lastUpdatedAt: day(400 + i), items: {}, shipments: [] });
  let now = 0;
  const s = await takeInboundSnapshot({ ...opts(w), budgetMs: 10, clock: () => (now += 5) });
  assert.equal(s.complete, false);
  assert.ok(s.deferred >= 1, `${s.deferred}`);
  assert.ok(s.deferredIds.every((id) => id === 'wfOld' || id.startsWith('wfE')), '後回しにするのは空の古いプランだけ');
  for (const id of ['wfA', 'wfB', 'wfC', 'wfV']) assert.ok(s.plans.some((p) => p.id === id), id);
});

await t('追跡中のプランは一覧から消えても 1 件ずつ取る。便 0 件の SHIPPED は解決にしない。全便が出荷済みになったら追跡をやめる (あとは v0 で追う)', async () => {
  const w = baseWorld();
  const s = await takeInboundSnapshot({ ...opts(w), cache: { plans: {}, tracked: ['wfOldShipped'] } });
  assert.ok(s.plans.some((p) => p.id === 'wfOldShipped'), '60 日前の SHIPPED でも追跡中なら取る');
  const tr = nextTracked(s, ['wfOldShipped', 'wfGone']);
  assert.ok(tr.includes('wfOldShipped'), '便 0 件の SHIPPED は解決していない');
  assert.ok(tr.includes('wfGone'), '取れなかった (今回見ていない) プランは追跡を外さない');
  assert.ok(tr.includes('wfA') && tr.includes('wfB'), '出荷前の便・配置未確定は追う');
  assert.ok(!tr.includes('wfC'), '全便が出荷済み (受領中・輸送中) = v0 で追うので v2024 の追跡はやめる');
  w.plans.find((p) => p.id === 'wfB').shipments.forEach((x) => { x.status = 'SHIPPED'; });
  w.plans.find((p) => p.id === 'wfB').status = 'SHIPPED';
  const s2 = await takeInboundSnapshot({ ...opts(w), cache: { plans: {}, tracked: tr } });
  assert.ok(!nextTracked(s2, tr).includes('wfB'), '出荷したら追跡をやめる');
});

await t('取り消し済みのプランは中身を取らない。全便が終わったプランは前回と同じなら取り直さない (取り直していないプランは記録も進めない)', async () => {
  const w = baseWorld();
  w.plans.push({ id: 'wfDone', name: '9/20', status: 'SHIPPED', createdAt: day(6), lastUpdatedAt: day(3), items: { 'sku-d': 5 },
    shipments: [{ id: 'shD', status: 'CLOSED', conf: 'FBA15D', items: { 'sku-d': 5 } }] });
  const amz = fakeAmazon(w);
  const s1 = await takeInboundSnapshot({ ...opts(w), call: amz.call });
  assert.ok(!amz.calls.some((c) => c.includes('/wfV')), '取り消し済みは取りにいかない');
  assert.equal(s1.plans.find((p) => p.id === 'wfV').voided, true);
  const cache = { plans: nextPlanCache(s1, {}), tracked: nextTracked(s1, []) };
  assert.equal(cache.plans.wfDone.resolved, true);
  assert.equal(cache.plans.wfC.resolved, true, '全便が出荷済み = v2024 側は終わり (受領の進みは v0 で見る)');
  assert.equal(cache.plans.wfB.resolved, false, '出荷前の便がある = まだ');
  const amz2 = fakeAmazon(w);
  const s2 = await takeInboundSnapshot({ ...opts(w), call: amz2.call, cache, nowMs: NOW + 3600e3 });
  assert.ok(!amz2.calls.some((c) => c.includes('/wfDone')), '終わったプランは取り直さない');
  assert.ok(amz2.calls.some((c) => c.includes('/wfB/shipments')), 'まだ出荷前の便があるプランは毎回');
  w.plans.find((p) => p.id === 'wfC').lastUpdatedAt = day(0);
  const amz3 = fakeAmazon(w);
  await takeInboundSnapshot({ ...opts(w), call: amz3.call, cache, nowMs: NOW + 3600e3 });
  assert.ok(amz3.calls.some((c) => c.includes('/wfC/shipments')), '更新されたら取り直す');
  assert.equal(nextPlanCache(s2, cache.plans).wfDone.verifiedAt, cache.plans.wfDone.verifiedAt, '取り直していないプランの確かめた時刻は進めない');
  assert.equal(nextPlanCache(s2, cache.plans).wfDone.resolved, true, '取り直していないプランを「空」に書き換えない');
});

await t('結べない v0 便を分ける: 探す範囲より前に作った出荷済みの便 = v0 だけで追う (想定どおり) / 範囲の中・出荷前・名前が読めない = 結べない (異常)', async () => {
  const w = baseWorld();
  w.v0.find((s) => s.id === 'FBA15OLD').name = 'FBA STA (2026/06/10 10:00)-HND2';
  w.v0.push({ id: 'FBA15NEW', name: 'FBA STA (2026/09/25 10:00)-HND2', status: 'SHIPPED', items: { 'sku-n': [3, 0] } });
  const amz = fakeAmazon(w);
  const named = async (p) => {
    const r = await amz.call(p);
    if (p.startsWith('/fba/inbound/v0/shipments?')) r.ShipmentData = r.ShipmentData.map((x) => ({ ...x, ShipmentName: w.v0.find((v) => v.id === x.ShipmentId).name || x.ShipmentName }));
    return r;
  };
  const s = await takeInboundSnapshot({ ...opts(w), call: named });
  assert.ok(!s.plans.some((p) => p.id === 'wfOldShipped'), '60 日前の SHIPPED はさかのぼって探さない');
  const sum = summarizeSnapshot(s);
  assert.deepEqual(sum.v0OnlyTracked, ['FBA15OLD']);
  assert.deepEqual(sum.unlinkedV0, ['FBA15NEW']);
  assert.equal(sum.bySku['sku-8'].v0Open, 5, 'v0 だけで追う便も輸送中に数える');
});

await t('🚨 v0 明細の続き (別の口) まで取る。続きの無い NextToken では止まる (Codex PR #1463 R1 High 2)', async () => {
  const w = baseWorld();
  w.v0.find((x) => x.id === 'FBA15C1').page2 = { 'sku-4b': [30, 5] };
  const s = await takeInboundSnapshot(opts(w));
  assert.equal(s.complete, true, s.errors.join());
  assert.deepEqual(s.v0.find((x) => x.id === 'FBA15C1').items, { 'sku-4': { shipped: 100, received: 60 }, 'sku-4b': { shipped: 30, received: 5 } });
  assert.deepEqual(s.v0.find((x) => x.id === 'FBA15C2').items, { 'sku-5': { shipped: 10, received: 0 } });
});

await t('🚨 取得全体の締め切り: 過ぎたら残りは取らず complete=false で返す・1 回の呼び出しの待ちにも上限 (Codex PR #1463 R1 High 1)', async () => {
  const w = baseWorld();
  let now = 0;
  const s = await takeInboundSnapshot({ ...opts(w), deadlineMs: 20, clock: () => (now += 3) });
  assert.equal(s.complete, false);
  assert.match(s.errors.join(), /締め切り/);
  assert.ok(s.calls < 10, `${s.calls}`);
  const hang = { call: () => new Promise(() => {}), nowMs: NOW, sleep: async () => {}, paceMs: 0, callTimeoutMs: 30 };
  const t0 = Date.now();
  const s2 = await takeInboundSnapshot({ ...hang, deadlineMs: 400 });
  assert.equal(s2.complete, false);
  assert.match(s2.errors.join(), /応答が/);
  assert.ok(Date.now() - t0 < 3000, '返らない呼び出しを待ち続けない');
});

await t('🚨 取り直さなかった空プランに品目が足された日も差分に出す (前回の中身 = 空 を持ち越して比べる。Codex PR #1463 R1 Medium 3)', async () => {
  const w = baseWorld();
  const s0 = await takeInboundSnapshot(opts(w));
  const cache = { plans: nextPlanCache(s0, {}), tracked: nextTracked(s0, []) };
  const a = await takeInboundSnapshot({ ...opts(w), cache, nowMs: NOW + 3600e3 });
  assert.equal(a.plans.find((p) => p.id === 'wfOld').reused, true);
  w.plans.find((p) => p.id === 'wfOld').items = { 'sku-q': 9 };
  w.plans.find((p) => p.id === 'wfOld').lastUpdatedAt = day(0);
  const b = await takeInboundSnapshot({ ...opts(w), cache: { plans: nextPlanCache(a, cache.plans), tracked: nextTracked(a, cache.tracked) }, nowMs: NOW + 7200e3 });
  const d = diffSnapshots(a, b);
  assert.deepEqual(d.changes.filter((c) => c.id === 'wfOld').map((c) => c.why), ['plan_items']);
  assert.ok(d.skus.includes('sku-q'));
  // 中身の分からないプラン (古い記録) が更新されていたら unknown
  const a2 = { ...a, plans: a.plans.map((p) => (p.id === 'wfOld' ? { ...p, contentKnown: false } : p)) };
  assert.ok(diffSnapshots(a2, b).unknown.includes('wfOld'));
});

await t('🚨 出荷し終わったプランを取り直さない日も、便の対応 (FBA15 ID) と品目を持ち越す = 結べた v0 便を「結べない」にしない (Codex PR #1463 R1 Medium 4)', async () => {
  const w = baseWorld();
  const s0 = await takeInboundSnapshot(opts(w));
  const cache = { plans: nextPlanCache(s0, {}), tracked: nextTracked(s0, []) };
  const s1 = await takeInboundSnapshot({ ...opts(w), cache, nowMs: NOW + 3600e3 });
  const c = s1.plans.find((p) => p.id === 'wfC');
  assert.equal(c.reused, true);
  assert.deepEqual(c.shipments.map((x) => x.confirmationId), ['FBA15C1', 'FBA15C2']);
  assert.deepEqual(summarizeSnapshot(s1).unlinkedV0, summarizeSnapshot(s0).unlinkedV0);
  assert.deepEqual(diffSnapshots(s0, s1).changes, [], '何も変わっていない日は差分なし');
});

await t('取り消し済みのプランは、取り消し前に記録した SKU を持ち越す (取り消しの差分に SKU が出る)', async () => {
  const w = baseWorld();
  const s0 = await takeInboundSnapshot(opts(w));
  const cache = { plans: nextPlanCache(s0, {}), tracked: nextTracked(s0, []) };
  w.plans.find((p) => p.id === 'wfA').status = 'VOIDED';
  const s1 = await takeInboundSnapshot({ ...opts(w), cache, nowMs: NOW + 3600e3 });
  const d = diffSnapshots(s0, s1);
  assert.deepEqual(d.changes.filter((x) => x.id === 'wfA').map((x) => x.why), ['plan_status ACTIVE→VOIDED']);
  assert.ok(d.skus.includes('sku-1') && d.skus.includes('sku-2'));
  const s2 = await takeInboundSnapshot({ ...opts(w), cache: { plans: nextPlanCache(s1, cache.plans), tracked: nextTracked(s1, cache.tracked) }, nowMs: NOW + 7200e3 });
  assert.deepEqual(s2.plans.find((p) => p.id === 'wfA').items, { 'sku-1': 30, 'sku-2': 20 }, '次の回も取り消し前の品目が残る');
});

await t('🚨 取り消したプランの便は今の「準備中」に数えない (取り消し前の中身は差分のためだけ。Codex PR #1463 R2 Medium 2)', async () => {
  const w = baseWorld();
  const s0 = await takeInboundSnapshot(opts(w));
  const cache = { plans: nextPlanCache(s0, {}), tracked: nextTracked(s0, []) };
  w.plans.find((p) => p.id === 'wfB').status = 'VOIDED';
  const s1 = await takeInboundSnapshot({ ...opts(w), cache, nowMs: NOW + 3600e3 });
  assert.equal(s1.complete, true);
  assert.equal(s1.plans.find((p) => p.id === 'wfB').shipments.length, 1, '中身は持っている');
  assert.equal(summarizeSnapshot(s1).bySku['sku-3']?.unshipped ?? 0, 0, '今の数には入れない');
  assert.ok(diffSnapshots(s0, s1).skus.includes('sku-3'), '取り消しの差分には SKU が出る');
});

await t('🚨 中身の分からないプランの状態が変わったら unknown (どの SKU に効いたか確定できない。Codex PR #1463 R2 Medium 3)', async () => {
  const base = { plans: [], v0: [], deferredIds: [], failedIds: [] };
  const x = { id: 'wfZ', status: 'SHIPPED', lastUpdatedAt: 'a', items: {}, shipments: [], reused: true, contentKnown: false };
  const y = { id: 'wfZ', status: 'VOIDED', lastUpdatedAt: 'b', items: {}, shipments: [], contentKnown: false, voided: true };
  const d = diffSnapshots({ ...base, plans: [x] }, { ...base, plans: [y] });
  assert.deepEqual(d.unknown, ['wfZ']);
  assert.deepEqual(d.changes, []);
  const known = { ...x, reused: false, contentKnown: true, items: { 'sku-z': 3 } };
  const d2 = diffSnapshots({ ...base, plans: [known] }, { ...base, plans: [y] });
  assert.deepEqual([d2.unknown, d2.skus], [[], ['sku-z']], '取り消し前の中身が分かっていれば SKU を出せる');
});

await t('締め切りの確認は待ったあとにも (待っている間に過ぎたら呼ばない。Codex PR #1463 R2 Low)・呼ぶ側に締め切りの時刻を渡す', async () => {
  let now = 0;
  const seen = [];
  const s = await takeInboundSnapshot({
    call: async (p, label, o) => { seen.push(o); return {}; }, nowMs: NOW, paceMs: 10,
    sleep: async (ms) => { now += ms; }, clock: () => now, deadlineMs: 5,
  });
  assert.equal(s.calls, 0);
  assert.equal(s.complete, false);
  const s2 = await takeInboundSnapshot({ ...opts(baseWorld()), call: async (p, label, o) => { seen.push(o); return fakeAmazon(baseWorld()).call(p); } });
  assert.ok(seen.length && seen.every((o) => Number.isFinite(o?.deadlineAt)), '締め切りの時刻を渡す');
  assert.equal(s2.complete, true);
});

console.log('2 回の差');
await t('🚨 相殺でも止める: 出荷 100 と 別の確定 100 が同じ SKU で起きても、両方のプランの SKU が変化に出る', async () => {
  const w = baseWorld();
  w.plans.push({ id: 'wfX', name: 'X', status: 'ACTIVE', createdAt: day(1), lastUpdatedAt: day(1), items: { 'sku-z': 100 }, shipments: [{ id: 'shX', status: 'READY_TO_SHIP', conf: 'FBA15X', items: { 'sku-z': 100 } }] });
  const a = await takeInboundSnapshot(opts(w));
  w.plans.find((p) => p.id === 'wfX').status = 'SHIPPED';
  w.plans.find((p) => p.id === 'wfX').shipments[0].status = 'SHIPPED';
  w.plans.push({ id: 'wfY', name: 'Y', status: 'ACTIVE', createdAt: day(0), lastUpdatedAt: day(0), items: { 'sku-z': 100 }, shipments: [{ id: 'shY', status: 'READY_TO_SHIP', conf: 'FBA15Y', items: { 'sku-z': 100 } }] });
  const b = await takeInboundSnapshot(opts(w));
  assert.equal(summarizeSnapshot(a).bySku['sku-z'].unshipped, summarizeSnapshot(b).bySku['sku-z'].unshipped, '数だけ見ると同じ (相殺)');
  const d = diffSnapshots(a, b);
  assert.ok(d.skus.includes('sku-z'));
  assert.deepEqual(d.changes.filter((c) => c.kind === 'plan').map((c) => [c.id, c.why]).sort(), [['wfX', 'plan_status ACTIVE→SHIPPED'], ['wfY', 'new_plan']]);
});

await t('受領だけが進んだ v0 便は receivedOnly に分ける。受領が減った・送った数が変わった・状態が変わった は変化', async () => {
  const w = baseWorld();
  const a = await takeInboundSnapshot(opts(w));
  w.v0.find((s) => s.id === 'FBA15C1').items['sku-4'] = [100, 80];
  const b = await takeInboundSnapshot(opts(w));
  const d = diffSnapshots(a, b);
  assert.deepEqual(d.receivedOnly.map((x) => x.id), ['FBA15C1']);
  assert.ok(!d.skus.includes('sku-4'));
  w.v0.find((s) => s.id === 'FBA15C1').items['sku-4'] = [100, 70];
  const c = await takeInboundSnapshot(opts(w));
  assert.deepEqual(diffSnapshots(b, c).changes.map((x) => x.why), ['v0_received_decreased']);
  w.v0.find((s) => s.id === 'FBA15B1').status = 'SHIPPED';
  const e = await takeInboundSnapshot(opts(w));
  assert.ok(diffSnapshots(c, e).changes.some((x) => x.why === 'v0_status READY_TO_SHIP→SHIPPED'));
});

await t('品目の数の変更・便の構成の変化・プランが消えた を拾う。lastUpdatedAt だけの変化は記録するが SKU は止めない', async () => {
  const w = baseWorld();
  const a = await takeInboundSnapshot(opts(w));
  w.plans.find((p) => p.id === 'wfA').items['sku-2'] = 25;
  w.plans.find((p) => p.id === 'wfB').lastUpdatedAt = day(0);
  w.plans = w.plans.filter((p) => p.id !== 'wfV');
  const b = await takeInboundSnapshot(opts(w));
  const d = diffSnapshots(a, b);
  const why = Object.fromEntries(d.changes.map((c) => [c.id, c.why]));
  assert.equal(why.wfA, 'plan_items');
  assert.equal(why.wfB, 'plan_updated_only');
  assert.equal(why.wfV, 'plan_vanished');
  assert.ok(d.skus.includes('sku-1') && d.skus.includes('sku-2'), '変わったプランの SKU は全部');
  assert.ok(!d.skus.includes('sku-3'), 'lastUpdatedAt だけなら止めない');
});

await t('取れなかった・確かめなかったプランは「消えた」ではなく unknown', async () => {
  const w = baseWorld();
  const a = await takeInboundSnapshot(opts(w));
  w.fail = (p) => p.endsWith('/wfA/items');
  const b = await takeInboundSnapshot(opts(w));
  const d = diffSnapshots(a, b);
  assert.deepEqual(d.unknown, ['wfA']);
  assert.ok(!d.changes.some((c) => c.id === 'wfA'));
});

console.log('毎朝の組み込み');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-inbound-'));
process.env.DATA_DIR = tmp;
const { captureInboundPhase, openInboundStateDb } = await import('../apps/warehouse/inbound-baseline.js');
const { runFbaReportSnapshot } = await import('../apps/warehouse/fba-report-snapshot.js');

await t('S0 → S1 で基準を保存 (レポート作成中に動いた・照合・Render が使う世代)。追跡表と空プランの記録も残す', async () => {
  const w = baseWorld();
  const s0 = await captureInboundPhase('S0', { businessDate: '2026-09-27', call: fakeAmazon(w).call, snapshotOpts: { sleep: async () => {}, paceMs: 0, nowMs: NOW } });
  assert.match(s0.note, /^S0 ok/);
  w.v0.find((s) => s.id === 'FBA15C2').status = 'RECEIVING';   // レポート作成中に荷受けが始まった
  const s1 = await captureInboundPhase('S1', {
    businessDate: '2026-09-27', call: fakeAmazon(w).call, snapshotOpts: { sleep: async () => {}, paceMs: 0, nowMs: NOW + 300e3 },
    fetchedAt: '2026-09-26T22:43:00Z', freshness: { restock_source_at: '2026-09-26 22:43:54', planning_source_at: '2026-09-26 22:43:58' },
    saved: { restock: true, planning: true },
    restockRows: [{ amazon_sku: 'sku-3', fba_inbound_working: 50 }, { amazon_sku: 'sku-4', fba_inbound_received: 40 }],
  });
  assert.equal(s1.usable, true);
  assert.match(s1.note, /レポート作成中に動いた 1・準備中の不一致 0・輸送中の不一致 2/);
  const d = openInboundStateDb();
  const b = d.prepare('SELECT * FROM inbound_baselines').get();
  assert.equal(b.restock_source_at, '2026-09-26 22:43:54');
  const r = JSON.parse(b.result);
  assert.deepEqual(r.changed_during_report.skus, ['sku-5']);
  assert.deepEqual(r.open_mismatch.top.map((x) => x.sku).sort(), ['sku-5', 'sku-8']);
  const tracked = JSON.parse(d.prepare("SELECT value FROM inbound_state_kv WHERE key = 'tracked_plans'").get().value);
  assert.deepEqual(tracked, ['wfA', 'wfB']);
});

await t('🚨 今回のレポートを保存できなかった・表の世代が S0 より前 なら基準は使えない (Codex PR #1463 R1 Medium 5)', async () => {
  const w = baseWorld();
  const run = async (saved, freshness) => {
    await captureInboundPhase('S0', { businessDate: '2026-09-28', call: fakeAmazon(w).call, snapshotOpts: { sleep: async () => {}, paceMs: 0, nowMs: NOW } });
    return captureInboundPhase('S1', { businessDate: '2026-09-28', call: fakeAmazon(w).call, snapshotOpts: { sleep: async () => {}, paceMs: 0, nowMs: NOW + 300e3 },
      fetchedAt: '2026-09-26T22:43:00Z', restockRows: [{ amazon_sku: 'sku-3', fba_inbound_working: 50 }], saved, freshness });
  };
  const fresh = { restock_source_at: '2026-09-26 22:43:54', planning_source_at: '2026-09-26 22:43:58' };
  assert.equal((await run({ restock: true, planning: true }, fresh)).usable, true);
  assert.equal((await run({ restock: false, planning: true }, fresh)).usable, false, 'RESTOCK の保存に失敗');
  assert.equal((await run({ restock: true, planning: true }, { ...fresh, restock_source_at: '2026-09-25 22:43:54' })).usable, false, '前回の世代が残っている');
  assert.equal((await run(undefined, fresh)).usable, false, '保存できたか分からない');
});

await t('🚨 スナップショットが落ちても投げない (日次処理を止めない)', async () => {
  const r = await captureInboundPhase('S0', { businessDate: '2026-09-27', call: null, snapshotOpts: { sleep: async () => {}, paceMs: 0 } });
  assert.match(r.note, /^S0 (不完全|失敗)/);
});

await t('runFbaReportSnapshot: S0 → レポート → S1 の順に呼び、結果の 1 行に注記。inboundCapture が投げてもレポートの保存は成功', async () => {
  const order = [];
  const db = {
    saveRestockInventoryToDailySnapshot: (rows) => { order.push('save'); return { updated: 0, inserted: rows.length }; },
    saveRestockLatest: (rows) => ({ saved: rows.length }), updateFnskuBatch: () => {}, savePlanningData: (rows) => rows.length,
    savePlanningLatest: (rows) => ({ saved: rows.length }), syncFnskuBatch: () => {}, saveStockExport: () => ({ saved: true }),
    getInputFreshness: () => ({ restock_source_at: 'x' }),
  };
  const r = await runFbaReportSnapshot({
    db, businessDate: '2026-09-27', usContext: { market: 'us' }, log: () => {}, warn: () => {},
    fetchReports: async () => { order.push('fetch'); return { restock: [{ 'Merchant SKU': 'a', Available: '1', 'Inbound Working': '3' }], planning: [], errors: [] }; },
    inboundCapture: async (phase, ctx) => { order.push(`${phase}${ctx.restockRows ? `:${ctx.restockRows.length}:${ctx.freshness.restock_source_at}:${ctx.saved.restock}:${ctx.saved.planning}` : ''}`); return { note: `${phase} ok` }; },
  });
  assert.deepEqual(order, ['S0', 'fetch', 'save', 'S1:1:x:true:false'], 'PLANNING が無い回は planning の保存 = false を渡す');
  assert.equal(r.ok, true);
  assert.match(r.lastLine, / \/ 準備中の基準: S0 ok → S1 ok$/);
  const r2 = await runFbaReportSnapshot({
    db, businessDate: '2026-09-27', usContext: { market: 'us' }, log: () => {}, warn: () => {},
    fetchReports: async () => ({ restock: [{ 'Merchant SKU': 'a', Available: '1' }], planning: [], errors: [] }),
    inboundCapture: async () => { throw new Error('SP-API 403'); },
  });
  assert.equal(r2.ok, true);
  assert.match(r2.lastLine, /S0 失敗: SP-API 403 → S1 失敗: SP-API 403$/);
});

try { openInboundStateDb().close(); } catch { /* 閉じ済み */ }
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
