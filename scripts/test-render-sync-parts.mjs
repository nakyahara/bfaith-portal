#!/usr/bin/env node
/**
 * test-render-sync-parts.mjs — Render 同期の「部」(1 回の POST) の組み立てと大きさの見張り (2026-09-15〜16 の 413 の再発防止)
 *
 *   出荷サマリ (shipments_daily) はマスタとは別の部 / state で送る・送らない / 大きさはバイト数 / 11MB を超える部は送る前に止める
 * 実行: node scripts/test-render-sync-parts.mjs
 */
import assert from 'node:assert/strict';
import { buildMasterSyncParts, partSize, assertPartFits, makeSendPart, MAX_PART_BYTES, RECEIVER_LIMIT_BYTES } from '../apps/warehouse/sync-to-render.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + e.message); } };
const master = { products: [{ 商品コード: 'a', 商品名: '見本' }], set_components: [], amazon_sku_fees: [], rakuten_sku_map: [], inv_daily_summary: [] };

await t('出荷サマリはマスタとは別の部 (受け口は鍵ごとに全件置換)。state ok なら 0 件でも送る', () => {
  const rows = [{ ship_date: '2025-01-02', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 3, cancelled_slips: 0 }];
  const parts = buildMasterSyncParts({ masterPart: master, shipments_daily: rows, shipments_daily_state: 'ok' });
  assert.deepEqual(parts.map((p) => p.label), ['マスタ', '出荷サマリ 1件']);
  assert.equal(parts[0].payload, master);
  assert.ok(!('shipments_daily' in parts[0].payload));
  assert.deepEqual(Object.keys(parts[1].payload), ['shipments_daily']);
  assert.equal(parts[1].payload.shipments_daily, rows);
  const empty = buildMasterSyncParts({ masterPart: master, shipments_daily: [], shipments_daily_state: 'ok' });
  assert.deepEqual([empty.length, empty[1].payload.shipments_daily], [2, []]);
});
await t('stale / failed のときは出荷サマリを送らない (Render は前回分を保持)。マスタに混ぜたら例外', () => {
  for (const st of ['stale', 'failed', 'empty_skipped']) {
    const parts = buildMasterSyncParts({ masterPart: master, shipments_daily: [], shipments_daily_state: st });
    assert.deepEqual(parts.map((p) => p.label), ['マスタ']);
  }
  assert.throws(() => buildMasterSyncParts({ masterPart: { ...master, shipments_daily: [] }, shipments_daily: [], shipments_daily_state: 'ok' }), /masterPart に shipments_daily を入れない/);
  assert.throws(() => buildMasterSyncParts({ masterPart: master, shipments_daily: null, shipments_daily_state: 'ok' }), /配列でない/);
});
await t('大きさはバイト数で見る (日本語は 1 文字 3 バイト = 文字数では小さく見える)', () => {
  const { json, bytes } = partSize({ products: [{ 商品名: 'あいうえお' }] });
  assert.ok(bytes > json.length, `${bytes} > ${json.length}`);
  assert.equal(bytes, Buffer.byteLength(json));
});
await t('受け口の上限 (12MB) に余裕を見た 11MB を超える部は送る前に止める。以内なら通る (2026-09-15 の実測値で)', () => {
  assert.equal(MAX_PART_BYTES, 11 * 1024 * 1024);
  assert.equal(RECEIVER_LIMIT_BYTES, 12 * 1024 * 1024);
  assert.doesNotThrow(() => assertPartFits('マスタ', MAX_PART_BYTES));
  assert.throws(() => assertPartFits('マスタ', MAX_PART_BYTES + 1), /マスタ: 11\.00MB は受け口の上限 \(12MB。余裕を見て 11MB まで\) を超える/);
  assert.throws(() => assertPartFits('マスタ', 12998961), /12\.40MB/);                              // 9/16 実測: 出荷サマリ込みのマスタ → 止まる
  assert.doesNotThrow(() => assertPartFits('マスタ', Math.round(8.79 * 1048576)));                  // 出荷サマリを外したマスタ → 通る
  assert.doesNotThrow(() => assertPartFits('出荷サマリ 15930件', Math.round(3.6 * 1048576)));      // 出荷サマリ単独 → 通る
});

await t('送信関数 (本物の経路): 上限を超える部は fetch を 1 回も呼ばずに例外 / 許容なら同じ JSON を 1 回 POST / ログは UTF-8 のバイト数 (文字数ではない)', async () => {
  const calls = [], logs = [];
  const fetchImpl = async (url, init) => { calls.push({ url, init }); return new Response(JSON.stringify({ ok: true, echo: init.body.length }), { status: 200 }); };
  const sendPart = makeSendPart({ url: 'https://render.example/apps/mirror/api/sync', headers: { 'Content-Type': 'application/json', 'x-sync-key': 'k' }, fetchImpl, log: (m) => logs.push(m) });
  const data = { products: [{ 商品コード: 'a', 商品名: 'あいうえお' }] };
  const json = JSON.stringify(data);
  const r = await sendPart(data, 'マスタ');
  assert.deepEqual([r.ok, calls.length, calls[0].url, calls[0].init.method, calls[0].init.body, calls[0].init.headers['x-sync-key']], [true, 1, 'https://render.example/apps/mirror/api/sync', 'POST', json, 'k']);
  assert.ok(logs[0].includes(`= ${Buffer.byteLength(json)} bytes`) && Buffer.byteLength(json) > json.length, logs[0]);   // 日本語は 3 バイト → 文字数より大きい数がログに出る
  // 上限超過: fetch は呼ばれない (送っても 413 が続くだけ)
  const big = { products: [{ 商品名: 'あ'.repeat(4 * 1024 * 1024) }] };                                                // 4M 文字 = 12MB
  await assert.rejects(() => sendPart(big, 'マスタ'), /マスタ: 12\.00MB は受け口の上限/);
  assert.equal(calls.length, 1);
  // HTTP エラーは label 付きの例外 (呼ぶ側が握りつぶさなければ全体の失敗になる)
  const failing = makeSendPart({ url: 'https://render.example/x', headers: {}, fetchImpl: async () => new Response('{"error":"payload_too_large","limit":12582912}', { status: 413 }), log: () => {} });
  await assert.rejects(() => failing({ a: 1 }, '出荷サマリ 1件'), /出荷サマリ 1件: HTTP 413 .*payload_too_large/);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
