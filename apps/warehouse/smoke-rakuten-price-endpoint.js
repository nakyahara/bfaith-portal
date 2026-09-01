#!/usr/bin/env node
/**
 * smoke-rakuten-price-endpoint.js — 価格更新エンドポイントの実機 smoke (価格一括改定 M2)
 *
 * PATCH /service-api/rakuten-rms/items/manage-numbers/:mn/prices を、**非公開のテスト商品**に対して
 * 一通り通す。安全装置 (楽観ロック・冪等・新規SKU作成の防止・0円拒否) が実機でも効くかを確かめる。
 *
 * 🚨対象は `zz-` で始まる管理番号だけ。本番商品では動かない (このスクリプト自身が拒否する)。
 *    書き込みは --live を付けたときだけ。付けなければ「何を送るか」を出して終わる。
 *
 * 使い方 (miniPC 上・リポジトリ直下):
 *   node -r dotenv/config apps/warehouse/smoke-rakuten-price-endpoint.js            … dry-run
 *   node -r dotenv/config apps/warehouse/smoke-rakuten-price-endpoint.js --live     … 実行
 * env: WAREHOUSE_INTERNAL_URL (既定 http://localhost:3000) / SERVICE_TOKEN
 *      M2_MANAGE_NUMBER でテスト商品を変えられる (既定 zz-price-m2-0901)
 */
import crypto from 'node:crypto';

const MN = (process.env.M2_MANAGE_NUMBER || 'zz-price-m2-0901').trim().toLowerCase();
const BASE = (process.env.WAREHOUSE_INTERNAL_URL || 'http://localhost:3000').replace(/\/+$/, '');
const TOKEN = process.env.SERVICE_TOKEN;
const LIVE = process.argv.includes('--live');

if (!MN.startsWith('zz-')) {
  console.error(`FATAL: 対象は zz- で始まるテスト商品だけです (指定: ${MN})`);
  process.exit(2);
}
if (!TOKEN) { console.error('FATAL: SERVICE_TOKEN が必要です'); process.exit(2); }

const H = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
const newOpId = (tag) => `smoke-${tag}-${Date.now().toString(36)}-${crypto.randomBytes(3).toString('hex')}`;

let failed = 0;
const ok = (cond, label, extra = '') => {
  console.log(`${cond ? '  ok  ' : '  FAIL'} ${label}${extra ? ' — ' + extra : ''}`);
  if (!cond) failed++;
};

async function getItem() {
  const res = await fetch(`${BASE}/service-api/rakuten-rms/items/details-bulk`, {
    method: 'POST', headers: H, body: JSON.stringify({ itemCodes: [MN] }),
  });
  const j = await res.json();
  return (j.items || [])[0] || null;
}

async function patchPrices(body) {
  const res = await fetch(`${BASE}/service-api/rakuten-rms/items/manage-numbers/${MN}/prices`, {
    method: 'PATCH', headers: H, body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json().catch(() => null) };
}

(async () => {
  const item = await getItem();
  if (!item) { console.error(`FATAL: ${MN} が見つかりません。先にテスト商品を作ってください`); process.exit(1); }
  const skus = Object.keys(item.variants || {});
  if (skus.length === 0) { console.error('FATAL: SKU がありません'); process.exit(1); }
  const sku = skus[0];
  const current = Number(String(item.variants[sku].standardPrice).trim());
  console.log(`対象: ${MN} / SKU ${sku} / 現在価格 ${current} 円 (非公開=${item.hideItem})`);
  const next = current + 11;   // 戻すときに分かりやすい端数

  if (!LIVE) {
    console.log('\n--- dry-run: --live を付けると以下を実行します ---');
    console.log(`1. 更新前価格を間違えて送る (expected=${current + 999}) → CONFLICT で何も起きないこと`);
    console.log(`2. 存在しない SKU を送る → SKU_NOT_FOUND (新規SKUが作られないこと)`);
    console.log('3. 0円を送る → INVALID_PRICE');
    console.log(`4. 正しく更新 (${current} → ${next}) → applied`);
    console.log('5. 同じ operation_id で再送 → 実行されず初回と同じ応答が返ること');
    console.log('6. 同じ ID で別価格を送る → OPERATION_ID_REUSED');
    console.log(`7. 同じ価格を送る → noop`);
    console.log(`8. 元の価格 (${current}) に戻す`);
    return;
  }

  console.log('\n[1] 更新前価格が違う → CONFLICT');
  {
    const r = await patchPrices({ operation_id: newOpId('conflict'), expected: { [sku]: current + 999 }, prices: { [sku]: next } });
    ok(r.status === 409 && r.body?.error === 'CONFLICT', 'CONFLICT が返る', `status=${r.status}`);
    const after = await getItem();
    ok(Number(after.variants[sku].standardPrice) === current, '価格は変わっていない');
  }

  console.log('\n[2] 存在しない SKU → SKU_NOT_FOUND (新規SKUを作らない)');
  {
    const ghost = `${MN}-ghost`;
    const r = await patchPrices({ operation_id: newOpId('ghost'), expected: { [ghost]: 100 }, prices: { [ghost]: 200 } });
    ok(r.status === 400 && r.body?.error === 'SKU_NOT_FOUND', 'SKU_NOT_FOUND が返る', `status=${r.status}`);
    const after = await getItem();
    ok(!Object.keys(after.variants).includes(ghost), '★存在しない SKU が作られていない');
  }

  console.log('\n[3] 0円 → INVALID_PRICE');
  {
    const r = await patchPrices({ operation_id: newOpId('zero'), expected: { [sku]: current }, prices: { [sku]: 0 } });
    ok(r.status === 400 && r.body?.error === 'INVALID_PRICE', 'INVALID_PRICE が返る', `status=${r.status}`);
    const after = await getItem();
    ok(Number(after.variants[sku].standardPrice) === current, '★0円にされていない (楽天API単体では204で通る値)');
  }

  console.log(`\n[4] 正しく更新 ${current} → ${next}`);
  const opId = newOpId('apply');
  {
    const r = await patchPrices({ operation_id: opId, expected: { [sku]: current }, prices: { [sku]: next } });
    ok(r.status === 200 && r.body?.state === 'applied', 'applied が返る', `status=${r.status}`);
    const after = await getItem();
    ok(Number(after.variants[sku].standardPrice) === next, `価格が ${next} になった`);
    const others = skus.slice(1);
    for (const o of others) {
      ok(String(after.variants[o].standardPrice) === String(item.variants[o].standardPrice), `他SKU (${o}) は無傷`);
    }
  }

  console.log('\n[5] 同じ operation_id で再送 → 実行されず同じ応答');
  {
    const r = await patchPrices({ operation_id: opId, expected: { [sku]: current }, prices: { [sku]: next } });
    ok(r.status === 200 && r.body?.replay === true && r.body?.state === 'applied', '初回と同じ応答 + replay', `status=${r.status}`);
  }

  console.log('\n[6] 同じ ID で別価格 → OPERATION_ID_REUSED');
  {
    const r = await patchPrices({ operation_id: opId, expected: { [sku]: next }, prices: { [sku]: next + 5 } });
    ok(r.status === 409 && r.body?.error === 'OPERATION_ID_REUSED', '使い回しを拒否', `status=${r.status}`);
    const after = await getItem();
    ok(Number(after.variants[sku].standardPrice) === next, '価格は変わっていない');
  }

  console.log('\n[7] 同じ価格 → noop');
  {
    const r = await patchPrices({ operation_id: newOpId('noop'), expected: { [sku]: next }, prices: { [sku]: next } });
    ok(r.status === 200 && r.body?.state === 'noop', 'noop が返る', `status=${r.status}`);
  }

  console.log(`\n[8] 元の価格 ${current} に戻す`);
  {
    const r = await patchPrices({ operation_id: newOpId('revert'), expected: { [sku]: next }, prices: { [sku]: current } });
    ok(r.status === 200 && r.body?.state === 'applied', '戻せた', `status=${r.status}`);
    const after = await getItem();
    ok(Number(after.variants[sku].standardPrice) === current, `価格が ${current} に戻った`);
  }

  console.log(`\n${failed === 0 ? '✅ smoke 全項目 pass' : `❌ ${failed} 件 FAIL`}`);
  process.exitCode = failed === 0 ? 0 : 1;
})();
