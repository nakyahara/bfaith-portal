/** Chrome拡張向けAPI (x-api-key認証) のHTTP経由テスト。SP-APIは呼ばずスタブで差し替える。 */
import assert from 'node:assert/strict';
import express from 'express';

let pass = 0;
const t = async (name, fn) => { await fn(); pass++; console.log(`  ok  ${name}`); };
console.log('tracking-ext-api');

// SP-API を叩かせないため、依存をスタブで差し込む (createTrackingExtRouter の deps)
const STUB = {
  planName: '8/10プラン①', planStatus: 'ACTIVE',
  shipments: [
    { shipmentId: 'sh-a', shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', status: 'READY_TO_SHIP', boxCount: 5, hasTracking: false, address: {} },
    { shipmentId: 'sh-b', shipmentConfirmationId: 'FBA15NEW0001', fcCode: 'ZZZ9', status: 'READY_TO_SHIP', boxCount: 2, hasTracking: false,
      address: { postalCode: '350-1301', stateOrProvinceCode: '埼玉県', city: '狭山市', addressLine1: '青柳 915' } },
  ],
};

process.env.FBA_TRACKING_EXT_TOKEN = 'test-token-1234567890';
const { createTrackingExtRouter } = await import('../tracking-ext-router.js');
const extRouter = createTrackingExtRouter({ getPlanShipments: async () => STUB, missingEnv: () => [] });

const app = express();
app.use('/ext', extRouter);
const server = app.listen(0);
const port = server.address().port;
const url = (p) => `http://127.0.0.1:${port}/ext${p}`;
const WF = 'wfa4a9183e-9582-4c92-8304-48194873be9b';

await t('APIキーが無ければ401', async () => {
  const r = await fetch(url(`/plan?wf=${WF}`));
  assert.equal(r.status, 401);
});

await t('APIキーが違えば401 (長さ違いでも落ちない)', async () => {
  const r = await fetch(url(`/plan?wf=${WF}`), { headers: { 'x-api-key': 'short' } });
  assert.equal(r.status, 401);
});

const H = { 'x-api-key': 'test-token-1234567890' };

await t('プランIDが不正なら400', async () => {
  const r = await fetch(url('/plan?wf=abc'), { headers: H });
  assert.equal(r.status, 400);
  assert.match((await r.json()).error.message, /納品プランID/);
});

await t('Seller CentralのURLをそのまま渡せる', async () => {
  const u = encodeURIComponent(`https://sellercentral.amazon.co.jp/fba/sendtoamazon?wf=${WF}&x=1`);
  const r = await fetch(url(`/plan?url=${u}`), { headers: H });
  assert.equal(r.status, 200);
  assert.equal((await r.json()).data.inboundPlanId, WF);
});

await t('下見: 納品ごとの箱数とマスタ登録状況が返る', async () => {
  const r = await fetch(url(`/plan?wf=${WF}`), { headers: H });
  const { data } = await r.json();
  assert.equal(data.伝票枚数, 7);
  assert.equal(data.shipments[0].宛先, 'マスタ登録済み');
  assert.match(data.shipments[1].宛先, /マスタ未登録/);
});

await t('⭐CSVが落ちてくる。U列に納品番号が入る', async () => {
  const r = await fetch(url(`/plan-csv?wf=${WF}&date=20260810`), { headers: H });
  assert.equal(r.status, 200);
  assert.match(r.headers.get('content-type'), /text\/csv/);
  assert.match(r.headers.get('content-disposition'), /attachment; filename="fukutu_/);
  assert.equal(r.headers.get('x-fukutsu-rows'), '7');
  const lines = (await r.text()).trimEnd().split('\n');
  assert.equal(lines.length, 2 + 7, 'ヘッダー + 空行 + 7行');
  const c = lines[2].split(',');
  assert.equal(c[0], 'HND2');
  assert.equal(c[20], 'FBA15GGL5J2X'); // お客様管理番号
  assert.equal(c[24], '20260810');
  const neo = lines[7].split(',');   // ZZZ9 の1行目
  assert.equal(neo[0], 'ZZZ9');
  assert.equal(neo[2], '埼玉県狭山市青柳 915', 'マスタ未登録なので住所が入る');
});

await t('トークン未設定なら503 fail-closed', async () => {
  const saved = process.env.FBA_TRACKING_EXT_TOKEN;
  delete process.env.FBA_TRACKING_EXT_TOKEN;
  const r = await fetch(url(`/plan?wf=${WF}`), { headers: H });
  assert.equal(r.status, 503);
  process.env.FBA_TRACKING_EXT_TOKEN = saved;
});

server.close();
console.log(`\n${pass} 件すべて通過`);
