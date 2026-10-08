// /upload → /confirm の統合テスト — 一時ディレクトリに warehouse-mirror.db を実初期化 (DATA_DIR) して、
// 本物の mirror_products と mart_amazon_monthly_summary で確定の可否を確かめる。
//   ・未登録SKUがあっても確定できる (10%・その他/未分類・原価0円で集計に入り、unresolved_count が残る)
//   ・税率未登録は引き続き確定できない
//   node --test apps/amazon-accounting/confirm.test.mjs
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

// DATA_DIR は db.js / router.js の import 時に評価されるため、動的 import の前に設定する
const prevDataDir = process.env.DATA_DIR;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'aacct-confirm-'));
process.env.DATA_DIR = tmp;
let db, base, server;

before(async () => {
  const dbMod = await import('../warehouse-mirror/db.js');
  const router = (await import('./router.js')).default;
  db = dbMod.initMirrorDB();
  const ins = db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価, 原価状態, 消費税率, 売上分類, updated_at)
    VALUES (?, ?, '単品', ?, 'ok', ?, ?, '2026-10-01')`);
  ins.run('sku-a', '登録済み商品', 100, 0.1, 1);
  ins.run('sku-notax', '税率なし商品', 50, null, 1);
  const app = express();
  app.use(express.json());
  app.use('/apps/amazon-accounting', router);
  server = app.listen(0);
  base = 'http://127.0.0.1:' + server.address().port + '/apps/amazon-accounting';
});

after(() => {
  server?.close();
  try { db?.close(); } catch {}
  if (prevDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prevDataDir;
  fs.rmSync(tmp, { recursive: true, force: true });
});

const HDR = ['日付/時間','決済番号','トランザクションの種類','注文番号','SKU','説明','数量','Amazon 出品サービス','フルフィルメント','市町村','都道府県','郵便番号','税金徴収型',
  '商品売上','商品の売上税','配送料','配送料の税金','ギフト包装手数料','ギフト包装クレジットの税金','Amazonポイントの費用','プロモーション割引額','プロモーション割引の税金',
  '源泉徴収税を伴うマーケットプレイス','手数料','FBA 手数料','トランザクションに関するその他の手数料','その他','合計','トランザクションのステータス','トランザクション開始日'];
const q = a => a.map(v => '"' + v + '"').join(',');
const order = (date, sku, desc, qty, sales, total) =>
  q([date + ' 12:00:00 JST','1','注文','249-' + sku,sku,desc,String(qty),'','','','','','',String(sales),'0','0','0','0','0','0','0','0','','0','0','0','0',String(total),'支払い実行済み','']);

async function upload(lines) {
  const fd = new FormData();
  fd.append('file', new Blob([[q(HDR), ...lines].join('\r\n')], { type: 'text/csv' }), 'payment.csv');
  return (await fetch(base + '/upload', { method: 'POST', body: fd })).json();
}
async function confirmMonth(yearMonth) {
  const r = await fetch(base + '/confirm', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ yearMonth, adCost: 0, csvFilename: 'payment.csv' }) });
  return { status: r.status, body: await r.json() };
}

test('未登録SKUがあっても確定できる: 10%・その他/未分類・原価0円で集計に入り、unresolved_count が残る', async () => {
  const data = await upload([
    order('2026/07/03', 'SKU-A', '登録済み商品', 2, 1000, 900),
    order('2026/07/04', 'SKU-NEW', '未登録の新商品', 1, 500, 450),
  ]);
  assert.equal(data.yearMonth, '2026-07');
  assert.equal(data.unresolvedSkus.length, 1);
  assert.equal(data.unresolvedSkus[0].sku, 'sku-new');
  assert.equal(data.canConfirm, true);
  // 未登録SKUの行は 10% と「その他/未分類」に原価0円で入っている (登録済みは分類1)
  assert.equal(data.byTax['10'].商品売上, 1500);
  assert.equal(data.bySegment['other'].商品売上, 500);
  assert.equal(data.bySegment['other'].原価合計, 0);
  assert.equal(data.bySegment['1'].原価合計, 200);

  const { status, body } = await confirmMonth('2026-07');
  assert.equal(status, 200, JSON.stringify(body));
  assert.equal(body.ok, true);
  const saved = db.prepare('SELECT unresolved_count, by_segment FROM mart_amazon_monthly_summary WHERE year_month = ?').get('2026-07');
  assert.equal(saved.unresolved_count, 1);
  assert.equal(JSON.parse(saved.by_segment).other.商品売上, 500);
});

test('税率未登録は引き続き確定できない', async () => {
  const data = await upload([
    order('2026/08/03', 'SKU-A', '登録済み商品', 1, 500, 450),
    order('2026/08/04', 'SKU-NOTAX', '税率なし商品', 1, 300, 270),
  ]);
  assert.equal(data.yearMonth, '2026-08');
  assert.equal(data.unresolvedTax.length, 1);
  assert.equal(data.canConfirm, false);
  const { status, body } = await confirmMonth('2026-08');
  assert.equal(status, 400);
  assert.match(body.error, /税率未登録/);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM mart_amazon_monthly_summary WHERE year_month = ?').get('2026-08').n, 0);
});
