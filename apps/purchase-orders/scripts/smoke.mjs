// purchase-orders スモークテスト (scratch DATA_DIR + 実 express 起動)
// 実行: node apps/purchase-orders/scripts/smoke.mjs
import fs from 'fs';
import os from 'os';
import path from 'path';
import { pathToFileURL, fileURLToPath } from 'url';

const SCRATCH = fs.mkdtempSync(path.join(os.tmpdir(), 'po-smoke-'));
process.env.DATA_DIR = SCRATCH;

const WORK = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const imp = p => import(pathToFileURL(path.join(WORK, p)).href);
const { initMirrorDB } = await imp('apps/warehouse-mirror/db.js');
initMirrorDB(); // 本番では server.js (warehouse-mirror router import) が起動時に実行する
const { getDB } = await imp('apps/purchase-orders/db.js');
const { computeProduct, stockConstant } = await imp('apps/purchase-orders/logic.js');
const routerMod = await imp('apps/purchase-orders/router.js');
const express = (await import('express')).default;

let pass = 0, fail = 0;
function ok(cond, name, extra) {
  if (cond) { pass++; console.log('  ✅', name); }
  else { fail++; console.log('  ❌', name, extra == null ? '' : JSON.stringify(extra)); }
}

// ── 1. DB init + PML fixtures ──
const db = getDB();
db.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, synced_at) VALUES (1, 'run_test', 'ok', ?, ?)`)
  .run(new Date(Date.now() - 86400000).toISOString().slice(0, 10), new Date().toISOString());
const insRow = db.prepare(`INSERT INTO mirror_pml_snapshot_rows
  (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 売上分類, 総在庫数_引当なし, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数, 売価, 原価, 最終仕入日, 登録日)
  VALUES ('run_test', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
// 実スプレッドシートの行を fixture 化 (発注量の期待値はシート実値)
insRow.run('noflyersticker', 'チラシ お断り ステッカー', '0001', '取扱中', 2, 490, 0, 46, 368, 100, 1.5, 380, 70, '2026-07-02', '2020-05-24');
insRow.run('cardstand-silver-r', 'AMC カードスタンド シルバー', '0001', '取扱中', 2, 728, 0, 80, 485, 10000, 2.5, 398, 55, '2026-05-19', '2021-10-05');
insRow.run('0726-001060', '肉球クリーム 30g', '0001', '取扱中', 1, 3559, 0, 176, 888, 1000, 2.5, 698, 245, '2026-07-02', '2019-12-11');
insRow.run('deaditem', '休眠商品', '0001', '取扱中', 2, 50, 0, 0, 0, 100, 1.5, 500, 200, '2025-01-01', '2020-01-01');
insRow.run('teishi-item', '取扱中止商品', '0001', '取扱中止', 2, 0, 0, 0, 10, 100, 1.5, 500, 200, '2025-01-01', '2020-01-01');
insRow.run('gyoumuhandcream60-BI', 'プロ業務用ハンドクリーム 60g 微香', '0002', '取扱中', 3, 195, 178, 55, 258, 24, 1, 1236, 672, '2026-07-01', '2022-02-03');
insRow.run('diyorangeoil100', '木工用オレンジオイル 100ml', '0001', '取扱中', 1, 1536, 900, 169, 808, 600, 1.5, 698, 270, '2026-06-30', '2021-05-23');

console.log('── computeProduct (シート数式一致) ──');
const get = code => computeProduct(db.prepare(`SELECT * FROM mirror_pml_snapshot_rows WHERE 商品コード=?`).get(code));
const nofly = get('noflyersticker');
ok(nofly.isTarget === true, 'noflyersticker 発注対象');
ok(nofly.recQty === 400, 'noflyersticker 発注量=400 (シート実値)', nofly.recQty);
const card = get('cardstand-silver-r');
ok(card.isTarget === true, 'cardstand-silver-r 発注対象');
ok(card.recQty === 10000, 'cardstand-silver-r 発注量=10000 (最低1ロット)', card.recQty);
const niku = get('0726-001060');
ok(niku.isTarget === false, '0726-001060 在庫4ヶ月分→対象外', niku.stockMonths);
const dead = get('deaditem');
ok(dead.isHorikoshi === true && dead.isTarget === false, 'deaditem 掘り起こし');
const teishi = get('teishi-item');
ok(teishi.isTarget === false && teishi.isHorikoshi === false, '取扱中止は対象外');
const oil = get('diyorangeoil100');
// L=(1536+900)/808=3.0148>1.5 → 対象外 (シートも空)
ok(oil.isTarget === false, 'diyorangeoil100 注残込みで対象外', oil.stockMonths);
ok(stockConstant(1) === 0.5 && stockConstant(1.5) === 1 && stockConstant(2.5) === 2 && stockConstant(4) === 3, '在庫定数 IFS 移植');

// ── 2. express 起動 (認証なしで直 mount) ──
const app = express();
app.use('/apps/purchase-orders', express.json({ limit: '1mb' }), routerMod.default);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}/apps/purchase-orders`;
const j = async (p, opt) => {
  const r = await fetch(base + p, opt);
  return { status: r.status, body: await r.json().catch(() => null) };
};

console.log('── マスタ API ──');
let r = await j('/api/masters/suppliers', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ supplier_code: '0001', name: 'アメージングクラフト様', order_memo: '' }) });
ok(r.status === 200 && r.body.ok, '仕入先 upsert (0001→1 正規化)');
r = await j('/api/masters/suppliers');
ok(r.body.rows.length === 1 && r.body.rows[0].supplier_code === '1', '正規化コードで保存', r.body.rows[0]);
r = await j('/api/masters/conditions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ condition_id: 'testcond', supplier_code: '1', maker_name: 'テスト', display_name: 'テスト条件', condition_type: '金額', condition_value: 50000, unit: '円' }) });
ok(r.status === 200 && r.body.ok, '発注条件 upsert');
r = await j('/api/masters/materials', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ group_id: 'mokouorange', name: '木工用オレンジオイル', min_order_qty: 100000, unit: 'ml' }) });
ok(r.status === 200 && r.body.ok, '原料グループ upsert');
r = await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'diyorangeoil100', material_group_id: 'mokouorange', capacity_per_unit: 100 }) });
ok(r.status === 200 && r.body.ok, '商品紐付け upsert');
r = await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'NOFLYERSTICKER ', condition_id: 'testcond' }) });
ok(r.status === 200, '商品紐付け (大文字+空白→key正規化)');
r = await j('/api/masters/conditions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ condition_id: 'bad', display_name: 'x', condition_type: '金額', condition_value: -5 }) });
ok(r.status === 400, '条件値マイナスは 400');

console.log('── CSV 取込 ──');
const csv = '﻿仕入先コード,仕入先名,発注メモ\r\n0002,ビーフリー様,FAX発注\r\n107,株式会社サロンジェ様,\r\n';
const fd = new FormData();
fd.append('file', new Blob([csv], { type: 'text/csv' }), 'suppliers.csv');
r = await j('/api/masters/suppliers/csv', { method: 'POST', body: fd });
ok(r.status === 200 && r.body.upserted === 2, '仕入先CSV取込 2件', r.body);
const badCsv = 'ダメな見出し,x\r\na,b\r\n';
const fd2 = new FormData();
fd2.append('file', new Blob([badCsv]), 'bad.csv');
r = await j('/api/masters/suppliers/csv', { method: 'POST', body: fd2 });
ok(r.status === 400, '見出し不一致CSVは 400');

console.log('── overview / supplier ──');
r = await j('/api/overview');
ok(r.body.ok && r.body.cards.length === 1 && r.body.cards[0].code === '1', 'overview: 仕入先1のみ要発注', r.body.cards);
ok(r.body.cards[0].targetCount === 2, 'overview: 要発注2SKU', r.body.cards[0]);
r = await j('/api/supplier/0001');
ok(r.body.ok && r.body.supplier.name === 'アメージングクラフト様', 'supplier: 名前解決 (0001→1)');
ok(r.body.targets.length === 2, 'supplier: targets=2', r.body.targets.map(t => t.code));
ok(r.body.horikoshi.length === 1 && r.body.horikoshi[0].code === 'deaditem', 'supplier: 掘り起こし=deaditem');
ok(r.body.conditions.length === 1 && r.body.conditions[0].memberCodes.includes('noflyersticker'), 'supplier: 条件メンバー解決', r.body.conditions);
ok(r.body.materialGroups.length === 1 && r.body.materialGroups[0].memberCodes.includes('diyorangeoil100'), 'supplier: 原料グループ解決');

console.log('── draft / issue ──');
r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 400 }], note: 'メモ1', supplierName: 'アメージングクラフト様' }) });
ok(r.status === 200 && r.body.ok, 'draft 保存');
const draftId = r.body.id;
r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 500 }, { code: 'cardstand-silver-r', qty: 10000 }], note: 'メモ2', supplierName: 'アメージングクラフト様' }) });
ok(r.body.id === draftId, 'draft 再保存で同一ID (仕入先1件制約)', { first: draftId, second: r.body.id });
r = await j('/api/supplier/1');
ok(r.body.draft && r.body.draft.items.length === 2 && r.body.draft.note === 'メモ2', 'draft 復元');
r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'unknown-xyz', qty: 10 }] }) });
ok(r.status === 400, 'PML非存在コードは 400');
r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 2.5 }] }) });
ok(r.status === 400, '小数量は 400');
r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 400 }, { code: 'cardstand-silver-r', qty: 10000 }], note: '確定テスト', supplierName: 'アメージングクラフト様' }) });
ok(r.status === 200 && r.body.ok, 'issue 確定');
const issuedId = r.body.id;
r = await j('/api/orders');
ok(r.body.orders.length === 1 && r.body.orders[0].status === 'issued' && r.body.orders[0].sku_count === 2, '履歴 1件 issued', r.body.orders[0]);
r = await j('/api/orders/' + issuedId);
ok(r.body.order.items.length === 2 && r.body.order.items[0].unit_cost === 70, '明細+原価スナップショット', r.body.order.items);
r = await j('/api/supplier/1');
ok(r.body.draft === null, 'issue 後 draft は消える');
ok(r.body.targets.find(t => t.code === 'noflyersticker').recentIssued != null, '発注済みバッジ (recentIssued)');
r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [], note: '' }) });
ok(r.status === 400, '空 issue は 400');

console.log('── Codex R1 修正回帰 ──');
r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'gyoumuhandcream60-BI', qty: 24 }] }) });
ok(r.status === 400 && r.body.error.includes('仕入先が一致しない'), '別仕入先の商品混入は 400 (High)');
r = await j('/api/orders/' + issuedId);
ok(r.body.order.supplier_name === 'アメージングクラフト様', '仕入先名はサーバ解決', r.body.order.supplier_name);
r = await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'noflyersticker', condition_id: 'no-such-cond' }) });
ok(r.status === 400 && r.body.error.includes('未登録'), '存在しない条件グループ紐付けは 400');
r = await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'noflyersticker', case_lot: -3 }) });
ok(r.status === 400, 'ケースロット負数は 400');
// 大文字違いは同一 product_key に collapse (PK=product_key)
r = await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'DIYORANGEOIL100', material_group_id: 'mokouorange', capacity_per_unit: 200 }) });
ok(r.status === 200, '大文字違い attrs upsert 成功');
r = await j('/api/masters/attrs');
const oilAttrs = r.body.rows.filter(x => x.product_key === 'diyorangeoil100');
ok(oilAttrs.length === 1 && oilAttrs[0].capacity_per_unit === 200, '同一keyは1行に collapse (更新扱い)', oilAttrs);
r = await j('/api/masters/conditions/testcond', { method: 'DELETE' });
ok(r.status === 400 && r.body.error.includes('参照'), '被参照条件グループの削除は 400');
r = await j('/api/masters/materials/mokouorange', { method: 'DELETE' });
ok(r.status === 400, '被参照原料グループの削除は 400');
// 参照を外してから削除は成功する
await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'NOFLYERSTICKER ' }) });
r = await j('/api/masters/conditions/testcond', { method: 'DELETE' });
ok(r.status === 200 && r.body.deleted === 1, '参照解除後の削除は成功');
// 後続テスト用に条件を戻す
await j('/api/masters/conditions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ condition_id: 'testcond', supplier_code: '1', maker_name: 'テスト', display_name: 'テスト条件', condition_type: '金額', condition_value: 50000, unit: '円' }) });
await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'NOFLYERSTICKER ', condition_id: 'testcond' }) });

console.log('── Codex R2 修正回帰 ──');
// issue は draft を mutate せず新規 insert (id が draft と別)
r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 100 }], note: 'draft again' }) });
const draftId2 = r.body.id;
r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 200 }], note: 'issue2' }) });
ok(r.body.id !== draftId2, 'issue は新規 order (draft と別ID)', { draft: draftId2, issued: r.body.id });
r = await j('/api/orders');
ok(r.body.orders.filter(o => o.status === 'draft').length === 0, 'issue 後 draft 行は削除');
// CSV fail-closed: 1行でも不正なら全ロールバック
const mixedCsv = '原料グループID,原料グループ名,最低発注量,単位\r\nnewmat1,テスト原料1,1000,ml\r\n,名前なしID,500,ml\r\n';
const fd3 = new FormData();
fd3.append('file', new Blob([mixedCsv]), 'mixed.csv');
r = await j('/api/masters/materials/csv', { method: 'POST', body: fd3 });
ok(r.status === 400 && Array.isArray(r.body.errors), 'CSV不正行ありは 400 + errors');
r = await j('/api/masters/materials');
ok(!r.body.rows.some(x => x.group_id === 'newmat1'), 'CSV は全件ロールバック (部分反映なし)');
// draftExtras: draft 保存後に PML 側が取扱中止になっても明細が見える
await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 100 }], note: 'extra test' }) });
db.prepare(`UPDATE mirror_pml_snapshot_rows SET 取扱区分='取扱中止' WHERE 商品コード='noflyersticker'`).run();
r = await j('/api/supplier/1');
ok(r.body.draftExtras && r.body.draftExtras.length === 1 && r.body.draftExtras[0].extra === true, 'リスト外 draft 明細は draftExtras で返る', r.body.draftExtras);
// R3 Medium: リスト外明細も条件グループの memberCodes に含まれる (noflyersticker は testcond 紐付け済み)
const cond = r.body.conditions.find(c => c.conditionId === 'testcond');
ok(cond && cond.memberCodes.includes('noflyersticker'), 'リスト外明細も条件 membership に含まれる', cond && cond.memberCodes);
db.prepare(`UPDATE mirror_pml_snapshot_rows SET 取扱区分='取扱中' WHERE 商品コード='noflyersticker'`).run();
await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items: [] }) });

console.log('── 画面 (HTML) ──');
for (const p of ['/', '/supplier/1', '/orders', '/admin']) {
  const res = await fetch(base + p);
  const html = await res.text();
  ok(res.status === 200 && html.includes('<!DOCTYPE html>'), `GET ${p} → 200 HTML`);
  ok(!html.includes('undefined') || p === '/', `GET ${p} に undefined 露出なし`);
}
r = await j('/api/attrs/unlinked');
ok(r.body.ok && r.body.rows.every(x => x.code.toLowerCase() !== 'diyorangeoil100'), 'unlinked: 紐付け済みは出ない');
ok(r.body.rows.some(x => x.code === '0726-001060'), 'unlinked: 未紐付け取扱中は出る');

server.close();
console.log(`\n=== RESULT: pass=${pass} fail=${fail} ===`);
process.exit(fail ? 1 : 0);
