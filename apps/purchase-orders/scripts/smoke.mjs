// purchase-orders スモークテスト (scratch DATA_DIR + 実 express 起動)
// 実行: node apps/purchase-orders/scripts/smoke.mjs
import fs from 'fs';
import os from 'os';
import path from 'path';
import { pathToFileURL, fileURLToPath } from 'url';
import iconv from 'iconv-lite';

const SCRATCH = fs.mkdtempSync(path.join(os.tmpdir(), 'po-smoke-'));
process.env.DATA_DIR = SCRATCH;

const WORK = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const imp = p => import(pathToFileURL(path.join(WORK, p)).href);
const { initMirrorDB } = await imp('apps/warehouse-mirror/db.js');
initMirrorDB(); // 本番では server.js (warehouse-mirror router import) が起動時に実行する
const { getDB } = await imp('apps/purchase-orders/db.js');
const { computeProduct, stockConstant, evaluateCondition } = await imp('apps/purchase-orders/logic.js');
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
// 商品紐付けのない仕入先直付き条件は supplierWide=true でカート全体評価 (Codex P5 High)
db.prepare(`INSERT INTO po_order_conditions (condition_id, supplier_code, display_name, condition_type, condition_value, unit, created_at, updated_at)
  VALUES ('SUPWIDE', '1', '仕入先全体5万', '金額', 50000, '円', ?, ?)`).run(new Date().toISOString(), new Date().toISOString());
r = await j('/api/supplier/1');
const supWide = r.body.conditions.find(c => c.conditionId === 'SUPWIDE');
ok(supWide && supWide.supplierWide === true && supWide.memberCodes.length === 0, 'supplier: 紐付けなし直付き条件は supplierWide', supWide);
ok(r.body.conditions.find(c => c.conditionId !== 'SUPWIDE' && !c.supplierWide), 'supplier: 商品紐付きの条件は supplierWide でない');
// 紐付け商品が現在リスト外でも、attrsに1件でも紐付けがあれば supplierWide にしない (Codex P5 R2 High)
db.prepare(`INSERT INTO po_order_conditions (condition_id, supplier_code, display_name, condition_type, condition_value, unit, created_at, updated_at)
  VALUES ('OFFLIST', '1', 'リスト外紐付き条件', '金額', 30000, '円', ?, ?)`).run(new Date().toISOString(), new Date().toISOString());
db.prepare(`INSERT INTO po_product_attrs (product_key, product_code, condition_id, created_at, updated_at)
  VALUES ('gyoumuhandcream60-bi', 'gyoumuhandcream60-BI', 'OFFLIST', ?, ?)`).run(new Date().toISOString(), new Date().toISOString());
r = await j('/api/supplier/1');
const offList = r.body.conditions.find(c => c.conditionId === 'OFFLIST');
ok(offList && offList.supplierWide === false, 'supplier: リスト外商品に紐付く条件は supplierWide でない (カート全体で誤達成しない)', offList);
db.prepare(`DELETE FROM po_product_attrs WHERE product_key='gyoumuhandcream60-bi'`).run();
db.prepare(`DELETE FROM po_order_conditions WHERE condition_id='OFFLIST'`).run();

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

console.log('── NEオーバーレイ (手動CSV) ──');
// NE商品マスタCSV (実ヘッダー準拠)。noflyersticker: 在庫100 / 注残50 に更新 → 総在庫_引当なし=100+FBA(20)=120
db.prepare(`UPDATE mirror_pml_snapshot_rows SET FBA在庫数=20 WHERE 商品コード='noflyersticker'`).run();
const neCsv = '"商品コード","商品名","仕入先コード","原価","売価","取扱区分","代表商品コード","ロケーションコード","配送業者","発注ロット単位","最終仕入日","商品分類タグ","作成日","在庫数","引当数","最終更新日","消費税率（%）","発注残数"\r\n' +
  '"noflyersticker","チラシ お断り","0001","75.00","380.00","取扱中","","","","100","2026-07-04 12:00:00","","2020-05-24","100","0","2026-07-04 15:00:00","10","50"\r\n' +
  '"unknown-in-ne","NEにしかない商品","0001","100.00","500.00","取扱中","","","","10","","","2026-01-01","5","0","2026-07-04","10","0"\r\n';
const fdNe = new FormData();
fdNe.append('file', new Blob([neCsv], { type: 'text/csv' }), 'nedldata.csv');
r = await j('/api/ne-overlay/csv', { method: 'POST', body: fdNe });
ok(r.status === 200 && r.body.rowCount === 2, 'NE CSV取込 2件', r.body);
r = await j('/api/supplier/1');
ok(r.body.overlay && r.body.overlay.applied === true, 'overlay 適用中フラグ');
const noflyOv = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'noflyersticker');
ok(noflyOv && noflyOv.stock === 120 && noflyOv.backOrder === 50, 'overlay: 在庫=NE100+FBA20, 注残=50', noflyOv && { stock: noflyOv.stock, back: noflyOv.backOrder });
ok(noflyOv && noflyOv.cost === 75, 'overlay: 原価も最新化 (70→75)', noflyOv && noflyOv.cost);
// L=(120+50)/368=0.46 → 発注対象のまま、推奨量再計算 = ROUND((2.5-0.4620)*368/100)*100 = 800
ok(noflyOv && noflyOv.recQty === 800, 'overlay: 推奨発注量が再計算される (800)', noflyOv && noflyOv.recQty);
// 朝同期の方が新しい場合は自動で無視
db.prepare(`UPDATE mirror_pml_published SET src_ne_products_synced_at=? WHERE id=1`).run(new Date(Date.now() + 3600000).toISOString());
r = await j('/api/supplier/1');
ok(r.body.overlay && r.body.overlay.applied === false, '朝同期が新しければ overlay 無視');
db.prepare(`UPDATE mirror_pml_published SET src_ne_products_synced_at=NULL WHERE id=1`).run();
// 解除
r = await j('/api/ne-overlay', { method: 'DELETE' });
ok(r.status === 200, 'overlay 解除');
r = await j('/api/supplier/1');
ok(r.body.overlay === null, '解除後 overlay なし');
const noflyBack = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'noflyersticker');
ok(noflyBack && noflyBack.backOrder === 0, '解除後は PML の値に戻る', noflyBack && noflyBack.backOrder);
// 見出し不正CSV
const badNe = new FormData();
badNe.append('file', new Blob(['foo,bar\r\n1,2\r\n']), 'bad.csv');
r = await j('/api/ne-overlay/csv', { method: 'POST', body: badNe });
ok(r.status === 400, 'NE CSV見出し不正は 400');
// 在庫数が数値でない行があれば全件rollback
const badNumNe = new FormData();
badNumNe.append('file', new Blob(['"商品コード","在庫数","発注残数"\r\n"a1","10","0"\r\n"a2","abc","0"\r\n']), 'badnum.csv');
r = await j('/api/ne-overlay/csv', { method: 'POST', body: badNumNe });
ok(r.status === 400 && r.body.errors && r.body.errors.length === 1, 'NE CSV数値不正行は全件rollback + errors', r.body);
r = await j('/api/supplier/1');
ok(r.body.overlay === null, '不正CSVは一切反映されない');
// カンマ区切り数値は許容
const commaNe = new FormData();
commaNe.append('file', new Blob(['"商品コード","在庫数","発注残数"\r\n"noflyersticker","1,234","0"\r\n']), 'comma.csv');
r = await j('/api/ne-overlay/csv', { method: 'POST', body: commaNe });
ok(r.status === 200, 'カンマ入り数値は許容');
r = await j('/api/supplier/1');
const noflyComma = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'noflyersticker');
// NE1,234 + FBA20 (この時点ではまだFBA=20のまま)
ok(noflyComma && noflyComma.stock === 1254, 'カンマ除去して数値化 (1,234→1234 +FBA20)', noflyComma && noflyComma.stock);
await j('/api/ne-overlay', { method: 'DELETE' });
db.prepare(`UPDATE mirror_pml_snapshot_rows SET FBA在庫数=NULL WHERE 商品コード='noflyersticker'`).run();

console.log('── 生スプシCSV 自動判別取込 + 文字コード ──');
// 生の「発注条件マスタ」スプレッドシートDL形式 (列名が仕入先/商品グループID管理名 等)。商品名に壊れ文字 � を混ぜる
const rawShohin = '商品コード,商品名,確認FLG,仕入先,仕入れ先名,商品グループID,商品グループID管理名,原料グループID,原料グループID管理名,容量/個,取扱区分,原価,発注ロット単位,ケースグループ,ケースロット\r\n' +
  'cardbarrierperfect100,KMCカードバリアー,1,0199,テスト�商事,KMC,KMCカードバリア,,,,取扱中,230,360,,\r\n' +
  'diyorangeoil100,木工用オレンジオイル,1,0199,テスト�商事,,,mokouorange,木工用オレンジオイル,100,取扱中,270,600,,\r\n' +
  'noflyersticker,チラシお断り,1,0199,テスト�商事,,,,,,取扱中,70,100,,\r\n';
const rawJyoken = '条件ID,仕入先,メーカー名,商品グループID管理名,条件タイプ,条件値,単位\r\n' +
  'KMC,1,KMC,KMCカードバリア,数量,360,個\r\n';
const rawGenryo = '原料グループID,原料グループ名,最低発注量,単位\r\n' +
  'mokouorange,木工用オレンジオイル,100000,ml\r\n';
const fdImp = new FormData();
fdImp.append('files', new Blob([rawShohin], { type: 'text/csv' }), '発注条件マスタ - 商品マスタ.csv');
fdImp.append('files', new Blob([rawJyoken], { type: 'text/csv' }), '発注条件マスタ - 発注条件マスタ.csv');
fdImp.append('files', new Blob([rawGenryo], { type: 'text/csv' }), '発注条件マスタ - 原料グループマスタ.csv');
r = await j('/api/import', { method: 'POST', body: fdImp });
ok(r.status === 200 && r.body.ok, '生スプシ3ファイル 一括取込 成功', r.body && r.body.summary);
ok(r.body.counts && r.body.counts.suppliers === 1 && r.body.counts.conditions === 1 && r.body.counts.materials === 1 && r.body.counts.attrs === 2,
  '商品マスタ→仕入先1+紐付け2 / 条件1 / 原料1', r.body && r.body.counts);
r = await j('/api/masters/suppliers');
const sup199 = r.body.rows.find(x => x.supplier_code === '199');
ok(sup199 && sup199.name === 'テスト�商事', '文字コード: 壊れ文字�を含む名前もそのまま保持 (Shift-JIS誤判定しない)', sup199 && sup199.name);
r = await j('/api/masters/attrs');
const cbp = r.body.rows.find(x => x.product_key === 'cardbarrierperfect100');
ok(cbp && cbp.condition_id === 'KMC', '同一バッチ内で条件を先に入れて紐付けが成立 (dangling回避)', cbp && cbp.condition_id);
// 判別できないファイルは 400
const fdBad = new FormData();
fdBad.append('files', new Blob(['foo,bar\r\n1,2\r\n']), 'nazo.csv');
r = await j('/api/import', { method: 'POST', body: fdBad });
ok(r.status === 400 && r.body.errors && r.body.errors.length === 1, '種類判別不能ファイルは 400 + errors', r.body);

// Shift_JIS 実バイト列 (丸数字①/半角ｶﾅ/波ダッシュ〜 を含む) の fallback デコード
const sjisGenryo = '原料グループID,原料グループ名,最低発注量,単位\r\n' +
  'sjistest,①ﾃｽﾄ原料ｰA,500,g\r\n';
const fdSjis = new FormData();
fdSjis.append('files', new Blob([iconv.encode(sjisGenryo, 'Shift_JIS')], { type: 'text/csv' }), '発注条件マスタ - 原料グループマスタ.csv');
r = await j('/api/import', { method: 'POST', body: fdSjis });
ok(r.status === 200 && r.body.ok, 'Shift_JIS 実バイトCSV 取込成功', r.body && r.body.summary);
r = await j('/api/masters/materials');
const sjisRow = r.body.rows.find(x => x.group_id === 'sjistest');
ok(sjisRow && sjisRow.name === '①ﾃｽﾄ原料ｰA', 'Shift_JIS: 丸数字/半角カナを正しくデコード', sjisRow && sjisRow.name);

// 数値不正・dangling は「スキップ+warnings」で取込は続行 (rollbackしない)
const fdWarn = new FormData();
fdWarn.append('files', new Blob(['条件ID,仕入先,条件タイプ,条件値,単位\r\nWARNCOND,1,数量,36O,個\r\n'], { type: 'text/csv' }), '発注条件マスタ - 発注条件マスタ.csv');
fdWarn.append('files', new Blob(['商品コード,商品名,仕入先,商品グループID,原料グループID\r\ndanglingprod,ダングリング,1,,nonexistgroup\r\n'], { type: 'text/csv' }), '発注条件マスタ - 商品マスタ.csv');
r = await j('/api/import', { method: 'POST', body: fdWarn });
ok(r.status === 200 && r.body.ok, '数値不正/dangling でも200で続行', r.body && r.body.summary);
ok(r.body.skipped && r.body.skipped.conditions === 1, '条件値36O は数値不正でスキップ', r.body && r.body.skipped);
ok(r.body.warnings && r.body.warnings.some(w => w.includes('36O')), '不正数値がwarningsに記録される', r.body && r.body.warnings);
ok(r.body.warnings && r.body.warnings.some(w => w.includes('原料グループ')), 'dangling原料グループ参照がwarningsに記録される', r.body && r.body.warnings);

// NE商品マスタCSV (商品コード+仕入先だけ、発注特有列なし) は shohin と誤判定しない
const fdNeMaster = new FormData();
fdNeMaster.append('files', new Blob(['商品コード,仕入先コード,在庫数,発注残数\r\nx1,1,10,0\r\n'], { type: 'text/csv' }), 'ネクストエンジン商品マスタ.csv');
r = await j('/api/import', { method: 'POST', body: fdNeMaster });
ok(r.status === 400, 'NE商品マスタCSVは /api/import で誤判定しない (400)', r.body);

// 負の最低発注量はDB CHECKで500にせず、null化+警告で取込続行 (CRUD挙動と整合)
const fdNeg = new FormData();
fdNeg.append('files', new Blob(['原料グループID,原料グループ名,最低発注量,単位\r\nneggrp,負テスト,-5,個\r\n'], { type: 'text/csv' }), '発注条件マスタ - 原料グループマスタ.csv');
r = await j('/api/import', { method: 'POST', body: fdNeg });
ok(r.status === 200 && r.body.counts.materials === 1, '負の最低発注量でも500にならず取込続行', r.body);
ok(r.body.warnings && r.body.warnings.some(w => w.includes('最低発注量')), '負の最低発注量がwarningsに記録', r.body && r.body.warnings);
r = await j('/api/masters/materials');
const negRow = r.body.rows.find(x => x.group_id === 'neggrp');
ok(negRow && negRow.min_order_qty == null, '負値はnull化されて格納', negRow && negRow.min_order_qty);

// dangling (未登録グループ参照) は unlinked API の dangling で可視化される
r = await j('/api/attrs/unlinked?days=0');
ok(r.body.ok && Array.isArray(r.body.dangling), 'unlinked APIに dangling リスト', r.body && r.body.danglingCount);
ok(r.body.dangling.some(x => x.missMat === 'nonexistgroup'), 'dangling: danglingprod の未登録原料グループが可視化', r.body && r.body.dangling);

console.log('── 未紐付け 新商品フィルタ ──');
// 登録日が古い商品は既定(60日)では出ない、days=0で全件
db.prepare(`UPDATE mirror_pml_snapshot_rows SET 登録日='2020-01-01' WHERE 商品コード='0726-001060'`).run();
r = await j('/api/attrs/unlinked?days=60');
ok(r.body.ok && r.body.rows.every(x => x.code !== '0726-001060'), '古い商品は直近60日フィルタで除外', r.body && r.body.count);
r = await j('/api/attrs/unlinked?days=0');
ok(r.body.ok && typeof r.body.totalUnlinked === 'number', 'days=0 で全件モード');

console.log('── 上限条件 (出荷制限) ──');
{
  const cap = { condition_type: '上限', condition_value: 300, unit: '枚' };
  const keys = new Set(['biwako-ki']);
  let ev = evaluateCondition(cap, keys, [{ key: 'biwako-ki', qty: 300, cost: 100 }]);
  ok(ev.auto && ev.auto.kind === 'cap' && ev.auto.met === true, '上限: ちょうど300はOK', ev.auto);
  ev = evaluateCondition(cap, keys, [{ key: 'biwako-ki', qty: 360, cost: 100 }]);
  ok(ev.auto && ev.auto.met === false, '上限: 360は超過でNG', ev.auto);
}

console.log('── 商品別モール販売内訳 API ──');
{
  const now = new Date().toISOString();
  const today = now.slice(0, 10);
  // 速報 (NE受注ベース)
  db.prepare(`INSERT INTO mirror_f_sales_velocity_by_product_mall (商品コード, mall, qty_7d, qty_30d, as_of_date, synced_at) VALUES (?,?,?,?,?,?)`)
    .run('noflyersticker', 'rakuten', 5, 20, today, now);
  db.prepare(`INSERT INTO mirror_f_sales_velocity_by_product_mall (商品コード, mall, qty_7d, qty_30d, as_of_date, synced_at) VALUES (?,?,?,?,?,?)`)
    .run('noflyersticker', 'amazon_fba', 3, 10, today, now);
  // 確定: 楽天 直接 (ne_code=自身) 2個 + セット経由 (3個セット×2件=6個)
  db.prepare(`INSERT INTO mirror_set_components (セット商品コード, 構成商品コード, 数量, updated_at) VALUES ('noflyset3', 'noflyersticker', 3, ?)`).run(now);
  // NOT NULL/CHECK制約の多いミラーfact表に最小fixtureを入れる汎用insert
  const insFact = (table, want) => {
    const cols = db.prepare(`SELECT * FROM pragma_table_info('${table}') WHERE "notnull"=1 AND dflt_value IS NULL AND pk=0`).all().map(c => c.name);
    const vals = { ...want };
    for (const c of cols) if (!(c in vals)) {
      if (c.includes('quality')) vals[c] = 'actual';
      else if (c === 'cost_status') vals[c] = 'complete';
      else if (c === 'sku_resolution') vals[c] = 'resolved';
      else if (c.includes('_at') || c.includes('date')) vals[c] = now;
      else vals[c] = 0;
    }
    const names = Object.keys(vals);
    db.prepare(`INSERT INTO ${table} (${names.join(',')}) VALUES (${names.map(n => '@' + n).join(',')})`).run(vals);
  };
  insFact('mirror_rakuten_finance_sku_daily', { date_jst: today, rakuten_code: 'rk-nofly', ne_code: 'noflyersticker', sku_resolution: 'resolved', units_net_sold: 2 });
  insFact('mirror_rakuten_finance_sku_daily', { date_jst: today, rakuten_code: 'rk-noflyset', ne_code: 'noflyset3', sku_resolution: 'resolved', units_net_sold: 2 });
  // 確定: Amazon (sku_resolved経由)。同一SKUでFBA日とFBM日が混在するケース
  db.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, synced_at) VALUES ('AMZ-NOFLY', 'noflyersticker', 1, 'master', ?)`).run(now);
  const yesterday = new Date(Date.parse(today + 'T00:00:00Z') - 86400000).toISOString().slice(0, 10);
  insFact('mirror_amazon_finance_sku_daily', { date_jst: today, seller_sku: 'AMZ-NOFLY', units_net_sold: 4, fba_fulfillment_jpy: 500 });
  insFact('mirror_amazon_finance_sku_daily', { date_jst: yesterday, seller_sku: 'AMZ-NOFLY', units_net_sold: 3, fba_fulfillment_jpy: 0 });

  r = await j('/api/products/noflyersticker/mall-sales?days=90');
  ok(r.status === 200 && r.body.ok, 'mall-sales API 200', r.body);
  ok(r.body.sokuho.rows.length === 2 && r.body.sokuho.rows[0].qty30 === 20, '速報: モール別 7/30日', r.body.sokuho.rows);
  const fin = {};
  r.body.finance.rows.forEach(x => { fin[x.mall] = x.pieces; });
  ok(fin.rakuten === 8, '確定: 楽天 直接2 + セット3×2=8ピース', fin);
  ok(fin.amazon_fba === 4 && fin.amazon_fbm === 3, '確定: Amazon FBA4/FBM3 (同一SKU混在を行単位で判定)', fin);
  r = await j('/api/products/noflyersticker/mall-sales?days=9999');
  ok(r.status === 200 && r.body.finance.days === 90, '不正daysは90にフォールバック', r.body.finance && r.body.finance.days);
  r = await j('/api/products/zzz-nonexistent/mall-sales');
  ok(r.status === 200 && r.body.sokuho.rows.length === 0, '存在しない商品コードは空で200');
}

console.log('── 選べるセット構成 / 月次上限 / bind ──');
{
  // 今月発注済み数量 (issue済み noflyersticker 400) が issuedMonth に載る
  r = await j('/api/supplier/1');
  ok(r.body.issuedMonth && r.body.issuedMonth['noflyersticker'] >= 400, '上限用: 今月確定済み数量 issuedMonth', r.body.issuedMonth);
  const nfCond = r.body.conditions.find(c => c.memberCodes.includes('noflyersticker'));
  ok(nfCond && r.body.issuedByCond && r.body.issuedByCond[nfCond.conditionId] >= 400, '上限用: 条件別スナップショット issuedByCond', r.body.issuedByCond);
  ok(r.body.issuedTotal >= 10400, '上限用: 仕入先全体 issuedTotal', r.body.issuedTotal);
  ok(r.body.allGroups && r.body.allGroups.conditions.length > 0 && r.body.allGroups.materials.length > 0, 'グループ紐付けUI用 allGroups');

  // 選べるセット構成: deaditem (掘り起こし=在庫50) を最低在庫60で登録 → 在庫が下回るので要発注に昇格 + selectableLow
  r = await j('/api/masters/selectable', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'deaditem', set_names: '選べる5種セット', min_stock: 60 }) });
  ok(r.status === 200 && r.body.ok, '選べるセット構成 登録', r.body);
  r = await j('/api/supplier/1');
  const dead = r.body.targets.find(p => p.code === 'deaditem');
  ok(dead && dead.selectableLow && dead.selectableLow.sets === '選べる5種セット', '在庫減の構成商品が要発注に昇格 + selectableLow', dead && dead.selectableLow);
  ok(!r.body.horikoshi.some(p => p.code === 'deaditem'), '昇格後は掘り起こしから消える');
  ok(dead && dead.recQty > 0, '販売0でも推奨発注が入る (最低在庫×2まで補充をロット切上げ)', dead && dead.recQty);

  // bind API: 条件だけ更新しても既存の原料/容量は保持される
  r = await j('/api/attrs/bind', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'diyorangeoil100', condition_id: 'KMC' }) });
  ok(r.status === 200 && r.body.ok, 'bind: 条件のみ更新 200', r.body);
  r = await j('/api/masters/attrs');
  const oil = r.body.rows.find(x => x.product_key === 'diyorangeoil100');
  ok(oil && oil.condition_id === 'KMC' && oil.material_group_id === 'mokouorange' && oil.capacity_per_unit === 100, 'bind: 原料/容量を保持したまま条件だけ変わる', oil);
  r = await j('/api/attrs/bind', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'diyorangeoil100', condition_id: '', material_group_id: 'mokouorange' }) });
  ok(r.status === 200, 'bind: 条件解除もできる');
  r = await j('/api/attrs/bind', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'diyorangeoil100', condition_id: 'NOEXIST' }) });
  ok(r.status === 400, 'bind: 未登録グループは400 (fail-closed)');
  r = await j('/api/attrs/bind', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ product_code: 'ghost-product-xyz', condition_id: 'KMC' }) });
  ok(r.status === 400 && r.body.error.includes('PML'), 'bind: PML非存在の商品コードは400 (Codex P9 High)');

  // マスタAPIのユーザビリティ: attrs/selectableに商品名、conditionsに仕入先名が付く
  r = await j('/api/masters/attrs');
  const oilNamed = r.body.rows.find(x => x.product_key === 'diyorangeoil100');
  ok(oilNamed && oilNamed.商品名 && oilNamed.商品名.length > 0, 'masters/attrs に商品名が付与される', oilNamed && oilNamed.商品名);
  r = await j('/api/masters/conditions');
  ok(r.body.rows.some(x => x.仕入先名 === 'アメージングクラフト様'), 'masters/conditions に仕入先名が付与される (placeholder上書きバグの回帰込み)');
  r = await j('/api/masters/selectable');
  ok(r.body.rows.every(x => '商品名' in x), 'masters/selectable に商品名列');

  // 選べるセットCSV取込 (自動判別、同一商品の複数セットはセット名を束ねる)
  const fdSel = new FormData();
  fdSel.append('files', new Blob(['商品コード,セット名,最低在庫数\r\ndeaditem,選べる5種セット,5\r\ndeaditem,選べる7種セット,8\r\n'], { type: 'text/csv' }), '選べるセット.csv');
  r = await j('/api/import', { method: 'POST', body: fdSel });
  ok(r.status === 200 && r.body.counts.selectable === 1, '選べるセットCSV自動判別取込 (2行→1商品にマージ)', r.body && r.body.counts);
  r = await j('/api/masters/selectable');
  const deadSel = r.body.rows.find(x => x.product_key === 'deaditem');
  ok(deadSel && deadSel.set_names.includes('5種') && deadSel.set_names.includes('7種') && deadSel.min_stock === 8, 'セット名マージ+最低在庫はmax', deadSel);
}

console.log('── 画面 (HTML) ──');
for (const p of ['/', '/supplier/1', '/products', '/orders', '/admin']) {
  const res = await fetch(base + p);
  const html = await res.text();
  ok(res.status === 200 && html.includes('<!DOCTYPE html>'), `GET ${p} → 200 HTML`);
  ok(!html.includes('undefined') || p === '/', `GET ${p} に undefined 露出なし`);
}
// 全商品情報: PML全行が埋め込まれる (取扱中止含む) + 仕入先名map + モール別内訳UI
{
  const res = await fetch(base + '/products');
  const html = await res.text();
  ok(html.includes('noflyersticker') && html.includes('全商品情報'), '/products にPML商品が埋め込まれる');
  ok(html.includes('"t":1') || html.includes('"h":1'), '/products に要発注/掘り起こしフラグ');
  ok(html.includes('mall-sales') && html.includes('mallBoxHtml') && html.includes('msDays'), '/products 商品名クリック→モール別販売内訳のJSを配信');
}
// 仕入先ページ: グループ化アコーディオン+下部固定バー+シミュレーション+検索 (カート/独立条件セクション廃止)
{
  const res = await fetch(base + '/supplier/1');
  const html = await res.text();
  ok(html.includes('renderTargets') && html.includes('accHtml') && html.includes('condCheck'), '/supplier グループ化+アコーディオン+条件チェックのJSを配信');
  ok(html.includes('renderBar') && html.includes('fbar') && !html.includes('cartArea'), '/supplier カート廃止→下部固定バー');
  ok(html.includes('needQty') && html.includes('updateSim'), '/supplier ◯ヶ月分シミュレーション');
  ok(html.includes('pageQ') && html.includes('applySearch'), '/supplier ページ内商品検索 (debounce再描画方式)');
  ok(html.includes('data-dis') && html.includes('data-undis'), '/supplier 要発注の✕非表示+戻す');
  ok(html.includes('bindSave') && html.includes('bindCond'), '/supplier アコーディオンからグループ紐付け');
  ok(html.includes('gaddQ') && html.includes('data-gadd'), '/supplier グループへの商品追加検索');
  ok(html.includes('needAll') && html.includes('(この商品)'), '/supplier 必要数一括コピー+同グループ表に自分自身');
  ok(html.includes('accApply') && html.includes('＋追加'), '/supplier 要発注リストに反映して閉じる+追加行バッジ');
  ok(html.includes('data-copy=') && html.includes('data-copyq='), '/supplier 商品コード/発注数のクリックコピー');
  ok(html.includes('data-gdis=') && html.includes('グループごと非表示'), '/supplier グループ一括✕');
  ok(html.includes('1換算'), '/supplier 容量未設定=1換算でゲージが動く');
  ok(html.includes('issuedMonthFor'), '/supplier 上限=月次累計 (今月確定分込み)');
  ok(html.includes('max-height: 74vh; overflow: auto'), '見出し追随: .bd自体をスクロール領域化 (確実に効く方式)');
  ok(html.includes('position: sticky; top: 0;'), '見出しは.bd上端に貼り付く');
  ok(html.includes('追加発注候補') && !html.includes('ついで買い'), '/supplier 「ついで買い」→「追加発注候補」に改名');
  ok(!html.includes('condArea'), '/supplier 独立した発注条件セクションは廃止済み');
  ok(html.includes('未達の発注条件'), '/supplier 確定前の条件未達警告');
  ok(html.includes('dlCsv') && html.includes('btnCsv'), '/supplier 確定リストCSVダウンロード');
  ok(html.includes('<a href="/" class="back sp">'), 'ポータルに戻るリンクは / (修正済)');
}
// ダッシュボード: 仕入先カードの✕非表示+戻す
{
  const res = await fetch(base + '/');
  const html = await res.text();
  ok(html.includes('data-cdis=') && html.includes('data-cundis'), '/ 仕入先カードの✕非表示+戻す');
}
r = await j('/api/attrs/unlinked?days=0');
ok(r.body.ok && r.body.rows.every(x => x.code.toLowerCase() !== 'diyorangeoil100'), 'unlinked: 紐付け済みは出ない');
ok(r.body.rows.some(x => x.code === '0726-001060'), 'unlinked: 未紐付け取扱中は出る (全件)');

// ═══ P13a 発注ライフサイクル台帳 ═══
console.log('── P13a: 台帳 (イベント/逆仕訳/クローズ/整合性) ──');
{
  const L = await imp('apps/purchase-orders/ledger.js');
  const boundary = L.getSetting('tracking_started_at');
  ok(!!boundary, '移行境界 tracking_started_at が初回issueで確定');

  // 発注確定: PO番号+tracked+希望納期スナップショット
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 50 }], note: '台帳テスト', requestedDate: '2026-07-20' }) });
  ok(r.status === 200 && r.body.ok && /^PO-\d{4}-\d{4}$/.test(r.body.poNumber), 'issue: PO番号採番 (PO-YYYY-NNNN)', r.body.poNumber);
  const ledgerOrderId = r.body.id;
  const ord0 = db.prepare('SELECT * FROM po_orders WHERE id=?').get(ledgerOrderId);
  ok(ord0.tracking_mode === 'tracked' && ord0.requested_date === '2026-07-20' && ord0.closed_at == null, 'issue: tracked+希望納期+オープン');
  const item = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(ledgerOrderId);
  ok(item.requested_date === '2026-07-20', 'issue: 希望納期を明細へスナップショット');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1 }], requestedDate: '2026/07/20' }) });
  ok(r.status === 400, 'issue: 希望納期の形式不正は 400');

  // 部分入荷 40/50 → 残10
  const rc = L.appendPoItemEvent({ orderItemId: item.id, eventType: 'receipt', qty: 40, source: 'manual', actor: 'smoke' });
  ok(rc.remaining === 10 && !rc.orderClosed, '入荷40: 残10・オープン維持', rc);
  // 残数超過は拒否
  let threw = null;
  try { L.appendPoItemEvent({ orderItemId: item.id, eventType: 'receipt', qty: 11, source: 'manual' }); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('残数超過'), '残数超過の入荷は拒否', threw);
  // 減数10 (50個発注→40個しか作れない) → 残0で自動クローズ、close_reason=completed (cutoffなし)
  const sh = L.appendPoItemEvent({ orderItemId: item.id, eventType: 'shortage', qty: 10, reasonCode: 'supplier_shortage', note: '原料不足', actor: 'smoke' });
  ok(sh.remaining === 0 && sh.orderClosed, '減数10: 残0→自動クローズ', sh);
  ok(L.deriveCloseReason(ledgerOrderId) === 'completed', 'close_reason導出=completed (通常消込)');
  // クローズ後の通常イベントは拒否、逆仕訳のみ可
  threw = null;
  try { L.appendPoItemEvent({ orderItemId: item.id, eventType: 'receipt', qty: 1, source: 'manual' }); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('完了済み'), 'クローズ後の通常イベントは拒否');
  // 入荷40を逆仕訳 → 残40で自動再オープン
  const rv = L.reverseEvent(rc.eventId, { note: '数量誤登録の訂正', actor: 'smoke' });
  ok(rv.remaining === 40 && !rv.orderClosed, '逆仕訳: 残40で自動再オープン', rv);
  ok(db.prepare('SELECT closed_at FROM po_orders WHERE id=?').get(ledgerOrderId).closed_at == null, '再オープンで closed_at クリア');
  // 二重逆仕訳は拒否 (UNIQUE + txn検証)
  threw = null;
  try { L.reverseEvent(rc.eventId, { note: '二重' }); } catch (e) { threw = e.message; }
  ok(!!threw, '同一イベントの二重逆仕訳は拒否', threw);
  // 正しい入荷35を再登録 → 残5 (減数10は有効のまま)
  const rc2 = L.appendPoItemEvent({ orderItemId: item.id, eventType: 'receipt', qty: 35, source: 'manual', effectiveDate: '2026-07-11' });
  ok(rc2.remaining === 5, '再入荷35: 残5 (減数10は維持)', rc2);
  const bal = L.balanceOf(item.id);
  ok(bal.received_qty === 35 && bal.shortage_qty === 10 && bal.ordered_qty === 50, '残数内訳 (受入35/減数10/発注50)', bal);

  // disposition三択: 分納待ち (次回予定)
  L.setItemPlan(item.id, { remainder_disposition: 'awaiting_delivery', next_expected_date: '2026-07-25', next_expected_qty: 5 }, { actor: 'smoke' });
  let it2 = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(item.id);
  ok(it2.remainder_disposition === 'awaiting_delivery' && it2.next_expected_qty === 5, '分納待ち: 次回予定を保存');
  threw = null;
  try { L.setItemPlan(item.id, { remainder_disposition: 'awaiting_delivery', next_expected_date: '2026-07-25', next_expected_qty: 6 }); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('残数'), '次回予定数量>残数は拒否');
  // 確認中へ変更 → next_expected_* は自動クリア、履歴が change_id で束なる
  L.setItemPlan(item.id, { remainder_disposition: 'awaiting_confirmation', next_action_date: '2026-07-18' }, { actor: 'smoke' });
  it2 = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(item.id);
  ok(it2.remainder_disposition === 'awaiting_confirmation' && it2.next_expected_date == null && it2.next_expected_qty == null, '確認中: next_expected_*クリア');
  const histChanges = db.prepare('SELECT COUNT(DISTINCT change_id) AS c, COUNT(*) AS n FROM po_item_history WHERE order_item_id=?').get(item.id);
  ok(histChanges.n >= 4 && histChanges.c >= 2, '納期/disposition変更は履歴に記録 (change_id束ね)', histChanges);
  // 最後の入荷5 → 残0 → disposition自動クリア+クローズ
  const rc3 = L.appendPoItemEvent({ orderItemId: item.id, eventType: 'receipt', qty: 5, source: 'manual' });
  it2 = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(item.id);
  ok(rc3.orderClosed && it2.remainder_disposition == null && it2.next_action_date == null, '残0: disposition自動クリア+クローズ', rc3);

  // 手動クローズ: 入荷30→打切 → close_reason=manual、cutoffは逆仕訳後も維持
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'cardstand-silver-r', qty: 100 }] }) });
  const mcOrderId = r.body.id;
  const mcItem = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(mcOrderId);
  const mcRc = L.appendPoItemEvent({ orderItemId: mcItem.id, eventType: 'receipt', qty: 30, source: 'manual' });
  threw = null;
  try { L.manualCloseOrder(mcOrderId, {}); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('note'), '手動クローズは理由必須');
  const mc = L.manualCloseOrder(mcOrderId, { note: '仕入先廃番のため打切', actor: 'smoke' });
  ok(mc.closed && L.deriveCloseReason(mcOrderId) === 'manual', '手動クローズ: cutoff登録→close_reason=manual', mc);
  const mcRv = L.reverseEvent(mcRc.eventId, { note: '入荷誤登録' });
  const mcBal = L.balanceOf(mcItem.id);
  ok(mcRv.remaining === 30 && mcBal.cutoff_qty === 70, '手動クローズ後の入荷逆仕訳: 再オープン+cutoff70維持', mcBal);

  // draft へのイベントは拒否
  r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 10 }] }) });
  const dItem = db.prepare('SELECT i.* FROM po_order_items i JOIN po_orders o ON o.id=i.order_id WHERE o.status=?').get('draft');
  threw = null;
  try { L.appendPoItemEvent({ orderItemId: dItem.id, eventType: 'receipt', qty: 1, source: 'manual' }); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('draft'), 'draft明細へのイベントは拒否');

  // legacy (境界以前のissued) へのイベントは拒否。fixture は draft で明細を作ってから issued へ (発行済み明細追加はトリガが拒否するため)
  db.prepare(`INSERT INTO po_orders (supplier_code, supplier_name, status, created_at, updated_at)
              VALUES ('999','旧発注','draft','2020-01-01T00:00:00.000Z','2020-01-01T00:00:00.000Z')`).run();
  const legacyId = db.prepare("SELECT id FROM po_orders WHERE supplier_code='999'").get().id;
  db.prepare("INSERT INTO po_order_items (order_id, product_code, product_key, product_name, qty) VALUES (?,?,?,?,?)")
    .run(legacyId, 'noflyersticker', 'noflyersticker', '旧明細', 10);
  db.prepare("UPDATE po_orders SET status='issued', issued_at='2020-01-01T00:00:00.000Z', po_number='PO-2020-9999', tracking_mode='tracked' WHERE id=?").run(legacyId);
  const legacyItem = db.prepare('SELECT id FROM po_order_items WHERE order_id=?').get(legacyId);
  threw = null;
  try { L.appendPoItemEvent({ orderItemId: legacyItem.id, eventType: 'receipt', qty: 1, source: 'manual' }); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('legacy'), 'legacy発注へのイベントは拒否 (境界時刻が正)');

  // 発行の世代ゲート: 境界後は po_number 等の無い issued を物理拒否 (旧コード相当のINSERT)
  threw = null;
  try {
    db.prepare(`INSERT INTO po_orders (supplier_code, supplier_name, status, created_at, updated_at, issued_at)
                VALUES ('1','旧経路','issued',?,?,?)`).run(nowIsoStr(), nowIsoStr(), nowIsoStr());
  } catch (e) { threw = e.message; }
  ok(threw && threw.includes('issue gate'), '発行ゲート: 旧経路の不完全発行をトリガが拒否');

  // append-only 強制
  threw = null;
  try { db.prepare('UPDATE po_item_events SET qty=999 WHERE id=?').run(rc.eventId); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('append-only'), 'イベント台帳のUPDATEはトリガが拒否');
  threw = null;
  try { db.prepare('DELETE FROM po_item_events WHERE id=?').run(rc.eventId); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('append-only'), 'イベント台帳のDELETEはトリガが拒否');

  // 発行済みPO/明細の不変性トリガ (直接SQLからも守る)
  threw = null;
  try { db.prepare('UPDATE po_order_items SET qty=999 WHERE id=?').run(mcItem.id); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('immutable'), '発行済み明細の数量UPDATEはトリガが拒否', threw);
  threw = null;
  try { db.prepare("UPDATE po_orders SET issued_at='2020-01-01T00:00:00.000Z' WHERE id=?").run(mcOrderId); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('immutable'), '発行済みPOの issued_at 変更はトリガが拒否 (tracked判定の改変防止)');
  threw = null;
  try { db.prepare('DELETE FROM po_order_items WHERE id=?').run(mcItem.id); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('immutable'), '発行済み明細のDELETEはトリガが拒否');

  // 移行境界の不変性 (DBトリガ+setSettingホワイトリスト)
  threw = null;
  try { db.prepare("UPDATE po_settings SET value='2030-01-01T00:00:00.000Z' WHERE key='tracking_started_at'").run(); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('immutable'), '移行境界のUPDATEはトリガが拒否');
  threw = null;
  try { L.setSetting('tracking_started_at', 'x'); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('設定キー'), 'setSetting は許可キーのみ (境界は変更不可)');

  // 直接SQLのイベント登録もトリガが検証 (legacy/残数超過)
  threw = null;
  try {
    db.prepare(`INSERT INTO po_item_events (order_item_id, event_type, qty, source, effective_date, recorded_at, actor_type)
                VALUES (?,?,?,?,?,?,?)`).run(legacyItem.id, 'receipt', 1, 'manual', '2026-07-11', nowIsoStr(), 'user');
  } catch (e) { threw = e.message; }
  ok(threw && threw.includes('event scope'), '直接SQLでもlegacyへのイベントはトリガが拒否');
  threw = null;
  try {
    db.prepare(`INSERT INTO po_item_events (order_item_id, event_type, qty, source, effective_date, recorded_at, actor_type)
                VALUES (?,?,?,?,?,?,?)`).run(mcItem.id, 'receipt', 9999, 'manual', '2026-07-11', nowIsoStr(), 'user');
  } catch (e) { threw = e.message; }
  ok(threw && threw.includes('残数超過'), '直接SQLでも残数超過はトリガが拒否');

  // 冪等キー: 同一キー再送は同じ結果、異なる内容は409、キー順・明細順の違いは同一視
  const idemBody = { items: [{ code: 'noflyersticker', qty: 3 }], note: '冪等テスト' };
  const idemOpts = key => ({ method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(idemBody) });
  r = await j('/api/supplier/1/issue', idemOpts('smoke-idem-1'));
  const idemFirst = r.body;
  r = await j('/api/supplier/1/issue', idemOpts('smoke-idem-1'));
  ok(r.status === 200 && r.body.id === idemFirst.id && r.body.replay === true, '冪等キー: 再送は同じ発注を返す (二重作成なし)', r.body);
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'smoke-idem-1' },
    body: JSON.stringify({ note: '冪等テスト', items: [{ qty: 3, code: 'noflyersticker' }] }) });
  ok(r.status === 200 && r.body.id === idemFirst.id && r.body.replay === true, '冪等キー: JSONキー順が違っても同一内容はreplay', r.body);
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'smoke-idem-1' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 4 }] }) });
  ok(r.status === 409, '冪等キー: 同一キー+異なる内容は409', r.status);

  // 発行直後 (イベント登録前) の明細追加も無条件拒否 / unit_cost も不変
  const idemItem = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(idemFirst.id);
  threw = null;
  try {
    db.prepare("INSERT INTO po_order_items (order_id, product_code, product_key, product_name, qty) VALUES (?,?,?,?,?)")
      .run(idemFirst.id, 'cardstand-silver-r', 'cardstand-silver-r', '後付け', 1);
  } catch (e) { threw = e.message; }
  ok(threw && threw.includes('immutable'), '発行済みPOへの明細追加は無条件拒否 (イベント前でも)', threw);
  threw = null;
  try { db.prepare('UPDATE po_order_items SET unit_cost=1 WHERE id=?').run(idemItem.id); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('immutable'), '発行済み明細の unit_cost 変更も拒否');

  // 直接SQLの消込でも closed_at がDBトリガで再計算される (正本の乖離なし)
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 2 }] }) });
  const sqlOrderId = r.body.id;
  const sqlItem = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(sqlOrderId);
  db.prepare(`INSERT INTO po_item_events (order_item_id, event_type, qty, source, effective_date, recorded_at, actor_type)
              VALUES (?,?,?,?,?,?,?)`).run(sqlItem.id, 'receipt', 2, 'manual', '2026-07-11', nowIsoStr(), 'user');
  ok(db.prepare('SELECT closed_at FROM po_orders WHERE id=?').get(sqlOrderId).closed_at != null, '直接SQL消込でも closed_at をトリガが再計算');
  // closed_at の直接操作ガード
  threw = null;
  try { db.prepare('UPDATE po_orders SET closed_at=NULL WHERE id=?').run(sqlOrderId); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('closed_at guard'), '全消込済みPOの closed_at 直接クリアは拒否');
  threw = null;
  try { db.prepare('UPDATE po_orders SET closed_at=? WHERE id=?').run(nowIsoStr(), mcOrderId); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('closed_at guard'), '残数があるPOの closed_at 直接セットは拒否');

  // 発行ゲートの値形式検査 (PO-2026-abc 等を弾く)
  threw = null;
  try {
    db.prepare(`INSERT INTO po_orders (supplier_code, supplier_name, status, created_at, updated_at, issued_at, po_number, tracking_mode)
                VALUES ('1','形式不正','issued',?,?,?,'PO-2026-abc','tracked')`).run(nowIsoStr(), nowIsoStr(), nowIsoStr());
  } catch (e) { threw = e.message; }
  ok(threw && threw.includes('issue gate'), '発行ゲート: PO番号の形式不正 (PO-2026-abc) を拒否');

  // disposition列間規則のDBトリガ (直接SQLでも規則違反を保存できない。UPDATE/INSERT両方)
  threw = null;
  try { db.prepare('UPDATE po_order_items SET next_expected_qty=5 WHERE id=?').run(mcItem.id); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('plan rules'), '直接SQLでも disposition列間規則をトリガが拒否');
  db.prepare("INSERT INTO po_orders (supplier_code, supplier_name, status, created_at, updated_at) VALUES ('998','plan試験','draft',?,?)")
    .run(nowIsoStr(), nowIsoStr());
  const planOrderId = db.prepare("SELECT id FROM po_orders WHERE supplier_code='998'").get().id;
  threw = null;
  try {
    db.prepare("INSERT INTO po_order_items (order_id, product_code, product_key, product_name, qty, next_expected_qty) VALUES (?,?,?,?,?,?)")
      .run(planOrderId, 'dummy-plan', 'dummy-plan', 'plan違反', 1, 5);
  } catch (e) { threw = e.message; }
  ok(threw && threw.includes('plan rules'), 'INSERT時も disposition列間規則をトリガが拒否', threw);

  // NULLを素通しするCHECKの封鎖 (shortage理由NULL / reversal理由NULL)
  threw = null;
  try {
    db.prepare(`INSERT INTO po_item_events (order_item_id, event_type, qty, effective_date, recorded_at, actor_type)
                VALUES (?,?,?,?,?,?)`).run(mcItem.id, 'shortage', 1, '2026-07-11', nowIsoStr(), 'user');
  } catch (e) { threw = e.message; }
  ok(!!threw, '理由コードNULLの減数は直接SQLでも拒否 (NULL素通しCHECK対策)', threw);

  // closed_at の閉鎖時刻改変 (非NULL→別の非NULL) は拒否
  threw = null;
  try { db.prepare("UPDATE po_orders SET closed_at='2026-01-01T00:00:00.000Z' WHERE id=?").run(sqlOrderId); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('closed_at guard'), '閉鎖時刻の改変 (非NULL→非NULL) は拒否');

  // 整合性検査: 違反なし。部分消込済み残の「扱い未選択」はwarning (mcItem=残30が該当)
  r = await j('/api/ledger/integrity');
  ok(r.body.ok && r.body.healthy === true, '整合性検査: 違反なし', r.body.issues);
  ok(r.body.warnings.some(w => w.kind === 'remainder_without_disposition' && w.itemId === mcItem.id), '整合性検査: 再オープン残の扱い未選択をwarning表示', r.body.warnings);
  r = await j('/api/ledger/integrity?orderId=abc');
  ok(r.status === 400, '整合性検査: orderId不正は400');

  // 監査ログが要点操作を記録している
  const auditActions = db.prepare('SELECT DISTINCT action FROM po_audit_log').all().map(x => x.action);
  ok(auditActions.includes('tracking_boundary_set') && auditActions.includes('order_closed')
     && auditActions.includes('order_reopened') && auditActions.includes('manual_close'), '監査ログ: 境界確定/クローズ/再オープン/手動クローズ', auditActions);
}

// ═══ P13b 発注残UI/API ═══
console.log('── P13b: 発注残ページ+消込API ──');
{
  // 発注確定 → 部分入荷 (三択つき) → 納期回答 → 消込完走
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 30 }], note: 'P13bテスト', requestedDate: '2026-07-20' }) });
  const boId = r.body.id;
  const boItem = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(boId);

  // 部分入荷で残数の扱い未指定は 400
  r = await j('/api/items/' + boItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 10 }) });
  ok(r.status === 400 && r.body.error.includes('残数の扱い'), '部分入荷: 三択未指定は400', r.body.error);

  // 分納待ちを選んで部分入荷
  r = await j('/api/items/' + boItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 10, effectiveDate: '2026-07-11',
      remainder: { action: 'await_delivery', nextExpectedDate: '2026-07-30', nextExpectedQty: 20 } }) });
  ok(r.status === 200 && r.body.ok && r.body.remaining === 20, '部分入荷10+分納待ち: 残20', r.body);
  let it3 = db.prepare('SELECT * FROM po_order_items WHERE id=?').get(boItem.id);
  ok(it3.remainder_disposition === 'awaiting_delivery' && it3.next_expected_date === '2026-07-30', '三択が同一txnで保存');

  // 回答納期の入力 (PATCH plan)
  r = await j('/api/items/' + boItem.id + '/plan', { method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ promisedDate: '2026-07-25' }) });
  ok(r.status === 200 && r.body.ok, '回答納期の保存 (PATCH plan)');

  // /api/backorders に反映されている (フラグ・サマリ)
  r = await j('/api/backorders');
  ok(r.body.ok && r.body.boundary, '発注残API: boundary/サマリ');
  const boOrder = r.body.orders.find(o => o.id === boId);
  ok(boOrder && boOrder.open && boOrder.remainingQty === 20 && /^PO-\d{4}-/.test(boOrder.poNumber), '発注残API: 対象POが残20でオープン', boOrder && boOrder.remainingQty);
  const boIt = boOrder.items[0];
  ok(boIt.promised_date === '2026-07-25' && boIt.due === '2026-07-25' && boIt.flags.unanswered === false, '発注残API: 回答納期が遅延判定の基準になる');
  ok(r.body.summary.openOrders >= 1 && r.body.summary.remainingQty >= 20, '発注残API: サマリ集計', r.body.summary);

  // 確認中へ変更しつつさらに部分入荷 → 残りを減数で完了
  r = await j('/api/items/' + boItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 5, remainder: { action: 'await_confirmation', nextActionDate: '2026-07-15' } }) });
  ok(r.body.ok && r.body.remaining === 15, '部分入荷5+確認中: 残15');
  r = await j('/api/items/' + boItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 10, remainder: { action: 'shortage', reasonCode: 'supplier_shortage', note: '原料不足' } }) });
  ok(r.body.ok && r.body.remaining === 0 && r.body.orderClosed, '入荷10+残り5を減数で完了→自動クローズ', r.body);

  // イベント履歴API + 逆仕訳API
  r = await j('/api/items/' + boItem.id + '/events');
  ok(r.body.ok && r.body.events.length === 4, 'イベント履歴API: 4イベント (入荷3+減数1)', r.body.events.length);
  const firstReceipt = r.body.events.find(e => e.event_type === 'receipt');
  r = await j('/api/events/' + firstReceipt.id + '/reverse', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ note: '入力ミス訂正' }) });
  ok(r.body.ok && r.body.remaining === 10 && !r.body.orderClosed, '逆仕訳API: 残10で再オープン', r.body);

  // 手動クローズAPI
  r = await j('/api/orders/' + boId + '/close', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ note: '仕入先と合意の上打切' }) });
  ok(r.body.ok && r.body.closed, '手動クローズAPI');
  r = await j('/api/backorders');
  const boOrder2 = r.body.orders.find(o => o.id === boId);
  ok(boOrder2 && !boOrder2.open && boOrder2.closeReason === 'manual', '発注残API: 手動クローズ後は完了(打切)', boOrder2 && boOrder2.closeReason);

  // 消込APIの冪等再送 (通信断→再クリックで二重登録しない)
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 8 }] }) });
  const idemEvItem = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(r.body.id);
  const evBody = JSON.stringify({ type: 'receipt', qty: 8 });
  const evOpts = { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'smoke-ev-1' }, body: evBody };
  r = await j('/api/items/' + idemEvItem.id + '/events', evOpts);
  ok(r.body.ok && r.body.remaining === 0, '消込API: 冪等キーつき入荷');
  r = await j('/api/items/' + idemEvItem.id + '/events', evOpts);
  ok(r.body.ok && r.body.replay === true && r.body.remaining === 0, '消込API: 同一キー再送はreplay (二重消込なし)', r.body);
  ok(db.prepare('SELECT COUNT(*) AS n FROM po_item_events WHERE order_item_id=?').get(idemEvItem.id).n === 1, '消込API: イベントは1件のみ');

  // 残数0の明細に扱い/予定は設定できない (stale_disposition を公開APIから作らせない)。
  // オープンな複数明細POで、片方だけ全量入荷→その明細に扱いを設定しようとするケース
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 5 }, { code: 'cardstand-silver-r', qty: 5 }] }) });
  const twoItems = db.prepare('SELECT * FROM po_order_items WHERE order_id=? ORDER BY id').all(r.body.id);
  r = await j('/api/items/' + twoItems[0].id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 5 }) });
  ok(r.body.ok && r.body.remaining === 0 && !r.body.orderClosed, '複数明細PO: 片方全量入荷でもオープン維持');
  r = await j('/api/items/' + twoItems[0].id + '/plan', { method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ disposition: 'awaiting_confirmation', nextActionDate: '2026-07-20' }) });
  ok(r.status === 400 && r.body.error.includes('残数0'), '残数0明細への扱い設定は400 (Codex P13b R2)', r.body.error);

  // 発注確定の冪等再送 (issue APIにキー: 再送は同じPO)
  const issueOpts = key => ({ method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 2 }], requestedDate: '2026-08-01' }) });
  r = await j('/api/supplier/1/issue', issueOpts('smoke-issue-idem'));
  const issFirst = r.body;
  r = await j('/api/supplier/1/issue', issueOpts('smoke-issue-idem'));
  ok(r.body.ok && r.body.id === issFirst.id && r.body.replay === true, 'issue API: 同一キー再送は同じPO (二重発注なし)');

  // 下書きにも希望納期が保存される
  r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1 }], note: 'd', requestedDate: '2026-08-15' }) });
  const draftRow = db.prepare("SELECT requested_date FROM po_orders WHERE status='draft' AND supplier_code='1'").get();
  ok(r.body.ok && draftRow && draftRow.requested_date === '2026-08-15', '下書き保存で希望納期も保持 (Codex P13b R3)');

  // ページ配信 (発注残ページ・ダッシュボードのサマリ・履歴のPO番号列)
  {
    const html = await (await fetch(base + '/backorders')).text();
    ok(html.includes('boTabs') && html.includes('data-act') && html.includes('要対応'), '/backorders ページ配信 (タブ+消込ボタン)');
    ok(html.includes('remBox') && html.includes('await_delivery'), '/backorders 部分入荷の三択UI');
    ok(html.includes('data-rev') && html.includes('data-closeui'), '/backorders 逆仕訳+手動クローズUI (promptなし)');
    ok(html.includes("td.innerHTML = ''") && html.includes('Idempotency-Key'), '/backorders パネルDOM破棄+冪等キー送信 (Codex P13b R1)');
    ok(html.includes('data-disp') && html.includes('dspGo'), '/backorders 残数の扱い設定パネル (逆仕訳後の再設定用)');
    ok(html.includes('getJson') && html.includes('再読込'), '/backorders GET失敗時の再読込UI');
    // 発注確定の冪等キー (供給ページ): ノンス+内容ハッシュ
    const sup = await (await fetch(base + '/supplier/1')).text();
    ok(sup.includes('ISSUE_NONCE') && sup.includes('contentHash') && sup.includes('Idempotency-Key'), '/supplier 発注確定に冪等キー (ノンス+内容ハッシュ)');
    ok(sup.includes('orderReqDate'), '/supplier 希望納期入力');
    const dash = await (await fetch(base + '/')).text();
    ok(dash.includes('発注残') && dash.includes('/apps/purchase-orders/backorders'), '/ ダッシュボードに発注残サマリ');
    const orders = await (await fetch(base + '/orders')).text();
    ok(orders.includes('PO番号') && orders.includes('発注残'), '/orders にPO番号・発注残列');
  }
}
// ═══ P14 ロジザード入庫消込 ═══
console.log('── P14: 入庫CSV取込+突合+割当 ──');
{
  const csvOf = rows => iconv.encode(rows.map(r => r.map(v => '"' + String(v) + '"').join(',')).join('\r\n'), 'Shift_JIS');
  const HDR = ['伝票NO', '型番', '品名', '取引先ID', '仕入単価', '良品数', '不良品数', '入庫日'];
  const upload = async (name, rows) => {
    const fd = new FormData();
    fd.append('file', new Blob([csvOf(rows)]), name);
    return j('/api/inbound/import', { method: 'POST', body: fd });
  };

  // 発注 (tracked) を用意: noflyersticker ×20
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 20 }], requestedDate: '2026-07-12' }) });
  const inbOrderId = r.body.id;
  const inbPoItem = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(inbOrderId);

  // 取込: 伝票AR001 = noflyersticker 良品12
  r = await upload('lz1.csv', [HDR, ['AR001', 'noflyersticker', '商品', '0001', '70', '12', '0', '2026/07/11']]);
  ok(r.status === 200 && r.body.ok && r.body.receipts === 1 && r.body.newItems === 1, '入庫CSV取込 (SJIS+入庫日+仕入先)', r.body);
  r = await j('/api/inbound');
  const inb1 = r.body.open.find(x => x.slip === 'AR001');
  ok(inb1 && inb1.goodQty === 12 && inb1.remainingCapacity === 12 && inb1.receiptDate === '2026-07-11' && inb1.supplierCode === '1', '未割当リストに載る (正規化済み仕入先)', inb1);

  // 同一ファイル再取込は冪等
  r = await upload('lz1.csv', [HDR, ['AR001', 'noflyersticker', '商品', '0001', '70', '12', '0', '2026/07/11']]);
  ok(r.body.ok && r.body.alreadyImported === true, '同一ファイル再取込は alreadyImported');

  // 突合候補: 仕入先+商品一致でPOが出る
  r = await j('/api/inbound/' + inb1.id + '/candidates');
  const cand = r.body.candidates.find(c => c.orderItemId === inbPoItem.id);
  ok(r.body.ok && cand && cand.remaining === 20 && cand.suggestedQty === 12, '突合候補: 仕入先+商品一致のPO', r.body.candidates.length);

  // 割当 (入庫12→PO残20、残り8は分納待ち) — logizard入荷として記録される
  r = await j('/api/items/' + inbPoItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 12, inboundItemId: inb1.id,
      remainder: { action: 'await_delivery', nextExpectedDate: '2026-07-25', nextExpectedQty: 8 } }) });
  ok(r.body.ok && r.body.remaining === 8, '割当: PO残20→8 (入庫日が業務日付に)', r.body);
  const lzEv = db.prepare('SELECT * FROM po_item_events WHERE inbound_item_id=?').get(inb1.id);
  ok(lzEv && lzEv.source === 'logizard' && lzEv.effective_date === '2026-07-11', '割当イベント: source=logizard+入庫日');
  r = await j('/api/inbound');
  ok(!r.body.open.some(x => x.id === inb1.id), '全量割当済みは未割当リストから消える');

  // 容量ガード: 伝票AR002 良品5 に 6 は割当不可
  await upload('lz2.csv', [HDR, ['AR002', 'noflyersticker', '商品', '0001', '70', '5', '0', '2026/07/11']]);
  r = await j('/api/inbound');
  const inb2 = r.body.open.find(x => x.slip === 'AR002');
  r = await j('/api/items/' + inbPoItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 6, inboundItemId: inb2.id, remainder: { action: 'await_confirmation', nextActionDate: '2026-07-20' } }) });
  ok(r.status === 400 && r.body.error.includes('良品数'), '容量ガード: 入庫良品数を超える割当は拒否', r.body.error);

  // 訂正版CSV: AR001 が良品10に変わる → 旧行 (割当12あり) は訂正競合
  r = await upload('lz1-rev.csv', [HDR, ['AR001', 'noflyersticker', '商品', '0001', '70', '10', '0', '2026/07/11']]);
  ok(r.body.ok && r.body.supersededItems === 1 && r.body.conflicts.length === 1, '訂正版: 旧行supersede+競合検出', r.body);
  r = await j('/api/inbound');
  ok(r.body.conflicts.some(c => c.id === inb1.id && c.allocated === 12), '訂正競合リストに割当済み旧行が載る');
  // 無効化された行への追加割当は拒否
  r = await j('/api/items/' + inbPoItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 1, inboundItemId: inb1.id, remainder: { action: 'await_confirmation', nextActionDate: '2026-07-20' } }) });
  ok(r.status === 400 && r.body.error.includes('無効化'), '訂正で無効化された入庫行への割当は拒否');

  // 対象外 (履歴型) → 割当拒否 → 解除
  r = await j('/api/inbound/' + inb2.id + '/ignore', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ignore: true, reason: '返品の再入庫' }) });
  ok(r.body.ok && r.body.changed, '対象外の指定');
  r = await j('/api/items/' + inbPoItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 1, inboundItemId: inb2.id, remainder: { action: 'await_confirmation', nextActionDate: '2026-07-20' } }) });
  ok(r.status === 400 && r.body.error.includes('対象外'), '対象外の入庫への割当は拒否');
  r = await j('/api/inbound/' + inb2.id + '/ignore', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ignore: false }) });
  r = await j('/api/inbound');
  ok(r.body.open.some(x => x.id === inb2.id), '対象外解除で未割当に戻る');

  // 仕入先不一致は候補から除外
  await upload('lz3.csv', [HDR, ['AR003', 'noflyersticker', '商品', '0113', '70', '3', '0', '2026/07/11']]);
  r = await j('/api/inbound');
  const inb3 = r.body.open.find(x => x.slip === 'AR003');
  r = await j('/api/inbound/' + inb3.id + '/candidates');
  ok(r.body.ok && r.body.candidates.length === 0, '仕入先不一致 (0113) は候補から除外');

  // 不正行を含むファイルは全体拒否 (部分取込で正常な旧行をsupersedeさせない)
  r = await upload('lz-bad.csv', [HDR, ['AR001', 'noflyersticker', '商品', '0001', '70', 'abc', '0', '2026/07/11']]);
  ok(r.status === 400 && r.body.error.includes('取込を中止'), '不正行を含むCSVは全体拒否 (Codex P14 R1)', r.body.error);
  r = await j('/api/inbound');
  ok(r.body.conflicts.length === 1, '拒否されたファイルで競合が増えない');

  // 割当が残る入庫は対象外にできない (inb1は競合中=superseded、inb2の割当ありケースを作る)
  r = await j('/api/items/' + inbPoItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 2, inboundItemId: inb2.id, remainder: { action: 'await_confirmation', nextActionDate: '2026-07-20' } }) });
  ok(r.body.ok, '容量内の割当は成功 (inb2に2)');
  r = await j('/api/inbound/' + inb2.id + '/ignore', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ignore: true, reason: 'x' }) });
  ok(r.status === 400 && r.body.error.includes('逆仕訳'), '割当が残る入庫の対象外化は拒否 (Codex P14 R1)', r.body.error);

  // 商品・仕入先不一致の割当はDBトリガも拒否 (APIを直接叩いても通らない)
  const otherPo = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'cardstand-silver-r', qty: 3 }] }) });
  const otherItem = db.prepare('SELECT * FROM po_order_items WHERE order_id=?').get(otherPo.body.id);
  r = await j('/api/items/' + otherItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'receipt', qty: 1, inboundItemId: inb2.id, remainder: { action: 'await_confirmation', nextActionDate: '2026-07-20' } }) });
  ok(r.status === 400 && r.body.error.includes('商品が一致しません'), '商品不一致の割当はトリガが拒否 (Codex P14 R2)', r.body.error);

  // 訂正版で仕入先だけ変わった行は supplier_code が更新される
  await upload('lz3-fix.csv', [HDR, ['AR003', 'noflyersticker', '商品', '0001', '70', '3', '0', '2026/07/11']]);
  r = await j('/api/inbound');
  const inb3b = r.body.open.find(x => x.slip === 'AR003');
  ok(inb3b && inb3b.supplierCode === '1', '訂正版: 仕入先のみの訂正も反映', inb3b && inb3b.supplierCode);
  r = await j('/api/inbound/' + inb3b.id + '/candidates');
  ok(r.body.candidates.length >= 1, '仕入先訂正後は候補に出る');

  // 復活経路での仕入先変更ガード迂回 (割当→supersede→別仕入先で同一内容が復活)
  // inb1 (AR001 良品12、割当12、現在superseded) と同一内容を仕入先0113で復活させようとする
  r = await upload('lz1-revive.csv', [HDR, ['AR001', 'noflyersticker', '商品', '0113', '70', '12', '0', '2026/07/11'], ['AR001', 'noflyersticker', '商品', '0113', '70', '10', '0', '2026/07/11']]);
  ok(r.status === 400 && r.body.error.includes('逆仕訳'), '復活経路でも割当残り行の仕入先変更は拒否 (Codex P14 R4)', r.body.error);

  // 不正な入庫日はファイル全体拒否 (原本の日付を壊さない)
  r = await upload('lz-baddate.csv', [HDR, ['AR009', 'noflyersticker', '商品', '0001', '70', '1', '0', '2026/99/99']]);
  ok(r.status === 400 && r.body.error.includes('入庫日'), '不正な入庫日はファイル全体拒否', r.body.error);

  // 壊れた引用符のCSVはパースエラー
  {
    const fdq = new FormData();
    fdq.append('file', new Blob([iconv.encode('"伝票NO","型番","取引先ID","仕入単価","良品数"\r\nAR"9",x,0001,70,1', 'Shift_JIS')]), 'broken.csv');
    r = await j('/api/inbound/import', { method: 'POST', body: fdq });
    ok(r.status === 400 && r.body.error.includes('引用符'), '引用符が壊れたCSVは拒否', r.body.error);
  }

  // 訂正競合は台帳整合性検査でも違反として検出される (自動監視で見逃さない)
  r = await j('/api/ledger/integrity');
  ok(r.body.healthy === false && r.body.issues.some(x => x.kind === 'superseded_with_alloc' && x.inboundItemId === inb1.id),
    '整合性検査: 訂正競合 (superseded_with_alloc) を検出', r.body.issues);

  // ページ配信
  const html = await (await fetch(base + '/inbound')).text();
  ok(html.includes('inbForm') && html.includes('data-match') && html.includes('訂正競合'), '/inbound ページ配信 (取込+割当+競合)');
  ok(html.includes('mRemBox') && html.includes('Idempotency-Key'), '/inbound 割当も三択+冪等キー');
}

// ═══ P15 発注書メール送信 ═══
console.log('── P15: メール送信 (fake transport) ──');
{
  process.env.PO_EMAIL_FAKE = '1';
  // /email/send は expectedMode 必須 (プレビュー時モードの一致検証)。テストでは現在モードを自動注入
  let CUR_MODE = 'dry_run';
  const jsonPost = (p, body, key) => {
    if (p.includes('/email/send') && body && body.expectedMode === undefined) body = { expectedMode: CUR_MODE, ...body };
    return j(p, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) }, body: JSON.stringify(body) });
  };

  // 送信対象PO (tracked)
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 7 }], requestedDate: '2026-07-30' }) });
  const emOrderId = r.body.id;

  // 宛先未登録 → preview はエラー
  r = await j('/api/orders/' + emOrderId + '/email/preview');
  ok(r.status === 400 && r.body.error.includes('メールアドレスが未登録'), 'preview: 宛先未登録はエラー案内', r.body.error);

  // 宛先マスタCSV取込 (仕入先名称で突合、様の有無も吸収)
  {
    const csv = iconv.encode('"仕入先名称","担当者名（メールに使う項目なので必ず「様」をつける）","メールアドレス","CCメールアドレス"\r\n"アメージングクラフト","田中","tanaka@example.com","cc@example.com"\r\n"存在しない仕入先","x","x@example.com",""', 'Shift_JIS');
    const fd = new FormData();
    fd.append('file', new Blob([csv]), 'recipients.csv');
    r = await j('/api/email/recipients/csv', { method: 'POST', body: fd });
    ok(r.body.ok && r.body.updated === 1 && r.body.unmatched.length === 1, '宛先マスタCSV取込 (突合1件+不一致1件)', r.body);
    const sup = db.prepare("SELECT * FROM po_suppliers WHERE supplier_code='1'").get();
    ok(sup.email_to === 'tanaka@example.com' && sup.email_cc === 'cc@example.com' && sup.send_method === 'email', '仕入先マスタに宛先が入る');
  }

  // 対応表CSV取込 (ヘッダなし→A/C列フォールバック)
  {
    const csv = iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"AMC-001","","noflyersticker"', 'Shift_JIS');
    const fd = new FormData();
    fd.append('supplier_code', '0001');
    fd.append('file', new Blob([csv]), 'taiouhyou.csv');
    r = await j('/api/vendor-map/csv', { method: 'POST', body: fd });
    ok(r.body.ok && r.body.count === 1, '対応表CSV取込 (0001→1正規化)', r.body);
  }

  // dry-run宛先未設定 → send はエラー / 設定APIで dry-run 宛先を登録
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', {}, 'em-key-0');
  ok(r.status === 400 && r.body.error.includes('dry-run'), 'send: dry-run宛先未設定はエラー');
  r = await jsonPost('/api/email/settings', { dryrunTo: 'me@b-faith.biz' });
  ok(r.body.ok && r.body.dryrunTo === 'me@b-faith.biz' && r.body.mode === 'dry_run', 'メール設定保存 (既定dry_run)');

  // preview: テンプレ変数・担当者様・先方管理番号列
  r = await j('/api/orders/' + emOrderId + '/email/preview');
  ok(r.body.ok && r.body.subject.includes('【発注書】') && r.body.subject.includes('アメージングクラフト'), 'preview: 件名テンプレ (GAS互換)', r.body.subject);
  ok(r.body.body.startsWith('田中様'), 'preview: 担当者に様を自動付与');
  ok(r.body.vendorColUsed && r.body.csvText.includes('先方管理番号') && r.body.csvText.includes('AMC-001'), 'preview: 添付CSVに先方管理番号列', r.body.csvText.split('\r\n')[0]);

  // 冪等キーなしの送信は拒否 (再送はdedup対象外のためキーが唯一の再実行ガード)
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', {});
  ok(r.status === 400 && r.body.error.includes('Idempotency-Key'), '送信APIは冪等キー必須 (Codex P15 R4)');

  // dry-run送信 (fake): 宛先差し替え+【DRYRUN】+整理番号+送信済み
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', {}, 'em-key-1');
  ok(r.body.ok && r.body.status === 'sent' && r.body.dryRun !== false, 'dry-run送信 (fake Gmail)', r.body);
  const job1 = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(r.body.jobId);
  ok(job1.to_addr === 'me@b-faith.biz' && job1.subject.startsWith('【DRYRUN】') && job1.body.includes('整理番号: DK') && job1.is_dry_run === 1,
    'dry-run: 宛先差替+件名+整理番号', job1.to_addr);
  ok(job1.body.includes('本来の宛先: TO=tanaka@example.com'), 'dry-run: 本来の宛先を本文に明記');

  // 冪等: 同一キー再送は同じジョブ (二重送信なし)
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', {}, 'em-key-1');
  ok(r.body.ok && r.body.jobId === job1.id && r.body.replay === true, 'send: 同一キー再送はreplay');
  ok(db.prepare('SELECT COUNT(*) AS n FROM po_email_jobs WHERE order_id=?').get(emOrderId).n === 1, 'ジョブは1件のみ');

  // live切替は専用API+確認文字列 (通常設定APIでは変更不可)
  r = await jsonPost('/api/email/settings', { mode: 'live' });
  ok(r.body.ok && r.body.mode === 'dry_run', '通常設定APIではmodeを変更できない');
  r = await jsonPost('/api/email/mode', { mode: 'live' });
  ok(r.status === 400 && r.body.error.includes('LIVE'), 'live切替は確認文字列なしでは拒否');
  r = await jsonPost('/api/email/mode', { mode: 'live', confirm: 'LIVE' });
  ok(r.body.ok && r.body.mode === 'live', 'live切替 (confirm=LIVE)');
  // プレビュー時モードと現在モードの不一致は拒否 (プレビュー後にliveへ変わっていたら送らない)
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { expectedMode: 'dry_run' }, 'em-key-mode');
  ok(r.status === 400 && r.body.error.includes('モードが変わって'), 'expectedMode不一致は拒否 (Codex P15 R9)', r.body.error);
  CUR_MODE = 'live';

  // 本番送信 → 同一内容の二重送信はdedup拒否 → 再送は元ジョブ指定必須
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', {}, 'em-key-2');
  ok(r.body.ok && r.body.status === 'sent' && r.body.dryRun === false, 'live送信 (fake)', r.body);
  const job2 = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(r.body.jobId);
  ok(job2.to_addr === 'tanaka@example.com' && job2.cc_addr === 'cc@example.com' && !job2.subject.includes('DRYRUN'), 'live: 本来の宛先+CC');
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', {}, 'em-key-3');
  ok(r.status === 400 && r.body.error.includes('送信済み'), '同一内容の二重送信はdedup拒否', r.body.error);
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { resend: true }, 'em-key-4a');
  ok(r.status === 400 && r.body.error.includes('resendOfJobId'), '再送は元ジョブ指定が必須 (Codex P15 R1)');
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { resend: true, resendOfJobId: job2.id }, 'em-key-4');
  ok(r.body.ok && r.body.status === 'sent', '再送 (元ジョブ=sent済み本送信を検証してresend_of保存)');
  ok(db.prepare('SELECT resend_of FROM po_email_jobs WHERE id=?').get(r.body.jobId).resend_of === job2.id, 'resend_of が保存される');

  // 失敗分類: 要求前の失敗=failed (再試行可)
  process.env.PO_EMAIL_FAKE = 'fail';
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { resend: true, resendOfJobId: job2.id }, 'em-key-5');
  ok(r.body.ok && r.body.status === 'failed' && r.body.error.includes('要求前'), '要求前の失敗はfailed (再試行可)', r.body);
  const failedJobId = r.body.jobId;
  process.env.PO_EMAIL_FAKE = '1';
  r = await jsonPost('/api/email-jobs/' + failedJobId + '/retry', {});
  ok(r.body.ok && r.body.status === 'sent', 'failedジョブの再試行→送信');

  // 失敗分類: 要求後の失敗=unknown (自動・通常再試行は不可。人間確認→queued→送信、Codex P15 R1 High-1)
  process.env.PO_EMAIL_FAKE = 'fail_unknown';
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { resend: true, resendOfJobId: job2.id }, 'em-key-6');
  ok(r.body.ok && r.body.status === 'unknown', '要求後の失敗はunknown (結果不明)', r.body);
  const unknownJobId = r.body.jobId;
  process.env.PO_EMAIL_FAKE = '1';
  r = await jsonPost('/api/email-jobs/' + unknownJobId + '/retry', {});
  ok(r.status === 400 && r.body.error.includes('二重送信防止'), 'unknownは通常再試行できない');
  // unknown中は同一内容の新規通常送信もdedupが拒否 (状態機械の迂回不可、Codex P15 R3 High)
  // ※このジョブはresendなのでdedup対象外 → 通常送信ジョブのunknownで検証する
  r = await jsonPost('/api/email-jobs/' + unknownJobId + '/mark-unsent', {});
  ok(r.status === 400, 'mark-unsentは確認文字列必須');
  // 15分フェンス: 送信試行直後は未送信宣言できない (停止中の旧送信の可能性)
  r = await jsonPost('/api/email-jobs/' + unknownJobId + '/mark-unsent', { confirm: '未送信' });
  ok(r.status === 400 && r.body.error.includes('15分'), 'mark-unsent: 送信試行15分以内は拒否 (Codex P15 R16)', r.body.error);
  db.prepare("UPDATE po_email_jobs SET sending_started_at='2020-01-01T00:00:00.000Z' WHERE id=?").run(unknownJobId);
  r = await jsonPost('/api/email-jobs/' + unknownJobId + '/mark-unsent', { confirm: '未送信' });
  ok(r.body.ok && r.body.status === 'queued', '人間確認後にqueuedへ');
  ok(db.prepare('SELECT attempt_count FROM po_email_jobs WHERE id=?').get(unknownJobId).attempt_count === 0,
    'mark-unsentで再試行カウントもリセット (上限で復旧不能にならない)');
  // generation: markUnsent と lease で世代が進む (照合の競合検知に使う)
  {
    const g = db.prepare('SELECT generation FROM po_email_jobs WHERE id=?').get(unknownJobId).generation;
    ok(g >= 2, 'markUnsent/leaseで世代 (generation) が進む', g);
  }
  r = await jsonPost('/api/email-jobs/' + unknownJobId + '/retry', {});
  ok(r.body.ok && r.body.status === 'sent', '確認後の再試行→送信');

  // 再送回数の上限 (同一元ジョブへ3回まで。em-key-4/5/6で3回済み)
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { resend: true, resendOfJobId: job2.id }, 'em-key-7');
  ok(r.status === 400 && r.body.error.includes('3回まで'), '再送回数の上限 (3回)');
  // 再送ジョブを起点にしても上限は回避できない (ルートに正規化、Codex P15 R2 High)
  const resendChild = db.prepare("SELECT id FROM po_email_jobs WHERE resend_of=? AND status='sent' LIMIT 1").get(job2.id);
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { resend: true, resendOfJobId: resendChild.id }, 'em-key-7b');
  ok(r.status === 400 && r.body.error.includes('3回まで'), '再送チェーンでも上限回避不可 (ルート正規化)', r.body.error);

  // 実在しない予約日時は拒否
  r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { scheduledAt: '2026-02-30T10:00' }, 'em-key-7c');
  ok(r.status === 400 && r.body.error.includes('実在しない'), '実在しない予約日時 (2/30) は拒否');

  // live時、対応表があるのに先方管理番号が無い商品は送信ブロック (Codex P15 R1 M5)
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'cardstand-silver-r', qty: 2 }] }) });
  const schedOrderId = r.body.id;
  r = await jsonPost('/api/orders/' + schedOrderId + '/email/send', {}, 'em-key-vm');
  ok(r.status === 400 && r.body.error.includes('先方管理番号'), 'live: 先方管理番号の欠落は送信ブロック', r.body.error);

  // unknown中は同一内容の新規通常送信もdedupが拒否 (状態機械の迂回不可、Codex P15 R3 High)
  {
    r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 9 }] }) });
    const unkOrderId = r.body.id;
    process.env.PO_EMAIL_FAKE = 'fail_unknown';
    r = await jsonPost('/api/orders/' + unkOrderId + '/email/send', {}, 'em-key-unk1');
    ok(r.body.ok && r.body.status === 'unknown', '通常送信がunknownに', r.body);
    process.env.PO_EMAIL_FAKE = '1';
    r = await jsonPost('/api/orders/' + unkOrderId + '/email/send', {}, 'em-key-unk2');
    ok(r.status === 400 && r.body.error.includes('送信済み'), 'unknown中の新規通常送信はdedup拒否 (迂回不可)', r.body.error);
  }

  // failed もdedup対象 (failed再試行+新規送信の併存で二重送信しない、Codex P15 R12 High)
  {
    r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 4 }] }) });
    const fOrderId = r.body.id;
    process.env.PO_EMAIL_FAKE = 'fail';
    r = await jsonPost('/api/orders/' + fOrderId + '/email/send', {}, 'em-key-f1');
    ok(r.body.ok && r.body.status === 'failed', '通常送信がfailedに');
    const fJobId = r.body.jobId;
    process.env.PO_EMAIL_FAKE = '1';
    r = await jsonPost('/api/orders/' + fOrderId + '/email/send', {}, 'em-key-f2');
    ok(r.status === 400 && r.body.error.includes('送信済み'), 'failed併存中の新規通常送信はdedup拒否');
    r = await jsonPost('/api/email-jobs/' + fJobId + '/cancel', {});
    ok(r.body.ok, 'failedジョブの取消');
    r = await jsonPost('/api/orders/' + fOrderId + '/email/send', {}, 'em-key-f3');
    ok(r.body.ok && r.body.status === 'sent', '取消後は新規送信できる');
  }

  // 予約送信: 未来時刻はqueuedのまま (scheduled)→時刻到来で送信。過去時刻は拒否。取消可能 (dry-runで実施)
  {
    await jsonPost('/api/email/mode', { mode: 'dry_run' });
    CUR_MODE = 'dry_run';
    r = await jsonPost('/api/orders/' + schedOrderId + '/email/send', { scheduledAt: '2020-01-01T00:00' }, 'em-key-8');
    ok(r.status === 400 && r.body.error.includes('過去'), '過去日時の予約は拒否');
    const jst = new Date(Date.now() + 9 * 3600000 + 3600000).toISOString().slice(0, 16); // 1時間後 (JST表記)
    r = await jsonPost('/api/orders/' + schedOrderId + '/email/send', { scheduledAt: jst }, 'em-key-9');
    ok(r.body.ok && r.body.status === 'scheduled', '予約送信: 時刻までqueued (scheduled)', r.body);
    const schedJob = db.prepare('SELECT * FROM po_email_jobs WHERE id=?').get(r.body.jobId);
    ok(schedJob.status === 'queued' && schedJob.scheduled_at != null, '予約ジョブ: queued+scheduled_at保存');
    r = await jsonPost('/api/email-jobs/' + schedJob.id + '/retry', {});
    ok(r.body.ok && r.body.status === 'scheduled', '予約時刻前のretryは送信しない');
    // 予約の取消
    r = await jsonPost('/api/email-jobs/' + schedJob.id + '/cancel', {});
    ok(r.body.ok && r.body.status === 'cancelled', '予約の取消');
    // 新しい予約→時刻到来をシミュレート→送信 (ディスパッチャはretryと同じ経路)
    r = await jsonPost('/api/orders/' + schedOrderId + '/email/send', { scheduledAt: jst }, 'em-key-10');
    const schedJob2Id = r.body.jobId;
    db.prepare("UPDATE po_email_jobs SET scheduled_at='2020-01-01T00:00:00.000Z' WHERE id=?").run(schedJob2Id);
    r = await jsonPost('/api/email-jobs/' + schedJob2Id + '/retry', {});
    ok(r.body.ok && r.body.status === 'sent', '予約時刻到来→送信');

    // 取り残された即時ジョブ (コミット直後クラッシュ相当) をディスパッチャが回収する (Codex P15 R14)
    const em = await imp('apps/purchase-orders/email.js');
    r = await jsonPost('/api/orders/' + schedOrderId + '/email/send', { scheduledAt: jst }, 'em-key-orphan');
    const orphanId = r.body.jobId;
    db.prepare("UPDATE po_email_jobs SET scheduled_at=NULL, created_at='2020-01-01T00:00:00.000Z' WHERE id=?").run(orphanId);
    const disp = await em.dispatchDueEmailJobs();
    ok(disp.processed >= 1 && db.prepare('SELECT status FROM po_email_jobs WHERE id=?').get(orphanId).status === 'sent',
      'ディスパッチャ: 孤児queued (即時・2分超) を回収して送信', disp);
  }

  // MIME 生成 (Message-ID=delivery_key、添付CP932)
  {
    const em = await imp('apps/purchase-orders/email.js');
    const mime = em.buildMime(job2);
    ok(mime.includes('Message-ID: <' + job2.delivery_key + '@') && mime.includes('Content-Disposition: attachment; filename="' + job2.attachment_name + '"'),
      'MIME: Message-ID (照合キー)+添付ファイル名 (PO番号.csv)');
    ok(/Subject: =\?UTF-8\?B\?/.test(mime), 'MIME: 件名RFC2047エンコード');
  }

  // 状態遷移トリガ: sent は終端 (直接SQLでも戻せない)
  let threw = null;
  try { db.prepare("UPDATE po_email_jobs SET status='queued' WHERE id=?").run(failedJobId); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('終端状態'), '送信済みジョブの状態巻き戻しはトリガが拒否');

  // 照合: stale sending → Gmail検索1件=sent / 0件=unknownのまま (queuedに自動で戻さない)
  {
    process.env.PO_EMAIL_FAKE = 'fail';
    r = await jsonPost('/api/orders/' + schedOrderId + '/email/send', {}, 'em-key-11');
    const staleId = r.body.jobId; // failed (dry-run。dedupはdry-run対象外なので作れる)
    process.env.PO_EMAIL_FAKE = '1';
    db.prepare("UPDATE po_email_jobs SET status='queued', error=NULL WHERE id=?").run(staleId);
    db.prepare("UPDATE po_email_jobs SET status='sending', sending_started_at='2020-01-01T00:00:00.000Z' WHERE id=?").run(staleId);
    process.env.PO_EMAIL_FAKE_SEARCH = 'error';
    r = await jsonPost('/api/email/reconcile', {});
    ok(r.body.ok && r.body.checked >= 1, '照合: staleなsendingを検査', r.body);
    let st1 = db.prepare('SELECT status FROM po_email_jobs WHERE id=?').get(staleId);
    ok(st1.status === 'unknown', '照合不能→unknownに落とす (queuedに戻さない)');
    process.env.PO_EMAIL_FAKE_SEARCH = 'found';
    r = await jsonPost('/api/email/reconcile', {});
    st1 = db.prepare('SELECT status FROM po_email_jobs WHERE id=?').get(staleId);
    ok(st1.status === 'sent', '照合: Gmailに存在→sentに回復');
    delete process.env.PO_EMAIL_FAKE_SEARCH;
  }

  // /admin にメール設定セクション、/backorders に📧ボタン
  {
    const adminHtml = await (await fetch(base + '/admin')).text();
    ok(adminHtml.includes('発注書メール設定') && adminHtml.includes('recipForm') && adminHtml.includes('vmapForm'), '/admin メール設定+宛先/対応表取込');
    const boHtml = await (await fetch(base + '/backorders')).text();
    ok(boHtml.includes('data-emailui') && boHtml.includes('DRYRUN') === false && boHtml.includes('emailPanel'), '/backorders 発注書メールパネル');
  }
}

// ═══ 対応表 1件管理 (先方番号対応表タブ) ═══
console.log('── 対応表 1件管理 (entries/products/entry upsert/delete) ──');
{
  const jsonPost2 = (p, body) => j(p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });

  // 一覧: PMLから商品名解決、仕入先コード正規化
  r = await j('/api/vendor-map/entries?supplier=0001');
  ok(r.body.ok && String(r.body.supplierName).includes('アメージングクラフト'), 'entries: 一覧+仕入先名 (0001→1正規化)', r.body);
  const e1 = (r.body.rows || []).find(x => x.product_code === 'noflyersticker');
  ok(e1 && e1.vendor_code === 'AMC-001' && e1.name === 'チラシ お断り ステッカー', 'entries: 既存対応にPML商品名が付く', e1);
  r = await j('/api/vendor-map/entries');
  ok(r.status === 400, 'entries: 仕入先なしは400');
  r = await j('/api/vendor-map/entries?supplier=9999');
  ok(r.status === 400 && r.body.error.includes('未登録'), 'entries: 未登録仕入先は400');

  // 商品検索: 部分一致 (商品名/コード)、選択仕入先の商品を先頭に
  r = await j('/api/vendor-map/products?supplier=2&q=' + encodeURIComponent('ハンドクリーム'));
  ok(r.body.ok && r.body.rows.length === 1 && r.body.rows[0].code === 'gyoumuhandcream60-BI', 'products: 商品名の部分一致', r.body.rows);
  r = await j('/api/vendor-map/products?supplier=0002&q=0');
  ok(r.body.ok && r.body.rows.length >= 3 && r.body.rows[0].code === 'gyoumuhandcream60-BI', 'products: 選択仕入先の商品が先頭', r.body.rows.map(x => x.code));
  r = await j('/api/vendor-map/products?q=');
  ok(r.body.ok && r.body.rows.length === 0, 'products: 空クエリは空配列');

  // upsert: 新規追加 (大文字入力でも product_key は正規化キーで同一商品扱い)
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '0001', product_code: 'CARDSTAND-SILVER-R', vendor_code: 'ZZ-CS-1' });
  ok(r.body.ok && r.body.updated === false, 'entry: 新規追加', r.body);
  let vmRow = db.prepare("SELECT * FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='cardstand-silver-r'").get();
  ok(vmRow && vmRow.vendor_code === 'ZZ-CS-1' && vmRow.product_code === 'CARDSTAND-SILVER-R', 'entry: key正規化+入力コード保存', vmRow);
  // upsert: 上書きは旧値を返す (誤上書きに気づける)
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: 'ZZ-CS-2' });
  ok(r.body.ok && r.body.updated === true && r.body.oldVendorCode === 'ZZ-CS-1', 'entry: 上書きで旧値返却', r.body);
  // バリデーション
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'not-in-pml-xyz', vendor_code: 'X' });
  ok(r.status === 400 && r.body.error.includes('商品マスタに無い'), 'entry: PMLに無い商品コードは拒否 (タイポ防止)', r.body.error);
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: '  ' });
  ok(r.status === 400 && r.body.error.includes('先方管理番号'), 'entry: 先方番号が空は拒否');
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '9999', product_code: 'cardstand-silver-r', vendor_code: 'X' });
  ok(r.status === 400 && r.body.error.includes('未登録'), 'entry: 未登録仕入先は拒否');
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: 'A\nB' });
  ok(r.status === 400 && r.body.error.includes('改行'), 'entry: 先方番号の改行は拒否');

  // 編集が発注書プレビューの添付CSVへ即反映される
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'cardstand-silver-r', qty: 3 }], requestedDate: '2026-08-01' }) });
  const vmOrderId = r.body.id;
  r = await j('/api/orders/' + vmOrderId + '/email/preview');
  ok(r.body.ok && r.body.csvText.includes('ZZ-CS-2'), 'entry編集がプレビュー添付CSVに反映', r.body.ok ? r.body.csvText.split('\r\n').slice(0, 2) : r.body);

  // 削除 → 二重削除は404。監査ログが残る
  r = await j('/api/vendor-map/entry?supplier=1&product=CARDSTAND-SILVER-R', { method: 'DELETE' });
  ok(r.body.ok === true, 'entry: 削除 (コードは正規化して照合)', r.body);
  ok(!db.prepare("SELECT 1 FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='cardstand-silver-r'").get(), 'entry: 削除でDBから消える');
  r = await j('/api/vendor-map/entry?supplier=1&product=cardstand-silver-r', { method: 'DELETE' });
  ok(r.status === 404, 'entry: 二重削除は404');
  const auN = db.prepare("SELECT COUNT(*) AS n FROM po_audit_log WHERE action IN ('vendor_map_entry_upsert','vendor_map_entry_delete')").get().n;
  ok(auN >= 3, 'entry: upsert/deleteが監査ログに残る', auN);

  // /admin にタブとスクリプト
  const adminHtml2 = await (await fetch(base + '/admin')).text();
  ok(adminHtml2.includes('先方番号対応表') && adminHtml2.includes('loadVmap') && adminHtml2.includes('vmProdDl'), '/admin 対応表タブ配信');
}
function nowIsoStr() { return new Date().toISOString(); }

server.close();
console.log(`\n=== RESULT: pass=${pass} fail=${fail} ===`);
process.exit(fail ? 1 : 0);
