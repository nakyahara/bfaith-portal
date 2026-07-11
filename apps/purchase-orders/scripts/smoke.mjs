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

  // ページ配信 (発注残ページ・ダッシュボードのサマリ・履歴のPO番号列)
  {
    const html = await (await fetch(base + '/backorders')).text();
    ok(html.includes('boTabs') && html.includes('data-act') && html.includes('要対応'), '/backorders ページ配信 (タブ+消込ボタン)');
    ok(html.includes('remBox') && html.includes('await_delivery'), '/backorders 部分入荷の三択UI');
    ok(html.includes('data-rev') && html.includes('data-closeui'), '/backorders 逆仕訳+手動クローズUI (promptなし)');
    ok(html.includes("td.innerHTML = ''") && html.includes('Idempotency-Key'), '/backorders パネルDOM破棄+冪等キー送信 (Codex P13b R1)');
    const dash = await (await fetch(base + '/')).text();
    ok(dash.includes('発注残') && dash.includes('/apps/purchase-orders/backorders'), '/ ダッシュボードに発注残サマリ');
    const orders = await (await fetch(base + '/orders')).text();
    ok(orders.includes('PO番号') && orders.includes('発注残'), '/orders にPO番号・発注残列');
  }
}
function nowIsoStr() { return new Date().toISOString(); }

server.close();
console.log(`\n=== RESULT: pass=${pass} fail=${fail} ===`);
process.exit(fail ? 1 : 0);
