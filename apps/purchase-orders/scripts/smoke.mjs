// purchase-orders スモークテスト (scratch DATA_DIR + 実 express 起動)
// 実行: node apps/purchase-orders/scripts/smoke.mjs
import fs from 'fs';
import os from 'os';
import path from 'path';
import { createHash } from 'crypto';
import { pathToFileURL, fileURLToPath } from 'url';
import iconv from 'iconv-lite';

const SCRATCH = fs.mkdtempSync(path.join(os.tmpdir(), 'po-smoke-'));
process.env.DATA_DIR = SCRATCH;

const WORK = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const imp = p => import(pathToFileURL(path.join(WORK, p)).href);
const { initMirrorDB } = await imp('apps/warehouse-mirror/db.js');
initMirrorDB(); // 本番では server.js (warehouse-mirror router import) が起動時に実行する
// ⚠️ mirror router は import 時に initMirrorDB を再実行して接続を開き直すため、
// getDB() でハンドルを掴む前 (=ここ) で読み込む (注残SSoTテストで mount する)
process.env.PML_READ_TOKEN = 'smoke-pml-token';
const mirrorRouter = (await imp('apps/warehouse-mirror/router.js')).default;
const pmlListRouter = (await imp('apps/product-management-list/router.js')).default;
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
app.use('/apps/warehouse-mirror', express.json({ limit: '8mb' }), mirrorRouter);
app.use('/apps/product-management-list', pmlListRouter);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}/apps/purchase-orders`;
const siteBase = `http://127.0.0.1:${server.address().port}`;
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
// 注残はアプリ台帳が正 (NE CSVの発注残数は使わない)。台帳が空の初期状態では
// NE注残で対象外だった gyoumuhandcream60-BI (仕入先2) も要発注に入る
ok(r.body.ok && r.body.cards.length === 2 && r.body.cards[0].code === '1' && r.body.cards[1].code === '2', 'overview: 台帳注残ベースで仕入先1+2が要発注', r.body.cards);
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
{
  const noflyAfter = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(t => t.code === 'noflyersticker');
  ok(noflyAfter && noflyAfter.recentIssued != null, '発注済みバッジ (recentIssued)');
  // 台帳注残: 確定した瞬間に注残へ反映され、NE CSVを待たずに要発注から消える
  ok(noflyAfter && noflyAfter.backOrder === 400 && !r.body.targets.some(t => t.code === 'noflyersticker'),
    '確定即時に台帳注残へ反映され要発注から消える', noflyAfter && noflyAfter.backOrder);
}
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
// 取込前: 直近のissue (400+200) で仕入先1が「発注確定済み」サイクルに載っている
r = await j('/api/cycle-issued');
ok(r.body.ok && r.body.suppliers.some(s => s.code === '1'), 'cycle-issued: 発注確定済み仕入先が載る', r.body.suppliers);
r = await j('/api/overview');
{
  const ovCard = r.body.cards.find(c => c.code === '1');
  ok(ovCard && ovCard.issuedCount >= 1, 'overview: 発注確定済みカウント付き', ovCard);
}
r = await j('/api/ne-overlay/csv', { method: 'POST', body: fdNe });
ok(r.status === 200 && r.body.rowCount === 2, 'NE CSV取込 2件', r.body);
// データ更新 (NE取込) で発注確定サイクルがリセットされる
r = await j('/api/cycle-issued');
ok(r.body.ok && r.body.suppliers.length === 0, 'NE CSV取込で発注確定サイクルがリセット', r.body.suppliers);
r = await j('/api/supplier/1');
ok(r.body.overlay && r.body.overlay.applied === true, 'overlay 適用中フラグ');
const noflyOv = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'noflyersticker');
// 注残はNEオーバーレイの発注残数(50)ではなくアプリ台帳 (issue 400 + issue2 200 = 600) が正
ok(noflyOv && noflyOv.stock === 120 && noflyOv.backOrder === 600, 'overlay: 在庫=NE100+FBA20, 注残=台帳600 (NE値50は使わない)', noflyOv && { stock: noflyOv.stock, back: noflyOv.backOrder });
ok(noflyOv && noflyOv.cost === 75, 'overlay: 原価も最新化 (70→75)', noflyOv && noflyOv.cost);
// overlay在庫+台帳注残で在庫月数が再計算される: (120+600)/368 = 1.9565 → M1.5超で対象外
ok(noflyOv && Math.abs(noflyOv.stockMonths - 720 / 368) < 0.01 && noflyOv.recQty == null,
  'overlay: 在庫月数再計算 (overlay在庫+台帳注残)', noflyOv && { m: noflyOv.stockMonths, rec: noflyOv.recQty });
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
ok(noflyBack && noflyBack.backOrder === 600, '解除後も注残は台帳値のまま (在庫はPMLに戻る)', noflyBack && noflyBack.backOrder);
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

  // 台帳注残 (要発注判定) に legacy PO の数量は入らない (tracking_mode='tracked' でも境界以前は除外)
  {
    r = await j('/api/supplier/1');
    const noflyLZ = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'noflyersticker');
    const zanNoBoundary = db.prepare(`SELECT COALESCE(SUM(b.remaining_qty),0) AS z FROM po_order_items i
      JOIN po_orders o ON o.id = i.order_id JOIN v_po_item_balance b ON b.order_item_id = i.id
      WHERE i.product_key='noflyersticker' AND o.status='issued' AND o.tracking_mode='tracked'
        AND o.closed_at IS NULL AND b.remaining_qty > 0`).get().z;
    ok(noflyLZ && zanNoBoundary - noflyLZ.backOrder === 10, '台帳注残: 境界以前のlegacy PO (10個) を除外', { back: noflyLZ && noflyLZ.backOrder, all: zanNoBoundary });
  }

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

  // 明細単位の希望納期 (下書き): item.requestedDate が明細に保存され、混在ならヘッダはNULL
  r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1, requestedDate: '2026-09-01' }, { code: 'cardstand-silver-r', qty: 2 }], note: 'd2' }) });
  {
    const dHdr = db.prepare("SELECT id, requested_date FROM po_orders WHERE status='draft' AND supplier_code='1'").get();
    const dIts = db.prepare('SELECT product_code, requested_date FROM po_order_items WHERE order_id=? ORDER BY id').all(dHdr.id);
    ok(r.body.ok && dIts[0].requested_date === '2026-09-01' && dIts[1].requested_date == null, '下書きに明細単位の希望納期を保存 (未指定はNULL)');
    ok(dHdr.requested_date == null, '明細納期が混在/一部指定ならヘッダはNULL');
  }
  r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1, requestedDate: '2026-09-31' }] }) });
  ok(r.status === 400, '明細希望納期の実在しない日付は400');
  // ヘッダ既定値互換のdraftへ戻す
  await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1 }], note: 'd', requestedDate: '2026-08-15' }) });
  {
    const dIt = db.prepare("SELECT i.requested_date FROM po_order_items i JOIN po_orders o ON o.id=i.order_id WHERE o.status='draft' AND o.supplier_code='1'").get();
    ok(dIt && dIt.requested_date === '2026-08-15', 'ヘッダ希望納期は明細の既定値として保存 (旧API互換)');
  }

  // 旧形式draft互換 (ヘッダ日付あり+明細NULL): 復元時の実効希望納期はヘッダ値 (Codex 明細納期R1 High)
  {
    const dHdr = db.prepare("SELECT id FROM po_orders WHERE status='draft' AND supplier_code='1'").get();
    db.prepare('UPDATE po_order_items SET requested_date=NULL WHERE order_id=?').run(dHdr.id);
    const supHtml = await (await fetch(base + '/supplier/1')).text();
    ok(supHtml.includes('2026-08-15'), '旧形式draft (明細NULL) はヘッダ希望納期を実効値として復元', supHtml.includes('requestedDate'));
  }

  // 空カート保存 = draft削除。deleted は実削除時のみ true (draftなしでの入力クリア誤発火防止)
  const emptyPost = () => j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [] }) });
  r = await emptyPost();
  ok(r.body.ok && r.body.deleted === true, '空カート保存: 既存draftあり → deleted=true');
  r = await emptyPost();
  ok(r.body.ok && r.body.deleted === false, '空カート保存: draftなし → deleted=false');
  // 状態を元に戻す
  await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1 }], note: 'd', requestedDate: '2026-08-15' }) });

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
    ok(sup.includes('data-date=') && sup.includes('希望納期'), '/supplier 商品ごとの希望納期入力');
    ok(!sup.includes('orderReqDate'), '/supplier ヘッダ一括納期入力は廃止');
    ok(sup.includes('data-act="save"') && sup.includes('data-act="issue"'), '/supplier 保存/確定ボタン (上下2箇所)');
    ok(sup.includes('fSaved') && sup.includes('未保存の変更あり') && sup.includes('発注金額合計'), '/supplier 保存済みインジケータ (SKU数+発注金額合計)');
    const dash = await (await fetch(base + '/')).text();
    ok(dash.includes('発注残') && dash.includes('/apps/purchase-orders/backorders'), '/ ダッシュボードに発注残サマリ');
    ok(dash.includes('confirmCycleReset') && dash.includes('cycle-issued'), '/ データ更新前の二段確認 (確定リセット+メール未送信)');
    ok(dash.includes('更新を中止しました'), '/ 確認情報の取得失敗時は更新しない (fail-closed)');
    ok(dash.includes('発注確定済み'), '/ 仕入先カードに発注確定済みバッジ');
    const adminHtml = await (await fetch(base + '/admin')).text();
    ok(adminHtml.includes('発注方法') && adminHtml.includes('📠 FAX') && adminHtml.includes('🌐 WEBサイト'), '/admin 発注方法プルダウン (email/fax/web/none)');
    // IA整理 (2026-07-13): 4グループナビ+行動ベース説明+対応表CSVは対応表グループ内+未紐付けバッジ
    ok(adminHtml.includes('grpBar') && adminHtml.includes('setGroup') && adminHtml.includes('📦 発注条件・商品') && adminHtml.includes('ここで設定します'), '/admin 4グループナビ+行動ベース説明');
    ok(adminHtml.includes('zoneVmapCsv') && adminHtml.includes('unlinkedBadge') && adminHtml.includes('subBar'), '/admin 対応表CSV内包+未紐付けバッジ+サブタブ');
    const orders = await (await fetch(base + '/orders')).text();
    ok(orders.includes('PO番号') && orders.includes('発注残'), '/orders にPO番号・発注残列');
    // 仕入先ごと並び替え+絞り込み。仕入先名クリック=確定明細 (ワークスペース行きリンクは廃止、中原さん報告 2026-07-13)
    ok(orders.includes('ordSort') && orders.includes('仕入先ごと') && orders.includes('ordSup'), '/orders 並び順+仕入先絞り込みUI');
    ok(!orders.includes("'<td><a href=\"/apps/purchase-orders/supplier/'"), '/orders 一覧の仕入先名はワークスペースリンクではない');
    ok(orders.includes('新しい発注作業') && orders.includes('確定時の明細'), '/orders 明細=確定時の内容+ワークスペースは明示リンク');
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

  // preview: テンプレ変数・担当者様・添付CSVは「いつもの発注書」フォーマット (GAS/NE発注書互換、中原さん指定 2026-07-13)
  r = await j('/api/orders/' + emOrderId + '/email/preview');
  ok(r.body.ok && r.body.subject.includes('【発注書】') && r.body.subject.includes('アメージングクラフト'), 'preview: 件名テンプレ (GAS互換)', r.body.subject);
  ok(r.body.body.startsWith('田中様'), 'preview: 担当者に様を自動付与');
  {
    const cl = r.body.csvText.split('\r\n');
    ok(cl[0] === 'Header,発注伝票番号,発注日,仕入先名,発行担当者, ,合計金額,備考', 'preview: CSV Header行 (発注書フォーマット)', cl[0]);
    const h1 = cl[1].split(',');
    ok(/^PO-\d{4}-\d{4}$/.test(h1[1]) && /^\d{4}-\d{2}-\d{2}$/.test(h1[2]) && h1[3].includes('アメージングクラフト') && h1[4] === '中原　大輔',
      'preview: 伝票情報行 (PO番号/発注日/仕入先/発行担当者)', cl[1]);
    ok(/^\d+\.\d{2}$/.test(h1[6]), 'preview: 合計金額は小数2桁', h1[6]);
    ok(cl[2] === '--,--,--,--,--,--,--,--', 'preview: 区切り行 (--×8)');
    ok(cl[3] === '発注区分,商品コード,商品名,希望納期,発注単価,発注数,小計,備考', 'preview: 明細ヘッダ8列 (4列目=希望納期)', cl[3]);
    const d1 = cl[4].split(',');
    ok(d1[0] === '通常' && d1[1] === 'noflyersticker' && /^\d+\.\d{2}$/.test(d1[4]) && /^\d+\.\d{2}$/.test(d1[6]),
      'preview: 明細行 (通常/コード/単価・小計2桁)', cl[4]);
    ok(r.body.vendorColUsed && d1[7] === 'AMC-001', 'preview: 先方管理番号は備考列 (旧GASのH列追記と同じ)', d1[7]);
    ok(d1[3] === '2026/07/30', 'preview: 希望納期は明細4列目', d1[3]);
    // ポップアッププレビュー用の行列 (csvRows) はCSV文字列と同一内容
    ok(Array.isArray(r.body.csvRows) && r.body.csvRows.length === cl.length && r.body.csvRows[3][3] === '希望納期' &&
      r.body.csvRows[4][1] === 'noflyersticker', 'preview: csvRows (ポップアップ表表示用) がCSVと一致', r.body.csvRows && r.body.csvRows[3]);
  }
  ok(r.body.body.includes('希望納期：2026年7月30日'), 'preview: 本文に希望納期 ({{nouki}})', r.body.body.split('\n').find(l => l.includes('希望納期')));

  // 発行担当者はメール設定で変更できる (既定=中原　大輔)
  r = await jsonPost('/api/email/settings', { issuerName: 'テスト担当' });
  ok(r.body.ok && r.body.issuerName === 'テスト担当', 'メール設定: 発行担当者を変更');
  r = await j('/api/orders/' + emOrderId + '/email/preview');
  ok(r.body.csvText.split('\r\n')[1].split(',')[4] === 'テスト担当', 'preview: 発行担当者がCSVに反映');
  r = await jsonPost('/api/email/settings', { issuerName: '' });
  ok(r.body.ok && r.body.issuerName === '中原　大輔', 'メール設定: 空にすると既定に戻る');
  r = await jsonPost('/api/email/settings', { issuerName: '担当\n者' });
  ok(r.status === 400 && r.body.error.includes('改行'), 'メール設定: 発行担当者の改行は拒否 (末尾改行も)');

  // 希望納期なしの発注 → 「指定なし」。カスタムテンプレに {{nouki}} が無い場合は末尾に追記される
  {
    r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1 }] }) });
    const noDateId = r.body.id;
    r = await j('/api/orders/' + noDateId + '/email/preview');
    ok(r.body.ok && r.body.body.includes('希望納期：指定なし'), 'preview: 納期未指定は「指定なし」');
    ok(r.body.csvText.split('\r\n')[4].split(',')[3] === '', 'preview: 納期未指定は希望納期列 (4列目) が空欄');
    r = await jsonPost('/api/email/settings', { bodyTpl: '{{contact}}\nいつもの内容でお願いします。' });
    ok(r.body.ok, 'カスタム本文テンプレ保存');
    r = await j('/api/orders/' + emOrderId + '/email/preview');
    ok(r.body.body.includes('希望納期: 2026年7月30日'), 'preview: {{nouki}}なしテンプレでも希望納期を末尾追記', r.body.body);
    r = await j('/api/orders/' + noDateId + '/email/preview');
    ok(!r.body.body.includes('希望納期'), 'preview: {{nouki}}なしテンプレ+納期未指定なら追記しない');
    r = await jsonPost('/api/email/settings', { bodyTpl: '' }); // 既定テンプレに戻す
    ok(r.body.ok, '本文テンプレを既定に戻す');
  }

  // 商品ごとに異なる希望納期 → ヘッダNULL・本文は個別案内・添付CSVに希望納期列
  {
    r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: [
        { code: 'noflyersticker', qty: 2, requestedDate: '2026-08-10' },
        { code: 'cardstand-silver-r', qty: 3, requestedDate: '2026-08-20' },
      ] }) });
    const mixedId = r.body.id;
    ok(r.body.ok && db.prepare('SELECT requested_date FROM po_orders WHERE id=?').get(mixedId).requested_date == null, 'issue: 明細納期が混在ならヘッダNULL');
    const its = db.prepare('SELECT requested_date FROM po_order_items WHERE order_id=? ORDER BY id').all(mixedId);
    ok(its[0].requested_date === '2026-08-10' && its[1].requested_date === '2026-08-20', 'issue: 明細単位の希望納期を保存');
    r = await j('/api/orders/' + mixedId + '/email/preview');
    ok(r.body.ok && r.body.body.includes('商品ごとに指定'), 'preview: 混在納期は個別案内', r.body.body.split('\n').find(l => l.includes('希望納期')));
    {
      const mcl = r.body.csvText.split('\r\n');
      ok(mcl[4].split(',')[3] === '2026/08/10' && mcl[5].split(',')[3] === '2026/08/20',
        'preview: 納期混在も明細4列目に商品ごとの日付 (8列維持)', mcl.slice(4, 6));
    }
    // 明細納期が不正なら400
    r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1, requestedDate: '2026/08/10' }] }) });
    ok(r.status === 400, 'issue: 明細希望納期の形式不正は400');
  }

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

  // dry-run送信のみのPOは「発注書メール未送信」扱い (cycle-issuedのunsent)
  r = await j('/api/cycle-issued');
  {
    const cyc1 = r.body.suppliers.find(s => s.code === '1');
    ok(r.body.ok && cyc1 && cyc1.unsent >= 1, 'cycle-issued: dry-run送信のみは未送信扱い (unsent>=1)', cyc1);
    ok(cyc1 && cyc1.unsentPoNumbers.length === cyc1.unsent && cyc1.poNumbers.length >= cyc1.unsentPoNumbers.length,
      'cycle-issued: unsentPoNumbers は未送信POだけ列挙', cyc1);
  }

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

  // live時、先方管理番号が未登録でも送信できる (中原さん決定 2026-07-13: ブロックせず警告のみ。旧Codex P15 R1 M5のブロックは撤回)
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'cardstand-silver-r', qty: 2 }] }) });
  const schedOrderId = r.body.id; // こちらは未送信のまま後続の予約テストで使う
  {
    r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: [{ code: 'cardstand-silver-r', qty: 3 }] }) });
    const vmOpenId = r.body.id;
    r = await j('/api/orders/' + vmOpenId + '/email/preview');
    ok(r.body.ok && r.body.missingVendorCodes.includes('cardstand-silver-r'), 'preview: 先方番号未登録は警告情報として返す (ブロックしない)', r.body.missingVendorCodes);
    ok(r.body.csvText.split('\r\n')[4].split(',')[7] === '', 'preview: 未登録商品の備考列は空欄');
    r = await jsonPost('/api/orders/' + vmOpenId + '/email/send', {}, 'em-key-vm');
    ok(r.body.ok && r.body.status === 'sent', 'live: 先方管理番号未登録でも送信できる (中原さん決定)', r.body);
  }

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
    ok(boHtml.includes('emPreviewModal') && boHtml.includes('送信内容をプレビュー') && boHtml.includes('pomodal'), '/backorders 送信内容ポップアッププレビュー');
    ok(boHtml.includes('メールは送信されていません') && boHtml.includes('備考欄は空欄のまま送られます'),
      '/backorders 送信失敗はalert+赤バナー / 先方番号未登録は確認ダイアログで警告');
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

  // 商品検索: 部分一致 (商品名/コード)、選択仕入先の商品だけ返す (Codex R1 High-2)
  r = await j('/api/vendor-map/products?supplier=2&q=' + encodeURIComponent('ハンドクリーム'));
  ok(r.body.ok && r.body.rows.length === 1 && r.body.rows[0].code === 'gyoumuhandcream60-BI', 'products: 商品名の部分一致', r.body.rows);
  r = await j('/api/vendor-map/products?supplier=0002&q=0');
  ok(r.body.ok && r.body.rows.length === 1 && r.body.rows[0].code === 'gyoumuhandcream60-BI', 'products: 他仕入先の商品は候補に出ない', r.body.rows.map(x => x.code));
  r = await j('/api/vendor-map/products?q=');
  ok(r.body.ok && r.body.rows.length === 0, 'products: 空クエリは空配列');

  // 仕入先未設定 (空欄) の商品は候補にも出ず登録も拒否 (Codex R2 High)
  insRow.run('nosupplier-item', '仕入先未設定商品', '', '取扱中', 2, 10, 0, 0, 0, 100, 1.5, 500, 200, '2026-01-01', '2020-01-01');
  r = await j('/api/vendor-map/products?supplier=1&q=nosupplier');
  ok(r.body.ok && r.body.rows.length === 0, 'products: 仕入先未設定の商品は候補に出ない');
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'nosupplier-item', vendor_code: 'X' });
  ok(r.status === 400 && r.body.error.includes('仕入先が未設定'), 'entry: 仕入先未設定の商品は登録拒否', r.body.error);

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
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: 'x'.repeat(101) });
  ok(r.status === 400 && r.body.error.includes('長すぎ'), 'entry: 先方番号の長さ上限 (Codex R1 Med)');
  // 他仕入先の商品は登録拒否 (仕入先2の商品を仕入先1の対応表へ)
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'gyoumuhandcream60-BI', vendor_code: 'X-1' });
  ok(r.status === 400 && r.body.error.includes('この商品の仕入先は 2'), 'entry: 他仕入先の商品は登録拒否 (Codex R1 High)', r.body.error);

  // 楽観ロック (baseUpdatedAt): 追加フォーム=null → 既存があれば409 / 古い版での保存・削除は409
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: 'ZZ-DUP', baseUpdatedAt: null });
  ok(r.status === 409 && r.body.conflict && r.body.current.vendor_code === 'ZZ-CS-2', 'entry: 追加フォームで既存商品は409+現在値', r.body);
  const curBase = db.prepare("SELECT updated_at FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='cardstand-silver-r'").get().updated_at;
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: 'ZZ-CS-3', baseUpdatedAt: '2020-01-01T00:00:00.000Z' });
  ok(r.status === 409 && r.body.conflict, 'entry: 古い版での保存は409 (他画面の変更を握り潰さない)', r.body);
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: 'ZZ-CS-3', baseUpdatedAt: curBase });
  ok(r.body.ok && r.body.updated === true, 'entry: 一致する版での保存は成功');
  r = await j('/api/vendor-map/entry?supplier=1&product=cardstand-silver-r&base=' + encodeURIComponent(curBase), { method: 'DELETE' });
  ok(r.status === 409 && r.body.conflict, 'entry: 古い版での削除は409', r.body);
  // 上の保存でZZ-CS-3になった分をZZ-CS-2へ戻す (以降のプレビュー反映テストの前提を単純に保つ)
  r = await jsonPost2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'cardstand-silver-r', vendor_code: 'ZZ-CS-2' });
  ok(r.body.ok, 'entry: base未指定は無条件upsert (スクリプト/AI連携用)');

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

console.log('── NE発注残 初期取込 (移行PO) ──');
{
  const neCsvOf = rows => iconv.encode(rows.map(r => r.map(v => '"' + String(v) + '"').join(',')).join('\r\n'), 'Shift_JIS');
  const NEHDR = ['発注伝票番号', '発注先名', '商品コード', '商品名', 'option', '発注数', '注残計', '予定納期', '備考', '商品区分', '受注伝票番号', '明細行', '発行日', '仕入先cd', '発注明細行', '商品区分値'];
  const neRow = (slip, code, name, qty, rem, date, sup, due = '') => [slip, 'テスト様', code, name, '', qty, rem, due, '', '通常', '0', '0', date, sup, '1', '0'];
  const upNe = async (name, rows, opts = {}) => {
    const fd = new FormData();
    fd.append('file', new Blob([neCsvOf(rows)]), name);
    if (opts.commit) { fd.append('commit', '1'); fd.append('fileHash', opts.hash || ''); fd.append('planHash', opts.plan || ''); }
    return j('/api/backorders/ne-import', { method: 'POST', body: fd });
  };

  // 事前状態 (発注済みバッジ・月次上限・PO件数)
  r = await j('/api/supplier/1');
  const beforeIssuedTotal = r.body.issuedTotal;
  const beforeDead = (r.body.horikoshi.find(p => p.code === 'deaditem') || {}).recentIssued || null;
  const ordersBefore = db.prepare('SELECT COUNT(*) n FROM po_orders').get().n;
  // 整合性はP14が意図的に残した訂正競合が既に載っているため、取込で「増えない」ことを検証する
  r = await j('/api/ledger/integrity');
  const integrityBefore = r.body.issues.length;

  // プレビュー (書込なし)。NE実CSVと同じ列構成 + YYYY/M/D 日付 + 部分入庫済 + PML外商品
  const NE1 = [NEHDR,
    neRow('6274', 'noflyersticker', 'チラシ', 500, 200, '2025/12/18', '0001'),
    neRow('6274', 'deaditem', '休眠', 100, 100, '2025/12/18', '0001', '2026/8/1'),
    neRow('7645', 'gyoumuhandcream60-BI', 'ハンドクリーム', 24, 24, '2026/6/8', '0002'),
    neRow('7645', 'not-in-pml-item', 'PML外商品', 10, 3, '2026/6/8', '0002'),
  ];
  r = await upNe('ne1.csv', NE1);
  ok(r.status === 200 && r.body.ok && r.body.commit === false && r.body.orders === 2 && r.body.items === 4, 'プレビュー: 2伝票4明細', r.body);
  ok(r.body.totalRemaining === 327 && r.body.receiptEvents === 2, 'プレビュー: 残数計327 / 取込前入庫2明細', { rem: r.body.totalRemaining, ev: r.body.receiptEvents });
  ok(r.body.warnings.some(w => w.includes('PMLに存在しない')), 'プレビュー: PML外商品は警告', r.body.warnings);
  ok(db.prepare('SELECT COUNT(*) n FROM po_orders').get().n === ordersBefore, 'プレビューは書込なし');
  const NE1_HASH = r.body.fileHash;
  const NE1_PLAN = r.body.planHash;
  ok(typeof NE1_HASH === 'string' && NE1_HASH.length === 64 && typeof NE1_PLAN === 'string' && NE1_PLAN.length === 64, 'プレビュー: fileHash/planHash (SHA-256) を返す');

  // fileHash/planHash不一致 (プレビューと別ファイル・プレビューなし・DB変化) の確定は拒否、書込なし
  r = await upNe('ne1.csv', NE1, { commit: true, hash: 'deadbeef', plan: NE1_PLAN });
  ok(r.status === 400 && r.body.error.includes('一致しません'), '確定: fileHash不一致は拒否', r.body.error);
  r = await upNe('ne1.csv', NE1, { commit: true });
  ok(r.status === 400, '確定: ハッシュなしは拒否');
  r = await upNe('ne1.csv', NE1, { commit: true, hash: NE1_HASH, plan: 'stale-plan' });
  ok(r.status === 400 && r.body.error.includes('変わっています'), '確定: planHash不一致は拒否 (プレビュー後のDB変化)', r.body.error);
  ok(db.prepare('SELECT COUNT(*) n FROM po_orders').get().n === ordersBefore, 'ハッシュ不一致では書込なし');

  // 取込実行
  r = await upNe('ne1.csv', NE1, { commit: true, hash: NE1_HASH, plan: NE1_PLAN });
  ok(r.status === 200 && r.body.ok && r.body.commit === true && r.body.created.length === 2, '取込実行: 移行PO 2件作成', r.body.created);
  const mig1 = db.prepare("SELECT * FROM po_orders WHERE ne_slip_number='6274'").get();
  ok(mig1 && mig1.status === 'issued' && mig1.origin === 'migration' && mig1.send_blocked === 1 && /^PO-\d{4}-\d{4,}$/.test(mig1.po_number),
    '移行PO: issued + origin=migration + send_blocked + PO番号採番', mig1 && { origin: mig1.origin, po: mig1.po_number });
  ok(mig1.note.includes('伝票6274') && mig1.note.includes('2025-12-18'), '移行PO: noteにNE伝票番号+NE発行日', mig1.note);
  // 残数 = NE注残計 (発注500 → migration入荷300 → 残200)
  const migItem = db.prepare(`SELECT i.*, b.remaining_qty, b.received_qty FROM po_order_items i
    JOIN v_po_item_balance b ON b.order_item_id=i.id WHERE i.order_id=? AND i.product_key='noflyersticker'`).get(mig1.id);
  ok(migItem.qty === 500 && migItem.received_qty === 300 && migItem.remaining_qty === 200, '明細: qty=NE発注数500 / 取込前入庫300 / 残200',
    { q: migItem.qty, rcv: migItem.received_qty, rem: migItem.remaining_qty });
  const migEv = db.prepare('SELECT * FROM po_item_events WHERE order_item_id=?').get(migItem.id);
  ok(migEv && migEv.source === 'migration' && migEv.actor_type === 'migration' && migEv.qty === 300, 'イベント: source/actor_type=migration ×300', migEv && migEv.source);
  // 予定納期 → 明細の希望納期 + PML原価スナップショット
  const deadItem = db.prepare("SELECT * FROM po_order_items WHERE order_id=? AND product_key='deaditem'").get(mig1.id);
  ok(deadItem.requested_date === '2026-08-01' && deadItem.unit_cost === 200, '明細: 予定納期→希望納期 + PML原価200', { d: deadItem.requested_date, c: deadItem.unit_cost });
  // PML外商品は単価未設定で取り込まれる
  const mig2 = db.prepare("SELECT * FROM po_orders WHERE ne_slip_number='7645'").get();
  const npItem = db.prepare(`SELECT i.*, b.remaining_qty FROM po_order_items i
    JOIN v_po_item_balance b ON b.order_item_id=i.id WHERE i.order_id=? AND i.product_key='not-in-pml-item'`).get(mig2.id);
  ok(npItem && npItem.unit_cost === null && npItem.remaining_qty === 3, 'PML外商品: 単価NULL + 残3', npItem && { c: npItem.unit_cost, rem: npItem.remaining_qty });

  // 取込済み化でplanが変わるため、取込前の古いプレビューでの確定は拒否される
  r = await upNe('ne1.csv', NE1, { commit: true, hash: NE1_HASH, plan: NE1_PLAN });
  ok(r.status === 400 && r.body.error.includes('変わっています'), '取込済み化後、古いプレビューでの再確定は拒否');
  // 冪等: 同じCSVを再度プレビュー→取込しても全スキップ (二重登録なし)
  r = await upNe('ne1.csv', NE1);
  ok(r.body.ok && r.body.orders === 0 && r.body.skipped.length === 2, '再プレビュー: 全伝票取込済み', r.body.skipped);
  r = await upNe('ne1.csv', NE1, { commit: true, hash: r.body.fileHash, plan: r.body.planHash });
  ok(r.body.ok && r.body.orders === 0 && r.body.created.length === 0 && r.body.skipped.length === 2 && r.body.skipped.every(s => /^PO-/.test(s.poNumber)), '再取込: 全伝票スキップ (冪等)', r.body.skipped);

  // planHashがDB由来フィールドを束縛していることの回帰テスト (プレビュー後のPML原価変更で拒否、Codex R3 Low)
  {
    const NE3 = [NEHDR, neRow('9200', 'deaditem', '休眠', 10, 10, '2026/7/1', '0001')];
    r = await upNe('ne3.csv', NE3);
    const h3 = r.body.fileHash, p3 = r.body.planHash;
    db.prepare("UPDATE mirror_pml_snapshot_rows SET 原価=999 WHERE 商品コード='deaditem'").run();
    r = await upNe('ne3.csv', NE3, { commit: true, hash: h3, plan: p3 });
    ok(r.status === 400 && r.body.error.includes('変わっています'), 'プレビュー後のPML原価変更は確定拒否 (planHash束縛)', r.body.error);
    db.prepare("UPDATE mirror_pml_snapshot_rows SET 原価=200 WHERE 商品コード='deaditem'").run();
    r = await upNe('ne3.csv', NE3);
    r = await upNe('ne3.csv', NE3, { commit: true, hash: r.body.fileHash, plan: r.body.planHash });
    ok(r.body.ok && r.body.created.length === 1, '原価復元後の再プレビュー→確定は成功');
  }

  // 移行POは発注提案の「発注済み」バッジ・月次上限集計を汚染しない (数量はNE注残としてPML反映済みのため)
  r = await j('/api/supplier/1');
  const afterDead = (r.body.horikoshi.find(p => p.code === 'deaditem') || {}).recentIssued || null;
  ok(JSON.stringify(afterDead) === JSON.stringify(beforeDead), '移行POは「発注済み」バッジに出ない', afterDead);
  ok(r.body.issuedTotal === beforeIssuedTotal, '移行POは月次上限集計に入らない', { before: beforeIssuedTotal, after: r.body.issuedTotal });

  // 移行POへ発注書メールは送れない (send_blocked)
  r = await j('/api/orders/' + mig1.id + '/email/preview');
  ok(r.status === 400 && r.body.error.includes('送信対象外'), '移行POのメールプレビューは拒否', r.body.error);

  // 発注残一覧に載る + 台帳整合性クリーン
  r = await j('/api/backorders');
  const boMig = r.body.orders.find(o => o.id === mig1.id);
  ok(boMig && boMig.origin === 'migration' && boMig.sendBlocked === true && boMig.remainingQty === 300, '発注残一覧: 移行PO (残200+100)', boMig && boMig.remainingQty);
  r = await j('/api/ledger/integrity');
  ok(r.body.ok && r.body.issues.length === integrityBefore, '取込で整合性違反が増えない', r.body.issues);

  // ロジザード入庫CSVの消込が移行POに効く (突合候補→割当)
  {
    const HDR2 = ['伝票NO', '型番', '品名', '取引先ID', '仕入単価', '良品数', '不良品数', '入庫日'];
    const fd2 = new FormData();
    fd2.append('file', new Blob([neCsvOf([HDR2, ['AR-MIG', 'deaditem', '休眠', '0001', '200', '40', '0', '2026/07/12']])]), 'lz-mig.csv');
    r = await j('/api/inbound/import', { method: 'POST', body: fd2 });
    ok(r.body.ok && r.body.newItems === 1, '入庫CSV取込 (移行PO消込用)', r.body);
    r = await j('/api/inbound');
    const inbM = r.body.open.find(x => x.slip === 'AR-MIG');
    r = await j('/api/inbound/' + inbM.id + '/candidates');
    const candM = r.body.candidates.find(c => c.orderItemId === deadItem.id);
    ok(candM && candM.remaining === 100 && candM.suggestedQty === 40, '突合候補に移行POが出る', r.body.candidates && r.body.candidates.length);
    r = await j('/api/items/' + deadItem.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'receipt', qty: 40, inboundItemId: inbM.id,
        remainder: { action: 'await_delivery', nextExpectedDate: '2026-08-15', nextExpectedQty: 60 } }) });
    ok(r.body.ok && r.body.remaining === 60, '割当: 移行PO残100→60', r.body);
  }

  // バリデーション
  r = await upNe('bad1.csv', [NEHDR, neRow('9001', 'x-item', 'x', 10, 20, '2026/7/1', '0001')]);
  ok(r.status === 400 && r.body.error.includes('注残計'), '注残計>発注数は拒否', r.body.error);
  r = await upNe('bad2.csv', [['見出し', '違い'], ['a', 'b']]);
  ok(r.status === 400 && r.body.error.includes('見出し'), '見出し不一致は拒否');
  r = await upNe('bad3.csv', [NEHDR, neRow('9002', 'y-item', 'y', 5, 5, '2026/7/1', '0999')]);
  ok(r.status === 400 && r.body.error.includes('マスタ未登録'), '未登録仕入先は拒否', r.body.error);
  r = await upNe('bad4.csv', [NEHDR, neRow('9003', 'z-item', 'z', 5, 5, '2026/13/45', '0001')]);
  ok(r.status === 400 && r.body.error.includes('発行日'), '不正な発行日は拒否');
  // 同一伝票に別仕入先・別発行日が混在
  r = await upNe('bad5.csv', [NEHDR, neRow('9004', 'a-item', 'a', 5, 5, '2026/7/1', '0001'), neRow('9004', 'b-item', 'b', 5, 5, '2026/7/1', '0002')]);
  ok(r.status === 400 && r.body.error.includes('仕入先cdが混在'), '同一伝票の仕入先混在は拒否');
  r = await upNe('bad6.csv', [NEHDR, neRow('9010', 'a-item', 'a', 5, 5, '2026/7/1', '0001'), neRow('9010', 'b-item', 'b', 5, 5, '2026/7/2', '0001')]);
  ok(r.status === 400 && r.body.error.includes('発行日が混在'), '同一伝票の発行日混在は拒否 (伝票ヘッダ属性)');
  // 注残0の行はスキップ (発注残エクスポートに通常含まれないが防御)。スキップ行も重複検証の対象
  r = await upNe('warn1.csv', [NEHDR, neRow('9005', 'noflyersticker', 'x', 10, 0, '2026/7/1', '0001'), neRow('9006', 'deaditem', 'y', 5, 5, '2026/7/1', '0001')]);
  ok(r.body.ok && r.body.orders === 1 && r.body.warnings.some(w => w.includes('注残0')), '注残0行はスキップ+警告', r.body.warnings);
  r = await upNe('bad7.csv', [NEHDR, neRow('9011', 'c-item', 'c', 5, 0, '2026/7/1', '0001'), neRow('9011', 'c-item', 'c', 5, 5, '2026/7/1', '0001')]);
  ok(r.status === 400 && r.body.error.includes('重複'), '注残0でスキップされる行も重複検証の対象');

  // 移行PO属性の列間規則トリガ (直接SQLでも守られる)
  {
    let thr = null;
    try { db.prepare("INSERT INTO po_orders (supplier_code, supplier_name, status, created_at, updated_at, origin, send_blocked) VALUES ('1','x','draft',?,?,'migration',1)").run(nowIsoStr(), nowIsoStr()); } catch (e) { thr = e.message; }
    ok(thr && thr.includes('origin rules'), 'トリガ: ne_slip_numberなしの移行POは拒否', thr);
    thr = null;
    try { db.prepare("INSERT INTO po_orders (supplier_code, supplier_name, status, created_at, updated_at, ne_slip_number) VALUES ('1','x','draft',?,?,'99999')").run(nowIsoStr(), nowIsoStr()); } catch (e) { thr = e.message; }
    ok(thr && thr.includes('origin rules'), 'トリガ: originなしのne_slip_numberは拒否', thr);
    thr = null;
    try { db.prepare("INSERT INTO po_orders (supplier_code, supplier_name, status, created_at, updated_at, origin, send_blocked, ne_slip_number) VALUES ('1','x','draft',?,?,'other',1,'99998')").run(nowIsoStr(), nowIsoStr()); } catch (e) { thr = e.message; }
    ok(thr && thr.includes('origin rules'), 'トリガ: origin不正値は拒否', thr);
  }

  // 作業中draftがある仕入先の取込は拒否 (取込用一時draftとUNIQUE衝突するため)
  r = await j('/api/supplier/1/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 1 }] }) });
  ok(r.body.ok, 'draft作成 (競合テスト用)');
  r = await upNe('ne2.csv', [NEHDR, neRow('9100', 'noflyersticker', 'x', 5, 5, '2026/7/1', '0001')]);
  ok(r.status === 400 && r.body.error.includes('draft'), 'draftのある仕入先の取込は拒否 (プレビューでも検知)', r.body.error);
  db.prepare("DELETE FROM po_orders WHERE status='draft' AND supplier_code='1'").run();

  // 発注残ページに取込UIが配信される
  const boHtml = await (await fetch(base + '/backorders')).text();
  ok(boHtml.includes('NE発注残の初期取込') && boHtml.includes('neImpPrev') && boHtml.includes('NE移行分'), '/backorders 取込UI配信');
}

// ═══ 追加発注 (supplement: 確定後の電話等の口頭追加・増量分) ═══
console.log('── 追加発注 (supplement) ──');
{
  const jp = (p, body, key) => j(p, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) }, body: JSON.stringify(body) });
  r = await jp('/api/supplier/1/issue', { items: [{ code: 'noflyersticker', qty: 5 }] });
  const parentId = r.body.id, parentPo = r.body.poNumber;

  // 追加発注 (メールなし=既定): 新商品+既存商品の増量を1つの追加POに
  r = await jp('/api/orders/' + parentId + '/supplement',
    { items: [{ code: 'cardstand-silver-r', qty: 4 }, { code: 'noflyersticker', qty: 3 }], note: '7/13 電話 芦田様', sendEmail: false }, 'sup-key-1');
  ok(r.body.ok && r.body.poNumber, 'supplement: 追加発注を発行', r.body);
  const supId = r.body.id;
  const supRow = db.prepare('SELECT * FROM po_orders WHERE id=?').get(supId);
  ok(supRow.origin === 'supplement' && supRow.parent_order_id === parentId && supRow.send_blocked === 1 &&
    supRow.status === 'issued' && supRow.tracking_mode === 'tracked', 'supplement: origin/parent/send_blocked=1/issued', supRow.origin);
  ok(supRow.note.includes(parentPo) && supRow.note.includes('7/13 電話 芦田様'), 'supplement: noteに親PO+メモ', supRow.note);

  // 発注残に残数付きで載り、親子が相互に見える
  r = await j('/api/backorders');
  const supBo = r.body.orders.find(x => x.id === supId);
  const parBo = r.body.orders.find(x => x.id === parentId);
  ok(supBo && supBo.remainingQty === 7 && supBo.origin === 'supplement' && supBo.parentPoNumber === parentPo,
    'supplement: 発注残に残7で載る+親PO参照', supBo && [supBo.remainingQty, supBo.parentPoNumber]);
  ok(parBo && parBo.supplementPoNumbers.includes(supRow.po_number), 'supplement: 親側に追加PO一覧', parBo && parBo.supplementPoNumbers);

  // 冪等: 同一キー再送は同じPO
  r = await jp('/api/orders/' + parentId + '/supplement',
    { items: [{ code: 'cardstand-silver-r', qty: 4 }, { code: 'noflyersticker', qty: 3 }], note: '7/13 電話 芦田様', sendEmail: false }, 'sup-key-1');
  ok(r.body.ok && r.body.id === supId && r.body.replay === true, 'supplement: 同一キー再送はreplay (二重発行なし)');

  // メールなし分は未送信バッジ対象外+preview拒否
  r = await j('/api/cycle-issued');
  const cyc1 = (r.body.suppliers || []).find(s => s.code === '1');
  ok(!cyc1 || !(cyc1.unsentPoNumbers || []).includes(supRow.po_number), 'supplement: メールなし分は📧未送信バッジ対象外');
  r = await j('/api/orders/' + supId + '/email/preview');
  ok(r.status === 400 && r.body.error.includes('送信対象外'), 'supplement: メールなし分はpreview拒否 (send_blocked)');

  // メールあり (sendEmail=true) → send_blocked=0 でpreview可
  r = await jp('/api/orders/' + parentId + '/supplement', { items: [{ code: 'cardstand-silver-r', qty: 6 }], sendEmail: true }, 'sup-key-2');
  ok(r.body.ok, 'supplement: メールあり追加発注');
  const supId2 = r.body.id;
  ok(db.prepare('SELECT send_blocked FROM po_orders WHERE id=?').get(supId2).send_blocked === 0, 'supplement: sendEmail=true は send_blocked=0');
  r = await j('/api/orders/' + supId2 + '/email/preview');
  ok(r.body.ok && r.body.csvText.includes('cardstand-silver-r'), 'supplement: メールあり分はpreview可');

  // バリデーション
  r = await jp('/api/orders/' + parentId + '/supplement', { items: [{ code: 'gyoumuhandcream60-BI', qty: 1 }] }, 'sup-key-3');
  ok(r.status === 400 && r.body.error.includes('仕入先が一致しない'), 'supplement: 他仕入先の商品は拒否');
  r = await jp('/api/orders/' + parentId + '/supplement', { items: [] }, 'sup-key-4');
  ok(r.status === 400, 'supplement: 明細空は拒否');
  r = await jp('/api/orders/999999/supplement', { items: [{ code: 'noflyersticker', qty: 1 }] }, 'sup-key-5');
  ok(r.status === 400 && r.body.error.includes('見つかりません'), 'supplement: 親不存在は拒否');
  // 下書きカートがあると拒否 (黙って消さない)
  r = await jp('/api/supplier/1/draft', { items: [{ code: 'noflyersticker', qty: 1 }] });
  ok(r.body.ok, 'supplement: 競合テスト用draft作成');
  r = await jp('/api/orders/' + parentId + '/supplement', { items: [{ code: 'noflyersticker', qty: 1 }] }, 'sup-key-6');
  ok(r.status === 400 && r.body.error.includes('下書き'), 'supplement: draft存在時は拒否 (カート破壊防止)', r.body.error);
  db.prepare("DELETE FROM po_orders WHERE status='draft' AND supplier_code='1'").run();

  // 直接SQLでもトリガが防御
  let thr = null;
  try { db.prepare("INSERT INTO po_orders (supplier_code, supplier_name, status, origin, created_at, updated_at) VALUES ('1','x','draft','foo',?,?)").run(nowIsoStr(), nowIsoStr()); } catch (e) { thr = e.message; }
  ok(thr && thr.includes('origin rules'), 'trigger: 不正originは拒否', thr);
  thr = null;
  try { db.prepare("INSERT INTO po_orders (supplier_code, supplier_name, status, origin, created_at, updated_at) VALUES ('1','x','draft','supplement',?,?)").run(nowIsoStr(), nowIsoStr()); } catch (e) { thr = e.message; }
  ok(thr && thr.includes('parent_order_id が必須'), 'trigger: 親なしsupplementは拒否', thr);
  thr = null;
  try { db.prepare("INSERT INTO po_orders (supplier_code, supplier_name, status, parent_order_id, created_at, updated_at) VALUES ('1','x','draft',1,?,?)").run(nowIsoStr(), nowIsoStr()); } catch (e) { thr = e.message; }
  ok(thr && thr.includes('追加発注POのみ'), 'trigger: originなしparent_order_idは拒否', thr);

  // 整合性検査: 正常な supplement は origin規則違反として報告されない (Codex sup-R1 Medium)
  r = await j('/api/ledger/integrity');
  ok(r.body.ok && !(r.body.issues || []).some(x => x.kind === 'migration_attrs'),
    'supplement: 整合性検査で正常なsupplementは違反にならない', (r.body.issues || []).filter(x => x.kind === 'migration_attrs'));

  // ページに➕UI
  const boSup = await (await fetch(base + '/backorders')).text();
  ok(boSup.includes('data-supplyui') && boSup.includes('supPanel') && boSup.includes('追加発注を確定'), '/backorders ➕追加発注UI配信');
}

// ═══ FBAジョブ完了検知 (発注サイクルリセットの冪等性) ═══
console.log('── FBAジョブ完了検知 (A→B→A) ──');
{
  const { markCycleFbaJobDone } = await imp('apps/purchase-orders/ledger.js');
  ok(markCycleFbaJobDone('job-A', 'smoke') === true, 'ジョブA完了: 初回はサイクルリセット');
  ok(markCycleFbaJobDone('job-B', 'smoke') === true, 'ジョブB完了: 別ジョブもリセット');
  const afterB = db.prepare("SELECT value FROM po_settings WHERE key='po_cycle_reset_at'").get().value;
  ok(markCycleFbaJobDone('job-A', 'smoke') === false, 'ジョブA再ポーリング: 検知済みはリセットしない (A→B→A)');
  const afterA2 = db.prepare("SELECT value FROM po_settings WHERE key='po_cycle_reset_at'").get().value;
  ok(afterB === afterA2, '再検知で po_cycle_reset_at は動かない');
}

// ═══ ダッシュボード×非表示のサーバ保存 + 営業日自動リセット廃止 (中原さん要望 2026-07-13) ═══
console.log('── ダッシュボード非表示 (サーバ保存/サイクル失効) + サイクルの営業日非依存 ──');
{
  const { setSetting: setL } = await imp('apps/purchase-orders/ledger.js');
  const jp = body => j('/api/dashboard/hidden', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });

  // 非表示にする → サーバに保存され、ダッシュボードHTMLに埋め込まれる (PC間共有の実体)
  r = await jp({ code: '0001', hidden: true });
  ok(r.body.ok && r.body.codes.includes('1'), 'hidden: 非表示保存 (0001→1正規化)', r.body);
  let dashHtml = await (await fetch(base + '/')).text();
  ok(dashHtml.includes('["1"].forEach'), 'hidden: ダッシュボードに非表示リスト埋め込み');
  ok(!dashHtml.includes('po_dash_dis'), 'hidden: 旧localStorage方式は廃止');
  ok(dashHtml.includes('発注サイクル:'), 'hidden: サイクル開始表示あり');

  // バリデーション
  r = await jp({ code: '9999', hidden: true });
  ok(r.status === 400 && r.body.error.includes('未登録'), 'hidden: 未登録仕入先は400');
  r = await jp({ code: '1' });
  ok(r.status === 400, 'hidden: hidden/clear指定なしは400');
  r = await jp({});
  ok(r.status === 400, 'hidden: コードなしは400');

  // 戻す
  r = await jp({ code: '1', hidden: false });
  ok(r.body.ok && r.body.codes.length === 0, 'hidden: ↩戻す');

  // サイクル失効: 非表示 → データ更新 (サイクルbump) → 自動で無効化 (明示クリア不要の導出方式)
  r = await jp({ code: '1', hidden: true });
  ok(r.body.ok && r.body.codes.includes('1'), 'hidden: 再度非表示');
  setL('po_cycle_reset_at', nowIsoStr(), { actor: 'smoke', reason: 'テスト: データ更新相当' });
  dashHtml = await (await fetch(base + '/')).text();
  ok(dashHtml.includes('[].forEach'), 'hidden: サイクルが進むと自動失効 (データ更新でリセット)');
  r = await jp({ code: '1', hidden: true });
  r = await jp({ clear: true });
  ok(r.body.ok && r.body.codes.length === 0, 'hidden: clear (全て戻す)');

  // 営業日 (as_of) では発注確定サイクルはもうリセットされない: as_ofを未来日にしてもcycle-issuedが変わらない
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 2 }] }) });
  ok(r.body.ok !== false && r.body.id, 'サイクル: テスト用発注確定', r.body);
  r = await j('/api/cycle-issued');
  ok(r.body.ok && r.body.suppliers.some(s => s.code === '1'), 'サイクル: 確定直後は載る', r.body.suppliers);
  const asOfBefore = db.prepare('SELECT as_of_date FROM mirror_pml_published WHERE id=1').get().as_of_date;
  const tomorrow = new Date(Date.now() + 86400000).toISOString().slice(0, 10);
  db.prepare('UPDATE mirror_pml_published SET as_of_date=? WHERE id=1').run(tomorrow);
  r = await j('/api/cycle-issued');
  ok(r.body.ok && r.body.suppliers.some(s => s.code === '1'),
    'サイクル: 営業日が変わっても (朝の自動同期でも) ✅発注確定済みは消えない — リセットはデータ更新ボタンのみ', r.body.suppliers);
  db.prepare('UPDATE mirror_pml_published SET as_of_date=? WHERE id=1').run(asOfBefore);

  // 発注残ページ: 仕入先名はワークスペースへのリンクではなくなった (明細が消えたと誤解しない導線)
  const boHtml2 = await (await fetch(base + '/backorders')).text();
  ok(boHtml2.includes('新しい発注作業') && boHtml2.includes('クリックでこの発注の明細'), '/backorders: 明細展開が主導線+ワークスペースは明示リンク');
}

// ═══ 注残SSoT (正本=アプリ台帳。NE由来の注残数はlegacy) — 2026-07-13 データ契約 ═══
console.log('── 注残SSoT (正本ビュー/GAS endpoint差替/商品管理リスト/契約テスト) ──');
{
  const { setSetting: setL2 } = await imp('apps/purchase-orders/ledger.js');
  // NE由来の注残数にゾンビ値を入れても、正本ビューは台帳値を返す
  db.prepare("UPDATE mirror_pml_snapshot_rows SET 注残数=999 WHERE 商品コード='noflyersticker'").run();
  const ledgerZan = db.prepare("SELECT backorder_qty FROM v_ledger_backorder_by_product WHERE product_key='noflyersticker'").get();
  ok(ledgerZan && ledgerZan.backorder_qty > 0, 'SSoT: v_ledger_backorder_by_product に台帳注残', ledgerZan);
  const authRow = db.prepare("SELECT 注残数, ne_backorder_qty, backorder_source FROM v_pml_rows_authoritative WHERE 商品コード='noflyersticker'").get();
  ok(authRow && authRow.注残数 === ledgerZan.backorder_qty && authRow.ne_backorder_qty === 999 && authRow.backorder_source === 'app',
    'SSoT: v_pml_rows_authoritative の注残数=台帳値 (NEゾンビ値999は ne_backorder_qty に隔離)', authRow);
  // loadLedgerBackorders もビュー経由 (集計一本化)
  const { loadLedgerBackorders: llb } = await imp('apps/purchase-orders/logic.js');
  ok(llb().get('noflyersticker') === ledgerZan.backorder_qty, 'SSoT: loadLedgerBackorders はビューと同値');

  // GAS向け /api/pml/published: 注残数差替+checksum再計算+由来表示
  const gasRes = await fetch(siteBase + '/apps/warehouse-mirror/api/pml/published', { headers: { 'x-read-token': 'smoke-pml-token' } });
  const gas = await gasRes.json();
  ok(gas.ok === true && gas.backorder_source === 'app_ledger', 'SSoT: GAS endpoint は backorder_source=app_ledger', gas.backorder_source);
  const gasRow = gas.rows.find(r => r.商品コード === 'noflyersticker');
  ok(gasRow && gasRow.注残数 === ledgerZan.backorder_qty && gasRow.注残数 !== 999, 'SSoT: GAS行の注残数=台帳値', gasRow && gasRow.注残数);
  {
    const canonical = gas.rows.map(r => gas.columns.map(c => r[c] == null ? '' : String(r[c])).join('\t')).join('\n');
    const recomputed = createHash('sha256').update(canonical).digest('hex');
    ok(recomputed === gas.payload_checksum, 'SSoT: GAS checksum は差替後の行と一致 (GAS側検証が通る)');
  }
  const noTok = await fetch(siteBase + '/apps/warehouse-mirror/api/pml/published');
  ok(noTok.status === 401, 'SSoT: GAS endpoint はトークン必須のまま');

  // 商品管理リスト: 画面に由来表示+CSVも台帳値
  const pmlHtml = await (await fetch(siteBase + '/apps/product-management-list/')).text();
  ok(pmlHtml.includes('発注アプリの台帳'), 'SSoT: 商品管理リスト画面に注残の由来表示');
  const csvBuf = Buffer.from(await (await fetch(siteBase + '/apps/product-management-list/export.csv')).arrayBuffer());
  const csvTxt = iconv.decode(csvBuf, 'Shift_JIS');
  const csvLine = csvTxt.split('\r\n').find(l => l.startsWith('noflyersticker,'));
  ok(csvLine && csvLine.split(',')[11] === String(ledgerZan.backorder_qty), 'SSoT: 商品管理リストCSVの注残数=台帳値', csvLine && csvLine.split(',')[11]);

  // 緊急ロールバック: backorder_source='ne' でGAS/商品管理リストの両経路がNE由来値に戻る
  setL2('backorder_source', 'ne', { actor: 'smoke', reason: 'テスト' });
  const gasNe = await (await fetch(siteBase + '/apps/warehouse-mirror/api/pml/published', { headers: { 'x-read-token': 'smoke-pml-token' } })).json();
  const gasNeRow = gasNe.rows.find(r => r.商品コード === 'noflyersticker');
  ok(gasNe.backorder_source === 'ne_legacy' && gasNeRow.注残数 === 999, 'SSoT: ロールバック時GASはNE由来値 (999) に戻る');
  {
    const canonicalNe = gasNe.rows.map(r2 => gasNe.columns.map(c => r2[c] == null ? '' : String(r2[c])).join('\t')).join('\n');
    ok(createHash('sha256').update(canonicalNe).digest('hex') === gasNe.payload_checksum, 'SSoT: ロールバック時もchecksum一致 (Codex SSoT-R1 Low)');
  }
  const pmlNeHtml = await (await fetch(siteBase + '/apps/product-management-list/')).text();
  ok(pmlNeHtml.includes('緊急ロールバック中'), 'SSoT: ロールバック時はリスト画面にNE由来警告');
  {
    const csvNe = iconv.decode(Buffer.from(await (await fetch(siteBase + '/apps/product-management-list/export.csv')).arrayBuffer()), 'Shift_JIS');
    const lineNe = csvNe.split('\r\n').find(l => l.startsWith('noflyersticker,'));
    ok(lineNe && lineNe.split(',')[11] === '999', 'SSoT: ロールバック時はリストCSVもNE由来値 (999)', lineNe && lineNe.split(',')[11]);
  }
  setL2('backorder_source', 'app', { actor: 'smoke', reason: 'テスト戻し' });

  // 整合性検査: 台帳注残がPML商品にJOINできないと警告
  const savedRow = db.prepare("SELECT * FROM mirror_pml_snapshot_rows WHERE 商品コード='cardstand-silver-r'").get();
  db.prepare("DELETE FROM mirror_pml_snapshot_rows WHERE 商品コード='cardstand-silver-r'").run();
  r = await j('/api/ledger/integrity');
  ok((r.body.warnings || []).some(w => w.kind === 'backorder_not_in_pml' && w.productKey === 'cardstand-silver-r'),
    'SSoT: PMLに無い台帳注残は警告 (backorder_not_in_pml)', (r.body.warnings || []).filter(w => w.kind === 'backorder_not_in_pml'));
  insRow.run(savedRow.商品コード, savedRow.商品名, savedRow.仕入先, savedRow.取扱区分, savedRow.売上分類, savedRow.総在庫数_引当なし,
    savedRow.注残数, savedRow.販売数7日_合計, savedRow.販売数30日_合計, savedRow.発注ロット単位, savedRow.推奨保有月数,
    savedRow.売価, savedRow.原価, savedRow.最終仕入日, savedRow.登録日);
  db.prepare("UPDATE mirror_pml_snapshot_rows SET 注残数=0 WHERE 商品コード='noflyersticker'").run(); // ゾンビ値を戻す

  // 静的契約テスト: 許可リスト外のコードが mirror_pml_snapshot_rows の注残数を直接参照していないこと
  // (docs/contracts/pml_backorder_authority.contract.md。将来のアプリ/AIが誤って古いNE注残を読む事故の防波堤)
  {
    // 許可はできるだけファイル単位 (ディレクトリ丸ごと除外は miniPC側の apps/warehouse のみ、Codex SSoT-R1 Medium)
    const ALLOW = [
      'apps/warehouse/',                          // miniPC側 (raw/snapshot生成元。Render配信対象外)
      'apps/warehouse-mirror/db.js',              // mirror DDL (契約コメントの置き場)
      'apps/warehouse-mirror/router.js',          // ingest検証+GAS endpoint差替の実装そのもの
      'apps/purchase-orders/db.js',               // 正本ビュー定義
      'apps/purchase-orders/logic.js',            // NEオーバーレイ/override実装
      'apps/product-management-list/router.js',   // ロールバックfallback
      'apps/purchase-orders/scripts/',            // テスト自身
    ];
    const scanOne = p => {
      // コメント行 (// や * 始まり) は除外して実コードだけを検査する
      const src = fs.readFileSync(p, 'utf8').split('\n')
        .filter(l => { const t = l.trim(); return !t.startsWith('//') && !t.startsWith('*') && !t.startsWith('/*'); })
        .join('\n');
      return src.includes('mirror_pml_snapshot_rows') && /注残数|発注残数/.test(src);
    };
    const offenders = [];
    const walk = dir => {
      for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, ent.name);
        if (ent.isDirectory()) { if (ent.name !== 'node_modules') walk(p); continue; }
        if (!/\.(js|mjs|cjs)$/.test(ent.name)) continue;
        const rel = path.relative(WORK, p).replace(/\\/g, '/');
        // ディレクトリ項目 (末尾/) は前方一致、ファイル項目は完全一致 (xxx.js.backup.cjs 等のすり抜け防止、Codex SSoT-R2)
        if (ALLOW.some(a => a.endsWith('/') ? rel.startsWith(a) : rel === a)) continue;
        if (scanOne(p)) offenders.push(rel);
      }
    };
    walk(path.join(WORK, 'apps'));
    ok(offenders.length === 0, '契約: mirror_pml_snapshot_rows の注残数を直接参照するコードなし (許可リスト外)', offenders);
    // 検出器の自己テスト: 違反fixtureを一時作成して確実にFAIL側へ倒れることを確認 (ガード自身の回帰防止)
    const fixture = path.join(WORK, 'apps', '_smoke_contract_fixture.js');
    try {
      fs.writeFileSync(fixture, "const q = db.prepare('SELECT 注残数 FROM mirror_pml_snapshot_rows');\n");
      ok(scanOne(fixture) === true, '契約: 検出器は違反コードを検出できる (自己テスト)');
    } finally { try { fs.unlinkSync(fixture); } catch {} }
  }
}

// ═══ ロジザード入荷予定の作成 (出荷明細変換) + 未紐付け先方番号の仮登録 ═══
console.log('── 入荷予定変換 (inbound-plan) + 仮登録 (pending) ──');
{
  const jp2 = (p, body) => j(p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  // AMC出荷明細フォーマット (見出し行+備考/倉庫コード0000列あり) をそのまま貼り付け
  const shipText = '商品コード\t商品名\t出荷数量\t備考\t倉庫コード\t摘要\n' +
    'AMC-001\tﾁﾗｼ ｽﾃｯｶｰ\t1,600\t\t0000\t\n' +
    'ZZZ-NEW-1\t新商品X\t50\t\t0000\t\n' +
    'BADLINE-NO-QTY\t数量なし行\t\t\t\t\n';
  r = await jp2('/api/inbound-plan/convert', { supplier_code: '0001', text: shipText });
  ok(r.body.ok && r.body.rowCount === 1 && r.body.totalQty === 1600, 'convert: 対応表逆引きで変換 (カンマ数量/見出し行/0000列を処理)', r.body);
  ok(r.body.pasteText === 'noflyersticker\t1600\t70', 'convert: 貼り付けデータ=商品ID/入荷予定数/仕入単価(PML原価)', r.body.pasteText);
  ok(r.body.unmatched.length === 1 && r.body.unmatched[0].vendorCode === 'ZZZ-NEW-1' && r.body.unmatched[0].vendorName === '新商品X',
    'convert: 対応表に無い番号はunmatched (先方商品名付き)', r.body.unmatched);
  ok(r.body.skipped.length === 1, 'convert: 数量が読めない行はskipped');

  // 仮登録 (再実行は refreshed)
  r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: r.body.unmatched });
  ok(r.body.ok && r.body.added === 1, 'pending: 仮登録', r.body);
  r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-NEW-1', vendorName: '新商品X', qty: 60 }] });
  ok(r.body.ok && r.body.refreshed === 1 && r.body.added === 0, 'pending: 再登録はrefreshed (UNIQUE upsert)');
  r = await j('/api/vendor-map/pending?supplier=0001');
  const pend1 = r.body.rows.find(x => x.vendor_code === 'ZZZ-NEW-1');
  ok(pend1 && pend1.vendor_name === '新商品X' && pend1.last_qty === 60, 'pending: 一覧に載る (直近数量更新)', pend1);

  // 紐づけ: PMLに無い商品/他仕入先はエラー、正しい商品で対応表へ昇格
  r = await jp2('/api/vendor-map/pending/' + pend1.id + '/link', { product_code: 'not-in-pml-zzz' });
  ok(r.status === 400 && r.body.error.includes('商品マスタに無い'), 'pending link: PMLに無い商品は拒否 (NE登録翌朝を案内)', r.body.error);
  r = await jp2('/api/vendor-map/pending/' + pend1.id + '/link', { product_code: 'gyoumuhandcream60-BI' });
  ok(r.status === 400 && r.body.error.includes('仕入先'), 'pending link: 他仕入先の商品は拒否');
  r = await jp2('/api/vendor-map/pending/' + pend1.id + '/link', { product_code: 'cardstand-silver-r' });
  ok(r.body.ok === true, 'pending link: 紐づけ成功');
  const mapRow = db.prepare("SELECT * FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='cardstand-silver-r'").get();
  ok(mapRow && mapRow.vendor_code === 'ZZZ-NEW-1', 'pending link: 対応表へ昇格', mapRow && mapRow.vendor_code);
  ok(db.prepare('SELECT status, linked_product_code FROM po_vendor_code_pending WHERE id=?').get(pend1.id).status === 'linked', 'pending link: status=linked');
  r = await jp2('/api/vendor-map/pending/' + pend1.id + '/link', { product_code: 'cardstand-silver-r' });
  ok(r.status === 400 && r.body.error.includes('処理済み'), 'pending link: 二重処理は拒否');

  // 紐づけ後の再変換: 同じ出荷明細が全行マッチ
  r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: shipText });
  ok(r.body.ok && r.body.rowCount === 2 && r.body.unmatched.length === 0, 'convert: 紐づけ後は全行マッチ (E2E)', r.body.rowCount);

  // 破棄
  r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-MISTAKE', qty: 1 }] });
  r = await j('/api/vendor-map/pending?supplier=1');
  const pend2 = r.body.rows.find(x => x.vendor_code === 'ZZZ-MISTAKE');
  r = await jp2('/api/vendor-map/pending/' + pend2.id + '/dismiss', {});
  ok(r.body.ok, 'pending: 破棄');
  r = await j('/api/vendor-map/pending?supplier=1');
  ok(!r.body.rows.some(x => x.vendor_code === 'ZZZ-MISTAKE'), 'pending: 破棄後は一覧に出ない');
  // 破棄済みは再登録で復活しない (Codex plan-R1 Medium)
  r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-MISTAKE', qty: 2 }] });
  ok(r.body.ok && r.body.skippedProcessed === 1 && r.body.added === 0, 'pending: 破棄済みの再登録はスキップ (復活しない)', r.body);
  r = await j('/api/vendor-map/pending?supplier=1');
  ok(!r.body.rows.some(x => x.vendor_code === 'ZZZ-MISTAKE'), 'pending: スキップ後も一覧に出ない');
  // 大小文字違いはバッチ内でも既存とも1件に正規化 (Codex plan-R1 Medium)
  r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'zzz-case', qty: 1 }, { vendorCode: 'ZZZ-CASE', qty: 2 }] });
  ok(r.body.ok && r.body.added === 1, 'pending: 大小文字違いは1件 (norm一意)', r.body);
  // 既に別商品へ登録済みの番号 (大小文字違い) への紐づけは拒否 (逆引き曖昧の発生防止)
  r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'amc-001', qty: 1 }] });
  r = await j('/api/vendor-map/pending?supplier=1');
  const pendDup = r.body.rows.find(x => x.vendor_code === 'amc-001');
  r = await jp2('/api/vendor-map/pending/' + pendDup.id + '/link', { product_code: '0726-001060' });
  ok(r.status === 400 && r.body.error.includes('既に商品'), 'pending link: 同じ番号が別商品に登録済みなら拒否 (ambiguous防止)', r.body.error);
  r = await jp2('/api/vendor-map/pending/' + pendDup.id + '/dismiss', {});

  // 同じ先方番号が複数商品に対応 → ambiguous (黙ってどちらかに変換しない)
  db.prepare("INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES ('1','0726-001060','0726-001060','ZZZ-NEW-1',?)").run(nowIsoStr());
  r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: 'ZZZ-NEW-1\t新商品X\t10\n' });
  ok(r.body.ok && r.body.ambiguous.length === 1 && r.body.rowCount === 0, 'convert: 逆引き曖昧はambiguous (変換しない)', r.body.ambiguous);
  // 手動upsertで同番号を別商品に付けると警告 (ブロックはしない、Codex plan-R2)
  r = await jp2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'noflyersticker', vendor_code: 'zzz-new-1' });
  ok(r.body.ok && r.body.warning && r.body.warning.includes('曖昧'), 'entry: 同番号の別商品登録は警告 (大小文字違いも検知)', r.body.warning);
  r = await jp2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'noflyersticker', vendor_code: 'AMC-001' }); // 戻す
  ok(r.body.ok && !r.body.warning, 'entry: 重複解消後は警告なし');
  db.prepare("DELETE FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='0726-001060'").run();

  // 画面配信
  const inbHtml2 = await (await fetch(base + '/inbound')).text();
  ok(inbHtml2.includes('ロジザード入荷予定の作成') && inbHtml2.includes('ipConvert'), '/inbound 入荷予定作成セクション');
  const adminHtml3 = await (await fetch(base + '/admin')).text();
  ok(adminHtml3.includes('loadVmapPending') && adminHtml3.includes('未紐付けの先方番号'), '/admin 対応表タブに仮登録リスト');
}

server.close();
console.log(`\n=== RESULT: pass=${pass} fail=${fail} ===`);
process.exit(fail ? 1 : 0);
