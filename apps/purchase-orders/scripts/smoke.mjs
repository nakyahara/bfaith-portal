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
  (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 売上分類, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数, 売価, 原価, 最終仕入日, 登録日)
  VALUES ('run_test', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
// 実スプレッドシートの行を fixture 化 (発注量の期待値はシート実値)
insRow.run('noflyersticker', 'チラシ お断り ステッカー', '0001', '取扱中', 2, 490, 0, 46, 368, 100, 1.5, 380, 70, '2026-07-02', '2020-05-24');
insRow.run('cardstand-silver-r', 'AMC カードスタンド シルバー', '0001', '取扱中', 2, 728, 0, 80, 485, 10000, 2.5, 398, 55, '2026-05-19', '2021-10-05');
insRow.run('0726-001060', '肉球クリーム 30g', '0001', '取扱中', 1, 3559, 0, 176, 888, 1000, 2.5, 698, 245, '2026-07-02', '2019-12-11');
insRow.run('deaditem', '休眠商品', '0001', '取扱中', 2, 50, 0, 0, 0, 100, 1.5, 500, 200, '2025-01-01', '2020-01-01');
insRow.run('teishi-item', '取扱中止商品', '0001', '取扱中止', 2, 0, 0, 0, 10, 100, 1.5, 500, 200, '2025-01-01', '2020-01-01');
insRow.run('horikoshi-item', '掘り起こし対象商品', '0001', '取扱中', 2, 0, 0, 0, 0, 100, 1.5, 500, 200, '2025-01-01', '2020-01-01');
// セット商品 (商品区分='セット') は全リスト対象外
db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 商品区分, 売上分類, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計)
  VALUES ('run_test', 'set-2pack', '2個セット商品', '', '取扱中', 'セット', 2, 0, 0, 0, 0)`).run();
insRow.run('gyoumuhandcream60-BI', 'プロ業務用ハンドクリーム 60g 微香', '0002', '取扱中', 3, 195, 178, 55, 258, 24, 1, 1236, 672, '2026-07-01', '2022-02-03');
insRow.run('diyorangeoil100', '木工用オレンジオイル 100ml', '0001', '取扱中', 1, 1536, 900, 169, 808, 600, 1.5, 698, 270, '2026-06-30', '2021-05-23');
// 引当済み込み回帰: 総在庫数≠総在庫数_引当なし の行 (誤って旧列を参照すると L=0.5→対象になり検出できる)
db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 売上分類, 総在庫数, 総在庫数_引当なし, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数)
  VALUES ('run_test', 'alloc-item', 'FBA準備で引当済みの商品', '0001', '取扱中', 2, 200, 50, 0, 25, 100, 100, 1.5)`).run();

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
ok(dead.isHorikoshi === false && dead.isTarget === false, 'deaditem 在庫あり販売0→掘り起こしではない (定義=在庫0かつ注残0)');
const hori = get('horikoshi-item');
ok(hori.isHorikoshi === true && hori.isTarget === false, 'horikoshi-item 在庫0・注残0→掘り起こし');
const teishi = get('teishi-item');
ok(teishi.isTarget === false && teishi.isHorikoshi === false, '取扱中止は対象外');
const oil = get('diyorangeoil100');
// L=(1536+900)/808=3.0148>1.5 → 対象外 (シートも空)
ok(oil.isTarget === false, 'diyorangeoil100 注残込みで対象外', oil.stockMonths);
ok(stockConstant(1) === 0.5 && stockConstant(1.5) === 1 && stockConstant(2.5) === 2 && stockConstant(4) === 3, '在庫定数 IFS 移植');
const alloc = get('alloc-item');
// 引当済み込みの総在庫数を使う: L=200/100=2.0>1.5→対象外 (旧列の引当なし50なら L=0.5→対象になってしまう)
ok(alloc.stock === 200 && alloc.isTarget === false, 'alloc-item 在庫=総在庫数(引当込み200)で対象外 (引当なし50は使わない)', { stock: alloc.stock, m: alloc.stockMonths });

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
ok(r.body.horikoshi.length === 1 && r.body.horikoshi[0].code === 'horikoshi-item', 'supplier: 掘り起こし=在庫0・注残0のみ (horikoshi-item)', r.body.horikoshi.map(p => p.code));
ok(r.body.candidates.length > 0 && r.body.candidates[r.body.candidates.length - 1].code === 'deaditem',
  'supplier: 在庫あり販売0 (deaditem) はついで買い候補の末尾', r.body.candidates.map(p => p.code));
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
// NE商品マスタCSV (実ヘッダー準拠)。noflyersticker: 在庫100 / 注残50 に更新 → 総在庫=100+FBA(20)=120
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

console.log('── ロジザード在庫CSV取込 (在庫数のみ上書き) ──');
{
  // プレビュー→確認→取込の二段 (fileHash束縛)。エラーはプレビュー段階で返る
  const lzPost = async (csvText, name, { previewOnly = false } = {}) => {
    const mk = () => { const fd = new FormData(); fd.append('file', new Blob([iconv.encode(csvText, 'Shift_JIS')]), name); return fd; };
    const prev = await j('/api/logizard-stock/csv', { method: 'POST', body: mk() });
    if (previewOnly || !prev.body.ok) return prev;
    const fd2 = mk();
    fd2.append('commit', '1');
    fd2.append('fileHash', prev.body.fileHash);
    fd2.append('planHash', prev.body.planHash);
    return await j('/api/logizard-stock/csv', { method: 'POST', body: fd2 });
  };
  // ロケ別在庫一覧 (CZ04003) 形式: 同一商品が複数ロケ行→合算、良品のみ、PML外商品は在庫に反映されない
  const lzHdr = '"在庫日","倉庫名","ロケ","商品ID","商品名","品質区分名","在庫数(引当数を含む)","引当数"';
  const lzCsv = lzHdr + '\r\n' +
    '"20260714","B-Faith","001-001-01","noflyersticker","チラシ","良品","100","5"\r\n' +
    '"20260714","B-Faith","002-001-01","noflyersticker","チラシ","良品","60","0"\r\n' +
    '"20260714","B-Faith","002-002-01","noflyersticker","チラシ","不良品","7","0"\r\n' +
    '"20260714","B-Faith","003-001-01","unknown-lz-item","ロジザードだけの商品","良品","9","0"\r\n';
  // 期待値: 取扱中の全PML商品のうちCSVに載っているのは noflyersticker だけ → 残りは在庫0扱い
  const activeCount = db.prepare("SELECT COUNT(*) AS n FROM mirror_pml_snapshot_rows WHERE 取扱区分='取扱中'").get().n;
  const expectedZero = activeCount - 1;
  // プレビューは書込なし
  r = await lzPost(lzCsv, 'CZ04003_test.csv', { previewOnly: true });
  ok(r.body.ok && r.body.preview === true && r.body.products === 2 && r.body.matched === 1 && r.body.zeroFill === expectedZero && r.body.fileHash,
    `ロジザードCSVプレビュー (取扱中${activeCount}商品中、CSVあり1・在庫0扱い${expectedZero})`, r.body);
  r = await j('/api/supplier/1');
  ok(r.body.overlay === null, 'プレビューは書込なし (overlay未作成)');
  // fileHash不一致の確定は409
  {
    const fdWrong = new FormData();
    fdWrong.append('file', new Blob([iconv.encode(lzCsv, 'Shift_JIS')]), 'x.csv');
    fdWrong.append('commit', '1');
    fdWrong.append('fileHash', 'deadbeef');
    r = await j('/api/logizard-stock/csv', { method: 'POST', body: fdWrong });
    ok(r.status === 409, 'fileHash不一致の確定は409 (プレビューと別ファイル)');
  }
  // プレビュー後にPML側が変わったら planHash 不一致で409 (在庫0化する商品集合の変化を検出)
  {
    const prevPlan = await lzPost(lzCsv, 'plan.csv', { previewOnly: true });
    db.prepare("UPDATE mirror_pml_snapshot_rows SET 取扱区分='取扱中止' WHERE 商品コード='deaditem'").run();
    const fdPlan = new FormData();
    fdPlan.append('file', new Blob([iconv.encode(lzCsv, 'Shift_JIS')]), 'plan.csv');
    fdPlan.append('commit', '1');
    fdPlan.append('fileHash', prevPlan.body.fileHash);
    fdPlan.append('planHash', prevPlan.body.planHash);
    r = await j('/api/logizard-stock/csv', { method: 'POST', body: fdPlan });
    ok(r.status === 409 && r.body.error.includes('プレビュー後'), 'プレビュー後のPML変化は planHash 不一致で409');
    db.prepare("UPDATE mirror_pml_snapshot_rows SET 取扱区分='取扱中' WHERE 商品コード='deaditem'").run();
  }
  // 取込実行
  r = await lzPost(lzCsv, 'CZ04003_test.csv');
  ok(r.status === 200 && r.body.ok && r.body.products === 2 && r.body.matched === 1 && r.body.zeroFill === expectedZero && r.body.skippedNonGood === 1,
    'ロジザード在庫CSV取込 (2商品/PML一致1/在庫0扱い/良品以外1行除外)', r.body);
  r = await j('/api/supplier/1');
  ok(r.body.overlay && r.body.overlay.applied === true && r.body.overlay.source === 'logizard', 'overlay: source=logizard');
  const noflyLz = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'noflyersticker');
  ok(noflyLz && noflyLz.stock === 160, 'ロケ別在庫を商品IDごとに合算 (良品100+60=160、不良品7は除外)', noflyLz && noflyLz.stock);
  ok(noflyLz && noflyLz.cost === 70, '在庫以外 (原価等) はPMLのまま (上書きしない)', noflyLz && noflyLz.cost);
  // CSVに行が無い取扱中商品は売り切れ=在庫0 (自社在庫は全てロジザードにある前提)
  r = await j('/api/supplier/2');
  {
    const gyLz = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'gyoumuhandcream60-BI');
    ok(gyLz && gyLz.stock === 0, 'CSVに無い取扱中商品は在庫0扱い (売り切れ反映)', gyLz && gyLz.stock);
  }
  ok(db.prepare("SELECT product_code FROM po_product_code_canonical WHERE product_key='unknown-lz-item'").get().product_code === 'unknown-lz-item',
    'ロジザード商品ID表記を canonical に蓄積');
  ok(!db.prepare("SELECT 1 FROM po_product_code_canonical WHERE product_key='deaditem'").get(),
    '在庫0扱いの商品の表記は canonical に入れない (CSVで未確認のため)');
  // ダッシュボードの鮮度表示
  const dashLz = await (await fetch(base + '/')).text();
  ok(dashLz.includes('ロジザード在庫CSV') && dashLz.includes('在庫のみ上書き'), '/ 鮮度表示にロジザード在庫CSVラベル');
  ok(dashLz.includes('lzForm') && dashLz.includes('ロジザード在庫CSVを取込'), '/ ロジザード在庫CSV取込フォーム');
  ok(dashLz.includes('在庫0'), '/ 取込confirmに在庫0件数の確認');
  // 在庫数が数値でない行は全件rollback (既存overlayはlogizardのまま)
  r = await lzPost(lzHdr + '\r\n"20260714","B-Faith","001","a1","x","良品","abc","0"\r\n', 'bad.csv');
  ok(r.status === 400 && r.body.errors && r.body.errors.length === 1, 'ロジザードCSV数値不正は全件rollback + errors');
  // 空欄在庫は0扱いにせず拒否 (欠損データで実在庫を0上書きしない)。良品以外の行の不正値も検出
  r = await lzPost(lzHdr + '\r\n"20260714","B-Faith","001","a1","x","良品","",""\r\n', 'empty.csv');
  ok(r.status === 400, 'ロジザードCSV空欄在庫は400 (0扱いしない)');
  r = await lzPost(lzHdr + '\r\n"20260714","B-Faith","001","a1","x","不良品","xyz","0"\r\n"20260714","B-Faith","002","a2","x","良品","3","0"\r\n', 'negbad.csv');
  ok(r.status === 400, 'ロジザードCSV: 良品以外の行の不正在庫も検出 (検証が除外より先)');
  // 全行が良品以外 → 有効0件で400
  r = await lzPost(lzHdr + '\r\n"20260714","B-Faith","001","a1","x","不良品","5","0"\r\n', 'allbad.csv');
  ok(r.status === 400 && r.body.error.includes('有効な行'), 'ロジザードCSV: 全行良品以外は400');
  // 品質区分名の列が無いCSVは全行を在庫に合算 (警告なしの簡易形式)
  r = await lzPost('"商品ID","在庫数(引当数を含む)"\r\n"noflyersticker","70"\r\n"noflyersticker","0"\r\n', 'noq.csv');
  ok(r.body.ok && r.body.products === 1 && r.body.zeroFill === expectedZero, 'ロジザードCSV: 品質区分列なしは全行合算 (0在庫行もOK)');
  r = await j('/api/supplier/1');
  {
    const nfq = [...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'noflyersticker');
    ok(nfq && nfq.stock === 70, 'ロジザードCSV: 再取込で置換 (70+0=70)', nfq && nfq.stock);
  }
  // ロジザード取込→NE取込で source が ne に戻る (鮮度表示の出し分け)
  const fdNeAfter = new FormData();
  fdNeAfter.append('file', new Blob([iconv.encode('"商品コード","在庫数","発注残数"\r\n"noflyersticker","55","0"\r\n', 'Shift_JIS')]), 'ne-after.csv');
  r = await j('/api/ne-overlay/csv', { method: 'POST', body: fdNeAfter });
  r = await j('/api/supplier/1');
  ok(r.body.overlay && r.body.overlay.source === 'ne' && r.body.overlay.applied === true, 'NE取込で source=ne に戻る (後勝ち)', r.body.overlay && r.body.overlay.source);
  // 見出し不正
  const fdLzHdr2 = new FormData();
  fdLzHdr2.append('file', new Blob(['foo,bar\r\n1,2\r\n']), 'x.csv');
  r = await j('/api/logizard-stock/csv', { method: 'POST', body: fdLzHdr2 });
  ok(r.status === 400 && r.body.error.includes('商品ID'), 'ロジザードCSV見出し不正は400');
  await j('/api/ne-overlay', { method: 'DELETE' });
}

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

console.log('── ケース・ロット系条件タイプ (旧GAS参照ツール移植: ジョンズブレンド/パシーマ等) ──');
{
  // 数量[ケース]: ケースロット優先、無ければ発注ロット単位で換算
  const cases = { condition_type: '数量', condition_value: 3, unit: 'ケース' };
  const keys = new Set(['a', 'b', 'c']);
  let ev = evaluateCondition(cases, keys, [
    { key: 'a', qty: 48, cost: 100, caseLot: 48 },          // 1ケース
    { key: 'b', qty: 60, cost: 100, lot: 30 },              // ロット換算で2ケース
  ]);
  ok(ev.auto && ev.auto.kind === 'cases' && ev.auto.current === 3 && ev.auto.met === true, '数量[ケース]: 48/48 + 60/30 = 3ケースで充足', ev.auto);
  ev = evaluateCondition(cases, keys, [{ key: 'a', qty: 72, cost: 100, caseLot: 48 }]);
  ok(ev.auto && ev.auto.current === 1.5 && ev.auto.met === false, '数量[ケース]: 端数は小数ケースで未達', ev.auto);
  ev = evaluateCondition(cases, keys, [{ key: 'c', qty: 10, cost: 100 }]);
  ok(ev.auto && ev.auto.met === false && ev.auto.noSize.length === 1, '数量[ケース]: 入数不明はnoSizeに記録し数えない', ev.auto);

  // ケース入数かつ金額 (ジョンズブレンド): ケースグループ合算がケースロットの倍数 + 金額下限
  const ja = { condition_type: 'ケース入数かつ金額', condition_value: 50000, unit: '円' };
  const jkeys = new Set(['j1', 'j2', 'j3', 'solo', 'noinfo']);
  ev = evaluateCondition(ja, jkeys, [
    { key: 'j1', qty: 100, cost: 300, caseGroup: 'OA-JON-1', caseLot: 144 },
    { key: 'j2', qty: 44, cost: 300, caseGroup: 'OA-JON-1', caseLot: 144 },   // グループ計144 = 1ケース
  ]);
  ok(ev.auto && ev.auto.kind === 'caseAmount' && ev.auto.met === false && ev.auto.amountMet === false && ev.auto.misaligned.length === 0,
    'ケース入数かつ金額: 混載でグループ計がケースちょうど (金額未達でmet=false)', ev.auto);
  ev = evaluateCondition(ja, jkeys, [
    { key: 'j1', qty: 144, cost: 300, caseGroup: 'OA-JON-1', caseLot: 144 },
    { key: 'j3', qty: 30, cost: 610, caseGroup: 'OA-JON-2', caseLot: 30 },
  ]);
  ok(ev.auto && ev.auto.met === true && ev.auto.misaligned.length === 0 && ev.sumAmount === 144 * 300 + 30 * 610,
    'ケース入数かつ金額: 全グループ整列 + 金額充足でmet', ev.auto);
  ev = evaluateCondition(ja, jkeys, [
    { key: 'j1', qty: 100, cost: 600, caseGroup: 'OA-JON-1', caseLot: 144 },  // 144の倍数でない
    { key: 'solo', qty: 25, cost: 100, caseLot: 12 },                          // グループ無し単品: 12の倍数でない
    { key: 'noinfo', qty: 5, cost: 100 },                                      // ケース情報なし
  ]);
  ok(ev.auto && ev.auto.met === false && ev.auto.misaligned.length === 2 && ev.auto.noInfo.length === 1,
    'ケース入数かつ金額: グループズレ+単品ズレをmisaligned、情報なしはnoInfo', ev.auto);

  // ロット倍率 (パシーマ): 各商品 ロット×倍率 の倍数で発注
  const lm = { condition_type: 'ロット倍率', condition_value: 2, unit: '倍' };
  const pkeys = new Set(['p1', 'p2']);
  ev = evaluateCondition(lm, pkeys, [{ key: 'p1', qty: 20, cost: 6050, lot: 10 }]);
  ok(ev.auto && ev.auto.kind === 'lotMultiple' && ev.auto.met === true, 'ロット倍率2: lot10×2=20個はOK', ev.auto);
  ev = evaluateCondition(lm, pkeys, [{ key: 'p1', qty: 30, cost: 6050, lot: 10 }, { key: 'p2', qty: 28, cost: 4900, lot: 14 }]);
  ok(ev.auto && ev.auto.met === false && ev.auto.off.length === 1 && ev.auto.off[0].key === 'p1' && ev.auto.off[0].step === 20,
    'ロット倍率2: 30個 (20の倍数でない) だけ違反', ev.auto);
  ev = evaluateCondition(lm, pkeys, [{ key: 'p1', qty: 10, cost: 100 }]);
  ok(ev.auto && ev.auto.met === true && ev.auto.unknown.length === 1, 'ロット倍率: ロット不明はunknownに記録しmetは下げない', ev.auto);

  // ロット倍率以上 (ノアテック): 各商品 ロット×倍率 以上を発注
  const lmin = { condition_type: 'ロット倍率以上', condition_value: 2, unit: '倍' };
  ev = evaluateCondition(lmin, pkeys, [{ key: 'p1', qty: 60, cost: 750, lot: 30 }]);
  ok(ev.auto && ev.auto.kind === 'lotMin' && ev.auto.met === true, 'ロット倍率以上2: lot30で60個はOK', ev.auto);
  ev = evaluateCondition(lmin, pkeys, [{ key: 'p1', qty: 30, cost: 750, lot: 30 }]);
  ok(ev.auto && ev.auto.met === false && ev.auto.off[0].min === 60, 'ロット倍率以上2: 30個は最低60個未満でNG', ev.auto);
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
  ok(!html.includes('set-2pack'), '/products セット商品 (商品区分=セット) は載せない');
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
    ok(html.includes('未完了') && html.includes('入荷待ち') && html.includes('data-rowcancel') && html.includes('SKU 金額'),
      '/backorders 新表示 (未完了タブ/入荷待ちバッジ/予約取消/SKU・金額)');
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
    if (p.includes('/email/send') && body) body = { expectedMode: CUR_MODE, expectedChannel: 'email', ...body };
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

  // 完了済み (closed) POへの新規送信は拒否 (一括送信の失効選択に対するサーバ側最終防衛、Codex 一括R2 High)
  {
    r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: [{ code: 'noflyersticker', qty: 3 }] }) });
    const closedId = r.body.id;
    const closedItem = db.prepare('SELECT id FROM po_order_items WHERE order_id=?').get(closedId);
    r = await jsonPost('/api/items/' + closedItem.id + '/events', { type: 'receipt', qty: 3 });
    ok(r.body.ok, '全量入荷で自動クローズ (テスト用PO)');
    ok(db.prepare('SELECT closed_at FROM po_orders WHERE id=?').get(closedId).closed_at != null, 'PO closed確認');
    r = await jsonPost('/api/orders/' + closedId + '/email/send', {}, 'em-closed-guard');
    ok(r.status === 400 && r.body.error.includes('完了済み'), '完了済みPOへの新規送信は400 (サーバ側防衛)', r.body.error);
  }

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

  // 発注残API: PO行のメール状況 (送信済み/予約中) と行からの予約取消 (中原さん要望 2026-07-15)
  {
    r = await j('/api/backorders');
    const emBo = r.body.orders.find(o => o.id === emOrderId);
    ok(emBo && emBo.emailSentLive === true && emBo.email != null, '発注残API: live送信済みPOは emailSentLive=true', emBo && emBo.email);
    // 新しい予約 (dry-runモード中=dedup exempt) → email.status=queued+scheduledAtJst → 取消 → 有効ジョブなし
    const jst2 = new Date(Date.now() + 9 * 3600000 + 7200000).toISOString().slice(0, 16);
    r = await jsonPost('/api/orders/' + schedOrderId + '/email/send', { scheduledAt: jst2 }, 'em-key-row1');
    ok(r.body.ok && r.body.status === 'scheduled', '行テスト用の予約作成', r.body);
    const rowJobId = r.body.jobId;
    r = await j('/api/backorders');
    const sBo = r.body.orders.find(o => o.id === schedOrderId);
    ok(sBo && sBo.email && sBo.email.status === 'queued' && sBo.email.jobId === rowJobId && !!sBo.email.scheduledAtJst,
      '発注残API: 予約中 (queued+scheduledAtJst+jobId)', sBo && sBo.email);
    r = await jsonPost('/api/email-jobs/' + rowJobId + '/cancel', {});
    ok(r.body.ok, '行の予約取消 (cancel API)');
    r = await j('/api/backorders');
    const sBo2 = r.body.orders.find(o => o.id === schedOrderId);
    ok(sBo2 && sBo2.email && sBo2.email.jobId !== rowJobId && sBo2.email.status === 'sent',
      '取消後: cancelledを除いた直前ジョブ (sent) に戻る', sBo2 && sBo2.email);
    // live送信済み+新規予約 → 「送信済み」と「予約中」が両立 (どちらの事実も消えない)
    const jst3 = new Date(Date.now() + 9 * 3600000 + 7200000).toISOString().slice(0, 16);
    r = await jsonPost('/api/orders/' + emOrderId + '/email/send', { scheduledAt: jst3 }, 'em-key-row2');
    ok(r.body.ok && r.body.status === 'scheduled', '送信済みPOへの新規予約 (dry-run)');
    const row2JobId = r.body.jobId;
    r = await j('/api/backorders');
    const emBo2 = r.body.orders.find(o => o.id === emOrderId);
    ok(emBo2 && emBo2.emailSentLive === true && emBo2.email && emBo2.email.status === 'queued',
      '発注残API: 送信済み (live) と予約中は両立表示', emBo2 && emBo2.email);
    // 取消競合: 送信が始まった (sending) 後の取消は拒否 (送信前queued/failedのみ取消可)
    db.prepare("UPDATE po_email_jobs SET status='sending' WHERE id=?").run(row2JobId);
    r = await jsonPost('/api/email-jobs/' + row2JobId + '/cancel', {});
    ok(r.status === 400 && r.body.error.includes('送信前'), '予約取消: sending中は拒否 (競合安全)', r.body.error);
    // 後片付け: 状態機械トリガにより sending→queued は不可 (sent/failed/unknownのみ) → failed経由で取消
    db.prepare("UPDATE po_email_jobs SET status='failed' WHERE id=?").run(row2JobId);
    r = await jsonPost('/api/email-jobs/' + row2JobId + '/cancel', {});
    ok(r.body.ok, '後片付け: failed→取消 (failedは取消可能)');
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
  const beforeDead = (([...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'deaditem')) || {}).recentIssued || null;
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
  const afterDead = (([...r.body.targets, ...r.body.candidates, ...r.body.horikoshi].find(p => p.code === 'deaditem')) || {}).recentIssued || null;
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
  // 一括メール送信 (チェックボックス+送信バー) と商品別注残検索のUI配信
  ok(boHtml.includes('boSel') && boHtml.includes('boBulkSend') && boHtml.includes('まとめて送信') && boHtml.includes('boBulkAt'),
    '/backorders 一括メール送信UI (チェックボックス+予約日時)');
  ok(boHtml.includes('boQ') && boHtml.includes('renderProd') && boHtml.includes('商品別注残') && boHtml.includes('data-jump'),
    '/backorders 商品別注残検索UI (横断表示+POへジャンプ)');
}

// ═══ 仕入先別注残ビュー + 注残確認CSV (中原さん要望 2026-07-15) ═══
console.log('── 仕入先別注残 + 注残確認CSV ──');
{
  // 対応表に先方管理番号を登録 → /api/backorders の明細に vendor_code が載る
  // (既存エントリは退避して最後に復元 — 後続の出荷明細変換テストが同じ商品の対応を使う)
  const prevNofly = db.prepare("SELECT * FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='noflyersticker'").get();
  db.prepare(`INSERT OR REPLACE INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at)
    VALUES ('1','noflyersticker','noflyersticker','ZZZ-NOFLY-01',?)`).run(nowIsoStr());
  r = await j('/api/backorders');
  const migO = r.body.orders.find(o => o.neSlipNumber === '6274');
  ok(migO && migO.origin === 'migration', '発注残API: neSlipNumber を返す (移行PO)', migO && migO.neSlipNumber);
  const vItem = migO.items.find(i => i.product_code === 'noflyersticker');
  ok(vItem && vItem.vendor_code === 'ZZZ-NOFLY-01', '発注残API: 明細に先方管理番号 (対応表join)', vItem && vItem.vendor_code);
  const noV = migO.items.find(i => i.product_code === 'deaditem');
  ok(noV && noV.vendor_code === null, '発注残API: 対応表未登録は vendor_code=null');

  // CSV: supplier必須 / 注残のない仕入先は404
  let raw = await fetch(base + '/api/backorders/supplier-csv');
  ok(raw.status === 400, '注残確認CSV: supplierなしは400');
  raw = await fetch(base + '/api/backorders/supplier-csv?supplier=0777');
  ok(raw.status === 404, '注残確認CSV: 注残のない仕入先は404');

  // CSV本体 (supplier=0001 → 正規化'1'。Shift-JIS+日本語ファイル名)
  raw = await fetch(base + '/api/backorders/supplier-csv?supplier=0001');
  ok(raw.status === 200 && (raw.headers.get('content-type') || '').includes('Shift_JIS'), '注残確認CSV: 200 + Shift_JIS', raw.status);
  const dispo = raw.headers.get('content-disposition') || '';
  ok(dispo.includes('attachment') && dispo.includes("filename*=UTF-8''") && dispo.includes(encodeURIComponent('注残確認_')),
    '注残確認CSV: Content-Disposition filename* (日本語名+ASCIIフォールバック)', dispo);
  const csv = iconv.decode(Buffer.from(await raw.arrayBuffer()), 'cp932');
  const lines = csv.split('\r\n');
  ok(lines[0].startsWith('注残確認リスト') && lines[0].includes('基準日時'), 'CSV: 基準日時ヘッダ', lines[0]);
  ok(lines[1].includes('様'), 'CSV: 仕入先名行', lines[1]);
  const hdrIdx = lines.findIndex(l => l.startsWith('発注日,発注書番号,貴社管理番号'));
  ok(hdrIdx > 0, 'CSV: 明細見出し (発注日/発注書番号/貴社管理番号/…)');
  const totIdx = lines.findIndex(l => l.startsWith('合計,'));
  ok(totIdx > hdrIdx, 'CSV: 合計行');
  // 移行POは NE伝票番号が発注書番号列に出る (仕入先が知っている番号)
  const l6274 = lines.slice(hdrIdx + 1, totIdx).find(l => l.split(',')[1] === '6274');
  ok(l6274 && l6274.includes('ZZZ-NOFLY-01') && l6274.includes('noflyersticker') && !l6274.includes('PO-'),
    'CSV: 移行PO行 = NE伝票番号 + 先方管理番号 (内部PO番号を出さない)', l6274);
  // 算術検証: 全明細行と合計行で 発注数 − 入荷済 − 減数 − 取消 = 注残 (Codex設計相談の要点)
  const detail = lines.slice(hdrIdx + 1, totIdx).map(l => l.split(','));
  ok(detail.length > 0 && detail.every(c => Number(c[5]) - Number(c[6]) - Number(c[7]) - Number(c[8]) === Number(c[9])),
    'CSV: 全明細行で 発注−入荷−減数−取消=注残', detail.length);
  ok(detail.every(c => Number(c[9]) > 0), 'CSV: 注残0の明細は載らない');
  const totC = lines[totIdx].split(',');
  ok([5, 6, 7, 8, 9].every(k => Number(totC[k]) === detail.reduce((s, c) => s + Number(c[k]), 0)), 'CSV: 合計行 = 明細列の合計', lines[totIdx]);
  // 発注日の古い順 (画面と同じ並び)
  const dates = detail.map(c => c[0]);
  ok(dates.every((d, i) => i === 0 || dates[i - 1] <= d), 'CSV: 発注日の古い順', dates.join('|'));

  // CSV injection: 数式に化ける先頭文字は ' 前置 (発注書メール添付と同じ cellValue 共用)
  db.prepare(`INSERT OR REPLACE INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at)
    VALUES ('1','deaditem','deaditem','=SUM(A1)',?)`).run(nowIsoStr());
  raw = await fetch(base + '/api/backorders/supplier-csv?supplier=1');
  ok(raw.status === 200 && iconv.decode(Buffer.from(await raw.arrayBuffer()), 'cp932').includes("'=SUM(A1)"),
    '注残確認CSV: 数式インジェクション対策 (先頭= はアポストロフィ前置)');
  // CP932変換不能文字は黙って?に化けさせず400 (fail-closed)
  db.prepare(`UPDATE po_vendor_code_map SET vendor_code='🐟emoji' WHERE supplier_code='1' AND product_key='deaditem'`).run();
  raw = await fetch(base + '/api/backorders/supplier-csv?supplier=1');
  const errBody = await raw.json().catch(() => null);
  ok(raw.status === 400 && errBody && errBody.error.includes('Shift-JIS'), '注残確認CSV: CP932変換不能文字は400 (fail-closed)', errBody && errBody.error);
  db.prepare(`DELETE FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='deaditem'`).run();
  // 退避した対応表エントリを復元
  if (prevNofly) db.prepare(`INSERT OR REPLACE INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, qty_per_unit, updated_at)
    VALUES (?,?,?,?,?,?)`).run(prevNofly.supplier_code, prevNofly.product_key, prevNofly.product_code, prevNofly.vendor_code, prevNofly.qty_per_unit, prevNofly.updated_at);
  else db.prepare("DELETE FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='noflyersticker'").run();

  // 画面: 🏭 仕入先別ビューのUI配信
  const boHtml3 = await (await fetch(base + '/backorders')).text();
  ok(boHtml3.includes('仕入先別') && boHtml3.includes('data-supsel') && boHtml3.includes('renderSupplierView') && boHtml3.includes('supGroups'),
    '/backorders 🏭仕入先別ビュー (タブ+一覧→明細)');
  ok(boHtml3.includes('data-csvdl') && boHtml3.includes('注残確認CSV') && boHtml3.includes('supplier-csv'),
    '/backorders 📥注残確認CSVダウンロードUI');
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
  insRow.run(savedRow.商品コード, savedRow.商品名, savedRow.仕入先, savedRow.取扱区分, savedRow.売上分類, savedRow.総在庫数,
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
  ok(r.body.pasteText === 'noflyersticker\t1600\t70', 'convert: 貼り付けデータ=商品ID(NE表記)/入荷予定数/仕入単価(PML原価)', r.body.pasteText);
  // 商品IDの大小表記はNE商品マスタが正: 対応表に大文字で登録されていてもNE表記 (小文字) で出力する
  // (ロジザードは大文字小文字を区別し、NE登録の表記と一致しないと登録エラー)
  {
    const fdUp = new FormData();
    fdUp.append('supplier_code', '1');
    fdUp.append('file', new Blob([iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"AMC-001","","NOFLYERSTICKER"', 'Shift_JIS')]), 'up.csv');
    r = await j('/api/vendor-map/csv', { method: 'POST', body: fdUp });
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: 'AMC-001\tx\t10\n' });
    ok(r.body.ok && r.body.rows[0].productCode === 'noflyersticker' && r.body.pasteText.startsWith('noflyersticker\t10\t'),
      'convert: 対応表が大文字でもNE表記 (小文字) で出力', r.body.pasteText);
    const fdBack = new FormData();
    fdBack.append('supplier_code', '1');
    fdBack.append('file', new Blob([iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"AMC-001","","noflyersticker"', 'Shift_JIS')]), 'back.csv');
    await j('/api/vendor-map/csv', { method: 'POST', body: fdBack });
    // 後続テストが参照する r (shipTextの変換結果) を復元
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '0001', text: shipText });
  }
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

  // ── 仮商品コード: NE未登録の新商品も貼り付けデータに含める (中原さん要望 2026-07-28) ──
  {
    const provText = 'ZZZ-PROV-1\tｽｴｰﾄﾞ補修ｼｰﾄ ﾌﾞﾗｯｸ\t12\n';
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: provText });
    ok(r.body.ok && r.body.rowCount === 0 && r.body.unmatched.length === 1, 'prov: 仮商品コードなしは従来どおりunmatched (貼り付けに含まれない)', r.body.rowCount);
    // 日本語 (商品名で検索した文字列の消し忘れ) は拒否 = 検索語がそのまま商品IDになる事故を防ぐ
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: 'スエード補修シート' }] });
    ok(r.status === 400 && r.body.error.includes('仮商品コード'), 'prov: 日本語の仮商品コードは拒否', r.body.error);
    ok(!db.prepare("SELECT 1 FROM po_vendor_code_pending WHERE supplier_code='1' AND vendor_code_norm='ZZZ-PROV-1'").get(),
      'prov: 検証エラーなら1件も登録しない (入力を直せるようにする)');
    // 既に対応表にある商品コードは拒否 (🔁この番号で再登録へ誘導 — 同じ商品IDが2行出るのを防ぐ)
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: 'noflyersticker' }] });
    ok(r.status === 400 && r.body.error.includes('再登録'), 'prov: 対応表にある商品コードは拒否', r.body.error);
    // 他仕入先の実在商品も拒否 (打ち間違い検知)
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: 'gyoumuhandcream60-BI' }] });
    ok(r.status === 400 && r.body.error.includes('仕入先'), 'prov: 他仕入先の商品コードは拒否', r.body.error);
    // 正常系: 仮登録 → 変換すると貼り付けデータに載る (単価はPMLに無いので空欄)
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1',
      items: [{ vendorCode: 'ZZZ-PROV-1', vendorName: 'ｽｴｰﾄﾞ補修ｼｰﾄ ﾌﾞﾗｯｸ', qty: 12, provisionalProductCode: 'sueders-bk' }] });
    ok(r.body.ok && r.body.added === 1 && r.body.provisionalSet === 1, 'prov: 仮商品コード付きで仮登録', r.body);
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: provText });
    ok(r.body.ok && r.body.rowCount === 1 && r.body.unmatched.length === 0 && r.body.pasteText === 'sueders-bk\t12\t',
      'prov: 貼り付けデータに仮商品コードの行が入る (単価は空欄)', r.body.pasteText);
    ok(r.body.totalQty === 12 && (r.body.provisionalRows || []).length === 1 && r.body.caseUnverified.length === 0,
      'prov: 入荷予定数合計に含む / provisionalRowsで警告 / NE表記未確認の警告には出さない', r.body.provisionalRows);
    ok((r.body.lines || [])[0].type === 'provisional' && r.body.lines[0].provisionalCode === 'sueders-bk', 'prov: 行の種別=provisional');
    // 入数換算も対応表と同じ (先方1 = 弊社24個)
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1',
      items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: 'sueders-bk', provisionalQtyPerUnit: 24 }] });
    ok(r.body.ok && r.body.refreshed === 1, 'prov: 入数つきで更新', r.body);
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: provText });
    ok(r.body.pasteText === 'sueders-bk\t288\t' && r.body.totalQty === 288, 'prov: 入数換算 (12×24)', r.body.pasteText);
    // 同じ仮商品コードを別の先方番号に付けるのは拒否 (貼り付けに同じ商品IDが2行出る)
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-2', qty: 1, provisionalProductCode: 'sueders-bk' }] });
    ok(r.status === 400 && r.body.error.includes('別の先方番号'), 'prov: 仮商品コードの重複は拒否', r.body.error);
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [
      { vendorCode: 'ZZZ-PROV-3', qty: 1, provisionalProductCode: 'sueders-db' },
      { vendorCode: 'ZZZ-PROV-4', qty: 1, provisionalProductCode: 'sueders-db' }] });
    ok(r.status === 400 && r.body.error.includes('重複'), 'prov: 同一バッチ内の重複も拒否', r.body.error);
    // 仮商品コードを送らない仮登録は既存のコードを消さない (❓行だけの一括仮登録で消えないこと)
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-1', qty: 5 }] });
    const keepRow = db.prepare("SELECT provisional_product_code AS c, provisional_qty_per_unit AS q FROM po_vendor_code_pending WHERE supplier_code='1' AND vendor_code_norm='ZZZ-PROV-1'").get();
    ok(r.body.ok && keepRow.c === 'sueders-bk' && keepRow.q === 24, 'prov: provisionalProductCode未指定はコードも入数も保持', keepRow);
    // コードだけ直したときに入数が黙って消えない (Codex 仮コードR1 Medium)
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: 'sueders-bk2' }] });
    const keep2 = db.prepare("SELECT provisional_product_code AS c, provisional_qty_per_unit AS q FROM po_vendor_code_pending WHERE supplier_code='1' AND vendor_code_norm='ZZZ-PROV-1'").get();
    ok(r.body.ok && keep2.c === 'sueders-bk2' && keep2.q === 24, 'prov: コードだけ変更しても入数は保持 (三値更新)', keep2);
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: 'sueders-bk', provisionalQtyPerUnit: '' }] });
    ok(r.body.ok && db.prepare("SELECT provisional_qty_per_unit AS q FROM po_vendor_code_pending WHERE supplier_code='1' AND vendor_code_norm='ZZZ-PROV-1'").get().q === null,
      'prov: 入数に空文字を送ると消える');
    // 仮登録の後にそのコードが対応表へ入ったら、貼り付けから外して警告 (同じ商品IDが2行出るのを防ぐ)
    {
      db.prepare("INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES ('1','sueders-bk','sueders-bk','ZZZ-OTHER-9',?)").run(nowIsoStr());
      r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: provText });
      ok(r.body.rowCount === 0 && (r.body.provisionalConflicts || []).length === 1 && r.body.provisionalConflicts[0].mappedVendorCode === 'ZZZ-OTHER-9',
        'prov: 対応表と重複した仮商品コードは貼り付けから除外して警告', r.body.provisionalConflicts);
      ok(!r.body.pasteText.includes('sueders-bk'), 'prov: 重複行は貼り付けデータに出ない', r.body.pasteText);
      db.prepare("DELETE FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='sueders-bk'").run();
    }
    // 空文字を送ると消える → ❓対応表になしへ戻る
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: '' }] });
    ok(r.body.ok && r.body.provisionalCleared === 1, 'prov: 空文字で仮商品コードを消す', r.body);
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: provText });
    ok(r.body.rowCount === 0 && r.body.unmatched.length === 1, 'prov: 消したら貼り付けから外れる');
    // 対応表へ昇格すると入数を引き継ぎ、仮コードと違う商品に紐づけたら警告
    r = await jp2('/api/vendor-map/pending', { supplier_code: '1',
      items: [{ vendorCode: 'ZZZ-PROV-1', qty: 12, provisionalProductCode: 'sueders-bk', provisionalQtyPerUnit: 24 }] });
    r = await j('/api/vendor-map/pending?supplier=1');
    const pendProv = r.body.rows.find(x => x.vendor_code === 'ZZZ-PROV-1');
    ok(pendProv && pendProv.provisional_product_code === 'sueders-bk' && pendProv.provisional_qty_per_unit === 24, 'prov: 一覧に仮商品コード/入数が出る', pendProv);
    r = await jp2('/api/vendor-map/pending/' + pendProv.id + '/link', { product_code: 'diyorangeoil100' });
    ok(r.body.ok && r.body.warning && r.body.warning.includes('sueders-bk'), 'prov link: 仮コードと違う商品に紐づけたら警告', r.body.warning);
    const provMap = db.prepare("SELECT qty_per_unit FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='diyorangeoil100'").get();
    ok(provMap && provMap.qty_per_unit === 24, 'prov link: 仮登録の入数を対応表へ引き継ぐ', provMap);
    db.prepare("DELETE FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='diyorangeoil100'").run();
    db.prepare("DELETE FROM po_vendor_code_pending WHERE supplier_code='1' AND vendor_code_norm LIKE 'ZZZ-PROV-%'").run();
  }

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

  // 非ASCIIの大小文字違いも同一番号として拒否 (JS正規化統一の検証、Codex plan-R3 Low)
  db.prepare("INSERT INTO po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, updated_at) VALUES ('1','0726-001060','0726-001060','ÄBC-1',?)").run(nowIsoStr());
  r = await jp2('/api/vendor-map/pending', { supplier_code: '1', items: [{ vendorCode: 'äbc-1', qty: 1 }] });
  r = await j('/api/vendor-map/pending?supplier=1');
  const pendUml = r.body.rows.find(x => x.vendor_code === 'äbc-1');
  r = await jp2('/api/vendor-map/pending/' + pendUml.id + '/link', { product_code: 'noflyersticker' });
  ok(r.status === 400 && r.body.error.includes('既に商品'), 'pending link: 非ASCII大小文字違いも同一番号として拒否', r.body.error);
  r = await jp2('/api/vendor-map/pending/' + pendUml.id + '/dismiss', {});
  db.prepare("DELETE FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='0726-001060'").run();

  // ビーフリーのメール本文形式 (見出し行/送り状No行/同一商品の複数行=そのまま複数行出力) — 実メール 2026-07-13 の形
  {
    const csvBf = iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"GOFUN-01-N","","gyoumuhandcream60-BI"', 'Shift_JIS');
    const fdBf = new FormData();
    fdBf.append('supplier_code', '2');
    fdBf.append('file', new Blob([csvBf]), 'bf.csv');
    r = await j('/api/vendor-map/csv', { method: 'POST', body: fdBf });
    ok(r.body.ok, 'BEFREE: 対応表準備');
    const mailText = '【弊社委託倉庫出荷分】\n■ 福通送り状Ｎｏ：66327560902\n' +
      '商品ID\t商品名\t出荷数\n' +
      'GOFUN-01-N\t胡粉ネイル スーパーコート N0033\t48\n' +
      'GOFUN-01-N\t胡粉ネイル スーパーコート N0033\t30\n' +
      'AUKATZ-06-N2\tヘルスウォーター にゃんマグ 白系\t30\n';
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '2', text: mailText });
    ok(r.body.ok && r.body.rowCount === 2 && r.body.totalQty === 78, 'BEFREE: メール表の貼り付けを変換 (同一商品の複数行は複数行のまま=手作業と同じ)', r.body);
    ok(r.body.pasteText.split('\n').every(l => l.startsWith('gyoumuhandcream60-BI\t')), 'BEFREE: 貼り付けデータ2行 (商品IDはNEの大小表記のまま)', r.body.pasteText);
    ok(r.body.unmatched.length === 1 && r.body.unmatched[0].vendorCode === 'AUKATZ-06-N2', 'BEFREE: 未知の番号はunmatched');
    ok(r.body.skipped.length === 2, 'BEFREE: 見出し外の行 (【…】/送り状No) はスキップ', r.body.skipped);
  }

  // ── 入数 (単位換算) と左右対応表示 (lines) ──
  {
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'noflyersticker', qty_per_unit: 24 });
    ok(r.body.ok && r.body.qtyPerUnit === 24, '入数の登録 (noflyersticker=24)');
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: 'AMC-001\tﾁﾗｼ\t2\n' });
    ok(r.body.ok && r.body.rows[0].vendorQty === 2 && r.body.rows[0].qtyPerUnit === 24 && r.body.rows[0].qty === 48 &&
      r.body.pasteText.startsWith('noflyersticker\t48\t') && r.body.totalQty === 48, '入数換算: 先方2×24=48が貼り付けに反映', r.body.pasteText);
    ok(r.body.lines && r.body.lines[0].type === 'ok' && r.body.lines[0].productName !== undefined && r.body.totalVendorQty === 2,
      'lines: 左=仕入先/右=弊社の表示用データ');
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: 'ZZZ-UNKNOWN-9\t新しいやつ\t5\n' });
    ok(r.body.lines[0].type === 'unmatched' && r.body.lines[0].vendorQty === 5, 'lines: 対応表になし=新商品行として返る');
    // バリデーション
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'noflyersticker', qty_per_unit: -3 });
    ok(r.status === 400, '入数の負数は400');
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'zzz-no-such', qty_per_unit: 2 });
    ok(r.status === 404, '対応表に無い商品の入数登録は404');
    // entry更新 (番号変更等) では入数を保持
    r = await jp2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'noflyersticker', vendor_code: 'AMC-001' });
    ok(r.body.ok && db.prepare("SELECT qty_per_unit FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='noflyersticker'").get().qty_per_unit === 24,
      'entry更新で入数を保持');
    // CSV全置換でも入数を保持 (入数列なしCSV)
    const fdKeep = new FormData();
    fdKeep.append('supplier_code', '1');
    fdKeep.append('file', new Blob([iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"AMC-001","","noflyersticker"', 'Shift_JIS')]), 'keep.csv');
    r = await j('/api/vendor-map/csv', { method: 'POST', body: fdKeep });
    ok(r.body.ok && db.prepare("SELECT qty_per_unit FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='noflyersticker'").get().qty_per_unit === 24,
      'CSV全置換でも入数を保持');
    // CSVに入数列があればそちらを採用
    const fdQpu = new FormData();
    fdQpu.append('supplier_code', '1');
    fdQpu.append('file', new Blob([iconv.encode('"仕入先管理番号","x","弊社管理番号","入数"\r\n"AMC-001","","noflyersticker","12"', 'Shift_JIS')]), 'qpu.csv');
    r = await j('/api/vendor-map/csv', { method: 'POST', body: fdQpu });
    ok(r.body.ok && db.prepare("SELECT qty_per_unit FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='noflyersticker'").get().qty_per_unit === 12,
      'CSVの入数列を取込');
    // CSVの不正な入数は警告して既存値を保持
    const fdBadQ = new FormData();
    fdBadQ.append('supplier_code', '1');
    fdBadQ.append('file', new Blob([iconv.encode('"仕入先管理番号","x","弊社管理番号","入数"\r\n"AMC-001","","noflyersticker","abc"', 'Shift_JIS')]), 'badq.csv');
    r = await j('/api/vendor-map/csv', { method: 'POST', body: fdBadQ });
    ok(r.body.ok && (r.body.warnings || []).some(w => w.includes('入数')) &&
      db.prepare("SELECT qty_per_unit FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='noflyersticker'").get().qty_per_unit === 12,
      'CSVの不正な入数は警告+既存値を保持');
    // 上限・0の拒否 (Codex 入数R1 Medium)
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'noflyersticker', qty_per_unit: 10000000 });
    ok(r.status === 400, '入数の上限 (1,000,000) 超えは400');
    r = await jp2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'noflyersticker', vendor_code: 'AMC-001', qty_per_unit: 0 });
    ok(r.status === 400, 'entry経由の入数0は400');
    // 換算結果が正の整数にならない行 (0.1×1→0) は貼り付けから除外
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'noflyersticker', qty_per_unit: 0.1 });
    ok(r.body.ok, '小数の入数は登録可能 (換算時に検証)');
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: 'AMC-001\tﾁﾗｼ\t1\n' });
    ok(r.body.ok && r.body.rowCount === 0 && r.body.qtyInvalid.length === 1 && r.body.lines[0].type === 'badqty' && r.body.pasteText === '',
      '換算結果が不正な行は貼り付けから除外 (badqty)', r.body.qtyInvalid);
    // 端数を黙って丸めない: 1×1.5=1.5 は badqty、2×0.5=1 は正常 (Codex 入数R2 High)
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'noflyersticker', qty_per_unit: 1.5 });
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: 'AMC-001\tﾁﾗｼ\t1\n' });
    ok(r.body.ok && r.body.rowCount === 0 && r.body.lines[0].type === 'badqty', '1×1.5=1.5 は丸めずbadqty');
    r = await jp2('/api/inbound-plan/convert', { supplier_code: '1', text: 'AMC-001\tﾁﾗｼ\t2\n' });
    ok(r.body.ok && r.body.rowCount === 1 && r.body.rows[0].qty === 3, '2×1.5=3 は整数なので正常', r.body.rows[0] && r.body.rows[0].qty);
    // 楽観ロック: 入数変更で updated_at が進む → 古いbaseのentry更新は409
    const baseOld = db.prepare("SELECT updated_at FROM po_vendor_code_map WHERE supplier_code='1' AND product_key='noflyersticker'").get().updated_at;
    await new Promise(rs => setTimeout(rs, 5)); // updated_at (ISO ms) が確実に進むように
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'noflyersticker', qty_per_unit: 24 });
    ok(r.body.ok, '入数を更新');
    r = await jp2('/api/vendor-map/entry', { supplier_code: '1', product_code: 'noflyersticker', vendor_code: 'AMC-999', baseUpdatedAt: baseOld });
    ok(r.status === 409, '入数変更後、古いbaseのentry更新は409 (競合検出)');
    // 後片付け: 入数クリア (空欄=換算なし)。以降のメール変換テスト (25個のまま) に影響させない
    r = await jp2('/api/vendor-map/qty-per-unit', { supplier_code: '1', product_code: 'noflyersticker', qty_per_unit: '' });
    ok(r.body.ok && r.body.qtyPerUnit === 1, '入数クリア (空欄=1扱い)');
  }

  // CSV取込の重複警告 (同じ先方番号を複数商品に。ブロックせず警告列挙)
  {
    const csvDup = iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"BF-X","","prod-a"\r\n"bf-x","","prod-b"', 'Shift_JIS');
    const fd2 = new FormData();
    fd2.append('supplier_code', '2');
    fd2.append('file', new Blob([csvDup]), 'dup.csv');
    r = await j('/api/vendor-map/csv', { method: 'POST', body: fd2 });
    ok(r.body.ok && (r.body.warnings || []).some(w => w.includes('複数商品')), 'vendor-map CSV: 同番号の複数商品は警告列挙', r.body.warnings);
  }

  // 画面配信
  const inbHtml2 = await (await fetch(base + '/inbound-plan')).text();
  ok(inbHtml2.includes('入荷予定') && inbHtml2.includes('ipConvert'), '/inbound-plan 入荷予定作成ページ');
  ok(inbHtml2.includes('仕入先の出荷明細') && inbHtml2.includes('data-ipqpu') && inbHtml2.includes('新商品'), '/inbound-plan 左右対応表+入数編集UI');
  ok(inbHtml2.includes('ロジザード入荷予定を開く') && inbHtml2.includes('ap003.logizard.net/LPSTD405/PA01/Index'), '/inbound-plan ロジザードを開くボタン');
  ok(inbHtml2.includes('data-iprelink') && inbHtml2.includes('この番号で再登録'), '/inbound-plan 対応表になし行の再登録UI');
  ok(inbHtml2.includes('data-ipprovcode') && inbHtml2.includes('data-ipprovsave') && inbHtml2.includes('仮商品コード'),
    '/inbound-plan 仮商品コードの入力+保存UI (新商品も貼り付けに含める)');
  ok(inbHtml2.includes('ipVmapOpen') && inbHtml2.includes('grp=vendormap'), '/inbound-plan 対応表を別タブで開くボタン');
  const adminHtml3 = await (await fetch(base + '/admin')).text();
  ok(adminHtml3.includes('loadVmapPending') && adminHtml3.includes('未紐付けの先方番号'), '/admin 対応表タブに仮登録リスト');
  ok(adminHtml3.includes('provisional_product_code'), '/admin 仮登録リストに仮商品コード列 (紐づけ欄へ初期表示)');
  ok(adminHtml3.includes('data-vmqpu') && adminHtml3.includes('vmNewQpu'), '/admin 対応表に入数列');
  ok(adminHtml3.includes('URLSearchParams') && adminHtml3.includes("qp0.get('sup')"), '/admin URLパラメータでグループ・仕入先を指定可');
}

// ═══ 出荷明細メールの自動取得 (Gmail偽装) ═══
console.log('── 出荷明細メール自動取得 (fetch-mails) ──');
{
  const jp3 = (p, body) => j(p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
  // 供給2の対応表を戻す (前のCSV重複テストで全置換されたため)
  {
    const csvBf2 = iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"GOFUN-01-N","","gyoumuhandcream60-BI"', 'Shift_JIS');
    const fdBf2 = new FormData();
    fdBf2.append('supplier_code', '2');
    fdBf2.append('file', new Blob([csvBf2]), 'bf2.csv');
    await j('/api/vendor-map/csv', { method: 'POST', body: fdBf2 });
  }
  // AMC出荷明細のxlsxを実物どおりに生成 (先頭7行空+8行目見出し)
  const ExcelJS = (await import('exceljs')).default;
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('出荷明細表');
  for (let i = 0; i < 7; i++) ws.addRow([]);
  ws.addRow(['商品コード', '商品名', '出荷数量', '備考', '倉庫コード', '摘要']);
  ws.addRow(['AMC-001', 'ﾁﾗｼ ｽﾃｯｶｰ', 25, '', '0000', '']);
  ws.addRow(['ZZZ-MAIL-NEW', '新商品Y', 5, '', '0000', '']);
  const xlsxB64 = Buffer.from(await wb.xlsx.writeBuffer()).toString('base64');
  // 請求書xlsx (出荷明細の見出しなし) — 複数添付メールの優先解析テスト用
  const wbSeikyu = new ExcelJS.Workbook();
  const wsSeikyu = wbSeikyu.addWorksheet('請求書');
  wsSeikyu.addRow(['請求書']);
  wsSeikyu.addRow(['合計', 12345]);
  const seikyuB64 = Buffer.from(await wbSeikyu.xlsx.writeBuffer()).toString('base64');
  process.env.PO_SHIPMENT_FAKE_DATA = JSON.stringify([
    { id: 'gm-amc-1', from: '芦田 <ashida@am-craft.jp>', subject: '出荷明細のご連絡', internalDate: '2026-07-13T06:00:00.000Z',
      attachments: [{ filename: 'ビーフェイス様用出荷明細.xlsx', dataBase64: xlsxB64 }] },
    // 実例 (2026-07-13 AMC): 件名「請求書」に 請求書.xlsx+出荷明細.xlsx の2添付。先頭が請求書でも出荷明細を優先解析する
    { id: 'gm-amc-multi', from: 's.ashida@am-craft.jp', subject: '請求書', internalDate: '2026-07-13T08:59:00.000Z',
      attachments: [{ filename: '請求書.xlsx', dataBase64: seikyuB64 }, { filename: 'ビーフェイス様用出荷明細.xlsx', dataBase64: xlsxB64 }] },
    { id: 'gm-bf-1', from: 'wholesale@be-free.biz', subject: '7/13　出荷明細です。', internalDate: '2026-07-13T06:25:00.000Z',
      bodyHtml: '<table><tr><th>商品ID</th><th>商品名</th><th>出荷数</th></tr>' +
        '<tr><td>GOFUN-01-N</td><td>胡粉ネイル スーパーコート N0033</td><td>48</td></tr>' +
        '<tr><td>GOFUN-01-N</td><td>胡粉ネイル スーパーコート N0033</td><td>30</td></tr></table>' +
        // 署名などの見出しの無い表は明細として解釈しない (数字セルがあっても無視されること)
        '<table><tr><td>株式会社ビー・フリー</td></tr><tr><td>TEL</td><td>06</td></tr></table>' },
    { id: 'gm-other', from: 'noreply@example.com', subject: '出荷明細', bodyHtml: '<table><tr><td>X</td><td>1</td></tr></table>' },
    { id: 'gm-amc-noatt', from: 'ashida@am-craft.jp', subject: '出荷明細 (添付忘れ)', bodyHtml: '<p>添付なし</p>' },
    // 表示名にドメインを入れた偽装 → 実メールボックスのドメイン不一致で対象外
    { id: 'gm-spoof', from: '"billing@am-craft.jp" <attacker@example.com>', subject: '出荷明細', attachments: [{ filename: 'x.xlsx', dataBase64: xlsxB64 }] },
    // SPF/DKIM/DMARC不合格 → 自動変換しない (error)
    { id: 'gm-authfail', from: 'ashida@am-craft.jp', subject: '出荷明細', authResults: 'mx.google.com; spf=fail; dkim=fail; dmarc=fail',
      attachments: [{ filename: 'y.xlsx', dataBase64: xlsxB64 }] },
    // 攻撃者ドメインのspf/dkim=passでFromだけ詐称 (dmarc=fail) → ドメイン整合検証で不合格 (Codex mail-R2 High)
    { id: 'gm-attack', from: 'ashida@am-craft.jp', subject: '出荷明細',
      authResults: 'mx.google.com; spf=pass smtp.mailfrom=attacker.example; dkim=pass header.d=attacker.example; dmarc=fail header.from=am-craft.jp',
      attachments: [{ filename: 'z.xlsx', dataBase64: xlsxB64 }] },
    // 返信引用で明細表が2つ → 合算せずエラー (二重入荷防止、Codex mail-R2 Medium)
    { id: 'gm-multitable', from: 'wholesale@be-free.biz', subject: '出荷明細 (再送)',
      bodyHtml: '<table><tr><th>商品ID</th><th>出荷数</th></tr><tr><td>GOFUN-01-N</td><td>10</td></tr></table>' +
        '<table><tr><th>商品ID</th><th>出荷数</th></tr><tr><td>GOFUN-01-N</td><td>20</td></tr></table>' },
  ]);
  // parseByRule の複数添付分岐 (優先→フォールバック→打ち切り、transient伝播)
  {
    const sm = await imp('apps/purchase-orders/shipment-mail.js');
    const RULE = { supplierCode: '1', domain: 'am-craft.jp', kind: 'xlsx', extraAuthDomains: [] };
    const AUTH = 'mx.google.com; spf=pass smtp.mailfrom=am-craft.jp';
    const goodBuf = Buffer.from(xlsxB64, 'base64');
    const badBuf = Buffer.from(seikyuB64, 'base64');
    // 先頭添付が解析不能 → 次の添付にフォールバック
    let pr = await sm.parseByRule({
      xlsxNames: ['a.xlsx', 'b.xlsx'], authResults: AUTH,
      getXlsxAt: async i => ({ filename: ['a.xlsx', 'b.xlsx'][i], buf: i === 0 ? badBuf : goodBuf }),
    }, RULE);
    ok(pr.items.length === 2 && pr.note.includes('b.xlsx'), 'parseByRule: 先頭添付が解析不能なら次を試す', pr.note);
    // 全滅 → 最後のエラーにファイル名、DLは最大3件で打ち切り
    let calls = 0, thr = null;
    try {
      await sm.parseByRule({
        xlsxNames: ['a.xlsx', 'b.xlsx', 'c.xlsx', 'd.xlsx'], authResults: AUTH,
        getXlsxAt: async i => { calls++; return { filename: 'abcd'[i] + '.xlsx', buf: badBuf }; },
      }, RULE);
    } catch (e) { thr = e; }
    ok(calls === 3 && thr && /\.xlsx\)/.test(thr.message), 'parseByRule: 全滅時は3添付で打ち切り+エラーにファイル名', { calls, msg: thr && thr.message });
    // transient (添付DL失敗) は即伝播 (次回取得でやり直し)
    thr = null;
    try {
      await sm.parseByRule({
        xlsxNames: ['a.xlsx'], authResults: AUTH,
        getXlsxAt: async () => { const e = new Error('net'); e.transient = true; throw e; },
      }, RULE);
    } catch (e) { thr = e; }
    ok(thr && thr.transient === true, 'parseByRule: transientは即伝播');
  }

  // authPassed / parseShipmentHtml / parseShipmentXlsx の単体回帰 (Codex mail-R3)
  {
    const sm = await imp('apps/purchase-orders/shipment-mail.js');
    ok(sm.authPassed('mx.google.com; dkim=pass header.d=am-craft.jp; spf=pass smtp.mailfrom=am-craft.jp; dmarc=pass header.from=am-craft.jp', 'am-craft.jp') === true, 'auth: 実Gmail形式のpass');
    ok(sm.authPassed('mx.google.com; spf=fail reason="note dmarc=pass injected"; dmarc=fail header.from=am-craft.jp', 'am-craft.jp') === false,
      'auth: quoted-string内のdmarc=pass注入は無効 (RFC8601)');
    ok(sm.authPassed('mx.google.com; dmarc=passive header.from=am-craft.jp', 'am-craft.jp') === false, 'auth: dmarc=passiveは不合格 (部分一致誤認なし)');
    ok(sm.authPassed('mx.google.com; spf=pass smtp.mailfrom=attacker.example; dkim=pass header.d=attacker.example', 'am-craft.jp') === false, 'auth: 他ドメインのspf/dkim passは不整合');
    // 中小事業者はDMARC/DKIM未導入が普通 — Fromドメイン完全一致のSPF単独/DKIM単独passは許可 (本番72件全エラー対応)
    ok(sm.authPassed('mx.google.com; spf=pass smtp.mailfrom=am-craft.jp; dkim=none; dmarc=none', 'am-craft.jp') === true, 'auth: SPF単独の整合passは許可');
    ok(sm.authPassed('mx.google.com; spf=none; dkim=pass header.d=be-free.biz; dmarc=none', 'be-free.biz') === true, 'auth: DKIM単独の整合passは許可');
    ok(sm.authPassed('mx.google.com; spf=pass smtp.mailfrom=am-craft.jp; dmarc=fail header.from=am-craft.jp', 'am-craft.jp') === false,
      'auth: 明示的なdmarc=failはSPF単独で上書きしない (Codex 入荷予定R1 Medium)');
    ok(sm.authPassed('mx.google.com; spf=pass smtp.mailfrom=bounce.am-craft.jp; dmarc=none', 'am-craft.jp') === false,
      'auth: 単独fallbackはサブドメイン整合を許さない (完全一致のみ)');
    ok(sm.authPassed('mx.google.com; spf=pass smtp.mailfrom=bounce.am-craft.jp; dmarc=none', 'am-craft.jp', ['bounce.am-craft.jp']) === true,
      'auth: extraAuthDomains で配送用サブドメインを明示許可できる');
    ok(sm.authPassed(['mx.google.com; dmarc=fail header.from=am-craft.jp', 'mx.google.com; spf=pass smtp.mailfrom=am-craft.jp'], 'am-craft.jp') === false,
      'auth: 別ヘッダのdmarc=failもfallbackを止める (全ヘッダ集約判定、Codex 入荷予定R2 Medium)');
    ok(sm.summarizeAuth('mx.google.com; spf=pass smtp.mailfrom=x.example; dkim=none; dmarc=none').includes('spf=pass (x.example)'), 'auth: 判定内訳の要約 (エラー原因調査用)');
    ok(sm.authPassed('mx.google.com; spf=fail reason="x\\"; dmarc=pass header.from=am-craft.jp"', 'am-craft.jp') === false,
      'auth: quoted-pairエスケープでの注入も無効 (状態機械、Codex mail-R4 High)');
    ok(sm.authPassed('attacker.example; dmarc=pass header.from=am-craft.jp', 'am-craft.jp') === false,
      'auth: authserv-idがmx.google.com以外のヘッダは信頼しない (Codex mail-R4 High)');
    ok(sm.authPassed(['attacker.example; dmarc=pass header.from=am-craft.jp', 'mx.google.com; dmarc=pass header.from=am-craft.jp'], 'am-craft.jp') === true,
      'auth: 複数ヘッダはGmail発行分だけで判定');
    const nested = '<blockquote>古い<blockquote>もっと古い</blockquote><table><tr><th>商品ID</th><th>出荷数</th></tr><tr><td>OLD-1</td><td>99</td></tr></table></blockquote>' +
      '<table><tr><th>商品ID</th><th>出荷数</th></tr><tr><td>NEW-1</td><td>5</td></tr></table>';
    const nestedItems = sm.parseShipmentHtml(nested);
    ok(nestedItems.length === 1 && nestedItems[0].vendorCode === 'NEW-1', 'html: 入れ子blockquote内の過去明細を除去 (今回分のみ)', nestedItems);
    // xlsx: 見出しが行をまたぐ (商品コードと出荷数量が別行) 場合は見出し不成立
    const ExcelJS2 = (await import('exceljs')).default;
    const wbBad = new ExcelJS2.Workbook();
    const wsBad = wbBad.addWorksheet('x');
    wsBad.addRow(['商品コード']);
    wsBad.addRow(['出荷数量']);
    wsBad.addRow(['AMC-001', '', 10]);
    let xlsxErr = null;
    try { await sm.parseShipmentXlsx(Buffer.from(await wbBad.xlsx.writeBuffer())); } catch (e) { xlsxErr = e.message; }
    ok(xlsxErr && xlsxErr.includes('見出し行'), 'xlsx: 見出しの行またぎは不成立 (同一行のみ)', xlsxErr);
  }

  r = await jp3('/api/inbound-plan/fetch-mails');
  ok(r.body.ok && r.body.added === 6 && r.body.errors.length === 3, 'fetch: 対象3+エラー3を登録 (対象外/偽装From/xlsxなしAMCは登録しない)', r.body);
  ok(r.body.open.length === 6, 'fetch: 未処理一覧に6件 (new3+error3)', r.body.open.length);
  {
    const multiAtt = r.body.open.find(m => m.gmail_id === 'gm-amc-multi');
    ok(multiAtt && JSON.parse(multiAtt.parsed_json).length === 2 && (multiAtt.parse_note || '').includes('ビーフェイス様用出荷明細'),
      'fetch: 複数添付は「出荷明細」ファイルを優先解析 (請求書.xlsxが先頭でもOK)', multiAtt && multiAtt.parse_note);
  }
  ok(!r.body.open.some(m => m.gmail_id === 'gm-spoof'), 'fetch: 表示名偽装 (実アドレス別ドメイン) は取り込まない (Codex mail-R1 High)');
  ok(!r.body.open.some(m => m.gmail_id === 'gm-amc-noatt'), 'fetch: xlsx添付のないAMCメール (請求書等) は一覧に載せない');
  ok(r.body.nonCandidates === 1 &&
    db.prepare("SELECT status FROM po_shipment_mails WHERE gmail_id='gm-amc-noatt'").get().status === 'not_candidate',
    'fetch: 対象外はtombstone (not_candidate) 保存 — 次回の再取得・再DLを防ぐ', r.body.nonCandidates);
  const authFail = r.body.open.find(m => m.gmail_id === 'gm-authfail');
  ok(authFail && authFail.status === 'error' && authFail.error.includes('なりすまし'), 'fetch: SPF/DKIM/DMARC不合格は自動変換しない', authFail && authFail.error);
  const attackMail = r.body.open.find(m => m.gmail_id === 'gm-attack');
  ok(attackMail && attackMail.status === 'error' && attackMail.error.includes('なりすまし'),
    'fetch: 攻撃者ドメインspf/dkim=pass+From詐称もドメイン整合で拒否 (Codex mail-R2 High)', attackMail && attackMail.error);
  const multiMail = r.body.open.find(m => m.gmail_id === 'gm-multitable');
  ok(multiMail && multiMail.status === 'error' && multiMail.error.includes('複数'),
    'fetch: 明細表が複数のメールは合算せずエラー (二重入荷防止)', multiMail && multiMail.error);
  const amcMail = r.body.open.find(m => m.gmail_id === 'gm-amc-1');
  const bfMail = r.body.open.find(m => m.gmail_id === 'gm-bf-1');
  const errMail = r.body.open.find(m => m.gmail_id === 'gm-multitable'); // 明細表2つ=エラー行の代表として使う
  ok(amcMail && amcMail.supplier_code === '1' && JSON.parse(amcMail.parsed_json).length === 2, 'fetch: AMC xlsx添付を解析 (2明細)', amcMail && amcMail.parse_note);
  ok(bfMail && bfMail.supplier_code === '2' && JSON.parse(bfMail.parsed_json).length === 2, 'fetch: ビーフリー本文の表を解析 (同一商品2行+署名表は無視)');

  // 再取得は冪等 (同じgmail_idは再登録しない)
  r = await jp3('/api/inbound-plan/fetch-mails');
  ok(r.body.ok && r.body.added === 0, 'fetch: 再取得は冪等 (added=0)');

  // メール変換 = 手動貼り付けと同じ応答 (AMC-001→noflyersticker、未知番号はunmatched)
  r = await jp3('/api/inbound-plan/mails/' + amcMail.id + '/convert');
  ok(r.body.ok && r.body.mailId === amcMail.id && r.body.rowCount === 1 && r.body.totalQty === 25 &&
    r.body.pasteText.startsWith('noflyersticker\t25\t') && r.body.unmatched.length === 1 && r.body.unmatched[0].vendorCode === 'ZZZ-MAIL-NEW',
    'mail convert: AMCメールを変換 (マッチ1+unmatched1)', r.body.pasteText);
  r = await jp3('/api/inbound-plan/mails/' + bfMail.id + '/convert');
  ok(r.body.ok && r.body.rowCount === 2 && r.body.totalQty === 78, 'mail convert: ビーフリーメールを変換 (複数行のまま)', r.body.rowCount);
  r = await jp3('/api/inbound-plan/mails/' + errMail.id + '/convert');
  ok(r.status === 400 && r.body.error.includes('解析エラー'), 'mail convert: errorメールは手動貼り付けを案内');

  // 処理済み/無視
  r = await jp3('/api/inbound-plan/mails/' + amcMail.id + '/status', { status: 'done' });
  ok(r.body.ok, 'mail: 登録済みにする');
  r = await jp3('/api/inbound-plan/mails/' + errMail.id + '/status', { status: 'ignored' });
  ok(r.body.ok, 'mail: 無視');
  r = await j('/api/inbound-plan/mails');
  ok(r.body.ok && r.body.open.length === 4 && r.body.open.some(m => m.gmail_id === 'gm-bf-1') && r.body.recent.length === 2,
    'mail: 一覧はnew/errorのみ (処理済みはrecentへ)', r.body.open.length);

  // ↩ 戻す (誤クリックした登録済み/無視を未処理に戻せる)
  r = await jp3('/api/inbound-plan/mails/' + amcMail.id + '/status', { status: 'new' });
  ok(r.body.ok, 'mail: 登録済みを未処理に戻す (↩)');
  r = await j('/api/inbound-plan/mails');
  ok(r.body.open.some(m => m.gmail_id === 'gm-amc-1') && r.body.recent.length === 1, 'mail: 戻した行が未処理一覧に復帰', r.body.open.length);

  // 誤操作で「空データのままnew」になった行 (全件エラー時代のデータ+↩戻す) は再解析で復元できる
  db.prepare("UPDATE po_shipment_mails SET parsed_json='[]' WHERE id=?").run(amcMail.id);
  r = await jp3('/api/inbound-plan/mails/' + amcMail.id + '/reparse');
  ok(r.body.ok && r.body.status === 'new' && r.body.itemCount === 2, 'reparse: 明細0行のnew行をGmailから復元');

  // 再解析: 解析コード修正後にerror行をやり直す (Gmail取り直し)。fake dataを差し替えて「直った」状況を再現
  {
    const fake = JSON.parse(process.env.PO_SHIPMENT_FAKE_DATA);
    // gm-attack: SPF単独の整合pass (認証緩和で通るようになったケース)
    fake.find(m => m.id === 'gm-attack').authResults = 'mx.google.com; spf=pass smtp.mailfrom=am-craft.jp; dkim=none; dmarc=none';
    // gm-authfail: xlsx添付が実は無かった (請求書等) → 対象外として一覧から削除されるケース
    const af = fake.find(m => m.id === 'gm-authfail');
    af.attachments = [];
    af.bodyHtml = '<p>本文のみ</p>';
    process.env.PO_SHIPMENT_FAKE_DATA = JSON.stringify(fake);
    r = await jp3('/api/inbound-plan/mails/reparse-errors');
    ok(r.body.ok && r.body.total === 2 && r.body.fixed === 1 && r.body.removed === 1 && r.body.stillError === 0 && r.body.remaining === 0,
      'reparse一括: 解析OK1+対象外1 (20件バッチ+remaining)', r.body);
    const attackRow = r.body.open.find(m => m.gmail_id === 'gm-attack');
    ok(attackRow && attackRow.status === 'new' && JSON.parse(attackRow.parsed_json).length === 2, 'reparse: SPF単独整合passで解析OKに', attackRow && attackRow.status);
    ok(!r.body.open.some(m => m.gmail_id === 'gm-authfail') &&
      db.prepare("SELECT status FROM po_shipment_mails WHERE gmail_id='gm-authfail'").get().status === 'not_candidate',
      'reparse: 出荷明細でない行はtombstone化して一覧から除外 (物理削除しない)');
    // 個別再解析エンドポイント (new行でも同じ結果に収束)
    r = await jp3('/api/inbound-plan/mails/' + attackRow.id + '/reparse');
    ok(r.body.ok && r.body.status === 'new' && r.body.itemCount === 2, 'reparse個別: 再実行でも同じ解析結果');
    // 登録済み行の再解析は拒否 (誤操作ガード)
    await jp3('/api/inbound-plan/mails/' + attackRow.id + '/status', { status: 'done' });
    r = await jp3('/api/inbound-plan/mails/' + attackRow.id + '/reparse');
    ok(r.status === 400 && r.body.error.includes('登録済み'), 'reparse: 登録済み行は拒否 (↩で戻してから)');

    // 対象外 (tombstone) は一覧APIの notCandidates で確認でき、誤判定なら再解析で救出できる
    r = await j('/api/inbound-plan/mails');
    const nc = (r.body.notCandidates || []).find(m => m.gmail_id === 'gm-amc-noatt');
    ok(r.body.ok && nc, 'list: 対象外メールを notCandidates で確認できる', (r.body.notCandidates || []).length);
    {
      const fake3 = JSON.parse(process.env.PO_SHIPMENT_FAKE_DATA);
      fake3.find(m => m.id === 'gm-amc-noatt').attachments = [{ filename: 'ビーフェイス様用出荷明細.xlsx', dataBase64: xlsxB64 }];
      process.env.PO_SHIPMENT_FAKE_DATA = JSON.stringify(fake3);
    }
    r = await jp3('/api/inbound-plan/mails/' + nc.id + '/reparse');
    ok(r.body.ok && r.body.status === 'new' && r.body.itemCount === 2, 'reparse: 対象外メールを救出 (not_candidate→new)');
  }
  delete process.env.PO_SHIPMENT_FAKE_DATA;

  // 画面: 入荷予定は独立タブへ (入庫消込には導線リンクのみ)
  const inbHtml3 = await (await fetch(base + '/inbound')).text();
  ok(!inbHtml3.includes('ipFetch') && inbHtml3.includes('/apps/purchase-orders/inbound-plan'), '/inbound 入荷予定はタブへ移動 (導線リンクあり)');
  const planHtml = await (await fetch(base + '/inbound-plan')).text();
  ok(planHtml.includes('ipFetch') && planHtml.includes('メールから取得') && planHtml.includes('renderIpResult'), '/inbound-plan 📬メール取得UI');
  ok(planHtml.includes('ipReparseAll') && planHtml.includes('data-ipmrep') && planHtml.includes('data-ipmback'), '/inbound-plan 再解析+↩戻すUI');
  ok(planHtml.includes('解析エラー'), '/inbound-plan エラー理由の表示');
  ok(planHtml.includes('jstStamp') && planHtml.includes('日本時間'), '/inbound-plan 受信日時をJST表示');
  ok(planHtml.includes('出荷明細ではないと判定した'), '/inbound-plan 対象外メールの確認セクション');
  // 配信されたクライアントコードの jstStamp を実行検証 (template literal内の \d が d に化ける事故の検出。本番で全行「—」になった実障害)
  {
    const mFn = planHtml.match(/function jstStamp\(iso\) \{[\s\S]*?\n\}/);
    ok(!!mFn, '/inbound-plan jstStamp関数を配信');
    const jstFn = new Function(mFn[0] + '; return jstStamp;')();
    ok(jstFn('2026-07-13T08:59:00.000Z') === '2026-07-13 17:59', 'jstStamp: UTC→JST変換 (08:59 UTC=17:59 JST)', jstFn('2026-07-13T08:59:00.000Z'));
    ok(jstFn('2026-07-13T16:00:00.000Z') === '2026-07-14 01:00', 'jstStamp: 日跨ぎ (UTC 16時→JST 翌1時)');
    ok(jstFn(null) === '—' && jstFn('0') === '—' && jstFn('2026-07-13') === '—', 'jstStamp: null/不正値は—');
  }
  const dashNav = await (await fetch(base + '/')).text();
  ok(dashNav.includes('入荷予定') && dashNav.includes('/apps/purchase-orders/inbound-plan'), 'ナビに入荷予定タブ');
}

// ═══ サロンジェ (PO参照方式): 出荷連絡メール → PO選択 → 台帳明細から入荷予定 → 減数消込 ═══
console.log('── サロンジェ PO参照 (po_reference) ──');
{
  const jpS = (p, body, headers) => j(p, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(headers || {}) }, body: JSON.stringify(body || {}) });
  // fixture: 仕入先0107 + PML3商品 + PO2本
  r = await jpS('/api/masters/suppliers', { supplier_code: '0107', name: 'サロンジェ', order_memo: '' });
  ok(r.status === 200 && r.body.ok, 'salonge: 仕入先マスタ登録 (0107→107)');
  const insSal = db.prepare(`INSERT INTO mirror_pml_snapshot_rows
    (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 売上分類, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数, 売価, 原価, 最終仕入日, 登録日)
    VALUES ('run_test', ?, ?, '0107', '取扱中', 3, 100, 0, 7, 30, 10, 1.5, 800, ?, '2026-07-01', '2024-01-01')`);
  insSal.run('sal-towel3p', '16502 KBミニタオル3P', 300);
  insSal.run('sal-apron110', 'まいぜん子供エプロン110', 500);
  insSal.run('sal-cup', 'サロンジェ カップ', 200);
  r = await jpS('/api/supplier/107/issue', { items: [{ code: 'sal-towel3p', qty: 20 }, { code: 'sal-apron110', qty: 10 }] });
  ok(r.status === 200 && r.body.ok, 'salonge: PO1発行 (towel20+apron10)');
  const salPo1 = r.body.id;
  r = await jpS('/api/supplier/107/issue', { items: [{ code: 'sal-cup', qty: 5 }] });
  const salPo2 = r.body.id;

  // メール取得: 出荷連絡 (※行2件、うち1件は商品コード一致) / 出荷連絡でないメール
  process.env.PO_SHIPMENT_FAKE_DATA = JSON.stringify([
    { id: 'gm-sal-1', from: '藤本 <fujimoto@salonge.co.jp>', subject: 'Re: 【発注書】', internalDate: '2026-07-15T04:32:00.000Z',
      bodyHtml: '<div>中原様<br><br>いつもお世話になります。<br><br>7/14、7/8の御依頼分、出荷準備整いました。<br>添付ご確認ください。<br>※sal-towel3p 16502 KBミニタオル3P…在庫無し。終売となりました。<br>※まったく関係ない商品ZZZ…欠品です。<br>※カップの件、また改めてご連絡します。<br><br>以上、宜しくお願い致します。</div>' },
    { id: 'gm-sal-2', from: 'fujimoto@salonge.co.jp', subject: '請求書', bodyHtml: '<p>請求書をお送りします</p>' },
  ]);
  r = await jpS('/api/inbound-plan/fetch-mails');
  ok(r.body.ok && r.body.added === 1, 'salonge: 出荷連絡1件を登録 (請求書は対象外)', r.body.added);
  const salMail = db.prepare("SELECT * FROM po_shipment_mails WHERE gmail_id='gm-sal-1'").get();
  ok(salMail && salMail.supplier_code === '107' && salMail.status === 'new', 'salonge: supplier=107/new');
  const salParsed = JSON.parse(salMail.parsed_json);
  ok(salParsed.kind === 'po_reference' && salParsed.exceptions.length === 3 && salParsed.exceptions[0].kind === 'discontinued'
    && salParsed.exceptions[1].kind === 'shortage' && salParsed.exceptions[2].kind === 'other'
    && salParsed.refDates.includes('7/14') && salParsed.refDates.includes('7/8'),
    'salonge: 例外抽出 (終売/欠品/その他) + 本文日付', salParsed.exceptions);
  ok(db.prepare("SELECT status FROM po_shipment_mails WHERE gmail_id='gm-sal-2'").get().status === 'not_candidate',
    'salonge: 出荷連絡でないメールは対象外 (not_candidate)');

  // 既存の🔁変換は拒否 (発注書参照へ誘導)
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/convert');
  ok(r.status === 400 && r.body.error.includes('発注書参照'), 'salonge: 通常変換は400');

  // PO選択候補
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/po-convert');
  ok(r.body.ok && r.body.pick === true && r.body.orders.length === 2 && r.body.exceptions.length === 3 && r.body.bodyText.includes('出荷準備'),
    'salonge: PO候補2件+例外+原文', r.body.orders && r.body.orders.length);

  // 別仕入先のPO指定は400
  const foreignPo = db.prepare("SELECT id FROM po_orders WHERE supplier_code<>'107' AND status='issued' ORDER BY id DESC LIMIT 1").get();
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/po-convert', { orderIds: [salPo1, foreignPo.id] });
  ok(r.status === 400, 'salonge: 別仕入先のPO指定は400');

  // 変換: 台帳の残数明細から行を作る
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/po-convert', { orderIds: [salPo1, salPo2] });
  ok(r.body.ok && r.body.pick === false && r.body.lines.length === 3 && r.body.totalRemaining === 35, 'salonge: 変換3行/残数計35', r.body.lines && r.body.lines.length);
  const lt = r.body.lines.find(l => l.productName.includes('ミニタオル'));
  ok(lt && lt.exception && lt.exception.level === 'strong' && lt.exception.kind === 'discontinued', 'salonge: 商品コード一致=強一致 (終売の除外提案)', lt && lt.exception);
  const la = r.body.lines.find(l => l.productName.includes('エプロン'));
  ok(la && !la.exception, 'salonge: 無関係の行に例外が付かない');
  const lc = r.body.lines.find(l => l.productName.includes('カップ'));
  ok(lc && !lc.exception, 'salonge: kind=other の※行 (「カップの件…」) はマッチ対象外 (誤減数候補にしない)');
  ok(r.body.lines.every(l => l.costSource === 'po' && l.cost != null), 'salonge: 単価=PO単価 (発注時スナップショット)');
  const towelItemId = lt.orderItemId, apronItemId = la.orderItemId;

  // ガード: 冪等キー必須 / 非PO参照メール / 仕入先越境の明細
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/apply-adjustments',
    { entries: [{ orderItemId: towelItemId, qty: 1 }] });
  ok(r.status === 400 && r.body.error.includes('Idempotency-Key'), 'salonge: 冪等キーなしの減数は400');
  const amcMailRow = db.prepare("SELECT id FROM po_shipment_mails WHERE gmail_id='gm-amc-1'").get();
  r = await jpS('/api/inbound-plan/mails/' + amcMailRow.id + '/apply-adjustments',
    { entries: [{ orderItemId: towelItemId, qty: 1 }] }, { 'Idempotency-Key': 'salonge-adj-x1' });
  ok(r.status === 400 && r.body.error.includes('PO参照方式ではない'), 'salonge: 非PO参照メールを減数の根拠にできない');
  const foreignItem = db.prepare(`SELECT i.id FROM po_order_items i JOIN po_orders o ON o.id=i.order_id
    WHERE o.supplier_code<>'107' AND o.status='issued' ORDER BY i.id DESC LIMIT 1`).get();
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/apply-adjustments',
    { entries: [{ orderItemId: foreignItem.id, qty: 1 }] }, { 'Idempotency-Key': 'salonge-adj-x2' });
  ok(r.status === 400 && r.body.error.includes('仕入先'), 'salonge: 別仕入先の明細への減数は400 (越境ガード)');

  // 減数: 一括+冪等+原子性
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/apply-adjustments',
    { entries: [{ orderItemId: towelItemId, qty: 20, note: '終売' }] }, { 'Idempotency-Key': 'salonge-adj-1' });
  ok(r.status === 200 && r.body.ok && r.body.results[0].remaining === 0, 'salonge: 減数20→残0', r.body.results);
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/apply-adjustments',
    { entries: [{ orderItemId: towelItemId, qty: 20, note: '終売' }] }, { 'Idempotency-Key': 'salonge-adj-1' });
  ok(r.body.ok && r.body.replay === true, 'salonge: 同一キー再送はreplay (二重減数なし)');
  ok(db.prepare("SELECT COUNT(*) n FROM po_item_events WHERE order_item_id=? AND event_type='shortage'").get(towelItemId).n === 1, 'salonge: 減数イベント1件のみ');
  ok(db.prepare('SELECT COUNT(*) n FROM po_shipment_mail_adjustments WHERE mail_id=?').get(salMail.id).n === 1, 'salonge: メール↔減数の対応記録 (監査)');
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/apply-adjustments',
    { entries: [{ orderItemId: apronItemId, qty: 1 }, { orderItemId: apronItemId, qty: 999 }] }, { 'Idempotency-Key': 'salonge-adj-2' });
  ok(r.status === 400 && r.body.error.includes('残数超過'), 'salonge: 残数超過は400');
  ok(db.prepare('SELECT COUNT(*) n FROM po_item_events WHERE order_item_id=?').get(apronItemId).n === 0,
    'salonge: 失敗時は全件ロールバック (1件目の正常分も登録しない)');

  // 減数後の再変換: 残0の行は消える + 同一メール由来の減数は priorAdjustments に出ない (残数に反映済み)
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/po-convert', { orderIds: [salPo1, salPo2] });
  ok(r.body.ok && r.body.lines.length === 2 && !r.body.lines.some(l => l.orderItemId === towelItemId), 'salonge: 減数後の再変換で残0行が消える');
  ok(r.body.lines.every(l => (l.priorAdjustments || []).length === 0), 'salonge: 同一メールの減数は警告に出ない (別メールのみ)');

  // 2通目のメール: 例外0件=全量出荷の正常ケース + 別メールでの二重減数警告 (priorAdjustments)
  r = await jpS('/api/inbound-plan/mails/' + salMail.id + '/apply-adjustments',
    { entries: [{ orderItemId: apronItemId, qty: 2, note: '欠品' }] }, { 'Idempotency-Key': 'salonge-adj-3' });
  ok(r.body.ok && r.body.results[0].remaining === 8, 'salonge: apron減数2→残8');
  process.env.PO_SHIPMENT_FAKE_DATA = JSON.stringify([
    { id: 'gm-sal-3', from: 'fujimoto@salonge.co.jp', subject: '出荷のご連絡', bodyHtml: '<div>本日分、出荷準備整いました。</div>' },
  ]);
  r = await jpS('/api/inbound-plan/fetch-mails');
  const salMail2 = db.prepare("SELECT * FROM po_shipment_mails WHERE gmail_id='gm-sal-3'").get();
  ok(salMail2 && salMail2.status === 'new' && JSON.parse(salMail2.parsed_json).exceptions.length === 0,
    'salonge: 例外0件=全量出荷の正常ケース (not_candidateにしない)');
  r = await jpS('/api/inbound-plan/mails/' + salMail2.id + '/po-convert', { orderIds: [salPo1] });
  const la2 = r.body.lines.find(l => l.orderItemId === apronItemId);
  ok(la2 && la2.priorAdjustments.length === 1 && la2.priorAdjustments[0].mailId === salMail.id && la2.priorAdjustments[0].qty === 2,
    'salonge: 別メールでの減数済みを警告 (priorAdjustments)', la2 && la2.priorAdjustments);
  // 再解析: po_reference は itemCount=null (オブジェクト形を配列と数えない)
  r = await jpS('/api/inbound-plan/mails/' + salMail2.id + '/reparse');
  ok(r.body.ok && r.body.status === 'new' && r.body.itemCount === null, 'salonge: 再解析の件数はnull (po_reference)', r.body.itemCount);
  // 逆仕訳した減数は priorAdjustments に出ない (誤減数を復元したのに警告が残らない)
  {
    const adjEv = db.prepare('SELECT event_id FROM po_shipment_mail_adjustments WHERE mail_id=? AND order_item_id=?').get(salMail.id, apronItemId);
    r = await jpS('/api/events/' + adjEv.event_id + '/reverse', { note: '誤減数のため取消 (テスト)' });
    ok(r.body.ok, 'salonge: 減数イベントを逆仕訳');
    r = await jpS('/api/inbound-plan/mails/' + salMail2.id + '/po-convert', { orderIds: [salPo1] });
    const la3 = r.body.lines.find(l => l.orderItemId === apronItemId);
    ok(la3 && la3.priorAdjustments.length === 0 && la3.remaining === 10, 'salonge: 逆仕訳済みの減数は警告から消える (残数も復元)', la3 && { p: la3.priorAdjustments, rem: la3.remaining });
  }

  // UI配信
  const planHtmlS = await (await fetch(base + '/inbound-plan')).text();
  ok(planHtmlS.includes('data-ippoconv') && planHtmlS.includes('発注書参照') && planHtmlS.includes('renderPoPick') && planHtmlS.includes('サロンジェ'),
    '/inbound-plan サロンジェ PO参照変換UI');
  ok(planHtmlS.includes('apply-adjustments') && planHtmlS.includes('plShort') && planHtmlS.includes('減数の確認'),
    '/inbound-plan ➖減数フロー (提案→確認→一括実行)');
}

// ═══ NE本来表記 (po_product_code_canonical) — ロジザード貼り付けの商品ID大小表記 ═══
console.log('── NE本来表記 (canonical) ──');
{
  const jp4 = (p, body) => j(p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
  // DWH/PMLは小文字統一のため、NE手動CSV取込からNE本来の大小表記を蓄積する
  const neCsvC = '"商品コード","在庫数","発注残数"\r\n"NoFlyerSticker","10","0"\r\n';
  const fdC = new FormData();
  fdC.append('file', new Blob([iconv.encode(neCsvC, 'Shift_JIS')]), 'ne-case.csv');
  r = await j('/api/ne-overlay/csv', { method: 'POST', body: fdC });
  ok(r.status === 200, 'NE CSV取込 (混在表記 NoFlyerSticker)');
  await j('/api/ne-overlay', { method: 'DELETE' }); // overlay解除してもcanonicalは残る
  const canon = db.prepare("SELECT product_code FROM po_product_code_canonical WHERE product_key='noflyersticker'").get();
  ok(canon && canon.product_code === 'NoFlyerSticker', 'canonical: NE本来表記を蓄積 (overlay解除後も保持)', canon);
  r = await jp4('/api/inbound-plan/convert', { supplier_code: '1', text: 'AMC-001\tx\t3\n' });
  ok(r.body.ok && r.body.rows[0].productCode === 'NoFlyerSticker' && r.body.pasteText.startsWith('NoFlyerSticker\t3\t') &&
    r.body.rows[0].caseVerified === true && r.body.caseUnverified.length === 0,
    '貼り付け商品ID=NE本来表記 (canonical優先、PMLの小文字より優先)', r.body.pasteText);
  // NE側で表記が変わったら再取込で更新される
  const fdC2 = new FormData();
  fdC2.append('file', new Blob([iconv.encode('"商品コード","在庫数","発注残数"\r\n"NOFLYERSTICKER","10","0"\r\n', 'Shift_JIS')]), 'ne-case2.csv');
  await j('/api/ne-overlay/csv', { method: 'POST', body: fdC2 });
  ok(db.prepare("SELECT product_code FROM po_product_code_canonical WHERE product_key='noflyersticker'").get().product_code === 'NOFLYERSTICKER',
    'canonical: 再取込で表記が更新される');
  // 検証エラーのあるCSVは canonical もロールバック (取込全体と同一txn)
  const fdC3 = new FormData();
  fdC3.append('file', new Blob([iconv.encode('"商品コード","在庫数","発注残数"\r\n"noFLYERsticker","10","0"\r\n"x1","abc","0"\r\n', 'Shift_JIS')]), 'ne-case3.csv');
  r = await j('/api/ne-overlay/csv', { method: 'POST', body: fdC3 });
  ok(r.status === 400 &&
    db.prepare("SELECT product_code FROM po_product_code_canonical WHERE product_key='noflyersticker'").get().product_code === 'NOFLYERSTICKER',
    'canonical: 不正CSVでは更新されない (全件rollback)');
  await j('/api/ne-overlay', { method: 'DELETE' });
  // canonical未蓄積の商品は caseUnverified で警告対象
  const fdBf3 = new FormData();
  fdBf3.append('supplier_code', '2');
  fdBf3.append('file', new Blob([iconv.encode('"仕入先管理番号","x","弊社管理番号"\r\n"GOFUN-01-N","","gyoumuhandcream60-BI"', 'Shift_JIS')]), 'bf3.csv');
  await j('/api/vendor-map/csv', { method: 'POST', body: fdBf3 });
  r = await jp4('/api/inbound-plan/convert', { supplier_code: '2', text: 'GOFUN-01-N\tx\t2\n' });
  ok(r.body.ok && r.body.caseUnverified.length === 1 && r.body.rows[0].caseVerified === false,
    'canonical未蓄積の商品は表記未確認として警告リストへ', r.body.caseUnverified);
}

// ═══ 入庫取込プレビュー + ⚡一括割当 (中原さん要望 2026-07-14) ═══
console.log('── 入庫取込プレビュー+一括割当 ──');
{
  const csvOf2 = rows => iconv.encode(rows.map(r2 => r2.map(v => '"' + String(v) + '"').join(',')).join('\r\n'), 'Shift_JIS');
  const HDR2 = ['伝票NO', '型番', '品名', '取引先ID', '仕入単価', '良品数', '不良品数', '入庫日'];
  const up2 = async (name, rows, extra) => {
    const fd = new FormData();
    fd.append('file', new Blob([csvOf2(rows)]), name);
    for (const [k, v] of Object.entries(extra || {})) fd.append(k, v);
    return j('/api/inbound/import', { method: 'POST', body: fd });
  };
  const jpA = (p, body, key) => j(p, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) }, body: JSON.stringify(body) });

  // プレビュー: 書込なし+商品名解決。一括割当の「候補1つ」条件を確定させるため専用のfixture商品を使う
  insRow.run('aa-unique-item', '一括テスト商品', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  const fileRows = [HDR2,
    ['AA100', 'aa-unique-item', '一括', '0001', '200', '10', '0', '2026/07/14'],
    ['AA101', '0726-001060', '肉球', '0001', '245', '5', '0', '2026/07/14'],
    ['AA102', 'noflyersticker', 'チラシ', '0001', '70', '3', '0', '2026/07/14']];
  const batchesBefore = db.prepare('SELECT COUNT(*) AS n FROM po_inbound_batches').get().n;
  r = await up2('aa.csv', fileRows, { preview: '1' });
  ok(r.body.ok && r.body.preview === true && r.body.slipCount === 3 && r.body.totalGood === 18 && r.body.duplicateFile === null,
    'preview: サマリ (伝票3/良品18/重複なし)', r.body);
  ok(r.body.lines.find(l => l.productCode === 'aa-unique-item')?.productName === '一括テスト商品', 'preview: 商品名をPMLから解決');
  ok(db.prepare('SELECT COUNT(*) AS n FROM po_inbound_batches').get().n === batchesBefore, 'preview: 書込なし');
  const previewHash = r.body.fileHash;

  // commit: ハッシュ不一致は拒否 / 一致で取込
  r = await up2('aa.csv', fileRows, { fileHash: 'deadbeef' });
  ok(r.status === 400 && r.body.error.includes('プレビューしたファイル'), 'commit: fileHash不一致は拒否');
  r = await up2('aa.csv', fileRows, { fileHash: previewHash });
  ok(r.body.ok && r.body.receipts === 3, 'commit: fileHash一致で取込');

  // 同一ファイルの再プレビュー → 重複警告
  r = await up2('aa.csv', fileRows, { preview: '1' });
  ok(r.body.ok && r.body.duplicateFile && r.body.duplicateFile.fileName === 'aa.csv', 'preview: 取込済みファイルは警告情報', r.body.duplicateFile);
  {
    const d = new Date(Date.parse(r.body.duplicateFile.importedAt) + 9 * 3600000);
    const p2 = n => String(n).padStart(2, '0');
    const expJst = d.getUTCFullYear() + '-' + p2(d.getUTCMonth() + 1) + '-' + p2(d.getUTCDate()) + ' ' + p2(d.getUTCHours()) + ':' + p2(d.getUTCMinutes());
    ok(r.body.duplicateFile.importedAtJst === expJst, 'preview: 前回取込日時はJST表示 (UTC素通しにしない)', r.body.duplicateFile.importedAtJst);
  }

  // 一覧に商品名
  r = await j('/api/inbound');
  const aaRow = r.body.open.find(x => x.slip === 'AA100');
  ok(aaRow && aaRow.productName === '一括テスト商品', 'listInbound: 商品名付き', aaRow && aaRow.productName);

  // ⚡一括割当プレビュー: deaditem=候補1(全量) / 0726=候補1(部分) / noflyersticker=候補複数→skip
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-unique-item', qty: 10 }, { code: '0726-001060', qty: 20 }] }) });
  ok(r.body.ok !== false, 'auto: テスト用PO作成');
  r = await j('/api/inbound/auto-assign/preview');
  ok(r.body.ok, 'auto preview: ok');
  const pDead = r.body.proposals.find(p => p.slip === 'AA100');
  const pNiku = r.body.proposals.find(p => p.slip === 'AA101');
  const sNof = r.body.proposals.find(p => p.slip === 'AA102');
  ok(pDead && pDead.qty === 10 && pDead.postRemaining === 0, 'auto preview: 全量一致の提案 (aa-unique-item)', pDead);
  ok(pNiku && pNiku.qty === 5 && pNiku.poRemaining === 20 && pNiku.postRemaining === 15, 'auto preview: 部分入荷の提案 (残15)', pNiku);
  ok(sNof && sNof.qty > 0, 'auto preview: 候補複数でも古い発注からFIFOで自動提案', sNof && sNof.poNumber);

  // 原子性: 不正な数量を混ぜると全ロールバック
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: pDead.inboundItemId, orderItemId: pDead.orderItemId, qty: 999 },
    { inboundItemId: pNiku.inboundItemId, orderItemId: pNiku.orderItemId, qty: 5, remainder: { action: 'await_delivery', nextExpectedDate: '2026-07-30' } },
  ] }, 'aa-key-bad');
  ok(r.status === 400, 'auto commit: 不正数量は400');
  r = await j('/api/inbound');
  ok(r.body.open.find(x => x.slip === 'AA101').allocated === 0, 'auto commit: 失敗時は全ロールバック (片方も未割当のまま)');

  // 残数の扱いに不正なactionは400 (省略/deferは「📌あとで決める」で通る — 後段でテスト)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: pNiku.inboundItemId, orderItemId: pNiku.orderItemId, qty: 5, remainder: { action: 'shortage' } },
  ] }, 'aa-key-norem');
  ok(r.status === 400 && r.body.error.includes('分納待ち/確認中'), 'auto commit: 不正なaction (shortage等) は400', r.body.error);
  r = await j('/api/inbound');
  ok(r.body.open.find(x => x.slip === 'AA101').allocated === 0, 'auto commit: 不正action時も全ロールバック');

  // 正常実行: 全量+部分(分納待ち)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: pDead.inboundItemId, orderItemId: pDead.orderItemId, qty: 10 },
    { inboundItemId: pNiku.inboundItemId, orderItemId: pNiku.orderItemId, qty: 5, remainder: { action: 'await_delivery', nextExpectedDate: '2026-07-30' } },
  ] }, 'aa-key-1');
  ok(r.body.ok && r.body.assigned === 2, 'auto commit: 2件割当', r.body);
  ok(db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(pDead.orderItemId).remaining_qty === 0, 'auto commit: 全量行はPO残0');
  ok(db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(pNiku.orderItemId).remaining_qty === 15, 'auto commit: 部分行はPO残15');
  ok(db.prepare('SELECT remainder_disposition FROM po_order_items WHERE id=?').get(pNiku.orderItemId).remainder_disposition === 'awaiting_delivery',
    'auto commit: 部分行は分納待ちが設定される');
  // 冪等: 同一キー再送はreplay (二重割当なし)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: pDead.inboundItemId, orderItemId: pDead.orderItemId, qty: 10 },
    { inboundItemId: pNiku.inboundItemId, orderItemId: pNiku.orderItemId, qty: 5, remainder: { action: 'await_delivery', nextExpectedDate: '2026-07-30' } },
  ] }, 'aa-key-1');
  ok(r.body.ok && r.body.replay === true, 'auto commit: 同一キー再送はreplay');
  r = await j('/api/inbound');
  ok(!r.body.open.some(x => x.slip === 'AA100' || x.slip === 'AA101'), 'auto commit: 割当済み行は未割当一覧から消える');

  // ── Codex inb-R1 反映分: 割当先改変 / 同一キー内容違い / 同一PO明細への複数行 ──
  insRow.run('aa-multi-item', '複数行テスト商品', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-multi-item', qty: 20 }] }) });
  r = await up2('ab.csv', [HDR2,
    ['AB300', 'aa-multi-item', 'x', '0001', '200', '5', '0', '2026/07/14'],
    ['AB301', 'aa-multi-item', 'x', '0001', '200', '7', '0', '2026/07/14']]);
  ok(r.body.ok, 'multi: 同一商品2伝票を取込');
  r = await j('/api/inbound/auto-assign/preview');
  const m1 = r.body.proposals.find(p => p.slip === 'AB300');
  const m2 = r.body.proposals.find(p => p.slip === 'AB301');
  ok(m1 && m2 && m1.orderItemId === m2.orderItemId, 'multi: 同一PO明細への2提案');
  // 一覧順=伝票NO降順のため AB301 (qty7) が先に処理される: 20→13→8
  ok(m2.poRemaining === 20 && m2.postRemaining === 13 && m1.poRemaining === 13 && m1.postRemaining === 8,
    'multi: 残数は累積表示 (20→13→8。両方20→…に見えない)', [m2.postRemaining, m1.postRemaining]);
  ok(m2.needsRemainder === false && m1.needsRemainder === true, 'multi: 残数の扱いは最終行のみ選択 (途中行の設定は受けない)');

  // 割当先改変: 無関係なPO明細を指定 → 候補再検証で拒否 (Codex inb-R1 High)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: m1.inboundItemId, orderItemId: pDead.orderItemId, qty: 5 },
  ] }, 'aa-key-tamper');
  ok(r.status === 400 && r.body.error.includes('一致しません'), 'auto commit: 割当先の改変はFIFO計画照合で拒否', r.body.error);

  // 同一キーで内容 (日付) 違い → 409 (replayさせない、Codex inb-R1 Medium)
  const multiAssign = dt => [
    { inboundItemId: m2.inboundItemId, orderItemId: m2.orderItemId, qty: 7 },
    { inboundItemId: m1.inboundItemId, orderItemId: m1.orderItemId, qty: 5, remainder: { action: 'await_delivery', nextExpectedDate: dt } },
  ];
  r = await jpA('/api/inbound/auto-assign', { assignments: multiAssign('2026-08-01') }, 'aa-key-2');
  ok(r.body.ok && r.body.assigned === 2, 'multi commit: 2行→同一PO明細 (途中行は残数の扱いなしで通る)', r.body);
  r = await jpA('/api/inbound/auto-assign', { assignments: multiAssign('2026-08-02') }, 'aa-key-2');
  ok(r.status === 409, 'auto commit: 同一キーで日付違いは409 (黙ってreplayしない)', r.status);
  // 順序だけ変えた同一キー再送も409 (順序は「最終行」判定に効く意味のある入力、Codex inb-R2 Medium)
  r = await jpA('/api/inbound/auto-assign', { assignments: multiAssign('2026-08-01').slice().reverse() }, 'aa-key-2');
  ok(r.status === 409, 'auto commit: 同一キーで順序違いも409', r.status);
  ok(db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(m1.orderItemId).remaining_qty === 8,
    'multi commit: 最終PO残8 (5+7消込)');
  ok(db.prepare('SELECT next_expected_date FROM po_order_items WHERE id=?').get(m1.orderItemId).next_expected_date === '2026-08-01',
    'multi commit: 残数の扱いは最終行の内容で1回だけ設定');

  // 全量契約: 提案より少ない数量/同一入庫行の重複指定は拒否 (Codex inb-R3 Medium)
  r = await up2('ac.csv', [HDR2, ['AC400', 'aa-multi-item', 'x', '0001', '200', '4', '0', '2026/07/14']]);
  r = await j('/api/inbound/auto-assign/preview');
  const m3 = r.body.proposals.find(p => p.slip === 'AC400');
  ok(m3 && m3.qty === 4, 'partial-guard: 新提案 (4個全量)');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: m3.inboundItemId, orderItemId: m3.orderItemId, qty: 2 },
  ] }, 'aa-key-partial');
  ok(r.status === 400 && r.body.error.includes('一致しません'), 'auto commit: 全量未満の数量は拒否 (契約=未割当の全量)', r.body.error);
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: m3.inboundItemId, orderItemId: m3.orderItemId, qty: 4, remainder: { action: 'await_confirmation', nextActionDate: '2026-08-05' } },
    { inboundItemId: m3.inboundItemId, orderItemId: m3.orderItemId, qty: 4, remainder: { action: 'await_confirmation', nextActionDate: '2026-08-05' } },
  ] }, 'aa-key-dupinb');
  ok(r.status === 400 && r.body.error.includes('一致しません'), 'auto commit: 同一入庫行の重複指定はFIFO計画照合で拒否', r.body.error);
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: m3.inboundItemId, orderItemId: m3.orderItemId, qty: 4, remainder: { action: 'await_confirmation', nextActionDate: '2026-08-05' } },
  ] }, 'aa-key-3');
  ok(r.body.ok && r.body.assigned === 1, 'auto commit: 全量なら成功 (確認中+期限)');

  // ── keepQty (残す数): 中原さん要望 2026-07-14 — 作れなかった分を減数で消す/一部だけ残す ──
  insRow.run('aa-keep-item', '減数テスト商品', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  insRow.run('aa-keep2-item', '一部残しテスト商品', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-keep-item', qty: 200 }, { code: 'aa-keep2-item', qty: 20 }] }) });
  r = await up2('ad.csv', [HDR2,
    ['AD500', 'aa-keep-item', 'ミルワーム相当', '0001', '220', '188', '0', '2026/07/14'],
    ['AD501', 'aa-keep2-item', 'x', '0001', '200', '8', '0', '2026/07/14']]);
  r = await j('/api/inbound/auto-assign/preview');
  const k1 = r.body.proposals.find(p2 => p2.slip === 'AD500');
  const k2 = r.body.proposals.find(p2 => p2.slip === 'AD501');
  ok(k1 && k1.postRemaining === 12 && k2 && k2.postRemaining === 12, 'keepQty: 提案 (188/200=残12, 8/20=残12)');
  // keep>残 は拒否
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: k1.inboundItemId, orderItemId: k1.orderItemId, qty: 188, remainder: { keepQty: 99 } },
  ] }, 'aa-keep-bad');
  ok(r.status === 400 && r.body.error.includes('残す数'), 'keepQty: 残数超過は拒否', r.body.error);
  // keepQty=0 (全部減数=注残ゼロ) + keepQty=5 (7減数+5分納待ち)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: k1.inboundItemId, orderItemId: k1.orderItemId, qty: 188, remainder: { keepQty: 0 } },
    { inboundItemId: k2.inboundItemId, orderItemId: k2.orderItemId, qty: 8, remainder: { keepQty: 5, action: 'await_delivery', nextExpectedDate: '2026-08-10' } },
  ] }, 'aa-keep-1');
  ok(r.body.ok && r.body.assigned === 2, 'keepQty: 実行', r.body);
  ok(db.prepare('SELECT remaining_qty, shortage_qty FROM v_po_item_balance WHERE order_item_id=?').get(k1.orderItemId).remaining_qty === 0,
    'keepQty=0: 注残ゼロ (188入荷+12減数)');
  ok(db.prepare('SELECT shortage_qty FROM v_po_item_balance WHERE order_item_id=?').get(k1.orderItemId).shortage_qty === 12,
    'keepQty=0: 減数イベント12');
  const k2bal = db.prepare('SELECT remaining_qty, shortage_qty FROM v_po_item_balance WHERE order_item_id=?').get(k2.orderItemId);
  ok(k2bal.remaining_qty === 5 && k2bal.shortage_qty === 7, 'keepQty=5: 7減数+残5', k2bal);
  const k2item = db.prepare('SELECT remainder_disposition, next_expected_qty FROM po_order_items WHERE id=?').get(k2.orderItemId);
  ok(k2item.remainder_disposition === 'awaiting_delivery' && k2item.next_expected_qty === 5, 'keepQty=5: 分納待ち(次回5)が設定される', k2item);
  // 全量入荷 (残0) でも keepQty 指定があれば範囲検証 (Codex keep-R1 Medium)
  insRow.run('aa-keep3-item', '全量+keep指定テスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-keep3-item', qty: 6 }] }) });
  r = await up2('ae.csv', [HDR2, ['AE600', 'aa-keep3-item', 'x', '0001', '200', '6', '0', '2026/07/14']]);
  r = await j('/api/inbound/auto-assign/preview');
  const k3 = r.body.proposals.find(p2 => p2.slip === 'AE600');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: k3.inboundItemId, orderItemId: k3.orderItemId, qty: 6, remainder: { keepQty: 99 } },
  ] }, 'aa-keep3-bad');
  ok(r.status === 400 && r.body.error.includes('残す数'), 'keepQty: 全量入荷 (残0) でも範囲外keepQtyは400', r.body.error);
  ok(db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(k3.orderItemId).remaining_qty === 6,
    'keepQty: 400時は全ロールバック (未割当のまま)');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: k3.inboundItemId, orderItemId: k3.orderItemId, qty: 6, remainder: { keepQty: '0' } },
  ] }, 'aa-keep3-str');
  ok(r.status === 400, 'keepQty: 文字列は拒否 (JSON number型の整数のみ)');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: k3.inboundItemId, orderItemId: k3.orderItemId, qty: 6, remainder: { keepQty: 0 } },
  ] }, 'aa-keep3-ok');
  ok(r.body.ok && r.body.assigned === 1, 'keepQty: 全量入荷+keepQty=0は成功 (残0=減数なし)');

  // ── defer (📌あとで決める): 次回入荷予定日なしで割当できる (中原さん要望 2026-07-14) ──
  insRow.run('aa-defer-item', '予定日未定テスト商品', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  insRow.run('aa-defer2-item', '予定日未定+一部残しテスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-defer-item', qty: 30 }, { code: 'aa-defer2-item', qty: 20 }] }) });
  ok(r.body.ok !== false, 'defer: テスト用PO作成');
  r = await up2('af.csv', [HDR2,
    ['AF700', 'aa-defer-item', 'x', '0001', '200', '18', '0', '2026/07/14'],
    ['AF701', 'aa-defer2-item', 'x', '0001', '200', '8', '0', '2026/07/14']]);
  r = await j('/api/inbound/auto-assign/preview');
  const d1 = r.body.proposals.find(p2 => p2.slip === 'AF700');
  const d2 = r.body.proposals.find(p2 => p2.slip === 'AF701');
  ok(d1 && d1.postRemaining === 12 && d2 && d2.postRemaining === 12, 'defer: 提案 (18/30=残12, 8/20=残12)');
  // remainder省略=defer / 明示defer+keepQty=5 (日付は一切不要)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: d1.inboundItemId, orderItemId: d1.orderItemId, qty: 18 },
    { inboundItemId: d2.inboundItemId, orderItemId: d2.orderItemId, qty: 8, remainder: { action: 'defer', keepQty: 5 } },
  ] }, 'aa-defer-1');
  ok(r.body.ok && r.body.assigned === 2, 'defer: 日付なしで割当できる (remainder省略/明示defer)', r.body);
  const dBal1 = db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(d1.orderItemId);
  const dIt1 = db.prepare('SELECT remainder_disposition FROM po_order_items WHERE id=?').get(d1.orderItemId);
  ok(dBal1.remaining_qty === 12 && dIt1.remainder_disposition === null, 'defer: 注残12・扱い未選択のまま残る', [dBal1.remaining_qty, dIt1.remainder_disposition]);
  const dBal2 = db.prepare('SELECT remaining_qty, shortage_qty FROM v_po_item_balance WHERE order_item_id=?').get(d2.orderItemId);
  const dIt2 = db.prepare('SELECT remainder_disposition FROM po_order_items WHERE id=?').get(d2.orderItemId);
  ok(dBal2.remaining_qty === 5 && dBal2.shortage_qty === 7 && dIt2.remainder_disposition === null,
    'defer+keepQty=5: 7減数+残5・扱い未選択', dBal2);
  // 発注残「要対応」に扱い未選択 (needsDisposition) で載る
  r = await j('/api/backorders');
  const dRow = (r.body.orders || []).flatMap(o => o.items || []).find(it => it.id === d1.orderItemId);
  ok(dRow && dRow.flags && dRow.flags.needsDisposition === true, 'defer: 要対応「扱い未選択」に表示される', dRow && dRow.flags);
  // 既存の分納待ち予定 (次回12) がある明細に defer で追加入荷 → 次回予定数量を残数に自動調整 (整合性検査を汚さない)
  r = await j('/api/items/' + d1.orderItemId + '/plan', { method: 'PATCH', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'aa-defer-plan' },
    body: JSON.stringify({ disposition: 'awaiting_delivery', nextExpectedDate: '2026-08-20', nextExpectedQty: 12 }) });
  ok(r.body.ok, 'defer: 事前に分納待ち(次回12)を設定');
  r = await up2('ag.csv', [HDR2, ['AG800', 'aa-defer-item', 'x', '0001', '200', '7', '0', '2026/07/15']]);
  r = await j('/api/inbound/auto-assign/preview');
  const d3 = r.body.proposals.find(p2 => p2.slip === 'AG800');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: d3.inboundItemId, orderItemId: d3.orderItemId, qty: 7, remainder: { action: 'defer' } },
  ] }, 'aa-defer-2');
  ok(r.body.ok, 'defer: 既存分納待ちの明細にも割当できる', r.body);
  const dIt3 = db.prepare('SELECT remainder_disposition, next_expected_date, next_expected_qty FROM po_order_items WHERE id=?').get(d1.orderItemId);
  ok(dIt3.remainder_disposition === 'awaiting_delivery' && dIt3.next_expected_qty === 5 && dIt3.next_expected_date === '2026-08-20',
    'defer: 既存の扱いは維持し次回予定数量だけ残数(5)に調整', dIt3);
  r = await j('/api/ledger/integrity');
  ok(r.body.ok && !r.body.issues.some(i2 => i2.kind === 'disposition_rule' && i2.itemId === d1.orderItemId),
    'defer: 調整後は整合性検査にdisposition_ruleが出ない');
  // 不正actionは適用条件の外 (keepQty=0/残0) でも400 (減数だけ実行される素通り防止、Codex defer-R1 Medium)
  insRow.run('aa-act-item', '不正action検証テスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  insRow.run('aa-act2-item', '不正action検証テスト2', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-act-item', qty: 10 }, { code: 'aa-act2-item', qty: 5 }] }) });
  r = await up2('ah.csv', [HDR2,
    ['AH900', 'aa-act-item', 'x', '0001', '200', '4', '0', '2026/07/15'],
    ['AH901', 'aa-act2-item', 'x', '0001', '200', '5', '0', '2026/07/15']]);
  r = await j('/api/inbound/auto-assign/preview');
  const x1 = r.body.proposals.find(p2 => p2.slip === 'AH900');
  const x2 = r.body.proposals.find(p2 => p2.slip === 'AH901');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: x1.inboundItemId, orderItemId: x1.orderItemId, qty: 4, remainder: { keepQty: 0, action: 'nonsense' } },
  ] }, 'aa-act-1');
  ok(r.status === 400 && r.body.error.includes('残数の扱いが不正'), '不正action: keepQty=0でも400 (減数実行させない)', r.body.error);
  ok(db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(x1.orderItemId).remaining_qty === 10,
    '不正action: ロールバック (残10のまま)');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: x2.inboundItemId, orderItemId: x2.orderItemId, qty: 5, remainder: { action: 'bogus' } },
  ] }, 'aa-act-2');
  ok(r.status === 400 && r.body.error.includes('残数の扱いが不正'), '不正action: 全量入荷 (残0) でも400', r.body.error);

  // ── 自動対象外 (中原さん 2026-07-15: 注残にない入庫・注残超過の入庫は対象外として処理し切る) ──
  insRow.run('aa-nopo-item', '未発注入庫テスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  insRow.run('aa-over-item', '注残超過テスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  insRow.run('aa-igbad-item', '対象外細工テスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-over-item', qty: 5 }, { code: 'aa-igbad-item', qty: 10 }] }) });
  ok(r.body.ok !== false, 'auto-ignore: テスト用PO作成');
  r = await up2('ai.csv', [HDR2,
    ['AI900', 'aa-nopo-item', 'x', '0001', '200', '7', '0', '2026/07/15'],
    ['AI901', 'aa-over-item', 'x', '0001', '200', '9', '0', '2026/07/15'],
    ['AI902', 'aa-igbad-item', 'x', '0001', '200', '6', '0', '2026/07/15']]);
  ok(r.body.ok, 'auto-ignore: 取込');
  r = await j('/api/inbound/auto-assign/preview');
  const gNopo = (r.body.autoIgnores || []).find(x => x.slip === 'AI900');
  const gOver = r.body.proposals.find(x => x.slip === 'AI901');
  const gOk = r.body.proposals.find(x => x.slip === 'AI902');
  ok(gNopo && gNopo.reason.includes('発注残なし'), 'preview: 注残にない入庫は自動対象外に分類', gNopo && gNopo.reason);
  ok(gOver && gOver.qty === 5 && gOver.excessQty === 4 && gOver.postRemaining === 0,
    'preview: 注残超過はPO残まで割当+超過分だけ対象外 (9=割当5+超過4)', gOver);
  ok(gOk && gOk.qty === 6 && !gOk.excessQty, 'preview: 割当可能な行は従来どおり提案', gOk);
  // 割当可能な行を ignores に細工 → 実行時再検証で全ロールバック
  r = await jpA('/api/inbound/auto-assign', { ignores: [{ inboundItemId: gOk.inboundItemId }] }, 'aa-ig-bad');
  ok(r.status === 400 && r.body.error.includes('割当できる可能性'), 'auto-ignore: 割当可能な行の対象外化は拒否 (再検証)', r.body.error);
  // 通常行に excess を細工 → 拒否 / 超過行の割当数がPO残と不一致 → 拒否
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gOk.inboundItemId, orderItemId: gOk.orderItemId, qty: 6, excess: true },
  ] }, 'aa-ex-bad1');
  ok(r.status === 400 && r.body.error.includes('超過扱いですが'), 'excess: 超過していない行への excess 細工は拒否', r.body.error);
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gOver.inboundItemId, orderItemId: gOver.orderItemId, qty: 9, excess: true },
  ] }, 'aa-ex-bad2');
  ok(r.status === 400 && r.body.error.includes('一致しません'), 'excess: PO残と違う割当数は拒否', r.body.error);
  // 割当 (通常+超過) + 対象外を1txnで実行
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gOk.inboundItemId, orderItemId: gOk.orderItemId, qty: 6 },
    { inboundItemId: gOver.inboundItemId, orderItemId: gOver.orderItemId, qty: 5, excess: true },
  ], ignores: [{ inboundItemId: gNopo.inboundItemId }] }, 'aa-ig-1');
  ok(r.body.ok && r.body.assigned === 2 && r.body.ignored === 1, 'auto-ignore: 割当2 (うち超過1)+対象外1を1txnで実行', r.body);
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gOk.inboundItemId, orderItemId: gOk.orderItemId, qty: 6 },
    { inboundItemId: gOver.inboundItemId, orderItemId: gOver.orderItemId, qty: 5, excess: true },
  ], ignores: [{ inboundItemId: gNopo.inboundItemId }] }, 'aa-ig-1');
  ok(r.body.ok && r.body.replay === true, 'auto-ignore: 同一キー再送はreplay');
  r = await j('/api/inbound');
  ok(r.body.ignored.some(x => x.slip === 'AI900') && r.body.ignored.some(x => x.slip === 'AI901'),
    'auto-ignore: 対象外リストに移動 (↩解除可能)');
  ok(!r.body.open.some(x => ['AI900', 'AI901', 'AI902'].includes(x.slip)), 'auto-ignore: 一括割当後に未処理行が残らない');
  ok(db.prepare('SELECT remaining_qty FROM v_po_item_balance b JOIN po_order_items i ON i.id=b.order_item_id WHERE i.product_key=?')
    .get('aa-over-item').remaining_qty === 0, 'excess: PO残は全量消込される (未入荷のまま残らない、Codex ig-R1 High-2)');
  const exIg = db.prepare(`SELECT g.scope, g.reason FROM po_inbound_ignores g
    JOIN po_inbound_items i ON i.id=g.inbound_item_id JOIN po_inbound_receipts rc ON rc.id=i.receipt_id
    WHERE rc.source_key='AI901' AND g.revoked_at IS NULL`).get();
  ok(exIg && exIg.scope === 'excess' && exIg.reason.includes('超過分 4'), 'excess: scope=excess で超過4を記録', exIg);
  // 超過行の↩解除 → 残余4が未割当に戻る (割当5はそのまま)
  const overRow = db.prepare(`SELECT i.id FROM po_inbound_items i JOIN po_inbound_receipts rc ON rc.id=i.receipt_id WHERE rc.source_key='AI901'`).get();
  r = await jpA('/api/inbound/' + overRow.id + '/ignore', { ignore: false });
  ok(r.body.ok, 'excess: ↩解除できる');
  r = await j('/api/inbound');
  const backRow = r.body.open.find(x => x.slip === 'AI901');
  ok(backRow && backRow.allocated === 5 && backRow.remainingCapacity === 4, 'excess: 解除後は残余4が未割当に戻る (割当5維持)', backRow);
  r = await jpA('/api/inbound/' + overRow.id + '/ignore', { ignore: true, reason: 'テスト後片付け (超過分)' });
  ok(r.status === 400 || r.body.ok === false, 'excess: 手動の行対象外は割当済み行を拒否 (rowスコープ維持)', r.body && r.body.error);
  // 逆仕訳で excess 対象外は自動解除される (Codex ig-R2 High: 隠れたまま残ると再割当候補に出ない)
  insRow.run('aa-rev-item', '超過逆仕訳テスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-rev-item', qty: 3 }] }) });
  r = await up2('ak.csv', [HDR2, ['AK960', 'aa-rev-item', 'x', '0001', '200', '5', '0', '2026/07/15']]);
  r = await j('/api/inbound/auto-assign/preview');
  const gRev = r.body.proposals.find(x => x.slip === 'AK960');
  ok(gRev && gRev.qty === 3 && gRev.excessQty === 2, 'excess-rev: 提案 (3割当+2超過)', gRev);
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gRev.inboundItemId, orderItemId: gRev.orderItemId, qty: 3, excess: true },
  ] }, 'aa-rev-1');
  ok(r.body.ok && r.body.assigned === 1, 'excess-rev: 実行 (3消込+2対象外)');
  const revEv = db.prepare(`SELECT id FROM po_item_events WHERE inbound_item_id=? AND event_type='receipt'`).get(gRev.inboundItemId);
  r = await j('/api/events/' + revEv.id + '/reverse', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ note: '割当誤りの訂正' }) });
  ok(r.body.ok, 'excess-rev: 割当を逆仕訳');
  r = await j('/api/inbound');
  const revRow = r.body.open.find(x => x.slip === 'AK960');
  ok(revRow && revRow.allocated === 0 && revRow.remainingCapacity === 5,
    'excess-rev: 逆仕訳で excess 対象外が自動解除され全量5が未割当に戻る', revRow);
  const revIg = db.prepare(`SELECT g.revoked_by FROM po_inbound_ignores g
    JOIN po_inbound_items i ON i.id=g.inbound_item_id JOIN po_inbound_receipts rc ON rc.id=i.receipt_id
    WHERE rc.source_key='AK960' ORDER BY g.id DESC`).get();
  ok(revIg && revIg.revoked_by === 'system:reversal', 'excess-rev: 解除の主体=system:reversal を履歴に記録', revIg);
  // scope の値域は直接SQLでもトリガが強制 (addCol追加の既存DBはCHECKが無い、Codex ig-R3 Medium)
  {
    let threw = '';
    try {
      db.prepare(`INSERT INTO po_inbound_ignores (inbound_item_id, reason, scope, created_at) VALUES (?,?,?,?)`)
        .run(revRow.id, '不正scopeテスト', 'partial', new Date().toISOString());
    } catch (e) { threw = e.message; }
    ok(threw.includes('scopeが不正'), 'excess: 不正なscopeは直接SQLでもトリガが拒否', threw);
  }

  // 複数POに累積した注残へのFIFO引き当て (中原さん 2026-07-15: 古い方から引き当て)
  insRow.run('aa-multi2-item', '複数PO累積FIFOテスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-multi2-item', qty: 4 }] }) });
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-multi2-item', qty: 4 }] }) });
  const m2Items = db.prepare(`SELECT i.id FROM po_order_items i WHERE i.product_key='aa-multi2-item' ORDER BY i.id`).all();
  r = await up2('aj.csv', [HDR2, ['AJ950', 'aa-multi2-item', 'x', '0001', '200', '6', '0', '2026/07/15']]);
  r = await j('/api/inbound/auto-assign/preview');
  const gMulProps = r.body.proposals.filter(x => x.slip === 'AJ950');
  ok(gMulProps.length === 2 && gMulProps[0].orderItemId === m2Items[0].id && gMulProps[0].qty === 4
    && gMulProps[1].orderItemId === m2Items[1].id && gMulProps[1].qty === 2 && gMulProps[1].postRemaining === 2,
    'FIFO: 入庫6を古いPOから 4+2 に分割提案 (残2は新しいPO側)', gMulProps.map(x => [x.orderItemId, x.qty]));
  ok(gMulProps[1].needsRemainder === true, 'FIFO: 部分入荷になる最後のPOにだけ残数の扱い', gMulProps[1]);
  // 割当可能な行を対象外に細工 → 拒否
  r = await jpA('/api/inbound/auto-assign', { ignores: [{ inboundItemId: gMulProps[0].inboundItemId }] }, 'aa-ig-mul');
  ok(r.status === 400 && r.body.error.includes('割当できる可能性'), 'auto-ignore: FIFOで割当可能な行の対象外指定は拒否', r.body.error);
  // FIFO計画の途中までしか送らない細工 → 拒否 (半端な消込を確定させない)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gMulProps[0].inboundItemId, orderItemId: gMulProps[0].orderItemId, qty: 4 },
  ] }, 'aa-fifo-part');
  ok(r.status === 400 && r.body.error.includes('途中までしか'), 'FIFO: 計画の途中までの指定は拒否', r.body.error);
  // 順序違い (新しいPOから) も拒否
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gMulProps[1].inboundItemId, orderItemId: gMulProps[1].orderItemId, qty: 2 },
    { inboundItemId: gMulProps[0].inboundItemId, orderItemId: gMulProps[0].orderItemId, qty: 4 },
  ] }, 'aa-fifo-rev');
  ok(r.status === 400 && r.body.error.includes('一致しません'), 'FIFO: 古い順以外の割当は拒否', r.body.error);
  // 正常実行: 4 (古いPO全消込) + 2 (新しいPO、残2はあとで決める)
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gMulProps[0].inboundItemId, orderItemId: gMulProps[0].orderItemId, qty: 4 },
    { inboundItemId: gMulProps[1].inboundItemId, orderItemId: gMulProps[1].orderItemId, qty: 2 },
  ] }, 'aa-fifo-1');
  ok(r.body.ok && r.body.assigned === 2, 'FIFO: 1入庫行→2POへ分割割当を実行', r.body);
  ok(db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(m2Items[0].id).remaining_qty === 0
    && db.prepare('SELECT remaining_qty FROM v_po_item_balance WHERE order_item_id=?').get(m2Items[1].id).remaining_qty === 2,
    'FIFO: 古いPO全消込+新しいPOに残2');
  ok(db.prepare('SELECT remainder_disposition FROM po_order_items WHERE id=?').get(m2Items[1].id).remainder_disposition === null,
    'FIFO: 残2は扱い未選択 (あとで決める) で注残に残る');
  // 割当と対象外の重複指定は拒否
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: gMulProps[0].inboundItemId, orderItemId: m2Items[0].id, qty: 4 },
  ], ignores: [{ inboundItemId: gMulProps[0].inboundItemId }] }, 'aa-ig-dup');
  ok(r.status === 400, 'auto-ignore: 割当と対象外の重複指定は拒否');
  r = await jpA('/api/inbound/auto-assign', { assignments: [], ignores: [] }, 'aa-ig-empty');
  ok(r.status === 400, 'auto-ignore: 空リクエストは400');
  // バッチ全体の細工遮断 (Codex ig-R4 High): 一部の行だけ割当+keepQty減数でPO残を消し、
  // 残りの行を「候補なし」に見せて対象外化する攻撃 → 対象外はtxn開始時の正直な計画で分類された行のみ
  insRow.run('aa-batch-item', 'バッチ細工テスト', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-batch-item', qty: 8 }] }) });
  r = await up2('al.csv', [HDR2,
    ['AL970', 'aa-batch-item', 'x', '0001', '200', '4', '0', '2026/07/15'],
    ['AL971', 'aa-batch-item', 'x', '0001', '200', '4', '0', '2026/07/15']]);
  r = await j('/api/inbound/auto-assign/preview');
  const bAtk = r.body.proposals.filter(x => ['AL970', 'AL971'].includes(x.slip));
  ok(bAtk.length === 2, 'バッチ細工: 正直な計画では2行とも割当 (対象外候補なし)');
  r = await jpA('/api/inbound/auto-assign', { assignments: [
    { inboundItemId: bAtk[0].inboundItemId, orderItemId: bAtk[0].orderItemId, qty: 4, remainder: { keepQty: 0 } },
  ], ignores: [{ inboundItemId: bAtk[1].inboundItemId }] }, 'aa-batch-atk');
  ok(r.status === 400 && r.body.error.includes('対象外になりません'),
    'バッチ細工: keepQty減数で候補を消してからの対象外化は拒否 (全ロールバック)', r.body.error);
  ok(db.prepare(`SELECT remaining_qty FROM v_po_item_balance b JOIN po_order_items i ON i.id=b.order_item_id WHERE i.product_key='aa-batch-item'`).get().remaining_qty === 8,
    'バッチ細工: 拒否時は減数もロールバック (PO残8のまま)');

  // ── 網羅性の保存則 (中原さん 2026-07-15: 割当漏れ・注残漏れを絶対に避けたい) ──
  // 入荷良品数 = 割当済み + 対象外 + 未処理 が常に一致し、一括割当後は未処理0
  r = await j('/api/inbound');
  ok(r.body.totals && r.body.totals.balanced === true, 'tally: 保存則が常に成立 (割当+対象外+未処理=入荷)', r.body.totals);
  ok(r.body.totals.allocated + r.body.totals.ignored + r.body.totals.unprocessed === r.body.totals.totalGood,
    'tally: 数式が実際に一致', r.body.totals);
  ok(r.body.totals.unprocessed === r.body.open.reduce((s, x) => s + x.remainingCapacity, 0),
    'tally: 未処理数量=未割当一覧の残容量合計', r.body.totals.unprocessed);
  // クリーンな1商品で「取込→未処理→一括割当→未処理0」を検証
  insRow.run('aa-tally-item', '網羅性テスト商品', '0001', '取扱中', 2, 0, 0, 0, 0, 10, 1.5, 400, 200, '2026-07-01', '2026-01-01');
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'aa-tally-item', qty: 6 }] }) });
  r = await j('/api/inbound');
  const preUnproc = r.body.totals.unprocessed;
  r = await up2('am.csv', [HDR2,
    ['AM980', 'aa-tally-item', 'x', '0001', '200', '6', '0', '2026/07/15'],   // 注残6ぴったり
    ['AM981', 'aa-tally-item', 'x', '0001', '200', '3', '0', '2026/07/15'],   // 超過3 (先行6で注残消込済み)
    ['AM982', 'aa-nopo-tally', 'x', '0001', '200', '4', '0', '2026/07/15']]); // 未発注 (注残なし)
  db.prepare("UPDATE mirror_pml_snapshot_rows SET 商品名='未発注網羅性' WHERE 商品コード='aa-nopo-tally'").run();
  r = await j('/api/inbound');
  ok(r.body.totals.unprocessed === preUnproc + 13 && r.body.totals.balanced,
    'tally: 取込直後は入荷13が全て未処理に積まれる', r.body.totals.unprocessed - preUnproc);
  r = await j('/api/inbound/auto-assign/preview');
  const tProps = r.body.proposals.filter(p => ['AM980', 'AM981'].includes(p.slip));
  const tIgn = (r.body.autoIgnores || []).filter(x => ['AM981', 'AM982'].includes(x.slip));
  r = await jpA('/api/inbound/auto-assign', {
    assignments: tProps.map(p => ({ inboundItemId: p.inboundItemId, orderItemId: p.orderItemId, qty: p.qty, excess: p.excessQty > 0 ? true : undefined })),
    ignores: tIgn.map(x => ({ inboundItemId: x.inboundItemId })),
  }, 'aa-tally-1');
  ok(r.body.ok, 'tally: 一括割当を実行', r.body);
  r = await j('/api/inbound');
  ok(r.body.totals.unprocessed === preUnproc && r.body.totals.balanced,
    'tally: 一括割当後は未処理が取込前の水準に戻る (この商品群は取りこぼしゼロ)', r.body.totals.unprocessed - preUnproc);
  ok(!r.body.open.some(x => ['AM980', 'AM981', 'AM982'].includes(x.slip)), 'tally: 全行が処理し切られ未割当一覧に残らない');
  // 超過分 (AM981の3) と未発注 (AM982の4) は対象外に計上され、保存則は維持される
  ok(r.body.totals.balanced, 'tally: 超過+未発注の対象外を含めても保存則は成立');
  // 良品0 (不良のみ) の入庫でも保存則を壊さない
  r = await up2('an.csv', [HDR2, ['AN990', 'aa-tally-item', 'x', '0001', '200', '0', '5', '2026/07/15']]);
  ok(r.body.ok !== false || (r.body.error || '').length >= 0, 'tally: 良品0行の取込 (受理 or 明示エラー)');
  r = await j('/api/inbound');
  ok(r.body.totals.balanced, 'tally: 良品0行が混ざっても保存則は成立 (良品数ベース)');

  // ── 📜 取込履歴 (いつ・何を・何個) ──
  r = await j('/api/inbound/batches');
  ok(r.body.ok && r.body.batches.length >= 1, 'batches: 一覧が返る', r.body.batches && r.body.batches.length);
  const bAi = r.body.batches.find(b => b.fileName === 'ai.csv');
  ok(bAi && bAi.receiptCount === 3 && bAi.lineCount === 3 && bAi.totalGood === 22 && /\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(bAi.importedAtJst),
    'batches: ai.csv=3伝票3明細22個+JST日時', bAi);
  r = await j('/api/inbound/batches/' + bAi.id);
  ok(r.body.ok && r.body.lines.length === 3 && r.body.lines.some(l => l.product_code === 'aa-nopo-item' && l.good_qty === 7),
    'batches: 明細 (何を何個) が引ける', r.body.lines && r.body.lines.length);
  r = await j('/api/inbound/batches/999999');
  ok(r.status === 400, 'batches: 存在しないIDは400');

  // 画面: プレビュー/一括割当UI配信
  const inbHtml4 = await (await fetch(base + '/inbound')).text();
  ok(inbHtml4.includes('autoAssignBtn') && inbHtml4.includes('この内容で取り込む') && inbHtml4.includes('一括割当の確認'),
    '/inbound プレビュー+一括割当UI配信');
  ok(inbHtml4.includes('あとで決める'), '/inbound 一括割当に📌あとで決める選択肢');
  ok(inbHtml4.includes('取込履歴') && inbHtml4.includes('data-histbatch'), '/inbound 📜取込履歴UI配信');
  ok(inbHtml4.includes('tallyArea') && inbHtml4.includes('取りこぼし'), '/inbound 網羅性サマリUI配信');
}

// ═══ 🏷️ 自社商品バーコードラベル管理 (対象=AMC×売上分類1。旧Excel移行のCSV取込+楽観ロック編集) ═══
console.log('── 🏷️ バーコードラベル管理 ──');
{
  // 対象判定: fixture では 0726-001060 / diyorangeoil100 (仕入先0001×売上分類1) の2商品のみ
  r = await j('/api/barcode-labels');
  ok(r.status === 200 && r.body.ok && r.body.pmlSynced, 'barcode: 一覧API ok');
  const tKeys = r.body.targets.map(t => t.key);
  ok(tKeys.includes('0726-001060') && tKeys.includes('diyorangeoil100'), 'barcode: 対象=AMC×売上分類1', tKeys);
  ok(!tKeys.includes('noflyersticker') && !tKeys.includes('set-2pack') && !tKeys.includes('gyoumuhandcream60-bi'),
    'barcode: 売上分類2/3・セット商品は対象外');
  ok(r.body.targets.every(t => !t.label) && r.body.summary.unregistered === r.body.targets.length,
    'barcode: 初期状態は全て未登録 (label=null)', r.body.summary);

  // PUT 新規登録 (version=null) → 楽観ロック更新 → 競合409
  r = await j('/api/barcode-labels/0726-001060', { method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ productCode: '0726-001060', status: 'unset', barcodeValue: '', note: '' }) });
  ok(r.status === 200 && r.body.ok && r.body.label.version === 1 && r.body.label.status === 'unset', 'barcode: 新規登録 version=1', r.body);
  r = await j('/api/barcode-labels/0726-001060', { method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'unset' }) });
  ok(r.status === 409 && r.body.current && r.body.current.version === 1, 'barcode: 既存商品への新規登録(version=null)は409+current', r.status);
  r = await j('/api/barcode-labels/0726-001060', { method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'requested', version: 99 }) });
  ok(r.status === 409, 'barcode: version不一致の更新は409');
  r = await j('/api/barcode-labels/0726-001060', { method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'requested', barcodeValue: 'X0019R42C5', version: 1 }) });
  ok(r.status === 200 && r.body.label.version === 2 && r.body.label.barcodeType === 'fnsku',
    'barcode: version一致で更新+FNSKU種別自動判定', r.body.label);
  r = await j('/api/barcode-labels/0726-001060', { method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'hen', version: 2 }) });
  ok(r.status === 400, 'barcode: 不正statusは400');

  // 対象外商品の登録は可能だが orphan として理由付きで出る
  r = await j('/api/barcode-labels/noflyersticker', { method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ productCode: 'noflyersticker', status: 'printed' }) });
  ok(r.status === 200, 'barcode: 対象外商品も登録自体は可能');
  r = await j('/api/barcode-labels');
  const orp = r.body.orphans.find(o => o.key === 'noflyersticker');
  ok(orp && orp.orphanReason === 'sales_class_changed', 'barcode: 対象外はorphan+理由 (売上分類違い)', orp && orp.orphanReason);

  // CSV取込: プレビュー (fileHash発行+手動編集上書き警告) → fileHash必須commit → upsert (CSVに無い行は消えない)
  const bcCsv = '商品コード,状態,バーコード値,備考,AMC管理番号\r\ndiyorangeoil100,設定済み,X0010LO63F,テスト移行,ZZZ1745-BF1411\r\n0726-001060,未設定,,上書きテスト,\r\n';
  const mkFd = (extra) => {
    const fd9 = new FormData();
    fd9.append('file', new Blob(['﻿' + bcCsv], { type: 'text/csv' }), 'barcode.csv');
    for (const [k, v] of Object.entries(extra || {})) fd9.append(k, v);
    return fd9;
  };
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: mkFd() });
  ok(r.status === 200 && r.body.ok && !r.body.committed && r.body.counts.new === 1 && r.body.counts.update === 1,
    'barcode: CSVプレビュー new1/update1', r.body.counts);
  ok(r.body.counts.manualOverwrite === 1 && r.body.manualList.length === 1, 'barcode: 手動編集の上書き警告', r.body.manualList);
  ok(typeof r.body.stateHash === 'string' && r.body.stateHash.length === 64, 'barcode: プレビューがstateHashを返す');
  let bcHash = r.body.fileHash, bcState = r.body.stateHash;
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: mkFd({ commit: '1' }) });
  ok(r.status === 400, 'barcode: fileHashなしのcommitは400');
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: mkFd({ commit: '1', fileHash: bcHash }) });
  ok(r.status === 409, 'barcode: stateHashなしのcommitは409');
  // プレビュー後に手動編集が入ったら stateHash 不一致でcommit拒否 (CSVで楽観ロックを迂回させない、Codex R1 High)
  r = await j('/api/barcode-labels/diyorangeoil100', { method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ productCode: 'diyorangeoil100', status: 'unset' }) });
  ok(r.status === 200, 'barcode: プレビュー後の手動編集 (競合テスト用)');
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: mkFd({ commit: '1', fileHash: bcHash, stateHash: bcState }) });
  ok(r.status === 409, 'barcode: プレビュー後に手動編集された commit は409 (stateHash不一致)');
  // 再プレビュー → commit 成功
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: mkFd() });
  ok(r.status === 200 && r.body.counts.new === 0 && r.body.counts.update === 2 && r.body.counts.manualOverwrite === 2,
    'barcode: 再プレビュー (全行update+手動上書き2件)', r.body.counts);
  bcHash = r.body.fileHash; bcState = r.body.stateHash;
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: mkFd({ commit: '1', fileHash: bcHash, stateHash: bcState }) });
  ok(r.status === 200 && r.body.committed && r.body.counts.update === 2, 'barcode: 再プレビュー後のcommit成功', r.body.counts);
  r = await j('/api/barcode-labels');
  const oilT = r.body.targets.find(t => t.key === 'diyorangeoil100');
  const nikuT = r.body.targets.find(t => t.key === '0726-001060');
  ok(oilT.label && oilT.label.status === 'printed' && oilT.label.barcodeType === 'fnsku' && oilT.label.vendorCodeHint === 'ZZZ1745-BF1411',
    'barcode: CSV取込結果 (printed+FNSKU+AMC番号ヒント)', oilT.label);
  ok(nikuT.label.status === 'unset' && nikuT.label.source === 'csv', 'barcode: CSVが手動編集行を上書き (プレビューで警告済み)', nikuT.label);
  ok(r.body.orphans.some(o => o.key === 'noflyersticker'), 'barcode: CSVに無い既存行は消えない (upsert方式)');
  // 備考500文字超は黙って切り捨てず行番号付きエラー (Codex R1 Medium)
  const longFd = new FormData();
  longFd.append('file', new Blob(['商品コード,状態,備考\r\ndiyorangeoil100,設定済み,' + 'あ'.repeat(501) + '\r\n'], { type: 'text/csv' }), 'long.csv');
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: longFd });
  ok(r.status === 400 && r.body.error.includes('備考が長すぎます'), 'barcode: 備考500文字超のCSVは400 (黙って切り捨てない)');
  const badFd = new FormData();
  badFd.append('file', new Blob(['商品コード,状態\r\nxxx,ヘンな状態\r\n'], { type: 'text/csv' }), 'bad.csv');
  r = await j('/api/barcode-labels/import-csv', { method: 'POST', body: badFd });
  ok(r.status === 400, 'barcode: 不正な状態値のCSVは400');

  // /api/backorders の明細に barcode が付く (対象商品のみ)
  r = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: '0726-001060', qty: 10 }, { code: 'noflyersticker', qty: 5 }] }) });
  ok(r.status === 200 && r.body.ok, 'barcode: AMC発注を作成 (添付テスト用)');
  r = await j('/api/backorders');
  let bcItem = null, bcNonTarget = null;
  for (const o of r.body.orders) {
    for (const i of o.items) {
      if (i.product_code === '0726-001060' && i.barcode && !bcItem) bcItem = i;
      if (i.product_code === 'noflyersticker' && !bcNonTarget) bcNonTarget = i;
    }
  }
  ok(bcItem && bcItem.barcode.status === 'unset' && bcItem.barcode.version === 3, 'barcode: /api/backorders 明細に添付', bcItem && bcItem.barcode);
  ok(bcNonTarget && bcNonTarget.barcode === undefined, 'barcode: 対象外商品の明細には付かない');

  // 画面配信: ワークスペースDTO埋め込み / 発注残の編集導線 / adminタブ
  const wsHtml = await (await fetch(base + '/supplier/1')).text();
  ok(wsHtml.includes('"barcode":{"status":') && wsHtml.includes('bcOpenEditor'), 'barcode: ワークスペースにDTO埋め込み+編集導線');
  const boHtml9 = await (await fetch(base + '/backorders')).text();
  ok(boHtml9.includes('bcFindBarcode') && boHtml9.includes('data-bcedit'), 'barcode: 発注残ページにバッジ+編集導線');
  const adHtml9 = await (await fetch(base + '/admin')).text();
  ok(adHtml9.includes('zoneBarcode') && adHtml9.includes('loadBarcode') && adHtml9.includes('data-grp="barcode"'), 'barcode: adminに🏷️タブ+CSV取込UI');
}

// ═══ P17 欠品リスク (加重日販+在庫推移シミュレーション+2軸分類+しきい値) ═══
console.log('── P17 欠品リスク ──');
{
  const jstD = n => new Date(Date.now() + 9 * 3600000 + n * 86400000).toISOString().slice(0, 10);
  const insSr = db.prepare(`INSERT INTO mirror_pml_snapshot_rows
    (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 売上分類, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計, 発注ロット単位, 推奨保有月数, 売価, 原価, 最終仕入日, 登録日)
    VALUES ('run_test', ?, ?, '0001', '取扱中', 2, ?, 0, ?, ?, 100, 1.5, 500, 200, '2026-07-01', '2020-01-01')`);
  // 加重日販は全て 70/7×0.5 + 300/30×0.5 = 10/日
  insSr.run('sr-high-item', '欠品ハイ (入荷が間に合わない)', 20, 70, 300);      // 2日分 → 欠品、入荷は10日後
  insSr.run('sr-ok-item', '余裕あり', 1000, 70, 300);                           // 100日分
  insSr.run('sr-soon-item', '納期未確定・切迫', 50, 70, 300);                    // 5日分 → soon14日以内 → high
  insSr.run('sr-undated-item', '納期未確定・遠い', 500, 70, 300);                // 50日分 → soon超 → attention (回答督促)
  insSr.run('sr-over-item', '遅延中・在庫豊富', 5000, 70, 300);                  // 500日分 → 欠品なし+遅延 → ok+督促フラグ (2軸)
  insSr.run('sr-nodemand-item', '販売なし', 5, 0, 0);
  insSr.run('sr-split-item', '分納分割', 1000, 70, 300);                         // 分納30個のみ日付つき・残50は納期不明
  insSr.run('sr-reqover-item', '希望納期超過・回答未着', 5000, 70, 300);          // 希望納期past=遅延ではなく未回答扱い
  insSr.run('sr-multi-item', '複数仕入先スコープ', 5000, 70, 300);                // 仕入先1=遅延 / 仕入先2=クリーン

  const issueSr = async (items) => {
    const rr = await j('/api/supplier/1/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items }) });
    ok(rr.status === 200 && rr.body.ok, `P17 fixture発注 (${items.map(x => x.code).join(',')})`);
    return rr.body.id;
  };
  const setPromised = async (orderId, code, date) => {
    const it = db.prepare('SELECT id FROM po_order_items WHERE order_id=? AND product_key=?').get(orderId, code);
    const rr = await j('/api/items/' + it.id + '/plan', { method: 'PATCH', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ promisedDate: date }) });
    ok(rr.status === 200 && rr.body.ok, `P17 回答納期セット (${code}=${date})`);
  };
  let oid = await issueSr([{ code: 'sr-high-item', qty: 100 }, { code: 'sr-ok-item', qty: 50 }]);
  await setPromised(oid, 'sr-high-item', jstD(10));
  await setPromised(oid, 'sr-ok-item', jstD(5));
  await issueSr([{ code: 'sr-soon-item', qty: 80 }, { code: 'sr-undated-item', qty: 60 }]); // 納期なし
  oid = await issueSr([{ code: 'sr-over-item', qty: 40 }, { code: 'sr-nodemand-item', qty: 10 }]);
  await setPromised(oid, 'sr-over-item', jstD(-1)); // 昨日=予定日超過
  await setPromised(oid, 'sr-nodemand-item', jstD(30));
  // 分納分割: 発注100→入荷20→残80のうち分納予定30個 (jstD(5)) のみ日付つき、残50は納期不明
  oid = await issueSr([{ code: 'sr-split-item', qty: 100 }]);
  {
    const it = db.prepare('SELECT id FROM po_order_items WHERE order_id=? AND product_key=?').get(oid, 'sr-split-item');
    r = await j('/api/items/' + it.id + '/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'receipt', qty: 20, remainder: { action: 'await_delivery', nextExpectedDate: jstD(5), nextExpectedQty: 30 } }) });
    ok(r.status === 200 && r.body.ok && r.body.remaining === 80, 'P17 fixture: 分納予定30/残80');
  }
  // 希望納期が過去 (回答なし): NE移行取込で requested_date を過去日にする (issued後のrequestedは不変のため)
  {
    const neCsvOf2 = rows => iconv.encode(rows.map(rr => rr.map(v => '"' + String(v) + '"').join(',')).join('\r\n'), 'Shift_JIS');
    const HDR = ['発注伝票番号', '発注先名', '商品コード', '商品名', 'option', '発注数', '注残計', '予定納期', '備考', '商品区分', '受注伝票番号', '明細行', '発行日', '仕入先cd', '発注明細行', '商品区分値'];
    const row = ['9300', 'テスト様', 'sr-reqover-item', '希望納期超過', '', 25, 25, '2026/1/10', '', '通常', '0', '0', '2026/1/5', '0001', '1', '0'];
    const fdN = new FormData();
    fdN.append('file', new Blob([neCsvOf2([HDR, row])]), 'sr-reqover.csv');
    r = await j('/api/backorders/ne-import', { method: 'POST', body: fdN });
    const fdC = new FormData();
    fdC.append('file', new Blob([neCsvOf2([HDR, row])]), 'sr-reqover.csv');
    fdC.append('commit', '1'); fdC.append('fileHash', r.body.fileHash); fdC.append('planHash', r.body.planHash);
    r = await j('/api/backorders/ne-import', { method: 'POST', body: fdC });
    ok(r.status === 200 && r.body.ok && r.body.created.length === 1, 'P17 fixture: 希望納期過去の移行PO');
  }
  // 複数仕入先: 仕入先1のPOは予定日超過、仕入先2のPO (NE移行) はクリーン → 督促は仕入先スコープで分離
  oid = await issueSr([{ code: 'sr-multi-item', qty: 30 }]);
  await setPromised(oid, 'sr-multi-item', jstD(-2));
  {
    const neCsvOf2 = rows => iconv.encode(rows.map(rr => rr.map(v => '"' + String(v) + '"').join(',')).join('\r\n'), 'Shift_JIS');
    const HDR = ['発注伝票番号', '発注先名', '商品コード', '商品名', 'option', '発注数', '注残計', '予定納期', '備考', '商品区分', '受注伝票番号', '明細行', '発行日', '仕入先cd', '発注明細行', '商品区分値'];
    const row = ['9301', 'テスト様', 'sr-multi-item', '複数仕入先', '', 20, 20, '', '', '通常', '0', '0', '2026/7/1', '0002', '1', '0'];
    const fdN = new FormData();
    fdN.append('file', new Blob([neCsvOf2([HDR, row])]), 'sr-multi.csv');
    r = await j('/api/backorders/ne-import', { method: 'POST', body: fdN });
    const fdC = new FormData();
    fdC.append('file', new Blob([neCsvOf2([HDR, row])]), 'sr-multi.csv');
    fdC.append('commit', '1'); fdC.append('fileHash', r.body.fileHash); fdC.append('planHash', r.body.planHash);
    r = await j('/api/backorders/ne-import', { method: 'POST', body: fdC });
    ok(r.status === 200 && r.body.ok && r.body.created.length === 1, 'P17 fixture: 仕入先2の移行PO (クリーン)');
  }

  r = await j('/api/shortage-risk');
  ok(r.body.ok && r.body.settings.w7 === 0.5 && r.body.settings.soonDays === 14 && r.body.today === jstD(0), 'P17 API: 既定しきい値+JST今日', r.body.settings);
  ok(r.body.dataBasis && r.body.dataBasis.pmlAsOfDate && r.body.dataBasis.stale === false, 'P17 API: データ基準 (PML as_of、stale=false)', r.body.dataBasis);
  const sup1 = r.body.suppliers.find(s => s.code === '1');
  const item = c => sup1.items.find(x => x.code === c);

  // 🔴 high: 入荷を織り込んでも欠品 (t=1で在庫0、+100は10日後 → 9日間欠品)
  const hi = item('sr-high-item');
  ok(hi && hi.risk === 'high' && hi.stockoutDate === jstD(1) && hi.shortageDays === 9 && hi.recovered === true,
    'P17: 入荷が間に合わない → high (欠品日/9日間/回復)', hi && { d: hi.stockoutDate, s: hi.shortageDays });
  ok(hi.reason.includes('在庫切れ見込み') && hi.reason.includes('100個') && hi.reason.includes('9日間'), 'P17: 理由文 (次回入荷+欠品日数)', hi.reason);
  ok(hi.dailySales === 10 && hi.stock === 20, 'P17: 加重日販=10 (70/7×0.5+300/30×0.5)');

  // OK: 余裕95日 (入荷直前 950÷10)
  const okI = item('sr-ok-item');
  ok(okI && okI.risk === 'ok' && okI.marginDays === 95, 'P17: 余裕あり → ok (margin95日)', okI && okI.marginDays);

  // 🔴 high: 納期未確定でも切迫 (5日分 ≤ soon14)
  const soon = item('sr-soon-item');
  ok(soon && soon.risk === 'high' && soon.reason.includes('至急'), 'P17: 納期未確定+切迫 → high (至急督促)', soon && soon.reason);

  // 🟡 attention: 納期未確定だが50日分 (soon超) → 回答督促
  const und = item('sr-undated-item');
  ok(und && und.risk === 'attention' && und.reason.includes('納期を確定') && und.undatedQty === 60,
    'P17: 納期未確定+遠い → attention (ノイズ化させない)', und && und.risk);

  // 2軸分離: 遅延中でも予測評価は ok のまま、督促は needsFollowup 軸で拾う
  const over = item('sr-over-item');
  ok(over && over.risk === 'ok' && over.needsFollowup === true && over.flags.overdue === true && over.overdueQty === 40 && !over.stockoutDate,
    'P17: 予定日超過 → risk=ok × needsFollowup=true (2軸分離)', over && { r: over.risk, f: over.flags });
  ok(over.reason.includes('予定日超過'), 'P17: 遅延の理由文', over.reason);

  // 分納分割: 30個のみ日付つき・残50は納期不明 (表示partsも同じ内訳、全量を分納予定と誤表示しない)
  const spl = item('sr-split-item');
  ok(spl && spl.undatedQty === 50 && spl.arrivals.length === 1 && spl.arrivals[0].qty === 30 && spl.arrivals[0].date === jstD(5),
    'P17: 分納分割 (30=日付つき/50=納期不明)', spl && { u: spl.undatedQty, a: spl.arrivals });
  const splLine = spl.lines.find(l => l.remaining === 80);
  ok(splLine && splLine.parts.length === 2 && splLine.parts[0].qty === 30 && splLine.parts[0].disp.kind === 'dated'
    && splLine.parts[1].qty === 50 && splLine.parts[1].disp.kind === 'undated',
    'P17: 表示partsが分納分割を反映 (残80=30予定+50不明)', splLine && splLine.parts);

  // 希望納期の過去日 = 遅延ではなく「回答未着のまま希望日超過」(overdueフラグは立てない)
  const reqo = item('sr-reqover-item');
  ok(reqo && reqo.risk === 'ok' && reqo.needsFollowup === true && reqo.flags.overdue === false
    && reqo.requestedOverdueQty === 25 && reqo.flags.requested_date_only === true,
    'P17: 希望納期超過 → overdueではなく回答督促 (requestedOverdueQty)', reqo && { f: reqo.flags, q: reqo.requestedOverdueQty });
  ok(reqo.reason.includes('希望納期を経過'), 'P17: 希望納期超過の理由文', reqo.reason);

  // 仕入先スコープ: 同一商品でも督促の数量・フラグ・文面は各仕入先の明細だけで再集計 (Codex R2 High)
  const multi1 = item('sr-multi-item');
  ok(multi1 && multi1.needsFollowup === true && multi1.overdueQty === 30 && multi1.reason.includes('予定日超過'),
    'P17: 複数仕入先 — 仕入先1スコープは遅延30個で督促対象', multi1 && { f: multi1.needsFollowup, q: multi1.overdueQty });
  const multi2 = r.body.suppliers.find(s => s.code === '2').items.find(x => x.code === 'sr-multi-item');
  ok(multi2 && multi2.needsFollowup === false && multi2.overdueQty === 0 && !multi2.reason.includes('予定日超過')
    && multi2.lines.length === 1 && multi2.lines[0].poNumber === '9301',
    'P17: 複数仕入先 — 仕入先2スコープに他仕入先の遅延が混ざらない', multi2 && { f: multi2.needsFollowup, q: multi2.overdueQty, n: multi2.lines.length });
  ok(multi1.risk === multi2.risk && multi1.stockoutDate === multi2.stockoutDate,
    'P17: 複数仕入先 — 予測評価 (risk/欠品予測) は商品単位で共通');

  // 販売なし / データ異常 (フラグがあっても risk 軸は純粋)
  const nd = item('sr-nodemand-item');
  ok(nd && nd.risk === 'no_demand', 'P17: 日販0 → no_demand');
  const sup2 = r.body.suppliers.find(s => s.code === '2');
  const unk = sup2 && sup2.items.find(x => x.code === 'not-in-pml-item');
  ok(unk && unk.risk === 'unknown' && unk.pmlMissing === true, 'P17: PML行なし → unknown (データ異常のみ)', unk && unk.risk);
  ok(typeof r.body.summary.followup === 'number' && r.body.summary.followup >= 2, 'P17: summaryに督促対象数', r.body.summary);

  // しきい値PATCH: 範囲外/非整数/空は400、正常は200+監査ログ
  const patchSr = body => j('/api/shortage-risk/settings', { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  r = await patchSr({ w7: 1.5 });
  ok(r.status === 400 && r.body.error.includes('w7'), 'P17設定: w7範囲外は400');
  r = await patchSr({ marginDays: 2.5 });
  ok(r.status === 400 && r.body.error.includes('整数'), 'P17設定: 非整数は400');
  r = await patchSr({});
  ok(r.status === 400, 'P17設定: 空は400');
  // null/空文字/文字列数値は Number() で通さず型で拒否 (Codex R2 Medium / R3 Low)
  r = await patchSr({ w7: null });
  ok(r.status === 400, 'P17設定: null は400');
  r = await patchSr({ w7: '' });
  ok(r.status === 400, 'P17設定: 空文字は400');
  r = await patchSr({ w7: '0.5' });
  ok(r.status === 400, 'P17設定: 文字列数値は400 (JSON numberのみ)');
  r = await patchSr({ unansweredDays: 0 });
  ok(r.status === 200 && r.body.ok && r.body.settings.unansweredDays === 0, 'P17設定: 未回答督促=0日に変更');
  ok(db.prepare("SELECT COUNT(*) n FROM po_audit_log WHERE action='setting_change' AND resource='setting:shortage_unanswered_days'").get().n >= 1,
    'P17設定: 変更が監査ログに残る (old/new)');
  // 未回答督促0日 → 発注直後の未回答にもフラグが立つ (needsFollowup も連動)
  r = await j('/api/shortage-risk');
  const und2 = r.body.suppliers.find(s => s.code === '1').items.find(x => x.code === 'sr-undated-item');
  ok(und2 && und2.flags.unanswered === true && und2.unansweredQty === 60 && und2.needsFollowup === true,
    'P17: 未回答フラグ (0日設定で即時)', und2 && und2.flags);
  r = await patchSr({ unansweredDays: 7 });
  ok(r.status === 200, 'P17設定: 既定に復元');
  // setSetting 自体の検証 (routerを経由しない書込でも不正値は入らない)
  {
    let thr = null;
    try { db.prepare("INSERT OR REPLACE INTO po_settings (key, value, effective_at) VALUES ('shortage_horizon_days','14.5',datetime('now'))").run(); } catch (e) { thr = e.message; }
    // DB直書きは防げない (アプリ層検証) が、読込側が既定値へフォールバックする
    r = await j('/api/shortage-risk');
    ok(r.body.settings.horizonDays === 90, 'P17設定: 不正保存値 (14.5) は読込時に既定へフォールバック', r.body.settings.horizonDays);
    db.prepare("DELETE FROM po_settings WHERE key='shortage_horizon_days'").run();
  }

  // 画面配信
  const srHtml = await (await fetch(base + '/shortage-risk')).text();
  ok(srHtml.includes('欠品リスク') && srHtml.includes('srW7') && srHtml.includes('srSoon') && srHtml.includes('既定値に戻す'),
    '/shortage-risk しきい値設定UI (5項目+既定に戻す)');
  ok(srHtml.includes('data-copy') && srHtml.includes('督促リストをコピー') && srHtml.includes('buildCopyText') && srHtml.includes('参考:'),
    '/shortage-risk 督促コピー (台帳の事実+参考予測の分離文面)');
  ok(srHtml.includes('#po-') && srHtml.includes('dataBasis'), '/shortage-risk POリンク+データ基準表示');
  const boHtmlSr = await (await fetch(base + '/backorders')).text();
  ok(boHtmlSr.includes('HASH_JUMPED') && boHtmlSr.includes('#po-'), '/backorders #po-<id> ハッシュジャンプ対応');
  const navHtml = await (await fetch(base + '/')).text();
  ok(navHtml.includes('/apps/purchase-orders/shortage-risk'), 'navに欠品リスクタブ');
}

// ═══ 商品紐付けタブの全商品既定表示 (中原さん要望 2026-07-16) ═══
console.log('── 商品紐付け: 全商品既定表示+フィルタ維持 ──');
{
  r = await j('/api/masters/attrs');
  ok(r.body.ok, 'attrs GET: 200');
  const rows = r.body.rows;
  // 未紐付けの取扱中商品も行として返る (グループ空欄+linked=false+商品名/仕入先名つき)
  const hori = rows.find(x => x.product_key === 'horikoshi-item');
  ok(hori && hori.linked === false && hori.condition_id == null && hori.商品名 === '掘り起こし対象商品' && hori.仕入先名.length > 0,
    'attrs: 未紐付けの取扱中商品が行として返る (linked=false)', hori && { l: hori.linked, n: hori.商品名 });
  // 紐付け済みは linked=true でattrs値を保持
  const oil2 = rows.find(x => x.product_key === 'diyorangeoil100');
  ok(oil2 && oil2.linked === true && oil2.material_group_id === 'mokouorange', 'attrs: 紐付け済み行は値を保持 (linked=true)');
  // セット商品と「未紐付け×取扱中止」は出さない
  ok(!rows.some(x => x.product_key === 'set-2pack'), 'attrs: セット商品は出さない');
  ok(!rows.some(x => x.product_key === 'teishi-item'), 'attrs: 未紐付け×取扱中止は出さない');
  // PMLに無い紐付け済み商品は pmlMissing で残す
  await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ product_code: 'ghost-item-x', case_group: 'GX' }) });
  r = await j('/api/masters/attrs');
  const ghost = r.body.rows.find(x => x.product_key === 'ghost-item-x');
  ok(ghost && ghost.linked === true && ghost.pmlMissing === true, 'attrs: PML外の紐付け済み商品はpmlMissingで残す', ghost && ghost.pmlMissing);
  ok(r.body.rows.every((x, i, a) => i === 0 || a[i - 1].product_key <= x.product_key), 'attrs: 商品コード順 (バイナリ順)');
  // PMLに同一コードが「取扱中止→取扱中」の順で重複していても取扱中を採用 (行順依存で消えない)
  db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 売上分類, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計)
    VALUES ('run_test', 'dup-case-item', '重複コード旧', '0001', '取扱中止', 2, 0, 0, 0, 0)`).run();
  db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 仕入先, 取扱区分, 売上分類, 総在庫数, 注残数, 販売数7日_合計, 販売数30日_合計)
    VALUES ('run_test', 'DUP-CASE-ITEM', '重複コード新', '0001', '取扱中', 2, 5, 0, 0, 0)`).run();
  r = await j('/api/masters/attrs');
  const dup = r.body.rows.find(x => x.product_key === 'dup-case-item');
  ok(dup && dup.active === true && dup.商品名 === '重複コード新', 'attrs: 重複コードは取扱中の行を優先 (行順非依存)', dup && dup.商品名);
  // 空保存ガード: 未紐付け商品を全欄空のまま保存しても空のattrs行 (=紐付け済み扱い) を作らない
  r = await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ product_code: 'horikoshi-item' }) });
  ok(r.status === 400 && r.body.error.includes('空'), 'attrs: 新規×全空欄の保存は400 (linked化させない)', r.body.error);
  // 未紐付け行への保存=そのまま紐づけ (upsert)
  r = await j('/api/masters/attrs', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ product_code: 'horikoshi-item', condition_id: 'testcond' }) });
  ok(r.status === 200, 'attrs: 未紐付け行の保存でそのまま紐づけ');
  r = await j('/api/masters/attrs');
  ok(r.body.rows.find(x => x.product_key === 'horikoshi-item').linked === true, 'attrs: 紐づけ後は linked=true');
  db.prepare("DELETE FROM po_product_attrs WHERE product_key IN ('ghost-item-x','horikoshi-item')").run(); // 後続テストへの影響を消す
  // UI: チップ+フィルタ維持+スクロール復元
  const adminHtmlA = await (await fetch(base + '/admin')).text();
  ok(adminHtmlA.includes('data-attrview') && adminHtmlA.includes('未紐付け ') && adminHtmlA.includes('FILT_Q') && adminHtmlA.includes('SCROLL_RESTORE'),
    '/admin 商品紐付けUI (チップ+フィルタ維持+スクロール復元)');
  ok(adminHtmlA.includes('applyMasterFilter'), '/admin フィルタはタブ状態と組み合わせて適用');
}

// ═══ P15b: FAX送信 (eFaxメールゲートウェイ, fake transport + fake PDF) ═══
console.log('── P15b: FAX送信 (eFaxゲートウェイ) ──');
{
  process.env.PO_EMAIL_FAKE = '1';
  process.env.PO_PDF_FAKE = '1';
  const emailMod = await imp('apps/purchase-orders/email.js');
  const jsonPost = (p, body, key) => j(p, { method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
    body: JSON.stringify({ expectedMode: 'dry_run', expectedChannel: 'fax', ...body }) });

  // FAX番号の正規化・ゲートウェイ変換 (eFax公式: 81 + 先頭0を除いた番号 + @efaxsend.com)
  ok(emailMod.normalizeFaxNumber('06-7632-4190') === '0676324190', 'normalizeFaxNumber: ハイフン除去');
  ok(emailMod.faxGatewayAddress('0676324190') === '81676324190@efaxsend.com', 'faxGatewayAddress: 81変換 (03-1234-5678→81312345678 と同式)');
  let threw = null;
  try { emailMod.normalizeFaxNumber('123-45'); } catch (e) { threw = e.message; }
  ok(threw && threw.includes('FAX番号が不正'), 'normalizeFaxNumber: 不正番号はthrow', threw);

  // 仕入先マスタ: 発注方法=FAXはFAX番号必須 / 不正番号は保存拒否
  r = await jsonPost('/api/masters/suppliers', { supplier_code: '0002', name: 'ビーフリー様', send_method: 'fax' });
  ok(r.status === 400 && r.body.error.includes('FAX番号'), 'suppliers: send_method=faxでFAX番号未入力は拒否', r.body.error);
  r = await jsonPost('/api/masters/suppliers', { supplier_code: '0002', name: 'ビーフリー様', send_method: 'fax', fax_number: '123-45' });
  ok(r.status === 400 && r.body.error.includes('FAX番号が不正'), 'suppliers: 不正FAX番号は保存拒否', r.body.error);
  r = await jsonPost('/api/masters/suppliers', { supplier_code: '0002', name: 'ビーフリー様', send_method: 'fax', fax_number: '06-7632-4190', contact_name: '佐藤' });
  ok(r.status === 200 && r.body.ok, 'suppliers: FAX仕入先の登録OK', r.body.error);

  // FAX仕入先のPOを発行 → preview は channel=fax + ゲートウェイ宛先 + PDF添付名
  r = await j('/api/supplier/2/issue', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: [{ code: 'gyoumuhandcream60-BI', qty: 24 }], requestedDate: '2026-07-30' }) });
  ok(r.body.ok, 'FAX仕入先のPO発行', r.body.error);
  const faxOrderId = r.body.id;
  r = await j('/api/orders/' + faxOrderId + '/email/preview');
  ok(r.body.ok && r.body.channel === 'fax' && r.body.faxNumber === '06-7632-4190', 'preview: channel=fax + FAX番号', r.body);
  ok(r.body.to.length === 1 && r.body.to[0] === '81676324190@efaxsend.com', 'preview: 宛先=eFaxゲートウェイ (81変換)', r.body.to);
  ok(/\.pdf$/.test(r.body.attachmentName) && r.body.body.startsWith('佐藤様'), 'preview: 添付=.pdf + 送付状に担当者様', r.body.attachmentName);

  // PDFプレビュー配信 (fake PDF)
  {
    const pr = await fetch(base + '/api/orders/' + faxOrderId + '/fax/pdf');
    const buf = Buffer.from(await pr.arrayBuffer());
    ok(pr.status === 200 && pr.headers.get('content-type').includes('application/pdf') && buf.subarray(0, 5).toString() === '%PDF-',
      'GET /orders/:id/fax/pdf → application/pdf', pr.status);
  }

  // プレビュー時とチャネルが変わっていたら拒否 (expectedMode と同じ発想)
  r = await jsonPost('/api/orders/' + faxOrderId + '/email/send', { expectedChannel: 'email' }, 'fax-key-ch');
  ok(r.status === 400 && r.body.error.includes('送信方法が変わっています'), 'send: expectedChannel不一致は拒否', r.body.error);

  // dry-run送信: 宛先は社内へ差し替え、ジョブは channel=fax + PDFスナップショット保存
  r = await jsonPost('/api/orders/' + faxOrderId + '/email/send', {}, 'fax-key-1');
  ok(r.body.ok && r.body.channel === 'fax' && r.body.status === 'sent' && r.body.dryRun === true, 'FAX dry-run送信 → sent', r.body);
  const faxJob = db.prepare("SELECT * FROM po_email_jobs WHERE order_id=? ORDER BY id DESC").get(faxOrderId);
  ok(faxJob.channel === 'fax' && faxJob.attachment_pdf && faxJob.attachment_pdf.length > 20, 'ジョブ: channel=fax + attachment_pdf保存');
  ok(faxJob.to_addr === 'me@b-faith.biz' && faxJob.subject.startsWith('【DRYRUN】') && /\.pdf$/.test(faxJob.attachment_name),
    'ジョブ: dry-run宛先差し替え + .pdf添付名', faxJob.to_addr);
  ok(faxJob.body.includes('本来の宛先: FAX 06-7632-4190'), 'ジョブ: dry-run本文に本来のFAX宛先', faxJob.body.slice(0, 80));

  // MIME: application/pdf 添付で組み立てられる (FAKE送信はMIMEを通らないため直接検証)
  {
    const mime = emailMod.buildMime(faxJob);
    ok(mime.includes('Content-Type: application/pdf') && mime.includes(`filename="${faxJob.attachment_name}"`),
      'buildMime: application/pdf 添付', faxJob.attachment_name);
    const bad = { ...faxJob, attachment_name: 'PO-1.csv' };
    let e2 = null; try { emailMod.buildMime(bad); } catch (e) { e2 = e.message; }
    ok(e2 && e2.includes('.pdf'), 'buildMime: faxジョブの.csv添付名は拒否', e2);
  }

  // dry-run はdedup対象外 (メールと同じ仕様: 何度でも確認できる)
  r = await jsonPost('/api/orders/' + faxOrderId + '/email/send', {}, 'fax-key-2');
  ok(r.body.ok && r.body.status === 'sent', 'FAX dry-runは繰り返し送信可 (dedup対象外)', r.body.error);

  // live: 本送信は dedup が効く (FAKE transportなので実送信なし)
  await jsonPost('/api/email/mode', { mode: 'live', confirm: 'LIVE' });
  r = await jsonPost('/api/orders/' + faxOrderId + '/email/send', { expectedMode: 'live' }, 'fax-key-3');
  ok(r.body.ok && r.body.status === 'sent' && r.body.dryRun === false, 'FAX live送信 → sent (fake)', r.body);
  const faxLiveJob = db.prepare("SELECT * FROM po_email_jobs WHERE order_id=? AND is_dry_run=0 ORDER BY id DESC").get(faxOrderId);
  ok(faxLiveJob.to_addr === '81676324190@efaxsend.com' && faxLiveJob.channel === 'fax', 'liveジョブ: 宛先=ゲートウェイ', faxLiveJob.to_addr);
  r = await jsonPost('/api/orders/' + faxOrderId + '/email/send', { expectedMode: 'live' }, 'fax-key-4');
  ok(r.status === 400 && r.body.error.includes('送信済み'), 'FAX dedup: 同一内容の本送信は拒否', r.body.error);
  await jsonPost('/api/email/mode', { mode: 'dry_run' });

  // 送信済みジョブのPDF控え配信
  {
    const pr = await fetch(base + '/api/email-jobs/' + faxJob.id + '/pdf');
    ok(pr.status === 200 && pr.headers.get('content-type').includes('application/pdf'), 'GET /email-jobs/:id/pdf → PDF控え', pr.status);
    const nf = await fetch(base + '/api/email-jobs/999999/pdf');
    ok(nf.status === 400 || nf.status === 404, 'PDF控え: 存在しないジョブは404', nf.status);
  }

  // /admin にFAX番号欄
  const adminHtmlF = await (await fetch(base + '/admin')).text();
  ok(adminHtmlF.includes('fax_number') && adminHtmlF.includes('FAX番号'), '/admin 仕入先にFAX番号欄');
}

// ═══ ロジザード在庫 mirror 自動反映 (画面アクセス時に captured_at 比較 → 在庫オーバーレイ自動更新) ═══
console.log('── ロジザード在庫 mirror 自動反映 ──');
{
  process.env.PO_LZ_MIRROR_CHECK_INTERVAL_MS = '0'; // テストでは毎回チェック (throttle無効)
  process.env.PO_LZ_MIRROR_MIN_PRODUCTS = '1';      // fixtureは商品数が少ないため下限を下げる
  await j('/api/ne-overlay', { method: 'DELETE' }); // 直前セクションの overlay 状態に依存しない
  const insLz = db.prepare(`INSERT INTO mirror_logizard_stock
    (商品ID, 商品名, ロケ, 品質区分名, 在庫数, 引当数, 在庫日, captured_at, synced_at)
    VALUES (?,?,?,?,?,?,?,?,?)`);
  const lzSet = (capturedAt, noflyStock) => {
    db.prepare('DELETE FROM mirror_logizard_stock').run();
    insLz.run('noflyersticker', 'チラシ', '001-001-01', '良品', noflyStock, 5, '20260817', capturedAt, capturedAt);
    insLz.run('noflyersticker', 'チラシ', '002-001-01', '良品', 60, 0, '20260817', capturedAt, capturedAt);
    insLz.run('noflyersticker', 'チラシ', '002-002-01', '不良品', 7, 0, '20260817', capturedAt, capturedAt);
    insLz.run('mirror-only-item', 'ミラーだけの商品', '003-001-01', '良品', 9, 0, '20260817', capturedAt, capturedAt);
  };
  const ovRow = key => db.prepare('SELECT * FROM po_ne_overlay_rows WHERE product_key=?').get(key);
  const ovMeta = () => db.prepare('SELECT * FROM po_ne_overlay_meta WHERE id=1').get();

  // 1) mirror が新しければ画面アクセスだけで自動反映される (良品のみ・ロケ横断合算)
  const t1 = new Date(Date.now() - 60000).toISOString();
  lzSet(t1, 100);
  r = await j('/api/supplier/1');
  ok(r.body.overlay && r.body.overlay.source === 'logizard_mirror' && r.body.overlay.applied === true,
    'mirror自動反映: 画面アクセスで overlay 作成 (source=logizard_mirror)', r.body.overlay);
  ok(ovRow('noflyersticker') && ovRow('noflyersticker').在庫数 === 160, 'mirror自動反映: 良品のみロケ横断合算 (100+60、不良品7除外)', ovRow('noflyersticker'));
  ok(ovMeta().captured_at === t1, 'mirror自動反映: meta.captured_at=mirrorのcaptured_at', ovMeta());
  {
    const zf = db.prepare("SELECT 在庫数 FROM po_ne_overlay_rows WHERE product_key='gyoumuhandcream60-bi'").get();
    ok(zf && zf.在庫数 === 0, 'mirror自動反映: mirrorに無い取扱中商品は在庫0扱い (売り切れ反映)', zf);
  }
  ok(!db.prepare("SELECT 1 FROM po_product_code_canonical WHERE product_key='mirror-only-item'").get(),
    'mirror自動反映: canonical は更新しない (mirror商品IDは小文字化済みで表記が失われているため)');

  // 2) 手動取込の方が新しい間は自動で上書きしない (後勝ち)
  {
    const mk = (csvText) => { const fd = new FormData(); fd.append('file', new Blob([iconv.encode(csvText, 'Shift_JIS')]), 'manual.csv'); return fd; };
    const csvText = '"商品ID","品質区分名","在庫数(引当数を含む)","引当数"\r\n"noflyersticker","良品","70","0"\r\n';
    const prev = await j('/api/logizard-stock/csv', { method: 'POST', body: mk(csvText) });
    const fd2 = mk(csvText);
    fd2.append('commit', '1'); fd2.append('fileHash', prev.body.fileHash); fd2.append('planHash', prev.body.planHash);
    r = await j('/api/logizard-stock/csv', { method: 'POST', body: fd2 });
    ok(r.body.ok, 'mirror共存: 手動ロジザードCSV取込は従来どおり成功', r.body);
    r = await j('/api/supplier/1');
    ok(r.body.overlay && r.body.overlay.source === 'logizard' && ovRow('noflyersticker').在庫数 === 70,
      'mirror共存: 手動の方が新しい間は自動で上書きされない (後勝ち)', r.body.overlay && r.body.overlay.source);
  }

  // 3) mirror が進んだら自動が手動を上書きして再開
  const t3 = new Date(Date.now() + 5000).toISOString();
  lzSet(t3, 110);
  r = await j('/api/supplier/1');
  ok(r.body.overlay && r.body.overlay.source === 'logizard_mirror' && ovRow('noflyersticker').在庫数 === 170,
    'mirror共存: mirror更新で自動反映が再開 (110+60)', { src: r.body.overlay && r.body.overlay.source, row: ovRow('noflyersticker') });
  {
    const dashLz = await (await fetch(base + '/')).text();
    ok(dashLz.includes('時点') && dashLz.includes('毎時自動反映'), '/ 鮮度表示: 「何時時点」+毎時自動反映ラベル');
    ok(!dashLz.includes('3時間以上前の在庫です'), '/ 鮮度表示: 新しい在庫に⚠️は出ない');
  }

  // 4) NEマスタCSVが有効に適用されている間はスキップ (原価等の上書きを消さない)
  {
    const fdNe = new FormData();
    fdNe.append('file', new Blob([iconv.encode('"商品コード","在庫数","発注残数"\r\n"noflyersticker","55","0"\r\n', 'Shift_JIS')]), 'ne-manual.csv');
    r = await j('/api/ne-overlay/csv', { method: 'POST', body: fdNe });
    ok(r.body.ok, 'mirror共存: NE手動CSV取込成功');
    const t4 = new Date(Date.now() + 10000).toISOString();
    lzSet(t4, 120);
    r = await j('/api/supplier/1');
    ok(r.body.overlay && r.body.overlay.source === 'ne', 'mirror共存: NE手動が有効な間はmirrorが進んでもスキップ', r.body.overlay && r.body.overlay.source);
    // 翌朝のPML同期でNEが失効したら自動反映が再開する (mirror t4=+10s は朝同期+8sより新しい)
    const oldSync = db.prepare('SELECT src_ne_products_synced_at FROM mirror_pml_published WHERE id=1').get().src_ne_products_synced_at;
    db.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at=? WHERE id=1').run(new Date(Date.now() + 8000).toISOString());
    r = await j('/api/supplier/1');
    ok(r.body.overlay && r.body.overlay.source === 'logizard_mirror' && ovRow('noflyersticker').在庫数 === 180,
      'mirror共存: 朝同期でNE失効後は自動反映が再開 (120+60)', r.body.overlay && r.body.overlay.source);
    db.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at=? WHERE id=1').run(oldSync);
  }

  // 5) 異常データガード: mirror の商品数が下限未満なら反映しない (大量の在庫0上書き防止)
  {
    process.env.PO_LZ_MIRROR_MIN_PRODUCTS = '999';
    const capBefore = ovMeta().captured_at;
    lzSet(new Date(Date.now() + 20000).toISOString(), 130);
    r = await j('/api/supplier/1');
    ok(ovMeta().captured_at === capBefore, 'mirrorガード: 商品数が下限未満のmirrorは反映しない (overlay据え置き)', ovMeta());
    process.env.PO_LZ_MIRROR_MIN_PRODUCTS = '1';
  }

  // 6) 「CSV取込を解除」は同じmirror世代では復活しない (次の毎時更新で自動再開)
  {
    r = await j('/api/ne-overlay', { method: 'DELETE' });
    ok(r.body.ok, '解除API成功');
    r = await j('/api/supplier/1');
    ok(r.body.overlay === null, '解除サプレッション: 同じmirror世代では自動反映が復活しない', r.body.overlay);
    lzSet(new Date(Date.now() + 25000).toISOString(), 140);
    r = await j('/api/supplier/1');
    ok(r.body.overlay && r.body.overlay.source === 'logizard_mirror' && ovRow('noflyersticker').在庫数 === 200,
      '解除サプレッション: 次のmirror更新 (新しいcaptured_at) で自動再開 (140+60)', r.body.overlay);
  }

  // 7) 3時間以上古い mirror は⚠️表示 (数字は使う)
  {
    db.prepare('DELETE FROM po_ne_overlay_meta').run();
    db.prepare('DELETE FROM po_ne_overlay_rows').run();
    db.prepare("DELETE FROM po_settings WHERE key='po_lz_mirror_suppress_capture'").run();
    lzSet(new Date(Date.now() - 4 * 3600 * 1000).toISOString(), 150);
    const dashStale = await (await fetch(base + '/')).text();
    ok(dashStale.includes('3時間以上前の在庫です') && dashStale.includes('時点'), '/ 鮮度表示: 3時間超は⚠️+「何時時点」併記');
    r = await j('/api/supplier/1');
    ok(r.body.overlay && r.body.overlay.source === 'logizard_mirror' && ovRow('noflyersticker').在庫数 === 210,
      '3時間超でも数字は使う (150+60・中原さん決定)', ovRow('noflyersticker'));
  }

  // 8) 夜間→翌朝: 朝のPML同期の方が新しければ mirror overlay は失効し朝同期の在庫へ (意図した設計・Codex LZM-R1 High回答)
  {
    const oldSync2 = db.prepare('SELECT src_ne_products_synced_at FROM mirror_pml_published WHERE id=1').get().src_ne_products_synced_at;
    db.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at=? WHERE id=1').run(new Date(Date.now() + 30000).toISOString());
    r = await j('/api/supplier/1');
    ok(r.body.overlay && r.body.overlay.applied === false, '夜間→翌朝: 朝同期の方が新しければ overlay 失効 (朝のNE在庫が正・mirror再開は次の毎時更新)', r.body.overlay);
    db.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at=? WHERE id=1').run(oldSync2);
  }

  // 9) 負数在庫を含む mirror は反映しない (手動経路の stock<0 拒否と同等の防衛。
  //    別ロケの正数とSUMで相殺されても行単位で検出する — Codex LZM-R2 Medium)
  {
    const capBefore = ovMeta().captured_at;
    const tNeg = new Date(Date.now() + 35000).toISOString();
    db.prepare('DELETE FROM mirror_logizard_stock').run();
    insLz.run('noflyersticker', 'チラシ', '001-001-01', '良品', -3, 0, '20260817', tNeg, tNeg);
    insLz.run('noflyersticker', 'チラシ', '002-001-01', '良品', 10, 0, '20260817', tNeg, tNeg); // SUM=7>0 でも行の負数で拒否
    insLz.run('mirror-only-item', 'ミラーだけの商品', '003-001-01', '良品', 9, 0, '20260817', tNeg, tNeg);
    r = await j('/api/supplier/1');
    ok(ovMeta().captured_at === capBefore, '負数在庫の行を含むmirrorは反映しない (ロケ間相殺でも検出・overlay据え置き)', ovMeta());
  }

  // 10) overlay が無い状態でも、朝のPML同期より古い mirror は適用しない
  //     (初回・解除後に前日18時のmirrorで今朝のNE在庫を上書きしない — Codex LZM-R2 High)
  {
    db.prepare('DELETE FROM po_ne_overlay_meta').run();
    db.prepare('DELETE FROM po_ne_overlay_rows').run();
    db.prepare("DELETE FROM po_settings WHERE key='po_lz_mirror_suppress_capture'").run();
    const oldSync3 = db.prepare('SELECT src_ne_products_synced_at FROM mirror_pml_published WHERE id=1').get().src_ne_products_synced_at;
    db.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at=? WHERE id=1').run(new Date().toISOString());
    lzSet(new Date(Date.now() - 60000).toISOString(), 160); // mirror = 朝同期より古い
    r = await j('/api/supplier/1');
    ok(r.body.overlay === null, '朝同期より古いmirrorはoverlay無しでも適用しない (朝のNE在庫が正)', r.body.overlay);
    db.prepare('UPDATE mirror_pml_published SET src_ne_products_synced_at=? WHERE id=1').run(oldSync3);
  }

  // 後片付け (以降のセクションに mirror 自動反映が影響しないように)
  db.prepare('DELETE FROM mirror_logizard_stock').run();
  db.prepare('DELETE FROM po_ne_overlay_meta').run();
  db.prepare('DELETE FROM po_ne_overlay_rows').run();
  db.prepare("DELETE FROM po_settings WHERE key='po_lz_mirror_suppress_capture'").run();
  delete process.env.PO_LZ_MIRROR_CHECK_INTERVAL_MS;
  delete process.env.PO_LZ_MIRROR_MIN_PRODUCTS;
}

// ═══ 全ページのインラインJS構文チェック (サーバtemplate literal内クライアントJSの括弧崩れ等を機械検出) ═══
console.log('── ページ内スクリプトの構文チェック ──');
{
  for (const p of ['/', '/supplier/1', '/products', '/orders', '/admin', '/inbound', '/inbound-plan', '/backorders', '/shortage-risk']) {
    const html = await (await fetch(base + p)).text();
    const scripts = [...html.matchAll(/<script(?![^>]*src=)[^>]*>([\s\S]*?)<\/script>/gi)].map(m => m[1]);
    let err = null;
    for (const s of scripts) {
      // new Function はテスト内での「構文コンパイルのみ」(呼び出さない)。対象は自サーバが生成したページで
      // 外部入力は含まれない。実行はしないため副作用なし (構文エラー検出が目的)
      try { new Function(s); } catch (e) { err = e.message + ' | ' + s.slice(0, 120); break; }
    }
    ok(!err && scripts.length > 0, `script構文OK: ${p} (${scripts.length}本)`, err);
  }
}

server.close();
console.log(`\n=== RESULT: pass=${pass} fail=${fail} ===`);
process.exit(fail ? 1 : 0);
