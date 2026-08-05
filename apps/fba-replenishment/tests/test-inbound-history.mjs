/**
 * FBA納品実績 (fba_inbound_shipments / _items) のテスト。
 *   node apps/fba-replenishment/tests/test-inbound-history.mjs [repo-root]
 * DATA_DIR を一時ディレクトリに向けるので、本番・開発の fba.db には触れない。
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-ih-'));
process.env.DATA_DIR = tmp;

const db = await import(pathToFileURL(path.join(repo, 'apps/fba-replenishment/db.js')));
await db.initDb();

let pass = 0, fail = 0;
const ok = (c, n) => { if (c) { pass++; console.log(`  ok - ${n}`); } else { fail++; console.log(`  NG - ${n}`); } };
const eq = (a, b, n) => ok(a === b, `${n} (got ${JSON.stringify(a)}, want ${JSON.stringify(b)})`);

// ===== 1. ShipmentName から作成日時を抽出 =====
console.log('\n[1] parseShipmentCreatedAt');
{
  const r = db.parseShipmentCreatedAt('FBA STA (2026/06/16 05:49)-TPY1');
  eq(r.createdAt, '2026-06-16 05:49', 'STA形式から日時を抽出');
  eq(r.createdDate, '2026-06-16', 'STA形式から日付を抽出');

  // 実データに1件だけ存在した非STA名。日付が取れないことを明示的に確認する
  const r2 = db.parseShipmentCreatedAt('i2i shipment');
  eq(r2.createdAt, null, '非STA名は日時null');
  eq(r2.createdDate, null, '非STA名は日付null');

  eq(db.parseShipmentCreatedAt('').createdDate, null, '空文字はnull');
  eq(db.parseShipmentCreatedAt(null).createdDate, null, 'nullでも落ちない');
}

// ===== 2. upsert と明細の集計 =====
console.log('\n[2] upsertInboundShipments / replaceInboundItems');
{
  const shipments = [
    { ShipmentId: 'FBA_A', ShipmentName: 'FBA STA (2026/07/01 10:00)-TPY1', DestinationFulfillmentCenterId: 'TPY1', ShipmentStatus: 'CLOSED', LabelPrepType: 'SELLER_LABEL' },
    { ShipmentId: 'FBA_B', ShipmentName: 'FBA STA (2026/07/01 15:30)-KIX5', DestinationFulfillmentCenterId: 'KIX5', ShipmentStatus: 'RECEIVING', LabelPrepType: 'SELLER_LABEL' },
    { ShipmentId: 'FBA_C', ShipmentName: 'FBA STA (2026/07/02 09:00)-TPY1', DestinationFulfillmentCenterId: 'TPY1', ShipmentStatus: 'CANCELLED', LabelPrepType: 'SELLER_LABEL' },
    { ShipmentId: 'FBA_D', ShipmentName: '手動で付けた名前', DestinationFulfillmentCenterId: 'XJW1', ShipmentStatus: 'CLOSED', LabelPrepType: 'SELLER_LABEL' },
  ];
  const r1 = db.upsertInboundShipments(shipments);
  eq(r1.inserted, 4, '4件insert');
  eq(r1.updated, 0, 'updateは0');

  // 同じものを再投入しても増えない (冪等)
  const r2 = db.upsertInboundShipments(shipments);
  eq(r2.inserted, 0, '再投入でinsertは0');
  eq(r2.updated, 4, '再投入は4件update');

  // FBA_A: 2SKU / 送70 受70 (欠品なし)
  db.replaceInboundItems('FBA_A', [
    { SellerSKU: 'sku-1', FulfillmentNetworkSKU: 'X1', QuantityShipped: 50, QuantityReceived: 50 },
    { SellerSKU: 'sku-2', FulfillmentNetworkSKU: 'X2', QuantityShipped: 20, QuantityReceived: 20 },
  ]);
  // FBA_B: 2SKU / 送30 受22 (未受領8) ※sku-1 は FBA_A と重複 → SKU数の重複除去を確認するため
  const rb = db.replaceInboundItems('FBA_B', [
    { SellerSKU: 'sku-1', FulfillmentNetworkSKU: 'X1', QuantityShipped: 10, QuantityReceived: 5 },
    { SellerSKU: 'sku-3', FulfillmentNetworkSKU: 'X3', QuantityShipped: 20, QuantityReceived: 17 },
  ]);
  eq(rb.skus, 2, 'FBA_B は2SKU');
  eq(rb.shipped, 30, 'FBA_B 送付30');
  eq(rb.received, 22, 'FBA_B 受領22');
  // FBA_C (キャンセル): 集計から外れることを確認するため入れておく
  db.replaceInboundItems('FBA_C', [
    { SellerSKU: 'sku-9', QuantityShipped: 999, QuantityReceived: 0 },
  ]);

  // 同一SKUが複数行で来てもPK衝突で落ちない
  const rd = db.replaceInboundItems('FBA_D', [
    { SellerSKU: 'sku-5', QuantityShipped: 3, QuantityReceived: 3 },
    { SellerSKU: 'sku-5', QuantityShipped: 7, QuantityReceived: 7 },
  ]);
  eq(rd.skus, 1, '重複SKUは1件に畳まれる');
  eq(rd.shipped, 3, '重複SKUの2行目は無視 (先勝ち)');

  // 明細の入れ替え: 古い行が残らない
  db.replaceInboundItems('FBA_A', [
    { SellerSKU: 'sku-1', QuantityShipped: 50, QuantityReceived: 50 },
  ]);
  eq(db.getInboundItems('FBA_A').length, 1, '入れ替えで古い明細が消える');
  db.replaceInboundItems('FBA_A', [
    { SellerSKU: 'sku-1', QuantityShipped: 50, QuantityReceived: 50 },
    { SellerSKU: 'sku-2', QuantityShipped: 20, QuantityReceived: 20 },
  ]);
}

// ===== 3. 日別サマリ =====
console.log('\n[3] getInboundDailySummary');
{
  const rows = db.getInboundDailySummary({});
  const d0701 = rows.find(r => r.created_date === '2026-07-01');
  ok(!!d0701, '7/1 の行がある');
  eq(d0701.shipment_count, 2, '7/1 はプラン2件');
  eq(d0701.sku_count, 3, '7/1 のSKU数は重複除去して3 (sku-1が2プランに跨る)');
  eq(d0701.qty_shipped, 100, '7/1 の送付は100');
  eq(d0701.qty_received, 92, '7/1 の受領は92');
  eq(d0701.qty_unreceived, 8, '7/1 の未受領は8');

  const d0702 = rows.find(r => r.created_date === '2026-07-02');
  ok(!d0702, 'キャンセルのみの日は既定で出ない');

  const withCancel = db.getInboundDailySummary({ includeCancelled: true });
  ok(!!withCancel.find(r => r.created_date === '2026-07-02'), 'includeCancelled で7/2が出る');

  // 作成日が取れなかったシップメントは日別集計に混ざらない
  ok(!rows.some(r => r.created_date === null), '作成日nullは集計に含まれない');
  eq(db.getInboundShipmentsWithoutDate().length, 1, '作成日不明は1件として別に拾える');

  // 期間フィルタ
  eq(db.getInboundDailySummary({ from: '2026-07-02' }).length, 0, 'from で範囲外は落ちる');
  eq(db.getInboundDailySummary({ to: '2026-07-01' }).length, 1, 'to で範囲内だけ残る');
}

// ===== 4. 月別サマリ =====
console.log('\n[4] getInboundMonthlySummary');
{
  const rows = db.getInboundMonthlySummary({});
  const m = rows.find(r => r.ym === '2026-07');
  ok(!!m, '2026-07 の行がある');
  eq(m.shipment_count, 2, '月内のプラン数');
  eq(m.sku_count, 3, '月内のSKU数 (重複除去)');
  eq(m.qty_shipped, 100, '月内の送付数');
}

// ===== 5. 未受領一覧 =====
console.log('\n[5] getInboundUnreceived');
{
  const all = db.getInboundUnreceived({});
  eq(all.length, 2, '未受領は2明細 (sku-1@B, sku-3@B)');
  ok(all.every(r => r.qty_unreceived > 0), '全行が未受領>0');
  ok(!all.some(r => r.shipment_id === 'FBA_C'), 'キャンセル分は未受領に出さない');

  // 経過日数フィルタ: テストデータは2026-07-01 なので、大きすぎる閾値なら0件になる
  const far = db.getInboundUnreceived({ minDays: 100000 });
  eq(far.length, 0, 'minDays が大きければ0件');
}

// ===== 6. 明細取り直し対象の判定 =====
console.log('\n[6] getShipmentsNeedingItemSync');
{
  const targets = db.getShipmentsNeedingItemSync({ graceDays: 60 });
  const ids = targets.map(t => t.shipment_id);
  ok(ids.includes('FBA_B'), 'RECEIVING は対象 (受領が増えうる)');
  ok(!ids.includes('FBA_A'), 'CLOSED かつ 未受領なし は対象外');

  eq(db.getShipmentsNeedingItemSync({ all: true }).length, 4, 'all指定なら全件');
}

// ===== 7. 取込状況 =====
console.log('\n[7] getInboundSyncStatus');
{
  const s = db.getInboundSyncStatus();
  eq(s.total_shipments, 4, '総シップメント数');
  eq(s.items_pending, 0, '明細未取得は0');
  eq(s.created_date_missing, 1, '作成日不明は1');
  eq(s.date_from, '2026-07-01', '期間の開始');
  eq(s.date_to, '2026-07-02', '期間の終了');
}

// ===== 8. ミニPC → Render の同期 (export → import) =====
console.log('\n[8] exportInboundRows / importInboundRows');
{
  const exported = db.exportInboundRows({});
  eq(exported.shipments.length, 4, '4シップメントをエクスポート');
  ok(exported.items.length >= 5, '明細も一緒に出る');
  eq(exported.has_more, false, '4件ならhas_moreはfalse');

  // 別DBに入れ直して同じ集計になるか (Render 側の再現)
  const tmp2 = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-ih2-'));
  process.env.DATA_DIR = tmp2;
  const db2 = await import(pathToFileURL(path.join(repo, 'apps/fba-replenishment/db.js')) + '?v=2');
  await db2.initDb();

  const imported = db2.importInboundRows(exported);
  eq(imported.shipments, 4, '4件import');

  const src = db.getInboundDailySummary({});
  const dst = db2.getInboundDailySummary({});
  eq(JSON.stringify(dst), JSON.stringify(src), '同期後の日別サマリが一致する');
  eq(db2.getInboundUnreceived({}).length, 2, '同期後も未受領が一致');

  // カーソル: 同期済みの位置が取れる
  ok(!!db2.getInboundSyncCursor(), '同期カーソルが取れる');

  // 差分同期: カーソル以降だけ返る
  const cursor = db2.getInboundSyncCursor();
  const delta = db.exportInboundRows({ since: cursor });
  eq(delta.shipments.length, 0, 'カーソル以降に更新がなければ0件');

  // has_more: limit で切れることを確認
  const paged = db.exportInboundRows({ limit: 2 });
  eq(paged.shipments.length, 2, 'limitで2件に切れる');
  eq(paged.has_more, true, '続きがあればhas_more=true');
  eq(paged.items.every(i => ['FBA_A', 'FBA_B', 'FBA_C', 'FBA_D'].includes(i.shipment_id)), true,
     'ページ内の明細だけが返る');

  fs.rmSync(tmp2, { recursive: true, force: true });
}

fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n${pass} pass / ${fail} fail`);
process.exit(fail === 0 ? 0 : 1);
