/**
 * movements-seed.mjs — 入出庫の履歴と月別のまとめの試験の見本 (scripts/test-master-movements.mjs・test-master-movements-ui.mjs で同じものを使う)
 *
 * Company DB (PGlite): ロジザードの日次 (本物の取込 → 日の締め) と NE の出荷。持ち主のロール (deploy) で入れて、終わったら deploy のまま返す
 *   mv-a: 在庫 100 (10/01) → 90 (10/02) → 10/03 は取れない → 75 (10/04) → 123 (10/05) → 123 (10/06)
 *         出荷 S0 9/30 4 (錨の前)・S1 10/02 10 (同じ伝票の 2 明細 + 取消の明細)・S2 10/03 5 (FBA納品)・S3 10/04 5・S4 10/05 19:00 2 (18 時の取得の後)・
 *         S5 10/07 10:00 3・S6 10/07 12:00 1 / 取消の伝票 S7・出荷していない受注 S9 (6)
 *   mv-b: 1 日 45 ずつ減る (300 → 255 → … → 75)・出荷 45 × 5 日 (10/02〜10/06) + 9/30 に 30 (錨の前) = 255 行 (ページの試験)
 * 発注アプリ (warehouse-mirror.db): mv-a の仕入 LZ-1 (10/05・良品 50・不良品 1・単価 100・訂正で消えた行 70)・発注 9/20 80 × 372・移行の発注 7/13・下書き /
 *   いまのロジザードの写し 10/07 11:00 JST (mv-a = 118・mv-b = 75)
 * 今 = 2026-10-07 13:00 JST (SEED_NOW)
 */
import { captureLogizardInventory, closeStockDays } from '../../apps/company-db/inventory/logizard.mjs';

export const SEED_NOW = Date.parse('2026-10-07T04:00:00Z');
const quiet = () => {};
const J = (s) => `${s}+09:00`;
const lzRow = (id, qty) => ({ '商品ID': id, '商品名': id, 'バーコード': '4900000000001', 'ブロック略称': 'P3FA', 'ロケ': '001', '品質区分名': '良品', '有効期限': '', '入荷日': '2026/08/01', '在庫数': qty, '引当数': 0, 'ロケ業務区分': 'ピック', '最終入荷日': '2026/08/01', '最終出荷日': null, '在庫日': '2026/10/01' });

/** Company DB の見本 (pg = PGlite・db = その adapter・skuA / skuB = sku_id) */
export async function seedMovementsDb(pg, db, { skuA, skuB }) {
  await pg.query('set role deploy');
  for (const [d, a, b] of [['2026-10-01', 100, 300], ['2026-10-02', 90, 255], ['2026-10-04', 75, 165], ['2026-10-05', 123, 120], ['2026-10-06', 123, 75]]) {
    await captureLogizardInventory(db, { rows: [lzRow('mv-a', a), lzRow('mv-b', b)], capturedAt: `${d}T09:00:00.000Z` /* JST 18:00 */, log: quiet });
  }
  await closeStockDays(db, { todayJst: '2026-10-07', log: quiet });
  let seq = 0;
  const ship = async (slip, atJst, lines, { shop = '1', cancelled = false, shipped = true, orderJst = null } = {}) => {
    const at = shipped ? J(atJst) : null;
    const s = (await pg.query(`insert into core.shipments (company_id, ne_slip_no, shop_code, status, is_cancelled, shipped_at, ship_date_jst, order_date_jst, tracking_no, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, $1, $2, $3, $4, $5::timestamptz, ($5::timestamptz at time zone 'Asia/Tokyo')::date, $6::date, 'TRK-SECRET', $7, now(), 't', 'h') returning shipment_id`,
    [slip, shop, cancelled ? 'cancelled' : shipped ? 'shipped' : 'confirmed', cancelled, at, (orderJst || atJst).slice(0, 10), ++seq])).rows[0];
    let n = 0;
    for (const [sku, qty, lineCancelled] of lines) await pg.query(`insert into core.shipment_lines (company_id, shipment_id, line_no, sku_id, qty, is_cancelled, received_batch_seq) values (1, $1, $2, $3, $4, $5, $6)`, [s.shipment_id, String(++n), sku, qty, !!lineCancelled, seq]);
  };
  await ship('S0', '2026-09-30T10:00:00', [[skuA, 4]]);
  await ship('S1', '2026-10-02T10:00:00', [[skuA, 6], [skuA, 4], [skuA, 9, true]], { shop: '4' });   // 同じ伝票の 2 明細は足す・取消の明細は数えない
  await ship('S2', '2026-10-03T10:00:00', [[skuA, 5]], { shop: '15' });
  await ship('S3', '2026-10-04T10:00:00', [[skuA, 5]]);
  await ship('S4', '2026-10-05T19:00:00', [[skuA, 2]], { shop: '2' });
  await ship('S5', '2026-10-07T10:00:00', [[skuA, 3]]);
  await ship('S6', '2026-10-07T12:00:00', [[skuA, 1]]);
  await ship('S7', '2026-10-02T11:00:00', [[skuA, 99]], { cancelled: true });                     // 取消の伝票 = 数えない
  await ship('S9', null, [[skuA, 6]], { shipped: false, orderJst: '2026-10-07T09:00:00' });          // 出荷していない受注 = 受注数だけ
  for (const [d, base] of [['2026-10-02', 0], ['2026-10-03', 45], ['2026-10-04', 90], ['2026-10-05', 135], ['2026-10-06', 180]]) {
    for (let i = 0; i < 45; i++) await ship(`B${base + i}`, `${d}T${String(10 + Math.floor(i / 10)).padStart(2, '0')}:${String((i % 10) * 5).padStart(2, '0')}:00`, [[skuB, 1]]);
  }
  for (let i = 0; i < 30; i++) await ship(`B-old${i}`, `2026-09-30T10:${String(i).padStart(2, '0')}:00`, [[skuB, 1]]);
}

/** 発注アプリの見本 (mirrorDb = initMirrorDB の戻り・initPurchaseOrders の後) */
export function seedMovementsMirror(mirrorDb) {
  const now = new Date().toISOString();
  mirrorDb.prepare(`insert into po_suppliers (supplier_code, name, created_at, updated_at) values ('1', 'AMC', ?, ?) on conflict (supplier_code) do nothing`).run(now, now);
  const bid = mirrorDb.prepare(`insert into po_inbound_batches (file_name, file_hash, row_count, imported_at, actor) values ('a.csv', 'mv-h1', 1, ?, 'naka@test')`).run(now).lastInsertRowid;
  const rid = mirrorDb.prepare(`insert into po_inbound_receipts (source, source_key, receipt_date, first_batch_id, last_batch_id, imported_at) values ('logizard', 'LZ-1', '2026-10-05', ?, ?, ?)`).run(bid, bid, now).lastInsertRowid;
  const item = mirrorDb.prepare(`insert into po_inbound_items (receipt_id, line_key, batch_id, supplier_code, product_code, product_key, good_qty, defective_qty, unit_cost, superseded, created_at) values (?, ?, ?, '0001', 'MV-A', 'mv-a', ?, ?, ?, ?, ?)`);
  item.run(rid, 'k1', bid, 50, 1, 100, 0, now);
  item.run(rid, 'k0', bid, 70, 0, 100, 1, now);   // 訂正で消えた行 = 数えない
  const po = (issuedAt, qty, cost, origin = null) => {
    // 下書きで明細を入れてから発行にする (発行した発注に明細は足せない = 発注アプリの trigger)
    const id = mirrorDb.prepare(`insert into po_orders (supplier_code, supplier_name, status, created_at, updated_at, origin, ne_slip_number, send_blocked) values ('1', 'AMC', 'draft', ?, ?, ?, ?, ?)`)
      .run(now, now, origin, origin === 'migration' ? 'NE-3637' : null, origin === 'migration' ? 1 : null).lastInsertRowid;
    mirrorDb.prepare('insert into po_order_items (order_id, product_code, product_key, qty, unit_cost) values (?, ?, ?, ?, ?)').run(id, 'MV-A', 'mv-a', qty, cost);
    mirrorDb.prepare(`update po_orders set status = 'issued', issued_at = ?, po_number = ? where id = ?`).run(issuedAt, `PO-MV-${id}`, id);
  };
  po('2026-09-20T01:00:00.000Z', 80, 372); po('2026-07-13T01:00:00.000Z', 100, 300, 'migration');
  const draft = mirrorDb.prepare(`insert into po_orders (supplier_code, supplier_name, status, created_at, updated_at) values ('1', 'AMC', 'draft', ?, ?)`).run(now, now).lastInsertRowid;
  mirrorDb.prepare('insert into po_order_items (order_id, product_code, product_key, qty) values (?, ?, ?, ?)').run(draft, 'MV-A', 'mv-a', 999);   // 下書き = 数えない
  // いまのロジザードの写し (10/07 11:00 JST): mv-a = 118 (10:00 の 3 個の後に 2 個のずれ)・mv-b = 75
  const lz = mirrorDb.prepare(`insert into mirror_logizard_stock (商品ID, 商品名, ブロック略称, ロケ, 品質区分名, 在庫数, 引当数, captured_at, synced_at) values (?, ?, 'P', 'A-01', '良品', ?, 0, ?, ?)`);
  lz.run('mv-a', 'mv-a', 118, '2026-10-07T02:00:00.000Z', now); lz.run('mv-b', 'mv-b', 75, '2026-10-07T02:00:00.000Z', now);
}
