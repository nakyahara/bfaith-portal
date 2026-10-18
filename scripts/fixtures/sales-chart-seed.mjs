/**
 * sales-chart-seed.mjs — 販売数のグラフの試験の見本 (scripts/test-master-sales-chart.mjs・test-master-sales-chart-ui.mjs で同じものを使う)
 *
 * Company DB (PGlite): 注文は本物の取込 (core.apply_order_batch)・在庫は本物の取込 → 日の締め。試験の接続 (superuser) で入れる
 *   商品: sc-a (単品)・sc-b (単品)・sc-s (NE のセット = sc-a×2 + sc-b×1)・sc-s2 (入れ子のセット = sc-s×1 + sc-a×1)
 *   出品: 楽天 r-a (= sc-a×1)・Amazon am-s (= セット sc-s×1)・Amazon am-a3 (= sc-a×3 のまとめ売り)
 *   注文 (注文日 JST): 楽天 R1 10/16 sc-a 2 + sc-b 1 / R2 10/16 出品 r-a 1 / R4 10/16 sc-a 10 → 取り直しで外れる (明細が sc-b なし = 外れた明細) /
 *     Amazon M1 10/15 am-s 1・M2 10/15 am-a3 2 / Yahoo! Y1 10/10 sc-s2 1・Y2 10/10 取消の注文 sc-a 5 / au PAY U1 9/01 sc-a 4 (明細の取消 1) /
 *     メルカリ 10/01 sc-a 1 / 楽天 R3 2024-12-20 sc-a 9 (記録の前) / 楽天 R5 10/18 (今日) sc-a 7
 *   在庫 (sc-a): 10/13 5・10/14 0 (欠品)・10/15 は取れない・10/16 3 (sc-b はロジザードに無い)
 * 今 = 2026-10-18 13:00 JST (SALES_NOW)
 */
import { captureLogizardInventory, closeStockDays } from '../../apps/company-db/inventory/logizard.mjs';

export const SALES_NOW = Date.parse('2026-10-18T04:00:00Z');
const quiet = () => {};
const lzRow = (id, qty) => ({ '商品ID': id, '商品名': id, 'バーコード': '4900000000001', 'ブロック略称': 'P3FA', 'ロケ': '001', '品質区分名': '良品', '有効期限': '', '入荷日': '2026/08/01', '在庫数': qty, '引当数': 0, 'ロケ業務区分': 'ピック', '最終入荷日': '2026/08/01', '最終出荷日': null, '在庫日': '2026/10/01' });

/**
 * @param pg PGlite・db = その adapter
 * @param have { A, B, S, S2 } もうある sku_id (画面の試験 = 夜間ロードで作った商品)。無ければここで作る
 * @returns { A, B, S, S2 } (数)
 */
export async function seedSalesChartDb(pg, db, have = null) {
  await pg.query('reset role');
  const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
  const mk = async (code, kind) => {
    const p = kind === 'single' ? (await one(`insert into core.products (company_id, name) values (1, $1) returning product_id`, [`見本 ${code}`])).product_id : null;
    return Number((await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, $2, $3, $3) returning sku_id`, [p, kind, code])).sku_id);
  };
  const ids = have ? { ...have } : { A: await mk('sc-a', 'single'), B: await mk('sc-b', 'single'), S: await mk('sc-s', 'set'), S2: await mk('sc-s2', 'set') };
  if (!have) {
    await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 2, 'ne'), (1, $1, $3, 1, 'ne'), (1, $4, $1, 1, 'ne'), (1, $4, $2, 1, 'ne')`, [ids.S, ids.A, ids.B, ids.S2]);
  }
  const listing = async (mall, code, comps) => {
    const id = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, $1, '', $2, 'active') returning listing_id`, [mall, code])).listing_id);
    for (const [s, q] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, $3, 'imported', 'system')`, [id, s, q]);
  };
  await listing('rakuten', 'r-a', [[ids.A, 1]]);
  await listing('amazon', 'am-s', [[ids.S, 1]]);
  await listing('amazon', 'am-a3', [[ids.A, 3]]);
  let seq = 0;
  const H = (at, x = {}) => ({ source_system: 'mall_api', shop_code: '1', ordered_at: `${at}+09:00`, status: 'shipped', amount_source: 'mall_api', source_updated_at: '2026-10-18T00:00:00Z', transform_version: 't-1',
    content_hash: 'h' + (++seq), total_amount_jpy: null, items_amount_jpy: null, shipping_fee_jpy: null, shop_coupon_jpy: null, mall_coupon_jpy: null, points_used_jpy: null, ...x });
  const L = (key, x = {}) => ({ line_key: key, listing_code: null, sku_code: null, qty: 1, cancelled_qty: 0, unit_price_jpy: null, line_amount_jpy: 1000, tax_rate: null, amount_source: 'mall_api', ...x });
  const apply = (mall, scope, no, header, lines) => pg.query(`select core.apply_order_batch(1::smallint, $1, $2, $3, $4::bigint, $5::jsonb, $6::jsonb)`, [mall, scope, no, ++seq, JSON.stringify(header), JSON.stringify(lines)]);
  await apply('rakuten', 'main', 'R1', H('2026-10-16T10:00:00'), [L('1', { sku_code: 'sc-a', qty: 2 }), L('2', { sku_code: 'sc-b', qty: 1 })]);
  await apply('rakuten', 'main', 'R2', H('2026-10-16T11:00:00'), [L('1', { listing_code: 'r-a', qty: 1 })]);
  await apply('rakuten', 'main', 'R4', H('2026-10-16T12:00:00'), [L('1', { sku_code: 'sc-a', qty: 10 })]);
  await apply('rakuten', 'main', 'R4', H('2026-10-16T12:00:00'), [L('2', { sku_code: 'sc-b', qty: 0 })]);   // 取り直し = sc-a 10 の明細は外れる
  await apply('amazon', 'jp', 'M1', H('2026-10-15T10:00:00', { shop_code: null }), [L('1', { listing_code: 'am-s', qty: 1 })]);
  await apply('amazon', 'jp', 'M2', H('2026-10-15T11:00:00', { shop_code: '4' }), [L('1', { listing_code: 'am-a3', qty: 2 })]);
  await apply('yahoo', 'main', 'Y1', H('2026-10-10T10:00:00', { shop_code: '2' }), [L('1', { sku_code: 'sc-s2', qty: 1 })]);
  await apply('yahoo', 'main', 'Y2', H('2026-10-10T11:00:00', { shop_code: '2', status: undefined, is_cancelled: true }), [L('1', { sku_code: 'sc-a', qty: 5, cancelled_qty: 5 })]);
  await apply('aupay', 'main', 'U1', H('2026-09-01T10:00:00', { shop_code: '5' }), [L('1', { sku_code: 'sc-a', qty: 4, cancelled_qty: 1 })]);
  await apply('mercari', 'main', 'ME1', H('2026-10-01T10:00:00', { shop_code: '8', amount_source: 'ne', source_system: 'ne' }), [L('1', { sku_code: 'sc-a', qty: 1, line_amount_jpy: null, amount_source: 'ne' })]);
  await apply('rakuten', 'main', 'R3', H('2024-12-20T10:00:00'), [L('1', { sku_code: 'sc-a', qty: 9 })]);
  await apply('rakuten', 'main', 'R5', H('2026-10-18T09:00:00'), [L('1', { sku_code: 'sc-a', qty: 7 })]);
  // 在庫 (ロジザードの日次): 持ち主のロールで取込 → 日の締め
  await pg.query('set role deploy');
  for (const [d, a] of [['2026-10-13', 5], ['2026-10-14', 0], ['2026-10-16', 3]]) {
    const rows = a > 0 ? [lzRow('sc-a', a), lzRow('zz-other', 1)] : [lzRow('zz-other', 1)];   // 0 個 = その日の倉庫に無い (行が無い)
    await captureLogizardInventory(db, { rows, capturedAt: `${d}T09:00:00.000Z`, log: quiet });
  }
  await closeStockDays(db, { todayJst: '2026-10-17', log: quiet });
  await pg.query('reset role');
  return ids;
}
