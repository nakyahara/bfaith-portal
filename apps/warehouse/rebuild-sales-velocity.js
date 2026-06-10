/**
 * f_sales_velocity_by_product 再構築（商品管理リスト用 販売速度サマリ）
 *
 * FBA / FBA以外 × 7日 / 30日 の純販売数を商品コード単位で集計する。
 *   FBA     = raw_sp_orders(fulfillment_channel='Amazon', order_status≠Cancelled)
 *             を v_sku_resolved(+商品コード直接一致) で NE商品コードへ解決し、セット展開(構成数倍)。
 *             受注日は purchase_date(UTC) を +9h で JST 日付に変換。
 *   FBA以外 = raw_ne_orders(キャンセル区分='有効'、店舗 platform≠'_ignore') を商品コード単位で集計。
 *             NE は既にセット展開済み。受注日(JST)をそのまま使用。
 *   合計    = FBA + FBA以外。
 * 期間: as_of = 前日(JST)。 7日 = as_of-6〜as_of、 30日 = as_of-29〜as_of。
 *
 * 二重計上について: NE受注に Amazon FBA は構造的に含まれない(NEのAmazon店=FBMのみ)。
 *   ne_fba_overlap 実測(2026-06-08): FBA注文ID∩NE受注番号 = 0/175,097。
 *   本スクリプトでも overlap 件数を毎回計測し、>0 なら警告(④スナップショット側で status=failed ゲート化)。
 *
 * 使い方:
 *   node apps/warehouse/rebuild-sales-velocity.js
 */
import { getDB, initDB } from './db.js';

function nowTs() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }

// 前日(JST)を YYYY-MM-DD で返す + n日前
function jstDateStr(offsetDays = 0) {
  const jstMidnightUtcMs = (() => {
    const j = new Date(Date.now() + 9 * 3600 * 1000);
    return Date.UTC(j.getUTCFullYear(), j.getUTCMonth(), j.getUTCDate());
  })();
  const d = new Date(jstMidnightUtcMs + offsetDays * 86400000);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

export async function rebuildSalesVelocity() {
  const db = getDB();
  const ts = nowTs();
  const asOf = jstDateStr(-1);        // 前日(JST)
  const cutoff7 = jstDateStr(-7);     // as_of-6 の前日 = -7 (inclusive 比較は > cutoff7)
  const cutoff30 = jstDateStr(-30);   // as_of-29 の前日 = -30
  // 比較は cutoffN < d <= asOf （= 直近N日、前日まで）

  console.log(`[velocity] 再構築開始 as_of=${asOf} (7d>${cutoff7}, 30d>${cutoff30})`);
  const t0 = Date.now();

  let setComponentsRows, skuMapRows, productMpRows, fbaRows, neRows;
  const readTx = db.transaction(() => {
    setComponentsRows = db.prepare('SELECT セット商品コード, 構成商品コード, 数量 FROM m_set_components').all();
    skuMapRows = db.prepare('SELECT seller_sku, ne_code, 数量 FROM v_sku_resolved').all();
    productMpRows = db.prepare('SELECT 商品コード, 商品区分 FROM m_products').all();

    // FBA: Amazon受注(FBAのみ) を 日付(JST)×seller_sku で集計
    fbaRows = db.prepare(`
      SELECT date(purchase_date, '+9 hours') AS d, LOWER(seller_sku) AS sku, SUM(quantity) AS qty
      FROM raw_sp_orders
      WHERE fulfillment_channel = 'Amazon'
        AND order_status NOT IN ('Cancelled')
        AND date(purchase_date, '+9 hours') > ? AND date(purchase_date, '+9 hours') <= ?
      GROUP BY d, sku
    `).all(cutoff30, asOf);

    // FBA以外: NE受注(有効・_ignore除外) を 日付×商品コード で集計
    neRows = db.prepare(`
      SELECT SUBSTR(o.受注日, 1, 10) AS d, LOWER(o.商品コード) AS code, SUM(o.受注数) AS qty
      FROM raw_ne_orders o
      LEFT JOIN shops s ON o.店舗コード = s.shop_code
      WHERE o.キャンセル区分 = '有効'
        AND COALESCE(s.platform, '') <> '_ignore'
        AND SUBSTR(o.受注日, 1, 10) > ? AND SUBSTR(o.受注日, 1, 10) <= ?
      GROUP BY d, code
    `).all(cutoff30, asOf);
  });
  readTx();

  // ─── マスタ Map ───
  const setComponents = new Map();
  for (const r of setComponentsRows) {
    if (!setComponents.has(r.セット商品コード)) setComponents.set(r.セット商品コード, []);
    setComponents.get(r.セット商品コード).push({ code: r.構成商品コード, qty: r.数量 || 1 });
  }
  const skuMap = new Map();
  for (const r of skuMapRows) {
    const k = r.seller_sku?.toLowerCase();
    if (!k) continue;
    if (!skuMap.has(k)) skuMap.set(k, []);
    skuMap.get(k).push({ ne_code: r.ne_code, qty: r.数量 || 1 });
  }
  const productTypes = new Map();
  for (const r of productMpRows) productTypes.set(r.商品コード, r.商品区分);

  function resolveAmazonSku(sku) {
    const mapped = skuMap.get(sku);
    if (mapped) return mapped;
    if (productTypes.has(sku)) return [{ ne_code: sku, qty: 1 }];
    return null; // unmapped
  }

  // 集計バケツ: 商品コード → {f7,n7,f30,n30}
  const acc = new Map();
  function bucket(code) {
    let e = acc.get(code);
    if (!e) { e = { f7: 0, n7: 0, f30: 0, n30: 0 }; acc.set(code, e); }
    return e;
  }
  function addFba(code, qty, d) {
    const e = bucket(code);
    e.f30 += qty;
    if (d > cutoff7) e.f7 += qty;
  }
  function addNonFba(code, qty, d) {
    const e = bucket(code);
    e.n30 += qty;
    if (d > cutoff7) e.n7 += qty;
  }
  // セット展開して FBA 側に加算
  function expandFba(neCode, qty, d) {
    const type = productTypes.get(neCode);
    const comps = setComponents.get(neCode);
    if (type === 'セット' && comps && comps.length > 0) {
      for (const c of comps) addFba(c.code, qty * (c.qty || 1), d);
    } else {
      addFba(neCode, qty, d);
    }
  }

  // FBA 集計
  let unmappedFba = 0;
  const unmappedSkus = new Set();
  for (const row of fbaRows) {
    const mappings = resolveAmazonSku(row.sku);
    if (!mappings) { unmappedFba += row.qty; unmappedSkus.add(row.sku); continue; }
    for (const m of mappings) expandFba(m.ne_code, row.qty * (m.qty || 1), row.d);
  }
  // FBA以外 集計（NEは展開済み）
  for (const row of neRows) addNonFba(row.code, row.qty, row.d);

  // ─── 書き込み（原子的 DELETE+INSERT）───
  const insert = db.prepare(`
    INSERT INTO f_sales_velocity_by_product
      (商品コード, qty_7d_fba, qty_7d_nonfba, qty_7d_total, qty_30d_fba, qty_30d_nonfba, qty_30d_total, as_of_date, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const writeTx = db.transaction(() => {
    db.exec('DELETE FROM f_sales_velocity_by_product');
    for (const [code, e] of acc) {
      insert.run(code, e.f7, e.n7, e.f7 + e.n7, e.f30, e.n30, e.f30 + e.n30, asOf, ts);
    }
  });
  writeTx();
  try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

  // ─── ne_fba_overlap 計測（参考・DQ用）───
  let overlapFba = 0;
  try {
    const fbaIds = db.prepare(`SELECT DISTINCT amazon_order_id FROM raw_sp_orders WHERE fulfillment_channel='Amazon' AND order_status NOT IN ('Cancelled') AND date(purchase_date,'+9 hours') > ? AND date(purchase_date,'+9 hours') <= ?`).all(cutoff30, asOf).map(r => r.amazon_order_id);
    if (fbaIds.length) {
      // NE側は本集計と同条件(有効・_ignore除外・同じ窓)に揃える。
      // = 「FBA以外として実際に計上される NE受注」に FBA注文IDが混入していないかを検査する。
      const neSet = new Set(db.prepare(`
        SELECT DISTINCT o.受注番号
        FROM raw_ne_orders o LEFT JOIN shops s ON o.店舗コード = s.shop_code
        WHERE o.キャンセル区分 = '有効'
          AND COALESCE(s.platform, '') <> '_ignore'
          AND SUBSTR(o.受注日, 1, 10) > ? AND SUBSTR(o.受注日, 1, 10) <= ?
      `).all(cutoff30, asOf).map(r => r.受注番号));
      for (const id of fbaIds) if (neSet.has(id)) overlapFba++;
    }
  } catch (e) { console.error('[velocity] overlap計測スキップ:', e.message); }

  const t1 = Date.now();
  const total = acc.size;
  console.log(`[velocity] ✅ 完了 (${((t1 - t0) / 1000).toFixed(1)}秒): 商品=${total}件, FBA未解決=${unmappedFba}個(${unmappedSkus.size}SKU), ne_fba_overlap=${overlapFba}`);
  if (overlapFba > 0) console.log(`[velocity] ⚠️ ne_fba_overlap=${overlapFba} > 0: NEにFBA混入の疑い。④スナップショットは status=failed にすべき`);

  return {
    ok: true, as_of: asOf, products: total,
    unmapped_fba_qty: unmappedFba, unmapped_fba_skus: unmappedSkus.size,
    ne_fba_overlap: overlapFba,
  };
}

// ─── 単体実行 ───
const isMain = process.argv[1]?.includes('rebuild-sales-velocity');
if (isMain) {
  await initDB();
  const r = await rebuildSalesVelocity();
  console.log('\n結果:', JSON.stringify(r, null, 2));
  process.exit(0);
}
