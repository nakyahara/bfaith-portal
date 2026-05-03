/**
 * snapshot-inventory-aggregate.js — 日次在庫スナップショットの集計
 *
 * PR-A (ne_stock_daily_snapshot) + PR-0 (fba.db.daily_snapshots / daily_snapshots_us) のデータを
 * SKU解決 (v_sku_resolved) + 原価 (m_products) で金額化し、inv_daily_summary に書く。
 *
 * カテゴリ (market='jp'):
 *   fba_warehouse = fba_available + fba_fc_transfer + fba_fc_processing + fba_customer_order
 *                   (月末ツールと同じ4カラム合算定義)
 *   fba_inbound   = fba_inbound_working + fba_inbound_shipped + fba_inbound_received
 *   own_warehouse = ne_stock_daily_snapshot.在庫数 (NE 商品コード単位、セット展開なし)
 *
 * カテゴリ (market='us'):
 *   fba_us_warehouse = JP と同じ4カラム合算 (daily_snapshots_us 由来)
 *   fba_us_inbound   = JP と同じ3カラム合算 (daily_snapshots_us 由来)
 *   ※ Phase 1 は JPY 原価ベース管理 (m_products.原価) のため為替変換なし
 *
 * 完全性指標:
 *   source_status: 'ok' | 'partial' | 'failed' | 'no_source'
 *     - ok: 当日のソースデータが存在し、ほぼ全行原価解決できた
 *     - partial: ソースはあるが unresolved_count > 0 (master/sku_map にない SKU)
 *     - failed: ソースデータがあったが集計時例外 (このスクリプトでは "no_source" になることはあっても failed は呼出元で設定)
 *     - no_source: 当日の SP-API 取得が走ってない (daily_snapshots に当日行なし)
 *
 * daily-sync.js から PR-0 (snapshot-fba-stock.js) の後に呼ばれる。
 * 単独実行可: node apps/warehouse/snapshot-inventory-aggregate.js [--date=YYYY-MM-DD]
 */
import crypto from 'crypto';
import { initDB, getDB } from './db.js';

function toJstDate(d) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function resolveBusinessDate() {
  if (process.env.WAREHOUSE_BUSINESS_DATE) return process.env.WAREHOUSE_BUSINESS_DATE;
  const cliArg = process.argv.slice(2).find(a => a.startsWith('--date='));
  if (cliArg) return cliArg.split('=')[1];
  return toJstDate(new Date());
}

/**
 * fba.db (別 DB) を ATTACH して daily_snapshots を読む。
 * fba.db のパスは process.env.FBA_DB_PATH もしくはデフォルト data/fba.db
 */
function attachFbaDb(db) {
  const path = process.env.FBA_DB_PATH || 'data/fba.db';
  try {
    db.exec(`ATTACH DATABASE '${path.replace(/'/g, "''")}' AS fba`);
    return true;
  } catch (e) {
    console.warn(`[inv-agg] fba.db ATTACH 失敗 (${path}): ${e.message}`);
    return false;
  }
}

function detachFbaDb(db) {
  try { db.exec('DETACH DATABASE fba'); } catch {}
}

/**
 * カテゴリ別に集計を実行して inv_daily_summary に upsert
 *
 * 戻り値: { fba_warehouse, fba_inbound, own_warehouse } のサマリ
 */
export function aggregateInventorySnapshot(businessDate) {
  if (!businessDate || !/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) {
    throw new Error(`business_date が不正: ${businessDate}`);
  }
  const db = getDB();
  const captured = new Date().toISOString();

  const upsert = db.prepare(`
    INSERT INTO inv_daily_summary
      (business_date, market, category, total_qty, total_value,
       resolved_count, unresolved_count, cost_missing_count,
       source_status, source_row_count, captured_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(business_date, market, category) DO UPDATE SET
      total_qty = excluded.total_qty,
      total_value = excluded.total_value,
      resolved_count = excluded.resolved_count,
      unresolved_count = excluded.unresolved_count,
      cost_missing_count = excluded.cost_missing_count,
      source_status = excluded.source_status,
      source_row_count = excluded.source_row_count,
      captured_at = excluded.captured_at
  `);

  const fbaAttached = attachFbaDb(db);

  // 共通: SKU解決 + 原価
  const resolveStmt = db.prepare(`SELECT ne_code, 数量 FROM v_sku_resolved WHERE seller_sku = ?`);
  const costStmt = db.prepare(`SELECT 原価 FROM m_products WHERE 商品コード = ?`);

  function aggregateFbaCategory(rows, qtyKey) {
    let totalQty = 0, totalValue = 0, resolved = 0, unresolved = 0, costMissing = 0;
    for (const row of rows) {
      const baseQty = row[qtyKey];
      if (baseQty <= 0) continue;
      const sku = (row.amazon_sku || '').toLowerCase();
      const components = resolveStmt.all(sku);
      if (components.length === 0) {
        totalQty += baseQty;
        unresolved++;
        continue;
      }
      for (const c of components) {
        const lineQty = baseQty * (c.数量 || 1);
        totalQty += lineQty;
        const cost = costStmt.get(c.ne_code)?.原価;
        if (cost != null) {
          totalValue += lineQty * cost;
          resolved++;
        } else {
          costMissing++;
        }
      }
    }
    return { totalQty, totalValue, resolved, unresolved, costMissing };
  }

  function statusOf(s) {
    return (s.unresolved + s.costMissing > 0) ? 'partial' : 'ok';
  }

  // 指定 (table, market, warehouseCategory, inboundCategory) で FBA 集計1セット実行
  function processFbaMarket({ table, market, warehouseCategory, inboundCategory }) {
    const sectionResult = {};
    if (!fbaAttached) {
      for (const cat of [warehouseCategory, inboundCategory]) {
        upsert.run(businessDate, market, cat, 0, null, 0, 0, 0, 'no_source', 0, captured);
        sectionResult[cat] = { qty: 0, value: 0, resolved: 0, costMissing: 0, status: 'no_source', rowCount: 0 };
      }
      return sectionResult;
    }
    const fbaSrc = db.prepare(`
      SELECT amazon_sku,
             COALESCE(fba_available,0) + COALESCE(fba_fc_transfer,0) + COALESCE(fba_fc_processing,0) + COALESCE(fba_customer_order,0) AS qty_warehouse,
             COALESCE(fba_inbound_working,0) + COALESCE(fba_inbound_shipped,0) + COALESCE(fba_inbound_received,0) AS qty_inbound
      FROM fba.${table}
      WHERE snapshot_date = ?
    `).all(businessDate);

    if (fbaSrc.length === 0) {
      for (const cat of [warehouseCategory, inboundCategory]) {
        upsert.run(businessDate, market, cat, 0, null, 0, 0, 0, 'no_source', 0, captured);
        sectionResult[cat] = { qty: 0, value: 0, resolved: 0, costMissing: 0, status: 'no_source', rowCount: 0 };
      }
      return sectionResult;
    }

    const wh = aggregateFbaCategory(fbaSrc, 'qty_warehouse');
    const ib = aggregateFbaCategory(fbaSrc, 'qty_inbound');
    upsert.run(businessDate, market, warehouseCategory, wh.totalQty, wh.totalValue, wh.resolved, wh.unresolved, wh.costMissing, statusOf(wh), fbaSrc.length, captured);
    upsert.run(businessDate, market, inboundCategory,   ib.totalQty, ib.totalValue, ib.resolved, ib.unresolved, ib.costMissing, statusOf(ib), fbaSrc.length, captured);
    sectionResult[warehouseCategory] = { qty: wh.totalQty, value: wh.totalValue, resolved: wh.resolved, unresolved: wh.unresolved, costMissing: wh.costMissing, status: statusOf(wh), rowCount: fbaSrc.length };
    sectionResult[inboundCategory]   = { qty: ib.totalQty, value: ib.totalValue, resolved: ib.resolved, unresolved: ib.unresolved, costMissing: ib.costMissing, status: statusOf(ib), rowCount: fbaSrc.length };
    return sectionResult;
  }

  const result = {};
  try {
    // ─── 1. own_warehouse (自社倉庫: ne_stock_daily_snapshot, market='jp') ───
    // ne_code 単位で 数量 × 原価 (m_products.原価) を集計。セット展開なし。
    const ownSrc = db.prepare(`
      SELECT
        n.商品コード AS ne_code,
        n.在庫数,
        p.原価
      FROM ne_stock_daily_snapshot n
      LEFT JOIN m_products p ON n.商品コード = p.商品コード
      WHERE n.business_date = ? AND n.在庫数 > 0
    `).all(businessDate);

    let ownQty = 0, ownValue = 0, ownResolved = 0, ownCostMissing = 0;
    for (const r of ownSrc) {
      ownQty += r.在庫数;
      if (r.原価 != null) {
        ownValue += r.在庫数 * r.原価;
        ownResolved++;
      } else {
        ownCostMissing++;
      }
    }
    const ownStatus = ownSrc.length === 0 ? 'no_source' : (ownCostMissing > 0 ? 'partial' : 'ok');
    upsert.run(businessDate, 'jp', 'own_warehouse', ownQty, ownStatus === 'no_source' ? null : ownValue, ownResolved, 0, ownCostMissing, ownStatus, ownSrc.length, captured);
    result.own_warehouse = { qty: ownQty, value: ownValue, resolved: ownResolved, costMissing: ownCostMissing, status: ownStatus, rowCount: ownSrc.length };

    // ─── 2/3. FBA JP (daily_snapshots) ───
    Object.assign(result, processFbaMarket({
      table: 'daily_snapshots',
      market: 'jp',
      warehouseCategory: 'fba_warehouse',
      inboundCategory: 'fba_inbound',
    }));

    // ─── 4/5. FBA US (daily_snapshots_us, market='us') ───
    // テーブル未作成 (旧バージョン環境) でも落ちないように try
    try {
      Object.assign(result, processFbaMarket({
        table: 'daily_snapshots_us',
        market: 'us',
        warehouseCategory: 'fba_us_warehouse',
        inboundCategory: 'fba_us_inbound',
      }));
    } catch (e) {
      console.warn('[inv-agg] US FBA 集計スキップ:', e.message);
    }

    return result;
  } finally {
    if (fbaAttached) detachFbaDb(db);
  }
}

/**
 * inv_daily_detail を1日分 UPSERT で書き込む。
 * - own_warehouse: ne_stock_daily_snapshot を ne_code 単位で展開
 * - fba_warehouse / fba_inbound: fba.db.daily_snapshots → v_sku_resolved で展開
 * 戻り値: { snapshotRunId, status, totalRows, totalValue, costMissing, neMissing }
 */
export function aggregateInventoryDetail(businessDate) {
  if (!businessDate || !/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) {
    throw new Error(`business_date が不正: ${businessDate}`);
  }
  const db = getDB();
  const snapshotRunId = crypto.randomUUID();
  const startedAt = new Date().toISOString();

  // run_log に running を先に記録
  db.prepare(`INSERT INTO inv_daily_run_log (snapshot_run_id, business_date, started_at, status) VALUES (?, ?, ?, 'running')`)
    .run(snapshotRunId, businessDate, startedAt);

  let status = 'ok';
  let errorMessage = null;
  let totalRows = 0, totalValue = 0, costMissing = 0, neMissing = 0;

  const fbaAttached = attachFbaDb(db);

  try {
    // ─── 1. マスタ系をメモリに事前ロード ───
    const mProducts = new Map(); // ne_code → { ... }
    for (const r of db.prepare(`SELECT 商品コード, 商品名, 商品区分, 取扱区分, 売上分類, 原価, 仕入先コード, seasonality_flag, season_months, new_product_flag, new_product_launch_date FROM m_products`).all()) {
      mProducts.set(r.商品コード, r);
    }
    // 注: ロケーションコードは取得しない (WMS が正、NE側のは古い/不正確)
    const rawNe = new Map(); // ne_code → { ... }
    for (const r of db.prepare(`SELECT 商品コード, 引当数, 発注残数, 最終仕入日, 代表商品コード, 発注ロット単位 FROM raw_ne_products`).all()) {
      rawNe.set(r.商品コード, r);
    }

    // SKU解決マップ (seller_sku → [{ne_code, qty}])
    const resolveMap = new Map();
    for (const r of db.prepare(`SELECT seller_sku, ne_code, 数量 FROM v_sku_resolved`).all()) {
      const k = r.seller_sku;
      if (!resolveMap.has(k)) resolveMap.set(k, []);
      resolveMap.get(k).push({ ne_code: r.ne_code, qty: r.数量 || 1 });
    }

    // 売上履歴を ne_code 単位で集約 (直近90日、business_date 未満)
    // last_sold_date / sales_7/30/90d_qty / value
    const salesAgg = new Map();
    const salesRows = db.prepare(`
      SELECT 商品コード AS ne_code,
             MAX(日付) AS last_sold_date,
             SUM(CASE WHEN 日付 >= date(?, '-7 days')  THEN 数量    ELSE 0 END) AS s7q,
             SUM(CASE WHEN 日付 >= date(?, '-30 days') THEN 数量    ELSE 0 END) AS s30q,
             SUM(CASE WHEN 日付 >= date(?, '-90 days') THEN 数量    ELSE 0 END) AS s90q,
             SUM(CASE WHEN 日付 >= date(?, '-7 days')  THEN 売上金額 ELSE 0 END) AS s7v,
             SUM(CASE WHEN 日付 >= date(?, '-30 days') THEN 売上金額 ELSE 0 END) AS s30v,
             SUM(CASE WHEN 日付 >= date(?, '-90 days') THEN 売上金額 ELSE 0 END) AS s90v
      FROM f_sales_by_product
      WHERE 日付 >= date(?, '-90 days') AND 日付 < ?
      GROUP BY 商品コード
    `).all(businessDate, businessDate, businessDate, businessDate, businessDate, businessDate, businessDate, businessDate);
    for (const r of salesRows) salesAgg.set(r.ne_code, r);

    // ─── 2. detail 行を生成 ───
    const detailRows = [];

    function buildRow(category, market, sourceSystem, sourceItemCode, neCode, qty, opts = {}) {
      if (qty == null || qty === 0) return null;
      const m = mProducts.get(neCode);
      const ne = rawNe.get(neCode);
      const s = salesAgg.get(neCode);
      const unitCost = m?.原価 ?? null;
      let costStatus = 'ok';
      let costSource = 'm_products';
      if (!m) {
        costStatus = 'ne_missing';
        costSource = 'missing';
        neMissing++;
      } else if (unitCost == null) {
        costStatus = 'cost_missing';
        costSource = 'missing';
        costMissing++;
      }
      const totalVal = (unitCost != null) ? qty * unitCost : null;
      if (totalVal != null) totalValue += totalVal;

      return {
        business_date: businessDate,
        market,
        category,
        source_system: sourceSystem,
        source_item_code: sourceItemCode,
        ne_code: neCode,
        qty,
        unit_cost: unitCost,
        total_value: totalVal,
        cost_status: costStatus,
        cost_source: costSource,
        resolution_method: opts.resolution_method || null,
        is_bundle_expanded: opts.is_bundle_expanded ? 1 : 0,
        component_qty: opts.component_qty ?? null,
        product_name: m?.商品名 ?? null,
        source_product_name: opts.source_product_name ?? null,
        supplier_code: m?.仕入先コード ?? null,
        product_type: m?.商品区分 ?? null,
        handling_class: m?.取扱区分 ?? null,
        sales_class: m?.売上分類 ?? null,
        representative_product_code: ne?.代表商品コード ?? null,
        order_lot_size: ne?.発注ロット単位 ?? null,
        seasonality_flag: m?.seasonality_flag ?? null,
        season_months: m?.season_months ?? null,
        new_product_flag: m?.new_product_flag ?? null,
        new_product_launch_date: m?.new_product_launch_date ?? null,
        last_sold_date: s?.last_sold_date ?? null,
        sales_7d_qty: s?.s7q ?? null,
        sales_30d_qty: s?.s30q ?? null,
        sales_90d_qty: s?.s90q ?? null,
        sales_7d_value: s?.s7v ?? null,
        sales_30d_value: s?.s30v ?? null,
        sales_90d_value: s?.s90v ?? null,
        working_first_seen: opts.working_first_seen ?? null,
        fba_unfulfillable_qty: opts.fba_unfulfillable_qty ?? null,
        reserved_qty: (category === 'own_warehouse') ? (ne?.引当数 ?? null) : null,
        pending_order_qty: (category === 'own_warehouse') ? (ne?.発注残数 ?? null) : null,
        location_code: null, // WMS が source of truth、NE 側のは不正確なので保存しない
        last_purchase_date: (category === 'own_warehouse') ? (ne?.最終仕入日 ?? null) : null,
        snapshot_run_id: snapshotRunId,
      };
    }

    // 2a. own_warehouse (market='jp')
    const ownSrc = db.prepare(`SELECT 商品コード, 在庫数 FROM ne_stock_daily_snapshot WHERE business_date = ? AND 在庫数 > 0`).all(businessDate);
    for (const r of ownSrc) {
      const row = buildRow('own_warehouse', 'jp', 'ne', r.商品コード, r.商品コード, r.在庫数, { resolution_method: 'direct' });
      if (row) detailRows.push(row);
    }

    // 2b. FBA helper: 指定 market/table/category 名で1セット展開
    function processFbaDetail({ table, market, warehouseCategory, inboundCategory }) {
      const exists = db.prepare(`SELECT name FROM fba.sqlite_master WHERE type='table' AND name=?`).get(table);
      if (!exists) return; // 旧環境などでテーブル無し
      const fbaCols = db.prepare(`PRAGMA fba.table_info(${table})`).all().map(c => c.name);
      const hasUnfulfillable = fbaCols.includes('fba_unfulfillable');
      const hasWorkingFirstSeen = fbaCols.includes('working_first_seen');
      const unfulfillableExpr = hasUnfulfillable ? 'fba_unfulfillable' : 'NULL AS fba_unfulfillable';
      const workingExpr = hasWorkingFirstSeen ? 'working_first_seen' : 'NULL AS working_first_seen';
      const fbaSrc = db.prepare(`
        SELECT amazon_sku, product_name,
               COALESCE(fba_available,0) + COALESCE(fba_fc_transfer,0) + COALESCE(fba_fc_processing,0) + COALESCE(fba_customer_order,0) AS qty_warehouse,
               COALESCE(fba_inbound_working,0) + COALESCE(fba_inbound_shipped,0) + COALESCE(fba_inbound_received,0) AS qty_inbound,
               ${workingExpr}, ${unfulfillableExpr}
        FROM fba.${table}
        WHERE snapshot_date = ?
      `).all(businessDate);

      for (const fba of fbaSrc) {
        const sku = (fba.amazon_sku || '').toLowerCase();
        const components = resolveMap.get(sku) || [];
        const isBundle = components.length > 1;
        const resolution = components.length === 0 ? 'unresolved' : (isBundle ? 'master' : 'sku_map'); // 簡易: 1件なら sku_map 多い

        for (const cat of [
          { name: warehouseCategory, qty: fba.qty_warehouse, working_first_seen: null, unfulfillable: fba.fba_unfulfillable },
          { name: inboundCategory,   qty: fba.qty_inbound,   working_first_seen: fba.working_first_seen, unfulfillable: null },
        ]) {
          if (cat.qty <= 0) continue;
          if (components.length === 0) {
            // 未解決: source_item_code = seller_sku, ne_code = seller_sku (PKを満たすため)
            const row = buildRow(cat.name, market, 'fba', sku, sku, cat.qty, {
              resolution_method: 'unresolved',
              source_product_name: fba.product_name,
              working_first_seen: cat.working_first_seen,
              fba_unfulfillable_qty: cat.unfulfillable,
            });
            if (row) detailRows.push(row);
          } else {
            for (const c of components) {
              const row = buildRow(cat.name, market, 'fba', sku, c.ne_code, cat.qty * c.qty, {
                resolution_method: resolution,
                is_bundle_expanded: isBundle,
                component_qty: isBundle ? c.qty : null,
                source_product_name: fba.product_name,
                working_first_seen: cat.working_first_seen,
                fba_unfulfillable_qty: cat.unfulfillable,
              });
              if (row) detailRows.push(row);
            }
          }
        }
      }
    }

    if (fbaAttached) {
      processFbaDetail({ table: 'daily_snapshots',    market: 'jp', warehouseCategory: 'fba_warehouse',    inboundCategory: 'fba_inbound' });
      try {
        processFbaDetail({ table: 'daily_snapshots_us', market: 'us', warehouseCategory: 'fba_us_warehouse', inboundCategory: 'fba_us_inbound' });
      } catch (e) {
        console.warn('[inv-agg] US FBA detail スキップ:', e.message);
      }
    }

    totalRows = detailRows.length;

    // ─── 3. UPSERT (1 トランザクション) ───
    const upsertStmt = db.prepare(`
      INSERT INTO inv_daily_detail (
        business_date, market, category, source_system, source_item_code, ne_code,
        qty, unit_cost, total_value, cost_status, cost_source, resolution_method,
        is_bundle_expanded, component_qty,
        product_name, source_product_name, supplier_code, product_type, handling_class,
        sales_class, representative_product_code, order_lot_size,
        seasonality_flag, season_months, new_product_flag, new_product_launch_date,
        last_sold_date, sales_7d_qty, sales_30d_qty, sales_90d_qty,
        sales_7d_value, sales_30d_value, sales_90d_value,
        working_first_seen, fba_unfulfillable_qty,
        reserved_qty, pending_order_qty, location_code, last_purchase_date,
        snapshot_run_id
      ) VALUES (
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?,
        ?
      )
      ON CONFLICT(business_date, market, category, source_system, source_item_code, ne_code) DO UPDATE SET
        qty = excluded.qty,
        unit_cost = excluded.unit_cost,
        total_value = excluded.total_value,
        cost_status = excluded.cost_status,
        cost_source = excluded.cost_source,
        resolution_method = excluded.resolution_method,
        is_bundle_expanded = excluded.is_bundle_expanded,
        component_qty = excluded.component_qty,
        product_name = excluded.product_name,
        source_product_name = excluded.source_product_name,
        supplier_code = excluded.supplier_code,
        product_type = excluded.product_type,
        handling_class = excluded.handling_class,
        sales_class = excluded.sales_class,
        representative_product_code = excluded.representative_product_code,
        order_lot_size = excluded.order_lot_size,
        seasonality_flag = excluded.seasonality_flag,
        season_months = excluded.season_months,
        new_product_flag = excluded.new_product_flag,
        new_product_launch_date = excluded.new_product_launch_date,
        last_sold_date = excluded.last_sold_date,
        sales_7d_qty = excluded.sales_7d_qty,
        sales_30d_qty = excluded.sales_30d_qty,
        sales_90d_qty = excluded.sales_90d_qty,
        sales_7d_value = excluded.sales_7d_value,
        sales_30d_value = excluded.sales_30d_value,
        sales_90d_value = excluded.sales_90d_value,
        working_first_seen = excluded.working_first_seen,
        fba_unfulfillable_qty = excluded.fba_unfulfillable_qty,
        reserved_qty = excluded.reserved_qty,
        pending_order_qty = excluded.pending_order_qty,
        location_code = excluded.location_code,
        last_purchase_date = excluded.last_purchase_date,
        snapshot_run_id = excluded.snapshot_run_id,
        ingested_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `);

    const tx = db.transaction(() => {
      for (const r of detailRows) {
        upsertStmt.run(
          r.business_date, r.market, r.category, r.source_system, r.source_item_code, r.ne_code,
          r.qty, r.unit_cost, r.total_value, r.cost_status, r.cost_source, r.resolution_method,
          r.is_bundle_expanded, r.component_qty,
          r.product_name, r.source_product_name, r.supplier_code, r.product_type, r.handling_class,
          r.sales_class, r.representative_product_code, r.order_lot_size,
          r.seasonality_flag, r.season_months, r.new_product_flag, r.new_product_launch_date,
          r.last_sold_date, r.sales_7d_qty, r.sales_30d_qty, r.sales_90d_qty,
          r.sales_7d_value, r.sales_30d_value, r.sales_90d_value,
          r.working_first_seen, r.fba_unfulfillable_qty,
          r.reserved_qty, r.pending_order_qty, r.location_code, r.last_purchase_date,
          r.snapshot_run_id,
        );
      }
    });
    tx();

    // 完全性判定
    if (costMissing + neMissing > totalRows * 0.05) status = 'partial'; // 5% 超で partial
    else if (totalRows === 0) status = 'failed';
  } catch (e) {
    status = 'failed';
    errorMessage = e.message;
    throw e;
  } finally {
    if (fbaAttached) detachFbaDb(db);
    db.prepare(`UPDATE inv_daily_run_log SET finished_at=?, status=?, detail_total_rows=?, detail_total_value=?, cost_missing_count=?, ne_missing_count=?, error_message=? WHERE snapshot_run_id=?`)
      .run(new Date().toISOString(), status, totalRows, totalValue, costMissing, neMissing, errorMessage, snapshotRunId);
  }

  return { snapshotRunId, status, totalRows, totalValue, costMissing, neMissing };
}

// ─── CLI ───
const isMain = process.argv[1]?.endsWith('snapshot-inventory-aggregate.js');
if (isMain) {
  await initDB();
  const businessDate = resolveBusinessDate();
  console.log(`[inv-agg] business_date=${businessDate} 開始`);
  const t0 = Date.now();
  const result = aggregateInventorySnapshot(businessDate);
  const t1 = Date.now();
  console.log(`[inv-agg] summary 完了 (${t1 - t0}ms):`);
  for (const [cat, s] of Object.entries(result)) {
    console.log(`  ${cat}: qty=${s.qty} value=${Math.round(s.value)} status=${s.status} (resolved=${s.resolved}/unresolved=${s.unresolved||0}/cost_missing=${s.costMissing||0}, src=${s.rowCount})`);
  }
  console.log(`[inv-agg] detail 開始`);
  const detail = aggregateInventoryDetail(businessDate);
  const t2 = Date.now();
  console.log(`[inv-agg] detail 完了 (${t2 - t1}ms): run_id=${detail.snapshotRunId} rows=${detail.totalRows} value=${Math.round(detail.totalValue)} cost_missing=${detail.costMissing} ne_missing=${detail.neMissing} status=${detail.status}`);
  process.exit(0);
}
