/**
 * snapshot-inventory-aggregate.js — 日次在庫スナップショットの集計
 *
 * PR-A (ne_stock_daily_snapshot) + PR-0 (fba.db.daily_snapshots) のデータを
 * SKU解決 (v_sku_resolved) + 原価 (m_products) で金額化し、inv_daily_summary に書く。
 *
 * カテゴリ:
 *   fba_warehouse = fba_available + fba_fc_transfer + fba_fc_processing + fba_customer_order
 *                   (月末ツールと同じ4カラム合算定義)
 *   fba_inbound   = fba_inbound_working + fba_inbound_shipped + fba_inbound_received
 *   own_warehouse = ne_stock_daily_snapshot.在庫数 (NE 商品コード単位、セット展開なし)
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
  const market = 'jp';
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

  const result = {};
  try {
    // ─── 1. own_warehouse (自社倉庫: ne_stock_daily_snapshot) ───
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
    upsert.run(businessDate, market, 'own_warehouse', ownQty, ownStatus === 'no_source' ? null : ownValue, ownResolved, 0, ownCostMissing, ownStatus, ownSrc.length, captured);
    result.own_warehouse = { qty: ownQty, value: ownValue, resolved: ownResolved, costMissing: ownCostMissing, status: ownStatus, rowCount: ownSrc.length };

    // ─── 2/3. FBA: fba.db ATTACH 経由で daily_snapshots を読む ───
    if (!fbaAttached) {
      // fba.db が読めなければ FBA 系は no_source として記録
      for (const cat of ['fba_warehouse', 'fba_inbound']) {
        upsert.run(businessDate, market, cat, 0, null, 0, 0, 0, 'no_source', 0, captured);
        result[cat] = { qty: 0, value: 0, resolved: 0, costMissing: 0, status: 'no_source', rowCount: 0 };
      }
      return result;
    }

    // 当日分の daily_snapshots を取得 (Amazon SKU 単位、月末ツールと同じ fba_warehouse 4列合算)
    const fbaSrc = db.prepare(`
      SELECT amazon_sku,
             COALESCE(fba_available,0) + COALESCE(fba_fc_transfer,0) + COALESCE(fba_fc_processing,0) + COALESCE(fba_customer_order,0) AS qty_warehouse,
             COALESCE(fba_inbound_working,0) + COALESCE(fba_inbound_shipped,0) + COALESCE(fba_inbound_received,0) AS qty_inbound
      FROM fba.daily_snapshots
      WHERE snapshot_date = ?
    `).all(businessDate);

    if (fbaSrc.length === 0) {
      for (const cat of ['fba_warehouse', 'fba_inbound']) {
        upsert.run(businessDate, market, cat, 0, null, 0, 0, 0, 'no_source', 0, captured);
        result[cat] = { qty: 0, value: 0, resolved: 0, costMissing: 0, status: 'no_source', rowCount: 0 };
      }
      return result;
    }

    // Amazon SKU → ne_code 展開 (v_sku_resolved 経由で master 優先 + sku_map fallback)
    // Map<ne_code, 数量> に展開した上で原価×数量を計算
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
          // SKU 解決できず (master/sku_map 両方になし)
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

    const wh = aggregateFbaCategory(fbaSrc, 'qty_warehouse');
    const ib = aggregateFbaCategory(fbaSrc, 'qty_inbound');

    function statusOf(s) {
      return (s.unresolved + s.costMissing > 0) ? 'partial' : 'ok';
    }

    upsert.run(businessDate, market, 'fba_warehouse', wh.totalQty, wh.totalValue, wh.resolved, wh.unresolved, wh.costMissing, statusOf(wh), fbaSrc.length, captured);
    upsert.run(businessDate, market, 'fba_inbound',   ib.totalQty, ib.totalValue, ib.resolved, ib.unresolved, ib.costMissing, statusOf(ib), fbaSrc.length, captured);
    result.fba_warehouse = { qty: wh.totalQty, value: wh.totalValue, resolved: wh.resolved, unresolved: wh.unresolved, costMissing: wh.costMissing, status: statusOf(wh), rowCount: fbaSrc.length };
    result.fba_inbound   = { qty: ib.totalQty, value: ib.totalValue, resolved: ib.resolved, unresolved: ib.unresolved, costMissing: ib.costMissing, status: statusOf(ib), rowCount: fbaSrc.length };

    return result;
  } finally {
    if (fbaAttached) detachFbaDb(db);
  }
}

// ─── CLI ───
const isMain = process.argv[1]?.endsWith('snapshot-inventory-aggregate.js');
if (isMain) {
  await initDB();
  const businessDate = resolveBusinessDate();
  console.log(`[inv-agg] business_date=${businessDate} 開始`);
  const t0 = Date.now();
  const result = aggregateInventorySnapshot(businessDate);
  const elapsed = Date.now() - t0;
  console.log(`[inv-agg] ✅ 完了 (${elapsed}ms):`);
  for (const [cat, s] of Object.entries(result)) {
    console.log(`  ${cat}: qty=${s.qty} value=${Math.round(s.value)} status=${s.status} (resolved=${s.resolved}/unresolved=${s.unresolved||0}/cost_missing=${s.costMissing||0}, src=${s.rowCount})`);
  }
  process.exit(0);
}
