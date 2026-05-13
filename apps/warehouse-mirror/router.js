/**
 * warehouse-mirror API
 *
 * ミニPCからデータを受信し、mirrorテーブルに格納。
 * ツール用のデータ参照APIも提供。
 *
 * エンドポイント:
 *   POST /api/sync          — ミニPCからデータ受信（APIキー認証）
 *   GET  /api/products      — mirror_products 検索
 *   GET  /api/sales/monthly — mirror_sales_monthly 検索
 *   GET  /api/sales/daily   — mirror_sales_daily 検索
 *   GET  /api/status        — 同期状態
 */
import { Router } from 'express';
import { initMirrorDB, getMirrorDB } from './db.js';
import { bootStart, bootEnd, bootFail } from '../observability/boot-log.js';

const router = Router();

// DB初期化
let dbReady = false;
bootStart('mirror-db', 'warehouse-mirror.db');
(async () => {
  try {
    initMirrorDB();
    dbReady = true;
    bootEnd('mirror-db', 'warehouse-mirror.db');
  } catch (e) {
    bootFail('mirror-db', 'warehouse-mirror.db', e);
    console.error('[Mirror] DB初期化失敗:', e.message);
  }
})();

function ensureDB(req, res, next) {
  if (!dbReady) return res.status(503).json({ error: 'mirror DB 未初期化' });
  next();
}

// 同期APIキー認証 (二重防御: server.js 側の requireSyncKeyStrict が一次防御)
function requireSyncKey(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) {
    // dev で明示フラグがあれば skip、そうでなければ拒否
    if (process.env.ALLOW_INSECURE_MIRROR_SYNC === '1') return next();
    return res.status(503).json({ error: 'mirror_sync_key_unset' });
  }
  const provided = req.headers['x-sync-key'] || req.query.sync_key;
  if (provided !== key) return res.status(401).json({ error: 'invalid_sync_key' });
  next();
}

router.use(ensureDB);

// ─── POST /api/sync ───
// ミニPCからデータを受信して一括反映

router.post('/api/sync', requireSyncKey, (req, res) => {
  const db = getMirrorDB();
  const { products, set_components, sales_monthly, sales_daily, meta } = req.body;
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const log = [];

  try {
    // products（全件置換）
    //   Codex PR1 review High #2 反映: seasonality_flag/season_months/new_product_flag/
    //   new_product_launch_date を列リストに含めて、ミニPC側の手動設定値を保持する。
    if (products && products.length > 0) {
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_products');
        const stmt = db.prepare(`INSERT INTO mirror_products (
          product_id, 商品コード, 商品名, 商品区分, 取扱区分,
          標準売価, 原価, 原価ソース, 原価状態,
          送料, 送料コード, 配送方法, 消費税率, 税区分,
          在庫数, 引当数, 仕入先コード, セット構成品数, 売上分類, 代表商品コード,
          seasonality_flag, season_months, new_product_flag, new_product_launch_date,
          updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const p of products) {
          stmt.run(p.product_id, p.商品コード, p.商品名, p.商品区分, p.取扱区分,
            p.標準売価, p.原価, p.原価ソース, p.原価状態,
            p.送料, p.送料コード, p.配送方法, p.消費税率, p.税区分,
            p.在庫数, p.引当数, p.仕入先コード, p.セット構成品数, p.売上分類 ?? null, p.代表商品コード ?? null,
            p.seasonality_flag ?? 0, p.season_months ?? null,
            p.new_product_flag ?? 0, p.new_product_launch_date ?? null,
            now);
        }
      });
      tx();
      log.push(`products: ${products.length}件`);
    }

    // set_components（全件置換）
    if (set_components && set_components.length > 0) {
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_set_components');
        const stmt = db.prepare(`INSERT INTO mirror_set_components (
          セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at
        ) VALUES (?,?,?,?,?,?)`);
        for (const c of set_components) {
          stmt.run(c.セット商品コード, c.構成商品コード, c.数量, c.構成商品名, c.構成商品原価, now);
        }
      });
      tx();
      log.push(`set_components: ${set_components.length}件`);
    }

    // sku_resolved（全件置換、新規ツールはこちらを参照）
    // 0件payloadも受け付ける（meta.clear_sku_resolved=trueで明示クリア可、無くても全件置換動作）
    if (req.body.sku_resolved && Array.isArray(req.body.sku_resolved)) {
      const resolved = req.body.sku_resolved;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_sku_resolved');
        const stmt = db.prepare(`INSERT INTO mirror_sku_resolved (
          seller_sku, ne_code, quantity, source, 商品名, source_updated_at, synced_at
        ) VALUES (?,?,?,?,?,?,?)`);
        for (const r of resolved) {
          stmt.run(
            r.seller_sku,
            r.ne_code,
            r.quantity ?? r.数量 ?? 1,
            r.source,
            r.商品名 ?? null,
            r.source_updated_at ?? null,
            now
          );
        }
      });
      tx();
      log.push(`sku_resolved: ${resolved.length}件`);
    }

    // sku_master (1 SKU = 1 行、Sheets 商品コード変換テーブルとの差分検出用)
    //
    // fail-closed 設計 (Codex review 2026-05-13: critical/high #1+#2):
    //   N>0 件 → 全件置換 (DELETE → INSERT)
    //   0 件 + meta.clear_sku_master=true → 明示 clear、DELETE のみ実行
    //   0 件 + flag 無し → 反映拒否 (上流異常 / worktree 別 DB / 送信側 bug の可能性)、前回の mirror 保持
    //
    // 過去事故 (2026-05-08〜10 Yahoo VPS proxy regression: 取得失敗を「空配列の正常同期」として
    // 流し mirror 3 日間全空文字) と (2026-05-06 worktree で別 DB が静かに分裂) の再発防止。
    if (req.body.sku_master !== undefined && Array.isArray(req.body.sku_master)) {
      const masterRows = req.body.sku_master;
      const explicitClear = req.body.meta?.clear_sku_master === true;
      if (masterRows.length === 0 && !explicitClear) {
        log.push(`sku_master: 0件 + clear flag 無し → 反映拒否 (前回 mirror を保持)`);
        console.warn('[Mirror] sku_master 0件 + meta.clear_sku_master 無し: 上流異常の可能性、mirror_sku_master は前回値を保持');
      } else {
        const tx = db.transaction(() => {
          db.exec('DELETE FROM mirror_sku_master');
          if (masterRows.length > 0) {
            const stmt = db.prepare(`INSERT INTO mirror_sku_master (
              seller_sku, 商品名, source_created_at, source_updated_at, synced_at
            ) VALUES (?,?,?,?,?)`);
            for (const r of masterRows) {
              stmt.run(
                r.seller_sku,
                r.商品名 ?? null,
                r.source_created_at ?? null,
                r.source_updated_at ?? null,
                now
              );
            }
          }
        });
        tx();
        log.push(`sku_master: ${masterRows.length}件${explicitClear && masterRows.length === 0 ? ' (explicit clear)' : ''}`);
      }
    }

    // inv_daily_detail (D-1c 詳細層、差分sync UPSERT + 古い行クリーン)
    // ペイロード: { inv_daily_detail: [...rows], meta: {
    //   inv_daily_detail_clear_old: true,                      // 365日より古い行 housekeeping
    //   inv_daily_detail_clear_dates: ['YYYY-MM-DD', ...],    // この sync で再投入する日付を先に DELETE
    //                                                          // (Codex R3 #2 対応: 同日再集計で消えた SKU の stale 行を残さない)
    // } }
    if (req.body.inv_daily_detail && Array.isArray(req.body.inv_daily_detail)) {
      const rows = req.body.inv_daily_detail;
      const clearOld = req.body.meta?.inv_daily_detail_clear_old === true;
      const clearDates = Array.isArray(req.body.meta?.inv_daily_detail_clear_dates) ? req.body.meta.inv_daily_detail_clear_dates : null;
      const tx = db.transaction(() => {
        if (clearOld) {
          // 365日より古い行を削除 (Render disk 圧迫抑制)
          db.prepare(`DELETE FROM mirror_inv_daily_detail WHERE business_date < date('now','-365 days')`).run();
        }
        if (clearDates && clearDates.length > 0) {
          // 同日 stale 防止: 送信元 (miniPC) が「今回 sync で再投入する日付」を明示
          //   → mirror 側でその日付を先に全削除 → 後続 chunk の UPSERT で新値だけ残る
          //   → 古い detail 行 (再集計で消えた SKU 等) を残さない
          // 初回 chunk のみ meta が乗ってる想定なので、後続 chunk でも複数回呼ばれることはない (はず)
          const placeholders = clearDates.map(() => '?').join(',');
          db.prepare(`DELETE FROM mirror_inv_daily_detail WHERE business_date IN (${placeholders})`).run(...clearDates);
        }
        const stmt = db.prepare(`
          INSERT INTO mirror_inv_daily_detail (
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
            snapshot_run_id, synced_at
          )
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(business_date, market, category, source_system, source_item_code, ne_code) DO UPDATE SET
            qty=excluded.qty, unit_cost=excluded.unit_cost, total_value=excluded.total_value,
            cost_status=excluded.cost_status, cost_source=excluded.cost_source, resolution_method=excluded.resolution_method,
            is_bundle_expanded=excluded.is_bundle_expanded, component_qty=excluded.component_qty,
            product_name=excluded.product_name, source_product_name=excluded.source_product_name,
            supplier_code=excluded.supplier_code, product_type=excluded.product_type, handling_class=excluded.handling_class,
            sales_class=excluded.sales_class, representative_product_code=excluded.representative_product_code, order_lot_size=excluded.order_lot_size,
            seasonality_flag=excluded.seasonality_flag, season_months=excluded.season_months,
            new_product_flag=excluded.new_product_flag, new_product_launch_date=excluded.new_product_launch_date,
            last_sold_date=excluded.last_sold_date, sales_7d_qty=excluded.sales_7d_qty,
            sales_30d_qty=excluded.sales_30d_qty, sales_90d_qty=excluded.sales_90d_qty,
            sales_7d_value=excluded.sales_7d_value, sales_30d_value=excluded.sales_30d_value, sales_90d_value=excluded.sales_90d_value,
            working_first_seen=excluded.working_first_seen, fba_unfulfillable_qty=excluded.fba_unfulfillable_qty,
            reserved_qty=excluded.reserved_qty, pending_order_qty=excluded.pending_order_qty,
            location_code=excluded.location_code, last_purchase_date=excluded.last_purchase_date,
            snapshot_run_id=excluded.snapshot_run_id, synced_at=excluded.synced_at
        `);
        for (const r of rows) {
          stmt.run(
            r.business_date, r.market || 'jp', r.category, r.source_system, r.source_item_code, r.ne_code,
            r.qty ?? 0, r.unit_cost ?? null, r.total_value ?? null, r.cost_status, r.cost_source ?? null, r.resolution_method ?? null,
            r.is_bundle_expanded ?? 0, r.component_qty ?? null,
            r.product_name ?? null, r.source_product_name ?? null, r.supplier_code ?? null, r.product_type ?? null, r.handling_class ?? null,
            r.sales_class ?? null, r.representative_product_code ?? null, r.order_lot_size ?? null,
            r.seasonality_flag ?? null, r.season_months ?? null, r.new_product_flag ?? null, r.new_product_launch_date ?? null,
            r.last_sold_date ?? null, r.sales_7d_qty ?? null, r.sales_30d_qty ?? null, r.sales_90d_qty ?? null,
            r.sales_7d_value ?? null, r.sales_30d_value ?? null, r.sales_90d_value ?? null,
            r.working_first_seen ?? null, r.fba_unfulfillable_qty ?? null,
            r.reserved_qty ?? null, r.pending_order_qty ?? null, r.location_code ?? null, r.last_purchase_date ?? null,
            r.snapshot_run_id ?? null, now
          );
        }
      });
      tx();
      log.push(`inv_daily_detail: ${rows.length}件 (${clearOld ? 'cutoff済み' : 'append'})`);
    }

    // inv_daily_summary（全件置換、PR-C 日次在庫スナップショット）
    if (req.body.inv_daily_summary && Array.isArray(req.body.inv_daily_summary)) {
      const rows = req.body.inv_daily_summary;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_inv_daily_summary');
        const stmt = db.prepare(`INSERT INTO mirror_inv_daily_summary (
          business_date, market, category, total_qty, total_value,
          resolved_count, unresolved_count, cost_missing_count,
          source_status, source_row_count, captured_at, synced_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const r of rows) {
          stmt.run(
            r.business_date,
            r.market || 'jp',
            r.category,
            r.total_qty ?? 0,
            r.total_value ?? null,
            r.resolved_count ?? 0,
            r.unresolved_count ?? 0,
            r.cost_missing_count ?? 0,
            r.source_status,
            r.source_row_count ?? null,
            r.captured_at ?? null,
            now
          );
        }
      });
      tx();
      log.push(`inv_daily_summary: ${rows.length}件`);
    }

    // rakuten_sku_map（全件置換）
    if (req.body.rakuten_sku_map && req.body.rakuten_sku_map.length > 0) {
      const rskmData = req.body.rakuten_sku_map;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_rakuten_sku_map');
        const stmt = db.prepare(`INSERT INTO mirror_rakuten_sku_map (
          rakuten_code, ne_code, source, updated_at
        ) VALUES (?,?,?,?)`);
        for (const m of rskmData) {
          stmt.run(m.rakuten_code, m.ne_code, m.source, now);
        }
      });
      tx();
      log.push(`rakuten_sku_map: ${rskmData.length}件`);
    }

    // amazon_sku_fees（全件置換）
    if (req.body.amazon_sku_fees && req.body.amazon_sku_fees.length > 0) {
      const feesData = req.body.amazon_sku_fees;
      const tx = db.transaction(() => {
        db.exec('DELETE FROM mirror_amazon_sku_fees');
        const stmt = db.prepare(`INSERT INTO mirror_amazon_sku_fees (
          seller_sku, asin, fulfillment_channel, referral_fee, referral_fee_rate,
          fba_fee, variable_closing_fee, per_item_fee, total_fee, price_used, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
        for (const f of feesData) {
          stmt.run(f.seller_sku, f.asin, f.fulfillment_channel,
            f.referral_fee, f.referral_fee_rate,
            f.fba_fee, f.variable_closing_fee, f.per_item_fee,
            f.total_fee, f.price_used, f.fetched_at);
        }
      });
      tx();
      log.push(`amazon_sku_fees: ${feesData.length}件`);
    }

    // sales_monthly（初回チャンクでDELETE、以降は追記）
    if (sales_monthly && sales_monthly.length > 0) {
      const tx = db.transaction(() => {
        if (meta?.clear_monthly) db.exec('DELETE FROM mirror_sales_monthly');
        const stmt = db.prepare(`INSERT INTO mirror_sales_monthly (
          月, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
          売上金額, 注文数, データ種別, チャネル, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const s of sales_monthly) {
          stmt.run(s.月, s.商品コード, s.モール, s.商品名, s.数量,
            s.直接販売数 || 0, s.セット経由数 || 0,
            s.売上金額, s.注文数, s.データ種別, s.チャネル || '', now);
        }
      });
      tx();
      log.push(`sales_monthly: ${sales_monthly.length}件`);
    }

    // sales_daily（初回チャンクでDELETE、以降は追記）
    if (sales_daily && sales_daily.length > 0) {
      const tx = db.transaction(() => {
        if (meta?.clear_daily) db.exec('DELETE FROM mirror_sales_daily');
        const stmt = db.prepare(`INSERT INTO mirror_sales_daily (
          日付, 商品コード, モール, 商品名, 数量, 直接販売数, セット経由数,
          売上金額, 注文数, データ種別, チャネル, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
        for (const s of sales_daily) {
          stmt.run(s.日付, s.商品コード, s.モール, s.商品名, s.数量,
            s.直接販売数 || 0, s.セット経由数 || 0,
            s.売上金額, s.注文数, s.データ種別, s.チャネル || '', now);
        }
      });
      tx();
      log.push(`sales_daily: ${sales_daily.length}件`);
    }

    // stock_monthly_snapshot（PR2a 追加、商品収益性ダッシュボード タブB GMROI用）
    //   初回チャンクで meta.clear_stock_snapshot=true → DELETE、以降は追記
    //   Codex PR2a review Medium #1 反映: 空配列でも clear_stock_snapshot だけは処理する
    //   （ミニPC側で対象月内の在庫が0件になった時に mirror 側の stale を消すため）
    if (req.body.stock_monthly_snapshot !== undefined) {
      const snapshotData = req.body.stock_monthly_snapshot;
      const tx = db.transaction(() => {
        if (meta?.clear_stock_snapshot) db.exec('DELETE FROM mirror_stock_monthly_snapshot');
        if (snapshotData.length > 0) {
          const stmt = db.prepare(`INSERT INTO mirror_stock_monthly_snapshot (
            年月, 商品コード, 月末在庫数, 月末引当数, snapshot_source, captured_at, updated_at
          ) VALUES (?,?,?,?,?,?,?)`);
          for (const s of snapshotData) {
            stmt.run(s.年月, s.商品コード, s.月末在庫数 ?? 0, s.月末引当数 ?? 0,
              s.snapshot_source || null, s.captured_at || null, now);
          }
        }
      });
      tx();
      log.push(`stock_monthly_snapshot: ${snapshotData.length}件${meta?.clear_stock_snapshot ? ' (clear)' : ''}`);
    }

    // 同期状態更新
    db.prepare('INSERT OR REPLACE INTO mirror_sync_status (key, value, updated_at) VALUES (?,?,?)').run('last_sync', now, now);
    if (meta) {
      for (const [k, v] of Object.entries(meta)) {
        db.prepare('INSERT OR REPLACE INTO mirror_sync_status (key, value, updated_at) VALUES (?,?,?)').run(k, String(v), now);
      }
    }

    // WALチェックポイント
    try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

    console.log('[Mirror] 同期完了:', log.join(', '));
    res.json({ ok: true, log, synced_at: now });
  } catch (e) {
    console.error('[Mirror] 同期エラー:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── Phase 1 #1-4a: POST /api/sync/:entity/chunk (entity-driven contract sync) ───
//
// 受信処理:
//   1. 認証 (x-sync-key)
//   2. payload validation (sync_run_id / contract_version / chunk_index 等)
//   3. checksum 検証 (改ざん検知)
//   4. is_first=true なら scope clear (entity 別 clear strategy)
//   5. payload を mirror テーブルに INSERT OR REPLACE
//   6. sync_run_chunks に ledger 記録 (received_at)
//   7. apply 成功で applied_at を更新
//   8. is_last=true なら status response
//
// Phase 1 #1-4 で sync_contracts に登録済みの entity のみ受信
// (現状: amazon_finance_sku_daily v1)
//
// payload 構造 (POST body):
// {
//   sync_run_id: "amazon_finance_sku_daily-v1-2026-05-08T0731",
//   contract_version: 1,
//   scope_from: "2026-04-01",
//   scope_to: "2026-04-30",
//   chunk_index: 0,
//   chunk_count: 5,
//   is_first: true,
//   is_last: false,
//   row_count: 5000,
//   payload_checksum: "sha256...",
//   meta: { clear_amazon_finance_dates: ["2026-04-01", ...] },  // first chunk のみ
//   payload: { rows: [...] }
// }
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// Insert/ledger statement キャッシュ (DB 接続単位、Codex Round 13 #2)
// モジュール変数だと DB 切替時に古い connection の statement を再利用してしまうため WeakMap
const _stmtCache = new WeakMap();  // db => { stmts by entity }
function getStmtBundle(db) {
  let bundle = _stmtCache.get(db);
  if (!bundle) {
    bundle = {};
    _stmtCache.set(db, bundle);
  }
  return bundle;
}

function getAmazonFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.amazonFinanceInsert) {
    b.amazonFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_amazon_finance_sku_daily (
        date_jst, seller_sku, asin_norm, product_name,
        units_ordered, units_refunded_customer, units_marketplace_guarantee,
        units_a_to_z_refund, units_net_sold,
        sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy,
        commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy,
        shipping_chargeback_jpy, giftwrap_chargeback_jpy, promotion_jpy,
        warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy,
        refund_principal_jpy, reversal_reimbursement_jpy,
        misc_fee_jpy, other_fee_jpy, other_amount_jpy,
        unit_cost_snapshot, cost_snapshot_date_jst, latest_unit_cost_reference,
        cogs_amount, profit_amount, is_cost_complete, cost_status,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @seller_sku, @asin_norm, @product_name,
        @units_ordered, @units_refunded_customer, @units_marketplace_guarantee,
        @units_a_to_z_refund, @units_net_sold,
        @sales_principal_jpy, @sales_shipping_jpy, @sales_giftwrap_jpy, @sales_tax_jpy,
        @commission_jpy, @fba_fulfillment_jpy, @fba_storage_jpy, @closing_fee_jpy,
        @shipping_chargeback_jpy, @giftwrap_chargeback_jpy, @promotion_jpy,
        @warehouse_damage_jpy, @warehouse_lost_jpy, @safe_t_jpy,
        @refund_principal_jpy, @reversal_reimbursement_jpy,
        @misc_fee_jpy, @other_fee_jpy, @other_amount_jpy,
        @unit_cost_snapshot, @cost_snapshot_date_jst, @latest_unit_cost_reference,
        @cogs_amount, @profit_amount, @is_cost_complete, @cost_status,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.amazonFinanceInsert;
}

function getRakutenFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.rakutenFinanceInsert) {
    b.rakutenFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_rakuten_finance_sku_daily (
        date_jst, rakuten_code, ne_code, sku_resolution, product_name,
        units_ordered, units_cancelled, units_net_sold,
        allocated_units_cancelled, units_cancelled_same_day_matched,
        allocation_method, cancel_exceeds_ordered_warning,
        sales_principal_jpy_incl, sales_postage_jpy_incl,
        coupon_shop_jpy_incl, coupon_all_jpy_incl, promotion_jpy_incl,
        refund_amount_jpy_incl, allocated_refund_amount_jpy_incl, refund_amount_same_day_matched_jpy_incl,
        mall_fee_jpy_incl,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        gross_sales_jpy_incl, net_sales_jpy_incl,
        variable_margin_jpy_incl, refund_adjusted_net_sales_jpy_incl,
        cost_status, is_cost_complete, data_quality_score, price_variance_warning,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @rakuten_code, @ne_code, @sku_resolution, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @allocated_units_cancelled, @units_cancelled_same_day_matched,
        @allocation_method, @cancel_exceeds_ordered_warning,
        @sales_principal_jpy_incl, @sales_postage_jpy_incl,
        @coupon_shop_jpy_incl, @coupon_all_jpy_incl, @promotion_jpy_incl,
        @refund_amount_jpy_incl, @allocated_refund_amount_jpy_incl, @refund_amount_same_day_matched_jpy_incl,
        @mall_fee_jpy_incl,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @gross_sales_jpy_incl, @net_sales_jpy_incl,
        @variable_margin_jpy_incl, @refund_adjusted_net_sales_jpy_incl,
        @cost_status, @is_cost_complete, @data_quality_score, @price_variance_warning,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.rakutenFinanceInsert;
}

function getYahooFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.yahooFinanceInsert) {
    b.yahooFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_yahoo_finance_sku_daily (
        date_jst, yahoo_sku_key, ne_code, variant_key, resolution_method,
        unresolved_sku_flag, product_name,
        units_ordered, units_cancelled, units_net_sold,
        sales_principal_jpy_incl, sales_postage_jpy_incl, gross_sales_jpy_incl,
        net_sales_before_point_jpy_incl, listing_sales_estimated_jpy_incl,
        coupon_shop_jpy_incl, use_point_jpy_incl,
        mall_fee_jpy_incl, mall_fee_calc_method, mall_fee_estimate_delta_jpy,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        variable_margin_partial_jpy_incl, variable_margin_full_jpy_incl,
        refund_adjusted_net_sales_jpy_incl, margin_confidence, margin_full_finalized_at,
        pay_charge_audit_jpy_incl, ship_charge_audit_jpy_incl, discount_audit_jpy_incl,
        cost_status, is_cost_complete, data_quality_score, price_variance_warning,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @yahoo_sku_key, @ne_code, @variant_key, @resolution_method,
        @unresolved_sku_flag, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @sales_principal_jpy_incl, @sales_postage_jpy_incl, @gross_sales_jpy_incl,
        @net_sales_before_point_jpy_incl, @listing_sales_estimated_jpy_incl,
        @coupon_shop_jpy_incl, @use_point_jpy_incl,
        @mall_fee_jpy_incl, @mall_fee_calc_method, @mall_fee_estimate_delta_jpy,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @variable_margin_partial_jpy_incl, @variable_margin_full_jpy_incl,
        @refund_adjusted_net_sales_jpy_incl, @margin_confidence, @margin_full_finalized_at,
        @pay_charge_audit_jpy_incl, @ship_charge_audit_jpy_incl, @discount_audit_jpy_incl,
        @cost_status, @is_cost_complete, @data_quality_score, @price_variance_warning,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.yahooFinanceInsert;
}

function getAupayFinanceInsert(db) {
  const b = getStmtBundle(db);
  if (!b.aupayFinanceInsert) {
    b.aupayFinanceInsert = db.prepare(`
      INSERT OR REPLACE INTO mirror_aupay_finance_sku_daily (
        date_jst, aupay_sku_key, ne_code, variant_key, resolution_method,
        unresolved_sku_flag, product_name,
        units_ordered, units_cancelled, units_net_sold,
        sales_principal_jpy_incl, postage_allocated_jpy_incl, gross_sales_jpy_incl,
        net_sales_after_coupon_jpy_incl, request_price_jpy_incl,
        coupon_shop_jpy_incl,
        gift_point_jpy_incl, use_ponta_point_jpy_incl, use_au_point_jpy_incl,
        premium_member_point_jpy_incl, point_cost_pending_jpy_incl,
        tax_normal_sales_jpy_incl, tax_reduced_sales_jpy_incl, tax_free_sales_jpy_incl,
        mall_fee_jpy_incl, mall_fee_rate_applied, mall_fee_calc_method, mall_fee_estimate_delta_jpy,
        shipping_cost_jpy_incl, shipping_quality,
        unit_cost_snapshot_incl, cost_snapshot_date_jst,
        latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
        variable_margin_partial_jpy_incl, variable_margin_full_jpy_incl,
        refund_adjusted_net_sales_jpy_incl, margin_confidence, margin_full_finalized_at,
        before_discount_jpy_incl, detail_discount_jpy_incl, charge_allocated_jpy_incl,
        item_option_jpy_incl, gift_wrapping_jpy_incl,
        cost_status, is_cost_complete, data_quality_score, price_variance_warning,
        order_count, line_count,
        source_layer_summary, source_row_count, built_at,
        source_run_id, source_row_hash, synced_at
      ) VALUES (
        @date_jst, @aupay_sku_key, @ne_code, @variant_key, @resolution_method,
        @unresolved_sku_flag, @product_name,
        @units_ordered, @units_cancelled, @units_net_sold,
        @sales_principal_jpy_incl, @postage_allocated_jpy_incl, @gross_sales_jpy_incl,
        @net_sales_after_coupon_jpy_incl, @request_price_jpy_incl,
        @coupon_shop_jpy_incl,
        @gift_point_jpy_incl, @use_ponta_point_jpy_incl, @use_au_point_jpy_incl,
        @premium_member_point_jpy_incl, @point_cost_pending_jpy_incl,
        @tax_normal_sales_jpy_incl, @tax_reduced_sales_jpy_incl, @tax_free_sales_jpy_incl,
        @mall_fee_jpy_incl, @mall_fee_rate_applied, @mall_fee_calc_method, @mall_fee_estimate_delta_jpy,
        @shipping_cost_jpy_incl, @shipping_quality,
        @unit_cost_snapshot_incl, @cost_snapshot_date_jst,
        @latest_unit_cost_reference_incl, @cogs_amount_jpy_incl,
        @variable_margin_partial_jpy_incl, @variable_margin_full_jpy_incl,
        @refund_adjusted_net_sales_jpy_incl, @margin_confidence, @margin_full_finalized_at,
        @before_discount_jpy_incl, @detail_discount_jpy_incl, @charge_allocated_jpy_incl,
        @item_option_jpy_incl, @gift_wrapping_jpy_incl,
        @cost_status, @is_cost_complete, @data_quality_score, @price_variance_warning,
        @order_count, @line_count,
        @source_layer_summary, @source_row_count, @built_at,
        @source_run_id, @source_row_hash, @synced_at
      )
    `);
  }
  return b.aupayFinanceInsert;
}

function getLedgerInsert(db) {
  const b = getStmtBundle(db);
  if (!b.ledgerInsert) {
    b.ledgerInsert = db.prepare(`
      INSERT INTO sync_run_chunks
        (run_id, entity, chunk_index, chunk_count, row_count, payload_checksum,
         contract_version, scope_from, scope_to, received_at, applied_at, mf_source_run_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
  }
  return b.ledgerInsert;
}

// ─── MF Phase 1a: insert factory + 列定義 (ENTITY_REGISTRY より先に必要) ───
// 全 MF mirror テーブル共通: INSERT OR REPLACE (run_id+業務PK でユニーク、idempotent)
function makeMfInsertFactory(table, cols) {
  return (db) => {
    const b = getStmtBundle(db);
    const cacheKey = '_mfInsert_' + table;
    if (!b[cacheKey]) {
      const colsList = cols.join(', ');
      const placeholders = cols.map(c => '@' + c).join(', ');
      b[cacheKey] = db.prepare(`INSERT OR REPLACE INTO ${table} (${colsList}) VALUES (${placeholders})`);
    }
    return b[cacheKey];
  };
}

const MF_PUBLISH_RUNS_COLS = [
  'run_id', 'scope', 'status', 'started_at', 'finished_at',
  'error_message', 'source_run_hash', 'synced_at', 'finalized_at'
];
const MF_EXECUTIVE_TOP_COLS = [
  'run_id', 'snapshot_date', 'current_month_ym',
  'sales_mtd_excl_tax', 'gross_profit_mtd_excl_tax', 'operating_income_mtd_excl_tax',
  'sales_month_end_forecast', 'gross_profit_month_end_forecast', 'operating_income_month_end_forecast',
  'forecast_status',
  'yoy_sales_pct', 'yoy_gross_profit_pct', 'yoy_operating_income_pct',
  'cash_balance_total', 'cash_balance_json', 'danger_signals_json',
  'data_window_from', 'data_window_to', 'reliability_label',
  'source_row_hash', 'synced_at'
];
const MF_PL_MONTHLY_COLS = [
  'run_id', 'month_ym', 'role_key', 'amount_excl_tax', 'tax_amount',
  'line_count', 'is_realized_only', 'source_row_hash', 'synced_at'
];
const MF_CHANNEL_SALES_COLS = [
  'run_id', 'month_ym', 'channel_key', 'channel_display_name',
  'gross_sales_excl_tax', 'pf_fee_excl_tax', 'ad_cost_excl_tax', 'fba_fee_excl_tax',
  'net_sales_after_pf_excl_tax', 'unmapped_amount_excl_tax', 'mapping_coverage_pct',
  'source_row_hash', 'synced_at'
];
const MF_CASH_EVENTS_DAILY_COLS = [
  'run_id', 'movement_date', 'bank_account_key', 'direction',
  'amount_excl_tax', 'event_count', 'source_row_hash', 'synced_at'
];
const MF_BALANCE_SNAPSHOT_COLS = [
  'run_id', 'month_ym', 'account_key', 'account_name', 'sub_account_name',
  'role_key', 'closing_balance_excl_tax', 'source_row_hash', 'synced_at'
];
const MF_ANOMALY_SIGNALS_COLS = [
  'run_id', 'signal_id', 'detected_at', 'signal_code', 'signal_key',
  'severity', 'severity_rank', 'title', 'description',
  'observed_value', 'threshold_value', 'related_entity_key',
  'recommended_action', 'source_mart',
  'source_row_hash', 'synced_at'
];
// Phase 1d-2: 期 (FY) 累計 + 平均残高
const MF_FY_SUMMARY_COLS = [
  'run_id', 'fy_number', 'fy_start_ym', 'fy_end_ym',
  'cumulative_through_ym', 'months_in_cumulative', 'is_fy_completed',
  'sales_cum', 'cogs_cum', 'gross_profit_cum', 'sgae_cum', 'operating_income_cum',
  'non_op_revenue_cum', 'non_op_expense_cum', 'ordinary_income_cum',
  'personnel_cost_cum',
  'ar_average', 'inventory_average', 'ap_average', 'total_asset_average',
  'ar_opening', 'ar_closing', 'inventory_opening', 'inventory_closing',
  'ap_opening', 'ap_closing', 'total_asset_opening', 'total_asset_closing',
  'cash_closing', 'short_loan_closing', 'long_loan_closing',
  'current_liab_closing', 'total_equity_closing',
  'source_row_hash', 'synced_at'
];
// Phase 1d-3b: BS section別月末
const MF_BS_MONTHLY_COLS = [
  'run_id', 'month_ym',
  'cash_total', 'ar_total', 'inventory_total', 'other_current_asset', 'current_asset_total',
  'tangible_fixed_asset', 'investment_other', 'fixed_asset_total',
  'total_asset',
  'ap_total', 'short_loan_total', 'other_current_liab', 'current_liab_total',
  'long_loan_total', 'other_fixed_liab', 'fixed_liab_total',
  'total_liab',
  'capital', 'retained', 'total_equity',
  'bs_other_total',
  'retained_balance', 'current_period_profit', 'display_total_equity',
  'source_row_hash', 'synced_at'
];
// Phase 1d-3b: BS 細目
const MF_BS_SUBACCOUNT_COLS = [
  'run_id', 'month_ym', 'account_name', 'sub_account_name', 'role_key', 'section',
  'closing_balance_excl_tax', 'is_hub_null_sub', 'source_row_hash', 'synced_at'
];

// ─── Entity Registry (entity-driven dispatch、楽天 #R-3b で導入) ───
// 新 entity (Yahoo / メルカリ等) 追加時はここに 1 エントリ追加するだけで
// chunk endpoint / backout endpoint が自動対応する
//
// clear_strategy:
//   'date_range' (default): 第1 chunk で meta[clear_meta_key] の date 配列を DELETE
//   'no_clear':             clear をスキップ (run_id ベース append-only 用、MF Phase 1a)
//
// requires_parent_run (MF Phase 1a):
//   true:  meta.mf_source_run_id を含み、Render 側で mirror_mf_publish_runs(run_id) 存在チェック
const ENTITY_REGISTRY = {
  amazon_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_amazon_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_amazon_finance_dates',
    getInsertStmt: getAmazonFinanceInsert,
    normalizeRow: (r) => normalizeAmazonFinanceRow(r),
  },
  rakuten_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_rakuten_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_rakuten_finance_dates',
    getInsertStmt: getRakutenFinanceInsert,
    normalizeRow: (r) => normalizeRakutenFinanceRow(r),
  },
  yahoo_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_yahoo_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_yahoo_finance_dates',
    getInsertStmt: getYahooFinanceInsert,
    normalizeRow: (r) => normalizeYahooFinanceRow(r),
  },
  aupay_finance_sku_daily: {
    contract_version: 1,
    mirror_table: 'mirror_aupay_finance_sku_daily',
    clear_strategy: 'date_range',
    clear_meta_key: 'clear_aupay_finance_dates',
    getInsertStmt: getAupayFinanceInsert,
    normalizeRow: (r) => normalizeAupayFinanceRow(r),
  },

  // ─── MF Phase 1a (Codex 5ラウンド確定設計) ─────────────────────────
  // 受信順序契約 (sync-to-render.js 側で保証):
  //   1. mf_publish_runs を最初に送信 (status='pending_sync' で親 row 作成)
  //   2. 6 mart entities (children) 全部送信
  //   3. POST /api/sync/mf/runs/:run_id/finalize で status='success' に flip
  // VIEW v_mirror_mf_*_latest は status='success' の最新 run のみ公開
  mf_publish_runs: {
    contract_version: 1,
    mirror_table: 'mirror_mf_publish_runs',
    clear_strategy: 'no_clear',
    requires_parent_run: false,
    getInsertStmt: makeMfInsertFactory('mirror_mf_publish_runs', MF_PUBLISH_RUNS_COLS),
    normalizeRow: (r) => normalizeMfPublishRunRow(r),
  },
  mf_executive_top: {
    contract_version: 1,
    mirror_table: 'mirror_mf_executive_top',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_executive_top', MF_EXECUTIVE_TOP_COLS),
    normalizeRow: (r) => normalizeMfExecutiveTopRow(r),
  },
  mf_pl_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_pl_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_pl_monthly', MF_PL_MONTHLY_COLS),
    normalizeRow: (r) => normalizeMfPlMonthlyRow(r),
  },
  mf_channel_sales: {
    contract_version: 1,
    mirror_table: 'mirror_mf_channel_sales',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_channel_sales', MF_CHANNEL_SALES_COLS),
    normalizeRow: (r) => normalizeMfChannelSalesRow(r),
  },
  mf_cash_events_daily: {
    contract_version: 1,
    mirror_table: 'mirror_mf_cash_events_daily',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_cash_events_daily', MF_CASH_EVENTS_DAILY_COLS),
    normalizeRow: (r) => normalizeMfCashEventsDailyRow(r),
  },
  mf_balance_snapshot_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_balance_snapshot_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_balance_snapshot_monthly', MF_BALANCE_SNAPSHOT_COLS),
    normalizeRow: (r) => normalizeMfBalanceSnapshotRow(r),
  },
  mf_anomaly_signals: {
    contract_version: 1,
    mirror_table: 'mirror_mf_anomaly_signals',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_anomaly_signals', MF_ANOMALY_SIGNALS_COLS),
    normalizeRow: (r) => normalizeMfAnomalySignalsRow(r),
  },
  // Phase 1d-2: 期 (FY) 累計 + 平均残高
  mf_fy_summary: {
    contract_version: 1,
    mirror_table: 'mirror_mf_fy_summary',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_fy_summary', MF_FY_SUMMARY_COLS),
    normalizeRow: (r) => normalizeMfFySummaryRow(r),
  },
  // Phase 1d-3b: BS section別月末
  mf_bs_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_bs_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_bs_monthly', MF_BS_MONTHLY_COLS),
    normalizeRow: (r) => normalizeMfBsMonthlyRow(r),
  },
  // Phase 1d-3b: BS 細目
  mf_bs_subaccount_monthly: {
    contract_version: 1,
    mirror_table: 'mirror_mf_bs_subaccount_monthly',
    clear_strategy: 'no_clear',
    requires_parent_run: true,
    getInsertStmt: makeMfInsertFactory('mirror_mf_bs_subaccount_monthly', MF_BS_SUBACCOUNT_COLS),
    normalizeRow: (r) => normalizeMfBsSubaccountRow(r),
  },
};

// HttpError: tx 内から throw して outer catch で http response に変換
class HttpError extends Error {
  constructor(status, body) { super(body.error || 'http_error'); this.status = status; this.body = body; }
}

router.post('/api/sync/:entity/chunk', requireSyncKey, async (req, res) => {
  const entity = req.params.entity;
  const body = req.body || {};
  const {
    sync_run_id, contract_version, scope_from, scope_to,
    chunk_index, chunk_count, is_first, is_last, row_count,
    payload_checksum, meta = {}, payload = {}
  } = body;

  // ────────── 1. payload validation ──────────
  if (!sync_run_id || typeof sync_run_id !== 'string') {
    return res.status(400).json({ error: 'sync_run_id required' });
  }
  if (!Number.isInteger(contract_version)) {
    return res.status(400).json({ error: 'contract_version must be integer' });
  }
  if (!Number.isInteger(chunk_index) || chunk_index < 0) {
    return res.status(400).json({ error: 'chunk_index must be non-negative integer' });
  }
  if (!Number.isInteger(chunk_count) || chunk_count <= 0) {
    return res.status(400).json({ error: 'chunk_count must be positive integer' });
  }
  if (chunk_index >= chunk_count) {
    return res.status(400).json({ error: `chunk_index=${chunk_index} >= chunk_count=${chunk_count}` });
  }
  if (typeof payload_checksum !== 'string' || payload_checksum.length === 0) {
    return res.status(400).json({ error: 'payload_checksum required' });
  }
  if (!DATE_RE.test(scope_from) || !DATE_RE.test(scope_to)) {
    return res.status(400).json({ error: 'scope_from/scope_to must be YYYY-MM-DD' });
  }
  if (scope_from > scope_to) {
    return res.status(400).json({ error: `scope_from=${scope_from} > scope_to=${scope_to}` });
  }
  const isFirstExpected = chunk_index === 0;
  const isLastExpected = chunk_index === chunk_count - 1;
  if (Boolean(is_first) !== isFirstExpected) {
    return res.status(400).json({ error: `is_first=${is_first} mismatch chunk_index=${chunk_index} (expected ${isFirstExpected})` });
  }
  if (Boolean(is_last) !== isLastExpected) {
    return res.status(400).json({ error: `is_last=${is_last} mismatch chunk_index=${chunk_index}/${chunk_count} (expected ${isLastExpected})` });
  }
  const rows = Array.isArray(payload?.rows) ? payload.rows : null;
  if (rows === null) {
    return res.status(400).json({ error: 'payload.rows must be array' });
  }
  if (!Number.isInteger(row_count) || row_count !== rows.length) {
    return res.status(400).json({ error: `row_count=${row_count} != payload.rows.length=${rows.length}` });
  }

  // ────────── 2. entity / contract_version 値域確認 (Codex Round 12 #5) ──────────
  const entityCfg = ENTITY_REGISTRY[entity];
  if (!entityCfg) {
    return res.status(400).json({ error: `unsupported entity: ${entity}` });
  }
  if (contract_version !== entityCfg.contract_version) {
    return res.status(400).json({
      error: 'contract_version_mismatch',
      message: `entity=${entity} requires contract_version=${entityCfg.contract_version} (got ${contract_version})`,
    });
  }

  // ────────── 3. checksum 検証 + first chunk meta 形式 ──────────
  const crypto = await import('node:crypto');
  const payloadStr = JSON.stringify(payload);
  const computedChecksum = crypto.createHash('sha256').update(payloadStr).digest('hex');
  if (computedChecksum !== payload_checksum) {
    console.error(`[Mirror] checksum mismatch req entity=${entity} chunk=${chunk_index}`);
    return res.status(400).json({
      error: 'payload_checksum mismatch',
      expected: payload_checksum, computed: computedChecksum,
    });
  }
  let clearDates = null;
  const clearStrategy = entityCfg.clear_strategy || 'date_range';  // backward compat
  if (is_first && clearStrategy === 'date_range') {
    clearDates = meta[entityCfg.clear_meta_key];
    if (!Array.isArray(clearDates) || clearDates.length === 0) {
      return res.status(400).json({ error: `first chunk requires meta.${entityCfg.clear_meta_key} (non-empty array)` });
    }
    for (const d of clearDates) {
      if (!DATE_RE.test(d)) return res.status(400).json({ error: `invalid date in clear list: ${d}` });
    }
  }

  // MF Phase 1a (Codex review #88 反映):
  //   - 全 MF entity (mf_publish_runs 親 + 6 mart 子) で meta.mf_source_run_id を要求 + ledger 保存
  //   - 子 entity は parent.status='pending_sync' を tx 内で再 check (race 防止)
  //   - finalize 時に ledger.mf_source_run_id == finalize_run_id を cross-check
  const isMfEntity = clearStrategy === 'no_clear';
  let mfSourceRunIdForLedger = null;
  if (isMfEntity) {
    const mfSourceRunId = meta.mf_source_run_id;
    if (is_first && !Number.isInteger(mfSourceRunId)) {
      return res.status(400).json({ error: `MF entity '${entity}' first chunk requires meta.mf_source_run_id (integer)` });
    }
    if (Number.isInteger(mfSourceRunId)) {
      mfSourceRunIdForLedger = mfSourceRunId;
    }
  }

  // ────────── 4. tx 実行 (BEGIN IMMEDIATE で write 直列化、Codex Round 12 #1 #3) ──────────
  const db = getMirrorDB();
  const now = new Date().toISOString();
  const requestId = `sync-${Date.now().toString(36)}`;
  const insertStmt = entityCfg.getInsertStmt(db);
  const insertLedger = getLedgerInsert(db);
  let result;
  try {
    db.transaction(() => {
      // 4a. run 単位の不変条件: 既存 chunk があれば contract_version / scope / chunk_count 一致 (#2)
      const runFirst = db.prepare(`
        SELECT contract_version, scope_from, scope_to, chunk_count
        FROM sync_run_chunks WHERE run_id = ? AND entity = ? LIMIT 1
      `).get(sync_run_id, entity);
      if (runFirst) {
        if (runFirst.contract_version !== contract_version
            || runFirst.scope_from !== scope_from
            || runFirst.scope_to !== scope_to
            || runFirst.chunk_count !== chunk_count) {
          throw new HttpError(409, {
            error: 'run_invariant_mismatch',
            message: 'run 内の chunk で contract_version / scope / chunk_count が一致しません',
            run_existing: runFirst,
            received: { contract_version, scope_from, scope_to, chunk_count },
          });
        }
      }

      // 4b. 同一 chunk_index 既存確認 (idempotent re-send 短絡 / 不一致は 409、Codex Round 12 #3)
      const existing = db.prepare(`
        SELECT chunk_count, row_count, payload_checksum, scope_from, scope_to
        FROM sync_run_chunks WHERE run_id = ? AND entity = ? AND chunk_index = ?
      `).get(sync_run_id, entity, chunk_index);
      if (existing) {
        if (existing.payload_checksum !== payload_checksum
            || existing.row_count !== row_count
            || existing.chunk_count !== chunk_count) {
          throw new HttpError(409, {
            error: 'chunk_resend_mismatch',
            existing, received: { payload_checksum, row_count, chunk_count },
          });
        }
        result = { replayed: true };
        return;
      }

      // 4c. scope clear (is_first かつ この run でまだ何も apply 無い時のみ、#1 race 解消版)
      // MF Phase 1a: clear_strategy='no_clear' は scope clear をスキップ (run_id ベース append-only)
      let didClear = false;
      let clearedRows = 0;
      if (is_first && clearStrategy === 'date_range') {
        const priorCount = db.prepare(`
          SELECT COUNT(*) AS c FROM sync_run_chunks WHERE run_id = ? AND entity = ?
        `).get(sync_run_id, entity).c;
        if (priorCount === 0) {
          const placeholders = clearDates.map(() => '?').join(',');
          clearedRows = db.prepare(`
            DELETE FROM ${entityCfg.mirror_table} WHERE date_jst IN (${placeholders})
          `).run(...clearDates).changes;
          didClear = true;
        }
      }

      // 4c-mf. MF entity の場合、tx 内で parent.status='pending_sync' を再 check (race 防止)
      // Codex review #88: success/failed flip 後の追加 INSERT/REPLACE を完全拒否
      if (entityCfg.requires_parent_run) {
        if (!Number.isInteger(mfSourceRunIdForLedger)) {
          throw new HttpError(400, {
            error: 'mf_source_run_id_missing',
            message: `MF mart entity '${entity}' chunk_index=${chunk_index} requires meta.mf_source_run_id (integer) on every chunk`,
          });
        }
        const parent = db.prepare(`SELECT run_id, status FROM mirror_mf_publish_runs WHERE run_id = ?`).get(mfSourceRunIdForLedger);
        if (!parent) {
          throw new HttpError(409, {
            error: 'parent_run_not_found',
            message: `mirror_mf_publish_runs.run_id=${mfSourceRunIdForLedger} が存在しません。先に entity='mf_publish_runs' を送信してください。`,
            entity, sync_run_id, mf_source_run_id: mfSourceRunIdForLedger,
          });
        }
        if (parent.status !== 'pending_sync') {
          throw new HttpError(409, {
            error: 'parent_run_not_pending',
            message: `mirror_mf_publish_runs.run_id=${mfSourceRunIdForLedger} status=${parent.status} (pending_sync のみ受信可、success/failed 確定済 run の追加変更を拒否)`,
            entity, sync_run_id, mf_source_run_id: mfSourceRunIdForLedger, parent_status: parent.status,
          });
        }
        // sync_run_id 内で mf_source_run_id 一貫性チェック (異 run 混入防止)
        if (runFirst) {
          const firstMfRow = db.prepare(`
            SELECT mf_source_run_id FROM sync_run_chunks
            WHERE run_id = ? AND entity = ? LIMIT 1
          `).get(sync_run_id, entity);
          if (firstMfRow && firstMfRow.mf_source_run_id !== mfSourceRunIdForLedger) {
            throw new HttpError(409, {
              error: 'mf_source_run_id_mismatch_within_sync_run',
              message: `sync_run_id 内で異なる mf_source_run_id が混入 (first=${firstMfRow.mf_source_run_id}, current=${mfSourceRunIdForLedger})`,
              entity, sync_run_id,
            });
          }
        }
      }

      // 4d. row INSERT
      for (const r of rows) {
        insertStmt.run(entityCfg.normalizeRow(r));
      }

      // 4e. ledger insert (mf_source_run_id NULL=非MF entity)
      insertLedger.run(sync_run_id, entity, chunk_index, chunk_count, row_count,
                       payload_checksum, contract_version, scope_from, scope_to, now, now,
                       mfSourceRunIdForLedger);

      result = { applied: true, didClear, clearedRows };
    }).immediate();  // BEGIN IMMEDIATE で write 直列化 (cross-process 安全)
  } catch (e) {
    if (e instanceof HttpError) {
      return res.status(e.status).json({ ...e.body, request_id: requestId });
    }
    console.error(`[Mirror] sync chunk error req=${requestId}: ${e.message}`);
    return res.status(500).json({ error: e.message, request_id: requestId });
  }

  if (result.replayed) {
    console.log(`[Mirror] sync chunk replayed (idempotent) req=${requestId} entity=${entity} run=${sync_run_id} chunk=${chunk_index}`);
    return res.json({ ok: true, request_id: requestId, sync_run_id, entity, chunk_index, replayed: true });
  }

  console.log(`[Mirror] sync chunk applied req=${requestId} entity=${entity} run=${sync_run_id} chunk=${chunk_index}/${chunk_count} rows=${rows.length} cleared=${result.didClear ? result.clearedRows : 'no'}`);

  // ────────── 5. is_last なら欠番チェック ──────────
  if (is_last) {
    const present = db.prepare(`
      SELECT chunk_index, row_count FROM sync_run_chunks
      WHERE run_id = ? AND entity = ? ORDER BY chunk_index
    `).all(sync_run_id, entity);
    const presentSet = new Set(present.map(p => p.chunk_index));
    const missing = [];
    for (let i = 0; i < chunk_count; i++) if (!presentSet.has(i)) missing.push(i);
    const totalRows = present.reduce((s, p) => s + p.row_count, 0);
    if (missing.length > 0) {
      console.warn(`[Mirror] sync is_last but missing chunks: ${missing.join(',')} run=${sync_run_id}`);
      return res.status(409).json({
        ok: false, request_id: requestId, sync_run_id, entity,
        status: 'incomplete',
        chunks_received: present.length, expected: chunk_count,
        missing_chunks: missing, rows_received: totalRows,
      });
    }
    return res.json({
      ok: true, request_id: requestId, sync_run_id, entity, status: 'completed',
      chunks_received: present.length, expected: chunk_count, rows_received: totalRows,
    });
  }

  return res.json({ ok: true, request_id: requestId, sync_run_id, entity, chunk_index });
});

// row 列正規化 (mirror_yahoo_finance_sku_daily 用、Yahoo Phase 1 Y-3b)
function normalizeYahooFinanceRow(r) {
  return {
    date_jst: r.date_jst, yahoo_sku_key: r.yahoo_sku_key,
    ne_code: r.ne_code ?? null,
    variant_key: r.variant_key ?? '',
    resolution_method: r.resolution_method,
    unresolved_sku_flag: r.unresolved_sku_flag ?? 0,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    sales_principal_jpy_incl: r.sales_principal_jpy_incl ?? 0,
    sales_postage_jpy_incl: r.sales_postage_jpy_incl ?? 0,
    gross_sales_jpy_incl: r.gross_sales_jpy_incl ?? 0,
    net_sales_before_point_jpy_incl: r.net_sales_before_point_jpy_incl ?? 0,
    listing_sales_estimated_jpy_incl: r.listing_sales_estimated_jpy_incl ?? 0,
    coupon_shop_jpy_incl: r.coupon_shop_jpy_incl ?? 0,
    use_point_jpy_incl: r.use_point_jpy_incl ?? 0,
    mall_fee_jpy_incl: r.mall_fee_jpy_incl ?? 0,
    mall_fee_calc_method: r.mall_fee_calc_method ?? 'estimated_10pct',
    mall_fee_estimate_delta_jpy: r.mall_fee_estimate_delta_jpy ?? null,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    variable_margin_partial_jpy_incl: r.variable_margin_partial_jpy_incl ?? 0,
    variable_margin_full_jpy_incl: r.variable_margin_full_jpy_incl ?? null,
    refund_adjusted_net_sales_jpy_incl: r.refund_adjusted_net_sales_jpy_incl ?? null,
    margin_confidence: r.margin_confidence ?? 'partial',
    margin_full_finalized_at: r.margin_full_finalized_at ?? null,
    pay_charge_audit_jpy_incl: r.pay_charge_audit_jpy_incl ?? 0,
    ship_charge_audit_jpy_incl: r.ship_charge_audit_jpy_incl ?? 0,
    discount_audit_jpy_incl: r.discount_audit_jpy_incl ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    price_variance_warning: r.price_variance_warning ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_aupay_finance_sku_daily 用、au PAY Phase 1 A-2)
function normalizeAupayFinanceRow(r) {
  return {
    date_jst: r.date_jst, aupay_sku_key: r.aupay_sku_key,
    ne_code: r.ne_code ?? null,
    variant_key: r.variant_key ?? '',
    resolution_method: r.resolution_method,
    unresolved_sku_flag: r.unresolved_sku_flag ?? 0,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    sales_principal_jpy_incl: r.sales_principal_jpy_incl ?? 0,
    postage_allocated_jpy_incl: r.postage_allocated_jpy_incl ?? 0,
    gross_sales_jpy_incl: r.gross_sales_jpy_incl ?? 0,
    net_sales_after_coupon_jpy_incl: r.net_sales_after_coupon_jpy_incl ?? 0,
    request_price_jpy_incl: r.request_price_jpy_incl ?? 0,
    coupon_shop_jpy_incl: r.coupon_shop_jpy_incl ?? 0,
    gift_point_jpy_incl: r.gift_point_jpy_incl ?? 0,
    use_ponta_point_jpy_incl: r.use_ponta_point_jpy_incl ?? 0,
    use_au_point_jpy_incl: r.use_au_point_jpy_incl ?? 0,
    premium_member_point_jpy_incl: r.premium_member_point_jpy_incl ?? 0,
    point_cost_pending_jpy_incl: r.point_cost_pending_jpy_incl ?? 0,
    tax_normal_sales_jpy_incl: r.tax_normal_sales_jpy_incl ?? 0,
    tax_reduced_sales_jpy_incl: r.tax_reduced_sales_jpy_incl ?? 0,
    tax_free_sales_jpy_incl: r.tax_free_sales_jpy_incl ?? 0,
    mall_fee_jpy_incl: r.mall_fee_jpy_incl ?? null,
    mall_fee_rate_applied: r.mall_fee_rate_applied ?? null,
    mall_fee_calc_method: r.mall_fee_calc_method ?? 'unknown',
    mall_fee_estimate_delta_jpy: r.mall_fee_estimate_delta_jpy ?? null,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    variable_margin_partial_jpy_incl: r.variable_margin_partial_jpy_incl ?? 0,
    variable_margin_full_jpy_incl: r.variable_margin_full_jpy_incl ?? null,
    refund_adjusted_net_sales_jpy_incl: r.refund_adjusted_net_sales_jpy_incl ?? null,
    margin_confidence: r.margin_confidence ?? 'partial',
    margin_full_finalized_at: r.margin_full_finalized_at ?? null,
    before_discount_jpy_incl: r.before_discount_jpy_incl ?? 0,
    detail_discount_jpy_incl: r.detail_discount_jpy_incl ?? 0,
    charge_allocated_jpy_incl: r.charge_allocated_jpy_incl ?? 0,
    item_option_jpy_incl: r.item_option_jpy_incl ?? 0,
    gift_wrapping_jpy_incl: r.gift_wrapping_jpy_incl ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    price_variance_warning: r.price_variance_warning ?? 0,
    order_count: r.order_count ?? 0,
    line_count: r.line_count ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_rakuten_finance_sku_daily 用、Phase 1a #R-3b)
function normalizeRakutenFinanceRow(r) {
  return {
    date_jst: r.date_jst, rakuten_code: r.rakuten_code,
    ne_code: r.ne_code ?? null,
    sku_resolution: r.sku_resolution,
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_cancelled: r.units_cancelled ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    // Phase 1b 按分関連 (旧 mirror payload なら DEFAULT 0 / 'no_refund')
    allocated_units_cancelled: r.allocated_units_cancelled ?? 0,
    units_cancelled_same_day_matched: r.units_cancelled_same_day_matched ?? 0,
    allocation_method: r.allocation_method ?? 'no_refund',
    cancel_exceeds_ordered_warning: r.cancel_exceeds_ordered_warning ?? 0,
    sales_principal_jpy_incl: r.sales_principal_jpy_incl ?? 0,
    sales_postage_jpy_incl: r.sales_postage_jpy_incl ?? 0,
    coupon_shop_jpy_incl: r.coupon_shop_jpy_incl ?? 0,
    coupon_all_jpy_incl: r.coupon_all_jpy_incl ?? 0,
    promotion_jpy_incl: r.promotion_jpy_incl ?? 0,
    refund_amount_jpy_incl: r.refund_amount_jpy_incl ?? 0,
    allocated_refund_amount_jpy_incl: r.allocated_refund_amount_jpy_incl ?? 0,
    refund_amount_same_day_matched_jpy_incl: r.refund_amount_same_day_matched_jpy_incl ?? 0,
    mall_fee_jpy_incl: r.mall_fee_jpy_incl ?? 0,
    shipping_cost_jpy_incl: r.shipping_cost_jpy_incl ?? 0,
    shipping_quality: r.shipping_quality,
    unit_cost_snapshot_incl: r.unit_cost_snapshot_incl ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference_incl: r.latest_unit_cost_reference_incl ?? null,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl ?? 0,
    gross_sales_jpy_incl: r.gross_sales_jpy_incl ?? 0,
    net_sales_jpy_incl: r.net_sales_jpy_incl ?? 0,
    variable_margin_jpy_incl: r.variable_margin_jpy_incl ?? 0,
    refund_adjusted_net_sales_jpy_incl: r.refund_adjusted_net_sales_jpy_incl ?? 0,
    cost_status: r.cost_status,
    is_cost_complete: r.is_cost_complete ?? 0,
    data_quality_score: r.data_quality_score ?? 0,
    price_variance_warning: r.price_variance_warning ?? 0,
    source_layer_summary: r.source_layer_summary || '',
    source_row_count: r.source_row_count ?? 0,
    built_at: r.built_at || new Date().toISOString(),
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// row 列正規化 (mirror_amazon_finance_sku_daily 用)
function normalizeAmazonFinanceRow(r) {
  return {
    date_jst: r.date_jst, seller_sku: r.seller_sku, asin_norm: r.asin_norm || '',
    product_name: r.product_name || '',
    units_ordered: r.units_ordered ?? 0,
    units_refunded_customer: r.units_refunded_customer ?? 0,
    units_marketplace_guarantee: r.units_marketplace_guarantee ?? 0,
    units_a_to_z_refund: r.units_a_to_z_refund ?? 0,
    units_net_sold: r.units_net_sold ?? 0,
    sales_principal_jpy: r.sales_principal_jpy ?? 0,
    sales_shipping_jpy: r.sales_shipping_jpy ?? 0,
    sales_giftwrap_jpy: r.sales_giftwrap_jpy ?? 0,
    sales_tax_jpy: r.sales_tax_jpy ?? 0,
    commission_jpy: r.commission_jpy ?? 0,
    fba_fulfillment_jpy: r.fba_fulfillment_jpy ?? 0,
    fba_storage_jpy: r.fba_storage_jpy ?? 0,
    closing_fee_jpy: r.closing_fee_jpy ?? 0,
    shipping_chargeback_jpy: r.shipping_chargeback_jpy ?? 0,
    giftwrap_chargeback_jpy: r.giftwrap_chargeback_jpy ?? 0,
    promotion_jpy: r.promotion_jpy ?? 0,
    warehouse_damage_jpy: r.warehouse_damage_jpy ?? 0,
    warehouse_lost_jpy: r.warehouse_lost_jpy ?? 0,
    safe_t_jpy: r.safe_t_jpy ?? 0,
    refund_principal_jpy: r.refund_principal_jpy ?? 0,
    reversal_reimbursement_jpy: r.reversal_reimbursement_jpy ?? 0,
    misc_fee_jpy: r.misc_fee_jpy ?? 0,
    other_fee_jpy: r.other_fee_jpy ?? 0,
    other_amount_jpy: r.other_amount_jpy ?? 0,
    unit_cost_snapshot: r.unit_cost_snapshot ?? null,
    cost_snapshot_date_jst: r.cost_snapshot_date_jst ?? null,
    latest_unit_cost_reference: r.latest_unit_cost_reference ?? null,
    cogs_amount: r.cogs_amount ?? 0,
    profit_amount: r.profit_amount ?? 0,
    is_cost_complete: r.is_cost_complete ?? 0,
    cost_status: r.cost_status,
    source_run_id: r.source_run_id, source_row_hash: r.source_row_hash,
    synced_at: r.synced_at,
  };
}

// ─── MF Phase 1a: row 列正規化 (7 entity 分) ─────────────────────────
function normalizeMfPublishRunRow(r) {
  return {
    run_id: r.run_id, scope: r.scope, status: r.status,
    started_at: r.started_at, finished_at: r.finished_at ?? null,
    error_message: r.error_message ?? null,
    source_run_hash: r.source_run_hash ?? null, synced_at: r.synced_at,
    finalized_at: r.finalized_at ?? null,
  };
}
function normalizeMfExecutiveTopRow(r) {
  return {
    run_id: r.run_id, snapshot_date: r.snapshot_date, current_month_ym: r.current_month_ym,
    sales_mtd_excl_tax: r.sales_mtd_excl_tax ?? 0,
    gross_profit_mtd_excl_tax: r.gross_profit_mtd_excl_tax ?? 0,
    operating_income_mtd_excl_tax: r.operating_income_mtd_excl_tax ?? 0,
    sales_month_end_forecast: r.sales_month_end_forecast ?? 0,
    gross_profit_month_end_forecast: r.gross_profit_month_end_forecast ?? 0,
    operating_income_month_end_forecast: r.operating_income_month_end_forecast ?? 0,
    forecast_status: r.forecast_status || '会計確定待ち',
    yoy_sales_pct: r.yoy_sales_pct ?? null,
    yoy_gross_profit_pct: r.yoy_gross_profit_pct ?? null,
    yoy_operating_income_pct: r.yoy_operating_income_pct ?? null,
    cash_balance_total: r.cash_balance_total ?? 0,
    cash_balance_json: r.cash_balance_json || '[]',
    danger_signals_json: r.danger_signals_json || '[]',
    data_window_from: r.data_window_from ?? null,
    data_window_to: r.data_window_to ?? null,
    reliability_label: r.reliability_label || '業務速報',
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfPlMonthlyRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym, role_key: r.role_key,
    amount_excl_tax: r.amount_excl_tax ?? 0, tax_amount: r.tax_amount ?? 0,
    line_count: r.line_count ?? 0, is_realized_only: r.is_realized_only ?? 1,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfChannelSalesRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    channel_key: r.channel_key, channel_display_name: r.channel_display_name,
    gross_sales_excl_tax: r.gross_sales_excl_tax ?? 0,
    pf_fee_excl_tax: r.pf_fee_excl_tax ?? 0,
    ad_cost_excl_tax: r.ad_cost_excl_tax ?? 0,
    fba_fee_excl_tax: r.fba_fee_excl_tax ?? 0,
    net_sales_after_pf_excl_tax: r.net_sales_after_pf_excl_tax ?? 0,
    unmapped_amount_excl_tax: r.unmapped_amount_excl_tax ?? 0,
    mapping_coverage_pct: r.mapping_coverage_pct ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfCashEventsDailyRow(r) {
  return {
    run_id: r.run_id, movement_date: r.movement_date,
    bank_account_key: r.bank_account_key, direction: r.direction,
    amount_excl_tax: r.amount_excl_tax ?? 0, event_count: r.event_count ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfBalanceSnapshotRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    account_key: r.account_key, account_name: r.account_name,
    sub_account_name: r.sub_account_name ?? null, role_key: r.role_key ?? null,
    closing_balance_excl_tax: r.closing_balance_excl_tax ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
function normalizeMfAnomalySignalsRow(r) {
  return {
    run_id: r.run_id, signal_id: r.signal_id, detected_at: r.detected_at,
    signal_code: r.signal_code, signal_key: r.signal_key ?? null,
    severity: r.severity, severity_rank: r.severity_rank ?? null,
    title: r.title, description: r.description,
    observed_value: r.observed_value ?? null,
    threshold_value: r.threshold_value ?? null,
    related_entity_key: r.related_entity_key ?? null,
    recommended_action: r.recommended_action ?? null,
    source_mart: r.source_mart ?? null,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
// Phase 1d-2: mf_fy_summary
function normalizeMfFySummaryRow(r) {
  return {
    run_id: r.run_id,
    fy_number: r.fy_number,
    fy_start_ym: r.fy_start_ym,
    fy_end_ym: r.fy_end_ym,
    cumulative_through_ym: r.cumulative_through_ym,
    months_in_cumulative: r.months_in_cumulative ?? 0,
    is_fy_completed: r.is_fy_completed ?? 0,
    sales_cum: r.sales_cum ?? 0,
    cogs_cum: r.cogs_cum ?? 0,
    gross_profit_cum: r.gross_profit_cum ?? 0,
    sgae_cum: r.sgae_cum ?? 0,
    operating_income_cum: r.operating_income_cum ?? 0,
    non_op_revenue_cum: r.non_op_revenue_cum ?? 0,
    non_op_expense_cum: r.non_op_expense_cum ?? 0,
    ordinary_income_cum: r.ordinary_income_cum ?? 0,
    personnel_cost_cum: r.personnel_cost_cum ?? 0,
    ar_average: r.ar_average ?? 0,
    inventory_average: r.inventory_average ?? 0,
    ap_average: r.ap_average ?? 0,
    total_asset_average: r.total_asset_average ?? 0,
    ar_opening: r.ar_opening ?? 0,
    ar_closing: r.ar_closing ?? 0,
    inventory_opening: r.inventory_opening ?? 0,
    inventory_closing: r.inventory_closing ?? 0,
    ap_opening: r.ap_opening ?? 0,
    ap_closing: r.ap_closing ?? 0,
    total_asset_opening: r.total_asset_opening ?? 0,
    total_asset_closing: r.total_asset_closing ?? 0,
    cash_closing: r.cash_closing ?? 0,
    short_loan_closing: r.short_loan_closing ?? 0,
    long_loan_closing: r.long_loan_closing ?? 0,
    current_liab_closing: r.current_liab_closing ?? 0,
    total_equity_closing: r.total_equity_closing ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
// Phase 1d-3b: mf_bs_monthly
function normalizeMfBsMonthlyRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    cash_total: r.cash_total ?? 0,
    ar_total: r.ar_total ?? 0,
    inventory_total: r.inventory_total ?? 0,
    other_current_asset: r.other_current_asset ?? 0,
    current_asset_total: r.current_asset_total ?? 0,
    tangible_fixed_asset: r.tangible_fixed_asset ?? 0,
    investment_other: r.investment_other ?? 0,
    fixed_asset_total: r.fixed_asset_total ?? 0,
    total_asset: r.total_asset ?? 0,
    ap_total: r.ap_total ?? 0,
    short_loan_total: r.short_loan_total ?? 0,
    other_current_liab: r.other_current_liab ?? 0,
    current_liab_total: r.current_liab_total ?? 0,
    long_loan_total: r.long_loan_total ?? 0,
    other_fixed_liab: r.other_fixed_liab ?? 0,
    fixed_liab_total: r.fixed_liab_total ?? 0,
    total_liab: r.total_liab ?? 0,
    capital: r.capital ?? 0,
    retained: r.retained ?? 0,
    total_equity: r.total_equity ?? 0,
    bs_other_total: r.bs_other_total ?? 0,
    retained_balance: r.retained_balance ?? 0,
    current_period_profit: r.current_period_profit ?? 0,
    display_total_equity: r.display_total_equity ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}
// Phase 1d-3b: mf_bs_subaccount_monthly
function normalizeMfBsSubaccountRow(r) {
  return {
    run_id: r.run_id, month_ym: r.month_ym,
    account_name: r.account_name,
    sub_account_name: r.sub_account_name ?? '',
    role_key: r.role_key ?? null,
    section: r.section,
    closing_balance_excl_tax: r.closing_balance_excl_tax ?? 0,
    is_hub_null_sub: r.is_hub_null_sub ?? 0,
    source_row_hash: r.source_row_hash, synced_at: r.synced_at,
  };
}

// ─── Phase 1 #1-4a: GET /api/sync/runs/:run_id (status 確認) ───
router.get('/api/sync/runs/:run_id', requireSyncKey, (req, res) => {
  const db = getMirrorDB();
  const chunks = db.prepare(`
    SELECT entity, chunk_index, chunk_count, row_count, payload_checksum,
           received_at, applied_at
    FROM sync_run_chunks WHERE run_id = ? ORDER BY entity, chunk_index
  `).all(req.params.run_id);
  if (chunks.length === 0) return res.status(404).json({ error: 'run not found' });
  const summary = {
    run_id: req.params.run_id,
    chunk_count_expected: chunks[0].chunk_count,
    chunk_count_received: chunks.length,
    row_count_received: chunks.reduce((s, c) => s + c.row_count, 0),
    all_applied: chunks.every(c => c.applied_at !== null),
  };
  res.json({ summary, chunks });
});

// ─── Phase 1 #1-4a: POST /api/sync/runs/:run_id/backout (run_id 単位 DELETE) ───
//
// 安全性ガード (Codex Round 11 指摘 #1): INSERT OR REPLACE で旧 run の値が
// 上書きされている可能性があるため、scope-overlap する後続 run があれば
// 409 で refuse する。?force=1 で明示 override 可能 (運用判断)。
router.post('/api/sync/runs/:run_id/backout', requireSyncKey, (req, res) => {
  const runId = req.params.run_id;
  const force = req.query.force === '1' || req.body?.force === true;
  const db = getMirrorDB();

  // 対象 run の chunk 集約 (entity × scope)
  const targetChunks = db.prepare(`
    SELECT DISTINCT entity, scope_from, scope_to, MAX(received_at) AS max_received_at
    FROM sync_run_chunks WHERE run_id = ?
    GROUP BY entity, scope_from, scope_to
  `).all(runId);
  if (targetChunks.length === 0) return res.status(404).json({ error: 'run not found' });

  // 各 entity-scope について、scope-overlap する後続 run を探す
  const conflicts = [];
  for (const t of targetChunks) {
    const overlap = db.prepare(`
      SELECT DISTINCT run_id, scope_from, scope_to, MAX(received_at) AS max_received_at
      FROM sync_run_chunks
      WHERE entity = ?
        AND run_id <> ?
        AND scope_from <= ?
        AND scope_to   >= ?
        AND received_at > ?
      GROUP BY run_id, scope_from, scope_to
      ORDER BY max_received_at
    `).all(t.entity, runId, t.scope_to, t.scope_from, t.max_received_at);
    for (const o of overlap) conflicts.push({ entity: t.entity, target_scope: [t.scope_from, t.scope_to], conflicting: o });
  }

  if (conflicts.length > 0 && !force) {
    return res.status(409).json({
      error: 'backout_unsafe_overlap',
      message: 'scope-overlap する後続 run があるため backout で旧 run の上書き値を消失させる可能性があります。?force=1 で override 可能',
      run_id: runId,
      conflicts,
    });
  }

  const entities = [...new Set(targetChunks.map(t => t.entity))];
  const deleted = {};
  const tx = db.transaction(() => {
    for (const entity of entities) {
      const cfg = ENTITY_REGISTRY[entity];
      if (cfg) {
        // MF entity (clear_strategy='no_clear') は source_run_id 列を持たないので skip
        // backout は MF 専用 endpoint (Phase 2 で追加予定) で対応
        if (cfg.clear_strategy === 'no_clear') {
          deleted[entity] = 0;
          continue;
        }
        const info = db.prepare(`
          DELETE FROM ${cfg.mirror_table} WHERE source_run_id = ?
        `).run(runId);
        deleted[entity] = info.changes;
      }
    }
    db.prepare(`DELETE FROM sync_run_chunks WHERE run_id = ?`).run(runId);
  });
  tx();

  console.log(`[Mirror] backout run=${runId} deleted=${JSON.stringify(deleted)} force=${force} conflicts=${conflicts.length}`);
  res.json({ ok: true, run_id: runId, deleted, force, conflicts_overridden: conflicts.length });
});

// ─── Phase 1 #1-7: POST /api/sync/runs/:run_id/rebuild-marts (downstream mart rebuild trigger) ───
//
// 現状 noop: ledger 確認 + ログだけ (Phase 2 で mart_amazon_sku_* を実装する際に中身を実装)。
// 完了判定は Render 側の sync_run_chunks のみで行う (sync_runs テーブルは miniPC 側のみ
// で Render mirror には存在しない、Codex Round 1 #medium-1 対応):
//   - run_id 存在
//   - chunk 数 = chunk_count (received_count 一致)
//   - chunk_index が 0..chunk_count-1 連続 (欠番なし、Codex Round 1 #medium-2 対応)
//   - 全 chunk の applied_at が NOT NULL
// 将来 Phase 2 で mart 実装時、ここで:
//   - 該当 entity の mart を rebuild (例: mart_amazon_sku_daily, mart_amazon_sku_monthly)
//   - rebuild 結果を sync_run に紐付けて記録
// 失敗しても呼び出し元の sync 結果には影響させない (warn のみ) 設計の明示。
router.post('/api/sync/runs/:run_id/rebuild-marts', requireSyncKey, (req, res) => {
  const runId = req.params.run_id;
  const db = getMirrorDB();

  // ledger 確認 (run_id 存在 + 全 chunk applied + chunk_index 連続)
  const chunks = db.prepare(`
    SELECT entity, chunk_index, chunk_count, applied_at
    FROM sync_run_chunks WHERE run_id = ? ORDER BY entity, chunk_index
  `).all(runId);
  if (chunks.length === 0) {
    return res.status(404).json({ error: 'run not found', run_id: runId });
  }
  const entitiesByName = {};
  for (const c of chunks) {
    if (!entitiesByName[c.entity]) entitiesByName[c.entity] = { chunks: [], expectedCount: c.chunk_count };
    entitiesByName[c.entity].chunks.push(c);
  }
  const incomplete = [];
  for (const [entity, info] of Object.entries(entitiesByName)) {
    const allApplied = info.chunks.every(c => c.applied_at !== null);
    const presentSet = new Set(info.chunks.map(c => c.chunk_index));
    const missingIndexes = [];
    for (let i = 0; i < info.expectedCount; i++) {
      if (!presentSet.has(i)) missingIndexes.push(i);
    }
    if (!allApplied || info.chunks.length !== info.expectedCount || missingIndexes.length > 0) {
      incomplete.push({
        entity,
        received: info.chunks.length,
        expected: info.expectedCount,
        missing_chunks: missingIndexes,
        all_applied: allApplied,
      });
    }
  }
  if (incomplete.length > 0) {
    return res.status(409).json({
      error: 'rebuild_blocked_incomplete_sync',
      message: 'sync が未完了の entity が含まれているため rebuild を保留',
      run_id: runId,
      incomplete,
    });
  }

  // 現状 noop (Phase 2 で実装、Phase 1 では trigger 経路の確保のみ)
  const triggered = Object.keys(entitiesByName);
  console.log(`[Mirror] rebuild-marts triggered (noop) run=${runId} entities=${triggered.join(',')}`);
  res.json({
    ok: true,
    run_id: runId,
    triggered_entities: triggered,
    rebuilt: [],
    note: 'Phase 1 #1-7: rebuild trigger 経路のみ整備、mart 実装は Phase 2 で追加',
  });
});

// ─── MF Phase 1a: POST /api/sync/mf/runs/:run_id/finalize ─────────────────
// 全 entity の chunk 受信完了を検証し、mirror_mf_publish_runs.status を
// 'pending_sync' → 'success' に flip。VIEW v_mirror_mf_*_latest がこの瞬間に活性化。
//
// body: { entity_run_ids: { mf_publish_runs: 'sync_run_id...', mf_executive_top: '...', ... } }
// idempotent: status='success' 既にだったら 200 で no-op
const MF_REQUIRED_ENTITIES = [
  'mf_publish_runs', 'mf_executive_top', 'mf_pl_monthly', 'mf_channel_sales',
  'mf_cash_events_daily', 'mf_balance_snapshot_monthly', 'mf_anomaly_signals',
];
// Phase 1d-2: optional entity (旧 sync (deploy 前) との互換のため、欠けても finalize 通過)
//   将来 (Phase 1d-3 以降で sync 側完全移行後) に MF_REQUIRED_ENTITIES へ昇格予定
const MF_OPTIONAL_ENTITIES = ['mf_fy_summary', 'mf_bs_monthly', 'mf_bs_subaccount_monthly'];
router.post('/api/sync/mf/runs/:run_id/finalize', requireSyncKey, (req, res) => {
  const runId = parseInt(req.params.run_id, 10);
  if (!Number.isInteger(runId) || runId <= 0) {
    return res.status(400).json({ error: 'run_id must be positive integer' });
  }
  const body = req.body || {};
  const entityRunIds = body.entity_run_ids || {};
  const requestId = `mf-finalize-${Date.now().toString(36)}`;
  const db = getMirrorDB();

  const parent = db.prepare(`SELECT run_id, status, scope FROM mirror_mf_publish_runs WHERE run_id = ?`).get(runId);
  if (!parent) {
    return res.status(404).json({ error: 'parent_run_not_found', run_id: runId, request_id: requestId });
  }
  if (parent.status === 'success') {
    console.log(`[Mirror] mf finalize idempotent (already success) req=${requestId} run=${runId}`);
    return res.json({ ok: true, run_id: runId, status: 'success', already_finalized: true, request_id: requestId });
  }
  if (parent.status !== 'pending_sync') {
    return res.status(409).json({
      error: 'invalid_status_for_finalize',
      message: `run_id=${runId} status=${parent.status}, finalize is only valid from 'pending_sync'`,
      request_id: requestId,
    });
  }

  // 検証 + status flip を tx 内で atomic に (Codex review #88: race 防止)
  const incomplete = [];
  let updatedChanges = 0;
  let alreadyFinalized = false;
  const now = new Date().toISOString();

  try {
    db.transaction(() => {
      // tx 内で再 status check (BEGIN IMMEDIATE で書き込み直列化、他の chunk INSERT が完了済を保証)
      const parent2 = db.prepare(`SELECT run_id, status FROM mirror_mf_publish_runs WHERE run_id = ?`).get(runId);
      if (!parent2) {
        throw new HttpError(404, { error: 'parent_run_not_found', run_id: runId });
      }
      if (parent2.status === 'success') {
        alreadyFinalized = true;
        return;
      }
      if (parent2.status !== 'pending_sync') {
        throw new HttpError(409, {
          error: 'invalid_status_for_finalize',
          message: `run_id=${runId} status=${parent2.status}`,
        });
      }

      // Phase 1d-2: required + optional 両方 validate (optional は欠けても通過)
      const allEntitiesToCheck = [
        ...MF_REQUIRED_ENTITIES.map(e => ({ entity: e, optional: false })),
        ...MF_OPTIONAL_ENTITIES.map(e => ({ entity: e, optional: true })),
      ];
      for (const { entity, optional } of allEntitiesToCheck) {
        const syncRunId = entityRunIds[entity];
        if (!syncRunId || typeof syncRunId !== 'string') {
          if (optional) {
            console.log(`[Mirror] mf finalize req=${requestId} run=${runId}: optional entity '${entity}' not provided, skipping`);
            continue;
          }
          incomplete.push({ entity, reason: 'missing_entity_run_id_in_request_body' });
          continue;
        }
        const chunks = db.prepare(`
          SELECT chunk_index, chunk_count, applied_at, mf_source_run_id
          FROM sync_run_chunks WHERE run_id = ? AND entity = ?
          ORDER BY chunk_index
        `).all(syncRunId, entity);
        if (chunks.length === 0) {
          if (optional) {
            console.log(`[Mirror] mf finalize req=${requestId} run=${runId}: optional entity '${entity}' has no chunks, skipping`);
            continue;
          }
          incomplete.push({ entity, sync_run_id: syncRunId, reason: 'no_chunks_received' });
          continue;
        }
        // Codex Medium fix: chunks の mf_source_run_id が finalize の run_id と一致するか cross-check
        const mismatched = chunks.find(c => c.mf_source_run_id !== runId);
        if (mismatched) {
          incomplete.push({
            entity, sync_run_id: syncRunId,
            reason: 'mf_source_run_id_mismatch',
            expected: runId, got: mismatched.mf_source_run_id,
          });
          continue;
        }
        const expected = chunks[0].chunk_count;
        const applied = chunks.every(c => c.applied_at !== null);
        const indexes = chunks.map(c => c.chunk_index);
        const missing = [];
        for (let i = 0; i < expected; i++) if (!indexes.includes(i)) missing.push(i);
        if (chunks.length !== expected || missing.length > 0 || !applied) {
          incomplete.push({
            entity, sync_run_id: syncRunId,
            received: chunks.length, expected,
            missing_chunks: missing, all_applied: applied,
          });
        }
      }
      if (incomplete.length > 0) {
        throw new HttpError(409, {
          error: 'finalize_blocked_incomplete',
          message: '一部 entity の sync が未完了 / cross-check 失敗のため finalize を拒否',
          run_id: runId, incomplete,
        });
      }

      // 全検証 PASS、status を atomic flip
      updatedChanges = db.prepare(`
        UPDATE mirror_mf_publish_runs
           SET status = 'success', finalized_at = ?
         WHERE run_id = ? AND status = 'pending_sync'
      `).run(now, runId).changes;
    }).immediate();
  } catch (e) {
    if (e instanceof HttpError) {
      return res.status(e.status).json({ ...e.body, request_id: requestId });
    }
    console.error(`[Mirror] mf finalize error req=${requestId}: ${e.message}`);
    return res.status(500).json({ error: e.message, request_id: requestId });
  }

  if (alreadyFinalized) {
    console.log(`[Mirror] mf finalize idempotent (already success) req=${requestId} run=${runId}`);
    return res.json({ ok: true, run_id: runId, status: 'success', already_finalized: true, request_id: requestId });
  }
  if (updatedChanges !== 1) {
    return res.status(500).json({
      error: 'finalize_update_failed',
      run_id: runId, request_id: requestId,
    });
  }

  console.log(`[Mirror] mf finalize success req=${requestId} run=${runId} scope=${parent.scope}`);
  res.json({ ok: true, run_id: runId, status: 'success', finalized_at: now, request_id: requestId });
});

// ─── GET /api/products ───

router.get('/api/products', (req, res) => {
  const db = getMirrorDB();
  const { search, status, type, limit = '100', offset = '0' } = req.query;
  let sql = 'SELECT * FROM mirror_products WHERE 1=1';
  const params = [];
  if (search) { sql += ' AND (商品コード LIKE ? OR 商品名 LIKE ?)'; const t = `%${search}%`; params.push(t, t); }
  if (status) { sql += ' AND 取扱区分 = ?'; params.push(status); }
  if (type) { sql += ' AND 商品区分 = ?'; params.push(type); }
  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as cnt');
  const total = db.prepare(countSql).get(...params)?.cnt || 0;
  sql += ' ORDER BY 商品コード LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));
  res.json({ rows: db.prepare(sql).all(...params), total });
});

// ─── GET /api/sales/monthly ───

router.get('/api/sales/monthly', (req, res) => {
  const db = getMirrorDB();
  const { product, mall, type, months = '6' } = req.query;
  let sql = 'SELECT * FROM mirror_sales_monthly WHERE 1=1';
  const params = [];
  if (product) { sql += ' AND 商品コード LIKE ?'; params.push(`%${product}%`); }
  if (mall) { sql += ' AND モール = ?'; params.push(mall); }
  if (type) { sql += ' AND データ種別 = ?'; params.push(type); }
  sql += ' ORDER BY 月 DESC, 数量 DESC LIMIT 1000';
  res.json(db.prepare(sql).all(...params));
});

// ─── GET /api/sales/daily ───

router.get('/api/sales/daily', (req, res) => {
  const db = getMirrorDB();
  const { product, mall, type, days = '30' } = req.query;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - parseInt(days));
  let sql = 'SELECT * FROM mirror_sales_daily WHERE 日付 >= ?';
  const params = [cutoff.toISOString().slice(0, 10)];
  if (product) { sql += ' AND 商品コード LIKE ?'; params.push(`%${product}%`); }
  if (mall) { sql += ' AND モール = ?'; params.push(mall); }
  if (type) { sql += ' AND データ種別 = ?'; params.push(type); }
  sql += ' ORDER BY 日付 DESC, 数量 DESC LIMIT 5000';
  res.json(db.prepare(sql).all(...params));
});

// ─── GET /api/status ───

router.get('/api/status', (req, res) => {
  const db = getMirrorDB();
  const status = {};
  try {
    for (const r of db.prepare('SELECT key, value, updated_at FROM mirror_sync_status').all()) {
      status[r.key] = { value: r.value, updated_at: r.updated_at };
    }
    status.products_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_products').get().cnt;
    status.sales_monthly_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_sales_monthly').get().cnt;
    status.sales_daily_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_sales_daily').get().cnt;
    try { status.amazon_sku_fees_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees').get().cnt; } catch { status.amazon_sku_fees_count = 0; }
    try { status.rakuten_sku_map_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_rakuten_sku_map').get().cnt; } catch { status.rakuten_sku_map_count = 0; }
    // Codex PR2a Round 4 非ブロッカー #3 反映: stock_snapshot 件数も同期検証に乗せる
    try { status.stock_snapshot_count = db.prepare('SELECT COUNT(*) as cnt FROM mirror_stock_monthly_snapshot').get().cnt; } catch { status.stock_snapshot_count = 0; }
    try {
      const r = db.prepare(`SELECT
        COUNT(*) AS cnt,
        SUM(CASE WHEN source='master' THEN 1 ELSE 0 END) AS master_cnt,
        SUM(CASE WHEN source='auto'   THEN 1 ELSE 0 END) AS auto_cnt
        FROM mirror_sku_resolved`).get();
      status.sku_resolved_count = r.cnt;
      status.sku_resolved_master_count = r.master_cnt ?? 0;
      status.sku_resolved_auto_count = r.auto_cnt ?? 0;
    } catch {
      status.sku_resolved_count = 0;
    }

    // inv_daily_summary (PR-C 日次在庫スナップショット)
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MAX(business_date) AS latest_date
        FROM mirror_inv_daily_summary
      `).get();
      status.inv_daily_summary_count = r.cnt;
      status.inv_daily_summary_latest_date = r.latest_date;
    } catch {
      status.inv_daily_summary_count = 0;
    }
    // inv_daily_detail (D-1c 詳細層)
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MIN(business_date) AS oldest, MAX(business_date) AS latest
        FROM mirror_inv_daily_detail
      `).get();
      status.inv_daily_detail_count = r.cnt;
      status.inv_daily_detail_oldest_date = r.oldest;
      status.inv_daily_detail_latest_date = r.latest;
    } catch {
      status.inv_daily_detail_count = 0;
    }
    // mirror_sku_master (商品コード変換テーブル差分検出用)
    try {
      const r = db.prepare(`
        SELECT COUNT(*) AS cnt, MAX(source_updated_at) AS latest, MAX(synced_at) AS synced
        FROM mirror_sku_master
      `).get();
      status.sku_master_count = r.cnt;
      status.sku_master_latest_updated_at = r.latest;
      status.sku_master_synced_at = r.synced;
    } catch {
      status.sku_master_count = 0;
    }
  } catch {}
  res.json(status);
});

// ─── GET /api/sku-master/recent-missing-candidates ───
// マスタ登録ツール (m_sku_master) に登録済みで、Google Sheets「商品コード変換テーブル」
// にまだ載っていない SKU を、Sheets 側 GAS から日次でチェックするための専用 endpoint。
//
// 認証: x-read-token (MIRROR_READ_TOKEN 環境変数。Render env にのみ置く read-only token)
//   ・WAREHOUSE_API_KEY (miniPC の master write 権限キー) とは完全分離
//   ・キー未設定なら 503 (fail-closed)
//
// レスポンス:
//   {
//     since_days: 7,
//     server_time_utc: 'YYYY-MM-DDTHH:MM:SS.fffZ',
//     mirror_last_synced_at: 'YYYY-MM-DD HH:MM:SS' | null,    // mirror_sync_status.last_sync
//     mirror_sku_master_synced_at: 'YYYY-MM-DD HH:MM:SS' | null, // 当テーブルの最新 synced_at
//     count: <number>,
//     items: [ { seller_sku, 商品名, source_updated_at: 'ISO 8601 Z' }, ... ]
//   }
//
// 仕様メモ (Codex review 2026-05-13 反映):
//   ・since_days は固定 7、パラメータ受け付けない (誤用防止)
//   ・GAS 側はレスポンスの mirror_last_synced_at が当日かを必ず検証 (古ければ処理停止)
//   ・Cache-Control: no-store
//   ・件数 0 でも 200 で返す (差分判定は呼び出し側)
function requireReadToken(req, res, next) {
  const token = process.env.MIRROR_READ_TOKEN;
  if (!token) {
    return res.status(503).json({ error: 'mirror_read_token_unset' });
  }
  // header only (Codex review 2026-05-13 medium #3): query fallback は URL / アクセスログ /
  // 監視 / 例外メッセージ / ブラウザ履歴に token を残すため受け付けない。
  const provided = req.headers['x-read-token'];
  if (!provided || provided !== token) {
    return res.status(401).json({ error: 'invalid_read_token' });
  }
  next();
}

router.get('/api/sku-master/recent-missing-candidates', requireReadToken, (req, res) => {
  const db = getMirrorDB();
  const SINCE_DAYS = 7; // 固定 (Codex review #2: パラメータ受け付けない)

  try {
    // mirror_sku_master.source_updated_at は 'YYYY-MM-DDTHH:MM:SS.fffZ' (UTC ISO 8601、m_sku_master.updated_at 素通し)
    // 'datetime' 関数は ISO 8601 を解釈できるので直接比較可
    const items = db.prepare(`
      SELECT seller_sku, 商品名, source_updated_at
      FROM mirror_sku_master
      WHERE source_updated_at IS NOT NULL
        AND source_updated_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-7 days')
      ORDER BY source_updated_at DESC, seller_sku
    `).all();

    // mirror freshness 情報 (GAS 側で stale 判定に使う)
    let mirrorLastSync = null;
    let masterSyncedAt = null;
    try {
      const r = db.prepare(`SELECT value FROM mirror_sync_status WHERE key='last_sync'`).get();
      mirrorLastSync = r?.value ?? null;
    } catch {}
    try {
      const r = db.prepare(`SELECT MAX(synced_at) AS s FROM mirror_sku_master`).get();
      masterSyncedAt = r?.s ?? null;
    } catch {}

    res.setHeader('Cache-Control', 'no-store');
    res.json({
      since_days: SINCE_DAYS,
      server_time_utc: new Date().toISOString(),
      mirror_last_synced_at: mirrorLastSync,
      mirror_sku_master_synced_at: masterSyncedAt,
      count: items.length,
      items,
    });
  } catch (e) {
    console.error('[sku-master/recent-missing-candidates] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── GET /api/download/:table ───
// CSVダウンロード

router.get('/api/download/:table', (req, res) => {
  const db = getMirrorDB();
  const table = req.params.table;
  const allowed = ['products', 'set_components', 'sales_monthly', 'sales_daily'];
  if (!allowed.includes(table)) return res.status(400).json({ error: '無効なテーブル名' });

  const tableName = 'mirror_' + table;
  const rows = db.prepare(`SELECT * FROM ${tableName}`).all();
  if (!rows.length) return res.status(404).json({ error: 'データなし' });

  const headers = Object.keys(rows[0]);
  const escapeCsv = v => { const s = String(v ?? ''); return s.includes(',') || s.includes('"') || s.includes('\n') ? '"' + s.replace(/"/g, '""') + '"' : s; };
  const lines = [headers.join(','), ...rows.map(r => headers.map(h => escapeCsv(r[h])).join(','))];

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="mirror_${table}.csv"`);
  res.send('\uFEFF' + lines.join('\r\n'));
});

export default router;
