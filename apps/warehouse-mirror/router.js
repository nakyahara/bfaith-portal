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
const SUPPORTED_ENTITIES = new Set(['amazon_finance_sku_daily']);
// entity 別の期待 contract_version (Codex Round 12 #5)
const ENTITY_CONTRACT_VERSION = {
  amazon_finance_sku_daily: 1,
};

// Insert/ledger statement キャッシュ (DB 接続単位、Codex Round 13 #2)
// モジュール変数だと DB 切替時に古い connection の statement を再利用してしまうため WeakMap
const _stmtCache = new WeakMap();  // db => { amazonFinanceInsert, ledgerInsert }
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
function getLedgerInsert(db) {
  const b = getStmtBundle(db);
  if (!b.ledgerInsert) {
    b.ledgerInsert = db.prepare(`
      INSERT INTO sync_run_chunks
        (run_id, entity, chunk_index, chunk_count, row_count, payload_checksum,
         contract_version, scope_from, scope_to, received_at, applied_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
  }
  return b.ledgerInsert;
}

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
  if (!SUPPORTED_ENTITIES.has(entity)) {
    return res.status(400).json({ error: `unsupported entity: ${entity}` });
  }
  const expectedVer = ENTITY_CONTRACT_VERSION[entity];
  if (contract_version !== expectedVer) {
    return res.status(400).json({
      error: 'contract_version_mismatch',
      message: `entity=${entity} requires contract_version=${expectedVer} (got ${contract_version})`,
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
  if (is_first && entity === 'amazon_finance_sku_daily') {
    clearDates = meta.clear_amazon_finance_dates;
    if (!Array.isArray(clearDates) || clearDates.length === 0) {
      return res.status(400).json({ error: 'first chunk requires meta.clear_amazon_finance_dates (non-empty array)' });
    }
    for (const d of clearDates) {
      if (!DATE_RE.test(d)) return res.status(400).json({ error: `invalid date in clear list: ${d}` });
    }
  }

  // ────────── 4. tx 実行 (BEGIN IMMEDIATE で write 直列化、Codex Round 12 #1 #3) ──────────
  const db = getMirrorDB();
  const now = new Date().toISOString();
  const requestId = `sync-${Date.now().toString(36)}`;
  const insertStmt = getAmazonFinanceInsert(db);
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
      let didClear = false;
      let clearedRows = 0;
      if (is_first) {
        const priorCount = db.prepare(`
          SELECT COUNT(*) AS c FROM sync_run_chunks WHERE run_id = ? AND entity = ?
        `).get(sync_run_id, entity).c;
        if (priorCount === 0) {
          const placeholders = clearDates.map(() => '?').join(',');
          clearedRows = db.prepare(`
            DELETE FROM mirror_amazon_finance_sku_daily WHERE date_jst IN (${placeholders})
          `).run(...clearDates).changes;
          didClear = true;
        }
      }

      // 4d. row INSERT
      for (const r of rows) {
        insertStmt.run(normalizeAmazonFinanceRow(r));
      }

      // 4e. ledger insert
      insertLedger.run(sync_run_id, entity, chunk_index, chunk_count, row_count,
                       payload_checksum, contract_version, scope_from, scope_to, now, now);

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
      if (entity === 'amazon_finance_sku_daily') {
        const info = db.prepare(`
          DELETE FROM mirror_amazon_finance_sku_daily WHERE source_run_id = ?
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
  } catch {}
  res.json(status);
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
