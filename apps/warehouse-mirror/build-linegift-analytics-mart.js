#!/usr/bin/env node
/**
 * build-linegift-analytics-mart.js — LINEギフト v1.0 Commit 3
 *   全 analytics mart を同一 as_of_date_jst で 1 transaction 原子化 build
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §6
 * Codex v1.0 Round 2 Critical 2 (atomic publish + rollback)
 *
 * 入力 (Render warehouse-mirror.db、miniPC からの mirror sync 由来):
 *   mirror_linegift_finance_sku_daily  (= miniPC f_linegift_finance_sku_daily_v1 の mirror)
 *
 * 出力 (Render 派生 mart):
 *   mart_linegift_kpi_summary_daily       (7/28/90 期間別)
 *   mart_linegift_sku_perf_daily          (期間別 SKU 集計)
 *   mart_linegift_price_band_summary_daily (8段階 価格帯別)
 *
 * 排他制御: mart_build_locks (acquireLock/releaseLock + heartbeat、Commit 2)
 *
 * 原子性: 全 mart を 1 transaction で書く (window 別分割しない、partial publish 防止)
 *   失敗時は SQLite が自動 rollback + lock を status='FAILED' に
 *
 * 使い方:
 *   node apps/warehouse-mirror/build-linegift-analytics-mart.js              実行
 *   node apps/warehouse-mirror/build-linegift-analytics-mart.js --dry-run    集計のみ、DB 書込なし
 *   node apps/warehouse-mirror/build-linegift-analytics-mart.js --windows=90 90 のみ
 *   node apps/warehouse-mirror/build-linegift-analytics-mart.js --as-of=2026-05-27  as_of 上書き
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initMirrorDB, getMirrorDB } from './db.js';
import {
  acquireLock, releaseLock, updateHeartbeat, nowJstIso,
} from './build-lock.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');

const SQL_FILE = 'sql/linegift-analytics/mart_analytics_marts.sql';
const LOCK_NAME = 'build-linegift-analytics-mart';
const SUPPORTED_WINDOWS = [7, 28, 90];

function jstWallclock() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

function todayJstDate() {
  const j = jstWallclock();
  return `${j.getUTCFullYear()}-${String(j.getUTCMonth() + 1).padStart(2,'0')}-${String(j.getUTCDate()).padStart(2,'0')}`;
}

function buildPriceBandResolver(db) {
  const bands = db.prepare(`
    SELECT band_code, min_price_jpy_incl, max_price_jpy_incl
    FROM mart_price_bands
    WHERE is_active = 1 AND valid_to IS NULL
    ORDER BY sort_order
  `).all();
  return (priceRaw) => {
    if (priceRaw == null || Number.isNaN(priceRaw)) return null;
    for (const b of bands) {
      const ok = priceRaw >= b.min_price_jpy_incl &&
                 (b.max_price_jpy_incl == null || priceRaw <= b.max_price_jpy_incl);
      if (ok) return b.band_code;
    }
    return null;
  };
}

/**
 * 1) mart_linegift_sku_perf_daily を build
 * Codex Round 1 A-5c High 反映: 厳密 N 日窓 (start = as_of - (N - 1))
 */
function buildSkuPerf(db, asOf, windowDays, syncedAt) {
  const startOffset = windowDays - 1;
  // 2026-05-27 中原さん指摘で送料引き算を追加:
  //   LINEギフトは表示送料無料 (送料込み価格) だが、実態は自社が送料を負担している。
  //   miniPC の variable_margin_jpy_incl は送料 0 で計算済 (設計書 v0.5 §4-2 の旧前提)。
  //   Render 側で mirror_products.送料 × units を引いて powr"想定送料控除後"の粗利に直す。
  //   Amazon FBM (feedback_amazon_fbm_easyship.md) と同じパターン。
  //
  // shipping_cost_jpy_incl = SUM(units_net_sold) × mirror_products.送料
  //   (mirror_products.送料 は product_shipping から反映、税扱いは原価と同じ慣習で × (1 + 消費税率) で税込換算)
  // gross_profit_jpy_incl = variable_margin_jpy_incl - shipping_cost_jpy_incl
  const sql = `
    INSERT OR REPLACE INTO mart_linegift_sku_perf_daily (
      as_of_date_jst, window_days, ne_code, sku_code, product_name,
      units, sales_amount_jpy_incl, gross_profit_jpy_incl, orders, active_days,
      velocity_per_active_day, velocity_per_calendar_day,
      unit_price_jpy_incl_raw, unit_price_jpy_incl_display,
      gross_margin_rate, synced_at
    )
    WITH base AS (
      SELECT f.date_jst, f.sku_code, f.ne_code, f.product_name,
             f.units_net_sold, f.gross_sales_jpy_incl,
             f.variable_margin_jpy_incl, f.order_count,
             -- mirror_products.送料 (税抜) を税込換算 (LINEギフト売価が税込のため整合)。消費税率不明なら 0.1 を既定
             COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1)) AS unit_shipping_cost_jpy_incl
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst >= date(?, '-' || ? || ' days')
        AND f.date_jst <= ?
    )
    SELECT
      ? AS as_of_date_jst,
      ? AS window_days,
      b.ne_code, b.sku_code,
      MAX(b.product_name) AS product_name,
      COALESCE(SUM(b.units_net_sold), 0) AS units,
      ROUND(COALESCE(SUM(b.gross_sales_jpy_incl), 0)) AS sales_amount_jpy_incl,
      -- 粗利 = miniPC variable_margin - 想定送料 (units × unit_shipping_cost)
      ROUND(COALESCE(SUM(b.variable_margin_jpy_incl), 0) - COALESCE(SUM(b.units_net_sold * b.unit_shipping_cost_jpy_incl), 0)) AS gross_profit_jpy_incl,
      COALESCE(SUM(b.order_count), 0) AS orders,
      COUNT(DISTINCT b.date_jst) AS active_days,
      CASE WHEN COUNT(DISTINCT b.date_jst) = 0 THEN 0.0
           ELSE 1.0 * COALESCE(SUM(b.units_net_sold), 0) / COUNT(DISTINCT b.date_jst) END AS velocity_per_active_day,
      1.0 * COALESCE(SUM(b.units_net_sold), 0) / ? AS velocity_per_calendar_day,
      CASE WHEN COALESCE(SUM(b.units_net_sold), 0) = 0 THEN 0.0
           ELSE 1.0 * COALESCE(SUM(b.gross_sales_jpy_incl), 0) / COALESCE(SUM(b.units_net_sold), 0) END AS unit_price_jpy_incl_raw,
      CASE WHEN COALESCE(SUM(b.units_net_sold), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(b.gross_sales_jpy_incl), 0) * 1.0 / COALESCE(SUM(b.units_net_sold), 0)) END AS unit_price_jpy_incl_display,
      -- 粗利率 = 送料引き後粗利 / 売上
      CASE WHEN COALESCE(SUM(b.gross_sales_jpy_incl), 0) = 0 THEN 0
           ELSE ROUND((COALESCE(SUM(b.variable_margin_jpy_incl), 0) - COALESCE(SUM(b.units_net_sold * b.unit_shipping_cost_jpy_incl), 0)) * 1.0 / COALESCE(SUM(b.gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate,
      ? AS synced_at
    FROM base b
    GROUP BY b.ne_code, b.sku_code
  `;
  const info = db.prepare(sql).run(asOf, startOffset, asOf, asOf, windowDays, windowDays, syncedAt);
  return info.changes;
}

/**
 * 2) mart_linegift_kpi_summary_daily を build
 */
function buildKpiSummary(db, asOf, windowDays, syncedAt) {
  const startOffset = windowDays - 1;
  // 送料引き算 (sku_perf と同じパターン、mirror_products.送料 × units を粗利から引く)
  const sql = `
    INSERT OR REPLACE INTO mart_linegift_kpi_summary_daily (
      as_of_date_jst, window_days,
      total_sales_jpy_incl, total_orders, avg_order_value_jpy_incl,
      total_gross_profit_jpy_incl, gross_margin_rate, synced_at
    )
    WITH base AS (
      SELECT f.gross_sales_jpy_incl, f.variable_margin_jpy_incl, f.order_count, f.units_net_sold,
             COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1)) AS unit_shipping_cost_jpy_incl
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst >= date(?, '-' || ? || ' days')
        AND f.date_jst <= ?
    )
    SELECT
      ? AS as_of_date_jst,
      ? AS window_days,
      ROUND(COALESCE(SUM(gross_sales_jpy_incl), 0)) AS total_sales_jpy_incl,
      COALESCE(SUM(order_count), 0) AS total_orders,
      CASE WHEN COALESCE(SUM(order_count), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(gross_sales_jpy_incl), 0) / COALESCE(SUM(order_count), 0)) END AS avg_order_value_jpy_incl,
      ROUND(COALESCE(SUM(variable_margin_jpy_incl), 0) - COALESCE(SUM(units_net_sold * unit_shipping_cost_jpy_incl), 0)) AS total_gross_profit_jpy_incl,
      CASE WHEN COALESCE(SUM(gross_sales_jpy_incl), 0) = 0 THEN 0
           ELSE ROUND((COALESCE(SUM(variable_margin_jpy_incl), 0) - COALESCE(SUM(units_net_sold * unit_shipping_cost_jpy_incl), 0)) * 1.0 / COALESCE(SUM(gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate,
      ? AS synced_at
    FROM base
  `;
  // bind: [as_of(WHERE start), startOffset, as_of(WHERE end), as_of(SELECT), windowDays(SELECT), syncedAt]
  const info = db.prepare(sql).run(asOf, startOffset, asOf, asOf, windowDays, syncedAt);
  return info.changes;
}

/**
 * 3) mart_linegift_price_band_summary_daily を build
 * 既存 mart_linegift_sku_perf_daily を参照して band 派生 + 集計
 */
function buildPriceBandSummary(db, asOf, windowDays, syncedAt) {
  const resolveBand = buildPriceBandResolver(db);
  const bands = db.prepare(`
    SELECT band_code, band_label, sort_order
    FROM mart_price_bands
    WHERE is_active = 1 AND valid_to IS NULL
    ORDER BY sort_order
  `).all();

  const rows = db.prepare(`
    SELECT ne_code, sku_code, units, sales_amount_jpy_incl, gross_profit_jpy_incl, orders, unit_price_jpy_incl_raw
    FROM mart_linegift_sku_perf_daily
    WHERE as_of_date_jst = ? AND window_days = ?
  `).all(asOf, windowDays);

  const agg = new Map();
  for (const b of bands) {
    agg.set(b.band_code, { sku_count: 0, orders_count: 0, sales_jpy_incl: 0, gross_profit_jpy_incl: 0 });
  }
  for (const r of rows) {
    const band = resolveBand(Number(r.unit_price_jpy_incl_raw));
    if (!band || !agg.has(band)) continue;
    const a = agg.get(band);
    a.sku_count += 1;
    a.orders_count += Number(r.orders) || 0;
    a.sales_jpy_incl += Number(r.sales_amount_jpy_incl) || 0;
    a.gross_profit_jpy_incl += Number(r.gross_profit_jpy_incl) || 0;
  }

  db.prepare(`DELETE FROM mart_linegift_price_band_summary_daily WHERE as_of_date_jst=? AND window_days=?`).run(asOf, windowDays);

  const insertStmt = db.prepare(`
    INSERT INTO mart_linegift_price_band_summary_daily (
      as_of_date_jst, window_days, price_band_code_sales, band_label, sort_order,
      sku_count, orders_count, sales_jpy_incl, gross_profit_jpy_incl, gross_margin_rate, synced_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  for (const b of bands) {
    const a = agg.get(b.band_code);
    const gmr = a.sales_jpy_incl === 0 ? 0 : Math.round(a.gross_profit_jpy_incl / a.sales_jpy_incl * 10000) / 10000;
    insertStmt.run(
      asOf, windowDays, b.band_code, b.band_label, b.sort_order,
      a.sku_count, a.orders_count, a.sales_jpy_incl, a.gross_profit_jpy_incl, gmr, syncedAt
    );
    inserted += 1;
  }
  return inserted;
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const winArg = args.find((a) => a.startsWith('--windows='));
  const asOfArg = args.find((a) => a.startsWith('--as-of='));

  const windows = winArg
    ? winArg.split('=')[1].split(',').map((s) => Number(s.trim())).filter((n) => SUPPORTED_WINDOWS.includes(n))
    : SUPPORTED_WINDOWS;
  const asOf = asOfArg ? asOfArg.split('=')[1] : todayJstDate();

  if (!/^\d{4}-\d{2}-\d{2}$/.test(asOf)) {
    console.error(`[build-linegift-analytics-mart] FATAL: invalid as_of=${asOf}`);
    process.exit(1);
  }

  console.log(`[build-linegift-analytics-mart] start (dryRun=${dryRun}, as_of=${asOf}, windows=${windows.join(',')})`);

  initMirrorDB();
  const db = getMirrorDB();

  // schema migration が走っていない場合は失敗させる (依存関係明示)
  for (const t of ['mirror_linegift_finance_sku_daily', 'mart_price_bands', 'mart_linegift_kpi_summary_daily',
                   'mart_linegift_sku_perf_daily', 'mart_linegift_price_band_summary_daily', 'mart_build_locks']) {
    const ok = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(t);
    if (!ok) {
      console.error(`[build-linegift-analytics-mart] FATAL: required table not found: ${t}`);
      console.error('  migration を先に実行: node apps/warehouse-mirror/migrate-linegift-analytics-v1-{dimensions,locks,marts}.js');
      process.exit(1);
    }
  }

  if (dryRun) {
    // 件数だけ確認
    for (const w of windows) {
      const startOffset = w - 1;
      const count = db.prepare(`
        SELECT COUNT(DISTINCT ne_code || '|' || sku_code) AS n
        FROM mirror_linegift_finance_sku_daily
        WHERE date_jst >= date(?, '-' || ? || ' days') AND date_jst <= ?
      `).get(asOf, startOffset, asOf).n;
      console.log(`  [dry] window=${w} (range: as_of - ${startOffset} 〜 as_of): ${count} SKU candidates`);
    }
    console.log('[build-linegift-analytics-mart] dry-run end');
    return;
  }

  // lock 取得 (Codex Round 2 Critical 2: DB lock + heartbeat + 409)
  const lock = acquireLock(db, LOCK_NAME, { acquired_by: `pid:${process.pid}@build-mart`, lease_ttl_seconds: 600 });
  if (!lock) {
    console.error(`[build-linegift-analytics-mart] CONFLICT (409): lock ${LOCK_NAME} is currently held`);
    process.exit(2);
  }
  console.log(`[build-linegift-analytics-mart] lock acquired: ${lock.acquired_by}`);

  // heartbeat (build 中は 30s 間隔で更新、stale 失効防止)
  const heartbeat = setInterval(() => {
    try { updateHeartbeat(db, LOCK_NAME); } catch (_) {}
  }, 30_000);

  const syncedAt = nowJstIso();
  const summary = { as_of: asOf, windows };

  try {
    // 全 mart を 1 transaction で原子化 (window 別分割しない、partial publish 防止)
    db.transaction(() => {
      for (const w of windows) {
        console.log(`  --- window_days=${w} ---`);
        summary[`sku_perf_${w}`]           = buildSkuPerf(db, asOf, w, syncedAt);
        summary[`kpi_summary_${w}`]        = buildKpiSummary(db, asOf, w, syncedAt);
        summary[`price_band_summary_${w}`] = buildPriceBandSummary(db, asOf, w, syncedAt);
      }
    })();

    clearInterval(heartbeat);
    releaseLock(db, LOCK_NAME, 'COMPLETED');
    console.log('[build-linegift-analytics-mart] done (COMPLETED)');
    console.log('  summary=' + JSON.stringify(summary));
  } catch (e) {
    clearInterval(heartbeat);
    // SQLite が自動 rollback (transaction が exception で抜けると)、partial publish は発生しない
    releaseLock(db, LOCK_NAME, 'FAILED', e?.message || String(e));
    console.error('[build-linegift-analytics-mart] FAILED:', e);
    process.exit(1);
  }
}

main().catch((e) => {
  console.error('[build-linegift-analytics-mart] FATAL:', e);
  process.exit(1);
});
