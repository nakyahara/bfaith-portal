#!/usr/bin/env node
/**
 * build-linegift-analytics-mart.js — LINEギフト Phase 1 A-5c
 *   分析 materialized table を同一 as_of_date_jst で一括 build
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-5
 * Codex 議論: Round 3 Critical (percentile 代替=Node.js 案3) + Round 5 High-1 (as_of 統一)
 *
 * Build 順序 (依存関係):
 *   1. f_linegift_sku_perf_daily       ← f_linegift_finance_sku_daily_v1 から集計、window_days=7/28/90
 *   2. f_linegift_kpi_summary_daily    ← f_linegift_finance_sku_daily_v1 から集計、同 as_of_date_jst + window_days
 *   3. f_linegift_segment_benchmark_daily ← f_linegift_sku_perf_daily + m_product_classifications JOIN、
 *                                         Node.js で p50/p80 を計算して materialized
 *   4. f_linegift_price_band_summary_daily ← f_linegift_sku_perf_daily から価格帯 8段階別集計
 *
 * 全 materialized table が同一 as_of_date_jst を共有する契約。
 * ダッシュボード読込時、アプリ層は MAX(as_of_date_jst) を 1 回引いて全テーブル参照。
 *
 * 使い方:
 *   node apps/warehouse/build-linegift-analytics-mart.js              実行 (7/28/90 全 build)
 *   node apps/warehouse/build-linegift-analytics-mart.js --dry-run    集計のみ、DB 書込なし
 *   node apps/warehouse/build-linegift-analytics-mart.js --windows=90 90 のみ
 *   node apps/warehouse/build-linegift-analytics-mart.js --as-of=2026-05-26  as_of 上書き
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { initDB, getDB } from './db.js';

const LOCK_FILE = path.resolve('data', 'build-linegift-analytics-mart.lock');

function jstWallclock() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

function nowJstIso() {
  const j = jstWallclock();
  const yy = j.getUTCFullYear();
  const mm = String(j.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(j.getUTCDate()).padStart(2, '0');
  const hh = String(j.getUTCHours()).padStart(2, '0');
  const mi = String(j.getUTCMinutes()).padStart(2, '0');
  const ss = String(j.getUTCSeconds()).padStart(2, '0');
  return `${yy}-${mm}-${dd}T${hh}:${mi}:${ss}+09:00`;
}

function todayJstDate() {
  const j = jstWallclock();
  return `${j.getUTCFullYear()}-${String(j.getUTCMonth() + 1).padStart(2,'0')}-${String(j.getUTCDate()).padStart(2,'0')}`;
}

let lockOwned = false;

function acquireLock() {
  try {
    fs.mkdirSync(path.dirname(LOCK_FILE), { recursive: true });
    const fd = fs.openSync(LOCK_FILE, 'wx');
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: nowJstIso() }), 'utf8');
    fs.closeSync(fd);
    lockOwned = true;
    return true;
  } catch (e) {
    if (e.code === 'EEXIST') {
      const content = fs.existsSync(LOCK_FILE) ? fs.readFileSync(LOCK_FILE, 'utf8') : '?';
      console.error(`[build-linegift-analytics-mart] FATAL: lock file exists (${LOCK_FILE}): ${content}`);
      return false;
    }
    throw e;
  }
}

function releaseLock() {
  if (!lockOwned) return;
  try { fs.unlinkSync(LOCK_FILE); } catch (_) {}
  lockOwned = false;
}

/**
 * 線形補間 percentile (Codex Round 3 案3、apps/warehouse/notify 等の慣習と整合)
 * 入力配列は呼び出し側で昇順 sort 済を想定
 */
function percentileSorted(arr, p) {
  if (!arr.length) return 0;
  const idx = (arr.length - 1) * p;
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  if (lo === hi) return arr[lo];
  return arr[lo] + (arr[hi] - arr[lo]) * (idx - lo);
}

/**
 * 価格帯コードを丸め前単価から判定 (m_price_bands.band_code、Codex Round 4 R4-4)
 */
function buildPriceBandResolver(db) {
  const bands = db.prepare(`
    SELECT band_code, min_price_jpy_incl, max_price_jpy_incl
    FROM m_price_bands
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
 * 1) f_linegift_sku_perf_daily を build
 * INSERT OR REPLACE、window_days パラメタ完全注入 (Codex Round 5)
 */
function buildSkuPerf(db, asOf, windowDays, syncedAt, dryRun) {
  // Codex Round 1 A-5c High 反映: window_days=7 で as_of-7 〜 as_of は 8 日になる off-by-one を修正
  // 厳密に N 日窓にするため start = as_of - (N - 1)
  const startOffset = windowDays - 1;
  const sql = `
    INSERT OR REPLACE INTO f_linegift_sku_perf_daily (
      as_of_date_jst, window_days, ne_code, sku_code,
      units, sales_amount_jpy_incl, gross_profit_jpy_incl, orders, active_days,
      velocity_per_active_day, velocity_per_calendar_day,
      unit_price_jpy_incl_raw, unit_price_jpy_incl_display,
      gross_margin_rate, synced_at
    )
    WITH base AS (
      SELECT f.date_jst, f.sku_code, f.ne_code, f.units_net_sold, f.gross_sales_jpy_incl,
             f.variable_margin_jpy_incl, f.order_count
      FROM f_linegift_finance_sku_daily_v1 f
      WHERE f.date_jst >= date(?, '-' || ? || ' days')
        AND f.date_jst <= ?
    )
    SELECT
      ? AS as_of_date_jst,
      ? AS window_days,
      b.ne_code, b.sku_code,
      COALESCE(SUM(b.units_net_sold), 0) AS units,
      ROUND(COALESCE(SUM(b.gross_sales_jpy_incl), 0)) AS sales_amount_jpy_incl,
      ROUND(COALESCE(SUM(b.variable_margin_jpy_incl), 0)) AS gross_profit_jpy_incl,
      COALESCE(SUM(b.order_count), 0) AS orders,
      COUNT(DISTINCT b.date_jst) AS active_days,
      CASE WHEN COUNT(DISTINCT b.date_jst) = 0 THEN 0.0
           ELSE 1.0 * COALESCE(SUM(b.units_net_sold), 0) / COUNT(DISTINCT b.date_jst) END AS velocity_per_active_day,
      1.0 * COALESCE(SUM(b.units_net_sold), 0) / ? AS velocity_per_calendar_day,
      CASE WHEN COALESCE(SUM(b.units_net_sold), 0) = 0 THEN 0.0
           ELSE 1.0 * COALESCE(SUM(b.gross_sales_jpy_incl), 0) / COALESCE(SUM(b.units_net_sold), 0) END AS unit_price_jpy_incl_raw,
      CASE WHEN COALESCE(SUM(b.units_net_sold), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(b.gross_sales_jpy_incl), 0) * 1.0 / COALESCE(SUM(b.units_net_sold), 0)) END AS unit_price_jpy_incl_display,
      CASE WHEN COALESCE(SUM(b.gross_sales_jpy_incl), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(b.variable_margin_jpy_incl), 0) * 1.0 / COALESCE(SUM(b.gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate,
      ? AS synced_at
    FROM base b
    GROUP BY b.ne_code, b.sku_code
  `;
  if (dryRun) {
    const count = db.prepare(`
      SELECT COUNT(DISTINCT ne_code || '|' || sku_code) AS n
      FROM f_linegift_finance_sku_daily_v1
      WHERE date_jst >= date(?, '-' || ? || ' days') AND date_jst <= ?
    `).get(asOf, startOffset, asOf).n;
    console.log(`  [dry] sku_perf window=${windowDays} (range: as_of - ${startOffset} 〜 as_of, 計 ${windowDays} 日): ${count} SKU candidates`);
    return count;
  }
  // bind: [as_of, startOffset(WHERE start), as_of(WHERE end), as_of(SELECT), windowDays(SELECT), windowDays(velocity分母), syncedAt]
  const info = db.prepare(sql).run(asOf, startOffset, asOf, asOf, windowDays, windowDays, syncedAt);
  return info.changes;
}

/**
 * 2) f_linegift_kpi_summary_daily を build
 */
function buildKpiSummary(db, asOf, windowDays, syncedAt, dryRun) {
  // Codex Round 1 A-5c High 反映: 厳密に N 日窓 (start = as_of - (N - 1))
  const startOffset = windowDays - 1;
  const sql = `
    INSERT OR REPLACE INTO f_linegift_kpi_summary_daily (
      as_of_date_jst, window_days,
      total_sales_jpy_incl, total_orders, avg_order_value_jpy_incl,
      total_gross_profit_jpy_incl, gross_margin_rate, synced_at
    )
    SELECT
      ? AS as_of_date_jst,
      ? AS window_days,
      ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS total_sales_jpy_incl,
      COALESCE(SUM(f.order_count), 0) AS total_orders,
      CASE WHEN COALESCE(SUM(f.order_count), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0) / COALESCE(SUM(f.order_count), 0)) END AS avg_order_value_jpy_incl,
      ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0)) AS total_gross_profit_jpy_incl,
      CASE WHEN COALESCE(SUM(f.gross_sales_jpy_incl), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) * 1.0 / COALESCE(SUM(f.gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate,
      ? AS synced_at
    FROM f_linegift_finance_sku_daily_v1 f
    WHERE f.date_jst >= date(?, '-' || ? || ' days')
      AND f.date_jst <= ?
  `;
  if (dryRun) {
    console.log(`  [dry] kpi_summary window=${windowDays} (range: as_of - ${startOffset} 〜 as_of, 計 ${windowDays} 日): skipped (1 row aggregate)`);
    return 0;
  }
  // bind: [as_of(SELECT), windowDays(SELECT), syncedAt, as_of(WHERE start), startOffset, as_of(WHERE end)]
  const info = db.prepare(sql).run(asOf, windowDays, syncedAt, asOf, startOffset, asOf);
  return info.changes;
}

/**
 * 3) f_linegift_segment_benchmark_daily を build (Node.js で p50/p80 計算)
 */
function buildSegmentBenchmark(db, asOf, windowDays, syncedAt, dryRun) {
  // f_linegift_sku_perf_daily と m_product_classifications を JOIN して
  // (genre × price_band) ごとに SKU の velocity_per_calendar_day 配列を集める
  const rows = db.prepare(`
    SELECT
      c.main_genre_id,
      c.price_band_code_master,
      p.velocity_per_calendar_day
    FROM f_linegift_sku_perf_daily p
    LEFT JOIN m_product_classifications c
      ON c.ne_product_code = p.ne_code
     AND COALESCE(c.ne_sku_code, '') = COALESCE(p.sku_code, '')
     AND c.is_active = 1
     AND c.valid_to IS NULL
    WHERE p.as_of_date_jst = ?
      AND p.window_days = ?
  `).all(asOf, windowDays);

  // segment key で grouping
  const segMap = new Map();
  for (const r of rows) {
    const key = `${r.main_genre_id ?? ''}|${r.price_band_code_master ?? ''}`;
    if (!segMap.has(key)) segMap.set(key, { main_genre_id: r.main_genre_id, price_band_code_master: r.price_band_code_master, values: [] });
    segMap.get(key).values.push(Number(r.velocity_per_calendar_day) || 0);
  }

  if (dryRun) {
    console.log(`  [dry] segment_benchmark window=${windowDays}: ${segMap.size} segments`);
    return segMap.size;
  }

  // 既存 as_of+window の行を削除して再投入 (INSERT OR REPLACE は PK が NULL を許す列を含むため UPSERT が複雑、削除→挿入が安全)
  const insertStmt = db.prepare(`
    INSERT INTO f_linegift_segment_benchmark_daily (
      as_of_date_jst, window_days, main_genre_id, price_band_code_master,
      sku_count, median_velocity_per_calendar_day, p80_velocity_per_calendar_day, synced_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const deleteStmt = db.prepare(`DELETE FROM f_linegift_segment_benchmark_daily WHERE as_of_date_jst=? AND window_days=?`);

  let inserted = 0;
  db.transaction(() => {
    deleteStmt.run(asOf, windowDays);
    for (const seg of segMap.values()) {
      const sorted = seg.values.slice().sort((a, b) => a - b);
      const p50 = percentileSorted(sorted, 0.5);
      const p80 = percentileSorted(sorted, 0.8);
      insertStmt.run(
        asOf, windowDays,
        seg.main_genre_id, seg.price_band_code_master,
        seg.values.length, p50, p80, syncedAt
      );
      inserted += 1;
    }
  })();

  return inserted;
}

/**
 * 4) f_linegift_price_band_summary_daily を build (実売単価ベース band 派生)
 */
function buildPriceBandSummary(db, asOf, windowDays, syncedAt, dryRun) {
  const resolveBand = buildPriceBandResolver(db);
  const bands = db.prepare(`
    SELECT band_code, band_label, sort_order
    FROM m_price_bands
    WHERE is_active = 1 AND valid_to IS NULL
    ORDER BY sort_order
  `).all();
  const bandMeta = Object.fromEntries(bands.map((b) => [b.band_code, b]));

  // SKU 別行を取得して band を派生
  const rows = db.prepare(`
    SELECT
      ne_code, sku_code, units, sales_amount_jpy_incl, gross_profit_jpy_incl, orders, unit_price_jpy_incl_raw
    FROM f_linegift_sku_perf_daily
    WHERE as_of_date_jst = ? AND window_days = ?
  `).all(asOf, windowDays);

  // band ごとに集計
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

  if (dryRun) {
    console.log(`  [dry] price_band_summary window=${windowDays}: ${bands.length} bands`);
    return bands.length;
  }

  const deleteStmt = db.prepare(`DELETE FROM f_linegift_price_band_summary_daily WHERE as_of_date_jst=? AND window_days=?`);
  const insertStmt = db.prepare(`
    INSERT INTO f_linegift_price_band_summary_daily (
      as_of_date_jst, window_days, price_band_code_sales, band_label, sort_order,
      sku_count, orders_count, sales_jpy_incl, gross_profit_jpy_incl, gross_margin_rate, synced_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  db.transaction(() => {
    deleteStmt.run(asOf, windowDays);
    for (const b of bands) {
      const a = agg.get(b.band_code);
      const gmr = a.sales_jpy_incl === 0 ? 0 : Math.round(a.gross_profit_jpy_incl / a.sales_jpy_incl * 10000) / 10000;
      insertStmt.run(
        asOf, windowDays, b.band_code, b.band_label, b.sort_order,
        a.sku_count, a.orders_count, a.sales_jpy_incl, a.gross_profit_jpy_incl, gmr, syncedAt
      );
      inserted += 1;
    }
  })();

  return inserted;
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const winArg = args.find((a) => a.startsWith('--windows='));
  const asOfArg = args.find((a) => a.startsWith('--as-of='));

  const windows = winArg
    ? winArg.split('=')[1].split(',').map((s) => Number(s.trim())).filter((n) => [7, 28, 90].includes(n))
    : [7, 28, 90];
  const asOf = asOfArg ? asOfArg.split('=')[1] : todayJstDate();

  if (!/^\d{4}-\d{2}-\d{2}$/.test(asOf)) {
    console.error(`[build-linegift-analytics-mart] FATAL: invalid as_of=${asOf}`);
    process.exit(1);
  }

  if (!dryRun && !acquireLock()) {
    process.exit(2);
  }

  console.log(`[build-linegift-analytics-mart] start (dryRun=${dryRun}, as_of=${asOf}, windows=${windows.join(',')})`);

  initDB();
  const db = getDB();
  const syncedAt = nowJstIso();
  const summary = { as_of: asOf, windows };

  for (const w of windows) {
    console.log(`  --- window_days=${w} ---`);
    summary[`sku_perf_${w}`]            = buildSkuPerf(db, asOf, w, syncedAt, dryRun);
    summary[`kpi_summary_${w}`]         = buildKpiSummary(db, asOf, w, syncedAt, dryRun);
    summary[`segment_benchmark_${w}`]   = buildSegmentBenchmark(db, asOf, w, syncedAt, dryRun);
    summary[`price_band_summary_${w}`]  = buildPriceBandSummary(db, asOf, w, syncedAt, dryRun);
  }

  console.log('[build-linegift-analytics-mart] done');
  console.log('  summary=' + JSON.stringify(summary));
}

main().catch((e) => {
  console.error('[build-linegift-analytics-mart] FATAL:', e);
  process.exit(1);
}).finally(() => {
  releaseLock();
});
