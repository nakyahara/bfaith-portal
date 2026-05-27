/**
 * apps/sales-analytics-linegift/db.js — 手動分類 UI の DB 操作層
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-7
 *
 * 対象テーブル:
 *   - x_rakuten_item_product_map     (manageNumber ↔ ne_product_code 紐付け、SCD2)
 *   - m_product_classifications      (商品分類マスタ、SCD2)
 *   - raw_rakuten_items_master       (items.search 生データ、参考表示用)
 *   - m_rakuten_genres               (genre_id → name 解決)
 *   - m_price_bands                  (価格帯コード一覧)
 *   - m_products                     (商品コード入力 validate / 標準売価 取得)
 *
 * SCD2 ルール:
 *   - 変更時は valid_to で旧 active 行を閉じて新行 INSERT
 *   - close + insert は db.transaction で atomic
 *   - source_type='MANUAL' で reason 必須 (API 層で強制)
 */
import { getDB } from '../warehouse/db.js';

const SHOP_CODE_DEFAULT = process.env.RAKUTEN_SHOP_CODE || 'b-faith';

// JST 壁時計 (feedback_jst_to_iso_string_trap.md)
function jstWallclock() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

export function nowJstIso() {
  const j = jstWallclock();
  const yy = j.getUTCFullYear();
  const mm = String(j.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(j.getUTCDate()).padStart(2, '0');
  const hh = String(j.getUTCHours()).padStart(2, '0');
  const mi = String(j.getUTCMinutes()).padStart(2, '0');
  const ss = String(j.getUTCSeconds()).padStart(2, '0');
  return `${yy}-${mm}-${dd}T${hh}:${mi}:${ss}+09:00`;
}

/**
 * 未分類キュー (UNMAPPED) 一覧
 * 売上 fact (f_linegift_finance_sku_daily_v1) と LEFT JOIN して
 * 「直近7日の売上 / 件数」を取得、降順ソート。
 *
 * @returns Array<{
 *   rakuten_shop_code, rakuten_item_code, rakuten_item_name, rakuten_item_url,
 *   genre_id, genre_name, sales_7d, units_7d, first_seen_at, last_seen_at
 * }>
 */
export function listUnmapped(opts = {}) {
  const limit = Math.max(1, Math.min(500, Number(opts.limit) || 200));
  const db = getDB();
  return db.prepare(`
    WITH unmapped AS (
      SELECT
        x.rakuten_shop_code, x.rakuten_item_code, x.rakuten_item_name, x.rakuten_item_url,
        x.synced_at AS last_seen_at,
        r.genre_id, r.title AS raw_title, r.synced_at AS raw_synced_at
      FROM x_rakuten_item_product_map x
      LEFT JOIN raw_rakuten_items_master r
        ON r.rakuten_shop_code = x.rakuten_shop_code
       AND r.manage_number     = x.rakuten_item_code
       AND r.is_active = 1
       AND r.valid_to IS NULL
      WHERE x.is_active = 1
        AND x.valid_to IS NULL
        AND x.mapping_type = 'UNMAPPED'
    )
    SELECT
      u.rakuten_shop_code, u.rakuten_item_code, u.rakuten_item_name, u.rakuten_item_url,
      u.genre_id, u.last_seen_at,
      g.genre_name,
      COALESCE(SUM(f.gross_sales_jpy_incl), 0) AS sales_7d,
      COALESCE(SUM(f.units_net_sold), 0)       AS units_7d
    FROM unmapped u
    LEFT JOIN m_rakuten_genres g
      ON g.genre_id = u.genre_id
     AND g.is_active = 1
     AND g.valid_to IS NULL
    LEFT JOIN f_linegift_finance_sku_daily_v1 f
      ON f.sku_code = u.rakuten_item_code
     AND f.date_jst >= date('now', '+9 hours', '-7 days')
    GROUP BY u.rakuten_shop_code, u.rakuten_item_code, u.rakuten_item_name, u.rakuten_item_url,
             u.genre_id, u.last_seen_at, g.genre_name
    ORDER BY sales_7d DESC, units_7d DESC, u.last_seen_at DESC
    LIMIT ?
  `).all(limit);
}

/**
 * m_products から商品コードを 1 件取得 (入力 validate)
 */
export function lookupNeProduct(neProductCode) {
  if (!neProductCode || typeof neProductCode !== 'string') return null;
  const db = getDB();
  return db.prepare(`SELECT 商品コード, 商品名, 標準売価 FROM m_products WHERE 商品コード = ?`)
    .get(neProductCode.trim());
}

/**
 * x_rakuten_item_product_map に対する手動分類 (CONFIRMED への昇格 or 紐付け修正)
 *
 * @param params {
 *   rakuten_shop_code, rakuten_item_code, ne_product_code, reason, user
 * }
 * @returns { action: 'inserted'|'updated', ne_product_code }
 */
export function setManualMapping(params) {
  const { rakuten_shop_code, rakuten_item_code, ne_product_code, reason, user } = params;
  if (!rakuten_shop_code || !rakuten_item_code || !ne_product_code || !reason) {
    throw new Error('rakuten_shop_code, rakuten_item_code, ne_product_code, reason are required');
  }
  if (typeof reason !== 'string' || reason.trim().length < 3) {
    throw new Error('reason must be at least 3 characters');
  }
  // ne_product_code が m_products に存在するか check
  const np = lookupNeProduct(ne_product_code);
  if (!np) throw new Error(`ne_product_code='${ne_product_code}' not found in m_products`);

  const db = getDB();
  const syncedAt = nowJstIso();
  const userTag = user || 'system:manual';

  // SCD2: existing 取得 + UPDATE + INSERT を全部 transaction 内に
  // (Codex Round 1 A-5b high 2 反映: 同時更新で active 重複を作る事故防止)
  const txn = db.transaction(() => {
    const existing = db.prepare(`
      SELECT id, ne_product_code, mapping_type
      FROM x_rakuten_item_product_map
      WHERE rakuten_shop_code = ?
        AND rakuten_item_code = ?
        AND is_active = 1
        AND valid_to IS NULL
    `).get(rakuten_shop_code, rakuten_item_code);

    if (existing) {
      db.prepare(`UPDATE x_rakuten_item_product_map
                  SET is_active=0, valid_to=?, updated_by=?, synced_at=?
                  WHERE id=?`)
        .run(syncedAt, userTag, syncedAt, existing.id);
    }
    db.prepare(`
      INSERT INTO x_rakuten_item_product_map (
        rakuten_shop_code, rakuten_item_code, rakuten_item_url, rakuten_item_name,
        ne_product_code, ne_sku_code,
        mapping_type, confidence, reason, is_active,
        valid_from, valid_to, created_by, updated_by, synced_at
      ) VALUES (?, ?, NULL, NULL,
                ?, NULL,
                'CONFIRMED', 1.0, ?, 1,
                ?, NULL, ?, ?, ?)
    `).run(
      rakuten_shop_code, rakuten_item_code,
      np['商品コード'],
      reason,
      syncedAt, userTag, userTag, syncedAt
    );
    return { action: existing ? 'updated' : 'inserted' };
  });
  const result = txn();
  return { ...result, ne_product_code: np['商品コード'] };
}

/**
 * m_product_classifications の手動上書き (主に MANUAL ソースで分類補正)
 *
 * 注意: A-5a-2 (build-product-classifications-from-rakuten.js) は MANUAL を上書きしない仕様。
 *       本関数で MANUAL 行を作ると、以降の RAKUTEN_AUTO 投入で skipped_manual になる。
 *
 * @param params {
 *   ne_product_code, main_genre_id, main_genre_name, price_band_code_master, reason, user
 * }
 */
export function setManualClassification(params) {
  const {
    ne_product_code, main_genre_id, main_genre_name, price_band_code_master, reason, user,
  } = params;
  if (!ne_product_code || !reason) {
    throw new Error('ne_product_code and reason are required');
  }
  if (typeof reason !== 'string' || reason.trim().length < 3) {
    throw new Error('reason must be at least 3 characters');
  }
  const np = lookupNeProduct(ne_product_code);
  if (!np) throw new Error(`ne_product_code='${ne_product_code}' not found in m_products`);

  // price_band_code_master が指定されていれば m_price_bands に存在するか check
  if (price_band_code_master) {
    const db0 = getDB();
    const band = db0.prepare(`SELECT band_code FROM m_price_bands WHERE band_code=? AND is_active=1 AND valid_to IS NULL`).get(price_band_code_master);
    if (!band) throw new Error(`price_band_code_master='${price_band_code_master}' is not active`);
  }

  const db = getDB();
  const syncedAt = nowJstIso();
  const userTag = user || 'system:manual';

  // SCD2: existing 取得 + UPDATE + INSERT を全部 transaction 内に
  // (Codex Round 1 A-5b high 2 反映)
  const txn = db.transaction(() => {
    const existing = db.prepare(`
      SELECT id
      FROM m_product_classifications
      WHERE ne_product_code = ?
        AND ne_sku_code IS NULL
        AND classification_level = 'PRODUCT'
        AND is_active = 1
        AND valid_to IS NULL
    `).get(ne_product_code);

    if (existing) {
      db.prepare(`UPDATE m_product_classifications
                  SET is_active=0, valid_to=?, updated_by=?, synced_at=?
                  WHERE id=?`)
        .run(syncedAt, userTag, syncedAt, existing.id);
    }
    db.prepare(`
      INSERT INTO m_product_classifications (
        ne_product_code, ne_sku_code, classification_level,
        main_genre_id, main_genre_name, sub_genre_id, sub_genre_name,
        price_band_code_master, source_type, source_ref, source_hash,
        is_provisional, is_classified, is_active,
        valid_from, valid_to, created_by, updated_by, reason, synced_at
      ) VALUES (?, NULL, 'PRODUCT',
                ?, ?, NULL, NULL,
                ?, 'MANUAL', NULL, NULL,
                0, 1, 1,
                ?, NULL, ?, ?, ?, ?)
    `).run(
      ne_product_code,
      main_genre_id || null, main_genre_name || null,
      price_band_code_master || null,
      syncedAt, userTag, userTag, reason, syncedAt
    );
    return { action: existing ? 'updated' : 'inserted' };
  });
  const result = txn();
  return { ...result, ne_product_code };
}

/**
 * 既存 active な x_rakuten_item_product_map / m_product_classifications を集計
 * (UI ヘッダー表示用)
 */
export function getCounters() {
  const db = getDB();
  const x = db.prepare(`
    SELECT mapping_type, COUNT(*) AS n
    FROM x_rakuten_item_product_map
    WHERE is_active=1 AND valid_to IS NULL
    GROUP BY mapping_type
  `).all();
  const c = db.prepare(`
    SELECT source_type, COUNT(*) AS n
    FROM m_product_classifications
    WHERE is_active=1 AND valid_to IS NULL
    GROUP BY source_type
  `).all();
  return {
    mapping: Object.fromEntries(x.map((r) => [r.mapping_type, r.n])),
    classification: Object.fromEntries(c.map((r) => [r.source_type, r.n])),
  };
}

/**
 * m_price_bands 一覧 (UI ドロップダウン用)
 */
export function listPriceBands() {
  const db = getDB();
  return db.prepare(`
    SELECT band_code, band_label, min_price_jpy_incl, max_price_jpy_incl
    FROM m_price_bands
    WHERE is_active=1 AND valid_to IS NULL
    ORDER BY sort_order
  `).all();
}

// ─────────────────────────────────────────────────────────────────
// A-6 ダッシュボード API 用関数
// ─────────────────────────────────────────────────────────────────

const VALID_WINDOWS = new Set([7, 28, 90]);

function validateWindow(w) {
  const n = Number(w);
  if (!VALID_WINDOWS.has(n)) throw new Error(`invalid window: ${w} (must be 7/28/90)`);
  return n;
}

/**
 * ① KPI summary (期間別、materialized から)
 * 同一 as_of_date_jst 契約 (Codex Round 5 High-1) に従い、各 window で最新 as_of の行を返す
 */
export function getKpiSummary(windowDays) {
  const w = validateWindow(windowDays);
  const db = getDB();
  return db.prepare(`
    SELECT total_sales_jpy_incl, total_orders, avg_order_value_jpy_incl,
           total_gross_profit_jpy_incl, gross_margin_rate, as_of_date_jst, synced_at
    FROM f_linegift_kpi_summary_daily
    WHERE window_days = ?
      AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM f_linegift_kpi_summary_daily WHERE window_days = ?)
  `).get(w, w) || null;
}

/**
 * ② 月別トレンド (過去12ヶ月、view 直 SELECT)
 */
export function getMonthlyTrend() {
  const db = getDB();
  return db.prepare(`SELECT * FROM v_linegift_monthly_trend`).all();
}

/**
 * ③④ 商品ランキング (filters: window/genre/price_band_master/season_code/sort/limit)
 * 集計データは f_linegift_sku_perf_daily (期間別 materialized) から取得、
 * m_product_classifications JOIN で genre/price_band master を取得。
 * season_code 指定時は d_gift_season_occurrences と f_linegift_finance_sku_daily_v1 から
 * 期間内売上を集計し直す (window_days 無視、シーン期間が優先)。
 */
export function getSkuRanking(filters = {}) {
  const db = getDB();
  const limit = Math.max(1, Math.min(500, Number(filters.limit) || 100));
  const sort = ['sales', 'units', 'profit'].includes(filters.sort) ? filters.sort : 'sales';
  const orderCol = { sales: 'sales_amount_jpy_incl', units: 'units', profit: 'gross_profit_jpy_incl' }[sort];

  if (filters.season_code) {
    // シーン期間集計 (最新の season_year を採用、または明示指定可)
    // Codex Round 1 A-6 High 反映: UI が描画する unit_price_jpy_incl_display と gross_margin_rate を
    // シーン分岐でも返す。NULLIF で 0 除算保護、UI と列契約を揃える。
    const seasonRows = db.prepare(`
      SELECT
        f.ne_code, f.sku_code,
        SUM(f.units_net_sold) AS units,
        ROUND(SUM(f.gross_sales_jpy_incl)) AS sales_amount_jpy_incl,
        ROUND(SUM(f.variable_margin_jpy_incl)) AS gross_profit_jpy_incl,
        SUM(f.order_count) AS orders,
        -- 単価 (実売平均): SUM(sales) / SUM(units)、0 除算は NULLIF で NULL → UI 側で '-' 表示
        CASE WHEN COALESCE(SUM(f.units_net_sold), 0) = 0 THEN 0
             ELSE ROUND(SUM(f.gross_sales_jpy_incl) * 1.0 / SUM(f.units_net_sold)) END AS unit_price_jpy_incl_display,
        -- 粗利率: variable_margin / gross_sales、0 除算は 0 に丸める
        CASE WHEN COALESCE(SUM(f.gross_sales_jpy_incl), 0) = 0 THEN 0
             ELSE ROUND(SUM(f.variable_margin_jpy_incl) * 1.0 / SUM(f.gross_sales_jpy_incl), 4) END AS gross_margin_rate,
        c.main_genre_id, c.main_genre_name, c.price_band_code_master,
        s.season_code, s.season_year, s.start_date_jst, s.end_date_jst
      FROM d_gift_season_occurrences s
      JOIN f_linegift_finance_sku_daily_v1 f
        ON f.date_jst BETWEEN s.start_date_jst AND s.end_date_jst
      LEFT JOIN m_product_classifications c
        ON c.ne_product_code = f.ne_code
       AND COALESCE(c.ne_sku_code, '') = COALESCE(f.sku_code, '')
       AND c.is_active = 1 AND c.valid_to IS NULL
      WHERE s.is_active = 1
        AND s.season_code = ?
        AND s.season_year = COALESCE(?, (SELECT MAX(season_year) FROM d_gift_season_occurrences WHERE season_code = ?))
        AND (? IS NULL OR c.main_genre_id = ?)
        AND (? IS NULL OR c.price_band_code_master = ?)
      GROUP BY f.ne_code, f.sku_code, c.main_genre_id, c.main_genre_name, c.price_band_code_master,
               s.season_code, s.season_year, s.start_date_jst, s.end_date_jst
      ORDER BY ${orderCol} DESC, f.sku_code ASC
      LIMIT ?
    `).all(
      filters.season_code,
      filters.season_year || null, filters.season_code,
      filters.genre || null, filters.genre || null,
      filters.price_band || null, filters.price_band || null,
      limit
    );
    return seasonRows;
  }

  // 通常の window_days ベース
  const w = validateWindow(filters.window);
  return db.prepare(`
    SELECT
      p.ne_code, p.sku_code,
      p.units, p.sales_amount_jpy_incl, p.gross_profit_jpy_incl, p.orders, p.active_days,
      p.velocity_per_active_day, p.velocity_per_calendar_day,
      p.unit_price_jpy_incl_raw, p.unit_price_jpy_incl_display,
      p.gross_margin_rate,
      c.main_genre_id, c.main_genre_name, c.price_band_code_master,
      r.velocity_index_vs_median, r.velocity_percent_rank
    FROM f_linegift_sku_perf_daily p
    LEFT JOIN m_product_classifications c
      ON c.ne_product_code = p.ne_code
     AND COALESCE(c.ne_sku_code, '') = COALESCE(p.sku_code, '')
     AND c.is_active = 1 AND c.valid_to IS NULL
    LEFT JOIN v_linegift_sku_relative_score_90d r
      ON r.ne_code = p.ne_code AND COALESCE(r.sku_code, '') = COALESCE(p.sku_code, '')
    WHERE p.window_days = ?
      AND p.as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM f_linegift_sku_perf_daily WHERE window_days = ?)
      AND (? IS NULL OR c.main_genre_id = ?)
      AND (? IS NULL OR c.price_band_code_master = ?)
    ORDER BY ${orderCol} DESC, p.sku_code ASC
    LIMIT ?
  `).all(
    w, w,
    filters.genre || null, filters.genre || null,
    filters.price_band || null, filters.price_band || null,
    limit
  );
}

/**
 * 価格帯テーブル (sales 系、materialized から)
 */
export function getPriceBandSummaryByWindow(windowDays) {
  const w = validateWindow(windowDays);
  const db = getDB();
  return db.prepare(`
    SELECT *
    FROM f_linegift_price_band_summary_daily
    WHERE window_days = ?
      AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM f_linegift_price_band_summary_daily WHERE window_days = ?)
    ORDER BY sort_order
  `).all(w, w);
}

/**
 * ジャンル一覧 (フィルタ用、最新 active 分類で使われている genre のみ)
 */
export function listAvailableGenres() {
  const db = getDB();
  return db.prepare(`
    SELECT c.main_genre_id, c.main_genre_name, COUNT(*) AS sku_count
    FROM m_product_classifications c
    WHERE c.is_active = 1 AND c.valid_to IS NULL
      AND c.main_genre_id IS NOT NULL
    GROUP BY c.main_genre_id, c.main_genre_name
    ORDER BY sku_count DESC, c.main_genre_name
  `).all();
}

/**
 * シーン一覧 (フィルタ用、active な m_gift_seasons の最新 season_year occurrence 付き)
 */
export function listAvailableSeasons() {
  const db = getDB();
  return db.prepare(`
    SELECT
      s.season_code, s.season_name,
      MAX(o.season_year) AS latest_year,
      (SELECT start_date_jst FROM d_gift_season_occurrences WHERE season_code = s.season_code ORDER BY season_year DESC LIMIT 1) AS latest_start,
      (SELECT end_date_jst   FROM d_gift_season_occurrences WHERE season_code = s.season_code ORDER BY season_year DESC LIMIT 1) AS latest_end
    FROM m_gift_seasons s
    LEFT JOIN d_gift_season_occurrences o ON o.season_code = s.season_code
    WHERE s.is_active = 1 AND s.valid_to IS NULL
    GROUP BY s.season_code, s.season_name
    ORDER BY s.priority DESC, s.season_name
  `).all();
}
