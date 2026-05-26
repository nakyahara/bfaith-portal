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
