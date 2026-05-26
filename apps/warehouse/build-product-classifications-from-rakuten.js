#!/usr/bin/env node
/**
 * build-product-classifications-from-rakuten.js — LINEギフト Phase 1 A-5a-2
 *   raw_rakuten_items_master + x_rakuten_item_product_map + m_products から
 *   m_product_classifications に RAKUTEN_AUTO 行を投入する。
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2 / §13-4
 * Codex 議論: Round 2-6
 *
 * 動作:
 *   1. raw_rakuten_items_master (active) と x_rakuten_item_product_map (active, CONFIRMED) を JOIN
 *   2. m_rakuten_genres から genre_name を lookup
 *   3. m_products.標準売価 と m_price_bands から price_band_code_master を派生
 *   4. m_product_classifications に SCD2 UPSERT (source_type='RAKUTEN_AUTO')
 *      - 既存 active 行が同じ内容なら no-op (source_hash 比較)
 *      - 異なれば valid_to で旧行を閉じて新行 INSERT (transaction で atomic)
 *      - 既存 active 行が source_type='MANUAL' なら touch しない (手動分類を優先)
 *
 * 投入粒度: 1 楽天商品 → 1 PRODUCT level 行 (ne_sku_code=NULL)
 *
 * UNMAPPED 商品 (x_rakuten_item_product_map.mapping_type='UNMAPPED') は対象外。
 * 別途、運用画面 (A-5b 手動分類 UI) で手動分類を入れる。
 *
 * 使い方:
 *   node apps/warehouse/build-product-classifications-from-rakuten.js              実行
 *   node apps/warehouse/build-product-classifications-from-rakuten.js --dry-run    集計のみ、DB 書込なし
 */
import 'dotenv/config';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { initDB, getDB } from './db.js';

const LOCK_FILE = path.resolve('data', 'build-product-classifications-from-rakuten.lock');

// JST 壁時計 (feedback_jst_to_iso_string_trap.md、toISOString の TZ 依存を回避)
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

let lockOwned = false;

function acquireLock() {
  try {
    const fd = fs.openSync(LOCK_FILE, 'wx');
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: nowJstIso() }), 'utf8');
    fs.closeSync(fd);
    lockOwned = true;
    return true;
  } catch (e) {
    if (e.code === 'EEXIST') {
      const content = fs.existsSync(LOCK_FILE) ? fs.readFileSync(LOCK_FILE, 'utf8') : '?';
      console.error(`[build-product-classifications-from-rakuten] FATAL: lock file exists (${LOCK_FILE}): ${content}`);
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

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

/**
 * m_price_bands を読み込んで、価格を band_code に判定するヘルパーを返す。
 * 境界判定は包含 (min <= price <= max、max が NULL なら上限なし)。
 * Codex Round 4 SQL #4 反映: 入力は丸め前の数値 (REAL 比較)
 */
function buildPriceBandResolver(db) {
  const bands = db
    .prepare(`SELECT band_code, min_price_jpy_incl, max_price_jpy_incl
              FROM m_price_bands
              WHERE is_active=1 AND valid_to IS NULL
              ORDER BY sort_order`)
    .all();
  return (price) => {
    if (price == null || Number.isNaN(price)) return null;
    for (const b of bands) {
      const ok = price >= b.min_price_jpy_incl &&
                 (b.max_price_jpy_incl == null || price <= b.max_price_jpy_incl);
      if (ok) return b.band_code;
    }
    return null;
  };
}

/**
 * 既存 active 行を取得 (1 ne_product_code、PRODUCT level)
 */
function getActiveClassification(db, neProductCode) {
  return db
    .prepare(`SELECT id, source_type, source_hash, main_genre_id, main_genre_name,
                     price_band_code_master, is_provisional, source_ref
              FROM m_product_classifications
              WHERE ne_product_code = ?
                AND ne_sku_code IS NULL
                AND classification_level = 'PRODUCT'
                AND is_active = 1
                AND valid_to IS NULL`)
    .get(neProductCode);
}

/**
 * SCD2 UPSERT
 * 戻り値の action:
 *   'unchanged'      既存 RAKUTEN_AUTO 行と同じ hash → no-op
 *   'inserted'       既存 active 行なし → 新規 INSERT
 *   'updated'        既存 RAKUTEN_AUTO 行とは hash 違い → close + insert
 *   'skipped_manual' 既存 MANUAL 行あり → 手動分類を優先、touch しない
 */
function upsertClassification(db, row, syncedAt) {
  // source_hash 構築 (差分検知用)
  const hashInput = [
    row.ne_product_code,
    row.main_genre_id ?? '',
    row.main_genre_name ?? '',
    row.price_band_code_master ?? '',
    'RAKUTEN_AUTO',
    String(row.is_provisional ? 1 : 0),
  ].join('\t');
  const sourceHash = sha256Hex(hashInput);

  const existing = getActiveClassification(db, row.ne_product_code);

  // MANUAL を上書きしない (Codex Round 2 Medium: 手動分類優先)
  if (existing && existing.source_type === 'MANUAL') {
    return { action: 'skipped_manual' };
  }

  if (existing && existing.source_hash === sourceHash) {
    return { action: 'unchanged' };
  }

  // SCD2 close + insert を transaction で atomic 化 (Codex Round 1 Critical 3)
  const txn = db.transaction(() => {
    if (existing) {
      db.prepare(`UPDATE m_product_classifications
                  SET is_active=0, valid_to=?, synced_at=?, updated_by=?
                  WHERE id=?`)
        .run(syncedAt, syncedAt, 'system:build-product-classifications-from-rakuten', existing.id);
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
                ?, 'RAKUTEN_AUTO', ?, ?,
                ?, 1, 1,
                ?, NULL, ?, ?, NULL, ?)
    `).run(
      row.ne_product_code,
      row.main_genre_id || null, row.main_genre_name || null,
      row.price_band_code_master || null,
      row.source_ref || null, sourceHash,
      row.is_provisional ? 1 : 0,
      syncedAt,
      'system:build-product-classifications-from-rakuten',
      'system:build-product-classifications-from-rakuten',
      syncedAt
    );
  });
  txn();
  return { action: existing ? 'updated' : 'inserted' };
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');

  if (!dryRun && !acquireLock()) {
    process.exit(2);
  }

  console.log(`[build-product-classifications-from-rakuten] start (dryRun=${dryRun})`);

  initDB();
  const db = getDB();

  const resolveBand = buildPriceBandResolver(db);
  const bandCount = db.prepare(`SELECT COUNT(*) AS n FROM m_price_bands WHERE is_active=1 AND valid_to IS NULL`).get().n;
  console.log(`  m_price_bands: ${bandCount} bands active`);

  // 入力 (raw_rakuten_items_master と x_rakuten_item_product_map の JOIN、CONFIRMED のみ)
  // m_rakuten_genres から genre_name を lookup、m_products から 標準売価 を取得して band 派生
  const rows = db.prepare(`
    SELECT
      x.ne_product_code   AS ne_product_code,
      r.genre_id          AS main_genre_id,
      g.genre_name        AS main_genre_name,
      mp.標準売価         AS standard_price,
      r.manage_number     AS source_ref
    FROM x_rakuten_item_product_map x
    JOIN raw_rakuten_items_master r
      ON r.rakuten_shop_code = x.rakuten_shop_code
     AND r.manage_number = x.rakuten_item_code
     AND r.is_active = 1
     AND r.valid_to IS NULL
    LEFT JOIN m_rakuten_genres g
      ON g.genre_id = r.genre_id
     AND g.is_active = 1
     AND g.valid_to IS NULL
    LEFT JOIN m_products mp
      ON mp.商品コード = x.ne_product_code
    WHERE x.is_active = 1
      AND x.valid_to IS NULL
      AND x.mapping_type = 'CONFIRMED'
      AND x.ne_product_code <> ''
  `).all();

  console.log(`  candidates: ${rows.length} 件 (CONFIRMED 紐付け済)`);

  const syncedAt = nowJstIso();
  const counters = { total: 0, inserted: 0, updated: 0, unchanged: 0, skipped_manual: 0, missing_genre: 0 };

  for (const r of rows) {
    counters.total += 1;

    // genre_id があるのに genre_name が引けない場合は warn (m_rakuten_genres 未投入の可能性)
    if (r.main_genre_id && !r.main_genre_name) {
      counters.missing_genre += 1;
    }

    const priceBand = resolveBand(r.standard_price);

    const row = {
      ne_product_code: r.ne_product_code,
      main_genre_id: r.main_genre_id || null,
      main_genre_name: r.main_genre_name || null,
      price_band_code_master: priceBand,
      source_ref: r.source_ref || null,
      // genre_name が NULL (= m_rakuten_genres 未投入) なら is_provisional=1 として後で補正可
      is_provisional: r.main_genre_id && !r.main_genre_name ? 1 : 0,
    };

    if (dryRun) {
      // 集計のみ (action は判定しない)
      continue;
    }

    const result = upsertClassification(db, row, syncedAt);
    counters[result.action] = (counters[result.action] ?? 0) + 1;
  }

  console.log('[build-product-classifications-from-rakuten] done');
  console.log(`  counters=${JSON.stringify(counters)}`);
  if (counters.missing_genre > 0) {
    console.warn(`  warn: ${counters.missing_genre} 件で main_genre_id があるが m_rakuten_genres 未投入 (is_provisional=1 でマーク済)`);
  }
}

main().catch((e) => {
  console.error('[build-product-classifications-from-rakuten] FATAL:', e);
  process.exit(1);
}).finally(() => {
  releaseLock();
});
