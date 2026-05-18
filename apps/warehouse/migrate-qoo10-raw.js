/**
 * migrate-qoo10-raw.js — Qoo10 Phase 1 A-1 旧 raw_qoo10_orders → 新スキーマ移行
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §6-1.1
 * 参照 SQL: sql/qoo10/raw_qoo10_orders.sql + sql/qoo10/migrate_legacy_raw_qoo10_orders.sql
 *
 * Codex R1 Medium #1 反映: idempotent 化のため Node script で旧/新スキーマを判定。
 *   - 旧スキーマ (11 cols, order_id=packNo) → rename + backfill + 新スキーマ DDL
 *   - 新スキーマ (legacy_fields_missing 列あり) → no-op (再実行安全)
 *   - 退避表 raw_qoo10_orders_legacy_v0 が既存 + 新スキーマも既存 → 移行済とみなし no-op
 *   - 退避表 raw_qoo10_orders_legacy_v0 が既存 + 新スキーマ無し → 異常状態、明示エラー
 *
 * 使い方:
 *   node apps/warehouse/migrate-qoo10-raw.js              実行
 *   node apps/warehouse/migrate-qoo10-raw.js --dry-run    判定のみ (実書き込みなし)
 *
 * ★ 実行前に warehouse.db backup 必須 (warehouse.db.bak_pre_qoo10_a1_<yyyymmdd>)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';

const NEW_DDL = `
CREATE TABLE IF NOT EXISTS raw_qoo10_orders (
  order_id            TEXT NOT NULL PRIMARY KEY CHECK (
    order_id LIKE 'legacy:%:%' OR order_id LIKE 'api:%'
  ),
  source_type         TEXT NOT NULL CHECK (
    source_type IN ('legacy_migration', 'api_v2', 'api_v3')
  ),
  source_order_key    TEXT NOT NULL,
  pack_no             INTEGER NOT NULL,
  seller_id           TEXT NOT NULL DEFAULT '',
  shipping_status     TEXT NOT NULL,
  payment_method      TEXT,
  item_code           TEXT NOT NULL,
  seller_item_code    TEXT NOT NULL,
  item_title          TEXT NOT NULL DEFAULT '',
  option_info         TEXT NOT NULL DEFAULT '',
  option_code         TEXT NOT NULL DEFAULT '',
  order_price         REAL NOT NULL DEFAULT 0,
  order_qty           INTEGER NOT NULL DEFAULT 0,
  discount            REAL NOT NULL DEFAULT 0,
  total               REAL NOT NULL DEFAULT 0,
  settle_price        REAL NOT NULL DEFAULT 0,
  seller_discount         REAL NOT NULL DEFAULT 0,
  cart_discount_seller    REAL NOT NULL DEFAULT 0,
  cart_discount_qoo10     REAL NOT NULL DEFAULT 0,
  voucher_code            TEXT NOT NULL DEFAULT '',
  shipping_rate       REAL NOT NULL DEFAULT 0,
  shipping_rate_type  TEXT NOT NULL DEFAULT '',
  shipping_way        TEXT NOT NULL DEFAULT '',
  delivery_company    TEXT NOT NULL DEFAULT '',
  tracking_no         TEXT NOT NULL DEFAULT '',
  order_date          TEXT NOT NULL,
  payment_date        TEXT,
  shipping_date       TEXT,
  delivered_date      TEXT,
  oversea_consignment TEXT NOT NULL DEFAULT 'N',
  cod_price           REAL NOT NULL DEFAULT 0,
  related_order       TEXT NOT NULL DEFAULT '',
  branch_name         TEXT NOT NULL DEFAULT '',
  first_seen_at         TEXT NOT NULL,
  last_seen_at          TEXT NOT NULL,
  last_api_snapshot_at  TEXT NOT NULL,
  is_frozen_after_horizon INTEGER NOT NULL DEFAULT 0,
  legacy_fields_missing  INTEGER NOT NULL DEFAULT 0,
  synced_at           TEXT NOT NULL
)`;

const NEW_INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_qoo10_order_date ON raw_qoo10_orders(order_date)',
  'CREATE INDEX IF NOT EXISTS idx_qoo10_status     ON raw_qoo10_orders(shipping_status)',
  'CREATE INDEX IF NOT EXISTS idx_qoo10_sku        ON raw_qoo10_orders(seller_item_code)',
  'CREATE INDEX IF NOT EXISTS idx_qoo10_pack       ON raw_qoo10_orders(pack_no)',
  'CREATE INDEX IF NOT EXISTS idx_qoo10_frozen     ON raw_qoo10_orders(is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1',
  'CREATE INDEX IF NOT EXISTS idx_qoo10_legacy     ON raw_qoo10_orders(legacy_fields_missing) WHERE legacy_fields_missing = 1',
  'CREATE INDEX IF NOT EXISTS idx_qoo10_source     ON raw_qoo10_orders(source_type)',
];

const BACKFILL_SQL = `
INSERT INTO raw_qoo10_orders (
  order_id, source_type, source_order_key, pack_no, seller_id,
  shipping_status, payment_method,
  item_code, seller_item_code, item_title, option_info, option_code,
  order_price, order_qty, discount, total, settle_price,
  seller_discount, cart_discount_seller, cart_discount_qoo10, voucher_code,
  shipping_rate, shipping_rate_type, shipping_way, delivery_company, tracking_no,
  order_date, payment_date, shipping_date, delivered_date,
  oversea_consignment, cod_price, related_order, branch_name,
  first_seen_at, last_seen_at, last_api_snapshot_at, is_frozen_after_horizon,
  legacy_fields_missing, synced_at
)
SELECT
  'legacy:' || COALESCE(order_id, '0') || ':' || CAST(id AS TEXT),
  'legacy_migration',
  COALESCE(order_id, '0'),
  CAST(COALESCE(order_id, '0') AS INTEGER),
  '',
  COALESCE(order_status, 'Unknown'),
  NULL,
  COALESCE(item_code, ''),
  COALESCE(item_code, ''),
  COALESCE(item_name, ''),
  COALESCE(option_info, ''),
  '',
  COALESCE(unit_price, 0),
  COALESCE(quantity, 0),
  0,
  COALESCE(total_price, 0),
  COALESCE(total_price, 0),
  0, 0, 0, '',
  0, '', '', '', '',
  COALESCE(order_date, '1970-01-01 00:00:00'),
  NULL, NULL, NULL,
  'N', 0, '', '',
  COALESCE(synced_at, '1970-01-01 00:00:00'),
  COALESCE(synced_at, '1970-01-01 00:00:00'),
  COALESCE(synced_at, '1970-01-01 00:00:00'),
  1,  -- is_frozen_after_horizon=1 (旧データは全て horizon 外、UPDATE 禁止)
  1,  -- legacy_fields_missing=1 (fact 対象外マーカー)
  COALESCE(synced_at, '1970-01-01 00:00:00')
FROM raw_qoo10_orders_legacy_v0
`;

const LEGACY_NAME = 'raw_qoo10_orders_legacy_v0';

function detectState(db) {
  const tables = db.prepare(`
    SELECT name FROM sqlite_master WHERE type='table' AND name IN ('raw_qoo10_orders', ?)
  `).all(LEGACY_NAME).map(r => r.name);
  const hasRaw = tables.includes('raw_qoo10_orders');
  const hasLegacy = tables.includes(LEGACY_NAME);

  let rawIsNew = false, rawIsLegacy = false, rawRowCount = 0, rawLegacyMissingCount = 0;
  if (hasRaw) {
    const cols = db.prepare(`PRAGMA table_info(raw_qoo10_orders)`).all().map(c => c.name);
    rawIsNew = cols.includes('legacy_fields_missing');
    rawIsLegacy = !rawIsNew && cols.includes('order_id') && cols.includes('synced_at') && !cols.includes('source_type');
    rawRowCount = db.prepare(`SELECT COUNT(*) AS n FROM raw_qoo10_orders`).get().n;
    if (rawIsNew) {
      rawLegacyMissingCount = db.prepare(`SELECT COUNT(*) AS n FROM raw_qoo10_orders WHERE legacy_fields_missing = 1`).get().n;
    }
  }
  const legacyRowCount = hasLegacy ? db.prepare(`SELECT COUNT(*) AS n FROM ${LEGACY_NAME}`).get().n : 0;

  return { hasRaw, hasLegacy, rawIsNew, rawIsLegacy, rawRowCount, rawLegacyMissingCount, legacyRowCount };
}

function main() {
  const isDryRun = process.argv.includes('--dry-run');

  initDB();
  const db = getDB();

  const s = detectState(db);
  console.log(`[migrate-qoo10] 現状: raw=${s.hasRaw ? `${s.rawIsNew ? `new (${s.rawRowCount} 行, うち legacy_fields_missing=1 が ${s.rawLegacyMissingCount} 行)` : s.rawIsLegacy ? `legacy (${s.rawRowCount} 行)` : `unknown (${s.rawRowCount} 行)`}` : 'なし'}, ${LEGACY_NAME}=${s.hasLegacy ? `${s.legacyRowCount} 行` : 'なし'}`);

  // ケース分岐
  if (s.hasRaw && s.rawIsNew && !s.hasLegacy) {
    console.log('[migrate-qoo10] ✅ 既に新スキーマに移行済 (legacy 退避表もなし) → no-op');
    return;
  }
  if (s.hasRaw && s.rawIsNew && s.hasLegacy) {
    // Codex R2 Medium #1 反映: legacy 行数と新表の legacy_fields_missing=1 行数の一致を確認。
    // 一致しなければ「新表 DDL だけ作成済みで backfill 未完了」の可能性が高いので fatal。
    if (s.rawLegacyMissingCount === s.legacyRowCount) {
      console.log(`[migrate-qoo10] ✅ 既に新スキーマに移行済 (legacy 退避表 ${LEGACY_NAME}=${s.legacyRowCount} 行、新表 legacy_fields_missing=1 が ${s.rawLegacyMissingCount} 行で一致、退避表は後で削除可) → no-op`);
      return;
    }
    console.error(`[migrate-qoo10] FATAL: 新表+退避表は共存するが件数不一致 (legacy=${s.legacyRowCount} 行 vs raw.legacy_fields_missing=1 が ${s.rawLegacyMissingCount} 行) — backfill 未完了の可能性、手動確認要`);
    process.exit(1);
  }
  if (!s.hasRaw && s.hasLegacy) {
    console.log(`[migrate-qoo10] ⚠️ raw_qoo10_orders は無いが ${LEGACY_NAME} (${s.legacyRowCount} 行) が残存 → 新 DDL 作成 + backfill`);
    if (isDryRun) { console.log('[migrate-qoo10] --dry-run 終了'); return; }
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(NEW_DDL);
      const r = db.prepare(BACKFILL_SQL).run();
      for (const idx of NEW_INDEXES) db.exec(idx);
      console.log(`[migrate-qoo10] ✅ backfill 完了 (${r.changes} 行 INSERT)`);
      db.exec('COMMIT');
    } catch (e) {
      try { db.exec('ROLLBACK'); } catch (_) {}
      console.error(`[migrate-qoo10] ROLLBACK: ${e.message}`);
      process.exit(1);
    }
    return;
  }
  if (s.hasRaw && s.rawIsLegacy && !s.hasLegacy) {
    console.log(`[migrate-qoo10] 旧スキーマ raw_qoo10_orders (${s.rawRowCount} 行) を検出 → 退避 + 新 DDL 作成 + backfill`);
    if (isDryRun) { console.log('[migrate-qoo10] --dry-run 終了'); return; }
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(`ALTER TABLE raw_qoo10_orders RENAME TO ${LEGACY_NAME}`);
      db.exec(NEW_DDL);
      const r = db.prepare(BACKFILL_SQL).run();
      for (const idx of NEW_INDEXES) db.exec(idx);
      console.log(`[migrate-qoo10] ✅ 退避 (${LEGACY_NAME}) + backfill 完了 (${r.changes} 行 INSERT)`);
      db.exec('COMMIT');
    } catch (e) {
      try { db.exec('ROLLBACK'); } catch (_) {}
      console.error(`[migrate-qoo10] ROLLBACK: ${e.message}`);
      process.exit(1);
    }
    return;
  }
  if (s.hasRaw && s.rawIsLegacy && s.hasLegacy) {
    console.error(`[migrate-qoo10] FATAL: 旧スキーマ raw_qoo10_orders + 退避表 ${LEGACY_NAME} の両方が存在 (異常状態、過去 migration が途中で失敗?) → 手動確認要`);
    process.exit(1);
  }
  if (!s.hasRaw && !s.hasLegacy) {
    console.log('[migrate-qoo10] raw_qoo10_orders もなし → 新 DDL を空で作成 (新規環境)');
    if (isDryRun) { console.log('[migrate-qoo10] --dry-run 終了'); return; }
    db.exec(NEW_DDL);
    for (const idx of NEW_INDEXES) db.exec(idx);
    console.log('[migrate-qoo10] ✅ 新 DDL 作成完了 (0 行)');
    return;
  }
  // 想定外のスキーマ (新でも旧でもない)
  console.error(`[migrate-qoo10] FATAL: 想定外のスキーマ raw_qoo10_orders → 手動確認要 (cols: 旧 11 / 新 30+)`);
  process.exit(1);
}

main();
