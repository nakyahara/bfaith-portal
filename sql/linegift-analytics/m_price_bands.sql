-- m_price_bands: 価格帯マスタ (8段階、SCD2 履歴管理対応)
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2
--
-- 中原さんが既存 Looker Studio で運用している 8段階 (¥0-499 / ¥500-999 / ... / ¥6000+) を踏襲。
-- 全モール共通利用前提 (LINEギフト Phase 1 で先行投入、他モール横展開予定)。
--
-- 価格帯派生ルール:
--   master 系: m_product_classifications.price_band_code_master ← m_products.標準売価 ベース、SKU sticky
--   sales 系: 別 view (v_linegift_price_band_summary 等) で 90日平均単価から動的派生
--   両者一致を保証しない (Codex Round 4 R4-3 反映)

CREATE TABLE IF NOT EXISTS m_price_bands (
  band_code            TEXT PRIMARY KEY,                                   -- 'B0_499' / 'B500_999' / ...
  band_label           TEXT NOT NULL,                                      -- '0〜499円' 表示用
  min_price_jpy_incl   INTEGER NOT NULL,                                   -- 包含 (>=)
  max_price_jpy_incl   INTEGER,                                            -- 包含 (<=)、NULL = 上限なし
  sort_order           INTEGER NOT NULL,
  is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  valid_from           TEXT NOT NULL,
  valid_to             TEXT,
  synced_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_m_price_bands_active
  ON m_price_bands (is_active, sort_order);

-- 初期 seed (Looker 既存 8段階を踏襲、2026-05-26 設計時点)
-- 既存行があれば INSERT OR IGNORE で no-op
INSERT OR IGNORE INTO m_price_bands (band_code, band_label, min_price_jpy_incl, max_price_jpy_incl, sort_order, valid_from, synced_at) VALUES
  ('B0_499',    '0〜499円',           0,    499,  1, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('B500_999',  '500〜999円',         500,  999,  2, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('B1000_1999','1,000〜1,999円',     1000, 1999, 3, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('B2000_2999','2,000〜2,999円',     2000, 2999, 4, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('B3000_3999','3,000〜3,999円',     3000, 3999, 5, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('B4000_4999','4,000〜4,999円',     4000, 4999, 6, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('B5000_5999','5,000〜5,999円',     5000, 5999, 7, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('B6000_PLUS','6,000円以上',        6000, NULL, 8, '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00');
