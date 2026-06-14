-- Phase E-8: 楽天画像ファイル名パターンによる Yahoo 移行除外。
-- 設計原則:
--   - lib/yahoo-image.js EXCLUDED_URL_PATTERNS (`coupon`/`review`) は **hardcode のまま維持**。
--     これらは 「組込み」 として常に有効、 UI からは削除不可。 ユーザーは UI から追加分のみ管理。
--   - 中原さん R1 確定: 部分一致 + 大文字小文字区別なし。 楽天画像 URL を小文字化して
--     pattern を含むかで判定。
--   - 「除外している」 ことが UI で見えるよう、 reason を保持して詳細ドロワーで表示できる。
CREATE TABLE image_exclusion_patterns (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  pattern     TEXT NOT NULL,                                              -- マッチ対象文字列 (小文字化後 includes 判定)
  reason      TEXT NOT NULL,                                              -- なぜ除外するか (UI で見せる)
  created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  created_by  TEXT NOT NULL DEFAULT 'manual',
  CHECK(length(trim(pattern)) > 0)
);

-- 同 pattern の重複登録は防ぐ (case-insensitive 比較)
CREATE UNIQUE INDEX idx_image_exclusion_patterns_unique
  ON image_exclusion_patterns(lower(trim(pattern)));
