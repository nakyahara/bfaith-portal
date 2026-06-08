-- Phase A: readiness_check (Codex Round 1 Critical-2) のための jobs 列追加。
--
-- display=1 昇格の前に必須項目 11 個を機械判定し、 1 つでも fail なら display=0 維持。
-- 判定結果は jobs 行に保持して publish 試行前/後に再評価する。
--
--   readiness_status         : 'pending' (未評価) / 'ok' (全項目 pass) / 'blocked' (1 個以上 fail)
--   readiness_blocked_reasons: JSON array of reason codes、 例 ['notion_title_missing', 'tax_rate_mismatch']
--   last_readiness_at        : 最後に readiness_check した時刻 (再評価判断用)
--
-- 既存 jobs 行 (W1 までの 7 件 published_verified + その他) は readiness_status='pending' になる。
-- field-mapper / publish-runner は publish 試行前に readiness_check を再評価して、
-- 'blocked' なら display=0 維持 + reasons を返す。

ALTER TABLE jobs ADD COLUMN readiness_status TEXT NOT NULL DEFAULT 'pending'
  CHECK(readiness_status IN ('pending', 'ok', 'blocked'));

-- Codex R1 Medium-2: JSON 配列型を強制 (object/string/number で reason code 前提が崩れるのを防ぐ)。
-- Codex R1 Medium-3: readiness_status と reasons の相互整合を制約化。
-- Codex R2 Medium-1: blocked なら配列長 >= 1 必須 (空配列 [] = 「fail 0 件なのに blocked」の仕様矛盾防止)。
--   pending/ok → reasons は NULL 必須 (古い blocked reason が残る事故防止)
--   blocked    → reasons は valid JSON 配列 (要素 1 個以上) 必須
ALTER TABLE jobs ADD COLUMN readiness_blocked_reasons TEXT
  CHECK(
    (readiness_status IN ('pending', 'ok') AND readiness_blocked_reasons IS NULL)
    OR
    (readiness_status = 'blocked'
      AND readiness_blocked_reasons IS NOT NULL
      AND json_valid(readiness_blocked_reasons)
      AND json_type(readiness_blocked_reasons) = 'array'
      AND json_array_length(readiness_blocked_reasons) > 0)
  );

ALTER TABLE jobs ADD COLUMN last_readiness_at TEXT;

CREATE INDEX idx_jobs_readiness_status ON jobs(readiness_status);
