-- 楽天ジャンルの補完を最後に試した時刻 (2026-10-22)。
--
-- 旧: 補完の対象は「ジャンルが空の候補を first_detected_at の古い順に 100 件」。楽天側にジャンルが無い・
--     取得に失敗する行は埋まらないまま古い順の先頭に残り続け、100 件以上たまると毎回同じ行だけを試して
--     0 件更新 → 全部更新はそこで打ち切り、後ろの埋められる行に一度も届かなかった。
-- 新: 試した行にこの時刻を付け、「まだ試していない → 試したのが古い」順に拾う (lib/rakuten-title-backfill.js)。
ALTER TABLE migration_candidates ADD COLUMN genre_backfill_attempted_at TEXT;
