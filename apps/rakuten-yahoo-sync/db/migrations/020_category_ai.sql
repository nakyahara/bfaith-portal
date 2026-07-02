-- 再設計 R2 (2026-07-02): AI 初期カテゴリ紐づけ。
--
-- 中原さん決定 (2026-07-02 ヒアリング): カテゴリ変換テーブルの初期値は Claude Code が
-- オフラインで一括生成する (アプリに LLM API は組み込まない)。 運用中の新カテゴリは
-- 紐付け画面で人間が AI 候補から選んで確定する。
--
-- 生成プロセス (precision first):
--   1. 機械候補生成: rakuten_genre 3,551 × yahoo_category_master 12,044 を
--      lexical (名前/階層セグメント) + 観測 prior でスコアリング → genre ごと候補 top8
--   2. 正規化名が完全一致かつ一意 → exact_match として自動確定
--   3. 残りは Claude (subagent バッチ) が「候補から選ぶだけ」で判定。 自信がなければ棄権
--   4. 機械検証: 選択値が候補内・マスタに実在・is_active を確認してから seed 化
--
-- resolver での位置づけ: Notion > manual > decisions > learned > 【ai】 > unresolved
--   (AI 推定は実出品の観測より弱い根拠なので実績系 tier の下、 空白を埋める最終 fallback)
--   path は category_default_path からのみ補完。 無ければ ai_path_missing (店の棚は人の管轄)。
--
-- seed データは migration 021 (機械生成) で投入。 schema と分離するが、 021 は INSERT のみで
-- 021 失敗時も 020 の表が空で残るだけ (resolver は fail-soft) なので 019 のような統合は不要。

-- AI 確定分 (1 genre = 1 決定)
-- Codex R2 R1 Medium: confidence は NOT NULL + 0..1 CHECK (seed 生成ミスの混入を schema で弾く)。
--   resolver 側も exact_match 以外は confidence >= 0.6 を要求する二重ゲート。
CREATE TABLE category_ai (
  rakuten_genre_id   TEXT PRIMARY KEY,
  yahoo_category_id  INTEGER NOT NULL,
  confidence         REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),  -- exact_match は 1.0
  decided_by         TEXT NOT NULL DEFAULT 'llm'
                       CHECK(decided_by IN ('exact_match', 'llm')),
  note               TEXT,                                   -- 判定理由 (短文)
  created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- AI 候補 top3 (全 genre、 棄権分含む。 紐付け画面 v2 の「候補から選ぶ」表示素)
-- Codex R2 R1 Low: 同一 genre への同一カテゴリ重複投入を UNIQUE で弾く。
CREATE TABLE category_ai_candidates (
  rakuten_genre_id   TEXT NOT NULL,
  rank               INTEGER NOT NULL CHECK(rank BETWEEN 1 AND 3),
  yahoo_category_id  INTEGER NOT NULL,
  score              REAL,                                   -- 機械スコア (参考値)
  PRIMARY KEY (rakuten_genre_id, rank),
  UNIQUE (rakuten_genre_id, yahoo_category_id)
);
