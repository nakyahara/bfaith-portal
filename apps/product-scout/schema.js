/**
 * 新商品企画スカウト (apps/product-scout) のテーブル定義。
 *
 * warehouse-mirror.db に同居する。DDL をここに置いてあるのは、
 * mirror 本体を起動せずに単体で検証できるようにするため (test-schema.mjs)。
 */
// ▼▼▼ 新商品企画スカウト (apps/product-scout) 正本テーブル ▼▼▼
// miniPC の product-idea-scout (Keepa収集 → concepts.js でテーマ集約) が push したものを受ける。
// 「Render 完結書込」ではなく「miniPC が push、Render が正本」— 判断 (採否) は Render 側でしか起きず、
// miniPC には戻さないため、ここが唯一の正本になる。
//
// ⭐設計の肝: 現在状態を concepts 側に直接上書きしない。
//   「以前は不採用 → 条件が変わって再審査 → 採用」を残せなくなるため、
//   採否は scout_decisions への追記だけで表現し、現在状態は最新イベントから導出する。
//   不採用理由の蓄積こそがこのツールの資産 (中原さんの明示方針)。
export function createProductScoutTables(db) {
  // 取り込み単位。同じ concept を何度取り込んでも履歴が追えるようにする
  db.exec(`
    CREATE TABLE IF NOT EXISTS scout_snapshots (
      snapshot_id        TEXT PRIMARY KEY,
      generated_at       TEXT NOT NULL,
      ingested_at        TEXT NOT NULL,
      algorithm_version  INTEGER NOT NULL,
      source_products    INTEGER,
      after_base_filter  INTEGER,
      concept_count      INTEGER,
      -- ⭐「最後に前進した時刻」。件数の比較だけで「収集中」を名乗ると、
      --   Keepa が返さない数件が残ったカテゴリが永遠に収集中になり、
      --   止まっていても信号が緑のままになる (2026-08-07〜27 の空回りが別の形で戻る)。
      last_progress_at   TEXT,
      remaining_total    INTEGER
    )
  `);

  // 収集の進捗と「分母の質」。⭐complete=0 のカテゴリは 100% と表示してはいけない
  db.exec(`
    CREATE TABLE IF NOT EXISTS scout_categories (
      snapshot_id        TEXT NOT NULL,
      root_category      TEXT NOT NULL,
      name               TEXT NOT NULL,
      state              TEXT NOT NULL,       -- queued | collecting | collected | not_started
      asin_target        INTEGER,             -- finder が把握している対象ASIN数
      fetched            INTEGER,             -- 商品詳細を取得済みのASIN数
      complete           INTEGER,             -- 1=完全 / 0=不完全 / NULL=旧版で判定不能
      estimated_missing  INTEGER,
      fetched_at         TEXT,
      remaining          INTEGER,
      PRIMARY KEY (snapshot_id, root_category)
    )
  `);

  // 審査対象のテーマ。concept_id は (カテゴリパス + 剤型) から作る安定キーなので、
  // 取り込みし直しても同じテーマには同じ id が付き、過去の判断と繋がる
  db.exec(`
    CREATE TABLE IF NOT EXISTS scout_concepts (
      concept_id            TEXT PRIMARY KEY,
      snapshot_id           TEXT NOT NULL,
      concept               TEXT NOT NULL,
      category_path         TEXT NOT NULL,
      root_category_name    TEXT,
      form                  TEXT,
      amc_capable           TEXT,             -- pass | fail | unknown
      hard_gate             TEXT NOT NULL,    -- pass | fail | unknown
      gate_fail_reason      TEXT,
      commodity             TEXT,             -- ok | suspect
      big_brand             TEXT,             -- open | dominated
      product_count         INTEGER,
      total_monthly_sold    INTEGER,
      brand_count           INTEGER,
      top1_brand            TEXT,
      top1_share_pct        INTEGER,
      top3_share_pct        INTEGER,
      median_price          INTEGER,
      median_fee_pct        REAL,
      small_size_rate_pct   INTEGER,
      ascii_brand_rate_pct  INTEGER,
      median_review_count   INTEGER,
      source_complete       INTEGER,
      source_fetched_at     TEXT,
      examples_json         TEXT,
      rank_in_snapshot      INTEGER,
      first_seen_at         TEXT NOT NULL,
      updated_at            TEXT NOT NULL
    )
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_scout_concepts_gate ON scout_concepts(hard_gate, rank_in_snapshot)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_scout_concepts_snapshot ON scout_concepts(snapshot_id)');

  // 採否イベント (追記専用)。UPDATE / DELETE はトリガーで拒否する
  db.exec(`
    CREATE TABLE IF NOT EXISTS scout_decisions (
      decision_id        TEXT PRIMARY KEY,
      concept_id         TEXT NOT NULL,
      decision           TEXT NOT NULL CHECK (decision IN ('adopt','reject','hold')),
      failed_gate        TEXT,
      reason_code        TEXT,
      comment            TEXT,
      recheck_condition  TEXT,                -- 「原料が30%下がれば再検討」等。不採用台帳を墓場にしない
      decided_by         TEXT NOT NULL,
      decided_at         TEXT NOT NULL,
      snapshot_id        TEXT,
      metrics_json       TEXT,                -- ⭐判断時点の指標を固定する (後知恵で上書きしない)
      prior_decision_id  TEXT
    )
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_scout_decisions_concept ON scout_decisions(concept_id, decided_at)');
  // append-only を DB 側で保証する (アプリのバグで履歴が消えないように)
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_scout_decisions_no_update
    BEFORE UPDATE ON scout_decisions
    BEGIN SELECT RAISE(ABORT, 'scout_decisions は追記専用です'); END
  `);
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_scout_decisions_no_delete
    BEFORE DELETE ON scout_decisions
    BEGIN SELECT RAISE(ABORT, 'scout_decisions は追記専用です'); END
  `);
}
