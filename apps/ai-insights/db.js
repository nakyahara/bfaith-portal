// ai-insights: AI経営アドバイス定期配信 (週次/月次) の Render 側テーブル群
//
// 要件定義: AI_reference『システム設計/AI経営アドバイス定期配信_要件定義_20260719.md』
// (Codex 5ラウンド意見交換で critical/high なし収束済みの設計を実装)
//
// すべて warehouse-mirror.db に同居する Render ローカル書き込みテーブル (sync 対象外)。
// 命名: ai_* = 本アプリ所有。mirror_* (同期受信) / mart_* (MF系ローカル) とは衝突しない。
//
// ⚠️ init は fail-soft 必須 (2026-07-12 mirror全停止事故の教訓 / #482):
//    本モジュールの initAiInsightsTables() は throw し得るが、呼び出し側 (router.js) が
//    try/catch で受けて aiInsightsInitError に記録し、mirror 全体を道連れにしない。
//
// 設計メモ:
// - 金額はすべて INTEGER 円 (REAL 禁止)。粗利率予算は保存しない (粗利額÷売上で導出。
//   額と率の不整合が構造的に起きない = Codex 指摘の整合性チェックを設計で不要化)
// - ai_budget は append-only (revision_no)。現在値は v_ai_budget_current
// - ai_events は append-only + 論理削除 (deleted_at)。編集 = 削除して再登録
// - ai_report_jobs: pending → generating → generated → posting → posted / failed /
//   reconciliation_required。UNIQUE(report_type, period_start, period_end, edition)
// - ID は日時 ms + 乱数 (同分内 PK collision 防止 INV-21)

const SCHEMA_STATEMENTS = [
  // ── レポート本体 (全文保存。GChat には要約版のみ投稿) ──
  `CREATE TABLE IF NOT EXISTS ai_reports (
    report_id      TEXT PRIMARY KEY,        -- rp_<ms>_<rand>
    public_id      TEXT NOT NULL UNIQUE,    -- 投稿本文埋め込み用短ID (例 WK-20260713-f)
    report_type    TEXT NOT NULL CHECK(report_type IN ('weekly','monthly')),
    period_start   TEXT NOT NULL,           -- YYYY-MM-DD (JST、週次=月曜)
    period_end     TEXT NOT NULL,           -- YYYY-MM-DD (JST、排他的境界)
    edition        TEXT NOT NULL,           -- final / provisional / correction-N
    data_as_of     TEXT,
    data_quality_status TEXT,               -- normal / degraded / fallback
    prompt_version TEXT,
    input_schema_version TEXT,
    generation_mode TEXT NOT NULL CHECK(generation_mode IN ('claude','fallback')),
    input_hash     TEXT,
    body_json      TEXT NOT NULL,           -- フルスキーマ (総括+論点+助言+根拠)
    gchat_message_id TEXT,
    body_hash      TEXT,
    posted_at      TEXT,
    budget_revision_id TEXT,                -- 生成時に参照した予算の最新 budget_row_id
    created_at     TEXT NOT NULL,
    UNIQUE(report_type, period_start, period_end, edition)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ai_reports_period
     ON ai_reports(report_type, period_start)`,

  // ── ジョブ状態機械 (冪等性・配信状態管理。Codexラウンド2/3 High対応) ──
  `CREATE TABLE IF NOT EXISTS ai_report_jobs (
    job_id       TEXT PRIMARY KEY,          -- jb_<ms>_<rand>
    report_type  TEXT NOT NULL CHECK(report_type IN ('weekly','monthly')),
    period_start TEXT NOT NULL,
    period_end   TEXT NOT NULL,
    edition      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending' CHECK(status IN
      ('pending','generating','generated','posting','posted','failed','reconciliation_required')),
    claimed_by   TEXT,
    claimed_at   TEXT,
    heartbeat_at TEXT,
    input_hash   TEXT,
    report_id    TEXT,
    error_class  TEXT,                      -- claude_auth / claude_limit / claude_invalid_json /
                                            -- generation_failed / post_failed / orphaned ...
    error_detail TEXT,
    resolved_by  TEXT,                      -- reconciliation を処理した人
    resolved_at  TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    UNIQUE(report_type, period_start, period_end, edition)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ai_jobs_status ON ai_report_jobs(status)`,

  // ── 論点 (前回未解決論点の持ち越し + 第3段階承認フローの土台) ──
  `CREATE TABLE IF NOT EXISTS ai_report_topics (
    topic_id   TEXT PRIMARY KEY,            -- tp_<ms>_<rand>
    report_id  TEXT NOT NULL REFERENCES ai_reports(report_id),
    seq        INTEGER NOT NULL,            -- レポート内の論点順 (1..3)
    title      TEXT NOT NULL,
    category   TEXT,                        -- sales / margin / inventory / cash / other
    status     TEXT NOT NULL DEFAULT 'open' CHECK(status IN
      ('open','carried','resolved','worsened','dismissed')),
    body_json  TEXT,                        -- 論点のフルスキーマ (根拠・仮説・アクション等)
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ai_topics_report ON ai_report_topics(report_id)`,
  `CREATE INDEX IF NOT EXISTS idx_ai_topics_status ON ai_report_topics(status)`,

  // ── 予算 (全社×月次。append-only revision) ──
  // metric に gross_margin_pct は無い: 粗利率 = gross_profit / sales で導出する設計
  `CREATE TABLE IF NOT EXISTS ai_budget (
    budget_row_id TEXT PRIMARY KEY,         -- bg_<ms>_<rand>
    year_month  TEXT NOT NULL,              -- YYYY-MM (regex検証 + padStart 組立のみ)
    metric      TEXT NOT NULL CHECK(metric IN
      ('sales','gross_profit','operating_income','cash_eom','inventory_value')),
    budget_kind TEXT NOT NULL DEFAULT 'initial' CHECK(budget_kind IN
      ('initial','revised','forecast')),    -- v1 入力は initial のみ (モデル上は区別可能に)
    value_yen   INTEGER NOT NULL,           -- 円 (整数)。空欄は行を作らない (0 と区別)
    revision_no INTEGER NOT NULL,
    reason      TEXT,
    entered_by  TEXT NOT NULL,
    entered_at  TEXT NOT NULL,
    UNIQUE(year_month, metric, budget_kind, revision_no)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ai_budget_ym ON ai_budget(year_month)`,

  // 現在値ビュー (metric×月×kind ごとに最大 revision の行)
  `CREATE VIEW IF NOT EXISTS v_ai_budget_current AS
   SELECT b.* FROM ai_budget b
   WHERE b.revision_no = (
     SELECT MAX(b2.revision_no) FROM ai_budget b2
     WHERE b2.year_month = b.year_month
       AND b2.metric = b.metric
       AND b2.budget_kind = b.budget_kind
   )`,

  // ── イベントメモ (施策・イベント履歴の軽量版。日付+一行を分離 = Codex推奨形) ──
  `CREATE TABLE IF NOT EXISTS ai_events (
    event_id    TEXT PRIMARY KEY,           -- ev_<ms>_<rand>
    event_date  TEXT NOT NULL,              -- YYYY-MM-DD (開始日)
    event_end_date TEXT,                    -- 期間イベントの終了日 (単日は NULL)
    description TEXT NOT NULL,
    tag         TEXT CHECK(tag IN ('sale','price','ad','incident','procure','other') OR tag IS NULL),
    scope       TEXT,                       -- 'all' またはモール名 (任意)
    entered_by  TEXT NOT NULL,
    entered_at  TEXT NOT NULL,
    deleted_at  TEXT,                       -- 論理削除 (履歴保持)
    deleted_by  TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ai_events_date ON ai_events(event_date)`,

  // ── 月次締め状態 (PR-3。要件 §6 の状態機械の「人間宣言」部分) ──
  // 未締め(open) → 確定宣言(declared) → [誤操作時 reopened → 再宣言で declared]
  // 暫定版/確定版/訂正版の発行状態は ai_report_jobs/ai_reports (edition) が持つ。
  // declare_seq: 宣言のたびに +1。確定版発行時の seq (final_declare_seq) より大きければ
  // 「再オープン→再宣言」なので次の確定は correction 版として発行する
  `CREATE TABLE IF NOT EXISTS ai_monthly_closing (
    year_month   TEXT PRIMARY KEY,        -- YYYY-MM
    status       TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','declared','reopened')),
    declare_seq  INTEGER NOT NULL DEFAULT 0,
    declared_by  TEXT,
    declared_at  TEXT,
    reopened_by  TEXT,
    reopened_at  TEXT,
    final_report_id   TEXT,               -- 確定版 ai_reports.report_id
    final_declare_seq INTEGER,
    final_pl_hash     TEXT,               -- 確定版生成時の対象月PLハッシュ (確定後変更検知)
    change_notified_at TEXT,              -- 「確定後変更あり」通知済みマーカー
    reminder_15_sent_at TEXT,
    reminder_20_sent_at TEXT,
    sync_stall_notified_at TEXT,          -- 宣言後7日MF同期なし警告の通知済みマーカー
    updated_at   TEXT NOT NULL
  )`,

  // ── 期待データソースマスタ (充足判定の基準。有効期間付き) ──
  // requirement: required=欠損なら縮退/生成なし、conditional=該当論点の生成禁止、reference=参考
  `CREATE TABLE IF NOT EXISTS ai_expected_sources (
    source_id   TEXT PRIMARY KEY,           -- 'sales:rakuten' 等の決定的キー
    source_kind TEXT NOT NULL CHECK(source_kind IN ('sales','finance','inventory','po','mf')),
    unit        TEXT NOT NULL,              -- モール名 / 在庫category / 'pml' / 'mf'
    requirement TEXT NOT NULL CHECK(requirement IN ('required','conditional','reference')),
    valid_from  TEXT NOT NULL,              -- YYYY-MM-DD
    valid_to    TEXT,                       -- NULL = 現役。休止・取得停止時に設定
    note        TEXT,
    updated_by  TEXT,
    updated_at  TEXT NOT NULL
  )`,
];

// 期待データソースの初期セット (存在しない行のみ投入。運用中の変更は ⚙️ 画面 or 直接SQL)
// 売上 (f_sales_by_listing) = required だが判定は per-mall 縮退 (要件 §5.4)
// finance (SKU粗利) = conditional (欠損モールの粗利論点を生成禁止)
// 在庫 = fba_warehouse/own_warehouse を required、fba_inbound は reference
const EXPECTED_SOURCE_SEEDS = [
  ...['amazon', 'rakuten', 'yahoo', 'aupay', 'linegift', 'qoo10'].flatMap((mall) => [
    { source_id: `sales:${mall}`, source_kind: 'sales', unit: mall, requirement: 'required' },
    { source_id: `finance:${mall}`, source_kind: 'finance', unit: mall, requirement: 'conditional' },
  ]),
  { source_id: 'inventory:fba_warehouse', source_kind: 'inventory', unit: 'fba_warehouse', requirement: 'required' },
  { source_id: 'inventory:own_warehouse', source_kind: 'inventory', unit: 'own_warehouse', requirement: 'required' },
  { source_id: 'inventory:fba_inbound', source_kind: 'inventory', unit: 'fba_inbound', requirement: 'reference' },
  { source_id: 'po:pml', source_kind: 'po', unit: 'pml', requirement: 'conditional' },
  { source_id: 'mf:mf', source_kind: 'mf', unit: 'mf', requirement: 'reference' },
];

/**
 * ai-insights テーブル群を作成 (冪等)。
 * 呼び出し側 (router.js) が try/catch + initError 記録で fail-soft にする契約。
 */
export function initAiInsightsTables(db) {
  for (const stmt of SCHEMA_STATEMENTS) {
    db.prepare(stmt).run();
  }
  // PR-3 追加列 (PR-1 で本番作成済みのテーブルへの追加は ALTER で冪等に)
  // declare_seq: 月次ジョブが「どの宣言に対する生成か」を固定する (生成中の再オープン/再宣言を
  // 誤って確定扱いしないため。Codex PR-3 レビュー high 対応)
  const jobCols = db.prepare('PRAGMA table_info(ai_report_jobs)').all().map((c) => c.name);
  if (!jobCols.includes('declare_seq')) {
    db.prepare('ALTER TABLE ai_report_jobs ADD COLUMN declare_seq INTEGER').run();
  }
  // 1宣言 = 1 correction を DB レベルでも保証 (並行 claim の二重INSERT防止)
  db.prepare(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_jobs_correction_per_declare
    ON ai_report_jobs(report_type, period_start, period_end, declare_seq)
    WHERE edition LIKE 'correction-%' AND declare_seq IS NOT NULL
  `).run();
  const seedStmt = db.prepare(`
    INSERT OR IGNORE INTO ai_expected_sources
      (source_id, source_kind, unit, requirement, valid_from, updated_by, updated_at)
    VALUES (?, ?, ?, ?, ?, 'seed', ?)
  `);
  const now = new Date().toISOString();
  for (const s of EXPECTED_SOURCE_SEEDS) {
    seedStmt.run(s.source_id, s.source_kind, s.unit, s.requirement, '2026-01-01', now);
  }
}

/** 日時 ms + 乱数の一意 ID (INV-21: 同分内 collision 防止) */
export function newId(prefix) {
  const rand = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${Date.now()}_${rand}`;
}

export function nowIso() {
  return new Date().toISOString();
}

/** 'YYYY-MM-DD' 形式チェック (実在日チェックは呼び出し側で必要なら) */
export function isYmd(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

/** 'YYYY-MM' 形式チェック */
export function isYm(s) {
  return typeof s === 'string' && /^\d{4}-(0[1-9]|1[0-2])$/.test(s);
}

/**
 * public_id 生成 (GChat 投稿本文に埋め込む短ID。運用上の二重投稿検出用)
 * weekly: WK-20260713-f / WK-20260713-c1
 * monthly: MO-202607-p / MO-202607-f / MO-202607-c2
 */
export function buildPublicId(reportType, periodStart, edition) {
  const typePrefix = reportType === 'weekly' ? 'WK' : 'MO';
  const compact = reportType === 'weekly'
    ? periodStart.replaceAll('-', '')
    : periodStart.slice(0, 7).replace('-', '');
  let ed;
  if (edition === 'final') ed = 'f';
  else if (edition === 'provisional') ed = 'p';
  else if (/^correction-\d+$/.test(edition)) ed = `c${edition.split('-')[1]}`;
  else ed = edition;
  return `${typePrefix}-${compact}-${ed}`;
}
