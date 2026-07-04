/**
 * product-hub (商品登録一元化) DB 初期化。
 *
 * 設計: 要件定義 = AI_reference『システム設計/商品登録一元化_要件定義_20260703.md』
 *   - warehouse-mirror.db 同居 (inventory-monthly と同じ Pattern)
 *   - 冪等マイグレーション: CREATE TABLE IF NOT EXISTS + PRAGMA table_info → ALTER
 *   - 金額は整数 (円)。REAL 禁止
 *   - draft_events は append-only の監査ログ
 *
 * ステータス遷移 (§3):
 *   draft(下書き) → ready_for_ai(生成待ち) → review(レビュー待ち) → approved(承認済み)
 *   → listed(楽天出品済み) → expanded(展開済み)。どこからでも on_hold / excluded へ退避可。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

export const DRAFT_STATUSES = [
  'draft', 'ready_for_ai', 'review', 'approved', 'listed', 'expanded', 'on_hold', 'excluded',
];

export const STATUS_LABELS = {
  draft: '下書き',
  ready_for_ai: '生成待ち',
  review: 'レビュー待ち',
  approved: '承認済み',
  listed: '楽天出品済み',
  expanded: '展開済み',
  on_hold: '保留',
  excluded: '除外',
};

// AI 出力スロット (§4/§6: 自由文でなくスロット構造。店舗共通フッターは出品時にシステム結合)
export const AI_OUTPUT_KINDS = [
  'rakuten_title', 'yahoo_title', 'desc_catch', 'desc_features', 'desc_spec', 'desc_notes',
];

let initialized = false;

export function initProductHubDB() {
  if (initialized) return getMirrorDB();
  const db = getMirrorDB();

  // 注意: CHECK 制約は CREATE TABLE 時のみ有効。本アプリは PR #1 で新規テーブルとして
  // デプロイされるため既存 DB の retrofit は不要だが、将来 CHECK を変更する場合は
  // テーブル再作成型マイグレーション (swap) が必要 (Codex R2 low)。
  db.exec(`
    CREATE TABLE IF NOT EXISTS product_drafts (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      ne_code             TEXT NOT NULL UNIQUE,
      name                TEXT NOT NULL,
      status              TEXT NOT NULL DEFAULT 'draft'
                          CHECK (status IN ('draft','ready_for_ai','review','approved','listed','expanded','on_hold','excluded')),
      official_url        TEXT,
      price               INTEGER             -- 売価 (税込・円・整数)
                          CHECK (price IS NULL OR (price BETWEEN 0 AND 1000000000000)),
      jan_code            TEXT,
      has_variation       INTEGER NOT NULL DEFAULT 0 CHECK (has_variation IN (0, 1)),
      drive_folder_url    TEXT,               -- 商品画像フォルダ (商品コード_商品名 規則を推奨)
      memo                TEXT,
      notion_page_id      TEXT,
      notion_card_status  TEXT NOT NULL DEFAULT 'pending'
                          CHECK (notion_card_status IN ('pending','creating','created','failed')),
      notion_card_error   TEXT,
      notion_card_claim   TEXT,               -- creating 中の claim token (stale 奪取の二重作成防止)
      notion_card_attempts INTEGER NOT NULL DEFAULT 0,
      created_by          TEXT,
      created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_product_drafts_status ON product_drafts(status);
    CREATE INDEX IF NOT EXISTS idx_product_drafts_notion ON product_drafts(notion_card_status);

    CREATE TABLE IF NOT EXISTS draft_reference_urls (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      url        TEXT NOT NULL,
      sort       INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_draft_refs_draft ON draft_reference_urls(draft_id);

    CREATE TABLE IF NOT EXISTS draft_images (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id         INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      drive_file_id    TEXT NOT NULL,
      drive_url        TEXT,
      sort             INTEGER NOT NULL DEFAULT 0,
      validation_error TEXT,
      created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      UNIQUE(draft_id, drive_file_id)
    );
    CREATE INDEX IF NOT EXISTS idx_draft_images_draft ON draft_images(draft_id);

    CREATE TABLE IF NOT EXISTS draft_specs (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      spec_key   TEXT NOT NULL,
      spec_value TEXT,
      sort       INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_draft_specs_draft ON draft_specs(draft_id);

    CREATE TABLE IF NOT EXISTS draft_ai_outputs (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id        INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      kind            TEXT NOT NULL,
      content         TEXT,
      generated_at    TEXT,
      model_note      TEXT,
      edited_by_human INTEGER NOT NULL DEFAULT 0,
      UNIQUE(draft_id, kind)
    );

    CREATE TABLE IF NOT EXISTS draft_events (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL,
      event      TEXT NOT NULL,
      detail     TEXT,
      actor      TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_draft_events_draft ON draft_events(draft_id);
  `);

  // 既存 DB へのカラム追加 (warehouse-mirror/db.js の addColumnIfMissing と同方針の冪等 ALTER)
  const draftCols = new Set(db.prepare('PRAGMA table_info(product_drafts)').all().map((c) => c.name));
  if (!draftCols.has('notion_card_claim')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN notion_card_claim TEXT');
  }

  // draft_events は append-only (mis-shipment と同じ trigger ガード)
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_draft_events_no_update
    BEFORE UPDATE ON draft_events
    BEGIN SELECT RAISE(ABORT, 'draft_events is append-only'); END;
  `);
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_draft_events_no_delete
    BEFORE DELETE ON draft_events
    BEGIN SELECT RAISE(ABORT, 'draft_events is append-only'); END;
  `);

  initialized = true;
  return db;
}

export function getDB() {
  return initProductHubDB();
}

export function logEvent(db, draftId, event, detail, actor) {
  db.prepare(`
    INSERT INTO draft_events (draft_id, event, detail, actor) VALUES (?, ?, ?, ?)
  `).run(draftId, event, detail == null ? null : String(detail), actor || null);
}

/**
 * 生成待ち (ready_for_ai) に進めるための必須条件チェック (§4 参考URL必須ゲート)。
 * @returns {string[]} 不足理由 (空配列 = 進める)
 */
export function gateReasons(db, draft) {
  const reasons = [];
  if (!draft.name || !String(draft.name).trim()) reasons.push('商品名が未入力です');
  if (!draft.ne_code || !String(draft.ne_code).trim()) reasons.push('NE商品コードが未入力です');
  if (!draft.official_url || !String(draft.official_url).trim()) reasons.push('公式ページURLが未入力です');
  const imgCount = db.prepare('SELECT COUNT(*) AS c FROM draft_images WHERE draft_id = ?').get(draft.id).c;
  if (imgCount === 0) reasons.push('商品画像 (Driveリンク) が1枚もありません');
  return reasons;
}

/**
 * ゲート必須項目が後から壊された場合 (公式URL削除・最後の画像削除など) に
 * ready_for_ai を draft に自動差し戻す (Codex R1 high 対応: ゲートすり抜け防止)。
 * @returns {string[]|null} 差し戻した場合はその理由、しなかった場合は null
 */
export function demoteIfGateBroken(db, draftId, actor) {
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft || draft.status !== 'ready_for_ai') return null;
  const reasons = gateReasons(db, draft);
  if (reasons.length === 0) return null;
  db.prepare(`
    UPDATE product_drafts SET status = 'draft', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ? AND status = 'ready_for_ai'
  `).run(draftId);
  logEvent(db, draftId, 'auto_demoted_to_draft', reasons.join(' / '), actor);
  return reasons;
}
