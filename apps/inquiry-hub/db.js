/**
 * inquiry-hub DB — EC問い合わせ管理システム (メールディーラー置き換え)
 *
 * 設計書: AI_reference/システム設計/問い合わせ管理システム_設計書_v1.2_20260716.md (§6)
 * Codexアーキテクチャレビュー4巡承認済みのスキーマをそのまま実装する。
 *
 * warehouse-mirror.db とは分離した専用DB (inquiry-hub.db) を DATA_DIR に持つ。
 * 問い合わせデータは本アプリが正本 (外部モール由来の生データ + 社内ワークフロー状態)。
 *
 * 方針 (設計書§6):
 *   - PRAGMA foreign_keys=ON + WAL を毎接続で明示
 *   - 外部由来レコードの外部IDは NOT NULL (SQLiteのUNIQUEはNULL重複を許すため)
 *   - internal_status (社内) と external_status (モール側) を分離
 *   - 削除は論理削除のみ (is_archived)。物理削除機能は作らない
 *   - FTS5は使わない (LIKE検索+インデックス)
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'inquiry-hub.db');

let db = null;

/**
 * 日時の正準形式 (Codex R1 medium: 形式混在によるTEXT比較破綻の防止)。
 * 本DBの日時カラムは全て UTC 'YYYY-MM-DDTHH:MM:SSZ' (秒精度・ミリ秒なし) で保存する。
 * DB DEFAULT (strftime) も同形式。外部由来の日時 (Gmail/楽天/Yahoo!) は
 * 同期アダプターが必ずこの関数を通してから INSERT すること。
 */
export function toUtcIso(input) {
  const t = input instanceof Date ? input.getTime()
    : typeof input === 'number' ? input
    : Date.parse(String(input));
  if (Number.isNaN(t)) throw new Error(`不正な日時: ${input}`);
  return new Date(t).toISOString().slice(0, 19) + 'Z';
}

export function initInquiryHubDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) { try { db.close(); } catch { /* close済み等は無視 */ } db = null; }
  db = new Database(DB_FILE);
  // PRAGMAは接続単位。foreign_keys は SQLite デフォルトOFFなので毎接続で明示 (設計書§6)
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL'); // WAL下で安全かつ Render persistent disk の fsync 遅延を回避
  db.pragma('busy_timeout = 5000');
  createTables();
  return db;
}

export function getDB() {
  if (!db) initInquiryHubDB();
  return db;
}

// 既存テーブルへのカラム追加ヘルパー (冪等。warehouse-mirror/db.js と同パターン)
function addColumnIfMissing(table, column, typeClause) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  if (!cols.includes(column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${typeClause}`);
  }
}

function createTables() {
  // 任意フォルダ (スタッフが作る分類箱。2026-08-02 要望)。
  // 「新着/返信処理中/完了」= 対応の進行状態 とは直交する分類軸で、フォルダに入れても
  // 受信トレイからは消えない (入れたまま返信を忘れる事故を作らないため)。
  // inquiries.folder_id から参照されるので inquiries より先に作る
  db.exec(`CREATE TABLE IF NOT EXISTS inquiry_folders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    name_key TEXT,                  -- 重複判定用の正規化キー (NFKC + 小文字。folders.js が入れる)
    sort_order INTEGER NOT NULL DEFAULT 100,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),  -- 削除は論理削除のみ
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  addColumnIfMissing('inquiry_folders', 'name_key', 'TEXT');
  db.exec('UPDATE inquiry_folders SET name_key = LOWER(name) WHERE name_key IS NULL');
  // 同名フォルダの禁止は「有効なもの同士」だけ (削除済みと同じ名前は作り直せる)。
  // 判定はアプリと同じ正規化キーで行う (アプリ側だけの検査だとDBが素通しし、
  // 見た目が同じフォルダが並ぶ。Codexレビュー2巡目 Medium-6)
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_folders_name_active
    ON inquiry_folders(name) WHERE is_active = 1`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_folders_namekey_active
    ON inquiry_folders(name_key) WHERE is_active = 1 AND name_key IS NOT NULL`);

  // 色付きラベル (2026-08-24 中原さん要望。メールディーラーの「ラベルの設定」相当)。
  // フォルダ (分類箱・1件1フォルダ) とは別軸の「目印」。1件1ラベル (メールディーラーと同じ運用)。
  // メールルールの label_id からも参照され、条件一致で取り込み時に自動付与される
  db.exec(`CREATE TABLE IF NOT EXISTS inquiry_labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    name_key TEXT,                  -- 重複判定用の正規化キー (NFKC + 小文字。labels.js が入れる)
    color TEXT NOT NULL DEFAULT '#64748b',  -- チップ背景色 '#rrggbb' (labels.js が検証)
    sort_order INTEGER NOT NULL DEFAULT 100,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),  -- 削除は論理削除のみ
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_labels_namekey_active
    ON inquiry_labels(name_key) WHERE is_active = 1 AND name_key IS NOT NULL`);

  // 店舗・チャネル
  db.exec(`CREATE TABLE IF NOT EXISTS shops (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_type TEXT NOT NULL CHECK(channel_type IN ('email','rakuten','yahoo')),
    shop_name TEXT NOT NULL,
    account_identifier TEXT NOT NULL,        -- 楽天shopId / YahooセラーID / メールアドレス
    authentication_status TEXT NOT NULL DEFAULT 'ok'
      CHECK(authentication_status IN ('ok','expiring','expired','error')),
    auth_expires_at TEXT,
    last_synced_at TEXT,
    executor TEXT NOT NULL DEFAULT 'server' CHECK(executor IN ('server','runner')),
      -- このshopの同期・送信ジョブを実行する主体 (設計書§5.3。runner=ローカルランナー専用)
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    UNIQUE(channel_type, account_identifier),
    UNIQUE(id, channel_type)  -- inquiries の複合FK用 (channel_type と shop の整合を DB で保証。Codex R1 medium)
  )`);

  // 問い合わせチケット
  db.exec(`CREATE TABLE IF NOT EXISTS inquiries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_type TEXT NOT NULL,
    shop_id INTEGER NOT NULL,
    external_inquiry_id TEXT NOT NULL,       -- 楽天inquiryNumber / Yahoo topicId / Gmail threadId
    customer_name TEXT,
    customer_identifier TEXT,
    subject TEXT,
    -- 社内ワークフロー状態 (同期処理は直接触らない。§8.1の遷移ルールのみ)
    internal_status TEXT NOT NULL DEFAULT 'open'
      CHECK(internal_status IN ('open','in_progress','pending','waiting_reply','done')),
    -- 外部モール側の状態 (同期が上書きする。表示専用)
    external_status TEXT,
    external_is_read INTEGER,
    last_external_synced_at TEXT,
    assigned_user_id TEXT,
    order_number TEXT,
    product_code TEXT,
    product_name TEXT,
    is_unread INTEGER NOT NULL DEFAULT 1 CHECK(is_unread IN (0,1)),   -- 社内の未読
    needs_attention INTEGER NOT NULL DEFAULT 0 CHECK(needs_attention IN (0,1)),
    ai_needed INTEGER NOT NULL DEFAULT 0 CHECK(ai_needed IN (0,1,2,3)),
      -- 0:不要 1:AI返信必要 2:社長確認 3:責任者確認 (Codex R1 low: 値域をDBでも保証)
    ai_reply_type TEXT,
    -- 会話リビジョン (メッセージ追加ごとに+1。AI結果の鮮度判定に使用)
    conversation_rev INTEGER NOT NULL DEFAULT 0,
    -- 編集ロック (社内スタッフ間の二重対応防止。外部競合は防げない→§7.5)
    locked_by TEXT,
    locked_at TEXT,
    lock_expires_at TEXT,
    is_archived INTEGER NOT NULL DEFAULT 0 CHECK(is_archived IN (0,1)),  -- 論理削除/旧データ
    received_at TEXT NOT NULL,
    last_message_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    UNIQUE(channel_type, shop_id, external_inquiry_id),
    -- 複合FK: shop の存在 + channel_type の一致を同時に保証 (email shop に楽天チケット等の不整合を防ぐ)
    FOREIGN KEY (shop_id, channel_type) REFERENCES shops(id, channel_type)
  )`);
  // 任意フォルダへの所属 (1件1フォルダ。NULL=未分類)。既存DBへの冪等migration
  addColumnIfMissing('inquiries', 'folder_id', 'INTEGER REFERENCES inquiry_folders(id)');
  // ラベル (1件1ラベル。NULL=ラベルなし。2026-08-24)
  addColumnIfMissing('inquiries', 'label_id', 'INTEGER REFERENCES inquiry_labels(id)');
  // 配信失敗 (バウンス) を最後に観測した日時。NULL=失敗なし (2026-08-26)。
  // 「返信したのに顧客へ届いていない」を画面で気付けるようにするための印。
  // 同期がバウンス通知を検知したときに立ち、その後の返信が実際に送信できた時点で消える
  addColumnIfMissing('inquiries', 'delivery_failed_at', 'TEXT');

  // メッセージ (受信・送信の両方)
  db.exec(`CREATE TABLE IF NOT EXISTS inquiry_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    external_message_id TEXT NOT NULL,       -- Gmail messageId / 楽天reply id / YahooメッセージID。
                                             -- 外部IDが無いケースは決定的synthetic ID
                                             -- 'syn:' + hash(inquiry外部ID + 送受信区分 + 外部登録日時 + 本文hash)
    sender_type TEXT NOT NULL CHECK(sender_type IN ('customer','shop','system')),
    sender_name TEXT,
    message_body_text TEXT,
    message_body_html TEXT,                  -- 表示時は必ずサニタイズ
    is_incoming INTEGER NOT NULL CHECK(is_incoming IN (0,1)),
    sent_by_user_id TEXT,
    outbox_id INTEGER REFERENCES outbox_replies(id),
    sent_at TEXT,
    received_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    UNIQUE(inquiry_id, external_message_id)
  )`);

  // 添付ファイル
  db.exec(`CREATE TABLE IF NOT EXISTS inquiry_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_message_id INTEGER NOT NULL REFERENCES inquiry_messages(id),
    external_attachment_id TEXT NOT NULL,    -- 外部IDが無いチャネルはメッセージ同様の決定的synthetic ID
                                             -- (Codex R1 high: 再同期リトライでの重複登録防止)
    file_name TEXT,                          -- 元ファイル名はメタデータとしてのみ保持
    content_type TEXT,
    file_size INTEGER,
    storage_path TEXT,                       -- サーバー生成UUIDのファイル名 (パストラバーサル防止)
    fetch_status TEXT NOT NULL DEFAULT 'pending' CHECK(fetch_status IN ('pending','fetched','failed')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    UNIQUE(inquiry_message_id, external_attachment_id)
  )`);

  // 返信送信ジョブ (outbox・全チャネル共通)。Step 1 ではテーブルのみ (workerはStep 3以降)
  db.exec(`CREATE TABLE IF NOT EXISTS outbox_replies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    channel_type TEXT NOT NULL,
    client_operation_id TEXT NOT NULL UNIQUE, -- 送信ボタン押下時にUUID発行。リトライでも不変
    body_text TEXT NOT NULL,
    attachments_json TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
      CHECK(status IN ('pending','sending','sent','failed','unknown','needs_review','cancelled')),
    external_reply_id TEXT,
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    base_conversation_rev INTEGER NOT NULL,  -- 送信操作時点の会話リビジョン (競合検知)
    lease_token TEXT,                        -- claim所有者の識別 (多重ワーカー・ゾンビ処理対策)
    lease_until TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    sent_at TEXT,
    -- unknown の人手解決 (§8.3。「確認できない」と「未送信確認済み」を区別する)
    resolution TEXT CHECK(resolution IN ('confirmed_sent','confirmed_not_sent','abandoned')),
    resolved_by TEXT,
    resolved_at TEXT
  )`);
  // 送信と同時に完了にする (メールディーラーの「返信して完了」。2026-07-25 中原さん要望)
  addColumnIfMissing('outbox_replies', 'complete_on_send', 'INTEGER NOT NULL DEFAULT 0');

  // 返信に付ける送信用添付 (2026-08-20 スタッフ要望「PDFなどを添付できるように」)。
  // 受信添付 (inquiry_attachments) はオンデマンド取得だが、送信用はアップロード〜ワーカー送信の間
  // 実体を保持する必要があるためBLOBで持つ (1ファイル5MB・1返信3つまで。reply-attachments.js が検証)。
  // outbox_id NULL = ジョブ未紐付けの下書き添付 (24時間で掃除対象)
  db.exec(`CREATE TABLE IF NOT EXISTS outbox_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    outbox_id INTEGER REFERENCES outbox_replies(id),
    file_name TEXT NOT NULL,
    content_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    body BLOB NOT NULL,
    uploaded_by TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_outbox_attachments_inquiry ON outbox_attachments(inquiry_id)');

  // 同期状態 (チャネル×店舗)
  db.exec(`CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shop_id INTEGER NOT NULL REFERENCES shops(id) UNIQUE,
    committed_until TEXT,                    -- ここまでの期間は完全に取り込み済み (high-water mark)
    observed_until TEXT,                     -- 直近の同期で観測した最大時刻 (committedとは分離)
    sync_cursor TEXT,                        -- Gmail historyId 等チャネル固有カーソル
    lease_until TEXT,                        -- 同期ジョブの多重起動防止リース
    last_sync_started_at TEXT,
    last_sync_completed_at TEXT,
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0
  )`);

  // 同期リースの所有者トークン (Codexレビュー: 期限切れ後の旧ジョブが新ジョブのリースを解除/上書きしないため)
  addColumnIfMissing('sync_state', 'lease_token', 'TEXT');

  // 同期・送信エラーログ
  db.exec(`CREATE TABLE IF NOT EXISTS sync_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shop_id INTEGER REFERENCES shops(id),
    inquiry_id INTEGER REFERENCES inquiries(id),
    error_type TEXT NOT NULL,
    error_detail TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    resolved INTEGER NOT NULL DEFAULT 0 CHECK(resolved IN (0,1)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    resolved_at TEXT
  )`);

  // 操作ログ (構造化。秘密情報・本文全量は入れない)
  db.exec(`CREATE TABLE IF NOT EXISTS inquiry_activity_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    actor_type TEXT NOT NULL CHECK(actor_type IN ('user','ai','system')),
    user_id TEXT,
    action_type TEXT NOT NULL,
    operation_id TEXT,                       -- 送信操作等はclient_operation_idと紐付け
    before_json TEXT,
    after_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);

  // 社内メモ
  db.exec(`CREATE TABLE IF NOT EXISTS internal_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    user_id TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);

  // 署名 (2026-08-27 中原さん要望。メールディーラーの新規メール作成1段目「署名」相当)。
  // 本文の末尾に付ける定型の差出人表記。新規メール作成で選ぶと本文に展開される
  // (展開後は普通のテキストなので、送る前に画面で編集できる = メールディーラーと同じ挙動)。
  // 削除は論理削除のみ (is_active=0)。既定署名は1件だけ (signatures.js が保証)
  db.exec(`CREATE TABLE IF NOT EXISTS inquiry_signatures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    name_key TEXT,                  -- 重複判定用の正規化キー (NFKC + 小文字。signatures.js が入れる)
    body TEXT NOT NULL,
    is_default INTEGER NOT NULL DEFAULT 0 CHECK(is_default IN (0,1)),
    sort_order INTEGER NOT NULL DEFAULT 100,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_signatures_namekey_active
    ON inquiry_signatures(name_key) WHERE is_active = 1 AND name_key IS NOT NULL`);

  // 返信テンプレート (メールディーラーのテンプレートエクスポートCSVを取込)
  db.exec(`CREATE TABLE IF NOT EXISTS reply_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_type TEXT,                       -- NULL=全チャネル共通
    category TEXT,                           -- メールディーラーのテンプレートグループ (階層は ' > ' 区切り)
    template_name TEXT NOT NULL,
    template_body TEXT NOT NULL,             -- 本文 (メールディーラー「本文（上）」)
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
    usage_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  // メールディーラー取込用の追加カラム (Step 1 で作成済みの本番テーブルへの冪等migration)
  addColumnIfMissing('reply_templates', 'subject', 'TEXT');            // 件名
  addColumnIfMissing('reply_templates', 'body_bottom', 'TEXT');        // 本文（下）= 署名等 (引用返信の下に付く部分)
  addColumnIfMissing('reply_templates', 'keywords', 'TEXT');           // 重要キーワード+キーワード
  addColumnIfMissing('reply_templates', 'notes', 'TEXT');              // 備考
  addColumnIfMissing('reply_templates', 'sort_order', 'INTEGER');      // 表示順序
  addColumnIfMissing('reply_templates', 'external_id', 'TEXT');        // メールディーラーテンプレートID (再取込の冪等キー)
  addColumnIfMissing('reply_templates', 'source_created_at', 'TEXT');
  addColumnIfMissing('reply_templates', 'source_updated_at', 'TEXT');
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_tpl_external
    ON reply_templates(external_id) WHERE external_id IS NOT NULL`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_tpl_category ON reply_templates(category, sort_order)');

  // Q&A (社内ナレッジ。メールディーラーのQ&AエクスポートCSVを取込。手動追加も可)
  db.exec(`CREATE TABLE IF NOT EXISTS qa_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT,                        -- メールディーラーQ&A ID (再取込の冪等キー。手動追加はNULL)
    category TEXT,
    title TEXT NOT NULL,                     -- 件名
    question TEXT,
    answer TEXT NOT NULL,
    notes TEXT,
    staff TEXT,                              -- 担当者 (取込元の表示値)
    is_published INTEGER NOT NULL DEFAULT 1 CHECK(is_published IN (0,1)),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),   -- 論理削除
    source_created_at TEXT,
    source_updated_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_external
    ON qa_entries(external_id) WHERE external_id IS NOT NULL`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_qa_category ON qa_entries(category)');

  // AIジョブ (キュー。inquiriesのカラムではなく独立テーブル。処理系はStep 7)
  // メール取込ルール (メールチャネルのノイズ除去。メールディーラー振り分け設定の移行先)
  // action: skip=取り込まない / import_done=取り込んで対応完了扱い (通常取込はルール不要の既定動作)
  // action: skip=取り込まない / import_done=取り込むが完了扱い / import=通常取り込み (フォルダ振り分け用)
  // folder_id: 取り込み時にこのフォルダへ入れる (import / import_done で有効。2026-08-17 Gmail風振り分け)
  const MAIL_RULES_COLS = `
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    priority INTEGER NOT NULL DEFAULT 100,       -- 小さいほど先に評価 (先勝ち)
    name TEXT,
    match_mode TEXT NOT NULL DEFAULT 'all' CHECK(match_mode IN ('all','any')),
    conditions_json TEXT NOT NULL,               -- [{field: from|to|reply_to|subject|body, op: contains|not_contains|equals|not_equals|starts_with|ends_with, value}]
    action TEXT NOT NULL CHECK(action IN ('skip','import_done','import')),
    folder_id INTEGER REFERENCES inquiry_folders(id),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
    external_key TEXT,                           -- メールディーラー条件ID (再取込の冪等キー。手動追加はNULL)
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))`;
  db.exec(`CREATE TABLE IF NOT EXISTS mail_rules (${MAIL_RULES_COLS})`);
  // 移行 (2026-08-17): 旧スキーマは action CHECK が ('skip','import_done') のみで folder_id 列も無い。
  // SQLite は CHECK を変更できないためテーブルを作り直してコピーする (mail_rules を参照する表は無い)
  {
    const ddl = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='mail_rules'").get()?.sql || '';
    if (ddl && !ddl.includes("'import'")) {
      db.transaction(() => {
        db.exec(`CREATE TABLE mail_rules_new (${MAIL_RULES_COLS})`);
        db.exec(`INSERT INTO mail_rules_new (id, priority, name, match_mode, conditions_json, action, is_active, external_key, created_at, updated_at)
          SELECT id, priority, name, match_mode, conditions_json, action, is_active, external_key, created_at, updated_at FROM mail_rules`);
        db.exec('DROP TABLE mail_rules');
        db.exec('ALTER TABLE mail_rules_new RENAME TO mail_rules');
      })();
    }
  }
  // label_id: 取り込み時にこのラベルを付ける (skip 以外で有効。2026-08-24 メールディーラーのラベル振り分け相当)
  addColumnIfMissing('mail_rules', 'label_id', 'INTEGER REFERENCES inquiry_labels(id)');
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_mail_rules_external
    ON mail_rules(external_key) WHERE external_key IS NOT NULL`);

  db.exec(`CREATE TABLE IF NOT EXISTS ai_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','processing','done','failed','stale')),
    input_rev INTEGER NOT NULL,              -- claim時点の conversation_rev
    lease_token TEXT,                        -- claim時に発行。結果POSTで一致必須
    lease_until TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    completed_at TEXT
  )`);

  // AI生成結果 (履歴として蓄積。最新版を詳細画面に表示)
  db.exec(`CREATE TABLE IF NOT EXISTS ai_drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    ai_job_id INTEGER NOT NULL REFERENCES ai_jobs(id),
    input_rev INTEGER NOT NULL,              -- どの会話リビジョンを見て生成したか
    is_stale INTEGER NOT NULL DEFAULT 0 CHECK(is_stale IN (0,1)),  -- 新着で古くなったら1
    summary TEXT,
    category TEXT,
    draft_body TEXT,
    notes TEXT,                              -- AI注意事項
    confirmation_items TEXT,                 -- AI要確認事項
    model_info TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);

  // AIバッチ実行ログ (設計書§9.2。ローカルランナーが1回のバッチごとに記録し、運用管理画面に表示)
  // 移行 (Step 7): Step 1で定義した旧スキーマ (started_at NOT NULL/processed_count/status等) が
  // 既存DBに残っている。旧スキーマは書き込みコードが存在せず常に空のため、空ならDROPして作り直す。
  // 万一データがある場合はDROPせず不足カラムをALTERで足す (Codexレビュー反映)
  {
    const cols = db.prepare('PRAGMA table_info(ai_runs)').all().map(c => c.name);
    if (cols.length > 0 && !cols.includes('runner_info')) {
      const cnt = db.prepare('SELECT COUNT(*) AS c FROM ai_runs').get().c;
      if (cnt === 0) {
        db.exec('DROP TABLE ai_runs');
      } else {
        for (const [name, ddl] of [['runner_info', 'TEXT'], ['finished_at', 'TEXT'],
          ['claimed', 'INTEGER NOT NULL DEFAULT 0'], ['done', 'INTEGER NOT NULL DEFAULT 0'],
          ['failed', 'INTEGER NOT NULL DEFAULT 0'], ['discarded', 'INTEGER NOT NULL DEFAULT 0'],
          ['error', 'TEXT']]) {
          addColumnIfMissing('ai_runs', name, ddl);
        }
      }
    }
  }
  db.exec(`CREATE TABLE IF NOT EXISTS ai_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    runner_info TEXT,                        -- 実行元 (ホスト名+モデル等)
    started_at TEXT,
    finished_at TEXT,
    claimed INTEGER NOT NULL DEFAULT 0,
    done INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    discarded INTEGER NOT NULL DEFAULT 0,    -- rev競合で破棄された結果数
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);

  db.exec('CREATE INDEX IF NOT EXISTS idx_inquiries_status ON inquiries(internal_status, last_message_at)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inquiries_order ON inquiries(order_number)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inquiries_product ON inquiries(product_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inquiries_customer ON inquiries(customer_identifier)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inquiries_folder ON inquiries(folder_id, last_message_at)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_inquiries_label ON inquiries(label_id, last_message_at)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_messages_inquiry ON inquiry_messages(inquiry_id, received_at)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_replies(status, lease_until)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_ai_jobs_status ON ai_jobs(status)');

  // 一括操作のバッチ記録 (2026-08-25 Codex議論の採用: 滞留整理の安全装置)。
  // チェック行への一括・「この条件の全件」・メールルールの既存一括適用を1回=1バッチとして記録し、
  // バッチ単位で取り消せるようにする。items には変更したフィールドの変更前後だけを持つ (batches.js)
  db.exec(`CREATE TABLE IF NOT EXISTS bulk_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT,
    source TEXT NOT NULL CHECK(source IN ('bulk','bulk_filter','rule_apply')),
    ops_json TEXT NOT NULL,          -- 適用した変更内容 (表示用)
    filter_json TEXT,                -- 対象の条件 (bulk_filter/rule_apply。表示用)
    target_count INTEGER NOT NULL,   -- 対象にした件数
    changed_count INTEGER NOT NULL,  -- 実際に変更した件数 (=items件数)
    reverted_at TEXT,                -- 取り消し済みなら日時 (取り消しは1回だけ)
    reverted_by TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS bulk_batch_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES bulk_batches(id),
    inquiry_id INTEGER NOT NULL REFERENCES inquiries(id),
    before_json TEXT NOT NULL,       -- 変更したフィールドの変更前値 (取り消し時に戻す値)
    after_json TEXT NOT NULL         -- 変更後値 (取り消し時に「今もその値のままか」を確認する)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_batch_items_batch ON bulk_batch_items(batch_id)');

  // クイックリンク (2026-08-25 中原さん要望「リンク先はこっちで登録して自動で設定できるように」)。
  // 一覧上部に出す外部サイトへの導線 (楽天R-Messe・Yahoo!ストアクリエイターPro・Gmail 等) を
  // コードに直書きせず画面から登録・編集する。URLの検証は links.js (http/https のみ)
  const linksExisted = !!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='inquiry_quick_links'").get();
  db.exec(`CREATE TABLE IF NOT EXISTS inquiry_quick_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    icon TEXT,                      -- 絵文字1〜2字 (links.js が検証)
    sort_order INTEGER NOT NULL DEFAULT 100,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),  -- 削除は論理削除のみ
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
  )`);
  // 初回作成時だけ、登録済み店舗に合わせた既定リンクを入れておく (すぐ使える状態にする)。
  // 以後は画面での編集が正 — 全部消しても復活しない (2回目以降は何もしない)
  if (!linksExisted) seedDefaultQuickLinks();
}

/** 既定のクイックリンク投入 (テーブル新規作成時のみ)。店舗が登録されているチャネルの分だけ入れる */
function seedDefaultQuickLinks() {
  const shops = db.prepare('SELECT channel_type, account_identifier FROM shops WHERE is_active = 1').all();
  const rows = [];
  if (shops.some(s => s.channel_type === 'rakuten')) {
    rows.push({ name: '楽天 R-Messe', url: 'https://rmesse.rms.rakuten.co.jp/', icon: '🛍️' });
  }
  for (const s of shops.filter(s => s.channel_type === 'yahoo')) {
    const acct = String(s.account_identifier || '').trim();
    rows.push({ name: 'Yahoo! ストアクリエイターPro', icon: '🟡',
      url: acct ? `https://pro.store.yahoo.co.jp/pro.${encodeURIComponent(acct)}` : 'https://pro.store.yahoo.co.jp/' });
  }
  if (shops.some(s => s.channel_type === 'email')) {
    rows.push({ name: 'Gmail', url: 'https://mail.google.com/', icon: '📧' });
  }
  const ins = db.prepare('INSERT INTO inquiry_quick_links (name, url, icon, sort_order, created_by) VALUES (?,?,?,?,?)');
  rows.forEach((r, i) => ins.run(r.name, r.url, r.icon, (i + 1) * 10, 'system:default'));
  if (rows.length) console.log(`[inquiry-hub] クイックリンクの既定 ${rows.length}件を投入しました (🔗リンク管理で編集できます)`);
}

/** 操作ログ記録 (設計書§6 inquiry_activity_logs。本文全量・秘密情報は入れない) */
export function logActivity(inquiryId, { actorType = 'user', userId = null, actionType, operationId = null, before = null, after = null }) {
  getDB().prepare(`INSERT INTO inquiry_activity_logs
    (inquiry_id, actor_type, user_id, action_type, operation_id, before_json, after_json)
    VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run(inquiryId, actorType, userId, actionType, operationId,
      before == null ? null : JSON.stringify(before),
      after == null ? null : JSON.stringify(after));
}
