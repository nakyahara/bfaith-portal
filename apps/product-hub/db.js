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
import { fileViewUrl } from './lib/drive-link.js';

export const DRAFT_STATUSES = [
  'draft', 'ready_for_ai', 'review', 'approved', 'listed', 'expanded', 'on_hold', 'excluded',
];

/**
 * ドラフトの出自 (§8 「商品コード単位でどちらを正とするか」)。
 *   portal        … ポータル起点の新規商品。ポータルが正 → Notion カードを作成/同期する
 *   notion_import … Notion 既存カードの取り込み。**Notion が正** → ポータルから書き戻さない
 * 中原さん方針 (2026-07-25): 既存カードは Notion 側で運用、新商品はアプリ、検証用に一部だけ取り込む。
 */
export const DRAFT_SOURCES = ['portal', 'notion_import'];
export const SOURCE_PORTAL = 'portal';
export const SOURCE_NOTION_IMPORT = 'notion_import';

/**
 * Notion へ書き戻してよい行か。
 * **allow-list (fail-closed)**: portal 起点だけを許可する。
 * 「notion_import でなければ許可」の deny-list だと、source が未知の値や誤記
 * ('notion-import' 等) になった瞬間に書き戻しが通ってしまう (Codex R1 high-1)。
 * ALTER で足した列には CHECK を付けられないため、コード側を fail-closed にして担保する。
 */
export function canWriteToNotion(draft) {
  return !!draft && draft.source === SOURCE_PORTAL;
}

/** Notion 取り込み由来か (削除許可など「取り込みだけ」を対象にする判定用) */
export function isNotionImported(draft) {
  return !!draft && draft.source === SOURCE_NOTION_IMPORT;
}

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

/**
 * ne_code の正規化 UNIQUE index を張れたか。
 * 既存データに 'ABC'/'abc' の重複があると張れず、その状態では
 * 「代表コードにまとめる」の衝突検出 (LOWER(TRIM()) 照合) が DB 制約に守られない。
 * 起動は止めず (ポータル全体を巻き添えにしない)、**登録系だけ fail-closed** にする (Codex medium)。
 */
let neCodeUniqueEnforced = false;
export function isNeCodeUniqueEnforced() {
  return neCodeUniqueEnforced;
}

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
      asin                TEXT,
      amazon_url          TEXT,
      own_brand           INTEGER NOT NULL DEFAULT 0 CHECK (own_brand IN (0, 1)),
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

    -- Yahoo 向け追記項目 (要件定義 §12: RYS notion_overrides と同じ意味論。
    -- 将来 RYS の参照先を Notion → ここへアダプタ方式で切替するための受け皿)
    CREATE TABLE IF NOT EXISTS draft_yahoo (
      draft_id            INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      yahoo_price         INTEGER CHECK (yahoo_price IS NULL OR (yahoo_price BETWEEN 0 AND 1000000000000)),
      yahoo_price_sagawa  INTEGER CHECK (yahoo_price_sagawa IS NULL OR (yahoo_price_sagawa BETWEEN 0 AND 1000000000000)),
      delivery_label      TEXT,
      tax_rate            TEXT,
      yahoo_category_id   INTEGER,
      yahoo_path          TEXT,
      updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- 画像制作ワークフロー (要件定義 §13: Notion「商品ページ商品画像登録」の固有項目。自社商品のみ作成)
    CREATE TABLE IF NOT EXISTS draft_image_production (
      draft_id               INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      status                 TEXT,
      importance_tier        TEXT,
      production_type        TEXT,
      aplus_content          TEXT,
      aplus_related          TEXT,
      camera_instruction_url TEXT,
      shipping_status        TEXT,
      reference_collection   TEXT,
      designer               TEXT,
      page_composer          TEXT,
      request_text           TEXT,
      updated_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- バリエーションから外したSKU (2026-07-25 中原さん方針: 既定でまとめ、例外だけ外す)。
    -- NE の代表商品コードは概ね正しいが例外がある (代表コード '10' に無関係な訳アリ品50件など)。
    -- 外した SKU はこのドラフトのSKU一覧から消え、別ページにしたければそのコードで新規登録する
    -- (登録時の自動まとめは、ここに載っているコードではスキップされる)。
    -- 1 SKU は NE の代表商品コードを1つしか持たない = 属するグループ (ドラフト) も1つ。
    -- したがって除外も **SKU 単位でグローバルに一意**にする (Codex high-4)。
    -- draft 単位 UNIQUE だと、A から戻しても B の除外行が残って detached のままになる。
    -- 照合は LOWER(TRIM()) なので UNIQUE も式インデックスで揃える (生の UNIQUE では 'ABC'/' abc ' が別行になる)
    CREATE TABLE IF NOT EXISTS draft_variation_exclusions (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      ne_code    TEXT NOT NULL,
      actor      TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_draft_vari_excl_draft ON draft_variation_exclusions(draft_id);

    -- NE で「見たことがある商品コード」の記録 (2026-07-25: 新商品の自動取込)。
    -- ⚠️ これが無いと「mirror にあって product_drafts に無いコード」= 取扱中3,723件が全部
    --    新商品として流れ込む。初回実行では**全件をここに登録するだけでドラフトは作らず**、
    --    2回目以降に現れた未知コードだけを「今日以降の新商品」として扱う (カットオフ)。
    CREATE TABLE IF NOT EXISTS ph_ne_seen_codes (
      code_key      TEXT PRIMARY KEY,   -- LOWER(TRIM(商品コード))
      ne_code       TEXT NOT NULL,
      draft_id      INTEGER,            -- 取り込んだ先のドラフト (NULL = 初回シード)
      first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- 楽天出品 (P3、2026-07-26 権限smoke実証後に実装)。
    -- ジャンルID・商品属性は中原さん決定で手入力 (attributes_json = RMS 2.0 の
    -- variants[].attributes と同形 [{name, values:[..]}])。
    CREATE TABLE IF NOT EXISTS draft_rakuten (
      draft_id       INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      genre_id       TEXT,
      attributes_json TEXT,                 -- [{name, values:[string]}]
      article_number TEXT,                  -- メーカー型番 (空 = exemptionReason で送る)
      registered_at  TEXT,                  -- 非公開登録に成功した日時
      last_error     TEXT,                  -- 直近の RMS エラー (人が直す材料)
      -- 2026-07-27 仕様確定: 「アプリが正、RMS手直しは最終手段」— 公開に必要な情報をアプリで持つ
      shipping_method_group  TEXT,          -- variants[].shipping.shippingMethodGroup (店舗の配送方法ID '1'〜'9')
      postage_included       INTEGER,       -- variants[].shipping.postageIncluded (NULL=未設定 / 0=送料別 / 1=送料込み)
      normal_delivery_date_id TEXT,         -- variants[].normalDeliveryDateId (RMS 納期情報ID = リードタイム)
      white_bg_drive_file_id TEXT,          -- 白抜き背景画像 (whiteBgImage) の Drive fileId
      white_bg_drive_url     TEXT,
      published_at   TEXT,                  -- アプリから公開に切り替えた日時 (NULL = 非公開のまま)
      updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- R-Cabinet へ転送済みの画像 (draft_images の Drive 画像 → cabinet location)
    CREATE TABLE IF NOT EXISTS draft_cabinet_images (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id       INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      drive_file_id  TEXT NOT NULL,
      cabinet_location TEXT NOT NULL,       -- item images[].location に入れる形 (/dir/file.jpg)
      cabinet_file_id  INTEGER,
      uploaded_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      UNIQUE(draft_id, drive_file_id)
    );
    CREATE INDEX IF NOT EXISTS idx_draft_cabinet_draft ON draft_cabinet_images(draft_id);

    -- 楽天の店舗内カテゴリ (お店の棚) マスタ。RMS 画面からの貼り付けで取り込む
    -- (Category API での自動取得/自動紐付けは miniPC service-api にルート追加が必要 = 未実装)。
    -- 全置き換え取り込みでも行は消さず is_active で外す (draft_shop_categories が参照するため)
    CREATE TABLE IF NOT EXISTS ph_shop_categories (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      category_id  TEXT,                   -- RMS上のカテゴリID (貼り付けに含まれていた場合のみ。将来の自動紐付け用)
      path         TEXT NOT NULL,          -- 例: 犬用品 > おやつ > 無添加 (' > ' 区切りに正規化)
      path_key     TEXT NOT NULL UNIQUE,   -- LOWER(path)
      is_active    INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
      sort_order   INTEGER NOT NULL DEFAULT 0,
      imported_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- ドラフトが載る店舗内カテゴリ (複数選択)。公開時に RMS 画面で設定する指示として使う
    CREATE TABLE IF NOT EXISTS draft_shop_categories (
      draft_id         INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      shop_category_id INTEGER NOT NULL REFERENCES ph_shop_categories(id),
      created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (draft_id, shop_category_id)
    );

    -- 楽天ジャンル属性辞書のキャッシュ (Genre API、2026-07-28 プローブで実証)。
    -- ドラフト間で共有。buildItemPayload の事前検証 (IE1002防止・必須属性チェック) と
    -- 「辞書に カタログID があるジャンルだけ JAN を自動付与」の判定に使う。
    CREATE TABLE IF NOT EXISTS ph_genre_attributes (
      genre_id     TEXT PRIMARY KEY,
      genre_name   TEXT,                    -- 例: 付箋紙
      genre_path   TEXT,                    -- 例: 日用品雑貨… > 文房具… > 付箋紙
      payload_json TEXT NOT NULL,           -- 正規化済み attributes 配列 (JSON)
      fixed_at     TEXT,                    -- RMS 辞書の version.fixedAt
      fetched_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- 商品ページ表記 (化粧品・食品) — 旧「商品ページ詳細ページ作成.xlsm」の移植 (2026-07-29)。
    -- 楽天必須記載事項 (広告文責/メーカーor販売業者名/製造国/商品区分 — テキスト記載必須・画像化不可)
    -- + 食品表示系の項目。HTML は buildPageInfoHtml が xlsm と同じ表形式で生成し説明文に結合する
    CREATE TABLE IF NOT EXISTS draft_page_info (
      draft_id        INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      product_type    TEXT NOT NULL DEFAULT 'general'
                      CHECK (product_type IN ('general','cosmetics','health_food','food')),
      content_volume  TEXT,               -- 内容量 (例: 50ml)
      size_text       TEXT,               -- サイズ (例: 縦5cm×横10cm×高さ15cm)
      ingredients     TEXT,               -- 成分/素材/材質 (化粧品=全成分、雑貨=素材)
      usage_notes     TEXT,               -- 使用上の注意
      origin_type     TEXT CHECK (origin_type IN (NULL, '日本製', '海外製')),
      origin_country  TEXT,               -- 原産国名 (海外製のとき。健康食品は必須)
      category_label  TEXT,               -- 商品分類区分 (化粧品/医薬部外品/健康食品/…)
      seller_name     TEXT,               -- 発売元 (メーカー名 or 販売業者名)
      importer_name   TEXT,               -- 輸入者名 (輸入品はメーカー名と両記載が楽天必須)
      food_name       TEXT,               -- 名称 (食品表示)
      food_ingredients TEXT,              -- 原材料名 (食品表示)
      food_expiry     TEXT,               -- 賞味期限 (例: 商品ラベルに記載)
      food_storage    TEXT,               -- 保存方法
      updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- NE の配送方法 ↔ 楽天の発送方法コード (配送方法グループ 1〜9) の紐付け (2026-07-29 中原さん指示)。
    -- 出品カードの配送方法デフォルトと、商品ページ表記の「発送方法」行に使う
    CREATE TABLE IF NOT EXISTS ph_shipping_method_map (
      ne_label        TEXT PRIMARY KEY,   -- NE の配送方法 (例: ネコポス)
      rakuten_group   TEXT,               -- 楽天 配送方法グループID '1'〜'9' (NULL=未割当)
      updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- 自動取込の状態 (シード完了の判定は seen 件数でなくここで行う — Codex critical:
    -- 一括登録も ph_ne_seen_codes に書くため、件数>0 を「シード済み」とすると
    -- シード前に手動登録1件しただけで既存3,723件が全部「新商品」扱いになる)
    CREATE TABLE IF NOT EXISTS ph_intake_state (
      key   TEXT PRIMARY KEY,
      value TEXT
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
  // P1.5: 自社商品フラグ + Amazon 識別子 (要件定義 §13)
  if (!draftCols.has('asin')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN asin TEXT');
  }
  if (!draftCols.has('amazon_url')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN amazon_url TEXT');
  }
  if (!draftCols.has('own_brand')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN own_brand INTEGER NOT NULL DEFAULT 0 CHECK (own_brand IN (0, 1))');
  }
  // Notion 取り込み (テスト検証用)。source='notion_import' の行は Notion 側が正であり、
  // ポータルから Notion へ書き戻してはならない (既存カードの破壊防止 — notion-card.js のガード参照)。
  //   注意: ALTER で足す列に CHECK は付けられない。そのため書き戻し判定は canWriteToNotion の
  //   allow-list (source==='portal' のみ許可) で fail-closed にしてある。
  if (!draftCols.has('source')) {
    db.exec(`ALTER TABLE product_drafts ADD COLUMN source TEXT NOT NULL DEFAULT 'portal'`);
  }
  if (!draftCols.has('source_notion_status')) {
    // 取り込み時点の Notion Status (⓪新規商品_高島 等)。product_drafts.status とは別軸なので原文保持
    db.exec('ALTER TABLE product_drafts ADD COLUMN source_notion_status TEXT');
  }
  if (!draftCols.has('imported_at')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN imported_at TEXT');
  }

  // 楽天出品仕様 2026-07-27 (配送/納期/白抜き/公開状態)。#629 デプロイ済み DB への冪等 ALTER
  const rkCols = new Set(db.prepare('PRAGMA table_info(draft_rakuten)').all().map((c) => c.name));
  for (const [col, ddl] of [
    ['shipping_method_group', 'TEXT'],
    ['postage_included', 'INTEGER'],
    ['normal_delivery_date_id', 'TEXT'],
    ['white_bg_drive_file_id', 'TEXT'],
    ['white_bg_drive_url', 'TEXT'],
    ['published_at', 'TEXT'],
  ]) {
    if (!rkCols.has(col)) db.exec(`ALTER TABLE draft_rakuten ADD COLUMN ${col} ${ddl}`);
  }

  // 除外の一意性を「SKU単位グローバル」へ移行する (Codex R2 high)。
  //   CREATE ... IF NOT EXISTS は、旧定義 (draft 単位 UNIQUE) が残っていても何もしない。
  //   旧形のまま動くと「A で外して B から戻せない」不整合が残るので、定義を実測して張り替える。
  try {
    const tableSql = db.prepare(
      `SELECT sql FROM sqlite_master WHERE type='table' AND name='draft_variation_exclusions'`
    ).get()?.sql || '';
    const idxSql = db.prepare(
      `SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_draft_vari_excl_code'`
    ).get()?.sql || null;

    // 旧テーブル制約 UNIQUE(draft_id, ne_code) が残っているか / 期待する式インデックスが無いか
    const hasOldTableUnique = /UNIQUE\s*\(\s*draft_id\s*,\s*ne_code\s*\)/i.test(tableSql);
    const hasWantedIndex = !!idxSql && /LOWER\s*\(\s*TRIM\s*\(\s*ne_code/i.test(idxSql) && /UNIQUE/i.test(idxSql);

    if (hasOldTableUnique || !hasWantedIndex) {
      // テーブル入れ替えは SQLite の手順どおり foreign_keys を一時 OFF にする
      // (ON のままだと DROP/RENAME と INSERT..SELECT が FK 違反で落ちる)。
      // PRAGMA はトランザクション外で切り替える必要がある
      const fkWasOn = db.pragma('foreign_keys', { simple: true }) === 1;
      if (fkWasOn) db.pragma('foreign_keys = OFF');
      try {
        db.transaction(() => {
          // ① 親ドラフトを失った孤児行を先に落とす。
          //    集約を先にやると「最古が孤児・後発が有効」のとき有効な除外まで消える (Codex R3 high)
          db.exec(`
            DELETE FROM draft_variation_exclusions
            WHERE NOT EXISTS (SELECT 1 FROM product_drafts d WHERE d.id = draft_variation_exclusions.draft_id)
          `);
          // ② 残った有効行のうち、正規化後に重複するものは最古の1件だけ残す
          db.exec(`
            DELETE FROM draft_variation_exclusions WHERE id NOT IN (
              SELECT MIN(id) FROM draft_variation_exclusions GROUP BY LOWER(TRIM(ne_code))
            )
          `);
          if (hasOldTableUnique) {
            // テーブル制約は ALTER で外せないので作り直す (swap)。
            // 親を失った孤児行はここで落とす (FK を復帰させるため)
            db.exec(`
              CREATE TABLE draft_variation_exclusions_new (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
                ne_code    TEXT NOT NULL,
                actor      TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
              );
              INSERT INTO draft_variation_exclusions_new (id, draft_id, ne_code, actor, created_at)
                SELECT e.id, e.draft_id, e.ne_code, e.actor, e.created_at
                FROM draft_variation_exclusions e
                WHERE EXISTS (SELECT 1 FROM product_drafts d WHERE d.id = e.draft_id);
              DROP TABLE draft_variation_exclusions;
              ALTER TABLE draft_variation_exclusions_new RENAME TO draft_variation_exclusions;
              CREATE INDEX IF NOT EXISTS idx_draft_vari_excl_draft ON draft_variation_exclusions(draft_id);
            `);
          } else if (idxSql) {
            db.exec('DROP INDEX idx_draft_vari_excl_code');
          }
          db.exec(`CREATE UNIQUE INDEX idx_draft_vari_excl_code
                   ON draft_variation_exclusions(LOWER(TRIM(ne_code)))`);
        })();
        const violations = db.pragma('foreign_key_check(draft_variation_exclusions)');
        if (violations.length > 0) {
          console.warn(`[product-hub] 除外テーブル移行後に FK 違反 ${violations.length} 件`);
        }
      } finally {
        if (fkWasOn) db.pragma('foreign_keys = ON');
      }
    }
  } catch (e) {
    // 起動をポータルごと落とさない。張れていない場合は exclude API の INSERT が
    // 冪等でなくなるだけで、表示・既存機能は動く
    console.warn('[product-hub] 除外テーブルの一意制約移行に失敗:', e.message);
  }

  // バリエーション判定は LOWER(TRIM()) 照合なので、通常の索引が効かない (Codex medium-6)。
  // 式インデックスを張って全走査を避ける。mirror 側の所有テーブルなので失敗しても無視する
  for (const stmt of [
    'CREATE INDEX IF NOT EXISTS idx_mirp_sku_norm ON mirror_products(LOWER(TRIM(商品コード)))',
    'CREATE INDEX IF NOT EXISTS idx_mirp_rep_norm ON mirror_products(LOWER(TRIM(代表商品コード)))',
  ]) {
    try { db.exec(stmt); } catch (e) { console.warn('[product-hub] index skip:', e.message); }
  }

  // ne_code の一意性: DB の UNIQUE は BINARY 比較だが、NE/Notion 突合は LOWER(TRIM()) で行う。
  // 'ABC' と 'abc' が別ドラフトとして共存すると突合が壊れるので正規化 UNIQUE も張る (Codex medium-4)。
  // ⚠️ 既存データが衝突していると CREATE が失敗する → 起動を巻き添えにしないよう事前検査 + try/catch
  try {
    const dup = db.prepare(`
      SELECT COUNT(*) AS c FROM (
        SELECT LOWER(TRIM(ne_code)) k FROM product_drafts GROUP BY LOWER(TRIM(ne_code)) HAVING COUNT(*) > 1
      )
    `).get().c;
    if (dup === 0) {
      db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_product_drafts_ne_norm ON product_drafts(LOWER(TRIM(ne_code)))');
      neCodeUniqueEnforced = true;
    } else {
      console.warn(`[product-hub] ne_code の正規化重複 ${dup} 件のため UNIQUE index を張れません (要データ修正)`);
    }
  } catch (e) {
    console.warn('[product-hub] ne_code 正規化 UNIQUE index skip:', e.message);
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

/** smoke 専用: 初期化フラグを戻して migration を再実行させる (本番からは呼ばない) */
export function _resetInitForTest() {
  initialized = false;
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

/** draft_yahoo の upsert (部分更新: undefined のキーは既存値を維持) */
export function upsertDraftYahoo(db, draftId, fields) {
  const existing = db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(draftId) || {};
  const merged = {
    yahoo_price: fields.yahoo_price !== undefined ? fields.yahoo_price : (existing.yahoo_price ?? null),
    yahoo_price_sagawa: fields.yahoo_price_sagawa !== undefined ? fields.yahoo_price_sagawa : (existing.yahoo_price_sagawa ?? null),
    delivery_label: fields.delivery_label !== undefined ? fields.delivery_label : (existing.delivery_label ?? null),
    tax_rate: fields.tax_rate !== undefined ? fields.tax_rate : (existing.tax_rate ?? null),
    yahoo_category_id: fields.yahoo_category_id !== undefined ? fields.yahoo_category_id : (existing.yahoo_category_id ?? null),
    yahoo_path: fields.yahoo_path !== undefined ? fields.yahoo_path : (existing.yahoo_path ?? null),
  };
  db.prepare(`
    INSERT INTO draft_yahoo (draft_id, yahoo_price, yahoo_price_sagawa, delivery_label, tax_rate, yahoo_category_id, yahoo_path)
    VALUES (@draft_id, @yahoo_price, @yahoo_price_sagawa, @delivery_label, @tax_rate, @yahoo_category_id, @yahoo_path)
    ON CONFLICT(draft_id) DO UPDATE SET
      yahoo_price = excluded.yahoo_price,
      yahoo_price_sagawa = excluded.yahoo_price_sagawa,
      delivery_label = excluded.delivery_label,
      tax_rate = excluded.tax_rate,
      yahoo_category_id = excluded.yahoo_category_id,
      yahoo_path = excluded.yahoo_path,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `).run({ draft_id: draftId, ...merged });
}

const IMAGE_PRODUCTION_FIELDS = [
  'status', 'importance_tier', 'production_type', 'aplus_content', 'aplus_related',
  'camera_instruction_url', 'shipping_status', 'reference_collection',
  'designer', 'page_composer', 'request_text',
];

/** draft_image_production の upsert (部分更新)。自社商品のみ呼ぶ想定 (router 側でガード) */
export function upsertImageProduction(db, draftId, fields) {
  const existing = db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(draftId) || {};
  const merged = {};
  for (const f of IMAGE_PRODUCTION_FIELDS) {
    merged[f] = fields[f] !== undefined ? fields[f] : (existing[f] ?? null);
  }
  db.prepare(`
    INSERT INTO draft_image_production (draft_id, ${IMAGE_PRODUCTION_FIELDS.join(', ')})
    VALUES (@draft_id, ${IMAGE_PRODUCTION_FIELDS.map((f) => `@${f}`).join(', ')})
    ON CONFLICT(draft_id) DO UPDATE SET
      ${IMAGE_PRODUCTION_FIELDS.map((f) => `${f} = excluded.${f}`).join(', ')},
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `).run({ draft_id: draftId, ...merged });
}

/**
 * 生成待ち (ready_for_ai) の一覧を、AI 生成に必要な材料つきで返す (P2 スキル接続用)。
 * 返す形: [{ id, ne_code, name, official_url, price, jan_code, drive_folder_url,
 *            reference_urls: [], specs: [{key,value}], yahoo: {...}|null, image_count }]
 */
export function listGenerationQueue(db, { limit = 50 } = {}) {
  const drafts = db.prepare(`
    SELECT id, ne_code, name, official_url, price, jan_code, drive_folder_url, own_brand
    FROM product_drafts WHERE status = 'ready_for_ai' ORDER BY updated_at ASC LIMIT ?
  `).all(limit);
  const refStmt = db.prepare('SELECT url FROM draft_reference_urls WHERE draft_id = ? ORDER BY sort, id');
  const specStmt = db.prepare('SELECT spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id');
  const yahooStmt = db.prepare('SELECT yahoo_price, yahoo_price_sagawa, delivery_label, tax_rate, yahoo_category_id, yahoo_path FROM draft_yahoo WHERE draft_id = ?');
  const imgStmt = db.prepare('SELECT COUNT(*) AS c FROM draft_images WHERE draft_id = ?');
  return drafts.map((d) => ({
    ...d,
    reference_urls: refStmt.all(d.id).map((r) => r.url),
    specs: specStmt.all(d.id).map((s) => ({ key: s.spec_key, value: s.spec_value })),
    yahoo: yahooStmt.get(d.id) || null,
    image_count: imgStmt.get(d.id).c,
  }));
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

/**
 * 画像フォルダ一括取り込みの結果 (assignImageSlots の戻り値) を DB に反映する。
 *   - slots があるときだけ draft_images を全置き換え (sort = スロット番号 - 1)。
 *     白抜きだけ見つかった場合は既存の商品画像を触らない
 *   - whiteBg があれば draft_rakuten の白抜き背景を upsert (他カラムは触らない)
 *   - フォルダURLが基本情報と違えば product_drafts.drive_folder_url も更新
 */
export function applyFolderImport(db, draftId, assigned, { folderUrl = null, currentFolderUrl = null } = {}) {
  db.transaction(() => {
    if (assigned.slots.length > 0) {
      db.prepare('DELETE FROM draft_images WHERE draft_id = ?').run(draftId);
      const ins = db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, drive_url, sort) VALUES (?, ?, ?, ?)');
      for (const s of assigned.slots) ins.run(draftId, s.id, fileViewUrl(s.id), s.slot - 1);
    }
    if (assigned.whiteBg) {
      db.prepare(`
        INSERT INTO draft_rakuten (draft_id, white_bg_drive_file_id, white_bg_drive_url) VALUES (?, ?, ?)
        ON CONFLICT(draft_id) DO UPDATE SET
          white_bg_drive_file_id = excluded.white_bg_drive_file_id,
          white_bg_drive_url = excluded.white_bg_drive_url,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      `).run(draftId, assigned.whiteBg.id, fileViewUrl(assigned.whiteBg.id));
    }
    if (folderUrl && folderUrl !== currentFolderUrl) {
      db.prepare(`UPDATE product_drafts SET drive_folder_url = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?`)
        .run(folderUrl, draftId);
    }
  })();
}
