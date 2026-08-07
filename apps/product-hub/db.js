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

/**
 * draft_shop_categories に枠番 (slot) を導入する冪等マイグレーション (2026-08-02)。
 * RMS の「表示先カテゴリ」は 1〜5 の 5 枠 — 既存行はマスタの並び順で採番し、
 * 旧上限 (30) 時代に 6 件以上選んでいた draft は先頭 5 件だけ残す
 * (残すと保存 API と RMS 同期が全部 400 で詰む。Codex R1 high)。
 * 黙って消さず、何を外したかを draft_events に記録する。
 */
export function migrateShopCategorySlots(db) {
  const cols = new Set(db.prepare('PRAGMA table_info(draft_shop_categories)').all().map((c) => c.name));
  if (cols.has('slot')) return false;
  // 列追加〜採番〜切り詰めを単一トランザクションで (Codex R2 high)。
  // 別々に走らせると、列追加直後のクラッシュで「slot 列はあるが全行 slot=1・6件超も残存」の
  // 中途半端な状態になり、再実行判定 (列の有無) が二度と移行を走らせない。
  // SQLite は DDL もトランザクショナルなので、失敗すれば列追加ごとロールバックされる
  db.transaction(() => {
    db.exec('ALTER TABLE draft_shop_categories ADD COLUMN slot INTEGER NOT NULL DEFAULT 1');
    // 既存の選択はマスタの並び順を枠順とみなす (それ以外に順序の手がかりが無い)
    db.exec(`
      UPDATE draft_shop_categories AS d SET slot = (
        SELECT COUNT(*) FROM draft_shop_categories x
        JOIN ph_shop_categories cx ON cx.id = x.shop_category_id
        JOIN ph_shop_categories cd ON cd.id = d.shop_category_id
        WHERE x.draft_id = d.draft_id
          AND (cx.sort_order < cd.sort_order OR (cx.sort_order = cd.sort_order AND cx.id <= cd.id))
      )
    `);
    const overDrafts = db.prepare(
      'SELECT draft_id, COUNT(*) AS c FROM draft_shop_categories GROUP BY draft_id HAVING c > 5'
    ).all();
    for (const o of overDrafts) {
      const dropped = db.prepare(`
        SELECT c.path FROM draft_shop_categories s
        JOIN ph_shop_categories c ON c.id = s.shop_category_id
        WHERE s.draft_id = ? AND s.slot > 5 ORDER BY s.slot
      `).all(o.draft_id).map((r) => r.path);
      db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ? AND slot > 5').run(o.draft_id);
      db.prepare(`
        INSERT INTO draft_events (draft_id, event, detail, actor)
        VALUES (?, 'shop_categories_trimmed_to_5', ?, 'migration')
      `).run(o.draft_id, `RMSの5枠制限に合わせ ${dropped.length} 件を外しました: ${dropped.join(' ／ ').slice(0, 400)}`);
    }
  })();
  return true;
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
      registered_at  TEXT,                  -- 楽天への登録に成功した日時 (2026-08-05〜 公開直行。それ以前は非公開登録)
      last_error     TEXT,                  -- 直近の RMS エラー (人が直す材料)
      -- 2026-07-27 仕様確定: 「アプリが正、RMS手直しは最終手段」— 公開に必要な情報をアプリで持つ
      shipping_method_group  TEXT,          -- variants[].shipping.shippingMethodGroup (店舗の配送方法ID '1'〜'9')
      postage_included       INTEGER,       -- variants[].shipping.postageIncluded (NULL=未設定 / 0=送料別 / 1=送料込み)
      normal_delivery_date_id TEXT,         -- variants[].normalDeliveryDateId (RMS 納期情報ID = リードタイム)
      white_bg_drive_file_id TEXT,          -- 白抜き背景画像 (whiteBgImage) の Drive fileId
      white_bg_drive_url     TEXT,
      published_at   TEXT,                  -- 公開になった日時 (2026-08-05〜 登録時に同時記録。NULL = 非公開)
      shop_categories_synced_at TEXT,       -- 店舗内カテゴリ (item-mappings) を RMS へ反映した日時
      shop_categories_synced_key TEXT,      -- そのとき反映した categoryId の並び (選択を変えたら「未反映」に戻すため)
      shop_categories_error     TEXT,       -- 直近の反映エラー (人が直す材料)
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

    -- SKU画像 (バリエーションページで SKU 選択時に出る画像。2026-08-07 中原さん指示)。
    -- Drive フォルダに「SKUコード」名で置かれたファイルを取り込み、R-Cabinet 転送後に
    -- 楽天の variants[sku].images へ PATCH で紐づける (PATCH は per-SKU マージ = 実測済)
    CREATE TABLE IF NOT EXISTS draft_sku_images (
      draft_id       INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      sku_code       TEXT NOT NULL,          -- LOWER(TRIM()) した SKU 商品コード
      drive_file_id  TEXT NOT NULL,
      file_name      TEXT,
      cabinet_location TEXT,                 -- 転送後に /dir/file.jpg (未転送は NULL)
      cabinet_file_id  INTEGER,
      uploaded_at    TEXT,
      synced_at      TEXT,                   -- RMS の variants[sku].images へ反映した日時
      PRIMARY KEY (draft_id, sku_code)
    );

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
      -- RMS の「表示先カテゴリ 1〜5」に対応する枠番 (2026-08-02)。
      -- 順序に意味がある: 枠1 = item-mappings の mainPluralCategoryId (メインページ)
      slot             INTEGER NOT NULL DEFAULT 1,
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
  // P2 (2026-08-03): AI生成の取り合い防止 (claim/lease)。
  // 定期実行と手動、あるいは前夜のハングした実行が同じ draft を二重生成しないため (Codex設計相談 Critical)
  if (!draftCols.has('generation_claim_run_id')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_claim_run_id TEXT');
  }
  if (!draftCols.has('generation_claim_until')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_claim_until TEXT');
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
  // ページ表記の自動保存 (#691): ページロードごとのトークン + 単調増加 seq。
  // 自動保存とpagehideビーコンの到着順が逆転しても「古いリクエストが新しい保存を
  // 上書きしない」ためのリビジョン (同一トークン内でのみ seq を比較する)
  const pageInfoCols = new Set(db.prepare('PRAGMA table_info(draft_page_info)').all().map((c) => c.name));
  if (!pageInfoCols.has('save_token')) {
    db.exec('ALTER TABLE draft_page_info ADD COLUMN save_token TEXT');
  }
  if (!pageInfoCols.has('save_seq')) {
    db.exec('ALTER TABLE draft_page_info ADD COLUMN save_seq INTEGER');
  }

  // 店舗内カテゴリの枠番 (2026-08-02、RMS「表示先カテゴリ 1〜5」対応)
  migrateShopCategorySlots(db);

  // 楽天出品仕様 2026-07-27 (配送/納期/白抜き/公開状態)。#629 デプロイ済み DB への冪等 ALTER
  const rkCols = new Set(db.prepare('PRAGMA table_info(draft_rakuten)').all().map((c) => c.name));
  for (const [col, ddl] of [
    ['shipping_method_group', 'TEXT'],
    ['postage_included', 'INTEGER'],
    ['normal_delivery_date_id', 'TEXT'],
    ['white_bg_drive_file_id', 'TEXT'],
    ['white_bg_drive_url', 'TEXT'],
    ['published_at', 'TEXT'],
    // 店舗内カテゴリの RMS 反映 (2026-08-02、item-mappings API)。
    // 商品APIに棚のフィールドが無く登録とは別呼び出しになるため、結果を個別に持つ
    ['shop_categories_synced_at', 'TEXT'],
    ['shop_categories_synced_key', 'TEXT'],
    ['shop_categories_error', 'TEXT'],
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
  // AI が説明文を書くための「材料」が1つでもあれば通す (中原さん 2026-08-02)。
  // 公式ページが無い商品でも、参考URL や Amazon の販売ページがあれば書ける
  const has = (v) => !!(v && String(v).trim());
  if (!has(draft.official_url) && !has(draft.amazon_url)) {
    const refCount = db.prepare('SELECT COUNT(*) AS c FROM draft_reference_urls WHERE draft_id = ?').get(draft.id).c;
    if (refCount === 0) {
      reasons.push('AIが参照できるURLがありません (公式ページURL / 参考URL / Amazon URL のどれか1つ)');
    }
  }
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
 * 返す形: [{ id, ne_code, name, official_url, amazon_url, asin, price, jan_code, drive_folder_url,
 *            reference_urls: [], specs: [{key,value}], yahoo: {...}|null, image_count }]
 */
export function listGenerationQueue(db, { limit = 50, ids = null } = {}) {
  // amazon_url / asin も返す: ゲートは「公式URL / 参考URL / Amazon URL のどれか」なので
  // Amazon URL だけで通過した draft の参照元がキューから欠けないように (Codex R1 high)
  // ids 指定時はその draft を直接引く (claim 応答用。一覧の LIMIT に依存させない — Codex R2 medium)
  const drafts = ids
    ? db.prepare(`
        SELECT id, ne_code, name, official_url, amazon_url, asin, price, jan_code, drive_folder_url, own_brand
        FROM product_drafts WHERE status = 'ready_for_ai' AND id IN (${ids.map(() => '?').join(',') || 'NULL'})
        ORDER BY updated_at ASC
      `).all(...ids)
    : db.prepare(`
        SELECT id, ne_code, name, official_url, amazon_url, asin, price, jan_code, drive_folder_url, own_brand
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
 * draft から ASIN を取り出す (2026-08-04)。asin 列優先、無ければ amazon_url の /dp/ から抽出。
 * AI 生成の材料として Amazon 広告の推奨キーワードを引くために使う。
 */
export function extractAsin(draft) {
  const direct = String(draft?.asin || '').trim().toUpperCase();
  if (/^[A-Z0-9]{10}$/.test(direct)) return direct;
  const m = String(draft?.amazon_url || '').match(/\/dp\/([A-Z0-9]{10})(?:[/?#]|$)/i);
  return m ? m[1].toUpperCase() : null;
}

/**
 * SP広告マニュアルKWのスナップショット (2026-08-04)。
 * Amazon Ads の全件取得は5〜10分かかり claim のタイムアウト (15s) に間に合わないため、
 * 取得成功時に ph_intake_state へ保存し、以後の claim は即時に使う (stale-while-revalidate)。
 * KW はめったに変わらないので TTL は7日。
 */
export const SP_KW_SNAPSHOT_KEY = 'sp_keywords_snapshot';
export const SP_KW_SNAPSHOT_TTL_MS = 7 * 24 * 60 * 60 * 1000;

export function saveSpKeywordSnapshot(db, byAsin) {
  const obj = {};
  for (const [asin, kws] of byAsin) obj[asin] = [...kws];
  db.prepare(`INSERT INTO ph_intake_state (key, value) VALUES (?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
    .run(SP_KW_SNAPSHOT_KEY, JSON.stringify({ fetchedAt: new Date().toISOString(), byAsin: obj }));
}

/** @returns {Map<string, string[]>|null} TTL内のスナップショット (無ければ null) */
export function loadSpKeywordSnapshot(db, { ttlMs = SP_KW_SNAPSHOT_TTL_MS } = {}) {
  const row = db.prepare('SELECT value FROM ph_intake_state WHERE key = ?').get(SP_KW_SNAPSHOT_KEY);
  if (!row?.value) return null;
  try {
    const parsed = JSON.parse(row.value);
    const fetchedAt = Date.parse(parsed?.fetchedAt);
    // NaN は比較が常に false になり無期限に受理されてしまう (Codex R2 low) → 明示的に弾く
    if (!Number.isFinite(fetchedAt) || Date.now() - fetchedAt > ttlMs) return null;
    return new Map(Object.entries(parsed.byAsin || {}));
  } catch (_) { return null; }
}

// ─── P2: AI生成の claim/lease (2026-08-03、Codex設計相談の Critical/High 対応) ───

export const GENERATION_LEASE_MINUTES = 30;

/**
 * 生成待ち draft を run_id で claim して材料付きで返す (取得と排他を1回で)。
 * 対象 = status='ready_for_ai' かつ (未claim or lease切れ)。CAS UPDATE の changes=1 だけ採用。
 * ハングした実行の claim は lease (30分) が切れれば別 run が取り直せる。
 */
export function claimGenerationDrafts(db, runId, { limit = 2 } = {}) {
  const now = new Date().toISOString();
  const until = new Date(Date.now() + GENERATION_LEASE_MINUTES * 60_000).toISOString();
  const candidates = db.prepare(`
    SELECT id FROM product_drafts
    WHERE status = 'ready_for_ai'
      AND (generation_claim_until IS NULL OR generation_claim_until < ?)
    ORDER BY updated_at ASC LIMIT ?
  `).all(now, limit);
  const claimed = [];
  for (const c of candidates) {
    const info = db.prepare(`
      UPDATE product_drafts SET generation_claim_run_id = ?, generation_claim_until = ?
      WHERE id = ? AND status = 'ready_for_ai'
        AND (generation_claim_until IS NULL OR generation_claim_until < ?)
    `).run(runId, until, c.id, now);
    if (info.changes === 1) claimed.push(c.id);
  }
  if (claimed.length === 0) return { claimed: [], leaseUntil: until };
  return { claimed: listGenerationQueue(db, { ids: claimed }), leaseUntil: until };
}

/**
 * 書き込み直前の「書き込み権の原子的な再取得」(Codex R2 high)。
 * 事前チェック (generationClaimError) と UPSERT の間に lease 切れ・再claim が起きても、
 * この条件付き UPDATE (changes=1) を通らない限り一切書き込まない。
 * 成功時は lease を延長する (書き込み中の失効を防ぐ)。
 */
export function acquireGenerationWriteLock(db, draftId, runId) {
  const now = new Date().toISOString();
  const until = new Date(Date.now() + GENERATION_LEASE_MINUTES * 60_000).toISOString();
  return db.prepare(`
    UPDATE product_drafts SET generation_claim_until = ?
    WHERE id = ? AND status = 'ready_for_ai'
      AND generation_claim_run_id = ? AND generation_claim_until >= ?
  `).run(until, draftId, runId, now).changes === 1;
}

/**
 * 書き込み前の claim 検証 (Codex: claimしていない draft には書けないこと)。
 * @returns {null | string} null = OK / それ以外 = 拒否理由
 */
export function generationClaimError(draft, runId) {
  if (!runId) return 'run_id が必要です (先に claim してください)';
  if (draft.status !== 'ready_for_ai') {
    return `status が ready_for_ai ではありません (${draft.status})。人がレビュー中の可能性があるため書き込みません`;
  }
  if (draft.generation_claim_run_id !== runId) return 'この draft は別の実行が claim しています';
  if (!draft.generation_claim_until || draft.generation_claim_until < new Date().toISOString()) {
    return 'claim の有効期限が切れています (claim し直してください)';
  }
  return null;
}

/** claim の解放 (生成を断念した draft を他の実行がすぐ拾えるように)。run_id 一致時のみ */
export function releaseGenerationClaim(db, draftId, runId) {
  return db.prepare(`
    UPDATE product_drafts SET generation_claim_run_id = NULL, generation_claim_until = NULL
    WHERE id = ? AND generation_claim_run_id = ?
  `).run(draftId, runId).changes === 1;
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
 * サムネイルプロキシ (/api/thumb) の取得対象を「product-hub が管理している画像」に限定する。
 * SA は Drive の広い範囲を読めるため、形式チェックだけだと任意の Drive ID を
 * SA 権限で覗ける confused-deputy になる (Codex R1 high)。
 */
export function isKnownImageFileId(db, fileId) {
  if (!fileId) return false;
  return !!db.prepare(`
    SELECT 1 FROM draft_images WHERE drive_file_id = ?
    UNION SELECT 1 FROM draft_rakuten WHERE white_bg_drive_file_id = ?
    UNION SELECT 1 FROM draft_sku_images WHERE drive_file_id = ?
    LIMIT 1
  `).get(fileId, fileId, fileId);
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
