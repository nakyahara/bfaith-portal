/**
 * material-tables.js — mirror_products / mirror_set_components の定義 (1 か所に持つ)
 *
 * 使う所: Render の mirror (warehouse-mirror/db.js の createTables) と、毎朝の照合の ①ロードの検証
 *   (apps/company-db/master-compare = 夜間ロードが読んだ材料の控えを一時の SQLite に戻し、同じ型の表から buildPlanFromRender を通す。
 *   型 (TEXT / REAL / INTEGER) が違うと、同じ控えでも読んだ値が変わる。Codex ③a-2 B-R0 Medium)
 * 🚨 列を足すときは warehouse-mirror/db.js の addColumnIfMissing も (古い DB に足す)。material-lineage.js の MATERIAL_COLUMNS も合わせる
 */
export const MIRROR_PRODUCTS_DDL = `CREATE TABLE IF NOT EXISTS mirror_products (
    product_id                INTEGER PRIMARY KEY,
    商品コード                TEXT UNIQUE NOT NULL,
    商品名                    TEXT,
    商品区分                  TEXT NOT NULL,
    取扱区分                  TEXT,
    標準売価                  REAL,
    原価                      REAL,
    原価ソース                TEXT,
    原価状態                  TEXT NOT NULL,
    送料                      REAL,
    送料コード                TEXT,
    配送方法                  TEXT,
    消費税率                  REAL,
    税区分                    TEXT,
    在庫数                    INTEGER,
    引当数                    INTEGER,
    仕入先コード              TEXT,
    セット構成品数            INTEGER,
    売上分類                  INTEGER,
    代表商品コード            TEXT,
    seasonality_flag          INTEGER DEFAULT 0,
    season_months             TEXT,
    new_product_flag          INTEGER DEFAULT 0,
    new_product_launch_date   TEXT,
    updated_at                TEXT NOT NULL
  )`;

export const MIRROR_SET_COMPONENTS_DDL = `CREATE TABLE IF NOT EXISTS mirror_set_components (
    セット商品コード  TEXT NOT NULL,
    構成商品コード    TEXT NOT NULL,
    数量              INTEGER NOT NULL DEFAULT 1,
    構成商品名        TEXT,
    構成商品原価      REAL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (セット商品コード, 構成商品コード)
  )`;
