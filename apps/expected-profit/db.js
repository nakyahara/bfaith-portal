/**
 * 商品別 想定利益 (単品販売シナリオ) — DB 層
 *
 * 正本 = AI_reference『システム設計/商品別想定利益_要件定義_20260907.md』(v1.4)
 *
 * 目的: 販売実績を使わず、登録済みマスタと当夜取得したモール登録価格だけで
 *   「この出品を、定義された標準シナリオで1個売ったときにいくら残るか」を全出品ぶん計算する。
 *
 * 設計 (Codex R1〜R3 + PR-0 調査で確定):
 *   - **専用DB expected-profit.db を DATA_DIR に持つ** (§15-10)。warehouse.db は読み取り専用で参照するだけ。
 *     warehouse.db は 11GB あり、product-idea-scout が 14:00〜翌09:00 常駐で読み書きしているため同居させない
 *   - 出品の粒度は **複合キー (mall, shop_id, mall_item_key)** (§15-3)。listing_id 単独では複数店舗で衝突する
 *   - 欠損は 0 で埋めない。「取得済みの有効な 0」と「未登録」を区別する (§4.5)
 *   - 公開は **世代 (generation) 単位**。検証を通った世代だけをポインタで切り替える (§7.5)
 *   - 価格スナップショットは **表示専用**。値付けの基準に使わせない (§13)
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'expected-profit.db');

let db = null;

export function initExpectedProfitDB() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (db) { try { db.close(); } catch { /* close 済み等は無視 */ } db = null; }
  db = new Database(DB_FILE);
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  createTables(db);
  return db;
}

export function getExpectedProfitDB() {
  if (!db) return initExpectedProfitDB();
  return db;
}

/**
 * 任意のハンドルにこのアプリのスキーマを作る (受信側 = Render も同じ形にする)。
 *
 * 🚨 テストが db.js を正規表現で読んで CREATE 文を拾う作りだと、
 *    ALTER 文が増えただけで壊れるし、migrate を通らない。本番と同じ関数を通す
 */
export function createExpectedProfitSchema(db) {
  createTables(db);
  return db;
}

function createTables(db) {
  // ─── 1. 取得の実行管理 (§7.1) ───
  // 「未実行 / API失敗 / 出品終了 / 対象0件」を区別するための台帳。
  // 正常に全件列挙した上での 0 件と、列挙そのものの失敗を混同しない
  db.exec(`CREATE TABLE IF NOT EXISTS price_fetch_run (
    run_id              TEXT PRIMARY KEY,
    mall                TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    status              TEXT NOT NULL,          -- running / ok / partial / failed
    listing_enum_status TEXT NOT NULL,          -- ok / partial / failed  (出品列挙が完走したか)
    expected_count      INTEGER,                -- 列挙で得た期待件数
    fetched_count       INTEGER,                -- 価格まで取れた件数
    failed_count        INTEGER,
    disappeared_count   INTEGER,                -- 前回の完全集合から消えた出品数 (§15-4)
    resume_cursor       TEXT,                   -- 未完了の対象集合 (§8.4 再開用)
    error_summary       TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_epr_run_mall ON price_fetch_run(mall, started_at DESC)');

  // ─── 2. モール価格のスナップショット (§7.2) ───
  // 🚨 表示専用。価格一括改定ツールはライブ再取得を使うこと (§13)
  db.exec(`CREATE TABLE IF NOT EXISTS mall_price_snapshot (
    run_id                   TEXT NOT NULL,
    mall                     TEXT NOT NULL,
    shop_id                  TEXT NOT NULL,     -- Amazon = seller@marketplace、楽天 = shop_code
    mall_item_key            TEXT NOT NULL,     -- Amazon = seller_sku、楽天 = manageNumber/variantKey
    -- モール側の商品参照。Amazon = ASIN (手数料見積の入力キーに必須)、楽天 = merchantDefinedSkuId
    -- 🚨 ここに残さないと build 時に再取得が要る (見積は ASIN 単位で引くため)
    mall_item_ref            TEXT,
    fulfillment              TEXT,              -- FBA / FBM / self
    ne_code                  TEXT,
    price_type               TEXT,              -- normal (セール価格は採用しない §3.2)
    price_incl_tax           INTEGER,           -- 税込。整数円のみ。読めなければ NULL
    -- 🚨 モール側が税込で持っているとは限らない。税区分が確認できない価格は price_incl_tax に入れない
    price_tax_included       INTEGER,           -- 1 = 税込 / 0 = 税抜 / NULL = 不明
    price_raw                INTEGER,           -- モールが返した元の価格 (調査用)
    mall_tax_rate            REAL,              -- モール側が持つ税率 (楽天 payment.taxRate)。無ければ NULL
    postage_included         INTEGER,           -- 1 / 0 / NULL
    postage_revenue_incl_tax INTEGER,           -- 別途送料収入 (税込)。不明は NULL
    points                   INTEGER,           -- 出品のポイント (手数料見積の入力 §15-1)
    listing_status           TEXT,              -- active / inactive / incomplete / hidden
    fetch_status             TEXT NOT NULL,     -- ok / api_error / not_found   ← API 取得の結果
    resolve_status           TEXT NOT NULL,     -- ok / ambiguous / unresolved  ← NE 商品への対応付け
    resolve_reason           TEXT,
    valid_until              TEXT NOT NULL,     -- 失効時刻 (§15-8。コピーしても延ばさない)
    source                   TEXT NOT NULL,     -- merchant_listings_report / rms_items_search / ...
    shipping_group           TEXT,              -- Amazon の配送パターン (送料込みの判断根拠)
    fetched_at               TEXT NOT NULL,
    PRIMARY KEY (run_id, mall, shop_id, mall_item_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_eps_key ON mall_price_snapshot(mall, shop_id, mall_item_key)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_eps_ne ON mall_price_snapshot(ne_code)');

  // ─── 3. Amazon 手数料見積のキャッシュ (§7.3) ───
  // 🚨 再利用は「見積入力の完全一致 + 期限内 + 改定日を跨がない」ときだけ。
  //    PK だけで照合しない (asin / 通貨も比較する — Codex R3)
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_fee_estimate (
    seller_id            TEXT NOT NULL,
    marketplace_id       TEXT NOT NULL,
    seller_sku           TEXT NOT NULL,
    asin                 TEXT NOT NULL,
    -- 見積入力 (この全てが一致しなければ再利用しない)
    in_listing_price     INTEGER NOT NULL,
    in_shipping          INTEGER NOT NULL,
    in_points            INTEGER NOT NULL,
    in_fulfillment       TEXT NOT NULL,          -- FBA / FBM
    in_currency          TEXT NOT NULL,
    -- 見積結果 (§15-1 の対応表)
    referral_fee_ex_tax  REAL,                   -- ReferralFee.FinalFee (税抜)
    closing_fee_ex_tax   REAL,                   -- VariableClosingFee.FinalFee
    per_item_fee_ex_tax  REAL,                   -- PerItemFee.FinalFee
    fba_fee_incl_tax     REAL,                   -- FBAFees.FinalFee (税込)。FBM は NULL = not_applicable
    total_fees_estimate  REAL,                   -- 照合用
    fee_breakdown        TEXT NOT NULL,          -- 生の FeeDetailList (JSON。親子構造つき)
    fee_status           TEXT NOT NULL,          -- ok / unknown_fee_type / inconsistent
    fetched_at           TEXT NOT NULL,
    valid_until          TEXT NOT NULL,
    PRIMARY KEY (seller_id, marketplace_id, seller_sku, in_listing_price, in_shipping, in_points, in_fulfillment)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_efe_sku ON amazon_fee_estimate(seller_sku)');

  // ─── 4. 公開世代 (§7.5) ───
  // ローカルの生成状態と Render の配信状態を分ける。seq で逆転公開を防ぐ
  db.exec(`CREATE TABLE IF NOT EXISTS expected_profit_generation (
    generation_id    TEXT PRIMARY KEY,
    seq              INTEGER NOT NULL UNIQUE,   -- 単調増加
    built_at         TEXT NOT NULL,
    local_status     TEXT NOT NULL,             -- building / validated / rejected
    remote_status    TEXT NOT NULL,             -- not_sent / sending / received / published / superseded
    row_count        INTEGER,
    ok_count         INTEGER,
    incomplete_count INTEGER,
    rank_eligible_count INTEGER,
    content_hash     TEXT,                      -- manifest 用
    malls_included   TEXT,                      -- JSON配列
    malls_degraded   TEXT,                      -- JSON配列 (部分取得で劣化したモール)
    validation_note  TEXT
  )`);

  // ─── 5. 計算結果 (§7.4) ───
  // 金額はすべて税抜・小数のまま保持する (丸めは表示時。§7.4)
  db.exec(`CREATE TABLE IF NOT EXISTS mart_listing_expected_profit (
    generation_id             TEXT NOT NULL,
    mall                      TEXT NOT NULL,
    shop_id                   TEXT NOT NULL,
    mall_item_key             TEXT NOT NULL,
    ne_code                   TEXT,
    ne_code_source            TEXT,              -- sku_map / fbm_ne_code (§16-12)
    product_name              TEXT,
    sales_class               INTEGER,
    -- 🚨 ここから 3 列は**表示専用** (2026-09-09 中原さん指示)。想定利益の計算には一切使わない。
    --    「赤字だが在庫が無い / もう取扱終了」を画面で見分けて、直す順番を決めるために持つ。
    --    値は**夜間バッチが動いた時点**のもの (在庫は日中に動くので、画面に as-of を書くこと)
    handling_class            TEXT,              -- m_products.取扱区分 (取扱中 / 取扱終了 など)
    stock_qty                 INTEGER,           -- m_products.在庫数 (NE の自社倉庫。FBA 在庫は含まない)
    stock_allocated_qty       INTEGER,           -- m_products.引当数 (フリー在庫 = 在庫数 − 引当数)
    fulfillment               TEXT,
    listing_status            TEXT,
    -- 売上側 (税抜)
    price_incl_tax            INTEGER,           -- 画面に併記する (中原さん決定 §4.2.1)
    price_ex_tax              REAL,
    postage_revenue_ex_tax    REAL,
    revenue_ex_tax            REAL,
    tax_rate                  REAL,              -- 採用した商品消費税率 (小数)
    -- 費用側 (税抜)
    cost_ex_tax               REAL,
    cost_method               TEXT,              -- single / set_master (§15-5)
    unit_quantity             INTEGER,           -- 1出品が単品何個ぶんか (v_sku_resolved.数量)
    shipping_code             TEXT,
    shipping_method           TEXT,              -- NE 商品マスタの「配送方法」
    -- 🚨 実際にこの計算で引いた送料の出どころ (2026-09-08 中原さん指示)。
    --    送料コードだけでは「501 が何なのか」が画面で分からず、数字を信用できない
    shipping_rate_name        TEXT,              -- shipping_rates.小分類区分名称 (例 ネコポス)
    shipping_rate_category    TEXT,              -- shipping_rates.大分類区分 (例 メール便)
    shipping_group            TEXT,              -- モール側の配送パターン (Amazon FBM の送料込み判断根拠 §16-13)
    shipping_fee_ex_tax       REAL,              -- 送料 ÷ 1.1
    shipping_work_ex_tax      REAL,              -- 出荷作業料 (社内見積・税の概念なし)
    shipping_material_ex_tax  REAL,
    shipping_labor_ex_tax     REAL,
    shipping_total_ex_tax     REAL,              -- 配送関係費合計 (控除する額)
    fba_fee_ex_tax            REAL,              -- 配送費側で 1 回だけ引く
    referral_fee_ex_tax       REAL,
    closing_fee_ex_tax        REAL,
    per_item_fee_ex_tax       REAL,
    fee_total_ex_tax          REAL,              -- 🚨 FBA費用を含めない (§4.4.1 の控除境界)
    fee_rate_display          REAL,              -- 表示専用の実効率。計算に使わない
    fee_breakdown             TEXT,              -- JSON (表示用)
    -- 結果
    expected_profit           REAL,
    expected_margin_rate      REAL,
    -- 入力ごとの状態 (§7.4。1列にまとめない)
    listing_enum_status       TEXT NOT NULL,
    listing_enum_valid_until  TEXT,
    price_status              TEXT NOT NULL,     -- ok / expired / missing / tax_mismatch
    price_valid_until         TEXT,
    fee_status                TEXT NOT NULL,     -- ok / expired / missing / not_applicable / unknown_fee_type / inconsistent
    fee_valid_until           TEXT,
    cost_status               TEXT NOT NULL,
    cost_valid_until          TEXT,
    shipping_master_status    TEXT NOT NULL,     -- FBA では not_applicable
    shipping_master_valid_until TEXT,
    shipping_revenue_status   TEXT,              -- ok / unknown (§15-2 FBM の標準送料)
    scenario_fit              TEXT NOT NULL,     -- ok / undecidable
    calculation_status        TEXT NOT NULL,     -- ok / incomplete / not_applicable
    incomplete_reason         TEXT,
    rank_eligible             INTEGER NOT NULL,  -- 既定ランキングに載せてよいか (§15-9)
    rank_exclusion_reason     TEXT,
    expense_scope_version     TEXT NOT NULL,     -- self_v1 / fba_v1 (§4.8)
    -- 再現性 (§7.4)
    input_snapshot            TEXT NOT NULL,     -- 採用した元金額・税率・見積条件・原価根拠・配送マスタ版 (JSON)
    formula_version           TEXT NOT NULL,
    scenario_version          TEXT NOT NULL,
    fee_rate_version          TEXT NOT NULL,
    code_version              TEXT NOT NULL,
    price_run_id              TEXT,
    built_at                  TEXT NOT NULL,
    PRIMARY KEY (generation_id, mall, shop_id, mall_item_key)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlep_rank ON mart_listing_expected_profit(generation_id, rank_eligible, expected_margin_rate DESC)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_mlep_ne ON mart_listing_expected_profit(generation_id, ne_code)');
  // 🚨 スキーマに列を足したら MART_ROW_COLUMNS にも足す (足し忘れると保存されない)。
  //    列名の集合が食い違っていないことは test-publish.mjs が固定している

  // ─── 6. 受信したチャンクの記録 (§15-7) ───
  // 🚨 同じ index を別内容で上書きさせないために持つ。
  //    これが無いと「複数回の送信が混ざった世代」を作れてしまう
  db.exec(`CREATE TABLE IF NOT EXISTS expected_profit_chunk (
    generation_id  TEXT NOT NULL,
    chunk_index    INTEGER NOT NULL,
    checksum       TEXT NOT NULL,
    row_count      INTEGER NOT NULL,
    received_at    TEXT NOT NULL,
    PRIMARY KEY (generation_id, chunk_index)
  )`);

  // ─── 7. 公開ポインタ (§15-7) ───
  // Render 側にも同じ形で持つ。単一行 (id = 1)
  db.exec(`CREATE TABLE IF NOT EXISTS expected_profit_publish_pointer (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    generation_id  TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    published_at   TEXT NOT NULL
  )`);

  // ─── 8. アプリの覚え書き (§15-3) ───
  // 🚨 「今どのセラーで見積もっているか」を明示して持つ。
  //    amazon_fee_estimate の DISTINCT から推測すると、旧セラーの行が1つ残っただけで
  //    「決められない」に落ち、env が空の環境ではキーを作れなくなる (Codex R7-3)
  db.exec(`CREATE TABLE IF NOT EXISTS expected_profit_setting (
    key         TEXT PRIMARY KEY,
    value       TEXT,
    updated_at  TEXT NOT NULL
  )`);

  // ─── 9. 見積の失敗記録 (§16-9) ───
  // 🚨 毎晩同じ対象で失敗し続けるのを止める。SP-API は 0.5 req/s しかないので、
  //    確実に失敗するものを毎晩叩き続けると、取れるはずの分の枠を奪う。
  //    入力が変われば別キーになるので、値が直れば自動でやり直す
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_fee_failure (
    fee_key      TEXT PRIMARY KEY,     -- feeCacheKey() と同じキー
    seller_sku   TEXT,
    attempts     INTEGER NOT NULL,
    last_error   TEXT,
    failed_at    TEXT NOT NULL,
    retry_after  TEXT NOT NULL
  )`);

  // ─── 10. 取得待ちに入った時刻 (§16-10) ───
  // 🚨 見積が1件も無い対象どうしでは「どれが長く待っているか」が分からない。
  //    1晩の上限で溢れたときに、毎晩あとから来た新規に押しのけられ続ける
  //    (Codex R10)。最初に待ちに入った時刻を残して、古い順に取る
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_fee_queue (
    fee_key      TEXT PRIMARY KEY,     -- feeCacheKey() と同じキー
    seller_sku   TEXT,
    queued_at    TEXT NOT NULL         -- 最初に「取得が要る」と判定された時刻
  )`);

  // ─── 11. 承知のうえの赤字 (許容記録) ───
  // 🚨 「意図した赤字」と「気づいていない赤字」を分けるための台帳。
  //    フラグ1個だと期限切れも上限超過も検出できないので、**期限と金額条件を必ず持たせる**。
  //    無期限は作らせない (valid_until NOT NULL)。
  //
  // 🚨 世代 (mart_listing_expected_profit) とは別のテーブルにする。
  //    世代は毎晩入れ替わり pruneGenerations で消えるが、この判断は残り続ける。
  //
  // 🚨 キーに expense_scope_version を含める。同じ商品でも自社出荷と FBA では
  //    費用の範囲が違うので、片方の判断をもう片方に自動適用してはいけない
  db.exec(`CREATE TABLE IF NOT EXISTS expected_profit_allowance (
    mall                   TEXT NOT NULL,
    shop_id                TEXT NOT NULL,
    mall_item_key          TEXT NOT NULL,
    expense_scope_version  TEXT NOT NULL,      -- self_v1 / fba_v1
    reason_code            TEXT NOT NULL,      -- stock_clearance / customer_acquisition / partner_commitment / other
    reason_note            TEXT NOT NULL,      -- 狙いの説明 (必須。「その他」を選んだだけで通させない)
    loss_cap_yen           INTEGER NOT NULL,   -- 1個あたりの損失上限。正の数で持つ (画面は「300円まで」)
    valid_from             TEXT NOT NULL,      -- JST の 'YYYY-MM-DD'
    valid_until            TEXT NOT NULL,      -- JST の 'YYYY-MM-DD'。この日いっぱい有効
    decided_by             TEXT NOT NULL,      -- 決めた人
    review_by              TEXT,               -- 見直す人
    snapshot_profit        REAL,               -- 決めたときの想定利益 (何を見て決めたか)
    snapshot_generation_id TEXT,
    created_at             TEXT NOT NULL,
    created_by             TEXT NOT NULL,      -- ログインユーザー (decided_by とは別。入力ではなく事実)
    updated_at             TEXT NOT NULL,
    revoked_at             TEXT,               -- NULL = 有効
    revoked_by             TEXT,
    PRIMARY KEY (mall, shop_id, mall_item_key, expense_scope_version)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_epa_live ON expected_profit_allowance(expense_scope_version, revoked_at)');

  // 変更履歴。「誰がいつ何を許容したか」を後から追えるようにする (取り消しても残す)
  db.exec(`CREATE TABLE IF NOT EXISTS expected_profit_allowance_log (
    log_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    mall                   TEXT NOT NULL,
    shop_id                TEXT NOT NULL,
    mall_item_key          TEXT NOT NULL,
    expense_scope_version  TEXT NOT NULL,
    action                 TEXT NOT NULL,      -- create / update / revoke
    payload                TEXT NOT NULL,      -- JSON (そのときの内容)
    actor                  TEXT NOT NULL,
    acted_at               TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_epal_key ON expected_profit_allowance_log(mall, shop_id, mall_item_key, acted_at DESC)');

  migrate(db);
}

export const SETTING_AMAZON_SELLER_ID = 'amazon_seller_id';

export function getSetting(db, key) {
  try { return db.prepare('SELECT value FROM expected_profit_setting WHERE key = ?').get(key)?.value ?? null; }
  catch { return null; }
}

export function setSetting(db, key, value, now = new Date().toISOString()) {
  db.prepare(`INSERT INTO expected_profit_setting (key, value, updated_at) VALUES (?, ?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`)
    .run(key, value == null ? null : String(value), now);
}

/**
 * 既にあるDBへの列追加。
 *
 * 🚨 CREATE TABLE IF NOT EXISTS は既存テーブルに列を足さない。
 *    miniPC と Render の両方に既存DBがあるので、ここを通さないと
 *    「送る側にはある列が、受け取る側では無い」状態になる。
 */
/**
 * 既存DBに足す列の一覧。
 * 🚨 列を足すときは**ここに1行足すだけ**にする。migrate に直接書くと、
 *    試験が特定の列名を決め打ちしていて「足し忘れ」を検出できない
 */
export const MIGRATED_COLUMNS = [
  ['mart_listing_expected_profit', 'unit_quantity', 'INTEGER'],
  ['mart_listing_expected_profit', 'ne_code_source', 'TEXT'],
  ['mall_price_snapshot', 'shipping_group', 'TEXT'],
  // どの配送方法で計算したかを行に残す (2026-09-08)
  ['mart_listing_expected_profit', 'shipping_rate_name', 'TEXT'],
  ['mart_listing_expected_profit', 'shipping_rate_category', 'TEXT'],
  ['mart_listing_expected_profit', 'shipping_group', 'TEXT'],
  // 在庫数・取扱区分を行に残す (2026-09-09)。表示専用
  ['mart_listing_expected_profit', 'handling_class', 'TEXT'],
  ['mart_listing_expected_profit', 'stock_qty', 'INTEGER'],
  ['mart_listing_expected_profit', 'stock_allocated_qty', 'INTEGER'],
];

export function migrate(db) {
  for (const [table, column, type] of MIGRATED_COLUMNS) addColumnIfMissing(db, table, column, type);
}

/**
 * 計算結果に書き込む列。
 *
 * 🚨 世代を作る側 (miniPC) と受け取る側 (Render) で **同じ一覧を使う**。
 *    以前は同じ 57 列の INSERT が 2 か所に写してあり、片方だけ直すと
 *    「送ったのに保存されない列」ができる状態だった。
 */
export const MART_ROW_COLUMNS = [
  'generation_id', 'mall', 'shop_id', 'mall_item_key', 'ne_code', 'ne_code_source', 'product_name', 'sales_class',
  'handling_class', 'stock_qty', 'stock_allocated_qty',
  'fulfillment', 'listing_status', 'price_incl_tax', 'price_ex_tax', 'postage_revenue_ex_tax', 'revenue_ex_tax',
  'tax_rate', 'cost_ex_tax', 'cost_method', 'unit_quantity',
  'shipping_code', 'shipping_method', 'shipping_rate_name', 'shipping_rate_category', 'shipping_group',
  'shipping_fee_ex_tax', 'shipping_work_ex_tax', 'shipping_material_ex_tax', 'shipping_labor_ex_tax',
  'shipping_total_ex_tax', 'fba_fee_ex_tax', 'referral_fee_ex_tax', 'closing_fee_ex_tax', 'per_item_fee_ex_tax',
  'fee_total_ex_tax', 'fee_rate_display', 'fee_breakdown', 'expected_profit', 'expected_margin_rate',
  'listing_enum_status', 'listing_enum_valid_until', 'price_status', 'price_valid_until', 'fee_status',
  'fee_valid_until', 'cost_status', 'cost_valid_until', 'shipping_master_status', 'shipping_master_valid_until',
  'shipping_revenue_status', 'scenario_fit', 'calculation_status', 'incomplete_reason', 'rank_eligible',
  'rank_exclusion_reason', 'expense_scope_version', 'input_snapshot', 'formula_version', 'scenario_version',
  'fee_rate_version', 'code_version', 'price_run_id', 'built_at',
];

/** @param {string} verb 'INSERT INTO' か 'INSERT OR REPLACE INTO' */
export function martRowInsertSql(verb = 'INSERT INTO') {
  return `${verb} mart_listing_expected_profit (${MART_ROW_COLUMNS.join(', ')})`
    + ` VALUES (${MART_ROW_COLUMNS.map(c => '@' + c).join(', ')})`;
}

/**
 * 受信した行を「この版が知っている列」だけに揃える。
 *
 * 🚨 送信側 (miniPC) が先に新しい版になっても受信が落ちないようにする。
 *    better-sqlite3 は名前付きパラメータに余計なキーがあると例外を投げるため、
 *    ここで落とさないと「列を1つ足しただけで一晩ぶん公開できない」が起きる (§16-15 の類型)。
 *    知らない列は保存されない = 画面には出ないが、その夜のぶんが丸ごと消えるよりよい。
 */
export function pickMartRow(row) {
  const out = {};
  for (const col of MART_ROW_COLUMNS) out[col] = row[col] === undefined ? null : row[col];
  return out;
}

/** 冪等な列追加 (エラーは握り潰さない) */
export function addColumnIfMissing(db, table, column, typeClause) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
  if (!cols.includes(column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${typeClause}`);
    return true;
  }
  return false;
}

/**
 * 現在公開中の世代 (無ければ null)。
 * 🚨 malls_degraded / malls_included は DB では JSON 文字列。
 *    ここで配列に直して返す (画面が .map するので、文字列のままだと描画が落ちる)
 */
export function getPublishedGeneration(dbh = getExpectedProfitDB()) {
  const row = dbh.prepare(`
    SELECT p.generation_id, p.seq, p.published_at, g.built_at, g.row_count,
           g.ok_count, g.rank_eligible_count, g.malls_included, g.malls_degraded
    FROM expected_profit_publish_pointer p
    JOIN expected_profit_generation g ON g.generation_id = p.generation_id
    WHERE p.id = 1
  `).get();
  if (!row) return null;
  return {
    ...row,
    malls_included: parseJsonArray(row.malls_included),
    malls_degraded: parseJsonArray(row.malls_degraded),
  };
}

function parseJsonArray(v) {
  if (Array.isArray(v)) return v;
  if (typeof v !== 'string' || v.trim() === '') return [];
  try {
    const parsed = JSON.parse(v);
    return Array.isArray(parsed) ? parsed : [];
  } catch { return []; }
}

/** 次の seq (単調増加。逆転公開の防止に使う) */
export function nextSeq(dbh = getExpectedProfitDB()) {
  const row = dbh.prepare('SELECT COALESCE(MAX(seq), 0) AS m FROM expected_profit_generation').get();
  return (row?.m || 0) + 1;
}
