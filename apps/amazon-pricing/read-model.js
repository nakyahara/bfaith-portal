/**
 * read-model.js — 「1 出品の全部を 1 行で」読むための SQL (人の画面と AI の view の両方がこれを使う)。
 *
 * 入力はすべて Render に既にある mirror 表 (miniPC の daily-sync が毎日送ってくる)。
 * ★このアプリのために新しい取得処理 (API 呼び出し) は増やしていない
 *   (feedback_外部API連携「分析ツールは UI サーバ完結、実行サーバに新規 API 取得を増やさない」)。
 *
 *   mirror_amazon_sku_fees               … 出品 SKU の母集合 + 手数料 (fetch-amazon-fees.js、daily)
 *   mirror_amazon_price_snapshot_daily   … 自分の価格・カート価格・カート保有 (fetch-amazon-prices.js、daily)
 *   mirror_sku_resolved × mirror_products … Amazon SKU → NE商品コード × 数量 → 原価・税率・送料・商品名
 *   mirror_amazon_finance_sku_daily      … 直近 30 日の販売数 (settlement 起点なので 1〜2 週間遅れる)
 *   ap_policies                          … このアプリの値付け方針
 *
 * 原価は「構成品の原価 × (1+消費税率) × 数量」の合計。★構成品に原価の無いものが 1 つでもあれば
 * 合計を NULL にする (SUM が NULL を無視して「安い合計」を出すのを防ぐ。原価不明を 0 円として扱わない)。
 * SKU の突合は fact 側 (fees / finance / resolved) を LOWER(TRIM()) で片側正規化 (lib/sku-norm.js の規約)。
 */

export const REQUIRED_MIRROR_TABLES = [
  'mirror_amazon_sku_fees',
  'mirror_amazon_price_snapshot_daily',
  'mirror_sku_resolved',
  'mirror_products',
  'mirror_amazon_finance_sku_daily',
];

export const LISTING_360_SQL = `
WITH latest AS (
  SELECT MAX(date_jst) AS d FROM mirror_amazon_price_snapshot_daily
),
cost AS (
  SELECT LOWER(TRIM(r.seller_sku)) AS sku_norm,
         CASE WHEN SUM(CASE WHEN p.原価 IS NULL OR p.原価状態 NOT IN ('COMPLETE','OVERRIDDEN') THEN 1 ELSE 0 END) > 0
              THEN NULL
              ELSE ROUND(SUM(r.quantity * p.原価 * (1 + COALESCE(p.消費税率, 0.10))), 2) END AS cost_incl_tax,
         SUM(CASE WHEN p.原価 IS NULL OR p.原価状態 NOT IN ('COMPLETE','OVERRIDDEN') THEN 1 ELSE 0 END) AS cost_missing_parts,
         COUNT(*) AS parts,
         MAX(p.送料) AS ship_cost,
         MIN(r.ne_code) AS ne_code,
         MIN(COALESCE(p.商品名, r.商品名)) AS ne_name
    FROM mirror_sku_resolved r
    LEFT JOIN mirror_products p ON p.商品コード = r.ne_code
   GROUP BY LOWER(TRIM(r.seller_sku))
),
sales AS (
  SELECT LOWER(TRIM(seller_sku)) AS sku_norm,
         SUM(units_net_sold) AS units_30d,
         SUM(sales_principal_jpy) AS sales_30d
    FROM mirror_amazon_finance_sku_daily
   WHERE date_jst > date((SELECT d FROM latest), '-30 days')
   GROUP BY LOWER(TRIM(seller_sku))
)
SELECT f.seller_sku,
       f.asin,
       f.fulfillment_channel AS channel,
       f.referral_fee_rate, f.fba_fee, f.per_item_fee, f.variable_closing_fee, f.total_fee, f.price_used,
       f.fetched_at AS fees_fetched_at,
       (SELECT d FROM latest) AS snapshot_date_jst,
       s.my_price, s.buybox_price, s.buybox_is_mine,
       c.ne_code, c.ne_name, c.cost_incl_tax, c.cost_missing_parts, c.parts, c.ship_cost,
       sa.units_30d, sa.sales_30d,
       po.mode, po.floor_price, po.ceiling_price, po.offset_jpy, po.min_margin_rate,
       po.note AS policy_note, po.updated_at AS policy_updated_at, po.updated_by AS policy_updated_by
  FROM mirror_amazon_sku_fees f
  LEFT JOIN mirror_amazon_price_snapshot_daily s
         ON s.seller_sku = f.seller_sku AND s.date_jst = (SELECT d FROM latest)
  LEFT JOIN cost c  ON c.sku_norm = LOWER(TRIM(f.seller_sku))
  LEFT JOIN sales sa ON sa.sku_norm = LOWER(TRIM(f.seller_sku))
  LEFT JOIN ap_policies po ON po.seller_sku = f.seller_sku
`;

/** 必要な mirror 表がそろっているか */
export function mirrorTablesAvailable(db) {
  const have = new Set(db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all().map((r) => r.name));
  const missing = REQUIRED_MIRROR_TABLES.filter((t) => !have.has(t));
  return { ok: missing.length === 0, missing };
}

/**
 * 原価は 1/100 円の分解能で持つ (税率 × 数量の積は小数 2 桁まで意味がある)。整数円に丸めると
 * 下限が 1 円低く出る場合がある (Codex R3 Medium: 1103.3 → 1103 で 1880 → 1879)。表示は yen() が丸める
 */
const shapeRow = (r) => ({
  ...r,
  // SQL 側で ROUND(…, 2) 済み (view と同じ値)。JS 側は浮動小数の表現ゆれを整えるだけ
  cost_incl_tax: r.cost_incl_tax == null ? null : Math.round(r.cost_incl_tax * 100) / 100,
  mode: r.mode || 'off',
  offset_jpy: r.offset_jpy ?? 0,
});

/** 全出品の 360 行 (表示・判定の両方でこの値を使う) */
export function loadListings(db) {
  return db.prepare(LISTING_360_SQL).all().map(shapeRow);
}

export function loadListing(db, sku) {
  const r = db.prepare(`SELECT * FROM (${LISTING_360_SQL}) WHERE seller_sku = ?`).get(sku);
  return r ? shapeRow(r) : null;
}

/** 1 SKU の価格の推移 (日次スナップショット、新しい順) */
export function priceHistory(db, sku, days = 90) {
  return db.prepare(`
    SELECT date_jst, my_price, buybox_price, buybox_is_mine
      FROM mirror_amazon_price_snapshot_daily
     WHERE seller_sku = ? AND date_jst > date((SELECT MAX(date_jst) FROM mirror_amazon_price_snapshot_daily), ?)
     ORDER BY date_jst DESC`).all(sku, `-${days} days`);
}

/** データの鮮度 (画面の「今日のデータ」欄) */
export function dataFreshness(db) {
  const snap = db.prepare('SELECT MAX(date_jst) AS d, COUNT(*) AS n FROM mirror_amazon_price_snapshot_daily WHERE date_jst = (SELECT MAX(date_jst) FROM mirror_amazon_price_snapshot_daily)').get();
  const fees = db.prepare('SELECT MAX(fetched_at) AS t, COUNT(*) AS n FROM mirror_amazon_sku_fees').get();
  const fin = db.prepare('SELECT MAX(date_jst) AS d FROM mirror_amazon_finance_sku_daily').get();
  return {
    snapshot_date_jst: snap?.d ?? null, snapshot_rows: snap?.n ?? 0,
    fees_fetched_at: fees?.t ?? null, fees_rows: fees?.n ?? 0,
    finance_last_date_jst: fin?.d ?? null,
    sku_norm_collisions: skuNormCollisions(db),
  };
}

/**
 * 正規化 (LOWER/TRIM) すると同じになる出品 SKU が母集合に 2 つ以上あるか。
 * 原価・販売は正規化キーで突合するので、衝突していると別々の出品に同じ原価・販売が付く (Codex R1 Medium)。
 * 0 件が前提 (Amazon の 99.99% 名寄せ実測)。出たら画面に警告を出し、その行は疑って見る
 * @returns {string[]} 衝突している正規化キー
 */
export function skuNormCollisions(db) {
  return db.prepare(`
    SELECT LOWER(TRIM(seller_sku)) AS k FROM mirror_amazon_sku_fees
     GROUP BY LOWER(TRIM(seller_sku)) HAVING COUNT(*) > 1`).all().map((r) => r.k);
}
