/**
 * 自社/AMC 商品を「すでに採用した企画」として書き出す (miniPC で実行)。
 *
 *   node scripts/export-own-products.cjs [出力先パス]
 *   既定の出力先 = C:\Users\bfaith\product-idea-scout\data\own-products.json
 *
 * なぜ必要か:
 *   新商品企画スカウトは、これから採否を貯めても年に数十件しか溜まらない。
 *   一方 **自社商品948ファミリーは「すでに採用した企画」の実例**であり、
 *   終売品も残すが、終売を失敗とは断定しない。
 *   これを入れないと、ツールはいつまでも過去から学べない。
 *
 * ⚠️このスクリプトは warehouse.db を **読むだけ**。書き込みはしない。
 *   warehouse.db は miniPC にしかないので、Render では動かない (product-idea-scout と同じ側で実行する)。
 *
 * 出す値の決め方:
 *   - 実績は **数量ベース**。NE の受注金額は信頼できないため (社内既知)
 *   - 初回180日 = new_product_launch_date から180日。発売から180日経っていないものは null
 *     (「売れなかった」と「まだ分からない」を混ぜない)
 *   - ファミリー = 商品名の「【」より前。色/容量違いを1つに束ねる。
 *     SKU単位のままだと、ジャージ補修シートが色ごとに5行に散って「どれも数個しか売れていない」に見える
 */
const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

// ⚠️%USERPROFILE% を当てにしてはいけない。
//   このスクリプトは miniPC のタスクスケジューラから **SYSTEM** で走るので、
//   USERPROFILE は C:\WINDOWS\system32\config\systemprofile になり、warehouse.db を見失う
//   (2026-08-28 に実際に踏んだ)。リポジトリからの相対で解決すれば実行ユーザーに依らない。
const REPO_ROOT = path.join(__dirname, '..');
const DB_FILE = process.env.WAREHOUSE_DB
  || path.join(process.env.DATA_DIR || path.join(REPO_ROOT, 'data'), 'warehouse.db');
// 出力先は bat から明示的に渡す。既定は「ポータルと同じ階層の product-idea-scout」
const OUT = process.argv[2]
  || path.join(REPO_ROOT, '..', 'product-idea-scout', 'data', 'own-products.json');

/** 色/容量違いを束ねるキー。「【」より前が空なら商品名そのもの (【で始まる商品が222件ある) */
function familyKey(name) {
  const head = String(name || '').split('【')[0].split(/[_｜|]/)[0].trim();
  return head || String(name || '').trim() || '(名称なし)';
}

if (!fs.existsSync(DB_FILE)) {
  console.error(`warehouse.db が見つかりません: ${DB_FILE}`);
  process.exit(1);
}
const db = new Database(DB_FILE, { readonly: true, fileMustExist: true });
db.pragma('query_only = ON');

// 売上ビューを商品ごとに繰り返し走査せず、一度だけ集計する。
const rows = db.prepare(`
  WITH sales AS MATERIALIZED (
    SELECT s.商品コード AS code, SUM(s.数量) AS qtyAll, MAX(s.日付) AS lastSoldOn,
      SUM(CASE WHEN s.日付 >= p.new_product_launch_date
        AND s.日付 < date(p.new_product_launch_date, '+180 day') THEN s.数量 ELSE 0 END) AS qty180
    FROM f_sales_by_product s JOIN m_products p ON p.商品コード = s.商品コード
    WHERE p.売上分類 IN (1, 2) GROUP BY s.商品コード
  ), single_skus AS (
    SELECT seller_sku, MIN(ne_code) AS ne_code FROM m_sku_components GROUP BY seller_sku HAVING COUNT(*)=1
  ), asin_links AS (
    SELECT c.ne_code AS code, a.asin FROM single_skus c
      JOIN amazon_sku_fees a ON lower(a.seller_sku)=c.seller_sku
    UNION
    SELECT lower(a.seller_sku) AS code, a.asin FROM amazon_sku_fees a
      WHERE NOT EXISTS (SELECT 1 FROM m_sku_components c WHERE c.seller_sku=lower(a.seller_sku))
  ), asins AS (
    SELECT code, GROUP_CONCAT(DISTINCT asin) AS asins FROM asin_links
      WHERE asin IS NOT NULL AND asin <> '' GROUP BY code
  )
  SELECT p.商品コード AS code, p.商品名 AS name, p.取扱区分 AS status,
    p.標準売価 AS price, p.原価 AS cost, p.売上分類 AS salesClass,
    p.商品区分 AS productType, p.仕入先コード AS supplierCode,
    p.原価状態 AS costStatus, p.送料 AS shipping, p.updated_at AS updatedAt,
    p.new_product_launch_date AS launchedOn, a.asins,
    CASE WHEN date(p.new_product_launch_date, '+180 day') <= date('now') THEN COALESCE(s.qty180,0) END AS qty180,
    COALESCE(s.qtyAll,0) AS qtyAll, s.lastSoldOn
  FROM m_products p
  LEFT JOIN sales s ON s.code=p.商品コード LEFT JOIN asins a ON a.code=p.商品コード
  WHERE p.売上分類 IN (1,2) ORDER BY p.new_product_launch_date, p.商品コード
`).all();

// ── ファミリー単位に束ねる ──
const families = new Map();
for (const r of rows) {
  const label = familyKey(r.name) + (r.productType === '単品' ? '' : `（${r.productType}）`);
  const key = r.salesClass === 2 ? `AMC参考: ${label}` : label;
  let f = families.get(key);
  if (!f) {
    f = {
      familyKey: key, skuCount: 0, asins: [], products: [], sourceUpdatedAt: r.updatedAt, unknownQty180: false,
      // 発売日はファミリー内で最も古いもの = その企画をいつ出したか
      launchedOn: r.launchedOn, lastSoldOn: null,
      qty180: null, qtyAll: 0,
      activeSkus: 0, discontinuedSkus: 0,
      medianPrice: [], salesClass: r.salesClass,
    };
    families.set(key, f);
  }
  f.skuCount++;
  f.products.push({ code: r.code, name: r.name, productType: r.productType, handling: r.status, supplierCode: r.supplierCode, price: r.price, cost: r.cost, costStatus: r.costStatus, shipping: r.shipping, updatedAt: r.updatedAt });
  if (r.updatedAt < f.sourceUpdatedAt) f.sourceUpdatedAt = r.updatedAt;
  if (r.qty180 == null) f.unknownQty180 = true;
  if (r.asins) for (const a of r.asins.split(',')) { if (a && !f.asins.includes(a)) f.asins.push(a); }
  if (r.launchedOn && (!f.launchedOn || r.launchedOn < f.launchedOn)) f.launchedOn = r.launchedOn;
  if (r.lastSoldOn && (!f.lastSoldOn || r.lastSoldOn > f.lastSoldOn)) f.lastSoldOn = r.lastSoldOn;
  // ⚠️180日が未経過のSKUは加算しない。混ぜると「まだ分からない」が「売れなかった」に化ける
  if (r.qty180 !== null && r.qty180 !== undefined) f.qty180 = (f.qty180 || 0) + r.qty180;
  f.qtyAll += r.qtyAll || 0;
  if (r.status === '取扱中') f.activeSkus++; else f.discontinuedSkus++;
  if (r.price) f.medianPrice.push(r.price);
}

const median = (a) => {
  if (!a.length) return null;
  const s = [...a].sort((x, y) => x - y);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : Math.round((s[m - 1] + s[m]) / 2);
};

const out = [...families.values()].map((f) => ({
  familyKey: f.familyKey,
  skuCount: f.skuCount,
  asins: f.asins,
  launchedOn: f.launchedOn,
  lastSoldOn: f.lastSoldOn,
  qty180: f.unknownQty180 ? null : f.qty180,
  products: f.products, sourceUpdatedAt: f.sourceUpdatedAt,
  qtyAll: f.qtyAll,
  activeSkus: f.activeSkus,
  discontinuedSkus: f.discontinuedSkus,
  medianPrice: median(f.medianPrice),
  salesClass: f.salesClass,
  // ⭐この企画がどうなったか。学習の教師データになる部分
  //   active     = いま売っている
  //   withdrawn  = 全SKU終売 (= うまくいかなかった、または役目を終えた)
  //   shrinking  = 一部だけ終売 (色を増やしすぎた等)
  outcome: f.activeSkus === 0 ? 'withdrawn' : (f.discontinuedSkus > 0 ? 'shrinking' : 'active'),
})).sort((a, b) => (b.qty180 || 0) - (a.qty180 || 0));

db.close();
// CSV/JSONを書き直した時刻ではなく、商品DBの最も古い更新日時で鮮度を示す。
const sourceTimes = rows.map(r => {
  const value = String(r.updatedAt || '');
  return Date.parse(/(?:Z|[+-]\d{2}:\d{2})$/.test(value) ? value : value.replace(' ', 'T') + 'Z');
});
const sourceUpdatedAt = sourceTimes.length && sourceTimes.every(Number.isFinite)
  ? new Date(Math.min(...sourceTimes)).toISOString() : null;

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify({
  generatedAt: new Date().toISOString(),
  algorithmVersion: 2,
  sourceUpdatedAt,
  scope: "売上分類1を自社、2をAMC参考として分離。単品・セット・例外と終売を含む。発売日欠損も含む",
  asinSource: "m_sku_components + amazon_sku_fees（単一構成、または商品コード直接一致）",
  source: DB_FILE,
  skuCount: rows.length,
  familyCount: out.length,
  families: out,
}, null, 1));

const withAsin = out.filter((f) => f.asins.length).length;
const byOutcome = out.reduce((a, f) => { a[f.outcome] = (a[f.outcome] || 0) + 1; return a; }, {});
console.log(`自社/AMC 全商品 ${rows.length}SKU → ${out.length}ファミリー`);
console.log(`  ASIN紐付けあり ${withAsin}ファミリー (${Math.round(withAsin / out.length * 100)}%)`);
console.log(`  結果: ${JSON.stringify(byOutcome)}`);
console.log(`→ ${OUT}`);
