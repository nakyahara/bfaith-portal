/**
 * 自社/AMC 商品を「すでに採用した企画」として書き出す (miniPC で実行)。
 *
 *   node scripts/export-own-products.cjs [出力先パス]
 *   既定の出力先 = C:\Users\bfaith\product-idea-scout\data\own-products.json
 *
 * なぜ必要か:
 *   新商品企画スカウトは、これから採否を貯めても年に数十件しか溜まらない。
 *   一方 **自社商品948ファミリーは「すでに採用した企画」の実例**であり、
 *   撤退品まで含めれば「うまくいかなかった企画」= 負例も最初から手に入る。
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
const db = new Database(DB_FILE, { readonly: true });

const rows = db.prepare(`
  SELECT
    p.商品コード                     AS code,
    p.商品名                         AS name,
    p.取扱区分                       AS status,
    p.標準売価                       AS price,
    p.原価                           AS cost,
    p.売上分類                       AS salesClass,
    p.new_product_launch_date        AS launchedOn,
    (SELECT GROUP_CONCAT(DISTINCT s.asin) FROM sku_map s WHERE s.ne_code = p.商品コード) AS asins,
    -- 発売から180日ぶんの数量。180日経っていないものは NULL (「売れなかった」と混ぜない)
    CASE WHEN date(p.new_product_launch_date, '+180 day') <= date('now') THEN (
      SELECT COALESCE(SUM(s.数量), 0) FROM f_sales_by_product s
       WHERE s.商品コード = p.商品コード
         AND s.日付 >= p.new_product_launch_date
         AND s.日付 <  date(p.new_product_launch_date, '+180 day')
    ) END                            AS qty180,
    (SELECT COALESCE(SUM(s.数量), 0) FROM f_sales_by_product s WHERE s.商品コード = p.商品コード) AS qtyAll,
    (SELECT MAX(s.日付) FROM f_sales_by_product s WHERE s.商品コード = p.商品コード) AS lastSoldOn
  FROM m_products p
  WHERE p.売上分類 IN (1, 2)
    AND p.商品区分 = '単品'
    AND p.new_product_launch_date IS NOT NULL AND p.new_product_launch_date <> ''
  ORDER BY p.new_product_launch_date
`).all();

// ── ファミリー単位に束ねる ──
const families = new Map();
for (const r of rows) {
  const key = familyKey(r.name);
  let f = families.get(key);
  if (!f) {
    f = {
      familyKey: key, skuCount: 0, asins: [],
      // 発売日はファミリー内で最も古いもの = その企画をいつ出したか
      launchedOn: r.launchedOn, lastSoldOn: null,
      qty180: null, qtyAll: 0,
      activeSkus: 0, discontinuedSkus: 0,
      medianPrice: [], salesClass: r.salesClass,
    };
    families.set(key, f);
  }
  f.skuCount++;
  if (r.asins) for (const a of r.asins.split(',')) { if (a && !f.asins.includes(a)) f.asins.push(a); }
  if (r.launchedOn < f.launchedOn) f.launchedOn = r.launchedOn;
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
  qty180: f.qty180,
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

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify({
  generatedAt: new Date().toISOString(),
  source: DB_FILE,
  skuCount: rows.length,
  familyCount: out.length,
  families: out,
}, null, 1));

const withAsin = out.filter((f) => f.asins.length).length;
const byOutcome = out.reduce((a, f) => { a[f.outcome] = (a[f.outcome] || 0) + 1; return a; }, {});
console.log(`自社/AMC 単品 ${rows.length}SKU → ${out.length}ファミリー`);
console.log(`  ASIN紐付けあり ${withAsin}ファミリー (${Math.round(withAsin / out.length * 100)}%)`);
console.log(`  結果: ${JSON.stringify(byOutcome)}`);
console.log(`→ ${OUT}`);
