/**
 * site-products — コーポレートサイト (bfaith-site) 向け商品スナップショットAPI
 *
 * bfaith-siteのGitHub Actions (sync-products) が夜間に取得し、静的サイトの
 * 商品ページ/商品カード生成に使う (設計正本=AI_reference『ブログ自動化_サイト建て替え_20260731\設計書_v1.3』§9)。
 *
 * 認証: GET /products : SITE_PRODUCTS_READ_TOKEN (専用read token、x-read-tokenヘッダのみ、
 *       query不可)。env未設定なら503 fail-closed (mirror系の既存慣例に準拠)。
 * mount: session認証の外側 (server.jsで requireAppAccess を通さない)。読み取り専用・書き込み系なし。
 *
 * ⚠️性能 (2026-08-02): 全lookupを「一括ロード → メモリ上のMapで解決」する。
 *   商品ごとの個別照会 (6,800商品 × 8lookup) は応答90秒超でsite側の同期がタイムアウトした。
 *   特に LOWER(TRIM()) を使ったJOINはインデックスが効かず致命的だった。
 *
 * データ契約 (bfaith-site側 sync-products.js が検証する):
 *  - products[].code/name/price/stockFree/malls.{rakuten,yahoo,amazon,qoo10,aupay}
 *  - モールURLは判明分のみ (null許容)。site側はurl非nullのモールだけカードに描画する
 *  - 価格は税込 (mirror_products.標準売価そのまま)。site側の表示は/products/ページのみ
 */
import express from 'express';
import crypto from 'node:crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = express.Router();

const RAKUTEN_SHOP_SLUG = process.env.RAKUTEN_SHOP_SLUG || 'b-faith';
const YAHOO_SELLER_ID = process.env.YAHOO_SELLER_ID || 'b-faith01';

function timingSafeEq(got, expected) {
  const a = Buffer.from(got, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// 専用read token (MIRROR_READ_TOKENと分離: サイトビルド用トークンが漏れても他のmirror APIは読めない)
function requireSiteReadToken(req, res, next) {
  const expected = process.env.SITE_PRODUCTS_READ_TOKEN || '';
  if (!expected) {
    return res.status(503).json({ ok: false, error: 'site_products_read_token_unset' });
  }
  const got = String(req.headers['x-read-token'] || '');
  if (!got || !timingSafeEq(got, expected)) {
    console.warn('[site-products] auth failed', { ip: req.ip });
    return res.status(401).json({ ok: false, error: 'invalid_read_token' });
  }
  next();
}

const norm = (v) => String(v ?? '').trim().toLowerCase();

/**
 * lookupに使う表を一括ロードする。1表につき1クエリ。
 * mirror表はデプロイ時期によって存在しないことがあるため、失敗した表だけnull運用に落とす
 * (fail-soft。新mirror表はfail-soft必須の運用ルールに準拠)。
 */
function loadIndexes(db, degraded) {
  const q = (key, sql, build) => {
    try {
      return build(db.prepare(sql).all());
    } catch (e) {
      degraded.add(key);
      console.warn(`[site-products] 一括ロード失敗 (${key}) — このlookupはnull運用:`, e.message);
      return null;
    }
  };
  const pairMap = (rows, k, v) => {
    const m = new Map();
    for (const r of rows) {
      const key = norm(r[k]);
      if (key) m.set(key, r[v]);
    }
    return m;
  };
  // ASIN用: 日付も保持する (複数SKUを持つ商品で「最新のASIN」を選ぶため)
  const datedMap = (rows) => {
    const m = new Map();
    for (const r of rows) {
      const key = norm(r.seller_sku);
      if (!key || !r.asin) continue;
      const prev = m.get(key);
      if (!prev || String(r.date ?? '') > String(prev.date ?? '')) {
        m.set(key, { asin: r.asin, date: String(r.date ?? '') });
      }
    }
    return m;
  };

  return {
    // ne_code (小文字) → 楽天SKUコード
    rakutenByNe: q(
      'rakutenMap',
      `SELECT ne_code, rakuten_code FROM mirror_rakuten_sku_map ORDER BY updated_at ASC`,
      (rows) => pairMap(rows, 'ne_code', 'rakuten_code')
    ),
    // 実在する楽天商品管理番号 (小文字 → 元表記)。親商品の特定に使う
    rakutenItemNumbers: q(
      'rakutenItemNumbers',
      `SELECT DISTINCT item_manage_number FROM mirror_rakuten_item_daily
       WHERE item_manage_number IS NOT NULL AND trim(item_manage_number) <> ''`,
      (rows) => {
        const m = new Map();
        for (const r of rows) m.set(norm(r.item_manage_number), r.item_manage_number);
        return m;
      }
    ),
    // 商品管理番号 (小文字) → 実URL (楽天が返した本物のURL)。
    // ⚠️日次履歴表は全件ロードせず、SQL側でキーごとの最新1行に絞る (Codex指摘: 履歴は増え続ける)
    rakutenUrlByItem: q(
      'rakutenUrls',
      `SELECT s.item_manage_number, s.item_url
       FROM mirror_rakuten_item_purchaser_snapshot s
       JOIN (SELECT item_manage_number, MAX(date_jst) AS d
             FROM mirror_rakuten_item_purchaser_snapshot
             WHERE item_url IS NOT NULL AND item_url <> ''
             GROUP BY item_manage_number) l
         ON l.item_manage_number = s.item_manage_number AND l.d = s.date_jst
       WHERE s.item_url IS NOT NULL AND s.item_url <> ''`,
      (rows) => pairMap(rows, 'item_manage_number', 'item_url')
    ),
    // Yahooに実在するアイテムコード (小文字 → 元表記)
    yahooItems: q(
      'yahooItems',
      `SELECT DISTINCT item_code FROM mirror_yahoo_item_daily
       WHERE item_code IS NOT NULL AND trim(item_code) <> ''`,
      (rows) => {
        const m = new Map();
        for (const r of rows) m.set(norm(r.item_code), r.item_code);
        return m;
      }
    ),
    // ne_code (小文字) → seller_sku[] (Amazon SKU。1商品が複数SKUに紐づく)
    skusByNe: q('skuResolved', `SELECT seller_sku, ne_code FROM mirror_sku_resolved`, (rows) => {
      const m = new Map();
      for (const r of rows) {
        const k = norm(r.ne_code);
        if (!k) continue;
        if (!m.has(k)) m.set(k, []);
        m.get(k).push(norm(r.seller_sku));
      }
      return m;
    }),
    // seller_sku (小文字) → {asin, date}。供給元3つを別Mapで持ち、優先順に引く。
    // ⚠️日次履歴はSQL側でSKUごとの最新1行に絞る+dateを保持し、複数SKUでは最新を選ぶ (Codex指摘)
    asinFinance: q(
      'asinFinance',
      `SELECT f.seller_sku, f.asin_norm AS asin, f.date_jst AS date
       FROM mirror_amazon_finance_sku_daily f
       JOIN (SELECT seller_sku, MAX(date_jst) AS d FROM mirror_amazon_finance_sku_daily
             WHERE TRIM(asin_norm) <> '' GROUP BY seller_sku) l
         ON l.seller_sku = f.seller_sku AND l.d = f.date_jst
       WHERE TRIM(f.asin_norm) <> ''`,
      (rows) => datedMap(rows)
    ),
    asinPrice: q(
      'asinPrice',
      `SELECT p.seller_sku, p.asin, p.date_jst AS date
       FROM mirror_amazon_price_snapshot_daily p
       JOIN (SELECT seller_sku, MAX(date_jst) AS d FROM mirror_amazon_price_snapshot_daily
             WHERE TRIM(asin) <> '' GROUP BY seller_sku) l
         ON l.seller_sku = p.seller_sku AND l.d = p.date_jst
       WHERE TRIM(p.asin) <> ''`,
      (rows) => datedMap(rows)
    ),
    asinFees: q(
      'asinFees',
      `SELECT seller_sku, asin, '' AS date FROM mirror_amazon_sku_fees WHERE TRIM(asin) <> ''`,
      (rows) => datedMap(rows)
    ),
    // 商品コード (小文字) → Qoo10商品番号
    qoo10ByCode: q(
      'qoo10Items',
      `SELECT seller_code, item_no FROM mirror_qoo10_items
       WHERE seller_code IS NOT NULL AND trim(seller_code) <> ''`,
      (rows) => pairMap(rows, 'seller_code', 'item_no')
    ),
    // ne_code (小文字) → auPAY SKUキー (日次履歴のためSQL側で最新1行に絞る)
    aupayByNe: q(
      'aupaySkus',
      `SELECT a.ne_code, a.aupay_sku_key
       FROM mirror_aupay_finance_sku_daily a
       JOIN (SELECT ne_code, MAX(date_jst) AS d FROM mirror_aupay_finance_sku_daily
             WHERE ne_code IS NOT NULL GROUP BY ne_code) l
         ON l.ne_code = a.ne_code AND l.d = a.date_jst
       WHERE a.ne_code IS NOT NULL`,
      (rows) => pairMap(rows, 'ne_code', 'aupay_sku_key')
    ),
  };
}

/**
 * 楽天SKUコードから「実在する商品管理番号 (親商品)」を解決する。
 * 楽天の商品ページは親商品単位で、色/サイズはバリエーションとして1ページにまとまる。
 * そのためSKUコード (例: 0726-000629-bk) でURLを組むと404になる (2026-08-02に実測)。
 * ハイフン区切りの末尾を1段ずつ削りながら、RMS由来データに実在する番号かを確認する。
 * 見つからなければ null = URLを出さない。
 */
function resolveRakutenItemNumber(itemNumbers, rakutenCode) {
  if (!itemNumbers) return null;
  const parts = String(rakutenCode).split('-');
  const maxStrip = Math.min(3, parts.length - 1); // 1要素まで削ると別商品に当たる恐れ
  for (let i = 0; i <= maxStrip; i++) {
    const candidate = parts.slice(0, parts.length - i).join('-');
    if (!candidate) break;
    const hit = itemNumbers.get(norm(candidate));
    if (hit) return hit;
  }
  return null;
}

function lookupMalls(idx, code, asinSourceCounts) {
  const key = norm(code);
  const malls = {
    rakuten: { code: null, url: null },
    yahoo: { code: null, url: null },
    amazon: { asin: null, url: null },
    qoo10: { itemNo: null, url: null },
    aupay: { skuKey: null, url: null }, // URL形式が未確定のためidのみ (site側で将来対応)
  };

  const rakutenCode = idx.rakutenByNe?.get(key);
  if (rakutenCode) {
    malls.rakuten.code = rakutenCode;
    const itemNumber = resolveRakutenItemNumber(idx.rakutenItemNumbers, rakutenCode);
    if (itemNumber) {
      malls.rakuten.url =
        idx.rakutenUrlByItem?.get(norm(itemNumber)) ??
        `https://item.rakuten.co.jp/${RAKUTEN_SHOP_SLUG}/${itemNumber.toLowerCase()}/`;

      // Yahoo (RYS連携): アイテムコード=楽天商品管理番号の小文字。実在確認できた場合のみURL化
      const yahooCode = idx.yahooItems?.get(norm(itemNumber));
      if (yahooCode) {
        malls.yahoo.code = yahooCode;
        malls.yahoo.url = `https://store.shopping.yahoo.co.jp/${YAHOO_SELLER_ID}/${yahooCode}.html`;
      }
    }
  }

  // ASINは finance → price_snapshot → fees の順。同一ソース内では全SKUのうち最新日付を選ぶ
  // (Codex指摘: 複数SKUを持つ商品で古い/不定のASINを拾わないため)
  const sellerSkus = idx.skusByNe?.get(key) ?? [];
  for (const [srcName, map] of [
    ['amazonAsinFinance', idx.asinFinance],
    ['amazonAsinPrice', idx.asinPrice],
    ['amazonAsinFees', idx.asinFees],
  ]) {
    if (!map) continue;
    let best = null;
    for (const sku of sellerSkus) {
      const hit = map.get(sku);
      if (hit && (!best || hit.date > best.date)) best = hit;
    }
    if (best) {
      malls.amazon.asin = best.asin;
      malls.amazon.url = `https://www.amazon.co.jp/dp/${best.asin}`;
      asinSourceCounts[srcName] = (asinSourceCounts[srcName] ?? 0) + 1;
      break;
    }
  }

  const itemNo = idx.qoo10ByCode?.get(key);
  if (itemNo) {
    malls.qoo10.itemNo = itemNo;
    malls.qoo10.url = `https://www.qoo10.jp/g/${itemNo}`;
  }

  const aupaySku = idx.aupayByNe?.get(key);
  if (aupaySku) malls.aupay.skuKey = aupaySku;

  return malls;
}

const PRODUCTS_SQL = `SELECT 商品コード AS code, 商品名 AS name, 商品区分 AS productType,
                             取扱区分 AS handling, 売上分類 AS salesCategory,
                             標準売価 AS price, 在庫数 AS stock, 引当数 AS allocated,
                             代表商品コード AS representativeCode
                      FROM mirror_products
                      WHERE 商品コード IS NOT NULL AND trim(商品コード) <> ''
                      ORDER BY 商品コード`;

// GET /products — 全商品スナップショット (読み取り専用)
router.get('/products', requireSiteReadToken, (req, res) => {
  res.set('Cache-Control', 'no-store');
  let db;
  try {
    db = getMirrorDB();
  } catch (e) {
    return res
      .status(503)
      .json({ ok: false, error: 'mirror_db_unavailable', message: String(e.message || e) });
  }
  try {
    const degraded = new Set();
    const asinSourceCounts = {};
    let rows;
    let idx;
    // 1トランザクションで読む (syncのatomic swapとの競合回避、product-management-listと同じ慣例)
    try {
      db.transaction(() => {
        rows = db.prepare(PRODUCTS_SQL).all();
        idx = loadIndexes(db, degraded);
      })();
    } catch (e) {
      console.error('[site-products] products読み込み失敗:', e);
      return res.status(503).json({ ok: false, error: 'mirror_products_unavailable' });
    }

    const result = rows.map((r) => ({
      code: r.code,
      name: r.name ?? '',
      productType: r.productType ?? null,
      handling: r.handling ?? null,
      salesCategory: r.salesCategory ?? null,
      price: r.price ?? null,
      stockFree: Math.max(0, (r.stock ?? 0) - (r.allocated ?? 0)),
      representativeCode: r.representativeCode ?? null,
      malls: lookupMalls(idx, r.code, asinSourceCounts),
    }));

    // 監査ログ (設計書§15 PR-4a): 誰に何件返したか。PIIなし
    console.log('[site-products] served', { count: result.length, ip: req.ip });

    return res.json({
      ok: true,
      generatedAt: new Date().toISOString(),
      source: 'warehouse-mirror',
      count: result.length,
      degradedLookups: [...degraded],
      // 診断: モール別のURL解決数。0が続く場合は元データ (mirror同期) 側を疑う
      resolved: {
        rakuten: result.filter((p) => p.malls.rakuten.url).length,
        yahoo: result.filter((p) => p.malls.yahoo.url).length,
        amazon: result.filter((p) => p.malls.amazon.url).length,
        qoo10: result.filter((p) => p.malls.qoo10.url).length,
        asinSources: asinSourceCounts,
      },
      products: result,
    });
  } catch (e) {
    console.error('[site-products] error:', e);
    return res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

export default router;
