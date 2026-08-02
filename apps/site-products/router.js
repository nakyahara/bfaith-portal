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

/**
 * モール別lookupのprepared statement群。
 * mirror表はデプロイ時期によって存在しないことがあるため、prepare失敗はそのモールを
 * null運用に落とす (fail-soft。新mirror表はfail-soft必須の運用ルールに準拠)。
 */
let stmts = null;
let stmtsError = {};
function prepareStmts(db) {
  if (stmts) return stmts;
  stmts = {};
  const defs = {
    products: `SELECT 商品コード AS code, 商品名 AS name, 商品区分 AS productType,
                      取扱区分 AS handling, 売上分類 AS salesCategory,
                      標準売価 AS price, 在庫数 AS stock, 引当数 AS allocated,
                      代表商品コード AS representativeCode
               FROM mirror_products
               WHERE 商品コード IS NOT NULL AND trim(商品コード) <> ''
               ORDER BY 商品コード`,
    rakutenCode: `SELECT rakuten_code FROM mirror_rakuten_sku_map
                  WHERE ne_code = ? ORDER BY updated_at DESC LIMIT 1`,
    rakutenUrl: `SELECT item_url FROM mirror_rakuten_item_purchaser_snapshot
                 WHERE item_manage_number = ? AND item_url IS NOT NULL AND item_url <> ''
                 ORDER BY date_jst DESC LIMIT 1`,
    // 楽天の商品ページは「親商品」単位。色/サイズのバリエーションSKUコードでURLを組むと404になる
    // (2026-08-02: サンプル20件中7件が404だった)。実在する商品管理番号かをRMS由来データで確認する。
    // ⚠️商品ごとに個別照会すると6,800商品×最大4候補で応答が90秒を超えたため、一括で読んで
    // メモリ上のSetで判定する (2026-08-02のタイムアウト対処)
    rakutenItemNumbers: `SELECT DISTINCT item_manage_number FROM mirror_rakuten_item_daily
                         WHERE item_manage_number IS NOT NULL AND trim(item_manage_number) <> ''`,
    yahooItem: `SELECT item_code FROM mirror_yahoo_item_daily
                WHERE item_code = ? ORDER BY date_jst DESC LIMIT 1`,
    // Amazon ASINは供給元が3つあるので順に試す (2026-08-02: 販売実績のみだと解決0件だった)。
    // SKU突合は case/空白ゆれに強くするためLOWER(TRIM())で行う (運用ルール: SKUのcase揃え)
    amazonAsinFinance: `SELECT f.asin_norm AS asin
                        FROM mirror_sku_resolved r
                        JOIN mirror_amazon_finance_sku_daily f
                          ON LOWER(TRIM(f.seller_sku)) = LOWER(TRIM(r.seller_sku))
                        WHERE LOWER(TRIM(r.ne_code)) = LOWER(TRIM(?)) AND TRIM(f.asin_norm) <> ''
                        ORDER BY f.date_jst DESC LIMIT 1`,
    amazonAsinPrice: `SELECT p.asin AS asin
                      FROM mirror_sku_resolved r
                      JOIN mirror_amazon_price_snapshot_daily p
                        ON LOWER(TRIM(p.seller_sku)) = LOWER(TRIM(r.seller_sku))
                      WHERE LOWER(TRIM(r.ne_code)) = LOWER(TRIM(?)) AND TRIM(p.asin) <> ''
                      ORDER BY p.date_jst DESC LIMIT 1`,
    amazonAsinFees: `SELECT s.asin AS asin
                     FROM mirror_sku_resolved r
                     JOIN mirror_amazon_sku_fees s
                       ON LOWER(TRIM(s.seller_sku)) = LOWER(TRIM(r.seller_sku))
                     WHERE LOWER(TRIM(r.ne_code)) = LOWER(TRIM(?)) AND TRIM(s.asin) <> ''
                     LIMIT 1`,
    qoo10Item: `SELECT item_no FROM mirror_qoo10_items WHERE seller_code = ? LIMIT 1`,
    aupaySku: `SELECT aupay_sku_key FROM mirror_aupay_finance_sku_daily
               WHERE ne_code = ? ORDER BY date_jst DESC LIMIT 1`,
  };
  for (const [key, sql] of Object.entries(defs)) {
    try {
      stmts[key] = db.prepare(sql);
    } catch (e) {
      stmts[key] = null;
      stmtsError[key] = String(e.message || e);
      console.warn(`[site-products] prepare失敗 (${key}) — このlookupはnull運用:`, e.message);
    }
  }
  return stmts;
}

/**
 * lookupの実行時エラーもモール単位でfail-softにする (Codex R1 High):
 * prepare成功後でもatomic swap等で表が変わると .get() が失敗しうる。
 * その場合は該当lookupだけnull運用に落とし、runtimeDegradedへ記録する (500にしない)。
 */
function safeGet(s, key, runtimeDegraded, arg) {
  const stmt = s[key];
  if (!stmt) return null;
  try {
    return stmt.get(arg);
  } catch (e) {
    if (!runtimeDegraded.has(key)) {
      runtimeDegraded.add(key);
      console.warn(`[site-products] lookup実行時エラー (${key}) — null運用に降格:`, e.message);
    }
    return null;
  }
}

/**
 * 楽天SKUコードから「実在する商品管理番号 (親商品)」を解決する。
 * 楽天の商品ページは親商品単位で、色/サイズはバリエーションとして1ページにまとまる。
 * そのためSKUコード (例: 0726-000629-bk) でURLを組むと404になる (2026-08-02に実測)。
 * ハイフン区切りの末尾を1段ずつ削りながら、RMS由来データ (mirror_rakuten_item_daily) に
 * 実在する番号かを確認する。見つからなければ null = URLを出さない。
 */
function resolveRakutenItemNumber(itemNumbers, rakutenCode) {
  if (!itemNumbers) return null;
  const parts = String(rakutenCode).split('-');
  // 例: a-b-c-d → [a-b-c-d, a-b-c, a-b] (最大3段まで遡る。1要素まで削ると別商品に当たる恐れ)
  const maxStrip = Math.min(3, parts.length - 1);
  for (let i = 0; i <= maxStrip; i++) {
    const candidate = parts.slice(0, parts.length - i).join('-');
    if (!candidate) break;
    const hit = itemNumbers.get(candidate.toLowerCase());
    if (hit) return hit;
  }
  return null;
}

function lookupMalls(s, code, runtimeDegraded, asinSourceCounts, itemNumbers) {
  const malls = {
    rakuten: { code: null, url: null },
    yahoo: { code: null, url: null },
    amazon: { asin: null, url: null },
    qoo10: { itemNo: null, url: null },
    aupay: { skuKey: null, url: null }, // URL形式が未確定のためid のみ (site側で将来対応)
  };

  const rk = safeGet(s, 'rakutenCode', runtimeDegraded, code);
  if (rk?.rakuten_code) {
    malls.rakuten.code = rk.rakuten_code;
    // 楽天の商品ページは親商品単位。SKUコード (例: xxx-bk) から親 (xxx) を段階的に探し、
    // **RMS由来データに実在する商品管理番号だけ**をURL化する (存在しなければURLを出さない)
    const itemNumber = resolveRakutenItemNumber(itemNumbers, rk.rakuten_code);
    if (itemNumber) {
      const snap = safeGet(s, 'rakutenUrl', runtimeDegraded, itemNumber);
      malls.rakuten.url =
        snap?.item_url ?? `https://item.rakuten.co.jp/${RAKUTEN_SHOP_SLUG}/${itemNumber.toLowerCase()}/`;

      // Yahoo (RYS連携): アイテムコード=楽天商品管理番号の小文字。実在確認できた場合のみURL化
      const y = safeGet(s, 'yahooItem', runtimeDegraded, itemNumber.toLowerCase());
      if (y?.item_code) {
        malls.yahoo.code = y.item_code;
        malls.yahoo.url = `https://store.shopping.yahoo.co.jp/${YAHOO_SELLER_ID}/${y.item_code}.html`;
      }
    }
  }

  // ASINは finance → price_snapshot → fees の順に探す (どれで解決したかを診断に記録)
  for (const key of ['amazonAsinFinance', 'amazonAsinPrice', 'amazonAsinFees']) {
    const az = safeGet(s, key, runtimeDegraded, code);
    if (az?.asin) {
      malls.amazon.asin = az.asin;
      malls.amazon.url = `https://www.amazon.co.jp/dp/${az.asin}`;
      asinSourceCounts[key] = (asinSourceCounts[key] ?? 0) + 1;
      break;
    }
  }

  const q = safeGet(s, 'qoo10Item', runtimeDegraded, code);
  if (q?.item_no) {
    malls.qoo10.itemNo = q.item_no;
    malls.qoo10.url = `https://www.qoo10.jp/g/${q.item_no}`;
  }

  const au = safeGet(s, 'aupaySku', runtimeDegraded, code);
  if (au?.aupay_sku_key) {
    malls.aupay.skuKey = au.aupay_sku_key;
  }

  return malls;
}

// GET /products — 全商品スナップショット (読み取り専用)
router.get('/products', requireSiteReadToken, (req, res) => {
  res.set('Cache-Control', 'no-store');
  let db;
  try {
    db = getMirrorDB();
  } catch (e) {
    return res.status(503).json({ ok: false, error: 'mirror_db_unavailable', message: String(e.message || e) });
  }
  try {
    const s = prepareStmts(db);
    if (!s.products) {
      // detailはログのみ (スキーマ情報をレスポンスに漏らさない)
      return res.status(503).json({ ok: false, error: 'mirror_products_unavailable' });
    }
    // 1トランザクションで読む (syncのatomic swapとの競合回避、product-management-listと同じ慣例)
    const runtimeDegraded = new Set();
    const asinSourceCounts = {}; // どのソースでASINが解決できたか (診断用)
    const result = db.transaction(() => {
      // 楽天の実在商品管理番号を一括ロード (小文字キー → 元表記)。商品ごとの個別照会は遅すぎる
      let itemNumbers = null;
      try {
        itemNumbers = new Map(
          (s.rakutenItemNumbers?.all() ?? []).map((r) => [
            String(r.item_manage_number).toLowerCase(),
            r.item_manage_number,
          ])
        );
      } catch (e) {
        runtimeDegraded.add('rakutenItemNumbers');
        console.warn('[site-products] 楽天商品番号の一括読込に失敗:', e.message);
      }
      const rows = s.products.all();
      return rows.map((r) => ({
        code: r.code,
        name: r.name ?? '',
        productType: r.productType ?? null,
        handling: r.handling ?? null,
        salesCategory: r.salesCategory ?? null,
        price: r.price ?? null,
        stockFree: Math.max(0, (r.stock ?? 0) - (r.allocated ?? 0)),
        representativeCode: r.representativeCode ?? null,
        malls: lookupMalls(s, r.code, runtimeDegraded, asinSourceCounts, itemNumbers),
      }));
    })();

    // 監査ログ (設計書§15 PR-4a): 誰に何件返したか。PIIなし
    console.log('[site-products] served', { count: result.length, ip: req.ip });

    return res.json({
      ok: true,
      generatedAt: new Date().toISOString(),
      source: 'warehouse-mirror',
      count: result.length,
      degradedLookups: [...new Set([...Object.keys(stmtsError), ...runtimeDegraded])],
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
