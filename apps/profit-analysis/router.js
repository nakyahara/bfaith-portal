/**
 * 粗利分析ダッシュボード — ルーター
 *
 * 4ビュー:
 *   1. 粗利ワースト商品ランキング
 *   2. 粗利率ボーダー帯商品
 *   3. 前月比悪化商品
 *   4. モール別粗利比較
 *
 * 管理会計の近似値。現行原価・現行料率ベースの管理指標。
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();

// ─── モール別手数料率（設計書確定値、CASE文ハードコード） ───
const MALL_FEE_RATES = {
  amazon:   0.15,
  rakuten:  0.10,
  yahoo:    0.10,
  aupay:    0.13,
  qoo10:    0.10,
  linegift: 0.13,
  mercari:  0.10,
};

// ─── メイン画面 ───
router.get('/', (req, res) => {
  res.render('profit-analysis', {
    title: '粗利分析',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ─── API: 粗利データ取得 ───

/**
 * 粗利計算エンジン
 * mirror_sales_monthly(by_listing) + mirror_products + mirror_amazon_sku_fees を結合
 */
function calculateProfitData(db, { days = 30, mall = null } = {}) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  // 1. 日次売上（by_listing: モール商品コード粒度で売上金額あり）
  let salesSql = `
    SELECT 商品コード as listing_code, モール, チャネル,
      SUM(数量) as 数量, SUM(売上金額) as 売上金額
    FROM mirror_sales_daily
    WHERE データ種別 = 'by_listing' AND 日付 >= ?
  `;
  const params = [cutoffStr];
  if (mall) { salesSql += ' AND モール = ?'; params.push(mall); }
  salesSql += ' GROUP BY 商品コード, モール, チャネル HAVING SUM(数量) > 0';

  const sales = db.prepare(salesSql).all(...params);

  // 2. NE商品コード粒度の売上（原価計算用、セット展開済み）
  let prodSalesSql = `
    SELECT 商品コード, モール, SUM(数量) as 数量
    FROM mirror_sales_daily
    WHERE データ種別 = 'by_product' AND 日付 >= ?
  `;
  const prodParams = [cutoffStr];
  if (mall) { prodSalesSql += ' AND モール = ?'; prodParams.push(mall); }
  prodSalesSql += ' GROUP BY 商品コード, モール';
  const prodSales = db.prepare(prodSalesSql).all(...prodParams);

  // 3. 商品マスタ（原価 + 送料）
  const products = db.prepare(`
    SELECT 商品コード, 商品名, 原価, 原価ソース, 原価状態, 標準売価, 消費税率, 売上分類, 送料
    FROM mirror_products
  `).all();
  const productMap = new Map();
  for (const p of products) {
    // f_sales_by_listingはLOWER()で格納されるのでキーも小文字化
    productMap.set(p.商品コード?.toLowerCase(), p);
  }

  // 4. SKUマップ（seller_skuもne_codeも小文字で統一）
  const skuMap = db.prepare('SELECT seller_sku, ne_code, 数量 FROM mirror_sku_map').all();
  const skuToNeMap = new Map();
  for (const m of skuMap) {
    const key = m.seller_sku?.toLowerCase();
    if (!skuToNeMap.has(key)) skuToNeMap.set(key, []);
    skuToNeMap.get(key).push({ ne_code: m.ne_code?.toLowerCase(), qty: m.数量 || 1 });
  }

  // 5. Amazon手数料キャッシュ
  let feeMap = new Map();
  try {
    const fees = db.prepare('SELECT * FROM mirror_amazon_sku_fees').all();
    for (const f of fees) {
      feeMap.set(f.seller_sku?.toLowerCase(), f);
    }
  } catch { /* テーブルがまだない場合 */ }

  // 6. 粗利計算
  const results = [];

  for (const s of sales) {
    const mallId = s.モール;
    const listingCode = s.listing_code;
    const channel = s.チャネル || '';
    const revenue = s.売上金額 || 0;
    const qty = s.数量 || 0;

    if (revenue <= 0 || qty <= 0) continue;

    // 原価計算: 原価(税抜) × 税率 = 原価(税込)
    // 送料: Amazon FBA以外は送料を加算
    let costExTax = 0; // 原価(税抜)
    let taxRate = 1.1;  // デフォルト10%
    let shipping = 0;
    let costSource = '不明';
    let productName = listingCode;

    if (mallId === 'amazon') {
      // SKUマップでNE商品コードを取得 → 構成品原価合計
      const neEntries = skuToNeMap.get(listingCode);
      if (neEntries) {
        let totalCost = 0;
        let totalShip = 0;
        for (const entry of neEntries) {
          const prod = productMap.get(entry.ne_code);
          if (prod?.原価 != null) {
            totalCost += prod.原価 * entry.qty;
            taxRate = 1 + (prod.消費税率 || 10) / 100;
            if (prod.送料) totalShip += prod.送料 * entry.qty;
            if (!productName || productName === listingCode) productName = prod.商品名;
          }
        }
        costExTax = totalCost * qty;
        // FBMは商品マスタの送料、FBAは後でfba_feeを送料欄に入れる
        if (channel !== 'FBA') {
          shipping = totalShip * qty;
        }
        costSource = 'SKU→NE';
      }
    } else {
      // 非Amazon: listingCodeがNE商品コードのケースもある
      const prod = productMap.get(listingCode);
      if (prod?.原価 != null) {
        costExTax = prod.原価 * qty;
        taxRate = 1 + (prod.消費税率 || 10) / 100;
        shipping = (prod.送料 || 0) * qty;
        productName = prod.商品名 || listingCode;
        costSource = prod.原価ソース || 'NE';
      }
    }

    // 原価(税込) = 原価(税抜) × 税率
    const cost = costExTax * taxRate;

    // 手数料計算
    let platformFee = 0;
    let fbaFee = 0;

    if (mallId === 'amazon') {
      const feeData = feeMap.get(listingCode);
      if (feeData) {
        platformFee = (feeData.referral_fee || 0) * qty;
        // FBA: 配送代行手数料を送料欄に入れる
        if (channel === 'FBA') {
          shipping = (feeData.fba_fee || 0) * qty;
        }
      } else {
        platformFee = revenue * 0.15;
      }
    } else {
      const rate = MALL_FEE_RATES[mallId] || 0.10;
      platformFee = revenue * rate;
    }

    // 粗利 = 売価 - PF手数料 - 送料(FBA配送代行 or 自社送料) - 原価(税込)
    const grossProfit = revenue - platformFee - shipping - cost;
    const grossMarginRate = revenue > 0 ? (grossProfit / revenue * 100) : 0;

    results.push({
      listing_code: listingCode,
      product_name: productName,
      mall: mallId,
      channel,
      qty,
      revenue: Math.round(revenue),
      cost: Math.round(cost),
      shipping: Math.round(shipping),
      platform_fee: Math.round(platformFee),
      gross_profit: Math.round(grossProfit),
      margin_rate: Math.round(grossMarginRate * 10) / 10,
      cost_source: costSource,
      has_fee_cache: mallId === 'amazon' ? feeMap.has(listingCode) : null,
    });
  }

  return results;
}

/**
 * 前月比計算用: 指定期間の粗利を月単位で集計
 */
function calculateMonthlyProfit(db, { months = 3, mall = null } = {}) {
  const now = new Date();
  const results = {};

  for (let i = 0; i < months; i++) {
    const target = new Date(now);
    target.setMonth(target.getMonth() - i);
    const yearMonth = target.toISOString().slice(0, 7);

    // その月の初日〜末日
    const firstDay = `${yearMonth}-01`;
    const lastDay = new Date(target.getFullYear(), target.getMonth() + 1, 0).toISOString().slice(0, 10);

    let salesSql = `
      SELECT 商品コード as listing_code, モール, チャネル,
        SUM(数量) as 数量, SUM(売上金額) as 売上金額
      FROM mirror_sales_daily
      WHERE データ種別 = 'by_listing' AND 日付 >= ? AND 日付 <= ?
    `;
    const params = [firstDay, lastDay];
    if (mall) { salesSql += ' AND モール = ?'; params.push(mall); }
    salesSql += ' GROUP BY 商品コード, モール, チャネル HAVING SUM(数量) > 0';

    results[yearMonth] = db.prepare(salesSql).all(...params);
  }

  return results;
}

// ─── API エンドポイント ───

// 粗利ワースト / ボーダー帯 / モール別比較
router.get('/api/profit', (req, res) => {
  try {
    const db = getMirrorDB();
    const days = parseInt(req.query.days) || 30;
    const mall = req.query.mall || null;

    const data = calculateProfitData(db, { days, mall });

    // ソート: 粗利率昇順（ワースト順）
    data.sort((a, b) => a.margin_rate - b.margin_rate);

    // 集計サマリー
    const totalRevenue = data.reduce((s, d) => s + d.revenue, 0);
    const totalCost = data.reduce((s, d) => s + d.cost, 0);
    const totalShipping = data.reduce((s, d) => s + d.shipping, 0);
    const totalFee = data.reduce((s, d) => s + d.platform_fee, 0);
    const totalProfit = data.reduce((s, d) => s + d.gross_profit, 0);

    res.json({
      items: data,
      summary: {
        count: data.length,
        total_revenue: totalRevenue,
        total_cost: totalCost,
        total_shipping: totalShipping,
        total_fee: totalFee,
        total_profit: totalProfit,
        avg_margin_rate: totalRevenue > 0 ? Math.round(totalProfit / totalRevenue * 1000) / 10 : 0,
      },
      meta: { days, mall, generated_at: new Date().toISOString() },
    });
  } catch (e) {
    console.error('[ProfitAnalysis] Error:', e);
    res.status(500).json({ error: e.message });
  }
});

// 前月比悪化商品
router.get('/api/profit/trend', (req, res) => {
  try {
    const db = getMirrorDB();
    const mall = req.query.mall || null;

    // 当月 vs 前月 の日次データから粗利を比較
    const now = new Date();
    const thisMonth = now.toISOString().slice(0, 7);
    const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1).toISOString().slice(0, 7);

    const thisMonthData = calculateProfitData(db, { days: 30, mall });
    const lastMonthData = calculateProfitData(db, { days: 60, mall });

    // 先月分だけフィルタ（60日データから30日以上前のもの）
    const cutoff30 = new Date();
    cutoff30.setDate(cutoff30.getDate() - 30);

    // 商品×モールでグルーピング
    const thisMap = new Map();
    for (const d of thisMonthData) {
      const key = `${d.listing_code}__${d.mall}`;
      thisMap.set(key, d);
    }

    const lastMap = new Map();
    for (const d of lastMonthData) {
      const key = `${d.listing_code}__${d.mall}`;
      if (!thisMap.has(key)) {
        lastMap.set(key, d);
      }
    }

    // 悪化判定: 粗利額が前月比 -20% 以上減少
    const deteriorated = [];
    for (const [key, current] of thisMap) {
      const prev = lastMap.get(key);
      if (!prev) continue;
      if (prev.gross_profit <= 0) continue;

      const change = (current.gross_profit - prev.gross_profit) / Math.abs(prev.gross_profit) * 100;
      if (change <= -20) {
        deteriorated.push({
          ...current,
          prev_profit: prev.gross_profit,
          prev_margin_rate: prev.margin_rate,
          profit_change_pct: Math.round(change * 10) / 10,
        });
      }
    }

    deteriorated.sort((a, b) => a.profit_change_pct - b.profit_change_pct);

    res.json({
      items: deteriorated,
      meta: { this_month: thisMonth, last_month: lastMonth, mall },
    });
  } catch (e) {
    console.error('[ProfitAnalysis] Trend Error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Amazon手数料キャッシュ状態
router.get('/api/fee-status', (req, res) => {
  try {
    const db = getMirrorDB();
    let total = 0, fba = 0, fbm = 0, oldest = null;
    try {
      total = db.prepare('SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees').get().cnt;
      fba = db.prepare("SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees WHERE fulfillment_channel = 'FBA'").get().cnt;
      fbm = db.prepare("SELECT COUNT(*) as cnt FROM mirror_amazon_sku_fees WHERE fulfillment_channel = 'FBM'").get().cnt;
      oldest = db.prepare('SELECT MIN(fetched_at) as oldest FROM mirror_amazon_sku_fees').get().oldest;
    } catch { /* テーブル未作成 */ }

    res.json({ total, fba, fbm, oldest_fetch: oldest });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
