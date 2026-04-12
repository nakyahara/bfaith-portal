/**
 * FBA収益性分析ツール — ルーター
 *
 * 全FBA在庫の利益率を一覧表示し、低利益率商品を炙り出す
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getActiveListingsReport, getFees } from '../profit-calculator/sp-api.js';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();

// ===== メイン画面 =====
router.get('/', (req, res) => {
  res.render('fba-profitability', {
    title: 'FBA収益性分析',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ===== API: FBA出品一覧取得 + 原価突合 =====
router.post('/api/listings', async (req, res) => {
  try {
    console.log('[FBA-Profit] 出品レポート取得開始...');
    const report = await getActiveListingsReport();
    console.log(`[FBA-Profit] 全出品: ${report.totalCount}件`);

    // FBA出品のみフィルタ（日本語ヘッダー「フルフィルメント・チャンネル」にも対応）
    const fbaListings = report.listings.filter(r => {
      const fc = (
        r['fulfillment-channel'] || r['fulfillment channel'] ||
        r['フルフィルメント・チャンネル'] || r['フルフィルメントチャンネル'] || ''
      ).toLowerCase();
      return fc.includes('amazon') || fc === 'afn' || fc.includes('fba') ||
             fc.includes('default') || fc === 'amazon_na' || fc === 'amazon_jp';
    });
    console.log(`[FBA-Profit] FBA出品: ${fbaListings.length}件`);

    // warehouse-mirror.db から原価データ取得
    let costMap = new Map();
    try {
      const db = getMirrorDB();

      // mirror_sku_map: seller_sku → ne_code
      const skuMappings = db.prepare('SELECT seller_sku, ne_code, 数量 FROM mirror_sku_map').all();
      const skuToNe = new Map();
      for (const m of skuMappings) {
        if (!skuToNe.has(m.seller_sku?.toLowerCase())) {
          skuToNe.set(m.seller_sku?.toLowerCase(), []);
        }
        skuToNe.get(m.seller_sku?.toLowerCase()).push({ ne_code: m.ne_code, qty: m.数量 || 1 });
      }

      // mirror_products: 商品コード → 原価, 消費税率
      const products = db.prepare('SELECT 商品コード, 原価, 原価ソース, 原価状態, 消費税率 FROM mirror_products').all();
      const productMap = new Map();
      for (const p of products) {
        productMap.set(p.商品コード?.toLowerCase(), p);
      }

      // FBA SKU ごとに原価を計算
      for (const listing of fbaListings) {
        const sku = (listing['seller-sku'] || listing['seller sku'] || listing['出品者SKU'] || listing['sku'] || '').trim();
        if (!sku) continue;

        const neEntries = skuToNe.get(sku.toLowerCase());
        if (neEntries && neEntries.length > 0) {
          // セット商品: 構成品の原価 × 数量の合計
          let totalCost = 0;
          let taxRate = 10;
          let allFound = true;
          let costSource = '';

          for (const entry of neEntries) {
            const prod = productMap.get(entry.ne_code?.toLowerCase());
            if (prod && prod.原価 != null) {
              totalCost += prod.原価 * entry.qty;
              taxRate = prod.消費税率 ?? 10;
              costSource = prod.原価ソース || '';
            } else {
              allFound = false;
            }
          }

          if (allFound && totalCost > 0) {
            costMap.set(sku.toLowerCase(), {
              cost: totalCost,
              taxRate,
              costSource,
              neCode: neEntries.map(e => e.ne_code).join(', '),
            });
          }
        } else {
          // SKU = NE商品コードの場合もある
          const prod = productMap.get(sku.toLowerCase());
          if (prod && prod.原価 != null) {
            costMap.set(sku.toLowerCase(), {
              cost: prod.原価,
              taxRate: prod.消費税率 ?? 10,
              costSource: prod.原価ソース || '',
              neCode: sku,
            });
          }
        }
      }
      console.log(`[FBA-Profit] 原価マッチ: ${costMap.size}/${fbaListings.length}件`);
    } catch (e) {
      console.error('[FBA-Profit] warehouse.db アクセスエラー:', e.message);
    }

    // レスポンス組み立て（日本語ヘッダー対応）
    const items = fbaListings.map(listing => {
      const sku = (listing['seller-sku'] || listing['seller sku'] || listing['出品者SKU'] || listing['sku'] || '').trim();
      const asin = (listing['asin1'] || listing['asin'] || listing['商品ID'] || '').trim();
      const price = parseFloat(listing['price'] || listing['your-price'] || listing['価格'] || '0') || 0;
      const productName = listing['item-name'] || listing['item name'] || listing['product-name'] || listing['商品名'] || '';
      const quantity = parseInt(listing['quantity'] || listing['afn-fulfillable-quantity'] || listing['在庫数'] || listing['数量'] || '0') || 0;

      const costData = costMap.get(sku.toLowerCase());

      return {
        sku,
        asin,
        productName,
        price,
        quantity,
        cost: costData?.cost ?? null,
        taxRate: costData?.taxRate ?? null,
        costSource: costData?.costSource ?? null,
        neCode: costData?.neCode ?? null,
      };
    }).filter(item => item.asin); // ASINなしは除外

    res.json({ success: true, items, total: items.length });
  } catch (e) {
    console.error('[FBA-Profit] listings取得エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== API: 手数料バッチ取得 =====
// フロントエンドから少しずつ呼ぶ（SP-APIレート制限対策）
router.post('/api/fees', async (req, res) => {
  const { items } = req.body; // [{ asin, price }]
  if (!items || !Array.isArray(items)) {
    return res.status(400).json({ error: 'items配列が必要です' });
  }

  const results = [];
  for (const item of items) {
    try {
      const fees = await getFees(item.asin, item.price, true);
      results.push({
        asin: item.asin,
        success: true,
        ...fees,
      });
    } catch (e) {
      console.error(`[FBA-Profit] 手数料取得エラー (${item.asin}):`, e.message);
      results.push({
        asin: item.asin,
        success: false,
        error: e.message,
      });
    }
    // レート制限対策: 1リクエストごとに少し待つ
    if (items.length > 1) {
      await new Promise(r => setTimeout(r, 200));
    }
  }

  res.json({ results });
});

// ===== API: 原価手動更新 =====
router.post('/api/update-cost', (req, res) => {
  const { sku, cost, taxRate } = req.body;
  if (!sku || cost === undefined) {
    return res.status(400).json({ error: 'sku と cost は必須です' });
  }

  try {
    const db = getMirrorDB();
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

    // mirror_products に即時反映
    db.prepare(
      "UPDATE mirror_products SET 原価 = ?, 原価ソース = '例外', 原価状態 = 'OVERRIDDEN', updated_at = ? WHERE 商品コード = ?"
    ).run(parseFloat(cost), now, sku);

    res.json({ ok: true, sku, cost: parseFloat(cost) });
  } catch (e) {
    console.error('[FBA-Profit] 原価更新エラー:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ===== デバッグ: ヘッダー確認用 =====
router.get('/api/debug-headers', async (req, res) => {
  try {
    const report = await getActiveListingsReport();
    const sample = report.listings[0] || {};
    res.json({
      totalCount: report.totalCount,
      headers: report.headers,
      sampleKeys: Object.keys(sample).slice(0, 30),
      sampleValues: Object.fromEntries(Object.entries(sample).slice(0, 20)),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
