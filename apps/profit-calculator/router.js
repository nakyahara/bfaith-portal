/**
 * FBA仕入れ利益計算ツール — Express Router
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getProduct, getFees, createListing, patchListing, getShippingTemplates, getItemOffers, updatePrice, getActiveListingsReport, getSalesCountBySku, searchByJan, searchByKeyword, estimateMonthlySales } from './sp-api.js';
import { initDb, saveResearch, getResearch, getResearchById, updateResearchStatus, updateResearch, promoteToProduct, saveProduct, getProducts, getProductById, updateProductStatus, updateProduct, deleteProduct, getSetItems, saveSetItems, syncListings as dbSyncListings, getListings, updateListing, bulkSave, getSyncMeta, getTrackingProducts, getPriceHistory, getRecentPriceHistory, savePriceHistory, updateProductPriceInfo, syncProductsFromListings } from './db.js';
import { loadSuppliers, addSupplier, deleteSupplier } from './suppliers.js';
import { loadShipping, addShipping, updateShipping, deleteShipping } from './shipping.js';
import { getSetting, setSetting, getAllSettings } from './settings.js';
import { startPriceWorker, stopPriceWorker, getWorkerStatus, refreshProductCache } from './price-scheduler.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// DB初期化
let dbReady = false;
async function ensureDb() {
  if (!dbReady) {
    await initDb();
    dbReady = true;
  }
}

// ── ページ配信 ──
router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

router.get('/research', (req, res) => {
  res.sendFile(path.join(__dirname, 'list.html'));
});

router.get('/products', (req, res) => {
  res.sendFile(path.join(__dirname, 'products.html'));
});

router.get('/amazon', (req, res) => {
  res.sendFile(path.join(__dirname, 'amazon.html'));
});

router.get('/amazon-manual', (req, res) => {
  res.sendFile(path.join(__dirname, 'amazon-manual.html'));
});

router.get('/price-revision', (req, res) => {
  res.sendFile(path.join(__dirname, 'price-revision.html'));
});

router.get('/suppliers', (req, res) => {
  res.sendFile(path.join(__dirname, 'suppliers.html'));
});

router.get('/shipping', (req, res) => {
  res.sendFile(path.join(__dirname, 'shipping.html'));
});

router.get('/settings', (req, res) => {
  res.sendFile(path.join(__dirname, 'settings.html'));
});

router.get('/price-revision', (req, res) => {
  res.sendFile(path.join(__dirname, 'price-revision.html'));
});

// 旧URL互換
router.get('/list', (req, res) => {
  res.redirect('./research');
});

// ── API: 商品情報取得 ──
router.get('/api/product/:asin', async (req, res) => {
  try {
    const data = await getProduct(req.params.asin);
    res.json(data);
  } catch (err) {
    console.error('[ProfitCalc] 商品情報エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: 手数料取得 ──
router.post('/api/fees', async (req, res) => {
  try {
    const { asin, price, isFba } = req.body;
    if (!asin || !price) return res.status(400).json({ error: 'ASINと価格は必須です' });
    const fees = await getFees(asin, price, isFba !== false);
    res.json(fees);
  } catch (err) {
    console.error('[ProfitCalc] 手数料エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: リサーチDB ──
router.post('/api/research', async (req, res) => {
  try {
    await ensureDb();
    const id = saveResearch(req.body);
    res.json({ id });
  } catch (err) {
    console.error('[ProfitCalc] リサーチ保存エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/research', async (req, res) => {
  try {
    await ensureDb();
    const { status, search } = req.query;
    const data = getResearch({ status, search });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/research/:id', async (req, res) => {
  try {
    await ensureDb();
    const data = getResearchById(parseInt(req.params.id));
    if (!data) return res.status(404).json({ error: '見つかりません' });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.put('/api/research/:id', async (req, res) => {
  try {
    await ensureDb();
    const id = parseInt(req.params.id);
    const body = req.body;
    // statusのみの場合は従来通り
    if (Object.keys(body).length === 1 && body.status) {
      updateResearchStatus(id, body.status);
    } else {
      updateResearch(id, body);
    }
    const updated = getResearchById(id);
    res.json(updated || { ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API: 商品登録DB ──
router.post('/api/research/:id/promote', async (req, res) => {
  try {
    await ensureDb();
    const productId = promoteToProduct(parseInt(req.params.id));
    res.json({ productId });
  } catch (err) {
    console.error('[ProfitCalc] 転送エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// 商品を直接保存（商品計算ページから）
router.post('/api/products', async (req, res) => {
  try {
    await ensureDb();
    const id = saveProduct(req.body);
    res.json({ id });
  } catch (err) {
    console.error('[ProfitCalc] 商品保存エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/products', async (req, res) => {
  try {
    await ensureDb();
    const { status, search } = req.query;
    const data = getProducts({ status, search });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/products/:id', async (req, res) => {
  try {
    await ensureDb();
    const product = getProductById(parseInt(req.params.id));
    if (!product) return res.status(404).json({ error: '見つかりません' });
    product.set_items = getSetItems(product.id);
    res.json(product);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.put('/api/products/:id', async (req, res) => {
  try {
    await ensureDb();
    const id = parseInt(req.params.id);
    const { set_items, ...fields } = req.body;
    updateProduct(id, fields);
    if (set_items && Array.isArray(set_items)) {
      saveSetItems(id, set_items);
    }
    const updated = getProductById(id);
    if (updated) updated.set_items = getSetItems(id);
    res.json(updated || { ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API: Amazon出品登録 ──
router.post('/api/products/:id/list-amazon', async (req, res) => {
  const id = parseInt(req.params.id);
  try {
    await ensureDb();
    const product = getProductById(id);
    if (!product) return res.status(404).json({ error: '商品が見つかりません' });

    const isFba = product.fulfillment === 'FBA';
    const price = product.selling_price || product.ne_selling_price;
    if (!price) return res.status(400).json({ error: '販売価格が設定されていません' });
    if (!product.asin) return res.status(400).json({ error: 'ASINが設定されていません' });

    const sku = isFba ? null : (product.ne_product_code || null);
    if (!isFba && !sku) return res.status(400).json({ error: 'NE商品コード（SKU）が設定されていません' });

    // 保存済み設定を取得（FBM/FBAで別設定）
    const shippingTemplate = !isFba ? getSetting('amazon_shipping_template') : null;
    const paymentRestriction = isFba
      ? (getSetting('amazon_fba_payment_restriction') || 'none')
      : (getSetting('amazon_payment_restriction') || 'none');

    const result = await createListing({
      asin: product.asin,
      price,
      isFba,
      sku,
      shippingTemplate,
      paymentRestriction,
    });

    // 成功時にステータスを「Amazon出品済」に更新
    if (result.status === 'ACCEPTED') {
      updateProduct(id, { status: 'Amazon出品済' });

      // 在庫一覧（listings）テーブルに自動登録 — ASIN/SKUで連携
      try {
        const { upsertListing } = await import('./db.js');
        upsertListing({
          sku: result.sku,
          asin: product.asin,
          product_name: product.product_name || '',
          image_url: product.image_url || '',
          price: price,
          quantity: 0,
          status: 'Active',
          condition: '新品',
          fulfillment: isFba ? 'FBA' : 'FBM',
          open_date: new Date().toISOString().slice(0, 10),
          listing_id: '',
          item_description: '',
          // productsテーブルから仕入価格・ストッパー・価格追従を引き継ぎ
          cost_price: product.wholesale_price_with_tax || 0,
          loss_stopper: product.loss_stopper || 0,
          high_stopper: product.high_stopper || 0,
          price_tracking: product.price_tracking || 'しない',
        });
        console.log(`[ProfitCalc] listings連携: SKU=${result.sku}, 仕入=${product.wholesale_price_with_tax}, 追従=${product.price_tracking}`);
      } catch (linkErr) {
        console.error('[ProfitCalc] listings連携エラー:', linkErr.message);
      }

      // LISTING_OFFER_ONLYでは支払い制限が適用されないため、patchで後追い設定（FBA/FBM共通）
      if (paymentRestriction && paymentRestriction !== 'none') {
        try {
          const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
          const exclusions = [];
          if (paymentRestriction === 'cvs' || paymentRestriction === 'cod_cvs') {
            exclusions.push({ value: 'cvs', marketplace_id: marketplaceId });
          }
          if (paymentRestriction === 'cod' || paymentRestriction === 'cod_cvs') {
            exclusions.push({ value: 'cash_on_delivery', marketplace_id: marketplaceId });
          }
          if (exclusions.length > 0) {
            const patches = [{ op: 'replace', path: '/attributes/optional_payment_type_exclusion', value: exclusions }];
            const patchResult = await patchListing({ sku: result.sku, patches });
            result.paymentPatch = patchResult;
            console.log(`[SP-API] 支払い制限patch: SKU=${result.sku}, status=${patchResult.status}`);
          }
        } catch (patchErr) {
          console.error('[SP-API] 支払い制限patch失敗:', patchErr.message);
          result.paymentPatchError = patchErr.message;
        }
      }

      res.json(result);
    } else {
      // ACCEPTED以外（INVALID等）はエラー扱い
      updateProduct(id, { status: 'Amazon出品エラー' });
      const issueMsg = (result.issues || []).map(i => i.message || i.code || JSON.stringify(i)).join('; ');
      res.status(422).json({ error: `出品ステータス: ${result.status}${issueMsg ? ' — ' + issueMsg : ''}`, ...result });
    }
  } catch (err) {
    console.error('[ProfitCalc] Amazon出品エラー:', err.message);
    // API呼び出し自体が失敗した場合もエラーステータスに
    try { updateProduct(id, { status: 'Amazon出品エラー' }); } catch (_) {}
    res.status(500).json({ error: err.message });
  }
});

// ── API: 既存出品の支払い制限を修正（patchListingsItem） ──
router.post('/api/products/:id/patch-amazon', async (req, res) => {
  try {
    await ensureDb();
    const id = parseInt(req.params.id);
    const product = getProductById(id);
    if (!product) return res.status(404).json({ error: '商品が見つかりません' });

    const isFba = product.fulfillment === 'FBA';
    const sku = isFba ? null : (product.ne_product_code || null);
    if (!sku) return res.status(400).json({ error: 'SKUが特定できません' });

    const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
    const patches = [
      {
        op: 'replace',
        path: '/attributes/optional_payment_type_exclusion',
        value: [
          { value: 'cvs', marketplace_id: marketplaceId },
          { value: 'cash_on_delivery', marketplace_id: marketplaceId },
        ],
      },
    ];

    const result = await patchListing({ sku, patches });
    res.json(result);
  } catch (err) {
    console.error('[ProfitCalc] Amazon出品パッチエラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: SKU直接指定で支払い制限を修正 ──
router.post('/api/amazon/patch-payment', async (req, res) => {
  try {
    const { sku } = req.body;
    if (!sku) return res.status(400).json({ error: 'SKUが必要です' });

    const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
    const patches = [
      {
        op: 'replace',
        path: '/attributes/optional_payment_type_exclusion',
        value: [
          { value: 'cvs', marketplace_id: marketplaceId },
          { value: 'cash_on_delivery', marketplace_id: marketplaceId },
        ],
      },
    ];

    const result = await patchListing({ sku, patches });
    res.json(result);
  } catch (err) {
    console.error('[ProfitCalc] Amazon支払い制限パッチエラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: Amazon手動出品（DBを経由しない直接出品） ──
router.post('/api/amazon/manual-list', async (req, res) => {
  try {
    const { asin, price, sku, condition, isFba } = req.body;
    if (!asin) return res.status(400).json({ error: 'ASINが必要です' });
    if (!price || price <= 0) return res.status(400).json({ error: '出品価格が必要です' });
    if (!isFba && !sku) return res.status(400).json({ error: '自社出荷の場合はSKUが必要です' });

    // 設定から配送テンプレート・支払い制限を取得（Amazon出品と共通設定）
    const shippingTemplate = !isFba ? getSetting('amazon_shipping_template') : null;
    const paymentRestriction = isFba
      ? (getSetting('amazon_fba_payment_restriction') || 'none')
      : (getSetting('amazon_payment_restriction') || 'none');

    const result = await createListing({
      asin,
      price: parseInt(price),
      isFba: !!isFba,
      sku: sku || null,
      condition: condition || 'new_new',
      shippingTemplate,
      paymentRestriction,
    });

    // 出品成功時に支払い制限をpatchで後追い設定
    if (result.status === 'ACCEPTED' && paymentRestriction && paymentRestriction !== 'none') {
      try {
        const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
        const exclusions = [];
        if (paymentRestriction === 'cvs' || paymentRestriction === 'cod_cvs') {
          exclusions.push({ value: 'cvs', marketplace_id: marketplaceId });
        }
        if (paymentRestriction === 'cod' || paymentRestriction === 'cod_cvs') {
          exclusions.push({ value: 'cash_on_delivery', marketplace_id: marketplaceId });
        }
        if (exclusions.length > 0) {
          const patches = [{ op: 'replace', path: '/attributes/optional_payment_type_exclusion', value: exclusions }];
          const patchResult = await patchListing({ sku: result.sku, patches });
          result.paymentPatch = patchResult;
          console.log(`[SP-API] 手動出品 支払い制限patch: SKU=${result.sku}, status=${patchResult.status}`);
        }
      } catch (patchErr) {
        console.error('[SP-API] 手動出品 支払い制限patch失敗:', patchErr.message);
        result.paymentPatchError = patchErr.message;
      }
    }

    if (result.status === 'ACCEPTED') {
      res.json(result);
    } else {
      const issueMsg = (result.issues || []).map(i => i.message || i.code || JSON.stringify(i)).join('; ');
      res.status(422).json({ error: `出品ステータス: ${result.status}${issueMsg ? ' — ' + issueMsg : ''}`, ...result });
    }
  } catch (err) {
    console.error('[ProfitCalc] Amazon手動出品エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.delete('/api/products/:id', async (req, res) => {
  try {
    await ensureDb();
    deleteProduct(parseInt(req.params.id));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── API: NE在庫連携用CSVエクスポート（Shift_JIS） ──
router.get('/api/products/csv/ne-stock', async (req, res) => {
  try {
    await ensureDb();
    const ids = (req.query.ids || '').split(',').map(Number).filter(n => !isNaN(n));
    if (ids.length === 0) return res.status(400).json({ error: '対象商品が選択されていません' });

    let products = getProducts({});
    products = products.filter(p => ids.includes(p.id));

    // バリデーション: FBM + Amazon出品済のみ
    const invalid = products.filter(p => p.fulfillment === 'FBA' || p.status !== 'Amazon出品済');
    if (invalid.length === products.length) {
      return res.status(400).json({ error: '対象の商品がありません（FBMかつAmazon出品済が必要です）' });
    }
    const valid = products.filter(p => p.fulfillment !== 'FBA' && p.status === 'Amazon出品済');

    // NE汎用モール商品CSV: 商品コード,商品名,売価
    const header = '商品コード,商品名,売価';
    const csvQuote = (c) => { const s = String(c ?? ''); return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s; };
    const rows = valid.map(p => [
      csvQuote(p.ne_product_code || ''),
      csvQuote(p.product_name || ''),
      csvQuote(p.selling_price || p.ne_selling_price || ''),
    ].join(','));

    const csvUtf8 = header + '\r\n' + rows.join('\r\n') + '\r\n';

    // Shift_JISに変換
    const encoder = new TextEncoder();
    // Shift_JIS変換テーブルがないため、iconv-liteがあれば使う、なければBOM付きUTF-8
    let csvBuffer;
    try {
      const iconv = await import('iconv-lite');
      csvBuffer = iconv.default.encode(csvUtf8, 'Shift_JIS');
    } catch {
      // iconv-liteがなければBOM付きUTF-8
      csvBuffer = Buffer.from('\uFEFF' + csvUtf8, 'utf-8');
    }

    const date = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename="hanyo-mallshouhin_${date}.csv"`);
    res.send(csvBuffer);
  } catch (err) {
    console.error('[ProfitCalc] NE在庫連携CSV:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: NE用CSVエクスポート ──
router.get('/api/products/csv/ne', async (req, res) => {
  try {
    await ensureDb();
    let products = getProducts({ status: req.query.status });
    // チェックボックス選択対応
    if (req.query.ids) {
      const ids = req.query.ids.split(',').map(Number).filter(n => !isNaN(n));
      products = products.filter(p => ids.includes(p.id));
    }
    const type = req.query.type || 'single';
    const date = new Date().toISOString().slice(0, 10);
    let csv, filename;

    if (type === 'set') {
      // セット商品CSV: ne_registration_type = '複数' のみ、内訳テーブルからJOIN
      const setProducts = products.filter(p => p.ne_registration_type === '複数');
      const header = ['set_syohin_code', 'set_syohin_name', 'set_baika_tnk', 'syohin_code', 'suryo', 'daihyo_syohin_code'];
      const rows = [];
      for (const p of setProducts) {
        const items = getSetItems(p.id);
        if (items.length === 0) {
          // 内訳なしでも1行出す
          rows.push([p.ne_product_code||'', p.product_name||'', p.ne_selling_price||'', '', '', p.ne_representative_code||'']);
        } else {
          for (const item of items) {
            rows.push([
              p.ne_product_code || '',
              p.product_name || '',
              p.ne_selling_price || '',
              item.syohin_code || '',
              item.suryo || '',
              p.ne_representative_code || '',
            ]);
          }
        }
      }
      const csvQuote = (c) => { const s = String(c); return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s; };
      csv = header.join(',') + '\n' + rows.map(r => r.map(csvQuote).join(',')).join('\n');
      filename = `set_syohin_ikkatsu_${date}.csv`;
    } else {
      // 単品CSV: ne_registration_type = '単品' のみ
      const singleProducts = products.filter(p => !p.ne_registration_type || p.ne_registration_type === '単品');
      const header = ['syohin_code', 'syohin_name', 'sire_code', 'genka_tnk', 'baika_tnk', 'daihyo_syohin_code'];
      const rows = singleProducts.map(p => [
        p.ne_product_code || '',
        p.product_name || '',
        p.ne_supplier_code || p.supplier_code || '',
        p.ne_cost_excl_tax || p.wholesale_price || '',
        p.ne_selling_price || '',
        p.ne_representative_code || '',
      ]);
      const csvQuote = (c) => { const s = String(c); return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s; };
      csv = header.join(',') + '\n' + rows.map(r => r.map(csvQuote).join(',')).join('\n');
      filename = `syohin_ikkatsu_${date}.csv`;
    }

    // 必須項目バリデーション
    const errors = [];
    const targetProducts = type === 'set'
      ? products.filter(p => p.ne_registration_type === '複数')
      : products.filter(p => !p.ne_registration_type || p.ne_registration_type === '単品');

    for (const p of targetProducts) {
      const missing = [];
      if (!p.ne_product_code) missing.push('商品コード(ne_product_code)');
      if (!p.product_name) missing.push('商品名(product_name)');
      if (type === 'set') {
        if (!p.ne_selling_price) missing.push('セット販売価格(ne_selling_price)');
        const items = getSetItems(p.id);
        if (items.length === 0) missing.push('内訳商品(set_items)');
      } else {
        if (!p.ne_supplier_code && !p.supplier_code) missing.push('仕入先コード(sire_code)');
        if (!p.ne_cost_excl_tax && !p.wholesale_price) missing.push('原価(genka_tnk)');
        if (!p.ne_selling_price) missing.push('売価(baika_tnk)');
      }
      if (missing.length > 0) {
        errors.push({ id: p.id, name: p.product_name || p.ne_product_code || `ID:${p.id}`, missing });
      }
    }

    if (errors.length > 0) {
      return res.status(400).json({
        error: '必須項目が不足しています',
        details: errors,
      });
    }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API: 仕入れ先マスタ ──
router.get('/api/suppliers', (req, res) => {
  res.json(loadSuppliers());
});

router.post('/api/suppliers', (req, res) => {
  try {
    const { code, name } = req.body;
    if (!code || !name) return res.status(400).json({ error: 'コードと名前は必須です' });
    const suppliers = addSupplier({ code, name });
    res.json(suppliers);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/api/suppliers', (req, res) => {
  try {
    const { code } = req.body;
    if (!code) return res.status(400).json({ error: 'コードは必須です' });
    const suppliers = deleteSupplier(code);
    res.json(suppliers);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API: 日→英変換（商品コード用） ──
router.get('/api/translate', async (req, res) => {
  const word = req.query.word;
  if (!word) return res.json({ english: '' });

  // Google Translate の非公式API（無料・軽量）
  try {
    const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=ja&tl=en&dt=t&q=${encodeURIComponent(word)}`;
    const resp = await fetch(url);
    const data = await resp.json();
    const translated = data?.[0]?.[0]?.[0] || '';
    // 英語に変換してケバブケースに
    const english = translated.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
    res.json({ english, raw: translated });
  } catch (err) {
    console.error('[Translate]', err.message);
    res.json({ english: '', error: err.message });
  }
});

// ── ページ: 一括リサーチ ──
router.get('/bulk-research', (req, res) => {
  res.sendFile(path.join(__dirname, 'bulk-research.html'));
});

// ── API: 一括リサーチ（SSE ストリーミング） ──
router.post('/api/bulk-research/stream', async (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });

  const send = (event, data) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  const { items, taxRate = 10 } = req.body;
  if (!items || !Array.isArray(items) || items.length === 0) {
    send('error', { message: 'アイテムが指定されていません' });
    res.end();
    return;
  }

  send('start', { total: items.length });

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const { jan, productName, wholesalePrice } = item;
    const idx = i + 1;

    try {
      send('progress', { current: idx, total: items.length, jan, productName });

      // Step 1: JANコードでASIN検索（JANがなければキーワード検索）
      let candidates = [];
      if (jan) {
        candidates = await searchByJan(jan);
      }
      if (candidates.length === 0 && productName) {
        candidates = await searchByKeyword(productName);
      }

      if (candidates.length === 0) {
        send('result', {
          idx, jan, productName, wholesalePrice,
          status: 'not_found', message: 'Amazon商品が見つかりません',
        });
        continue;
      }

      // Step 2: 最初の候補のASINで詳細取得
      const asin = candidates[0].asin;

      // 少し待機（レート制限対策）
      await new Promise(r => setTimeout(r, 300));

      const product = await getProduct(asin);

      // Step 3: 手数料取得
      const sellingPrice = product.currentPrice;
      if (!sellingPrice || sellingPrice <= 0) {
        send('result', {
          idx, jan, productName: productName || product.itemName,
          wholesalePrice, asin,
          image: product.image,
          amazonName: product.itemName,
          status: 'no_price', message: '販売価格が取得できません',
        });
        await new Promise(r => setTimeout(r, 300));
        continue;
      }

      await new Promise(r => setTimeout(r, 300));
      const fees = await getFees(asin, sellingPrice, true);

      // Step 4: 利益計算
      const rate = taxRate / 100;
      const wholesalePriceWithTax = Math.round(wholesalePrice * (1 + rate));
      const profit = sellingPrice - wholesalePriceWithTax - fees.totalFee;
      const profitRate = sellingPrice > 0 ? (profit / sellingPrice * 100) : 0;

      let judgment = '×';
      if (profitRate >= 30) judgment = '◎';
      else if (profitRate >= 20) judgment = '○';
      else if (profitRate >= 10) judgment = '△';

      // Step 5: 月間販売数推定
      const estSales = estimateMonthlySales(product.salesRank);

      send('result', {
        idx, jan, productName: productName || product.itemName,
        wholesalePrice, wholesalePriceWithTax,
        asin,
        image: product.image,
        amazonName: product.itemName,
        category: product.category,
        sellingPrice,
        salesRank: product.salesRank,
        offerCount: product.offerCount,
        referralFee: fees.referralFee,
        fbaFee: fees.fbaFee,
        totalFee: fees.totalFee,
        profit,
        profitRate: Math.round(profitRate * 10) / 10,
        judgment,
        estMonthlySales: estSales,
        manufacturer: product.manufacturer,
        status: 'ok',
      });

      // レート制限対策: 各アイテム間で待機
      await new Promise(r => setTimeout(r, 500));

    } catch (err) {
      console.error(`[BulkResearch] エラー (${jan || productName}):`, err.message);
      send('result', {
        idx, jan, productName, wholesalePrice,
        status: 'error', message: err.message,
      });
      // エラー後も少し待機
      await new Promise(r => setTimeout(r, 1000));
    }
  }

  send('complete', { total: items.length });
  res.end();
});

// ── API: 送料テーブル ──
router.get('/api/shipping', (req, res) => {
  res.json(loadShipping());
});

router.post('/api/shipping', (req, res) => {
  try {
    const data = addShipping(req.body);
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.put('/api/shipping', (req, res) => {
  try {
    const { originalName, ...item } = req.body;
    const data = updateShipping(originalName, item);
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/api/shipping', (req, res) => {
  try {
    const { name } = req.body;
    const data = deleteShipping(name);
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── API: アプリ設定 ──
router.get('/api/settings', (req, res) => {
  res.json(getAllSettings());
});

router.get('/api/settings/:key', (req, res) => {
  res.json({ key: req.params.key, value: getSetting(req.params.key) });
});

router.put('/api/settings/:key', (req, res) => {
  try {
    setSetting(req.params.key, req.body.value);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── API: Amazon配送テンプレート取得 ──
router.get('/api/amazon/shipping-templates', async (req, res) => {
  try {
    const templates = await getShippingTemplates();
    res.json(templates);
  } catch (err) {
    console.error('[ProfitCalc] 配送テンプレート取得エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: 支払い制限の有効値を取得（診断用） ──
router.get('/api/amazon/payment-options', async (req, res) => {
  try {
    const { getShippingTemplates: _unused, ...rest } = {};
    // スキーマを直接取得
    const sp = (await import('./sp-api.js'));
    // getShippingTemplatesと同じスキーマを使う
    const SellingPartner = (await import('amazon-sp-api')).default;
    const client = new SellingPartner({
      region: 'fe',
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
    const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
    const sellerId = process.env.SP_API_SELLER_ID || 'A6HMLHKUUJC27';

    const result = await client.callAPI({
      operation: 'getDefinitionsProductType',
      endpoint: 'productTypeDefinitions',
      path: { productType: 'PRODUCT' },
      query: { marketplaceIds: [marketplaceId], sellerId, requirements: 'LISTING', locale: 'ja_JP' },
      options: { version: '2020-09-01' },
    });

    const schemaUrl = result.schema?.link?.resource;
    if (!schemaUrl) return res.status(500).json({ error: 'スキーマURL取得失敗' });

    const schemaRes = await fetch(schemaUrl);
    const schema = await schemaRes.json();

    const prop = schema.properties?.optional_payment_type_exclusion;
    if (!prop) {
      // 近い名前のプロパティを探す
      const paymentKeys = Object.keys(schema.properties || {}).filter(k => k.includes('payment') || k.includes('exclusion') || k.includes('cod') || k.includes('cvs'));
      return res.json({ found: false, paymentRelatedKeys: paymentKeys });
    }

    const valueProp = prop.items?.properties?.value || prop.properties?.value || {};
    res.json({
      found: true,
      attribute: 'optional_payment_type_exclusion',
      enum: valueProp.enum || [],
      enumNames: valueProp.enumNames || [],
      rawProp: prop,
    });
  } catch (err) {
    console.error('[ProfitCalc] 支払いオプション取得エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: 在庫一覧（価格改定ツール） ──
router.get('/api/listings', async (req, res) => {
  try {
    await ensureDb();
    const listings = getListings();
    const lastSync = getSyncMeta('listings_last_sync');
    res.json({ listings, lastSync: lastSync ? new Date(lastSync).toLocaleString('ja-JP') : null });
  } catch (err) {
    console.error('[ProfitCalc] listings取得エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: Amazon同期（出品レポート取得） ──
router.post('/api/listings/sync', async (req, res) => {
  try {
    await ensureDb();
    console.log('[ProfitCalc] Amazon出品レポート同期開始...');
    const report = await getActiveListingsReport();
    console.log('[ProfitCalc] レポートヘッダー:', report.headers.join(', '));
    console.log('[ProfitCalc] レポート行数:', report.totalCount);

    // サンプル1行目のキーと値をログ出力（デバッグ用）
    if (report.listings.length > 0) {
      const sample = report.listings[0];
      console.log('[ProfitCalc] サンプル行キー:', Object.keys(sample).join(', '));
      console.log('[ProfitCalc] サンプル行値:', JSON.stringify(sample).slice(0, 500));
    }

    // TSVヘッダーキー → listingsテーブルのキーにマッピング
    // ヘッダーは英語の場合もShift_JIS日本語の場合もあるので、
    // 各行のキーを柔軟にマッチさせる
    const mapped = report.listings.map(row => {
      // ヘッダー名候補を試してマッチさせるヘルパー
      const get = (...keys) => {
        for (const k of keys) {
          if (row[k] !== undefined && row[k] !== '') return row[k];
        }
        return '';
      };

      return {
        sku: get('seller-sku', 'Seller SKU', 'seller_sku', '出品者SKU'),
        asin: get('asin1', 'ASIN1', 'asin', 'ASIN'),
        product_name: get('item-name', 'Item Name', 'item_name', '商品名'),
        image_url: get('image-url', 'Image URL', 'image_url', '画像URL'),
        price: parseFloat(get('price', 'Price', '価格')) || 0,
        shipping_price: parseFloat(get('expedited-shipping', 'Expedited Shipping', '配送料')) || 0,
        quantity: parseInt(get('quantity', 'Quantity', '数量')) || 0,
        status: get('status', 'Status', 'ステータス') || 'Active',
        condition: get('item-condition', 'Item Condition', 'コンディション') || '',
        fulfillment: (get('fulfillment-channel', 'Fulfillment Channel', 'フルフィルメントチャネル') || '').toUpperCase().includes('AMAZON') ? 'FBA' : 'FBM',
        open_date: get('open-date', 'Open Date', 'open_date', '出品日'),
        listing_id: get('listing-id', 'Listing ID', 'listing_id'),
        item_description: get('item-description', 'Item Description', 'item_description', '商品説明'),
      };
    }).filter(item => item.sku);

    console.log(`[ProfitCalc] マッピング後: ${mapped.length}件 (SKUあり)`);
    if (mapped.length > 0) {
      console.log('[ProfitCalc] マッピング例:', JSON.stringify(mapped[0]));
    }

    dbSyncListings(mapped);
    console.log(`[ProfitCalc] DB保存完了: ${mapped.length}件`);
    res.json({ count: mapped.length, message: '同期完了' });
  } catch (err) {
    console.error('[ProfitCalc] 同期エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: listings デバッグ情報 ──
router.get('/api/listings/debug', async (req, res) => {
  try {
    await ensureDb();
    const listings = getListings();
    const withQty = listings.filter(l => (l.quantity || 0) > 0);
    const sample = listings.slice(0, 3).map(l => ({
      sku: l.sku, asin: l.asin, product_name: (l.product_name || '').slice(0, 30),
      price: l.price, quantity: l.quantity, status: l.status, fulfillment: l.fulfillment,
    }));
    res.json({
      total: listings.length,
      withQuantity: withQty.length,
      lastSync: getSyncMeta('listings_last_sync'),
      sample,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API: 販売個数同期（注文レポートから集計） ──
router.post('/api/listings/sync-sales', async (req, res) => {
  try {
    await ensureDb();
    const days = parseInt(req.query.days) || 30;
    console.log(`[ProfitCalc] 販売個数同期開始 (過去${days}日)...`);

    const salesMap = await getSalesCountBySku(days);
    let updated = 0;
    for (const [sku, count] of Object.entries(salesMap)) {
      const changes = updateListing(sku, { total_sold: count }, true);
      if (changes > 0) updated++;
    }
    bulkSave();

    console.log(`[ProfitCalc] 販売個数同期完了: ${updated}SKU更新`);
    res.json({ updated, totalSkus: Object.keys(salesMap).length, days });
  } catch (err) {
    console.error('[ProfitCalc] 販売個数同期エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: listing個別更新 ──
router.put('/api/listings/update', async (req, res) => {
  try {
    await ensureDb();
    const { sku, ...fields } = req.body;
    if (!sku) return res.status(400).json({ error: 'SKUは必須です' });
    updateListing(sku, fields);
    res.json({ ok: true, sku });
  } catch (err) {
    console.error('[ProfitCalc] listing更新エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: プライスターCSVアップロード（仕入価格インポート） ──
import multer from 'multer';
const csvUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

router.post('/api/listings/import-csv', csvUpload.single('file'), async (req, res) => {
  try {
    await ensureDb();
    if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });

    // Shift_JIS → UTF-8 変換
    let text;
    try {
      const iconv = await import('iconv-lite');
      text = iconv.default.decode(req.file.buffer, 'Shift_JIS');
    } catch {
      text = req.file.buffer.toString('utf-8');
    }

    const lines = text.split('\n').filter(l => l.trim());
    if (lines.length < 2) return res.status(400).json({ error: 'CSVが空です' });

    // ヘッダー解析
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"/, '').replace(/"$/, ''));
    console.log('[ProfitCalc] CSV import headers:', headers.join(', '));

    const skuIdx = headers.indexOf('SKU');
    const costIdx = headers.indexOf('cost');
    const akajiIdx = headers.indexOf('akaji');
    const takaneIdx = headers.indexOf('takane');
    const priceTraceIdx = headers.indexOf('priceTrace');

    if (skuIdx < 0) return res.status(400).json({ error: 'SKU列が見つかりません' });

    let updated = 0, skipped = 0;
    for (let i = 1; i < lines.length; i++) {
      const cols = parseCSVLine(lines[i]);
      // プライスターCSVは ="値" 形式 → = と " を全て除去
      const cleanVal = (v) => (v || '').replace(/^[="]+/, '').replace(/[="]+$/, '').trim();
      const sku = cleanVal(cols[skuIdx]);
      if (!sku) continue;

      const updates = {};
      if (costIdx >= 0) {
        const cost = parseFloat(cleanVal(cols[costIdx])) || 0;
        if (cost > 0) updates.cost_price = cost;
      }
      if (akajiIdx >= 0) {
        const akaji = parseFloat(cleanVal(cols[akajiIdx])) || 0;
        if (akaji > 0) updates.loss_stopper = akaji;
      }
      if (takaneIdx >= 0) {
        const takane = parseFloat(cleanVal(cols[takaneIdx])) || 0;
        if (takane > 0) updates.high_stopper = takane;
      }

      if (Object.keys(updates).length > 0) {
        const changes = updateListing(sku, updates, true); // skipSave=true for bulk
        if (changes > 0) {
          updated++;
        } else {
          skipped++; // SKUがDBに存在しない
        }
      } else {
        skipped++;
      }
    }

    bulkSave(); // 一括保存
    console.log(`[ProfitCalc] CSVインポート完了: 更新${updated}件, スキップ${skipped}件`);
    res.json({ updated, skipped, total: lines.length - 1 });
  } catch (err) {
    console.error('[ProfitCalc] CSVインポートエラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// CSVの1行をパース（ダブルクォート対応）
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"' && !inQuotes) { inQuotes = true; continue; }
    if (ch === '"' && inQuotes) {
      if (line[i + 1] === '"') { current += '"'; i++; continue; }
      inQuotes = false; continue;
    }
    if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; continue; }
    current += ch;
  }
  result.push(current.trim());
  return result;
}

// ── API: 在庫CSVダウンロード（FBA/自己発送別） ──
router.get('/api/listings/download-csv', async (req, res) => {
  try {
    await ensureDb();
    const type = req.query.type || 'all'; // 'fba', 'fbm', 'all'
    let listings = getListings();

    if (type === 'fba') {
      listings = listings.filter(l => l.fulfillment === 'FBA');
    } else if (type === 'fbm') {
      listings = listings.filter(l => l.fulfillment !== 'FBA');
    }

    // プライスター互換CSVフォーマット
    const header = 'SKU,ASIN,商品名,数量,出品価格,仕入価格,赤字ストッパー,高値ストッパー,コンディション,価格追従,FBA/自己発送,手数料,利益';
    const rows = listings.map(l => {
      const profit = (l.price || 0) - (l.cost_price || 0) - (l.referral_fee || 0) - (l.fba_fee || 0);
      return [
        csvEscape(l.sku),
        csvEscape(l.asin),
        csvEscape(l.product_name),
        l.quantity || 0,
        l.price || 0,
        l.cost_price || 0,
        l.loss_stopper || 0,
        l.high_stopper || 0,
        csvEscape(l.condition || '新品'),
        csvEscape(l.price_tracking || 'しない'),
        l.fulfillment === 'FBA' ? 'FBA' : '自己発送',
        (l.referral_fee || 0) + (l.fba_fee || 0),
        profit,
      ].join(',');
    });

    const csv = '\uFEFF' + header + '\n' + rows.join('\n'); // BOM付きUTF-8
    const label = type === 'fba' ? 'FBA' : type === 'fbm' ? '自己発送' : '全商品';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="inventory_${label}_${new Date().toISOString().slice(0,10)}.csv"`);
    res.send(csv);
  } catch (err) {
    console.error('[ProfitCalc] CSVダウンロードエラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

function csvEscape(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ── API: 競合オファー取得（価格改定用・技術検証） ──
router.get('/api/amazon/offers/:asin', async (req, res) => {
  try {
    const { asin } = req.params;
    const condition = req.query.condition || 'New';
    const offers = await getItemOffers(asin, condition);
    res.json(offers);
  } catch (err) {
    console.error('[ProfitCalc] オファー取得エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: 価格更新（価格改定用・技術検証） ──
router.post('/api/amazon/update-price', async (req, res) => {
  try {
    const { sku, price } = req.body;
    const result = await updatePrice({ sku, price: Number(price) });
    res.json(result);
  } catch (err) {
    console.error('[ProfitCalc] 価格更新エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: Amazon出品商品レポート（SKU数確認用） ──
router.get('/api/amazon/listings-report', async (req, res) => {
  try {
    const report = await getActiveListingsReport();
    // サマリー情報を返す（全データは大きいのでカウントと概要のみ）
    const statusCounts = {};
    for (const row of report.listings) {
      const status = row['status'] || row['Status'] || 'unknown';
      statusCounts[status] = (statusCounts[status] || 0) + 1;
    }
    res.json({
      totalCount: report.totalCount,
      statusCounts,
      sampleHeaders: report.headers.slice(0, 20),
      sample: report.listings.slice(0, 5),
    });
  } catch (err) {
    console.error('[ProfitCalc] 出品レポート取得エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: 商品数診断 ──
router.get('/api/price-revision/diagnostics', async (req, res) => {
  try {
    await ensureDb();
    const all = getProducts({});
    const uniqueAsins = new Set(all.map(p => p.asin).filter(Boolean));
    const uniqueSkus = new Set(all.map(p => p.sku).filter(Boolean));
    const noAsin = all.filter(p => !p.asin || p.asin.length < 5);
    const duplicateAsins = {};
    all.forEach(p => {
      if (p.asin) {
        if (!duplicateAsins[p.asin]) duplicateAsins[p.asin] = [];
        duplicateAsins[p.asin].push({ id: p.id, sku: p.sku, status: p.status });
      }
    });
    const dupes = Object.entries(duplicateAsins).filter(([, v]) => v.length > 1);

    // ASIN形式の分析
    const realAsins = all.filter(p => p.asin && /^[A-Z0-9]{10}$/.test(p.asin)); // 10桁英数字
    const janCodes = all.filter(p => p.asin && /^\d{13}$/.test(p.asin));        // 13桁数字（JAN/EAN）
    const otherIds = all.filter(p => p.asin && !/^[A-Z0-9]{10}$/.test(p.asin) && !/^\d{13}$/.test(p.asin));

    res.json({
      totalProducts: all.length,
      uniqueAsins: uniqueAsins.size,
      uniqueSkus: uniqueSkus.size,
      realAsinCount: realAsins.length,
      janCodeCount: janCodes.length,
      otherIdCount: otherIds.length,
      noAsinCount: noAsin.length,
      janSamples: janCodes.slice(0, 3).map(p => ({ id: p.id, asin: p.asin, sku: p.sku, name: p.product_name?.slice(0, 30) })),
      otherSamples: otherIds.slice(0, 3).map(p => ({ id: p.id, asin: p.asin, sku: p.sku, name: p.product_name?.slice(0, 30) })),
      duplicateAsinCount: dupes.length,
      // 在庫・価格による分析
      withPrice: all.filter(p => p.selling_price > 0).length,
      withoutPrice: all.filter(p => !p.selling_price || p.selling_price === 0).length,
      statusBreakdown: (() => {
        const sb = {};
        all.forEach(p => { const s = p.status || '不明'; sb[s] = (sb[s] || 0) + 1; });
        return sb;
      })(),
      fulfillmentBreakdown: (() => {
        const fb = {};
        all.forEach(p => { const f = p.fulfillment || '不明'; fb[f] = (fb[f] || 0) + 1; });
        return fb;
      })(),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API: Amazon商品同期 ──
router.post('/api/price-revision/sync', async (req, res) => {
  try {
    await ensureDb();
    console.log('[PriceRevision] Amazon商品同期開始...');
    const report = await getActiveListingsReport();
    console.log(`[PriceRevision] レポート取得完了: ${report.totalCount}件`);
    const result = syncProductsFromListings(report.listings);
    console.log(`[PriceRevision] 同期完了: 新規${result.inserted}件, 更新${result.updated}件`);
    res.json(result);
  } catch (err) {
    console.error('[PriceRevision] 同期エラー:', err.message, err.stack);
    res.status(500).json({ error: err.message });
  }
});

// ── API: 価格改定機能 ──

// 価格改定: 全商品一覧（追従設定の有無に関わらず）
router.get('/api/price-revision/products', async (req, res) => {
  try {
    await ensureDb();
    const { tracking } = req.query;
    let products;
    if (tracking === 'true') {
      products = getTrackingProducts();
    } else {
      products = getProducts({});
    }
    res.json(products);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 価格変動履歴（全体）
router.get('/api/price-revision/history', async (req, res) => {
  try {
    await ensureDb();
    const limit = parseInt(req.query.limit) || 100;
    const history = getRecentPriceHistory(limit);
    res.json(history);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 商品別の価格変動履歴
router.get('/api/price-revision/history/:productId', async (req, res) => {
  try {
    await ensureDb();
    const limit = parseInt(req.query.limit) || 50;
    const history = getPriceHistory(parseInt(req.params.productId), limit);
    res.json(history);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ワーカー制御
router.get('/api/price-revision/worker', (req, res) => {
  res.json(getWorkerStatus());
});

router.post('/api/price-revision/worker/start', (req, res) => {
  startPriceWorker();
  res.json({ ok: true });
});

router.post('/api/price-revision/worker/stop', (req, res) => {
  stopPriceWorker();
  res.json({ ok: true });
});

// 商品キャッシュ手動リフレッシュ
router.post('/api/price-revision/refresh-cache', (req, res) => {
  refreshProductCache();
  res.json({ ok: true });
});

export default router;
