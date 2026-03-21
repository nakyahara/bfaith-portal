/**
 * FBA仕入れ利益計算ツール — Express Router
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getProduct, getFees, createListing, getShippingTemplates } from './sp-api.js';
import { initDb, saveResearch, getResearch, getResearchById, updateResearchStatus, updateResearch, promoteToProduct, saveProduct, getProducts, getProductById, updateProductStatus, updateProduct, deleteProduct, getSetItems, saveSetItems } from './db.js';
import { loadSuppliers, addSupplier, deleteSupplier } from './suppliers.js';
import { loadShipping, addShipping, updateShipping, deleteShipping } from './shipping.js';
import { getSetting, setSetting, getAllSettings } from './settings.js';

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

router.get('/suppliers', (req, res) => {
  res.sendFile(path.join(__dirname, 'suppliers.html'));
});

router.get('/shipping', (req, res) => {
  res.sendFile(path.join(__dirname, 'shipping.html'));
});

router.get('/settings', (req, res) => {
  res.sendFile(path.join(__dirname, 'settings.html'));
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

    // FBM: 保存済み設定を取得
    const shippingTemplate = !isFba ? getSetting('amazon_shipping_template') : null;
    const paymentRestriction = !isFba ? (getSetting('amazon_payment_restriction') || 'none') : 'none';

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

export default router;
