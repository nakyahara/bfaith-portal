/**
 * FBA仕入れ利益計算ツール — Express Router
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getProduct, getFees } from './sp-api.js';
import { initDb, saveResearch, getResearch, getResearchById, updateResearchStatus, updateResearch, promoteToProduct, getProducts, updateProductStatus, updateProduct } from './db.js';
import { loadSuppliers, addSupplier } from './suppliers.js';

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

router.put('/api/products/:id', async (req, res) => {
  try {
    await ensureDb();
    const { status } = req.body;
    if (status) {
      updateProductStatus(parseInt(req.params.id), status);
    } else {
      updateProduct(parseInt(req.params.id), req.body);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API: NE用CSVエクスポート ──
router.get('/api/products/csv/ne', async (req, res) => {
  try {
    await ensureDb();
    const products = getProducts({ status: req.query.status });

    const header = ['商品コード', 'ASIN', '商品名', 'JAN', '型番', '仕入先', '仕入価格（税込）', '販売価格', 'FBA/FBM'];
    const rows = products.map(p => [
      p.ne_product_code || '', p.asin, p.product_name || '', p.jan || '', p.part_number || '',
      p.supplier_name || '', p.wholesale_price_with_tax || '', p.selling_price || '', p.fulfillment || '',
    ]);

    const csv = [header, ...rows].map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');
    const bom = '\uFEFF';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="ne_products_${new Date().toISOString().slice(0,10)}.csv"`);
    res.send(bom + csv);
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

export default router;
