/**
 * SKUマスタ CRUD API
 *
 * mount 想定:
 *   import { mountSkuMasterApi } from './sku-master-api.js';
 *   mountSkuMasterApi(router);  // warehouse/router.js の export 前に呼ぶ
 *
 * パス（router.js の /apps/warehouse/api/* 階層に乗る想定）:
 *   GET    /api/m-sku-master              一覧（q検索、ページング）
 *   GET    /api/m-sku-master/:sku         単票（components + NE側商品名 join）
 *   POST   /api/m-sku-master              新規登録（master + components 一括）
 *   PUT    /api/m-sku-master/:sku         更新（商品名・構成 上書き）
 *   DELETE /api/m-sku-master/:sku         削除（CASCADE で components も）
 *   GET    /api/m-sku-master/:sku/related 同じNEコードを持つ過去SKUの商品名一覧（参考表示用）
 */
import { getDB } from './db.js';
import { getSkuMasterDetail } from './sku-resolver.js';

function normalize(s) {
  return String(s ?? '').trim().toLowerCase();
}

/**
 * 登録/更新の共通バリデーション
 * @returns {string|null} エラーメッセージ、問題なければ null
 */
function validatePayload(db, payload) {
  if (!payload || typeof payload !== 'object') return 'payload が不正';
  const sku = normalize(payload.seller_sku);
  const name = String(payload.商品名 ?? '').trim();
  const components = Array.isArray(payload.components) ? payload.components : [];

  if (!sku) return 'seller_sku は必須';
  if (!name) return '商品名 は必須';
  if (components.length === 0) return 'components は1件以上必要';

  // ne_code 正規化済みの構造をチェック
  const seen = new Set();
  for (let i = 0; i < components.length; i++) {
    const c = components[i];
    const ne = normalize(c.ne_code);
    if (!ne) return `components[${i}].ne_code が空`;
    if (seen.has(ne)) return `components[${i}].ne_code が重複: ${ne}`;
    seen.add(ne);
    const qty = parseInt(c.数量 ?? c.quantity ?? 1, 10);
    if (!Number.isFinite(qty) || qty <= 0) return `components[${i}].数量 が不正: ${c.数量}`;
  }

  // ne_code が raw_ne_products に存在するかチェック
  const checkNe = db.prepare('SELECT 1 FROM raw_ne_products WHERE 商品コード = ?');
  for (const c of components) {
    const ne = normalize(c.ne_code);
    if (!checkNe.get(ne)) return `ne_code "${ne}" は raw_ne_products に存在しません`;
  }

  return null;
}

function buildNormalizedPayload(payload) {
  return {
    seller_sku: normalize(payload.seller_sku),
    商品名: String(payload.商品名).trim(),
    components: payload.components.map((c, i) => ({
      ne_code: normalize(c.ne_code),
      数量: parseInt(c.数量 ?? c.quantity ?? 1, 10),
      sort_order: i,
    })),
    user: String(payload.user ?? '').trim() || null,
  };
}

export function mountSkuMasterApi(router) {
  // ─── GET 一覧 ───
  router.get('/api/m-sku-master', (req, res) => {
    const db = getDB();
    const limit = Math.min(parseInt(req.query.limit ?? '100', 10), 1000);
    const offset = parseInt(req.query.offset ?? '0', 10);
    const q = String(req.query.q ?? '').trim();

    let where = '';
    const params = [];
    if (q) {
      where = 'WHERE m.seller_sku LIKE ? OR m.商品名 LIKE ?';
      const like = `%${q}%`;
      params.push(like, like);
    }

    const items = db.prepare(`
      SELECT
        m.seller_sku,
        m.商品名,
        m.created_at,
        m.updated_at,
        m.created_by,
        m.updated_by,
        (SELECT COUNT(*) FROM m_sku_components c WHERE c.seller_sku = m.seller_sku) AS 構成数
      FROM m_sku_master m
      ${where}
      ORDER BY m.updated_at DESC
      LIMIT ? OFFSET ?
    `).all(...params, limit, offset);

    const total = db.prepare(`SELECT COUNT(*) c FROM m_sku_master m ${where}`).get(...params).c;
    res.json({ total, limit, offset, items });
  });

  // ─── GET 単票 ───
  router.get('/api/m-sku-master/:sku', (req, res) => {
    const db = getDB();
    const sku = normalize(req.params.sku);
    const detail = getSkuMasterDetail(db, sku);
    if (!detail.exists) return res.status(404).json({ error: 'not found' });
    res.json(detail);
  });

  // ─── GET 関連SKU（過去登録の商品名参考表示） ───
  router.get('/api/m-sku-master/:sku/related', (req, res) => {
    const db = getDB();
    const sku = normalize(req.params.sku);
    // このSKUの構成NEコードと共通のNEコードを持つ別のSKUを探す
    const related = db.prepare(`
      SELECT DISTINCT m.seller_sku, m.商品名
      FROM m_sku_components c1
      INNER JOIN m_sku_components c2 ON c1.ne_code = c2.ne_code AND c1.seller_sku <> c2.seller_sku
      INNER JOIN m_sku_master m ON c2.seller_sku = m.seller_sku
      WHERE c1.seller_sku = ?
      ORDER BY m.商品名
      LIMIT 50
    `).all(sku);
    res.json({ items: related });
  });

  // ─── POST 新規登録 ───
  router.post('/api/m-sku-master', (req, res) => {
    const db = getDB();
    const err = validatePayload(db, req.body);
    if (err) return res.status(400).json({ error: err });

    const p = buildNormalizedPayload(req.body);

    // 重複チェック
    const existing = db.prepare('SELECT 1 FROM m_sku_master WHERE seller_sku = ?').get(p.seller_sku);
    if (existing) return res.status(409).json({ error: `seller_sku "${p.seller_sku}" は既に登録されています` });

    const insMaster = db.prepare(`
      INSERT INTO m_sku_master (seller_sku, 商品名, created_by, updated_by)
      VALUES (?, ?, ?, ?)
    `);
    const insComp = db.prepare(`
      INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order)
      VALUES (?, ?, ?, ?)
    `);

    const tx = db.transaction(() => {
      insMaster.run(p.seller_sku, p.商品名, p.user, p.user);
      for (const c of p.components) {
        insComp.run(p.seller_sku, c.ne_code, c.数量, c.sort_order);
      }
    });

    try {
      tx();
    } catch (e) {
      return res.status(400).json({ error: `DB制約エラー: ${e.message}` });
    }

    res.status(201).json(getSkuMasterDetail(db, p.seller_sku));
  });

  // ─── PUT 更新 ───
  router.put('/api/m-sku-master/:sku', (req, res) => {
    const db = getDB();
    const sku = normalize(req.params.sku);

    const existing = db.prepare('SELECT 1 FROM m_sku_master WHERE seller_sku = ?').get(sku);
    if (!existing) return res.status(404).json({ error: 'not found' });

    const payload = { ...req.body, seller_sku: sku };
    const err = validatePayload(db, payload);
    if (err) return res.status(400).json({ error: err });

    const p = buildNormalizedPayload(payload);

    const updMaster = db.prepare(`
      UPDATE m_sku_master
      SET 商品名 = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), updated_by = ?
      WHERE seller_sku = ?
    `);
    const delComp = db.prepare('DELETE FROM m_sku_components WHERE seller_sku = ?');
    const insComp = db.prepare(`
      INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order)
      VALUES (?, ?, ?, ?)
    `);

    const tx = db.transaction(() => {
      updMaster.run(p.商品名, p.user, p.seller_sku);
      delComp.run(p.seller_sku);
      for (const c of p.components) {
        insComp.run(p.seller_sku, c.ne_code, c.数量, c.sort_order);
      }
    });

    try {
      tx();
    } catch (e) {
      return res.status(400).json({ error: `DB制約エラー: ${e.message}` });
    }

    res.json(getSkuMasterDetail(db, p.seller_sku));
  });

  // ─── DELETE ───
  router.delete('/api/m-sku-master/:sku', (req, res) => {
    const db = getDB();
    const sku = normalize(req.params.sku);

    const existing = db.prepare('SELECT 1 FROM m_sku_master WHERE seller_sku = ?').get(sku);
    if (!existing) return res.status(404).json({ error: 'not found' });

    const result = db.prepare('DELETE FROM m_sku_master WHERE seller_sku = ?').run(sku);
    res.json({ ok: true, deleted: result.changes });
  });
}
