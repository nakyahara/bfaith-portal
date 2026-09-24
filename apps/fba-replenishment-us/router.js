/**
 * 米国FBA在庫補充 (/apps/fba-replenishment-us)。
 *
 * PR1 (2026-09-24) = 見るだけ: 毎朝 miniPC が取った米国の RESTOCK / PLANNING を、取れたままの行から一覧にする。
 *   推奨・仮確定・出力・ピッキング準備・納品実績は後の PR (設計方針 = AI_reference システム設計/米国FBA納品アプリ_設計方針_20260924.md §8)。
 *   日本の FBA在庫補充 (apps/fba-replenishment) の表・計算・キャッシュには触らない (読むのは warehouse-mirror の SKU マスタの写しだけ)。
 *
 * データの流れ: miniPC daily-sync「FBA在庫snapshot」→ DATA_DIR/fba-us-reports/*.json (apps/warehouse/fba-us-reports-store.js)
 *   → miniPC GET /service-api/fba/us/reports/latest → この画面を開くたびに読む (Render には保存しない。15 行ほど)
 */
import express from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { buildUsInventoryView } from './us-view.js';

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';

/** miniPC のサービス API を GET で呼ぶ (日本版 router.js の callMiniPC と同じ認証ヘッダ。読むだけなので 1 回だけ再試行) */
export async function fetchUsReportsFromMiniPC({ fetchImpl = fetch, timeout = 30000 } = {}) {
  const url = `${WAREHOUSE_URL}/service-api/fba/us/reports/latest`;
  const headers = {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
  };
  let lastError;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await fetchImpl(url, { headers, redirect: 'manual', signal: AbortSignal.timeout(timeout) });
      const ct = res.headers.get('content-type') || '';
      if (res.status === 302 || res.status === 303) throw new Error(`miniPC の認証 (Cloudflare Access) の設定がおかしい (HTTP ${res.status})`);
      if (res.status === 401 || res.status === 403) throw new Error(`miniPC に断られた (HTTP ${res.status})`);
      if (res.status === 404) throw new Error('miniPC にこの口がまだ無い (HTTP 404 = miniPC の更新と WarehouseServer の再起動がまだ)');
      if (!res.ok || !ct.includes('application/json')) {
        const txt = await res.text().catch(() => '');
        throw Object.assign(new Error(`miniPC HTTP ${res.status}: ${txt.slice(0, 160)}`), { retryable: res.status >= 500 });
      }
      const body = await res.json();
      if (!body || body.ok !== true) throw new Error(`miniPC の応答の形がおかしい: ${JSON.stringify(body).slice(0, 160)}`);
      return body;
    } catch (e) {
      lastError = e;
      const retryable = e.retryable || e.name === 'TimeoutError' || /fetch failed|ECONNRESET|ETIMEDOUT/i.test(String(e.message));
      if (!retryable || attempt === 2) throw e;
      await new Promise((r) => setTimeout(r, 800));
    }
  }
  throw lastError;
}

/**
 * 米国 SKU → 自社の商品コード (NE 商品コード = ロジザード商品ID)。
 *   1) SKU マスタ (mirror_sku_resolved・source='master') = 構成と数量つき (例: cardstand-r-40 = cardstand-r × 40)
 *   2) 無ければ 商品コードと同じ文字列 (mirror_products) = 1 個として扱えるが、SKU マスタに未登録なので「要登録」と出す
 *   3) どちらも無ければ none
 * 写しを読めなければ unknown (エラーつき) = 「結びつかない」と混ぜない
 */
export function resolveUsSkus(skus, { mdb } = {}) {
  const out = new Map();
  if (!skus.length) return out;
  let db;
  try { db = mdb || getMirrorDB(); }
  catch (e) { for (const s of skus) out.set(s, { route: 'unknown', components: [], error: e.message }); return out; }
  try {
    const ph = skus.map(() => '?').join(',');
    const master = db.prepare(`SELECT lower(trim(seller_sku)) AS k, ne_code, quantity, 商品名 AS name, sort_order
      FROM mirror_sku_resolved WHERE source = 'master' AND lower(trim(seller_sku)) IN (${ph})
      ORDER BY k, sort_order, ne_code`).all(...skus);
    for (const r of master) {
      if (!out.has(r.k)) out.set(r.k, { route: 'master', components: [], name: r.name || null });
      out.get(r.k).components.push({ ne_code: r.ne_code, qty: r.quantity });
    }
    const rest = skus.filter((s) => !out.has(s));
    if (rest.length) {
      const ph2 = rest.map(() => '?').join(',');
      for (const r of db.prepare(`SELECT lower(trim(商品コード)) AS k, 商品コード AS code, 商品名 AS name FROM mirror_products WHERE lower(trim(商品コード)) IN (${ph2})`).all(...rest)) {
        out.set(r.k, { route: 'product_code', components: [{ ne_code: r.code, qty: 1 }], name: r.name || null });
      }
    }
    for (const s of skus) if (!out.has(s)) out.set(s, { route: 'none', components: [] });
  } catch (e) {
    for (const s of skus) out.set(s, { route: 'unknown', components: [], error: e.message });
  }
  return out;
}

const router = express.Router();

router.get('/', (req, res) => {
  res.render('fba-replenishment-us', { username: req.session && req.session.email, displayName: req.session && req.session.displayName });
});

router.get('/api/inventory', async (req, res) => {
  let payload;
  try {
    payload = await fetchUsReportsFromMiniPC();
  } catch (e) {
    return res.status(502).json({ ok: false, error: 'minipc_unreachable', message: `miniPC から米国のレポートを読めませんでした: ${e.message}` });
  }
  try {
    const view = buildUsInventoryView(payload, { resolveSkus: (skus) => resolveUsSkus(skus) });
    res.json({ ok: true, ...view });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'build_failed', message: e.message });
  }
});

export default router;
