/**
 * 同梱商品検索 (Cross-Sell Finder)
 *
 * NE商品コードで検索 → 過去90日に同じ伝票で一緒に買われた商品ランキング。
 * 本体ロジックはミニPC側 /apps/warehouse/api/cross-sell。Render側はその中継 + UI。
 */
import { Router } from 'express';

const router = Router();

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';

function getApiHeaders() {
  const h = {};
  if (process.env.CF_ACCESS_CLIENT_ID) h['CF-Access-Client-Id'] = process.env.CF_ACCESS_CLIENT_ID;
  if (process.env.CF_ACCESS_CLIENT_SECRET) h['CF-Access-Client-Secret'] = process.env.CF_ACCESS_CLIENT_SECRET;
  if (process.env.WAREHOUSE_API_KEY) h['X-API-Key'] = process.env.WAREHOUSE_API_KEY;
  return h;
}

router.get('/', (req, res) => {
  res.render('cross-sell-finder', {
    title: '同梱商品検索',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

router.get('/api/search', async (req, res) => {
  const code = (req.query.code || '').trim();
  const days = String(req.query.days || '90');
  if (!code) return res.status(400).json({ error: 'code が必要です' });

  const url = `${WAREHOUSE_URL}/apps/warehouse/api/cross-sell`
    + `?code=${encodeURIComponent(code)}&days=${encodeURIComponent(days)}`;

  try {
    const r = await fetch(url, {
      headers: getApiHeaders(),
      signal: AbortSignal.timeout(30000),
    });
    const text = await r.text();
    const ct = r.headers.get('content-type') || '';
    // CF Access 未認証や upstream エラーで HTML が返ってくるケースを JSON 化。
    // detail はサーバログのみ。レスポンスには内部 HTML / stack を漏らさない。
    if (!r.ok || !ct.includes('json')) {
      console.warn(
        `[CrossSell] upstream non-json response status=${r.status} ct=${ct} `
        + `code=${code} body=${text.slice(0, 500).replace(/\s+/g, ' ')}`
      );
      return res.status(r.ok ? 502 : r.status).json({
        error: 'warehouse_api_error',
        status: r.status,
      });
    }
    res.type('application/json').send(text);
  } catch (e) {
    // AbortSignal.timeout のキャンセルは TimeoutError 名で投げられる。
    // 上流遅延と Render 側バグを区別するため 504 で返す。
    if (e.name === 'TimeoutError' || e.name === 'AbortError') {
      console.warn(`[CrossSell] upstream timeout code=${code}`);
      return res.status(504).json({ error: 'warehouse_api_timeout' });
    }
    console.error(`[CrossSell] proxy error code=${code} err=${e.message}`);
    res.status(502).json({ error: 'warehouse_api_unreachable' });
  }
});

export default router;
