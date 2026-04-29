/**
 * 同梱商品検索 (Cross-Sell Finder)
 *
 * NE商品コードで検索 → 過去90日に同じ伝票で一緒に買われた商品ランキング。
 * 本体ロジックはミニPC側 /service-api/cross-sell/search。
 * Render → ミニPC は CF Access + serviceAuth (Bearer) 経由。
 * (/apps/warehouse/* はセッション認証必須でサーバ間通信できないため使えない)
 */
import { Router } from 'express';

const router = Router();

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';

function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
  };
}

router.get('/', (req, res) => {
  res.render('cross-sell-finder', {
    title: '同梱商品検索',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

router.get('/api/suggest', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (q.length < 2) return res.json([]);

  const url = `${WAREHOUSE_URL}/service-api/cross-sell/suggest`
    + `?q=${encodeURIComponent(q)}`;

  try {
    const r = await fetch(url, {
      headers: getServiceHeaders(),
      signal: AbortSignal.timeout(10000),
    });
    const text = await r.text();
    const ct = r.headers.get('content-type') || '';
    if (!ct.includes('json')) {
      console.warn(`[CrossSell suggest] non-json status=${r.status} ct=${ct}`);
      return res.status(r.ok ? 502 : r.status).json([]);
    }
    const data = JSON.parse(text);
    if (!data.ok) return res.status(r.status || 502).json([]);
    res.json(data.result || []);
  } catch (e) {
    console.warn(`[CrossSell suggest] error q=${q} err=${e.message}`);
    res.status(502).json([]);
  }
});

router.get('/api/search', async (req, res) => {
  const code = (req.query.code || '').trim();
  const days = String(req.query.days || '90');
  if (!code) return res.status(400).json({ error: 'code が必要です' });

  const url = `${WAREHOUSE_URL}/service-api/cross-sell/search`
    + `?code=${encodeURIComponent(code)}&days=${encodeURIComponent(days)}`;

  try {
    const r = await fetch(url, {
      headers: getServiceHeaders(),
      signal: AbortSignal.timeout(30000),
    });
    const text = await r.text();
    const ct = r.headers.get('content-type') || '';
    // CF Access 未認証や upstream エラーで HTML が返ってくるケースを JSON 化。
    // detail はサーバログのみ。レスポンスには内部 HTML / stack を漏らさない。
    if (!ct.includes('json')) {
      console.warn(
        `[CrossSell] upstream non-json status=${r.status} ct=${ct} `
        + `code=${code} body=${text.slice(0, 500).replace(/\s+/g, ' ')}`
      );
      return res.status(r.ok ? 502 : r.status).json({
        error: 'warehouse_api_error',
        status: r.status,
      });
    }
    const data = JSON.parse(text);
    if (!data.ok) {
      console.warn(`[CrossSell] upstream error status=${r.status} code=${code} `
        + `error=${data.error} message=${data.message}`);
      return res.status(r.status || 502).json({
        error: data.error || 'warehouse_api_error',
        message: data.message,
      });
    }
    // service API は { ok, result: {...} } 形式 → result を展開して UI に返す
    res.json(data.result);
  } catch (e) {
    if (e.name === 'TimeoutError' || e.name === 'AbortError') {
      console.warn(`[CrossSell] upstream timeout code=${code}`);
      return res.status(504).json({ error: 'warehouse_api_timeout' });
    }
    console.error(`[CrossSell] proxy error code=${code} err=${e.message}`);
    res.status(502).json({ error: 'warehouse_api_unreachable' });
  }
});

export default router;
