/**
 * au PAY Market APIプロキシ（さくらVPS用）
 *
 * VPSの固定IPからau PAY API（api.manager.wowma.jp）にリクエストを中継する。
 * ミニPC（動的IP）からはVPS経由でAPIにアクセスする。
 *
 * 使い方（VPS上で実行）:
 *   AUPAY_API_KEY=xxx PROXY_SECRET=yyy node aupay-proxy.js
 *
 * ミニPCからのアクセス:
 *   GET http://133.167.122.198:8080/wmshopapi/searchTradeInfoListProc?shopId=...
 *   Header: X-Proxy-Secret: yyy
 *
 * セキュリティ:
 *   - X-Proxy-Secretヘッダーで認証（第三者のアクセスを防止）
 *   - au PAY APIキーはVPS側で管理（ミニPCには不要）
 */

const http = require('http');
const PORT = process.env.PROXY_PORT || 8080;
const API_KEY = process.env.AUPAY_API_KEY || '';
const PROXY_SECRET = process.env.PROXY_SECRET || '';
const BASE = 'https://api.manager.wowma.jp';

if (!API_KEY) { console.error('AUPAY_API_KEY is required'); process.exit(1); }
if (!PROXY_SECRET) { console.error('PROXY_SECRET is required'); process.exit(1); }

const server = http.createServer(async (req, res) => {
  // ヘルスチェック
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK');
    return;
  }

  // 認証チェック
  const secret = req.headers['x-proxy-secret'] || '';
  if (secret !== PROXY_SECRET) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Forbidden' }));
    return;
  }

  // /wmshopapi/ で始まるパスのみ中継
  if (!req.url.startsWith('/wmshopapi/')) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
    return;
  }

  const targetUrl = BASE + req.url;
  const ts = new Date().toISOString().slice(0, 19);

  try {
    const response = await fetch(targetUrl, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    });

    const data = await response.text();
    console.log(`[${ts}] ${req.url.slice(0, 80)} -> ${response.status} (${data.length} bytes)`);

    res.writeHead(response.status, {
      'Content-Type': response.headers.get('content-type') || 'application/xml',
    });
    res.end(data);
  } catch (e) {
    console.error(`[${ts}] ERROR: ${e.message}`);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: e.message }));
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`au PAY proxy running on port ${PORT}`);
  console.log(`API Key: ${API_KEY.slice(0, 8)}...`);
});
