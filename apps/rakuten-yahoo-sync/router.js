/**
 * RakutenYahooSync (RYS) — Render-side router (W3 UI 骨柄)
 *
 * 全 API はミニPC上の RysServer (winsw、port 3010) を Cloudflare Tunnel 経由で叩く。
 * Render に Rakuten / Yahoo の鍵は置かない (CLAUDE.md 原則: APIはミニPC経由のみ、Render=UI のみ)。
 *
 * env:
 *   RYS_MINIPC_URL       — miniPC API ベース (default https://wh.bfaith-wh.uk/rys-api)
 *   RYS_API_TOKEN        — miniPC RysServer の認証 token (32byte ランダム、共有)
 *   CF_ACCESS_CLIENT_ID  — Cloudflare Access Service Token (warehouse 系と同じパターン)
 *   CF_ACCESS_CLIENT_SECRET
 *
 * UI 骨柄なので最小: 未公開品一覧 + ワンクリック公開 + 進捗 polling だけ。
 * U2-U5 (needs_category 解消 / 公開済一覧 / 変換表 / 監査ログ) は同 BASE で endpoint 揃えば後追い可。
 */
import express from 'express';

const router = express.Router();

const MINIPC_URL = process.env.RYS_MINIPC_URL || 'https://wh.bfaith-wh.uk/rys-api';
const TIMEOUT_MS = 60_000;

function getMiniPcHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'x-rys-token': process.env.RYS_API_TOKEN || '',
    'x-rys-actor': '',  // actor は呼び出し側で req.session.username から注入
    'Content-Type': 'application/json',
  };
}

// miniPC への proxy 呼び出し。fail-closed (token 未設定なら即 503)、HTML/auth redirect 区別。
async function callMiniPc(path, { method = 'GET', body, timeout = TIMEOUT_MS, actor = 'web' } = {}) {
  if (!process.env.RYS_API_TOKEN) {
    const err = new Error('RYS_API_TOKEN not configured on Render');
    err.statusCode = 503;
    throw err;
  }
  const url = `${MINIPC_URL}${path}`;
  const headers = { ...getMiniPcHeaders(), 'x-rys-actor': actor };
  const opts = { method, headers, redirect: 'manual', signal: AbortSignal.timeout(timeout) };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  // Cloudflare Access auth redirect = 302 → 認証設定ミス
  if (res.status >= 300 && res.status < 400) {
    const err = new Error(`miniPC redirected (${res.status}) — Cloudflare Access auth misconfigured?`);
    err.statusCode = 502;
    throw err;
  }
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch {
    const err = new Error(`miniPC returned non-JSON (status=${res.status}): ${text.slice(0, 200)}`);
    err.statusCode = 502;
    throw err;
  }
  if (res.status >= 400) {
    const err = new Error(json?.error || json?.message || `miniPC ${res.status}`);
    err.statusCode = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

// クライアント (UI) からの actor。session.username を優先、なければ 'web:anon'。
function actorFrom(req) {
  return (req.session && (req.session.displayName || req.session.username)) || 'web:anon';
}

// ───────────────── 画面 ─────────────────

router.get('/', (req, res) => {
  res.render('rakuten-yahoo-sync', {
    username: req.session?.username || '',
    displayName: req.session?.displayName || '',
    role: req.session?.role || '',
    minipcUrl: MINIPC_URL,
  });
});

// ───────────────── API proxy ─────────────────

// 読み取り系 ─ ミニPC RysServer に straight-through
router.get('/api/health', async (req, res) => {
  try {
    // miniPC は /health (auth 不要) を持つが、ここは authorized check 兼ねるので
    // migration-targets を limit=0 で叩いて 200 を確認するパターン
    const r = await callMiniPc('/rys/migration-targets?limit=1', { actor: actorFrom(req) });
    res.json({ ok: true, sample: r });
  } catch (e) {
    res.status(e.statusCode || 500).json({ ok: false, error: e.message });
  }
});

router.get('/api/migration-targets', async (req, res) => {
  try {
    const qs = new URLSearchParams(req.query).toString();
    const r = await callMiniPc(`/rys/migration-targets${qs ? '?' + qs : ''}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.get('/api/registered-items', async (req, res) => {
  try {
    const qs = new URLSearchParams(req.query).toString();
    const r = await callMiniPc(`/rys/registered-items${qs ? '?' + qs : ''}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.get('/api/needs-category', async (req, res) => {
  try {
    const qs = new URLSearchParams(req.query).toString();
    const r = await callMiniPc(`/rys/needs-category${qs ? '?' + qs : ''}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.get('/api/conversion-table', async (req, res) => {
  try {
    const qs = new URLSearchParams(req.query).toString();
    const r = await callMiniPc(`/rys/conversion-table${qs ? '?' + qs : ''}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.get('/api/audit', async (req, res) => {
  try {
    const qs = new URLSearchParams(req.query).toString();
    const r = await callMiniPc(`/rys/audit${qs ? '?' + qs : ''}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.get('/api/yahoo-cat/search', async (req, res) => {
  try {
    const qs = new URLSearchParams(req.query).toString();
    const r = await callMiniPc(`/rys/yahoo-cat/search${qs ? '?' + qs : ''}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.post('/api/category-manual/preview-impact', express.json({ limit: '32kb' }), async (req, res) => {
  try {
    const r = await callMiniPc('/rys/category-manual/preview-impact', {
      method: 'POST', body: req.body, actor: actorFrom(req),
    });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.post('/api/category-manual', express.json({ limit: '32kb' }), async (req, res) => {
  try {
    const r = await callMiniPc('/rys/category-manual', {
      method: 'POST', body: req.body, actor: actorFrom(req),
    });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.get('/api/batches/:id', async (req, res) => {
  try {
    const r = await callMiniPc(`/rys/batches/${encodeURIComponent(req.params.id)}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.get('/api/batches/:id/events', async (req, res) => {
  try {
    const qs = new URLSearchParams(req.query).toString();
    const r = await callMiniPc(`/rys/batches/${encodeURIComponent(req.params.id)}/events${qs ? '?' + qs : ''}`, { actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

// 書き込み系 ─ Render側でも idempotency_key を必須化 (UI から発行されるべき UUID v4)
router.post('/api/publish', express.json({ limit: '128kb' }), async (req, res) => {
  try {
    const { idempotency_key, item_codes } = req.body || {};
    if (!idempotency_key || !Array.isArray(item_codes) || item_codes.length === 0) {
      res.status(400).json({ error: 'idempotency_key and non-empty item_codes required' });
      return;
    }
    if (item_codes.length > 1000) {
      res.status(400).json({ error: 'item_codes too many (max 1000 per batch)' });
      return;
    }
    const r = await callMiniPc('/rys/publish', {
      method: 'POST',
      body: { idempotency_key, item_codes, created_by: actorFrom(req) },
      actor: actorFrom(req),
    });
    res.status(r.deduped ? 200 : 202).json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.post('/api/batches/:id/cancel', express.json({ limit: '32kb' }), async (req, res) => {
  try {
    const r = await callMiniPc(`/rys/batches/${encodeURIComponent(req.params.id)}/cancel`, {
      method: 'POST',
      body: { reason: req.body?.reason || 'web_cancel' },
      actor: actorFrom(req),
    });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

router.post('/api/dry-run', express.json({ limit: '128kb' }), async (req, res) => {
  try {
    const r = await callMiniPc('/rys/dry-run', { method: 'POST', body: req.body, actor: actorFrom(req) });
    res.json(r);
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

export default router;
