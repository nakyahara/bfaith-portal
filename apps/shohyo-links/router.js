/**
 * shohyo-links — MF仕訳用 証憑リンク集
 * UI: views/index.html (自己完結)。API: /api/links CRUD ({ ok, result } / { ok, error } 形式)。
 * 認証は server.js の requireAppAccess('shohyo-links') に委譲。
 */
import { Router } from 'express';
import crypto from 'node:crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import { listLinks, createLink, updateLink, deleteLink } from './db.js';
import {
  mfConfigured, authorizeUrl, exchangeCode, loadTokens, clearTokens,
  currentOffice, getJournals, postVoucher, matchVendors, revokeTokens, journalDigest,
} from './mf-api.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

router.get('/', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  const query = qIdx === -1 ? '' : req.originalUrl.slice(qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + query);
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

router.get('/api/links', (req, res) => {
  try {
    res.json({ ok: true, result: listLinks() });
  } catch (e) {
    console.error('[shohyo-links] list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.post('/api/links', (req, res) => {
  try {
    res.json({ ok: true, result: createLink(req.body || {}) });
  } catch (e) {
    if (e.message === 'name_required') return res.status(400).json({ ok: false, error: 'name_required' });
    console.error('[shohyo-links] create', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.patch('/api/links/:id', (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'bad_id' });
  try {
    const row = updateLink(id, req.body || {});
    if (!row) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, result: row });
  } catch (e) {
    if (e.message === 'name_required') return res.status(400).json({ ok: false, error: 'name_required' });
    console.error('[shohyo-links] update', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.delete('/api/links/:id', (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'bad_id' });
  try {
    if (!deleteLink(id)) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, result: { deleted: id } });
  } catch (e) {
    console.error('[shohyo-links] delete', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ---- MF会計API連携 (Phase 2) ----

router.get('/mf', (req, res) => {
  // 末尾スラッシュ (/mf/) だと画面内の相対fetch (api/mf/...) が 1階層ずれるため /mf に寄せる
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (pathname.endsWith('/')) {
    return res.redirect(308, `${req.baseUrl}/mf` + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  }
  res.sendFile(path.join(__dirname, 'views', 'mf.html'));
});

// OAuth開始: MFの認可画面へリダイレクト
router.get('/mf/connect', (req, res) => {
  if (!mfConfigured()) return res.status(500).send('MF_CLIENT_ID / MF_CLIENT_SECRET が未設定です (Render環境変数)');
  const state = crypto.randomBytes(16).toString('hex');
  req.session.mfOauthState = state;
  res.redirect(authorizeUrl(state));
});

// OAuthコールバック
router.get('/mf/callback', async (req, res) => {
  // 相対リダイレクト ('mf?...') は /apps/shohyo-links/mf/callback を基準に解決され /mf/mf になるため絶対パスで返す
  const backToMf = (q) => res.redirect(`${req.baseUrl}/mf${q}`);
  try {
    const { code, state, error } = req.query;
    if (error) return backToMf('?error=' + encodeURIComponent(String(error)));
    if (!code || !state || state !== req.session.mfOauthState) {
      return backToMf('?error=state_mismatch');
    }
    delete req.session.mfOauthState;
    await exchangeCode(String(code));
    backToMf('?connected=1');
  } catch (e) {
    console.error('[shohyo-links] mf callback', e.message, e.detail || '');
    backToMf('?error=' + encodeURIComponent(e.message));
  }
});

router.get('/api/mf/status', async (req, res) => {
  try {
    if (!mfConfigured()) return res.json({ ok: true, result: { configured: false, connected: false } });
    if (!loadTokens()) return res.json({ ok: true, result: { configured: true, connected: false } });
    let office = null;
    try { office = await currentOffice(); } catch (e) {
      if (e.message === 'mf_not_connected' || String(e.message).includes('invalid_grant')) {
        return res.json({ ok: true, result: { configured: true, connected: false, reason: 'token_expired' } });
      }
      throw e;
    }
    res.json({ ok: true, result: { configured: true, connected: true, office } });
  } catch (e) {
    console.error('[shohyo-links] mf status', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.post('/api/mf/disconnect', async (req, res) => {
  // MF側でも失効させてから手元のトークンを消す (残しておくと第三者が使える状態が続く)
  const revoke = await revokeTokens();
  clearTokens();
  res.json({ ok: true, result: { disconnected: true, ...revoke } });
});

// 期間内の仕訳を取得し、証憑未添付を支払い先マスタと突合して返す
router.get('/api/mf/unattached', async (req, res) => {
  try {
    const { start, end, all } = req.query;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(start)) || !/^\d{4}-\d{2}-\d{2}$/.test(String(end))) {
      return res.status(400).json({ ok: false, error: 'bad_period' });
    }
    const vendors = listLinks();
    const journals = await getJournals(String(start), String(end));
    const rows = journals
      .filter(j => all === '1' || !(j.voucher_file_ids || []).length)
      .map(j => ({
        id: j.id,
        number: j.number,
        date: j.transaction_date,
        memo: j.memo || '',
        vouchers: (j.voucher_file_ids || []).length,
        ...journalDigest(j),
        vendors: matchVendors(j, vendors).map(v => ({
          id: v.id, name: v.name, url: v.url, storage_path: v.storage_path, fetch_method: v.fetch_method,
        })),
      }));
    res.json({ ok: true, result: { total: journals.length, rows } });
  } catch (e) {
    console.error('[shohyo-links] mf unattached', e.message, e.detail || '');
    res.status(e.message === 'mf_not_connected' ? 401 : 500).json({ ok: false, error: e.message });
  }
});

// 証憑ファイルを仕訳に添付 (journal_id 無しならBoxへ未添付保存)
router.post('/api/mf/attach', async (req, res) => {
  try {
    const { journal_id, file_name, file_data } = req.body || {};
    if (!file_name || !file_data) return res.status(400).json({ ok: false, error: 'file_required' });
    // MF証憑API仕様 (developers /specs/vouchers): 1件5MB・名称255文字・1仕訳あたり5件まで
    if (String(file_name).length > 255) return res.status(400).json({ ok: false, error: 'file_name_too_long' });
    if (decodedSize_(String(file_data)) > 5 * 1024 * 1024) return res.status(413).json({ ok: false, error: 'file_too_large_5mb' });
    const result = await postVoucher(journal_id || null, String(file_name), String(file_data));
    res.json({ ok: true, result });
  } catch (e) {
    console.error('[shohyo-links] mf attach', e.message, e.detail || '');
    const status = e.message === 'mf_not_connected' ? 401 : (e.status === 413 ? 413 : 500);
    res.status(status).json({ ok: false, error: e.message });
  }
});

/** base64文字列から元ファイルのバイト数を求める (パディング考慮) */
function decodedSize_(b64) {
  const s = b64.replace(/=+$/, '');
  return Math.floor(s.length * 3 / 4);
}

export default router;
