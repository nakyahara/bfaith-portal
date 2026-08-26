/**
 * shohyo-links — MF仕訳用 証憑リンク集
 * UI: views/index.html (自己完結)。API: /api/links CRUD ({ ok, result } / { ok, error } 形式)。
 * 認証は server.js の requireAppAccess('shohyo-links') に委譲。
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { listLinks, createLink, updateLink, deleteLink } from './db.js';

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

export default router;
