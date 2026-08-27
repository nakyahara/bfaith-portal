/**
 * 商品リンク台帳 — 取込 (admin 専用)。/apps/product-links/admin 配下。
 * CSRF ガード・JSON parser は親 router (router.js) の /api/ ミドルウェアが効く (このファイルは /admin/api/... に mount)。
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getDB, PURPOSES, PURPOSE_LABELS, LINK_TYPE_LABELS, SOURCE_LABELS } from './db.js';
import {
  listCandidates, candidateCounts, acceptCandidate, rejectCandidate, acceptAllExact, addCandidates, parseCsvItems, newBatchId,
} from './candidates.js';
import { scanDriveProductFolders, importNotionImageDb, driveFolderId } from './import-sources.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();
const view = (name) => path.join(__dirname, 'views', name);

router.use((req, res, next) => {
  if (req.session?.role === 'admin') return next();
  if (req.path.startsWith('/api/')) return res.status(403).json({ ok: false, error: 'admin のみ操作できます' });
  return res.status(403).send('admin のみ');
});

function actorOf(req) { return req.session?.email || req.session?.displayName || 'admin'; }
function apiError(res, e, where) {
  if (e?.code === 'VALIDATION') return res.status(400).json({ ok: false, error: e.message });
  console.error(`[product-links] ${where}:`, e);
  return res.status(500).json({ ok: false, error: String(e?.message || 'サーバーエラー').slice(0, 300) });
}

// 同時実行ガード (Drive / Notion の照会が重なると API 上限とバッチ重複の元)
let running = null;
async function runExclusive(name, fn) {
  if (running) { const e = new Error(`${running} を実行中です。終わってからもう一度押してください`); e.code = 'VALIDATION'; throw e; }
  running = name;
  try { return await fn(); } finally { running = null; }
}

router.get('/', (req, res) => {
  const db = getDB();
  const resolution = ['pending', 'accepted', 'rejected', 'duplicate', 'all'].includes(req.query.r) ? req.query.r : 'pending';
  const source = ['drive_scan', 'notion_image', 'csv'].includes(req.query.s) ? req.query.s : null;
  res.render(view('admin.ejs'), {
    title: '商品リンク台帳 取込',
    displayName: req.session?.displayName || req.session?.email || '',
    candidates: listCandidates(db, { resolution, source, limit: 500 }),
    counts: candidateCounts(db), resolution, source,
    driveFolderId: driveFolderId(),
    purposes: PURPOSES, purposeLabels: PURPOSE_LABELS, linkTypeLabels: LINK_TYPE_LABELS, sourceLabels: SOURCE_LABELS,
  });
});

router.post('/api/scan-drive', async (req, res) => {
  try {
    const r = await runExclusive('Drive走査', () => scanDriveProductFolders(getDB(), { actor: actorOf(req) }));
    res.json({ ok: true, ...r });
  } catch (e) { apiError(res, e, 'scan-drive'); }
});

router.post('/api/import-notion', async (req, res) => {
  try {
    const r = await runExclusive('Notion取込', () => importNotionImageDb(getDB(), { actor: actorOf(req) }));
    res.json({ ok: true, ...r });
  } catch (e) { apiError(res, e, 'import-notion'); }
});

router.post('/api/import-csv', (req, res) => {
  try {
    const items = parseCsvItems(req.body?.text);
    if (items.length === 0) return res.status(400).json({ ok: false, error: 'データ行がありません' });
    if (items.length > 5000) return res.status(400).json({ ok: false, error: '1 回の取込は 5,000 行までです' });
    const r = addCandidates(getDB(), { batchId: newBatchId('csv'), source: 'csv', items, actor: actorOf(req) });
    res.json({ ok: true, rows: items.length, ...r });
  } catch (e) { apiError(res, e, 'import-csv'); }
});

router.post('/api/candidates/:id/accept', (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'id が不正です' });
  try {
    const b = req.body || {};
    const r = acceptCandidate(getDB(), id, {
      neCode: b.ne_code, purpose: b.purpose !== undefined ? (b.purpose || null) : undefined, label: b.label || null, actor: actorOf(req),
    });
    if (!r) return res.status(404).json({ ok: false, error: '候補が見つかりません' });
    res.json({ ok: true, ...r });
  } catch (e) { apiError(res, e, 'accept'); }
});

router.post('/api/candidates/:id/reject', (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'id が不正です' });
  if (!rejectCandidate(getDB(), id, { actor: actorOf(req) })) return res.status(404).json({ ok: false, error: '未処理の候補が見つかりません' });
  res.json({ ok: true });
});

router.post('/api/accept-all-exact', (req, res) => {
  const source = ['drive_scan', 'notion_image', 'csv'].includes(req.body?.source) ? req.body.source : null;
  res.json({ ok: true, ...acceptAllExact(getDB(), { actor: actorOf(req), source }) });
});

export default router;
