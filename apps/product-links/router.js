/**
 * 商品リンク台帳 (product-links) — 画面 + API
 *
 * 閲覧 = このアプリ (または product-hub) にアクセスできる人全員。
 * 編集 (追加・用途/ラベル・主リンク・非表示・削除) = admin か product-hub を使える人 (要件定義 §7)。
 * 更新系 API は inquiry-hub と同じ CSRF 二段ガード (Origin があれば自ホスト一致必須 + JSON Content-Type 必須)。
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  getDB, searchProducts, getLink, upsertLink, patchLink, softDeleteLink, stats, analyzeUrl,
  LINK_TYPES, LINK_TYPE_LABELS, PURPOSES, PURPOSE_LABELS, SOURCE_LABELS, normalizeCode, loadCatalog,
} from './db.js';
import { runReconcile } from './cron.js';
import adminRouter from './admin-router.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();
const view = (name) => path.join(__dirname, 'views', name);

// ─── CSRF 二段ガード (更新系だけ。/api/ と /admin/api/ の両方) ───
router.use(['/api/', '/admin/api/'], (req, res, next) => {
  if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
  const origin = req.headers.origin;
  if (origin) {
    let host = null;
    try { host = new URL(origin).host; } catch { /* 不正 Origin は不一致として拒否 */ }
    if (!host || host !== req.headers.host) return res.status(403).json({ ok: false, error: 'origin_mismatch' });
  }
  if (!/^application\/json\b/i.test(String(req.headers['content-type'] || ''))) {
    return res.status(415).json({ ok: false, error: 'Content-Type は application/json にしてください' });
  }
  next();
});
router.use(express.json({ limit: '1mb' })); // CSV 貼り付け取込 (admin) があるので 1MB
// 取込 (admin 専用): 候補表・Drive 走査・Notion・CSV
router.use('/admin', adminRouter);

function canEdit(req) {
  const s = req.session || {};
  if (s.role === 'admin') return true;
  const allowed = s.allowedApps;
  return allowed === '*' || (Array.isArray(allowed) && allowed.includes('product-hub'));
}
function requireEdit(req, res) {
  if (canEdit(req)) return true;
  res.status(403).json({ ok: false, error: '編集できるのは 商品登録ハブ を使える人か管理者だけです' });
  return false;
}
function actorOf(req) {
  return req.session?.email || req.session?.displayName || 'unknown';
}
function cleanText(v, max) {
  if (v === undefined || v === null) return null;
  const s = String(v).replace(/[\x00-\x1f\x7f]/g, '').trim();
  return s ? s.slice(0, max) : null;
}
/** http/https だけ・資格情報つき URL は拒否 (正規表現でなく URL パーサで判定) */
function isHttpUrl(u) {
  try {
    const p = new URL(String(u || ''));
    return (p.protocol === 'http:' || p.protocol === 'https:') && !p.username && !p.password && !!p.hostname;
  } catch { return false; }
}
/** 想定済みの入力エラーだけ文言を返し、それ以外はログに残して丸める (Codex PR1 R1 L9) */
function apiError(res, e, where) {
  if (e?.code === 'VALIDATION') return res.status(400).json({ ok: false, error: e.message });
  console.error(`[product-links] ${where}:`, e);
  return res.status(500).json({ ok: false, error: 'サーバーエラーが発生しました' });
}
function parseKind(v) { return v === 'set' ? true : (v === 'single' ? false : null); }

router.get('/', (req, res) => {
  const db = getDB();
  const q = cleanText(req.query.q, 200) || '';
  const onlyMissing = req.query.missing === '1';
  const result = searchProducts(db, { q, onlyMissing, onlySet: parseKind(req.query.kind), limit: 100 });
  res.render(view('index.ejs'), {
    title: '商品リンク台帳',
    displayName: req.session?.displayName || req.session?.email || '',
    canEdit: canEdit(req),
    isAdmin: req.session?.role === 'admin',
    q, onlyMissing, kind: ['set', 'single'].includes(req.query.kind) ? req.query.kind : '',
    result, stats: stats(db),
    linkTypes: LINK_TYPES, linkTypeLabels: LINK_TYPE_LABELS,
    purposes: PURPOSES, purposeLabels: PURPOSE_LABELS, sourceLabels: SOURCE_LABELS,
  });
});

router.get('/api/search', (req, res) => {
  const db = getDB();
  const q = cleanText(req.query.q, 200) || '';
  const onlyMissing = req.query.missing === '1';
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 500);
  res.json({ ok: true, ...searchProducts(db, { q, onlyMissing, onlySet: parseKind(req.query.kind), limit }) });
});

// 手入力でリンクを足す (由来 = manual)。存在しない商品コードは拒否 (誤コードの行を作らない)
router.post('/api/links', (req, res) => {
  if (!requireEdit(req, res)) return;
  const db = getDB();
  const b = req.body || {};
  const neCode = normalizeCode(b.ne_code);
  const url = cleanText(b.url, 1000);
  if (!neCode) return res.status(400).json({ ok: false, error: '商品コードを入力してください' });
  if (!url || !isHttpUrl(url)) return res.status(400).json({ ok: false, error: 'URLの形式が不正です (http/https)' });
  const linkType = LINK_TYPES.includes(b.link_type) ? b.link_type : (analyzeUrl(url)?.link_type_hint || 'other');
  const purpose = b.purpose ? String(b.purpose) : null;
  if (purpose && !PURPOSES.includes(purpose)) return res.status(400).json({ ok: false, error: '用途の値が不正です' });
  const product = loadCatalog(db).find((r) => r.code === neCode);
  if (!product) return res.status(400).json({ ok: false, error: `商品コード ${neCode} は商品マスタにありません (コードを確認してください)` });
  try {
    const r = db.transaction(() => upsertLink(db, {
      neCode, linkType, purpose, url, label: cleanText(b.label, 100), productName: product.name,
      source: 'manual', sourceEntityId: actorOf(req), createdBy: actorOf(req),
    }))();
    res.json({ ok: true, id: r.id, created: r.created, link: getLink(db, r.id) });
  } catch (e) {
    apiError(res, e, 'POST /api/links');
  }
});

router.patch('/api/links/:id', (req, res) => {
  if (!requireEdit(req, res)) return;
  const db = getDB();
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'id が不正です' });
  const b = req.body || {};
  const patch = {};
  if (b.is_primary !== undefined) patch.is_primary = !!b.is_primary;
  if (b.label !== undefined) patch.label = cleanText(b.label, 100);
  if (b.purpose !== undefined) patch.purpose = b.purpose ? String(b.purpose) : null;
  if (b.hidden !== undefined) patch.hidden = !!b.hidden;
  try {
    if (!patchLink(db, id, patch)) return res.status(404).json({ ok: false, error: 'リンクが見つかりません' });
    res.json({ ok: true, link: getLink(db, id) });
  } catch (e) {
    apiError(res, e, 'PATCH /api/links');
  }
});

router.delete('/api/links/:id', (req, res) => {
  if (!requireEdit(req, res)) return;
  const db = getDB();
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ ok: false, error: 'id が不正です' });
  try {
    if (!softDeleteLink(db, id, actorOf(req))) return res.status(404).json({ ok: false, error: 'リンクが見つかりません' });
    res.json({ ok: true });
  } catch (e) {
    apiError(res, e, 'DELETE /api/links');
  }
});

// 照合を今すぐ (admin)。夜間 cron と同じ処理
router.post('/api/reconcile', (req, res) => {
  if (req.session?.role !== 'admin') return res.status(403).json({ ok: false, error: 'admin のみ操作できます' });
  const r = runReconcile(`manual:${actorOf(req)}`);
  res.status(r.ok ? 200 : 500).json({ ok: r.ok, drafts: r.drafts, upserted: r.upserted, detached: r.detached, failed: r.failed, error: r.ok ? undefined : '一部のドラフトで照合に失敗しました (Render ログを確認)' });
});

export default router;
