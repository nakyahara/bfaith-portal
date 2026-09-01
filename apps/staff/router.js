/**
 * スタッフマスタ (apps/staff) — router (Render)
 *
 * URL: /apps/staff/                 管理画面 (管理者のみ)
 *      /apps/staff/api/*            管理 API (管理者のみ)
 *      /apps/staff/export           他マシン (miniPC の picking/packing 同期) 向け読み取り専用。
 *                                   Authorization: Bearer <STAFF_EXPORT_TOKEN>。env 未設定なら 404
 *
 * 同一プロセスの他アプリ (inbound-check 等) は HTTP でなく ./db.js を直接 import して読む。
 */
import { Router } from 'express';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  listStaff, getStaff, createStaff, updateStaff, setStaffActive, listAudit, listTapCandidates,
  STAFF_KINDS, STAFF_KIND_LABELS,
} from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();
const BASE = '/apps/staff';

function checkOrigin(req, res, next) {
  const reject = () => res.status(403).json({ ok: false, error: 'bad_origin', message: '不正なオリジンからのリクエストです' });
  const src = req.headers.origin || req.headers.referer;
  if (!src || src === 'null') return reject();
  let host = null;
  try { host = new URL(src).host; } catch { /* 不正値は下で403 */ }
  if (!host || host !== req.headers.host) return reject();
  next();
}

function requireAdmin(req, res, next) {
  if (!req.session?.authenticated) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ ok: false, error: 'session_expired' });
    if (req.session) req.session.returnTo = req.originalUrl;
    return res.redirect('/login');
  }
  if (req.session.role !== 'admin') {
    if (req.path.startsWith('/api/')) return res.status(403).json({ ok: false, error: 'forbidden', message: '管理者のみ' });
    return res.status(403).render('forbidden', { username: req.session.email, displayName: req.session.displayName });
  }
  next();
}

const api = fn => (req, res) => {
  try { fn(req, res); } catch (e) {
    console.error(`[staff] ${req.method} ${req.path}`, e.message);
    res.status(400).json({ ok: false, error: 'bad_request', message: e.message });
  }
};

// ─── 他マシン向け読み取り専用 (トークン) ───
// 定数時間比較。env 未設定のときは経路ごと無い扱い (404) にして、設定漏れで公開されないようにする
router.get('/export', (req, res) => {
  const expected = process.env.STAFF_EXPORT_TOKEN;
  if (!expected) return res.status(404).end();
  const m = /^Bearer\s+(\S+)$/.exec(req.headers.authorization || '');
  const given = m ? m[1] : '';
  // 固定長ハッシュ同士を比較 (長さ分岐でトークン長を漏らさない — Codex R4 Low)
  const h = s => crypto.createHash('sha256').update(String(s)).digest();
  if (!crypto.timingSafeEqual(h(given), h(expected))) return res.status(401).json({ ok: false, error: 'unauthorized' });
  res.setHeader('Cache-Control', 'no-store');
  res.json({ ok: true, generated_at: new Date().toISOString(), staff: listStaff({ includeInactive: true }).map(s => ({
    id: s.id, staff_no: s.staff_no, display_name: s.display_name, short_name: s.short_name, kind: s.kind,
    portal_email: s.portal_email, active: s.active, sort: s.sort, updated_at: s.updated_at, version: s.version,
  })) });
});

router.use(requireAdmin);

router.get('/', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  res.render(path.join(__dirname, 'views/admin'), {
    title: 'スタッフマスタ',
    username: req.session.email,
    displayName: req.session.displayName,
    base: BASE,
    staff: listStaff({ includeInactive: true }),
    kinds: STAFF_KINDS,
    kindLabels: STAFF_KIND_LABELS,
    exportEnabled: !!process.env.STAFF_EXPORT_TOKEN,
  });
});

router.get('/api/list', api((req, res) => {
  res.json({ ok: true, staff: listStaff({ includeInactive: req.query.all === '1' }), candidates: listTapCandidates() });
}));

router.get('/api/staff/:id(\\d+)', api((req, res) => {
  const s = getStaff(req.params.id);
  if (!s) return res.status(404).json({ ok: false, error: 'not_found' });
  res.json({ ok: true, staff: s, audit: listAudit(s.id) });
}));

router.post('/api/staff', checkOrigin, api((req, res) => {
  res.json({ ok: true, staff: createStaff(req.body || {}, req.session.email) });
}));

const statusOf = r => (r.error === 'not_found' ? 404 : r.error === 'conflict' ? 409 : 400);

// expect_version は必須の正整数 (省略で楽観ロックを迂回させない — Codex R4 High)
router.post('/api/staff/:id(\\d+)', checkOrigin, api((req, res) => {
  const { fields, expect_version } = req.body || {};
  if (!Number.isSafeInteger(expect_version) || expect_version < 1) return res.status(400).json({ ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' });
  if (fields != null && typeof fields !== 'object') return res.status(400).json({ ok: false, error: 'bad_request', message: 'fields が不正です' });
  const r = updateStaff(req.params.id, fields || {}, req.session.email, expect_version);
  if (!r.ok) return res.status(statusOf(r)).json(r);
  res.json(r);
}));

router.post('/api/staff/:id(\\d+)/active', checkOrigin, api((req, res) => {
  const { active, expect_version, left_on } = req.body || {};
  if (typeof active !== 'boolean') return res.status(400).json({ ok: false, error: 'bad_request', message: 'active は true/false で指定してください' });
  if (!Number.isSafeInteger(expect_version) || expect_version < 1) return res.status(400).json({ ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' });
  const r = setStaffActive(req.params.id, active, req.session.email, { leftOn: left_on || null, expectVersion: expect_version });
  if (!r.ok) return res.status(statusOf(r)).json(r);
  res.json(r);
}));

export default router;
