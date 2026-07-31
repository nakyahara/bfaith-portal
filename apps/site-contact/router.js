/**
 * site-contact — コーポレートサイト問い合わせ受付API
 *
 * bfaith-siteのPages Functions (/api/contact) がoutbox方式で配送してくる (設計書§11)。
 * 認証: POST /inquiries : SITE_CONTACT_SERVICE_TOKEN (Bearer)。未設定なら503 fail-closed。
 *       GET  /recent    : MIRROR_READ_TOKEN (既存read-only token、運用確認用)。
 * 冪等: 受付ID (id) をPKにINSERT OR IGNORE。重複配送は {ok:true, duplicate:true} で成功応答
 *       (site側の再送が安全に空振りできる)。
 * 通知: 新規のみGChat (SITE_CONTACT_GCHAT_WEBHOOK設定時)。受付ID+種別のみ・個人情報を流さない。
 * mount: session認証の外側 (server.jsでrequireAppAccessを通さない)。
 */
import express from 'express';
import crypto from 'node:crypto';
import { insertInquiry, recentInquiries } from './db.js';

const router = express.Router();

const CATEGORIES = new Set(['business', 'product', 'other']);
const MAX = { name: 100, email: 254, message: 5000, id: 64 };

function timingSafeEq(got, expected) {
  const a = Buffer.from(got, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function requireServiceToken(req, res, next) {
  const expected = process.env.SITE_CONTACT_SERVICE_TOKEN || '';
  if (!expected) {
    return res.status(503).json({ ok: false, error: 'site_contact_service_token_unset' });
  }
  const m = /^Bearer\s+(\S+)$/.exec(String(req.headers.authorization || ''));
  if (!m || !timingSafeEq(m[1], expected)) {
    console.warn('[site-contact] auth failed', { ip: req.ip });
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  next();
}

function requireReadToken(req, res, next) {
  const expected = process.env.MIRROR_READ_TOKEN || '';
  if (!expected) return res.status(503).json({ ok: false, error: 'mirror_read_token_unset' });
  const got = String(req.headers['x-read-token'] || '');
  if (!got || !timingSafeEq(got, expected)) {
    return res.status(401).json({ ok: false, error: 'invalid_read_token' });
  }
  next();
}

function hasControl(s) {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 32 && c !== 10 && c !== 13 && c !== 9) return true;
  }
  return false;
}
function cleanStr(v, name, max, { required = true, multiline = false } = {}) {
  if (v == null || v === '') {
    if (required) throw new Error(`${name} は必須`);
    return '';
  }
  if (typeof v !== 'string') throw new Error(`${name} は文字列で指定`);
  const s = v.normalize('NFC').trim();
  if (!multiline && /[\r\n]/.test(s)) throw new Error(`${name} に改行は不可`);
  if (hasControl(s)) throw new Error(`${name} に制御文字が含まれる`);
  if (s.length > max) throw new Error(`${name} が長すぎる (${max}文字まで)`);
  return s;
}

async function notifyGchat(id, category) {
  const url = process.env.SITE_CONTACT_GCHAT_WEBHOOK || '';
  if (!url) return;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        text: `📩 *サイト問い合わせ* 受付ID: ${id} / 種別: ${category}\nポータルの受信記録を確認してください`,
      }),
    });
    if (!res.ok) console.warn('[site-contact] GChat通知失敗', res.status);
  } catch (e) {
    console.warn('[site-contact] GChat通知エラー', e.message);
  }
}

// POST /inquiries — outbox配送の受け口 (冪等)
router.post('/inquiries', requireServiceToken, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  let row;
  try {
    const b = req.body ?? {};
    const id = cleanStr(b.id, 'id', MAX.id);
    if (!/^[0-9A-Za-z_-]{8,64}$/.test(id)) throw new Error('id の形式が不正');
    const category = cleanStr(b.category, 'category', 32);
    if (!CATEGORIES.has(category)) throw new Error('category が不正');
    row = {
      id,
      category,
      name: cleanStr(b.name, 'name', MAX.name),
      email: cleanStr(b.email, 'email', MAX.email),
      message: cleanStr(b.message, 'message', MAX.message, { multiline: true }),
      client_ip: cleanStr(b.clientIp, 'clientIp', 64, { required: false }) || null,
      site_sent_at: cleanStr(b.sentAt, 'sentAt', 40, { required: false }) || null,
      received_at: new Date().toISOString(),
    };
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(row.email)) throw new Error('email の形式が不正');
  } catch (e) {
    return res.status(400).json({ ok: false, error: 'validation', message: String(e.message) });
  }

  try {
    const inserted = insertInquiry(row);
    if (inserted) {
      console.log('[site-contact] 受付', { id: row.id, category: row.category });
      // 通知はレスポンスをブロックしない (失敗しても受付は成立、DB保存が正)
      notifyGchat(row.id, row.category);
    }
    return res.json({ ok: true, duplicate: !inserted });
  } catch (e) {
    console.error('[site-contact] DB error:', e);
    return res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

// GET /recent — 運用確認用 (read-only token)
router.get('/recent', requireReadToken, (req, res) => {
  res.set('Cache-Control', 'no-store');
  try {
    return res.json({ ok: true, inquiries: recentInquiries(50) });
  } catch (e) {
    console.error('[site-contact] recent error:', e);
    return res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

export default router;
