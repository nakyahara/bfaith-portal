/**
 * site-contact API の契約テスト (bfaith-site Pages Functions outbox とのデータ契約)
 *
 * 実行: node scripts/test-site-contact.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'site-contact-test-'));
process.env.DATA_DIR = tmpDir;
delete process.env.SITE_CONTACT_SERVICE_TOKEN;
delete process.env.SITE_CONTACT_GCHAT_WEBHOOK;
process.env.MIRROR_READ_TOKEN = 'read-token';

const { default: router } = await import('../apps/site-contact/router.js');
const app = express();
app.use('/apps/site-contact/api', express.json({ limit: '64kb' }), router);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}/apps/site-contact/api`;

let pass = 0;
let fail = 0;
function ok(name, cond, detail = '') {
  if (cond) {
    pass++;
    console.log(`  ✓ ${name}`);
  } else {
    fail++;
    console.error(`  ✗ ${name} ${detail}`);
  }
}
const VALID = {
  id: '01SAMPLEINQ0000000000001',
  category: 'business',
  name: 'テスト太郎',
  email: 'test@example.com',
  message: 'お問い合わせのテスト本文です。\n複数行もOK。',
  sentAt: '2026-08-01T00:00:00.000Z',
};
async function post(body, headers = {}) {
  return fetch(`${base}/inquiries`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
}

// 1. 認証
{
  const r = await post(VALID);
  ok('token未設定 → 503', r.status === 503, `got ${r.status}`);
}
process.env.SITE_CONTACT_SERVICE_TOKEN = 'svc-token';
const AUTH = { authorization: 'Bearer svc-token' };
{
  const r = await post(VALID, { authorization: 'Bearer wrong' });
  ok('token不一致 → 401', r.status === 401, `got ${r.status}`);
  const r2 = await post(VALID, { 'x-read-token': 'svc-token' });
  ok('Bearer以外 → 401', r2.status === 401, `got ${r2.status}`);
}

// 2. 受付+冪等
{
  const r = await post(VALID, AUTH);
  const b = await r.json();
  ok('新規受付 → 200 duplicate=false', r.status === 200 && b.ok && b.duplicate === false, JSON.stringify(b));
  const r2 = await post(VALID, AUTH);
  const b2 = await r2.json();
  ok('同一idの再送 → duplicate=true (冪等)', r2.status === 200 && b2.duplicate === true, JSON.stringify(b2));
}

// 3. バリデーション
{
  const cases = [
    ['id不正', { ...VALID, id: 'bad id!' }],
    ['category不正', { ...VALID, id: '01SAMPLEINQ0000000000002', category: 'spam' }],
    ['email不正', { ...VALID, id: '01SAMPLEINQ0000000000003', email: 'not-an-email' }],
    ['name改行不可', { ...VALID, id: '01SAMPLEINQ0000000000004', name: 'a\nb' }],
    ['message長すぎ', { ...VALID, id: '01SAMPLEINQ0000000000005', message: 'あ'.repeat(5001) }],
    ['message欠落', { id: '01SAMPLEINQ0000000000006', category: 'other', name: 'x', email: 'a@b.jp' }],
  ];
  for (const [name, body] of cases) {
    const r = await post(body, AUTH);
    ok(`${name} → 400`, r.status === 400, `got ${r.status}`);
  }
}

// 4. recent (read token)
{
  const r = await fetch(`${base}/recent`);
  ok('recent: tokenなし → 401', r.status === 401, `got ${r.status}`);
  const r2 = await fetch(`${base}/recent`, { headers: { 'x-read-token': 'read-token' } });
  const b2 = await r2.json();
  ok('recent: 1件 (重複は保存されない)', r2.status === 200 && b2.inquiries.length === 1, JSON.stringify(b2.inquiries?.length));
  ok('recentに本文が保存されている', b2.inquiries?.[0]?.message?.includes('テスト本文'));
}

server.close();
console.log(`\n結果: pass=${pass} fail=${fail}`);
process.exit(fail === 0 ? 0 : 1);
