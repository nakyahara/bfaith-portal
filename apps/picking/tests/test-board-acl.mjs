/**
 * picking — 掲示端末 (kind='board') のアクセス制御 (HTTP結合テスト)。
 *
 * 倉庫に常時表示するモニターは誰でも物理的に触れるので、実績ボード以外は開けない
 * 読み取り専用端末でなければならない。特に「その端末で誰かがログインしたら作業できる」
 * 穴が開いていないことを回帰テストで固定する (Codexレビュー high)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));
process.env.PICKING_STATS_MIN_DATE = '2026-08-15';

const { initPickingDB, createDevice } = await import('../db.js');
initPickingDB();
const workerToken = createDevice('倉庫iPhone1', 'admin@b-faith.biz', 'worker');
const boardToken = createDevice('倉庫モニター', 'admin@b-faith.biz', 'board');

const router = (await import('../router.js')).default;

// セッションは差し替え可能なスタブ (express-session を持ち込まない)
let session = null;
const app = express();
app.set('view engine', 'ejs');   // router は res.render(絶対パス) を使う
app.use(express.json());
app.use((req, res, next) => { req.session = session ? { ...session, destroy: (cb) => cb() } : {}; next(); });
app.use('/apps/picking', router);

const server = app.listen(0, '127.0.0.1');
await new Promise((r) => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/picking`;

async function req(method, url, { token, body } = {}) {
  const res = await fetch(base + url, {
    method,
    redirect: 'manual',
    headers: {
      ...(token ? { cookie: `pk_device=${token}` } : {}),
      ...(body ? { 'content-type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { status: res.status, text: await res.text(), headers: res.headers };
}

let passed = 0;
async function t(name, fn) { await fn(); passed++; console.log(`  ok: ${name}`); }

await t('掲示端末: ボードと board API は開ける', async () => {
  const board = await req('GET', '/board', { token: boardToken });
  assert.equal(board.status, 200);
  assert.ok(board.text.includes('速さランキング'));

  const api = await req('GET', '/api/board', { token: boardToken });
  assert.equal(api.status, 200);
  assert.equal(JSON.parse(api.text).ok, true);
});

await t('掲示端末: 作業画面・一覧・作業APIは403 (書き込ませない)', async () => {
  assert.equal((await req('GET', '/', { token: boardToken })).status, 403);
  assert.equal((await req('GET', '/work/1', { token: boardToken })).status, 403);
  assert.equal((await req('GET', '/batches/1', { token: boardToken })).status, 403);

  const post = await req('POST', '/api/batches/1/events', {
    token: boardToken, body: { op_id: 'x', event: 'start' },
  });
  assert.equal(post.status, 403);
  assert.equal(JSON.parse(post.text).code, 'board_only');
});

await t('⭐掲示端末は管理者ログイン中でも閉じたまま (セッションで穴が開かない)', async () => {
  session = { email: 'admin@b-faith.biz', role: 'admin', allowedApps: '*' };
  try {
    assert.equal((await req('GET', '/', { token: boardToken })).status, 403, '一覧も開かない');
    assert.equal((await req('GET', '/work/1', { token: boardToken })).status, 403);
    assert.equal((await req('GET', '/admin/stats', { token: boardToken })).status, 403);
    const post = await req('POST', '/api/batches/1/events', {
      token: boardToken, body: { op_id: 'y', event: 'start' },
    });
    assert.equal(post.status, 403, '作業APIも塞がったまま');
    // 掲示端末Cookieを持たないブラウザなら、同じセッションで管理画面に入れる
    assert.equal((await req('GET', '/admin/stats')).status, 200);
  } finally {
    session = null;
  }
});

await t('掲示モードの解除口がある (Cookie削除)', async () => {
  const res = await req('GET', '/board/exit', { token: boardToken });
  assert.equal(res.status, 200);
  const setCookie = res.headers.get('set-cookie') || '';
  assert.ok(/pk_device=/.test(setCookie), 'Cookieを消しにいく');
});

await t('作業端末は従来どおり (一覧・作業画面OK / ボードも見られる)', async () => {
  assert.equal((await req('GET', '/', { token: workerToken })).status, 200);
  assert.equal((await req('GET', '/board', { token: workerToken })).status, 200);
  assert.equal((await req('GET', '/api/board', { token: workerToken })).status, 200);
  // 管理画面は端末Cookieだけでは開けない (従来どおり)
  assert.equal((await req('GET', '/admin/stats', { token: workerToken })).status, 403);
});

await t('未登録端末はログインへ / APIは401', async () => {
  assert.equal((await req('GET', '/board')).status, 302);
  assert.equal((await req('GET', '/api/board')).status, 401);
});

server.close();
console.log(`\ntest-board-acl: ${passed} 件 pass`);
