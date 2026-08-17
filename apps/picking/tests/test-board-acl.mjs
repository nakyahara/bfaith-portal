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
let sessionDestroyed = false;
let destroyFails = false;   // セッションストア障害の再現用
const app = express();
app.set('view engine', 'ejs');   // router は res.render(絶対パス) を使う
app.use(express.json());
app.use((req, res, next) => {
  req.session = session
    ? {
      ...session,
      destroy: (cb) => {
        if (destroyFails) return cb(new Error('store down'));
        sessionDestroyed = true;
        cb();
      },
    }
    : {};
  next();
});
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

await t('⭐解除はGETでは起きない (Cookieだけ消してセッションを残す穴を作らない)', async () => {
  const anon = await req('GET', '/board/exit', { token: boardToken });
  assert.equal(anon.status, 200);
  assert.equal(anon.headers.get('set-cookie'), null, 'GETではCookieを消さない');
  assert.ok(anon.text.includes('管理者のログインが必要'), '未ログインには案内だけ出す');

  // 管理者ログイン中でもGETでは消えない (クロスサイト誘導で解除されない)
  session = { email: 'admin@b-faith.biz', role: 'admin', allowedApps: '*' };
  try {
    const asAdmin = await req('GET', '/board/exit', { token: boardToken });
    assert.equal(asAdmin.headers.get('set-cookie'), null);
    assert.ok(asAdmin.text.includes('解除する'), '管理者には解除ボタンを出す');
  } finally {
    session = null;
  }

  // 管理者以外はPOSTしても解除できない
  const denied = await req('POST', '/board/exit', { token: boardToken });
  assert.equal(denied.status, 403);
  assert.equal(denied.headers.get('set-cookie'), null);
});

await t('⭐セッション破棄に失敗したら解除しない (Cookieだけ消して権限を残さない)', async () => {
  session = { email: 'admin@b-faith.biz', role: 'admin', allowedApps: '*' };
  destroyFails = true;
  try {
    const res = await req('POST', '/board/exit', { token: boardToken });
    assert.equal(res.status, 500);
    assert.equal(res.headers.get('set-cookie'), null, '破棄できないならCookieも消さない');
    // 端末登録も同様 (破棄失敗のまま端末Cookieを配らない)
    const reg = await req('POST', '/admin/devices', { body: { label: 'x', kind: 'worker' } });
    assert.equal(reg.status, 500);
    assert.equal(reg.headers.get('set-cookie'), null);
  } finally {
    destroyFails = false;
    session = null;
  }
});

await t('解除は管理者のPOSTのみ / 解除と同時にログアウトする', async () => {
  session = { email: 'admin@b-faith.biz', role: 'admin', allowedApps: '*' };
  sessionDestroyed = false;
  try {
    const res = await req('POST', '/board/exit', { token: boardToken });
    assert.equal(res.status, 200);
    assert.ok(/pk_device=/.test(res.headers.get('set-cookie') || ''), 'Cookieを消す');
    assert.equal(sessionDestroyed, true, '端末Cookieだけ消してセッションを残さない');
  } finally {
    session = null;
  }
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
