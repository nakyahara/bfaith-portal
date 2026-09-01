/**
 * 入荷受付チェック — HTTP スモーク (server.js を子プロセスで起動し、実際の経路で叩く)
 *
 * 実行: node scripts/smoke-inbound-check-http.mjs [入荷状況照会CSVのパス]
 *   一時 DATA_DIR / PORT=3457 / PORTAL_PASS=smoke で起動。既定管理者 (d.nakahara@b-faith.biz) でログイン。
 * 検証: 未認証の扱い / セッションでの取込・確認 / 端末登録 (セッション破棄→端末Cookie) / 端末Cookieの権限境界 / Origin チェック
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3457;
const BASE = `http://127.0.0.1:${PORT}`;
const APP = `${BASE}/apps/inbound-check`;
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-http-'));
const csvPath = process.argv[2];

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: { ...process.env, DATA_DIR, PORT: String(PORT), PORTAL_PASS: 'smoke', NODE_ENV: 'development', SESSION_SECRET: 'smoke-secret',
    INBOUND_INFO_SYNC_ENABLED: 'false' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let logs = '';
child.stdout.on('data', d => { logs += d; });
child.stderr.on('data', d => { logs += d; });

async function waitUp() {
  for (let i = 0; i < 120; i++) {
    try { const r = await fetch(`${BASE}/login`); if (r.status === 200) return true; } catch {}
    await new Promise(r => setTimeout(r, 500));
  }
  return false;
}

// 最小 cookie jar
function jar() {
  const store = new Map();
  return {
    absorb(res) {
      const set = res.headers.getSetCookie ? res.headers.getSetCookie() : [];
      for (const c of set) {
        const [kv, ...attrs] = c.split(';');
        const i = kv.indexOf('=');
        const k = kv.slice(0, i).trim(), v = kv.slice(i + 1).trim();
        const expires = attrs.map(a => a.trim().toLowerCase()).find(a => a.startsWith('expires=') || a.startsWith('max-age='));
        if ((expires && /max-age=0|1970/.test(expires)) || v === '') store.delete(k); else store.set(k, v);
      }
    },
    header() { return [...store.entries()].map(([k, v]) => `${k}=${v}`).join('; '); },
    has(k) { return store.has(k); },
    del(k) { store.delete(k); },
  };
}
async function req(j, url, { method = 'GET', body, headers = {}, form, multipart } = {}) {
  const h = { 'x-forwarded-proto': 'https', origin: BASE, ...headers };   // 変更系は Origin 必須 (ブラウザの fetch と同じ)   // セッションCookieは secure:true (trust proxy 経由の https として叩く)
  if (j) h.cookie = j.header();
  let payload;
  if (multipart) payload = multipart;
  else if (form) { h['content-type'] = 'application/x-www-form-urlencoded'; payload = new URLSearchParams(form).toString(); }
  else if (body !== undefined) { h['content-type'] = 'application/json'; payload = JSON.stringify(body); }
  const r = await fetch(url, { method, headers: h, body: payload, redirect: 'manual' });
  if (j) j.absorb(r);
  const text = await r.text();
  let json = null; try { json = JSON.parse(text); } catch {}
  return { status: r.status, text, json, location: r.headers.get('location'), ctype: r.headers.get('content-type') || '' };
}

try {
  console.log('DATA_DIR =', DATA_DIR);
  if (!await waitUp()) throw new Error('server did not start\n' + logs.slice(-3000));

  console.log('\n[A] 未認証');
  let r = await req(null, `${APP}/`);
  ok(r.status === 302 && /\/login/.test(r.location || ''), '作業画面 → /login へ');
  r = await req(null, `${APP}/api/state`);
  ok(r.status === 401 && r.json?.error === 'unauthorized', 'API → 401');
  r = await req(null, `${APP}/manifest.json`);
  ok(r.status === 200 && r.json?.name === '入荷受付チェック', 'manifest.json は認証不要');
  r = await req(null, `${APP}/admin/upload`, { method: 'POST', body: {} });
  ok(r.status === 302 && /\/login/.test(r.location || ''), '未認証の取込 POST → /login へ (API パス外)');

  console.log('\n[B] セッション (管理者)');
  const J = jar();
  r = await req(J, `${BASE}/login`, { method: 'POST', form: { email: 'd.nakahara@b-faith.biz', password: 'smoke' } });
  ok(r.status === 302 && J.header().length > 0, 'ログイン成功');
  r = await req(J, `${APP}`);
  ok(r.status === 308 && /\/apps\/inbound-check\/$/.test(r.location || ''), '末尾スラッシュ正規化 308');
  r = await req(J, `${APP}/`);
  ok(r.status === 200 && r.text.includes('入荷受付チェック') && r.text.includes('workerPick'), '作業画面 HTML');
  r = await req(J, `${APP}/api/state`);
  ok(r.status === 200 && r.json.ok && r.json.batch === null && r.json.me.admin === true, 'state (取込なし)');
  r = await req(J, `${APP}/admin`);
  ok(r.status === 200 && r.text.includes('取込履歴') && r.text.includes('この端末を登録'), '管理画面 (admin 節あり)');
  r = await req(J, `${APP}/admin/workers`, { method: 'POST', body: { name: '山田' } });
  ok(r.status === 200 && r.json.worker?.code === 'w01', '作業者追加');
  r = await req(J, `${APP}/admin/workers`, { method: 'POST', body: { name: '山田' }, headers: { origin: 'http://evil.example' } });
  ok(r.status === 403 && r.json?.error === 'bad_origin', '別 Origin の POST は 403');
  r = await req(J, `${APP}/admin/workers`, { method: 'POST', body: { name: '鈴木' }, headers: { origin: '' } });
  ok(r.status === 403, 'Origin も Referer も無い POST は 403');
  r = await req(J, `${APP}/admin/workers`, { method: 'POST', body: { name: '鈴木' }, headers: { origin: 'null' } });
  ok(r.status === 403, 'Origin: null は 403');
  r = await req(J, `${APP}/admin/workers`, { method: 'POST', body: { name: '鈴木' }, headers: { origin: '', referer: BASE + '/apps/inbound-check/admin' } });
  ok(r.status === 200 && r.json.worker?.code === 'w02', 'Origin 無しでも同一ホストの Referer があれば通る');

  // 取込 (multipart)
  const csvBuf = csvPath && fs.existsSync(csvPath) ? fs.readFileSync(csvPath) : null;
  let batchId = null, firstKey = null;
  if (csvBuf) {
    const fd = new FormData();
    fd.append('file', new Blob([csvBuf]), 'CA04001_test.csv');
    fd.append('file_modified', String(Date.now() - 60_000));
    r = await req(J, `${APP}/admin/upload`, { method: 'POST', multipart: fd });
    ok(r.status === 200 && r.json.ok && r.json.rowCount === 16, `CSV 取込 16行 (${r.status} ${r.json?.message || ''})`);
    batchId = r.json.batch?.id;
    r = await req(J, `${APP}/admin/upload`, { method: 'POST', multipart: (() => { const f = new FormData(); f.append('file', new Blob([csvBuf]), 'again.csv'); f.append('file_modified', String(Date.now())); return f; })() });
    ok(r.status === 409 && r.json.error === 'duplicate_file', '同じCSVの再取込は 409');
    r = await req(J, `${APP}/api/state`);
    ok(r.json.lines.length === 16 && r.json.slips.length === 1 && r.json.totals.checked === 0, 'state 16行/1伝票');
    firstKey = r.json.lines[0].line_key;
    ok(r.json.lines[0].info === null || typeof r.json.lines[0].info === 'object', '補助情報フィールドあり');
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: firstKey, expect_version: 1, worker_code: 'w01' } });
    ok(r.status === 200 && r.json.state.status === 'checked' && r.json.state.checked_by === '山田', '確認 (セッション + 作業者コード)');
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: firstKey, expect_version: 1, worker_code: 'w01' } });
    ok(r.status === 409 && r.json.error === 'conflict' && r.json.current.status === 'checked', '二重確認 → 409 conflict');
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId + 99, line_key: firstKey, worker_code: 'w01', expect_version: 1 } });
    ok(r.status === 409 && r.json.error === 'stale_batch', '旧/不明 batch_id → 409 stale_batch');
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: r.json.line_key || 'AR00110005164|2|1', expect_version: 1 } });
    ok(r.status === 200 && r.json.state.checked_by === '中原 大輔', '作業者コード無し (セッション) = 表示名で記録');
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: 'AR00110005164|3|1', worker_code: 'w99', expect_version: 1 } });
    ok(r.status === 400 && r.json.error === 'worker_required', '不明な作業者コード → 400');
    for (const bad of [undefined, null, 0, -1, 1.5, 'x', '']) {
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: 'AR00110005164|3|1', worker_code: 'w01', expect_version: bad } });
      ok(r.status === 400 && r.json.error === 'bad_request', `expect_version=${String(bad)} → 400`);
    }
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: 'AR00110005164|3|1', worker_code: 'w01', expect_version: '1' } });
    ok(r.status === 200, "expect_version='1' (文字列の整数) は許容");
    r = await req(J, `${APP}/admin/history.csv?batch_id=${batchId}`);
    ok(r.status === 200 && /text\/csv/.test(r.ctype) && r.text.includes('確認'), '履歴 CSV');
    r = await req(J, `${APP}/admin/history?batch_id=${batchId}`);
    ok(r.status === 200 && r.json.events.length === 3, '履歴 JSON 3件 (確認2 + 文字列version確認1)');
  } else {
    console.log('  (CSV パス未指定: 取込系はスキップ)');
  }

  console.log('\n[C] 端末登録 → 端末Cookie');
  r = await req(J, `${APP}/admin/devices`, { method: 'POST', body: { label: '入荷iPad1' } });
  ok(r.status === 200 && r.json.ok && r.json.loggedOut === true && J.has('ic_device'), '端末登録: ic_device Cookie 発行');
  const savedDeviceToken = (J.header().match(/ic_device=([^;]+)/) || [])[1];
  r = await req(J, `${APP}/admin`);
  ok(r.status === 302 && /\/login/.test(r.location || ''), '登録と同時にセッション破棄 (管理画面はログインへ)');
  r = await req(J, `${APP}/api/state`);
  ok(r.status === 200 && r.json.me.device?.label === '入荷iPad1' && r.json.me.session === null, '端末Cookieで state 取得');
  r = await req(J, `${APP}/`);
  ok(r.status === 200, '端末Cookieで作業画面');
  r = await req(J, `${APP}/admin/upload`, { method: 'POST', body: {} });
  ok(r.status === 403 && r.json.error === 'session_required', '端末Cookieでは取込不可 (403)');
  r = await req(J, `${APP}/admin/workers`, { method: 'POST', body: { name: 'x' } });
  ok(r.status === 403, '端末Cookieでは作業者追加不可');
  r = await req(J, `${APP}/admin/history.csv?batch_id=1`);
  ok(r.status === 403 || r.status === 302, '端末Cookieでは履歴不可');
  if (batchId) {
    r = await req(J, `${APP}/api/lines/uncheck`, { method: 'POST', body: { batch_id: batchId, line_key: firstKey, expect_version: 2, worker_code: 'w01' } });
    ok(r.status === 200 && r.json.state.status === 'unchecked', '端末Cookie + 作業者コードで取消');
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: firstKey, expect_version: 3 } });
    ok(r.status === 400 && r.json.error === 'worker_required', '端末Cookieで作業者未選択 → 400');
  }
  r = await req(J, `${APP}/device/exit`, { method: 'POST', body: {} });
  ok(r.status === 200 && r.json.revoked === true && !J.has('ic_device'), '端末解除で Cookie 削除 + サーバー側も失効');
  r = await req(null, `${APP}/api/state`, { headers: { cookie: 'ic_device=' + (savedDeviceToken || '') } });
  ok(r.status === 401, '解除後は控えておいたトークンでも 401 (失効済み)');
  r = await req(J, `${APP}/api/state`);
  ok(r.status === 401, '解除後は 401');

  console.log('\n[D] 一般ユーザー (非admin・アプリ権限あり) の境界');
  // 管理者で再ログインしてユーザー作成 → そのユーザーでログイン
  const A = jar();
  await req(A, `${BASE}/login`, { method: 'POST', form: { email: 'd.nakahara@b-faith.biz', password: 'smoke' } });
  r = await req(A, `${BASE}/admin/users/add`, { method: 'POST', form: { email: 'staff@example.com', password: 'staffpass', displayName: '事務', role: 'user', allowedApps: 'inbound-check' } });
  const U = jar();
  r = await req(U, `${BASE}/login`, { method: 'POST', form: { email: 'staff@example.com', password: 'staffpass' } });
  if (r.status === 302 && U.header()) {
    r = await req(U, `${APP}/api/state`);
    ok(r.status === 200 && r.json.me.admin === false, '一般ユーザー: state OK / admin=false');
    r = await req(U, `${APP}/admin`);
    ok(r.status === 200 && !r.text.includes('この端末を登録'), '一般ユーザー: 管理画面は見えるが端末登録節は出ない');
    r = await req(U, `${APP}/admin/devices`, { method: 'POST', body: { label: 'x' } });
    ok(r.status === 403, '一般ユーザー: 端末登録は 403');
    r = await req(U, `${APP}/admin/history.csv?batch_id=1`);
    ok(r.status === 403, '一般ユーザー: 履歴CSV は 403');
    if (csvBuf) {
      const f = new FormData(); f.append('file', new Blob([Buffer.from('broken')]), 'x.csv');
      r = await req(U, `${APP}/admin/upload`, { method: 'POST', multipart: f });
      ok(r.status === 400 && r.json.error === 'bad_csv', '一般ユーザー: 取込は可 (壊れたCSVは 400)');
    }
  } else {
    console.log('  (ユーザー作成 API の形式が想定と異なるためスキップ: ' + r.status + ')');
  }
} catch (e) {
  fail++;
  console.log('  ✗ 例外:', e.message);
} finally {
  child.kill();
}
console.log(`\n${pass} PASS / ${fail} FAIL`);
if (fail) console.log('--- server log tail ---\n' + logs.slice(-2500));
process.exitCode = fail ? 1 : 0;
