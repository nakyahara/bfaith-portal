/**
 * 🚚 「いま入荷を取りに行く」 — 予定外の納品を定時を待たずに iPad へ出す
 *   (apps/inbound-check/logizard-refresh.js + apps/warehouse/logizard-export-service.js)
 *
 * 実行: node scripts/test-inbound-check-refresh.mjs
 *
 * 守りたいのは3つ。
 *   ① ロジザードへ何度もログインしに行かない (同時に1本・終わった直後は待たせる)
 *   ② 押したのに何も起きない状態を作らない (miniPC を呼べない環境ははっきり失敗させる・
 *      失敗した理由がそのまま画面に出る)
 *   ③ 「取れなかった」と「増えていなかった」を混ぜない (後者は失敗ではない)
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-refresh-'));
process.env.WAREHOUSE_URL = 'https://wh.example.test';
process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';
process.env.CF_ACCESS_CLIENT_ID = 'cf-id';
process.env.CF_ACCESS_CLIENT_SECRET = 'cf-secret';

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const rf = await import('../apps/inbound-check/logizard-refresh.js');
const { startRefresh, refreshState, refreshConfigured, _resetForTest, _waitIdleForTest } = rf;

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// テスト用の短い待ち (待ち時間そのものではなく、遷移を確かめたい)
const FAST = { jobPollMs: 5, driveWaitMs: 60, drivePollMs: 5, cooldownMs: 60_000 };

/** miniPC の service-api を模す。calls に呼ばれた順で記録する */
function makeFetch({ jobStates = ['completed'], startBody = { ok: true, jobId: 'job-1', status: 'running' }, startStatus = 202, jobError = null, progress = null, calls = [], jobResult = { ok: true } } = {}) {
  let i = 0;
  return async (url, opts = {}) => {
    calls.push(`${opts.method || 'GET'} ${String(url).replace('https://wh.example.test', '')}`);
    const json = (body, status = 200) => ({
      ok: status >= 200 && status < 300, status,
      headers: { get: (k) => (k.toLowerCase() === 'content-type' ? 'application/json' : null) },
      json: async () => body,
    });
    if (String(url).includes('/nyuka-refresh')) return json(startBody, startStatus);
    if (String(url).includes('/service-api/jobs/')) {
      const st = jobStates[Math.min(i++, jobStates.length - 1)];
      const job = { jobId: 'job-1', type: 'logizard-nyuka-refresh', status: st };
      if (st === 'running' && progress) job.progress = progress;
      if (st === 'failed') job.error = jobError || { code: 'SCRIPT_FAILED', message: '取得に失敗しました' };
      if (st === 'completed') job.result = jobResult;
      return json({ ok: true, job });
    }
    throw new Error(`想定外の呼び出し: ${url}`);
  };
}

/** Drive 層の差し替え (実際の Drive も SA 資格情報も使わない) */
function makeDrive({ modifiedBefore = '2026-09-05T10:00:00.000Z', modifiedAfter = '2026-09-05T10:05:00.000Z', importResult = { ok: true, slipCount: 3, rowCount: 12, batch: { id: 9 } }, infoThrows = false } = {}) {
  let n = 0;
  return {
    getDriveInfo: async () => {
      if (infoThrows) throw new Error('drive down');
      return { modified_time: n++ === 0 ? modifiedBefore : modifiedAfter };
    },
    fetchAndImportFromDrive: async () => importResult,
  };
}

console.log('[1] miniPC を呼べない環境でははっきり失敗する');
{
  _resetForTest();
  const keep = process.env.WAREHOUSE_SERVICE_TOKEN;
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  ok(refreshConfigured() === false, '資格情報が無ければ configured=false (画面はボタンを出さない)');
  const r = startRefresh({ actor: 'test' });
  ok(!r.ok && r.error === 'not_configured' && /miniPC/.test(r.message), '押しても走らせず理由を返す');
  eq(refreshState().state, 'idle', '状態は idle のまま');
  process.env.WAREHOUSE_SERVICE_TOKEN = keep;
  ok(refreshConfigured() === true, '資格情報が揃えば configured=true');
}

console.log('\n[2] 取りに行って取り込むまで');
{
  _resetForTest();
  const calls = [];
  const r = startRefresh({ actor: 'device:入荷iPad1/山田', fetchFn: makeFetch({ calls }), drive: makeDrive(), timing: FAST });
  ok(r.ok && r.run.state === 'running', '押した瞬間は running で即返る (画面を待たせない)');
  ok(r.run.by === 'device:入荷iPad1/山田', '誰が押したかを残す');
  const s = await _waitIdleForTest();
  eq(s.state, 'done', '完了');
  ok(s.ok === true && /3伝票 \/ 12行/.test(s.message), `取り込んだ件数を返す (${s.message})`);
  eq(s.batchId, 9, '新しいバッチ');
  ok(calls[0] === 'POST /service-api/logizard/nyuka-refresh', `まず miniPC に出し直させる (${calls[0]})`);
  ok(calls[1] === 'GET /service-api/jobs/job-1', 'ジョブの完了を見に行く');
}

console.log('\n[3] 進み具合が画面に出る (ロック待ちも伝える)');
{
  _resetForTest();
  const fetchFn = makeFetch({
    jobStates: ['running', 'completed'],
    progress: { phase: 'waiting_lock', message: 'ロジザードの別の取得が終わるのを待っています' },
  });
  // 1回目のポーリング (running) と2回目 (completed) の間を空けて、途中の表示を捕まえる
  startRefresh({ actor: 'test', fetchFn, drive: makeDrive(), timing: { ...FAST, jobPollMs: 300 } });
  await new Promise(r => setTimeout(r, 80));
  const mid = refreshState();
  ok(mid.state === 'running' && /待っています/.test(mid.message || ''), `途中経過が出る (${mid.message})`);
  const s = await _waitIdleForTest();
  eq(s.state, 'done', 'その後ちゃんと完了する');
}

console.log('\n[4] 増えていなかった = 失敗ではない (ただし届いたことを確かめてから言う)');
{
  _resetForTest();
  // Drive の更新日時が動いた = 取り直した結果が確かに届いている → 中身が同じなら「新規なし」
  const s0 = startRefresh({
    actor: 'test', fetchFn: makeFetch(),
    drive: makeDrive({ importResult: { ok: false, error: 'duplicate_file', message: '同じファイルです' } }),
    timing: FAST,
  });
  ok(s0.ok, '走り出す');
  const s = await _waitIdleForTest();
  eq(s.state, 'done', 'done (failed にしない)');
  ok(s.ok === true && s.unchanged === true && /新しい受付はありませんでした/.test(s.message), `文言で伝える (${s.message})`);
}
{
  _resetForTest();
  // Drive は動かないが、miniPC が「出力CSVは前回と同じ」と言っている → 増えていないことは確か
  startRefresh({
    actor: 'test', fetchFn: makeFetch({ jobResult: { ok: true, csvWritten: true, csvSameContent: true } }),
    drive: makeDrive({ modifiedBefore: 'A', modifiedAfter: 'A', importResult: { ok: false, error: 'duplicate_file' } }),
    timing: FAST,
  });
  const s = await _waitIdleForTest();
  ok(s.state === 'done' && s.unchanged && s.verified, '今回の実行が書き直して中身が同じなら「新規なし」と言ってよい');
}
{
  _resetForTest();
  // 🚨 取り直したのに新しい世代を確認できない (転送漏れ / Drive 障害) → 「新規なし」と言い切らない
  startRefresh({
    actor: 'test', fetchFn: makeFetch({ jobResult: { ok: true, csvWritten: true, csvSameContent: false } }),
    drive: makeDrive({ modifiedBefore: 'A', modifiedAfter: 'A', importResult: { ok: false, error: 'duplicate_file' } }),
    timing: FAST,
  });
  const s = await _waitIdleForTest();
  eq(s.state, 'failed', '確かめられないときは done にしない');
  eq(s.error, 'not_verified', 'not_verified');
  ok(/反映を確認できませんでした/.test(s.message), `そのまま伝える (${s.message})`);
}
{
  _resetForTest();
  // 新しいバッチができたなら裏取りは不要 (新しい世代が届いた証拠そのもの)
  startRefresh({
    actor: 'test', fetchFn: makeFetch({ jobResult: { ok: true, csvWritten: true, csvSameContent: false } }),
    drive: makeDrive({ modifiedBefore: 'A', modifiedAfter: 'A' }),
    timing: FAST,
  });
  const s = await _waitIdleForTest();
  ok(s.state === 'done' && s.verified && /3伝票/.test(s.message), '取り込めたときは Drive の更新日時を待たずに成功');
}
{
  _resetForTest();
  // 🚨 終了コード0でも今回の実行が CSV を書いていない (csvWritten=false) ときは「新規なし」と言わない
  startRefresh({
    actor: 'test', fetchFn: makeFetch({ jobResult: { ok: true, csvWritten: false, csvSameContent: null } }),
    drive: makeDrive({ modifiedBefore: 'A', modifiedAfter: 'A', importResult: { ok: false, error: 'duplicate_file' } }),
    timing: FAST,
  });
  const s = await _waitIdleForTest();
  ok(s.state === 'failed' && s.error === 'not_verified', '今回の実行が CSV を書いていなければ未検証として扱う');
}
{
  _resetForTest();
  // 🚨 更新日時が**戻った** (古いファイルに差し替わった) のを「新しい世代が届いた」と読まない
  startRefresh({
    actor: 'test', fetchFn: makeFetch({ jobResult: { ok: true, csvWritten: true, csvSameContent: false } }),
    drive: makeDrive({ modifiedBefore: '2026-09-05T10:00:00.000Z', modifiedAfter: '2026-09-05T09:00:00.000Z', importResult: { ok: false, error: 'duplicate_file' } }),
    timing: FAST,
  });
  const s = await _waitIdleForTest();
  ok(s.state === 'failed' && s.error === 'not_verified', '時刻が戻ったら verified にしない (進んだときだけ)');
}

console.log('\n[5] 失敗はそのまま画面に出す');
{
  _resetForTest();
  const s0 = startRefresh({
    actor: 'test', timing: FAST, drive: makeDrive(),
    fetchFn: makeFetch({ jobStates: ['failed'], jobError: { code: 'LOCK_BUSY', message: 'ロジザードの別の取得 (在庫CSV等) が終わりませんでした。1〜2分おいてからもう一度お試しください' } }),
  });
  ok(s0.ok, '走り出す');
  const s = await _waitIdleForTest();
  eq(s.state, 'failed', 'failed');
  ok(/1〜2分おいて/.test(s.message), `miniPC の理由がそのまま出る (${s.message})`);
  eq(s.error, 'LOCK_BUSY', '理由コードも残る');
}
{
  _resetForTest();
  startRefresh({ actor: 'test', timing: FAST, drive: makeDrive(), fetchFn: makeFetch({ startBody: { ok: true }, startStatus: 202 }) });
  const s = await _waitIdleForTest();
  ok(s.state === 'failed' && /ジョブID/.test(s.message), 'ジョブIDが返らなければ失敗にする (黙って成功にしない)');
}
{
  _resetForTest();
  const boom = async () => { throw new Error('つながりません'); };
  startRefresh({ actor: 'test', timing: FAST, drive: makeDrive(), fetchFn: boom });
  const s = await _waitIdleForTest();
  ok(s.state === 'failed' && /つながりません/.test(s.message), '通信できないときも状態に残る');
}
{
  _resetForTest();
  startRefresh({
    actor: 'test', timing: { ...FAST, jobTimeoutMs: 40 }, drive: makeDrive(),
    fetchFn: makeFetch({ jobStates: ['running'] }),
  });
  const s = await _waitIdleForTest();
  ok(s.state === 'failed' && /時間内に終わりません/.test(s.message), '終わらないジョブは打ち切って失敗にする');
}

console.log('\n[6] ロジザードへ何度もログインしに行かない');
{
  _resetForTest();
  const calls = [];
  const fetchFn = makeFetch({ jobStates: ['running', 'running', 'completed'], calls });
  const a = startRefresh({ actor: 'iPad1', fetchFn, drive: makeDrive(), timing: FAST });
  const b = startRefresh({ actor: 'iPad2', fetchFn, drive: makeDrive(), timing: FAST });
  ok(a.ok && !a.already, '1台目は走り出す');
  ok(b.ok && b.already === true, '2台目は「いま取りに行っています」で相乗り (2本目を走らせない)');
  const s = await _waitIdleForTest();
  eq(s.state, 'done', '1本だけ走って完了');
  eq(calls.filter(c => c.includes('nyuka-refresh')).length, 1, 'miniPC への依頼は1回だけ');
  // 終わった直後は待たせる
  const c = startRefresh({ actor: 'iPad1', fetchFn, drive: makeDrive(), timing: FAST });
  ok(!c.ok && c.error === 'cooldown' && /秒あけて/.test(c.message), `終わった直後は cooldown (${c.message})`);
  const d = startRefresh({ actor: 'iPad1', fetchFn, drive: makeDrive(), timing: { ...FAST, cooldownMs: 0 } });
  ok(d.ok, '待ち時間を過ぎればまた押せる');
  await _waitIdleForTest();
}

console.log('\n[7] Drive の反映待ち');
{
  _resetForTest();
  // 更新日時が動いたらすぐ取り込む
  const s1 = startRefresh({ actor: 'test', fetchFn: makeFetch(), drive: makeDrive({ modifiedBefore: 'A', modifiedAfter: 'B' }), timing: FAST });
  ok(s1.ok, '走り出す');
  const t0 = Date.now();
  const s = await _waitIdleForTest();
  eq(s.state, 'done', '完了');
  ok(Date.now() - t0 < 300, `更新日時が動いていれば待たない (${Date.now() - t0}ms)`);
}
{
  _resetForTest();
  // 更新日時が動かなくても上限まで待って取り込みには行く (判定は取込側 + 裏取りで決める)
  const calls = [];
  startRefresh({ actor: 'test', fetchFn: makeFetch({ calls, jobResult: { ok: true, csvWritten: true, csvSameContent: true } }), drive: makeDrive({ modifiedBefore: 'A', modifiedAfter: 'A', importResult: { ok: false, error: 'duplicate_file', message: '同じ' } }), timing: FAST });
  const s = await _waitIdleForTest();
  ok(s.state === 'done' && s.unchanged, '待っても動かなければ取り込みに行き、裏取りできていれば変化なしとして返す');
}
{
  _resetForTest();
  // Drive の情報が取れなくても取込までは進む (取込側が fail-closed で判定する)
  startRefresh({ actor: 'test', fetchFn: makeFetch(), drive: makeDrive({ infoThrows: true }), timing: FAST });
  const s = await _waitIdleForTest();
  eq(s.state, 'done', 'Drive の更新日時が読めなくても取り込みは試みる');
}

// ─── HTTP: 誰が押せるか ───
console.log('\n[8] HTTP — 押せる人と Origin');
{
  _resetForTest();
  const express = (await import('express')).default;
  const dbMod = await import('../apps/inbound-check/db.js');
  const router = (await import('../apps/inbound-check/router.js')).default;
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    const s = req.headers['x-test-session'];
    req.session = s ? { authenticated: true, email: 'tester@example.com', displayName: 'テスター', allowedApps: '*', role: s === 'admin' ? 'admin' : 'user', destroy: (cb) => cb() } : {};
    next();
  });
  app.use('/apps/inbound-check', router);
  const server = http.createServer(app);
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const base = `${origin}/apps/inbound-check`;
  const call = async (method, url, { cookie = null, session = null, body = null, withOrigin = true } = {}) => {
    const headers = {};
    if (cookie) headers.Cookie = cookie;
    if (session) headers['x-test-session'] = session;
    if (body) { headers['Content-Type'] = 'application/json'; if (withOrigin) headers.Origin = origin; }
    const res = await fetch(base + url, { method, headers, body: body ? JSON.stringify(body) : undefined, redirect: 'manual' });
    const ct = res.headers.get('content-type') || '';
    return { status: res.status, body: ct.includes('json') ? await res.json().catch(() => ({})) : null };
  };
  const device = dbMod.createDevice('入荷iPad9', 'admin');

  eq((await call('POST', '/api/refresh-now', { body: {} })).status, 401, '未ログイン・未登録端末は押せない');
  eq((await call('POST', '/api/refresh-now', { session: 'user', body: {}, withOrigin: false })).status, 403, 'Origin の無い POST は 403 (CSRF)');
  const noWorker = await call('POST', '/api/refresh-now', { cookie: `ic_device=${device.token}`, body: {} });
  ok(noWorker.status === 400 && noWorker.body.error === 'worker_required', '端末から押すときは作業者が要る (誰が押したかを残す)');
  // 実環境の miniPC は呼ばない: 資格情報を外して not_configured で返ることだけ確かめる
  const keep = process.env.CF_ACCESS_CLIENT_ID;
  delete process.env.CF_ACCESS_CLIENT_ID;
  const off = await call('POST', '/api/refresh-now', { session: 'user', body: {} });
  ok(off.status === 503 && off.body.error === 'not_configured', 'miniPC を呼べない環境は 503 (押せたのに何も起きない、を作らない)');
  process.env.CF_ACCESS_CLIENT_ID = keep;
  const st = await call('GET', '/api/state', { session: 'user' });
  ok(st.status === 200 && st.body.refresh && typeof st.body.refresh.configured === 'boolean', '/api/state に refresh の状態が出る (画面はこれを見る)');
  server.close();
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
// 🚨 fetch (undici) を使った直後の process.exit() は Windows の Node で libuv assertion を踏む。
//    exitCode を立てて自然終了させる ([[feedback_notify_job_exit_libuv_crash]])
process.exitCode = fail ? 1 : 0;
