/**
 * 🚚 miniPC 側「ロジザードから入荷受付CSVを取り直す」入口 (apps/warehouse/logizard-export-service.js)
 *
 * 実行: node scripts/test-logizard-export-service.mjs
 *
 * ここで守りたいのは「ロジザードのセッションを荒らさない」こと。
 *   ① 在庫CSV等がログイン中 (ロックあり) なら**待つ**。割り込んで追い出さない
 *   ② 同時に1本・連打は断る
 *   ③ 自動化が入っていない PC で「成功した」と言わない
 * 本物のロジザードは呼ばない。取得スクリプトの代わりに**偽スクリプトを実際に spawn** して確かめる
 * (spawn の経路そのものが本番と同じになるので、cwd や終了コードの扱いを取り違えない)。
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';

const DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lz-export-'));
fs.mkdirSync(path.join(DIR, 'logs'), { recursive: true });
process.env.LOGIZARD_AUTOMATION_DIR = DIR;

const express = (await import('express')).default;
const svc = await import('../apps/warehouse/logizard-export-service.js');
const router = svc.default;

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const app = express();
app.use(express.json());
app.use('/service-api/logizard', router);
// ジョブの照会は本番と同じ job-manager を使う
const { getJob } = await import('../apps/warehouse/job-manager.js');
app.get('/service-api/jobs/:id', (req, res) => {
  const j = getJob(req.params.id);
  if (!j) return res.status(404).json({ ok: false, error: 'JOB_NOT_FOUND' });
  res.json({ ok: true, job: j });
});
const server = http.createServer(app);
await new Promise(r => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;

const call = async (method, url, body = null) => {
  const res = await fetch(base + url, {
    method, headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  return { status: res.status, body: await res.json().catch(() => ({})) };
};
const SCRIPT = path.join(DIR, 'auto-nyuka-csv.js');
const LOCK = path.join(DIR, 'logs', 'logizard-session.lock');
const OUT_CSV = path.join(DIR, 'out', 'nyuka_uketsuke.csv');
/** 取得スクリプトの代わり。exitCode と出力を指定できる。writesCsv=true なら出力CSVも書く */
const writeScript = (exitCode = 0, out = 'ok', { writesCsv = false, csvBody = null } = {}) => fs.writeFileSync(SCRIPT,
  `console.log(${JSON.stringify(out)}); console.log('cwd=' + process.cwd());\n`
  + (writesCsv
    ? `const fs=require('fs');const p=require('path');fs.mkdirSync(p.join(process.cwd(),'out'),{recursive:true});`
      + `fs.writeFileSync(p.join(process.cwd(),'out','nyuka_uketsuke.csv'), ${JSON.stringify(csvBody ?? 'row1\n')});\n`
    : '')
  + `process.exitCode = ${exitCode};\n`);
/** 置き去りのロック (持ち主が死んでいて古い) を作る */
function writeStaleLock() {
  fs.writeFileSync(LOCK, JSON.stringify({ pid: 999999, token: 'x', startedAt: '2020-01-01T00:00:00.000Z' }));
  const old = new Date(Date.now() - 60 * 60 * 1000);
  fs.utimesSync(LOCK, old, old);
}
/** ジョブが終わるまで待つ */
async function waitJob(jobId, timeoutMs = 15000) {
  const until = Date.now() + timeoutMs;
  for (;;) {
    const r = await call('GET', `/service-api/jobs/${jobId}`);
    if (r.body.job && r.body.job.status !== 'running') return r.body.job;
    if (Date.now() > until) return r.body.job || null;
    await sleep(50);
  }
}

console.log('[1] 自動化が入っていない PC では成功と言わない');
{
  svc._resetForTest();
  if (fs.existsSync(SCRIPT)) fs.unlinkSync(SCRIPT);
  const r = await call('POST', '/service-api/logizard/nyuka-refresh');
  eq(r.status, 503, '503 を返す');
  eq(r.body.error, 'NOT_AVAILABLE', 'NOT_AVAILABLE');
  ok(/ロジザード自動化がありません/.test(r.body.message), `どこを見ればよいか言う (${r.body.message})`);
  const s = await call('GET', '/service-api/logizard/status');
  eq(s.body.available, false, 'status も available=false');
}

console.log('\n[2] 取得スクリプトを実際に起動して完了まで');
{
  svc._resetForTest();
  writeScript(0, 'nyuka ok');
  const r = await call('POST', '/service-api/logizard/nyuka-refresh');
  eq(r.status, 202, '202 で即返す (呼び出し側を待たせない)');
  eq(r.body.status, 'running', 'running');
  ok(!!r.body.jobId, 'jobId を返す');
  const job = await waitJob(r.body.jobId);
  eq(job.status, 'completed', 'ジョブは completed');
  ok(/nyuka ok/.test(job.result.output || ''), '取得スクリプトの出力を残す (失敗時の手掛かり)');
  ok(job.result.output.includes(`cwd=${DIR}`), `自動化フォルダで動かす (${DIR})`);
  eq(job.result.waitedForLock, false, 'ロック待ちはしていない');
  // 連打防止は「終わった時刻」から数える (受付時刻からだと長いジョブの直後に再実行できてしまう)
  const st = await call('GET', '/service-api/logizard/status');
  ok(st.body.cooldownRemainSec >= 58, `完了直後は残り約60秒 (${st.body.cooldownRemainSec}秒) = 終了時刻から数えている`);
}

console.log('\n[2b] 出力CSVが書き変わったかを返す (呼び出し側の裏取り用)');
{
  svc._resetForTest();
  try { fs.rmSync(OUT_CSV, { force: true }); } catch { /* 無ければよい */ }
  writeScript(0, 'first', { writesCsv: true, csvBody: 'a\n' });
  const r1 = await call('POST', '/service-api/logizard/nyuka-refresh');
  const j1 = await waitJob(r1.body.jobId);
  eq(j1.result.csvWritten, true, '初回は今回の実行が書いた');
  eq(j1.result.csvSameContent, null, '前回が無ければ同じかどうかは言えない');
  ok(j1.result.csv && j1.result.csv.size > 0, `出力CSVの世代を返す (${JSON.stringify(j1.result.csv)})`);

  svc._resetForTest();
  // 中身も更新時刻も変わらない = 増えていない (rclone が転送を省いても呼び出し側が判断できる)
  writeScript(0, 'again');
  const before = fs.statSync(OUT_CSV).mtimeMs;
  const r2 = await call('POST', '/service-api/logizard/nyuka-refresh');
  const j2 = await waitJob(r2.body.jobId);
  eq(j2.result.csvWritten, false, '古い CSV を残したまま終了コード0 → 今回は書いていないと分かる');
  eq(j2.result.csvSameContent, null, '書いていないので「同じ」とは言わない');
  eq(fs.statSync(OUT_CSV).mtimeMs, before, 'CSV は触っていない');

  svc._resetForTest();
  // 今回の実行が同じ中身で書き直した → 「増えていない」と言い切ってよい
  writeScript(0, 'same again', { writesCsv: true, csvBody: 'a\n' });
  const r2b = await call('POST', '/service-api/logizard/nyuka-refresh');
  const j2b = await waitJob(r2b.body.jobId);
  eq(j2b.result.csvWritten, true, '今回の実行が書き直した');
  eq(j2b.result.csvSameContent, true, '中身は前回と同じ');

  svc._resetForTest();
  // 中身が変わった
  writeScript(0, 'changed', { writesCsv: true, csvBody: 'a\nb\n' });
  const r2c = await call('POST', '/service-api/logizard/nyuka-refresh');
  const j2c = await waitJob(r2c.body.jobId);
  eq(j2c.result.csvSameContent, false, '中身が変わったら false');

  svc._resetForTest();
  // 🚨 終了コード0でも CSV が無いときは「同じ」ではなく「分からない」(null)。
  //    false にすると呼び出し側が古い一覧を「新しい受付なし」と誤って言い切る
  fs.rmSync(OUT_CSV, { force: true });
  writeScript(0, 'no csv written');
  const r3 = await call('POST', '/service-api/logizard/nyuka-refresh');
  const j3 = await waitJob(r3.body.jobId);
  eq(j3.result.csvWritten, false, 'CSV が無ければ csvWritten=false (未検証)');
  eq(j3.result.csvSameContent, null, 'csvSameContent も null');
  eq(j3.result.csv, null, 'csv も null');
}

console.log('\n[3] 連打と二重起動を止める');
{
  const busy = await call('POST', '/service-api/logizard/nyuka-refresh');
  eq(busy.status, 429, '直後にもう一度押すと 429');
  eq(busy.body.error, 'COOLDOWN', 'COOLDOWN');
  ok(/秒あけて/.test(busy.body.message), `あと何秒かを言う (${busy.body.message})`);

  svc._resetForTest();
  // 走っている最中にもう一度押す → 同じジョブを返す (2本目を起動しない)
  writeScript(0, 'slow');
  fs.writeFileSync(SCRIPT, "setTimeout(() => { console.log('slow done'); }, 700);\n");
  const a = await call('POST', '/service-api/logizard/nyuka-refresh');
  const b = await call('POST', '/service-api/logizard/nyuka-refresh');
  eq(b.status, 200, '走っている間は 200');
  eq(b.body.status, 'already_running', 'already_running');
  eq(b.body.jobId, a.body.jobId, '同じジョブを見てもらう');
  const st = await call('GET', '/service-api/logizard/status');
  eq(st.body.running, true, 'status も running');
  await waitJob(a.body.jobId);
}

console.log('\n[4] 他のロジザード取得中 (ロックあり) は割り込まずに待つ');
{
  svc._resetForTest();
  writeScript(0, 'after lock');
  fs.writeFileSync(LOCK, JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }));
  const r = await call('POST', '/service-api/logizard/nyuka-refresh');
  eq(r.status, 202, '受け付けはする');
  await sleep(300);
  const mid = await call('GET', `/service-api/jobs/${r.body.jobId}`);
  eq(mid.body.job.status, 'running', 'ロックが空くまで走り出さない');
  ok(/待っています/.test((mid.body.job.progress || {}).message || ''), `待っていることを伝える (${JSON.stringify(mid.body.job.progress)})`);
  const lockStatus = await call('GET', '/service-api/logizard/status');
  eq(lockStatus.body.lockHeld, true, 'status に lockHeld が出る');
  fs.unlinkSync(LOCK);   // 在庫CSVが終わった
  const job = await waitJob(r.body.jobId);
  eq(job.status, 'completed', 'ロックが空いたら取得が走る');
  eq(job.result.waitedForLock, true, '待ったことを記録する');
}

console.log('\n[4b] 置き去りのロックで永久に待たされない');
{
  svc._resetForTest();
  writeScript(0, 'after stale lock');
  writeStaleLock();   // 持ち主の PID が死んでいて 1時間前から放置
  const st = await call('GET', '/service-api/logizard/status');
  ok(st.body.lockHeld === false && st.body.lockStale === true, '置き去りと判定する (握られているとは読まない)');
  const t0 = Date.now();
  const r = await call('POST', '/service-api/logizard/nyuka-refresh');
  const job = await waitJob(r.body.jobId);
  eq(job.status, 'completed', '待たずに取得へ進む');
  eq(job.result.waitedForLock, false, 'ロック待ちに入っていない');
  ok(Date.now() - t0 < 3000, `90秒待ちに落ちない (${Date.now() - t0}ms)`);
  // 🚨 置き去りロックを**こちらで消さない** (回収は取得スクリプトの acquireLock に任せる)
  ok(fs.existsSync(LOCK), 'ロックファイルは消さない (取り違えて他人のロックを消さないため)');
  fs.unlinkSync(LOCK);
}
{
  svc._resetForTest();
  // 持ち主が死んでいても「若い」ロックは待つ (起動直後・心拍の谷を置き去りと決めつけない)
  fs.writeFileSync(LOCK, JSON.stringify({ pid: 999999, token: 'x', startedAt: new Date().toISOString() }));
  const st = await call('GET', '/service-api/logizard/status');
  eq(st.body.lockHeld, true, '若いロックは「実行中」として扱う');
  eq(st.body.lockStale, false, 'stale ではない');
  fs.unlinkSync(LOCK);
}

console.log('\n[5] 取得スクリプトが失敗したとき');
{
  svc._resetForTest();
  writeScript(1, 'なにかのエラー');
  const r = await call('POST', '/service-api/logizard/nyuka-refresh');
  const job = await waitJob(r.body.jobId);
  eq(job.status, 'failed', 'failed');
  eq(job.error.code, 'SCRIPT_FAILED', 'SCRIPT_FAILED');
  ok(/終了コード 1/.test(job.error.message), `終了コードを言う (${job.error.message})`);

  svc._resetForTest();
  // ロック待ちを抜けた直後に別プロセスが取った場合 (スクリプトが即終了する) は「重なった」と言う
  writeScript(1, 'ロックの取得に失敗しました (他プロセスと競合)');
  const r2 = await call('POST', '/service-api/logizard/nyuka-refresh');
  const job2 = await waitJob(r2.body.jobId);
  eq(job2.error.code, 'LOCK_BUSY', 'ロックのメッセージなら LOCK_BUSY');
  ok(/重なりました/.test(job2.error.message), `もう一度どうぞ、と言う (${job2.error.message})`);
}

server.close();
try { fs.rmSync(DIR, { recursive: true, force: true }); } catch { /* 後始末の失敗は無視 */ }
console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
// fetch を使った直後の process.exit() は Windows の Node で libuv assertion を踏む
process.exitCode = fail ? 1 : 0;
