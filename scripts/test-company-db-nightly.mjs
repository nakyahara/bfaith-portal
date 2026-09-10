/**
 * test-company-db-nightly.mjs — 夜間の再ロード (apps/company-db/nightly.mjs) の受入試験
 *
 * 本物の Postgres も SQLite も要らない。「ロードを始める」ところを差し替えて、
 * 成功・失敗・見送り・打ち切り・設定漏れ の 5 通りで、戻り値と ping の中身を見る。
 * 使い方: node scripts/test-company-db-nightly.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { runNightlyLoad, startCompanyDbNightlyLoadCron, summarize, JOB_ID } from '../apps/company-db/nightly.mjs';

// 材料 (warehouse-mirror.db) がある一時ディレクトリ。中身は空でよい (ここでは開かない)
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-nightly-'));
fs.writeFileSync(path.join(TMP, 'warehouse-mirror.db'), '');
process.on('exit', () => { try { fs.rmSync(TMP, { recursive: true, force: true }); } catch { /* 消せなくても試験の結果は変わらない */ } });

let passed = 0;
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
const quiet = () => {};

/** 呼ばれた ping を貯める */
const spyPing = () => { const calls = []; const fn = (...a) => { calls.push(a); return true; }; fn.calls = calls; return fn; };
/** すぐ終わるロードの真似 */
const fakeStart = (cur) => () => ({ started: true, current: cur, done: Promise.resolve(cur) });
const doneRun = {
  run_id: 'load_test_1', status: 'done',
  summary: { products: { planned: 10, applied: 2, same: 8 }, skus: { planned: 5, applied: 0, same: 5 } },
  conflicts: 3, unresolved: { listing_components: 4, suppliers: 0 },
};

const withEnv = async (patch, fn) => {
  const before = {};
  for (const [k, v] of Object.entries(patch)) { before[k] = process.env[k]; if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  try { return await fn(); }
  finally { for (const [k, v] of Object.entries(before)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; } }
};
const configured = { DATA_DIR: TMP, COMPANY_DB_URL: 'postgres://u:p@127.0.0.1:5432/x' };

console.log('夜間の再ロード');

await ta('成功したら ok を ping する。何が変わったかが note に入る', async () => {
  const ping = spyPing();
  const r = await withEnv(configured, () => runNightlyLoad({ log: quiet, start: fakeStart(doneRun), ping }));
  assert.equal(r.ok, true);
  assert.equal(r.skipped, false);
  assert.equal(ping.calls.length, 1);
  assert.deepEqual(ping.calls[0].slice(0, 2), [JOB_ID, 'ok']);
  const note = ping.calls[0][2];
  assert.match(note, /load_test_1/);
  assert.match(note, /products\+2/);
  assert.ok(!note.includes('skus+'), '変わっていない区分は書かない');
  assert.match(note, /不一致 3/);
  assert.match(note, /listing_components:4/);
  assert.ok(!note.includes('suppliers'), '0 件の未解決は書かない');
  assert.ok(note.length <= 180, `ping の note は 180 字で切られる (${note.length})`);
});

await ta('何も変わらなかった晩は「変化なし」と書く (ロードは冪等なので、これが普通の晩)', async () => {
  const ping = spyPing();
  const same = { run_id: 'load_test_2', status: 'done', summary: { products: { planned: 10, applied: 0, same: 10 } }, conflicts: 0, unresolved: {} };
  const r = await withEnv(configured, () => runNightlyLoad({ log: quiet, start: fakeStart(same), ping }));
  assert.equal(r.ok, true);
  assert.match(ping.calls[0][2], /変化なし/);
  assert.ok(!ping.calls[0][2].includes('不一致'));
});

await ta('失敗したら fail を ping する (理由つき)', async () => {
  const ping = spyPing();
  const failed = { run_id: 'load_test_3', status: 'failed', error: 'connect ECONNREFUSED', error_code: 'ECONNREFUSED' };
  const r = await withEnv(configured, () => runNightlyLoad({ log: quiet, start: fakeStart(failed), ping }));
  assert.equal(r.ok, false);
  assert.deepEqual(ping.calls[0].slice(0, 2), [JOB_ID, 'fail']);
  assert.match(ping.calls[0][2], /ECONNREFUSED/);
  assert.match(ping.calls[0][2], /load_test_3/);
});

await ta('別のロードが走っていたら見送る (二重に流さない・ping も打たない)', async () => {
  const ping = spyPing();
  const busy = () => ({ started: false, current: { run_id: 'load_manual_9', started_at: new Date().toISOString() }, done: Promise.resolve({}) });
  const r = await withEnv(configured, () => runNightlyLoad({ log: quiet, start: busy, ping }));
  assert.equal(r.skipped, true);
  assert.equal(r.ok, true, '見送りは失敗ではない');
  assert.equal(ping.calls.length, 0, '成功の ping を打つと「動いている」と誤解される');
  assert.match(r.note, /load_manual_9/);
});

await ta('見送りが長引いていたら「前の回が終わっていない」として失敗を ping する', async () => {
  // 🚨 黙って見送り続けると、止まっているのか動いているのか分からない時間ができる (Codex 2026-09-10)
  const ping = spyPing();
  const old = new Date(Date.now() - 5 * 3600 * 1000).toISOString();
  const stuck = () => ({ started: false, current: { run_id: 'load_stuck_1', started_at: old }, done: Promise.resolve({}) });
  const r = await withEnv(configured, () => runNightlyLoad({ log: quiet, start: stuck, ping }));
  assert.equal(r.ok, false);
  assert.equal(r.skipped, true);
  assert.deepEqual(ping.calls[0].slice(0, 2), [JOB_ID, 'fail']);
  assert.match(ping.calls[0][2], /前の回が終わっていない/);
  assert.match(ping.calls[0][2], /5\.0時間前から/);
  // 何時間で怒るかは env で変えられる
  const ping2 = spyPing();
  const r2 = await withEnv({ ...configured, COMPANY_DB_LOAD_SKIP_ALERT_H: '6' }, () => runNightlyLoad({ log: quiet, start: stuck, ping: ping2 }));
  assert.equal(r2.ok, true, '6 時間より短いのでまだ怒らない');
  assert.equal(ping2.calls.length, 0);
});

await ta('材料 (warehouse-mirror.db) が無ければ始めない', async () => {
  // 🚨 手元や miniPC で間違って本適用が始まらないための歯止め (Codex 2026-09-10)
  const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-nightly-empty-'));
  try {
    const ping = spyPing();
    let startCalled = false;
    const r = await withEnv({ ...configured, DATA_DIR: empty }, () => runNightlyLoad({
      log: quiet, ping, start: () => { startCalled = true; return { started: true, current: doneRun, done: Promise.resolve(doneRun) }; },
    }));
    assert.equal(r.ok, false);
    assert.equal(startCalled, false);
    assert.match(ping.calls[0][2], /材料が無い/);
  } finally { fs.rmSync(empty, { recursive: true, force: true }); }
});

await ta('開始のところが投げても、失敗として ping して終わる (cron の中で投げっぱなしにしない)', async () => {
  const ping = spyPing();
  const boom = () => { throw new Error('いきなり壊れた'); };
  const r = await withEnv(configured, () => runNightlyLoad({ log: quiet, start: boom, ping }));
  assert.equal(r.ok, false);
  assert.deepEqual(ping.calls[0].slice(0, 2), [JOB_ID, 'fail']);
  assert.match(ping.calls[0][2], /いきなり壊れた/);
});

await ta('待っても終わらないときは失敗にする (🚨 ロード自体は止まらない。次の回の見送り判定に任せる)', async () => {
  const ping = spyPing();
  const never = () => ({ started: true, current: { run_id: 'load_test_slow' }, done: new Promise(() => {}) });
  const r = await withEnv({ ...configured, COMPANY_DB_LOAD_TIMEOUT_MS: '1000' }, () => runNightlyLoad({ log: quiet, start: never, ping }));
  assert.equal(r.ok, false);
  assert.match(r.note, /待っても終わらない/);
  assert.match(r.note, /走り続けている/, 'ロードを止めたわけではないと書く');
  assert.deepEqual(ping.calls[0].slice(0, 2), [JOB_ID, 'fail']);
});

await ta('設定漏れ (DATA_DIR / COMPANY_DB_URL が無い) は失敗として ping する', async () => {
  for (const missing of [{ DATA_DIR: undefined }, { COMPANY_DB_URL: undefined }]) {
    const ping = spyPing();
    let startCalled = false;
    const r = await withEnv({ ...configured, ...missing }, () => runNightlyLoad({
      log: quiet, ping, start: () => { startCalled = true; return { started: true, current: doneRun, done: Promise.resolve(doneRun) }; },
    }));
    assert.equal(r.ok, false, JSON.stringify(missing));
    assert.equal(startCalled, false, '設定が無いのにロードを始めない');
    assert.deepEqual(ping.calls[0].slice(0, 2), [JOB_ID, 'fail']);
  }
});

await ta('上限の env が不正なら、ロードを始めずに失敗にする', async () => {
  const ping = spyPing();
  let startCalled = false;
  const r = await withEnv({ ...configured, COMPANY_DB_LOAD_TIMEOUT_MS: '10' }, () => runNightlyLoad({
    log: quiet, ping, start: () => { startCalled = true; return { started: true, current: doneRun, done: Promise.resolve(doneRun) }; },
  }));
  assert.equal(r.ok, false);
  assert.equal(startCalled, false);
  assert.match(ping.calls[0][2], /COMPANY_DB_LOAD_TIMEOUT_MS/);
});

console.log('\n開始の口 (startLoad)');

await ta('待つための約束 (done) は /status の JSON に混ざらない', async () => {
  const { startLoad, getLoadState } = await import('../apps/company-db/router.mjs');
  const r = startLoad({ dataDir: TMP, url: 'postgres://u:p@127.0.0.1:1/none', apply: false, log: quiet });
  assert.equal(r.started, true);
  assert.equal(typeof r.done.then, 'function', '終わったら分かる約束が返る');
  const shown = JSON.parse(JSON.stringify(getLoadState()));
  assert.ok(!('_done' in (shown.current || {})), '/status がそのまま JSON にするので、約束は列挙されない形で持つ');
  const cur = await r.done;                       // 接続できないので失敗で終わる (それでも resolve する)
  assert.equal(cur.status, 'failed');
  assert.equal(getLoadState().current, null, '終わったら current は空に戻る');
  assert.equal(getLoadState().last.run_id, cur.run_id);
  const shownAfter = JSON.parse(JSON.stringify(getLoadState()));
  assert.ok(!('_done' in (shownAfter.last || {})));
});

console.log('\ncron の起動');

await ta('env が無ければ起動しない (Dark Launch)', async () => {
  await withEnv({ COMPANY_DB_LOAD_CRON_ENABLED: undefined }, () => {
    assert.equal(startCompanyDbNightlyLoadCron(), null);
  });
});

await ta('cron 式が不正なら起動しない (黙って毎分動かさない)', async () => {
  await withEnv({ COMPANY_DB_LOAD_CRON_ENABLED: '1', COMPANY_DB_LOAD_CRON: 'まいばん' }, () => {
    assert.equal(startCompanyDbNightlyLoadCron(), null);
  });
});

await ta('有効なら起動する。既定は UTC 17:00 = JST 02:00 (夜間の取り込みの後・03:30 より前)', async () => {
  await withEnv({ COMPANY_DB_LOAD_CRON_ENABLED: '1', COMPANY_DB_LOAD_CRON: undefined }, () => {
    const task = startCompanyDbNightlyLoadCron();
    assert.ok(task, '起動している');
    task.stop();
  });
});

console.log('\n台帳');

await ta('台帳に登録されていて、締切が cron の既定と合っている', async () => {
  const { JOBS_REGISTRY, validateRegistry } = await import('../config/jobs-registry.mjs');
  const job = JOBS_REGISTRY.find((j) => j.id === JOB_ID);
  assert.ok(job, `台帳に ${JOB_ID} が無い (定期実行は必ず登録する)`);
  assert.equal(job.anchor_hour_jst, 2, 'cron の既定 (UTC 17:00 = JST 02:00) と締切が合っている');
  assert.equal(job.anchor_minute_jst, 0);
  assert.ok(job.owner && job.purpose && job.runbook, 'owner / purpose / runbook は必須');
  assert.ok(!/nightly\.mjs run/.test(job.schedule + job.runbook),
    '手動の案内に「別プロセスで直接動かす」を書かない (単一飛行を迂回する)');
  assert.match(job.schedule, /remote-load\.mjs/, '手動は HTTP の口 (remote-load) に統一する');
  assert.deepEqual(validateRegistry(), [], '台帳の検証が通る');
});

await ta('summarize は run_id と変化だけを短く書く', async () => {
  assert.equal(summarize({ run_id: 'r1', summary: {}, unresolved: {} }), 'run=r1 / 変化なし');
  assert.match(summarize(doneRun), /^run=load_test_1 \/ 変化 products\+2 \/ 不一致 3 \/ 未解決 listing_components:4$/);
});

console.log(`\n${passed} 件 PASS`);
