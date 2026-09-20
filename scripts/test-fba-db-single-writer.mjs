#!/usr/bin/env node
/**
 * test-fba-db-single-writer.mjs — fba.db の「2 プロセスの書き戻しで行が消える」事故 (2026-09-20) の再発防止の試験。
 *   ① fba-replenishment/db.js の歯止め: 外から書き換えられたファイルを黙って上書きしない (読み直して例外・やり直せば通る・相手の行は消えない)
 *   ② 朝の cron (snapshot-fba-stock.js): 常駐サーバが居れば頼んで待つ / 居なければ (接続拒否) 自分で書く / 居るのに頼めないときは自分で書かずに失敗
 *   ③ 本体 (fba-report-snapshot.js): 保存の順番・business_date・US・何も取れなかった回は失敗・「外から書かれた」を握りつぶさない
 * DATA_DIR を一時ディレクトリに向けるので、本番・開発の fba.db には触れない。SP-API にも行かない (取得は差し替え)。
 * 🚨 試験に無いもの: 常駐サーバの口 (POST /service-api/fba/snapshot-reports) を HTTP で叩くこと・SP-API への本物の要求 (マージ後に miniPC で 1 回確かめる)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-db-sw-'));
process.env.DATA_DIR = tmp;   // db.js は import の時点で DATA_DIR を読む
const dbUrl = pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'db.js')).href;
const dbFile = path.join(tmp, 'fba.db');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
/** ファイルの更新時刻が確実に変わるまで待つ (同じ ms に 2 回書くと stat で見分けられない = 試験の側の都合) */
const tick = () => new Promise((r) => setTimeout(r, 25));
const codeOf = (fn) => { try { fn(); return null; } catch (e) { return e.code || e.message; } };
/** ファイルの中身を、db.js を通さずに読む (🚨 db.js の initDb() は最後に保存する = 確認のつもりで使うとファイルを書き換えてしまう) */
const readFile = () => { const f = new Database(dbFile, { readonly: true, fileMustExist: true }); try { return { snaps: f.prepare(`select snapshot_date d, amazon_sku sku, fba_available a, fba_fc_processing p from daily_snapshots order by 1, 2`).all(), settings: Object.fromEntries(f.prepare(`select key, value from settings`).all().map((r) => [r.key, r.value])) }; } finally { f.close(); } };

console.log('① fba.db の歯止め (2 つの module の実体 = 2 プロセスの代わり)');
const A = await import(dbUrl + '?proc=A');   // 常駐サーバの役 (読んだメモリを持ち続ける)
const B = await import(dbUrl + '?proc=B');   // 朝の cron の役 (後から読んで書く)
await t('🚨 本番で起きた形: A が読む → B が読んで日次を書く → A が何か保存 … A は上書きせず例外 (FBA_DB_EXTERNAL_WRITE)。B の入れた行はファイルに残り、A のメモリにも入る。A がやり直すと通り、両方が残る', async () => {
  await A.initDb();
  A.updateSetting('draft_memo', 'a1');
  await tick();
  await B.initDb();
  B.saveRestockInventoryToDailySnapshot([{ amazon_sku: 'sku-1', fba_available: 7, fba_fc_transfer: 1, fba_fc_processing: 2, fba_customer_order: 3, fba_inbound_working: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 }], '2026-09-18');
  assert.equal(B.getDailySnapshots('sku-1').length, 1);
  assert.equal(A.getDailySnapshots('sku-1').length, 0, '前提: A のメモリは B の行を知らない');
  await tick();
  const st = A._fileStampState();
  assert.notDeepEqual(st.known, st.current, '前提: A の覚えている姿と、いまのファイルが違う');
  assert.equal(codeOf(() => A.updateSetting('draft_memo', 'a2')), 'FBA_DB_EXTERNAL_WRITE');
  // ファイルは B が書いたまま (A に上書きされていない) — 別の実体で読み直して確かめる
  const f1 = readFile();
  assert.deepEqual([f1.snaps.map((r) => [r.d, r.sku, r.a, r.p]), f1.settings.draft_memo], [[['2026-09-18', 'sku-1', 7, 2]], 'a1']);
  // A は読み直した = B の行が見える・自分の保存し損ねた変更は無い
  assert.deepEqual([A.getDailySnapshots('sku-1').length, A.getSettings().draft_memo], [1, 'a1']);
  // やり直せば通る。両方残る
  A.updateSetting('draft_memo', 'a2');
  const f2 = readFile();
  assert.deepEqual([f2.snaps.length, f2.settings.draft_memo], [1, 'a2']);
});
await t('取引つきの保存 (BEGIN → COMMIT → 保存) でも同じ例外が出る: catch の ROLLBACK が「取引が無い」で元の例外を隠さない。古くなった側 (今度は B) は、もう一度で通る', async () => {
  // 直前の試験で最後に書いたのは A = B のメモリは古い。A にもう一度書かせてから、B に取引つきの保存をさせる
  await tick();
  A.updateSetting('draft_memo', 'a3');
  await tick();
  const rows = [{ amazon_sku: 'sku-2', fba_available: 1, fba_fc_transfer: 0, fba_fc_processing: 0, fba_customer_order: 0, fba_inbound_working: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 }];
  let err = null;
  try { B.saveRestockInventoryToDailySnapshot(rows, '2026-09-19'); } catch (e) { err = e; }
  assert.equal(err && err.code, 'FBA_DB_EXTERNAL_WRITE', `元の例外が隠れた: ${err && err.message}`);
  assert.match(err.message, /もう一度実行する/);
  B.saveRestockInventoryToDailySnapshot(rows, '2026-09-19');
  const f3 = readFile();
  assert.deepEqual([f3.snaps.map((r) => r.sku), f3.settings.draft_memo], [['sku-1', 'sku-2'], 'a3']);
});
await t('ふつうの 1 プロセスの連続した保存は止めない / ファイルが消えていたら (失うものが無いので) そのまま書く', async () => {
  const F = await import(dbUrl + '?proc=F'); await F.initDb();
  for (let i = 0; i < 5; i++) F.updateSetting('n', String(i));
  assert.equal(F.getSettings().n, '4');
  fs.rmSync(dbFile);
  F.updateSetting('n', 'after-delete');
  assert.equal(fs.existsSync(dbFile), true);
  assert.equal(readFile().settings.n, 'after-delete');
});

console.log('② 朝の cron: 常駐サーバに頼む / 居なければ自分で書く / 居るのに頼めなければ失敗');
const { snapshotViaServer, runSnapshotCli, isConnectionRefused, resolveBusinessDate } = await import(pathToFileURL(path.join(root, 'apps', 'warehouse', 'snapshot-fba-stock.js')).href);
const json = (status, body) => ({ ok: status >= 200 && status < 300, status, json: async () => body });
const refused = () => Object.assign(new TypeError('fetch failed'), { cause: Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3000'), { code: 'ECONNREFUSED' }) });
const via = (fetchImpl, extra = {}) => snapshotViaServer({ businessDate: '2026-09-20', base: 'http://127.0.0.1:3000', token: 'tok', fetchImpl, sleepFn: async () => {}, log: quiet, ...extra });
await t('常駐サーバが居る: POST (business_date とトークンつき) → ジョブを待つ (running → completed) → 結果をそのまま返す', async () => {
  const calls = []; let polls = 0;
  const f = async (url, init = {}) => {
    calls.push([init.method || 'GET', url.replace('http://127.0.0.1:3000', ''), init.headers && init.headers['x-service-token'], init.body || null]);
    if ((init.method || 'GET') === 'POST') return json(202, { ok: true, jobId: 'job-1', status: 'running' });
    polls++;
    return polls < 3 ? json(200, { ok: true, jobId: 'job-1', status: 'running', progress: { step: 'fetching' } }) : json(200, { ok: true, jobId: 'job-1', status: 'completed', result: { ok: true, lastLine: '✅ FBA在庫スナップショット 2026-09-20: JP restock=3992 planning=3999 / US planning=15 restock=15' } });
  };
  const r = await via(f);
  assert.deepEqual([r.mode, r.result.ok, polls], ['server', true, 3]);
  assert.deepEqual(calls[0], ['POST', '/service-api/fba/snapshot-reports', 'tok', JSON.stringify({ businessDate: '2026-09-20' })]);
  assert.deepEqual(calls[1].slice(0, 3), ['GET', '/service-api/jobs/job-1', 'tok']);
});
await t('🚨 「居ない」と判定するのは接続拒否 (ECONNREFUSED) だけ: 応答なし (timeout)・401・404・500 は「居るかもしれない」= 例外 (自分では書かない)', async () => {
  assert.deepEqual(await via(async () => { throw refused(); }), { mode: 'not_running' });
  assert.equal(isConnectionRefused(Object.assign(new TypeError('fetch failed'), { cause: Object.assign(new AggregateError([Object.assign(new Error('x'), { code: 'ECONNREFUSED' }), Object.assign(new Error('y'), { code: 'ECONNREFUSED' })]), {}) })), true);
  assert.equal(isConnectionRefused(Object.assign(new TypeError('fetch failed'), { cause: new AggregateError([Object.assign(new Error('x'), { code: 'ECONNREFUSED' }), Object.assign(new Error('y'), { code: 'ETIMEDOUT' })]) })), false);
  await assert.rejects(via(async () => { throw Object.assign(new Error('The operation was aborted due to timeout'), { name: 'TimeoutError' }); }), /自分では fba\.db に書かない/);
  await assert.rejects(via(async () => json(401, { ok: false })), /HTTP 401 = SERVICE_TOKEN/);
  await assert.rejects(via(async () => json(404, { ok: false })), /HTTP 404 = 常駐サーバが古い版/);
  await assert.rejects(via(async () => json(500, { ok: false })), /HTTP 500/);
});
await t('ジョブの失敗・ジョブが消えた (途中で再起動)・時間切れ は例外 / 確認の一時的な失敗は続ける / 既に実行中は busy', async () => {
  const post = json(202, { ok: true, jobId: 'job-2', status: 'running' });
  const seq = (answers) => { let i = 0; return async (url, init = {}) => ((init.method || 'GET') === 'POST' ? post : answers[Math.min(i++, answers.length - 1)]()); };
  await assert.rejects(via(seq([() => json(200, { ok: true, status: 'failed', error: { code: 'X', message: 'SP-API 403' } })])), /ジョブが失敗: SP-API 403/);
  await assert.rejects(via(seq([() => json(404, { ok: false, error: 'JOB_NOT_FOUND' })])), /知らない \(途中で再起動した\?\)/);
  let clock = 0;
  await assert.rejects(via(seq([() => json(200, { ok: true, status: 'running' })]), { now: () => (clock += 60000), waitMs: 5 * 60000 }), /5 分で終わらない/);
  const flaky = await via(seq([() => { throw new Error('socket hang up'); }, () => json(503, null), () => json(200, { ok: true, status: 'completed', result: { ok: true, lastLine: '✅ x' } })]));
  assert.equal(flaky.mode, 'server');
  assert.deepEqual(await via(async () => json(202, { ok: true, status: 'already_running', holder: { kind: 'manual', pid: 1 } })), { mode: 'busy', holder: { kind: 'manual', pid: 1 } });
});
await t('CLI: 居なければ direct を呼ぶ・居れば呼ばない・頼めなければ direct を呼ばずに例外 / 終了コードは結果の ok / 最後の行に経路 / 既に実行中は ⏭️ で exit 0 / business_date の解決', async () => {
  const env = { WAREHOUSE_BUSINESS_DATE: '2026-09-20', PORT: '3100', SERVICE_TOKEN: 'tok' };
  let directCalls = 0; const direct = async (d) => { directCalls++; return { mode: 'direct', result: { ok: true, lastLine: `✅ FBA在庫スナップショット ${d}: JP restock=1 planning=1 / US 未設定` } }; };
  const seenBase = [];
  const r1 = await runSnapshotCli({ env, argv: [], viaServer: async (o) => { seenBase.push([o.base, o.token, o.businessDate]); return { mode: 'not_running' }; }, direct, log: quiet });
  assert.deepEqual([r1.exitCode, directCalls, seenBase[0]], [0, 1, ['http://127.0.0.1:3100', 'tok', '2026-09-20']]);
  assert.match(r1.lastLine, /^✅ FBA在庫スナップショット 2026-09-20: .*\[直接 \(常駐サーバは起動していない\)\]$/);
  const r2 = await runSnapshotCli({ env, argv: [], viaServer: async () => ({ mode: 'server', result: { ok: false, lastLine: '❌ FBA在庫スナップショット 2026-09-20: JP のレポートが 1 つも取れなかった' } }), direct, log: quiet });
  assert.deepEqual([r2.exitCode, directCalls, r2.lastLine.endsWith('[常駐サーバ経由]')], [1, 1, true]);
  await assert.rejects(runSnapshotCli({ env, argv: [], viaServer: async () => { throw new Error('常駐サーバに頼めなかった (HTTP 401)'); }, direct, log: quiet }), /HTTP 401/);
  assert.equal(directCalls, 1, '頼めなかったのに自分で書いている');
  const busy = await runSnapshotCli({ env, argv: [], viaServer: async () => ({ mode: 'busy', holder: { kind: 'manual' } }), direct, log: quiet });
  assert.deepEqual([busy.exitCode, busy.lastLine.startsWith('⏭️ FBA在庫スナップショット: 既に実行中のためスキップ')], [0, true]);
  assert.equal((await runSnapshotCli({ env: { WAREHOUSE_BUSINESS_DATE: '2026-13-01' }, argv: [], viaServer: async () => ({ mode: 'not_running' }), direct, log: quiet })).exitCode, 1);
  assert.deepEqual([resolveBusinessDate({}, ['7', '--date=2026-09-01']), resolveBusinessDate({}, ['7'], new Date('2026-09-19T16:00:00Z'))], ['2026-09-01', '2026-09-20'], 'daily-sync の runScript は引数が無いと 7 を足す / 既定は JST');
});

console.log('③ 本体: 保存の順番と、黙った緑にしないこと');
const { runFbaReportSnapshot, isBusinessDate } = await import(pathToFileURL(path.join(root, 'apps', 'warehouse', 'fba-report-snapshot.js')).href);
const fakeDb = (over = {}) => {
  const calls = [];
  const rec = (name, ret) => (...args) => { calls.push([name, ...args.map((a) => (Array.isArray(a) ? a.length : a && typeof a === 'object' ? Object.keys(a).sort().join(',') : a))]); return typeof ret === 'function' ? ret(...args) : ret; };
  return { calls, saveRestockInventoryToDailySnapshot: rec('restockDaily', (rows) => ({ updated: 0, inserted: rows.length })), saveRestockLatest: rec('restockLatest', (rows) => ({ saved: rows.length })), updateFnskuBatch: rec('fnskuUpdate'),
    savePlanningData: rec('planning', (rows) => rows.length), savePlanningLatest: rec('planningLatest', (rows) => ({ saved: rows.length })), syncFnskuBatch: rec('fnskuSync'), saveUsDailySnapshots: rec('us', () => ({ inserted: 1, updated: 0 })), ...over };
};
const restockRow = (sku) => ({ 'Merchant SKU': sku, FNSKU: 'X00' + sku, ASIN: 'B0' + sku, Available: '3' });
const planningRow = (sku) => ({ sku, fnsku: 'X00' + sku, available: '3' });
const noUs = { market: 'us' };
await t('保存の順番は RESTOCK 先行 → PLANNING (在庫の区分を後から 0 で潰さない)。どの保存にも同じ business_date を渡す (UTC の今日にしない)。US は env がそろっているときだけ', async () => {
  const db = fakeDb();
  const r = await runFbaReportSnapshot({ db, businessDate: '2026-09-20', fetchReports: async () => ({ restock: [restockRow('a'), restockRow('b')], planning: [planningRow('a'), planningRow('b'), planningRow('c')], errors: [] }), usContext: noUs, log: quiet, warn: quiet });
  assert.deepEqual(db.calls.map((c) => c[0]), ['restockDaily', 'restockLatest', 'fnskuUpdate', 'planning', 'planningLatest', 'fnskuSync']);
  assert.deepEqual([db.calls[0][2], db.calls[3][2]], ['2026-09-20', '2026-09-20']);
  assert.deepEqual([r.ok, r.us, r.jp.restockDaily, r.jp.planning], [true, null, 2, 3]);
  assert.match(r.lastLine, /^✅ FBA在庫スナップショット 2026-09-20: JP restock=2 planning=3 \/ US 未設定$/);
  const db2 = fakeDb(); const seenCtx = [];
  const us = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
  const r2 = await runFbaReportSnapshot({ db: db2, businessDate: '2026-09-20', fetchReports: async (ctx) => { seenCtx.push(ctx ? ctx.market : 'jp'); return { restock: [restockRow('a')], planning: [planningRow('a')], errors: [] }; }, usContext: us, log: quiet, warn: quiet });
  assert.deepEqual([seenCtx, db2.calls.at(-1)[0], r2.us.planning, /US planning=1 restock=1$/.test(r2.lastLine)], [['jp', 'us'], 'us', 1, true]);
});
await t('🚨 黙った緑にしない: JP が 1 つも取れなかった回は ok = false (今までは exit 0。9/17 の 403 の朝も ✅ だった) / RESTOCK だけ取れなかった回は ⚠️ +「0 ではなく不明」/ US の失敗は全体を落とさない', async () => {
  const none = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-17', fetchReports: async () => ({ restock: null, planning: null, errors: [{ report: 'restock', error: 'Access to requested resource is denied.' }, { report: 'planning', error: 'Access to requested resource is denied.' }] }), usContext: noUs, log: quiet, warn: quiet });
  assert.equal(none.ok, false);
  assert.match(none.lastLine, /^❌ FBA在庫スナップショット 2026-09-17: JP のレポートが 1 つも取れなかった \(restock: Access to requested/);
  const noRestock = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-20', fetchReports: async () => ({ restock: [], planning: [planningRow('a')], errors: [{ report: 'restock', error: 'timeout' }] }), usContext: noUs, log: quiet, warn: quiet });
  assert.equal(noRestock.ok, true);
  assert.match(noRestock.lastLine, /^⚠️ .*取れなかったレポート: restock.*RESTOCK が取れていない = この日の FC 移管中・処理中・出荷待ちは 0 ではなく不明/);
  const us = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
  const usFail = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-20', fetchReports: async (ctx) => { if (ctx) throw new Error('US 403'); return { restock: [restockRow('a')], planning: [planningRow('a')], errors: [] }; }, usContext: us, log: quiet, warn: quiet });
  assert.deepEqual([usFail.ok, usFail.us.error, /US ❌ US 403/.test(usFail.lastLine)], [true, 'US 403', true]);
  assert.deepEqual([isBusinessDate('2026-09-20'), isBusinessDate('2026-02-30'), isBusinessDate('2026-9-1'), isBusinessDate(undefined)], [true, false, false, false]);
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb(), businessDate: 'x', fetchReports: async () => ({}), usContext: noUs, log: quiet }), /business_date が不正/);
});
await t('🚨 「外から書き換えられていた」(FBA_DB_EXTERNAL_WRITE) は、失敗を警告にする try の中でも握りつぶさない (保存されていない回を成功にしない)', async () => {
  const ext = () => { throw Object.assign(new Error('fba.db がほかのプロセスに書き換えられていた'), { code: 'FBA_DB_EXTERNAL_WRITE' }); };
  const fetchReports = async () => ({ restock: [restockRow('a')], planning: [planningRow('a')], errors: [] });
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb({ saveRestockLatest: ext }), businessDate: '2026-09-20', fetchReports, usContext: noUs, log: quiet, warn: quiet }), (e) => e.code === 'FBA_DB_EXTERNAL_WRITE');
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb({ savePlanningLatest: ext }), businessDate: '2026-09-20', fetchReports, usContext: noUs, log: quiet, warn: quiet }), (e) => e.code === 'FBA_DB_EXTERNAL_WRITE');
  const us = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb({ saveUsDailySnapshots: ext }), businessDate: '2026-09-20', fetchReports, usContext: us, log: quiet, warn: quiet }), (e) => e.code === 'FBA_DB_EXTERNAL_WRITE');
  // ふつうの失敗 (saveRestockLatest) は今までどおり警告で続ける
  const soft = await runFbaReportSnapshot({ db: fakeDb({ saveRestockLatest: () => { throw new Error('guard'); } }), businessDate: '2026-09-20', fetchReports, usContext: noUs, log: quiet, warn: quiet });
  assert.deepEqual([soft.ok, soft.jp.restockLatest], [true, 0]);
});
await t('常駐サーバの口と cron の形 (ソース): 口は lock を取り、本体 (runFbaReportSnapshot) を常駐の DB で呼ぶ / cron は自分で initDb() するのを direct の中だけにしている', async () => {
  const svc = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'fba-service.js'), 'utf8');
  const i = svc.indexOf("router.post('/snapshot-reports'"), j = svc.indexOf("router.post('/pml/fba-refresh'");
  assert.ok(i > 0 && j > i);
  const route = svc.slice(i, j);
  assert.match(route, /acquireFbaFetchLock\('cron-via-server'\)/);
  assert.match(route, /runFbaReportSnapshot\(\{ db, businessDate, log \}\)/);
  assert.match(route, /releaseFbaFetchLock\(lock\)/);
  const cron = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'snapshot-fba-stock.js'), 'utf8');
  assert.equal((cron.match(/initDb\(\)/g) || []).length, 1);
  assert.match(cron, /async function snapshotDirect[\s\S]*?await db\.initDb\(\);[\s\S]*?\n}/);
  assert.equal(/from '\.\.\/fba-replenishment\/db\.js'/.test(cron), false, 'cron が db.js を静的に import している (常駐サーバ経由のときも fba.db を開く準備をしてしまう)');
});

fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
