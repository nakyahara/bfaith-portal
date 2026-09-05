/**
 * 🗂 Notion「在庫化作業管理」の運用廃止 (2026-09-05) — 入荷受付チェックの Notion の入口が閉じていること
 *
 * 実行: node scripts/test-inbound-check-notion-retired.mjs
 *
 * 守りたいのは3つ。
 *   ① 正本がアプリ (通常) のとき、Notion へ送る API は 410 で「在庫化アプリの未着手に入っている」と言う
 *      (押しても何も起きないボタンや、静かに no-op する API を残さない)
 *   ② 退路 (在庫化アプリ /admin/source で Notion に戻した) では従来どおり送れる (自動送信は無い)
 *   ③ jobs-monitor が退役した id を「台帳に無い id」として毎朝鳴らさない・古い記録は消える
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-notion-retired-'));
delete process.env.NOTION_TOKEN;
delete process.env.INBOUND_CHECK_NOTION_DB_ID;
process.env.JOBS_MONITOR_TOKEN = 'jm-test-token';

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const iroha = await import('../apps/iroha-work/db.js');
const { setMetaValue, sourceOfTruth } = iroha;

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

console.log('[1] 台帳: Notion 送信ジョブは退役として記録され、現役の台帳には無い');
{
  const reg = await import('../config/jobs-registry.mjs');
  ok(!reg.JOBS_REGISTRY.some((e) => e.id === 'inbound-check-notion-cards'), '現役の台帳に inbound-check-notion-cards は無い');
  const r = reg.RETIRED_JOBS.find((e) => e.id === 'inbound-check-notion-cards');
  ok(!!r && r.retired_at === '2026-09-05' && /在庫化アプリ/.test(r.reason) && /task-intake/.test(r.replaced_by), '退役の記録 (いつ・なぜ・何に置き換わったか)');
  eq(reg.validateRegistry(), [], '台帳のバリデーションは通る');
  ok(!reg.JOBS_REGISTRY.some((e) => /startInboundCheckNotionCron/.test(e.where || '')), 'どの台帳エントリも Notion cron を指していない');
}

console.log('\n[2] cron: 17:30 の Notion 送信は起動されない (関数ごと無い)');
{
  const sj = await import('../apps/inbound-check/sync-job.js');
  ok(typeof sj.startInboundCheckNotionCron === 'undefined', 'startInboundCheckNotionCron は export されていない');
  ok(typeof sj.startInboundCheckCron === 'function' && typeof sj.startInboundCheckPrintQueueWorker === 'function', '残す cron (Drive 取込 / 印刷キュー) はそのまま');
  const src = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  ok(!/startInboundCheckNotionCron\(\)/.test(src), 'server.js から呼び出しが消えている');
}

// ─── HTTP: 入荷受付チェックの Notion の入口 ───
console.log('\n[3] HTTP: 正本がアプリなら Notion へ送る API は 410');
const express = (await import('express')).default;
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
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const origin = `http://127.0.0.1:${server.address().port}`;
const call = async (method, url, { session = null, body = null } = {}) => {
  const headers = {};
  if (session) headers['x-test-session'] = session;
  if (body) { headers['Content-Type'] = 'application/json'; headers.Origin = origin; }
  const res = await fetch(`${origin}/apps/inbound-check${url}`, { method, headers, body: body ? JSON.stringify(body) : undefined, redirect: 'manual' });
  const ct = res.headers.get('content-type') || '';
  return { status: res.status, body: ct.includes('json') ? await res.json().catch(() => ({})) : null };
};
{
  setMetaValue('source_of_truth', 'app');
  eq(sourceOfTruth(), 'app', '正本 = app');
  const st = await call('GET', '/api/state', { session: 'user' });
  eq(st.body && st.body.iroha_source, 'app', '/api/state に iroha_source=app (iPad はこれで「Notionへ送る」を隠す)');
  const a = await call('POST', '/api/notion-sync', { session: 'user', body: {} });
  eq(a.status, 410, 'iPad/セッションからの送信は 410');
  ok(a.body.error === 'notion_retired' && /在庫化アプリ/.test(a.body.message) && a.body.app_url === '/apps/iroha-work/', `理由と行き先を返す (${a.body.message})`);
  const b = await call('POST', '/admin/notion-sync', { session: 'admin', body: {} });
  eq(b.status, 410, '管理画面の送信・再送も 410');
  eq(b.body.error, 'notion_retired', 'notion_retired');
}

console.log('\n[4] HTTP: 退路 (正本を Notion に戻した) では従来どおり通る');
{
  setMetaValue('source_of_truth', 'notion');
  eq(sourceOfTruth(), 'notion', '正本 = notion');
  const st = await call('GET', '/api/state', { session: 'user' });
  eq(st.body && st.body.iroha_source, 'notion', '/api/state に iroha_source=notion (iPad はボタンを出す)');
  const a = await call('POST', '/api/notion-sync', { session: 'user', body: {} });
  ok(a.status !== 410, `410 ではなく sweep へ進む (HTTP ${a.status})`);
  eq(a.body.error, 'not_configured', 'env が無いこのテストでは not_configured で止まる (= sweep が呼ばれた証拠)');
  setMetaValue('source_of_truth', 'app');
}
server.close();

// ─── jobs-monitor: 退役した id を鳴らさない ───
console.log('\n[5] jobs-monitor: 退役した id は「台帳に無い id」にしない・記録は消える');
{
  const store = await import('../apps/jobs-monitor/store.js');
  const { recordPing, getStates, purgeJobStates, setAlertState, getAlertState } = store;
  const jmRouter = (await import('../apps/jobs-monitor/router.js')).default;
  const now = Date.now();
  // 退路で手動実行したときの ping が来た体
  recordPing('inbound-check-notion-cards', 'ok', 'アプリ正本 — Notion カードは作らない', now);
  recordPing('really-unknown-job', 'ok', null, now);
  setAlertState('inbound-check-notion-cards', 'late', now, now);
  ok(!!getStates()['inbound-check-notion-cards'], 'ping は 200 で受けて記録される (ジョブ側を失敗させない)');

  const app2 = express();
  app2.use('/apps/jobs-monitor', jmRouter);
  const s2 = http.createServer(app2);
  await new Promise((r) => s2.listen(0, '127.0.0.1', r));
  const res = await fetch(`http://127.0.0.1:${s2.address().port}/apps/jobs-monitor/status`, { headers: { Authorization: 'Bearer jm-test-token' } });
  const j = await res.json();
  s2.close();
  eq(res.status, 200, '/status');
  ok(!j.unknownIds.includes('inbound-check-notion-cards'), '退役した id は unknownIds に出ない');
  ok(j.unknownIds.includes('really-unknown-job'), '本当に台帳に無い id は今までどおり出る');
  ok(Array.isArray(j.retiredIds) && j.retiredIds.includes('inbound-check-notion-cards'), 'retiredIds として見える');

  // 起動時に呼ばれる本物の関数で消す (RETIRED_JOBS 全部が対象)
  const { purgeRetiredJobStates } = await import('../apps/jobs-monitor/notify-job.js');
  const n = purgeRetiredJobStates();
  eq(n, 1, '起動時の purgeRetiredJobStates で job_state が 1 行消える');
  ok(!getStates()['inbound-check-notion-cards'], '消えた後は状態に無い');
  ok(getAlertState('inbound-check-notion-cards') === null, 'alert_state も消える');
  ok(!!getStates()['really-unknown-job'], '他のジョブの記録は消さない');
  eq(purgeJobStates([]), 0, '空なら何もしない');
  eq(purgeRetiredJobStates(), 0, '二度目は 0 (冪等)');
  const src = fs.readFileSync(new URL('../apps/jobs-monitor/notify-job.js', import.meta.url), 'utf8');
  ok(/setMeta\('monitoring_since'[\s\S]{0,120}purgeRetiredJobStates\(\);/.test(src), '起動処理 (monitoring_since の直後) で purgeRetiredJobStates を呼んでいる');
}

// ─── 管理画面の節: 3 つの見え方 (アプリ正本 / 退路 / 状態不明) ───
console.log('\n[6] 管理画面: 正本ごとの見え方');
{
  const ejs = (await import('ejs')).default;
  const src = fs.readFileSync(new URL('../apps/inbound-check/views/admin.ejs', import.meta.url), 'utf8');
  const base = { title: 't', username: 'u', displayName: 'd', isAdmin: true, base: '/apps/inbound-check', active: null, batches: [], importLog: [], devices: [], enrollCodes: [], workers: [], drive: { config: {} }, workMaster: { total: 0, filled: 0 }, printAgents: [], printJobs: [], PRINT_STATE_LABELS: {}, refreshAvailable: true };
  const body = (html) => html.split('<script')[0];
  const appMode = ejs.render(src, { ...base, notion: { configured: true, source: 'app', linkConflicts: [], linkConflictsTotal: 0, unsent: 3, waitingRetry: 2, blocked: [], attention: [] } });
  ok(/いろはへの作業指示 \(在庫化アプリ\)/.test(appMode) && /2026-09-05 に運用廃止/.test(appMode), 'アプリ正本: 廃止と行き先 (在庫化アプリ) を書く');
  ok(!/id="notionSyncBtn"/.test(body(appMode)) && !/送信待ち/.test(appMode) && !/再試行待ち/.test(appMode), 'アプリ正本: 送信ボタン・Notion 時代の件数を出さない');
  const notionMode = ejs.render(src, { ...base, notion: { configured: true, source: 'notion', linkConflicts: [], linkConflictsTotal: 0, unsent: 3, waitingRetry: 2, cancelPending: 0, sentRecent: 0, dbIdTail: 'abc', blocked: [], attention: [] } });
  ok(/退路モード/.test(notionMode) && /id="notionSyncBtn"/.test(body(notionMode)), '退路: 送信ボタンを出す');
  ok(!/30分おきに自動で再試行/.test(notionMode) && /自動再試行はもうありません/.test(notionMode), '退路: 「30分おきに自動で再試行」とは言わない (巡回は削除済み)');
  const unknownMode = ejs.render(src, { ...base, notion: { error: 'boom' } });
  ok(/状態を取得できませんでした: boom/.test(unknownMode) && !/在庫化アプリ\)/.test(unknownMode.split('<h2>🏠 いろはへの作業指示</h2>')[1].slice(0, 40)), '状態不明: 断定せず理由を出す');
  ok(!/id="notionSyncBtn"/.test(body(unknownMode)) && !/2026-09-05 に運用廃止になりました/.test(unknownMode), '状態不明: 送信ボタンも「廃止」の断定も出さない');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
