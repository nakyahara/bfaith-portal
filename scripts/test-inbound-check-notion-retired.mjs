/**
 * 🗂 Notion「在庫化作業管理」の運用廃止 (2026-09-05) → コード削除 (2026-09-09) —
 *   入荷受付チェックに Notion が残っていないこと
 *
 * 実行: node scripts/test-inbound-check-notion-retired.mjs
 *
 * 守りたいのは4つ。
 *   ① 送信のモジュール (notion-sync.js / notion.js) が無い。中にあった生きた集計は enrich.js に残っている
 *   ② Notion へ送る API は**経路ごと無い** (404)。正本を Notion に戻しても復活しない = 退路は無い
 *   ③ iPad・管理画面に Notion の操作が出ない
 *   ④ jobs-monitor が退役した id を「台帳に無い id」として毎朝鳴らさない・古い記録は消える
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

console.log('\n[3] モジュール: 送信のコードは消え、生きた集計だけ残っている');
{
  const gone = async (path) => { try { await import(path); return false; } catch (e) { return /Cannot find module|ERR_MODULE_NOT_FOUND/.test(e.message) || e.code === 'ERR_MODULE_NOT_FOUND'; } };
  ok(await gone('../apps/inbound-check/notion-sync.js'), 'apps/inbound-check/notion-sync.js は無い (17:30 の一括送信・sweep)');
  ok(await gone('../apps/inbound-check/notion.js'), 'apps/inbound-check/notion.js は無い (Notion HTTP クライアント)');
  const enrich = await import('../apps/inbound-check/enrich.js');
  ok(typeof enrich.buildEnrichContext === 'function' && typeof enrich.calcExternal === 'function',
    '⭐sweep の中にあった純粋な集計は enrich.js に残る (いろは在庫化アプリが現役で使う)');
  const svc = fs.readFileSync(new URL('../apps/iroha-work/service.js', import.meta.url), 'utf8');
  ok(/from '\.\.\/inbound-check\/enrich\.js'/.test(svc), 'iroha-work/service.js は enrich.js を見ている');
  // いろは作業アプリ側の Notion (カード読み取り・完成写真の送信・移行) は対象外 = 従来どおり動く
  const client = await import('../apps/iroha-work/notion-client.js');
  ok(typeof client.notionRequest === 'function' && typeof client.ensureCardSchema === 'function',
    'いろは作業アプリの Notion クライアントは残っている (この削除の対象外)');
  ok(typeof client.createCard === 'undefined' && typeof client.findCardsByDedupeKey === 'undefined',
    'カード作成系は送信専用だったので消えている');
  const dir = fs.readdirSync(new URL('../apps/inbound-check/', import.meta.url));
  ok(!dir.some((f) => /notion/i.test(f)), 'apps/inbound-check に notion のファイルは 1 つも無い');
}

// ─── HTTP: 入荷受付チェックの Notion の入口 ───
console.log('\n[4] HTTP: Notion へ送る API は経路ごと無い (退路でも復活しない)');
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
  eq(st.status, 200, '/api/state は今までどおり返る');
  ok(st.body && !('iroha_source' in st.body), '/api/state に iroha_source は無い (Notion ボタンを出し分ける材料が要らなくなった)');
  const a = await call('POST', '/api/notion-sync', { session: 'user', body: {} });
  eq(a.status, 404, 'iPad/セッションからの送信は 404 (経路が無い)');
  const b = await call('POST', '/admin/notion-sync', { session: 'admin', body: {} });
  eq(b.status, 404, '管理画面の送信・再送も 404');

  // ⭐退路は無い: 正本を Notion に戻しても、送る口は生えない
  setMetaValue('source_of_truth', 'notion');
  eq(sourceOfTruth(), 'notion', '正本 = notion (退路の設定)');
  const c = await call('POST', '/api/notion-sync', { session: 'user', body: {} });
  eq(c.status, 404, '⭐正本を Notion に戻しても 404 のまま (送信のコードごと消したため)');
  setMetaValue('source_of_truth', 'app');
}
server.close();

console.log('\n[5] 画面: iPad と管理画面に Notion の操作が無い');
{
  const idx = fs.readFileSync(new URL('../apps/inbound-check/views/index.html', import.meta.url), 'utf8');
  ok(!/id="notionBtn"/.test(idx) && !/Notionへ送る<\/button>/.test(idx), 'iPad に「🗂 Notionへ送る」ボタンが無い');
  ok(!/\/notion-sync/.test(idx), 'iPad から送信 API を叩く箇所が無い');
  const adm = fs.readFileSync(new URL('../apps/inbound-check/views/admin.ejs', import.meta.url), 'utf8');
  ok(!/notion/i.test(adm), '管理画面に notion の文字が 1 つも無い');
}

// ─── jobs-monitor: 退役した id を鳴らさない ───
console.log('\n[6] jobs-monitor: 退役した id は「台帳に無い id」にしない・記録は消える');
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

// ─── 管理画面: notion を渡さなくても描けること (router が渡さなくなった) ───
console.log('\n[7] 管理画面: notion 無しでレンダリングできる');
{
  const ejs = (await import('ejs')).default;
  const src = fs.readFileSync(new URL('../apps/inbound-check/views/admin.ejs', import.meta.url), 'utf8');
  const base = { title: 't', username: 'u', displayName: 'd', isAdmin: true, base: '/apps/inbound-check', active: null, batches: [], importLog: [], devices: [], enrollCodes: [], workers: [], drive: { config: {} }, workMaster: { total: 0, filled: 0 }, printAgents: [], printJobs: [], PRINT_STATE_LABELS: {}, refreshAvailable: true };
  const html = ejs.render(src, base);   // ⭐notion を渡さない
  ok(/いろはへの作業指示 \(在庫化アプリ\)/.test(html), 'いろはへの作業指示の案内は出る');
  ok(/入荷側はカードを作るだけ/.test(html), '⭐「入荷側はカードを作るだけ」と書いてある (2026-09-09 の決め)');
  ok(!/notionSyncBtn/.test(html) && !/送信待ち/.test(html), '送信ボタン・Notion 時代の件数は出ない');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
