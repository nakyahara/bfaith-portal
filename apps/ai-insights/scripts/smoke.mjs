#!/usr/bin/env node
// ai-insights smoke test
// 実行: node apps/ai-insights/scripts/smoke.mjs
// 一時 DATA_DIR に実 DB を作り、router を express に直 mount して検証する
// (requireAppAccess は通さない。session は fake を注入)

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import express from 'express';

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'ai-insights-smoke-'));
process.env.DATA_DIR = scratch;
process.env.AI_READ_TOKEN = 'smoke-read-token';
process.env.AI_INSIGHT_SERVICE_TOKEN = 'smoke-service-token';

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '../../..');
const load = (rel) => import(pathToFileURL(path.join(repoRoot, rel)).href);

const { initMirrorDB, getMirrorDB } = await load('apps/warehouse-mirror/db.js');
initMirrorDB();
const { default: uiRouter, aiInsightsApiRouter } = await load('apps/ai-insights/router.js');
const factsMod = await load('apps/ai-insights/facts.js');

let pass = 0, fail = 0;
function ok(cond, name, extra) {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name}${extra !== undefined ? ` — ${JSON.stringify(extra)?.slice(0, 300)}` : ''}`); }
}

const app = express();
app.use((req, res, next) => { req.session = { authenticated: true, username: 'smoke' }; next(); });
app.use('/api/ai-insights', aiInsightsApiRouter);
app.use('/apps/ai-insights', express.json({ limit: '256kb' }), uiRouter);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}`;

async function j(pathname, opt = {}) {
  const res = await fetch(base + pathname, opt);
  let body = null;
  try { body = await res.json(); } catch { body = null; }
  return { status: res.status, body };
}
const READ = { 'x-read-token': 'smoke-read-token' };
const SVC = { authorization: 'Bearer smoke-service-token', 'content-type': 'application/json' };
const post = (p, body, headers) => j(p, { method: 'POST', headers: headers || { 'content-type': 'application/json' }, body: JSON.stringify(body || {}) });

const db = getMirrorDB();

// ═══ fixtures ═══
// 対象週: 2026-07-06(月)〜2026-07-12(日)
const PS = '2026-07-06';
const insSales = db.prepare(`
  INSERT INTO mirror_f_sales_by_listing
    (date_jst, month_ym, mall, item_code, channel, item_name, units, sales_jpy_incl, order_count,
     data_source, source_updated_at, source_run_id, source_row_hash, synced_at)
  VALUES (?, ?, ?, ?, '', ?, ?, ?, 1, 'smoke', '2026-07-13', 'run1', ?, '2026-07-13T00:00:00Z')
`);
// 楽天: 今週 + 前週 + 4週前 (wow / median 検証用)
insSales.run('2026-07-06', '2026-07', 'rakuten', 'r-001', '商品A', 10, 10000, 'h1');
insSales.run('2026-07-11', '2026-07', 'rakuten', 'r-001', '商品A', 5, 5000, 'h2');
insSales.run('2026-06-29', '2026-06', 'rakuten', 'r-001', '商品A', 8, 10000, 'h3');
insSales.run('2026-06-08', '2026-06', 'rakuten', 'r-001', '商品A', 8, 8000, 'h4');
// amazon: 過去28日に実績あり・今週 0 行 → suspect (全欠損パターン)
insSales.run('2026-06-15', '2026-06', 'amazon', 'a-001', '商品B', 3, 3000, 'h5');
// qoo10: 今週は月曜のみ・過去4週は火曜4回+水曜3回に実績 → 部分欠損 suspect (曜日パターン照合)
insSales.run('2026-07-06', '2026-07', 'qoo10', 'q-001', '商品C', 1, 500, 'h6');
for (const d of ['2026-06-09', '2026-06-16', '2026-06-23', '2026-06-30']) {
  insSales.run(d, d.slice(0, 7), 'qoo10', 'q-001', '商品C', 1, 500, 'ht' + d);
}
for (const d of ['2026-06-10', '2026-06-17', '2026-06-24']) {
  insSales.run(d, d.slice(0, 7), 'qoo10', 'q-001', '商品C', 1, 500, 'hw' + d);
}

const insChunk = db.prepare(`
  INSERT INTO sync_run_chunks
    (run_id, entity, chunk_index, chunk_count, row_count, payload_checksum, contract_version,
     scope_from, scope_to, received_at, applied_at)
  VALUES (?, ?, 0, 1, ?, 'c', 1, ?, ?, '2026-07-13T00:00:00Z', '2026-07-13T00:05:00Z')
`);
// 売上: 対象週を含む広い scope で取得完了
insChunk.run('sr1', 'f_sales_by_listing', 100, '2026-06-01', '2026-07-12');
// finance: 楽天のみ取得完了 (他モールは missing → 粗利論点禁止)
insChunk.run('sr2', 'rakuten_finance_sku_daily', 50, '2026-07-01', '2026-07-12');
// yahoo finance: chunk_count=2 のうち 1 chunk だけ applied = 未完了 run → 完了扱いしない
db.prepare(`
  INSERT INTO sync_run_chunks
    (run_id, entity, chunk_index, chunk_count, row_count, payload_checksum, contract_version,
     scope_from, scope_to, received_at, applied_at)
  VALUES ('sr3', 'yahoo_finance_sku_daily', 0, 2, 10, 'c', 1, '2026-07-06', '2026-07-12',
          '2026-07-13T00:00:00Z', '2026-07-13T00:05:00Z')
`).run();
// aupay finance: chunk_count=2 が両方 applied = 完了 run → 取得完了
db.prepare(`
  INSERT INTO sync_run_chunks
    (run_id, entity, chunk_index, chunk_count, row_count, payload_checksum, contract_version,
     scope_from, scope_to, received_at, applied_at)
  VALUES
    ('sr4', 'aupay_finance_sku_daily', 0, 2, 10, 'c', 1, '2026-07-06', '2026-07-09', '2026-07-13T00:00:00Z', '2026-07-13T00:05:00Z'),
    ('sr4', 'aupay_finance_sku_daily', 1, 2, 10, 'c', 1, '2026-07-10', '2026-07-12', '2026-07-13T00:00:00Z', '2026-07-13T00:05:00Z')
`).run();

const insInv = db.prepare(`
  INSERT INTO mirror_inv_daily_summary
    (business_date, market, category, total_qty, total_value, resolved_count, unresolved_count,
     cost_missing_count, source_status, source_row_count, captured_at, synced_at)
  VALUES (?, 'jp', ?, ?, ?, 1, 0, 0, 'ok', 1, '2026-07-13T00:00:00Z', '2026-07-13T00:00:00Z')
`);
for (const d of ['2026-07-06', '2026-07-07', '2026-07-08', '2026-07-09', '2026-07-10', '2026-07-11']) {
  insInv.run(d, 'fba_warehouse', 950, 4700000);
}
insInv.run('2026-07-12', 'fba_warehouse', 1000, 5000000);
insInv.run('2026-07-05', 'fba_warehouse', 900, 4500000);
// own_warehouse: 中間日1日だけ → partial (最終日なし・5日未満)
insInv.run('2026-07-08', 'own_warehouse', 100, 300000);

// ═══ 1. 認証 ═══
console.log('\n■ 認証 (read token / service token)');
{
  let r = await j(`/api/ai-insights/report-input?type=weekly&period_start=${PS}`);
  ok(r.status === 401, 'report-input: トークンなし → 401', r);
  r = await j(`/api/ai-insights/report-input?period_start=${PS}`, { headers: { 'x-read-token': 'wrong' } });
  ok(r.status === 401, 'report-input: 不正トークン → 401', r);
  r = await j(`/api/ai-insights/report-input?period_start=${PS}&token=smoke-read-token`, { headers: READ });
  ok(r.status === 400, 'report-input: query トークン → 400 (URL残留対策)', r);
  r = await post('/api/ai-insights/service/jobs/claim', {}, { 'content-type': 'application/json' });
  ok(r.status === 401, 'service: トークンなし → 401', r);
  r = await post('/api/ai-insights/service/jobs/claim', {}, { authorization: 'Bearer wrong', 'content-type': 'application/json' });
  ok(r.status === 403, 'service: 不正トークン → 403', r);
  const saveRead = process.env.AI_READ_TOKEN;
  delete process.env.AI_READ_TOKEN;
  r = await j(`/api/ai-insights/report-input?period_start=${PS}`, { headers: READ });
  ok(r.status === 503, 'report-input: env 未設定 → 503 fail-closed', r);
  process.env.AI_READ_TOKEN = saveRead;
}

// ═══ 2. report-input バリデーション ═══
console.log('\n■ report-input バリデーション');
{
  let r = await j('/api/ai-insights/report-input?type=quarterly', { headers: READ });
  ok(r.status === 400, '未知の type → 400', r);
  r = await j('/api/ai-insights/report-input?period_start=2026-07-07', { headers: READ });
  ok(r.status === 400, '月曜以外の period_start → 400', r);
  r = await j('/api/ai-insights/report-input?period_start=garbage', { headers: READ });
  ok(r.status === 400, '不正日付 → 400', r);
}

// ═══ 3. 事実層 + 充足判定 ═══
console.log('\n■ 事実層 + 充足判定');
{
  const r = await j(`/api/ai-insights/report-input?period_start=${PS}`, { headers: READ });
  ok(r.status === 200, 'report-input 200', r);
  const inp = r.body;
  ok(inp.meta.period_start === PS && inp.meta.period_end === '2026-07-13', 'period 境界 (排他的)', inp.meta);
  ok(inp.constraints.generation === 'allowed', '楽天が生きていれば生成 allowed', inp.constraints);
  ok(inp.constraints.data_quality_status === 'degraded', '欠損ありなので degraded', inp.constraints);
  const areas = inp.constraints.prohibited_topic_areas.map((p) => p.area);
  ok(areas.includes('sales:amazon'), 'amazon 売上 suspect → 論点禁止', areas);
  ok(areas.includes('company_totals'), '欠損モールあり → 全社合計論点禁止', areas);
  ok(areas.includes('margin:yahoo'), 'finance 未完了 run (chunk 1/2) → 完了扱いせず粗利論点禁止', areas);
  ok(!areas.includes('margin:aupay'), '完了 run (chunk 2/2 applied) → 取得完了として扱う', areas);
  ok(!areas.includes('margin:rakuten'), '楽天 finance は取込済 → 禁止されない', areas);
  ok(!areas.includes('sales:rakuten'), '楽天売上は ok', areas);
  ok(areas.includes('inventory:own_warehouse'), '自社倉庫在庫なし → 禁止', areas);
  ok(!areas.includes('inventory:fba_warehouse'), 'FBA在庫はあり → 禁止されない', areas);

  const covSalesAmazon = inp.coverage.find((c) => c.source_id === 'sales:amazon');
  ok(covSalesAmazon?.status === 'suspect', 'amazon = suspect (マーカーありデータ0行)', covSalesAmazon);
  const covSalesQoo10 = inp.coverage.find((c) => c.source_id === 'sales:qoo10');
  ok(covSalesQoo10?.status === 'suspect', 'qoo10 = suspect (曜日パターン照合で部分欠損検出)', covSalesQoo10);
  const covInvOwn = inp.coverage.find((c) => c.source_id === 'inventory:own_warehouse');
  ok(covInvOwn?.status === 'partial', 'own_warehouse = partial (中間日1日のみ)', covInvOwn);

  const rk = inp.facts.sales.by_mall.find((m) => m.mall === 'rakuten');
  ok(rk?.sales_yen === 15000 && rk?.units === 15, '楽天 今週売上 15,000円/15個', rk);
  ok(rk?.prev_week_sales_yen === 10000, '楽天 前週 10,000円', rk);
  ok(rk?.wow_pct === 50, '楽天 WoW +50%', rk);
  ok(rk?.prior_4w_median_sales_yen === 4000, '過去4週中央値 (10000,8000,0,0 → 4000)', rk);
  ok(inp.facts.sales.company.sales_yen === 15500, '全社今週 15,500円 (楽天15,000+qoo10 500)', inp.facts.sales.company);

  const fba = inp.facts.inventory.by_category.find((c) => c.category === 'fba_warehouse');
  ok(fba?.value_yen === 5000000 && fba?.week_ago_value_yen === 4500000, '在庫金額 今週/先週', fba);
  ok(inp.facts.mf.available === false, 'MF データなし → available:false', inp.facts.mf);
  ok(inp.facts.po.error != null || inp.facts.po.outstanding_qty != null, '発注残は error か値のどちらか', inp.facts.po);
  ok(inp.budget.available === false, '予算未入力 → available:false', inp.budget);
  ok(String(inp.budget.note).includes('生成禁止'), '予算未入力 note に生成禁止の契約', inp.budget);
}

// blocked: chunk の無い週
{
  const r = await j('/api/ai-insights/report-input?period_start=2026-01-05', { headers: READ });
  ok(r.status === 200 && r.body.constraints.generation === 'blocked', '全売上未取込の週 → blocked', r.body?.constraints);
  ok(r.body.facts === null, 'blocked 時は facts を組み立てない', null);
}

// ═══ 4. 予算 API ═══
console.log('\n■ 予算 API (append-only revision)');
{
  let r = await post('/apps/ai-insights/api/budget', { entries: [{ year_month: '2026-13', metric: 'sales', value_yen: 1 }] });
  ok(r.status === 400, '不正 year_month → 400', r);
  r = await post('/apps/ai-insights/api/budget', { entries: [{ year_month: '2026-07', metric: 'gross_margin_pct', value_yen: 1 }] });
  ok(r.status === 400, '粗利率 metric は存在しない → 400', r);
  r = await post('/apps/ai-insights/api/budget', { entries: [{ year_month: '2026-07', metric: 'sales', value_yen: 100.5 }] });
  ok(r.status === 400, '非整数 → 400', r);
  r = await post('/apps/ai-insights/api/budget', {
    entries: [
      { year_month: '2026-07', metric: 'sales', value_yen: 50000000 },
      { year_month: '2026-07', metric: 'gross_profit', value_yen: 15000000 },
    ],
  });
  ok(r.status === 200 && r.body.saved === 2, '予算保存 2件', r);
  r = await post('/apps/ai-insights/api/budget', { entries: [{ year_month: '2026-07', metric: 'sales', value_yen: 50000000 }] });
  ok(r.status === 200 && r.body.saved === 0 && r.body.unchanged === 1, '同値再送 → 新 revision を作らない', r);
  r = await post('/apps/ai-insights/api/budget', { entries: [{ year_month: '2026-07', metric: 'sales', value_yen: 55000000 }] });
  ok(r.status === 200 && r.body.saved === 1, '値の変更 → 新 revision', r);
  const cur = db.prepare("SELECT value_yen, revision_no FROM v_ai_budget_current WHERE year_month='2026-07' AND metric='sales' AND budget_kind='initial'").get();
  ok(cur?.value_yen === 55000000 && cur?.revision_no === 2, 'current view が最新 revision を返す', cur);
  const hist = db.prepare("SELECT COUNT(*) AS c FROM ai_budget WHERE year_month='2026-07' AND metric='sales'").get();
  ok(hist.c === 2, '履歴は append-only で残る', hist);

  const inp = (await j(`/api/ai-insights/report-input?period_start=${PS}`, { headers: READ })).body;
  ok(inp.budget.available === true && inp.budget.entries.length === 2, 'report-input に予算が載る', inp.budget);
}

// ═══ 5. イベント API ═══
console.log('\n■ イベントメモ API');
{
  let r = await post('/apps/ai-insights/api/events', { event_date: '2026/07/08', description: 'x' });
  ok(r.status === 400, '不正日付 → 400', r);
  r = await post('/apps/ai-insights/api/events', { event_date: '2026-07-08', event_end_date: '2026-07-07', description: 'x' });
  ok(r.status === 400, '終了日 < 開始日 → 400', r);
  r = await post('/apps/ai-insights/api/events', { event_date: '2026-07-08', description: '楽天セール参加', tag: 'sale', scope: 'rakuten' });
  ok(r.status === 200, 'イベント追加', r);
  const evId = r.body.event_id;
  r = await post('/apps/ai-insights/api/events', { event_date: '2026-05-01', description: '過去イベント (期間外)' });
  ok(r.status === 200, '期間外イベント追加', r);
  const inp = (await j(`/api/ai-insights/report-input?period_start=${PS}`, { headers: READ })).body;
  ok(inp.events.entries.length === 1 && inp.events.entries[0].description === '楽天セール参加', '期間重複イベントのみ input に載る', inp.events);
  r = await post(`/apps/ai-insights/api/events/${evId}/delete`, {});
  ok(r.status === 200, 'イベント論理削除', r);
  r = await post(`/apps/ai-insights/api/events/${evId}/delete`, {});
  ok(r.status === 404, '二重削除 → 404', r);
}

// ═══ 6. ジョブ状態機械 ═══
console.log('\n■ ジョブ状態機械 (claim → report → posting → posted)');
const KEY = { report_type: 'weekly', period_start: PS, period_end: '2026-07-13', edition: 'final', claimed_by: 'smoke' };
{
  let r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_end: '2026-07-14' }, SVC);
  ok(r.status === 400, '週次 7日以外 → 400', r);
  r = await post('/api/ai-insights/service/jobs/claim', KEY, SVC);
  ok(r.status === 200 && r.body.result === 'claimed', 'claim → claimed', r);
  const jobId = r.body.job.job_id;
  r = await post('/api/ai-insights/service/jobs/claim', KEY, SVC);
  ok(r.status === 200 && r.body.result === 'busy', '二重 claim → busy (原子的獲得)', r);
  r = await post(`/api/ai-insights/service/jobs/${jobId}/heartbeat`, {}, SVC);
  ok(r.status === 200, 'heartbeat', r);
  r = await post(`/api/ai-insights/service/jobs/${jobId}/report`, { generation_mode: 'bad', body_json: {} }, SVC);
  ok(r.status === 400, '不正 generation_mode → 400', r);
  r = await post(`/api/ai-insights/service/jobs/${jobId}/report`, {
    generation_mode: 'claude',
    body_json: { summary: '今週の総括', topics: [] },
    topics: [{ title: '楽天売上が前週比+50%', category: 'sales' }],
    input_hash: 'ih1', data_as_of: '2026-07-14', data_quality_status: 'degraded',
    prompt_version: 'v1', input_schema_version: '1.0',
  }, SVC);
  ok(r.status === 200 && r.body.public_id === 'WK-20260706-f', 'report 保存 + public_id 形式', r);
  r = await post(`/api/ai-insights/service/jobs/${jobId}/posting`, {}, SVC);
  ok(r.status === 200, 'generated → posting', r);
  r = await post(`/api/ai-insights/service/jobs/${jobId}/posted`, { gchat_message_id: 'spaces/x/messages/y', body_hash: 'bh' }, SVC);
  ok(r.status === 200, 'posting → posted', r);
  r = await post('/api/ai-insights/service/jobs/claim', KEY, SVC);
  ok(r.status === 200 && r.body.result === 'already_posted', '投稿済み → already_posted (再claim不可)', r);
  const rep = db.prepare("SELECT gchat_message_id, posted_at FROM ai_reports WHERE public_id='WK-20260706-f'").get();
  ok(rep?.gchat_message_id === 'spaces/x/messages/y' && rep?.posted_at, 'ai_reports に投稿情報が記録される', rep);
  const topic = db.prepare("SELECT title, status FROM ai_report_topics").get();
  ok(topic?.status === 'open', '論点は open で保存される', topic);
}

// correction 採番
{
  let r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, edition: 'correction-next' }, SVC);
  ok(r.status === 200 && r.body.result === 'claimed' && r.body.job.edition === 'correction-1', 'correction-next → correction-1 採番', r.body?.job);
  const jobId = r.body.job.job_id;
  r = await post(`/api/ai-insights/service/jobs/${jobId}/failed`, { error_class: 'claude_limit', error_detail: 'サブスク上限' }, SVC);
  ok(r.status === 200, 'generating → failed 申告', r);
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, edition: 'correction-1' }, SVC);
  ok(r.status === 200 && r.body.result === 'claimed', 'failed は再claim可能', r);
}

// 孤児 sweep → reconciliation
console.log('\n■ 孤児回収 + 人間照合');
{
  // generating 孤児 → failed
  let r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-29', period_end: '2026-07-06' }, SVC);
  const genJob = r.body.job.job_id;
  db.prepare("UPDATE ai_report_jobs SET heartbeat_at = '2026-01-01T00:00:00Z' WHERE job_id = ?").run(genJob);
  await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-22', period_end: '2026-06-29' }, SVC); // claim が sweep を起動
  let st = db.prepare('SELECT status, error_class FROM ai_report_jobs WHERE job_id = ?').get(genJob);
  ok(st.status === 'failed' && st.error_class === 'orphaned', 'generating 孤児 → failed', st);

  // posting 孤児 → reconciliation_required (自動再投稿しない)
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-29', period_end: '2026-07-06' }, SVC);
  const jobId = r.body.job.job_id;
  await post(`/api/ai-insights/service/jobs/${jobId}/report`, {
    generation_mode: 'fallback', body_json: { summary: 'x' }, topics: [],
  }, SVC);
  await post(`/api/ai-insights/service/jobs/${jobId}/posting`, {}, SVC);
  db.prepare("UPDATE ai_report_jobs SET heartbeat_at = '2026-01-01T00:00:00Z' WHERE job_id = ?").run(jobId);
  const settings = await j('/apps/ai-insights/api/settings'); // settings 参照でも sweep が走る
  st = db.prepare('SELECT status FROM ai_report_jobs WHERE job_id = ?').get(jobId);
  ok(st.status === 'reconciliation_required', 'posting 孤児 → reconciliation_required', st);
  ok(settings.body.reconciliation?.length >= 0, 'settings に reconciliation 一覧', null);

  r = await post(`/api/ai-insights/service/jobs/${jobId}/heartbeat`, {}, SVC);
  ok(r.status === 409, 'reconciliation_required に heartbeat 不可', r);
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-29', period_end: '2026-07-06' }, SVC);
  ok(r.body.result === 'reconciliation_required', 'reconciliation 中は claim 不可 (自動再投稿禁止)', r.body);

  // 人間照合: mark_posted
  r = await post(`/apps/ai-insights/api/jobs/${jobId}/reconcile`, { action: 'mark_posted' });
  ok(r.status === 200 && r.body.status === 'posted', '照合: 投稿済み扱い → posted', r);
  r = await post(`/apps/ai-insights/api/jobs/${jobId}/reconcile`, { action: 'repost' });
  ok(r.status === 409, '照合済みジョブへの再照合 → 409', r);

  // 人間照合: repost → 再生成ではなく既存レポートの投稿再開になる (UNIQUE衝突しない)
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-08', period_end: '2026-06-15' }, SVC);
  const repostJob = r.body.job.job_id;
  await post(`/api/ai-insights/service/jobs/${repostJob}/report`, {
    generation_mode: 'claude', body_json: { summary: 'z' }, topics: [],
  }, SVC);
  await post(`/api/ai-insights/service/jobs/${repostJob}/posting`, {}, SVC);
  db.prepare("UPDATE ai_report_jobs SET heartbeat_at = '2026-01-01T00:00:00Z' WHERE job_id = ?").run(repostJob);
  await j('/apps/ai-insights/api/settings'); // sweep
  r = await post(`/apps/ai-insights/api/jobs/${repostJob}/reconcile`, { action: 'repost' });
  ok(r.status === 200 && r.body.status === 'pending', '照合: 再投稿指示 → pending', r);
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-08', period_end: '2026-06-15' }, SVC);
  ok(r.body.result === 'claimed_for_posting' && r.body.report?.body_json,
    'repost 後の claim → 投稿だけ再開 (再生成しない)', r.body?.result);
  // 投稿失敗申告 (posting → failed) 後も同様に投稿だけ再開
  await post(`/api/ai-insights/service/jobs/${repostJob}/failed`, { error_class: 'post_failed', error_detail: 'webhook接続前に失敗' }, SVC);
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-08', period_end: '2026-06-15' }, SVC);
  ok(r.body.result === 'claimed_for_posting', '投稿失敗 (report あり) の再claim → claimed_for_posting', r.body?.result);
  await post(`/api/ai-insights/service/jobs/${repostJob}/posted`, { gchat_message_id: 'm2' }, SVC);
}

// topic_updates のスコープ制限
console.log('\n■ topic_updates スコープ制限');
{
  const oldTopic = db.prepare(`
    SELECT t.topic_id FROM ai_report_topics t JOIN ai_reports r ON r.report_id = t.report_id
    WHERE r.public_id = 'WK-20260706-f'
  `).get();
  // 対象期間より前の論点 → 更新できる (2026-07-13 週のジョブから 07-06 週の論点を resolve)
  let r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-07-13', period_end: '2026-07-20' }, SVC);
  const futureJob = r.body.job.job_id;
  await post(`/api/ai-insights/service/jobs/${futureJob}/report`, {
    generation_mode: 'claude', body_json: { summary: 'w' }, topics: [],
    topic_updates: [{ topic_id: oldTopic.topic_id, status: 'resolved' }],
  }, SVC);
  let st = db.prepare('SELECT status FROM ai_report_topics WHERE topic_id = ?').get(oldTopic.topic_id);
  ok(st.status === 'resolved', '前期間の論点は更新できる', st);
  // 対象期間より後の論点 → 更新されない (2026-06-01 週のジョブから 07-13 週の論点は触れない)
  const newTopic = db.prepare(`
    INSERT INTO ai_report_topics (topic_id, report_id, seq, title, status, created_at, updated_at)
    SELECT 'tp_smoke_future', report_id, 9, '未来論点', 'open', '2026-07-20', '2026-07-20'
    FROM ai_reports WHERE public_id = 'WK-20260713-f'
  `).run();
  ok(newTopic.changes === 1, '(fixture) 未来論点を作成', null);
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-01', period_end: '2026-06-08' }, SVC);
  const pastJob = r.body.job.job_id;
  await post(`/api/ai-insights/service/jobs/${pastJob}/report`, {
    generation_mode: 'claude', body_json: { summary: 'v' }, topics: [],
    topic_updates: [{ topic_id: 'tp_smoke_future', status: 'dismissed' }],
  }, SVC);
  st = db.prepare("SELECT status FROM ai_report_topics WHERE topic_id = 'tp_smoke_future'").get();
  ok(st.status === 'open', '自分より後の期間の論点は更新されない', st);
}

// generated からの投稿再開 (claim_for_posting)
{
  let r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-15', period_end: '2026-06-22' }, SVC);
  const jobId = r.body.job.job_id;
  await post(`/api/ai-insights/service/jobs/${jobId}/report`, {
    generation_mode: 'claude', body_json: { summary: 'y' }, topics: [],
  }, SVC);
  // posting へ進まず runner が死んだ想定 (generated のまま)
  r = await post('/api/ai-insights/service/jobs/claim', { ...KEY, period_start: '2026-06-15', period_end: '2026-06-22' }, SVC);
  ok(r.body.result === 'claimed_for_posting' && r.body.report?.body_json, 'generated 再claim → 投稿だけ再開 (report 同梱)', r.body?.result);
}

// ═══ 6c. 月次 (PR-3): report-input / 締め宣言 / 状態機械 ═══
console.log('\n■ 月次 report-input + 締め宣言');
{
  // MF PL fixture (2026-06、run=success)
  db.prepare(`
    INSERT INTO mirror_mf_publish_runs (run_id, scope, status, started_at, finished_at, synced_at)
    VALUES (900, 'all', 'success', '2026-07-01T00:00:00Z', '2026-07-01T00:10:00Z', '2026-07-01T00:10:00Z')
  `).run();
  const insPl = db.prepare(`
    INSERT INTO mirror_mf_pl_monthly (run_id, month_ym, role_key, amount_excl_tax, tax_amount, line_count, is_realized_only, source_row_hash, synced_at)
    VALUES (900, ?, ?, ?, 0, 1, 1, 'h', '2026-07-01T00:10:00Z')
  `);
  insPl.run('2026-06', 'sales', 10000000);
  insPl.run('2026-06', 'cogs_purchase', 6000000);
  insPl.run('2026-06', 'sgae_rent', 1000000);

  let r = await j('/api/ai-insights/report-input?type=monthly&month=2026-13', { headers: READ });
  ok(r.status === 400, '不正 month → 400', r);
  r = await j('/api/ai-insights/report-input?type=monthly&month=2026-06', { headers: READ });
  ok(r.status === 200, '月次 report-input 200', r);
  const mi = r.body;
  ok(mi.meta.period_start === '2026-06-01' && mi.meta.period_end === '2026-07-01', '月境界 (排他的)', mi.meta);
  ok(mi.closing.status === 'open', '未宣言月は open', mi.closing);
  ok(mi.facts.pl.this_month.sales_yen === 10000000, 'PL 売上 1,000万', mi.facts.pl.this_month);
  ok(mi.facts.pl.this_month.gross_profit_yen === 4000000 && mi.facts.pl.this_month.operating_income_yen === 3000000,
    '粗利/営業利益の導出', mi.facts.pl.this_month);
  ok(typeof mi.pl_hash === 'string' && mi.pl_hash.length === 64, 'pl_hash (確定後変更検知の基準)', mi.pl_hash);
  ok(mi.budget.available === false, '予算未入力 (月次)', mi.budget);

  // 締め宣言
  r = await post('/apps/ai-insights/api/closing/2099-01/declare', {});
  ok(r.status === 400, '未来月の宣言 → 400', r);
  r = await post('/apps/ai-insights/api/closing/2026-06/declare', {});
  ok(r.status === 200, '2026-06 確定宣言', r);
  r = await post('/apps/ai-insights/api/closing/2026-06/declare', {});
  ok(r.status === 409, '二重宣言 → 409', r);
  r = await j('/api/ai-insights/report-input?type=monthly&month=2026-06', { headers: READ });
  ok(r.body.closing.status === 'declared', '宣言後は declared', r.body.closing);
  ok(r.body.closing.mf_synced_after_declare === false, '宣言後のMF同期はまだ無い', r.body.closing);
  // 宣言後に新しい MF 同期が来たことにする
  db.prepare(`
    INSERT INTO mirror_mf_publish_runs (run_id, scope, status, started_at, finished_at, synced_at)
    VALUES (901, 'all', 'success', ?, ?, ?)
  `).run(new Date().toISOString(), new Date().toISOString(), new Date(Date.now() + 1000).toISOString());
  db.prepare(`
    INSERT INTO mirror_mf_pl_monthly (run_id, month_ym, role_key, amount_excl_tax, tax_amount, line_count, is_realized_only, source_row_hash, synced_at)
    SELECT 901, month_ym, role_key, amount_excl_tax, tax_amount, line_count, is_realized_only, source_row_hash, synced_at
    FROM mirror_mf_pl_monthly WHERE run_id = 900
  `).run();
  r = await j('/api/ai-insights/report-input?type=monthly&month=2026-06', { headers: READ });
  ok(r.body.closing.mf_synced_after_declare === true, '宣言後のMF同期を検知', r.body.closing);

  // 月次 claim: 期間バリデーション
  const MKEY = { report_type: 'monthly', period_start: '2026-06-01', period_end: '2026-07-01', edition: 'final', claimed_by: 'smoke' };
  r = await post('/api/ai-insights/service/jobs/claim', { ...MKEY, period_end: '2026-06-30' }, SVC);
  ok(r.status === 400, '月次: 1ヶ月丁度でない期間 → 400', r);
  r = await post('/api/ai-insights/service/jobs/claim', MKEY, SVC);
  ok(r.status === 200 && r.body.result === 'claimed', '月次 final claim', r.body?.result);
  const mJobId = r.body.job.job_id;
  r = await post(`/api/ai-insights/service/jobs/${mJobId}/report`, {
    generation_mode: 'claude', body_json: { summary: '月次テスト' }, topics: [],
    pl_hash: 'plhash-final-1',
  }, SVC);
  ok(r.status === 200 && r.body.public_id === 'MO-202606-f', '月次 final 保存 + public_id', r.body);
  const closingRow = db.prepare("SELECT * FROM ai_monthly_closing WHERE year_month = '2026-06'").get();
  ok(closingRow.final_pl_hash === 'plhash-final-1' && closingRow.final_report_id,
    '確定版保存で closing に pl_hash 記録', closingRow);
  ok(closingRow.final_declare_seq === closingRow.declare_seq, 'final_declare_seq = 宣言 seq', closingRow);

  // 再オープン → 再宣言 → needs_correction
  r = await post('/apps/ai-insights/api/closing/2026-06/reopen', {});
  ok(r.status === 200, '再オープン', r);
  r = await post('/apps/ai-insights/api/closing/2026-06/declare', {});
  ok(r.status === 200, '再宣言', r);
  r = await j('/api/ai-insights/report-input?type=monthly&month=2026-06', { headers: READ });
  ok(r.body.closing.needs_correction === true, '再宣言後は needs_correction', r.body.closing);
  r = await post('/api/ai-insights/service/jobs/claim', { ...MKEY, edition: 'correction-next' }, SVC);
  ok(r.status === 200 && r.body.job.edition === 'correction-1', '月次 correction-next 採番', r.body?.job);
  ok(r.body.job.declare_seq === 2, 'correction は宣言番号に紐付く', r.body?.job);

  // 同じ宣言からの correction-next 再要求 → 既存 correction-1 を再利用 (増殖しない)
  r = await post('/api/ai-insights/service/jobs/claim', { ...MKEY, edition: 'correction-next' }, SVC);
  ok(r.body.result === 'busy' && r.body.job.edition === 'correction-1',
    '同一宣言の correction-next → 既存jobを再利用 (busy、correction-2を作らない)', r.body);

  // 生成中に再オープン→再宣言 → 旧生成の保存は stale として 409
  const corrJob = db.prepare(`
    SELECT job_id FROM ai_report_jobs WHERE edition = 'correction-1' AND period_start = '2026-06-01'
  `).get();
  r = await post('/apps/ai-insights/api/closing/2026-06/reopen', {});
  ok(r.status === 200, '(生成中に) 再オープン', r);
  r = await post('/apps/ai-insights/api/closing/2026-06/declare', {});
  ok(r.status === 200, '(生成中に) 再宣言 (seq=3)', r);
  r = await post(`/api/ai-insights/service/jobs/${corrJob.job_id}/report`, {
    generation_mode: 'claude', body_json: { summary: '旧宣言の生成' }, topics: [], pl_hash: 'stale',
  }, SVC);
  ok(r.status === 409 && r.body.error_class === 'stale_declare_seq',
    '旧宣言の生成保存 → 409 stale (確定扱いしない)', r.body);
  const closingAfterStale = db.prepare("SELECT final_pl_hash FROM ai_monthly_closing WHERE year_month='2026-06'").get();
  ok(closingAfterStale.final_pl_hash === 'plhash-final-1', 'stale 保存で closing が汚れない', closingAfterStale);

  // 未宣言月の final claim は拒否
  r = await post('/api/ai-insights/service/jobs/claim', {
    report_type: 'monthly', period_start: '2026-05-01', period_end: '2026-06-01', edition: 'final', claimed_by: 'smoke',
  }, SVC);
  ok(r.status === 409, '未宣言月の final claim → 409', r);

  const settings = await j('/apps/ai-insights/api/settings');
  ok(Array.isArray(settings.body.closing) && settings.body.closing.length === 3, 'settings に月次締め3ヶ月', settings.body.closing?.length);
}

// ═══ 6d. notify-job (リマインダー / 確定後変更検知) ═══
console.log('\n■ notify-job (月次リマインダー)');
{
  const { runClosingChecks } = await load('apps/ai-insights/notify-job.js');
  const posts = [];
  const post_ = async (text) => posts.push(text);
  // 2026-08-16 時点 → 前月 2026-07 が未宣言 → 15日リマインダー
  let sent = await runClosingChecks(db, { today: '2026-08-16', post: post_ });
  ok(sent.includes('reminder_15'), '15日リマインダー送信', sent);
  sent = await runClosingChecks(db, { today: '2026-08-16', post: post_ });
  ok(sent.length === 0, '同日再実行では再送しない', sent);
  sent = await runClosingChecks(db, { today: '2026-08-21', post: post_ });
  ok(sent.includes('reminder_20'), '20日リマインダー (文言強め)', sent);
  ok(posts.length === 2 && posts[1].includes('2回目'), '通知本文', posts.map((p) => p.split('\n')[0]));

  // 確定後変更検知: 2026-06 (final発行済み) の pl_hash を古い値に書き換え → 差分検知
  db.prepare(`
    UPDATE ai_monthly_closing SET final_pl_hash = 'stale-hash', status = 'declared', change_notified_at = NULL
    WHERE year_month = '2026-06'
  `).run();
  sent = await runClosingChecks(db, { today: '2026-07-25', post: post_ });
  ok(sent.includes('post_final_change'), '確定後変更あり → 訂正版候補通知', sent);
  sent = await runClosingChecks(db, { today: '2026-07-26', post: post_ });
  ok(!sent.includes('post_final_change'), '変更通知は1回だけ', sent);
}

// ═══ 7. ⚙️画面 ═══
console.log('\n■ ⚙️画面');
{
  const r = await j('/apps/ai-insights/api/settings');
  ok(r.status === 200 && Array.isArray(r.body.budget) && Array.isArray(r.body.events)
    && Array.isArray(r.body.sources) && Array.isArray(r.body.recent_jobs), 'settings 構造', Object.keys(r.body || {}));
  ok(r.body.sources.some((s) => s.source_id === 'sales:rakuten' && s.requirement === 'required'), 'expected sources seed 済み', null);
  const a = await j('/apps/ai-insights/api/budget/actuals?year=2025');
  ok(a.status === 200 && Array.isArray(a.body.actuals), 'budget/actuals (MF空でも 200)', a);
  const bad = await j('/apps/ai-insights/api/budget/actuals?year=20xx');
  ok(bad.status === 400, 'budget/actuals 不正 year → 400', bad);
}

server.close();
console.log(`\n═══ 結果: ${pass} PASS / ${fail} FAIL ═══`);
process.exit(fail === 0 ? 0 : 1);
