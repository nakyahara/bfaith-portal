#!/usr/bin/env node
// ai-insights PC runner smoke test
// 実行: node scripts/ai-insights/test/smoke.mjs
// モック portal + 偽 claude で runner を子プロセス実行し、E2E フローを検証する
// (依存パッケージなし: node:http のみ)

import http from 'node:http';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const RUNNER = path.join(TEST_DIR, '..', 'run-weekly-report.js');
const FAKE_CLAUDE = path.join(TEST_DIR, 'fake-claude.mjs');
const PS = '2026-07-06';

let pass = 0, fail = 0;
function ok(cond, name, extra) {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name}${extra !== undefined ? ` — ${JSON.stringify(extra)?.slice(0, 300)}` : ''}`); }
}

// ── モック portal ────────────────────────────────────────────────────

const state = {
  calls: [],            // {path, body, headers}
  inputOverride: null,  // report-input 応答の差し替え
  claimResult: null,    // claim 応答の差し替え (null = 通常 claimed)
  webhookTexts: [],
  authSeen: { read: null, bearer: null },
};

function baseInput() {
  return {
    meta: {
      report_type: 'weekly', period_start: PS, period_end: '2026-07-13',
      period_label: '2026-07-06〜2026-07-12', generated_at: new Date().toISOString(),
      data_as_of: '2026-07-14', input_schema_version: '1.0',
    },
    coverage: [
      { source_id: 'sales:rakuten', source_kind: 'sales', unit: 'rakuten', requirement: 'required', status: 'ok', detail: null },
      { source_id: 'sales:amazon', source_kind: 'sales', unit: 'amazon', requirement: 'required', status: 'suspect', detail: 'x' },
    ],
    constraints: {
      generation: 'allowed', data_quality_status: 'degraded',
      // サーバ仕様 (facts.js deriveConstraints): 売上欠損モールがあれば company_totals も禁止
      prohibited_topic_areas: [
        { area: 'sales:amazon', reason: 'suspect' },
        { area: 'company_totals', reason: '売上欠損モールがあるため全社合計を使う論点は禁止' },
      ],
    },
    facts: {
      sales: {
        by_mall: [{ mall: 'rakuten', sales_yen: 15000, units: 15, prev_week_sales_yen: 10000, wow_pct: 50, prior_4w_median_sales_yen: 4000 }],
        daily: [],
        company: { sales_yen: 15500, units: 16, prev_week_sales_yen: 10000, wow_pct: 55 },
      },
      margin: { by_mall: [], negative_margin_skus: [{ mall: 'rakuten', sku: 'r-9', product_name: '赤字商品', units: 3, sales_yen: 3000, margin_yen: -1500 }] },
      inventory: { by_category: [], as_of: '2026-07-12', stale_skus: 1, low_supply_skus_14d: 2 },
      po: { outstanding_qty: 10, outstanding_value_yen: 50000, as_of: '2026-07-12' },
      mf: { available: false },
    },
    events: { entries: [] },
    budget: { available: false, entries: [], budget_revision_id: null, note: '予算未入力' },
    previous_open_topics: [{ topic_id: 'tp_prev_1', title: '前回論点', category: 'sales', status: 'open', public_id: 'WK-20260629-f', period_start: '2026-06-29' }],
    recent_reports: [],
  };
}

const server = http.createServer((req, res) => {
  let raw = '';
  req.on('data', (d) => { raw += d; });
  req.on('end', () => {
    const body = raw ? JSON.parse(raw) : null;
    const url = req.url.split('?')[0];
    state.calls.push({ path: url, body });
    const json = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };

    if (url === '/api/ai-insights/report-input') {
      state.authSeen.read = req.headers['x-read-token'];
      return json(200, state.inputOverride || baseInput());
    }
    if (url === '/api/ai-insights/service/jobs/claim') {
      state.authSeen.bearer = req.headers.authorization;
      if (state.claimResult) return json(200, state.claimResult);
      return json(200, { result: 'claimed', job: { job_id: 'jb_smoke_1', report_type: 'weekly', period_start: PS, period_end: '2026-07-13', edition: 'final' } });
    }
    if (/^\/api\/ai-insights\/service\/jobs\/[^/]+\/report$/.test(url)) {
      return json(200, { ok: true, report_id: 'rp_smoke_1', public_id: 'WK-20260706-f' });
    }
    if (/^\/api\/ai-insights\/service\/jobs\/[^/]+\/posted$/.test(url) && state.postedFail) {
      return json(500, { error: 'simulated posted failure' });
    }
    if (/^\/api\/ai-insights\/service\/jobs\/[^/]+\/(posting|posted|heartbeat|failed)$/.test(url)) {
      return json(200, { ok: true });
    }
    if (url === '/webhook') {
      state.webhookTexts.push(body?.text || '');
      return json(200, { name: 'spaces/x/messages/smoke-1' });
    }
    json(404, { error: `unexpected ${url}` });
  });
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const port = server.address().port;

// ── runner 実行ヘルパー ───────────────────────────────────────────────

function runRunner(extraEnv = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [RUNNER, `--period-start=${PS}`], {
      env: {
        ...process.env,
        PORTAL_BASE_URL: `http://127.0.0.1:${port}`,
        AI_READ_TOKEN: 'tok-read',
        AI_INSIGHT_SERVICE_TOKEN: 'tok-service',
        GCHAT_WEBHOOK_URL: `http://127.0.0.1:${port}/webhook`,
        CLAUDE_CMD: `node ${FAKE_CLAUDE}`,
        CLAUDE_RETRY_BASE_MS: '10',
        CLAUDE_TIMEOUT_MS: '20000',
        AI_RUNNER_NOW: '2026-07-14T01:00:00+09:00', // 火曜 01:00 JST (締切前だが allowed なら関係なし)
        ...extraEnv,
      },
    });
    let out = '';
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { out += d; });
    child.on('close', (code) => resolve({ code, out }));
  });
}
function reset() {
  state.calls = [];
  state.webhookTexts = [];
  state.inputOverride = null;
  state.claimResult = null;
  state.postedFail = false;
}
const calledPaths = () => state.calls.map((c) => c.path.replace(/jb_[a-z0-9_]+/i, ':job'));

// ═══ 1. 正常系 (claude 生成 → 投稿) ═══
console.log('\n■ 正常系 (claude 生成 → 投稿)');
{
  reset();
  const { code, out } = await runRunner({ FAKE_MODE: 'ok' });
  ok(code === 0, 'exit 0', out.slice(-500));
  ok(out.includes('[NOTIFY:status=ok]'), 'NOTIFY:status=ok', out.slice(-300));
  ok(state.authSeen.read === 'tok-read', 'read token 送信', state.authSeen);
  ok(state.authSeen.bearer === 'Bearer tok-service', 'service token 送信', state.authSeen);
  const seq = calledPaths().filter((p) => !p.endsWith('/heartbeat'));
  ok(JSON.stringify(seq) === JSON.stringify([
    '/api/ai-insights/report-input',
    '/api/ai-insights/service/jobs/claim',
    '/api/ai-insights/service/jobs/:job/report',
    '/api/ai-insights/service/jobs/:job/posting',
    '/webhook',
    '/api/ai-insights/service/jobs/:job/posted',
  ]), '呼び出し順序 (claim→report→posting→webhook→posted)', seq);
  const reportCall = state.calls.find((c) => c.path.endsWith('/report'));
  ok(reportCall.body.generation_mode === 'claude', 'generation_mode=claude', reportCall.body.generation_mode);
  ok(reportCall.body.topic_updates.length === 1 && reportCall.body.topic_updates[0].topic_id === 'tp_prev_1',
    '未知 topic_id はフィルタされ既知のみ送信', reportCall.body.topic_updates);
  ok(typeof reportCall.body.input_hash === 'string' && reportCall.body.input_hash.length === 64, 'input_hash (sha256)', reportCall.body.input_hash);
  const text = state.webhookTexts[0] || '';
  ok(text.startsWith('*📊 AI経営レポート（週次）*\n2026-07-06〜2026-07-12'), 'GChat ヘッダー形式', text.split('\n')[0]);
  ok(text.includes('(WK-20260706-f)'), 'public_id 埋め込み', text.slice(-80));
  ok(text.includes('*1. 楽天売上が前週比+50%*'), '論点タイトル (太字+番号)', null);
  ok(text.includes('*1.5万円*') && text.includes('*1.0万円*'), '金額は万円表記+太字強調', text);
  ok(!text.includes('**'), 'AI出力に*があっても二重太字にならない', text);
  ok(text.includes('→ ') && text.includes('（中原さん・金曜まで）'), '対応 → 誰・いつまで', null);
  ok(text.includes('\n\n📈 *1.'), '論点は空行区切り+カテゴリアイコン', null);
  ok(!text.includes('全社'), '正常系でも禁止された全社集計は本文に出ない', text);
  ok(text.length <= 3500, '3500字以内', text.length);
  const postedCall = state.calls.find((c) => c.path.endsWith('/posted'));
  ok(postedCall.body.gchat_message_id === 'spaces/x/messages/smoke-1', 'message ID 保存', postedCall.body);
}

// ═══ 2. 投稿済み (冪等) ═══
console.log('\n■ 投稿済み (冪等 skip)');
{
  reset();
  state.claimResult = { result: 'already_posted', job: { job_id: 'jb_smoke_1' } };
  const { code, out } = await runRunner({ FAKE_MODE: 'ok' });
  ok(code === 0 && out.includes('[NOTIFY:status=skip]'), 'already_posted → skip', out.slice(-200));
  ok(state.webhookTexts.length === 0, 'webhook 投稿なし', state.webhookTexts);
}

// ═══ 3. claude 失敗 → フォールバック ═══
console.log('\n■ claude 失敗 → フォールバック (数字は必ず届く)');
{
  reset();
  const { code, out } = await runRunner({ FAKE_MODE: 'fail' });
  ok(code === 0 && out.includes('[NOTIFY:status=fallback]'), 'NOTIFY:status=fallback', out.slice(-300));
  const reportCall = state.calls.find((c) => c.path.endsWith('/report'));
  ok(reportCall.body.generation_mode === 'fallback', 'generation_mode=fallback', reportCall.body.generation_mode);
  const text = state.webhookTexts[0] || '';
  ok(text.includes('AI生成に失敗'), 'フォールバック明記', text.slice(0, 200));
  ok(!text.includes('全社売上'), 'company_totals 禁止時は全社集計を出さない', text);
  ok(text.includes('rakuten 売上 前週比 +50%'), '代わりに健全モールの個別数字', text);
  ok(!text.includes('+55%'), '禁止された全社WoWは載らない', text);
}

// ═══ 3b. AI出力のルール違反 → 機械検査で棄却 → フォールバック ═══
console.log('\n■ AI出力の機械検査 (禁止モール言及 / 数字創作)');
{
  reset();
  let r = await runRunner({ FAKE_MODE: 'prohibited' });
  ok(r.code === 0 && r.out.includes('[NOTIFY:status=fallback]'), '禁止モール言及 → 棄却 → fallback', r.out.slice(-300));
  ok(r.out.includes('禁止領域モール'), '違反理由がログに出る', null);
  reset();
  r = await runRunner({ FAKE_MODE: 'invented' });
  ok(r.code === 0 && r.out.includes('[NOTIFY:status=fallback]'), 'facts に無い小数 (1.9万円) → 棄却 → fallback (整数部一致では許容しない)', r.out.slice(-300));
  ok(r.out.includes('数字創作の疑い'), '違反理由がログに出る', null);
  reset();
  r = await runRunner({ FAKE_MODE: 'inventedtitle' });
  ok(r.code === 0 && r.out.includes('[NOTIFY:status=fallback]') && r.out.includes('title'), 'title の創作数値も棄却 → fallback', r.out.slice(-300));
  reset();
  r = await runRunner({ FAKE_MODE: 'badsummary' });
  ok(r.code === 0 && r.out.includes('[NOTIFY:status=fallback]'), 'summary の全社集計言及 → 棄却 → fallback', r.out.slice(-300));
  ok(r.out.includes('全社集計に言及'), 'summary 検査の違反理由がログに出る', null);
}

// ═══ 3c. webhook成功後の /posted 失敗 → 成否不明扱い (二重投稿させない) ═══
console.log('\n■ /posted 失敗 → 人間照合行き (自動再投稿しない)');
{
  reset();
  state.postedFail = true;
  const { code, out } = await runRunner({ FAKE_MODE: 'ok' });
  ok(code === 1, 'exit 1', code);
  ok(out.includes('[NOTIFY:status=failed_posting_uncertain]'), 'failed_posting_uncertain', out.slice(-300));
  ok(out.includes('投稿済み・記録失敗'), '投稿済みと明示', null);
  ok(!calledPaths().some((p) => p.endsWith('/failed')), '/failed を呼ばない (claimed_for_postingでの再投稿を防ぐ)', calledPaths());
}

// ═══ 4. 壊れた出力 → リトライ → フォールバック ═══
console.log('\n■ 壊れた claude 出力 → フォールバック');
{
  reset();
  const { code, out } = await runRunner({ FAKE_MODE: 'bad' });
  ok(code === 0 && out.includes('[NOTIFY:status=fallback]'), 'JSON なし出力 → fallback', out.slice(-300));
  ok((out.match(/attempt \d\/3/g) || []).length === 3, '3回リトライした', out.match(/attempt \d\/3/g));
}

// ═══ 5. blocked (締切前 → 待ち / 締切後 → 生成なし投稿) ═══
console.log('\n■ blocked 縮退');
{
  reset();
  const blocked = baseInput();
  blocked.constraints = { generation: 'blocked', data_quality_status: 'blocked', prohibited_topic_areas: [] };
  blocked.coverage = blocked.coverage.map((c) => ({ ...c, status: 'missing' }));
  blocked.facts = null;
  state.inputOverride = blocked;
  let r = await runRunner({ FAKE_MODE: 'ok', AI_RUNNER_NOW: '2026-07-14T09:00:00+09:00' }); // 火曜 9:00
  ok(r.code === 0 && r.out.includes('[NOTIFY:status=retry_later]'), '締切前 → retry_later', r.out.slice(-200));
  ok(!calledPaths().includes('/api/ai-insights/service/jobs/claim'), '締切前は claim しない', calledPaths());

  reset();
  state.inputOverride = blocked;
  r = await runRunner({ FAKE_MODE: 'ok', AI_RUNNER_NOW: '2026-07-14T19:00:00+09:00' }); // 火曜 19:00
  ok(r.code === 0 && r.out.includes('[NOTIFY:status=blocked_notice]'), '締切後 → 生成なし投稿', r.out.slice(-200));
  const text = state.webhookTexts[0] || '';
  ok(text.includes('生成なし') && text.includes('rakuten=missing'), '生成なし通知の内容', text);
  const reportCall = state.calls.find((c) => c.path.endsWith('/report'));
  ok(reportCall.body.data_quality_status === 'blocked', 'blocked としてレポート記録', reportCall.body.data_quality_status);
}

// ═══ 6. 生成済みの投稿再開 (claimed_for_posting) ═══
console.log('\n■ 生成済みレポートの投稿再開');
{
  reset();
  state.claimResult = {
    result: 'claimed_for_posting',
    job: { job_id: 'jb_smoke_2', report_type: 'weekly', period_start: PS, period_end: '2026-07-13', edition: 'final' },
    report: {
      report_id: 'rp_x', public_id: 'WK-20260706-f', data_quality_status: 'degraded',
      body_json: JSON.stringify({
        summary: '再投稿テスト', topics: [], topic_updates: [], data_notes: '',
      }),
    },
  };
  const { code, out } = await runRunner({ FAKE_MODE: 'ok' });
  ok(code === 0 && out.includes('[NOTIFY:status=ok_repost]'), 'ok_repost', out.slice(-200));
  ok(!calledPaths().some((p) => p.endsWith('/report')), '再生成しない (report 呼ばず)', calledPaths());
  ok(state.webhookTexts[0]?.includes('再投稿テスト'), '保存済み本文で投稿', state.webhookTexts[0]);
}

// ═══ 6b. --dry-run (投稿せずプレビューのみ) ═══
console.log('\n■ --dry-run');
{
  reset();
  const child = await new Promise((resolve) => {
    const c = spawn(process.execPath, [RUNNER, `--period-start=${PS}`, '--dry-run'], {
      env: {
        ...process.env,
        PORTAL_BASE_URL: `http://127.0.0.1:${port}`,
        AI_READ_TOKEN: 'tok-read', AI_INSIGHT_SERVICE_TOKEN: 'tok-service',
        GCHAT_WEBHOOK_URL: `http://127.0.0.1:${port}/webhook`,
        CLAUDE_CMD: `node ${FAKE_CLAUDE}`, CLAUDE_RETRY_BASE_MS: '10', CLAUDE_TIMEOUT_MS: '20000',
        FAKE_MODE: 'ok',
      },
    });
    let out = '';
    c.stdout.on('data', (d) => { out += d; });
    c.stderr.on('data', (d) => { out += d; });
    c.on('close', (code) => resolve({ code, out }));
  });
  ok(child.code === 0 && child.out.includes('[NOTIFY:status=dry_run]'), 'dry_run 終了', child.out.slice(-200));
  ok(child.out.includes('GChat本文プレビュー') && child.out.includes('*1.5万円*'), 'プレビュー本文が出る', null);
  ok(state.webhookTexts.length === 0, 'webhook 投稿なし', state.webhookTexts);
  ok(!calledPaths().some((p) => p.includes('/service/')), 'サーバ書き込みなし (claim すら呼ばない)', calledPaths());
}

// ═══ 7. 要照合 (自動再投稿しない) ═══
console.log('\n■ reconciliation_required');
{
  reset();
  state.claimResult = { result: 'reconciliation_required', job: { job_id: 'jb_smoke_3' } };
  const { code, out } = await runRunner({ FAKE_MODE: 'ok' });
  ok(code === 0 && out.includes('[NOTIFY:status=reconciliation_required]'), '照合待ちは何もしない', out.slice(-200));
  ok(state.webhookTexts.length === 0, 'webhook 投稿なし (自動再投稿禁止)', state.webhookTexts);
}

server.close();
console.log(`\n═══ 結果: ${pass} PASS / ${fail} FAIL ═══`);
process.exit(fail === 0 ? 0 : 1);
