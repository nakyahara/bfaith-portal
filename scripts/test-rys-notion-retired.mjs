/**
 * 🗂 楽天→Yahoo! 商品移行 (RYS) の Notion 連携廃止 (2026-09-14 中原さん決定 D-1) — 止血 PR 1 の受入試験
 *
 * 実行: node scripts/test-rys-notion-retired.mjs
 *
 * 守りたいのは 5 つ。
 *   ① 「全部更新」は Notion 3 ステップを **理由付き skipped** で記録して success で終わる (中身は
 *      apps/rakuten-yahoo-sync/services/refresh-pipeline.test.mjs が担当。ここでは実装が Notion のサービスを
 *      import していないこと = 呼ぶ経路が残っていないことを見る)
 *   ② Notion へ書く / 取り込む 4 経路は **410 Gone** (404 ではない = 「あったが廃止した」)。読み取りの status は 200
 *   ③ 画面 (dashboard / manual) に Notion への導線が無い。廃止の告知と残存する確定値の件数が出る。
 *      描画した HTML の <script> が構文的に正しい (feedback_ejs_output_tag_in_js_value_position)
 *   ④ env: RYS_NOTION_TOKEN / NOTION_PRODUCT_MASTER_DB_ID が無くても healthy。「廃止」として一覧に出る
 *   ⑤ 台帳: rys-daily-refresh が現役の台帳にあり validate が通る。cron は成功 / 失敗で ping を打つ配線がある
 *
 * 正本 = AI_reference『システム設計/RakutenYahooSync_Notion廃止後の方針案_20260914.md』
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';
import vm from 'vm';
import { fileURLToPath } from 'url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'rys-notion-retired-'));
delete process.env.RYS_NOTION_TOKEN;
delete process.env.NOTION_PRODUCT_MASTER_DB_ID;
delete process.env.JOBS_MONITOR_ENABLED;
// 必須 env (Notion 以外) はダミーで埋める = 「Notion が無いだけ」の状態を作る
process.env.WAREHOUSE_URL = 'https://wh.example.test';
process.env.WAREHOUSE_SERVICE_TOKEN = 'dummy-token';
process.env.CF_ACCESS_CLIENT_ID = 'dummy-id';
process.env.CF_ACCESS_CLIENT_SECRET = 'dummy-secret';
process.env.YAHOO_SELLER_ID = 'b-faith';

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const src = (rel) => fs.readFileSync(path.join(ROOT, rel), 'utf8');

console.log('[1] 実装から Notion を呼ぶ経路が消えている');
{
  const pipeline = src('apps/rakuten-yahoo-sync/services/refresh-pipeline.js');
  ok(!/notion-sync\.js|notion-create-page\.js|notion-draft-seed\.js|sync-lock\.js/.test(pipeline), 'refresh-pipeline は Notion 系サービスと sync-lock を import しない');
  ok(/NOTION_RETIRED_STEPS/.test(pipeline) && /skipped: true, reason: NOTION_RETIRED_REASON/.test(pipeline), '3 ステップは理由付き skipped で steps に残す');
  const router = src('apps/rakuten-yahoo-sync/router.js');
  ok(!/notion-sync\.js|notion-client\.js|notion-create-page\.js|notion-draft-seed\.js|sync-lock\.js/.test(router), 'router は Notion 系サービスを import しない');
  ok(!/notionPageUrl|notionAppUrl/.test(router), 'router に削除済み Notion ページへのリンク生成が残っていない');
}

console.log('[2] Notion へ書く / 取り込む 4 経路は 410、読み取りは 200');
{
  const express = (await import('express')).default;
  const { default: router } = await import('../apps/rakuten-yahoo-sync/router.js');
  const app = express();
  app.set('view engine', 'ejs');
  app.use(express.json());
  app.use('/apps/rakuten-yahoo-sync', router);
  const server = await new Promise((r) => { const sv = http.createServer(app); sv.listen(0, '127.0.0.1', () => r(sv)); });
  const port = server.address().port;
  const call = (method, p, body) => new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: '/apps/rakuten-yahoo-sync' + p, headers: { 'Content-Type': 'application/json' } }, (res) => {
      let buf = ''; res.setEncoding('utf8'); res.on('data', (d) => { buf += d; }); res.on('end', () => resolve({ status: res.statusCode, body: buf }));
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
  for (const p of ['/api/notion/sync', '/api/notion/seed-category', '/api/admin/seed-notion-drafts', '/api/admin/create-notion-pages-from-rakuten']) {
    const r = await call('POST', p, { dryRun: true });
    let j = null; try { j = JSON.parse(r.body); } catch (_) {}
    ok(r.status === 410 && j?.status === 'retired' && j?.retiredAt === '2026-09-14' && /廃止/.test(j?.error || ''), `POST ${p} → 410 retired (実際 ${r.status})`);
  }
  {
    const r = await call('GET', '/api/notion/sync/status');
    let j = null; try { j = JSON.parse(r.body); } catch (_) {}
    ok(r.status === 200 && j && typeof j.notion_overrides?.total === 'number' && 'complete' in (j.notion_overrides || {}), `GET /api/notion/sync/status は 200 で残存件数 (total / complete) を返す (実際 ${r.status})`);
  }

  console.log('[3] 画面: Notion への導線が無く、廃止の告知と残存件数が出る。script は構文的に正しい');
  const scriptsOf = (html) => [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map((m) => m[1]).filter((c) => c.trim() && !/^\s*\/\//.test(c) === false || c.trim());
  const checkScripts = (html, label) => {
    const blocks = [...html.matchAll(/<script(?![^>]*\ssrc=)[^>]*>([\s\S]*?)<\/script>/gi)].map((m) => m[1]).filter((c) => c.trim());
    let bad = null;
    for (const code of blocks) { try { new vm.Script(code); } catch (e) { bad = e.message; break; } }
    ok(blocks.length > 0 && !bad, `${label}: 描画済み HTML の inline script ${blocks.length} 個が構文 OK${bad ? ' — ' + bad : ''}`);
  };
  void scriptsOf;
  {
    const r = await call('GET', '/');
    ok(r.status === 200, `GET / (dashboard) は 200 (実際 ${r.status})`);
    const h = r.body;
    ok(/js-notion-retired-notice/.test(h) && /Notion 連携は 2026-09-14 に廃止しました/.test(h), 'dashboard に廃止の告知が出る');
    ok(/旧 Notion から引き継いだ <strong>0<\/strong> 商品分が残っています/.test(h), 'dashboard に残存する確定値の件数 (空 DB なら 0) が出る');
    ok(!/Notion で修正|Notion で開く|Notion 取込のみ|Notion 新規作成|楽天から下書き|aburatoishi/.test(h), 'dashboard に Notion への導線 (ボタン・リンク) が無い');
    ok(!/api\/notion\/sync'|seed-notion-drafts|create-notion-pages-from-rakuten|seed-category/.test(h), 'dashboard の JS が廃止した 4 経路を呼ばない');
    ok(!/syncModal|runSync\(|seedNotionDrafts|createNotionPages\(|notionPageUrl|notionAppUrl/.test(h), 'dashboard に Notion 取込モーダル・関数・ページリンクが残っていない');
    ok(/廃止・スキップ/.test(h) && /廃止のためスキップ/.test(h), '全部更新の進捗ラベルと結果に「廃止・スキップ」が出る');
    ok(/RYS_NOTION_TOKEN/.test(h) && /— \(廃止\)/.test(h), '環境変数の表に Notion の 2 つが「廃止」として出る');
    checkScripts(h, 'dashboard');
  }
  {
    const r = await call('GET', '/manual');
    ok(r.status === 200, `GET /manual は 200 (実際 ${r.status})`);
    const h = r.body;
    ok(/Notion 連携は 2026-09-14 に廃止しました/.test(h), 'manual に廃止の告知が出る');
    ok(!/Notion で修正|Notionアプリが直接開きます|Notion商品マスター ────/.test(h), 'manual に Notion を開く案内・Notion を含むデータフロー図が無い');
    ok(/\(廃止\) Notionページ自動作成/.test(h), 'manual のパイプライン説明で 3 ステップが廃止と書かれている');
  }
  {
    const r = await call('GET', '/api/products/no-such-item/detail');
    ok(r.status === 404, `商品詳細 API は動く (存在しない商品は 404、実際 ${r.status})`);
  }
  server.close();
}

console.log('[4] env: Notion の 2 つが無くても healthy。「廃止」として一覧に出る');
{
  const { inspectEnvStatus } = await import('../apps/rakuten-yahoo-sync/env-check.js');
  const st = inspectEnvStatus();
  ok(st.healthy === true, 'RYS_NOTION_TOKEN / NOTION_PRODUCT_MASTER_DB_ID が無くても healthy');
  ok(!st.required.some((r) => /NOTION/.test(r.key)), 'required に Notion の env が無い');
  eq(st.retired.map((r) => r.key).sort(), ['NOTION_PRODUCT_MASTER_DB_ID', 'RYS_NOTION_TOKEN'], 'retired に Notion の 2 つ');
  ok(st.retired.every((r) => r.set === false && r.retired === true && /廃止/.test(r.purpose)), 'retired の行は set=false・retired=true・用途に「廃止」');
}

console.log('[5] 台帳と監視: rys-daily-refresh が現役台帳にあり、cron は成功/失敗で ping を打つ');
{
  const reg = await import('../config/jobs-registry.mjs');
  const j = reg.JOBS_REGISTRY.find((e) => e.id === 'rys-daily-refresh');
  ok(!!j && j.type === 'scheduled_job' && j.anchor_hour_jst === 7 && j.anchor_minute_jst === 30, '台帳に rys-daily-refresh (07:30 JST, scheduled_job)');
  ok(!!j && /Notion/.test(j.purpose) && /RYS_FULL_SYNC_CRON_ENABLED/.test(j.where) && /rys-cron/.test(j.runbook), '台帳の purpose / where / runbook に廃止・起動条件・ログの探し方がある');
  eq(reg.validateRegistry(), [], '台帳のバリデーションは通る');
  ok(!reg.RETIRED_JOBS.some((e) => e.id === 'rys-daily-refresh'), '退役台帳には無い (現役)');
  const cron = src('apps/rakuten-yahoo-sync/services/rys-cron.js');
  ok(/import \{ pingJob \} from '\.\.\/\.\.\/jobs-monitor\/ping-local\.js'/.test(cron), 'rys-cron は Render 内 ping ヘルパー (ping-local) を使う');
  ok(/export const RYS_JOB_ID = 'rys-daily-refresh'/.test(cron), 'cron の job id は台帳と同じ');
  eq((cron.match(/pingJob\(RYS_JOB_ID, 'ok'/g) || []).length, 2, 'ok ping = パイプライン成功 + full sync のみ成功 の 2 経路');
  eq((cron.match(/pingJob\(RYS_JOB_ID, 'fail'/g) || []).length, 2, 'fail ping = パイプライン失敗 + full sync 失敗 の 2 経路');
  ok(!/pingJob\(RYS_JOB_ID, '(ok|fail)'[^\n]*already_running/.test(cron), '「前の回がまだ走っている」(409) では ping を打たない (締切超過で見える)');
  const inv = src('apps/jobs-monitor/test-schedule-inventory.js');
  ok(/rys-cron\.js': \{ count: 1, job: 'rys-daily-refresh' \}/.test(inv), '棚卸しテストは rys-cron を台帳 id で宣言 (exempt ではない)');
}

console.log(`\n${fail === 0 ? '✅' : '❌'} pass=${pass} fail=${fail}`);
process.exit(fail === 0 ? 0 : 1);
