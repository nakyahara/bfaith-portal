/**
 * ping-local.js (Render 内ジョブ用 ping ヘルパー) の検証
 * 実行: node apps/jobs-monitor/test-ping-local.js
 *
 * 守りたい不変条件:
 *   1. jobs-monitor が mount されていない環境 (miniPC) では記録先 DB を作らない
 *   2. 監視の記録失敗でジョブ本体を巻き添えにしない (throw しない)
 *   3. 常駐ワーカー用の間引きが効く (60秒ワーカーが1日1,440回書き込まない)
 *   4. コード内の ping 先 id が全部台帳に存在する (id のタイプミスで無音になるのを防ぐ)
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'ping-local-test-'));
process.env.DATA_DIR = TMP;

let pass = 0;
const fails = [];
function ok(cond, label) { if (cond) pass++; else fails.push(label); }
function eq(actual, expected, label) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) pass++;
  else fails.push(`${label}: 期待 ${JSON.stringify(expected)} / 実際 ${JSON.stringify(actual)}`);
}

const DB_FILE = path.join(TMP, 'jobs-monitor.db');

// ── 1. 無効環境では DB を作らない ──
delete process.env.JOBS_MONITOR_ENABLED;
const { pingJob, pingJobThrottled, _resetThrottleForTest } = await import('./ping-local.js');

eq(pingJob('fba-daily-sync', 'ok', 'note'), false, '無効環境の pingJob は false');
eq(pingJobThrottled('po-email-dispatcher', 'note'), false, '無効環境の pingJobThrottled は false');
ok(!fs.existsSync(DB_FILE), '無効環境では jobs-monitor.db を作らない (miniPC で余計な DB を生やさない)');

// ── 2. 有効環境では記録される ──
process.env.JOBS_MONITOR_ENABLED = '1';
eq(pingJob('fba-daily-sync', 'ok', 'sku=100'), true, '有効環境の pingJob は true');
ok(fs.existsSync(DB_FILE), '有効環境では jobs-monitor.db が作られる');

const { getStates } = await import('./store.js');
const states = getStates();
ok(states['fba-daily-sync']?.lastOkAtMs > 0, 'ok ping が last_ok_at に記録される');
eq(states['fba-daily-sync']?.lastStatus, 'ok', 'last_status が ok');

pingJob('inbound-info-daily', 'fail', 'nefuda取得失敗');
const states2 = getStates();
eq(states2['inbound-info-daily']?.lastStatus, 'fail', 'fail ping も記録される');
ok(!states2['inbound-info-daily']?.lastOkAtMs, 'fail では last_ok_at が入らない (dead-man の基準は ok だけ)');

// ── 3. note は 180 字で切る (長い例外メッセージで DB を膨らませない) ──
pingJob('mgmt-auto-sync', 'fail', 'あ'.repeat(500));
const states3 = getStates();
ok((states3['mgmt-auto-sync']?.lastNote ?? '').length <= 180, 'note は 180 字までに切られる');

// ── 4. 間引き ──
_resetThrottleForTest();
eq(pingJobThrottled('warehouse-healthcheck', 'x', 60_000), true, '間引き: 初回は打つ (再起動を無音にしない)');
eq(pingJobThrottled('warehouse-healthcheck', 'x', 60_000), false, '間引き: 直後の2回目は打たない');
eq(pingJobThrottled('po-email-dispatcher', 'x', 60_000), true, '間引き: id が違えば打つ');
eq(pingJobThrottled('warehouse-healthcheck', 'x', 0), true, '間引き: 間隔0なら毎回打つ');

// ── 5. isRender の判定 ──
{
  const { isRender } = await import('../../lib/is-render.js');
  ok(isRender({ RENDER: 'true' }), 'isRender: RENDER=true');
  ok(isRender({ RENDER: '1' }), 'isRender: RENDER=1');
  ok(!isRender({}), 'isRender: 未設定は false (miniPC)');
  ok(!isRender({ RENDER: '' }), 'isRender: 空文字は false');
  // truthy 判定だと "false"/"0" を Render 扱いしてしまう (Codexレビュー Medium)
  ok(!isRender({ RENDER: 'false' }), 'isRender: "false" は false');
  ok(!isRender({ RENDER: '0' }), 'isRender: "0" は false');
  ok(isRender({ RENDER: ' TRUE ' }), 'isRender: 前後空白と大文字も拾う');
  // 許可リスト方式: 打ち間違いを Render 扱いしない (falsy 除外方式だと通ってしまう)
  ok(!isRender({ RENDER: 'tru' }), 'isRender: 打ち間違い "tru" は false');
  ok(!isRender({ RENDER: 'render' }), 'isRender: 想定外の値は false');
}

// ── 6. 記録が失敗しても throw しない / 間引きを進めない ──
{
  const store = await import('./store.js');
  const db = store.getJobsDB();
  db.exec('DROP TABLE job_state'); // 記録先を壊す
  let threw = false;
  try { pingJob('fba-daily-sync', 'ok', 'after drop'); } catch { threw = true; }
  ok(!threw, '記録先が壊れていても pingJob は throw しない (ジョブ本体を巻き添えにしない)');
  eq(pingJob('fba-daily-sync', 'ok'), false, '記録に失敗したら false を返す');

  // 失敗した周期で間引きの起点を進めてしまうと、生きているワーカーが締切超過に見える
  _resetThrottleForTest();
  eq(pingJobThrottled('mgmt-auto-sync', 'x', 60_000), false, '記録失敗時の throttled は false');
  db.exec(`CREATE TABLE job_state (
    job_id TEXT PRIMARY KEY, last_ok_at INTEGER, last_alive_at INTEGER,
    last_ping_at INTEGER NOT NULL, last_status TEXT NOT NULL, last_note TEXT,
    partial_streak INTEGER NOT NULL DEFAULT 0)`); // 記録先を復旧
  eq(pingJobThrottled('mgmt-auto-sync', 'x', 60_000), true,
    '記録が復旧したら間引き間隔を待たずに次の ping が通る (失敗周期で起点を進めない)');
}

// ── 7. コード内の ping 先 id が全部台帳にある ──
const { JOBS_REGISTRY } = await import('../../config/jobs-registry.mjs');
const registered = new Set(JOBS_REGISTRY.map((e) => e.id));
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const SOURCES = [
  'apps/fba-replenishment/router.js',
  'apps/inbound-info/sync-job.js',
  'apps/ranking-checker/scheduler.js',
  'apps/mgmt-accounting/router.js',
  'apps/purchase-orders/email.js',
  'apps/warehouse/healthcheck.js',
  'apps/product-hub/intake-cron.js',
];
const usedIds = new Set();
for (const rel of SOURCES) {
  const src = fs.readFileSync(path.join(ROOT, rel), 'utf8');
  for (const m of src.matchAll(/(?:pingJob|pingJobThrottled|recordPing)\(\s*'([a-z0-9-]+)'/g)) {
    usedIds.add(m[1]);
  }
}
ok(usedIds.size >= 7, `ping 先 id を検出できる (検出: ${[...usedIds].join(', ')})`);
for (const id of usedIds) {
  ok(registered.has(id), `ping 先 "${id}" が台帳に登録されている`);
}

// ── 後片付け ──
try {
  const store = await import('./store.js');
  store.getJobsDB().close();
} catch { /* 5. でテーブルを壊しているので閉じられなくても構わない */ }
try { fs.rmSync(TMP, { recursive: true, force: true }); } catch { /* Windows で掴まれていたら残す */ }

if (fails.length) {
  console.error(`❌ ${fails.length} 件失敗 / ${pass} 件成功`);
  for (const f of fails) console.error(`  - ${f}`);
  process.exitCode = 1;
} else {
  console.log(`✅ 全 ${pass} 件パス`);
}
