/**
 * test-new-entry-gate-step.mjs — 毎朝の照合 ② の次の 1 段「新商品の許可」(PR-7・計画 newentry_min_plan.md §3 の 2)
 *
 * 固定する契約:
 *   1 照合 (マスタ照合) が失敗・見送り = この段を流さない (⏭️・blocked = この段だけを retry に載せない)。成功 (⚠️ を含む) = 流す
 *   2 許可が出た = 「🆕 新商品の入口: 開 (〜翌日 10:00」・exit 0・grant は ('single', その朝の照合の回) で 1 回
 *   3 拒まれた = revoke してから理由つきで「閉」・exit 1 (retry)。widen の前だけで拒まれた = ⏸️ 閉・exit 0 (準備中)。取り消しも落ちた = exit 1
 *   4 接続先が無い = 「未設定」・exit 0・接続しない / 関数が無い (0058 の前) = 「閉のまま」・exit 0・grant も revoke も呼ばない
 *   5 照合 ② が判定できない・落ちた・完了していない = grant を呼ばない (revoke だけ・exit 0) / その回の証跡が無い・別の回・別の日 = grant を呼ばない・exit 1
 *   6 接続できない・関数の失敗 = exit 1
 *   7 本物のプロセス (CLI): 未設定 = 最後の行が「未設定」で exit 0
 *   8 daily-sync の配線: 照合の直後 (ロジザードの影の前)・skipAfterCompare で守る・retry の対象
 *   9 retry: JOB_DEFINITIONS・RETRY_ORDER (照合より後)・RERUN_AFTER (照合が直ったら流す)・照合をこの回で再試行して失敗したら見送る
 *  10 台帳: warehouse-daily-sync の purpose / runbook に載る (新しいエントリは作らない)・写しの門の一覧 (止めない側) に載る
 * 使い方: node scripts/test-new-entry-gate-step.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const S = await import('../apps/company-db/master-compare/new-entry-gate.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const NOW = new Date('2026-10-07T00:30:00Z');   // JST 2026-10-07 09:30
const AS_OF = '2026-10-07';
const RUN = 'ds_20261006220000000';
const CR = 'mc_20261006T221500123Z_abc123';
const ENV = { COMPANY_DB_NEW_ENTRY_GATE_URL: 'postgres://new_entry_gate@x/db', DAILY_SYNC_RUN_ID: RUN };
const evOk = (over = {}) => ({ name: 'master-compare', state: 'complete', as_of: AS_OF, sync_run_id: RUN, compare_run_id: CR, verdict: 'pass', ne: { verdict: 'pass' }, ...over });

/** 偽の DB (0058 の関数の約束どおり: 拒む = 'lease_denied: …' の例外) */
function fakeDb({ hasFn = true, grant = null, grantError = null, revokeError = null, queryError = null } = {}) {
  const calls = [];
  let closed = 0;
  const db = {
    async query(text, params) {
      calls.push({ text, params });
      if (queryError) throw new Error(queryError);
      if (/to_regprocedure\('ops\.grant_new_entry_lease\(text, text\)'\)/.test(text)) return { rows: [{ ok: hasFn }] };
      if (/ops\.grant_new_entry_lease\(\$1, \$2\)/.test(text)) {
        if (grantError) { const e = new Error(grantError); e.code = 'P0001'; throw e; }
        return { rows: [{ r: grant ?? { lease_id: '7', kind: 'single', result_id: '42', compare_run_id: params[1], expires_at: '2026-10-08T01:00:00+00:00' } }] };
      }
      if (/ops\.revoke_new_entry_lease\(\$1, \$2\)/.test(text)) {
        if (revokeError) throw new Error(revokeError);
        return { rows: [{ r: { revoked: 0, floor_result_id: '42' } }] };
      }
      throw new Error(`知らない問い合わせ: ${text}`);
    },
  };
  return { calls, db, connect: async () => ({ db, close: async () => { closed++; } }), closed: () => closed };
}
const grants = (f) => f.calls.filter((c) => /grant_new_entry_lease\(\$1/.test(c.text));
const revokes = (f) => f.calls.filter((c) => /revoke_new_entry_lease/.test(c.text));
const run = (f, { env = ENV, ev = evOk(), name = 'master-compare' } = {}) =>
  S.runGateStep({ env, dataDir: 'D:/fake', now: NOW, connect: f.connect, readEv: (d, a) => { assert.equal(a, AS_OF); return ev === null ? {} : { [name]: ev }; } });

await ta('[1] 照合が失敗・見送り = 流さない (⏭️・blocked)。成功 (⚠️ を含む) = 流す', async () => {
  for (const r of [{ success: false, summary: '❌ x' }, { success: false, blocked: true, gated: true, summary: '⚠️ 見送り' }, undefined, null]) {
    const s = S.skipAfterCompare(r);
    assert.deepEqual([s.name, s.success, s.skipped, s.blocked], ['新商品の許可', false, true, true]);
    assert.match(s.summary, /^⏭️ 見送り \(マスタ照合が失敗 = 🆕 新商品の入口: 閉のまま/);
  }
  assert.equal(S.skipAfterCompare({ success: true, summary: '✅' }), null);
  assert.equal(S.skipAfterCompare({ success: true, summary: '⚠️ ②: 判定できない' }), null);   // blocked の判定はこの段が証跡で見る
});

await ta('[2] 許可が出た = 開 (〜翌日 10:00)・exit 0・grant は (single, その朝の照合の回) で 1 回・接続を閉じる', async () => {
  const f = fakeDb();
  const r = await run(f);
  assert.equal(r.code, 0);
  assert.equal(r.state, 'opened');
  assert.match(r.line, /^🆕 新商品の入口: 開 \(〜翌日 10:00 = 10\/08 10:00 JST・照合 mc_20261006T221500123Z_abc123・許可 #7\)$/);
  assert.deepEqual(grants(f).map((c) => c.params), [['single', CR]]);
  assert.equal(revokes(f).length, 0);
  assert.equal(f.closed(), 1);
  // ② の警告 (差がある) は許可を止めない = DB の関数が kind_gate で決める
  const g = fakeDb();
  assert.equal((await run(g, { ev: evOk({ verdict: 'breach', ne: { verdict: 'breach' } }) })).state, 'opened');
});

await ta('[3] 拒まれた = revoke してから理由つきで閉・exit 1 / widen の前だけ = ⏸️ 閉・exit 0 / 取り消しも落ちた = exit 1', async () => {
  const f = fakeDb({ grantError: 'lease_denied: kind_gate: 区分のゲートの数が 0 でない (E_only) / stop_floor: 結果の行 41 は止めた時点の行 (41) より新しくない' });
  const r = await run(f);
  assert.deepEqual([r.code, r.state], [1, 'denied']);
  assert.match(r.line, /^🆕 新商品の入口: 閉 \(拒まれた: kind_gate: 区分のゲートの数が 0 でない \(E_only\) \/ stop_floor: /);
  assert.equal(revokes(f).length, 1);
  assert.equal(revokes(f)[0].params[0], 'single');
  assert.match(revokes(f)[0].params[1], /^許可を出せない \(mc_20261006T221500123Z_abc123\): lease_denied: kind_gate/);
  assert.ok(f.calls.findIndex((c) => /revoke/.test(c.text)) > f.calls.findIndex((c) => /grant_new_entry_lease\(\$1/.test(c.text)));
  // widen の前 (区分の持ち主が company でない・widen の記録が無い) だけ = 準備中
  const p = fakeDb({ grantError: 'lease_denied: sku_kind_not_company: 区分の持ち主が company でない (widen の前) / not_widened: skus.sku_kind を広げた記録が無い' });
  const rp = await run(p);
  assert.deepEqual([rp.code, rp.state], [0, 'prep']);
  assert.match(rp.line, /^⏸️ 🆕 新商品の入口: 閉 \(widen の前 = 準備中: sku_kind_not_company/);
  assert.equal(revokes(p).length, 1);
  // widen の前 + ほかの理由 = 拒まれた (exit 1)
  const q = fakeDb({ grantError: 'lease_denied: not_widened: x / compare_run_mismatch: 一番新しい結果の行の回 (なし) が今回の照合の回 (y) でない' });
  assert.deepEqual([(await run(q)).code, (await run(fakeDb({ grantError: 'lease_denied: not_widened: x / compare_run_mismatch: y' }))).state], [1, 'denied']);
  // 取り消しも落ちた = exit 1 (準備中でも)
  const v = fakeDb({ grantError: 'lease_denied: not_widened: x', revokeError: 'permission denied' });
  const rv = await run(v);
  assert.equal(rv.code, 1);
  assert.match(rv.line, /取り消しも失敗 \(permission denied\)/);
  // 拒むのでない失敗 (鍵の待ちの打ち切りなど) = 許可を出せない・revoke・exit 1
  const t = fakeDb({ grantError: 'canceling statement due to statement timeout' });
  const rt = await run(t);
  assert.deepEqual([rt.code, rt.state], [1, 'error']);
  assert.match(rt.line, /閉 \(許可を出せない: canceling statement/);
  assert.equal(revokes(t).length, 1);
  assert.deepEqual(S.deniedCodes('lease_denied: kind_gate: a / shape: {"x": 1} / not_today: b'), ['kind_gate', 'shape', 'not_today']);
  assert.deepEqual(S.deniedCodes('別の失敗'), []);
});

await ta('[4] 接続先が無い = 未設定・exit 0・接続しない / 関数が無い (0058 の前) = 閉のまま・exit 0・grant も revoke も呼ばない', async () => {
  let connected = 0;
  for (const env of [{ DAILY_SYNC_RUN_ID: RUN }, { COMPANY_DB_NEW_ENTRY_GATE_URL: '  ', DAILY_SYNC_RUN_ID: RUN }]) {
    const r = await S.runGateStep({ env, dataDir: 'D:/fake', now: NOW, connect: async () => { connected++; throw new Error('x'); }, readEv: () => ({ 'master-compare': evOk() }) });
    assert.deepEqual([r.code, r.state], [0, 'not_configured']);
    assert.equal(r.line, '🆕 新商品の入口: 未設定 (COMPANY_DB_NEW_ENTRY_GATE_URL が無い = この段は飛ばした・閉のまま)');
  }
  assert.equal(connected, 0);
  const f = fakeDb({ hasFn: false });
  const r = await run(f);
  assert.deepEqual([r.code, r.state], [0, 'not_applied']);
  assert.match(r.line, /^🆕 新商品の入口: 閉のまま \(許可の関数が無い = 0058 の前/);
  assert.equal(grants(f).length + revokes(f).length, 0);
  // 照合 ② が判定できない朝でも 0058 の前なら同じ (exit 0)
  const g = fakeDb({ hasFn: false });
  assert.equal((await run(g, { ev: evOk({ ne: { verdict: 'blocked', blocked_reason: 'x' } }) })).code, 0);
  assert.equal(revokes(g).length, 0);
});

await ta('[5] 照合 ② が判定できない・落ちた・完了していない = grant を呼ばない (revoke・exit 0) / 証跡が無い・別の回・別の日 = grant を呼ばない・exit 1', async () => {
  const closedCases = [
    [evOk({ ne: { verdict: 'blocked', blocked_reason: 'material_not_matched' } }), /照合 ② が判定できない \(material_not_matched\)/],
    [evOk({ ne: { verdict: 'error', error: 'timeout' } }), /照合 ② が落ちた \(timeout\)/],
    [evOk({ ne: null }), /照合 ② が流れていない/],
    [{ state: 'skipped', as_of: AS_OF, sync_run_id: RUN, reason: 'COMPANY_DB_WATCH_URL が無い' }, /照合が完了していない \(skipped: COMPANY_DB_WATCH_URL が無い\)/],
    [{ state: 'running', as_of: AS_OF, sync_run_id: RUN, compare_run_id: CR }, /照合が完了していない \(running\)/],
    [{ state: 'failed', as_of: AS_OF, sync_run_id: RUN, compare_run_id: CR }, /照合が完了していない \(failed\)/],
  ];
  for (const [ev, re] of closedCases) {
    const f = fakeDb();
    const r = await run(f, { ev });
    assert.deepEqual([r.code, r.state], [0, 'compare_not_ready'], JSON.stringify(ev));
    assert.match(r.line, /^🆕 新商品の入口: 閉 \(/);
    assert.match(r.line, re);
    assert.equal(grants(f).length, 0);
    assert.equal(revokes(f).length, 1);
  }
  const errorCases = [
    [null, /照合の証跡が無い/],
    [evOk({ sync_run_id: 'ds_other' }), /この daily-sync の回のものでない \(ds_other\)/],
    [evOk({ sync_run_id: null }), /この daily-sync の回のものでない \(なし\)/],
    [evOk({ as_of: '2026-10-06' }), /今日のものでない \(2026-10-06\)/],
    [evOk({ compare_run_id: null }), /照合の回 \(compare_run_id\) が無い/],
    [{ name: 'master-compare', error: '証跡が読めない: Unexpected token' }, /照合の証跡が読めない/],
  ];
  for (const [ev, re] of errorCases) {
    const f = fakeDb();
    const r = await run(f, { ev });
    assert.deepEqual([r.code, r.state], [1, 'error'], JSON.stringify(ev));
    assert.match(r.line, re);
    assert.equal(grants(f).length, 0);
  }
  // 取り消しが落ちた = exit 1
  const g = fakeDb({ revokeError: 'boom' });
  const rg = await run(g, { ev: evOk({ ne: { verdict: 'blocked' } }) });
  assert.equal(rg.code, 1);
  assert.match(rg.line, /取り消しも失敗 \(boom\)/);
  // 手で流した回 (DAILY_SYNC_RUN_ID が無い) = 手の照合の証跡 master-compare.manual を読む
  const m = fakeDb();
  const rm = await run(m, { env: { COMPANY_DB_NEW_ENTRY_GATE_URL: 'postgres://x' }, ev: evOk({ sync_run_id: null }), name: 'master-compare.manual' });
  assert.equal(rm.state, 'opened');
  const m2 = fakeDb();
  assert.equal((await run(m2, { env: { COMPANY_DB_NEW_ENTRY_GATE_URL: 'postgres://x' }, ev: evOk() })).state, 'error');   // 朝の証跡は手の回には使わない
  assert.equal(grants(m2).length, 0);
});

await ta('[6] 接続できない・問い合わせの失敗 = exit 1', async () => {
  const r = await S.runGateStep({ env: ENV, dataDir: 'D:/fake', now: NOW, connect: async () => { throw new Error('ECONNREFUSED'); }, readEv: () => ({ 'master-compare': evOk() }) });
  assert.deepEqual([r.code, r.state], [1, 'error']);
  assert.match(r.line, /^🆕 新商品の入口: 閉 \(接続できない: ECONNREFUSED\)$/);
  const f = fakeDb({ queryError: 'connection terminated' });
  const r2 = await run(f);
  assert.deepEqual([r2.code, r2.state], [1, 'error']);
  assert.equal(f.closed(), 1);
  // 返り値が読めない = exit 1
  const g = fakeDb({ grant: { lease_id: '1' } });
  assert.equal((await run(g)).code, 1);
  // DATA_DIR が無い
  assert.equal((await S.runGateStep({ env: ENV, dataDir: '', now: NOW, connect: fakeDb().connect })).code, 1);
});

await ta('[7] 本物のプロセス: 未設定 = 最後の行が「未設定」で exit 0 / 知らない引数 = exit 1', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'neg-'));
  const env = { ...process.env, DATA_DIR: dir, DAILY_SYNC_RUN_ID: RUN, DOTENV_CONFIG_PATH: path.join(dir, 'none.env') };
  delete env.COMPANY_DB_NEW_ENTRY_GATE_URL;
  const out = execFileSync(process.execPath, [path.join(ROOT, 'apps/company-db/master-compare/new-entry-gate.mjs'), '--daily'], { cwd: dir, env, encoding: 'utf8', timeout: 30000 });
  assert.equal(out.trim().split('\n').pop(), '🆕 新商品の入口: 未設定 (COMPANY_DB_NEW_ENTRY_GATE_URL が無い = この段は飛ばした・閉のまま)');
  let st = null;
  try { execFileSync(process.execPath, [path.join(ROOT, 'apps/company-db/master-compare/new-entry-gate.mjs'), '--bogus'], { cwd: dir, env, encoding: 'utf8', timeout: 30000, stdio: 'pipe' }); } catch (e) { st = e.status; }
  assert.equal(st, 1);
  fs.rmSync(dir, { recursive: true, force: true });
});

await ta('[8] daily-sync の配線: 照合の直後 (影の前)・skipAfterCompare で守る・retry の対象', async () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const iCompare = ds.indexOf("runScript('apps/company-db/master-compare/run.mjs --daily', 'マスタ照合'");
  const iGuard = ds.indexOf('const newEntryGateSkip = skipAfterCompare(masterCompareResult);');
  const iGate = ds.indexOf("runScript('apps/company-db/master-compare/new-entry-gate.mjs --daily', NEW_ENTRY_GATE_STEP");
  const iLz = ds.indexOf("runScript('scripts/company-db/lz-daily.mjs --daily'");
  assert.ok(iCompare > 0 && iCompare < iGuard && iGuard < iGate && iGate < iLz, [iCompare, iGuard, iGate, iLz].join(','));
  assert.match(ds.slice(iGuard, iGate + 300), /const newEntryGateSkip = skipAfterCompare\(masterCompareResult\);\n\s*if \(newEntryGateSkip\) results\.push\(newEntryGateSkip\);\n\s*else \{\n\s*const newEntryGateResult = runScript\(/);
  assert.match(ds.slice(iGate, iGate + 300), /results\.push\(\{ name: NEW_ENTRY_GATE_STEP, \.\.\.newEntryGateResult, warn: newEntryGateResult\.success && isWarnSummary\(newEntryGateResult\.summary\) \}\);/);
  assert.equal([...ds.matchAll(/new-entry-gate\.mjs --daily/g)].length, 1);
  assert.match(ds, /import \{ skipAfterCompare, STEP_NAME as NEW_ENTRY_GATE_STEP \} from '\.\.\/company-db\/master-compare\/new-entry-gate\.mjs';/);
  const rj = ds.match(/const RETRYABLE_JOBS = \[([^\]]+)\]/)[1];
  assert.ok(rj.includes("'新商品の許可'"));
  assert.equal(S.STEP_NAME, '新商品の許可');
});

await ta('[9] retry: 定義・順 (照合より後)・照合が直ったら流す・照合をこの回で再試行して失敗したら見送る', async () => {
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  assert.deepEqual(R.JOB_DEFINITIONS['新商品の許可'], { script: 'apps/company-db/master-compare/new-entry-gate.mjs', args: ['--daily'], timeoutMs: 120000 });
  assert.ok(fs.existsSync(path.join(ROOT, R.JOB_DEFINITIONS['新商品の許可'].script)));
  const o = R.RETRY_ORDER;
  assert.ok(o.indexOf('マスタ照合') >= 0 && o.indexOf('マスタ照合') < o.indexOf('新商品の許可'));
  assert.ok(R.RERUN_AFTER['マスタ照合'].includes('新商品の許可'));
  assert.equal(R.UPSTREAM_OF['新商品の許可'], 'マスタ照合');
  assert.deepEqual(R.rerunAfterProblems(), []);
  const quiet = () => {};
  const fakeRun = (fails = {}) => { const calls = []; return { calls, run: (script, name) => { calls.push(name); return fails[name] ? { success: false, summary: '❌ 失敗' } : { success: true, summary: '✅' }; } }; };
  // 照合が retry で直った → 新商品の許可も流す (照合の直後)
  const a = fakeRun();
  R.runRetryRound(['マスタ照合'], { run: a.run, log: quiet });
  assert.deepEqual(a.calls.slice(0, 2), ['マスタ照合', '新商品の許可']);
  // Render同期 が直った → 照合 → 新商品の許可 (連鎖)
  const b = fakeRun();
  R.runRetryRound(['Render同期'], { run: b.run, log: quiet });
  assert.deepEqual(b.calls.slice(0, 3), ['Render同期', 'マスタ照合', '新商品の許可']);
  // 照合が再失敗 = 流さない
  const c = fakeRun({ 'マスタ照合': true });
  R.runRetryRound(['マスタ照合'], { run: c.run, log: quiet });
  assert.deepEqual(c.calls, ['マスタ照合']);
  // 新商品の許可も残っていて照合が再失敗 = 見送り (⏸️)
  const d = fakeRun({ 'マスタ照合': true });
  const rd = R.runRetryRound(['マスタ照合', '新商品の許可'], { run: d.run, log: quiet });
  assert.deepEqual(d.calls, ['マスタ照合']);
  assert.deepEqual(rd.map((r) => [r.name, r.success]), [['マスタ照合', false], ['新商品の許可', false]]);
  assert.match(rd[1].summary, /^⏸️ skipped \(マスタ照合 再失敗\)/);
  // 新商品の許可だけが残っていた (朝は照合が成功) = そのまま流す
  const e = fakeRun();
  R.runRetryRound(['新商品の許可'], { run: e.run, log: quiet });
  assert.deepEqual(e.calls, ['新商品の許可']);
});

await ta('[10] 台帳: warehouse-daily-sync の purpose / runbook に載る (新しいエントリは作らない)・写しの門は止めない側', async () => {
  const { JOBS_REGISTRY } = await import('../config/jobs-registry.mjs');
  const e = JOBS_REGISTRY.find((j) => j.id === 'warehouse-daily-sync');
  assert.match(e.purpose, /「新商品の許可」\(apps\/company-db\/master-compare\/new-entry-gate\.mjs --daily。新しい定期実行ではない/);
  assert.match(e.purpose, /COMPANY_DB_NEW_ENTRY_GATE_URL/);
  assert.match(e.runbook, /「新商品の許可」が閉/);
  assert.match(e.runbook, /new-entry-gate\.mjs --daily/);
  assert.deepEqual(JOBS_REGISTRY.filter((j) => j.id !== 'warehouse-daily-sync' && /new-entry-gate|新商品の許可/.test(`${j.id} ${j.where || ''}`)).map((j) => j.id), []);
  const G = await import('../apps/warehouse/publish-gate.js');
  assert.ok(Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, 'apps/company-db/master-compare/new-entry-gate.mjs'));
  assert.equal(G.publishGateDecision('apps/company-db/master-compare/new-entry-gate.mjs --daily', { broken: true }).skip, false);
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
