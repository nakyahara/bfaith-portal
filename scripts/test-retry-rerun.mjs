/**
 * test-retry-rerun.mjs — 自動 retry の「走らせ直しの依存」RERUN_AFTER (Company DB構想 10 §6.1.1 B4。Codex ③a-2 R1 H5・B-R0 #3)
 *
 * 固定する契約:
 *   1 RERUN_AFTER の決まり: どのジョブも JOB_DEFINITIONS と RETRY_ORDER にあり、下流は上流より後 (= 循環しない)
 *   2 Render同期 がこの回で成功 → 朝に成功していた マスタ照合 → CompanyDB見張り も走らせ直す (連鎖・1 回だけ・順番どおり)
 *   3 上流が失敗 / 見送り (f_sales が再失敗) なら走らせ直さない。照合が blocked で exit 0 (成功) でも見張りは走らせ直す
 *   4 足した下流の失敗も結果に入る (= 次の回の remaining_jobs に残る)
 * 使い方: node scripts/test-retry-rerun.mjs
 */
import assert from 'node:assert/strict';

const { runRetryRound, RERUN_AFTER, rerunAfterProblems, RETRY_ORDER, JOB_DEFINITIONS, runScript, failSummary, AMAZON_MAP_CHAIN, amazonChainActive } = await import('../apps/warehouse/retry-failed-jobs.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
/** 走らせた順と、ジョブごとの成否 (既定は成功) */
const fakeRun = (fails = {}) => { const calls = []; return { calls, run: (script, name) => { calls.push(name); return fails[name] ? { success: false, summary: '❌ 失敗' } : { success: true, summary: fails[`warn:${name}`] || '✅' }; } }; };

await ta('[1] RERUN_AFTER の決まり (定義・順番・下流は上流より後)', async () => {
  assert.deepEqual(rerunAfterProblems(), []);
  assert.deepEqual(RERUN_AFTER['Render同期'], ['マスタ照合']);
  // ⑦-2 PR-A: Amazon SKU の写しの鎖 (写し → f_sales → sales_velocity → pml_snapshot → Render同期) は写しの鎖の回だけ = RERUN_AFTER には載せない (今の本番の retry を変えない。#1649 Codex R2 Medium)
  assert.deepEqual(Object.keys(RERUN_AFTER), ['Render同期', 'マスタ照合']);
  assert.deepEqual(AMAZON_MAP_CHAIN, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot', 'Render同期']);
  for (let i = 1; i < AMAZON_MAP_CHAIN.length; i++) { assert.ok(RETRY_ORDER.indexOf(AMAZON_MAP_CHAIN[i - 1]) < RETRY_ORDER.indexOf(AMAZON_MAP_CHAIN[i]), AMAZON_MAP_CHAIN[i]); assert.ok(Object.hasOwn(JOB_DEFINITIONS, AMAZON_MAP_CHAIN[i])); }
  assert.deepEqual(JOB_DEFINITIONS['CompanyDB写し(Amazon SKU)'].args, ['--daily']);
  assert.deepEqual(RERUN_AFTER['マスタ照合'], ['新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);   // 照合が直ったら、新商品の許可 (PR-7) と影運転 (③c-1a) も新しい照合の回で
  assert.ok(RETRY_ORDER.indexOf('マスタ照合') < RETRY_ORDER.indexOf('新商品の許可'));
  assert.ok(RETRY_ORDER.indexOf('マスタ照合') < RETRY_ORDER.indexOf('ロジザード毎日の商品マスタ(影)'));
  assert.deepEqual(JOB_DEFINITIONS['ロジザード毎日の商品マスタ(影)'].args, ['--daily']);
  assert.ok(RETRY_ORDER.indexOf('Render同期') < RETRY_ORDER.indexOf('マスタ照合') && RETRY_ORDER.indexOf('マスタ照合') < RETRY_ORDER.indexOf('CompanyDB見張り'));
  assert.deepEqual(JOB_DEFINITIONS['マスタ照合'].args, ['--daily']);   // 引数が無いと daily-sync の runScript と同じく '7' を付けられる
  // 決まりを破る例は見つかる
  assert.ok(rerunAfterProblems({ 'CompanyDB見張り': ['マスタ照合'] }).some((x) => /より後/.test(x)));
  assert.ok(rerunAfterProblems({ 'Render同期': ['無いジョブ'] }).some((x) => /JOB_DEFINITIONS/.test(x)));
});

await ta('[2] Render同期 がこの回で成功 → マスタ照合 → 見張り を走らせ直す (1 回だけ・順番どおり)', async () => {
  const f = fakeRun();
  const results = runRetryRound(['Render同期', 'CompanyDB見張り'], { run: f.run, log: quiet });
  assert.deepEqual(f.calls, ['Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  assert.deepEqual(results.map((r) => [r.name, r.success]), [['Render同期', true], ['マスタ照合', true], ['新商品の許可', true], ['ロジザード毎日の商品マスタ(影)', true], ['CompanyDB見張り', true]]);
  // 照合だけ失敗していた朝 → 照合 → 見張り
  const g = fakeRun();
  runRetryRound(['マスタ照合'], { run: g.run, log: quiet });
  assert.deepEqual(g.calls, ['マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  // 照合が blocked (exit 0 = 成功 + ⚠️) でも見張りは走らせ直す
  const h = fakeRun({ 'warn:マスタ照合': '⚠️ マスタ照合 ①: 判定できない (material_not_matched)' });
  runRetryRound(['Render同期'], { run: h.run, log: quiet });
  assert.deepEqual(h.calls, ['Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
});

await ta('[3] 上流が失敗 / 見送りなら走らせ直さない', async () => {
  const f = fakeRun({ 'Render同期': true });
  const results = runRetryRound(['Render同期'], { run: f.run, log: quiet });
  assert.deepEqual(f.calls, ['Render同期']);
  assert.deepEqual(results.map((r) => [r.name, r.success]), [['Render同期', false]]);
  // f_sales が再失敗 = Render同期 は見送り (⏸️) = 走らせ直さない
  const g = fakeRun({ f_sales: true });
  const r2 = runRetryRound(['f_sales', 'Render同期'], { run: g.run, log: quiet });
  assert.deepEqual(g.calls, ['f_sales']);
  assert.deepEqual(r2.map((r) => [r.name, r.success]), [['f_sales', false], ['Render同期', false]]);
});

await ta('[4] 足した下流の失敗も結果に入る (次の回の remaining_jobs に残る)', async () => {
  const f = fakeRun({ 'マスタ照合': true });
  const results = runRetryRound(['Render同期'], { run: f.run, log: quiet });
  assert.deepEqual(f.calls, ['Render同期', 'マスタ照合']);   // 照合が失敗 = 見張りは走らせ直さない (新しい結果が無い)
  assert.deepEqual(results.filter((r) => !r.success).map((r) => r.name), ['マスタ照合']);
});

await ta('[4b] ⑦-2 PR-A: 朝は写しだけ失敗 (f_sales・Render同期 は古い対応で成功) → 写しが直った回に f_sales → sales_velocity → pml_snapshot → Render同期 (→ 照合の鎖) を走らせ直す (新しい mirror_sku_* と古い対応の f_sales を同じ回に送らない)', async () => {
  const f = fakeRun();
  const results = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: f.run, log: quiet });
  assert.deepEqual(f.calls, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  assert.ok(results.every((r) => r.success));
  // 写しがまた失敗 = 何も走らせ直さない (朝の f_sales・Render同期 は前の対応のまま = 混ざらない)
  const g = fakeRun({ 'CompanyDB写し(Amazon SKU)': true });
  runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: g.run, log: quiet });
  assert.deepEqual(g.calls, ['CompanyDB写し(Amazon SKU)']);
  // 写しは直ったが作り直した f_sales が失敗 = その後 (速度・リスト・Render同期) は流さず「見送り」で残す (次の回も鎖で f_sales から)
  const h = fakeRun({ f_sales: true });
  const r3 = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: h.run, log: quiet });
  assert.deepEqual(h.calls, ['CompanyDB写し(Amazon SKU)', 'f_sales']);
  assert.deepEqual(r3.filter((r) => !r.success).map((r) => r.name), ['f_sales', 'sales_velocity', 'pml_snapshot', 'Render同期']);
  assert.equal(r3.amazonChainPending, true);
  // 写しの反映の門が壊れている朝 = f_sales 以降は再試行でも動かさない (publish-gate.js。写しそのものは止めない)
  const k = fakeRun();
  const r4 = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: k.run, log: quiet, publishGate: { broken: true, state: 'broken' } });
  assert.deepEqual(k.calls, ['CompanyDB写し(Amazon SKU)']);
  assert.deepEqual(r4.map((r) => [r.name, r.success, !!r.gated]), [['CompanyDB写し(Amazon SKU)', true, false], ['f_sales', false, true], ['sales_velocity', false, false], ['pml_snapshot', false, false], ['Render同期', false, false]]);
});

await ta('[4c] 写しの鎖の回 (#1649 Codex R1 Medium 2 / R2 Medium): 写し → f_sales → 速度 → リスト → Render同期 を一段ずつ・途中が落ちたらその先は流さず残す・次の回は落ちたところから一段ずつ', async () => {
  // 鍵待ち (exit 73) の朝 = retry-state は写しだけ → 写しが直った回に一段ずつ
  const z = fakeRun();
  const rz = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: z.run, log: quiet });
  assert.deepEqual(z.calls, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  assert.equal(rz.amazonChainPending, false);
  // 速度が落ちる = リスト・Render は流さず残す
  const a = fakeRun({ sales_velocity: true });
  const ra = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: a.run, log: quiet });
  assert.deepEqual(a.calls, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity']);
  assert.deepEqual(ra.filter((r) => !r.success).map((r) => [r.name, r.summary]), [['sales_velocity', '❌ 失敗'], ['pml_snapshot', '⏸️ skipped (sales_velocity 失敗・Amazon SKU の写しの鎖)'], ['Render同期', '⏸️ skipped (sales_velocity 失敗・Amazon SKU の写しの鎖)']]);
  assert.equal(ra.amazonChainPending, true);
  // 次の回 (retry-state の amazon_map_chain = true) = 速度から一段ずつ最後まで
  const state = { remaining_jobs: ra.filter((r) => !r.success).map((r) => r.name), amazon_map_chain: ra.amazonChainPending };
  assert.equal(amazonChainActive(state), true);
  const a2 = fakeRun();
  runRetryRound(state.remaining_jobs, { run: a2.run, log: quiet, amazonChain: amazonChainActive(state) });
  assert.deepEqual(a2.calls, ['sales_velocity', 'pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  // 次の回も速度が落ちる = その先は流さない (残る)
  const a3 = fakeRun({ sales_velocity: true });
  const ra3 = runRetryRound(state.remaining_jobs, { run: a3.run, log: quiet, amazonChain: true });
  assert.deepEqual(a3.calls, ['sales_velocity']);
  assert.deepEqual(ra3.map((r) => [r.name, r.success]), [['sales_velocity', false], ['pml_snapshot', false], ['Render同期', false]]);
  // リストが落ちる = Render は流さない
  const b = fakeRun({ pml_snapshot: true });
  const rb = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: b.run, log: quiet });
  assert.deepEqual(b.calls, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot']);
  assert.deepEqual(rb.filter((r) => !r.success).map((r) => r.name), ['pml_snapshot', 'Render同期']);
  const b2 = fakeRun();
  runRetryRound(['pml_snapshot', 'Render同期'], { run: b2.run, log: quiet, amazonChain: true });
  assert.deepEqual(b2.calls, ['pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  // 写しの鎖の回か: 写しが remaining にある・前の回の印 / どちらも無い = 今までの retry
  assert.equal(amazonChainActive({ remaining_jobs: ['CompanyDB写し(Amazon SKU)'] }), true);
  assert.equal(amazonChainActive({ remaining_jobs: ['f_sales', 'Render同期'] }), false);
  assert.equal(amazonChainActive({ remaining_jobs: ['f_sales'], amazon_map_chain: false }), false);
  assert.equal(amazonChainActive(null), false);
});

await ta('[4d] 🚨 今の本番 (持ち主 load = 写しは retry に載らない) の retry は前のまま (#1649 Codex R2 Medium): リスト・速度が落ちても Render同期 は流れる・f_sales が直っても速度・リストを走らせ直さない', async () => {
  const p = fakeRun({ pml_snapshot: true });
  const rp = runRetryRound(['pml_snapshot', 'Render同期'], { run: p.run, log: quiet });
  assert.deepEqual(p.calls, ['pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  assert.deepEqual(rp.filter((r) => !r.success).map((r) => r.name), ['pml_snapshot']);
  assert.equal(rp.amazonChainPending, false);
  const v = fakeRun({ sales_velocity: true });
  runRetryRound(['sales_velocity', 'pml_snapshot', 'Render同期'], { run: v.run, log: quiet });
  assert.deepEqual(v.calls, ['sales_velocity', 'pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  const f = fakeRun();
  runRetryRound(['f_sales', 'Render同期'], { run: f.run, log: quiet });
  assert.deepEqual(f.calls, ['f_sales', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  // retry-state の書き方: 鎖の途中のときだけ amazon_map_chain = true (main が残す)
  const src = (await import('node:fs')).readFileSync(new URL('../apps/warehouse/retry-failed-jobs.js', import.meta.url), 'utf8');
  assert.match(src, /amazon_map_chain: results\.amazonChainPending === true,/);
  assert.match(src, /runRetryRound\(state\.remaining_jobs, \{ publishGate, amazonChain: amazonChainActive\(state\) && await amazonHintNow\(\) \}\)/);   // 肯定の手がかりがある日だけ (#1649 Codex R3 Medium 2)
});

await ta('[6] 手の口 (--amazon-map-chain・#1649 Codex R3 High / Medium 3): 回の鍵を持ったまま 写し → f_sales → 速度 → リスト → Render同期 を一続き・途中で落ちたら先は流さない・写しに手の旗を渡す', async () => {
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  const fs = await import('node:fs'); const os = await import('node:os'); const path = await import('node:path');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'amzA-chain-'));
  const lockFile = path.join(dir, 'retry-failed-jobs.lock.json'), dailySyncLockFile = path.join(dir, 'daily-sync.lock.json');
  const NOON = new Date('2030-01-10T03:00:00Z');   // 12:00 JST
  const base = { lockFile, dailySyncLockFile, now: NOON, log: quiet, publishGate: { broken: false }, isAlive: (pid) => pid === process.pid };
  try {
    const calls = [];
    const run = (fails = {}) => (script, name, t, args) => { calls.push([name, args]); return fails[name] ? { success: false, summary: '❌' } : { success: true, summary: '✅' }; };
    const ok = await R.manualAmazonChain(['--amazon-map-chain', '--allow-shrink', '--expect-hash', 'a'.repeat(64)], { ...base, run: run() });
    assert.equal(ok.code, 0);
    assert.deepEqual(calls.map((c) => c[0]), ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot', 'Render同期']);
    assert.deepEqual(calls[0][1], ['--chain', '--allow-shrink', '--expect-hash', 'a'.repeat(64)]);   // 写しには手の旗 (回の鍵を持つ親の子 = --chain)
    assert.deepEqual(calls[1][1], R.JOB_DEFINITIONS.f_sales.args);
    assert.equal(fs.existsSync(lockFile), false, '終わったら回の鍵を外す');
    // 途中 (速度) で落ちる = リスト・Render は流さない
    calls.length = 0;
    const ng = await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: run({ sales_velocity: true }) });
    assert.equal(ng.code, 1);
    assert.deepEqual(calls.map((c) => c[0]), ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity']);
    // 写しが落ちる = 何も流さない
    calls.length = 0;
    assert.equal((await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: run({ 'CompanyDB写し(Amazon SKU)': true }) })).code, 1);
    assert.deepEqual(calls.map((c) => c[0]), ['CompanyDB写し(Amazon SKU)']);
    // 写しの反映の門が壊れている = 門の一覧の工程 (f_sales) で止まる
    calls.length = 0;
    const g = await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: run(), publishGate: { broken: true, state: 'broken' } });
    assert.equal(g.code, 1); assert.deepEqual(calls.map((c) => c[0]), ['CompanyDB写し(Amazon SKU)']);
    // 知らない引数・07:00 の daily-sync の前 (06:00〜07:00 JST) = 断る (鍵も取らない)
    calls.length = 0;
    assert.equal((await R.manualAmazonChain(['--amazon-map-chain', '--daily'], { ...base, run: run() })).reason, 'args');
    assert.equal((await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: run(), now: new Date('2030-01-09T21:30:00Z') })).reason, 'quiet_window');   // 06:30 JST
    // 禁止の時間は鎖の最長 (各段の timeout の和) + 余裕を 07:00 から引いた時刻〜07:30 (#1649 Codex R4 High)。境を全部見る (JST = UTC + 9)
    const jst = (hm) => new Date(`2030-01-09T${String((Number(hm.slice(0, 2)) + 24 - 9) % 24).padStart(2, '0')}:${hm.slice(3)}:00Z`);
    for (const [hm, blocked] of [['04:29', false], ['04:30', true], ['05:59', true], ['06:30', true], ['07:00', true], ['07:29', true], ['07:30', false], ['12:00', false], ['23:59', false]]) {
      assert.equal(!!R.manualChainQuietReason(jst(hm)), blocked, hm);
    }
    assert.equal(R.MANUAL_CHAIN_MAX_MS, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot', 'Render同期'].reduce((n, j) => n + R.JOB_DEFINITIONS[j].timeoutMs, 0));
    assert.deepEqual(R.MANUAL_CHAIN_QUIET_JST, ['04:30', '07:30']);
    const toMin = (hm) => Number(hm.slice(0, 2)) * 60 + Number(hm.slice(3));
    assert.ok(toMin('07:00') - toMin(R.MANUAL_CHAIN_QUIET_JST[0]) >= R.MANUAL_CHAIN_MAX_MS / 60000 + 1, '始めの時刻から最長の鎖が 07:00 (と 60 秒の待ち) より前に終わる');
    assert.ok(toMin(R.MANUAL_CHAIN_QUIET_JST[1]) > toMin('07:01'), '07:00 の起動と 60 秒の待ちの間も断る');
    assert.deepEqual(calls, []);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

await ta('[7] 🚨 daily・retry・手の 3 つは同じ回の鍵でどれか 1 つだけ (Codex R3 の順番を再現): 手が先 → retry が退く / retry が先 → 手が退く / daily が動いている → 手が退く・どれも待たない', async () => {
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  const { acquireRetryLock, releaseRetryLock } = await import('../apps/warehouse/retry-lock.js');
  const fs = await import('node:fs'); const os = await import('node:os'); const path = await import('node:path');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'amzA-runlock-'));
  const lockFile = path.join(dir, 'retry-failed-jobs.lock.json'), dailySyncLockFile = path.join(dir, 'daily-sync.lock.json');
  const NOON = new Date('2030-01-10T03:00:00Z');
  const alive = new Set([process.pid]);
  const isAlive = (pid) => alive.has(pid);
  const base = { lockFile, dailySyncLockFile, now: NOON, log: quiet, publishGate: { broken: false }, isAlive };
  try {
    // 手が先: 写しの最中に retry の回が起動 = retry は回の鍵を取れず退く (retry-state に触らない = runLocked に入らない)
    let retryTry = null;
    const r1 = await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: (s, name) => {
      if (name === 'CompanyDB写し(Amazon SKU)') retryTry = acquireRetryLock({ lockFile, dailySyncLockFile, isAlive, now: NOON, pid: 99999 });
      return { success: true, summary: '✅' };
    } });
    assert.equal(r1.code, 0);
    assert.equal(retryTry.ok, false); assert.match(retryTry.reason, /前の再試行の回がまだ動いている/);
    // retry が先: 手は回の鍵を取れず退く (何も流さない・待たない)
    alive.add(4242);
    const held = acquireRetryLock({ lockFile, dailySyncLockFile, isAlive, now: NOON, pid: 4242 });
    assert.equal(held.ok, true);
    let ran = 0;
    const t0 = Date.now();
    const r2 = await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: () => { ran++; return { success: true }; } });
    assert.deepEqual([r2.code, r2.reason, ran], [1, 'locked', 0]);
    assert.ok(Date.now() - t0 < 5000);
    releaseRetryLock(held);
    // daily-sync が動いている: 手は退く
    alive.add(5151);
    fs.writeFileSync(dailySyncLockFile, JSON.stringify({ run_id: 'x', pid: 5151, started_at: NOON.toISOString() }));
    const r3 = await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: () => { ran++; return { success: true }; } });
    assert.deepEqual([r3.code, r3.reason, ran], [1, 'daily_sync', 0]);
    // daily-sync が終わった (持ち主が死んだ) = 流せる
    alive.delete(5151);
    const r4 = await R.manualAmazonChain(['--amazon-map-chain'], { ...base, run: () => { ran++; return { success: true }; } });
    assert.equal(r4.code, 0); assert.equal(ran, 5);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

await ta('[5] 失敗した子の最後の行 (❌ 理由) を要約に残す (Codex #1540 R1 Low)', async () => {
  const fs = await import('node:fs');
  const os = await import('node:os');
  const path = await import('node:path');
  const { fileURLToPath } = await import('node:url');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'retry-'));
  const file = path.join(dir, 'fail.mjs');
  fs.writeFileSync(file, "console.log('途中の行'); console.log('❌ 成果物をポータルに送れない (unreachable)'); process.exitCode = 1;");
  const projectDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const origErr = console.error, origLog = console.log;
  console.error = quiet; console.log = quiet;
  let r;
  try { r = runScript(path.relative(projectDir, file), '試験', 30000, []); } finally { console.error = origErr; console.log = origLog; }
  assert.equal(r.success, false);
  assert.match(r.summary, /^❌ 成果物をポータルに送れない \(unreachable\) \| /);
  // 長い最後の行でも失敗の内容は残る・timeout (stdout なし) は失敗の内容だけ・stdout が空でも
  const long = failSummary({ stdout: `途中\n${'あ'.repeat(500)}`, message: 'Command failed: node x.mjs ETIMEDOUT' });
  assert.ok(long.includes(' | Command failed: node x.mjs ETIMEDOUT') && long.length <= 200, long);
  assert.equal(failSummary({ message: 'spawnSync node ETIMEDOUT' }), 'spawnSync node ETIMEDOUT');
  assert.equal(failSummary({ stdout: '  \n', message: 'Command failed' }), 'Command failed');
  assert.equal(failSummary({ stdout: 'x', message: 'm'.repeat(300) }), `x | ${'m'.repeat(77)}`);   // 長い失敗の内容も 77 字で切れて子の行は残る
});

console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
