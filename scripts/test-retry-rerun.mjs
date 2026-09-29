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

const { runRetryRound, RERUN_AFTER, rerunAfterProblems, RETRY_ORDER, JOB_DEFINITIONS, runScript, failSummary } = await import('../apps/warehouse/retry-failed-jobs.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
/** 走らせた順と、ジョブごとの成否 (既定は成功) */
const fakeRun = (fails = {}) => { const calls = []; return { calls, run: (script, name) => { calls.push(name); return fails[name] ? { success: false, summary: '❌ 失敗' } : { success: true, summary: fails[`warn:${name}`] || '✅' }; } }; };

await ta('[1] RERUN_AFTER の決まり (定義・順番・下流は上流より後)', async () => {
  assert.deepEqual(rerunAfterProblems(), []);
  assert.deepEqual(RERUN_AFTER['Render同期'], ['マスタ照合']);
  assert.deepEqual(RERUN_AFTER['マスタ照合'], ['ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);   // 照合が直ったら、影運転 (③c-1a) も新しい照合の回で作り直す
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
  assert.deepEqual(f.calls, ['Render同期', 'マスタ照合', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  assert.deepEqual(results.map((r) => [r.name, r.success]), [['Render同期', true], ['マスタ照合', true], ['ロジザード毎日の商品マスタ(影)', true], ['CompanyDB見張り', true]]);
  // 照合だけ失敗していた朝 → 照合 → 見張り
  const g = fakeRun();
  runRetryRound(['マスタ照合'], { run: g.run, log: quiet });
  assert.deepEqual(g.calls, ['マスタ照合', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  // 照合が blocked (exit 0 = 成功 + ⚠️) でも見張りは走らせ直す
  const h = fakeRun({ 'warn:マスタ照合': '⚠️ マスタ照合 ①: 判定できない (material_not_matched)' });
  runRetryRound(['Render同期'], { run: h.run, log: quiet });
  assert.deepEqual(h.calls, ['Render同期', 'マスタ照合', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
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
