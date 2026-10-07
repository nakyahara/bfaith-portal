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
  // ⑦-2 PR-A (Codex 計画 R2 High 2): Amazon SKU の写し → f_sales → sales_velocity / pml_snapshot / Render同期 の鎖
  assert.deepEqual(RERUN_AFTER['CompanyDB写し(Amazon SKU)'], ['f_sales']);
  assert.deepEqual([RERUN_AFTER['f_sales'], RERUN_AFTER['sales_velocity'], RERUN_AFTER['pml_snapshot']], [['sales_velocity'], ['pml_snapshot'], ['Render同期']]);   // 直列 (#1649 Codex R1 Medium 2)
  assert.ok(RETRY_ORDER.indexOf('CompanyDB写し(Amazon SKU)') < RETRY_ORDER.indexOf('f_sales') && RETRY_ORDER.indexOf('f_sales') < RETRY_ORDER.indexOf('sales_velocity')
    && RETRY_ORDER.indexOf('sales_velocity') < RETRY_ORDER.indexOf('pml_snapshot') && RETRY_ORDER.indexOf('pml_snapshot') < RETRY_ORDER.indexOf('Render同期'));
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
  // 写しは直ったが作り直した f_sales が失敗 = その後 (速度・リスト・Render同期) は走らせ直さない (f_sales は次の回に残る)
  const h = fakeRun({ f_sales: true });
  const r3 = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: h.run, log: quiet });
  assert.deepEqual(h.calls, ['CompanyDB写し(Amazon SKU)', 'f_sales']);
  assert.deepEqual(r3.filter((r) => !r.success).map((r) => r.name), ['f_sales']);
  // 写しの反映の門が壊れている朝 = f_sales 以降は再試行でも動かさない (publish-gate.js。写しそのものは止めない)
  const k = fakeRun();
  const r4 = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: k.run, log: quiet, publishGate: { broken: true, state: 'broken' } });
  assert.deepEqual(k.calls, ['CompanyDB写し(Amazon SKU)']);
  assert.deepEqual(r4.map((r) => [r.name, r.success, !!r.gated]), [['CompanyDB写し(Amazon SKU)', true, false], ['f_sales', false, true]]);
});

await ta('[4c] f_sales → sales_velocity → pml_snapshot → Render同期 は直列 (#1649 Codex R1 Medium 2): 速度が落ちた / リストが落ちた回はその先を流さない・次の回で落ちたところから最後まで流す', async () => {
  // 速度が落ちる (走らせ直しの鎖)
  const a = fakeRun({ sales_velocity: true });
  const ra = runRetryRound(['CompanyDB写し(Amazon SKU)'], { run: a.run, log: quiet });
  assert.deepEqual(a.calls, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity']);
  assert.deepEqual(ra.filter((r) => !r.success).map((r) => r.name), ['sales_velocity']);
  // 次の回 = 速度から最後まで
  const a2 = fakeRun();
  runRetryRound(ra.filter((r) => !r.success).map((r) => r.name), { run: a2.run, log: quiet });
  assert.deepEqual(a2.calls, ['sales_velocity', 'pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  // リストが落ちる
  const b = fakeRun({ pml_snapshot: true });
  const rb = runRetryRound(['f_sales'], { run: b.run, log: quiet });
  assert.deepEqual(b.calls, ['f_sales', 'sales_velocity', 'pml_snapshot']);
  assert.deepEqual(rb.filter((r) => !r.success).map((r) => r.name), ['pml_snapshot']);
  const b2 = fakeRun();
  runRetryRound(['pml_snapshot'], { run: b2.run, log: quiet });
  assert.deepEqual(b2.calls, ['pml_snapshot', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  // 下流が朝から remaining_jobs にある回も、この回で試した上流が落ちたら止まる (見送りの失敗 = 次の回に残る)
  const c = fakeRun({ sales_velocity: true });
  const rc = runRetryRound(['sales_velocity', 'pml_snapshot', 'Render同期'], { run: c.run, log: quiet });
  assert.deepEqual(c.calls, ['sales_velocity']);
  assert.deepEqual(rc.map((r) => [r.name, r.success, r.summary]), [['sales_velocity', false, '❌ 失敗'], ['pml_snapshot', false, '⏸️ skipped (sales_velocity 再失敗)'], ['Render同期', false, '⏸️ skipped (pml_snapshot 再失敗)']]);
  const d = fakeRun({ pml_snapshot: true });
  const rd = runRetryRound(['pml_snapshot', 'Render同期'], { run: d.run, log: quiet });
  assert.deepEqual(d.calls, ['pml_snapshot']);
  assert.deepEqual(rd.filter((r) => !r.success).map((r) => r.name), ['pml_snapshot', 'Render同期']);
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
