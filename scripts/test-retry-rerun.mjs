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
  assert.match(src, /runRetryRound\(state\.remaining_jobs, \{ publishGate, amazonChain: amazonChainActive\(state\) \}\)/);
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
