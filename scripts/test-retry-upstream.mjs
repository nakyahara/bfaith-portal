#!/usr/bin/env node
/**
 * test-retry-upstream.mjs — retry-failed-jobs.js の「上流 (取込) → 下流」の規則の試験。
 *   取込 (Qoo10) が失敗した朝に見送った Company DB への送信は、取込の再試行が成功した回に送る。取込がまた失敗した回は送らない (古い・途中の raw を送らない)。
 * 🚨 ここで見るのは規則・表の整合・実行ループ (子プロセスの起動を差し替えて)・起動の判定。state の読み書き・通知・最大回数の扱いと daily-sync.js の実行は試験に無い
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { JOB_DEFINITIONS, RETRY_ORDER, UPSTREAM_OF, upstreamBlock, runRetryRound, isDirectRun } from '../apps/warehouse/retry-failed-jobs.js';

let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const dailySync = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'daily-sync.js'), 'utf8');
const retryable = JSON.parse(`[${/const RETRYABLE_JOBS = \[([^\]]*)\]/.exec(dailySync)[1].replace(/'/g, '"')}]`);

t('表の整合: 上流も下流も retry の定義と順序にあり、上流が先に走る。上流は daily-sync でも retry の対象', () => {
  for (const [down, up] of Object.entries(UPSTREAM_OF)) {
    for (const j of [down, up]) { assert.ok(Object.hasOwn(JOB_DEFINITIONS, j), `${j} が JOB_DEFINITIONS に無い`); assert.ok(RETRY_ORDER.includes(j), `${j} が RETRY_ORDER に無い`); assert.ok(retryable.includes(j), `${j} が daily-sync の RETRYABLE_JOBS に無い`); }
    assert.ok(RETRY_ORDER.indexOf(up) < RETRY_ORDER.indexOf(down), `${up} が ${down} より後に走る`);
  }
  // 決済の取込 → 財務の送り手の組 (F2b-3) は、スイッチ CDB_FINANCE_COORDINATOR が無いとき (今までの 2 工程) の組 (#1567)。あるときは coordinator 'Amazon決済と財務' の 1 工程 = この組は走らない
  assert.deepEqual(UPSTREAM_OF, { 'CompanyDB注文(Qoo10)': 'Qoo10', 'CompanyDB広告費(Amazon)': 'Amazon Ads (SKU)', 'CompanyDB財務(Amazon)': 'Amazon Settlement', 'CompanyDB観測原価': 'm_products_history', '新商品の許可': 'マスタ照合',
    'f_sales': 'CompanyDB写し(Amazon SKU)' });   // ⑦-2 PR-A (Codex 計画 R2 High 2)。速度・リスト・Render の直列は写しの鎖の回だけ (AMAZON_MAP_CHAIN・#1649 Codex R2 Medium)
});
t('⑦-2 PR-A: Amazon SKU の対応の写しは 3 か所 (RETRYABLE_JOBS・JOB_DEFINITIONS・RETRY_ORDER) にあり、f_sales より前・daily-sync でも f_sales より前に走る', () => {
  const J = 'CompanyDB写し(Amazon SKU)';
  assert.ok(retryable.includes(J));
  assert.deepEqual(JOB_DEFINITIONS[J], { script: 'apps/company-db/publish/amazon-map.mjs', args: ['--daily'], timeoutMs: 300000 });
  assert.ok(fs.existsSync(path.join(root, JOB_DEFINITIONS[J].script)));
  assert.ok(RETRY_ORDER.indexOf(J) >= 0 && RETRY_ORDER.indexOf(J) === RETRY_ORDER.indexOf('f_sales') - 1, '写しは f_sales の直前');
  const iMap = dailySync.indexOf("runScript('apps/company-db/publish/amazon-map.mjs --daily'");
  const iApply = dailySync.indexOf("runScript('apps/company-db/publish/fetch.mjs --verify-apply --daily'");
  const iFsales = dailySync.indexOf("runScript('apps/warehouse/rebuild-f-sales.js'");
  assert.ok(iApply > 0 && iApply < iMap && iMap < iFsales, 'daily-sync: 写しの反映 → Amazon SKU の写し → f_sales の順');
  assert.match(dailySync, /results\.push\(\{ name: 'CompanyDB写し\(Amazon SKU\)', \.\.\.cdbAmazonMapResult/);
});
t('🚨 ⑦-2 PR-A (#1649 Codex R1 High / R2 Medium): 写しが鍵待ち (exit 73) の朝は f_sales・速度・リスト・Render同期 を流さず、retry-state には写しだけを残す (下流は blocked)。ふつうの拒否 (exit 1) は今までどおり流す', () => {
  assert.match(dailySync, /const amazonMapBusy = !cdbAmazonMapResult\.success && cdbAmazonMapResult\.exitCode === 73;/);
  for (const [v, script] of [['fSalesResult', 'rebuild-f-sales.js'], ['velocityResult', 'rebuild-sales-velocity.js'], ['pmlSnapResult', 'build-product-management-snapshot.js']]) {
    assert.match(dailySync, new RegExp(`const ${v} = amazonMapBusy \\? amazonMapBusySkip\\('[a-z_]+'\\) : runScript\\('apps/warehouse/${script.replace(/\./g, '\\.')}'`), v);
  }
  assert.match(dailySync, /return \{ success: false, blocked: true, summary: '⏸️ skipped \(Amazon SKU の写しが別の写しの鍵待ち/);   // blocked = retry-state に載らない (写しの鎖で流す)
  assert.match(dailySync, /\} else if \(amazonMapBusy\) \{\r?\n\s+syncResult = amazonMapBusySkip\('Render同期'\);/);
  assert.ok(dailySync.indexOf("} else if (amazonMapBusy) {") < dailySync.indexOf("} else if (fSalesOk && skuMapOk) {"));
  assert.match(dailySync, /\.filter\(r => RETRYABLE_JOBS\.includes\(r\.name\) && !r\.success && !r\.blocked\)/);   // blocked は retry-state に入らない
  assert.ok(retryable.includes('CompanyDB写し(Amazon SKU)'));
});
t('🚨 ⑦-2 PR-A: 写しをこの回で再試行して鍵待ち (73) → f_sales を作り直さない (Render同期 も見送り) / 鍵待ち以外で失敗 (#1652 Codex R1 Medium) = 古い表のまま f_sales・Render同期 は普通に再試行・写しは残る / 写しが朝に成功していれば f_sales はそのまま走る', () => {
  const started = []; const quiet = () => {};
  const runner = (outcome) => (script, label) => { started.push(label); return outcome[label] ?? { success: true, summary: 'ok' }; };
  const r1 = runRetryRound(['CompanyDB写し(Amazon SKU)', 'f_sales', 'Render同期'], { run: runner({ 'CompanyDB写し(Amazon SKU)': { success: false, summary: '❌ 別の写しが動いている', exitCode: 73 } }), log: quiet });
  assert.deepEqual(started, ['CompanyDB写し(Amazon SKU)']);
  assert.deepEqual(r1.map((x) => [x.name, x.success]), [['CompanyDB写し(Amazon SKU)', false], ['f_sales', false], ['Render同期', false]]);
  assert.equal(r1.find((x) => x.name === 'f_sales').summary, '⏸️ skipped (CompanyDB写し(Amazon SKU) 再失敗)');
  assert.equal(r1.amazonChainPending, true);
  started.length = 0;
  // 鍵待ち以外 (断った・読めない・timeout) = 写しは何も書いていない = f_sales・Render同期 は古い表で普通に再試行。写しだけが残る (鎖の未完)
  const r1b = runRetryRound(['CompanyDB写し(Amazon SKU)', 'f_sales', 'Render同期'], { run: runner({ 'CompanyDB写し(Amazon SKU)': { success: false, summary: '❌ 断った (shrunk)', exitCode: 1 } }), log: quiet });
  assert.deepEqual(started, ['CompanyDB写し(Amazon SKU)', 'f_sales', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  assert.deepEqual(r1b.filter((x) => !x.success).map((x) => x.name), ['CompanyDB写し(Amazon SKU)']);
  assert.equal(r1b.amazonChainPending, true);
  started.length = 0;
  runRetryRound(['f_sales'], { run: runner({}), log: quiet });   // 写しは朝に成功 (remaining に無い) = f_sales はそのまま・今までどおり (鎖でない = 速度・リストを走らせ直さない)
  assert.deepEqual(started, ['f_sales']);
});
t('定義と順序の食い違いが無い (定義にあるのに順序に無いジョブは、retry-state に載っても永久に走らない)', () => {
  assert.deepEqual(Object.keys(JOB_DEFINITIONS).filter((j) => !RETRY_ORDER.includes(j)), []);
  assert.deepEqual(RETRY_ORDER.filter((j) => !Object.hasOwn(JOB_DEFINITIONS, j)), []);
  // 'Amazon finance build' は意図された例外 (--month の引数が動的なので未登録のまま。retry 側が unhandled として通知する = retry-failed-jobs.js のコメント)。それ以外に増えたら気づく
  assert.deepEqual(retryable.filter((j) => !Object.hasOwn(JOB_DEFINITIONS, j)), ['Amazon finance build'], 'daily-sync が retry に載せるのに、retry 側に定義が無いジョブが増えた');
});
t('規則: この回で上流を再試行して失敗 → 下流は見送り / 成功 → 走らせる / 上流をこの回で試していない (朝は成功・前の回で復旧済み) → 走らせる / 表に無いジョブ・継承プロパティの名前は対象外', () => {
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', [{ name: 'Qoo10', success: false }]), 'Qoo10 再失敗');
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', [{ name: 'Qoo10', success: true }]), null);
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', [{ name: '楽天未発送アラート', success: false }]), null);
  assert.equal(upstreamBlock('CompanyDB注文(Qoo10)', []), null);
  assert.equal(upstreamBlock('CompanyDB注文(楽天)', [{ name: 'Qoo10', success: false }]), null);
  assert.equal(upstreamBlock('toString', [{ name: 'Qoo10', success: false }]), null);
});
t('daily-sync: Qoo10 の取込が失敗した朝は、送信を「⏭️ skipped」の失敗として結果に載せる (= retry-state に入る)。取込が retry されないモールの見送りは載せない', () => {
  assert.match(dailySync, /results\.push\(\{ name: 'CompanyDB注文\(Qoo10\)', success: false, summary: '⏭️ skipped \(Qoo10 の取込が失敗/);
  assert.match(dailySync, /results\.push\(\{ name: 'CompanyDB広告費\(Amazon\)', success: false, summary: '⏭️ skipped \(Amazon Ads \(SKU\) の取込が失敗/);   // 広告の取込は retry の対象 (Company DB構想 11 の ②)
  for (const mall of ['楽天', 'Amazon', 'auPAY', 'LINEギフト']) assert.equal(new RegExp(`results\\.push\\(\\{ name: 'CompanyDB注文\\(${mall}\\)', success: false`).test(dailySync), false, `${mall} の見送りを retry に載せている (取込が retry されないので、失敗したままの raw を送ってしまう)`);
});

t('🚨 実際の実行ループ (runRetryRound) で規則が効いている: Qoo10 の取込がまた失敗した回は送信を起動しない → 次の回で取込が成功すると、取込 → 送信 の順に起動する → 取込が済んでいれば送信だけ (Codex R1 #2)', () => {
  const started = []; const quiet = () => {};
  const runner = (outcome) => (script, label) => { started.push(label); return outcome[label] ?? { success: true, summary: 'ok' }; };
  const r1 = runRetryRound(['CompanyDB注文(Qoo10)', 'Qoo10'], { run: runner({ Qoo10: { success: false, summary: 'HTTP 500' } }), log: quiet });
  assert.deepEqual(started, ['Qoo10'], '取込が失敗した回に送信を起動した');
  assert.deepEqual(r1.map((x) => [x.name, x.success, x.summary]), [['Qoo10', false, 'HTTP 500'], ['CompanyDB注文(Qoo10)', false, '⏸️ skipped (Qoo10 再失敗)']]);
  started.length = 0;
  const r2 = runRetryRound(r1.filter((x) => !x.success).map((x) => x.name), { run: runner({}), log: quiet });
  assert.deepEqual(started, ['Qoo10', 'CompanyDB注文(Qoo10)']);
  assert.ok(r2.every((x) => x.success));
  started.length = 0;
  runRetryRound(['CompanyDB注文(Qoo10)'], { run: runner({}), log: quiet });   // 朝は取込が成功していて送信だけ失敗した・前の回で取込だけ復旧した
  assert.deepEqual(started, ['CompanyDB注文(Qoo10)']);
  started.length = 0;
  runRetryRound(['CompanyDB注文(楽天)', 'Qoo10'], { run: runner({ Qoo10: { success: false, summary: 'x' } }), log: quiet });   // ほかのモールの送信は Qoo10 の失敗に巻き込まれない
  assert.deepEqual(started, ['Qoo10', 'CompanyDB注文(楽天)']);
});
t('🚨 起動の判定は実体パスで: junction (リンク) 経由のパスで起動しても「直接起動」と判定する (不一致だと main が走らず exit 0 で無言終了する。Codex R1 #1)。別のファイルは直接起動ではない', () => {
  const self = path.join(root, 'apps', 'warehouse', 'retry-failed-jobs.js');
  const selfUrl = pathToFileURL(self).href;
  assert.equal(isDirectRun(self, selfUrl), true);
  assert.equal(isDirectRun(self.toUpperCase(), selfUrl), process.platform === 'win32');   // Windows はパスの大文字小文字を区別しない
  assert.equal(isDirectRun(path.join(root, 'apps', 'warehouse', 'daily-sync.js'), selfUrl), false);
  assert.equal(isDirectRun(undefined, selfUrl), false);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'retry-link-'));
  const link = path.join(dir, 'wh');
  try {
    fs.symlinkSync(path.join(root, 'apps', 'warehouse'), link, 'junction');
    assert.equal(isDirectRun(path.join(link, 'retry-failed-jobs.js'), selfUrl), true, 'リンク経由のパスを直接起動と判定できない');
  } finally {
    // 🚨 後始末は「リンクだけ」を外す (rmdir は junction そのものを消し、先の中身には触らない)。リンクが残っているうちは親を再帰で消さない (先の apps/warehouse を消しに行かせない)
    try { fs.rmdirSync(link); } catch { /* 作る前に落ちた */ }
    if (!fs.existsSync(link)) fs.rmSync(dir, { recursive: true, force: true });
  }
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
