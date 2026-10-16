/**
 * test-daily-sync-step-order.mjs — 毎朝の daily-sync の段の順 (2026-10-11 中原さん Q1 = a)
 *
 * 固定する契約:
 *   1 「マスタ照合」は「Render同期」のすぐ後ろ (あいだに別の段を挟まない)。照合は全部の push と Render同期 より後
 *   2 「新商品の許可」は照合の直後 (skipAfterCompare で照合の結果を見る)・照合より前に許可を出さない
 *   3 「DBバックアップ」は main の最後の段 (後ろに runScript が無い)。中身 (スクリプト・timeout 6h) は前のまま
 *   4 照合 → ロジザードの影 → 見張り → DBバックアップ の順 (影は照合の全件 JSON を使う・見張りの W13 は照合の証跡を読む)
 *   5 かかった秒 (endTime) と retry-state の名前の集め (retryableFailed) はバックアップの後 = 全部の段の結果を数える
 *   6 retry の順 (RETRY_ORDER) と矛盾しない: Render同期 → マスタ照合 → 新商品の許可 → 影 → 見張り の相対の順が daily と同じ・RERUN_AFTER の決まりが通る
 *   7 台帳 (warehouse-daily-sync) の説明が新しい置き場所を言う
 *   8 新商品と要確認のまとめ (Company DB構想 22 の Q-4) = 新商品の許可の後・影と見張りの前の 1 段・再試行に載せない・台帳に書いてある
 * 使い方: node scripts/test-daily-sync-step-order.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const iMain = ds.indexOf('\nasync function main() {');
const iMainEnd = ds.indexOf('\nmain().catch(');
assert.ok(iMain > 0 && iMainEnd > iMain, 'main が見つからない');
const body = ds.slice(iMain, iMainEnd);
/** main の中の runScript の呼び出し (出てきた順) = [{ file, at }] */
const calls = [...body.matchAll(/runScript\(\s*(?:'([^']+)'|`([^`]+)`)/g)].map((m) => ({ file: (m[1] || m[2]).split(' ')[0], at: m.index }));
const first = (f) => { const c = calls.find((x) => x.file === f); assert.ok(c, `runScript が無い: ${f}`); return c.at; };
const count = (f) => calls.filter((x) => x.file === f).length;

const COMPARE = 'apps/company-db/master-compare/run.mjs';
const GATE = 'apps/company-db/master-compare/new-entry-gate.mjs';
const LZ = 'scripts/company-db/lz-daily.mjs';
const WATCH = 'apps/company-db/watch/run.mjs';
const BACKUP = 'apps/warehouse/backup-warehouse.js';
const RENDER_PUSH = "results.push({ name: 'Render同期', ...syncResult });";

await ta('[1] マスタ照合は Render同期 のすぐ後ろ (あいだに段が無い)・全部の push と Render同期 より後', async () => {
  const iRender = body.indexOf(RENDER_PUSH);
  assert.ok(iRender > 0);
  assert.equal(body.indexOf(RENDER_PUSH, iRender + 1), -1, 'Render同期 の結果を積むのは 1 か所');
  const iCompare = first(COMPARE);
  assert.ok(iCompare > iRender, '照合が Render同期 より前');
  const between = body.slice(iRender + RENDER_PUSH.length, iCompare);
  assert.doesNotMatch(between, /runScript\(|results\.push\(|await /, `Render同期 と照合のあいだに別の段がある:\n${between.slice(0, 300)}`);
  assert.equal(count(COMPARE), 1);
  // Render へ送る段 (sync-to-render) と Company DB への push は全部 照合より前
  for (const c of calls.filter((x) => x.file === 'apps/warehouse/sync-to-render.js' || x.file.startsWith('apps/company-db/push/') || x.file.startsWith('apps/company-db/publish/'))) {
    assert.ok(c.at < iCompare, `${c.file} が照合より後`);
  }
});

await ta('[2] 新商品の許可は照合の直後 (skipAfterCompare で守る)', async () => {
  const iCompare = first(COMPARE), iGate = first(GATE);
  const iComparePush = body.indexOf("results.push({ name: 'マスタ照合', ...masterCompareResult");
  const iGuard = body.indexOf('const newEntryGateSkip = skipAfterCompare(masterCompareResult);');
  assert.ok(iCompare < iComparePush && iComparePush < iGuard && iGuard < iGate, [iCompare, iComparePush, iGuard, iGate].join(','));
  const between = body.slice(iComparePush, iGuard);
  assert.doesNotMatch(between.slice(between.indexOf('\n')), /runScript\(|results\.push\(/, '照合と許可のあいだに別の段がある');
  // 照合と許可のあいだに他の runScript が無い
  assert.deepEqual(calls.filter((c) => c.at > iCompare && c.at < iGate).map((c) => c.file), []);
  assert.equal(count(GATE), 1);
});

await ta('[3] DBバックアップは main の最後の段 (後ろに runScript が無い)・中身は前のまま', async () => {
  assert.equal(count(BACKUP), 1);
  const iBackup = first(BACKUP);
  assert.equal(calls[calls.length - 1].file, BACKUP, `最後の段が ${calls[calls.length - 1].file}`);
  assert.doesNotMatch(body.slice(iBackup + 1), /runScript\(/);
  assert.match(body, /const backupResult = runScript\('apps\/warehouse\/backup-warehouse\.js', 'DBバックアップ', 21600000\);\n\s*results\.push\(\{ name: 'DBバックアップ', \.\.\.backupResult \}\);/);
});

await ta('[4] 照合 → 許可 → ロジザードの影 → 見張り → DBバックアップ', async () => {
  const order = [COMPARE, GATE, LZ, WATCH, BACKUP].map(first);
  for (let i = 1; i < order.length; i++) assert.ok(order[i - 1] < order[i], `${[COMPARE, GATE, LZ, WATCH, BACKUP][i - 1]} が ${[COMPARE, GATE, LZ, WATCH, BACKUP][i]} より後`);
  assert.equal(count(LZ), 1);
  assert.equal(count(WATCH), 1);
});

await ta('[5] かかった秒と retry-state の名前の集めはバックアップの後 (全部の段の結果を数える)', async () => {
  const iBackupPush = body.indexOf("results.push({ name: 'DBバックアップ', ...backupResult });");
  const iWatchPush = body.indexOf("results.push({ name: 'CompanyDB見張り', ...watchResult");
  assert.ok(iBackupPush > 0 && iWatchPush > 0);
  assert.ok(body.indexOf('const endTime = new Date();') > iBackupPush, 'endTime がバックアップより前');
  const iRetry = body.indexOf('const retryableFailed = gateRetryJobs(results');
  assert.ok(iRetry > iBackupPush && iRetry > iWatchPush, 'retry-state の名前の集めが最後の段より前');
  assert.ok(body.indexOf('const notifyOk = await notify(notifyMsg);') > iRetry);
});

await ta('[6] retry の順と矛盾しない (Render同期 → 照合 → 許可 → 影 → 見張り)・RERUN_AFTER の決まり', async () => {
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  assert.deepEqual(R.rerunAfterProblems(), []);
  const names = ['Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り'];
  const inRetry = names.map((n) => R.RETRY_ORDER.indexOf(n));
  assert.ok(inRetry.every((i) => i >= 0), inRetry.join(','));
  for (let i = 1; i < inRetry.length; i++) assert.ok(inRetry[i - 1] < inRetry[i], `RETRY_ORDER で ${names[i - 1]} が ${names[i]} より後`);
  const inDaily = [body.indexOf(RENDER_PUSH), first(COMPARE), first(GATE), first(LZ), first(WATCH)];
  for (let i = 1; i < inDaily.length; i++) assert.ok(inDaily[i - 1] < inDaily[i], `daily で ${names[i - 1]} が ${names[i]} より後`);
  assert.deepEqual(R.RERUN_AFTER['Render同期'], ['マスタ照合']);
  assert.ok(R.RERUN_AFTER['マスタ照合'].includes('新商品の許可'));
  // 走らせ直す側の名前 = daily の結果の名前 (retry-state に載る名前と同じ)
  for (const n of ['マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り', 'DBバックアップ', 'Render同期']) {
    assert.ok(Object.hasOwn(R.JOB_DEFINITIONS, n), n);
    assert.ok(new RegExp(`const RETRYABLE_JOBS = \\[[^\\]]*'${n.replace(/[()]/g, '\\$&')}'`).test(ds), `RETRYABLE_JOBS に無い: ${n}`);
  }
});

await ta('[7] 台帳 (warehouse-daily-sync) の説明が新しい置き場所を言う', async () => {
  const reg = fs.readFileSync(path.join(ROOT, 'config/jobs-registry.mjs'), 'utf8');
  const entry = reg.slice(reg.indexOf("id: 'warehouse-daily-sync'"), reg.indexOf("id: 'mf-daily-sync'"));
  assert.ok(entry.includes('「Render同期」の直後 (2026-10-11 から。DBバックアップ = daily-sync の一番最後の段を待たない)・見張りの前に「マスタ照合」'), '台帳に新しい置き場所の説明が無い');
  assert.ok(!entry.includes("'全部の push の後・見張りの前に「マスタ照合」"), '台帳に前の置き場所の説明が残っている');
});

await ta('[8] 新商品と要確認のまとめ (Company DB構想 22 の Q-4) = 新商品の許可の後・ロジザードの影と見張りの前の 1 段 (--daily)・再試行に載せない・「読めない」の朝だけ warn・台帳に書いてある', async () => {
  const SUMMARY = 'scripts/company-db/master-register-summary.mjs';
  assert.equal(count(SUMMARY), 1);
  const iSum = first(SUMMARY);
  assert.ok(first(GATE) < iSum && iSum < first(LZ) && iSum < first(WATCH), '置き場所が違う');
  assert.match(body, /const registerSummaryRaw = runScript\('scripts\/company-db\/master-register-summary\.mjs --daily', REGISTER_SUMMARY_STEP, 120000\)/);
  // 親の 120 秒で殺された・子が落ちた = ⚠️ 読めない の成功に変える (❌ にしない。Codex #1717 R1 High)
  assert.match(body, /const registerSummaryResult = registerSummaryRaw\.success \|\| registerSummaryRaw\.gated \? registerSummaryRaw\n\s+: \{ success: true, summary: `⚠️ 新商品と要確認のまとめ: 読めない \(/);
  assert.match(body, /results\.push\(\{ name: REGISTER_SUMMARY_STEP, \.\.\.registerSummaryResult, warn: registerSummaryResult\.success && \/読めない\/\.test\(/);
  assert.match(ds, /\nconst REGISTER_SUMMARY_STEP = '新商品と要確認のまとめ';\n/);
  assert.doesNotMatch(ds.slice(ds.indexOf('const RETRYABLE_JOBS = ['), ds.indexOf('];', ds.indexOf('const RETRYABLE_JOBS = ['))), /新商品と要確認のまとめ/);
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  assert.ok(!Object.hasOwn(R.JOB_DEFINITIONS, '新商品と要確認のまとめ') && !R.RETRY_ORDER.includes('新商品と要確認のまとめ'));
  const reg = fs.readFileSync(path.join(ROOT, 'config/jobs-registry.mjs'), 'utf8');
  const entry = reg.slice(reg.indexOf("id: 'warehouse-daily-sync'"), reg.indexOf("id: 'mf-daily-sync'"));
  assert.ok(entry.includes('「新商品の許可」の後に「新商品と要確認のまとめ」(scripts/company-db/master-register-summary.mjs --daily。新しい定期実行ではない・この台帳の ping に乗る)'), '台帳に段の説明が無い');
  assert.ok(entry.includes('「新商品と要確認のまとめ」が ⚠️ 読めない'), '台帳の runbook に無い');
});

await ta('[9] NE の商品コードの書き方 (10/16 中原さん) = ロジザードの影の直後にこの回の報告を読む (段ではない = results に足さない・新しい runScript なし)・まとめの最後 (ディスクの知らせの後・切り詰めの前) に足す・台帳に書いてある', async () => {
  const iLz = first(LZ);
  const iRead = body.indexOf('const lzCodeNotice = readLzCodeNotice({');
  assert.ok(iRead > iLz && iRead < first(WATCH), 'ロジザードの影の後・見張りの前に読む');
  assert.ok(body.slice(iLz - 200, iLz).includes('const lzDailyAsOf = toJstDate(new Date());'), 'as_of は影を流す前の JST の日付');
  assert.match(body, /readLzCodeNotice\(\{ dataDir: process\.env\.DATA_DIR \|\| path\.join\(PROJECT_DIR, 'data'\), asOf: lzDailyAsOf, syncRunId: process\.env\.DAILY_SYNC_RUN_ID,/);
  assert.equal(count('apps/master-decisions/lz-code-notice.mjs'), 0, '段 (runScript) にしない');
  assert.ok(!/results\.push\([^)]*lzCodeNotice/.test(body), 'results に足さない = 見出し・retry は今のまま');
  const iAdd = body.indexOf('if (lzCodeNotice.length > 0) msg += ');
  const iDisk = body.indexOf('for (const w of diskWarnings) msg += ');
  assert.ok(iDisk > 0 && iAdd > iDisk && iAdd < body.indexOf('const GCHAT_MAX = 3900;'), 'まとめの最後・切り詰めの前');
  assert.match(ds, /\nimport \{ readLzCodeNotice \} from '\.\.\/master-decisions\/lz-code-notice\.mjs';\n/);
  const reg = fs.readFileSync(path.join(ROOT, 'config/jobs-registry.mjs'), 'utf8');
  const entry = reg.slice(reg.indexOf("id: 'warehouse-daily-sync'"), reg.indexOf("id: 'mf-daily-sync'"));
  assert.ok(entry.includes('まとめの最後に「⚠️ NE の商品コードの書き方 N 件」') && entry.includes('「⚠️ NE の商品コードの書き方」が出た:'), '台帳に説明と runbook が無い');
});

console.log(`\n${passed} passed${process.exitCode ? ' (失敗あり)' : ''}`);
