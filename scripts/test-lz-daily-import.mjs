/**
 * test-lz-daily-import.mjs — ロジザードの毎日の商品マスタの取込 (影) (マスタ正本切替 ③c-1b-2a)
 *   apps/master-decisions/lz-import-plan.mjs・scripts/logizard-import/lz-daily-import.mjs・tools/logizard-automation/run-nyuka-csv-scheduled.bat
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b 契約 v3」と v2 §1・§3):
 *   1 動くのは JST 00:15〜00:55 の回だけ・1 日 1 回 (08:40 / 11:45 は何もしない)
 *   2 対象 = 前の日の lz-daily の正式な証跡 1 つだけ (daily-sync の回・complete・CSV の sha256 と行数・期限)。前の日を探しに行かない
 *   3 ポータルの取込の状態と、この PC の初期化の印を照合。合わない・届かない = しない
 *   4 直前の書き出しに CSV の全部の商品があり・削除されていない ときだけプレビュー。**実行ボタンは押さない**
 *   5 記録 (shadow.json・pre.csv) は途中の失敗でも残す / bat の 1.5 ステップ目 (終了コードを変えない・ASCII・CRLF) / 台帳
 * 使い方: node scripts/test-lz-daily-import.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

process.env.DAILY_SYNC_RUN_ID = 'ds_test';
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const { default: iconv } = await import('iconv-lite');
const P = await import('../apps/master-decisions/lz-import-plan.mjs');
const D = await import('../apps/master-decisions/lz-cdb.mjs');
const RUN = await import('./logizard-import/lz-daily-import.mjs');
const { writeEvidence } = await import('../apps/company-db/push/evidence.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sj = (s) => iconv.encode(s, 'cp932');
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
// ロジザードの商品マスタの書き出しの実ファイルの 1 行目 (test-lz-daily.mjs と同じバイト)
const REAL_LZ_HEADER_HEX = '228c5f96f18ed24944222c228c5f96f18ed296bc222c2289d78ee54944222c2289d78ee596bc222c228fa495694944222c228fa4956996bc222c228c9f8df596bc8fcc222c228c9f8df596bc8fcc32222c228e6493fc925089bf222c2288f89396957389c293fa9094222c2291e595aa97de222c22928695aa97de222c228fac95aa97de222c22838d836283678ac7979d83748389834f222c22974c8cf88afa8cc08be695aa222c2289b7937891d18be695aa222c228fac948489bf8a69222c22835a836283678d5c90ac8be695aa222c228ded8f9c83748389834f222c22936f985e93fa8e9e222c2295cf8d5893fa8e9e222c2283438393837c815b836793fa8e9e222c228ddd8cc98be695aa222c2293fc89d78afa8cc093fa9094222c2293fc89d793fa8ac7979d83748389834f222c228fa49569975c94f58d8096da824f824f8250222c228fa49569975c94f58d8096da824f824f8251222c228fa49569975c94f58d8096da824f824f8252222c228fa49569975c94f58d8096da824f824f8253222c228fa49569975c94f58d8096da824f824f8254222c228fa49569975c94f58d8096da824f824f8255222c228fa49569975c94f58d8096da824f824f8256222c228fa49569975c94f58d8096da824f824f8257222c228fa49569975c94f58d8096da824f824f8258222c228fa49569975c94f58d8096da824f8250824f222c22959496e54944222c228fac948489bf8a6932222c228fac948489bf8a6933222c228fac948489bf8a6934222c228fac948489bf8a6935222c2289708cea96bc222c228f6497ca222c228356838a8341838b936f985e83748389834f22';
const q = (v) => '"' + String(v).replace(/"/g, '""') + '"';
const lzRow = (o) => { const c = Array(43).fill(''); c[4] = o.id; c[5] = o.name ?? 'x'; c[8] = o.cost ?? '0'; c[18] = o.del ?? '0'; c[27] = o.sup ?? '0001'; return sj(c.map(q).join(',')); };
const lzCsv = (rows) => Buffer.concat([Buffer.from(REAL_LZ_HEADER_HEX, 'hex'), ...rows.flatMap((r) => [Buffer.from('\r\n'), lzRow(r)])]);
const dailyCsv = (ids) => sj(['形式/型番,商品名,ふりがな,仕入単価,取引先id', ...ids.map((id) => `${id},商品${id},商品${id},100,0001`)].join('\r\n'));

// 前の日 (2030-01-15) の朝の lz-daily を置いた DATA_DIR。今 = 2030-01-16 00:20 JST
const NOW = new Date('2030-01-15T15:20:00Z');
const AS_OF = '2030-01-15', RUN_DIR = 'lzd_20300115T001500000Z_abcdef';
function setup({ ids = ['A-1', 'B-2'], state = 'complete', verdict = 'pass', syncRun = true, deadline = '2030-01-16T01:00:00+09:00', csvBuf = null, rowsOverride = null, shaOverride = null, csvPath = null } = {}) {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-'));
  const buf = csvBuf || dailyCsv(ids);
  const rel = csvPath || `lz-daily/${AS_OF}/${RUN_DIR}/cdb_logizard_shohinmaster_upload.csv`;
  fs.mkdirSync(path.join(dataDir, path.dirname(rel)), { recursive: true });
  fs.writeFileSync(path.join(dataDir, rel), buf);
  const prev = process.env.DAILY_SYNC_RUN_ID;
  if (!syncRun) delete process.env.DAILY_SYNC_RUN_ID;
  writeEvidence(dataDir, 'lz-daily', { state, version: 'lzd-v3', portal: { ok: true, stored: true }, as_of: AS_OF, run_id: RUN_DIR, verdict, deadline, csv: { path: rel, sha256: shaOverride || sha(buf), rows: rowsOverride ?? ids.length } }, { now: new Date('2030-01-15T00:20:00Z'), warn: () => {} });
  process.env.DAILY_SYNC_RUN_ID = prev;
  return { dataDir, csvFull: path.join(dataDir, rel) };
}

console.log('test-lz-daily-import');

await ta('[1] 時刻の窓 (JST 00:15〜00:55) と対象の日 (前の日)', async () => {
  const at = (hhmm) => new Date(`2030-01-16T${hhmm}:00+09:00`);
  assert.deepEqual(['00:14', '00:15', '00:20', '00:54', '00:55', '08:40', '11:45'].map((t) => P.inWindow(at(t))), [false, true, true, true, false, false, false]);
  assert.deepEqual([P.targetAsOf(at('00:20')), P.jstDateOf(at('00:20')), P.targetAsOf(new Date('2030-01-01T15:20:00Z'))], ['2030-01-15', '2030-01-16', '2030-01-01']);
});

await ta('[2] 対象: 前の日の正式な証跡だけ (無い・手の回・running / skipped・不合格 (本番)・期限切れ・sha256・行数・場所の形 = しない)', async () => {
  let s = setup();
  let t = P.pickTarget({ dataDir: s.dataDir, now: NOW });
  assert.deepEqual([t.ok, t.asOf, t.csvPath, t.evidence.run_id], [true, AS_OF, s.csvFull, RUN_DIR]);
  const cases = [
    [{}, 'no_evidence', (d) => fs.rmSync(path.join(d, 'company-db-evidence'), { recursive: true })],
    [{ syncRun: false }, 'no_evidence'],   // 手の回 (lz-daily.manual) は見ない
    [{ state: 'running' }, 'not_complete_running'],
    [{ state: 'skipped' }, 'not_complete_skipped'],
    [{ verdict: 'fail' }, 'not_pass'],
    [{ deadline: '2030-01-16T00:10:00+09:00' }, 'deadline_passed'],
    [{ shaOverride: 'f'.repeat(64) }, 'csv_sha256_mismatch'],
    [{ rowsOverride: 5 }, 'csv_rows_mismatch'],
    [{ csvPath: 'elsewhere/x.csv' }, 'csv_path_bad'],
  ];
  for (const [o, reason, mutate] of cases) {
    s = setup(o);
    if (mutate) mutate(s.dataDir);
    t = P.pickTarget({ dataDir: s.dataDir, now: NOW });
    assert.deepEqual([t.ok, t.reason], [false, reason], JSON.stringify(o));
  }
  // lz-daily という名前でも daily-sync の実行 ID が無い = 使わない
  s = setup();
  const evFile = path.join(s.dataDir, 'company-db-evidence', AS_OF, 'lz-daily.json');
  const ev = JSON.parse(fs.readFileSync(evFile, 'utf8'));
  fs.writeFileSync(evFile, JSON.stringify({ ...ev, sync_run_id: null }));
  assert.equal(P.pickTarget({ dataDir: s.dataDir, now: NOW }).reason, 'not_daily_sync');
  fs.writeFileSync(evFile, JSON.stringify({ ...ev, as_of: '2030-01-14' }));
  assert.equal(P.pickTarget({ dataDir: s.dataDir, now: NOW }).reason, 'as_of_mismatch');
  // 影の取込は合否を問わない (記録する)
  s = setup({ verdict: 'fail' });
  assert.equal(P.pickTarget({ dataDir: s.dataDir, now: NOW, requirePass: false }).ok, true);
  // 前の日を探しに行かない: 今日の日付の証跡 (00:20 の時点ではまだ無い) も、2 日前も見ない
  s = setup();
  assert.equal(P.pickTarget({ dataDir: s.dataDir, now: new Date('2030-01-16T15:20:00Z') }).reason, 'no_evidence');
});

await ta('[3] 取り込む前の確かめ: CSV の全部の商品が直前の書き出しにあり・削除されていない', async () => {
  const lz = D.readLzShohinMaster(lzCsv([{ id: 'A-1' }, { id: 'B-2', del: '1' }]), { minRows: 1 });
  assert.deepEqual(P.precheck({ csvBuf: dailyCsv(['A-1']), lz }), { ok: true, rows: 1, missing: [], deleted: [] });
  assert.deepEqual(P.precheck({ csvBuf: dailyCsv(['A-1', 'B-2', 'C-3']), lz }), { ok: false, rows: 3, missing: ['C-3'], deleted: ['B-2'] });
  assert.equal(P.precheck({ csvBuf: dailyCsv([]), lz }).ok, false);
  assert.equal(P.precheck({ csvBuf: dailyCsv(['a-1']), lz }).ok, false);   // 大文字小文字だけ違う = 無い
});

// ── 影の取込 (runShadow) ──
function fakes({ initOk = true, initThrows = false, lzRows = [{ id: 'A-1' }, { id: 'B-2' }], exportThrows = false } = {}) {
  const calls = { exported: 0, preview: [] };
  return {
    calls,
    client: { status: async () => ({ ok: true }) },
    checkInit: async () => { if (initThrows) throw new Error('unreachable'); return initOk ? { ok: true, reason: null, status: { state: 'idle', halted: false } } : { ok: false, reason: '識別子が違う', status: { state: 'idle', halted: false } }; },
    withSession: async (fn) => fn({
      exportShohin: async () => { calls.exported++; if (exportThrows) throw new Error('ダウンロードが始まりません'); return { buf: lzCsv(lzRows) }; },
      previewImport: async (csvPath) => { calls.preview.push(csvPath); return { previewed: true, pattern: 'デイリー取込商品マスタ' }; },
    }),
  };
}
const shadow = (s, f, o = {}) => RUN.runShadow({ dataDir: s.dataDir, now: NOW, localInitFile: path.join(s.dataDir, 'lz-import', 'init.json'), lzMinRows: 1, log: () => {}, enabled: true, ...f, ...o });
const shadowJson = (s, r) => JSON.parse(fs.readFileSync(path.join(s.dataDir, 'lz-import', '2030-01-16', r.runId, 'shadow.json'), 'utf8'));

await ta('[4] 影の取込: 直前の書き出し → 全部ある → プレビュー (CSV は lz-daily のもの) → 記録と済みの印 / 2 回目は何もしない / 窓の外は何もしない', async () => {
  const s = setup();
  const f = fakes();
  let r = await shadow(s, f);
  assert.equal(r.state, 'shadow_ok', r.line);
  assert.deepEqual([f.calls.exported, f.calls.preview], [1, [s.csvFull]]);
  const rec = shadowJson(s, r);
  assert.deepEqual([rec.state, rec.target.lz_daily_run_id, rec.precheck.ok, rec.precheck.rows, rec.pre.rows, rec.portal.init_ok, rec.preview.previewed], ['shadow_ok', RUN_DIR, true, 2, 2, true, true]);
  assert.ok(fs.existsSync(path.join(s.dataDir, 'lz-import', '2030-01-16', r.runId, 'pre.csv')));
  assert.match(r.line, /実行ボタンは押していない/);
  r = await shadow(s, f);
  assert.deepEqual([r.state, f.calls.exported], ['already', 1]);
  r = await shadow(s, f, { now: new Date('2030-01-15T23:40:00Z') });   // 08:40 JST
  assert.equal(r.state, 'outside_window');
  // --force-window = 窓の外でも動く・済みの印を書かない (期限 = 翌日 01:00 は守る)
  const s2 = setup();
  r = await shadow(s2, fakes(), { now: new Date('2030-01-15T15:10:00Z'), forceWindow: true });   // 00:10 JST (窓の前・期限の内)
  assert.equal(r.state, 'shadow_ok');
  assert.equal(fs.existsSync(path.join(s2.dataDir, 'lz-import', '2030-01-16', 'shadow-done.json')), false);
});

await ta('[5] 影の取込: 対象が無い・初期化の印が合わない・ポータルに届かない・CSV の商品が無い / 削除・直前の書き出しが壊れた = しない (プレビューしない・理由を記録)', async () => {
  const cases = [
    [setup({ state: 'skipped' }), fakes(), 'target_not_complete_skipped', 0],
    [setup(), fakes({ initOk: false }), 'init_mismatch', 0],
    [setup(), fakes({ initThrows: true }), 'portal_unreachable', 0],
    [setup({ ids: ['A-1', 'Z-9'] }), fakes(), 'precheck_failed', 1],
    [setup(), fakes({ lzRows: [{ id: 'A-1' }, { id: 'B-2', del: '1' }] }), 'precheck_failed', 1],
  ];
  for (const [s, f, reason, exported] of cases) {
    const r = await shadow(s, f);
    assert.deepEqual([r.state, r.reason, f.calls.exported, f.calls.preview.length], ['skipped', reason, exported, 0], reason);
    assert.equal(shadowJson(s, r).reason, reason);
    assert.equal(fs.existsSync(path.join(s.dataDir, 'lz-import', '2030-01-16', 'shadow-done.json')), false);   // 済みにしない
  }
  // 直前の書き出しが壊れた (見出しが違う)
  const s = setup(), f = fakes();
  f.withSession = async (fn) => fn({ exportShohin: async () => ({ buf: sj('"商品ID"\r\n"A-1"') }), previewImport: async () => { throw new Error('呼ばれない'); } });
  const r = await shadow(s, f);
  assert.deepEqual([r.state, r.reason], ['skipped', 'pre_export_lz_master_header']);
  // ロジザードの操作が途中で失敗 = 記録 (error) を残して投げる
  const s3 = setup(), f3 = fakes({ exportThrows: true });
  await assert.rejects(shadow(s3, f3), /ダウンロードが始まりません/);
  const dayDir = path.join(s3.dataDir, 'lz-import', '2030-01-16');
  const runDir = fs.readdirSync(dayDir).find((d) => d.startsWith('lzsh_'));
  const rec = JSON.parse(fs.readFileSync(path.join(dayDir, runDir, 'shadow.json'), 'utf8'));
  assert.deepEqual([rec.state, /ダウンロード/.test(rec.error)], ['error', true]);
});

await ta('[6] 影の取込は実行ボタンを押さない (押すのは画面の部品の executeImport の 1 か所だけ・影のランナーは呼ばない)・bat の 1.5 ステップ目 (終了コードを変えない・ASCII・CRLF)・台帳・写すファイル', async () => {
  const screen = fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'lz-import-screen.js'), 'utf8');
  // 実行ボタンを押すのは executeImport の中の 1 か所だけ (③c-1b-2b-1b)。previewImport の中には無い・影のランナーは executeImport を呼ばない
  const presses = [...screen.matchAll(/clickExecuteInPage\(page[,)]/g)].map((m) => m.index);
  const ex = screen.indexOf('export async function executeImport'), pv = screen.indexOf('export async function previewImport');
  assert.equal(presses.length, 1, '実行ボタンを押すのは 1 か所');
  assert.ok(pv > 0 && ex > pv && presses[0] > ex, 'その 1 つは executeImport の中 (previewImport の中には無い)');
  // Playwright の click (押す前に待つ) は機能バーを開く 1 か所だけ = 取込を始めるボタン (実行・確認の OK) はページの中の 1 回の処理で押す
  const pwClicks = [...screen.matchAll(/page\.click\(([^)]*)\)/g)].map((m) => m[1]);
  assert.deepEqual(pwClicks.map((a) => /openFunctionBar\('FM07_01'/.test(a)), [true], 'page.click は機能バーを開く所だけ');
  assert.ok(!/\.click\(\{ ?timeout/.test(screen), 'locator の click (待つ) を使わない');
  assert.equal([...screen.matchAll(/'#FM07_01_executeBtn'/g)].length, 3, '実行ボタンに触るのは プレビューの待ち・押せる状態の確かめ・ページの中で押す処理 だけ');
  const shadowRunner = fs.readFileSync(path.join(ROOT, 'scripts', 'logizard-import', 'lz-daily-import.mjs'), 'utf8');
  assert.ok(!/executeImport|clickExecuteInPage|onExecuteIssued/.test(shadowRunner), '影のランナーは押す部品を呼ばない');
  assert.ok(screen.includes("export const DAILY_PATTERN_LABEL = 'デイリー取込商品マスタ';"));
  const bat = fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'run-nyuka-csv-scheduled.bat'));
  assert.ok(!/[^\x00-\x7f]/.test(bat.toString('latin1')), 'ASCII だけ');
  assert.ok(!/(?<!\r)\n/.test(bat.toString('latin1')), 'CRLF');
  const b = bat.toString('latin1');
  const iNyuka = b.indexOf('node auto-nyuka-csv.js'), iImp = b.indexOf('node C:\\Users\\bfaith\\bfaith-portal\\scripts\\logizard-import\\lz-daily-import.mjs >> logs\\scheduled.log 2>&1'), iShohin = b.indexOf('node auto-shohin-csv.js --once-per-day');
  assert.ok(iNyuka > 0 && iImp > iNyuka && iShohin > iImp, '入荷受付 → 取込 (影) → 商品マスタ の順');
  // 影のステップの直前に DATA_DIR (夜の定時のタスクは DATA_DIR を持たない = 2026-09-29 00:21 に「DATA_DIR が無い」で失敗)
  const iData = b.indexOf('set "DATA_DIR=C:\\Users\\bfaith\\bfaith-portal\\data"'), iEcho = b.indexOf('==== lz-daily-import (shadow');
  assert.ok(iEcho > 0 && iData > iEcho && iData < iImp, 'DATA_DIR は影のステップの直前');
  assert.ok(!/set "RC=/.test(b.slice(iImp, iShohin)), '取込の終了コードで bat の RC を変えない');
  assert.match(b.slice(b.lastIndexOf('exit /b')), /exit \/b %RC%/);
  const { JOBS_REGISTRY, validateRegistry } = await import('../config/jobs-registry.mjs');
  const job = JOBS_REGISTRY.find((e) => e.id === RUN.JOB_SHADOW), retire = JOBS_REGISTRY.find((e) => e.id === 'lz-daily-import-shadow-retire');
  assert.deepEqual([job.type, job.anchor_hour_jst, job.anchor_minute_jst, retire.type, validateRegistry([job, retire])], ['scheduled_job', 0, 20, 'temporary_asset', []]);
  const m = JSON.parse(fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'manifest.json'), 'utf8'));
  for (const pc of ['minipc', 'streamdeck']) assert.ok(m.pcs[pc].includes('lz-import-screen.js'), pc);
});

await ta('[7] CLI: LZ_DAILY_IMPORT=on = 毎晩の本番 = 確かめの列の決まりが無いうち (2b-2a) は ❌ で何もしない (ファイル・ポータル・ロジザード・知らせ = 0・fail の ping は影の項目に 1 回)・送り先が無い = ❌・--force-window は影だけ・DATA_DIR が無い = ❌', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-cli-'));
  const cli = (env, args = ['--force-window']) => spawnSync(process.execPath, ['scripts/logizard-import/lz-daily-import.mjs', ...args], { cwd: ROOT, encoding: 'utf8', env: { ...process.env, JOBS_MONITOR_TOKEN: '', ...env } });
  // 送った先を数える (ポータル・GChat・ping = fetch を全部ファイルに)
  const spyDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-spy-'));
  const spy = path.join(spyDir, 'spy.mjs'), log = path.join(spyDir, 'fetch.txt');
  fs.writeFileSync(spy, "import fs from 'node:fs';\nglobalThis.fetch = async (url) => { fs.appendFileSync(process.env.FETCH_LOG, String(url) + '\\n'); return new Response('{}', { status: 200 }); };\n");
  const { pathToFileURL } = await import('node:url');
  const spied = (env) => spawnSync(process.execPath, ['--import', pathToFileURL(spy).href, 'scripts/logizard-import/lz-daily-import.mjs'], { cwd: ROOT, encoding: 'utf8',
    env: { ...process.env, FETCH_LOG: log, JOBS_MONITOR_TOKEN: 'dummy-token', JOBS_MONITOR_URL: 'https://jobs.example.test', LZ_LOCK_TOKEN: 'tok', LZ_IMPORT_STATE_URL: 'https://portal.example.test', ...env } });
  const fetched = () => { try { return fs.readFileSync(log, 'utf8').trim().split('\n').filter(Boolean); } catch { return []; } };
  let c = spied({ DATA_DIR: tmp, LZ_DAILY_IMPORT: 'on', GCHAT_WEBHOOK_JOBS: 'https://chat.example.test/hook' });
  assert.equal(c.status, 1, c.stdout + c.stderr);
  assert.match(c.stdout.trim().split('\n').pop(), /^❌ ロジザード毎日の商品マスタの取込 \(毎晩\): .*確かめの列の決まりがまだ無い.*何もしない/);
  assert.deepEqual(fs.readdirSync(tmp), [], 'ファイルを作らない');
  const f1 = fetched();
  assert.equal(f1.length, 1, 'ポータル・GChat には行かない (ping の 1 回だけ): ' + f1.join(' '));
  assert.match(f1[0], /^https:\/\/jobs\.example\.test\/apps\/jobs-monitor\/ping\/lz-daily-import-shadow\?status=fail&note=/, '早すぎる on = 影の項目に fail');
  // 送り先 (GCHAT_WEBHOOK_JOBS) が無い = 決まりより先に ❌ (ログインの前)
  c = spied({ DATA_DIR: tmp, LZ_DAILY_IMPORT: 'on', GCHAT_WEBHOOK_JOBS: '' });
  assert.equal(c.status, 1);
  assert.match(c.stdout.trim().split('\n').pop(), /^❌ ロジザード毎日の商品マスタの取込 \(毎晩\): 要対応スペースの送り先 GCHAT_WEBHOOK_JOBS が/);
  // 手の試しの引数は影だけ・on の途中の失敗の fail は毎晩の項目 lz-daily-import へ (影の項目ではない。Codex #1547 R1 Medium)
  fs.rmSync(log, { force: true });
  c = spawnSync(process.execPath, ['--import', pathToFileURL(spy).href, 'scripts/logizard-import/lz-daily-import.mjs', '--force-window'], { cwd: ROOT, encoding: 'utf8',
    env: { ...process.env, FETCH_LOG: log, JOBS_MONITOR_TOKEN: 'dummy-token', JOBS_MONITOR_URL: 'https://jobs.example.test', DATA_DIR: tmp, LZ_DAILY_IMPORT: 'on', GCHAT_WEBHOOK_JOBS: 'https://chat.example.test/hook' } });
  assert.equal(c.status, 1);
  assert.match(c.stdout.trim().split('\n').pop(), /--force-window \/ --as-of は影の手の試しだけ/);
  assert.deepEqual(fetched().map((u) => u.replace(/\?.*/, '')), ['https://jobs.example.test/apps/jobs-monitor/ping/lz-daily-import'], '毎晩の項目に fail');
  assert.deepEqual(fs.readdirSync(tmp), []);
  c = cli({ DATA_DIR: '' });
  assert.equal(c.status, 1);
  assert.equal(fs.readdirSync(tmp).length, 0);
});

await ta('[8] 毎晩の影は on のときだけ (既定 = 止めてある)・手の試しは昼でも対象の日を選べる (期限の内) / --as-of は手の試しだけ (Codex #1516 R1)', async () => {
  let s = setup(), f = fakes();
  let r = await shadow(s, f, { enabled: false });
  assert.deepEqual([r.state, f.calls.exported], ['disabled', 0]);
  assert.equal(fs.existsSync(path.join(s.dataDir, 'lz-import')), false);
  // 昼 (2030-01-16 13:00 JST) に、その朝 (2030-01-16) の lz-daily で手の試し。定時と同じ「前の日」だと 2030-01-15 = 期限切れ
  const noon = new Date('2030-01-16T04:00:00Z');
  const d2 = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-'));
  const rel = `lz-daily/2030-01-16/lzd_20300116T001500000Z_abcdef/cdb_logizard_shohinmaster_upload.csv`, buf = dailyCsv(['A-1']);
  fs.mkdirSync(path.join(d2, path.dirname(rel)), { recursive: true }); fs.writeFileSync(path.join(d2, rel), buf);
  writeEvidence(d2, 'lz-daily', { state: 'complete', version: 'lzd-v3', portal: { ok: true, stored: true }, as_of: '2030-01-16', run_id: 'lzd_20300116T001500000Z_abcdef', verdict: 'pass', deadline: '2030-01-17T01:00:00+09:00', csv: { path: rel, sha256: sha(buf), rows: 1 } }, { now: new Date('2030-01-16T00:20:00Z'), warn: () => {} });
  f = fakes();
  r = await RUN.runShadow({ dataDir: d2, now: noon, forceWindow: true, asOf: '2030-01-16', localInitFile: 'x', lzMinRows: 1, log: () => {}, ...f });
  assert.deepEqual([r.state, r.record.target.as_of, f.calls.preview.length], ['shadow_ok', '2030-01-16', 1]);
  f = fakes();
  r = await RUN.runShadow({ dataDir: d2, now: noon, forceWindow: true, localInitFile: 'x', lzMinRows: 1, log: () => {}, ...f });
  assert.deepEqual([r.state, r.reason], ['skipped', 'target_no_evidence']);   // 指定しなければ前の日 (2030-01-15) = 無い
  await assert.rejects(RUN.runShadow({ dataDir: d2, now: NOW, asOf: '2030-01-16', enabled: true, localInitFile: 'x', ...fakes() }), /--force-window/);
  assert.throws(() => RUN.parseArgs(['--as-of', '2030-01-16']), /--force-window/);
  assert.throws(() => RUN.parseArgs(['--force-window', '--as-of', '2030/01/16']), /YYYY-MM-DD/);
  assert.deepEqual(RUN.parseArgs(['--force-window', '--as-of', '2030-01-16']).asOf, '2030-01-16');
});

await ta('[9] 本物のロジザードの操作の包み: ブラウザの起動・ログインに失敗しても鍵を返す・ID とパスワードが無ければ鍵を取る前に止める / 共通部品が process.exit しても鍵を返す (Codex #1516 R1・R2)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-auto-'));
  const common = (launchThrows, loginThrows, creds = true) => `export const calls = globalThis.__lzcalls = [];
export function loadEnv() { calls.push('loadEnv'); ${creds ? "process.env.LOGIZARD_USER_ID = 'u'; process.env.LOGIZARD_PASSWORD = 'p';" : "delete process.env.LOGIZARD_USER_ID; delete process.env.LOGIZARD_PASSWORD;"} }
export function assertLocalWriteDirs() {}
export function acquireLock() { calls.push('acquire'); }
export function releaseLock() { calls.push('release'); }
export async function launchBrowser() { calls.push('launch'); ${launchThrows ? "throw new Error('Chrome が起動しない');" : ''} return { browser: { close: async () => calls.push('close') }, page: {} }; }
export async function login() { calls.push('login'); ${loginThrows ? "throw new Error('ログイン失敗');" : ''} }
`;
  fs.writeFileSync(path.join(dir, 'shohin-export.js'), 'export async function exportShohinMaster() { return { buf: Buffer.alloc(0) }; }\n');
  fs.writeFileSync(path.join(dir, 'lz-import-screen.js'), 'export async function previewImport() { return {}; }\n');
  for (const [launchThrows, loginThrows, want, creds = true] of [[true, false, ['loadEnv', 'acquire', 'launch', 'release']], [false, true, ['loadEnv', 'acquire', 'launch', 'login', 'close', 'release']], [false, false, ['loadEnv', 'acquire', 'launch', 'login', 'close', 'release']], [false, false, ['loadEnv'], false]]) {
    const sub = fs.mkdtempSync(path.join(dir, 'v-'));
    fs.writeFileSync(path.join(sub, 'logizard-common.js'), common(launchThrows, loginThrows, creds));
    fs.copyFileSync(path.join(dir, 'shohin-export.js'), path.join(sub, 'shohin-export.js'));
    fs.copyFileSync(path.join(dir, 'lz-import-screen.js'), path.join(sub, 'lz-import-screen.js'));
    const run = RUN.realWithSession({ automationDir: sub })(async () => 'done');
    if (launchThrows || loginThrows || !creds) await assert.rejects(run); else assert.equal(await run, 'done');
    assert.deepEqual(globalThis.__lzcalls, want, JSON.stringify({ launchThrows, loginThrows, creds }));
  }
  assert.equal(process.listenerCount('exit') >= 0, true);
  // 共通部品の launchBrowser が process.exit(1) (Chrome が無い) = finally は通らないが、鍵は返る (子プロセスで)
  const sub = fs.mkdtempSync(path.join(dir, 'x-'));
  const marker = path.join(sub, 'released.txt');
  fs.writeFileSync(path.join(sub, 'logizard-common.js'), `import fs from 'node:fs';
export function loadEnv() { process.env.LOGIZARD_USER_ID = 'u'; process.env.LOGIZARD_PASSWORD = 'p'; }
export function assertLocalWriteDirs() {}
export function acquireLock() {}
export function releaseLock() { fs.appendFileSync(${JSON.stringify(marker)}, 'released\\n'); }
export async function launchBrowser() { process.exit(1); }
export async function login() {}
`);
  fs.copyFileSync(path.join(dir, 'shohin-export.js'), path.join(sub, 'shohin-export.js'));
  fs.copyFileSync(path.join(dir, 'lz-import-screen.js'), path.join(sub, 'lz-import-screen.js'));
  const runner = path.join(sub, 'run.mjs');
  fs.writeFileSync(runner, `import { realWithSession } from ${JSON.stringify(new URL('./logizard-import/lz-daily-import.mjs', import.meta.url).href)};
await realWithSession({ automationDir: ${JSON.stringify(sub)} })(async () => 'never');
`);
  const c = spawnSync(process.execPath, [runner], { encoding: 'utf8' });
  assert.equal(c.status, 1, c.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'released\n');   // exit の処理で 1 回だけ返す
});

await ta('[9b] 止めてある間・窓の外はポータルの呼び手を作らない (token が無くても ok で終われる) / 作れない = 理由つきでしない (Codex #1516 R2)', async () => {
  let made = 0;
  const factory = () => { made++; const e = new Error('LZ_LOCK_TOKEN が無い'); e.code = 'no_token'; throw e; };
  let s = setup();
  let r = await shadow(s, { ...fakes(), client: factory }, { enabled: false });
  assert.deepEqual([r.state, made], ['disabled', 0]);
  r = await shadow(s, { ...fakes(), client: factory }, { now: new Date('2030-01-15T23:40:00Z') });
  assert.deepEqual([r.state, made], ['outside_window', 0]);
  r = await shadow(s, { ...fakes(), client: factory, checkInit: async (c) => ({ ok: true, status: c.status() }) });
  assert.deepEqual([r.state, r.reason, made], ['skipped', 'portal_no_token', 1]);
  // CLI: token も有効化も無いまま、定時の形 (--force-window 無し) で流す = 窓の外 か 止めてある で exit 0 (❌ にしない)
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-cli-'));
  const c = spawnSync(process.execPath, ['scripts/logizard-import/lz-daily-import.mjs'], { cwd: ROOT, encoding: 'utf8', env: { ...process.env, DATA_DIR: tmp, LZ_LOCK_TOKEN: '', LZ_DAILY_IMPORT_SHADOW: '', JOBS_MONITOR_TOKEN: '' } });
  assert.equal(c.status, 0, c.stdout + c.stderr);
  assert.match(c.stdout.trim().split('\n').pop(), /^ℹ ロジザード毎日の商品マスタの取込 \(影\): (時刻の窓の外|止めてある)/);
});

await ta('[9c] ロジザードの操作の途中で process.exit しても shadow.json に途中で止まったと残す・ふつうに終わったら exit の処理を外す (Codex #1516 R3 Low)', async () => {
  const s = setup();
  const before = process.listenerCount('exit');
  const r = await shadow(s, fakes());
  assert.deepEqual([r.state, process.listenerCount('exit')], ['shadow_ok', before]);
  await assert.rejects(shadow(setup(), fakes({ exportThrows: true })));
  assert.equal(process.listenerCount('exit'), before);
  // 子プロセスで本当に process.exit(1)
  const s2 = setup();
  const runner = path.join(s2.dataDir, 'run.mjs');
  fs.writeFileSync(runner, `import { runShadow } from ${JSON.stringify(new URL('./logizard-import/lz-daily-import.mjs', import.meta.url).href)};
await runShadow({ dataDir: ${JSON.stringify(s2.dataDir)}, now: new Date(${JSON.stringify(NOW.toISOString())}), enabled: true, lzMinRows: 1, log: () => {},
  localInitFile: 'unused', client: { status: async () => ({}) }, checkInit: async () => ({ ok: true, reason: null, status: { state: 'idle', halted: false } }),
  withSession: async () => { process.exit(1); } });
`);
  const c = spawnSync(process.execPath, [runner], { encoding: 'utf8' });
  assert.equal(c.status, 1, c.stderr);
  const dayDir = path.join(s2.dataDir, 'lz-import', '2030-01-16');
  const runs = fs.readdirSync(dayDir).filter((d) => d.startsWith('lzsh_'));
  assert.equal(runs.length, 1);
  const rec = JSON.parse(fs.readFileSync(path.join(dayDir, runs[0], 'shadow.json'), 'utf8'));
  assert.deepEqual([rec.state, rec.target.ok, rec.portal.init_ok], ['error', true, true]);
  assert.ok(rec.error.startsWith('途中で process.exit(1) '), rec.error);
  assert.equal(fs.existsSync(path.join(dayDir, 'shadow-done.json')), false);   // 済みの印は書かない
});

await ta('[10] 画面: 本物のブラウザと模擬の画面で、プレビューができた (処理の開始・画面の変化・ファイル名) ときだけ成功・実行ボタンは押さない (Codex #1516 R1)', async () => {
  let chromium;
  try { ({ chromium } = await import('playwright')); } catch { console.log('      (playwright が無い = この試験はとばす)'); passed--; return; }
  const http = await import('node:http');
  const shotDir = path.join(ROOT, 'tools', 'logizard-automation', 'error-shots');
  const shotExisted = fs.existsSync(shotDir);
  // 模擬の PM07: variant = ok (処理中 → 表を描く) / none (何も起きない) / late (3 秒後に始まる) / modal (エラーのモーダル)
  const page = (variant) => `<!doctype html><html><body>
<a onclick="openFunctionBar('FM07_01')">imp</a>
<div id="FM07_01_FORM"><select id="FM07_01_fileId"><option value="">-</option><option value="9">商品マスタ</option></select>
<select id="FM07_01_ptrnId"><option value="">-</option><option value="3">デイリー取込商品マスタ</option></select>
<input type="file" id="FM07_01_impFile"><input type="button" id="FM07_01_executeBtn" value="実行" onclick="window.__executed=true"><div id="pv"></div></div>
<div class="blockUI blockOverlay" id="busy" style="display:none;position:fixed;inset:0;background:#0003"></div>
<div id="popup_overlay" style="display:none;position:fixed;inset:0"><div class="ui-dialog">エラーが発生しました (取込できません)</div></div>
<script>
document.getElementById('FM07_01_impFile').addEventListener('change', () => {
  const v = ${JSON.stringify(variant)};
  if (v === 'none') return;
  const go = () => { document.getElementById('busy').style.display = 'block';
    setTimeout(() => { document.getElementById('busy').style.display = 'none';
      if (v === 'modal') document.getElementById('popup_overlay').style.display = 'block';
      else document.getElementById('pv').innerHTML = '<table><tr><td>A-1</td><td>商品A</td></tr></table>'; }, 300); };
  if (v === 'late') setTimeout(go, 3000); else go();
});
</script></body></html>`;
  let variant = 'ok';
  const srv = http.createServer((req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(page(variant)); });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${srv.address().port}`;
  const { previewImport } = await import('../tools/logizard-automation/lz-import-screen.js');
  const browser = await chromium.launch({ headless: true });
  const csv = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-pv-')), 'cdb_logizard_shohinmaster_upload.csv');
  fs.writeFileSync(csv, dailyCsv(['A-1']));
  const cap = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-cap-'));
  try {
    const run = async (v, o = {}) => { variant = v; const p = await browser.newPage(); try { const r = await previewImport(p, { csvPath: csv, base, log: () => {}, captureDir: cap, startTimeoutMs: 5000, ...o }); return { r, executed: await p.evaluate(() => !!window.__executed) }; } finally { await p.close(); } };
    let x = await run('ok');
    assert.deepEqual([x.r.previewed, x.r.confirmed, x.executed], [true, { started: true, changed: true, file: true }, false]);
    assert.ok(fs.existsSync(path.join(cap, 'preview.html')));
    x = await run('late');
    assert.deepEqual([x.r.confirmed.started, x.executed], [true, false]);
    await assert.rejects(run('none', { startTimeoutMs: 1500 }), /プレビューが作られたか確かめられない \(処理の開始 false・画面の変化 false/);
    await assert.rejects(run('modal'), /モーダル/);
  } finally {
    await browser.close();
    await new Promise((r) => srv.close(r));
    if (!shotExisted) fs.rmSync(shotDir, { recursive: true, force: true });   // 失敗の画面の保存 (errorShot) はリポジトリに残さない
  }
});

// ── 押す部品 (③c-1b-2b-1b・契約 v3 K6・K7・C): 本物のブラウザと模擬の PM07 ──
const MOCK_PM07 = (variant) => `<!doctype html><html><body>
<a onclick="openFunctionBar('FM07_01')">imp</a>
<div id="FM07_01_FORM"><select id="FM07_01_fileId"><option value="">-</option><option value="9">商品マスタ</option></select>
<select id="FM07_01_ptrnId"><option value="">-</option><option value="3">デイリー取込商品マスタ</option></select>
<input type="file" id="FM07_01_impFile"><input type="button" id="FM07_01_executeBtn" value="実行"><div id="pv"></div><div id="res"></div></div>
<div class="blockUI blockOverlay" id="busy" style="display:none;position:fixed;inset:0;background:#0003"></div>
<div id="popup_overlay" style="display:none;position:fixed;top:0;left:0;width:400px;height:300px"></div>
<script>
const V = ${JSON.stringify(variant)};
const log = (x) => fetch('/log?' + encodeURIComponent(x));
const show = (html) => { const o = document.getElementById('popup_overlay'); o.innerHTML = html; o.style.display = 'block'; };
const hide = () => { document.getElementById('popup_overlay').style.display = 'none'; };
const RES = 'インポート結果 総件数 : 1 処理件数 : 1 処理不要件数 : 0 エラー件数 : 0';
function dialog(text, okId, extra = '') { return '<div class="ui-dialog"><div class="ui-dialog-content">' + text + '</div><div class="ui-dialog-buttonpane"><input type="button" value="OK" id="' + okId + '"' + extra + '></div></div>'; }
// 本物のロジザードの確認 (jAlerts の jConfirm・2026-09-30 の実機の形): 覆い #popup_overlay の兄弟に箱 #popup_container (z-index が覆いより上)
function jbox(kind, msg, panel) {
  const ov = document.getElementById('popup_overlay'); ov.innerHTML = ''; ov.style.cssText = 'position:absolute;z-index:99998;top:0;left:0;width:100%;height:1400px;background:#fff;opacity:0.01';
  document.body.insertAdjacentHTML('beforeend', '<div id="popup_container" style="position:fixed;z-index:99999;min-width:395px;top:100px;left:100px;background:#fff"><h1 id="popup_title"></h1><div id="popup_content" class="' + kind + '"><div id="popup_message">' + msg + '</div><div id="popup_panel">' + panel + '</div></div></div>');
}
function jclose() { const c = document.getElementById('popup_container'); if (c) c.remove(); document.getElementById('popup_overlay').style.display = 'none'; }
const JPANEL = '<input type="button" value="&nbsp;OK&nbsp;" id="popup_ok"> <input type="button" value="&nbsp;Cancel&nbsp;" id="popup_cancel">';
function result() {
  if (V === 'session') { location.href = '/login'; return; }
  const n = V === 'tworesults' ? 2 : 1;
  show(Array.from({ length: n }, (_, i) => dialog(RES, 'resOk' + i)).join(''));
}
let decoy = '';
if (V === 'decoy') decoy = '<div class="ui-dialog" style="position:fixed;top:320px"><div>お知らせ</div><input type="button" value="OK" id="decoyOk" onclick="log(\\'decoy\\')"></div>';
if (V === 'stale') document.getElementById('res').innerText = RES;
if (V === 'mousedown') document.getElementById('FM07_01_executeBtn').addEventListener('mousedown', () => { log('md'); document.getElementById('res').innerText = RES; });   // mousedown に付いた処理 (古い結果) = 走らないはず
if (V === 'coveredexec') document.body.insertAdjacentHTML('beforeend', '<div id="cover" style="position:fixed;top:0;left:0;width:100%;height:100%;z-index:9"></div>');   // 実行ボタンの上に覆い
if (V === 'lateexec') { let first = true; Object.defineProperty(window, '__lzimpStop', { configurable: true, set() {}, get() { if (first) { first = false; const t = Date.now(); while (Date.now() - t < 1500) { /* 固まる */ } } return false; } }); }
if (V === 'stopinexec') { let first = true; Object.defineProperty(window, '__lzimpStop', { configurable: true, set() {}, get() { if (first) { first = false; const x = new XMLHttpRequest(); x.open('GET', '/stop', false); try { x.send(); } catch { /* 閉じた */ } } return false; } }); }
if (V === 'latescrollexec') { const orig = HTMLElement.prototype.scrollIntoView; HTMLElement.prototype.scrollIntoView = function (...a) { HTMLElement.prototype.scrollIntoView = orig; const t = Date.now(); while (Date.now() - t < 1500) { /* 固まる */ } return orig.apply(this, a); }; }
if (V === 'modalexec') document.body.insertAdjacentHTML('beforeend', '<div class="ui-widget-overlay" style="position:fixed;top:500px;left:0;width:10px;height:10px"></div>');   // モーダルの覆い (ボタンの上ではない)
if (V === 'staleinclick') window.addEventListener('click', (e) => { if (e.target && e.target.id === 'FM07_01_executeBtn') document.getElementById('res').innerText = RES; }, true);   // click の途中 (見張りより前) に古い結果
if (V === 'stalelate') { const b = document.getElementById('FM07_01_executeBtn'); b.disabled = true; setTimeout(() => { document.getElementById('res').innerText = RES; }, 3000); setTimeout(() => { b.disabled = false; }, 4500); }
document.body.insertAdjacentHTML('beforeend', decoy);
document.getElementById('FM07_01_impFile').addEventListener('change', () => {
  if (V === 'retry' && !window.__retried) { window.__retried = true; show(dialog('エラーが発生しました サポートセンターへ', 'errOk')); document.getElementById('errOk').onclick = () => { log('errOk'); hide(); document.getElementById('FM07_01_impFile').value = ''; }; return; }
  if (V === 'retry-nook') { show('<div class="ui-dialog">エラーが発生しました</div>'); return; }
  if (V === 'retry-mixed') { show('<div class="ui-dialog"><div>エラーが発生しました</div><div>在庫を削除しますか</div><div class="ui-dialog-buttonpane"><input type="button" value="OK" id="otherOk"></div></div>'); document.getElementById('otherOk').onclick = () => log('otherOk'); return; }
  if (V === 'retry-plain') { show('<div class="ui-dialog"><div>エラーが発生しました</div><div>在庫を削除します。続行するには OK を押してください。</div><div class="ui-dialog-buttonpane"><input type="button" value="OK" id="otherOk"></div></div>'); document.getElementById('otherOk').onclick = () => log('otherOk'); return; }
  if (V === 'retry-cancel') { show('<div class="ui-dialog"><div class="ui-dialog-content">エラーが発生しました</div><div class="ui-dialog-buttonpane"><input type="button" value="OK" id="errOk"><input type="button" value="キャンセル"></div></div>'); document.getElementById('errOk').onclick = () => log('errOk'); return; }
  document.getElementById('pv').innerHTML = '<table><tr><td>A-1</td></tr></table>';
});
document.getElementById('FM07_01_executeBtn').onclick = () => {
  log('execute');
  if (V === 'nativedialog') { confirm('インポートを実行しますか'); log('after-native'); return; }
  if (V === 'noconfirm') { setTimeout(result, 200); return; }
  if (V === 'staleinclick') { show(dialog('ファイルアップロードを開始します', 'cfmOk')); document.getElementById('cfmOk').onclick = () => log('cfmOk'); return; }   // 古い結果の後に確認が出る
  if (V === 'othermodal') { show('<div class="ui-dialog">取込できません (形式が違います)</div>'); return; }
  // ── 本物の形 (jAlerts) ──
  if (V.startsWith('jconfirm')) {
    const msg = V === 'jconfirm-extra' ? 'ファイルアップロードを開始します。在庫をすべて削除します。よろしいですか?' : 'ファイルアップロードを開始します。よろしいですか?';
    const kind = V === 'jconfirm-alert' ? 'alert' : 'confirm';
    const panel = V === 'jconfirm-twook' ? JPANEL + ' <input type="button" value="OK" id="popup_ok2">' : V === 'jconfirm-okonly' ? '<input type="button" value="&nbsp;OK&nbsp;" id="popup_ok">'
      : V === 'jconfirm-otherbtn' ? '<input type="button" value="&nbsp;OK&nbsp;" id="popup_ok"> <input type="button" value="削除" id="popup_delete">' : JPANEL;
    jbox(kind, msg, panel);
    // 入力欄が足された形 (jPrompt に近い = 確認ではない)
    if (V === 'jconfirm-prompt') document.getElementById('popup_message').insertAdjacentHTML('afterend', '<input type="text" id="popup_prompt">');
    if (V === 'jconfirm-covered') document.body.insertAdjacentHTML('beforeend', '<div style="position:fixed;z-index:100000;top:0;left:0;width:100%;height:100%"></div>');   // 箱の上にさらに覆い
    document.getElementById('popup_ok').onclick = () => { log('cfmOk'); jclose(); document.getElementById('busy').style.display = 'block';
      // 結果の表示も jAlerts (jAlert = .alert) で出る形 / いつもの形
      setTimeout(() => { document.getElementById('busy').style.display = 'none'; if (V === 'jconfirm-jresult') { jbox('alert', RES, '<input type="button" value="&nbsp;OK&nbsp;" id="popup_ok">'); } else result(); }, 300); };
    if (document.getElementById('popup_ok2')) document.getElementById('popup_ok2').onclick = () => log('otherOk');
    return;
  }
  if (V === 'sharedshort') { show('<div class="ui-dialog"><div>ファイルアップロードを開始します</div><div>削除しますか<input type="button" value="OK" id="otherOk"></div></div>'); document.getElementById('otherOk').onclick = () => log('otherOk'); return; }
  if (V === 'secondconfirm') {   // 確認の OK が押せるようになる前に 2 つ目の確認が出る
    show(dialog('ファイルアップロードを開始します', 'cfmOk', ' disabled'));
    document.getElementById('cfmOk').onclick = () => log('cfmOk');
    setTimeout(() => { document.getElementById('popup_overlay').insertAdjacentHTML('beforeend', dialog('ファイルアップロードを開始します', 'cfmOk2')); document.getElementById('cfmOk2').onclick = () => log('cfmOk2'); }, 400);
    setTimeout(() => { document.getElementById('cfmOk').disabled = false; }, 900);
    return;
  }
  if (V === 'sharedparent') { show('<div>ファイルアップロードを開始します</div><div>選択した在庫をすべて削除します。よろしいですか？<input type="button" value="OK" id="otherOk"></div>'); document.getElementById('otherOk').onclick = () => log('otherOk'); return; }
  if (V === 'nestedother') { show('<div class="ui-dialog"><div>ファイルアップロードを開始します</div><div class="ui-dialog">在庫を削除<input type="button" value="OK" id="otherOk"></div></div>'); document.getElementById('otherOk').onclick = () => log('otherOk'); return; }
  const slow = V === 'slowok' || V === 'deadline' || V === 'stallstop';
  if (V === 'stallstop') setTimeout(() => { const t = Date.now(); while (Date.now() - t < 1500) { /* 固まる */ } }, 200);
  const one = V === 'brconfirm'
    ? '<div class="ui-dialog" role="dialog"><div class="ui-dialog-titlebar"><span>確認メッセージ</span><button>×</button></div><div class="ui-dialog-content">ファイルアップロードを<br>開始します。<br>よろしいですか？</div><div class="ui-dialog-buttonpane"><input type="button" value="OK" id="cfmOk"><input type="button" value="キャンセル"></div></div>'
    : dialog('ファイルアップロードを開始します', 'cfmOk', slow ? ' disabled' : '');
  show(V === 'twoconfirm' ? one + dialog('ファイルアップロードを開始します', 'cfmOk2') : V === 'twoconfirm1' ? one + '<div class="ui-dialog"><div>ファイルアップロードを開始します</div></div>' : one);
  if (slow) setTimeout(() => { document.getElementById('cfmOk').disabled = false; }, 3000);
  if (V === 'mousedown') document.getElementById('cfmOk').addEventListener('mousedown', () => { log('md2'); document.getElementById('popup_overlay').insertAdjacentHTML('beforeend', dialog('ファイルアップロードを開始します', 'cfmOk2')); });
  if (V === 'coveredok') document.getElementById('popup_overlay').insertAdjacentHTML('beforeend', '<div style="position:absolute;top:0;left:0;width:100%;height:100%;z-index:5"></div>');   // 確認の OK の上に覆い
  if (V === 'latescroll') { const orig = HTMLElement.prototype.scrollIntoView; HTMLElement.prototype.scrollIntoView = function (...a) { HTMLElement.prototype.scrollIntoView = orig; const t = Date.now(); while (Date.now() - t < 1500) { /* 固まる */ } return orig.apply(this, a); }; }
  if (V === 'confirmandresult') document.getElementById('res').innerText = RES;   // 確認と一緒に (前の) 結果が出る
  // 押す処理 (ページの中) の始めで 1 回だけ止まる: lateconfirm = 1.5 秒ページが固まる / stopinpress = 同期の通信の間に Node 側で止める
  if (V === 'lateconfirm' || V === 'stopinpress') { let first = true; Object.defineProperty(window, '__lzimpStop', { configurable: true, set() {}, get() {
    if (first) { first = false; if (V === 'lateconfirm') { const t = Date.now(); while (Date.now() - t < 1500) { /* 固まる */ } } else { const x = new XMLHttpRequest(); x.open('GET', '/stop', false); try { x.send(); } catch { /* 閉じた */ } } }
    return false; } }); }
  document.getElementById('cfmOk').onclick = () => { log('cfmOk'); hide(); document.getElementById('busy').style.display = 'block';
    if (V === 'confirmlinger') { document.getElementById('busy').style.display = 'none'; document.getElementById('popup_overlay').style.display = 'block'; document.getElementById('popup_overlay').insertAdjacentHTML('beforeend', dialog(RES, 'resOk0')); return; }
    if (V === 'secondafterok') { setTimeout(() => { document.getElementById('busy').style.display = 'none'; show(dialog('ファイルアップロードを開始します', 'cfmOk2') + dialog(RES, 'resOk0')); document.getElementById('cfmOk2').onclick = () => log('cfmOk2'); }, 300); return; }
    if (V === 'twobusy') {
      document.getElementById('busy').style.display = 'none';
      document.body.insertAdjacentHTML('beforeend', '<div class="blockUI blockOverlay" id="busy2" style="position:fixed;inset:0"></div>');
      show(dialog(RES.replace('処理件数 : 1', '処理件数 : 0'), 'resOk0'));
      setTimeout(() => { document.querySelector('.ui-dialog-content').innerText = RES; document.getElementById('busy2').style.display = 'none'; }, 1500);
      return;
    }
    if (V === 'slowresult') {   // 処理中のまま途中の数を出し、1.5 秒後に最後の数へ
      show(dialog(RES.replace('処理件数 : 1', '処理件数 : 0'), 'resOk0'));
      setTimeout(() => { document.querySelector('.ui-dialog-content').innerText = RES; document.getElementById('busy').style.display = 'none'; }, 1500);
      return;
    }
    setTimeout(() => { document.getElementById('busy').style.display = 'none'; result(); }, 300); };
};
</script></body></html>`;

async function withMockPm07(fn) {
  let chromium;
  try { ({ chromium } = await import('playwright')); } catch { return 'skip'; }
  const http = await import('node:http');
  const shotDir = path.join(ROOT, 'tools', 'logizard-automation', 'error-shots');
  const shotExisted = fs.existsSync(shotDir);
  const state = { variant: 'ok', log: [] };
  const srv = http.createServer((req, res) => {
    const u = new URL(req.url, 'http://x');
    if (u.pathname === '/log') { state.log.push(decodeURIComponent(u.search.slice(1))); res.end('ok'); return; }
    if (u.pathname === '/stop') { if (state.onStopReq) state.onStopReq(); setTimeout(() => res.end('ok'), 300); return; }
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(u.pathname === '/login' ? '<html><body><input id="user_id"></body></html>' : MOCK_PM07(state.variant));
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ headless: true });
  const csv = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-ex-')), 'import.csv');
  fs.writeFileSync(csv, dailyCsv(['A-1']));
  try { return await fn({ browser, base, csv, state }); } finally {
    await browser.close();
    await new Promise((r) => srv.close(r));
    if (!shotExisted) fs.rmSync(shotDir, { recursive: true, force: true });
  }
}

await ta('[11] 押す部品 executeImport: 実行 → 決まった文言のモーダルの中の OK だけ → 押した後に新しく出た結果 / 止める旗・持ち時間・想定外の dialog とモーダル・押す前に結果がある・セッション切れ (本物のブラウザ・K6・K7・C)', async () => {
  const S = await import('../tools/logizard-automation/lz-import-screen.js');
  const G = await import('../tools/logizard-automation/import-guard.js');
  const K = await import('../apps/master-decisions/lz-import-check.mjs');
  const r = await withMockPm07(async ({ browser, base, csv, state }) => {
    const run = async (variant, { guard = G.createGuard(), stopAfterMs = null, deadlineAfterPreviewMs = null, stopBeforeExecute = null, onIssued = null, logFn = () => {}, pressWindowMs = 2000, stopReq = null, noClose = false } = {}) => {
      state.variant = variant; state.log = []; state.onStopReq = stopReq;
      const p = await browser.newPage();
      const issued = [];
      let stopAt = null, closedAt = null;
      const realClose = p.close.bind(p);
      if (noClose) p.close = async () => {};
      p.on('close', () => { closedAt = Date.now(); });
      try {
        await S.previewImport(p, { csvPath: csv, base, log: () => {}, startTimeoutMs: 3000 });
        if (stopAfterMs != null) setTimeout(() => { stopAt = Date.now(); guard.stop('lock_extend_failed'); }, stopAfterMs);
        if (deadlineAfterPreviewMs != null) guard.setDeadline(Date.now() + deadlineAfterPreviewMs);
        if (stopBeforeExecute) guard.stop(stopBeforeExecute);
        const out = await S.executeImport(p, { guard, onExecuteIssued: onIssued || (() => issued.push('issued')), log: logFn, confirmTimeoutMs: 6000, resultTimeoutMs: 6000, pollMs: 100, readyTimeoutMs: 8000, pressWindowMs });
        await new Promise((res) => setTimeout(res, 3500));
        return { out, issued, log: [...state.log], closed: p.isClosed() };
      } catch (e) {
        await new Promise((res) => setTimeout(res, 3500));
        return { err: e, issued, log: [...state.log], closed: p.isClosed(), closeLagMs: stopAt != null && closedAt != null ? closedAt - stopAt : null };
      } finally { if (!p.isClosed()) await realClose(); }
    };
    let x = await run('ok');
    assert.deepEqual([x.out.executeIssued, x.out.confirm, x.out.reason, x.issued, x.log], [true, 'clicked', null, ['issued'], ['execute', 'cfmOk']]);
    assert.equal(K.judgeImportResult(K.parseImportResult(x.out.resultText), 1).to, 'imported_unverified');
    // ── 本物の形 (jAlerts の jConfirm・2026-09-30 の実機の試験で見た) ──
    x = await run('jconfirm');   // 覆いと箱が兄弟・OK の値に &nbsp; = 押せる → 結果を読む
    assert.deepEqual([x.out && x.out.confirm, x.out && x.out.reason, x.log], ['clicked', null, ['execute', 'cfmOk']]);
    assert.equal(K.judgeImportResult(K.parseImportResult(x.out.resultText), 1).to, 'imported_unverified');
    x = await run('jconfirm-jresult');   // 結果も jAlerts の箱で出る = 読める
    assert.deepEqual([x.out && x.out.confirm, x.out && x.out.reason, x.log], ['clicked', null, ['execute', 'cfmOk']]);
    assert.equal(K.judgeImportResult(K.parseImportResult(x.out.resultText), 1).to, 'imported_unverified');
    for (const [v, why] of [['jconfirm-alert', 'お知らせ (jAlert) の箱 = 確認の枠ではない'], ['jconfirm-extra', '文言の後に別の文'], ['jconfirm-twook', 'OK が 2 つ'], ['jconfirm-okonly', 'Cancel が無い (形が違う)'], ['jconfirm-otherbtn', '2 つ目のボタンが Cancel でない'], ['jconfirm-prompt', '箱の中に入力欄 (jPrompt に近い)'], ['jconfirm-covered', '箱の上に覆い']]) {
      x = await run(v);
      assert.deepEqual([x.err && x.err.executeIssued, x.log], [true, ['execute']], `${v}: ${why} = 押さない`);
      assert.match(String(x.err && x.err.reason), /^confirm_(unidentified|covered|ambiguous)$/, v);
    }
    x = await run('decoy');   // 関係ない「お知らせ」の OK は押さない
    assert.deepEqual([x.out.confirm, x.log], ['clicked', ['execute', 'cfmOk']]);
    x = await run('noconfirm');   // 確認が出ない = 押さずに結果を待つ
    assert.deepEqual([x.out.confirm, x.out.reason, x.log], ['not_shown', null, ['execute']]);
    x = await run('twoconfirm');   // 確認が 2 つ = 押さずに止める
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.log], ['confirm_ambiguous', true, ['execute']]);
    x = await run('othermodal');   // 決まった文言のない別のモーダル = 押さない
    assert.deepEqual([x.out.reason, x.log], ['error_modal', ['execute']]);
    assert.match(x.out.resultText, /取込できません/);
    x = await run('slowok', { stopAfterMs: 500 });   // OK が押せるようになる前に止めた = ページを閉じる・押されない
    assert.deepEqual([x.err && x.err.reason, x.closed, x.log], ['lock_extend_failed', true, ['execute']]);
    x = await run('deadline', { guard: G.createGuard({ marginMs: 6000 }), deadlineAfterPreviewMs: 8000 });   // 持ち時間 = 約 2 秒・OK は 3 秒後に押せる
    assert.deepEqual([x.err && x.err.reason, x.closed, x.log], ['deadline', true, ['execute']]);
    x = await run('nativedialog');   // ブラウザの dialog = 承認しない・止める (押した後の例外 = 呼び手は unknown)
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.closed, x.log[0], x.log.includes('cfmOk')], ['unexpected_dialog', true, true, 'execute', false]);
    x = await run('ok', { stopBeforeExecute: 'lock_lost' });   // 押す前から止めてある = 押していない (executeIssued: false = 呼び手は failed_before_execute)
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.issued, x.log], ['lock_lost', false, [], []]);
    x = await run('stale');   // 押す前に結果の表示がある = 押さない (executeIssued: false = 押す前の失敗)
    assert.deepEqual([x.err && /押す前に結果/.test(x.err.message), x.err && x.err.executeIssued, x.issued, x.log], [true, false, [], []]);
    x = await run('session');   // 押した後にセッション切れ
    assert.deepEqual([x.out.reason, x.log], ['session_lost', ['execute', 'cfmOk']]);
    x = await run('tworesults');   // 結果が 2 つ = 読み方で unknown
    assert.equal(K.judgeImportResult(K.parseImportResult(x.out.resultText), 1).to, 'unknown');
    // ── Codex #1521 R1 ──
    x = await run('sharedparent');   // 共通の親 (popup) にある別の操作の OK = 押さない (popup は枠にしない = 枠が無い)
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.closed, x.log], ['confirm_unidentified', true, true, ['execute']]);
    x = await run('nestedother');   // 入れ子の別の枠の中の OK = 数えない
    assert.deepEqual([x.err && x.err.reason, x.log], ['confirm_unidentified', ['execute']]);
    x = await run('twoconfirm1');   // 確認が 2 つ・片方に OK が無い = 押さない (文言の場所が 2 つ)
    assert.deepEqual([x.err && x.err.reason, x.log], ['confirm_ambiguous', ['execute']]);
    x = await run('stalelate');   // 実行ボタンが押せるようになるのを待つ間に古い結果が出た = 押さない
    assert.deepEqual([x.err && /押す前に結果の表示が(もうある|出た)/.test(x.err.message), x.err && x.err.executeIssued, x.issued, x.log], [true, false, [], []]);
    x = await run('slowresult');   // 処理中の途中の数 (処理件数 0) を読まない = 最後の数 (処理件数 1)
    assert.deepEqual([x.out && x.out.reason, x.log], [null, ['execute', 'cfmOk']]);
    assert.equal(K.parseImportResult(x.out.resultText).processed, 1);
    x = await run('ok', { onIssued: () => { throw new Error('記録に失敗'); } });   // 押した記録に失敗 = 押さない (executeIssued: false)
    assert.deepEqual([x.err && x.err.message, x.err && x.err.executeIssued, x.log], ['記録に失敗', false, []]);
    x = await run('ok', { logFn: (m) => { if (/ファイルアップロード/.test(m)) throw 'boom'; } });   // 押した後の文字列の例外も Error に包んで executeIssued つき
    assert.deepEqual([x.err instanceof Error, x.err && x.err.message, x.err && x.err.executeIssued], [true, 'boom', true]);
    // ── Codex #1521 R2 ──
    x = await run('sharedshort');   // 同じ枠 (ui-dialog) の短い別の文 (削除しますか) と OK = 押さない (枠の中に決まった語以外の文字)
    assert.deepEqual([x.err && x.err.reason, x.log], ['confirm_unidentified', ['execute']]);
    x = await run('secondconfirm');   // OK が押せるようになる前に 2 つ目の確認 = 押さない
    assert.deepEqual([x.err && x.err.reason, x.log.includes('cfmOk') || x.log.includes('cfmOk2')], ['confirm_ambiguous', false]);
    x = await run('staleinclick');   // click の途中 (見張りより前) に出た結果 = 押した後に出た結果ではない = 確認の OK は押さない・受け取らない
    assert.deepEqual([x.out && x.out.reason, x.out && x.out.resultText, x.log], ['stale_result', null, ['execute']]);
    const g3 = G.createGuard();
    x = await run('ok', { guard: g3, onIssued: () => { g3.stop('lock_lost'); } });   // 記録の間に止めた = 押さない (記録の後にもう一度旗を見る)
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.log], ['lock_lost', false, []]);
    const g2 = G.createGuard();
    const check0 = g2.check;
    g2.check = (w) => { if (/記録の後/.test(w)) throw new G.StopError(`${w}: 鍵を失った`, 'lock_lost'); return check0(w); };   // 記録の後の最後の確かめで止まる
    x = await run('ok', { guard: g2 });
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.issued, x.log], ['lock_lost', false, ['issued'], []]);   // 記録はある・click は出していない
    // ── Codex #1521 R3 ──
    x = await run('mousedown');   // mousedown / mouseup を出さない = そこに付いた処理 (古い結果・2 つ目の確認) は走らない・click 1 回だけ
    assert.deepEqual([x.out && x.out.reason, x.out && x.out.confirm, x.log], [null, 'clicked', ['execute', 'cfmOk']]);
    x = await run('coveredexec');   // 実行ボタンが覆われている = 押さない
    assert.deepEqual([x.err && /covered/.test(x.err.message), x.err && x.err.executeIssued, x.log], [true, false, []]);
    x = await run('modalexec');   // モーダルの覆い (ui-widget-overlay) がある = 押さない
    assert.deepEqual([x.err && /modal_open/.test(x.err.message), x.err && x.err.executeIssued, x.log], [true, false, []]);
    x = await run('coveredok');   // 確認の OK が別のものに覆われている = 押さずに止める
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.log], ['confirm_covered', true, ['execute']]);
    const logs = [];
    x = await run('lateconfirm', { pressWindowMs: 1000, logFn: (m) => logs.push(m) });   // ページの処理が遅れて始まった = その回は押さない → もう一度確かめて押す
    assert.deepEqual([x.out && x.out.confirm, x.log, logs.some((m) => /遅れて/.test(m))], ['clicked', ['execute', 'cfmOk'], true]);
    x = await run('lateconfirm', { guard: G.createGuard({ marginMs: 100 }), deadlineAfterPreviewMs: 1500 });   // 固まっている間に締め切りを越えた = 押さない
    assert.deepEqual([x.err && x.err.reason, x.log], ['deadline', ['execute']]);
    const g4 = G.createGuard();
    x = await run('stopinpress', { guard: g4, stopReq: () => g4.stop('lock_lost') });   // 押す処理の途中で止めた = 閉じて止める か 止めた後に押された (afterStop) = 成功と報告しない
    assert.ok((x.err && x.err.reason === 'lock_lost' && x.err.executeIssued === true) || (x.out && x.out.afterStop === 'confirm'), JSON.stringify({ err: x.err && [x.err.message, x.err.reason, x.err.executeIssued, x.err.afterStop], out: x.out, log: x.log }));
    if (x.log.includes('cfmOk')) assert.ok((x.out && x.out.afterStop === 'confirm') || (x.err && x.err.executeIssued === true));
    // ページを閉じるのが間に合わない = 止めた後に押された = afterStop を残して押したとして扱う (確認 = 取込は始まった = 結果を読む / 実行 = 次の確かめで止める)
    const g5 = G.createGuard();
    x = await run('stopinpress', { guard: g5, stopReq: () => g5.stop('lock_lost'), noClose: true });
    assert.deepEqual([x.out && x.out.afterStop, x.out && x.out.confirm, x.log], ['confirm', 'clicked', ['execute', 'cfmOk']], JSON.stringify(x.err && x.err.message));
    const g6 = G.createGuard();
    x = await run('stopinexec', { guard: g6, stopReq: () => g6.stop('lock_lost'), noClose: true });
    assert.deepEqual([x.err && x.err.reason, x.err && x.err.executeIssued, x.err && x.err.afterStop, x.log], ['lock_lost', true, 'execute', ['execute']]);
    x = await run('lateexec', { guard: G.createGuard({ marginMs: 100 }), deadlineAfterPreviewMs: 1500 });   // 実行ボタンの押す処理が遅れて始まった = 押さない (executeIssued false)
    assert.deepEqual([x.err && /late/.test(x.err.message), x.err && x.err.executeIssued, x.log], [true, false, []]);
    x = await run('stallstop', { stopAfterMs: 500 });   // ページが固まっている間に止めた = すぐページを閉じる (固まりが解けるのを待たない)
    assert.deepEqual([x.err && x.err.reason, x.closed, x.log], ['lock_extend_failed', true, ['execute']]);
    assert.ok(x.closeLagMs != null && x.closeLagMs < 900, `止めてから閉じるまで ${x.closeLagMs} ms`);
    x = await run('brconfirm');   // 本文が <br> で分かれる・タイトルの帯 (確認メッセージ・×)・キャンセル = 押せる (Codex #1521 R3 Medium)
    assert.deepEqual([x.out && x.out.confirm, x.log], ['clicked', ['execute', 'cfmOk']]);
    // ── Codex #1521 R4 ──
    x = await run('latescroll', { guard: G.createGuard({ marginMs: 100 }), deadlineAfterPreviewMs: 1500 });   // 確認の OK の位置の計算の間に押してよい時刻を過ぎた = 押さない
    assert.deepEqual([x.err && x.err.reason, x.log], ['deadline', ['execute']]);
    x = await run('latescrollexec', { guard: G.createGuard({ marginMs: 100 }), deadlineAfterPreviewMs: 1500 });   // 実行ボタンの位置の計算の間に過ぎた = 押さない
    assert.deepEqual([x.err && /late/.test(x.err.message), x.err && x.err.executeIssued, x.log], [true, false, []]);
    x = await run('confirmandresult');   // 確認の OK を押していないのに確認と結果が同時に出た = 結果を受け取らない (取込が始まったか分からない)
    assert.deepEqual([x.out && x.out.reason, x.out && x.out.confirm, x.out && x.out.resultText, x.log], ['confirm_not_clicked', 'not_shown', null, ['execute']]);
    x = await run('twobusy');   // 2 つ目の処理中の表示の間の途中の数は読まない
    assert.deepEqual([x.out && x.out.reason, x.log], [null, ['execute', 'cfmOk']]);
    assert.equal(K.parseImportResult(x.out.resultText).processed, 1);
    // ── Codex #1521 R5 ──
    x = await run('confirmlinger');   // 確認の OK を押した後も確認が残ったまま結果 = 受け取らない (消えなければ confirm_still_shown)
    assert.deepEqual([x.out && x.out.reason, x.out && x.out.confirm, x.out && x.out.resultText, x.log], ['confirm_still_shown', 'clicked', null, ['execute', 'cfmOk']]);
    x = await run('secondafterok');   // 押した後に 2 つ目の確認と (古い) 結果 = 受け取らない・2 つ目は押さない
    assert.deepEqual([x.out && x.out.reason, x.out && x.out.resultText, x.log], ['confirm_still_shown', null, ['execute', 'cfmOk']]);
    // 止めた後の check は押さない
    const g = G.createGuard(); g.stop('x');
    assert.throws(() => g.check('実行ボタンの前'), (e) => e.stopped && e.reason === 'x');
  });
  if (r === 'skip') { console.log('      (playwright が無い = この試験はとばす)'); passed--; }
});

await ta('[12] プレビューのサーバーエラー (「エラーが発生しました」) は OK を押さずに止める (画面を残す)・同じ枠の別の操作の OK も押さない (画面全体の最初の OK も押さない・K6・Codex #1521 R5・R6)', async () => {
  const S = await import('../tools/logizard-automation/lz-import-screen.js');
  const r = await withMockPm07(async ({ browser, base, csv, state }) => {
    const run = async (variant) => {
      state.variant = variant; state.log = [];
      const p = await browser.newPage();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-pv-'));
      try { return { out: await S.previewImport(p, { csvPath: csv, base, log: () => {}, startTimeoutMs: 3000, captureDir: dir }), log: [...state.log], dir }; }
      catch (e) { return { err: e, log: [...state.log], dir }; }
      finally { await p.close(); }
    };
    // エラーだけの枠 (OK 1 つ) でも・OK が無い・問いかけ・「OK を押してください」の別の操作・キャンセル = どれも OK を押さずに止める・画面を残す
    for (const v of ['retry', 'retry-nook', 'retry-mixed', 'retry-plain', 'retry-cancel']) {
      const x = await run(v);
      assert.match(x.err && x.err.message, /エラーが発生しました/, v);
      assert.deepEqual(x.log, [], v);
      assert.ok(fs.existsSync(path.join(x.dir, 'preview-failed.html')), v);
    }
  });
  if (r === 'skip') { console.log('      (playwright が無い = この試験はとばす)'); passed--; }
  const src = fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'lz-import-screen.js'), 'utf8');
  assert.ok(!/\.first\(\)/.test(src), '画面全体の最初の OK (.first()) を使わない');
});

await ta('[13] CLI: 時刻の窓の外 (08:40 / 11:45 の回) は DATA_DIR などの設定を見る前に ℹ で終わる (exit 0・ping しない) / 窓の中で DATA_DIR が無い = ❌ (2026-09-29 00:21 の件)', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-now-'));
  const fake = path.join(tmp, 'fake-now.mjs');
  // 時刻の差し替え + fetch の差し替え (ping を送った回数と中身をファイルに = 本当に送らないかを数える。Codex R1 Low)
  fs.writeFileSync(fake, [
    "import fs from 'node:fs';",
    'const F = Date.parse(process.env.FAKE_NOW); const R = Date; class D extends R { constructor(...a) { super(...(a.length ? a : [F])); } static now() { return F; } } globalThis.Date = D;',
    'globalThis.fetch = async (url) => { fs.appendFileSync(process.env.PING_LOG, String(url) + "\\n"); return new Response("{}", { status: 200 }); };',
  ].join('\n') + '\n');
  const pingLog = path.join(tmp, 'pings.txt');
  const pings = () => { try { return fs.readFileSync(pingLog, 'utf8').trim().split('\n').filter(Boolean); } catch { return []; } };
  const { pathToFileURL } = await import('node:url');
  const cli = (at) => spawnSync(process.execPath, ['--import', pathToFileURL(fake).href, 'scripts/logizard-import/lz-daily-import.mjs'],
    { cwd: ROOT, encoding: 'utf8', env: { ...process.env, FAKE_NOW: at, DATA_DIR: '', JOBS_MONITOR_TOKEN: 'dummy-token', JOBS_MONITOR_URL: 'https://jobs.example.test', PING_LOG: pingLog, LZ_DAILY_IMPORT_SHADOW: '' } });
  for (const at of ['2030-01-16T08:40:00+09:00', '2030-01-16T11:45:00+09:00', '2030-01-16T00:14:00+09:00']) {
    const c = cli(at);
    assert.equal(c.status, 0, at + c.stdout + c.stderr);
    assert.match(c.stdout.trim().split('\n').pop(), /^ℹ ロジザード毎日の商品マスタの取込 \(影\): 時刻の窓の外/, at);
  }
  assert.deepEqual(pings(), [], '窓の外は ping を送らない');
  const c = cli('2030-01-16T00:20:00+09:00');
  assert.equal(c.status, 1, c.stdout + c.stderr);
  assert.match(c.stdout.trim().split('\n').pop(), /^❌ ロジザード毎日の商品マスタの取込 \(影\): DATA_DIR が無い/);
  const sent = pings();
  assert.equal(sent.length, 1, '窓の中で設定が欠けた = fail の ping を 1 回');
  assert.match(sent[0], /^https:\/\/jobs\.example\.test\/apps\/jobs-monitor\/ping\/lz-daily-import-shadow\?status=fail&note=/);
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
