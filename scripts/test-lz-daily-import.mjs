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
  writeEvidence(dataDir, 'lz-daily', { state, as_of: AS_OF, run_id: RUN_DIR, verdict, deadline, csv: { path: rel, sha256: shaOverride || sha(buf), rows: rowsOverride ?? ids.length } }, { now: new Date('2030-01-15T00:20:00Z'), warn: () => {} });
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

await ta('[6] 画面の部品はプレビューまで (実行ボタンを押さない)・bat の 1.5 ステップ目 (終了コードを変えない・ASCII・CRLF)・台帳・写すファイル', async () => {
  const screen = fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'lz-import-screen.js'), 'utf8');
  assert.ok(!/click\([^)]*executeBtn/.test(screen), '実行ボタンを押す行が無い');
  assert.ok(screen.includes("export const DAILY_PATTERN_LABEL = 'デイリー取込商品マスタ';"));
  const bat = fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'run-nyuka-csv-scheduled.bat'));
  assert.ok(!/[^\x00-\x7f]/.test(bat.toString('latin1')), 'ASCII だけ');
  assert.ok(!/(?<!\r)\n/.test(bat.toString('latin1')), 'CRLF');
  const b = bat.toString('latin1');
  const iNyuka = b.indexOf('node auto-nyuka-csv.js'), iImp = b.indexOf('node C:\\Users\\bfaith\\bfaith-portal\\scripts\\logizard-import\\lz-daily-import.mjs >> logs\\scheduled.log 2>&1'), iShohin = b.indexOf('node auto-shohin-csv.js --once-per-day');
  assert.ok(iNyuka > 0 && iImp > iNyuka && iShohin > iImp, '入荷受付 → 取込 (影) → 商品マスタ の順');
  assert.ok(!/set "RC=/.test(b.slice(iImp, iShohin)), '取込の終了コードで bat の RC を変えない');
  assert.match(b.slice(b.lastIndexOf('exit /b')), /exit \/b %RC%/);
  const { JOBS_REGISTRY, validateRegistry } = await import('../config/jobs-registry.mjs');
  const job = JOBS_REGISTRY.find((e) => e.id === RUN.JOB_SHADOW), retire = JOBS_REGISTRY.find((e) => e.id === 'lz-daily-import-shadow-retire');
  assert.deepEqual([job.type, job.anchor_hour_jst, job.anchor_minute_jst, retire.type, validateRegistry([job, retire])], ['scheduled_job', 0, 20, 'temporary_asset', []]);
  const m = JSON.parse(fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'manifest.json'), 'utf8'));
  for (const pc of ['minipc', 'streamdeck']) assert.ok(m.pcs[pc].includes('lz-import-screen.js'), pc);
});

await ta('[7] CLI: LZ_DAILY_IMPORT=on でも本番の取込はしない (この版は断る)・DATA_DIR が無い = ❌', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimp-cli-'));
  const cli = (env) => spawnSync(process.execPath, ['scripts/logizard-import/lz-daily-import.mjs', '--force-window'], { cwd: ROOT, encoding: 'utf8', env: { ...process.env, JOBS_MONITOR_TOKEN: '', ...env } });
  let c = cli({ DATA_DIR: tmp, LZ_DAILY_IMPORT: 'on' });
  assert.equal(c.status, 1, c.stderr);
  assert.match(c.stdout.trim().split('\n').pop(), /^❌ .*本番の取込をしない/);
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
  writeEvidence(d2, 'lz-daily', { state: 'complete', as_of: '2030-01-16', run_id: 'lzd_20300116T001500000Z_abcdef', verdict: 'pass', deadline: '2030-01-17T01:00:00+09:00', csv: { path: rel, sha256: sha(buf), rows: 1 } }, { now: new Date('2030-01-16T00:20:00Z'), warn: () => {} });
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

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
