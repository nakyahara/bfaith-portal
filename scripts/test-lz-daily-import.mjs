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
const shadow = (s, f, o = {}) => RUN.runShadow({ dataDir: s.dataDir, now: NOW, localInitFile: path.join(s.dataDir, 'lz-import', 'init.json'), lzMinRows: 1, log: () => {}, ...f, ...o });
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

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
