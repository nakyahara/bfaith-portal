/**
 * test-lz-cutover-check.mjs — 切替・戻しの手順の確かめ (scripts/logizard-import/lz-cutover-check.mjs・2b-2 契約 v3 の切替の PR)
 *
 * 本物のポータルの状態の機械 (apps/logizard-import-state/store.js・メモリの DB) を口の形で包んで確かめる:
 *   before / cutover / ready / rollback の各段階で、そのとおりなら ok・1 つでも違えば ❌
 *   Render の時計で夜の窓の外 (01:30〜23:30) だけ / cutover_phase を人が設定した / GAS の CSV は本当の道と同じ判定で断る
 *   旧い手の ③ (manual_daily) の鍵の要求は、旗 on を読み戻した cutover / ready だけ・本物の store が retired で断る (DB に何も書かない)
 *   万一取れた = すぐ返して ❌ / DATA_DIR はこの miniPC のもの (初期化の印) / 次の夜の済みの印 (N8) / 送り先は本番と同じ判定 / 値は出さない
 *   戻しの固定の版 (台帳 lz-gas-rollback) = 期限の内 (Render の日付)・11 ファイルの sha256・tag が commit を指す (無ければ失敗)
 * 使い方: node scripts/test-lz-cutover-check.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const S = await import('../apps/logizard-import-state/store.js');
const C = await import('./logizard-import/lz-cutover-check.mjs');
const N = await import('./logizard-import/lz-nightly.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('test-lz-cutover-check');

const JST = (s) => Date.parse(`${s}+09:00`);
const DAYTIME = JST('2030-01-17T14:00:00');   // 切替は昼 (夜の窓の外。N8)
const setV4 = (on) => { process.env.LZ_MANUAL_V4 = on ? 'on' : ''; };
const HOOK = 'https://chat.googleapis.com/v1/spaces/SECRET/messages?key=k';
const TIME = '切替・戻しをしてよい時刻 (Render の時計で JST 01:30〜23:30)';
const DIR = 'DATA_DIR がこの miniPC のもの (初期化の印がポータルと同じ)';

/** 本物の store を口の形で包む (呼ばれた口を残す・hooks で差し替え) */
function portal({ at = DAYTIME, halted = true, phase = 'cutover', hooks = {} } = {}) {
  const now = () => at;
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: now() });
  if (phase) S.setSetting(db, { key: 'cutover_phase', value: phase, by: 'x', now: now() });
  if (halted) S.halt(db, { by: 'x', reason: '切替の手順 1 (止める)', now: now() });
  const calls = [];
  const w = (name, f) => async (b) => { calls.push({ name, b }); if (hooks[name]) { const r = await hooks[name](b, { db, now }); if (r !== undefined) return r; } return { ok: true, ...f(b) }; };
  const client = {
    status: w('status', (n) => S.getStatus(db, { now: now(), events: n })),
    nightlyReadiness: w('nightlyReadiness', (x) => S.nightlyReadiness(db, { sourceRunId: x.source_run_id, csvSha256: x.csv_sha256, rows: x.rows, targetAsOf: x.target_as_of, now: now() })),
    acquire: w('acquire', (b) => S.acquire(db, { initId: b.init_id, holder: b.holder, purpose: b.purpose, runId: b.run_id, ttlSec: b.ttl_sec, by: b.by, now: now() })),
    release: w('release', (b) => S.release(db, { lockToken: b.lock_token, by: b.by, now: now() })),
  };
  const events = () => db.prepare('SELECT COUNT(*) AS n FROM import_events').get().n;
  return { db, init_id, client, calls, events, now };
}
/** この miniPC の DATA_DIR (初期化の印 = ポータルと同じ) */
const dataDirOf = (p, initId = p.init_id) => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'lzcut-'));
  if (initId) { fs.mkdirSync(path.join(d, 'lz-import'), { recursive: true }); fs.writeFileSync(path.join(d, 'lz-import', 'init.json'), JSON.stringify({ init_id: initId, by: 'x' })); }
  return d;
};
const run = (p, expect, { env = {}, dir, registry } = {}) => C.cutoverCheck({ expect, client: p.client, env, dataDir: dir === undefined ? dataDirOf(p) : dir, ...(registry ? { registry } : {}) });
const bad = (r) => r.checks.filter((c) => !c.ok).map((c) => c.name);
const names = (p) => p.calls.map((c) => c.name);

await ta('[1] before (止めた後・旗の前): 止め・鍵なし・手の取込なし・未解決なし・旗 off・毎晩の本番 off = ok / 1 つでも違う = ❌ / 鍵の要求を送らない', async () => {
  setV4(false);
  let p = portal();
  let r = await run(p, 'before');
  assert.deepEqual([r.ok, bad(r)], [true, []]);
  assert.deepEqual(names(p), ['status'], '読むだけ');
  r = await run(portal({ halted: false }), 'before');
  assert.deepEqual(bad(r), ['自動の取込を止めてある (halt)']);
  setV4(true);
  r = await run(portal(), 'before');
  assert.deepEqual(bad(r), ['ポータルの旗 LZ_MANUAL_V4 = off (読み戻し)']);
  setV4(false);
  r = await run(portal(), 'before', { env: { LZ_DAILY_IMPORT: ' ON ' } });
  assert.deepEqual(bad(r), ['miniPC の毎晩の本番 LZ_DAILY_IMPORT = off']);
  // 生きた鍵がある (試験の取込の途中) / 手の取込が開いている / 未解決の取込
  p = portal({ halted: false });
  S.acquire(p.db, { initId: p.init_id, holder: 'auto', purpose: 'import', runId: 'lzim_test_20300117T050000_abcdef', ttlSec: 600, by: 'x', now: p.now() });
  S.halt(p.db, { by: 'x', reason: '止める (鍵は生きたまま)', now: p.now() });
  r = await run(p, 'before');
  assert.deepEqual(bad(r), ['生きた鍵が無い']);
  p = portal({ hooks: { status: (n, { db, now }) => { const s = S.getStatus(db, { now: now() }); return { ok: true, ...s, manual: { ...s.manual, open: true }, state: 'unknown' }; } } });
  r = await run(p, 'before');
  assert.deepEqual(bad(r), ['手の取込が開いていない', '未解決の取込が無い (状態 = idle / verified)']);
});

await ta('[2] cutover (旗 on・cutover_phase の後): 人が cutover を設定・GAS の CSV = 本当の道と同じ判定で gas_closed・成果物の無い毎晩 = artifact_missing・旧い手の ③ = 本物の store が retired (DB に何も書かない・鍵なし)・DATA_DIR の印・次の夜の済みの印なし = ok', async () => {
  setV4(true);
  const p = portal();
  const ev0 = p.events();
  const r = await run(p, 'cutover');
  assert.deepEqual([r.ok, bad(r)], [true, []], JSON.stringify(r.checks));
  assert.deepEqual(names(p), ['status', 'nightlyReadiness', 'acquire'], 'release は呼ばない (取れていない)');
  const acq = p.calls.find((c) => c.name === 'acquire').b;
  assert.deepEqual([acq.holder, acq.purpose, acq.init_id, acq.ttl_sec, /^lzcut_\d+_[0-9a-f]{6}$/.test(acq.run_id)], ['manual_daily', 'import', p.init_id, 30, true]);
  const rd = p.calls.find((c) => c.name === 'nightlyReadiness').b;
  assert.equal(rd.target_as_of, '2030-01-16', 'Render の時刻で前の日 (形の正しい識別で聞く = artifact_missing まで届く)');
  assert.deepEqual([p.events(), S.getStatus(p.db, { now: p.now() }).lock, S.getStatus(p.db, { now: p.now() }).halted], [ev0, null, true], '断られた要求は何も書かない');
  assert.ok(r.checks.some((c) => c.name === '旧い手の ③ (manual_daily) を断る (retired)' && c.ok && c.detail === 'retired'));
});

await ta('[3] cutover の ❌: 人が設定していない (既定の cutover を読んだだけ = 手順 4 を飛ばした) / transition (GAS の CSV を開いたまま) / 古い Render (答えが無い) / 旗が off = 鍵の要求を送らない / 次の夜の済みの印 / 毎晩の本番がもう on', async () => {
  setV4(true);
  let r = await run(portal({ phase: null }), 'cutover');
  assert.deepEqual(bad(r), ['cutover_phase = cutover を人が設定した (手順 4)']);
  assert.match(r.checks.find((c) => !c.ok).detail, /設定 無い \(既定で読んだだけ\)|設定 無し \(既定で読んだだけ\)/);
  r = await run(portal({ phase: 'transition' }), 'cutover');
  assert.deepEqual(bad(r), ['cutover_phase = cutover を人が設定した (手順 4)', 'GAS の CSV の手の取込を断る (gas_closed)']);
  let p = portal({ hooks: { nightlyReadiness: (x, { db, now }) => { const { gas_upload: _g, cutover_phase_explicit: _e, ...old } = S.nightlyReadiness(db, { sourceRunId: x.source_run_id, csvSha256: x.csv_sha256, rows: x.rows, targetAsOf: x.target_as_of, now: now() }); return { ok: true, ...old }; } } });
  r = await run(p, 'cutover');
  assert.deepEqual(bad(r), ['cutover_phase = cutover を人が設定した (手順 4)', 'GAS の CSV の手の取込を断る (gas_closed)'], '古い Render の版 = 答えが無い = ❌');
  assert.ok(r.checks.filter((c) => !c.ok).every((c) => /古い Render の版/.test(c.detail)));
  setV4(false);
  p = portal();
  r = await run(p, 'cutover');
  assert.deepEqual(bad(r), ['ポータルの旗 LZ_MANUAL_V4 = on (読み戻し)', '成果物の無い毎晩を断る (artifact_missing)', 'readiness の旗も on', '旧い手の ③ (manual_daily) を断る (retired)']);
  assert.ok(!names(p).includes('acquire'), '旗 off で manual_daily を送ると止めている間は鍵が取れてしまう = 送らない');
  setV4(true);
  p = portal();
  const dir = dataDirOf(p);
  fs.mkdirSync(path.dirname(N.nightlyMarker(dir, '2030-01-18')), { recursive: true });
  fs.writeFileSync(N.nightlyMarker(dir, '2030-01-18'), '{}');
  r = await run(p, 'cutover', { dir });
  assert.deepEqual(bad(r), ['次の夜 (2030-01-18) の済みの印が無い']);
  fs.mkdirSync(path.dirname(N.nightlyMarker(dir, '2030-01-17')), { recursive: true });
  fs.rmSync(N.nightlyMarker(dir, '2030-01-18'));
  fs.writeFileSync(N.nightlyMarker(dir, '2030-01-17'), '{}');
  r = await run(p, 'cutover', { dir });
  assert.equal(r.ok, true, '今日の印は次の夜に効かない = ok (知らせだけ)');
  r = await run(portal(), 'cutover', { env: { LZ_DAILY_IMPORT: 'on' } });
  assert.deepEqual(bad(r), ['miniPC の毎晩の本番 LZ_DAILY_IMPORT = off']);
});

await ta('[4] DATA_DIR の取り違え (Codex #1558 R1 Medium): 無い・印の無い別の場所・別の初期化の印・壊れた印 = ❌ (「済みの印が無い」を偽の ok にしない)', async () => {
  setV4(true);
  const p = portal();
  for (const [dir, why] of [[null, 'DATA_DIR が無い'], [dataDirOf(p, null), /初期化の印が無い/], [dataDirOf(p, 'lzi_other'), /印 lzi_other・ポータル lzi_/]]) {
    const r = await run(p, 'cutover', { dir });
    assert.deepEqual(bad(r), [DIR, '次の夜の済みの印が無い'], String(why));
    assert.match(r.checks.find((c) => c.name === DIR).detail, why instanceof RegExp ? why : new RegExp(why));
  }
  const broken = dataDirOf(p);
  fs.writeFileSync(path.join(broken, 'lz-import', 'init.json'), '{');
  const r = await run(p, 'cutover', { dir: broken });
  assert.deepEqual(bad(r), [DIR, '次の夜の済みの印が無い']);
});

await ta('[5] 夜の窓 (Codex #1558 R1 Medium): Render の時計で JST 01:30 以上 23:30 未満だけ ok / 00:20 は readiness に artifact_missing があっても ❌ / miniPC の壁時計ではない', async () => {
  setV4(true);
  for (const [t, ok] of [['01:29:59', false], ['01:30:00', true], ['12:00:00', true], ['23:29:59', true], ['23:30:00', false], ['00:15:00', false], ['00:20:00', false], ['00:55:00', false]]) {
    const at = JST(`2030-01-17T${t}`);
    const r = await run(portal({ at }), 'cutover');
    assert.equal(r.checks.find((c) => c.name === TIME).ok, ok, t);
    assert.equal(r.ok, ok, `${t} ${JSON.stringify(bad(r))}`);
  }
  const realNow = Date.now;
  Date.now = () => JST('2030-01-17T00:20:00');   // miniPC の壁時計が夜でも Render の昼なら ok
  try { assert.equal((await run(portal(), 'cutover')).checks.find((c) => c.name === TIME).ok, true); } finally { Date.now = realNow; }
  setV4(false);
  const r = await run(portal({ hooks: { status: (n, { db, now }) => { const s = S.getStatus(db, { now: now() }); return { ok: true, ...s, clock: { ...s.clock, server_now: undefined } }; } } }), 'before');
  assert.deepEqual(bad(r), [TIME]);
});

await ta('[6] 万一 manual_daily の鍵が取れた (旗が効いていない) = すぐ返して ❌ / 返せない = そう書く / 断りの code が retired でない = ❌', async () => {
  setV4(true);
  let p = portal({ hooks: { acquire: () => ({ ok: true, lock_token: 'tok', expires_at: 1 }) } });
  let r = await run(p, 'cutover');
  assert.deepEqual([bad(r), names(p).slice(-2)], [['旧い手の ③ (manual_daily) を断る (retired)'], ['acquire', 'release']]);
  assert.deepEqual(p.calls.at(-1).b.lock_token, 'tok');
  assert.match(r.checks.find((c) => !c.ok).detail, /鍵が取れてしまった = 旗が効いていない・返した/);
  p = portal({ hooks: { acquire: () => ({ ok: true, lock_token: 'tok' }), release: () => { throw Object.assign(new Error('x'), { code: 'unreachable' }); } } });
  r = await run(p, 'cutover');
  assert.match(r.checks.find((c) => !c.ok).detail, /返せない \(unreachable: x\)・期限 30 秒で切れる/);
  p = portal({ hooks: { acquire: () => { throw Object.assign(new Error('busy'), { code: 'busy' }); } } });
  r = await run(p, 'cutover');
  assert.deepEqual([bad(r), r.checks.find((c) => !c.ok).detail], [['旧い手の ③ (manual_daily) を断る (retired)'], 'busy']);
});

await ta('[7] ready (miniPC の LZ_DAILY_IMPORT=on の後・止めの解除の前): 毎晩の本番 on・送り先が本番と同じ判定で使える・cutover の設定のまま = ok / 送り先が無い・壊れている・毎晩の本番 off = ❌ / 値は出さない', async () => {
  setV4(true);
  const logs = [];
  const p = portal();
  let r = await C.cutoverCheck({ expect: 'ready', client: p.client, env: { LZ_DAILY_IMPORT: 'on', GCHAT_WEBHOOK_JOBS: HOOK, LZ_DAILY_IMPORT_SHADOW: 'on' }, dataDir: dataDirOf(p), log: (s) => logs.push(s) });
  assert.deepEqual([r.ok, bad(r)], [true, []]);
  assert.ok(!logs.join('\n').includes('SECRET'), '送り先の値は出さない');
  assert.ok(logs.some((s) => /LZ_DAILY_IMPORT_SHADOW=on が残っている/.test(s)));
  for (const hook of [undefined, 'x', 'http://chat.googleapis.com/v1/x', 'https://localhost/x', 'https://a b.example/x']) {
    r = await run(portal(), 'ready', { env: { LZ_DAILY_IMPORT: 'on', ...(hook === undefined ? {} : { GCHAT_WEBHOOK_JOBS: hook }) } });
    assert.deepEqual(bad(r), ['要対応スペースの送り先 GCHAT_WEBHOOK_JOBS が使える (本番と同じ判定)'], String(hook));
  }
  r = await run(portal(), 'ready', { env: { GCHAT_WEBHOOK_JOBS: HOOK } });
  assert.deepEqual(bad(r), ['miniPC の毎晩の本番 LZ_DAILY_IMPORT = on']);
});

await ta('[8] rollback (GAS への戻しの後): 止め・旗 off・毎晩の本番 off・戻しの固定の版の期限の内 (Render の日付・その日まで) = ok / 期限の後・台帳に無い・旗 on のまま・毎晩の本番 on = ❌ / 鍵の要求も readiness も送らない', async () => {
  setV4(false);
  const { JOBS_REGISTRY } = await import('../config/jobs-registry.mjs');
  const rb = JOBS_REGISTRY.find((e) => e.id === 'lz-gas-rollback');
  const reg = (remove_by) => [{ ...rb, remove_by }];
  const LIVE = reg('2030-12-31');   // 試験の日付 (2030) で期限の内
  let p = portal();
  let r = await run(p, 'rollback', { registry: LIVE });
  assert.deepEqual([r.ok, names(p)], [true, ['status']]);
  r = await run(portal(), 'rollback');
  assert.deepEqual(bad(r), [`戻しの固定の版の期限の内 (${rb.remove_by} まで・tag lz-gas-rollback-20260930)`], '本物の台帳の期限 (2026) は 2030 には過ぎている');
  r = await run(portal(), 'rollback', { registry: reg('2030-01-17') });
  assert.equal(r.ok, true, '期限の日は使える');
  r = await run(portal(), 'rollback', { registry: reg('2030-01-16') });
  assert.deepEqual(bad(r), ['戻しの固定の版の期限の内 (2030-01-16 まで・tag lz-gas-rollback-20260930)']);
  r = await run(portal(), 'rollback', { registry: [] });
  assert.deepEqual(bad(r), ['戻しの固定の版 (台帳 lz-gas-rollback) がある']);
  setV4(true);
  r = await run(portal(), 'rollback', { env: { LZ_DAILY_IMPORT: 'on' }, registry: LIVE });
  assert.deepEqual(bad(r), ['ポータルの旗 LZ_MANUAL_V4 = off (読み戻し)', 'miniPC の毎晩の本番 LZ_DAILY_IMPORT = off']);
  setV4(false);
  r = await run(portal({ halted: false }), 'rollback', { registry: LIVE });
  assert.deepEqual(bad(r), ['自動の取込を止めてある (halt)']);
});

await ta('[9] 届かない = ❌ で終わる (ほかの口を呼ばない) / 知らない --expect = 投げる / CLI: 知らない段階 = exit 1 (ポータルに行く前)', async () => {
  const p = portal({ hooks: { status: () => { throw Object.assign(new Error('ポータルの口に届かない'), { code: 'unreachable' }); } } });
  const r = await run(p, 'cutover');
  assert.deepEqual([r.ok, bad(r), names(p)], [false, ['ポータルの状態を読む'], ['status']]);
  await assert.rejects(() => C.cutoverCheck({ expect: 'go', client: p.client }), /--expect は before \/ cutover \/ ready \/ rollback/);
  const c = spawnSync(process.execPath, [path.join(ROOT, 'scripts', 'logizard-import', 'lz-cutover-check.mjs'), '--expect', 'go'], { encoding: 'utf8', timeout: 60000, env: { ...process.env, LZ_LOCK_TOKEN: '' } });
  assert.equal(c.status, 1, c.stdout + c.stderr);
  assert.match(c.stdout, /--expect before\|cutover\|ready\|rollback が要る/);
});

await ta('[10] 戻しの版 (台帳 lz-gas-rollback・N9): commit の 11 ファイルのチェックアウトの形の sha256・manifest の sha256・streamdeck の一覧と同じ / その版の auto-barcode.js に ③ がある (戻す意味がある) / tag が commit を指す (commit・tag が無い = 失敗 = git fetch origin tag を)', async () => {
  const { JOBS_REGISTRY } = await import('../config/jobs-registry.mjs');
  const rb = JOBS_REGISTRY.find((e) => e.id === 'lz-gas-rollback').rollback;
  const git = (...a) => spawnSync('git', a, { cwd: ROOT, encoding: 'buffer', maxBuffer: 64 * 1024 * 1024 });
  assert.equal(git('cat-file', '-e', `${rb.commit}^{commit}`).status, 0, `commit ${rb.commit} がこの clone に無い (git fetch origin tag ${rb.tag})`);
  const show = (p) => { const r = git('show', `${rb.commit}:tools/logizard-automation/${p}`); assert.equal(r.status, 0, p); return r.stdout; };
  const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
  // チェックアウトの形 = .bat は CRLF (.gitattributes の *.bat text eol=crlf)・ほかはそのまま
  const checkout = (p, b) => (p.endsWith('.bat') ? Buffer.from(b.toString('latin1').replace(/\r?\n/g, '\r\n'), 'latin1') : b);
  const manifest = show('manifest.json');
  assert.equal(sha(manifest), rb.manifest_sha256, 'manifest.json');
  assert.deepEqual(Object.keys(rb.files).sort(), [...JSON.parse(manifest.toString('utf8')).pcs[rb.pc]].sort(), '配るのは manifest の streamdeck の全部');
  for (const [p, want] of Object.entries(rb.files)) assert.equal(sha(checkout(p, show(p))), want, p);
  const ab = show('auto-barcode.js').toString('utf8');
  assert.ok(ab.includes("withRelogin('③', () => runImport(IMPORT2_CSV") && ab.includes('デイリー取込商品マスタ'), 'その版には GAS の ③ がある');
  const tag = git('rev-parse', '-q', '--verify', `refs/tags/${rb.tag}^{commit}`);
  assert.equal(tag.status, 0, `tag ${rb.tag} がこの clone に無い (git fetch origin tag ${rb.tag})`);
  assert.equal(tag.stdout.toString().trim(), rb.commit, 'tag は固定の commit を指す');
});

setV4(false);
console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
