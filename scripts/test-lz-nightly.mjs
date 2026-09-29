/**
 * test-lz-nightly.mjs — ロジザードの毎日の商品マスタの取込の毎晩の本番 (miniPC 側・③c-1b-2b-2a-2) の試験
 *
 * 契約 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b-2 契約 v3」の「2b-2a-2 の合格の条件」:
 *   1〜3 エンジン: 時刻 (Render の時計の窓・締め切り)・毎晩の呼び手の値・決まりが無い = ファイル・鍵・ログイン・知らせが全部ゼロ
 *   4〜6 確かめのやり直し: 鍵を先に取って記録を読む (無い・違う回 = verify_failed)・鍵の延長・締め切りの旗 (書き出しの前)
 *   7    共用の包み: 同じブラウザ・ページ・鍵で 商品 → バーコード → プレビュー → 実行 → 商品 → バーコード・ops と capabilities が一致
 *   8〜15 入口 (runNightly): 知らせの順と予算・窓の外は知らせだけ・済みの印・旗・状態の振り分け・mark-unknown の競合・
 *        確かめのやり直しだけ・対象と成果物・readiness・Render の時計・ping の条件
 *   16   nightlyMain: 送り先 → 決まり → DATA_DIR の順 (決まりが無い = 何もしない)
 * 本物の nightly の決まりを通る端から端までの試験は 2b-2b (決まりが入ってから)。試験のための裏口の決まりは作らない。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

process.env.DAILY_SYNC_RUN_ID = 'ds_test';
process.env.LZ_MANUAL_V4 = 'on';
const { default: iconv } = await import('iconv-lite');
const E = await import('./logizard-import/lz-import-engine.mjs');
const N = await import('./logizard-import/lz-nightly.mjs');
const RS = await import('./logizard-import/lz-real-session.mjs');
const S = await import('../apps/logizard-import-state/store.js');
const G = await import('../tools/logizard-automation/import-guard.js');
const TP = await import('../apps/master-decisions/lz-import-test-plan.mjs');
const { LZ_SHOHIN } = await import('../apps/master-decisions/lz-cdb.mjs');
const { writeEvidence } = await import('../apps/company-db/push/evidence.mjs');

let passed = 0;
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { process.exitCode = 1; console.log(`  NG  ${name}\n      ${String(e && e.stack).split('\n').slice(0, 6).join('\n      ')}`); }
}
const sj = (s) => iconv.encode(s, 'cp932');
const q = (c) => `"${String(c).replace(/"/g, '""')}"`;
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const csvBuf = (header, rows) => sj([header.map(q).join(','), ...rows.map((r) => r.map(q).join(','))].join('\r\n'));
const DAILY = [['A-1', '新しい名前', '新しい名前', '1200', '0007'], ['B-2', 'B', 'B', '0', '0002']];
const dailyBuf = () => csvBuf(['形式/型番', '商品名', 'ふりがな', '仕入単価', '取引先id'], DAILY);
const JST = (s) => Date.parse(`${s}+09:00`);
const MIN = 60000;
const TARGET = '2030-01-16', RUN_DIR = 'lzd_20300116T070000000Z_abcdef';
const NIGHT = JST('2030-01-17T00:20:00'), DAY = JST('2030-01-17T08:40:00');

/** 前の日 (TARGET) の lz-daily の正式な証跡と CSV */
function setupData({ verdict = 'pass', portalOk = true } = {}) {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-'));
  const rel = `lz-daily/${TARGET}/${RUN_DIR}/cdb_logizard_shohinmaster_upload.csv`;
  fs.mkdirSync(path.join(dataDir, path.dirname(rel)), { recursive: true });
  const buf = dailyBuf();
  fs.writeFileSync(path.join(dataDir, rel), buf);
  writeEvidence(dataDir, 'lz-daily', { state: 'complete', version: 'lzd-v3', portal: { ok: portalOk, stored: portalOk }, as_of: TARGET, run_id: RUN_DIR, verdict, deadline: '2030-01-17T01:00:00+09:00', csv: { path: rel, sha256: sha(buf), rows: DAILY.length } },
    { now: new Date('2030-01-15T22:00:00Z'), warn: () => {} });
  return dataDir;
}

/** ポータル = 本物の状態の機械 (メモリの SQLite)・Render の時刻 = clock.now (動かせる)。hooks で競合を差し込む */
function portal({ at = NIGHT, hooks = {} } = {}) {
  const clock = { now: at };
  const now = () => clock.now;
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: now() });
  const calls = [];
  const w = (name, f) => async (b) => { calls.push(name); if (hooks[name]) { const r = await hooks[name](b, { db, now }); if (r !== undefined) return r; } return { ok: true, ...f(b) }; };
  const client = {
    status: async (n = 20) => { calls.push('status'); return { ok: true, ...S.getStatus(db, { now: now(), events: n }) }; },
    outbox: w('outbox', (limit) => ({ outbox: S.outboxPending(db, { limit: Number(limit) || 20 }) })),
    outboxSent: w('outboxSent', (b) => S.outboxMarkSent(db, { id: b.id, by: b.by, now: now() })),
    notified: w('notified', (b) => S.markNotified(db, { runId: b.run_id, state: b.state, stateEventId: b.state_event_id, by: b.by, now: now() })),
    markUnknown: w('markUnknown', (b) => S.markUnknown(db, { runId: b.run_id, by: b.by, reason: b.reason, now: now() })),
    getArtifact: w('getArtifact', (id) => { const a = S.getArtifact(db, { sourceRunId: id }); if (!a) throw new S.ImportStateError('not_found', 'その成果物は無い', 404); return { artifact: a }; }),
    nightlyReadiness: w('nightlyReadiness', (x) => S.nightlyReadiness(db, { sourceRunId: x.source_run_id, csvSha256: x.csv_sha256, rows: x.rows, targetAsOf: x.target_as_of, now: now() })),
    acquire: w('acquire', (b) => S.acquire(db, { initId: b.init_id, holder: b.holder, purpose: b.purpose, runId: b.run_id, ttlSec: b.ttl_sec, by: b.by, now: now() })),
    extend: w('extend', (b) => S.extend(db, { lockToken: b.lock_token, ttlSec: b.ttl_sec, now: now() })),
    release: w('release', (b) => S.release(db, { lockToken: b.lock_token, by: b.by, now: now() })),
    transition: w('transition', (b) => S.transition(db, { lockToken: b.lock_token, runId: b.run_id, to: b.to, detail: b.detail, by: b.by, now: now() })),
  };
  const checkInit = async (c) => { const s = await c.status(5); return { ok: true, reason: null, status: s }; };
  const putArtifact = (o = {}) => { const buf = dailyBuf(); return S.putArtifact(db, { sourceRunId: RUN_DIR, targetAsOf: TARGET, verdict: 'pass', csvBuf: buf, sha256: sha(buf), rows: DAILY.length, by: 'lz-daily', now: now(), ...o }); };
  return { db, init_id, client, checkInit, clock, calls, putArtifact };
}
const nightId = (ms) => `lzim_night_${new Date(ms).toISOString().replace(/[-:.]/g, '').slice(0, 15)}_abcdef`;
/** 状態を作る (毎晩の回を importing まで・その先へ) */
function makeNightly(p, { to = null, ttl = 60 } = {}) {
  const runId = nightId(p.clock.now);
  const exp = S.nightlyClock(p.clock.now).expected_target_as_of;
  const src = `lzd_${exp.replace(/-/g, '')}T070000000Z_abcdef`;
  p.putArtifact({ targetAsOf: exp, sourceRunId: src });
  const L = S.acquire(p.db, { initId: p.init_id, holder: 'auto', purpose: 'import', runId, ttlSec: ttl, by: 'auto', now: p.clock.now });
  const buf = dailyBuf();
  S.transition(p.db, { lockToken: L.lock_token, runId, to: 'importing', detail: { csv_sha256: sha(buf), rows: DAILY.length, mode: 'nightly', target_as_of: exp, source_run_id: src }, by: 'auto', now: p.clock.now });
  if (to) S.transition(p.db, { lockToken: L.lock_token, runId, to, by: 'auto', now: p.clock.now });
  return { runId, L };
}
/** 偽物のエンジン (呼ばれた値を残す) */
function fakeEngine({ importState = 'verified', verifyState = 'verified' } = {}) {
  const calls = { importOne: [], verifyAgain: [] };
  return {
    calls,
    importOneFn: async (o) => { calls.importOne.push(o); return { runId: nightId(o.now.getTime()), state: importState, record: {} }; },
    verifyAgainFn: async (o) => { calls.verifyAgain.push(o); return { runId: o.runId, state: verifyState }; },
  };
}
const noSession = async () => { throw new Error('ロジザードに入った (入らないはず)'); };
const perfClock = () => { const c = { t: 1000 }; return { c, perfNow: () => c.t }; };
function nightlyOpts(p, dataDir, extra = {}) {
  const sent = [];
  const { perfNow } = perfClock();
  return { sent, o: { dataDir, client: p.client, checkInit: p.checkInit, localInitFile: 'x', withSession: noSession, capabilities: { exportBarcodes: true, executeImport: true },
    notify: async (t) => { sent.push(t); return true; }, createGuard: G.createGuard, perfNow, log: () => {}, ...extra } };
}

console.log('test-lz-nightly');

// ───────── エンジン ─────────
await ta('[1] 時刻 (N1・F): nightly = JST [00:15, 00:50) に始める・締め切り = その日の 00:55 − 余白 / test = 00:00〜01:30 の外・締め切り = 次の 00:00 − 余白 / Render の値 (store の NIGHTLY) と同じ', async () => {
  const nl = E.POLICIES.nightly, tp = E.POLICIES.test;
  const at = (s) => JST(`2030-01-17T${s}`);
  assert.deepEqual(['00:14:59.999', '00:15:00.000', '00:49:59.999', '00:50:00.000', '08:40:00'].map((s) => E.startAllowed(nl, 'import', at(s))), [false, true, true, false, false]);
  assert.deepEqual(['00:20:00', '08:40:00'].map((s) => E.startAllowed(nl, 'verify', at(s))), [true, false], '確かめのやり直しも同じ窓 (昼は知らせだけ)');
  assert.deepEqual(['00:20:00', '01:29:59.999', '01:30:00', '12:00:00'].map((s) => E.startAllowed(tp, 'import', at(s))), [false, false, true, true]);
  assert.equal(E.deadlineAt(nl, at('00:20:00'), 60000), at('00:54:00'));
  assert.equal(E.deadlineAt(tp, at('12:00:00'), 60000), JST('2030-01-17T23:59:00'));
  assert.deepEqual(E.NIGHT, S.NIGHTLY, 'miniPC と Render の窓は同じ値');
  assert.equal(E.newRunId(new Date(NIGHT), nl).match(S.NIGHTLY_RUN_RE) !== null, true, 'エンジンの実行 ID の形 = Render が照らす形');
});

await ta('[2] 毎晩の決まりの確かめの列の決まりが無い (2b-2a) = preflight・importOne・verifyAgain が何もせずに断る (鍵・ログイン・ファイル・知らせ = 0)', async () => {
  assert.equal(E.POLICIES.nightly.rules, null);
  assert.throws(() => E.assertPolicyReady(E.POLICIES.nightly), /確かめの列の決まりがまだ無い/);
  assert.throws(() => E.preflight({ policy: E.POLICIES.nightly, now: new Date(NIGHT), capabilities: { exportBarcodes: true } }), /確かめの列の決まりがまだ無い/);
  const p = portal();
  const touched = [];
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-none-'));
  const base = { policy: E.POLICIES.nightly, lzMinRows: 1, runsDir: path.join(dataDir, 'runs'), csvBuf: dailyBuf(), csv: { sha256: sha(dailyBuf()), rows: 2, target_as_of: TARGET, source_run_id: RUN_DIR },
    context: { artifact: { source_run_id: RUN_DIR, target_as_of: TARGET, verdict: 'pass', csv_sha256: sha(dailyBuf()), rows: 2 } }, now: new Date(NIGHT), localInitFile: 'x', client: p.client,
    checkInit: async () => { touched.push('checkInit'); return { ok: true }; }, withSession: async () => { touched.push('session'); }, capabilities: { exportBarcodes: true, executeImport: true },
    notify: async () => { touched.push('notify'); return true; }, createGuard: G.createGuard, log: () => {} };
  await assert.rejects(E.importOne(base), /確かめの列の決まりがまだ無い/);
  await assert.rejects(E.verifyAgain({ ...base, runId: nightId(NIGHT), locateRun: () => { touched.push('locate'); return dataDir; }, context: {} }), /確かめの列の決まりがまだ無い/);
  assert.deepEqual([touched, p.calls, fs.readdirSync(dataDir)], [[], [], []]);
});

await ta('[3] 毎晩の呼び手の値 (CONTEXTS.nightly): 成果物の識別と CSV の識別が全部同じ・判定 pass・余計なキーなし / 直前の一覧に無い・削除の商品 = 押さない / 比べる商品 = CSV の商品だけ', async () => {
  const buf = dailyBuf();
  const csv = { sha256: sha(buf), rows: 2, target_as_of: TARGET, source_run_id: RUN_DIR };
  const art = { source_run_id: RUN_DIR, target_as_of: TARGET, verdict: 'pass', csv_sha256: sha(buf), rows: 2 };
  const imp = (c, o = {}) => E.CONTEXTS.nightly.import(c, { csvBuf: buf, csv: { ...csv, ...o } });
  const ok = imp({ artifact: art });
  assert.deepEqual([ok.tag, ok.extraIds, ok.recExtra, ok.notifyTail], [{}, [], { artifact: RUN_DIR }, `・対象 ${TARGET}`]);
  for (const [name, c, re] of [
    ['成果物なし', {}, /キーは/], ['余計なキー', { artifact: art, plan: {} }, /キーは/], ['成果物に余計なキー', { artifact: { ...art, received_at: 1 } }, /キーは/],
    ['判定 fail', { artifact: { ...art, verdict: 'fail' } }, /pass でない/], ['出どころ違い', { artifact: { ...art, source_run_id: 'lzd_other' } }, /識別がポータルの成果物と違う/],
    ['対象の日違い', { artifact: { ...art, target_as_of: '2030-01-15' } }, /識別が/], ['sha256 違い', { artifact: { ...art, csv_sha256: 'b'.repeat(64) } }, /識別が/], ['行数違い', { artifact: { ...art, rows: 3 } }, /識別が/],
  ]) assert.throws(() => imp(c), re, name);
  const H = LZ_SHOHIN.header, col = (n) => H.indexOf(n);
  const lzOf = (rows) => ({ byId: new Map(rows.map(([id, del]) => { const c = H.map(() => ''); c[col('商品ID')] = id; c[col('削除フラグ')] = del; return [id, { cells: c, deleted: del }]; })) });
  assert.equal(ok.preCheck(lzOf([['A-1', '0'], ['B-2', '0']])), null);
  const miss = ok.preCheck(lzOf([['A-1', '0']]));
  assert.deepEqual([miss.stage[0], miss.stage[1].missing, /押さない \(L-7\)/.test(miss.error)], ['precheck_failed', ['B-2'], true]);
  assert.deepEqual(ok.preCheck(lzOf([['A-1', '0'], ['B-2', '1']])).stage[1].deleted, ['B-2']);
  assert.deepEqual(E.CONTEXTS.nightly.verify({}).readExtraIds(), []);
  assert.throws(() => E.CONTEXTS.nightly.verify({ readPlan: () => ({}) }), /キーは/);
  // 影の包み (押す部品なし) では取り込まない
  assert.throws(() => E.preflight({ policy: E.POLICIES.test, now: new Date(DAY), occupancy: '倉庫は使っていない (確認)', capabilities: { exportBarcodes: true, executeImport: false } }), /押す部品の無い包み/);
});

// ───────── 確かめのやり直し (試験の決まりで = 決まりの通る道。毎晩も同じ関数) ─────────
function unverifiedTest(p, { at }) {
  const runId = `lzim_test_${new Date(at).toISOString().replace(/[-:.]/g, '').slice(0, 15)}_abcdef`;
  const L = S.acquire(p.db, { initId: p.init_id, holder: 'auto', purpose: 'import', runId, by: 'x', now: at });
  S.transition(p.db, { lockToken: L.lock_token, runId, to: 'importing', detail: { mode: 'test', target_as_of: TARGET, csv_sha256: sha(dailyBuf()), rows: 2, source_run_id: RUN_DIR }, by: 'x', now: at });
  S.transition(p.db, { lockToken: L.lock_token, runId, to: 'imported_unverified', by: 'x', now: at });
  S.release(p.db, { lockToken: L.lock_token, by: 'x', now: at });
  return runId;
}
const PLAN_BODY = { groups: [{ ids: ['A-1'] }] };
const PLAN_REC = { plan_id: 'lzt_x', plan_sha256: TP.planSha256(PLAN_BODY) };
const verifyBase = (p, extra = {}) => ({ policy: E.POLICIES.test, lzMinRows: 1, context: { readPlan: () => ({ plan_id: 'lzt_x', ...PLAN_BODY }) }, occupancy: '倉庫は使っていない (中原さん確認)', localInitFile: 'x', client: p.client, checkInit: p.checkInit,
  capabilities: { exportBarcodes: true }, notify: async () => true, createGuard: G.createGuard, log: () => {}, ...extra });

await ta('[4] 確かめのやり直し = 鍵を取ってから記録を読む: 記録が無い = verify_failed (evidence_missing)・ロジザードに入らない・鍵は返す / 記録の場所が分からない (locateRun の例外) も同じ', async () => {
  for (const locate of [(d) => path.join(d, 'nothing-here'), () => { throw new Error('実行 ID の記録が 0 個'); }]) {
    const at = JST('2030-01-17T12:00:00');
    const p = portal({ at });
    const runId = unverifiedTest(p, { at });
    const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-ev-'));
    const touched = [];
    const r = await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), locateRun: () => locate(dataDir), withSession: async () => { touched.push('session'); } });
    const st = S.getStatus(p.db, { now: at });
    assert.deepEqual([r.state, r.reason, st.state, st.run.detail.verify_detail.reason, st.lock, touched], ['verify_failed', 'evidence_missing', 'verify_failed', 'evidence_missing', null, []]);
    assert.ok(p.calls.indexOf('acquire') >= 0 && p.calls.indexOf('acquire') < p.calls.indexOf('transition'), '鍵を取ってから状態を書く');
  }
});

await ta('[5] 確かめのやり直しも鍵を延ばす (30 秒ごと・書き出しが長くても鍵を持つ)・延ばせない (鍵を失った) = 次の書き出しを始めない = 未確かめのまま', async () => {
  const at = JST('2030-01-17T12:00:00');
  // 記録 (import.json・import.csv・pre.csv・pre-barcode.csv) をそろえる = 試験のランナーの回と同じ形
  const mkRun = (p, runId) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-run-'));
    const H = LZ_SHOHIN.header;
    const cells = (id) => H.map((h) => (h === '商品ID' ? id : h === '削除フラグ' ? '0' : `${h}-${id}`));
    const pre = csvBuf(H, [cells('A-1'), cells('B-2'), cells('C-3')]);
    const preBc = csvBuf(['商品ID', '商品名', 'バーコード'], [['A-1', 'a', '4900000000001'], ['B-2', 'b', '4900000000002'], ['C-3', 'c', '4900000000003']]);
    fs.writeFileSync(path.join(dir, 'import.csv'), dailyBuf()); fs.writeFileSync(path.join(dir, 'pre.csv'), pre); fs.writeFileSync(path.join(dir, 'pre-barcode.csv'), preBc);
    fs.writeFileSync(path.join(dir, 'import.json'), JSON.stringify({ run_id: runId, mode: 'test', ...PLAN_REC, stages: [], files: { import_csv: sha(dailyBuf()), pre: sha(pre), pre_barcode: sha(preBc) } }));
    return { dir, pre, preBc };
  };
  {
    const p = portal({ at });
    const runId = unverifiedTest(p, { at });
    const { dir, pre, preBc } = mkRun(p, runId);
    let slowDone;
    const ops = { exportShohin: async () => { await new Promise((r) => { slowDone = r; setTimeout(r, 250); }); return { buf: pre }; }, exportBarcodes: async () => ({ buf: preBc }) };
    await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), heartbeatMs: 50, locateRun: () => dir, withSession: (fn) => fn(ops) });
    assert.ok(p.calls.filter((c) => c === 'extend').length >= 2, `延ばした回数 = ${p.calls.filter((c) => c === 'extend').length}`);
    void slowDone;
  }
  {
    // 延長を断られた (鍵を失った) = 次の書き出しを始めない
    const p = portal({ at, hooks: { extend: async () => { throw new S.ImportStateError('lock_lost', '鍵が切れた・ほかに移った', 409); } } });
    const runId = unverifiedTest(p, { at });
    const { dir, pre } = mkRun(p, runId);
    const called = [];
    const ops = { exportShohin: async () => { called.push('shohin'); await new Promise((r) => setTimeout(r, 200)); return { buf: pre }; }, exportBarcodes: async () => { called.push('barcode'); return { buf: Buffer.alloc(0) }; } };
    const r = await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), heartbeatMs: 50, locateRun: () => dir, withSession: (fn) => fn(ops) });
    assert.deepEqual([r.state, r.reason, called], ['imported_unverified', 'stopped_lock_lost', ['shohin']]);
    assert.equal(S.getStatus(p.db, { now: at }).state, 'imported_unverified');
  }
});

await ta('[6] 確かめのやり直しの締め切りの旗: 締め切り (test = 次の 00:00 − 余白) を過ぎていたら書き出しを始めない = 未確かめのまま (ロジザードに触らない)', async () => {
  const at = JST('2030-01-17T23:58:30');   // 次の 00:00 − 60 秒 − 旗の余白 5 秒 = 23:58:55 の前だが、鍵の期限の写しの余白 20 秒で 23:58:30 + 300 − 20 と 23:59:00 の早い方 = 23:59:00 → 旗の余白で 23:58:55
  const p = portal({ at });
  const runId = unverifiedTest(p, { at: JST('2030-01-17T12:00:00') });
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-dl-'));
  fs.writeFileSync(path.join(dir, 'import.csv'), dailyBuf()); fs.writeFileSync(path.join(dir, 'pre.csv'), Buffer.from('x')); fs.writeFileSync(path.join(dir, 'pre-barcode.csv'), Buffer.from('y'));
  fs.writeFileSync(path.join(dir, 'import.json'), JSON.stringify({ run_id: runId, mode: 'test', ...PLAN_REC, stages: [], files: { import_csv: sha(dailyBuf()), pre: sha(Buffer.from('x')), pre_barcode: sha(Buffer.from('y')) } }));
  const touched = [];
  // 締め切りの手前で鍵を取り、セッションを始める前に時間が過ぎた (締め切りの旗の余白の内) = 始めない
  const r = await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), nightMarginMs: 95000, locateRun: () => dir, withSession: async () => { touched.push('session'); } });
  assert.deepEqual([r.state, r.reason, touched, S.getStatus(p.db, { now: at }).state, S.getStatus(p.db, { now: at }).lock], ['imported_unverified', 'stopped_deadline', [], 'imported_unverified', null]);
});

// ───────── 共用の包み ─────────
await ta('[7] 共用の包み (C): 1 つの鍵・1 つのブラウザとページの中で全部の操作 / allowExecute = false (影) = executeImport を渡さない / barcode-export.js が無い = exportBarcodes を渡さない / capabilities = 本当の ops / allowExecute を決めないと断る', async () => {
  const mk = ({ barcode }) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-rs-'));
    fs.writeFileSync(path.join(dir, 'logizard-common.js'), `export const calls = globalThis.__rs = { list: [], pages: [] };
export function loadEnv() { process.env.LOGIZARD_USER_ID = 'u'; process.env.LOGIZARD_PASSWORD = 'p'; }
export function assertLocalWriteDirs() {}
export function acquireLock(o) { calls.list.push('lock:' + o.name); }
export function releaseLock() { calls.list.push('unlock'); }
export async function launchBrowser() { calls.list.push('launch'); return { browser: { close: async () => calls.list.push('close') }, page: { id: 'P1' } }; }
export async function login(page, o) { calls.list.push('login:' + o.label); calls.pages.push(page); }
`);
    fs.writeFileSync(path.join(dir, 'shohin-export.js'), 'export async function exportShohinMaster(page) { globalThis.__rs.pages.push(page); globalThis.__rs.list.push("shohin"); return { buf: Buffer.from("s") }; }\n');
    fs.writeFileSync(path.join(dir, 'lz-import-screen.js'), 'export async function previewImport(page) { globalThis.__rs.pages.push(page); globalThis.__rs.list.push("preview"); return {}; }\nexport async function executeImport(page) { globalThis.__rs.pages.push(page); globalThis.__rs.list.push("execute"); return {}; }\n');
    if (barcode) fs.writeFileSync(path.join(dir, 'barcode-export.js'), 'export async function exportBarcodeMaster(page) { globalThis.__rs.pages.push(page); globalThis.__rs.list.push("barcode"); return { buf: Buffer.from("b") }; }\n');
    return dir;
  };
  assert.throws(() => RS.realSession({ automationDir: mk({ barcode: true }), label: 'x' }), /allowExecute/);
  // 押す (試験・毎晩): 商品 → バーコード → プレビュー → 実行 → 商品 → バーコード を同じページで
  const full = RS.realSession({ automationDir: mk({ barcode: true }), label: '毎日の商品マスタの取込 (毎晩)', allowExecute: true });
  assert.deepEqual(full.capabilities, { exportBarcodes: true, executeImport: true });
  const keys = await full.withSession(async (ops) => { await ops.exportShohin(); await ops.exportBarcodes(); await ops.previewImport('x.csv'); await ops.executeImport({}); await ops.exportShohin(); await ops.exportBarcodes(); return Object.keys(ops).sort(); });
  assert.deepEqual(keys, ['executeImport', 'exportBarcodes', 'exportShohin', 'previewImport']);
  assert.deepEqual(globalThis.__rs.list, ['lock:logizard-session.lock', 'launch', 'login:毎日の商品マスタの取込 (毎晩)', 'shohin', 'barcode', 'preview', 'execute', 'shohin', 'barcode', 'close', 'unlock']);
  assert.ok(globalThis.__rs.pages.every((pg) => pg === globalThis.__rs.pages[0]) && globalThis.__rs.pages.length === 7, '同じページ');
  // 影 = 押す部品なし・バーコードの部品が無い = バーコードなし (capabilities も同じ)
  const shadow = RS.realSession({ automationDir: mk({ barcode: false }), label: '影', allowExecute: false });
  assert.deepEqual(shadow.capabilities, { exportBarcodes: false, executeImport: false });
  assert.deepEqual(await shadow.withSession(async (ops) => Object.keys(ops).sort()), ['exportShohin', 'previewImport']);
  // capabilities と ops が一致 (試験の包み・影の包みの本物の形も)
  for (const [allowExecute, barcode] of [[true, false], [false, true]]) {
    const s = RS.realSession({ automationDir: mk({ barcode }), label: 'y', allowExecute });
    const k = await s.withSession(async (ops) => Object.keys(ops));
    assert.deepEqual([k.includes('executeImport'), k.includes('exportBarcodes')], [s.capabilities.executeImport, s.capabilities.exportBarcodes]);
  }
});

// ───────── 入口 (runNightly) ─────────
await ta('[8] 08:40 / 11:45 (Render の時刻で窓の外) = 知らせだけ: 止め・要確認の outbox → 止まった状態 → 再適用待ちの順・ロジザードに入らない・済みの印も ping もなし', async () => {
  const p = portal({ at: JST('2030-01-16T12:00:00') });
  const dataDir = setupData();
  // 再適用待ち (古い) → 止まった取込 (unknown) → 止め
  p.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('pending_reapply', 'pending:1', '再適用待ち 1', 1);
  unverifiedTest(p, { at: p.clock.now });
  const st = S.getStatus(p.db);
  const L = S.acquire(p.db, { initId: p.init_id, holder: 'auto', purpose: 'verify', runId: st.run.run_id, by: 'x', now: p.clock.now });
  S.transition(p.db, { lockToken: L.lock_token, runId: st.run.run_id, to: 'verify_failed', by: 'x', now: p.clock.now });
  S.release(p.db, { lockToken: L.lock_token, by: 'x', now: p.clock.now });
  S.halt(p.db, { by: '中原', reason: '止めの知らせ', now: p.clock.now });
  p.clock.now = DAY;
  const eng = fakeEngine();
  const { sent, o } = nightlyOpts(p, dataDir, eng);
  const r = await N.runNightly(o);
  assert.deepEqual([r.state, r.ping, eng.calls.importOne.length, eng.calls.verifyAgain.length], ['notify_only', null, 0, 0]);
  assert.equal(sent.length, 3);
  assert.match(sent[0], /止めた/); assert.match(sent[1], /止まっている: 状態 verify_failed/); assert.equal(sent[2], '再適用待ち 1');
  assert.deepEqual([S.outboxPending(p.db).length, S.getStatus(p.db).notified, r.notices[0].remaining], [0, true, 0]);
  assert.equal(fs.existsSync(path.join(dataDir, 'lz-import')), false, '済みの印を書かない');
  // もう一度 = 送るものが無い
  sent.length = 0;
  await N.runNightly(o);
  assert.deepEqual(sent, []);
});

await ta('[9] 00:20 (Render の時刻で窓の中) の取込: 対象 = 前の日の lz-daily (合格・送れた)・成果物の識別が同じ・readiness → 済みの印 → importOne (nightly の決まり・直のパス・成果物の識別だけ・Render の時計の now) / verified かつ未送 0 = ok の ping', async () => {
  const p = portal();
  p.putArtifact();
  const dataDir = setupData();
  const eng = fakeEngine();
  const { c, perfNow } = perfClock();
  const { o } = nightlyOpts(p, dataDir, { ...eng, perfNow });
  const orig = o.client.nightlyReadiness;
  o.client.nightlyReadiness = async (x) => { c.t += 7000; return orig(x); };   // readiness の後 = Render の時刻 + 7 秒
  const r = await N.runNightly(o);
  assert.deepEqual([r.state, r.result, r.ping], ['imported', 'verified', 'ok']);
  const a = eng.calls.importOne[0];
  assert.equal(a.policy, E.POLICIES.nightly);
  assert.equal(a.runsDir, path.join(dataDir, 'lz-import', 'runs'));
  assert.deepEqual(a.csv, { sha256: sha(dailyBuf()), rows: 2, target_as_of: TARGET, source_run_id: RUN_DIR });
  assert.deepEqual(a.context, { artifact: { source_run_id: RUN_DIR, target_as_of: TARGET, verdict: 'pass', csv_sha256: sha(dailyBuf()), rows: 2 } });
  assert.equal(a.now.getTime(), NIGHT + 7000, 'now = Render の server_now + 単調な時計の経過 (miniPC の壁時計ではない)');
  assert.ok(a.now.getTime() !== Date.now());
  assert.equal(JSON.parse(fs.readFileSync(N.nightlyMarker(dataDir, '2030-01-17'), 'utf8')).kind, 'import');
  // 同じ夜の 2 回目 = 済み (ポータルにもロジザードにも行かない)
  const r2 = await N.runNightly(o);
  assert.deepEqual([r2.state, r2.ping, eng.calls.importOne.length], ['already', null, 1]);
});

await ta('[10] ping の条件: verified でも未送の知らせが残る = ping しない / verified でない (partial・unknown・verify_failed・imported_unverified) = ping しない', async () => {
  for (const [importState, unsent, want] of [['verified', 30, null], ['partial', 0, null], ['unknown', 0, null], ['imported_unverified', 0, null], ['verify_failed', 0, null], ['verified', 0, 'ok']]) {
    const p = portal();
    p.putArtifact();
    for (let i = 0; i < unsent; i++) p.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('pending_reapply', `p:${i}`, `待ち ${i}`, i);
    const eng = fakeEngine({ importState });
    const { o } = nightlyOpts(p, setupData(), { ...eng, notify: async () => false });   // 送れない = 未送が残る
    const r = await N.runNightly(o);
    assert.deepEqual([r.result, r.ping], [importState, want], `${importState}・未送 ${unsent}`);
  }
});

await ta('[11] 知らせの予算 (N5): 1 回で最大 50 件・60 秒まで・残りの数を数える / 送れない = 送れた印を付けない / 送れたが印の応答が分からない = 未送のまま (次の回に重複してよい)', async () => {
  {
    const p = portal({ at: DAY });
    for (let i = 0; i < 60; i++) p.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('pending_reapply', `p:${i}`, `待ち ${i}`, i);
    const { sent, o } = nightlyOpts(p, setupData());
    const r = await N.runNightly(o);
    assert.deepEqual([sent.length, r.notices[0].skipped_by_budget, r.notices[0].remaining], [50, 10, 10]);
  }
  {
    const p = portal({ at: DAY });
    for (let i = 0; i < 20; i++) p.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('pending_reapply', `p:${i}`, `待ち ${i}`, i);
    const { c, perfNow } = perfClock();
    const { sent, o } = nightlyOpts(p, setupData(), { perfNow });
    o.notify = async (t) => { c.t += 7000; sent.push(t); return true; };   // 1 件 7 秒 = 60 秒で 9 件
    const r = await N.runNightly(o);
    assert.deepEqual([sent.length, r.notices[0].remaining], [9, 11]);
  }
  {
    const p = portal({ at: DAY, hooks: { outboxSent: async () => { throw Object.assign(new Error('fetch failed'), { code: 'unreachable', status: null }); } } });
    S.halt(p.db, { by: '中原', reason: '印の応答が分からない', now: DAY });
    const { sent, o } = nightlyOpts(p, setupData());
    const r = await N.runNightly(o);
    assert.deepEqual([sent.length, r.notices[0].sent, r.notices[0].remaining, S.outboxPending(p.db).length], [1, 1, 1, 1], '未送のまま');
    const p2 = portal({ at: DAY });
    S.halt(p2.db, { by: '中原', reason: '送れない', now: DAY });
    const x = nightlyOpts(p2, setupData());
    x.o.notify = async () => false;
    const r2 = await N.runNightly(x.o);
    assert.deepEqual([r2.notices[0].failed, S.outboxPending(p2.db).length, p2.calls.includes('outboxSent')], [1, 1, false]);
  }
});

await ta('[12] 窓の中の振り分け: 止めてある・手の取込が開いている・unknown / partial / verify_failed・試験の回の imported_unverified = 始めない (ロジザードに入らない・ping しない) / 旗 (LZ_MANUAL_V4) が無い = ❌', async () => {
  const run = async (setup) => {
    const p = portal();
    setup(p);
    const eng = fakeEngine();
    const { o } = nightlyOpts(p, setupData(), eng);
    const r = await N.runNightly(o);
    return { r, eng };
  };
  let x = await run((p) => S.halt(p.db, { by: '中原', reason: '止めておく理由', now: NIGHT }));
  assert.deepEqual([x.r.state, x.r.ping, x.eng.calls.importOne.length], ['halted', null, 0]);
  for (const to of ['unknown', 'partial']) {
    x = await run((p) => makeNightly(p, { to }));
    assert.deepEqual([x.r.state, x.r.reason, x.eng.calls.importOne.length, x.eng.calls.verifyAgain.length], ['stopped', to, 0, 0], to);
  }
  x = await run((p) => unverifiedTest(p, { at: NIGHT }));
  assert.deepEqual([x.r.state, x.r.reason], ['stopped', 'imported_unverified_not_nightly']);
  const prev = process.env.LZ_MANUAL_V4;
  delete process.env.LZ_MANUAL_V4;
  try { await assert.rejects(run(() => {}), /LZ_MANUAL_V4/); } finally { process.env.LZ_MANUAL_V4 = prev; }
});

await ta('[13] 前の夜の毎晩の回が未確かめ = その夜は確かめのやり直しだけ (L-25): 記録 = 実行 ID からの直のパス・Render の時計の now / verified = ok の ping', async () => {
  const p = portal({ at: JST('2030-01-16T00:20:00') });
  const { runId } = makeNightly(p, { to: 'imported_unverified' });
  p.clock.now = NIGHT;
  const dataDir = setupData();
  const eng = fakeEngine({ verifyState: 'verified' });
  const { o } = nightlyOpts(p, dataDir, eng);
  const r = await N.runNightly(o);
  assert.deepEqual([r.state, r.result, r.ping, eng.calls.importOne.length], ['verify_again', 'verified', 'ok', 0]);
  const v = eng.calls.verifyAgain[0];
  assert.deepEqual([v.policy, v.runId, v.locateRun(), v.context, v.now.getTime()], [E.POLICIES.nightly, runId, path.join(dataDir, 'lz-import', 'runs', runId), {}, NIGHT]);
  assert.throws(() => N.nightlyRunDir(dataDir, 'lzim_test_20300116T152000_abcdef'), /形が違う/);
  assert.throws(() => N.nightlyRunDir(dataDir, '../lzim_night_20300116T152000_abcdef'), /形が違う/);
});

await ta('[14] importing が残っている (N7): 鍵が生きている = 動いている (何もしない) / 鍵が無い = mark-unknown → 読み直し → 知らせる / 断られた (busy・鍵が生きていた) = 動いている / 断られた (bad_transition・もう進んだ・解除された) = 報告して終わる (取込に進まない。Codex #1547 R1) / 応答が分からない = 読み直して照らす。どれもロジザードに入らない', async () => {
  const run = async (p, extra = {}) => { const eng = fakeEngine(); const { sent, o } = nightlyOpts(p, setupData(), { ...eng, ...extra }); const r = await N.runNightly(o); return { r, eng, sent }; };
  // 鍵が生きている
  let p = portal();
  makeNightly(p, { ttl: 600 });
  let x = await run(p);
  assert.deepEqual([x.r.state, p.calls.includes('markUnknown')], ['running', false]);
  // 鍵が無い = unknown にして知らせる
  p = portal();
  makeNightly(p, { ttl: 30 });
  p.clock.now += 60000;
  x = await run(p);
  assert.deepEqual([x.r.state, x.r.reason, S.getStatus(p.db, { now: p.clock.now }).state, x.sent.some((t) => /止まっている: 状態 unknown/.test(t))], ['stopped', 'marked_unknown', 'unknown', true]);
  // 断られた (busy) = ほかが鍵を取り直した = 動いている
  p = portal({ hooks: { markUnknown: async (b, { db, now }) => { const s = S.getStatus(db, { now: now() }); db.prepare('UPDATE import_state SET lock_token = ?, lock_expires_at = ? WHERE id = 1').run('other', now() + 600000); void s; throw new S.ImportStateError('busy', 'まだ鍵が生きている', 409); } } });
  makeNightly(p, { ttl: 30 });
  p.clock.now += 60000;
  x = await run(p);
  assert.deepEqual([x.r.state, x.r.mark], ['running', 'refused']);
  // 断られた (bad_transition) = ほかが先に unknown にして解除した (idle) = 報告して終わる (この起動では取込に進まない)
  p = portal({ hooks: { markUnknown: async (b, { db, now }) => { S.markUnknown(db, { runId: b.run_id, by: 'other', now: now() }); S.resolve(db, { runId: b.run_id, outcome: 'not_imported', note: '履歴を見た', by: '中原', now: now() }); throw new S.ImportStateError('bad_transition', 'unknown にできるのは importing の回だけ', 409); } } });
  makeNightly(p, { ttl: 30 });
  p.clock.now += 60000;
  x = await run(p);
  assert.deepEqual([x.r.state, x.r.reason, x.eng.calls.importOne.length, x.eng.calls.verifyAgain.length], ['stopped', 'state_changed_after_mark_unknown', 0, 0]);
  // 応答が分からない (入っていた) = 読み直すと unknown = 止まった
  p = portal({ hooks: { markUnknown: async (b, { db, now }) => { S.markUnknown(db, { runId: b.run_id, by: 'lz-daily-import', now: now() }); throw Object.assign(new Error('fetch failed'), { code: 'unreachable', status: null }); } } });
  makeNightly(p, { ttl: 30 });
  p.clock.now += 60000;
  x = await run(p);
  assert.deepEqual([x.r.state, x.r.reason, x.r.mark], ['stopped', 'marked_unknown', 'unknown']);
  // 応答が分からない (入っていない・鍵も無い) = importing のまま = 照らせない = 止まった (押さない)
  p = portal({ hooks: { markUnknown: async () => { throw Object.assign(new Error('fetch failed'), { code: 'unreachable', status: null }); } } });
  makeNightly(p, { ttl: 30 });
  p.clock.now += 60000;
  x = await run(p);
  assert.deepEqual([x.r.state, x.r.reason, x.r.mark], ['stopped', 'mark_unknown_unconfirmed', 'unknown']);
});

await ta('[15] 対象と成果物: 前の日の証跡が無い・不合格・送れていない = 始めない / ポータルに成果物が無い (404)・識別が違う・判定 fail = 始めない / readiness が断る = 始めない (codes) / 同じ対象の日がもう始まった (nightly_last) = 何もしない / どれもロジザードに入らない・済みの印を書かない', async () => {
  const run = async (p, dataDir, extra = {}) => { const eng = fakeEngine(); const { o } = nightlyOpts(p, dataDir, { ...eng, ...extra }); const r = await N.runNightly(o); return { r, eng }; };
  let p = portal(); p.putArtifact();
  let x = await run(p, fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-empty-')));
  assert.deepEqual([x.r.state, x.r.reason], ['skipped', 'target_no_evidence']);
  x = await run(p, setupData({ verdict: 'fail' }));
  assert.deepEqual(x.r.reason, 'target_not_pass');
  x = await run(p, setupData({ portalOk: false }));
  assert.deepEqual(x.r.reason, 'target_portal_not_stored');
  p = portal();
  x = await run(p, setupData());
  assert.deepEqual([x.r.state, x.r.reason], ['skipped', 'artifact_missing']);
  p = portal(); p.putArtifact({ verdict: 'fail' });
  x = await run(p, setupData());
  assert.deepEqual(x.r.reason, 'artifact_mismatch');
  p = portal(); p.putArtifact();
  const d = setupData();
  const ev = path.join(d, 'lz-daily', TARGET, RUN_DIR, 'cdb_logizard_shohinmaster_upload.csv');
  void ev;
  p = portal({ hooks: { nightlyReadiness: async () => ({ ok: true, ready: false, codes: ['busy'] }) } }); p.putArtifact();
  x = await run(p, setupData());
  assert.deepEqual([x.r.state, x.r.reason, x.r.codes], ['skipped', 'not_ready', ['busy']]);
  // 同じ対象の日がもう始まって解除された (nightly_last) = 何もしない (readiness まで行かない)
  p = portal({ at: JST('2030-01-17T00:16:00') });
  const { runId } = makeNightly(p, { to: 'unknown' });
  S.resolve(p.db, { runId, outcome: 'not_imported', note: '履歴を見た', by: '中原', now: p.clock.now });
  p.clock.now = NIGHT;
  const dd = setupData();
  x = await run(p, dd);
  assert.deepEqual([x.r.state, x.r.runId, p.calls.includes('nightlyReadiness'), x.eng.calls.importOne.length, fs.existsSync(N.nightlyMarker(dd, '2030-01-17'))], ['already_started', runId, false, 0, false]);
});

await ta('[16] nightlyMain: 送り先 (GCHAT_WEBHOOK_JOBS) → 毎晩の確かめの列の決まり → DATA_DIR の順に見る (どれも何もする前)・決まりが無い = 影の項目に fail / summarize の終了コード', async () => {
  let r = await N.nightlyMain({ env: { DATA_DIR: 'x' }, deps: {} });
  assert.deepEqual([r.code, r.ping, r.job, /GCHAT_WEBHOOK_JOBS/.test(r.line)], [1, 'fail', N.JOB_NIGHTLY, true]);
  r = await N.nightlyMain({ env: { GCHAT_WEBHOOK_JOBS: 'https://chat.example.test/h', DATA_DIR: '' }, deps: {} });
  assert.deepEqual([r.code, r.ping, r.job, /確かめの列の決まりがまだ無い/.test(r.line)], [1, 'fail', N.JOB_SHADOW, true], '決まりが無い = DATA_DIR より先に断る (何もしない)');
  assert.deepEqual([N.summarize({ state: 'imported', result: 'verified', ping: 'ok', runId: 'r' }).code, N.summarize({ state: 'notify_only', ping: null }).code, N.summarize({ state: 'running', ping: null }).code,
    N.summarize({ state: 'skipped', reason: 'x', ping: null }).code, N.summarize({ state: 'imported', result: 'partial', ping: null }).code], [0, 0, 0, 3, 3]);
});

await ta('[17] 前の夜の毎晩の回の止まった状態の知らせが知らせ済みにならない (送れない・予算の外) = 確かめのやり直しに進まない・ping しない (故障が隠れない。Codex #1547 R1 High)', async () => {
  // 送れない
  let p = portal({ at: JST('2030-01-16T00:20:00') });
  makeNightly(p, { to: 'imported_unverified' });
  p.clock.now = NIGHT;
  let eng = fakeEngine();
  let x = nightlyOpts(p, setupData(), eng);
  x.o.notify = async () => false;
  let r = await N.runNightly(x.o);
  assert.deepEqual([r.state, r.reason, r.ping, eng.calls.verifyAgain.length, S.getStatus(p.db).state], ['stopped', 'stop_notice_pending', null, 0, 'imported_unverified']);
  // 止め・要確認の outbox が 50 件あって止まった状態の知らせが予算の外
  p = portal({ at: JST('2030-01-16T00:20:00') });
  makeNightly(p, { to: 'imported_unverified' });
  for (let i = 0; i < 50; i++) p.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('halt', `h:${i}`, `止め ${i}`, i);
  p.clock.now = NIGHT;
  eng = fakeEngine();
  x = nightlyOpts(p, setupData(), eng);
  r = await N.runNightly(x.o);
  assert.deepEqual([r.state, r.reason, r.ping, eng.calls.verifyAgain.length, x.sent.length, r.notices[0].stop_notice], ['stopped', 'stop_notice_pending', null, 0, 50, 'budget']);
  // 知らせ済みになっていれば確かめる
  p = portal({ at: JST('2030-01-16T00:20:00') });
  makeNightly(p, { to: 'imported_unverified' });
  p.clock.now = NIGHT;
  eng = fakeEngine();
  x = nightlyOpts(p, setupData(), eng);
  r = await N.runNightly(x.o);
  assert.deepEqual([r.state, r.result, r.ping, eng.calls.verifyAgain.length, x.sent.length], ['verify_again', 'verified', 'ok', 1, 1]);
});

await ta('[18] ping は前後の回を通して: 最初の回で止まった状態を知らせ済みにできなかった (読み直しが食い違っても) = 最後の回の残りが 0 で verified でも ping しない', async () => {
  const p = portal({ at: JST('2030-01-16T00:20:00') });
  makeNightly(p, { to: 'imported_unverified' });
  p.clock.now = NIGHT;
  const eng = fakeEngine();
  const x = nightlyOpts(p, setupData(), eng);
  // 知らせ済みの印の応答が分からない (入っていない) → 最初の回の終わりは知らせ済みでない / 振り分けの読み直しでは知らせ済み (食い違い = ほかの入口が印を付けた)
  let statusCalls = 0;
  const orig = x.o.client.status;
  x.o.client = { ...x.o.client, notified: async () => { throw Object.assign(new Error('fetch failed'), { code: 'unreachable', status: null }); },
    status: async (n) => { const s = await orig(n); statusCalls++; if (statusCalls === 4) { S.markNotified(p.db, { runId: s.run.run_id, state: s.state, stateEventId: s.state_event_id, by: 'other', now: p.clock.now }); return orig(n); } return s; } };
  const r = await N.runNightly(x.o);
  assert.equal(r.notices[0].stop_pending, true, '最初の回は知らせ済みにできなかった');
  assert.deepEqual([r.state, r.result, r.notices[1].remaining, r.ping], ['verify_again', 'verified', 0, null], '最後の回の残りが 0 でも ping しない');
});

await ta('[19] 知らせの予算は 1 回の起動で共通 (前後の送り直し + エンジンの知らせ・50 件 / 60 秒。Codex #1547 R1 Medium) / 窓の中の最初の回は再適用待ちを送らない (止めを先に)', async () => {
  const p = portal();
  p.putArtifact();
  for (let i = 0; i < 49; i++) p.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('manual_review', `m:${i}`, `要確認 ${i}`, i);
  p.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('pending_reapply', 'p:1', '再適用待ち', 100);
  const engineSent = [];
  const eng = fakeEngine();
  const x = nightlyOpts(p, setupData(), { ...eng, importOneFn: async (o) => { engineSent.push(await o.notify('エンジン 1'), await o.notify('エンジン 2')); return { runId: 'r', state: 'verified' }; } });
  const r = await N.runNightly(x.o);
  assert.deepEqual([x.sent.length, engineSent, x.sent.includes('再適用待ち')], [50, [true, false], false], '49 + エンジン 1 = 50 件で打ち止め');
  assert.deepEqual([r.notices[0].sent, r.notices[1].skipped_by_budget, r.ping], [49, 1, null], '再適用待ちは予算の外 = 未送 = ping しない');
  // 時間の予算も前後で共通
  const p2 = portal();
  p2.putArtifact();
  for (let i = 0; i < 5; i++) p2.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('halt', `h:${i}`, `止め ${i}`, i);
  p2.db.prepare('INSERT INTO outbox (kind, dedupe_key, text, created_at) VALUES (?, ?, ?, ?)').run('pending_reapply', 'p:1', '再適用待ち', 100);
  const { c, perfNow } = perfClock();
  const y = nightlyOpts(p2, setupData(), { ...fakeEngine(), perfNow });
  y.o.notify = async (t) => { c.t += 13000; y.sent.push(t); return true; };   // 1 件 13 秒 = 5 件で 65 秒 = 後の回は予算の外
  const r2 = await N.runNightly(y.o);
  assert.deepEqual([y.sent.length, r2.notices[1].skipped_by_budget, r2.ping], [5, 1, null]);
});

await ta('[20] 確かめのやり直しの最後の書き出しの途中で締め切り・鍵を失った = 結果を書かない = 未確かめのまま (Codex #1547 R1 Medium) / 記録の分け方: JSON でない・形が違う = evidence_broken (Codex #1547 R1 Low)', async () => {
  const at = JST('2030-01-17T12:00:00');
  const mkRun = (runId) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-run2-'));
    const H = LZ_SHOHIN.header;
    const cells = (id) => H.map((h) => (h === '商品ID' ? id : h === '削除フラグ' ? '0' : `${h}-${id}`));
    const pre = csvBuf(H, [cells('A-1'), cells('B-2'), cells('C-3')]);
    const preBc = csvBuf(['商品ID', '商品名', 'バーコード'], [['A-1', 'a', '4900000000001'], ['B-2', 'b', '4900000000002'], ['C-3', 'c', '4900000000003']]);
    fs.writeFileSync(path.join(dir, 'import.csv'), dailyBuf()); fs.writeFileSync(path.join(dir, 'pre.csv'), pre); fs.writeFileSync(path.join(dir, 'pre-barcode.csv'), preBc);
    fs.writeFileSync(path.join(dir, 'import.json'), JSON.stringify({ run_id: runId, mode: 'test', ...PLAN_REC, stages: [], files: { import_csv: sha(dailyBuf()), pre: sha(pre), pre_barcode: sha(preBc) } }));
    return { dir, pre, preBc };
  };
  // 締め切り: 始めたときは余白の外 (あと 800 ミリ秒)・最後の書き出しに 1.5 秒 = 結果を書く前に締め切り
  {
    const p = portal({ at });
    const runId = unverifiedTest(p, { at });
    const { dir, pre, preBc } = mkRun(runId);
    const ops = { exportShohin: async () => ({ buf: pre }), exportBarcodes: async () => { await new Promise((res) => setTimeout(res, 1500)); return { buf: preBc }; } };
    const nightMarginMs = (JST('2030-01-18T00:00:00') - at) - 5000 - 800;
    const r = await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), nightMarginMs, heartbeatMs: 60000, locateRun: () => dir, withSession: (fn) => fn(ops) });
    assert.deepEqual([r.state, r.reason, S.getStatus(p.db, { now: at }).state], ['imported_unverified', 'stopped_deadline', 'imported_unverified']);
  }
  // 鍵を失った: 最後の書き出しの途中で延長が断られた
  {
    let n = 0;
    const p = portal({ at, hooks: { extend: async () => { n++; throw new S.ImportStateError('lock_lost', '鍵が切れた・ほかに移った', 409); } } });
    const runId = unverifiedTest(p, { at });
    const { dir, pre, preBc } = mkRun(runId);
    const ops = { exportShohin: async () => ({ buf: pre }), exportBarcodes: async () => { await new Promise((res) => setTimeout(res, 300)); return { buf: preBc }; } };
    const r = await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), heartbeatMs: 50, locateRun: () => dir, withSession: (fn) => fn(ops) });
    assert.deepEqual([r.state, r.reason, S.getStatus(p.db, { now: at }).state, n >= 1], ['imported_unverified', 'stopped_lock_lost', 'imported_unverified', true]);
  }
  // 記録の分け方
  for (const [name, body, want] of [['JSON でない', '{壊れ', 'evidence_broken'], ['形が違う (stages なし)', JSON.stringify({ run_id: 'x', mode: 'test' }), 'evidence_broken'], ['配列', '[]', 'evidence_broken']]) {
    const p = portal({ at });
    const runId = unverifiedTest(p, { at });
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-bad-'));
    fs.writeFileSync(path.join(dir, 'import.json'), body);
    const r = await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), locateRun: () => dir, withSession: async () => { throw new Error('入った'); } });
    assert.deepEqual([r.state, r.reason, S.getStatus(p.db, { now: at }).run.detail.verify_detail.reason], ['verify_failed', want, want], name);
  }
  {
    const p = portal({ at });
    const runId = unverifiedTest(p, { at });
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzn-mm-'));
    fs.writeFileSync(path.join(dir, 'import.json'), JSON.stringify({ run_id: 'lzim_test_other', mode: 'test', stages: [] }));
    const r = await E.verifyAgain({ ...verifyBase(p), runId, now: new Date(at), locateRun: () => dir, withSession: async () => { throw new Error('入った'); } });
    assert.deepEqual([r.state, r.reason], ['verify_failed', 'evidence_mismatch']);
  }
});

await ta('[21] package.json の test:company-db (いつもの試験の組) にこの試験が入っている (Codex #1547 R1 Low)', async () => {
  const pkg = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
  assert.ok(pkg.scripts['test:company-db'].split('&&').map((x) => x.trim()).includes('node scripts/test-lz-nightly.mjs'));
});

await ta('[22] 済みの印は新しい取込を始める門だけ (Codex #1547 R2 Medium): 印があっても 残った importing (鍵なし) は mark-unknown して知らせる (ログインしない) / 前の夜の未確かめは確かめ直す / 取込は始めない', async () => {
  // 印あり + 残った importing (取込の途中でプロセスが落ちた)
  let p = portal();
  makeNightly(p, { ttl: 30 });
  p.clock.now += 60000;
  let dd = setupData();
  fs.mkdirSync(path.dirname(N.nightlyMarker(dd, '2030-01-17')), { recursive: true });
  fs.writeFileSync(N.nightlyMarker(dd, '2030-01-17'), '{}');
  let eng = fakeEngine();
  let x = nightlyOpts(p, dd, eng);
  let r = await N.runNightly(x.o);
  assert.deepEqual([r.state, r.reason, S.getStatus(p.db, { now: p.clock.now }).state, x.sent.some((t) => /状態 unknown/.test(t)), eng.calls.importOne.length], ['stopped', 'marked_unknown', 'unknown', true, 0]);
  // 印あり + 前の夜の毎晩の回が未確かめ (知らせ済み) = 確かめ直す
  p = portal({ at: JST('2030-01-16T00:20:00') });
  makeNightly(p, { to: 'imported_unverified' });
  const s0 = S.getStatus(p.db);
  S.markNotified(p.db, { runId: s0.run.run_id, state: s0.state, stateEventId: s0.state_event_id, by: 'x' });
  p.clock.now = NIGHT;
  dd = setupData();
  fs.mkdirSync(path.dirname(N.nightlyMarker(dd, '2030-01-17')), { recursive: true });
  fs.writeFileSync(N.nightlyMarker(dd, '2030-01-17'), '{}');
  eng = fakeEngine();
  x = nightlyOpts(p, dd, eng);
  r = await N.runNightly(x.o);
  assert.deepEqual([r.state, r.result, eng.calls.verifyAgain.length], ['verify_again', 'verified', 1]);
  // 確かめ直した夜は、その夜の取込を始めない (L-25)
  S.transition(p.db, { lockToken: S.acquire(p.db, { initId: p.init_id, holder: 'auto', purpose: 'verify', runId: s0.run.run_id, by: 'x', now: NIGHT }).lock_token, runId: s0.run.run_id, to: 'verified', by: 'x', now: NIGHT });
  p.putArtifact();
  r = await N.runNightly(x.o);
  assert.deepEqual([r.state, eng.calls.importOne.length], ['already', 0]);
});

await ta('[23] 同時の起動 (Codex #1547 R2 Low): 済みの印がぶつかった (EEXIST) = もう動いている = 失敗にしない・取込は最大 1 回 / 送り先の URL が壊れている = ログインの前に ❌ (ファイル・ポータル・ログイン = 0。Codex #1547 R2 Medium)', async () => {
  // 2 つの起動が同じ夜に同時に: 片方の readiness の間にもう片方が印を書いた
  const p = portal();
  p.putArtifact();
  const dd = setupData();
  const eng = fakeEngine();
  const x = nightlyOpts(p, dd, eng);
  const orig = x.o.client.nightlyReadiness;
  x.o.client = { ...x.o.client, nightlyReadiness: async (q) => { const res = await orig(q); fs.mkdirSync(path.dirname(N.nightlyMarker(dd, '2030-01-17')), { recursive: true }); fs.writeFileSync(N.nightlyMarker(dd, '2030-01-17'), '{"other":1}'); return res; } };
  const r = await N.runNightly(x.o);
  assert.deepEqual([r.state, r.reason, r.ping, eng.calls.importOne.length, N.summarize(r).code], ['already', 'concurrent', null, 0, 0]);
  // 送り先の URL が壊れている
  const { jobsHook } = await import('./logizard-import/notify-jobs.mjs');
  for (const bad of ['https://', 'https://not a url', 'http://chat.googleapis.com/x', 'https://localhost/x', 'ftp://chat.example.test/x', ' ']) {
    assert.equal(jobsHook({ GCHAT_WEBHOOK_JOBS: bad }), null, bad);
    const touched = [];
    const m = await N.nightlyMain({ env: { GCHAT_WEBHOOK_JOBS: bad, DATA_DIR: dd }, deps: { client: new Proxy({}, { get: () => { touched.push('client'); return async () => ({}); } }), session: { withSession: async () => { touched.push('login'); }, capabilities: {} } } });
    assert.deepEqual([m.code, /GCHAT_WEBHOOK_JOBS/.test(m.line), touched], [1, true, []], bad);
  }
  assert.equal(jobsHook({ GCHAT_WEBHOOK_JOBS: 'https://chat.googleapis.com/v1/spaces/AAA/messages?key=k&token=t' }), 'https://chat.googleapis.com/v1/spaces/AAA/messages?key=k&token=t');
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
