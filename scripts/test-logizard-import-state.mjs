/**
 * test-logizard-import-state.mjs — ロジザードの取込の状態 (apps/logizard-import-state・tools/logizard-automation/import-state-*.js。マスタ正本切替 ③c-1b-1)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b 契約 v3」H1・H4・H5・H6):
 *   1 自動の ③ と手の ③ は 1 つの状態と鍵を共用。自動 = halted でなく idle / verified・手の ③ = halted かつ idle / verified
 *   2 鍵が切れても importing / imported_unverified / unknown / partial / verify_failed は戻らない (人の resolve だけ)
 *   3 成功を読んだら imported_unverified → 確かめの成功で verified。verified になるまで次の取込はしない
 *   4 起動したときに importing が残っている (鍵は切れている) = unknown に移す
 *   5 初期化の識別子: ポータルと手元が合わない・片方が無い = 止める。init は 1 回だけ・recover は識別子を作り直す (ポータルが無ければ止めた状態で)
 *   6 解除: note 必須・partial は確かめ (partial_check) か人の直し (repaired) が要る・再開は未解決が無いときだけ
 *   7 出来事は追記だけ / 口は Bearer LZ_LOCK_TOKEN (無ければ 503)・Render だけに立てる
 *   8 (③c-1b-2b K9) 知らせ済みは今の状態と状態を変えた出来事の番号に結ぶ (送っている間に状態が変わった = stale)
 *   9 (③c-1b-2b E) importing には mode と対象の日・nightly は同じ対象の日に 1 回だけ / 前からある表にも列を足す
 * 使い方: node scripts/test-logizard-import-state.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const S = await import('../apps/logizard-import-state/store.js');
const { createImportStateRouter } = await import('../apps/logizard-import-state/router.js');
const C = await import('../tools/logizard-automation/import-state-client.js');
const CLI = await import('../tools/logizard-automation/import-state-cli.js');
const { default: express } = await import('express');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const SHA = 'a'.repeat(64);
const throwsCode = (fn, code) => assert.throws(fn, (e) => e instanceof S.ImportStateError && e.code === code, `期待 = ${code}`);
const T0 = Date.UTC(2030, 0, 15, 15, 20);   // JST 2030-01-16 00:20

console.log('test-logizard-import-state');

await ta('[1] 初期化は 1 回だけ・状態を見る', async () => {
  const db = S.openImportStateDb(':memory:');
  assert.equal(S.getStatus(db).initialized, false);
  throwsCode(() => S.acquire(db, { initId: 'x', holder: 'auto', purpose: 'import', runId: 'lzim_1', by: 'auto', now: T0 }), 'not_initialized');
  const r = S.init(db, { by: '中原', now: T0 });
  assert.match(r.init_id, /^lzi_/);
  throwsCode(() => S.init(db, { by: '中原', now: T0 }), 'already_initialized');
  const s = S.getStatus(db, { now: T0 });
  assert.deepEqual([s.initialized, s.state, s.halted, s.lock, s.events.map((e) => e.kind)], [true, 'idle', false, null, ['init']]);
});

await ta('[2] 自動の取込: 鍵 → importing (sha256・行数が要る) → imported_unverified → verified → 鍵を返す → 次の夜も始められる', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_20300115_a', by: 'auto', now: T0 });
  throwsCode(() => S.transition(db, { lockToken: L.lock_token, runId: 'lzim_20300115_a', to: 'importing', detail: { csv_sha256: 'x', rows: 1, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 }), 'bad_request');
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_20300115_a', to: 'importing', detail: { csv_sha256: SHA, rows: 5006, lz_daily_run_id: 'lzd_x', mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 1000 });
  throwsCode(() => S.transition(db, { lockToken: L.lock_token, runId: 'lzim_20300115_a', to: 'manual_done', by: 'auto', now: T0 }), 'bad_request');   // 成功は imported_unverified だけ (手の ③ も)
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_20300115_a', to: 'imported_unverified', detail: { total: 5006, processed: 5006, errors: 0 }, by: 'auto', now: T0 + 30000 });
  // verified になるまで次の取込はしない
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_other', by: 'auto', now: T0 + 999999 }), 'state');
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_20300115_a', to: 'verified', detail: { rows_ok: 5006 }, by: 'auto', now: T0 + 90000 });
  assert.deepEqual(S.release(db, { lockToken: L.lock_token, by: 'auto', now: T0 + 91000 }), { released: true });
  const s = S.getStatus(db, { now: T0 + 91000 });
  assert.deepEqual([s.state, s.run.run_id, s.run.detail.csv_sha256, s.run.detail.result, s.run.detail.verify, s.lock], ['verified', 'lzim_20300115_a', SHA, 'imported_unverified', 'verified', null]);
  S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_20300116_b', by: 'auto', now: T0 + 86400000 });
});

await ta('[3] 鍵: 生きている間は断る・切れても importing / imported_unverified は戻らない (ほかは取れない)・結果は同じ鍵なら切れていても書ける・違う鍵は断る', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 60, by: 'auto', now: T0 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_b', by: 'auto', now: T0 + 1000 }), 'busy');
  S.extend(db, { lockToken: L.lock_token, ttlSec: 60, now: T0 + 50000 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 51000 });
  // 鍵が切れた後: ほかは取れない (state が importing)・手の ③ も取れない
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_b', by: 'auto', now: T0 + 999999 }), 'state');
  S.halt(db, { by: 'x', reason: '試験で止める', now: T0 + 999999 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'import', runId: 'lzim_m', by: 'm', now: T0 + 999999 }), 'state');
  throwsCode(() => S.extend(db, { lockToken: L.lock_token, now: T0 + 999999 }), 'lock_lost');   // 切れた鍵は延ばせない
  throwsCode(() => S.transition(db, { lockToken: 'other', runId: 'lzim_a', to: 'unknown', by: 'auto', now: T0 + 999999 }), 'lock_lost');
  throwsCode(() => S.transition(db, { lockToken: L.lock_token, runId: 'lzim_zz', to: 'unknown', by: 'auto', now: T0 + 999999 }), 'lock_lost');
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'partial', detail: { total: 2, processed: 1, errors: 1 }, by: 'auto', now: T0 + 999999 });   // 同じ鍵なら切れていても結果は書ける
  assert.equal(S.getStatus(db, { now: T0 + 999999 }).state, 'partial');
  throwsCode(() => S.resume(db, { by: 'x', note: '再開したい', now: T0 + 999999 }), 'state');   // 未解決がある = 再開しない
});

await ta('[4] 起動したときに importing が残っている = unknown (鍵が生きている間はしない)・解除は note と partial の確かめが要る', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 60, by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 });
  throwsCode(() => S.markUnknown(db, { runId: 'lzim_a', by: 'auto', now: T0 + 1000 }), 'busy');
  throwsCode(() => S.markUnknown(db, { runId: 'lzim_b', by: 'auto', now: T0 + 120000 }), 'bad_transition');
  S.markUnknown(db, { runId: 'lzim_a', by: 'auto', reason: 'importing が残っていた', now: T0 + 120000 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_c', by: 'auto', now: T0 + 130000 }), 'state');
  throwsCode(() => S.resolve(db, { runId: 'lzim_a', outcome: 'imported', note: '', by: '中原', now: T0 }), 'bad_request');
  throwsCode(() => S.resolve(db, { runId: 'lzim_x', outcome: 'imported', note: '履歴を見た', by: '中原', now: T0 }), 'run_mismatch');
  throwsCode(() => S.resolve(db, { runId: 'lzim_a', outcome: 'partial', note: '履歴を見た', by: '中原', now: T0 }), 'partial_unchecked');
  S.resolve(db, { runId: 'lzim_a', outcome: 'partial', note: 'インポート履歴 エラー 1', partialCheck: { non_target_unchanged: true, all_in_next_csv: true }, by: '中原', now: T0 + 140000 });
  const s = S.getStatus(db, { now: T0 + 140000 });
  assert.deepEqual([s.state, s.run.detail.resolved.outcome, s.run.detail.resolved.from], ['idle', 'partial', 'unknown']);
  // partial の状態も確かめか直しが要る
  const L2 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_d', by: 'auto', now: T0 + 150000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_d', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 150000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_d', to: 'partial', by: 'auto', now: T0 + 150000 });
  throwsCode(() => S.resolve(db, { runId: 'lzim_d', outcome: 'imported', note: '履歴を見ただけ', by: '中原', now: T0 }), 'partial_unchecked');
  S.resolve(db, { runId: 'lzim_d', outcome: 'imported', note: 'ロジザードで直した', repaired: true, by: '中原', now: T0 + 160000 });
  // 押す前の失敗 = 前の状態に戻る (止めない)
  S.release(db, { lockToken: L2.lock_token, by: 'auto', now: T0 + 160000 });
  const L3 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_e', by: 'auto', now: T0 + 170000 });
  S.transition(db, { lockToken: L3.lock_token, runId: 'lzim_e', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 170000 });
  S.transition(db, { lockToken: L3.lock_token, runId: 'lzim_e', to: 'failed_before_execute', detail: { why: 'プレビューで止まった' }, by: 'auto', now: T0 + 171000 });
  assert.equal(S.getStatus(db, { now: T0 + 171000 }).state, 'idle');
});

await ta('[5] 手の ③ は止めてから (halted)・自動は止めてある間は動かない・手の ③ も確かめ (verified) を通る (止めたまま)・再開は未解決が無いときだけ', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'import', runId: 'lzim_m1', by: 'm', now: T0 }), 'not_halted');
  throwsCode(() => S.halt(db, { by: 'm', reason: '', now: T0 }), 'bad_request');
  S.halt(db, { by: '中原', reason: '自動の取込がおかしいので GAS に戻す', now: T0 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a1', by: 'auto', now: T0 }), 'halted');
  const L = S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'import', runId: 'lzim_m1', by: 'm', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_m1', to: 'importing', detail: { csv_sha256: SHA, rows: 5008, source: 'gas', mode: 'manual', target_as_of: '2030-01-15' }, by: 'm', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_m1', to: 'imported_unverified', detail: { total: 5008, processed: 5008, errors: 0 }, by: 'm', now: T0 + 1000 });
  // 確かめるまで: 次の手の ③ も・再開も・自動の確かめもできない (Codex #1513 R1 High)
  S.release(db, { lockToken: L.lock_token, by: 'm', now: T0 + 1500 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'import', runId: 'lzim_m9', by: 'm', now: T0 + 1600 }), 'state');
  throwsCode(() => S.resume(db, { by: 'x', note: '再開する', now: T0 + 1600 }), 'state');
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'verify', runId: 'lzim_m1', by: 'auto', now: T0 + 1600 }), 'state');   // 取り込んだ側 (手の PC) が確かめる
  const V = S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'verify', runId: 'lzim_m1', by: 'm', now: T0 + 1700 });
  S.transition(db, { lockToken: V.lock_token, runId: 'lzim_m1', to: 'verified', detail: { rows_ok: 5008 }, by: 'm', now: T0 + 1800 });
  let s = S.getStatus(db, { now: T0 + 1800 });
  assert.deepEqual([s.state, s.halted, s.run.by], ['verified', true, 'manual_daily']);
  throwsCode(() => S.resume(db, { by: 'x', note: '再開する', now: T0 + 1800 }), 'busy');   // 鍵がまだ生きている
  S.release(db, { lockToken: V.lock_token, by: 'm', now: T0 + 2000 });
  S.resume(db, { by: '中原', note: '自動を直したので再開', now: T0 + 3000 });
  s = S.getStatus(db, { now: T0 + 3000 });
  assert.deepEqual([s.halted, s.halted_reason], [false, null]);
  // 手の ③ の結果が分からない = 自動も手も止まる (解除まで)
  S.halt(db, { by: '中原', reason: 'もう一度 GAS で', now: T0 + 4000 });
  const L2 = S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'import', runId: 'lzim_m2', by: 'm', now: T0 + 4000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_m2', to: 'importing', detail: { csv_sha256: SHA, rows: 5008, mode: 'manual', target_as_of: '2030-01-15' }, by: 'm', now: T0 + 4000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_m2', to: 'unknown', by: 'm', now: T0 + 5000 });
  S.release(db, { lockToken: L2.lock_token, by: 'm', now: T0 + 5000 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'import', runId: 'lzim_m3', by: 'm', now: T0 + 6000 }), 'state');
  throwsCode(() => S.resume(db, { by: 'x', note: '再開する', now: T0 + 6000 }), 'state');
});

await ta('[6] recover: ポータルに状態が無い = 止めた状態で作る / ある = 識別子だけ作り直す・古い識別子は断る', async () => {
  let db = S.openImportStateDb(':memory:');
  throwsCode(() => S.recover(db, { by: 'x', note: '', now: T0 }), 'bad_request');
  let r = S.recover(db, { by: '中原', note: 'ロジザードの履歴を見た', now: T0 });
  let s = S.getStatus(db, { now: T0 });
  assert.deepEqual([r.halted, s.halted, s.state, /recovered/.test(s.halted_reason)], [true, true, 'idle', true]);
  db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  r = S.recover(db, { by: '中原', note: 'miniPC の印が消えた', now: T0 + 1000 });
  assert.notEqual(r.init_id, init_id);
  assert.equal(S.getStatus(db, { now: T0 }).halted, false);
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', by: 'auto', now: T0 }), 'init_mismatch');
  throwsCode(() => S.acquire(db, { initId: null, holder: 'auto', purpose: 'import', runId: 'lzim_a', by: 'auto', now: T0 }), 'init_mismatch');
  S.acquire(db, { initId: r.init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', by: 'auto', now: T0 });
});

await ta('[7] 出来事は追記だけ・形の誤り (holder・purpose・実行 ID・by) は 400', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  assert.throws(() => db.prepare("UPDATE import_events SET kind = 'x'").run(), /追記だけ/);
  assert.throws(() => db.prepare('DELETE FROM import_events').run(), /追記だけ/);
  for (const [o, code] of [[{ holder: 'root' }, 'bad_request'], [{ purpose: 'delete' }, 'bad_request'], [{ runId: '../x' }, 'bad_request'], [{ by: '' }, 'bad_request']]) {
    assert.throws(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', by: 'auto', now: T0, ...o }), (e) => e.code === code && e.status === 400, JSON.stringify(o));
  }
  // 確かめの鍵は、確かめ待ちの回・自動の回だけ
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'verify', runId: 'lzim_a', by: 'auto', now: T0 }), 'state');
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 60, by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'imported_unverified', by: 'auto', now: T0 });
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'verify', runId: 'lzim_b', by: 'auto', now: T0 + 999999 }), 'run_mismatch');
  const V = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'verify', runId: 'lzim_a', by: 'auto', now: T0 + 999999 });   // 08:40 の回で確かめだけ
  S.transition(db, { lockToken: V.lock_token, runId: 'lzim_a', to: 'verify_failed', detail: { mismatched: 3 }, by: 'auto', now: T0 + 999999 });
  const st9 = S.getStatus(db, { now: T0 + 999999 });
  S.markNotified(db, { runId: 'lzim_a', state: 'verify_failed', stateEventId: st9.state_event_id, by: 'auto', now: T0 + 999999 });
  assert.deepEqual([S.getStatus(db, { now: T0 + 999999 }).notified_at, S.getStatus(db, { now: T0 + 999999 }).notified], [T0 + 999999, true]);
  assert.ok(S.getStatus(db, { now: T0 + 999999, events: 100 }).events.length >= 7);
});

// ── 口 (router) と呼び手 (client)・CLI ──
async function withServer(fn, { token = 'tok' } = {}) {
  const db = S.openImportStateDb(':memory:');
  let clock = T0;
  const app = express();
  app.use('/apps/logizard-import-state', createImportStateRouter({ getDb: () => db, now: () => clock, token: () => token }));
  const srv = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  const url = `http://127.0.0.1:${srv.address().port}`;
  try { await fn({ url, db, tick: (ms) => { clock += ms; } }); } finally { await new Promise((r) => srv.close(r)); }
}

await ta('[8] 口: 鍵 (LZ_LOCK_TOKEN) が無い = 503・違う = 401・断りは 409 / 400 / 404 と code・呼び手で一通り', async () => {
  await withServer(async ({ url }) => {
    let res = await fetch(`${url}/apps/logizard-import-state/api/status`);
    assert.equal(res.status, 503);
  }, { token: '' });
  await withServer(async ({ url, tick }) => {
    let res = await fetch(`${url}/apps/logizard-import-state/api/status`, { headers: { Authorization: 'Bearer nope' } });
    assert.equal(res.status, 401);
    const c = C.createImportStateClient({ url, token: 'tok' });
    await assert.rejects(c.acquire({ init_id: 'x', holder: 'auto', purpose: 'import', run_id: 'lzim_a', by: 'auto' }), (e) => e.code === 'not_initialized' && e.status === 404);
    const { init_id } = await c.init({ by: '中原' });
    await assert.rejects(c.init({ by: '中原' }), (e) => e.code === 'already_initialized' && e.status === 409);
    await assert.rejects(c.acquire({ init_id, holder: 'root', purpose: 'import', run_id: 'lzim_a', by: 'auto' }), (e) => e.code === 'bad_request' && e.status === 400);
    const L = await c.acquire({ init_id, holder: 'auto', purpose: 'import', run_id: 'lzim_a', ttl_sec: 60, by: 'auto' });
    await c.transition({ lock_token: L.lock_token, run_id: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 3, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto' });
    tick(120000);
    await c.markUnknown({ run_id: 'lzim_a', by: 'auto', reason: '試験' });
    await c.resolve({ run_id: 'lzim_a', outcome: 'not_imported', note: '履歴に無い', by: '中原' });
    const s = await c.status(5);
    assert.deepEqual([s.state, s.events[0].kind], ['idle', 'resolve']);
    // 64KB を超える本文は読まない
    res = await fetch(`${url}/apps/logizard-import-state/api/halt`, { method: 'POST', headers: { Authorization: 'Bearer tok', 'Content-Type': 'application/json' }, body: JSON.stringify({ by: 'x', reason: 'y'.repeat(70000) }) });
    assert.equal(res.status, 413);
  });
  // 届かない = unreachable・https でない (localhost 以外) は作らない・token が無い
  const c = C.createImportStateClient({ url: 'http://127.0.0.1:9', token: 'tok', timeoutMs: 2000 });
  await assert.rejects(c.status(), (e) => e.code === 'unreachable');
  assert.throws(() => C.createImportStateClient({ url: 'http://example.com', token: 't' }), (e) => e.code === 'bad_url');
  assert.throws(() => C.createImportStateClient({ url: 'https://example.com', token: '' }), (e) => e.code === 'no_token');
});

await ta('[9] 初期化の印の照合: 両方無い・ポータルだけ・手元だけ・食い違い・壊れた = 止める / 合う = 続ける・印は上書きしない', async () => {
  await withServer(async ({ url }) => {
    const c = C.createImportStateClient({ url, token: 'tok' });
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzis-'));
    const local = path.join(dir, 'init.json');
    assert.match((await C.checkInit(c, local)).reason, /まだ初期化していない/);
    C.writeLocalInit(local, { initId: 'lzi_x', by: 'x' });
    assert.match((await C.checkInit(c, local)).reason, /ポータルの状態が無い/);
    fs.unlinkSync(local);
    const { init_id } = await c.init({ by: 'x' });
    assert.match((await C.checkInit(c, local)).reason, /この PC の初期化の印が無い/);
    C.writeLocalInit(local, { initId: init_id, by: 'x' });
    assert.deepEqual([(await C.checkInit(c, local)).ok], [true]);
    assert.throws(() => C.writeLocalInit(local, { initId: 'lzi_y', by: 'x' }), /EEXIST/);   // 上書きしない
    C.writeLocalInit(local, { initId: 'lzi_y', by: 'x', replace: true });
    assert.match((await C.checkInit(c, local)).reason, /識別子が違う/);
    fs.writeFileSync(local, '{"init_id":"bad"}');
    assert.match((await C.checkInit(c, local)).reason, /読めない/);
  });
});

await ta('[10] CLI: init はポータル + この PC の印 (印があれば断る)・adopt は --replace が無ければ上書きしない・recover・halt / resume / resolve --partial-ok', async () => {
  await withServer(async ({ url, tick }) => {
    const c = C.createImportStateClient({ url, token: 'tok' });
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzis-'));
    const mini = path.join(dir, 'mini', 'init.json'), deck = path.join(dir, 'deck', 'init.json');
    const run = (args) => CLI.main(args, { client: c, log: () => {} });
    const r = await run(['init', '--by', '中原', '--local', mini]);
    assert.equal(C.readLocalInit(mini).init_id, r.init_id);
    await assert.rejects(run(['init', '--by', '中原', '--local', mini]), /すでにある/);
    await assert.rejects(run(['adopt', '--by', '中原', '--local', deck]), /--note/);
    await run(['adopt', '--by', '中原', '--local', deck, '--note', 'Stream Deck の PC']);
    assert.equal(C.readLocalInit(deck).init_id, r.init_id);
    await assert.rejects(run(['adopt', '--by', '中原', '--local', deck, '--note', 'x']), /--replace/);
    const rr = await run(['recover', '--by', '中原', '--local', mini, '--note', 'miniPC の DATA_DIR を作り直した']);
    assert.equal(C.readLocalInit(mini).init_id, rr.init_id);
    assert.equal((await C.checkInit(c, deck)).ok, false);   // もう 1 台は adopt --replace するまで止まる
    await run(['adopt', '--by', '中原', '--local', deck, '--note', '作り直しに合わせる', '--replace']);
    assert.equal((await C.checkInit(c, deck)).ok, true);
    await run(['halt', '--by', '中原', '--reason', 'GAS に戻す']);
    assert.equal((await c.status()).halted, true);
    // partial の解除 (--partial-ok)
    const L = await c.acquire({ init_id: rr.init_id, holder: 'manual_daily', purpose: 'import', run_id: 'lzim_m', ttl_sec: 60, by: 'm' });
    await c.transition({ lock_token: L.lock_token, run_id: 'lzim_m', to: 'importing', detail: { csv_sha256: SHA, rows: 3, mode: 'manual', target_as_of: '2030-01-15' }, by: 'm' });
    await c.transition({ lock_token: L.lock_token, run_id: 'lzim_m', to: 'partial', by: 'm' });
    await assert.rejects(run(['resolve', '--by', '中原', '--run', 'lzim_m', '--outcome', 'partial', '--note', '履歴を見た']), (e) => e.code === 'partial_unchecked');
    await run(['resolve', '--by', '中原', '--run', 'lzim_m', '--outcome', 'partial', '--note', '履歴を見た', '--partial-ok']);
    tick(120000);
    await run(['resume', '--by', '中原', '--note', '確かめたので再開']);
    assert.deepEqual([(await c.status()).state, (await c.status()).halted], ['idle', false]);
  });
});

await ta('[11] Render だけに立てる (JOBS_MONITOR_ENABLED の中)・どの body parser よりも前に mount / 写すファイルに client と CLI', async () => {
  const s = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
  const at = s.indexOf("app.use('/apps/logizard-import-state', logizardImportStateRouter);");
  assert.ok(at > 0);
  assert.equal(s.indexOf("app.use('/apps/logizard-import-state'", at + 1), -1, 'ほかの場所で mount しない');
  const guard = s.lastIndexOf("if (process.env.JOBS_MONITOR_ENABLED === '1') {", at);
  assert.ok(guard > 0 && !s.slice(guard, at).includes('}'), 'JOBS_MONITOR_ENABLED の中');
  assert.ok(at < s.indexOf('app.use(express.urlencoded('), 'urlencoded より前');
  assert.ok(at < s.indexOf('return globalJsonParser(req, res, next);'), '共通の JSON より前');
  assert.ok(s.includes("if (normalizedPath.toLowerCase().startsWith('/apps/logizard-import-state')) return next();"));
  const m = JSON.parse(fs.readFileSync(path.join(ROOT, 'tools', 'logizard-automation', 'manifest.json'), 'utf8'));
  for (const pc of ['minipc', 'streamdeck']) for (const f of ['import-state-client.js', 'import-state-cli.js']) assert.ok(m.pcs[pc].includes(f), `${pc}: ${f}`);
});

await ta('[12] 解除・unknown は鍵を消す = 解除した回を古い鍵で再開できない (期限内の鍵でも)・始めるのは期限内の鍵だけ (Codex #1513 R1)', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 600, by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'unknown', by: 'auto', now: T0 + 1000 });
  S.resolve(db, { runId: 'lzim_a', outcome: 'not_imported', note: '履歴に無かった', by: '中原', now: T0 + 2000 });
  throwsCode(() => S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 3000 }), 'lock_lost');
  assert.equal(S.getStatus(db, { now: T0 + 3000 }).run.detail.resolved.outcome, 'not_imported');   // 解除の記録が消えない
  // markUnknown も鍵を消す
  const L2 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_b', ttlSec: 60, by: 'auto', now: T0 + 10000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_b', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 10000 });
  S.markUnknown(db, { runId: 'lzim_b', by: 'auto', now: T0 + 100000 });
  throwsCode(() => S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_b', to: 'imported_unverified', by: 'auto', now: T0 + 100001 }), 'lock_lost');
  S.resolve(db, { runId: 'lzim_b', outcome: 'imported', note: '履歴で全件を見た', by: '中原', now: T0 + 110000 });
  // 期限の切れた鍵では始めない (結果は書ける = [3])
  const L3 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_c', ttlSec: 60, by: 'auto', now: T0 + 200000 });
  throwsCode(() => S.transition(db, { lockToken: L3.lock_token, runId: 'lzim_c', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 261000 }), 'lock_lost');
  assert.equal(S.getStatus(db, { now: T0 + 261000 }).state, 'idle');
});

await ta('[13] recover はまだ始めていない鍵を消す・今の世代の鍵でなければ始めない / 取込の途中の鍵は残す (結果は書ける) (Codex #1513 R1)', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 600, by: 'auto', now: T0 });
  S.recover(db, { by: '中原', note: '手元の印を作り直す', now: T0 + 1000 });
  throwsCode(() => S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 2000 }), 'lock_lost');
  assert.equal(S.getStatus(db, { now: T0 + 2000 }).lock, null);
  // 取込の途中 = 鍵は残る・結果は書ける
  const cur = S.getStatus(db, { now: T0 }).init_id;
  const L2 = S.acquire(db, { initId: cur, holder: 'auto', purpose: 'import', runId: 'lzim_b', ttlSec: 600, by: 'auto', now: T0 + 3000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_b', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 3000 });
  S.recover(db, { by: '中原', note: 'もう一度作り直す', now: T0 + 4000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_b', to: 'imported_unverified', by: 'auto', now: T0 + 5000 });
  assert.equal(S.getStatus(db, { now: T0 + 5000 }).state, 'imported_unverified');
  // 鍵を取った後に世代が変わった (未開始の鍵を消さない経路が将来できても) = 始めない
  const db2 = S.openImportStateDb(':memory:');
  const i2 = S.init(db2, { by: 'x', now: T0 }).init_id;
  const L3 = S.acquire(db2, { initId: i2, holder: 'auto', purpose: 'import', runId: 'lzim_c', ttlSec: 600, by: 'auto', now: T0 });
  db2.prepare("UPDATE import_state SET init_id = 'lzi_other' WHERE id = 1").run();
  throwsCode(() => S.transition(db2, { lockToken: L3.lock_token, runId: 'lzim_c', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 1000 }), 'init_mismatch');
});

await ta('[14] 状態の行が消えて履歴が残っている = init を断る (初回と取り違えない)・recover で止めた状態から (Codex #1513 R1)', async () => {
  const db = S.openImportStateDb(':memory:');
  S.init(db, { by: 'x', now: T0 });
  db.prepare('DELETE FROM import_state').run();
  throwsCode(() => S.init(db, { by: 'x', now: T0 + 1000 }), 'history_exists');
  const r = S.recover(db, { by: '中原', note: 'ロジザードの履歴を確かめた', now: T0 + 2000 });
  assert.equal(r.halted, true);
});

await ta('[15] 口: どの body parser よりも前 = 認証の前に本文を読まない (PUT・フォーム)・断りの文言は決まったもの (本文・内部のパスを返さない) (Codex #1513 R1)', async () => {
  const db = S.openImportStateDb(':memory:');
  const app = express();
  app.use('/apps/logizard-import-state', createImportStateRouter({ getDb: () => db, token: () => 'tok' }));
  app.use(express.urlencoded({ extended: true, limit: '1kb' }));
  app.use(express.json({ limit: '1kb' }));
  const broken = express();
  broken.use('/apps/logizard-import-state', createImportStateRouter({ getDb: () => { throw new Error('open C:/secret/path/logizard-import-state.db'); }, token: () => 'tok' }));
  const listen = (a) => new Promise((resolve) => { const sv = a.listen(0, '127.0.0.1', () => resolve(sv)); });
  const sv = await listen(app), sv2 = await listen(broken);
  const url = `http://127.0.0.1:${sv.address().port}/apps/logizard-import-state`, url2 = `http://127.0.0.1:${sv2.address().port}/apps/logizard-import-state`;
  try {
    let res = await fetch(`${url}/api/halt`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{bad json' });
    assert.equal(res.status, 401);
    res = await fetch(`${url}/api/halt`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: 'a=' + 'x'.repeat(5000) });
    assert.equal(res.status, 401);
    res = await fetch(`${url}/api/halt`, { method: 'POST', headers: { Authorization: 'Bearer tok', 'Content-Type': 'application/json' }, body: 'secret-fragment' });
    let j = await res.json();
    assert.deepEqual([res.status, j.error, JSON.stringify(j).includes('secret')], [400, 'bad_json', false]);
    res = await fetch(`${url2}/api/status`, { headers: { Authorization: 'Bearer tok' } });
    j = await res.json();
    assert.deepEqual([res.status, j.error, JSON.stringify(j).includes('secret')], [500, 'internal', false]);
  } finally { await new Promise((r) => sv.close(r)); await new Promise((r) => sv2.close(r)); }
});

await ta('[16] 1 つの鍵で始めるのは 1 回だけ (完了・押す前の失敗の後も)・古い回の結果の再送は新しい回に当たらない / 断りに送られてきた値を入れない (Codex #1513 R2)', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 600, by: 'auto', now: T0 });
  const start = (lock, runId, now) => S.transition(db, { lockToken: lock.lock_token, runId, to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now });
  start(L, 'lzim_a', T0 + 1000);
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'imported_unverified', by: 'auto', now: T0 + 2000 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'verified', by: 'auto', now: T0 + 3000 });
  throwsCode(() => start(L, 'lzim_a', T0 + 4000), 'lock_used');   // 期限内でも、完了した鍵で同じ回をもう一度始めない
  assert.equal(S.getStatus(db, { now: T0 + 4000 }).state, 'verified');
  S.release(db, { lockToken: L.lock_token, by: 'auto', now: T0 + 5000 });
  // 押す前の失敗の後も、同じ鍵では始めない (取り直す)
  const L2 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_b', ttlSec: 600, by: 'auto', now: T0 + 6000 });
  start(L2, 'lzim_b', T0 + 6000);
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_b', to: 'failed_before_execute', by: 'auto', now: T0 + 7000 });
  throwsCode(() => start(L2, 'lzim_b', T0 + 8000), 'lock_used');
  S.release(db, { lockToken: L2.lock_token, by: 'auto', now: T0 + 9000 });
  // 新しい回 (c) の途中に、古い回 (a) の結果が遅れて届いても当たらない
  const L3 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_c', ttlSec: 600, by: 'auto', now: T0 + 10000 });
  start(L3, 'lzim_c', T0 + 10000);
  throwsCode(() => S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'imported_unverified', by: 'auto', now: T0 + 11000 }), 'lock_lost');
  throwsCode(() => S.transition(db, { lockToken: L3.lock_token, runId: 'lzim_a', to: 'imported_unverified', by: 'auto', now: T0 + 11000 }), 'lock_lost');
  assert.deepEqual([S.getStatus(db, { now: T0 + 11000 }).state, S.getStatus(db, { now: T0 + 11000 }).run.run_id], ['importing', 'lzim_c']);
  // 断りの文言に、送られてきた値を入れない
  const msgOf = (fn) => { try { fn(); } catch (e) { return e.message; } return ''; };
  for (const m of [
    msgOf(() => S.acquire(db, { initId: 'EVIL-init-<x>', holder: 'auto', purpose: 'import', runId: 'lzim_d', by: 'auto', now: T0 + 999999 })),
    msgOf(() => S.transition(db, { lockToken: L3.lock_token, runId: 'lzim_c', to: 'EVIL-to-<x>', by: 'auto', now: T0 + 12000 })),
    msgOf(() => S.acquire(db, { initId: init_id, holder: 'manual_daily', purpose: 'EVIL-p', runId: 'lzim_d', by: 'auto', now: T0 })),
    msgOf(() => S.acquire(db, { initId: init_id, holder: 'EVIL-h', purpose: 'import', runId: 'lzim_d', by: 'auto', now: T0 })),
  ]) { assert.ok(m.length > 0); assert.ok(!m.includes('EVIL'), m); }
});

await ta('[17] 一度始めた実行 ID は二度と使えない = 古い resolve の再送が新しい回の unknown を解除しない (Codex #1513 R3)', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 600, by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_a', to: 'unknown', by: 'auto', now: T0 + 1000 });
  const resolveA = () => S.resolve(db, { runId: 'lzim_a', outcome: 'not_imported', note: '履歴に無かった', by: '中原', now: T0 + 2000 });
  resolveA();
  // 同じ実行 ID で次の回は始められない (鍵を取る時点で断る)
  throwsCode(() => S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_a', ttlSec: 600, by: 'auto', now: T0 + 3000 }), 'run_used');
  // 新しい実行 ID の回 (b) が unknown になった後、a の resolve が再送されても b は解除されない
  const L2 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_b', ttlSec: 600, by: 'auto', now: T0 + 4000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_b', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 4000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_b', to: 'unknown', by: 'auto', now: T0 + 5000 });
  throwsCode(resolveA, 'run_mismatch');
  assert.deepEqual([S.getStatus(db, { now: T0 + 6000 }).state, S.getStatus(db, { now: T0 + 6000 }).run.run_id], ['unknown', 'lzim_b']);
  // 始める時点でも断る (鍵を取った後に、同じ実行 ID が別の鍵で始まっていた)
  const db2 = S.openImportStateDb(':memory:');
  const i2 = S.init(db2, { by: 'x', now: T0 }).init_id;
  const K1 = S.acquire(db2, { initId: i2, holder: 'auto', purpose: 'import', runId: 'lzim_x', ttlSec: 60, by: 'auto', now: T0 });
  S.transition(db2, { lockToken: K1.lock_token, runId: 'lzim_x', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 });
  S.transition(db2, { lockToken: K1.lock_token, runId: 'lzim_x', to: 'failed_before_execute', by: 'auto', now: T0 + 1000 });
  S.release(db2, { lockToken: K1.lock_token, by: 'auto', now: T0 + 1000 });
  throwsCode(() => S.acquire(db2, { initId: i2, holder: 'auto', purpose: 'import', runId: 'lzim_x', ttlSec: 60, by: 'auto', now: T0 + 2000 }), 'run_used');
  db2.prepare('UPDATE import_state SET lock_token = ?, lock_holder = ?, lock_purpose = ?, lock_run_id = ?, lock_expires_at = ?, lock_init_id = ?, lock_started = 0 WHERE id = 1').run('forged', 'auto', 'import', 'lzim_x', T0 + 999999, i2);
  throwsCode(() => S.transition(db2, { lockToken: 'forged', runId: 'lzim_x', to: 'importing', detail: { csv_sha256: SHA, rows: 2, mode: 'test', target_as_of: '2030-01-15' }, by: 'auto', now: T0 + 3000 }), 'run_used');
  assert.throws(() => db2.prepare('DELETE FROM import_runs').run(), /追記だけ/);
});

// ── ③c-1b-2b 契約 v3 ──
const imp = (mode, target = '2030-01-15', extra = {}) => ({ csv_sha256: SHA, rows: 2, mode, target_as_of: target, ...extra });

await ta('[18] 知らせ済みは「今の状態」と「状態を変えた出来事の番号」に結ぶ: 送っている間に状態が変わったら古い知らせは stale・状態が変わるたびに消える (K9)', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_n1', by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_n1', to: 'importing', detail: imp('test'), by: 'auto', now: T0 });
  const t1 = S.transition(db, { lockToken: L.lock_token, runId: 'lzim_n1', to: 'imported_unverified', by: 'auto', now: T0 + 1000 });
  let s = S.getStatus(db, { now: T0 + 1000 });
  assert.deepEqual([s.state, s.state_event_id, s.notified], ['imported_unverified', t1.state_event_id, false]);
  // 未確かめを知らせている間に確かめが失敗 (verify_failed) → 古い知らせの完了は stale = 新しい状態は知らせ済みにならない
  const t2 = S.transition(db, { lockToken: L.lock_token, runId: 'lzim_n1', to: 'verify_failed', detail: { diffs: 1 }, by: 'auto', now: T0 + 2000 });
  assert.ok(t2.state_event_id > t1.state_event_id);
  throwsCode(() => S.markNotified(db, { runId: 'lzim_n1', state: 'imported_unverified', stateEventId: t1.state_event_id, by: 'auto', now: T0 + 3000 }), 'stale');
  throwsCode(() => S.markNotified(db, { runId: 'lzim_n1', state: 'verify_failed', stateEventId: t1.state_event_id, by: 'auto', now: T0 + 3000 }), 'stale');
  throwsCode(() => S.markNotified(db, { runId: 'lzim_n1', state: 'verify_failed', by: 'auto', now: T0 + 3000 }), 'bad_request');
  throwsCode(() => S.markNotified(db, { runId: 'lzim_n1', state: 'unknown', stateEventId: t2.state_event_id, by: 'auto', now: T0 + 3000 }), 'stale');   // 番号は今・状態が違う
  assert.equal(S.getStatus(db, { now: T0 + 3000 }).notified, false);
  S.markNotified(db, { runId: 'lzim_n1', state: 'verify_failed', stateEventId: t2.state_event_id, by: 'auto', now: T0 + 4000 });
  assert.equal(S.getStatus(db, { now: T0 + 4000 }).notified, true);
  // 解除 (状態が変わる) = 知らせ済みは消える / markUnknown も同じ
  const r = S.resolve(db, { runId: 'lzim_n1', outcome: 'imported', note: '履歴を見た', by: '中原', now: T0 + 5000 });
  s = S.getStatus(db, { now: T0 + 5000 });
  assert.deepEqual([s.state, s.state_event_id, s.notified, s.notified_at], ['idle', r.state_event_id, false, null]);
  const L2 = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId: 'lzim_n2', ttlSec: 30, by: 'auto', now: T0 + 6000 });
  S.transition(db, { lockToken: L2.lock_token, runId: 'lzim_n2', to: 'importing', detail: imp('test'), by: 'auto', now: T0 + 6000 });
  const u = S.markUnknown(db, { runId: 'lzim_n2', by: 'auto', reason: '残っていた', now: T0 + 99000 });
  s = S.getStatus(db, { now: T0 + 99000 });
  assert.deepEqual([s.state, s.state_event_id, s.notified], ['unknown', u.state_event_id, false]);
  S.markNotified(db, { runId: 'lzim_n2', state: 'unknown', stateEventId: u.state_event_id, by: 'auto', now: T0 + 99500 });
  assert.equal(S.getStatus(db, { now: T0 + 99500 }).notified, true);
  // 状態の変わらない出来事 (halt・手元の印の recover) は知らせ済みを消さない
  S.halt(db, { by: '中原', reason: '止めて見る', now: T0 + 99600 });
  S.recover(db, { by: '中原', note: '手元の印を作り直す', now: T0 + 99700 });
  assert.equal(S.getStatus(db, { now: T0 + 99700 }).notified, true);
});

await ta('[19] importing には mode (auto = nightly / test・手の ③ = manual) と対象の日が要る・nightly は同じ対象の日に 1 回だけ (resolve の後も)・test は数えない (E)', async () => {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: T0 });
  let n = 0;
  const start = (mode, target, now = T0 + (++n) * 1000) => {
    const runId = `lzim_e${n}`;
    const L = S.acquire(db, { initId: init_id, holder: 'auto', purpose: 'import', runId, ttlSec: 30, by: 'auto', now });
    return { L, runId, go: () => S.transition(db, { lockToken: L.lock_token, runId, to: 'importing', detail: imp(mode, target), by: 'auto', now }) };
  };
  for (const [mode, target] of [[undefined, '2030-01-15'], ['manual', '2030-01-15'], ['nightly', '2030/01/15'], ['nightly', null], ['shadow', '2030-01-15']]) {
    const x = start(mode, target);
    throwsCode(() => x.go(), 'bad_request');
    S.release(db, { lockToken: x.L.lock_token, by: 'auto', now: T0 + n * 1000 });
  }
  const done = (x) => { x.go(); S.transition(db, { lockToken: x.L.lock_token, runId: x.runId, to: 'unknown', by: 'auto', now: T0 + n * 1000 }); S.resolve(db, { runId: x.runId, outcome: 'not_imported', note: '履歴を見た', by: '中原', now: T0 + n * 1000 }); };
  done(start('test', '2030-01-15'));
  done(start('test', '2030-01-15'));   // test は何回でも
  done(start('nightly', '2030-01-15'));   // 1 回目 (unknown → 解除の後も)
  const again = start('nightly', '2030-01-15');
  throwsCode(() => again.go(), 'nightly_done');
  S.release(db, { lockToken: again.L.lock_token, by: 'auto', now: T0 + n * 1000 });
  done(start('nightly', '2030-01-16'));   // 次の対象の日は始められる
  const runs = db.prepare('SELECT run_id, mode, target_as_of FROM import_runs ORDER BY started_at').all();
  assert.deepEqual(runs.map((r) => `${r.mode}:${r.target_as_of}`), ['test:2030-01-15', 'test:2030-01-15', 'nightly:2030-01-15', 'nightly:2030-01-16']);
});

await ta('[20] 前からある表 (Render の今の DB) にも列を足す・足した後も追記だけ', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzis-mig-'));
  const file = path.join(dir, 'old.db');
  const { default: Database } = await import('better-sqlite3');
  const old = new Database(file);
  old.exec(`CREATE TABLE import_state (id INTEGER PRIMARY KEY CHECK (id = 1), init_id TEXT NOT NULL, state TEXT NOT NULL, prev_state TEXT, halted INTEGER NOT NULL DEFAULT 0, halted_reason TEXT, halted_by TEXT, halted_at INTEGER,
    run_id TEXT, run_by TEXT, run_detail TEXT, lock_token TEXT, lock_holder TEXT, lock_purpose TEXT, lock_run_id TEXT, lock_expires_at INTEGER, lock_init_id TEXT, lock_started INTEGER NOT NULL DEFAULT 0, notified_at INTEGER, updated_at INTEGER NOT NULL);
    CREATE TABLE import_events (id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, kind TEXT NOT NULL, run_id TEXT, by TEXT, detail TEXT);
    CREATE TABLE import_runs (run_id TEXT PRIMARY KEY, by TEXT NOT NULL, started_at INTEGER NOT NULL);
    INSERT INTO import_state (id, init_id, state, halted, notified_at, updated_at) VALUES (1, 'lzi_old', 'idle', 0, 5, 1);
    INSERT INTO import_events (at, kind) VALUES (1, 'init');
    INSERT INTO import_runs (run_id, by, started_at) VALUES ('lzim_old', 'auto', 1);`);
  old.close();
  const db = S.openImportStateDb(file);
  const s = S.getStatus(db, { now: T0 });
  assert.deepEqual([s.init_id, s.state, s.state_event_id, s.notified], ['lzi_old', 'idle', null, false]);
  assert.deepEqual(db.prepare('SELECT run_id, mode, target_as_of FROM import_runs').all(), [{ run_id: 'lzim_old', mode: null, target_as_of: null }]);
  assert.throws(() => db.prepare("UPDATE import_runs SET mode = 'nightly'").run(), /追記だけ/);
  const L = S.acquire(db, { initId: 'lzi_old', holder: 'auto', purpose: 'import', runId: 'lzim_new', by: 'auto', now: T0 });
  S.transition(db, { lockToken: L.lock_token, runId: 'lzim_new', to: 'importing', detail: imp('nightly'), by: 'auto', now: T0 });
  db.close();
  const again = S.openImportStateDb(file);   // 2 回開いても列は 1 回だけ足す
  assert.equal(again.prepare("SELECT mode FROM import_runs WHERE run_id = 'lzim_new'").get().mode, 'nightly');
  again.close();
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
