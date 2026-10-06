/**
 * test-ne-upload-queue.mjs — NE のアップロードキューの取得 (広げる道 PR-9b。設計 v20 §3.9 の 4・R17 M1・R18 H2/H3・R19 M2/Low)
 *
 * 固定する契約:
 *   1 読むだけ: 呼ぶのは /api_v1_system_que/count と /search だけ。順 = count (前) → 全部のページ (1 回目) → 全部のページ (2 回目) → count (後)
 *   2 条件 = que_creation_date が [DB の ops.ne_reg_queue_window_start(), 始めた時刻] (JST)・que_method_name = SYOHIN_KIHON_CSV。
 *     DB の関数が null = 判定を待つ商品が無い = 1 日。DB を読めない = キューを読まない (完全でない)
 *   3 完全 = 前後の総件数が同じ / 2 回とも 読んだ行 = 総件数・que_id の重なりなし・ページの形・応答の count = 行の数 / 2 回で que_id の集合と行のハッシュが同じ / 版がある
 *   4 行のハッシュ = RFC 8785 (JCS) の canonical JSON の sha256 (値は生の文字・null と "" を分ける・欠けた項目はキーを書かない)
 *   5 10 ページを超える期間は読まない (too_many_pages)
 *   6 回の記録に、取り始めた時点の商品の完了の印 (products_complete_at・rev) を残す。sync は商品・セットの後にキューを読む
 *   7 API の失敗 = failed (throw しない)・始めた印 (running) は最初の API の前に commit・読む関数は最新の回の 2 回目の行・古い回は 14 回だけ残す
 * 使い方: node scripts/test-ne-upload-queue.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ne-upload-queue-'));
process.env.DATA_DIR = tmp;
delete process.env.COMPANY_DB_WATCH_URL;   // 本物の Company DB にはつながない
fs.writeFileSync(path.join(tmp, 'ne-tokens.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quietly = async (fn) => { const l = console.log, w = console.warn, er = console.error; console.log = () => {}; console.warn = () => {}; console.error = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; console.error = er; } };

// NE の API の mock (test-ne-src.mjs と同じ形)。que.rows のうち条件 (期間・機能) に入る行を返す (期間の文字は同じ形 = 文字の大小で比べられる)
const que = { rows: [], calls: [], failSearch: false };
const realFetch = globalThis.fetch;
const match = (q) => que.rows.filter((r) => r.que_creation_date >= q['que_creation_date-gte'] && r.que_creation_date <= q['que_creation_date-lte'] && r.que_method_name === q['que_method_name-eq']);
globalThis.fetch = async (url, opts) => {
  const u = String(url);
  if (!u.startsWith('https://api.next-engine.org')) return realFetch(url, opts);
  const q = Object.fromEntries(new URLSearchParams(opts.body));
  que.calls.push({ path: u.replace('https://api.next-engine.org', ''), q });
  if (u.endsWith('/api_v1_system_que/count')) return { ok: true, status: 200, json: async () => ({ result: 'success', count: String(match(q).length) }) };
  if (u.endsWith('/api_v1_system_que/search')) {
    if (que.failSearch) return { ok: true, status: 200, json: async () => ({ result: 'error', message: 'テストのキューの失敗' }) };
    const d = match(q).slice(Number(q.offset), Number(q.offset) + Number(q.limit));
    return { ok: true, status: 200, json: async () => ({ result: 'success', count: String(d.length), data: d }) };
  }
  throw new Error('知らない API: ' + u);
};
const { fetchNeUploadQueue } = await quietly(() => import('../apps/warehouse/ne-api.js'));
const { initDB, getDB } = await import('../apps/warehouse/db.js');
const { fetchUploadQueue, judgeQueueRun, readLatestUploadQueueRun, readQueueWindowStart, jstText, queueRowHash, queueRowCanonical, WINDOW_START_SQL,
  NE_QUEUE_FIELDS, NE_QUEUE_KEEP_RUNS } = await import('../apps/warehouse/ne-upload-queue.js');
const { computeFetchFingerprint } = await import('../apps/warehouse/ne-fetch-counts.js');
await quietly(() => initDB());
const db = () => getDB();
const runOf = (id) => db().prepare('SELECT * FROM ne_upload_queue_runs WHERE run_id = ?').get(id);
const rowsOf = (id, pass = 2) => db().prepare('SELECT * FROM raw_ne_upload_queue WHERE run_id = ? AND pass = ? ORDER BY row_no').all(id, pass);
const setMeta = (k, v) => db().prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(k, v, 'x');
const readInTx = (fn) => {
  const r = new Database(path.join(tmp, 'warehouse.db'), { readonly: true, fileMustExist: true });
  try { r.exec('BEGIN'); try { return fn(r); } finally { r.exec('COMMIT'); } } finally { r.close(); }
};
const FP = 'a'.repeat(64);
const DAY = 86400000;
const agoJst = (ms) => jstText(new Date(Date.now() - ms));
const atDb = (ms) => async () => ({ source: 'db', start: new Date(Date.now() - ms) });
const qrow = (id, extra = {}) => ({ que_id: String(id), que_method_name: 'SYOHIN_KIHON_CSV', que_upload_name: '商品マスタ CSV アップロード', que_client_file_name: `ne_register_single_x_${id}.csv`,
  que_file_name: `f${id}`, que_status_id: '2', que_message: '', que_deleted_flag: '0', que_creation_date: agoJst(3600000), que_last_modified_date: agoJst(3500000), ...extra });
/** 試験用の callNE (API の応答を順に返す) */
const scripted = (steps) => { const calls = []; const fn = async (ep, params) => { calls.push({ ep, params }); const s = steps.shift(); if (!s) throw new Error('予定に無い呼び出し ' + ep); if (s instanceof Error) throw s; assert.equal(ep, s.ep); return s.res; }; fn.calls = calls; fn.left = steps; return fn; };
const C = (n) => ({ ep: '/api_v1_system_que/count', res: { count: String(n) } });
const S = (rows, count = rows.length) => ({ ep: '/api_v1_system_que/search', res: { count: String(count), data: rows } });
const WS = { source: 'db', start: new Date(Date.now() - 2 * DAY) };

await ta('[1] 本物の ne-api.js の道 (読むだけ): count → search → search → count・条件 = DB の始め〜始めた時刻・SYOHIN_KIHON_CSV だけ・生の文字で 2 回とも残す・商品の完了の印を記録・完全', async () => {
  setMeta('ne_api_products_complete_at', '2026-10-06 00:01:02'); setMeta('ne_api_products_complete_rev', '77');
  setMeta('ne_api_products_fetch_counts', JSON.stringify({ complete_at: '2026-10-06 00:01:02', finished_at: '2026-10-06T00:03:04.567Z' }));
  que.rows = [qrow(101), qrow(102, { que_status_id: '-1', que_message: '失敗しました', que_last_modified_date: null }),
    qrow(103, { que_method_name: 'MALL_SYOHIN_CSV_TO_MASTER' }),     // 別の機能 = 条件の外
    qrow(104, { que_creation_date: agoJst(3 * DAY) }),               // 期間の外
    qrow(105, { que_client_file_name: 'a (2).csv' })];
  delete que.rows[4].que_deleted_flag;   // 欄が欠けた行 = 列は NULL・raw_json に欠けたまま残る
  que.calls = [];
  const r = await quietly(() => fetchNeUploadQueue({ readWindowStart: atDb(2 * DAY) }));
  assert.deepEqual([r.state, r.problems, r.rows], ['complete', [], 3]);
  assert.deepEqual(que.calls.map((c) => c.path.split('/').pop()), ['count', 'search', 'search', 'count']);
  const s = que.calls[1].q;
  assert.deepEqual([s.fields, s.limit, s.offset, s['que_method_name-eq']], [NE_QUEUE_FIELDS.join(','), '1000', '0', 'SYOHIN_KIHON_CSV']);
  for (const c of que.calls) for (const k of ['que_creation_date-gte', 'que_creation_date-lte', 'que_method_name-eq']) assert.equal(c.q[k], s[k], k);   // 4 回とも同じ条件
  const run = runOf(r.run_id);
  assert.deepEqual([run.state, run.window_from, run.window_to, run.window_tz, run.method, run.max_pages, run.page_rows, run.rows_read, run.distinct_que_ids, run.api_count_before, run.api_count_after],
    ['complete', s['que_creation_date-gte'], s['que_creation_date-lte'], 'Asia/Tokyo', 'SYOHIN_KIHON_CSV', 10, '[[3],[3]]', 3, 3, '3', '3']);
  assert.deepEqual([run.window_source, run.products_complete_at, run.products_complete_rev, run.products_finished_at], ['db', '2026-10-06 00:01:02', '77', '2026-10-06T00:03:04.567Z']);
  assert.equal(run.fetch_fingerprint, computeFetchFingerprint());
  const jms = (t) => Date.parse(t.replace(' ', 'T') + '+09:00');
  assert.ok(Math.abs(jms(run.window_to) - jms(run.window_from) - 2 * DAY) < 2000);
  assert.ok(Math.abs(Date.parse(run.started_at) - jms(run.window_to)) < 1000, '期間の終わり = 始めた時刻 (JST)');
  const rows = rowsOf(r.run_id);
  assert.deepEqual(rows.map((x) => [x.que_id, x.que_status_id, x.que_last_modified_date === null, x.que_deleted_flag]),
    [['101', '2', false, '0'], ['102', '-1', true, '0'], ['105', '2', false, null]]);
  assert.deepEqual(JSON.parse(rows[1].raw_json), que.rows[1]);   // null と欠落の区別は raw_json に
  assert.equal('que_deleted_flag' in JSON.parse(rows[2].raw_json), false);
  assert.equal(rows[0].row_hash, queueRowHash(que.rows[0]));
  assert.deepEqual(rowsOf(r.run_id, 1).map((x) => x.que_id), ['101', '102', '105']);   // 1 回目も残る
  const rd = readInTx((rdb) => readLatestUploadQueueRun(rdb));
  assert.deepEqual([rd.complete, rd.run.run_id, rd.rows.length, rd.rows[0].pass], [true, r.run_id, 3, 2]);
  // 件数の記録が別の回 (完了の印と時刻が違う) なら完了の時刻は書かない (null = DB は待つ)
  setMeta('ne_api_products_fetch_counts', JSON.stringify({ complete_at: '2026-10-05 00:01:02', finished_at: '2026-10-05T00:03:04.567Z' }));
  const r2 = await quietly(() => fetchNeUploadQueue({ readWindowStart: atDb(2 * DAY) }));
  assert.equal(runOf(r2.run_id).products_finished_at, null);
});

await ta('[2] 期間の始め (R18 H2): DB の始めが 9 日前 = 発行から 8 日以上後に取り込んだ行も・発行の直後の行も入る / 判定を待つ商品が無い = 1 日 / DB を読めない = キューを読まない (完全でない・API を呼ばない)', async () => {
  const now = new Date('2026-10-06T00:30:00Z');
  const issued = new Date(now.getTime() - 9 * DAY);
  const start = new Date(issued.getTime() - 5 * 60000);   // DB の関数 = issued_at − 5 分
  const rowsAll = [qrow(1, { que_creation_date: jstText(new Date(issued.getTime() + 60000)) }),   // 発行の 1 分後に取り込んだ
    qrow(2, { que_creation_date: jstText(new Date(issued.getTime() + 8.5 * DAY)) }),                // 発行から 8.5 日後に取り込んだ
    qrow(3, { que_creation_date: jstText(new Date(issued.getTime() - 6 * 60000)) })];               // 始めより前 = 期間の外
  const run = async (windowStart) => {
    const calls = [];
    const callNE = async (ep, params) => {
      calls.push(ep);
      const d = rowsAll.filter((r) => r.que_creation_date >= params['que_creation_date-gte'] && r.que_creation_date <= params['que_creation_date-lte']);
      return ep.endsWith('count') ? { count: String(d.length) } : { count: String(d.length), data: d };
    };
    const r = await fetchUploadQueue({ db: db(), callNE, fetchFingerprint: FP, windowStart, now: () => now });
    return { r, calls, run: runOf(r.run_id), ids: rowsOf(r.run_id).map((x) => x.que_id).sort() };
  };
  const a = await run({ source: 'db', start });
  assert.deepEqual([a.r.state, a.run.window_from, a.run.window_to, a.ids], ['complete', jstText(start), '2026-10-06 09:30:00', ['1', '2']]);
  const b = await run({ source: 'none_pending', start: null });
  assert.deepEqual([b.r.state, b.run.window_from, b.run.window_source, b.ids], ['complete', '2026-10-05 09:30:00', 'none_pending', ['2']]);
  const c = await run({ source: 'unavailable', start: null, reason: 'つながらない' });
  assert.deepEqual([c.r.state, c.r.problems, c.calls, c.run.window_from, c.run.window_source, c.run.window_reason], ['incomplete', ['window_start_unavailable'], [], null, 'unavailable', 'つながらない']);
  assert.equal(rowsOf(c.r.run_id).length, 0);   // 読んでいない (この回は完全でない)
  const d = await quietly(() => fetchNeUploadQueue());   // 本物の入口: env が無い = 読めない = API を呼ばない
  assert.deepEqual([d.state, d.problems, runOf(d.run_id).window_reason], ['incomplete', ['window_start_unavailable'], 'no_COMPANY_DB_WATCH_URL']);
  assert.equal(jstText(new Date('2026-10-06T00:30:00Z')), '2026-10-06 09:30:00');
});

await ta('[3] 複数のページ (満杯 → 短い) を 2 回・順 = count → 1 回目の全部 → 2 回目の全部 → count・数の型が数でも文字でも読む', async () => {
  const rows = [1, 2, 3, 4, 5].map((i) => qrow(i));
  const pass = [S(rows.slice(0, 2)), { ep: '/api_v1_system_que/search', res: { count: 2, data: rows.slice(2, 4) } }, S(rows.slice(4))];
  const callNE = scripted([{ ep: '/api_v1_system_que/count', res: { count: 5 } }, ...pass, ...pass, C(5)]);
  const r = await fetchUploadQueue({ db: db(), callNE, fetchFingerprint: FP, windowStart: WS, pageLimit: 2 });
  assert.deepEqual([r.state, r.problems, r.rows], ['complete', [], 5]);
  assert.deepEqual(callNE.calls.map((c) => (c.ep.endsWith('count') ? 'C' : c.params.offset)), ['C', '0', '2', '4', '0', '2', '4', 'C']);
  assert.equal(runOf(r.run_id).page_rows, '[[2,2,1],[2,2,1]]');
});

await ta('[4] 完全でない (理由つき・行は残す): 前後の総件数の違い / 2 回の間の追加・削除・中身の変化 / ページの間の追加 (並びのずれ)・削除 / 総件数と行の数 / 応答の count / que_id の無い行 / 読めない総件数 / 版が無い', async () => {
  const run = async (steps, extra = {}) => {
    const r = await fetchUploadQueue({ db: db(), callNE: scripted(steps), fetchFingerprint: FP, windowStart: WS, pageLimit: 2, ...extra });
    assert.equal(r.state, 'incomplete', JSON.stringify(r));
    assert.deepEqual([runOf(r.run_id).state, JSON.parse(runOf(r.run_id).problems), rowsOf(r.run_id).length], ['incomplete', r.problems, r.rows]);
    const rd = readInTx((rdb) => readLatestUploadQueueRun(rdb));
    assert.deepEqual([rd.complete, rd.reason], [false, 'incomplete']);
    return r.problems;
  };
  const [a, b, c] = [qrow(1), qrow(2), qrow(3)];
  assert.deepEqual(await run([C(1), S([a]), S([a]), C(2)]), ['count_changed']);                              // 読み終えた後に増えた
  assert.deepEqual(await run([C(2), S([a, b]), S([]), S([a, c]), S([]), C(2)]), ['que_ids_differ']);          // 2 回の間に 1 行消えて 1 行増えた (数は同じ)
  assert.deepEqual(await run([C(1), S([{ ...a, que_status_id: '1' }]), S([a]), C(1)]), ['row_hash_differs']); // 2 回の間に状態が変わった (処理中 → 成功)
  assert.deepEqual(await run([C(3), S([a, b]), S([b]), S([a, b]), S([c]), C(3)]), ['pass1_que_id_not_distinct', 'que_ids_differ']);   // 1 回目のページの間に前に 1 行増えた (並びのずれ)
  assert.deepEqual(await run([C(3), S([a, b]), S([]), S([a, b]), S([c]), C(3)]), ['pass1_rows_ne_count', 'que_ids_differ']);           // 1 回目のページの間に 1 行消えた
  assert.deepEqual(await run([C(3), S([a]), S([a]), C(3)]), ['pass1_rows_ne_count', 'pass2_rows_ne_count']);
  assert.deepEqual(await run([C(1), S([a], 2), S([a]), C(1)]), ['pass1_page_0_count_ne_rows']);
  assert.deepEqual(await run([C(1), S([{ ...a, que_id: '' }]), S([{ ...a, que_id: '' }]), C(1)]), ['pass1_row_without_que_id', 'pass2_row_without_que_id']);
  assert.deepEqual(await run([{ ep: '/api_v1_system_que/count', res: { count: 'abc' } }, S([a]), S([a]), C(1)]), ['count_before_unreadable']);
  assert.deepEqual(await run([C(1), S([a]), S([a]), C(1)], { fetchFingerprint: null }), ['no_fetch_fingerprint']);
  assert.deepEqual(await run([C(1), { ep: '/api_v1_system_que/search', res: { count: '0' } }, S([a]), C(1)]), ['pass1_page_0_not_array', 'pass1_rows_ne_count', 'que_ids_differ']);
});

await ta('[5] 10 ページを超える期間は読まない (総件数で分かる = search を呼ばない) / 読んでいる途中で 10 ページを超えても止める', async () => {
  const callNE = scripted([C(21)]);
  const r = await fetchUploadQueue({ db: db(), callNE, fetchFingerprint: FP, windowStart: WS, pageLimit: 2 });
  assert.deepEqual([r.state, r.problems, callNE.calls.length, runOf(r.run_id).api_count_before], ['incomplete', ['too_many_pages'], 1, '21']);
  const full = (k) => S([qrow(`${k}a`), qrow(`${k}b`)]);
  const pages = Array.from({ length: 10 }, (_, i) => full(i));   // 総件数は 20 と言うが、10 ページ目も満杯
  const callNE2 = scripted([C(20), ...pages, ...pages, C(20)]);
  const r2 = await fetchUploadQueue({ db: db(), callNE: callNE2, fetchFingerprint: FP, windowStart: WS, pageLimit: 2 });
  assert.equal(r2.state, 'incomplete');
  assert.ok(r2.problems.includes('too_many_pages'), JSON.stringify(r2.problems));
  assert.equal(callNE2.calls.filter((c) => c.ep.endsWith('search')).length, 20);   // 1 回の読みで 10 ページまで
});

await ta('[6] API の失敗 = failed (行は残さない・throw しない) / 始めた印 (running) は最初の API の前に commit・途中で止まった回は running = 完全でない', async () => {
  let seen = null;
  const callNE = async (ep) => {
    seen = readInTx((rdb) => rdb.prepare('SELECT state FROM ne_upload_queue_runs ORDER BY started_at DESC, run_id DESC LIMIT 1').get());
    if (ep.endsWith('count')) return { count: '1' };
    throw new Error('NE API エラー: テスト');
  };
  const r = await fetchUploadQueue({ db: db(), callNE, fetchFingerprint: FP, windowStart: WS });
  assert.deepEqual([r.state, r.rows, r.error, seen], ['failed', 0, 'NE API エラー: テスト', { state: 'running' }]);
  assert.deepEqual([runOf(r.run_id).state, runOf(r.run_id).error, rowsOf(r.run_id).length, rowsOf(r.run_id, 1).length], ['failed', 'NE API エラー: テスト', 0, 0]);
  assert.equal(readInTx((rdb) => readLatestUploadQueueRun(rdb)).reason, 'failed');
  db().prepare("UPDATE ne_upload_queue_runs SET state = 'running', finished_at = NULL WHERE run_id = ?").run(r.run_id);   // 途中で process が止まった
  assert.deepEqual((({ complete, reason }) => ({ complete, reason }))(readInTx((rdb) => readLatestUploadQueueRun(rdb))), { complete: false, reason: 'running' });
  que.rows = [qrow(1)]; que.failSearch = true;   // ne-api.js の入口も throw しない (商品・受注の取得を止めない)
  try { const x = await quietly(() => fetchNeUploadQueue({ readWindowStart: atDb(DAY) })); assert.equal(x.state, 'failed'); } finally { que.failSearch = false; }
  const y = await quietly(() => fetchNeUploadQueue({ readWindowStart: async () => { throw new Error('想定外'); } }));
  assert.deepEqual([y.state, y.error], ['failed', '想定外']);
});

await ta('[7] 読む関数: 完全な回でも 2 回目の行の数が記録と違えば完全でない / 表が無い DB・回が無い DB', async () => {
  que.rows = [qrow(7), qrow(8)];
  const r = await quietly(() => fetchNeUploadQueue({ readWindowStart: atDb(DAY) }));
  assert.equal(readInTx((rdb) => readLatestUploadQueueRun(rdb)).complete, true);
  db().prepare('DELETE FROM raw_ne_upload_queue WHERE run_id = ? AND pass = 2 AND row_no = 1').run(r.run_id);
  assert.deepEqual((({ complete, reason }) => ({ complete, reason }))(readInTx((rdb) => readLatestUploadQueueRun(rdb))), { complete: false, reason: 'record_mismatch' });
  const empty = new Database(':memory:');
  assert.equal(readLatestUploadQueueRun(empty).reason, 'no_table');
  empty.exec('CREATE TABLE ne_upload_queue_runs (run_id TEXT, started_at TEXT); CREATE TABLE raw_ne_upload_queue (run_id TEXT, pass INTEGER, row_no INTEGER)');
  assert.equal(readLatestUploadQueueRun(empty).reason, 'no_run');
  empty.close();
});

await ta('[8] 古い回は 14 回だけ残す (行も一緒に消える)', async () => {
  que.rows = [qrow(9)];
  const ids = [];
  for (let i = 0; i < NE_QUEUE_KEEP_RUNS + 3; i++) ids.push((await quietly(() => fetchNeUploadQueue({ readWindowStart: atDb(DAY) }))).run_id);
  const left = db().prepare('SELECT run_id FROM ne_upload_queue_runs').all().map((x) => x.run_id);
  assert.equal(left.length, NE_QUEUE_KEEP_RUNS);
  assert.ok(left.includes(ids[ids.length - 1]));
  assert.equal(db().prepare(`SELECT COUNT(*) AS n FROM raw_ne_upload_queue WHERE run_id NOT IN (${left.map(() => '?').join(',')})`).get(...left).n, 0);
});

await ta('[9] judgeQueueRun: ページの形 (途中が満杯でない・最後が満杯・上限を超える)・2 回でない / 版の形', async () => {
  const pg = (ids) => ({ items: ids.map((i) => qrow(i, { que_creation_date: 'x', que_last_modified_date: 'y' })), respCount: String(ids.length) });
  const j = (passes, extra = {}) => judgeQueueRun({ countBefore: 3, countAfter: 3, passes, pageLimit: 2, fetchFingerprint: FP, ...extra });
  const ok = [pg([1, 2]), pg([3])];
  assert.deepEqual(j([ok, ok]), []);
  assert.deepEqual(j([[pg([1]), pg([2, 3])], ok]), ['pass1_page_0_not_full', 'pass1_last_page_not_short']);
  assert.deepEqual(j([ok, [pg([1, 2, 3])]]), ['pass2_page_0_over_limit', 'pass2_last_page_not_short']);
  assert.deepEqual(j([ok]), ['not_two_passes']);
  assert.deepEqual(j([ok, ok], { fetchFingerprint: 'A'.repeat(64) }), ['no_fetch_fingerprint']);
  assert.deepEqual(j([ok, ok], { countAfter: -1 }), ['count_after_unreadable']);
});

await ta('[10] 期間の始めを読む: 偽の query (始め・null・例外・形の違う結果) / URL が無い / 本物の PG (PGlite) で関数を呼ぶ形が通る・関数が無い DB = unavailable', async () => {
  const at = new Date('2026-09-27T01:02:03Z');
  assert.deepEqual(await readQueueWindowStart({ query: async (sql) => { assert.equal(sql, WINDOW_START_SQL); return { rows: [{ window_start: at }] }; } }), { source: 'db', start: at });
  assert.deepEqual(await readQueueWindowStart({ query: async () => ({ rows: [{ window_start: null }] }) }), { source: 'none_pending', start: null });
  assert.deepEqual(await readQueueWindowStart({ query: async () => { throw new Error('つながらない'); } }), { source: 'unavailable', start: null, reason: 'つながらない' });
  assert.deepEqual(await readQueueWindowStart({ query: async () => ({ rows: [] }) }), { source: 'unavailable', start: null, reason: 'unexpected_result' });
  assert.deepEqual(await readQueueWindowStart({ query: async () => ({ rows: [{ window_start: 'x' }] }) }), { source: 'unavailable', start: null, reason: 'start_unreadable' });
  assert.deepEqual(await readQueueWindowStart({ url: '' }), { source: 'unavailable', start: null, reason: 'no_COMPANY_DB_WATCH_URL' });
  const { PGlite } = await import('@electric-sql/pglite');
  const pg = new PGlite();
  try {
    const query = (sql) => pg.query(sql);
    const r0 = await readQueueWindowStart({ query });
    assert.equal(r0.source, 'unavailable');   // 0058 の前の DB (関数が無い) = 読めない = キューを読まない
    // PR-1 の関数の形だけを置く (中身は PR-1 の試験)
    await pg.exec(`create schema ops; create function ops.ne_reg_queue_window_start() returns timestamptz language sql stable as $$ select timestamptz '2026-09-27 10:02:03+09' $$;`);
    const r1 = await readQueueWindowStart({ query });
    assert.deepEqual([r1.source, r1.start.toISOString()], ['db', '2026-09-27T01:02:03.000Z']);
    await pg.exec('create or replace function ops.ne_reg_queue_window_start() returns timestamptz language sql stable as $$ select null::timestamptz $$;');
    assert.deepEqual(await readQueueWindowStart({ query }), { source: 'none_pending', start: null });
  } finally { await pg.close(); }
});

await ta('[11] 行のハッシュ = RFC 8785 の canonical JSON: キーの並びに依らない・null と "" を分ける・数は文字 (1 と "1" は同じ)・欠けた項目は書かない・項目の境目をずらすと違う', async () => {
  assert.equal(queueRowCanonical({ b: '2', a: null, c: 3 }), '{"a":null,"b":"2","c":"3"}');
  assert.equal(queueRowHash({ que_id: '1', que_message: 'x' }), queueRowHash({ que_message: 'x', que_id: '1' }));
  assert.notEqual(queueRowHash({ que_id: '1', que_message: null }), queueRowHash({ que_id: '1', que_message: '' }));
  assert.equal(queueRowHash({ que_id: 1 }), queueRowHash({ que_id: '1' }));
  assert.notEqual(queueRowHash({ que_id: '1', que_message: undefined }), queueRowHash({ que_id: '1', que_message: null }));
  assert.equal(queueRowCanonical({ que_id: '1', que_message: undefined }), '{"que_id":"1"}');
  assert.notEqual(queueRowHash({ a: 'xy', b: 'z' }), queueRowHash({ a: 'x', b: 'yz' }));
  assert.equal(queueRowCanonical({ 'é': '1', e: '2' }), '{"e":"2","é":"1"}');   // UTF-16 の順
});

await ta('[12] ne-api.js: sync は商品・セットの取得の後にキューを読み、受注の前 / キューの取得は書き込みの API を呼ばない (search / count だけ)', async () => {
  const src = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/ne-api.js'), 'utf8');
  const sync = src.slice(src.indexOf("command === 'sync'"));
  const i = (s) => sync.indexOf(s);
  assert.ok(i('await fetchProducts();') > 0 && i('await fetchSetProducts();') > i('await fetchProducts();'));
  assert.ok(i('await fetchNeUploadQueue();') > i('await fetchSetProducts();') && i('await fetchOrders(7);') > i('await fetchNeUploadQueue();'));
  const qsrc = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/ne-upload-queue.js'), 'utf8');
  assert.deepEqual([...new Set(qsrc.match(/'\/api_v1_[a-z_]+\/[a-z_]+'/g))].sort(), ["'/api_v1_system_que/count'", "'/api_v1_system_que/search'"]);
  assert.ok(que.calls.every((c) => /^\/api_v1_system_que\/(count|search)$/.test(c.path)));
});

globalThis.fetch = realFetch;
try { getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
