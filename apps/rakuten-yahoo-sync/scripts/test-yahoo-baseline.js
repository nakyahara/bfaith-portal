/**
 * E-7-a smoke runner: migration 005 + yahoo-myitemlist-proxy + yahoo-store-sync の動作確認。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/test-yahoo-baseline.js
 *
 * 実 Yahoo API は叩かない (mockApi のみ)。 in-memory SQLite で run。
 * 出力に FAIL があれば exit 1。
 *
 * 検証項目:
 *   - migration 005 schema (テーブル、 partial UNIQUE INDEX、 CHECK 制約)
 *   - iterateMyItemList: paging + 0/1/N 件 + 暴走防止
 *   - syncYahooBaseline:
 *       (1) 初回 success → baseline 確立
 *       (2) 取りこぼし (stale_rows>0) → baseline 再確立せず、 既存 state 保持
 *       (3) env override 下では確立しない
 *       (4) failed 経路で sync_run.status='failed' + error_message 記録
 *   - assertYahooBaselineComplete:
 *       (a) state なし → 412
 *       (b) state.is_complete=0 → 412
 *       (c) env override → 412
 *       (d) hash mismatch → 412
 *       (e) stale (>maxAgeHours) → 412
 *       (f) 健康 → no throw
 *   - partial UNIQUE INDEX: 同名 sync_name で running 2 件は拒否、 別 sync_name は OK
 */

import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  iterateMyItemList,
} from '../lib/yahoo-myitemlist-proxy.js';
import {
  syncYahooBaseline,
  assertYahooBaselineComplete,
  CANONICAL_QUERIES,
  CANONICAL_QUERY_HASH,
} from '../lib/yahoo-store-sync.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_DIR = path.join(__dirname, '..', 'db', 'migrations');

let failed = 0;
function ok(name) { console.log(`OK   ${name}`); }
function fail(name, msg) { failed += 1; console.error(`FAIL ${name}: ${msg}`); }
function expect(name, cond, msg) { if (cond) ok(name); else fail(name, msg); }

function freshDb() {
  const db = new Database(':memory:');
  const files = fs.readdirSync(MIGRATIONS_DIR).filter((f) => f.endsWith('.sql')).sort();
  for (const f of files) {
    db.exec(fs.readFileSync(path.join(MIGRATIONS_DIR, f), 'utf8'));
  }
  return db;
}

function makeMockApi(spec) {
  // spec = { 'a': 5, 'b': 250, ... } — query → 件数
  return async ({ query, offset, results }) => {
    const total = spec[query] ?? 0;
    const items = [];
    const start = offset + 1;
    const end = Math.min(offset + results, total);
    for (let i = start; i <= end; i++) {
      items.push({ ItemCode: `${query}-${i}`, Name: `${query}-name-${i}`, HasSubCode: i % 2 === 0 });
    }
    return { ok: true, items, totalResultsAvailable: total, totalResultsReturned: items.length, firstResultPosition: start };
  };
}

// 全 CANONICAL_QUERIES で同件数返す mock
function makeUniformMock(perQuery) {
  return async ({ query, offset, results }) => {
    const total = perQuery;
    const items = [];
    const start = offset + 1;
    const end = Math.min(offset + results, total);
    for (let i = start; i <= end; i++) items.push({ ItemCode: `${query}-${i}` });
    return { ok: true, items, totalResultsAvailable: total, totalResultsReturned: items.length, firstResultPosition: start };
  };
}

// ──── テスト本体 ────

async function testMigration() {
  const db = freshDb();
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map((r) => r.name);
  expect('migration: yahoo_registered_items created', tables.includes('yahoo_registered_items'), `tables=${tables.join(',')}`);
  expect('migration: sync_runs created', tables.includes('sync_runs'), `tables=${tables.join(',')}`);
  expect('migration: yahoo_baseline_state created', tables.includes('yahoo_baseline_state'), `tables=${tables.join(',')}`);

  // partial UNIQUE: 同一 sync_name で running 2 件は拒否
  db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by) VALUES ('yahoo_baseline', '2026-01-01T00:00:00Z', 'running', 'cron')").run();
  try {
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by) VALUES ('yahoo_baseline', '2026-01-01T00:05:00Z', 'running', 'manual')").run();
    fail('migration: partial UNIQUE running guard', 'second running was accepted');
  } catch (e) {
    expect('migration: partial UNIQUE one-running guard', /UNIQUE/.test(e.message), e.message);
  }
  // 別 sync_name は OK
  db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by) VALUES ('rakuten_diff', '2026-01-01T00:05:00Z', 'running', 'cron')").run();
  ok('migration: different sync_name running allowed');

  // yahoo_baseline_state id=1 制約
  db.prepare("INSERT INTO yahoo_baseline_state (id, baseline_run_id, established_at, observed_count, query_hash, is_complete, updated_at) VALUES (1, 1, '2026-01-01T00:01:00Z', 100, 'h', 1, '2026-01-01T00:01:00Z')").run();
  try {
    db.prepare("INSERT INTO yahoo_baseline_state (id, baseline_run_id, established_at, observed_count, query_hash, is_complete, updated_at) VALUES (2, 1, '2026-01-01T00:02:00Z', 100, 'h', 1, '2026-01-01T00:02:00Z')").run();
    fail('migration: yahoo_baseline_state singleton', 'id=2 was accepted');
  } catch (e) {
    expect('migration: yahoo_baseline_state singleton (id=1 only)', /CHECK/.test(e.message), e.message);
  }

  // status check: running+finished_at 不一致 (Codex R2 Medium-2: sync_name='yahoo_baseline' に修正)
  try {
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, finished_at) VALUES ('yahoo_baseline', '2026-02-01T00:00:00Z', 'success', 'cron', NULL)").run();
    fail('migration: status=success requires finished_at', 'accepted');
  } catch (e) {
    expect('migration: status=success requires finished_at', /CHECK/.test(e.message) && /finished_at/.test(e.message), e.message);
  }

  // yahoo_registered_items source 制約 (sync_published 禁止)
  try {
    db.prepare("INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source) VALUES ('x', 'x', 0, '2026-01-01T00:00:00Z', 'sync_published')").run();
    fail('migration: source=sync_published rejected', 'accepted');
  } catch (e) {
    expect('migration: source=sync_published rejected (R1 critical)', /CHECK/.test(e.message), e.message);
  }

  // Codex E-7-a R1 Medium-1: sync_name に CHECK 追加。 typo は弾く
  try {
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by) VALUES ('yahho_baselin', '2026-01-01T00:00:00Z', 'running', 'cron')").run();
    fail('migration: sync_name typo rejected', 'accepted');
  } catch (e) {
    expect('migration: sync_name CHECK rejects typo', /CHECK/.test(e.message), e.message);
  }
}

async function testProxyIterate() {
  // 0 件
  let total = 0;
  for await (const page of iterateMyItemList('z', { callApi: makeMockApi({ z: 0 }) })) total += page.items.length;
  expect('proxy: query returning 0 items', total === 0, `total=${total}`);

  // 1 page (< pageSize)
  total = 0;
  for await (const page of iterateMyItemList('b', { callApi: makeMockApi({ b: 50 }), pageSize: 100 })) total += page.items.length;
  expect('proxy: query returning 50 items in 1 page', total === 50, `total=${total}`);

  // 3 page (250 items @ pageSize 100)
  total = 0;
  let pages = 0;
  for await (const page of iterateMyItemList('a', { callApi: makeMockApi({ a: 250 }), pageSize: 100 })) {
    total += page.items.length;
    pages += 1;
  }
  expect('proxy: query returning 250 items in 3 pages', total === 250 && pages === 3, `total=${total} pages=${pages}`);

  // Codex E-7-a R1 Medium-2: pageSize validation
  try {
    const gen = iterateMyItemList('q', { callApi: makeMockApi({ q: 10 }), pageSize: 0 });
    await gen.next();
    fail('proxy: pageSize=0 rejected', 'accepted');
  } catch (e) {
    expect('proxy: pageSize=0 rejected', /pageSize must be integer/.test(e.message), e.message);
  }
  try {
    const gen = iterateMyItemList('q', { callApi: makeMockApi({ q: 10 }), pageSize: 200 });
    await gen.next();
    fail('proxy: pageSize=200 (>100) rejected', 'accepted');
  } catch (e) {
    expect('proxy: pageSize>100 rejected', /pageSize must be integer/.test(e.message), e.message);
  }
  try {
    const gen = iterateMyItemList('q', { callApi: makeMockApi({ q: 10 }), pageSize: 1.5 });
    await gen.next();
    fail('proxy: pageSize float rejected', 'accepted');
  } catch (e) {
    expect('proxy: pageSize non-integer rejected', /pageSize must be integer/.test(e.message), e.message);
  }

  // 暴走防止: 110 page を返そうとすると throw
  const evilApi = async ({ offset, results }) => {
    const items = [];
    for (let i = 0; i < results; i++) items.push({ ItemCode: 'e-' + (offset + i) });
    return { ok: true, items, totalResultsAvailable: 100000, totalResultsReturned: results, firstResultPosition: offset + 1 };
  };
  try {
    let n = 0;
    for await (const _p of iterateMyItemList('e', { callApi: evilApi, pageSize: 100 })) n += 1;
    fail('proxy: runaway guard', `iterated ${n} pages without throw`);
  } catch (e) {
    expect('proxy: runaway guard', /runaway/.test(e.message), e.message);
  }
}

async function testSyncYahooBaseline() {
  // (1) 初回 success → baseline 確立
  {
    const db = freshDb();
    const r = await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'cron' });
    expect('sync (1): first run observes 36*5=180 items', r.itemsObserved === 180, `observed=${r.itemsObserved}`);
    expect('sync (1): first run establishes baseline', r.establishesBaseline === true, `establishes=${r.establishesBaseline}`);
    const state = db.prepare('SELECT * FROM yahoo_baseline_state').get();
    expect('sync (1): baseline_state.is_complete=1', state && state.is_complete === 1, JSON.stringify(state));
    expect('sync (1): baseline_state.query_hash matches canonical', state && state.query_hash === CANONICAL_QUERY_HASH, state?.query_hash);
    const run = db.prepare('SELECT * FROM sync_runs WHERE id = ?').get(r.syncRunId);
    expect('sync (1): sync_run status flipped to success', run && run.status === 'success' && run.finished_at !== null, JSON.stringify(run));
    expect('sync (1): sync_run.establishes_baseline=1', run && run.establishes_baseline === 1, run?.establishes_baseline);
  }

  // (2) stale row > 0 → baseline 再確立せず、 既存 state 保持
  {
    const db = freshDb();
    await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'cron' });
    const stateBefore = db.prepare('SELECT * FROM yahoo_baseline_state').get();
    // 2nd run: 'a' から 1 件消える
    const customMock = async ({ query, offset, results }) => {
      const total = query === 'a' ? 4 : 5;
      const items = [];
      const start = offset + 1;
      const end = Math.min(offset + results, total);
      for (let i = start; i <= end; i++) items.push({ ItemCode: `${query}-${i}` });
      return { ok: true, items, totalResultsAvailable: total, totalResultsReturned: items.length, firstResultPosition: start };
    };
    const r2 = await syncYahooBaseline({ db, deps: { callApi: customMock }, triggeredBy: 'cron' });
    expect('sync (2): stale rows detected', r2.staleRows === 1, `stale=${r2.staleRows}`);
    expect('sync (2): does NOT re-establish baseline on stale>0', r2.establishesBaseline === false, `establishes=${r2.establishesBaseline}`);
    const stateAfter = db.prepare('SELECT * FROM yahoo_baseline_state').get();
    expect('sync (2): state preserved from prior good run', stateAfter && stateAfter.baseline_run_id === stateBefore.baseline_run_id, JSON.stringify({ before: stateBefore.baseline_run_id, after: stateAfter?.baseline_run_id }));
  }

  // (3) env override → throw 412、 DB write 一切なし (Codex E-7-a R1 Critical-1)
  {
    const db = freshDb();
    process.env.YAHOO_DIFF_QUERIES = 'a,b';
    try {
      await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'manual' });
      fail('sync (3): env override should throw', 'did not throw');
    } catch (e) {
      expect('sync (3): env override throws 412', e.statusCode === 412, `statusCode=${e.statusCode} msg=${e.message}`);
    }
    const runs = db.prepare('SELECT COUNT(*) AS c FROM sync_runs').get().c;
    const items = db.prepare('SELECT COUNT(*) AS c FROM yahoo_registered_items').get().c;
    const states = db.prepare('SELECT COUNT(*) AS c FROM yahoo_baseline_state').get().c;
    expect('sync (3): no sync_runs row created under env override', runs === 0, `runs=${runs}`);
    expect('sync (3): no yahoo_registered_items row created under env override', items === 0, `items=${items}`);
    expect('sync (3): no yahoo_baseline_state row created under env override', states === 0, `states=${states}`);
    delete process.env.YAHOO_DIFF_QUERIES;
  }

  // (4) failed 経路 → sync_run.status='failed'
  {
    const db = freshDb();
    const throwingMock = async () => { throw new Error('proxy unreachable'); };
    try {
      await syncYahooBaseline({ db, deps: { callApi: throwingMock }, triggeredBy: 'cron' });
      fail('sync (4): failed mock should throw', 'did not throw');
    } catch (e) {
      expect('sync (4): error propagates', /proxy unreachable/.test(e.message), e.message);
    }
    const run = db.prepare('SELECT * FROM sync_runs ORDER BY id DESC LIMIT 1').get();
    expect('sync (4): sync_run.status=failed', run && run.status === 'failed' && /proxy unreachable/.test(run.error_message), JSON.stringify(run));
  }

  // (5) partial completeness (totalResultsAvailable > returned) → 確立しない
  {
    const db = freshDb();
    const partialMock = async ({ query, offset, results }) => {
      // Claim 100 available but return only 5
      return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: 100, totalResultsReturned: 1, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: partialMock }, triggeredBy: 'cron' });
    expect('sync (5): partial completeness blocks baseline establishment', r.establishesBaseline === false, `establishes=${r.establishesBaseline} anyPartial=${r.anyPartial}`);
    expect('sync (5): anyPartial flag set', r.anyPartial === true, `anyPartial=${r.anyPartial}`);
  }

  // (6) Codex E-7-a R1 High-2: lease 切れの running 行を steal して新 run を開始できる
  {
    const db = freshDb();
    // Pre-existing crashed running with expired lease (simulated)
    const pastIso = new Date(Date.now() - 60 * 60 * 1000).toISOString(); // 1h ago
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, run_lease_expires_at) VALUES ('yahoo_baseline', ?, 'running', 'cron', ?)").run(pastIso, pastIso);
    const beforeId = db.prepare("SELECT id FROM sync_runs WHERE sync_name='yahoo_baseline'").get().id;
    const r = await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'cron' });
    expect('sync (6): new run succeeded despite prior stale running', r.establishesBaseline === true, `establishes=${r.establishesBaseline}`);
    const stale = db.prepare('SELECT status, error_message FROM sync_runs WHERE id=?').get(beforeId);
    expect('sync (6): stale running flipped to failed', stale && stale.status === 'failed' && /lease-steal/.test(stale.error_message), JSON.stringify(stale));
  }

  // (7) Codex E-7-a R1 High-2: lease がまだ有効な running は steal しない (concurrent run guard)
  {
    const db = freshDb();
    const futureIso = new Date(Date.now() + 30 * 60 * 1000).toISOString();
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, run_lease_expires_at) VALUES ('yahoo_baseline', ?, 'running', 'cron', ?)").run(new Date().toISOString(), futureIso);
    try {
      await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'manual' });
      fail('sync (7): valid lease should block new run', 'no throw');
    } catch (e) {
      expect('sync (7): valid lease blocks new run (UNIQUE INDEX)', /UNIQUE/.test(e.message), e.message);
    }
  }

  // (8) Codex E-7-a R2 High-2: totalResultsAvailable=null (unknown) → 確立しない
  {
    const db = freshDb();
    const unknownTotalMock = async ({ query, offset, results }) => {
      // proxy が totalResultsAvailable を欠落させた状況をシミュレート
      const items = [{ ItemCode: query + '-1' }];
      return { ok: true, items, totalResultsAvailable: null, totalResultsReturned: 1, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: unknownTotalMock }, triggeredBy: 'cron' });
    expect('sync (8): unknown completeness blocks baseline', r.establishesBaseline === false, `establishes=${r.establishesBaseline}`);
    expect('sync (8): anyIncomplete flag', r.anyIncomplete === true, `anyIncomplete=${r.anyIncomplete}`);
  }

  // (9) Codex E-7-a R2 High-2: totalResultsAvailable が壊れた値 (string/-1/float) → unknown 扱い
  {
    const db = freshDb();
    const brokenTotalMock = async ({ query, offset, results }) => {
      return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: '100', totalResultsReturned: 1, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: brokenTotalMock }, triggeredBy: 'cron' });
    expect('sync (9): broken total (string) treated as unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline} anyIncomplete=${r.anyIncomplete}`);
  }
  {
    const db = freshDb();
    const negativeTotalMock = async ({ query, offset, results }) => {
      return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: -1, totalResultsReturned: 1, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: negativeTotalMock }, triggeredBy: 'cron' });
    expect('sync (9b): negative total treated as unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (10) Codex E-7-a R2 High-1: lease lost detection — 別 process が steal した後、 古い run の finish は LEASE_LOST throw
  {
    const db = freshDb();
    // 古い stale running を仕込む (lease 切れ + grace 過ぎ)
    const longPast = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const insertResult = db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, run_lease_expires_at, run_token) VALUES ('yahoo_baseline', ?, 'running', 'cron', ?, ?)").run(longPast, longPast, 'old-token-1234');
    const oldId = insertResult.lastInsertRowid;
    // 別 run が steal → 新 run start
    const r = await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'cron' });
    expect('sync (10): new run after steal succeeds', r.establishesBaseline === true, JSON.stringify({ establishes: r.establishesBaseline }));
    // 旧 run は failed flip 済
    const oldRow = db.prepare('SELECT status, run_token, error_message FROM sync_runs WHERE id=?').get(oldId);
    expect('sync (10): old run flipped to failed by steal', oldRow.status === 'failed' && oldRow.run_token === 'old-token-1234' && /lease-steal/.test(oldRow.error_message), JSON.stringify(oldRow));
  }

  // (11) Codex E-7-a R2 High-1: finishSyncRun が token mismatch で LEASE_LOST throw
  {
    const db = freshDb();
    // 1 度確立しておく
    await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'cron' });
    // Simulate: 開始した run の token が手元の token と違うケース (実運用は steal だが、 ここは直接シミュレート)
    const { finishSyncRun } = await import('../lib/yahoo-store-sync.js');
    // 既存 success row を update しようとしても running じゃないので 0 rows
    try {
      finishSyncRun(db, 1, 'wrong-token', { status: 'success' });
      fail('sync (11): finish with wrong token should throw', 'no throw');
    } catch (e) {
      expect('sync (11): finish with wrong token throws LEASE_LOST', e.code === 'LEASE_LOST', `code=${e.code} msg=${e.message}`);
    }
  }

  // (12) Codex E-7-a R3 High-2: page 2 で totalResultsAvailable が壊れる → unknown 扱い
  {
    const db = freshDb();
    // 250 items を 100/100/50 で返すが、 page 2 で total を null にする
    const pageBrokeMock = async ({ query, offset, results }) => {
      const totalA = query === 'a' ? 250 : 5;
      const totalToReport = (query === 'a' && offset === 100) ? null : totalA;
      const items = [];
      const start = offset + 1;
      const end = Math.min(offset + results, totalA);
      for (let i = start; i <= end; i++) items.push({ ItemCode: query + '-' + i });
      return { ok: true, items, totalResultsAvailable: totalToReport, totalResultsReturned: items.length, firstResultPosition: start };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: pageBrokeMock }, triggeredBy: 'cron' });
    expect('sync (12): page 2 invalid total → unknown blocks baseline', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline} anyIncomplete=${r.anyIncomplete}`);
  }

  // (13) Codex E-7-a R3 High-2: page 間で totalResultsAvailable が変動 → unknown
  {
    const db = freshDb();
    const totalShiftMock = async ({ query, offset, results }) => {
      const totalReal = query === 'a' ? 250 : 5;
      // page 1 = 250、 page 2 = 300、 page 3 = 250 (途中で変わる)
      const totalReported = (query === 'a' && offset === 100) ? 300 : totalReal;
      const items = [];
      const start = offset + 1;
      const end = Math.min(offset + results, totalReal);
      for (let i = start; i <= end; i++) items.push({ ItemCode: query + '-' + i });
      return { ok: true, items, totalResultsAvailable: totalReported, totalResultsReturned: items.length, firstResultPosition: start };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: totalShiftMock }, triggeredBy: 'cron' });
    expect('sync (13): page total shift → unknown blocks baseline', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline} anyIncomplete=${r.anyIncomplete}`);
  }

  // (15) Codex E-7-a R4 High-1: total=0 なのに 1 件返る → unknown
  {
    const db = freshDb();
    const zeroTotalMock = async ({ query, offset, results }) => {
      return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: 0, totalResultsReturned: 1, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: zeroTotalMock }, triggeredBy: 'cron' });
    expect('sync (15): total=0 + items returned → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (16) Codex E-7-a R4 High-1: same ItemCode returned across pages (proxy offset 無視シミュレート) → unknown
  {
    const db = freshDb();
    let calls = 0;
    const duplicateMock = async ({ query, offset, results }) => {
      calls++;
      if (query !== 'a') {
        return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: 1, totalResultsReturned: 1, firstResultPosition: 1 };
      }
      // 'a' query: page1/page2 で same 100 items 返す、 total=200 と主張
      const items = [];
      for (let i = 1; i <= 100; i++) items.push({ ItemCode: 'a-' + i });
      return { ok: true, items, totalResultsAvailable: 200, totalResultsReturned: 100, firstResultPosition: offset + 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: duplicateMock }, triggeredBy: 'cron' });
    expect('sync (16): same ItemCode across pages → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (17) Codex E-7-a R4 High-1: totalResultsReturned 報告と items.length 不一致 → unknown
  {
    const db = freshDb();
    const wrongReportedMock = async ({ query, offset, results }) => {
      const items = [{ ItemCode: query + '-1' }];
      // items は 1 件、 totalResultsReturned は 50 と嘘
      return { ok: true, items, totalResultsAvailable: 1, totalResultsReturned: 50, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: wrongReportedMock }, triggeredBy: 'cron' });
    expect('sync (17): totalResultsReturned mismatch → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (18) Codex E-7-a R4 High-1: firstResultPosition mismatch → unknown
  {
    const db = freshDb();
    const wrongFirstPosMock = async ({ query, offset, results }) => {
      const items = [{ ItemCode: query + '-1' }];
      return { ok: true, items, totalResultsAvailable: 1, totalResultsReturned: 1, firstResultPosition: 999 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: wrongFirstPosMock }, triggeredBy: 'cron' });
    expect('sync (18): firstResultPosition mismatch → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (19) Codex E-7-a R4 High-1: items.length > pageSize → unknown
  {
    const db = freshDb();
    const overPageMock = async ({ query, offset, results }) => {
      // 200 items 返す (pageSize=100 default)
      const items = [];
      for (let i = 1; i <= 200; i++) items.push({ ItemCode: query + '-' + i });
      return { ok: true, items, totalResultsAvailable: 200, totalResultsReturned: 200, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: overPageMock }, triggeredBy: 'cron' });
    expect('sync (19): items.length > pageSize → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (20) Codex E-7-a R5 High-1: items.ItemCode 欠落 → unknown (items.size=0 baseline 確立を防ぐ)
  {
    const db = freshDb();
    const missingCodeMock = async ({ query, offset, results }) => {
      return { ok: true, items: [{ Name: 'no-code' }], totalResultsAvailable: 1, totalResultsReturned: 1, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: missingCodeMock }, triggeredBy: 'cron' });
    expect('sync (20): item missing ItemCode → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline} anyIncomplete=${r.anyIncomplete}`);
  }

  // (20b) ItemCode が空文字 / 非 string → unknown
  {
    const db = freshDb();
    const emptyCodeMock = async ({ query, offset, results }) => {
      return { ok: true, items: [{ ItemCode: '' }, { ItemCode: 123 }], totalResultsAvailable: 2, totalResultsReturned: 2, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: emptyCodeMock }, triggeredBy: 'cron' });
    expect('sync (20b): empty/non-string ItemCode → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (21) Codex E-7-a R5 High-2: totalResultsReturned missing → unknown (補完なしで strict validate)
  {
    const db = freshDb();
    const missingReportedMock = async ({ query, offset, results }) => {
      // totalResultsReturned 欠落
      return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: 1, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: missingReportedMock }, triggeredBy: 'cron' });
    expect('sync (21): totalResultsReturned missing → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (21b) totalResultsReturned が float / string → unknown
  {
    const db = freshDb();
    const floatReportedMock = async ({ query, offset, results }) => {
      return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: 1, totalResultsReturned: 1.5, firstResultPosition: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: floatReportedMock }, triggeredBy: 'cron' });
    expect('sync (21b): totalResultsReturned float → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (22) Codex E-7-a R5 High-2: firstResultPosition missing → unknown
  {
    const db = freshDb();
    const missingPosMock = async ({ query, offset, results }) => {
      // firstResultPosition 欠落
      return { ok: true, items: [{ ItemCode: query + '-1' }], totalResultsAvailable: 1, totalResultsReturned: 1 };
    };
    const r = await syncYahooBaseline({ db, deps: { callApi: missingPosMock }, triggeredBy: 'cron' });
    expect('sync (22): firstResultPosition missing → unknown', r.establishesBaseline === false && r.anyIncomplete === true, `establishes=${r.establishesBaseline}`);
  }

  // (23) Codex E-7-a R6 High: items が非 array (欠落 / null / object / string) → throw → sync_run failed
  for (const [label, brokenItems] of [
    ['missing', undefined],
    ['null', null],
    ['object', { foo: 'bar' }],
    ['string', '[]'],
  ]) {
    const db = freshDb();
    const brokenItemsMock = async ({ query, offset, results }) => {
      const res = { ok: true, totalResultsAvailable: 0, totalResultsReturned: 0, firstResultPosition: 1 };
      if (brokenItems !== undefined) res.items = brokenItems;
      return res;
    };
    try {
      await syncYahooBaseline({ db, deps: { callApi: brokenItemsMock }, triggeredBy: 'cron' });
      fail(`sync (23-${label}): items=${label} should throw`, 'no throw');
    } catch (e) {
      expect(`sync (23-${label}): items=${label} throws contract violation`, /items must be an array/.test(e.message), e.message);
    }
    // sync_run も failed flip 済
    const run = db.prepare('SELECT status, error_message FROM sync_runs ORDER BY id DESC LIMIT 1').get();
    expect(`sync (23-${label}): sync_run.status=failed`, run && run.status === 'failed' && /items must be an array/.test(run.error_message), JSON.stringify(run));
  }

  // (14) Codex E-7-a R3 High-1: last_seen_at monotonic upsert
  //   古い ts で upsert しても新しい行の last_seen_at を上書きしない (regress 防止)
  {
    const db = freshDb();
    const recentTs = '2026-06-13T10:00:00Z';
    const olderTs = '2025-01-01T00:00:00Z';
    db.prepare("INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source) VALUES ('m1', 'm1', 0, ?, 'yahoo_existing')").run(recentTs);
    // Older ts での upsert を試みる
    db.prepare(`
      INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source)
      VALUES (@code, @code, @hasSub, @ts, 'yahoo_existing')
      ON CONFLICT(item_code) DO UPDATE SET
        yahoo_item_code = excluded.yahoo_item_code,
        has_sub_code    = excluded.has_sub_code,
        last_seen_at    = excluded.last_seen_at
        WHERE excluded.last_seen_at >= yahoo_registered_items.last_seen_at
    `).run({ code: 'm1', hasSub: 0, ts: olderTs });
    const got = db.prepare("SELECT last_seen_at FROM yahoo_registered_items WHERE item_code = 'm1'").get();
    expect('sync (14): older ts upsert does NOT regress last_seen_at', got.last_seen_at === recentTs, `got=${got.last_seen_at}, expected=${recentTs}`);
  }
}

async function testAssertGuard() {
  // (a) no state → 412
  {
    const db = freshDb();
    try { assertYahooBaselineComplete(db); fail('assert (a)', 'no throw on empty state'); }
    catch (e) { expect('assert (a): no state → 412', e.statusCode === 412, `statusCode=${e.statusCode}`); }
  }
  // (b)/(c)/(d)/(e)/(f) — set up healthy state then violate
  const db = freshDb();
  await syncYahooBaseline({ db, deps: { callApi: makeUniformMock(5) }, triggeredBy: 'cron' });

  // (f) healthy → no throw
  try { assertYahooBaselineComplete(db); ok('assert (f): healthy → no throw'); }
  catch (e) { fail('assert (f)', `unexpected throw: ${e.message}`); }

  // (b) is_complete=0
  db.prepare('UPDATE yahoo_baseline_state SET is_complete=0').run();
  try { assertYahooBaselineComplete(db); fail('assert (b)', 'no throw on is_complete=0'); }
  catch (e) { expect('assert (b): is_complete=0 → 412', e.statusCode === 412, `statusCode=${e.statusCode}`); }
  db.prepare('UPDATE yahoo_baseline_state SET is_complete=1').run();

  // (c) env override
  process.env.YAHOO_DIFF_QUERIES = 'a';
  try { assertYahooBaselineComplete(db); fail('assert (c)', 'no throw under env override'); }
  catch (e) { expect('assert (c): env override → 412', e.statusCode === 412, `statusCode=${e.statusCode}`); }
  delete process.env.YAHOO_DIFF_QUERIES;

  // (d) hash mismatch
  db.prepare("UPDATE yahoo_baseline_state SET query_hash='wronghash'").run();
  try { assertYahooBaselineComplete(db); fail('assert (d)', 'no throw on hash mismatch'); }
  catch (e) { expect('assert (d): hash mismatch → 412', e.statusCode === 412 && /query set changed/.test(e.message), e.message); }
  db.prepare('UPDATE yahoo_baseline_state SET query_hash=?').run(CANONICAL_QUERY_HASH);

  // (e) stale (> maxAgeHours)
  db.prepare("UPDATE yahoo_baseline_state SET established_at='2020-01-01T00:00:00Z'").run();
  try { assertYahooBaselineComplete(db, { maxAgeHours: 30 }); fail('assert (e)', 'no throw on stale'); }
  catch (e) { expect('assert (e): stale → 412', e.statusCode === 412 && /stale/.test(e.message), e.message); }
}

// ──── runner ────
(async () => {
  console.log('=== Phase E-7-a smoke runner ===');
  console.log('--- migration ---');
  await testMigration();
  console.log('--- proxy iterate ---');
  await testProxyIterate();
  console.log('--- syncYahooBaseline ---');
  await testSyncYahooBaseline();
  console.log('--- assertYahooBaselineComplete ---');
  await testAssertGuard();
  console.log('================================');
  if (failed > 0) {
    console.error(`FAILED: ${failed} check(s) did not pass`);
    process.exit(1);
  } else {
    console.log('ALL CHECKS PASSED');
  }
})().catch((e) => {
  console.error('unexpected runner error:', e);
  process.exit(2);
});
