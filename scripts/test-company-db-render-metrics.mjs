#!/usr/bin/env node
/**
 * test-company-db-render-metrics.mjs — Render の metrics の client (apps/company-db/profit/render-metrics.mjs) の試験
 *
 * 設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10 (D-60 v3.4) の PR 2・「資源の関門」
 * 🚨 Render の API は本物で呼ばない。127.0.0.1 の HTTP の server が scripts/fixtures/render-metrics/ の応答 (と、それを壊した形) を返す
 *   正常 (4 つの値・bytes への直し方・最新の点・要求の形) / HTTP の 401・403・429・5xx・3xx・404・timeout・網の失敗 /
 *   形 (JSON でない・配列でない・系列や点の形・空・重複の系列・同じ時刻の点・label 無し・別の resource・単位・負・未来・2 分超え・0・使用 > 上限) /
 *   使う直前の鮮度の再検査 / 設定 / 🚨 鍵 (RENDER_API_KEY) が戻り値・例外・console・fixture に出ない
 *   🆕 #1600 R1: 実在しない日付 (2026-02-30 ほか) は拒む・ナノ秒で比べる / 本文は逐次読み 1MB を超えたらやめる (Content-Length なし・少なく書いた・chunked) /
 *     値は本文の文字のままで整数の safe integer だけ (1073741824.00000001 ほか) / 400 ほか全部の非 200 / 理由の優先 (timeout が先・次に endpoint の順)
 *   🆕 #1600 R2: 本文は自前の厳密な JSON の読み方 (reviver の context.source を使わない = 本番の node:20-slim で動く)。Node 20 でも流す:
 *     npx -y node@20 scripts/test-company-db-render-metrics.mjs
 * 実行: node scripts/test-company-db-render-metrics.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import crypto from 'node:crypto';
import { inspect } from 'node:util';
import {
  fetchPostgresMetrics, checkFreshness, readRenderMetricsConfig, parseRfc3339Nanos, parseJsonIntegersOnly, RenderApiKey, REASONS, METRIC_ENDPOINTS, RENDER_API_BASE, RESOURCE_LABEL_FIELD,
} from '../apps/company-db/profit/render-metrics.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };

const FIX_DIR = new URL('./fixtures/render-metrics/', import.meta.url);
const FIX = Object.fromEntries(Object.entries({ memory: 'memory.json', memoryLimit: 'memory-limit.json', diskUsage: 'disk-usage.json', diskCapacity: 'disk-capacity.json' })
  .map(([k, f]) => [k, JSON.parse(fs.readFileSync(new URL(f, FIX_DIR), 'utf8'))]));
const RID = 'dpg-d60fixture0000000000-a';
const NOW = Date.parse('2026-10-03T03:00:00.000Z');
// 試験の鍵 = 毎回の乱数 (fixture・source に同じ文字は無い)
const KEY_TEXT = `rnd_TESTONLY${crypto.randomBytes(16).toString('hex')}`;
const KEY = new RenderApiKey(KEY_TEXT);
const clone = (x) => JSON.parse(JSON.stringify(x));

// ─── 127.0.0.1 の Render の偽物 ───
let routes = {};          // path → { status, body (object | string), delayMs, headers }
let requests = [];
const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://127.0.0.1');
  requests.push({ method: req.method, path: u.pathname, query: Object.fromEntries(u.searchParams), auth: req.headers.authorization, url: req.url });
  const r = routes[u.pathname.replace(/^\/v1/, '')];
  const send = () => {
    if (!r) { res.writeHead(404); return res.end('not found'); }
    if (r.stream) {   // Content-Length なしの chunked で、相手が切るまで 64KB ずつ書き続ける (全部を読んでから拒む作りなら終わらない)
      res.writeHead(200, { 'content-type': 'application/json' });
      const chunk = Buffer.alloc(64 * 1024, 0x20);
      res.write('[');
      let open = true;
      res.on('close', () => { open = false; r.stream.closed = true; });
      const pump = () => { while (open && r.stream.written < 64 * 1024 * 1024) { r.stream.written += chunk.length; if (!res.write(chunk)) return res.once('drain', pump); } };
      return pump();
    }
    const body = typeof r.body === 'string' ? r.body : JSON.stringify(r.body);
    res.writeHead(r.status ?? 200, { 'content-type': 'application/json', ...(r.headers || {}) });
    res.end(body);
  };
  if (r && r.delayMs) setTimeout(send, r.delayMs); else send();
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const BASE = `http://127.0.0.1:${server.address().port}`;
const okRoutes = () => Object.fromEntries(Object.entries(METRIC_ENDPOINTS).map(([k, p]) => [p, { status: 200, body: clone(FIX[k]) }]));

// console に出たものを全部とっておく (鍵が出ないことを確かめる)
const consoleSeen = [];
for (const m of ['log', 'error', 'warn', 'info', 'debug']) {
  const orig = console[m].bind(console);
  console[m] = (...a) => { consoleSeen.push(a.map((x) => (typeof x === 'string' ? x : inspect(x))).join(' ')); orig(...a); };
}
const allResults = [];
const run = async (over = {}, opts = {}) => {
  routes = okRoutes();
  for (const [k, v] of Object.entries(over)) routes[METRIC_ENDPOINTS[k]] = { ...routes[METRIC_ENDPOINTS[k]], ...v };
  requests = [];
  const r = await fetchPostgresMetrics({ apiKey: KEY, resourceId: RID, baseUrl: BASE, now: () => NOW, ...opts });
  allResults.push(r);
  return r;
};
const expectReason = async (reason, over, opts) => {
  assert.ok(REASONS.includes(reason), `試験の理由 ${reason} が REASONS に無い`);
  const r = await run(over, opts);
  assert.deepEqual(r, { ok: false, reason }, JSON.stringify(r));
};
const series = (k) => FIX[k][0];
const withValues = (k, values) => [{ ...clone(series(k)), values }];

console.log('正常');
await t('4 つの値を bytes の整数のまま返す (丸めない)・最新の点・peakProof は false', async () => {
  const r = await run();
  assert.equal(r.ok, true, JSON.stringify(r));
  const s = r.snapshot;
  assert.equal(s.resourceId, RID);
  assert.equal(s.memoryUsedBytes, 629145601);
  assert.equal(s.memoryLimitBytes, 1073741824);
  assert.equal(s.diskUsedBytes, 7516193001);
  assert.equal(s.diskCapacityBytes, 16106127360);
  assert.equal(s.peakProof, false);
  assert.equal(s.resolutionSeconds, 30);
  assert.equal(s.fetchedAt, '2026-10-03T03:00:00.000Z');
  assert.deepEqual({ ...s.points }, { memory: '2026-10-03T02:59:30.000Z', memoryLimit: '2026-10-03T02:59:30.000Z', diskUsage: '2026-10-03T02:59:30.000Z', diskCapacity: '2026-10-03T02:59:30.000Z' });
  assert.equal(s.oldestPointAt, '2026-10-03T02:59:30.000Z');
  assert.ok(Object.isFrozen(s) && Object.isFrozen(r));
});
await t('点の順が逆でも最新の点を採る', async () => {
  const r = await run({ memory: { body: withValues('memory', [...series('memory').values].reverse()) } });
  assert.equal(r.ok, true); assert.equal(r.snapshot.memoryUsedBytes, 629145601);
});
await t('要求の形 = GET の 4 つの path・resource / startTime / endTime / resolutionSeconds=30・aggregationMethod なし・Bearer の鍵・URL に鍵なし', async () => {
  await run();
  assert.deepEqual(requests.map((q) => q.path).sort(), ['/metrics/disk-capacity', '/metrics/disk-usage', '/metrics/memory', '/metrics/memory-limit']);
  for (const q of requests) {
    assert.equal(q.method, 'GET');
    assert.deepEqual(q.query, { resource: RID, startTime: '2026-10-03T02:50:00.000Z', endTime: '2026-10-03T03:00:00.000Z', resolutionSeconds: '30' });
    assert.equal(q.auth, `Bearer ${KEY_TEXT}`);
    assert.ok(!q.url.includes(KEY_TEXT));
  }
  assert.equal(RENDER_API_BASE, 'https://api.render.com/v1');
  assert.equal(RESOURCE_LABEL_FIELD, 'resource');
});

console.log('使う直前の鮮度の再検査 (checkFreshness)');
await t('最新の点から 2 分以内は ok・2 分を超えたら STALE・時計が戻ったら STALE・手で作った snapshot は STALE', async () => {
  const { snapshot } = await run();
  assert.deepEqual(checkFreshness(snapshot, NOW), { ok: true });
  assert.deepEqual(checkFreshness(snapshot, Date.parse('2026-10-03T03:01:30.000Z')), { ok: true });          // ちょうど 2 分
  assert.deepEqual(checkFreshness(snapshot, Date.parse('2026-10-03T03:01:30.001Z')), { ok: false, reason: 'METRICS_STALE' });
  assert.deepEqual(checkFreshness(snapshot, NOW - 1), { ok: false, reason: 'METRICS_STALE' });
  assert.deepEqual(checkFreshness({ ...snapshot }, NOW), { ok: false, reason: 'METRICS_STALE' });
  assert.deepEqual(checkFreshness(null, NOW), { ok: false, reason: 'METRICS_STALE' });
  assert.deepEqual(checkFreshness(snapshot, NaN), { ok: false, reason: 'METRICS_STALE' });
});

console.log('HTTP の失敗 = 全部「不可」(呼び手は 503)');
await t('401・403 → METRICS_AUTH (本文に鍵が映っていても返さない)', async () => {
  await expectReason('METRICS_AUTH', { memory: { status: 401, body: { message: `invalid key ${KEY_TEXT}` } } });
  await expectReason('METRICS_AUTH', { diskCapacity: { status: 403, body: { message: 'forbidden' } } });
});
await t('429 → METRICS_RATE_LIMITED', async () => { await expectReason('METRICS_RATE_LIMITED', { memoryLimit: { status: 429, body: { message: 'slow down' }, headers: { 'retry-after': '30' } } }); });
await t('500・502・503・599 → METRICS_UPSTREAM', async () => {
  for (const status of [500, 502, 503, 599]) await expectReason('METRICS_UPSTREAM', { diskUsage: { status, body: 'oops' } });
});
await t('404・201・204 → METRICS_HTTP', async () => {
  await expectReason('METRICS_HTTP', { memory: { status: 404, body: 'nf' } });
  await expectReason('METRICS_HTTP', { memory: { status: 201, body: clone(FIX.memory) } });
  await expectReason('METRICS_HTTP', { memory: { status: 204, body: '' } });
});
await t('3xx の redirect は追わない (別の host に鍵を送らない) → METRICS_NETWORK', async () => {
  await expectReason('METRICS_NETWORK', { memory: { status: 302, body: '', headers: { location: 'http://127.0.0.2:9/steal' } } });
  assert.ok(!requests.some((q) => q.path === '/steal'));
});
await t('timeout (全体 5 秒の既定・試験は 150ms) → METRICS_TIMEOUT で早く返る', async () => {
  const t0 = Date.now();
  await expectReason('METRICS_TIMEOUT', { diskCapacity: { delayMs: 3000 } }, { timeoutMs: 150 });
  assert.ok(Date.now() - t0 < 2000, `${Date.now() - t0}ms`);
});
await t('網の失敗 (つながらない port) → METRICS_NETWORK', async () => {
  const s2 = http.createServer(); await new Promise((r) => s2.listen(0, '127.0.0.1', r)); const port = s2.address().port; await new Promise((r) => s2.close(r));
  const r = await fetchPostgresMetrics({ apiKey: KEY, resourceId: RID, baseUrl: `http://127.0.0.1:${port}`, now: () => NOW });
  allResults.push(r);
  assert.deepEqual(r, { ok: false, reason: 'METRICS_NETWORK' });
});
await t('fetch の例外の文に鍵が入っていても返さない → METRICS_NETWORK', async () => {
  const r = await fetchPostgresMetrics({ apiKey: KEY, resourceId: RID, now: () => NOW, fetchImpl: async (url, init) => { throw new Error(`boom ${init.headers.Authorization}`); } });
  allResults.push(r);
  assert.deepEqual(r, { ok: false, reason: 'METRICS_NETWORK' });
});
await t('本文の読み取りの例外に鍵が入っていても返さない → METRICS_SHAPE', async () => {
  const r = await fetchPostgresMetrics({ apiKey: KEY, resourceId: RID, now: () => NOW,
    fetchImpl: async () => ({ status: 200, headers: new Headers(), body: new ReadableStream({ pull(c) { c.error(new Error(`read failed ${KEY_TEXT}`)); } }) }) });
  allResults.push(r);
  assert.deepEqual(r, { ok: false, reason: 'METRICS_SHAPE' });
});
await t('400・300・304 も「不可」→ METRICS_HTTP (200 でない応答は全部)', async () => {
  await expectReason('METRICS_HTTP', { memory: { status: 400, body: { message: 'bad request' } } });
  await expectReason('METRICS_HTTP', { diskUsage: { status: 300, body: '' } });
  await expectReason('METRICS_HTTP', { diskUsage: { status: 304, body: '' } });
});
await t('理由の優先: timeout はほかの endpoint の理由より先 / timeout が無ければ endpoint の順で最初 (混ざった異常)', async () => {
  await expectReason('METRICS_TIMEOUT', { memory: { status: 401, body: '' }, diskCapacity: { delayMs: 3000 } }, { timeoutMs: 150 });
  await expectReason('METRICS_AUTH', { memory: { status: 401, body: '' }, memoryLimit: { status: 500, body: '' }, diskUsage: { body: '<html>' } });
  await expectReason('METRICS_SHAPE', { memory: { body: '<html>' }, memoryLimit: { status: 429, body: '' } });
  await expectReason('METRICS_UPSTREAM', { memoryLimit: { status: 503, body: '' }, diskUsage: { status: 401, body: '' } });
});

console.log('本文の上限 (1MB) = 逐次読んで超えた時点でやめる');
const bigStreamFetch = (contentLength) => {
  const st = { pulled: 0, cancelled: false };
  const fetchImpl = async () => ({
    status: 200,
    headers: new Headers(contentLength == null ? {} : { 'content-length': String(contentLength) }),
    body: new ReadableStream({
      pull(c) { const chunk = new Uint8Array(64 * 1024).fill(0x20); st.pulled += chunk.length; c.enqueue(chunk); },
      cancel() { st.cancelled = true; },
    }, { highWaterMark: 0 }),
  });
  return { st, fetchImpl };
};
await t('Content-Length が無い・少なく書いた (10 bytes) 応答が終わらなくても、1MB を超えた所で読むのをやめて METRICS_SHAPE', async () => {
  for (const cl of [null, 10]) {
    const { st, fetchImpl } = bigStreamFetch(cl);
    const r = await fetchPostgresMetrics({ apiKey: KEY, resourceId: RID, now: () => NOW, fetchImpl });
    allResults.push(r);
    assert.deepEqual(r, { ok: false, reason: 'METRICS_SHAPE' }, `content-length=${cl}`);
    assert.ok(st.cancelled, `stream を cancel していない (content-length=${cl})`);
    assert.ok(st.pulled <= 4 * (1024 * 1024 + 2 * 64 * 1024), `読みすぎ: ${st.pulled} (4 つの endpoint の合計)`);
  }
});
await t('本物の HTTP の chunked で終わらない応答 → METRICS_SHAPE で早く返り、相手の接続を切る', async () => {
  const stream = { written: 0, closed: false };
  const t0 = Date.now();
  await expectReason('METRICS_SHAPE', { diskUsage: { stream } });
  assert.ok(Date.now() - t0 < 3000, `${Date.now() - t0}ms`);
  for (let i = 0; i < 50 && !stream.closed; i++) await new Promise((r) => setTimeout(r, 20));
  assert.ok(stream.closed, '相手 (server) の応答が閉じられていない');
  assert.ok(stream.written < 64 * 1024 * 1024, `server が上限まで書けた = 読み続けている: ${stream.written}`);
});
await t('Content-Length が 1MB を超えると読まずに METRICS_SHAPE・UTF-8 でない本文も METRICS_SHAPE', async () => {
  await expectReason('METRICS_SHAPE', { memory: { body: `[${' '.repeat(1024 * 1024 + 10)}]` } });
  const r = await fetchPostgresMetrics({ apiKey: KEY, resourceId: RID, now: () => NOW,
    fetchImpl: async () => ({ status: 200, headers: new Headers(), body: new ReadableStream({ start(c) { c.enqueue(new Uint8Array([0x5b, 0xff, 0xfe, 0x5d])); c.close(); } }) }) });
  allResults.push(r);
  assert.deepEqual(r, { ok: false, reason: 'METRICS_SHAPE' });
});

console.log('応答の形の異常 = 全部「不可」');
await t('JSON でない・配列でない → METRICS_SHAPE', async () => {
  await expectReason('METRICS_SHAPE', { memory: { body: '<html>' } });
  await expectReason('METRICS_SHAPE', { memory: { body: { data: FIX.memory } } });
  await expectReason('METRICS_SHAPE', { memory: { body: 'null' } });
  await expectReason('METRICS_SHAPE', { memory: { body: '42' } });
});
await t('系列・label・点の形が違う → METRICS_SHAPE', async () => {
  const s = series('memory');
  await expectReason('METRICS_SHAPE', { memory: { body: [{ ...clone(s), unit: undefined }] } });
  await expectReason('METRICS_SHAPE', { memory: { body: [{ ...clone(s), labels: {} }] } });
  await expectReason('METRICS_SHAPE', { memory: { body: [{ ...clone(s), values: null }] } });
  await expectReason('METRICS_SHAPE', { memory: { body: [{ ...clone(s), labels: [{ field: 'resource', value: 1 }] }] } });
  await expectReason('METRICS_SHAPE', { memory: { body: [null] } });
  await expectReason('METRICS_SHAPE', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03T02:59:30Z', value: '629145600' }]) } });
  await expectReason('METRICS_SHAPE', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03T02:59:30Z', value: null }]) } });
  await expectReason('METRICS_SHAPE', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03 02:59:30', value: 1 }]) } });
  await expectReason('METRICS_SHAPE', { memory: { body: withValues('memory', [{ timestamp: 1791000000, value: 1 }]) } });
  await expectReason('METRICS_SHAPE', { memory: { body: withValues('memory', [[1, 2]]) } });
});

console.log('値は本文の文字のままで整数の safe integer だけ (JSON.parse の binary64 で安全の向きが逆にならない)');
// 本文の文字を直に作る (JS の数では 1073741824.00000001 を表せない)
const rawSeries = (k, valueText) => `[{"labels":[{"field":"resource","value":"${RID}"}],"values":[{"timestamp":"2026-10-03T02:59:30Z","value":${valueText}}],"unit":"bytes"}]`;
await t('境界: 使用 1073741824.00000001 (binary64 では 1073741824)・上限 1073741823.99999999 (同じく 1073741824) → METRICS_NOT_INTEGER', async () => {
  assert.equal(JSON.parse('1073741824.00000001'), 1073741824);   // ふつうに読むと境界が消える (これが穴)
  await expectReason('METRICS_NOT_INTEGER', { memory: { body: rawSeries('memory', '1073741824.00000001') } });
  await expectReason('METRICS_NOT_INTEGER', { memoryLimit: { body: rawSeries('memoryLimit', '1073741823.99999999') } });
});
await t('小数・指数・2^53 以上 → METRICS_NOT_INTEGER (1.0・1e3・1E3・629145600.4・9007199254740992・1e300)', async () => {
  for (const v of ['1.0', '1e3', '1E3', '629145600.4', '9007199254740992', '9007199254740993', '1e300', '-1.5']) {
    await expectReason('METRICS_NOT_INTEGER', { diskUsage: { body: rawSeries('diskUsage', v) } });
  }
});
await t('整数の文字の safe integer は受け取る (9007199254740991 の容量・0 の使用・-0 は 0)', async () => {
  let r = await run({ diskCapacity: { body: rawSeries('diskCapacity', '9007199254740991') } });
  assert.equal(r.ok, true, JSON.stringify(r)); assert.equal(r.snapshot.diskCapacityBytes, 9007199254740991);
  r = await run({ memory: { body: rawSeries('memory', '0') } });
  assert.equal(r.ok, true, JSON.stringify(r)); assert.equal(r.snapshot.memoryUsedBytes, 0);
  r = await run({ diskUsage: { body: rawSeries('diskUsage', '-0') } });
  assert.equal(r.ok, true, JSON.stringify(r)); assert.ok(Object.is(r.snapshot.diskUsedBytes, -0) || r.snapshot.diskUsedBytes === 0);
});

console.log(`JSON の読み方 = 自前の厳密な文法で数の字句をそのまま見る (Node 20 でも同じ・#1600 R2) [Node ${process.version}]`);
await t('reviver の第 3 引数 (context.source) を使っていない (本番の node:20-slim では渡らない)', async () => {
  const src = fs.readFileSync(new URL('../apps/company-db/profit/render-metrics.mjs', import.meta.url), 'utf8').replace(/^\s*\*.*$/gm, '').replace(/\/\/.*$/gm, '');
  assert.doesNotMatch(src, /context\.source|JSON\.parse\(\s*[a-zA-Z_.]+\s*,/);   // JSON.parse(x, reviver) の形が無い (文字列の字句の JSON.parse(lex(…)) だけ)
  const docker = fs.readFileSync(new URL('../Dockerfile', import.meta.url), 'utf8');
  assert.match(docker.split('\n')[0], /^FROM node:20/, '本番の Node の版が変わったら、この試験を Node の版に合わせて見直す');
});
await t('正常の 4 つの fixture の本文を JSON.parse と同じ値に読む (数は整数の safe integer のまま)', async () => {
  for (const f of fs.readdirSync(FIX_DIR)) {
    const text = fs.readFileSync(new URL(f, FIX_DIR), 'utf8');
    assert.deepEqual(JSON.parse(JSON.stringify(parseJsonIntegersOnly(text))), JSON.parse(text), f);
  }
});
await t('数の字句: 整数の safe integer だけ数・小数 / 指数 / 2^53 以上は NOT_INTEGER (境界 1073741824.00000001・1073741823.99999999 も)', async () => {
  const one = (t) => parseJsonIntegersOnly(`[${t}]`)[0];
  for (const [t, v] of [['0', 0], ['-0', -0], ['7', 7], ['-12', -12], ['9007199254740991', 9007199254740991], ['-9007199254740991', -9007199254740991]]) assert.ok(Object.is(one(t), v), t);
  for (const t of ['1073741824.00000001', '1073741823.99999999', '1.0', '1e3', '1E3', '1e+3', '1e-3', '0.5', '-1.5', '9007199254740992', '9007199254740993', '1e300', '123456789012345678901234567890']) {
    const v = one(t);
    assert.ok(v !== null && typeof v === 'object' && v.notInteger === true, t);
  }
});
await t('文法の違反・同じ key・深すぎる・末尾の余り → METRICS_SHAPE / "__proto__" の key は prototype を変えない', async () => {
  const bad = ['', ' ', '[', '[1,]', '[,1]', '{"a":1,}', '{"a" 1}', '{a:1}', "{'a':1}", '[01]', '[+1]', '[1.]', '[.5]', '[-]', '[1e]', '[NaN]', '[Infinity]', '[tru]',
    '["a\u0001"]', '["\\x41"]', '["\\u12"]', '[1] [2]', '[1]x', '{"a":1,"a":2}', '﻿[1]', `${'['.repeat(40)}${']'.repeat(40)}`, '[1 2]', '"\\"'];
  for (const t of bad) assert.throws(() => parseJsonIntegersOnly(t), (e) => e && e.reason === 'METRICS_SHAPE', JSON.stringify(t));
  const o = parseJsonIntegersOnly('{"__proto__":{"polluted":1},"a":[true,false,null,"x\\u00e9\\n"]}');
  assert.equal(Object.getPrototypeOf(o), null);
  assert.deepEqual(Object.keys(o), ['__proto__', 'a']);
  assert.equal(({}).polluted, undefined);
  assert.deepEqual(o.a, [true, false, null, 'xé\n']);
  assert.deepEqual(JSON.parse(JSON.stringify(parseJsonIntegersOnly(' \r\n\t{ "k" : [ 1 , "2" ] } \n'))), { k: [1, '2'] });
});
await t('深さの境界 (R3 Low): container ([ と {) の入れ子は 32 段まで受け取り 33 段から拒む (配列・object・混ぜた形・一番下に値がある形)', async () => {
  const arr = (n, inner = '') => `${'['.repeat(n)}${inner}${']'.repeat(n)}`;
  const obj = (n, inner = '1') => `${'{"a":'.repeat(n)}${inner}${'}'.repeat(n)}`;
  const mix = (n) => { let s = '7'; for (let k = 0; k < n; k++) s = k % 2 ? `{"k":${s}}` : `[${s}]`; return s; };
  for (const [name, make] of [['配列', (n) => arr(n)], ['配列 + 値', (n) => arr(n, '5')], ['object', (n) => obj(n)], ['object + {}', (n) => obj(n - 1, '{}')], ['混ぜた形', mix]]) {
    for (const n of [1, 31, 32]) assert.doesNotThrow(() => parseJsonIntegersOnly(make(n)), `${name} ${n} 段`);
    for (const n of [33, 34, 40]) assert.throws(() => parseJsonIntegersOnly(make(n)), (e) => e && e.reason === 'METRICS_SHAPE', `${name} ${n} 段`);
  }
  assert.equal(parseJsonIntegersOnly(arr(32, '5')).flat(Infinity)[0], 5);
  assert.equal(parseJsonIntegersOnly('5'), 5);   // container の無い値は 0 段
});

console.log('時刻 = RFC 3339 の暦の要素を全部確かめる (Date.parse に直させない)');
await t('実在しない日・時刻 → METRICS_SHAPE (2026-02-30・2025-02-29・04-31・月 13・日 00・25 時・60 分・60 秒・時差 +24:00 / +09:60・小数 10 桁・小文字)', async () => {
  assert.equal(new Date(Date.parse('2026-02-30T00:00:00Z')).toISOString(), '2026-03-02T00:00:00.000Z');   // Date.parse は 3/2 に直す (これが穴)
  const bad = ['2026-02-30T02:59:30Z', '2025-02-29T02:59:30Z', '2026-04-31T02:59:30Z', '2026-13-01T02:59:30Z', '2026-10-00T02:59:30Z', '2026-10-03T25:59:30Z',
    '2026-10-03T02:60:30Z', '2026-10-03T02:59:60Z', '2026-10-03T11:59:30+24:00', '2026-10-03T11:59:30+09:60', '2026-10-03T02:59:30.1234567891Z',
    '2026-10-03t02:59:30z', '2026-10-03T02:59:30', '2026-10-03T02:59:30+0900', ' 2026-10-03T02:59:30Z'];
  for (const ts of bad) {
    assert.equal(parseRfc3339Nanos(ts), null, ts);
    await expectReason('METRICS_SHAPE', { memory: { body: withValues('memory', [{ timestamp: ts, value: 1 }]) } });
  }
});
await t('うるう年の 2/29・月末と時差をまたぐ値・年をまたぐ値・小数秒 9 桁はナノ秒で正しく読む', async () => {
  const z = (s) => parseRfc3339Nanos(s);
  assert.equal(z('2024-02-29T00:00:00Z'), BigInt(Date.parse('2024-02-29T00:00:00Z')) * 1_000_000n);
  assert.equal(z('2000-02-29T12:00:00Z'), BigInt(Date.parse('2000-02-29T12:00:00Z')) * 1_000_000n);
  assert.equal(parseRfc3339Nanos('1900-02-29T00:00:00Z'), null);
  assert.equal(z('2026-02-28T23:30:00-01:00'), z('2026-03-01T00:30:00Z'));          // 月末を時差でまたぐ
  assert.equal(z('2026-03-01T08:30:00+09:00'), z('2026-02-28T23:30:00Z'));
  assert.equal(z('2026-12-31T23:59:59.999999999-00:30'), z('2027-01-01T00:29:59.999999999Z'));   // 年をまたぐ
  assert.equal(z('2026-10-03T02:59:30.123456789Z') - z('2026-10-03T02:59:30Z'), 123456789n);
  assert.equal(z('2026-10-03T02:59:30.1Z'), z('2026-10-03T02:59:30.100000000Z'));
  // 時差つき・小数 9 桁の点で全体が通る
  const r = await run({ memory: { body: withValues('memory', [{ timestamp: '2026-10-02T23:59:30.123456789-03:00', value: 5 }]) } });
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.equal(r.snapshot.points.memory, '2026-10-03T02:59:30.123Z');   // ミリ秒に切り捨て (古い向き)
});
await t('小数秒 4 桁以上を丸めない: 1 ナノ秒でも未来なら METRICS_FUTURE・1 ナノ秒違えば別の点・同じナノ秒は重複', async () => {
  await expectReason('METRICS_FUTURE', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03T03:00:00.000000001Z', value: 1 }]) } });
  let r = await run({ memory: { body: withValues('memory', [{ timestamp: '2026-10-03T02:59:30Z', value: 1 }, { timestamp: '2026-10-03T02:59:30.000000001Z', value: 2 }]) } });
  assert.equal(r.ok, true, JSON.stringify(r)); assert.equal(r.snapshot.memoryUsedBytes, 2);   // 1 ナノ秒新しい方
  await expectReason('METRICS_DUPLICATE_POINT', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03T02:59:30.1Z', value: 1 }, { timestamp: '2026-10-03T02:59:30.100000000Z', value: 2 }]) } });
  // 2 分ちょうど (ナノ秒まで) は ok・1 ナノ秒でも超えれば STALE
  const at = (ts) => (k) => withValues(k, [{ timestamp: ts, value: series(k).values[0].value }]);
  r = await run({ memory: { body: at('2026-10-03T02:58:00.000000000Z')('memory') }, memoryLimit: { body: at('2026-10-03T02:58:00Z')('memoryLimit') } });
  assert.equal(r.ok, true, JSON.stringify(r));
  await expectReason('METRICS_STALE', { memory: { body: at('2026-10-03T02:57:59.999999999Z')('memory') } });
});
await t('空の配列・点の無い系列 → METRICS_EMPTY', async () => {
  await expectReason('METRICS_EMPTY', { diskUsage: { body: [] } });
  await expectReason('METRICS_EMPTY', { diskUsage: { body: withValues('diskUsage', []) } });
});
await t('系列が 2 つ (同じ resource の重複) → METRICS_DUPLICATE_SERIES', async () => {
  await expectReason('METRICS_DUPLICATE_SERIES', { memory: { body: [clone(series('memory')), clone(series('memory'))] } });
});
await t('同じ時刻の点が 2 つ (書き方が違っても) → METRICS_DUPLICATE_POINT', async () => {
  await expectReason('METRICS_DUPLICATE_POINT', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03T02:59:30Z', value: 1 }, { timestamp: '2026-10-03T02:59:30.000Z', value: 2 }]) } });
  await expectReason('METRICS_DUPLICATE_POINT', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03T02:59:30Z', value: 1 }, { timestamp: '2026-10-03T11:59:30+09:00', value: 2 }]) } });
});
await t('resource の label が無い・2 つ → METRICS_LABEL_MISSING / 別の resource・混ざり → METRICS_WRONG_RESOURCE', async () => {
  const s = series('diskCapacity');
  await expectReason('METRICS_LABEL_MISSING', { diskCapacity: { body: [{ ...clone(s), labels: [{ field: 'instance', value: 'x' }] }] } });
  await expectReason('METRICS_LABEL_MISSING', { diskCapacity: { body: [{ ...clone(s), labels: [{ field: 'resource', value: RID }, { field: 'resource', value: RID }] }] } });
  await expectReason('METRICS_WRONG_RESOURCE', { diskCapacity: { body: [{ ...clone(s), labels: [{ field: 'resource', value: 'dpg-other00000000000000-a' }] }] } });
  await expectReason('METRICS_WRONG_RESOURCE', { diskCapacity: { body: [clone(s), { ...clone(s), labels: [{ field: 'resource', value: 'srv-web' }] }] } });
});
await t('bytes でない単位 (MB・GB・percent・空) → METRICS_UNIT (推測で掛け算しない)', async () => {
  for (const unit of ['MB', 'GB', 'percent', '', 'BYTES', 'KiB']) await expectReason('METRICS_UNIT', { memoryLimit: { body: [{ ...clone(series('memoryLimit')), unit }] } });
});
await t('負の値 → METRICS_NEGATIVE', async () => {
  await expectReason('METRICS_NEGATIVE', { diskUsage: { body: withValues('diskUsage', [{ timestamp: '2026-10-03T02:59:30Z', value: -1 }]) } });
});
await t('未来の時刻 (1 ms でも) → METRICS_FUTURE', async () => {
  await expectReason('METRICS_FUTURE', { memory: { body: withValues('memory', [...series('memory').values, { timestamp: '2026-10-03T03:00:00.001Z', value: 1 }]) } });
});
await t('最新の点が 2 分を超えて古い → METRICS_STALE (ちょうど 2 分は ok)', async () => {
  const stale = (k) => withValues(k, [{ timestamp: '2026-10-03T02:57:59.999Z', value: series(k).values[0].value }]);
  await expectReason('METRICS_STALE', { diskCapacity: { body: stale('diskCapacity') } });
  const edge = (k) => withValues(k, [{ timestamp: '2026-10-03T02:58:00.000Z', value: series(k).values[0].value }]);
  const r = await run({ memory: { body: edge('memory') }, memoryLimit: { body: edge('memoryLimit') } });
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.equal(r.snapshot.oldestPointAt, '2026-10-03T02:58:00.000Z');
});
await t('上限・容量が 0 → METRICS_ZERO_LIMIT / 使用 > 上限・容量 → METRICS_INCONSISTENT', async () => {
  await expectReason('METRICS_ZERO_LIMIT', { diskCapacity: { body: withValues('diskCapacity', [{ timestamp: '2026-10-03T02:59:30Z', value: 0 }]) } });
  await expectReason('METRICS_INCONSISTENT', { memory: { body: withValues('memory', [{ timestamp: '2026-10-03T02:59:30Z', value: 1073741825 }]) } });
  await expectReason('METRICS_INCONSISTENT', { diskUsage: { body: withValues('diskUsage', [{ timestamp: '2026-10-03T02:59:30Z', value: 16106127361 }]) } });
});
await t('理由は METRIC_ENDPOINTS の順で最初のもの (memory → memoryLimit → diskUsage → diskCapacity)', async () => {
  await expectReason('METRICS_AUTH', { memory: { status: 401, body: '' }, diskCapacity: { status: 500, body: '' } });
  await expectReason('METRICS_UNIT', { memoryLimit: { body: [{ ...clone(series('memoryLimit')), unit: 'MB' }] }, diskUsage: { status: 429, body: '' } });
});

console.log('設定');
await t('readRenderMetricsConfig: 鍵・resource の ID が無い / 形が違う → METRICS_CONFIG・正しければ鍵は包まれて見えない', async () => {
  const bad = [{}, { RENDER_API_KEY: KEY_TEXT }, { CDB_RENDER_PG_RESOURCE_ID: RID }, { RENDER_API_KEY: KEY_TEXT, CDB_RENDER_PG_RESOURCE_ID: 'srv-abc123' },
    { RENDER_API_KEY: 'has space key', CDB_RENDER_PG_RESOURCE_ID: RID }, { RENDER_API_KEY: 'short', CDB_RENDER_PG_RESOURCE_ID: RID },
    { RENDER_API_KEY: `${KEY_TEXT}\r\nX-Evil: 1`, CDB_RENDER_PG_RESOURCE_ID: RID }, { RENDER_API_KEY: KEY_TEXT, CDB_RENDER_PG_RESOURCE_ID: `${RID}&resource=dpg-x` }];
  for (const env of bad) {
    const r = readRenderMetricsConfig(env);
    assert.deepEqual(r, { ok: false, reason: 'METRICS_CONFIG' });
    assert.ok(!JSON.stringify(r).includes(KEY_TEXT));
  }
  const r = readRenderMetricsConfig({ RENDER_API_KEY: KEY_TEXT, CDB_RENDER_PG_RESOURCE_ID: RID });
  assert.equal(r.ok, true);
  assert.equal(r.config.resourceId, RID);
  for (const text of [JSON.stringify(r), String(r.config.apiKey), `${r.config.apiKey}`, inspect(r, { depth: 10, showHidden: true }), Object.keys(r.config.apiKey).join()]) {
    assert.ok(!text.includes(KEY_TEXT), text);
  }
  assert.throws(() => new RenderApiKey(''), (e) => !String(e.message).includes(KEY_TEXT));
});
await t('fetchPostgresMetrics: 鍵が包まれていない・resource の ID が違う・送り先が公式でも 127.0.0.1 でもない・timeout が変 → METRICS_CONFIG (要求を送らない)', async () => {
  let called = 0; const fetchImpl = async () => { called++; throw new Error('x'); };
  const cases = [
    { apiKey: KEY_TEXT, resourceId: RID }, { apiKey: KEY, resourceId: 'srv-x' }, { apiKey: KEY, resourceId: RID, baseUrl: 'https://evil.example.com/v1' },
    { apiKey: KEY, resourceId: RID, baseUrl: 'http://localhost:8080' }, { apiKey: KEY, resourceId: RID, baseUrl: 'http://api.render.com/v1' },
    { apiKey: KEY, resourceId: RID, timeoutMs: 0 }, { apiKey: KEY, resourceId: RID, timeoutMs: 60000 }, { apiKey: KEY, resourceId: RID, now: () => NaN },
  ];
  for (const c of cases) {
    const r = await fetchPostgresMetrics({ fetchImpl, now: () => NOW, ...c });
    allResults.push(r);
    assert.deepEqual(r, { ok: false, reason: 'METRICS_CONFIG' }, JSON.stringify(c));
  }
  assert.equal(called, 0);
});

console.log('🚨 鍵 (RENDER_API_KEY) が出ない');
await t('どの戻り値にも鍵が無い (JSON・inspect)', async () => {
  assert.ok(allResults.length > 40, `${allResults.length}`);
  for (const r of allResults) {
    assert.ok(!JSON.stringify(r).includes(KEY_TEXT));
    assert.ok(!inspect(r, { depth: 10, showHidden: true }).includes(KEY_TEXT));
  }
});
await t('console に何も出していない (部品は console を使わない)・試験の出力にも鍵が無い', async () => {
  for (const line of consoleSeen) assert.ok(!line.includes(KEY_TEXT));
  const src = fs.readFileSync(new URL('../apps/company-db/profit/render-metrics.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(src, /console\./);
  assert.doesNotMatch(src, /rnd_[A-Za-z0-9]{8,}/);
});
await t('fixture に鍵らしい文字が無い', async () => {
  for (const f of fs.readdirSync(FIX_DIR)) {
    const text = fs.readFileSync(new URL(f, FIX_DIR), 'utf8');
    assert.ok(!text.includes(KEY_TEXT), f);
    assert.doesNotMatch(text, /rnd_[A-Za-z0-9]{8,}|Bearer|authorization/i, f);
  }
});

console.log('使う所はまだ無い (利益の受け口は 503 のまま)');
// 名前を書いただけ (コメントの参照など) は数えない。import / export from / 動的 import() / require() の指定子だけを見る
const IMPORTS_RENDER_METRICS = /(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s+|\brequire\s*\(\s*)['"`][^'"`\n]*render-metrics(?:\.m?js)?['"`]/;
await t('import の見分け: 指定子だけを数え、コメントの中の名前は数えない', async () => {
  for (const s of ["import { REASONS } from './render-metrics.mjs';", 'export { readMemory } from "../profit/render-metrics.mjs";', "const m = await import('./render-metrics.mjs');", "const m = require('./render-metrics');", "import './render-metrics.mjs';"]) assert.match(s, IMPORTS_RENDER_METRICS, s);
  for (const s of [' * metrics の client (apps/company-db/profit/render-metrics.mjs の REASONS) の理由のコード。', '// render-metrics を使う所はまだ無い', "const label = 'render-metrics';"]) assert.doesNotMatch(s, IMPORTS_RENDER_METRICS, s);
});
await t('apps の下でこの部品を import している所は無い・router は 503 の封じ込めのまま', async () => {
  const hits = [];
  const walk = (dir) => {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      if (e.name === 'node_modules') continue;
      const p = new URL(e.name + (e.isDirectory() ? '/' : ''), dir);
      if (e.isDirectory()) walk(p);
      else if (/\.(m?js|cjs)$/.test(e.name) && !p.pathname.endsWith('/profit/render-metrics.mjs') && IMPORTS_RENDER_METRICS.test(fs.readFileSync(p, 'utf8'))) hits.push(p.pathname);
    }
  };
  walk(new URL('../apps/', import.meta.url));
  assert.deepEqual(hits, []);
  const router = fs.readFileSync(new URL('../apps/company-db/router.mjs', import.meta.url), 'utf8');
  assert.match(router, /router\.get\(`\/amazon-profit\/\$\{kind\}`, requireSyncKey, \(req, res\) => res\.status\(503\)\.json\(PROFIT_ROUTE_DISABLED\)\)/);
});

await new Promise((resolve) => server.close(resolve));
console.log(`\n${ok} 件 PASS${ng ? ` / ${ng} 件 NG` : ''}`);
// 🚨 fetch の直後に process.exit() すると Windows の Node で libuv の assertion が出て終了コードが 127 になる = exitCode を置いて自然に終わらせる (保険に unref つきの setTimeout)
process.exitCode = ng ? 1 : 0;
setTimeout(() => process.exit(ng ? 1 : 0), 10000).unref();
