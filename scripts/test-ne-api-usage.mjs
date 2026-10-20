/**
 * test-ne-api-usage.mjs — NE の API の回数を数える (apps/warehouse/ne-api-usage.js・2026-10-19)
 *
 * 固定する契約:
 *   1 月は JST で切る (UTC 10/31 15:00 = JST 11/01 は 11 月)。1 回 = 1 行・日ごと・API ごと・呼んだもの ごとに数える。壊れた行は数えない
 *   2 知らせる数 (NE_API_MONTHLY_WARN) の読み方・まとめの 1 行 (ふだん / ⚠️ / 1,000 回を超えた)
 *   3 知らせはその数ごとに月 1 回だけ (送れなかった朝は次の朝にまた送る・月が変われば数え直す・送る関数が投げても止めない)
 *   4 置き場所が壊れていても throw しない (書けない・読めない)
 *   5 callNE の返す値・投げるエラーは前と同じで、NE に送った回だけ 1 行 (トークンが無くて送らなかった回は数えない・置き場所が壊れても取得は通る)
 *   6 商品の取得 (ページあり) で、NE に送った回数 = 数えた回数 (2 回数えない)
 * 使い方: node scripts/test-ne-api-usage.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ne-api-usage-'));
process.env.DATA_DIR = tmp;
delete process.env.NE_API_CALLER;

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; } };

const U = await import('../apps/warehouse/ne-api-usage.js');
const freshDir = (name) => { const d = path.join(tmp, name); fs.mkdirSync(d, { recursive: true }); return d; };
const fill = (dataDir, n, now, ep = '/api_v1_master_goods/search') => { for (let i = 0; i < n; i++) U.recordNeApiCall({ endpoint: ep, ok: true, result: 'success' }, { dataDir, now }); };

await ta('[1] 月は JST で切る・日ごと / API ごと / 呼んだもの ごと・壊れた行は数えない', async () => {
  const d = freshDir('m1');
  assert.equal(U.jstMonthOf(new Date('2026-10-31T14:59:59Z')), '2026-10');
  assert.equal(U.jstMonthOf(new Date('2026-10-31T15:00:00Z')), '2026-11');
  assert.equal(U.jstMonthOf(new Date('2026-12-31T15:00:00Z')), '2027-01');
  assert.equal(U.prevMonth('2027-01'), '2026-12');
  assert.equal(U.prevMonth('2026-11'), '2026-10');
  U.recordNeApiCall({ endpoint: '/api_v1_master_goods/search', ok: true, result: 'success', ms: 12.4 }, { dataDir: d, now: new Date('2026-10-31T14:59:59Z') });
  U.recordNeApiCall({ endpoint: '/api_v1_receiveorder_base/search', ok: false, result: 'error' }, { dataDir: d, now: new Date('2026-10-31T15:00:00Z') });
  U.recordNeApiCall({ endpoint: '/api_v1_receiveorder_base/search', ok: true, result: 'success' }, { dataDir: d, now: new Date('2026-11-01T03:00:00Z') });
  assert.ok(fs.existsSync(path.join(d, 'ne-api-usage', '2026-10.jsonl')));
  assert.ok(fs.existsSync(path.join(d, 'ne-api-usage', '2026-11.jsonl')));
  const oct = U.readMonthUsage('2026-10', { dataDir: d });
  assert.equal(oct.total, 1); assert.deepEqual(oct.byDay, { '2026-10-31': 1 });
  const nov = U.readMonthUsage('2026-11', { dataDir: d });
  assert.equal(nov.total, 2); assert.equal(nov.ok, 1); assert.equal(nov.error, 1);
  assert.deepEqual(nov.byDay, { '2026-11-01': 2 });
  assert.deepEqual(nov.byEndpoint, { '/api_v1_receiveorder_base/search': 2 });
  assert.equal(Object.keys(nov.byCaller).length, 1);
  assert.match(Object.keys(nov.byCaller)[0], /test-ne-api-usage\.mjs/);
  // 壊れた行・別の月の行は数えない
  fs.appendFileSync(path.join(d, 'ne-api-usage', '2026-11.jsonl'), '{"t":"2026-11-01T0\n' + JSON.stringify({ t: '2026-10-01T00:00:00Z', ep: 'x', ok: true }) + '\n');
  const nov2 = U.readMonthUsage('2026-11', { dataDir: d });
  assert.equal(nov2.total, 2); assert.equal(nov2.badLines, 2);
  // 無い月 = 0 回
  assert.equal(U.readMonthUsage('2025-01', { dataDir: d }).total, 0);
  // 1 行の中身 (秘密 = トークン・送った値 は書かない)
  const line = JSON.parse(fs.readFileSync(path.join(d, 'ne-api-usage', '2026-10.jsonl'), 'utf8').trim());
  assert.deepEqual(Object.keys(line).sort(), ['ep', 'job', 'ms', 'ok', 'pid', 'r', 't']);
  assert.equal(line.ms, 12);
});

await ta('[2] 知らせる数の読み方・まとめの 1 行', async () => {
  assert.deepEqual(U.warnThresholds({}), [700, 900, 1000]);
  assert.deepEqual(U.warnThresholds({ NE_API_MONTHLY_WARN: '800' }), [800]);
  assert.deepEqual(U.warnThresholds({ NE_API_MONTHLY_WARN: ' 900, 600 ,900' }), [600, 900]);
  assert.deepEqual(await quietly(() => U.warnThresholds({ NE_API_MONTHLY_WARN: 'abc' })), [700, 900, 1000]);
  const th = [700, 900, 1000];
  let p = U.planUsageNotice({ total: 312, thresholds: th });
  assert.equal(p.line, 'NE の API: 今月 312 回 (無料は 1,000 回まで)'); assert.equal(p.warn, false); assert.deepEqual(p.toNotify, []);
  p = U.planUsageNotice({ total: 699, thresholds: th });
  assert.equal(p.warn, false); assert.deepEqual(p.toNotify, []);
  p = U.planUsageNotice({ total: 700, thresholds: th });
  assert.equal(p.line, '⚠️ NE の API: 今月 700 回 (無料は 1,000 回まで)'); assert.equal(p.warn, true); assert.deepEqual(p.toNotify, [700]);
  p = U.planUsageNotice({ total: 950, thresholds: th, alreadyNotified: [700] });
  assert.deepEqual(p.toNotify, [900]);
  p = U.planUsageNotice({ total: 950, thresholds: th, alreadyNotified: [700, 900] });
  assert.deepEqual(p.toNotify, []);
  p = U.planUsageNotice({ total: 1001, thresholds: th, alreadyNotified: [700, 900] });
  assert.match(p.line, /^⚠️ NE の API: 今月 1,001 回 = 無料の 1,000 回を超えた/); assert.deepEqual(p.toNotify, [1000]);
  p = U.planUsageNotice({ total: 7, thresholds: th, prevMonthTotal: 245 });
  assert.equal(p.line, 'NE の API: 今月 7 回 (無料は 1,000 回まで) (先月 245 回)');
});

await ta('[3] 知らせはその数ごとに月 1 回だけ (送れなかった朝は次にまた・月が変われば数え直す・送る関数が投げても止めない)', async () => {
  const d = freshDir('m3');
  const day = new Date('2026-10-20T00:00:00Z');   // JST 10/20 09:00
  const sent = [];
  const ok = async (t) => { sent.push(t); return true; };
  const run = (opt) => quietly(() => U.neApiUsageForDailySync({ dataDir: d, env: {}, now: day, ...opt }));

  fill(d, 699, day);
  let r = await run({ notify: ok });
  assert.equal(r.total, 699); assert.equal(r.warn, false); assert.equal(sent.length, 0);
  assert.equal(r.line, 'NE の API: 今月 699 回 (無料は 1,000 回まで)');

  fill(d, 1, day);   // 700
  // 送れなかった (false) = 印を付けない
  r = await run({ notify: async () => false });
  assert.equal(r.warn, true); assert.deepEqual(r.notified, []);
  // 送る関数が投げても止めない
  r = await run({ notify: async () => { throw new Error('boom'); } });
  assert.deepEqual(r.notified, []);
  // 送れた = 1 回だけ
  r = await run({ notify: ok });
  assert.deepEqual(r.notified, [700]); assert.equal(sent.length, 1);
  assert.match(sent[0], /今月 700 回/); assert.match(sent[0], /2026-10/);
  r = await run({ notify: ok });
  r = await run({ notify: ok });
  assert.equal(sent.length, 1, '同じ月に 2 回知らせない');
  assert.match(r.line, /^⚠️ /);

  fill(d, 250, day);   // 950 = 900 を超えた
  r = await run({ notify: ok });
  assert.deepEqual(r.notified, [900]); assert.equal(sent.length, 2);
  r = await run({ notify: ok });
  assert.equal(sent.length, 2);

  // 一度に 2 つ超えた = 知らせは 1 通 (数を両方書く)
  const d2 = freshDir('m3b');
  fill(d2, 1000, day);
  const sent2 = [];
  r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d2, env: {}, now: day, notify: async (t) => { sent2.push(t); return true; } }));
  assert.equal(sent2.length, 1); assert.deepEqual(r.notified, [700, 900, 1000]);

  // 月が変われば数え直す (JST 11/01 07:00 = UTC 10/31 22:00)。1 日は先月の回数も添える
  const nov1 = new Date('2026-10-31T22:00:00Z');
  fill(d, 3, nov1);
  r = await run({ notify: ok, now: nov1 });
  assert.equal(r.total, 3); assert.equal(r.warn, false); assert.equal(sent.length, 2);
  assert.equal(r.line, 'NE の API: 今月 3 回 (無料は 1,000 回まで) (先月 950 回)');
  fill(d, 700, nov1);
  r = await run({ notify: ok, now: nov1 });
  assert.deepEqual(r.notified, [700]); assert.equal(sent.length, 3, '新しい月は 700 をまた知らせる');

  // 月末の朝に送れなかった (or 月末の朝より後に超えた) 先月の数は、翌月の朝に 1 回だけ知らせる
  const d4 = freshDir('m3d');
  const oct31 = new Date('2026-10-30T22:00:00Z');   // JST 10/31 07:00
  fill(d4, 650, oct31);
  r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d4, env: {}, now: oct31, notify: ok }));
  assert.deepEqual(r.notified, []);
  fill(d4, 100, new Date('2026-10-31T10:00:00Z'));   // JST 10/31 19:00 に手で流した = 750
  const sent4 = [];
  const ok4 = async (t) => { sent4.push(t); return true; };
  r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d4, env: {}, now: nov1, notify: async () => false }));
  assert.deepEqual(r.notifiedPrev, []);
  r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d4, env: {}, now: nov1, notify: ok4 }));
  assert.deepEqual(r.notifiedPrev, [700]); assert.deepEqual(r.notified, []);
  assert.equal(sent4.length, 1); assert.match(sent4[0], /先月 \(2026-10\) の NE の API は 750 回/);
  assert.equal(r.line, 'NE の API: 今月 0 回 (無料は 1,000 回まで) (先月 750 回)');
  r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d4, env: {}, now: new Date('2026-11-01T22:00:00Z'), notify: ok4 }));
  assert.equal(sent4.length, 1, '先月の分も 2 回知らせない');
  assert.equal(r.line, 'NE の API: 今月 0 回 (無料は 1,000 回まで)');

  // 知らせる数を env で変える
  const d3 = freshDir('m3c');
  fill(d3, 500, day);
  const sent3 = [];
  r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d3, env: { NE_API_MONTHLY_WARN: '500' }, now: day, notify: async (t) => { sent3.push(t); return true; } }));
  assert.equal(r.warn, true); assert.equal(sent3.length, 1);

  // notify が無い = 行だけ
  r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d3, env: { NE_API_MONTHLY_WARN: '400' }, now: day }));
  assert.equal(r.warn, true); assert.deepEqual(r.notified, []);
});

await ta('[4] 置き場所が壊れていても throw しない (書けない・読めない)', async () => {
  const d = freshDir('m4');
  fs.writeFileSync(path.join(d, 'ne-api-usage'), 'これはファイル');   // ディレクトリを作れない
  await quietly(async () => {
    assert.doesNotThrow(() => U.recordNeApiCall({ endpoint: '/x', ok: true }, { dataDir: d }));
    assert.doesNotThrow(() => U.recordNeApiCall({ endpoint: '/x', ok: true }, { dataDir: d }));
  });
  // 読めない (月のファイルがディレクトリ) = ⚠️ の行を返すだけ
  const d2 = freshDir('m4b');
  fs.mkdirSync(path.join(d2, 'ne-api-usage', '2026-10.jsonl'), { recursive: true });
  const r = await quietly(() => U.neApiUsageForDailySync({ dataDir: d2, env: {}, now: new Date('2026-10-20T00:00:00Z'), notify: async () => true }));
  assert.equal(r.total, null); assert.match(r.line, /^⚠️ NE の API: 今月の回数を読めない/);
  // 知らせた印が書けなくても止めない
  const d3 = freshDir('m4c');
  fill(d3, 700, new Date('2026-10-20T00:00:00Z'));
  fs.mkdirSync(path.join(d3, 'ne-api-usage', 'alerts.json'));
  const r3 = await quietly(() => U.neApiUsageForDailySync({ dataDir: d3, env: {}, now: new Date('2026-10-20T00:00:00Z'), notify: async () => true }));
  assert.deepEqual(r3.notified, [700]);
  // 1,000 回書いても遅くない (1 回 5ms 未満)
  const d4 = freshDir('m4d');
  const t0 = performance.now();
  fill(d4, 1000, new Date());
  const per = (performance.now() - t0) / 1000;
  assert.ok(per < 5, `1 回 ${per.toFixed(2)}ms`);
});

// ─── callNE (ne-api.js) ───
fs.writeFileSync(path.join(tmp, 'ne-tokens.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));
const ne = { mode: 'success', calls: 0, goods: [] };
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, opts) => {
  const u = String(url);
  if (!u.startsWith('https://api.next-engine.org')) return realFetch(url, opts);
  ne.calls++;
  const ep = u.slice('https://api.next-engine.org'.length);
  if (ne.mode === 'network') throw new Error('fetch failed');
  if (ne.mode === 'badjson') return { ok: false, status: 502, json: async () => { throw new SyntaxError('Unexpected token <'); } };
  if (ne.mode === 'error') return { ok: true, status: 200, json: async () => ({ result: 'error', code: '002002', message: 'だめ' }) };
  if (ep === '/api_v1_master_goods/search') {
    const q = new URLSearchParams(opts.body);
    const offset = Number(q.get('offset')), limit = Number(q.get('limit'));
    return { ok: true, status: 200, json: async () => ({ result: 'success', count: '1', data: ne.goods.slice(offset, offset + limit) }) };
  }
  return { ok: true, status: 200, json: async () => ({ result: 'success', count: '1', data: [{ a: '1' }] }) };
};
const NE = await quietly(() => import('../apps/warehouse/ne-api.js'));
const month = () => U.jstMonthOf(new Date());
const usage = () => U.readMonthUsage(month(), { dataDir: tmp });

await ta('[5] callNE の返す値・投げるエラーは前と同じ・NE に送った回だけ 1 行', async () => {
  ne.mode = 'success';
  const before = usage().total;
  const data = await quietly(() => NE.callNE('/api_v1_system_que/search', { fields: 'que_id' }));
  assert.deepEqual(data, { result: 'success', count: '1', data: [{ a: '1' }] });
  let u = usage();
  assert.equal(u.total, before + 1); assert.equal(u.byEndpoint['/api_v1_system_que/search'], 1);

  ne.mode = 'error';
  await assert.rejects(() => quietly(() => NE.callNE('/api_v1_receiveorder_base/update', {})), { message: 'NE API エラー: だめ' });
  ne.mode = 'network';
  await assert.rejects(() => quietly(() => NE.callNE('/api_v1_receiveorder_base/shipped', {})), { message: 'fetch failed' });
  ne.mode = 'badjson';
  await assert.rejects(() => quietly(() => NE.callNE('/api_v1_receiveorder_base/search', {})), SyntaxError);
  u = usage();
  assert.equal(u.total, before + 4); assert.equal(u.error, 3);
  const lines = fs.readFileSync(path.join(tmp, 'ne-api-usage', `${month()}.jsonl`), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.deepEqual(lines.slice(-4).map((l) => [l.ep, l.ok, l.r]), [
    ['/api_v1_system_que/search', true, 'success'],
    ['/api_v1_receiveorder_base/update', false, 'error'],
    ['/api_v1_receiveorder_base/shipped', false, 'fail'],
    ['/api_v1_receiveorder_base/search', false, 'fail'],
  ]);
  for (const l of lines) assert.ok(!JSON.stringify(l).includes('access_token'));

  // トークンが無い = NE に送らない = 数えない
  fs.renameSync(path.join(tmp, 'ne-tokens.json'), path.join(tmp, 'ne-tokens.json.off'));
  ne.mode = 'success';
  const c0 = ne.calls;
  await assert.rejects(() => NE.callNE('/api_v1_master_goods/search', {}), /トークンがありません/);
  assert.equal(ne.calls, c0); assert.equal(usage().total, before + 4);
  fs.renameSync(path.join(tmp, 'ne-tokens.json.off'), path.join(tmp, 'ne-tokens.json'));
});

await ta('[6] 商品の取得 (ページあり): NE に送った回数 = 数えた回数', async () => {
  ne.mode = 'success';
  ne.goods = Array.from({ length: NE.NE_SEARCH_LIMIT + 5 }, (_, i) => ({ goods_id: `G${i}`, goods_name: `商品${i}` }));
  const c0 = ne.calls, u0 = usage().total;
  await quietly(() => NE.fetchProducts());
  const sentCount = ne.calls - c0;
  assert.equal(sentCount, 2, 'ページは 2 回');
  assert.equal(usage().total - u0, sentCount);
});

await ta('[5b] 置き場所が壊れていても callNE は同じ値を返す', async () => {
  const dir = path.join(tmp, 'ne-api-usage');
  fs.renameSync(dir, `${dir}.moved`);
  fs.writeFileSync(dir, 'これはファイル');
  ne.mode = 'success';
  const data = await quietly(() => NE.callNE('/api_v1_system_que/search', {}));
  assert.deepEqual(data, { result: 'success', count: '1', data: [{ a: '1' }] });
});

globalThis.fetch = realFetch;
console.log(`\n${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
try { const { getDB } = await import('../apps/warehouse/db.js'); getDB()?.close?.(); } catch { /* 閉じられなくてもよい */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* 一時フォルダ */ }
