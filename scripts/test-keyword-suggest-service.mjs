import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * Amazon サジェスト収集 (apps/keyword-researcher/suggest.js + apps/warehouse/keyword-suggest-service.js)
 *
 * 実行: node scripts/test-keyword-suggest-service.mjs
 *
 * 守りたいこと (SP広告KW 設計 §4.3・Codex R1 #8 / R2 #3):
 *   ① 通信の失敗を「0 件」と混ぜない (prefix ごとに success / empty / failed / unrun)
 *   ② タイムアウトで止まらない・再試行は 1 回まで
 *   ③ 総リクエスト数の上限に達したら残りは unrun として残す (黙って飛ばさない)
 *   ④ 既存の戻り値 (seed / total / suggestions[]{keyword, source, depth}) は変わらない (MCP と router は素通し)
 *   ⑤ service-api の口: 種の検査・深掘り拒否・同時 1 本 (待ちが溜まれば 429)・状態つきで返す
 * 本物の Amazon は呼ばない (fetch を差し替える)。
 */
import http from 'http';

const express = (await import('express')).default;
const sug = await import('../apps/keyword-researcher/suggest.js');
const svc = await import('../apps/warehouse/keyword-suggest-service.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** 偽の Amazon。prefix → 応答 を決められる。calls に (prefix, UA) を記録 */
const calls = [];
let behavior = () => ({ suggestions: [] });
sug._setFetchForTest(async (url, init) => {
  const u = new URL(url);
  const prefix = u.searchParams.get('prefix');
  calls.push({ prefix, ua: init.headers['User-Agent'] });
  const b = behavior(prefix, init);
  if (b instanceof Promise) return b;
  if (b.throw) throw b.throw;
  if (b.hang) {
    // タイムアウトまで返さない (abort で終わる)
    return new Promise((_, rej) => { init.signal.addEventListener('abort', () => { const e = new Error('aborted'); e.name = 'AbortError'; rej(e); }); });
  }
  return { ok: b.status ? b.status < 400 : true, status: b.status || 200, json: async () => {
    if (b.badJson) throw new Error('bad');
    if ('raw' in b) return b.raw;   // 形が想定外の 200 応答を再現する
    return { suggestions: (b.suggestions || []).map(v => ({ value: v })) };
  } };
});
const reset = () => { calls.length = 0; };
const FAST = { delayMs: 1, timeoutMs: 200 };

console.log('[1] prefix ごとの状態: 失敗と 0 件を混ぜない');
{
  reset();
  behavior = (prefix) => {
    if (prefix === 'ハッカ油') return { suggestions: ['ハッカ油 スプレー', 'ハッカ油 虫除け'] };
    if (prefix === 'ハッカ油 あ') return { suggestions: [] };            // 本当に 0 件
    if (prefix === 'ハッカ油 い') return { status: 503 };               // 落ちている
    if (prefix === 'ハッカ油 う') return { throw: new Error('ECONNRESET') };
    if (prefix === 'ハッカ油 え') return { badJson: true };
    return { suggestions: [`${prefix}の候補`] };
  };
  const r = await sug.getSuggestions('ハッカ油', { ...FAST, retries: 0 });
  const st = Object.fromEntries(r.prefixes.map(p => [p.prefix, p.status]));
  eq(st['ハッカ油'], 'success', '基本は success');
  eq(st['ハッカ油 あ'], 'empty', '0 件は empty');
  eq(st['ハッカ油 い'], 'failed', 'HTTP 503 は failed (empty ではない)');
  eq(st['ハッカ油 う'], 'failed', '通信例外は failed');
  eq(st['ハッカ油 え'], 'failed', 'JSON でない応答は failed');
  ok(r.prefixes.find(p => p.prefix === 'ハッカ油 い').error === 'HTTP 503', 'failed には理由が付く');
  eq(r.summary.requested, 47, '基本 1 + ひらがな 46 = 47 prefix');
  eq([r.summary.success, r.summary.empty, r.summary.failed, r.summary.unrun], [43, 1, 3, 0], '内訳');
  ok(r.suggestions.some(s => s.keyword === 'ハッカ油 スプレー' && s.source === 'base' && s.depth === 0), '既存の項目 (keyword/source/depth) はそのまま');
  ok(!r.suggestions.some(s => s.keyword === 'ハッカ油'), 'seed そのものは候補に入れない (これまでどおり)');
  ok(typeof r.total === 'number' && r.total === r.suggestions.length, 'total もそのまま');
  ok(r.prefixes.every(p => p.status === 'unrun' || p.fetchedAt), '取りに行った prefix には取得時刻が付く');
}

console.log('[1b] HTTP 200 でも形が想定外なら failed (0 件と混ぜない) — PR #1408 R1 #4');
{
  reset();
  behavior = (prefix) => {
    if (prefix === 'w') return { raw: {} };
    if (prefix === 'w あ') return { raw: null };
    if (prefix === 'w い') return { raw: { error: 'blocked' } };
    if (prefix === 'w う') return { raw: { suggestions: [{ foo: 1 }, { value: 42 }] } };   // 要素に value が無い
    if (prefix === 'w え') return { raw: { suggestions: [] } };                           // 本当の 0 件
    return { suggestions: [] };
  };
  const r = await sug.getSuggestions('w', { ...FAST, retries: 0 });
  const st = Object.fromEntries(r.prefixes.map(p => [p.prefix, p.status]));
  eq([st['w'], st['w あ'], st['w い'], st['w う']], ['failed', 'failed', 'failed', 'failed'], '{} / null / エラーオブジェクト / value 無しは failed');
  eq(st['w え'], 'empty', 'suggestions: [] だけが empty');
  ok(/想定外/.test(r.prefixes.find(p => p.prefix === 'w').error), `理由に「想定外」: ${r.prefixes.find(p => p.prefix === 'w').error}`);
  // 正常な要素と value 欠落の要素が混在 → 欠落分を黙って捨てて success にしない (R2 #8)
  reset(); behavior = () => ({ raw: { suggestions: [{ value: 'ok' }, { foo: 1 }] } });
  const m = await sug.getSuggestions('m', { ...FAST, retries: 0, hiragana: false });
  eq([m.prefixes[0].status, m.suggestions.length], ['failed', 0], '混在は failed (黙って捨てない)');
}

console.log('[2] 再試行は 1 回まで・成功したら止める');
{
  reset();
  let n = 0;
  behavior = (prefix) => {
    if (prefix === 'x か') { n++; return n === 1 ? { throw: new Error('flaky') } : { suggestions: ['x か 復活'] }; }
    if (prefix === 'x き') return { throw: new Error('dead') };
    return { suggestions: [] };
  };
  const r = await sug.getSuggestions('x', { ...FAST, retries: 1 });
  const pk = r.prefixes.find(p => p.prefix === 'x か'), pki = r.prefixes.find(p => p.prefix === 'x き');
  eq([pk.status, pk.attempts], ['success', 2], '1 回失敗 → 再試行で成功');
  eq([pki.status, pki.attempts], ['failed', 2], 'ずっと失敗 → 2 回 (=再試行 1 回) で諦める');
  ok(r.summary.requests === r.prefixes.reduce((a, p) => a + p.attempts, 0), 'requests は実際に叩いた回数');
}

console.log('[3] タイムアウトで止まらない');
{
  reset();
  behavior = (prefix) => (prefix === 'y' ? { hang: true } : { suggestions: [] });
  const t0 = Date.now();
  const r = await sug.getSuggestions('y', { ...FAST, timeoutMs: 120, retries: 0, hiragana: false });
  const p = r.prefixes[0];
  eq(p.status, 'failed', '止まったら failed');
  ok(/timeout/.test(p.error), `理由に timeout: ${p.error}`);
  ok(Date.now() - t0 < 2000, '待ち続けない');
}

console.log('[4] 総リクエスト数の上限 → 残りは unrun (黙って飛ばさない)');
{
  reset();
  behavior = () => ({ suggestions: ['a'] });
  const r = await sug.getSuggestions('z', { ...FAST, maxRequests: 10, retries: 0 });
  eq(r.summary.requests, 10, '10 回で止まる');
  eq(r.summary.unrun, 37, '残り 37 prefix は unrun');
  ok(r.prefixes.filter(p => p.status === 'unrun').every(p => p.fetchedAt === null && /maxRequests/.test(p.error)), 'unrun は取得時刻なし・理由つき');
  eq(r.prefixes.length, 47, 'prefix の記録は全部残る');
}

console.log('[4b] 全体の期限 → 残りは unrun (理由 = 期限)・summary.stopped');
{
  reset();
  behavior = () => new Promise(r => setTimeout(() => r({ ok: true, status: 200, json: async () => ({ suggestions: [{ value: 'a' }] }) }), 30));
  const t0 = Date.now();
  const r = await sug.getSuggestions('d', { ...FAST, retries: 0, deadlineMs: 120 });
  ok(Date.now() - t0 < 250, `期限 (120ms) のすぐあとに戻る (${Date.now() - t0}ms)`);
  ok(r.summary.unrun > 0 && r.summary.success > 0, `取れた分 (${r.summary.success}) と未実行 (${r.summary.unrun}) が分かれる`);
  eq(r.summary.stopped, 'deadline', 'stopped = deadline');
  ok(r.prefixes.filter(p => p.status === 'unrun').every(p => /期限/.test(p.error)), 'unrun の理由 = 期限');
  eq(r.prefixes.length, 47, 'prefix の記録は全部残る');
  eq(r.options.deadlineMs, 120, 'options に期限が残る');

  // 期限は実行中の取得にも効く (R2 #1): 期限の直前に始まった遅い取得を待ち続けない。その 1 回は失敗ではなく unrun (期限)
  reset();
  behavior = () => new Promise(r => setTimeout(() => r({ ok: true, status: 200, json: async () => ({ suggestions: [] }) }), 500));
  const t1 = Date.now();
  const r2 = await sug.getSuggestions('e', { ...FAST, timeoutMs: 5000, retries: 0, hiragana: false, deadlineMs: 100 });
  ok(Date.now() - t1 < 300, `実行中の取得 (500ms) を期限 (100ms) で切る (${Date.now() - t1}ms)`);
  eq([r2.prefixes[0].status, r2.summary.stopped], ['unrun', 'deadline'], '期限で切られた試行は failed ではなく unrun (理由 = 期限)');
  ok(/期限/.test(r2.prefixes[0].error), `理由: ${r2.prefixes[0].error}`);

  // fetch が signal を無視して決着しなくても戻る (R2 #2)。戻ったあと新しい送信はしない
  reset();
  behavior = () => new Promise(() => {});   // 永遠に返さない・abort も無視
  const t2 = Date.now();
  const r3 = await sug.getSuggestions('f', { ...FAST, timeoutMs: 5000, retries: 0, deadlineMs: 100 });
  ok(Date.now() - t2 < 300, `signal を無視する fetch でも期限で戻る (${Date.now() - t2}ms)`);
  eq(calls.length, 1, '戻ったあと Amazon に新しい送信をしない (送ったのは最初の 1 回だけ)');
  eq(r3.summary.unrun, 47, '全部 unrun (取れたと言わない)');
}

console.log('[4d] 再試行待ちの中断で、確定した失敗を未実行に変えない (R2 #7)');
{
  reset();
  const ac = new AbortController();
  behavior = () => ({ status: 503 });
  setTimeout(() => ac.abort(), 30);
  const r = await sug.getSuggestions('g', { delayMs: 100, timeoutMs: 200, retries: 1, hiragana: false, signal: ac.signal });
  eq([r.prefixes[0].status, r.prefixes[0].attempts, r.prefixes[0].error], ['failed', 1, 'HTTP 503'], '503 → 再試行待ちの間に中断 → failed のまま (attempts 1)');
}

console.log('[4c] 外からの中断 (signal) → 以後は unrun (理由 = 中断)・途中の 1 回は failed にしない');
{
  reset();
  const ac = new AbortController();
  behavior = () => new Promise((res, rej) => setTimeout(() => res({ ok: true, status: 200, json: async () => ({ suggestions: [] }) }), 40));
  setTimeout(() => ac.abort(), 60);
  const r = await sug.getSuggestions('c', { ...FAST, retries: 0, signal: ac.signal });
  eq(r.summary.stopped, 'aborted', 'stopped = aborted');
  ok(r.summary.unrun >= 40 && r.summary.failed === 0, `中断後は failed ではなく unrun (unrun ${r.summary.unrun} / failed ${r.summary.failed})`);
  ok(r.prefixes.filter(p => p.status === 'unrun').every(p => /中断/.test(p.error)), 'unrun の理由 = 中断');
  ok(calls.length <= 3, `中断後は Amazon を叩かない (叩いた回数 ${calls.length})`);
}

console.log('[5] UA の切替');
{
  reset(); behavior = () => ({ suggestions: [] });
  await sug.getSuggestions('u', { ...FAST, hiragana: false });
  ok(/Mozilla/.test(calls[0].ua), '既定はブラウザ UA (これまでどおり)');
  reset();
  await sug.getSuggestions('u', { ...FAST, hiragana: false, userAgent: 'plain' });
  ok(/bfaith-portal/.test(calls[0].ua) && !/Mozilla/.test(calls[0].ua), 'plain で素の UA');
}

console.log('[6] service-api の口');
{
  const app = express();
  app.use(express.json());
  app.use('/service-api/keyword-suggest', svc.default);
  const server = http.createServer(app);
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = async (body) => {
    const res = await fetch(base + '/service-api/keyword-suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    return { status: res.status, body: await res.json().catch(() => ({})) };
  };
  eq((await call({})).status, 400, 'seed 無しは 400');
  eq((await call({ seed: '   ' })).status, 400, '空白だけは 400');
  eq((await call({ seed: 'a'.repeat(61) })).status, 400, '61 文字は 400');
  eq((await call({ seed: 'x', depth: 2 })).status, 400, '深掘りは受け付けない');
  ok(svc.normalizeSeed(' ハッカ油 \t スプレー ').seed === 'ハッカ油 スプレー', '空白を 1 つに寄せる');
  ok(!svc.normalizeSeed('a\x00b').seed?.includes('\x00'), '制御文字を落とす');

  reset(); behavior = (prefix) => ({ suggestions: prefix === 'ひば油' ? ['ひば油 スプレー'] : [] });
  const r = await call({ seed: 'ひば油', hiragana: false });
  eq(r.status, 200, '正常');
  ok(r.body.ok === true && r.body.result && r.body.result.seed === 'ひば油', 'okResponse の形 {ok, result}');
  ok(Array.isArray(r.body.result.prefixes) && r.body.result.summary, '状態つきで返す');
  ok(r.body.result.options.depth === 1 && r.body.result.options.maxRequests === svc.MAX_REQUESTS, '深掘りなし・上限つきで呼んでいる');

  // 同時 1 本: 走っている間に来た依頼は待たせずに即 429 (待ち行列を使わない — 待たされた分だけ Render の 45 秒を食う。R2 #3)
  reset(); behavior = () => new Promise(r => setTimeout(() => r({ ok: true, status: 200, json: async () => ({ suggestions: [] }) }), 150));
  const inflight = [];
  for (let i = 0; i < 7; i++) inflight.push(call({ seed: `q${i}`, hiragana: false }));
  const t429 = Date.now();
  const results = await Promise.all(inflight);
  const s429 = results.filter(x => x.status === 429).length;
  eq(s429, 6, `同時 7 本 → 1 本だけ走り 6 本は 429 (${s429} 本)`);
  eq(results.filter(x => x.status === 200).length, 1, '走った 1 本は 200 で状態つきで返る');
  ok(Date.now() - t429 < 600, `429 は待たされない (全部で ${Date.now() - t429}ms)`);
  ok(calls.length === 1, `Amazon を叩いたのは 1 本分 (${calls.length} 回)`);

  // 🚨 同時 1 本は「収集そのもの」で守る (PR #1408 R1 #1): 呼び手が切断しても収集が終わるまで次を入れない
  reset();
  behavior = () => new Promise(r => setTimeout(() => r({ ok: true, status: 200, json: async () => ({ suggestions: [{ value: 'slow' }] }) }), 300));
  const ac1 = new AbortController();
  const first = fetch(base + '/service-api/keyword-suggest', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ seed: 'slow1', hiragana: false }), signal: ac1.signal,
  }).catch(() => null);
  await sleep(40);
  ac1.abort();                    // 呼び手 (Render) が待ち切れずに切った
  await first;
  await sleep(40);
  ok(svc._activeForTest() && svc._activeForTest().seed === 'slow1', '切断されても収集はまだ走っている (active が残る)');
  const second = await call({ seed: 'slow2', hiragana: false });
  eq(second.status, 429, '走っている間の次の依頼は 429 (接続の有無にかかわらず)');
  ok(/別の収集/.test(second.body.message || ''), `理由に「別の収集」: ${second.body.message}`);
  ok(!calls.some(c => c.prefix === 'slow2'), '429 になった種は Amazon を叩いていない');
  await sleep(400);
  ok(svc._activeForTest() === null, '収集が終われば active が消える');
  const third = await call({ seed: 'slow3', hiragana: false });
  eq(third.status, 200, '終わったあとは 200');
  ok(third.body.result.options.deadlineMs === svc.DEADLINE_MS && svc.DEADLINE_MS < 45_000, '全体の期限つきで呼んでいる (Render の 45 秒より短い)');
  server.close();
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
