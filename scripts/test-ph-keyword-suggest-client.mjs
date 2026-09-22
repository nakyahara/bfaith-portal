import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * product-hub → miniPC サジェスト収集クライアント (apps/product-hub/lib/keyword-suggest-client.js)
 *
 * 実行: node scripts/test-ph-keyword-suggest-client.mjs
 *
 * 守りたいこと:
 *   ① トークン未設定なら miniPC を叩かない (空トークンで叩かない)
 *   ② miniPC が落ちている / 混んでいる / 応答の形が古い / 時間切れ を**呼び手が見分けられる**形で返し、throw しない
 *   ③ 状態 (prefixes/summary) の無い応答は受け取らない — 失敗と 0 件を混ぜないため
 * 本物の miniPC は呼ばない (fetcher を差し替える)。
 */
const mod = await import('../apps/product-hub/lib/keyword-suggest-client.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

console.log('[1] トークン未設定なら叩かない');
{
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  let called = 0;
  mod._setSuggestFetcher(async () => { called++; return {}; });
  const r = await mod.collectSuggestions('ハッカ油');
  eq([r.ok, r.code, called], [false, 'not_configured', 0], '叩かずに not_configured');
}

process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';

console.log('[2] 失敗の種類を見分けて返す (throw しない)');
{
  const err = (code, message, name) => { const e = new Error(message); if (code) e.code = code; if (name) e.name = name; return e; };
  mod._setSuggestFetcher(async () => { throw err('busy', '混んでいる'); });
  let r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'busy'], '429 → busy');
  mod._setSuggestFetcher(async () => { throw err('unreachable', 'HTTP 502'); });
  r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'unreachable'], '非 2xx → unreachable');
  mod._setSuggestFetcher(async () => { throw err(null, 'The operation was aborted due to timeout', 'TimeoutError'); });
  r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'timeout'], '時間切れ → timeout');
  ok(/秒以内/.test(r.message), `時間切れの文言: ${r.message}`);
  mod._setSuggestFetcher(async () => { throw err(null, 'ECONNREFUSED'); });
  r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'unreachable'], 'code 無しの例外は unreachable 扱い');
}

console.log('[3] 状態の無い応答は受け取らない');
{
  mod._setSuggestFetcher(async () => ({ seed: 'x', total: 1, suggestions: [{ keyword: 'x y' }] }));   // 古い版の miniPC
  let r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'bad_response'], 'prefixes/summary が無ければ bad_response');
  ok(/版が古い/.test(r.message), `理由に「版が古い」: ${r.message}`);
  // 形だけ揃った空・整合しない・別の種 (PR #1408 R1 #4)
  mod._setSuggestFetcher(async () => ({ seed: 'x', total: 0, suggestions: [], prefixes: [], summary: {} }));
  r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'bad_response'], '{prefixes:[], summary:{}} は 0 件ではなく bad_response');
  mod._setSuggestFetcher(async () => ({ seed: 'x', total: 0, suggestions: [], prefixes: [{ prefix: 'x', status: 'empty' }], summary: { requested: 47, success: 0, empty: 1, failed: 0, unrun: 0 } }));
  r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'bad_response'], 'prefix の件数と requested が合わなければ bad_response');
  mod._setSuggestFetcher(async () => ({ seed: 'y', total: 0, suggestions: [], prefixes: [{ prefix: 'y', status: 'empty' }], summary: { requested: 1, success: 0, empty: 1, failed: 0, unrun: 0 } }));
  r = await mod.collectSuggestions('x');
  eq([r.ok, r.code], [false, 'bad_response'], '別の種の応答は受け取らない');
}

console.log('[4] 正常: 種 1 つ・ひらがな固定・アルファベットは指定時だけ');
{
  let sent = null;
  const result = { seed: 'ひば油', total: 2, suggestions: [{ keyword: 'ひば油 スプレー', source: 'base', depth: 0 }, { keyword: 'ひば油 あ', source: 'hiragana:あ', depth: 0 }],
    prefixes: [{ prefix: 'ひば油', status: 'success', count: 1 }, { prefix: 'ひば油 あ', status: 'success', count: 1 }], summary: { requested: 2, success: 2, empty: 0, failed: 0, unrun: 0, requests: 2 }, fetchedAt: '2026-09-23T00:00:00.000Z' };
  mod._setSuggestFetcher(async (body) => { sent = body; return result; });
  let r = await mod.collectSuggestions('ひば油');
  eq([r.ok, r.result.total], [true, 2], '結果をそのまま返す');
  eq(sent, { seed: 'ひば油', hiragana: true, alphabet: false }, '送る中身 (深掘りは送らない)');
  r = await mod.collectSuggestions('ひば油', { alphabet: true });
  eq(sent.alphabet, true, 'alphabet は指定時だけ true');
}

mod._setSuggestFetcher(null);
console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
