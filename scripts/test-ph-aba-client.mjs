import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * product-hub → miniPC ABA 参照クライアント (apps/product-hub/lib/aba-client.js)
 *
 * 実行: node scripts/test-ph-aba-client.mjs
 *
 * 守りたいこと:
 *   ① トークン未設定なら miniPC を叩かない
 *   ② miniPC が落ちている / aba.db を開けない / 応答の形が古い / 時間切れ を**呼び手が見分けられる**形で返し、throw しない
 *   ③ 証明の無い「該当なし」(none なのに coverage≠complete) や 別の ASIN・未知の状態 は受け取らない — 該当なし ≠ 注文なし
 * 本物の miniPC は呼ばない (fetcher を差し替える)。
 */
const mod = await import('../apps/product-hub/lib/aba-client.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const week = { week_start: '2026-09-13', week_end: '2026-09-19', ingested_at: '2026-09-24T00:10:00Z', mode: 'full', skipped_count: 0, pruned_at: null };
const found = (asin, terms) => ({ week, requested_week: null, week_coverage: 'complete', registered: true, register_errors: [], invalid: [],
  items: [{ asin, status: 'found', proof: 'week_ingested', coverage: 'complete', reason: null, terms }] });
const term = (t, rank, pos = 1) => ({ search_term: t, department: 'amazon.co.jp', search_frequency_rank: rank, click_position: pos, click_share: 0.12, conversion_share: 0.08 });

console.log('[1] トークン未設定なら叩かない');
{
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  let called = 0;
  mod._setAbaFetcher(async () => { called++; return found('B0AAAAAAA1', [term('ハッカ油', 100)]); });
  const r = await mod.lookupAbaTerms('B0AAAAAAA1');
  eq([r.ok, r.code, called], [false, 'not_configured', 0], '叩かずに not_configured');
}

process.env.WAREHOUSE_SERVICE_TOKEN = 'test-token';

console.log('[2] 失敗の種類を見分けて返す (throw しない)');
{
  const err = (code, message, name) => { const e = new Error(message); if (code) e.code = code; if (name) e.name = name; return e; };
  mod._setAbaFetcher(async () => { throw err('unavailable', 'aba.db を開けません'); });
  let r = await mod.lookupAbaTerms('B0AAAAAAA1');
  eq([r.ok, r.code], [false, 'unavailable'], '503 → unavailable');
  mod._setAbaFetcher(async () => { throw err('unreachable', 'HTTP 502'); });
  r = await mod.lookupAbaTerms('B0AAAAAAA1');
  eq([r.ok, r.code], [false, 'unreachable'], '非 2xx → unreachable');
  mod._setAbaFetcher(async () => { throw err('bad_request', 'asins は 1 回 5 件まで'); });
  r = await mod.lookupAbaTerms('B0AAAAAAA1');
  eq([r.ok, r.code], [false, 'bad_request'], '400 → bad_request');
  mod._setAbaFetcher(async () => { throw err(null, 'The operation was aborted due to timeout', 'TimeoutError'); });
  r = await mod.lookupAbaTerms('B0AAAAAAA1');
  eq([r.ok, r.code], [false, 'timeout'], '時間切れ → timeout');
  ok(/秒以内/.test(r.message), `時間切れの文言: ${r.message}`);
  mod._setAbaFetcher(async () => { throw err(null, 'ECONNREFUSED'); });
  r = await mod.lookupAbaTerms('B0AAAAAAA1');
  eq([r.ok, r.code], [false, 'unreachable'], 'code の無い例外 → unreachable');
}

console.log('[3] 形の検査: 壊れた応答・証明の無い「該当なし」は受け取らない');
{
  const v = (result, asin = 'B0AAAAAAA1') => mod.validateAbaResult(result, asin);
  ok(v(found('B0AAAAAAA1', [term('ハッカ油', 100)])) === null, '正常 (found) は通る');
  ok(v({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'none', proof: 'week_ingested', coverage: 'complete', reason: null, terms: [] }] }) === null, '正常 (none・complete) は通る');
  ok(v({ week: null, items: [{ asin: 'B0AAAAAAA1', status: 'no_week', proof: null, coverage: 'unknown', reason: 'no_ingested_week', terms: [] }] }) === null, '正常 (no_week・週なし) は通る');
  ok(v({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'not_covered', proof: null, coverage: 'unknown', reason: 'incomplete_ingest', terms: [] }] }) === null, '正常 (not_covered) は通る');
  ok(/items/.test(v({ ok: true }) || ''), 'items が無い (旧い版) は拒む');
  ok(/件数/.test(v({ ...found('B0AAAAAAA1', []), items: [] }) || ''), '0 件の items は拒む');
  ok(/別の ASIN/.test(v(found('B0BBBBBBB2', [term('x', 1)])) || ''), '別の ASIN の応答は拒む');
  ok(/未知の状態/.test(v({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'scanning', coverage: 'complete', terms: [] }] }) || ''), '未知の状態は拒む');
  ok(/未知の網羅/.test(v({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'found', coverage: 'full', terms: [term('x', 1)] }] }) || ''), '未知の網羅状態は拒む');
  ok(/terms/.test(v({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'found', coverage: 'complete' }] }) || ''), 'terms が無いのは拒む');
  ok(/search_term/.test(v(found('B0AAAAAAA1', [{ search_frequency_rank: 1 }])) || ''), 'search_term の無い要素は拒む');
  ok(/found なのに terms が空/.test(v(found('B0AAAAAAA1', [])) || ''), 'found なのに 0 語は拒む');
  ok(/なのに terms がある/.test(v({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'none', coverage: 'complete', terms: [term('x', 1)] }] }) || ''), 'none なのに語があるのは拒む');
  ok(/証明の無い/.test(v({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'none', coverage: 'partial', terms: [] }] }) || ''), '🚨 none なのに coverage が complete でない = 証明の無い「該当なし」は拒む');
  ok(/対象週/.test(v({ ...found('B0AAAAAAA1', [term('x', 1)]), week: null }) || ''), 'found なのに週が無いのは拒む');
  ok(/対象週/.test(v({ ...found('B0AAAAAAA1', [term('x', 1)]), week: { week_start: '9/13', week_end: '2026-09-19' } }) || ''), '週の形が YYYY-MM-DD でないのは拒む');
  ok(/no_week なのに週/.test(v({ week, items: [{ asin: 'B0AAAAAAA1', status: 'no_week', coverage: 'unknown', terms: [] }] }) || ''), 'no_week なのに週があるのは拒む');
}

console.log('[4] 正常な応答は miniPC に ASIN 1 つ・register の指定どおりで頼む');
{
  let sent = null;
  mod._setAbaFetcher(async (body) => { sent = body; return found('B0AAAAAAA1', [term('ハッカ油 スプレー', 120, 2)]); });
  let r = await mod.lookupAbaTerms('B0AAAAAAA1', { register: true });
  eq([r.ok, sent.asins, sent.register, 'week_start' in sent], [true, ['B0AAAAAAA1'], true, false], 'asins は 1 件・register=true・週の指定なし');
  eq(r.result.items[0].terms[0].search_term, 'ハッカ油 スプレー', '結果はそのまま渡る (集計・換算しない)');
  r = await mod.lookupAbaTerms('B0AAAAAAA1', { weekStart: '2026-09-13' });
  eq([sent.register, sent.week_start], [false, '2026-09-13'], '既定は register=false・週を固定できる');
  mod._setAbaFetcher(async () => ({ ...found('B0AAAAAAA1', []), items: [{ asin: 'B0AAAAAAA1', status: 'none', coverage: 'partial', terms: [] }] }));
  r = await mod.lookupAbaTerms('B0AAAAAAA1');
  eq([r.ok, r.code], [false, 'bad_response'], '壊れた応答は bad_response (0 件として扱わない)');
  mod._setAbaFetcher(null);
}

console.log('[5] 語 → クリック上位 3 (/terms・競合 ASIN の自動取得): 送った語の順・件数と一致する応答だけ受け取る');
{
  const g = (asins) => [{ department: 'amazon.co.jp', search_frequency_rank: 800, asins: asins.map((a, i) => ({ asin: a, click_position: i + 1, product_title: null, click_share: 0.1, conversion_share: 0.05 })) }];
  const it = (term, status, extra = {}) => ({ term, matched_term: status === 'found' ? term : null, variants: [term], status, coverage: status === 'none' || status === 'found' ? 'complete' : 'unknown', reason: null, departments: status === 'found' ? g(['B0AAAAAAA1']) : [], ...extra });
  const res = (items, extra = {}) => ({ week, requested_week: null, week_coverage: 'complete', invalid: [], items, ...extra });
  const T = ['ハッカ油 スプレー', '無い語'];
  eq(mod.validateAbaTermsResult(res([it(T[0], 'found'), it(T[1], 'none')]), T), null, '正常 (found と none)');
  const bad = (r, l) => ok(typeof mod.validateAbaTermsResult(r, T) === 'string', l);
  bad(res([it(T[0], 'found')]), '件数が違う');
  bad(res([it(T[1], 'none'), it(T[0], 'found')]), '語の並びが違う');
  bad(res([it(T[0], 'found', { departments: [] }), it(T[1], 'none')]), 'found なのに部門が空');
  bad(res([it(T[0], 'none', { departments: g(['B0AAAAAAA1']) }), it(T[1], 'none')]), 'none なのに部門がある');
  bad(res([it(T[0], 'found'), it(T[1], 'none', { coverage: 'partial' })]), '証明の無い「該当なし」(語の coverage)');
  bad(res([it(T[0], 'found', { coverage: 'partial' }), it(T[1], 'none')], { week_coverage: 'partial' }), '週が complete でないのに「該当なし」');
  bad(res([it(T[0], 'found', { departments: g(['NOT-ASIN']) }), it(T[1], 'none')]), 'ASIN の形でない値');
  bad(res([it(T[0], 'found', { status: 'maybe' }), it(T[1], 'none')]), '未知の状態');
  bad(res([it(T[0], 'no_week'), it(T[1], 'none')], { week: null }), 'no_week とそれ以外が混ざる');
  eq(mod.validateAbaTermsResult(res([it(T[0], 'no_week'), it(T[1], 'no_week')], { week: null, week_coverage: 'unknown' }), T), null, '全部 no_week・週なしは正常');
  bad(res([it(T[0], 'no_week'), it(T[1], 'no_week')]), 'no_week なのに週がある');
  bad(res([it(T[0], 'found'), it(T[1], 'none')], { week: { week_start: '9/13' } }), '対象週の形が違う');

  let sent = null, sentPath = null;
  mod._setAbaFetcher(async (body, path) => { sent = body; sentPath = path; return res([it(T[0], 'found'), it(T[1], 'none')]); });
  let r = await mod.lookupAbaTopAsins(T);
  eq([r.ok, sentPath, sent.terms, 'week_start' in sent, 'register' in sent], [true, '/terms', T, false, false], '/terms に語をそのまま送る (監視登録の指定は無い)');
  r = await mod.lookupAbaTopAsins(T, { weekStart: '2026-09-13' });
  eq(sent.week_start, '2026-09-13', '週を固定できる');
  mod._setAbaFetcher(async () => res([it(T[0], 'found')]));
  r = await mod.lookupAbaTopAsins(T);
  eq([r.ok, r.code], [false, 'bad_response'], '壊れた応答は bad_response');
  mod._setAbaFetcher(async () => { const e = new Error('x'); e.name = 'TimeoutError'; throw e; });
  r = await mod.lookupAbaTopAsins(T);
  eq([r.ok, r.code], [false, 'timeout'], '時間切れは timeout (throw しない)');
  const saved = process.env.WAREHOUSE_SERVICE_TOKEN;
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  r = await mod.lookupAbaTopAsins(T);
  eq([r.ok, r.code], [false, 'not_configured'], 'トークン未設定なら叩かない');
  if (saved !== undefined) process.env.WAREHOUSE_SERVICE_TOKEN = saved;
  mod._setAbaFetcher(null);
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
