import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * ABA 参照サービス (apps/warehouse/aba-service.js): 取込済みの最新週から ASIN の検索語を引く (走査しない)
 * 実行: node scripts/test-aba-lookup-service.mjs
 *
 * 守りたいこと:
 *   ① 「行が無い」を「該当なし」と言えるのは、証明があるとき (full モードで週が取込済み / watched で走査済み) だけ
 *   ② 週が 1 つも取込済みでなければ no_week (該当なしとは言わない)
 *   ③ 指標は原値 (rank・click_position・share 0〜1)。集計や換算を足さない
 *   ④ 照会した ASIN は監視に登録する。ASIN は 1〜5 件・形式検証
 */
import path from 'node:path';
import http from 'node:http';
import fs from 'node:fs';
if (!process.env.DATA_DIR) { process.env.DATA_DIR = path.join(process.cwd(), '.tmp-aba-lookup-test'); }
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });
try { fs.unlinkSync(path.join(process.env.DATA_DIR, 'aba.db')); } catch { /* 無ければ無視 */ }

const express = (await import('express')).default;
const abadb = await import('../apps/aba-keywords/db.js');
const svc = await import('../apps/warehouse/aba-service.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const db = abadb.initAbaDB();

console.log('[1] 週が 1 つも無ければ no_week');
{
  const r = svc.lookupAsins(db, ['B0AAAAAAA1'], { mode: 'full' });
  eq([r.week, r.items[0].status, r.items[0].terms.length], [null, 'no_week', 0], '週なし → no_week・terms 空');
  ok(db.prepare('SELECT query_count FROM aba_watch_asins WHERE asin = ?').get('B0AAAAAAA1')?.query_count === 1, '照会した ASIN は監視に登録される');
}

// 取込済みの週 (9/13〜9/19) と検索語
db.prepare(`INSERT INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count, parsed_count) VALUES ('2026-09-13', '2026-09-19', '2026-09-24T22:05:00Z', 439692, 1313530, 1313530)`).run();
db.prepare(`INSERT INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count, parsed_count) VALUES ('2026-09-06', '2026-09-12', '2026-09-24T22:00:00Z', 400000, 1200000, 1200000)`).run();
const ins = db.prepare(`INSERT INTO aba_search_terms (week_start, department, search_term, search_frequency_rank, click_position, asin, click_share, conversion_share) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
ins.run('2026-09-13', 'amazon.co.jp', 'ハッカ油 スプレー', 1200, 1, 'B0AAAAAAA1', 0.31, 0.28);
ins.run('2026-09-13', 'amazon.co.jp', 'ハッカ油 スプレー', 1200, 2, 'B0BBBBBBB2', 0.12, 0.10);
ins.run('2026-09-13', 'amazon.co.jp', 'ハッカ油', 300, 3, 'B0AAAAAAA1', 0.05, 0.04);
ins.run('2026-09-06', 'amazon.co.jp', '虫除け スプレー', 900, 1, 'B0CCCCCCC3', 0.2, 0.2);   // 前の週だけ
db.prepare(`INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count, last_scanned_week) VALUES ('B0DDDDDDD4', datetime('now'), datetime('now'), 1, '2026-09-13')`).run();

console.log('[2] full モード: 取込済みの最新週から引く。無ければ「無い」と言える (証明 = 週の取込完了)');
{
  const r = svc.lookupAsins(db, ['B0AAAAAAA1', 'B0BBBBBBB2', 'B0CCCCCCC3', 'B0ZZZZZZZ9'], { mode: 'full' });
  eq([r.week.week_start, r.week.week_end, r.week.parsed_count, r.mode], ['2026-09-13', '2026-09-19', 1313530, 'full'], '対象週 = 取込済みの最新週 (証明の数字つき)');
  const a = r.items.find((i) => i.asin === 'B0AAAAAAA1');
  eq([a.status, a.proof, a.terms.map((t) => t.search_term)], ['found', 'week_ingested', ['ハッカ油', 'ハッカ油 スプレー']], 'found・順位 (search_frequency_rank) の昇順');
  eq(a.terms[1], { search_term: 'ハッカ油 スプレー', department: 'amazon.co.jp', search_frequency_rank: 1200, click_position: 1, click_share: 0.31, conversion_share: 0.28 }, '指標は原値のまま (集計・換算なし)');
  ok(!('top3_click_share' in a.terms[0]) && !('score' in a.terms[0]), '合計や点数を足さない');
  eq(r.items.find((i) => i.asin === 'B0BBBBBBB2').terms.length, 1, '同じ語の別の上位 ASIN は自分の行だけ');
  eq([r.items.find((i) => i.asin === 'B0CCCCCCC3').status, r.items.find((i) => i.asin === 'B0CCCCCCC3').proof], ['none', 'week_ingested'], '前の週にしか無い ASIN は「最新週には無い」(対象週固定)');
  eq(r.items.find((i) => i.asin === 'B0ZZZZZZZ9').status, 'none', '未知の ASIN も full なら「無い」(証明つき)');
}

console.log('[3] watched モード: 走査済みの監視 ASIN だけ「無い」と言える。それ以外は取込対象外');
{
  const r = svc.lookupAsins(db, ['B0AAAAAAA1', 'B0DDDDDDD4', 'B0ZZZZZZZ9'], { mode: 'watched' });
  eq(r.items.find((i) => i.asin === 'B0AAAAAAA1').status, 'found', '行があれば found');
  eq([r.items.find((i) => i.asin === 'B0DDDDDDD4').status, r.items.find((i) => i.asin === 'B0DDDDDDD4').proof], ['none', 'scanned'], '走査済み (last_scanned_week ≥ 週) の監視 ASIN → none (証明 = 走査)');
  eq(r.items.find((i) => i.asin === 'B0ZZZZZZZ9').status, 'not_covered', '未走査の ASIN は「該当なし」ではなく取込対象外');
  ok(db.prepare('SELECT 1 FROM aba_watch_asins WHERE asin = ?').get('B0ZZZZZZZ9'), '取込対象外でも監視に登録される (次の週次取込から拾う)');
  const r2 = svc.lookupAsins(db, ['B0EEEEEEE5'], { mode: 'watched', register: false });
  ok(!db.prepare('SELECT 1 FROM aba_watch_asins WHERE asin = ?').get('B0EEEEEEE5') && r2.items[0].status === 'not_covered', 'register=false なら登録しない');
}

console.log('[4] service-api の口');
{
  const app = express();
  app.use(express.json());
  app.use('/service-api/aba', svc.default);
  const server = http.createServer(app);
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = async (body) => {
    const res = await fetch(base + '/service-api/aba/lookup', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    return { status: res.status, body: await res.json().catch(() => ({})) };
  };
  eq((await call({})).status, 400, 'asins 無しは 400');
  eq((await call({ asins: 'bad, also-bad' })).status, 400, '形式違いだけなら 400');
  eq((await call({ asins: ['B0AAAAAAA1', 'B0AAAAAAA2', 'B0AAAAAAA3', 'B0AAAAAAA4', 'B0AAAAAAA5', 'B0AAAAAAA6'] })).status, 400, '6 件は 400');
  const saved = process.env.ABA_INGEST_MODE;
  process.env.ABA_INGEST_MODE = 'full';
  const r = await call({ asins: 'b0aaaaaaa1, https://www.amazon.co.jp/dp/B0ZZZZZZZ9/ref=x nope' });
  eq(r.status, 200, '正常');
  ok(r.body.ok === true && r.body.result && r.body.result.mode === 'full' && r.body.result.week.week_start === '2026-09-13', 'okResponse の形 {ok, result:{week, mode, items}} (mode は env から)');
  eq(r.body.result.items.map((i) => [i.asin, i.status]), [['B0AAAAAAA1', 'found'], ['B0ZZZZZZZ9', 'none']], '大文字化・URL から抽出。形式違いは invalid に');
  eq(r.body.result.invalid, ['nope'], 'invalid を返す');
  if (saved === undefined) delete process.env.ABA_INGEST_MODE; else process.env.ABA_INGEST_MODE = saved;
  server.close();
}

abadb.closeAbaDB();
console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
