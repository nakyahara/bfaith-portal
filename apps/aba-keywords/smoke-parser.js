/**
 * aba-report-parser のスモークテスト (依存なし・ネットワークなし)
 * 実行: node apps/aba-keywords/smoke-parser.js
 */
import { parseAbaReportStream, normalizeAbaItem } from './aba-report-parser.js';

let passed = 0, failed = 0;
function check(name, cond) {
  if (cond) { passed++; console.log(`  ✅ ${name}`); }
  else { failed++; console.error(`  ❌ ${name}`); }
}

// チャンク分割 (境界バグ検出のため n バイトずつ)
async function* chunked(text, n) {
  const buf = Buffer.from(text, 'utf-8');
  for (let i = 0; i < buf.length; i += n) yield buf.subarray(i, i + n);
}

const FIXTURE = JSON.stringify({
  reportSpecification: {
    reportType: 'GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT',
    reportOptions: { reportPeriod: 'WEEK' },
    dataStartTime: '2026-07-19', dataEndTime: '2026-07-25',
  },
  dataByDepartmentAndSearchTerm: [
    { departmentName: 'Amazon.co.jp', searchTerm: 'オレンジオイル ギター', searchFrequencyRank: 183357, clickedAsin: 'B000RY68PS', clickShareRank: 1, clickShare: 0.1667, conversionShare: 0.1333 },
    { departmentName: 'Amazon.co.jp', searchTerm: 'オレンジオイル ギター', searchFrequencyRank: 183357, clickedAsin: 'B0002E1O3C', clickShareRank: 2, clickShare: 0.25, conversionShare: 0.2667 },
    // 文字列内に JSON 構造っぽい記号・エスケープを含むケース (パーサ殺し)
    { departmentName: 'Amazon.co.jp', searchTerm: 'say "hello" {test} [x], ok\\', searchFrequencyRank: 999999, clickedAsin: 'b072hhz1sm', clickShareRank: 3, clickShare: 0.01, conversionShare: null },
    // ネストされた値を持つ将来形にも耐える
    { departmentName: 'Amazon.co.jp', searchTerm: 'ネスト', searchFrequencyRank: 5, clickedAsin: 'B000000005', clickShareRank: 1, clickShare: 0.5, conversionShare: 0.5, extra: { a: [1, 2, { b: ']}' }] } },
  ],
});

// --- 1. 1バイトずつ流しても全要素を正しく拾う ---
{
  const items = [];
  const { itemCount } = await parseAbaReportStream(chunked(FIXTURE, 1), (it) => items.push(it));
  check('チャンク1B: 4要素', itemCount === 4 && items.length === 4);
  check('チャンク1B: 日本語検索語', items[0].searchTerm === 'オレンジオイル ギター');
  check('チャンク1B: エスケープ文字列', items[2].searchTerm === 'say "hello" {test} [x], ok\\');
  check('チャンク1B: ネスト要素', items[3].extra.a[2].b === ']}');
}

// --- 2. 大きめチャンクでも同一結果 ---
{
  const items = [];
  await parseAbaReportStream(chunked(FIXTURE, 7), (it) => items.push(it));
  const items2 = [];
  await parseAbaReportStream(chunked(FIXTURE, 64 * 1024), (it) => items2.push(it));
  check('チャンクサイズ非依存', JSON.stringify(items) === JSON.stringify(items2) && items.length === 4);
}

// --- 3. マルチバイト文字がチャンク境界で割れるケース ---
{
  // UTF-8の3バイト文字を2バイト目で割る: chunked(2) は必ずどこかで割る
  const items = [];
  await parseAbaReportStream(chunked(FIXTURE, 2), (it) => items.push(it));
  check('マルチバイト境界', items[0].searchTerm === 'オレンジオイル ギター');
}

// --- 4. 空配列 ---
{
  const empty = '{"reportSpecification":{},"dataByDepartmentAndSearchTerm":[]}';
  const { itemCount } = await parseAbaReportStream(chunked(empty, 3), () => {});
  check('空配列: 0要素', itemCount === 0);
}

// --- 5. キーが無い → エラー ---
{
  let threw = false;
  try { await parseAbaReportStream(chunked('{"foo": [1,2,3]}', 5), () => {}); }
  catch { threw = true; }
  check('配列キー欠落でエラー', threw);
}

// --- 6. 途中で切れたレポート → エラー ---
{
  let threw = false;
  const truncated = FIXTURE.slice(0, FIXTURE.length - 40);
  try { await parseAbaReportStream(chunked(truncated, 11), () => {}); }
  catch { threw = true; }
  check('途中切断でエラー', threw);
}

// --- 7. normalizeAbaItem ---
{
  const n = normalizeAbaItem({ departmentName: 'Amazon.co.jp', searchTerm: 'x', searchFrequencyRank: 10, clickedAsin: 'b072hhz1sm', clickShareRank: 2, clickShare: 0.1, conversionShare: 0.2 });
  check('normalize: ASIN大文字化', n.asin === 'B072HHZ1SM');
  check('normalize: position', n.click_position === 2);
  const snake = normalizeAbaItem({ department_name: 'd', search_term: 'y', search_frequency_rank: 3, clicked_asin: 'B000000001' });
  check('normalize: snake_case変種', snake && snake.search_term === 'y' && snake.click_position === null);
  const bad = normalizeAbaItem({ searchTerm: 'z' });
  check('normalize: 必須欠落はnull', bad === null);
}

console.log(`\n結果: ${passed} passed / ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
