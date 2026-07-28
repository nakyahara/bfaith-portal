/**
 * aba-report-parser のスモークテスト (依存なし・ネットワークなし)
 * 実行: node apps/aba-keywords/smoke-parser.js
 */
import { parseAbaReportStream, normalizeAbaItem, streamTermGroups } from './aba-report-parser.js';

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

// --- 4b. 文字列値に囮キーが含まれても誤検知しない (Codex R1 medium) ---
{
  const decoy = JSON.stringify({
    reportSpecification: { note: 'this mentions "dataByDepartmentAndSearchTerm" inside a value', fake: { dataByDepartmentAndSearchTerm: [{ searchTerm: 'ネスト内は無視' }] } },
    decoyValue: 'dataByDepartmentAndSearchTerm',
    dataByDepartmentAndSearchTerm: [{ departmentName: 'Amazon.co.jp', searchTerm: '本物', searchFrequencyRank: 1, clickedAsin: 'B000000001' }],
  });
  const items = [];
  await parseAbaReportStream(chunked(decoy, 5), (it) => items.push(it));
  check('囮キー: 本物の配列だけ解析', items.length === 1 && items[0].searchTerm === '本物');
}

// --- 4c. 最後の要素が完成していても配列が閉じていなければエラー (Codex R1 high) ---
{
  let threw = false;
  const noClose = '{"dataByDepartmentAndSearchTerm":[{"searchTerm":"x","searchFrequencyRank":1,"clickedAsin":"B000000001"}';
  try { await parseAbaReportStream(chunked(noClose, 9), () => {}); }
  catch { threw = true; }
  check('配列未クローズでエラー', threw);
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

// --- 6b. 非オブジェクト要素・要素間ゴミ・末尾ゴミ・外側未クローズ (Codex R2) ---
{
  const cases = [
    ['非object要素 [null]', '{"dataByDepartmentAndSearchTerm":[null]}'],
    ['非object要素 [123]', '{"dataByDepartmentAndSearchTerm":[123]}'],
    ['要素間ゴミ', '{"dataByDepartmentAndSearchTerm":[{"searchTerm":"x","searchFrequencyRank":1,"clickedAsin":"B000000001"} garbage]}'],
    ['trailing comma', '{"dataByDepartmentAndSearchTerm":[{"a":1},]}'],
    ['文書終端後のゴミ', '{"dataByDepartmentAndSearchTerm":[]}x'],
    ['外側未クローズ', '{"dataByDepartmentAndSearchTerm":[]'],
    ['括弧種類の不一致 (Codex R3)', '{"dataByDepartmentAndSearchTerm":[{"a":1}]]'],
    ['tail内の不正リテラル', '{"dataByDepartmentAndSearchTerm":[],"x":garbage}'],
    ['seek内の括弧不一致', '{"reportSpecification":{"a":1],"dataByDepartmentAndSearchTerm":[]}'],
    ['値なしキー (Codex R4)', '{"dataByDepartmentAndSearchTerm":[],"x"}'],
    ['objのtrailing comma (Codex R4)', '{"dataByDepartmentAndSearchTerm":[],}'],
    ['コロン欠落', '{"dataByDepartmentAndSearchTerm" [1]}'],
    ['対象キーの値が配列でない', '{"dataByDepartmentAndSearchTerm":{"a":1}}'],
  ];
  for (const [name, json] of cases) {
    let threw = false;
    try { await parseAbaReportStream(chunked(json, 4), () => {}); }
    catch { threw = true; }
    check(`厳格検証: ${name} でエラー`, threw);
  }
  // 正常系: 配列の後に別プロパティ (数値/真偽/null/ネスト/括弧入り文字列) が続いても外側が正しく閉じればOK
  const after = '{"dataByDepartmentAndSearchTerm":[{"searchTerm":"x","searchFrequencyRank":1,"clickedAsin":"B000000001"}],"summary":{"count":1,"ratio":-1.5e3,"ok":true,"none":null,"note":"a]b}c"}}';
  const items = [];
  await parseAbaReportStream(chunked(after, 4), (it) => items.push(it));
  check('厳格検証: 配列後の後続プロパティは許容', items.length === 1);
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

// --- 8. streamTermGroups (検索語グループ化 + position補完 + skip集計) ---
{
  const doc = JSON.stringify({
    dataByDepartmentAndSearchTerm: [
      { departmentName: 'A', searchTerm: 'kw1', searchFrequencyRank: 10, clickedAsin: 'B000000001' },
      { departmentName: 'A', searchTerm: 'kw1', searchFrequencyRank: 10, clickedAsin: 'B000000002' },
      { departmentName: 'A', searchTerm: 'kw1', searchFrequencyRank: 10, clickedAsin: 'B000000003' },
      { departmentName: 'A', searchTerm: 'kw2', searchFrequencyRank: 20, clickedAsin: 'B000000004', clickShareRank: 1 },
      { departmentName: 'A', searchTerm: 'broken' }, // 必須欠落 → skip (グループは形成されない)
      { departmentName: 'A', searchTerm: 'kw3', searchFrequencyRank: 30, clickedAsin: 'B000000005' },
    ],
  });
  const groups = [];
  const { itemCount, skipped } = await streamTermGroups(chunked(doc, 6), (g) => groups.push(g));
  check('groups: グループ数', groups.length === 3);
  check('groups: itemCount/skipped', itemCount === 6 && skipped === 1);
  check('groups: position補完 1..3', groups[0].map(r => r.click_position).join(',') === '1,2,3');
  check('groups: 明示rankを尊重', groups[1][0].click_position === 1);
  check('groups: skip行を跨いでもグループ継続', groups[2][0].search_term === 'kw3' && groups[2][0].click_position === 1);
}

console.log(`\n結果: ${passed} passed / ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
