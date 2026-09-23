import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * ASIN の共通部品 (lib/asin.js)
 * 実行: node scripts/test-lib-asin.mjs
 *
 * 🚨 ASIN_RE は 5 ファイルで共有している (.test() を直接呼ぶ)。g / y フラグが付くと lastIndex を持ち、
 *    同じ入力への判定が呼ぶたびに true / false と入れ替わる → [1] で検出する
 */
const { ASIN_RE, normalizeAsin, parseAsinList } = await import('../lib/asin.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

console.log('[1] ASIN_RE は状態を持たない');
{
  eq(ASIN_RE.flags, '', 'フラグ無し (g / y を付けない)');
  eq(ASIN_RE.source, '^[A-Z0-9]{10}$', '形は 英大文字・数字の 10 桁ちょうど');
  eq([1, 2, 3, 4].map(() => ASIN_RE.test('B0ABCDEFGH')), [true, true, true, true], '同じ入力を 4 回続けても毎回 true');
  eq(ASIN_RE.lastIndex, 0, '呼んだあとも lastIndex は 0');
}

console.log('[2] ASIN_RE の境界 (置き換え前の /^[A-Z0-9]{10}$/ と同じ判定)');
{
  const OLD = /^[A-Z0-9]{10}$/;
  const inputs = ['B0ABCDEFGH', '4101010013', 'b0abcdefgh', ' B0ABCDEFGH', 'B0ABCDEFGH\n', 'B0ABCDEFG', 'B0ABCDEFGHI', 'Ｂ0ABCDEFGH', '4901234567894', ''];
  eq(inputs.map((s) => ASIN_RE.test(s)), inputs.map((s) => OLD.test(s)), '置き換え前と全入力で一致');
  eq(inputs.map((s) => ASIN_RE.test(s)), [true, true, false, false, false, false, false, false, false, false], '小文字・空白・改行・9/11 桁・全角・JAN 13 桁は通さない');
}

console.log('[3] normalizeAsin');
{
  eq(normalizeAsin('  b0abcdefgh '), 'B0ABCDEFGH', '前後の空白を落として大文字に');
  eq(normalizeAsin('B0ABCDEFG'), null, '9 桁は null');
  eq(normalizeAsin(null), null, 'null は null');
}

console.log('[4] parseAsinList');
{
  const r = parseAsinList('B0ABCDEFGH, https://www.amazon.co.jp/dp/b0zzzzzzzz?th=1\nbad B0ABCDEFGH');
  eq(r.asins, ['B0ABCDEFGH', 'B0ZZZZZZZZ'], 'URL 混じり・重複を除いて出た順');
  eq(r.invalid, ['bad'], '形式違いは invalid に');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
if (fail) process.exitCode = 1;
