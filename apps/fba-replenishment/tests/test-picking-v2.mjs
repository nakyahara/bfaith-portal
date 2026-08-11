/**
 * v2 シールCSV (PDF情報統合) の純関数テスト
 * 実行: node apps/fba-replenishment/tests/test-picking-v2.mjs
 */
import assert from 'node:assert/strict';
import {
  normalizeBlock, normalizeLocation, buildMatchKey, parseStrictNonNegInt,
  formatExpiry, reconcilePdfWithLz, buildLabelRowsV2, isValidDateYmd,
  UNALLOCATED, UNALLOCATED_LOC_DISPLAY, LABEL_V2_HEADER,
} from '../picking-prep.js';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ✓ ${name}`); }
  catch (e) { console.error(`  ✗ ${name}: ${e.message}`); process.exitCode = 1; }
}

console.log('--- normalizeLocation ---');
t('8桁数字 → ハイフン形式', () => assert.equal(normalizeLocation('00101202'), '001-012-02'));
t('ハイフン形式はそのまま', () => assert.equal(normalizeLocation('001-012-02'), '001-012-02'));
t('前後空白を除去', () => assert.equal(normalizeLocation(' 00101202 '), '001-012-02'));
t('全角ハイフンを許容', () => assert.equal(normalizeLocation('001－012－02'), '001-012-02'));
t('ZZZ 8桁 → UNALLOCATED', () => assert.equal(normalizeLocation('ZZZZZZZZ'), UNALLOCATED));
t('ZZZ-ZZZ-ZZ → UNALLOCATED', () => assert.equal(normalizeLocation('ZZZ-ZZZ-ZZ'), UNALLOCATED));
t('小文字zzzも UNALLOCATED', () => assert.equal(normalizeLocation('zzz-zzz-zz'), UNALLOCATED));
t('未知形式は null (数字に潰さない)', () => assert.equal(normalizeLocation('1-2-3'), null));
t('7桁数字は null', () => assert.equal(normalizeLocation('0010120'), null));
t('空は null', () => assert.equal(normalizeLocation(''), null));

console.log('--- buildMatchKey ---');
t('ブロック|ロケ|normCode', () => assert.equal(buildMatchKey('p1fb', '00101202', 'Vitaminc-30'), 'P1FB|001-012-02|vitaminc-30'));
t('ロケ不正は null', () => assert.equal(buildMatchKey('P1FB', 'xx', 'a'), null));

console.log('--- parseStrictNonNegInt ---');
t('"20" → 20', () => assert.equal(parseStrictNonNegInt('20'), 20));
t('"0" → 0', () => assert.equal(parseStrictNonNegInt('0'), 0));
t('先行ゼロ "01" は null', () => assert.equal(parseStrictNonNegInt('01'), null));
t('"1.5" は null', () => assert.equal(parseStrictNonNegInt('1.5'), null));
t('"1個" は null', () => assert.equal(parseStrictNonNegInt('1個'), null));
t('空は null (0扱いしない)', () => assert.equal(parseStrictNonNegInt(''), null));
t('負数は null', () => assert.equal(parseStrictNonNegInt('-1'), null));

console.log('--- formatExpiry ---');
t('空欄は warn:empty で継続', () => assert.deepEqual(formatExpiry(''), { value: '', warn: 'empty' }));
t('29991231 (無期限) は空表示', () => assert.deepEqual(formatExpiry('29991231'), { value: '' }));
t('実在日付は YYYY/MM/DD', () => assert.deepEqual(formatExpiry('20280323'), { value: '2028/03/23' }));
t('形式不正は error', () => assert.ok(formatExpiry('2028-03-23').error));
t('存在しない日付は error', () => assert.ok(formatExpiry('20281332').error));
t('うるう年でない2/29は error', () => assert.ok(formatExpiry('20270229').error));

// ---- reconcilePdfWithLz ----
console.log('--- reconcilePdfWithLz ---');
const lz = (block, location, code, qty, opts = {}) => ({
  block, location, code, name: `${code}名`, planNo: opts.planNo ?? `通常_1`, dodai: opts.dodai ?? '',
  qty: String(qty), expiry: opts.expiry ?? '',
});
const pi = (page, item, block, location, productId, qty, zansu) => ({ page, item, block, location, productId, qty, zansu });

t('正常: 同一キー複数行のFIFO対応 (行順で残数が付く)', () => {
  const lzRows = [
    lz('P1FB', '00101202', 'itemA', 20),
    lz('P1FB', '00101202', 'itemA', 5),   // 同一キー2行目 (数量違い)
    lz('ZZZ', 'ZZZZZZZZ', 'itemB', 1),
  ];
  const pdfItems = [
    pi(0, 0, 'P1FB', '001-012-02', 'itemA', 20, 0),
    pi(0, 1, 'P1FB', '001-012-02', 'itemA', 5, 15),
    pi(0, 2, 'ZZZ', 'ZZZ-ZZZ-ZZ', 'itemB', 1, 0),
  ];
  const r = reconcilePdfWithLz(pdfItems, lzRows);
  assert.deepEqual(r.errors, []);
  assert.equal(r.rows.length, 3);
  assert.equal(r.rows[0].zansu, 0);
  assert.equal(r.rows[1].zansu, 15);
  assert.equal(r.rows[1].qty, 5);
  assert.equal(r.rows[2].zansu, 0);
  assert.equal(r.rows[0].pdfPage, 0);
  assert.equal(r.rows[1].pdfItem, 1);
});

t('行数不一致は fail', () => {
  const r = reconcilePdfWithLz([pi(0, 0, 'P1FB', '001-012-02', 'a', 1, 0)], []);
  assert.equal(r.rows.length, 0);
  assert.match(r.errors[0], /一致しません/);
});

t('キー毎件数不一致は fail (別行同士の誤対応を防ぐ)', () => {
  const r = reconcilePdfWithLz(
    [pi(0, 0, 'P1FB', '001-012-02', 'a', 1, 0), pi(0, 1, 'P1FB', '001-012-03', 'a', 1, 0)],
    [lz('P1FB', '00101202', 'a', 1), lz('P1FB', '00101202', 'a', 1)],
  );
  assert.ok(r.errors.some(e => /突合不一致/.test(e)));
  assert.ok(r.errors.some(e => /PDFのみに存在/.test(e)));
});

t('数量不一致は fail (ファイル取り違え検出)', () => {
  const r = reconcilePdfWithLz(
    [pi(0, 0, 'P1FB', '001-012-02', 'a', 99, 0)],
    [lz('P1FB', '00101202', 'a', 20)],
  );
  assert.ok(r.errors.some(e => /数量不一致/.test(e)));
});

t('lz数量が不正 (空/非整数) は fail (0扱いしない)', () => {
  const r = reconcilePdfWithLz(
    [pi(0, 0, 'P1FB', '001-012-02', 'a', 0, 0)],
    [lz('P1FB', '00101202', 'a', '')],
  );
  assert.ok(r.errors.some(e => /数量\(出荷引当\)が不正/.test(e)));
});

t('lzロケ未知形式は fail', () => {
  const r = reconcilePdfWithLz(
    [pi(0, 0, 'P1FB', '001-012-02', 'a', 1, 0)],
    [lz('P1FB', 'BAD-LOC', 'a', 1)],
  );
  assert.ok(r.errors.some(e => /ロケーション形式を解釈できません/.test(e)));
});

// ---- buildLabelRowsV2 ----
console.log('--- buildLabelRowsV2 ---');
const merged = (over = {}) => ({
  block: 'P1FB', location: '00101202', code: 'itemA', name: 'A名', planNo: '通常_1', dodai: '',
  qty: 20, zansu: 3, expiry: '', pdfPage: 0, pdfItem: 0, ...over,
});

t('10列ヘッダ + 全行出力 (シール枚数 = 行数)', () => {
  const rows = [merged(), merged({ code: 'itemB', planNo: '' })];
  const v2 = buildLabelRowsV2(rows, new Map([['itema', '4901234567890']]));
  assert.deepEqual(v2.csvRows[0], LABEL_V2_HEADER);
  assert.equal(v2.csvRows.length - 1, rows.length);
});

t('プランNo未解決は「プランなし」印字で残す', () => {
  const v2 = buildLabelRowsV2([merged({ planNo: '' })], new Map());
  assert.equal(v2.csvRows[1][1], 'プランなし');
  assert.equal(v2.planlessCount, 1);
  assert.ok(v2.warnings.some(w => /プランNo未解決/.test(w)));
});

t('複数プランは " / " 連結', () => {
  const v2 = buildLabelRowsV2([merged({ planNo: '通常_1\n危険_2' })], new Map());
  assert.equal(v2.csvRows[1][1], '通常_1 / 危険_2');
});

t('ロケは表示形式・数量残数は文字列化・バーコードはnormCode引き', () => {
  const v2 = buildLabelRowsV2([merged()], new Map([['itema', '4901234567890']]));
  const r = v2.csvRows[1];
  assert.deepEqual(r, ['itemA', '通常_1', 'A名', '4901234567890', '', 'P1FB', '001-012-02', '20', '3', '']);
});

t('未引当(ZZZ)行は末尾にまとめ、ロケ表示は ZZZ-ZZZ-ZZ', () => {
  const rows = [
    merged({ block: 'ZZZ', location: 'ZZZZZZZZ', code: 'z1' }),
    merged({ code: 'itemB' }),
  ];
  const v2 = buildLabelRowsV2(rows, new Map());
  assert.equal(v2.unallocatedCount, 1);
  assert.equal(v2.csvRows[1][0], 'itemB');            // 通常行が先
  assert.equal(v2.csvRows[2][0], 'z1');               // 未引当は末尾
  assert.equal(v2.csvRows[2][6], UNALLOCATED_LOC_DISPLAY);
  assert.ok(v2.warnings.some(w => /未引当/.test(w)));
});

t('期限: 実在日付は変換、非空不正は expiryErrors (422用)', () => {
  const v2 = buildLabelRowsV2(
    [merged({ expiry: '20280323' }), merged({ code: 'bad', expiry: '99xx' })],
    new Map(),
  );
  assert.equal(v2.csvRows[1][9], '2028/03/23');
  assert.equal(v2.expiryErrors.length, 1);
  assert.match(v2.expiryErrors[0], /bad/);
});

t('無期限29991231は空欄・空欄期限は警告のみ', () => {
  const v2 = buildLabelRowsV2([merged({ expiry: '29991231' }), merged({ expiry: '' })], new Map());
  assert.equal(v2.csvRows[1][9], '');
  assert.equal(v2.csvRows[2][9], '');
  assert.equal(v2.expiryErrors.length, 0);
  assert.ok(v2.warnings.some(w => /期限が空欄/.test(w)));
});

console.log('--- isValidDateYmd ---');
t('実在日付は true', () => assert.equal(isValidDateYmd('2026-08-11'), true));
t('2026-99-99 は false', () => assert.equal(isValidDateYmd('2026-99-99'), false));
t('2027-02-29 は false', () => assert.equal(isValidDateYmd('2027-02-29'), false));
t('形式不正は false', () => assert.equal(isValidDateYmd('20260811'), false));

console.log(`\n${passed} tests passed${process.exitCode ? ' (with FAILURES)' : ''}`);
