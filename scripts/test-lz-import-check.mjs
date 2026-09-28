/**
 * test-lz-import-check.mjs — ロジザードの毎日の商品マスタの取込の「押す前」と「押した後」の決まり (マスタ正本切替 ③c-1b-2b-1a)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b 契約 v3」G・K1・K4・K8・v1 §6):
 *   1 取り込む CSV: 見出しが DAILY と完全一致・5 列・CRLF・CSV として正しい・文字が戻る・制御文字なし・商品ID が空でない・重複なし (文字でも小文字でも)
 *   2 結果の文字: 総件数・処理件数・処理不要件数・エラー件数 (カンマ付き) を読む / 無い・2 つ・読めない = unknown / 件数が合わない・エラー > 0 = partial
 *   3 試験の CSV: 5 列を独立に・文字を「?」に落とさない・読み直して一致・毎日の CSV と同じ形
 *   4 取込の後の確かめ: 取り込んだ商品 (exact の列・対象外の列) / 取り込まなかった商品 (全部の列) / 増えた・消えた / observe は記録だけ (decided: false)
 *   5 バーコード: 行を文字として前後で比べる (増えた・消えた・重複の数・見出し)
 * 使い方: node scripts/test-lz-import-check.mjs
 */
import assert from 'node:assert/strict';

const { default: iconv } = await import('iconv-lite');
const K = await import('../apps/master-decisions/lz-import-check.mjs');
const V = await import('../apps/master-decisions/lz-import-verify.mjs');
const { LZ_SHOHIN, readLzShohinMaster } = await import('../apps/master-decisions/lz-cdb.mjs');
const { DAILY, buildLzCsv } = await import('../apps/master-decisions/lz-csv.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sj = (s) => iconv.encode(s, 'cp932');
const q = (c) => `"${String(c).replace(/"/g, '""')}"`;
const csvOf = (rows, { header = DAILY.header, eol = '\r\n' } = {}) => sj([header.map(q).join(','), ...rows.map((r) => r.map(q).join(','))].join(eol));

console.log('test-lz-import-check');

await ta('[1] 取り込む CSV の確かめ: 形・見出し・5 列・CRLF・文字・制御文字・空の ID・重複 (文字でも小文字でも)', async () => {
  const ok = K.validateImportCsv(csvOf([['A-1', '商品A', '商品A', '100', '0001'], ['B-2', 'B, "2"', 'B', '0', '0002']]));
  assert.deepEqual([ok.ok, ok.rows, ok.table[1]], [true, 2, ['B-2', 'B, "2"', 'B', '0', '0002']]);
  const cases = [
    [Buffer.alloc(0), 'import_csv_empty'],
    [Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), csvOf([['A', 'a', 'a', '1', '0001']])]), 'import_csv_bom'],
    [sj('"形式/型番","商品名","ふりがな","仕入単価","取引先id"\r\n"A,a,a,1,0001'), 'import_csv_broken'],
    [csvOf([['A', 'a', 'a', '1', '0001']], { eol: '\n' }), 'import_csv_newline'],
    [Buffer.concat([csvOf([]), Buffer.from('\r\nA,\x82,a,1,0001')]), 'import_csv_encoding'],
    [csvOf([['A', 'a', 'a', '1', '0001']], { header: ['商品ID', '商品名', '検索名称', '仕入単価', '取引先id'] }), 'import_csv_header'],
    [csvOf([]), 'import_csv_no_rows'],
    [csvOf([['A', 'a', 'a', '1']]), 'import_csv_row_width'],
    [csvOf([['A', 'a\r\nb', 'a', '1', '0001']]), 'import_csv_control'],
    [csvOf([[' ', 'a', 'a', '1', '0001']]), 'import_csv_blank_id'],
    [csvOf([['A', 'a', 'a', '1', '0001'], ['A', 'b', 'b', '1', '0001']]), 'import_csv_duplicate_id'],
    [csvOf([['abc-1', 'a', 'a', '1', '0001'], ['ABC-1', 'b', 'b', '1', '0001']]), 'import_csv_duplicate_id_case'],
  ];
  for (const [buf, reason] of cases) assert.equal(K.validateImportCsv(buf).reason, reason, reason);
  // 最後の改行はあってもよい (毎日の CSV は無い)
  assert.equal(K.validateImportCsv(Buffer.concat([csvOf([['A', 'a', 'a', '1', '0001']]), Buffer.from('\r\n')])).ok, true);
});

await ta('[2] 結果の文字: 4 つの数 (カンマ付き・全角のコロン) / 無い・読めない・2 つ・カンマの形が違う = unknown / エラー > 0・総件数 ≠ CSV の行数・処理 + 処理不要 ≠ 総件数 = partial', async () => {
  const txt = (t, p, n, e) => `インポート結果 総件数 : ${t} 処理件数 : ${p} 処理不要件数 : ${n} エラー件数 : ${e}`;
  let r = K.parseImportResult(`ヘッダ\n${txt('5,008', '5,008', '0', '0')}\nOK`);
  assert.deepEqual([r.found, r.total, r.processed, r.noop, r.errors], [true, 5008, 5008, 0, 0]);
  assert.equal(K.parseImportResult('インポート結果\n総件数：3\n処理件数：2\n処理不要件数：1\nエラー件数：0').total, 3);
  assert.deepEqual(K.judgeImportResult(r, 5008), { to: 'imported_unverified', why: 'counts_match' });
  assert.equal(K.judgeImportResult(K.parseImportResult(txt(3, 2, 1, 0)), 3).to, 'imported_unverified');   // 処理不要も数える
  for (const [t, reason] of [['取込が完了しました', 'result_missing'], ['インポート結果 総件数 : ?', 'result_unreadable'], [txt(1, 1, 0, 0) + ' / ' + txt(1, 1, 0, 0), 'result_ambiguous'], [txt('5,00', 5, 0, 0), 'result_bad_number']]) {
    const p = K.parseImportResult(t);
    assert.deepEqual([p.found, p.reason, K.judgeImportResult(p, 1).to], [false, reason, 'unknown'], reason);
  }
  assert.deepEqual(K.judgeImportResult(K.parseImportResult(txt('1,076', '1,075', 0, 1)), 1076), { to: 'partial', why: 'errors_1' });
  assert.equal(K.judgeImportResult(K.parseImportResult(txt(5007, 5007, 0, 0)), 5008).to, 'partial');
  assert.equal(K.judgeImportResult(K.parseImportResult(txt(5008, 5000, 0, 0)), 5008).to, 'partial');
  assert.throws(() => K.judgeImportResult(r, 0), /csvRows/);
});

await ta('[3] 試験の CSV: 5 列を独立に・文字を落とさない (① ～ も)・戻せない文字 / 制御文字は作らない・読み直して一致・毎日の CSV と同じ形 (K1)', async () => {
  const rows = [['A-1', '商品A', 'しょうひんA', '1200', '0001'], ['B-2', '①番 ～ "quote", comma', 'ふりがなは別', '0', '0002']];
  const out = K.buildLosslessCsv(rows);
  const v = K.validateImportCsv(out.bytes);
  assert.deepEqual([v.ok, v.table], [true, rows]);
  // 毎日の生成器と同じ形 (商品名 = ふりがな・? に落ちない文字だけの行)
  const same = [['A-1', '商品A', '商品A', '1200', '0001']];
  const daily = buildLzCsv([{ code_norm: 'A-1', ne_code: 'A-1', name: '商品A', cost_text: '1200', supplier: '0001' }], 'daily');
  assert.ok(K.buildLosslessCsv(same).bytes.equals(daily.bytes));
  for (const bad of ['é', '𠮷', 'a\tb', 'a\nb']) assert.throws(() => K.buildLosslessCsv([['A', bad, 'a', '1', '0001']]), /戻せない|制御文字/, bad);
  assert.throws(() => K.buildLosslessCsv([['A', 'a', 'a', '1']]), /5 つ/);
  assert.throws(() => K.buildLosslessCsv([['A', 'a', 'a', '1', '0001'], ['a', 'b', 'b', '1', '0001']]), /duplicate_id_case/);
});

// ── 取込の後の確かめ ──
const H = LZ_SHOHIN.header;
const col = (name) => H.indexOf(name);
function lzRow(id, over = {}) {
  const cells = H.map((h) => `${h}-${id}`);
  Object.assign(cells, { [col('商品ID')]: id, [col('削除フラグ')]: '0', [col('登録日時')]: '2030/01/01 00:00', [col('変更日時')]: '2030/01/01 00:00', [col('インポート日時')]: '' });
  for (const [k, v] of Object.entries(over)) cells[col(k)] = v;
  return cells;
}
const lzBuf = (rows) => csvOf(rows, { header: H });
const lz = (rows) => { const r = readLzShohinMaster(lzBuf(rows), { minRows: 1 }); assert.ok(r.ok, r.reason); return r; };
const IMPORTED = { 商品名: '新しい名前', 検索名称: 'しんしい', 仕入単価: '1200', 商品予備項目００３: '0007', 変更日時: '2030/01/16 00:21', インポート日時: '2030/01/16 00:21' };

await ta('[4] 一覧は 43 列を文字のまま返す (今の name / cost / supplier / deleted はそのまま)', async () => {
  const r = lz([lzRow('A-1', { 商品名: 'あ', 仕入単価: '10' })]);
  const a = r.byId.get('A-1');
  assert.deepEqual([a.cells.length, a.name, a.cost, a.deleted, a.cells[col('有効期限区分')]], [43, 'あ', '10', '0', '有効期限区分-A-1']);
});

await ta('[5] 取込の後の確かめ: 取り込んだ商品 (exact の列・対象外の列) / 取り込まなかった商品 (全部の列) / 増えた・消えた / observe は記録だけ (decided: false)', async () => {
  const table = [['A-1', '新しい名前', 'しんしい', '1200', '0007']];
  const pre = lz([lzRow('A-1'), lzRow('B-2'), lzRow('abc-1')]);
  // 正しく入った: 対象の列が CSV のとおり・取り込んだ商品のシステムの列は観察・ほかは同じ
  let r = V.verifyImport({ table, pre, post: lz([lzRow('A-1', IMPORTED), lzRow('B-2'), lzRow('abc-1')]) });
  assert.deepEqual([r.ok, r.decided, r.rules_version, r.counts.imported, r.counts.untouched, r.diffs], [true, false, 'lzv-2b1-observe', 1, 2, []]);
  assert.deepEqual(r.observed.targets.map((o) => [o.csv_col, o.lz.map((x) => `${x.col}:${x.post}`).join('|')]),
    [['ふりがな', `検索名称:しんしい|検索名称2:検索名称2-A-1`], ['仕入単価', '仕入単価:1200']]);
  assert.deepEqual(r.observed.imported_system.map((o) => o.col), ['変更日時', 'インポート日時']);
  // それぞれの差
  const kinds = (post, t = table) => V.verifyImport({ table: t, pre, post: lz(post) }).diffs.map((d) => `${d.kind}:${d.id}:${d.col || ''}`);
  assert.deepEqual(kinds([lzRow('A-1', { ...IMPORTED, 商品名: '違う' }), lzRow('B-2'), lzRow('abc-1')]), ['target_mismatch:A-1:商品名']);
  assert.deepEqual(kinds([lzRow('A-1', { ...IMPORTED, 商品予備項目００３: '0008' }), lzRow('B-2'), lzRow('abc-1')]), ['target_mismatch:A-1:商品予備項目００３']);
  assert.deepEqual(kinds([lzRow('A-1', { ...IMPORTED, 有効期限区分: '管理する', 削除フラグ: '1' }), lzRow('B-2'), lzRow('abc-1')]), ['non_target_changed:A-1:有効期限区分', 'non_target_changed:A-1:削除フラグ']);
  assert.deepEqual(kinds([lzRow('A-1', IMPORTED), lzRow('B-2', { 商品名: '新しい名前' }), lzRow('abc-1', { インポート日時: '2030/01/16 00:21' })]),
    ['untouched_changed:B-2:商品名', 'untouched_system_changed:abc-1:インポート日時']);
  assert.deepEqual(kinds([lzRow('A-1', IMPORTED), lzRow('abc-1'), lzRow('C-3')]), ['vanished:B-2:', 'appeared:C-3:']);
  assert.deepEqual(kinds([lzRow('B-2'), lzRow('abc-1')]), ['missing_after:A-1:']);
  // 大文字小文字だけ違う ID: CSV は ABC-1 (一覧に無い) → ロジザードが abc-1 を書き換えた = 取り込まなかった商品の差 (K8)
  assert.deepEqual(kinds([lzRow('A-1'), lzRow('B-2'), lzRow('abc-1', { 商品名: '新しい名前' })], [['ABC-1', '新しい名前', 'x', '1', '0007']]),
    ['missing_after:ABC-1:', 'untouched_changed:abc-1:商品名']);
});

await ta('[6] 決まりの形: 全部 exact・システムの列も変わらない = decided: true / 取り込んだ商品のシステムの列が変わったら差 / 形の誤りは例外', async () => {
  const exactRules = { version: 'lzv-test-exact', targets: V.RULES_2B1.targets.map((t) => (t.mode === 'observe' ? { ...t, mode: 'exact', lz: [t.lz[0]] } : t)), importedSystem: 'exact_unchanged' };
  const table = [['A-1', '新しい名前', 'しんしい', '1200', '0007']];
  const pre = lz([lzRow('A-1')]);
  let r = V.verifyImport({ table, pre, post: lz([lzRow('A-1', { ...IMPORTED, 変更日時: '2030/01/01 00:00', インポート日時: '' })]), rules: exactRules });
  assert.deepEqual([r.ok, r.decided, r.rules_version], [true, true, 'lzv-test-exact']);
  r = V.verifyImport({ table, pre, post: lz([lzRow('A-1', { ...IMPORTED, 仕入単価: '1200.00' })]), rules: exactRules });
  assert.deepEqual(r.diffs.map((d) => `${d.kind}:${d.col}`), ['target_mismatch:仕入単価', 'system_changed:変更日時', 'system_changed:インポート日時']);
  assert.throws(() => V.compileRules({ ...exactRules, targets: exactRules.targets.slice(1) }), /5 つ/);
  assert.throws(() => V.compileRules({ ...exactRules, targets: exactRules.targets.map((t, i) => (i === 1 ? { ...t, lz: ['無い列'] } : t)) }), /列が無い/);
  assert.throws(() => V.compileRules({ ...exactRules, targets: exactRules.targets.map((t, i) => (i === 2 ? { ...t, lz: ['検索名称', '検索名称2'] } : t)) }), /exact は 1 列/);
  assert.throws(() => V.verifyImport({ table, pre: { ok: false }, post: pre }), /ok/);
});

await ta('[7] バーコード: 見出しに 商品ID・バーコード・列の数・HTML = 読まない / 前後の比べ = 増えた・消えた・重複の数・見出しの変化 (K4)', async () => {
  const BH = ['商品ID', '商品名', 'バーコード', '入数'];
  const bc = (rows, header = BH) => V.readBarcodeExport(csvOf(rows, { header }));
  const pre = bc([['A-1', 'a', '4900000000001', '1'], ['A-1', 'a', '4900000000002', '1'], ['B-2', 'b', '4900000000003', '1']]);
  assert.deepEqual([pre.ok, pre.rows, pre.byId.get('A-1').length], [true, 3, 2]);
  assert.equal(V.readBarcodeExport(Buffer.from('<html>login</html>')).reason, 'barcode_html');
  assert.equal(bc([['A-1', 'a', '1']], ['商品ID', '商品名', 'JAN']).reason, 'barcode_header');
  assert.equal(V.readBarcodeExport(sj('"商品ID","バーコード"\r\n"A-1"')).reason, 'barcode_row_width');
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: ['A-1', 'B-2', 'Z-9'] }), { ok: true, diffs: [] });
  const post = bc([['A-1', 'a', '4900000000001', '1'], ['A-1', 'a', '4900000000001', '1'], ['B-2', 'b', '4900000000009', '1']]);
  const d = V.compareBarcodes({ pre, post, ids: ['A-1', 'B-2'] }).diffs.map((x) => `${x.kind}:${x.id}:${JSON.parse(x.row)[2]}`);
  assert.deepEqual(d, ['removed:A-1:4900000000002', 'added:A-1:4900000000001', 'removed:B-2:4900000000003', 'added:B-2:4900000000009']);
  assert.deepEqual(V.compareBarcodes({ pre, post: bc([['A-1', 'a', '4900000000001', '1']], ['商品ID', '名前', 'バーコード', '入数']), ids: [] }).diffs, [{ id: null, kind: 'header_changed' }]);
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
