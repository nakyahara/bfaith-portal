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
  // 表示をまたいで数を組み合わせない・「1.5」を 1 と読まない・1 つの表示に見出しが 2 回 = unknown (Codex #1519 R1 High)
  for (const [t, reason] of [
    ['インポート結果 (読めない) / ' + txt(2, 2, 0, 0), 'result_ambiguous'],
    ['インポート結果 総件数 : 1 処理件数 : 1 / インポート結果 処理不要件数 : 0 エラー件数 : 0', 'result_ambiguous'],
    [txt('1.5', 1, 0, 0), 'result_unreadable'],
    [txt(1, '1．0', 0, 0), 'result_unreadable'],
    [txt(1, 1, 0, 0) + ' エラー件数 : 3', 'result_ambiguous'],
    [txt('5,008,', '5,008', 0, 0), 'result_bad_number'],
  ]) {
    const p = K.parseImportResult(t);
    assert.deepEqual([p.found, p.reason, K.judgeImportResult(p, 1).to], [false, reason, 'unknown'], t);
  }
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
  const d = V.compareBarcodes({ pre, post, ids: ['A-1', 'B-2'] }).diffs.map((x) => `${x.kind}:${x.id}:${x.barcode}`);
  assert.deepEqual(d, ['removed:A-1:4900000000002', 'added:A-1:4900000000001', 'removed:B-2:4900000000003', 'added:B-2:4900000000009']);
  // 商品名などほかの列だけが変わった = 差にしない (名前を変える正常の試験を落とさない。Codex #1519 R1 Medium)
  const renamed = bc([['A-1', '新しい名前', '4900000000002', '9'], ['A-1', '新しい名前', '4900000000001', '9'], ['B-2', 'b2', '4900000000003', '1']]);
  assert.deepEqual(V.compareBarcodes({ pre, post: renamed, ids: ['A-1', 'B-2'] }), { ok: true, diffs: [] });
  // 見出しの 商品ID・バーコード の位置が変わった = 差 / 見出しの名前だけ違う列 = 差にしない / 同じ見出しが 2 つ = 読まない
  assert.deepEqual(V.compareBarcodes({ pre, post: bc([['a', 'A-1', '4900000000001', '1'], ['a', 'A-1', '4900000000002', '1'], ['b', 'B-2', '4900000000003', '1']], ['商品名', '商品ID', 'バーコード', '入数']), ids: [] }).diffs, [{ id: null, kind: 'header_changed' }]);
  assert.deepEqual(V.compareBarcodes({ pre, post: bc([['A-1', 'a', '4900000000001', '1'], ['A-1', 'a', '4900000000002', '1'], ['B-2', 'b', '4900000000003', '1']], ['商品ID', '名前', 'バーコード', '数']), ids: [] }).diffs, []);
  // 途中で切れた (Codex #1530 R1 High): 末尾が改行で終わる = 読まない / 後の行が前より少ない = 差 (前後とも対象より手前で切れて「同じ」に見えるのを防ぐ)
  assert.equal(V.readBarcodeExport(Buffer.concat([csvOf([['A-1', 'a', '4900000000001', '1']], { header: BH }), Buffer.from('\r\n')])).reason, 'barcode_truncated');
  assert.deepEqual(V.compareBarcodes({ pre, post: bc([['A-1', 'a', '4900000000001', '1'], ['A-1', 'a', '4900000000002', '1']]), ids: [] }).diffs, [{ id: null, kind: 'rows_decreased', pre: 3, post: 2 }]);
  assert.deepEqual(V.compareBarcodes({ pre, post: bc([['A-1', 'a', '4900000000001', '1'], ['A-1', 'a', '4900000000002', '1'], ['B-2', 'b', '4900000000003', '1'], ['C-3', 'c', '4900000000004', '1']]), ids: [] }).diffs, []);   // 増えた (ほかの人の新商品) は差にしない
  // 同じ回の商品マスタの全商品がバーコードにある (行の切れ目でちょうど切れて、行の数も前後で同じに見えても分かる。Codex #1530 R2 High)
  const L2 = lz([lzRow('A-1'), lzRow('B-2')]), L3 = lz([lzRow('A-1'), lzRow('B-2'), lzRow('C-3')]);
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: [], cover: { pre: L2, post: L2 } }).diffs, []);
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: [], cover: { pre: L3, post: L3 } }).diffs,
    [{ id: null, kind: 'missing_in_pre_barcode', count: 1, head: ['C-3'] }, { id: null, kind: 'missing_in_post_barcode', count: 1, head: ['C-3'] }]);
  assert.deepEqual(V.barcodeMissing(L3, pre), ['C-3']);
  // 比べる商品が最後の商品 (2 本目以降で行の切れ目ちょうどに切れても分からない) = 確かめられない / 商品ごとの行がひとまとまりでない = 確かめられない (Codex #1530 R3 High)
  assert.deepEqual([pre.grouped, pre.lastId], [true, 'B-2']);
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: ['A-1'], cover: { pre: L2, post: L2 } }).diffs, []);
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: ['B-2'], cover: { pre: L2, post: L2 } }).diffs.map((d) => `${d.kind}:${d.id}`), ['target_is_last_pre:B-2', 'target_is_last_post:B-2']);
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: ['B-2'] }).diffs, []);   // cover が無い比べ (単体の差) は今までどおり
  // 毎晩 (lastTarget = 'compare'・ほぼ全商品を比べる = 最後の商品が毎晩入る): 最後の商品を差にしない (exempt_last に残す)・その商品の増えた / 消えたは差のまま・ひとまとまりでない も差のまま
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: ['A-1', 'B-2'], cover: { pre: L2, post: L2 }, lastTarget: 'compare' }),
    { ok: true, diffs: [], exempt_last: [{ id: 'B-2', side: 'pre' }, { id: 'B-2', side: 'post' }] });
  const lastChanged = bc([['A-1', 'a', '4900000000001', '1'], ['A-1', 'a', '4900000000002', '1'], ['B-2', 'b', '4900000000008', '1']]);
  assert.deepEqual(V.compareBarcodes({ pre, post: lastChanged, ids: ['B-2'], cover: { pre: L2, post: L2 }, lastTarget: 'compare' }).diffs.map((d) => `${d.kind}:${d.id}`), ['removed:B-2', 'added:B-2']);
  const lastMore = bc([['A-1', 'a', '4900000000001', '1'], ['A-1', 'a', '4900000000002', '1'], ['B-2', 'b', '4900000000003', '1'], ['B-2', 'b', '4900000000007', '1']]);
  assert.deepEqual(V.compareBarcodes({ pre, post: lastMore, ids: ['B-2'], cover: { pre: L2, post: L2 }, lastTarget: 'compare' }).diffs.map((d) => `${d.kind}:${d.id}`), ['added:B-2'], '前か後の片方だけが最後の商品の行の切れ目で切れた = 差 (verify_failed)');
  const split0 = bc([['A-1', 'a', '4900000000001', '1'], ['B-2', 'b', '4900000000003', '1'], ['A-1', 'a', '4900000000002', '1'], ['C-3', 'c', '4900000000004', '1']]);
  assert.deepEqual(V.compareBarcodes({ pre: split0, post: split0, ids: ['C-3'], cover: { pre: L2, post: L2 }, lastTarget: 'compare' }).diffs.map((d) => d.kind), ['barcode_not_grouped_pre', 'barcode_not_grouped_post']);
  // 知らない値 = 今までどおり確かめられない (fail-closed)
  assert.deepEqual(V.compareBarcodes({ pre, post: pre, ids: ['B-2'], cover: { pre: L2, post: L2 }, lastTarget: 'yes' }).diffs.map((d) => d.kind), ['target_is_last_pre', 'target_is_last_post']);
  const split = bc([['A-1', 'a', '4900000000001', '1'], ['B-2', 'b', '4900000000003', '1'], ['A-1', 'a', '4900000000002', '1'], ['C-3', 'c', '4900000000004', '1']]);
  assert.deepEqual([split.grouped, split.lastId], [false, 'C-3']);
  assert.deepEqual(V.compareBarcodes({ pre: split, post: split, ids: ['A-1'], cover: { pre: L2, post: L2 } }).diffs.map((d) => d.kind), ['barcode_not_grouped_pre', 'barcode_not_grouped_post']);
  assert.equal(bc([['A-1', '1', '2']], ['商品ID', 'バーコード', 'バーコード']).reason, 'barcode_header');
});

await ta('[8] 一覧の文字の壊れ = 証跡の破損 (比べない)・違うバイトが同じ文字に読めても差・一覧の読み込みは壊れを数えるだけ (lz-daily は捨てない) (Codex #1519 R1 High)', async () => {
  const table = [['A-1', '新しい名前', 'しんしい', '1200', '0007']];
  // 生のバイトで一覧を作る (英語名の列だけ差し替える)
  const rawLz = (enBytes) => {
    const rows = [lzRow('A-1', IMPORTED), lzRow('B-2')];
    const en = col('英語名');
    const lines = [H.map(q).join(',')].map((l) => sj(l));
    for (const [k, r] of rows.entries()) {
      const parts = r.map((c, j) => (k === 1 && j === en ? null : sj(q(c))));
      const withEn = parts.map((p) => p || Buffer.concat([Buffer.from('"'), enBytes, Buffer.from('"')]));
      lines.push(Buffer.concat(withEn.flatMap((p, j) => (j ? [Buffer.from(','), p] : [p]))));
    }
    return Buffer.concat(lines.flatMap((l, i) => (i ? [Buffer.from('\r\n'), l] : [l])));
  };
  const read = (b) => { const r = readLzShohinMaster(b, { minRows: 1 }); assert.ok(r.ok, r.reason); return r; };
  const preOk = read(rawLz(Buffer.from('ok')));
  const pre82 = read(rawLz(Buffer.from([0x82]))), post83 = read(rawLz(Buffer.from([0x83])));
  assert.deepEqual([pre82.encoding, preOk.encoding], [{ fffd: 1, not_round_trip: 1 }, { fffd: 0, not_round_trip: 0 }]);   // 数えるだけ (ok のまま)
  const pre = { ...read(rawLz(Buffer.from('ok'))) };
  pre.byId = new Map([...pre.byId].map(([id, v]) => [id, id === 'A-1' ? { ...v, cells: lzRow('A-1'), raw: lzRow('A-1').map((c) => sj(c)) } : v]));
  let r = V.verifyImport({ table, pre: pre82, post: post83 });
  assert.deepEqual([r.ok, r.diffs.map((d) => `${d.kind}:${d.side}`)], [false, ['evidence_broken:pre', 'evidence_broken:post']]);
  r = V.verifyImport({ table, pre, post: post83 });
  assert.deepEqual(r.diffs.map((d) => `${d.kind}:${d.side}`), ['evidence_broken:post']);
  // 0xED40 と 0xFA5C = どちらも「纊」に読める (NEC 選定 IBM 拡張と IBM 拡張)。文字は同じでもバイトが違う = 差
  const preEd = read(rawLz(Buffer.from([0xed, 0x40]))), postFa = read(rawLz(Buffer.from([0xfa, 0x5c])));
  assert.equal(preEd.byId.get('B-2').cells[col('英語名')], postFa.byId.get('B-2').cells[col('英語名')]);
  preEd.byId.set('A-1', pre.byId.get('A-1'));
  r = V.verifyImport({ table, pre: preEd, post: postFa });
  assert.deepEqual(r.diffs.map((d) => `${d.kind}:${d.id}:${d.col}`), ['untouched_changed:B-2:英語名']);
});

// ── 試験の計画 (K1・K2・K3・K8) ──
const TP = await import('../apps/master-decisions/lz-import-test-plan.mjs');
const crypto = await import('node:crypto');
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');

await ta('[9] 試験の計画: 承認 = 計画の全部の sha256・失敗の試験は対応が決まってから・無い ID / 削除 / 大文字小文字の作り方・押す直前の照らし直し・取り込む CSV = 承認した CSV (Codex #1519 R1 Medium・K1・K2・K8)', async () => {
  const source = { run_id: 'lzd_x', as_of: '2030-01-15', csv_sha256: 'e'.repeat(64),
    table: [['A-1', '新しい名前', '新しい名前', '1200', '0007'], ['B-2', 'B', 'B', '0', '0002'], ['C-3', 'C', 'C', '5', '0003']] };
  const lzRows = () => [lzRow('A-1', { 仕入単価: '1100.00' }), lzRow('B-2'), lzRow('D-4', { 削除フラグ: '1', 仕入単価: '300.00' }),
    lzRow('abc-1', { 商品名: 'x', 検索名称: 'x', 仕入単価: '1.00', 商品予備項目００３: '0001' }), lzRow('ABC-1', { 商品名: 'x', 検索名称: 'x', 仕入単価: '1.00', 商品予備項目００３: '0001' })];
  const pre = lz(lzRows());
  const MAP = { version: 'map-test', furiganaCol: '検索名称', costRule: 'strip_dot00' };
  // 正常の試験だけ = 対応が無くても作れる (戻しの CSV は無い・退避した値はある)
  const n = TP.buildTestPlan({ source, pre, tests: { normal: ['A-1'] } });
  assert.deepEqual([n.plan.rows.map((r) => r.cells), n.plan.restore_csvs, n.plan.retained.map((r) => r.id), n.plan.test_csv.sha256], [[source.table[0]], [], ['A-1'], sha(n.testCsv)]);
  assert.equal(TP.buildTestPlan({ source, pre, tests: { normal: ['A-1'] } }).planSha256, n.planSha256);   // 同じ入力 = 同じ印
  assert.notEqual(TP.buildTestPlan({ source: { ...source, run_id: 'lzd_y' }, pre, tests: { normal: ['A-1'] } }).planSha256, n.planSha256);
  for (const [tests, re] of [[{ normal: ['C-3'] }, /一覧に無い/], [{ normal: ['Z-9'] }, /CSV に無い/], [{ missing: [{ id: 'N-1', copy_from: 'A-1' }] }, /決まってから/], [{ weird: ['A-1'] }, /知らない/], [{}, /行が無い/]]) {
    assert.throws(() => TP.buildTestPlan({ source, pre, tests }), re, JSON.stringify(tests));
  }
  // 失敗の試験 (対応が決まった後)
  const f = TP.buildTestPlan({ source, pre, mapping: MAP, tests: { missing: [{ id: 'N-1', copy_from: 'A-1' }], deleted: ['D-4'], case: [{ id: 'Abc-1', from: 'abc-1' }] } });
  assert.deepEqual(f.plan.rows.map((r) => [r.kind, r.cells]), [
    ['missing', ['N-1', '新しい名前', '新しい名前', '1200', '0007']],
    ['deleted', ['D-4', '商品名-D-4', '検索名称-D-4', '300', '商品予備項目００３-D-4']],
    ['case', ['Abc-1', 'x', 'x', '1', '0001']],
  ]);
  // 戻しの資料: 大文字小文字の候補は別の CSV (K8)
  assert.deepEqual([f.plan.groups, f.plan.restore_csvs.map((r) => r.ids), f.plan.restore_csvs.map((r) => r.sha256)],
    [[{ key: 'a-1', ids: ['A-1'] }, { key: 'abc-1', ids: ['ABC-1', 'abc-1'] }, { key: 'd-4', ids: ['D-4'] }, { key: 'n-1', ids: [] }], [['A-1', 'ABC-1', 'D-4'], ['abc-1']], f.restoreCsvs.map(sha)]);
  assert.deepEqual(K.validateImportCsv(f.restoreCsvs[0]).table.map((r) => r[3]), ['1100', '1', '300']);   // 今の値 (仕入単価は決まりの書き方で)
  assert.match(f.plan.rows[0].if_registered, /削除/);
  for (const [tests, re] of [
    [{ missing: [{ id: 'a-1', copy_from: 'A-1' }] }, /ある/],   // 小文字にすると一覧にある
    [{ deleted: ['A-1'] }, /削除の商品ではない/],
    [{ case: [{ id: 'abc-1', from: 'abc-1' }] }, /同じ文字/],
  ]) assert.throws(() => TP.buildTestPlan({ source, pre, mapping: MAP, tests }), re);
  // 候補の 商品ID を除く 4 列が違う = しない (K8) / 仕入単価の書き方が決まりと違う = 作らない
  const pre2 = lz([...lzRows().slice(0, 3), lzRow('abc-1', { 商品名: 'x', 検索名称: 'x', 仕入単価: '1.00', 商品予備項目００３: '0001' }), lzRow('ABC-1', { 商品名: 'y', 検索名称: 'x', 仕入単価: '1.00', 商品予備項目００３: '0001' })]);
  assert.throws(() => TP.buildTestPlan({ source, pre: pre2, mapping: MAP, tests: { case: [{ id: 'Abc-1', from: 'abc-1' }] } }), /4 列が違う/);
  assert.throws(() => TP.lzToCsvCells(lzRow('X', { 仕入単価: '12.5' }), MAP), /書き方/);
  assert.throws(() => TP.checkMapping({ version: 'v', furiganaCol: '商品名', costRule: 'same' }), /furiganaCol/);
  // 押す直前の照らし直し (K2)
  assert.deepEqual(TP.checkPlanAgainstPre(f.plan, pre), { ok: true, diffs: [] });
  const changed = lz([lzRow('A-1', { 仕入単価: '999' }), lzRow('B-2'), lzRow('D-4', { 削除フラグ: '1', 仕入単価: '300.00' }), lzRow('abc-1', { 商品名: 'x', 検索名称: 'x', 仕入単価: '1.00', 商品予備項目００３: '0001' }),
    lzRow('ABC-1', { 商品名: 'x', 検索名称: 'x', 仕入単価: '1.00', 商品予備項目００３: '0001' }), lzRow('aBc-1'), lzRow('n-1')]);
  assert.deepEqual(TP.checkPlanAgainstPre(f.plan, changed).diffs.map((d) => `${d.kind}:${d.id}:${d.col || ''}`),
    ['retained_changed:A-1:仕入単価', 'case_group_changed:abc-1:', 'case_group_changed:n-1:', 'missing_now_exists:N-1:']);
  assert.deepEqual(TP.checkPlanAgainstPre(f.plan, lz([lzRow('A-1', { 仕入単価: '1100.00' }), lzRow('B-2')])).diffs.map((d) => `${d.kind}:${d.id}`),
    ['retained_vanished:D-4', 'retained_vanished:abc-1', 'retained_vanished:ABC-1', 'case_group_changed:abc-1', 'case_group_changed:d-4']);
  // 取り込む CSV = 承認した CSV
  assert.deepEqual(TP.checkTestCsv(f.plan, f.testCsv), { ok: true, reason: null, rows: 3 });
  assert.equal(TP.checkTestCsv(f.plan, Buffer.concat([f.testCsv, Buffer.from('\r\n')])).reason, 'test_csv_sha256_mismatch');
  assert.equal(TP.checkTestCsv({ ...f.plan, test_csv: { sha256: sha(n.testCsv) } }, n.testCsv).reason, 'test_csv_rows_mismatch');
  // 正常の試験でも、承認の後に大文字小文字だけ違う商品が増えた = 止める / 作るときに組が 2 つ以上 = 作らない (Codex #1519 R2)
  assert.deepEqual(TP.checkPlanAgainstPre(n.plan, lz([...lzRows(), lzRow('a-1')])).diffs.map((d) => `${d.kind}:${d.id}`), ['case_group_changed:a-1']);
  assert.throws(() => TP.buildTestPlan({ source, pre: lz([...lzRows(), lzRow('a-1')]), tests: { normal: ['A-1'] } }), /大文字小文字/);
});

await ta('[10] 試験の計画: 退避はバイトも (違うバイトが同じ文字に読めても・文字の壊れ = 作らない / evidence_broken)・商品ID が __proto__ でも抜けない (配列で持つ・JSON にしても) (Codex #1519 R2)', async () => {
  const en = col('英語名');
  // 一覧をバイトで作る (A-1 の英語名だけ差し替える)
  const rawList = (enBytes, rows) => {
    const lines = [sj(H.map(q).join(','))];
    for (const r of rows) lines.push(Buffer.concat(r.flatMap((c, j) => { const b = r[col('商品ID')] === 'A-1' && j === en ? Buffer.concat([Buffer.from('"'), enBytes, Buffer.from('"')]) : sj(q(c)); return j ? [Buffer.from(','), b] : [b]; })));
    const r = readLzShohinMaster(Buffer.concat(lines.flatMap((l, i) => (i ? [Buffer.from('\r\n'), l] : [l]))), { minRows: 1 });
    assert.ok(r.ok, r.reason); return r;
  };
  const source = { run_id: 'lzd_x', as_of: '2030-01-15', csv_sha256: 'e'.repeat(64), table: [['A-1', 'n', 'n', '1', '0001'], ['__proto__', 'p', 'p', '2', '0002']] };
  const rows = [lzRow('A-1'), lzRow('__proto__')];
  const ed = rawList(Buffer.from([0xed, 0x40]), rows), fa = rawList(Buffer.from([0xfa, 0x5c]), rows);
  const p = TP.buildTestPlan({ source, pre: ed, tests: { normal: ['A-1', '__proto__'] } });
  assert.deepEqual(p.plan.retained.map((r) => r.id), ['A-1', '__proto__']);
  assert.deepEqual(TP.checkPlanAgainstPre(p.plan, ed), { ok: true, diffs: [] });
  assert.deepEqual(TP.checkPlanAgainstPre(p.plan, fa).diffs.map((d) => `${d.kind}:${d.id}:${d.col}`), ['retained_changed:A-1:英語名']);   // 同じ「纊」でもバイトが違う
  // 文字の壊れ: 計画を作らない / 照らし直しは evidence_broken
  const broken = rawList(Buffer.from([0x82]), rows);
  assert.throws(() => TP.buildTestPlan({ source, pre: broken, tests: { normal: ['A-1'] } }), /文字の壊れ/);
  assert.deepEqual(TP.checkPlanAgainstPre(p.plan, broken).diffs, [{ id: null, kind: 'evidence_broken' }]);
  // __proto__: JSON に書いて読み直しても退避した値が残る・値が違えば印も違う・照らし直しで見つかる
  const saved = JSON.parse(JSON.stringify(p.plan));
  assert.equal(TP.planSha256(saved), p.planSha256);
  assert.equal(saved.retained[1].id, '__proto__');
  const renamed = rawList(Buffer.from([0xed, 0x40]), [lzRow('A-1'), lzRow('__proto__', { 商品名: '変えた' })]);
  assert.deepEqual(TP.checkPlanAgainstPre(saved, renamed).diffs.map((d) => `${d.kind}:${d.id}:${d.col}`), ['retained_changed:__proto__:商品名']);
  assert.notEqual(TP.buildTestPlan({ source, pre: renamed, tests: { normal: ['A-1', '__proto__'] } }).planSha256, p.planSha256);
});

await ta('[5b] 2b-2b の決まり RULES_2B2 (9/30 の実機): ふりがな → 検索名称・仕入単価は文字のまま・取り込んだ商品は import_stamp (登録日時は同じ・変更日時とインポート日時は同じ 14 桁で前より後) / decided: true / 毎晩の決まり = RULES_2B2', async () => {
  assert.equal(V.RULES_NIGHTLY, V.RULES_2B2);
  const C = V.compileRules(V.RULES_2B2);
  assert.deepEqual([C.decided, C.version, C.targets.map((t) => t.mode).filter((m) => m === 'observe').length], [true, 'lzv-2b2', 0]);
  assert.ok(Object.isFrozen(V.RULES_2B2) && Object.isFrozen(V.RULES_2B2.targets));
  // 中の配列まで全部凍結 = 実行中に照らす列を書き換えられない (Codex #1556 R2 Medium)
  const unfrozen = (x, at = '') => (x && typeof x === 'object' ? (Object.isFrozen(x) ? [] : [at || '(root)']).concat(...Object.entries(x).map(([k, v]) => unfrozen(v, `${at}.${k}`))) : []);
  for (const [name, r] of [['RULES_2B1', V.RULES_2B1], ['RULES_2B2', V.RULES_2B2], ['RULES_NIGHTLY', V.RULES_NIGHTLY]]) assert.deepEqual(unfrozen(r), [], name);
  assert.throws(() => { V.RULES_NIGHTLY.targets[2].lz[0] = '検索名称2'; }, TypeError);
  assert.throws(() => { V.RULES_NIGHTLY.targets[2].lz.push('検索名称2'); }, TypeError);
  assert.deepEqual([V.RULES_NIGHTLY.targets[2].lz, V.compileRules(V.RULES_NIGHTLY).decided], [['検索名称'], true]);
  const R = V.RULES_2B2;
  const table = [['A-1', '新しい名前', 'しんしい', '5200', '0007']];
  const PRE = { 登録日時: '20260101000000', 変更日時: '20260929171153', インポート日時: '20260929171153' };
  const OK = { 商品名: '新しい名前', 検索名称: 'しんしい', 仕入単価: '5200', 商品予備項目００３: '0007', 登録日時: '20260101000000', 変更日時: '20260930143813', インポート日時: '20260930143813' };
  const pre = lz([lzRow('A-1', PRE), lzRow('B-2', PRE)]);
  const W = { from: '20260930140000', to: '20260930150000' };   // 取込の時刻の窓 (押した時刻 − 余白 〜 結果の時刻 + 余白)
  const run = (a1, b2 = PRE, w = W) => V.verifyImport({ table, pre, post: lz([lzRow('A-1', a1), lzRow('B-2', b2)]), rules: R, importWindow: w });
  let r = run(OK);
  assert.deepEqual([r.ok, r.decided, r.rules_version, r.diffs, r.observed.targets, r.observed.imported_system], [true, true, 'lzv-2b2', [], [], []]);
  const kinds = (a1, b2, w) => run(a1, b2, w).diffs.map((d) => `${d.kind}:${d.col || d.why || ''}`);
  assert.deepEqual(kinds({ ...OK, 検索名称: 'ちがう' }), ['target_mismatch:検索名称'], 'ふりがな = 検索名称');
  assert.deepEqual(kinds({ ...OK, 検索名称2: 'しんしい' }), ['non_target_changed:検索名称2'], '検索名称2 は変わらない');
  assert.deepEqual(kinds({ ...OK, 仕入単価: '5200.00' }), ['target_mismatch:仕入単価'], '仕入単価は文字のまま');
  assert.deepEqual(kinds({ ...OK, 登録日時: '20260930143813' }), ['system_changed:登録日時'], '登録日時は変わらない');
  assert.deepEqual(kinds({ ...OK, 変更日時: PRE.変更日時, インポート日時: PRE.インポート日時 }), ['import_stamp_bad:not_after_pre'], '時刻が進んでいない = 取り込まれていない');
  assert.deepEqual(kinds({ ...OK, インポート日時: '20260930143814' }), ['import_stamp_bad:not_equal']);
  assert.deepEqual(kinds({ ...OK, 変更日時: '2026/09/30 14:38', インポート日時: '2026/09/30 14:38' }), ['import_stamp_bad:not_14_digits']);
  assert.deepEqual(kinds({ ...OK, 変更日時: '20260928000000', インポート日時: '20260928000000' }), ['import_stamp_bad:not_after_pre'], '前より前');
  assert.deepEqual(kinds({ ...OK, 変更日時: '20260930143813', インポート日時: '20260929171153' }), ['import_stamp_bad:not_equal']);
  // 取込の時刻の窓 (Codex #1556 R1 High): 窓の後 (別の操作で進んだ)・窓の前・実在しない日時・日付をまたぐ窓・窓が無い = 例外
  assert.deepEqual(kinds({ ...OK, 変更日時: '20260930160000', インポート日時: '20260930160000' }), ['import_stamp_bad:outside_import_window'], '窓の後');
  assert.deepEqual(kinds({ ...OK, 変更日時: '20260930133000', インポート日時: '20260930133000' }), ['import_stamp_bad:outside_import_window'], '窓の前 (前の値より後でも)');
  assert.deepEqual(kinds({ ...OK, 変更日時: '20260931143813', インポート日時: '20260931143813' }), ['import_stamp_bad:not_real_datetime'], '9 月 31 日');
  assert.deepEqual(kinds({ ...OK, 変更日時: '20260930250000', インポート日時: '20260930250000' }), ['import_stamp_bad:not_real_datetime'], '25 時');
  assert.deepEqual(kinds({ ...OK, 変更日時: '20261001000130', インポート日時: '20261001000130' }, PRE, { from: '20260930235500', to: '20261001000500' }), [], '日付をまたぐ窓');
  assert.throws(() => run(OK, PRE, null), /取込の時刻の窓/);
  assert.throws(() => run(OK, PRE, { from: '20260930150000', to: '20260930140000' }), /取込の時刻の窓/);
  assert.throws(() => run(OK, PRE, { from: '20260230140000', to: '20260930150000' }), /取込の時刻の窓/);
  assert.deepEqual([V.isRealStamp('20240229000000'), V.isRealStamp('20230229000000'), V.jstStamp(Date.UTC(2026, 8, 30, 5, 38, 13))], [true, false, '20260930143813']);
  // 取り込まなかった商品の時刻が変わった = 差 (今までどおり)
  assert.deepEqual(kinds(OK, { ...PRE, インポート日時: '20260930143813' }), ['untouched_system_changed:インポート日時']);
  // 前の値が空 (はじめての取込) = 14 桁ならよい
  const pre0 = lz([lzRow('A-1', { ...PRE, 変更日時: '', インポート日時: '' })]);
  assert.equal(V.verifyImport({ table, pre: pre0, post: lz([lzRow('A-1', OK)]), rules: R, importWindow: W }).ok, true);
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
