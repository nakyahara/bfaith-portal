/**
 * 展開ロジック (expand.js) のテスト。DBにもRMSにも触れない純粋なテスト。
 *   node apps/select-set/tests/test-expand.mjs
 *
 * ここで守りたいこと:
 *   - モール4種 (楽天/Yahoo!/Qoo10/auPAY) の商品OP書式を全部読めること
 *   - 同じ選択肢名でもセットが違えば別SKUになること
 *   - 解決できない選択枠が1つでもあれば ok=false で止まること (fail-closed)
 *   - おまけは優先順位の上から「在庫があるもの」を選ぶこと
 */
import path from 'path';
import { pathToFileURL } from 'url';

const repo =
  process.argv[2] ||
  path.resolve(
    path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')),
    '../../..',
  );
const { buildCatalog, expandOp, splitOp, isPickLabel, normalizeValue, extractCodeCandidates, pickOmake, toPasteBlocks } =
  await import(pathToFileURL(path.join(repo, 'apps/select-set/expand.js')));

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) { pass++; console.log(`  ok - ${n}`); }
  else { fail++; console.log(`  NG - ${n}`); }
};
const eq = (a, b, n) => ok(JSON.stringify(a) === JSON.stringify(b), `${n}${JSON.stringify(a) === JSON.stringify(b) ? '' : `  期待=${JSON.stringify(b)} 実際=${JSON.stringify(a)}`}`);

const known = new Set([
  'ae-coconut10', 'ae-rose10', 'ae-plumeria10', 'ae-verbena10', 'ae-whitemusk10', 'sabon10',
  'ae-resort10', 'chamomile10', 'musksavon10', 'ae-amber10', 'as-kinmokusei', 'ae-energy',
  'wae10ml-pl', 'wae10-ca', 'wae10-ro', 'wae10-sw', 'nemune-s',
  'ganeshisc20-wm', 'ganeshisc20-bc', 'goneshic-va',
  'cineole10',
]);

// ---- セット定義 (RMS customizationOptions を模したもの) ----
const aeOptions = [
  { displayName: '1本目', selections: [
    { displayValue: 'ココナッツ_ae-coconut10' }, { displayValue: 'ローズ_ae-rose10' },
    { displayValue: 'プルメリア_ae-plumeria10' }, { displayValue: 'バーベナ_ae-verbena10' },
    { displayValue: 'ホワイトムスク_ae-whitemusk10' }, { displayValue: 'サボン_sabon10' },
    { displayValue: 'リゾート＆スパ_ae-resort10' }, { displayValue: 'カモミール_chamomile10' },
    { displayValue: 'ムスク＆サボン_musksavon10' }, { displayValue: 'アンバー_ae-amber10' },
    { displayValue: 'エナジー_ae-energy' },
  ] },
  { displayName: '2本目', selections: [] },
  { displayName: '3本目', selections: [] },
  { displayName: '4本目', selections: [] },
  { displayName: '5本目', selections: [] },
];
const aeCat = buildCatalog({ setCode: 'selectae10-5', rmsOptions: aeOptions, knownProductCodes: known });

const waOptions = [
  { displayName: '1本目', selections: [
    { displayValue: '【カモミール】wae10-ca' }, { displayValue: '【ローズ】wae10-ro' },
    { displayValue: '【サンダルウッド】wae10-sw' }, { displayValue: '【プルメリア】wae10ml-pl' },
  ] },
  { displayName: '2本目', selections: [] },
  { displayName: '3本目', selections: [] },
  { displayName: 'シークレットプレゼントについては', selections: [{ displayValue: '当店が選んだアイテムですnemune-s' }] },
];
const waCat = buildCatalog({ setCode: 'selectwa10-3', rmsOptions: waOptions, knownProductCodes: known });

const ganeshCat = buildCatalog({
  setCode: 'ganesh-select3',
  rmsOptions: [
    { displayName: '1種類目', selections: [{ displayValue: 'スティック型_WHITE MUSK' }, { displayValue: 'スティック型_BLACK CHERRY' }, { displayValue: 'コーン型_VANILLA' }] },
    { displayName: '2種類目', selections: [] },
    { displayName: '3種類目', selections: [] },
  ],
  manualRows: [
    { option: 'スティック型_WHITE MUSK', code: 'ganeshisc20-wm' },
    { option: 'スティック型_BLACK CHERRY', code: 'ganeshisc20-bc' },
    { option: 'コーン型_VANILLA', code: 'goneshic-va' },
  ],
  knownProductCodes: known,
});

console.log('=== 部品 ===');
ok(isPickLabel('1本目') && isPickLabel('3種類目') && isPickLabel(' 12 本目 '), 'isPickLabel が選択枠を判定する');
ok(!isPickLabel('シークレットプレゼントについては'), 'isPickLabel はおまけ枠を選択枠と誤判定しない');
eq(normalizeValue('リゾート＆スパ'), normalizeValue('リゾートアンドスパ'), '＆ と アンド を同一視する');
eq(normalizeValue('【ホワイトムスク】'), 'ホワイトムスク', '【】を落とす');
ok(extractCodeCandidates('ココナッツ_ae-coconut10').includes('ae-coconut10'), '_ の後ろを候補に取る');
ok(extractCodeCandidates('【カモミール】wae10-ca').includes('wae10-ca'), '】の後ろを候補に取る');
ok(extractCodeCandidates('16.スイートオレンジsweetorange10').includes('sweetorange10'), '末尾の英数字塊を候補に取る');

console.log('\n=== モール4種の商品OP ===');
const rakuten = '1本目:プルメリア_ae-plumeria10|2本目:バーベナ_ae-verbena10|3本目:サボン_sabon10|4本目:ローズ_ae-rose10|5本目:ココナッツ_ae-coconut10';
eq(expandOp({ catalog: aeCat, op: rakuten }).lines.map((l) => l.code),
  ['ae-plumeria10', 'ae-verbena10', 'sabon10', 'ae-rose10', 'ae-coconut10'], '楽天 (| 区切り)');

const yahoo = '1本目:ココナッツ_ae-coconut10 2本目:ローズ_ae-rose10 3本目:サボン_sabon10 4本目:サボン_sabon10 5本目:サボン_sabon10';
eq(expandOp({ catalog: aeCat, op: yahoo }).lines.map((l) => l.code),
  ['ae-coconut10', 'ae-rose10', 'sabon10', 'sabon10', 'sabon10'], 'Yahoo! (半角スペース区切り・同じ商品の重複も行を分ける)');

const qoo10 = '1本目:【ホワイトムスク】ae-whitemusk10,2本目:【エナジー】ae-energy,3本目:【プルメリア】ae-plumeria10,4本目:【ローズ】ae-rose10,5本目:【アンバー】ae-amber10';
eq(expandOp({ catalog: aeCat, op: qoo10 }).lines.map((l) => l.code),
  ['ae-whitemusk10', 'ae-energy', 'ae-plumeria10', 'ae-rose10', 'ae-amber10'], 'Qoo10 (, 区切り・【】付き)');

const aupay = '1本目=リゾートアンドスパ_ae-resort10&2本目=カモミール_chamomile10&3本目=ムスクサボン_musksavon10&4本目=サボン_sabon10&5本目=アンバー_ae-amber10';
eq(expandOp({ catalog: aeCat, op: aupay }).lines.map((l) => l.code),
  ['ae-resort10', 'chamomile10', 'musksavon10', 'sabon10', 'ae-amber10'], 'auPAY (& 区切り・= ラベル区切り・アンド表記)');

console.log('\n=== 商品コードが埋まっていない場合 ===');
eq(expandOp({ catalog: waCat, op: '1本目:カモミール|2本目:ローズ|3本目:サンダルウッド' }).lines.map((l) => l.code),
  ['wae10-ca', 'wae10-ro', 'wae10-sw'], '表示名だけでも辞書で引ける');
ok(expandOp({ catalog: aeCat, op: '1本目:プルメリア|2本目:ローズ|3本目:サボン|4本目:ローズ|5本目:サボン' }).lines[0].code === 'ae-plumeria10'
  && expandOp({ catalog: waCat, op: '1本目:プルメリア|2本目:カモミール|3本目:ローズ' }).lines[0].code === 'wae10ml-pl',
  '同じ「プルメリア」でもセットが違えば別SKUになる');

console.log('\n=== ganesh (N種類目・値に空白が入る) ===');
const gRes = expandOp({ catalog: ganeshCat, op: '1種類目:スティック型_WHITE MUSK|2種類目:スティック型_BLACK CHERRY|3種類目:コーン型_VANILLA' });
eq(gRes.lines.map((l) => l.code), ['ganeshisc20-wm', 'ganeshisc20-bc', 'goneshic-va'], '「N種類目」でも分割でき、値の空白で壊れない');
eq(expandOp({ catalog: ganeshCat, op: '1種類目:スティック型_WHITE　MUSK 2種類目:スティック型_BLACK CHERRY 3種類目:コーン型_VANILLA' }).lines.length,
  3, '全角スペース + 空白区切りでも壊れない');

console.log('\n=== おまけ ===');
const stock = { 'nemune-s': 0, 'nemune-g': 0, 'petirfleur-wr': 630 };
const stockOf = (c) => ({ available: stock[c] ?? 0, name: c });
const priority = ['nemune-s', 'nemune-g', 'petirfleur-wr'];
const omakeOp = '1本目:カモミール|2本目:ローズ|3本目:サンダルウッド|シークレットプレゼントについては:当店が選んだアイテムですnemune-s';
const oRes = expandOp({ catalog: waCat, op: omakeOp, omakePriority: priority, stockOf });
ok(oRes.ok, 'おまけ付きでも ok');
eq(oRes.omake.code, 'petirfleur-wr', '在庫0を飛ばして在庫のある候補を選ぶ');
eq(oRes.omake.candidates.map((c) => `${c.code}:${c.available}`), ['nemune-s:0', 'nemune-g:0', 'petirfleur-wr:630'], '候補と在庫を全部返す (人が変更できるように)');
eq(oRes.lines.length, 3, 'おまけは lines には入らない (別扱い)');

const oldLabel = '1本目:カモミール|2本目:ローズ|3本目:サンダルウッド|おまけについては:当店が選んだアイテムですnemune-s';
eq(expandOp({ catalog: waCat, op: oldLabel, omakePriority: priority, stockOf }).omake?.code, 'petirfleur-wr',
  'RMSに無い「おまけについては」ラベルでも拾える');

const allOut = expandOp({ catalog: waCat, op: omakeOp, omakePriority: ['nemune-s', 'nemune-g'], stockOf });
ok(allOut.ok && !allOut.omake.code && allOut.warnings.some((w) => /在庫切れ/.test(w)),
  '候補が全滅ならおまけ行を入れず警告する (本体は展開する)');

console.log('\n=== 無視すべき項目 / fail-closed ===');
const withConsent = '文字数の関係でオイルを選択する項目の表示名を省略しております。ご了承ください。:確認しました。|1本目:プルメリア_ae-plumeria10|2本目:ローズ_ae-rose10|3本目:サボン_sabon10|4本目:ローズ_ae-rose10|5本目:サボン_sabon10';
const cRes = expandOp({ catalog: aeCat, op: withConsent });
ok(cRes.ok && cRes.lines.length === 5, '同意確認の項目は無視して本体だけ展開する');

const bad = expandOp({ catalog: waCat, op: '1本目:カモミール|2本目:ローズローズ|3本目:サンダルウッド' });
ok(!bad.ok, '解決できない選択枠があれば ok=false (自動投入させない)');
ok(bad.warnings.some((w) => /ローズローズ/.test(w)), '止めた理由が分かる警告が出る');
ok(!expandOp({ catalog: aeCat, op: '' }).ok, '商品OPが空なら ok=false');
ok(!expandOp({ catalog: aeCat, op: 'ラッピング:あり' }).ok, '選択枠が1つも無ければ ok=false');

console.log('\n=== 商品マスタ照合のフォールバック ===');
const fb = expandOp({ catalog: aeCat, op: '1本目:7.ロ-ズマリ-シネオ-ル_cineole10|2本目:ローズ_ae-rose10|3本目:サボン_sabon10|4本目:ローズ_ae-rose10|5本目:サボン_sabon10' });
ok(fb.ok && fb.lines[0].code === 'cineole10', '選択肢定義に無くても商品マスタに実在すれば拾う');
eq(fb.lines[0].via, 'master', 'どう解決したかが分かる');
ok(fb.notices.some((n) => /商品マスタ照合/.test(n)), 'マスタ照合で拾ったことを人に見せる notice が出る');

console.log('\n=== 数量 ===');
const q2 = expandOp({ catalog: waCat, op: omakeOp, quantity: 2, omakePriority: priority, stockOf });
ok(q2.lines.every((l) => l.quantity === 2) && q2.omake.quantity === 2, 'セット2個ならおまけ含め全行が×2');

console.log('\n=== NEに貼る形 ===');
const blocks = toPasteBlocks(oRes);
eq(blocks.rowCount, 4, 'おまけを足した行数');
eq(blocks.codes.split('\n'), ['wae10-ca', 'wae10-ro', 'wae10-sw', 'petirfleur-wr'], '商品コードは改行区切り');
eq(blocks.quantities.split('\n'), ['1', '1', '1', '1'], '受注数も改行区切りで同じ行数');

console.log('\n=== splitOp 単体 ===');
eq(splitOp('1本目:あ|2本目:い', [{ name: '1本目', isPick: true }, { name: '2本目', isPick: true }]).map((p) => p.value), ['あ', 'い'], 'ラベルで切って値だけ取る');
eq(splitOp('1本目:あ', []).map((p) => p.label), ['1本目'], 'ラベル定義が無くても N本目 で拾える');

// ===== Codexレビュー (2026-08-08) で指摘された事故シナリオの再現テスト =====
// いずれも「誤った商品を明細に入れてしまう」経路なので、必ず止まることを固定する。

console.log('\n=== #1 正規化キーの衝突 ===');
const dupCat = buildCatalog({
  setCode: 'dup-3',
  rmsOptions: [
    { displayName: '1本目', selections: [
      { displayValue: 'ローズ_ae-rose10' },      // 表示名だけのキー「ローズ」を作る
      { displayValue: 'ローズ_ae-amber10' },     // 同じ「ローズ」で別SKU → 曖昧
      { displayValue: 'サボン_sabon10' },
    ] },
    { displayName: '2本目', selections: [] },
    { displayName: '3本目', selections: [] },
  ],
  knownProductCodes: known,
});
ok(dupCat.conflicts.length > 0, '衝突を検出して記録する');
ok(!dupCat.options.has(normalizeValue('ローズ')), '衝突したキーは辞書から消える (先勝ちさせない)');
ok(!expandOp({ catalog: dupCat, op: '1本目:ローズ|2本目:サボン_sabon10|3本目:サボン_sabon10' }).ok,
  '曖昧な選択肢は解決せず ok=false になる');
eq(expandOp({ catalog: dupCat, op: '1本目:サボン_sabon10|2本目:サボン_sabon10|3本目:サボン_sabon10' }).lines.length,
  3, '衝突と無関係な選択肢は今までどおり解決できる');

console.log('\n=== #2 部分文字列での誤展開 ===');
const subKnown = new Set(['abc10', 'abc100x', 'sabon10']);
const subCat = buildCatalog({
  setCode: 'sub-3',
  rmsOptions: [
    { displayName: '1本目', selections: [{ displayValue: 'テストA_abc10' }, { displayValue: 'サボン_sabon10' }] },
    { displayName: '2本目', selections: [] },
    { displayName: '3本目', selections: [] },
  ],
  knownProductCodes: subKnown,
});
ok(subCat.skus.includes('abc10'), '前提: abc10 はこのセットの許可SKU');
const subRes = expandOp({ catalog: subCat, op: '1本目:別商品_abc100x|2本目:サボン_sabon10|3本目:サボン_sabon10' });
ok(subRes.lines[0]?.code !== 'abc10', '🚨 abc100x を abc10 と誤解しない (部分一致で拾わない)');
eq(subRes.lines[0]?.code, 'abc100x', '商品マスタに実在する abc100x として解決する');
ok(!expandOp({ catalog: subCat, op: '1本目:未登録_abc10zzz|2本目:サボン_sabon10|3本目:サボン_sabon10' }).ok,
  'どこにも実在しないコードは解決せず止まる');

console.log('\n=== #3 選択枠の数の検証 ===');
ok(!expandOp({ catalog: aeCat, op: '1本目:ローズ_ae-rose10|2本目:サボン_sabon10|3本目:サボン_sabon10' }).ok,
  '🚨 5本セットで3枠しか読めなければ ok=false (全部解決できても通さない)');
ok(expandOp({ catalog: aeCat, op: '1本目:ローズ_ae-rose10|2本目:サボン_sabon10|3本目:サボン_sabon10' })
  .warnings.some((w) => /5個/.test(w)), '何個あるはずかを警告に書く');
ok(!expandOp({ catalog: waCat, op: '1本目:カモミール|1本目:ローズ|3本目:サンダルウッド' }).ok,
  '同じ枠が2回出てきたら ok=false');
const noLabelCat = buildCatalog({ setCode: 'x', rmsOptions: [], manualRows: [{ option: 'あ', code: 'sabon10' }], knownProductCodes: known });
ok(!expandOp({ catalog: noLabelCat, op: '1本目:あ' }).ok,
  '選択枠の数が分からない (RMS定義なし) なら展開しない');

console.log('\n=== #4 値の中の記号をラベルと誤認しない ===');
const eqRes = expandOp({ catalog: waCat, op: '1本目:カモミール|2本目:ローズ|3本目:サンダルウッド' });
ok(eqRes.ok, '前提: 通常の3枠は通る');
eq(splitOp('1本目:あ=い|2本目:う|3本目:え', waCat.labels).map((p) => p.value), ['あ=い', 'う', 'え'],
  '🚨 値の中の = をラベル区切りと誤認して値を切らない');
eq(splitOp('1本目:A,B|2本目:う|3本目:え', waCat.labels).map((p) => p.value), ['A,B', 'う', 'え'],
  '値の中の , でも切らない');

console.log('\n=== #10 数量は補正せず拒否する ===');
for (const q of [0, -10, 1.5, 'あ', '', null, 1000]) {
  ok(!expandOp({ catalog: waCat, op: '1本目:カモミール|2本目:ローズ|3本目:サンダルウッド', quantity: q }).ok,
    `受注数 ${JSON.stringify(q)} は拒否する`);
}
ok(expandOp({ catalog: waCat, op: '1本目:カモミール|2本目:ローズ|3本目:サンダルウッド', quantity: 3 }).lines
  .every((l) => l.quantity === 3), '正しい数量はそのまま使う');

console.log(`\n合計: ${pass} pass / ${fail} NG`);
process.exit(fail === 0 ? 0 : 1);
