/**
 * いろは作業仕様マスタ (apps/inbound-check/work-master.js) のテスト
 *
 * 実行: node scripts/test-inbound-check-work-master.mjs
 *
 * 検証項目:
 *   1. xlsx 読み取り: IMPORTRANGE 数式セルの result 解決 / 重複コード先勝ち / FLG・数値の検証
 *   2. FLG × f_inbound_info の突合 (8区分・食い違い一覧・書き込み候補)
 *   3. 本取込 (upsert): 新規 / 変化なし / 更新で version が上がる
 *   4. seedIrohaFlags: 未設定の SKU だけに書く (行が無ければ addManual 経由・値がある行は触らない)
 *   5. 管理画面編集: 楽観ロック / 数値検証 / 追加は mirror_products に居る商品のみ
 *   6. Notion カードへの反映: wm があれば 資材セットID・収納容器・入数・工程数・備考 が載る
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const ExcelJS = require('exceljs');

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-wm-test-'));
}

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { getDB } = await import('../apps/inbound-check/db.js');
const {
  parseWorkMasterXlsx, compareIrohaFlags, applyWorkMaster, seedIrohaFlags,
  workMasterStats, searchWorkMaster, updateWorkMasterRow, addWorkMasterRow, logWorkMasterImport,
  importIssueCount, computeDeletions,
} = await import('../apps/inbound-check/work-master.js');
const { buildCardProperties, calcExternal } = await import('../apps/inbound-check/notion-sync.js');
const { addManual } = await import('../apps/inbound-info/db.js');

console.log('DATA_DIR =', process.env.DATA_DIR);
const db = getDB();

// ─── 参照データ: mirror_products (PROD-1〜7。PROD-8 はわざと入れない = 廃番の再現) ───
const insProd = db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at)
  VALUES (?, ?, ?, '単品', '取扱中', '確定', '2026-09-02T00:00:00Z')`);
for (let i = 1; i <= 7; i++) insProd.run(i, `PROD-${i}`, `商品${i}`);

// f_inbound_info: PROD-2 = 行あり未設定 / PROD-6・7 = 有り
for (const c of ['PROD-2', 'PROD-6', 'PROD-7']) {
  const a = addManual(c, 'test');
  ok(a.ok, `f_inbound_info 準備 ${c}`);
}
db.prepare("UPDATE f_inbound_info SET いろは在庫化作業有無 = '有り' WHERE code_key IN ('prod-6', 'prod-7')").run();

// ─── xlsx を組み立てる (実ファイルと同じ列構成 + IMPORTRANGE 数式ヘッダー/セル) ───
async function buildXlsx() {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('作業内容管理マスター');
  ws.addRow(['商品コード', '商品名', '仕入先', '取扱区分', '在庫化必要FLG', '資材', '収納容器', '入数', '工程数',
    '作業動画URL', '作業拠点初期値', '外部委託対象', '備考', '作業工程', '単価']);
  // ヘッダー1列目を数式に置き換え (実ファイルは IMPORTRANGE。result が読めることを確認する)
  ws.getCell('A1').value = { formula: 'IFERROR(__xludf.DUMMYFUNCTION("IMPORTRANGE(...)"),"商品コード")', result: '商品コード' };
  const put = (cells) => ws.addRow(cells);
  put(['PROD-1', '商品1', '0001', '取扱中', '1', 'D-8', '20Lコンテナ', 180, 3, '', '', '', '割れ注意', '', '']);
  ws.getCell(`A${ws.rowCount}`).value = { formula: 'X', result: 'PROD-1' };   // データ行の数式セル
  put(['PROD-2', '商品2', '', '', '0', '', '', '', '', '', '', '', '', '', '']);
  put(['prod-1', '重複', '', '', '1', '', '', '', '', '', '', '', '', '', '']);      // 大文字小文字違いの重複
  put(['PROD-3', '商品3', '', '', 'x', '', '', '', '', '', '', '', '', '', '']);     // FLG 不正
  put(['PROD-4', '商品4', '', '', '1', '', '', 'abc', '', '', '', '', '', '', '']);  // 入数 不正
  put(['PROD-5', '商品5', '', '', '1', 'T7-18', '9Lコンテナ', 80, 1, '', '', '', '', '', '']);
  put(['PROD-6', '商品6', '', '', '1', '', '', '', '', '', '', '', '', '', '']);
  put(['PROD-7', '商品7', '', '', '0', '', '', '', '', '', '', '', '', '', '']);
  put(['PROD-8', '商品8(廃番)', '', '', '1', '', '', '', '', '', '', '', '', '', '']);
  put([1234, '数値セル商品', '', '', '1', '', '', '', '', '', '', '', '', '', '']);   // 商品コードが数値セル → 検証エラー
  put(['', '', '', '', '', '', '', '', '', '', '', '', '', '', '']);                 // 全空行 → 無視
  return Buffer.from(await wb.xlsx.writeBuffer());
}

console.log('\n[1] xlsx 読み取り');
const buf = await buildXlsx();
const parsed = await parseWorkMasterXlsx(buf);
{
  ok(parsed.rows.length === 8, `有効行 8 (実際 ${parsed.rows.length})`);
  ok(parsed.rows[0].code === 'PROD-1', 'IMPORTRANGE 数式セルは result を読む');
  const p1 = parsed.rows[0];
  ok(p1.material === 'D-8' && p1.container === '20Lコンテナ' && p1.units === 180 && p1.processCount === 3 && p1.note === '割れ注意',
    '資材・収納容器・入数・工程数・備考を読める');
  ok(parsed.issues.duplicates.length === 1, '重複コード (大文字小文字違い) は先勝ちで記録');
  ok(parsed.issues.badFlg.length === 1 && parsed.rows.find(r => r.code === 'PROD-3').flg === null, 'FLG 不正は未記入扱い + 記録');
  ok(parsed.issues.badUnits.length === 1 && parsed.rows.find(r => r.code === 'PROD-4').units === null, '入数不正は null + 記録');
  ok(parsed.issues.numericCode.length === 1 && !parsed.rows.some(r => r.code === '1234'),
    '数値セルの商品コードは検証エラーにして行を除外 (先頭ゼロ喪失の防止)');
  ok(importIssueCount(parsed.issues) === 4, `検証エラー合計 4 → 本取込は拒否される (実際 ${importIssueCount(parsed.issues)})`);
  ok(parsed.hasFlgColumn === true, 'FLG列があれば hasFlgColumn=true');
}

console.log('\n[1b] 在庫化必要FLG 列なしでも取り込める (FLGは廃止 — 中原さん 2026-09-02)');
{
  const wb2 = new ExcelJS.Workbook();
  const ws2 = wb2.addWorksheet('作業内容管理マスター');
  ws2.addRow(['商品コード', '商品名', '資材', '収納容器', '入数', '工程数', '備考']);
  ws2.addRow(['PROD-1', '商品1', 'D-9', '9Lコンテナ', 60, 2, '']);
  const p2 = await parseWorkMasterXlsx(Buffer.from(await wb2.xlsx.writeBuffer()));
  ok(p2.hasFlgColumn === false && p2.rows.length === 1 && p2.rows[0].flg === null,
    'FLG列なし: hasFlgColumn=false・行は読める・flg=null');
  ok(importIssueCount(p2.issues) === 0, 'FLG列が無いこと自体はエラーにしない');
}

console.log('\n[2] FLG × f_inbound_info の突合');
const compare = compareIrohaFlags(parsed.rows);
{
  const b = compare.buckets;
  ok(b.old1_yes === 1, '旧1/現行有り = PROD-6');
  ok(b.old0_yes === 1, '旧0/現行有り (食い違い) = PROD-7');
  ok(b.cur_unset === 5, `現行未設定 (書き込み候補) = 5 (実際 ${b.cur_unset})`);
  ok(b.flg_blank === 1, 'FLG未記入 = PROD-3');
  ok(b.not_in_mirror === 1, '商品マスタに無い = PROD-8');
  ok(compare.mismatches.length === 1 && compare.mismatches[0].code === 'PROD-7', '食い違い一覧 = PROD-7 だけ');
  ok(compare.seedable.length === 5, '書き込み候補 = PROD-1,2,4,5,8');
}

console.log('\n[3] 本取込 (upsert)');
{
  const c1 = applyWorkMaster(parsed.rows, { user: 'test' });
  ok(c1.inserted === 8 && c1.updated === 0, `初回は全部 insert (${c1.inserted})`);
  const c2 = applyWorkMaster(parsed.rows, { user: 'test' });
  ok(c2.inserted === 0 && c2.updated === 0 && c2.unchanged === 8, '同じ内容の再取込は変化なし');
  const modified = parsed.rows.map(r => (r.code === 'PROD-1' ? { ...r, note: '割れ注意 (更新)' } : r));
  const c3 = applyWorkMaster(modified, { user: 'test' });
  const row = db.prepare("SELECT * FROM f_iroha_work_master WHERE code_key = 'prod-1'").get();
  ok(c3.updated === 1 && row.note === '割れ注意 (更新)' && row.version === 2, '変更行だけ更新され version が上がる');
  logWorkMasterImport({ actor: 'test', fileName: 't.xlsx', ok: true, message: 'test' });
  ok(!!db.prepare("SELECT 1 FROM f_inbound_check_import_log WHERE source = 'work_master_xlsx'").get(), '取込ログが残る');
}

console.log('\n[4] seedIrohaFlags (未設定の SKU だけに書く)');
{
  const s = seedIrohaFlags(compare.seedable, { user: 'test' });
  ok(s.seeded === 4, `書き込み 4 件 (実際 ${s.seeded})`);      // PROD-1,2,4,5 (PROD-8 は mirror に無い)
  ok(s.added === 3, `行の新規作成 3 件 (PROD-1,4,5。実際 ${s.added})`);
  ok(s.notInMaster === 1, 'PROD-8 は商品マスタに無く見送り');
  ok(Array.isArray(s.errorDetails) && s.errors === 0, '失敗の内訳 (errorDetails) を返す (今回は0件)');
  const v = (k) => db.prepare('SELECT いろは在庫化作業有無 AS i, 入庫時BCシール貼りフラグ AS bc FROM f_inbound_info WHERE code_key = ?').get(k);
  ok(v('prod-1')?.i === '有り', 'PROD-1 (FLG=1) → 有り');
  ok(v('prod-1')?.bc === '－', '有り の連動ルール (BCシール等=－) が効いている (updateInbound 経由の証拠)');
  ok(v('prod-2')?.i === '無し', 'PROD-2 (FLG=0・行あり未設定) → 無し');
  ok(v('prod-7')?.i === '有り', 'PROD-7 (食い違い) は触らない');
  // もう一度呼んでも上書きしない (値が入ったので skipped になる)
  const s2 = seedIrohaFlags(compare.seedable, { user: 'test' });
  ok(s2.seeded === 0 && s2.skipped >= 4, '2回目は書かない (冪等)');
}

console.log('\n[5] 管理画面編集');
{
  const bad = updateWorkMasterRow('PROD-1', { units_per_container: 'xx' }, 'test', 2);
  ok(bad.ok === false && bad.error === 'bad_number', '数値でない入数は拒否');
  const conflict = updateWorkMasterRow('PROD-1', { note: 'x' }, 'test', 99);
  ok(conflict.ok === false && conflict.error === 'conflict', 'version 不一致は conflict');
  const okUpd = updateWorkMasterRow('PROD-1', { units_per_container: '200', note: '' }, 'test', 2);
  ok(okUpd.ok && okUpd.row.units_per_container === 200 && okUpd.row.note === null && okUpd.row.version === 3,
    '正しい version なら更新 (空文字は null)');
  const nf = updateWorkMasterRow('PROD-99', { note: 'x' }, 'test', 1);
  ok(nf.ok === false && nf.error === 'not_found', '無い商品は not_found');
  const addNg = addWorkMasterRow('PROD-99', 'test');
  ok(addNg.ok === false && addNg.error === 'not_in_master', '商品マスタに無いコードは追加できない');
  const addDup = addWorkMasterRow('PROD-1', 'test');
  ok(addDup.ok === false && addDup.error === 'duplicate', '既存コードは duplicate');
  const st = workMasterStats();
  ok(st.total === 8 && st.filled >= 2, `stats: total=${st.total} filled=${st.filled}`);
  const found = searchWorkMaster('PROD-1');
  ok(found.length >= 1 && found[0].product_name === '商品1', '検索は mirror_products の商品名付き');
}

console.log('\n[6] Notion カードへの反映 (buildCardProperties + wm)');
{
  const names = new Set(['名前', 'ステータス', '数量', '資材セットID', '収納容器', '入数', '工程数', '備考', '台帳キー', 'destination_id']);
  const wm = db.prepare("SELECT * FROM f_iroha_work_master WHERE code_key = 'prod-5'").get();
  const row = { id: 1, product_id: 'PROD-5', product_name: '商品5', actual_qty: 10, planned_qty: 10, ar_no: 'AR1', work_date: '2026-09-02', code_key: 'prod-5' };
  const props = buildCardProperties(row, { barcode: null, product: null, supplierName: null, ext: calcExternal(null, null), dedupeKey: 'd1-test', wm }, names);
  ok(props['資材セットID'].select.name === 'T7-18', '資材 → 資材セットID (select — 実DBの型)');
  ok(props['収納容器'].select.name === '9Lコンテナ', '収納容器 (select)');
  ok(props['入数'].number === 80, '入数 = units_per_container (いろは容器あたり)');
  ok(props['工程数'].number === 1, '工程数');
  const noWm = buildCardProperties(row, { barcode: null, product: null, supplierName: null, ext: calcExternal(null, null), dedupeKey: 'd1-test', wm: null }, names);
  ok(!('入数' in noWm) && !('資材セットID' in noWm), 'マスタ未整備の商品はこれらの項目を送らない');
}

console.log('\n[7] [PR2-R2 High] 取込は全置換 — xlsx に無い既存行は削除される');
{
  const subset = parsed.rows.filter(r => r.code !== 'PROD-8');
  const preview = computeDeletions(subset);
  ok(preview.count === 1 && preview.codes.length === preview.count && preview.codes[0] === 'PROD-8',
    'dry-run の削除予定は全件のコード一覧を返す');
  const c = applyWorkMaster(subset, { user: 'test' });
  ok(c.deleted === 1 && !db.prepare("SELECT 1 FROM f_iroha_work_master WHERE code_key = 'prod-8'").get(),
    'xlsx から消えた PROD-8 の行が削除される (廃止した作業仕様を残さない)');
  const c2 = applyWorkMaster(parsed.rows, { user: 'test' });
  ok(c2.inserted === 1 && c2.deleted === 0, '戻せば再作成される');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail === 0 ? 0 : 1;
