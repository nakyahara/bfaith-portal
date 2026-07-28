/**
 * 入庫情報管理 (apps/inbound-info) — E2E テスト
 *
 * 実行: DATA_DIR=<空ディレクトリ> node scripts/test-inbound-info.mjs [入庫情報管理表.xlsx のパス]
 *   - xlsx パスを渡すと実ファイルで Excel 取込を検証 (省略時は合成ワークブックで検証)
 *   - DATA_DIR 未指定時は一時ディレクトリを作って使う (終了後も残す: 内容確認用)
 *
 * 検証項目:
 *   1. DDL: initMirrorDB() で f_inbound_info / f_inbound_origin が作成される
 *   2. syncNewProducts: 単品・取扱中のみ / 大文字小文字違いは追加しない / mirror空ならskip
 *   3. Excel 取込: dry_run が書き込まない / 本取込の件数・値 / 数値コードが "xxx.0" にならない
 *   4. updateInbound: source が manual に倒れる / 入数バリデーション
 *   5. addManual / deleteInbound / 原産国 upsert・重複拒否
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import assert from 'assert';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'inbound-info-test-'));
}
console.log('DATA_DIR =', process.env.DATA_DIR);

const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const db = initMirrorDB();
const {
  syncNewProducts, importInboundRows, importOriginRows, importWorkbook, listInbound, updateInbound,
  addManual, deleteInbound, upsertOrigin, deleteOrigin, listOrigin, stats, parseIrisu, getInbound,
} = await import('../apps/inbound-info/db.js');
const { parseIrisuSheet, parseOriginSheet } = await import('../apps/inbound-info/router.js');
const { parseNefudaCsv } = await import('../apps/inbound-info/nefuda-fetch.js');
const { replaceSchedule, scheduleState } = await import('../apps/inbound-info/db.js');
const ExcelJS = (await import('exceljs')).default;
const iconv = (await import('iconv-lite')).default;

let passed = 0;
function ok(cond, label) {
  assert.ok(cond, label);
  passed++;
  console.log('  ✅', label);
}

// ─── 1. DDL ───
console.log('\n[1] DDL');
const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'f_inbound%' ORDER BY name").all().map((r) => r.name);
ok(tables.includes('f_inbound_info') && tables.includes('f_inbound_origin'), 'f_inbound_info / f_inbound_origin 作成');

// ─── 2. syncNewProducts ───
console.log('\n[2] syncNewProducts');
const empty = syncNewProducts();
ok(empty.ok === false && empty.error === 'mirror_empty', 'mirror_products 空なら skip (fail-safe)');

const now = new Date().toISOString();
const insProd = db.prepare(`
  INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at)
  VALUES (?, ?, ?, ?, '確定', ?)
`);
insProd.run('newitem01', '新商品テスト1', '単品', '取扱中', now);
insProd.run('NEWITEM02', '新商品テスト2 (大文字コード)', '単品', '取扱中', now);
insProd.run('setitem01', 'セット商品 (対象外)', 'セット', '取扱中', now);
insProd.run('stopitem01', '取扱中止 (対象外)', '単品', '取扱中止', now);
insProd.run('exceptitem01', '例外 (対象外)', '例外', '取扱中', now);

let r = syncNewProducts();
ok(r.ok && r.inserted === 2, `単品・取扱中のみ追加 (inserted=${r.inserted}, 期待2)`);
const auto1 = db.prepare("SELECT * FROM f_inbound_info WHERE code_key = 'newitem02'").get();
ok(auto1 && auto1.商品コード === 'NEWITEM02' && auto1.source === 'auto', '元表記は保持しつつ code_key は小文字化');

// 再実行しても増えない
r = syncNewProducts();
ok(r.ok && r.inserted === 0, '再実行で重複追加しない (冪等)');

// 大文字小文字違いの既存行は追加しない
db.prepare("UPDATE f_inbound_info SET 商品コード = 'NewItem01' WHERE code_key = 'newitem01'").run();
r = syncNewProducts();
ok(r.ok && r.inserted === 0, '大文字小文字違いを別商品として誤追加しない');

// ─── 3. Excel 取込 ───
console.log('\n[3] Excel 取込');
const realXlsx = process.argv[2];
let wb = new ExcelJS.Workbook();
if (realXlsx) {
  await wb.xlsx.readFile(realXlsx);
  console.log('  (実ファイルで検証:', realXlsx + ')');
} else {
  const ws = wb.addWorksheet('入数マスタ');
  ws.addRow(['商品コード', '商品名', '入数', '入庫時BCシール貼りフラグ', '直接ピックロケ保管', 'BF保管荷姿', 'いろは在庫化作業有無']);
  ws.addRow(['excelitem01', 'Excel商品1', 100, 'BCシール貼付必要', null, 'そのまま', '無し']);
  ws.addRow([884389, '数値コード商品', 500, null, null, null, null]); // 数値セルのコード
  ws.addRow(['newitem01', '自動追加済みを上書き', 30, null, null, 'バラ', null]);
  ws.addRow(['excelitem01', '重複行 (スキップ)', 1, null, null, null, null]);
  ws.addRow(['baditem01', '入数不正 (スキップ)', 'abc', null, null, null, null]);
  const wo = wb.addWorksheet('原産国');
  wo.addRow(['管理コード', '識別番号', '商品コード', '商品名', '産地・数値等', '画像']);
  wo.addRow(['P3FB', 201302, 'lavender10', '真正ラベンダーオイル 【10ml】', 'ブルガリア', true]);
  wo.addRow(['P3FB', 201303, 'hiba10', 'ひば油 【10mL】', '日本', false]);
}
const irisuWs = wb.getWorksheet('入数マスタ') || wb.worksheets[0];
const parsed = parseIrisuSheet(irisuWs);
ok(!parsed.error, `入数マスタのヘッダー認識 (${parsed.rows?.length}行)`);

const before = db.prepare('SELECT COUNT(*) AS c FROM f_inbound_info').get().c;
const dry = importInboundRows(parsed.rows, { dryRun: true, user: 'test' });
const afterDry = db.prepare('SELECT COUNT(*) AS c FROM f_inbound_info').get().c;
ok(afterDry === before, `dry_run は書き込まない (件数 ${before} のまま)`);
console.log('  dry_run report:', JSON.stringify({ ...dry, warnings: dry.warnings.slice(0, 3) }));

const live = importInboundRows(parsed.rows, { dryRun: false, user: 'test' });
ok(live.inserted === dry.inserted && live.updated === dry.updated && live.skipped === dry.skipped,
  `dry_run と本取込のレポート一致 (新規${live.inserted}/上書き${live.updated}/スキップ${live.skipped})`);
const afterLive = db.prepare('SELECT COUNT(*) AS c FROM f_inbound_info').get().c;
ok(afterLive === before + live.inserted, `本取込後の件数 = ${before} + ${live.inserted}`);

if (!realXlsx) {
  const numCode = db.prepare("SELECT * FROM f_inbound_info WHERE code_key = '884389'").get();
  ok(numCode && numCode.商品コード === '884389' && numCode.入数 === 500, "数値セルのコードが '884389.0' にならない");
  const overwritten = db.prepare("SELECT * FROM f_inbound_info WHERE code_key = 'newitem01'").get();
  ok(overwritten.入数 === 30 && overwritten.source === 'excel' && overwritten.BF保管荷姿 === 'バラ', 'Excel 値で上書き + source=excel');
  ok(live.skipped === 2, '重複行 + 入数不正行をスキップ');
  const po = parseOriginSheet(wb.getWorksheet('原産国'));
  const originRep = importOriginRows(po.rows, { dryRun: false, user: 'test' });
  ok(originRep.inserted === 2, '原産国 2行取込');
  const lav = db.prepare("SELECT * FROM f_inbound_origin WHERE code_key = 'lavender10'").get();
  ok(lav && lav.識別番号 === '201302' && lav.画像有無 === 1, '原産国: 識別番号の数値→文字列化 + 画像フラグ');
} else {
  // 実ファイル: 取扱中止も含む旧シート全量が入ることと、入数が整数で入ることを確認
  const st = stats();
  console.log('  実ファイル取込後 stats:', JSON.stringify(st));
  ok(st.total >= 4000, `実ファイル: 4000件以上取込 (total=${st.total})`);
  const sample = db.prepare("SELECT * FROM f_inbound_info WHERE code_key = 'razor'").get();
  ok(sample && sample.入数 === 100 && sample.入庫時BCシール貼りフラグ === 'BCシール貼付必要', '実ファイル: razor の入数=100 / BCシール値');
  const po = parseOriginSheet(wb.getWorksheet('原産国'));
  if (po.rows) {
    const originRep = importOriginRows(po.rows, { dryRun: false, user: 'test' });
    console.log('  原産国 report:', JSON.stringify({ ...originRep, warnings: originRep.warnings.slice(0, 3) }));
    ok(originRep.inserted + originRep.updated >= 40, `実ファイル: 原産国 ${originRep.inserted + originRep.updated} 行`);
  }
  // 実ファイル取込後、mirror 由来の新商品が sync で追加される (Excel に無い newitem01 等は取込済みなので 0)
  const r2 = syncNewProducts();
  ok(r2.ok, `実ファイル取込後の syncNewProducts (inserted=${r2.inserted})`);
}

// ─── 4. updateInbound ───
console.log('\n[4] updateInbound');
const target = db.prepare("SELECT code_key, version FROM f_inbound_info WHERE source = 'auto' LIMIT 1").get()
  || db.prepare('SELECT code_key, version FROM f_inbound_info LIMIT 1').get();
let u = updateInbound(target.code_key, { 入数: '250', BF保管荷姿: 'そのまま' }, 'tester', target.version);
ok(u.ok, '更新成功');
const updated = db.prepare('SELECT * FROM f_inbound_info WHERE code_key = ?').get(target.code_key);
ok(updated.入数 === 250 && updated.source === 'manual' && updated.updated_by === 'tester', '入数反映 + source=manual + updated_by');
ok(updated.version === target.version + 1, 'version がインクリメントされる');
const v2 = updated.version;
u = updateInbound(target.code_key, { 入数: '-5' }, 'tester', v2);
ok(!u.ok && u.error === 'invalid_irisu', '負の入数を拒否');
u = updateInbound(target.code_key, { 入数: '1.5' }, 'tester', v2);
ok(!u.ok && u.error === 'invalid_irisu', '小数の入数を拒否');
u = updateInbound(target.code_key, { 商品名: '  ' }, 'tester', v2);
ok(!u.ok && u.error === 'empty_name', '商品名の空クリアを拒否');
u = updateInbound(target.code_key, { 入数: 1 }, 'tester');
ok(!u.ok && u.error === 'version_required', 'version 未指定は拒否 (楽観ロック必須)');
u = updateInbound('no-such-code', { 入数: 1 }, 'tester', 1);
ok(!u.ok && u.error === 'not_found', '存在しないコードは not_found');
ok(parseIrisu(null).ok && parseIrisu(null).value === null, '入数 null は許容 (未記入)');

// ─── 5. addManual / deleteInbound / 原産国 CRUD ───
console.log('\n[5] addManual / delete / 原産国');
let a = addManual('setitem01', 'tester');
ok(a.ok, 'セット商品も手動でなら追加できる (自動対象外なだけ)');
a = addManual('setitem01', 'tester');
ok(!a.ok && a.error === 'duplicate', '重複追加を拒否');
a = addManual('ghost-code-xyz', 'tester');
ok(!a.ok && a.error === 'not_in_master', 'マスタに無いコードを拒否');
let d = deleteInbound('setitem01');
ok(d.ok, '削除成功');

let o = upsertOrigin({ 管理コード: 'TEST', 商品コード: 'origintest01', 商品名: 'テスト', 産地: '日本', 画像有無: true }, 'tester');
ok(o.ok, '原産国: 新規追加');
const oDup = upsertOrigin({ 管理コード: 'TEST', 商品コード: 'ORIGINTEST01', 産地: '中国' }, 'tester');
ok(!oDup.ok && oDup.error === 'duplicate', '原産国: 大文字小文字違いの重複を拒否');
const oVer = listOrigin().find((x) => x.商品コード === 'origintest01').version;
o = upsertOrigin({ id: o.id, expected_version: oVer, 管理コード: 'TEST', 商品コード: 'origintest01', 商品名: 'テスト改', 産地: '米国', 画像有無: false }, 'tester');
ok(o.ok, '原産国: 更新');
const oRow = listOrigin().find((x) => x.商品コード === 'origintest01');
ok(oRow && oRow.産地 === '米国' && oRow.画像有無 === 0, '原産国: 更新内容反映');
d = deleteOrigin(oRow.id);
ok(d.ok, '原産国: 削除');

// ─── 一覧/統計 ───
console.log('\n[6] 一覧/統計');
const list = listInbound({ filter: 'missing', limit: 5 });
ok(list.rows.every((x) => x.入数 == null), 'filter=missing は入数 NULL のみ');
const searchTerm = realXlsx ? 'razor' : 'excelitem01';
const list2 = listInbound({ q: searchTerm });
ok(list2.total >= 1, `検索 "${searchTerm}" ヒット (${list2.total}件)`);
const st = stats();
ok(typeof st.total === 'number' && typeof st.auto_new === 'number', 'stats 形状');
// getInbound: 保存時の競合復旧で「その1行だけ」最新化するための単票取得
const one = listInbound({ limit: 1 }).rows[0];
const got = getInbound(one.商品コード.toUpperCase());
ok(got && got.code_key === one.code_key && typeof got.version === 'number',
  'getInbound: 大文字小文字を問わず1行取得 (version 付き)');
ok(getInbound('存在しないコードzz') === null, 'getInbound: 無いコードは null');
// limit='all' (印刷用): 500件上限を無視して該当全件を1クエリで返す
const allList = listInbound({ limit: 'all' });
ok(allList.limit === 'all' && allList.rows.length === allList.total && allList.total === stats().total,
  `limit='all' は該当全件を返す (${allList.rows.length}/${allList.total})`);
ok(listInbound({ limit: 99999 }).rows.length <= 500, 'limit は通常 500 件で丸める');
ok(!listInbound({ limit: 'all' }).error, `limit='all' は上限内なら error なし (現在 ${allList.total}件)`);
// updateInbound は保存後の行 (正規化済み・新 version) を返す
const upRow = listInbound({ limit: 1 }).rows[0];
const upRes = updateInbound(upRow.code_key, { memo: '  前後に空白  ' }, 'tester', upRow.version);
ok(upRes.ok && upRes.row && upRes.row.memo === '前後に空白' && upRes.row.version === upRow.version + 1,
  'updateInbound: 保存後の行を返す (trim済み・version+1)');

// ─── 7. Codex R1/R2 対応の検証 ───
console.log('\n[7] 楽観ロック / 一括トランザクション');
const lockRow = db.prepare('SELECT code_key, version FROM f_inbound_info LIMIT 1').get();
let c1 = updateInbound(lockRow.code_key, { 入数: 999 }, 'userA', lockRow.version + 5);
ok(!c1.ok && c1.error === 'conflict', '楽観ロック: version 不一致で conflict');
c1 = updateInbound(lockRow.code_key, { 入数: 999 }, 'userA', lockRow.version);
ok(c1.ok, '楽観ロック: version 一致で更新成功');

const oc = upsertOrigin({ 管理コード: 'LOCK', 商品コード: 'locktest01', 産地: '日本' }, 'tester');
const ocRow = listOrigin().find((x) => x.商品コード === 'locktest01');
let c2 = upsertOrigin({ id: oc.id, expected_version: ocRow.version + 5, 管理コード: 'LOCK', 商品コード: 'locktest01', 産地: '中国' }, 'tester');
ok(!c2.ok && c2.error === 'conflict', '原産国 楽観ロック: conflict');
c2 = upsertOrigin({ id: oc.id, 管理コード: 'LOCK', 商品コード: 'locktest01', 産地: '中国' }, 'tester');
ok(!c2.ok && c2.error === 'version_required', '原産国 楽観ロック: version 未指定は拒否');
c2 = upsertOrigin({ id: oc.id, expected_version: ocRow.version, 管理コード: 'LOCK', 商品コード: 'locktest01', 産地: '中国' }, 'tester');
ok(c2.ok, '原産国 楽観ロック: 一致で更新成功');
deleteOrigin(oc.id);

// importWorkbook: 原産国側が途中で throw したら入数マスタ側も rollback される
const beforeAtomic = db.prepare('SELECT COUNT(*) AS c FROM f_inbound_info').get().c;
let threw = false;
try {
  importWorkbook([{ 商品コード: 'atomictest01', 商品名: 'アトミック', 入数: 1 }], [null], { user: 'tester' });
} catch (e) {
  threw = true;
}
const afterAtomic = db.prepare('SELECT COUNT(*) AS c FROM f_inbound_info').get().c;
ok(threw && afterAtomic === beforeAtomic
  && !db.prepare("SELECT 1 FROM f_inbound_info WHERE code_key = 'atomictest01'").get(),
  '一括トランザクション: 原産国側の失敗で入数マスタ側も rollback');

// ─── 8. 入荷予定 (nefuda.csv) ───
console.log('\n[8] 入荷予定 (nefuda.csv)');
const nefudaText = [
  '"商品ID","商品名","バーコード","有効期限"',
  '"newitem01","新商品テスト1 (マスタ登録済)","4560278235130",""',
  '"NEFUDAONLY01","入荷予定のみの商品 (マスタ未登録・mirror有)","4900000000001","2027-01-31"',
  '"ghostitem99","NEマスタに無いコード","4900000000002",""',
  '"newitem01","重複行","4560278235130",""',
  '"",""',
].join('\r\n') + '\r\n';
// mirror に NEFUDAONLY01 相当 (小文字表記) を用意 → CSV とは大小違いでも同一視されるべき
insProd.run('nefudaonly01', '入荷予定のみ商品 (NE正式名)', '単品', '取扱中止', now);

const nefudaRows = parseNefudaCsv(iconv.encode(nefudaText, 'Shift_JIS'));
ok(nefudaRows.length === 4, `パース: 空行除外で4行 (実際 ${nefudaRows.length})`);
ok(nefudaRows[1].有効期限 === '2027-01-31' && nefudaRows[0].バーコード === '4560278235130', 'パース: バーコード/有効期限');

const sr = replaceSchedule(nefudaRows, { filename: 'nefuda.csv', fileModifiedTime: now, user: 'tester' });
ok(sr.schedule_rows === 3 && sr.duplicates === 1, `置換: 3件+重複1スキップ (rows=${sr.schedule_rows}, dup=${sr.duplicates})`);
ok(sr.added_to_master === 2, `マスタ未登録2件を自動追加 (added=${sr.added_to_master})`);
const addedFromMirror = db.prepare("SELECT * FROM f_inbound_info WHERE code_key = 'nefudaonly01'").get();
ok(addedFromMirror && addedFromMirror.商品名 === '入荷予定のみ商品 (NE正式名)' && addedFromMirror.source === 'auto',
  '自動追加: mirror の正式名を優先 + source=auto (取扱中止でも入荷予定なら追加)');
const addedGhost = db.prepare("SELECT * FROM f_inbound_info WHERE code_key = 'ghostitem99'").get();
ok(addedGhost && sr.not_in_master.length === 1 && sr.not_in_master[0] === 'ghostitem99',
  'マスタに無いコードも追加しつつ not_in_master で警告');

const schedList = listInbound({ filter: 'scheduled', limit: 50 });
ok(schedList.total === 3, `filter=scheduled は入荷予定の3件のみ (実際 ${schedList.total})`);
// 並び順 = CSV の行順 (コード順なら ghostitem99 → newitem01 → nefudaonly01 になる)
ok(schedList.order === 'csv'
  && schedList.rows.map((r) => r.code_key).join(',') === 'newitem01,nefudaonly01,ghostitem99',
  `filter=scheduled は CSV の行順 (実際 ${schedList.rows.map((r) => r.code_key).join(',')})`);
ok(db.prepare("SELECT seq FROM f_inbound_schedule WHERE code_key = 'ghostitem99'").get().seq === 3,
  'seq に CSV 行番号 (1始まり) を保存');
ok(listInbound({ filter: 'all', limit: 5 }).order === 'code', 'scheduled 以外は従来の 新着優先→コード順');
const st8 = scheduleState();
ok(st8 && st8.row_count === 3 && st8.filename === 'nefuda.csv' && st8.fetched_by === 'tester', 'scheduleState 反映');

// 2回目の置換で完全入替 (前回行が残らない)
const sr2 = replaceSchedule([{ 商品コード: 'newitem01', 商品名: 'x', バーコード: null, 有効期限: null }], { user: 'tester' });
ok(sr2.schedule_rows === 1 && listInbound({ filter: 'scheduled' }).total === 1, 'full-replace: 前回分は残らない');

// fail-closed パース (Codex nefuda R1 High/Medium): 空・ヘッダのみ・列欠落・列数不一致は取込拒否
const enc = (t) => iconv.encode(t, 'Shift_JIS');
const throwsParse = (text) => { try { parseNefudaCsv(enc(text)); return false; } catch (e) { return e.code === 'VALIDATION'; } };
ok(throwsParse(''), 'パース拒否: 完全に空');
ok(throwsParse('"商品ID","商品名","バーコード","有効期限"\r\n'), 'パース拒否: ヘッダのみ (既存スナップショット保持)');
ok(throwsParse('"商品ID","商品名"\r\n"a","b"\r\n'), 'パース拒否: 必須ヘッダ欠落');
ok(throwsParse('"商品ID","商品名","バーコード","有効期限","商品ID"\r\n"a","b","c","d","e"\r\n'), 'パース拒否: ヘッダ重複');
ok(throwsParse('"商品ID","商品名","バーコード","有効期限"\r\n"a","b","c"\r\n'), 'パース拒否: 列数不一致 (破損CSV)');
ok(throwsParse('"商品ID","商品名","バーコード","有効期限"\r\n"a","b","c","未閉鎖\r\n'), 'パース拒否: 閉じていない引用符 (破損CSV)');

// 鮮度ガード (Codex nefuda R1 Medium): 反映済みより古い file_modified_time は上書き拒否
const newer = replaceSchedule([{ 商品コード: 'newitem01', 商品名: 'x', バーコード: null, 有効期限: null }],
  { fileModifiedTime: '2026-07-21T10:00:00.000Z', user: 'tester' });
ok(newer.ok, '鮮度ガード: 新しい版の反映は成功');
const staleR = replaceSchedule([{ 商品コード: 'setitem01', 商品名: 'y', バーコード: null, 有効期限: null }],
  { fileModifiedTime: '2026-07-21T09:00:00.000Z', user: 'tester' });
ok(!staleR.ok && staleR.error === 'stale_file', '鮮度ガード: 古い版は stale_file で拒否');
ok(listInbound({ filter: 'scheduled' }).rows[0].code_key === 'newitem01', '鮮度ガード: データは新しい版のまま');
const sameR = replaceSchedule([{ 商品コード: 'newitem01', 商品名: 'x', バーコード: null, 有効期限: null }],
  { fileModifiedTime: '2026-07-21T10:00:00.000Z', user: 'tester' });
ok(sameR.ok, '鮮度ガード: 同一時刻 (同一ファイル再取得) は冪等に成功');

// ─── 9. 自動実行の設定 + 入荷予定リストPDF ───
console.log('\n[9] 自動実行の設定 / 入荷予定リストPDF');
const { getJobSettings, saveJobSettings, parseHhmm, pdfState, savePdfState } = await import('../apps/inbound-info/db.js');

ok(getJobSettings().time === '09:00' && getJobSettings().pdf_enabled === true, '設定: 未保存なら既定 09:00 / PDF有効');
ok(parseHhmm('7:05').text === '07:05' && parseHhmm('23:59') && parseHhmm('00:00'), 'parseHhmm: 正常値');
ok(['24:00', '9:60', 'あ', '', '9', '09:0', null, '09:00 ', '0:00:00'].every((v) => parseHhmm(v) === null || v === '09:00 '),
  'parseHhmm: 不正値は null (前後空白は許容)');
ok(!saveJobSettings({ time: '25:00' }, 'tester').ok, '設定: 不正な時刻は拒否');
const sv = saveJobSettings({ time: '7:30', pdf_enabled: false }, 'tester');
ok(sv.ok && sv.time === '07:30' && sv.pdf_enabled === false, '設定: 保存 (HH:MM に正規化)');
const gs = getJobSettings();
ok(gs.time === '07:30' && gs.pdf_enabled === false && gs.updated_by === 'tester', '設定: 読み出し');
// 壊れたJSONでも既定値で動く (自動実行が止まる方が損害が大きい)
db.prepare("UPDATE dashboard_settings SET value_json = '{壊れ' WHERE key = 'inbound_info.daily_job'").run();
ok(getJobSettings().time === '09:00', '設定: JSON破損時は既定値にフォールバック');
saveJobSettings({ time: '09:00', pdf_enabled: true }, 'tester');

// cron 式の組み立て (env 上書きが勝つ)
const { effectiveSchedule } = await import('../apps/inbound-info/sync-job.js');
saveJobSettings({ time: '08:05', pdf_enabled: true }, 'tester');
const eff = effectiveSchedule();
ok(eff.expr === '5 8 * * *' && eff.source === 'settings', `cron式: 画面設定から組み立て (${eff.expr})`);
process.env.INBOUND_INFO_SYNC_CRON = '0 10 * * *';
ok(effectiveSchedule().expr === '0 10 * * *' && effectiveSchedule().source === 'env', 'cron式: env 上書きが優先');
delete process.env.INBOUND_INFO_SYNC_CRON;

// PDF 保存状態の記録 (成功/失敗どちらも上書き)
ok(pdfState() === null, 'PDF状態: 初期は null');
savePdfState({ ok: false, error: 'テスト失敗', filename: 'x.pdf', user: 'tester' });
ok(pdfState().ok === 0 && pdfState().error === 'テスト失敗', 'PDF状態: 失敗を記録');
savePdfState({ ok: true, filename: '入荷予定リスト.pdf', rows: 3, bytes: 12345, file_id: 'fid', folder_name: 'F', user: 'tester' });
const ps = pdfState();
ok(ps.ok === 1 && ps.row_count === 3 && ps.bytes === 12345 && ps.error === null, 'PDF状態: 成功で上書き (前回のエラーは消える)');

// PDF 組み立て (スタブ経路 — Drive には触らない)
process.env.INBOUND_PDF_FAKE = '1';
const { buildSchedulePdf } = await import('../apps/inbound-info/schedule-pdf.js');
const fake = await buildSchedulePdf();
ok(fake.buffer.subarray(0, 5).toString() === '%PDF-' && fake.rows === listInbound({ filter: 'scheduled' }).total,
  `PDF組立: 入荷予定の件数と一致 (${fake.rows}件)`);
delete process.env.INBOUND_PDF_FAKE;

// 共通 mutex: CSV取得とPDF出力が交錯しない (Codex pdf-R1 Medium)
const { runExclusive } = await import('../apps/inbound-info/job-lock.js');
const order = [];
const slow = (label, ms) => runExclusive(async () => {
  order.push(`${label}:start`);
  await new Promise((r) => setTimeout(r, ms));
  order.push(`${label}:end`);
});
await Promise.all([slow('A', 30), slow('B', 1), runExclusive(() => { throw new Error('boom'); }).catch(() => {}), slow('C', 1)]);
ok(order.join(',') === 'A:start,A:end,B:start,B:end,C:start,C:end',
  `job-lock: 直列実行 + 失敗しても後続を塞がない (${order.join(',')})`);

// cron 張り替え: 不正な env 式では既存の予約を壊さない (Codex pdf-R1 Medium)
const { applyInboundInfoSchedule } = await import('../apps/inbound-info/sync-job.js');
const a1 = applyInboundInfoSchedule();
ok(a1.started, 'cron: 起動');
process.env.INBOUND_INFO_SYNC_CRON = 'これは cron 式ではない';
const a2 = applyInboundInfoSchedule();
ok(!a2.started === false && a2.reason === 'invalid_expr', 'cron: 不正な式なら既存の予約を維持 (started=true)');
delete process.env.INBOUND_INFO_SYNC_CRON;
// 何度張り替えても壊れない (destroy → schedule)
for (let i = 0; i < 5; i++) applyInboundInfoSchedule();
ok(applyInboundInfoSchedule().started, 'cron: 繰り返し張り替えても起動状態を保つ');
// 日次ジョブ: CSV取得が失敗した日は Drive のPDFを上書きしない (Codex pdf-R1 High)
// (このテスト環境には GOOGLE_SERVICE_ACCOUNT_KEY が無いので nefuda 取得は必ず失敗する)
const { runDailyJob } = await import('../apps/inbound-info/sync-job.js');
savePdfState({ ok: true, filename: '入荷予定リスト.pdf', rows: 99, bytes: 1, user: 'before' });
saveJobSettings({ time: '09:00', pdf_enabled: true }, 'tester');
process.env.INBOUND_PDF_FAKE = '1'; // ここまで来たら生成しないはずだが、万一来ても Drive を叩かせない
await runDailyJob();
const afterFail = pdfState();
ok(afterFail.ok === 0 && /取得に失敗したため、PDFは更新していません/.test(afterFail.error),
  'CSV取得失敗時: PDFを上書きせず理由を記録 (古い紙を刷らせない)');
ok(afterFail.row_count === null, 'CSV取得失敗時: 前回の成功件数を「成功」として残さない');
// PDF出力が無効なら状態も触らない
savePdfState({ ok: true, filename: 'x.pdf', rows: 5, bytes: 1, user: 'keep' });
saveJobSettings({ time: '09:00', pdf_enabled: false }, 'tester');
await runDailyJob();
ok(pdfState().saved_by === 'keep', 'PDF出力を無効にしていれば状態を上書きしない');
delete process.env.INBOUND_PDF_FAKE;
saveJobSettings({ time: '08:05', pdf_enabled: true }, 'tester');

// 最後は停止状態で終える (予約が残るとプロセスが終了しない = テストがハングする)
process.env.INBOUND_INFO_SYNC_ENABLED = 'false';
ok(applyInboundInfoSchedule().reason === 'disabled_by_env', 'cron: env=false で停止 (予約も破棄)');
delete process.env.INBOUND_INFO_SYNC_ENABLED;

// 実 PDF 生成 (python + reportlab がある環境のみ。無い場合はスキップ)
try {
  const real = await buildSchedulePdf({ timeoutMs: 60000 });
  ok(real.buffer.length > 1000 && real.buffer.subarray(0, 5).toString() === '%PDF-',
    `PDF実生成: reportlab で ${Math.round(real.buffer.length / 1024)}KB`);
} catch (e) {
  console.log(`  ⏭  PDF実生成はスキップ (${e.message.slice(0, 80)})`);
}

// ─── 10. いろは在庫化=「有り」の規則 (2026-07-28) ───
// いろは側で在庫化する商品は BF 倉庫の入庫作業が無いので、BCシール/直接ピックロケ/BF保管荷姿は
// '－' 固定。入数は値を残すが「入数未記入」には数えない (画面はグレーアウトで編集させない)。
console.log('\n[10] いろは在庫化=有り の規則');
const NA = '－';
const nowI = new Date().toISOString();
db.prepare(`
  INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ,
    直接ピックロケ保管, BF保管荷姿, いろは在庫化作業有無, source, created_at, updated_at)
  VALUES ('irohatest01', 'irohatest01', 'いろはテスト (旧データ)', NULL,
    'BCシール貼付必要', '直接ピックロケ', 'バラ', '有り', 'excel', ?, ?)
`).run(nowI, nowI);

// 入数未記入の集計・絞り込みから外れる (入数を記入できない行なので拾っても直せない)
const missAll = listInbound({ filter: 'missing', limit: 'all' });
ok(!missAll.rows.some((x) => x.code_key === 'irohatest01'),
  'いろは=有り の行は「入数未記入」に出ない (入数はグレーアウトで記入できないため)');
ok(stats().missing_irisu === missAll.total, 'stats.missing_irisu と filter=missing の件数が一致');

// 保存時: 画面から何を送られても3項目は '－' に倒す (API 直叩き対策)
const ih0 = getInbound('irohatest01');
let ih = updateInbound('irohatest01', {
  入数: '24', 入庫時BCシール貼りフラグ: 'BCシール貼付必要', 直接ピックロケ保管: '直接ピックロケ',
  BF保管荷姿: 'そのまま', いろは在庫化作業有無: '有り', memo: 'メモは残る',
}, 'tester', ih0.version);
ok(ih.ok && ih.row.入庫時BCシール貼りフラグ === NA && ih.row.直接ピックロケ保管 === NA && ih.row.BF保管荷姿 === NA,
  'いろは=有り で保存すると3項目は「－」に固定される');
ok(ih.row.入数 === 24 && ih.row.memo === 'メモは残る', 'いろは=有り でも入数と memo は保存される');

// 更新内容に いろは列 が含まれない場合も、現在値が 有り なら固定は続く
ih = updateInbound('irohatest01', { BF保管荷姿: '20L折りコン入替' }, 'tester', ih.row.version);
ok(ih.ok && ih.row.BF保管荷姿 === NA, 'いろは列を送らない更新でも、現在値が有りなら「－」のまま');

// 「状況による」「無し」は対象外 (通常どおり編集できる)
ih = updateInbound('irohatest01', { いろは在庫化作業有無: '状況による', BF保管荷姿: 'そのまま' }, 'tester', ih.row.version);
ok(ih.ok && ih.row.いろは在庫化作業有無 === '状況による' && ih.row.BF保管荷姿 === 'そのまま',
  '「状況による」は固定対象外 (3項目は通常どおり編集できる)');
ih = updateInbound('irohatest01', { いろは在庫化作業有無: '無し', BF保管荷姿: 'バラ' }, 'tester', ih.row.version);
ok(ih.ok && ih.row.BF保管荷姿 === 'バラ', '「無し」も固定対象外');

// 競合時 (version 不一致) は「－」への強制も含めて一切書き換わらない
// (先に いろは列 を SELECT していても、最終 UPDATE が version 条件付きなので取りこぼさない)
ih = updateInbound('irohatest01', { いろは在庫化作業有無: '有り', BF保管荷姿: 'そのまま' }, 'tester', getInbound('irohatest01').version);
const beforeConflict = getInbound('irohatest01');
const cf = updateInbound('irohatest01', { BF保管荷姿: 'そのまま' }, 'userB', beforeConflict.version + 5);
const afterConflict = getInbound('irohatest01');
ok(!cf.ok && cf.error === 'conflict' && afterConflict.version === beforeConflict.version
  && ['入庫時BCシール貼りフラグ', '直接ピックロケ保管', 'BF保管荷姿', '入数', 'memo', 'updated_by']
       .every((f) => afterConflict[f] === beforeConflict[f]),
  'version 不一致なら「－」への強制も含めて何も書き換えない (conflict)');

// Excel 再取込 (移行ツール経路) でも同じ規則を通す
importInboundRows([{ 商品コード: 'irohatest01', 商品名: 'いろはテスト', 入数: 12,
  入庫時BCシール貼りフラグ: '不要', 直接ピックロケ保管: '直接ピックロケ', BF保管荷姿: 'バラ',
  いろは在庫化作業有無: '有り' }], { user: 'tester' });
const ihImp = getInbound('irohatest01');
ok(ihImp.入庫時BCシール貼りフラグ === NA && ihImp.直接ピックロケ保管 === NA && ihImp.BF保管荷姿 === NA && ihImp.入数 === 12,
  'Excel取込でも いろは=有り なら3項目は「－」(入数は取り込む)');

// 既存データの backfill (initMirrorDB 内の冪等 UPDATE)。旧データを直接書き戻して再初期化する
db.prepare(`
  UPDATE f_inbound_info
     SET 入庫時BCシール貼りフラグ = 'BCシール貼付必要', 直接ピックロケ保管 = '直接ピックロケ',
         BF保管荷姿 = 'バラ', いろは在庫化作業有無 = '有り'
   WHERE code_key = 'irohatest01'
`).run();
const beforeBf = getInbound('irohatest01');
initMirrorDB(); // ← 起動時と同じ経路 (createInboundInfoTables の backfill が走る)
const afterBf = getInbound('irohatest01');
ok(afterBf.入庫時BCシール貼りフラグ === NA && afterBf.直接ピックロケ保管 === NA && afterBf.BF保管荷姿 === NA,
  '既存データ: 起動時の backfill で「－」に揃う');
ok(afterBf.version === beforeBf.version + 1 && afterBf.updated_by === 'system(iroha-rule)',
  'backfill: version を進めて更新者を残す (開いたままの画面は conflict で気付ける)');
initMirrorDB();
ok(getInbound('irohatest01').version === afterBf.version, 'backfill: 2回目以降は 0 件更新 (冪等)');

// 表記ゆれ「　有り　」(全角スペース付き) も同じ扱いにする。SQLite の素の trim() は全角空白を
// 落とさないため、JS の .trim() と判定がズレて backfill / 入数未記入 だけすり抜ける (Codex R2 Medium)
const nowW = new Date().toISOString();
getMirrorDB().prepare(`
  INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ,
    直接ピックロケ保管, BF保管荷姿, いろは在庫化作業有無, source, created_at, updated_at)
  VALUES ('irohaws01', 'irohaws01', '全角スペース付き 有り', NULL, 'BCシール貼付必要',
    '直接ピックロケ', 'バラ', '　有り　', 'excel', ?, ?)
`).run(nowW, nowW);
// NBSP (U+00A0) 付きも同様 (JS の .trim() は落とすので SQL 側と判定を揃えている)
getMirrorDB().prepare(`
  INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ,
    直接ピックロケ保管, BF保管荷姿, いろは在庫化作業有無, source, created_at, updated_at)
  VALUES ('irohaws02', 'irohaws02', 'NBSP付き 有り', NULL, 'BCシール貼付必要',
    '直接ピックロケ', 'バラ', ' 有り ', 'excel', ?, ?)
`).run(nowW, nowW);
const missWs = listInbound({ filter: 'missing', limit: 'all' }).rows.map((x) => x.code_key);
ok(!missWs.includes('irohaws01') && !missWs.includes('irohaws02'),
  '「　有り　」(全角スペース) / NBSP 付きも「入数未記入」から外れる');
const db3 = initMirrorDB();
const wsRows = ['irohaws01', 'irohaws02'].map(getInbound);
ok(wsRows.every((w) => w.入庫時BCシール貼りフラグ === NA && w.直接ピックロケ保管 === NA && w.BF保管荷姿 === NA),
  '「　有り　」(全角スペース) / NBSP 付きも backfill の対象になる');
// 上書き前の値は控えてある (戻せる形にしておく)。db2 は initMirrorDB() の再入で閉じているので db3 を使う
const bk = db3.prepare("SELECT * FROM f_inbound_info_iroha_backup WHERE code_key = 'irohatest01'").get();
ok(bk && bk.入庫時BCシール貼りフラグ === 'BCシール貼付必要' && bk.直接ピックロケ保管 === '直接ピックロケ'
  && bk.BF保管荷姿 === 'バラ' && bk.backed_up_at,
  'backfill: 上書き前の値を f_inbound_info_iroha_backup に控えている');
for (const t of ['f_inbound_info', 'f_inbound_info_iroha_backup']) {
  db3.prepare(`DELETE FROM ${t} WHERE code_key IN ('irohatest01', 'irohaws01', 'irohaws02')`).run();
}

// ─── 11. 値札印刷用CSV (nefudaprint.csv) — 旧 GAS processNefudaCsv の置き換え (2026-07-28) ───
// nefuda.csv (4列) + この画面の入数 → 6列 (商品ID/商品名/JAN/FNSKU/有効期限/入数)、Shift_JIS。
// 現場の値札印刷ソフトが読むファイルなので、書式は旧 GAS と1バイトも変えない。
console.log('\n[11] 値札印刷用CSV (nefudaprint.csv)');
const { buildNefudaPrintCsv, OUT_HEADER, NEFUDA_PRINT_FILENAME } = await import('../apps/inbound-info/nefuda-print.js');
const { regenerateNefudaPrint } = await import('../apps/inbound-info/nefuda-fetch.js');
const { irisuMap, nefudaPrintState, saveNefudaPrintState } = await import('../apps/inbound-info/db.js');

const npDb = getMirrorDB();
const nowN = new Date().toISOString();
const insNp = npDb.prepare(`
  INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, source, created_at, updated_at)
  VALUES (?, ?, ?, ?, 'excel', ?, ?)
  ON CONFLICT(code_key) DO UPDATE SET 入数 = excluded.入数, 商品名 = excluded.商品名
`);
insNp.run('npjan01', 'npJAN01', 'JAN商品', 24, nowN, nowN);       // JAN + 入数あり
insNp.run('npfnsku01', 'npFNSKU01', 'FNSKU商品', 6, nowN, nowN);  // FNSKU + 入数あり
insNp.run('npnoirisu01', 'npNoIrisu01', '入数未記入', null, nowN, nowN);

const npRows = [
  { 商品コード: 'npJAN01', 商品名: 'JAN商品', バーコード: '4532416200201', 有効期限: '' },
  { 商品コード: 'npFNSKU01', 商品名: 'FNSKU商品', バーコード: 'X001976RMN', 有効期限: '2027-01-31' },
  { 商品コード: 'npNoIrisu01', 商品名: '入数未記入', バーコード: '4900000000001', 有効期限: '' },
  { 商品コード: 'npGhost01', 商品名: 'マスタに無い商品', バーコード: '', 有効期限: '' },
  // 大文字小文字違いでも入数が引ける (code_key 照合) / 引用符が要る商品名
  { 商品コード: 'NPJAN01', 商品名: 'カンマ, と "引用符" を含む商品', バーコード: '4900000000002', 有効期限: '' },
];
const np = buildNefudaPrintCsv(npRows);
const npText = iconv.decode(np.buffer, 'Shift_JIS');
const npLines = npText.split('\r\n');

ok(npLines[0] === OUT_HEADER.join(','), `ヘッダは6列 (${npLines[0]})`);
ok(npLines[1] === 'npJAN01,JAN商品,4532416200201,,,24', `数字だけのバーコードは JAN 列 + 入数を補完 (${npLines[1]})`);
ok(npLines[2] === 'npFNSKU01,FNSKU商品,,X001976RMN,2027-01-31,6', `英字を含むバーコードは FNSKU 列 (${npLines[2]})`);
ok(npLines[3] === 'npNoIrisu01,入数未記入,4900000000001,,,', '入数未記入なら空欄 (旧GASと同じ)');
ok(npLines[4] === 'npGhost01,マスタに無い商品,,,,', 'マスタに無いコード / バーコード空は該当列を空に');
ok(npLines[5] === 'NPJAN01,"カンマ, と ""引用符"" を含む商品",4900000000002,,,24',
  '大文字小文字違いでも入数を引ける + カンマ/引用符のエスケープ');
ok(np.rows === 5 && np.missingIrisu === 2, `件数と入数が空の件数 (rows=${np.rows}, missing=${np.missingIrisu})`);
ok(!npText.endsWith('\r\n') && npLines.length === 6, '末尾に余分な改行を付けない (旧GASと同じ)');
ok(np.buffer.includes(Buffer.from('JAN商品', 'utf8')) === false
  && iconv.decode(np.buffer, 'Shift_JIS').includes('JAN商品'), '出力は Shift_JIS (UTF-8 ではない)');

// 旧 GAS のロジックをそのまま書き下したものと1バイトも違わないこと (移植の等価性)
function gasReference(rows, master) {
  const esc = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const out = [['商品ID', '商品名', 'JAN', 'FNSKU', '有効期限', '入数']];
  for (const r of rows) {
    const id = String(r.商品コード ?? '').trim();
    const barcode = String(r.バーコード ?? '').trim();
    let irisu = '';
    if (id) irisu = master.has(id) ? master.get(id) : '';
    let jan = '', fnsku = '';
    if (/[A-Za-z]/.test(barcode)) fnsku = barcode;
    else if (/^[0-9]+$/.test(barcode)) jan = barcode;
    out.push([id, r.商品名 ?? '', jan, fnsku, r.有効期限 ?? '', irisu]);
  }
  return out.map((r) => r.map(esc).join(',')).join('\r\n');
}
// 旧 GAS の master は「シートの商品コードそのまま」で突合していたので、同じ表記で用意する
const gasMaster = new Map([['npJAN01', 24], ['npFNSKU01', 6], ['NPJAN01', 24]]);
ok(gasReference(npRows, gasMaster) === npText, '旧 GAS processNefudaCsv の出力と完全一致 (移植の等価性)');

// 入数を画面で直すと次の出力に反映される (= 入数の正がスプレッドシートからこの画面に移った)
const npTarget = getInbound('npjan01');
updateInbound('npjan01', { 入数: '48' }, 'tester', npTarget.version);
ok(iconv.decode(buildNefudaPrintCsv([npRows[0]]).buffer, 'Shift_JIS').endsWith(',48'),
  '画面で入数を直すと値札印刷用CSVに反映される');
ok(irisuMap().get('npjan01') === 48, 'irisuMap は code_key で引ける');

// Excel で数式扱いされ得る先頭文字は「数えるだけ」(旧GASとのバイト等価性を壊さない — Codex R1 Medium)
const npFormula = buildNefudaPrintCsv([
  { 商品コード: 'npJAN01', 商品名: '=HYPERLINK("x")', バーコード: '1', 有効期限: '' },
  { 商品コード: 'npJAN01', 商品名: '+81 対応品', バーコード: '1', 有効期限: '' },
  { 商品コード: 'npJAN01', 商品名: '普通の商品名', バーコード: '1', 有効期限: '' },
]);
ok(npFormula.formulaCells === 2, `数式に見えるセルを数える (${npFormula.formulaCells}個)`);
ok(iconv.decode(npFormula.buffer, 'Shift_JIS').includes('=HYPERLINK("x")') === false
  && iconv.decode(npFormula.buffer, 'Shift_JIS').includes('"=HYPERLINK(""x"")"'),
  '数式に見える値も書き換えない (引用符のエスケープだけ)');

// nefuda.csv が取れない時は Drive を触らず失敗を記録する (既存の値札CSVを壊さない)
saveNefudaPrintState({ ok: true, filename: 'nefudaprint.csv', rows: 1, bytes: 1, user: 'keep-before' });
let npThrew = false;
try {
  await regenerateNefudaPrint('tester'); // この環境は GOOGLE_SERVICE_ACCOUNT_KEY 無し
} catch (e) {
  npThrew = true;
}
ok(npThrew && nefudaPrintState().ok === 0 && nefudaPrintState().saved_by === 'tester',
  '「今すぐ作成」で nefuda.csv が取れない時は失敗を記録して Drive に触らない');

// 保存状態の記録 (成功/失敗どちらも上書き。画面の「最終保存」表示用)
saveNefudaPrintState({ ok: false, error: 'テスト失敗', filename: NEFUDA_PRINT_FILENAME(), user: 'tester' });
ok(nefudaPrintState().ok === 0 && nefudaPrintState().error === 'テスト失敗', '値札CSV状態: 失敗を記録');
saveNefudaPrintState({ ok: true, filename: 'nefudaprint.csv', rows: 5, missing_irisu: 2, bytes: 300,
  file_id: 'fid', folder_name: 'F', user: 'tester' });
const nps = nefudaPrintState();
ok(nps.ok === 1 && nps.row_count === 5 && nps.missing_irisu === 2 && nps.error === null,
  '値札CSV状態: 成功で上書き (前回のエラーは消える)');

// 設定: 既定ON / 保存と読み出し
ok(getJobSettings().nefuda_print_enabled === true, '設定: 値札印刷用CSVは既定ON');
saveJobSettings({ time: '08:05', pdf_enabled: true, nefuda_print_enabled: false }, 'tester');
ok(getJobSettings().nefuda_print_enabled === false, '設定: 値札印刷用CSVをOFFにできる');
saveJobSettings({ time: '08:05', pdf_enabled: true, nefuda_print_enabled: true }, 'tester');

// 日次ジョブ: CSV取得が失敗した日は Drive の値札CSVを上書きしない (PDF と同じ扱い)
saveNefudaPrintState({ ok: true, filename: 'nefudaprint.csv', rows: 99, bytes: 1, user: 'before' });
await runDailyJob(); // この環境は GOOGLE_SERVICE_ACCOUNT_KEY 無し = nefuda 取得は必ず失敗する
const npAfterFail = nefudaPrintState();
ok(npAfterFail.ok === 0 && /取得に失敗したため、値札印刷用CSVは更新していません/.test(npAfterFail.error),
  'CSV取得失敗時: 値札CSVを上書きせず理由を記録');
ok(npAfterFail.row_count === null, 'CSV取得失敗時: 前回の成功件数を「成功」として残さない');
// 無効にしていれば状態も触らない
saveNefudaPrintState({ ok: true, filename: 'nefudaprint.csv', rows: 5, bytes: 1, user: 'keep' });
saveJobSettings({ time: '08:05', pdf_enabled: false, nefuda_print_enabled: false }, 'tester');
await runDailyJob();
ok(nefudaPrintState().saved_by === 'keep', '値札印刷用CSVを無効にしていれば状態を上書きしない');
saveJobSettings({ time: '08:05', pdf_enabled: true, nefuda_print_enabled: true }, 'tester');
for (const c of ['npjan01', 'npfnsku01', 'npnoirisu01']) deleteInbound(c);

console.log(`\n🎉 全 ${passed} 項目 PASS`);
