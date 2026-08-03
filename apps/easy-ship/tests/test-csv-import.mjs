/** CSVパーサ・インポート・エクスポートのテスト */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

const repo =
  process.argv[2] ||
  path.resolve(
    path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')),
    '../../..',
  );

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'es-csv-'));
process.env.DATA_DIR = tmp;
delete process.env.EASY_SHIP_SKU_CASE_INSENSITIVE;

const dbMod = await import(pathToFileURL(path.join(repo, 'apps/easy-ship/db.js')));
const csv = await import(pathToFileURL(path.join(repo, 'apps/easy-ship/csv.js')));
const svc = await import(pathToFileURL(path.join(repo, 'apps/easy-ship/service.js')));
dbMod.initEasyShipDB();
const db = dbMod.getDB();

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) {
    pass++;
    console.log(`  ok - ${n}`);
  } else {
    fail++;
    console.log(`  NG - ${n}`);
  }
};
const throws = (fn, pred, name) => {
  try {
    fn();
    ok(false, `${name} (エラーなし)`);
  } catch (e) {
    ok(pred(e), `${name} (${e.message})`);
  }
};

const HEADER = 'sku,package_size_code,package_size_label,amazon_option_value,is_active,note';

// --- パーサ厳格性 ---
ok(
  JSON.stringify(csv.parseCsv('a,"b,1","c""x"\r\n"multi\nline",2,3')) ===
    JSON.stringify([['a', 'b,1', 'c"x'], ['multi\nline', '2', '3']]),
  'クォート・エスケープ・改行入りフィールド',
);
throws(() => csv.parseCsv('a,b\n"unclosed,x\n'), (e) => e instanceof csv.CsvParseError, '閉じ忘れはエラー');
throws(() => csv.parseCsv('a,b"c\n'), (e) => e instanceof csv.CsvParseError, 'フィールド途中の引用符はエラー');
throws(() => csv.parseCsv('"abc"x,y\n'), (e) => e instanceof csv.CsvParseError, '閉じ引用符後の余分な文字はエラー');
ok(csv.sanitizeExcelCell('=SUM(A1)') === "'=SUM(A1)" && csv.sanitizeExcelCell('ABC') === 'ABC', 'sanitizeExcelCell');

// --- インポート ---
const previewRes = svc.importCsv(`${HEADER}\nNEW-001,SIZE_60,60サイズ (26 cm x 19 cm x 11 cm),uuid-60,true,\n`, 'preview', false, 'a');
ok(previewRes.created === 1 && previewRes.applied === false, 'previewは件数だけ返す');
ok(db.prepare('SELECT COUNT(*) c FROM es_package_size_master').get().c === 0, 'previewはDBを変更しない');

svc.createMaster({ sku: 'OLD-001', packageSizeCode: 'SIZE_60', packageSizeLabel: '60サイズ', amazonOptionValue: '60' }, 'a');
svc.createMaster({ sku: 'SAME-001', packageSizeCode: 'SIZE_60', packageSizeLabel: '60サイズ', amazonOptionValue: '60' }, 'a');
const commitRes = svc.importCsv(
  [
    HEADER,
    'NEW-001,SIZE_60,60サイズ,60,true,',
    'OLD-001,SIZE_100,100サイズ,100,true,',
    'SAME-001,SIZE_60,60サイズ,60,true,',
  ].join('\n'),
  'commit',
  false,
  'a',
);
ok(
  commitRes.created === 1 && commitRes.updated === 1 && commitRes.skipped === 1 && commitRes.applied === true,
  'commitで新規/更新/変更なしスキップ',
);
ok(
  db.prepare("SELECT package_size_label l FROM es_package_size_master WHERE sku='OLD-001'").get().l === '100サイズ',
  '更新が反映される',
);

// エラー行があると既定でブロック
const blockedRes = svc.importCsv(
  [HEADER, 'GOOD-002,SIZE_60,60サイズ,60,true,', 'BAD-001,SIZE_60,60サイズ,60,maybe,'].join('\n'),
  'commit',
  false,
  'a',
);
ok(blockedRes.blocked === true && blockedRes.applied === false, 'エラー行があればcommitは全件ブロック');
ok(db.prepare("SELECT COUNT(*) c FROM es_package_size_master WHERE sku='GOOD-002'").get().c === 0, 'ブロック時は正常行も未反映');

const partialRes = svc.importCsv(
  [HEADER, 'GOOD-002,SIZE_60,60サイズ,60,true,', 'BAD-001,SIZE_60,60サイズ,60,maybe,'].join('\n'),
  'commit',
  true,
  'a',
);
ok(partialRes.applied === true && partialRes.errors === 1, 'allowPartial=trueで正常行だけ反映');
ok(db.prepare("SELECT COUNT(*) c FROM es_package_size_master WHERE sku='GOOD-002'").get().c === 1, '正常行が反映される');

// ファイル内の大小違い重複
const dupRes = svc.importCsv(
  [HEADER, 'DUP-001,SIZE_60,60サイズ,60,true,', 'dup-001,SIZE_80,80サイズ,80,true,'].join('\n'),
  'preview',
  false,
  'a',
);
ok(dupRes.errors === 1 && dupRes.rows.some((r) => r.action === 'error' && r.message.includes('重複')), 'ファイル内の大小違い重複はエラー');

// 大小区別モードで既存SKUと大小違い衝突
const caseRes = svc.importCsv(`${HEADER}\nold-001,SIZE_80,80サイズ,80,true,\n`, 'preview', false, 'a');
ok(caseRes.errors === 1 && caseRes.rows[0].message.includes('大文字小文字違い'), '大小違いの既存SKUへの上書きはエラー');

// ヘッダー不正・構文不正
throws(() => svc.importCsv('sku,note\nX,1\n', 'preview', false, 'a'), (e) => e.code === 'INVALID_CSV', '必須列欠落は400');
throws(
  () => svc.importCsv(`${HEADER}\n"UNCLOSED,SIZE_60,60サイズ,60,true,`, 'preview', false, 'a'),
  (e) => e.code === 'INVALID_CSV' && e.message.includes('行目'),
  '構文不正は行番号付き400',
);

// --- エクスポート ---
svc.createMaster({ sku: 'EXP-001', packageSizeCode: 'SIZE_60', packageSizeLabel: '60サイズ', amazonOptionValue: '60', note: '=SUM(A1)' }, 'a');
const raw = svc.exportCsv(false);
ok(raw.includes('EXP-001,SIZE_60,60サイズ,60,true,=SUM(A1)'), '再取込用エクスポートはデータそのまま');
const excel = svc.exportCsv(true);
ok(excel.includes("'=SUM(A1)") && !excel.includes(',=SUM(A1)'), 'Excel用は数式インジェクション対策の前置');
ok(raw.charCodeAt(0) === 0xfeff, 'エクスポートはBOM付き (Excel文字化け防止)');

console.log(`\n${pass} pass / ${fail} fail`);
process.exit(fail ? 1 : 0);
