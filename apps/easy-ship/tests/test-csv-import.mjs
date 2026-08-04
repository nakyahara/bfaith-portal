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

// 大小違いのSKUは同一SKUとして扱われ、更新になる (常に大小無視)
const caseRes = svc.importCsv(`${HEADER}\nold-001,SIZE_80,80サイズ,80,true,\n`, 'preview', false, 'a');
ok(
  caseRes.errors === 0 && caseRes.updated === 1,
  '大小違いの既存SKUは同一SKUとして更新扱いになる',
);

// ヘッダー不正・構文不正
throws(() => svc.importCsv('sku,note\nX,1\n', 'preview', false, 'a'), (e) => e.code === 'INVALID_CSV', '必須列欠落は400');
throws(
  () => svc.importCsv(`${HEADER}\n"UNCLOSED,SIZE_60,60サイズ,60,true,`, 'preview', false, 'a'),
  (e) => e.code === 'INVALID_CSV' && e.message.includes('行目'),
  '構文不正は行番号付き400',
);

// --- CSVインジェクション対策 (入力時点で拒否) ---
throws(
  () => svc.createMaster({ sku: '=cmd', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a'),
  (e) => e.code === 'VALIDATION_ERROR' && e.message.includes('数式'),
  '=で始まるSKUは登録拒否',
);
throws(
  () => svc.createMaster({ sku: 'OK-1', packageSizeCode: 'X', packageSizeLabel: 'Y', note: '  +1+cmd' }, 'a'),
  (e) => e.code === 'VALIDATION_ERROR',
  '空白を挟んで+で始まる備考も登録拒否',
);
const injImport = svc.importCsv(`${HEADER}\n@RISKY,SIZE_60,60サイズ,60,true,\n`, 'preview', false, 'a');
ok(injImport.errors === 1, 'CSVインポート経由でも数式接頭辞は行エラー');

// --- エクスポート ---
// 旧データ等でDBに危険な値が入っていても、Excel用は無害化されること (直接INSERTで再現)
db.prepare(
  `INSERT INTO es_package_size_master (sku, package_size_code, package_size_label, amazon_option_value, is_active, note, created_at, updated_at)
   VALUES ('EXP-001', 'SIZE_60', '60サイズ', '60', 1, '=SUM(A1)', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
).run();
const raw = svc.exportCsv(false);
ok(raw.includes('EXP-001,SIZE_60,60サイズ,60,true,=SUM(A1)'), '再取込用エクスポートはデータそのまま');
const excel = svc.exportCsv(true);
ok(excel.includes("'=SUM(A1)") && !excel.includes(',=SUM(A1)'), 'Excel用は数式インジェクション対策の前置');
ok(raw.charCodeAt(0) === 0xfeff, 'エクスポートはBOM付き (Excel文字化け防止)');

console.log(`\n${pass} pass / ${fail} fail`);
process.exit(fail ? 1 : 0);
