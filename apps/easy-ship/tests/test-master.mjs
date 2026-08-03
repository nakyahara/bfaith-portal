/** マスターCRUD・照合・操作ログのテスト */
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

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'es-master-'));
process.env.DATA_DIR = tmp; // DB初期化より前に必ず設定
delete process.env.EASY_SHIP_SKU_CASE_INSENSITIVE;
delete process.env.EASY_SHIP_ALLOW_ORDER_ID_LOGGING;

const dbMod = await import(pathToFileURL(path.join(repo, 'apps/easy-ship/db.js')));
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
function expectErr(fn, status, code, name) {
  try {
    fn();
    ok(false, `${name} (エラーが投げられなかった)`);
  } catch (e) {
    ok(e instanceof svc.EsError && e.status === status && e.code === code, `${name} (${e.code ?? e.message})`);
  }
}

// --- CRUD ---
const created = svc.createMaster(
  {
    sku: '  nanairosks-2new  ',
    packageSizeCode: 'MAIL_NEW',
    packageSizeLabel: 'メール便サイズnew (34 cm x 21 cm x 3 cm)',
    amazonOptionValue: 'ebe77864-285b-41f9-83f1-b57e3d3831d5',
    note: '',
  },
  'admin@test',
);
ok(created.sku === 'nanairosks-2new', 'SKUの前後空白は保存前に除去される');
ok(created.isActive === true && created.note === null, 'isActive既定true・空noteはnull');

expectErr(
  () => svc.createMaster({ sku: '  ', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a'),
  400,
  'VALIDATION_ERROR',
  '空SKUは400',
);
expectErr(
  () => svc.createMaster({ sku: 'nanairosks-2new', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a'),
  409,
  'DUPLICATE_SKU',
  '同一SKUは409',
);
expectErr(
  () => svc.createMaster({ sku: 'NANAIROSKS-2NEW', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a'),
  409,
  'DUPLICATE_SKU',
  '大小違いSKUも409 (DBのLOWER一意に合わせる)',
);

// DB制約そのものの確認 (事前チェックをすり抜けた場合の最終防衛)
let constraintCaught = false;
try {
  db.prepare(
    `INSERT INTO es_package_size_master (sku, package_size_code, package_size_label, amazon_option_value, is_active, note, created_at, updated_at)
     VALUES ('Nanairosks-2NEW', 'X', 'Y', '', 1, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
  ).run();
} catch (e) {
  constraintCaught = String(e.code).startsWith('SQLITE_CONSTRAINT');
}
ok(constraintCaught, 'LOWER(sku)一意インデックスが直接INSERTも拒否する');

const updated = svc.updateMaster(
  created.id,
  {
    sku: 'nanairosks-2new',
    packageSizeCode: 'SIZE_60',
    packageSizeLabel: '60サイズ (26 cm x 19 cm x 11 cm)',
    amazonOptionValue: '84797239-91e6-4101-a8b8-9b86c45482e7',
    isActive: true,
  },
  'admin@test',
);
ok(updated.packageSizeCode === 'SIZE_60', '更新が反映される');
expectErr(() => svc.updateMaster(9999, { sku: 'x', packageSizeCode: 'y', packageSizeLabel: 'z' }, 'a'), 404, 'NOT_FOUND', '存在しないIDの更新は404');

// --- 照会 ---
const hit = svc.lookupOne('nanairosks-2new');
ok(hit.amazonOptionValue === '84797239-91e6-4101-a8b8-9b86c45482e7', 'lookupOneが登録値を返す');
ok(svc.lookupOne(' nanairosks-2new ').sku === 'nanairosks-2new', '前後空白付きでも照合できる');
expectErr(() => svc.lookupOne('NANAIROSKS-2NEW'), 404, 'SKU_NOT_FOUND', '既定 (大小区別) では大小違いは未登録扱い');
expectErr(() => svc.lookupOne('nope-999'), 404, 'SKU_NOT_FOUND', '未登録SKUはSKU_NOT_FOUND');

process.env.EASY_SHIP_SKU_CASE_INSENSITIVE = '1';
ok(svc.lookupOne('NANAIROSKS-2NEW').sku === 'nanairosks-2new', 'EASY_SHIP_SKU_CASE_INSENSITIVE=1 なら大小無視で一致');
delete process.env.EASY_SHIP_SKU_CASE_INSENSITIVE;

const dead = svc.createMaster({ sku: 'dead-001', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a');
svc.removeMaster(dead.id, false, 'a');
expectErr(() => svc.lookupOne('dead-001'), 404, 'SKU_INACTIVE', '無効化SKUはSKU_INACTIVE');

const bulk = svc.bulkLookup(['nanairosks-2new', 'nope-999', 'dead-001', 'nanairosks-2new', ' ']);
ok(bulk.found.length === 1 && bulk.found[0].sku === 'nanairosks-2new', 'bulkLookup: found (重複・空は除外)');
ok(bulk.notFound.length === 1 && bulk.notFound[0] === 'nope-999', 'bulkLookup: notFound');
ok(bulk.inactive.length === 1 && bulk.inactive[0] === 'dead-001', 'bulkLookup: inactive');
expectErr(() => svc.bulkLookup([]), 400, 'VALIDATION_ERROR', 'bulkLookup: 空配列は400');

// --- 無効化・削除 ---
const offRow = svc.listMaster({ q: 'dead-001' }).items[0];
ok(offRow.isActive === false, '無効化がis_activeに反映される');
svc.removeMaster(dead.id, true, 'a');
ok(svc.listMaster({ q: 'dead-001' }).total === 0, '物理削除で消える');

// --- 一覧 ---
svc.createMaster({ sku: 'aaa-001', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a');
svc.createMaster({ sku: 'aaa-002', packageSizeCode: 'X', packageSizeLabel: 'Y', isActive: false }, 'a');
svc.createMaster({ sku: 'bbb-001', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a');
ok(svc.listMaster({ q: 'AAA' }).total === 2, '一覧の検索は大小無視の部分一致');
// LIKE特殊文字は literal として扱う
svc.createMaster({ sku: 'esc_1', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a');
svc.createMaster({ sku: 'escX1', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a');
svc.createMaster({ sku: 'esc%2', packageSizeCode: 'X', packageSizeLabel: 'Y' }, 'a');
ok(svc.listMaster({ q: 'esc_1' }).total === 1, "SKU検索の '_' はワイルドカードにならない");
ok(svc.listMaster({ q: 'esc%' }).total === 1, "SKU検索の '%' はワイルドカードにならない");
ok(svc.listMaster({ q: 'esc\\' }).total === 0, "SKU検索の '\\' はエスケープとして誤爆しない");
ok(svc.listMaster({ q: 'aaa', active: 'true' }).total === 1, '有効フィルター');
const paged = svc.listMaster({ perPage: '2', page: '2', sort: 'sku', order: 'asc' });
ok(paged.items.length === 2 && paged.items[0].sku === 'bbb-001', 'ページネーション+SKU順');

// --- 商品名の付与 (mirror_products) ---
// mirror未初期化の間は fail-soft で productName=null (一覧自体は成立する)
ok(
  svc.listMaster({ q: 'aaa-001' }).items[0].productName === null,
  'mirror未初期化でも一覧が返り productName は null',
);
{
  const mirrorMod = await import(pathToFileURL(path.join(repo, 'apps/warehouse-mirror/db.js')));
  mirrorMod.initMirrorDB();
  mirrorMod
    .getMirrorDB()
    .prepare(
      'INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価状態, updated_at) VALUES (?, ?, ?, ?, ?)',
    )
    .run('AAA-001', '七色 お風呂のせっけん 柿渋', '通常', '確定', '2026-08-04T00:00:00Z');
  ok(
    svc.listMaster({ q: 'aaa-001' }).items[0].productName === '七色 お風呂のせっけん 柿渋',
    '商品名が mirror_products から付与される (コードの大小文字差も吸収)',
  );
  ok(svc.listMaster({ q: 'bbb-001' }).items[0].productName === null, 'mirrorに無いSKUは productName null');
  const byName = svc.listMaster({ q: 'せっけん' });
  ok(
    byName.total === 1 && byName.items[0].sku === 'aaa-001',
    '商品名の部分一致でも検索できる',
  );
  ok(svc.listMaster({ q: '存在しない商品名zzz' }).total === 0, '商品名も一致しなければ0件');
}

// --- 拡張ログ ---
svc.addExtLogs([
  {
    sku: 'aaa-001',
    action: 'autofill',
    result: 'success',
    message: '60サイズを選択',
    browserIdentifier: 'uuid-1',
    pageUrl: 'https://sellercentral.amazon.co.jp/easyship/bulkscheduling?orderIds=SECRET#f',
    orderId: '250-1234567-1234567',
  },
]);
let log = db.prepare('SELECT * FROM es_operation_logs WHERE action = ? ORDER BY id DESC').get('autofill');
ok(log.page_url === 'https://sellercentral.amazon.co.jp/easyship/bulkscheduling', 'pageUrlはクエリ・フラグメント除去');
ok(!String(log.message).includes('250-1234567'), '注文番号は既定で破棄される');

process.env.EASY_SHIP_ALLOW_ORDER_ID_LOGGING = '1';
svc.addExtLogs([{ action: 'autofill', result: 'success', orderId: '250-1234567-1234567' }]);
log = db.prepare('SELECT * FROM es_operation_logs WHERE action = ? ORDER BY id DESC').get('autofill');
ok(String(log.message).includes('250-1234567-1234567'), 'EASY_SHIP_ALLOW_ORDER_ID_LOGGING=1 なら注文番号を保存');
delete process.env.EASY_SHIP_ALLOW_ORDER_ID_LOGGING;

const adminLog = db.prepare("SELECT * FROM es_operation_logs WHERE action = 'admin_create' LIMIT 1").get();
ok(adminLog && adminLog.user_identifier === 'admin@test', '管理操作がuser_identifier付きでログされる');

console.log(`\n${pass} pass / ${fail} fail`);
process.exit(fail ? 1 : 0);
