/** 組み合わせマスター (数量2以上・同梱) のテスト */
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

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'es-combo-'));
process.env.DATA_DIR = tmp;

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
const throws = (fn, pred, name) => {
  try {
    fn();
    ok(false, `${name} (エラーなし)`);
  } catch (e) {
    ok(pred(e), `${name} (${e.message})`);
  }
};

// --- 正規化キー ---
const a = svc.normalizeComboItems([
  { sku: 'Bbb-2', qty: 1 },
  { sku: 'aaa-1', qty: 2 },
]);
ok(a.comboKey === 'aaa-1*2|bbb-2*1', 'キーは小文字化+辞書順 (順序に依存しない)');
const b = svc.normalizeComboItems([
  { sku: 'AAA-1', qty: 2 },
  { sku: 'bbb-2', qty: 1 },
]);
ok(a.comboKey === b.comboKey, '並び・大小違いでも同じキーになる');
const merged = svc.normalizeComboItems([
  { sku: 'ccc-3', qty: 1 },
  { sku: 'CCC-3', qty: 2 },
]);
ok(merged.comboKey === 'ccc-3*3', '同一SKU (大小違い含む) は数量を合算する');
ok(merged.items.length === 1 && merged.items[0].qty === 3, 'items も合算される');
throws(() => svc.normalizeComboItems([]), (e) => e.code === 'VALIDATION_ERROR', '空配列は400');
throws(
  () => svc.normalizeComboItems([{ sku: 'x', qty: 0 }]),
  (e) => e.code === 'VALIDATION_ERROR',
  '数量0は400',
);
throws(
  () => svc.normalizeComboItems([{ sku: '', qty: 1 }]),
  (e) => e.code === 'VALIDATION_ERROR',
  '空SKUは400',
);

// --- 登録 (auto-register) ---
const reg = svc.autoRegisterCombo({
  items: [
    { sku: 'sionetsu168g-le-2', qty: 2 },
  ],
  packageSizeLabel: '60サイズ (26 cm x 19 cm x 11 cm)',
  amazonOptionValue: '84797239-91e6-4101-a8b8-9b86c45482e7',
});
ok(reg.created === true && reg.item.comboKey === 'sionetsu168g-le-2*2', '組み合わせを登録できる');

// 同一構成 (大小・順序違い) は上書きしない
const dup = svc.autoRegisterCombo({
  items: [{ sku: 'SIONETSU168G-LE-2', qty: 2 }],
  packageSizeLabel: '100サイズ (35 cm x 35 cm x 28 cm)',
  amazonOptionValue: 'other-uuid',
});
ok(dup.created === false && dup.reason === 'already_exists', '既存の同一構成は上書きしない');
ok(
  db.prepare("SELECT package_size_label l FROM es_combo_size_master WHERE combo_key='sionetsu168g-le-2*2'").get().l ===
    '60サイズ (26 cm x 19 cm x 11 cm)',
  '既存の内容が保持されている',
);

throws(
  () => svc.autoRegisterCombo({ items: [{ sku: 'x', qty: 1 }], packageSizeLabel: '', amazonOptionValue: 'v' }),
  (e) => e.code === 'VALIDATION_ERROR',
  'ラベル空は400',
);
throws(
  () => svc.autoRegisterCombo({ items: [{ sku: 'x', qty: 1 }], packageSizeLabel: '=SUM(A1)', amazonOptionValue: 'v' }),
  (e) => e.code === 'VALIDATION_ERROR',
  '数式接頭辞のラベルは登録拒否',
);

// --- 一括照会 ---
svc.autoRegisterCombo({
  items: [
    { sku: 'aroma-a', qty: 1 },
    { sku: 'aroma-b', qty: 1 },
  ],
  packageSizeLabel: 'メール便サイズnew (34 cm x 21 cm x 3 cm)',
  amazonOptionValue: 'ebe77864-285b-41f9-83f1-b57e3d3831d5',
});

const lookup = svc.comboBulkLookup([
  { items: [{ sku: 'SIONETSU168G-LE-2', qty: 2 }] }, // 大小違いでも一致
  { items: [{ sku: 'aroma-b', qty: 1 }, { sku: 'AROMA-A', qty: 1 }] }, // 順序違いでも一致
  { items: [{ sku: 'unknown-x', qty: 3 }] },
  { items: [{ sku: 'sionetsu168g-le-2', qty: 3 }] }, // 数量違いは別構成
  { items: [{ sku: '', qty: 1 }] }, // 不正
]);
ok(lookup.results.length === 5, 'リクエストと同じ件数のresultsが返る');
ok(
  lookup.results[0].status === 'found' &&
    lookup.results[0].amazonOptionValue === '84797239-91e6-4101-a8b8-9b86c45482e7',
  '大小違いでも一致してサイズが返る',
);
ok(lookup.results[1].status === 'found', '順序違いでも一致する');
ok(lookup.results[2].status === 'notFound', '未登録構成は notFound');
ok(lookup.results[3].status === 'notFound', '数量が違えば別構成 (完全一致のみ)');
ok(lookup.results[4].status === 'invalid', '不正な構成は invalid (他の照会は継続)');

// --- 無効化・一覧 ---
const listed = svc.listCombos({ q: 'sionetsu' });
ok(listed.total === 1 && listed.items[0].items[0].sku === 'sionetsu168g-le-2', '一覧をSKUで検索できる');

const toggled = svc.removeCombo(listed.items[0].id, false, 'admin@test');
ok(toggled.isActive === false, '無効化できる');
ok(
  svc.comboBulkLookup([{ items: [{ sku: 'sionetsu168g-le-2', qty: 2 }] }]).results[0].status === 'inactive',
  '無効化された構成は inactive',
);
const reToggled = svc.removeCombo(listed.items[0].id, false, 'admin@test');
ok(reToggled.isActive === true, '有効化に戻せる');

svc.removeCombo(listed.items[0].id, true, 'admin@test');
ok(svc.listCombos({ q: 'sionetsu' }).total === 0, '物理削除できる');

const adminLog = db
  .prepare("SELECT COUNT(*) c FROM es_operation_logs WHERE action LIKE 'admin_combo_%'")
  .get().c;
ok(adminLog >= 3, '管理操作がログされる');

console.log(`\n${pass} pass / ${fail} fail`);
process.exit(fail ? 1 : 0);
