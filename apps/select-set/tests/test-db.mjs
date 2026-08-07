/**
 * DB層 (db.js) のテスト。一時ディレクトリに DATA_DIR を向けるので本番DBには触れない。
 * warehouse.db にも RMS にも依存しない。
 */
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

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ss-db-'));
process.env.DATA_DIR = tmp;

const db = await import(pathToFileURL(path.join(repo, 'apps/select-set/db.js')));
const seed = await import(pathToFileURL(path.join(repo, 'apps/select-set/seed-data.js')));

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) { pass++; console.log(`  ok - ${n}`); }
  else { fail++; console.log(`  NG - ${n}`); }
};

db.initSelectSetDB();

console.log('=== 初期シード ===');
const sets = db.listSets();
ok(sets.length === seed.SET_CODES.length, `セットが ${seed.SET_CODES.length} 件入る (実際 ${sets.length})`);
ok(sets.some((s) => s.set_code === 'selectae10-5'), 'selectae10-5 が入っている');

const seedTotal = Object.values(seed.MANUAL_MAPPINGS).reduce((a, r) => a + r.length, 0);
const maps = db.listMappings();
ok(maps.length === seedTotal, `手動マッピングが ${seedTotal} 件入る (実際 ${maps.length})`);
ok(db.listMappings('ganesh-select3').length === 39, 'ganesh は39件');
ok(db.listMappings('selecteo10ml3').some((m) => m.product_code === 'hakka-eucalyptus'),
  '文字数で切れたコードの別名が入っている');

const omake = db.listOmake();
ok(omake.length === 10, `おまけ優先順位が10件 (実際 ${omake.length})`);
ok(omake[0].product_code === 'nemune-s' && omake[9].product_code === 'richbathp-cf', '順位が原価の安い順で保存されている');

console.log('\n=== 冪等性 ===');
db.initSelectSetDB();
ok(db.listMappings().length === seedTotal, '2回初期化しても手動マッピングが増えない');
ok(db.listOmake().length === 10, '2回初期化してもおまけが増えない');

console.log('\n=== マッピングの追加・重複・削除 ===');
const id = db.upsertMapping({ setCode: 'selectae10-5', optionText: 'テスト香り_ae-rose10', productCode: 'ae-rose10' });
ok(!!id, '追加できる');
db.upsertMapping({ setCode: 'selectae10-5', optionText: '【テスト香り】ae-rose10', productCode: 'ae-plumeria10' });
const dup = db.listMappings('selectae10-5').filter((m) => /テスト香り/.test(m.option_text));
ok(dup.length === 1, '括弧・記号違いは同じ選択肢として1件にまとまる (正規化キーで一意)');
ok(dup[0].product_code === 'ae-plumeria10', '同じキーへの再登録は上書きになる');
db.deleteMapping(dup[0].id);
ok(!db.listMappings('selectae10-5').some((m) => /テスト香り/.test(m.option_text)), '削除できる');

console.log('\n=== おまけの並べ替え ===');
db.replaceOmake(['petirfleur-wr', 'nemune-s', 'nemune-g']);
const re = db.listOmake();
ok(re.length === 3 && re[0].product_code === 'petirfleur-wr' && re[0].rank === 1, '全置換で順位が振り直される');
db.replaceOmake(['a-1', 'a-1', 'b-2']);
ok(db.listOmake().length === 2, '重複した商品コードは1つにまとめる');

console.log('\n=== セット削除は手動マッピングも消す ===');
const before = db.listMappings('ganesh-select3').length;
ok(before === 39, '削除前は39件');
db.deleteSet('ganesh-select3');
ok(db.listMappings('ganesh-select3').length === 0, 'セットを消すと紐づく手動マッピングも消える');
ok(!db.listSets().some((s) => s.set_code === 'ganesh-select3'), 'セット自体も消える');

console.log(`\n合計: ${pass} pass / ${fail} NG`);
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {}
process.exit(fail === 0 ? 0 : 1);
