/**
 * ロジザード商品マスタ (エクスポート[FM08_01] 種類=商品 / パターン=デフォルト) 取込のテスト
 *
 * 実行: node scripts/test-inbound-check-product-master.mjs
 * 検証: 有効期限区分の読み方 / fail-closed (列欠落・列数不一致・0件・壊れたCP932) /
 *       手動設定の上書きと、その件数が結果に出ること / 一覧への反映
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import iconv from 'iconv-lite';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-master-'));
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { getDB, setExpiryManaged, productInfoMap } = await import('../apps/inbound-check/db.js');
const { parseProductMasterCsv, importProductMaster, isExpiryManagedValue, productMasterStatus } =
  await import('../apps/inbound-check/product-master.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const throwsWith = (fn, re, l) => { try { fn(); ok(false, `${l} (例外なし)`); } catch (e) { ok(re.test(e.message), `${l}: ${e.message}`); } };

const HEADER = ['商品ID', '商品名', 'バーコード', '有効期限区分', '備考'];
const csv = (rows, { header = HEADER } = {}) => {
  const q = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  return iconv.encode([header.map(q).join(','), ...rows.map(r => r.map(q).join(','))].join('\r\n') + '\r\n', 'cp932');
};

console.log('DATA_DIR =', process.env.DATA_DIR);
getDB();   // 表を作る

console.log('\n[1] 有効期限区分の読み方');
{
  // 「管理しない」と読める値だけを false にし、それ以外は true に倒す。
  // 迷う値を false にすると期限を聞かずに通してしまうため (fail-safe)
  for (const v of ['', '0', 'なし', '無し', '無', 'しない', '管理しない', '対象外', '-', '－', ' 0 ']) {
    ok(isExpiryManagedValue(v) === false, `「${v}」= 期限管理なし`);
  }
  for (const v of ['1', '2', 'あり', '有り', '賞味期限', '消費期限', '製造日']) {
    ok(isExpiryManagedValue(v) === true, `「${v}」= 期限管理あり`);
  }
}

console.log('\n[2] fail-closed (壊れたファイルで既存の設定を壊さない)');
{
  throwsWith(() => parseProductMasterCsv(Buffer.alloc(0)), /空/, '空ファイルを拒否');
  throwsWith(() => parseProductMasterCsv(csv([['a', 'A', '', '1', '']], { header: ['商品ID', '商品名', 'バーコード', '備考'] })),
    /有効期限区分/, '有効期限区分が無ければ拒否 (列名が変わったのを黙って通さない)');
  throwsWith(() => parseProductMasterCsv(csv([['a', 'A', '', '1', '']], { header: ['商品名', 'バーコード', '有効期限区分', '備考', 'x'] })),
    /商品ID/, '商品IDが無ければ拒否');
  throwsWith(() => parseProductMasterCsv(iconv.encode('"商品ID","有効期限区分"\r\n"a","1","余分"\r\n', 'cp932')),
    /列数/, '列数が違う行を拒否');
  throwsWith(() => parseProductMasterCsv(csv([])), /1件も/, '0件を拒否 (商品マスタが空になることは無い)');
  throwsWith(() => parseProductMasterCsv(Buffer.from([0x83, 0xff, 0xfe, 0x41])), /Shift-JIS/, '壊れた Shift-JIS を拒否');
  throwsWith(() => parseProductMasterCsv(csv([['a', 'A', '', '1', '']], { header: ['商品ID', '商品ID', 'バーコード', '有効期限区分', '備考'] })),
    /重複/, '列名の重複を拒否');
}

console.log('\n[3] 取込');
{
  const r = importProductMaster(csv([
    ['abcDEF', '商品A', '4900000000001', '1', ''],
    ['x2', '商品B', '4900000000002', '0', ''],
    ['x3', '商品C', '4900000000003', '', ''],
    ['x4', '商品D', '4900000000004', '賞味期限', ''],
    ['', '集計行など', '', '', ''],           // 商品IDが空 → 読み飛ばす
  ]), { actor: 'tester' });
  ok(r.ok && r.total === 4, `商品IDのある4件だけ取り込む (${r.total})`);
  ok(r.managed === 2, `期限管理あり = 2件 (${r.managed})`);
  ok(r.kubunCounts['1'] === 1 && r.kubunCounts['(空欄)'] === 1 && r.kubunCounts['賞味期限'] === 1,
    `区分の内訳を返す (${JSON.stringify(r.kubunCounts)})`);
  const st = productMasterStatus();
  ok(st.total === 4 && st.managed === 2, '商品マスタ由来の件数を数えられる');
  const m = productInfoMap(['abcdef', 'x2', 'x4']);
  ok(m.get('abcdef').expiry_managed === true && m.get('abcdef').expiry_source === 'logizard', '一覧に反映される (あり)');
  ok(m.get('x2').expiry_managed === false, '一覧に反映される (なし)');
  ok(m.get('x4').expiry_managed === true, '「賞味期限」も あり として反映される');
}

console.log('\n[4] 商品IDの大文字小文字・重複');
{
  const r = importProductMaster(csv([
    ['ABCdef', '商品A (大文字違い)', '', '0', ''],
    ['abcdef', '同じ商品がもう一度', '', '1', ''],
  ]), { actor: 'tester' });
  ok(r.total === 1, '大文字小文字が違うだけの行は同じ商品として1件にまとめる');
  ok(productInfoMap(['abcdef']).get('abcdef').expiry_managed === false, '同じ商品が2度出たら先勝ち');
}

console.log('\n[5] 手動設定はロジザードの値で上書きし、件数を報告する');
{
  // 現場が応急で「あり」にしていた商品。ロジザード側が正なので上書きするが、黙って消さない
  setExpiryManaged('x2', true, '現場の人');
  ok(productInfoMap(['x2']).get('x2').expiry_source === 'manual', '手動設定が効いている');
  const r = importProductMaster(csv([['x2', '商品B', '', '0', '']]), { actor: 'tester' });
  ok(r.overroteManual === 1, `手動設定を上書きした件数を返す (${r.overroteManual})`);
  ok(r.changed === 1, '変化した件数を返す');
  const m = productInfoMap(['x2']).get('x2');
  ok(m.expiry_managed === false && m.expiry_source === 'logizard', 'ロジザードの値が正になる');
  // 値が同じなら「変化」に数えない (毎回同じ数字が出て意味を失わないように)
  const r2 = importProductMaster(csv([['x2', '商品B', '', '0', '']]), { actor: 'tester' });
  ok(r2.changed === 0 && r2.overroteManual === 0, '同じ内容の取込では変化0件');
}

console.log('\n[6] 取込に失敗しても既存の設定は残る');
{
  const before = productInfoMap(['abcdef']).get('abcdef').expiry_managed;
  try { importProductMaster(csv([])); } catch { /* 期待どおり */ }
  ok(productInfoMap(['abcdef']).get('abcdef').expiry_managed === before, '空CSVで既存の設定が消えない');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
