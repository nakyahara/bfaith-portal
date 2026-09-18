import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
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
  // ⭐実データで確定 (2026-09-01): ロジザードはゼロ埋め2桁のコードで 01=無し / 02=有効期限あり。
  //   在庫データと 2,875 件を突き合わせて例外ゼロで一致した。数値は先頭ゼロを外して判定する
  for (const v of ['0', '00', '01', '1', ' 01 ', 'なし', '無し', '無', 'しない', '管理しない', '対象外', '-', '－']) {
    ok(isExpiryManagedValue(v) === false, `「${v}」= 期限管理なし`);
  }
  // 🚨 空欄は「管理しない」ではなく「分からない」。0 として書くと下流 (FBA箱詰めの期限入力欄) が
  //    確かな「期限管理でない」として読み、期限を聞かずに通してしまう (Codex PR #1356 R1)
  for (const v of ['', '  ', null, undefined]) {
    ok(isExpiryManagedValue(v) === null, `「${v}」= 分からない (書かない)`);
  }
  // 02 以降は別の期限種別 (製造日・消費期限など) が増えても「管理する」に入る
  for (const v of ['02', '2', '03', '3', '10', 'あり', '有り', '賞味期限', '消費期限', '製造日']) {
    ok(isExpiryManagedValue(v) === true, `「${v}」= 期限管理あり`);
  }
}

console.log('\n[2] fail-closed (壊れたファイルで既存の設定を壊さない)');
{
  throwsWith(() => parseProductMasterCsv(Buffer.alloc(0)), /空/, '空ファイルを拒否');
  throwsWith(() => parseProductMasterCsv(csv([['a', 'A', '', '02', '']], { header: ['商品ID', '商品名', 'バーコード', '備考'] })),
    /有効期限区分/, '有効期限区分が無ければ拒否 (列名が変わったのを黙って通さない)');
  throwsWith(() => parseProductMasterCsv(csv([['a', 'A', '', '02', '']], { header: ['商品名', 'バーコード', '有効期限区分', '備考', 'x'] })),
    /商品ID/, '商品IDが無ければ拒否');
  throwsWith(() => parseProductMasterCsv(iconv.encode('"商品ID","有効期限区分"\r\n"a","1","余分"\r\n', 'cp932')),
    /列数/, '列数が違う行を拒否');
  throwsWith(() => parseProductMasterCsv(csv([])), /1件も/, '0件を拒否 (商品マスタが空になることは無い)');
  throwsWith(() => parseProductMasterCsv(Buffer.from([0x83, 0xff, 0xfe, 0x41])), /Shift-JIS/, '壊れた Shift-JIS を拒否');
  throwsWith(() => parseProductMasterCsv(csv([['a', 'A', '', '02', '']], { header: ['商品ID', '商品ID', 'バーコード', '有効期限区分', '備考'] })),
    /重複/, '列名の重複を拒否');
}

console.log('\n[3] 取込');
{
  const r = importProductMaster(csv([
    ['abcDEF', '商品A', '4900000000001', '02', ''],
    ['x2', '商品B', '4900000000002', '01', ''],
    ['x3', '商品C', '4900000000003', '', ''],
    ['x4', '商品D', '4900000000004', '賞味期限', ''],
    ['', '集計行など', '', '', ''],           // 商品IDが空 → 読み飛ばす
  ]), { actor: 'tester' });
  ok(r.ok && r.total === 4, `商品IDのある4件だけ取り込む (${r.total})`);
  ok(r.managed === 2, `期限管理あり = 2件 (${r.managed})`);
  ok(r.kubunCounts['02'] === 1 && r.kubunCounts['(空欄)'] === 1 && r.kubunCounts['賞味期限'] === 1,
    `区分の内訳を返す (${JSON.stringify(r.kubunCounts)})`);
  const st = productMasterStatus();
  ok(r.skippedUnknown === 1, `区分が空欄の商品は書かない (${r.skippedUnknown}件)`);
  ok(r.clearedStale === 0, '前に書いた値が無ければ取り下げる件数も 0');
  ok(st.total === 3 && st.managed === 2, `商品マスタ由来の件数を数えられる (空欄の1件は入らない: ${st.total})`);
  const blank = productInfoMap(['x3']).get('x3');
  ok(blank.expiry_managed === false && blank.expiry_source === 'none',
    '区分が空欄の商品は、入荷受付チェックではこれまでどおり (在庫からの推定に落ちる)');
  const m = productInfoMap(['abcdef', 'x2', 'x4']);
  ok(m.get('abcdef').expiry_managed === true && m.get('abcdef').expiry_source === 'logizard', '一覧に反映される (あり)');
  ok(m.get('x2').expiry_managed === false, '一覧に反映される (なし)');
  ok(m.get('x4').expiry_managed === true, '「賞味期限」も あり として反映される');
}

console.log('\n[4] 商品IDの大文字小文字・重複');
{
  const r = importProductMaster(csv([
    ['ABCdef', '商品A (大文字違い)', '', '01', ''],
    ['abcdef', '同じ商品がもう一度', '', '02', ''],
  ]), { actor: 'tester' });
  ok(r.total === 1, '大文字小文字が違うだけの行は同じ商品として1件にまとめる');
  ok(productInfoMap(['abcdef']).get('abcdef').expiry_managed === false, '同じ商品が2度出たら先勝ち');
}

console.log('\n[5] 手動設定はロジザードの値で上書きし、件数を報告する');
{
  // 現場が応急で「あり」にしていた商品。ロジザード側が正なので上書きするが、黙って消さない
  setExpiryManaged('x2', true, '現場の人');
  ok(productInfoMap(['x2']).get('x2').expiry_source === 'manual', '手動設定が効いている');
  const r = importProductMaster(csv([['x2', '商品B', '', '01', '']]), { actor: 'tester' });
  ok(r.overroteManual === 1, `手動設定を上書きした件数を返す (${r.overroteManual})`);
  ok(r.changed === 1, '変化した件数を返す');
  const m = productInfoMap(['x2']).get('x2');
  ok(m.expiry_managed === false && m.expiry_source === 'logizard', 'ロジザードの値が正になる');
  // 値が同じなら「変化」に数えない (毎回同じ数字が出て意味を失わないように)
  const r2 = importProductMaster(csv([['x2', '商品B', '', '01', '']]), { actor: 'tester' });
  ok(r2.changed === 0 && r2.overroteManual === 0, '同じ内容の取込では変化0件');
}

console.log('\n[5b] 区分が空欄になったら、前にロジザードから書いた値を取り下げる (Codex PR #1356 R2 #1)');
{
  // 🚨 書かずに飛ばすだけだと、01 → 空欄 と変わった商品に古い 0 が残り、
  //    下流 (FBA箱詰めの期限入力欄) がそれを確かな「期限管理でない」として読み続ける
  importProductMaster(csv([['stale1', '商品S', '', '01', ''], ['keep1', '商品K', '', '01', '']]), { actor: 'tester' });
  ok(productInfoMap(['stale1']).get('stale1').expiry_source === 'logizard', '前の取込で「期限管理でない」が入っている');
  const r = importProductMaster(csv([['stale1', '商品S', '', '', ''], ['keep1', '商品K', '', '01', '']]), { actor: 'tester' });
  ok(r.clearedStale === 1, `空欄になった商品の古い値を取り下げた件数を返す (${r.clearedStale})`);
  const m = productInfoMap(['stale1']).get('stale1');
  ok(m.expiry_source === 'none', `分からない状態に戻る (${m.expiry_source})`);
  ok(productInfoMap(['keep1']).get('keep1').expiry_source === 'logizard', '同じ取込の他の商品は消さない');
  // 人が手で設定した値は消さない (マスタが分からなくても、人の判断は残す)
  setExpiryManaged('hand1', true, { actor: 'tester' });
  const r2 = importProductMaster(csv([['hand1', '商品H', '', '', '']]), { actor: 'tester' });
  ok(r2.clearedStale === 0 && productInfoMap(['hand1']).get('hand1').expiry_source === 'manual', '手動設定は取り下げない');
}

console.log('\n[5c] ⚠ 空欄の商品は、在庫に有効期限があれば「期限管理あり」に変わる (この PR で変わる動き)');
{
  // 以前は空欄から書かれた 0 が在庫の推定より優先されて false だった。いまはフラグが無いので在庫の推定が出る。
  // 実データ (2026-09-01) は 4,987 件すべて 01/02 で空欄 0 件なので、現時点で該当する商品は無い
  const mdb = (await import('../apps/warehouse-mirror/db.js')).getMirrorDB();
  const nowIso = new Date().toISOString();
  mdb.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, バーコード, ブロック略称, ロケ, 品質区分名, 有効期限, 入荷日, 在庫数, 引当数, ロケ業務区分, 最終入荷日, 最終出荷日, 在庫日, captured_at, synced_at)
    VALUES ('BLANK1', '期限つき在庫のある商品', '', 'P3F', 'A-01', '良品', '2027-03-31', '', 5, 0, '', '', '', '', ?, ?)`).run(nowIso, nowIso);
  importProductMaster(csv([['blank1', '商品X', '', '01', '']]), { actor: 'tester' });
  ok(productInfoMap(['blank1']).get('blank1').expiry_managed === false, '区分 01 のうちは「期限管理でない」');
  importProductMaster(csv([['blank1', '商品X', '', '', '']]), { actor: 'tester' });
  const m = productInfoMap(['blank1']).get('blank1');
  ok(m.expiry_managed === true && m.expiry_source === 'stock',
    `空欄になると在庫の推定に落ちて「期限管理あり」になる (${m.expiry_source}) — 確認のときに期限を聞く側に倒れる`);
}

console.log('\n[6] 取込に失敗しても既存の設定は残る');
{
  const before = productInfoMap(['abcdef']).get('abcdef').expiry_managed;
  try { importProductMaster(csv([])); } catch { /* 期待どおり */ }
  ok(productInfoMap(['abcdef']).get('abcdef').expiry_managed === before, '空CSVで既存の設定が消えない');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
