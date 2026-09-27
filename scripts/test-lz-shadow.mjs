/**
 * test-lz-shadow.mjs — ロジザード用 CSV の影運転 (③b-2a。apps/master-decisions/lz-csv.mjs・lz-compare.mjs・lz-snapshot.mjs・scripts/company-db/lz-shadow.mjs)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.2「③b-2」契約 v1〜v3・実測 2026-09-27):
 *   1 文字: CP932 で書く / NEC 特殊文字 (0x87xx) と CP932 に無い文字は「?」(理由つき) / ～ － は Windows の対応 / 実測していない形は unverified の印
 *   2 引用符: カンマ・"・改行の値だけ囲み " は "" / 見出しは実ファイルの 1 行目のバイトと同じ / CRLF・最後の改行なし
 *   3 仕入単価 "N.00" → N・小数や形の違いは unverified / 取引先は 4 桁のまま
 *   4 突き合わせ: 同じ = pass / 並びだけ = 許す差 (pass) / 値の差 = 説明できない (fail) / GAS の入力で再現できた差だけ時刻のずれ /
 *     推測で書いた形が違う = 判定できない / 同じなら初めて確かめた形 / 作れない行・集合を確かめていない = 判定できない / 重複は上書きしない
 *   5 材料: NE の取得の印が今の中身と合わないときは使わない / 元のコードは、この世代の書き方と Company DB の対応が同じ商品だけ
 *   6 CLI: 写しは全部 shadow_<実行 ID>_ の名前・GAS のフォルダに何も書かない・GAS の出力が前の回と同じなら流さない (L-1)
 * 使い方: node scripts/test-lz-shadow.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'lz-shadow-test-'));
process.env.DATA_DIR = tmp;
const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; } };
const WH = await quietly(() => import('../apps/warehouse/db.js'));
await quietly(() => WH.initDB());
const wh = () => WH.getDB();
const { default: iconv } = await import('iconv-lite');
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const L = await import('../apps/master-decisions/lz-csv.mjs');
const C = await import('../apps/master-decisions/lz-compare.mjs');
const S = await import('../apps/master-decisions/lz-snapshot.mjs');
const { takeLzSnapshot } = await import('./company-db/lz-shadow-snapshot.mjs');
const CLI = await import('./company-db/lz-shadow.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sj = (s) => iconv.encode(s, 'cp932');
const hex = (b) => Buffer.from(b).toString('hex');
const J = (v) => JSON.stringify(v);
const item = (code, name, cost = '100.00', sup = '0001', norm = code.toLowerCase()) => ({ code_norm: norm, ne_code: code, code_reason: null, name, cost_src: J(cost), supplier: sup });
/** GAS の出力の形 (CRLF・最後の改行なし) で CSV を作る (試験用。値はそのまま・引用符は要る値だけ) */
const gasCsv = (rows) => Buffer.concat(rows.map((r, i) => Buffer.concat([i ? Buffer.from('\r\n') : Buffer.alloc(0), Buffer.concat(r.map((c, j) => {
  const t = String(c); const q = /[",\r\n]/.test(t); const b = sj(q ? `"${t.replace(/"/g, '""')}"` : t);
  return j ? Buffer.concat([Buffer.from(','), b]) : b;
}))])));
const DH = [...L.DAILY.header];
// 実ファイルの 1 行目 (2026-09-24 の GAS の出力をそのまま読んだバイト)
const REAL_DAILY_HEADER_HEX = '8c608eae2f8c5e94d42c8fa4956996bc2c82d382e882aa82c82c8e6493fc925089bf2c8ee688f890e66964';
const REAL_NEW_HEADER_HEX = '8fa4956949442c8fa4956996bc2c8c9f8df596bc8fcc2c8e6493fc925089bf2c974c8cf88afa8cc08be695aa2c93fc89d793fa8ac7979d83748389834f2c8ee688f890e68352815b83682c836f815b8352815b8368';

console.log('test-lz-shadow');

await ta('[1] 文字: 実測した決まり (① ㎏ ➁ U+FFFD は「?」・～ － は Windows の対応) / 実測していない形は印つき', async () => {
  const e = L.encodeText('ab漢字① 1㎏➁�～－');
  assert.equal(hex(e.bytes), hex(Buffer.concat([sj('ab漢字'), Buffer.from('? 1??'), Buffer.from('?'), Buffer.from([0x81, 0x60, 0x81, 0x7c])])));
  assert.deepEqual(e.subs.map((s) => [s.at, s.why]), [[4, 'nec_special'], [7, 'nec_special'], [8, 'not_in_cp932'], [9, 'ne_replacement']]);
  assert.deepEqual(e.unverified, []);
  const why = (s) => L.encodeText(s).unverified.map((u) => u.why);
  assert.deepEqual(why('髙'), ['ibm_ext']);
  assert.deepEqual(why('〜'), ['jis_windows_split']);   // U+301C (JIS の波ダッシュ)
  assert.deepEqual(why('∥'), ['jis_windows_split']);
  assert.deepEqual(why('Ⅰ'), ['nec_ibm_dup']);
  assert.deepEqual(L.encodeText('Ⅰ').subs.map((s) => s.why), ['nec_special']);
  assert.deepEqual(why('ｱ'), ['halfwidth_kana']);
  assert.deepEqual(why('a\tb'), ['control']);
  assert.deepEqual(why('𠮷'), ['astral']);
  assert.equal(hex(L.encodeText('𠮷').bytes), '3f');
  assert.deepEqual(why('\\'), ['jis_windows_split']);
  assert.deepEqual(why('~'), []);   // ASCII の ~ は実測 (3 行)
  assert.equal(L.iconvVersion(), L.ICONV_EXPECTED, 'iconv-lite の版が変わった = CP932 の表を実測し直す');
});

await ta('[2] 引用符・見出し・改行: " の値は囲んで "" / 見出しは実ファイルの 1 行目と同じバイト / CRLF・最後の改行なし', async () => {
  const c = L.cellBytes('計算用フセン"Toketa!" 【1】');
  assert.equal(c.quoted, true);
  assert.equal(hex(c.bytes), hex(sj('"計算用フセン""Toketa!"" 【1】"')));
  assert.equal(L.cellBytes('a,b').quoted, true);
  assert.equal(L.cellBytes(' 前後の空白 ').quoted, false);
  const d = L.buildLzCsv([], 'daily');
  assert.equal(hex(d.bytes), REAL_DAILY_HEADER_HEX);
  assert.deepEqual(d.file_unverified, ['zero_rows']);
  assert.equal(hex(L.buildLzCsv([], 'new').bytes), REAL_NEW_HEADER_HEX);
  const b = L.buildLzCsv([item('B-2', '商品B'), item('A-1', '商品A')], 'daily');
  assert.equal(b.bytes.toString('latin1').endsWith('\r\n'), false);
  const lines = b.bytes.toString('latin1').split('\r\n');
  assert.equal(lines.length, 3);
  assert.deepEqual(lines.slice(1).map((l) => l.split(',')[0]), ['A-1', 'B-2']);   // 元のコードの順
  assert.equal(b.bytes.toString('latin1').split('\n').length, 3);   // 裸の LF は無い
});

await ta('[3] 仕入単価と取引先: "N.00" → N・"0.00" → 0 / 小数・形の違い・短い取引先は印つき', async () => {
  assert.deepEqual(L.costText(J('500.00')), { text: '500' });
  assert.deepEqual(L.costText(J('0.00')), { text: '0' });
  assert.deepEqual(L.costText(J('12.50')), { text: '12.5', unverified: 'cost_fraction' });
  assert.equal(L.costText(J('')).unverified, 'cost_shape');
  assert.equal(L.costText(null).unverified, 'cost_shape');
  assert.equal(L.costText(J('0500.00')).unverified, 'cost_shape');
  assert.equal(L.costText('壊れた').unverified, 'cost_not_json');
  assert.deepEqual(L.supplierText('0107'), { text: '0107' });
  assert.deepEqual(L.supplierText('7'), { text: '0007', unverified: 'supplier_short' });
  assert.equal(L.supplierText('12345').unverified, 'supplier_shape');
  assert.equal(L.supplierText(null).unverified, 'supplier_shape');
  // 数字・日付に見える値 (シートが読み替えるかもしれない) は印つき。先頭が 0 でない整数は印なし
  const flags = (code, name) => L.rowTexts({ ne_code: code, name, cost_src: J('1.00'), supplier: '0001' }, 'daily').flags.map((f) => `${f.col}:${f.why}`);
  assert.deepEqual(flags('15600', '普通の名前'), []);
  assert.deepEqual(flags('015600', '1.50'), ['0:sheet_number_like', '1:sheet_number_like', '2:sheet_number_like']);
  assert.deepEqual(flags('10-12', ''), ['0:sheet_number_like', '1:empty_name', '2:empty_name']);
});

await ta('[4] 新商品の CSV: 8 列・人の 3 列は空 (最後のカンマも残る)・作れない行は理由つきで数える', async () => {
  const r = L.buildLzCsv([item('chlorellap', 'クロレラ粉末 10g', '240.00', '0001'), { code_norm: 'x', ne_code: null, code_reason: 'code_collided' }], 'new');
  const line = iconv.decode(r.bytes, 'cp932').split('\r\n')[1];
  assert.equal(line, 'chlorellap,クロレラ粉末 10g,クロレラ粉末 10g,240,,,0001,');
  assert.deepEqual(r.unmade, [{ code_norm: 'x', reason: 'code_collided' }]);
  assert.deepEqual(r.counts, { target: 2, made: 1, unmade: 1, rows_with_subs: 0, rows_unverified: 0 });
});

await ta('[5] CSV を読む: 引用符・"" ・セルの中の改行・最後の空の列・BOM・最後の改行・壊れた引用符', async () => {
  const p = C.parseCsvBytes(Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), sj('a,b,c\r\n"x""y","1\n2",\r\n')]));
  assert.equal(p.shape.bom, true);
  assert.equal(p.shape.trailing_newline, true);
  assert.equal(p.records.length, 2);
  assert.deepEqual(p.records[1].cells.map((c) => c.toString()), ['x"y', '1\n2', '']);
  assert.deepEqual(p.records[1].quoted, [true, true, false]);
  assert.equal(p.shape.crlf, 2);
  const q = C.parseCsvBytes(sj('a,b\r\nx"y,"z'));
  assert.equal(q.shape.bare_quote, 1);
  assert.equal(q.shape.unterminated, true);
  // 閉じ引用符のあとに普通の文字 = 壊れた CSV ("a,"b を "a,b" と同じに読まない。Codex #1498 R1 M5)
  const ours = L.buildLzCsv([item('A-1', 'a,b')], 'daily');
  const broken = Buffer.from(ours.bytes.toString('latin1').replace('A-1,"a,b"', 'A-1,"a,"b'), 'latin1');
  assert.equal(C.parseCsvBytes(broken).shape.after_quote, 1);
  assert.equal(hex(C.parseCsvBytes(broken).records[1].cells[1]), hex(Buffer.from('a,b')));   // 中身だけなら同じに見える
  const rb = C.compareLz({ gas: broken, ours, compareCols: [0, 1, 2, 3, 4] });
  assert.equal(rb.verdict, 'fail');
  assert.deepEqual(rb.unexplained.map((u) => [u.what, u.after_quote]), [['gas_csv_broken', 1]]);
  assert.equal(C.lzIdsFromBarcodeMaster(sj('商品ID,バーコード\r\n"A-1"x,1')).reason, 'barcode_master_broken');
  const e = C.parseCsvBytes(sj('a,b\r\nx,'));
  assert.deepEqual(e.records[1].cells.map((c) => c.toString()), ['x', '']);
  assert.equal(e.shape.trailing_newline, false);
});

const base = () => [item('A-1', '商品A①'), item('B-2', '商品B', '200.00'), item('c-3', '商品C', '0.00', '0107')];
const gasOf = (items) => gasCsv([DH, ...items.map((it) => L.rowTexts(it, 'daily').texts)]).toString('latin1');
/** GAS の出力 = 仮の決まりどおり (① は ?) */
const gasBytes = (rows) => Buffer.from(rows, 'latin1');
const cmp = (gas, ours, extra = {}) => C.compareLz({ gas, ours, compareCols: [0, 1, 2, 3, 4], ...extra });

await ta('[6] 突き合わせ: 同じ = pass / 並びだけ = 許す差で pass / 値の差 = 説明できない / GAS の入力で再現できた差だけ時刻のずれ', async () => {
  const ours = L.buildLzCsv(base(), 'daily');
  let r = cmp(ours.bytes, ours);
  assert.equal(r.verdict, 'pass');
  assert.equal(r.counts.same_rows, 3);
  // GAS は NE の出力の順 (コードの順ではない)
  const rows = C.parseCsvBytes(ours.bytes).records;
  const reordered = Buffer.concat([rows[0], rows[3], rows[1], rows[2]].map((x, i) => Buffer.concat([i ? Buffer.from('\r\n') : Buffer.alloc(0), Buffer.concat(x.cells.map((c, j) => Buffer.concat([j ? Buffer.from(',') : Buffer.alloc(0), c])))])));
  r = cmp(reordered, ours);
  assert.equal(r.verdict, 'pass');
  assert.deepEqual(r.allowed.map((a) => a.what), ['row_order']);
  // 原価だけ違う (GAS の出力は 3 日前 = NE の原価が変わった?)
  const older = base(); older[1] = item('B-2', '商品B', '250.00');
  const gas = L.buildLzCsv(older, 'daily').bytes;
  r = cmp(gas, ours);
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.unexplained.map((u) => [u.what, u.code, u.col, u.gas.text, u.ours.text]), [['value', 'B-2', 3, '250', '200']]);
  assert.match(r.unexplained[0].note, /GAS の入力が無い/);
  // GAS が読んだ入力 (原価 250) をこちらの変換に通すと GAS と同じ = 時刻のずれ = pass
  r = cmp(gas, ours, { repro: gas });
  assert.equal(r.verdict, 'pass');
  assert.deepEqual(r.input.map((u) => [u.what, u.code]), [['value', 'B-2']]);
  // 再現で中身は消えても、GAS だけ引用符つき ("250") = 形の差が残る = 合格にしない (Codex #1498 R3 M)
  const gasQ2 = Buffer.from(gas.toString('latin1').replace(sj('B-2,商品B,商品B,250,').toString('latin1'), sj('B-2,商品B,商品B,"250",').toString('latin1')), 'latin1');
  assert.notEqual(hex(gasQ2), hex(gas));
  r = cmp(gasQ2, ours, { repro: L.buildLzCsv(older, 'daily') });
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.shape.map((x) => [x.what, x.code, x.col, x.gas, x.repro]), [['quoting', 'B-2', 3, true, false]]);
  assert.equal(r.input.length, 0);
  // 再現しても違う = 説明できない
  const other = base(); other[1] = item('B-2', '商品B', '999.00');
  r = cmp(gas, ours, { repro: L.buildLzCsv(other, 'daily').bytes });
  assert.equal(r.verdict, 'fail');
  assert.equal(r.unexplained.length, 1);
});

await ta('[7] 集合と重複: 片側だけの行 = 説明できない (GAS の入力で再現できれば時刻のずれ) / 同じコードの 2 行は上書きしない / 作れない行 = 判定できない', async () => {
  const ours = L.buildLzCsv(base(), 'daily');
  const gasMore = L.buildLzCsv([...base(), item('D-4', '新しい商品')], 'daily').bytes;
  let r = cmp(gasMore, ours);
  assert.deepEqual(r.unexplained.map((u) => [u.what, u.code]), [['only_gas', 'D-4']]);
  r = cmp(gasMore, ours, { repro: gasMore });
  assert.equal(r.verdict, 'pass');
  assert.deepEqual(r.input.map((u) => [u.what, u.code]), [['only_gas', 'D-4']]);
  // GAS にだけある行も、再現の行と引用符の付き方が違えば形の差 (Codex #1498 R3 M)
  const gasMoreQ = Buffer.from(gasMore.toString('latin1').replace('D-4,', '"D-4",'), 'latin1');
  r = cmp(gasMoreQ, ours, { repro: gasMore });
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.shape.map((x) => [x.what, x.code, x.cols]), [['quoting', 'D-4', [0]]]);
  // こちらにだけある (NE の取得のほうが新しい): GAS の入力に無いと分かれば時刻のずれ
  const oursMore = L.buildLzCsv([...base(), item('E-5', 'もっと新しい')], 'daily');
  r = cmp(ours.bytes, oursMore);
  assert.deepEqual(r.unexplained.map((u) => [u.what, u.code]), [['only_ours', 'E-5']]);
  r = cmp(ours.bytes, oursMore, { repro: ours });
  assert.deepEqual([r.verdict, r.input.map((u) => u.code)], ['pass', ['E-5']]);
  // バイトだけの再現の材料 = 作れなかった行か分からない = 時刻のずれと言わない
  r = cmp(ours.bytes, oursMore, { repro: ours.bytes });
  assert.deepEqual([r.verdict, r.unexplained.map((u) => [u.what, u.code])], ['fail', [['only_ours', 'E-5']]]);
  // 再現の材料で E-5 が作れなかった (元のコードが無い) = GAS の入力に無いとは言えない (Codex #1498 R2 M)
  const reproUnmade = L.buildLzCsv([...base(), { code_norm: 'e-5', ne_code: null, code_reason: 'code_not_in_cdb' }], 'daily');
  r = cmp(ours.bytes, oursMore, { repro: reproUnmade });
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code, u.reason]), [['only_ours', 'E-5', 'repro_unmade']]);
  // GAS に同じコードが 2 行
  const lines = ours.bytes.toString('latin1').split('\r\n');
  const dup = gasBytes([...lines, lines[2]].join('\r\n'));   // B-2 の行をもう 1 つ
  r = cmp(dup, ours);
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.unexplained.map((u) => [u.what, u.code, u.rows]), [['gas_duplicate_code', 'B-2', 2]]);
  // 元のコードが無くて作れない行 (GAS には大文字で載っている)
  const withUnmade = L.buildLzCsv([...base().slice(0, 2), { code_norm: 'c-3', ne_code: null, code_reason: 'code_collided' }], 'daily');
  r = cmp(ours.bytes, withUnmade);
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code, u.reason]), [['unmade', 'c-3', 'code_collided']]);
  // GAS にも無い作れない行も数える
  const unmadeOnly = L.buildLzCsv([...base(), { code_norm: 'zz', ne_code: null, code_reason: 'no_spelling' }], 'daily');
  r = cmp(ours.bytes, unmadeOnly);
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code]), [['unmade', 'zz']]);
});

await ta('[8] 推測で書いた形: GAS と同じでも GAS の入力で同じ文字・形だったと確かめられるまで判定できない / 確かめられたら初めて確かめた形 (pass) / 違えば判定できない / 集合を確かめていない = 判定できない', async () => {
  const its = [item('A-1', '髙島屋の品'), item('B-2', '小数', '12.50')];
  const ours = L.buildLzCsv(its, 'daily');
  // 出力が同じだけ = 確かめたことにしない (Codex #1498 R1 H3)
  let r = cmp(ours.bytes, ours);
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code, u.col]).sort(), [['unverified_unconfirmed', 'A-1', 1], ['unverified_unconfirmed', 'A-1', 2], ['unverified_unconfirmed', 'B-2', 3]]);
  // バイトだけの再現の材料でも確かめられない
  r = cmp(ours.bytes, ours, { repro: ours.bytes });
  assert.deepEqual([r.verdict, r.repro], ['fail', 'given_bytes_only']);
  // GAS が読んだ入力が同じ文字・形 (こちらの変換に通して同じ印・同じ出力) = 確かめた = pass
  r = cmp(ours.bytes, ours, { repro: L.buildLzCsv(its, 'daily') });
  assert.equal(r.verdict, 'pass');
  assert.deepEqual(r.rules_first_seen.map((x) => [x.code, x.col, x.why]).sort(), [['A-1', 1, 'ibm_ext'], ['A-1', 2, 'ibm_ext'], ['B-2', 3, 'cost_fraction']]);
  // Codex の再現: GAS が読んだ商品名はもともと「?」、今の NE は 𠮷 = どちらも「?」になるが、GAS がその文字を変えた証拠は無い
  const now = L.buildLzCsv([item('C-3', '𠮷野家')], 'daily');
  const gasIn = L.buildLzCsv([item('C-3', '?野家')], 'daily');
  assert.equal(hex(now.bytes), hex(gasIn.bytes));
  r = cmp(gasIn.bytes, now, { repro: gasIn });
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code, u.col]), [['unverified_unconfirmed', 'C-3', 1], ['unverified_unconfirmed', 'C-3', 2]]);
  assert.equal(r.rules_first_seen.length, 0);
  // GAS は髙を「?」、小数を 13 にしていた
  const gas = gasBytes(gasOf([]) + '\r\n' + ['A-1,?島屋の品,?島屋の品,100,0001', 'B-2,小数,小数,13,0001'].map((l) => sj(l).toString('latin1')).join('\r\n'));
  r = cmp(gas, ours);
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code, u.col]).sort(), [['unverified_rule', 'A-1', 1], ['unverified_rule', 'A-1', 2], ['unverified_rule', 'B-2', 3]]);
  assert.equal(r.unexplained.length, 0);
  // 新商品で、どれが載るかを確かめていない
  const plain = L.buildLzCsv(base(), 'daily');
  r = C.compareLz({ gas: plain.bytes, ours: plain, compareCols: [0, 1, 2, 3, 4], setCheck: '一覧が無い' });
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.undeterminable.map((u) => u.what), ['set_not_checked']);
});

await ta('[9] 形の差: 見出し・最後の改行・LF・BOM・引用符の付き方は合格にしない', async () => {
  const ours = L.buildLzCsv(base(), 'daily');
  const t = ours.bytes.toString('latin1');
  const what = (g) => cmp(gasBytes(g), ours).shape.map((s) => s.what);
  assert.deepEqual(what(t + '\r\n'), ['trailing_newline']);
  assert.deepEqual(what(t.replace(/\r\n/g, '\n')), ['newline']);
  assert.deepEqual(what('\xef\xbb\xbf' + t), ['bom']);
  assert.deepEqual(what(t.replace(sj('取引先id').toString('latin1'), sj('取引先ID').toString('latin1'))), ['header']);
  assert.deepEqual(what(t.replace('B-2,', '"B-2",')), ['quoting']);
  // 見出しの引用符の付き方も (Codex #1498 R2 Low)
  const f0 = sj('形式/型番').toString('latin1');
  assert.deepEqual(what(t.replace(f0, '"' + f0 + '"')), ['header']);
  assert.equal(cmp(gasBytes(t + '\r\n'), ours).verdict, 'fail');
  // 列の数が違う行
  assert.deepEqual(what(t + '\r\nX-9,a,a,1'), ['gas_row_width']);
});

await ta('[10] ロジザードの商品の一覧 (バーコードマスタ.csv): A 列が「商品ID」のときだけ読む・同じ商品ID の行は 1 つ', async () => {
  const good = C.lzIdsFromBarcodeMaster(gasCsv([['商品ID', 'バーコード'], ['A-1', '4900000000001'], ['A-1', '4900000000002'], ['c-3', '']]));
  assert.deepEqual([[...good.ids].sort(), good.reason, good.rows], [['A-1', 'c-3'], null, 3]);
  const bad = C.lzIdsFromBarcodeMaster(gasCsv([['バーコード', '商品ID'], ['1', 'A-1']]));
  assert.deepEqual([bad.ids, bad.reason], [null, 'barcode_master_header']);
  assert.deepEqual(CLI.newItemsFor(base(), { lzIds: good.ids, gasNewKeys: [] }).map((x) => x.ne_code), ['B-2']);
  assert.deepEqual(CLI.newItemsFor([...base(), { code_norm: 'q', ne_code: null }], { lzIds: good.ids, gasNewKeys: [] }).map((x) => x.code_norm), ['b-2', 'q']);
  assert.deepEqual(CLI.newItemsFor(base(), { lzIds: null, gasNewKeys: ['c-3'] }).map((x) => x.ne_code), ['c-3']);
  // 大文字・小文字だけ違う商品ID がロジザードにある = 新商品にしない・作れない行として止める (Codex #1498 R1 H2)
  const caseIds = new Set(['a-1', 'B-2', 'c-3']);   // ロジザードは a-1 (NE は A-1)
  const got = CLI.newItemsFor(base(), { lzIds: caseIds, gasNewKeys: [] });
  assert.deepEqual(got.map((x) => [x.code_norm, x.ne_code, x.code_reason]), [['a-1', null, 'lz_case_collision']]);
  // 完全一致の A-1 と a-1 が両方ある = それでも止める (Codex #1498 R2 High)
  const both = CLI.newItemsFor(base(), { lzIds: new Set(['A-1', 'a-1', 'B-2', 'c-3']), gasNewKeys: [] });
  assert.deepEqual(both.map((x) => [x.code_norm, x.ne_code, x.code_reason]), [['a-1', null, 'lz_case_collision']]);
  // GAS は完全一致なので A-1 を新商品にしている → こちらは作れない = 判定できない (合格にしない)
  const gasNew = gasCsv([[...L.NEW.header], ['A-1', '商品A?', '商品A?', '100', '02', '0', '0001', '']]);
  const r = C.compareLz({ gas: gasNew, ours: L.buildLzCsv(got, 'new'), compareCols: [0, 1, 2, 3, 6], header: [...L.NEW.header] });
  assert.equal(r.verdict, 'fail');
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code, u.reason]), [['file_zero_rows', undefined, undefined], ['unmade', 'A-1', 'lz_case_collision']]);   // こちらは 0 行 (0 行の形も未実測)
  // 比べた列・比べない列を報告に出す (L-2)
  assert.deepEqual([r.compared_cols, r.not_compared_cols], [['商品ID', '商品名', '検索名称', '仕入単価', '取引先コード'], ['有効期限区分', '入荷日管理フラグ', 'バーコード']]);
});

// ── 材料 (warehouse.db と Company DB) ──
const J2 = (v) => JSON.stringify(v);
function setNe(products, { ts = '2030-01-10 22:01:00', spellings = null, tamper = null } = {}) {
  const d = wh();
  d.exec('DELETE FROM raw_ne_products; DELETE FROM raw_ne_set_products');
  const ip = d.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 取扱区分, 原価, 売価, 消費税率, synced_at, 原価_src, 売価_src, 消費税率_src, 代表商品コード, 代表商品コード_src) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)');
  for (const p of products) ip.run(p.code, p.name, p.sup, '取扱中', 0, 0, 0, ts, J2(p.cost), J2('1000.00'), J2('10'), '', J2(''));
  const is = d.prepare('INSERT INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at, セット販売価格_src, 数量_src) VALUES (?,?,?,?,?,?,?,?)');
  is.run('s001', 'セット', 0, products[0].code, 1, ts, J2('5000'), J2('1'));
  const meta = (k) => d.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value;
  const up = (k, v) => d.prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(k, v, ts);
  up('ne_api_products_complete_at', ts); up('ne_api_products_complete_count', String(products.length)); up('ne_api_products_complete_rev', meta('ne_raw_products_rev'));
  up('ne_api_setproducts_complete_at', ts); up('ne_api_setproducts_complete_count', '1'); up('ne_api_setproducts_complete_rev', meta('ne_raw_setproducts_rev'));
  up('ne_api_setproducts_complete_parents', '1');
  if (spellings) { WH.writeCodeSpellings('products', ts, spellings.products); WH.writeCodeSpellings('sets', ts, spellings.sets); }
  if (tamper) tamper(d);
}
const spm = (o) => new Map(Object.entries(o).map(([k, v]) => [k, new Set(v)]));
const NEP = [{ code: 'a-1', name: ' 前後に空白 ', sup: '0001', cost: '100.00' }, { code: 'b-2', name: '商品B①', sup: '0002', cost: '0.00' }, { code: 'c-3', name: '商品C', sup: '0001', cost: '300.00' }, { code: 'd-4', name: '商品D', sup: '0001', cost: '400.00' }];
const SPELL = { products: { single: spm({ 'a-1': ['A-1'], 'b-2': ['b-2'], 'c-3': ['C-3', 'c-3'], 'd-4': ['D-4'] }), rep: new Map() }, sets: { set: spm({ s001: ['S001'] }), child: spm({ 'a-1': ['A-1'] }), set_rep: new Map() } };

const pg = new PGlite(); const db = pgliteAdapter(pg);
await applyMigrations(db, { log: () => {} });
async function putCodes(entries, run = 'mc_20300110T220100000Z_aaaaaa') {
  await db.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T22:01:00Z', 0) on conflict do nothing`, [run]);
  await db.query('select ops.record_ne_codes($1::jsonb)', [J2({ compare_run_id: run, entries })]);
}
const conn = async () => ({ db, close: async () => {} });

await ta('[11] 材料: この世代の書き方と Company DB の対応が同じ商品だけ元のコードを使う (無い・違う・衝突・書き方なし = 作れない・理由つき)', async () => {
  setNe(NEP, { spellings: SPELL });
  await putCodes([
    { code_norm: 'a-1', kind: 'product', state: 'ok', ne_code: 'A-1', spellings: ['A-1'] },
    { code_norm: 'b-2', kind: 'product', state: 'ok', ne_code: 'B-2', spellings: ['B-2'] },   // 前の世代の書き方 (今の世代は b-2) = 違う
    { code_norm: 'c-3', kind: 'product', state: 'collided', ne_code: null, spellings: ['C-3', 'c-3'] },
  ]);
  const s = await takeLzSnapshot({ dataDir: tmp, connect: conn, now: new Date('2030-01-10T23:00:00Z') });
  assert.equal(s.ne.ok, true);
  assert.equal(s.format, S.LZ_SNAPSHOT_FORMAT);
  assert.equal(s.cdb.mark.compare_run_id, 'mc_20300110T220100000Z_aaaaaa');
  const by = Object.fromEntries(s.items.map((x) => [x.code_norm, [x.ne_code, x.code_reason]]));
  assert.deepEqual(by, { 'a-1': ['A-1', null], 'b-2': [null, 'code_cdb_mismatch'], 'c-3': [null, 'code_collided'], 'd-4': [null, 'code_not_in_cdb'] });
  assert.deepEqual(s.counts, { products: 4, with_code: 1, reasons: { code_cdb_mismatch: 1, code_collided: 1, code_not_in_cdb: 1 } });
  // 値は NE の取得の元のまま (前後の空白も・原価は元の値)
  const a = s.items.find((x) => x.code_norm === 'a-1');
  assert.deepEqual([a.name, a.cost_src, a.supplier], [' 前後に空白 ', J2('100.00'), '0001']);
});

await ta('[12] 材料: 書き方を集めていない世代 = 全部作れない / 印の後に書かれた・件数が合わない = 使わない', async () => {
  setNe(NEP, { ts: '2030-01-11 22:01:00' });   // この世代の書き方の印が無い
  let s = await takeLzSnapshot({ dataDir: tmp, connect: conn });
  assert.equal(s.ne.ok, true);
  assert.deepEqual(s.counts.reasons, { no_spelling_not_collected: 4 });
  setNe(NEP, { ts: '2030-01-12 22:01:00', spellings: SPELL, tamper: (d) => d.prepare("UPDATE raw_ne_products SET 商品名 = '後から' WHERE 商品コード = 'b-2'").run() });
  s = await takeLzSnapshot({ dataDir: tmp, connect: conn });
  assert.deepEqual([s.ne.ok, s.ne.reason, s.items.length], [false, 'ne_written_after_mark', 0]);
  setNe(NEP, { ts: '2030-01-13 22:01:00', spellings: SPELL, tamper: (d) => d.prepare("UPDATE sync_meta SET value = '5' WHERE key = 'ne_api_products_complete_count'").run() });
  s = await takeLzSnapshot({ dataDir: tmp, connect: conn });
  assert.deepEqual([s.ne.ok, s.ne.reason], [false, 'ne_count_mismatch']);
});

await ta('[13] CLI: 写しは全部 shadow_<実行 ID>_・GAS のフォルダに何も書かない (置き場所がその近くなら止める・リンクも)・前の回と同じ GAS の出力なら流さない (--force で流す)・「?」の位置を残す・合格なら ping の案内', async () => {
  setNe(NEP, { ts: '2030-01-14 22:01:00', spellings: SPELL });
  await putCodes(['a-1', 'b-2', 'c-3', 'd-4'].map((n) => ({ code_norm: n, kind: 'product', state: n === 'c-3' ? 'collided' : 'ok', ne_code: n === 'c-3' ? null : ({ 'a-1': 'A-1', 'b-2': 'b-2', 'd-4': 'D-4' })[n], spellings: [] })), 'mc_20300114T220100000Z_bbbbbb');
  const snap = await takeLzSnapshot({ dataDir: tmp, connect: conn });
  const work = fs.mkdtempSync(path.join(os.tmpdir(), 'lz-cli-'));
  const snapPath = path.join(work, 'snap.json'); fs.writeFileSync(snapPath, JSON.stringify(snap));
  const gasDir = path.join(work, 'parent', 'gas'); fs.mkdirSync(path.join(gasDir, '商品マスタ'), { recursive: true });   // parent = 入荷バーコード発行 にあたる
  // GAS の出力 = 元のコードのある 3 商品 (c-3 は衝突 = こちらは作れない)
  const withCodes = snap.items.filter((x) => x.ne_code);
  fs.writeFileSync(path.join(gasDir, '商品マスタ', L.DAILY.file), L.buildLzCsv(withCodes, 'daily').bytes);
  fs.writeFileSync(path.join(gasDir, L.NEW.file), gasCsv([[...L.NEW.header], ['D-4', '商品D', '商品D', '400', '02', '0', '0001', '4900000000001']]));
  const before = fs.readdirSync(gasDir, { recursive: true }).sort();
  const outRoot = path.join(work, 'out');
  // 置き場所の守り (Codex #1498 R1 M6): GAS のフォルダの中・その親の中 (GAS の入力の場所)・GAS のフォルダを含む場所・リンクで入る場所 = 止める
  const refuse = (o) => assert.throws(() => CLI.runLzShadow({ snapshotPath: snapPath, gasDir, outRoot: o }), /GAS のフォルダ/);
  refuse(path.join(gasDir, 'x')); refuse(gasDir); refuse(path.join(work, 'parent', 'other')); refuse(work); refuse(path.join(work, 'parent'));
  fs.symlinkSync(path.join(work, 'parent'), path.join(work, 'link'), 'junction');
  refuse(path.join(work, 'link', 'new'));
  refuse(path.join(work, 'parent', '..shadow'));   // 「..」で始まる普通のフォルダ名は親の中 (Codex #1498 R2 M)
  assert.equal(fs.existsSync(path.join(work, 'parent', '..shadow')), false);
  assert.equal(fs.existsSync(path.join(work, 'parent', 'new')), false);
  const r1 = CLI.runLzShadow({ snapshotPath: snapPath, gasDir, outRoot, now: new Date('2030-01-15T00:00:00Z') });
  assert.match(r1.runId, CLI.RUN_ID_RE);
  const names = fs.readdirSync(r1.dir);
  assert.ok(names.length >= 7 && names.every((n) => n.startsWith(`shadow_${r1.runId}_`)), names.join(' '));
  assert.ok(!names.some((n) => CLI.GAS_NAMES.includes(n)));
  assert.deepEqual(fs.readdirSync(gasDir, { recursive: true }).sort(), before);
  const m = r1.manifest;
  assert.equal(m.verdict, 'fail');   // c-3 は作れない + 新商品の集合は確かめていない
  assert.deepEqual([m.summary.daily.verdict, m.summary.daily.undeterminable, m.summary.daily.unexplained], ['fail', 1, 0]);
  assert.deepEqual([m.summary.new.verdict, m.summary.new.same_rows, m.summary.new.undeterminable], ['fail', 1, 1]);   // 人の 3 列は比べない = 値は同じ
  assert.equal(m.files.gas_daily.sha256.length, 64);
  assert.equal(m.gas_input, 'none');
  assert.equal(m.snapshot.code_mark.compare_run_id, 'mc_20300114T220100000Z_bbbbbb');
  const rep = JSON.parse(fs.readFileSync(path.join(r1.dir, `shadow_${r1.runId}_report.json`), 'utf8'));
  assert.deepEqual(rep.daily.compare.undeterminable.map((u) => [u.what, u.code]), [['unmade', 'c-3']]);
  // 「?」にした文字の位置・元の文字・理由も残る (Codex #1498 R1 M7)
  assert.deepEqual(rep.daily.build.subs.map((x) => [x.code, x.col, x.at, x.ch, x.why]), [['b-2', 1, 3, '①', 'nec_special'], ['b-2', 2, 3, '①', 'nec_special']]);
  assert.deepEqual([m.new_set.basis, m.new_set.certified], ['not_checked', false]);
  // 同じ GAS の出力 = 流さない
  const r2 = CLI.runLzShadow({ snapshotPath: snapPath, gasDir, outRoot, now: new Date('2030-01-15T01:00:00Z') });
  assert.deepEqual(r2, { skipped: 'gas_unchanged', prev_run: r1.runId });
  assert.match(CLI.summaryLines(r2)[0], /変わっていない/);
  // ロジザードの一覧あり + 衝突が解けた材料 = 合格
  const snap2 = { ...snap, items: snap.items.map((x) => (x.code_norm === 'c-3' ? { ...x, ne_code: 'C-3', code_reason: null } : x)) };
  fs.writeFileSync(snapPath + '2', JSON.stringify(snap2));
  fs.writeFileSync(path.join(gasDir, '商品マスタ', L.DAILY.file), L.buildLzCsv(snap2.items, 'daily').bytes);
  const lzPath = path.join(work, 'barcode.csv');
  fs.writeFileSync(lzPath, gasCsv([['商品ID', 'バーコード'], ['A-1', '1'], ['b-2', '2'], ['C-3', '3']]));
  const giPath = path.join(work, 'logi_hinban.csv'); fs.writeFileSync(giPath, sj('形式/型番\r\nA-1'));
  const r3 = CLI.runLzShadow({ snapshotPath: snapPath + '2', gasDir, outRoot, lzListPath: lzPath, gasInputPath: giPath, now: new Date('2030-01-15T02:00:00Z') });
  assert.equal(r3.manifest.verdict, 'pass', JSON.stringify(r3.report, null, 1).slice(0, 2000));
  // 新商品の合格が言うのは「GAS と同じ一覧から同じものが作れた」まで・比べたのは 5 列だけ (Codex #1498 R1 H1・M8)
  assert.deepEqual([r3.manifest.new_set.basis, r3.manifest.new_set.certified], ['gas_list_reproduction', false]);
  assert.deepEqual(r3.manifest.summary.new.not_compared_cols, ['有効期限区分', '入荷日管理フラグ', 'バーコード']);
  const lines3 = CLI.summaryLines(r3).join('\n');
  assert.match(lines3, /比べない \(人が入れる\) = 有効期限区分・入荷日管理フラグ・バーコード/);
  assert.match(lines3, /GAS が読んだ一覧 \(直近 30 日の書き出し\) での再現だけ/);
  // GAS の入力は写しだけ残す (再現の道はまだ無い)
  assert.deepEqual(r3.manifest.gas_input, { saved: true, reproduced: false });
  assert.ok(fs.readdirSync(r3.dir).includes(`shadow_${r3.runId}_logi_hinban.csv`));
  assert.match(lines3, /写しを残した/);
  assert.ok(fs.readdirSync(r3.dir).includes(`shadow_${r3.runId}_バーコードマスタ.csv`));
  assert.match(CLI.summaryLines(r3).join('\n'), /合格。miniPC で台帳の完了の ping/);
  // --force は同じ出力でも流す
  const r4 = CLI.runLzShadow({ snapshotPath: snapPath + '2', gasDir, outRoot, lzListPath: lzPath, force: true, now: new Date('2030-01-15T03:00:00Z') });
  assert.ok(r4.runId && r4.runId !== r3.runId);
  assert.throws(() => CLI.shadowName('', 'x'), /重なる/);
});

await ta('[14] 台帳 lz-shadow-compare: 見張りの計算で、台帳に載ってから 14 日で期限切れ・3 日前から催促・途中で ping しなければ延びない・合格の ping で完了', async () => {
  const { JOBS_REGISTRY, validateRegistry } = await import('../config/jobs-registry.mjs');
  const { evaluateEntry } = await import('../apps/jobs-monitor/evaluate.js');
  const def = JOBS_REGISTRY.find((e) => e.id === 'lz-shadow-compare');
  assert.ok(def);
  assert.deepEqual(validateRegistry([def]), []);
  const D = 24 * 3600 * 1000, seen = Date.UTC(2030, 0, 15, 0, 0, 0);
  const at = (days, st = {}) => evaluateEntry(def, { firstSeenAtMs: seen, ...st }, seen + days * D).status;
  assert.equal(at(1), 'uninitialized');
  assert.equal(at(10.9), 'uninitialized');
  assert.equal(at(11.5), 'due_soon');
  assert.equal(at(14.1), 'overdue');   // 途中の (不合格の) 回で ping しない = 延びない
  assert.equal(at(14.1, { lastOkAtMs: seen + 13 * D }), 'ok');   // 合格の ping (この後 RETIRED_JOBS へ移す)
});

try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
