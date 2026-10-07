/**
 * test-lz-daily.mjs — ロジザードの毎日の商品マスタを Company DB の値で作る (③c-1a。apps/master-decisions/lz-cdb.mjs・scripts/company-db/lz-daily.mjs)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c 契約 v1〜v3」・中原さんの答え L-4〜L-8):
 *   1 ロジザードの全件の一覧: 見出しは実ファイルの 1 行目・壊れた / 列の数が違う / 商品ID が空の行 / 少なすぎる / 同じ ID が 2 つ / 前回の半分未満 = 使わない
 *   2 3 つに分ける: 比べる (ロジザードにある・値が全部そろう) / 新商品待ち (ロジザードに無い) / 不正 (理由つき。0 や空で埋めない。原価 0 以下も出さない)
 *   3 差の説明: 名前の前後の空白 (L-4) と、照合 ② の差の一覧に同じ商品・列・両側の値で載る差だけ許す。NE の道の推測の形は判定できない
 *   4 材料の条件: その朝の照合 (complete・sha256)・NE の取得がその朝・一覧がその日の成功した書き出し (成功の印)・元のコードの印がその回。欠ける = 作らない (exit 3・fail の ping)
 *   5 出すもの: 変えない CSV・報告・完了の印 (sha256・行数・期限)。始めに running・書けない = 失敗。不正・比べる 0 = 合格にしない
 *   6 監視: 作れた回だけ ok の ping (台帳 lz-daily-build)
 * 使い方: node scripts/test-lz-daily.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

process.env.DAILY_SYNC_RUN_ID = 'ds_test';   // 証跡を daily-sync の名前 (master-compare.json) で書く
const { default: iconv } = await import('iconv-lite');
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const D = await import('../apps/master-decisions/lz-cdb.mjs');
const C = await import('../apps/master-decisions/lz-compare.mjs');
const L = await import('../apps/master-decisions/lz-csv.mjs');
const RUN = await import('./company-db/lz-daily.mjs');
const { writeEvidence } = await import('../apps/company-db/push/evidence.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sj = (s) => iconv.encode(s, 'cp932');
const J = (v) => JSON.stringify(v);
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
// ロジザードの商品マスタの書き出しの実ファイルの 1 行目 (2026-09-28 に miniPC の shohin_master.csv をそのまま読んだバイト。全部の値が引用符つき)
const REAL_LZ_HEADER_HEX = '228c5f96f18ed24944222c228c5f96f18ed296bc222c2289d78ee54944222c2289d78ee596bc222c228fa495694944222c228fa4956996bc222c228c9f8df596bc8fcc222c228c9f8df596bc8fcc32222c228e6493fc925089bf222c2288f89396957389c293fa9094222c2291e595aa97de222c22928695aa97de222c228fac95aa97de222c22838d836283678ac7979d83748389834f222c22974c8cf88afa8cc08be695aa222c2289b7937891d18be695aa222c228fac948489bf8a69222c22835a836283678d5c90ac8be695aa222c228ded8f9c83748389834f222c22936f985e93fa8e9e222c2295cf8d5893fa8e9e222c2283438393837c815b836793fa8e9e222c228ddd8cc98be695aa222c2293fc89d78afa8cc093fa9094222c2293fc89d793fa8ac7979d83748389834f222c228fa49569975c94f58d8096da824f824f8250222c228fa49569975c94f58d8096da824f824f8251222c228fa49569975c94f58d8096da824f824f8252222c228fa49569975c94f58d8096da824f824f8253222c228fa49569975c94f58d8096da824f824f8254222c228fa49569975c94f58d8096da824f824f8255222c228fa49569975c94f58d8096da824f824f8256222c228fa49569975c94f58d8096da824f824f8257222c228fa49569975c94f58d8096da824f824f8258222c228fa49569975c94f58d8096da824f8250824f222c22959496e54944222c228fac948489bf8a6932222c228fac948489bf8a6933222c228fac948489bf8a6934222c228fac948489bf8a6935222c2289708cea96bc222c228f6497ca222c228356838a8341838b936f985e83748389834f22';
const q = (v) => '"' + String(v).replace(/"/g, '""') + '"';
const lzRow = (o) => { const c = Array(43).fill(''); c[4] = o.id; c[5] = o.name ?? 'x'; c[8] = o.cost ?? '0'; c[18] = o.del ?? '0'; c[27] = o.sup ?? '0001'; return sj(c.map(q).join(',')); };
const lzCsv = (rows, extra = []) => Buffer.concat([Buffer.from(REAL_LZ_HEADER_HEX, 'hex'), ...rows.flatMap((r) => [Buffer.from('\r\n'), lzRow(r)]), ...extra]);   // 最後の改行なし (実ファイルと同じ)

console.log('test-lz-daily');

await ta('[1] ロジザードの全件の一覧: 実ファイルの見出し・列の取り出し / 壊れた・見出し違い・列の数違い・少なすぎる・同じ ID が 2 つ = 使わない', async () => {
  let r = D.readLzShohinMaster(lzCsv([{ id: 'A-1', name: '商品A', cost: '100', sup: '0107' }, { id: 'b-2', del: '1' }]), { minRows: 1 });
  const { cells: a1cells, raw: a1raw, ...a1 } = r.byId.get('A-1');   // cells = 43 列を文字のまま・raw = バイトのまま (③c-1b-2b-1a)
  assert.deepEqual([r.ok, r.rows, a1, a1cells.length, a1cells[4], r.byId.get('b-2').deleted], [true, 2, { name: '商品A', cost: '100', supplier: '0107', deleted: '0' }, 43, 'A-1', '1']);
  assert.deepEqual([a1raw.length, Buffer.isBuffer(a1raw[4]), r.encoding], [43, true, { fffd: 0, not_round_trip: 0 }]);
  assert.deepEqual(r.lowerGroups.get('a-1'), ['A-1']);
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }]), { minRows: 2 }).reason, 'lz_master_too_few');
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }])).reason, 'lz_master_too_few');   // 既定の下限 4,000
  assert.equal(D.readLzShohinMaster(sj('"商品ID","商品名"\r\n"A-1","x"'), { minRows: 1 }).reason, 'lz_master_header');
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }], [sj('\r\n"x","y"')]), { minRows: 1 }).reason, 'lz_master_row_width');
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }, { id: 'A-1' }]), { minRows: 1 }).reason, 'lz_master_duplicate_id');
  assert.equal(D.readLzShohinMaster(Buffer.concat([lzCsv([{ id: 'A-1' }]), sj('\r\n"x","y')]), { minRows: 1 }).reason, 'lz_master_broken');
  r = D.readLzShohinMaster(lzCsv([{ id: 'ABC' }, { id: 'abc' }]), { minRows: 1 });
  assert.deepEqual(r.lowerGroups.get('abc').sort(), ['ABC', 'abc']);
  // 商品ID が空・空白だけの行がある = 壊れた一覧 (黙って飛ばすと、ID が全部空の一覧で「全部が新商品待ち」の合格になる。Codex #1507 R1)
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }, { id: '' }]), { minRows: 1 }).reason, 'lz_master_blank_id');
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }, { id: '  ' }]), { minRows: 1 }).reason, 'lz_master_blank_id');
  assert.equal(D.readLzShohinMaster(lzCsv(Array.from({ length: 3 }, () => ({ id: '' }))), { minRows: 3 }).reason, 'lz_master_blank_id');
});

// ── 材料の組み立て ──
const neItem = (code, name = `商品${code}`, cost = '100.00', sup = '0001', over = {}) => ({ code_norm: code.toLowerCase(), ne_code: code, code_reason: null, name, cost_src: J(cost), supplier: sup, ...over });
const cdbOf = (rows) => ({
  skuByNorm: new Map(rows.map((r) => [r.norm, { code_norm: r.norm, name: r.name }])),
  costs: new Map(rows.filter((r) => r.cost !== undefined).map((r) => [r.norm, { cost_jpy: r.cost }])),
  primary: new Map(rows.filter((r) => r.sups).map((r) => [r.norm, r.sups])),
});
const lzOf = (ids, over = {}) => D.readLzShohinMaster(lzCsv(ids.map((id) => ({ id, ...(over[id] || {}) }))), { minRows: 1 });

await ta('[2] 3 つに分ける: 比べる / 新商品待ち (ロジザードに無い) / 不正 (元のコードなし・大文字小文字の衝突・ロジザードで削除・Company DB に無い・名前・原価・仕入先)', async () => {
  const ne = [neItem('A-1', ' 前後に空白 '), neItem('NEW-1'), { ...neItem('col-1'), ne_code: null, code_reason: 'code_collided' }, neItem('Case-1'), neItem('Del-1'), neItem('Nosku-1'),
    neItem('Blank-1', '  '), neItem('Cblank-1'), neItem('Nocost-1'), neItem('Frac-1'), neItem('Neg-1'), neItem('Nosup-1'), neItem('Twosup-1'), neItem('Badsup-1')];
  const cdb = cdbOf([
    { norm: 'a-1', name: '前後に空白', cost: 120, sups: ['0001'] }, { norm: 'case-1', name: 'x', cost: 1, sups: ['0001'] }, { norm: 'del-1', name: 'x', cost: 1, sups: ['0001'] },
    { norm: 'blank-1', name: 'blank-1', cost: 1, sups: ['0001'] }, { norm: 'cblank-1', name: '  ', cost: 1, sups: ['0001'] }, { norm: 'nocost-1', name: 'x', sups: ['0001'] },
    { norm: 'frac-1', name: 'x', cost: 1.5, sups: ['0001'] }, { norm: 'neg-1', name: 'x', cost: -1, sups: ['0001'] }, { norm: 'nosup-1', name: 'x', cost: 1, sups: [] },
    { norm: 'twosup-1', name: 'x', cost: 1, sups: ['0001', '0002'] }, { norm: 'badsup-1', name: 'x', cost: 1, sups: ['107'] }]);
  const lz = lzOf(['A-1', 'case-1', 'Del-1', 'Nosku-1', 'Blank-1', 'Cblank-1', 'Nocost-1', 'Frac-1', 'Neg-1', 'Nosup-1', 'Twosup-1', 'Badsup-1'], { 'Del-1': { del: '1' } });
  const r = D.classifyForLz({ neItems: ne, cdb, lz });
  assert.deepEqual(r.compare.map((x) => [x.key, x.cdb.name, x.cdb.cost_text, x.cdb.supplier]), [['A-1', '前後に空白', '120', '0001']]);
  assert.deepEqual(r.awaiting.map((x) => x.ne_code), ['NEW-1']);
  assert.deepEqual(r.invalid.map((x) => [x.code_norm, x.reason]), [['col-1', 'code_collided'], ['case-1', 'lz_case_collision'], ['del-1', 'lz_deleted'], ['nosku-1', 'cdb_no_sku'],
    ['blank-1', 'ne_name_blank'], ['cblank-1', 'cdb_name_blank'], ['nocost-1', 'cdb_cost_missing'], ['frac-1', 'cdb_cost_shape'], ['neg-1', 'cdb_cost_shape'], ['nosup-1', 'cdb_supplier_count'],
    ['twosup-1', 'cdb_supplier_count'], ['badsup-1', 'cdb_supplier_shape']]);
  assert.deepEqual([r.counts.targets, r.counts.compare, r.counts.awaiting, r.counts.invalid], [14, 1, 1, 12]);
  // 原価 0 = そのまま出す (中原さん L-9 C。Company DB の 0 は NE に 0 と入っている値・今の GAS も 0 を書く)。
  // ロジザードに 0 でない原価 (空も含む) がある商品は、止めずに cost_zero_over_lz に残す (0 で上書きする = 今の GAS と同じ)
  const neZ = [neItem('Z-1', '商品Z-1', '0.00'), neItem('Z-2', '商品Z-2', '0.00'), neItem('Z-3', '商品Z-3', '0.00'), neItem('Z-4', '商品Z-4', '0.00'), neItem('Z-5', '商品Z-5', '0.00')];
  const cdbZ = cdbOf([{ norm: 'z-1', name: 'z', cost: 0, sups: ['0001'] }, { norm: 'z-2', name: 'z', cost: 0, sups: ['0001'] }, { norm: 'z-3', name: 'z', cost: 0, sups: ['0001'] },
    { norm: 'z-4', name: 'z', cost: 0, sups: ['107'] }, { norm: 'z-5', name: 'z', cost: 5, sups: ['0001'] }]);
  const z = D.classifyForLz({ neItems: neZ, cdb: cdbZ, lz: lzOf(['Z-1', 'Z-2', 'Z-3', 'Z-4', 'Z-5'], { 'Z-1': { cost: '0' }, 'Z-2': { cost: '500' }, 'Z-3': { cost: '' }, 'Z-4': { cost: '500' }, 'Z-5': { cost: '500' } }) });
  assert.deepEqual(z.compare.map((x) => [x.key, x.cdb.cost_text]), [['Z-1', '0'], ['Z-2', '0'], ['Z-3', '0'], ['Z-5', '5']]);
  assert.deepEqual(z.invalid.map((x) => [x.code_norm, x.reason]), [['z-4', 'cdb_supplier_shape']]);
  assert.deepEqual(z.cost_zero_over_lz.map((x) => [x.ne_code, x.lz_cost]), [['Z-2', '500'], ['Z-3', '']]);   // 出さない商品 (Z-4) は数えない
  assert.deepEqual([z.counts.cost_zero, z.counts.cost_zero_over_lz], [3, 2]);
});

/** 比べる商品から両方の道の CSV を作って比べ、差を説明する */
function compareBoth(items, compareJson) {
  const cdbCsv = L.buildLzCsv(items.map((x) => x.cdb), 'daily'), neCsv = L.buildLzCsv(items.map((x) => x.ne), 'daily');
  const raw = C.compareLz({ gas: neCsv.bytes, ours: cdbCsv, compareCols: [0, 1, 2, 3, 4], header: [...L.DAILY.header] });
  return D.explainCdbDiffs(raw, { compareIndex: D.compareNeIndex(compareJson), byKey: new Map(items.map((x) => [x.key, x])), neRows: neCsv.rows });
}
const cmpJson = (items) => ({ ne: { items: items.map(([norm, col, n, c, cls = 'ne_no_value']) => ({ norm, columns: [{ col, n, c, cls }] })) } });

await ta('[3] 差の説明: 名前の前後の空白 (L-4) / 照合 ② に同じ商品・列・両側の値で載る差だけ許す / 値が違う・載っていない・一致の行 = 説明できない', async () => {
  const cdb = cdbOf([{ norm: 'a-1', name: '名前A', cost: 100, sups: ['0001'] }, { norm: 'b-2', name: '名前B', cost: 5200, sups: ['0001'] }, { norm: 'c-3', name: '別の名前', cost: 100, sups: ['0001'] },
    { norm: 'd-4', name: '名前D', cost: 100, sups: ['0002'] }]);
  const ne = [neItem('A-1', ' 名前A '), neItem('B-2', '名前B', '0.00'), neItem('C-3', '名前C'), neItem('D-4', '名前D', '100.00', '0001')];
  const cls = D.classifyForLz({ neItems: ne, cdb, lz: lzOf(['A-1', 'B-2', 'C-3', 'D-4']) });
  // 照合 ② の差の一覧: B-2 の原価 (NE 0 / Company DB 5200) と D-4 の仕入先。C-3 の名前は載っていない
  let r = compareBoth(cls.compare, cmpJson([['b-2', 'cost', 0, 5200], ['d-4', 'primary_supplier', '0001', ['0002'], 'unexplained']]));
  assert.deepEqual(r.allowed.map((a) => [a.code, a.col, a.why]).sort(), [['A-1', 1, 'name_trim'], ['A-1', 2, 'name_trim'], ['B-2', 3, 'compare_ne'], ['D-4', 4, 'compare_ne']]);
  assert.deepEqual(r.unexplained.map((u) => [u.code, u.col]), [['C-3', 1], ['C-3', 2]]);
  assert.equal(r.verdict, 'fail');
  // 照合 ② の値が違う (Company DB 5000 と載っている) = 説明できない
  r = compareBoth(cls.compare.filter((x) => x.key === 'B-2'), cmpJson([['b-2', 'cost', 0, 5000]]));
  assert.deepEqual([r.verdict, r.unexplained.map((u) => u.code)], ['fail', ['B-2']]);
  // Company DB 側の値は合うが NE 側の値が違う (照合が見た NE の値と今の NE の値が違う) = 説明できない
  r = compareBoth(cls.compare.filter((x) => x.key === 'B-2'), cmpJson([['b-2', 'cost', 10, 5200]]));
  assert.deepEqual([r.verdict, r.unexplained.map((u) => u.code)], ['fail', ['B-2']]);
  // 照合 ② で「一致」の列 = 説明にしない
  r = compareBoth(cls.compare.filter((x) => x.key === 'B-2'), cmpJson([['b-2', 'cost', 0, 5200, 'match']]));
  assert.equal(r.verdict, 'fail');
  // 全部説明できる = 合格
  r = compareBoth(cls.compare.filter((x) => x.key !== 'C-3'), cmpJson([['b-2', 'cost', 0, 5200], ['d-4', 'primary_supplier', '0001', ['0002'], 'unexplained']]));
  assert.deepEqual([r.verdict, r.summary.unexplained, r.summary.allowed], ['pass', 0, 4]);
  // 名前の前後の空白 (L-4) と照合 ② の名前の差が重なる: 照合 ② は前後の空白を削った形で持つ (Codex #1507 R2 Medium)
  const nm = D.classifyForLz({ neItems: [neItem('N-9', ' 古い名前 ')], cdb: cdbOf([{ norm: 'n-9', name: '新しい名前', cost: 100, sups: ['0001'] }]), lz: lzOf(['N-9']) });
  r = compareBoth(nm.compare, cmpJson([['n-9', 'name', '古い名前', '新しい名前', 'unexplained']]));
  assert.deepEqual([r.verdict, r.allowed.map((a) => [a.col, a.why])], ['pass', [[1, 'compare_ne'], [2, 'compare_ne']]]);
  r = compareBoth(nm.compare, cmpJson([['n-9', 'name', '別の名前', '新しい名前', 'unexplained']]));
  assert.deepEqual([r.verdict, r.unexplained.map((u) => u.col)], ['fail', [1, 2]]);   // 照合 ② の NE の名前が違う = 説明できない
});

await ta('[3b] NE の道の推測の形 (GAS で確かめていないセル) = 同じ値でも差でも判定できない / 引用符を含む名前の前後の空白 = 許す差', async () => {
  // E-5: NE の原価 "100" (cost_shape)・仕入先 "1" (supplier_short) が、変換の後に Company DB と同じになる = 確かめたことにしない (Codex #1507 R1 High)
  // F-6: NE の原価 "100" と Company DB 120 の差 = 照合 ② に載っていても、NE の道が推測 = 判定できない (許す差にも説明できないにもしない)
  // G-7: 引用符を含む名前の前後の空白 (L-4) = 許す差 (セルの "" を " に戻して比べる。Codex #1507 R1 Low)
  const cdb = cdbOf([{ norm: 'e-5', name: '名前E', cost: 100, sups: ['0001'] }, { norm: 'f-6', name: '名前F', cost: 120, sups: ['0001'] }, { norm: 'g-7', name: 'Product "A"', cost: 100, sups: ['0001'] }]);
  const ne = [neItem('E-5', '名前E', '100', '1'), neItem('F-6', '名前F', '100'), neItem('G-7', ' Product "A" ')];
  const cls = D.classifyForLz({ neItems: ne, cdb, lz: lzOf(['E-5', 'F-6', 'G-7']) });
  let r = compareBoth(cls.compare, cmpJson([['f-6', 'cost', 100, 120]]));
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.code, u.col, u.why]).sort(),
    [['ne_unverified', 'E-5', 3, ['cost_shape']], ['ne_unverified', 'E-5', 4, ['supplier_short']], ['ne_unverified', 'F-6', 3, ['cost_shape']]]);
  assert.deepEqual([r.unexplained.length, r.allowed.map((a) => [a.code, a.col, a.why]).sort()], [0, [['G-7', 1, 'name_trim'], ['G-7', 2, 'name_trim']]]);
  assert.deepEqual([r.verdict, r.summary.undeterminable], ['fail', 3]);
  r = compareBoth(cls.compare.filter((x) => x.key === 'G-7'), cmpJson([]));
  assert.deepEqual([r.verdict, r.summary.allowed, r.summary.unexplained], ['pass', 2, 0]);
  // 両方の道で推測の形 (半角カナ) = 1 つのセルに 1 件だけ (Company DB の道の分は compareLz が数える。二重に数えない)
  const h = D.classifyForLz({ neItems: [neItem('H-8', 'ｱｲｳ')], cdb: cdbOf([{ norm: 'h-8', name: 'ｱｲｳ', cost: 100, sups: ['0001'] }]), lz: lzOf(['H-8']) });
  r = compareBoth(h.compare, cmpJson([]));
  assert.deepEqual(r.undeterminable.map((u) => [u.what, u.col]), [['unverified_unconfirmed', 1], ['unverified_unconfirmed', 2]]);
  // NE の行を渡さない (今までの呼び方) = NE の推測は見えない。lz-daily.mjs は必ず渡す ([4b])
  const cdbCsv = L.buildLzCsv(cls.compare.filter((x) => x.key === 'E-5').map((x) => x.cdb), 'daily'), neCsv = L.buildLzCsv(cls.compare.filter((x) => x.key === 'E-5').map((x) => x.ne), 'daily');
  const raw = C.compareLz({ gas: neCsv.bytes, ours: cdbCsv, compareCols: [0, 1, 2, 3, 4], header: [...L.DAILY.header] });
  assert.equal(D.explainCdbDiffs(raw, { compareIndex: new Map(), byKey: new Map() }).verdict, 'pass');
  assert.equal(D.explainCdbDiffs(raw, { compareIndex: new Map(), byKey: new Map(), neRows: neCsv.rows }).verdict, 'fail');
});

await ta('[2b] Company DB 待ち: ロジザードにあり Company DB に無い新しい商品のうち、照合 ② が「NE だけにある・lag」と言うもの = 出さない・不正に数えない / lag でない・照合 ② に無い = 不正 (cdb_no_sku) (2026-09-29)', async () => {
  const idx = D.compareNeIndex(cmpJson([['lag-1', 'exists', null, null, 'lag'], ['unx-1', 'exists', null, null, 'unexplained'], ['val-1', 'name', 'a', 'b', 'lag']]));
  const r = D.classifyForLz({ neItems: [neItem('Lag-1'), neItem('Unx-1'), neItem('Val-1'), neItem('Nosku-1')], cdb: cdbOf([]), lz: lzOf(['Lag-1', 'Unx-1', 'Val-1', 'Nosku-1']), cdbLag: D.cdbLagOf(idx) });
  assert.deepEqual(r.lagging.map((x) => [x.ne_code, x.why]), [['Lag-1', 'cdb_lag']]);
  assert.deepEqual(r.invalid.map((x) => [x.code_norm, x.reason]), [['unx-1', 'cdb_no_sku'], ['val-1', 'cdb_no_sku'], ['nosku-1', 'cdb_no_sku']]);   // 列の lag (名前) は「無い」の lag ではない
  assert.deepEqual([r.counts.lagging, r.counts.invalid, r.counts.compare], [1, 3, 0]);
  // ロジザードに無い = 新商品待ちが先 (lag でも)
  const a = D.classifyForLz({ neItems: [neItem('Lag-1')], cdb: cdbOf([]), lz: lzOf(['X-1']), cdbLag: D.cdbLagOf(idx) });
  assert.deepEqual([a.awaiting.length, a.lagging.length], [1, 0]);
  // NE の名前が空 (コードで補った名前の疑い) = lag でも待ちにしない = 不正 (Codex #1527 R1 High)
  const idx2 = D.compareNeIndex(cmpJson([['blank-9', 'exists', null, null, 'lag']]));
  assert.deepEqual(D.classifyForLz({ neItems: [neItem('Blank-9', '  ')], cdb: cdbOf([]), lz: lzOf(['Blank-9']), cdbLag: D.cdbLagOf(idx2) }).invalid.map((x) => x.reason), ['cdb_no_sku']);
  // cdbLag を渡さない = 今までどおり不正
  assert.deepEqual(D.classifyForLz({ neItems: [neItem('Lag-1')], cdb: cdbOf([]), lz: lzOf(['Lag-1']) }).invalid.map((x) => x.reason), ['cdb_no_sku']);
});

await ta('[2c] Company DB 待ちを置いてよいか: ロジザードの今の値が NE の道の値と同じ = held / 名前・原価の数・取引先が違う = stale / 形を決められない (取引先 "1")・一覧に無い = 不合格 (Codex #1527 R1 High)', async () => {
  const items = [neItem('H-1', '名前H', '10.00'), neItem('N-1', '新しい名前', '10.00'), neItem('C-1', '名前C', '200.00'), neItem('S-1', '名前S', '10.00', '0002'), neItem('U-1', '名前U', '10.00', '1'), neItem('M-1', '名前M')];
  const lz = lzOf(['H-1', 'N-1', 'C-1', 'S-1', 'U-1'], { 'H-1': { name: '名前H', cost: '10.00', sup: '0001' }, 'N-1': { name: '古い名前', cost: '10', sup: '0001' },
    'C-1': { name: '名前C', cost: '10', sup: '0001' }, 'S-1': { name: '名前S', cost: '10', sup: '0001' }, 'U-1': { name: '名前U', cost: '10', sup: '0001' } });
  const r = D.checkLagging({ rows: L.buildLzCsv(items, 'daily').rows, lz });
  assert.deepEqual(r.held.map((x) => x.ne_code), ['H-1']);   // 原価は数として同じ (10.00 と 10)
  assert.deepEqual(r.stale.map((x) => [x.ne_code, (x.diffs || []).map((d) => d.col).join('+') || x.why]).sort(), [['C-1', '仕入単価'], ['M-1', 'lz_missing'], ['N-1', '商品名'], ['S-1', '取引先id']]);
  assert.deepEqual(r.stale.find((x) => x.ne_code === 'C-1').diffs[0], { col: '仕入単価', ne: '200', lz: '10' });
  assert.deepEqual(r.undeterminable.map((x) => x.ne_code), ['U-1']);
  // ロジザードの原価が空 = 同じと言えない
  assert.deepEqual(D.checkLagging({ rows: L.buildLzCsv([neItem('E-1', '名前E', '0.00')], 'daily').rows, lz: lzOf(['E-1'], { 'E-1': { name: '名前E', cost: '' } }) }).stale.map((x) => x.ne_code), ['E-1']);
});

// ── CLI (runLzDaily) の材料: 一時の DATA_DIR ──
const now = new Date('2030-01-15T01:00:00Z');   // JST 2030-01-15 10:00
const asOf = '2030-01-15';
// lzTime = 一覧の保存の時刻 (JST 2030-01-15 00:30)・stamp = auto-shohin-csv.js の成功の印の中身・stampLagMs = 保存から印までの時間 (実測 45 秒)
function setup({ lzRows = [{ id: 'A-1', name: 'x' }, { id: 'B-2' }], lzTime = new Date('2030-01-14T15:30:00Z'), stamp = asOf, stampLagMs = 45000, compareItems = [['b-2', 'cost', 0, 5200]], evState = 'complete' } = {}) {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzd-test-'));
  const cj = Buffer.from(JSON.stringify(cmpJson(compareItems)), 'utf8');
  const rel = 'cdb-master-compare/2030-01-15/mc_20300115T000000000Z_aaaaaa.json';
  fs.mkdirSync(path.join(dataDir, path.dirname(rel)), { recursive: true });
  fs.writeFileSync(path.join(dataDir, rel), cj);
  writeEvidence(dataDir, 'master-compare', { state: evState, as_of: asOf, compare_run_id: 'mc_20300115T000000000Z_aaaaaa', json_path: rel, sha256: sha(cj) }, { now, warn: () => {} });
  const lzPath = path.join(dataDir, 'shohin_master.csv');
  fs.writeFileSync(lzPath, lzCsv(lzRows));
  fs.utimesSync(lzPath, lzTime, lzTime);
  const stampPath = path.join(dataDir, 'shohin-last-success.txt');
  if (stamp != null) { fs.writeFileSync(stampPath, stamp); const t = new Date(lzTime.getTime() + stampLagMs); fs.utimesSync(stampPath, t, t); }
  return { dataDir, lzPath, stampPath, compareRel: rel };
}
/** 前の日の完了の印 (半減の見張りの材料) */
const prevEv = (dir, day, rows) => writeEvidence(dir, 'lz-daily', { state: 'complete', as_of: day, inputs: { lz_master: { rows } } }, { now: new Date(`${day}T01:00:00Z`), warn: () => {} });
const neFake = (over = {}) => () => ({ ok: true, marks: { products: { at: '2030-01-14 22:01:00' }, sets: { at: '2030-01-14 22:01:02' } },
  products: [{ code: 'a-1', name: ' 商品A ', cost_src: J('100.00'), supplier: '0001' }, { code: 'b-2', name: '商品B', cost_src: J('0.00'), supplier: '0001' }, { code: 'new-1', name: '新', cost_src: J('1.00'), supplier: '0001' }],
  entries: [{ code_norm: 'a-1', kind: 'product', state: 'ok', ne_code: 'A-1' }, { code_norm: 'b-2', kind: 'product', state: 'ok', ne_code: 'B-2' }, { code_norm: 'new-1', kind: 'product', state: 'ok', ne_code: 'NEW-1' }],
  ...over });
const cdbFake = (over = {}) => async () => ({
  cdb: cdbOf([{ norm: 'a-1', name: '商品A', cost: 100, sups: ['0001'] }, { norm: 'b-2', name: '商品B', cost: 5200, sups: ['0001'] }, { norm: 'new-1', name: '新', cost: 1, sups: ['0001'] }]),
  mark: { compare_run_id: 'mc_20300115T000000000Z_aaaaaa' },
  codes: [{ code_norm: 'a-1', state: 'ok', ne_code: 'A-1' }, { code_norm: 'b-2', state: 'ok', ne_code: 'B-2' }, { code_norm: 'new-1', state: 'ok', ne_code: 'NEW-1' }], ...over });
const run = (s, extra = {}) => RUN.runLzDaily({ dataDir: s.dataDir, asOf, lzMasterPath: s.lzPath, lzStampPath: s.stampPath, now, readNe: neFake(), readCdb: cdbFake(), lzMinRows: 1, write: (d, n, p) => writeEvidence(d, n, p, { now, warn: () => {} }), ...extra });
const evOf = (dir) => JSON.parse(fs.readFileSync(path.join(dir, 'company-db-evidence', asOf, 'lz-daily.json'), 'utf8'));

await ta('[4] CLI: 作れた = 変えない CSV・報告・完了の印 (sha256・行数・期限・入力の世代)・合格 / 出す場所を分けられる / 2 回目は別の実行 ID', async () => {
  const s = setup();
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'lzd-out-'));
  const before = fs.readdirSync(s.dataDir, { recursive: true }).sort();
  const r = await run(s, { outDir: out });
  assert.equal(r.state, 'complete');
  assert.deepEqual(fs.readdirSync(s.dataDir, { recursive: true }).sort(), before);   // 材料の場所には何も書かない
  const ev = evOf(out);
  assert.equal(ev.verdict, 'pass', JSON.stringify(r.report.compare, null, 1).slice(0, 1500));
  assert.deepEqual([ev.state, ev.run_id, ev.fail_by], ['complete', r.runId, []]);
  assert.deepEqual([ev.counts.compare, ev.counts.awaiting, ev.counts.invalid, ev.allowed_by], [2, 1, 0, { name_trim: 2, compare_ne: 1 }]);
  assert.deepEqual([ev.inputs.lz_master.stamp.text, ev.inputs.lz_master.prev], [asOf, null]);   // 成功の印・前回なし (初回)
  const csv = fs.readFileSync(path.join(out, ev.csv.path));
  assert.deepEqual([ev.csv.sha256, ev.csv.rows, ev.deadline], [sha(csv), 2, '2030-01-16T01:00:00+09:00']);
  assert.equal(iconv.decode(csv, 'cp932').split('\r\n')[1], 'A-1,商品A,商品A,100,0001');   // Company DB の値 (名前は前後の空白を削った形)
  assert.equal(iconv.decode(csv, 'cp932').split('\r\n')[2], 'B-2,商品B,商品B,5200,0001');   // 照合 ② が知っている原価の差
  assert.deepEqual([ev.inputs.compare_run_id, ev.inputs.lz_master.rows, ev.inputs.lz_master.sha256], ['mc_20300115T000000000Z_aaaaaa', 2, sha(fs.readFileSync(s.lzPath))]);
  assert.ok(fs.existsSync(path.join(out, ev.report.path)));
  assert.equal(fs.existsSync(path.join(s.dataDir, 'company-db-evidence', asOf, 'lz-daily.json')), false);   // 材料の場所には書かない
  assert.match(r.line, /^✅ ロジザード毎日の商品マスタ \(影\): 合格 \/ 比べる 2・新商品待ち 1・Company DB 待ち 0 \(値が同じ 0\)・不正 0/);
  const r2 = await run(s, { outDir: out });
  assert.notEqual(r2.runId, r.runId);
  assert.equal(fs.readFileSync(path.join(out, ev.csv.path)).toString('hex'), csv.toString('hex'));   // 前の回の CSV は変わらない
});

await ta('[5] CLI: 不正が残る = 不合格 (説明できない差 0 でも) / 照合 ② に無い差 = 不合格', async () => {
  let s = setup();
  let r = await run(s, { readCdb: cdbFake({ codes: [{ code_norm: 'a-1', state: 'ok', ne_code: 'A-1' }, { code_norm: 'b-2', state: 'collided', ne_code: null }, { code_norm: 'new-1', state: 'ok', ne_code: 'NEW-1' }] }) });
  let ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.counts.invalid, ev.summary.unexplained], ['fail', ['invalid'], 1, 0]);
  assert.match(r.line, /^⚠️ .*不合格 \(invalid\).*不正 1/);
  s = setup({ compareItems: [] });   // B-2 の原価の差が照合 ② に無い
  r = await run(s);
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.summary.unexplained], ['fail', ['unexplained'], 1]);
  // ロジザードの一覧に NE の商品が 1 つも無い = 全部「新商品待ち」・比べる 0 = 何も確かめていない = 不合格 (Codex #1507 R1 High)
  s = setup({ lzRows: [{ id: 'X-9' }] });
  r = await run(s);
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.counts.compare, ev.counts.awaiting, ev.csv.rows], ['fail', ['no_compare'], 0, 3, 0]);
  // Company DB の原価 0 = そのまま出す (L-9 C)。NE も 0 = 同じ行 = 合格。ロジザードに 0 でない原価があれば報告に残す (合否は変えない)
  const zeroB = cdbFake({ cdb: cdbOf([{ norm: 'a-1', name: '商品A', cost: 100, sups: ['0001'] }, { norm: 'b-2', name: '商品B', cost: 0, sups: ['0001'] }, { norm: 'new-1', name: '新', cost: 1, sups: ['0001'] }]) });
  s = setup();
  r = await run(s, { readCdb: zeroB });
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.counts.invalid, ev.counts.cost_zero, ev.counts.cost_zero_over_lz, ev.csv.rows], ['pass', 0, 1, 0, 2]);
  assert.equal(iconv.decode(fs.readFileSync(path.join(s.dataDir, ev.csv.path)), 'cp932').split('\r\n')[2], 'B-2,商品B,商品B,0,0001');
  s = setup({ lzRows: [{ id: 'A-1', name: 'x' }, { id: 'B-2', cost: '5200' }] });
  r = await run(s, { readCdb: zeroB });
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.counts.cost_zero_over_lz], ['pass', 1]);
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(s.dataDir, ev.report.path), 'utf8')).classes.cost_zero_over_lz, [{ code_norm: 'b-2', ne_code: 'B-2', lz_cost: '5200' }]);
});

await ta('[5c] CLI: 新しい商品が Company DB にまだ無い = 照合 ② が lag と言えば Company DB 待ち (合格・CSV に出さない) / 言わなければ不正 (不合格) (2026-09-29 の 23 件)', async () => {
  const ne2 = () => { const n = neFake()(); n.products = [...n.products, { code: 'new-2', name: '新しい', cost_src: J('10.00'), supplier: '0001' }];
    n.entries = [...n.entries, { code_norm: 'new-2', kind: 'product', state: 'ok', ne_code: 'NEW-2' }]; return n; };
  const cdb2 = cdbFake({ codes: [{ code_norm: 'a-1', state: 'ok', ne_code: 'A-1' }, { code_norm: 'b-2', state: 'ok', ne_code: 'B-2' }, { code_norm: 'new-1', state: 'ok', ne_code: 'NEW-1' }, { code_norm: 'new-2', state: 'ok', ne_code: 'NEW-2' }] });
  const lagCmp = [['b-2', 'cost', 0, 5200], ['new-2', 'exists', null, null, 'lag']];
  let s = setup({ lzRows: [{ id: 'A-1', name: 'x' }, { id: 'B-2' }, { id: 'NEW-2', name: '新しい', cost: '10', sup: '0001' }], compareItems: lagCmp });
  let r = await run(s, { readNe: ne2, readCdb: cdb2 });
  let ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.counts.lagging, ev.counts.lagging_held, ev.counts.invalid, ev.csv.rows], ['pass', [], 1, 1, 0, 2]);
  assert.match(r.line, /Company DB 待ち 1 \(値が同じ 1\)・不正 0/);
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(s.dataDir, ev.report.path), 'utf8')).classes.lagging, [{ code_norm: 'new-2', ne_code: 'NEW-2', why: 'cdb_lag' }]);
  // 待ちでも、ロジザードの原価が NE と違う (取り込まないと古いまま) = 不合格 (Codex #1527 R1 High)
  s = setup({ lzRows: [{ id: 'A-1', name: 'x' }, { id: 'B-2' }, { id: 'NEW-2', name: '新しい', cost: '5', sup: '0001' }], compareItems: lagCmp });
  r = await run(s, { readNe: ne2, readCdb: cdb2 });
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.counts.lagging_stale], ['fail', ['lagging_stale'], 1]);
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(s.dataDir, ev.report.path), 'utf8')).classes.lagging_check.stale[0].diffs, [{ col: '仕入単価', ne: '10', lz: '5' }]);
  // 待ちでも、NE の道の形を決められない (取引先 "1") = 不合格
  const ne3 = () => { const n = ne2(); n.products = n.products.map((p) => (p.code === 'new-2' ? { ...p, supplier: '1' } : p)); return n; };
  s = setup({ lzRows: [{ id: 'A-1', name: 'x' }, { id: 'B-2' }, { id: 'NEW-2', name: '新しい', cost: '10', sup: '0001' }], compareItems: lagCmp });
  r = await run(s, { readNe: ne3, readCdb: cdb2 });
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.counts.lagging_undeterminable], ['fail', ['lagging_undeterminable'], 1]);
  s = setup({ lzRows: [{ id: 'A-1', name: 'x' }, { id: 'B-2' }, { id: 'NEW-2', name: '新しい', cost: '10', sup: '0001' }] });   // 照合 ② が lag と言わない
  r = await run(s, { readNe: ne2, readCdb: cdb2 });
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.counts.lagging, ev.counts.invalid], ['fail', ['invalid'], 0, 1]);
});

await ta('[5b] CLI: NE の道の推測の形は判定できない = 不合格 (NE の仕入先 "1" が Company DB の 0001 と同じになっても)', async () => {
  const s = setup();
  const readNe = () => { const n = neFake()(); n.products = n.products.map((p) => (p.code === 'a-1' ? { ...p, supplier: '1' } : p)); return n; };
  await run(s, { readNe });
  const ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.fail_by, ev.summary.undeterminable, ev.summary.unexplained], ['fail', ['undeterminable'], 1, 0]);
});

await ta('[6] CLI: 材料が欠ける = 作らない (⏭️・理由つき・CSV なし)', async () => {
  const cases = [
    ['compare_not_complete', setup({ evState: 'running' }), {}],
    ['ne_not_today', setup(), { readNe: neFake({ marks: { products: { at: '2030-01-13 22:01:00' }, sets: { at: '2030-01-13 22:01:02' } } }) }],
    ['lz_master_not_today', setup({ lzTime: new Date('2030-01-13T15:30:00Z') }), {}],
    ['codes_not_this_run', setup(), { readCdb: cdbFake({ mark: { compare_run_id: 'mc_20300114T000000000Z_bbbbbb' } }) }],
    ['ne_no_ne_marks', setup(), { readNe: () => ({ ok: false, reason: 'no_ne_marks' }) }],
  ];
  for (const [reason, s, extra] of cases) {
    const r = await run(s, extra);
    assert.deepEqual([r.state, r.reason], ['skipped', reason], reason);
    assert.equal(evOf(s.dataDir).state, 'skipped');
    assert.equal(fs.existsSync(path.join(s.dataDir, 'lz-daily')), false, reason);
    assert.match(r.line, /^⏭️ /);
  }
  // 全件 JSON が書き換わった = sha256 が合わない
  let s = setup();
  fs.appendFileSync(path.join(s.dataDir, s.compareRel), ' ');
  assert.equal((await run(s)).reason, 'compare_json_mismatch');
  // ロジザードの一覧が無い・見出しが違う
  s = setup();
  assert.equal((await run(s, { lzMasterPath: path.join(s.dataDir, 'nothing.csv') })).reason, 'lz_master_missing');
  fs.writeFileSync(s.lzPath, sj('"商品ID"\r\n"A-1"')); fs.utimesSync(s.lzPath, new Date('2030-01-14T15:30:00Z'), new Date('2030-01-14T15:30:00Z'));
  assert.equal((await run(s)).reason, 'lz_master_header');
  // 商品ID が空の行
  assert.equal((await run(setup({ lzRows: [{ id: 'A-1' }, { id: '' }] }))).reason, 'lz_master_blank_id');
  // 成功の印 (Codex #1507 R1 Medium): 無い / 前の日 / 一覧の前に書かれた (その後の書き出しは成功していない) / 一覧の 15 分より後 (別の回) = 作らない
  assert.equal((await run(setup({ stamp: null }))).reason, 'lz_stamp_missing');
  assert.equal((await run(setup({ stamp: '2030-01-14' }))).reason, 'lz_export_not_confirmed');
  assert.equal((await run(setup({ stampLagMs: -1000 }))).reason, 'lz_export_not_confirmed');
  assert.equal((await run(setup({ stampLagMs: RUN.STAMP_MAX_LAG_MS + 1000 }))).reason, 'lz_export_not_confirmed');
  assert.equal((await run(setup({ stampLagMs: RUN.STAMP_MAX_LAG_MS }))).state, 'complete');   // 境目 = よい
  assert.equal((await run(setup({ stamp: `${asOf}\r\n` }))).state, 'complete');   // 印の末尾の改行は見ない
  // 前回の半分より少ない (v2 H2) = 作らない。前回 = 7 日以内でいちばん近い日の完了の印
  s = setup(); prevEv(s.dataDir, '2030-01-14', 5);
  assert.deepEqual([(await run(s)).reason, evOf(s.dataDir).lz_master.prev.rows], ['lz_master_shrunk', 5]);   // 2 行 × 2 < 5
  s = setup(); prevEv(s.dataDir, '2030-01-14', 5);
  const tryOut = fs.mkdtempSync(path.join(os.tmpdir(), 'lzd-out-'));
  assert.equal((await run(s, { outDir: tryOut })).reason, 'lz_master_shrunk');   // 出す場所を分けた手の試しでも本番の履歴で見る (Codex #1507 R2 Medium)
  s = setup(); prevEv(s.dataDir, '2030-01-14', 4);
  assert.equal((await run(s)).state, 'complete');   // ちょうど半分 = よい
  s = setup(); prevEv(s.dataDir, '2030-01-13', 100); prevEv(s.dataDir, '2030-01-14', 4);
  assert.equal((await run(s)).state, 'complete');   // いちばん近い日 (01-14) を使う
  s = setup(); prevEv(s.dataDir, '2030-01-07', 100);
  assert.equal((await run(s)).state, 'complete');   // 8 日前 = 見ない
  s = setup(); prevEv(s.dataDir, '2030-01-08', 100);
  assert.equal((await run(s)).reason, 'lz_master_shrunk');   // 7 日前 = 見る
  s = setup();
  writeEvidence(s.dataDir, 'lz-daily', { state: 'running', as_of: '2030-01-14', inputs: { lz_master: { rows: 100 } } }, { now: new Date('2030-01-14T01:00:00Z'), warn: () => {} });
  assert.equal((await run(s)).state, 'complete');   // 完了していない回の印は前回にしない
});

await ta('[8] 証跡: 始めに running (前の回の完了の印を無効にする) / 書けない = 作ること自体の失敗 (throw = ❌)・前の合格を残さない', async () => {
  const quiet = (d, n, p) => writeEvidence(d, n, p, { now, warn: () => {} });
  let s = setup();
  await assert.rejects(run(s, { write: () => null }), /証跡 lz-daily を書けない/);
  assert.equal(fs.existsSync(path.join(s.dataDir, 'lz-daily')), false);   // running を書けない = 何も作らない
  // 1 回目は合格 → 2 回目の完了の印だけ書けない = 失敗・印は running (1 回目の合格は残らない)
  s = setup();
  await run(s);
  assert.equal(evOf(s.dataDir).verdict, 'pass');
  await assert.rejects(run(s, { write: (d, n, p) => (p.state === 'complete' ? null : quiet(d, n, p)) }), /書けない/);
  assert.deepEqual([evOf(s.dataDir).state, evOf(s.dataDir).verdict], ['running', undefined]);
  // 途中の失敗 (Company DB が読めない) = running のまま
  s = setup();
  await run(s);
  await assert.rejects(run(s, { readCdb: async () => { throw new Error('db down'); } }), /db down/);
  assert.equal(evOf(s.dataDir).state, 'running');
  // 作らない (⏭️) の印を書けない = 失敗
  s = setup({ evState: 'running' });
  await assert.rejects(run(s, { write: (d, n, p) => (p.state === 'skipped' ? null : quiet(d, n, p)) }), /書けない/);
  // 作らない回の印にも実行 ID
  s = setup({ evState: 'running' });
  const r = await run(s);
  assert.deepEqual([evOf(s.dataDir).state, evOf(s.dataDir).run_id], ['skipped', r.runId]);
  // runLzDaily を呼ばずに作らない回 (未設定) も、前の回の完了の印を無効にする。書けない = throw (Codex #1507 R2 High)
  s = setup();
  await run(s);
  const id = RUN.markSkipped({ outDir: s.dataDir, asOf, reason: 'not_configured', now, write: quiet });
  assert.deepEqual([evOf(s.dataDir).state, evOf(s.dataDir).reason, evOf(s.dataDir).run_id, evOf(s.dataDir).verdict], ['skipped', 'not_configured', id, undefined]);
  assert.throws(() => RUN.markSkipped({ outDir: s.dataDir, asOf, reason: 'not_configured', now, write: () => null }), /証跡 lz-daily を書けない/);
});

await ta('[9] 監視への報告: 作れた回だけ ok・作らない・失敗は fail (理由つき) / 口は ping.ps1 と同じ (クエリの status・Bearer・https だけ) / 報告の失敗でステップを落とさない', async () => {
  assert.deepEqual(RUN.pingFor(RUN.EXIT.complete, '⚠️ ロジザード毎日の商品マスタ (影): 不合格'), { status: 'ok', note: '⚠️ ロジザード毎日の商品マスタ (影): 不合格' });
  assert.equal(RUN.pingFor(RUN.EXIT.skipped, '⏭️ x').status, 'fail');
  assert.equal(RUN.pingFor(RUN.EXIT.error, '❌ x').status, 'fail');
  assert.equal(RUN.pingFor(0, 'a'.repeat(500)).note.length, 180);
  assert.deepEqual([RUN.EXIT.complete, RUN.EXIT.error, RUN.EXIT.skipped], [0, 1, 3]);   // 2 = 朝の再試行が「通知済み・打ち切り」と読む = 使わない
  const calls = [];
  const fetchImpl = async (url, opt) => { calls.push({ url, opt }); return { ok: true, status: 200 }; };
  const env = { JOBS_MONITOR_TOKEN: 'tok', JOBS_MONITOR_URL: 'https://jobs.example.test/some/path' };
  assert.equal(await RUN.sendPing(RUN.JOB_ID, { status: 'fail', note: '⏭️ 作らない (lz_stamp_missing)' }, { env, fetchImpl }), true);
  const u = new URL(calls[0].url);
  assert.deepEqual([u.origin + u.pathname, u.searchParams.get('status'), u.searchParams.get('note'), calls[0].opt.method, calls[0].opt.headers.Authorization],
    ['https://jobs.example.test/apps/jobs-monitor/ping/lz-daily-build', 'fail', '⏭️ 作らない (lz_stamp_missing)', 'POST', 'Bearer tok']);
  const warns = [];
  assert.equal(await RUN.sendPing(RUN.JOB_ID, { status: 'ok' }, { env: { JOBS_MONITOR_URL: env.JOBS_MONITOR_URL }, fetchImpl }), false);   // トークンが無い = 送らない
  assert.equal(await RUN.sendPing(RUN.JOB_ID, { status: 'ok' }, { env: { ...env, JOBS_MONITOR_URL: 'http://jobs.example.test' }, fetchImpl }), false);   // https でない
  assert.equal(calls.length, 1);
  assert.equal(await RUN.sendPing(RUN.JOB_ID, { status: 'ok' }, { env, fetchImpl: async () => ({ ok: false, status: 400 }), warn: (m) => warns.push(m) }), false);
  assert.equal(await RUN.sendPing(RUN.JOB_ID, { status: 'ok' }, { env, fetchImpl: async () => { throw new Error('offline'); }, warn: (m) => warns.push(m) }), false);
  assert.equal(warns.length, 2);
  // 見張りの台帳に無い id (registered: false) = 締切で見てもらえない = 送れたと数えない (Codex #1558 R2 Medium)
  assert.equal(await RUN.sendPing('lz-no-such-job', { status: 'ok' }, { env, fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ ok: true, registered: false }) }), warn: (m) => warns.push(m) }), false);
  assert.match(warns.at(-1), /lz-no-such-job が Render の見張りの台帳に無い/);
  assert.equal(await RUN.sendPing(RUN.JOB_ID, { status: 'ok' }, { env, fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ ok: true, registered: true }) }) }), true);
  assert.equal(await RUN.sendPing(RUN.JOB_ID, { status: 'ok' }, { env, fetchImpl: async () => ({ ok: true, status: 200, json: async () => { throw new Error('not json'); } }) }), true, '本文が読めない = 今までどおり');
  // 台帳: 作るステップの項目 (v2 M9・v3 M6) = 毎日 07:00 の daily-sync・締切まで ok が無ければ気づく
  const { JOBS_REGISTRY, validateRegistry } = await import('../config/jobs-registry.mjs');
  const { evaluateEntry } = await import('../apps/jobs-monitor/evaluate.js');
  const def = JOBS_REGISTRY.find((e) => e.id === RUN.JOB_ID);
  assert.ok(def);
  assert.deepEqual([def.type, validateRegistry([def])], ['scheduled_job', []]);
  const seen = Date.UTC(2030, 0, 14, 0, 0, 0), at = (iso, st = {}) => evaluateEntry(def, { firstSeenAtMs: seen, ...st }, Date.parse(iso)).status;
  assert.equal(at('2030-01-15T05:30:00Z', { lastOkAtMs: Date.parse('2030-01-14T23:10:00Z') }), 'ok');   // 当日 08:10 JST に作れた
  assert.equal(at('2030-01-15T05:30:00Z', { lastOkAtMs: Date.parse('2030-01-13T23:10:00Z') }), 'late');   // 14:30 JST まで今日の ok が無い (作らない朝)
});

await ta('[10] CLI の終わり方: 未設定 = ⏭️ exit 3 (daily-sync と朝の再試行では失敗)・引数の誤り = ❌ exit 1 / daily-sync と朝の再試行は --daily で呼ぶ', async () => {
  const { spawnSync } = await import('node:child_process');
  const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'lzd-cli-'));
  const cli = (args) => spawnSync(process.execPath, ['scripts/company-db/lz-daily.mjs', ...args], { cwd: ROOT, encoding: 'utf8', env: { ...process.env, COMPANY_DB_WATCH_URL: '', JOBS_MONITOR_TOKEN: '' } });
  let c = cli(['--daily', '--data-dir', tmp, '--as-of', '2030/01/15']);
  assert.equal(c.status, 1);
  assert.match(c.stdout.trim().split('\n').pop(), /^❌ /);
  assert.equal(fs.readdirSync(tmp).length, 0);   // 引数の誤り = 何も書かない
  // 同じ日に合格した後、未設定で流れた = 前の合格を無効にする (Codex #1507 R2 High)
  const today = new Date();
  writeEvidence(tmp, 'lz-daily', { state: 'complete', verdict: 'pass', as_of: 'x' }, { now: today, warn: () => {} });
  c = cli(['--daily', '--data-dir', tmp]);
  assert.equal(c.status, 3, c.stderr);
  assert.match(c.stdout.trim().split('\n').pop(), /^⏭️ ロジザード毎日の商品マスタ \(影\): 作らない \(未設定 COMPANY_DB_WATCH_URL\)$/);
  const ev = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', (await import('../lib/jst-date.js')).jstDateStr(today), 'lz-daily.json'), 'utf8'));
  assert.deepEqual([ev.state, ev.reason, ev.verdict], ['skipped', 'not_configured', undefined]);
  assert.equal(fs.existsSync(path.join(tmp, 'lz-daily')), false);
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8'), rt = fs.readFileSync(path.join(ROOT, 'apps/warehouse/retry-failed-jobs.js'), 'utf8');
  assert.match(ds, /runScript\('scripts\/company-db\/lz-daily\.mjs --daily', 'ロジザード毎日の商品マスタ\(影\)'/);
  assert.match(rt, /'ロジザード毎日の商品マスタ\(影\)': \{ script: 'scripts\/company-db\/lz-daily\.mjs', args: \['--daily'\]/);
});

await ta('[7] Company DB の読み手: 1 つの読み取りの取引 (repeatable read) で値・元のコード・印を読む', async () => {
  const pg = new PGlite(); const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: () => {} });
  let r = await RUN.readCdbForLz(db);
  assert.deepEqual([r.mark, r.codes.length, r.cdb.skuByNorm.size], [null, 0, 0]);
  await db.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ('mc_20300115T000000000Z_aaaaaa', '2030-01-15T00:00:00Z', 0)`);
  await db.query('select ops.record_ne_codes($1::jsonb)', [J({ compare_run_id: 'mc_20300115T000000000Z_aaaaaa', entries: [{ code_norm: 'a-1', kind: 'product', state: 'ok', ne_code: 'A-1', spellings: ['A-1'] }, { code_norm: 'a-1', kind: 'rep', state: 'ok', ne_code: 'A-1', spellings: ['A-1'] }] })]);
  r = await RUN.readCdbForLz(db);
  assert.deepEqual([r.mark.compare_run_id, r.codes], ['mc_20300115T000000000Z_aaaaaa', [{ code_norm: 'a-1', state: 'ok', ne_code: 'A-1' }]]);
  const seen = [];
  const fake = { query: async (sql) => { seen.push(sql.trim().split(/\s+/).slice(0, 2).join(' ')); if (/information_schema|to_regclass/.test(sql)) return { rows: [{ exists: false, ok: false }] }; return { rows: [] }; } };
  await RUN.readCdbForLz(fake).catch(() => {});
  assert.equal(seen[0], 'begin transaction');
  assert.equal(seen[seen.length - 1], 'commit');
});

await ta('[11] 成果物をポータルへ送る (③c-1b-3b-3・K3-1): 作った CSV がポータルの確かめを通る (本物の状態の機械)・受け取れた = 成功 / 2 回目 = 受け取り済み / 届かない・5xx = 待って 3 回まで・4xx = すぐ失敗 / 送れない = ❌ exit 1 と証跡の portal / CSV が証跡と違う = 送らない / 口の答えの識別が違う = 失敗', async () => {
  const s = setup();
  const r = await run(s);
  assert.equal(r.state, 'complete');
  const S = await import('../apps/logizard-import-state/store.js');
  const db = S.openImportStateDb(':memory:');
  S.init(db, { by: 'x' });
  const calls = [];
  const real = { putArtifact: async (m) => { calls.push(m); return { ok: true, ...S.putArtifact(db, { sourceRunId: m.sourceRunId, targetAsOf: m.targetAsOf, verdict: m.verdict, csvBuf: m.csvBuf, sha256: m.sha256, rows: m.rows, by: m.by }) }; } };
  const w = (d, n, p) => writeEvidence(d, n, p, { now, warn: () => {} });
  const fast = (o) => RUN.sendArtifact({ ...o, sleep: async () => {} });
  let f = await RUN.afterBuild({ outDir: s.dataDir, r, makeClient: () => real, now, write: w, send: fast });
  assert.equal(f.code, RUN.EXIT.complete);
  assert.match(f.line, /^✅ ロジザード毎日の商品マスタ \(影\): 合格 .* \/ ポータル 受け取った$/);
  const art = S.getArtifact(db, { sourceRunId: r.runId });
  assert.deepEqual([art.csv_sha256, art.rows, art.verdict, art.target_as_of, art.received_by], [r.evidence.csv.sha256, r.evidence.csv.rows, 'pass', asOf, 'lz-daily']);
  let ev = evOf(s.dataDir);
  assert.deepEqual([ev.state, ev.run_id, ev.portal.ok, ev.portal.stored, ev.portal.tries], ['complete', r.runId, true, true, 1]);
  f = await RUN.afterBuild({ outDir: s.dataDir, r, makeClient: () => real, now, write: w, send: fast });
  assert.match(f.line, /ポータル 受け取り済み$/);
  // 届かない 2 回 → 3 回目で届く = 成功 / 3 回とも = 失敗 / 5xx も待つ / 4xx はすぐ
  const err = (code, status = null) => Object.assign(new Error(code), { code, status });
  let n = 0;
  const flaky = { putArtifact: async (m) => { n++; if (n < 3) throw err('unreachable'); return real.putArtifact(m); } };
  assert.deepEqual(await fast({ outDir: s.dataDir, evidence: r.evidence, client: flaky }), { ok: true, stored: false, same: true, tries: 3 });
  const always = (e) => { let k = 0; return { client: { putArtifact: async () => { k++; throw e; } }, count: () => k }; };
  for (const [e, want, tries] of [[err('unreachable'), 'unreachable', 3], [err('internal', 500), 'internal_500', 3], [err('conflict', 409), 'conflict_409', 1], [err('mismatch', 400), 'mismatch_400', 1]]) {
    const c = always(e);
    const x = await fast({ outDir: s.dataDir, evidence: r.evidence, client: c.client });
    assert.deepEqual([x.ok, x.error, x.tries, c.count()], [false, want, tries, tries], want);
  }
  const down = always(err('unreachable'));
  f = await RUN.afterBuild({ outDir: s.dataDir, r, makeClient: () => down.client, now, write: w, send: fast });
  assert.equal(f.code, RUN.EXIT.error);
  assert.match(f.line, /^❌ ロジザード毎日の商品マスタ \(影\): 成果物をポータルに送れない \(unreachable\) = 成功にしない \/ ✅/);
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.state, ev.portal.ok, ev.portal.error], ['complete', false, 'unreachable']);
  assert.equal(RUN.pingFor(f.code, f.line).status, 'fail');
  // 証跡を書けない = 成功にしない
  f = await RUN.afterBuild({ outDir: s.dataDir, r, makeClient: () => real, now, write: () => false, send: fast });
  assert.deepEqual([f.code, /証跡 lz-daily を書けない/.test(f.line)], [RUN.EXIT.error, true]);
  // 口の答えの識別が違う = 失敗
  const good = { source_run_id: r.runId, target_as_of: asOf, verdict: 'pass', csv_sha256: r.evidence.csv.sha256, rows: r.evidence.csv.rows };
  for (const bad of [{ csv_sha256: 'x'.repeat(64) }, { source_run_id: 'lzd_other' }, { target_as_of: '2030-01-14' }, { verdict: 'fail' }, { rows: 99 }]) {
    const liar = { putArtifact: async () => ({ ok: true, stored: true, ...good, ...bad }) };
    assert.deepEqual((await fast({ outDir: s.dataDir, evidence: r.evidence, client: liar })).error, 'portal_answer_mismatch', JSON.stringify(bad));
  }
  assert.equal((await fast({ outDir: s.dataDir, evidence: r.evidence, client: { putArtifact: async () => ({ ok: true, stored: true, ...good }) } })).ok, true);
  // 口の用意の失敗 (LZ_LOCK_TOKEN が無い) = 送れない・証跡に残す (Codex #1540 R1 Medium)
  f = await RUN.afterBuild({ outDir: s.dataDir, r, makeClient: () => { throw Object.assign(new Error('LZ_LOCK_TOKEN が無い'), { code: 'no_token' }); }, now, write: w, send: fast });
  assert.deepEqual([f.code, /送れない \(no_token\)/.test(f.line), evOf(s.dataDir).portal.error], [RUN.EXIT.error, true, 'no_token']);
  // CSV が証跡と違う (書き換わった・消えた) = 送らない
  const before = calls.length;
  const csvPath = path.join(s.dataDir, r.evidence.csv.path);
  const orig = fs.readFileSync(csvPath);
  fs.chmodSync(csvPath, 0o666);
  fs.writeFileSync(csvPath, Buffer.concat([orig, Buffer.from('x')]));
  assert.deepEqual(await fast({ outDir: s.dataDir, evidence: r.evidence, client: real }), { ok: false, error: 'csv_changed' });
  fs.rmSync(csvPath);
  assert.deepEqual(await fast({ outDir: s.dataDir, evidence: r.evidence, client: real }), { ok: false, error: 'csv_missing' });
  assert.equal(calls.length, before);
});

await ta('[12] 入口 (cli): daily-sync の回 (--daily・--out-dir なし) だけ作れた後に送る / LZ_LOCK_TOKEN が無い・Render が落ちている = ❌ exit 1・fail の ping・証跡の portal / --out-dir・--daily なし = 送らない・ping しない / 送れた = ✅ exit 0・ok の ping (Codex #1540 R1 Medium)', async () => {
  const S = await import('../apps/logizard-import-state/store.js');
  const go = async (argv, { client = undefined, makeClient = undefined, outDir = null } = {}) => {
    const s = setup();
    const pings = [], logs = [];
    const res = await RUN.cli([...argv, ...(outDir ? ['--out-dir', outDir] : [])], {
      env: { DATA_DIR: s.dataDir, COMPANY_DB_WATCH_URL: 'postgres://fake', LZ_SHOHIN_MASTER_PATH: s.lzPath, LZ_SHOHIN_STAMP_PATH: s.stampPath },
      now, runBuild: (o) => RUN.runLzDaily({ ...o, readNe: neFake(), readCdb: cdbFake(), lzMinRows: 1 }),
      makeClient: makeClient || (() => client), ping: async (id, p) => { pings.push([id, p.status]); }, log: (x) => logs.push(x),
      write: (d, n, p) => writeEvidence(d, n, p, { now, warn: () => {} }), sleep: async () => {},
    });
    return { ...res, pings, logs, s };
  };
  const db = S.openImportStateDb(':memory:');
  S.init(db, { by: 'x' });
  const sent = [];
  const real = { putArtifact: async (m) => { sent.push(m.sourceRunId); return { ok: true, ...S.putArtifact(db, { sourceRunId: m.sourceRunId, targetAsOf: m.targetAsOf, verdict: m.verdict, csvBuf: m.csvBuf, sha256: m.sha256, rows: m.rows, by: m.by }) }; } };
  // 送れた = ✅ exit 0・ok の ping・証跡の portal
  let x = await go(['--daily', '--as-of', asOf], { client: real });
  assert.deepEqual([x.code, x.pings, sent.length, /ポータル 受け取った$/.test(x.last), evOf(x.s.dataDir).portal.ok, evOf(x.s.dataDir).version], [0, [['lz-daily-build', 'ok']], 1, true, true, 'lzd-v3']);
  // LZ_LOCK_TOKEN が無い = 口を作れない = ❌ exit 1・fail の ping・証跡の portal.error
  x = await go(['--daily', '--as-of', asOf], { makeClient: () => { throw Object.assign(new Error('LZ_LOCK_TOKEN が無い'), { code: 'no_token' }); } });
  assert.deepEqual([x.code, x.pings, /^❌ .*送れない \(no_token\)/.test(x.last), evOf(x.s.dataDir).state, evOf(x.s.dataDir).portal.error], [1, [['lz-daily-build', 'fail']], true, 'complete', 'no_token']);
  // Render が落ちている = 3 回送って ❌ exit 1
  let tries = 0;
  x = await go(['--daily', '--as-of', asOf], { client: { putArtifact: async () => { tries++; throw Object.assign(new Error('x'), { code: 'unreachable', status: null }); } } });
  assert.deepEqual([x.code, x.pings, tries, evOf(x.s.dataDir).portal.error], [1, [['lz-daily-build', 'fail']], 3, 'unreachable']);
  // 手の試し (--out-dir)・--daily なし = 送らない・ping しない (作れた = exit 0)
  const before = sent.length;
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'lzd-cli-'));
  x = await go(['--daily', '--as-of', asOf], { client: real, outDir: out });
  assert.deepEqual([x.code, x.pings, sent.length], [0, [], before]);
  x = await go(['--as-of', asOf], { client: real });
  assert.deepEqual([x.code, x.pings, sent.length, 'portal' in evOf(x.s.dataDir)], [0, [], before, false]);
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
