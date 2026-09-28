/**
 * test-lz-daily.mjs — ロジザードの毎日の商品マスタを Company DB の値で作る (③c-1a。apps/master-decisions/lz-cdb.mjs・scripts/company-db/lz-daily.mjs)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c 契約 v1〜v3」・中原さんの答え L-4〜L-8):
 *   1 ロジザードの全件の一覧: 見出しは実ファイルの 1 行目・壊れた / 列の数が違う / 少なすぎる / 同じ ID が 2 つ = 使わない
 *   2 3 つに分ける: 比べる (ロジザードにある・値が全部そろう) / 新商品待ち (ロジザードに無い) / 不正 (理由つき。0 や空で埋めない)
 *   3 差の説明: 名前の前後の空白 (L-4) と、照合 ② の差の一覧に同じ商品・列・両側の値で載る差だけ許す
 *   4 材料の条件: その朝の照合 (complete・sha256)・NE の取得がその朝・一覧がその日・元のコードの印がその回。欠ける = 作らない
 *   5 出すもの: 変えない CSV・報告・完了の印 (sha256・行数・期限)。不正が残れば合格にしない
 * 使い方: node scripts/test-lz-daily.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

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
  assert.deepEqual([r.ok, r.rows, r.byId.get('A-1'), r.byId.get('b-2').deleted], [true, 2, { name: '商品A', cost: '100', supplier: '0107', deleted: '0' }, '1']);
  assert.deepEqual(r.lowerGroups.get('a-1'), ['A-1']);
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }]), { minRows: 2 }).reason, 'lz_master_too_few');
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }])).reason, 'lz_master_too_few');   // 既定の下限 4,000
  assert.equal(D.readLzShohinMaster(sj('"商品ID","商品名"\r\n"A-1","x"'), { minRows: 1 }).reason, 'lz_master_header');
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }], [sj('\r\n"x","y"')]), { minRows: 1 }).reason, 'lz_master_row_width');
  assert.equal(D.readLzShohinMaster(lzCsv([{ id: 'A-1' }, { id: 'A-1' }]), { minRows: 1 }).reason, 'lz_master_duplicate_id');
  assert.equal(D.readLzShohinMaster(Buffer.concat([lzCsv([{ id: 'A-1' }]), sj('\r\n"x","y')]), { minRows: 1 }).reason, 'lz_master_broken');
  r = D.readLzShohinMaster(lzCsv([{ id: 'ABC' }, { id: 'abc' }]), { minRows: 1 });
  assert.deepEqual(r.lowerGroups.get('abc').sort(), ['ABC', 'abc']);
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
  // 原価 0 は今のロジザードと同じ値 = そのまま出す (無い・負・小数とは別)
  const z = D.classifyForLz({ neItems: [neItem('Z-1')], cdb: cdbOf([{ norm: 'z-1', name: 'z', cost: 0, sups: ['0001'] }]), lz: lzOf(['Z-1']) });
  assert.deepEqual(z.compare.map((x) => x.cdb.cost_text), ['0']);
});

/** 比べる商品から両方の道の CSV を作って比べ、差を説明する */
function compareBoth(items, compareJson) {
  const cdbCsv = L.buildLzCsv(items.map((x) => x.cdb), 'daily'), neCsv = L.buildLzCsv(items.map((x) => x.ne), 'daily');
  const raw = C.compareLz({ gas: neCsv.bytes, ours: cdbCsv, compareCols: [0, 1, 2, 3, 4], header: [...L.DAILY.header] });
  return D.explainCdbDiffs(raw, { compareIndex: D.compareNeIndex(compareJson), byKey: new Map(items.map((x) => [x.key, x])) });
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
});

// ── CLI (runLzDaily) の材料: 一時の DATA_DIR ──
const now = new Date('2030-01-15T01:00:00Z');   // JST 2030-01-15 10:00
const asOf = '2030-01-15';
function setup({ lzRows = [{ id: 'A-1', name: 'x' }, { id: 'B-2' }], lzTime = new Date('2030-01-14T15:30:00Z'), compareItems = [['b-2', 'cost', 0, 5200]], evState = 'complete' } = {}) {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzd-test-'));
  const cj = Buffer.from(JSON.stringify(cmpJson(compareItems)), 'utf8');
  const rel = 'cdb-master-compare/2030-01-15/mc_20300115T000000000Z_aaaaaa.json';
  fs.mkdirSync(path.join(dataDir, path.dirname(rel)), { recursive: true });
  fs.writeFileSync(path.join(dataDir, rel), cj);
  writeEvidence(dataDir, 'master-compare', { state: evState, as_of: asOf, compare_run_id: 'mc_20300115T000000000Z_aaaaaa', json_path: rel, sha256: sha(cj) }, { now, warn: () => {} });
  const lzPath = path.join(dataDir, 'shohin_master.csv');
  fs.writeFileSync(lzPath, lzCsv(lzRows));
  fs.utimesSync(lzPath, lzTime, lzTime);
  return { dataDir, lzPath, compareRel: rel };
}
const neFake = (over = {}) => () => ({ ok: true, marks: { products: { at: '2030-01-14 22:01:00' }, sets: { at: '2030-01-14 22:01:02' } },
  products: [{ code: 'a-1', name: ' 商品A ', cost_src: J('100.00'), supplier: '0001' }, { code: 'b-2', name: '商品B', cost_src: J('0.00'), supplier: '0001' }, { code: 'new-1', name: '新', cost_src: J('1.00'), supplier: '0001' }],
  entries: [{ code_norm: 'a-1', kind: 'product', state: 'ok', ne_code: 'A-1' }, { code_norm: 'b-2', kind: 'product', state: 'ok', ne_code: 'B-2' }, { code_norm: 'new-1', kind: 'product', state: 'ok', ne_code: 'NEW-1' }],
  ...over });
const cdbFake = (over = {}) => async () => ({
  cdb: cdbOf([{ norm: 'a-1', name: '商品A', cost: 100, sups: ['0001'] }, { norm: 'b-2', name: '商品B', cost: 5200, sups: ['0001'] }, { norm: 'new-1', name: '新', cost: 1, sups: ['0001'] }]),
  mark: { compare_run_id: 'mc_20300115T000000000Z_aaaaaa' },
  codes: [{ code_norm: 'a-1', state: 'ok', ne_code: 'A-1' }, { code_norm: 'b-2', state: 'ok', ne_code: 'B-2' }, { code_norm: 'new-1', state: 'ok', ne_code: 'NEW-1' }], ...over });
const run = (s, extra = {}) => RUN.runLzDaily({ dataDir: s.dataDir, asOf, lzMasterPath: s.lzPath, now, readNe: neFake(), readCdb: cdbFake(), lzMinRows: 1, write: (d, n, p) => writeEvidence(d, n, p, { now, warn: () => {} }), ...extra });
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
  assert.deepEqual([ev.counts.compare, ev.counts.awaiting, ev.counts.invalid, ev.allowed_by], [2, 1, 0, { name_trim: 2, compare_ne: 1 }]);
  const csv = fs.readFileSync(path.join(out, ev.csv.path));
  assert.deepEqual([ev.csv.sha256, ev.csv.rows, ev.deadline], [sha(csv), 2, '2030-01-15T23:59:59+09:00']);
  assert.equal(iconv.decode(csv, 'cp932').split('\r\n')[1], 'A-1,商品A,商品A,100,0001');   // Company DB の値 (名前は前後の空白を削った形)
  assert.equal(iconv.decode(csv, 'cp932').split('\r\n')[2], 'B-2,商品B,商品B,5200,0001');   // 照合 ② が知っている原価の差
  assert.deepEqual([ev.inputs.compare_run_id, ev.inputs.lz_master.rows, ev.inputs.lz_master.sha256], ['mc_20300115T000000000Z_aaaaaa', 2, sha(fs.readFileSync(s.lzPath))]);
  assert.ok(fs.existsSync(path.join(out, ev.report.path)));
  assert.equal(fs.existsSync(path.join(s.dataDir, 'company-db-evidence', asOf, 'lz-daily.json')), false);   // 材料の場所には書かない
  assert.match(r.line, /^✅ ロジザード毎日の商品マスタ \(影\): 合格 \/ 比べる 2・新商品待ち 1・不正 0/);
  const r2 = await run(s, { outDir: out });
  assert.notEqual(r2.runId, r.runId);
  assert.equal(fs.readFileSync(path.join(out, ev.csv.path)).toString('hex'), csv.toString('hex'));   // 前の回の CSV は変わらない
});

await ta('[5] CLI: 不正が残る = 不合格 (説明できない差 0 でも) / 照合 ② に無い差 = 不合格', async () => {
  let s = setup();
  let r = await run(s, { readCdb: cdbFake({ codes: [{ code_norm: 'a-1', state: 'ok', ne_code: 'A-1' }, { code_norm: 'b-2', state: 'collided', ne_code: null }, { code_norm: 'new-1', state: 'ok', ne_code: 'NEW-1' }] }) });
  let ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.counts.invalid, ev.summary.unexplained], ['fail', 1, 0]);
  assert.match(r.line, /^⚠️ .*不合格.*不正 1/);
  s = setup({ compareItems: [] });   // B-2 の原価の差が照合 ② に無い
  r = await run(s);
  ev = evOf(s.dataDir);
  assert.deepEqual([ev.verdict, ev.summary.unexplained], ['fail', 1]);
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

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
