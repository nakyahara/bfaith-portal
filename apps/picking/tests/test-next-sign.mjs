/**
 * NEXTサイン — 判定ロジック (next-sign-core.cjs) と面マスタ (location-faces.js) のテスト。
 * 同梱の初期CSV (45面) をそのまま使い、2026-08-23 に現場確認した9パターンを検算する。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-nextsign-'));

const core = (await import('../next-sign-core.cjs')).default;
const { initPickingDB, getDB } = await import('../db.js');
const {
  parseFacesCsv, validateFaces, importFaces, listFaces, getFaceIndex, exportFacesCsv,
  ensureLocationFacesSeeded, listFaceImports, DEFAULT_CSV_PATH,
} = await import('../location-faces.js');
const { PkError } = await import('../service.js');

initPickingDB();

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ✓ ${name}`); }
  catch (e) { console.error(`  ✗ ${name}\n    ${e.stack}`); process.exitCode = 1; }
}

// ─── 初期CSV ───

t('同梱CSVが 45面 / 503マス で検証を通る', () => {
  const faces = parseFacesCsv(fs.readFileSync(DEFAULT_CSV_PATH));
  const v = validateFaces(faces);
  assert.deepEqual(v.errors, []);
  assert.equal(v.faces.length, 45);
  assert.equal(v.totalSlots, 503);
  assert.equal(new Set(v.faces.map((f) => f.rack_id)).size, 27);   // A008+B001 が同じ棚
});

t('起動時の初期化でマスタが入り、2回目は何もしない', () => {
  assert.equal(ensureLocationFacesSeeded(), true);
  assert.equal(listFaces().length, 45);
  assert.equal(ensureLocationFacesSeeded(), false);
  assert.equal(listFaceImports().length, 1);
  assert.equal(listFaceImports()[0].actor, 'seed');
});

const idx = () => getFaceIndex();
const d = (cur, next) => core.describe(idx(), cur, next);
const L = (block, loc) => ({ block, location: loc });

// ─── 9パターン (モック v11 と同じ例) ───

t('同じ面: 002-016 → 002-019 = 3つ左 / 4段目', () => {
  const r = d(L('P3FB', '00201602'), L('P3FB', '00201904'));
  assert.equal(r.kind, 'same_face');
  assert.deepEqual(r.ren, { icon: 'left', n: 3, words: '3つ左' });
  assert.equal(r.dan.abs, 4);
  assert.equal(r.heavy, false);
  assert.equal(core.speechPrefix(r), '');
});

t('同じ場所・段だけ: 007-008-03 → 007-008-06 = ここ / ↑3 6段目', () => {
  const r = d(L('P3FD', '00700803'), L('P3FD', '00700806'));
  assert.equal(r.kind, 'same_slot');
  assert.equal(r.ren.here, true);
  assert.equal(r.dan.rel, '↑3');
  assert.equal(r.dan.abs, 6);
});

t('同じロケで商品だけ違う: rel なし・「同じ場所」', () => {
  const r = d(L('P3FD', '00700803'), L('P3FD', '00700803'));
  assert.equal(r.kind, 'same_slot');
  assert.equal(r.ren.sub, '同じ場所');
  assert.equal(r.dan.rel, undefined);
});

t('振り向く: A001-024 → A002-005 = 端から5つ目 / 4段目', () => {
  const r = d(L('P3FA', '00102403'), L('P3FA', '00200504'));
  assert.equal(r.kind, 'turn');
  assert.equal(r.act.verb, '振り向く');
  assert.deepEqual(r.ren, { n: 5, unit: 'つ目', words: '端から' });
  assert.equal(r.heavy, false);
});

t('折り返す: B002-012 → B002-016 = 端から4つ目 / 5段目 (重い移動・読み上げ「折り返し。」)', () => {
  const r = d(L('P3FB', '00201206'), L('P3FB', '00201605'));
  assert.equal(r.kind, 'fold');
  assert.deepEqual(r.ren, { n: 4, unit: 'つ目', words: '端から' });
  assert.equal(r.dan.abs, 5);
  assert.equal(r.heavy, true);
  assert.equal(core.speechPrefix(r), '折り返し。');
});

t('A008 → B001 は同じ棚の表裏 = 折り返す (列・ブロックが違っても)', () => {
  const r = d(L('P3FA', '00801202'), L('P3FB', '00100303'));
  assert.equal(r.kind, 'fold');
  assert.deepEqual(r.ren, { n: 3, unit: 'つ目', words: '端から' });
});

t('階段まわり (reliable=0) へは相対表現を出さずコードで確認', () => {
  const r = d(L('P3FC', '00202102'), L('P3FC', '00202503'));
  assert.equal(r.kind, 'around_stairs');
  assert.equal(r.reliable, false);
  assert.deepEqual(r.ren, { code: '002-025', words: 'コードで確認' });
  assert.equal(r.dan.abs, 3);
});

t('面を飛ばす: D003-005 → D006-014 = 大きく移動 / 006-014 / 3棚先 (端からは出さない)', () => {
  const r = d(L('P3FD', '00300502'), L('P3FD', '00601403'));
  assert.equal(r.kind, 'far');
  assert.equal(r.act.verb, '大きく移動');
  assert.deepEqual(r.ren, { code: '006-014', words: '3棚先・P3FD' });   // R17→R20 = 3棚
  assert.equal(r.heavy, true);
});

t('吊り下げ: D009-024 → F001-003 = 吊り下げ / 端から3つ目 / ポケット5', () => {
  const r = d(L('P3FD', '00902402'), L('P3FF', '00100305'));
  assert.equal(r.kind, 'hang');
  assert.deepEqual(r.ren, { n: 3, unit: 'つ目', words: '端から' });
  assert.deepEqual(r.dan, { abs: 5, label: 'ポケット' });
});

t('マスタ外のロケ (P1FC) = 案内できず / コードそのまま / 段なし', () => {
  const r = d(L('P3FD', '00901002'), L('P1FC', '00100201'));
  assert.equal(r.kind, 'unknown');
  assert.equal(r.act.verb, '案内できず');
  assert.equal(r.ren.code, 'P1FC-001-002-01');
  assert.equal(r.dan, null);
  assert.equal(core.speechPrefix(r), '場所を確認。');
});

t('現在地がマスタ外 (cur=null) は「案内できず」+ 次の列-連・段だけ', () => {
  const r = d(null, L('P3FA', '00100503'));
  assert.equal(r.kind, 'unknown');
  assert.equal(r.act.verb, '案内できず');
  assert.deepEqual(r.ren, { code: '001-005', words: 'コードで確認' });
  assert.equal(r.dan.abs, 3);
});

t('次が無ければ null (最後の商品)', () => {
  assert.equal(d(L('P3FA', '00100503'), null), null);
});

t('同じ面で連が減る方向 (逆走) は「右」', () => {
  const r = d(L('P3FB', '00201902'), L('P3FB', '00201604'));
  assert.deepEqual(r.ren, { icon: 'right', n: 3, words: '3つ右' });
});

t('向き=右 の面では左右が反転する', () => {
  const faces = listFaces().map((f) => (f.seq_no === 1 ? { ...f, direction: 'right' } : f));
  const i2 = core.buildIndex(faces);
  const r = core.describe(i2, L('P3FA', '00100302'), L('P3FA', '00100602'));
  assert.deepEqual(r.ren, { icon: 'right', n: 3, words: '3つ右' });
});

t('ZZZ / 特殊ロケはマスタ外として扱う (落ちない)', () => {
  const r = d(L('P3FA', '00100302'), L('ZZZ', 'ZZZZZZZZ'));
  assert.equal(r.kind, 'unknown');
  assert.equal(r.ren.code, 'ZZZ-ZZZZZZZZ');
});

// ─── 検証ルール ───

function csvOf(rows) {
  const header = ['面番号', 'ブロック', '列', '連from', '連to', '面の種類', '棚ID', '前の面からの動き', '信頼', '向き', 'メモ'];
  const q = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  return Buffer.from('﻿' + [header, ...rows].map((r) => r.map(q).join(',')).join('\r\n') + '\r\n', 'utf8');
}
const base = [
  [1, 'P3FA', '001', 1, 12, '表', 'R01', '開始', 1, '左', ''],
  [2, 'P3FA', '001', 13, 24, '裏', 'R01', '折り返す', 1, '左', ''],
  [3, 'P3FA', '002', 1, 12, '片面', 'R02', '振り向く', 1, '左', ''],
];

t('正しいCSVは通る (BOM付きUTF-8)', () => {
  const v = validateFaces(parseFacesCsv(csvOf(base)));
  assert.deepEqual(v.errors, []);
  assert.equal(v.totalSlots, 36);
});

t('面番号が飛ぶとエラー', () => {
  const rows = base.map((r) => [...r]); rows[2][0] = 4;
  const v = validateFaces(parseFacesCsv(csvOf(rows)));
  assert.ok(v.errors.some((e) => e.includes('連番')));
});

t('同じ列の連に隙間があるとエラー', () => {
  const rows = base.map((r) => [...r]); rows[1][3] = 14;
  const v = validateFaces(parseFacesCsv(csvOf(rows)));
  assert.ok(v.errors.some((e) => e.includes('隙間')));
});

t('「折り返す」なのに前の面と棚IDが違うとエラー', () => {
  const rows = base.map((r) => [...r]); rows[1][6] = 'R99';
  const v = validateFaces(parseFacesCsv(csvOf(rows)));
  assert.ok(v.errors.some((e) => e.includes('折り返す')));
});

t('「振り向く」なのに前の面と同じ棚IDだとエラー', () => {
  const rows = base.map((r) => [...r]); rows[2][6] = 'R01';
  const v = validateFaces(parseFacesCsv(csvOf(rows)));
  assert.ok(v.errors.some((e) => e.includes('振り向く')));
});

t('不正な値は行番号つきで PkError', () => {
  const rows = base.map((r) => [...r]); rows[1][7] = '回り込む';
  assert.throws(() => parseFacesCsv(csvOf(rows)), (e) => e instanceof PkError && /行3/.test(e.message));
});

t('実績ロケがマスタに載らないと警告 (エラーにはしない)', () => {
  const v = validateFaces(parseFacesCsv(csvOf(base)), { knownLocations: [{ block: 'P3FD', location: '00300502', count: 7 }] });
  assert.deepEqual(v.errors, []);
  assert.ok(v.warnings.some((w) => w.includes('P3FD-003')));
});

// ─── 取込・履歴・書き出し ───

t('取込は全置換で、履歴にCSV全文が残り、キャッシュが更新される', () => {
  const before = getFaceIndex();
  const r = importFaces(csvOf(base), { actor: 'tester@example.com', filename: 'x.csv' });
  assert.equal(r.faceCount, 3);
  assert.equal(listFaces().length, 3);
  assert.notEqual(getFaceIndex(), before);
  const imports = listFaceImports();
  assert.equal(imports[0].actor, 'tester@example.com');
  assert.equal(imports[0].face_count, 3);
  assert.equal(getDB().prepare('SELECT csv_text FROM pk_location_face_imports WHERE id=?').get(r.importId).csv_text.includes('R02'), true);
});

t('検証エラーのCSVは取り込まれない (前の版が残る)', () => {
  const rows = base.map((r) => [...r]); rows[1][3] = 14;
  assert.throws(() => importFaces(csvOf(rows), { actor: 't' }), PkError);
  assert.equal(listFaces().length, 3);
});

t('書き出したCSVをそのまま取り込める (往復)', () => {
  const csv = exportFacesCsv();
  const faces = parseFacesCsv(Buffer.from(csv, 'utf8'));
  const v = validateFaces(faces);
  assert.deepEqual(v.errors, []);
  assert.equal(v.faces.length, 3);
  assert.equal(v.faces[1].move_in, 'forward_turn');
});

t('Shift_JIS のCSVも読める', () => {
  const header = ['面番号', 'ブロック', '列', '連from', '連to', '面の種類', '棚ID', '前の面からの動き', '信頼', '向き', 'メモ'];
  const text = [header, ...base].map((r) => r.join(',')).join('\r\n');
  const faces = parseFacesCsv(iconv.encode(text, 'Shift_JIS'));
  assert.equal(faces.length, 3);
  assert.equal(faces[1].face_kind, 'back');
});

t('Shift_JIS で取り込んだ履歴も UTF-8 で残り、そのまま再取込できる (復元)', () => {
  const header = ['面番号', 'ブロック', '列', '連from', '連to', '面の種類', '棚ID', '前の面からの動き', '信頼', '向き', 'メモ'];
  const text = [header, ...base].map((r) => r.join(',')).join('\r\n');
  const r = importFaces(iconv.encode(text, 'Shift_JIS'), { actor: 't', filename: 'sjis.csv' });
  const saved = getDB().prepare('SELECT csv_text FROM pk_location_face_imports WHERE id=?').get(r.importId).csv_text;
  assert.ok(saved.includes('折り返す'), '履歴が文字化けしていない');
  assert.equal(parseFacesCsv(Buffer.from(saved, 'utf8')).length, 3);
});

t('書き出しは = + - @ 始まりの値を文字列化する (Excel数式インジェクション対策)', () => {
  const csv = exportFacesCsv([{ seq_no: 1, block: 'P3FA', col: '001', ren_from: 1, ren_to: 12, face_kind: 'single', rack_id: '=1+1', move_in: 'start', reliable: 1, direction: 'left', note: '@x' }]);
  assert.ok(csv.includes('"\'=1+1"'));
  assert.ok(csv.includes('"\'@x"'));
});

console.log(`\ntest-next-sign: ${passed} passed${process.exitCode ? ' (with failures)' : ''}`);
