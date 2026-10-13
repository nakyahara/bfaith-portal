import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url, 'lzbarcode2-mirror-');
/**
 * test-lz-mirror.mjs — ロジザードの写しを読む lib/lz-mirror.mjs の readLzMirror / lzCopyCheck を本物の SQLite で (#1719 Codex R2 Medium 2)
 *
 * 表は本番と同じ作り方 (apps/inbound-check/db.js の createTables)・行は本番の取込 (barcode-master.js の importBarcodeMaster・
 * product-master.js の importProductMaster = 共有ドライブの実ファイルの 1 行目のヘッダ) で入れる = 列名と時刻の形 (new Date().toISOString()) を本物で固定する。
 *   M1 表が無い = unavailable (時刻なし)・M2 表はあるが 0 行 = empty (時刻なし)・M3 本物の取込の後 = checked (時刻 = 取込の時刻・ISO)・バーコード → 商品 ID・小文字の鍵
 *   M4 36 時間より前 = stale (時刻あり)・境目・M5 時刻が読めない / 5 分より先 = unavailable・M6 商品の一覧だけ無い = 一覧は ok でない・バーコードは照らせる
 *   M7 開けない = unavailable
 * 使い方: node scripts/test-lz-mirror.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';
import Database from 'better-sqlite3';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbarcode2-mirror-'));
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { createTables } = await import('../apps/inbound-check/db.js');
const { importBarcodeMaster } = await import('../apps/inbound-check/barcode-master.js');
const { importProductMaster } = await import('../apps/inbound-check/product-master.js');
const { readLzMirror, lzCopyCheck, LZ_COPY_FRESH_HOURS } = await import('../lib/lz-mirror.mjs');

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const H = 3600e3;
const q = (v) => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
const csv = (header, rows) => iconv.encode([header.map(q).join(',')].concat(rows.map((r) => r.map(q).join(','))).join('\r\n') + '\r\n', 'cp932');
// 🚨 ヘッダは共有ドライブの実ファイルの 1 行目 (test-inbound-check-barcode-master.mjs の REAL_HEADER と同じ・2026-09-08 採取)
const BC_HEADER = ['商品ID', '商品名', '検索名称', 'バーコード', '有効期限区分'];
const PM_HEADER = ['商品ID', '商品名', '有効期限区分'];

console.log('ロジザードの写し (本物の SQLite)');
t('[M1] 表が無い = unavailable (時刻なし)・一覧も ok でない', () => {
  const sq = new Database(':memory:');
  const m = readLzMirror(() => sq);
  assert.deepEqual([m.ok, m.bc.ok, m.bc.empty, m.bc_as_of, m.list_as_of], [false, false, false, null, null]);
  assert.match(m.bc.reason, /バーコードマスタの写しがありません/);
  const c = lzCopyCheck(m, Date.now());
  assert.deepEqual([c.state, c.as_of], ['unavailable', null]);
  sq.close();
});
t('[M2] 本番と同じ表 (createTables) で 0 行 = empty (時刻なし)', () => {
  createTables(getMirrorDB());
  const m = readLzMirror(() => getMirrorDB());
  assert.deepEqual([m.ok, m.bc.ok, m.bc.empty, m.bc.rows, m.bc_as_of], [false, false, true, 0, null]);
  assert.match(m.bc.reason, /バーコードマスタの写しが空です/);
  assert.match(m.reason, /商品の一覧の写しが空です/);
  const c = lzCopyCheck(m, Date.now());
  assert.deepEqual([c.state, c.as_of], ['empty', null]);
});
let importedAt;
t('[M3] 本物の取込の後 = checked (時刻 = 取込の時刻・ISO の文字)・バーコード → 商品 ID (書いたとおり)・小文字の鍵', () => {
  const before = Date.now();
  const b = importBarcodeMaster(csv(BC_HEADER, [
    ['Awa-Shio-250', '粟国の塩', '粟国の塩', '4936695001014', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', 'X002ABCD1F', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', '4903357200047', '01'],
  ]), { actor: 'test' });
  assert.equal(b.ok, true, JSON.stringify(b));
  assert.equal(importProductMaster(csv(PM_HEADER, [['Awa-Shio-250', '粟国の塩', '01'], ['only-in-list', '一覧だけ', '02']]), { actor: 'test' }).ok, true);
  const raw = getMirrorDB().prepare('SELECT max(updated_at) AS at FROM f_inbound_check_barcode_master').get().at;
  assert.match(raw, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/, '取込の時刻の形が ISO でない');
  importedAt = raw;
  const m = readLzMirror(() => getMirrorDB());
  assert.deepEqual([m.ok, m.bc.ok, m.bc.rows, m.rows, m.bc_as_of], [true, true, 3, 2, raw]);
  assert.equal(m.barcodes.get('X002ABCD1F'), 'pashima-single-KI');
  assert.equal(m.barcodes.get('4936695001014'), 'Awa-Shio-250');
  assert.ok(m.keys.has('awa-shio-250') && m.keys.has('only-in-list') && m.keys.has('pashima-single-ki'));
  assert.deepEqual([...m.ids.get('awa-shio-250')], ['Awa-Shio-250']);
  const c = lzCopyCheck(m, Date.now());
  assert.deepEqual([c.state, c.as_of], ['checked', raw]);
  assert.ok(Date.parse(c.as_of) >= before - 1000);
});
t('[M4] 一番新しい行が 36 時間より前 = stale (時刻あり)・36 時間の少し前 = checked', () => {
  assert.equal(LZ_COPY_FRESH_HOURS, 36);
  const m = readLzMirror(() => getMirrorDB());
  const at = Date.parse(importedAt);
  assert.deepEqual(Object.values(lzCopyCheck(m, at + 37 * H)).slice(0, 2), ['stale', importedAt]);
  assert.equal(lzCopyCheck(m, at + 36 * H).state, 'stale', 'ちょうど 36 時間 = 古い');
  assert.equal(lzCopyCheck(m, at + 36 * H - 60e3).state, 'checked');
  // 本物の表で古い時刻にする (全部の行)
  const old = new Date(Date.now() - 40 * H).toISOString();
  getMirrorDB().prepare('UPDATE f_inbound_check_barcode_master SET updated_at = ?').run(old);
  const m2 = readLzMirror(() => getMirrorDB());
  assert.deepEqual([lzCopyCheck(m2, Date.now()).state, lzCopyCheck(m2, Date.now()).as_of], ['stale', old]);
  // 1 行だけ新しい = 一番新しい行で決める (取込は全部入れ替え = ふつうは同じ時刻)。どの行が新しくても (読む順に左右されない)
  for (const bc of ['4936695001014', 'X002ABCD1F', '4903357200047']) {
    getMirrorDB().prepare('UPDATE f_inbound_check_barcode_master SET updated_at = ?').run(old);
    getMirrorDB().prepare('UPDATE f_inbound_check_barcode_master SET updated_at = ? WHERE barcode = ?').run(importedAt, bc);
    const c = lzCopyCheck(readLzMirror(() => getMirrorDB()), Date.parse(importedAt) + H);
    assert.deepEqual([c.state, c.as_of], ['checked', importedAt], bc);
  }
});
t('[M5] 時刻が読めない・5 分より先 = unavailable (時刻なし = DB の関数の組み合わせに合う)', () => {
  const sq = getMirrorDB();
  sq.prepare('UPDATE f_inbound_check_barcode_master SET updated_at = ?').run('not-a-time');
  let c = lzCopyCheck(readLzMirror(() => sq), Date.now());
  assert.deepEqual([c.state, c.as_of], ['unavailable', null]);
  sq.prepare('UPDATE f_inbound_check_barcode_master SET updated_at = ?').run(new Date(Date.now() + H).toISOString());
  c = lzCopyCheck(readLzMirror(() => sq), Date.now());
  assert.deepEqual([c.state, c.as_of], ['unavailable', null]);
  const soon = new Date(Date.now() + 60e3).toISOString();
  sq.prepare('UPDATE f_inbound_check_barcode_master SET updated_at = ?').run(soon);
  c = lzCopyCheck(readLzMirror(() => sq), Date.now());
  assert.deepEqual([c.state, c.as_of], ['checked', soon], '1 分先 (時計のずれ) は通す');
  sq.prepare('UPDATE f_inbound_check_barcode_master SET updated_at = ?').run(importedAt);
});
t('[M6] 商品の一覧の表だけ無い = 一覧は ok でない・バーコードマスタは照らせる', () => {
  const sq = new Database(':memory:');
  createTables(sq);
  sq.exec('DROP TABLE f_inbound_check_product_flags');
  const now = new Date().toISOString();
  sq.prepare("INSERT INTO f_inbound_check_barcode_master (barcode, code_key, product_id, barcode_type, rank, updated_at) VALUES ('X00LZM6001', 'm6', 'M6', 'fnsku', 0, ?)").run(now);
  const m = readLzMirror(() => sq);
  assert.deepEqual([m.ok, m.bc.ok, m.barcodes.get('X00LZM6001'), m.bc_as_of], [false, true, 'M6', now]);
  assert.match(m.reason, /商品の一覧の写しがありません/);
  assert.equal(lzCopyCheck(m, Date.now()).state, 'checked');
  sq.close();
});
t('[M7] 開けない = unavailable', () => {
  const m = readLzMirror(() => { throw new Error('warehouse-mirror.db が初期化されていません'); });
  assert.deepEqual([m.ok, m.bc.ok, lzCopyCheck(m, Date.now()).state, lzCopyCheck(m, Date.now()).as_of], [false, false, 'unavailable', null]);
  assert.match(m.reason, /写しを開けない/);
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
