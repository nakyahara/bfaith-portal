#!/usr/bin/env node
/**
 * 米国の STA 用 Excel (apps/fba-replenishment-us/sta-excel.js と POST /api/sta-excel) の試験。miniPC・SP-API には行かない。
 *   node scripts/test-fba-us-sta-excel.mjs
 * 土台 = 中原さんが 2026-09-26 に米国セラーセントラルから落としたテンプレート (templates/sta-us-template.xlsx)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-us-sta-'));
process.env.DATA_DIR = tmp;
process.env.WAREHOUSE_URL = 'http://minipc.test';
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);
const ExcelJS = (await import('exceljs')).default;
const sta = await imp('apps/fba-replenishment-us/sta-excel.js');

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}\n     ${e.stack.split('\n').slice(0, 5).join('\n     ')}`); }
}
const readBack = async (buf) => { const wb = new ExcelJS.Workbook(); await wb.xlsx.load(buf); return wb; };

console.log('① Excel を作る');
await t('テンプレートの見出しは中原さんの実物のまま (末尾の空白を含む 10 列・in / lb・Prep/Labeling owner の列なし)', async () => {
  const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(sta.TEMPLATE_FILE);
  const ws = wb.getWorksheet(sta.SHEET);
  assert.deepEqual(sta.EXPECTED_HEADERS.map((_, i) => ws.getCell(8, i + 1).value), sta.EXPECTED_HEADERS);
  assert.deepEqual(wb.worksheets.map((w) => w.name), ['Instructions', 'Data definitions', 'Create workflow – template', 'Create workflow – example']);
  assert.ok(sta.EXPECTED_HEADERS.includes('Manufacturing lot code ') && sta.EXPECTED_HEADERS.includes('Box weight (lb)'));
  assert.ok(!sta.EXPECTED_HEADERS.some((h) => /Prep owner|Labeling owner|\(cm\)|\(kg\)/.test(h)), '日本の列が混ざっている');
});
await t('9 行目から SKU・数量 (・期限は MM/DD/YYYY)・Default prep / labeling owner = Seller・ほかのシートと見出しはそのまま', async () => {
  const buf = await sta.buildStaUsWorkbook([{ sku: 'cardstand-r-40', qty: 116, expiry: null }, { sku: 'cardstand-w-20', qty: 31, expiry: '10/15/2030' }]);
  const wb = await readBack(buf);
  const ws = wb.getWorksheet(sta.SHEET);
  assert.deepEqual([ws.getCell('A9').value, ws.getCell('B9').value, ws.getCell('C9').value], ['cardstand-r-40', 116, null]);
  assert.deepEqual([ws.getCell('A10').value, ws.getCell('B10').value, ws.getCell('C10').value], ['cardstand-w-20', 31, '10/15/2030']);
  assert.equal(ws.getCell('A11').value, null);
  assert.deepEqual([ws.getCell('B3').value, ws.getCell('B4').value], ['Seller', 'Seller']);
  assert.deepEqual(sta.EXPECTED_HEADERS.map((_, i) => ws.getCell(8, i + 1).value), sta.EXPECTED_HEADERS);
  assert.equal(wb.worksheets.length, 4);
  assert.equal(wb.getWorksheet('Create workflow – example').getCell('A8').value, 'MySKU001', 'サンプルのシートが変わっている');
});
await t('🚨 テンプレートの見出しが想定と違う (Amazon が変えた・壊れた) / 9 行目以降に既に何か書いてある → 作らない', async () => {
  const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(sta.TEMPLATE_FILE);
  wb.getWorksheet(sta.SHEET).getCell('D8').value = 'Manufacturing lot code';   // 末尾の空白が無い
  const f1 = path.join(tmp, 'bad-header.xlsx'); await wb.xlsx.writeFile(f1);
  await assert.rejects(sta.buildStaUsWorkbook([{ sku: 'a', qty: 1 }], { templateFile: f1 }), /見出しが想定と違う.*4 列目/);
  const wb2 = new ExcelJS.Workbook(); await wb2.xlsx.readFile(sta.TEMPLATE_FILE);
  wb2.getWorksheet(sta.SHEET).getCell('A9').value = 'leftover';
  const f2 = path.join(tmp, 'dirty.xlsx'); await wb2.xlsx.writeFile(f2);
  await assert.rejects(sta.buildStaUsWorkbook([{ sku: 'a', qty: 1 }], { templateFile: f2 }), /9 行目が空でない/);
});
await t('期限: YYYY-MM-DD / YYYYMMDD / YYYY/MM/DD → MM/DD/YYYY・空は null・存在しない日や別の形は例外 (推測で書かない)', async () => {
  assert.deepEqual(['2030-10-15', '20301015', '2030/10/15', '', null].map(sta.toUsDate), ['10/15/2030', '10/15/2030', '10/15/2030', null, null]);
  for (const bad of ['2030-02-30', '10/15/2030', 'soon', '2030-1-5']) assert.throws(() => sta.toUsDate(bad), /期限/);
});

console.log('② 画面から来た行の検査');
const known = new Map([['cardstand-r-40', 'cardstand-r-40'], ['mixed-case', 'Mixed-Case']]);
await t('RESTOCK にある SKU だけ・表記は RESTOCK に直す・同じ SKU 2 行・数量は 1〜100000 の整数・空・200 行超 は断る', async () => {
  const ok = sta.validateStaItems([{ sku: ' MIXED-case ', qty: 3 }, { sku: 'cardstand-r-40', qty: '116' }], known);
  assert.deepEqual([ok.errors, ok.rows], [[], [{ sku: 'Mixed-Case', qty: 3, expiry: null }, { sku: 'cardstand-r-40', qty: 116, expiry: null }]]);
  const bad = sta.validateStaItems([{ sku: 'nope', qty: 1 }, { sku: 'mixed-case', qty: 0 }, { sku: 'cardstand-r-40', qty: 1.5 }, { sku: 'cardstand-r-40', qty: 2 }, { sku: '', qty: 1 }], known);
  assert.equal(bad.errors.length, 5);   // 数量のおかしい行も SKU を使ったものとして数える = 同じ SKU の 2 行目は重複でも知らせる
  assert.match(bad.errors.join('\n'), /米国の RESTOCK に無い SKU[\s\S]*数量は 1〜100000 の整数 \(0\)[\s\S]*数量は 1〜100000 の整数 \(1\.5\)[\s\S]*同じ SKU が 2 行[\s\S]*SKU が空/);
  assert.match(sta.validateStaItems([{ sku: 'cardstand-r-40', qty: 1 }, { sku: 'CARDSTAND-R-40', qty: 1 }], known).errors[0], /同じ SKU が 2 行/);
  assert.deepEqual(sta.validateStaItems([], known).errors, ['送る SKU がありません']);
  assert.deepEqual(sta.validateStaItems(Array.from({ length: 201 }, () => ({ sku: 'x', qty: 1 })), known).errors, ['一度に 200 行まで']);
  assert.match(sta.validateStaItems([{ sku: 'cardstand-r-40', qty: 1e6 }], known).errors[0], /1〜100000/);
});

await t('🚨 テンプレートの定義 (Data definitions C4 = 80 文字以内・英語以外の文字不可) に合わない SKU は断る (Codex #1473 R1 Medium 2)', async () => {
  const wb = new ExcelJS.Workbook(); await wb.xlsx.readFile(sta.TEMPLATE_FILE);
  assert.match(String(wb.getWorksheet('Data definitions').getCell('C4').value), /80文字以内[\s\S]*英語以外の文字は使用できません/, 'テンプレートの SKU の定義が変わった');
  const k = new Map([['商品-01', '商品-01'], ['a'.repeat(81), 'a'.repeat(81)], ['a'.repeat(80), 'a'.repeat(80)], ['ok-sku_1 (x)', 'OK-SKU_1 (x)']]);
  const r = sta.validateStaItems([{ sku: '商品-01', qty: 1 }, { sku: 'a'.repeat(81), qty: 1 }, { sku: 'a'.repeat(80), qty: 1 }, { sku: 'ok-sku_1 (x)', qty: 1 }], k);
  assert.equal(r.errors.length, 2);
  assert.ok(r.errors.every((e) => /80 文字以内の英数字・記号しか使えない/.test(e)));
  assert.deepEqual(r.rows.map((x) => x.sku), ['a'.repeat(80), 'OK-SKU_1 (x)']);
});
await t('数量の型: 整数の数・10 進の数字の文字列だけ。true・[12]・"0x10"・"1e2"・" 12 " 以外の空白入り・小数は断る (Codex #1473 R1 Low)', async () => {
  const k = new Map([['a', 'a']]);
  for (const bad of [true, [12], '0x10', '1e2', '1.0', 1.5, null, {}, '']) {
    assert.equal(sta.validateStaItems([{ sku: 'a', qty: bad }], k).errors.length, 1, `通してしまう: ${JSON.stringify(bad)}`);
  }
  assert.deepEqual([12, '12', ' 12 '].map((q) => sta.validateStaItems([{ sku: 'a', qty: q }], k).rows[0].qty), [12, 12, 12]);
});

console.log('③ POST /api/sta-excel (miniPC の応答だけ差し替え)');
const rRow = (sku) => ({ 'Merchant SKU': sku, Available: '0', Working: '0', Shipped: '0', Receiving: '0', 'Units Sold Last 30 Days': '30' });
let miniPcPayload;
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, init) => {
  if (String(url).startsWith('http://minipc.test/')) {
    return { status: 200, ok: true, headers: { get: () => 'application/json' }, json: async () => ({ ok: true, ...miniPcPayload }), text: async () => '' };
  }
  return realFetch(url, init);
};
const express = (await import('express')).default;
const router = (await imp('apps/fba-replenishment-us/router.js')).default;
const app = express(); app.use('/apps/fba-replenishment-us', router);
const server = await new Promise((r) => { const s = app.listen(0, () => r(s)); });
const post = (body) => new Promise((resolve, reject) => {
  const data = Buffer.from(JSON.stringify(body));
  const req = http.request({ port: server.address().port, path: '/apps/fba-replenishment-us/api/sta-excel', method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': data.length } }, (res) => {
    const chunks = []; res.on('data', (c) => chunks.push(c)); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
  });
  req.on('error', reject); req.end(data);
});
const setReports = (rows) => { miniPcPayload = { last_attempt: null, file_errors: [], save_failure: null, latest: { business_date: '2026-09-26', reports: { restock: { ok: true, fetched_at: '2026-09-25T22:44:00Z', rows }, planning: { ok: false, rows: null } } } }; };
try {
  await t('今の米国 RESTOCK の SKU で xlsx を返す (表記は RESTOCK のもの・ファイル名に日付)', async () => {
    setReports([rRow('Cardstand-R-40'), rRow('cardstand-w-20')]);
    const r = await post({ items: [{ sku: 'cardstand-r-40', qty: 116 }, { sku: 'cardstand-w-20', qty: 31 }] });
    assert.equal(r.status, 200, r.body.toString());
    assert.match(r.headers['content-type'], /spreadsheetml/);
    assert.match(r.headers['content-disposition'], /filename=US_STA_Manifest_\d{4}-\d{2}-\d{2}\.xlsx/);
    const ws = (await readBack(r.body)).getWorksheet(sta.SHEET);
    assert.deepEqual([ws.getCell('A9').value, ws.getCell('B9').value, ws.getCell('A10').value], ['Cardstand-R-40', 116, 'cardstand-w-20']);
  });
  await t('🚨 RESTOCK に無い SKU・同じレポートに 2 行ある SKU (数字が正しいか分からない)・数量がおかしい → 400 で理由', async () => {
    setReports([rRow('a'), rRow('dup'), rRow('DUP')]);
    const r = await post({ items: [{ sku: 'zzz', qty: 1 }, { sku: 'dup', qty: 1 }, { sku: 'a', qty: -1 }] });
    assert.equal(r.status, 400);
    const j = JSON.parse(r.body.toString());
    assert.match(j.message, /zzz.*RESTOCK に無い[\s\S]*dup.*RESTOCK に無い[\s\S]*数量は/);
  });
} finally {
  server.close();
  globalThis.fetch = realFetch;
}

console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
