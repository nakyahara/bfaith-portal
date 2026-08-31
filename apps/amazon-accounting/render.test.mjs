// 画面 (インラインJS) の統合テスト — GET / を実レンダリングし、インラインJSを vm + スタブDOM で実行して
//   ①アップロード結果のセグメント表 ②過去確定データの見出し+表 ③セグメント別集計CSVダウンロード
// の列順・差引後の合計・旧月の「—」を固定値で検証する。(④集計サマリーCSVは fee-breakdown.test.mjs の segmentCsvSection)
// ※ router.js (express/multer) を import する統合テスト。純粋関数のテストは fee-breakdown.test.mjs
//   node --test apps/amazon-accounting/render.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import express from 'express';
import router from './router.js';
import { aggregate } from './payment-csv.js';
import { FEE_COLUMNS } from './fee-breakdown.js';

const EASY = 'Amazon Easy Ship料金';
const STORAGE = 'FBA在庫保管手数料';
const LONG = 'FBA長期在庫保管手数料';

// ── GET / を実レンダリングしてインラインJSを取り出す ──
async function fetchInlineScript() {
  const app = express();
  app.use('/apps/amazon-accounting', router);
  const server = app.listen(0);
  try {
    const html = await (await fetch('http://127.0.0.1:' + server.address().port + '/apps/amazon-accounting/')).text();
    const m = /<script>([\s\S]*?)<\/script>/.exec(html);
    assert.ok(m, 'inline script not found');
    return m[1];
  } finally {
    server.close();
  }
}

// ── スタブDOM: getElementById は id ごとの要素オブジェクトを返す。Blob/URL/createElement は CSV ダウンロードの捕捉用 ──
function makeContext(historyRows) {
  const els = new Map();
  const el = id => {
    if (!els.has(id)) els.set(id, { id, innerHTML: '', textContent: '', value: '0', disabled: false, style: {}, dataset: {}, classList: { toggle() {} } });
    return els.get(id);
  };
  const downloads = [];
  class Blob { constructor(parts) { this.text = parts.join(''); } }
  const ctx = {
    console, Math, Number, String, Array, Object, JSON, Date, parseInt, parseFloat, isNaN, Promise, setTimeout,
    document: {
      getElementById: el,
      createElement: () => ({ href: null, download: '', click() { downloads.push({ href: this.href, download: this.download }); } }),
    },
    location: { pathname: '/apps/amazon-accounting' },
    fetch: async url => ({ json: async () => (String(url).endsWith('/history') ? historyRows : []) }),
    Blob,
    URL: { createObjectURL: b => b },
    alert() {}, confirm() { return false; }, window: {},
  };
  ctx.window = ctx;
  return { ctx, el, downloads };
}

// <table> の HTML から行ごとのセル文字列 (タグ除去) を取り出す
function tableRows(html) {
  const rows = [];
  for (const tr of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)) {
    const cells = [...tr[1].matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/g)].map(c => c[1].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').trim());
    rows.push(cells);
  }
  return rows;
}
const num = s => Number(String(s).replace(/,/g, '').replace(/[^\d.-]/g, ''));

// ── サンプルデータ: aggregate() で本番と同じ形を作る ──
const noSku = (desc, extra) => ({ sku: '', 説明: desc, 解決方法: 'no_sku', 合計: 0, ...extra });
const SAMPLE_ROWS = [
  { sku: 'a1', 説明: '商品A', 解決方法: 'direct', 売上分類: 1, 税率: 10, 原価: 100, 数量: 2, 商品売上: 1000, 手数料: -100, 合計: 900 },
  noSku('Amazon Easy Ship料金', { トランザクション種類: 'Amazon手数料', 手数料: -500, 合計: -500 }),
  noSku('FBA在庫保管手数料', { トランザクション種類: 'FBA手数料', FBA手数料: -300, 合計: -300 }),
  noSku('FBA長期在庫保管手数料', { トランザクション種類: 'FBA手数料', FBA手数料: -100, 合計: -100 }),
  noSku('月額登録料', { トランザクション種類: '注文外料金', その他: -50, 合計: -50 }),
];
const agg = aggregate(SAMPLE_ROWS);
const AMOUNT_COLS = agg.columns.filter(c => c !== '合計');
const EXPECT_HEADER = ['セグメント', ...AMOUNT_COLS, EASY, STORAGE, LONG, '合計', '広告費', '原価合計'];

const uploadData = {
  yearMonth: '2026-07', totalRows: SAMPLE_ROWS.length, resolvedCount: 1, unresolvedSkus: [], unresolvedTax: [], conflicts: [], canConfirm: true,
  byTax: agg.byTax, bySegment: agg.bySegment, excluded: agg.excluded, otherDetails: agg.otherDetails, noSkuDetails: agg.noSkuDetails,
  columns: agg.columns, feeColumns: agg.feeColumns, mfRow: agg.mfRow, mfColumns: agg.mfColumns,
  segmentNames: { 1: '自社商品', 2: '取引先限定', 3: '仕入れ商品' }, excludedNames: { 4: '輸出' }, zeroGenka: [], existing: null,
};

// 過去確定データ: 新月 (内訳キーあり) と旧月 (内訳キーなし)
const oldSeg = {};
for (const [k, r] of Object.entries(agg.bySegment)) { const c = { ...r }; for (const f of FEE_COLUMNS) delete c[f]; oldSeg[k] = c; }
const HISTORY = [
  { year_month: '2026-07', by_segment: agg.bySegment, excluded: {}, mf_row: {}, ad_cost: 0, confirmed_at: '2026-08-01 10:00:00' },
  { year_month: '2026-02', by_segment: oldSeg, excluded: {}, mf_row: {}, ad_cost: 0, confirmed_at: '2026-03-01 10:00:00' },
];

const NET_OTHER = -950 - (-500 - 300 - 100); // = -50: other 行の差引後合計
const NET_ALL = 900 + NET_OTHER;              // 合計行

test('①アップロード結果のセグメント表: 列順 = 金額列 → 内訳3列 → 合計(差引) → 広告費 → 原価合計、合計行も差引後', async () => {
  const script = await fetchInlineScript();
  const { ctx, el } = makeContext([]);
  vm.runInNewContext(script, ctx, { filename: 'inline.js' });
  ctx.showResult(uploadData);
  const rows = tableRows(el('segmentTable').innerHTML);
  assert.deepEqual(rows[0], EXPECT_HEADER);
  const other = rows.find(r => r[0].startsWith('other:'));
  assert.ok(other, 'other row');
  const iEasy = EXPECT_HEADER.indexOf(EASY), iTotal = EXPECT_HEADER.indexOf('合計');
  assert.equal(num(other[iEasy]), -500);
  assert.equal(num(other[iEasy + 1]), -300);
  assert.equal(num(other[iEasy + 2]), -100);
  assert.equal(num(other[iTotal]), NET_OTHER);
  assert.equal(num(other[EXPECT_HEADER.indexOf('手数料')]), -500); // 手数料列は内訳分を含んだまま
  const seg1 = rows.find(r => r[0].startsWith('1:'));
  assert.equal(num(seg1[iTotal]), 900);
  const totalRow = rows[rows.length - 1];
  assert.equal(totalRow[0], '合計');
  assert.equal(num(totalRow[iTotal]), NET_ALL);
  assert.equal(num(totalRow[iEasy]), -500);
  assert.equal(num(totalRow[EXPECT_HEADER.indexOf('原価合計')]), 200);
});

test('②過去確定データ: 見出しの合計と展開後の表が一致 (差引後)、旧月は「—」+従来の合計', async () => {
  const script = await fetchInlineScript();
  const { ctx, el } = makeContext(HISTORY);
  vm.runInNewContext(script, ctx, { filename: 'inline.js' });
  await ctx.loadHistory();
  const html = el('historyList').innerHTML;
  // 見出し: 「合計: ¥…」
  const heads = [...html.matchAll(/<b>(\d{4}-\d{2})<\/b>[^<]*合計: ¥([-\d,]+)/g)].map(m => [m[1], num(m[2])]);
  assert.deepEqual(heads, [['2026-07', NET_ALL], ['2026-02', 900 - 950]]);
  // 各月の表
  const bodies = [...html.matchAll(/<div class="acc-body"[^>]*>([\s\S]*?)<\/div>\s*(?=<div class="acc-header"|$)/g)].map(m => m[1]);
  assert.equal(bodies.length, 2);
  const iTotal = EXPECT_HEADER.indexOf('合計'), iEasy = EXPECT_HEADER.indexOf(EASY);
  const newRows = tableRows(bodies[0]);
  assert.deepEqual(newRows[0], EXPECT_HEADER);
  const newOther = newRows.find(r => r[0].startsWith('other:'));
  assert.equal(num(newOther[iEasy]), -500);
  assert.equal(num(newOther[iTotal]), NET_OTHER);
  assert.equal(num(newRows[newRows.length - 1][iTotal]), NET_ALL);
  const oldRows = tableRows(bodies[1]);
  assert.deepEqual(oldRows[0], EXPECT_HEADER);
  const oldOther = oldRows.find(r => r[0].startsWith('other:'));
  assert.equal(oldOther[iEasy], '—');
  assert.equal(oldOther[iEasy + 2], '—');
  assert.equal(num(oldOther[iTotal]), -950); // 旧月は従来の合計のまま
  assert.equal(num(oldRows[oldRows.length - 1][iTotal]), 900 - 950);
  assert.ok(bodies[1].includes('この月の確定時点では未対応でした'));
});

test('③セグメント別集計CSVダウンロード: ヘッダーと行の列順・差引後の合計・旧月は内訳空欄', async () => {
  const script = await fetchInlineScript();
  const { ctx, downloads } = makeContext(HISTORY);
  vm.runInNewContext(script, ctx, { filename: 'inline.js' });
  await ctx.downloadHistoryCsv();
  assert.equal(downloads.length, 1);
  const csv = downloads[0].href.text.replace(/^﻿/, '');
  const lines = csv.trim().split('\n');
  assert.equal(lines[0], '集計月,セグメント,' + AMOUNT_COLS.join(',') + ',' + FEE_COLUMNS.join(',') + ',合計,広告費,原価合計');
  const header = lines[0].split(',');
  const iEasy = header.indexOf(EASY), iTotal = header.indexOf('合計');
  const newOther = lines.map(l => l.split(',')).find(c => c[0] === '2026/07/31' && c[1] === 'other:その他/未分類');
  assert.ok(newOther, 'new month other row');
  assert.equal(Number(newOther[iEasy]), -500);
  assert.equal(Number(newOther[iEasy + 2]), -100);
  assert.equal(Number(newOther[iTotal]), NET_OTHER);
  const oldOther = lines.map(l => l.split(',')).find(c => c[0] === '2026/02/28' && c[1] === 'other:その他/未分類');
  assert.ok(oldOther, 'old month other row');
  assert.equal(oldOther[iEasy], '');
  assert.equal(oldOther[iEasy + 2], '');
  assert.equal(Number(oldOther[iTotal]), -950);
});
