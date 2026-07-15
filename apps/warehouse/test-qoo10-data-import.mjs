#!/usr/bin/env node
/**
 * test-qoo10-data-import.mjs — Qoo10 Analytics xlsx取込のスモークテスト (mall-csv-fetcher P1-Q R1)
 *
 * exceljsで実測構造 (2026-07-14サンプル) を再現した合成xlsxフィクスチャで、
 * 判別〜チャネル動的パース〜検算〜scope置換〜冪等〜ガードを検証。DBはtemp (本番DB不触)。
 *
 * 実行: node apps/warehouse/test-qoo10-data-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import ExcelJS from 'exceljs';
import { ensureQoo10DataTables, importQoo10File, prepareQoo10File } from './qoo10-data-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'qdata-smoke-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('journal_mode = WAL');
ensureQoo10DataTables(db);

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');

const CHANNELS = ['ホーム_全体', 'ホーム_メガ割', '検索結果_全体', '検索結果_Keyword Plus', '検索結果_スマートセールス',
  'ランキング', 'お気に入り', 'カート', 'ショップ', '商品詳細ページ', 'カテゴリページ', '外部サイト_全体',
  '外部サイト_Google', '外部サイト_Instagram', '外部サイト_その他', 'ダイレクト', 'その他',
  'プロモーションページ_全体', 'Myページ', '最近見た商品', '特集ページ', '商品カタログ'];

/** 店舗版xlsx生成 */
async function makeStoreXlsx(rows, { cvrPercentFmt = false, channels = CHANNELS } = {}) {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('data');
  ws.addRow(['開始日', '終了日', '曜日', ...channels.map((c) => `上位のチャネル(PV) : ${c}`),
    '上位のチャネル(PV)_合計(ページビュー合計)', '訪問者数', '商品カート数', '注文完了', 'コンバージョン率(%)']);
  for (const r of rows) {
    const row = ws.addRow([r.date, r.date, '月', ...r.pv, r.total, r.visitors, r.cart, r.orders,
      cvrPercentFmt ? r.cvr / 100 : r.cvr]);
    if (cvrPercentFmt) row.getCell(3 + r.pv.length + 5).numFmt = '0.00%';
  }
  return Buffer.from(await wb.xlsx.writeBuffer());
}

/** 商品版xlsx生成 */
async function makeItemXlsx(rows, { channels = CHANNELS } = {}) {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('data');
  ws.addRow(['開始日', '終了日', '曜日', '商品番号', '販売者商品コード', '商品名', 'ブランド名',
    ...channels.map((c) => `上位のチャネル(PV) : ${c}`), '上位のチャネル(PV)_合計(ページビュー合計)']);
  for (const r of rows) ws.addRow([r.date, r.date, '火', r.itemNo, r.sku, r.name, r.brand ?? '', ...r.pv, r.total]);
  return Buffer.from(await wb.xlsx.writeBuffer());
}

const zeroPv = (n = CHANNELS.length) => Array(n).fill(0);
const pvWith = (idx, v) => { const a = zeroPv(); for (const [i, x] of Object.entries(idx === undefined ? {} : Object.fromEntries([[idx, v]]))) a[i] = x; return a; };

console.log('=== 1. 店舗版: チャネル縦持ち + CVR日次 ===');
{
  const pv = zeroPv(); pv[5] = 10; pv[4] = 5; // ランキング=10, 検索結果_スマートセールス=5 (_全体列は検算除外)
  const buf = await makeStoreXlsx([
    { date: '2026-07-01', pv, total: 15, visitors: 8, cart: 3, orders: 2, cvr: 25.0 },
    { date: '2026-07-02', pv: zeroPv(), total: 0, visitors: 0, cart: 0, orders: 0, cvr: 0 },
  ]);
  const r = await importQoo10File(db, { name: 'qoo10_cvr_d2026-07-01_2026-07-02_x.xlsx', buffer: buf, sha256: sha(buf), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results).slice(0, 200));
  const n = db.prepare(`SELECT COUNT(*) n FROM fact_qoo10_traffic_channel_daily`).get().n;
  check(`チャネル縦持ち ${CHANNELS.length}×2日`, n === CHANNELS.length * 2, `got ${n}`);
  const smart = db.prepare(`SELECT pv FROM fact_qoo10_traffic_channel_daily WHERE date_jst='2026-07-01' AND channel='検索結果_スマートセールス'`).get();
  check('チャネル生ラベル保存', smart?.pv === 5, JSON.stringify(smart));
  const cvr = db.prepare(`SELECT * FROM fact_qoo10_cvr_daily WHERE date_jst='2026-07-01'`).get();
  check('CVR行', cvr?.pv_total === 15 && cvr?.visitors === 8 && cvr?.cvr_pct === 25.0, JSON.stringify(cvr));
  check('検算差0', cvr?.channel_pv_diff === 0);
}

console.log('=== 2. CVRが%書式セル (比率0.25) でも25%に正規化 ===');
{
  const pv = zeroPv(); pv[5] = 15;
  const buf = await makeStoreXlsx([{ date: '2026-07-03', pv, total: 15, visitors: 8, cart: 3, orders: 2, cvr: 25.0 }], { cvrPercentFmt: true });
  const r = await importQoo10File(db, { name: 'qoo10_cvr_d2026-07-03_2026-07-03_x.xlsx', buffer: buf, sha256: sha(buf), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results).slice(0, 200));
  const cvr = db.prepare(`SELECT cvr_pct FROM fact_qoo10_cvr_daily WHERE date_jst='2026-07-03'`).get();
  check('%書式→25.0', Math.abs((cvr?.cvr_pct ?? 0) - 25.0) < 0.01, JSON.stringify(cvr));
}

console.log('=== 3. 商品版: sparse縦持ち + (合計)行 + マスタ ===');
{
  const pv1 = zeroPv(); pv1[2] = 7; // 検索結果_全体=7
  const buf = await makeItemXlsx([
    { date: '2026-07-01', itemNo: '1005417281', sku: 'hakka-LG', name: 'ハッカ油', brand: 'Sin', pv: pv1, total: 7 },
    { date: '2026-07-01', itemNo: '1007046616', sku: 'tart950', name: 'タルトストーン', pv: zeroPv(), total: 0 },
  ]);
  const r = await importQoo10File(db, { name: 'qoo10_cvritem_d2026-07-01_2026-07-01_x.xlsx', buffer: buf, sha256: sha(buf), source: 'test' });
  check('取込ok', r.status === 'ok', JSON.stringify(r.results).slice(0, 200));
  const rows = db.prepare(`SELECT * FROM fact_qoo10_item_traffic_daily ORDER BY item_no, channel`).all();
  check('sparse: pv>0 1行+(合計)2行=3行', rows.length === 3, JSON.stringify(rows.map((x) => `${x.item_no}/${x.channel}=${x.pv}`)));
  check('(合計)行はpv0でも存在', rows.some((x) => x.item_no === '1007046616' && x.channel === '(合計)' && x.pv === 0));
  const m = db.prepare(`SELECT * FROM m_qoo10_items WHERE item_no='1005417281'`).get();
  check('マスタupsert+SKU正規化', m?.seller_code === 'hakka-lg' && m?.seller_code_raw === 'hakka-LG', JSON.stringify(m));
}

console.log('=== 4. scope置換 (補正版で消えた行が残留しない) ===');
{
  const pvA = zeroPv(); pvA[0] = 3;
  const v1 = await makeItemXlsx([
    { date: '2026-07-05', itemNo: 'A1', sku: 'a1', name: 'A', pv: pvA, total: 3 },
    { date: '2026-07-05', itemNo: 'B2', sku: 'b2', name: 'B', pv: pvA, total: 3 },
  ]);
  await importQoo10File(db, { name: 'qoo10_cvritem_d2026-07-05_2026-07-05_v1.xlsx', buffer: v1, sha256: sha(v1), source: 'test' });
  const v2 = await makeItemXlsx([{ date: '2026-07-05', itemNo: 'A1', sku: 'a1', name: 'A', pv: pvA, total: 3 }]);
  const r = await importQoo10File(db, { name: 'qoo10_cvritem_d2026-07-05_2026-07-05_v2.xlsx', buffer: v2, sha256: sha(v2), source: 'test' });
  check('補正版取込ok', r.status === 'ok');
  const items = db.prepare(`SELECT DISTINCT item_no FROM fact_qoo10_item_traffic_daily WHERE date_jst='2026-07-05'`).all().map((x) => x.item_no);
  check('消えたB2が残留しない', items.length === 1 && items[0] === 'A1', JSON.stringify(items));
}

console.log('=== 5. 冪等 (同一sha256=duplicate) ===');
{
  const pv = zeroPv(); pv[5] = 1;
  const buf = await makeStoreXlsx([{ date: '2026-07-06', pv, total: 1, visitors: 1, cart: 0, orders: 0, cvr: 0 }]);
  const r1 = await importQoo10File(db, { name: 'qoo10_cvr_d2026-07-06_2026-07-06_a.xlsx', buffer: buf, sha256: sha(buf), source: 'test' });
  check('初回ok', r1.status === 'ok');
  const r2 = await importQoo10File(db, { name: 'qoo10_cvr_d2026-07-06_2026-07-06_b.xlsx', buffer: buf, sha256: sha(buf), source: 'test' });
  check('同一内容=duplicate', r2.status === 'duplicate', r2.status);
}

console.log('=== 6. ガード類 ===');
{
  // 非xlsx (HTML偽装)
  const html = Buffer.from('<html><body>error</body></html>', 'utf8');
  const p1 = await prepareQoo10File('x.xlsx', html);
  check('HTML偽装を検出', !p1.ok && /xlsxでない/.test(p1.error), p1.ok ? 'ok?' : p1.error);

  // 週別ファイル (開始日≠終了日)
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('data');
  ws.addRow(['開始日', '終了日', '曜日', ...CHANNELS.map((c) => `上位のチャネル(PV) : ${c}`),
    '上位のチャネル(PV)_合計(ページビュー合計)', '訪問者数', '商品カート数', '注文完了', 'コンバージョン率(%)']);
  ws.addRow(['2026-06-30', '2026-07-06', '週', ...zeroPv(), 0, 0, 0, 0, 0]);
  const weekly = Buffer.from(await wb.xlsx.writeBuffer());
  const p2 = await prepareQoo10File('w.xlsx', weekly);
  check('週別ファイルは拒否', !p2.ok && /日別でない/.test(p2.error), p2.ok ? 'ok?' : p2.error);

  // チャネル激減 (形式変更検知)
  const few = await makeStoreXlsx([{ date: '2026-07-07', pv: [1, 2], total: 3, visitors: 1, cart: 0, orders: 0, cvr: 0 }],
    { channels: ['ホーム_全体', 'その他'] });
  const p3 = await prepareQoo10File('f.xlsx', few);
  check('チャネル激減は拒否', !p3.ok && /チャネル列が少なすぎる/.test(p3.error), p3.ok ? 'ok?' : p3.error);

  // CVR不整合 (単位判定ミス検知): orders/visitors=25%なのに表記2.5
  const pvz = zeroPv(); pvz[5] = 15;
  const bad = await makeStoreXlsx([{ date: '2026-07-08', pv: pvz, total: 15, visitors: 8, orders: 2, cart: 0, cvr: 2.5 }]);
  const p4 = await prepareQoo10File('c.xlsx', bad);
  check('CVR不整合を検出', !p4.ok && /CVR不整合/.test(p4.error), p4.ok ? 'ok?' : p4.error);

  // 数式セル拒否
  const wb2 = new ExcelJS.Workbook();
  const ws2 = wb2.addWorksheet('data');
  ws2.addRow(['開始日', '終了日', '曜日', ...CHANNELS.map((c) => `上位のチャネル(PV) : ${c}`),
    '上位のチャネル(PV)_合計(ページビュー合計)', '訪問者数', '商品カート数', '注文完了', 'コンバージョン率(%)']);
  const row = ws2.addRow(['2026-07-09', '2026-07-09', '木', ...zeroPv(), 0, 0, 0, 0, 0]);
  row.getCell(4).value = { formula: 'SUM(1,2)', result: 3 };
  const formulaBuf = Buffer.from(await wb2.xlsx.writeBuffer());
  const p5 = await prepareQoo10File('fm.xlsx', formulaBuf);
  check('数式セルは拒否', !p5.ok && /数式セル/.test(p5.error), p5.ok ? 'ok?' : p5.error);

  // PV合計≠チャネル計 → warning付きで取込は成功 (未分類PV許容)
  const pvw = zeroPv(); pvw[5] = 5;
  const wdiff = await makeStoreXlsx([{ date: '2026-07-10', pv: pvw, total: 9, visitors: 3, cart: 0, orders: 0, cvr: 0 }]);
  const p6 = await prepareQoo10File('d.xlsx', wdiff);
  check('PV合計差はwarning (取込可)', p6.ok && p6.warnings.length > 0 && /差4/.test(p6.warnings[0]), p6.ok ? JSON.stringify(p6.warnings) : p6.error);

  // 実サンプル (miniPCから回収済みのファイルがある場合のみ)
}

console.log('=== 6b. scope安全性 (Codex R1 High/Medium対応) ===');
{
  // 店舗版の中抜け: 要求期間 7/20〜7/22 なのに 7/21 が無い → エラー (巻き添え削除防止)
  const pv = zeroPv(); pv[5] = 1;
  const gap = await makeStoreXlsx([
    { date: '2026-07-20', pv, total: 1, visitors: 1, cart: 0, orders: 0, cvr: 0 },
    { date: '2026-07-22', pv, total: 1, visitors: 1, cart: 0, orders: 0, cvr: 0 },
  ]);
  const p1 = await prepareQoo10File('qoo10_cvr_d2026-07-20_2026-07-22_x.xlsx', gap);
  check('店舗版の中抜けは拒否', !p1.ok && /欠落日/.test(p1.error), p1.ok ? 'ok?' : p1.error);

  // 商品版マーカー無し: 実データ日付のみ置換 (中間日を巻き添えしない)
  const pvA = zeroPv(); pvA[5] = 2;
  const mid = await makeItemXlsx([{ date: '2026-07-21', itemNo: 'MID', sku: 'mid', name: '中間日', pv: pvA, total: 2 }]);
  await importQoo10File(db, { name: 'qoo10_cvritem_d2026-07-21_2026-07-21_mid.xlsx', buffer: mid, sha256: sha(mid), source: 'test' });
  const noMarker = await makeItemXlsx([
    { date: '2026-07-20', itemNo: 'EDGE', sku: 'e1', name: '端', pv: pvA, total: 2 },
    { date: '2026-07-22', itemNo: 'EDGE', sku: 'e1', name: '端', pv: pvA, total: 2 },
  ]);
  const r = await importQoo10File(db, { name: 'manual-download.xlsx', buffer: noMarker, sha256: sha(noMarker), source: 'test' });
  check('マーカー無し取込ok (warning付)', r.status === 'ok' && (r.results[0].warnings || []).some((w) => /期間マーカー/.test(w)), JSON.stringify(r.results).slice(0, 150));
  const midRow = db.prepare(`SELECT COUNT(*) n FROM fact_qoo10_item_traffic_daily WHERE date_jst='2026-07-21'`).get().n;
  check('中間日 (7/21) が巻き添え削除されない', midRow > 0, `got ${midRow}`);

  // CVR値域外 (二重換算検知)
  const pvz2 = zeroPv(); pvz2[5] = 4;
  const badCvr = await makeStoreXlsx([{ date: '2026-07-23', pv: pvz2, total: 4, visitors: 2, cart: 0, orders: 1, cvr: 250 }]);
  const p2 = await prepareQoo10File('qoo10_cvr_d2026-07-23_2026-07-23_x.xlsx', badCvr);
  check('CVR値域外は拒否', !p2.ok && /値域外/.test(p2.error), p2.ok ? 'ok?' : p2.error);

  // 合計>チャネル計 (未分類PV) は warning で取込可 (2024実測: 古い期間は差19-28%が正常)
  const pvd = zeroPv(); pvd[5] = 5;
  const unclassified = await makeStoreXlsx([{ date: '2026-07-24', pv: pvd, total: 100, visitors: 3, cart: 0, orders: 0, cvr: 0 }]);
  const p3 = await prepareQoo10File('qoo10_cvr_d2026-07-24_2026-07-24_x.xlsx', unclassified);
  check('未分類PV (合計>計) はwarningで取込可', p3.ok && p3.warnings.some((w) => /未分類/.test(w)), p3.ok ? JSON.stringify(p3.warnings) : p3.error);

  // チャネル計>合計 (二重計上疑い) はエラー
  const pvo = zeroPv(); pvo[5] = 100;
  const overSum = await makeStoreXlsx([{ date: '2026-07-27', pv: pvo, total: 5, visitors: 3, cart: 0, orders: 0, cvr: 0 }]);
  const p3b = await prepareQoo10File('qoo10_cvr_d2026-07-27_2026-07-27_x.xlsx', overSum);
  check('チャネル計>合計は拒否', !p3b.ok && /超過/.test(p3b.error), p3b.ok ? 'ok?' : p3b.error);

  // 負のPV (Qoo10側補正 -1) は許容
  const pvn = zeroPv(); pvn[5] = 3; pvn[16] = -1; // その他=-1
  const negBuf = await makeStoreXlsx([{ date: '2026-07-28', pv: pvn, total: 2, visitors: 1, cart: 0, orders: 0, cvr: 0 }]);
  const p3c = await prepareQoo10File('qoo10_cvr_d2026-07-28_2026-07-28_x.xlsx', negBuf);
  check('負値PV (-1補正) は許容', p3c.ok, p3c.ok ? 'ok' : p3c.error);

  // マスタは最新日の属性を採用
  const old = zeroPv(); old[5] = 1;
  const two = await makeItemXlsx([
    { date: '2026-07-25', itemNo: 'REN', sku: 'old-sku', name: '旧名', pv: old, total: 1 },
    { date: '2026-07-26', itemNo: 'REN', sku: 'new-sku', name: '新名', pv: old, total: 1 },
  ]);
  const r2 = await importQoo10File(db, { name: 'qoo10_cvritem_d2026-07-25_2026-07-26_x.xlsx', buffer: two, sha256: sha(two), source: 'test' });
  check('取込ok', r2.status === 'ok');
  const m2 = db.prepare(`SELECT seller_code_raw, item_name FROM m_qoo10_items WHERE item_no='REN'`).get();
  check('マスタ=最新日の属性', m2?.seller_code_raw === 'new-sku' && m2?.item_name === '新名', JSON.stringify(m2));
}

console.log('=== 7. 実サンプル (取得済みがあれば) ===');
{
  const samples = [
    ['C:/Users/中原　~1/AppData/Local/Temp/claude/c--Users-------OneDrive--------Downloads/4a7f3785-b5df-4d24-9770-9bb57a618a74/scratchpad/qsm_sample_traffic_Qoo10_CVR_20260630_20260713.xlsx', 'qoo10_cvr_daily'],
    ['C:/Users/中原　~1/AppData/Local/Temp/claude/c--Users-------OneDrive--------Downloads/4a7f3785-b5df-4d24-9770-9bb57a618a74/scratchpad/qsm_sample_cvritem_Qoo10_CVR_DateGoods_20260630_20260713.xlsx', 'qoo10_item_traffic_daily'],
  ];
  for (const [f, expectType] of samples) {
    if (!fs.existsSync(f)) { console.log(`  (skip: ${path.basename(f)} なし)`); continue; }
    const buf = fs.readFileSync(f);
    const p = await prepareQoo10File(path.basename(f), buf);
    check(`実サンプル ${expectType} パース`, p.ok && p.preps.some((x) => x.type === expectType),
      p.ok ? p.label : p.error);
    if (p.ok) console.log(`    → ${p.label} (${p.dateFrom}〜${p.dateTo})${p.warnings.length ? ' ⚠' + p.warnings[0] : ''}`);
  }
}

db.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n=== 結果: ${passed} passed / ${failed} failed ===`);
process.exit(failed > 0 ? 1 : 0);
