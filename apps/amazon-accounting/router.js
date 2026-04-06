/**
 * Amazon売上集計ツール
 *
 * セラセンのペイメントレポートCSVをアップロードし、
 * mirror_products + mirror_sku_map を使って
 * 税率別・セグメント別の売上集計を自動計算する。
 *
 * Phase 1: CSVアップロード → SKU照合 → 未登録検出 → 集計プレビュー
 */
import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();
const UPLOAD_DIR = process.env.DATA_DIR ? process.env.DATA_DIR + '/import' : 'data/import';
if (!fs.existsSync(UPLOAD_DIR)) { try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch {} }
const upload = multer({ dest: UPLOAD_DIR });

// セグメント名称マップ（1〜3が集計対象）
const SEGMENT_NAMES = { 1: '自社商品', 2: '取扱限定', 3: '仕入れ商品' };

// エビデンスCSV一時保存（yearMonth → { detail, summary }）
const evidenceStore = new Map();
// 除外セグメント（4=輸出はセグメント集計に含めない）
const EXCLUDED_SEGMENTS = { 4: '輸出' };

// ─── CSV解析 ───

function parseCsvBuffer(buf) {
  let text;
  if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    text = buf.toString('utf-8');
  } else {
    text = buf.toString('utf-8');
  }
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  return lines.map(l => {
    const result = [];
    let current = '', inQuotes = false;
    for (let i = 0; i < l.length; i++) {
      const ch = l[i];
      if (ch === '"') { inQuotes = !inQuotes; }
      else if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
      else { current += ch; }
    }
    result.push(current.trim());
    return result;
  });
}

// ─── SKU解決（3段階）───

function resolveSkus(rows, db) {
  // mirror_productsの商品コードマップ（小文字統一）
  const productsMap = new Map();
  for (const p of db.prepare('SELECT * FROM mirror_products').all()) {
    productsMap.set((p.商品コード || '').toLowerCase(), p);
  }

  // mirror_sku_mapの変換マップ
  const skuMapEntries = new Map();
  for (const s of db.prepare('SELECT * FROM mirror_sku_map').all()) {
    const key = s.seller_sku?.toLowerCase();
    if (!key) continue;
    if (!skuMapEntries.has(key)) skuMapEntries.set(key, []);
    skuMapEntries.get(key).push(s);
  }

  const resolved = [];
  const unresolved = new Map(); // SKU → { sku, name, count, amount }

  for (const row of rows) {
    const sku = (row.sku || '').toLowerCase();
    const txType = row.トランザクション種類 || '';

    // 振込みは集計対象外だがSKU解決は不要
    if (txType === '振込み') {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 商品コード: null, 解決方法: 'skip' });
      continue;
    }

    if (!sku) {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 商品コード: null, 解決方法: 'no_sku' });
      continue;
    }

    // Stage 1: mirror_productsで直一致
    let product = productsMap.get(sku);
    let resolveMethod = 'direct';

    // Stage 2: sku_mapで変換
    if (!product) {
      const mappings = skuMapEntries.get(sku);
      if (mappings && mappings.length > 0) {
        product = productsMap.get(mappings[0].ne_code);
        resolveMethod = 'sku_map';
      }
    }

    if (product) {
      resolved.push({
        ...row,
        商品コード: product.商品コード,
        原価: product.原価 || 0,
        税率: product.消費税率 ? Math.round(product.消費税率 * 100) : null, // 0.1→10, 0.08→8
        売上分類: product.売上分類,
        解決方法: resolveMethod,
      });
    } else {
      // Stage 3: 未登録
      resolved.push({
        ...row,
        商品コード: null,
        原価: 0,
        税率: null,
        売上分類: null,
        解決方法: 'unresolved',
      });
      const existing = unresolved.get(sku) || { sku, name: row.説明 || '', count: 0, amount: 0 };
      existing.count++;
      existing.amount += row.合計 || 0;
      unresolved.set(sku, existing);
    }
  }

  // 原価ゼロの商品を検出
  const zeroGenka = new Map();
  for (const row of resolved) {
    if (row.解決方法 === 'skip' || row.解決方法 === 'no_sku') continue;
    if (row.商品コード && (row.原価 === 0 || row.原価 === null)) {
      const key = row.商品コード;
      const existing = zeroGenka.get(key) || { 商品コード: key, sku: row.sku || '', 商品名: row.説明 || '', 数量合計: 0, 売上合計: 0, count: 0 };
      existing.数量合計 += row.数量 || 0;
      existing.売上合計 += row.商品売上 || 0;
      existing.count++;
      zeroGenka.set(key, existing);
    }
  }

  return { resolved, unresolved: [...unresolved.values()], zeroGenka: [...zeroGenka.values()] };
}

// ─── 集計 ───

function aggregate(resolvedRows) {
  const columns = ['商品売上', '商品の売上税', '配送料', '配送料の税金',
    'ギフト包装手数料', 'ギフト包装の税金', 'Amazonポイント費用',
    'プロモーション割引額', 'プロモーション割引の税金', '手数料', 'FBA手数料',
    'トランザクション他', 'その他', '合計'];

  function emptyRow() {
    const r = {};
    columns.forEach(c => r[c] = 0);
    r.原価合計 = 0;
    r.行数 = 0;
    return r;
  }

  function addRow(target, row) {
    columns.forEach(c => target[c] += row[c] || 0);
    target.原価合計 += (row.原価 || 0) * (row.数量 || 1);
    target.行数++;
  }

  // 税率別
  const byTax = { '10': emptyRow(), '8': emptyRow() };

  // セグメント別（1〜3 + other。4=輸出は除外）
  const bySegment = { '1': emptyRow(), '2': emptyRow(), '3': emptyRow(), 'other': emptyRow() };

  // 除外セグメント（4=輸出）
  const excluded = { '4': emptyRow() };

  // 「その他/未分類」に入った行の明細を記録
  const otherDetails = new Map();

  for (const row of resolvedRows) {
    if (row.解決方法 === 'skip') continue; // 振込み

    // 税率別
    const taxKey = row.税率 === 8 ? '8' : '10'; // 未登録は10%仮扱い
    addRow(byTax[taxKey], row);

    // セグメント別
    const segKey = row.売上分類 ? String(row.売上分類) : 'other';

    if (excluded[segKey]) {
      // 4=輸出 → 除外集計
      addRow(excluded[segKey], row);
    } else if (bySegment[segKey]) {
      addRow(bySegment[segKey], row);
    } else {
      addRow(bySegment['other'], row);
    }

    // 「その他」に入った行の明細を記録
    if (!row.売上分類 && !excluded[segKey]) {
      const detailKey = row.商品コード || row.sku || '_no_sku_' + (row.トランザクション種類 || '');
      const existing = otherDetails.get(detailKey) || {
        sku: row.sku || '',
        商品コード: row.商品コード || '',
        商品名: row.説明 || '',
        トランザクション種類: row.トランザクション種類 || '',
        解決方法: row.解決方法,
        商品売上: 0,
        合計: 0,
        数量: 0,
        count: 0,
      };
      existing.商品売上 += row.商品売上 || 0;
      existing.合計 += row.合計 || 0;
      existing.数量 += row.数量 || 0;
      existing.count++;
      otherDetails.set(detailKey, existing);
    }
  }

  // MF連携用 税込み集計行
  const t10 = byTax['10'];
  const t8 = byTax['8'];
  const mfColumns = ['商品売上(10%)', '商品売上(8%)', '配送料', 'ギフト包装手数料',
    'Amazonポイントの費用', 'プロモーション割引額', '手数料', 'FBA手数料',
    'トランザクションに関するその他の手数料+その他', '合計', '端数調整'];
  const mfRow = {
    '商品売上(10%)': t10['商品売上'] + t10['商品の売上税'],
    '商品売上(8%)': t8['商品売上'] + t8['商品の売上税'],
    '配送料': t10['配送料'] + t10['配送料の税金'] + t8['配送料'] + t8['配送料の税金'],
    'ギフト包装手数料': t10['ギフト包装手数料'] + t10['ギフト包装の税金'] + t8['ギフト包装手数料'] + t8['ギフト包装の税金'],
    'Amazonポイントの費用': t10['Amazonポイント費用'] + t8['Amazonポイント費用'],
    'プロモーション割引額': t10['プロモーション割引額'] + t10['プロモーション割引の税金'] + t8['プロモーション割引額'] + t8['プロモーション割引の税金'],
    '手数料': t10['手数料'] + t8['手数料'],
    'FBA手数料': t10['FBA手数料'] + t8['FBA手数料'],
    'トランザクションに関するその他の手数料+その他': t10['トランザクション他'] + t10['その他'] + t8['トランザクション他'] + t8['その他'],
    '合計': t10['合計'] + t8['合計'],
  };
  // 端数調整 = 合計 - 他全列の合計
  const mfSubtotal = mfRow['商品売上(10%)'] + mfRow['商品売上(8%)'] + mfRow['配送料']
    + mfRow['ギフト包装手数料'] + mfRow['Amazonポイントの費用'] + mfRow['プロモーション割引額']
    + mfRow['手数料'] + mfRow['FBA手数料'] + mfRow['トランザクションに関するその他の手数料+その他'];
  mfRow['端数調整'] = mfRow['合計'] - mfSubtotal;

  return {
    byTax,
    bySegment,
    excluded,
    otherDetails: [...otherDetails.values()].sort((a, b) => Math.abs(b.商品売上) - Math.abs(a.商品売上)),
    columns,
    mfRow,
    mfColumns,
  };
}

// ─── GET / — メイン画面 ───

router.get('/', (req, res) => {
  res.send(renderPage());
});

// ─── POST /upload — CSVアップロード＆集計 ───

router.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });

  let db, buf;
  try {
    db = getMirrorDB();
  } catch (e) {
    return res.status(500).json({ error: 'ミラーDB未初期化: ' + e.message });
  }
  try {
    buf = fs.readFileSync(req.file.path);
    fs.unlinkSync(req.file.path);
  } catch (e) {
    return res.status(500).json({ error: 'ファイル読み込みエラー: ' + e.message });
  }

  try {
  // CSV解析（テキストベースで軽量処理）
  const text = buf[0] === 0xEF ? buf.toString('utf-8') : buf.toString('utf-8');
  const lines = text.split(/\r?\n/);

  // 先頭7行スキップ + ヘッダー1行 = 8行目以降がデータ
  const num = v => { const n = parseFloat((v || '').replace(/"/g, '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };
  const clean = v => (v || '').replace(/^"|"$/g, '').trim();

  const parsedRows = [];
  for (let i = 8; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    // CSV行をパース（ダブルクォート対応）
    const cols = [];
    let current = '', inQ = false;
    for (let j = 0; j < line.length; j++) {
      const ch = line[j];
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { cols.push(current); current = ''; }
      else current += ch;
    }
    cols.push(current);

    const date = clean(cols[0]);
    if (!date) continue;
    // 日付から時刻部分を除去
    const dateOnly = date.replace(/ .+$/, '');

    parsedRows.push({
      日付: dateOnly,
      決済番号: clean(cols[1]),
      トランザクション種類: clean(cols[2]),
      注文番号: clean(cols[3]),
      sku: clean(cols[4]).toLowerCase(),
      説明: clean(cols[5]),
      数量: parseInt(clean(cols[6])) || 0,
      商品売上: num(cols[13]),
      商品の売上税: num(cols[14]),
      配送料: num(cols[15]),
      配送料の税金: num(cols[16]),
      ギフト包装手数料: num(cols[17]),
      ギフト包装の税金: num(cols[18]),
      Amazonポイント費用: num(cols[19]),
      プロモーション割引額: num(cols[20]),
      プロモーション割引の税金: num(cols[21]),
      手数料: num(cols[23]),
      FBA手数料: num(cols[24]),
      トランザクション他: num(cols[25]),
      その他: num(cols[26]),
      合計: num(cols[27]),
    });
  }

  // 対象年月を推定（最初の日付から）
  const firstDate = parsedRows[0]?.日付 || '';
  const yearMonth = firstDate.slice(0, 7).replace('/', '-');

  // SKU解決
  const { resolved, unresolved, zeroGenka } = resolveSkus(parsedRows, db);

  // 集計
  const { byTax, bySegment, excluded, otherDetails, columns, mfRow, mfColumns } = aggregate(resolved);

  // 未登録税率の件数
  const unresolvedTaxCount = resolved.filter(r => r.解決方法 !== 'skip' && r.解決方法 !== 'no_sku' && r.税率 === null).length;

  // ─── エビデンスCSV生成 ───
  // 1. 明細CSV（元CSVの各行 + 判定結果）
  const detailCols = ['日付','トランザクション種類','注文番号','sku','説明','数量',
    '商品売上','商品の売上税','配送料','配送料の税金','ギフト包装手数料','ギフト包装の税金',
    'Amazonポイント費用','プロモーション割引額','プロモーション割引の税金','手数料','FBA手数料',
    'トランザクション他','その他','合計','商品コード','税率','売上分類','原価','解決方法'];
  let detailCsv = '\uFEFF' + detailCols.join(',') + '\n';
  for (const r of resolved) {
    const vals = detailCols.map(c => {
      const v = r[c];
      if (v === null || v === undefined) return '';
      if (typeof v === 'string' && (v.includes(',') || v.includes('"'))) return '"' + v.replace(/"/g, '""') + '"';
      return v;
    });
    detailCsv += vals.join(',') + '\n';
  }

  // 2. 集計サマリーCSV（税率別 + MF税込 + セグメント別）
  let summaryCsv = '\uFEFF';
  // 税率別
  summaryCsv += '【税率別集計】\n';
  summaryCsv += '税率,' + columns.join(',') + '\n';
  for (const [key, label] of [['10','10%'],['8','8%']]) {
    summaryCsv += label + ',' + columns.map(c => byTax[key][c] || 0).join(',') + '\n';
  }
  summaryCsv += '合計,' + columns.map(c => (byTax['10'][c] || 0) + (byTax['8'][c] || 0)).join(',') + '\n';
  // MF税込
  summaryCsv += '\n【MF連携用 税込み集計】\n';
  summaryCsv += mfColumns.join(',') + '\n';
  summaryCsv += mfColumns.map(c => mfRow[c] || 0).join(',') + '\n';
  // セグメント別
  summaryCsv += '\n【セグメント別集計（管理会計用）】\n';
  summaryCsv += 'セグメント,' + columns.join(',') + ',原価合計\n';
  for (const [key, row] of Object.entries(bySegment)) {
    const label = SEGMENT_NAMES[key] || (key === 'other' ? 'その他/未分類' : key);
    summaryCsv += key + ':' + label + ',' + columns.map(c => row[c] || 0).join(',') + ',' + (row.原価合計 || 0) + '\n';
  }

  evidenceStore.set(yearMonth, { detail: detailCsv, summary: summaryCsv });

  res.json({
    yearMonth,
    totalRows: parsedRows.length,
    resolvedCount: resolved.filter(r => r.解決方法 !== 'unresolved' && r.解決方法 !== 'skip' && r.解決方法 !== 'no_sku').length,
    unresolvedSkus: unresolved,
    unresolvedTaxCount,
    canConfirm: unresolved.length === 0 && unresolvedTaxCount === 0,
    byTax,
    bySegment,
    excluded,
    otherDetails,
    columns,
    mfRow,
    mfColumns,
    segmentNames: SEGMENT_NAMES,
    excludedNames: EXCLUDED_SEGMENTS,
    zeroGenka,
  });
  } catch (e) {
    console.error('[AmazonAccounting] エラー:', e.message, e.stack);
    res.status(500).json({ error: '集計処理エラー: ' + e.message });
  }
});

// ─── HTML ───

function renderPage() {
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Amazon売上集計 - B-Faith</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#333;font-size:14px}
    .header{background:#1a5276;color:white;padding:12px 24px;display:flex;align-items:center;gap:16px}
    .header h1{font-size:18px}
    .header a{color:#aed6f1;text-decoration:none;font-size:13px}
    .wrap{max-width:1800px;margin:16px auto;padding:0 16px}
    .card{background:white;border-radius:8px;padding:20px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1);overflow-x:auto}
    .card h2{font-size:15px;color:#555;margin-bottom:10px}
    .btn{padding:8px 20px;border:none;border-radius:4px;cursor:pointer;font-size:14px}
    .btn-p{background:#2980b9;color:white}.btn-p:hover{background:#1a6da0}
    .btn-s{background:#27ae60;color:white}.btn-s:hover{background:#1e8449}
    .btn:disabled{opacity:.5;cursor:default}
    table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px;white-space:nowrap}
    th{background:#f0f0f0;padding:6px 8px;text-align:left;font-size:12px}
    td{padding:5px 8px;border-bottom:1px solid #eee;text-align:right}
    td:first-child{text-align:left;font-weight:600}
    .warn{background:#fef9e7;border:1px solid #f9e79f;padding:10px;border-radius:4px;margin:8px 0}
    .ok{background:#eafaf1;border:1px solid #a9dfbf;padding:10px;border-radius:4px;margin:8px 0}
    .err{background:#fdedec;border:1px solid #f5b7b1;padding:10px;border-radius:4px;margin:8px 0}
    .excluded{background:#f4ecf7;border:1px solid #d7bde2;padding:10px;border-radius:4px;margin:8px 0;font-size:13px}
    .meta{font-size:12px;color:#888;margin-top:6px}
    #result{display:none}
    .num{font-family:monospace}
    .negative{color:#e74c3c}
    .detail-table td{font-size:12px;font-weight:normal}
    .detail-table th{font-size:11px}
    .modal-overlay{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.5);z-index:1000;justify-content:center;align-items:flex-start;padding:30px}
    .modal-content{background:white;border-radius:8px;max-width:900px;width:100%;max-height:calc(100vh - 60px);overflow-y:auto;padding:24px;position:relative;line-height:1.8}
    .modal-close{position:sticky;top:0;float:right;background:#e74c3c;color:white;border:none;border-radius:50%;width:32px;height:32px;font-size:18px;cursor:pointer;z-index:1}
    .modal-content h2{font-size:16px;color:#1a5276;margin:20px 0 6px;border-bottom:2px solid #aed6f1;padding-bottom:4px}
    .modal-content h3{font-size:13px;color:#555;margin:12px 0 4px}
    .modal-content .m-tbl{border-collapse:collapse;font-size:12px;margin:6px 0;width:auto}
    .modal-content .m-tbl th,.modal-content .m-tbl td{border:1px solid #ddd;padding:4px 8px}
    .modal-content .m-tbl th{background:#f0f0f0}
    .modal-content .flow{background:#eaf2f8;padding:10px;border-radius:6px;font-family:monospace;font-size:12px;margin:6px 0;white-space:pre-line}
    .modal-content .note{background:#fef9e7;border-left:4px solid #f39c12;padding:6px 10px;margin:6px 0;font-size:12px}
    .modal-content ul{margin:4px 0 4px 18px;font-size:13px}
    .modal-content code{background:#f4f4f4;padding:1px 4px;border-radius:3px;font-size:11px}
    .acc-header{cursor:pointer;padding:10px 12px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:4px;margin-bottom:2px;display:flex;justify-content:space-between;align-items:center;font-size:13px}
    .acc-header:hover{background:#e9ecef}
    .acc-header .arrow{transition:transform .2s;font-size:10px}
    .acc-header.open .arrow{transform:rotate(90deg)}
    .acc-body{display:none;padding:12px;border:1px solid #eee;border-top:none;margin-bottom:8px;background:#fff}
    .acc-body.open{display:block}
  </style>
</head>
<body>
  <div class="header">
    <h1>Amazon売上集計</h1>
    <a href="/">← ポータルに戻る</a>
    <a href="#" onclick="document.getElementById('manualModal').style.display='flex';return false" style="margin-left:auto;background:rgba(255,255,255,.2);padding:4px 12px;border-radius:4px">マニュアル</a>
  </div>
  <div class="wrap">
    <div class="card">
      <h2>ペイメントレポートCSVアップロード</h2>
      <p class="meta">セラーセントラル → ペイメント → レポートリポジトリ からダウンロードしたCSVファイル</p>
      <div style="margin-top:12px;display:flex;gap:8px;align-items:center">
        <input type="file" id="csvFile" accept=".csv,.txt">
        <button class="btn btn-p" id="uploadBtn" onclick="doUpload()">アップロード＆集計</button>
      </div>
      <div id="uploadStatus" class="meta" style="margin-top:8px"></div>
    </div>

    <div id="result">
      <div class="card">
        <h2>集計概要</h2>
        <div id="summary"></div>
      </div>

      <div id="unresolvedCard" class="card" style="display:none">
        <h2>⚠️ 未登録SKU</h2>
        <div id="unresolvedList"></div>
      </div>

      <div class="card">
        <h2>税率別集計</h2>
        <div id="taxTable"></div>
      </div>

      <div class="card">
        <h2>MF連携用 税込み集計</h2>
        <div id="mfTable"></div>
      </div>

      <div class="card">
        <h2>セグメント別集計（管理会計用）</h2>
        <div id="segmentTable"></div>
        <div id="excludedInfo"></div>
      </div>

      <div id="otherDetailCard" class="card" style="display:none">
        <h2>「その他/未分類」明細</h2>
        <p class="meta">売上分類が未登録の商品・SKUなし行の内訳</p>
        <div id="otherDetailList"></div>
      </div>

      <div id="zeroGenkaCard" class="card" style="display:none">
        <h2>⚠️ 原価ゼロで計算された商品</h2>
        <p class="meta">商品マスタの原価が0またはNULLのため、原価0円で集計されています。正確な粗利計算には原価登録が必要です。</p>
        <div id="zeroGenkaList"></div>
      </div>

      <div class="card" id="confirmCard">
        <h2>確定・エビデンス</h2>
        <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:8px">
          <label>広告費（税込）: <input type="number" id="adCost" value="0" style="width:120px;padding:4px" oninput="updateAdCost()"></label>
          <button class="btn btn-s" id="confirmBtn" onclick="doConfirm()">この月の集計を確定</button>
        </div>
        <div style="display:flex;gap:8px;margin-top:8px">
          <button class="btn btn-p" onclick="downloadEvidence('detail')">明細エビデンスCSV</button>
          <button class="btn btn-p" onclick="downloadEvidence('summary')">集計サマリーCSV</button>
        </div>
        <p class="meta" style="margin-top:6px">明細: アップロードCSVの全行+税率・分類・原価の判定結果 / 集計: 税率別+MF税込+セグメント別</p>
        <div id="confirmStatus" class="meta"></div>
      </div>
    </div>

    <div class="card">
      <h2>過去の確定データ</h2>
      <div style="margin-bottom:8px"><button class="btn btn-p" onclick="downloadHistoryCsv()">セグメント別集計CSVダウンロード</button></div>
      <div id="historyList"><span class="meta">読み込み中...</span></div>
    </div>
  </div>

  <script>
    const fmt = n => {
      if (n === 0) return '0';
      const s = Math.round(n).toLocaleString();
      return n < 0 ? '<span class="negative">' + s + '</span>' : s;
    };

    async function doUpload() {
      const fileInput = document.getElementById('csvFile');
      if (!fileInput.files.length) { alert('ファイルを選択してください'); return; }
      const btn = document.getElementById('uploadBtn');
      btn.disabled = true;
      btn.textContent = '処理中...';
      document.getElementById('uploadStatus').textContent = 'アップロード中...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);

      try {
        const r = await fetch(location.pathname + '/upload', { method: 'POST', body: formData });
        const data = await r.json();
        if (data.error) { document.getElementById('uploadStatus').innerHTML = '<span class="negative">エラー: ' + data.error + '</span>'; return; }
        showResult(data);
      } catch(e) {
        document.getElementById('uploadStatus').innerHTML = '<span class="negative">エラー: ' + e.message + '</span>';
      }
      btn.disabled = false;
      btn.textContent = 'アップロード＆集計';
    }

    function showResult(data) {
      lastData = data;
      document.getElementById('result').style.display = 'block';
      document.getElementById('uploadStatus').textContent = '';

      // 概要
      let summaryHtml = '<div class="' + (data.canConfirm ? 'ok' : 'warn') + '">';
      summaryHtml += '<b>対象年月: ' + data.yearMonth + '</b><br>';
      summaryHtml += '総行数: ' + data.totalRows + ' / SKU解決済: ' + data.resolvedCount + ' / 未登録SKU: ' + data.unresolvedSkus.length + '件';
      if (data.unresolvedTaxCount > 0) summaryHtml += ' / <span class="negative">税率未登録: ' + data.unresolvedTaxCount + '件（10%仮扱い）</span>';
      if (data.canConfirm) summaryHtml += '<br><b style="color:#27ae60">✅ 全SKU解決済み — 確定可能</b>';
      else summaryHtml += '<br><b style="color:#e74c3c">❌ 未登録SKUあり — 確定不可</b>';
      summaryHtml += '</div>';
      document.getElementById('summary').innerHTML = summaryHtml;

      // 未登録SKU
      if (data.unresolvedSkus.length > 0) {
        const card = document.getElementById('unresolvedCard');
        card.style.display = 'block';
        let html = '<table><tr><th>SKU</th><th>商品名</th><th>出現数</th><th>金額合計</th></tr>';
        for (const u of data.unresolvedSkus) {
          html += '<tr><td>' + u.sku + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('unresolvedList').innerHTML = html;
      } else {
        document.getElementById('unresolvedCard').style.display = 'none';
      }

      // 税率別
      const cols = data.columns;
      let taxHtml = '<table><tr><th>税率</th>';
      cols.forEach(c => taxHtml += '<th>' + c + '</th>');
      taxHtml += '</tr>';
      for (const [key, label] of [['10', '10%'], ['8', '8%']]) {
        const row = data.byTax[key];
        taxHtml += '<tr><td>' + label + '</td>';
        cols.forEach(c => taxHtml += '<td class="num">' + fmt(row[c]) + '</td>');
        taxHtml += '</tr>';
      }
      taxHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      cols.forEach(c => taxHtml += '<td class="num">' + fmt((data.byTax['10'][c] || 0) + (data.byTax['8'][c] || 0)) + '</td>');
      taxHtml += '</tr></table>';
      document.getElementById('taxTable').innerHTML = taxHtml;

      // MF連携用 税込み集計
      if (data.mfRow && data.mfColumns) {
        let mfHtml = '<table><tr><th style="text-align:center" colspan="' + data.mfColumns.length + '">税込み</th></tr><tr>';
        data.mfColumns.forEach(c => mfHtml += '<th>' + c + '</th>');
        mfHtml += '</tr><tr>';
        data.mfColumns.forEach(c => mfHtml += '<td class="num" style="font-weight:bold">' + fmt(data.mfRow[c]) + '</td>');
        mfHtml += '</tr></table>';
        document.getElementById('mfTable').innerHTML = mfHtml;
      }

      // セグメント別（1〜3 + other。4=輸出は除外）
      renderSegmentTable('segmentTable', data.bySegment, data.segmentNames, cols, null);
    }

    function renderSegmentTable(targetId, bySegment, segmentNames, cols, adCost) {
      const ad = adCost !== null ? adCost : (parseFloat(document.getElementById('adCost')?.value) || 0);

      // 広告費を売上按分: セグメント1・2の商品売上比率で配分（3とotherは対象外）
      const adTargets = ['1', '2'];
      const salesByKey = {};
      let totalSales = 0;
      for (const [key, row] of Object.entries(bySegment)) {
        const s = row['商品売上'] || 0;
        salesByKey[key] = s;
        if (adTargets.includes(key)) totalSales += s;
      }
      const adByKey = {};
      let adSum = 0;
      const keys = Object.keys(bySegment);
      for (const key of keys) {
        if (!adTargets.includes(key) || totalSales === 0) { adByKey[key] = 0; continue; }
        const share = Math.round(ad * salesByKey[key] / totalSales);
        adByKey[key] = share;
        adSum += share;
      }
      // 丸め誤差を最大セグメントに調整
      if (ad && totalSales > 0) {
        const maxKey = keys.filter(k => adTargets.includes(k)).sort((a, b) => (salesByKey[b] || 0) - (salesByKey[a] || 0))[0];
        if (maxKey) adByKey[maxKey] += (ad - adSum);
      }

      let segHtml = '<table><tr><th>セグメント</th>';
      cols.forEach(c => segHtml += '<th>' + c + '</th>');
      segHtml += '<th>広告費</th><th>原価合計</th></tr>';
      let totalRow = {};
      cols.forEach(c => totalRow[c] = 0);
      totalRow.原価合計 = 0;
      let totalAd = 0;
      for (const [key, row] of Object.entries(bySegment)) {
        const label = segmentNames[key] || (key === 'other' ? 'その他/未分類' : key);
        segHtml += '<tr><td>' + key + ': ' + label + '</td>';
        cols.forEach(c => { segHtml += '<td class="num">' + fmt(row[c] || 0) + '</td>'; totalRow[c] += (row[c] || 0); });
        segHtml += '<td class="num">' + fmt(adByKey[key] || 0) + '</td>';
        totalAd += (adByKey[key] || 0);
        segHtml += '<td class="num">' + fmt(row.原価合計 || 0) + '</td>';
        totalRow.原価合計 += (row.原価合計 || 0);
        segHtml += '</tr>';
      }
      segHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      cols.forEach(c => segHtml += '<td class="num">' + fmt(totalRow[c]) + '</td>');
      segHtml += '<td class="num">' + fmt(totalAd) + '</td>';
      segHtml += '<td class="num">' + fmt(totalRow.原価合計) + '</td></tr></table>';
      document.getElementById(targetId).innerHTML = segHtml;

      // 除外セグメント（4=輸出）
      let exclHtml = '';
      if (data.excluded) {
        for (const [key, row] of Object.entries(data.excluded)) {
          if (row.行数 > 0) {
            const label = data.excludedNames[key] || key;
            exclHtml += '<div class="excluded">';
            exclHtml += '<b>除外: ' + key + ': ' + label + '</b>（' + row.行数 + '行）';
            exclHtml += ' — 商品売上: ' + fmt(row['商品売上']) + ' / 合計: ' + fmt(row['合計']) + ' / 原価合計: ' + fmt(row.原価合計);
            exclHtml += '</div>';
          }
        }
      }
      document.getElementById('excludedInfo').innerHTML = exclHtml;

      // 「その他/未分類」明細
      if (data.otherDetails && data.otherDetails.length > 0) {
        const card = document.getElementById('otherDetailCard');
        card.style.display = 'block';
        let html = '<table class="detail-table"><tr><th>SKU</th><th>商品コード</th><th>商品名</th><th>種類</th><th>解決方法</th><th>行数</th><th>数量</th><th>商品売上</th><th>合計</th></tr>';
        for (const d of data.otherDetails) {
          const method = { direct: '商品コード一致', sku_map: 'SKUマップ経由', unresolved: '未解決', no_sku: 'SKUなし' }[d.解決方法] || d.解決方法;
          html += '<tr>';
          html += '<td style="text-align:left">' + (d.sku || '-') + '</td>';
          html += '<td style="text-align:left">' + (d.商品コード || '-') + '</td>';
          html += '<td style="text-align:left">' + (d.商品名 || '').slice(0, 50) + '</td>';
          html += '<td style="text-align:left">' + (d.トランザクション種類 || '-') + '</td>';
          html += '<td style="text-align:left">' + method + '</td>';
          html += '<td class="num">' + d.count + '</td>';
          html += '<td class="num">' + d.数量 + '</td>';
          html += '<td class="num">' + fmt(d.商品売上) + '</td>';
          html += '<td class="num">' + fmt(d.合計) + '</td>';
          html += '</tr>';
        }
        html += '</table>';
        document.getElementById('otherDetailList').innerHTML = html;
      } else {
        document.getElementById('otherDetailCard').style.display = 'none';
      }

      // 原価ゼロ警告
      if (data.zeroGenka && data.zeroGenka.length > 0) {
        const card = document.getElementById('zeroGenkaCard');
        card.style.display = 'block';
        let html = '<div class="warn" style="margin-bottom:8px"><b>' + data.zeroGenka.length + '商品</b>が原価0円で計算されています</div>';
        html += '<table class="detail-table"><tr><th>商品コード</th><th>SKU</th><th>商品名</th><th>出現行数</th><th>数量合計</th><th>商品売上合計</th></tr>';
        for (const z of data.zeroGenka) {
          html += '<tr>';
          html += '<td style="text-align:left">' + z.商品コード + '</td>';
          html += '<td style="text-align:left">' + (z.sku || '-') + '</td>';
          html += '<td style="text-align:left">' + (z.商品名 || '').slice(0, 50) + '</td>';
          html += '<td class="num">' + z.count + '</td>';
          html += '<td class="num">' + z.数量合計 + '</td>';
          html += '<td class="num">' + fmt(z.売上合計) + '</td>';
          html += '</tr>';
        }
        html += '</table>';
        document.getElementById('zeroGenkaList').innerHTML = html;
      } else {
        document.getElementById('zeroGenkaCard').style.display = 'none';
      }
    }
    let lastData = null;

    function updateAdCost() {
      if (!lastData) return;
      renderSegmentTable('segmentTable', lastData.bySegment, lastData.segmentNames, lastData.columns, null);
    }

    function downloadEvidence(type) {
      if (!lastData) { alert('先にCSVをアップロードしてください'); return; }
      window.open(location.pathname + '/evidence/' + type + '/' + lastData.yearMonth);
    }

    async function doConfirm() {
      if (!lastData) { alert('先にCSVをアップロードしてください'); return; }
      if (!confirm(lastData.yearMonth + ' の集計を確定しますか？')) return;
      const btn = document.getElementById('confirmBtn');
      btn.disabled = true;
      btn.textContent = '保存中...';
      try {
        const adCost = parseFloat(document.getElementById('adCost').value) || 0;
        const r = await fetch(location.pathname + '/confirm', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            yearMonth: lastData.yearMonth,
            totalRows: lastData.totalRows,
            resolvedCount: lastData.resolvedCount,
            unresolvedCount: lastData.unresolvedSkus?.length || 0,
            byTax: lastData.byTax,
            bySegment: lastData.bySegment,
            excluded: lastData.excluded,
            mfRow: lastData.mfRow,
            adCost,
          }),
        });
        const result = await r.json();
        if (result.ok) {
          document.getElementById('confirmStatus').innerHTML = '<span style="color:#27ae60">OK ' + lastData.yearMonth + ' 確定済（' + result.confirmed_at + '）</span>';
          loadHistory();
        } else {
          document.getElementById('confirmStatus').innerHTML = '<span class="negative">エラー: ' + (result.error || '') + '</span>';
        }
      } catch(e) {
        document.getElementById('confirmStatus').innerHTML = '<span class="negative">エラー: ' + e.message + '</span>';
      }
      btn.disabled = false;
      btn.textContent = 'この月の集計を確定';
    }

    async function loadHistory() {
      try {
        const r = await fetch(location.pathname + '/history');
        const rows = await r.json();
        if (!rows.length) {
          document.getElementById('historyList').innerHTML = '<span class="meta">確定データはまだありません</span>';
          return;
        }
        let html = '';
        for (let i = 0; i < rows.length; i++) {
          const row = rows[i];
          const mf = row.mf_row || {};
          const sales10 = mf['商品売上(10%)'] || 0;
          const sales8 = mf['商品売上(8%)'] || 0;
          const total = mf['合計'] || 0;
          const ad = Math.round(row.ad_cost || 0);

          // ヘッダーの合計: セグメント全体の商品売上と合計を集計
          const segAll = row.by_segment || {};
          let hdrSales = 0, hdrTotal = 0;
          for (const sr of Object.values(segAll)) { hdrSales += (sr['商品売上'] || 0) + (sr['商品の売上税'] || 0); hdrTotal += (sr['合計'] || 0); }

          html += '<div class="acc-header" onclick="toggleAcc(this)" data-idx="' + i + '">';
          html += '<span><b>' + row.year_month + '</b> — 商品売上(税込): \\u00a5' + Math.round(hdrSales).toLocaleString()
            + ' / 合計: \\u00a5' + Math.round(hdrTotal).toLocaleString()
            + (ad ? ' / 広告費: \\u00a5' + ad.toLocaleString() : '')
            + ' <span class="meta">（' + (row.confirmed_at || '') + '）</span></span>';
          html += '<span class="arrow">&#9654;</span></div>';
          html += '<div class="acc-body" id="acc-' + i + '">';

          // MF連携用 税込み集計（データがある場合のみ）
          const hasMf = mf && Object.keys(mf).length > 0 && (mf['合計'] || 0) !== 0;
          if (hasMf) {
            const mfCols = ['商品売上(10%)', '商品売上(8%)', '配送料', 'ギフト包装手数料',
              'Amazonポイントの費用', 'プロモーション割引額', '手数料', 'FBA手数料',
              'トランザクションに関するその他の手数料+その他', '合計', '端数調整'];
            html += '<h3 style="font-size:13px;color:#555;margin-bottom:4px">MF連携用 税込み集計</h3>';
            html += '<table><tr><th style="text-align:center" colspan="' + mfCols.length + '">税込み</th></tr><tr>';
            mfCols.forEach(c => html += '<th>' + c + '</th>');
            html += '</tr><tr>';
            mfCols.forEach(c => html += '<td class="num" style="font-weight:bold">' + fmt(mf[c] || 0) + '</td>');
            html += '</tr></table>';
          }

          // セグメント別集計
          const seg = row.by_segment || {};
          const segNames = {1:'自社商品', 2:'取扱限定', 3:'仕入れ商品'};
          const segCols = ['商品売上', '商品の売上税', '配送料', '配送料の税金',
            'ギフト包装手数料', 'ギフト包装の税金', 'Amazonポイント費用',
            'プロモーション割引額', 'プロモーション割引の税金', '手数料', 'FBA手数料',
            'トランザクション他', 'その他', '合計'];
          html += '<h3 style="font-size:13px;color:#555;margin:12px 0 4px">セグメント別集計（管理会計用）</h3>';
          // 広告費を売上按分（セグメント1・2のみ）
          const hAdTargets = ['1', '2'];
          const hSales = {}; let hTotalSales = 0;
          for (const [k, sr] of Object.entries(seg)) { const s = sr['商品売上'] || 0; hSales[k] = s; if (hAdTargets.includes(k)) hTotalSales += s; }
          const hAd = {}; let hAdSum = 0;
          const segKeys = Object.keys(seg);
          for (const k of segKeys) {
            if (!hAdTargets.includes(k) || hTotalSales === 0) { hAd[k] = 0; continue; }
            hAd[k] = Math.round(ad * hSales[k] / hTotalSales); hAdSum += hAd[k];
          }
          if (ad && hTotalSales > 0) {
            const mk = segKeys.filter(k => hAdTargets.includes(k)).sort((a, b) => (hSales[b]||0) - (hSales[a]||0))[0];
            if (mk) hAd[mk] += (ad - hAdSum);
          }

          html += '<table><tr><th>セグメント</th>';
          segCols.forEach(c => html += '<th>' + c + '</th>');
          html += '<th>広告費</th><th>原価合計</th></tr>';
          let sTot = {}; segCols.forEach(c => sTot[c] = 0); sTot.原価合計 = 0; let sAdTot = 0;
          for (const [key, sr] of Object.entries(seg)) {
            const lb = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
            html += '<tr><td>' + key + ': ' + lb + '</td>';
            segCols.forEach(c => { html += '<td class="num">' + fmt(sr[c] || 0) + '</td>'; sTot[c] += (sr[c] || 0); });
            html += '<td class="num">' + fmt(hAd[key] || 0) + '</td>';
            sAdTot += (hAd[key] || 0);
            html += '<td class="num">' + fmt(sr.原価合計 || 0) + '</td>';
            sTot.原価合計 += (sr.原価合計 || 0);
            html += '</tr>';
          }
          html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
          segCols.forEach(c => html += '<td class="num">' + fmt(sTot[c]) + '</td>');
          html += '<td class="num">' + fmt(sAdTot) + '</td>';
          html += '<td class="num">' + fmt(sTot.原価合計) + '</td></tr></table>';

          // 除外セグメント
          const excl = row.excluded || {};
          for (const [ek, er] of Object.entries(excl)) {
            if ((er.行数 || 0) > 0) {
              html += '<div class="excluded"><b>除外: ' + ek + ': 輸出</b>（' + er.行数 + '行） — 商品売上: ' + fmt(er['商品売上'] || 0) + ' / 合計: ' + fmt(er['合計'] || 0) + '</div>';
            }
          }

          html += '</div>';
        }
        document.getElementById('historyList').innerHTML = html;
      } catch(e) {
        document.getElementById('historyList').innerHTML = '<span class="meta">読み込みエラー</span>';
      }
    }

    function toggleAcc(el) {
      const idx = el.dataset.idx;
      const body = document.getElementById('acc-' + idx);
      el.classList.toggle('open');
      body.classList.toggle('open');
    }

    async function downloadHistoryCsv() {
      try {
        const r = await fetch(location.pathname + '/history');
        const rows = await r.json();
        if (!rows.length) { alert('確定データがありません'); return; }

        const segNames = {1:'自社商品', 2:'取扱限定', 3:'仕入れ商品', other:'その他/未分類'};
        const segCols = ['商品売上','商品の売上税','配送料','配送料の税金','ギフト包装手数料','ギフト包装の税金','Amazonポイント費用','プロモーション割引額','プロモーション割引の税金','手数料','FBA手数料','トランザクション他','その他','合計'];
        const adTargets = ['1','2'];

        let csv = '\\uFEFF'; // BOM
        csv += '集計月,セグメント,' + segCols.join(',') + ',広告費,原価合計\\n';

        for (const row of rows) {
          const seg = row.by_segment || {};
          const ad = row.ad_cost || 0;
          // 按分計算
          let tSales = 0;
          const sales = {};
          for (const [k, sr] of Object.entries(seg)) { sales[k] = sr['商品売上'] || 0; if (adTargets.includes(k)) tSales += sales[k]; }
          const adMap = {};
          let adSum = 0;
          const keys = Object.keys(seg);
          for (const k of keys) {
            if (!adTargets.includes(k) || tSales === 0) { adMap[k] = 0; continue; }
            adMap[k] = Math.round(ad * sales[k] / tSales); adSum += adMap[k];
          }
          if (ad && tSales > 0) {
            const mk = keys.filter(k => adTargets.includes(k)).sort((a,b) => (sales[b]||0)-(sales[a]||0))[0];
            if (mk) adMap[mk] += (ad - adSum);
          }

          // 集計月を yyyy/mm/dd（月末日）形式に変換
          const [y, m] = row.year_month.split('-');
          const lastDay = new Date(parseInt(y), parseInt(m), 0).getDate();
          const ymStr = y + '/' + m + '/' + String(lastDay).padStart(2, '0');

          for (const [key, sr] of Object.entries(seg)) {
            const label = segNames[key] || key;
            const vals = segCols.map(c => sr[c] || 0);
            csv += ymStr + ',' + key + ':' + label + ',' + vals.join(',') + ',' + (adMap[key] || 0) + ',' + (sr.原価合計 || 0) + '\\n';
          }
        }

        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'Amazon_segment_history.csv';
        a.click();
      } catch(e) {
        alert('ダウンロードエラー: ' + e.message);
      }
    }

    loadHistory();
  </script>

  <div class="modal-overlay" id="manualModal" onclick="if(event.target===this)this.style.display='none'">
    <div class="modal-content">
      <button class="modal-close" onclick="document.getElementById('manualModal').style.display='none'">&times;</button>

      <h2>1. 概要</h2>
      <p>Amazonセラーセントラルのペイメントレポート（CSV）をアップロードすると、以下を自動計算するツールです。</p>
      <ul>
        <li><b>税率別集計</b> — 10%/8%に分類して集計（MF連携用）</li>
        <li><b>MF連携用 税込み集計</b> — マネーフォワードに入力する金額</li>
        <li><b>セグメント別集計</b> — 管理会計用（自社商品/取扱限定/仕入れ商品/その他）</li>
      </ul>
      <p>従来GAS（Google Apps Script）で7工程かけていた月次売上集計を、CSV1回のアップロードで完了します。</p>

      <h2>2. 全体の処理フロー</h2>
      <div class="flow">セラセンからCSVダウンロード
  ↓
ツールにアップロード
  ↓
① CSV解析（先頭7行スキップ、8行目以降がデータ）
  ↓
② SKU解決（3段階照合 → 商品コード・原価・税率・売上分類を特定）
  ↓
③ 集計（税率別・MF税込・セグメント別）
  ↓
④ プレビュー確認 → エビデンスCSVダウンロード
  ↓
⑤ 広告費入力 → 確定保存</div>

      <h2>3. CSVの取得方法</h2>
      <p>セラーセントラル → <b>ペイメント</b> → <b>レポートリポジトリ</b> → 対象月のペイメントレポートをダウンロード。</p>
      <div class="note">CSVは先頭7行が説明文、8行目がヘッダー、9行目以降がデータです。ツールが自動的にスキップします。</div>

      <h2>4. SKU解決（3段階）</h2>
      <p>CSVの各行のSKUから商品マスタを照合し、原価・税率・売上分類を特定します。</p>
      <table class="m-tbl">
        <tr><th>段階</th><th>処理</th><th>参照先</th></tr>
        <tr><td>Stage 1</td><td>SKUが商品コードと直接一致するか</td><td>mirror_products</td></tr>
        <tr><td>Stage 2</td><td>SKUマップで変換してから商品コードを検索</td><td>mirror_sku_map → mirror_products</td></tr>
        <tr><td>Stage 3</td><td>どちらにも一致しない → <b>未登録SKU</b></td><td>—</td></tr>
      </table>
      <div class="note">SKUと商品コードは全て<b>小文字に統一</b>して照合しています。</div>

      <h3>解決結果の列（エビデンスCSVに出力）</h3>
      <table class="m-tbl">
        <tr><th>列</th><th>内容</th></tr>
        <tr><td>商品コード</td><td>照合で特定されたNE商品コード（未解決の場合は空）</td></tr>
        <tr><td>税率</td><td>10 or 8（商品マスタの消費税率から判定）</td></tr>
        <tr><td>売上分類</td><td>1:自社 / 2:取扱限定 / 3:仕入れ / 4:輸出 / 空:未分類</td></tr>
        <tr><td>原価</td><td>商品マスタの原価（未解決 or 原価未登録の場合は0）</td></tr>
        <tr><td>解決方法</td><td>direct / sku_map / unresolved / no_sku / skip</td></tr>
      </table>

      <h2>5. 税率別集計</h2>
      <table class="m-tbl">
        <tr><th>分類</th><th>条件</th></tr>
        <tr><td><b>10%</b></td><td>消費税率=0.10 の商品、または税率未登録の商品（10%仮扱い）</td></tr>
        <tr><td><b>8%</b></td><td>消費税率=0.08 の商品</td></tr>
      </table>
      <div class="note">トランザクション種類が「振込み」の行は集計から除外されます。</div>

      <h2>6. MF連携用 税込み集計</h2>
      <p>マネーフォワードへの入力用に、税込み金額に変換して集計します。</p>
      <table class="m-tbl">
        <tr><th>項目</th><th>計算方法</th></tr>
        <tr><td>商品売上(10%)</td><td>10%の商品売上 + 商品の売上税</td></tr>
        <tr><td>商品売上(8%)</td><td>8%の商品売上 + 商品の売上税</td></tr>
        <tr><td>配送料</td><td>全税率の配送料 + 配送料の税金</td></tr>
        <tr><td>ギフト包装手数料</td><td>全税率のギフト包装手数料 + 税金</td></tr>
        <tr><td>Amazonポイント</td><td>全税率合計</td></tr>
        <tr><td>プロモーション割引額</td><td>全税率の割引額 + 税金</td></tr>
        <tr><td>手数料 / FBA手数料</td><td>全税率合計</td></tr>
        <tr><td>トランザクション他+その他</td><td>全税率合計</td></tr>
        <tr><td>端数調整</td><td>合計 − 各項目の合計（丸め誤差の吸収）</td></tr>
      </table>

      <h2>7. セグメント別集計（管理会計用）</h2>
      <table class="m-tbl">
        <tr><th>セグメント</th><th>売上分類</th><th>内容</th><th>広告費</th></tr>
        <tr><td><b>1: 自社商品</b></td><td>1</td><td>自社ブランド・独占商品</td><td>売上按分あり</td></tr>
        <tr><td><b>2: 取扱限定</b></td><td>2</td><td>取扱限定品</td><td>売上按分あり</td></tr>
        <tr><td><b>3: 仕入れ商品</b></td><td>3</td><td>一般仕入れ商品</td><td>なし</td></tr>
        <tr><td><b>other: その他</b></td><td>空/未登録</td><td>SKUなし行（FBA保管手数料等）+ 分類未登録商品</td><td>なし</td></tr>
      </table>
      <div class="note"><b>セグメント4（輸出）</b>は集計テーブルから除外、別枠で表示されます。</div>

      <h3>広告費の按分</h3>
      <p>Amazon広告はクレカ払い（ペイメントCSVに含まれない）のため手入力で追加。セグメント1と2の<b>商品売上比率</b>で按分します。</p>
      <p>例: 広告費100万、セグメント1売上4,000万、セグメント2売上1,000万 → 1に80万、2に20万</p>

      <h3>原価合計</h3>
      <p>各行の <code>原価 × 数量</code> をセグメントごとに合算（税抜）。</p>

      <h2>8. エビデンスCSV</h2>
      <p>アップロード後に2種類ダウンロード可能:</p>
      <table class="m-tbl">
        <tr><th>種類</th><th>内容</th></tr>
        <tr><td><b>明細エビデンス</b></td><td>元CSVの全行 + 商品コード・税率・売上分類・原価・解決方法</td></tr>
        <tr><td><b>集計サマリー</b></td><td>税率別 + MF税込 + セグメント別の集計表</td></tr>
      </table>
      <div class="note">エビデンスはアップロード時にメモリに一時保存。ページを離れると再アップロードが必要です。</div>

      <h2>9. 確定保存・過去データ</h2>
      <ul>
        <li>広告費を入力して「確定」→ DBに保存</li>
        <li>同じ年月で再確定すると上書き</li>
        <li>確定済みデータはアコーディオンで展開表示</li>
        <li>「セグメント別集計CSVダウンロード」で全月分を一括CSV出力（集計月は各月末日 yyyy/mm/dd）</li>
      </ul>

      <h2>10. データソース・更新</h2>
      <table class="m-tbl">
        <tr><th>データ</th><th>ソース</th><th>更新</th></tr>
        <tr><td>商品マスタ / SKUマップ</td><td>ミニPC → Render</td><td>毎朝7時自動同期</td></tr>
        <tr><td>ペイメントCSV</td><td>セラセン手動DL</td><td>月次</td></tr>
        <tr><td>広告費</td><td>手入力（クレカ払い）</td><td>月次</td></tr>
      </table>

      <h2>11. 注意事項</h2>
      <ul>
        <li>CSV金額のカンマ区切り（例: <code>3,200</code>）は自動除去されます</li>
        <li>税率未登録の商品は<b>10%仮扱い</b>（概要に件数表示）</li>
        <li>原価0の商品は「原価ゼロ警告」タブに一覧表示</li>
        <li>未登録SKUがあると確定不可（先にミニPC管理画面で登録）</li>
        <li>2022/7〜2026/2のヒストリカルデータは旧スプレッドシートから移行済み</li>
      </ul>
    </div>
  </div>
</body>

</html>`;
}

// ─── GET /evidence/:type/:yearMonth — エビデンスCSVダウンロード ───

router.get('/evidence/:type/:yearMonth', (req, res) => {
  const { type, yearMonth } = req.params;
  const ev = evidenceStore.get(yearMonth);
  if (!ev) return res.status(404).json({ error: yearMonth + ' のエビデンスがありません。先にCSVをアップロードしてください。' });

  const csv = type === 'detail' ? ev.detail : ev.summary;
  if (!csv) return res.status(404).json({ error: 'データが見つかりません' });

  const filename = type === 'detail'
    ? 'Amazon_' + yearMonth + '_明細エビデンス.csv'
    : 'Amazon_' + yearMonth + '_集計サマリー.csv';
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="' + encodeURIComponent(filename) + '"');
  res.send(csv);
});

// ─── POST /confirm — 集計確定（DB保存） ───

router.post('/confirm', (req, res) => {
  const db = getMirrorDB();
  const { yearMonth, totalRows, resolvedCount, unresolvedCount,
    byTax, bySegment, excluded, mfRow, adCost, csvFilename } = req.body;

  if (!yearMonth) return res.status(400).json({ error: 'yearMonth は必須です' });

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    db.prepare(`INSERT OR REPLACE INTO mart_amazon_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, ad_cost, confirmed_at, csv_filename)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      yearMonth, totalRows || 0, resolvedCount || 0, unresolvedCount || 0,
      JSON.stringify(byTax), JSON.stringify(bySegment), JSON.stringify(excluded),
      JSON.stringify(mfRow), adCost || 0, now, csvFilename || ''
    );

    db.prepare(`INSERT INTO mart_amazon_upload_log
      (year_month, filename, total_rows, resolved_count, unresolved_count, uploaded_at)
      VALUES (?,?,?,?,?,?)
    `).run(yearMonth, csvFilename || '', totalRows || 0, resolvedCount || 0, unresolvedCount || 0, now);

    res.json({ ok: true, yearMonth, confirmed_at: now });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── GET /history — 過去月一覧 ───

router.get('/history', (req, res) => {
  const db = getMirrorDB();
  try {
    const rows = db.prepare('SELECT * FROM mart_amazon_monthly_summary ORDER BY year_month DESC').all();
    const parsed = rows.map(r => ({
      ...r,
      by_tax: JSON.parse(r.by_tax || '{}'),
      by_segment: JSON.parse(r.by_segment || '{}'),
      excluded: JSON.parse(r.excluded || '{}'),
      mf_row: JSON.parse(r.mf_row || '{}'),
    }));
    res.json(parsed);
  } catch (e) {
    res.json([]);
  }
});

// ─── GET /history/:yearMonth — 特定月の詳細 ───

router.get('/history/:yearMonth', (req, res) => {
  const db = getMirrorDB();
  try {
    const row = db.prepare('SELECT * FROM mart_amazon_monthly_summary WHERE year_month = ?').get(req.params.yearMonth);
    if (!row) return res.status(404).json({ error: '該当月のデータがありません' });
    res.json({
      ...row,
      by_tax: JSON.parse(row.by_tax || '{}'),
      by_segment: JSON.parse(row.by_segment || '{}'),
      excluded: JSON.parse(row.excluded || '{}'),
      mf_row: JSON.parse(row.mf_row || '{}'),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── POST /import-history — ヒストリカルデータ一括投入 ───

router.post('/import-history', (req, res) => {
  // APIキー認証（一時投入用）
  const key = req.headers['x-import-key'] || req.query.key;
  if (key !== 'bfaith-import-2026') return res.status(401).json({ error: 'Invalid key' });

  const db = getMirrorDB();
  const { months } = req.body;
  if (!Array.isArray(months)) return res.status(400).json({ error: 'months 配列が必要です' });

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    const stmt = db.prepare(`INSERT OR IGNORE INTO mart_amazon_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, ad_cost, confirmed_at, csv_filename)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`);

    let inserted = 0;
    const tx = db.transaction(() => {
      for (const m of months) {
        const r = stmt.run(
          m.yearMonth, 0, 0, 0,
          '{}', JSON.stringify(m.bySegment || {}), '{}', '{}',
          m.adCost || 0, now, 'historical-import'
        );
        if (r.changes > 0) inserted++;
      }
    });
    tx();
    res.json({ ok: true, inserted, total: months.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


export default router;
