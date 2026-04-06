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
    .wrap{max-width:1200px;margin:16px auto;padding:0 16px}
    .card{background:white;border-radius:8px;padding:20px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1)}
    .card h2{font-size:15px;color:#555;margin-bottom:10px}
    .btn{padding:8px 20px;border:none;border-radius:4px;cursor:pointer;font-size:14px}
    .btn-p{background:#2980b9;color:white}.btn-p:hover{background:#1a6da0}
    .btn-s{background:#27ae60;color:white}.btn-s:hover{background:#1e8449}
    .btn:disabled{opacity:.5;cursor:default}
    table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px}
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
        <h2>確定</h2>
        <div style="display:flex;gap:12px;align-items:center;margin-bottom:8px">
          <label>広告費（税込）: <input type="number" id="adCost" value="0" style="width:120px;padding:4px" oninput="updateAdCost()"></label>
          <button class="btn btn-s" id="confirmBtn" onclick="doConfirm()">この月の集計を確定</button>
        </div>
        <div id="confirmStatus" class="meta"></div>
      </div>
    </div>

    <div class="card">
      <h2>過去の確定データ</h2>
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
      let segHtml = '<table><tr><th>セグメント</th>';
      cols.forEach(c => segHtml += '<th>' + c + '</th>');
      segHtml += '<th>原価合計</th></tr>';
      let totalRow = {};
      cols.forEach(c => totalRow[c] = 0);
      totalRow.原価合計 = 0;
      for (const [key, row] of Object.entries(bySegment)) {
        const label = segmentNames[key] || (key === 'other' ? 'その他/未分類' : key);
        segHtml += '<tr><td>' + key + ': ' + label + '</td>';
        cols.forEach(c => {
          let v = row[c] || 0;
          if (key === 'other' && c === 'その他') v -= ad;
          segHtml += '<td class="num">' + fmt(v) + '</td>';
          totalRow[c] += v;
        });
        segHtml += '<td class="num">' + fmt(row.原価合計 || 0) + '</td>';
        totalRow.原価合計 += (row.原価合計 || 0);
        segHtml += '</tr>';
      }
      segHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      cols.forEach(c => segHtml += '<td class="num">' + fmt(totalRow[c]) + '</td>');
      segHtml += '<td class="num">' + fmt(totalRow.原価合計) + '</td></tr></table>';
      if (ad) segHtml += '<div class="meta" style="margin-top:4px">※ 広告費 ' + fmt(ad) + ' をその他/未分類の「その他」列に含んでいます</div>';
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
          html += '<table><tr><th>セグメント</th>';
          segCols.forEach(c => html += '<th>' + c + '</th>');
          html += '<th>原価合計</th></tr>';
          let sTot = {}; segCols.forEach(c => sTot[c] = 0); sTot.原価合計 = 0;
          for (const [key, sr] of Object.entries(seg)) {
            const lb = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
            html += '<tr><td>' + key + ': ' + lb + '</td>';
            segCols.forEach(c => {
              let v = sr[c] || 0;
              if (key === 'other' && c === 'その他') v -= ad;
              html += '<td class="num">' + fmt(v) + '</td>';
              sTot[c] += v;
            });
            html += '<td class="num">' + fmt(sr.原価合計 || 0) + '</td>';
            sTot.原価合計 += (sr.原価合計 || 0);
            html += '</tr>';
          }
          html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
          segCols.forEach(c => html += '<td class="num">' + fmt(sTot[c]) + '</td>');
          html += '<td class="num">' + fmt(sTot.原価合計) + '</td></tr></table>';
          if (ad) html += '<div class="meta">※ 広告費 ' + fmt(ad) + ' をその他/未分類の「その他」列に含んでいます</div>';

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

    loadHistory();
  </script>
</body>

</html>`;
}

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

