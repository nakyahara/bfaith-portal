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

// セグメント名称マップ
const SEGMENT_NAMES = { 1: '自社商品', 2: '取扱限定', 3: '仕入れ商品', 4: 'その他' };

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
  // mirror_productsの商品コードマップ
  const productsMap = new Map();
  for (const p of db.prepare('SELECT * FROM mirror_products').all()) {
    productsMap.set(p.商品コード, p);
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

  return { resolved, unresolved: [...unresolved.values()] };
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

  // セグメント別
  const bySegment = { '1': emptyRow(), '2': emptyRow(), '3': emptyRow(), '4': emptyRow(), 'other': emptyRow() };

  for (const row of resolvedRows) {
    if (row.解決方法 === 'skip') continue; // 振込み

    // 税率別
    const taxKey = row.税率 === 8 ? '8' : '10'; // 未登録は10%仮扱い
    addRow(byTax[taxKey], row);

    // セグメント別
    const segKey = row.売上分類 ? String(row.売上分類) : 'other';
    if (bySegment[segKey]) addRow(bySegment[segKey], row);
    else addRow(bySegment['other'], row);
  }

  return { byTax, bySegment, columns };
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
  const { resolved, unresolved } = resolveSkus(parsedRows, db);

  // 集計
  const { byTax, bySegment, columns } = aggregate(resolved);

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
    columns,
    segmentNames: SEGMENT_NAMES,
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
    .meta{font-size:12px;color:#888;margin-top:6px}
    #result{display:none}
    .num{font-family:monospace}
    .negative{color:#e74c3c}
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
        <h2>セグメント別集計（管理会計用）</h2>
        <div id="segmentTable"></div>
      </div>
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
      // 合計行
      taxHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      cols.forEach(c => taxHtml += '<td class="num">' + fmt((data.byTax['10'][c] || 0) + (data.byTax['8'][c] || 0)) + '</td>');
      taxHtml += '</tr></table>';
      document.getElementById('taxTable').innerHTML = taxHtml;

      // セグメント別
      let segHtml = '<table><tr><th>セグメント</th>';
      cols.forEach(c => segHtml += '<th>' + c + '</th>');
      segHtml += '<th>原価合計</th></tr>';
      let totalRow = {};
      cols.forEach(c => totalRow[c] = 0);
      totalRow.原価合計 = 0;
      for (const [key, row] of Object.entries(data.bySegment)) {
        const label = data.segmentNames[key] || (key === 'other' ? 'その他/未分類' : key);
        segHtml += '<tr><td>' + key + ': ' + label + '</td>';
        cols.forEach(c => { segHtml += '<td class="num">' + fmt(row[c]) + '</td>'; totalRow[c] += row[c]; });
        segHtml += '<td class="num">' + fmt(row.原価合計) + '</td>';
        totalRow.原価合計 += row.原価合計;
        segHtml += '</tr>';
      }
      segHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      cols.forEach(c => segHtml += '<td class="num">' + fmt(totalRow[c]) + '</td>');
      segHtml += '<td class="num">' + fmt(totalRow.原価合計) + '</td></tr></table>';
      document.getElementById('segmentTable').innerHTML = segHtml;
    }
  </script>
</body>
</html>`;
}

export default router;
