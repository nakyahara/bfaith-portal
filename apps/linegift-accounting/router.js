/**
 * LINEギフト売上集計ツール
 *
 * LINEギフトの注文CSVをアップロードし、
 * mirror_products を使って税率別・セグメント別の売上集計を自動計算する。
 * PF手数料は手入力（LINEギフト振込通知PDFから目視確認）。
 *
 * 工程1: 注文データCSVアップロード（Shift_JIS、複数対応）
 * 工程2: 未登録商品の税率・セグメント登録
 * 工程3: 税率別・セグメント別集計
 * 工程4: PF手数料入力（手入力）
 * 工程5: 確定
 */
import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import iconv from 'iconv-lite';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();
const UPLOAD_DIR = process.env.DATA_DIR ? process.env.DATA_DIR + '/import' : 'data/import';
if (!fs.existsSync(UPLOAD_DIR)) { try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch {} }
const upload = multer({ dest: UPLOAD_DIR });

const SEGMENT_NAMES = { 1: '自社商品', 2: '取扱限定', 3: '仕入れ商品' };
const EXCLUDED_SEGMENTS = { 4: '輸出' };

// ─── CSV解析（Shift_JIS対応）───

function parseShiftJisCsv(buf) {
  let text;
  if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    text = buf.toString('utf-8').slice(1);
  } else {
    text = iconv.decode(buf, 'Shift_JIS');
  }
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  return lines.map(line => {
    const result = [];
    let current = '', inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') { inQuotes = !inQuotes; }
      else if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
      else { current += ch; }
    }
    result.push(current.trim());
    return result;
  });
}

// ─── 商品コード解決 ───

function resolveProducts(rows, db) {
  const productsMap = new Map();
  const repCodeMap = new Map();
  for (const p of db.prepare('SELECT * FROM mirror_products').all()) {
    productsMap.set((p.商品コード || '').toLowerCase(), p);
    const rep = (p.代表商品コード || '').toLowerCase();
    if (rep && rep !== (p.商品コード || '').toLowerCase() && !repCodeMap.has(rep)) {
      repCodeMap.set(rep, p);
    }
  }

  const resolved = [];
  const unresolved = new Map();
  const unresolvedTax = new Map();
  const unresolvedSegment = new Map();
  const resolvedByRepCode = new Map();

  for (const row of rows) {
    const varCode = (row.バリエーションコード || '').toLowerCase();
    const itemCode = (row.商品コード || '').toLowerCase();
    // LINEギフトCSV: バリエーションコード > 商品コードの優先順
    const primaryCode = varCode || itemCode;

    if (!primaryCode) {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 解決方法: 'no_code' });
      continue;
    }

    // Stage 1: バリエーションコードで検索
    let product = null;
    let resolveMethod = null;
    if (varCode) {
      product = productsMap.get(varCode);
      if (product) resolveMethod = 'variation';
    }
    // Stage 2: 商品コードで検索
    if (!product && itemCode) {
      product = productsMap.get(itemCode);
      if (product) resolveMethod = 'direct';
    }
    // Stage 3: 代表商品コードとして検索
    if (!product) {
      product = repCodeMap.get(primaryCode);
      if (product) resolveMethod = 'rep_code';
    }

    if (product) {
      const taxRate = product.消費税率 ? Math.round(product.消費税率 * 100) : null;
      resolved.push({
        ...row,
        商品コード_resolved: product.商品コード,
        原価: product.原価 || 0,
        税率: taxRate,
        売上分類: product.売上分類,
        解決方法: resolveMethod,
      });

      if (resolveMethod === 'rep_code') {
        const key = primaryCode;
        const existing = resolvedByRepCode.get(key) || {
          lineCode: row.バリエーションコード || row.商品コード || '',
          matchedCode: product.商品コード,
          matchedName: product.商品名 || '', name: row.商品名 || '',
          count: 0, amount: 0,
        };
        existing.count++;
        existing.amount += row.売上合計 || 0;
        resolvedByRepCode.set(key, existing);
      }

      if (taxRate === null) {
        const key = product.商品コード.toLowerCase();
        const existing = unresolvedTax.get(key) || {
          code: product.商品コード, name: row.商品名 || '',
          csvTaxRate: null, count: 0, amount: 0,
        };
        existing.count++;
        existing.amount += row.売上合計 || 0;
        unresolvedTax.set(key, existing);
      }

      if (!product.売上分類) {
        const key = product.商品コード.toLowerCase();
        const existing = unresolvedSegment.get(key) || {
          code: product.商品コード, name: row.商品名 || '',
          genka: product.原価 || 0, count: 0, amount: 0,
        };
        existing.count++;
        existing.amount += row.売上合計 || 0;
        unresolvedSegment.set(key, existing);
      }
    } else {
      resolved.push({
        ...row,
        商品コード_resolved: null,
        原価: 0, 税率: null, 売上分類: null,
        解決方法: 'unresolved',
      });
      const existing = unresolved.get(primaryCode) || {
        code: primaryCode, name: row.商品名 || '',
        csvTaxRate: null, count: 0, amount: 0,
      };
      existing.count++;
      existing.amount += row.売上合計 || 0;
      unresolved.set(primaryCode, existing);
    }
  }

  const zeroGenka = new Map();
  for (const row of resolved) {
    if (row.解決方法 === 'no_code' || row.解決方法 === 'unresolved') continue;
    if (row.商品コード_resolved && (row.原価 === 0 || row.原価 === null)) {
      const key = row.商品コード_resolved;
      const existing = zeroGenka.get(key) || {
        商品コード: key, 商品名: row.商品名 || '', 数量合計: 0, 売上合計: 0, count: 0,
      };
      existing.数量合計 += row.個数 || 0;
      existing.売上合計 += row.売上合計 || 0;
      existing.count++;
      zeroGenka.set(key, existing);
    }
  }

  return {
    resolved,
    unresolved: [...unresolved.values()],
    unresolvedTax: [...unresolvedTax.values()],
    unresolvedSegment: [...unresolvedSegment.values()],
    zeroGenka: [...zeroGenka.values()],
    resolvedByRepCode: [...resolvedByRepCode.values()],
  };
}

// ─── 集計 ───

function aggregate(resolvedRows) {
  function emptyRow() {
    return { 売上合計: 0, クーポン値引額: 0, クーポン値引後売上: 0, 原価合計: 0, 行数: 0 };
  }

  const byTax = { '10': emptyRow(), '8': emptyRow() };
  const bySegment = { '1': emptyRow(), '2': emptyRow(), '3': emptyRow(), 'other': emptyRow() };
  const excluded = { '4': emptyRow() };
  const otherDetails = new Map();

  for (const row of resolvedRows) {
    if (row.解決方法 === 'no_code') continue;

    const sale = row.売上合計 || 0;
    const coupon = 0; // LINEギフトCSVにクーポン値引は含まれない
    const afterCoupon = sale - coupon;
    const genka = (row.原価 || 0) * (row.個数 || 1);

    // 税率別: マスター税率 > 10%仮扱い
    const taxRate = row.税率 || 10;
    const taxKey = taxRate === 8 ? '8' : '10';
    byTax[taxKey].売上合計 += sale;
    byTax[taxKey].クーポン値引額 += coupon;
    byTax[taxKey].クーポン値引後売上 += afterCoupon;
    byTax[taxKey].原価合計 += genka;
    byTax[taxKey].行数++;

    // セグメント別
    const segKey = row.売上分類 ? String(row.売上分類) : 'other';

    if (excluded[segKey]) {
      excluded[segKey].売上合計 += sale;
      excluded[segKey].クーポン値引額 += coupon;
      excluded[segKey].クーポン値引後売上 += afterCoupon;
      excluded[segKey].原価合計 += genka;
      excluded[segKey].行数++;
    } else {
      const target = bySegment[segKey] || bySegment['other'];
      target.売上合計 += sale;
      target.クーポン値引額 += coupon;
      target.クーポン値引後売上 += afterCoupon;
      target.原価合計 += genka;
      target.行数++;
    }

    if (!row.売上分類 && !excluded[segKey]) {
      const detailKey = row.商品コード_resolved || row.商品コード || '_no_code_';
      const existing = otherDetails.get(detailKey) || {
        商品コード: row.商品コード || '', 商品名: row.商品名 || '',
        解決方法: row.解決方法, 売上合計: 0, 個数: 0, count: 0,
      };
      existing.売上合計 += sale;
      existing.個数 += row.個数 || 0;
      existing.count++;
      otherDetails.set(detailKey, existing);
    }
  }

  // MF連携用: 税込み集計
  const t10 = byTax['10'];
  const t8 = byTax['8'];
  const mfRow = {
    '商品売上(10%)': Math.round(t10.クーポン値引後売上 * 1.1),
    '商品売上(8%)': Math.round(t8.クーポン値引後売上 * 1.08),
  };
  mfRow['合計'] = mfRow['商品売上(10%)'] + mfRow['商品売上(8%)'];

  // 原価率計算
  for (const seg of [...Object.values(bySegment), ...Object.values(excluded)]) {
    if (seg.クーポン値引後売上 > 0) {
      seg.原価率 = (seg.原価合計 / seg.クーポン値引後売上 * 100).toFixed(1);
    } else {
      seg.原価率 = '0.0';
    }
  }

  return {
    byTax, bySegment, excluded,
    otherDetails: [...otherDetails.values()].sort((a, b) => Math.abs(b.売上合計) - Math.abs(a.売上合計)),
    mfRow,
  };
}

// ─── GET / — メイン画面 ───

router.get('/', (req, res) => {
  res.send(renderPage());
});

// ─── POST /upload — 注文データCSVアップロード＆集計 ───

router.post('/upload', upload.array('files', 10), (req, res) => {
  if (!req.files || req.files.length === 0) return res.status(400).json({ error: 'ファイルが必要です' });

  let db;
  try {
    db = getMirrorDB();
  } catch (e) {
    return res.status(500).json({ error: 'ミラーDB未初期化: ' + e.message });
  }

  try {
    const allRows = [];
    const fileNames = [];

    for (const file of req.files) {
      const buf = fs.readFileSync(file.path);
      fs.unlinkSync(file.path);
      fileNames.push(file.originalname);

      const csvRows = parseShiftJisCsv(buf);
      if (csvRows.length < 2) continue;

      // ヘッダー確認（LINEギフト形式: 先頭列=ショップID）
      const header = csvRows[0];
      if (header[0] !== 'ショップID') {
        return res.status(400).json({ error: file.originalname + ' はLINEギフト形式ではありません（先頭列: ' + header[0] + '）' });
      }

      const num = v => { const n = parseFloat((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };

      // ヘッダーからインデックスを構築
      const colIdx = {};
      header.forEach((h, i) => { colIdx[h] = i; });

      for (let i = 1; i < csvRows.length; i++) {
        const cols = csvRows[i];
        if (cols.length < 10) continue;

        const col = name => cols[colIdx[name]] || '';

        const orderId = col('注文ID');
        if (!orderId) continue;

        // 注文ステータスが「取引完了」または「決済済み」以外はスキップ（キャンセル等除外）
        const status = col('注文ステータス');
        if (status !== '取引完了' && status !== '決済済み') continue;

        const itemCode = col('商品コード');
        const varCode = col('バリエーションコード');
        const quantity = parseInt(col('数量')) || 1;
        const unitPrice = num(col('商品単価'));
        const totalPrice = num(col('商品合計金額')) || unitPrice * quantity;
        const title = col('商品名');
        const deliveryDate = col('発送可能日時');
        const orderDate = '';

        allRows.push({
          注文番号: orderId,
          商品コード: itemCode,
          バリエーションコード: varCode,
          商品名: title,
          単価: unitPrice,
          個数: quantity,
          売上合計: totalPrice,
          クーポン値引額: 0,
          発送日: deliveryDate,
          注文日時: orderDate,
        });
      }
    }

    if (allRows.length === 0) {
      return res.status(400).json({ error: 'CSVからデータを読み取れませんでした' });
    }

    // 対象年月を推定（発送可能日 or 注文日時から）
    let yearMonth = '';
    for (const row of allRows) {
      const dateStr = row.発送日 || row.注文日時 || '';
      if (dateStr) {
        const d = dateStr.replace(/\//g, '-');
        yearMonth = d.slice(0, 7);
        break;
      }
    }

    // 商品コード解決
    const { resolved, unresolved, unresolvedTax, unresolvedSegment, zeroGenka, resolvedByRepCode } = resolveProducts(allRows, db);

    // 集計
    const agg = aggregate(resolved);

    // 明細CSVダウンロード用: 各行に税率・売上分類・原価を付与
    const detailRows = resolved.map(r => ({
      注文番号: r.注文番号,
      商品コード: r.商品コード,
      バリエーションコード: r.バリエーションコード,
      商品名: r.商品名,
      単価: r.単価,
      個数: r.個数,
      売上合計: r.売上合計,
      解決コード: r.商品コード_resolved || '',
      税率: r.税率 || '',
      売上分類: r.売上分類 || '',
      原価単価: r.原価 || 0,
      原価合計: (r.原価 || 0) * (r.個数 || 1),
      解決方法: r.解決方法,
    }));

    res.json({
      yearMonth,
      totalRows: allRows.length,
      fileCount: req.files.length,
      fileNames,
      resolvedCount: resolved.filter(r => r.解決方法 !== 'unresolved' && r.解決方法 !== 'no_code').length,
      unresolvedProducts: unresolved,
      unresolvedTax,
      unresolvedSegment,
      segmentNames: SEGMENT_NAMES,
      excludedNames: EXCLUDED_SEGMENTS,
      zeroGenka,
      resolvedByRepCode,
      byTax: agg.byTax,
      bySegment: agg.bySegment,
      excluded: agg.excluded,
      otherDetails: agg.otherDetails,
      mfRow: agg.mfRow,
      detailRows,
    });
  } catch (e) {
    console.error('[LinegiftAccounting] エラー:', e.message, e.stack);
    res.status(500).json({ error: '集計処理エラー: ' + e.message });
  }
});

// ─── POST /register — 税率・セグメント登録 ───

router.post('/register', (req, res) => {
  let db;
  try { db = getMirrorDB(); } catch (e) {
    return res.status(500).json({ error: 'DB未初期化: ' + e.message });
  }

  const { items } = req.body || {};
  if (!items || !Array.isArray(items)) return res.status(400).json({ error: 'items配列が必要です' });

  let updatedTax = 0, updatedSeg = 0;

  for (const item of items) {
    if (item.taxRate != null) {
      const r = db.prepare('UPDATE mirror_products SET 消費税率 = ? WHERE lower(商品コード) = lower(?)').run(item.taxRate / 100, item.code);
      if (r.changes > 0) updatedTax++;
    }
    if (item.segment != null) {
      const r = db.prepare('UPDATE mirror_products SET 売上分類 = ? WHERE lower(商品コード) = lower(?)').run(item.segment, item.code);
      if (r.changes > 0) updatedSeg++;
    }
  }

  res.json({ ok: true, updatedTax, updatedSeg });
});

// ─── POST /confirm — 集計確定（DB保存）───

router.post('/confirm', (req, res) => {
  const db = getMirrorDB();
  const { yearMonth, totalRows, resolvedCount, unresolvedCount,
    byTax, bySegment, excluded, mfRow, pfFee, adCost } = req.body;

  if (!yearMonth) return res.status(400).json({ error: 'yearMonth は必須です' });

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    db.prepare(`INSERT OR REPLACE INTO mart_linegift_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, pf_fee, ad_cost, confirmed_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      yearMonth, totalRows || 0, resolvedCount || 0, unresolvedCount || 0,
      JSON.stringify(byTax), JSON.stringify(bySegment), JSON.stringify(excluded),
      JSON.stringify(mfRow), pfFee || 0, adCost || 0, now
    );

    db.prepare(`INSERT INTO mart_linegift_upload_log
      (year_month, total_rows, resolved_count, unresolved_count, uploaded_at)
      VALUES (?,?,?,?,?)
    `).run(yearMonth, totalRows || 0, resolvedCount || 0, unresolvedCount || 0, now);

    res.json({ ok: true, yearMonth, confirmed_at: now });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── GET /history — 過去月一覧 ───

router.get('/history', (req, res) => {
  const db = getMirrorDB();
  try {
    const rows = db.prepare('SELECT * FROM mart_linegift_monthly_summary ORDER BY year_month DESC').all();
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

// ─── HTML ───

function renderPage() {
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LINEギフト売上集計 - B-Faith</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#333;font-size:14px}
    .header{background:#06C755;color:white;padding:12px 24px;display:flex;align-items:center;gap:16px}
    .header h1{font-size:18px}
    .header a{color:#c8f7d8;text-decoration:none;font-size:13px}
    .wrap{max-width:1800px;margin:16px auto;padding:0 16px}
    .card{background:white;border-radius:8px;padding:20px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1);overflow-x:auto}
    .card h2{font-size:15px;color:#555;margin-bottom:10px}
    .btn{padding:8px 20px;border:none;border-radius:4px;cursor:pointer;font-size:14px}
    .btn-p{background:#06C755;color:white}.btn-p:hover{background:#05a648}
    .btn-s{background:#27ae60;color:white}.btn-s:hover{background:#1e8449}
    .btn-w{background:#e67e22;color:white}.btn-w:hover{background:#d35400}
    .btn:disabled{opacity:.5;cursor:default}
    table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px;white-space:nowrap}
    th{background:#f0f0f0;padding:6px 8px;text-align:right;font-size:12px}
    th:first-child{text-align:left}
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
    .tab-bar{display:flex;gap:4px;margin-bottom:12px;border-bottom:2px solid #ddd;padding-bottom:0}
    .tab-btn{padding:8px 16px;border:none;background:#eee;cursor:pointer;border-radius:4px 4px 0 0;font-size:13px}
    .tab-btn.active{background:#06C755;color:white}
    .tab-content{display:none}.tab-content.active{display:block}
    .acc-header{cursor:pointer;padding:10px 12px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:4px;margin-bottom:2px;display:flex;justify-content:space-between;align-items:center;font-size:13px}
    .acc-header:hover{background:#e9ecef}
    .acc-header .arrow{transition:transform .2s;font-size:10px}
    .acc-header.open .arrow{transform:rotate(90deg)}
    .acc-body{display:none;padding:12px;border:1px solid #eee;border-top:none;margin-bottom:8px;background:#fff}
    .acc-body.open{display:block}
    .detail-table td{font-size:12px;font-weight:normal}
    .detail-table th{font-size:11px}
    select.reg-sel{padding:2px 4px;font-size:12px;border:1px solid #ccc;border-radius:3px}
    .pf-input{padding:6px 10px;font-size:14px;border:1px solid #ccc;border-radius:4px;width:160px;text-align:right}
  </style>
</head>
<body>
  <div class="header">
    <h1>LINEギフト売上集計</h1>
    <a href="/">\\u2190 \\u30dd\\u30fc\\u30bf\\u30eb\\u306b\\u623b\\u308b</a>
  </div>
  <div class="wrap">
    <!-- 工程1: 注文データCSV -->
    <div class="card">
      <h2>工程1: 注文データCSVアップロード</h2>
      <p class="meta">LINEギフト管理画面 → 注文管理 → CSVダウンロード（注文ステータス: 決済済み）</p>
      <p class="meta" style="color:#06C755;font-weight:bold">複数ファイル選択可（最大10ファイル）</p>
      <div style="margin-top:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <input type="file" id="csvFiles" accept=".csv" multiple>
        <button class="btn btn-p" id="uploadBtn" onclick="doUpload()">アップロード＆集計</button>
        <span id="fileCount" class="meta"></span>
      </div>
      <div id="uploadStatus" class="meta" style="margin-top:8px"></div>
    </div>

    <div id="result">
      <!-- 集計概要 -->
      <div class="card">
        <h2>集計概要</h2>
        <div id="summary"></div>
      </div>

      <!-- 未登録情報 -->
      <div id="unresolvedArea" class="card" style="display:none">
        <h2>未登録情報</h2>
        <div class="tab-bar" id="unresolvedTabs"></div>
        <div id="unresolvedContent"></div>
        <div style="margin-top:12px">
          <button class="btn btn-w" id="registerBtn" onclick="doRegister()" style="display:none">選択した税率・セグメントを登録して再集計</button>
        </div>
      </div>

      <!-- 税率別集計 -->
      <div class="card">
        <h2>税率別売上集計（税抜）</h2>
        <div id="taxTable"></div>
      </div>

      <!-- MF連携用 -->
      <div class="card">
        <h2>MF連携用 税込み集計</h2>
        <div id="mfTable"></div>
      </div>

      <!-- PF手数料（セグメント別の上に配置） -->
      <div class="card">
        <h2>工程2: PF手数料</h2>
        <p class="meta">LINEギフト管理画面の振込通知から手数料を確認して入力してください。</p>
        <p class="meta"><a href="https://gift-shop-cms.line.biz/shops/838894/transfers" target="_blank" style="color:#06C755">LINEギフト振込一覧を開く</a></p>
        <div style="margin-top:12px;display:flex;gap:16px;align-items:center;flex-wrap:wrap">
          <label>PF手数料（税込）: <input type="text" id="pfFeeInput" class="pf-input" value="0"></label>
          <button class="btn btn-p" onclick="applyPfFee()">反映</button>
        </div>
        <div id="costSummary" style="margin-top:8px"></div>
      </div>

      <!-- セグメント別集計（PF手数料の下） -->
      <div class="card">
        <h2>セグメント別集計（管理会計用）</h2>
        <div id="segmentTable"></div>
        <div id="excludedInfo"></div>
      </div>

      <!-- CSVダウンロード -->
      <div class="card">
        <h2>CSVダウンロード</h2>
        <div style="display:flex;gap:12px;flex-wrap:wrap">
          <button class="btn btn-s" onclick="downloadSummaryCsv()">集計サマリーCSV</button>
          <button class="btn btn-p" onclick="downloadDetailCsv()">明細CSV（税率・売上分類・原価付き）</button>
        </div>
      </div>

      <!-- 代表商品コードで紐付けた商品 -->
      <div id="repCodeCard" class="card" style="display:none">
        <h2>代表商品コードで紐付けた商品</h2>
        <p class="meta">LINEギフト側の商品コードがマスタに直接存在せず、代表商品コード経由で紐付けました。</p>
        <div id="repCodeList"></div>
      </div>

      <!-- 原価ゼロ警告 -->
      <div id="zeroGenkaCard" class="card" style="display:none">
        <h2>原価ゼロで計算された商品</h2>
        <p class="meta">商品マスタの原価が0またはNULLのため、原価0円で集計されています。</p>
        <div id="zeroGenkaList"></div>
      </div>
    </div>

    <!-- 確定 -->
    <div class="card" id="confirmCard">
      <h2>確定</h2>
      <div id="confirmPreCheck" class="warn" style="margin-bottom:8px">注文データCSVをアップロードしてから確定してください</div>
      <div style="display:flex;gap:12px;align-items:center;margin-bottom:8px">
        <button class="btn btn-s" id="confirmBtn" onclick="doConfirm()" disabled>この月の集計を確定</button>
      </div>
      <div id="confirmStatus" class="meta"></div>
    </div>

    <!-- 過去の確定データ -->
    <div class="card">
      <h2>過去の確定データ</h2>
      <div style="margin-bottom:8px"><button class="btn btn-p" onclick="downloadHistoryCsv()">セグメント別集計CSVダウンロード</button></div>
      <div id="historyList"><span class="meta">読み込み中...</span></div>
    </div>
  </div>

  <script>
    let lastData = null;
    let pfFeeData = { pfFee: 0, adCost: 0 };
    const fmt = n => {
      if (n === 0) return '0';
      const s = Math.round(n).toLocaleString();
      return n < 0 ? '<span class="negative">' + s + '</span>' : s;
    };

    async function fetchWithRetry(url, options, maxRetries = 2) {
      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          const r = await fetch(url, options);
          if (r.redirected && r.url.includes('/login')) {
            throw new Error('セッションが切れました。ページを再読み込みしてログインし直してください。');
          }
          if ((r.status === 502 || r.status === 503) && attempt < maxRetries) {
            await new Promise(ok => setTimeout(ok, 3000));
            continue;
          }
          return r;
        } catch(e) {
          if (e.message.includes('セッション')) throw e;
          if (attempt < maxRetries) {
            await new Promise(ok => setTimeout(ok, 3000));
            continue;
          }
          throw new Error('サーバーに接続できません。ページを再読み込みしてください。');
        }
      }
    }

    async function doUpload() {
      const fileInput = document.getElementById('csvFiles');
      if (!fileInput.files.length) { alert('ファイルを選択してください'); return; }
      const btn = document.getElementById('uploadBtn');
      btn.disabled = true;
      btn.textContent = '処理中...';
      document.getElementById('uploadStatus').textContent = 'アップロード中...';

      const formData = new FormData();
      for (const f of fileInput.files) formData.append('files', f);

      try {
        const r = await fetchWithRetry(location.pathname + '/upload', { method: 'POST', body: formData });
        if (!r.ok) {
          const text = await r.text();
          try { const j = JSON.parse(text); document.getElementById('uploadStatus').innerHTML = '<span class="negative">エラー: ' + (j.error || r.status) + '</span>'; }
          catch { document.getElementById('uploadStatus').innerHTML = '<span class="negative">サーバーエラー (HTTP ' + r.status + ')</span>'; }
          return;
        }
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

      const hasUnresolved = data.unresolvedProducts.length > 0;
      const hasUnresTax = data.unresolvedTax && data.unresolvedTax.length > 0;
      const hasUnresSeg = data.unresolvedSegment && data.unresolvedSegment.length > 0;

      let summaryHtml = '<div class="' + (!hasUnresolved ? 'ok' : 'warn') + '">';
      summaryHtml += '<b>対象年月: ' + (data.yearMonth || '不明') + '</b> （' + data.fileCount + 'ファイル）<br>';
      summaryHtml += '総行数: ' + data.totalRows + ' / 解決済: ' + data.resolvedCount + ' / 未登録商品: ' + data.unresolvedProducts.length + '件';
      if (hasUnresTax) summaryHtml += ' / <span class="negative">税率未登録: ' + data.unresolvedTax.length + '件</span>';
      if (hasUnresSeg) summaryHtml += ' / <span class="negative">セグメント未登録: ' + data.unresolvedSegment.length + '件</span>';
      if (!hasUnresolved) summaryHtml += '<br><b style="color:#27ae60">全商品解決済み</b>';
      else summaryHtml += '<br><b style="color:#e74c3c">未登録商品あり — warehouse側で登録後に再アップロード</b>';
      summaryHtml += '</div>';
      document.getElementById('summary').innerHTML = summaryHtml;

      // 未登録タブ
      const tabs = [];
      if (hasUnresolved) tabs.push({ id: 'unres-product', label: '未登録商品 (' + data.unresolvedProducts.length + ')' });
      if (hasUnresTax) tabs.push({ id: 'unres-tax', label: '税率未登録 (' + data.unresolvedTax.length + ')' });
      if (hasUnresSeg) tabs.push({ id: 'unres-seg', label: 'セグメント未登録 (' + data.unresolvedSegment.length + ')' });

      if (tabs.length > 0) {
        document.getElementById('unresolvedArea').style.display = 'block';
        let tabHtml = '';
        tabs.forEach((t, i) => tabHtml += '<button class="tab-btn' + (i===0?' active':'') + '" onclick="switchTab(this,\\'' + t.id + '\\')">' + t.label + '</button>');
        document.getElementById('unresolvedTabs').innerHTML = tabHtml;

        let contentHtml = '';
        let showRegisterBtn = false;

        if (hasUnresolved) {
          contentHtml += '<div id="unres-product" class="tab-content active">';
          contentHtml += '<div class="warn">商品マスタに未登録の商品です。warehouse側で登録してから再アップロードしてください。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>出現数</th><th>売上合計</th></tr>';
          for (const u of data.unresolvedProducts) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
          }
          contentHtml += '</table></div>';
        }

        if (hasUnresTax) {
          showRegisterBtn = true;
          contentHtml += '<div id="unres-tax" class="tab-content' + (!hasUnresolved?' active':'') + '">';
          contentHtml += '<div class="warn">税率未登録の商品です。プルダウンで税率を選択し「登録して再集計」できます。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>出現数</th><th>売上合計</th><th>税率登録</th></tr>';
          for (const u of data.unresolvedTax) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td>';
            contentHtml += '<td><select class="reg-sel tax-reg" data-code="' + u.code + '"><option value="">-</option><option value="10">10%</option><option value="8">8%</option></select></td></tr>';
          }
          contentHtml += '</table></div>';
        }

        if (hasUnresSeg) {
          showRegisterBtn = true;
          contentHtml += '<div id="unres-seg" class="tab-content' + (!hasUnresolved && !hasUnresTax?' active':'') + '">';
          contentHtml += '<div class="warn">セグメント未登録の商品です。プルダウンでセグメントを選択し「登録して再集計」できます。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>原価</th><th>出現数</th><th>売上合計</th><th>セグメント登録</th></tr>';
          for (const u of data.unresolvedSegment) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + fmt(u.genka) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td>';
            contentHtml += '<td><select class="reg-sel seg-reg" data-code="' + u.code + '"><option value="">-</option><option value="1">1:自社商品</option><option value="2">2:取扱限定</option><option value="3">3:仕入れ商品</option><option value="4">4:輸出</option></select></td></tr>';
          }
          contentHtml += '</table></div>';
        }

        document.getElementById('unresolvedContent').innerHTML = contentHtml;
        document.getElementById('registerBtn').style.display = showRegisterBtn ? 'inline-block' : 'none';
      } else {
        document.getElementById('unresolvedArea').style.display = 'none';
      }

      renderTaxTable(data);
      renderMfTable(data);
      renderSegmentTable(data);

      if (data.resolvedByRepCode && data.resolvedByRepCode.length > 0) {
        document.getElementById('repCodeCard').style.display = 'block';
        let html = '<div class="warn" style="margin-bottom:8px"><b>' + data.resolvedByRepCode.length + '商品</b>を代表商品コード経由で紐付けました</div>';
        html += '<table class="detail-table"><tr><th>LINEギフト商品コード</th><th>商品名</th><th>紐付先マスタコード</th><th>マスタ商品名</th><th>出現数</th><th>売上合計</th></tr>';
        for (const r of data.resolvedByRepCode) {
          html += '<tr><td style="text-align:left">' + r.lineCode + '</td><td style="text-align:left">' + (r.name || '').slice(0, 40) + '</td><td style="text-align:left;color:#06C755;font-weight:bold">' + r.matchedCode + '</td><td style="text-align:left">' + (r.matchedName || '').slice(0, 40) + '</td><td class="num">' + r.count + '</td><td class="num">' + fmt(r.amount) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('repCodeList').innerHTML = html;
      } else { document.getElementById('repCodeCard').style.display = 'none'; }

      if (data.zeroGenka && data.zeroGenka.length > 0) {
        document.getElementById('zeroGenkaCard').style.display = 'block';
        let html = '<div class="warn" style="margin-bottom:8px"><b>' + data.zeroGenka.length + '商品</b>が原価0円で計算されています</div>';
        html += '<table class="detail-table"><tr><th>商品コード</th><th>商品名</th><th>出現行数</th><th>数量合計</th><th>売上合計</th></tr>';
        for (const z of data.zeroGenka) {
          html += '<tr><td style="text-align:left">' + z.商品コード + '</td><td style="text-align:left">' + (z.商品名 || '').slice(0, 50) + '</td><td class="num">' + z.count + '</td><td class="num">' + z.数量合計 + '</td><td class="num">' + fmt(z.売上合計) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('zeroGenkaList').innerHTML = html;
      } else { document.getElementById('zeroGenkaCard').style.display = 'none'; }

      updateConfirmState();
    }

    function switchTab(btn, tabId) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      document.getElementById(tabId).classList.add('active');
    }

    function renderTaxTable(data) {
      const bt = data.byTax;
      let html = '<table><tr><th>税率</th><th>売上合計</th><th>原価合計</th><th>行数</th></tr>';
      let tot = { s:0, g:0, n:0 };
      for (const [key, row] of Object.entries(bt)) {
        html += '<tr><td>' + key + '%</td><td class="num">' + fmt(row.売上合計) + '</td><td class="num">' + fmt(row.原価合計) + '</td><td class="num">' + row.行数 + '</td></tr>';
        tot.s += row.売上合計; tot.g += row.原価合計; tot.n += row.行数;
      }
      html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td><td class="num">' + fmt(tot.s) + '</td><td class="num">' + fmt(tot.g) + '</td><td class="num">' + tot.n + '</td></tr></table>';
      document.getElementById('taxTable').innerHTML = html;
    }

    function renderMfTable(data) {
      const mf = data.mfRow;
      let html = '<table><tr><th>商品売上(10%税込)</th><th>商品売上(8%税込)</th><th>合計</th></tr>';
      html += '<tr><td class="num" style="font-weight:bold">' + fmt(mf['商品売上(10%)']) + '</td>';
      html += '<td class="num" style="font-weight:bold">' + fmt(mf['商品売上(8%)']) + '</td>';
      html += '<td class="num" style="font-weight:bold">' + fmt(mf['合計']) + '</td></tr></table>';
      document.getElementById('mfTable').innerHTML = html;
    }

    function allocateByRatio(amount, salesByKey, targets) {
      let totalSales = 0;
      for (const k of targets) totalSales += (salesByKey[k] || 0);
      const result = {};
      let sum = 0;
      for (const k of Object.keys(salesByKey)) {
        if (!targets.includes(k) || totalSales === 0) { result[k] = 0; continue; }
        result[k] = Math.round(amount * salesByKey[k] / totalSales);
        sum += result[k];
      }
      if (amount && totalSales > 0) {
        const maxKey = targets.sort((a, b) => (salesByKey[b]||0) - (salesByKey[a]||0))[0];
        if (maxKey) result[maxKey] += (amount - sum);
      }
      return result;
    }

    function renderSegmentTable(data) {
      const seg = data.bySegment;
      const segNames = { 1: '自社商品', 2: '取扱限定', 3: '仕入れ商品' };
      const pf = pfFeeData.pfFee || 0;

      const allocTargets = ['1', '2', '3'];
      const salesByKey = {};
      for (const [key, row] of Object.entries(seg)) { salesByKey[key] = row.クーポン値引後売上 || 0; }
      const pfByKey = allocateByRatio(pf, salesByKey, allocTargets);

      let html = '<table><tr><th>セグメント</th><th>売上合計</th><th>PF手数料</th><th>原価合計</th><th>原価率</th><th>行数</th></tr>';
      let tot = { s:0, g:0, n:0 };
      let totPf = 0;
      for (const [key, row] of Object.entries(seg)) {
        const label = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
        html += '<tr><td>' + key + ': ' + label + '</td>';
        html += '<td class="num">' + fmt(row.売上合計) + '</td>';
        html += '<td class="num">' + fmt(pfByKey[key] || 0) + '</td>';
        html += '<td class="num">' + fmt(row.原価合計) + '</td>';
        html += '<td class="num">' + (row.原価率 || '0.0') + '%</td>';
        html += '<td class="num">' + row.行数 + '</td></tr>';
        tot.s += row.売上合計; tot.g += row.原価合計; tot.n += row.行数;
        totPf += (pfByKey[key] || 0);
      }
      const totGross = tot.s > 0 ? (tot.g / tot.s * 100).toFixed(1) : '0.0';
      html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      html += '<td class="num">' + fmt(tot.s) + '</td><td class="num">' + fmt(totPf) + '</td>';
      html += '<td class="num">' + fmt(tot.g) + '</td><td class="num">' + totGross + '%</td>';
      html += '<td class="num">' + tot.n + '</td></tr></table>';
      document.getElementById('segmentTable').innerHTML = html;

      const excl = data.excluded || {};
      let exclHtml = '';
      for (const [key, row] of Object.entries(excl)) {
        if (row.行数 > 0) exclHtml += '<div class="excluded"><b>除外: ' + key + ': 輸出</b>（' + row.行数 + '件） — 売上: ' + fmt(row.売上合計) + ' / 原価: ' + fmt(row.原価合計) + '</div>';
      }
      document.getElementById('excludedInfo').innerHTML = exclHtml;
    }

    function applyPfFee() {
      pfFeeData.pfFee = parseInt(document.getElementById('pfFeeInput').value.replace(/,/g, '')) || 0;
      pfFeeData.adCost = 0;
      if (lastData) renderSegmentTable(lastData);
      document.getElementById('costSummary').innerHTML = '<div class="ok">PF手数料 ' + fmt(pfFeeData.pfFee) + ' を反映しました</div>';
    }

    function updateConfirmState() {
      const preCheck = document.getElementById('confirmPreCheck');
      const confirmBtn = document.getElementById('confirmBtn');
      if (lastData) {
        preCheck.className = 'ok';
        preCheck.innerHTML = '<b>' + (lastData.yearMonth || '不明') + '</b> の集計データを確定できます';
        confirmBtn.disabled = false;
      }
    }

    async function doRegister() {
      const items = [];
      document.querySelectorAll('.tax-reg').forEach(sel => {
        if (sel.value) items.push({ code: sel.dataset.code, taxRate: parseInt(sel.value) });
      });
      document.querySelectorAll('.seg-reg').forEach(sel => {
        if (sel.value) items.push({ code: sel.dataset.code, segment: parseInt(sel.value) });
      });
      if (items.length === 0) { alert('登録項目を選択してください'); return; }

      try {
        const r = await fetchWithRetry(location.pathname + '/register', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ items }),
        });
        const data = await r.json();
        if (data.ok) {
          alert('登録完了（税率: ' + (data.updatedTax || 0) + '件, セグメント: ' + (data.updatedSeg || 0) + '件）\\n再アップロードして反映してください');
          // 自動再アップロード
          const fileInput = document.getElementById('csvFiles');
          if (fileInput.files.length > 0) doUpload();
        }
      } catch(e) { alert('登録エラー: ' + e.message); }
    }

    async function doConfirm() {
      if (!lastData) return;
      if (!confirm(lastData.yearMonth + ' の集計を確定しますか？')) return;
      try {
        const r = await fetchWithRetry(location.pathname + '/confirm', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            yearMonth: lastData.yearMonth,
            totalRows: lastData.totalRows,
            resolvedCount: lastData.resolvedCount,
            unresolvedCount: lastData.unresolvedProducts.length,
            byTax: lastData.byTax,
            bySegment: lastData.bySegment,
            excluded: lastData.excluded,
            mfRow: lastData.mfRow,
            pfFee: pfFeeData.pfFee,
            adCost: 0,
          }),
        });
        const data = await r.json();
        if (data.ok) {
          document.getElementById('confirmStatus').innerHTML = '<div class="ok">確定完了: ' + data.yearMonth + ' (' + data.confirmed_at + ')</div>';
          loadHistory();
        } else {
          document.getElementById('confirmStatus').innerHTML = '<span class="negative">エラー: ' + (data.error || '不明') + '</span>';
        }
      } catch(e) { document.getElementById('confirmStatus').innerHTML = '<span class="negative">エラー: ' + e.message + '</span>'; }
    }

    async function loadHistory() {
      try {
        const r = await fetchWithRetry(location.pathname + '/history');
        const rows = await r.json();
        if (!rows.length) { document.getElementById('historyList').innerHTML = '<span class="meta">確定データなし</span>'; return; }
        const segNames = { '1': '自社商品', '2': '取扱限定', '3': '仕入れ商品', 'other': 'その他' };
        let html = '';
        for (const row of rows) {
          html += '<div class="acc-header" onclick="toggleAcc(this)"><span><b>' + row.year_month + '</b> — 行数: ' + row.total_rows + ' / PF手数料: ' + fmt(row.pf_fee) + ' / 確定: ' + row.confirmed_at + '</span><span class="arrow">\\u25B6</span></div>';
          html += '<div class="acc-body"><table><tr><th>セグメント</th><th>売上合計</th><th>原価合計</th></tr>';
          const seg = row.by_segment || {};
          for (const [k, v] of Object.entries(seg)) {
            html += '<tr><td>' + k + ': ' + (segNames[k] || k) + '</td><td class="num">' + fmt(v.売上合計 || 0) + '</td><td class="num">' + fmt(v.原価合計 || 0) + '</td></tr>';
          }
          html += '</table></div>';
        }
        document.getElementById('historyList').innerHTML = html;
      } catch(e) { document.getElementById('historyList').innerHTML = '<span class="meta">履歴取得エラー</span>'; }
    }

    function toggleAcc(el) {
      el.classList.toggle('open');
      const body = el.nextElementSibling;
      body.classList.toggle('open');
    }

    function downloadSummaryCsv() {
      if (!lastData) { alert('先にCSVをアップロードしてください'); return; }
      let csv = '\\uFEFF';

      // 税率別売上
      csv += '--- 税率別売上（税抜）---\\n';
      csv += '税率,売上合計,原価合計,行数\\n';
      const bt = lastData.byTax;
      let taxTotS = 0, taxTotG = 0, taxTotN = 0;
      for (const [k, v] of Object.entries(bt)) {
        csv += k + '%,' + Math.round(v.売上合計) + ',' + Math.round(v.原価合計) + ',' + v.行数 + '\\n';
        taxTotS += v.売上合計; taxTotG += v.原価合計; taxTotN += v.行数;
      }
      csv += '合計,' + Math.round(taxTotS) + ',' + Math.round(taxTotG) + ',' + taxTotN + '\\n';
      csv += '\\n';

      // セグメント別集計
      const seg = lastData.bySegment;
      const segNames = { '1': '自社商品', '2': '取扱限定', '3': '仕入れ商品', 'other': 'その他' };
      const pf = pfFeeData.pfFee || 0;
      const salesByKey = {};
      for (const [k, v] of Object.entries(seg)) salesByKey[k] = v.クーポン値引後売上 || 0;
      const pfByKey = allocateByRatio(pf, salesByKey, ['1','2','3']);

      csv += '--- セグメント別集計 ---\\n';
      csv += 'セグメント,売上合計,原価合計,PF手数料,原価率\\n';
      for (const [k, v] of Object.entries(seg)) {
        csv += (segNames[k] || k) + ',' + Math.round(v.売上合計) + ',' + Math.round(v.原価合計) + ',' + (pfByKey[k] || 0) + ',' + (v.原価率 || '0.0') + '%\\n';
      }
      csv += '\\n';

      // MF連携用
      const mf = lastData.mfRow;
      csv += '--- MF連携用（税込）---\\n';
      csv += '商品売上(10%税込),商品売上(8%税込),合計\\n';
      csv += mf['商品売上(10%)'] + ',' + mf['商品売上(8%)'] + ',' + mf['合計'] + '\\n';

      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'linegift-summary-' + lastData.yearMonth + '.csv';
      a.click();
    }

    async function downloadHistoryCsv() {
      try {
        const r = await fetchWithRetry(location.pathname + '/history');
        const rows = await r.json();
        if (!rows.length) { alert('確定データがありません'); return; }
        const segNames = { '1': '自社商品', '2': '取扱限定', '3': '仕入れ商品', 'other': 'その他' };
        let csv = '\\uFEFF年月,セグメント,売上合計,原価合計,PF手数料,確定日時\\n';
        for (const row of rows) {
          const seg = row.by_segment || {};
          for (const [k, v] of Object.entries(seg)) {
            csv += row.year_month + ',' + (segNames[k] || k) + ',' + Math.round(v.売上合計 || 0) + ',' + Math.round(v.原価合計 || 0) + ',' + (row.pf_fee || 0) + ',' + row.confirmed_at + '\\n';
          }
        }
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'linegift-history.csv';
        a.click();
      } catch(e) { alert('エラー: ' + e.message); }
    }

    function downloadDetailCsv() {
      if (!lastData || !lastData.detailRows) { alert('先にCSVをアップロードしてください'); return; }
      const segNames = { 1: '自社商品', 2: '取扱限定', 3: '仕入れ商品', 4: '輸出' };
      let csv = '\\uFEFF注文番号,商品コード,バリエーションコード,商品名,単価,個数,売上合計,解決コード,税率,売上分類,売上分類名,原価単価,原価合計,解決方法\\n';
      for (const r of lastData.detailRows) {
        const name = (r.商品名 || '').replace(/"/g, '""');
        const segName = segNames[r.売上分類] || (r.売上分類 ? String(r.売上分類) : '');
        csv += r.注文番号 + ',' + r.商品コード + ',' + r.バリエーションコード + ',"' + name + '",' + r.単価 + ',' + r.個数 + ',' + r.売上合計 + ',' + r.解決コード + ',' + r.税率 + ',' + r.売上分類 + ',' + segName + ',' + r.原価単価 + ',' + Math.round(r.原価合計) + ',' + r.解決方法 + '\\n';
      }
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'linegift-detail-' + (lastData.yearMonth || 'unknown') + '.csv';
      a.click();
    }

    // 起動時に履歴を読み込み
    loadHistory();
  </script>
</body>
</html>`;
}

export default router;
