/**
 * 楽天売上集計ツール
 *
 * 楽天RMSからダウンロードした注文データCSV（Shift_JIS）を複数アップロードし、
 * mirror_products を使って税率別・セグメント別の売上集計を自動計算する。
 * 店舗別仕訳書CSVの取り込みにも対応。
 *
 * 工程1: CSVアップロード（複数対応、Shift_JIS→UTF-8変換）
 * 工程2-3: 未登録商品の税率・セグメント確認
 * 工程4: マスター再照合（自動）
 * 工程5: 税率別・セグメント別集計
 * 工程6: 店舗別仕訳書CSV取り込み
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

// セグメント名称マップ
const SEGMENT_NAMES = { 1: '自社商品', 2: '取扱限定', 3: '仕入れ商品' };
const EXCLUDED_SEGMENTS = { 4: '輸出' };

// ─── CSV解析（Shift_JIS対応）───

function parseShiftJisCsv(buf) {
  // BOMチェック → Shift_JISデコード
  let text;
  if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    text = buf.toString('utf-8').slice(1); // BOM除去
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

// ─── 商品番号解決 ───

function resolveProducts(rows, db) {
  // mirror_productsの商品コードマップ（小文字統一）
  const productsMap = new Map();
  for (const p of db.prepare('SELECT * FROM mirror_products').all()) {
    productsMap.set((p.商品コード || '').toLowerCase(), p);
  }

  const resolved = [];
  const unresolved = new Map(); // 商品番号 → { code, name, count, amount }
  const unresolvedTax = new Map(); // 税率未登録
  const unresolvedSegment = new Map(); // セグメント未登録

  // AL列で無意味な値（商品コードとして使えない）をスキップするセット
  const INVALID_AL = new Set(['normal-inventory', 'normal-size', 'normal', '']);

  for (const row of rows) {
    const amCode = (row.システム連携用SKU番号 || '').toLowerCase();
    const alCode = (row.SKU管理番号 || '').toLowerCase();
    const wCode = (row.商品番号 || '').toLowerCase();

    if (!amCode && INVALID_AL.has(alCode) && !wCode) {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 解決方法: 'no_code' });
      continue;
    }

    // Stage 1: AM列（システム連携用SKU番号）で検索
    let product = amCode ? productsMap.get(amCode) : null;
    let resolveMethod = amCode ? 'am_direct' : null;

    // Stage 2: AM列が空の場合、AL列（SKU管理番号）で検索（無意味な値はスキップ）
    if (!product && !amCode && !INVALID_AL.has(alCode)) {
      product = productsMap.get(alCode);
      resolveMethod = 'al_fallback';
    }

    // Stage 3: AL列も無意味な値の場合、W列（商品番号）で検索
    if (!product && !amCode && INVALID_AL.has(alCode) && wCode) {
      product = productsMap.get(wCode);
      resolveMethod = 'w_fallback';
    }

    if (product) {
      const taxRate = product.消費税率 ? Math.round(product.消費税率 * 100) : null;
      resolved.push({
        ...row,
        商品コード: product.商品コード,
        原価: product.原価 || 0,
        税率: taxRate,
        売上分類: product.売上分類,
        解決方法: resolveMethod,
      });

      // 税率未登録チェック
      if (taxRate === null) {
        const key = product.商品コード.toLowerCase();
        const existing = unresolvedTax.get(key) || { code: product.商品コード, name: row.商品名 || '', count: 0, amount: 0 };
        existing.count++;
        existing.amount += row.売上合計 || 0;
        unresolvedTax.set(key, existing);
      }

      // セグメント未登録チェック
      if (!product.売上分類) {
        const key = product.商品コード.toLowerCase();
        const existing = unresolvedSegment.get(key) || { code: product.商品コード, name: row.商品名 || '', genka: product.原価 || 0, count: 0, amount: 0 };
        existing.count++;
        existing.amount += row.売上合計 || 0;
        unresolvedSegment.set(key, existing);
      }
    } else {
      // 未登録
      resolved.push({
        ...row,
        商品コード: null,
        原価: 0,
        税率: null,
        売上分類: null,
        解決方法: 'unresolved',
      });
      const key = amCode || (INVALID_AL.has(alCode) ? wCode : alCode) || wCode;
      const existing = unresolved.get(key) || { code: key, name: row.商品名 || '', count: 0, amount: 0 };
      existing.count++;
      existing.amount += row.売上合計 || 0;
      unresolved.set(key, existing);
    }
  }

  // 原価ゼロの商品を検出
  const zeroGenka = new Map();
  for (const row of resolved) {
    if (row.解決方法 === 'no_code' || row.解決方法 === 'unresolved') continue;
    if (row.商品コード && (row.原価 === 0 || row.原価 === null)) {
      const key = row.商品コード;
      const existing = zeroGenka.get(key) || { 商品コード: key, 商品名: row.商品名 || '', 数量合計: 0, 売上合計: 0, count: 0 };
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
  };
}

// ─── 集計 ───

function aggregate(resolvedRows) {
  // 楽天の集計列
  const columns = ['売上合計', 'クーポン値引額', 'クーポン値引後売上'];

  function emptyRow() {
    return { 売上合計: 0, クーポン値引額: 0, クーポン値引後売上: 0, 原価合計: 0, 行数: 0 };
  }

  // 税率別
  const byTax = { '10': emptyRow(), '8': emptyRow() };

  // セグメント別（1〜3 + other。4=輸出は除外）
  const bySegment = { '1': emptyRow(), '2': emptyRow(), '3': emptyRow(), 'other': emptyRow() };

  // 除外セグメント
  const excluded = { '4': emptyRow() };

  // 「その他/未分類」明細
  const otherDetails = new Map();

  for (const row of resolvedRows) {
    if (row.解決方法 === 'no_code') continue;

    const sale = row.売上合計 || 0;
    const coupon = row.按分クーポン || 0;
    const afterCoupon = sale - coupon;
    const genka = (row.原価 || 0) * (row.個数 || 1);

    // 税率別
    const taxKey = row.税率 === 8 ? '8' : '10'; // 未登録は10%仮扱い
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

    // 「その他」明細
    if (!row.売上分類 && !excluded[segKey]) {
      const detailKey = row.商品コード || row.商品番号 || '_no_code_';
      const existing = otherDetails.get(detailKey) || {
        商品番号: row.商品番号 || '',
        商品コード: row.商品コード || '',
        商品名: row.商品名 || '',
        解決方法: row.解決方法,
        売上合計: 0, 個数: 0, count: 0,
      };
      existing.売上合計 += sale;
      existing.個数 += row.個数 || 0;
      existing.count++;
      otherDetails.set(detailKey, existing);
    }
  }

  // MF連携用：税込み集計
  const t10 = byTax['10'];
  const t8 = byTax['8'];
  const mfColumns = ['商品売上(10%)', '商品売上(8%)', '合計'];
  const mfRow = {
    '商品売上(10%)': Math.round(t10.クーポン値引後売上 * 1.1),
    '商品売上(8%)': Math.round(t8.クーポン値引後売上 * 1.08),
  };
  mfRow['合計'] = mfRow['商品売上(10%)'] + mfRow['商品売上(8%)'];

  // 粗利率計算
  for (const seg of [...Object.values(bySegment), ...Object.values(excluded)]) {
    if (seg.クーポン値引後売上 > 0) {
      seg.粗利率 = ((seg.クーポン値引後売上 - seg.原価合計) / seg.クーポン値引後売上 * 100).toFixed(1);
    } else {
      seg.粗利率 = '0.0';
    }
  }

  return {
    byTax,
    bySegment,
    excluded,
    otherDetails: [...otherDetails.values()].sort((a, b) => Math.abs(b.売上合計) - Math.abs(a.売上合計)),
    columns,
    mfRow,
    mfColumns,
  };
}

// ─── GET / — メイン画面 ───

router.get('/', (req, res) => {
  res.send(renderPage());
});

// ─── POST /upload — 複数CSVアップロード＆集計 ───

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

      // ヘッダー行を確認して列インデックスを特定
      const header = csvRows[0];
      // 楽天CSVの列マッピング（固定位置）
      const num = v => { const n = parseFloat((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };

      for (let i = 1; i < csvRows.length; i++) {
        const cols = csvRows[i];
        if (cols.length < 26) continue;

        const orderNo = cols[0] || '';
        if (!orderNo) continue;

        const confirmDate = cols[5] || '';
        const totalAmount = num(cols[15]); // 合計金額(P列)
        const storeCoupon = num(cols[18]); // 店舗発行クーポン利用額(S列)
        const productCode = cols[22] || ''; // 商品番号(W列)※表示用のみ
        const unitPrice = num(cols[24]); // 単価(Y列)
        const quantity = parseInt(cols[25]) || 0; // 個数(Z列)
        const skuCode = cols.length > 37 ? (cols[37] || '') : ''; // SKU管理番号(AL列)
        const systemSku = cols.length > 38 ? (cols[38] || '') : ''; // システム連携用SKU番号(AM列)
        const productName = cols[21] || ''; // 商品名(V列)

        // セグメント計算用売上合計: 単価 × 個数
        const saleAmount = unitPrice * quantity;

        // 按分後店舗発行クーポン額
        let couponShare = 0;
        if (storeCoupon !== 0 && totalAmount !== 0) {
          couponShare = storeCoupon * (saleAmount / totalAmount);
        }

        allRows.push({
          注文番号: orderNo,
          注文確定日時: confirmDate,
          商品番号: productCode,
          商品名: productName,
          単価: unitPrice,
          個数: quantity,
          売上合計: saleAmount,
          按分クーポン: couponShare,
          合計金額: totalAmount,
          店舗発行クーポン: storeCoupon,
          システム連携用SKU番号: systemSku,
          SKU管理番号: skuCode,
        });
      }
    }

    if (allRows.length === 0) {
      return res.status(400).json({ error: 'CSVからデータを読み取れませんでした' });
    }

    // 対象年月を推定（注文確定日時から）
    const firstDate = allRows[0]?.注文確定日時 || '';
    const yearMonth = firstDate.slice(0, 7).replace(/\//g, '-');

    // 商品番号解決
    const { resolved, unresolved, unresolvedTax, unresolvedSegment, zeroGenka } = resolveProducts(allRows, db);

    // 集計
    const { byTax, bySegment, excluded, otherDetails, columns, mfRow, mfColumns } = aggregate(resolved);

    res.json({
      yearMonth,
      totalRows: allRows.length,
      fileCount: req.files.length,
      fileNames,
      resolvedCount: resolved.filter(r => r.解決方法 !== 'unresolved' && r.解決方法 !== 'no_code').length,
      unresolvedProducts: unresolved,
      unresolvedTax,
      unresolvedSegment,
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
    console.error('[RakutenAccounting] エラー:', e.message, e.stack);
    res.status(500).json({ error: '集計処理エラー: ' + e.message });
  }
});

// ─── POST /upload-billing — 店舗別仕訳書CSVアップロード ───

router.post('/upload-billing', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });

  try {
    const buf = fs.readFileSync(req.file.path);
    fs.unlinkSync(req.file.path);

    const csvRows = parseShiftJisCsv(buf);
    if (csvRows.length < 2) return res.status(400).json({ error: 'CSVにデータがありません' });

    const num = v => { const n = parseFloat((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };

    // ヘッダー: 発行日,決済期No,店舗別入金No,店舗部ID,ＦＲＬ,店舗名,支払(税込額),相殺(税抜額),相殺(税額),支払/相殺区分,集計区分,品目,決済対象期間開始日,決済対象期間終了日,金額,消費税額,税率
    const billingRows = [];
    let totalPayment = 0;
    let totalOffset = 0;

    for (let i = 1; i < csvRows.length; i++) {
      const cols = csvRows[i];
      if (cols.length < 17) continue;

      const row = {
        発行日: cols[0] || '',
        店舗名: cols[5] || '',
        '支払(税込額)': num(cols[6]),
        '相殺(税抜額)': num(cols[7]),
        '相殺(税額)': num(cols[8]),
        '支払/相殺区分': cols[9] || '',
        集計区分: cols[10] || '',
        品目: cols[11] || '',
        決済対象期間開始日: cols[12] || '',
        決済対象期間終了日: cols[13] || '',
        金額: num(cols[14]),
        消費税額: num(cols[15]),
        税率: cols[16] || '',
      };

      billingRows.push(row);

      if (row['支払/相殺区分'] === '支払') {
        totalPayment += row.金額;
      } else {
        totalOffset += row.金額;
      }
    }

    // カテゴリ別集計
    const byCategory = {};
    for (const row of billingRows) {
      const cat = row.品目 || '(空)';
      if (!byCategory[cat]) byCategory[cat] = { 品目: cat, 金額: 0, 消費税額: 0, 税込合計: 0, 件数: 0 };
      byCategory[cat].金額 += row.金額;
      byCategory[cat].消費税額 += row.消費税額;
      byCategory[cat].税込合計 += row.金額 + row.消費税額;
      byCategory[cat].件数++;
    }

    res.json({
      totalRows: billingRows.length,
      rows: billingRows,
      byCategory: Object.values(byCategory),
      totalPayment,
      totalOffset,
      発行日: billingRows[0]?.発行日 || '',
    });
  } catch (e) {
    console.error('[RakutenAccounting] 仕訳書エラー:', e.message, e.stack);
    res.status(500).json({ error: '仕訳書取込エラー: ' + e.message });
  }
});

// ─── POST /confirm — 集計確定（DB保存）───

router.post('/confirm', (req, res) => {
  const db = getMirrorDB();
  const { yearMonth, totalRows, resolvedCount, unresolvedCount,
    byTax, bySegment, excluded, mfRow, adCost, billing } = req.body;

  if (!yearMonth) return res.status(400).json({ error: 'yearMonth は必須です' });

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    db.prepare(`INSERT OR REPLACE INTO mart_rakuten_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, ad_cost, billing, confirmed_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      yearMonth, totalRows || 0, resolvedCount || 0, unresolvedCount || 0,
      JSON.stringify(byTax), JSON.stringify(bySegment), JSON.stringify(excluded),
      JSON.stringify(mfRow), adCost || 0, JSON.stringify(billing || {}), now
    );

    db.prepare(`INSERT INTO mart_rakuten_upload_log
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
    const rows = db.prepare('SELECT * FROM mart_rakuten_monthly_summary ORDER BY year_month DESC').all();
    const parsed = rows.map(r => ({
      ...r,
      by_tax: JSON.parse(r.by_tax || '{}'),
      by_segment: JSON.parse(r.by_segment || '{}'),
      excluded: JSON.parse(r.excluded || '{}'),
      mf_row: JSON.parse(r.mf_row || '{}'),
      billing: JSON.parse(r.billing || '{}'),
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
    const row = db.prepare('SELECT * FROM mart_rakuten_monthly_summary WHERE year_month = ?').get(req.params.yearMonth);
    if (!row) return res.status(404).json({ error: '該当月のデータがありません' });
    res.json({
      ...row,
      by_tax: JSON.parse(row.by_tax || '{}'),
      by_segment: JSON.parse(row.by_segment || '{}'),
      excluded: JSON.parse(row.excluded || '{}'),
      mf_row: JSON.parse(row.mf_row || '{}'),
      billing: JSON.parse(row.billing || '{}'),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── HTML ───

function renderPage() {
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>楽天売上集計 - B-Faith</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#333;font-size:14px}
    .header{background:#bf0000;color:white;padding:12px 24px;display:flex;align-items:center;gap:16px}
    .header h1{font-size:18px}
    .header a{color:#ffcccc;text-decoration:none;font-size:13px}
    .wrap{max-width:1800px;margin:16px auto;padding:0 16px}
    .card{background:white;border-radius:8px;padding:20px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1);overflow-x:auto}
    .card h2{font-size:15px;color:#555;margin-bottom:10px}
    .btn{padding:8px 20px;border:none;border-radius:4px;cursor:pointer;font-size:14px}
    .btn-p{background:#bf0000;color:white}.btn-p:hover{background:#990000}
    .btn-s{background:#27ae60;color:white}.btn-s:hover{background:#1e8449}
    .btn-w{background:#e67e22;color:white}.btn-w:hover{background:#d35400}
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
    .tab-bar{display:flex;gap:4px;margin-bottom:12px;border-bottom:2px solid #ddd;padding-bottom:0}
    .tab-btn{padding:8px 16px;border:none;background:#eee;cursor:pointer;border-radius:4px 4px 0 0;font-size:13px}
    .tab-btn.active{background:#bf0000;color:white}
    .tab-content{display:none}.tab-content.active{display:block}
    .acc-header{cursor:pointer;padding:10px 12px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:4px;margin-bottom:2px;display:flex;justify-content:space-between;align-items:center;font-size:13px}
    .acc-header:hover{background:#e9ecef}
    .acc-header .arrow{transition:transform .2s;font-size:10px}
    .acc-header.open .arrow{transform:rotate(90deg)}
    .acc-body{display:none;padding:12px;border:1px solid #eee;border-top:none;margin-bottom:8px;background:#fff}
    .acc-body.open{display:block}
    .detail-table td{font-size:12px;font-weight:normal}
    .detail-table th{font-size:11px}
  </style>
</head>
<body>
  <div class="header">
    <h1>楽天売上集計</h1>
    <a href="/">← ポータルに戻る</a>
  </div>
  <div class="wrap">
    <!-- 工程1: 注文データCSVアップロード -->
    <div class="card">
      <h2>工程1: 楽天注文データCSVアップロード</h2>
      <p class="meta">楽天RMS → 注文データダウンロード → 決済確定日指定・楽天売上集計用テンプレート（Shift_JIS）</p>
      <p class="meta" style="color:#bf0000;font-weight:bold">複数ファイル選択可（最大10ファイル） — Ctrl+クリックまたはShift+クリックで複数選択</p>
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

      <!-- タブ: 未登録情報 -->
      <div id="unresolvedArea" class="card" style="display:none">
        <h2>未登録情報</h2>
        <div class="tab-bar" id="unresolvedTabs"></div>
        <div id="unresolvedContent"></div>
      </div>

      <!-- 工程5: 税率別集計 -->
      <div class="card">
        <h2>工程5: 税率別集計</h2>
        <div id="taxTable"></div>
      </div>

      <!-- MF連携用 -->
      <div class="card">
        <h2>MF連携用 税込み集計</h2>
        <div id="mfTable"></div>
      </div>

      <!-- セグメント別集計 -->
      <div class="card">
        <h2>工程5: セグメント別集計（管理会計用）</h2>
        <div id="segmentTable"></div>
        <div id="excludedInfo"></div>
      </div>

      <!-- 変動費サマリー -->
      <div class="card">
        <h2>変動費サマリー</h2>
        <div id="costSummary"><span class="meta">店舗別仕訳書CSVを取り込むと表示されます</span></div>
        <div id="costBySegment" style="margin-top:12px"></div>
      </div>

      <!-- その他明細 -->
      <div id="otherDetailCard" class="card" style="display:none">
        <h2>「その他/未分類」明細</h2>
        <div id="otherDetailList"></div>
      </div>

      <!-- 原価ゼロ警告 -->
      <div id="zeroGenkaCard" class="card" style="display:none">
        <h2>原価ゼロで計算された商品</h2>
        <p class="meta">商品マスタの原価が0またはNULLのため、原価0円で集計されています。</p>
        <div id="zeroGenkaList"></div>
      </div>

    </div>

    <!-- 工程6: 店舗別仕訳書（常時表示） -->
    <div class="card">
      <h2>工程6: 店舗別仕訳書CSV取り込み</h2>
      <p class="meta">楽天RMS → 支払明細 からダウンロードしたCSV（Shift_JIS）</p>
      <div style="margin-top:8px;display:flex;gap:8px;align-items:center">
        <input type="file" id="billingFile" accept=".csv">
        <button class="btn btn-w" id="billingBtn" onclick="doBillingUpload()">仕訳書取り込み</button>
      </div>
      <div id="billingResult" style="margin-top:8px"></div>
    </div>

    <!-- 確定（工程6の後） -->
    <div class="card" id="confirmCard">
      <h2>確定</h2>
      <div id="confirmPreCheck" class="warn" style="margin-bottom:8px">注文データCSVと店舗別仕訳書CSVの両方をアップロードしてから確定してください</div>
      <div style="display:flex;gap:12px;align-items:center;margin-bottom:8px">
        <span id="adCostDisplay" class="meta"></span>
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
    let billingData = null;
    const fmt = n => {
      if (n === 0) return '0';
      const s = Math.round(n).toLocaleString();
      return n < 0 ? '<span class="negative">' + s + '</span>' : s;
    };

    // ─── 工程1: アップロード ───
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
      const hasUnresolved = data.unresolvedProducts.length > 0;
      const hasUnresTax = data.unresolvedTax && data.unresolvedTax.length > 0;
      const hasUnresSeg = data.unresolvedSegment && data.unresolvedSegment.length > 0;
      const canConfirm = !hasUnresolved;

      let summaryHtml = '<div class="' + (canConfirm ? 'ok' : 'warn') + '">';
      summaryHtml += '<b>対象年月: ' + data.yearMonth + '</b> （' + data.fileCount + 'ファイル）<br>';
      summaryHtml += '総行数: ' + data.totalRows + ' / 解決済: ' + data.resolvedCount + ' / 未登録商品: ' + data.unresolvedProducts.length + '件';
      if (hasUnresTax) summaryHtml += ' / <span class="negative">税率未登録: ' + data.unresolvedTax.length + '件（10%仮扱い）</span>';
      if (hasUnresSeg) summaryHtml += ' / <span class="negative">セグメント未登録: ' + data.unresolvedSegment.length + '件</span>';
      if (canConfirm) summaryHtml += '<br><b style="color:#27ae60">全商品解決済み — 確定可能</b>';
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

        // 未登録商品
        if (hasUnresolved) {
          contentHtml += '<div id="unres-product" class="tab-content active">';
          contentHtml += '<table><tr><th>商品番号</th><th>商品名</th><th>出現数</th><th>売上合計</th></tr>';
          for (const u of data.unresolvedProducts) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
          }
          contentHtml += '</table></div>';
        }

        // 税率未登録
        if (hasUnresTax) {
          contentHtml += '<div id="unres-tax" class="tab-content' + (!hasUnresolved?' active':'') + '">';
          contentHtml += '<div class="warn">税率未登録の商品は10%として仮集計されています。正確な集計にはwarehouse側で税率登録が必要です。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>出現数</th><th>売上合計</th></tr>';
          for (const u of data.unresolvedTax) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
          }
          contentHtml += '</table></div>';
        }

        // セグメント未登録
        if (hasUnresSeg) {
          contentHtml += '<div id="unres-seg" class="tab-content' + (!hasUnresolved && !hasUnresTax?' active':'') + '">';
          contentHtml += '<div class="warn">セグメント未登録の商品は「その他/未分類」に分類されています。warehouse側で売上分類を登録してください。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>原価</th><th>出現数</th><th>売上合計</th></tr>';
          for (const u of data.unresolvedSegment) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + fmt(u.genka) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
          }
          contentHtml += '</table></div>';
        }

        document.getElementById('unresolvedContent').innerHTML = contentHtml;
      } else {
        document.getElementById('unresolvedArea').style.display = 'none';
      }

      // 税率別集計
      let taxHtml = '<table><tr><th>税率</th><th>売上合計</th><th>クーポン値引額</th><th>クーポン値引後売上</th><th>原価合計</th><th>行数</th></tr>';
      for (const [key, label] of [['10', '10%'], ['8', '8%']]) {
        const row = data.byTax[key];
        taxHtml += '<tr><td>' + label + '</td><td class="num">' + fmt(row.売上合計) + '</td><td class="num">' + fmt(row.クーポン値引額) + '</td><td class="num">' + fmt(row.クーポン値引後売上) + '</td><td class="num">' + fmt(row.原価合計) + '</td><td class="num">' + row.行数 + '</td></tr>';
      }
      const t10 = data.byTax['10'], t8 = data.byTax['8'];
      taxHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      taxHtml += '<td class="num">' + fmt(t10.売上合計 + t8.売上合計) + '</td>';
      taxHtml += '<td class="num">' + fmt(t10.クーポン値引額 + t8.クーポン値引額) + '</td>';
      taxHtml += '<td class="num">' + fmt(t10.クーポン値引後売上 + t8.クーポン値引後売上) + '</td>';
      taxHtml += '<td class="num">' + fmt(t10.原価合計 + t8.原価合計) + '</td>';
      taxHtml += '<td class="num">' + (t10.行数 + t8.行数) + '</td></tr></table>';
      document.getElementById('taxTable').innerHTML = taxHtml;

      // MF連携用
      if (data.mfRow) {
        let mfHtml = '<table><tr>';
        data.mfColumns.forEach(c => mfHtml += '<th>' + c + '</th>');
        mfHtml += '</tr><tr>';
        data.mfColumns.forEach(c => mfHtml += '<td class="num" style="font-weight:bold">' + fmt(data.mfRow[c]) + '</td>');
        mfHtml += '</tr></table>';
        document.getElementById('mfTable').innerHTML = mfHtml;
      }

      // セグメント別
      renderSegmentTable(data);

      // その他明細
      if (data.otherDetails && data.otherDetails.length > 0) {
        document.getElementById('otherDetailCard').style.display = 'block';
        let html = '<table class="detail-table"><tr><th>商品番号</th><th>商品コード</th><th>商品名</th><th>解決方法</th><th>行数</th><th>個数</th><th>売上合計</th></tr>';
        for (const d of data.otherDetails) {
          const method = { am_direct: 'AM列一致', al_fallback: 'AL列一致', w_fallback: 'W列一致', unresolved: '未解決', no_code: 'コードなし' }[d.解決方法] || d.解決方法;
          html += '<tr><td style="text-align:left">' + (d.商品番号 || '-') + '</td><td style="text-align:left">' + (d.商品コード || '-') + '</td><td style="text-align:left">' + (d.商品名 || '').slice(0, 50) + '</td><td style="text-align:left">' + method + '</td><td class="num">' + d.count + '</td><td class="num">' + d.個数 + '</td><td class="num">' + fmt(d.売上合計) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('otherDetailList').innerHTML = html;
      } else {
        document.getElementById('otherDetailCard').style.display = 'none';
      }

      // 原価ゼロ警告
      if (data.zeroGenka && data.zeroGenka.length > 0) {
        document.getElementById('zeroGenkaCard').style.display = 'block';
        let html = '<div class="warn" style="margin-bottom:8px"><b>' + data.zeroGenka.length + '商品</b>が原価0円で計算されています</div>';
        html += '<table class="detail-table"><tr><th>商品コード</th><th>商品名</th><th>出現行数</th><th>数量合計</th><th>売上合計</th></tr>';
        for (const z of data.zeroGenka) {
          html += '<tr><td style="text-align:left">' + z.商品コード + '</td><td style="text-align:left">' + (z.商品名 || '').slice(0, 50) + '</td><td class="num">' + z.count + '</td><td class="num">' + z.数量合計 + '</td><td class="num">' + fmt(z.売上合計) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('zeroGenkaList').innerHTML = html;
      } else {
        document.getElementById('zeroGenkaCard').style.display = 'none';
      }
      updateConfirmState();
    }

    function getAdCostFromBilling() {
      if (!billingData || !billingData.byCategory) return 0;
      let ad = 0;
      for (const cat of billingData.byCategory) {
        if (cat.品目 && cat.品目.includes('広告')) {
          ad += (cat.金額 || 0) + (cat.消費税額 || 0);
        }
      }
      return ad;
    }

    function getBillingTotals() {
      if (!billingData || !billingData.rows) return null;
      // 請求行のみの合計（= 楽天から請求される経費合計）
      let seikyuTotal = 0;
      for (const row of billingData.rows) {
        if (row['支払/相殺区分'] === '請求') {
          seikyuTotal += (row.金額 || 0);
        }
      }
      const adCost = getAdCostFromBilling();
      const coupon = lastData ? (lastData.byTax['10'].クーポン値引額 + lastData.byTax['8'].クーポン値引額) : 0;
      const pfFee = seikyuTotal - adCost - coupon;
      return { seikyuTotal, adCost, coupon, pfFee };
    }

    function updateConfirmState() {
      const ready = lastData && billingData;
      document.getElementById('confirmBtn').disabled = !ready;
      const pre = document.getElementById('confirmPreCheck');
      if (ready) {
        const ad = getAdCostFromBilling();
        pre.className = 'ok';
        pre.innerHTML = '注文データ: <b>' + lastData.yearMonth + '</b>（' + lastData.totalRows + '行） / 仕訳書: <b>' + billingData.totalRows + '行</b>' + (ad ? ' / 広告費（税込）: <b>\\u00a5' + Math.round(ad).toLocaleString() + '</b>' : '');
        document.getElementById('adCostDisplay').textContent = '';
      } else {
        pre.className = 'warn';
        let missing = [];
        if (!lastData) missing.push('注文データCSV');
        if (!billingData) missing.push('店舗別仕訳書CSV');
        pre.innerHTML = missing.join('と') + 'をアップロードしてから確定してください';
      }
    }

    // 按分計算ヘルパー（丸め誤差を最大セグメントに吸収）
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
      const bt = getBillingTotals();
      const ad = bt ? bt.adCost : 0;
      const pf = bt ? bt.pfFee : 0;

      // セグメント1・2・3の売上按分
      const allocTargets = ['1', '2', '3'];
      const salesByKey = {};
      for (const [key, row] of Object.entries(seg)) { salesByKey[key] = row.売上合計 || 0; }
      const adByKey = allocateByRatio(ad, salesByKey, allocTargets);
      const pfByKey = allocateByRatio(pf, salesByKey, allocTargets);

      let html = '<table><tr><th>セグメント</th><th>売上合計</th><th>クーポン値引額</th><th>クーポン値引後売上</th><th>PF手数料</th><th>広告費</th><th>原価合計</th><th>粗利率</th><th>行数</th></tr>';
      let tot = { 売上合計: 0, クーポン値引額: 0, クーポン値引後売上: 0, 原価合計: 0, 行数: 0 };
      let totAd = 0, totPf = 0;
      for (const [key, row] of Object.entries(seg)) {
        const label = data.segmentNames[key] || (key === 'other' ? 'その他/未分類' : key);
        html += '<tr><td>' + key + ': ' + label + '</td>';
        html += '<td class="num">' + fmt(row.売上合計) + '</td>';
        html += '<td class="num">' + fmt(row.クーポン値引額) + '</td>';
        html += '<td class="num">' + fmt(row.クーポン値引後売上) + '</td>';
        html += '<td class="num">' + fmt(pfByKey[key] || 0) + '</td>';
        html += '<td class="num">' + fmt(adByKey[key] || 0) + '</td>';
        html += '<td class="num">' + fmt(row.原価合計) + '</td>';
        html += '<td class="num">' + (row.粗利率 || '0.0') + '%</td>';
        html += '<td class="num">' + row.行数 + '</td></tr>';
        tot.売上合計 += row.売上合計; tot.クーポン値引額 += row.クーポン値引額;
        tot.クーポン値引後売上 += row.クーポン値引後売上; tot.原価合計 += row.原価合計; tot.行数 += row.行数;
        totAd += (adByKey[key] || 0); totPf += (pfByKey[key] || 0);
      }
      const totGross = tot.クーポン値引後売上 > 0 ? ((tot.クーポン値引後売上 - tot.原価合計) / tot.クーポン値引後売上 * 100).toFixed(1) : '0.0';
      html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      html += '<td class="num">' + fmt(tot.売上合計) + '</td>';
      html += '<td class="num">' + fmt(tot.クーポン値引額) + '</td>';
      html += '<td class="num">' + fmt(tot.クーポン値引後売上) + '</td>';
      html += '<td class="num">' + fmt(totPf) + '</td>';
      html += '<td class="num">' + fmt(totAd) + '</td>';
      html += '<td class="num">' + fmt(tot.原価合計) + '</td>';
      html += '<td class="num">' + totGross + '%</td>';
      html += '<td class="num">' + tot.行数 + '</td></tr></table>';
      document.getElementById('segmentTable').innerHTML = html;

      // 除外セグメント
      let exclHtml = '';
      if (data.excluded) {
        for (const [key, row] of Object.entries(data.excluded)) {
          if (row.行数 > 0) {
            const label = data.excludedNames[key] || key;
            exclHtml += '<div class="excluded"><b>除外: ' + key + ': ' + label + '</b>（' + row.行数 + '行） — 売上合計: ' + fmt(row.売上合計) + ' / クーポン値引後: ' + fmt(row.クーポン値引後売上) + ' / 原価: ' + fmt(row.原価合計) + '</div>';
          }
        }
      }
      document.getElementById('excludedInfo').innerHTML = exclHtml;

      // 変動費サマリー
      if (bt) {
        let csHtml = '<table><tr><th>PF手数料</th><th>運賃</th><th>広告費</th><th>店舗発行クーポン利用分</th><th>合計</th></tr>';
        csHtml += '<tr>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(bt.pfFee) + '</td>';
        csHtml += '<td class="num" style="color:#aaa">-</td>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(bt.adCost) + '</td>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(bt.coupon) + '</td>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(bt.seikyuTotal) + '</td>';
        csHtml += '</tr></table>';
        csHtml += '<p class="meta" style="margin-top:4px">PF手数料 = 請求合計(' + fmt(bt.seikyuTotal) + ') − 広告費(' + fmt(bt.adCost) + ') − クーポン(' + fmt(bt.coupon) + ')</p>';
        document.getElementById('costSummary').innerHTML = csHtml;

        // セグメント別変動費按分テーブル
        let cbHtml = '<table><tr><th>セグメント</th><th>売上比率</th><th>PF手数料</th><th>広告費</th><th>変動費合計</th></tr>';
        let cbTotPf = 0, cbTotAd = 0;
        for (const [key, row] of Object.entries(seg)) {
          const label = data.segmentNames[key] || (key === 'other' ? 'その他/未分類' : key);
          const ratio = tot.売上合計 > 0 ? (row.売上合計 / tot.売上合計 * 100).toFixed(1) : '0.0';
          const segPf = pfByKey[key] || 0;
          const segAd = adByKey[key] || 0;
          cbHtml += '<tr><td>' + key + ': ' + label + '</td>';
          cbHtml += '<td class="num">' + ratio + '%</td>';
          cbHtml += '<td class="num">' + fmt(segPf) + '</td>';
          cbHtml += '<td class="num">' + fmt(segAd) + '</td>';
          cbHtml += '<td class="num" style="font-weight:bold">' + fmt(segPf + segAd) + '</td></tr>';
          cbTotPf += segPf; cbTotAd += segAd;
        }
        cbHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
        cbHtml += '<td class="num">100%</td>';
        cbHtml += '<td class="num">' + fmt(cbTotPf) + '</td>';
        cbHtml += '<td class="num">' + fmt(cbTotAd) + '</td>';
        cbHtml += '<td class="num">' + fmt(cbTotPf + cbTotAd) + '</td></tr></table>';
        document.getElementById('costBySegment').innerHTML = cbHtml;
      } else {
        document.getElementById('costSummary').innerHTML = '<span class="meta">店舗別仕訳書CSVを取り込むと表示されます</span>';
        document.getElementById('costBySegment').innerHTML = '';
      }
    }

    function updateAdCost() { /* legacy: no-op */ }

    function switchTab(el, tabId) {
      el.parentElement.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      el.classList.add('active');
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      document.getElementById(tabId).classList.add('active');
    }

    // ─── 工程6: 仕訳書取り込み ───
    async function doBillingUpload() {
      const fileInput = document.getElementById('billingFile');
      if (!fileInput.files.length) { alert('ファイルを選択してください'); return; }
      const btn = document.getElementById('billingBtn');
      btn.disabled = true;
      btn.textContent = '処理中...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);

      try {
        const r = await fetch(location.pathname + '/upload-billing', { method: 'POST', body: formData });
        const data = await r.json();
        if (data.error) { document.getElementById('billingResult').innerHTML = '<div class="err">' + data.error + '</div>'; return; }
        billingData = data;
        let html = '<div class="ok">仕訳書取り込み完了: ' + data.totalRows + '行（発行日: ' + data.発行日 + '）</div>';
        html += '<table><tr><th>品目</th><th>金額</th><th>消費税額</th><th>税込合計</th><th>件数</th></tr>';
        for (const cat of data.byCategory) {
          html += '<tr><td>' + cat.品目 + '</td><td class="num">' + fmt(cat.金額) + '</td><td class="num">' + fmt(cat.消費税額) + '</td><td class="num">' + fmt(cat.税込合計) + '</td><td class="num">' + cat.件数 + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('billingResult').innerHTML = html;
        updateConfirmState();
        if (lastData) renderSegmentTable(lastData); // 広告費反映でセグメント再描画
      } catch(e) {
        document.getElementById('billingResult').innerHTML = '<div class="err">エラー: ' + e.message + '</div>';
      }
      btn.disabled = false;
      btn.textContent = '仕訳書取り込み';
    }

    // ─── 確定 ───
    async function doConfirm() {
      if (!lastData) { alert('先に注文データCSVをアップロードしてください'); return; }
      if (!billingData) { alert('先に店舗別仕訳書CSVをアップロードしてください'); return; }
      if (!confirm(lastData.yearMonth + ' の集計を確定しますか？')) return;
      const btn = document.getElementById('confirmBtn');
      btn.disabled = true;
      btn.textContent = '保存中...';
      try {
        const adCost = getAdCostFromBilling();
        const r = await fetch(location.pathname + '/confirm', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            yearMonth: lastData.yearMonth,
            totalRows: lastData.totalRows,
            resolvedCount: lastData.resolvedCount,
            unresolvedCount: lastData.unresolvedProducts?.length || 0,
            byTax: lastData.byTax,
            bySegment: lastData.bySegment,
            excluded: lastData.excluded,
            mfRow: lastData.mfRow,
            adCost,
            billing: billingData ? billingData.byCategory : null,
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

    // ─── 過去データ ───
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
          const seg = row.by_segment || {};
          let hdrSales = 0, hdrAfter = 0;
          for (const sr of Object.values(seg)) { hdrSales += (sr.売上合計 || 0); hdrAfter += (sr.クーポン値引後売上 || 0); }
          const ad = Math.round(row.ad_cost || 0);

          html += '<div class="acc-header" onclick="toggleAcc(this)" data-idx="' + i + '">';
          html += '<span><b>' + row.year_month + '</b> — 売上合計: \\u00a5' + Math.round(hdrSales).toLocaleString()
            + ' / クーポン値引後: \\u00a5' + Math.round(hdrAfter).toLocaleString()
            + (ad ? ' / 広告費: \\u00a5' + ad.toLocaleString() : '')
            + ' <span class="meta">（' + (row.confirmed_at || '') + '）</span></span>';
          html += '<span class="arrow">&#9654;</span></div>';
          html += '<div class="acc-body" id="acc-' + i + '">';

          // MF連携用
          const mf = row.mf_row || {};
          if (mf['合計']) {
            const mfCols = ['商品売上(10%)', '商品売上(8%)', '合計'];
            html += '<h3 style="font-size:13px;color:#555;margin-bottom:4px">MF連携用 税込み集計</h3>';
            html += '<table><tr>';
            mfCols.forEach(c => html += '<th>' + c + '</th>');
            html += '</tr><tr>';
            mfCols.forEach(c => html += '<td class="num" style="font-weight:bold">' + fmt(mf[c] || 0) + '</td>');
            html += '</tr></table>';
          }

          // セグメント別
          const segNames = {1:'自社商品', 2:'取扱限定', 3:'仕入れ商品'};
          const adTargets = ['1','2'];
          const hSales = {}; let hTotalSales = 0;
          for (const [k, sr] of Object.entries(seg)) { hSales[k] = sr.売上合計 || 0; if (adTargets.includes(k)) hTotalSales += hSales[k]; }
          const hAd = {}; let hAdSum = 0;
          for (const k of Object.keys(seg)) {
            if (!adTargets.includes(k) || hTotalSales === 0) { hAd[k] = 0; continue; }
            hAd[k] = Math.round(ad * hSales[k] / hTotalSales); hAdSum += hAd[k];
          }
          if (ad && hTotalSales > 0) {
            const mk = Object.keys(seg).filter(k => adTargets.includes(k)).sort((a,b) => (hSales[b]||0)-(hSales[a]||0))[0];
            if (mk) hAd[mk] += (ad - hAdSum);
          }

          html += '<h3 style="font-size:13px;color:#555;margin:12px 0 4px">セグメント別集計</h3>';
          html += '<table><tr><th>セグメント</th><th>売上合計</th><th>クーポン値引額</th><th>クーポン値引後売上</th><th>広告費</th><th>原価合計</th><th>粗利率</th><th>行数</th></tr>';
          let sTot = { 売上合計:0, クーポン値引額:0, クーポン値引後売上:0, 原価合計:0, 行数:0 };
          let sAdTot = 0;
          for (const [key, sr] of Object.entries(seg)) {
            const lb = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
            html += '<tr><td>' + key + ': ' + lb + '</td>';
            html += '<td class="num">' + fmt(sr.売上合計||0) + '</td>';
            html += '<td class="num">' + fmt(sr.クーポン値引額||0) + '</td>';
            html += '<td class="num">' + fmt(sr.クーポン値引後売上||0) + '</td>';
            html += '<td class="num">' + fmt(hAd[key]||0) + '</td>';
            html += '<td class="num">' + fmt(sr.原価合計||0) + '</td>';
            html += '<td class="num">' + (sr.粗利率 || '0.0') + '%</td>';
            html += '<td class="num">' + (sr.行数||0) + '</td></tr>';
            sTot.売上合計 += (sr.売上合計||0); sTot.クーポン値引額 += (sr.クーポン値引額||0);
            sTot.クーポン値引後売上 += (sr.クーポン値引後売上||0); sTot.原価合計 += (sr.原価合計||0); sTot.行数 += (sr.行数||0);
            sAdTot += (hAd[key]||0);
          }
          const totGross = sTot.クーポン値引後売上 > 0 ? ((sTot.クーポン値引後売上 - sTot.原価合計) / sTot.クーポン値引後売上 * 100).toFixed(1) : '0.0';
          html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
          html += '<td class="num">' + fmt(sTot.売上合計) + '</td><td class="num">' + fmt(sTot.クーポン値引額) + '</td>';
          html += '<td class="num">' + fmt(sTot.クーポン値引後売上) + '</td><td class="num">' + fmt(sAdTot) + '</td>';
          html += '<td class="num">' + fmt(sTot.原価合計) + '</td><td class="num">' + totGross + '%</td>';
          html += '<td class="num">' + sTot.行数 + '</td></tr></table>';

          // 除外
          const excl = row.excluded || {};
          for (const [ek, er] of Object.entries(excl)) {
            if ((er.行数||0) > 0) {
              html += '<div class="excluded"><b>除外: ' + ek + ': 輸出</b>（' + er.行数 + '行） — 売上: ' + fmt(er.売上合計||0) + '</div>';
            }
          }

          // 仕訳書
          const bill = row.billing;
          if (bill && Array.isArray(bill) && bill.length > 0) {
            html += '<h3 style="font-size:13px;color:#555;margin:12px 0 4px">店舗別仕訳</h3>';
            html += '<table><tr><th>品目</th><th>金額</th><th>消費税額</th><th>税込合計</th></tr>';
            for (const cat of bill) {
              html += '<tr><td>' + cat.品目 + '</td><td class="num">' + fmt(cat.金額) + '</td><td class="num">' + fmt(cat.消費税額) + '</td><td class="num">' + fmt(cat.税込合計) + '</td></tr>';
            }
            html += '</table>';
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
        const adTargets = ['1','2'];
        let csv = '\\uFEFF';
        csv += '集計月,セグメント,売上合計,クーポン値引額,クーポン値引後売上,広告費,原価合計,粗利率,行数\\n';

        for (const row of rows) {
          const seg = row.by_segment || {};
          const ad = row.ad_cost || 0;
          let tSales = 0;
          const sales = {};
          for (const [k, sr] of Object.entries(seg)) { sales[k] = sr.売上合計 || 0; if (adTargets.includes(k)) tSales += sales[k]; }
          const adMap = {};
          let adSum = 0;
          for (const k of Object.keys(seg)) {
            if (!adTargets.includes(k) || tSales === 0) { adMap[k] = 0; continue; }
            adMap[k] = Math.round(ad * sales[k] / tSales); adSum += adMap[k];
          }
          if (ad && tSales > 0) {
            const mk = Object.keys(seg).filter(k => adTargets.includes(k)).sort((a,b) => (sales[b]||0)-(sales[a]||0))[0];
            if (mk) adMap[mk] += (ad - adSum);
          }

          for (const [key, sr] of Object.entries(seg)) {
            const label = segNames[key] || key;
            csv += row.year_month + ',' + key + ':' + label + ',' + Math.round(sr.売上合計||0) + ',' + Math.round(sr.クーポン値引額||0) + ',' + Math.round(sr.クーポン値引後売上||0) + ',' + (adMap[key]||0) + ',' + Math.round(sr.原価合計||0) + ',' + (sr.粗利率||'0.0') + ',' + (sr.行数||0) + '\\n';
          }
        }

        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'Rakuten_segment_history.csv';
        a.click();
      } catch(e) {
        alert('ダウンロードエラー: ' + e.message);
      }
    }

    // ファイル選択数表示
    document.getElementById('csvFiles').addEventListener('change', function() {
      const n = this.files.length;
      document.getElementById('fileCount').textContent = n > 0 ? n + 'ファイル選択中' : '';
    });

    loadHistory();
  </script>
</body>
</html>`;
}

export default router;
