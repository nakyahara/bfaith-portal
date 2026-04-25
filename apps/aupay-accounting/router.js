/**
 * auペイマーケット売上集計ツール
 *
 * auペイマーケットの「会計用注文データDL項目」CSVをアップロードし、
 * mirror_products を使って税率別・セグメント別の売上集計を自動計算する。
 * 口座振替通知書PDF（任意）からPF手数料を自動読み取り。
 *
 * 工程1: 注文データCSVアップロード（Shift_JIS、複数対応）
 * 工程2: 未登録商品の税率・セグメント登録
 * 工程3: 税率別・セグメント別集計
 * 工程4: PF手数料入力（PDF or 手入力）
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

const SEGMENT_NAMES = { 1: '自社商品', 2: '取引先限定', 3: '仕入れ商品' };
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
  // 代表商品コードで引けるマップ（複数商品が同じ代表コードを持つ場合は最初の1件）
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
  const resolvedByRepCode = new Map(); // 代表商品コードで紐付けた商品リスト

  for (const row of rows) {
    const itemCode = (row.商品コード || '').toLowerCase();
    const mgmtId = (row.管理コード || '').toLowerCase();

    if (!itemCode) {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 解決方法: 'no_code' });
      continue;
    }

    // Stage 1: F列ありの場合 itemCode+mgmtId で検索
    let product = null;
    let resolveMethod = null;
    if (mgmtId) {
      const combinedCode = itemCode + mgmtId;
      product = productsMap.get(combinedCode);
      if (product) resolveMethod = 'combined';
    }
    // Stage 2: itemCode のみで検索
    if (!product) {
      product = productsMap.get(itemCode);
      if (product) resolveMethod = 'direct';
    }
    // Stage 3: itemCode を代表商品コードとして検索
    if (!product) {
      product = repCodeMap.get(itemCode);
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

      // 代表商品コードで紐付けた場合、リストに記録
      if (resolveMethod === 'rep_code') {
        const key = itemCode;
        const existing = resolvedByRepCode.get(key) || {
          auPayCode: row.商品コード || '', matchedCode: product.商品コード,
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
          csvTaxRate: row.CSV税率 || null, count: 0, amount: 0,
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
      const unresolvedKey = mgmtId ? itemCode + mgmtId : itemCode;
      const existing = unresolved.get(unresolvedKey) || {
        code: unresolvedKey, name: row.商品名 || '',
        csvTaxRate: row.CSV税率 || null, count: 0, amount: 0,
      };
      existing.count++;
      existing.amount += row.売上合計 || 0;
      unresolved.set(unresolvedKey, existing);
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
    const coupon = row.クーポン値引額 || 0;
    const afterCoupon = sale - coupon;
    const genka = (row.原価 || 0) * (row.個数 || 1);

    // 税率別: マスター税率 > CSV税率 > 10%仮扱い
    const taxRate = row.税率 || row.CSV税率 || 10;
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

  // MF連携用: CSVの金額は税込みなのでそのまま使う
  const t10 = byTax['10'];
  const t8 = byTax['8'];
  const mfRow = {
    '商品売上(10%)': Math.round(t10.クーポン値引後売上),
    '商品売上(8%)': Math.round(t8.クーポン値引後売上),
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

      // ヘッダー確認（先頭列=controlType）
      const header = csvRows[0];
      if (header[0] !== 'controlType') {
        return res.status(400).json({ error: file.originalname + ' はauペイマーケット形式ではありません（先頭列: ' + header[0] + '）' });
      }

      const num = v => { const n = parseFloat((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };

      // ヘッダーからインデックスを構築
      const colIdx = {};
      header.forEach((h, i) => { colIdx[h] = i; });

      for (let i = 1; i < csvRows.length; i++) {
        const cols = csvRows[i];
        if (cols.length < 20) continue;

        const col = name => cols[colIdx[name]] || '';

        const orderId = col('orderId');
        if (!orderId) continue;

        // キャンセル行を除外
        const cancelStatus = col('cancelStatus');
        if (cancelStatus === 'C') continue;

        const itemCode = col('itemCode');
        const itemManagementId = col('itemManagementId');
        const quantity = parseInt(col('unit')) || 0;
        const unitPrice = num(col('itemPrice'));
        const totalItemPrice = num(col('totalItemPrice')) || unitPrice * quantity;
        const taxRate = parseFloat(col('taxRate')) || 0;
        const csvTaxRate = taxRate === 0.08 ? 8 : 10;
        const title = col('itemName');
        const shipDate = col('shipDate');

        // クーポン: couponType D/M = モール負担（店舗負担0）、U = 店舗負担
        const couponTotal = num(col('couponTotalPrice'));
        const totalPrice = num(col('totalPrice'));
        const couponType = col('couponType');
        // 店舗負担クーポン: couponType=Uの場合、注文合計に対する明細の按分
        let shopCoupon = 0;
        if (couponType === 'U' && totalPrice > 0) {
          shopCoupon = couponTotal * (unitPrice * quantity) / totalPrice;
        }

        allRows.push({
          注文番号: orderId,
          商品コード: itemCode,
          管理コード: itemManagementId,
          商品名: title,
          単価: unitPrice,
          個数: quantity,
          売上合計: totalItemPrice,
          クーポン値引額: shopCoupon,
          発送日: shipDate,
          CSV税率: csvTaxRate,
        });
      }
    }

    if (allRows.length === 0) {
      return res.status(400).json({ error: 'CSVからデータを読み取れませんでした' });
    }

    // 対象年月を推定（発送日から）
    let yearMonth = '';
    for (const row of allRows) {
      if (row.発送日) {
        const d = row.発送日.replace(/\//g, '-');
        yearMonth = d.slice(0, 7);
        break;
      }
    }

    // 商品コード解決
    const { resolved, unresolved, unresolvedTax, unresolvedSegment, zeroGenka, resolvedByRepCode } = resolveProducts(allRows, db);

    // 集計
    const agg = aggregate(resolved);

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
      resolvedRows: resolved,
      byTax: agg.byTax,
      bySegment: agg.bySegment,
      excluded: agg.excluded,
      otherDetails: agg.otherDetails,
      mfRow: agg.mfRow,
    });
  } catch (e) {
    console.error('[AuPayAccounting] エラー:', e.message, e.stack);
    res.status(500).json({ error: '集計処理エラー: ' + e.message });
  }
});

// ─── POST /upload-pdf — 口座振替通知書PDFアップロード ───

router.post('/upload-pdf', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'PDFファイルが必要です' });

  try {
    const buf = fs.readFileSync(req.file.path);
    fs.unlinkSync(req.file.path);

    // pdf-parseでテキスト抽出
    let pdfParse;
    try {
      const mod = await import('pdf-parse');
      pdfParse = mod.default || mod;
    } catch {
      return res.status(500).json({ error: 'pdf-parseモジュールが見つかりません' });
    }

    const pdfData = await pdfParse(buf);
    const text = pdfData.text || '';

    // ご請求額(A+B) の金額を抽出
    // PDF text pattern: "ご請求額（A+B）" followed by amount like ￥146,054 or 146,054
    let totalFee = null;
    let adCost = null;

    // パターン1: "ご請求額" 付近の数値
    const feeMatch = text.match(/ご請求額[^0-9￥]*[￥]?\s*([\d,]+)/);
    if (feeMatch) {
      totalFee = parseInt(feeMatch[1].replace(/,/g, ''));
    }

    // パターン2: 広告掲載料
    const adMatch = text.match(/広告掲載料[^0-9￥]*[￥]?\s*([\d,]+)/);
    if (adMatch) {
      adCost = parseInt(adMatch[1].replace(/,/g, ''));
    }

    // 年月を抽出（"2026 4" or "2026年4月"パターン）
    let pdfYearMonth = '';
    const ymMatch = text.match(/(\d{4})\s+(\d{1,2})\s+ご請求額/);
    if (ymMatch) {
      pdfYearMonth = ymMatch[1] + '-' + String(parseInt(ymMatch[2])).padStart(2, '0');
    }

    // 内訳を抽出
    const breakdown = {};
    const items = [
      { key: '出店料', pattern: /出店料[^0-9￥]*[￥]?\s*([\d,]+)/ },
      { key: '成約手数料等', pattern: /成約手数料等[^0-9￥]*[￥]?\s*([\d,]+)/ },
      { key: 'ポイント付与原資', pattern: /ポイント付与原資[^0-9￥]*[￥]?\s*([\d,]+)/ },
      { key: 'オプション料', pattern: /オプション料[^0-9￥]*[￥]?\s*([\d,]+)/ },
    ];
    for (const item of items) {
      const m = text.match(item.pattern);
      if (m) breakdown[item.key] = parseInt(m[1].replace(/,/g, ''));
    }

    res.json({
      ok: true,
      totalFee,
      adCost: adCost || 0,
      pfFee: totalFee != null ? totalFee - (adCost || 0) : null,
      pdfYearMonth,
      breakdown,
      rawTextPreview: text.slice(0, 500),
    });
  } catch (e) {
    console.error('[AuPayAccounting] PDF解析エラー:', e.message, e.stack);
    res.status(500).json({ error: 'PDF解析エラー: ' + e.message });
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
    db.prepare(`INSERT OR REPLACE INTO mart_aupay_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, pf_fee, ad_cost, confirmed_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      yearMonth, totalRows || 0, resolvedCount || 0, unresolvedCount || 0,
      JSON.stringify(byTax), JSON.stringify(bySegment), JSON.stringify(excluded),
      JSON.stringify(mfRow), pfFee || 0, adCost || 0, now
    );

    db.prepare(`INSERT INTO mart_aupay_upload_log
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
    const rows = db.prepare('SELECT * FROM mart_aupay_monthly_summary ORDER BY year_month DESC').all();
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

// ─── POST /import-history — ヒストリカルデータ一括投入 ───

router.post('/import-history', (req, res) => {
  const key = req.headers['x-import-key'] || req.query.key;
  if (key !== 'bfaith-import-2026') return res.status(401).json({ error: 'Invalid key' });

  const db = getMirrorDB();
  const { months } = req.body;
  if (!Array.isArray(months)) return res.status(400).json({ error: 'months 配列が必要です' });

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    const stmt = db.prepare(`INSERT OR IGNORE INTO mart_aupay_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, pf_fee, ad_cost, confirmed_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`);

    let inserted = 0;
    const tx = db.transaction(() => {
      for (const m of months) {
        const r = stmt.run(
          m.yearMonth, 0, 0, 0,
          '{}', JSON.stringify(m.bySegment || {}), '{}', '{}',
          m.pfFee || 0, m.adCost || 0, now
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

// ─── HTML ───

function renderPage() {
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>auペイマーケット売上集計 - B-Faith</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#333;font-size:14px}
    .header{background:#e85514;color:white;padding:12px 24px;display:flex;align-items:center;gap:16px}
    .header h1{font-size:18px}
    .header a{color:#ffd4c2;text-decoration:none;font-size:13px}
    .wrap{max-width:1800px;margin:16px auto;padding:0 16px}
    .card{background:white;border-radius:8px;padding:20px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1);overflow-x:auto}
    .card h2{font-size:15px;color:#555;margin-bottom:10px}
    .btn{padding:8px 20px;border:none;border-radius:4px;cursor:pointer;font-size:14px}
    .btn-p{background:#e85514;color:white}.btn-p:hover{background:#c74410}
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
    .tab-btn.active{background:#e85514;color:white}
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
    <h1>auペイマーケット売上集計</h1>
    <a href="/">\\u2190 \\u30dd\\u30fc\\u30bf\\u30eb\\u306b\\u623b\\u308b</a>
  </div>
  <div class="wrap">
    <!-- 工程1: 注文データCSV -->
    <div class="card">
      <h2>工程1: 注文データCSVアップロード</h2>
      <p class="meta">auペイマーケット管理画面 → 受注管理 → CSVダウンロード（発送日・完了・会計用注文データDL項目）</p>
      <p class="meta" style="color:#e85514;font-weight:bold">複数ファイル選択可（最大10ファイル）</p>
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
        <h2>税率別売上集計（税込）</h2>
        <div id="taxTable"></div>
      </div>

      <!-- MF連携用 -->
      <div class="card">
        <h2>MF連携用 税込み集計</h2>
        <div id="mfTable"></div>
      </div>

      <!-- PF手数料 -->
      <div class="card">
        <h2>工程2: PF手数料・広告費</h2>
        <p class="meta">口座振替通知書PDFをアップロードすると自動入力されます。手入力も可。</p>
        <div style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <input type="file" id="pdfFile" accept=".pdf">
          <button class="btn btn-w" id="pdfBtn" onclick="doPdfUpload()">PDF読み取り</button>
        </div>
        <div id="pdfResult" style="margin-top:8px"></div>
        <div style="margin-top:12px;display:flex;gap:16px;align-items:center;flex-wrap:wrap">
          <label>PF手数料（税込）: <input type="text" id="pfFeeInput" class="pf-input" value="0"></label>
          <label>広告費（税込）: <input type="text" id="adCostInput" class="pf-input" value="0"></label>
          <button class="btn btn-p" onclick="applyPfFee()">反映</button>
        </div>
        <div id="costSummary" style="margin-top:8px"></div>
        <div id="costBySegment" style="margin-top:12px"></div>
      </div>

      <!-- セグメント別集計 -->
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
          <button class="btn btn-s" onclick="downloadDetailCsv()">注文明細CSV</button>
        </div>
      </div>

      <!-- 代表商品コードで紐付けた商品 -->
      <div id="repCodeCard" class="card" style="display:none">
        <h2>代表商品コードで紐付けた商品</h2>
        <p class="meta">auペイ側の商品コードがマスタに直接存在せず、代表商品コード経由で紐付けました。auペイ管理画面の商品コード登録ミスの可能性があります。</p>
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
          throw new Error('サーバーに接続できません（' + e.message + '）。ページを再読み込みしてから再度お試しください。');
        }
      }
    }

    // ─── 工程1: CSVアップロード ───
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
      if (!hasUnresolved) summaryHtml += '<br><b style="color:#27ae60">全商品解決済み — 確定可能</b>';
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
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>CSV税率</th><th>出現数</th><th>売上合計</th></tr>';
          for (const u of data.unresolvedProducts) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + (u.csvTaxRate || '-') + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
          }
          contentHtml += '</table></div>';
        }

        if (hasUnresTax) {
          showRegisterBtn = true;
          contentHtml += '<div id="unres-tax" class="tab-content' + (!hasUnresolved?' active':'') + '">';
          contentHtml += '<div class="warn">税率未登録の商品です。下のプルダウンで税率を選択し「登録して再集計」できます。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>CSV税率</th><th>出現数</th><th>売上合計</th><th>税率登録</th></tr>';
          for (const u of data.unresolvedTax) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + (u.csvTaxRate || '-') + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td>';
            contentHtml += '<td><select class="reg-sel tax-reg" data-code="' + u.code + '"><option value="">-</option><option value="10"' + (u.csvTaxRate===10?' selected':'') + '>10%</option><option value="8"' + (u.csvTaxRate===8?' selected':'') + '>8%</option></select></td></tr>';
          }
          contentHtml += '</table></div>';
        }

        if (hasUnresSeg) {
          showRegisterBtn = true;
          contentHtml += '<div id="unres-seg" class="tab-content' + (!hasUnresolved && !hasUnresTax?' active':'') + '">';
          contentHtml += '<div class="warn">セグメント未登録の商品です。下のプルダウンでセグメントを選択し「登録して再集計」できます。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>原価</th><th>出現数</th><th>売上合計</th><th>セグメント登録</th></tr>';
          for (const u of data.unresolvedSegment) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + fmt(u.genka) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td>';
            contentHtml += '<td><select class="reg-sel seg-reg" data-code="' + u.code + '"><option value="">-</option><option value="1">1:自社商品</option><option value="2">2:取引先限定</option><option value="3">3:仕入れ商品</option><option value="4">4:輸出</option></select></td></tr>';
          }
          contentHtml += '</table></div>';
        }

        document.getElementById('unresolvedContent').innerHTML = contentHtml;
        document.getElementById('registerBtn').style.display = showRegisterBtn ? 'inline-block' : 'none';
      } else {
        document.getElementById('unresolvedArea').style.display = 'none';
      }

      // 税率別集計
      renderTaxTable(data);
      // MF連携用
      renderMfTable(data);
      // セグメント別集計
      renderSegmentTable(data);

      // 代表商品コードで紐付けた商品
      if (data.resolvedByRepCode && data.resolvedByRepCode.length > 0) {
        document.getElementById('repCodeCard').style.display = 'block';
        let html = '<div class="warn" style="margin-bottom:8px"><b>' + data.resolvedByRepCode.length + '商品</b>を代表商品コード経由で紐付けました</div>';
        html += '<table class="detail-table"><tr><th>auペイ商品コード</th><th>auペイ商品名</th><th>紐付先マスタコード</th><th>マスタ商品名</th><th>出現数</th><th>売上合計</th></tr>';
        for (const r of data.resolvedByRepCode) {
          html += '<tr><td style="text-align:left">' + r.auPayCode + '</td><td style="text-align:left">' + (r.name || '').slice(0, 40) + '</td><td style="text-align:left;color:#e85514;font-weight:bold">' + r.matchedCode + '</td><td style="text-align:left">' + (r.matchedName || '').slice(0, 40) + '</td><td class="num">' + r.count + '</td><td class="num">' + fmt(r.amount) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('repCodeList').innerHTML = html;
      } else {
        document.getElementById('repCodeCard').style.display = 'none';
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

    function renderTaxTable(data) {
      const bt = data.byTax;
      let html = '<table><tr><th>税率</th><th>売上合計</th><th>クーポン値引額</th><th>クーポン値引後売上</th><th>原価合計</th><th>行数</th></tr>';
      let tot = { s:0, c:0, a:0, g:0, n:0 };
      for (const [key, row] of Object.entries(bt)) {
        const label = key + '%';
        html += '<tr><td>' + label + '</td>';
        html += '<td class="num">' + fmt(row.売上合計) + '</td>';
        html += '<td class="num">' + fmt(row.クーポン値引額) + '</td>';
        html += '<td class="num">' + fmt(row.クーポン値引後売上) + '</td>';
        html += '<td class="num">' + fmt(row.原価合計) + '</td>';
        html += '<td class="num">' + row.行数 + '</td></tr>';
        tot.s += row.売上合計; tot.c += row.クーポン値引額; tot.a += row.クーポン値引後売上; tot.g += row.原価合計; tot.n += row.行数;
      }
      html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      html += '<td class="num">' + fmt(tot.s) + '</td><td class="num">' + fmt(tot.c) + '</td>';
      html += '<td class="num">' + fmt(tot.a) + '</td><td class="num">' + fmt(tot.g) + '</td>';
      html += '<td class="num">' + tot.n + '</td></tr></table>';
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
      const segNames = { 1: '自社商品', 2: '取引先限定', 3: '仕入れ商品' };
      const pf = pfFeeData.pfFee || 0;
      const ad = pfFeeData.adCost || 0;

      const allocTargets = ['1', '2', '3'];
      const adTargets = ['1', '2'];
      const salesByKey = {};
      for (const [key, row] of Object.entries(seg)) { salesByKey[key] = row.クーポン値引後売上 || 0; }
      const pfByKey = allocateByRatio(pf, salesByKey, allocTargets);
      const adByKey = allocateByRatio(ad, salesByKey, adTargets);

      let html = '<table><tr><th>セグメント</th><th>売上合計</th><th>クーポン値引額</th><th>値引後売上</th><th>PF手数料</th><th>広告費</th><th>原価合計</th><th>原価率</th><th>行数</th></tr>';
      let tot = { s:0, c:0, a:0, g:0, n:0 };
      let totPf = 0, totAd = 0;
      for (const [key, row] of Object.entries(seg)) {
        const label = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
        html += '<tr><td>' + key + ': ' + label + '</td>';
        html += '<td class="num">' + fmt(row.売上合計) + '</td>';
        html += '<td class="num">' + fmt(row.クーポン値引額) + '</td>';
        html += '<td class="num">' + fmt(row.クーポン値引後売上) + '</td>';
        html += '<td class="num">' + fmt(pfByKey[key] || 0) + '</td>';
        html += '<td class="num">' + fmt(adByKey[key] || 0) + '</td>';
        html += '<td class="num">' + fmt(row.原価合計) + '</td>';
        html += '<td class="num">' + (row.原価率 || '0.0') + '%</td>';
        html += '<td class="num">' + row.行数 + '</td></tr>';
        tot.s += row.売上合計; tot.c += row.クーポン値引額; tot.a += row.クーポン値引後売上; tot.g += row.原価合計; tot.n += row.行数;
        totPf += (pfByKey[key] || 0); totAd += (adByKey[key] || 0);
      }
      const totGross = tot.a > 0 ? ((tot.a - tot.g) / tot.a * 100).toFixed(1) : '0.0';
      html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      html += '<td class="num">' + fmt(tot.s) + '</td><td class="num">' + fmt(tot.c) + '</td>';
      html += '<td class="num">' + fmt(tot.a) + '</td>';
      html += '<td class="num">' + fmt(totPf) + '</td><td class="num">' + fmt(totAd) + '</td>';
      html += '<td class="num">' + fmt(tot.g) + '</td>';
      html += '<td class="num">' + totGross + '%</td>';
      html += '<td class="num">' + tot.n + '</td></tr></table>';
      document.getElementById('segmentTable').innerHTML = html;

      // 除外セグメント
      const excl = data.excluded || {};
      let exclHtml = '';
      for (const [key, row] of Object.entries(excl)) {
        if (row.行数 > 0) {
          exclHtml += '<div class="excluded"><b>除外: ' + key + ': 輸出</b>（' + row.行数 + '件） — 売上: ' + fmt(row.売上合計) + ' / 原価: ' + fmt(row.原価合計) + '</div>';
        }
      }
      document.getElementById('excludedInfo').innerHTML = exclHtml;

      // 変動費サマリー
      if (pf || ad) {
        let csHtml = '<table><tr><th>PF手数料</th><th>広告費</th><th>合計</th></tr>';
        csHtml += '<tr><td class="num" style="font-weight:bold">' + fmt(pf) + '</td>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(ad) + '</td>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(pf + ad) + '</td></tr></table>';
        document.getElementById('costSummary').innerHTML = csHtml;

        let cbHtml = '<table><tr><th>セグメント</th><th>売上比率</th><th>PF手数料</th><th>広告費</th><th>変動費合計</th></tr>';
        for (const [key, row] of Object.entries(seg)) {
          const label = segNames[key] || (key === 'other' ? 'その他' : key);
          const ratio = tot.a > 0 ? (row.クーポン値引後売上 / tot.a * 100).toFixed(1) : '0.0';
          cbHtml += '<tr><td>' + key + ': ' + label + '</td>';
          cbHtml += '<td class="num">' + ratio + '%</td>';
          cbHtml += '<td class="num">' + fmt(pfByKey[key] || 0) + '</td>';
          cbHtml += '<td class="num">' + fmt(adByKey[key] || 0) + '</td>';
          cbHtml += '<td class="num" style="font-weight:bold">' + fmt((pfByKey[key]||0) + (adByKey[key]||0)) + '</td></tr>';
        }
        cbHtml += '</table>';
        document.getElementById('costBySegment').innerHTML = cbHtml;
      } else {
        document.getElementById('costSummary').innerHTML = '';
        document.getElementById('costBySegment').innerHTML = '';
      }
    }

    function switchTab(el, tabId) {
      el.parentElement.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      el.classList.add('active');
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      document.getElementById(tabId).classList.add('active');
    }

    // ─── 税率・セグメント登録 ───
    async function doRegister() {
      const items = [];
      document.querySelectorAll('.tax-reg').forEach(el => {
        if (el.value) items.push({ code: el.dataset.code, taxRate: parseInt(el.value), segment: null });
      });
      document.querySelectorAll('.seg-reg').forEach(el => {
        if (el.value) {
          const existing = items.find(i => i.code === el.dataset.code);
          if (existing) existing.segment = parseInt(el.value);
          else items.push({ code: el.dataset.code, taxRate: null, segment: parseInt(el.value) });
        }
      });
      if (!items.length) { alert('登録する項目を選択してください'); return; }

      const btn = document.getElementById('registerBtn');
      btn.disabled = true;
      btn.textContent = '登録中...';
      try {
        const r = await fetchWithRetry(location.pathname + '/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ items }),
        });
        const result = await r.json();
        if (result.ok) {
          alert('登録完了: 税率 ' + result.updatedTax + '件 / セグメント ' + result.updatedSeg + '件\\n\\n再集計を実行します。');
          doUpload();
        } else {
          alert('登録エラー: ' + (result.error || ''));
        }
      } catch(e) {
        alert('登録エラー: ' + e.message);
      }
      btn.disabled = false;
      btn.textContent = '選択した税率・セグメントを登録して再集計';
    }

    // ─── PDF読み取り ───
    async function doPdfUpload() {
      const fileInput = document.getElementById('pdfFile');
      if (!fileInput.files.length) { alert('PDFファイルを選択してください'); return; }
      const btn = document.getElementById('pdfBtn');
      btn.disabled = true;
      btn.textContent = '読み取り中...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);

      try {
        const r = await fetchWithRetry(location.pathname + '/upload-pdf', { method: 'POST', body: formData });
        if (!r.ok) {
          const text = await r.text();
          try { const j = JSON.parse(text); document.getElementById('pdfResult').innerHTML = '<div class="err">' + (j.error || r.status) + '</div>'; }
          catch { document.getElementById('pdfResult').innerHTML = '<div class="err">サーバーエラー (HTTP ' + r.status + ')</div>'; }
          return;
        }
        const data = await r.json();
        if (data.error) { document.getElementById('pdfResult').innerHTML = '<div class="err">' + data.error + '</div>'; return; }

        if (data.totalFee != null) {
          document.getElementById('pfFeeInput').value = data.totalFee;
          document.getElementById('adCostInput').value = data.adCost || 0;
          let html = '<div class="ok">PDF読み取り成功: ご請求額 \\u00a5' + data.totalFee.toLocaleString();
          if (data.pdfYearMonth) html += '（' + data.pdfYearMonth + '）';
          html += '</div>';
          if (data.breakdown && Object.keys(data.breakdown).length > 0) {
            html += '<table style="margin-top:4px"><tr><th>項目</th><th>金額</th></tr>';
            for (const [k, v] of Object.entries(data.breakdown)) {
              html += '<tr><td>' + k + '</td><td class="num">' + fmt(v) + '</td></tr>';
            }
            html += '</table>';
          }
          document.getElementById('pdfResult').innerHTML = html;
          applyPfFee();
        } else {
          document.getElementById('pdfResult').innerHTML = '<div class="warn">PDF読み取りできましたが、ご請求額を検出できませんでした。手入力してください。</div>';
        }
      } catch(e) {
        document.getElementById('pdfResult').innerHTML = '<div class="err">エラー: ' + e.message + '</div>';
      }
      btn.disabled = false;
      btn.textContent = 'PDF読み取り';
    }

    function applyPfFee() {
      const pf = parseInt(document.getElementById('pfFeeInput').value.replace(/,/g, '')) || 0;
      const ad = parseInt(document.getElementById('adCostInput').value.replace(/,/g, '')) || 0;
      pfFeeData = { pfFee: pf, adCost: ad };
      if (lastData) renderSegmentTable(lastData);
      updateConfirmState();
    }

    function updateConfirmState() {
      const ready = !!lastData;
      document.getElementById('confirmBtn').disabled = !ready;
      const pre = document.getElementById('confirmPreCheck');
      if (ready) {
        pre.className = 'ok';
        pre.innerHTML = '対象年月: <b>' + (lastData.yearMonth || '不明') + '</b>（' + lastData.totalRows + '行）'
          + ' / PF手数料: <b>\\u00a5' + (pfFeeData.pfFee || 0).toLocaleString() + '</b>'
          + (pfFeeData.adCost ? ' / 広告費: <b>\\u00a5' + pfFeeData.adCost.toLocaleString() + '</b>' : '');
      } else {
        pre.className = 'warn';
        pre.innerHTML = '注文データCSVをアップロードしてから確定してください';
      }
    }

    // ─── 確定 ───
    async function doConfirm() {
      if (!lastData) { alert('先に注文データCSVをアップロードしてください'); return; }
      const ym = lastData.yearMonth || '';
      if (!ym) { alert('対象年月が検出できません'); return; }
      if (!confirm(ym + ' の集計を確定しますか？')) return;

      const btn = document.getElementById('confirmBtn');
      btn.disabled = true;
      btn.textContent = '保存中...';
      try {
        const r = await fetchWithRetry(location.pathname + '/confirm', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            yearMonth: ym,
            totalRows: lastData.totalRows,
            resolvedCount: lastData.resolvedCount,
            unresolvedCount: lastData.unresolvedProducts?.length || 0,
            byTax: lastData.byTax,
            bySegment: lastData.bySegment,
            excluded: lastData.excluded,
            mfRow: lastData.mfRow,
            pfFee: pfFeeData.pfFee || 0,
            adCost: pfFeeData.adCost || 0,
          }),
        });
        const result = await r.json();
        if (result.ok) {
          document.getElementById('confirmStatus').innerHTML = '<span style="color:#27ae60">OK ' + ym + ' 確定済（' + result.confirmed_at + '）</span>';
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
        const r = await fetchWithRetry(location.pathname + '/history', {});
        const rows = await r.json();
        if (!rows.length) {
          document.getElementById('historyList').innerHTML = '<span class="meta">確定データはまだありません</span>';
          return;
        }
        const segNames = {1:'自社商品', 2:'取引先限定', 3:'仕入れ商品'};
        let html = '';
        for (let i = 0; i < rows.length; i++) {
          const row = rows[i];
          const seg = row.by_segment || {};
          let hdrSales = 0;
          for (const sr of Object.values(seg)) hdrSales += (sr.クーポン値引後売上 || 0);
          const pf = Math.round(row.pf_fee || 0);
          const ad = Math.round(row.ad_cost || 0);

          html += '<div class="acc-header" onclick="toggleAcc(this)" data-idx="' + i + '">';
          html += '<span><b>' + row.year_month + '</b> — 売上: \\u00a5' + Math.round(hdrSales).toLocaleString()
            + ' / PF: \\u00a5' + pf.toLocaleString()
            + (ad ? ' / 広告: \\u00a5' + ad.toLocaleString() : '')
            + ' <span class="meta">（' + (row.confirmed_at || '') + '）</span></span>';
          html += '<span class="arrow">&#9654;</span></div>';
          html += '<div class="acc-body" id="acc-' + i + '">';

          // セグメント別
          html += '<table><tr><th>セグメント</th><th>売上合計</th><th>クーポン値引額</th><th>値引後売上</th><th>原価合計</th><th>原価率</th><th>行数</th></tr>';
          let sTot = { s:0, c:0, a:0, g:0, n:0 };
          for (const [key, sr] of Object.entries(seg)) {
            const lb = segNames[key] || (key === 'other' ? 'その他' : key);
            html += '<tr><td>' + key + ': ' + lb + '</td>';
            html += '<td class="num">' + fmt(sr.売上合計||0) + '</td>';
            html += '<td class="num">' + fmt(sr.クーポン値引額||0) + '</td>';
            html += '<td class="num">' + fmt(sr.クーポン値引後売上||0) + '</td>';
            html += '<td class="num">' + fmt(sr.原価合計||0) + '</td>';
            html += '<td class="num">' + (sr.原価率 || '0.0') + '%</td>';
            html += '<td class="num">' + (sr.行数||0) + '</td></tr>';
            sTot.s += (sr.売上合計||0); sTot.c += (sr.クーポン値引額||0); sTot.a += (sr.クーポン値引後売上||0); sTot.g += (sr.原価合計||0); sTot.n += (sr.行数||0);
          }
          const tg = sTot.a > 0 ? ((sTot.a - sTot.g) / sTot.a * 100).toFixed(1) : '0.0';
          html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
          html += '<td class="num">' + fmt(sTot.s) + '</td><td class="num">' + fmt(sTot.c) + '</td>';
          html += '<td class="num">' + fmt(sTot.a) + '</td><td class="num">' + fmt(sTot.g) + '</td>';
          html += '<td class="num">' + tg + '%</td><td class="num">' + sTot.n + '</td></tr></table>';

          // 除外
          const excl = row.excluded || {};
          for (const [ek, er] of Object.entries(excl)) {
            if ((er.行数||0) > 0) html += '<div class="excluded"><b>除外: ' + ek + ': 輸出</b>（' + er.行数 + '行） — 売上: ' + fmt(er.売上合計||0) + '</div>';
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

    // ─── 集計サマリーCSV ───
    function downloadSummaryCsv() {
      if (!lastData) { alert('先に注文データCSVをアップロードしてください'); return; }
      const data = lastData;
      let csv = '\\uFEFF';

      csv += '【税率別集計（税込）】\\n';
      csv += '税率,売上合計,クーポン値引額,クーポン値引後売上,原価合計,行数\\n';
      const bt = data.byTax;
      let tTot = { s:0, c:0, a:0, g:0, n:0 };
      for (const [key, row] of Object.entries(bt)) {
        csv += key + '%,' + Math.round(row.売上合計) + ',' + Math.round(row.クーポン値引額) + ',' + Math.round(row.クーポン値引後売上) + ',' + Math.round(row.原価合計) + ',' + row.行数 + '\\n';
        tTot.s += row.売上合計; tTot.c += row.クーポン値引額; tTot.a += row.クーポン値引後売上; tTot.g += row.原価合計; tTot.n += row.行数;
      }
      csv += '合計,' + Math.round(tTot.s) + ',' + Math.round(tTot.c) + ',' + Math.round(tTot.a) + ',' + Math.round(tTot.g) + ',' + tTot.n + '\\n';

      csv += '\\n【MF連携用 税込み集計】\\n';
      csv += '商品売上(10%),商品売上(8%),合計\\n';
      const mf = data.mfRow;
      csv += (mf['商品売上(10%)']||0) + ',' + (mf['商品売上(8%)']||0) + ',' + (mf['合計']||0) + '\\n';

      csv += '\\n【セグメント別集計】\\n';
      csv += 'セグメント,売上合計,クーポン値引額,値引後売上,原価合計,原価率,行数\\n';
      const seg = data.bySegment;
      const segNames = { 1:'自社商品', 2:'取引先限定', 3:'仕入れ商品' };
      for (const [key, row] of Object.entries(seg)) {
        const label = segNames[key] || (key === 'other' ? 'その他' : key);
        csv += key + ':' + label + ',' + Math.round(row.売上合計) + ',' + Math.round(row.クーポン値引額) + ',' + Math.round(row.クーポン値引後売上) + ',' + Math.round(row.原価合計) + ',' + (row.原価率||'0.0') + ',' + row.行数 + '\\n';
      }

      csv += '\\n【変動費】\\n';
      csv += 'PF手数料,広告費\\n';
      csv += (pfFeeData.pfFee||0) + ',' + (pfFeeData.adCost||0) + '\\n';

      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'auPay_summary_' + (data.yearMonth || 'unknown') + '.csv';
      a.click();
    }

    // ─── 注文明細CSV ───
    function downloadDetailCsv() {
      if (!lastData) { alert('先に注文データCSVをアップロードしてください'); return; }
      const data = lastData;
      const rows = data.resolvedRows || [];
      if (!rows.length) { alert('明細データがありません'); return; }

      const segNames = { 1:'自社商品', 2:'取引先限定', 3:'仕入れ商品', 4:'輸出' };
      let csv = '\\uFEFF';
      csv += '注文番号,商品コード,商品コード(マスタ),商品名,単価,個数,売上合計(税込),クーポン値引額,税率,売上分類,原価(単価),原価(小計),解決方法\\n';
      for (const r of rows) {
        const taxRate = r.税率 || r.CSV税率 || '';
        const segLabel = r.売上分類 ? (r.売上分類 + ':' + (segNames[r.売上分類] || '')) : '';
        const genkaUnit = r.原価 || 0;
        const genkaTotal = genkaUnit * (r.個数 || 1);
        const cols = [
          r.注文番号 || '',
          r.商品コード || '',
          r.商品コード_resolved || '',
          '"' + (r.商品名 || '').replace(/"/g, '""') + '"',
          r.単価 || 0,
          r.個数 || 0,
          Math.round(r.売上合計 || 0),
          Math.round(r.クーポン値引額 || 0),
          taxRate ? taxRate + '%' : '',
          segLabel,
          genkaUnit,
          Math.round(genkaTotal),
          r.解決方法 || '',
        ];
        csv += cols.join(',') + '\\n';
      }

      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'auPay_detail_' + (data.yearMonth || 'unknown') + '.csv';
      a.click();
    }

    async function downloadHistoryCsv() {
      try {
        const r = await fetchWithRetry(location.pathname + '/history', {});
        const rows = await r.json();
        if (!rows.length) { alert('確定データがありません'); return; }

        const segNames = {1:'自社商品', 2:'取引先限定', 3:'仕入れ商品', other:'その他'};
        let csv = '\\uFEFF';
        csv += '集計月,セグメント,売上合計,クーポン値引額,値引後売上,PF手数料,広告費,原価合計,原価率\\n';

        function toLastDay(ym) {
          const [y, m] = ym.split('-').map(Number);
          const last = new Date(y, m, 0).getDate();
          return ym + '-' + String(last).padStart(2, '0');
        }

        for (const row of rows) {
          const seg = row.by_segment || {};
          const pf = row.pf_fee || 0;
          const ad = row.ad_cost || 0;
          const allocTargets = ['1','2','3'];
          const adTargets = ['1','2'];
          const salesByKey = {};
          for (const [k, sr] of Object.entries(seg)) salesByKey[k] = sr.クーポン値引後売上 || 0;
          const pfByKey = allocateByRatio(pf, salesByKey, allocTargets);
          const adByKey = allocateByRatio(ad, salesByKey, adTargets);

          const ymDate = toLastDay(row.year_month);
          for (const [key, sr] of Object.entries(seg)) {
            const label = segNames[key] || key;
            csv += ymDate + ',' + key + ':' + label + ',' + Math.round(sr.売上合計||0) + ',' + Math.round(sr.クーポン値引額||0) + ',' + Math.round(sr.クーポン値引後売上||0) + ',' + (pfByKey[key]||0) + ',' + (adByKey[key]||0) + ',' + Math.round(sr.原価合計||0) + ',' + (sr.原価率||'0.0') + '\\n';
          }
        }

        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'auPay_segment_history.csv';
        a.click();
      } catch(e) {
        alert('ダウンロードエラー: ' + e.message);
      }
    }

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
