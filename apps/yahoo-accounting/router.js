/**
 * Yahoo!売上集計ツール
 *
 * NE_Items_ProからダウンロードしたYahoo注文データCSV（Shift_JIS）を複数アップロードし、
 * mirror_products を使って税率別・セグメント別の売上集計を自動計算する。
 * 請求明細CSVの取り込みにも対応。
 *
 * 工程1: 注文データCSVアップロード（複数対応、Shift_JIS→UTF-8変換）
 * 工程2: 未登録商品の税率・セグメント登録
 * 工程3: 税率別・セグメント別集計
 * 工程4: 請求明細CSV取り込み
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
  for (const p of db.prepare('SELECT * FROM mirror_products').all()) {
    productsMap.set((p.商品コード || '').toLowerCase(), p);
  }

  const resolved = [];
  const unresolved = new Map();
  const unresolvedTax = new Map();
  const unresolvedSegment = new Map();

  for (const row of rows) {
    const subCode = (row.サブコード || '').toLowerCase();
    const itemId = (row.商品コード || '').toLowerCase();
    const lookupCode = subCode || itemId;

    if (!lookupCode) {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 解決方法: 'no_code' });
      continue;
    }

    // オークション自動採番コード判定（AUCTIONで始まる = 毎回変わるので登録不要）
    const isAuction = lookupCode.startsWith('auction');

    // Stage 1: SubCodeで検索、なければ Stage 2: ItemIdで検索
    let product = subCode ? productsMap.get(subCode) : null;
    let resolveMethod = subCode ? 'subcode' : null;

    if (!product) {
      product = productsMap.get(itemId);
      resolveMethod = 'itemid';
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
    } else if (isAuction) {
      // オークション自動採番: CSV税率を使用、セグメント=1（自社商品）、原価=1000
      resolved.push({
        ...row,
        商品コード_resolved: lookupCode,
        原価: 1000,
        税率: row.CSV税率 || 10,
        売上分類: 1,
        解決方法: 'auction_auto',
      });
    } else {
      resolved.push({
        ...row,
        商品コード_resolved: null,
        原価: 0, 税率: null, 売上分類: null,
        解決方法: 'unresolved',
      });
      const key = subCode || itemId;
      const existing = unresolved.get(key) || {
        code: key, name: row.商品名 || '',
        csvTaxRate: row.CSV税率 || null, count: 0, amount: 0,
      };
      existing.count++;
      existing.amount += row.売上合計 || 0;
      unresolved.set(key, existing);
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
  };
}

// ─── 集計 ───

function aggregate(resolvedRows) {
  const columns = ['売上合計', 'クーポン値引額', 'クーポン値引後売上'];

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

  // MF連携用: 税込み集計
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
    byTax, bySegment, excluded,
    otherDetails: [...otherDetails.values()].sort((a, b) => Math.abs(b.売上合計) - Math.abs(a.売上合計)),
    columns, mfRow, mfColumns,
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

      // ヘッダー確認（NE_Items_Pro: 先頭列=OrderId）
      const header = csvRows[0];
      if (header[0] !== 'OrderId' && !header[0].includes('OrderId')) {
        return res.status(400).json({ error: file.originalname + ' はNE_Items_Pro形式ではありません（先頭列: ' + header[0] + '）' });
      }

      const num = v => { const n = parseFloat((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };

      for (let i = 1; i < csvRows.length; i++) {
        const cols = csvRows[i];
        if (cols.length < 20) continue;

        const orderId = cols[0] || '';
        if (!orderId) continue;

        const neId = cols[2] || ''; // C列: Id（b-faith01-XXXXX形式、受取明細の注文IDと紐付け用）
        const itemId = cols[4] || '';
        const subCode = cols[5] || '';
        const quantity = parseInt(cols[3]) || 0;
        const unitPrice = num(cols[15]);
        const lineSubTotal = num(cols[18]) || unitPrice * quantity;
        const couponDiscount = num(cols[24]);
        const originalPrice = num(cols[25]);
        const leadTimeStart = cols.length > 33 ? (cols[33] || '') : '';
        const itemTaxRatio = cols.length > 36 ? (parseInt(cols[36]) || null) : null;
        const title = cols[8] || '';

        // 売上合計: 元価格ベース（クーポン適用前）。元価格がない場合は単価ベース
        const saleAmount = (originalPrice > 0 ? originalPrice : unitPrice) * quantity;
        // クーポン値引額: 1単位あたりクーポン × 数量
        const couponAmount = couponDiscount * quantity;

        allRows.push({
          注文番号: orderId,
          注文ID: neId,
          商品コード: itemId,
          サブコード: subCode,
          商品名: title,
          単価: unitPrice,
          個数: quantity,
          売上合計: saleAmount,
          クーポン値引額: couponAmount,
          日付: leadTimeStart,
          CSV税率: itemTaxRatio,
        });
      }
    }

    if (allRows.length === 0) {
      return res.status(400).json({ error: 'CSVからデータを読み取れませんでした' });
    }

    // 対象年月を推定（LeadTimeStartから）
    let yearMonth = '';
    for (const row of allRows) {
      if (row.日付) {
        yearMonth = row.日付.slice(0, 7).replace(/\//g, '-');
        break;
      }
    }

    // 商品コード解決
    const { resolved, unresolved, unresolvedTax, unresolvedSegment, zeroGenka } = resolveProducts(allRows, db);

    // 注文IDベースのルックアップマップ（受取明細との紐付け用）
    // キー: C列のId（b-faith01-XXXXX形式）= 受取明細の注文ID
    // 1注文に複数明細がある場合は全行分の情報を保持
    const orderMap = {};
    for (const r of resolved) {
      if (r.解決方法 === 'no_code' || r.解決方法 === 'unresolved') continue;
      const oid = r.注文ID; // C列のId
      if (!oid) continue;
      if (!orderMap[oid]) orderMap[oid] = [];
      orderMap[oid].push({
        商品コード: r.商品コード_resolved || r.商品コード || '',
        商品名: r.商品名 || '',
        税率: r.税率 || r.CSV税率 || 10,
        売上分類: r.売上分類 || null,
        原価: r.原価 || 0,
        個数: r.個数 || 0,
        単価: r.単価 || 0,
        売上合計: r.売上合計 || 0,
      });
    }

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
      orderMap,
    });
  } catch (e) {
    console.error('[YahooAccounting] エラー:', e.message, e.stack);
    res.status(500).json({ error: '集計処理エラー: ' + e.message });
  }
});

// ─── POST /upload-billing — 請求明細CSVアップロード ───

router.post('/upload-billing', upload.array('files', 10), (req, res) => {
  if (!req.files || req.files.length === 0) return res.status(400).json({ error: 'ファイルが必要です' });

  try {
    const num = v => { const n = parseFloat((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };
    const allBillingRows = [];

    for (const file of req.files) {
      const buf = fs.readFileSync(file.path);
      fs.unlinkSync(file.path);

      const csvRows = parseShiftJisCsv(buf);
      if (csvRows.length < 2) continue;

      // 請求明細CSV: 利用日, 注文ID, 利用項目, 備考, 金額（税抜き）, 消費税, 金額（税込）
      for (let i = 1; i < csvRows.length; i++) {
        const cols = csvRows[i];
        if (cols.length < 7) continue;

        allBillingRows.push({
          利用日: cols[0] || '',
          注文ID: cols[1] || '',
          利用項目: cols[2] || '',
          備考: cols[3] || '',
          '金額(税抜)': num(cols[4]),
          消費税: num(cols[5]),
          '金額(税込)': num(cols[6]),
        });
      }
    }

    if (allBillingRows.length === 0) {
      return res.status(400).json({ error: 'CSVからデータを読み取れませんでした' });
    }

    // 利用項目別集計
    const byCategory = {};
    let totalTaxExcl = 0, totalTax = 0, totalTaxIncl = 0;

    for (const row of allBillingRows) {
      const cat = row.利用項目 || '(空)';
      if (!byCategory[cat]) byCategory[cat] = { 品目: cat, '金額(税抜)': 0, 消費税: 0, '金額(税込)': 0, 件数: 0 };
      byCategory[cat]['金額(税抜)'] += row['金額(税抜)'];
      byCategory[cat].消費税 += row.消費税;
      byCategory[cat]['金額(税込)'] += row['金額(税込)'];
      byCategory[cat].件数++;
      totalTaxExcl += row['金額(税抜)'];
      totalTax += row.消費税;
      totalTaxIncl += row['金額(税込)'];
    }

    // 広告費とPF手数料の分離
    let adCost = 0;
    for (const cat of Object.values(byCategory)) {
      if (cat.品目.includes('広告') || cat.品目.includes('アフィリエイト')) {
        adCost += cat['金額(税込)'];
      }
    }
    const pfFee = totalTaxIncl - adCost;

    // 利用日から年月推定
    let billingYearMonth = '';
    if (allBillingRows.length > 0 && allBillingRows[0].利用日) {
      billingYearMonth = allBillingRows[0].利用日.slice(0, 7).replace(/\//g, '-');
    }

    res.json({
      totalRows: allBillingRows.length,
      fileCount: req.files.length,
      byCategory: Object.values(byCategory),
      totalTaxExcl, totalTax, totalTaxIncl,
      adCost, pfFee,
      billingYearMonth,
    });
  } catch (e) {
    console.error('[YahooAccounting] 請求明細エラー:', e.message, e.stack);
    res.status(500).json({ error: '請求明細取込エラー: ' + e.message });
  }
});

// ─── POST /upload-receipt — 受取明細CSVアップロード ───

router.post('/upload-receipt', upload.array('files', 10), (req, res) => {
  if (!req.files || req.files.length === 0) return res.status(400).json({ error: 'ファイルが必要です' });

  try {
    const num = v => { const n = parseFloat((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };
    const allReceiptRows = [];

    for (const file of req.files) {
      const buf = fs.readFileSync(file.path);
      fs.unlinkSync(file.path);

      const csvRows = parseShiftJisCsv(buf);
      if (csvRows.length < 2) continue;

      // 受取明細CSV: 利用日, 注文ID, 利用項目, 備考, 金額（税抜き）, 消費税, 金額（税込）
      for (let i = 1; i < csvRows.length; i++) {
        const cols = csvRows[i];
        if (cols.length < 7) continue;

        allReceiptRows.push({
          利用日: cols[0] || '',
          注文ID: cols[1] || '',
          利用項目: cols[2] || '',
          備考: cols[3] || '',
          '金額(税抜)': num(cols[4]),
          消費税: num(cols[5]),
          '金額(税込)': num(cols[6]),
        });
      }
    }

    if (allReceiptRows.length === 0) {
      return res.status(400).json({ error: 'CSVからデータを読み取れませんでした' });
    }

    // 利用日から年月推定
    let receiptYearMonth = '';
    if (allReceiptRows[0]?.利用日) {
      receiptYearMonth = allReceiptRows[0].利用日.slice(0, 7).replace(/\//g, '-');
    }

    res.json({
      totalRows: allReceiptRows.length,
      fileCount: req.files.length,
      rows: allReceiptRows,
      receiptYearMonth,
    });
  } catch (e) {
    console.error('[YahooAccounting] 受取明細エラー:', e.message, e.stack);
    res.status(500).json({ error: '受取明細取込エラー: ' + e.message });
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
    byTax, bySegment, excluded, mfRow, adCost, billing } = req.body;

  if (!yearMonth) return res.status(400).json({ error: 'yearMonth は必須です' });

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    db.prepare(`INSERT OR REPLACE INTO mart_yahoo_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, ad_cost, billing, confirmed_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      yearMonth, totalRows || 0, resolvedCount || 0, unresolvedCount || 0,
      JSON.stringify(byTax), JSON.stringify(bySegment), JSON.stringify(excluded),
      JSON.stringify(mfRow), adCost || 0, JSON.stringify(billing || {}), now
    );

    db.prepare(`INSERT INTO mart_yahoo_upload_log
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
    const rows = db.prepare('SELECT * FROM mart_yahoo_monthly_summary ORDER BY year_month DESC').all();
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
    const row = db.prepare('SELECT * FROM mart_yahoo_monthly_summary WHERE year_month = ?').get(req.params.yearMonth);
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
  <title>Yahoo!売上集計 - B-Faith</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#333;font-size:14px}
    .header{background:#6001d2;color:white;padding:12px 24px;display:flex;align-items:center;gap:16px}
    .header h1{font-size:18px}
    .header a{color:#d4b3ff;text-decoration:none;font-size:13px}
    .wrap{max-width:1800px;margin:16px auto;padding:0 16px}
    .card{background:white;border-radius:8px;padding:20px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1);overflow-x:auto}
    .card h2{font-size:15px;color:#555;margin-bottom:10px}
    .btn{padding:8px 20px;border:none;border-radius:4px;cursor:pointer;font-size:14px}
    .btn-p{background:#6001d2;color:white}.btn-p:hover{background:#4a00a8}
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
    .tab-btn.active{background:#6001d2;color:white}
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
  </style>
</head>
<body>
  <div class="header">
    <h1>Yahoo!売上集計</h1>
    <a href="/">\\u2190 \\u30dd\\u30fc\\u30bf\\u30eb\\u306b\\u623b\\u308b</a>
  </div>
  <div class="wrap">
    <!-- 工程1: 受取明細CSV -->
    <div class="card">
      <h2>工程1: 受取明細CSV取り込み</h2>
      <p class="meta">Store Creator Pro → 受取明細タブ → CSV（Shift_JIS）— 複数ファイル対応</p>
      <div style="margin-top:8px;display:flex;gap:8px;align-items:center">
        <input type="file" id="receiptFiles" accept=".csv" multiple>
        <button class="btn btn-p" id="receiptBtn" onclick="doReceiptUpload()">受取明細取り込み</button>
      </div>
      <div id="receiptResult" style="margin-top:8px"></div>
    </div>

    <!-- 工程2: 請求明細CSV -->
    <div class="card">
      <h2>工程2: 請求明細CSV取り込み</h2>
      <p class="meta">Store Creator Pro → 請求明細タブ → CSV（Shift_JIS）— 複数ファイル対応</p>
      <div style="margin-top:8px;display:flex;gap:8px;align-items:center">
        <input type="file" id="billingFiles" accept=".csv" multiple>
        <button class="btn btn-w" id="billingBtn" onclick="doBillingUpload()">請求明細取り込み</button>
      </div>
      <div id="billingResult" style="margin-top:8px"></div>
    </div>

    <!-- 工程3: NE_Items_Pro CSV（注文データ） -->
    <div class="card">
      <h2>工程3: 注文データCSVアップロード（NE_Items_Pro）</h2>
      <p class="meta">NE_Items_Pro CSV（Shift_JIS）— 受取明細と紐付けて税率別・セグメント別集計を実行</p>
      <p class="meta" style="color:#6001d2;font-weight:bold">複数ファイル選択可（最大10ファイル）</p>
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

      <!-- 税率別集計（受取明細ベース） -->
      <div class="card" id="receiptTaxCard" style="display:none">
        <h2>税率別売上集計（受取明細ベース）</h2>
        <div id="receiptTaxTable"></div>
      </div>

      <!-- MF連携用 -->
      <div class="card" id="mfCard" style="display:none">
        <h2>MF連携用 税込み集計</h2>
        <div id="mfTable"></div>
      </div>

      <!-- セグメント別集計 -->
      <div class="card">
        <h2>セグメント別集計（管理会計用）</h2>
        <div id="segmentTable"></div>
        <div id="excludedInfo"></div>
      </div>

      <!-- 変動費サマリー -->
      <div class="card">
        <h2>変動費サマリー</h2>
        <div id="costSummary"><span class="meta">請求明細CSVを取り込むと表示されます</span></div>
        <div id="costBySegment" style="margin-top:12px"></div>
      </div>

      <!-- CSVダウンロード -->
      <div class="card" id="csvDownloadArea" style="display:none">
        <h2>CSVダウンロード</h2>
        <div style="display:flex;gap:12px;flex-wrap:wrap">
          <button class="btn btn-s" onclick="downloadDetailCsv()">明細データCSV（税率・セグメント付き）</button>
          <button class="btn btn-w" onclick="downloadSummaryCsv()">集計サマリーCSV</button>
        </div>
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

    <!-- 確定 -->
    <div class="card" id="confirmCard">
      <h2>確定</h2>
      <div id="confirmPreCheck" class="warn" style="margin-bottom:8px">受取明細CSV、請求明細CSV、NE_Items_Pro CSVの3つをアップロードしてから確定してください</div>
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
    let receiptData = null;
    let billingData = null;
    const fmt = n => {
      if (n === 0) return '0';
      const s = Math.round(n).toLocaleString();
      return n < 0 ? '<span class="negative">' + s + '</span>' : s;
    };

    // ─── リトライ付きfetch ───
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
      document.getElementById('csvDownloadArea').style.display = (receiptData && billingData) ? 'block' : 'none';

      const hasUnresolved = data.unresolvedProducts.length > 0;
      const hasUnresTax = data.unresolvedTax && data.unresolvedTax.length > 0;
      const hasUnresSeg = data.unresolvedSegment && data.unresolvedSegment.length > 0;
      const canConfirm = !hasUnresolved;

      let summaryHtml = '<div class="' + (canConfirm ? 'ok' : 'warn') + '">';
      summaryHtml += '<b>対象年月: ' + (data.yearMonth || '不明') + '</b> （' + data.fileCount + 'ファイル）<br>';
      summaryHtml += '総行数: ' + data.totalRows + ' / 解決済: ' + data.resolvedCount + ' / 未登録商品: ' + data.unresolvedProducts.length + '件';
      if (hasUnresTax) summaryHtml += ' / <span class="negative">税率未登録: ' + data.unresolvedTax.length + '件（CSV税率 or 10%仮扱い）</span>';
      if (hasUnresSeg) summaryHtml += ' / <span class="negative">セグメント未登録: ' + data.unresolvedSegment.length + '件</span>';
      if (canConfirm) summaryHtml += '<br><b style="color:#27ae60">全商品解決済み \\u2014 確定可能</b>';
      else summaryHtml += '<br><b style="color:#e74c3c">未登録商品あり \\u2014 warehouse側で登録後に再アップロード</b>';
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

        // 未登録商品
        if (hasUnresolved) {
          contentHtml += '<div id="unres-product" class="tab-content active">';
          contentHtml += '<div class="warn">商品マスタに未登録の商品です。warehouse側で登録してから再アップロードしてください。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>CSV税率</th><th>出現数</th><th>売上合計</th></tr>';
          for (const u of data.unresolvedProducts) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + (u.csvTaxRate || '-') + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
          }
          contentHtml += '</table></div>';
        }

        // 税率未登録
        if (hasUnresTax) {
          showRegisterBtn = true;
          contentHtml += '<div id="unres-tax" class="tab-content' + (!hasUnresolved?' active':'') + '">';
          contentHtml += '<div class="warn">税率未登録の商品です。下のプルダウンで税率を選択し「登録して再集計」できます。CSV内の税率がある場合はフォールバックとして使用されます。</div>';
          contentHtml += '<table><tr><th>商品コード</th><th>商品名</th><th>CSV税率</th><th>出現数</th><th>売上合計</th><th>税率登録</th></tr>';
          for (const u of data.unresolvedTax) {
            contentHtml += '<tr><td>' + u.code + '</td><td>' + (u.name || '').slice(0, 60) + '</td><td class="num">' + (u.csvTaxRate || '-') + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td>';
            contentHtml += '<td><select class="reg-sel tax-reg" data-code="' + u.code + '"><option value="">-</option><option value="10"' + (u.csvTaxRate===10?' selected':'') + '>10%</option><option value="8"' + (u.csvTaxRate===8?' selected':'') + '>8%</option></select></td></tr>';
          }
          contentHtml += '</table></div>';
        }

        // セグメント未登録
        if (hasUnresSeg) {
          showRegisterBtn = true;
          contentHtml += '<div id="unres-seg" class="tab-content' + (!hasUnresolved && !hasUnresTax?' active':'') + '">';
          contentHtml += '<div class="warn">セグメント未登録の商品です。下のプルダウンでセグメントを選択し「登録して再集計」できます。</div>';
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

      // 受取明細が既に取り込まれていれば、紐付けて集計を表示
      if (receiptData) {
        renderReceiptBasedAggregation();
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

    function getBillingTotals() {
      if (!billingData) return null;
      return {
        totalBilling: billingData.totalTaxIncl || 0,
        adCost: billingData.adCost || 0,
        pfFee: billingData.pfFee || 0,
      };
    }

    function updateConfirmState() {
      const ready = lastData && receiptData && billingData;
      document.getElementById('confirmBtn').disabled = !ready;
      const pre = document.getElementById('confirmPreCheck');
      if (ready) {
        const bt = getBillingTotals();
        pre.className = 'ok';
        pre.innerHTML = 'NE_Items_Pro: <b>' + (lastData.yearMonth || '不明') + '</b>（' + lastData.totalRows + '行）'
          + ' / 受取明細: <b>' + receiptData.totalRows + '行</b>'
          + ' / 請求明細: <b>' + billingData.totalRows + '行</b>'
          + ' / PF手数料: <b>\\u00a5' + Math.round(bt.pfFee).toLocaleString() + '</b>'
          + (bt.adCost ? ' / 広告費: <b>\\u00a5' + Math.round(bt.adCost).toLocaleString() + '</b>' : '');
      } else {
        pre.className = 'warn';
        let missing = [];
        if (!receiptData) missing.push('受取明細CSV');
        if (!billingData) missing.push('請求明細CSV');
        if (!lastData) missing.push('NE_Items_Pro CSV');
        pre.innerHTML = missing.join('、') + 'をアップロードしてから確定してください';
      }
    }

    // 按分計算ヘルパー
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

    function renderSegmentTableFromReceipt(bySegment, excludedSeg) {
      const segNames = { 1: '自社商品', 2: '取扱限定', 3: '仕入れ商品' };
      const bt = getBillingTotals();
      const ad = bt ? bt.adCost : 0;
      const pf = bt ? bt.pfFee : 0;

      const allocTargets = ['1', '2', '3'];
      const adTargets = ['1', '2'];
      const salesByKey = {};
      for (const [key, row] of Object.entries(bySegment)) { salesByKey[key] = row.売上 || 0; }
      const adByKey = allocateByRatio(ad, salesByKey, adTargets);
      const pfByKey = allocateByRatio(pf, salesByKey, allocTargets);

      let html = '<table><tr><th>セグメント</th><th>売上（税込）</th><th>PF手数料</th><th>広告費</th><th>原価合計</th><th>粗利率</th><th>件数</th></tr>';
      let tot = { 売上: 0, 原価: 0, 件数: 0 };
      let totAd = 0, totPf = 0;
      for (const [key, row] of Object.entries(bySegment)) {
        const label = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
        const gross = row.売上 > 0 ? ((row.売上 - Math.round(row.原価)) / row.売上 * 100).toFixed(1) : '0.0';
        html += '<tr><td>' + key + ': ' + label + '</td>';
        html += '<td class="num">' + fmt(row.売上) + '</td>';
        html += '<td class="num">' + fmt(pfByKey[key] || 0) + '</td>';
        html += '<td class="num">' + fmt(adByKey[key] || 0) + '</td>';
        html += '<td class="num">' + fmt(row.原価) + '</td>';
        html += '<td class="num">' + gross + '%</td>';
        html += '<td class="num">' + row.件数 + '</td></tr>';
        tot.売上 += row.売上; tot.原価 += row.原価; tot.件数 += row.件数;
        totAd += (adByKey[key] || 0); totPf += (pfByKey[key] || 0);
      }
      const totGross = tot.売上 > 0 ? ((tot.売上 - Math.round(tot.原価)) / tot.売上 * 100).toFixed(1) : '0.0';
      html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      html += '<td class="num">' + fmt(tot.売上) + '</td>';
      html += '<td class="num">' + fmt(totPf) + '</td>';
      html += '<td class="num">' + fmt(totAd) + '</td>';
      html += '<td class="num">' + fmt(tot.原価) + '</td>';
      html += '<td class="num">' + totGross + '%</td>';
      html += '<td class="num">' + tot.件数 + '</td></tr></table>';
      document.getElementById('segmentTable').innerHTML = html;

      // 除外セグメント
      let exclHtml = '';
      for (const [key, row] of Object.entries(excludedSeg)) {
        if (row.件数 > 0) {
          exclHtml += '<div class="excluded"><b>除外: ' + key + ': 輸出</b>（' + row.件数 + '件） \\u2014 売上: ' + fmt(row.売上) + ' / 原価: ' + fmt(row.原価) + '</div>';
        }
      }
      document.getElementById('excludedInfo').innerHTML = exclHtml;

      // 変動費サマリー
      if (bt) {
        let csHtml = '<table><tr><th>PF手数料</th><th>広告費</th><th>合計</th></tr>';
        csHtml += '<tr>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(bt.pfFee) + '</td>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(bt.adCost) + '</td>';
        csHtml += '<td class="num" style="font-weight:bold">' + fmt(bt.totalBilling) + '</td>';
        csHtml += '</tr></table>';
        csHtml += '<p class="meta" style="margin-top:4px">PF手数料 = 請求合計(' + fmt(bt.totalBilling) + ') \\u2212 広告費(' + fmt(bt.adCost) + ')</p>';
        document.getElementById('costSummary').innerHTML = csHtml;

        // セグメント別変動費按分
        let cbHtml = '<table><tr><th>セグメント</th><th>売上比率</th><th>PF手数料</th><th>広告費</th><th>変動費合計</th></tr>';
        let cbTotPf = 0, cbTotAd = 0;
        for (const [key, row] of Object.entries(bySegment)) {
          const label = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
          const ratio = tot.売上 > 0 ? (row.売上 / tot.売上 * 100).toFixed(1) : '0.0';
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
        document.getElementById('costSummary').innerHTML = '<span class="meta">請求明細CSVを取り込むと表示されます</span>';
        document.getElementById('costBySegment').innerHTML = '';
      }
    }

    function switchTab(el, tabId) {
      el.parentElement.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      el.classList.add('active');
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      document.getElementById(tabId).classList.add('active');
    }

    // ─── 工程2: 税率・セグメント登録 ───
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
          doUpload(); // 自動再集計
        } else {
          alert('登録エラー: ' + (result.error || ''));
        }
      } catch(e) {
        alert('登録エラー: ' + e.message);
      }
      btn.disabled = false;
      btn.textContent = '選択した税率・セグメントを登録して再集計';
    }

    // ─── 工程1: 受取明細取り込み ───
    async function doReceiptUpload() {
      const fileInput = document.getElementById('receiptFiles');
      if (!fileInput.files.length) { alert('ファイルを選択してください'); return; }
      const btn = document.getElementById('receiptBtn');
      btn.disabled = true;
      btn.textContent = '処理中...';

      const formData = new FormData();
      for (const f of fileInput.files) formData.append('files', f);

      try {
        const r = await fetchWithRetry(location.pathname + '/upload-receipt', { method: 'POST', body: formData });
        if (!r.ok) {
          const text = await r.text();
          try { const j = JSON.parse(text); document.getElementById('receiptResult').innerHTML = '<div class="err">' + (j.error || r.status) + '</div>'; }
          catch { document.getElementById('receiptResult').innerHTML = '<div class="err">サーバーエラー (HTTP ' + r.status + ')</div>'; }
          return;
        }
        const data = await r.json();
        if (data.error) { document.getElementById('receiptResult').innerHTML = '<div class="err">' + data.error + '</div>'; return; }
        receiptData = data;

        let html = '<div class="ok">受取明細取り込み完了: ' + data.totalRows + '行（' + data.fileCount + 'ファイル）</div>';
        html += '<p class="meta">工程3のNE_Items_Pro取り込み後に、注文IDで紐付けて税率別集計を表示します</p>';
        document.getElementById('receiptResult').innerHTML = html;
        updateConfirmState();
      } catch(e) {
        document.getElementById('receiptResult').innerHTML = '<div class="err">エラー: ' + e.message + '</div>';
      }
      btn.disabled = false;
      btn.textContent = '受取明細取り込み';
    }

    // ─── 受取明細ベースの全集計（NE_Items_Pro取込後に呼ばれる）───
    function renderReceiptBasedAggregation() {
      if (!receiptData || !lastData || !lastData.orderMap) return;
      const om = lastData.orderMap;

      // 受取明細の入金行をフィルタし、注文IDでNE_Items_Proと紐付けて集計
      const byTax = { '10': { 売上: 0, 件数: 0 }, '8': { 売上: 0, 件数: 0 }, 'unknown': { 売上: 0, 件数: 0 } };
      const bySegment = { '1': { 売上: 0, 原価: 0, 件数: 0 }, '2': { 売上: 0, 原価: 0, 件数: 0 }, '3': { 売上: 0, 原価: 0, 件数: 0 }, 'other': { 売上: 0, 原価: 0, 件数: 0 } };
      const excluded = { '4': { 売上: 0, 原価: 0, 件数: 0 } };
      let unmatchedOrders = 0;

      for (const row of receiptData.rows) {
        const amount = row['金額(税込)'] || 0;
        if (amount === 0) continue;
        const item = row.利用項目 || '';
        // 入金系のみ（決済金額）。手数料等は除外
        if (item.includes('手数料') || item.includes('原資') || item.includes('利用料')
            || item.includes('報酬') || item.includes('プラン')) continue;

        const orderId = row.注文ID || '';
        const orderItems = om[orderId];

        if (orderItems && orderItems.length > 0) {
          // 注文内の最初の商品の税率で判定（1注文=同一税率が基本）
          const taxRate = orderItems[0].税率;
          const taxKey = taxRate === 8 ? '8' : '10';
          byTax[taxKey].売上 += amount;
          byTax[taxKey].件数++;

          // セグメント: 注文内商品の売上按分でセグメント別に振り分け
          const totalOrderSales = orderItems.reduce((s, i) => s + (i.売上合計 || 0), 0);
          for (const oi of orderItems) {
            const ratio = totalOrderSales > 0 ? (oi.売上合計 || 0) / totalOrderSales : 1 / orderItems.length;
            const segKey = oi.売上分類 ? String(oi.売上分類) : 'other';
            const segAmount = amount * ratio;
            const genka = (oi.原価 || 0) * (oi.個数 || 1) * ratio;
            if (excluded[segKey]) {
              excluded[segKey].売上 += segAmount;
              excluded[segKey].原価 += genka;
              excluded[segKey].件数++;
            } else {
              const target = bySegment[segKey] || bySegment['other'];
              target.売上 += segAmount;
              target.原価 += genka;
              target.件数++;
            }
          }
        } else {
          // NE_Items_Proに該当注文なし → 不明
          byTax['unknown'].売上 += amount;
          byTax['unknown'].件数++;
          bySegment['other'].売上 += amount;
          bySegment['other'].件数++;
          unmatchedOrders++;
        }
      }

      // 集計結果を保存（確定時に使う）
      lastData.receiptByTax = byTax;
      lastData.receiptBySegment = bySegment;
      lastData.receiptExcluded = excluded;

      // ─── 税率別集計表示 ───
      document.getElementById('receiptTaxCard').style.display = 'block';
      let taxHtml = '<table><tr><th>税率</th><th>金額（税込）</th><th>件数</th></tr>';
      taxHtml += '<tr><td>10%</td><td class="num">' + fmt(byTax['10'].売上) + '</td><td class="num">' + byTax['10'].件数 + '</td></tr>';
      taxHtml += '<tr><td>8%</td><td class="num">' + fmt(byTax['8'].売上) + '</td><td class="num">' + byTax['8'].件数 + '</td></tr>';
      if (byTax['unknown'].件数 > 0) {
        taxHtml += '<tr><td>不明（10%仮扱い）</td><td class="num">' + fmt(byTax['unknown'].売上) + '</td><td class="num">' + byTax['unknown'].件数 + '</td></tr>';
      }
      const totalSales = byTax['10'].売上 + byTax['8'].売上 + byTax['unknown'].売上;
      const totalCount = byTax['10'].件数 + byTax['8'].件数 + byTax['unknown'].件数;
      taxHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td><td class="num">' + fmt(totalSales) + '</td><td class="num">' + totalCount + '</td></tr>';
      taxHtml += '</table>';
      if (unmatchedOrders > 0) {
        taxHtml += '<p class="meta" style="color:#e67e22">注文データに該当がない受取明細: ' + unmatchedOrders + '件（「その他」に分類）</p>';
      }
      document.getElementById('receiptTaxTable').innerHTML = taxHtml;

      // ─── MF連携用 ───
      const mf10 = byTax['10'].売上 + byTax['unknown'].売上;
      const mf8 = byTax['8'].売上;
      document.getElementById('mfCard').style.display = 'block';
      let mfHtml = '<table><tr><th>商品売上(10%税込)</th><th>商品売上(8%税込)</th><th>合計</th></tr>';
      mfHtml += '<tr><td class="num" style="font-weight:bold">' + fmt(mf10) + '</td>';
      mfHtml += '<td class="num" style="font-weight:bold">' + fmt(mf8) + '</td>';
      mfHtml += '<td class="num" style="font-weight:bold">' + fmt(mf10 + mf8) + '</td></tr></table>';
      document.getElementById('mfTable').innerHTML = mfHtml;

      // ─── セグメント別集計 ───
      renderSegmentTableFromReceipt(bySegment, excluded);

      // CSVダウンロードも表示
      document.getElementById('csvDownloadArea').style.display = 'block';
    }

    // ─── 工程2: 請求明細取り込み ───
    async function doBillingUpload() {
      const fileInput = document.getElementById('billingFiles');
      if (!fileInput.files.length) { alert('ファイルを選択してください'); return; }
      const btn = document.getElementById('billingBtn');
      btn.disabled = true;
      btn.textContent = '処理中...';

      const formData = new FormData();
      for (const f of fileInput.files) formData.append('files', f);

      try {
        const r = await fetchWithRetry(location.pathname + '/upload-billing', { method: 'POST', body: formData });
        if (!r.ok) {
          const text = await r.text();
          try { const j = JSON.parse(text); document.getElementById('billingResult').innerHTML = '<div class="err">' + (j.error || r.status) + '</div>'; }
          catch { document.getElementById('billingResult').innerHTML = '<div class="err">サーバーエラー (HTTP ' + r.status + ')</div>'; }
          return;
        }
        const data = await r.json();
        if (data.error) { document.getElementById('billingResult').innerHTML = '<div class="err">' + data.error + '</div>'; return; }
        billingData = data;

        let html = '<div class="ok">請求明細取り込み完了: ' + data.totalRows + '行（' + data.fileCount + 'ファイル）</div>';
        html += '<table><tr><th>利用項目</th><th>金額（税抜）</th><th>消費税</th><th>金額（税込）</th><th>件数</th></tr>';
        for (const cat of data.byCategory) {
          html += '<tr><td>' + cat.品目 + '</td><td class="num">' + fmt(cat['金額(税抜)']) + '</td><td class="num">' + fmt(cat.消費税) + '</td><td class="num">' + fmt(cat['金額(税込)']) + '</td><td class="num">' + cat.件数 + '</td></tr>';
        }
        html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
        html += '<td class="num">' + fmt(data.totalTaxExcl) + '</td>';
        html += '<td class="num">' + fmt(data.totalTax) + '</td>';
        html += '<td class="num">' + fmt(data.totalTaxIncl) + '</td>';
        html += '<td></td></tr></table>';
        html += '<p class="meta" style="margin-top:4px">PF手数料: ' + fmt(data.pfFee) + ' / 広告費: ' + fmt(data.adCost) + '</p>';
        document.getElementById('billingResult').innerHTML = html;

        updateConfirmState();
        if (lastData) renderSegmentTable(lastData);
      } catch(e) {
        document.getElementById('billingResult').innerHTML = '<div class="err">エラー: ' + e.message + '</div>';
      }
      btn.disabled = false;
      btn.textContent = '請求明細取り込み';
    }

    // ─── 確定 ───
    async function doConfirm() {
      if (!lastData) { alert('先にNE_Items_Pro CSVをアップロードしてください'); return; }
      if (!receiptData) { alert('先に受取明細CSVをアップロードしてください'); return; }
      if (!billingData) { alert('先に請求明細CSVをアップロードしてください'); return; }

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
            byTax: lastData.receiptByTax || {},
            bySegment: lastData.receiptBySegment || {},
            excluded: lastData.receiptExcluded || {},
            mfRow: {},
            adCost: billingData.adCost || 0,
            billing: billingData.byCategory || null,
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
        let html = '';
        for (let i = 0; i < rows.length; i++) {
          const row = rows[i];
          const seg = row.by_segment || {};
          let hdrSales = 0, hdrAfter = 0;
          for (const sr of Object.values(seg)) { hdrSales += (sr.売上合計 || 0); hdrAfter += (sr.クーポン値引後売上 || 0); }
          const ad = Math.round(row.ad_cost || 0);

          html += '<div class="acc-header" onclick="toggleAcc(this)" data-idx="' + i + '">';
          html += '<span><b>' + row.year_month + '</b> \\u2014 売上合計: \\u00a5' + Math.round(hdrSales).toLocaleString()
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
              html += '<div class="excluded"><b>除外: ' + ek + ': 輸出</b>（' + er.行数 + '行） \\u2014 売上: ' + fmt(er.売上合計||0) + '</div>';
            }
          }

          // 請求明細サマリー
          const bill = row.billing;
          if (bill && Array.isArray(bill) && bill.length > 0) {
            html += '<h3 style="font-size:13px;color:#555;margin:12px 0 4px">請求明細</h3>';
            html += '<table><tr><th>利用項目</th><th>金額（税抜）</th><th>消費税</th><th>金額（税込）</th></tr>';
            for (const cat of bill) {
              html += '<tr><td>' + cat.品目 + '</td><td class="num">' + fmt(cat['金額(税抜)']||0) + '</td><td class="num">' + fmt(cat.消費税||0) + '</td><td class="num">' + fmt(cat['金額(税込)']||0) + '</td></tr>';
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

    // ─── 明細データCSVダウンロード ───
    function downloadDetailCsv() {
      if (!lastData || !lastData.detailRows) { alert('先に注文データCSVをアップロードしてください'); return; }
      const rows = lastData.detailRows;
      let csv = '\\uFEFF';
      csv += '注文番号,商品コード,商品名,単価,個数,売上合計,クーポン値引額,クーポン値引後売上,原価,原価合計,税率,セグメント\\n';
      for (const r of rows) {
        const name = (r.商品名 || '').replace(/"/g, '""');
        csv += r.注文番号 + ',' + r.商品コード + ',"' + name + '",' + r.単価 + ',' + r.個数 + ',' + r.売上合計 + ',' + r.クーポン値引額 + ',' + r.クーポン値引後売上 + ',' + r.原価 + ',' + r.原価合計 + ',' + r.税率 + ',' + r.セグメント + '\\n';
      }
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'Yahoo_detail_' + (lastData.yearMonth || 'unknown') + '.csv';
      a.click();
    }

    // ─── 集計サマリーCSVダウンロード ───
    function downloadSummaryCsv() {
      if (!lastData) { alert('先に注文データCSVをアップロードしてください'); return; }
      const data = lastData;
      let csv = '\\uFEFF';

      csv += '【税率別集計】\\n';
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
      csv += 'セグメント,売上合計,クーポン値引額,クーポン値引後売上,原価合計,原価率,行数\\n';
      const seg = data.bySegment;
      const segNames = data.segmentNames || {};
      let sTot = { s:0, c:0, a:0, g:0, n:0 };
      for (const [key, row] of Object.entries(seg)) {
        const label = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
        const s = row.売上合計||0, g = row.原価合計||0;
        const genkaRate = s > 0 ? (g/s*100).toFixed(1) : '0.0';
        csv += key + ':' + label + ',' + Math.round(s) + ',' + Math.round(row.クーポン値引額||0) + ',' + Math.round(row.クーポン値引後売上||0) + ',' + Math.round(g) + ',' + genkaRate + ',' + row.行数 + '\\n';
        sTot.s += s; sTot.c += (row.クーポン値引額||0); sTot.a += (row.クーポン値引後売上||0); sTot.g += g; sTot.n += row.行数;
      }
      const totRate = sTot.s > 0 ? (sTot.g/sTot.s*100).toFixed(1) : '0.0';
      csv += '合計,' + Math.round(sTot.s) + ',' + Math.round(sTot.c) + ',' + Math.round(sTot.a) + ',' + Math.round(sTot.g) + ',' + totRate + ',' + sTot.n + '\\n';

      const bt2 = getBillingTotals();
      if (bt2) {
        csv += '\\n【変動費サマリー】\\n';
        csv += 'PF手数料,広告費,合計\\n';
        csv += Math.round(bt2.pfFee) + ',' + Math.round(bt2.adCost) + ',' + Math.round(bt2.totalBilling) + '\\n';
      }

      if (billingData && billingData.byCategory) {
        csv += '\\n【請求明細】\\n';
        csv += '利用項目,金額（税抜）,消費税,金額（税込）\\n';
        for (const cat of billingData.byCategory) {
          csv += cat.品目 + ',' + Math.round(cat['金額(税抜)']) + ',' + Math.round(cat.消費税) + ',' + Math.round(cat['金額(税込)']) + '\\n';
        }
      }

      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'Yahoo_summary_' + (data.yearMonth || 'unknown') + '.csv';
      a.click();
    }

    async function downloadHistoryCsv() {
      try {
        const r = await fetchWithRetry(location.pathname + '/history', {});
        const rows = await r.json();
        if (!rows.length) { alert('確定データがありません'); return; }

        const segNames = {1:'自社商品', 2:'取扱限定', 3:'仕入れ商品', other:'その他/未分類'};
        const adTargets = ['1','2'];
        let csv = '\\uFEFF';
        csv += '集計月,セグメント,売上合計,クーポン値引額,クーポン値引後売上,広告費,原価合計,原価率\\n';

        function toLastDay(ym) {
          const [y, m] = ym.split('-').map(Number);
          const last = new Date(y, m, 0).getDate();
          return ym + '-' + String(last).padStart(2, '0');
        }

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

          const ymDate = toLastDay(row.year_month);
          for (const [key, sr] of Object.entries(seg)) {
            const label = segNames[key] || key;
            const s = sr.売上合計 || 0;
            const g = sr.原価合計 || 0;
            const genkaRate = s > 0 ? (g / s * 100).toFixed(1) : '0.0';
            csv += ymDate + ',' + key + ':' + label + ',' + Math.round(s) + ',' + Math.round(sr.クーポン値引額||0) + ',' + Math.round(sr.クーポン値引後売上||0) + ',' + (adMap[key]||0) + ',' + Math.round(g) + ',' + genkaRate + '\\n';
          }
        }

        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'Yahoo_segment_history.csv';
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
