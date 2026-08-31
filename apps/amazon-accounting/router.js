/**
 * Amazon売上集計ツール
 *
 * セラセンのペイメントレポートCSVをアップロードし、
 * mirror_products + mirror_sku_resolved (master only) を使って
 * 税率別・セグメント別の売上集計を自動計算する。
 *
 * Phase 1: CSVアップロード → SKU照合 → 未登録検出 → 集計プレビュー
 */
import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { requireImportKey, importJsonParser } from '../../lib/import-key-auth.js';
import { normalizeYearMonth } from '../../lib/jst-date.js';
import { FEE_COLUMNS } from './fee-breakdown.js';
import { parsePaymentCsvText, aggregate } from './payment-csv.js';

const router = Router();
const UPLOAD_DIR = process.env.DATA_DIR ? process.env.DATA_DIR + '/import' : 'data/import';
if (!fs.existsSync(UPLOAD_DIR)) { try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch {} }
const upload = multer({ dest: UPLOAD_DIR });

// セグメント名称マップ（1〜3が集計対象）
const SEGMENT_NAMES = { 1: '自社商品', 2: '取引先限定', 3: '仕入れ商品' };

// エビデンスCSV一時保存（yearMonth → { detail, summary }）
const evidenceStore = new Map();
// 除外セグメント（4=輸出はセグメント集計に含めない）
const EXCLUDED_SEGMENTS = { 4: '輸出' };

// ─── CSV出力のセル整形 ───
// 外部入力 (CSVの説明・SKU・DB文字列) を安全に出力する。カンマ/引用符/改行 (CR含む) は引用し、
// 先頭が = + - @ の値はスプレッドシートで数式として評価されないようアポストロフィを付ける (先頭の空白・制御文字は読み飛ばして判定)
function csvCell(v) {
  let s = String(v == null ? '' : v);
  if (/^[\s\u0000-\u001f]*[=+\-@]/.test(s)) s = "'" + s;
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

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

// ─── SKU解決（3段階 + セット展開）───

function resolveSkus(rows, db) {
  // mirror_productsの商品コードマップ（小文字統一）
  const productsMap = new Map();
  for (const p of db.prepare('SELECT * FROM mirror_products').all()) {
    productsMap.set((p.商品コード || '').toLowerCase(), p);
  }

  const skuMapEntries = new Map();
  const skuRows = db.prepare('SELECT seller_sku, ne_code, quantity AS 数量 FROM mirror_sku_resolved').all();
  for (const s of skuRows) {
    const key = s.seller_sku?.toLowerCase();
    if (!key) continue;
    if (!skuMapEntries.has(key)) skuMapEntries.set(key, []);
    skuMapEntries.get(key).push(s);
  }

  const resolved = [];
  const unresolved = new Map(); // SKU → { sku, name, count, amount }
  const conflicts = []; // セット商品の解決失敗(混在/欠損)を hard fail で記録

  for (const row of rows) {
    const sku = (row.sku || '').toLowerCase();
    const txType = row.トランザクション種類 || '';

    // 振込みは集計対象外だがSKU解決は不要
    if (txType === '振込み') {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 商品コード: null, 解決方法: 'skip' });
      continue;
    }

    // 注文外料金（月額登録料/広告費の返金/納品不備手数料 等）は商品売上ではなく
    // SKUがあってもAmazon内部管理SKU(X-prefix)なのでマスタ照合せず「その他」へ
    if (txType === '注文外料金') {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 商品コード: null, 解決方法: 'no_sku' });
      continue;
    }

    if (!sku) {
      resolved.push({ ...row, 原価: 0, 税率: null, 売上分類: null, 商品コード: null, 解決方法: 'no_sku' });
      continue;
    }

    let product = null;
    let resolveMethod = null;

    // Stage 1: mirror_sku_resolved を優先 (セット情報が正しい構成数を持つため)
    // mirror_products に同一SKUの単品レコードが残存していても、ここで先にセット解決する
    const mappings = skuMapEntries.get(sku);
    if (mappings && mappings.length > 0) {
      // 構成品の数量を検証 (NULL→1扱い、0/負数/非整数→invalid_quantity で hard fail)
      const components = mappings.map(m => {
        const rawQty = m.数量;
        const validQty = (rawQty == null) ? 1
          : (Number.isInteger(rawQty) && rawQty > 0) ? rawQty : null;
        return {
          ne_code: m.ne_code,
          qty: validQty,
          rawQty,
          product: productsMap.get((m.ne_code || '').toLowerCase()),
        };
      });

      // invalid_quantity: 構成品の数量が 0/負数/非数値 (セット/単品問わず)
      const invalidQty = components.filter(c => c.qty === null);
      if (invalidQty.length > 0) {
        resolved.push({ ...row, 商品コード: null, 原価: 0, 税率: null, 売上分類: null, 解決方法: 'invalid_quantity' });
        conflicts.push({ sku, type: 'invalid_quantity', invalidQty: invalidQty.map(c => ({ ne_code: c.ne_code, rawQty: c.rawQty })) });
        continue;
      }

      // セット判定: 構成品 >1個 OR 単一構成品でも qty != 1 (例: 1 SKU = 1 ne_code × 3個 の3個セット)
      const isSet = components.length > 1 || components.some(c => c.qty !== 1);

      if (isSet) {
        // partial_component: 構成品の一部が mirror_products に存在しない
        const missing = components.filter(c => !c.product).map(c => c.ne_code);
        if (missing.length > 0) {
          resolved.push({ ...row, 商品コード: null, 原価: 0, 税率: null, 売上分類: null, 解決方法: 'partial_component' });
          conflicts.push({ sku, type: 'partial_component', missing });
          continue;
        }

        // セット合算: 原価は構成品(原価×数量)の合計
        const totalGenka = components.reduce((sum, c) => sum + (c.product.原価 || 0) * (c.qty || 1), 0);

        // 税率: 構成品の MIN(税率) を採用 (8%軽減税率優先、業務ルール)
        //   8% (軽減) と 10% (標準) 混在時は軽減税率優先(食品的扱い)
        const taxRatesArr = components.map(c => c.product.消費税率).filter(t => t != null);
        const taxRate = taxRatesArr.length > 0
          ? Math.round(Math.min(...taxRatesArr) * 100) // 0.08→8, 0.10→10
          : null;

        // 売上分類: 構成品の MIN(売上分類) を採用 (階層論理、業務ルール)
        //   1=自社優先 > 2=取引先限定 > 3=仕入れ
        //   1 を含む→1, 含まず2 含む→2, 3のみ→3
        //   理由: 自社商品(1)を含むセットは「自社商品セット」と認識される業務慣習
        const segmentsArr = components.map(c => c.product.売上分類).filter(s => s != null);
        const segmentValue = segmentsArr.length > 0 ? Math.min(...segmentsArr) : null;

        resolved.push({
          ...row,
          商品コード: components[0].product.商品コード,
          原価: totalGenka,
          税率: taxRate,
          売上分類: segmentValue,
          解決方法: 'set_components',
          components: components.map(c => ({ ne_code: c.ne_code, qty: c.qty })),
        });
        continue; // セット処理完了
      }

      // 単品 (length === 1 && qty === 1): ne_code 経由で解決
      // ne_code lookup 失敗時は mapped_target_missing で hard fail
      // (mirror_products direct fallback は意図的に行わない:
      //  「sku_map あるのに ne_code 壊れている」状態を direct で握りつぶすと
      //   Round 11 で潰した「mirror_products 残存単品優先」を復活させてしまうため)
      if (components[0].product) {
        product = components[0].product;
        resolveMethod = 'sku_map';
      } else {
        resolved.push({ ...row, 商品コード: null, 原価: 0, 税率: null, 売上分類: null, 解決方法: 'mapped_target_missing' });
        conflicts.push({ sku, type: 'mapped_target_missing', missing: [components[0].ne_code] });
        continue;
      }
    }

    // Stage 2: mirror_products で直一致 (mappings 自体が無い場合のみ)
    if (!product) {
      product = productsMap.get(sku);
      if (product) resolveMethod = 'direct';
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
    } else if (txType === '調整') {
      // 調整（FBA在庫の補償=紛失/破損の弁償金 等）は商品売上ではない。
      // 弁償行にはAmazonが独自採番したSKU(例: 820283975_b00fglltgw)が付くが、
      // これは商品コードではなくマスタ照合できないため「その他」へ流す（確定はブロックしない）。
      // ※商品連動の調整で実SKUを持つものは Stage 1/2 で解決済みのためここには来ない。
      resolved.push({
        ...row,
        商品コード: null,
        原価: 0,
        税率: null,
        売上分類: null,
        解決方法: 'adjustment_no_master',
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
  // 税率未登録の商品を検出
  const unresolvedTax = new Map();
  for (const row of resolved) {
    if (row.解決方法 === 'skip' || row.解決方法 === 'no_sku' || row.解決方法 === 'adjustment_no_master') continue;
    if (row.商品コード && (row.原価 === 0 || row.原価 === null)) {
      const key = row.商品コード;
      const existing = zeroGenka.get(key) || { 商品コード: key, sku: row.sku || '', 商品名: row.説明 || '', 数量合計: 0, 売上合計: 0, count: 0 };
      existing.数量合計 += row.数量 || 0;
      existing.売上合計 += row.商品売上 || 0;
      existing.count++;
      zeroGenka.set(key, existing);
    }
    if (row.商品コード && row.税率 === null) {
      const key = row.商品コード;
      const existing = unresolvedTax.get(key) || { 商品コード: key, sku: row.sku || '', 商品名: row.説明 || '', 数量合計: 0, 売上合計: 0, count: 0 };
      existing.数量合計 += row.数量 || 0;
      existing.売上合計 += row.商品売上 || 0;
      existing.count++;
      unresolvedTax.set(key, existing);
    }
  }

  return {
    resolved,
    unresolved: [...unresolved.values()],
    zeroGenka: [...zeroGenka.values()],
    unresolvedTax: [...unresolvedTax.values()],
    conflicts,
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
  // CSV解析（テキストベースで軽量処理。パーサは payment-csv.js に分離 = テストと本番で同一）
  const text = buf.toString('utf-8');
  const { headerIdx, rows: parsedRows } = parsePaymentCsvText(text);
  if (headerIdx < 0) {
    return res.status(400).json({ error: 'CSVヘッダー行（"日付/時間"と"SKU"を含む行）が見つかりません' });
  }

  // 対象年月を推定（最初の日付から）。監査M-1: slice(0,7)は '2026/3/15'→'2026-3/' の
  // ゼロ埋め漏れで月キーJOINから脱落する(mirrorにboot時修復migrationが存在=本番発生実績)
  const firstDate = parsedRows[0]?.日付 || '';
  const yearMonth = normalizeYearMonth(firstDate);
  if (!yearMonth) {
    return res.status(400).json({ error: `CSVの日付から年月を特定できません: "${firstDate}"` });
  }

  // SKU解決
  const { resolved, unresolved, zeroGenka, unresolvedTax, conflicts } = resolveSkus(parsedRows, db);

  // 集計
  const { byTax, bySegment, excluded, otherDetails, noSkuDetails, columns, feeColumns, mfRow, mfColumns } = aggregate(resolved);

  // 確定済みの月なら前回情報を返す (過去月の再集計時に広告費をプリフィルし、上書き注意を出す)
  let existing = null;
  try {
    const prev = db.prepare('SELECT confirmed_at, ad_cost, csv_filename FROM mart_amazon_monthly_summary WHERE year_month = ?').get(yearMonth);
    if (prev) existing = { confirmed_at: prev.confirmed_at || '', ad_cost: prev.ad_cost || 0, csv_filename: prev.csv_filename || '' };
  } catch {}

  // 未登録税率の件数
  const unresolvedTaxCount = resolved.filter(r => r.解決方法 !== 'skip' && r.解決方法 !== 'no_sku' && r.解決方法 !== 'adjustment_no_master' && r.税率 === null).length;

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
      if (typeof v === 'string') return csvCell(v);
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
  summaryCsv += 'セグメント,' + columns.join(',') + ',原価合計,' + feeColumns.join(',') + '\n';
  for (const [key, row] of Object.entries(bySegment)) {
    const label = SEGMENT_NAMES[key] || (key === 'other' ? 'その他/未分類' : key);
    summaryCsv += key + ':' + label + ',' + columns.map(c => row[c] || 0).join(',') + ',' + (row.原価合計 || 0)
      + ',' + feeColumns.map(c => row[c] || 0).join(',') + '\n';
  }
  // SKUなし行の説明別一覧 (手数料内訳の根拠)
  summaryCsv += '\n【SKUなし行の説明別一覧（手数料内訳）】\n';
  summaryCsv += 'トランザクションの種類,説明,判定,行数,合計\n';
  for (const d of noSkuDetails) {
    summaryCsv += [csvCell(d.トランザクション種類), csvCell(d.説明), csvCell(d.判定), d.行数, d.合計].join(',') + '\n';
  }

  // /confirm でサーバ側の真値として使うため集計結果も保管 (Codex 3R #1: 改竄防御)
  const canConfirm = unresolved.length === 0 && unresolvedTax.length === 0 && conflicts.length === 0;
  evidenceStore.set(yearMonth, {
    detail: detailCsv,
    summary: summaryCsv,
    serverState: {
      totalRows: parsedRows.length,
      resolvedCount: resolved.filter(r => r.解決方法 !== 'unresolved' && r.解決方法 !== 'skip' && r.解決方法 !== 'no_sku' && r.解決方法 !== 'adjustment_no_master' && !['mixed_tax','mixed_segment','partial_component','invalid_quantity','mapped_target_missing'].includes(r.解決方法)).length,
      unresolvedCount: unresolved.length,
      unresolvedTaxCount: unresolvedTax.length,
      conflictsCount: conflicts.length,
      canConfirm,
      byTax, bySegment, excluded, mfRow,
    },
  });

  res.json({
    yearMonth,
    totalRows: parsedRows.length,
    resolvedCount: resolved.filter(r => r.解決方法 !== 'unresolved' && r.解決方法 !== 'skip' && r.解決方法 !== 'no_sku' && r.解決方法 !== 'adjustment_no_master' && !['mixed_tax','mixed_segment','partial_component','invalid_quantity','mapped_target_missing'].includes(r.解決方法)).length,
    unresolvedSkus: unresolved,
    unresolvedTaxCount,
    unresolvedTax,
    canConfirm: unresolved.length === 0 && unresolvedTax.length === 0 && conflicts.length === 0,
    conflicts,
    byTax,
    bySegment,
    excluded,
    otherDetails,
    noSkuDetails,
    columns,
    feeColumns,
    mfRow,
    mfColumns,
    segmentNames: SEGMENT_NAMES,
    excludedNames: EXCLUDED_SEGMENTS,
    zeroGenka,
    existing,
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
    th.fee-col{background:#f3e8d6}
    td.fee-col{background:#fdf8f0}
    .fee-first{border-left:2px solid #e0c9a6}
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

      <div id="unresolvedTaxCard" class="card" style="display:none">
        <h2>⚠️ 税率未登録</h2>
        <p class="meta">以下の商品は商品マスタに消費税率が登録されていません。ミニPCの管理画面で税率を登録してください。税率未登録がある場合、確定できません。</p>
        <div id="unresolvedTaxList"></div>
      </div>

      <div id="conflictsCard" class="card" style="display:none">
        <h2>⚠️ セット商品の解決エラー</h2>
        <p class="meta">セット商品(1 SKU = N構成品)で、構成品の税率/売上分類が混在しているか、構成品が商品マスタに見つかりません。確定できません。</p>
        <div id="conflictsList"></div>
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
        <h2>MF振替伝票入力用 仕訳</h2>
        <p class="meta">MFクラウド会計の「振替伝票入力」（仕訳辞書: Amazon売上）にそのまま転記できる形式です。取引日は集計月の月末日を入力してください。</p>
        <div id="mfJournalTable"></div>
      </div>

      <div class="card">
        <h2>セグメント別集計（管理会計用）</h2>
        <div id="segmentTable"></div>
        <div id="excludedInfo"></div>
      </div>

      <div id="noSkuCard" class="card" style="display:none">
        <h2>SKUなし行の説明別一覧（手数料内訳の根拠）</h2>
        <p class="meta">SKUを持たない行を「トランザクションの種類 × 説明」で集計。「判定」列が手数料内訳（Amazon Easy Ship料金 / FBA在庫保管手数料 / FBA長期在庫保管手数料）のどれに入ったかです。Amazonが説明の文言を変えた月はここで気づけます。</p>
        <div id="noSkuList"></div>
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
        <p class="meta" style="margin-top:6px">明細: アップロードCSVの全行+税率・分類・原価の判定結果 / 集計: 税率別+MF税込+セグメント別（手数料内訳列付き）+SKUなし行の説明別一覧</p>
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
    const FEE_COLUMNS = ${JSON.stringify(FEE_COLUMNS)};
    const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
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
      if (data.unresolvedTax && data.unresolvedTax.length > 0) summaryHtml += ' / <span class="negative">税率未登録: ' + data.unresolvedTax.length + '商品</span>';
      if (data.conflicts && data.conflicts.length > 0) summaryHtml += ' / <span class="negative">セット解決エラー: ' + data.conflicts.length + '件</span>';
      if (data.canConfirm) summaryHtml += '<br><b style="color:#27ae60">✅ 全て解決済み — 確定可能</b>';
      else {
        const reasons = [];
        if (data.unresolvedSkus.length > 0) reasons.push('未登録SKU');
        if (data.unresolvedTax && data.unresolvedTax.length > 0) reasons.push('税率未登録');
        if (data.conflicts && data.conflicts.length > 0) reasons.push('セット解決エラー');
        summaryHtml += '<br><b style="color:#e74c3c">❌ ' + reasons.join('・') + 'あり — 確定不可</b>';
      }
      summaryHtml += '</div>';
      // 確定済みの月を再アップロードした場合 (過去月の再集計): 上書き注意 + 広告費を前回値でプリフィル
      if (data.existing) {
        summaryHtml += '<div class="warn">📌 <b>' + data.yearMonth + ' は確定済みです</b>（' + esc(data.existing.confirmed_at || '') + '・広告費 ¥' + Math.round(data.existing.ad_cost || 0).toLocaleString() + '）。'
          + 'このまま「確定」すると前回の集計を上書きします。広告費欄には前回の値を入れてあります。</div>';
      }
      // 広告費欄: 月が変わったら existing ? 前回値 : 0 に明示リセット (前の月のプリフィル値が別の月の按分・確定に流れるのを防ぐ)。
      // 同じ月の再アップロード (SKU登録後のやり直し等) では入力中の値を維持する
      const adInput = document.getElementById('adCost');
      if (adInput && data.yearMonth !== lastYearMonth) {
        adInput.value = data.existing ? Math.round(data.existing.ad_cost || 0) : 0;
      }
      lastYearMonth = data.yearMonth;
      document.getElementById('summary').innerHTML = summaryHtml;
      // 確定ボタンの disabled 制御 (hard fail)
      const confirmBtn = document.getElementById('confirmBtn');
      if (confirmBtn) confirmBtn.disabled = !data.canConfirm;

      // 未登録SKU
      if (data.unresolvedSkus.length > 0) {
        const card = document.getElementById('unresolvedCard');
        card.style.display = 'block';
        let html = '<table><tr><th>SKU</th><th>商品名</th><th>出現数</th><th>金額合計</th></tr>';
        for (const u of data.unresolvedSkus) {
          html += '<tr><td>' + esc(u.sku) + '</td><td>' + esc((u.name || '').slice(0, 60)) + '</td><td class="num">' + u.count + '</td><td class="num">' + fmt(u.amount) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('unresolvedList').innerHTML = html;
      } else {
        document.getElementById('unresolvedCard').style.display = 'none';
      }

      // セット解決エラー (mixed_tax / mixed_segment / partial_component)
      if (data.conflicts && data.conflicts.length > 0) {
        const card = document.getElementById('conflictsCard');
        card.style.display = 'block';
        const typeLabels = { mixed_tax: '税率混在', mixed_segment: '売上分類混在', partial_component: '構成品欠損', invalid_quantity: '数量不正', mapped_target_missing: 'マップ先商品欠損' };
        let html = '<table class="detail-table"><tr><th>SKU</th><th>エラー種別</th><th>詳細</th></tr>';
        for (const c of data.conflicts) {
          const label = typeLabels[c.type] || c.type;
          let detail = '';
          if (c.type === 'mixed_tax') detail = '税率: ' + (c.taxRates || []).map(r => (r * 100).toFixed(0) + '%').join(', ');
          else if (c.type === 'mixed_segment') detail = '分類: ' + (c.segments || []).join(', ');
          else if (c.type === 'partial_component') detail = '欠損ne_code: ' + (c.missing || []).join(', ');
          else if (c.type === 'invalid_quantity') detail = '不正数量: ' + (c.invalidQty || []).map(q => q.ne_code + '=' + q.rawQty).join(', ');
          else if (c.type === 'mapped_target_missing') detail = '欠損ne_code: ' + (c.missing || []).join(', ');
          html += '<tr><td style="text-align:left">' + esc(c.sku) + '</td><td>' + label + '</td><td style="text-align:left">' + esc(detail) + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('conflictsList').innerHTML = html;
      } else {
        document.getElementById('conflictsCard').style.display = 'none';
      }

      // 税率未登録
      if (data.unresolvedTax && data.unresolvedTax.length > 0) {
        const card = document.getElementById('unresolvedTaxCard');
        card.style.display = 'block';
        let html = '<div class="warn" style="margin-bottom:8px"><b>' + data.unresolvedTax.length + '商品</b>の税率が未登録です。税率別集計に含まれません。</div>';
        html += '<table class="detail-table"><tr><th>商品コード</th><th>SKU</th><th>商品名</th><th>出現行数</th><th>数量合計</th><th>商品売上合計</th></tr>';
        for (const t of data.unresolvedTax) {
          html += '<tr>';
          html += '<td style="text-align:left">' + esc(t.商品コード) + '</td>';
          html += '<td style="text-align:left">' + esc(t.sku || '-') + '</td>';
          html += '<td style="text-align:left">' + esc((t.商品名 || '').slice(0, 50)) + '</td>';
          html += '<td class="num">' + t.count + '</td>';
          html += '<td class="num">' + t.数量合計 + '</td>';
          html += '<td class="num">' + fmt(t.売上合計) + '</td>';
          html += '</tr>';
        }
        html += '</table>';
        document.getElementById('unresolvedTaxList').innerHTML = html;
      } else {
        document.getElementById('unresolvedTaxCard').style.display = 'none';
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

      // MF振替伝票入力用 仕訳
      if (data.mfRow) {
        document.getElementById('mfJournalTable').innerHTML = mfJournalTableHtml(data.mfRow);
      }

      // セグメント別（1〜3 + other。4=輸出は除外）
      renderSegmentTable('segmentTable', data.bySegment, data.segmentNames, cols, null);

      // SKUなし行の説明別一覧 (手数料内訳の根拠)
      renderNoSkuList(data.noSkuDetails || []);
    }

    function renderNoSkuList(list) {
      const card = document.getElementById('noSkuCard');
      if (!list.length) { card.style.display = 'none'; return; }
      card.style.display = 'block';
      let html = '<table class="detail-table"><tr><th>トランザクションの種類</th><th>説明</th><th>判定</th><th>行数</th><th>合計</th></tr>';
      let rows = 0, total = 0;
      for (const d of list) {
        html += '<tr>';
        html += '<td style="text-align:left;font-weight:normal">' + esc(d.トランザクション種類 || '-') + '</td>';
        html += '<td style="text-align:left">' + esc(d.説明 || '-') + '</td>';
        html += '<td style="text-align:left">' + (d.判定 ? '<b>' + esc(d.判定) + '</b>' : '<span class="meta">—</span>') + '</td>';
        html += '<td class="num">' + d.行数 + '</td>';
        html += '<td class="num">' + fmt(d.合計) + '</td>';
        html += '</tr>';
        rows += d.行数; total += d.合計;
      }
      html += '<tr style="font-weight:bold;border-top:2px solid #333"><td colspan="3">合計</td><td class="num">' + rows + '</td><td class="num">' + fmt(total) + '</td></tr>';
      html += '</table>';
      document.getElementById('noSkuList').innerHTML = html;
    }

    // MF振替伝票用の仕訳行を組み立てる。
    // 対応関係（会計処理の実務ルール）:
    //   貸方 売上高/課売10%   = 商品売上(10%) + 配送料 + ギフト包装手数料 (+端数調整)
    //   貸方 売上高/課売(軽)8% = 商品売上(8%)
    //   借方 支払手数料【原価】/課仕10%       = -(Amazonポイントの費用 + プロモーション割引額 + 手数料)
    //   借方 発送運賃【原価】(補助:AmazonFBA手数料)/課仕10% = -(FBA手数料)
    //   借方 広告宣伝費【原価】/課仕10%       = -(トランザクションに関するその他の手数料+その他)
    //   借方 売掛金_EC売上/対象外            = 合計（差引入金額）
    function buildMfJournalRows(mf) {
      const v = k => mf[k] || 0;
      return [
        { dc: 'credit', account: '売上高', sub: 'Amazon', tax: '課売10%',
          amount: v('商品売上(10%)') + v('配送料') + v('ギフト包装手数料'), memo: 'Amazon売上' },
        { dc: 'credit', account: '売上高', sub: 'Amazon', tax: '課売(軽)8%',
          amount: v('商品売上(8%)'), memo: 'Amazon売上 軽減8%' },
        { dc: 'debit', account: '支払手数料【原価】', sub: 'Amazon', tax: '課仕10%',
          amount: -(v('Amazonポイントの費用') + v('プロモーション割引額') + v('手数料')), memo: 'Amazon PF手数料' },
        { dc: 'debit', account: '発送運賃【原価】', sub: 'AmazonFBA手数料', tax: '課仕10%',
          amount: -v('FBA手数料'), memo: 'Amazon 運賃' },
        { dc: 'debit', account: '広告宣伝費【原価】', sub: 'Amazon', tax: '課仕10%',
          amount: -v('トランザクションに関するその他の手数料+その他'), memo: 'Amazon その他手数料' },
        { dc: 'debit', account: '売掛金_EC売上', sub: 'Amazon', tax: '対象外',
          amount: v('合計'), memo: 'Amazon 差し引き入金額' },
        { dc: 'credit', account: '売上高', sub: 'Amazon', tax: '課売10%',
          amount: v('端数調整'), memo: 'Amazon 端数調整' },
      ];
    }

    function mfJournalTableHtml(mf) {
      const rows = buildMfJournalRows(mf);
      let debitTotal = 0, creditTotal = 0;
      rows.forEach(r => { if (r.dc === 'debit') debitTotal += r.amount; else creditTotal += r.amount; });

      let html = '<table><tr>';
      html += '<th colspan="3" style="text-align:center">借方</th><th colspan="3" style="text-align:center">貸方</th><th>摘要</th></tr>';
      html += '<tr><th>勘定科目</th><th>税区分</th><th>金額</th><th>勘定科目</th><th>税区分</th><th>金額</th><th></th></tr>';
      for (const r of rows) {
        const acctCell = '<td style="text-align:left">' + r.account + '<br><span class="meta">' + r.sub + '</span></td>';
        const taxCell = '<td>' + r.tax + '</td>';
        const amtCell = '<td class="num">' + fmt(r.amount) + '</td>';
        const blank = '<td></td><td></td><td></td>';
        html += '<tr>';
        html += r.dc === 'debit' ? (acctCell + taxCell + amtCell + blank) : (blank + acctCell + taxCell + amtCell);
        html += '<td style="text-align:left">' + r.memo + '</td>';
        html += '</tr>';
      }
      html += '<tr style="font-weight:bold;border-top:2px solid #333">';
      html += '<td colspan="2">合計金額</td><td class="num">' + fmt(debitTotal) + '</td>';
      html += '<td colspan="2">合計金額</td><td class="num">' + fmt(creditTotal) + '</td><td></td>';
      html += '</tr></table>';

      const diff = Math.round(debitTotal) - Math.round(creditTotal);
      html += diff === 0
        ? '<div class="ok" style="margin-top:8px">✅ 借方・貸方 合計一致（' + fmt(debitTotal) + '円）</div>'
        : '<div class="err" style="margin-top:8px">❌ 借方・貸方が一致しません（差額: ' + fmt(diff) + '円）— 端数調整・按分ロジックを確認してください</div>';

      return html;
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

      // 手数料内訳列 (右端。合計に含まれる金額の抜き出しなので合計行の横並びに加算はしない)
      const feeCols = (lastData && lastData.feeColumns) || FEE_COLUMNS;
      const feeCls = i => 'fee-col' + (i === 0 ? ' fee-first' : '');
      let segHtml = '<table><tr><th>セグメント</th>';
      cols.forEach(c => segHtml += '<th>' + c + '</th>');
      segHtml += '<th>広告費</th><th>原価合計</th>';
      feeCols.forEach((c, i) => segHtml += '<th class="' + feeCls(i) + '" title="合計に含まれる金額の内訳（CSV「説明」で判別）">' + c + '</th>');
      segHtml += '</tr>';
      let totalRow = {};
      cols.forEach(c => totalRow[c] = 0);
      feeCols.forEach(c => totalRow[c] = 0);
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
        feeCols.forEach((c, i) => { segHtml += '<td class="num ' + feeCls(i) + '">' + fmt(row[c] || 0) + '</td>'; totalRow[c] += (row[c] || 0); });
        segHtml += '</tr>';
      }
      segHtml += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
      cols.forEach(c => segHtml += '<td class="num">' + fmt(totalRow[c]) + '</td>');
      segHtml += '<td class="num">' + fmt(totalAd) + '</td>';
      segHtml += '<td class="num">' + fmt(totalRow.原価合計) + '</td>';
      feeCols.forEach((c, i) => segHtml += '<td class="num ' + feeCls(i) + '">' + fmt(totalRow[c]) + '</td>');
      segHtml += '</tr></table>';
      segHtml += '<p class="meta">右端の3列（' + feeCols.join(' / ') + '）は、CSV「説明」で判別したSKUなし行の「合計」を合算した<b>内訳</b>です。同じ行の 手数料・FBA手数料・その他・合計 に既に含まれている金額の抜き出しで、加算対象ではありません。根拠は下の「SKUなし行の説明別一覧」を参照。</p>';
      document.getElementById(targetId).innerHTML = segHtml;

      // 除外セグメント（4=輸出）
      let exclHtml = '';
      if (lastData.excluded) {
        for (const [key, row] of Object.entries(lastData.excluded)) {
          if (row.行数 > 0) {
            const label = lastData.excludedNames[key] || key;
            exclHtml += '<div class="excluded">';
            exclHtml += '<b>除外: ' + key + ': ' + label + '</b>（' + row.行数 + '行）';
            exclHtml += ' — 商品売上: ' + fmt(row['商品売上']) + ' / 合計: ' + fmt(row['合計']) + ' / 原価合計: ' + fmt(row.原価合計);
            exclHtml += '</div>';
          }
        }
      }
      document.getElementById('excludedInfo').innerHTML = exclHtml;

      // 「その他/未分類」明細
      if (lastData.otherDetails && lastData.otherDetails.length > 0) {
        const card = document.getElementById('otherDetailCard');
        card.style.display = 'block';
        let html = '<table class="detail-table"><tr><th>SKU</th><th>商品コード</th><th>商品名</th><th>種類</th><th>解決方法</th><th>行数</th><th>数量</th><th>商品売上</th><th>合計</th></tr>';
        for (const d of lastData.otherDetails) {
          const method = { direct: '商品コード一致', sku_map: 'SKUマップ経由', unresolved: '未解決', no_sku: 'SKUなし', adjustment_no_master: '調整(照合対象外)' }[d.解決方法] || esc(d.解決方法);
          html += '<tr>';
          html += '<td style="text-align:left">' + esc(d.sku || '-') + '</td>';
          html += '<td style="text-align:left">' + esc(d.商品コード || '-') + '</td>';
          html += '<td style="text-align:left">' + esc((d.商品名 || '').slice(0, 50)) + '</td>';
          html += '<td style="text-align:left">' + esc(d.トランザクション種類 || '-') + '</td>';
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
      if (lastData.zeroGenka && lastData.zeroGenka.length > 0) {
        const card = document.getElementById('zeroGenkaCard');
        card.style.display = 'block';
        let html = '<div class="warn" style="margin-bottom:8px"><b>' + lastData.zeroGenka.length + '商品</b>が原価0円で計算されています</div>';
        html += '<table class="detail-table"><tr><th>商品コード</th><th>SKU</th><th>商品名</th><th>出現行数</th><th>数量合計</th><th>商品売上合計</th></tr>';
        for (const z of lastData.zeroGenka) {
          html += '<tr>';
          html += '<td style="text-align:left">' + esc(z.商品コード) + '</td>';
          html += '<td style="text-align:left">' + esc(z.sku || '-') + '</td>';
          html += '<td style="text-align:left">' + esc((z.商品名 || '').slice(0, 50)) + '</td>';
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
    let lastYearMonth = null; // 直前に表示した年月 (広告費欄のリセット判定用)

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
      if (!lastData.canConfirm) {
        alert('未登録SKU・税率未登録・セット解決エラーが残っています。確定できません。');
        return;
      }
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
            adCost,
            csvFilename: document.getElementById('csvFile').files[0]?.name || '',
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

            html += '<h3 style="font-size:13px;color:#555;margin:12px 0 4px">MF振替伝票入力用 仕訳</h3>';
            html += mfJournalTableHtml(mf);
          }

          // セグメント別集計
          const seg = row.by_segment || {};
          const segNames = {1:'自社商品', 2:'取引先限定', 3:'仕入れ商品'};
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

          // 手数料内訳列は本機能リリース後に確定した月だけ持つ (無い月は「—」)
          const hasFee = Object.values(seg).some(sr => FEE_COLUMNS.some(c => sr[c] !== undefined));
          const hFeeCls = i => 'fee-col' + (i === 0 ? ' fee-first' : '');
          const hFeeCell = v => hasFee ? fmt(v || 0) : '<span class="meta">—</span>';
          html += '<table><tr><th>セグメント</th>';
          segCols.forEach(c => html += '<th>' + c + '</th>');
          html += '<th>広告費</th><th>原価合計</th>';
          FEE_COLUMNS.forEach((c, i) => html += '<th class="' + hFeeCls(i) + '">' + c + '</th>');
          html += '</tr>';
          let sTot = {}; segCols.forEach(c => sTot[c] = 0); FEE_COLUMNS.forEach(c => sTot[c] = 0); sTot.原価合計 = 0; let sAdTot = 0;
          for (const [key, sr] of Object.entries(seg)) {
            const lb = segNames[key] || (key === 'other' ? 'その他/未分類' : key);
            html += '<tr><td>' + key + ': ' + lb + '</td>';
            segCols.forEach(c => { html += '<td class="num">' + fmt(sr[c] || 0) + '</td>'; sTot[c] += (sr[c] || 0); });
            html += '<td class="num">' + fmt(hAd[key] || 0) + '</td>';
            sAdTot += (hAd[key] || 0);
            html += '<td class="num">' + fmt(sr.原価合計 || 0) + '</td>';
            sTot.原価合計 += (sr.原価合計 || 0);
            FEE_COLUMNS.forEach((c, i) => { html += '<td class="num ' + hFeeCls(i) + '">' + hFeeCell(sr[c]) + '</td>'; sTot[c] += (sr[c] || 0); });
            html += '</tr>';
          }
          html += '<tr style="font-weight:bold;border-top:2px solid #333"><td>合計</td>';
          segCols.forEach(c => html += '<td class="num">' + fmt(sTot[c]) + '</td>');
          html += '<td class="num">' + fmt(sAdTot) + '</td>';
          html += '<td class="num">' + fmt(sTot.原価合計) + '</td>';
          FEE_COLUMNS.forEach((c, i) => html += '<td class="num ' + hFeeCls(i) + '">' + hFeeCell(sTot[c]) + '</td>');
          html += '</tr></table>';
          if (!hasFee) html += '<p class="meta">手数料内訳（右端3列）はこの月の確定時点では未対応でした。この月のペイメントCSVを再アップロードして再確定すると表示されます。</p>';

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

        const segNames = {1:'自社商品', 2:'取引先限定', 3:'仕入れ商品', other:'その他/未分類'};
        const segCols = ['商品売上','商品の売上税','配送料','配送料の税金','ギフト包装手数料','ギフト包装の税金','Amazonポイント費用','プロモーション割引額','プロモーション割引の税金','手数料','FBA手数料','トランザクション他','その他','合計'];
        const adTargets = ['1','2'];

        let csv = '\\uFEFF'; // BOM
        csv += '集計月,セグメント,' + segCols.join(',') + ',広告費,原価合計,' + FEE_COLUMNS.join(',') + '\\n';

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
            const feeVals = FEE_COLUMNS.map(c => sr[c] === undefined ? '' : sr[c]); // 未対応月は空欄
            csv += ymStr + ',' + key + ':' + label + ',' + vals.join(',') + ',' + (adMap[key] || 0) + ',' + (sr.原価合計 || 0) + ',' + feeVals.join(',') + '\\n';
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
        <li><b>セグメント別集計</b> — 管理会計用（自社商品/取引先限定/仕入れ商品/その他）</li>
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
        <tr><td>Stage 2</td><td>SKUマスタで変換してから商品コードを検索</td><td>mirror_sku_resolved → mirror_products</td></tr>
        <tr><td>Stage 3</td><td>どちらにも一致しない → <b>未登録SKU</b></td><td>—</td></tr>
      </table>
      <div class="note">SKUと商品コードは全て<b>小文字に統一</b>して照合しています。</div>

      <h3>解決結果の列（エビデンスCSVに出力）</h3>
      <table class="m-tbl">
        <tr><th>列</th><th>内容</th></tr>
        <tr><td>商品コード</td><td>照合で特定されたNE商品コード（未解決の場合は空）</td></tr>
        <tr><td>税率</td><td>10 or 8（商品マスタの消費税率から判定）</td></tr>
        <tr><td>売上分類</td><td>1:自社 / 2:取引先限定 / 3:仕入れ / 4:輸出 / 空:未分類</td></tr>
        <tr><td>原価</td><td>商品マスタの原価（未解決 or 原価未登録の場合は0）</td></tr>
        <tr><td>解決方法</td><td>direct / sku_map / unresolved / no_sku / adjustment_no_master / skip</td></tr>
      </table>

      <h2>5. 税率別集計</h2>
      <table class="m-tbl">
        <tr><th>分類</th><th>条件</th></tr>
        <tr><td><b>10%</b></td><td>消費税率=0.10 の商品、または税率未登録の商品（10%仮扱い）</td></tr>
        <tr><td><b>8%</b></td><td>消費税率=0.08 の商品</td></tr>
      </table>
      <div class="note">トランザクション種類が「振込み」の行は集計から除外されます。「注文外料金」はマスタ照合せず「その他」へ。「調整」（FBA在庫補償など）はマスタ照合を試み、Amazon独自採番SKUで照合できないものだけ「その他」に集計されます（確定はブロックしません）。</div>

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

      <h2>7. MF振替伝票入力用 仕訳</h2>
      <p>「6. MF連携用 税込み集計」の値を、MFクラウド会計の<b>振替伝票入力</b>画面（仕訳辞書「Amazon売上」）にそのまま転記できる借方/貸方の仕訳形式に組み替えたものです。</p>
      <table class="m-tbl">
        <tr><th>借方/貸方</th><th>勘定科目</th><th>税区分</th><th>金額の内訳</th></tr>
        <tr><td>貸方</td><td>売上高</td><td>課売10%</td><td>商品売上(10%) + 配送料 + ギフト包装手数料</td></tr>
        <tr><td>貸方</td><td>売上高</td><td>課売(軽)8%</td><td>商品売上(8%)</td></tr>
        <tr><td>借方</td><td>支払手数料【原価】</td><td>課仕10%</td><td>−(Amazonポイントの費用 + プロモーション割引額 + 手数料)</td></tr>
        <tr><td>借方</td><td>発送運賃【原価】（補助:AmazonFBA手数料）</td><td>課仕10%</td><td>−FBA手数料</td></tr>
        <tr><td>借方</td><td>広告宣伝費【原価】</td><td>課仕10%</td><td>−(トランザクションに関するその他の手数料+その他)</td></tr>
        <tr><td>借方</td><td>売掛金_EC売上</td><td>対象外</td><td>合計（差引入金額）</td></tr>
        <tr><td>貸方</td><td>売上高</td><td>課売10%</td><td>端数調整</td></tr>
      </table>
      <div class="note">借方・貸方の合計金額が一致するかを表の下に自動表示します。金額が赤字（マイナス）になっている行がある場合は、その行だけ借方/貸方を入れ替えてMFに入力してください。取引日は集計月の月末日を入力します。</div>

      <h2>8. セグメント別集計（管理会計用）</h2>
      <table class="m-tbl">
        <tr><th>セグメント</th><th>売上分類</th><th>内容</th><th>広告費</th></tr>
        <tr><td><b>1: 自社商品</b></td><td>1</td><td>自社ブランド・独占商品</td><td>売上按分あり</td></tr>
        <tr><td><b>2: 取引先限定</b></td><td>2</td><td>取引先限定品</td><td>売上按分あり</td></tr>
        <tr><td><b>3: 仕入れ商品</b></td><td>3</td><td>一般仕入れ商品</td><td>なし</td></tr>
        <tr><td><b>other: その他</b></td><td>空/未登録</td><td>SKUなし行（FBA保管手数料・FBA在庫補償等）+ 分類未登録商品</td><td>なし</td></tr>
      </table>
      <div class="note"><b>セグメント4（輸出）</b>は集計テーブルから除外、別枠で表示されます。</div>

      <h3>広告費の按分</h3>
      <p>Amazon広告はクレカ払い（ペイメントCSVに含まれない）のため手入力で追加。セグメント1と2の<b>商品売上比率</b>で按分します。</p>
      <p>例: 広告費100万、セグメント1売上4,000万、セグメント2売上1,000万 → 1に80万、2に20万</p>

      <h3>原価合計</h3>
      <p>各行の <code>原価 × 数量</code> をセグメントごとに合算（税抜）。</p>

      <h3>手数料内訳（右端3列: Amazon Easy Ship料金 / FBA在庫保管手数料 / FBA長期在庫保管手数料）</h3>
      <p>SKUを持たない手数料行をCSVの「説明」で判別し、その行の「合計」列を合算した金額です。<b>同じ行の 手数料・FBA手数料・その他・合計 に既に含まれている金額の抜き出し</b>であり、セグメント合計への加算対象ではありません（二重計上ではありません）。</p>
      <table class="m-tbl">
        <tr><th>列</th><th>判定（説明の部分一致・上から順に排他）</th><th>実データでの表記例</th></tr>
        <tr><td><b>FBA長期在庫保管手数料</b></td><td>「長期在庫保管手数料」を含む</td><td>FBA長期在庫保管手数料</td></tr>
        <tr><td><b>FBA在庫保管手数料</b></td><td>「在庫保管手数料」を含む（長期に該当しなかった行）</td><td>FBA在庫保管手数料 / FBA在庫保管手数料:（取り消し・訂正行）</td></tr>
        <tr><td><b>Amazon Easy Ship料金</b></td><td>「Easy Ship」を含む（大文字小文字は無視）</td><td>Amazon Easy Ship料金 / Amazon Easy Shipの発送重量手数料 / Easy Ship発送重量手数料</td></tr>
      </table>
      <ul>
        <li>説明の文言も金額が入る列も月によって変わる（例: Easy Ship は 2026年6月まで2種類の別名、7月から統合名）ため、完全一致ではなく部分一致＋「合計」列の合算で判定します</li>
        <li>在庫保管手数料の<b>取り消し・訂正</b>行（説明の末尾に「:」が付く）も含めたネット額です。内訳は「SKUなし行の説明別一覧」カードで確認できます</li>
        <li>SKUなし行は売上分類を持たないため、金額は原則「other: その他/未分類」行に入ります</li>
        <li>本機能リリース前に確定した月・旧スプレッドシート移行分は「—」表示。その月のペイメントCSVを再アップロードして再確定すると表示されます（2026年3月分以降を再集計する運用）</li>
      </ul>

      <h2>9. エビデンスCSV</h2>
      <p>アップロード後に2種類ダウンロード可能:</p>
      <table class="m-tbl">
        <tr><th>種類</th><th>内容</th></tr>
        <tr><td><b>明細エビデンス</b></td><td>元CSVの全行 + 商品コード・税率・売上分類・原価・解決方法</td></tr>
        <tr><td><b>集計サマリー</b></td><td>税率別 + MF税込 + セグメント別（手数料内訳列付き）の集計表 + SKUなし行の説明別一覧</td></tr>
      </table>
      <div class="note">エビデンスはアップロード時にメモリに一時保存。ページを離れると再アップロードが必要です。</div>

      <h2>10. 確定保存・過去データ</h2>
      <ul>
        <li>広告費を入力して「確定」→ DBに保存</li>
        <li>同じ年月で再確定すると上書き（確定済みの月を再アップロードすると画面上部に注意が出て、広告費欄に前回の値が入ります）</li>
        <li>確定済みデータはアコーディオンで展開表示</li>
        <li>「セグメント別集計CSVダウンロード」で全月分を一括CSV出力（集計月は各月末日 yyyy/mm/dd）</li>
      </ul>

      <h2>11. データソース・更新</h2>
      <table class="m-tbl">
        <tr><th>データ</th><th>ソース</th><th>更新</th></tr>
        <tr><td>商品マスタ / SKUマップ</td><td>ミニPC → Render</td><td>毎朝7時自動同期</td></tr>
        <tr><td>ペイメントCSV</td><td>セラセン手動DL</td><td>月次</td></tr>
        <tr><td>広告費</td><td>手入力（クレカ払い）</td><td>月次</td></tr>
      </table>

      <h2>12. 注意事項</h2>
      <ul>
        <li>CSV金額のカンマ区切り（例: <code>3,200</code>）は自動除去されます</li>
        <li>税率未登録の商品は<b>税率別集計から除外</b>（税率未登録リストに表示、確定不可）</li>
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
  const { yearMonth, adCost, csvFilename } = req.body;

  if (!yearMonth) return res.status(400).json({ error: 'yearMonth は必須です' });

  // サーバ側保管値を真値として使う (Codex 3R #1: クライアント申告値の改竄防御)
  const cached = evidenceStore.get(yearMonth);
  if (!cached || !cached.serverState) {
    return res.status(400).json({ error: 'アップロード結果が見つかりません。CSVを再アップロードしてください' });
  }
  const s = cached.serverState;
  if (!s.canConfirm) {
    if (s.unresolvedCount > 0) return res.status(400).json({ error: '未登録SKUが残っているため確定できません' });
    if (s.unresolvedTaxCount > 0) return res.status(400).json({ error: '税率未登録があるため確定できません' });
    if (s.conflictsCount > 0) return res.status(400).json({ error: 'セット解決エラー(税率/分類混在・構成品欠損・数量不正・マップ先商品欠損)があるため確定できません' });
    return res.status(400).json({ error: '確定不可状態です' });
  }

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    db.prepare(`INSERT OR REPLACE INTO mart_amazon_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       by_tax, by_segment, excluded, mf_row, ad_cost, confirmed_at, csv_filename)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      yearMonth, s.totalRows, s.resolvedCount, s.unresolvedCount,
      JSON.stringify(s.byTax), JSON.stringify(s.bySegment), JSON.stringify(s.excluded),
      JSON.stringify(s.mfRow), adCost || 0, now, csvFilename || ''
    );

    db.prepare(`INSERT INTO mart_amazon_upload_log
      (year_month, filename, total_rows, resolved_count, unresolved_count, uploaded_at)
      VALUES (?,?,?,?,?,?)
    `).run(yearMonth, csvFilename || '', s.totalRows, s.resolvedCount, s.unresolvedCount, now);

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

router.post('/import-history', requireImportKey('IMPORT_KEY_AMAZON'), importJsonParser, (req, res) => {
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
