/**
 * 米国Amazon売上集計ツール
 *
 * セラーセントラル(US)の Monthly Unified Transaction CSV をアップロードし、
 * USD集計→為替換算→管理会計用行(セグメント4=輸出)を自動計算する。
 *
 * 日本版との主な違い:
 *   - CSVはUTF-8 BOM付き、先頭9行スキップ(ヘッダーは10行目)
 *   - 全売上が売上分類4(輸出) → 税率分類なし・セグメント按分なし
 *   - USD→JPY為替換算(手入力レート、kabutanリンク併記)
 *   - type=Transfer の行は除外
 */
import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();
const UPLOAD_DIR = process.env.DATA_DIR ? process.env.DATA_DIR + '/import' : 'data/import';
if (!fs.existsSync(UPLOAD_DIR)) { try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch {} }
const upload = multer({ dest: UPLOAD_DIR });

// エビデンスCSV一時保存(yearMonth → { detail, summary })
const evidenceStore = new Map();

// CSV列インデックス (先頭9行スキップ後のヘッダーに対する)
const COL = {
  DATE: 0, SETTLEMENT: 1, TYPE: 2, ORDER_ID: 3, SKU: 4, DESC: 5, QTY: 6,
  MARKETPLACE: 7, ACCOUNT: 8, FULFILLMENT: 9,
  PRODUCT_SALES: 14, PRODUCT_SALES_TAX: 15,
  SHIPPING: 16, SHIPPING_TAX: 17,
  GIFT_WRAP: 18, GIFT_WRAP_TAX: 19,
  REGULATORY_FEE: 20, REGULATORY_FEE_TAX: 21,
  PROMO_REBATE: 22, PROMO_REBATE_TAX: 23,
  MARKETPLACE_WITHHELD: 24,
  SELLING_FEES: 25, FBA_FEES: 26, OTHER_TX_FEES: 27, OTHER: 28, TOTAL: 29,
};

// USDベース集計の列定義
const USD_COLS = [
  'product sales', 'product sales tax',
  'shipping credits', 'shipping credits tax',
  'gift wrap credits', 'giftwrap credits tax',
  'Regulatory Fee', 'Tax On Regulatory Fee',
  'promotional rebates', 'promotional rebates tax',
  'marketplace withheld tax',
  'selling fees', 'fba fees', 'other transaction fees', 'other', 'total',
];

// 管理会計用(B-Faith社内用)15列: セグメント4行の中身
const MGMT_COLS = [
  '商品売上', '商品の売上税', '配送料', '配送料の税金',
  'ギフト包装手数料', 'ギフト包装クレジットの税金',
  'Amazonポイントの費用', 'プロモーション割引額', 'プロモーション割引の税金',
  '手数料', 'FBA手数料', 'トランザクションに関するその他の手数料',
  'その他', '合計', '原価合計',
];

// ─── CSV解析 ───

function parseCsvLine(line) {
  const cols = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (ch === ',' && !inQ) { cols.push(cur); cur = ''; }
    else cur += ch;
  }
  cols.push(cur);
  return cols;
}

const num = v => {
  const n = parseFloat((v || '').toString().replace(/"/g, '').replace(/,/g, ''));
  return isNaN(n) ? 0 : n;
};
const clean = v => (v || '').toString().replace(/^"|"$/g, '').trim();

// "Mar 1, 2026 2:15:53 PM PST" → "2026-03-01"
function parseUsDate(s) {
  if (!s) return '';
  const MONTHS = { Jan:'01',Feb:'02',Mar:'03',Apr:'04',May:'05',Jun:'06',Jul:'07',Aug:'08',Sep:'09',Oct:'10',Nov:'11',Dec:'12' };
  const m = s.match(/^([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})/);
  if (!m) return s;
  return `${m[3]}-${MONTHS[m[1]] || '01'}-${String(m[2]).padStart(2, '0')}`;
}

function parseUsCsv(buf) {
  // UTF-8 BOM除去
  let text = buf.toString('utf-8');
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
  const lines = text.split(/\r?\n/);

  // 先頭9行は説明文、10行目(index=9)がヘッダー、11行目(index=10)以降がデータ
  const rows = [];
  for (let i = 10; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    const c = parseCsvLine(line);
    const type = clean(c[COL.TYPE]);
    if (type === 'Transfer') continue;  // 入金振替は除外

    rows.push({
      日付: parseUsDate(clean(c[COL.DATE])),
      決済番号: clean(c[COL.SETTLEMENT]),
      type,
      注文番号: clean(c[COL.ORDER_ID]),
      sku: clean(c[COL.SKU]).toLowerCase(),
      説明: clean(c[COL.DESC]),
      数量: parseInt(clean(c[COL.QTY])) || 0,
      'product sales': num(c[COL.PRODUCT_SALES]),
      'product sales tax': num(c[COL.PRODUCT_SALES_TAX]),
      'shipping credits': num(c[COL.SHIPPING]),
      'shipping credits tax': num(c[COL.SHIPPING_TAX]),
      'gift wrap credits': num(c[COL.GIFT_WRAP]),
      'giftwrap credits tax': num(c[COL.GIFT_WRAP_TAX]),
      'Regulatory Fee': num(c[COL.REGULATORY_FEE]),
      'Tax On Regulatory Fee': num(c[COL.REGULATORY_FEE_TAX]),
      'promotional rebates': num(c[COL.PROMO_REBATE]),
      'promotional rebates tax': num(c[COL.PROMO_REBATE_TAX]),
      'marketplace withheld tax': num(c[COL.MARKETPLACE_WITHHELD]),
      'selling fees': num(c[COL.SELLING_FEES]),
      'fba fees': num(c[COL.FBA_FEES]),
      'other transaction fees': num(c[COL.OTHER_TX_FEES]),
      'other': num(c[COL.OTHER]),
      'total': num(c[COL.TOTAL]),
    });
  }
  return rows;
}

// ─── SKU解決(3段階 + セット展開 — 日本版と同じパターン) ───

function resolveSkus(rows, db) {
  const productsMap = new Map();
  for (const p of db.prepare('SELECT * FROM mirror_products').all()) {
    productsMap.set((p.商品コード || '').toLowerCase(), p);
  }

  // 既定: mirror_sku_resolved (master優先 + sku_map fallback)
  // env WAREHOUSE_SKU_SOURCE=legacy で旧 mirror_sku_map 直参照に戻せる
  const useLegacy = process.env.WAREHOUSE_SKU_SOURCE === 'legacy';
  const skuMap = new Map();
  const skuRows = useLegacy
    ? db.prepare('SELECT seller_sku, ne_code, 数量 FROM mirror_sku_map').all()
    : db.prepare('SELECT seller_sku, ne_code, quantity AS 数量 FROM mirror_sku_resolved').all();
  for (const s of skuRows) {
    const key = s.seller_sku?.toLowerCase();
    if (!key) continue;
    if (!skuMap.has(key)) skuMap.set(key, []);
    skuMap.get(key).push(s);
  }

  const resolved = [];
  const unresolved = new Map();
  const conflicts = []; // セット商品の解決失敗

  for (const row of rows) {
    const sku = row.sku;
    if (!sku) {
      resolved.push({ ...row, 商品コード: null, 原価: 0, 解決方法: 'no_sku' });
      continue;
    }

    let product = productsMap.get(sku);
    let method = 'direct';
    if (!product) {
      const maps = skuMap.get(sku);
      if (maps && maps.length > 0) {
        if (maps.length === 1) {
          product = productsMap.get((maps[0].ne_code || '').toLowerCase());
          method = 'sku_map';
        } else {
          // セット商品: 構成品を解決して原価合算
          // 数量検証: NULL→1扱い、0/負数/非整数→invalid_quantity で hard fail
          const components = maps.map(m => {
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

          const invalidQty = components.filter(c => c.qty === null);
          if (invalidQty.length > 0) {
            resolved.push({ ...row, 商品コード: null, 原価: 0, 解決方法: 'invalid_quantity' });
            conflicts.push({ sku, type: 'invalid_quantity', invalidQty: invalidQty.map(c => ({ ne_code: c.ne_code, rawQty: c.rawQty })) });
            continue;
          }

          const missing = components.filter(c => !c.product).map(c => c.ne_code);
          if (missing.length > 0) {
            resolved.push({ ...row, 商品コード: null, 原価: 0, 解決方法: 'partial_component' });
            conflicts.push({ sku, type: 'partial_component', missing });
            continue;
          }
          const totalGenka = components.reduce((sum, c) => sum + (c.product.原価 || 0) * (c.qty || 1), 0);
          resolved.push({
            ...row,
            商品コード: components[0].product.商品コード,
            原価: totalGenka,
            解決方法: 'set_components',
            components: components.map(c => ({ ne_code: c.ne_code, qty: c.qty })),
          });
          continue;
        }
      }
    }

    if (product) {
      resolved.push({
        ...row,
        商品コード: product.商品コード,
        原価: product.原価 || 0,
        解決方法: method,
      });
    } else {
      resolved.push({ ...row, 商品コード: null, 原価: 0, 解決方法: 'unresolved' });
      const existing = unresolved.get(sku) || {
        sku, name: row.説明 || '', count: 0,
        quantity: 0, usd_amount: 0,
      };
      existing.count++;
      existing.quantity += row.数量 || 0;
      existing.usd_amount += row.total || 0;
      unresolved.set(sku, existing);
    }
  }

  // 原価ゼロ警告
  const zeroGenka = new Map();
  for (const r of resolved) {
    if (r.商品コード && (r.原価 === 0 || r.原価 === null)) {
      const key = r.商品コード;
      const e = zeroGenka.get(key) || {
        商品コード: key, sku: r.sku || '', 商品名: r.説明 || '',
        数量合計: 0, usd売上合計: 0, count: 0,
      };
      e.数量合計 += r.数量 || 0;
      e['usd売上合計'] += r['product sales'] || 0;
      e.count++;
      zeroGenka.set(key, e);
    }
  }

  return {
    resolved,
    unresolved: [...unresolved.values()],
    zeroGenka: [...zeroGenka.values()],
    conflicts,
  };
}

// ─── 集計 ───

function aggregate(resolved, rate) {
  // USDベース集計 — 全行の各列を合算
  const usd = {};
  USD_COLS.forEach(c => usd[c] = 0);
  let costTotalJpy = 0;  // 原価合計(円・税抜・qty×cost)
  let rowCount = 0;

  for (const r of resolved) {
    USD_COLS.forEach(c => { usd[c] += r[c] || 0; });
    costTotalJpy += (r.原価 || 0) * (r.数量 || 0);
    rowCount++;
  }

  // JPY換算(total以外は*rate、原価合計はそのまま円)
  const jpy = {};
  USD_COLS.forEach(c => jpy[c] = (usd[c] || 0) * rate);

  // 管理会計用15列(セグメント4行)
  const mgmt = {
    '商品売上': jpy['product sales'],
    '商品の売上税': jpy['product sales tax'],
    '配送料': jpy['shipping credits'],
    '配送料の税金': jpy['shipping credits tax'],
    'ギフト包装手数料': jpy['gift wrap credits'],
    'ギフト包装クレジットの税金': jpy['giftwrap credits tax'],
    'Amazonポイントの費用': jpy['Regulatory Fee'] + jpy['Tax On Regulatory Fee'],
    'プロモーション割引額': jpy['promotional rebates'],
    'プロモーション割引の税金': jpy['promotional rebates tax'],
    '手数料': jpy['marketplace withheld tax'] + jpy['selling fees'],
    'FBA手数料': jpy['fba fees'],
    'トランザクションに関するその他の手数料': jpy['other transaction fees'],
    'その他': jpy['other'],
    '合計': jpy['total'],
    '原価合計': costTotalJpy,
  };

  return { usd, jpy, mgmt, costTotalJpy, rowCount };
}

// ─── GET / — メイン画面 ───

router.get('/', (req, res) => {
  res.send(renderPage());
});

// ─── POST /upload — CSVアップロード&集計 ───

router.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });

  const rate = parseFloat(req.body.rate) || 0;
  if (!rate || rate <= 0) {
    try { fs.unlinkSync(req.file.path); } catch {}
    return res.status(400).json({ error: '為替レート(USD→JPY)を入力してください' });
  }

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
    const parsedRows = parseUsCsv(buf);
    if (parsedRows.length === 0) {
      return res.status(400).json({ error: 'データ行が見つかりません(CSVフォーマットを確認してください)' });
    }

    // 対象年月(最初の日付から)
    const firstDate = parsedRows[0].日付 || '';
    const yearMonth = firstDate.slice(0, 7);  // YYYY-MM

    const { resolved, unresolved, zeroGenka, conflicts } = resolveSkus(parsedRows, db);
    const { usd, jpy, mgmt, costTotalJpy } = aggregate(resolved, rate);

    // エビデンスCSV(明細)
    const detailCols = ['日付','type','注文番号','sku','説明','数量',
      ...USD_COLS, '商品コード','原価','解決方法'];
    let detailCsv = '\uFEFF' + detailCols.join(',') + '\n';
    for (const r of resolved) {
      const vals = detailCols.map(c => {
        const v = r[c];
        if (v === null || v === undefined) return '';
        if (typeof v === 'string' && (v.includes(',') || v.includes('"'))) {
          return '"' + v.replace(/"/g, '""') + '"';
        }
        return v;
      });
      detailCsv += vals.join(',') + '\n';
    }

    // エビデンスCSV(集計サマリー)
    let summaryCsv = '\uFEFF';
    summaryCsv += '【為替レート】\n1 USD =,' + rate + ',JPY\n\n';
    summaryCsv += '【USDベース集計】\n';
    summaryCsv += USD_COLS.join(',') + '\n';
    summaryCsv += USD_COLS.map(c => usd[c] || 0).join(',') + '\n\n';
    summaryCsv += '【JPY換算集計】\n';
    summaryCsv += USD_COLS.join(',') + '\n';
    summaryCsv += USD_COLS.map(c => Math.round(jpy[c] || 0)).join(',') + '\n\n';
    summaryCsv += '【管理会計用 セグメント4(輸出)】\n';
    summaryCsv += 'セグメント,' + MGMT_COLS.join(',') + ',広告費\n';
    summaryCsv += '4:輸出,' + MGMT_COLS.map(c => Math.round(mgmt[c] || 0)).join(',') + ',(確定時に手入力)\n';

    // /confirm でサーバ側真値として使うため集計結果も保管 (Codex 3R #1)
    const canConfirmFlag = conflicts.length === 0;
    const usResolvedCount = resolved.filter(r => r.解決方法 !== 'unresolved' && r.解決方法 !== 'no_sku' && !['partial_component','invalid_quantity'].includes(r.解決方法)).length;
    evidenceStore.set(yearMonth, {
      detail: detailCsv,
      summary: summaryCsv,
      serverState: {
        totalRows: parsedRows.length,
        resolvedCount: usResolvedCount,
        unresolvedCount: unresolved.length,
        conflictsCount: conflicts.length,
        canConfirm: canConfirmFlag,
        rate, usd, jpy, mgmt, costTotalJpy,
      },
    });

    res.json({
      yearMonth, rate,
      totalRows: parsedRows.length,
      resolvedCount: resolved.filter(r => r.解決方法 !== 'unresolved' && r.解決方法 !== 'no_sku' && !['partial_component','invalid_quantity'].includes(r.解決方法)).length,
      unresolvedSkus: unresolved,
      // 米国Amazon: 未登録SKUは原価0扱いで集計に含める(GAS互換)が、セット商品の構成品欠損(partial_component)があれば確定不可
      canConfirm: conflicts.length === 0,
      conflicts,
      usd, jpy, mgmt,
      costTotalJpy,
      zeroGenka,
      usdCols: USD_COLS,
      mgmtCols: MGMT_COLS,
    });
  } catch (e) {
    console.error('[AmazonUsaAccounting] エラー:', e.message, e.stack);
    res.status(500).json({ error: '集計処理エラー: ' + e.message });
  }
});

// ─── GET /evidence/:type/:yearMonth ───

router.get('/evidence/:type/:yearMonth', (req, res) => {
  const { type, yearMonth } = req.params;
  const ev = evidenceStore.get(yearMonth);
  if (!ev) return res.status(404).json({ error: yearMonth + ' のエビデンスがありません' });
  const csv = type === 'detail' ? ev.detail : ev.summary;
  if (!csv) return res.status(404).json({ error: 'データが見つかりません' });
  const filename = type === 'detail'
    ? 'AmazonUSA_' + yearMonth + '_明細エビデンス.csv'
    : 'AmazonUSA_' + yearMonth + '_集計サマリー.csv';
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="' + encodeURIComponent(filename) + '"');
  res.send(csv);
});

// ─── POST /confirm ───

router.post('/confirm', (req, res) => {
  const db = getMirrorDB();
  const { yearMonth, adCost, csvFilename } = req.body;
  if (!yearMonth) return res.status(400).json({ error: 'yearMonth は必須です' });

  // サーバ側保管値を真値として使う (Codex 3R #1)
  const cached = evidenceStore.get(yearMonth);
  if (!cached || !cached.serverState) {
    return res.status(400).json({ error: 'アップロード結果が見つかりません。CSVを再アップロードしてください' });
  }
  const s = cached.serverState;
  if (!s.canConfirm) {
    return res.status(400).json({ error: 'セット商品の構成品欠損(または数量不正)があるため確定できません' });
  }

  try {
    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
    db.prepare(`INSERT OR REPLACE INTO mart_amazon_usa_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       exchange_rate, usd_row, jpy_row, mgmt_row, cost_total, ad_cost, confirmed_at, csv_filename)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      yearMonth, s.totalRows, s.resolvedCount, s.unresolvedCount,
      s.rate || 0,
      JSON.stringify(s.usd || {}), JSON.stringify(s.jpy || {}),
      JSON.stringify(s.mgmt || {}), s.costTotalJpy || 0, adCost || 0,
      now, csvFilename || ''
    );
    db.prepare(`INSERT INTO mart_amazon_usa_upload_log
      (year_month, filename, total_rows, resolved_count, unresolved_count, uploaded_at)
      VALUES (?,?,?,?,?,?)
    `).run(yearMonth, csvFilename || '', s.totalRows, s.resolvedCount, s.unresolvedCount, now);
    res.json({ ok: true, yearMonth, confirmed_at: now });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── GET /history ───

router.get('/history', (req, res) => {
  const db = getMirrorDB();
  try {
    const rows = db.prepare('SELECT * FROM mart_amazon_usa_monthly_summary ORDER BY year_month DESC').all();
    const parsed = rows.map(r => ({
      ...r,
      usd_row: JSON.parse(r.usd_row || '{}'),
      jpy_row: JSON.parse(r.jpy_row || '{}'),
      mgmt_row: JSON.parse(r.mgmt_row || '{}'),
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
    const stmt = db.prepare(`INSERT OR IGNORE INTO mart_amazon_usa_monthly_summary
      (year_month, total_rows, resolved_count, unresolved_count,
       exchange_rate, usd_row, jpy_row, mgmt_row, cost_total, confirmed_at, csv_filename)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
    let inserted = 0;
    const tx = db.transaction(() => {
      for (const m of months) {
        const r = stmt.run(
          m.yearMonth, 0, 0, 0,
          m.exchangeRate || 0,
          '{}', '{}', JSON.stringify(m.mgmt || {}),
          m.costTotalJpy || 0, now, 'historical-import'
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
  <title>米国Amazon売上集計 - B-Faith</title>
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
    .rate-box{display:flex;gap:10px;align-items:center;margin-bottom:8px;flex-wrap:wrap}
    .rate-box input{width:100px;padding:4px 6px;border:1px solid #ccc;border-radius:4px}
    .rate-link{font-size:12px;color:#2980b9;text-decoration:none}
    .rate-link:hover{text-decoration:underline}
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
    <h1>米国Amazon売上集計</h1>
    <a href="/">← ポータルに戻る</a>
    <span style="margin-left:auto;font-size:12px;color:#aed6f1">全売上=セグメント4(輸出)</span>
  </div>
  <div class="wrap">
    <div class="card">
      <h2>Monthly Unified Transaction CSVアップロード</h2>
      <p class="meta">セラーセントラル(US) → Reports → Payments → Monthly Unified Transaction からダウンロードしたCSVファイル</p>

      <div class="rate-box" style="margin-top:12px">
        <label><b>為替レート (1 USD = ? JPY):</b></label>
        <input type="number" id="rate" step="0.01" placeholder="例: 156.05">
        <a href="https://kabutan.jp/stock/kabuka?code=0950" target="_blank" class="rate-link">📊 kabutanで確認する</a>
      </div>
      <div style="display:flex;gap:8px;align-items:center">
        <input type="file" id="csvFile" accept=".csv,.txt">
        <button class="btn btn-p" id="uploadBtn" onclick="doUpload()">アップロード&集計</button>
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
        <p class="meta">以下のSKUは商品マスタに未登録です。<b>原価0円として集計に含まれます</b>(GAS互換)。必要に応じてミニPC管理画面で商品マスタに登録してください。</p>
        <div id="unresolvedList"></div>
      </div>

      <div id="conflictsCard" class="card" style="display:none">
        <h2>⚠️ セット商品の解決エラー</h2>
        <p class="meta">セット商品(1 SKU = N構成品)で構成品が見つからないか、数量が不正です。確定できません。</p>
        <div id="conflictsList"></div>
      </div>

      <div class="card">
        <h2>USDベース集計</h2>
        <div id="usdTable"></div>
      </div>

      <div class="card">
        <h2>JPY換算集計</h2>
        <div id="jpyTable"></div>
      </div>

      <div class="card">
        <h2>管理会計用 セグメント4(輸出) — 円建</h2>
        <div class="rate-box" style="margin-bottom:8px">
          <label><b>広告費 (税込・円):</b></label>
          <input type="number" id="adCost" value="0" step="1" style="width:120px" oninput="updateMgmtTable()">
          <span class="meta">※セグメント4(輸出)行にのみ反映されます</span>
        </div>
        <div id="mgmtTable"></div>
      </div>

      <div id="zeroGenkaCard" class="card" style="display:none">
        <h2>⚠️ 原価ゼロで計算された商品</h2>
        <p class="meta">商品マスタの原価が0またはNULLのため、原価0円で集計されています。</p>
        <div id="zeroGenkaList"></div>
      </div>

      <div class="card" id="confirmCard">
        <h2>確定・エビデンス</h2>
        <button class="btn btn-s" id="confirmBtn" onclick="doConfirm()">この月の集計を確定</button>
        <div style="display:flex;gap:8px;margin-top:8px">
          <button class="btn btn-p" onclick="downloadEvidence('detail')">明細エビデンスCSV</button>
          <button class="btn btn-p" onclick="downloadEvidence('summary')">集計サマリーCSV</button>
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
      if (n === 0 || !n) return '0';
      const s = Math.round(n).toLocaleString();
      return n < 0 ? '<span class="negative">' + s + '</span>' : s;
    };
    const fmtUsd = n => {
      if (n === 0 || !n) return '0.00';
      const s = n.toFixed(2);
      return n < 0 ? '<span class="negative">' + s + '</span>' : s;
    };
    let lastData = null;

    async function doUpload() {
      const fileInput = document.getElementById('csvFile');
      const rate = parseFloat(document.getElementById('rate').value);
      if (!fileInput.files.length) { alert('ファイルを選択してください'); return; }
      if (!rate || rate <= 0) { alert('為替レートを入力してください'); return; }

      const btn = document.getElementById('uploadBtn');
      btn.disabled = true;
      btn.textContent = '処理中...';
      document.getElementById('uploadStatus').textContent = 'アップロード中...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);
      formData.append('rate', rate);

      try {
        const r = await fetch(location.pathname + '/upload', { method: 'POST', body: formData });
        const data = await r.json();
        if (data.error) {
          document.getElementById('uploadStatus').innerHTML = '<span class="negative">エラー: ' + data.error + '</span>';
        } else {
          showResult(data);
        }
      } catch(e) {
        document.getElementById('uploadStatus').innerHTML = '<span class="negative">エラー: ' + e.message + '</span>';
      }
      btn.disabled = false;
      btn.textContent = 'アップロード&集計';
    }

    function showResult(data) {
      lastData = data;
      document.getElementById('result').style.display = 'block';
      document.getElementById('uploadStatus').textContent = '';

      const hasUnresolved = data.unresolvedSkus.length > 0;
      const conflictsCount = (data.conflicts || []).length;
      const blockConfirm = conflictsCount > 0;
      let s = '<div class="' + (blockConfirm ? 'err' : (hasUnresolved ? 'warn' : 'ok')) + '">';
      s += '<b>対象年月: ' + data.yearMonth + '</b> / 為替: 1 USD = ' + data.rate + ' JPY<br>';
      s += '総行数: ' + data.totalRows + ' (Transfer除外済) / SKU解決済: ' + data.resolvedCount + ' / 未登録SKU: ' + data.unresolvedSkus.length + '件';
      if (conflictsCount > 0) s += ' / <span class="negative">セット解決エラー: ' + conflictsCount + '件</span>';
      if (blockConfirm) s += '<br><b style="color:#e74c3c">❌ セット商品の構成品欠損あり — 確定不可</b>';
      else if (hasUnresolved) s += '<br><b style="color:#d68910">⚠️ 未登録SKUあり(原価0円で集計に含まれます) — 確定可能</b>';
      else s += '<br><b style="color:#27ae60">✅ 確定可能</b>';
      s += '</div>';
      document.getElementById('summary').innerHTML = s;
      const confirmBtn = document.getElementById('confirmBtn');
      if (confirmBtn) confirmBtn.disabled = !data.canConfirm;

      // セット解決エラー (partial_component / invalid_quantity)
      if (data.conflicts && data.conflicts.length > 0) {
        document.getElementById('conflictsCard').style.display = 'block';
        const typeLabels = { partial_component: '構成品欠損', invalid_quantity: '数量不正' };
        let html = '<table class="detail-table"><tr><th>SKU</th><th>エラー種別</th><th>詳細</th></tr>';
        for (const c of data.conflicts) {
          const label = typeLabels[c.type] || c.type;
          let detail = '';
          if (c.type === 'partial_component') detail = '欠損ne_code: ' + (c.missing || []).join(', ');
          else if (c.type === 'invalid_quantity') detail = '不正数量: ' + (c.invalidQty || []).map(q => q.ne_code + '=' + q.rawQty).join(', ');
          html += '<tr><td style="text-align:left">' + c.sku + '</td><td>' + label + '</td><td style="text-align:left">' + detail + '</td></tr>';
        }
        html += '</table>';
        document.getElementById('conflictsList').innerHTML = html;
      } else {
        document.getElementById('conflictsCard').style.display = 'none';
      }

      // 未登録SKU
      if (data.unresolvedSkus.length > 0) {
        document.getElementById('unresolvedCard').style.display = 'block';
        let html = '<table><tr><th>SKU</th><th>商品名</th><th>出現行数</th><th>数量合計</th><th>USD売上合計</th></tr>';
        for (const u of data.unresolvedSkus) {
          html += '<tr>';
          html += '<td>' + u.sku + '</td>';
          html += '<td>' + (u.name || '').slice(0, 60) + '</td>';
          html += '<td class="num">' + u.count + '</td>';
          html += '<td class="num">' + u.quantity + '</td>';
          html += '<td class="num">$' + fmtUsd(u.usd_amount) + '</td>';
          html += '</tr>';
        }
        html += '</table>';
        document.getElementById('unresolvedList').innerHTML = html;
      } else {
        document.getElementById('unresolvedCard').style.display = 'none';
      }

      // USDテーブル
      let usdHtml = '<table><tr>';
      data.usdCols.forEach(c => usdHtml += '<th>' + c + '</th>');
      usdHtml += '</tr><tr>';
      data.usdCols.forEach(c => usdHtml += '<td class="num">$' + fmtUsd(data.usd[c]) + '</td>');
      usdHtml += '</tr></table>';
      document.getElementById('usdTable').innerHTML = usdHtml;

      // JPYテーブル
      let jpyHtml = '<table><tr>';
      data.usdCols.forEach(c => jpyHtml += '<th>' + c + '</th>');
      jpyHtml += '</tr><tr>';
      data.usdCols.forEach(c => jpyHtml += '<td class="num">¥' + fmt(data.jpy[c]) + '</td>');
      jpyHtml += '</tr></table>';
      document.getElementById('jpyTable').innerHTML = jpyHtml;

      // 管理会計用テーブル
      renderMgmtTable();

      // 原価ゼロ
      if (data.zeroGenka && data.zeroGenka.length > 0) {
        document.getElementById('zeroGenkaCard').style.display = 'block';
        let html = '<div class="warn"><b>' + data.zeroGenka.length + '商品</b>が原価0円で計算されています</div>';
        html += '<table class="detail-table"><tr><th>商品コード</th><th>SKU</th><th>商品名</th><th>出現行数</th><th>数量合計</th><th>USD売上合計</th></tr>';
        for (const z of data.zeroGenka) {
          html += '<tr>';
          html += '<td>' + z.商品コード + '</td>';
          html += '<td>' + (z.sku || '-') + '</td>';
          html += '<td>' + (z.商品名 || '').slice(0, 50) + '</td>';
          html += '<td class="num">' + z.count + '</td>';
          html += '<td class="num">' + z.数量合計 + '</td>';
          html += '<td class="num">$' + fmtUsd(z['usd売上合計']) + '</td>';
          html += '</tr>';
        }
        html += '</table>';
        document.getElementById('zeroGenkaList').innerHTML = html;
      } else {
        document.getElementById('zeroGenkaCard').style.display = 'none';
      }
    }

    function renderMgmtTable() {
      if (!lastData) return;
      const ad = parseFloat(document.getElementById('adCost')?.value) || 0;
      let mgHtml = '<table><tr><th>セグメント</th>';
      lastData.mgmtCols.forEach(c => mgHtml += '<th>' + c + '</th>');
      mgHtml += '<th>広告費</th></tr><tr><td>4: 輸出</td>';
      lastData.mgmtCols.forEach(c => mgHtml += '<td class="num">¥' + fmt(lastData.mgmt[c]) + '</td>');
      mgHtml += '<td class="num">¥' + fmt(ad) + '</td></tr></table>';
      document.getElementById('mgmtTable').innerHTML = mgHtml;
    }

    function updateMgmtTable() { renderMgmtTable(); }

    function downloadEvidence(type) {
      if (!lastData) { alert('先にCSVをアップロードしてください'); return; }
      window.open(location.pathname + '/evidence/' + type + '/' + lastData.yearMonth);
    }

    async function doConfirm() {
      if (!lastData) { alert('先にCSVをアップロードしてください'); return; }
      if (!lastData.canConfirm) {
        alert('セット商品の構成品欠損(partial_component)があるため確定できません。商品マスタを修正してください。');
        return;
      }
      let msg = lastData.yearMonth + ' の集計を確定しますか?\\n為替レート: ' + lastData.rate + ' JPY/USD';
      if (lastData.unresolvedSkus.length > 0) {
        msg += '\\n\\n⚠️ 未登録SKU ' + lastData.unresolvedSkus.length + '件あり(原価0円で集計に含まれます)';
      }
      if (!confirm(msg)) return;
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
          document.getElementById('confirmStatus').innerHTML = '<span style="color:#27ae60">OK ' + lastData.yearMonth + ' 確定済 (' + result.confirmed_at + ')</span>';
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
        const mgmtCols = ${JSON.stringify(MGMT_COLS)};
        let html = '';
        for (let i = 0; i < rows.length; i++) {
          const row = rows[i];
          const mg = row.mgmt_row || {};
          const total = mg['合計'] || 0;
          const sales = mg['商品売上'] || 0;
          const cost = mg['原価合計'] || row.cost_total || 0;
          const ad = Math.round(row.ad_cost || 0);

          html += '<div class="acc-header" onclick="toggleAcc(this)" data-idx="' + i + '">';
          html += '<span><b>' + row.year_month + '</b> — 商品売上: ¥' + Math.round(sales).toLocaleString()
            + ' / 合計: ¥' + Math.round(total).toLocaleString()
            + ' / 原価: ¥' + Math.round(cost).toLocaleString()
            + (ad ? ' / 広告費: ¥' + ad.toLocaleString() : '')
            + (row.exchange_rate ? ' / 為替: ' + row.exchange_rate : '')
            + ' <span class="meta">(' + (row.confirmed_at || '') + ')</span></span>';
          html += '<span class="arrow">&#9654;</span></div>';
          html += '<div class="acc-body" id="acc-' + i + '">';
          html += '<h3 style="font-size:13px;color:#555;margin-bottom:4px">管理会計用 セグメント4(輸出) — 円建</h3>';
          html += '<table><tr><th>セグメント</th>';
          mgmtCols.forEach(c => html += '<th>' + c + '</th>');
          html += '<th>広告費</th></tr><tr><td>4: 輸出</td>';
          mgmtCols.forEach(c => html += '<td class="num">¥' + fmt(mg[c] || 0) + '</td>');
          html += '<td class="num">¥' + fmt(ad) + '</td></tr></table>';
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

export default router;
