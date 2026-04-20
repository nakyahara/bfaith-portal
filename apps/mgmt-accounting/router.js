/**
 * 売上分類別粗利集計ツール
 *
 * 各モール売上集計ツール（mart_monthly_segment_sales）のデータと
 * 手入力の運賃・資材費から、売上分類別の変動費・粗利益を集計する。
 *
 * ビュー:
 *   1. 運賃・資材費入力（手入力）
 *   2. 月次PL（PF×セグメント別詳細）
 *   3. 年間PL（売上分類別×月サマリー）
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();

// B-Faith会計年度: 7月始まり
function getFiscalYear(yearMonth) {
  const [y, m] = yearMonth.split('-').map(Number);
  return m >= 7 ? y - 2017 : y - 2018; // 第1期=2018年7月開始
}
function getFiscalMonth(yearMonth) {
  const m = parseInt(yearMonth.split('-')[1]);
  return m >= 7 ? m - 6 : m + 6;
}
function getFiscalYearMonths(fiscalYear) {
  const startCalYear = fiscalYear + 2017;
  const months = [];
  for (let m = 7; m <= 12; m++) months.push(`${startCalYear}-${String(m).padStart(2, '0')}`);
  for (let m = 1; m <= 6; m++) months.push(`${startCalYear + 1}-${String(m).padStart(2, '0')}`);
  return months;
}

// 運送会社・仕入先のプリセット（Excel「運賃集計」「輸出運賃」「梱包資材費」シートのヘッダーに合わせる）
const CARRIERS = ['FBA運賃', 'RSL費用', 'ヤマト', 'ヤマト2', '佐川', '西濃', '福山通運', '郵便局（UPSIDER1）', '郵便局（UPSIDER2）', 'クリックポスト'];
const EXPORT_CARRIERS = ['TNK運賃(輸出)', 'FBA運賃(輸出)'];
const SUPPLIERS = ['ヤマト', 'ダイワハイテックス', 'アップサイダーカード', 'ダンボールワン', 'アスクル', 'シモジマ', 'ラクスル', '郵便局（レタパ）', 'イージーパック', 'スズヤエビス堂', 'アップサイダーカード2', '五洋パッケージ'];

const MALL_NAMES = {
  amazon_jp: 'Amazon', rakuten: '楽天', yahoo: 'Yahoo!',
  aupay: 'auPay', qoo10: 'Qoo10', linegift: 'LINEギフト',
  mercari: 'メルカリshops', dshop: 'Dショッピング', amazon_usa: '米国Amazon',
};
const SEGMENT_NAMES = { 1: '自社商品', 2: '取引先限定商品', 3: '仕入れ商品', 4: '米国Amazon輸出' };

// ─── API ───

// 認証ガード（APIキー認証 or セッション認証、未設定時はスキップ）
function checkAuth(req, res) {
  const key = process.env.MIRROR_SYNC_KEY;
  const provided = req.headers['x-sync-key'];
  const sessionOK = req.session?.authenticated;
  if (key && !sessionOK && provided !== key) {
    res.status(401).json({ error: 'Invalid sync key' });
    return false;
  }
  return true;
}

// 過去データ一括インポート（運賃・資材費・売上）
router.post('/import-historical', (req, res) => {
  if (!checkAuth(req, res)) return;

  const db = getMirrorDB();
  const { freight = [], material = [], sales = [] } = req.body;
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  const freightStmt = db.prepare(`INSERT INTO mgmt_freight_costs
    (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(year_month, carrier) DO UPDATE SET amount=excluded.amount, cost_scope=excluded.cost_scope, target_segment=excluded.target_segment, target_mall_id=excluded.target_mall_id, updated_at=excluded.updated_at`);
  const materialStmt = db.prepare(`INSERT INTO mgmt_material_costs
    (year_month, supplier, amount, note, entered_by, entered_at, updated_at)
    VALUES (?,?,?,?,?,?,?)
    ON CONFLICT(year_month, supplier) DO UPDATE SET amount=excluded.amount, updated_at=excluded.updated_at`);
  const salesStmt = db.prepare(`INSERT OR REPLACE INTO mart_monthly_segment_sales
    (year_month, mall_id, segment, sales, cost, pf_fee, ad_cost, confirmed_at, source_file, logic_version)
    VALUES (?,?,?,?,?,?,?,?,?,?)`);

  const tx = db.transaction(() => {
    for (const f of freight) {
      freightStmt.run(f.year_month, f.carrier, Math.round(f.amount || 0), f.cost_scope || 'shared', f.target_segment || null, f.target_mall_id || null, f.note || null, 'historical-import', now, now);
    }
    for (const m of material) {
      materialStmt.run(m.year_month, m.supplier, Math.round(m.amount || 0), m.note || null, 'historical-import', now, now);
    }
    for (const s of sales) {
      salesStmt.run(s.year_month, s.mall_id, s.segment, Math.round(s.sales || 0), Math.round(s.cost || 0), Math.round(s.pf_fee || 0), Math.round(s.ad_cost || 0), now, 'historical-excel', 'v1');
    }
  });
  tx();

  res.json({ ok: true, freight: freight.length, material: material.length, sales: sales.length });
});

// 無効レコード削除（carrier/supplier に「合計」等が紛れ込んだ場合のクリーンアップ）
router.post('/cleanup-invalid', (req, res) => {
  if (!checkAuth(req, res)) return;
  const db = getMirrorDB();
  const bad = ['合計', '運賃合計', '運賃合計(輸出)'];
  const placeholders = bad.map(() => '?').join(',');
  const f = db.prepare(`DELETE FROM mgmt_freight_costs WHERE carrier IN (${placeholders})`).run(...bad);
  const m = db.prepare(`DELETE FROM mgmt_material_costs WHERE supplier IN (${placeholders})`).run(...bad);
  res.json({ ok: true, freight_deleted: f.changes, material_deleted: m.changes });
});

// 一括確定: 指定月を除く全月について calculate を実行
router.post('/bulk-calculate', (req, res) => {
  if (!checkAuth(req, res)) return;

  const db = getMirrorDB();
  const { exclude_months = [] } = req.body;
  const user = req.session?.email || 'historical-bulk';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  // 対象月: mart_monthly_segment_sales にデータがあり exclude に含まれない月
  const months = db.prepare('SELECT DISTINCT year_month FROM mart_monthly_segment_sales ORDER BY year_month').all()
    .map(r => r.year_month)
    .filter(m => !exclude_months.includes(m));

  const results = [];
  for (const ym of months) {
    const segSales = db.prepare('SELECT * FROM mart_monthly_segment_sales WHERE year_month = ?').all(ym);
    if (segSales.length === 0) continue;

    const freightRows = db.prepare('SELECT * FROM mgmt_freight_costs WHERE year_month = ?').all(ym);
    const materialRows = db.prepare('SELECT * FROM mgmt_material_costs WHERE year_month = ?').all(ym);
    const sharedFreight = freightRows.filter(r => r.cost_scope === 'shared').reduce((s, r) => s + r.amount, 0);
    const directFreight = freightRows.filter(r => r.cost_scope !== 'shared');
    const materialTotal = materialRows.reduce((s, r) => s + r.amount, 0);
    const salesForAlloc = segSales.filter(r => r.segment !== 4).reduce((s, r) => s + (r.sales || 0), 0);
    const salesTotal = segSales.reduce((s, r) => s + (r.sales || 0), 0);
    const fiscalYear = getFiscalYear(ym);

    const tx = db.transaction(() => {
      const plStmt = db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_pl
        (year_month, mall_id, segment, sales, sales_ratio, cost, pf_fee, ad_cost, freight, material, variable_cost, gross_profit, gross_margin, fiscal_year)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);

      for (const row of segSales) {
        const sales = Math.round(row.sales || 0);
        const cost = Math.round(row.cost || 0);
        const pfFee = Math.round(row.pf_fee || 0);
        const adCost = Math.round(row.ad_cost || 0);

        let freight = 0;
        if (row.segment === 4) {
          freight = directFreight.filter(d => d.target_segment === 4 || d.target_mall_id === 'amazon_usa').reduce((s, d) => s + d.amount, 0);
          const exportTotal = segSales.filter(r => r.segment === 4).reduce((s, r) => s + (r.sales || 0), 0);
          if (exportTotal > 0 && exportTotal !== sales) freight = Math.round(freight * sales / exportTotal);
        } else {
          freight = salesForAlloc > 0 ? Math.round(sharedFreight * sales / salesForAlloc) : 0;
        }
        const material = salesTotal > 0 ? Math.round(materialTotal * sales / salesTotal) : 0;

        const salesRatio = salesTotal > 0 ? sales / salesTotal : 0;
        const variableCost = cost + pfFee + adCost + freight + material;
        const grossProfit = sales - variableCost;
        const grossMargin = sales > 0 ? grossProfit / sales : 0;

        plStmt.run(ym, row.mall_id, row.segment, sales, salesRatio, cost, pfFee, adCost, freight, material, variableCost, grossProfit, grossMargin, fiscalYear);
      }

      db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_closing
        (year_month, fiscal_year, fiscal_month, status, freight_total, material_total, confirmed_at, confirmed_by, calc_version)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(
        ym, fiscalYear, getFiscalMonth(ym), 'confirmed',
        sharedFreight + directFreight.reduce((s, d) => s + d.amount, 0), materialTotal,
        now, user, 'v1');
    });
    tx();
    results.push({ year_month: ym, rows: segSales.length });
  }

  res.json({ ok: true, processed: results.length, results });
});

// 運賃・資材費 取得
router.get('/api/costs/:yearMonth', (req, res) => {
  const db = getMirrorDB();
  const ym = req.params.yearMonth;
  const freight = db.prepare('SELECT * FROM mgmt_freight_costs WHERE year_month = ? ORDER BY id').all(ym);
  const material = db.prepare('SELECT * FROM mgmt_material_costs WHERE year_month = ? ORDER BY id').all(ym);
  const closing = db.prepare('SELECT * FROM mgmt_monthly_closing WHERE year_month = ?').get(ym);
  res.json({ freight, material, closing });
});

// 運賃 保存（一括UPSERT）
router.post('/api/freight', (req, res) => {
  const db = getMirrorDB();
  const { year_month, items } = req.body;
  const user = req.session?.email || 'unknown';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const stmt = db.prepare(`INSERT INTO mgmt_freight_costs (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(year_month, carrier) DO UPDATE SET amount=excluded.amount, cost_scope=excluded.cost_scope, target_segment=excluded.target_segment, target_mall_id=excluded.target_mall_id, note=excluded.note, updated_at=excluded.updated_at`);
  const tx = db.transaction(() => {
    for (const item of items) {
      stmt.run(year_month, item.carrier, Math.round(item.amount || 0), item.cost_scope || 'shared', item.target_segment || null, item.target_mall_id || null, item.note || null, user, now, now);
    }
  });
  tx();
  res.json({ ok: true });
});

// 資材費 保存（一括UPSERT）
router.post('/api/material', (req, res) => {
  const db = getMirrorDB();
  const { year_month, items } = req.body;
  const user = req.session?.email || 'unknown';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const stmt = db.prepare(`INSERT INTO mgmt_material_costs (year_month, supplier, amount, note, entered_by, entered_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(year_month, supplier) DO UPDATE SET amount=excluded.amount, note=excluded.note, updated_at=excluded.updated_at`);
  const tx = db.transaction(() => {
    for (const item of items) {
      stmt.run(year_month, item.supplier, Math.round(item.amount || 0), item.note || null, user, now, now);
    }
  });
  tx();
  res.json({ ok: true });
});

// セグメント売上データ取得（既存テーブルから）
router.get('/api/segment-sales/:yearMonth', (req, res) => {
  const db = getMirrorDB();
  const rows = db.prepare('SELECT * FROM mart_monthly_segment_sales WHERE year_month = ? ORDER BY mall_id, segment').all(req.params.yearMonth);
  res.json(rows);
});

// 各モール集計テーブルから mart_monthly_segment_sales を同期生成
const MALL_TABLES = [
  { table: 'mart_amazon_monthly_summary', mall_id: 'amazon_jp', adField: 'ad_cost', feeField: null },
  { table: 'mart_rakuten_monthly_summary', mall_id: 'rakuten', adField: 'ad_cost', feeField: 'billing' },
  { table: 'mart_yahoo_monthly_summary', mall_id: 'yahoo', adField: null, feeField: 'billing' },
  { table: 'mart_aupay_monthly_summary', mall_id: 'aupay', adField: null, feeField: 'pf_fee' },
  { table: 'mart_qoo10_monthly_summary', mall_id: 'qoo10', adField: 'ad_cost', feeField: 'pf_fee' },
  { table: 'mart_linegift_monthly_summary', mall_id: 'linegift', adField: null, feeField: 'pf_fee' },
  { table: 'mart_mercari_monthly_summary', mall_id: 'mercari', adField: null, feeField: 'pf_fee' },
  { table: 'mart_amazon_usa_monthly_summary', mall_id: 'amazon_usa', adField: 'ad_cost', feeField: null },
];

router.post('/api/sync-segment-sales', (req, res) => {
  const db = getMirrorDB();
  const { year_month } = req.body;
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  let totalInserted = 0;
  let fbaFreightInserted = null;

  const insertStmt = db.prepare(`INSERT OR REPLACE INTO mart_monthly_segment_sales
    (year_month, mall_id, segment, sales, cost, pf_fee, ad_cost, confirmed_at, source_file, logic_version)
    VALUES (?,?,?,?,?,?,?,?,?,?)`);

  const freightStmt = db.prepare(`INSERT INTO mgmt_freight_costs
    (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(year_month, carrier) DO UPDATE SET amount=excluded.amount, cost_scope=excluded.cost_scope, note=excluded.note, updated_at=excluded.updated_at`);

  const tx = db.transaction(() => {
    for (const mt of MALL_TABLES) {
      let row;
      try {
        row = db.prepare(`SELECT * FROM ${mt.table} WHERE year_month = ?`).get(year_month);
      } catch { continue; }
      if (!row) continue;

      const bySegment = JSON.parse(row.by_segment || '{}');
      const excluded = JSON.parse(row.excluded || '{}');
      const adCostTotal = row[mt.adField] || 0;

      // by_segment のキー: "1", "2", "3", "other"
      const allSegs = { ...bySegment };
      // excluded のキー: "4" （輸出）
      for (const [k, v] of Object.entries(excluded)) allSegs[k] = v;

      // セグメント全体の売上合計（広告費・PF手数料按分用）
      const segSalesTotal = Object.values(allSegs).reduce((s, v) => s + (v['売上合計'] || v['合計'] || v['商品売上'] || 0), 0);

      // PF手数料の全体値（テーブルカラムから取得）
      const pfFeeTotal = mt.feeField ? (row[mt.feeField] || 0) : 0;

      // Amazon JP: FBA手数料は販売手数料ではなく運賃として扱う（Excel運用踏襲）
      // by_segment の FBA手数料（全セグメント合計、税込負数）を |x|/1.1 で税抜化し
      // mgmt_freight_costs に carrier='FBA運賃', cost_scope='shared' で自動登録
      if (mt.mall_id === 'amazon_jp') {
        // 符号付き合計を取ってから絶対値化（返金 segment が含まれる場合もネットで計算するため）
        const fbaFeeSigned = Object.values(allSegs).reduce((s, v) => s + (v['FBA手数料'] || 0), 0);
        const fbaFeeTaxInc = Math.abs(fbaFeeSigned);
        if (fbaFeeTaxInc > 0) {
          const fbaFeeTaxEx = Math.round(fbaFeeTaxInc / 1.1);
          freightStmt.run(year_month, 'FBA運賃', fbaFeeTaxEx, 'shared', null, null,
            'auto from mart_amazon_monthly_summary.by_segment.FBA手数料', 'system-sync', now, now);
          fbaFreightInserted = fbaFeeTaxEx;
        }
      }

      for (const [segKey, segData] of Object.entries(allSegs)) {
        const seg = segKey === 'other' ? null : parseInt(segKey);
        if (seg === null || isNaN(seg)) continue;

        // 売上・原価（by_segmentの構造は売上合計/原価合計が標準）
        const sales = segData['売上合計'] || segData['合計'] || segData['商品売上'] || 0;
        const cost = segData['原価合計'] || 0;

        // PF手数料計算
        let pfFee = 0;
        if (mt.mall_id === 'amazon_jp') {
          // Amazon: 販売手数料 = |手数料 + プロモーション割引額 + プロモーション割引の税金 + Amazonポイント費用|
          // （税込符号付き合計を取ってから abs → /1.1 で税抜化）。FBA手数料は運賃として別計上するため含めない。
          const signed = (segData['手数料'] || 0)
                       + (segData['プロモーション割引額'] || 0)
                       + (segData['プロモーション割引の税金'] || 0)
                       + (segData['Amazonポイント費用'] || 0);
          pfFee = Math.round(Math.abs(signed) / 1.1);
        } else if (segData['手数料'] !== undefined || segData['FBA手数料'] !== undefined) {
          pfFee += segData['手数料'] || 0;
          pfFee += segData['FBA手数料'] || 0;
          if (segData['トランザクション他'] !== undefined) pfFee += Math.abs(segData['トランザクション他'] || 0);
        } else {
          // 全体PF手数料を売上按分
          const segRatio = segSalesTotal > 0 ? sales / segSalesTotal : 0;
          pfFee = Math.round(pfFeeTotal * segRatio);
        }

        // 広告費: 全体広告費をセグメント売上比で按分
        const segRatio = segSalesTotal > 0 ? sales / segSalesTotal : 0;
        const adCost = Math.round(adCostTotal * segRatio);

        insertStmt.run(year_month, mt.mall_id, seg, Math.round(sales), Math.round(cost), Math.round(pfFee), adCost, now, mt.table, 'v1');
        totalInserted++;
      }
    }
  });
  tx();
  res.json({ ok: true, inserted: totalInserted, fba_freight_tax_excluded: fbaFreightInserted });
});

// 集計計算＆確定
router.post('/api/calculate', (req, res) => {
  const db = getMirrorDB();
  const { year_month } = req.body;
  const user = req.session?.email || 'unknown';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  // 1. セグメント売上を取得
  const segSales = db.prepare('SELECT * FROM mart_monthly_segment_sales WHERE year_month = ?').all(year_month);
  if (segSales.length === 0) return res.status(400).json({ error: '売上データがありません' });

  // 2. 運賃・資材費を取得
  const freightRows = db.prepare('SELECT * FROM mgmt_freight_costs WHERE year_month = ?').all(year_month);
  const materialRows = db.prepare('SELECT * FROM mgmt_material_costs WHERE year_month = ?').all(year_month);

  // 共通運賃（按分対象）と直課運賃を分離
  const sharedFreight = freightRows.filter(r => r.cost_scope === 'shared').reduce((s, r) => s + r.amount, 0);
  const directFreight = freightRows.filter(r => r.cost_scope !== 'shared');
  const materialTotal = materialRows.reduce((s, r) => s + r.amount, 0);

  // 3. 売上合計（按分ベース: shared scope対象のみ = segment 1,2,3）
  const salesForAlloc = segSales.filter(r => r.segment !== 4).reduce((s, r) => s + (r.sales || 0), 0);
  const salesTotal = segSales.reduce((s, r) => s + (r.sales || 0), 0);

  const fiscalYear = getFiscalYear(year_month);

  // 4. PL行を生成
  const plRows = segSales.map(row => {
    const sales = Math.round(row.sales || 0);
    const cost = Math.round(row.cost || 0);
    const pfFee = Math.round(row.pf_fee || 0);
    const adCost = Math.round(row.ad_cost || 0);

    let freight = 0;
    if (row.segment === 4) {
      // 輸出: 直課運賃のみ
      freight = directFreight
        .filter(d => d.target_segment === 4 || d.target_mall_id === 'amazon_usa')
        .reduce((s, d) => s + d.amount, 0);
      // 輸出セグメントが複数行ある場合は売上比で按分（通常1行だが安全策）
      const exportTotal = segSales.filter(r => r.segment === 4).reduce((s, r) => s + (r.sales || 0), 0);
      if (exportTotal > 0 && exportTotal !== sales) {
        freight = Math.round(freight * sales / exportTotal);
      }
    } else {
      // 国内: 共通運賃を売上按分
      freight = salesForAlloc > 0 ? Math.round(sharedFreight * sales / salesForAlloc) : 0;
    }

    // 資材費: 全セグメント売上で按分
    const material = salesTotal > 0 ? Math.round(materialTotal * sales / salesTotal) : 0;

    const salesRatio = salesTotal > 0 ? sales / salesTotal : 0;
    const variableCost = cost + pfFee + adCost + freight + material;
    const grossProfit = sales - variableCost;
    const grossMargin = sales > 0 ? grossProfit / sales : 0;

    return {
      year_month, mall_id: row.mall_id, segment: row.segment,
      sales, sales_ratio: salesRatio, cost, pf_fee: pfFee, ad_cost: adCost,
      freight, material, variable_cost: variableCost,
      gross_profit: grossProfit, gross_margin: grossMargin, fiscal_year: fiscalYear,
    };
  });

  // 5. DB保存
  const tx = db.transaction(() => {
    // PL行
    const plStmt = db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_pl
      (year_month, mall_id, segment, sales, sales_ratio, cost, pf_fee, ad_cost, freight, material, variable_cost, gross_profit, gross_margin, fiscal_year)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    for (const r of plRows) {
      plStmt.run(r.year_month, r.mall_id, r.segment, r.sales, r.sales_ratio, r.cost, r.pf_fee, r.ad_cost, r.freight, r.material, r.variable_cost, r.gross_profit, r.gross_margin, r.fiscal_year);
    }
    // 締めヘッダ
    db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_closing
      (year_month, fiscal_year, fiscal_month, status, freight_total, material_total, confirmed_at, confirmed_by, calc_version)
      VALUES (?,?,?,?,?,?,?,?,?)`).run(
      year_month, fiscalYear, getFiscalMonth(year_month), 'confirmed',
      sharedFreight + directFreight.reduce((s, d) => s + d.amount, 0), materialTotal,
      now, user, 'v1');
    // 互換: mart_monthly_shared_costs にも書き込み
    db.prepare(`INSERT OR REPLACE INTO mart_monthly_shared_costs
      (year_month, freight_total, material_total, confirmed_at, freight_detail, material_detail)
      VALUES (?,?,?,?,?,?)`).run(
      year_month,
      sharedFreight + directFreight.reduce((s, d) => s + d.amount, 0), materialTotal, now,
      JSON.stringify(Object.fromEntries(freightRows.map(r => [r.carrier, r.amount]))),
      JSON.stringify(Object.fromEntries(materialRows.map(r => [r.supplier, r.amount]))));
  });
  tx();

  res.json({ ok: true, rows: plRows.length, freight_total: sharedFreight + directFreight.reduce((s, d) => s + d.amount, 0), material_total: materialTotal });
});

// 月次PL取得
router.get('/api/monthly-pl/:yearMonth', (req, res) => {
  const db = getMirrorDB();
  const rows = db.prepare('SELECT * FROM mgmt_monthly_pl WHERE year_month = ? ORDER BY mall_id, segment').all(req.params.yearMonth);
  const closing = db.prepare('SELECT * FROM mgmt_monthly_closing WHERE year_month = ?').get(req.params.yearMonth);
  res.json({ rows, closing });
});

// 年間PL取得（指定会計年度）
router.get('/api/annual-pl/:fiscalYear', (req, res) => {
  const db = getMirrorDB();
  const fy = parseInt(req.params.fiscalYear);
  const months = getFiscalYearMonths(fy);

  // セグメント別×月で集約
  const rows = db.prepare(`
    SELECT year_month, segment,
      SUM(sales) as sales, SUM(cost) as cost, SUM(pf_fee) as pf_fee,
      SUM(ad_cost) as ad_cost, SUM(freight) as freight, SUM(material) as material,
      SUM(variable_cost) as variable_cost, SUM(gross_profit) as gross_profit
    FROM mgmt_monthly_pl WHERE fiscal_year = ?
    GROUP BY year_month, segment ORDER BY year_month, segment
  `).all(fy);

  // 締めステータス
  const closings = db.prepare('SELECT year_month, status, confirmed_at FROM mgmt_monthly_closing WHERE fiscal_year = ?').all(fy);

  res.json({ fiscal_year: fy, months, rows, closings, label: `第${fy}期` });
});

// ヒストリカル統合取得（グラフ用）
router.get('/api/historical', (req, res) => {
  const db = getMirrorDB();
  const limit = parseInt(req.query.months) || 48;

  // 直近N ヶ月の月リストを取得（全データソースを結合）
  const monthsSet = new Set();
  for (const t of ['mgmt_freight_costs', 'mgmt_material_costs', 'mart_monthly_segment_sales', 'mgmt_monthly_pl']) {
    try {
      const rows = db.prepare(`SELECT DISTINCT year_month FROM ${t}`).all();
      for (const r of rows) monthsSet.add(r.year_month);
    } catch {}
  }
  const months = Array.from(monthsSet).sort().slice(-limit);
  if (months.length === 0) return res.json({ months: [], freight: [], material: [], sales: [], pl: [] });

  const placeholders = months.map(() => '?').join(',');

  // 運賃：月×carrier
  const freight = db.prepare(`SELECT year_month, carrier, cost_scope, SUM(amount) as amount
    FROM mgmt_freight_costs WHERE year_month IN (${placeholders})
    GROUP BY year_month, carrier, cost_scope ORDER BY year_month`).all(...months);

  // 資材費：月×supplier
  const material = db.prepare(`SELECT year_month, supplier, SUM(amount) as amount
    FROM mgmt_material_costs WHERE year_month IN (${placeholders})
    GROUP BY year_month, supplier ORDER BY year_month`).all(...months);

  // 売上：月×mall_id
  const sales = db.prepare(`SELECT year_month, mall_id, SUM(sales) as sales, SUM(cost) as cost, SUM(pf_fee) as pf_fee, SUM(ad_cost) as ad_cost
    FROM mart_monthly_segment_sales WHERE year_month IN (${placeholders})
    GROUP BY year_month, mall_id ORDER BY year_month`).all(...months);

  // PL：月×segment（粗利率用）
  const pl = db.prepare(`SELECT year_month, segment, SUM(sales) as sales, SUM(gross_profit) as gross_profit, SUM(variable_cost) as variable_cost
    FROM mgmt_monthly_pl WHERE year_month IN (${placeholders})
    GROUP BY year_month, segment ORDER BY year_month`).all(...months);

  res.json({ months, freight, material, sales, pl });
});

// 利用可能な会計年度一覧
router.get('/api/fiscal-years', (req, res) => {
  const db = getMirrorDB();
  // セグメント売上データがある期間から算出
  const range = db.prepare('SELECT MIN(year_month) as min_ym, MAX(year_month) as max_ym FROM mart_monthly_segment_sales').get();
  if (!range?.min_ym) return res.json([]);
  const minFY = getFiscalYear(range.min_ym);
  const maxFY = getFiscalYear(range.max_ym);
  const years = [];
  for (let fy = maxFY; fy >= minFY; fy--) {
    years.push({ value: fy, label: `第${fy}期` });
  }
  res.json(years);
});

// ─── HTML ───

router.get('/', (req, res) => {
  res.send(renderPage(req));
});

function renderPage(req) {
  return `<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>売上分類別粗利集計</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; }
.header { background: #1a1a2e; color: #fff; padding: 16px 24px; display: flex; align-items: center; gap: 16px; }
.header a { color: #aaa; text-decoration: none; font-size: 14px; }
.header h1 { font-size: 20px; font-weight: 600; }
.tabs { display: flex; background: #fff; border-bottom: 2px solid #e0e0e0; padding: 0 24px; }
.tab { padding: 12px 24px; cursor: pointer; border-bottom: 3px solid transparent; font-size: 14px; font-weight: 500; color: #666; }
.tab.active { color: #1a73e8; border-bottom-color: #1a73e8; }
.tab:hover { background: #f0f4ff; }
.container { max-width: 1400px; margin: 0 auto; padding: 24px; }
.controls { display: flex; gap: 16px; align-items: center; margin-bottom: 20px; flex-wrap: wrap; }
.controls label { font-weight: 500; font-size: 14px; }
.controls select, .controls input[type="month"] { padding: 8px 12px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
.btn { padding: 8px 20px; border: none; border-radius: 6px; font-size: 14px; font-weight: 500; cursor: pointer; }
.btn-primary { background: #1a73e8; color: #fff; }
.btn-primary:hover { background: #1557b0; }
.btn-success { background: #34a853; color: #fff; }
.btn-success:hover { background: #2d8e47; }
.btn-outline { background: #fff; color: #333; border: 1px solid #ccc; }
.btn-outline:hover { background: #f5f5f5; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.card { background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 20px; margin-bottom: 20px; }
.card h3 { font-size: 16px; margin-bottom: 16px; color: #1a1a2e; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { padding: 8px 10px; text-align: right; border-bottom: 1px solid #eee; }
th { background: #f8f9fa; font-weight: 600; position: sticky; top: 0; color: #555; }
td:first-child, th:first-child { text-align: left; }
tr:hover { background: #f0f4ff; }
.input-amount { width: 120px; padding: 6px 8px; border: 1px solid #ddd; border-radius: 4px; text-align: right; font-size: 13px; }
.input-amount:focus { border-color: #1a73e8; outline: none; }
.total-row { font-weight: 700; background: #f0f4ff !important; }
.total-row td { border-top: 2px solid #1a73e8; }
.status-badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 500; }
.status-confirmed { background: #e6f4ea; color: #1e8e3e; }
.status-draft { background: #fef7e0; color: #b06000; }
.negative { color: #d93025; }
.positive { color: #1e8e3e; }
.note-text { font-size: 12px; color: #888; margin-top: 8px; }
.hidden { display: none; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 20px; }
.summary-item { background: #fff; border-radius: 8px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.summary-item .label { font-size: 12px; color: #666; }
.summary-item .value { font-size: 24px; font-weight: 700; margin-top: 4px; }
.toast { position: fixed; bottom: 24px; right: 24px; background: #323232; color: #fff; padding: 12px 24px; border-radius: 8px; font-size: 14px; z-index: 1000; display: none; }
@media (max-width: 768px) {
  .container { padding: 12px; }
  table { font-size: 11px; }
  th, td { padding: 6px 4px; }
}
</style>
</head>
<body>
<div class="header">
  <a href="/">← ポータル</a>
  <h1>売上分類別粗利集計</h1>
</div>
<div class="tabs">
  <div class="tab active" data-tab="costs">運賃・資材費入力</div>
  <div class="tab" data-tab="monthly">月次PL</div>
  <div class="tab" data-tab="annual">年間PL</div>
  <div class="tab" data-tab="history">ヒストリカル</div>
</div>
<div class="container">

<!-- ===== タブ1: 運賃・資材費入力 ===== -->
<div id="tab-costs">
  <div class="controls">
    <label>対象月:</label>
    <input type="month" id="costMonth" />
    <button class="btn btn-primary" onclick="loadCosts()">読込</button>
    <button class="btn btn-success" onclick="saveCosts()">保存</button>
    <button class="btn btn-outline" onclick="syncSegmentSales()">売上同期</button>
    <button class="btn btn-success" onclick="doCalculate()" id="btnCalc">集計確定</button>
    <span id="closingStatus"></span>
  </div>

  <div class="card">
    <h3>運賃（国内共通）</h3>
    <table>
      <thead><tr><th>運送会社</th><th>金額（税込）</th><th>備考</th></tr></thead>
      <tbody id="freightBody"></tbody>
      <tfoot><tr class="total-row"><td>合計</td><td id="freightTotal">0</td><td></td></tr></tfoot>
    </table>
  </div>

  <div class="card">
    <h3>運賃（輸出専用）</h3>
    <table>
      <thead><tr><th>運送会社</th><th>金額（税込）</th><th>備考</th></tr></thead>
      <tbody id="exportFreightBody"></tbody>
      <tfoot><tr class="total-row"><td>合計</td><td id="exportFreightTotal">0</td><td></td></tr></tfoot>
    </table>
  </div>

  <div class="card">
    <h3>梱包資材費</h3>
    <table>
      <thead><tr><th>仕入先</th><th>金額（税込）</th><th>備考</th></tr></thead>
      <tbody id="materialBody"></tbody>
      <tfoot><tr class="total-row"><td>合計</td><td id="materialTotal">0</td><td></td></tr></tfoot>
    </table>
  </div>
  <p class="note-text">※ 粗利分析は現行原価・現行料率ベースの管理指標であり、過去時点の再現値ではありません。</p>
</div>

<!-- ===== タブ2: 月次PL ===== -->
<div id="tab-monthly" class="hidden">
  <div class="controls">
    <label>対象月:</label>
    <input type="month" id="plMonth" />
    <button class="btn btn-primary" onclick="loadMonthlyPL()">表示</button>
    <span id="monthlyStatus"></span>
  </div>
  <div id="monthlySummary" class="summary-grid"></div>
  <div class="card">
    <h3>PF×セグメント別 月次PL</h3>
    <div style="overflow-x:auto;">
      <table id="monthlyTable">
        <thead><tr>
          <th>PF</th><th>セグメント</th><th>売上高</th><th>売上比率</th>
          <th>仕入原価</th><th>原価率</th><th>PF手数料</th><th>PF率</th>
          <th>広告費</th><th>広告率</th><th>運賃(按分)</th><th>運賃率</th>
          <th>資材費(按分)</th><th>資材率</th><th>変動費計</th><th>粗利益</th><th>粗利率</th>
        </tr></thead>
        <tbody id="monthlyBody"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- ===== タブ3: 年間PL ===== -->
<div id="tab-annual" class="hidden">
  <div class="controls">
    <label>会計年度:</label>
    <select id="fySelect"></select>
    <button class="btn btn-primary" onclick="loadAnnualPL()">表示</button>
  </div>
  <div id="annualSummary" class="summary-grid"></div>
  <div class="card">
    <h3 id="annualTitle">年間PL</h3>
    <div style="overflow-x:auto;">
      <table id="annualTable"></table>
    </div>
  </div>
</div>

<!-- ===== タブ4: ヒストリカル ===== -->
<div id="tab-history" class="hidden">
  <div class="controls">
    <label>表示期間:</label>
    <select id="histMonths">
      <option value="12">直近12ヶ月</option>
      <option value="24">直近24ヶ月</option>
      <option value="48" selected>直近48ヶ月</option>
      <option value="0">全期間</option>
    </select>
    <button class="btn btn-primary" onclick="loadHistorical()">更新</button>
    <span id="histInfo" style="color:#666;font-size:13px"></span>
  </div>
  <div id="histSummary" class="summary-grid"></div>
  <div class="card">
    <h3>📈 月次売上推移（モール別）</h3>
    <div style="position:relative;height:320px;"><canvas id="chartSales"></canvas></div>
  </div>
  <div class="card">
    <h3>📊 月次粗利益・粗利率推移</h3>
    <div style="position:relative;height:320px;"><canvas id="chartProfit"></canvas></div>
  </div>
  <div class="card">
    <h3>🚚 月次運賃推移（運送会社別）</h3>
    <div style="position:relative;height:320px;"><canvas id="chartFreight"></canvas></div>
  </div>
  <div class="card">
    <h3>📦 月次資材費推移（仕入先別）</h3>
    <div style="position:relative;height:320px;"><canvas id="chartMaterial"></canvas></div>
  </div>
  <div class="card">
    <h3>🥧 セグメント別売上シェア（直近月）</h3>
    <div style="position:relative;height:320px;display:flex;align-items:center;justify-content:center;"><canvas id="chartSegShare" style="max-height:320px;"></canvas></div>
  </div>
</div>

</div>
<div class="toast" id="toast"></div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>

<script>
const MALL_NAMES = ${JSON.stringify(MALL_NAMES)};
const SEGMENT_NAMES = ${JSON.stringify(SEGMENT_NAMES)};
const CARRIERS = ${JSON.stringify(CARRIERS)};
const EXPORT_CARRIERS = ${JSON.stringify(EXPORT_CARRIERS)};
const SUPPLIERS = ${JSON.stringify(SUPPLIERS)};

// ─── ユーティリティ ───
const fmt = n => (n || 0).toLocaleString('ja-JP');
const fmtPct = n => ((n || 0) * 100).toFixed(1) + '%';
const fmtRatio = (n, d) => d > 0 ? fmtPct(n / d) : '-';
const clsVal = n => n < 0 ? 'negative' : n > 0 ? 'positive' : '';

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg; t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}

// ─── タブ切替 ───
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    document.querySelectorAll('[id^="tab-"]').forEach(p => p.classList.add('hidden'));
    document.getElementById('tab-' + tab.dataset.tab).classList.remove('hidden');
    if (tab.dataset.tab === 'annual' && !document.getElementById('fySelect').value) loadFiscalYears();
    if (tab.dataset.tab === 'history' && !window._histLoaded) { window._histLoaded = true; loadHistorical(); }
  });
});

// APIベースパス（相対パスだと/apps/mgmt-accountingにtrailing slashが無い時に壊れる）
const BASE = '/apps/mgmt-accounting';

// 初期値: 先月
const now = new Date();
now.setMonth(now.getMonth() - 1);
const defaultYM = now.toISOString().slice(0, 7);
document.getElementById('costMonth').value = defaultYM;
document.getElementById('plMonth').value = defaultYM;

// ─── タブ1: 運賃・資材費 ───
// DBは税抜保存、UIは税込表示（税率10%固定）
const TAX_RATE = 1.1;
const toTaxIn = v => Math.round((v || 0) * TAX_RATE);
const toTaxEx = v => Math.round((v || 0) / TAX_RATE);

function buildCostRows(containerId, names, data, keyField) {
  const tbody = document.getElementById(containerId);
  tbody.innerHTML = '';
  for (const name of names) {
    const existing = data.find(d => d[keyField] === name);
    const amountInc = existing ? toTaxIn(existing.amount) : 0; // 税抜→税込表示
    const note = existing ? (existing.note || '') : '';
    const tr = document.createElement('tr');
    tr.innerHTML = '<td>' + name + '</td>'
      + '<td><input type="number" class="input-amount" data-name="' + name + '" value="' + amountInc + '" onchange="updateTotals()"></td>'
      + '<td><input type="text" style="width:150px;padding:6px;border:1px solid #ddd;border-radius:4px;font-size:13px;" data-note="' + name + '" value="' + note + '"></td>';
    tbody.appendChild(tr);
  }
}

function updateTotals() {
  let ft = 0, et = 0, mt = 0;
  document.querySelectorAll('#freightBody .input-amount').forEach(i => ft += Number(i.value) || 0);
  document.querySelectorAll('#exportFreightBody .input-amount').forEach(i => et += Number(i.value) || 0);
  document.querySelectorAll('#materialBody .input-amount').forEach(i => mt += Number(i.value) || 0);
  document.getElementById('freightTotal').textContent = fmt(ft);
  document.getElementById('exportFreightTotal').textContent = fmt(et);
  document.getElementById('materialTotal').textContent = fmt(mt);
}

async function loadCosts() {
  const ym = document.getElementById('costMonth').value;
  if (!ym) return;
  let data;
  try {
    const res = await fetch(BASE + '/api/costs/' + ym);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    data = await res.json();
  } catch (e) {
    toast('読込失敗: ' + e.message);
    data = { freight: [], material: [], closing: null };
  }
  buildCostRows('freightBody', CARRIERS, (data.freight || []).filter(f => f.cost_scope === 'shared'), 'carrier');
  buildCostRows('exportFreightBody', EXPORT_CARRIERS, (data.freight || []).filter(f => f.cost_scope !== 'shared'), 'carrier');
  buildCostRows('materialBody', SUPPLIERS, data.material || [], 'supplier');
  updateTotals();
  const st = document.getElementById('closingStatus');
  if (data.closing) {
    st.innerHTML = '<span class="status-badge status-' + data.closing.status + '">' + data.closing.status + '</span> ' + (data.closing.confirmed_at || '');
  } else {
    st.innerHTML = '<span class="status-badge status-draft">未確定</span>';
  }
}

async function saveCosts() {
  const ym = document.getElementById('costMonth').value;
  if (!ym) return;
  // 運賃（国内）入力は税込→税抜で保存
  const freightItems = [];
  document.querySelectorAll('#freightBody .input-amount').forEach(i => {
    freightItems.push({ carrier: i.dataset.name, amount: toTaxEx(Number(i.value) || 0), cost_scope: 'shared' });
  });
  // 運賃（輸出）
  document.querySelectorAll('#exportFreightBody .input-amount').forEach(i => {
    freightItems.push({ carrier: i.dataset.name, amount: toTaxEx(Number(i.value) || 0), cost_scope: 'export_only', target_segment: 4, target_mall_id: 'amazon_usa' });
  });
  // 備考を追加
  freightItems.forEach(item => {
    const noteEl = document.querySelector('[data-note="' + item.carrier + '"]');
    if (noteEl) item.note = noteEl.value;
  });
  await fetch(BASE + '/api/freight', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ year_month: ym, items: freightItems }) });

  // 資材費: 税込→税抜
  const materialItems = [];
  document.querySelectorAll('#materialBody .input-amount').forEach(i => {
    const noteEl = document.querySelector('#materialBody [data-note="' + i.dataset.name + '"]');
    materialItems.push({ supplier: i.dataset.name, amount: toTaxEx(Number(i.value) || 0), note: noteEl?.value || '' });
  });
  await fetch(BASE + '/api/material', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ year_month: ym, items: materialItems }) });

  toast('保存しました');
}

async function syncSegmentSales() {
  const ym = document.getElementById('costMonth').value;
  if (!ym) return;
  const res = await fetch(BASE + '/api/sync-segment-sales', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ year_month: ym }) });
  const data = await res.json();
  if (data.error) { toast('エラー: ' + data.error); return; }
  let msg = '売上同期完了（' + data.inserted + '行）';
  if (data.fba_freight_tax_excluded) {
    msg += ' / FBA運賃 自動登録: ¥' + Math.round(data.fba_freight_tax_excluded * 1.1).toLocaleString() + '（税込）';
  }
  toast(msg);
  await loadCosts();
}

async function doCalculate() {
  const ym = document.getElementById('costMonth').value;
  if (!ym) return;
  if (!confirm(ym + ' の集計を確定しますか？')) return;
  await saveCosts();
  const res = await fetch(BASE + '/api/calculate', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ year_month: ym }) });
  const data = await res.json();
  if (data.error) { toast('エラー: ' + data.error); return; }
  toast('確定しました（' + data.rows + '行）');
  loadCosts();
}

// ─── タブ2: 月次PL ───
async function loadMonthlyPL() {
  const ym = document.getElementById('plMonth').value;
  if (!ym) return;
  const res = await fetch(BASE + '/api/monthly-pl/' + ym);
  const data = await res.json();

  const st = document.getElementById('monthlyStatus');
  if (data.closing) {
    st.innerHTML = '<span class="status-badge status-' + data.closing.status + '">' + data.closing.status + '</span> ' + (data.closing.confirmed_at || '');
  } else {
    st.innerHTML = '<span class="status-badge status-draft">未確定</span>';
  }

  if (data.rows.length === 0) {
    document.getElementById('monthlyBody').innerHTML = '<tr><td colspan="17">データがありません。運賃・資材費を入力して集計確定してください。</td></tr>';
    document.getElementById('monthlySummary').innerHTML = '';
    return;
  }

  // サマリー
  const totals = data.rows.reduce((a, r) => ({
    sales: a.sales + r.sales, cost: a.cost + r.cost, pf_fee: a.pf_fee + r.pf_fee,
    ad_cost: a.ad_cost + r.ad_cost, freight: a.freight + r.freight, material: a.material + r.material,
    variable_cost: a.variable_cost + r.variable_cost, gross_profit: a.gross_profit + r.gross_profit,
  }), { sales:0, cost:0, pf_fee:0, ad_cost:0, freight:0, material:0, variable_cost:0, gross_profit:0 });

  document.getElementById('monthlySummary').innerHTML =
    '<div class="summary-item"><div class="label">売上高</div><div class="value">' + fmt(totals.sales) + '</div></div>' +
    '<div class="summary-item"><div class="label">変動費</div><div class="value">' + fmt(totals.variable_cost) + '</div></div>' +
    '<div class="summary-item"><div class="label">粗利益</div><div class="value ' + clsVal(totals.gross_profit) + '">' + fmt(totals.gross_profit) + '</div></div>' +
    '<div class="summary-item"><div class="label">粗利率</div><div class="value ' + clsVal(totals.gross_profit) + '">' + fmtRatio(totals.gross_profit, totals.sales) + '</div></div>';

  // テーブル
  let html = '';
  for (const r of data.rows) {
    html += '<tr>'
      + '<td>' + (MALL_NAMES[r.mall_id] || r.mall_id) + '</td>'
      + '<td>' + (SEGMENT_NAMES[r.segment] || r.segment) + '</td>'
      + '<td>' + fmt(r.sales) + '</td>'
      + '<td>' + fmtPct(r.sales_ratio) + '</td>'
      + '<td>' + fmt(r.cost) + '</td>'
      + '<td>' + fmtRatio(r.cost, r.sales) + '</td>'
      + '<td>' + fmt(r.pf_fee) + '</td>'
      + '<td>' + fmtRatio(r.pf_fee, r.sales) + '</td>'
      + '<td>' + fmt(r.ad_cost) + '</td>'
      + '<td>' + fmtRatio(r.ad_cost, r.sales) + '</td>'
      + '<td>' + fmt(r.freight) + '</td>'
      + '<td>' + fmtRatio(r.freight, r.sales) + '</td>'
      + '<td>' + fmt(r.material) + '</td>'
      + '<td>' + fmtRatio(r.material, r.sales) + '</td>'
      + '<td>' + fmt(r.variable_cost) + '</td>'
      + '<td class="' + clsVal(r.gross_profit) + '">' + fmt(r.gross_profit) + '</td>'
      + '<td class="' + clsVal(r.gross_profit) + '">' + fmtPct(r.gross_margin) + '</td>'
      + '</tr>';
  }
  // 合計行
  html += '<tr class="total-row">'
    + '<td>合計</td><td></td>'
    + '<td>' + fmt(totals.sales) + '</td><td>100.0%</td>'
    + '<td>' + fmt(totals.cost) + '</td><td>' + fmtRatio(totals.cost, totals.sales) + '</td>'
    + '<td>' + fmt(totals.pf_fee) + '</td><td>' + fmtRatio(totals.pf_fee, totals.sales) + '</td>'
    + '<td>' + fmt(totals.ad_cost) + '</td><td>' + fmtRatio(totals.ad_cost, totals.sales) + '</td>'
    + '<td>' + fmt(totals.freight) + '</td><td>' + fmtRatio(totals.freight, totals.sales) + '</td>'
    + '<td>' + fmt(totals.material) + '</td><td>' + fmtRatio(totals.material, totals.sales) + '</td>'
    + '<td>' + fmt(totals.variable_cost) + '</td>'
    + '<td class="' + clsVal(totals.gross_profit) + '">' + fmt(totals.gross_profit) + '</td>'
    + '<td class="' + clsVal(totals.gross_profit) + '">' + fmtRatio(totals.gross_profit, totals.sales) + '</td>'
    + '</tr>';
  document.getElementById('monthlyBody').innerHTML = html;
}

// ─── タブ3: 年間PL ───
async function loadFiscalYears() {
  const res = await fetch(BASE + '/api/fiscal-years');
  const years = await res.json();
  const sel = document.getElementById('fySelect');
  sel.innerHTML = years.map(y => '<option value="' + y.value + '">' + y.label + '</option>').join('');
}

async function loadAnnualPL() {
  const fy = document.getElementById('fySelect').value;
  if (!fy) return;
  const res = await fetch(BASE + '/api/annual-pl/' + fy);
  const data = await res.json();

  document.getElementById('annualTitle').textContent = data.label + ' 売上分類別変動費・粗利益集計';

  if (data.rows.length === 0) {
    document.getElementById('annualTable').innerHTML = '<tr><td>データがありません</td></tr>';
    document.getElementById('annualSummary').innerHTML = '';
    return;
  }

  // 月ラベル（YYYYMM形式）
  const monthLabels = data.months.map(m => m.replace('-', ''));

  // セグメント別にグループ化
  const segments = [...new Set(data.rows.map(r => r.segment))].sort();
  const closingMap = {};
  for (const c of data.closings) closingMap[c.year_month] = c;

  // ヘッダー
  let html = '<thead><tr><th>売上分類</th><th>費目</th>';
  for (const ml of monthLabels) html += '<th>' + ml + '</th>';
  html += '<th>合計</th></tr></thead><tbody>';

  let grandTotals = { sales: 0, cost: 0, pf_fee: 0, ad_cost: 0, freight: 0, material: 0, variable_cost: 0, gross_profit: 0 };

  for (const seg of segments) {
    const segRows = data.rows.filter(r => r.segment === seg);
    const byMonth = {};
    for (const r of segRows) byMonth[r.year_month] = r;

    const segName = SEGMENT_NAMES[seg] || 'セグメント' + seg;
    const fields = [
      { key: 'sales', label: '売上高' },
      { key: 'cost', label: '仕入原価', indent: true },
      { key: 'pf_fee', label: '販売手数料', indent: true },
      { key: 'ad_cost', label: '広告費', indent: true },
      { key: 'freight', label: '送料', indent: true },
      { key: 'material', label: '梱包資材費', indent: true },
      { key: 'variable_cost', label: '変動費' },
      { key: 'gross_profit', label: '粗利益' },
    ];

    for (let fi = 0; fi < fields.length; fi++) {
      const f = fields[fi];
      html += '<tr>';
      if (fi === 0) html += '<td rowspan="' + (fields.length + 1) + '">' + segName + '</td>';
      html += '<td>' + (f.indent ? '　' : '') + f.label + '</td>';
      let rowTotal = 0;
      for (const m of data.months) {
        const val = byMonth[m] ? byMonth[m][f.key] : 0;
        rowTotal += val;
        const cls = f.key === 'gross_profit' ? clsVal(val) : '';
        html += '<td class="' + cls + '">' + fmt(val) + '</td>';
      }
      const cls = f.key === 'gross_profit' ? clsVal(rowTotal) : '';
      html += '<td class="' + cls + '">' + fmt(rowTotal) + '</td></tr>';
      if (f.key !== 'variable_cost') grandTotals[f.key] = (grandTotals[f.key] || 0) + rowTotal;
      else grandTotals.variable_cost += rowTotal;
    }
    // 粗利率行
    html += '<tr><td>粗利益率</td>';
    for (const m of data.months) {
      const r = byMonth[m];
      const margin = r && r.sales > 0 ? r.gross_profit / r.sales : 0;
      html += '<td class="' + clsVal(margin) + '">' + fmtPct(margin) + '</td>';
    }
    // 年間粗利率
    const segSalesTotal = segRows.reduce((s, r) => s + r.sales, 0);
    const segProfitTotal = segRows.reduce((s, r) => s + r.gross_profit, 0);
    const segMargin = segSalesTotal > 0 ? segProfitTotal / segSalesTotal : 0;
    html += '<td class="' + clsVal(segMargin) + '">' + fmtPct(segMargin) + '</td></tr>';
  }

  // 合計セクション
  html += '<tr class="total-row"><td rowspan="10">合計</td><td>売上高</td>';
  const allByMonth = {};
  for (const r of data.rows) {
    if (!allByMonth[r.year_month]) allByMonth[r.year_month] = { sales:0, cost:0, pf_fee:0, ad_cost:0, freight:0, material:0, variable_cost:0, gross_profit:0 };
    for (const k of ['sales','cost','pf_fee','ad_cost','freight','material','variable_cost','gross_profit']) {
      allByMonth[r.year_month][k] += r[k] || 0;
    }
  }
  const totalFields = ['sales','cost','pf_fee','ad_cost','freight','material','variable_cost','gross_profit'];
  const totalLabels = ['売上高','仕入原価','販売手数料','広告費','送料','梱包資材費','変動費','粗利益'];
  for (let i = 0; i < totalFields.length; i++) {
    if (i > 0) html += '<tr class="total-row"><td>' + totalLabels[i] + '</td>';
    let rowSum = 0;
    for (const m of data.months) {
      const val = allByMonth[m] ? allByMonth[m][totalFields[i]] : 0;
      rowSum += val;
      const cls = totalFields[i] === 'gross_profit' ? clsVal(val) : '';
      html += '<td class="' + cls + '">' + fmt(val) + '</td>';
    }
    const cls = totalFields[i] === 'gross_profit' ? clsVal(rowSum) : '';
    html += '<td class="' + cls + '">' + fmt(rowSum) + '</td></tr>';
  }
  // 合計粗利率
  html += '<tr class="total-row"><td>粗利益率</td>';
  for (const m of data.months) {
    const d = allByMonth[m];
    const margin = d && d.sales > 0 ? d.gross_profit / d.sales : 0;
    html += '<td class="' + clsVal(margin) + '">' + fmtPct(margin) + '</td>';
  }
  const totalSales = Object.values(allByMonth).reduce((s, d) => s + d.sales, 0);
  const totalProfit = Object.values(allByMonth).reduce((s, d) => s + d.gross_profit, 0);
  const totalMargin = totalSales > 0 ? totalProfit / totalSales : 0;
  html += '<td class="' + clsVal(totalMargin) + '">' + fmtPct(totalMargin) + '</td></tr>';

  html += '</tbody>';
  document.getElementById('annualTable').innerHTML = html;

  // サマリー
  document.getElementById('annualSummary').innerHTML =
    '<div class="summary-item"><div class="label">年間売上</div><div class="value">' + fmt(totalSales) + '</div></div>' +
    '<div class="summary-item"><div class="label">年間粗利</div><div class="value ' + clsVal(totalProfit) + '">' + fmt(totalProfit) + '</div></div>' +
    '<div class="summary-item"><div class="label">粗利率</div><div class="value ' + clsVal(totalMargin) + '">' + fmtPct(totalMargin) + '</div></div>';
}

// ─── タブ4: ヒストリカル ───
const CHART_COLORS = ['#1a73e8', '#ea4335', '#fbbc04', '#34a853', '#ff6d01', '#46bdc6', '#9334e8', '#b31412', '#7cb342', '#d81b60', '#00acc1', '#5e35b1', '#8e24aa', '#039be5', '#43a047'];
const _charts = {};

function destroyChart(key) {
  if (_charts[key]) { _charts[key].destroy(); delete _charts[key]; }
}

function groupByMonthAndKey(rows, monthField, keyField, valueField) {
  // rows: [{year_month, key, value}, ...] → {months: [...], keys: [...], data: {key: [v1, v2, ...]}}
  const months = [...new Set(rows.map(r => r[monthField]))].sort();
  const keys = [...new Set(rows.map(r => r[keyField]))];
  const keyMonthMap = {};
  for (const r of rows) {
    const k = r[keyField];
    if (!keyMonthMap[k]) keyMonthMap[k] = {};
    keyMonthMap[k][r[monthField]] = (keyMonthMap[k][r[monthField]] || 0) + (r[valueField] || 0);
  }
  const data = {};
  for (const k of keys) data[k] = months.map(m => keyMonthMap[k][m] || 0);
  return { months, keys, data };
}

async function loadHistorical() {
  const monthsLimit = document.getElementById('histMonths').value || '48';
  const url = BASE + '/api/historical' + (monthsLimit !== '0' ? ('?months=' + monthsLimit) : '');
  let data;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    data = await res.json();
  } catch (e) {
    toast('ヒストリカル取得失敗: ' + e.message);
    return;
  }

  if (!data.months || data.months.length === 0) {
    document.getElementById('histInfo').textContent = 'データがありません';
    return;
  }
  document.getElementById('histInfo').textContent = data.months[0] + ' 〜 ' + data.months[data.months.length - 1] + '（' + data.months.length + 'ヶ月）';

  // サマリー: 期間合計
  const totalSales = data.sales.reduce((s, r) => s + (r.sales || 0), 0);
  const totalFreight = data.freight.reduce((s, r) => s + (r.amount || 0), 0);
  const totalMaterial = data.material.reduce((s, r) => s + (r.amount || 0), 0);
  const totalProfit = data.pl.reduce((s, r) => s + (r.gross_profit || 0), 0);
  document.getElementById('histSummary').innerHTML =
    '<div class="summary-item"><div class="label">期間売上</div><div class="value">' + fmt(totalSales) + '</div></div>' +
    '<div class="summary-item"><div class="label">期間粗利</div><div class="value ' + clsVal(totalProfit) + '">' + fmt(totalProfit) + '</div></div>' +
    '<div class="summary-item"><div class="label">期間運賃</div><div class="value">' + fmt(totalFreight) + '</div></div>' +
    '<div class="summary-item"><div class="label">期間資材費</div><div class="value">' + fmt(totalMaterial) + '</div></div>';

  // ① 売上（モール別積み上げ棒）
  const salesGrp = groupByMonthAndKey(data.sales, 'year_month', 'mall_id', 'sales');
  destroyChart('sales');
  _charts.sales = new Chart(document.getElementById('chartSales'), {
    type: 'bar',
    data: {
      labels: salesGrp.months,
      datasets: salesGrp.keys.map((k, i) => ({
        label: MALL_NAMES[k] || k,
        data: salesGrp.data[k],
        backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
      })),
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      scales: { x: { stacked: true }, y: { stacked: true, ticks: { callback: v => fmt(v) } } },
      plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + fmt(ctx.parsed.y) } } },
    },
  });

  // ② 粗利益（セグメント別線）+ 全体粗利率（右軸線）
  const plGrp = groupByMonthAndKey(data.pl, 'year_month', 'segment', 'gross_profit');
  const salesBySegGrp = groupByMonthAndKey(data.pl, 'year_month', 'segment', 'sales');
  const totalGpByMonth = {};
  const totalSalesByMonth = {};
  for (const r of data.pl) {
    totalGpByMonth[r.year_month] = (totalGpByMonth[r.year_month] || 0) + (r.gross_profit || 0);
    totalSalesByMonth[r.year_month] = (totalSalesByMonth[r.year_month] || 0) + (r.sales || 0);
  }
  const marginRates = plGrp.months.map(m => totalSalesByMonth[m] > 0 ? (totalGpByMonth[m] / totalSalesByMonth[m] * 100) : 0);
  destroyChart('profit');
  _charts.profit = new Chart(document.getElementById('chartProfit'), {
    data: {
      labels: plGrp.months,
      datasets: [
        ...plGrp.keys.map((k, i) => ({
          type: 'bar',
          label: (SEGMENT_NAMES[k] || 'seg' + k) + ' 粗利',
          data: plGrp.data[k],
          backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
          yAxisID: 'y',
          stack: 'gp',
        })),
        {
          type: 'line',
          label: '全体粗利率(%)',
          data: marginRates,
          borderColor: '#d93025',
          backgroundColor: 'rgba(217,48,37,0.1)',
          yAxisID: 'y1',
          tension: 0.2,
          borderWidth: 2,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      scales: {
        x: { stacked: true },
        y: { stacked: true, position: 'left', ticks: { callback: v => fmt(v) }, title: { display: true, text: '粗利益（円）' } },
        y1: { position: 'right', grid: { drawOnChartArea: false }, ticks: { callback: v => v.toFixed(1) + '%' }, title: { display: true, text: '粗利率（%）' } },
      },
    },
  });

  // ③ 運賃（運送会社別積み上げ棒）
  const freightGrp = groupByMonthAndKey(data.freight, 'year_month', 'carrier', 'amount');
  destroyChart('freight');
  _charts.freight = new Chart(document.getElementById('chartFreight'), {
    type: 'bar',
    data: {
      labels: freightGrp.months,
      datasets: freightGrp.keys.map((k, i) => ({
        label: k,
        data: freightGrp.data[k],
        backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
      })),
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      scales: { x: { stacked: true }, y: { stacked: true, ticks: { callback: v => fmt(v) } } },
      plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + fmt(ctx.parsed.y) } } },
    },
  });

  // ④ 資材費（仕入先別積み上げ棒）
  const matGrp = groupByMonthAndKey(data.material, 'year_month', 'supplier', 'amount');
  destroyChart('material');
  _charts.material = new Chart(document.getElementById('chartMaterial'), {
    type: 'bar',
    data: {
      labels: matGrp.months,
      datasets: matGrp.keys.map((k, i) => ({
        label: k,
        data: matGrp.data[k],
        backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
      })),
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      scales: { x: { stacked: true }, y: { stacked: true, ticks: { callback: v => fmt(v) } } },
      plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + fmt(ctx.parsed.y) } } },
    },
  });

  // ⑤ 直近月のセグメント売上シェア（円グラフ）
  const latest = data.months[data.months.length - 1];
  const latestSeg = data.pl.filter(r => r.year_month === latest);
  destroyChart('segShare');
  if (latestSeg.length > 0) {
    _charts.segShare = new Chart(document.getElementById('chartSegShare'), {
      type: 'doughnut',
      data: {
        labels: latestSeg.map(r => SEGMENT_NAMES[r.segment] || 'seg' + r.segment),
        datasets: [{
          data: latestSeg.map(r => r.sales),
          backgroundColor: CHART_COLORS,
        }],
      },
      options: {
        maintainAspectRatio: false,
        responsive: true,
        plugins: {
          title: { display: true, text: latest + ' 売上構成' },
          tooltip: { callbacks: { label: ctx => ctx.label + ': ' + fmt(ctx.parsed) + ' (' + (ctx.parsed / latestSeg.reduce((s, r) => s + r.sales, 0) * 100).toFixed(1) + '%)' } },
        },
      },
    });
  }
}

// 初期読み込み
loadCosts();
</script>
</body>
</html>`;
}

export default router;
