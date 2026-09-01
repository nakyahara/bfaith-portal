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
import { loadDimMall } from '../../lib/dim-mall.js';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pingJob } from '../jobs-monitor/ping-local.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
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
// 'YYYY-MM' に delta ヶ月を加算（delta は負も可）
function addMonths(yearMonth, delta) {
  const [y, m] = yearMonth.split('-').map(Number);
  const idx = y * 12 + (m - 1) + delta;
  return `${Math.floor(idx / 12)}-${String((idx % 12) + 1).padStart(2, '0')}`;
}

// 凍結月（2026-02以前）: Excel 初期データ(seed)をそのまま確定値として表示する月。
// 2026-03以降はアプリが各モール売上集計から計算する(ライブ)。中原さん指示 2026-06-13:
// 「2月末以前は初期データなので計算せずそのまま入れる」→ 凍結月はライブ同期/再計算/一括計算の
// 対象外にして seed を保護する。seed は /admin/load-historical-seed で投入。
const FROZEN_THROUGH_YM = '2026-02';
function isFrozenMonth(ym) { return typeof ym === 'string' && ym <= FROZEN_THROUGH_YM; }
const HISTORICAL_SEED_PATH = path.join(__dirname, 'seed', 'historical-pl-seed.json');

// 運送会社・仕入先のプリセット（Excel「運賃集計」「輸出運賃」「梱包資材費」シートのヘッダーに合わせる）
const CARRIERS = ['FBA運賃', 'Easy Ship運賃', 'RSL費用', 'ヤマト', 'ヤマト2', '佐川', '西濃', '福山通運', '郵便局（UPSIDER1）', '郵便局（UPSIDER2）', 'クリックポスト'];
const EXPORT_CARRIERS = ['TNK運賃(輸出)', 'FBA運賃(輸出)'];

// Amazon ペイメント由来の自動運賃（売上同期で mart_amazon_monthly_summary.by_segment から投入。画面では読み取り専用・保存対象外）
//   FBA運賃       = |Σ FBA手数料|             … Excel 旧運用踏襲（2026-04-20・§12）
//   Easy Ship運賃 = |Σ Amazon Easy Ship料金|  … 代表指示 2026-09-01。amazon-accounting PR #1043 で by_segment に追加された説明別内訳。
//                   Easy Ship は SKUなし行 = other セグメントの 手数料/トランザクション他 列にしか入らず、PF手数料（seg1〜3 のみ）には
//                   数えられていないため運賃に立てても二重計上にならない。列が無い月（PR #1043 より前に確定した月）は 0 = 行を作らない
// いずれも税込負数 → |x|/1.1 で税抜化し cost_scope='shared'（全モール売上で按分）
const AUTO_FREIGHT = [
  { carrier: 'FBA運賃',       segKey: 'FBA手数料',            label: 'Amazon FBA手数料から自動計算（売上同期で更新）' },
  { carrier: 'Easy Ship運賃', segKey: 'Amazon Easy Ship料金', label: 'Amazon Easy Ship料金から自動計算（売上同期で更新）' },
];
const autoFreightNote = segKey => 'auto from mart_amazon_monthly_summary.by_segment.' + segKey;
// 「自動運賃が所有する行」の判定 (stale 削除と UPSERT の WHERE で必ず同じ条件を使う — Codex 指摘: 条件がずれると
// historical-import 由来 (note 無し) の行がネットゼロ/列なしの月に消えず、古い運賃で PL が再確定される)。
//   自動行 (note='auto from…') / system-sync / historical-import (Excel 取込・値は自動計算と同じ) が対象。
//   人が画面・API で入れた行 (entered_by=メールアドレス等) は対象外 = 上書きも削除もしない
const AUTO_FREIGHT_OWNED_SQL = "(mgmt_freight_costs.note LIKE 'auto from%' OR mgmt_freight_costs.entered_by IN ('system-sync', 'historical-import'))";
const isAutoFreightCarrier = carrier => AUTO_FREIGHT.some(a => a.carrier === carrier);
const SUPPLIERS = ['ヤマト', 'ダイワハイテックス', 'アップサイダーカード', 'ダンボールワン', 'アスクル', 'シモジマ', 'ラクスル', '郵便局（レタパ）', 'イージーパック', 'スズヤエビス堂', 'アップサイダーカード2', '五洋パッケージ'];

const MALL_NAMES = {
  amazon_jp: 'Amazon', rakuten: '楽天', yahoo: 'Yahoo!',
  aupay: 'auPay', qoo10: 'Qoo10', linegift: 'LINEギフト',
  mercari: 'メルカリshops', dshop: 'Dショッピング', amazon_usa: '米国Amazon',
};
const SEGMENT_NAMES = { 1: '自社商品', 2: '取引先限定商品', 3: '仕入れ商品', 4: '米国Amazon輸出' };

// ─── API ───

// 認証ガード（セッション認証 or APIキー認証）
// 監査 2026-07-06 I-43 対応: MIRROR_SYNC_KEY 未設定時に素通りする fail-open を
// 「session なし + key 未設定なら 503 拒否」の fail-closed に反転。
function checkAuth(req, res) {
  const sessionOK = req.session?.authenticated;
  if (sessionOK) return true;
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) {
    res.status(503).json({ error: 'mirror_sync_key_unset' });
    return false;
  }
  const provided = req.headers['x-sync-key'];
  if (provided !== key) {
    res.status(401).json({ error: 'Invalid sync key' });
    return false;
  }
  return true;
}

// 過去データ一括インポート（運賃・資材費・売上）
router.post('/import-historical', (req, res) => {
  if (!checkAuth(req, res)) return;

  const db = getMirrorDB();
  let { freight = [], material = [], sales = [] } = req.body;
  // 凍結月(2026-02以前)は seed ローダー(/admin/load-historical-seed)が唯一の入口。
  // 古い汎用インポートで凍結月の入力テーブルを書くと seed と不整合になるためここでは除外。
  const beforeCounts = { freight: freight.length, material: material.length, sales: sales.length };
  freight = freight.filter(f => !isFrozenMonth(f.year_month));
  material = material.filter(m => !isFrozenMonth(m.year_month));
  sales = sales.filter(s => !isFrozenMonth(s.year_month));
  const skippedFrozen = (beforeCounts.freight - freight.length) + (beforeCounts.material - material.length) + (beforeCounts.sales - sales.length);
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

  res.json({ ok: true, freight: freight.length, material: material.length, sales: sales.length, skipped_frozen: skippedFrozen });
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

// ── 過去データ(Excel初期値)シードの投入 (中原さん指示 2026-06-13) ──
// 2026-02以前(凍結月)の各月を、Excel 月次シート由来の確定PL(mgmt_monthly_pl)へ「そのまま」投入。
// 計算なし: seed の値(売上/原価/PF手数料/広告費/運賃/資材費/粗利益)を直接格納。
// closing は確定 + frozen マーカー(calc_version='excel_seed')。dryRun=1 で投入せず seed vs 現状の差分を返す。
router.post('/admin/load-historical-seed', (req, res) => {
  if (!checkAuth(req, res)) return;
  const db = getMirrorDB();
  const dryRun = req.query.dryRun === '1' || (req.body && req.body.dryRun === true);
  let seed;
  try {
    seed = JSON.parse(fs.readFileSync(HISTORICAL_SEED_PATH, 'utf8'));
  } catch (e) {
    return res.status(500).json({ error: 'seed_read_failed', message: e.message });
  }
  if (!Array.isArray(seed) || !seed.length) return res.status(500).json({ error: 'seed_empty' });
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  // 月ごとにグループ化 + 全行が凍結月であることを検証(ライブ月を誤って seed しない安全網)
  const byMonth = new Map();
  for (const r of seed) {
    if (!isFrozenMonth(r.year_month)) {
      return res.status(400).json({ error: 'non_frozen_in_seed',
        message: `seed に凍結対象外(${FROZEN_THROUGH_YM}超)の月が含まれます: ${r.year_month}` });
    }
    if (!byMonth.has(r.year_month)) byMonth.set(r.year_month, []);
    byMonth.get(r.year_month).push(r);
  }

  // 比較レポート: seed の月別合計 vs 現状 mgmt_monthly_pl の月別合計
  const curTotal = db.prepare('SELECT COALESCE(SUM(sales),0) sales, COALESCE(SUM(gross_profit),0) gp FROM mgmt_monthly_pl WHERE year_month=?');
  const report = [];
  for (const [ym, rows] of [...byMonth.entries()].sort()) {
    const sSales = rows.reduce((s, r) => s + (r.sales || 0), 0);
    const sGp = rows.reduce((s, r) => s + (r.gross_profit || 0), 0);
    const cur = curTotal.get(ym);
    report.push({ year_month: ym, rows: rows.length,
      seed_sales: Math.round(sSales), seed_margin: sSales ? +(sGp / sSales * 100).toFixed(1) : 0,
      current_sales: Math.round(cur.sales), current_margin: cur.sales ? +(cur.gp / cur.sales * 100).toFixed(1) : 0 });
  }
  if (dryRun) return res.json({ ok: true, dry_run: true, months: report.length, report });

  // 本投入: 各凍結月の mgmt_monthly_pl を seed で全置換 + closing を frozen 確定。
  const insPL = db.prepare(`INSERT INTO mgmt_monthly_pl
    (year_month, mall_id, segment, sales, sales_ratio, cost, pf_fee, ad_cost, freight, material, variable_cost, gross_profit, gross_margin, fiscal_year)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const insClosing = db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_closing
    (year_month, fiscal_year, fiscal_month, status, freight_total, material_total, confirmed_at, confirmed_by, calc_version)
    VALUES (?,?,?,?,?,?,?,?,?)`);
  // mart_monthly_segment_sales も seed 値に揃える(ヒストリカルタブの売上×モール系列が
  // seed PL と整合するように。凍結月は syncSegmentSalesForMonth 対象外なので上書きされない)。
  const insSeg = db.prepare(`INSERT INTO mart_monthly_segment_sales
    (year_month, mall_id, segment, sales, cost, pf_fee, ad_cost, confirmed_at, source_file, logic_version)
    VALUES (?,?,?,?,?,?,?,?,?,?)`);
  const tx = db.transaction(() => {
    for (const [ym, rows] of byMonth.entries()) {
      const monthSales = rows.reduce((s, r) => s + (r.sales || 0), 0);
      db.prepare('DELETE FROM mgmt_monthly_pl WHERE year_month = ?').run(ym);
      db.prepare('DELETE FROM mart_monthly_segment_sales WHERE year_month = ?').run(ym);
      let fTot = 0, mTot = 0;
      for (const r of rows) {
        const sales = Math.round(r.sales || 0);
        const gp = Math.round(r.gross_profit || 0);
        const varCost = sales - gp; // 整合性保証: sales = variable_cost + gross_profit
        const cost = Math.round(r.cost || 0), pf = Math.round(r.pf_fee || 0), ad = Math.round(r.ad_cost || 0);
        const fr = Math.round(r.freight || 0), mat = Math.round(r.material || 0);
        insPL.run(ym, r.mall_id, r.segment, sales, monthSales > 0 ? sales / monthSales : 0,
          cost, pf, ad, fr, mat, varCost, gp, sales > 0 ? gp / sales : 0, getFiscalYear(ym));
        insSeg.run(ym, r.mall_id, r.segment, sales, cost, pf, ad, now, 'historical-excel-seed', 'excel_seed');
        fTot += fr; mTot += mat;
      }
      insClosing.run(ym, getFiscalYear(ym), getFiscalMonth(ym), 'confirmed', fTot, mTot, now, 'excel-seed', 'excel_seed');
    }
  });
  tx();
  res.json({ ok: true, dry_run: false, months_loaded: byMonth.size, rows: seed.length, report });
});

// ── 指定月より前の mgmt データを削除 (誤った旧データ除去、中原さん 2026-06-14) ──
// 2024-07 より前(=2024-06以前)は誤りなので削除。凍結月のため削除後に再計算で復活しない。
// dryRun=1 で削除せず対象月・件数を返す。before は YYYY-MM、安全のため '2024-07' 以下に制限
// (= 良データ開始月 2024-07 以降は誤って消せない)。
const PURGE_MAX_BEFORE = '2024-07'; // これ以降(良データ=Excel seed範囲)は purge 対象にできない
router.post('/admin/purge-months-before', (req, res) => {
  if (!checkAuth(req, res)) return;
  const db = getMirrorDB();
  const before = String((req.body && req.body.before) || req.query.before || '').trim();
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(before)) {
    return res.status(400).json({ error: 'bad_before', message: 'before は YYYY-MM 形式(月は01〜12)で指定してください' });
  }
  if (before > PURGE_MAX_BEFORE) {
    return res.status(400).json({ error: 'before_too_large',
      message: `安全のため ${PURGE_MAX_BEFORE} より後ろは削除できません(良データを誤削除しないため)。指定=${before}` });
  }
  const dryRun = req.query.dryRun === '1' || (req.body && req.body.dryRun === true);
  const tables = ['mgmt_monthly_pl', 'mgmt_monthly_closing', 'mart_monthly_segment_sales', 'mgmt_freight_costs', 'mgmt_material_costs'];
  // 削除対象の件数(テーブル別) + 影響する月一覧(削除前にスナップショット)
  const counts = {};
  for (const t of tables) {
    try { counts[t] = db.prepare(`SELECT COUNT(*) c FROM ${t} WHERE year_month < ?`).get(before).c; }
    catch (e) { counts[t] = 'err: ' + e.message; }
  }
  const monthSet = new Set();
  for (const t of tables) {
    try { for (const r of db.prepare(`SELECT DISTINCT year_month FROM ${t} WHERE year_month < ?`).all(before)) monthSet.add(r.year_month); }
    catch {}
  }
  const months = [...monthSet].sort();
  if (dryRun) return res.json({ ok: true, dry_run: true, before, months, counts });
  // 本削除: 1tx で全テーブル DELETE。1つでも失敗したら例外を外へ出して全 rollback(Codex High:
  // catch で握りつぶすと一部だけ削除されて commit される事故になるため catch しない)。
  // 実削除件数(.changes)を返す。before<=PURGE_MAX_BEFORE<FROZEN_THROUGH_YM なので対象は全て凍結月
  // = 削除後にライブ再計算で復活しない。
  let deleted;
  try {
    const tx = db.transaction(() => {
      const out = {};
      for (const t of tables) out[t] = db.prepare(`DELETE FROM ${t} WHERE year_month < ?`).run(before).changes;
      return out;
    });
    deleted = tx();
  } catch (e) {
    return res.status(500).json({ ok: false, error: 'delete_failed', message: e.message });
  }
  res.json({ ok: true, dry_run: false, before, months, deleted });
});

// 一括確定: 指定月を除く全月について calculate を実行
router.post('/bulk-calculate', (req, res) => {
  if (!checkAuth(req, res)) return;

  const db = getMirrorDB();
  const { exclude_months = [] } = req.body;
  const user = req.session?.email || 'historical-bulk';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  // 対象月: mart_monthly_segment_sales にデータがあり exclude に含まれない月。
  // 凍結月(2026-02以前)は Excel seed が確定値なので一括計算の対象外。
  const months = db.prepare('SELECT DISTINCT year_month FROM mart_monthly_segment_sales ORDER BY year_month').all()
    .map(r => r.year_month)
    .filter(m => !exclude_months.includes(m) && !isFrozenMonth(m));

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
      // 完全置換: 消えたモール/セグメントの旧PL行を残さない
      db.prepare('DELETE FROM mgmt_monthly_pl WHERE year_month = ?').run(ym);
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
  // 凍結月(2026-02以前)は Excel 初期データで確定済み。運賃編集→needs_review 降格で
  // 凍結月が表示から消える事故を防ぐため拒否(Codex High 対応)。
  if (isFrozenMonth(year_month)) {
    return res.status(400).json({ error: 'frozen_month', message: '2026年2月以前は初期データ(Excel)で確定済みのため編集できません。' });
  }
  const user = req.session?.email || 'unknown';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  if (!Array.isArray(items)) return res.status(400).json({ error: 'invalid_items', message: 'items 配列が必要です' });
  // 自動運賃 (FBA運賃 / Easy Ship運賃) は売上同期が所有するため API からの手入力は受け付けない
  // (画面は data-auto で送らない。受け付けると次回同期で無警告上書き or 所有者不明の行が残る — Codex 指摘)
  const autoItems = items.filter(i => isAutoFreightCarrier(i && i.carrier)).map(i => i.carrier);
  if (autoItems.length > 0) {
    return res.status(400).json({ error: 'auto_carrier', message: autoItems.join(' / ') + ' は Amazon売上集計から売上同期で自動計算されるため手入力できません', carriers: autoItems });
  }
  // 手入力の UPSERT。競合更新では entered_by も更新し、所有者を「人」に移す (自動同期の上書き対象から外れる)
  const stmt = db.prepare(`INSERT INTO mgmt_freight_costs (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(year_month, carrier) DO UPDATE SET amount=excluded.amount, cost_scope=excluded.cost_scope, target_segment=excluded.target_segment, target_mall_id=excluded.target_mall_id, note=excluded.note, entered_by=excluded.entered_by, updated_at=excluded.updated_at`);
  const tx = db.transaction(() => {
    for (const item of items) {
      stmt.run(year_month, item.carrier, Math.round(item.amount || 0), item.cost_scope || 'shared', item.target_segment || null, item.target_mall_id || null, item.note || null, user, now, now);
    }
    // 確定済み月の運賃を書き換えたら確定PLが古くなるので要再確定に降格（同一tx）
    db.prepare("UPDATE mgmt_monthly_closing SET status='needs_review' WHERE year_month = ? AND status = 'confirmed'").run(year_month);
  });
  tx();
  res.json({ ok: true });
});

// 資材費 保存（一括UPSERT）
router.post('/api/material', (req, res) => {
  const db = getMirrorDB();
  const { year_month, items } = req.body;
  // 凍結月(2026-02以前)は編集不可(Codex High 対応、freight と同方針)
  if (isFrozenMonth(year_month)) {
    return res.status(400).json({ error: 'frozen_month', message: '2026年2月以前は初期データ(Excel)で確定済みのため編集できません。' });
  }
  const user = req.session?.email || 'unknown';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const stmt = db.prepare(`INSERT INTO mgmt_material_costs (year_month, supplier, amount, note, entered_by, entered_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(year_month, supplier) DO UPDATE SET amount=excluded.amount, note=excluded.note, updated_at=excluded.updated_at`);
  const tx = db.transaction(() => {
    for (const item of items) {
      stmt.run(year_month, item.supplier, Math.round(item.amount || 0), item.note || null, user, now, now);
    }
    // 確定済み月の資材費を書き換えたら確定PLが古くなるので要再確定に降格（同一tx）
    db.prepare("UPDATE mgmt_monthly_closing SET status='needs_review' WHERE year_month = ? AND status = 'confirmed'").run(year_month);
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
  { table: 'mart_rakuten_monthly_summary', mall_id: 'rakuten', adField: 'ad_cost', feeField: 'pf_fee' },
  { table: 'mart_yahoo_monthly_summary', mall_id: 'yahoo', adField: null, feeField: 'pf_fee' },
  { table: 'mart_aupay_monthly_summary', mall_id: 'aupay', adField: null, feeField: 'pf_fee' },
  { table: 'mart_qoo10_monthly_summary', mall_id: 'qoo10', adField: 'ad_cost', feeField: 'pf_fee' },
  { table: 'mart_linegift_monthly_summary', mall_id: 'linegift', adField: null, feeField: 'pf_fee' },
  { table: 'mart_mercari_monthly_summary', mall_id: 'mercari', adField: null, feeField: 'pf_fee' },
  { table: 'mart_amazon_usa_monthly_summary', mall_id: 'amazon_usa', adField: 'ad_cost', feeField: null },
];

// 仕訳書(billing)の「素通り金額」= 楽天ペイ等が顧客から集めた決済金の店舗への入金。
// これは費用(手数料)ではなく単なる入金で、本来は仕訳書の 支払/相殺区分='相殺'。
// 確定時のクライアント計算は請求行のみ合計するため正しいが、byCategory に集計された後は
// 支払/相殺区分が失われるため、下の backfill では品目名で識別して PF手数料 から除外する。
// 例: 「楽天ﾍﾟｲ_決済金等」(売上の約半分に達する巨額) を手数料に混ぜると粗利が大きく潰れる。
const SETTLEMENT_PASSTHROUGH_KEYWORDS = ['決済金等', '決済金'];
function isSettlementPassthrough(name) {
  const s = String(name || '');
  if (s.includes('手数料')) return false; // 「後払い決済_手数料」等は費用なので除外しない
  return SETTLEMENT_PASSTHROUGH_KEYWORDS.some(k => s.includes(k));
}

// 楽天/Yahoo の pf_fee 未保存(列追加前に確定された既存)月向け: billing から PF手数料を
// best-effort 再計算。再確定すれば app が正確値を pf_fee に保存して上書きする。
// pf_fee は他モール(auPay/Qoo10/LINEギフト/メルカリ)と同様 税込ベースで揃える。
function backfillBillingPfFee(mallId, row) {
  let billing;
  try { billing = JSON.parse(row.billing || '{}'); } catch { return 0; }
  // 旧Excel/一括登録形状: { 変動費: { PF手数料, 広告費, ... } } は PF手数料 を直接持つ
  if (billing && !Array.isArray(billing) && billing.変動費 && billing.変動費['PF手数料'] != null) {
    return Math.max(0, Math.round(Number(billing.変動費['PF手数料']) || 0));
  }
  if (!Array.isArray(billing)) return 0;
  const adCost = Number(row.ad_cost) || 0;
  if (mallId === 'yahoo') {
    // Yahoo: 請求合計(税込) − 広告費（byCategory は請求明細のみ＝正確）。
    // Yahoo の確定側(yahoo-accounting)も決済金除外をしておらず、素通り決済金が混ざる仕様も
    // 確認できていないため、ここでは楽天のような除外はしない(再確定値との不一致を避ける)。
    const totalTaxIncl = billing.reduce((s, c) => s + (Number(c['金額(税込)']) || 0), 0);
    return Math.max(0, Math.round(totalTaxIncl - adCost));
  }
  // 楽天: 請求合計(税込) − 広告費 − クーポン値引（決済金等の素通り入金は除外）
  const totalTaxIncl = billing.reduce((s, c) =>
    isSettlementPassthrough(c['品目']) ? s : s + (Number(c['税込合計']) || 0), 0);
  let coupon = 0;
  try {
    const byTax = JSON.parse(row.by_tax || '{}');
    for (const k of Object.keys(byTax)) coupon += Number(byTax[k]['クーポン値引額']) || 0;
  } catch {}
  return Math.max(0, Math.round(totalTaxIncl - adCost - coupon));
}

// 楽天/Yahoo の仕訳書(billing)に含まれる「物流費」の合計(税込)。
// RSL費用等は pf_fee(請求合計ベース)にも入るが、手入力の運賃でも計上するため二重になる。
// pf_fee からこの分を差し引いて二重計上を解消する。物流系の費目名だけを対象にする(安全側)。
const FULFILLMENT_KEYWORDS = ['RSL', 'ロジスティ', 'フルフィルメント', '配送代行', '物流代行'];
function fulfillmentInBilling(row) {
  let billing;
  try { billing = JSON.parse(row.billing || '[]'); } catch { return 0; }
  if (!Array.isArray(billing)) return 0; // 変動費shape等は対象外(0)
  let sum = 0;
  for (const c of billing) {
    const name = String(c['品目'] || '');
    if (FULFILLMENT_KEYWORDS.some(k => name.includes(k))) {
      sum += Number(c['税込合計'] != null ? c['税込合計'] : c['金額(税込)']) || 0;
    }
  }
  return Math.max(0, Math.round(sum));
}

// by_segment のキーはモールで異なる（楽天/Qoo10等='売上合計'・'原価合計' / Yahoo='売上'・'原価'）。
// 分母(按分基準 segSalesTotal)と分子(各segの sales)で必ず同じ解決を使うためヘルパー化する。
// 「0 が正当値で別キーに非ゼロが残る」データでの誤フォールバックを避けるため != null で判定する。
function pickNum(obj, keys) {
  for (const k of keys) if (obj && obj[k] != null) return Number(obj[k]) || 0;
  return 0;
}
// 国内非Amazonモール: mart の 売上/PF手数料/広告費 が「税込」で入る。mgmt は税抜基準なので /1.1 する。
// amazon_jp は税が別カラムで税抜分離済み、amazon_usa は米国(消費税なし)なので対象外(中原さん 2026-06-14)。
// 監査PR-11: ハードコードSetを dim_mall.tax_included=1 に集約 (値=従来の7モールと同一)。
// module初期化時はDB未初期化の可能性があるため遅延取得 (loadDimMall はプロセス内キャッシュ)。
function taxIncludedMalls() { return loadDimMall(getMirrorDB()).taxIncludedSet; }

// ─── 金額・丸め規約 (設計監査 2026-07-06 PR-7/S-7) ───
// 円は最終的に INTEGER で保存・出力する。丸め(Math.round=四捨五入)は「DB書き込み値/
// 集計出力値を確定する直前の1回だけ」。中間計算(按分・税抜化の途中経過)では丸めない。
// 全社規約の正本: AI_reference『EC統合データウェアハウス_設計書.md』の「全社DDL・金額規約」節。
// ※ /1.1 一律換算による軽減税率8%商品の恒常誤差は判断済みの既知事項(監査L-5)。
const TAX_RATE_JP = 1.1;
// 税込円→税抜円(整数確定)。|| 0 は付けない: 呼び出し元の式と厳密同一
// (NaN を黙って 0 に化かすと上流バグの検知が遅れる。Codex R1指摘)
const toTaxExcludedJpy = v => Math.round(v / TAX_RATE_JP);

// 売上の解決:
//  - amazon_jp: 商品売上 + 配送料 + ギフト包装手数料（顧客請求の売上性項目、すべて税抜=税は別カラム。中原さん 2026-06-14 確定）。
//    ネットの '合計' を使わないのは、後段で手数料/FBA運賃を再控除するため二重控除になるから。
//  - 非Amazon国内モール: 税込売上を /1.1 で税抜化。amazon_usa 等は税抜のまま。
function segmentSales(mallId, segData) {
  if (mallId === 'amazon_jp') {
    return pickNum(segData, ['商品売上']) + pickNum(segData, ['配送料']) + pickNum(segData, ['ギフト包装手数料']);
  }
  const raw = pickNum(segData, ['売上合計', '売上', '合計', '商品売上']);
  return taxIncludedMalls().has(mallId) ? raw / 1.1 : raw;
}
function segmentCost(segData) {
  return pickNum(segData, ['原価合計', '原価']);
}

// 各モール集計テーブル(mart_*_monthly_summary)から指定月の mart_monthly_segment_sales を生成。
// 各モールアプリは「確定」時のみ summary 行を作るため、行の存在 = そのモールの当月確定済み。
function syncSegmentSalesForMonth(db, year_month, now) {
  let totalInserted = 0;
  let fbaFreightInserted = null;      // 後方互換 (レスポンスの fba_freight_tax_excluded)
  const autoFreightInserted = {};     // carrier → 税抜額 (FBA運賃 / Easy Ship運賃)
  const autoFreightSkipped = {};      // carrier → { auto_amount, existing_amount, entered_by } (手入力行があり上書きしなかった)
  const mallsPresent = new Set();

  const insertStmt = db.prepare(`INSERT OR REPLACE INTO mart_monthly_segment_sales
    (year_month, mall_id, segment, sales, cost, pf_fee, ad_cost, confirmed_at, source_file, logic_version)
    VALUES (?,?,?,?,?,?,?,?,?,?)`);

  // 自動運賃の UPSERT。UNIQUE(year_month, carrier) で衝突した既存行が「自動行 (note='auto from…')」または
  // 「historical-import 由来 (Excel 取込・値は自動計算と同じ)」の場合だけ更新し、人が入れた行 (entered_by=ユーザー) は上書きしない
  // (Codex 指摘: 手入力行が黙って自動値に置換され entered_by だけ残る監査矛盾を防ぐ)。スキップは auto_freight_skipped で返す
  const freightStmt = db.prepare(`INSERT INTO mgmt_freight_costs
    (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(year_month, carrier) DO UPDATE SET amount=excluded.amount, cost_scope=excluded.cost_scope, note=excluded.note,
      target_segment=excluded.target_segment, target_mall_id=excluded.target_mall_id, entered_by=excluded.entered_by, updated_at=excluded.updated_at
    WHERE ${AUTO_FREIGHT_OWNED_SQL}`);
  const existingFreightStmt = db.prepare('SELECT amount, entered_by, note FROM mgmt_freight_costs WHERE year_month = ? AND carrier = ?');

  const tx = db.transaction(() => {
    // source-owned（各 summary 由来）の当月行を先に全消去。summary が削除/空化された
    // モールの stale 行も確実に除去する（source_file = テーブル名で識別）。
    // historical-excel 由来や米国Amazon(mgmt_row, historical由来)の行は対象外で保護される。
    for (const mt of MALL_TABLES) {
      db.prepare('DELETE FROM mart_monthly_segment_sales WHERE year_month = ? AND source_file = ?').run(year_month, mt.table);
    }
    // 自動登録の運賃 (FBA運賃 / Easy Ship運賃) も冒頭で消去（Amazon summary が消えた月・列が無くなった月の stale 運賃を残さない）。
    // 削除対象は UPSERT の更新対象と同じ所有権条件 (AUTO_FREIGHT_OWNED_SQL)。手入力分は対象外で保護。金額がある場合のみ下で再投入する。
    for (const af of AUTO_FREIGHT) {
      db.prepare('DELETE FROM mgmt_freight_costs WHERE year_month = ? AND carrier = ? AND ' + AUTO_FREIGHT_OWNED_SQL).run(year_month, af.carrier);
    }
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

      // セグメント全体の売上合計（広告費・PF手数料按分の分母）。各segの sales と同じ解決を使う。
      const segSalesTotal = Object.values(allSegs).reduce((s, v) => s + segmentSales(mt.mall_id, v), 0);

      // PF手数料の全体値（テーブルカラムから取得）
      let pfFeeTotal = mt.feeField ? (Number(row[mt.feeField]) || 0) : 0;
      // 楽天/Yahoo: pf_fee 未保存の既存月は billing から best-effort 再計算（再確定で正確値に置換）
      if ((mt.mall_id === 'rakuten' || mt.mall_id === 'yahoo') && !pfFeeTotal) {
        pfFeeTotal = backfillBillingPfFee(mt.mall_id, row);
      }
      // 楽天/Yahoo の pf_fee は「仕訳書の請求合計」ベースで、RSL費用等の物流費も含む。
      // 物流費は手入力の運賃で別途計上しているため、二重計上を避けて pf_fee から差し引く。
      if (mt.mall_id === 'rakuten' || mt.mall_id === 'yahoo') {
        pfFeeTotal = Math.max(0, pfFeeTotal - fulfillmentInBilling(row));
      }

      // Amazon JP: FBA手数料・Easy Ship料金は販売手数料ではなく運賃として扱う（FBA は Excel運用踏襲、Easy Ship は 2026-09-01 代表指示）
      // by_segment の該当列（全セグメント合計、税込負数）を |x|/1.1 で税抜化し
      // mgmt_freight_costs に carrier=AUTO_FREIGHT[].carrier, cost_scope='shared' で自動登録
      if (mt.mall_id === 'amazon_jp') {
        for (const af of AUTO_FREIGHT) {
          // 符号付き合計を取ってから絶対値化（返金 segment・取り消し/訂正行が含まれる場合もネットで計算するため）
          const signed = Object.values(allSegs).reduce((s, v) => s + (Number(v[af.segKey]) || 0), 0);
          const taxInc = Math.abs(signed);
          if (taxInc > 0) {
            const taxEx = toTaxExcludedJpy(taxInc);
            const r = freightStmt.run(year_month, af.carrier, taxEx, 'shared', null, null, autoFreightNote(af.segKey), 'system-sync', now, now);
            if (r.changes > 0) {
              autoFreightInserted[af.carrier] = taxEx;
              if (af.carrier === 'FBA運賃') fbaFreightInserted = taxEx;
            } else {
              // 同名 carrier の手入力行が残っている → 上書きせずスキップ (画面のトーストとログで知らせる)
              const ex = existingFreightStmt.get(year_month, af.carrier) || {};
              autoFreightSkipped[af.carrier] = { auto_amount: taxEx, existing_amount: ex.amount ?? null, entered_by: ex.entered_by ?? null };
            }
          }
          // fee=0 / summary 無し / by_segment に列が無い月は冒頭で削除済みなので何もしない
        }
      }

      // セグメント行を組み立てる
      const segRows = [];
      if (mt.mall_id === 'amazon_usa') {
        // 米国Amazon: mgmt_row(USD→JPY換算済の管理会計15列) を「PF=amazon_usa / セグメント4」の1行へマッピング。
        // 国内モールと同じ作り（売上=商品売上+配送料、手数料系は変動費として別建て）。US は消費税なしなので /1.1 しない。
        const mg = JSON.parse(row.mgmt_row || '{}');
        const jr = JSON.parse(row.jpy_row || '{}'); // 列別JPY換算値（手数料を税と分離して取る）
        const usSales = Math.round((mg['商品売上'] || 0) + (mg['配送料'] || 0));
        const usCost = Math.round((mg['原価合計'] != null ? mg['原価合計'] : row.cost_total) || 0);
        // PF手数料 = 販売系手数料のみ（手数料は控除(負値)→絶対値で変動費化）。
        let feeSigned;
        if (Object.keys(jr).length > 0) {
          // 通常のCSV確定経路: jpy_row から個別に取り、marketplace withheld tax
          // (Amazonが代理徴収・納付する米国Sales Taxの pass-through) は除外する。
          // 売上側も売上税/配送料の税金を含めないため整合する。
          feeSigned = (jr['selling fees'] || 0)
                    + (jr['fba fees'] || 0)
                    + (jr['other transaction fees'] || 0)
                    + (jr['promotional rebates'] || 0);
        } else {
          // 履歴インポート月(jpy_row='{}')は mgmt_row から。'手数料' は
          // (withheld tax + selling fees) 合算で分離不能なため withheld tax を含む
          // (=粗利わずかに保守的)。fees=0 で粗利過大にするよりは正確。
          feeSigned = (mg['手数料'] || 0)
                    + (mg['FBA手数料'] || 0)
                    + (mg['トランザクションに関するその他の手数料'] || 0)
                    + (mg['プロモーション割引額'] || 0);
        }
        const usPfFee = Math.round(Math.abs(feeSigned));
        const usAdCost = Math.round(row.ad_cost || 0);
        if (usSales || usCost || usPfFee || usAdCost) {
          segRows.push({ seg: 4, sales: usSales, cost: usCost, pfFee: usPfFee, adCost: usAdCost });
        }
      } else
      for (const [segKey, segData] of Object.entries(allSegs)) {
        const seg = segKey === 'other' ? null : parseInt(segKey);
        if (seg === null || isNaN(seg)) continue;

        // 売上・原価（モール別キー差は segmentSales/segmentCost が吸収。分母 segSalesTotal と同じ解決）。
        const sales = segmentSales(mt.mall_id, segData);
        const cost = segmentCost(segData);

        // PF手数料計算
        let pfFee = 0;
        if (mt.mall_id === 'amazon_jp') {
          // Amazon: 販売手数料 = |手数料 + プロモーション割引額 + プロモーション割引の税金 + Amazonポイント費用|
          // （税込符号付き合計を取ってから abs → /1.1 で税抜化）。FBA手数料は運賃として別計上するため含めない。
          const signed = (segData['手数料'] || 0)
                       + (segData['プロモーション割引額'] || 0)
                       + (segData['プロモーション割引の税金'] || 0)
                       + (segData['Amazonポイント費用'] || 0);
          pfFee = toTaxExcludedJpy(Math.abs(signed));
        } else if (segData['手数料'] !== undefined || segData['FBA手数料'] !== undefined) {
          // 手数料/FBA手数料は費用(正値)として変動費に積む。返金等で符号が負で入る月でも
          // 粗利を押し上げないよう abs で正規化(Amazon JP/USA と同方針、Codex High 対応)。
          pfFee += Math.abs(segData['手数料'] || 0);
          pfFee += Math.abs(segData['FBA手数料'] || 0);
          if (segData['トランザクション他'] !== undefined) pfFee += Math.abs(segData['トランザクション他'] || 0);
        } else {
          // 全体PF手数料を売上按分
          const segRatio = segSalesTotal > 0 ? sales / segSalesTotal : 0;
          pfFee = pfFeeTotal * segRatio;
        }

        // 広告費: 全体広告費をセグメント売上比で按分
        const segRatio = segSalesTotal > 0 ? sales / segSalesTotal : 0;
        let adCost = adCostTotal * segRatio;

        // 非Amazon国内モールは PF手数料・広告費も税込 → 税抜化（Amazon JP は手数料を上で /1.1 済み）。
        if (taxIncludedMalls().has(mt.mall_id)) { pfFee = pfFee / 1.1; adCost = adCost / 1.1; }

        segRows.push({ seg, sales: Math.round(sales), cost: Math.round(cost), pfFee: Math.round(pfFee), adCost: Math.round(adCost) });
      }

      // by_segment 形式で 1 行も作れないモール(米国Amazon等)は何も挿入しない
      // （source-owned 行は上で消去済みなので、historical 由来の既存行だけが残る）
      if (segRows.length === 0) continue;

      mallsPresent.add(mt.mall_id);
      for (const r of segRows) {
        insertStmt.run(year_month, mt.mall_id, r.seg, r.sales, r.cost, r.pfFee, r.adCost, now, mt.table, 'v1');
        totalInserted++;
      }
    }
  });
  tx();
  return { inserted: totalInserted, fba_freight_tax_excluded: fbaFreightInserted, auto_freight_tax_excluded: autoFreightInserted, auto_freight_skipped: autoFreightSkipped, malls: [...mallsPresent] };
}

// 指定月のPLを「現在の segment_sales + 既存の運賃・資材費」で再計算して保存し confirmed にする。
// /api/calculate（人手の確定）と auto-sync（自動再計算）で共有。
function recomputeMonthlyPL(db, year_month, now, user) {
  // 凍結月(2026-02以前)は Excel seed が確定値。ライブ再計算で mgmt_monthly_pl を上書きしない。
  if (isFrozenMonth(year_month)) return { rows: 0, freight_total: 0, material_total: 0, skipped: true, frozen: true };
  const segSales = db.prepare('SELECT * FROM mart_monthly_segment_sales WHERE year_month = ?').all(year_month);
  // 売上が空のとき（同期元障害・summary欠落等）は既存PLを壊さない（確定履歴を守る）
  if (segSales.length === 0) return { rows: 0, freight_total: 0, material_total: 0, skipped: true };
  // 既に確定済みなら confirmed_at（人が確定した日時）は保持し、再計算で上書きしない
  const prevConfirmedAt = db.prepare('SELECT confirmed_at FROM mgmt_monthly_closing WHERE year_month = ?').get(year_month)?.confirmed_at;
  const confirmedAt = prevConfirmedAt || now;
  const freightRows = db.prepare('SELECT * FROM mgmt_freight_costs WHERE year_month = ?').all(year_month);
  const materialRows = db.prepare('SELECT * FROM mgmt_material_costs WHERE year_month = ?').all(year_month);
  const directFreight = freightRows.filter(r => r.cost_scope !== 'shared');
  // 運賃按分 (2026-06-13 中原さん指示): FBA / FBM / ヤマト 等を区別せず、shared 運賃 (FBA運賃含む)
  // を全モール売上で一律按分する。元アップロードのスプレッドシート (全運賃→売上按分) の挙動に合わせる。
  // 旧実装は FBA運賃を Amazon 売上だけに分離していたが、Amazon の FBM(自社出荷)分にも FBA手数料が乗り、
  // 逆に FBM の実運賃(ヤマト)が他モールへ逃げる歪みがあったため撤廃。bulk-calculate と同一ロジック。
  const sharedFreight = freightRows.filter(r => r.cost_scope === 'shared').reduce((s, r) => s + r.amount, 0);
  const materialTotal = materialRows.reduce((s, r) => s + r.amount, 0);
  const salesForAlloc = segSales.filter(r => r.segment !== 4).reduce((s, r) => s + (r.sales || 0), 0);
  const salesTotal = segSales.reduce((s, r) => s + (r.sales || 0), 0);
  const fiscalYear = getFiscalYear(year_month);
  const freightTotal = sharedFreight + directFreight.reduce((s, d) => s + d.amount, 0);

  const plRows = segSales.map(row => {
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
      // 国内: shared 運賃 (FBA運賃含む全運賃) を全モール売上で一律按分。
      freight = salesForAlloc > 0 ? Math.round(sharedFreight * sales / salesForAlloc) : 0;
    }
    const material = salesTotal > 0 ? Math.round(materialTotal * sales / salesTotal) : 0;
    const salesRatio = salesTotal > 0 ? sales / salesTotal : 0;
    const variableCost = cost + pfFee + adCost + freight + material;
    const grossProfit = sales - variableCost;
    const grossMargin = sales > 0 ? grossProfit / sales : 0;
    return { year_month, mall_id: row.mall_id, segment: row.segment, sales, sales_ratio: salesRatio,
      cost, pf_fee: pfFee, ad_cost: adCost, freight, material, variable_cost: variableCost,
      gross_profit: grossProfit, gross_margin: grossMargin, fiscal_year: fiscalYear };
  });

  const tx = db.transaction(() => {
    db.prepare('DELETE FROM mgmt_monthly_pl WHERE year_month = ?').run(year_month);
    const plStmt = db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_pl
      (year_month, mall_id, segment, sales, sales_ratio, cost, pf_fee, ad_cost, freight, material, variable_cost, gross_profit, gross_margin, fiscal_year)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    for (const r of plRows) plStmt.run(r.year_month, r.mall_id, r.segment, r.sales, r.sales_ratio, r.cost, r.pf_fee, r.ad_cost, r.freight, r.material, r.variable_cost, r.gross_profit, r.gross_margin, r.fiscal_year);
    db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_closing
      (year_month, fiscal_year, fiscal_month, status, freight_total, material_total, confirmed_at, confirmed_by, calc_version)
      VALUES (?,?,?,?,?,?,?,?,?)`).run(
      year_month, fiscalYear, getFiscalMonth(year_month), 'confirmed', freightTotal, materialTotal, confirmedAt, user, 'v1');
    db.prepare(`INSERT OR REPLACE INTO mart_monthly_shared_costs
      (year_month, freight_total, material_total, confirmed_at, freight_detail, material_detail)
      VALUES (?,?,?,?,?,?)`).run(
      year_month, freightTotal, materialTotal, confirmedAt,
      JSON.stringify(Object.fromEntries(freightRows.map(r => [r.carrier, r.amount]))),
      JSON.stringify(Object.fromEntries(materialRows.map(r => [r.supplier, r.amount]))));
  });
  tx();
  return { rows: plRows.length, freight_total: freightTotal, material_total: materialTotal };
}

// 同期 → 確定実績(closing)のある月は現在データでPLを再計算して confirmed を維持。
// 旧実装は入力変動で needs_review に降格していたが、データ修正のたびに過去月が一斉に
// 非表示になる問題があったため「自動再計算して確定維持」に変更（人手の再確定は不要）。
function syncAndRefreshMonth(db, ym, now) {
  // 凍結月(2026-02以前)は Excel seed が確定値。segment_sales 再生成も再計算もしない。
  if (isFrozenMonth(ym)) return { inserted: 0, refreshed: false, frozen: true };
  return db.transaction(() => {
    // 過去に確定された月のみ自動再計算（新規/下書き月は人手の確定が必要）
    const closing = db.prepare("SELECT confirmed_by FROM mgmt_monthly_closing WHERE year_month = ? AND status IN ('confirmed','needs_review')").get(ym);
    const r = syncSegmentSalesForMonth(db, ym, now); // 内部 tx は savepoint としてネスト
    let refreshed = false;
    if (closing) {
      const res = recomputeMonthlyPL(db, ym, now, closing.confirmed_by || 'auto-sync');
      refreshed = !res.skipped;
    }
    return { ...r, refreshed };
  })();
}

router.post('/api/sync-segment-sales', (req, res) => {
  const db = getMirrorDB();
  const { year_month } = req.body;
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  // 同期 + 確定実績のある月は自動再計算（confirmed 維持）
  const r = syncAndRefreshMonth(db, year_month, now);
  const skippedCarriers = Object.keys(r.auto_freight_skipped || {});
  if (skippedCarriers.length > 0) console.warn('[mgmt-sync] ' + year_month + ': 手入力行があるため自動運賃を上書きしませんでした: ' + skippedCarriers.join(', '));
  res.json({ ok: true, inserted: r.inserted, fba_freight_tax_excluded: r.fba_freight_tax_excluded, auto_freight_tax_excluded: r.auto_freight_tax_excluded || {}, auto_freight_skipped: r.auto_freight_skipped || {}, refreshed: r.refreshed });
});

// ─── 売上自動同期の本体（in-process）───
// 各モール summary + 既存 segment_sales + 確定実績のある closing の和集合の月を再同期し、
// 確定実績のある月は現在データでPLを再計算して confirmed を維持する（過去月の表示を保つ）。
function runMgmtAutoSync(db) {
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const monthsSet = new Set();
  for (const mt of MALL_TABLES) {
    try {
      for (const r of db.prepare(`SELECT DISTINCT year_month FROM ${mt.table}`).all()) monthsSet.add(r.year_month);
    } catch {}
  }
  for (const r of db.prepare('SELECT DISTINCT year_month FROM mart_monthly_segment_sales').all()) monthsSet.add(r.year_month);
  // status を問わず確定実績のある全月（needs_review の旧データも再計算して confirmed に戻す）
  for (const r of db.prepare('SELECT year_month FROM mgmt_monthly_closing').all()) monthsSet.add(r.year_month);
  const months = [...monthsSet].sort();

  let refreshed = 0;
  const freightSkipped = []; // 'YYYY-MM carrier' — 手入力行があって自動運賃を上書きしなかった月×carrier (毎回同じ警告が並ばないよう最後に1行で出す)
  for (const ym of months) {
    const r = syncAndRefreshMonth(db, ym, now); // 月単位の同一tx（同期→確定実績があれば再計算）
    if (r.refreshed) refreshed++;
    for (const carrier of Object.keys(r.auto_freight_skipped || {})) freightSkipped.push(ym + ' ' + carrier);
  }
  if (freightSkipped.length > 0) {
    console.warn('[mgmt-auto-sync] 手入力行があるため自動運賃を上書きしなかった月×carrier ' + freightSkipped.length + '件: ' + freightSkipped.join(', ') + ' (手入力行を消すか金額を合わせてください)');
  }
  return { months: months.length, synced: months.length, refreshed, freight_skipped: freightSkipped.length, freight_skipped_detail: freightSkipped };
}

// 売上自動同期エンドポイント（手動/デバッグ用。MIRROR_SYNC_KEY 認証）。
// 定期実行は Render 常駐スケジューラ(startMgmtAutoSyncScheduler)が in-process で行う。
router.post('/auto-sync-sales', (req, res) => {
  if (!process.env.MIRROR_SYNC_KEY) return res.status(503).json({ error: 'MIRROR_SYNC_KEY 未設定 (fail-closed)' });
  if (!checkAuth(req, res)) return;
  res.json({ ok: true, ...runMgmtAutoSync(getMirrorDB()) });
});

// ─── Render 常駐スケジューラ ───
// この集計は Render の mart_*_monthly_summary を読み Render mirror に書く = Render 完結。
// miniPC に依存せず、Render の web プロセス内で定期的に自動同期する（server.js が RENDER 環境でのみ起動）。
let _mgmtAutoSyncTimer = null;
let _mgmtAutoSyncRunning = false;
function runMgmtAutoSyncSafely(label) {
  if (_mgmtAutoSyncRunning) return;
  _mgmtAutoSyncRunning = true;
  try {
    const r = runMgmtAutoSync(getMirrorDB());
    console.log(`[mgmt-auto-sync] ${label}: ${r.synced}ヶ月同期 / ${r.refreshed}ヶ月再計算`);
    // dead-man 監視 (jobs-registry: mgmt-auto-sync)
    pingJob('mgmt-auto-sync', 'ok', `${label}: ${r.synced}ヶ月同期/${r.refreshed}ヶ月再計算${r.freight_skipped ? ' / 自動運賃スキップ' + r.freight_skipped + '件' : ''}`);
  } catch (e) {
    console.error(`[mgmt-auto-sync] ${label} 失敗:`, e.message);
    pingJob('mgmt-auto-sync', 'fail', e.message);
  } finally {
    _mgmtAutoSyncRunning = false;
  }
}
// テスト用 (easy-ship-freight.test.mjs): 売上同期の純粋ロジックと自動運賃の定義
export { syncSegmentSalesForMonth, syncAndRefreshMonth, runMgmtAutoSync, AUTO_FREIGHT, CARRIERS };

export function startMgmtAutoSyncScheduler() {
  if (_mgmtAutoSyncTimer) return;
  const intervalMin = Math.max(10, parseInt(process.env.MGMT_AUTOSYNC_INTERVAL_MIN) || 120);
  // 起動直後に1回（DB初期化を待って60秒後）、以降は intervalMin ごと
  setTimeout(() => runMgmtAutoSyncSafely('boot'), 60 * 1000).unref?.();
  _mgmtAutoSyncTimer = setInterval(() => runMgmtAutoSyncSafely('interval'), intervalMin * 60 * 1000);
  _mgmtAutoSyncTimer.unref?.();
  console.log(`[mgmt-auto-sync] scheduler started (interval=${intervalMin}min)`);
}

// 集計計算＆確定
router.post('/api/calculate', (req, res) => {
  const db = getMirrorDB();
  const { year_month, allow_partial } = req.body;
  const user = req.session?.email || 'unknown';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  // 凍結月(2026-02以前)は Excel 初期データで確定済み。ライブ再計算は不可。
  if (isFrozenMonth(year_month)) {
    return res.status(400).json({ error: 'frozen_month',
      message: '2026年2月以前は初期データ(Excel)で確定済みのため、ここでは再計算できません。' });
  }

  // 0. 確定直前に各モール確定済みデータを取り込み segment_sales を最新化（サーバ側で確実に実行）。
  //    同期失敗時は古いデータで確定しないよう fail-closed（確定を中止）。
  try {
    syncSegmentSalesForMonth(db, year_month, now);
  } catch (e) {
    return res.status(500).json({ error: '売上同期に失敗したため確定を中止しました: ' + e.message });
  }

  // 1. セグメント売上を取得
  const segSales = db.prepare('SELECT * FROM mart_monthly_segment_sales WHERE year_month = ?').all(year_month);
  if (segSales.length === 0) return res.status(400).json({ error: '売上データがありません' });

  // 1.5 完全性チェック: 直近3ヶ月で当月を確定しているモールが当月も確定済みか。
  //     揃う前に確定すると「売上過少 × 運賃満額 → 粗利マイナス」事故になるため。
  //     判定は「各モール集計テーブル(mart_<mall>_monthly_summary)にその月の確定があるか」で行う。
  //     segment_sales の分類済み行ではなく確定の有無で見る（分類行ゼロ＝全て"その他"でも
  //     確定済みなら揃いとみなす。低頻度モールや未分類SKUでの誤検知を防ぐ）。allow_partial で強制可。
  const hasSummary = (table, fromYm, toYm) => {
    try {
      return toYm
        ? !!db.prepare(`SELECT 1 FROM ${table} WHERE year_month >= ? AND year_month < ? LIMIT 1`).get(fromYm, toYm)
        : !!db.prepare(`SELECT 1 FROM ${table} WHERE year_month = ? LIMIT 1`).get(fromYm);
    } catch { return false; }
  };
  const missingMalls = MALL_TABLES
    .filter(mt => hasSummary(mt.table, addMonths(year_month, -3), year_month) && !hasSummary(mt.table, year_month))
    .map(mt => mt.mall_id);
  if (missingMalls.length > 0 && allow_partial !== true) {
    const names = missingMalls.map(m => MALL_NAMES[m] || m);
    return res.status(409).json({
      error: 'incomplete_malls',
      missing_malls: missingMalls,
      message: names.join('・') + ' の当月データがまだ確定されていません。全モール確定してから確定してください。',
    });
  }

  // 2-5. PLを再計算して確定保存（auto-sync と共有ロジック）
  const result = recomputeMonthlyPL(db, year_month, now, user);
  res.json({ ok: true, rows: result.rows, freight_total: result.freight_total, material_total: result.material_total });
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
  // 表示は確定済みの月のみ（未確定月がゼロ列で出ないようにする）
  const confirmedSet = new Set(
    db.prepare("SELECT year_month FROM mgmt_monthly_closing WHERE fiscal_year = ? AND status = 'confirmed'").all(fy).map(r => r.year_month)
  );
  const months = getFiscalYearMonths(fy).filter(m => confirmedSet.has(m));

  // セグメント別×月で集約
  const rows = db.prepare(`
    SELECT year_month, segment,
      SUM(sales) as sales, SUM(cost) as cost, SUM(pf_fee) as pf_fee,
      SUM(ad_cost) as ad_cost, SUM(freight) as freight, SUM(material) as material,
      SUM(variable_cost) as variable_cost, SUM(gross_profit) as gross_profit
    FROM mgmt_monthly_pl WHERE fiscal_year = ?
      AND year_month IN (SELECT year_month FROM mgmt_monthly_closing WHERE status = 'confirmed')
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

  // 表示は確定済み(status='confirmed')の月のみ。未確定・要再確定(needs_review)は除外し、
  // 途中・不完全な月（売上過少→粗利マイナス）がグラフに出るのを防ぐ。
  const months = db.prepare("SELECT year_month FROM mgmt_monthly_closing WHERE status = 'confirmed' ORDER BY year_month")
    .all().map(r => r.year_month).slice(-limit);
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
.status-needs_review { background: #fce8e6; color: #c5221f; }
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
/* ── 月次/年間 PL テーブル 視覚強調 ── */
/* 合計列(年間PL 右端)を強調 */
#annualTable td:last-child, #annualTable th:last-child { background:#eef4ff; font-weight:700; }
/* セグメントブロックの区切り線 */
#annualTable tr.seg-start > td { border-top:2px solid #c7d2e8; }
/* 粗利益率の行を一段目立たせる(ボトムライン) */
#annualTable tr.pl-margin { background:#f5f8ff; }
#annualTable tr.pl-margin td { font-weight:700; }
#annualTable tr.total-row.pl-margin td { border-bottom:2px solid #1a73e8; }
/* 月次PL アコーディオン: PF ヘッダーのホバー強調 */
#monthlyTable tr.pf-header:hover > td { background:#e3ebf7; }
/* 月次PL: 詳細行(セグメント)の行頭を少し沈める */
#monthlyTable tr.pf-detail td:nth-child(2) { color:#555; }
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

  <div class="card" style="border:1px solid #d7e0ef; background:#fafcff;">
    <h3>📥 過去データ(Excel初期値)投入 — 2026年2月以前</h3>
    <p class="note-text">2026年2月以前は Excel 初期データを<b>そのまま確定値</b>として表示します(ライブ計算・上書きなし)。まず「差分を確認」で Excel値 と 現状表示 を見比べ、問題なければ「投入する」を押してください。</p>
    <div class="controls">
      <button class="btn btn-outline" onclick="seedDryRun()">差分を確認（ドライラン）</button>
      <button class="btn btn-success" id="btnSeedApply" onclick="seedApply()" style="display:none">この内容で投入する</button>
      <span id="seedStatus" class="note-text"></span>
    </div>
    <div id="seedReport"></div>
  </div>

  <div class="card" style="border:1px solid #f0c8c8; background:#fff8f8;">
    <h3>🗑️ 2024年7月より前の誤りデータを削除</h3>
    <p class="note-text">2024年7月より前（＝<b>2024年6月以前</b>）のデータは誤りのため削除します。凍結月なので削除後に再計算で復活しません。まず「削除対象を確認」で対象月・件数を見て、問題なければ「削除する」を押してください。<b>取り消せません。</b></p>
    <div class="controls">
      <button class="btn btn-outline" onclick="purgeDryRun()">削除対象を確認（ドライラン）</button>
      <button class="btn" id="btnPurgeApply" onclick="purgeApply()" style="display:none; background:#c5221f; color:#fff">この内容で削除する</button>
      <span id="purgeStatus" class="note-text"></span>
    </div>
    <div id="purgeReport"></div>
  </div>
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
const AUTO_FREIGHT = ${JSON.stringify(AUTO_FREIGHT)}; // 自動運賃 (FBA運賃 / Easy Ship運賃): 読み取り専用・保存対象外
const EXPORT_CARRIERS = ${JSON.stringify(EXPORT_CARRIERS)};
const SUPPLIERS = ${JSON.stringify(SUPPLIERS)};

// ─── ユーティリティ ───
const fmt = n => (n || 0).toLocaleString('ja-JP');
const fmtPct = n => ((n || 0) * 100).toFixed(1) + '%';
const fmtRatio = (n, d) => d > 0 ? fmtPct(n / d) : '-';
const clsVal = n => n < 0 ? 'negative' : n > 0 ? 'positive' : '';
const STATUS_LABELS = { confirmed: '確定済', draft: '未確定', needs_review: '要再確定' };
const statusLabel = s => STATUS_LABELS[s] || s;
function statusBadge(closing) {
  if (!closing) return '<span class="status-badge status-draft">未確定</span>';
  return '<span class="status-badge status-' + closing.status + '">' + statusLabel(closing.status) + '</span> ' + (closing.confirmed_at || '');
}

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
    // FBA運賃 / Easy Ship運賃 は Amazon ペイメント (by_segment) から自動計算。手入力で上書きさせない（読み取り専用・保存対象外）
    const auto = AUTO_FREIGHT.find(a => a.carrier === name);
    if (auto) {
      tr.innerHTML = '<td>' + name + ' <span style="font-size:11px;color:#888">（自動）</span></td>'
        + '<td><input type="number" class="input-amount" data-name="' + name + '" data-auto="1" value="' + amountInc + '" readonly style="background:#f1f3f4;color:#666;cursor:not-allowed"></td>'
        + '<td style="font-size:12px;color:#888">' + auto.label + '</td>';
    } else {
      tr.innerHTML = '<td>' + name + '</td>'
        + '<td><input type="number" class="input-amount" data-name="' + name + '" value="' + amountInc + '" onchange="updateTotals()"></td>'
        + '<td><input type="text" style="width:150px;padding:6px;border:1px solid #ddd;border-radius:4px;font-size:13px;" data-note="' + name + '" value="' + note + '"></td>';
    }
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
  document.getElementById('closingStatus').innerHTML = statusBadge(data.closing);
}

async function saveCosts() {
  const ym = document.getElementById('costMonth').value;
  if (!ym) return;
  // 運賃（国内）入力は税込→税抜で保存
  const freightItems = [];
  document.querySelectorAll('#freightBody .input-amount').forEach(i => {
    if (i.dataset.auto) return; // FBA運賃 / Easy Ship運賃 は自動管理なので保存対象外（手入力で上書きしない）
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
  const autoMap = data.auto_freight_tax_excluded || (data.fba_freight_tax_excluded ? { 'FBA運賃': data.fba_freight_tax_excluded } : {});
  for (const af of AUTO_FREIGHT) {
    if (autoMap[af.carrier]) msg += ' / ' + af.carrier + ' 自動登録: ¥' + Math.round(autoMap[af.carrier] * 1.1).toLocaleString() + '（税込）';
  }
  // 同名 carrier の手入力行があって自動値を入れなかった場合は警告 (手入力行を消すか金額を合わせる必要がある)
  const skipped = data.auto_freight_skipped || {};
  for (const [carrier, s] of Object.entries(skipped)) {
    msg += ' / ⚠ ' + carrier + ': 手入力行 (¥' + Math.round((s.existing_amount || 0) * 1.1).toLocaleString() + '・' + (s.entered_by || '?') + ') があるため自動値 ¥'
      + Math.round((s.auto_amount || 0) * 1.1).toLocaleString() + ' で上書きしていません';
  }
  toast(msg);
  await loadCosts();
}

async function doCalculate() {
  const ym = document.getElementById('costMonth').value;
  if (!ym) return;
  if (!confirm(ym + ' の集計を確定しますか？')) return;
  await saveCosts();
  // 集計確定（サーバ側で確定直前に売上同期を行うため、ここでの手動同期は不要）
  await postCalculate(ym, false);
}

// ── 過去データ(Excel初期値)投入 ──
function renderSeedReport(report) {
  let h = '<table style="margin-top:10px"><thead><tr><th>月</th><th>Excel売上</th><th>Excel粗利率</th><th>現状売上</th><th>現状粗利率</th></tr></thead><tbody>';
  for (const r of report) {
    const diff = (r.seed_margin !== r.current_margin);
    h += '<tr><td>' + r.year_month + '</td><td>' + fmt(r.seed_sales) + '</td>'
      + '<td>' + r.seed_margin + '%</td><td>' + fmt(r.current_sales) + '</td>'
      + '<td class="' + (diff ? 'negative' : '') + '">' + r.current_margin + '%</td></tr>';
  }
  h += '</tbody></table>';
  document.getElementById('seedReport').innerHTML = h;
}
async function seedDryRun() {
  const st = document.getElementById('seedStatus'); st.textContent = '確認中…';
  try {
    const res = await fetch(BASE + '/admin/load-historical-seed?dryRun=1', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' });
    const d = await res.json();
    if (!d.ok) throw new Error(d.message || d.error || 'error');
    renderSeedReport(d.report);
    document.getElementById('btnSeedApply').style.display = '';
    st.textContent = d.months + 'ヶ月。投入すると左(Excel)の値で確定表示になります。';
  } catch (e) { st.textContent = 'エラー: ' + e.message; }
}
async function seedApply() {
  if (!confirm('2026年2月以前の各月を Excel 初期データで確定表示に置き換えます。よろしいですか？')) return;
  const st = document.getElementById('seedStatus'); st.textContent = '投入中…';
  try {
    const res = await fetch(BASE + '/admin/load-historical-seed', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' });
    const d = await res.json();
    if (!d.ok) throw new Error(d.message || d.error || 'error');
    st.textContent = '✅ ' + d.months_loaded + 'ヶ月投入完了（' + d.rows + '行）。年間PL/月次PLを再表示して確認してください。';
    document.getElementById('btnSeedApply').style.display = 'none';
  } catch (e) { st.textContent = 'エラー: ' + e.message; }
}

// ── 2024年7月より前の誤りデータ削除 ──
const PURGE_BEFORE = '2024-07';
function renderPurgeReport(d) {
  const c = d.deleted || d.counts || {};
  let h = '<div class="note-text" style="margin-top:8px">削除対象月（' + (d.months || []).length + 'ヶ月）: ' + ((d.months || []).join(', ') || 'なし') + '</div>';
  h += '<table style="margin-top:6px"><thead><tr><th>テーブル</th><th>削除件数</th></tr></thead><tbody>';
  for (const t of Object.keys(c)) h += '<tr><td>' + t + '</td><td>' + c[t] + '</td></tr>';
  h += '</tbody></table>';
  document.getElementById('purgeReport').innerHTML = h;
}
async function purgeDryRun() {
  const st = document.getElementById('purgeStatus'); st.textContent = '確認中…';
  try {
    const res = await fetch(BASE + '/admin/purge-months-before?dryRun=1', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ before: PURGE_BEFORE }) });
    const d = await res.json();
    if (!d.ok) throw new Error(d.message || d.error || 'error');
    renderPurgeReport(d);
    document.getElementById('btnPurgeApply').style.display = (d.months || []).length ? '' : 'none';
    st.textContent = (d.months || []).length ? '上の対象を削除します（取り消せません）。' : '削除対象はありません。';
  } catch (e) { st.textContent = 'エラー: ' + e.message; }
}
async function purgeApply() {
  if (!confirm('2024年7月より前（2024年6月以前）のmgmtデータを削除します。取り消せません。よろしいですか？')) return;
  const st = document.getElementById('purgeStatus'); st.textContent = '削除中…';
  try {
    const res = await fetch(BASE + '/admin/purge-months-before', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ before: PURGE_BEFORE }) });
    const d = await res.json();
    if (!d.ok) throw new Error(d.message || d.error || 'error');
    renderPurgeReport(d);
    st.textContent = '✅ 削除完了（' + (d.months || []).length + 'ヶ月）。年間PL/会計年度プルダウンを再表示して確認してください。';
    document.getElementById('btnPurgeApply').style.display = 'none';
  } catch (e) { st.textContent = 'エラー: ' + e.message; }
}

async function postCalculate(ym, allowPartial) {
  const res = await fetch(BASE + '/api/calculate', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ year_month: ym, allow_partial: allowPartial }),
  });
  const data = await res.json();
  if (res.status === 409 && data.error === 'incomplete_malls') {
    if (confirm(data.message + '\\n\\nこのまま不完全な状態で確定しますか？（推奨しません）')) {
      await postCalculate(ym, true);
    }
    return;
  }
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

  document.getElementById('monthlyStatus').innerHTML = statusBadge(data.closing);

  if (data.rows.length === 0) {
    document.getElementById('monthlyBody').innerHTML = '<tr><td colspan="17">データがありません。運賃・資材費を入力して集計確定してください。</td></tr>';
    document.getElementById('monthlySummary').innerHTML = '';
    return;
  }

  // 米国Amazon輸出(segment 4) は 米国Amazon(amazon_usa) PF 以外では非表示 (他モールは常に0の空行)。
  // segment は数値前提だが念のため Number() で型ゆれ吸収。
  const visibleRows = data.rows.filter(r => !(Number(r.segment) === 4 && r.mall_id !== 'amazon_usa'));

  // サマリー
  const totals = visibleRows.reduce((a, r) => ({
    sales: a.sales + r.sales, cost: a.cost + r.cost, pf_fee: a.pf_fee + r.pf_fee,
    ad_cost: a.ad_cost + r.ad_cost, freight: a.freight + r.freight, material: a.material + r.material,
    variable_cost: a.variable_cost + r.variable_cost, gross_profit: a.gross_profit + r.gross_profit,
  }), { sales:0, cost:0, pf_fee:0, ad_cost:0, freight:0, material:0, variable_cost:0, gross_profit:0 });

  document.getElementById('monthlySummary').innerHTML =
    '<div class="summary-item"><div class="label">売上高</div><div class="value">' + fmt(totals.sales) + '</div></div>' +
    '<div class="summary-item"><div class="label">変動費</div><div class="value">' + fmt(totals.variable_cost) + '</div></div>' +
    '<div class="summary-item"><div class="label">粗利益</div><div class="value ' + clsVal(totals.gross_profit) + '">' + fmt(totals.gross_profit) + '</div></div>' +
    '<div class="summary-item"><div class="label">粗利率</div><div class="value ' + clsVal(totals.gross_profit) + '">' + fmtRatio(totals.gross_profit, totals.sales) + '</div></div>';

  // テーブル: PF(プラットフォーム) ごとにアコーディオン。
  // PF 小計ヘッダー(クリックで開閉) → 配下に セグメント詳細行(デフォルト折りたたみ)。
  const sumRows = (rows) => rows.reduce((a, r) => ({
    sales: a.sales + r.sales, cost: a.cost + r.cost, pf_fee: a.pf_fee + r.pf_fee,
    ad_cost: a.ad_cost + r.ad_cost, freight: a.freight + r.freight, material: a.material + r.material,
    variable_cost: a.variable_cost + r.variable_cost, gross_profit: a.gross_profit + r.gross_profit,
  }), { sales:0, cost:0, pf_fee:0, ad_cost:0, freight:0, material:0, variable_cost:0, gross_profit:0 });

  // PF ごとにグループ化 (Map で proto 汚染回避)。
  const pfOrder = [];
  const pfGroups = new Map();
  for (const r of visibleRows) {
    if (!pfGroups.has(r.mall_id)) { pfGroups.set(r.mall_id, []); pfOrder.push(r.mall_id); }
    pfGroups.get(r.mall_id).push(r);
  }
  // 並びは売上高の大きい順。APIは mall_id 順 (= ABC順) で返すため、2位の楽天・3位のYahoo! が
  // 小さいモールの下に埋もれて読みにくかった (2026-09-01 代表指摘)。
  // 同額なら元の順のまま (Array#sort は安定ソート)。
  // セグメントはランキングではなく定義された区分なので、PF配下の並びは触らない。
  const pfSales = new Map();
  for (const [id, rows] of pfGroups) pfSales.set(id, sumRows(rows).sales);
  pfOrder.sort((a, b) => pfSales.get(b) - pfSales.get(a));

  let html = '';
  // DOM キーは mall_id ではなく行番号(index)を使う → onclick/class/selector への文字列注入面を排除。
  for (let pfIdx = 0; pfIdx < pfOrder.length; pfIdx++) {
    const mallId = pfOrder[pfIdx];
    const rows = pfGroups.get(mallId);
    const t = sumRows(rows);
    const pfName = MALL_NAMES[mallId] || mallId;
    // PF 小計ヘッダー(クリックで配下セグメントを開閉)。売上比率は PF売上 / 全体売上。
    html += '<tr class="pf-header" onclick="togglePF(' + pfIdx + ')" style="cursor:pointer; background:#eef2f7; font-weight:600;">'
      + '<td><span class="pf-caret" id="caret-' + pfIdx + '">▶</span> ' + pfName + '</td>'
      + '<td>（全体）</td>'
      + '<td>' + fmt(t.sales) + '</td>'
      + '<td>' + fmtRatio(t.sales, totals.sales) + '</td>'
      + '<td>' + fmt(t.cost) + '</td><td>' + fmtRatio(t.cost, t.sales) + '</td>'
      + '<td>' + fmt(t.pf_fee) + '</td><td>' + fmtRatio(t.pf_fee, t.sales) + '</td>'
      + '<td>' + fmt(t.ad_cost) + '</td><td>' + fmtRatio(t.ad_cost, t.sales) + '</td>'
      + '<td>' + fmt(t.freight) + '</td><td>' + fmtRatio(t.freight, t.sales) + '</td>'
      + '<td>' + fmt(t.material) + '</td><td>' + fmtRatio(t.material, t.sales) + '</td>'
      + '<td>' + fmt(t.variable_cost) + '</td>'
      + '<td class="' + clsVal(t.gross_profit) + '">' + fmt(t.gross_profit) + '</td>'
      + '<td class="' + clsVal(t.gross_profit) + '">' + fmtRatio(t.gross_profit, t.sales) + '</td>'
      + '</tr>';
    // セグメント詳細 (デフォルト折りたたみ)
    for (const r of rows) {
      html += '<tr class="pf-detail pf-detail-' + pfIdx + '" style="display:none; background:#fbfcfe;">'
        + '<td></td>'
        + '<td style="padding-left:1.6em;">' + (SEGMENT_NAMES[r.segment] || r.segment) + '</td>'
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

// PF アコーディオン開閉: 配下の .pf-detail-<index> 行を一括トグル + キャレット切替。
// キーは行番号(数値)なので class/selector への文字列注入はない。
function togglePF(pfIdx) {
  const rows = document.querySelectorAll('.pf-detail-' + pfIdx);
  if (!rows.length) return;
  const willShow = rows[0].style.display === 'none';
  rows.forEach(function (tr) { tr.style.display = willShow ? '' : 'none'; });
  const caret = document.getElementById('caret-' + pfIdx);
  if (caret) caret.textContent = willShow ? '▼' : '▶';
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
      html += (fi === 0 ? '<tr class="seg-start">' : '<tr>');
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
    html += '<tr class="pl-margin"><td>粗利益率</td>';
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
  html += '<tr class="total-row pl-margin"><td>粗利益率</td>';
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
