// rakuten-analytics 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/rakuten-analytics/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import fs from 'node:fs';
import path from 'node:path';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';

// 本番 DB 誤実行ガード (Codex R1 High → R2 High で決定的判定に強化):
// ① DATA_DIR 未指定 (= cwd の本番 DB に向く) なら即中断
// ② サンドボックスマーカー方式: DATA_DIR に既存 DB があるのに .ra-smoke-sandbox マーカーが
//    無ければ「smoke が作った DB ではない」ので中断。ヒューリスティック (行数閾値) に頼らない
const dataDir = process.env.DATA_DIR;
if (!dataDir) {
  console.error('FATAL: DATA_DIR が未指定です。smoke 専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ra-smoke-data)');
  process.exit(2);
}
const marker = path.join(dataDir, '.ra-smoke-sandbox');
const dbFile = path.join(dataDir, 'warehouse-mirror.db');
if (fs.existsSync(dbFile) && !fs.existsSync(marker)) {
  console.error(`FATAL: ${dbFile} は smoke が作成した DB ではありません (マーカー ${marker} なし)。本番/既存 DB の可能性があるため中断します`);
  process.exit(2);
}
// DB がまだ無い場合も、マーカー無しの非空ディレクトリなら承認しない (本番 DATA_DIR の誤指定対策 — Codex R3 Medium)
if (!fs.existsSync(dbFile) && !fs.existsSync(marker) && fs.existsSync(dataDir)
    && fs.readdirSync(dataDir).length > 0) {
  console.error(`FATAL: ${dataDir} は空ではありません。smoke 専用の空ディレクトリを指定してください`);
  process.exit(2);
}
fs.mkdirSync(dataDir, { recursive: true });
fs.writeFileSync(marker, `rakuten-analytics smoke sandbox (created ${new Date().toISOString()})\n`);

initMirrorDB();
const db = getMirrorDB();

const today = q.jstToday();
const d = (n) => q.addDays(today, -n);
const ymNow = today.slice(0, 7);
const ymPrev = q.addMonths(ymNow, -1);

// ─── fixture ───
const tx = db.transaction(() => {
  for (const t of ['mirror_rakuten_finance_sku_daily', 'mart_rakuten_monthly_summary']) {
    try { db.exec(`DELETE FROM ${t}`); } catch {}
  }

  const ins = db.prepare(`INSERT INTO mirror_rakuten_finance_sku_daily (
    date_jst, rakuten_code, ne_code, sku_resolution, product_name,
    units_ordered, units_cancelled, units_net_sold, allocated_units_cancelled,
    sales_principal_jpy_incl, sales_postage_jpy_incl,
    coupon_shop_jpy_incl, coupon_all_jpy_incl, promotion_jpy_incl,
    refund_amount_jpy_incl, allocated_refund_amount_jpy_incl,
    mall_fee_jpy_incl, shipping_cost_jpy_incl, shipping_quality,
    unit_cost_snapshot_incl, cogs_amount_jpy_incl,
    gross_sales_jpy_incl, net_sales_jpy_incl, variable_margin_jpy_incl,
    refund_adjusted_net_sales_jpy_incl,
    cost_status, is_cost_complete, data_quality_score,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, 'resolved', ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 'actual', ?, ?, ?, ?, ?, ?, 'complete', 1, 100, 'smoke', 'h', 't')`);

  // 90日分 2 SKU (税込)。alpha: 10個/日 単価1000、beta: 4個/日 単価2000
  for (let i = 0; i < 90; i++) {
    const date = d(i);
    {
      // alpha: gross=10500 (principal 10000 + postage 500)、クーポン店負300、返金200
      const gross = 10500, coupon = 300, refund = 200;
      const net = gross - coupon;                       // 10200
      const mallFee = Math.round((10000 + 500 - 500) * 0.10); // coupon_all=500 → 課金ベース10000 → 1000
      const shipping = 800, cogs = 3000;
      const vm = net - refund - mallFee - shipping - cogs;    // 5200
      ins.run(date, 'rk-alpha', 'NE-A', 'アルファ精油', 10, 1, 9, 1,
        10000, 500, coupon, 500, refund, refund,
        mallFee, shipping, 300, cogs, gross, net, vm, net - refund);
    }
    {
      // beta: gross=8200、クーポンなし、返金なし
      const gross = 8200, mallFee = 820, shipping = 600, cogs = 4000;
      const vm = gross - mallFee - shipping - cogs;     // 2780
      ins.run(date, 'rk-beta', 'NE-B', 'ベータ茶葉', 4, 0, 4, 0,
        8000, 200, 0, 0, 0, 0,
        mallFee, shipping, 1000, cogs, gross, gross, vm, gross);
    }
  }

  // 先月は仕訳書取込済み (確定): 広告費 50,000 / 楽天手数料実額 90,000
  db.prepare(`INSERT INTO mart_rakuten_monthly_summary (year_month, ad_cost, pf_fee, confirmed_at)
    VALUES (?, 50000, 90000, 't')`).run(ymPrev);
});
tx();

// ─── 実行 ───
let pass = 0, fail = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
    pass++;
  } catch (e) {
    console.error(`✗ ${name}: ${e.message}`);
    fail++;
  }
}
function assert(cond, msg) { if (!cond) throw new Error(msg); }

check('resolvePeriod', () => {
  assert(q.resolvePeriod('last_month').from === q.monthStart(ymPrev), 'last_month from');
  assert(q.resolvePeriod('bogus').preset === '30d', 'fallback 30d');
  const c = q.resolvePeriod(null, '2026-01-01', '2026-01-31');
  assert(c.preset === 'custom' && c.from === '2026-01-01', 'custom range');
  assert(q.resolvePeriod(null, '2026-01-31', '2026-01-01').preset === '30d', '逆順は fallback');
  assert(!q.isValidDate('2026-02-30'), '存在しない日付');
  const wide = q.resolvePeriod(null, '2000-01-01', '2026-01-01');
  assert(wide.from === q.addDays('2026-01-01', -730), `巨大range clamp (got ${wide.from})`);
});

check('getTrend day→week 自動格上げ', () => {
  const r = q.getTrend(d(365), today, 'day');
  assert(r.granularity === 'week', `半年超の day は week に (got ${r.granularity})`);
});

check('getOverview', () => {
  const r = q.getOverview();
  assert(r.tiles.length === 4, 'tiles 4枚');
  assert(r.data_to === today, `data_to=${r.data_to}`);
  const tYest = r.tiles.find(t => t.key === 'yesterday');
  assert(tYest.sales_incl === 10500 + 8200, `昨日売上 (got ${tYest.sales_incl})`);
  assert(tYest.units_net === 13, `昨日販売数 (got ${tYest.units_net})`);
  assert(tYest.variable_margin === 5200 + 2780, `昨日粗利 (got ${tYest.variable_margin})`);
  assert(tYest.margin_pct !== null && tYest.cost_coverage_pct === 100, '率とカバー率');
  const tLast = r.tiles.find(t => t.key === 'last_month');
  assert(tLast.confirmed !== null && tLast.confirmed !== undefined, '先月は確定あり');
  assert(tLast.confirmed.ad_cost === 50000 && tLast.confirmed.pf_fee === 90000, '確定値');
  // 確定寄せ = VM + mall_fee(推定戻し) − pf_fee − ad_cost
  const days = tLast.days_with_data;
  const expected = (5200 + 2780) * days + (1000 + 820) * days - 90000 - 50000;
  assert(tLast.confirmed.full_margin === expected, `確定寄せ実質利益 (got ${tLast.confirmed.full_margin}, want ${expected})`);
  const tMonth = r.tiles.find(t => t.key === 'this_month');
  assert(tMonth.confirmed === null, '今月は未確定 (confirmed=null)');
});

check('getTrend day', () => {
  const r = q.getTrend(d(29), today, 'day');
  assert(r.rows.length === 30, `30日分 (got ${r.rows.length})`);
  const row = r.rows[0];
  assert(row.sales_incl === 18700, `日次売上 (got ${row.sales_incl})`);
  assert(row.margin_pct !== null, 'margin_pct');
  assert(row.cancel_rate_pct !== null, 'cancel_rate');
  assert(r.confirmed_months.length === 0, '日次では confirmed なし');
});

check('getTrend month + 確定オーバーレイ', () => {
  const r = q.getTrend(d(89), today, 'month');
  assert(r.rows.length >= 3, `3ヶ月分 (got ${r.rows.length})`);
  const c = r.confirmed_months.find(x => x.year_month === ymPrev);
  assert(c, '先月の確定行あり');
  assert(c.ad_cost === 50000 && c.pf_fee === 90000, '確定値');
  const prevRow = r.rows.find(x => x.bucket === ymPrev);
  assert(c.full_margin === prevRow.variable_margin + prevRow.mall_fee_est - 90000 - 50000, 'full_margin 整合');
});

check('getTrend week (bucket=月曜日始まり)', () => {
  const r = q.getTrend(d(29), today, 'week');
  assert(r.rows.length >= 4 && r.rows.length <= 6, `週次バケット (got ${r.rows.length})`);
  for (const row of r.rows) {
    assert(/^\d{4}-\d{2}-\d{2}$/.test(row.bucket), `bucket は日付 (got ${row.bucket})`);
    const dow = new Date(row.bucket + 'T00:00:00Z').getUTCDay();
    assert(dow === 1, `bucket は月曜 (got dow=${dow} for ${row.bucket})`);
  }
});

check('空期間 (データなし) は 0 で返る', () => {
  const r = q.getTrend('2020-01-01', '2020-01-31', 'day');
  assert(r.rows.length === 0, 'rows 空');
  const ov = q.getOverview();  // 今日タイルはデータあり得るので単に throw しないこと
  assert(ov.tiles.length === 4, 'overview は常に4枚');
});

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
