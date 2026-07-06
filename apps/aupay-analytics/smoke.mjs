// aupay-analytics 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/aupay-analytics/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import fs from 'fs';
import path from 'path';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';

// 本番 DB 誤実行ガード 1: DATA_DIR 未指定 (= cwd の本番 DB に向く) なら即中断
if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください (例: DATA_DIR=c:/tmp/apa-smoke-data)');
  process.exit(2);
}

// 本番 DB 誤実行ガード 2: marker file 方式。
// この smoke が過去に作った DB (marker あり) 以外の既存 warehouse-mirror.db は DELETE しない
const markerPath = path.join(process.env.DATA_DIR, '.aupay-analytics-smoke-db');
const dbExisted = fs.existsSync(path.join(process.env.DATA_DIR, 'warehouse-mirror.db'));
if (dbExisted && !fs.existsSync(markerPath)) {
  console.error('FATAL: DATA_DIR に既存の warehouse-mirror.db があります (smoke 生成マーカーなし)。実 DB の可能性があるため中断します');
  process.exit(2);
}

initMirrorDB();
const db = getMirrorDB();
fs.writeFileSync(markerPath, `created by apps/aupay-analytics/smoke.mjs at ${new Date().toISOString()}\n`);

// 本番 DB 誤実行ガード 3: fixture 規模を超えるデータがあれば実 DB とみなし中断
const factGuard = db.prepare(`SELECT COUNT(*) AS c FROM mirror_aupay_finance_sku_daily`).get().c;
const listGuard = db.prepare(`SELECT COUNT(*) AS c FROM mirror_f_sales_by_listing`).get().c;
if (factGuard > 500 || listGuard > 1000) {
  console.error(`FATAL: 実データらしき規模を検出 (fact ${factGuard} 行 / listing ${listGuard} 行)。本番 DB と思われるため中断します`);
  process.exit(2);
}

const today = q.jstToday();
const d = (n) => q.addDays(today, -n);
const ymNow = today.slice(0, 7);
const ymPrev = q.addMonths(ymNow, -1);

// ─── fixture ───
const tx = db.transaction(() => {
  for (const t of ['mirror_aupay_finance_sku_daily', 'mirror_f_sales_by_listing']) {
    try { db.exec(`DELETE FROM ${t}`); } catch {}
  }

  const ins = db.prepare(`INSERT INTO mirror_aupay_finance_sku_daily (
    date_jst, aupay_sku_key, ne_code, variant_key, resolution_method, unresolved_sku_flag,
    product_name, units_ordered, units_cancelled, units_net_sold,
    sales_principal_jpy_incl, postage_allocated_jpy_incl, gross_sales_jpy_incl,
    net_sales_after_coupon_jpy_incl, request_price_jpy_incl,
    coupon_shop_jpy_incl, gift_point_jpy_incl,
    mall_fee_jpy_incl, mall_fee_rate_applied, mall_fee_calc_method,
    shipping_cost_jpy_incl, shipping_quality,
    unit_cost_snapshot_incl, cogs_amount_jpy_incl,
    variable_margin_partial_jpy_incl, margin_confidence,
    cost_status, is_cost_complete, data_quality_score,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'partial', ?, ?, ?, 'smoke', 'h', 't')`);

  const insListing = db.prepare(`INSERT INTO mirror_f_sales_by_listing (
    date_jst, month_ym, mall, item_code, channel, item_name, units, sales_jpy_incl, order_count,
    data_source, source_updated_at, source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, ?, '', ?, ?, ?, ?, 'smoke', 't', 'smoke', ?, 't')`);

  // 90日分 3 SKU (税込。au PAY: 全送料無料プラン = 売上に送料行なし・送料負担が重い構造)
  for (let i = 0; i < 90; i++) {
    const date = d(i);
    {
      // alpha (master_match): 10個/日 単価1000。クーポン300。手数料13% (推定)。送料負担が重い
      const gross = 10000, coupon = 300, fee = 1261, shipping = 1700, cogs = 5000, gift = 93;
      const vm = gross - cogs - fee - shipping - coupon; // 1739
      ins.run(date, 'apa-alpha-bk', 'NE-A', 'BK', 'master_match', 0, 'アルファ精油BK', 10, 10,
        gross, gross, gross - coupon, gross - coupon,
        coupon, gift, fee, 0.13, 'estimated_rate', shipping, 'estimated_rates',
        500, cogs, vm, 'complete', 1, 100);
    }
    {
      // beta (master_match): 4個/日 単価2000。控除なし
      const gross = 8000, fee = 1040, shipping = 1360, cogs = 4000, gift = 74;
      const vm = gross - cogs - fee - shipping; // 1600
      ins.run(date, 'apa-beta', 'NE-B', '', 'master_match', 0, 'ベータ茶葉', 4, 4,
        gross, gross, gross, gross,
        0, gift, fee, 0.13, 'estimated_rate', shipping, 'estimated_rates',
        1000, cogs, vm, 'complete', 1, 100);
    }
    {
      // gamma (unresolved): 1個/日 単価500。原価0円計上 = 粗利過大の典型。mall_fee は unknown (NULL)
      const gross = 500, gift = 5;
      const vm = gross; // 500 (原価/手数料/送料とも計上なし)
      ins.run(date, 'apa-gamma', null, '', 'unresolved', 1, 'ガンマ雑貨', 1, 1,
        gross, gross, gross, gross,
        0, gift, null, null, 'unknown', 0, 'missing',
        null, 0, vm, 'missing_cost', 0, 40);
    }
    // 速報 (NE受注): fact より大きい = 未完了注文を含む。他モール行はフィルタされること
    insListing.run(date, date.slice(0, 7), 'aupay', 'apa-alpha', 'アルファ精油', 16, 20000, 12, `a-${date}`);
    insListing.run(date, date.slice(0, 7), 'rakuten', 'rk-x', '楽天ノイズ', 99, 99999, 99, `r-${date}`);
  }
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

const DAY_SALES = 10000 + 8000 + 500;   // 18500
const DAY_L1 = 1739 + 1600 + 500;       // 3839
const DAY_GIFT = 93 + 74 + 5;           // 172
const DAY_L2 = DAY_L1 - DAY_GIFT;       // 3667
const DAY_SHIP = 1700 + 1360;           // 3060

check('resolvePeriod', () => {
  assert(q.resolvePeriod('last_month').from === q.monthStart(ymPrev), 'last_month from');
  assert(q.resolvePeriod('bogus').preset === '30d', 'fallback 30d');
  const c = q.resolvePeriod(null, '2026-01-01', '2026-01-31');
  assert(c.preset === 'custom' && c.from === '2026-01-01', 'custom range');
  assert(q.resolvePeriod(null, '2026-01-31', '2026-01-01').preset === '30d', '逆順は fallback');
  assert(!q.isValidDate('2026-02-30'), '存在しない日付');
  const wide = q.resolvePeriod(null, '2000-01-01', '2026-01-01');
  assert(wide.from === q.addDays('2026-01-01', -729), `巨大range clamp 両端含み730日 (got ${wide.from})`);
});

check('isSantaroDay', () => {
  assert(q.isSantaroDay('2026-07-03') && q.isSantaroDay('2026-07-13') && q.isSantaroDay('2026-07-23'), '3/13/23');
  assert(!q.isSantaroDay('2026-07-30') && !q.isSantaroDay('2026-07-02'), '30日/2日は非該当');
});

check('getTrend day→week 自動格上げ', () => {
  const r = q.getTrend(d(365), today, 'day');
  assert(r.granularity === 'week', `半年超の day は week に (got ${r.granularity})`);
});

check('getOverview', () => {
  const r = q.getOverview();
  assert(r.tiles.length === 4, 'tiles 4枚');
  assert(r.data_to === today, `data_to=${r.data_to}`);
  assert(r.flash_data_to === today, `flash_data_to=${r.flash_data_to}`);
  const tYest = r.tiles.find(t => t.key === 'yesterday');
  assert(tYest.sales_incl === DAY_SALES, `昨日売上(完了) (got ${tYest.sales_incl})`);
  assert(tYest.flash_sales_incl === 20000, `昨日売上(速報) — 楽天行が混ざっていないこと (got ${tYest.flash_sales_incl})`);
  assert(tYest.flash_units === 16 && tYest.flash_orders === 12, '速報 units/orders');
  assert(tYest.units_net === 15, `昨日販売数 (got ${tYest.units_net})`);
  assert(tYest.variable_margin === DAY_L1, `昨日L1 (got ${tYest.variable_margin})`);
  assert(tYest.gift_point === DAY_GIFT, `付与pt (got ${tYest.gift_point})`);
  assert(tYest.l2_margin === DAY_L2, `L2 = L1 − 付与pt (got ${tYest.l2_margin})`);
  assert(tYest.mall_fee_est === 1261 + 1040, `手数料 (NULL は 0 扱い) (got ${tYest.mall_fee_est})`);
  assert(tYest.shipping === DAY_SHIP, `送料 (got ${tYest.shipping})`);
  assert(tYest.shipping_pct === Math.round(DAY_SHIP / DAY_SALES * 1000) / 10, `送料負担率 (got ${tYest.shipping_pct})`);
  assert(tYest.cost_coverage_pct === Math.round(18000 / 18500 * 1000) / 10, `原価カバー率 (got ${tYest.cost_coverage_pct})`);
  // SKU 未解決の可視化 (今月の gamma)
  const tMonth = r.tiles.find(t => t.key === 'this_month');
  assert(r.unresolved.sku_count === 1, `未解決SKU数 (got ${r.unresolved.sku_count})`);
  assert(r.unresolved.sales_incl === 500 * tMonth.days_with_data, `未解決売上 (got ${r.unresolved.sales_incl})`);
  assert(r.unresolved.share_pct !== null && r.unresolved.share_pct > 0, '未解決シェア');
  // 三太郎効果: fixture は全日同額 → 中央値比 1.00
  assert(r.santaro.days_santaro >= 8 && r.santaro.days_santaro <= 9, `三太郎日数 (got ${r.santaro.days_santaro})`);
  assert(r.santaro.lift_ratio === 1, `三太郎倍率 (got ${r.santaro.lift_ratio})`);
});

check('getTrend day + 三太郎フラグ', () => {
  const r = q.getTrend(d(29), today, 'day');
  assert(r.rows.length === 30, `30日分 (got ${r.rows.length})`);
  const row = r.rows[0];
  assert(row.sales_incl === DAY_SALES, `日次売上 (got ${row.sales_incl})`);
  assert(row.l2_margin === DAY_L2, `日次L2 (got ${row.l2_margin})`);
  assert(row.margin_pct !== null && row.l2_pct !== null, 'margin_pct / l2_pct');
  assert(row.shipping_pct === Math.round(DAY_SHIP / DAY_SALES * 1000) / 10, `送料負担率 (got ${row.shipping_pct})`);
  assert(row.avg_unit_price === Math.round(DAY_SALES / 15), `客単価 (got ${row.avg_unit_price})`);
  const flagged = r.rows.filter(x => x.is_santaro);
  const expected = r.rows.filter(x => ['03', '13', '23'].includes(x.bucket.slice(8, 10)));
  assert(flagged.length === expected.length && flagged.length >= 2,
    `三太郎フラグ (got ${flagged.length}, want ${expected.length})`);
});

check('getTrend month (三太郎フラグは立たない)', () => {
  const r = q.getTrend(d(89), today, 'month');
  assert(r.rows.length >= 3, `3ヶ月分 (got ${r.rows.length})`);
  assert(r.rows.every(x => /^\d{4}-\d{2}$/.test(x.bucket)), 'bucket は YYYY-MM');
  assert(r.rows.every(x => x.is_santaro === false), '月次では is_santaro=false');
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
  const r = q.getTrend('2019-01-01', '2019-01-31', 'day');
  assert(r.rows.length === 0, 'rows 空');
  const ov = q.getOverview();  // 今日タイルはデータあり得るので単に throw しないこと
  assert(ov.tiles.length === 4, 'overview は常に4枚');
});

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
