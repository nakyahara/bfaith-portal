// rakuten-analytics 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/rakuten-analytics/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import fs from 'node:fs';
import path from 'node:path';
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';
import * as rl from './rules.js';

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


// ============================================================
// P3: 広告×利益 / 売上方程式 / 診断ルール+アクションキュー (rules.js)
// fixture は master の mirror スキーマ (mall-csv-fetcher 由来) に準拠
// ============================================================

// ─── P3 fixture ───
const ymThis = ymNow;
const p3tx = db.transaction(() => {
  for (const t of ['mirror_rakuten_ads_rpp', 'mirror_rakuten_ads_rpp_daily', 'mirror_rakuten_item_daily',
    'mirror_rakuten_store_daily', 'mirror_rakuten_campaigns',
    'radash_rule_runs', 'radash_observations', 'radash_actions', 'radash_action_events',
    'radash_interventions', 'radash_settings']) {
    try { db.exec(`DELETE FROM ${t}`); } catch {}
  }

  // RPP 月次×SKU (alpha=赤字 roas150<損益分岐202 / beta=増額 roas500>295×1.5 / gamma=突合不能)
  const insAds = db.prepare(`INSERT INTO mirror_rakuten_ads_rpp
    (date_jst, month_ym, item_manage_number, clicks, ad_cost_yen, sales_720h_yen, orders_720h,
     cvr_720h_pct, roas_720h_pct, source_run_id, source_row_hash, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'smoke', 'h', 't')`);
  for (const ym of [ymPrev, ymThis]) {
    insAds.run(`${ym}-01`, ym, 'rk-alpha', 200, 10000, 15000, 5, 2.5, 150, );
    insAds.run(`${ym}-01`, ym, 'rk-beta', 100, 5000, 25000, 10, 10, 500);
    insAds.run(`${ym}-01`, ym, 'rk-gamma', 50, 4000, 2000, 1, 2, 50);
  }

  // RPP 日次 (店舗全体、直近30日 3,000円/日)
  const insAdsD = db.prepare(`INSERT INTO mirror_rakuten_ads_rpp_daily
    (date_jst, campaign_id, campaign_name, clicks, ad_cost_yen, sales_720h_yen, orders_720h,
     roas_720h_pct, source_run_id, source_row_hash, synced_at)
    VALUES (?, '', '通常', 100, 3000, 12000, 4, 400, 'smoke', 'h', 't')`);
  for (let i = 1; i <= 30; i++) insAdsD.run(d(i));

  // 商品分析 SKU×日次 (latest = 昨日):
  //  rk-alpha: 基準28日 CVR5% → 直近7日 CVR0.86% (急落 critical)。在庫は潤沢
  //  rk-beta: CVR安定、在庫5個×売れ1個/日 = 残5日 (在庫×広告 warning)
  const insItem = db.prepare(`INSERT INTO mirror_rakuten_item_daily
    (date_jst, item_manage_number, sales_yen, orders, units, access_users, stock_qty, item_name,
     source_run_id, source_row_hash, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'smoke', 'h', 't')`);
  for (let i = 1; i <= 35; i++) {
    const date = d(i);
    if (i <= 7) insItem.run(date, 'rk-alpha', i <= 3 ? 2000 : 0, i <= 3 ? 1 : 0, i <= 3 ? 1 : 0, 50, 500, 'アルファ精油');
    else insItem.run(date, 'rk-alpha', 4000, 2, 2, 40, 500, 'アルファ精油');
    insItem.run(date, 'rk-beta', 2000, 1, 1, 30, 5, 'ベータ茶葉');
  }

  // 店舗日次 (直近14日: 前週 access1000/orders50/aov2000 → 今週 access1200/orders48/aov2100)
  const insStore = db.prepare(`INSERT INTO mirror_rakuten_store_daily
    (date_jst, sales_all_yen, orders_all, access_all, source_run_id, source_row_hash, synced_at)
    VALUES (?, ?, ?, ?, 'smoke', 'h', 't')`);
  for (let i = 1; i <= 14; i++) {
    const date = d(i);
    if (i <= 7) insStore.run(date, 48 * 2100, 48, 1200);
    else insStore.run(date, 50 * 2000, 50, 1000);
  }

  // 楽天イベント (直近7日にだけマラソンが重なる → 方程式は交絡あり判定)
  db.prepare(`INSERT INTO mirror_rakuten_campaigns
    (campaign_type, campaign_name, start_at, end_at, date_jst, source_run_id, source_row_hash, synced_at)
    VALUES ('マラソン', 'お買い物マラソン', ?, ?, ?, 'smoke', 'h', 't')`)
    .run(d(4) + 'T20:00', d(2) + 'T01:59', d(4));
});
rl.ensureRadashTables();
p3tx();

check('P3 getAdsAnalysis: 損益分岐ROAS判定と突合率', () => {
  const r = q.getAdsAnalysis();
  assert(r.month === ymThis, `最新月 (got ${r.month})`);
  assert(r.months.length === 2, '2ヶ月分');
  const alpha = r.skus.find(x => x.sku === 'rk-alpha');
  assert(alpha.verdict === 'bleed', `alpha=bleed (got ${alpha.verdict}, be=${alpha.breakeven_roas_pct})`);
  assert(alpha.breakeven_roas_pct > 190 && alpha.breakeven_roas_pct < 215, `損益分岐≈202 (got ${alpha.breakeven_roas_pct})`);
  assert(alpha.est_ad_profit < 0, '広告損失');
  const beta = r.skus.find(x => x.sku === 'rk-beta');
  assert(beta.verdict === 'expand', `beta=expand (got ${beta.verdict})`);
  const gamma = r.skus.find(x => x.sku === 'rk-gamma');
  assert(gamma.verdict === 'no_baseline', 'gamma=突合不能');
  assert(r.match.matched_ad_cost_pct > 75 && r.match.matched_ad_cost_pct < 82, `突合率≈79% (got ${r.match.matched_ad_cost_pct})`);
  assert(r.daily.length === 30, `日次30日 (got ${r.daily.length})`);
});

check('P3 タイルに広告費と広告後利益', () => {
  const r = q.getOverview();
  const t = r.tiles.find(x => x.key === 'yesterday');
  assert(t.ad_cost === 3000, `昨日広告費 (got ${t.ad_cost})`);
  assert(t.margin_after_ads === t.variable_margin - 3000, '広告後利益 = 粗利 − 広告費');
});

check('P3 売上方程式 (店舗): 逐次差分の恒等性と交絡検知', () => {
  const eq = q.getEquation({ level: 'store', windowDays: 7 });
  assert(eq.available, 'available');
  const f = eq.factors;
  assert(Math.abs(f.residual) <= 2, `残差≈0 (got ${f.residual})`);
  assert(Math.abs(f.access + f.cvr + f.aov + f.residual - eq.delta) <= 2, '因子合計=Δ売上');
  assert(f.access > 0, 'アクセス因子は正 (1000→1200)');
  assert(f.cvr < 0, 'CVR因子は負 (5%→4%)');
  assert(f.aov > 0, '客単価因子は正 (2000→2100)');
  assert(eq.confounded === true, 'イベント交絡を検知');
});

check('P3 売上方程式 movers: 急落SKUと主因', () => {
  const m = q.getEquationMovers({});
  assert(m.available, 'available');
  const alpha = m.fallers.find(x => x.sku === 'rk-alpha');
  assert(alpha && alpha.delta < 0, 'alphaは急落');
  assert(alpha.main_factor === 'cvr', `主因=CVR (got ${alpha.main_factor})`);
});

check('P3 診断実行: 4ルールがアクションを生成', () => {
  const { run, executed } = rl.ensureDailyRun({ trigger: 'lazy' });
  assert(executed === true && run.status === 'succeeded', `実行成功 (got ${run.status})`);
  const items = rl.listActions({ status: 'open' });
  const byDim = (dim) => items.filter(a => a.dimension === dim);
  assert(byDim('ad_breakeven').some(a => a.entity_id === 'rk-alpha'), '広告赤字 rk-alpha');
  assert(byDim('ad_expand').some(a => a.entity_id === 'rk-beta'), '増額候補 rk-beta');
  const cvr = byDim('cvr_drop').find(a => a.entity_id === 'rk-alpha');
  assert(cvr && cvr.severity === 'critical', `CVR急落 critical (got ${cvr?.severity})`);
  assert(byDim('stock_ads').some(a => a.entity_id === 'rk-beta'), '在庫×広告 rk-beta');
  assert(!items.some(a => a.entity_type === 'dataset'), 'データ鮮度は正常 (fixtureは新しい)');
  const obs = db.prepare(`SELECT COUNT(*) AS c FROM radash_observations`).get().c;
  assert(obs > items.length, 'ok判定もobservationsに残る (履歴>アクション数)');
});

check('P3 再実行 = reseen (重複作成せず occurrence_count++)', () => {
  const before = db.prepare(`SELECT COUNT(*) AS c FROM radash_actions`).get().c;
  rl.ensureDailyRun({ trigger: 'manual', force: true });
  const after = db.prepare(`SELECT COUNT(*) AS c FROM radash_actions`).get().c;
  assert(after === before, `アクション数不変 (${before}→${after})`);
  const a = rl.listActions({ status: 'open' }).find(x => x.dimension === 'ad_breakeven' && x.entity_id === 'rk-alpha');
  assert(a.occurrence_count === 2, `検知2回 (got ${a.occurrence_count})`);
  const ev = db.prepare(`SELECT COUNT(*) AS c FROM radash_action_events WHERE action_id=? AND event='reseen'`).get(a.id).c;
  assert(ev >= 1, 'reseenイベント記録');
});

check('P3 ライフサイクル: ack → done → intervention + 効果測定', () => {
  const a = rl.listActions({ status: 'open' }).find(x => x.dimension === 'cvr_drop');
  rl.actOnAction(a.id, { event: 'ack', actor: 'smoke@test' });
  rl.actOnAction(a.id, { event: 'done', actor: 'smoke@test', note: '価格を見直した' });
  const detail = rl.getActionDetail(a.id);
  assert(detail.status === 'done', 'done');
  assert(detail.intervention && detail.intervention.description === '価格を見直した', 'intervention記録');
  assert(detail.effect !== null, '効果測定が返る');
  assert(/不足|low/.test(detail.effect.confidence), `実施直後は信頼度low (got ${detail.effect.confidence})`);
  assert(detail.events.length >= 3, 'イベント履歴 (created/acked/done)');
});

check('P3 dismiss(今回のみ) = 抑制期間中は再作成しない', () => {
  const a = rl.listActions({ status: 'open' }).find(x => x.dimension === 'stock_ads');
  rl.actOnAction(a.id, { event: 'dismiss', actor: 'smoke@test', note: '補充手配済み', dismissScope: 'occurrence' });
  rl.ensureDailyRun({ trigger: 'manual', force: true });
  const stockActions = db.prepare(`SELECT COUNT(*) AS c FROM radash_actions WHERE dimension='stock_ads'`).get().c;
  assert(stockActions === 1, `再作成されない (got ${stockActions})`);
  const dismissed = db.prepare(`SELECT * FROM radash_actions WHERE dimension='stock_ads'`).get();
  assert(dismissed.status === 'dismissed' && dismissed.suppress_until >= today, 'suppress_until設定');
});

check('P3 done後の再検知 = 新規アクションとして再発登録', () => {
  rl.ensureDailyRun({ trigger: 'manual', force: true });
  const cvrActions = db.prepare(`SELECT COUNT(*) AS c FROM radash_actions WHERE dimension='cvr_drop' AND entity_id='rk-alpha'`).get().c;
  assert(cvrActions === 2, `done後の再発は新規 (got ${cvrActions})`);
  const latest = db.prepare(`SELECT * FROM radash_actions WHERE dimension='cvr_drop' AND entity_id='rk-alpha' ORDER BY id DESC LIMIT 1`).get();
  assert(latest.status === 'open', '新規はopen');
});

check('P3 閾値設定の保存と反映', () => {
  const s = rl.saveThresholds({ stock_days_threshold: 3, bogus: 99 });
  assert(s.stock_days_threshold === 3, '保存');
  assert(!('bogus' in s), '未知キーは無視');
  assert(rl.getThresholds().stock_days_threshold === 3, '永続化');
});

check('P3 データ鮮度: 6データセット + 停滞検知', () => {
  const ds = rl.getDatasetFreshness();
  assert(ds.length === 6, `6件 (got ${ds.length})`);
  assert(ds.every(x => x.stale === false), '全て新鮮 (fixture)');
  // finance を古くして停滞検知
  db.exec(`DELETE FROM mirror_rakuten_finance_sku_daily WHERE date_jst > '${d(10)}'`);
  const ds2 = rl.getDatasetFreshness();
  assert(ds2.find(x => x.key === 'finance').stale === true, 'finance停滞を検知');
});


check('P3 解消sweep: 条件が直った open アクションは resolved に', () => {
  // rk-alpha の直近7日 CVR を回復させる (orders 0-1 → 2/日 = 基準比0.8 > warn閾値0.6)
  db.prepare(`UPDATE mirror_rakuten_item_daily SET orders=2, units=2, sales_yen=4000
    WHERE item_manage_number='rk-alpha' AND date_jst >= ?`).run(d(7));
  rl.ensureDailyRun({ trigger: 'manual', force: true });
  const latest = db.prepare(`SELECT * FROM radash_actions WHERE dimension='cvr_drop' AND entity_id='rk-alpha' ORDER BY id DESC LIMIT 1`).get();
  assert(latest.status === 'resolved', `自動解消 (got ${latest.status})`);
  const ev = db.prepare(`SELECT COUNT(*) AS c FROM radash_action_events WHERE action_id=? AND event='resolved'`).get(latest.id).c;
  assert(ev === 1, 'resolvedイベント記録');
});

check('P3 sweep skip: 依存データ断のとき既存アクションを誤解消しない', () => {
  const bleedBefore = db.prepare(`SELECT * FROM radash_actions WHERE dimension='ad_breakeven' AND entity_id='rk-alpha' ORDER BY id DESC LIMIT 1`).get();
  assert(bleedBefore.status === 'open' || bleedBefore.status === 'acked', `前提: bleedはactive (got ${bleedBefore.status})`);
  db.exec(`DELETE FROM mirror_rakuten_ads_rpp`);   // 広告データ断 (rows=0 → stale)
  rl.ensureDailyRun({ trigger: 'manual', force: true });
  const bleedAfter = db.prepare(`SELECT * FROM radash_actions WHERE id=?`).get(bleedBefore.id);
  assert(bleedAfter.status === bleedBefore.status, `データ断では解消しない (got ${bleedAfter.status})`);
  const run = db.prepare(`SELECT * FROM radash_rule_runs ORDER BY id DESC LIMIT 1`).get();
  const stats = JSON.parse(run.stats_json);
  assert(stats.sweep_skipped.includes('ad_breakeven'), `sweep_skipped記録 (got ${JSON.stringify(stats.sweep_skipped)})`);
});

check('P3 reopen: 同一案件の active があれば重複を拒否', () => {
  // cvr_drop: 1件目=done、2件目=resolved の状態。resolved を reopen → active になる
  const rows = db.prepare(`SELECT * FROM radash_actions WHERE dimension='cvr_drop' AND entity_id='rk-alpha' ORDER BY id`).all();
  assert(rows.length === 2, `前提2件 (got ${rows.length})`);
  rl.actOnAction(rows[1].id, { event: 'reopen', actor: 'smoke@test' });
  // done の1件目を reopen しようとすると active 重複で拒否
  let threw = false;
  try { rl.actOnAction(rows[0].id, { event: 'reopen', actor: 'smoke@test' }); }
  catch (e) { threw = /対応待ち/.test(e.message); }
  assert(threw, 'active重複でreopen拒否');
});

check('P3 movers: 今期間に行が無い (売上ゼロに落ちた) SKU も急落に出る', () => {
  const ins = db.prepare(`INSERT INTO mirror_rakuten_item_daily
    (date_jst, item_manage_number, sales_yen, orders, units, access_users, stock_qty, item_name,
     source_run_id, source_row_hash, synced_at)
    VALUES (?, 'rk-omega', 10000, 5, 5, 100, 50, 'オメガ石鹸', 'smoke', 'h', 't')`);
  for (let i = 8; i <= 14; i++) ins.run(d(i));   // 前7日のみ売上、直近7日は行なし
  const m = q.getEquationMovers({ limit: 50 });
  const omega = m.fallers.find(x => x.sku === 'rk-omega');
  assert(omega, '消えたSKUが急落に出る');
  assert(omega.sales_cur === 0 && omega.delta === -70000, `Δ=-70000 (got ${omega?.delta})`);
});

check('P3 閾値バリデーション: 範囲外と矛盾値は拒否', () => {
  const r1 = rl.saveThresholds({ stock_days_threshold: -5 });
  assert(r1.rejected.length === 1 && r1.stock_days_threshold === 3, '負値拒否 (前回値3のまま)');
  const r2 = rl.saveThresholds({ cvr_drop_crit_ratio: 0.8, cvr_drop_warn_ratio: 0.6 });
  assert(r2.rejected.some(x => /crit/.test(x)), 'crit>=warn の矛盾を拒否');
  const r3 = rl.saveThresholds({ stock_days_threshold: 12 });
  assert(r3.stock_days_threshold === 12 && r3.rejected.length === 0, '正常値は保存');
});


check('P3 vm<=0 = 広告赤字critical (画面/ルール整合) + preserveは両dimensionを守る', () => {
  // 前提整備: finance を新鮮に戻し (sweep が動く状態)、広告データを再投入
  db.prepare(`INSERT INTO mirror_rakuten_finance_sku_daily (
    date_jst, rakuten_code, ne_code, sku_resolution, product_name,
    units_ordered, units_cancelled, units_net_sold, allocated_units_cancelled,
    sales_principal_jpy_incl, sales_postage_jpy_incl, coupon_shop_jpy_incl, coupon_all_jpy_incl,
    promotion_jpy_incl, refund_amount_jpy_incl, allocated_refund_amount_jpy_incl,
    mall_fee_jpy_incl, shipping_cost_jpy_incl, shipping_quality, unit_cost_snapshot_incl,
    cogs_amount_jpy_incl, gross_sales_jpy_incl, net_sales_jpy_incl, variable_margin_jpy_incl,
    refund_adjusted_net_sales_jpy_incl, cost_status, is_cost_complete, data_quality_score,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'rk-alpha', 'NE-A', 'resolved', 'アルファ精油', 1, 0, 1, 0, 1000, 0, 0, 0, 0, 0, 0,
    100, 100, 'actual', 300, 300, 1000, 1000, 500, 1000, 'complete', 1, 100, 'smoke', 'h', 't')`).run(today);
  // rk-eps: 変動利益マイナスのSKUに広告出稿 / rk-beta: 広告のみ再投入 (今月のfactなし = 突合不能)
  const insAds2 = db.prepare(`INSERT INTO mirror_rakuten_ads_rpp
    (date_jst, month_ym, item_manage_number, clicks, ad_cost_yen, sales_720h_yen, orders_720h,
     cvr_720h_pct, roas_720h_pct, source_run_id, source_row_hash, synced_at)
    VALUES (?, ?, ?, 100, 5000, ?, 3, 3, ?, 'smoke', 'h', 't')`);
  insAds2.run(`${ymNow}-01`, ymNow, 'rk-eps', 8000, 160);
  insAds2.run(`${ymNow}-01`, ymNow, 'rk-beta', 25000, 500);
  const insFinEps = db.prepare(`INSERT INTO mirror_rakuten_finance_sku_daily (
    date_jst, rakuten_code, ne_code, sku_resolution, product_name,
    units_ordered, units_cancelled, units_net_sold, allocated_units_cancelled,
    sales_principal_jpy_incl, sales_postage_jpy_incl, coupon_shop_jpy_incl, coupon_all_jpy_incl,
    promotion_jpy_incl, refund_amount_jpy_incl, allocated_refund_amount_jpy_incl,
    mall_fee_jpy_incl, shipping_cost_jpy_incl, shipping_quality, unit_cost_snapshot_incl,
    cogs_amount_jpy_incl, gross_sales_jpy_incl, net_sales_jpy_incl, variable_margin_jpy_incl,
    refund_adjusted_net_sales_jpy_incl, cost_status, is_cost_complete, data_quality_score,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'rk-eps', 'NE-E', 'resolved', 'イプシロン', 2, 0, 2, 0, 3000, 0, 0, 0, 0, 0, 0,
    300, 800, 'actual', 1100, 2200, 3000, 3000, -300, 3000, 'complete', 1, 100, 'smoke', 'h', 't')`);
  insFinEps.run(`${ymNow}-01`);

  // beta は「fact突合不能」ケースにする (今月のfinance行を明示削除 — 実行日に依存させない)
  db.prepare(`DELETE FROM mirror_rakuten_finance_sku_daily WHERE rakuten_code='rk-beta' AND substr(date_jst,1,7)=?`).run(ymNow);

  // beta の expand アクションが active であることを前提確認
  const betaExpand = db.prepare(`SELECT * FROM radash_actions WHERE dimension='ad_expand' AND entity_id='rk-beta' ORDER BY id DESC LIMIT 1`).get();
  assert(betaExpand && (betaExpand.status === 'open' || betaExpand.status === 'acked'), `前提: beta expandはactive (got ${betaExpand?.status})`);

  rl.ensureDailyRun({ trigger: 'manual', force: true });

  // ① vm<=0 → bleed critical (rule)
  const eps = db.prepare(`SELECT * FROM radash_actions WHERE dimension='ad_breakeven' AND entity_id='rk-eps' ORDER BY id DESC LIMIT 1`).get();
  assert(eps && eps.severity === 'critical' && /変動利益ゼロ以下/.test(eps.title), `eps=critical (got ${eps?.severity} ${eps?.title})`);
  // ② beta は今月factなし = no_baseline → expand は preserve され resolve されない
  const betaAfter = db.prepare(`SELECT * FROM radash_actions WHERE id=?`).get(betaExpand.id);
  assert(betaAfter.status === betaExpand.status, `no_baselineでexpandを誤解消しない (got ${betaAfter.status})`);
  // ③ 画面 (getAdsAnalysis) も整合: eps=bleed_hard, beta=no_baseline
  const ads = q.getAdsAnalysis(ymNow);
  const epsRow = ads.skus.find(x => x.sku === 'rk-eps');
  assert(epsRow.verdict === 'bleed_hard' && epsRow.est_ad_profit === -5000, `画面eps=bleed_hard/損失-5000 (got ${epsRow.verdict}/${epsRow.est_ad_profit})`);
  assert(ads.skus.find(x => x.sku === 'rk-beta').verdict === 'no_baseline', '画面beta=no_baseline');
});

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
