/**
 * qoo10-analytics queries.js — データ層 (Render warehouse-mirror.db 読み取り)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/Qoo10統合管理ダッシュボード_要件定義_20260706.md
 *
 * 使用テーブル (P1):
 *   mirror_qoo10_finance_sku_daily  確定fact (配送完了 Delivered(5) のみ計上、税込、日次×SKU。
 *                                   手数料は API 実額 (platform_fee = GMV − SettlePrice)、
 *                                   送料は m_products 想定値、原価 snapshot。
 *                                   ⚠️ 配送完了までのラグで直近数日は必ず小さく出る → 速報と併記)
 *   mirror_f_sales_by_listing       速報売上 (NE受注ベース、税込、mall='qoo10'。キャンセル除外、
 *                                   biz-ops-overview と同じ層。毎朝 daily-sync で同期)
 *
 * 精度ラベル方針 (要件 §11-2、税はすべて税込):
 *   速報       = NE受注ベース (mirror_f_sales_by_listing)
 *   自動(日次) = 確定fact (配送完了ベース、手数料実額)
 *   推定       = メガ割/メガポ等のセラー負担 (プラットフォーム割引全体 × 50%)
 *
 * メガ割セラー負担の扱い (要件 §2.5):
 *   SettlePrice は割引前価格基準のため、メガ割 20% 割引のセラー負担分 (10%) は fact の
 *   variable_margin (L1) に入っていない (精算時に「販売関連差引額」で先行差引される)。
 *   → L1 と併せて「セラー負担(推定) = メガ割割引×負担率 + メガポ割引×負担率」と
 *     「L2参考 = L1 − セラー負担推定」を常時表示する。精算Excel取込 (P4) で実額に置換予定。
 *   負担率は設定タブで変更可 (既定 50%。公式仕様「20%割引のうちセラー10%+Qoo10 10%」= 割引全体の50%。
 *   メガポの負担率は要実機確認 — 精算内訳取込 (P4) で較正するまでの仮置き)。
 *   other_promo (由来不明のプラットフォーム割引) は負担推定に含めない (根拠が無い割引に
 *   負担率を掛けると過大控除になる — 実額確定は P4 の精算突合で行う)。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

// ─── アプリ専用テーブル (Render-local。qooda_ prefix、amzdash_/yadash_ と同パターン) ───
let _initialized = false;
export function ensureAppTables() {
  if (_initialized) return;
  const db = getMirrorDB();
  db.exec(`CREATE TABLE IF NOT EXISTS qooda_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);
  // メガ割/メガポ 開催回マスタ (要件 F-5/F-9。過去分はfact逆推定候補から人が採用して登録)
  db.exec(`CREATE TABLE IF NOT EXISTS qooda_mega_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL CHECK (kind IN ('megawari', 'megapo')),
    label        TEXT NOT NULL DEFAULT '',
    date_from    TEXT NOT NULL CHECK (date_from GLOB '????-??-??'),
    date_to      TEXT NOT NULL CHECK (date_to GLOB '????-??-??'),
    participated INTEGER NOT NULL DEFAULT 1 CHECK (participated IN (0, 1)),
    memo         TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    deleted_at   TEXT
  )`);
  _initialized = true;
}

// バリデーションエラー (router 側で 400 + メッセージ表示にする)
export function validationError(msg) {
  const e = new Error(msg);
  e.status = 400;
  return e;
}

// ─── 設定 (設定タブで変更可) ───
export const DEFAULT_SETTINGS = {
  burden_megawari_pct: 50,  // メガ割セラー負担率 (割引全体に対する%)
  burden_megapo_pct: 50,    // メガポセラー負担率 (要実機確認、P4精算突合で較正)
  abc_a_pct: 80,            // ABC: A = 売上累積 N% まで
  abc_b_pct: 95,
  new_product_days: 60,     // 新商品バッジ: 初売上から N 日
  movers_min_units: 5,      // 急上昇/急落: 当期 or 前期 N 個以上のみ (ノイズ除去)
};
// キーごとの許容範囲 (範囲外値で負担推定・ランキング分類が壊れるのを防ぐ — yahoo P3 と同思想)
const SETTING_RANGES = {
  burden_megawari_pct: [0, 100],
  burden_megapo_pct: [0, 100],
  abc_a_pct: [1, 99],
  abc_b_pct: [2, 99.9],
  new_product_days: [1, 365],
  movers_min_units: [0, 1000],
};
// 文字列設定 (数値と別扱い): QOO10_CERT_KEY の発行日 (年1回再発行 — 期限監視用)
const TEXT_SETTING_KEYS = ['cert_key_issued_on'];

export function getSettings() {
  ensureAppTables();
  const db = getMirrorDB();
  const rows = db.prepare(`SELECT key, value FROM qooda_settings`).all();
  const out = { ...DEFAULT_SETTINGS, cert_key_issued_on: '' };
  for (const r of rows) {
    if (r.key in DEFAULT_SETTINGS) {
      const n = Number(r.value);
      if (Number.isFinite(n)) out[r.key] = n;
    } else if (TEXT_SETTING_KEYS.includes(r.key)) {
      out[r.key] = r.value;
    }
  }
  // APIキー期限の派生値 (発行日+365日)
  if (out.cert_key_issued_on && isValidDate(out.cert_key_issued_on)) {
    out.cert_key_expires_on = addDays(out.cert_key_issued_on, 365);
    out.cert_key_days_left = Math.round(
      (new Date(out.cert_key_expires_on + 'T00:00:00Z') - new Date(jstToday() + 'T00:00:00Z')) / 86400000);
  } else {
    out.cert_key_expires_on = null;
    out.cert_key_days_left = null;
  }
  return out;
}

export function saveSettings(patch) {
  ensureAppTables();
  const db = getMirrorDB();
  const now = new Date().toISOString();
  // 先に patch 全体を検証してから書く (部分適用しない)
  const validated = {};
  for (const [k, v] of Object.entries(patch)) {
    if (k in DEFAULT_SETTINGS) {
      const n = Number(v);
      if (!Number.isFinite(n)) throw validationError(`${k} は数値で指定してください`);
      const [min, max] = SETTING_RANGES[k];
      if (n < min || n > max) throw validationError(`${k} は ${min}〜${max} の範囲で指定してください`);
      validated[k] = String(n);
    } else if (TEXT_SETTING_KEYS.includes(k)) {
      const s = String(v || '').trim();
      if (s !== '' && !isValidDate(s)) throw validationError(`${k} は YYYY-MM-DD で指定してください`);
      validated[k] = s;
    }
  }
  const mergedNums = { ...getSettings() };
  for (const [k, v] of Object.entries(validated)) if (k in DEFAULT_SETTINGS) mergedNums[k] = Number(v);
  if (mergedNums.abc_a_pct >= mergedNums.abc_b_pct) throw validationError('abc_a_pct は abc_b_pct より小さくしてください');
  const stmt = db.prepare(`
    INSERT INTO qooda_settings (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
  `);
  const tx = db.transaction(() => {
    for (const [k, v] of Object.entries(validated)) stmt.run(k, v, now);
  });
  tx();
  return getSettings();
}

// セラー負担推定 (メガ割×負担率 + メガポ×負担率。other_promo は含めない — ファイル冒頭コメント参照)
function burdenOf(megawariDiscount, megapoDiscount, settings) {
  return megawariDiscount * settings.burden_megawari_pct / 100
    + megapoDiscount * settings.burden_megapo_pct / 100;
}

// ─── 日付 helper (JST。Date.toISOString の UTC 罠に注意: feedback_jst_to_iso_string_trap) ───
export function jstToday() {
  return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
export function addDays(dateStr, n) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
export function monthOf(dateStr) { return dateStr.slice(0, 7); }
export function monthStart(ym) { return `${ym}-01`; }
export function monthEnd(ym) {
  const [y, m] = ym.split('-').map(Number);
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return `${ym}-${String(lastDay).padStart(2, '0')}`;
}
export function addMonths(ym, n) {
  const [y, m] = ym.split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export function isValidDate(s) {
  if (typeof s !== 'string' || !DATE_RE.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// カスタム期間の上限 (日)。巨大 range での全表スキャン抑止
const MAX_CUSTOM_RANGE_DAYS = 730;

// preset → {from, to} (JST)
export function resolvePeriod(preset, fromQ, toQ) {
  const today = jstToday();
  if (isValidDate(fromQ) && isValidDate(toQ) && fromQ <= toQ) {
    // 両端含みで MAX_CUSTOM_RANGE_DAYS 日に clamp
    const minFrom = addDays(toQ, -(MAX_CUSTOM_RANGE_DAYS - 1));
    return { from: minFrom > fromQ ? minFrom : fromQ, to: toQ, preset: 'custom' };
  }
  const ym = monthOf(today);
  const map = {
    today: { from: today, to: today },
    yesterday: { from: addDays(today, -1), to: addDays(today, -1) },
    '7d': { from: addDays(today, -6), to: today },
    '14d': { from: addDays(today, -13), to: today },
    '30d': { from: addDays(today, -29), to: today },
    '90d': { from: addDays(today, -89), to: today },
    this_month: { from: monthStart(ym), to: today },
    last_month: { from: monthStart(addMonths(ym, -1)), to: monthEnd(addMonths(ym, -1)) },
    this_year: { from: `${today.slice(0, 4)}-01-01`, to: today },
    '12m': { from: monthStart(addMonths(ym, -11)), to: today },
  };
  const r = map[preset] || map['30d'];
  return { ...r, preset: map[preset] ? preset : '30d' };
}

// ─── 確定fact の期間集計 (配送完了ベース、税込) ───
function factSummary(db, from, to) {
  return db.prepare(`
    SELECT
      COALESCE(SUM(gmv_list_price_jpy_incl),0)       AS gmv,
      COALESCE(SUM(customer_paid_jpy_incl),0)        AS customer_paid,
      COALESCE(SUM(net_settlement_api_jpy_incl),0)   AS net_settlement,
      COALESCE(SUM(platform_fee_jpy_incl),0)         AS platform_fee,
      COALESCE(SUM(shipping_cost_jpy_incl),0)        AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)          AS cogs,
      COALESCE(SUM(variable_margin_jpy_incl),0)      AS variable_margin,
      COALESCE(SUM(units_ordered),0)                 AS units_ordered,
      COALESCE(SUM(units_net_sold),0)                AS units_net,
      COALESCE(SUM(order_count),0)                   AS orders,
      COALESCE(SUM(megawari_discount_amount_jpy_incl),0) AS megawari_discount,
      COALESCE(SUM(megawari_order_count),0)          AS megawari_orders,
      COALESCE(SUM(megapo_discount_amount_jpy_incl),0)   AS megapo_discount,
      COALESCE(SUM(other_promo_discount_jpy_incl),0) AS other_promo_discount,
      COALESCE(SUM(total_platform_promo_jpy_incl),0) AS promo_total,
      COALESCE(SUM(CASE WHEN is_cost_complete = 1 THEN gmv_list_price_jpy_incl END),0) AS gmv_cost_complete,
      COALESCE(SUM(CASE WHEN match_tier = 'unresolved' THEN gmv_list_price_jpy_incl END),0) AS gmv_unresolved,
      COUNT(DISTINCT date_jst)                       AS days_with_data
    FROM mirror_qoo10_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 速報売上 (NE受注ベース、mall='qoo10'、税込) ───
function flashSummary(db, from, to) {
  return db.prepare(`
    SELECT COALESCE(SUM(sales_jpy_incl),0) AS sales_incl,
           COALESCE(SUM(units),0)          AS units,
           COALESCE(SUM(order_count),0)    AS orders
    FROM mirror_f_sales_by_listing
    WHERE mall = 'qoo10' AND date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// fact 集計行 → API 向けタイル/バケット値 (丸め + 派生指標)
function shapeFact(s, settings) {
  const burdenEst = burdenOf(s.megawari_discount, s.megapo_discount, settings);
  return {
    gmv: Math.round(s.gmv),
    customer_paid: Math.round(s.customer_paid),
    net_settlement: Math.round(s.net_settlement),
    platform_fee: Math.round(s.platform_fee),
    // 手数料率(実測) = platform_fee ÷ GMV (要件 §10。9%/10% SKU 混在の実測値)
    fee_pct: s.gmv > 0 ? Math.round(s.platform_fee / s.gmv * 1000) / 10 : null,
    shipping: Math.round(s.shipping),
    cogs: Math.round(s.cogs),
    variable_margin: Math.round(s.variable_margin),
    // 粗利率 L1 = variable_margin ÷ ショップ受取 (要件 §10 の正本定義)
    margin_pct: s.net_settlement > 0 ? Math.round(s.variable_margin / s.net_settlement * 1000) / 10 : null,
    units_net: s.units_net,
    orders: s.orders,
    megawari_discount: Math.round(s.megawari_discount),
    megawari_orders: s.megawari_orders,
    megapo_discount: Math.round(s.megapo_discount),
    other_promo_discount: Math.round(s.other_promo_discount),
    promo_total: Math.round(s.promo_total),
    burden_est: Math.round(burdenEst),
    // L2参考 = L1 − セラー負担推定 (広告費は P5 取込後に加わる)
    l2_ref: Math.round(s.variable_margin - burdenEst),
    cost_coverage_pct: s.gmv > 0 ? Math.round(s.gmv_cost_complete / s.gmv * 1000) / 10 : null,
    days_with_data: s.days_with_data,
  };
}

// ─── 概要タブ: タイル 4 枚 (今日/昨日/今月/先月) ───
export function getOverview() {
  const db = getMirrorDB();
  const settings = getSettings();
  const today = jstToday();
  const ym = monthOf(today);
  const lastYm = addMonths(ym, -1);
  const periods = [
    { key: 'today', label: '今日', from: today, to: today },
    { key: 'yesterday', label: '昨日', from: addDays(today, -1), to: addDays(today, -1) },
    { key: 'this_month', label: '今月', from: monthStart(ym), to: today },
    { key: 'last_month', label: '先月', from: monthStart(lastYm), to: monthEnd(lastYm) },
  ];

  // データ鮮度 (fact = 配送完了ベースで数日ラグ / flash = NE受注ベースで前日まで)
  const factFresh = db.prepare(`
    SELECT MAX(date_jst) AS data_to, MAX(synced_at) AS last_synced
    FROM mirror_qoo10_finance_sku_daily
  `).get();
  const flashFresh = db.prepare(`
    SELECT MAX(date_jst) AS data_to FROM mirror_f_sales_by_listing WHERE mall = 'qoo10'
  `).get();

  const tiles = periods.map(p => {
    const fact = shapeFact(factSummary(db, p.from, p.to), settings);
    const flash = flashSummary(db, p.from, p.to);
    const flashSales = Math.round(flash.sales_incl);
    return {
      key: p.key, label: p.label, from: p.from, to: p.to,
      flash: { sales_incl: flashSales, units: flash.units, orders: flash.orders },
      fact,
      // 確定待ち目安 = 速報売上 − 確定済み顧客支払額。基準が違う (NE受注 vs 配送完了×割引後)
      // ため厳密な差ではなく「まだ配送完了していない分のめやす」。負なら 0 に clamp。
      pending_est: Math.max(0, flashSales - fact.customer_paid),
    };
  });

  // SKU 未解決の可視化 (今月)。3段fallback 後は通常 ~0% (要件 付録A)。0 でも件数を返す
  const un = db.prepare(`
    SELECT COUNT(DISTINCT sku_code) AS sku_count,
           COALESCE(SUM(gmv_list_price_jpy_incl),0) AS gmv
    FROM mirror_qoo10_finance_sku_daily
    WHERE match_tier = 'unresolved' AND date_jst >= ? AND date_jst <= ?
  `).get(monthStart(ym), today);
  const monthTile = tiles.find(t => t.key === 'this_month');
  const unresolved = {
    sku_count: un.sku_count,
    gmv: Math.round(un.gmv),
    share_pct: monthTile && monthTile.fact.gmv > 0
      ? Math.round(un.gmv / monthTile.fact.gmv * 1000) / 10 : null,
  };

  return {
    generated_at: new Date().toISOString(),
    today,
    fact_data_to: factFresh.data_to,
    flash_data_to: flashFresh.data_to,
    last_synced: factFresh.last_synced,
    burden_rates: { megawari_pct: settings.burden_megawari_pct, megapo_pct: settings.burden_megapo_pct },
    tiles,
    unresolved,
  };
}

// ─── トレンド (日次/週次/月次)。fact + flash を同一バケットで返す ───
export function getTrend(from, to, granularity) {
  const db = getMirrorDB();
  const settings = getSettings();
  // 半年を超える day 指定は week に自動格上げ (点が潰れて読めない + 行数抑制)
  const spanDays = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  if (granularity === 'day' && spanDays > 183) granularity = 'week';

  // week バケットは「その週の月曜の日付」(-6 days して次の月曜へ進める SQLite idiom)
  const bucketExpr = granularity === 'month' ? `substr(date_jst, 1, 7)`
    : granularity === 'week' ? `date(date_jst, '-6 days', 'weekday 1')`
    : `date_jst`;

  const factRows = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
      SUM(gmv_list_price_jpy_incl)       AS gmv,
      SUM(customer_paid_jpy_incl)        AS customer_paid,
      SUM(net_settlement_api_jpy_incl)   AS net_settlement,
      SUM(platform_fee_jpy_incl)         AS platform_fee,
      SUM(variable_margin_jpy_incl)      AS variable_margin,
      SUM(units_net_sold)                AS units_net,
      SUM(megawari_discount_amount_jpy_incl) AS megawari_discount,
      SUM(megawari_order_count)          AS megawari_orders,
      SUM(megapo_discount_amount_jpy_incl)   AS megapo_discount,
      SUM(other_promo_discount_jpy_incl) AS other_promo_discount,
      SUM(total_platform_promo_jpy_incl) AS promo_total
    FROM mirror_qoo10_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to);

  const flashRows = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
      SUM(sales_jpy_incl) AS sales_incl,
      SUM(units)          AS units
    FROM mirror_f_sales_by_listing
    WHERE mall = 'qoo10' AND date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to);

  // bucket 合流 (fact/flash どちらか一方にしかない bucket も落とさない)
  const byBucket = new Map();
  for (const r of factRows) byBucket.set(r.bucket, { bucket: r.bucket, fact: r, flash: null });
  for (const r of flashRows) {
    if (byBucket.has(r.bucket)) byBucket.get(r.bucket).flash = r;
    else byBucket.set(r.bucket, { bucket: r.bucket, fact: null, flash: r });
  }
  const rows = [...byBucket.values()].sort((a, b) => a.bucket < b.bucket ? -1 : 1).map(({ bucket, fact, flash }) => {
    const gmv = fact ? fact.gmv : 0;
    const vm = fact ? fact.variable_margin : 0;
    const settlement = fact ? fact.net_settlement : 0;
    return {
      bucket,
      gmv: Math.round(gmv),
      flash_sales: flash ? Math.round(flash.sales_incl) : 0,
      flash_units: flash ? flash.units : 0,
      variable_margin: Math.round(vm),
      margin_pct: settlement > 0 ? Math.round(vm / settlement * 1000) / 10 : null,
      units_net: fact ? fact.units_net : 0,
      avg_unit_price: fact && fact.units_net > 0 ? Math.round(fact.customer_paid / fact.units_net) : null,
      platform_fee: fact ? Math.round(fact.platform_fee) : 0,
      megawari_discount: fact ? Math.round(fact.megawari_discount) : 0,
      megapo_discount: fact ? Math.round(fact.megapo_discount) : 0,
      other_promo_discount: fact ? Math.round(fact.other_promo_discount) : 0,
      burden_est: fact ? Math.round(burdenOf(fact.megawari_discount, fact.megapo_discount, settings)) : 0,
      // メガ割開催バケット判定 (fact 実データ由来。開催回マスタは P3 で導入予定)
      is_megawari: !!(fact && fact.megawari_orders > 0),
    };
  });

  return {
    from, to, granularity, rows,
    burden_rates: { megawari_pct: settings.burden_megawari_pct, megapo_pct: settings.burden_megapo_pct },
  };
}

// ─── 利益分析タブ: ウォーターフォール (F-3) ───
// GMV → 手数料実額 → 受取 → 送料/原価 → L1 → セラー負担推定 → L2参考。
// 広告費 (P5) / 精算実額 L3 (P4) は取込実装後に行が増える設計。
export function getWaterfall(from, to, sku) {
  const db = getMirrorDB();
  const settings = getSettings();
  const skuCond = sku ? `AND sku_code = ?` : '';
  const params = sku ? [from, to, sku] : [from, to];
  const s = db.prepare(`
    SELECT
      COALESCE(SUM(gmv_list_price_jpy_incl),0)       AS gmv,
      COALESCE(SUM(platform_fee_jpy_incl),0)         AS platform_fee,
      COALESCE(SUM(net_settlement_api_jpy_incl),0)   AS net_settlement,
      COALESCE(SUM(shipping_cost_jpy_incl),0)        AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)          AS cogs,
      COALESCE(SUM(variable_margin_jpy_incl),0)      AS variable_margin,
      COALESCE(SUM(megawari_discount_amount_jpy_incl),0) AS megawari_discount,
      COALESCE(SUM(megapo_discount_amount_jpy_incl),0)   AS megapo_discount
    FROM mirror_qoo10_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ? ${skuCond}
  `).get(...params);

  const burdenMegawari = s.megawari_discount * settings.burden_megawari_pct / 100;
  const burdenMegapo = s.megapo_discount * settings.burden_megapo_pct / 100;
  const steps = [
    { key: 'gmv', label: '売上 GMV (割引前・税込)', amount: s.gmv, kind: 'total', precision: 'fact' },
    { key: 'platform_fee', label: '手数料 (API実額)', amount: s.platform_fee, kind: 'cost', precision: 'fact' },
    { key: 'net_settlement', label: 'ショップ受取 (SettlePrice)', amount: s.net_settlement, kind: 'subtotal', precision: 'fact' },
    { key: 'shipping', label: '送料 (想定)', amount: s.shipping, kind: 'cost', precision: 'est' },
    { key: 'cogs', label: '原価 (snapshot)', amount: s.cogs, kind: 'cost', precision: 'fact' },
    { key: 'variable_margin', label: '粗利 L1', amount: s.variable_margin, kind: 'subtotal', precision: 'fact' },
    { key: 'burden_megawari', label: `メガ割セラー負担 (推定${settings.burden_megawari_pct}%)`, amount: burdenMegawari, kind: 'cost', precision: 'est' },
    { key: 'burden_megapo', label: `メガポセラー負担 (推定${settings.burden_megapo_pct}%)`, amount: burdenMegapo, kind: 'cost', precision: 'est' },
    { key: 'l2', label: 'L2参考 (広告費は未控除)', amount: s.variable_margin - burdenMegawari - burdenMegapo, kind: 'total', precision: 'est' },
  ];
  return {
    from, to, sku: sku || null,
    steps: steps.map(x => ({ ...x, amount: Math.round(x.amount) })),
  };
}

// ─── 利益分析タブ: SKU 別利益テーブル (F-4) ───
// 最新日の ne_code / match_tier を取るための連結ソートキー (ne_code に '|' は現れない前提)
const LATEST_EXPR = `MAX(date_jst || '|' || COALESCE(ne_code,'') || '|' || match_tier)`;

function skuAggSql() {
  return `
    SELECT
      sku_code,
      MAX(product_name)                                  AS product_name,
      ${LATEST_EXPR}                                     AS latest_key,
      MAX(CASE WHEN match_tier = 'unresolved' THEN 1 ELSE 0 END) AS ever_unresolved,
      SUM(units_net_sold)                                AS units_net,
      SUM(gmv_list_price_jpy_incl)                       AS gmv,
      SUM(customer_paid_jpy_incl)                        AS customer_paid,
      SUM(net_settlement_api_jpy_incl)                   AS net_settlement,
      SUM(platform_fee_jpy_incl)                         AS platform_fee,
      SUM(shipping_cost_jpy_incl)                        AS shipping,
      SUM(cogs_amount_jpy_incl)                          AS cogs,
      SUM(variable_margin_jpy_incl)                      AS variable_margin,
      SUM(megawari_discount_amount_jpy_incl)             AS megawari_discount,
      SUM(megapo_discount_amount_jpy_incl)               AS megapo_discount,
      SUM(CASE WHEN megawari_order_count > 0 OR megapo_order_count > 0
               THEN gmv_list_price_jpy_incl ELSE 0 END)  AS mega_day_gmv,
      MIN(is_cost_complete)                              AS all_cost_complete,
      MAX(CASE WHEN cost_status <> 'complete' THEN cost_status END) AS cost_status_sample
    FROM mirror_qoo10_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY sku_code`;
}

function shapeSkuRow(r, settings) {
  const parts = (r.latest_key || '||').split('|');
  const burdenEst = burdenOf(r.megawari_discount, r.megapo_discount, settings);
  const l2 = r.variable_margin - burdenEst;
  return {
    sku_code: r.sku_code,
    ne_code: parts[1] || null,
    product_name: r.product_name || '',
    match_tier: parts[2] || 'unresolved',
    units_net: r.units_net,
    gmv: Math.round(r.gmv),
    platform_fee: Math.round(r.platform_fee),
    fee_pct: r.gmv > 0 ? Math.round(r.platform_fee / r.gmv * 1000) / 10 : null,
    net_settlement: Math.round(r.net_settlement),
    shipping: Math.round(r.shipping),
    cogs: Math.round(r.cogs),
    variable_margin: Math.round(r.variable_margin),
    megawari_discount: Math.round(r.megawari_discount),
    megapo_discount: Math.round(r.megapo_discount),
    burden_est: Math.round(burdenEst),
    l2: Math.round(l2),
    margin_pct: r.net_settlement > 0 ? Math.round(r.variable_margin / r.net_settlement * 1000) / 10 : null,
    mega_dependency_pct: r.gmv > 0 ? Math.round(r.mega_day_gmv / r.gmv * 1000) / 10 : null,
    cost_status: r.all_cost_complete === 1 ? 'complete' : (r.cost_status_sample || 'missing_cost'),
    // 色分け: 未解決 (原価0円計上で過大) > プロモ赤字転落 (L1黒字だが負担後赤字 = 橙) > 赤字 > ok
    flag: r.ever_unresolved === 1 ? 'unresolved'
      : (r.variable_margin >= 0 && l2 < 0) ? 'promo_squeeze'
      : l2 < 0 ? 'loss' : 'ok',
  };
}

export function getSkuProfit(from, to, opts = {}) {
  const db = getMirrorDB();
  const settings = getSettings();
  let rows = db.prepare(skuAggSql()).all(from, to).map(r => shapeSkuRow(r, settings));

  const q = (opts.q || '').trim().toLowerCase();
  if (q) {
    rows = rows.filter(r =>
      r.sku_code.toLowerCase().includes(q)
      || (r.ne_code || '').toLowerCase().includes(q)
      || r.product_name.toLowerCase().includes(q));
  }

  // UI のテーブルヘッダ全列と1:1対応 (whitelist に無い列クリックで silently l2 に落ちない — Codex P3-R2 low)
  const SORTABLE = ['sku_code', 'product_name', 'units_net', 'gmv', 'platform_fee', 'fee_pct',
    'net_settlement', 'shipping', 'cogs', 'variable_margin', 'burden_est', 'l2',
    'margin_pct', 'mega_dependency_pct', 'cost_status'];
  const sortKey = SORTABLE.includes(opts.sort) ? opts.sort : 'l2';
  const dir = opts.dir === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    const av = a[sortKey], bv = b[sortKey];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return av < bv ? -dir : av > bv ? dir : 0;
  });

  const total = rows.length;
  const limit = Math.max(1, Math.min(Number(opts.limit) || 100, 20000));
  const offset = Math.max(Number(opts.offset) || 0, 0);
  return {
    from, to, total,
    burden_rates: { megawari_pct: settings.burden_megawari_pct, megapo_pct: settings.burden_megapo_pct },
    rows: rows.slice(offset, offset + limit),
  };
}

// ─── SKU 詳細 (ドリルダウン: 日次トレンド + マスタ情報) ───
export function getSkuDetail(sku, from, to) {
  const db = getMirrorDB();
  const settings = getSettings();
  const daily = db.prepare(`
    SELECT date_jst, units_net_sold AS units_net,
      gmv_list_price_jpy_incl AS gmv, net_settlement_api_jpy_incl AS net_settlement,
      platform_fee_jpy_incl AS platform_fee, shipping_cost_jpy_incl AS shipping,
      cogs_amount_jpy_incl AS cogs, variable_margin_jpy_incl AS variable_margin,
      megawari_discount_amount_jpy_incl AS megawari_discount,
      megapo_discount_amount_jpy_incl AS megapo_discount,
      megawari_order_count, megapo_order_count,
      match_tier, cost_status, unit_cost_snapshot_incl
    FROM mirror_qoo10_finance_sku_daily
    WHERE sku_code = ? AND date_jst >= ? AND date_jst <= ?
    ORDER BY date_jst
  `).all(sku, from, to).map(r => ({
    ...r,
    gmv: Math.round(r.gmv),
    variable_margin: Math.round(r.variable_margin),
    burden_est: Math.round(burdenOf(r.megawari_discount, r.megapo_discount, settings)),
    is_mega: r.megawari_order_count > 0 || r.megapo_order_count > 0,
  }));

  const latest = db.prepare(`
    SELECT ne_code, product_name, match_tier, unit_cost_snapshot_incl
    FROM mirror_qoo10_finance_sku_daily
    WHERE sku_code = ?
    ORDER BY date_jst DESC LIMIT 1
  `).get(sku) || null;

  let master = null;
  if (latest?.ne_code) {
    try {
      master = db.prepare(`
        SELECT 商品コード AS ne_code, 商品名 AS product_name, 原価 AS unit_cost, 標準売価 AS list_price, 消費税率 AS tax_rate
        FROM mirror_products WHERE 商品コード = ?
      `).get(latest.ne_code) || null;
    } catch { /* mirror_products 無しでも詳細は返す */ }
  }
  return { sku, from, to, latest, master, daily };
}

// ─── 売れ筋分析タブ (F-4b) ───
// 軸: units / sales (GMV) / margin (L1)。ABC・急上昇急落・曜日パターン・
// メガ割日売上比 (Qoo10独自)・スパークライン・新商品バッジ。
export function getBestsellers(from, to, axis) {
  const db = getMirrorDB();
  const settings = getSettings();
  const days = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  const prevTo = addDays(from, -1);
  const prevFrom = addDays(prevTo, -(days - 1));

  const cur = db.prepare(skuAggSql()).all(from, to);
  const prevMap = new Map(db.prepare(skuAggSql()).all(prevFrom, prevTo).map(r => [r.sku_code, r]));

  // 初売上日 (新商品バッジ用)。当期に売上のある SKU に限定して全期間 MIN を取る
  const firstSaleMap = new Map(db.prepare(`
    SELECT sku_code, MIN(date_jst) AS first_date
    FROM mirror_qoo10_finance_sku_daily
    WHERE units_net_sold > 0
      AND sku_code IN (
        SELECT DISTINCT sku_code FROM mirror_qoo10_finance_sku_daily
        WHERE date_jst >= ? AND date_jst <= ?
      )
    GROUP BY sku_code
  `).all(from, to).map(r => [r.sku_code, r.first_date]));
  const newCutoff = addDays(jstToday(), -settings.new_product_days);

  let rows = cur.map(r => {
    const shaped = shapeSkuRow(r, settings);
    const prev = prevMap.get(r.sku_code);
    return {
      ...shaped,
      prev_units: prev ? prev.units_net : 0,
      units_growth_pct: prev && prev.units_net > 0
        ? Math.round((r.units_net - prev.units_net) / prev.units_net * 1000) / 10
        : (r.units_net > 0 ? null : 0),   // null = 前期実績なし (新規扱い)
      is_new: (firstSaleMap.get(r.sku_code) || '') >= newCutoff,
      first_sale_date: firstSaleMap.get(r.sku_code) || null,
    };
  });

  // ABC 分析 (GMV累積構成比)。分類は「加算前の累積比」で判定
  // (加算後判定だと売上が1SKUに集中した時にA群が0件になる — yahoo P3 Codex medium の教訓)
  const byRevenue = [...rows].sort((a, b) => b.gmv - a.gmv);
  const totalRevenue = byRevenue.reduce((s, r) => s + Math.max(0, r.gmv), 0);
  let cum = 0;
  const abcCount = { A: 0, B: 0, C: 0 };
  const abcRevenue = { A: 0, B: 0, C: 0 };
  for (const r of byRevenue) {
    const pctBefore = totalRevenue > 0 ? cum / totalRevenue * 100 : 100;
    cum += Math.max(0, r.gmv);
    r.abc = pctBefore < settings.abc_a_pct ? 'A' : pctBefore < settings.abc_b_pct ? 'B' : 'C';
    abcCount[r.abc]++;
    abcRevenue[r.abc] += Math.max(0, r.gmv);
  }

  const axisKey = axis === 'units' ? 'units_net' : axis === 'margin' ? 'variable_margin' : 'gmv';
  rows = [...rows].sort((a, b) => b[axisKey] - a[axisKey]);

  // 急上昇 / 急落 (当期 or 前期 N 個以上のみ)
  const minU = settings.movers_min_units;
  const movers = rows.filter(r => r.prev_units >= minU || r.units_net >= minU);
  const risers = [...movers].filter(r => r.units_growth_pct !== null && r.units_growth_pct > 0).sort((a, b) => b.units_growth_pct - a.units_growth_pct).slice(0, 20);
  const fallers = [...movers].filter(r => r.units_growth_pct !== null && r.units_growth_pct < 0).sort((a, b) => a.units_growth_pct - b.units_growth_pct).slice(0, 20);

  // 曜日パターン
  const weekday = db.prepare(`
    SELECT CAST(strftime('%w', date_jst) AS INTEGER) AS dow,
      SUM(units_net_sold) AS units, SUM(gmv_list_price_jpy_incl) AS sales,
      COUNT(DISTINCT date_jst) AS days
    FROM mirror_qoo10_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY dow ORDER BY dow
  `).all(from, to).map(r => ({
    dow: r.dow, days: r.days,
    avg_units: r.days > 0 ? Math.round(r.units / r.days * 10) / 10 : 0,
    avg_sales: r.days > 0 ? Math.round(r.sales / r.days) : 0,
  }));

  // メガ日 vs 通常日のリフト (ストア全体。メガ日 = その日のfactにメガ割/メガポ注文が1件でもある日)
  const mega = db.prepare(`
    WITH daily AS (
      SELECT date_jst,
        SUM(gmv_list_price_jpy_incl) AS sales,
        SUM(units_net_sold) AS units,
        MAX(CASE WHEN megawari_order_count > 0 OR megapo_order_count > 0 THEN 1 ELSE 0 END) AS is_mega
      FROM mirror_qoo10_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY date_jst
    )
    SELECT is_mega, COUNT(*) AS days, SUM(sales) AS sales, SUM(units) AS units
    FROM daily GROUP BY is_mega
  `).all(from, to);
  const megaRow = mega.find(g => g.is_mega === 1);
  const normalRow = mega.find(g => g.is_mega === 0);
  const megaSummary = {
    mega_days: megaRow?.days || 0,
    mega_avg_sales: megaRow?.days > 0 ? Math.round(megaRow.sales / megaRow.days) : null,
    normal_days: normalRow?.days || 0,
    normal_avg_sales: normalRow?.days > 0 ? Math.round(normalRow.sales / normalRow.days) : null,
    lift_pct: megaRow?.days > 0 && normalRow?.days > 0 && normalRow.sales > 0
      ? Math.round((megaRow.sales / megaRow.days) / (normalRow.sales / normalRow.days) * 1000) / 10 - 100
      : null,
  };

  // 上位 30 SKU の日次スパークライン
  const topSkus = rows.slice(0, 30).map(r => r.sku_code);
  const sparkMap = new Map();
  if (topSkus.length > 0) {
    const ph = topSkus.map(() => '?').join(',');
    const sparkRows = db.prepare(`
      SELECT sku_code, date_jst, SUM(units_net_sold) AS units
      FROM mirror_qoo10_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ? AND sku_code IN (${ph})
      GROUP BY sku_code, date_jst ORDER BY date_jst
    `).all(from, to, ...topSkus);
    for (const r of sparkRows) {
      if (!sparkMap.has(r.sku_code)) sparkMap.set(r.sku_code, []);
      sparkMap.get(r.sku_code).push({ d: r.date_jst, u: r.units });
    }
  }
  for (const r of rows.slice(0, 30)) r.spark = sparkMap.get(r.sku_code) || [];

  return {
    from, to, prev_from: prevFrom, prev_to: prevTo, axis: axisKey,
    total_skus: rows.length,
    abc: { count: abcCount, revenue: { A: Math.round(abcRevenue.A), B: Math.round(abcRevenue.B), C: Math.round(abcRevenue.C) }, total_revenue: Math.round(totalRevenue) },
    ranking: rows.slice(0, 100),
    risers, fallers, weekday, mega: megaSummary,
  };
}

// ─── メガ割/メガポ 開催回マスタ (F-5。P6 メガ割タブの基盤) ───
export function getMegaEvents() {
  ensureAppTables();
  const db = getMirrorDB();
  const events = db.prepare(`
    SELECT id, kind, label, date_from, date_to, participated, memo, created_at
    FROM qooda_mega_events WHERE deleted_at IS NULL
    ORDER BY date_from DESC, id DESC
  `).all();
  return { events, suggestions: suggestMegaEvents(db, events) };
}

// fact 逆推定: メガ割/メガポ注文が出た日の連続区間 (gap 1日まで許容) を開催回候補として提示。
// §13-⑦「過去参加記録は残っていない」→ 候補から人が採用してマスタを初期構築する。
function suggestMegaEvents(db, existingEvents) {
  const activeDays = db.prepare(`
    SELECT date_jst,
      MAX(CASE WHEN megawari_order_count > 0 THEN 1 ELSE 0 END) AS mw,
      MAX(CASE WHEN megapo_order_count > 0 THEN 1 ELSE 0 END) AS mp
    FROM mirror_qoo10_finance_sku_daily
    GROUP BY date_jst
    HAVING mw = 1 OR mp = 1
    ORDER BY date_jst
  `).all();

  const covered = (kind, d) => existingEvents.some(e => e.kind === kind && e.date_from <= d && d <= e.date_to);
  const out = [];
  for (const kind of ['megawari', 'megapo']) {
    const flagKey = kind === 'megawari' ? 'mw' : 'mp';
    const days = activeDays.filter(r => r[flagKey] === 1 && !covered(kind, r.date_jst)).map(r => r.date_jst);
    let run = null;
    const flush = () => { if (run) { out.push({ kind, date_from: run.from, date_to: run.to, days: run.n }); run = null; } };
    for (const d of days) {
      if (run && d <= addDays(run.to, 2)) { run.to = d; run.n++; }
      else { flush(); run = { from: d, to: d, n: 1 }; }
    }
    flush();
  }
  // 新しい順に最大 12 件 (古い候補は登録が進むと自然に消える)
  return out.sort((a, b) => b.date_from < a.date_from ? -1 : 1).slice(0, 12);
}

export function addMegaEvent(body) {
  ensureAppTables();
  const db = getMirrorDB();
  const kind = body.kind;
  if (!['megawari', 'megapo'].includes(kind)) throw validationError('kind は megawari / megapo で指定してください');
  const dateFrom = String(body.date_from || '').trim();
  const dateTo = String(body.date_to || '').trim();
  if (!isValidDate(dateFrom) || !isValidDate(dateTo)) throw validationError('date_from / date_to は YYYY-MM-DD で指定してください');
  if (dateFrom > dateTo) throw validationError('date_from は date_to 以前にしてください');
  const label = String(body.label || '').trim().slice(0, 100);
  const memo = String(body.memo || '').trim().slice(0, 500);
  const participated = body.participated === 0 || body.participated === false ? 0 : 1;
  // 同種で期間が重なる既存回は二重登録として拒否 (開催回はカレンダー上排他)
  const overlap = db.prepare(`
    SELECT id FROM qooda_mega_events
    WHERE deleted_at IS NULL AND kind = ? AND date_from <= ? AND ? <= date_to
    LIMIT 1
  `).get(kind, dateTo, dateFrom);
  if (overlap) throw validationError(`同じ種別で期間が重なる開催回 (id=${overlap.id}) が登録済みです`);
  const info = db.prepare(`
    INSERT INTO qooda_mega_events (kind, label, date_from, date_to, participated, memo, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(kind, label, dateFrom, dateTo, participated, memo, new Date().toISOString());
  return { id: info.lastInsertRowid, ...getMegaEvents() };
}

export function deleteMegaEvent(id) {
  ensureAppTables();
  const db = getMirrorDB();
  const n = Number(id);
  if (!Number.isInteger(n) || n <= 0) throw validationError('id が不正です');
  const info = db.prepare(`UPDATE qooda_mega_events SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL`)
    .run(new Date().toISOString(), n);
  if (info.changes === 0) throw validationError(`id=${n} の開催回が見つかりません`);
  return getMegaEvents();
}
