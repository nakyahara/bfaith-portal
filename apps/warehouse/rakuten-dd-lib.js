/**
 * rakuten-dd-lib.js — 楽天RMS「データダウンロード」ハブCSVの取込ライブラリ (mall-csv-fetcher P1-R3)
 *
 * datatool.rms.rakuten.co.jp/datadownload の公式固定フォーマットCSV 7種
 * (実ファイルで仕様確定 2026-07-10。運用型ポイントは未使用で全行空のため対象外):
 *   1. 店舗データ (70列)              → fact_rakuten_store_device_daily (PK date×device)
 *   2. SKU別売上データ (15列)         → fact_rakuten_sku_daily (PK date×sku) + m_rakuten_skus
 *      ⭐システム連携用SKU番号 = NE連携キー。日付列が無い期間集計型 → 1日単位DL必須
 *   3. カテゴリページデータ (51列)    → fact_rakuten_category_daily (PK date×url×device)
 *   4. キャンペーンデータ (4列)       → m_rakuten_campaigns (PK 種類×名×開始日時)
 *   5. 新規・リピート購入者数(店舗別) → fact_rakuten_purchaser_monthly (PK 月初日、過去2年月次)
 *   6. 〃(商品別、2年通算・上位100件) → fact_rakuten_item_purchaser_snapshot (PK 取込日×商品)
 *   7. 〃(商品ジャンル別、1年通算)    → fact_rakuten_genre_purchaser_snapshot (PK 取込日×ジャンル)
 *
 * 設計判断:
 *   - rakuten-data-lib.js と同じ incoming/rakuten-data/ レーンに載せる (種別はヘッダ自動判別)。
 *     取込ログも raw_rakuten_data_import_log を共用 (file_type で区別)
 *   - 金額=税込円 INTEGER / 率・平均・ベンチマーク=REAL / SKU・商品コード=normSku+raw保持 (全社規約)
 *   - 店舗データのベンチマークは全月商クラス保持 (自店クラスは成長で変わる。cls1=月商1億以上,
 *     cls2=3,000万〜9,999万, cls3=1,000万〜2,999万, cls4=100万〜999万, cls5=50万〜99万, cls6=50万未満)
 *   - 通算スナップショット型 (商品別/ジャンル別購入者) は date_jst=取込日JST で日次スナップショット化。
 *     sha256冪等が同一ファイルの二重スナップショットを防ぐ
 *   - 商品別購入者はRMS仕様で上位100件のみ (全件ではない) — 分析時に注意
 */
import { normSku } from '../../lib/sku-norm.js';
import { numOrNull, normalizeDate, normalizeMonth } from './rakuten-ads-rpp-lib.js';
import {
  BENCH_METRICS, BENCH_GROUPS, STORE_BENCH_COLS, CATEGORY_DEMO_DEFS, CATEGORY_DEMO_COLS,
} from '../../lib/rakuten-dd-columns.js';

const trimS = v => String(v == null ? '' : v).trim();
const yenOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };
const intOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };

const DEVICE_MAP = { 'すべて': 'all', 'PC': 'pc', '楽天市場アプリ': 'app', 'スマートフォン': 'sp' };

// ─── DDL ───
export function ensureRakutenDdTables(db) {
  // ベンチ/DEAL等の列群はコード生成で列名を組む (タイポ防止。命名は下の *_COLS と一致)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_store_device_daily (
    date_jst   TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    device     TEXT NOT NULL CHECK (device IN ('all','pc','app','sp')),
    sales_yen INTEGER, orders INTEGER, access_users INTEGER, cvr_pct REAL, aov_yen INTEGER,
    unique_users INTEGER, buyers_member INTEGER, buyers_guest INTEGER, buyers_new INTEGER, buyers_repeat INTEGER,
    tax_out_yen INTEGER, shipping_yen INTEGER, coupon_store_yen INTEGER, coupon_rakuten_yen INTEGER,
    free_ship_coupon_yen INTEGER, wrapping_yen INTEGER, settlement_fee_yen INTEGER,
    ${STORE_BENCH_COLS.map(c => `${c} REAL`).join(', ')},
    deal_sales_yen INTEGER, deal_orders INTEGER, deal_access INTEGER, deal_cvr_pct REAL, deal_aov_yen INTEGER,
    deal_unique_users INTEGER, deal_buyers_member INTEGER, deal_buyers_guest INTEGER,
    deal_buyers_new INTEGER, deal_buyers_repeat INTEGER,
    point_boost_sales_yen INTEGER, point_boost_orders INTEGER, point_boost_grant_fee_yen INTEGER,
    social_gift_sales_yen INTEGER, social_gift_orders INTEGER,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, device)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_sku_daily (
    date_jst            TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    sku_key             TEXT NOT NULL,
    raw_sku_mgmt_number TEXT,
    item_manage_number  TEXT,
    sales_yen INTEGER, orders INTEGER, units INTEGER,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, sku_key)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frsd_sku ON fact_rakuten_sku_daily(sku_key, date_jst)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frsd_item ON fact_rakuten_sku_daily(item_manage_number, date_jst)`);

  // SKUマスタ (最新値のみ)。system_sku_number=RMS「システム連携用SKU番号」(NE連携キー)
  db.exec(`CREATE TABLE IF NOT EXISTS m_rakuten_skus (
    sku_key             TEXT PRIMARY KEY,
    raw_sku_mgmt_number TEXT,
    item_manage_number  TEXT,
    raw_item_manage     TEXT,
    item_number         TEXT,
    catalog_id          TEXT,
    system_sku_number   TEXT,
    sku_attr1 TEXT, sku_attr2 TEXT, sku_attr3 TEXT, sku_attr4 TEXT, sku_attr5 TEXT, sku_attr6 TEXT,
    item_name           TEXT,
    updated_at          TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mrs_item ON m_rakuten_skus(item_manage_number)`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_category_daily (
    date_jst      TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    category_key  TEXT NOT NULL,
    device        TEXT NOT NULL CHECK (device IN ('all','pc','app','sp')),
    hierarchy     TEXT,
    category_name TEXT,
    category_url  TEXT,
    access_users INTEGER, unique_users INTEGER, stay_seconds REAL,
    bounce_count INTEGER, exit_count INTEGER, exit_rate_pct REAL,
    ${CATEGORY_DEMO_COLS.map(c => `${c} INTEGER`).join(', ')},
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, category_key, device)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS m_rakuten_campaigns (
    campaign_type TEXT NOT NULL,
    campaign_name TEXT NOT NULL,
    start_at      TEXT NOT NULL,
    end_at        TEXT,
    date_jst      TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (campaign_type, campaign_name, start_at)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mrc_date ON m_rakuten_campaigns(date_jst)`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_purchaser_monthly (
    date_jst TEXT PRIMARY KEY CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-01'),
    new_buyers INTEGER, new_aov_yen INTEGER, new_sales_yen INTEGER, new_orders INTEGER, new_units INTEGER,
    repeat_buyers INTEGER, repeat_aov_yen INTEGER, repeat_sales_yen INTEGER, repeat_orders INTEGER, repeat_units INTEGER,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_item_purchaser_snapshot (
    date_jst           TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    item_manage_number TEXT NOT NULL,
    item_name TEXT, item_url TEXT, price_yen INTEGER, is_suspended INTEGER,
    new_buyers INTEGER, repeat_buyers INTEGER, repeat_rate_pct REAL,
    window_from TEXT, window_to TEXT,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, item_manage_number)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_genre_purchaser_snapshot (
    date_jst   TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    genre_name TEXT NOT NULL,
    new_buyers INTEGER, repeat_buyers INTEGER, repeat_rate_pct REAL,
    new_avg_purchase_yen INTEGER, repeat_avg_purchase_yen INTEGER,
    avg_purchase_count REAL, avg_purchase_yen INTEGER,
    window_from TEXT, window_to TEXT,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, genre_name)
  )`);
}

// ─── 列名 (lib/rakuten-dd-columns.js を共有。実測ヘッダ 2026-07-10 と1:1対応) ───
function storeBenchHeaderPairs() {
  // ヘッダは指標ごとにグループが並ぶ (売上金額×7group → 売上件数×7group → …)
  const pairs = [];
  for (const [jpMetric, enMetric] of BENCH_METRICS) {
    for (const [jpGroup, enGroup] of BENCH_GROUPS) {
      pairs.push([`${jpGroup} ${jpMetric}`, `${enGroup}_${enMetric}`]);
    }
  }
  return pairs;
}

// ─── 共通ヘルパ ───
function findHeaderRow(rows, requiredCols, maxScan = 12) {
  for (let i = 0; i < Math.min(rows.length, maxScan); i++) {
    const cells = rows[i].map(trimS);
    if (requiredCols.every(c => cells.includes(c))) return i;
  }
  return -1;
}
function colIndex(header, name) {
  const i = header.indexOf(name);
  if (i < 0) throw new Error(`列が見つかりません: ${name}`);
  return i;
}
/** 前置行から データ対象期間 (2026/07/01 ～ 2026/07/31 or 2024/08 ～ 2026/07) を読む */
function readTargetWindow(rows, headerIdx) {
  for (let i = 0; i < headerIdx; i++) {
    const joined = rows[i].map(trimS).join(',');
    let m = joined.match(/(\d{4})\/(\d{2})\/(\d{2})\s*[～~]\s*(\d{4})\/(\d{2})\/(\d{2})/);
    if (m) return { from: `${m[1]}-${m[2]}-${m[3]}`, to: `${m[4]}-${m[5]}-${m[6]}`, grain: 'day' };
    m = joined.match(/(\d{4})\/(\d{2})\s*[～~]\s*(\d{4})\/(\d{2})/);
    if (m) return { from: `${m[1]}-${m[2]}`, to: `${m[3]}-${m[4]}`, grain: 'month' };
  }
  return null;
}
function rowLenGuard(name, type, r, header, rowNo) {
  if (r.length !== header.length) {
    return { name, ok: false, type, error: `列数不一致の行 (row ${rowNo}: ${r.length}列/${header.length}列)。CSVが破損している可能性` };
  }
  return null;
}
function dupGuard(name, type, records, keyFn, keyLabel) {
  const seen = new Set(); const dups = [];
  for (const r of records) { const k = keyFn(r); if (seen.has(k)) dups.push(k); else seen.add(k); }
  if (dups.length > 0) {
    return { name, ok: false, type, error: `ファイル内で${keyLabel}が重複: ${[...new Set(dups)].slice(0, 5).join(', ')} 等 ${dups.length}件` };
  }
  return null;
}
const jstDateOf = (isoTs) => new Date(new Date(isoTs).getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10);

// ─── レシピ1: 店舗データ (日次×デバイス 70列) ───
function prepareStoreDevice(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const c = {
    date: colIndex(header, '日付'), device: colIndex(header, 'デバイス'),
    sales: colIndex(header, '売上金額'), orders: colIndex(header, '売上件数'),
    access: colIndex(header, 'アクセス人数'), cvr: colIndex(header, '転換率'), aov: colIndex(header, '客単価'),
    uu: colIndex(header, 'ユニークユーザー数'),
    buyersMember: colIndex(header, '購入者数（会員）'), buyersGuest: colIndex(header, '購入者数（非会員）'),
    buyersNew: colIndex(header, '新規購入者数'), buyersRepeat: colIndex(header, 'リピート購入者数'),
    taxOut: colIndex(header, '税額（外税額）'), shipping: colIndex(header, '送料額'),
    couponStore: colIndex(header, 'クーポン値引額（店舗）'), couponRakuten: colIndex(header, 'クーポン値引額（楽天）'),
    freeShip: colIndex(header, '送料無料クーポン'), wrapping: colIndex(header, 'のし・ラッピング代金'),
    fee: colIndex(header, '決済手数料'),
  };
  const benchIdx = storeBenchHeaderPairs().map(([jp, en]) => [en, header.indexOf(jp)]);
  const optIdx = (nm) => header.indexOf(nm);
  const opt = {
    deal_sales_yen: [optIdx('楽天スーパーDEAL 売上金額'), yenOrNull], deal_orders: [optIdx('楽天スーパーDEAL 売上件数'), intOrNull],
    deal_access: [optIdx('楽天スーパーDEAL アクセス人数'), intOrNull], deal_cvr_pct: [optIdx('楽天スーパーDEAL 転換率'), numOrNull],
    deal_aov_yen: [optIdx('楽天スーパーDEAL 客単価'), yenOrNull], deal_unique_users: [optIdx('楽天スーパーDEAL ユニークユーザー数'), intOrNull],
    deal_buyers_member: [optIdx('楽天スーパーDEAL 購入者数（会員）'), intOrNull], deal_buyers_guest: [optIdx('楽天スーパーDEAL 購入者数（非会員）'), intOrNull],
    deal_buyers_new: [optIdx('楽天スーパーDEAL 新規購入者数'), intOrNull], deal_buyers_repeat: [optIdx('楽天スーパーDEAL リピート購入者数'), intOrNull],
    point_boost_sales_yen: [optIdx('運用型ポイント変倍経由売上金額'), yenOrNull], point_boost_orders: [optIdx('運用型ポイント変倍経由売上件数'), intOrNull],
    point_boost_grant_fee_yen: [optIdx('運用型ポイント変倍経由ポイント付与料'), yenOrNull],
    social_gift_sales_yen: [optIdx('ソーシャルギフト売上'), yenOrNull], social_gift_orders: [optIdx('ソーシャルギフト売上件数'), intOrNull],
  };

  const records = [];
  let unaggregatedSkipped = 0;
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const rawDate = trimS(r[c.date]);
    const date = normalizeDate(rawDate);
    if (!date) {
      if (/実績|合計/.test(rawDate)) continue;
      return { name, ok: false, type: 'rakuten_store_device_daily', error: `日付を解釈できない行: 「${rawDate}」` };
    }
    const g = rowLenGuard(name, 'rakuten_store_device_daily', r, header, i + 1); if (g) return g;
    const device = DEVICE_MAP[trimS(r[c.device])];
    if (!device) return { name, ok: false, type: 'rakuten_store_device_daily', error: `未知のデバイス: 「${trimS(r[c.device])}」` };
    // 未集計行 (未来日): 自店指標=0 かつ ベンチ空 → スキップ (分析用レポートと同じ複合条件)
    const salesV = yenOrNull(r[c.sales]);
    const accessV = intOrNull(r[c.access]);
    const benchTop10Sales = trimS(benchIdx[0][1] >= 0 ? r[benchIdx[0][1]] : '');
    if ((salesV ?? 0) === 0 && (accessV ?? 0) === 0 && benchTop10Sales === '') { unaggregatedSkipped++; continue; }

    const rec = {
      date_jst: date, device,
      sales_yen: salesV, orders: intOrNull(r[c.orders]), access_users: accessV,
      cvr_pct: numOrNull(r[c.cvr]), aov_yen: yenOrNull(r[c.aov]), unique_users: intOrNull(r[c.uu]),
      buyers_member: intOrNull(r[c.buyersMember]), buyers_guest: intOrNull(r[c.buyersGuest]),
      buyers_new: intOrNull(r[c.buyersNew]), buyers_repeat: intOrNull(r[c.buyersRepeat]),
      tax_out_yen: yenOrNull(r[c.taxOut]), shipping_yen: yenOrNull(r[c.shipping]),
      coupon_store_yen: yenOrNull(r[c.couponStore]), coupon_rakuten_yen: yenOrNull(r[c.couponRakuten]),
      free_ship_coupon_yen: yenOrNull(r[c.freeShip]), wrapping_yen: yenOrNull(r[c.wrapping]),
      settlement_fee_yen: yenOrNull(r[c.fee]),
    };
    for (const [en, idx] of benchIdx) rec[en] = idx >= 0 ? numOrNull(r[idx]) : null;
    for (const [en, [idx, fn]] of Object.entries(opt)) rec[en] = idx >= 0 ? fn(r[idx]) : null;
    records.push(rec);
  }
  if (records.length === 0) return { name, ok: false, type: 'rakuten_store_device_daily', error: '実績のあるデータ行が0件 (未来期間のみのDL?)' };
  const d = dupGuard(name, 'rakuten_store_device_daily', records, (r) => `${r.date_jst}|${r.device}`, '日付×デバイス'); if (d) return d;
  const dates = records.map(r => r.date_jst).sort();
  const label = unaggregatedSkipped > 0 ? `店舗データ (日次×デバイス、未集計${unaggregatedSkipped}行スキップ)` : '店舗データ (日次×デバイス)';
  return { name, ok: true, type: 'rakuten_store_device_daily', label, records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ2: SKU別売上データ (期間集計×SKU、日付列なし → 1日単位DL必須) ───
function prepareSkuDaily(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const win = readTargetWindow(rows, headerIdx);
  if (!win || win.grain !== 'day') {
    return { name, ok: false, type: 'rakuten_sku_daily', error: '前置行から データ対象期間 (YYYY/MM/DD ～ YYYY/MM/DD) を特定できません' };
  }
  if (win.from !== win.to) {
    return { name, ok: false, type: 'rakuten_sku_daily', error: `対象期間が複数日 (${win.from}〜${win.to})。SKU別売上は日付列が無い期間集計型のため「1日単位」でDLしてください` };
  }
  const dateJst = win.from;
  const c = {
    catalog: colIndex(header, 'カタログID'), manage: colIndex(header, '商品管理番号'),
    itemNumber: colIndex(header, '商品番号'), name: colIndex(header, '商品名'),
    skuMgmt: colIndex(header, 'SKU管理番号'), systemSku: colIndex(header, 'システム連携用SKU番号'),
    attrs: [1, 2, 3, 4, 5, 6].map(n => colIndex(header, `SKU項目${n}`)),
    sales: colIndex(header, '売上金額'), orders: colIndex(header, '売上件数'), units: colIndex(header, '売上個数'),
  };
  const records = []; const dims = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const g = rowLenGuard(name, 'rakuten_sku_daily', r, header, i + 1); if (g) return g;
    const rawManage = trimS(r[c.manage]);
    const rawSkuMgmt = trimS(r[c.skuMgmt]) || rawManage; // SKU無し商品はSKU管理番号が空 → 商品管理番号で代替
    if (!rawManage && !rawSkuMgmt) continue;
    // ⭐実測 (2026-07-10): SKU管理番号は商品を跨いで重複する ('normal-inventory' が31商品等)。
    // 一意なのは (商品管理番号 × SKU管理番号) の複合 → sku_key は複合キーにする
    const skuKey = `${normSku(rawManage)}|${normSku(rawSkuMgmt)}`;
    records.push({
      date_jst: dateJst, sku_key: skuKey, raw_sku_mgmt_number: rawSkuMgmt,
      item_manage_number: rawManage ? normSku(rawManage) : null,
      sales_yen: yenOrNull(r[c.sales]), orders: intOrNull(r[c.orders]), units: intOrNull(r[c.units]),
    });
    dims.push({
      sku_key: skuKey, raw_sku_mgmt_number: rawSkuMgmt,
      item_manage_number: rawManage ? normSku(rawManage) : null, raw_item_manage: rawManage || null,
      item_number: trimS(r[c.itemNumber]) || null, catalog_id: trimS(r[c.catalog]) || null,
      system_sku_number: trimS(r[c.systemSku]) || null,
      sku_attr1: trimS(r[c.attrs[0]]) || null, sku_attr2: trimS(r[c.attrs[1]]) || null,
      sku_attr3: trimS(r[c.attrs[2]]) || null, sku_attr4: trimS(r[c.attrs[3]]) || null,
      sku_attr5: trimS(r[c.attrs[4]]) || null, sku_attr6: trimS(r[c.attrs[5]]) || null,
      item_name: trimS(r[c.name]).slice(0, 300) || null,
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'rakuten_sku_daily', error: 'データ行が0件 (売上ゼロの日は行が無い仕様のため、DL側で空判定すべきケース)' };
  const d = dupGuard(name, 'rakuten_sku_daily', records, (r) => r.sku_key, 'SKU管理番号'); if (d) return d;
  return { name, ok: true, type: 'rakuten_sku_daily', label: 'SKU別売上 (SKU×日次)', records, dims, dateFrom: dateJst, dateTo: dateJst };
}

// ─── レシピ3: カテゴリページデータ (日次×カテゴリ×デバイス 51列) ───
function prepareCategoryDaily(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const c = {
    date: colIndex(header, '日付'), hier: colIndex(header, 'カテゴリ階層'), name: colIndex(header, 'カテゴリ名'),
    url: colIndex(header, 'URL'), device: colIndex(header, 'デバイス'),
    access: colIndex(header, 'アクセス人数'), uu: colIndex(header, 'ユニークユーザー数'),
    stay: colIndex(header, '平均滞在時間（秒）'), bounce: colIndex(header, '店舗直帰数'),
    exits: colIndex(header, '店舗離脱数'), exitRate: colIndex(header, '店舗離脱率'),
  };
  const demoIdx = CATEGORY_DEMO_DEFS.map(([jp, en]) => [en, header.indexOf(jp)]);
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const rawDate = trimS(r[c.date]);
    const date = normalizeDate(rawDate);
    if (!date) {
      if (/実績|合計/.test(rawDate)) continue;
      return { name, ok: false, type: 'rakuten_category_daily', error: `日付を解釈できない行: 「${rawDate}」` };
    }
    const g = rowLenGuard(name, 'rakuten_category_daily', r, header, i + 1); if (g) return g;
    const device = DEVICE_MAP[trimS(r[c.device])];
    if (!device) return { name, ok: false, type: 'rakuten_category_daily', error: `未知のデバイス: 「${trimS(r[c.device])}」` };
    const url = trimS(r[c.url]);
    const rec = {
      date_jst: date, device,
      category_key: url || `${trimS(r[c.hier])}|${trimS(r[c.name])}`,
      hierarchy: trimS(r[c.hier]) || null, category_name: trimS(r[c.name]).slice(0, 200) || null,
      category_url: url || null,
      access_users: intOrNull(r[c.access]), unique_users: intOrNull(r[c.uu]), stay_seconds: numOrNull(r[c.stay]),
      bounce_count: intOrNull(r[c.bounce]), exit_count: intOrNull(r[c.exits]), exit_rate_pct: numOrNull(r[c.exitRate]),
    };
    for (const [en, idx] of demoIdx) rec[en] = idx >= 0 ? intOrNull(r[idx]) : null;
    records.push(rec);
  }
  if (records.length === 0) return { name, ok: false, type: 'rakuten_category_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'rakuten_category_daily', records, (r) => `${r.date_jst}|${r.category_key}|${r.device}`, '日付×カテゴリ×デバイス'); if (d) return d;
  const dates = records.map(r => r.date_jst).sort();
  return { name, ok: true, type: 'rakuten_category_daily', label: 'カテゴリページ (日次×デバイス)', records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ4: キャンペーンデータ (開催一覧、参照マスタ) ───
function prepareCampaigns(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const c = {
    type: colIndex(header, 'キャンペーン種類'), name: colIndex(header, 'キャンペーン名'),
    start: colIndex(header, '開始日時'), end: colIndex(header, '終了日時'),
  };
  const records = [];
  const seen = new Set();
  let dupSkipped = 0;
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const g = rowLenGuard(name, 'rakuten_campaigns', r, header, i + 1); if (g) return g;
    const startAt = trimS(r[c.start]);
    const dateJst = normalizeDate(startAt.split(' ')[0]);
    if (!dateJst) return { name, ok: false, type: 'rakuten_campaigns', error: `開始日時を解釈できない行: 「${startAt}」` };
    const rec = {
      campaign_type: trimS(r[c.type]), campaign_name: trimS(r[c.name]).slice(0, 300),
      start_at: startAt, end_at: trimS(r[c.end]) || null, date_jst: dateJst,
    };
    const k = `${rec.campaign_type}|${rec.campaign_name}|${rec.start_at}`;
    if (seen.has(k)) { dupSkipped++; continue; } // 参照マスタのため重複は先勝ちで黙殺せずカウント
    seen.add(k);
    records.push(rec);
  }
  if (records.length === 0) return { name, ok: false, type: 'rakuten_campaigns', error: 'データ行が0件' };
  const dates = records.map(r => r.date_jst).sort();
  const label = dupSkipped > 0 ? `キャンペーン一覧 (重複${dupSkipped}行スキップ)` : 'キャンペーン一覧';
  return { name, ok: true, type: 'rakuten_campaigns', label, records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ5: 新規・リピート購入者数 (店舗別、月次×過去2年) ───
function preparePurchaserMonthly(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const c = {
    date: colIndex(header, '日付'),
    newBuyers: colIndex(header, '新規購入者数'), newAov: colIndex(header, '新規購入 客単価'),
    newSales: colIndex(header, '新規購入 売上'), newOrders: colIndex(header, '新規購入 売上件数'), newUnits: colIndex(header, '新規購入 売上個数'),
    repBuyers: colIndex(header, 'リピート購入者数'), repAov: colIndex(header, 'リピート購入 客単価'),
    repSales: colIndex(header, 'リピート購入 売上'), repOrders: colIndex(header, 'リピート購入 売上件数'), repUnits: colIndex(header, 'リピート購入 売上個数'),
  };
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const rawDate = trimS(r[c.date]);
    const ym = normalizeMonth(rawDate);
    if (!ym) {
      if (/実績|合計/.test(rawDate)) continue;
      return { name, ok: false, type: 'rakuten_purchaser_monthly', error: `月を解釈できない行: 「${rawDate}」` };
    }
    const g = rowLenGuard(name, 'rakuten_purchaser_monthly', r, header, i + 1); if (g) return g;
    records.push({
      date_jst: `${ym}-01`,
      new_buyers: intOrNull(r[c.newBuyers]), new_aov_yen: yenOrNull(r[c.newAov]),
      new_sales_yen: yenOrNull(r[c.newSales]), new_orders: intOrNull(r[c.newOrders]), new_units: intOrNull(r[c.newUnits]),
      repeat_buyers: intOrNull(r[c.repBuyers]), repeat_aov_yen: yenOrNull(r[c.repAov]),
      repeat_sales_yen: yenOrNull(r[c.repSales]), repeat_orders: intOrNull(r[c.repOrders]), repeat_units: intOrNull(r[c.repUnits]),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'rakuten_purchaser_monthly', error: 'データ行が0件' };
  const d = dupGuard(name, 'rakuten_purchaser_monthly', records, (r) => r.date_jst, '月'); if (d) return d;
  const dates = records.map(r => r.date_jst).sort();
  return { name, ok: true, type: 'rakuten_purchaser_monthly', label: '新規・リピート購入者 (店舗別月次)', records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ6: 新規・リピート購入者数 (商品別、通算スナップショット・上位100件) ───
function prepareItemPurchaser(name, rows, headerIdx, snapshotDate) {
  const header = rows[headerIdx].map(trimS);
  const win = readTargetWindow(rows, headerIdx);
  const c = {
    name: colIndex(header, '商品名'), url: colIndex(header, '商品ページURL'),
    price: colIndex(header, '商品価格'), suspended: colIndex(header, '販売停止フラグ'),
    newBuyers: colIndex(header, '新規購入者数'), repBuyers: colIndex(header, 'リピート購入者数'),
    repRate: colIndex(header, 'リピート購入率'),
  };
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const g = rowLenGuard(name, 'rakuten_item_purchaser_snapshot', r, header, i + 1); if (g) return g;
    const url = trimS(r[c.url]);
    const m = url.match(/item\.rakuten\.co\.jp\/[^/]+\/([^/?#]+)/);
    if (!m) return { name, ok: false, type: 'rakuten_item_purchaser_snapshot', error: `商品ページURLから商品管理番号を抽出できない行: 「${url.slice(0, 80)}」` };
    records.push({
      date_jst: snapshotDate, item_manage_number: normSku(m[1]),
      item_name: trimS(r[c.name]).slice(0, 300) || null, item_url: url,
      price_yen: yenOrNull(r[c.price]), is_suspended: intOrNull(r[c.suspended]) ?? 0,
      new_buyers: intOrNull(r[c.newBuyers]), repeat_buyers: intOrNull(r[c.repBuyers]),
      repeat_rate_pct: numOrNull(r[c.repRate]),
      window_from: win?.from || null, window_to: win?.to || null,
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'rakuten_item_purchaser_snapshot', error: 'データ行が0件' };
  const d = dupGuard(name, 'rakuten_item_purchaser_snapshot', records, (r) => r.item_manage_number, '商品管理番号'); if (d) return d;
  return {
    name, ok: true, type: 'rakuten_item_purchaser_snapshot',
    label: `新規・リピート購入者 (商品別通算 上位${records.length}件スナップショット)`,
    records, dateFrom: snapshotDate, dateTo: snapshotDate,
  };
}

// ─── レシピ7: 新規・リピート購入者数 (商品ジャンル別、通算スナップショット) ───
function prepareGenrePurchaser(name, rows, headerIdx, snapshotDate) {
  const header = rows[headerIdx].map(trimS);
  const win = readTargetWindow(rows, headerIdx);
  const c = {
    genre: colIndex(header, 'ジャンル名'),
    newBuyers: colIndex(header, '新規購入者数'), repBuyers: colIndex(header, 'リピート購入者数'),
    repRate: colIndex(header, 'リピート購入率'),
    newAvg: colIndex(header, '新規購入者の平均購入金額'), repAvg: colIndex(header, 'リピート購入者の平均購入金額'),
    avgCount: colIndex(header, '平均購入回数'), avgYen: colIndex(header, '1回あたりの平均購入金額'),
  };
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const g = rowLenGuard(name, 'rakuten_genre_purchaser_snapshot', r, header, i + 1); if (g) return g;
    const genre = trimS(r[c.genre]);
    if (!genre) continue;
    records.push({
      date_jst: snapshotDate, genre_name: genre.slice(0, 300),
      new_buyers: intOrNull(r[c.newBuyers]), repeat_buyers: intOrNull(r[c.repBuyers]),
      repeat_rate_pct: numOrNull(r[c.repRate]),
      new_avg_purchase_yen: yenOrNull(r[c.newAvg]), repeat_avg_purchase_yen: yenOrNull(r[c.repAvg]),
      avg_purchase_count: numOrNull(r[c.avgCount]), avg_purchase_yen: yenOrNull(r[c.avgYen]),
      window_from: win?.from || null, window_to: win?.to || null,
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'rakuten_genre_purchaser_snapshot', error: 'データ行が0件' };
  const d = dupGuard(name, 'rakuten_genre_purchaser_snapshot', records, (r) => r.genre_name, 'ジャンル名'); if (d) return d;
  return {
    name, ok: true, type: 'rakuten_genre_purchaser_snapshot',
    label: '新規・リピート購入者 (ジャンル別通算スナップショット)',
    records, dateFrom: snapshotDate, dateTo: snapshotDate,
  };
}

// ─── 種別判別 + パース (rakuten-data-lib.js の prepareDataFile から fall-through で呼ばれる) ───
// snapshotDate: 通算スナップショット型の date_jst (取込日JST)。省略時は現在時刻から計算
export function prepareDdFile(name, rows, snapshotDate = jstDateOf(new Date().toISOString())) {
  let idx;
  // 判別は「そのレシピにしか無い列」の組み合わせで行う (誤判別防止のため具体的な順)
  if ((idx = findHeaderRow(rows, ['SKU管理番号', 'システム連携用SKU番号', '売上個数'])) >= 0) return prepareSkuDaily(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['日付', 'デバイス', '売上金額', 'ユニークユーザー数'])) >= 0) return prepareStoreDevice(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['カテゴリ階層', 'カテゴリ名', 'アクセス人数'])) >= 0) return prepareCategoryDaily(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['キャンペーン種類', 'キャンペーン名', '開始日時'])) >= 0) return prepareCampaigns(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['日付', '新規購入 客単価', 'リピート購入 売上'])) >= 0) return preparePurchaserMonthly(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['商品ページURL', '販売停止フラグ', 'リピート購入率'])) >= 0) return prepareItemPurchaser(name, rows, idx, snapshotDate);
  if ((idx = findHeaderRow(rows, ['ジャンル名', 'リピート購入率', '平均購入回数'])) >= 0) return prepareGenrePurchaser(name, rows, idx, snapshotDate);
  return null; // dd系ではない → 呼び出し元が unknown 処理
}

// ─── UPSERT (rakuten-data-lib.js の commitOne から delegate される。呼び出し元 tx 内) ───
const DD_UPSERT_SPECS = {
  rakuten_store_device_daily: {
    table: 'fact_rakuten_store_device_daily',
    pk: ['date_jst', 'device'],
    cols: ['date_jst', 'device', 'sales_yen', 'orders', 'access_users', 'cvr_pct', 'aov_yen',
      'unique_users', 'buyers_member', 'buyers_guest', 'buyers_new', 'buyers_repeat',
      'tax_out_yen', 'shipping_yen', 'coupon_store_yen', 'coupon_rakuten_yen',
      'free_ship_coupon_yen', 'wrapping_yen', 'settlement_fee_yen',
      ...STORE_BENCH_COLS,
      'deal_sales_yen', 'deal_orders', 'deal_access', 'deal_cvr_pct', 'deal_aov_yen',
      'deal_unique_users', 'deal_buyers_member', 'deal_buyers_guest', 'deal_buyers_new', 'deal_buyers_repeat',
      'point_boost_sales_yen', 'point_boost_orders', 'point_boost_grant_fee_yen',
      'social_gift_sales_yen', 'social_gift_orders'],
  },
  rakuten_sku_daily: {
    table: 'fact_rakuten_sku_daily',
    pk: ['date_jst', 'sku_key'],
    cols: ['date_jst', 'sku_key', 'raw_sku_mgmt_number', 'item_manage_number', 'sales_yen', 'orders', 'units'],
  },
  rakuten_category_daily: {
    table: 'fact_rakuten_category_daily',
    pk: ['date_jst', 'category_key', 'device'],
    cols: ['date_jst', 'category_key', 'device', 'hierarchy', 'category_name', 'category_url',
      'access_users', 'unique_users', 'stay_seconds', 'bounce_count', 'exit_count', 'exit_rate_pct',
      ...CATEGORY_DEMO_COLS],
  },
  rakuten_campaigns: {
    table: 'm_rakuten_campaigns',
    pk: ['campaign_type', 'campaign_name', 'start_at'],
    cols: ['campaign_type', 'campaign_name', 'start_at', 'end_at', 'date_jst'],
    noTaxCol: true,
  },
  rakuten_purchaser_monthly: {
    table: 'fact_rakuten_purchaser_monthly',
    pk: ['date_jst'],
    cols: ['date_jst', 'new_buyers', 'new_aov_yen', 'new_sales_yen', 'new_orders', 'new_units',
      'repeat_buyers', 'repeat_aov_yen', 'repeat_sales_yen', 'repeat_orders', 'repeat_units'],
  },
  rakuten_item_purchaser_snapshot: {
    table: 'fact_rakuten_item_purchaser_snapshot',
    pk: ['date_jst', 'item_manage_number'],
    cols: ['date_jst', 'item_manage_number', 'item_name', 'item_url', 'price_yen', 'is_suspended',
      'new_buyers', 'repeat_buyers', 'repeat_rate_pct', 'window_from', 'window_to'],
    noTaxCol: true,
  },
  rakuten_genre_purchaser_snapshot: {
    table: 'fact_rakuten_genre_purchaser_snapshot',
    pk: ['date_jst', 'genre_name'],
    cols: ['date_jst', 'genre_name', 'new_buyers', 'repeat_buyers', 'repeat_rate_pct',
      'new_avg_purchase_yen', 'repeat_avg_purchase_yen', 'avg_purchase_count', 'avg_purchase_yen',
      'window_from', 'window_to'],
    noTaxCol: true,
  },
};
export const DD_TYPES = new Set(Object.keys(DD_UPSERT_SPECS));

// キャンペーン・スナップショット系に is_tax_included は無い (金額の性質が snapshot/参照)。
// fact_rakuten_store_device_daily 等は DDL 側 DEFAULT 1 に任せる (INSERT 列に含めない)
export function commitDdOne(db, p, meta) {
  const spec = DD_UPSERT_SPECS[p.type];
  if (!spec) throw new Error(`unknown dd type: ${p.type}`);
  let inserted = 0, updated = 0;
  const existsStmt = db.prepare(`SELECT 1 FROM ${spec.table} WHERE ${spec.pk.map(k => `${k}=@${k}`).join(' AND ')}`);
  const upsert = db.prepare(`INSERT INTO ${spec.table}
    (${spec.cols.join(', ')}, source_file, import_id, imported_at, updated_at)
    VALUES (${spec.cols.map(k => '@' + k).join(', ')}, @source_file, @import_id, @imported_at, @updated_at)
    ON CONFLICT (${spec.pk.join(', ')}) DO UPDATE SET
      ${spec.cols.filter(k => !spec.pk.includes(k)).map(k => `${k}=excluded.${k}`).join(', ')},
      source_file=excluded.source_file, import_id=excluded.import_id,
      imported_at=excluded.imported_at, updated_at=excluded.updated_at`);
  for (const r of p.records) {
    const pkVals = Object.fromEntries(spec.pk.map(k => [k, r[k]])); // better-sqlite3 は未使用 named param でエラー
    const exists = existsStmt.get(pkVals);
    upsert.run({ ...r, ...meta });
    if (exists) updated++; else inserted++;
  }
  if (p.type === 'rakuten_sku_daily' && p.dims) {
    const dimUpsert = db.prepare(`INSERT INTO m_rakuten_skus
      (sku_key, raw_sku_mgmt_number, item_manage_number, raw_item_manage, item_number, catalog_id,
       system_sku_number, sku_attr1, sku_attr2, sku_attr3, sku_attr4, sku_attr5, sku_attr6, item_name, updated_at)
      VALUES (@sku_key, @raw_sku_mgmt_number, @item_manage_number, @raw_item_manage, @item_number, @catalog_id,
       @system_sku_number, @sku_attr1, @sku_attr2, @sku_attr3, @sku_attr4, @sku_attr5, @sku_attr6, @item_name, @updated_at)
      ON CONFLICT (sku_key) DO UPDATE SET
        raw_sku_mgmt_number=excluded.raw_sku_mgmt_number, item_manage_number=excluded.item_manage_number,
        raw_item_manage=excluded.raw_item_manage, item_number=excluded.item_number, catalog_id=excluded.catalog_id,
        system_sku_number=excluded.system_sku_number,
        sku_attr1=excluded.sku_attr1, sku_attr2=excluded.sku_attr2, sku_attr3=excluded.sku_attr3,
        sku_attr4=excluded.sku_attr4, sku_attr5=excluded.sku_attr5, sku_attr6=excluded.sku_attr6,
        item_name=excluded.item_name, updated_at=excluded.updated_at`);
    for (const d of p.dims) dimUpsert.run({ ...d, updated_at: meta.updated_at });
  }
  return { inserted, updated };
}
