/**
 * yahoo-data-lib.js — Yahoo!ストアクリエイターPro 統計CSVの取込ライブラリ (mall-csv-fetcher P1-Y)
 *
 * 対象6種 (実ファイルで仕様確定 2026-07-11、全てCP932・同期DL・/sales_manage/*):
 *   1. 全体分析 (store_overall_kpi): 日次×デバイス売上+PV → fact_yahoo_store_device_daily (縦持ち)
 *   2. 流入・離脱 (store_inoutflow): 日次 訪問者/購入/離脱 → fact_yahoo_inflow_daily
 *   3. お客様分析 (user_report): 日次×性別×年代×客層マトリクス128列 → fact_yahoo_user_attr_daily (縦持ち)
 *   4. 速報 (store_flash): 当日時間帯×デバイス9指標 → fact_yahoo_flash_hourly (縦持ち)
 *   5. 商品分析 (item_report): 商品×14指標・期間集計型 (日付列なし→1日単位DL必須) → fact_yahoo_item_daily
 *   6. 検索流入 (store_keyword): 検索KW×流入/売上600位・期間集計型→1日単位DL → fact_yahoo_keyword_daily
 *
 * 設計判断:
 *   - マトリクス型 (デバイス別・属性別) は縦持ちに正規化 (列セット変更に頑健、集計しやすい)
 *   - 「2未満」はYahoo!のプライバシーマスク → numOrNull が自然に null 化 (0と区別される)
 *   - 期間集計型2種は前置に日付が無い → DL側がファイル名 (yahoo_item_YYYY-MM-DD_*.csv) で
 *     対象日を伝える。ファイル名から日付が取れない場合はエラー (黙って当日扱いしない)
 *   - 金額=税込円INTEGER / 率=REAL / 商品コード=normSku+raw保持 / sha256冪等 / zip=1tx
 *     (楽天 rakuten-data-lib.js と同規約。CSV解析は rakuten-ads-rpp-lib.js を再利用)
 */
import { normSku } from '../../lib/sku-norm.js';
import {
  parseCsvBuffer, numOrNull, normalizeDate, expandFile, GroupAbortError,
} from './rakuten-ads-rpp-lib.js';

const trimS = v => String(v == null ? '' : v).trim();
const yenOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };
const intOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };

const DEVICE_MAP = { '合算値': 'all', 'PC': 'pc', 'スマホ': 'sp', 'アプリ': 'app' };

// ─── DDL ───
export function ensureYahooDataTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_store_device_daily (
    date_jst  TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    device    TEXT NOT NULL CHECK (device IN ('all','pc','sp','app')),
    sales_yen INTEGER, pageviews INTEGER,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, device)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_inflow_daily (
    date_jst TEXT PRIMARY KEY CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    inflow_visitors INTEGER, purchase_visitors INTEGER, purchase_ratio_pct REAL,
    exit_visitors INTEGER, exit_ratio_pct REAL,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL
  )`);

  // お客様分析: 性別×年代×客層 (新規/ライト/ヘビー/合計) の縦持ち
  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_user_attr_daily (
    date_jst    TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    gender      TEXT NOT NULL,
    age_band    TEXT NOT NULL,
    buyer_class TEXT NOT NULL,
    visitors    INTEGER,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, gender, age_band, buyer_class)
  )`);

  // 速報: 時間帯×デバイスの縦持ち (当日しか取れない可能性があるため毎朝スナップ)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_flash_hourly (
    date_jst  TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    hour_slot TEXT NOT NULL,
    device    TEXT NOT NULL CHECK (device IN ('all','pc','sp','app')),
    sales_yen INTEGER, orders INTEGER, units INTEGER, buyers INTEGER,
    purchase_rate_pct REAL, aov_yen INTEGER, pageviews INTEGER, visitors INTEGER, avg_pages REAL,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, hour_slot, device)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_item_daily (
    date_jst   TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    item_code  TEXT NOT NULL,
    sub_code   TEXT NOT NULL DEFAULT '',
    raw_item_code TEXT,
    item_name  TEXT,
    sales_yen INTEGER, orders INTEGER, units INTEGER, buyers INTEGER,
    avg_purchase_rate_pct REAL, favorites INTEGER, cart_adds INTEGER,
    pv_premium_ship INTEGER, pv_normal INTEGER, visitors INTEGER, category_contribution INTEGER,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, item_code, sub_code)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fyid_item ON fact_yahoo_item_daily(item_code, date_jst)`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_yahoo_keyword_daily (
    date_jst TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    keyword  TEXT NOT NULL,
    rank INTEGER, inflow INTEGER, sales_yen INTEGER, orders INTEGER, units INTEGER,
    avg_order_rate_pct REAL, avg_order_aov_yen INTEGER, avg_units_aov_yen INTEGER,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (date_jst, keyword)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fykd_kw ON fact_yahoo_keyword_daily(keyword, date_jst)`);

  // 取込ログ (楽天と同型・Yahoo専用)
  db.exec(`CREATE TABLE IF NOT EXISTS raw_yahoo_data_import_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at  TEXT NOT NULL,
    shop_code    TEXT NOT NULL DEFAULT 'b-faith01',
    source       TEXT NOT NULL,
    file_name    TEXT NOT NULL,
    file_sha256  TEXT NOT NULL,
    file_type    TEXT NOT NULL,
    date_from    TEXT,
    date_to      TEXT,
    row_count    INTEGER NOT NULL DEFAULT 0,
    inserted     INTEGER NOT NULL DEFAULT 0,
    updated      INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL CHECK (status IN ('ok','error','skipped','duplicate')),
    message      TEXT NOT NULL DEFAULT ''
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rydil_sha ON raw_yahoo_data_import_log(file_sha256, status)`);
}

// ─── 共通ヘルパ ───
function findHeaderRow(rows, requiredCols, maxScan = 6) {
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
/** 期間集計型CSVの対象日はファイル名から取る (yahoo_item_2026-07-10_... / *_20260710_*)。
 *  DL側が命名する契約。取れなければエラー (黙って当日扱いしない) */
function dateFromFileName(name) {
  // 実在日検証を通す (2026-99-99 のような不正日付を弾く — Codex Low)
  let m = name.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (m) return normalizeDate(`${m[1]}-${m[2]}-${m[3]}`);
  m = name.match(/(\d{4})(\d{2})(\d{2})/);
  if (m) return normalizeDate(`${m[1]}${m[2]}${m[3]}`);
  return null;
}

// ─── レシピ1: 全体分析 (日次×デバイス → 縦持ち) ───
function prepareStoreDevice(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const devices = [['PC', 'pc'], ['スマホ', 'sp'], ['アプリ', 'app'], ['合算値', 'all']];
  const c = { date: colIndex(header, '日付') };
  for (const [jp, en] of devices) {
    c[`sales_${en}`] = colIndex(header, `${jp}売上合計値`);
    c[`pv_${en}`] = colIndex(header, `${jp}ページビュー`);
  }
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const date = normalizeDate(trimS(r[c.date]));
    if (!date) {
      if (/合計|実績/.test(trimS(r[c.date]))) continue;
      return { name, ok: false, type: 'yahoo_store_device_daily', error: `日付を解釈できない行: 「${trimS(r[c.date])}」` };
    }
    const g = rowLenGuard(name, 'yahoo_store_device_daily', r, header, i + 1); if (g) return g;
    for (const [, en] of devices) {
      records.push({
        date_jst: date, device: en,
        sales_yen: yenOrNull(r[c[`sales_${en}`]]), pageviews: intOrNull(r[c[`pv_${en}`]]),
      });
    }
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_store_device_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_store_device_daily', records, (r) => `${r.date_jst}|${r.device}`, '日付×デバイス'); if (d) return d;
  const dates = records.map(r => r.date_jst).sort();
  return { name, ok: true, type: 'yahoo_store_device_daily', label: '全体分析 (日次×デバイス)', records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ2: 流入・離脱 (日次) ───
function prepareInflow(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const c = {
    date: colIndex(header, '日付'),
    inflow: colIndex(header, '流入全体（訪問者数）_合算値'),
    buy: colIndex(header, '自ストア購入（訪問者数）_合算値'),
    buyPct: colIndex(header, '自ストア購入（構成比）_合算値'),
    exit: colIndex(header, '離脱全体（訪問者数）_合算値'),
    exitPct: colIndex(header, '離脱全体（構成比）_合算値'),
  };
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const date = normalizeDate(trimS(r[c.date]));
    if (!date) {
      if (/合計|実績/.test(trimS(r[c.date]))) continue;
      return { name, ok: false, type: 'yahoo_inflow_daily', error: `日付を解釈できない行: 「${trimS(r[c.date])}」` };
    }
    const g = rowLenGuard(name, 'yahoo_inflow_daily', r, header, i + 1); if (g) return g;
    records.push({
      date_jst: date,
      inflow_visitors: intOrNull(r[c.inflow]), purchase_visitors: intOrNull(r[c.buy]),
      purchase_ratio_pct: numOrNull(r[c.buyPct]),
      exit_visitors: intOrNull(r[c.exit]), exit_ratio_pct: numOrNull(r[c.exitPct]),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_inflow_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_inflow_daily', records, (r) => r.date_jst, '日付'); if (d) return d;
  const dates = records.map(r => r.date_jst).sort();
  return { name, ok: true, type: 'yahoo_inflow_daily', label: '流入・離脱 (日次)', records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ3: お客様分析 (性別×年代×客層マトリクス → 縦持ち) ───
function prepareUserAttr(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const dateIdx = colIndex(header, '日付');
  // 列名 = 性別（X）-年代-客層 をパースして縦持ちマッピングを作る (列セット変更に頑健)
  const attrCols = [];
  for (let i = 0; i < header.length; i++) {
    const m = header[i].match(/^性別（(.+?)）-(.+?)-(.+)$/);
    if (m) attrCols.push({ i, gender: m[1], age: m[2], cls: m[3] });
  }
  if (attrCols.length < 10) {
    return { name, ok: false, type: 'yahoo_user_attr_daily', error: `属性列のパースに失敗 (認識 ${attrCols.length}列)。ヘッダ形式変更の疑い` };
  }
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const date = normalizeDate(trimS(r[dateIdx]));
    if (!date) {
      if (/合計|実績/.test(trimS(r[dateIdx]))) continue;
      return { name, ok: false, type: 'yahoo_user_attr_daily', error: `日付を解釈できない行: 「${trimS(r[dateIdx])}」` };
    }
    const g = rowLenGuard(name, 'yahoo_user_attr_daily', r, header, i + 1); if (g) return g;
    for (const a of attrCols) {
      records.push({ date_jst: date, gender: a.gender, age_band: a.age, buyer_class: a.cls, visitors: intOrNull(r[a.i]) });
    }
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_user_attr_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_user_attr_daily', records, (r) => `${r.date_jst}|${r.gender}|${r.age_band}|${r.buyer_class}`, '日付×属性'); if (d) return d;
  const dates = [...new Set(records.map(r => r.date_jst))].sort();
  return { name, ok: true, type: 'yahoo_user_attr_daily', label: `お客様分析 (属性縦持ち、${attrCols.length}属性)`, records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ4: 速報 (時間帯×デバイス → 縦持ち。日付は先頭行のみ記載→forward-fill) ───
function prepareFlash(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const metrics = [
    ['売上合計値（税込）', 'sales_yen', yenOrNull], ['注文数合計', 'orders', intOrNull],
    ['注文点数合計', 'units', intOrNull], ['注文者数合計', 'buyers', intOrNull],
    ['平均購買率', 'purchase_rate_pct', numOrNull], ['平均客単価', 'aov_yen', yenOrNull],
    ['ページビュー', 'pageviews', intOrNull], ['訪問者数', 'visitors', intOrNull],
    ['平均回遊ページ数', 'avg_pages', numOrNull],
  ];
  const devices = Object.entries(DEVICE_MAP); // [jp, en]
  const c = { date: colIndex(header, '日付'), hour: colIndex(header, '時間帯') };
  for (const [jm, em] of metrics) for (const [jd, ed] of devices) c[`${em}_${ed}`] = colIndex(header, `${jm}（${jd}）`);

  const records = [];
  let curDate = null;
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const rawDate = trimS(r[c.date]);
    if (rawDate) {
      const dNorm = normalizeDate(rawDate);
      if (!dNorm) {
        if (/合計|実績/.test(rawDate)) continue;
        return { name, ok: false, type: 'yahoo_flash_hourly', error: `日付を解釈できない行: 「${rawDate}」` };
      }
      curDate = dNorm;
    }
    if (!curDate) return { name, ok: false, type: 'yahoo_flash_hourly', error: '先頭データ行に日付がありません' };
    const hour = trimS(r[c.hour]);
    if (!hour) continue;
    const g = rowLenGuard(name, 'yahoo_flash_hourly', r, header, i + 1); if (g) return g;
    for (const [, ed] of devices) {
      const rec = { date_jst: curDate, hour_slot: hour.replace(/\s/g, ''), device: ed };
      for (const [, em, fn] of metrics) rec[em] = fn(r[c[`${em}_${ed}`]]);
      records.push(rec);
    }
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_flash_hourly', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_flash_hourly', records, (r) => `${r.date_jst}|${r.hour_slot}|${r.device}`, '日付×時間帯×デバイス'); if (d) return d;
  const dates = [...new Set(records.map(r => r.date_jst))].sort();
  return { name, ok: true, type: 'yahoo_flash_hourly', label: '速報 (時間帯×デバイス)', records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── レシピ5J/6J: 1日単位型のJSON (⭐実測 2026-07-11: 画面のCSVリンクはPOSTのJSONを
// クライアント側でCSV化しているだけ。JSONを直接取ると日付が meta.since に入り、CSVの
// 「2未満」マスクも無い生値が得られる → JSONを正規経路にする) ───
function ymdToIso(v) {
  const s = trimS(v);
  const m = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  return m ? `${m[1]}-${m[2]}-${m[3]}` : normalizeDate(s);
}

/** 商品分析JSON: {meta:{since,until}, results:[{name,code,subCode,gmv,orderCount,...}]} */
function prepareItemJson(name, json) {
  const since = ymdToIso(json?.meta?.since);
  const until = ymdToIso(json?.meta?.until);
  if (!since) return { name, ok: false, type: 'yahoo_item_daily', error: 'meta.since が読めません (JSON形式変更の疑い)' };
  if (since !== until) {
    return { name, ok: false, type: 'yahoo_item_daily', error: `対象期間が複数日 (${since}〜${until})。商品分析は1日単位で取得してください` };
  }
  const results = Array.isArray(json.results) ? json.results : null;
  if (!results) return { name, ok: false, type: 'yahoo_item_daily', error: 'results 配列がありません' };

  const records = [];
  for (const r of results) {
    const rawCode = trimS(r.code);
    if (!rawCode) continue;
    // subCode は 'sub=code=total' が商品合計行を意味する (実測) → 空文字に正規化
    const rawSub = trimS(r.subCode);
    const sub = (!rawSub || rawSub === 'sub=code=total') ? '' : rawSub.toLowerCase();
    records.push({
      date_jst: since, item_code: normSku(rawCode), sub_code: sub, raw_item_code: rawCode,
      item_name: trimS(r.name).slice(0, 300) || null,
      sales_yen: yenOrNull(r.gmv), orders: intOrNull(r.orderCount), units: intOrNull(r.orderQuantity),
      buyers: intOrNull(r.orderer), avg_purchase_rate_pct: numOrNull(r.cvr),
      favorites: intOrNull(r.favoriteCount), cart_adds: intOrNull(r.cartCount),
      pv_premium_ship: intOrNull(r.excellentDeliveryPv), pv_normal: intOrNull(r.notExcellentDeliveryPv),
      visitors: intOrNull(r.uu), category_contribution: intOrNull(r.categoryContributionRank),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_item_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_item_daily', records, (r) => `${r.item_code}|${r.sub_code}`, '商品コード×サブコード'); if (d) return d;
  return { name, ok: true, type: 'yahoo_item_daily', label: `商品分析 (商品×日次、JSON ${records.length}件)`, records, dateFrom: since, dateTo: since };
}

/** 検索流入JSON: 配列 [{keyword,pv,pvRank,gmv,orderCount,...}] (metaなし → 日付はファイル名から) */
function prepareKeywordJson(name, json) {
  const dateJst = dateFromFileName(name);
  if (!dateJst) {
    return { name, ok: false, type: 'yahoo_keyword_daily', error: 'ファイル名から対象日 (YYYY-MM-DD) を特定できません。DL側の命名 (yahoo_keyword_<日付>_*.json) が必要' };
  }
  const results = Array.isArray(json) ? json : (Array.isArray(json?.results) ? json.results : null);
  if (!results) return { name, ok: false, type: 'yahoo_keyword_daily', error: 'KW配列がありません (JSON形式変更の疑い)' };
  const records = [];
  for (const r of results) {
    const kw = trimS(r.keyword);
    if (!kw) continue;
    records.push({
      date_jst: dateJst, keyword: kw.slice(0, 200),
      rank: intOrNull(r.pvRank), inflow: intOrNull(r.pv),
      sales_yen: yenOrNull(r.gmv), orders: intOrNull(r.orderCount), units: intOrNull(r.orderQuantity),
      avg_order_rate_pct: numOrNull(r.orderRate),
      avg_order_aov_yen: yenOrNull(r.orderUnitPrice), avg_units_aov_yen: yenOrNull(r.orderProductPrice),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_keyword_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_keyword_daily', records, (r) => r.keyword, '検索キーワード'); if (d) return d;
  return { name, ok: true, type: 'yahoo_keyword_daily', label: `検索流入 (KW×日次、JSON ${records.length}語)`, records, dateFrom: dateJst, dateTo: dateJst };
}

// ─── レシピ5: 商品分析 (期間集計型 → 対象日はファイル名から) ───
function prepareItemDaily(name, rows, headerIdx) {
  const dateJst = dateFromFileName(name);
  if (!dateJst) {
    return { name, ok: false, type: 'yahoo_item_daily', error: 'ファイル名から対象日 (YYYY-MM-DD) を特定できません。DL側の命名 (yahoo_item_<日付>_*.csv) が必要' };
  }
  const header = rows[headerIdx].map(trimS);
  const c = {
    name: colIndex(header, '商品名'), code: colIndex(header, '商品コード'), sub: colIndex(header, 'サブコード'),
    sales: colIndex(header, '売上合計値（税込）'), orders: colIndex(header, '注文数合計'),
    units: colIndex(header, '注文点数合計'), buyers: colIndex(header, '注文者数合計'),
    rate: colIndex(header, '平均購買率'), fav: colIndex(header, 'お気に入り保存数'),
    cart: colIndex(header, 'カート投入数'),
    pvP: colIndex(header, 'ページビュー（優良配送あり）'), pvN: colIndex(header, 'ページビュー（優良配送なし）'),
    visitors: colIndex(header, '訪問者数'), contrib: colIndex(header, '貢献度（カテゴリ）'),
  };
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const g = rowLenGuard(name, 'yahoo_item_daily', r, header, i + 1); if (g) return g;
    const rawCode = trimS(r[c.code]);
    if (!rawCode) continue;
    // 「2未満」等のマスク値は numOrNull が null 化 (0と区別)
    records.push({
      date_jst: dateJst, item_code: normSku(rawCode), sub_code: trimS(r[c.sub]).toLowerCase(),
      raw_item_code: rawCode, item_name: trimS(r[c.name]).slice(0, 300) || null,
      sales_yen: yenOrNull(r[c.sales]), orders: intOrNull(r[c.orders]), units: intOrNull(r[c.units]),
      buyers: intOrNull(r[c.buyers]), avg_purchase_rate_pct: numOrNull(r[c.rate]),
      favorites: intOrNull(r[c.fav]), cart_adds: intOrNull(r[c.cart]),
      pv_premium_ship: intOrNull(r[c.pvP]), pv_normal: intOrNull(r[c.pvN]),
      visitors: intOrNull(r[c.visitors]), category_contribution: intOrNull(r[c.contrib]),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_item_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_item_daily', records, (r) => `${r.item_code}|${r.sub_code}`, '商品コード×サブコード'); if (d) return d;
  return { name, ok: true, type: 'yahoo_item_daily', label: '商品分析 (商品×日次)', records, dateFrom: dateJst, dateTo: dateJst };
}

// ─── レシピ6: 検索流入 (期間集計型 → 対象日はファイル名から) ───
function prepareKeywordDaily(name, rows, headerIdx) {
  const dateJst = dateFromFileName(name);
  if (!dateJst) {
    return { name, ok: false, type: 'yahoo_keyword_daily', error: 'ファイル名から対象日 (YYYY-MM-DD) を特定できません。DL側の命名 (yahoo_keyword_<日付>_*.csv) が必要' };
  }
  const header = rows[headerIdx].map(trimS);
  const c = {
    rank: colIndex(header, '順位'), kw: colIndex(header, '検索キーワード'),
    inflow: colIndex(header, '流入数'), sales: colIndex(header, '売上合計値(税込)'),
    orders: colIndex(header, '注文数合計'), units: colIndex(header, '注文点数合計'),
    rate: colIndex(header, '平均注文率'), aovOrder: colIndex(header, '平均注文数単価'),
    aovUnit: colIndex(header, '平均注文点数単価'),
  };
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const g = rowLenGuard(name, 'yahoo_keyword_daily', r, header, i + 1); if (g) return g;
    const kw = trimS(r[c.kw]);
    if (!kw) continue;
    records.push({
      date_jst: dateJst, keyword: kw.slice(0, 200),
      rank: intOrNull(r[c.rank]), inflow: intOrNull(r[c.inflow]),
      sales_yen: yenOrNull(r[c.sales]), orders: intOrNull(r[c.orders]), units: intOrNull(r[c.units]),
      avg_order_rate_pct: numOrNull(r[c.rate]),
      avg_order_aov_yen: yenOrNull(r[c.aovOrder]), avg_units_aov_yen: yenOrNull(r[c.aovUnit]),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'yahoo_keyword_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'yahoo_keyword_daily', records, (r) => r.keyword, '検索キーワード'); if (d) return d;
  return { name, ok: true, type: 'yahoo_keyword_daily', label: `検索流入 (KW×日次、${records.length}語)`, records, dateFrom: dateJst, dateTo: dateJst };
}

// ─── 種別判別 + パース ───
export function prepareYahooFile(name, buffer) {
  // JSON (1日単位型の正規経路: 商品分析 / 検索流入) を先に判別
  const head = buffer.slice(0, 8).toString('utf8').trimStart();
  if (head.startsWith('{') || head.startsWith('[')) {
    let json;
    try {
      json = JSON.parse(buffer.toString('utf8'));
    } catch (e) {
      return { name, ok: false, type: 'unknown', error: `JSONパース失敗: ${e.message}` };
    }
    if (json && typeof json === 'object' && !Array.isArray(json) && Array.isArray(json.results)) {
      return prepareItemJson(name, json);
    }
    if (Array.isArray(json)) return prepareKeywordJson(name, json);
    return { name, ok: false, type: 'unknown', error: 'JSON構造を判別できません (results配列でもKW配列でもない)' };
  }

  const rows = parseCsvBuffer(buffer);
  if (rows.length === 0) return { name, ok: false, type: 'unknown', error: '空ファイル' };
  let idx;
  if ((idx = findHeaderRow(rows, ['検索キーワード', '流入数', '平均注文率'])) >= 0) return prepareKeywordDaily(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['商品コード', 'サブコード', 'カート投入数'])) >= 0) return prepareItemDaily(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['日付', '時間帯', '売上合計値（税込）（合算値）'])) >= 0) return prepareFlash(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['日付', '合算値売上合計値', '合算値ページビュー'])) >= 0) return prepareStoreDevice(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['日付', '流入全体（訪問者数）_合算値'])) >= 0) return prepareInflow(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['日付', '性別（全体）-全体-新規'])) >= 0) return prepareUserAttr(name, rows, idx);
  return { name, ok: false, type: 'unknown', error: '種類を判別できません (Yahoo!統計6種のいずれでもない)' };
}

// ─── UPSERT (仕様は楽天 dd-lib と同型) ───
const Y_UPSERT_SPECS = {
  yahoo_store_device_daily: {
    table: 'fact_yahoo_store_device_daily', pk: ['date_jst', 'device'],
    cols: ['date_jst', 'device', 'sales_yen', 'pageviews'],
  },
  yahoo_inflow_daily: {
    table: 'fact_yahoo_inflow_daily', pk: ['date_jst'],
    cols: ['date_jst', 'inflow_visitors', 'purchase_visitors', 'purchase_ratio_pct', 'exit_visitors', 'exit_ratio_pct'],
  },
  yahoo_user_attr_daily: {
    table: 'fact_yahoo_user_attr_daily', pk: ['date_jst', 'gender', 'age_band', 'buyer_class'],
    cols: ['date_jst', 'gender', 'age_band', 'buyer_class', 'visitors'],
  },
  yahoo_flash_hourly: {
    table: 'fact_yahoo_flash_hourly', pk: ['date_jst', 'hour_slot', 'device'],
    cols: ['date_jst', 'hour_slot', 'device', 'sales_yen', 'orders', 'units', 'buyers',
      'purchase_rate_pct', 'aov_yen', 'pageviews', 'visitors', 'avg_pages'],
  },
  yahoo_item_daily: {
    table: 'fact_yahoo_item_daily', pk: ['date_jst', 'item_code', 'sub_code'],
    cols: ['date_jst', 'item_code', 'sub_code', 'raw_item_code', 'item_name',
      'sales_yen', 'orders', 'units', 'buyers', 'avg_purchase_rate_pct', 'favorites', 'cart_adds',
      'pv_premium_ship', 'pv_normal', 'visitors', 'category_contribution'],
  },
  yahoo_keyword_daily: {
    table: 'fact_yahoo_keyword_daily', pk: ['date_jst', 'keyword'],
    cols: ['date_jst', 'keyword', 'rank', 'inflow', 'sales_yen', 'orders', 'units',
      'avg_order_rate_pct', 'avg_order_aov_yen', 'avg_units_aov_yen'],
  },
};
export const YAHOO_DATA_TYPES = new Set(Object.keys(Y_UPSERT_SPECS));

function commitOne(db, logStmt, p, ctx) {
  const { importedAt, source, fileSha256 } = ctx;
  const spec = Y_UPSERT_SPECS[p.type];
  const logInfo = logStmt.run(importedAt, source, p.name, fileSha256, p.type, p.dateFrom, p.dateTo, p.records.length, 0, 0, 'ok', '');
  const importId = logInfo.lastInsertRowid;
  const meta = { source_file: p.name.slice(0, 200), import_id: importId, imported_at: importedAt, updated_at: importedAt };
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
    const pkVals = Object.fromEntries(spec.pk.map(k => [k, r[k]]));
    const exists = existsStmt.get(pkVals);
    upsert.run({ ...r, ...meta });
    if (exists) updated++; else inserted++;
  }
  db.prepare(`UPDATE raw_yahoo_data_import_log SET inserted=?, updated=? WHERE id=?`).run(inserted, updated, importId);
  return {
    file: p.name, ok: true, type: p.type, label: p.label,
    date_from: p.dateFrom, date_to: p.dateTo, rows: p.records.length, inserted, updated,
  };
}

function getLogInsertStmt(db) {
  return db.prepare(`INSERT INTO raw_yahoo_data_import_log
    (imported_at, source, file_name, file_sha256, file_type, date_from, date_to, row_count, inserted, updated, status, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
}

// ─── 入口: 物理ファイル1個 (zip or CSV) を1トランザクションで取込 ───
export function importYahooFile(db, { name, buffer, sha256, source = 'incoming' }) {
  const importedAt = new Date().toISOString();
  const logStmt = getLogInsertStmt(db);
  const logOutcome = (fileName, fileType, status, message) => {
    try {
      logStmt.run(importedAt, source, fileName, sha256, fileType, null, null, 0, 0, 0, status, String(message || '').slice(0, 500));
    } catch (e) { console.error(`[yahoo-data-import] ログ記録失敗: ${e.message}`); }
  };

  // 冪等キー: 本文sha256 + 論理日付 (ファイル名由来)。
  // ⭐検索流入JSONは本文に日付を持たないため、内容が同じ別日 (流入が動かない日等) を
  // sha256だけで duplicate 判定すると2日目が欠損する (Codex R1 High)。
  // ファイル名から日付が取れる場合は (sha256, date_from) の複合で判定する
  const logicalDate = dateFromFileName(name);
  const dup = logicalDate
    ? db.prepare(`SELECT id FROM raw_yahoo_data_import_log WHERE file_sha256 = ? AND status = 'ok' AND date_from = ? LIMIT 1`).get(sha256, logicalDate)
    : db.prepare(`SELECT id FROM raw_yahoo_data_import_log WHERE file_sha256 = ? AND status = 'ok' LIMIT 1`).get(sha256);
  if (dup) {
    logOutcome(name, 'unknown', 'duplicate', `同一内容のファイルを取込済み (import log id=${dup.id}${logicalDate ? `, date=${logicalDate}` : ''})`);
    return { status: 'duplicate', results: [{ file: name, ok: false, duplicate: true, type: 'unknown', error: '取込済み (同一sha256)' }] };
  }

  const g = expandFile(name, buffer);
  if (g.error) {
    logOutcome(g.name, 'unknown', 'error', g.error);
    return { status: 'error', results: [{ file: g.name, ok: false, type: 'unknown', error: g.error }] };
  }

  const fileNames = g.files.map(f => f.name);
  const collected = [];
  const ctx = { importedAt, source, fileSha256: sha256 };
  const tx = db.transaction(() => {
    for (const f of g.files) {
      const p = prepareYahooFile(f.name, f.buffer);
      if (!p.ok) throw new GroupAbortError(p);
      collected.push(commitOne(db, logStmt, p, ctx));
    }
  });
  try {
    tx();
    return { status: 'ok', results: collected };
  } catch (e) {
    if (e instanceof GroupAbortError) {
      const p = e.prep;
      const results = [];
      logOutcome(p.name, p.type, p.skipped ? 'skipped' : 'error', p.error);
      results.push({ file: p.name, ok: false, skipped: !!p.skipped, type: p.type, error: p.error });
      for (const otherName of fileNames.filter(n => n !== p.name)) {
        const msg = '同じzip内の他ファイルにエラーがあるため取込を中止しました (修正後にzipごと再投入してください)';
        logOutcome(otherName, 'unknown', 'skipped', msg);
        results.push({ file: otherName, ok: false, skipped: true, type: 'unknown', error: msg });
      }
      return { status: p.skipped ? 'skipped' : 'error', results };
    }
    console.error(`[yahoo-data-import] group ${g.name}: ${e.stack || e.message}`);
    const results = fileNames.map(fileName => {
      logOutcome(fileName, 'unknown', 'error', e.message);
      return { file: fileName, ok: false, type: 'unknown', error: e.message };
    });
    return { status: 'error', results };
  }
}
