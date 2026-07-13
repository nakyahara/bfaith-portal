/**
 * aupay-data-lib.js — au PAYマーケット WOW!マネージャー 分析CSVの取込ライブラリ (mall-csv-fetcher P1-A)
 *
 * 対象 (実ファイルで仕様確定 2026-07-12〜13):
 *   店舗分析CSVダウンロード (shopAnalyCsvDl、Shift_JIS、前文1-2行+空行+ヘッダ):
 *     1. 売上分析 (sales)    : セグメント×日/月×売上・クーポン・ポイント・CVR・販促費率
 *     2. 参照元分析 (referer): セグメント×日/月×参照元チャネル
 *     3. 検索分析 (search)   : セグメント×日/月×流入キーワード
 *     4. ページ分析 (page)   : 日/月×ページURL (セグメントなし)
 *     5. 商品分析 (product)  : セグメント×日/月×ロットナンバー
 *     6. リピート分析 (rf)   : 月次専用コホート行列 (「YYYY/MM起点」ブロック×過去13ヶ月列) → 縦持ち
 *   プラチナマッチ広告 (admin.ads.wowma.s4p.jp、UTF-8 BOM):
 *     7. PM日次レポート (aupay_pm_ad_daily): GET ?format=csv&aggregate=daily
 *     8. PM検索クエリ (aupay_pm_query_weekly): 画面スクレイプのJSON (前週分トップ100、週次更新)
 *
 * 設計判断 (Codex R1/R2反映):
 *   - 日次と月次は別テーブル (別粒度で保持、月次で日次を上書きしない)。月次の date_jst=月初日
 *   - 顧客セグメントは正規化コード (segment_code) を主キーに、原文 (segment_raw) も保持。
 *     未知セグメントは unk_<hash8> で取り込み warning (推測で既存コードに割り当てない)
 *   - CSV前文の「抽出年月日/抽出年月」を実データ日付と突合 (範囲逸脱=エラー — 未集計クランプ検知)
 *   - rf の「リピート受注数合計」列は明細と一緒に格納せず検算に使う (不一致=エラー)
 *   - 金額=税込円INTEGER / 率=REAL / sha256+論理日付の複合冪等 / zip=1tx
 *     (CSV解析は rakuten-ads-rpp-lib.js を再利用。yahoo-data-lib.js と同規約)
 */
import crypto from 'node:crypto';
import {
  parseCsvBuffer, numOrNull, normalizeDate, normalizeMonth, expandFile, GroupAbortError,
} from './rakuten-ads-rpp-lib.js';

const trimS = v => String(v == null ? '' : v).trim();
const yenOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };
const intOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };

// ─── DDL ───
export function ensureAupayDataTables(db) {
  const metaCols = `source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL`;
  const dateCheck = `CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')`;

  // 売上分析 (日次/月次同型。月次は date_jst=月初日)
  for (const t of ['fact_aupay_sales_daily', 'fact_aupay_sales_monthly']) {
    db.exec(`CREATE TABLE IF NOT EXISTS ${t} (
      date_jst TEXT NOT NULL ${dateCheck},
      segment_code TEXT NOT NULL,
      segment_raw  TEXT NOT NULL,
      sales_yen INTEGER, coupon_store_yen INTEGER, coupon_mall_yen INTEGER,
      point_ponta_au_yen INTEGER, point_used_yen INTEGER,
      orders INTEGER, units INTEGER, visits INTEGER, buyer_uu INTEGER, visitor_uu INTEGER, pageviews INTEGER,
      cvr_pct REAL, avg_units REAL, avg_price_yen INTEGER, avg_visits REAL, avg_pv REAL,
      coupon_store_rate_pct REAL, coupon_mall_rate_pct REAL, point_rate_pct REAL,
      is_tax_included INTEGER NOT NULL DEFAULT 1,
      ${metaCols},
      PRIMARY KEY (date_jst, segment_code)
    )`);
  }

  // 参照元分析
  for (const t of ['fact_aupay_referer_daily', 'fact_aupay_referer_monthly']) {
    db.exec(`CREATE TABLE IF NOT EXISTS ${t} (
      date_jst TEXT NOT NULL ${dateCheck},
      segment_code TEXT NOT NULL, segment_raw TEXT NOT NULL,
      channel TEXT NOT NULL,
      visits INTEGER, visitor_uu INTEGER, sales_yen INTEGER, orders INTEGER, units INTEGER,
      cvr_pct REAL, avg_units REAL, avg_price_yen INTEGER,
      is_tax_included INTEGER NOT NULL DEFAULT 1,
      ${metaCols},
      PRIMARY KEY (date_jst, segment_code, channel)
    )`);
  }

  // 検索分析 (流入キーワード)
  for (const t of ['fact_aupay_search_daily', 'fact_aupay_search_monthly']) {
    db.exec(`CREATE TABLE IF NOT EXISTS ${t} (
      date_jst TEXT NOT NULL ${dateCheck},
      segment_code TEXT NOT NULL, segment_raw TEXT NOT NULL,
      keyword TEXT NOT NULL,
      visits INTEGER, visitor_uu INTEGER,
      ${metaCols},
      PRIMARY KEY (date_jst, segment_code, keyword)
    )`);
  }
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fasd_kw ON fact_aupay_search_daily(keyword, date_jst)`);

  // ページ分析 (セグメントなし)
  for (const t of ['fact_aupay_page_daily', 'fact_aupay_page_monthly']) {
    db.exec(`CREATE TABLE IF NOT EXISTS ${t} (
      date_jst TEXT NOT NULL ${dateCheck},
      page_url TEXT NOT NULL,
      pageviews INTEGER, orders INTEGER, via_orders INTEGER,
      bounce_rate_pct REAL, exit_rate_pct REAL,
      ${metaCols},
      PRIMARY KEY (date_jst, page_url)
    )`);
  }

  // 商品分析 (ロットナンバー=au PAY商品ID)
  for (const t of ['fact_aupay_product_daily', 'fact_aupay_product_monthly']) {
    db.exec(`CREATE TABLE IF NOT EXISTS ${t} (
      date_jst TEXT NOT NULL ${dateCheck},
      segment_code TEXT NOT NULL, segment_raw TEXT NOT NULL,
      lot_number TEXT NOT NULL,
      product_name TEXT, category TEXT,
      sales_yen INTEGER, orders INTEGER, units INTEGER, avg_units REAL, avg_price_yen INTEGER,
      coupon_store_yen INTEGER, coupon_mall_yen INTEGER,
      buyer_uu INTEGER, visits INTEGER, visitor_uu INTEGER, pageviews INTEGER,
      cvr_visit_pct REAL, cvr_uu_pct REAL,
      is_tax_included INTEGER NOT NULL DEFAULT 1,
      ${metaCols},
      PRIMARY KEY (date_jst, segment_code, lot_number)
    )`);
  }
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fapd_lot ON fact_aupay_product_daily(lot_number, date_jst)`);

  // リピート分析コホート (月次専用): date_jst=起点月初日、target_month_jst=対象月初日
  db.exec(`CREATE TABLE IF NOT EXISTS fact_aupay_repeat_cohort_monthly (
    date_jst TEXT NOT NULL ${dateCheck},
    segment_code TEXT NOT NULL, segment_raw TEXT NOT NULL,
    target_month_jst TEXT NOT NULL CHECK (target_month_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-01'),
    orders INTEGER,
    ${metaCols},
    PRIMARY KEY (date_jst, segment_code, target_month_jst)
  )`);

  // プラチナマッチ広告 日次レポート
  db.exec(`CREATE TABLE IF NOT EXISTS fact_aupay_pm_ad_daily (
    date_jst TEXT NOT NULL ${dateCheck},
    impressions INTEGER, clicks INTEGER, ctr_pct REAL, cpc_yen REAL,
    gmv_via_ad_yen INTEGER, roas REAL, cost_yen INTEGER,
    is_tax_included INTEGER NOT NULL DEFAULT 1,
    ${metaCols},
    PRIMARY KEY (date_jst)
  )`);

  // プラチナマッチ 検索クエリ (前週分トップ100の週次スナップショット。date_jst=対象週の月曜)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_aupay_pm_query_weekly (
    date_jst TEXT NOT NULL ${dateCheck},
    keyword TEXT NOT NULL,
    rank INTEGER, impressions INTEGER, clicks INTEGER, ctr_pct REAL,
    ${metaCols},
    PRIMARY KEY (date_jst, keyword)
  )`);

  // 取込ログ (yahoo と同型・aupay 専用)
  db.exec(`CREATE TABLE IF NOT EXISTS raw_aupay_data_import_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at  TEXT NOT NULL,
    shop_code    TEXT NOT NULL DEFAULT '54318092',
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
  db.exec(`CREATE INDEX IF NOT EXISTS idx_radil_sha ON raw_aupay_data_import_log(file_sha256, status)`);
}

// ─── セグメント正規化 (Codex R2: コード主キー+raw保持、未知は推測割当しない) ───
const SEGMENT_MAP = {
  '全顧客': 'all',
  '初回購入顧客(新規)': 'new',
  '2回購入以上(既存合算)': 'repeat_all',
};
export function segmentCode(raw) {
  // 全角括弧/空白ゆれを正規化してからマップ (実CSVは half/full 混在: sales=半角、rf=全角)
  const norm = trimS(raw).replace(/（/g, '(').replace(/）/g, ')').replace(/\s+/g, '');
  if (SEGMENT_MAP[norm]) return { code: SEGMENT_MAP[norm], warning: null };
  let m = norm.match(/^(\d+)回購入顧客$/);
  if (m) return { code: `repeat_${m[1]}`, warning: null };
  m = norm.match(/^(\d+)回以上購入顧客\(継続\)$/);
  if (m) return { code: `repeat_${m[1]}plus`, warning: null };
  const hash = crypto.createHash('sha256').update(norm).digest('hex').slice(0, 8);
  return { code: `unk_${hash}`, warning: `未知の顧客セグメント名「${trimS(raw)}」→ unk_${hash} で取込 (マッピング追加を検討)` };
}

// ─── 共通ヘルパ ───
function findHeaderRow(rows, requiredCols, maxScan = 8) {
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
/** ファイル名の論理日付マーカー (yahoo と同じ `_d<YYYY-MM-DD>_` 契約) */
function dateFromFileName(name) {
  const m = name.match(/_d(\d{4}-\d{2}-\d{2})_/);
  if (!m) return null;
  return normalizeDate(m[1]);
}

/** 年月日セル → { grain: 'daily'|'monthly', dateJst } (月次は月初日へ)。解釈不能は null */
function parseDateCell(v) {
  const s = trimS(v);
  if (/^\d{4}\/\d{1,2}\/\d{1,2}$/.test(s)) {
    const d = normalizeDate(s);
    return d ? { grain: 'daily', dateJst: d } : null;
  }
  if (/^\d{4}\/\d{1,2}$/.test(s)) {
    const ym = normalizeMonth(s);
    return ym ? { grain: 'monthly', dateJst: `${ym}-01` } : null;
  }
  return null;
}

/** 前文の「抽出年月日：A～B」「抽出年月：A～B」→ {from,to} (日次はYYYY-MM-DD、月次は月初日) */
function parsePreambleRange(rows, headerIdx) {
  for (let i = 0; i < headerIdx; i++) {
    const cell = trimS(rows[i][0]);
    const m = cell.match(/^抽出年月日?[:：]\s*(.+?)\s*[～〜]\s*(.+)$/);
    if (!m) continue;
    const a = parseDateCell(m[1]); const b = parseDateCell(m[2]);
    if (a && b) return { from: a.dateJst, to: b.dateJst, grain: a.grain };
  }
  return null;
}

/**
 * セグメント×年月日型の共通パーサ。metricsMap = [{col, key, fn}]
 * keyExtra: 行から追加キー列 (channel/keyword/lot_number等) を取る関数 (null=行スキップ)
 */
function prepareSegmentDated(name, rows, headerIdx, {
  baseType, label, hasSegment = true, metrics, keyExtra = null, extraCols = null,
}) {
  const header = rows[headerIdx].map(trimS);
  const c = { date: colIndex(header, '年月日') };
  if (hasSegment) c.seg = colIndex(header, '顧客セグメント名');
  const mcols = metrics.map(([colName, key, fn]) => [colIndex(header, colName), key, fn]);
  const ecols = extraCols ? Object.fromEntries(Object.entries(extraCols).map(([k, col]) => [k, colIndex(header, col)])) : {};

  const preamble = parsePreambleRange(rows, headerIdx);
  const records = [];
  const warnings = [];
  const warned = new Set();
  let grain = null;
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const dc = parseDateCell(r[c.date]);
    if (!dc) {
      if (/合計/.test(trimS(r[c.date]))) continue; // 合計行は明細と混ぜない (Codex R2)
      return { name, ok: false, type: baseType, error: `年月日を解釈できない行 (row ${i + 1}): 「${trimS(r[c.date])}」` };
    }
    if (grain && dc.grain !== grain) {
      return { name, ok: false, type: baseType, error: `日次と月次の行が混在 (row ${i + 1})。CSVの仕様変更の疑い` };
    }
    grain = dc.grain;
    const g = rowLenGuard(name, baseType, r, header, i + 1); if (g) return g;

    const rec = { date_jst: dc.dateJst };
    if (hasSegment) {
      const raw = trimS(r[c.seg]);
      if (!raw) continue;
      const { code, warning } = segmentCode(raw);
      if (warning && !warned.has(code)) { warnings.push(warning); warned.add(code); }
      rec.segment_code = code;
      rec.segment_raw = raw;
    }
    if (keyExtra) {
      const extra = keyExtra(r, ecols);
      if (extra === null) continue;
      Object.assign(rec, extra);
    }
    for (const [idx, key, fn] of mcols) rec[key] = fn(r[idx]);
    records.push(rec);
  }
  if (records.length === 0) return { name, ok: false, type: baseType, error: 'データ行が0件' };
  const type = `${baseType}_${grain}`;

  const dates = [...new Set(records.map(r => r.date_jst))].sort();
  // 前文range と実データの突合 (範囲逸脱 = サーバ側クランプ/仕様変更の検知)
  if (preamble) {
    if (preamble.grain !== grain) {
      return { name, ok: false, type, error: `前文の粒度 (${preamble.grain}) とデータ行の粒度 (${grain}) が不一致` };
    }
    const out = dates.filter(d => d < preamble.from || d > preamble.to);
    if (out.length > 0) {
      return { name, ok: false, type, error: `抽出範囲 ${preamble.from}〜${preamble.to} 外の日付がデータに含まれる: ${out.slice(0, 3).join(', ')}` };
    }
  }
  // scope置換範囲: 前文の抽出範囲が正 (補正版で消えたディメンション行を残さないため、
  // 取込時に scope 内を DELETE→INSERT する — Codex 実装レビュー R1 High)
  const scopeFrom = preamble ? preamble.from : dates[0];
  const scopeTo = preamble ? preamble.to : dates[dates.length - 1];
  return { name, ok: true, type, label: `${label} (${grain === 'daily' ? '日次' : '月次'})`, records, warnings, dateFrom: dates[0], dateTo: dates[dates.length - 1], scopeFrom, scopeTo };
}

// ─── 各レシピ ───
function prepareSales(name, rows, headerIdx) {
  return prepareSegmentDated(name, rows, headerIdx, {
    baseType: 'aupay_sales', label: '売上分析',
    metrics: [
      ['売上高', 'sales_yen', yenOrNull],
      ['利用店舗原資クーポン', 'coupon_store_yen', yenOrNull],
      ['利用モール原資クーポン', 'coupon_mall_yen', yenOrNull],
      ['Pontaポイント(au PAY マーケット限定含む)・auポイント利用合算', 'point_ponta_au_yen', yenOrNull],
      ['利用ポイント', 'point_used_yen', yenOrNull],
      ['購入回数', 'orders', intOrNull],
      ['購入個数', 'units', intOrNull],
      ['来訪回数', 'visits', intOrNull],
      ['購入UU数', 'buyer_uu', intOrNull],
      ['訪問者(UU)', 'visitor_uu', intOrNull],
      ['PV数', 'pageviews', intOrNull],
      ['CVR', 'cvr_pct', numOrNull],
      ['平均購入個数', 'avg_units', numOrNull],
      ['平均購入単価', 'avg_price_yen', yenOrNull],
      ['平均来訪回数', 'avg_visits', numOrNull],
      ['平均PV数(回遊数)', 'avg_pv', numOrNull],
      ['売上に対する店舗原資クーポン利用額率(販促費率)', 'coupon_store_rate_pct', numOrNull],
      ['売上に対するモール原資クーポン利用額率(販促費率)', 'coupon_mall_rate_pct', numOrNull],
      ['ポイント利用額率(販促費率)', 'point_rate_pct', numOrNull],
    ],
  });
}

function prepareReferer(name, rows, headerIdx) {
  return prepareSegmentDated(name, rows, headerIdx, {
    baseType: 'aupay_referer', label: '参照元分析',
    extraCols: { channel: '参照元チャネル' },
    keyExtra: (r, e) => {
      const ch = trimS(r[e.channel]);
      return ch ? { channel: ch } : null;
    },
    metrics: [
      ['来訪回数', 'visits', intOrNull],
      ['訪問者(UU)', 'visitor_uu', intOrNull],
      ['売上高', 'sales_yen', yenOrNull],
      ['購入回数', 'orders', intOrNull],
      ['購入個数', 'units', intOrNull],
      ['CVR', 'cvr_pct', numOrNull],
      ['平均購入個数', 'avg_units', numOrNull],
      ['平均購入単価', 'avg_price_yen', yenOrNull],
    ],
  });
}

function prepareSearch(name, rows, headerIdx) {
  return prepareSegmentDated(name, rows, headerIdx, {
    baseType: 'aupay_search', label: '検索分析 (流入KW)',
    extraCols: { kw: '流入キーワード名' },
    keyExtra: (r, e) => {
      const kw = trimS(r[e.kw]);
      return kw ? { keyword: kw.slice(0, 200) } : null;
    },
    metrics: [
      ['来訪回数', 'visits', intOrNull],
      ['訪問者(UU)', 'visitor_uu', intOrNull],
    ],
  });
}

function preparePage(name, rows, headerIdx) {
  return prepareSegmentDated(name, rows, headerIdx, {
    baseType: 'aupay_page', label: 'ページ分析', hasSegment: false,
    extraCols: { url: 'ページURL' },
    keyExtra: (r, e) => {
      const u = trimS(r[e.url]);
      return u ? { page_url: u.slice(0, 500) } : null;
    },
    metrics: [
      ['PV数', 'pageviews', intOrNull],
      ['購入回数', 'orders', intOrNull],
      ['経由購入回数(経由CV)', 'via_orders', intOrNull],
      ['直帰率', 'bounce_rate_pct', numOrNull],
      ['離脱率', 'exit_rate_pct', numOrNull],
    ],
  });
}

function prepareProduct(name, rows, headerIdx) {
  return prepareSegmentDated(name, rows, headerIdx, {
    baseType: 'aupay_product', label: '商品分析',
    extraCols: { lot: 'ロットナンバー', pname: '商品名', cat: '商品カテゴリ' },
    keyExtra: (r, e) => {
      const lot = trimS(r[e.lot]);
      if (!lot) return null;
      return {
        lot_number: lot,
        product_name: trimS(r[e.pname]).slice(0, 300) || null,
        category: trimS(r[e.cat]).slice(0, 120) || null,
      };
    },
    metrics: [
      ['売上高', 'sales_yen', yenOrNull],
      ['購入回数', 'orders', intOrNull],
      ['購入個数', 'units', intOrNull],
      ['平均購入個数', 'avg_units', numOrNull],
      ['平均購入単価', 'avg_price_yen', yenOrNull],
      ['利用店舗原資クーポン', 'coupon_store_yen', yenOrNull],
      ['利用モール原資クーポン', 'coupon_mall_yen', yenOrNull],
      ['購入UU数', 'buyer_uu', intOrNull],
      ['来訪回数', 'visits', intOrNull],
      ['来訪者数(UU)', 'visitor_uu', intOrNull],
      ['PV数', 'pageviews', intOrNull],
      ['CVR(訪問回数ベース)', 'cvr_visit_pct', numOrNull],
      ['CVR(UUベース)', 'cvr_uu_pct', numOrNull],
    ],
  });
}

/**
 * リピート分析 (rf、月次専用コホート行列):
 *   「YYYY/MM起点」マーカー行 → ヘッダ行 (当月顧客セグメント名 + 対象月列... + リピート受注数合計)
 *   → セグメント行、を起点月ごとに繰り返す (月次レンジDLで複数ブロック)。
 *   「リピート受注数合計」は格納せず検算に使う (合計不一致=パース欠落の検知)。
 */
function prepareRepeatCohort(name, rows) {
  // 前文の抽出年月 (scope置換範囲=起点月レンジ)。最初のマーカー行より前を走査
  let preamble = null;
  for (let pi = 0; pi < rows.length; pi++) {
    if (/^\d{4}\/\d{1,2}起点$/.test(trimS(rows[pi][0]))) break;
    const m = trimS(rows[pi][0]).match(/^抽出年月[:：]\s*(.+?)\s*[～〜]\s*(.+)$/);
    if (m) {
      const a = normalizeMonth(m[1]); const b = normalizeMonth(m[2]);
      if (a && b) preamble = { from: `${a}-01`, to: `${b}-01` };
    }
  }
  const records = [];
  const warnings = [];
  const warned = new Set();
  let i = 0;
  let blocks = 0;
  while (i < rows.length) {
    const cell = trimS(rows[i][0]);
    const marker = cell.match(/^(\d{4})\/(\d{1,2})起点$/);
    if (!marker) { i++; continue; }
    const baseYm = normalizeMonth(`${marker[1]}/${marker[2]}`);
    if (!baseYm) return { name, ok: false, type: 'aupay_repeat_cohort_monthly', error: `起点月を解釈できない: 「${cell}」` };
    const baseDate = `${baseYm}-01`;
    // 次の非空行 = ヘッダ
    let h = i + 1;
    while (h < rows.length && rows[h].every(v => trimS(v) === '')) h++;
    const header = (rows[h] || []).map(trimS);
    const segIdx = header.indexOf('当月顧客セグメント名');
    const totalIdx = header.indexOf('リピート受注数合計');
    if (segIdx < 0 || totalIdx < 0) {
      return { name, ok: false, type: 'aupay_repeat_cohort_monthly', error: `起点 ${baseYm} のヘッダ行が想定形式でない (当月顧客セグメント名/リピート受注数合計が見つからない)` };
    }
    const monthCols = [];
    for (let ci = 0; ci < header.length; ci++) {
      if (ci === segIdx || ci === totalIdx) continue;
      const ym = /^\d{4}\/\d{1,2}$/.test(header[ci]) ? normalizeMonth(header[ci]) : null;
      if (ym) monthCols.push({ ci, target: `${ym}-01` });
    }
    if (monthCols.length === 0) {
      return { name, ok: false, type: 'aupay_repeat_cohort_monthly', error: `起点 ${baseYm} に対象月列がない (ヘッダ形式変更の疑い)` };
    }
    // データ行 (次のマーカー or 空行連続まで)
    let j = h + 1;
    for (; j < rows.length; j++) {
      const r = rows[j];
      const first = trimS(r[0]);
      if (/^\d{4}\/\d{1,2}起点$/.test(first)) break;
      if (r.every(v => trimS(v) === '')) continue;
      if (!first) continue;
      const g = rowLenGuard(name, 'aupay_repeat_cohort_monthly', r, header, j + 1); if (g) return g;
      const { code, warning } = segmentCode(first);
      if (warning && !warned.has(code)) { warnings.push(warning); warned.add(code); }
      let sum = 0;
      for (const { ci, target } of monthCols) {
        const v = intOrNull(r[ci]);
        sum += v ?? 0;
        records.push({ date_jst: baseDate, segment_code: code, segment_raw: first, target_month_jst: target, orders: v });
      }
      const total = intOrNull(r[totalIdx]);
      if (total !== null && total !== sum) {
        return { name, ok: false, type: 'aupay_repeat_cohort_monthly', error: `起点 ${baseYm} セグメント「${first}」の合計不一致 (明細計 ${sum} ≠ 合計列 ${total})。パース欠落の疑い` };
      }
    }
    blocks++;
    i = j;
  }
  if (blocks === 0) return { name, ok: false, type: 'aupay_repeat_cohort_monthly', error: '「YYYY/MM起点」ブロックが見つからない' };
  if (records.length === 0) return { name, ok: false, type: 'aupay_repeat_cohort_monthly', error: 'データ行が0件' };
  const d = dupGuard(name, 'aupay_repeat_cohort_monthly', records, (r) => `${r.date_jst}|${r.segment_code}|${r.target_month_jst}`, '起点×セグメント×対象月'); if (d) return d;
  const dates = [...new Set(records.map(r => r.date_jst))].sort();
  if (preamble) {
    const out = dates.filter(dd => dd < preamble.from || dd > preamble.to);
    if (out.length > 0) {
      return { name, ok: false, type: 'aupay_repeat_cohort_monthly', error: `抽出範囲 ${preamble.from}〜${preamble.to} 外の起点月: ${out.slice(0, 3).join(', ')}` };
    }
  }
  return { name, ok: true, type: 'aupay_repeat_cohort_monthly', label: `リピート分析 (コホート、起点${blocks}ヶ月)`, records, warnings, dateFrom: dates[0], dateTo: dates[dates.length - 1],
    scopeFrom: preamble ? preamble.from : dates[0], scopeTo: preamble ? preamble.to : dates[dates.length - 1] };
}

/** PM日次レポート (UTF-8 BOM、GET ?format=csv&aggregate=daily)。日付=YYYY-MM-DD 固定 */
function preparePmAdDaily(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const c = {
    date: colIndex(header, '日付'), imp: colIndex(header, '広告表示回数'), clicks: colIndex(header, 'クリック回数'),
    ctr: colIndex(header, 'CTR'), cpc: colIndex(header, 'CPC'), gmv: colIndex(header, '広告経由流通額'),
    roas: colIndex(header, 'ROAS'), cost: colIndex(header, '広告費'),
  };
  const records = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const raw = trimS(r[c.date]);
    if (/\d\s*-\s*\d{4}/.test(raw) || / - /.test(raw)) {
      return { name, ok: false, type: 'aupay_pm_ad_daily', error: `期間行 (${raw}) が混在 — aggregate=daily 以外のCSVは取込対象外` };
    }
    const date = normalizeDate(raw);
    if (!date) return { name, ok: false, type: 'aupay_pm_ad_daily', error: `日付を解釈できない行 (row ${i + 1}): 「${raw}」` };
    const g = rowLenGuard(name, 'aupay_pm_ad_daily', r, header, i + 1); if (g) return g;
    records.push({
      date_jst: date,
      impressions: intOrNull(r[c.imp]), clicks: intOrNull(r[c.clicks]),
      ctr_pct: numOrNull(r[c.ctr]), cpc_yen: numOrNull(r[c.cpc]),
      gmv_via_ad_yen: yenOrNull(r[c.gmv]), roas: numOrNull(r[c.roas]), cost_yen: yenOrNull(r[c.cost]),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'aupay_pm_ad_daily', error: 'データ行が0件' };
  const d = dupGuard(name, 'aupay_pm_ad_daily', records, (r) => r.date_jst, '日付'); if (d) return d;
  const dates = records.map(r => r.date_jst).sort();
  // PMレポートは前文なし+全日行が返る (0埋め) → scope=実データのmin/max
  return { name, ok: true, type: 'aupay_pm_ad_daily', label: 'PM広告 日次レポート', records, dateFrom: dates[0], dateTo: dates[dates.length - 1],
    scopeFrom: dates[0], scopeTo: dates[dates.length - 1] };
}

/** PM検索クエリ (fetcher が画面テーブルをスクレイプしたJSON):
 *  { kind:'aupay_pm_query_weekly', week_start:'YYYY-MM-DD', rows:[{rank,keyword,impressions,clicks,ctr_pct}] } */
function preparePmQueryJson(name, json) {
  const weekStart = normalizeDate(trimS(json?.week_start));
  if (!weekStart) return { name, ok: false, type: 'aupay_pm_query_weekly', error: 'week_start が読めません (fetcher出力形式の変更疑い)' };
  const fileDate = dateFromFileName(name);
  if (fileDate && fileDate !== weekStart) {
    return { name, ok: false, type: 'aupay_pm_query_weekly', error: `ファイル名の日付 (${fileDate}) と week_start (${weekStart}) が不一致` };
  }
  const rows = Array.isArray(json.rows) ? json.rows : null;
  if (!rows) return { name, ok: false, type: 'aupay_pm_query_weekly', error: 'rows 配列がありません' };
  const records = [];
  for (const r of rows) {
    const kw = trimS(r.keyword);
    if (!kw) continue;
    records.push({
      date_jst: weekStart, keyword: kw.slice(0, 200),
      rank: intOrNull(r.rank), impressions: intOrNull(r.impressions),
      clicks: intOrNull(r.clicks), ctr_pct: numOrNull(r.ctr_pct),
    });
  }
  if (records.length === 0) return { name, ok: false, type: 'aupay_pm_query_weekly', error: 'データ行が0件' };
  const d = dupGuard(name, 'aupay_pm_query_weekly', records, (r) => r.keyword, 'キーワード'); if (d) return d;
  return { name, ok: true, type: 'aupay_pm_query_weekly', label: `PM検索クエリ (週次、${records.length}語)`, records, dateFrom: weekStart, dateTo: weekStart,
    scopeFrom: weekStart, scopeTo: weekStart };
}

// ─── 種別判別 + パース ───
export function prepareAupayFile(name, buffer) {
  const head = buffer.slice(0, 8).toString('utf8').trimStart();
  if (head.startsWith('{')) {
    let json;
    try { json = JSON.parse(buffer.toString('utf8')); } catch (e) {
      return { name, ok: false, type: 'unknown', error: `JSONパース失敗: ${e.message}` };
    }
    if (json?.kind === 'aupay_pm_query_weekly') return preparePmQueryJson(name, json);
    return { name, ok: false, type: 'unknown', error: `JSONのkindを判別できません (${trimS(json?.kind) || 'kindなし'})` };
  }

  const rows = parseCsvBuffer(buffer);
  if (rows.length === 0) return { name, ok: false, type: 'unknown', error: '空ファイル' };
  // 「抽出条件を満たす〜ありません」等のエラーCSV (シェア分析 PME1303 実測) は明示エラー
  if (/^[A-Z]{3}\d{4}[:：]/.test(trimS(rows[0][0]))) {
    return { name, ok: false, type: 'unknown', error: `WOW!マネージャーのエラーCSV: ${trimS(rows[0][0]).slice(0, 120)}` };
  }
  let idx;
  if ((idx = findHeaderRow(rows, ['顧客セグメント名', '年月日', 'ロットナンバー', '商品カテゴリ'])) >= 0) return prepareProduct(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['顧客セグメント名', '年月日', '参照元チャネル'])) >= 0) return prepareReferer(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['顧客セグメント名', '年月日', '流入キーワード名'])) >= 0) return prepareSearch(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['年月日', 'ページURL', 'PV数', '直帰率'])) >= 0) return preparePage(name, rows, idx);
  if ((idx = findHeaderRow(rows, ['顧客セグメント名', '年月日', '売上高', '利用ポイント'])) >= 0) return prepareSales(name, rows, idx);
  if (rows.some(r => /^\d{4}\/\d{1,2}起点$/.test(trimS(r[0])))) return prepareRepeatCohort(name, rows);
  if ((idx = findHeaderRow(rows, ['日付', '広告表示回数', 'クリック回数', '広告費'])) >= 0) return preparePmAdDaily(name, rows, idx);
  return { name, ok: false, type: 'unknown', error: '種類を判別できません (au PAY分析CSV 8種のいずれでもない)' };
}

// ─── UPSERT specs ───
const SEG_COLS = ['segment_code', 'segment_raw'];
const SALES_COLS = ['date_jst', ...SEG_COLS, 'sales_yen', 'coupon_store_yen', 'coupon_mall_yen',
  'point_ponta_au_yen', 'point_used_yen', 'orders', 'units', 'visits', 'buyer_uu', 'visitor_uu', 'pageviews',
  'cvr_pct', 'avg_units', 'avg_price_yen', 'avg_visits', 'avg_pv',
  'coupon_store_rate_pct', 'coupon_mall_rate_pct', 'point_rate_pct'];
const REFERER_COLS = ['date_jst', ...SEG_COLS, 'channel', 'visits', 'visitor_uu', 'sales_yen', 'orders', 'units',
  'cvr_pct', 'avg_units', 'avg_price_yen'];
const SEARCH_COLS = ['date_jst', ...SEG_COLS, 'keyword', 'visits', 'visitor_uu'];
const PAGE_COLS = ['date_jst', 'page_url', 'pageviews', 'orders', 'via_orders', 'bounce_rate_pct', 'exit_rate_pct'];
const PRODUCT_COLS = ['date_jst', ...SEG_COLS, 'lot_number', 'product_name', 'category',
  'sales_yen', 'orders', 'units', 'avg_units', 'avg_price_yen', 'coupon_store_yen', 'coupon_mall_yen',
  'buyer_uu', 'visits', 'visitor_uu', 'pageviews', 'cvr_visit_pct', 'cvr_uu_pct'];

const A_UPSERT_SPECS = {};
for (const grain of ['daily', 'monthly']) {
  A_UPSERT_SPECS[`aupay_sales_${grain}`] = { table: `fact_aupay_sales_${grain}`, pk: ['date_jst', 'segment_code'], cols: SALES_COLS };
  A_UPSERT_SPECS[`aupay_referer_${grain}`] = { table: `fact_aupay_referer_${grain}`, pk: ['date_jst', 'segment_code', 'channel'], cols: REFERER_COLS };
  A_UPSERT_SPECS[`aupay_search_${grain}`] = { table: `fact_aupay_search_${grain}`, pk: ['date_jst', 'segment_code', 'keyword'], cols: SEARCH_COLS };
  A_UPSERT_SPECS[`aupay_page_${grain}`] = { table: `fact_aupay_page_${grain}`, pk: ['date_jst', 'page_url'], cols: PAGE_COLS };
  A_UPSERT_SPECS[`aupay_product_${grain}`] = { table: `fact_aupay_product_${grain}`, pk: ['date_jst', 'segment_code', 'lot_number'], cols: PRODUCT_COLS };
}
A_UPSERT_SPECS.aupay_repeat_cohort_monthly = {
  table: 'fact_aupay_repeat_cohort_monthly', pk: ['date_jst', 'segment_code', 'target_month_jst'],
  cols: ['date_jst', ...SEG_COLS, 'target_month_jst', 'orders'],
};
A_UPSERT_SPECS.aupay_pm_ad_daily = {
  table: 'fact_aupay_pm_ad_daily', pk: ['date_jst'],
  cols: ['date_jst', 'impressions', 'clicks', 'ctr_pct', 'cpc_yen', 'gmv_via_ad_yen', 'roas', 'cost_yen'],
};
A_UPSERT_SPECS.aupay_pm_query_weekly = {
  table: 'fact_aupay_pm_query_weekly', pk: ['date_jst', 'keyword'],
  cols: ['date_jst', 'keyword', 'rank', 'impressions', 'clicks', 'ctr_pct'],
};
export const AUPAY_DATA_TYPES = new Set(Object.keys(A_UPSERT_SPECS));

function commitOne(db, logStmt, p, ctx) {
  const { importedAt, source, fileSha256 } = ctx;
  const spec = A_UPSERT_SPECS[p.type];
  // ⭐scope置換 (DELETE→INSERT): ファイルが表す抽出範囲内を丸ごと入れ替える。
  // UPSERTだけだと補正版で消えたディメンション行 (商品/KW/チャネル等) が永久残留する
  // (Codex 実装レビュー R1 High)。scopeFrom/To は前文の抽出範囲が正
  const warnings = [...(p.warnings || [])];
  const del = db.prepare(`DELETE FROM ${spec.table} WHERE date_jst >= ? AND date_jst <= ?`)
    .run(p.scopeFrom, p.scopeTo);
  const scopeMsg = `scope置換 ${p.scopeFrom}〜${p.scopeTo} (旧${del.changes}行削除)`;
  const logInfo = logStmt.run(importedAt, source, p.name, fileSha256, p.type, p.dateFrom, p.dateTo, p.records.length, 0, 0, 'ok',
    [scopeMsg, ...warnings].join(' / ').slice(0, 500));
  const importId = logInfo.lastInsertRowid;
  const meta = { source_file: p.name.slice(0, 200), import_id: importId, imported_at: importedAt, updated_at: importedAt };
  const insert = db.prepare(`INSERT INTO ${spec.table}
    (${spec.cols.join(', ')}, source_file, import_id, imported_at, updated_at)
    VALUES (${spec.cols.map(k => '@' + k).join(', ')}, @source_file, @import_id, @imported_at, @updated_at)`);
  for (const r of p.records) insert.run({ ...r, ...meta });
  const inserted = p.records.length;
  const replaced = del.changes;
  db.prepare(`UPDATE raw_aupay_data_import_log SET inserted=?, updated=? WHERE id=?`).run(inserted, replaced, importId);
  return {
    file: p.name, ok: true, type: p.type, label: p.label, warnings,
    date_from: p.dateFrom, date_to: p.dateTo, rows: p.records.length, inserted, updated: replaced, scope: scopeMsg,
  };
}

function getLogInsertStmt(db) {
  return db.prepare(`INSERT INTO raw_aupay_data_import_log
    (imported_at, source, file_name, file_sha256, file_type, date_from, date_to, row_count, inserted, updated, status, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
}

// ─── 入口: 物理ファイル1個 (zip or CSV/JSON) を1トランザクションで取込 ───
export function importAupayFile(db, { name, buffer, sha256, source = 'incoming' }) {
  const importedAt = new Date().toISOString();
  const logStmt = getLogInsertStmt(db);
  const logOutcome = (fileName, fileType, status, message) => {
    try {
      logStmt.run(importedAt, source, fileName, sha256, fileType, null, null, 0, 0, 0, status, String(message || '').slice(0, 500));
    } catch (e) { console.error(`[aupay-data-import] ログ記録失敗: ${e.message}`); }
  };

  // 冪等キー: 本文sha256 + 論理日付 (ファイル名 `_d` マーカー由来)。yahoo と同じ契約 —
  // 内容が同一の別日/別期間 (動きの無い期間等) を sha256 だけで弾かない
  const logicalDate = dateFromFileName(name);
  const dup = logicalDate
    ? db.prepare(`SELECT id FROM raw_aupay_data_import_log WHERE file_sha256 = ? AND status = 'ok' AND date_from = ? LIMIT 1`).get(sha256, logicalDate)
    : db.prepare(`SELECT id FROM raw_aupay_data_import_log WHERE file_sha256 = ? AND status = 'ok' LIMIT 1`).get(sha256);
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
      const p = prepareAupayFile(f.name, f.buffer);
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
    console.error(`[aupay-data-import] group ${g.name}: ${e.stack || e.message}`);
    const results = fileNames.map(fileName => {
      logOutcome(fileName, 'unknown', 'error', e.message);
      return { file: fileName, ok: false, type: 'unknown', error: e.message };
    });
    return { status: 'error', results };
  }
}
