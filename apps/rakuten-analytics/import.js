/**
 * rakuten-analytics import.js — 取込タブ (P2) のデータ層
 *
 * 楽天RMSからDLしたRPPパフォーマンスレポートCSV (zip対応) を取り込む。
 * 要件定義 F-7: ヘッダ自動判別 (RECIPES方式) + 同期間UPSERT上書き + 鮮度ボード。
 *
 * RMS実画面の仕様 (2026-07-06 中原さんスクリーンショットで確認):
 *   - 商品別/キーワード別レポートは集計期間「月ごとに表示」「全期間で表示」のみ
 *     → SKU別・キーワード別は【月次】粒度で取り込む (日別は楽天側に存在しない)
 *   - 「すべての広告」「キャンペーン」単位なら「日ごとに表示」が可能
 *     → 店舗全体の【日次】広告費は別レシピ (rpp_daily) で取り込む
 *   - 期間は3ヶ月以内に絞る必要あり / 集計結果は昨日まで / 過去分は変動する可能性
 *     (不正クリック控除) → 再アップロードで PK 単位 UPSERT 上書き (冪等)
 *   - 出力項目は 顧客種類(合計/新規/既存) × CV種類(12時間/720時間) × 効果項目 のマトリクス
 *
 * 設計メモ:
 *   - 正確なCSVヘッダ名はDLしないと確認できないため、列名は正規化+パターンマッチで解決。
 *     判別できないファイルはヘッダ一覧を返して「未対応形式」として報告 (→ レシピ追記で対応)
 *   - テーブル名は楽天Phase1a設計書 §9 予約済みの fact_rakuten_ads_rpp / _rpp_keyword
 */
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { jstToday, monthOf, addMonths } from './queries.js';

// ─── テーブル (アプリ管理。CREATE IF NOT EXISTS で初回作成) ───
let _initialized = false;
export function ensureImportTables() {
  if (_initialized) return;
  const db = getMirrorDB();
  // 商品別 (月次 — RMSの仕様上、商品別レポートは月ごと表示のみ)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_ads_rpp (
    month_ym            TEXT NOT NULL CHECK(month_ym GLOB '????-??'),
    item_manage_number  TEXT NOT NULL CHECK(trim(item_manage_number) <> ''),
    campaign_id         TEXT NOT NULL DEFAULT '',
    campaign_name       TEXT NOT NULL DEFAULT '',
    item_name           TEXT NOT NULL DEFAULT '',
    clicks              INTEGER NOT NULL DEFAULT 0,
    impressions         INTEGER,
    ad_cost             REAL NOT NULL DEFAULT 0,
    cpc_actual          REAL,
    ctr_pct             REAL,
    sales_720h          REAL NOT NULL DEFAULT 0,
    orders_720h         INTEGER NOT NULL DEFAULT 0,
    cvr_720h_pct        REAL,
    roas_720h_pct       REAL,
    sales_12h           REAL,
    orders_12h          INTEGER,
    sales_720h_new      REAL,
    sales_720h_repeat   REAL,
    source_file         TEXT NOT NULL DEFAULT '',
    import_id           INTEGER NOT NULL,
    imported_at         TEXT NOT NULL,
    PRIMARY KEY (month_ym, item_manage_number, campaign_id)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frar_item ON fact_rakuten_ads_rpp(item_manage_number, month_ym)`);

  // キーワード別 (月次)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_ads_rpp_keyword (
    month_ym            TEXT NOT NULL CHECK(month_ym GLOB '????-??'),
    item_manage_number  TEXT NOT NULL CHECK(trim(item_manage_number) <> ''),
    keyword             TEXT NOT NULL CHECK(trim(keyword) <> ''),
    campaign_id         TEXT NOT NULL DEFAULT '',
    keyword_cpc         REAL,
    clicks              INTEGER NOT NULL DEFAULT 0,
    impressions         INTEGER,
    ad_cost             REAL NOT NULL DEFAULT 0,
    cpc_actual          REAL,
    ctr_pct             REAL,
    sales_720h          REAL NOT NULL DEFAULT 0,
    orders_720h         INTEGER NOT NULL DEFAULT 0,
    cvr_720h_pct        REAL,
    roas_720h_pct       REAL,
    source_file         TEXT NOT NULL DEFAULT '',
    import_id           INTEGER NOT NULL,
    imported_at         TEXT NOT NULL,
    PRIMARY KEY (month_ym, item_manage_number, keyword, campaign_id)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frark_kw ON fact_rakuten_ads_rpp_keyword(keyword, month_ym)`);

  // 店舗全体/キャンペーン別 (日次 — 集計単位「すべての広告」or「キャンペーン」×「日ごとに表示」)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_ads_rpp_daily (
    date_jst            TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    campaign_id         TEXT NOT NULL DEFAULT '',
    campaign_name       TEXT NOT NULL DEFAULT '',
    clicks              INTEGER NOT NULL DEFAULT 0,
    impressions         INTEGER,
    ad_cost             REAL NOT NULL DEFAULT 0,
    ad_cost_discounted  REAL,
    cpc_actual          REAL,
    ctr_pct             REAL,
    sales_720h          REAL NOT NULL DEFAULT 0,
    orders_720h         INTEGER NOT NULL DEFAULT 0,
    cvr_720h_pct        REAL,
    roas_720h_pct       REAL,
    sales_12h           REAL,
    orders_12h          INTEGER,
    sales_720h_new      REAL,
    sales_720h_repeat   REAL,
    source_file         TEXT NOT NULL DEFAULT '',
    import_id           INTEGER NOT NULL,
    imported_at         TEXT NOT NULL,
    PRIMARY KEY (date_jst, campaign_id)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS radash_import_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at  TEXT NOT NULL,
    uploaded_by  TEXT NOT NULL DEFAULT '',
    file_name    TEXT NOT NULL,
    file_type    TEXT NOT NULL,
    date_from    TEXT,
    date_to      TEXT,
    row_count    INTEGER NOT NULL DEFAULT 0,
    inserted     INTEGER NOT NULL DEFAULT 0,
    updated      INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL CHECK (status IN ('ok','error','skipped')),
    message      TEXT NOT NULL DEFAULT ''
  )`);
  _initialized = true;
}

// ─── CSV decode + parse (purchase-orders の実装パターンを踏襲) ───
export function decodeCsvBuffer(buf) {
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    return buf.slice(3).toString('utf8');
  }
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(buf);
  } catch {
    return iconv.decode(buf, 'Shift_JIS');
  }
}
export function parseCsvBuffer(buf) {
  const text = decodeCsvBuffer(buf);
  const rows = [];
  let row = [], field = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQ = false;
      } else field += c;
    } else if (c === '"') inQ = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      row.push(field); field = '';
      if (row.some(v => v !== '')) rows.push(row);
      row = [];
    } else field += c;
  }
  row.push(field);
  if (row.some(v => v !== '')) rows.push(row);
  return rows;
}

// ─── 値の正規化 ───
const trimS = v => String(v == null ? '' : v).trim();
// "1,234", "12.3%", "¥500", "500円", "-" を数値/nullへ
export function numOrNull(v) {
  const s = trimS(v).replace(/[,¥円%％\s]/g, '');
  if (s === '' || s === '-' || s === '−') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}
const num0 = v => numOrNull(v) ?? 0;

// 日付を YYYY-MM-DD に正規化 (2026/7/6, 2026-07-06, 20260706 対応)。
// ゼロ埋めは必ず padStart (slice(0,7) 罠 — feedback_year_month_slice_padding_trap)
export function normalizeDate(v) {
  const s = trimS(v);
  let m = s.match(/^(\d{4})[/\-年](\d{1,2})[/\-月](\d{1,2})日?$/);
  if (!m) m = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!m) return null;
  const [, y, mo, d] = m;
  const iso = `${y}-${String(Number(mo)).padStart(2, '0')}-${String(Number(d)).padStart(2, '0')}`;
  const dt = new Date(iso + 'T00:00:00Z');
  if (isNaN(dt.getTime()) || dt.toISOString().slice(0, 10) !== iso) return null;
  return iso;
}

// 年月を YYYY-MM に正規化 (2026/07, 2026-7, 2026年7月, 202607, フル日付も月に丸め)
export function normalizeMonth(v) {
  const s = trimS(v);
  let m = s.match(/^(\d{4})[/\-年](\d{1,2})月?$/);
  if (!m) m = s.match(/^(\d{4})(\d{2})$/);
  if (m) {
    const [, y, mo] = m;
    const n = Number(mo);
    if (n < 1 || n > 12) return null;
    return `${y}-${String(n).padStart(2, '0')}`;
  }
  const d = normalizeDate(s);
  return d ? d.slice(0, 7) : null;
}

// RPP商品別/KW別レポートの「日付」列は期間文字列 (実データ確認 2026-07-06):
//   「2026年07月01日～2026年07月05日」(範囲) / 「2026年07月」等。
// 範囲が同一月内なら月に丸め、複数月にまたがる場合は spans (=「月ごとに表示」でDLし直し案内)
export function parseMonthPeriod(v) {
  const s = trimS(v);
  if (!s) return { ym: null };
  const parts = s.split(/[～〜~]/).map(x => x.trim()).filter(Boolean);
  if (parts.length === 2) {
    const d1 = normalizeDate(parts[0]) || (normalizeMonth(parts[0]) ? normalizeMonth(parts[0]) + '-01' : null);
    const d2 = normalizeDate(parts[1]) || (normalizeMonth(parts[1]) ? normalizeMonth(parts[1]) + '-01' : null);
    if (!d1 || !d2) return { ym: null };
    if (d1.slice(0, 7) !== d2.slice(0, 7)) return { ym: null, spans: true };
    return { ym: d1.slice(0, 7) };
  }
  return { ym: normalizeMonth(s) };
}

// ─── ヘッダ列解決 (正規化 + include/exclude パターン) ───
function normHeader(h) {
  return trimS(h)
    .replace(/[（]/g, '(').replace(/[）]/g, ')')
    .replace(/％/g, '%')
    .replace(/[\s　]/g, '')
    .toLowerCase();
}
// includes: 全部含む / excludes: どれかを含んだら不採用。最初にマッチした列 index を返す
function pickCol(headers, includes, excludes = []) {
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    if (includes.every(p => h.includes(p)) && !excludes.some(p => h.includes(p))) return i;
  }
  return -1;
}

// CV系 (売上金額/件数/CVR/ROAS) の窓・顧客区分。「合計」列を最優先、無ければ区分なし列
function pickMetric(headers, base, window, opts = {}) {
  const ex = ['新規', '既存', ...(opts.excludes || [])];
  let i = window ? pickCol(headers, [base, window, '合計'], opts.excludes || []) : -1;
  if (i >= 0) return i;
  if (window) {
    i = pickCol(headers, [base, window], ex);
    if (i >= 0) return i;
  }
  // 「合計」付き・窓なし → 窓なし
  i = pickCol(headers, [base, '合計'], ['12時間', '720時間', ...(opts.excludes || [])]);
  if (i >= 0) return i;
  i = pickCol(headers, [base], ['12時間', '720時間', ...ex]);
  return i;
}

// 期間列 (年月 or 日付)。product/keyword は月次、daily は日次
function pickPeriodCol(headers) {
  let i = pickCol(headers, ['年月']);
  if (i >= 0) return i;
  i = pickCol(headers, ['集計期間']);
  if (i >= 0) return i;
  i = pickCol(headers, ['日付']);
  if (i >= 0) return i;
  return pickCol(headers, ['日'], ['曜日']);
}

// 共通のメトリクス列解決
function metricCols(headers) {
  return {
    clicks: pickMetric(headers, 'クリック数', ''),
    impressions: pickCol(headers, ['表示回数']) >= 0 ? pickCol(headers, ['表示回数']) : pickCol(headers, ['インプレッション']),
    adCost: pickMetric(headers, '実績額', ''),
    cpcActual: pickCol(headers, ['cpc実績']) >= 0 ? pickCol(headers, ['cpc実績']) : pickCol(headers, ['cpc'], ['キーワード', '目安', '商品']),
    ctr: pickCol(headers, ['ctr']),
    sales720: pickMetric(headers, '売上金額', '720時間'),
    orders720: pickMetric(headers, '売上件数', '720時間'),
    cvr720: pickMetric(headers, 'cvr', '720時間'),
    roas720: pickMetric(headers, 'roas', '720時間', { excludes: ['値引'] }),
    sales12: pickMetric(headers, '売上金額', '12時間'),
    orders12: pickMetric(headers, '売上件数', '12時間'),
    sales720New: pickCol(headers, ['売上金額', '720時間', '新規']),
    sales720Repeat: pickCol(headers, ['売上金額', '720時間', '既存']),
  };
}
function metricRecord(cells, col) {
  return {
    clicks: col.clicks >= 0 ? Math.round(num0(cells[col.clicks])) : 0,
    impressions: col.impressions >= 0 ? numOrNull(cells[col.impressions]) : null,
    ad_cost: col.adCost >= 0 ? num0(cells[col.adCost]) : 0,
    cpc_actual: col.cpcActual >= 0 ? numOrNull(cells[col.cpcActual]) : null,
    ctr_pct: col.ctr >= 0 ? numOrNull(cells[col.ctr]) : null,
    sales_720h: col.sales720 >= 0 ? num0(cells[col.sales720]) : 0,
    orders_720h: col.orders720 >= 0 ? Math.round(num0(cells[col.orders720])) : 0,
    cvr_720h_pct: col.cvr720 >= 0 ? numOrNull(cells[col.cvr720]) : null,
    roas_720h_pct: col.roas720 >= 0 ? numOrNull(cells[col.roas720]) : null,
    sales_12h: col.sales12 >= 0 ? numOrNull(cells[col.sales12]) : null,
    orders_12h: col.orders12 >= 0 ? numOrNull(cells[col.orders12]) : null,
    sales_720h_new: col.sales720New >= 0 ? numOrNull(cells[col.sales720New]) : null,
    sales_720h_repeat: col.sales720Repeat >= 0 ? numOrNull(cells[col.sales720Repeat]) : null,
  };
}

// ─── RECIPES: ヘッダからファイル種別を判別し、行→レコード変換を提供 ───
// period: 'month' = 商品別/KW別 (RMS仕様で月次のみ) / 'day' = すべての広告・キャンペーン日次
export const IMPORT_RECIPES = [
  {
    type: 'rpp_keyword',
    label: 'RPP 全キーワードレポート (月次)',
    period: 'month',
    detect: (headers) => pickCol(headers, ['キーワード'], ['cpc', '入札']) >= 0
      && (pickCol(headers, ['実績額']) >= 0 || pickCol(headers, ['クリック']) >= 0),
    columns: (headers) => ({
      period: pickPeriodCol(headers),
      keyword: pickCol(headers, ['キーワード'], ['cpc', '入札']),
      item: pickCol(headers, ['商品管理番号']),
      campaignId: pickCol(headers, ['キャンペーンid']),
      keywordCpc: pickCol(headers, ['キーワード', 'cpc']),
      ...metricCols(headers),
    }),
    // item は PK 構成要素 (欠けると複数商品が同一PKに潰れる)。根幹メトリクスは0値で静かに取り込まない
    required: ['period', 'keyword', 'item', 'clicks', 'adCost', 'sales720', 'orders720'],
    toRecord: (cells, col) => {
      const p = parseMonthPeriod(cells[col.period]);
      return {
        period_raw: trimS(cells[col.period]),
        month_ym: p.ym,
        period_spans: !!p.spans,
        item_manage_number: trimS(cells[col.item]),
        keyword: trimS(cells[col.keyword]),
        campaign_id: col.campaignId >= 0 ? trimS(cells[col.campaignId]) : '',
        keyword_cpc: col.keywordCpc >= 0 ? numOrNull(cells[col.keywordCpc]) : null,
        ...metricRecord(cells, col),
      };
    },
    validRecord: (r) => r.month_ym && !r.period_spans && r.keyword !== '' && r.item_manage_number !== '',
    invalidReason: (r) => r.period_spans ? '期間が複数月にまたがっています。集計期間「月ごとに表示」でDLし直してください'
      : !r.month_ym ? '年月が不正' : r.keyword === '' ? 'キーワードが空' : '商品管理番号が空',
    pk: (r) => `${r.month_ym}\x1f${r.item_manage_number}\x1f${r.keyword}\x1f${r.campaign_id}`,
  },
  {
    type: 'rpp_product',
    label: 'RPP 全商品レポート (月次)',
    period: 'month',
    // キーワード列が無く、商品管理番号 + (実績額 or クリック) がある
    detect: (headers) => pickCol(headers, ['キーワード'], ['cpc', '入札']) < 0
      && pickCol(headers, ['商品管理番号']) >= 0
      && (pickCol(headers, ['実績額']) >= 0 || pickCol(headers, ['クリック']) >= 0),
    columns: (headers) => ({
      period: pickPeriodCol(headers),
      item: pickCol(headers, ['商品管理番号']),
      itemName: pickCol(headers, ['商品名']),
      campaignId: pickCol(headers, ['キャンペーンid']),
      campaignName: pickCol(headers, ['キャンペーン名']),
      ...metricCols(headers),
    }),
    required: ['period', 'item', 'clicks', 'adCost', 'sales720', 'orders720'],
    toRecord: (cells, col) => {
      const p = parseMonthPeriod(cells[col.period]);
      return {
        period_raw: trimS(cells[col.period]),
        month_ym: p.ym,
        period_spans: !!p.spans,
        item_manage_number: trimS(cells[col.item]),
        campaign_id: col.campaignId >= 0 ? trimS(cells[col.campaignId]) : '',
        campaign_name: col.campaignName >= 0 ? trimS(cells[col.campaignName]) : '',
        item_name: col.itemName >= 0 ? trimS(cells[col.itemName]) : '',
        ...metricRecord(cells, col),
      };
    },
    validRecord: (r) => r.month_ym && !r.period_spans && r.item_manage_number !== '',
    invalidReason: (r) => r.period_spans ? '期間が複数月にまたがっています。集計期間「月ごとに表示」でDLし直してください'
      : !r.month_ym ? '年月が不正' : '商品管理番号が空',
    pk: (r) => `${r.month_ym}\x1f${r.item_manage_number}\x1f${r.campaign_id}`,
  },
  {
    type: 'rpp_daily',
    label: 'RPP 日次広告費 (すべての広告/キャンペーン)',
    period: 'day',
    // 日付があり、商品管理番号・キーワードが無い (集計単位「すべての広告」or「キャンペーン」の日別DL)
    detect: (headers) => (pickCol(headers, ['日付']) >= 0 || pickCol(headers, ['日'], ['曜日']) >= 0)
      && pickCol(headers, ['商品管理番号']) < 0
      && pickCol(headers, ['キーワード'], ['cpc', '入札']) < 0
      && (pickCol(headers, ['実績額']) >= 0 || pickCol(headers, ['クリック']) >= 0),
    columns: (headers) => ({
      period: pickCol(headers, ['日付']) >= 0 ? pickCol(headers, ['日付']) : pickCol(headers, ['日'], ['曜日']),
      campaignId: pickCol(headers, ['キャンペーンid']),
      campaignName: pickCol(headers, ['キャンペーン名']),
      adCostDiscounted: pickCol(headers, ['割引後実績額']),
      ...metricCols(headers),
    }),
    required: ['period', 'clicks', 'adCost', 'sales720', 'orders720'],
    toRecord: (cells, col) => ({
      period_raw: trimS(cells[col.period]),
      date_jst: normalizeDate(cells[col.period]),
      campaign_id: col.campaignId >= 0 ? trimS(cells[col.campaignId]) : '',
      campaign_name: col.campaignName >= 0 ? trimS(cells[col.campaignName]) : '',
      ad_cost_discounted: col.adCostDiscounted >= 0 ? numOrNull(cells[col.adCostDiscounted]) : null,
      ...metricRecord(cells, col),
    }),
    validRecord: (r) => !!r.date_jst,
    invalidReason: () => '日付が不正',
    pk: (r) => `${r.date_jst}\x1f${r.campaign_id}`,
  },
];

// ─── zip 展開 (zip爆弾ガード付き) ───
// 方針: ①展開前に全エントリを宣言値で検証し、1つでも違反したらその zip 全体を reject
// ②展開後の実測サイズが宣言を大きく超えたら zip 全体を破棄 (宣言詐称)。
// adm-zip は stream 展開できないため、上限値を小さめに取りメモリスパイクを抑える
const ZIP_MAX_ENTRIES = 100;
const ZIP_MAX_ENTRY_BYTES = 30 * 1024 * 1024;   // 1エントリ (宣言/実測) 30MB
const ZIP_MAX_TOTAL_BYTES = 60 * 1024 * 1024;   // 1 zip の展開後合計 (宣言) 60MB
const ZIP_MAX_RATIO = 300;                       // 圧縮率ガード (宣言/圧縮後 > 300倍 は爆弾扱い)
// 戻り値は「グループ」単位: zip 1個 = 1グループ (原子的に取込)、単体CSV = 1ファイルグループ。
// { name, files: [{name, buffer}] } または { name, error }
export function expandFiles(uploads) {
  const out = [];
  for (const f of uploads) {
    const isZip = /\.zip$/i.test(f.originalname)
      || (f.buffer.length >= 4 && f.buffer[0] === 0x50 && f.buffer[1] === 0x4B && f.buffer[2] === 0x03 && f.buffer[3] === 0x04);
    if (!isZip) { out.push({ name: f.originalname, files: [{ name: f.originalname, buffer: f.buffer }] }); continue; }

    let zip, entries;
    try {
      zip = new AdmZip(f.buffer);
      entries = zip.getEntries().filter(en => !en.isDirectory && /\.csv$/i.test(en.entryName));
    } catch (e) {
      out.push({ name: f.originalname, error: `zipの展開に失敗: ${e.message}` });
      continue;
    }

    // ① 事前検証 (宣言値ベース)。違反が1つでもあれば zip 全体を reject
    let verdict = null;
    if (entries.length === 0) verdict = 'zip内にCSVが見つかりません';
    else if (entries.length > ZIP_MAX_ENTRIES) verdict = `zip内のCSVが多すぎます (${entries.length} > ${ZIP_MAX_ENTRIES})`;
    else {
      let total = 0;
      for (const en of entries) {
        const declared = en.header.size;
        const compressed = en.header.compressedSize;
        total += declared;
        if (declared > ZIP_MAX_ENTRY_BYTES) { verdict = `${en.entryName}: CSVが大きすぎます (${Math.round(declared / 1048576)}MB > 30MB)`; break; }
        if (declared > 1048576 && compressed > 0 && declared / compressed > ZIP_MAX_RATIO) { verdict = `${en.entryName}: 圧縮率が異常です (zip爆弾の疑い)`; break; }
      }
      if (!verdict && total > ZIP_MAX_TOTAL_BYTES) verdict = `zip合計サイズ超過 (${Math.round(total / 1048576)}MB > 60MB)`;
    }
    if (verdict) { out.push({ name: f.originalname, error: verdict }); continue; }

    // ② 展開。宣言詐称 (実測が宣言を大きく超える) や壊れたエントリを検出したら zip 全体を破棄
    const extracted = [];
    let bombed = null;
    for (const en of entries) {
      let data;
      try { data = en.getData(); } catch (e) {
        bombed = `${en.entryName}: エントリの展開に失敗 (${e.message})`;
        break;
      }
      if (data.length > ZIP_MAX_ENTRY_BYTES || data.length > en.header.size + 1024) {
        bombed = `${en.entryName}: 展開後サイズが宣言と一致しません (zip爆弾の疑い)`;
        break;
      }
      extracted.push({ name: `${f.originalname}/${en.entryName}`, buffer: data });
    }
    if (bombed) { out.push({ name: f.originalname, error: bombed }); continue; }
    out.push({ name: f.originalname, files: extracted });
  }
  return out;
}

// ─── 検証フェーズ: 1ファイルを parse + レコード化 (DB 書き込みなし) ───
// 戻り値: { name, ok:false, type, error } または { name, ok:true, recipe, records, rawCount }
function prepareOne(name, buffer) {
  const fail = (type, message) => ({ name, ok: false, type, error: message });

  let rows;
  try { rows = parseCsvBuffer(buffer); } catch (e) { return fail('unknown', `読込失敗: ${e.message}`); }
  if (rows.length < 2) return fail('unknown', 'データ行がありません');

  // ヘッダ行の探索: RMSの実CSVは先頭に前置行がある (実データ確認 2026-07-06):
  //   L0「実行日時: …」/ L2「検索条件」/「集計単位: 商品別」/「■■■…削除してください■■■」など。
  // 先頭12行のうち「5列以上あり、いずれかのレシピに一致する行」をヘッダとする
  let headerIdx = -1, recipe = null, headersRaw = null, headers = null;
  for (let i = 0; i < Math.min(rows.length, 12); i++) {
    if (rows[i].length < 5) continue;
    const raw = rows[i].map(trimS);
    const norm = raw.map(normHeader);
    const rc = IMPORT_RECIPES.find(x => x.detect(norm));
    if (rc) { headerIdx = i; recipe = rc; headersRaw = raw; headers = norm; break; }
  }
  if (!recipe) {
    const preview = rows.slice(0, 3).map(r => r.slice(0, 6).join(' / ')).join(' | ');
    return fail('unknown', `種類を判別できません。先頭行: ${preview.slice(0, 300)}`);
  }
  rows = rows.slice(headerIdx);  // rows[0] = ヘッダ行に正規化
  const col = recipe.columns(headers);
  const missing = recipe.required.filter(k => col[k] < 0);
  if (missing.length) {
    const COL_LABEL = {
      period: '年月/日付', item: '商品管理番号', keyword: 'キーワード',
      clicks: 'クリック数', adCost: '実績額', sales720: '売上金額(720時間)', orders720: '売上件数(720時間)',
    };
    const names = missing.map(k => COL_LABEL[k] || k).join(' / ');
    const hint = missing.includes('period')
      ? `期間列 (年月/日付) が見つかりません。集計期間「月ごとに表示」でDLしてください。`
      : `必須列が見つかりません: ${names}。RMSのDL時に表示/出力項目「全てを選択」にしてください。`;
    return fail(recipe.type, `${hint} (見出し: ${headersRaw.slice(0, 12).join(' / ')})`);
  }

  // 行→レコード変換 + 検証 (1行でも不正なら取り込まない)
  const records = [];
  for (let i = 1; i < rows.length; i++) {
    const cells = rows[i];
    if (cells.length === 1 && trimS(cells[0]) === '') continue;
    const r = recipe.toRecord(cells, col);
    if (!recipe.validRecord(r)) {
      return fail(recipe.type, `${i + 1}行目: ${recipe.invalidReason(r)} (期間値: ${trimS(cells[col.period] ?? '')})`);
    }
    records.push(r);
  }
  if (records.length === 0) return fail(recipe.type, '有効なデータ行がありません');

  // 月次レシピに日別データが来ていないか検査: 同一PKに異なる期間文字列が複数あれば
  // 「全期間で表示」や日別集計の可能性 → 黙って後勝ち統合せずエラーにする
  if (recipe.period === 'month') {
    const periodsByPk = new Map();
    for (const r of records) {
      const k = recipe.pk(r);
      if (!periodsByPk.has(k)) periodsByPk.set(k, new Set());
      periodsByPk.get(k).add(r.period_raw);
    }
    for (const [, set] of periodsByPk) {
      if (set.size > 1) {
        return fail(recipe.type, `同じ商品×月に複数の期間値があります (${[...set].slice(0, 3).join(', ')})。集計期間「月ごとに表示」でDLし直してください。`);
      }
    }
  }

  // ファイル内 PK 重複は後勝ちで統合 (RMS 側の集計重複)
  const dedup = new Map();
  for (const r of records) dedup.set(recipe.pk(r), r);
  // 日次レシピで同一日付が複数行 = キャンペーン単位CSVにキャンペーンID列が無い可能性
  // (黙って後勝ち統合すると広告費が欠落する — Codex Medium)
  if (recipe.type === 'rpp_daily' && dedup.size < records.length) {
    return fail(recipe.type, `同一日付の行が複数あります (${records.length}行→${dedup.size}件)。キャンペーン単位のCSVはキャンペーンID列を含めてDLするか、集計単位「すべての広告」でDLしてください。`);
  }
  return { name, ok: true, recipe, records: [...dedup.values()], rawCount: records.length };
}

// グループ取込の中断シグナル (zip 内の不良ファイル検出時に transaction を rollback させる)
class GroupAbortError extends Error {
  constructor(prep) { super('group abort'); this.prep = prep; }
}

// ─── 確定: 検証済み1ファイル分を upsert (呼び出し元の transaction 内で実行) ───
function commitOne(db, logStmt, p, importedAt, uploadedBy) {
  const { name, recipe, records: finalRecords, rawCount } = p;
  let inserted = 0, updated = 0;
  let periodFrom = null, periodTo = null;
  const logInfo = logStmt.run(importedAt, uploadedBy, name, recipe.type, null, null, finalRecords.length, 0, 0, 'ok', '');
  const importId = logInfo.lastInsertRowid;

  let existsStmt, upsertStmt, periodKey;
  if (recipe.type === 'rpp_product') {
    periodKey = 'month_ym';
    existsStmt = db.prepare(`SELECT 1 FROM fact_rakuten_ads_rpp WHERE month_ym=? AND item_manage_number=? AND campaign_id=?`);
    upsertStmt = db.prepare(`INSERT INTO fact_rakuten_ads_rpp
      (month_ym, item_manage_number, campaign_id, campaign_name, item_name, clicks, impressions, ad_cost,
       cpc_actual, ctr_pct, sales_720h, orders_720h, cvr_720h_pct, roas_720h_pct,
       sales_12h, orders_12h, sales_720h_new, sales_720h_repeat, source_file, import_id, imported_at)
      VALUES (@month_ym, @item_manage_number, @campaign_id, @campaign_name, @item_name, @clicks, @impressions, @ad_cost,
       @cpc_actual, @ctr_pct, @sales_720h, @orders_720h, @cvr_720h_pct, @roas_720h_pct,
       @sales_12h, @orders_12h, @sales_720h_new, @sales_720h_repeat, @source_file, @import_id, @imported_at)
      ON CONFLICT (month_ym, item_manage_number, campaign_id) DO UPDATE SET
        campaign_name=excluded.campaign_name, item_name=excluded.item_name,
        clicks=excluded.clicks, impressions=excluded.impressions, ad_cost=excluded.ad_cost,
        cpc_actual=excluded.cpc_actual, ctr_pct=excluded.ctr_pct,
        sales_720h=excluded.sales_720h, orders_720h=excluded.orders_720h,
        cvr_720h_pct=excluded.cvr_720h_pct, roas_720h_pct=excluded.roas_720h_pct,
        sales_12h=excluded.sales_12h, orders_12h=excluded.orders_12h,
        sales_720h_new=excluded.sales_720h_new, sales_720h_repeat=excluded.sales_720h_repeat,
        source_file=excluded.source_file, import_id=excluded.import_id, imported_at=excluded.imported_at`);
  } else if (recipe.type === 'rpp_keyword') {
    periodKey = 'month_ym';
    existsStmt = db.prepare(`SELECT 1 FROM fact_rakuten_ads_rpp_keyword WHERE month_ym=? AND item_manage_number=? AND keyword=? AND campaign_id=?`);
    upsertStmt = db.prepare(`INSERT INTO fact_rakuten_ads_rpp_keyword
      (month_ym, item_manage_number, keyword, campaign_id, keyword_cpc, clicks, impressions, ad_cost,
       cpc_actual, ctr_pct, sales_720h, orders_720h, cvr_720h_pct, roas_720h_pct, source_file, import_id, imported_at)
      VALUES (@month_ym, @item_manage_number, @keyword, @campaign_id, @keyword_cpc, @clicks, @impressions, @ad_cost,
       @cpc_actual, @ctr_pct, @sales_720h, @orders_720h, @cvr_720h_pct, @roas_720h_pct, @source_file, @import_id, @imported_at)
      ON CONFLICT (month_ym, item_manage_number, keyword, campaign_id) DO UPDATE SET
        keyword_cpc=excluded.keyword_cpc, clicks=excluded.clicks, impressions=excluded.impressions,
        ad_cost=excluded.ad_cost, cpc_actual=excluded.cpc_actual, ctr_pct=excluded.ctr_pct,
        sales_720h=excluded.sales_720h, orders_720h=excluded.orders_720h,
        cvr_720h_pct=excluded.cvr_720h_pct, roas_720h_pct=excluded.roas_720h_pct,
        source_file=excluded.source_file, import_id=excluded.import_id, imported_at=excluded.imported_at`);
  } else {
    periodKey = 'date_jst';
    existsStmt = db.prepare(`SELECT 1 FROM fact_rakuten_ads_rpp_daily WHERE date_jst=? AND campaign_id=?`);
    upsertStmt = db.prepare(`INSERT INTO fact_rakuten_ads_rpp_daily
      (date_jst, campaign_id, campaign_name, clicks, impressions, ad_cost, ad_cost_discounted,
       cpc_actual, ctr_pct, sales_720h, orders_720h, cvr_720h_pct, roas_720h_pct,
       sales_12h, orders_12h, sales_720h_new, sales_720h_repeat, source_file, import_id, imported_at)
      VALUES (@date_jst, @campaign_id, @campaign_name, @clicks, @impressions, @ad_cost, @ad_cost_discounted,
       @cpc_actual, @ctr_pct, @sales_720h, @orders_720h, @cvr_720h_pct, @roas_720h_pct,
       @sales_12h, @orders_12h, @sales_720h_new, @sales_720h_repeat, @source_file, @import_id, @imported_at)
      ON CONFLICT (date_jst, campaign_id) DO UPDATE SET
        campaign_name=excluded.campaign_name,
        clicks=excluded.clicks, impressions=excluded.impressions, ad_cost=excluded.ad_cost,
        ad_cost_discounted=excluded.ad_cost_discounted,
        cpc_actual=excluded.cpc_actual, ctr_pct=excluded.ctr_pct,
        sales_720h=excluded.sales_720h, orders_720h=excluded.orders_720h,
        cvr_720h_pct=excluded.cvr_720h_pct, roas_720h_pct=excluded.roas_720h_pct,
        sales_12h=excluded.sales_12h, orders_12h=excluded.orders_12h,
        sales_720h_new=excluded.sales_720h_new, sales_720h_repeat=excluded.sales_720h_repeat,
        source_file=excluded.source_file, import_id=excluded.import_id, imported_at=excluded.imported_at`);
  }

  for (const r of finalRecords) {
    const exists = recipe.type === 'rpp_keyword'
      ? existsStmt.get(r.month_ym, r.item_manage_number, r.keyword, r.campaign_id)
      : recipe.type === 'rpp_product'
        ? existsStmt.get(r.month_ym, r.item_manage_number, r.campaign_id)
        : existsStmt.get(r.date_jst, r.campaign_id);
    const { period_raw, period_spans, ...rest } = r;
    upsertStmt.run({ ...rest, source_file: name.slice(0, 200), import_id: importId, imported_at: importedAt });
    if (exists) updated++; else inserted++;
    const pv = r[periodKey];
    if (periodFrom === null || pv < periodFrom) periodFrom = pv;
    if (periodTo === null || pv > periodTo) periodTo = pv;
  }
  db.prepare(`UPDATE radash_import_log SET date_from=?, date_to=?, inserted=?, updated=? WHERE id=?`)
    .run(periodFrom, periodTo, inserted, updated, importId);

  return {
    file: name, ok: true, type: recipe.type, label: recipe.label,
    date_from: periodFrom, date_to: periodTo,
    rows: finalRecords.length, inserted, updated,
    dedup_in_file: rawCount - finalRecords.length,
  };
}

// ─── 入口: アップロードされたファイル群を取り込む ───
export function importFiles(uploads, uploadedBy) {
  ensureImportTables();
  const db = getMirrorDB();
  const results = [];
  const logError = (fileName, fileType, status, message) => {
    try {
      db.prepare(`INSERT INTO radash_import_log
        (imported_at, uploaded_by, file_name, file_type, row_count, inserted, updated, status, message)
        VALUES (?, ?, ?, ?, 0, 0, 0, ?, ?)`)
        .run(new Date().toISOString(), uploadedBy, fileName, fileType, status, String(message || '').slice(0, 500));
    } catch {}
  };
  const logStmt = db.prepare(`INSERT INTO radash_import_log
    (imported_at, uploaded_by, file_name, file_type, date_from, date_to, row_count, inserted, updated, status, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

  // upload 1件ずつ展開し、グループ (zip 1個 or 単体CSV) を 1 トランザクションで取り込む。
  // メモリ対策: 全 upload の展開データを同時保持せず、グループ内も「1ファイルずつ parse→upsert」。
  // 途中のファイルで不良を検出したら GroupAbortError で transaction ごと rollback = 原子性維持。
  // 同時保持の最大 ≈ multer受信分 (4×30MB) + 処理中1 zip の展開分 (60MB) + parse中1ファイル分
  for (const uploadFile of uploads) {
    for (const g of expandFiles([uploadFile])) {
      if (g.error) {
        logError(g.name, 'unknown', 'error', g.error);
        results.push({ file: g.name, ok: false, type: 'unknown', error: g.error });
        continue;
      }
      const fileNames = g.files.map(f => f.name);
      const collected = [];
      const importedAt = new Date().toISOString();
      const tx = db.transaction(() => {
        for (const f of g.files) {
          const p = prepareOne(f.name, f.buffer);
          if (!p.ok) throw new GroupAbortError(p);
          collected.push(commitOne(db, logStmt, p, importedAt, uploadedBy));
        }
      });
      try {
        tx();
        results.push(...collected);
      } catch (e) {
        if (e instanceof GroupAbortError) {
          // 不良ファイル = エラー、同一グループの他ファイル = スキップ (取込済み分も rollback 済み)
          const p = e.prep;
          logError(p.name, p.type, 'error', p.error);
          results.push({ file: p.name, ok: false, type: p.type, error: p.error });
          // 同名エントリが複数ある zip でも件数が合うよう、不良ファイルは1件だけ除外
          const others = [...fileNames];
          const idx = others.indexOf(p.name);
          if (idx >= 0) others.splice(idx, 1);
          for (const name of others) {
            const msg = '同じzip内の他ファイルにエラーがあるため取込を中止しました (修正後にzipごと再アップロードしてください)';
            logError(name, 'unknown', 'skipped', msg);
            results.push({ file: name, ok: false, skipped: true, type: 'unknown', error: msg });
          }
        } else {
          // 予期しない DB エラー (tx は rollback 済み)。失敗履歴を tx 外で残す
          console.error(`[rakuten-analytics] import group ${g.name}: ${e.stack || e.message}`);
          for (const name of fileNames) {
            logError(name, 'unknown', 'error', `内部エラー: ${String(e.message).slice(0, 400)}`);
            results.push({ file: name, ok: false, type: 'unknown', error: '取込中に内部エラー (サーバーログ参照)' });
          }
        }
      }
      g.files = null;  // 展開バッファを解放
    }
  }
  return {
    results,
    imported: results.filter(r => r.ok).length,
    skipped: results.filter(r => !r.ok && r.skipped).length,
    failed: results.filter(r => !r.ok && !r.skipped).length,
  };
}

// ─── 鮮度ボード (F-7-4): データ種別ごとの取込状況 + 未取込月 ───
function monthsBetween(fromYm, toYm) {
  const out = [];
  let ym = fromYm;
  while (ym <= toYm) { out.push(ym); ym = addMonths(ym, 1); }
  return out;
}
export function getImportStatus() {
  ensureImportTables();
  const db = getMirrorDB();
  const currentYm = monthOf(jstToday());

  function statusOf(table, periodCol, type, label, granularity) {
    const monthExpr = periodCol === 'month_ym' ? 'month_ym' : `substr(${periodCol},1,7)`;
    const agg = db.prepare(`SELECT MIN(${periodCol}) AS d_from, MAX(${periodCol}) AS d_to, COUNT(*) AS rows FROM ${table}`).get();
    const lastLog = db.prepare(`SELECT imported_at, file_name FROM radash_import_log WHERE file_type=? AND status='ok' ORDER BY id DESC LIMIT 1`).get(type);
    let missingMonths = [];
    if (agg.d_from) {
      const have = new Set(db.prepare(`SELECT DISTINCT ${monthExpr} AS ym FROM ${table}`).all().map(r => r.ym));
      missingMonths = monthsBetween(agg.d_from.slice(0, 7), currentYm).filter(ym => !have.has(ym));
    }
    return {
      type, label, granularity, implemented: true,
      row_count: agg.rows, data_from: agg.d_from, data_to: agg.d_to,
      last_imported_at: lastLog?.imported_at || null,
      last_file: lastLog?.file_name || null,
      missing_months: missingMonths,
    };
  }

  return {
    generated_at: new Date().toISOString(),
    types: [
      statusOf('fact_rakuten_ads_rpp', 'month_ym', 'rpp_product', 'RPP 全商品レポート', '月次'),
      { ...statusOf('fact_rakuten_ads_rpp_keyword', 'month_ym', 'rpp_keyword', 'RPP 全キーワードレポート', '月次'), optional: true, note: 'キーワード入札 未使用のため取込不要' },
      statusOf('fact_rakuten_ads_rpp_daily', 'date_jst', 'rpp_daily', 'RPP 日次広告費 (すべての広告)', '日次'),
      { type: 'ca', label: 'クーポンアドバンス', implemented: false, note: 'P7 で対応予定' },
      { type: 'access', label: 'データ分析 (売上・アクセス)', implemented: false, note: 'P9 (Phase 2) で対応予定' },
      { type: 'monthly_costs', label: '月次費用明細', implemented: false, note: 'P8 で対応予定' },
    ],
  };
}

// ─── 取込履歴 ───
export function getImportLog(limit = 50) {
  ensureImportTables();
  const db = getMirrorDB();
  const n = Math.min(Math.max(Number(limit) || 50, 1), 200);
  return db.prepare(`SELECT * FROM radash_import_log ORDER BY id DESC LIMIT ?`).all(n);
}
