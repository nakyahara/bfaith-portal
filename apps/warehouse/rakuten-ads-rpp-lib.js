/**
 * rakuten-ads-rpp-lib.js — 楽天RPPレポートCSV 取込ライブラリ (mall-csv-fetcher P1)
 *
 * #445 (apps/rakuten-analytics/import.js, Render側アップロードUI) のパースロジックを
 * miniPC取込 (B案: miniPCでパース→warehouse.db fact→sync→mirror) へ移植したもの。
 * ヘッダ自動判別 (RECIPES方式) / zip=1グループ原子取込 / 同期間UPSERT上書き (720h遡及対応)。
 *
 * RMS実画面/実CSVの仕様 (2026-07-09 実DLファイルで確認):
 *   - エンコーディング=CP932(Shift-JIS)。ヘッダは「コントロールカラム」で始まる行 (実測7行目)
 *   - 商品別レポートは集計期間「月ごと/全期間」のみ (日別なし) → 月次×SKUで取り込む
 *   - 商品別レポートにキャンペーン列・商品名列・インプレッション列は存在しない (46列実測)
 *   - 「すべての広告/キャンペーン」単位は「日ごと」可 → 日次合計を別テーブルで取り込む
 *   - 「全期間で表示」は指定日付範囲を1バケツ集計 (all-timeではない)。複数月にまたがる
 *     期間値はエラーにして「月ごとに表示」でのDLし直しを案内
 *   - 過去分は変動する (不正クリック控除) → 毎日直近分を再取得してUPSERT (冪等)
 *
 * 設計 (案A確定 2026-07-09):
 *   - fact_rakuten_ads_rpp        月次×商品 (PK: month_ym, item_manage_number)
 *   - fact_rakuten_ads_rpp_daily  日次×キャンペーン合計 (PK: date_jst, campaign_id)
 *   - 金額=税込円INTEGER / 率=REAL (feedback_ddl_design_blindspots)
 *   - SKUは LOWER(TRIM()) 正規化 + raw_sku_code に原文保持 (feedback_sku_case_normalization)
 *   - キーワードレポートは未使用のため取込対象外 (検出したら明示スキップ)
 */
import AdmZip from 'adm-zip';
import iconv from 'iconv-lite';

export const SHOP_CODE = 'b-faith';

// ─── テーブル (取込側が所有。CREATE IF NOT EXISTS で初回作成) ───
export function ensureRppTables(db) {
  // 商品別 (月次)。商品別レポートは「月ごと/全期間」のみで日別が存在しないため月次粒度。
  // SKU別の広告後利益は原則「月次」で見る (日次に日割りしない — Codexガードレール)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_ads_rpp (
    month_ym              TEXT NOT NULL CHECK(month_ym GLOB '????-??'),
    item_manage_number    TEXT NOT NULL CHECK(trim(item_manage_number) <> ''),
    raw_sku_code          TEXT NOT NULL DEFAULT '',
    clicks                INTEGER NOT NULL DEFAULT 0,
    ad_cost_yen           INTEGER NOT NULL DEFAULT 0,
    cpc_actual            REAL,
    ctr_pct               REAL,
    bid_cpc_yen           INTEGER,
    item_cpc_yen          INTEGER,
    sales_720h_yen        INTEGER NOT NULL DEFAULT 0,
    orders_720h           INTEGER NOT NULL DEFAULT 0,
    cvr_720h_pct          REAL,
    roas_720h_pct         REAL,
    sales_12h_yen         INTEGER,
    orders_12h            INTEGER,
    sales_720h_new_yen    INTEGER,
    sales_720h_repeat_yen INTEGER,
    source_report_type    TEXT NOT NULL DEFAULT 'rpp_product_monthly',
    report_start          TEXT,
    report_end            TEXT,
    attribution_window_hours INTEGER NOT NULL DEFAULT 720,
    is_tax_included       INTEGER NOT NULL DEFAULT 1,
    source_file           TEXT NOT NULL DEFAULT '',
    import_id             INTEGER NOT NULL,
    imported_at           TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    PRIMARY KEY (month_ym, item_manage_number)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frar_item ON fact_rakuten_ads_rpp(item_manage_number, month_ym)`);

  // 店舗全体/キャンペーン別 (日次 — 集計単位「すべての広告」or「キャンペーン」×「日ごとに表示」)。
  // 掛け方変化 (5のつく日スポット出稿等) に自動追従する日次合計
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_ads_rpp_daily (
    date_jst              TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
    campaign_id           TEXT NOT NULL DEFAULT '',
    campaign_name         TEXT NOT NULL DEFAULT '',
    clicks                INTEGER NOT NULL DEFAULT 0,
    ad_cost_yen           INTEGER NOT NULL DEFAULT 0,
    ad_cost_discounted_yen INTEGER,
    cpc_actual            REAL,
    ctr_pct               REAL,
    sales_720h_yen        INTEGER NOT NULL DEFAULT 0,
    orders_720h           INTEGER NOT NULL DEFAULT 0,
    cvr_720h_pct          REAL,
    roas_720h_pct         REAL,
    sales_12h_yen         INTEGER,
    orders_12h            INTEGER,
    sales_720h_new_yen    INTEGER,
    sales_720h_repeat_yen INTEGER,
    source_report_type    TEXT NOT NULL DEFAULT 'rpp_all_daily',
    attribution_window_hours INTEGER NOT NULL DEFAULT 720,
    is_tax_included       INTEGER NOT NULL DEFAULT 1,
    source_file           TEXT NOT NULL DEFAULT '',
    import_id             INTEGER NOT NULL,
    imported_at           TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    PRIMARY KEY (date_jst, campaign_id)
  )`);

  // 取込ログ (冪等キー: file_sha256 — 同一バイト列の再投入は duplicate スキップ)
  db.exec(`CREATE TABLE IF NOT EXISTS raw_rakuten_ads_import_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at  TEXT NOT NULL,
    shop_code    TEXT NOT NULL DEFAULT '${SHOP_CODE}',
    source       TEXT NOT NULL DEFAULT 'incoming',
    file_name    TEXT NOT NULL,
    file_sha256  TEXT NOT NULL DEFAULT '',
    file_type    TEXT NOT NULL,
    date_from    TEXT,
    date_to      TEXT,
    row_count    INTEGER NOT NULL DEFAULT 0,
    inserted     INTEGER NOT NULL DEFAULT 0,
    updated      INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL CHECK (status IN ('ok','error','skipped','duplicate')),
    message      TEXT NOT NULL DEFAULT ''
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rrail_sha ON raw_rakuten_ads_import_log(file_sha256, status)`);

  // 自動DL側の取得ログ (downloader が書く。DL失敗/空期間の観測用)
  db.exec(`CREATE TABLE IF NOT EXISTS report_fetch_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at   TEXT NOT NULL,
    mall         TEXT NOT NULL,
    report_type  TEXT NOT NULL,
    period_from  TEXT,
    period_to    TEXT,
    file_name    TEXT,
    file_sha256  TEXT,
    file_bytes   INTEGER,
    status       TEXT NOT NULL CHECK (status IN ('ok','empty','error')),
    message      TEXT NOT NULL DEFAULT ''
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rfl_type ON report_fetch_log(mall, report_type, fetched_at)`);
}

// ─── CSV decode + parse (#445 移植、purchase-orders の実装パターン踏襲) ───
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
// 金額 (税込円) は INTEGER で保持
const yenOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };
const yen0 = v => Math.round(num0(v));

// 日付を YYYY-MM-DD に正規化 (2026/7/6, 2026-07-06, 20260706, 2026年07月06日 対応)。
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

// RPP商品別レポートの「日付」列は期間文字列 (実データ確認):
//   「2026年07月01日～2026年07月08日」(範囲) / 「2026年07月」等。
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
    return { ym: d1.slice(0, 7), from: d1, to: d2 };
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

// 期間列 (年月 or 日付)。product は月次、daily は日次
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
    adCost: pickMetric(headers, '実績額', ''),
    cpcActual: pickCol(headers, ['cpc実績', '合計']) >= 0 ? pickCol(headers, ['cpc実績', '合計'])
      : pickCol(headers, ['cpc実績'], ['新規', '既存']),
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
    ad_cost_yen: col.adCost >= 0 ? yen0(cells[col.adCost]) : 0,
    cpc_actual: col.cpcActual >= 0 ? numOrNull(cells[col.cpcActual]) : null,
    ctr_pct: col.ctr >= 0 ? numOrNull(cells[col.ctr]) : null,
    sales_720h_yen: col.sales720 >= 0 ? yen0(cells[col.sales720]) : 0,
    orders_720h: col.orders720 >= 0 ? Math.round(num0(cells[col.orders720])) : 0,
    cvr_720h_pct: col.cvr720 >= 0 ? numOrNull(cells[col.cvr720]) : null,
    roas_720h_pct: col.roas720 >= 0 ? numOrNull(cells[col.roas720]) : null,
    sales_12h_yen: col.sales12 >= 0 ? yenOrNull(cells[col.sales12]) : null,
    orders_12h: col.orders12 >= 0 ? numOrNull(cells[col.orders12]) : null,
    sales_720h_new_yen: col.sales720New >= 0 ? yenOrNull(cells[col.sales720New]) : null,
    sales_720h_repeat_yen: col.sales720Repeat >= 0 ? yenOrNull(cells[col.sales720Repeat]) : null,
  };
}

// SKU正規化 (feedback_sku_case_normalization: LOWER(TRIM()) 必須)
const normSku = v => trimS(v).toLowerCase();

// ─── RECIPES: ヘッダからファイル種別を判別し、行→レコード変換を提供 ───
// period: 'month' = 商品別 (RMS仕様で月次のみ) / 'day' = すべての広告・キャンペーン日次
export const IMPORT_RECIPES = [
  {
    // キーワード別は未使用のため取込対象外。検出だけして明示スキップ (黙って商品別扱いしない)
    type: 'rpp_keyword',
    label: 'RPP 全キーワードレポート (未対応)',
    unsupported: 'キーワードレポートは取込対象外です (キーワード入札 未使用のため)。商品別レポートをDLしてください',
    detect: (headers) => pickCol(headers, ['キーワード'], ['cpc', '入札']) >= 0
      && (pickCol(headers, ['実績額']) >= 0 || pickCol(headers, ['クリック']) >= 0),
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
      bidCpc: pickCol(headers, ['入札単価']),
      itemCpc: pickCol(headers, ['商品cpc']),
      campaignId: pickCol(headers, ['キャンペーンid']),
      ...metricCols(headers),
    }),
    // item は PK 構成要素。根幹メトリクスは0値で静かに取り込まない
    required: ['period', 'item', 'clicks', 'adCost', 'sales720', 'orders720'],
    // PK=(month_ym, item_manage_number) にキャンペーン粒度は含まない (案A)。
    // 万一キャンペーン列を含む商品別CSVが来たら黙って潰さずエラー (prepareFile 側で検査)
    toRecord: (cells, col) => {
      const p = parseMonthPeriod(cells[col.period]);
      return {
        period_raw: trimS(cells[col.period]),
        month_ym: p.ym,
        period_spans: !!p.spans,
        report_start: p.from || null,
        report_end: p.to || null,
        item_manage_number: normSku(cells[col.item]),
        raw_sku_code: trimS(cells[col.item]),
        bid_cpc_yen: col.bidCpc >= 0 ? yenOrNull(cells[col.bidCpc]) : null,
        item_cpc_yen: col.itemCpc >= 0 ? yenOrNull(cells[col.itemCpc]) : null,
        ...metricRecord(cells, col),
      };
    },
    validRecord: (r) => r.month_ym && !r.period_spans && r.item_manage_number !== '',
    invalidReason: (r) => r.period_spans ? '期間が複数月にまたがっています。集計期間「月ごとに表示」でDLし直してください'
      : !r.month_ym ? '年月が不正' : '商品管理番号が空',
    pk: (r) => `${r.month_ym}\x1f${r.item_manage_number}`,
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
      ad_cost_discounted_yen: col.adCostDiscounted >= 0 ? yenOrNull(cells[col.adCostDiscounted]) : null,
      ...metricRecord(cells, col),
    }),
    validRecord: (r) => !!r.date_jst,
    invalidReason: () => '日付が不正',
    pk: (r) => `${r.date_jst}\x1f${r.campaign_id}`,
  },
];

// ─── zip 展開 (zip爆弾ガード付き、#445 移植) ───
// 方針: ①展開前に全エントリを宣言値で検証し、1つでも違反したらその zip 全体を reject
// ②展開後の実測サイズが宣言を大きく超えたら zip 全体を破棄 (宣言詐称)。
const ZIP_MAX_ENTRIES = 100;
const ZIP_MAX_ENTRY_BYTES = 30 * 1024 * 1024;   // 1エントリ (宣言/実測) 30MB
const ZIP_MAX_TOTAL_BYTES = 60 * 1024 * 1024;   // 1 zip の展開後合計 (宣言) 60MB
const ZIP_MAX_RATIO = 300;                       // 圧縮率ガード (宣言/圧縮後 > 300倍 は爆弾扱い)
// 戻り値は「グループ」単位: zip 1個 = 1グループ (原子的に取込)、単体CSV = 1ファイルグループ。
// { name, files: [{name, buffer}] } または { name, error }
export function expandFile(name, buffer) {
  const isZip = /\.zip$/i.test(name)
    || (buffer.length >= 4 && buffer[0] === 0x50 && buffer[1] === 0x4B && buffer[2] === 0x03 && buffer[3] === 0x04);
  if (!isZip) return { name, files: [{ name, buffer }] };

  let zip, entries;
  try {
    zip = new AdmZip(buffer);
    entries = zip.getEntries().filter(en => !en.isDirectory && /\.csv$/i.test(en.entryName));
  } catch (e) {
    return { name, error: `zipの展開に失敗: ${e.message}` };
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
  if (verdict) return { name, error: verdict };

  // ② 展開。宣言詐称 (実測が宣言を大きく超える) や壊れたエントリを検出したら zip 全体を破棄
  const extracted = [];
  for (const en of entries) {
    let data;
    try { data = en.getData(); } catch (e) {
      return { name, error: `${en.entryName}: エントリの展開に失敗 (${e.message})` };
    }
    if (data.length > ZIP_MAX_ENTRY_BYTES || data.length > en.header.size + 1024) {
      return { name, error: `${en.entryName}: 展開後サイズが宣言と一致しません (zip爆弾の疑い)` };
    }
    extracted.push({ name: `${name}/${en.entryName}`, buffer: data });
  }
  return { name, files: extracted };
}

// ─── 検証フェーズ: 1ファイルを parse + レコード化 (DB 書き込みなし) ───
// 戻り値: { name, ok:false, type, error, skipped? } または { name, ok:true, recipe, records, rawCount, reportStart, reportEnd }
export function prepareFile(name, buffer) {
  const fail = (type, message, skipped = false) => ({ name, ok: false, type, error: message, skipped });

  let rows;
  try { rows = parseCsvBuffer(buffer); } catch (e) { return fail('unknown', `読込失敗: ${e.message}`); }
  if (rows.length < 2) return fail('unknown', 'データ行がありません');

  // ヘッダ行の探索: RMSの実CSVは先頭に前置行がある (実測):
  //   L0「実行日時: …」/ L2「検索条件」/ L4「集計期間: …」/ L5「■■■…削除してください■■■」。
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
  if (recipe.unsupported) return fail(recipe.type, recipe.unsupported, true);

  // ファイルレベルの検索期間 (前置行「集計期間: … 2026-07-01 - 2026-07-08」) — best effort
  let fileStart = null, fileEnd = null;
  for (let i = 0; i < headerIdx; i++) {
    const line = rows[i].join(',');
    const m = line.match(/(\d{4}-\d{2}-\d{2})\s*[-〜~]\s*(\d{4}-\d{2}-\d{2})/);
    if (m) { fileStart = m[1]; fileEnd = m[2]; break; }
  }

  rows = rows.slice(headerIdx);  // rows[0] = ヘッダ行に正規化
  const col = recipe.columns(headers);
  const missing = recipe.required.filter(k => col[k] < 0);
  if (missing.length) {
    const COL_LABEL = {
      period: '年月/日付', item: '商品管理番号',
      clicks: 'クリック数', adCost: '実績額', sales720: '売上金額(720時間)', orders720: '売上件数(720時間)',
    };
    const names = missing.map(k => COL_LABEL[k] || k).join(' / ');
    const hint = missing.includes('period')
      ? `期間列 (年月/日付) が見つかりません。集計期間「月ごとに表示」でDLしてください。`
      : `必須列が見つかりません: ${names}。RMSのDL時に表示/出力項目「全てを選択」にしてください。`;
    return fail(recipe.type, `${hint} (見出し: ${headersRaw.slice(0, 12).join(' / ')})`);
  }
  // 商品別レポートにキャンペーン列があったら PK 粒度が合わない (案A: 月×商品)。
  // 黙って後勝ちで潰すと広告費が欠落するため明示エラー
  if (recipe.type === 'rpp_product' && col.campaignId >= 0) {
    return fail(recipe.type, 'キャンペーン列を含む商品別レポートは未対応です (月次×商品の粒度で取り込むため)。集計単位「商品別」でDLし直してください');
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

  // ファイル内 PK 重複は黙って後勝ち統合せずエラー (広告費・売上の欠落防止 — Codex R1 Medium):
  //   日次: キャンペーン単位CSVにキャンペーンID列が無い可能性
  //   商品別: PK に含まない粒度 (キャンペーン等) の混入や RMS 仕様変化の可能性
  const dedup = new Map();
  for (const r of records) dedup.set(recipe.pk(r), r);
  if (dedup.size < records.length) {
    const seen = new Set(), dupKeys = [];
    for (const r of records) {
      const k = recipe.pk(r);
      if (seen.has(k) && dupKeys.length < 3) dupKeys.push(k.replace(/\x1f/g, ' × '));
      seen.add(k);
    }
    const hint = recipe.type === 'rpp_daily'
      ? 'キャンペーン単位のCSVはキャンペーンID列を含めてDLするか、集計単位「すべての広告」でDLしてください。'
      : '集計単位「商品別」×「月ごとに表示」でDLし直してください。';
    return fail(recipe.type, `同一キーの行が複数あります (${records.length}行→${dedup.size}件、例: ${dupKeys.join(' / ')})。${hint}`);
  }
  return {
    name, ok: true, recipe, records: [...dedup.values()], rawCount: records.length,
    reportStart: fileStart, reportEnd: fileEnd,
  };
}

// グループ取込の中断シグナル (zip 内の不良ファイル検出時に transaction を rollback させる)
export class GroupAbortError extends Error {
  constructor(prep) { super('group abort'); this.prep = prep; }
}

// ─── 確定: 検証済み1ファイル分を upsert (呼び出し元の transaction 内で実行) ───
export function commitOne(db, logStmt, p, ctx) {
  const { name, recipe, records: finalRecords, rawCount, reportStart, reportEnd } = p;
  const { importedAt, source, fileSha256 } = ctx;
  let inserted = 0, updated = 0;
  let periodFrom = null, periodTo = null;
  const logInfo = logStmt.run(importedAt, source, name, fileSha256, recipe.type, null, null, finalRecords.length, 0, 0, 'ok', '');
  const importId = logInfo.lastInsertRowid;

  let existsStmt, upsertStmt, periodKey;
  if (recipe.type === 'rpp_product') {
    periodKey = 'month_ym';
    existsStmt = db.prepare(`SELECT 1 FROM fact_rakuten_ads_rpp WHERE month_ym=? AND item_manage_number=?`);
    upsertStmt = db.prepare(`INSERT INTO fact_rakuten_ads_rpp
      (month_ym, item_manage_number, raw_sku_code, clicks, ad_cost_yen, cpc_actual, ctr_pct,
       bid_cpc_yen, item_cpc_yen, sales_720h_yen, orders_720h, cvr_720h_pct, roas_720h_pct,
       sales_12h_yen, orders_12h, sales_720h_new_yen, sales_720h_repeat_yen,
       source_report_type, report_start, report_end, attribution_window_hours, is_tax_included,
       source_file, import_id, imported_at, updated_at)
      VALUES (@month_ym, @item_manage_number, @raw_sku_code, @clicks, @ad_cost_yen, @cpc_actual, @ctr_pct,
       @bid_cpc_yen, @item_cpc_yen, @sales_720h_yen, @orders_720h, @cvr_720h_pct, @roas_720h_pct,
       @sales_12h_yen, @orders_12h, @sales_720h_new_yen, @sales_720h_repeat_yen,
       @source_report_type, @report_start, @report_end, 720, 1,
       @source_file, @import_id, @imported_at, @updated_at)
      ON CONFLICT (month_ym, item_manage_number) DO UPDATE SET
        raw_sku_code=excluded.raw_sku_code,
        clicks=excluded.clicks, ad_cost_yen=excluded.ad_cost_yen,
        cpc_actual=excluded.cpc_actual, ctr_pct=excluded.ctr_pct,
        bid_cpc_yen=excluded.bid_cpc_yen, item_cpc_yen=excluded.item_cpc_yen,
        sales_720h_yen=excluded.sales_720h_yen, orders_720h=excluded.orders_720h,
        cvr_720h_pct=excluded.cvr_720h_pct, roas_720h_pct=excluded.roas_720h_pct,
        sales_12h_yen=excluded.sales_12h_yen, orders_12h=excluded.orders_12h,
        sales_720h_new_yen=excluded.sales_720h_new_yen, sales_720h_repeat_yen=excluded.sales_720h_repeat_yen,
        source_report_type=excluded.source_report_type,
        report_start=excluded.report_start, report_end=excluded.report_end,
        source_file=excluded.source_file, import_id=excluded.import_id,
        imported_at=excluded.imported_at, updated_at=excluded.updated_at`);
  } else {
    periodKey = 'date_jst';
    existsStmt = db.prepare(`SELECT 1 FROM fact_rakuten_ads_rpp_daily WHERE date_jst=? AND campaign_id=?`);
    upsertStmt = db.prepare(`INSERT INTO fact_rakuten_ads_rpp_daily
      (date_jst, campaign_id, campaign_name, clicks, ad_cost_yen, ad_cost_discounted_yen,
       cpc_actual, ctr_pct, sales_720h_yen, orders_720h, cvr_720h_pct, roas_720h_pct,
       sales_12h_yen, orders_12h, sales_720h_new_yen, sales_720h_repeat_yen,
       source_report_type, attribution_window_hours, is_tax_included,
       source_file, import_id, imported_at, updated_at)
      VALUES (@date_jst, @campaign_id, @campaign_name, @clicks, @ad_cost_yen, @ad_cost_discounted_yen,
       @cpc_actual, @ctr_pct, @sales_720h_yen, @orders_720h, @cvr_720h_pct, @roas_720h_pct,
       @sales_12h_yen, @orders_12h, @sales_720h_new_yen, @sales_720h_repeat_yen,
       @source_report_type, 720, 1,
       @source_file, @import_id, @imported_at, @updated_at)
      ON CONFLICT (date_jst, campaign_id) DO UPDATE SET
        campaign_name=excluded.campaign_name,
        clicks=excluded.clicks, ad_cost_yen=excluded.ad_cost_yen,
        ad_cost_discounted_yen=excluded.ad_cost_discounted_yen,
        cpc_actual=excluded.cpc_actual, ctr_pct=excluded.ctr_pct,
        sales_720h_yen=excluded.sales_720h_yen, orders_720h=excluded.orders_720h,
        cvr_720h_pct=excluded.cvr_720h_pct, roas_720h_pct=excluded.roas_720h_pct,
        sales_12h_yen=excluded.sales_12h_yen, orders_12h=excluded.orders_12h,
        sales_720h_new_yen=excluded.sales_720h_new_yen, sales_720h_repeat_yen=excluded.sales_720h_repeat_yen,
        source_report_type=excluded.source_report_type,
        source_file=excluded.source_file, import_id=excluded.import_id,
        imported_at=excluded.imported_at, updated_at=excluded.updated_at`);
  }

  const sourceReportType = recipe.type === 'rpp_product' ? 'rpp_product_monthly' : 'rpp_all_daily';
  for (const r of finalRecords) {
    const exists = recipe.type === 'rpp_product'
      ? existsStmt.get(r.month_ym, r.item_manage_number)
      : existsStmt.get(r.date_jst, r.campaign_id);
    const { period_raw, period_spans, ...rest } = r;
    // 「2026年07月」形式の行は行内から期間を取れない → ファイル前置行の検索期間へフォールバック
    if (recipe.type === 'rpp_product') {
      rest.report_start = rest.report_start ?? reportStart ?? null;
      rest.report_end = rest.report_end ?? reportEnd ?? null;
    }
    upsertStmt.run({
      ...rest,
      source_report_type: sourceReportType,
      source_file: name.slice(0, 200),
      import_id: importId,
      imported_at: importedAt,
      updated_at: importedAt,
    });
    if (exists) updated++; else inserted++;
    const pv = r[periodKey];
    if (periodFrom === null || pv < periodFrom) periodFrom = pv;
    if (periodTo === null || pv > periodTo) periodTo = pv;
  }
  db.prepare(`UPDATE raw_rakuten_ads_import_log SET date_from=?, date_to=?, inserted=?, updated=? WHERE id=?`)
    .run(periodFrom, periodTo, inserted, updated, importId);

  return {
    file: name, ok: true, type: recipe.type, label: recipe.label,
    date_from: periodFrom, date_to: periodTo,
    rows: finalRecords.length, inserted, updated,
    dedup_in_file: rawCount - finalRecords.length,
  };
}

export function getLogInsertStmt(db) {
  return db.prepare(`INSERT INTO raw_rakuten_ads_import_log
    (imported_at, source, file_name, file_sha256, file_type, date_from, date_to, row_count, inserted, updated, status, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
}

// ─── 入口: 物理ファイル1個 (zip or CSV) を1トランザクションで取り込む ───
// 冪等: 同一 sha256 が status='ok' で取込済みなら duplicate としてスキップ。
// 戻り値: { status: 'ok'|'error'|'skipped'|'duplicate', results: [...] }
export function importPhysicalFile(db, { name, buffer, sha256, source = 'incoming' }) {
  const importedAt = new Date().toISOString();
  const logStmt = getLogInsertStmt(db);
  const logOutcome = (fileName, fileType, status, message) => {
    try {
      logStmt.run(importedAt, source, fileName, sha256, fileType, null, null, 0, 0, 0, status, String(message || '').slice(0, 500));
    } catch (e) { console.error(`[rpp-import] ログ記録失敗: ${e.message}`); }
  };

  const dup = db.prepare(`SELECT id FROM raw_rakuten_ads_import_log WHERE file_sha256 = ? AND status = 'ok' LIMIT 1`)
    .get(sha256);
  if (dup) {
    logOutcome(name, 'unknown', 'duplicate', `同一内容のファイルを取込済み (import log id=${dup.id})`);
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
      const p = prepareFile(f.name, f.buffer);
      if (!p.ok) throw new GroupAbortError(p);
      collected.push(commitOne(db, logStmt, p, ctx));
    }
  });
  try {
    tx();
    return { status: 'ok', results: collected };
  } catch (e) {
    if (e instanceof GroupAbortError) {
      // 不良ファイル = エラー、同一グループの他ファイル = スキップ (取込済み分も rollback 済み)
      const p = e.prep;
      const results = [];
      logOutcome(p.name, p.type, p.skipped ? 'skipped' : 'error', p.error);
      results.push({ file: p.name, ok: false, skipped: !!p.skipped, type: p.type, error: p.error });
      const others = [...fileNames];
      const idx = others.indexOf(p.name);
      if (idx >= 0) others.splice(idx, 1);
      for (const otherName of others) {
        const msg = '同じzip内の他ファイルにエラーがあるため取込を中止しました (修正後にzipごと再投入してください)';
        logOutcome(otherName, 'unknown', 'skipped', msg);
        results.push({ file: otherName, ok: false, skipped: true, type: 'unknown', error: msg });
      }
      return { status: p.skipped ? 'skipped' : 'error', results };
    }
    // 予期しない DB エラー (tx は rollback 済み)。失敗履歴を tx 外で残す
    console.error(`[rpp-import] group ${g.name}: ${e.stack || e.message}`);
    const results = fileNames.map(fileName => {
      logOutcome(fileName, 'unknown', 'error', `内部エラー: ${String(e.message).slice(0, 400)}`);
      return { file: fileName, ok: false, type: 'unknown', error: `内部エラー: ${String(e.message).slice(0, 200)}` };
    });
    return { status: 'error', results };
  }
}
