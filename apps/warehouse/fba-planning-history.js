/**
 * fba-planning-history.js — FBA 在庫計画レポート (GET_FBA_INVENTORY_PLANNING_DATA) の **日 × SKU の履歴** (2026-10-22)
 *
 * なぜ: 毎朝のスナップショット (fba-report-snapshot.js) は PLANNING を取っているのに、fba.db の planning_latest には最新 1 回分しか残さない。
 *   在庫の年齢 (0-90 / 91-180 / 181-270 / 271-365 / 365+ 日)・保管料の見込み・長期保管料 (AIS) の見込み・過剰在庫・推奨の処分 などは
 *   「いつから増えたか」を見るのに日ごとの履歴が要る (AI の分析と、週 1 回の長期在庫のアラート alert-fba-aged-inventory.js)。
 *   → **同じ取得の結果** (追加の API 呼び出しなし) から、この表に日ごとに残す。
 *
 * 置き場所 = warehouse.db (better-sqlite3)。🚨 fba.db (sql.js) には置かない:
 *   fba.db は保存のたびにファイル全体を書き戻す。数千 SKU × 400 日をそこに入れると、画面の保存のたびに重くなる。
 *   warehouse.db は毎日のバックアップ (backup-warehouse.js) の対象 = 取り直せない履歴の置き場所 (inbound-baseline.js が fba.db に書かないのと同じ考え)。
 *   既存の表 (planning_latest・daily_snapshots など) の列・鍵・意味は変えない。
 *
 * 表:
 *   fba_planning_daily       PK (snapshot_date, market, amazon_sku)。snapshot_date = 朝のスナップショットの business_date (JST)
 *   fba_planning_daily_days  PK (snapshot_date, market)。その日を「最後まで取れて置き換えた」印 (取得時刻・レポートの行数・残した行数・レポートに無かった列)
 *     → 履歴に行が無い SKU は「その日の在庫が 0 (在庫・入荷・年齢の数がどれも 0)」か「その日のレポートに載っていない」。印の無い日は「取れていない」
 *
 * 守り:
 *   - 列がレポートに無い・空・数でない ("--" など) は null (0 にしない)。どの列が無かったかは印 (missing_columns) とログに残す
 *   - 置き換えは日 × market ごと (1 取引)。同じ日に取り直すと、新しい取得で丸ごと入れ替わる (冪等)
 *   - 古い取得 (取得時刻が印より前) では戻さない・行数が前の 30% 未満に急に減った取得では置き換えない (planning_latest と同じ考え)・行が 0 の取得では消さない
 *   - 未来の日付は受けない。古い日の行は「いまの JST の日付」から KEEP_DAYS 日より前を消す (入力の日付で消す範囲を動かさない)
 */

export const PLANNING_HISTORY_KEEP_DAYS = 400;     // 1 年 + α (前年同週と比べられる・365 日超の在庫の 1 周期をまるごと見られる)
export const PLANNING_HISTORY_MIN_RATIO = 0.3;     // 前の取得の行数のこの割合より少ない取得では置き換えない

/**
 * [列名, 型, レポートの列の候補...]。候補の比べ方は normHeader (大文字小文字・空白・_ の違いを無視)。
 * 型: 'int' (整数でなければ null) / 'num' (数でなければ null) / 'text' (空なら null)
 * 🚨 コードの normalizePlanningRow (fba-replenishment/sp-api-reports.js) が読んでいる列 + Amazon の PLANNING レポートの列。
 *    在庫の年齢・AIS・推奨の処分の列は、このリポジトリのコードと試験のデータには無い (= 本物のレポートで確かめていない)。
 *    無ければ null になるだけで、どの列が無かったかは印とログに出る (ミニPC で初めて動かしたときに確かめる)
 */
export const PLANNING_HISTORY_COLUMNS = [
  ['report_snapshot_date', 'text', 'snapshot-date'],
  ['asin', 'text', 'asin'],
  ['fnsku', 'text', 'fnsku'],
  ['currency', 'text', 'currency'],
  ['your_price', 'num', 'your-price'],
  // 在庫
  ['available', 'int', 'available', 'afn-fulfillable-quantity'],
  ['reserved_qty', 'int', 'Total Reserved Quantity', 'reserved-quantity'],
  ['unfulfillable_qty', 'int', 'unfulfillable-quantity'],
  ['pending_removal_qty', 'int', 'pending-removal-quantity'],
  ['inbound_qty', 'int', 'inbound-quantity'],
  ['inbound_working', 'int', 'inbound-working'],
  ['inbound_shipped', 'int', 'inbound-shipped'],
  ['inbound_received', 'int', 'inbound-received'],
  // 在庫の年齢 (個数)
  ['inv_age_0_90', 'int', 'inv-age-0-to-90-days'],
  ['inv_age_91_180', 'int', 'inv-age-91-to-180-days'],
  ['inv_age_181_270', 'int', 'inv-age-181-to-270-days'],
  ['inv_age_271_365', 'int', 'inv-age-271-to-365-days'],
  ['inv_age_365_plus', 'int', 'inv-age-365-plus-days'],
  ['inv_age_0_30', 'int', 'inv-age-0-to-30-days'],
  ['inv_age_31_60', 'int', 'inv-age-31-to-60-days'],
  ['inv_age_61_90', 'int', 'inv-age-61-to-90-days'],
  ['inv_age_181_330', 'int', 'inv-age-181-to-330-days'],
  ['inv_age_331_365', 'int', 'inv-age-331-to-365-days'],
  // 販売の速さ
  ['units_shipped_t7', 'int', 'units-shipped-t7'],
  ['units_shipped_t30', 'int', 'units-shipped-t30'],
  ['units_shipped_t60', 'int', 'units-shipped-t60'],
  ['units_shipped_t90', 'int', 'units-shipped-t90'],
  ['sales_shipped_30d', 'num', 'sales-shipped-last-30-days'],
  ['sales_shipped_90d', 'num', 'sales-shipped-last-90-days'],
  ['sell_through', 'num', 'sell-through'],
  ['days_of_supply', 'num', 'days-of-supply'],
  ['total_days_of_supply', 'num', 'Total Days of Supply (including units from open shipments)'],
  ['historical_days_of_supply', 'num', 'historical-days-of-supply'],
  ['short_term_dos', 'num', 'Short term historical days of supply'],
  ['long_term_dos', 'num', 'Long term historical days of supply'],
  ['weeks_of_cover_t30', 'num', 'weeks-of-cover-t30'],
  ['weeks_of_cover_t90', 'num', 'weeks-of-cover-t90'],
  // 在庫の健全さ・過剰
  ['healthy_inventory_level', 'int', 'healthy-inventory-level'],
  ['fba_minimum_inventory_level', 'int', 'fba-minimum-inventory-level'],
  ['inventory_health_status', 'text', 'fba-inventory-level-health-status'],
  ['estimated_excess_qty', 'int', 'estimated-excess-quantity'],
  ['no_sale_last_6_months', 'int', 'no-sale-last-6-months'],
  ['alert', 'text', 'alert'],
  // 保管料・長期保管料 (AIS) の見込み
  ['estimated_storage_cost_next_month', 'num', 'estimated-storage-cost-next-month'],
  ['ais_qty_181_210', 'int', 'quantity-to-be-charged-ais-181-210-days'],
  ['ais_est_181_210', 'num', 'estimated-ais-181-210-days'],
  ['ais_qty_211_240', 'int', 'quantity-to-be-charged-ais-211-240-days'],
  ['ais_est_211_240', 'num', 'estimated-ais-211-240-days'],
  ['ais_qty_241_270', 'int', 'quantity-to-be-charged-ais-241-270-days'],
  ['ais_est_241_270', 'num', 'estimated-ais-241-270-days'],
  ['ais_qty_271_300', 'int', 'quantity-to-be-charged-ais-271-300-days'],
  ['ais_est_271_300', 'num', 'estimated-ais-271-300-days'],
  ['ais_qty_301_330', 'int', 'quantity-to-be-charged-ais-301-330-days'],
  ['ais_est_301_330', 'num', 'estimated-ais-301-330-days'],
  ['ais_qty_331_365', 'int', 'quantity-to-be-charged-ais-331-365-days'],
  ['ais_est_331_365', 'num', 'estimated-ais-331-365-days'],
  ['ais_qty_365_plus', 'int', 'quantity-to-be-charged-ais-365-plus-days'],
  ['ais_est_365_plus', 'num', 'estimated-ais-365-plus-days'],
  // 推奨の処分・値下げ
  ['recommended_action', 'text', 'recommended-action'],
  ['recommended_removal_qty', 'int', 'recommended-removal-quantity'],
  ['recommended_sales_price', 'num', 'recommended-sales-price'],
  ['recommended_sale_duration_days', 'int', 'recommended-sale-duration-days'],
  ['estimated_cost_savings', 'num', 'estimated-cost-savings-of-recommended-actions'],
  ['recommended_ship_qty', 'int', 'Recommended ship-in quantity', 'recommended-ship-in-quantity'],
  // 手数料・保管
  ['low_inv_fee_applied', 'text', 'Low-Inventory-Level fee applied in current week?'],
  ['storage_type', 'text', 'storage-type'],
  ['storage_volume', 'num', 'storage-volume'],
];

/** 「在庫がある」の判定に使う列 (どれかが 1 以上なら残す。どれも 0 なら残さない。どれも分からなければ残す) */
const STOCK_COLS = ['available', 'reserved_qty', 'unfulfillable_qty', 'pending_removal_qty', 'inbound_qty', 'inbound_working', 'inbound_shipped', 'inbound_received',
  'inv_age_0_90', 'inv_age_91_180', 'inv_age_181_270', 'inv_age_271_365', 'inv_age_365_plus',
  'ais_qty_181_210', 'ais_qty_211_240', 'ais_qty_241_270', 'ais_qty_271_300', 'ais_qty_301_330', 'ais_qty_331_365', 'ais_qty_365_plus'];

const COL_NAMES = PLANNING_HISTORY_COLUMNS.map(([c]) => c);
for (const c of COL_NAMES) if (!/^[a-z0-9_]+$/.test(c)) throw new Error(`列名が不正: ${c}`);   // SQL に埋めるので固定の英小文字だけ

/** レポートの列名の比べ方: 前後の空白・大文字小文字・空白 / _ / - の違いを無視 */
export const normHeader = (h) => String(h).trim().toLowerCase().replace(/[\s_-]+/g, '-');

function toInt(v) {
  const n = toNum(v);
  return n !== null && Number.isInteger(n) ? n : null;
}
function toNum(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  if (s === '') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}
function toText(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}
const CONVERT = { int: toInt, num: toNum, text: toText };

/** 生の行 (parseTsv の結果) の列名 → 正規化した名前の Map を作る */
function headerIndex(raw) {
  const m = new Map();
  for (const k of Object.keys(raw)) { const n = normHeader(k); if (!m.has(n)) m.set(n, k); }
  return m;
}

/**
 * PLANNING の生の行 1 つ → 履歴の行 (列が無い・空・数でないは null)。SKU が無い行は null
 * @param {object} raw  sp-api-reports.js の parseTsv の 1 行 (列名 → 文字列)
 */
export function normalizePlanningHistoryRow(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const idx = headerIndex(raw);
  // 候補ごとに: 同じ名前の列 → 無ければ表記違いの列 (大文字小文字・空白)。空の列は次の候補へ
  const has = (k) => k !== undefined && raw[k] !== undefined && raw[k] !== null && String(raw[k]).trim() !== '';
  const get = (cands) => { for (const c of cands) { if (has(c)) return raw[c]; const k = idx.get(normHeader(c)); if (has(k)) return raw[k]; } return undefined; };
  const sku = toText(get(['sku', 'merchant-sku']));
  if (!sku) return null;
  const out = { amazon_sku: sku };
  for (const [col, type, ...cands] of PLANNING_HISTORY_COLUMNS) out[col] = CONVERT[type](get(cands));
  return out;
}

/** レポートのどの行の列名にも無かった列 (= この回のレポートに無い列) */
export function missingPlanningColumns(rawRows) {
  const seen = new Set();
  for (const r of rawRows || []) if (r && typeof r === 'object') for (const k of Object.keys(r)) seen.add(normHeader(k));
  return PLANNING_HISTORY_COLUMNS.filter(([, , ...cands]) => !cands.some((c) => seen.has(normHeader(c)))).map(([c]) => c);
}

/** 在庫の数がどれか 1 以上 → true / 分かる数がどれも 0 → false / どれも分からない → true (捨てない) */
export function hasStock(row) {
  const known = STOCK_COLS.map((c) => row[c]).filter((v) => v !== null && v !== undefined);
  return known.length === 0 || known.some((v) => v > 0);
}

export function ensurePlanningHistoryTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS fba_planning_daily (
    snapshot_date TEXT NOT NULL,
    market        TEXT NOT NULL,
    amazon_sku    TEXT NOT NULL,
    ${PLANNING_HISTORY_COLUMNS.map(([c, type]) => `${c} ${type === 'text' ? 'TEXT' : type === 'int' ? 'INTEGER' : 'REAL'}`).join(',\n    ')},
    PRIMARY KEY (snapshot_date, market, amazon_sku)
  ) WITHOUT ROWID`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fba_planning_daily_sku ON fba_planning_daily (market, amazon_sku, snapshot_date)`);
  db.exec(`CREATE TABLE IF NOT EXISTS fba_planning_daily_days (
    snapshot_date    TEXT NOT NULL,
    market           TEXT NOT NULL,
    fetched_at       TEXT NOT NULL,
    saved_at         TEXT NOT NULL,
    report_rows      INTEGER NOT NULL,
    saved_rows       INTEGER NOT NULL,
    skipped_no_stock INTEGER NOT NULL,
    missing_columns  TEXT NOT NULL,
    PRIMARY KEY (snapshot_date, market)
  )`);
}

const isDate = (v) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v) && !Number.isNaN(Date.parse(`${v}T00:00:00Z`)) && new Date(`${v}T00:00:00Z`).toISOString().slice(0, 10) === v;
const jstDate = (ms) => new Date(ms + 9 * 3600000).toISOString().slice(0, 10);

/**
 * その日 × market の履歴を、この回の PLANNING の生の行で置き換える (1 取引)。
 * @param {import('better-sqlite3').Database} db  warehouse.db
 * @param {{ snapshotDate: string, market: 'jp'|'us', rawRows: object[], fetchedAt: string, now?: Date, keepDays?: number }} args
 * @returns {{ saved: boolean, reason?: string, reportRows: number, rows: number, skippedNoStock: number, duplicates: number, missingColumns: string[], deleted?: number }}
 */
export function savePlanningHistory(db, { snapshotDate, market, rawRows, fetchedAt, now = new Date(), keepDays = PLANNING_HISTORY_KEEP_DAYS }) {
  if (!isDate(snapshotDate)) throw new Error(`snapshotDate が不正: ${snapshotDate}`);
  const todayJst = jstDate(now.getTime());
  if (snapshotDate > todayJst) throw new Error(`snapshotDate が未来 (JST の今日 = ${todayJst}): ${snapshotDate}`);
  if (market !== 'jp' && market !== 'us') throw new Error(`market が不正: ${market}`);
  if (typeof fetchedAt !== 'string' || Number.isNaN(Date.parse(fetchedAt)) || !/(Z|[+-]\d{2}:\d{2})$/.test(fetchedAt)) throw new Error(`fetchedAt は Z か ±HH:MM つきの ISO 8601: ${fetchedAt}`);
  if (!Number.isInteger(keepDays) || keepDays < 1) throw new Error(`keepDays が不正: ${keepDays}`);
  ensurePlanningHistoryTables(db);
  const raw = Array.isArray(rawRows) ? rawRows : [];
  const missingColumns = missingPlanningColumns(raw);
  const bySku = new Map();
  let reportRows = 0, skippedNoStock = 0, duplicates = 0;
  for (const r of raw) {
    const row = normalizePlanningHistoryRow(r);
    if (!row) continue;   // SKU の無い行 (レポートの空行)
    reportRows++;
    if (bySku.has(row.amazon_sku)) duplicates++;   // 同じ SKU が 2 行 = 後の行を使う
    bySku.set(row.amazon_sku, row);
  }
  const base = { reportRows, rows: 0, skippedNoStock: 0, duplicates, missingColumns };
  if (reportRows === 0) return { saved: false, reason: 'no_rows', ...base };   // 0 行の取得では消さない
  const fetchedMs = Date.parse(fetchedAt);
  const ins = db.prepare(`INSERT INTO fba_planning_daily (snapshot_date, market, amazon_sku, ${COL_NAMES.join(', ')}) VALUES (?, ?, ?, ${COL_NAMES.map(() => '?').join(', ')})`);
  let result = null;
  db.transaction(() => {
    // 取引の中で前の印を読む (同時に 2 つ走っても、後から書く側が前の印を見てから決める)
    const prev = db.prepare('SELECT fetched_at, report_rows FROM fba_planning_daily_days WHERE snapshot_date = ? AND market = ?').get(snapshotDate, market);
    if (prev && Date.parse(prev.fetched_at) > fetchedMs) { result = { saved: false, reason: 'older_fetch', ...base }; return; }
    if (prev && reportRows < prev.report_rows * PLANNING_HISTORY_MIN_RATIO) { result = { saved: false, reason: 'count_guard', prev: prev.report_rows, ...base }; return; }
    db.prepare('DELETE FROM fba_planning_daily WHERE snapshot_date = ? AND market = ?').run(snapshotDate, market);
    let rows = 0;
    for (const row of bySku.values()) {
      if (!hasStock(row)) { skippedNoStock++; continue; }
      ins.run(snapshotDate, market, row.amazon_sku, ...COL_NAMES.map((c) => row[c]));
      rows++;
    }
    db.prepare(`INSERT INTO fba_planning_daily_days (snapshot_date, market, fetched_at, saved_at, report_rows, saved_rows, skipped_no_stock, missing_columns) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(snapshot_date, market) DO UPDATE SET fetched_at = excluded.fetched_at, saved_at = excluded.saved_at, report_rows = excluded.report_rows,
        saved_rows = excluded.saved_rows, skipped_no_stock = excluded.skipped_no_stock, missing_columns = excluded.missing_columns`)
      .run(snapshotDate, market, fetchedAt, now.toISOString(), reportRows, rows, skippedNoStock, JSON.stringify(missingColumns));
    // 古い日を消す。🚨 基準は入力の日付ではなく「いまの JST の日付」(cdb_stock_export と同じ)
    const limit = new Date(Date.parse(`${todayJst}T00:00:00Z`) - keepDays * 86400000).toISOString().slice(0, 10);
    const deleted = db.prepare('DELETE FROM fba_planning_daily WHERE snapshot_date < ?').run(limit).changes;
    db.prepare('DELETE FROM fba_planning_daily_days WHERE snapshot_date < ?').run(limit);
    result = { saved: true, ...base, rows, skippedNoStock, deleted };
  }).immediate();
  return result;
}

/** runFbaReportSnapshot の planningHistory に渡す形 (warehouse.db を返す関数を受ける)。結果の 1 行の注記も作る */
export function makePlanningHistorySaver(getDb, { log = console.log } = {}) {
  return (args) => {
    const r = savePlanningHistory(getDb(), args);
    log(`[fba-planning-history:${args.market}] ${r.saved ? `保存 ${r.rows} 行 (レポート ${r.reportRows} 行・在庫 0 で残さない ${r.skippedNoStock} 行${r.deleted ? `・古い行を消した ${r.deleted}` : ''})` : `置き換えない (${r.reason}${r.prev ? `: 前の取得 ${r.prev} 行 / 今回 ${r.reportRows} 行` : ''})`}${r.missingColumns.length ? ` / レポートに無い列: ${r.missingColumns.join(', ')}` : ''}`);
    return r;
  };
}
