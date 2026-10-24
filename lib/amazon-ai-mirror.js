/**
 * amazon-ai-mirror.js — Amazon AI の分析 (ai-insights の facts-amazon.js) が読む表を Render の mirror に写す決まり (2026-10-24)
 *
 * 送り手 (ミニPC apps/warehouse/sync-amazon-ai-mirror.js) と受け手 (Render apps/warehouse-mirror/db.js・router.js) と
 * 契約 (warehouse.db の sync_contracts) で共有する定数。import を持たない部品 (mirror の db.js が読む)。
 *
 * 決まり:
 *   - mirror の表 = `mirror_<元の表名>` (facts-amazon.js の TABLES の 1 番目の候補)。列の名前と型は元の表のまま
 *     (facts-amazon.js を変えずに読める)。送るのは facts-amazon.js が読む列と主キーの列だけ
 *   - 返品・補てんは元の主キー (注文番号など) を送らず、送り手がそのハッシュ row_key (16 桁) を作って主キーにする
 *   - 毎朝は直近 sendDays 日を「日ごとに置き換え」(date_range clear。日付の列は dateCol)。前の回に送れなかった日があれば、そこから送り直す
 *   - mirror には keepDays 日だけ残す (受け手が古い行を消す)。keepRecentDays がある表 (毎日の写し) は、それより古い日は
 *     「月の最後の 4 日」だけ残す (facts-amazon.js は期間の最後の日の 3 日前までの写しだけ読む = 週次は直近・月次は月末)
 *   - 1 回の run は runDays 日まで (初回の全部送りを小分けにして、受け手の stage = 一時の置き場を小さくする)
 *
 * 🚨 列を変えるときは contract_version を上げ、受け手の表を作り直す (CREATE TABLE IF NOT EXISTS は列を足さない)。
 */

export const AMAZON_AI_CONTRACT_VERSION = 1;
export const AMAZON_AI_OWNER = 'ai-insights';
/** 月の最後の何日を残すか = facts-amazon.js の STALE_AFTER_DAYS (3) + 1 */
export const MONTH_END_KEEP_DAYS = 4;

const T = 'TEXT', TN = 'TEXT NOT NULL', I = 'INTEGER', IN = 'INTEGER NOT NULL', R = 'REAL', RN = 'REAL NOT NULL';

/** 在庫計画 (fba_planning_daily) で facts-amazon.js (longTermStorage) が読む列 */
const PLANNING_READ_COLS = [
  ['asin', T], ['available', I], ['your_price', R], ['estimated_excess_qty', I], ['estimated_storage_cost_next_month', R],
  ['recommended_action', T], ['units_shipped_t30', I],
  ['inv_age_181_270', I], ['inv_age_271_365', I], ['inv_age_181_330', I], ['inv_age_331_365', I],
  ['inv_age_365_plus', I], ['inv_age_366_455', I], ['inv_age_456_plus', I],
  ['ais_est_181_210', R], ['ais_est_211_240', R], ['ais_est_241_270', R], ['ais_est_271_300', R], ['ais_est_301_330', R],
  ['ais_est_331_365', R], ['ais_est_365_plus', R], ['ais_est_366_455', R], ['ais_est_456_plus', R],
];

const SNAPSHOT_KEEP = { keepDays: 120, keepRecentDays: 14 };

/**
 * entity = 送り先の名前 (/api/sync/<entity>/chunk・sync_contracts.entity) = 元の表名。
 *   source / table / dateCol / cols ([列, 型]) / pk / where (送る行の絞り込み・任意) / rowKey (元の主キーの列・任意) /
 *   sendDays / keepDays / keepRecentDays (任意) / runDays / grain (契約の説明)
 */
export const AMAZON_AI_MIRROR_ENTITIES = Object.freeze([
  {
    entity: 'amazon_sales_traffic_daily', source: 'amazon_sales_traffic_daily',
    grain: 'one row = one date_jst (店全体のセッション・PV・注文・カート獲得率)',
    dateCol: 'date_jst',
    cols: [['date_jst', T], ['sessions', I], ['page_views', I], ['units_ordered', I], ['ordered_product_sales', R], ['buy_box_percentage', R]],
    pk: ['date_jst'],
    // 1 日 1 行なので長く残す (月次は今月 + 前の 3 か月の中央値 = 4 か月 + 確定版の遅れ)
    sendDays: 7, keepDays: 400, runDays: 400,
  },
  {
    entity: 'amazon_sales_traffic_sku_daily', source: 'amazon_sales_traffic_sku_daily',
    grain: 'one row = one (date_jst, seller_sku, child_asin)',
    dateCol: 'date_jst',
    cols: [['date_jst', TN], ['seller_sku', TN], ['child_asin', TN], ['sessions', I], ['page_views', I], ['buy_box_percentage', R],
      ['units_ordered', I], ['ordered_product_sales', R]],
    pk: ['date_jst', 'seller_sku', 'child_asin'],
    // 取得は直近 5 日を毎朝取り直す。月次は今月 + 前の月の全部の日 (85%) = 前の月の初め〜。月次の確定版は翌月の末までに出る前提で 100 日
    sendDays: 7, keepDays: 100, runDays: 10,
  },
  {
    entity: 'amazon_listing_status_daily', source: 'amazon_listing_status_daily',
    grain: 'one row = one (date_jst, sku_key) — 出品の状態の毎日の写し',
    dateCol: 'date_jst',
    cols: [['date_jst', TN], ['sku_key', TN], ['seller_sku', TN], ['asin', T], ['item_name', T], ['status', TN],
      ['is_buyable', IN], ['suppressed', IN], ['search_suppressed', IN], ['error_count', IN]],
    pk: ['date_jst', 'sku_key'],
    sendDays: 3, ...SNAPSHOT_KEEP, runDays: 5,
  },
  {
    entity: 'amazon_listing_issue_daily', source: 'amazon_listing_issue_daily',
    grain: 'one row = one (date_jst, sku_key, issue_key) — ERROR だけ',
    dateCol: 'date_jst',
    cols: [['date_jst', TN], ['sku_key', TN], ['issue_key', TN], ['code', TN], ['severity', TN], ['message', T]],
    pk: ['date_jst', 'sku_key', 'issue_key'],
    where: "severity = 'ERROR'",   // facts-amazon.js は ERROR だけ読む (WARNING / INFO は送らない)
    sendDays: 3, ...SNAPSHOT_KEEP, runDays: 5,
  },
  {
    entity: 'amazon_listing_fetch_runs', source: 'amazon_listing_fetch_runs',
    grain: 'one row = one date_jst — その日の写しが complete / partial か',
    dateCol: 'date_jst',
    cols: [['date_jst', T], ['status', TN]],
    pk: ['date_jst'],
    sendDays: 3, ...SNAPSHOT_KEEP, runDays: 400,
  },
  {
    entity: 'amazon_fba_returns', source: 'amazon_fba_returns',
    grain: 'one row = one 返品の明細 (row_key = 元の主キーのハッシュ)',
    dateCol: 'return_date_jst',
    rowKey: ['order_id', 'seller_sku', 'fnsku', 'return_date', 'license_plate_number', 'occurrence'],
    cols: [['row_key', TN], ['return_date_jst', TN], ['seller_sku', TN], ['asin', T], ['product_name', T], ['quantity', IN], ['reason', T]],
    pk: ['row_key'],
    // 取得は直近 60 日を UPSERT
    sendDays: 65, keepDays: 120, runDays: 400,
  },
  {
    entity: 'amazon_fba_reimbursements', source: 'amazon_fba_reimbursements',
    grain: 'one row = one 補てんの明細 (row_key = 元の主キーのハッシュ)',
    dateCol: 'approval_date_jst',
    rowKey: ['reimbursement_id', 'seller_sku', 'fnsku', 'occurrence'],
    cols: [['row_key', TN], ['approval_date_jst', TN], ['reason', T], ['amount_total', R], ['quantity_reimbursed_total', I]],
    pk: ['row_key'],
    sendDays: 65, keepDays: 120, runDays: 400,
  },
  {
    entity: 'sp_report_fetch_days', source: 'sp_report_fetch_days',
    grain: 'one row = one (report_type, date_jst) — SP-API のレポートが最後まで取れた日 (0 行の日も)',
    // 🚨 facts は「この記録がある日 = 0 件で取れた日」と読む = データの表の送りが失敗した回にこの記録だけ送ると「返品 0 件で正常」と誤る (Codex round2 P1)
    dependsOn: ['amazon_fba_returns', 'amazon_fba_reimbursements'],
    dateCol: 'date_jst',
    cols: [['report_type', TN], ['date_jst', TN]],
    pk: ['report_type', 'date_jst'],
    sendDays: 65, keepDays: 120, runDays: 400,
  },
  {
    entity: 'amazon_catalog_rank_daily', source: 'amazon_catalog_rank_daily',
    grain: 'one row = one (date_jst, marketplace_id, asin, rank_type, category_id) — BSR',
    dateCol: 'date_jst',
    cols: [['date_jst', TN], ['marketplace_id', TN], ['asin', TN], ['rank_type', TN], ['category_id', TN], ['category_title', T], ['rank', IN]],
    pk: ['date_jst', 'marketplace_id', 'asin', 'rank_type', 'category_id'],
    // 月次は今月と前の月の半分以上の日 = 前の月の後半から (翌月の末まで比べられる)。毎日つながった日が要る = 月末だけにはできない
    // 100 日 = 6 月の月次を 7/31 に作っても 5 月の 16 日以上が残る (75 日だと 5/18 からで 14 日しか無い。Codex round2 P2)
    sendDays: 3, keepDays: 100, runDays: 5,
  },
  {
    entity: 'fba_planning_daily', source: 'fba_planning_daily',
    grain: 'one row = one (snapshot_date, market, amazon_sku) — 在庫計画の毎日の写し (JP だけ)',
    dateCol: 'snapshot_date',
    cols: [['snapshot_date', TN], ['market', TN], ['amazon_sku', TN], ...PLANNING_READ_COLS],
    pk: ['snapshot_date', 'market', 'amazon_sku'],
    where: "market = 'jp'",   // facts-amazon.js は JP だけ読む
    sendDays: 3, ...SNAPSHOT_KEEP, runDays: 7,
  },
  {
    entity: 'fba_planning_daily_days', source: 'fba_planning_daily_days',
    grain: 'one row = one (snapshot_date, market) — 在庫計画の写しを最後まで取れた日 (JP だけ)',
    dateCol: 'snapshot_date',
    cols: [['snapshot_date', TN], ['market', TN]],
    pk: ['snapshot_date', 'market'],
    where: "market = 'jp'",
    sendDays: 3, ...SNAPSHOT_KEEP, runDays: 400,
  },
  {
    entity: 'fact_amazon_ads_sku_attrib', source: 'fact_amazon_ads_sku_attrib',
    grain: 'one row = one (date_jst, campaign_id, ad_group_id, target, target_granularity) — 広告の 14 日の帰属売上',
    dateCol: 'date_jst',
    cols: [['date_jst', TN], ['campaign_id', TN], ['ad_group_id', TN], ['target', TN], ['target_granularity', TN],
      ['advertised_sku', T], ['advertised_asin', T], ['clicks', IN], ['cost', RN], ['sales_14d', R], ['ingested_at', TN]],
    pk: ['date_jst', 'campaign_id', 'ad_group_id', 'target', 'target_granularity'],
    // 取得は直近 30 日を取り直す (帰属が後から伸びる) = 広告の mirror (sync-amazon-ads-daily.js) と同じ 35 日。月次はその月だけ = 翌月の末まで 75 日
    sendDays: 35, keepDays: 75, runDays: 10,
  },
  {
    entity: 'ads_attrib_fetch_days', source: 'ads_attrib_fetch_days',
    dependsOn: ['fact_amazon_ads_sku_attrib'],   // 上の sp_report_fetch_days と同じ理由 (広告の無い日 = 0 行で取れた日と読む)
    grain: 'one row = one (report_type, profile_id, date_jst) — 広告の帰属のレポートが取れた日と時刻',
    dateCol: 'date_jst',
    cols: [['report_type', TN], ['profile_id', TN], ['date_jst', TN], ['fetched_at', TN]],
    pk: ['report_type', 'profile_id', 'date_jst'],
    sendDays: 35, keepDays: 120, runDays: 400,
  },
].map((e) => Object.freeze({ ...e, table: `mirror_${e.source}`, clearMetaKey: `clear_${e.entity}_dates` })));

export const AMAZON_AI_ENTITY_NAMES = new Set(AMAZON_AI_MIRROR_ENTITIES.map((e) => e.entity));
export const amazonAiEntity = (name) => AMAZON_AI_MIRROR_ENTITIES.find((e) => e.entity === name) || null;
for (const e of AMAZON_AI_MIRROR_ENTITIES) {
  for (const [c] of e.cols) if (!/^[a-z0-9_]+$/.test(c)) throw new Error(`列名が不正: ${e.entity}.${c}`);   // SQL に埋めるので固定の英小文字だけ
  if (!e.cols.some(([c]) => c === e.dateCol)) throw new Error(`日付の列が送る列に無い: ${e.entity}.${e.dateCol}`);
  for (const k of e.pk) if (!e.cols.some(([c]) => c === k)) throw new Error(`主キーの列が送る列に無い: ${e.entity}.${k}`);
  if (!(e.sendDays >= 1 && e.keepDays >= e.sendDays && e.runDays >= 1)) throw new Error(`日数が不正: ${e.entity}`);
}

// ── 日付 (YYYY-MM-DD・UTC で数える = JST の日付の文字列をそのまま足し引き) ──
export function addDaysYmd(ymd, delta) {
  const d = new Date(`${ymd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + delta);
  return d.toISOString().slice(0, 10);
}
export function datesBetween(from, to) {
  const out = [];
  for (let d = from; d <= to; d = addDaysYmd(d, 1)) out.push(d);
  return out;
}
/** 月の最後の MONTH_END_KEEP_DAYS 日か */
export function isMonthEndDay(ymd) {
  const [y, m, d] = ymd.split('-').map(Number);
  const last = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return d > last - MONTH_END_KEEP_DAYS;
}

/** 残す期間 (scopeTo = その run の最後の日)。keepFrom より前は消す / recentFrom より前は月末の日だけ残す */
export function retentionBounds(spec, scopeTo) {
  return {
    keepFrom: addDaysYmd(scopeTo, -(spec.keepDays - 1)),
    recentFrom: spec.keepRecentDays ? addDaysYmd(scopeTo, -(spec.keepRecentDays - 1)) : null,
  };
}
/** その日の行を mirror に残すか (送り手が送る前に絞るのと、受け手が消すのを同じ決まりにする) */
export function isKeptDate(spec, ymd, scopeTo) {
  const { keepFrom, recentFrom } = retentionBounds(spec, scopeTo);
  if (ymd < keepFrom) return false;
  if (recentFrom && ymd < recentFrom) return isMonthEndDay(ymd);
  return true;
}
/**
 * 受け手が消す SQL (keepFrom / recentFrom を名前つきで渡す)。月の最後の 4 日 = date(日, 'start of month', '+1 month', '-4 days') 以降
 * 🚨 isKeptDate と同じ決まり (試験で突き合わせる)
 */
export function retentionDeleteSql(spec) {
  const c = spec.dateCol;
  const monthEnd = `date(${c}, 'start of month', '+1 month', '-${MONTH_END_KEEP_DAYS} days')`;
  return spec.keepRecentDays
    ? `DELETE FROM ${spec.table} WHERE ${c} < @keepFrom OR (${c} < @recentFrom AND ${c} < ${monthEnd})`
    : `DELETE FROM ${spec.table} WHERE ${c} < @keepFrom`;
}

/** mirror の表の DDL (列の型は元の表のまま + 監査の 3 列) */
export function mirrorTableDdl(spec) {
  return `CREATE TABLE IF NOT EXISTS ${spec.table} (
    ${spec.cols.map(([c, type]) => `${c} ${type}`).join(',\n    ')},
    source_run_id   TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    synced_at       TEXT NOT NULL,
    PRIMARY KEY (${spec.pk.join(', ')})
  )`;
}
/** 日付の列が主キーの先頭でなければ日付の索引 (日ごとの置き換え・古い行の掃除・facts の期間の読み) */
export function mirrorIndexDdl(spec) {
  return spec.pk[0] === spec.dateCol ? [] : [`CREATE INDEX IF NOT EXISTS idx_${spec.table}_date ON ${spec.table}(${spec.dateCol})`];
}

/** sync_contracts に入れる行 (warehouse.db。送り手の版の確かめ) */
export function contractRows() {
  return AMAZON_AI_MIRROR_ENTITIES.map((e) => ({
    entity: e.entity,
    contract_version: AMAZON_AI_CONTRACT_VERSION,
    source_system: 'minipc-warehouse',
    source_object: e.source,
    target_table: e.table,
    grain_definition: e.grain,
    key_columns_json: JSON.stringify(e.pk),
    payload_schema_json: JSON.stringify({
      columns: e.cols.map(([c]) => c), date_col: e.dateCol, where: e.where || null, row_key_of: e.rowKey || null,
      send_days: e.sendDays, keep_days: e.keepDays, keep_recent_days: e.keepRecentDays || null,
    }),
    clear_strategy: 'scope_clear_per_run',
    apply_mode: 'insert_or_replace',
    owner: AMAZON_AI_OWNER,
  }));
}
/** sync_contracts へ UPSERT (冪等。warehouse の initDB と送り手の両方から呼ぶ) */
export function seedAmazonAiMirrorContracts(db) {
  const st = db.prepare(`
    INSERT INTO sync_contracts (
      entity, contract_version, source_system, source_object, target_table,
      grain_definition, key_columns_json, payload_schema_json,
      clear_strategy, apply_mode, enabled, owner, created_at, updated_at
    ) VALUES (
      @entity, @contract_version, @source_system, @source_object, @target_table,
      @grain_definition, @key_columns_json, @payload_schema_json,
      @clear_strategy, @apply_mode, 1, @owner,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(entity) DO UPDATE SET
      contract_version    = excluded.contract_version,
      source_system       = excluded.source_system,
      source_object       = excluded.source_object,
      target_table        = excluded.target_table,
      grain_definition    = excluded.grain_definition,
      key_columns_json    = excluded.key_columns_json,
      payload_schema_json = excluded.payload_schema_json,
      clear_strategy      = excluded.clear_strategy,
      apply_mode          = excluded.apply_mode,
      enabled             = excluded.enabled,
      owner               = excluded.owner,
      updated_at          = excluded.updated_at`);
  db.transaction(() => { for (const r of contractRows()) st.run(r); })();
}
