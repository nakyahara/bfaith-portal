/**
 * rakuten-analytics rules.js — 診断ルールエンジン + アクションキュー (AI異常検知の土台)
 *
 * 設計 (2026-07-07 Codex 2ラウンド意見交換で確定):
 *   - 5分離: rule_run (実行記録) / observation (診断値 append-only) / action (打ち手キュー) /
 *     action_event (状態遷移ログ append-only) / intervention (実施記録)
 *   - dedup: fingerprint = rule_id × entity_type × entity_id × dimension。
 *     open/acked 中の再検知 = 同一 action を更新 (last_seen/occurrence_count/最新スナップショット、
 *     severity 昇格は action_event も追加) / done 後の再発 = 新規 action /
 *     dismissed = dismiss_scope (occurrence|until_date|permanent) と suppress_until を尊重
 *   - AI レーン: source='ai' は「提案の作成のみ」。人の ack/dismiss/実施は変更不可。
 *     idempotency_key 必須 + 来歴 (ai_meta_json: model/confidence 等) 保存。
 *     ルール実行 (SQL・決定的) と AI 提案 (将来 cron Claude) は別レーンで同じ actions に合流する
 *   - 実行: lazy (画面アクセス時に当日 JST 未実行なら実行)。UNIQUE(run_date_jst, rule_set_version)
 *     で二重実行を防止。stale running (>10分) は再実行可能。手動再実行あり。
 *     ※ ルールは better-sqlite3 の同期 SQL 集計 (数百SKU×90日規模、ms〜秒オーダー) なので
 *       リクエスト内同期実行とし、rule_run に成否を記録する (デプロイ再起動での中断も次回回収)
 *
 * 使用 mirror (読取のみ、mall-csv-fetcher が毎朝更新):
 *   mirror_rakuten_finance_sku_daily / mirror_rakuten_ads_rpp (SKU×月次) /
 *   mirror_rakuten_ads_rpp_daily (店舗×日次) / mirror_rakuten_item_daily (SKU×日次アクセス/CVR/在庫) /
 *   mirror_rakuten_store_daily / mirror_rakuten_campaigns (楽天イベント日程)
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { jstToday, addDays, monthOf, addMonths, monthStart, monthEnd } from './queries.js';

export const RULE_SET_VERSION = 'v1';

// ─── テーブル ───
let _initialized = false;
export function ensureRadashTables() {
  if (_initialized) return;
  const db = getMirrorDB();

  // 実行記録 (lazy claim の一意性もこの表で守る)
  db.exec(`CREATE TABLE IF NOT EXISTS radash_rule_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date_jst      TEXT NOT NULL,
    rule_set_version  TEXT NOT NULL,
    trigger_kind      TEXT NOT NULL CHECK (trigger_kind IN ('lazy','manual')),
    status            TEXT NOT NULL CHECK (status IN ('running','succeeded','failed')),
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    stats_json        TEXT NOT NULL DEFAULT '{}',
    error             TEXT,
    UNIQUE (run_date_jst, rule_set_version)
  )`);

  // 診断値 (append-only)。AI の学習材料 + 再現性の正本
  db.exec(`CREATE TABLE IF NOT EXISTS radash_observations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        INTEGER NOT NULL,
    rule_id       TEXT NOT NULL,
    entity_type   TEXT NOT NULL,
    entity_id     TEXT NOT NULL,
    dimension     TEXT NOT NULL DEFAULT '',
    verdict       TEXT NOT NULL,
    severity      TEXT NOT NULL CHECK (severity IN ('info','warning','critical')),
    metrics_json  TEXT NOT NULL DEFAULT '{}',
    observed_at   TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_radash_obs_entity ON radash_observations(rule_id, entity_type, entity_id, observed_at)`);

  // アクションキュー (PM の運転席 + 将来 AI 提案の合流先)
  db.exec(`CREATE TABLE IF NOT EXISTS radash_actions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint      TEXT NOT NULL,
    source           TEXT NOT NULL CHECK (source IN ('rule','ai','human')),
    rule_id          TEXT NOT NULL DEFAULT '',
    entity_type      TEXT NOT NULL,
    entity_id        TEXT NOT NULL,
    entity_label     TEXT NOT NULL DEFAULT '',
    dimension        TEXT NOT NULL DEFAULT '',
    severity         TEXT NOT NULL CHECK (severity IN ('info','warning','critical')),
    title            TEXT NOT NULL,
    evidence_json    TEXT NOT NULL DEFAULT '{}',
    suggested_action TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL CHECK (status IN ('open','acked','done','dismissed','expired','resolved')),
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    latest_observation_id INTEGER,
    dismiss_scope    TEXT CHECK (dismiss_scope IN ('occurrence','until_date','permanent')),
    dismiss_reason   TEXT,
    suppress_until   TEXT,
    idempotency_key  TEXT,
    ai_meta_json     TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_radash_act_fp ON radash_actions(fingerprint, status)`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_radash_act_idem ON radash_actions(idempotency_key) WHERE idempotency_key IS NOT NULL`);
  // 同一案件の「対応待ち」は常に1件 (部分UNIQUE。reopen/再発の重複をDBレベルで禁止 — Codex R3 High)
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_radash_act_active ON radash_actions(fingerprint) WHERE status IN ('open','acked')`);

  // 状態遷移ログ (append-only): 誰が/いつ/何→何/理由/actor種別。AIの最重要学習材料
  db.exec(`CREATE TABLE IF NOT EXISTS radash_action_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    action_id    INTEGER NOT NULL,
    event        TEXT NOT NULL CHECK (event IN (
      'created','reseen','severity_changed','acked','done','dismissed','reopened','expired','resolved'
    )),
    from_status  TEXT,
    to_status    TEXT,
    actor_kind   TEXT NOT NULL CHECK (actor_kind IN ('rule','ai','human')),
    actor        TEXT NOT NULL DEFAULT '',
    note         TEXT NOT NULL DEFAULT '',
    at           TEXT NOT NULL
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_radash_ev_action ON radash_action_events(action_id, at)`);

  // 実施記録 (施策 PDCA の正本。効果測定はここを起点に前後比較)
  db.exec(`CREATE TABLE IF NOT EXISTS radash_interventions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    action_id        INTEGER,
    entity_type      TEXT NOT NULL,
    entity_id        TEXT NOT NULL,
    implemented_date TEXT NOT NULL,
    description      TEXT NOT NULL DEFAULT '',
    created_by       TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL
  )`);

  // 閾値設定 (診断ルール用。設定タブで変更可)
  db.exec(`CREATE TABLE IF NOT EXISTS radash_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);
  _initialized = true;
}

// ─── 閾値 (デフォルト。radash_settings で上書き可) ───
export const DEFAULT_THRESHOLDS = {
  freshness_lag_days: 3,          // R1: mirror データが N 日以上古ければ警告
  ad_min_spend_yen: 3000,         // R2: 月広告費がこれ未満の SKU は判定対象外 (ノイズ除去)
  ad_expand_roas_ratio: 1.5,      // R2: 損益分岐×N 以上で増額候補
  cvr_min_access_7d: 100,         // R3: 直近7日アクセスがこれ未満は判定しない
  cvr_drop_warn_ratio: 0.6,       // R3: CVR7d < CVR28d×N で警告
  cvr_drop_crit_ratio: 0.4,       // R3: 同 critical
  stock_days_threshold: 10,       // R4: 在庫残日数がこれ未満で広告出稿中なら警告
  dismiss_occurrence_days: 7,     // dismiss(今回のみ) の抑制日数
};

export function getThresholds() {
  ensureRadashTables();
  const db = getMirrorDB();
  const out = { ...DEFAULT_THRESHOLDS };
  for (const r of db.prepare(`SELECT key, value FROM radash_settings`).all()) {
    if (r.key in out) {
      const n = Number(r.value);
      if (Number.isFinite(n)) out[r.key] = n;
    }
  }
  return out;
}
// キー別の許容範囲 (範囲外は保存拒否 — 負値や矛盾値で診断が無効化/大量発火する事故防止)
const THRESHOLD_BOUNDS = {
  freshness_lag_days: [1, 30],
  ad_min_spend_yen: [0, 1000000],
  ad_expand_roas_ratio: [1.1, 10],
  cvr_min_access_7d: [10, 100000],
  cvr_drop_warn_ratio: [0.05, 0.95],
  cvr_drop_crit_ratio: [0.01, 0.9],
  stock_days_threshold: [1, 60],
  dismiss_occurrence_days: [1, 90],
};

export function saveThresholds(patch) {
  ensureRadashTables();
  const db = getMirrorDB();
  const now = new Date().toISOString();
  const rejected = [];
  const stmt = db.prepare(`INSERT INTO radash_settings (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`);
  const merged = { ...getThresholds() };
  for (const [k, v] of Object.entries(patch)) {
    if (!(k in DEFAULT_THRESHOLDS)) continue;
    const n = Number(v);
    const [lo, hi] = THRESHOLD_BOUNDS[k];
    if (!Number.isFinite(n) || n < lo || n > hi) { rejected.push(`${k} (許容 ${lo}〜${hi})`); continue; }
    merged[k] = n;
  }
  // 相関条件: critical 閾値は warning より厳しく (小さく)
  if (merged.cvr_drop_crit_ratio >= merged.cvr_drop_warn_ratio) {
    rejected.push('cvr_drop_crit_ratio は cvr_drop_warn_ratio より小さくしてください');
  } else {
    const tx = db.transaction(() => {
      for (const [k, v] of Object.entries(merged)) {
        if (k in DEFAULT_THRESHOLDS) stmt.run(k, String(v), now);
      }
    });
    tx();
  }
  return { ...getThresholds(), rejected };
}

// ─── ルール定義 ───
// 各ルールは observations の配列を返す:
//   { entityType, entityId, entityLabel, dimension, verdict, severity, metrics,
//     actionable: bool, title, suggestedAction }
// actionable=true の observation だけが actions キューへ流れる (verdict='ok' は履歴のみ)

// needs: 依存データセット。停滞中は「解消sweep」をスキップする (データ断で検知ゼロ →
// 既存アクションを誤って解消済みにする事故の防止)
const RULES = [
  { id: 'data_freshness', label: 'データ鮮度', run: ruleDataFreshness, needs: [] },
  { id: 'ad_breakeven', label: '損益分岐ROAS', run: ruleAdBreakeven, needs: ['ads_rpp', 'finance'] },
  { id: 'cvr_drop', label: 'CVR急落', run: ruleCvrDrop, needs: ['item_daily'] },
  { id: 'stock_ads', label: '在庫切れ間近×広告', run: ruleStockAds, needs: ['item_daily', 'ads_rpp'] },
];

// データセット定義 (R1 とデータ鮮度タブの共通土台)
export const DATASETS = [
  { key: 'finance', table: 'mirror_rakuten_finance_sku_daily', dateCol: 'date_jst', label: '利益fact (受注ベース)', source: 'daily-sync 07:00' },
  { key: 'ads_rpp', table: 'mirror_rakuten_ads_rpp', dateCol: 'month_ym', label: 'RPP広告 (SKU×月次)', source: 'mall-csv-fetcher', monthly: true },
  { key: 'ads_rpp_daily', table: 'mirror_rakuten_ads_rpp_daily', dateCol: 'date_jst', label: 'RPP広告 (店舗×日次)', source: 'mall-csv-fetcher' },
  { key: 'item_daily', table: 'mirror_rakuten_item_daily', dateCol: 'date_jst', label: '商品分析 (アクセス/CVR/在庫)', source: 'mall-csv-fetcher' },
  { key: 'store_daily', table: 'mirror_rakuten_store_daily', dateCol: 'date_jst', label: '店舗日次 (端末別+商圏ベンチ)', source: 'mall-csv-fetcher' },
  // イベント日程は散発データ (開催があった日だけ) → 日次遅延では停滞判定しない
  { key: 'campaigns', table: 'mirror_rakuten_campaigns', dateCol: 'date_jst', label: '楽天イベント日程', source: 'mall-csv-fetcher', sporadic: true },
];

export function getDatasetFreshness() {
  const db = getMirrorDB();
  const today = jstToday();
  const th = getThresholds();
  return DATASETS.map(ds => {
    let latest = null, rows = 0;
    try {
      const r = db.prepare(`SELECT MAX(${ds.dateCol}) AS d, COUNT(*) AS c FROM ${ds.table}`).get();
      latest = r.d; rows = r.c;
    } catch { /* テーブル未作成 = mirror 初期化前 */ }
    // 月次データは「当月 or 前月があれば新鮮」扱い
    let lagDays = null, stale = false;
    if (latest) {
      const latestDate = ds.monthly ? `${latest}-01` : latest;
      lagDays = Math.round((new Date(today + 'T00:00:00Z') - new Date(latestDate + 'T00:00:00Z')) / 86400000);
      stale = ds.sporadic ? false
        : ds.monthly ? latest < addMonths(monthOf(today), -1)
        : lagDays > th.freshness_lag_days;
    } else {
      // sporadic (イベント日程) は 0 行 = 「開催なし」もあり得るため停滞扱いしない
      stale = !ds.sporadic;
    }
    return { ...ds, latest, rows, lag_days: lagDays, stale };
  });
}

// R1: データ鮮度 — mirror が止まったらまずこれが知らせる (他ルールの誤検知も防ぐ)
function ruleDataFreshness() {
  const out = [];
  for (const ds of getDatasetFreshness()) {
    const stale = ds.stale;
    out.push({
      entityType: 'dataset', entityId: ds.key, entityLabel: ds.label, dimension: 'freshness',
      verdict: stale ? 'stale' : 'ok',
      severity: stale ? (ds.rows === 0 ? 'critical' : 'warning') : 'info',
      metrics: { latest: ds.latest, rows: ds.rows, lag_days: ds.lag_days },
      actionable: stale,
      title: ds.rows === 0
        ? `${ds.label} が未同期 (0行)`
        : `${ds.label} が ${ds.lag_days}日 更新されていません (最新: ${ds.latest})`,
      suggestedAction: 'mall-csv-fetcher / daily-sync のログとGChat通知を確認。自動DLが壊れている場合はRMSから手動DLしてminiPCのincoming/へ',
    });
  }
  return out;
}

// SKU→商品名の解決 (item_daily 最新 → finance fact)
function skuNameMap(db) {
  const map = new Map();
  for (const r of db.prepare(`
    SELECT item_manage_number AS k, item_name AS n FROM mirror_rakuten_item_daily
    WHERE item_name IS NOT NULL AND item_name <> ''
    GROUP BY item_manage_number HAVING date_jst = MAX(date_jst)
  `).all()) map.set(r.k.toLowerCase(), r.n);
  for (const r of db.prepare(`
    SELECT LOWER(rakuten_code) AS k, MAX(product_name) AS n FROM mirror_rakuten_finance_sku_daily
    WHERE product_name <> '' GROUP BY LOWER(rakuten_code)
  `).all()) { if (!map.has(r.k)) map.set(r.k, r.n); }
  return map;
}

// R2: 損益分岐ROAS — 実ROAS(720h) vs 損益分岐ROAS = 100 ÷ 変動利益率
// 損益分岐ROAS = 「広告費1円が売上何円を生めば変動利益トントンか」。fact の変動利益率から SKU×月別に自動計算
function ruleAdBreakeven(th) {
  const db = getMirrorDB();
  const names = skuNameMap(db);
  // 判定対象月 = 広告データがある最新月 (当月は月初でデータが薄いので、当月+前月の合算で安定させる)
  const months = db.prepare(`SELECT DISTINCT month_ym FROM mirror_rakuten_ads_rpp ORDER BY month_ym DESC LIMIT 2`).all().map(r => r.month_ym);
  if (months.length === 0) return [];
  const ph = months.map(() => '?').join(',');
  const rows = db.prepare(`
    WITH ads AS (
      SELECT LOWER(item_manage_number) AS sku,
             SUM(ad_cost_yen) AS ad_cost, SUM(sales_720h_yen) AS ad_sales,
             SUM(clicks) AS clicks, SUM(orders_720h) AS ad_orders
      FROM mirror_rakuten_ads_rpp WHERE month_ym IN (${ph})
      GROUP BY LOWER(item_manage_number)
    ), fin AS (
      SELECT LOWER(rakuten_code) AS sku,
             SUM(gross_sales_jpy_incl) AS gross, SUM(variable_margin_jpy_incl) AS vm
      FROM mirror_rakuten_finance_sku_daily
      WHERE substr(date_jst,1,7) IN (${ph})
      GROUP BY LOWER(rakuten_code)
    )
    SELECT ads.sku, ads.ad_cost, ads.ad_sales, ads.clicks, ads.ad_orders, fin.gross, fin.vm
    FROM ads LEFT JOIN fin ON fin.sku = ads.sku
    WHERE ads.ad_cost >= ?
  `).all(...months, ...months, th.ad_min_spend_yen);

  const out = [];
  for (const r of rows) {
    const roas = r.ad_cost > 0 ? (r.ad_sales / r.ad_cost) * 100 : null;
    const hasFact = r.gross !== null && r.gross > 0;
    const vmRate = hasFact && r.vm > 0 ? r.vm / r.gross : null;
    const beRoas = vmRate ? 100 / vmRate : null;   // % 表記 (例 変動利益率30% → 333%)
    const metrics = {
      months, ad_cost: Math.round(r.ad_cost), ad_sales: Math.round(r.ad_sales),
      clicks: r.clicks, ad_orders: r.ad_orders,
      roas_pct: roas === null ? null : Math.round(roas * 10) / 10,
      breakeven_roas_pct: beRoas === null ? null : Math.round(beRoas * 10) / 10,
      vm_rate_pct: vmRate === null ? null : Math.round(vmRate * 1000) / 10,
    };
    const label = names.get(r.sku) || '';
    if (!hasFact) {
      // 利益factと突合できない — 判定不能として記録のみ。
      // preserve: 既存アクションの解消sweep対象にはしない (「判定できない」≠「条件解消」 — Codex R4 High)。
      // このルールは ad_breakeven / ad_expand の2次元でアクションを持つため両方を守る (Codex R5 High)
      out.push({
        entityType: 'sku', entityId: r.sku, entityLabel: label, dimension: 'ad_breakeven',
        verdict: 'no_baseline', severity: 'info', metrics, actionable: false, preserve: true,
      });
      out.push({
        entityType: 'sku', entityId: r.sku, entityLabel: label, dimension: 'ad_expand',
        verdict: 'no_baseline', severity: 'info', metrics, actionable: false, preserve: true,
      });
      continue;
    }
    if (r.vm <= 0) {
      // 変動利益ゼロ以下の商品に広告出稿 = 広告費は全額損失 (売れるほど赤字が膨らむ)
      out.push({
        entityType: 'sku', entityId: r.sku, entityLabel: label, dimension: 'ad_breakeven',
        verdict: 'bleed', severity: 'critical',
        metrics: { ...metrics, vm_total: Math.round(r.vm), est_ad_loss_yen: Math.round(r.ad_cost) },
        actionable: true,
        title: `変動利益ゼロ以下のSKUに広告出稿中 (広告費 ¥${Math.round(r.ad_cost).toLocaleString()} 全額が損失)`,
        suggestedAction: 'RPP除外を強く推奨。売れるほど赤字 — 価格/原価/送料の見直しが先',
      });
      continue;
    }
    if (roas < beRoas) {
      const estLoss = Math.round(r.ad_cost - r.ad_sales * vmRate);   // 広告費 − 広告経由売上が生んだ変動利益
      out.push({
        entityType: 'sku', entityId: r.sku, entityLabel: label, dimension: 'ad_breakeven',
        verdict: 'bleed', severity: roas < beRoas * 0.5 ? 'critical' : 'warning',
        metrics: { ...metrics, est_ad_loss_yen: estLoss },
        actionable: true,
        title: `広告赤字: 実ROAS ${metrics.roas_pct}% < 損益分岐 ${metrics.breakeven_roas_pct}% (広告費 ¥${metrics.ad_cost.toLocaleString()})`,
        suggestedAction: `RPPで商品CPCの引き下げ or 除外を検討。推定広告損失 ¥${estLoss.toLocaleString()} (${months.join('+')})`,
      });
    } else if (roas >= beRoas * th.ad_expand_roas_ratio) {
      out.push({
        entityType: 'sku', entityId: r.sku, entityLabel: label, dimension: 'ad_expand',
        verdict: 'expand', severity: 'info', metrics, actionable: true,
        title: `広告伸ばし先候補: 実ROAS ${metrics.roas_pct}% ≥ 損益分岐×${th.ad_expand_roas_ratio} (${metrics.breakeven_roas_pct}%)`,
        suggestedAction: 'RPPで商品CPCの引き上げを検討 (利益余裕が大きい)。在庫残と相談',
      });
    } else {
      out.push({
        entityType: 'sku', entityId: r.sku, entityLabel: label, dimension: 'ad_breakeven',
        verdict: 'ok', severity: 'info', metrics, actionable: false,
      });
    }
  }
  return out;
}

// R3: CVR急落 — 直近7日 vs 直前28日 (アクセスは十分あるのに売れなくなった = ページ/価格/レビュー/競合の異変)
function ruleCvrDrop(th) {
  const db = getMirrorDB();
  const latest = db.prepare(`SELECT MAX(date_jst) AS d FROM mirror_rakuten_item_daily`).get()?.d;
  if (!latest) return [];
  const d7from = addDays(latest, -6);
  const d28from = addDays(latest, -34);
  const d28to = addDays(latest, -7);
  const rows = db.prepare(`
    WITH cur AS (
      SELECT item_manage_number AS sku, MAX(item_name) AS name,
             SUM(access_users) AS access, SUM(orders) AS orders
      FROM mirror_rakuten_item_daily WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY item_manage_number
    ), base AS (
      SELECT item_manage_number AS sku, SUM(access_users) AS access, SUM(orders) AS orders
      FROM mirror_rakuten_item_daily WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY item_manage_number
    )
    SELECT cur.sku, cur.name, cur.access AS a7, cur.orders AS o7, base.access AS a28, base.orders AS o28
    FROM cur JOIN base ON base.sku = cur.sku
    WHERE cur.access >= ? AND base.access > 0 AND base.orders > 0
  `).all(d7from, latest, d28from, d28to, th.cvr_min_access_7d);

  const out = [];
  for (const r of rows) {
    const cvr7 = r.o7 / r.a7;
    const cvr28 = r.o28 / r.a28;
    const ratio = cvr28 > 0 ? cvr7 / cvr28 : null;
    if (ratio === null) continue;
    const metrics = {
      window: `${d7from}〜${latest} vs ${d28from}〜${d28to}`,
      access_7d: r.a7, orders_7d: r.o7, cvr_7d_pct: Math.round(cvr7 * 10000) / 100,
      access_28d: r.a28, orders_28d: r.o28, cvr_28d_pct: Math.round(cvr28 * 10000) / 100,
      ratio: Math.round(ratio * 100) / 100,
    };
    if (ratio < th.cvr_drop_warn_ratio) {
      out.push({
        entityType: 'sku', entityId: r.sku.toLowerCase(), entityLabel: r.name || '', dimension: 'cvr_drop',
        verdict: 'drop', severity: ratio < th.cvr_drop_crit_ratio ? 'critical' : 'warning',
        metrics, actionable: true,
        title: `CVR急落: 直近7日 ${metrics.cvr_7d_pct}% (基準28日 ${metrics.cvr_28d_pct}% の ${Math.round(ratio * 100)}%)`,
        suggestedAction: 'アクセスはあるのに売れていない。価格/画像/レビュー/在庫表示/競合の変化を確認。イベント谷間の可能性も比較',
      });
    }
  }
  return out;
}

// R4: 在庫切れ間近×広告出稿中 — 売れて在庫が尽きる商品に広告費を使うのは無駄弾
function ruleStockAds(th) {
  const db = getMirrorDB();
  const latest = db.prepare(`SELECT MAX(date_jst) AS d FROM mirror_rakuten_item_daily`).get()?.d;
  if (!latest) return [];
  const curYm = monthOf(jstToday());
  const months = [curYm, addMonths(curYm, -1)];
  const rows = db.prepare(`
    WITH latest_stock AS (
      SELECT item_manage_number AS sku, MAX(item_name) AS name, stock_qty
      FROM mirror_rakuten_item_daily WHERE date_jst = ? AND stock_qty IS NOT NULL
      GROUP BY item_manage_number
    ), velocity AS (
      SELECT item_manage_number AS sku, SUM(units) * 1.0 / 14 AS daily_units
      FROM mirror_rakuten_item_daily WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY item_manage_number
    ), ads AS (
      SELECT LOWER(item_manage_number) AS sku, SUM(ad_cost_yen) AS ad_cost
      FROM mirror_rakuten_ads_rpp WHERE month_ym IN (?, ?)
      GROUP BY LOWER(item_manage_number)
    )
    SELECT s.sku, s.name, s.stock_qty, v.daily_units, a.ad_cost
    FROM latest_stock s
    JOIN velocity v ON v.sku = s.sku AND v.daily_units > 0
    JOIN ads a ON a.sku = LOWER(s.sku) AND a.ad_cost > 0
  `).all(latest, addDays(latest, -13), latest, ...months);

  const out = [];
  for (const r of rows) {
    const daysLeft = r.stock_qty / r.daily_units;
    if (daysLeft >= th.stock_days_threshold) continue;
    out.push({
      entityType: 'sku', entityId: r.sku.toLowerCase(), entityLabel: r.name || '', dimension: 'stock_ads',
      verdict: 'stock_low_with_ads',
      severity: daysLeft < th.stock_days_threshold / 2 ? 'critical' : 'warning',
      metrics: {
        as_of: latest, stock_qty: r.stock_qty,
        daily_units: Math.round(r.daily_units * 10) / 10,
        days_left: Math.round(daysLeft * 10) / 10,
        ad_cost_recent: Math.round(r.ad_cost),
      },
      actionable: true,
      title: `在庫残 約${Math.round(daysLeft)}日なのに広告出稿中 (在庫${r.stock_qty}個、広告費 ¥${Math.round(r.ad_cost).toLocaleString()})`,
      suggestedAction: '補充手配 or RPP除外 (在庫切れ後の広告費は無駄弾+在庫切れページへの誘導)。補充見込みがあるなら維持',
    });
  }
  return out;
}

// ─── 実行エンジン ───

// 当日 JST の succeeded run があればそれを返し、無ければ claim して実行する。
// force=true (手動再実行) は当日 run の有無に関わらず既存 run を上書き実行
export function ensureDailyRun({ trigger = 'lazy', actor = '', force = false } = {}) {
  ensureRadashTables();
  const db = getMirrorDB();
  const today = jstToday();
  const now = () => new Date().toISOString();

  const existing = db.prepare(`SELECT * FROM radash_rule_runs WHERE run_date_jst = ? AND rule_set_version = ?`)
    .get(today, RULE_SET_VERSION);
  if (existing) {
    if (existing.status === 'running') {
      const ageMin = (Date.now() - Date.parse(existing.started_at)) / 60000;
      // 実行中は force でも割り込まない (並行実行の競合防止 — Codex R3 Critical)。stale (>10分) のみ回収
      if (ageMin < 10) return { run: existing, executed: false };
    }
    if (existing.status === 'succeeded' && !force) return { run: existing, executed: false };
    // failed / stale running / force+succeeded → 再実行
  }

  // claim (条件付き UPDATE / UNIQUE 制約で並行アクセスの二重実行を防ぐ)
  let runId;
  if (existing) {
    const staleCutoff = new Date(Date.now() - 10 * 60000).toISOString();
    const claimed = db.prepare(`UPDATE radash_rule_runs
      SET status='running', trigger_kind=?, started_at=?, finished_at=NULL, error=NULL
      WHERE id=? AND (status != 'running' OR started_at < ?)`)
      .run(trigger, now(), existing.id, staleCutoff);
    if (claimed.changes === 0) return { run: existing, executed: false };   // 別リクエストが実行中
    runId = existing.id;
  } else {
    try {
      runId = db.prepare(`INSERT INTO radash_rule_runs (run_date_jst, rule_set_version, trigger_kind, status, started_at)
        VALUES (?, ?, ?, 'running', ?)`).run(today, RULE_SET_VERSION, trigger, now()).lastInsertRowid;
    } catch (e) {
      // UNIQUE 競合 (並行リクエストが先に claim) のみ握る。他の DB 障害は上へ (Codex R4 Low)
      if (!String(e.code || '').startsWith('SQLITE_CONSTRAINT')) throw e;
      return { run: db.prepare(`SELECT * FROM radash_rule_runs WHERE run_date_jst=? AND rule_set_version=?`).get(today, RULE_SET_VERSION), executed: false };
    }
  }

  // 実行 (同期。ルールは ms〜秒オーダーの SQL 集計)
  const th = getThresholds();
  const stats = { rules: {}, actions_created: 0, actions_updated: 0, actions_resolved: 0, sweep_skipped: [] };
  try {
    const staleKeys = new Set(getDatasetFreshness().filter(d => d.stale).map(d => d.key));
    for (const rule of RULES) {
      const obs = rule.run(th) || [];
      stats.rules[rule.id] = obs.length;
      const applied = applyObservations(db, runId, rule.id, obs, today);
      stats.actions_created += applied.created;
      stats.actions_updated += applied.updated;
      // 解消 sweep: 今回検知されなかった open/acked (ルール由来) を resolved に (Codex R3 High)。
      // 依存データが停滞中はスキップ (データ断を「解消」と誤認しない)
      if ((rule.needs || []).some(k => staleKeys.has(k))) {
        stats.sweep_skipped.push(rule.id);
        continue;
      }
      const active = db.prepare(`SELECT id, fingerprint, status FROM radash_actions
        WHERE rule_id=? AND source='rule' AND status IN ('open','acked')`).all(rule.id);
      for (const a of active) {
        if (applied.fingerprints.has(a.fingerprint)) continue;
        db.prepare(`UPDATE radash_actions SET status='resolved', updated_at=? WHERE id=?`).run(now(), a.id);
        db.prepare(`INSERT INTO radash_action_events (action_id, event, from_status, to_status, actor_kind, actor, note, at)
          VALUES (?, 'resolved', ?, 'resolved', 'rule', '', '条件解消 (今回の診断で検知されず)', ?)`)
          .run(a.id, a.status, now());
        stats.actions_resolved++;
      }
    }
    db.prepare(`UPDATE radash_rule_runs SET status='succeeded', finished_at=?, stats_json=? WHERE id=?`)
      .run(now(), JSON.stringify(stats), runId);
  } catch (e) {
    db.prepare(`UPDATE radash_rule_runs SET status='failed', finished_at=?, error=? WHERE id=?`)
      .run(now(), String(e.stack || e.message).slice(0, 1000), runId);
    throw e;
  }
  return { run: db.prepare(`SELECT * FROM radash_rule_runs WHERE id=?`).get(runId), executed: true };
}

// observation の永続化 + actions への反映 (dedup/ライフサイクル — Codex R2 の表に従う)
function applyObservations(db, runId, ruleId, observations, today) {
  const nowIso = new Date().toISOString();
  let created = 0, updated = 0;
  const insObs = db.prepare(`INSERT INTO radash_observations
    (run_id, rule_id, entity_type, entity_id, dimension, verdict, severity, metrics_json, observed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  const insAction = db.prepare(`INSERT INTO radash_actions
    (fingerprint, source, rule_id, entity_type, entity_id, entity_label, dimension, severity, title,
     evidence_json, suggested_action, status, occurrence_count, first_seen_at, last_seen_at,
     latest_observation_id, created_at, updated_at)
    VALUES (?, 'rule', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', 1, ?, ?, ?, ?, ?)`);
  const insEvent = db.prepare(`INSERT INTO radash_action_events
    (action_id, event, from_status, to_status, actor_kind, actor, note, at)
    VALUES (?, ?, ?, ?, 'rule', ?, ?, ?)`);

  const sevRank = { info: 0, warning: 1, critical: 2 };
  const fingerprints = new Set();
  const tx = db.transaction(() => {
    for (const o of observations) {
      const obsId = insObs.run(runId, ruleId, o.entityType, o.entityId, o.dimension || '',
        o.verdict, o.severity, JSON.stringify(o.metrics || {}), nowIso).lastInsertRowid;
      const fp = `${ruleId}\x1f${o.entityType}\x1f${o.entityId}\x1f${o.dimension || ''}`;
      // sweep の「検知済み」集合: actionable に加え preserve (判定不能 = 解消と断定できない) も入れる
      if (o.actionable || o.preserve) fingerprints.add(fp);
      if (!o.actionable) continue;
      // active (open/acked) を最優先で見る (部分UNIQUEにより高々1件)。reopen 等で
      // 「最新行は dismissed だが古い行が active」という順序でも正しく更新側に入る
      const last = db.prepare(`SELECT * FROM radash_actions WHERE fingerprint=? AND status IN ('open','acked') ORDER BY id DESC LIMIT 1`).get(fp)
        || db.prepare(`SELECT * FROM radash_actions WHERE fingerprint=? ORDER BY id DESC LIMIT 1`).get(fp);

      if (last && (last.status === 'open' || last.status === 'acked')) {
        // 再検知 = 更新 (evidence は最新スナップショット、履歴は observations 側に残る)
        const sevUp = sevRank[o.severity] > sevRank[last.severity];
        db.prepare(`UPDATE radash_actions SET
            last_seen_at=?, occurrence_count=occurrence_count+1, latest_observation_id=?,
            evidence_json=?, title=?, suggested_action=?, severity=?, updated_at=?
          WHERE id=?`)
          .run(nowIso, obsId, JSON.stringify(o.metrics || {}), o.title, o.suggestedAction || '',
            sevUp ? o.severity : last.severity, nowIso, last.id);
        if (sevUp) insEvent.run(last.id, 'severity_changed', last.status, last.status, 'system',
          `severity: ${last.severity} → ${o.severity}`, nowIso);
        else insEvent.run(last.id, 'reseen', last.status, last.status, 'system', '', nowIso);
        updated++;
        continue;
      }
      if (last && last.status === 'dismissed') {
        // dismiss スコープを尊重 (severity 昇格時のみ新規作成を許す)
        const suppressed =
          last.dismiss_scope === 'permanent'
          || (last.dismiss_scope === 'until_date' && last.suppress_until && today <= last.suppress_until)
          || (last.dismiss_scope === 'occurrence' && last.suppress_until && today <= last.suppress_until);
        const sevUp = sevRank[o.severity] > sevRank[last.severity];
        if (suppressed && !sevUp) continue;
      }
      // 新規 (初検知 / done・expired 後の再発 / dismiss 抑制切れ)
      const actionId = insAction.run(fp, ruleId, o.entityType, o.entityId, o.entityLabel || '',
        o.dimension || '', o.severity, o.title, JSON.stringify(o.metrics || {}),
        o.suggestedAction || '', nowIso, nowIso, obsId, nowIso, nowIso).lastInsertRowid;
      insEvent.run(actionId, 'created', null, 'open', 'system',
        last ? `再発 (前回: ${last.status})` : '', nowIso);
      created++;
    }
  });
  tx();
  return { created, updated, fingerprints };
}

// ─── アクションキュー API ───

export function listActions({ status = 'open,acked', limit = 200 } = {}) {
  ensureRadashTables();
  const db = getMirrorDB();
  const statuses = String(status).split(',').map(s => s.trim()).filter(s =>
    ['open', 'acked', 'done', 'dismissed', 'expired', 'resolved'].includes(s));
  if (statuses.length === 0) statuses.push('open');
  const ph = statuses.map(() => '?').join(',');
  const n = Math.min(Math.max(Number(limit) || 200, 1), 500);
  const rows = db.prepare(`SELECT * FROM radash_actions WHERE status IN (${ph})
    ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, last_seen_at DESC
    LIMIT ?`).all(...statuses, n);
  return rows.map(r => ({ ...r, evidence: safeParse(r.evidence_json), evidence_json: undefined }));
}

function safeParse(s) { try { return JSON.parse(s || '{}'); } catch { return {}; } }

// 人の操作: ack / done / dismiss / reopen。
// AI (source='ai') が人の状態を変えることは API 層で禁止 (このモジュールは actorKind='human' 前提)
export function actOnAction(id, { event, actor = '', note = '', dismissScope, suppressUntil, implementedDate } = {}) {
  ensureRadashTables();
  const db = getMirrorDB();
  const nowIso = new Date().toISOString();
  const action = db.prepare(`SELECT * FROM radash_actions WHERE id=?`).get(Number(id));
  if (!action) throw new Error('action not found');

  const transitions = {
    ack: { from: ['open'], to: 'acked' },
    done: { from: ['open', 'acked'], to: 'done' },
    dismiss: { from: ['open', 'acked'], to: 'dismissed' },
    reopen: { from: ['dismissed', 'done', 'expired', 'resolved'], to: 'open' },
  };
  const t = transitions[event];
  if (!t) throw new Error(`不正な操作: ${event}`);
  if (!t.from.includes(action.status)) throw new Error(`状態 ${action.status} から ${event} はできません`);
  // reopen: 同一案件の active が既にあれば重複させない (部分UNIQUEの事前チェック — Codex R3 High)
  if (event === 'reopen') {
    const activeDup = db.prepare(`SELECT id FROM radash_actions WHERE fingerprint=? AND status IN ('open','acked') AND id != ?`)
      .get(action.fingerprint, action.id);
    if (activeDup) throw new Error(`同じ案件が対応待ち (ID ${activeDup.id}) に存在します。そちらを操作してください`);
  }

  const th = getThresholds();
  const tx = db.transaction(() => {
    let scope = null, until = null;
    if (event === 'dismiss') {
      scope = ['occurrence', 'until_date', 'permanent'].includes(dismissScope) ? dismissScope : 'occurrence';
      if (scope === 'occurrence') until = addDays(jstToday(), th.dismiss_occurrence_days);
      else if (scope === 'until_date') {
        if (!/^\d{4}-\d{2}-\d{2}$/.test(suppressUntil || '')) throw new Error('dismiss(期日まで) には suppress_until (YYYY-MM-DD) が必要です');
        until = suppressUntil;
      }
    }
    db.prepare(`UPDATE radash_actions SET status=?, dismiss_scope=?, dismiss_reason=?, suppress_until=?, updated_at=? WHERE id=?`)
      .run(t.to, event === 'dismiss' ? scope : action.dismiss_scope,
        event === 'dismiss' ? (note || '') : action.dismiss_reason,
        event === 'dismiss' ? until : action.suppress_until, nowIso, action.id);
    db.prepare(`INSERT INTO radash_action_events (action_id, event, from_status, to_status, actor_kind, actor, note, at)
      VALUES (?, ?, ?, ?, 'human', ?, ?, ?)`)
      .run(action.id, event === 'ack' ? 'acked' : event === 'done' ? 'done' : event === 'dismiss' ? 'dismissed' : 'reopened',
        action.status, t.to, actor, note || '', nowIso);
    // done = 施策を実施した → intervention 記録 (効果測定の起点)
    if (event === 'done') {
      const impl = /^\d{4}-\d{2}-\d{2}$/.test(implementedDate || '') ? implementedDate : jstToday();
      db.prepare(`INSERT INTO radash_interventions (action_id, entity_type, entity_id, implemented_date, description, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`)
        .run(action.id, action.entity_type, action.entity_id, impl, note || action.title, actor, nowIso);
    }
  });
  tx();
  return db.prepare(`SELECT * FROM radash_actions WHERE id=?`).get(action.id);
}

export function getActionDetail(id) {
  ensureRadashTables();
  const db = getMirrorDB();
  const action = db.prepare(`SELECT * FROM radash_actions WHERE id=?`).get(Number(id));
  if (!action) return null;
  const events = db.prepare(`SELECT * FROM radash_action_events WHERE action_id=? ORDER BY id`).all(action.id);
  const observations = db.prepare(`SELECT * FROM radash_observations
    WHERE rule_id=? AND entity_type=? AND entity_id=? AND dimension=?
    ORDER BY id DESC LIMIT 30`).all(action.rule_id, action.entity_type, action.entity_id, action.dimension)
    .map(o => ({ ...o, metrics: safeParse(o.metrics_json), metrics_json: undefined }));
  const intervention = db.prepare(`SELECT * FROM radash_interventions WHERE action_id=? ORDER BY id DESC LIMIT 1`).get(action.id) || null;
  return {
    ...action, evidence: safeParse(action.evidence_json), evidence_json: undefined,
    events, observations, intervention,
    effect: intervention && action.entity_type === 'sku' ? measureEffect(db, action.entity_id, intervention.implemented_date) : null,
  };
}

// ─── 効果測定 (参考評価。イベント日は除外せず交絡を注記 — Codex R2 (3)) ───
export function measureEffect(db, sku, implementedDate, windowDays = 7) {
  const before = { from: addDays(implementedDate, -windowDays), to: addDays(implementedDate, -1) };
  const after = { from: implementedDate, to: addDays(implementedDate, windowDays - 1) };
  const agg = (w) => db.prepare(`
    SELECT COALESCE(SUM(sales_yen),0) AS sales, COALESCE(SUM(orders),0) AS orders,
           COALESCE(SUM(access_users),0) AS access, COUNT(*) AS days
    FROM mirror_rakuten_item_daily
    WHERE item_manage_number = ? COLLATE NOCASE AND date_jst >= ? AND date_jst <= ?
  `).get(sku, w.from, w.to);
  const evts = (w) => db.prepare(`
    SELECT DISTINCT campaign_type || ': ' || campaign_name AS label
    FROM mirror_rakuten_campaigns
    WHERE date_jst <= ? AND COALESCE(substr(end_at,1,10), date_jst) >= ?
    LIMIT 10`).all(w.to, w.from).map(r => r.label);
  const b = agg(before), a = agg(after);
  const bEv = evts(before), aEv = evts(after);
  const cvr = (x) => x.access > 0 ? x.orders / x.access : null;
  const pct = (x, y) => (y > 0 && x !== null) ? Math.round((x / y - 1) * 1000) / 10 : null;
  const eventsDiffer = JSON.stringify(bEv.sort()) !== JSON.stringify(aEv.sort());
  const afterIncomplete = a.days < windowDays;
  return {
    window_days: windowDays,
    before: { ...before, ...b, cvr_pct: cvr(b) === null ? null : Math.round(cvr(b) * 10000) / 100, events: bEv },
    after: { ...after, ...a, cvr_pct: cvr(a) === null ? null : Math.round(cvr(a) * 10000) / 100, events: aEv },
    sales_change_pct: pct(a.sales, b.sales),
    access_change_pct: pct(a.access, b.access),
    cvr_change_pct: (cvr(a) !== null && cvr(b)) ? Math.round((cvr(a) / cvr(b) - 1) * 1000) / 10 : null,
    confidence: afterIncomplete ? 'low (実施後データ不足)' : eventsDiffer ? 'low (前後でイベント構成が異なる — 交絡あり)' : 'medium (参考評価)',
    note: '前後比較は参考評価です。イベント日は除外せず、構成差を注記しています',
  };
}

export function getRunStatus() {
  ensureRadashTables();
  const db = getMirrorDB();
  const latest = db.prepare(`SELECT * FROM radash_rule_runs ORDER BY id DESC LIMIT 1`).get() || null;
  const counts = db.prepare(`SELECT status, COUNT(*) AS c FROM radash_actions GROUP BY status`).all();
  return {
    latest_run: latest ? { ...latest, stats: safeParse(latest.stats_json), stats_json: undefined } : null,
    action_counts: Object.fromEntries(counts.map(r => [r.status, r.c])),
    rule_set_version: RULE_SET_VERSION,
    rules: RULES.map(r => ({ id: r.id, label: r.label })),
  };
}
