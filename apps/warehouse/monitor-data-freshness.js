/**
 * monitor-data-freshness.js — 取り込んだデータの「新しさ」の見張り (2026-10-24)
 *
 * なぜ要るか (2026-10-23 に調べた。ミニPC の warehouse.db の最新の date_jst):
 *   楽天RPP広告・データ分析 = 7/14〜7/15 / Yahoo統計 6 種 = 10/13・Yahooレビュー = 10/15 / au PAY分析 = 10/9 / Qoo10分析 = 10/3。
 *   原因はモールのログイン切れ (scripts/mall-csv-fetcher/*-download.mjs が exit 3)。fetch-all は exit 3 を再試行せず、子が GChat に 1 回知らせるだけ。
 *   daily-sync の取込は「置かれたファイル 0 件 = 正常」で ✅ = 毎朝のまとめでは何週間も見えなかった。
 *   「ジョブが動いたか」(jobs-monitor の ping) ではなく「データが新しいか」を見る = この見張り。
 *
 * 見方:
 *   - 決まった一覧 (FRESHNESS_TABLES。1 行 = 表・日付の列・しきい値 (日)・モール・何のデータか・止まったときに人がやること) の表ごとに、
 *     日付の列の最大を読み、「今日 (JST) − 最新の日」がしきい値を **超えたら** 止まっている (stale)。
 *     日付の列は YYYY-MM-DD / YYYY/MM/DD … / ISO の時刻 (Z・+09:00 は JST の日に直す) / 月 (YYYY-MM = その月の 1 日) を読める。
 *   - 表が無い・行が無い = **まだ始まっていない** (not_started)。止まった扱いにしない (例: 2026-10-22 に足した Amazon の表は、最初の取得の前は空)
 *   - 読めない (列が無い・SELECT が落ちる) = unreadable (⚠️。見張りの一覧の誤り)
 *   - **意図して止めているもの** (行の pause = 台帳 config/jobs-registry.mjs の temporary_asset の id): その台帳の項目がある、
 *     または env MALL_FETCH_ONLY がその取得 (fetcher) を外している間は、止まっていても「⏸️ 止めている」と短く出すだけ (paused)。
 *       台帳の remove_by を過ぎた = paused_overdue (⚠️ で目立たせる・GChat も新しく + 週 1 回)。
 *       MALL_FETCH_ONLY で外しているのに台帳に無い = paused_unregistered (⚠️)。止めていてもデータが新しい (手で入れた) = ふつう (ok)
 *   - 週末・祝日: 一覧の表はどれもモールの集計・API の注文・毎朝の写しで、土日・祝日も日付が進む (0 件の日も行がある・注文は毎日ある) ものだけにした。
 *     件数の少ないもの (au PAY・Qoo10・LINEギフトの注文、検索語・商品別、返品・補てん) はしきい値を長くしている
 *   - 週次・月次の表はしきい値を長く (週次 21 日・月次 = 月の 1 日から 70 日 / 楽天RPP の月次 = 今月分が毎朝入るので 40 日)
 *   - 今朝の自動取得 (fetch-all) の結果 (scripts/mall-csv-fetcher/logs/fetch-all-last.json = lib-fetch-status.mjs) も読み、
 *     exit 3 (ログイン切れなど手で直すもの) と、再試行でも直らなかった失敗を要約に出す (データが古くなる前の 1 日目から見える)
 *
 * 通知 (GChat・env GCHAT_WEBHOOK・monitor-fee-coverage.js と同じ形):
 *   - 止まっているもの (stale / unreadable / paused_overdue / paused_unregistered) を 1 通にまとめる (取得の単位 = feed ごと)
 *   - 同じものを毎日は鳴らさない: 新しく止まった日に 1 回 → その後は REMIND_EVERY_DAYS (7) 日ごとに念押し。戻ったら 1 回「✅ 戻った」
 *     (記録 = warehouse.db の data_freshness_alert_state。送れなかった日は記録しない = 翌朝もう一度)
 *   - 今朝の自動取得の exit 3 だけでは GChat を送らない (子のスクリプトが送っている)。止まった feed の行に添える
 * 要約 (最後の 1 行): 止まっているもの・ログイン切れ・止めているものの期限切れがあれば ⚠️ で始める (daily-sync が isWarnSummary で ⚠️ にする)。
 *   なければ ✅。🚨 毎朝の要約は、止まっている間は毎日 ⚠️ (GChat の 1 通だけを間引く)
 * 終了コード: 0 (止まっているものがあっても 0 = 朝の daily-sync を赤くしない・再試行に載せない)。warehouse.db が開けないときだけ 1
 *
 * 使い方:
 *   node apps/warehouse/monitor-data-freshness.js            # daily-sync から毎朝 (runScript が足す '7' は無視)
 *   node apps/warehouse/monitor-data-freshness.js --dry-run  # 文面を出すだけ (送らない・記録しない)
 *   node apps/warehouse/monitor-data-freshness.js --json     # 判定を JSON で出す (送らない・記録しない)
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { JOBS_REGISTRY } from '../../config/jobs-registry.mjs';
import { readFetchStatus, describeFetchExit } from '../../scripts/mall-csv-fetcher/lib-fetch-status.mjs';

export const REMIND_EVERY_DAYS = 7;          // 止まったままのものを念押しする間隔 (日)
export const FETCH_STATUS_MAX_AGE_HOURS = 30; // 自動取得 (毎朝 05:30) の最後の記録がこれより古い = 動いていない疑い
const GCHAT_MAX = 3900;
const MAX_LINE_GROUPS = 4;                   // 要約の 1 行に名前を出す feed の数

// ─── 取得の単位 (feed)。モール・何のデータか・止まったときに人がやること ───
const RELOGIN_RAKUTEN = 'ミニPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/rakuten-login-spike.mjs → 手動ログイン+「信頼できる端末」登録 (14日ごと)';
const RELOGIN_YAHOO = 'ミニPCの画面でデスクトップの「Yahoo-Relogin.bat」をダブルクリック →「Yahoo! JAPAN ID」でログイン (確認コードはメール)';
const RAKUTEN_BLOCKED = 'R-Login の「利用者管理（権限管理）」→「サービス別権限設定」で RPP広告・データ分析 の権限を確かめる (台帳 mall-fetch-skip-rakuten-blocked の runbook・検証は 1 日 1 回まで)。'
  + 'それまでは RMS から手で落とした CSV を incoming/ に置けば次の daily-sync が取り込む';
const ordersAction = (step) => `daily-sync の「${step}」の段 (❌・要約) と logs/daily-sync-*.log を見る (API の取得。ログインの切れではない)`;
const amazonAction = (step) => `daily-sync の「${step}」の段を見る (SP-API の注文の取得が失敗した朝は見送る)`;

export const FEEDS = Object.freeze({
  'rakuten-rpp': { mall: '楽天', label: '楽天RPP広告', fetcher: 'rakuten', pause: 'mall-fetch-skip-rakuten-blocked', action: RAKUTEN_BLOCKED },
  'rakuten-data': { mall: '楽天', label: '楽天データ分析', fetcher: 'rakuten-data', pause: 'mall-fetch-skip-rakuten-blocked', action: RAKUTEN_BLOCKED },
  'rakuten-review': { mall: '楽天', label: '楽天レビュー', fetcher: 'rakuten-review', action: RELOGIN_RAKUTEN },
  'yahoo-data': { mall: 'Yahoo', label: 'Yahoo統計', fetcher: 'yahoo', action: RELOGIN_YAHOO },
  'yahoo-review': { mall: 'Yahoo', label: 'Yahooレビュー', fetcher: 'yahoo-review', action: RELOGIN_YAHOO },
  'aupay-data': { mall: 'au PAY', label: 'au PAY分析', fetcher: 'aupay', action: 'WOW!マネージャーに手で一度ログインして確かめる (ID/PW の拒否・ロックの疑い。自動の再試行はしない)。ログ = scripts/mall-csv-fetcher/logs/aupay-*.log' },
  'qoo10-data': { mall: 'Qoo10', label: 'Qoo10分析', fetcher: 'qoo10', action: 'ミニPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/qoo10-login-spike.mjs → 本ID/PW+reCAPTCHA+サブIDで手動ログイン (「✅storageState保存」を確かめる)' },
  'orders-amazon': { mall: 'Amazon', label: 'Amazonの注文', fetcher: null, action: ordersAction('Amazon') },
  'orders-rakuten': { mall: '楽天', label: '楽天の注文', fetcher: null, action: ordersAction('楽天') },
  'orders-yahoo': { mall: 'Yahoo', label: 'Yahooの注文', fetcher: null, action: `${ordersAction('Yahoo')}。Yahoo のトークンの期限 (「Yahooトークン期限アラート」) も` },
  'orders-aupay': { mall: 'au PAY', label: 'au PAYの注文', fetcher: null, action: ordersAction('au PAY') },
  'orders-qoo10': { mall: 'Qoo10', label: 'Qoo10の注文', fetcher: null, action: ordersAction('Qoo10') },
  'orders-linegift': { mall: 'LINEギフト', label: 'LINEギフトの注文', fetcher: null, action: ordersAction('LINEギフト') },
  'orders-ne': { mall: 'NE', label: 'NEの受注', fetcher: null, action: `${ordersAction('NE')}。NE のトークン (data/ne-tokens.json) も` },
  'amazon-traffic': { mall: 'Amazon', label: 'Amazonアクセス・転換率', fetcher: null, action: amazonAction('Amazonアクセス・転換率') },
  'amazon-listing': { mall: 'Amazon', label: 'Amazon出品の状態', fetcher: null, action: amazonAction('Amazon出品の状態') },
  'amazon-bsr': { mall: 'Amazon', label: 'Amazon BSR', fetcher: null, action: amazonAction('Amazon BSR') },
  'amazon-returns': { mall: 'Amazon', label: 'Amazon返品', fetcher: null, action: amazonAction('Amazon返品') },
  'amazon-reimb': { mall: 'Amazon', label: 'Amazon補てん', fetcher: null, action: `${amazonAction('Amazon補てん')}。直近 60 日に補てんが 1 件も無いと取得日が進まない` },
  'amazon-ledger': { mall: 'Amazon', label: 'Amazon在庫台帳', fetcher: null, action: amazonAction('Amazon在庫台帳') },
  'amazon-ads': { mall: 'Amazon', label: 'Amazon広告 (キャンペーン)', fetcher: null, action: 'daily-sync の「Amazon Ads (campaign)」の段を見る (Amazon Ads API のトークン・レポートの遅れ)' },
});

/** 1 行 = { table, column, kind ('date' = 日付・日時 / 'month' = YYYY-MM), maxAgeDays, feed, mall, what, action, fetcher, pause } */
const row = (feed, table, column, maxAgeDays, what, kind = 'date') => {
  const f = FEEDS[feed];
  if (!f) throw new Error(`feed が無い: ${feed}`);
  return Object.freeze({ table, column, kind, maxAgeDays, feed, mall: f.mall, what, action: f.action, fetcher: f.fetcher, pause: f.pause || null });
};

/**
 * 見る表の一覧 (表と列はコードの CREATE TABLE で確かめた。2026-10-24)。
 * 🚨 足すときは「土日・祝日も日付が進むか」「行が無い日があるか」を考えてしきい値を決める。コードに CREATE TABLE が無い表は入れない
 *   (例: fact_ad_spend = Amazon 広告の SKU 別は CREATE TABLE がコードに無い = 入れていない。キャンペーン別の fact_ad_spend_campaign で見る)
 */
export const FRESHNESS_TABLES = Object.freeze([
  // 楽天 RPP 広告 (rakuten-ads-rpp-lib.js)。2026-07-17 から RMS のサブアプリで拒否 = MALL_FETCH_ONLY で取得から外している (台帳 mall-fetch-skip-rakuten-blocked)
  row('rakuten-rpp', 'fact_rakuten_ads_rpp_daily', 'date_jst', 4, 'RPP広告 日次 (キャンペーン別)'),
  row('rakuten-rpp', 'fact_rakuten_ads_rpp', 'month_ym', 40, 'RPP広告 月次 (商品別・今月分が毎朝入る)', 'month'),
  // 楽天 RMS データ分析 (rakuten-data-lib.js / rakuten-dd-lib.js)。同じく外している
  row('rakuten-data', 'fact_rakuten_store_daily', 'date_jst', 4, '店舗 日次'),
  row('rakuten-data', 'fact_rakuten_item_daily', 'date_jst', 4, '商品分析 日次'),
  row('rakuten-data', 'fact_rakuten_store_device_daily', 'date_jst', 4, '店舗データ 日次 (デバイス別)'),
  row('rakuten-data', 'fact_rakuten_sku_daily', 'date_jst', 4, 'SKU別売上 日次'),
  row('rakuten-data', 'fact_rakuten_category_daily', 'date_jst', 4, 'カテゴリページ 日次'),
  row('rakuten-data', 'fact_rakuten_purchaser_monthly', 'date_jst', 70, '新規・リピート購入者 月次 (date_jst = 月の 1 日)'),
  row('rakuten-data', 'fact_rakuten_item_purchaser_snapshot', 'date_jst', 4, '新規・リピート購入者 商品別 (取込日のスナップショット)'),
  row('rakuten-data', 'fact_rakuten_genre_purchaser_snapshot', 'date_jst', 4, '新規・リピート購入者 ジャンル別 (取込日のスナップショット)'),
  // 楽天レビュー (rakuten-review-lib.js)。投稿日は客しだい = 取込が最後に見た時刻 (直近 30 日の窓の全部を毎回見る) で見る
  row('rakuten-review', 'fact_rakuten_reviews', 'last_seen_at', 4, 'レビュー (取込が最後に見た時刻)'),
  // Yahoo ストクリ統計 6 種 (yahoo-data-lib.js)
  row('yahoo-data', 'fact_yahoo_store_device_daily', 'date_jst', 4, 'ストア 日次 (デバイス別)'),
  row('yahoo-data', 'fact_yahoo_inflow_daily', 'date_jst', 4, '流入 日次'),
  row('yahoo-data', 'fact_yahoo_user_attr_daily', 'date_jst', 4, 'お客様分析 日次'),
  row('yahoo-data', 'fact_yahoo_flash_hourly', 'date_jst', 4, '速報 時間帯別'),
  row('yahoo-data', 'fact_yahoo_item_daily', 'date_jst', 4, '商品 日次'),
  row('yahoo-data', 'fact_yahoo_keyword_daily', 'date_jst', 4, '検索キーワード 日次'),
  // Yahoo レビュー (yahoo-review-lib.js)。90 日の全量 ZIP = 取込が最後に見た時刻
  row('yahoo-review', 'fact_yahoo_reviews', 'last_seen_at', 4, 'レビュー (取込が最後に見た時刻)'),
  // au PAY 分析 (aupay-data-lib.js)
  row('aupay-data', 'fact_aupay_sales_daily', 'date_jst', 4, '売上分析 日次'),
  row('aupay-data', 'fact_aupay_referer_daily', 'date_jst', 4, '参照元分析 日次'),
  row('aupay-data', 'fact_aupay_page_daily', 'date_jst', 4, 'ページ分析 日次'),
  row('aupay-data', 'fact_aupay_search_daily', 'date_jst', 7, '検索分析 日次 (流入の無い日があり得る)'),
  row('aupay-data', 'fact_aupay_product_daily', 'date_jst', 7, '商品分析 日次 (流入の無い日があり得る)'),
  row('aupay-data', 'fact_aupay_pm_ad_daily', 'date_jst', 7, 'プラチナマッチ広告 日次'),
  row('aupay-data', 'fact_aupay_pm_query_weekly', 'date_jst', 21, 'プラチナマッチ検索クエリ 週次 (date_jst = 対象週の月曜)'),
  row('aupay-data', 'fact_aupay_sales_monthly', 'date_jst', 70, '売上分析 月次 (date_jst = 月の 1 日)'),
  // Qoo10 分析 (qoo10-data-lib.js)
  row('qoo10-data', 'fact_qoo10_cvr_daily', 'date_jst', 4, '転換率 日次'),
  row('qoo10-data', 'fact_qoo10_traffic_channel_daily', 'date_jst', 4, '流入チャネル 日次'),
  row('qoo10-data', 'fact_qoo10_item_traffic_daily', 'date_jst', 7, '商品別流入 日次 (流入の無い日があり得る)'),
  // 受注の raw (各モールの API。daily-sync の取得)
  row('orders-amazon', 'raw_sp_orders', 'purchase_date', 3, '注文 (SP-API・注文日)'),
  row('orders-rakuten', 'raw_rakuten_orders', 'order_date', 3, '注文 (RMS API・注文日)'),
  row('orders-yahoo', 'raw_yahoo_orders', 'order_time', 3, '注文 (注文日時)'),
  row('orders-aupay', 'raw_aupay_orders', 'order_date', 7, '注文 (注文日・件数が少ない)'),
  row('orders-qoo10', 'raw_qoo10_orders', 'order_date', 10, '注文 (注文日・件数が少ない)'),
  row('orders-linegift', 'raw_linegift_orders', 'bought_date_jst', 14, '注文 (注文日・件数が少ない)'),
  row('orders-ne', 'raw_ne_orders', '受注日', 3, '受注 (NE・受注日)'),
  // Amazon (2026-10-22 #1787 で足した表。最初の取得の前は空 = まだ始まっていない)
  row('amazon-traffic', 'amazon_sales_traffic_daily', 'date_jst', 5, 'アクセス・転換率 日次 (店全体・レポートは 2〜3 日遅れ)'),
  row('amazon-traffic', 'amazon_sales_traffic_sku_daily', 'date_jst', 5, 'アクセス・転換率 日次 (SKU 別)'),
  row('amazon-listing', 'amazon_listing_status_daily', 'date_jst', 3, '出品の状態 (毎朝の写し)'),
  row('amazon-bsr', 'amazon_catalog_rank_daily', 'date_jst', 3, '売れ筋順位 (毎朝の写し)'),
  row('amazon-returns', 'amazon_fba_returns', 'fetched_at', 3, 'FBA の返品 (直近 60 日を毎朝取り直す = 取得の時刻)'),
  row('amazon-reimb', 'amazon_fba_reimbursements', 'fetched_at', 7, 'FBA の補てん (直近 60 日を毎朝取り直す = 取得の時刻)'),
  row('amazon-ledger', 'amazon_inventory_ledger', 'fetched_at', 3, 'FBA の在庫の台帳 (直近 30 日を毎朝置き換え = 取得の時刻)'),
  // 広告
  row('amazon-ads', 'fact_ad_spend_campaign', '日付', 4, '広告費 日次 (キャンペーン別)'),
]);

// ─── 日付 ───
export const jstToday = (nowMs = Date.now()) => new Date(nowMs + 9 * 3600000).toISOString().slice(0, 10);
const dayMs = (d) => Date.parse(`${d}T00:00:00Z`);
export const daysBetween = (from, to) => Math.round((dayMs(to) - dayMs(from)) / 86400000);
const isRealDate = (d) => { if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return false; const ms = Date.parse(`${d}T00:00:00Z`); return Number.isFinite(ms) && new Date(ms).toISOString().slice(0, 10) === d; };
const md = (d) => (d ? `${Number(d.slice(5, 7))}/${Number(d.slice(8, 10))}` : '?');

/**
 * 列の値 → JST の日 (YYYY-MM-DD)。読めない = null
 *   'YYYY-MM' (kind = 'month') = その月の 1 日 / 'YYYY-MM-DD' / 'YYYY/MM/DD …' (時差なし = JST とみなす) / ISO の時刻で Z・+hh:mm・+hhmm つき = JST の日に直す
 */
export function toJstDay(v, kind = 'date') {
  const s = String(v ?? '').trim();
  if (!s) return null;
  if (kind === 'month') {
    const m = s.match(/^(\d{4})-(\d{2})$/);
    if (m) { const d = `${m[1]}-${m[2]}-01`; return isRealDate(d) ? d : null; }
  }
  const z = s.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?\s*(Z|[+-]\d{2}:?\d{2})$/);
  if (z) {
    const off = z[7] === 'Z' ? 'Z' : z[7].replace(/^([+-]\d{2})(\d{2})$/, '$1:$2');
    const ms = Date.parse(`${z[1]}-${z[2]}-${z[3]}T${z[4]}:${z[5]}:${z[6] || '00'}${off}`);
    return Number.isFinite(ms) ? jstToday(ms) : null;
  }
  const m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:$|[ T])/);
  if (m) { const d = `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`; return isRealDate(d) ? d : null; }
  return null;
}

const qid = (name) => `"${String(name).replace(/"/g, '""')}"`;

/** 1 表の最新の日。{ state: 'missing' | 'empty' | 'unreadable' | 'ok', latest, raw, error } */
export function readLatest(db, t) {
  const has = db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?").get(t.table);
  if (!has) return { state: 'missing', latest: null, raw: null, error: null };
  let r;
  try {
    r = db.prepare(`SELECT MAX(${qid(t.column)}) AS v FROM ${qid(t.table)} WHERE ${qid(t.column)} IS NOT NULL AND TRIM(${qid(t.column)}) <> ''`).get();
  } catch (e) {
    return { state: 'unreadable', latest: null, raw: null, error: String(e && e.message).slice(0, 160) };
  }
  if (!r || r.v === null || r.v === undefined) return { state: 'empty', latest: null, raw: null, error: null };
  const latest = toJstDay(r.v, t.kind);
  if (!latest) return { state: 'unreadable', latest: null, raw: String(r.v).slice(0, 40), error: `日付として読めない値: ${String(r.v).slice(0, 40)}` };
  return { state: 'ok', latest, raw: String(r.v), error: null };
}

/** MALL_FETCH_ONLY (カンマ区切り) → 取得するもの (空 = 全部) */
export const parseFetchOnly = (v) => String(v || '').split(',').map((s) => s.trim()).filter(Boolean);

/**
 * 意図して止めているか。{ paused: boolean, overdue: boolean, unregistered: boolean, entry, byEnv }
 *   止めている = 台帳に pause の temporary_asset がある、または MALL_FETCH_ONLY がその取得を外している
 */
export function pauseOf(t, { registry = JOBS_REGISTRY, fetchOnly = [], today }) {
  const entry = t.pause ? registry.find((e) => e.id === t.pause && e.type === 'temporary_asset') || null : null;
  const byEnv = !!t.fetcher && fetchOnly.length > 0 && !fetchOnly.includes(t.fetcher);
  const paused = !!entry || byEnv;
  return { paused, entry, byEnv, overdue: !!entry && typeof entry.remove_by === 'string' && entry.remove_by < today, unregistered: byEnv && !entry };
}

/**
 * 全部の表を判定する (DB を読むだけ・書かない)
 * @returns {{ today, items: object[] }} items[i] = 一覧の行 + { status, latest, ageDays, error, pause }
 *   status = ok / stale / not_started / unreadable / paused (止めている・期限内) / paused_overdue / paused_unregistered
 */
export function evaluateFreshness(db, { nowMs = Date.now(), tables = FRESHNESS_TABLES, registry = JOBS_REGISTRY, fetchOnly = [] } = {}) {
  const today = jstToday(nowMs);
  const items = tables.map((t) => {
    const r = readLatest(db, t);
    const p = pauseOf(t, { registry, fetchOnly, today });
    const base = { ...t, latest: r.latest, ageDays: r.latest ? daysBetween(r.latest, today) : null, error: r.error, pause: p };
    if (r.state === 'missing' || r.state === 'empty') return { ...base, status: 'not_started', note: r.state === 'missing' ? '表が無い' : '行が無い' };
    if (r.state === 'unreadable') return { ...base, status: 'unreadable' };
    const fresh = base.ageDays <= t.maxAgeDays;   // 未来の日 (ageDays < 0) もふつう
    if (fresh) return { ...base, status: 'ok' };
    if (p.paused) return { ...base, status: p.unregistered ? 'paused_unregistered' : p.overdue ? 'paused_overdue' : 'paused' };
    return { ...base, status: 'stale' };
  });
  return { today, items };
}

/** 今朝の自動取得の結果 → { at, atDay, old, failures: [{ mall, code, what }], problem } */
export function summarizeFetchStatus(read, { nowMs = Date.now() } = {}) {
  if (!read || !read.status) return { at: null, atDay: null, old: false, failures: [], problem: read ? read.problem : 'missing' };
  const st = read.status;
  const atMs = Date.parse(st.finished_at);
  const old = !Number.isFinite(atMs) || nowMs - atMs > FETCH_STATUS_MAX_AGE_HOURS * 3600000;
  const failures = st.results.filter((r) => r && r.code !== 0).map((r) => ({ mall: r.mall, code: r.code ?? null, timedOut: !!r.timedOut, what: describeFetchExit(r) }));
  return { at: st.finished_at, atDay: Number.isFinite(atMs) ? jstToday(atMs) : null, atText: Number.isFinite(atMs) ? new Date(atMs + 9 * 3600000).toISOString().slice(5, 16).replace('-', '/').replace('T', ' ') : '?', old, failures, problem: null };
}

// ─── 通知の記録 (同じものを毎日鳴らさない) ───
export function ensureFreshnessState(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS data_freshness_alert_state (
    item_key         TEXT PRIMARY KEY,   -- 'table:<表>' / 'pause:<feed>'
    status           TEXT NOT NULL,      -- stale / unreadable / paused_overdue / paused_unregistered
    feed             TEXT NOT NULL,
    since_on         TEXT NOT NULL,      -- この状態を最初に見た JST の日
    latest_date      TEXT,
    last_notified_on TEXT,               -- GChat に送れた JST の日 (null = まだ)
    updated_at       TEXT NOT NULL
  )`);
}

const ALERT_STATUSES = new Set(['stale', 'unreadable', 'paused_overdue', 'paused_unregistered']);

/** 知らせる対象 (表ごと。止めているものの期限切れ = 台帳の項目ごとに 1 つ・台帳なし = feed ごとに 1 つ) */
export function problemsOf(items) {
  const out = [];
  const byPause = new Map();
  for (const it of items) {
    if (!ALERT_STATUSES.has(it.status)) continue;
    if (it.status === 'paused_overdue' || it.status === 'paused_unregistered') {
      const key = it.status === 'paused_overdue' ? `pause:${it.pause.entry.id}` : `pause:${it.feed}`;
      const p = byPause.get(key);
      if (p) { if (!p.feeds.includes(it.feed)) p.feeds.push(it.feed); if (it.latest && (!p.latest || it.latest > p.latest)) p.latest = it.latest; continue; }
      const np = { key, status: it.status, feed: it.feed, feeds: [it.feed], latest: it.latest, item: it };
      byPause.set(key, np);
      out.push(np);
    } else {
      out.push({ key: `table:${it.table}`, status: it.status, feed: it.feed, latest: it.latest, item: it });
    }
  }
  return out;
}

/**
 * 今日の判断 (純粋関数): 前の記録 (state の行) と今日の問題 → 送るもの・念押ししないもの・戻ったもの
 * @returns {{ due: object[], quiet: object[], recovered: object[] }}  due[i].reason = 'new' | 'remind' | 'retry'
 */
export function planNotifications(problems, stateRows, today) {
  const prev = new Map(stateRows.map((r) => [r.item_key, r]));
  const due = [], quiet = [];
  for (const p of problems) {
    const s = prev.get(p.key);
    if (!s || s.status !== p.status) due.push({ ...p, reason: 'new', since: today });
    else if (!s.last_notified_on) due.push({ ...p, reason: 'retry', since: s.since_on });
    else if (daysBetween(s.last_notified_on, today) >= REMIND_EVERY_DAYS) due.push({ ...p, reason: 'remind', since: s.since_on });
    else quiet.push({ ...p, since: s.since_on, lastNotified: s.last_notified_on });
  }
  const now = new Set(problems.map((p) => p.key));
  const recovered = stateRows.filter((r) => !now.has(r.item_key));
  return { due, quiet, recovered };
}

const STATUS_WORD = { stale: '止まっている', unreadable: '読めない', paused_overdue: '止めている (台帳の期限切れ)', paused_unregistered: '止めている (台帳に無い)' };

/** feed ごとにまとめる (同じ feed の表は 1 行) */
function groupByFeed(list) {
  const m = new Map();
  for (const p of list) {
    if (!m.has(p.feed)) m.set(p.feed, []);
    m.get(p.feed).push(p);
  }
  return [...m.entries()].map(([feed, ps]) => ({ feed, ps }));
}

/** feed の止まった表の代表 = しきい値の一番短い表 (日次) のうち、いちばん新しい日の表 (月次の古い日を「最新」と言わない) */
function representative(items) {
  const withDate = items.filter((i) => i.latest);
  if (!withDate.length) return null;
  return [...withDate].sort((a, b) => a.maxAgeDays - b.maxAgeDays || (a.latest < b.latest ? 1 : a.latest > b.latest ? -1 : 0))[0];
}

function feedLine(feed, ps, { fetch, today }) {
  const f = FEEDS[feed];
  const it0 = ps[0].item;
  const st = ps[0].status;
  let line = '';
  if (st === 'paused_overdue' || st === 'paused_unregistered') {
    const p = it0.pause;
    const labels = (ps[0].feeds || [feed]).map((x) => FEEDS[x].label).join('・');
    line = `• ⏸️ *${labels}* — ${STATUS_WORD[st]}`;
    if (p.entry) line += `: 台帳 ${p.entry.id} の期限 ${p.entry.remove_by} を ${daysBetween(p.entry.remove_by, today)} 日過ぎた`;
    else line += ': env MALL_FETCH_ONLY で外しているが台帳 (config/jobs-registry.mjs) に temporary_asset が無い';
    if (ps[0].latest) line += ` (最新 ${md(ps[0].latest)})`;
  } else if (st === 'unreadable') {
    line = `• ❓ *${f.label}* — 読めない: ${ps.map((p) => `${p.item.table}.${p.item.column} (${p.item.error})`).join(' / ')}`;
    return `${line}\n   → 見張りの一覧 (monitor-data-freshness.js の FRESHNESS_TABLES) の表・列を確かめる`;
  } else {
    const rep = representative(ps.map((p) => p.item));
    line = `• *${f.label}* (${f.mall}・${ps.length} 表) — 最新 ${md(rep.latest)} (${rep.ageDays} 日前・しきい値 ${rep.maxAgeDays} 日)`;
    if (ps.length <= 3) line += `: ${ps.map((p) => p.item.table).join(', ')}`;
  }
  const fr = f.fetcher && fetch ? fetch.failures.find((x) => x.mall === f.fetcher) : null;
  if (fr) line += `\n   今朝の自動取得 (${fetch.atText}): ${fr.what}`;
  return `${line}\n   → ${f.action}`;
}

/** GChat の 1 通 (送るものが無ければ null) */
export function buildFreshnessMessage({ today, due, quiet, recovered, fetch }) {
  if (due.length === 0 && recovered.length === 0) return null;
  let msg = `⚠️ *データが止まっている* (${today} daily-sync・データの新しさ)\n`;
  const fresh = due.filter((d) => d.reason !== 'remind');
  const remind = due.filter((d) => d.reason === 'remind');
  if (fresh.length) msg += `\n🆕 *新しく止まった*\n${groupByFeed(fresh).map((g) => feedLine(g.feed, g.ps, { fetch, today })).join('\n')}\n`;
  if (remind.length) msg += `\n🔁 *止まったまま (${REMIND_EVERY_DAYS} 日ごとの念押し)*\n${groupByFeed(remind).map((g) => feedLine(g.feed, g.ps, { fetch, today }) + `\n   (${md(g.ps[0].since)} から)`).join('\n')}\n`;
  if (recovered.length) {
    const names = [...new Set(recovered.map((r) => FEEDS[r.feed] ? FEEDS[r.feed].label : r.feed))];
    msg += `\n✅ *戻った*: ${names.join('・')}\n`;
    if (due.length === 0) msg = msg.replace('⚠️ *データが止まっている*', '✅ *止まっていたデータが戻った*');
  }
  if (quiet.length) {
    const names = groupByFeed(quiet).map((g) => `${FEEDS[g.feed] ? FEEDS[g.feed].label : g.feed} (${md(g.ps[0].since)} から)`);
    msg += `\nほかに止まったまま (今日は念押ししない): ${names.join('・')}\n`;
  }
  msg += `\n見方: node apps/warehouse/monitor-data-freshness.js --dry-run (warehouse.db の表の最新の日付を読むだけ)`;
  if (msg.length > GCHAT_MAX) msg = `${msg.slice(0, GCHAT_MAX)}\n…[切り詰め]`;
  return msg;
}

/** 要約の 1 行 (daily-sync の朝の通知にそのまま載る) */
export function buildSummaryLine({ items, fetch, notify }) {
  const count = (s) => items.filter((i) => i.status === s).length;
  const stale = items.filter((i) => i.status === 'stale' || i.status === 'unreadable');
  const pausedAll = items.filter((i) => i.status.startsWith('paused'));
  const parts = [];
  if (stale.length) {
    const groups = groupByFeed(stale.map((i) => ({ feed: i.feed, item: i })));
    const names = groups.slice(0, MAX_LINE_GROUPS).map((g) => {
      const f = FEEDS[g.feed];
      const rep = representative(g.ps.map((p) => p.item));
      return rep ? `${f.label} 最新 ${md(rep.latest)} = ${rep.ageDays} 日前` : `${f.label} 読めない`;
    });
    parts.push(`止まっている ${stale.length} 表 (${names.join('・')}${groups.length > MAX_LINE_GROUPS ? ` ほか ${groups.length - MAX_LINE_GROUPS} 種` : ''})`);
  }
  const fetchWarn = fetch && (fetch.failures.length > 0 || fetch.old);
  if (fetch && fetch.failures.length) {
    const login = fetch.failures.filter((x) => x.code === 3).map((x) => x.mall);
    const other = fetch.failures.filter((x) => x.code !== 3).map((x) => `${x.mall}=${x.timedOut ? 'タイムアウト' : `exit${x.code ?? 'kill'}`}`);
    const what = [login.length ? `exit 3 = ログイン切れなど手で直すもの: ${login.join('・')}` : null, other.length ? `失敗: ${other.join('・')}` : null].filter(Boolean).join(' / ');
    parts.push(`自動取得 (${fetch.atText} の回) ${what}`);
  }
  if (fetch && fetch.old) parts.push(`自動取得の最後の記録が古い (${fetch.atText}・${FETCH_STATUS_MAX_AGE_HOURS} 時間より前 = 05:30 の取得が動いていない疑い)`);
  const overdue = pausedAll.filter((i) => i.status !== 'paused');
  if (pausedAll.length) {
    const groups = groupByFeed(pausedAll.map((i) => ({ feed: i.feed, item: i })));
    const names = groups.map((g) => FEEDS[g.feed].label).join('・');
    const p = pausedAll[0].pause;
    let s = `⏸️ 止めている: ${names}`;
    const od = overdue.find((i) => i.status === 'paused_overdue');
    const un = overdue.find((i) => i.status === 'paused_unregistered');
    if (od) s += ` (台帳 ${od.pause.entry.id} の期限 ${od.pause.entry.remove_by} を過ぎた)`;
    else if (un) s += ' (MALL_FETCH_ONLY で外しているが台帳に無い)';
    else if (p.entry) s += ` (台帳 ${p.entry.id}・期限 ${p.entry.remove_by})`;
    parts.push(s);
  }
  const warn = stale.length > 0 || overdue.length > 0 || fetchWarn;
  const ns = count('not_started');
  const tail = [`${items.length} 表を見た`];
  if (ns) tail.push(`まだ始まっていない ${ns} 表`);
  if (notify) tail.push(notify);
  if (!warn) return `✅ データの新しさ: 止まっているもの無し${parts.length ? ` / ${parts.join(' / ')}` : ''} (${tail.join('・')})`;
  return `⚠️ データの新しさ: ${parts.join(' / ')} (${tail.join('・')})`;
}

async function sendGChat(webhook, text, fetchImpl = fetch) {
  try {
    const res = await fetchImpl(webhook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }), signal: AbortSignal.timeout(30000) });
    return res.ok;
  } catch (e) {
    console.error(`[GChat] 通知失敗: ${e.message}`);
    return false;
  }
}

/** 記録を今日の状態にそろえる (送れた = last_notified_on を今日に) */
function saveState(db, { problems, plan, today, sent, nowIso }) {
  db.transaction(() => {
    const upsert = db.prepare(`INSERT INTO data_freshness_alert_state (item_key, status, feed, since_on, latest_date, last_notified_on, updated_at)
      VALUES (@key, @status, @feed, @since, @latest, @notified, @now)
      ON CONFLICT(item_key) DO UPDATE SET status = excluded.status, feed = excluded.feed, since_on = excluded.since_on, latest_date = excluded.latest_date,
        last_notified_on = excluded.last_notified_on, updated_at = excluded.updated_at`);
    const dueKeys = new Map(plan.due.map((d) => [d.key, d]));
    const quietKeys = new Map(plan.quiet.map((q) => [q.key, q]));
    for (const p of problems) {
      const d = dueKeys.get(p.key);
      const q = quietKeys.get(p.key);
      const since = d ? d.since : q ? q.since : today;
      const notified = d ? (sent ? today : null) : q ? q.lastNotified : null;
      upsert.run({ key: p.key, status: p.status, feed: p.feed, since, latest: p.latest || null, notified, now: nowIso });
    }
    // 戻ったもの: 知らせていた (last_notified_on あり) ものは「戻った」を送れたときだけ消す (送れなかった = 翌朝もう一度)。知らせていないものは黙って消す
    const del = db.prepare('DELETE FROM data_freshness_alert_state WHERE item_key = ?');
    for (const r of plan.recovered) if (sent || !r.last_notified_on) del.run(r.item_key);
  })();
}

/**
 * 本体 (試験で db・時刻・送り手・env を差し替えられる)
 * @returns {{ lastLine: string, items: object[], message: string|null, status: string }}
 */
export async function runFreshness(db, {
  nowMs = Date.now(), webhook = process.env.GCHAT_WEBHOOK, dryRun = false, send = sendGChat, log = console.log,
  tables = FRESHNESS_TABLES, registry = JOBS_REGISTRY, fetchOnly = parseFetchOnly(process.env.MALL_FETCH_ONLY), fetchStatus = readFetchStatus(),
} = {}) {
  const { today, items } = evaluateFreshness(db, { nowMs, tables, registry, fetchOnly });
  const fetch = summarizeFetchStatus(fetchStatus, { nowMs });
  for (const it of items) {
    log(`  ${it.status.padEnd(19)} ${it.table}.${it.column}  最新 ${it.latest || '-'}${it.ageDays !== null ? ` (${it.ageDays} 日前)` : ''}  しきい値 ${it.maxAgeDays} 日${it.note ? `  ${it.note}` : ''}${it.error ? `  ${it.error}` : ''}`);
  }
  if (fetch.problem && fetch.problem !== 'missing') log(`  自動取得の記録を読めない: ${fetch.problem}`);
  const problems = problemsOf(items);
  if (dryRun) {
    const plan = planNotifications(problems, [], today);   // 記録を読まない = 全部「新しく止まった」として文面を出す
    const message = buildFreshnessMessage({ today, ...plan, recovered: [], fetch });
    if (message) log(`\n--- GChat の文面 (--dry-run = 送らない・記録しない) ---\n${message}\n---`);
    return { status: 'dry_run', items, message, lastLine: buildSummaryLine({ items, fetch, notify: '--dry-run' }) };
  }
  ensureFreshnessState(db);
  const stateRows = db.prepare('SELECT item_key, status, feed, since_on, latest_date, last_notified_on FROM data_freshness_alert_state').all();
  const plan = planNotifications(problems, stateRows, today);
  const message = buildFreshnessMessage({ today, ...plan, recovered: plan.recovered.filter((r) => r.last_notified_on), fetch });
  let sent = false, notify = null, status = 'no_message';
  if (message) {
    log(`\n--- GChat の文面 ---\n${message}\n---`);
    if (!webhook) { notify = 'GCHAT_WEBHOOK 未設定で送らない'; status = 'no_webhook'; }
    else {
      try { sent = await send(webhook, message); } catch (e) { console.error(`[GChat] 通知失敗: ${e.message}`); sent = false; }
      notify = sent ? `GChat に送った (${plan.due.length} 件)` : 'GChat に送れなかった = 明日もう一度';
      status = sent ? 'sent' : 'send_failed';
    }
  } else if (plan.quiet.length) notify = `GChat は ${REMIND_EVERY_DAYS} 日ごとの念押しの日まで送らない`;
  saveState(db, { problems, plan, today, sent, nowIso: new Date(nowMs).toISOString() });
  return { status, items, message, lastLine: buildSummaryLine({ items, fetch, notify }) };
}

async function main() {
  await import('dotenv/config');
  const { initDB, getDB } = await import('./db.js');
  await initDB();
  const argv = process.argv.slice(2);
  const json = argv.includes('--json');
  const r = await runFreshness(getDB(), { dryRun: argv.includes('--dry-run') || json, log: json ? () => {} : console.log });
  if (json) console.log(JSON.stringify({ status: r.status, items: r.items.map(({ action, ...x }) => x) }, null, 2));
  console.log(r.lastLine);
  process.exitCode = 0;   // GChat の fetch の直後に process.exit() しない (Windows の libuv assertion。monitor-fee-coverage.js と同じ)
}

const realPath = (p) => { try { return fs.realpathSync.native(p); } catch { return path.resolve(p); } };
const foldCase = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);
const isMain = !!process.argv[1] && foldCase(realPath(process.argv[1])) === foldCase(realPath(fileURLToPath(import.meta.url)));
if (isMain) main().catch((e) => {
  console.log(`❌ データの新しさ: ${String(e.message).replace(/\s+/g, ' ').slice(0, 300)}`);
  process.exitCode = 1;
});
