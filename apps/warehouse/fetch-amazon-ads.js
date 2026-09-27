/**
 * fetch-amazon-ads.js — Amazon Ads spAdvertisedProduct レポート取得 (SKU 別広告費)
 *
 * 目的: SKU 別広告費を fact_ad_spend に取得
 *   - キャンペーン全広告費は fetch-amazon-ads-campaign.js (spCampaigns) が取得
 *   - 本スクリプトは「広告された SKU/ASIN 別」の広告費を取得
 *   - v_amazon_sku_profit_actual_v4 view が SKU 別 contribution margin 計算で参照
 *   - 注意: Auto-Targeting Campaign では advertised SKU が unallocated になるため、
 *     spAdvertisedProduct と spCampaigns の差分が unallocated 広告費 (47% 規模)
 *
 * 投入先: fact_ad_spend (既存)
 *   PK: (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度)
 *
 * 🚨 2026-09-27 に作り直した (Company DB構想 11 の Codex 設計レビュー D1):
 *   - **日ごとに置き換える**: レポートを最後まで取れて値の検査も通った期間だけ、その日の Amazon SP の行を消して入れ直す
 *     (以前は UPSERT だけ = レポートから消えた行 (止めたキャンペーン・入れ替えた対象) が残り続けた)。行の無い日も「0 行で取れた」として置き換える
 *   - **取得の完全性の記録 ads_fetch_days** (日ごと): どのレポート (report_id) で・いつ頼んだ取得 (generation = 頼んだ時刻 ms) か・行数・費用の合計。
 *     記録より古い取得 (後から届いた遅いレポート) では上書きしない。送り手 (Company DB) はこの記録と行を同じ読み取り取引で読む
 *   - **値の検査**: 日付・キャンペーン・クリック/表示/費用が読めない行が 1 つでもあれば、その期間は書かずに失敗 (欠落を 0 にしない)。
 *     想定外の応答 (配列でない) も失敗。広告経由の売上 (sales1d = 1 日) と数量 (unitsSoldClicks1d) は無ければ null (購入件数 purchases1d を数量に混ぜない)。
 *     SKU も ASIN も無い行は ターゲット粒度 'none' で残す (費用を捨てない)
 *   - 日付は JST (Ads のプロファイルは日本): ふだんは JST の昨日まで直近 N 日
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-ads.js              # 直近30日
 *   node apps/warehouse/fetch-amazon-ads.js --days 60
 *   node apps/warehouse/fetch-amazon-ads.js --from 2026-04-01 --to 2026-04-30
 */

import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initDB, getDB } from './db.js';

const TOKEN_URL = 'https://api.amazon.com/auth/o2/token';
const ADS_API_HOST = 'https://advertising-api-fe.amazon.com';
const POLL_INTERVAL_MS = 30000;
const MAX_POLL_ATTEMPTS = 30;
const MAX_WINDOW_DAYS = 31;

const CLIENT_ID = process.env.AMAZON_ADS_CLIENT_ID;
const CLIENT_SECRET = process.env.AMAZON_ADS_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.AMAZON_ADS_REFRESH_TOKEN;
const PROFILE_ID = process.env.AMAZON_ADS_PROFILE_ID;

function requireEnv() {
  if (!CLIENT_ID || !CLIENT_SECRET || !REFRESH_TOKEN || !PROFILE_ID) {
    console.error('[AdsProduct] 環境変数 不足 (AMAZON_ADS_CLIENT_ID/SECRET/REFRESH_TOKEN/PROFILE_ID)');
    process.exit(1);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString().replace('T', ' ').slice(0, 19);

let cachedToken = null;
let cachedExpiry = 0;
async function getAccessToken() {
  if (cachedToken && Date.now() < cachedExpiry - 60000) return cachedToken;
  const res = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: REFRESH_TOKEN,
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
    }),
  });
  const json = await res.json();
  if (!json.access_token) throw new Error('access_token 取得失敗: ' + JSON.stringify(json));
  cachedToken = json.access_token;
  cachedExpiry = Date.now() + json.expires_in * 1000;
  return cachedToken;
}

async function adsHeaders() {
  return {
    'Authorization': `Bearer ${await getAccessToken()}`,
    'Amazon-Advertising-API-ClientId': CLIENT_ID,
    'Amazon-Advertising-API-Scope': PROFILE_ID,
    'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
  };
}

async function createSpAdvertisedProductReport(startDate, endDate) {
  console.log(`[AdsProduct] レポート作成: spAdvertisedProduct (${startDate}〜${endDate})`);
  const body = {
    name: `SP AdvertisedProduct ${startDate} - ${endDate}`,
    startDate,
    endDate,
    configuration: {
      adProduct: 'SPONSORED_PRODUCTS',
      groupBy: ['advertiser'],
      columns: [
        'date', 'campaignId', 'campaignName', 'adGroupId', 'adGroupName',
        'advertisedAsin', 'advertisedSku',
        'impressions', 'clicks', 'cost',
        'sales1d', 'sales7d', 'sales14d', 'sales30d',
        'purchases1d', 'unitsSoldClicks1d',
      ],
      reportTypeId: 'spAdvertisedProduct',
      timeUnit: 'DAILY',
      format: 'GZIP_JSON',
    },
  };
  const res = await fetch(`${ADS_API_HOST}/reporting/reports`, {
    method: 'POST',
    headers: await adsHeaders(),
    body: JSON.stringify(body),
  });
  const json = await res.json();
  if (!json.reportId) throw new Error('createReport失敗: ' + JSON.stringify(json));
  console.log(`[AdsProduct] reportId: ${json.reportId}`);
  return json.reportId;
}

async function pollReport(reportId) {
  for (let i = 0; i < MAX_POLL_ATTEMPTS; i++) {
    await sleep(POLL_INTERVAL_MS);
    const res = await fetch(`${ADS_API_HOST}/reporting/reports/${reportId}`, {
      headers: { ...(await adsHeaders()), 'Content-Type': 'application/json' },
    });
    const json = await res.json();
    console.log(`[AdsProduct] poll ${i + 1}: status=${json.status}`);
    if (json.status === 'COMPLETED') return json;
    if (['CANCELLED', 'FAILED'].includes(json.status)) {
      throw new Error('Report失敗: ' + JSON.stringify(json));
    }
  }
  throw new Error('Report timeout');
}

async function downloadReport(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`レポートのダウンロードに失敗 (HTTP ${res.status})`);
  const buf = Buffer.from(await res.arrayBuffer());
  const zlib = await import('zlib');
  const decompressed = zlib.gunzipSync(buf);
  const data = JSON.parse(decompressed.toString('utf-8'));
  // 🚨 GZIP_JSON は行の配列。想定外の形を空 (0 行) と読まない = 失敗
  if (!Array.isArray(data)) throw new Error(`レポートの形が想定外 (配列でない: ${Object.prototype.toString.call(data)})`);
  return data;
}

export const REPORT_TYPE = 'spAdvertisedProduct';
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const isRealDate = (d) => typeof d === 'string' && DATE_RE.test(d) && new Date(`${d}T00:00:00Z`).toISOString().slice(0, 10) === d;
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
export function datesBetween(from, to) { const out = []; for (let d = from; d <= to; d = addDays(d, 1)) out.push(d); return out; }
/** 0 以上の有限の数 (必須)。無い・数でない・負は例外 */
function reqNum(v, label, { integer = false } = {}) {
  const n = typeof v === 'number' ? v : (typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN);
  if (!Number.isFinite(n) || n < 0 || (integer && !Number.isInteger(n))) throw new Error(`${label} が 0 以上の${integer ? '整数' : '数'}でない: ${JSON.stringify(v)}`);
  return n;
}
/** 任意の数: 無ければ null (0 にしない)。あれば 0 以上の有限の数 */
function optNum(v, label, opts) { return v === undefined || v === null || v === '' ? null : reqNum(v, label, opts); }
const round2 = (x) => Math.round(x * 100) / 100;

/** 取得の完全性の記録 (日ごと)。この取込が作る (Company DB の送り手は行と同じ読み取り取引で読む) */
export function ensureFetchDays(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS ads_fetch_days (
    report_type TEXT NOT NULL,
    profile_id  TEXT NOT NULL,
    date_jst    TEXT NOT NULL,
    generation  INTEGER NOT NULL,
    report_id   TEXT NOT NULL,
    window_from TEXT NOT NULL,
    window_to   TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    cost_total  REAL NOT NULL,
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (report_type, profile_id, date_jst)
  )`);
}

/**
 * レポートの行を検査して (日, キャンペーン, 対象, 粒度) で合算する。1 行でも読めなければ例外 (その期間は書かない)
 * @returns {Map<string, object[]>} 日 → 合算済みの行
 */
export function aggregateReportRows(rows, { from, to }) {
  if (!Array.isArray(rows)) throw new Error('レポートの行が配列でない');
  const aggregated = new Map();
  rows.forEach((r, i) => {
    const at = `行 ${i + 1}`;
    if (!r || typeof r !== 'object') throw new Error(`${at} が行の形でない`);
    if (!isRealDate(r.date) || r.date < from || r.date > to) throw new Error(`${at} の date が期間 ${from}〜${to} の日付でない: ${JSON.stringify(r.date)}`);
    const campaignId = r.campaignId == null ? '' : String(r.campaignId).trim();
    if (!campaignId) throw new Error(`${at} の campaignId が無い`);
    const sku = r.advertisedSku == null ? '' : String(r.advertisedSku).trim();
    const asin = r.advertisedAsin == null ? '' : String(r.advertisedAsin).trim();
    // 同じ行に SKU と ASIN の両方があれば SKU (重複計上しない)。どちらも無い行も費用を捨てない = 粒度 'none'
    const [target, granularity] = sku ? [sku.toLowerCase(), 'sku'] : asin ? [asin.toLowerCase(), 'asin'] : ['', 'none'];
    const clicks = reqNum(r.clicks, `${at} の clicks`, { integer: true });
    const impressions = reqNum(r.impressions, `${at} の impressions`, { integer: true });
    const cost = reqNum(r.cost, `${at} の cost`);
    const sales1d = optNum(r.sales1d, `${at} の sales1d`);
    const units1d = optNum(r.unitsSoldClicks1d, `${at} の unitsSoldClicks1d`, { integer: true });   // purchases1d (購入件数) は混ぜない
    const key = `${campaignId}|${target}|${granularity}`;
    const day = aggregated.get(r.date) || new Map();
    const cur = day.get(key) || { date: r.date, campaignId, target, granularity, clicks: 0, impressions: 0, cost: 0, sales1d: 0, qty1d: 0, salesKnown: true, qtyKnown: true };
    cur.clicks += clicks; cur.impressions += impressions; cur.cost += cost;
    if (sales1d == null) cur.salesKnown = false; else cur.sales1d += sales1d;
    if (units1d == null) cur.qtyKnown = false; else cur.qty1d += units1d;
    day.set(key, cur);
    aggregated.set(r.date, day);
  });
  const out = new Map();
  for (const [d, m] of aggregated) out.set(d, [...m.values()].map((a) => ({ ...a, cost: round2(a.cost), sales1d: a.salesKnown ? round2(a.sales1d) : null, qty1d: a.qtyKnown ? a.qty1d : null })));
  return out;
}

/**
 * 最後まで取れて検査も通った期間 [from, to] を、日ごとに置き換える (1 取引)。
 * 🚨 記録 (ads_fetch_days) の generation がこの取得より新しい日は触らない (後から届いた古いレポートで戻さない)
 * @returns {{ days: number, rows: number, skippedOlder: string[] }}
 */
export function saveAdProduct(db, rows, { from, to, generation, reportId, profileId, now = () => new Date().toISOString() }) {
  if (!isRealDate(from) || !isRealDate(to) || from > to) throw new Error(`期間が不正: ${from}〜${to}`);
  if (!Number.isSafeInteger(generation) || generation <= 0) throw new Error('generation が無い');
  if (!reportId || !profileId) throw new Error('reportId / profileId が無い');
  ensureFetchDays(db);
  const byDay = aggregateReportRows(rows, { from, to });   // 例外ならここで止まる = 何も書かない
  const ts = now();
  const getGen = db.prepare('SELECT generation, report_id FROM ads_fetch_days WHERE report_type = ? AND profile_id = ? AND date_jst = ?');
  const del = db.prepare(`DELETE FROM fact_ad_spend WHERE 日付 = ? AND モール = 'amazon' AND 広告タイプ = 'SP'`);
  const ins = db.prepare(`
    INSERT INTO fact_ad_spend (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度, クリック数, インプレッション, 広告費, 広告経由売上, 広告経由数量, ingested_at)
    VALUES (?, 'amazon', ?, 'SP', ?, ?, ?, ?, ?, ?, ?, ?)`);
  const rec = db.prepare(`
    INSERT INTO ads_fetch_days (report_type, profile_id, date_jst, generation, report_id, window_from, window_to, row_count, cost_total, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(report_type, profile_id, date_jst) DO UPDATE SET generation = excluded.generation, report_id = excluded.report_id, window_from = excluded.window_from,
      window_to = excluded.window_to, row_count = excluded.row_count, cost_total = excluded.cost_total, fetched_at = excluded.fetched_at`);
  // 🚨 広告のプロファイル (アカウント) は 1 つだけ (fact_ad_spend にプロファイルの列が無い = 別のプロファイルの空のレポートで今の行を消さない。#1483 Codex R1 任意)
  const otherProfile = db.prepare('SELECT profile_id FROM ads_fetch_days WHERE report_type = ? AND profile_id <> ? LIMIT 1').get(REPORT_TYPE, String(profileId));
  if (otherProfile) throw new Error(`別の広告プロファイル (${otherProfile.profile_id}) の取得の記録がある = プロファイルは 1 つだけの前提 (今は ${profileId})`);
  let nRows = 0, nDays = 0; const skippedOlder = [], skippedSame = [];
  // immediate = 世代を読む前に書き込みの権利を取る (読んだ後に別の接続が書いて昇格で失敗するのを避ける)
  db.transaction(() => {
    for (const d of datesBetween(from, to)) {
      const cur = getGen.get(REPORT_TYPE, String(profileId), d);
      if (cur && Number(cur.generation) > generation) { skippedOlder.push(d); continue; }
      if (cur && Number(cur.generation) === generation) {
        // 同じ世代: 同じレポートの取り直し = 入れ直さない / 別のレポート = どちらが新しいか分からない = 期間ごと失敗 (#1483 Codex R1 P2)
        if (String(cur.report_id) === String(reportId)) { skippedSame.push(d); continue; }
        throw new Error(`${d}: 同じ世代 (${generation}) の別のレポート (${cur.report_id} / ${reportId}) = どちらが新しいか分からないので書かない`);
      }
      const dayRows = byDay.get(d) || [];   // 行の無い日 = 0 行で取れた (レポートを最後まで取れているので「取れていない」ではない)
      del.run(d);
      for (const a of dayRows) { ins.run(a.date, a.campaignId, a.target, a.granularity, a.clicks, a.impressions, a.cost, a.sales1d, a.qty1d, ts); nRows++; }
      rec.run(REPORT_TYPE, String(profileId), d, generation, String(reportId), from, to, dayRows.length, round2(dayRows.reduce((x, a) => x + a.cost, 0)), ts);
      nDays++;
    }
  }).immediate();
  return { days: nDays, rows: nRows, skippedOlder, skippedSame };
}

function splitDateRange(from, to) {
  const ranges = [];
  let cur = from;
  while (cur <= to) {
    const end = addDays(cur, MAX_WINDOW_DAYS - 1) < to ? addDays(cur, MAX_WINDOW_DAYS - 1) : to;
    ranges.push({ startDate: cur, endDate: end });
    cur = addDays(end, 1);
  }
  return ranges;
}

/** JST の今日 (YYYY-MM-DD) */
export const jstToday = (nowMs = Date.now()) => new Date(nowMs + 9 * 3600000).toISOString().slice(0, 10);
/** ふだんは JST の昨日まで直近 days 日 (Ads のプロファイルは日本。今日は途中なので取らない) */
export function parseArgs(argv = process.argv.slice(2), nowMs = Date.now()) {
  const result = { days: 30, from: null, to: null };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--days' && argv[i + 1]) result.days = parseInt(argv[++i], 10);
    else if (argv[i] === '--from' && argv[i + 1]) result.from = argv[++i];
    else if (argv[i] === '--to' && argv[i + 1]) result.to = argv[++i];
  }
  if (!Number.isInteger(result.days) || result.days < 1) throw new Error(`--days が不正: ${result.days}`);
  if (!result.from) {
    result.to = addDays(jstToday(nowMs), -1);
    result.from = addDays(result.to, -(result.days - 1));
  }
  if (!result.to) result.to = addDays(jstToday(nowMs), -1);
  if (!isRealDate(result.from) || !isRealDate(result.to) || result.from > result.to) throw new Error(`期間が不正: ${result.from}〜${result.to}`);
  return result;
}

async function main() {
  requireEnv();
  const args = parseArgs();
  console.log(`[AdsProduct] 取得期間: ${args.from}〜${args.to}`);

  await initDB();
  const db = getDB();

  const ranges = splitDateRange(args.from, args.to);
  console.log(`[AdsProduct] 分割: ${ranges.length}個 (各最大${MAX_WINDOW_DAYS}日)`);

  let totalSaved = 0;
  let failedRanges = 0;
  for (const range of ranges) {
    console.log(`\n--- 期間: ${range.startDate} 〜 ${range.endDate} ---`);
    try {
      const generation = Date.now();   // この取得の世代 = レポートを頼んだ時刻 (後から届いた古いレポートで新しい取得を戻さない)
      const reportId = await createSpAdvertisedProductReport(range.startDate, range.endDate);
      const completed = await pollReport(reportId);
      const downloadUrl = completed.url;
      if (!downloadUrl) {
        console.error('[AdsProduct] download URL なし:', JSON.stringify(completed));
        failedRanges++;
        continue;
      }
      const rows = await downloadReport(downloadUrl);
      console.log(`[AdsProduct] 行数: ${rows.length}`);
      const saved = saveAdProduct(db, rows, { from: range.startDate, to: range.endDate, generation, reportId, profileId: PROFILE_ID });
      totalSaved += saved.rows;
      console.log(`[AdsProduct] ✅ ${saved.days} 日を置き換え (${saved.rows} 行)${saved.skippedOlder.length ? ` / 新しい取得がある日は触らない ${saved.skippedOlder.length} 日` : ''}`);
    } catch (e) {
      console.error(`[AdsProduct] 期間 ${range.startDate}〜${range.endDate} 失敗:`, e.message);
      failedRanges++;
    }
  }

  console.log(`\n[AdsProduct] 完了: 累計 ${totalSaved}件 投入 (失敗 ${failedRanges}/${ranges.length} 期間)`);

  // 月次サマリ
  const summary = db.prepare(`
    SELECT substr(日付, 1, 7) AS year_month,
      COUNT(DISTINCT ターゲット) AS skus_or_asins,
      ROUND(SUM(広告費)) AS total_cost,
      ROUND(SUM(広告経由売上)) AS total_sales
    FROM fact_ad_spend WHERE モール = 'amazon'
      AND 日付 >= ? AND 日付 <= ?
      AND ターゲット粒度 IN ('sku', 'asin')
    GROUP BY year_month ORDER BY year_month
  `).all(args.from, args.to);
  console.log(`[AdsProduct] サマリ:`);
  console.table(summary);

  // 部分失敗があれば exit 1 (daily-sync が失敗扱いにできるよう)
  if (failedRanges > 0) {
    console.error(`[AdsProduct] ❌ ${failedRanges}/${ranges.length} 期間が失敗 → 不完全データ`);
    process.exit(1);
  }
  // 通信 (mirror POST / GChat通知) 直後の process.exit() は Windows node で libuv assertion
  // (UV_HANDLE_CLOSING / abort) を踏み、成功しているのに失敗扱いになる → exitCode + 自然終了 (#614 と同根)
  process.exitCode = 0;
}

// 直接起動のときだけ (試験から import しても取得が走らない。retry-failed-jobs.js と同じ realpath の判定)
const realPath = (p) => { try { return fs.realpathSync.native(p); } catch { return path.resolve(p); } };
const foldCase = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);
const isMain = !!process.argv[1] && foldCase(realPath(process.argv[1])) === foldCase(realPath(fileURLToPath(import.meta.url)));
if (isMain) main().catch(e => {
  console.error('[AdsProduct] FATAL:', e);
  process.exit(1);
});
