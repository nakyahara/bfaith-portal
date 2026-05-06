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
 * 使い方:
 *   node apps/warehouse/fetch-amazon-ads.js              # 直近30日
 *   node apps/warehouse/fetch-amazon-ads.js --days 60
 *   node apps/warehouse/fetch-amazon-ads.js --from 2026-04-01 --to 2026-04-30
 */

import 'dotenv/config';
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

if (!CLIENT_ID || !CLIENT_SECRET || !REFRESH_TOKEN || !PROFILE_ID) {
  console.error('[AdsProduct] 環境変数 不足 (AMAZON_ADS_CLIENT_ID/SECRET/REFRESH_TOKEN/PROFILE_ID)');
  process.exit(1);
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
  const buf = Buffer.from(await res.arrayBuffer());
  const zlib = await import('zlib');
  const decompressed = zlib.gunzipSync(buf);
  return JSON.parse(decompressed.toString('utf-8'));
}

function saveAdProduct(db, rows) {
  const ts = nowIso();
  // fact_ad_spend PK は (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度)
  // adGroupId を含まないので、同一 SKU が複数 ad group に載るときは事前合算が必須
  // (Codex round 1 指摘: 後勝ち UPSERT で広告費が欠損するため)
  const upsert = db.prepare(`
    INSERT INTO fact_ad_spend (
      日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度,
      クリック数, インプレッション, 広告費,
      広告経由売上, 広告経由数量,
      ingested_at
    )
    VALUES (?, 'amazon', ?, 'SP', ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度) DO UPDATE SET
      クリック数 = excluded.クリック数,
      インプレッション = excluded.インプレッション,
      広告費 = excluded.広告費,
      広告経由売上 = excluded.広告経由売上,
      広告経由数量 = excluded.広告経由数量,
      ingested_at = excluded.ingested_at
  `);

  // step 1: ad group 別の行を (date, campaignId, target, granularity) キーで合算
  const aggregated = new Map();
  for (const r of rows) {
    if (!r.date || !r.campaignId) continue;
    const sku = r.advertisedSku || '';
    const asin = r.advertisedAsin || '';
    let target = '', granularity = '';
    if (sku) {
      target = String(sku).toLowerCase();
      granularity = 'sku';
    } else if (asin) {
      // SKU 不明だが ASIN だけある場合 (同一行に SKU/ASIN 両方ある場合は SKU 行のみ、重複計上回避)
      target = String(asin).toLowerCase();
      granularity = 'asin';
    } else continue;
    const key = `${r.date}|${r.campaignId}|${target}|${granularity}`;
    const cur = aggregated.get(key) || {
      date: r.date,
      campaignId: String(r.campaignId),
      target,
      granularity,
      clicks: 0, impressions: 0, cost: 0, sales1d: 0, qty1d: 0,
    };
    cur.clicks += r.clicks || 0;
    cur.impressions += r.impressions || 0;
    cur.cost += r.cost || 0;
    cur.sales1d += r.sales1d || 0;
    cur.qty1d += r.purchases1d || r.unitsSoldClicks1d || 0;
    aggregated.set(key, cur);
  }

  // step 2: 合算済を UPSERT
  let n = 0;
  const tx = db.transaction((items) => {
    for (const a of items) {
      upsert.run(
        a.date, a.campaignId, a.target, a.granularity,
        a.clicks, a.impressions, a.cost,
        a.sales1d, a.qty1d,
        ts,
      );
      n++;
    }
  });
  tx(Array.from(aggregated.values()));
  return n;
}

function splitDateRange(from, to) {
  const ranges = [];
  let cur = new Date(from);
  const end = new Date(to);
  while (cur <= end) {
    const winEnd = new Date(Math.min(cur.getTime() + (MAX_WINDOW_DAYS - 1) * 86400000, end.getTime()));
    ranges.push({
      startDate: cur.toISOString().slice(0, 10),
      endDate: winEnd.toISOString().slice(0, 10),
    });
    cur = new Date(winEnd.getTime() + 86400000);
  }
  return ranges;
}

function parseArgs() {
  const args = process.argv.slice(2);
  const result = { days: 30, from: null, to: null };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--days' && args[i + 1]) result.days = parseInt(args[++i], 10);
    else if (args[i] === '--from' && args[i + 1]) result.from = args[++i];
    else if (args[i] === '--to' && args[i + 1]) result.to = args[++i];
  }
  if (!result.from) {
    const to = new Date();
    const from = new Date(to.getTime() - result.days * 86400000);
    result.from = from.toISOString().slice(0, 10);
    result.to = to.toISOString().slice(0, 10);
  }
  return result;
}

async function main() {
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
      const reportId = await createSpAdvertisedProductReport(range.startDate, range.endDate);
      const completed = await pollReport(reportId);
      const downloadUrl = completed.url;
      if (!downloadUrl) {
        console.error('[AdsProduct] download URL なし:', JSON.stringify(completed));
        failedRanges++;
        continue;
      }
      const data = await downloadReport(downloadUrl);
      const rows = Array.isArray(data) ? data : (data.rows || []);
      console.log(`[AdsProduct] 行数: ${rows.length}`);
      const saved = saveAdProduct(db, rows);
      totalSaved += saved;
      console.log(`[AdsProduct] ✅ ${saved}件 投入`);
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
  process.exit(0);
}

main().catch(e => {
  console.error('[AdsProduct] FATAL:', e);
  process.exit(1);
});
