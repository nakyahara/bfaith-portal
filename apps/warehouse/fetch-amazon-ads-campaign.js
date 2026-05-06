/**
 * fetch-amazon-ads-campaign.js — Amazon Ads spCampaigns レポート取得 (Phase 3.4)
 *
 * 目的: 47% 漏れ広告費の解消
 *   - 既存 fetch-amazon-ads.js (spAdvertisedProduct 経由) は SKU 別広告費を取得 = 月¥692K (43% 漏れ)
 *   - 本スクリプト (spCampaigns 経由) はキャンペーン全体の広告費を取得 = 月¥1.22M (漏れなし)
 *   - 差分 ¥530K = Auto-Targeting Campaign で advertised SKU が unallocated になる分
 *   - v4 view で「campaign 全広告費 - SKU 別合計 = unallocated」として月次集計時に補正
 *
 * 投入先: fact_ad_spend_campaign (新規テーブル)
 *   PK: (日付, モール, キャンペーンID, 広告タイプ)
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-ads-campaign.js              # 直近30日
 *   node apps/warehouse/fetch-amazon-ads-campaign.js --days 60
 *   node apps/warehouse/fetch-amazon-ads-campaign.js --from 2026-04-01 --to 2026-04-30
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
  console.error('[AdsCampaign] 環境変数 不足 (AMAZON_ADS_CLIENT_ID/SECRET/REFRESH_TOKEN/PROFILE_ID)');
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

async function createSpCampaignsReport(startDate, endDate) {
  console.log(`[AdsCampaign] レポート作成: spCampaigns (${startDate}〜${endDate})`);
  const body = {
    name: `SP Campaigns ${startDate} - ${endDate}`,
    startDate,
    endDate,
    configuration: {
      adProduct: 'SPONSORED_PRODUCTS',
      groupBy: ['campaign'],
      columns: [
        'date', 'campaignId', 'campaignName', 'campaignStatus',
        'impressions', 'clicks', 'cost',
        'sales1d', 'sales7d', 'sales14d', 'sales30d',
        'purchases1d', 'unitsSoldClicks1d',
      ],
      reportTypeId: 'spCampaigns',
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
  console.log(`[AdsCampaign] reportId: ${json.reportId}`);
  return json.reportId;
}

async function pollReport(reportId) {
  for (let i = 0; i < MAX_POLL_ATTEMPTS; i++) {
    await sleep(POLL_INTERVAL_MS);
    const res = await fetch(`${ADS_API_HOST}/reporting/reports/${reportId}`, {
      headers: { ...(await adsHeaders()), 'Content-Type': 'application/json' },
    });
    const json = await res.json();
    console.log(`[AdsCampaign] poll ${i + 1}: status=${json.status}`);
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

function saveAdCampaign(db, rows) {
  const ts = nowIso();
  const upsert = db.prepare(`
    INSERT INTO fact_ad_spend_campaign (
      日付, モール, キャンペーンID, キャンペーン名, 広告タイプ, キャンペーンステータス,
      クリック数, インプレッション, 広告費,
      広告経由売上_1d, 広告経由売上_7d, 広告経由売上_14d, 広告経由売上_30d,
      広告経由数量_1d,
      ingested_at
    )
    VALUES (?, 'amazon', ?, ?, 'SP', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(日付, モール, キャンペーンID, 広告タイプ) DO UPDATE SET
      キャンペーン名 = excluded.キャンペーン名,
      キャンペーンステータス = excluded.キャンペーンステータス,
      クリック数 = excluded.クリック数,
      インプレッション = excluded.インプレッション,
      広告費 = excluded.広告費,
      広告経由売上_1d = excluded.広告経由売上_1d,
      広告経由売上_7d = excluded.広告経由売上_7d,
      広告経由売上_14d = excluded.広告経由売上_14d,
      広告経由売上_30d = excluded.広告経由売上_30d,
      広告経由数量_1d = excluded.広告経由数量_1d,
      ingested_at = excluded.ingested_at
  `);

  let n = 0;
  const tx = db.transaction((arr) => {
    for (const r of arr) {
      if (!r.date || !r.campaignId) continue;
      upsert.run(
        r.date, String(r.campaignId), r.campaignName || '', r.campaignStatus || '',
        r.clicks || 0, r.impressions || 0, r.cost || 0,
        r.sales1d || 0, r.sales7d || 0, r.sales14d || 0, r.sales30d || 0,
        r.purchases1d || r.unitsSoldClicks1d || 0,
        ts,
      );
      n++;
    }
  });
  tx(rows);
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
  console.log(`[AdsCampaign] 取得期間: ${args.from}〜${args.to}`);

  await initDB();
  const db = getDB();

  const ranges = splitDateRange(args.from, args.to);
  console.log(`[AdsCampaign] 分割: ${ranges.length}個 (各最大${MAX_WINDOW_DAYS}日)`);

  let totalSaved = 0;
  for (const range of ranges) {
    console.log(`\n--- 期間: ${range.startDate} 〜 ${range.endDate} ---`);
    try {
      const reportId = await createSpCampaignsReport(range.startDate, range.endDate);
      const completed = await pollReport(reportId);
      const downloadUrl = completed.url;
      if (!downloadUrl) {
        console.error('[AdsCampaign] download URL なし:', JSON.stringify(completed));
        continue;
      }
      const data = await downloadReport(downloadUrl);
      const rows = Array.isArray(data) ? data : (data.rows || []);
      console.log(`[AdsCampaign] 行数: ${rows.length}`);
      const saved = saveAdCampaign(db, rows);
      totalSaved += saved;
      console.log(`[AdsCampaign] ✅ ${saved}件 投入`);
    } catch (e) {
      console.error(`[AdsCampaign] 期間 ${range.startDate}〜${range.endDate} 失敗:`, e.message);
    }
  }

  console.log(`\n[AdsCampaign] 完了: 累計 ${totalSaved}件 投入`);

  // 月次サマリ
  const summary = db.prepare(`
    SELECT substr(日付, 1, 7) AS year_month,
      COUNT(DISTINCT キャンペーンID) AS campaigns,
      ROUND(SUM(広告費)) AS total_cost,
      ROUND(SUM(広告経由売上_1d)) AS total_sales_1d
    FROM fact_ad_spend_campaign WHERE モール = 'amazon'
      AND 日付 >= ? AND 日付 <= ?
    GROUP BY year_month ORDER BY year_month
  `).all(args.from, args.to);
  console.log(`[AdsCampaign] サマリ:`);
  console.table(summary);
  process.exit(0);
}

main().catch(e => {
  console.error('[AdsCampaign] FATAL:', e);
  process.exit(1);
});
