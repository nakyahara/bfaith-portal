/**
 * CSV生成 + Google Drive保存
 * 9:00 AM JSTにスケジューラーから呼ばれる
 *
 * 環境変数:
 *   GOOGLE_SERVICE_ACCOUNT_KEY — サービスアカウントJSONキー（Base64エンコード）
 *   GOOGLE_DRIVE_FILE_ID       — 上書き対象のGoogle DriveファイルID
 */
import { google } from 'googleapis';
import { Readable } from 'stream';
import {
  DATA_FILE, readJson, getRakutenUrl, getYahooUrl, getAmazonAsin, today, log,
} from './auto-check.js';

// ── CSV generation ──

function displayRank(rank) {
  if (rank === null || rank === undefined) return '';
  if (rank === 'error') return 'エラー';
  return String(rank);
}

function urlToCode(url) {
  if (!url) return '';
  const m = url.match(/item\.rakuten\.co\.jp\/b-faith\/([^/?#]+)/i);
  return m ? m[1] : '';
}

function calcAvgRank(history, field, daysStart, daysEnd) {
  if (!history || !history.length) return null;
  const now = new Date(Date.now() + 9 * 60 * 60 * 1000); // JST
  const vals = [];
  for (const entry of history) {
    const d = new Date(entry.date + 'T00:00:00+09:00');
    const diff = Math.floor((now - d) / 86400000);
    if (diff >= daysStart && diff <= daysEnd && entry[field] != null && entry[field] !== 'error') {
      vals.push(entry[field]);
    }
  }
  if (!vals.length) return null;
  return Math.round(vals.reduce((a, b) => a + b, 0) / vals.length * 10) / 10;
}

function getLatestEntry(history) {
  if (!history || !history.length) return null;
  return history[history.length - 1];
}

export function generateSummaryCSV(products) {
  const header = [
    '商品コード','検索ワード','楽天URL','Yahoo!URL','Amazon ASIN','チェック日',
    '楽天順位','楽天1-2日前','楽天3-10日前','楽天11-30日前','楽天31-90日前','楽天91-180日前',
    'Yahoo!順位','Yahoo!1-2日前','Yahoo!3-10日前','Yahoo!11-30日前','Yahoo!31-90日前','Yahoo!91-180日前',
    'Amazon順位','Amazon1-2日前','Amazon3-10日前','Amazon11-30日前','Amazon31-90日前','Amazon91-180日前',
    '競合①URL','競合①順位','競合①1-2日前','競合①3-10日前','競合①11-30日前','競合①31-90日前','競合①91-180日前',
    '競合②URL','競合②順位','競合②1-2日前','競合②3-10日前','競合②11-30日前','競合②31-90日前','競合②91-180日前',
    'レビュー件数',
  ];
  const rows = [header];

  for (const p of products) {
    const latest = getLatestEntry(p.history);
    const h = p.history;
    const av = (f, s, e) => { const v = calcAvgRank(h, f, s, e); return v !== null ? v : ''; };
    const amzAsin = getAmazonAsin(p);
    const code = p.product_code || urlToCode(getRakutenUrl(p));
    const yahooUrl = getYahooUrl(p);

    rows.push([
      code, p.keyword, getRakutenUrl(p), yahooUrl, amzAsin, latest ? latest.date : '',
      latest ? displayRank(latest.own_rank) : '',
      av('own_rank',1,2),av('own_rank',3,10),av('own_rank',11,30),av('own_rank',31,90),av('own_rank',91,180),
      latest && yahooUrl ? displayRank(latest.yahoo_own_rank) : '',
      yahooUrl?av('yahoo_own_rank',1,2):'', yahooUrl?av('yahoo_own_rank',3,10):'',
      yahooUrl?av('yahoo_own_rank',11,30):'', yahooUrl?av('yahoo_own_rank',31,90):'',
      yahooUrl?av('yahoo_own_rank',91,180):'',
      latest && amzAsin ? displayRank(latest.amazon_own_rank) : '',
      amzAsin?av('amazon_own_rank',1,2):'', amzAsin?av('amazon_own_rank',3,10):'',
      amzAsin?av('amazon_own_rank',11,30):'', amzAsin?av('amazon_own_rank',31,90):'',
      amzAsin?av('amazon_own_rank',91,180):'',
      p.competitor1_url||'',
      latest&&p.competitor1_url?displayRank(latest.competitor1_rank):'',
      p.competitor1_url?av('competitor1_rank',1,2):'', p.competitor1_url?av('competitor1_rank',3,10):'',
      p.competitor1_url?av('competitor1_rank',11,30):'', p.competitor1_url?av('competitor1_rank',31,90):'',
      p.competitor1_url?av('competitor1_rank',91,180):'',
      p.competitor2_url||'',
      latest&&p.competitor2_url?displayRank(latest.competitor2_rank):'',
      p.competitor2_url?av('competitor2_rank',1,2):'', p.competitor2_url?av('competitor2_rank',3,10):'',
      p.competitor2_url?av('competitor2_rank',11,30):'', p.competitor2_url?av('competitor2_rank',31,90):'',
      p.competitor2_url?av('competitor2_rank',91,180):'',
      p.review_count != null ? p.review_count : '',
    ]);
  }

  return '\uFEFF' + rows.map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(',')).join('\n');
}

// ── Google Drive upload ──

async function getGoogleAuth() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) return null;

  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });
  return auth;
}

export async function exportCSVToDrive() {
  const fileId = process.env.GOOGLE_DRIVE_FILE_ID;
  if (!fileId) {
    log('GOOGLE_DRIVE_FILE_ID が未設定。CSV出力スキップ');
    return;
  }

  const data = readJson(DATA_FILE, { products: [] });
  const products = data.products || [];
  if (!products.length) {
    log('商品データなし。CSV出力スキップ');
    return;
  }

  const csv = generateSummaryCSV(products);
  log(`CSV生成完了: ${products.length} 商品, ${csv.length} bytes`);

  const auth = await getGoogleAuth();
  if (!auth) {
    log('GOOGLE_SERVICE_ACCOUNT_KEY が未設定。Drive保存スキップ');
    return;
  }

  const drive = google.drive({ version: 'v3', auth });

  // 既存ファイルを上書き
  await drive.files.update({
    fileId,
    media: {
      mimeType: 'text/csv',
      body: Readable.from(csv),
    },
  });

  log(`Google Drive保存完了: fileId=${fileId}`);
}
