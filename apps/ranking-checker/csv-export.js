/**
 * CSV生成 + Google Drive保存（SQLite版）
 * 9:00 JST にスケジューラーから呼ばれる
 *
 * 設計:
 *   - products を iterator で1件ずつ取り出す
 *   - 各商品の history (最大365日) だけをメモリに載せる
 *   - 全件 JSON を一度に展開しない
 *
 * 環境変数:
 *   GOOGLE_SERVICE_ACCOUNT_KEY — サービスアカウントJSONキー（Base64エンコード）
 *   GOOGLE_DRIVE_FILE_ID       — 上書き対象のGoogle DriveファイルID
 */
import { google } from 'googleapis';
import { Readable } from 'stream';
import * as rdb from './db.js';
// Phase 3 で auto-check.js を削除する際にこの import が宙吊りにならないよう、
// 共通ヘルパーは helpers.js 経由で使う。
import { getRakutenUrl, getYahooUrl, getAmazonAsin, log, RANK_ERROR } from './helpers.js';

function displayRank(rank) {
  if (rank === null || rank === undefined) return '';
  if (rank === RANK_ERROR) return 'エラー';
  return String(rank);
}

function urlToCode(url) {
  if (!url) return '';
  const m = url.match(/item\.rakuten\.co\.jp\/b-faith\/([^/?#]+)/i);
  return m ? m[1] : '';
}

function calcAvgRank(history, field, daysStart, daysEnd) {
  if (!history || !history.length) return null;
  const now = new Date(Date.now() + 9 * 60 * 60 * 1000);
  const vals = [];
  for (const entry of history) {
    const d = new Date(entry.date + 'T00:00:00+09:00');
    const diff = Math.floor((now - d) / 86400000);
    if (diff >= daysStart && diff <= daysEnd && entry[field] != null && entry[field] !== RANK_ERROR) {
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

const HEADER = [
  '商品コード','検索ワード','楽天URL','Yahoo!URL','Amazon ASIN','チェック日',
  '楽天順位','楽天1-2日前','楽天3-10日前','楽天11-30日前','楽天31-90日前','楽天91-180日前',
  'Yahoo!順位','Yahoo!1-2日前','Yahoo!3-10日前','Yahoo!11-30日前','Yahoo!31-90日前','Yahoo!91-180日前',
  'Amazon順位','Amazon1-2日前','Amazon3-10日前','Amazon11-30日前','Amazon31-90日前','Amazon91-180日前',
  '競合①URL','競合①順位','競合①1-2日前','競合①3-10日前','競合①11-30日前','競合①31-90日前','競合①91-180日前',
  '競合②URL','競合②順位','競合②1-2日前','競合②3-10日前','競合②11-30日前','競合②31-90日前','競合②91-180日前',
  'レビュー件数',
];

function csvEscape(v) { return `"${String(v == null ? '' : v).replace(/"/g, '""')}"`; }
function csvRow(cols) { return cols.map(csvEscape).join(','); }

/**
 * 商品1件分のCSV行を組み立てる。history は呼び出し側で取得済み（per-product で小さい）。
 */
function buildRowFor(p, history) {
  const latest = getLatestEntry(history);
  const av = (f, s, e) => { const v = calcAvgRank(history, f, s, e); return v !== null ? v : ''; };
  const amzAsin = getAmazonAsin(p);
  const code = p.product_code || urlToCode(getRakutenUrl(p));
  const yahooUrl = getYahooUrl(p);

  return [
    code, p.keyword, getRakutenUrl(p), yahooUrl, amzAsin, latest ? latest.date : '',
    latest ? displayRank(latest.own_rank) : '',
    av('own_rank',1,2), av('own_rank',3,10), av('own_rank',11,30), av('own_rank',31,90), av('own_rank',91,180),
    latest && yahooUrl ? displayRank(latest.yahoo_own_rank) : '',
    yahooUrl ? av('yahoo_own_rank',1,2) : '', yahooUrl ? av('yahoo_own_rank',3,10) : '',
    yahooUrl ? av('yahoo_own_rank',11,30) : '', yahooUrl ? av('yahoo_own_rank',31,90) : '',
    yahooUrl ? av('yahoo_own_rank',91,180) : '',
    latest && amzAsin ? displayRank(latest.amazon_own_rank) : '',
    amzAsin ? av('amazon_own_rank',1,2) : '', amzAsin ? av('amazon_own_rank',3,10) : '',
    amzAsin ? av('amazon_own_rank',11,30) : '', amzAsin ? av('amazon_own_rank',31,90) : '',
    amzAsin ? av('amazon_own_rank',91,180) : '',
    p.competitor1_url || '',
    latest && p.competitor1_url ? displayRank(latest.competitor1_rank) : '',
    p.competitor1_url ? av('competitor1_rank',1,2) : '', p.competitor1_url ? av('competitor1_rank',3,10) : '',
    p.competitor1_url ? av('competitor1_rank',11,30) : '', p.competitor1_url ? av('competitor1_rank',31,90) : '',
    p.competitor1_url ? av('competitor1_rank',91,180) : '',
    p.competitor2_url || '',
    latest && p.competitor2_url ? displayRank(latest.competitor2_rank) : '',
    p.competitor2_url ? av('competitor2_rank',1,2) : '', p.competitor2_url ? av('competitor2_rank',3,10) : '',
    p.competitor2_url ? av('competitor2_rank',11,30) : '', p.competitor2_url ? av('competitor2_rank',31,90) : '',
    p.competitor2_url ? av('competitor2_rank',91,180) : '',
    p.review_count != null ? p.review_count : '',
  ];
}

/**
 * サマリーCSVを組み立てる。products を iterator で1件ずつ取り出しながら文字列を連結する。
 * 3566商品 × 40列 で数百KB程度なので、Drive upload の body としてまとめて渡す。
 */
export function generateSummaryCSV() {
  const parts = ['\uFEFF' + csvRow(HEADER) + '\n'];
  let count = 0;
  for (const p of rdb.iterAllProducts()) {
    const history = rdb.getHistory(p.id);
    parts.push(csvRow(buildRowFor(p, history)) + '\n');
    count++;
  }
  return { csv: parts.join(''), count };
}

/**
 * legacy shape (miniPC /service-api/rankcheck/data の返値) から CSV を組み立てる。
 * legacy の rank 'error' は DB表現の -1 に戻してから buildRowFor に渡す。
 */
function normalizeLegacyRank(v) {
  if (v === 'error') return RANK_ERROR;
  return v;
}
function legacyEntryToDbShape(entry) {
  return {
    date: entry.date,
    own_rank: normalizeLegacyRank(entry.own_rank),
    competitor1_rank: normalizeLegacyRank(entry.competitor1_rank),
    competitor2_rank: normalizeLegacyRank(entry.competitor2_rank),
    yahoo_own_rank: normalizeLegacyRank(entry.yahoo_own_rank),
    amazon_own_rank: normalizeLegacyRank(entry.amazon_own_rank),
  };
}
export function generateSummaryCSVFromLegacy(legacyShape) {
  const products = (legacyShape && legacyShape.products) || [];
  const parts = ['\uFEFF' + csvRow(HEADER) + '\n'];
  for (const p of products) {
    const history = (p.history || []).map(legacyEntryToDbShape);
    parts.push(csvRow(buildRowFor(p, history)) + '\n');
  }
  return { csv: parts.join(''), count: products.length };
}

/**
 * miniPC /service-api/rankcheck/data から legacy shape を fetch する。
 * CSV cron が Render で走るとき、DB は miniPC にあるのでこれ経由で取得する。
 */
async function fetchLegacyFromMiniPC() {
  const baseUrl = process.env.RANKCHECK_MINIPC_URL;
  if (!baseUrl) throw new Error('RANKCHECK_MINIPC_URL 未設定');
  const headers = {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
  };
  const res = await fetch(`${baseUrl}/service-api/rankcheck/data`, {
    headers, signal: AbortSignal.timeout(120000),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`miniPC /data HTTP ${res.status}: ${txt.slice(0, 200)}`);
  }
  return res.json();
}

async function getGoogleAuth() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) return null;
  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  return new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });
}

export async function exportCSVToDrive() {
  const fileId = process.env.GOOGLE_DRIVE_FILE_ID;
  if (!fileId) {
    log('GOOGLE_DRIVE_FILE_ID が未設定。CSV出力スキップ');
    return;
  }

  const proxyMode = !!process.env.RANKCHECK_MINIPC_URL;
  let csv, count;

  if (proxyMode) {
    log(`CSV生成: miniPC proxy モードで /service-api/rankcheck/data から取得`);
    const legacy = await fetchLegacyFromMiniPC();
    if (!legacy.products || legacy.products.length === 0) {
      log('商品データなし (miniPC)。CSV出力スキップ');
      return;
    }
    ({ csv, count } = generateSummaryCSVFromLegacy(legacy));
  } else {
    const total = rdb.countProducts();
    if (total === 0) {
      log('商品データなし。CSV出力スキップ');
      return;
    }
    ({ csv, count } = generateSummaryCSV());
  }

  log(`CSV生成完了: ${count} 商品, ${csv.length} bytes`);

  const auth = await getGoogleAuth();
  if (!auth) {
    log('GOOGLE_SERVICE_ACCOUNT_KEY が未設定。Drive保存スキップ');
    return;
  }

  const drive = google.drive({ version: 'v3', auth });
  await drive.files.update({
    fileId,
    media: { mimeType: 'text/csv', body: Readable.from(csv) },
  });
  log(`Google Drive保存完了: fileId=${fileId}`);
}
