/**
 * sp-api-report.js — SP-API のレポート取得の共通の部品 (AI の分析とアラート用のレポート・2026-10-22)
 *
 * createReport (reportOptions・dataStartTime/dataEndTime を渡せる) → getReport の polling (上限は引数) → getReportDocument
 * → ダウンロード → gzip 解凍 → 文字コードの判定 (UTF-8 / CP932) → TSV (行の配列) か JSON を返す。
 *
 * 使う側: fetch-amazon-sales-traffic.js / fetch-amazon-returns.js / fetch-amazon-reimbursements.js / fetch-amazon-ledger.js
 *   (apps/fba-replenishment/sp-api-reports.js の fetchReport は export されておらず、期間・reportOptions・polling の上限を渡せないので別に作った)
 *
 * 🚨 決まり:
 *   - SP クライアント (callAPI を持つもの)・fetch・sleep・時計は引数で差し替えられる (試験は見本のデータで回す)
 *   - FATAL / CANCELLED / polling の上限 / ダウンロードの失敗 / 解凍・解析の失敗 は SpReportError (code つき) で止める。
 *     CANCELLED は Amazon の説明では「データが無いと自動で取り消す」こともあるが、明示の取り消しと見分けられない = ここでは 0 行とは読まない (期間を空で置き換えない)。
 *     UPSERT だけのレポート (返品・補てん) は fetchSpReportOrNoData で 0 行と読む (0 行の UPSERT は何も消さない)
 *   - 1 回の取得の世代 (generation) = createReport を頼んだ時刻 (ms)。後から届いた古い取得で新しい取得を戻さないために使う (fetch-amazon-ads.js と同じ考え方)
 *   - 取得の完全性の記録 sp_report_fetch_days (レポートの種類 × 日): どのレポート・いつ頼んだ取得で・何行あったか
 *
 * レート (SP-API Reports 2021-06-30 の公式の値): createReport / getReportDocument = 0.0167 回/秒 (1 分に 1 回)・burst 15、getReport = 2 回/秒・burst 15。
 *   amazon-sp-api は 429 (QuotaExceeded) を自分で待って再送する (auto_request_throttled)。ここでも念のため 429 を待って再送する。
 *   createReport / getReportDocument の 1 回の待ちの上限は 1 分の回復を待てる長さにする (60 秒では回復待ちの途中で切れる)
 */
import { gunzipSync, gzipSync } from 'node:zlib';
import iconv from 'iconv-lite';

export const DEFAULT_MARKETPLACE_ID = 'A1VC38T7YXB528';   // JP
const REPORTS_VERSION = '2021-06-30';
const FAILURE_DOC_MAX_BYTES = 64 * 1024;   // FATAL の理由文の文書は数十バイト。想定外に大きいものは読まない

export function marketplaceId() { return process.env.SP_API_MARKETPLACE_ID || DEFAULT_MARKETPLACE_ID; }

/** 本番の SP クライアント (JP・FE)。amazon-sp-api は使うときにだけ読む (試験では読まない) */
let spClient = null;
export async function createSpClient() {
  if (spClient) return spClient;
  const { default: SellingPartner } = await import('amazon-sp-api');
  spClient = new SellingPartner({
    region: 'fe',
    refresh_token: process.env.SP_API_REFRESH_TOKEN,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });
  return spClient;
}

/** レポートの取得の失敗。code = FATAL / CANCELLED / TIMEOUT / CREATE / NO_DOCUMENT / DOWNLOAD / DECODE / PARSE */
export class SpReportError extends Error {
  constructor(message, { code, reportType = null, reportId = null, status = null, reason = null, generation = null } = {}) {
    super(message);
    this.name = 'SpReportError';
    this.code = code;
    this.reportType = reportType;
    this.reportId = reportId;
    this.status = status;
    this.reason = reason;
    this.generation = generation;   // createReport を頼んだ時刻 (頼んだ後の失敗のとき)
  }
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function withTimeout(promise, ms, label = '') {
  // 終わったらタイマーを片付ける (残すとプロセスが終われない。sp-api-reports.js と同じ。Codex #1386 R1)
  let timer;
  const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`タイムアウト (${ms / 1000}秒): ${label}`)), ms); });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

const isThrottle = (e) => /QuotaExceeded|429|throttl|TooManyRequests/i.test(`${e?.code || ''} ${e?.message || ''}`);

/** callAPI を 1 回。429 は waitMs × 回数 待って再送 (attempts 回まで)。それ以外の失敗はそのまま投げる (retryOther のときだけ短く待って再送) */
async function callApi(sp, params, { label, timeoutMs, attempts, waitMs, sleepImpl, log, retryOther = 0 }) {
  let other = 0;
  for (let attempt = 1; ; attempt++) {
    try {
      return await withTimeout(sp.callAPI(params), timeoutMs, label);
    } catch (e) {
      if (isThrottle(e) && attempt < attempts) {
        log(`  ⚠ ${label}: 混雑 (429) → ${Math.round(waitMs * attempt / 1000)} 秒待って再送 (${attempt}/${attempts - 1})`);
        await sleepImpl(waitMs * attempt);
        continue;
      }
      if (!isThrottle(e) && other < retryOther) {
        other++;
        log(`  ⚠ ${label}: 失敗 (${String(e?.message || e).slice(0, 120)}) → 再送 (${other}/${retryOther})`);
        await sleepImpl(5000);
        continue;
      }
      throw e;
    }
  }
}

/** 応答の本文を上限つきで読む (理由文用) */
async function readBounded(res, maxBytes) {
  if (!res.body || typeof res.body.getReader !== 'function') {
    const buf = Buffer.from(await res.arrayBuffer());
    return buf.length > maxBytes ? null : buf;
  }
  const reader = res.body.getReader();
  const chunks = []; let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) { try { await reader.cancel(); } catch (_) { /* 打ち切り */ } return null; }
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks);
}

/** FATAL / CANCELLED のレポートの理由文 (reportDocument の errorDetails)。読めなければ null */
async function readFailureReason(sp, report, { fetchImpl, sleepImpl, log, docTimeoutMs }) {
  if (!report?.reportDocumentId) return null;
  try {
    const doc = await callApi(sp, { operation: 'getReportDocument', endpoint: 'reports', path: { reportDocumentId: report.reportDocumentId }, options: { version: REPORTS_VERSION } },
      { label: 'getReportDocument(理由文)', timeoutMs: docTimeoutMs, attempts: 2, waitMs: 65000, sleepImpl, log });
    const res = await fetchImpl(doc.url, { signal: AbortSignal.timeout(30000) });
    if (!res.ok) return null;   // 配信側 (S3) の HTTP エラー本文を Amazon の理由文として扱わない
    let buf = await readBounded(res, FAILURE_DOC_MAX_BYTES);
    if (!buf) return '(理由文の文書が大きすぎて読まなかった)';
    if (doc.compressionAlgorithm === 'GZIP') buf = gunzipSync(buf, { maxOutputLength: FAILURE_DOC_MAX_BYTES });
    const text = buf.toString('utf8').slice(0, 2000);
    try { const j = JSON.parse(text); if (j && typeof j.errorDetails === 'string') return j.errorDetails; } catch (_) { /* JSON でない */ }
    return text.replace(/\s+/g, ' ').trim() || null;
  } catch (_) {
    return null;
  }
}

/**
 * ダウンロードした文書を文字列にする: gzip (compressionAlgorithm が GZIP か、先頭が gzip の印) を解凍 → BOM を外す →
 * UTF-8 として正しければ UTF-8・正しくなければ CP932 (Shift_JIS の Windows 版。JP のフラットファイルのレポートは Shift_JIS のことがある)
 * @returns {{ text: string, encoding: 'utf-8'|'cp932', bytes: number }}
 */
export function decodeReportBuffer(buf, compressionAlgorithm = null) {
  if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf);
  let data = buf;
  const gzipMagic = buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b;
  if (compressionAlgorithm === 'GZIP' || gzipMagic) {
    try { data = gunzipSync(buf); } catch (e) { throw new SpReportError(`レポートの文書を解凍できない (${compressionAlgorithm || 'gzip の印'}): ${e.message}`, { code: 'DECODE' }); }
  } else if (compressionAlgorithm) {
    throw new SpReportError(`知らない圧縮 (${compressionAlgorithm})`, { code: 'DECODE' });
  }
  if (data.length >= 3 && data[0] === 0xef && data[1] === 0xbb && data[2] === 0xbf) {
    return { text: data.subarray(3).toString('utf8'), encoding: 'utf-8', bytes: data.length };
  }
  try {
    return { text: new TextDecoder('utf-8', { fatal: true }).decode(data), encoding: 'utf-8', bytes: data.length };
  } catch (_) {
    return { text: iconv.decode(data, 'CP932'), encoding: 'cp932', bytes: data.length };
  }
}

/** TSV の 1 つの値: 前後の空白を外し、"..." で囲まれていれば外す ("" は " に戻す。在庫の台帳のレポートは値を "" で囲む) */
function unquote(v) {
  const s = String(v ?? '').trim();
  if (s.length >= 2 && s.startsWith('"') && s.endsWith('"')) return s.slice(1, -1).replace(/""/g, '"').trim();
  return s;
}

/**
 * TSV の文字列を { headers, rows } にする。rows の各行は 見出し → 値 (文字列・前後の空白は外す) と __line (元の行番号)
 * 🚨 列が見出しより多い行は例外 (列がずれた行を黙って読まない)。joinExtraIntoLast = 最後の列が自由な文 (返品の購入者のコメント) のときだけ、余った列を最後の列にタブでつなぐ
 *    列が見出しより少ない行は足りない列を '' (= 空) にする (必須の列なら各レポートの検査で止まる)。見出しの重複は例外
 */
export function parseTsv(text, { joinExtraIntoLast = false } = {}) {
  let s = String(text ?? '');
  if (s.charCodeAt(0) === 0xfeff) s = s.slice(1);
  const lines = s.split(/\r?\n/);
  let hi = lines.findIndex((l) => l.trim() !== '');
  if (hi < 0) return { headers: [], rows: [] };
  const headers = lines[hi].split('\t').map(unquote);
  const seen = new Set();
  for (const h of headers) {
    if (h === '') throw new SpReportError(`TSV の見出しに空の列がある (行 ${hi + 1})`, { code: 'PARSE' });
    if (seen.has(h)) throw new SpReportError(`TSV の見出しが重複: ${h}`, { code: 'PARSE' });
    seen.add(h);
  }
  const rows = [];
  for (let i = hi + 1; i < lines.length; i++) {
    if (lines[i].trim() === '') continue;
    let cols = lines[i].split('\t');
    if (cols.length > headers.length) {
      if (!joinExtraIntoLast) throw new SpReportError(`TSV の行 ${i + 1} の列が見出しより多い (${cols.length} > ${headers.length})`, { code: 'PARSE' });
      cols = [...cols.slice(0, headers.length - 1), cols.slice(headers.length - 1).join('\t')];
    }
    const row = { __line: i + 1 };
    headers.forEach((h, j) => { row[h] = unquote(cols[j]); });
    rows.push(row);
  }
  return { headers, rows };
}

/**
 * レポートを 1 つ取る: createReport → polling → getReportDocument → ダウンロード → 解凍・文字コード → 解析
 * @param {object} p
 * @param {{ callAPI: Function }} p.sp - SP クライアント (試験では差し替え)
 * @param {string} p.reportType
 * @param {'tsv'|'json'|'text'} [p.format='tsv']
 * @param {object} [p.reportOptions]
 * @param {string} [p.dataStartTime] / [p.dataEndTime] - ISO 8601
 * @param {number} [p.pollIntervalMs=30000] / [p.maxPolls=40] - polling の間隔と上限 (上限を超えたら TIMEOUT)
 * @param {Function} [p.beforeCreate] - createReport の前に待つ (createRateGate)
 * @returns {Promise<{ reportId, reportDocumentId, generation, encoding, compressionAlgorithm, bytes, text, headers?, rows?, json? }>}
 */
export async function fetchSpReport({
  sp, reportType, format = 'tsv',
  marketplaceIds = [marketplaceId()], reportOptions, dataStartTime, dataEndTime,
  pollIntervalMs = 30000, maxPolls = 40,
  createTimeoutMs = 150000, getTimeoutMs = 30000, docTimeoutMs = 150000, downloadTimeoutMs = 300000,
  throttleAttempts = 4, throttleWaitMs = 65000,
  joinExtraIntoLast = false,
  beforeCreate = null,   // createReport の前に待つ (createRateGate。レポートの種類ごとの上限)
  fetchImpl = globalThis.fetch, sleepImpl = sleep, now = Date.now, log = console.log,
}) {
  if (!sp || typeof sp.callAPI !== 'function') throw new Error('sp (callAPI を持つ SP クライアント) が無い');
  if (!reportType) throw new Error('reportType が無い');
  if (!['tsv', 'json', 'text'].includes(format)) throw new Error(`format が不正: ${format}`);
  if (!Number.isInteger(maxPolls) || maxPolls < 1) throw new Error(`maxPolls が不正: ${maxPolls}`);
  const common = { sleepImpl, log };
  const body = { reportType, marketplaceIds };
  if (reportOptions) body.reportOptions = reportOptions;
  if (dataStartTime) body.dataStartTime = dataStartTime;
  if (dataEndTime) body.dataEndTime = dataEndTime;

  if (beforeCreate) await beforeCreate();
  const generation = now();   // この取得の世代 = 頼んだ時刻
  const created = await callApi(sp, { operation: 'createReport', endpoint: 'reports', body, options: { version: REPORTS_VERSION } },
    { label: `${reportType} createReport`, timeoutMs: createTimeoutMs, attempts: throttleAttempts, waitMs: throttleWaitMs, ...common });
  const reportId = created?.reportId;
  if (!reportId) throw new SpReportError(`${reportType}: createReport の応答に reportId が無い: ${JSON.stringify(created).slice(0, 200)}`, { code: 'CREATE', reportType });
  log(`[SP-API] ${reportType} reportId=${reportId}${dataStartTime ? ` (${dataStartTime} 〜 ${dataEndTime || ''})` : ''}`);

  let report = null;
  for (let i = 0; i < maxPolls; i++) {
    await sleepImpl(pollIntervalMs);
    // getReport は読むだけ = 一時的な失敗は 2 回まで再送してよい
    report = await callApi(sp, { operation: 'getReport', endpoint: 'reports', path: { reportId }, options: { version: REPORTS_VERSION } },
      { label: `${reportType} getReport`, timeoutMs: getTimeoutMs, attempts: throttleAttempts, waitMs: 5000, retryOther: 2, ...common });
    const st = report?.processingStatus;
    if (i % 4 === 0 || ['DONE', 'FATAL', 'CANCELLED'].includes(st)) log(`[SP-API] ${reportType} polling ${i + 1}/${maxPolls}: ${st}`);
    if (['DONE', 'FATAL', 'CANCELLED'].includes(st)) break;
  }
  const status = report?.processingStatus || null;
  if (status === 'FATAL' || status === 'CANCELLED') {
    const reason = await readFailureReason(sp, report, { fetchImpl, docTimeoutMs, ...common });
    const what = status === 'CANCELLED' ? '取り消された (CANCELLED。期間にデータが無いときも Amazon は取り消す)' : '失敗した (FATAL)';
    throw new SpReportError(`${reportType} のレポートが${what}${reason ? `: ${reason}` : ''} (reportId=${reportId})`, { code: status, reportType, reportId, status, reason, generation });
  }
  if (status !== 'DONE') {
    throw new SpReportError(`${reportType} のレポートが ${maxPolls} 回の確認でも終わらない (最後の状態 ${status}・reportId=${reportId})`, { code: 'TIMEOUT', reportType, reportId, status, generation });
  }
  if (!report.reportDocumentId) throw new SpReportError(`${reportType}: DONE なのに reportDocumentId が無い (reportId=${reportId})`, { code: 'NO_DOCUMENT', reportType, reportId, status });

  const doc = await callApi(sp, { operation: 'getReportDocument', endpoint: 'reports', path: { reportDocumentId: report.reportDocumentId }, options: { version: REPORTS_VERSION } },
    { label: `${reportType} getReportDocument`, timeoutMs: docTimeoutMs, attempts: throttleAttempts, waitMs: throttleWaitMs, ...common });
  if (!doc?.url) throw new SpReportError(`${reportType}: getReportDocument の応答に url が無い`, { code: 'NO_DOCUMENT', reportType, reportId });

  let res;
  try {
    res = await fetchImpl(doc.url, { signal: AbortSignal.timeout(downloadTimeoutMs) });
  } catch (e) {
    throw new SpReportError(`${reportType}: 文書のダウンロードに失敗: ${e.message}`, { code: 'DOWNLOAD', reportType, reportId });
  }
  if (!res.ok) throw new SpReportError(`${reportType}: 文書のダウンロードに失敗 (HTTP ${res.status})`, { code: 'DOWNLOAD', reportType, reportId });
  const raw = Buffer.from(await res.arrayBuffer());
  let decoded;
  try { decoded = decodeReportBuffer(raw, doc.compressionAlgorithm || null); } catch (e) { e.reportType = reportType; e.reportId = reportId; throw e; }
  const out = { reportId, reportDocumentId: report.reportDocumentId, generation, encoding: decoded.encoding, compressionAlgorithm: doc.compressionAlgorithm || null, bytes: decoded.bytes, text: decoded.text };
  try {
    if (format === 'json') out.json = JSON.parse(decoded.text);
    else if (format === 'tsv') Object.assign(out, parseTsv(decoded.text, { joinExtraIntoLast }));
  } catch (e) {
    if (e instanceof SpReportError) { e.reportType = reportType; e.reportId = reportId; throw e; }
    throw new SpReportError(`${reportType}: 文書を ${format} として読めない: ${e.message}`, { code: 'PARSE', reportType, reportId });
  }
  log(`[SP-API] ${reportType} 文書 ${decoded.bytes} バイト (${decoded.encoding}${doc.compressionAlgorithm ? `・${doc.compressionAlgorithm}` : ''})${out.rows ? ` / ${out.rows.length} 行` : ''}`);
  return out;
}

// ─── 日付と値の小道具 (4 つの取得で共通) ───

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export const isRealDate = (d) => { if (typeof d !== 'string' || !DATE_RE.test(d)) return false; const ms = Date.parse(`${d}T00:00:00Z`); return Number.isFinite(ms) && new Date(ms).toISOString().slice(0, 10) === d; };
export const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
export function datesBetween(from, to) { const out = []; for (let d = from; d <= to; d = addDays(d, 1)) out.push(d); return out; }
/** JST の今日 (YYYY-MM-DD) */
export const jstToday = (nowMs = Date.now()) => new Date(nowMs + 9 * 3600000).toISOString().slice(0, 10);
/** JST の日の始まり / 終わり (ISO 8601・+09:00) */
export const jstStart = (d) => `${d}T00:00:00+09:00`;
export const jstEnd = (d) => `${d}T23:59:59+09:00`;

/**
 * 時刻の文字列 → JST の日付。時差の付いた ISO 8601 (例 2026-09-01T03:04:05+00:00 / ...Z) だけを受ける。
 * 時差の無い時刻・読めない値は null (どの日か決められない = 呼び側で行を失敗にする)
 */
export function isoToJstDate(v) {
  const s = String(v ?? '').trim();
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})$/.test(s)) return null;
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? jstToday(ms) : null;
}

/** 空 ('' / null / undefined) は null。それ以外は文字列 (前後の空白を外す) */
export const strOrNull = (v) => { if (v === undefined || v === null) return null; const s = String(v).trim(); return s === '' ? null : s; };

/**
 * 数を読む。空は null (0 にしない)。数でない・条件に合わない値は例外 (その期間を書かない)
 * @param {object} [o] - integer: 整数だけ / min: 下限 (既定 0。負を許すときは -Infinity)
 */
export function numOrNull(v, label, { integer = false, min = 0 } = {}) {
  if (v === undefined || v === null || (typeof v === 'string' && v.trim() === '')) return null;
  const n = typeof v === 'number' ? v : Number(String(v).trim().replace(/,/g, ''));
  if (!Number.isFinite(n) || n < min || (integer && !Number.isInteger(n))) {
    throw new Error(`${label} が${min === 0 ? ' 0 以上の' : ''}${integer ? '整数' : '数'}でない: ${JSON.stringify(v)}`);
  }
  return n;
}
export function reqNum(v, label, opts) {
  const n = numOrNull(v, label, opts);
  if (n === null) throw new Error(`${label} が無い`);
  return n;
}

/** TSV の見出しを大文字小文字・空白・_ / - の違いを無視して探す。見つからなければ null */
export function headerResolver(headers) {
  const norm = (h) => String(h).toLowerCase().replace(/[\s_-]+/g, '');
  const map = new Map(headers.map((h) => [norm(h), h]));
  return (...names) => { for (const n of names) { const h = map.get(norm(n)); if (h !== undefined) return h; } return null; };
}

// ─── 取得の完全性の記録 (レポートの種類 × 日) ───

export function ensureReportFetchDays(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS sp_report_fetch_days (
    report_type TEXT NOT NULL,          -- SP-API のレポートの種類
    date_jst    TEXT NOT NULL,          -- この取得で揃った日 (JST)
    generation  INTEGER NOT NULL,       -- 取得の世代 = createReport を頼んだ時刻 (ms)。古い世代では上書きしない
    report_id   TEXT NOT NULL,
    window_from TEXT NOT NULL,          -- 頼んだ期間 (JST の日付)
    window_to   TEXT NOT NULL,
    row_count   INTEGER NOT NULL,       -- その日の行数 (0 = 0 行で取れた)
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (report_type, date_jst)
  )`);
}

/**
 * その日をこの取得で書いてよいか。'write' / 'older' (もっと新しい取得がある = 触らない) / 'same' (同じレポートの取り直し = 入れ直さない)。
 * 同じ世代の別のレポート = どちらが新しいか分からない = 例外 (fetch-amazon-ads.js と同じ。#1483 Codex R1 P2)
 */
export function dayWriteDecision(db, reportType, date, generation, reportId) {
  const cur = db.prepare('SELECT generation, report_id FROM sp_report_fetch_days WHERE report_type = ? AND date_jst = ?').get(reportType, date);
  if (!cur) return 'write';
  if (Number(cur.generation) > generation) return 'older';
  if (Number(cur.generation) === generation) {
    if (String(cur.report_id) === String(reportId)) return 'same';
    throw new Error(`${date}: 同じ世代 (${generation}) の別のレポート (${cur.report_id} / ${reportId}) = どちらが新しいか分からないので書かない`);
  }
  return 'write';
}

export function recordFetchDay(db, { reportType, date, generation, reportId, windowFrom, windowTo, rowCount, fetchedAt }) {
  db.prepare(`INSERT INTO sp_report_fetch_days (report_type, date_jst, generation, report_id, window_from, window_to, row_count, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(report_type, date_jst) DO UPDATE SET generation = excluded.generation, report_id = excluded.report_id,
      window_from = excluded.window_from, window_to = excluded.window_to, row_count = excluded.row_count, fetched_at = excluded.fetched_at`)
    .run(reportType, date, generation, String(reportId), windowFrom, windowTo, rowCount, fetchedAt);
}

export function checkSaveArgs({ generation, reportId }) {
  if (!Number.isSafeInteger(generation) || generation <= 0) throw new Error('generation が無い');
  if (!reportId) throw new Error('reportId が無い');
}

/** 元の文書を gzip して保存する用 */
export const gzipText = (text) => gzipSync(Buffer.from(String(text), 'utf8'));
export const gunzipText = (buf) => gunzipSync(buf).toString('utf8');

/**
 * 取得のスクリプトの引数 (共通): --days N / --from YYYY-MM-DD --to YYYY-MM-DD / --dry-run / --check / --limit N
 * ふだんは JST の昨日まで直近 days 日 (今日は途中なので取らない)。daily-sync が足す '7' (引数なしのとき) は無視する
 */
export function parseCommonArgs(argv, { defaultDays, maxDays = 800, nowMs = Date.now() } = {}) {
  const r = { days: defaultDays, from: null, to: null, dryRun: false, check: false, limit: 0, extra: {} };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--days' && argv[i + 1]) r.days = Number(argv[++i]);
    else if (a === '--from' && argv[i + 1]) r.from = argv[++i];
    else if (a === '--to' && argv[i + 1]) r.to = argv[++i];
    else if (a === '--limit' && argv[i + 1]) r.limit = Number(argv[++i]);
    else if (a === '--dry-run') r.dryRun = true;
    else if (a === '--check') r.check = true;
    else if (a.startsWith('--') && argv[i + 1] && !argv[i + 1].startsWith('--')) r.extra[a.slice(2)] = argv[++i];
  }
  if (!Number.isInteger(r.days) || r.days < 1) throw new Error(`--days が不正: ${r.days}`);
  if (!Number.isInteger(r.limit) || r.limit < 0) throw new Error(`--limit が不正: ${r.limit}`);
  const yesterday = addDays(jstToday(nowMs), -1);
  if (!r.from) {
    r.to = r.to || yesterday;
    if (!isRealDate(r.to)) throw new Error(`期間が不正: 〜${r.to}`);
    r.from = addDays(r.to, -(r.days - 1));
  }
  if (!r.to) r.to = yesterday;
  if (!isRealDate(r.from) || !isRealDate(r.to) || r.from > r.to) throw new Error(`期間が不正: ${r.from}〜${r.to}`);
  if (r.to > yesterday) throw new Error(`期間の終わり ${r.to} が JST の昨日 (${yesterday}) より後 = 途中の日は取らない`);
  if (datesBetween(r.from, r.to).length > maxDays) throw new Error(`期間が長すぎる (${r.from}〜${r.to}・上限 ${maxDays} 日)`);
  return r;
}

/** 直接起動されたか (試験から import しても取得が走らない。fetch-amazon-ads.js と同じ realpath の判定) */
export async function isDirectRun(metaUrl) {
  const fs = await import('node:fs');
  const path = await import('node:path');
  const { fileURLToPath } = await import('node:url');
  const realPath = (p) => { try { return fs.realpathSync.native(p); } catch { return path.resolve(p); } };
  const foldCase = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);
  return !!process.argv[1] && foldCase(realPath(process.argv[1])) === foldCase(realPath(fileURLToPath(metaUrl)));
}

/**
 * 同じ鍵が 1 つのレポートに 2 回以上出たときの通し番号 (1, 2, …) を付ける。鍵の列が同じ別の行 (同じ時刻の 2 個の返品など) を 1 行に潰さない
 * @param {object[]} rows / @param {string[]} baseKeyCols
 */
export function assignOccurrence(rows, baseKeyCols) {
  const n = new Map();
  for (const r of rows) {
    const k = JSON.stringify(baseKeyCols.map((c) => r[c] ?? ''));
    const i = (n.get(k) || 0) + 1;
    n.set(k, i);
    r.occurrence = i;
  }
  return rows;
}

/**
 * 明細のレポート (返品・補てん) を UPSERT で保存する (1 取引)。
 * 🚨 行ごとに世代を持ち、記録より古い取得では行を上書きしない (WHERE excluded.generation >= 今の generation)。
 *    取得の記録 (sp_report_fetch_days) は頼んだ期間の日ごとに残す (もっと新しい取得の記録がある日は触らない)
 * @returns {{ upserted: number, days: number, olderDays: number }}
 */
export function saveUpsertRows(db, { reportType, table, keyCols, cols, rows, dateCol, from, to, generation, reportId, now = () => new Date().toISOString() }) {
  checkSaveArgs({ generation, reportId });
  if (!isRealDate(from) || !isRealDate(to) || from > to) throw new Error(`期間が不正: ${from}〜${to}`);
  ensureReportFetchDays(db);
  const ts = now();
  const all = [...keyCols, ...cols];
  const nonKey = cols.map((c) => `${c} = excluded.${c}`).join(', ');
  const up = db.prepare(`INSERT INTO ${table} (${all.join(', ')}, report_id, generation, first_seen_at, fetched_at)
    VALUES (${all.map(() => '?').join(', ')}, ?, ?, ?, ?)
    ON CONFLICT(${keyCols.join(', ')}) DO UPDATE SET ${nonKey}, report_id = excluded.report_id, generation = excluded.generation, fetched_at = excluded.fetched_at
    WHERE excluded.generation >= ${table}.generation`);
  const perDay = new Map();
  for (const r of rows) perDay.set(r[dateCol], (perDay.get(r[dateCol]) || 0) + 1);
  let upserted = 0, days = 0, olderDays = 0;
  db.transaction(() => {
    for (const r of rows) upserted += up.run(...all.map((c) => r[c] ?? null), String(reportId), generation, ts, ts).changes;
    for (const d of datesBetween(from, to)) {
      const decision = dayWriteDecision(db, reportType, d, generation, reportId);
      if (decision === 'older') { olderDays++; continue; }
      if (decision === 'same') continue;
      recordFetchDay(db, { reportType, date: d, generation, reportId, windowFrom: from, windowTo: to, rowCount: perDay.get(d) || 0, fetchedAt: ts });
      days++;
    }
  }).immediate();
  return { upserted, days, olderDays };
}

/**
 * 日付の文字列 → JST の日付 (YYYY-MM-DD)。読めなければ null。
 *   - 時差つきの ISO 8601 (…Z / …+00:00) = JST に直す
 *   - 時差の無い YYYY-MM-DD / YYYY/MM/DD (後ろに時刻があってもよい) = 書いてある日のまま (マーケットプレイスの日付と見なす)
 *   - MM/DD/YYYY = 米国の書き方 (在庫の台帳のレポートの Date はこの形のことがある) = 書いてある日のまま
 */
export function looseDateToJst(v) {
  const s = String(v ?? '').trim();
  if (!s) return null;
  const iso = isoToJstDate(s);
  if (iso) return iso;
  let m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?)?$/);
  if (m) { const d = `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`; return isRealDate(d) ? d : null; }
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$/);
  if (m) { const d = `${m[3]}-${m[1].padStart(2, '0')}-${m[2].padStart(2, '0')}`; return isRealDate(d) ? d : null; }
  return null;
}

/**
 * 明細を UPSERT するレポート (返品・補てん) 用: CANCELLED を「データなし (0 行)」として返す。
 *   Amazon は頼んだ期間にデータが無いとレポートを CANCELLED にする (公式の説明)。返品・補てんが 60 日ない店では毎朝 CANCELLED になりうる。
 *   UPSERT は 0 行なら何も消さない = 0 行と読んでも今の行は壊れない (取得の記録に 0 行の日として残すだけ)。
 *   🚨 日ごとに置き換えるレポート (アクセス・在庫の台帳) では使わない (0 行で置き換えると今の行を消してしまう)
 */
export async function fetchSpReportOrNoData(params) {
  try {
    return await fetchSpReport(params);
  } catch (e) {
    if (!(e instanceof SpReportError) || e.code !== 'CANCELLED' || !e.reportId || !e.generation) throw e;
    (params.log || console.log)(`[SP-API] ${params.reportType} は CANCELLED = データなし (0 行) と読む (reportId=${e.reportId})`);
    return { noData: true, reportId: e.reportId, reportDocumentId: null, generation: e.generation, headers: [], rows: [], text: '' };
  }
}

/**
 * レポートの種類ごとの上限 (例: GET_SALES_AND_TRAFFIC_REPORT は 5 分に 3 回まで = Reports API 全体の burst 15 とは別) に合わせて、
 * createReport の前に待つ門。呼ぶたびに「直近 windowMs に max 回」を超えないところまで待つ (同時に呼ばれても順に通す)
 * @returns {() => Promise<void>}
 */
export function createRateGate({ max, windowMs, now = Date.now, sleepImpl = sleep, log = () => {} }) {
  const stamps = [];
  let chain = Promise.resolve();
  return () => {
    chain = chain.then(async () => {
      for (;;) {
        const t = now();
        while (stamps.length && t - stamps[0] >= windowMs) stamps.shift();
        if (stamps.length < max) { stamps.push(t); return; }
        const wait = windowMs - (t - stamps[0]) + 1;
        log(`[SP-API] レポートの種類ごとの上限 (${Math.round(windowMs / 1000)} 秒に ${max} 回) → ${Math.ceil(wait / 1000)} 秒待つ`);
        await sleepImpl(wait);
      }
    });
    return chain;
  };
}
