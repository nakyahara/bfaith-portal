/**
 * NE 反映 worker (ミニPC 側、構成 4: simple-hybrid + ne_unknown、2026-06-06)
 *
 * 役割: Render の pd_shipment_tracking から ready/error 行を 1 件ずつ pull → NextEngine
 *       API で更新 (受注検索 → 配送方法+送り状番号 update → shipped) → 結果を Render に POST。
 *
 * 構成 4 の思想:
 *   - 通常時は 1x NE call (= search → update → shipped、合計 3 call/件)
 *   - 1 件失敗 = manual_review (NE 業務エラー) or ne_unknown (DB/通信/クラッシュ)
 *   - ne_unknown は自動 retry しない (= 中原さんが翌日確認)
 *   - claim_token / journal / late-results は廃止 (Bearer 認証で十分)
 *   - PID lock + URL allowlist + secret redaction は維持
 *
 * 使い方:
 *   node apps/ne-sync-runner/runner.js --run-id=<uuid> [--limit=50] [--dry-run]
 *
 * 環境変数 (fail-closed、未設定で起動拒否):
 *   RENDER_BASE_URL                   = https://bfaith-portal.onrender.com (allowlist 照合)
 *   RENDER_NE_SYNC_WORKER_KEY         = Render 側で発行した Bearer
 *   NE_CLIENT_ID / NE_CLIENT_SECRET   = NE OAuth (既存)
 *   DATA_DIR (任意)                   = ne-tokens.json + PID 保存先 (デフォルト data/)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { callNE, loadTokens } from '../warehouse/ne-api.js';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const PID_FILE = path.join(DATA_DIR, 'ne-sync-runner.pid');

// ─── env validation + allowlist (fail-closed) ───
const ALLOWED_ORIGINS = new Set([
  'https://bfaith-portal.onrender.com', // 本番のみ
]);
function loadEnv() {
  const required = {
    RENDER_BASE_URL: process.env.RENDER_BASE_URL,
    RENDER_NE_SYNC_WORKER_KEY: process.env.RENDER_NE_SYNC_WORKER_KEY,
    NE_CLIENT_ID: process.env.NE_CLIENT_ID,
    NE_CLIENT_SECRET: process.env.NE_CLIENT_SECRET,
  };
  const missing = Object.entries(required).filter(([, v]) => !v).map(([k]) => k);
  if (missing.length) {
    console.error('[ne-sync-runner] 環境変数が未設定: ' + missing.join(', '));
    process.exit(2);
  }
  let originBase;
  try {
    const u = new URL(required.RENDER_BASE_URL);
    if (u.protocol !== 'https:') throw new Error('must be https');
    if (u.pathname !== '/' && u.pathname !== '') throw new Error('must not contain path');
    if (u.search || u.hash) throw new Error('must not contain query or fragment');
    originBase = u.origin;
  } catch (e) {
    console.error('[ne-sync-runner] RENDER_BASE_URL 不正: ' + e.message);
    process.exit(2);
  }
  if (!ALLOWED_ORIGINS.has(originBase)) {
    console.error(`[ne-sync-runner] RENDER_BASE_URL=${originBase} は allowlist 外`);
    process.exit(2);
  }
  return { ...required, RENDER_BASE_URL: originBase };
}

function redact(s) {
  if (typeof s !== 'string') return s;
  return s.length > 8 ? s.slice(0, 4) + '...' + s.slice(-4) : '***';
}

// ─── PID file lock (二重起動防止、atomic + reclaim lock で race-safe) ───
function acquireLock() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const myPid = process.pid;
  const reclaimLock = PID_FILE + '.reclaim';
  const tryCreate = (p) => {
    const fd = fs.openSync(p, 'wx');
    try { fs.writeSync(fd, String(myPid)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  };
  try { tryCreate(PID_FILE); }
  catch (e) {
    if (e.code !== 'EEXIST') throw e;
    try { tryCreate(reclaimLock); }
    catch (e2) {
      if (e2.code === 'EEXIST') {
        console.error('[ne-sync-runner] 別プロセスが lock 取得中、または起動中');
        process.exit(2);
      }
      throw e2;
    }
    try {
      let existingPid;
      try { existingPid = parseInt(fs.readFileSync(PID_FILE, 'utf-8').trim(), 10); } catch { existingPid = NaN; }
      if (existingPid && isProcessAlive(existingPid)) {
        console.error(`[ne-sync-runner] 既に動作中 (pid=${existingPid})`);
        process.exit(2);
      }
      console.warn(`[ne-sync-runner] 古い PID file を回収 (pid=${existingPid || '?'})`);
      try { fs.unlinkSync(PID_FILE); } catch {}
      tryCreate(PID_FILE);
    } finally {
      try { fs.unlinkSync(reclaimLock); } catch {}
    }
  }
  const cleanup = () => {
    try {
      const c = fs.readFileSync(PID_FILE, 'utf-8').trim();
      if (parseInt(c, 10) === myPid) fs.unlinkSync(PID_FILE);
    } catch {}
  };
  process.on('exit', cleanup);
  process.on('SIGINT', () => { cleanup(); process.exit(130); });
  process.on('SIGTERM', () => { cleanup(); process.exit(143); });
}
function isProcessAlive(pid) {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

// ─── Render API client ───
function makeClient(env) {
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${env.RENDER_NE_SYNC_WORKER_KEY}`,
    'User-Agent': 'bfaith-ne-sync-runner/2.0',
  };
  async function post(pathSuffix, body) {
    if (!pathSuffix.startsWith('/apps/packing-dispatch/api/ne-sync-worker/')) {
      throw new Error('illegal path: ' + pathSuffix);
    }
    const url = env.RENDER_BASE_URL + pathSuffix;
    const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body || {}) });
    const text = await res.text();
    let data; try { data = JSON.parse(text); } catch { data = { ok: false, message: text.slice(0, 500) }; }
    if (!res.ok || !data.ok) {
      const err = new Error(`Render POST ${pathSuffix} failed: ${res.status} ${(data && data.message) || ''}`);
      err.status = res.status;
      throw err;
    }
    return data.result;
  }
  return { post };
}

// ─── NE 受注伝票更新 (Codex 公式 doc 仕様) ───
const ORDER_META_FIELDS = [
  'receive_order_id',
  'receive_order_last_modified_date',
  'receive_order_cancel_type_id',
  'receive_order_order_status_id',
].join(',');

function xmlText(value) {
  return String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

function buildUpdateXml(item) {
  const fields = [];
  if (item.ne_carrier_id !== null && item.ne_carrier_id !== undefined && item.ne_carrier_id !== '') {
    fields.push(`<receive_order_delivery_id>${xmlText(String(item.ne_carrier_id))}</receive_order_delivery_id>`);
  }
  const trackingNo = String(item.tracking_no || '').trim();
  if (trackingNo) {
    fields.push(`<receive_order_delivery_cut_form_id>${xmlText(trackingNo)}</receive_order_delivery_cut_form_id>`);
  }
  if (!fields.length) return null;
  return `<?xml version="1.0" encoding="utf-8"?><root><receiveorder_base>${fields.join('')}</receiveorder_base></root>`;
}

async function fetchOrderMeta(receiveOrderId) {
  const res = await callNE('/api_v1_receiveorder_base/search', {
    fields: ORDER_META_FIELDS,
    'receive_order_id-eq': String(receiveOrderId),
    limit: '1',
  });
  return (res && res.data && res.data[0]) || null;
}

// 構成 4: 業務エラー (NE 反映前) は manual_review、それ以外は ne_unknown
function assertCanShip(meta) {
  if (!meta) throw Object.assign(new Error('NE order not found'),
    { neCode: 'ORDER_NOT_FOUND', businessError: true });
  if (String(meta.receive_order_cancel_type_id) !== '0') throw Object.assign(new Error('NE order is canceled'),
    { neCode: 'ORDER_CANCELED', businessError: true });
  if (String(meta.receive_order_order_status_id) === '50') throw Object.assign(new Error('NE order already shipped'),
    { neCode: 'ALREADY_SHIPPED', businessError: true });
  if (!['20', '40'].includes(String(meta.receive_order_order_status_id))) throw Object.assign(
    new Error(`NE order is not shippable (status=${meta.receive_order_order_status_id})`),
    { neCode: 'NOT_SHIPPABLE', businessError: true });
}

/**
 * 1 件を NE 反映。構成 4 では「成功 / 業務エラー (manual_review) / それ以外 (ne_unknown)」の 3 値分岐。
 * 成功後の DB / ack / クラッシュは ne_unknown に落とす (Render 側で complete 時に自動)。
 */
async function reflectOne(item) {
  const receiveOrderId = String(item.ne_uketsuke_no || '').trim();
  if (!receiveOrderId) throw Object.assign(new Error('missing ne_uketsuke_no'),
    { neCode: 'MISSING_ID', businessError: true });
  let meta = await fetchOrderMeta(receiveOrderId);
  assertCanShip(meta); // update 前に business check
  const updateXml = buildUpdateXml(item);
  if (updateXml) {
    await callNE('/api_v1_receiveorder_base/update', {
      receive_order_id: receiveOrderId,
      receive_order_last_modified_date: meta.receive_order_last_modified_date,
      data: updateXml,
    });
    meta = await fetchOrderMeta(receiveOrderId);
    assertCanShip(meta); // 再取得後にも (NE 側で他操作が走った可能性)
  }
  await callNE('/api_v1_receiveorder_base/shipped', {
    receive_order_id: receiveOrderId,
    receive_order_last_modified_date: meta.receive_order_last_modified_date,
  });
}

// ─── メインジョブ ───
async function runJob({ run_id, limit, maxBatches }) {
  const env = loadEnv();
  const client = makeClient(env);

  if (!loadTokens()) {
    console.error('[ne-sync-runner] NE トークンが見つかりません。先に認証してください');
    process.exit(2);
  }

  console.log(`[ne-sync-runner] run_id=${run_id} limit=${limit}`);
  let batchSeq = 0;
  let totalSynced = 0, totalManual = 0, totalUnknown = 0;
  let lastError = null;

  try {
    while (batchSeq < maxBatches) {
      batchSeq++;
      const queue = await client.post(`/apps/packing-dispatch/api/ne-sync-worker/${run_id}/queue`, { limit });
      const items = queue.items || [];
      console.log(`[ne-sync-runner] batch=${batchSeq} claimed=${items.length}`);
      if (!items.length) break;

      // 構成 4: 1 件ずつ NE 反映 → 即 results POST (バッチング廃止、journal 不要)
      for (const item of items) {
        let result;
        try {
          await reflectOne(item);
          result = { ne_uketsuke_no: item.ne_uketsuke_no, status: 'synced', ne_synced_at: new Date().toISOString() };
        } catch (e) {
          // 構成 4 の分岐:
          // - businessError (受注なし、キャンセル済等、NE 反映前) → manual_review
          // - それ以外 (NE 通信途中で失敗、再試行可能なエラー) → ne_unknown
          //   ne_unknown は自動 retry しない、中原さんが翌日確認
          const isBusiness = !!e.businessError;
          result = {
            ne_uketsuke_no: item.ne_uketsuke_no,
            status: isBusiness ? 'manual_review' : 'ne_unknown',
            error_code: e.neCode || (isBusiness ? 'BUSINESS_ERROR' : 'NE_UNKNOWN'),
            error_message: String(e.message).slice(0, 500),
          };
        }
        // 即座に 1 件ずつ POST (バッチング廃止)。
        // ack 失敗時はジョブ全体を中止 → Render 側 complete で残った syncing は ne_unknown 化される。
        try {
          const ack = await client.post(
            `/apps/packing-dispatch/api/ne-sync-worker/${run_id}/results`, { results: [result] });
          totalSynced += ack.synced || 0;
          totalManual += ack.manual_review || 0;
          totalUnknown += ack.ne_unknown || 0;
        } catch (e) {
          // 構成 4: ack 失敗 = Render との通信障害。journal 不要、ジョブ中止して complete 経由で ne_unknown 化
          lastError = `results POST failed: ${e.message}`;
          console.error(`[ne-sync-runner] ${lastError} (ジョブ中止、未完了行は Render 側で ne_unknown 化)`);
          throw e;
        }
      }
    }
  } catch (e) {
    lastError = e.message;
    console.error(`[ne-sync-runner] ジョブ全体エラー: ${e.message}`);
  }

  // complete を呼ぶ (残った syncing → ne_unknown は Render 側で実行される)
  const completeStatus = lastError ? 'failed' : 'done';
  try {
    await client.post(`/apps/packing-dispatch/api/ne-sync-worker/${run_id}/complete`, {
      status: completeStatus, last_error: lastError,
    });
  } catch (e) {
    console.error(`[ne-sync-runner] complete POST failed: ${e.message} (Render 側 TTL で expired 回収)`);
  }

  console.log(`[ne-sync-runner] 終了: synced=${totalSynced} manual=${totalManual} unknown=${totalUnknown} status=${completeStatus}`);
  return { run_id, synced: totalSynced, manual_review: totalManual, ne_unknown: totalUnknown, status: completeStatus, last_error: lastError };
}

// ─── CLI ───
function parseArgs() {
  // 構成 4 + Codex R3 High: limit は **常に 1 固定** (overrideable 不可)。
  // batch claim を許すと「途中で失敗した時に NE 未反映の残り行も ne_unknown になる」
  // 問題が再発する。--limit が指定されたら起動拒否。
  const args = { runId: null, limit: 1, maxBatches: 2000 };
  for (const a of process.argv.slice(2)) {
    if (a === '--dry-run') {
      console.error('[ne-sync-runner] --dry-run は廃止しました (本番 DB を更新するため危険)');
      process.exit(2);
    } else if (a.startsWith('--run-id=')) args.runId = a.slice('--run-id='.length);
    else if (a.startsWith('--limit=')) {
      // 構成 4 不変条件: claim 1 件 = NE 反映を試みた 1 件。batch は許可しない。
      console.error('[ne-sync-runner] --limit は固定 1 です (構成 4 不変条件: 1 件ずつ処理で ne_unknown の意味を保つ)');
      process.exit(2);
    }
    else if (a.startsWith('--max-batches=')) args.maxBatches = Math.max(1, Math.min(10000, parseInt(a.slice('--max-batches='.length), 10) || 2000));
    else if (a === '--help' || a === '-h') {
      console.log('Usage: node apps/ne-sync-runner/runner.js --run-id=<uuid> [--max-batches=2000]');
      console.log('  limit=1 固定 (構成 4 不変条件)');
      process.exit(0);
    }
  }
  return args;
}

async function main() {
  const args = parseArgs();
  if (!args.runId) {
    console.error('[ne-sync-runner] --run-id=<uuid> が必須です。');
    process.exit(1);
  }
  acquireLock();
  const env = loadEnv();
  console.log(`[ne-sync-runner] RENDER_BASE_URL=${env.RENDER_BASE_URL} worker_key=${redact(env.RENDER_NE_SYNC_WORKER_KEY)}`);
  const result = await runJob({ run_id: args.runId, limit: args.limit, maxBatches: args.maxBatches });
  process.exit(result.last_error ? 1 : 0);
}

const isDirectRun = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isDirectRun) {
  main().catch(e => {
    console.error('[ne-sync-runner] FATAL:', e.message);
    process.exit(1);
  });
}

export { runJob, reflectOne, loadEnv };
