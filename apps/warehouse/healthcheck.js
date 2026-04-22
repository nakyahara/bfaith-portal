/**
 * warehouse healthcheck — ミニPCのwarehouseサーバー死活監視 + Google Chat通知
 *
 * 5分毎に https://wh.bfaith-wh.uk/service-api/health をチェック。
 * 3回連続失敗で通知、復旧時も通知。
 * 失敗の種類 (認証/tunnel/app/ネットワーク) を区別する。
 */
import cron from 'node-cron';
import { bootStart, bootEnd } from '../observability/boot-log.js';

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
const HEALTH_PATH = '/service-api/health';
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;
const CHECK_TIMEOUT_MS = 10000;
const ALERT_THRESHOLD = 3;

let consecutiveFailures = 0;
let alertSent = false;
let lastFailureKind = null;

function getHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
  };
}

async function notify(text) {
  if (!GCHAT_WEBHOOK) {
    console.warn('[Healthcheck] GCHAT_WEBHOOK未設定のため通知スキップ');
    return;
  }
  try {
    await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
  } catch (e) {
    console.error('[Healthcheck] 通知エラー:', e.message);
  }
}

async function probe() {
  const url = `${WAREHOUSE_URL}${HEALTH_PATH}`;
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: getHeaders(),
      redirect: 'manual',
      signal: AbortSignal.timeout(CHECK_TIMEOUT_MS),
    });
    const ct = res.headers.get('content-type') || '';

    if (res.status === 200 && ct.includes('application/json')) {
      return { ok: true };
    }
    if (res.status === 302 || res.status === 303) {
      const loc = res.headers.get('location') || '';
      if (/cloudflareaccess|\/cdn-cgi\/access/i.test(loc)) {
        return { ok: false, kind: 'cf-access-misconfig', detail: `302 → ${loc}` };
      }
      return { ok: false, kind: 'unexpected-redirect', detail: `${res.status} → ${loc}` };
    }
    if (res.status === 401 || res.status === 403) {
      return { ok: false, kind: 'auth-failed', detail: `HTTP ${res.status}` };
    }
    if ([502, 503, 504].includes(res.status)) {
      return { ok: false, kind: 'upstream-down', detail: `HTTP ${res.status} (CF tunnel / warehouse側障害)` };
    }
    return { ok: false, kind: 'unexpected-status', detail: `HTTP ${res.status} ct=${ct}` };
  } catch (e) {
    const msg = e?.message || String(e);
    if (e?.name === 'TimeoutError' || /aborted|timeout/i.test(msg)) {
      return { ok: false, kind: 'timeout', detail: `${CHECK_TIMEOUT_MS}ms timeout` };
    }
    return { ok: false, kind: 'network', detail: msg };
  }
}

async function runCheck() {
  const result = await probe();
  const ts = new Date().toISOString();

  if (result.ok) {
    if (alertSent) {
      await notify(`✅ warehouse復旧 (${ts})\n失敗${consecutiveFailures}回連続後に復旧しました`);
    }
    consecutiveFailures = 0;
    alertSent = false;
    lastFailureKind = null;
    return;
  }

  consecutiveFailures++;
  lastFailureKind = result.kind;
  console.warn(`[Healthcheck] 失敗 ${consecutiveFailures}/${ALERT_THRESHOLD}: ${result.kind} — ${result.detail}`);

  if (consecutiveFailures >= ALERT_THRESHOLD && !alertSent) {
    const msg = [
      `🚨 warehouse死活監視アラート (${ts})`,
      `URL: ${WAREHOUSE_URL}${HEALTH_PATH}`,
      `連続失敗: ${consecutiveFailures}回`,
      `種別: ${result.kind}`,
      `詳細: ${result.detail}`,
    ].join('\n');
    await notify(msg);
    alertSent = true;
  }
}

export function startWarehouseHealthcheck() {
  bootStart('healthcheck', 'warehouse-healthcheck');
  cron.schedule('*/5 * * * *', runCheck, { timezone: 'UTC' });
  console.log('[Healthcheck] warehouse死活監視開始 (5分毎、3回連続失敗でGChat通知)');
  setTimeout(runCheck, 30000);
  bootEnd('healthcheck', 'warehouse-healthcheck', `target=${WAREHOUSE_URL}${HEALTH_PATH}`);
}
