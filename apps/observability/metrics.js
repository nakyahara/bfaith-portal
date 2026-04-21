/**
 * metrics.js — event loop lag + heap/rss の定期観測
 *
 * 目的: 次回のサーバーハング時に"気づかない"を避ける。
 * warehouse は 2026-04-17 に node-cron の missed execution 警告だけ残して
 * 死んだため、event loop lag が普段からどれくらいだったかを記録する。
 *
 * 5分毎に:
 *   - heap/rss メモリ使用量
 *   - event loop lag (mean/p95/p99/max)
 * を stdout に 1 行ログ。
 *
 * 深刻なイベントループ停滞が起きたら GChat に1度だけ通知 (復旧で再武装)。
 */
import { monitorEventLoopDelay } from 'perf_hooks';
import os from 'os';
import { bootStart, bootEnd, getBootId, getBootStartedAt } from './boot-log.js';

const LOG_INTERVAL_MS = 5 * 60 * 1000;
const WARN_LAG_P99_MS = parseInt(process.env.METRICS_WARN_LAG_MS || '1000', 10);
const ALERT_LAG_P99_MS = parseInt(process.env.METRICS_ALERT_LAG_MS || '3000', 10);
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

let histogram;
let alertActive = false;

async function notify(text) {
  if (!GCHAT_WEBHOOK) return;
  try {
    await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
  } catch (e) {
    console.error('[Metrics] 通知エラー:', e.message);
  }
}

function hostLabel() {
  if (process.env.RENDER) return 'Render';
  const h = os.hostname();
  return h || 'unknown';
}

async function report() {
  const mem = process.memoryUsage();
  const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
  const rssMB = Math.round(mem.rss / 1024 / 1024);
  const lagMean = histogram.mean / 1e6;
  const lagP95 = histogram.percentile(95) / 1e6;
  const lagP99 = histogram.percentile(99) / 1e6;
  const lagMax = histogram.max / 1e6;
  const uptimeSec = Math.round(process.uptime());
  const line = `[Metrics] boot=${getBootId()} host=${hostLabel()} uptime=${uptimeSec}s heap=${heapMB}MB rss=${rssMB}MB lag(mean/p95/p99/max)=${lagMean.toFixed(1)}/${lagP95.toFixed(1)}/${lagP99.toFixed(1)}/${lagMax.toFixed(1)}ms`;

  if (lagP99 >= ALERT_LAG_P99_MS) {
    console.error(line + ' 🚨 SEVERE event loop stall');
    if (!alertActive) {
      await notify([
        `🚨 event loop 深刻停滞 (${hostLabel()})`,
        `lag p99=${lagP99.toFixed(0)}ms max=${lagMax.toFixed(0)}ms`,
        `heap=${heapMB}MB rss=${rssMB}MB`,
        `threshold=${ALERT_LAG_P99_MS}ms`,
      ].join('\n'));
      alertActive = true;
    }
  } else if (lagP99 >= WARN_LAG_P99_MS) {
    console.warn(line + ' (lag warning)');
    alertActive = false;
  } else {
    console.log(line);
    alertActive = false;
  }
  histogram.reset();
}

export function startMetrics() {
  bootStart('metrics', 'event-loop-observer');
  histogram = monitorEventLoopDelay({ resolution: 20 });
  histogram.enable();
  const timer = setInterval(report, LOG_INTERVAL_MS);
  timer.unref();
  console.log(`[Metrics] 観測開始 (5分毎、warn≥${WARN_LAG_P99_MS}ms alert≥${ALERT_LAG_P99_MS}ms) boot=${getBootId()} started_at=${getBootStartedAt()}`);
  bootEnd('metrics', 'event-loop-observer', `interval=${LOG_INTERVAL_MS}ms`);
}
