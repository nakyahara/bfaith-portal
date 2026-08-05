/**
 * jobs-monitor — Render 内ジョブ用 ping ヘルパー
 *
 * miniPC のランナーは scripts/jobs-monitor/ping.ps1 が HTTP で ping を打つが、
 * Render 内の node-cron / 常駐ワーカーは jobs-monitor と同一プロセスなので
 * recordPing を直接呼ぶ (自分自身に HTTP を投げない)。
 *
 * 原則: **監視の記録失敗でジョブ本体を巻き添えにしない。** 例外は必ずここで握り潰す。
 * ここが throw すると「監視を付けたらジョブが止まった」になり本末転倒。
 */
import { recordPing } from './store.js';

// jobs-monitor が mount されていない環境 (miniPC は同じ server.js を動かす) では
// 記録先の DB を作らない。ping は黙って捨てる。
function enabled() {
  return process.env.JOBS_MONITOR_ENABLED === '1';
}

/**
 * 成否を記録する。
 * @param {string} id     台帳 (config/jobs-registry.mjs) の id
 * @param {'ok'|'fail'|'partial'} status
 * @param {string} [note] 直近の状況 (180字で切る)
 */
export function pingJob(id, status, note) {
  if (!enabled()) return false;
  try {
    recordPing(id, status, note ? String(note).slice(0, 180) : null, Date.now());
    return true;
  } catch (e) {
    console.error(`[jobs-monitor] ping failed (${id}): ${e.message}`);
    return false;
  }
}

/**
 * 常駐ワーカー (数十秒〜数分間隔) 用の間引き ping。
 *
 * dead-man 監視が見たいのは「今日も生きているか」だけなので、毎周期打つ必要はない。
 * 60秒ごとのワーカーがそのまま ping すると 1日1,440回の書き込みになる。
 * 既定は1時間に1回 — 台帳側の grace_hours はこの間隔より十分大きく取ること。
 *
 * プロセス内メモリで間引くので、再起動直後は必ず1回打つ (再起動を無音にしない)。
 */
const lastPingAtMs = new Map();

export function pingJobThrottled(id, note, minIntervalMs = 60 * 60 * 1000) {
  if (!enabled()) return false;
  const now = Date.now();
  const prev = lastPingAtMs.get(id);
  if (prev !== undefined && now - prev < minIntervalMs) return false;
  // 記録できた時だけ間引きの起点を進める。先に進めると、DB ロック等で1回失敗しただけで
  // 次の1時間ぶん ping が止まり、生きているワーカーが締切超過に見える (Codexレビュー Medium)
  const sent = pingJob(id, 'ok', note);
  if (sent) lastPingAtMs.set(id, now);
  return sent;
}

/** テスト用: 間引き状態を初期化する */
export function _resetThrottleForTest() {
  lastPingAtMs.clear();
}
