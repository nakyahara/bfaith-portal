/**
 * jobs-monitor 判定ループと通知
 *
 *   - 5分ごと: 全エントリを評価。P1/P2 の締切超過 (late/overdue) は即時に GChat 要対応スペースへ。
 *     再通知は P1=6時間ごと / P2=24時間ごと。復旧したら復旧通知を1回。
 *   - 毎朝 08:50 JST 以降の最初の評価で「今日の要対応」サマリを1本 (5分ループ内で判定するため、
 *     08:50前後に Render が再起動してもその日のサマリは欠落しない — Codex R1 medium の反映)。
 *     アクションが必要なものだけを列挙し、無ければ「✅すべて正常」1行。
 *
 * 通知先は GCHAT_WEBHOOK_JOBS (要対応専用スペース)。
 * ⭐既定の GCHAT_WEBHOOK にはフォールバックしない — 要対応がレポートの流れに混ざって
 *   埋もれたのが Yahoo OAuth 失効見逃し (2026-07-31) の直接原因のため、混ぜない。
 *
 * 信頼性の要点 (Codex R1 critical の反映):
 *   - 通知は「送信に成功したときだけ」通知済み状態を進める。失敗したら次の周期に再送
 *   - 通知経路の健全性 (webhook設定・最終送信結果・サマリが毎日出ているか) を meta に記録し、
 *     /health がそれを見る → 「判定はできるが誰にも伝わらない」を GAS が検知できる
 *
 * 環境変数:
 *   JOBS_MONITOR_ENABLED=1   — server.js が mount と本ジョブの起動を有効化
 *   GCHAT_WEBHOOK_JOBS       — 要対応スペースの webhook (未設定 = unhealthy として扱う)
 *   JOBS_DIGEST_TIME_JST     — サマリ時刻 'HH:MM' (既定 '08:50')
 */
import cron from 'node-cron';
import { JOBS_REGISTRY, RETIRED_JOBS, validateRegistry } from '../../config/jobs-registry.mjs';
import {
  getStates, ensureFirstSeen, getAlertState, setAlertState, clearAlertState, getMeta, setMeta, purgeJobStates,
} from './store.js';
import {
  evaluateAll, needsImmediateAlert, realertIntervalMs, buildDigest, todayJst,
} from './evaluate.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

const REGISTERED = new Set(JOBS_REGISTRY.map((e) => e.id));
// 退役した id は「台帳に無い id」として鳴らさない (記録は起動時に purgeJobStates で消す)
const RETIRED = new Set(RETIRED_JOBS.map((e) => e.id));

/** 送信できたかを返す。成功/失敗を meta に刻む (/health が通知経路の健全性を見るため) */
async function notify(text, nowMs = Date.now()) {
  const webhook = process.env.GCHAT_WEBHOOK_JOBS;
  if (!webhook) {
    console.warn('[jobs-monitor] GCHAT_WEBHOOK_JOBS 未設定のため通知不可:', text.split('\n')[0]);
    setMeta('last_notify_fail_at', nowMs);
    return false;
  }
  try {
    await sendGChatMessage(webhook, text);
    setMeta('last_notify_ok_at', nowMs);
    return true;
  } catch (e) {
    console.error('[jobs-monitor] GChat送信失敗:', e.message);
    setMeta('last_notify_fail_at', nowMs);
    return false;
  }
}

function parseDigestTime() {
  const m = String(process.env.JOBS_DIGEST_TIME_JST || '08:50').match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return { h: 8, mi: 50 };
  return { h: Math.min(23, Number(m[1])), mi: Math.min(59, Number(m[2])) };
}

/** 今日のサマリをまだ送っておらず、サマリ時刻を過ぎているか */
function digestDue(nowMs) {
  const { h, mi } = parseDigestTime();
  const jst = new Date(nowMs + 9 * 3600 * 1000);
  const passed = jst.getUTCHours() > h || (jst.getUTCHours() === h && jst.getUTCMinutes() >= mi);
  return passed && getMeta('last_digest_date') !== todayJst(nowMs);
}

let inFlight = false; // 評価の多重実行防止 (単一インスタンス前提。複数インスタンス化するなら要リーダー選出)

/** 5分ごとの評価。評価が生きている証拠として meta.last_eval_at を必ず更新する */
export async function runEvaluation(nowMs = Date.now()) {
  if (inFlight) return null;
  inFlight = true;
  try {
    const states = getStates();
    const firstSeen = ensureFirstSeen([...REGISTERED], nowMs);
    const merged = {};
    for (const id of REGISTERED) {
      merged[id] = { ...(states[id] || {}), firstSeenAtMs: firstSeen[id] ?? nowMs };
    }
    const results = evaluateAll(JOBS_REGISTRY, merged, nowMs);

    for (const r of results) {
      const alerted = getAlertState(r.id);

      if (needsImmediateAlert(r)) {
        const interval = realertIntervalMs(r.importance);
        if (!alerted || alerted.status !== r.status || nowMs - alerted.last_alert_at >= interval) {
          const sent = await notify([
            `🔴 *${r.id}* が締切超過です (${r.importance})`,
            `　${r.detail}`,
            r.runbook ? `　▶ ${r.runbook}` : '',
          ].filter(Boolean).join('\n'), nowMs);
          // ⭐送信に成功したときだけ「通知済み」を進める。失敗したら次の5分で再試行
          if (sent) {
            const firstAt = alerted && alerted.status === r.status ? alerted.first_at : nowMs;
            setAlertState(r.id, r.status, firstAt, nowMs);
          }
        }
      } else if (alerted && r.status === 'ok') {
        const sent = await notify(`✅ *${r.id}* は復旧しました (${r.detail})`, nowMs);
        if (sent) clearAlertState(r.id);
      }
    }

    // 毎朝のサマリ (時刻を過ぎた最初の評価で送る。送信成功時だけ日付を刻む = 失敗したら次の5分で再試行)
    if (digestDue(nowMs)) {
      const unknown = Object.keys(states).filter((id) => !REGISTERED.has(id) && !RETIRED.has(id));
      const sent = await notify(buildDigest(results, unknown, nowMs), nowMs);
      if (sent) {
        setMeta('last_digest_date', todayJst(nowMs));
        setMeta('last_digest_ok_at', nowMs);
      }
    }

    setMeta('last_eval_at', nowMs);
    return results;
  } finally {
    inFlight = false;
  }
}

let started = false;

/** server.js から呼ぶ。JOBS_MONITOR_ENABLED=1 のときだけ呼ばれる想定 */
export function startJobsMonitor() {
  if (started) return; // 同一プロセス内の多重起動防止 (boot-log の教訓)
  started = true;

  // 台帳が壊れていたら unhealthy として公開する (黙って監視が欠けるのが最悪)。
  // サーバー全体は殺さない — ポータルは他の業務画面も担っているため
  const errs = validateRegistry();
  if (errs.length) {
    console.error('[jobs-monitor] ⚠️ 台帳にエラー:', errs.join(' / '));
    setMeta('registry_error', errs.join(' / '));
    notify(`⚠️ *jobs-monitor: 台帳 (jobs-registry) にエラーがあります — 監視は unhealthy 扱いです*\n${errs.map((e) => `　- ${e}`).join('\n')}`);
  } else {
    setMeta('registry_error', '');
  }
  if (!getMeta('monitoring_since')) setMeta('monitoring_since', Date.now());
  // 退役したジョブの記録を消す (残っていると「台帳に無い id」として毎朝出る)
  try {
    const purged = purgeJobStates(RETIRED_JOBS.map((e) => e.id));
    if (purged > 0) console.log(`[jobs-monitor] 退役ジョブの記録を消しました: ${purged} 件 (${RETIRED_JOBS.map((e) => e.id).join(', ')})`);
  } catch (e) {
    console.error('[jobs-monitor] 退役ジョブの記録の削除に失敗:', e.message);
  }

  cron.schedule('*/5 * * * *', () => {
    runEvaluation().catch((e) => console.error('[jobs-monitor] 評価失敗:', e.message));
  });

  // 起動直後に1回評価しておく (health がすぐ答えられるように + 再起動でサマリを取りこぼさないように)
  runEvaluation().catch((e) => console.error('[jobs-monitor] 初回評価失敗:', e.message));

  console.log(`[jobs-monitor] started (entries=${JOBS_REGISTRY.length}, digest=JST ${process.env.JOBS_DIGEST_TIME_JST || '08:50'})`);
}
