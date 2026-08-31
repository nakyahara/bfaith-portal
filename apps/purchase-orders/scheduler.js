/**
 * purchase-orders 定期実行 (Render 常駐) + 営業日カレンダー
 *
 *  - FBA在庫の自動更新: 平日 (土日祝を除く) の JST 16:00 台に miniPC の fba-refresh ジョブを起動する
 *    (画面の「🔄 FBA在庫を今すぐ更新」と同じジョブ)。土日祝は発注作業をしないため実行しない
 *    (中原さん指定 2026-08-27。第2・第4土曜の営業日扱いはしない — シンプルに土日祝一律スキップ)。
 *    ⚠️ 完了すると手動ボタンと同様に「✅発注確定済み」の発注サイクルもリセットされる —
 *    ここからは何もしないが、router.js の maybeCycleResetFromFbaLive (fba_fetched_at 検知の
 *    安全網、2026-08-18) がライブ更新の完了を拾ってサイクルを進めるため。挙動はボタン押下と同一。
 *  - 翌営業日 (土日祝を除く) 9:00 JST の算出: /backorders 一括送信の「⏰ 翌営業日9時に送信」用
 *
 * 祝日判定は apps/picking/jp-holiday.js (祝日法ベース・外部依存なし) を共用する。
 * 年末年始 (12/29〜1/3 等) は祝日法上の休日ではないため営業日扱いになる点に注意。
 */
import { isJstWeekendOrHoliday } from '../picking/jp-holiday.js';
import { isRender } from '../../lib/is-render.js';
import { pingJob } from '../jobs-monitor/ping-local.js';
import { getSetting, setSetting } from './ledger.js';

/** JSTの 'YYYY-MM-DD' */
function jstYmd(date) {
  return new Date(date.getTime() + 9 * 3600000).toISOString().slice(0, 10);
}

/**
 * 翌営業日 (今日より後の、土日でも祝日でもない最初の日) の 9:00 JST を
 * datetime-local / parseScheduleAt 互換の 'YYYY-MM-DDT09:00' で返す。
 */
export function nextBusinessDay9Jst(now = new Date()) {
  for (let i = 1; i <= 40; i++) {
    const cand = new Date(now.getTime() + i * 86400000);
    // 判定は候補日のJST正午で行う (日跨ぎの丸め誤差を避ける。JSTに夏時間はない)
    const noon = new Date(Date.parse(jstYmd(cand) + 'T12:00:00+09:00'));
    if (!isJstWeekendOrHoliday(noon)) return jstYmd(cand) + 'T09:00';
  }
  throw new Error('翌営業日が40日以内に見つかりません'); // 祝日法上あり得ない (防御)
}

/**
 * FBA自動更新を今起動すべきか。JST 16:00〜16:59 かつ当日未実行かつ平日のときだけ due。
 * 土日祝は skip=true (実行せず「休業日スキップ」として当日処理済みにする)。
 * 窓を過ぎたらその日は起動しない (夜中の再起動で無人更新が走るのを防ぐ)。
 */
export const FBA_AUTO_HOUR_JST = 16;
export function fbaAutoDue(now, lastYmd) {
  const jst = new Date(now.getTime() + 9 * 3600000);
  const ymd = jst.toISOString().slice(0, 10);
  if (jst.getUTCHours() !== FBA_AUTO_HOUR_JST || lastYmd === ymd) return { due: false, skip: false, ymd };
  if (isJstWeekendOrHoliday(now)) return { due: false, skip: true, ymd };
  return { due: true, skip: false, ymd };
}

const LAST_YMD_KEY = 'po_fba_auto_refresh_last_ymd';
const JOB_ID = 'po-fba-auto-refresh'; // config/jobs-registry.mjs の id

let started = false;
/**
 * FBA自動更新ワーカーを起動する (router.js から callWarehouse を注入)。
 * email dispatcher と同じ Render 限定 (miniPC も同じ server.js を動かすため、
 * 無条件だと2箇所からミニPCへ更新ジョブが飛ぶ)。
 */
export function startFbaAutoRefresh(callWarehouse) {
  if (started) return;
  if (!isRender()) {
    console.log('[po-fba-auto] 非Render環境のためFBA自動更新を起動しない');
    return;
  }
  started = true;
  const tick = async () => {
    try {
      const { due, skip, ymd } = fbaAutoDue(new Date(), getSetting(LAST_YMD_KEY));
      if (skip) {
        // 土日祝: 実行はしないが「ワーカーは生きていて判断した」ことを台帳に残す (dead-man の週末誤検知防止)
        setSetting(LAST_YMD_KEY, ymd, { actor: 'fba-auto-scheduler', actorType: 'system', reason: '土日祝のためFBA自動更新をスキップ' });
        console.log(`[po-fba-auto] ${ymd} は土日祝のためスキップ`);
        pingJob(JOB_ID, 'ok', `${ymd} 土日祝スキップ`);
      } else if (due) {
        const r = await callWarehouse('/service-api/fba/pml/fba-refresh', { method: 'POST', timeout: 30000 });
        if (r && r.jobId) {
          // 起動できた時点で当日実行済みにする (完了待ちにしない: 失敗してもその日は再突入せず、
          // 失敗は ping fail + ログで見える。毎分の再試行は「起動できなかった」場合のみ)
          setSetting(LAST_YMD_KEY, ymd, { actor: 'fba-auto-scheduler', actorType: 'system', reason: 'FBA在庫の16時自動更新を起動' });
          console.log(`[po-fba-auto] FBA更新ジョブ起動 jobId=${r.jobId}`);
          watchJob(callWarehouse, r.jobId);
        } else {
          // 手動更新が実行中など。窓内 (〜16:59) は毎分再試行し、窓を過ぎたら当日は諦める
          console.error('[po-fba-auto] ジョブを起動できず (次周期に再試行):', (r && (r.message || r.error)) || '応答にjobIdなし');
        }
      }
    } catch (e) {
      console.error('[po-fba-auto] tick失敗:', e.message);
    } finally {
      const t = setTimeout(tick, 60000);
      t.unref();
    }
  };
  const t0 = setTimeout(tick, 15000);
  t0.unref();
}

/** 起動したジョブの完了/失敗を追ってログと dead-man ping に残す (結果はUIに出ないためここが唯一の証跡) */
function watchJob(callWarehouse, jobId) {
  const deadline = Date.now() + 30 * 60000;
  const poll = async () => {
    try {
      const job = await callWarehouse(`/service-api/jobs/${encodeURIComponent(jobId)}`, { timeout: 15000 });
      if (job && job.status === 'completed') {
        console.log(`[po-fba-auto] FBA更新完了 jobId=${jobId}`);
        pingJob(JOB_ID, 'ok', `jobId=${jobId} 完了`);
        return;
      }
      if (job && job.status === 'failed') {
        console.error(`[po-fba-auto] FBA更新失敗 jobId=${jobId}: ${job.error || '理由不明'}`);
        pingJob(JOB_ID, 'fail', `jobId=${jobId} 失敗: ${job.error || '理由不明'}`);
        return;
      }
    } catch (e) {
      console.error('[po-fba-auto] ジョブ状態の取得失敗 (継続):', e.message);
    }
    if (Date.now() > deadline) {
      console.error(`[po-fba-auto] 30分待っても完了せず jobId=${jobId}`);
      pingJob(JOB_ID, 'fail', `jobId=${jobId} 30分タイムアウト`);
      return;
    }
    const t = setTimeout(poll, 20000);
    t.unref();
  };
  const t = setTimeout(poll, 20000);
  t.unref();
}
