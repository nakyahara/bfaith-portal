/**
 * Yahoo lead_time_instock=1000 preflight (Phase 0 Q26 + Codex R5 high-6 確定):
 *   全商品で固定 1000 を送る方針だが、 Yahoo ストア発送日設定マスタに ID=1000 が
 *   実在するかを publish 試行前にチェックする。
 *
 * Phase E-5b 改: vps-proxy 経由で実 API 呼び出しに差替。
 *   - vps-proxy /yahoo/getLeadTimeList を叩いて ID 一覧取得
 *   - 1000 が含まれているか確認
 *   - process 内 cache あり (forceRefresh で破棄)
 *
 * 旧 stub 経路 (env RYS_LEAD_TIME_VERIFIED=1) は、 vps-proxy が未 deploy or 障害時の
 * 緊急回避用に残置 (env で明示的に強制 ok を返せる)。
 *
 * 2026-10-22 修正: 旧実装は**失敗の結果もプロセスが終わるまで覚えていた**。vps-proxy が一瞬落ちただけで
 *   以後の出品前チェックが全商品 lead_time_invalid で「修正必要」に倒れ、Render を再起動するまで直らなかった。
 *   また緊急回避の env も、覚えた失敗を返す行より後ろにあったので効かなかった。今は:
 *     - env の緊急回避を最初に見る
 *     - 成功は SUCCESS_TTL_MS (6 時間) だけ覚える (Yahoo 側で設定を消したら半日以内に気づく)
 *     - 失敗は FAILURE_TTL_MS (1 分) だけ覚える (sweep の 400 件で 400 回叩かない程度。すぐ取り直す)
 */

import { fetchLeadTimeList, YahooProxyError } from './yahoo-publish-proxy.js';

export const TARGET_ID = 1000;
export const SUCCESS_TTL_MS = 6 * 60 * 60 * 1000;
export const FAILURE_TTL_MS = 60 * 1000;

let _cachedResult = null;
let _cachedAt = 0;
let _inflight = null;

function cacheValid(now = Date.now()) {
  if (!_cachedResult) return false;
  const ttl = _cachedResult.ok ? SUCCESS_TTL_MS : FAILURE_TTL_MS;
  return now - _cachedAt < ttl;
}

function remember(result) {
  _cachedResult = result;
  _cachedAt = Date.now();
  return result;
}

export async function runLeadTimePreflight({ forceRefresh = false, caller = null } = {}) {
  // 緊急回避 stub: env 明示時のみ ok を返す (vps-proxy 障害時の退避)。覚えた結果より先に見る
  if (String(process.env.RYS_LEAD_TIME_VERIFIED || '0').trim() === '1') {
    return { ok: true, targetId: TARGET_ID, source: 'env_override_stub' };
  }
  if (!forceRefresh && cacheValid()) return _cachedResult;

  if (!_inflight) {
    _inflight = (async () => {
      try {
        if (caller) {
          remember(await caller());
          return;
        }
        // 本実装: vps-proxy 経由で getLeadTimeList を叩く
        const { ids } = await fetchLeadTimeList();
        const ok = ids.includes(TARGET_ID);
        remember(ok
          ? { ok: true, targetId: TARGET_ID, totalIds: ids.length, source: 'yahoo_api' }
          : { ok: false, error: `target_id_${TARGET_ID}_not_found_in_yahoo_store`, totalIds: ids.length, source: 'yahoo_api' });
      } catch (e) {
        const errMsg = e instanceof YahooProxyError
          ? `vps-proxy: ${e.message}`
          : (e.message || String(e));
        remember(caller
          ? { ok: false, error: errMsg }
          : { ok: false, error: errMsg, targetId: TARGET_ID, source: 'yahoo_api_failed' });
      } finally {
        _inflight = null;
      }
    })();
  }
  await _inflight;
  return _cachedResult || { ok: false, error: 'unknown', targetId: TARGET_ID };
}

export function _resetForTest() {
  _cachedResult = null;
  _cachedAt = 0;
  _inflight = null;
}
