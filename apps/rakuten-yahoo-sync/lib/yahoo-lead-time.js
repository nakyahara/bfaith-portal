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
 */

import { fetchLeadTimeList, YahooProxyError } from './yahoo-publish-proxy.js';

export const TARGET_ID = 1000;

let _cachedResult = null;
let _inflight = null;

export async function runLeadTimePreflight({ forceRefresh = false, caller = null } = {}) {
  if (!forceRefresh && _cachedResult) return _cachedResult;

  // 緊急回避 stub: env 明示時のみ ok を返す (vps-proxy 障害時の退避)
  if (String(process.env.RYS_LEAD_TIME_VERIFIED || '0').trim() === '1') {
    _cachedResult = { ok: true, targetId: TARGET_ID, source: 'env_override_stub' };
    return _cachedResult;
  }

  if (caller) {
    if (!_inflight) {
      _inflight = (async () => {
        try {
          _cachedResult = await caller();
        } catch (e) {
          _cachedResult = { ok: false, error: e.message || String(e) };
        } finally {
          _inflight = null;
        }
      })();
    }
    await _inflight;
    return _cachedResult || { ok: false, error: 'unknown', targetId: TARGET_ID };
  }

  // 本実装: vps-proxy 経由で getLeadTimeList を叩く
  if (!_inflight) {
    _inflight = (async () => {
      try {
        const { ids } = await fetchLeadTimeList();
        const ok = ids.includes(TARGET_ID);
        _cachedResult = ok
          ? { ok: true, targetId: TARGET_ID, totalIds: ids.length, source: 'yahoo_api' }
          : { ok: false, error: `target_id_${TARGET_ID}_not_found_in_yahoo_store`, totalIds: ids.length, source: 'yahoo_api' };
      } catch (e) {
        const errMsg = e instanceof YahooProxyError
          ? `vps-proxy: ${e.message}`
          : (e.message || String(e));
        _cachedResult = { ok: false, error: errMsg, targetId: TARGET_ID, source: 'yahoo_api_failed' };
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
  _inflight = null;
}
