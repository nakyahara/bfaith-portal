/**
 * Yahoo lead_time_instock=1000 preflight (Codex R5 high-6 + Phase 0 Q26):
 *   全商品で固定 1000 を送る方針だが、 Yahoo ストア発送日設定マスタに ID=1000 が
 *   実在するかを publish 試行前にチェックする。
 *
 * E-4 (本ファイル): caller 差し替え可能な stub。 default は env RYS_LEAD_TIME_VERIFIED=1 のときのみ ok=true。
 *   E-5 で Yahoo getLeadTimeList API を実呼び出しする実装に置き換え予定。
 */

export const TARGET_ID = 1000;

let _cachedResult = null;
let _inflight = null;

/**
 * 1000 が実在するかを判定。
 *   - env RYS_LEAD_TIME_VERIFIED=1 なら ok=true (中原さんが手動で 1000 実在を Yahoo Store で確認した状態)
 *   - そうでなければ ok=false (E-5 で実 API 呼び出しに切り替えるまでの fail-closed)
 *   - caller オプションで実 API 呼び出しに差し替え可能
 */
export async function runLeadTimePreflight({ forceRefresh = false, caller = null } = {}) {
  if (!forceRefresh && _cachedResult) return _cachedResult;
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
  // default stub: env flag で OK/NG
  const verified = String(process.env.RYS_LEAD_TIME_VERIFIED || '0').trim() === '1';
  _cachedResult = verified
    ? { ok: true, targetId: TARGET_ID, source: 'env_stub' }
    : { ok: false, error: `lead_time_preflight_stub: set RYS_LEAD_TIME_VERIFIED=1 after confirming ID ${TARGET_ID} exists in Yahoo Store master`, targetId: TARGET_ID, source: 'env_stub' };
  return _cachedResult;
}

export function _resetForTest() {
  _cachedResult = null;
  _inflight = null;
}
