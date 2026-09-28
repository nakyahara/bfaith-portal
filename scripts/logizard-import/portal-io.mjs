/**
 * portal-io.mjs — ポータルの「ロジザードの取込の状態」への書き込みの 3 つの結末と、鍵の延長 (マスタ正本切替 ③c-1b-2b-1c・契約 v3 K5・K7)
 *
 * 書き込みの結末 (K5):
 *   ok       = 期待する形の成功の応答
 *   refused  = 更新していないことが保証された口の断り (決まった code を持つ 4xx)
 *   unknown  = 時間切れ・通信断・5xx・応答が無い / 読めない。**それだけで「更新されなかった」とは見ない**
 * 応答が分からない後は、操作ごとに状態を読み直して照らす (confirm)。照らせた = ok (confirmed)・照らせない = unknown のまま。
 * 押す動きは二度としない (呼び手の決まり)。
 */

/** 口の断りの code (store.js の fail の code と router の 4xx)。これ以外の 4xx・5xx・通信の失敗 = unknown */
export const REFUSAL_CODES = Object.freeze(new Set([
  'bad_request', 'not_initialized', 'init_mismatch', 'busy', 'halted', 'not_halted', 'state', 'run_mismatch', 'run_used',
  'lock_lost', 'lock_used', 'bad_transition', 'nightly_done', 'partial_unchecked', 'stale', 'already_initialized', 'history_exists',
  'unauthorized', 'bad_json', 'too_large',
]));
/** 送る前に止まった (呼び手を作れない) / 口が中身を読む前に断った (503 not_configured) = 更新していない */
const NOT_SENT = Object.freeze(new Set(['no_token', 'bad_url', 'not_configured']));

/** 呼び手の例外 → 'refused' | 'unknown' */
export function classifyPortalError(e) {
  const status = e && Number.isInteger(e.status) ? e.status : null;
  const code = e && typeof e.code === 'string' ? e.code : null;
  if (code && NOT_SENT.has(code) && (status == null || status === 503)) return 'refused';
  if (status != null && status >= 400 && status < 500 && code && REFUSAL_CODES.has(code)) return 'refused';
  return 'unknown';
}

/**
 * 書き込み 1 回 (やり直さない)。応答が分からない = confirm() で状態を読み直して照らす。
 * @param {() => Promise<object>} call
 * @param {object} o
 * @param {(res: object) => boolean} [o.expect]  成功の応答の形の確かめ (違う = unknown)
 * @param {() => Promise<boolean>} [o.confirm]   応答が分からないとき、この書き込みが入ったかを状態で照らす (true = 入った)
 * @returns {Promise<{ outcome: 'ok'|'refused'|'unknown', confirmed?: boolean, res?: object, error?: string, code?: string }>}
 */
export async function portalWrite(call, { expect = () => true, confirm = null } = {}) {
  let res;
  try {
    res = await call();
  } catch (e) {
    const kind = classifyPortalError(e);
    if (kind === 'refused') return { outcome: 'refused', code: e.code, error: String(e.message).slice(0, 200) };
    return confirmOrUnknown(confirm, { error: String(e && e.message).slice(0, 200), code: e && e.code });
  }
  if (!res || res.ok === false || !expect(res)) return confirmOrUnknown(confirm, { error: 'unexpected_response' });
  return { outcome: 'ok', res };
}

async function confirmOrUnknown(confirm, info) {
  if (!confirm) return { outcome: 'unknown', ...info };
  try {
    if (await confirm()) return { outcome: 'ok', confirmed: true, ...info };
  } catch { /* 照らせない */ }
  return { outcome: 'unknown', confirmed: false, ...info };
}

/**
 * 鍵を 30 秒ごとに延ばす (K7)。延ばせた = 旗の締め切りを後ろへ。断られた = stop('lock_lost')・分からない = stop('lock_extend_unknown')
 * (押す前なら押さない。押した後は結果を待って書く = 呼び手)。stop() で止める。
 */
export function startHeartbeat({ client, lockToken, guard, ttlSec = 180, everyMs = 30000, marginMs = 20000, mapDeadline = null, setTimer = setInterval, clearTimer = clearInterval, onEvent = () => {} }) {
  let busy = false;
  const tick = async () => {
    if (busy) return;
    busy = true;
    try {
      const r = await portalWrite(() => client.extend({ lock_token: lockToken, ttl_sec: ttlSec }), { expect: (x) => Number.isFinite(x.expires_at) });
      // mapDeadline = 呼び手の締め切りの決まり (夜の止めの上限など。延ばしても越えない)
      if (r.outcome === 'ok') { guard.setDeadline(mapDeadline ? mapDeadline(r.res.expires_at) : r.res.expires_at - marginMs); onEvent({ kind: 'extended', expires_at: r.res.expires_at }); }
      else { guard.stop(r.outcome === 'refused' ? 'lock_lost' : 'lock_extend_unknown'); onEvent({ kind: 'extend_failed', outcome: r.outcome, code: r.code || null }); }
    } finally { busy = false; }
  };
  const h = setTimer(tick, everyMs);
  if (h && typeof h.unref === 'function') h.unref();
  return { tick, stop: () => clearTimer(h) };
}
