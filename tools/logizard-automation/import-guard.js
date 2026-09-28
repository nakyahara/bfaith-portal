/**
 * import-guard.js — ロジザードの取込で「押してよいか」を 1 か所で持つ旗 (マスタ正本切替 ③c-1b-2b-1b・契約 v3 K7・C)
 *
 * 押す操作 (実行ボタン・「ファイルアップロードを開始します」の OK) の直前に check() を呼ぶ:
 *   止めてある (鍵の延長に失敗・鍵を失った・書き込みの応答が分からない・想定外の dialog など) = 例外 (StopError)
 *   締め切り (鍵の期限・毎晩は 00:55) の余白の内でない = 止めて例外
 *   押してよい = click の持ち時間 (ms) を返す (締め切りまでの残りと maxClickMs の短い方 = 押せるようになるまでの待ちで締め切りを越えない)
 * stop(reason) は 1 回だけ効く。onStop で待っている操作 (ページを閉じる) を中断する。
 * 呼び手 (2b-1c のランナー) が鍵を延ばせたら setDeadline で締め切りを後ろへ。延ばせなかったら stop('lock_extend_failed')。
 */
export class StopError extends Error {
  constructor(message, reason) { super(message); this.stopped = true; this.reason = reason; }
}

/**
 * @param {object} [o]
 * @param {() => number} [o.now]
 * @param {number|null} [o.deadlineMs]  押してよい最後の時刻 (ms・epoch)。null = 締め切りなし (試験用)
 * @param {number} [o.marginMs]  締め切りの前の余白 (この内では押さない)
 * @param {number} [o.maxClickMs]
 */
export function createGuard({ now = () => Date.now(), deadlineMs = null, marginMs = 5000, maxClickMs = 30000 } = {}) {
  let reason = null;
  let deadline = deadlineMs;
  const listeners = [];
  const guard = {
    get reason() { return reason; },
    get deadline() { return deadline; },
    isStopped: () => reason !== null,
    stop(why) {
      if (reason !== null) return false;
      reason = String(why || 'stopped');
      for (const f of listeners.splice(0)) { try { f(reason); } catch { /* 止める処理の失敗は無視 (止める旗は立っている) */ } }
      return true;
    },
    /** 止めたときに呼ぶ (もう止めてある = すぐ呼ぶ) */
    onStop(f) { if (reason !== null) { try { f(reason); } catch { /* */ } } else listeners.push(f); },
    setDeadline(ms) { deadline = ms; },
    /** 押す直前。止めてある・締め切りの余白の内でない = StopError / 押してよい = click の持ち時間 (ms) */
    check(where) {
      if (reason !== null) throw new StopError(`${where}: 止めた (${reason})`, reason);
      if (deadline == null) return maxClickMs;
      const left = deadline - marginMs - now();
      if (left <= 0) {
        guard.stop('deadline');
        throw new StopError(`${where}: 押してよい時刻を過ぎた (締め切り ${new Date(deadline).toISOString()}・余白 ${marginMs / 1000} 秒)`, 'deadline');
      }
      return Math.min(maxClickMs, left);
    },
  };
  return guard;
}
