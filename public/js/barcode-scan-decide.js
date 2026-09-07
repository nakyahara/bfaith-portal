/**
 * 📷 バーコード読み取りの「採用するか」の判断だけを切り出したもの。
 * カメラも DOM も wasm も触らない純粋な状態遷移なので、そのままテストできる
 * (scripts/test-inbound-check-barcode-scan.mjs)。
 *
 * 🚨なぜ切り出してあるか (2026-09-07 Codex #1233 R2):
 *   ここは「別の商品を出さない」ための安全弁そのもの。画面のインライン JS に埋めたままだと、
 *   テストは「その行が書いてあるか」を正規表現で見るしかなく、**挙動は守れない**。
 *
 * 使い方 (画面):
 *   let st = BarcodeScanDecide.initialState();
 *   const r = BarcodeScanDecide.step(st, { kind: 'codes', codes });   // 1フレームぶん
 *   st = r.state;
 *   r.action === 'accept'   → r.value で確定
 *   r.action === 'abort'    → 読み取りを止めて理由を出す
 *   r.action === 'continue' → 次のフレームへ
 */
(function (root, factory) {
  root.BarcodeScanDecide = factory();
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  // 読み取った値として受け付ける形。JAN(13桁) も FNSKU(英数字10桁) も通る
  var VALUE_RE = /^[A-Za-z0-9]{6,}$/;
  // 続けてこの回数だけ解析に失敗したら止める (250ms 間隔なので 8 回 = 約2秒)。
  // 黙って回し続けると、現場には正常時と同じ案内が出たままになる
  var MAX_FAIL_STREAK = 8;
  // 🚨 **映像が1コマも来ないまま**この回数を過ぎたら止める (250ms 間隔なので 32 回 = 約8秒)。
  //    2026-09-07 の実機では、カメラは開いたのに映像が黒いままで、案内だけが出続けた。
  //    「映像が来ない」を無言で回し続けると、現場は正常との区別がつかない
  var MAX_IDLE_STREAK = 32;
  // 確定に必要な「同じ値が続けて読めた回数」。
  // 🚨 Code39 / Code128 / ITF はチェックデジットが無く、枠に入れる途中のブレや
  //    棚の別ラベルの一部を「読めた」と返すことがある。1フレームでは信じない
  var REQUIRED_REPEATS = 2;

  /** 1フレームの読み取り結果から、使ってよい値を1つ選ぶ (無ければ null) */
  function pickValue(codes) {
    if (!codes || !codes.length) return null;
    for (var i = 0; i < codes.length; i++) {
      var c = codes[i];
      if (!c || !c.text) continue;
      // isValid === false = チェックデジットが合っていない (EAN/UPC)。そのまま検索しない
      if (c.isValid === false) continue;
      var v = String(c.text).trim();
      if (VALUE_RE.test(v)) return v;
    }
    return null;
  }

  function initialState() {
    return { lastSeen: null, repeats: 0, failStreak: 0, idleStreak: 0 };
  }

  /**
   * 1フレームぶん進める。
   * @param {{lastSeen: string|null, repeats: number, failStreak: number}} state
   * @param {{kind: 'codes'|'error'|'idle', codes?: Array}} event
   *   codes = 解析できた / error = 解析が例外で落ちた / idle = 映像がまだ1コマも来ていない
   * @returns {{state: object, action: 'continue'|'accept'|'abort', value?: string, reason?: 'decode'|'no_video'}}
   */
  function step(state, event) {
    var st = state || initialState();
    var kind = event && event.kind;

    // 🚨 例外も「映像がまだ」も **連続を切る**。A → 例外 → A で確定させない (Codex #1233 R2)
    if (kind === 'error') {
      var fs = (st.failStreak || 0) + 1;
      var next = { lastSeen: null, repeats: 0, failStreak: fs, idleStreak: 0 };
      return { state: next, action: fs >= MAX_FAIL_STREAK ? 'abort' : 'continue', reason: 'decode' };
    }
    if (kind !== 'codes') {
      var is = (st.idleStreak || 0) + 1;
      return {
        state: { lastSeen: null, repeats: 0, failStreak: st.failStreak || 0, idleStreak: is },
        action: is >= MAX_IDLE_STREAK ? 'abort' : 'continue',
        reason: 'no_video',
      };
    }

    var v = pickValue(event.codes);
    if (!v) return { state: { lastSeen: null, repeats: 0, failStreak: 0, idleStreak: 0 }, action: 'continue' };
    var repeats = v === st.lastSeen ? (st.repeats || 0) + 1 : 1;
    if (repeats >= REQUIRED_REPEATS) {
      return { state: initialState(), action: 'accept', value: v };
    }
    return { state: { lastSeen: v, repeats: repeats, failStreak: 0, idleStreak: 0 }, action: 'continue' };
  }

  return {
    initialState: initialState,
    step: step,
    pickValue: pickValue,
    MAX_FAIL_STREAK: MAX_FAIL_STREAK,
    MAX_IDLE_STREAK: MAX_IDLE_STREAK,
    REQUIRED_REPEATS: REQUIRED_REPEATS,
  };
}));
