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
  // 🚨 **新しい映像が届かなくなってから**この時間で打ち切る (実時間)。
  //    2026-09-07 の実機では、カメラは開いたのに映像が黒いままで、案内だけが出続けた。
  //    ⚠ ループ回数で数えてはいけない (Codex #1235 R1 P2) — 解析の重さや iOS のタイマー抑制で
  //      実時間が伸び縮みし、「8秒で止まる」という現場への約束が守れない
  var NO_VIDEO_TIMEOUT_MS = 8000;
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
    return { lastSeen: null, repeats: 0, failStreak: 0 };
  }

  /**
   * 1フレームぶん進める。
   * @param {{lastSeen: string|null, repeats: number, failStreak: number}} state
   * @param {{kind: 'codes'|'error'|'idle', codes?: Array}} event
   *   codes = 解析した / error = 解析が例外で落ちた / idle = 新しいコマがまだ来ていない
   * @returns {{state: object, action: 'continue'|'accept'|'abort', value?: string, reason?: 'decode'}}
   */
  function step(state, event) {
    var st = state || initialState();
    var kind = event && event.kind;

    // 🚨 例外も「新しい映像がまだ」も **連続を切る**。A → 例外 → A で確定させない (Codex #1233 R2)
    if (kind === 'error') {
      var fs = (st.failStreak || 0) + 1;
      return { state: { lastSeen: null, repeats: 0, failStreak: fs }, action: fs >= MAX_FAIL_STREAK ? 'abort' : 'continue', reason: 'decode' };
    }
    // idle = 新しいコマがまだ来ていない。**同じコマを2回読んで「2回続けて一致」にしない**ためにも要る
    if (kind !== 'codes') {
      return { state: { lastSeen: null, repeats: 0, failStreak: st.failStreak || 0 }, action: 'continue' };
    }

    var v = pickValue(event.codes);
    if (!v) return { state: { lastSeen: null, repeats: 0, failStreak: 0 }, action: 'continue' };
    var repeats = v === st.lastSeen ? (st.repeats || 0) + 1 : 1;
    if (repeats >= REQUIRED_REPEATS) {
      return { state: initialState(), action: 'accept', value: v };
    }
    return { state: { lastSeen: v, repeats: repeats, failStreak: 0 }, action: 'continue' };
  }

  // ─── 映像が届いているかの見張り (実時間) ───────────────────────────────────
  // 🚨 **「解析できたか」で判断してはいけない** (Codex #1235 R1 P1)。
  //    映像が黒一色でも止まっていても、デコード結果は「見つからなかった (空配列)」で返る。
  //    それを正常扱いすると、2026-09-07 の実機症状 (readyState は進むが表示は黒) がそのまま再発する。
  //    見るのは **新しいコマが届いたかどうか** (画面側は video.currentTime が進んだかで判定する)。

  function newVideoWatch(nowMs) { return { startedAt: nowMs, lastFrameAt: null }; }

  // ─── フレームの受け渡し ───────────────────────────────────────────────────
  // 🚨 **見張りと解析で印を分ける** (Codex #1235 R2 P2)。1つの印を共有すると、見張りが先に
  //    新しいコマを観測した瞬間に解析側が「新しくない」と判断して飛ばし、映像は正常なのに
  //    いつまでも読み取れなくなる。
  //    arrived = 届いた最新のコマ / decoded = 解析し終えたコマ。
  //    こうしておくと **同じコマを2回解析しない** = 「2回続けて一致」が実質1回にならない。

  function newFrameCursor() { return { arrived: null, decoded: null }; }

  /**
   * コマが届いた印を付ける。
   * 🚨 `sourceLive` が false のあいだは**進めない** (Codex #1235 R2 P1)。
   *    MediaStreamTrack が muted になると、カメラが映像を出せなくても video は黒いコマを
   *    再生し続ける (currentTime も進む)。それを「届いた」と数えると見張りが永久に鳴らない。
   */
  function markArrived(cursor, id, sourceLive) {
    if (!sourceLive || id === null || id === undefined) return cursor;
    // 🚨 **同じコマを見ただけなら「届いた」ではない** (Codex #1235 R3)。
    //    ここで新しいオブジェクトを返すと、requestVideoFrameCallback が使えない端末で
    //    currentTime を繰り返し渡したときに見張りの時刻が更新され続け、
    //    映像が止まっていても8秒の打ち切りが永久に発火しない
    if (id === cursor.arrived) return cursor;
    return { arrived: id, decoded: cursor.decoded };
  }

  /** 解析すべき新しいコマがあるか。あれば decoded を進めた cursor を返す */
  function nextDecode(cursor) {
    if (cursor.arrived === null || cursor.arrived === cursor.decoded) return { decode: false, cursor: cursor };
    return { decode: true, cursor: { arrived: cursor.arrived, decoded: cursor.arrived } };
  }

  /** 新しいコマが届いた */
  function noteFrame(watch, nowMs) { return { startedAt: watch.startedAt, lastFrameAt: nowMs }; }

  /**
   * 打ち切ってよいか。**1コマも来ない**場合も、**途中で止まった**場合も同じ物差しで測る。
   * @param {number} timeoutMs 既定 NO_VIDEO_TIMEOUT_MS
   */
  function videoStalled(watch, nowMs, timeoutMs) {
    if (!watch) return false;
    var limit = typeof timeoutMs === 'number' ? timeoutMs : NO_VIDEO_TIMEOUT_MS;
    var base = watch.lastFrameAt == null ? watch.startedAt : watch.lastFrameAt;
    return nowMs - base >= limit;
  }

  return {
    initialState: initialState,
    step: step,
    pickValue: pickValue,
    newVideoWatch: newVideoWatch,
    noteFrame: noteFrame,
    newFrameCursor: newFrameCursor,
    markArrived: markArrived,
    nextDecode: nextDecode,
    videoStalled: videoStalled,
    MAX_FAIL_STREAK: MAX_FAIL_STREAK,
    NO_VIDEO_TIMEOUT_MS: NO_VIDEO_TIMEOUT_MS,
    REQUIRED_REPEATS: REQUIRED_REPEATS,
  };
}));
