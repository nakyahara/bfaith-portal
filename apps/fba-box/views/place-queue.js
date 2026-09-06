/**
 * 投入 (割当) の送信キュー — 通信断で「二重に記録される」「記録が消える」を防ぐ。
 *
 * 背景 (Codex PR2.6-R4 high#1 / R5 high#1〜4): POST がサーバーでは成功したのに応答だけ
 * 失われることがある。このとき端末に残した1件を、**同じ request_id で**送り直す
 * (サーバーは device_key × request_id で冪等) のが基本方針。ところが素朴に書くと、
 *   - 送り直して確定した直後に、新しい request_id でもう一度投入して**二重に記録される**
 *   - 裏で送り直して成功したことを人に見せないので、利用者が「失敗した」と思って押し直す
 *   - 送信と送り直しが並行し、古いほうの完了が新しい1件を消す
 *   - 「押し直し」と「本当にもう一度入れた」を内容の比較では区別できない (期限・配置は
 *     開き直すと値が変わるので一致しない = 二重に入る。逆に一致させると本物の2回目を飲み込む)
 * が起きる。この4点を構造で潰すのがこのモジュール。
 *
 *   1. 状態は `sending` (未確定) → `resolved` (確定したが人にまだ見せていない) → 消す、の3段階。
 *      **確定しただけでは消さない**。人が結果を見た (ack) ときに初めて消す
 *   2. 端末に残した1件を消す・書き換えるのは、**いま保存されているのが自分の requestId のときだけ** (CAS)
 *   3. 送信も送り直しも1本の Promise チェーンに直列化する (同時に走らせない)
 *   4. 確定済みの1件と**同じ商品・同じ箱**への投入が来たら、自動で決めずに `confirm` を返す。
 *      呼び出し側が「さらに N 個入れましたか?」と人に聞く (既定 = 入れていない)。
 *      個数・期限・配置は開き直すと変わるので**判定に使わない**
 *
 * ブラウザ: 素の <script> で読んで `window.createPlaceQueue` を使う。
 * テスト: node:vm でこのファイルを評価して同じ関数を取り出す (scripts/test-fba-box-place-queue.mjs)
 */
(function (global) {
  'use strict';

  function defaultNewId() {
    if (global.crypto && global.crypto.randomUUID) return global.crypto.randomUUID();
    return Date.now() + '-' + Math.random().toString(36).slice(2);
  }

  /**
   * @param {object} deps
   * @param {(body:object)=>Promise<object>} deps.post
   *   確定応答 (アプリが返した JSON) を返す。サーバーが処理したか不明なときは throw する。
   * @param {{get:()=>object|null, set:(v:object|null)=>void}} deps.storage
   *   端末に残す1件 (localStorage 等)。読めない環境では get が null を返してよい。
   * @param {()=>string} [deps.newId]
   * @param {(ms:number)=>Promise<void>} [deps.wait]
   * @param {number} [deps.retryWaitMs] 新規送信のときだけ1回置く再試行の待ち
   */
  function createPlaceQueue({ post, storage, newId = defaultNewId, wait, retryWaitMs = 1200 }) {
    const sleep = wait || ((ms) => new Promise((r) => setTimeout(r, ms)));
    // 送信・送り直し・確認後の送信をすべて1本に並べる。並行して走らせると、
    // 遅れて終わったほうが新しい1件を消してしまう (Codex R5 high#3)
    let chain = Promise.resolve();
    function serial(fn) {
      const run = chain.then(fn, fn);
      chain = run.then(() => {}, () => {});
      return run;
    }

    const peek = () => storage.get() || null;

    /** いま保存されているのが自分の1件のときだけ書き換える (CAS)。他人の1件は消さない */
    function casSet(requestId, next) {
      const cur = peek();
      if (!cur || cur.requestId !== requestId) return false;
      storage.set(next);
      return true;
    }

    /**
     * 1件を送る。確定したら resolved にして返す。未確定のままなら null
     * (保存はそのまま = 電波が戻れば同じ request_id で送り直せる)
     */
    async function settle(rec, attempts) {
      for (let i = 0; i < attempts; i++) {
        try {
          const result = await post(rec.body);
          const resolved = Object.assign({}, rec, { status: 'resolved', result });
          casSet(rec.requestId, resolved);
          return resolved;
        } catch (e) {
          if (i < attempts - 1) await sleep(retryWaitMs);
        }
      }
      return null;
    }

    /** 新しい1件として送る。戻り値は resolved か、未確定のままの sending */
    async function postNew(body, meta) {
      const requestId = newId();
      const rec = {
        requestId, meta: meta || null, status: 'sending',
        body: Object.assign({}, body, { request_id: requestId }),
      };
      storage.set(rec);
      return (await settle(rec, 2)) || rec;
    }

    return {
      /** 端末に残っている1件 (なければ null)。画面の状態表示に使う */
      peek,

      /**
       * 裏で送り直す (画面の更新のたびに呼ぶ)。
       * 戻り値 = 確定していて**まだ人に見せていない**1件 / 見せるものが無ければ null。
       * ⚠ 呼び出し側は必ず見せて ack すること。捨てると
       * 「現物は箱に入れたのに記録が無い」「裏で入ったのに利用者が押し直す」を見逃す
       */
      flush() {
        return serial(async () => {
          const rec = peek();
          if (!rec) return null;
          if (rec.status === 'resolved') return rec;   // 前回見せそびれた結果
          return await settle(rec, 1);
        });
      },

      /**
       * 投入を送る。戻り値の kind:
       *   'done'    … record.status が 'resolved' なら result あり (成功でも業務エラーでも確定)。
       *               'sending' なら未確定 (電波が戻れば自動で送り直す)
       *   'blocked' … 前の1件がまだ確定していない。**新しい request_id は発行していない**
       *   'confirm' … 確定済みの1件が同じ商品・同じ箱にある。押し直しか本当の2回目か
       *               決められないので人に聞く (prev = その1件 / body・meta = いま押した分)
       */
      send({ body, meta }) {
        return serial(async () => {
          let prev = peek();
          if (prev && prev.status === 'sending') {
            const settled = await settle(prev, 1);      // まず前の1件を確定させる
            if (!settled) return { kind: 'blocked', record: prev };
            prev = settled;
          }
          if (prev && prev.status === 'resolved') {
            const same = prev.body.row_id === body.row_id && prev.body.box_id === body.box_id;
            if (same) return { kind: 'confirm', prev, body, meta };
            storage.set(null);                          // 別の商品・別の箱 = 迷いようがない
            return { kind: 'done', record: await postNew(body, meta), alsoResolved: prev };
          }
          return { kind: 'done', record: await postNew(body, meta) };
        });
      },

      /**
       * confirm に「はい、さらに入れました」と答えられたとき。
       * 前の1件を確認済みにしてから、新しい1件として送る
       */
      confirmAndSend({ ackRequestId, body, meta }) {
        return serial(async () => {
          casSet(ackRequestId, null);
          return { kind: 'done', record: await postNew(body, meta) };
        });
      },

      /** 人が結果を見た → 端末から消す。自分の1件でなければ何もしない (CAS) */
      ack(requestId) {
        return serial(async () => casSet(requestId, null));
      },
    };
  }

  global.createPlaceQueue = createPlaceQueue;
})(typeof window !== 'undefined' ? window : globalThis);
