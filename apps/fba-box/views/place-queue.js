/**
 * 投入 (割当) の送信キュー — 通信断で「二重に記録される」「記録が消える」を防ぐ。
 *
 * 背景 (Codex PR2.6-R4 high#1 / R5 high#1〜4): POST がサーバーでは成功したのに応答だけ
 * 失われることがある。このとき端末に残した1件を**同じ request_id で**送り直す
 * (サーバーは device_key × request_id で冪等) のが基本方針。ところが素朴に書くと、
 *   - 送り直して確定した直後に、新しい request_id でもう一度投入して**二重に記録される**
 *   - 裏で送り直して成功したことを人に見せないので、利用者が「失敗した」と思って押し直す
 *   - 送信と送り直しが並行し、古いほうの完了が新しい1件を消す
 *   - 「押し直し」と「本当にもう一度入れた」を内容の比較では区別できない (期限・配置は
 *     開き直すと値が変わるので一致しない = 二重に入る。逆に一致させると本物の2回目を飲み込む)
 * が起きる。これを構造で潰す。
 *
 *   1. 状態は `sending` (未確定) → `resolved` (確定したが人にまだ見せていない) → 消す、の3段階。
 *      **確定しただけでは消さない**。人が結果を見て押した (ack) ときに初めて消す
 *   2. **確定していない・見せていない1件があるうちは、次の投入を始めない**。
 *      勝手に消して先へ進むと、その結果は二度と復元できない (PQ-R1 high#2)
 *   3. 端末に残した1件を書き換える・消すのは、**いま保存されているのが自分の requestId の
 *      ときだけ** (CAS)。失敗したら conflict を返して**新しい id を発行しない** (PQ-R1 high#3)
 *   4. 送信も送り直しも1本の Promise チェーンに直列化する
 *   5. 保存できたことを読み直して確かめてから POST する。保存できない端末 (Safari の
 *      プライベートモード等) では送らない — 送り直せないので二重登録の元になる (PQ-R1 medium#1)
 *   6. 前の1件が**同じ商品**なら、押し直しか本当の2回目か決められないので `confirm` を返す
 *      (呼び出し側が人に聞く)。箱が違っても同じ商品なら聞く — 箱が閉じられた・入れ直した等で
 *      箱だけ変わることがあるため (PQ-R1 medium#2)
 *   7. 前の1件が**業務エラー (ok:false)** なら「記録できている」とは言えないので、
 *      confirm ではなく `previousFailed` を返す (PQ-R1 high#1)
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
   * 端末に残っている1件を読む。
   * この版より前は `{requestId, body}` (status なし) で保存していた。更新直後の iPad には
   * それが残っているので `sending` とみなす — 見落として新規送信すると、旧 POST が既に
   * 成功していた場合に二重登録になる (PQ-R1 high#6)。
   * 形が分からないものは null ではなく broken として返し、**絶対に上書きしない**
   */
  function readRecord(storage) {
    let raw = null;
    try { raw = storage.get(); } catch (e) { return { broken: true, reason: 'read_failed' }; }
    if (!raw) return null;
    if (typeof raw !== 'object' || !raw.requestId || !raw.body) return { broken: true, reason: 'unknown_shape', raw };
    if (raw.status === 'sending' || raw.status === 'resolved') return raw;
    if (raw.status == null) return Object.assign({}, raw, { status: 'sending', legacy: true });
    return { broken: true, reason: 'unknown_status', raw };
  }

  /**
   * @param {object} deps
   * @param {(body:object)=>Promise<object>} deps.post
   *   確定応答 (アプリが返した JSON) を返す。サーバーが処理したか不明なときは throw する
   * @param {{get:()=>object|null, set:(v:object|null)=>void}} deps.storage 端末に残す1件
   * @param {()=>string} [deps.newId]
   * @param {(ms:number)=>Promise<void>} [deps.wait]
   * @param {number} [deps.retryWaitMs] 新規送信のときだけ置く再試行の待ち
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

    const peek = () => readRecord(storage);

    /** いま保存されているのが自分の1件のときだけ書き換える (CAS)。他の画面の1件は消さない */
    function casSet(requestId, next) {
      const cur = peek();
      if (!cur || cur.broken || cur.requestId !== requestId) return false;
      try { storage.set(next); } catch (e) { return false; }
      return true;
    }

    /** 保存して、読み直して入っていることを確かめる (保存できない端末では送らない) */
    function saveNew(rec) {
      try { storage.set(rec); } catch (e) { return false; }
      const back = peek();
      return !!(back && !back.broken && back.requestId === rec.requestId);
    }

    /**
     * 1件を送る。確定したら resolved にして返す。未確定のままなら null
     * (保存はそのまま = 電波が戻れば同じ request_id で送り直せる)。
     * CAS に失敗しても**結果は捨てない** — 捨てると人に伝わらない。stored:false を付けて返す
     */
    async function settle(rec, attempts) {
      for (let i = 0; i < attempts; i++) {
        try {
          const result = await post(rec.body);
          const resolved = Object.assign({}, rec, { status: 'resolved', result });
          resolved.stored = casSet(rec.requestId, resolved);
          return resolved;
        } catch (e) {
          if (i < attempts - 1) await sleep(retryWaitMs);
        }
      }
      return null;
    }

    /** 新しい1件として送る。端末が空でなければ送らない (conflict) */
    async function postNew(body, meta) {
      const cur = peek();
      if (cur) return { kind: cur.broken ? 'broken' : 'conflict', record: cur };
      const requestId = newId();
      const rec = {
        requestId, meta: meta || null, status: 'sending',
        body: Object.assign({}, body, { request_id: requestId }),
      };
      if (!saveNew(rec)) return { kind: 'storage_failed' };
      return { kind: 'done', record: (await settle(rec, 2)) || rec };
    }

    return {
      /** 端末に残っている1件 (なければ null / 形が違えば {broken:true}) */
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
          if (!rec || rec.broken) return null;
          if (rec.status === 'resolved') return rec;   // 前回見せそびれた結果
          return await settle(rec, 1);
        });
      },

      /**
       * 投入を送る。戻り値の kind:
       *   'done'           … record.status が 'resolved' なら result あり (成功でも業務エラーでも確定)。
       *                      'sending' なら未確定 (電波が戻れば自動で送り直す)
       *   'blocked'        … 前の1件がまだ確定していない。**新しい request_id は発行していない**
       *   'confirm'        … 前の1件が確定・成功していて**同じ商品**。押し直しか本当の2回目か
       *                      決められないので人に聞く (prev / body / meta)
       *   'showFirst'      … 前の1件が確定・成功していて別の商品。先にその結果を見せてから続ける
       *   'previousFailed' … 前の1件が確定したが登録できていない。まずそれを見せる
       *   'conflict'       … 別の画面が端末の1件を書き換えた。画面を更新してもらう
       *   'broken'         … 端末に読めない1件が残っている (上書きしない)
       *   'storage_failed' … 端末に保存できない = 送り直せないので送らない
       */
      send({ body, meta }) {
        return serial(async () => {
          let prev = peek();
          if (prev && prev.broken) return { kind: 'broken', record: prev };
          if (prev && prev.status === 'sending') {
            const settled = await settle(prev, 1);      // まず前の1件を確定させる
            if (!settled) return { kind: 'blocked', record: prev };
            prev = settled;
          }
          if (prev && prev.status === 'resolved') {
            if (!prev.result || prev.result.ok !== true) return { kind: 'previousFailed', prev, body, meta };
            // 同じ商品なら「押し直し」か「本当の2回目」か決められない。箱が違っても聞く
            // (箱が閉じられた・入れ直したで箱だけ変わることがある — PQ-R1 medium#2)
            const kind = prev.body.row_id === body.row_id ? 'confirm' : 'showFirst';
            return { kind, prev, body, meta };
          }
          return await postNew(body, meta);
        });
      },

      /**
       * 前の1件を人が確認したうえで、新しい1件として送る。
       * confirm の「はい、さらに入れた」/ showFirst の「わかりました」から呼ぶ
       */
      ackAndSend({ ackRequestId, body, meta }) {
        return serial(async () => {
          if (!casSet(ackRequestId, null)) return { kind: 'conflict', record: peek() };
          return await postNew(body, meta);
        });
      },

      /** 人が結果を見た → 端末から消す。自分の1件でなければ何もしない (CAS) */
      ack(requestId) {
        return serial(async () => casSet(requestId, null));
      },

      /** 読めない1件を捨てる (職員が「この記録を捨てる」を選んだときだけ) */
      discardBroken() {
        return serial(async () => {
          const cur = peek();
          if (!cur || !cur.broken) return false;
          try { storage.set(null); } catch (e) { return false; }
          return true;
        });
      },
    };
  }

  global.createPlaceQueue = createPlaceQueue;
})(typeof window !== 'undefined' ? window : globalThis);
