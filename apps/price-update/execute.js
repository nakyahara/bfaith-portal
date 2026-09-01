/**
 * price-update / 実行フロー (要件定義 v1.0 F4・M2)
 *
 * 記録した run の中から「更新できる行」だけを、楽天へ順番に送る。
 *
 * ★ここが守ること (どれも「事故を小さく止める」ためのもの):
 *   ・実行するのは**保存済みの run**の内容だけ。画面から価格を再受領しない
 *     (画面を書き換えて別の値を送る、という抜け道を作らない)
 *   ・**同じ run は一度しか実行できない** (DB の claim を原子的に取る)。
 *     二重クリック・ブラウザの再送・複数インスタンスからの同時要求で、同じ行を2回送らない
 *   ・送る前にサーバ側でガードを**もう一度**評価する
 *   ・モール別の kill switch。env が明示的に有効でなければ 1件も送らない (fail-closed)
 *   ・**試運転**: 最初の1件だけ送って、API再取得で価格が変わったことを確かめてから残りを続ける
 *   ・**サーキットブレーカー**: 連続2件失敗で残りを止める。
 *     「意味の分かる失敗」以外 (想定外の応答・5xx・結果不明) は**1件目でも即停止**
 *   ・1件ずつ直列。並行して送らない
 *   ・状態はすべてイベントとして追記する (pu_events)。行は書き換えない
 *
 * 楽観ロック (更新前価格の一致確認) と冪等 (operation_id) は miniPC 側が持つ。
 * ここは「何をどの順で送り、どこで止めるか」を決める。
 */
import { appendEvent, claimRun } from './db.js';
import { evaluateRow } from './pricing.js';

/** 連続でこの回数失敗したら、そのモールの残りを止める */
export const BREAKER_CONSECUTIVE_FAILURES = 2;

/**
 * このバージョンで実際に送れるモール。
 * ★楽天だけ。Yahoo を有効にしても送り先のクライアントが楽天しか無いため、
 *   ここで止めないと「Yahoo の出品コードを楽天の管理番号として送る」ことになる (Codex R1 High)。
 *   Yahoo は M3 で送信経路と一緒に開ける。
 */
export const EXECUTABLE_MALLS = ['rakuten'];

const TRUE_VALUES = new Set(['1', 'true', 'on', 'yes']);

/**
 * モール別 kill switch。**明示的に有効でなければ送らない** (env 未設定 = 停止)。
 * @returns {{enabled:boolean, reason:string|null}}
 */
export function mallWriteEnabled(mall, env = process.env) {
  if (!EXECUTABLE_MALLS.includes(mall)) {
    return { enabled: false, reason: `${mall} はこのバージョンからは更新できません (送信経路がありません)` };
  }
  const key = { rakuten: 'PRICE_UPDATE_RAKUTEN_ENABLED' }[mall];
  const on = TRUE_VALUES.has(String(env[key] ?? '').trim().toLowerCase());
  return on ? { enabled: true, reason: null } : { enabled: false, reason: `${key} が有効でないため送信しません (fail-closed)` };
}

/** run の operation 行 → ガード評価にかける形 */
function rowOf(op, isRecovery = false) {
  return {
    mall: op.mall,
    confidence: op.confidence,
    currentPrice: op.expected_current_price,
    newPrice: op.new_price,
    cost: op.cost_excl_tax,
    taxRate: op.tax_rate,
    shipping: op.shipping_cost,
    feeRate: op.fee_rate,
    // ★復旧 run は変更率ガードを免除する。ここで渡し忘れると、作成時は通った復旧行が
    //   送信直前に「値下げ幅が大きすぎます」で全部止まる (値上げを戻すと必ず大きな値下げになる)
    isRecovery,
  };
}

/**
 * 同じ商品 (manageNumber) の SKU をまとめる。楽天は1商品1リクエストで複数SKUを送れる (M0実測)。
 * ★まとめ送りの冪等キーは先頭行の operation_id を使う。miniPC 側は
 *   「その ID + 商品 + expected/prices 一式」のハッシュで同一性を見るので、
 *   まとめた内容がそのまま冪等キーの一部になる (別の組み合わせは別の依頼として扱われる)。
 */
export function groupOperations(ops) {
  const groups = new Map();
  for (const op of ops) {
    const key = JSON.stringify([op.mall, op.listing_code]);
    if (!groups.has(key)) groups.set(key, { mall: op.mall, listingCode: op.listing_code, ops: [] });
    groups.get(key).ops.push(op);
  }
  return [...groups.values()];
}

/**
 * miniPC の応答を状態に落とす。
 *   applied / noop … 成功
 *   conflict       … 更新前価格が違った (送っていない)。意味が分かる失敗
 *   failed         … ガード・入力で弾かれた。意味が分かる失敗
 *   unknown        … 送られたか分からない (結果不明・5xx・応答なし)
 *   unexpected     … 想定していない応答。成功扱いにせず、その場で止める
 */
export function classify(res) {
  const s = res?.status;
  const body = res?.body || {};
  const state = body.state;
  const error = body.error;

  if (s === 200 && (state === 'applied' || state === 'noop')) return state;
  if (s === 409 && (state === 'conflict' || error === 'CONFLICT')) return 'conflict';
  if (s === 409 && error === 'OPERATION_RESULT_UNKNOWN') return 'unknown';
  // ID の使い回しは「こちらの採番が壊れている」合図。続けず止める
  if (s === 409 && error === 'OPERATION_ID_REUSED') return 'unexpected';
  // 意味の分かる失敗だけを「続行してよい失敗」にする
  if (s === 400 && typeof error === 'string' && error !== '') return 'failed';
  if (s === 404 && error === 'ITEM_NOT_FOUND') return 'failed';
  // 送られたか分からない類 (miniPC 側で落ちた・応答が返らない)
  if (s === 502 || s === 503 || s === 504 || s == null) return 'unknown';
  // それ以外 (未知の200・401・403・素の404・500 など) は想定外。止める
  return 'unexpected';
}

/** その場で送信をやめるべき状態か */
function isStopState(state) {
  return state === 'unknown' || state === 'unexpected';
}

/**
 * run を実行する。
 *
 * @param {object} db
 * @param {object} run getRun() の戻り
 * @param {object} opts
 * @param {string} opts.actor
 * @param {object} opts.client { patchItemPrices, fetchItemDetail } (テストでは差し替える)
 * @param {object} [opts.env]
 * @param {boolean} [opts.skipClaim] テスト用: claim を取らない
 * @returns {Promise<{summary:object, results:Array}>}
 */
export async function executeRun(db, run, { actor, client, env = process.env, skipClaim = false }) {
  const results = [];
  const summary = { sent: 0, applied: 0, noop: 0, conflict: 0, failed: 0, unknown: 0, skipped: 0, stopped: null };

  // ★同じ run を2つの要求が同時に実行しないよう、DB で claim を取る。
  //   取れなかった = 既に誰か (別タブ・別インスタンス) が実行している/した
  if (!skipClaim) {
    const claim = claimRun(db, run.run_id, actor);
    if (!claim.acquired) {
      const e = new Error(`この run は既に実行されています (${claim.claimedBy} / ${claim.claimedAt})。`
        + '同じ内容をもう一度送ることはしません。結果は履歴で確認してください');
      e.code = 'ALREADY_EXECUTED';
      throw e;
    }
  }

  // 実行対象 = previewed の行だけ。blocked_preview / manual_* は対象外
  const targets = run.operations.filter((o) => o.state === 'previewed');
  if (targets.length === 0) {
    summary.stopped = '実行できる行がありません (記録時にガードを通った行だけが対象です)';
    return { summary, results };
  }

  const groups = groupOperations(targets);
  const failStreak = new Map();     // mall → 連続失敗数
  const stoppedMalls = new Map();   // mall → 止めた理由
  const trialDone = new Set();      // 試運転が済んだモール

  const record = (op, state, reason, detail = {}) => {
    results.push({ operationId: op.operation_id, state, reason: reason ?? null });
    appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: state, detail: { reason: reason ?? null, ...detail } });
  };

  for (const group of groups) {
    const { mall, listingCode, ops } = group;

    if (stoppedMalls.has(mall)) {
      for (const op of ops) { summary.skipped++; record(op, 'skipped', stoppedMalls.get(mall)); }
      continue;
    }

    const gate = mallWriteEnabled(mall, env);
    if (!gate.enabled) {
      stoppedMalls.set(mall, gate.reason);
      for (const op of ops) { summary.skipped++; record(op, 'skipped', gate.reason); }
      continue;
    }

    // ★送る直前にもう一度ガードを評価する。記録時に通っていても、ここで通らなければ送らない
    const isRecovery = run.kind === 'recovery';
    const blocked = [];
    for (const op of ops) {
      const ev = evaluateRow(rowOf(op, isRecovery));
      if (!ev.canUpdate) blocked.push({ op, blocks: ev.blocks });
    }
    if (blocked.length > 0) {
      for (const b of blocked) { summary.skipped++; record(b.op, 'blocked', b.blocks.join(' / '), { blocks: b.blocks }); }
      // 同じ商品の他のSKUも送らない (部分的に送って中途半端な状態にしない)
      for (const op of ops.filter((o) => !blocked.some((b) => b.op.operation_id === o.operation_id))) {
        summary.skipped++;
        record(op, 'skipped', '同じ商品にガードで止まった行があります');
      }
      continue;
    }

    const expected = {};
    const prices = {};
    for (const op of ops) {
      expected[op.sku_code] = op.expected_current_price;
      prices[op.sku_code] = op.new_price;
    }
    const operationId = ops[0].operation_id;

    for (const op of ops) appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: 'executing' });
    summary.sent++;

    let res;
    try {
      res = await client.patchItemPrices(listingCode, { operationId, runId: run.run_id, expected, prices });
    } catch (e) {
      // 応答が返ってこなかった = 送られたか不明。★再送しない
      const reason = `送信結果が不明です (${e.message})。受領台帳を operation_id=${operationId} で照会してください`;
      // ★送信が届いたかどうか分からない = モールの価格が変わっているかもしれない。復旧の対象にする印
      for (const op of ops) { summary.unknown++; record(op, 'unknown', reason, { operationId, mayHaveChanged: true }); }
      stoppedMalls.set(mall, '送信結果が不明な行が出たため、残りを止めました');
      summary.stopped = stoppedMalls.get(mall);
      continue;
    }

    const state = classify(res);

    if (state === 'applied' || state === 'noop') {
      failStreak.set(mall, 0);
      // ★API再取得で本当にその価格になっているか確かめる (要件 F5 一次確認)。
      //   noop も確認する — 確かめずに「既に同価格だった」と確定させない (Codex R1 Medium)
      const verified = await verifyPrices(client, listingCode, prices);
      if (verified.ok) {
        for (const op of ops) {
          if (state === 'noop') { summary.noop++; record(op, 'noop', null, { live: verified.live?.[op.sku_code] ?? null }); }
          else { summary.applied++; record(op, 'confirmed', null, { sent: prices[op.sku_code], verified: verified.live?.[op.sku_code] ?? null }); }
        }
        // ★試運転が済んだ = 「実際に書き換えて、その通りになったことを確かめられた」1件があること。
        //   noop は楽天へ書き込んでいないので、書き込みが通る証拠にならない (Codex R2 Medium)
        if (state === 'applied') trialDone.add(mall);
        continue;
      }
      // 確認できなかった: 一致しない = failed / 取り直せない = unknown
      const outState = verified.reachable ? 'failed' : 'unknown';
      for (const op of ops) {
        summary[outState]++;
        // ★miniPC は「送った」と言っている。照合が通らなかっただけなので、価格は変わっているかもしれない
        record(op, outState, verified.reason,
          { sent: prices[op.sku_code], verified: verified.live?.[op.sku_code] ?? null, mayHaveChanged: true });
      }
      stoppedMalls.set(mall, `送った後の確認が通りませんでした (${verified.reason})`);
      summary.stopped = stoppedMalls.get(mall);
      continue;
    }

    // 失敗 (conflict / ガード / 想定外 / 結果不明)
    const reason = res.body?.message || res.body?.error || `HTTP ${res.status}`;
    const bucket = state === 'conflict' ? 'conflict' : (isStopState(state) ? 'unknown' : 'failed');
    // ★意味の分かる失敗 (理由つき400 / 商品が無い / 価格の食い違い) は送信前に弾かれている = 価格は変わっていない。
    //   想定外・結果不明は、届いて適用された可能性が残る
    const mayHaveChanged = isStopState(state);
    for (const op of ops) {
      summary[bucket]++;
      record(op, state === 'unexpected' ? 'unknown' : state, reason,
        { httpStatus: res.status, classified: state, mayHaveChanged });
    }

    if (isStopState(state)) {
      // ★想定外・結果不明は「意味の分かる失敗」ではない。試運転の前後にかかわらず即停止
      stoppedMalls.set(mall, state === 'unknown'
        ? '送信結果が不明な行が出たため、残りを止めました'
        : `想定していない応答 (HTTP ${res.status}) が返ったため、残りを止めました`);
    } else {
      const streak = (failStreak.get(mall) || 0) + 1;
      failStreak.set(mall, streak);
      if (!trialDone.has(mall)) {
        stoppedMalls.set(mall, '試運転 (実際に価格が変わったことの確認) が済む前に失敗したため、残りを送っていません');
      } else if (streak >= BREAKER_CONSECUTIVE_FAILURES) {
        stoppedMalls.set(mall, `${BREAKER_CONSECUTIVE_FAILURES} 件続けて失敗したため、残りを止めました`);
      }
    }
    if (stoppedMalls.has(mall)) summary.stopped = stoppedMalls.get(mall);
  }

  appendEvent(db, run.run_id, { actor, event: 'run_executed', detail: summary });
  return { summary, results };
}

/**
 * 送った後に取り直して、本当にその価格になっているか確かめる。
 * @returns {{ok:boolean, reachable:boolean, reason:string|null, live:object|null}}
 *   reachable=false は「取り直せなかった」= 価格が変わったかどうかも分からない
 */
async function verifyPrices(client, manageNumber, prices) {
  try {
    const got = await client.fetchItemDetail(manageNumber);
    const item = got?.item;
    if (!item) return { ok: false, reachable: false, reason: '確認のための再取得ができませんでした', live: null };
    const live = {};
    const mismatched = [];
    for (const [sku, want] of Object.entries(prices)) {
      const v = item.variants?.[sku]?.standardPrice;
      const n = typeof v === 'string' ? (/^\d+$/.test(v.trim()) ? Number(v.trim()) : null)
        : (Number.isInteger(v) ? v : null);
      live[sku] = n;
      if (n !== want) mismatched.push(`${sku}: 期待 ${want} / 実際 ${n ?? '読めません'}`);
    }
    if (mismatched.length > 0) return { ok: false, reachable: true, reason: mismatched.join(', '), live };
    return { ok: true, reachable: true, reason: null, live };
  } catch (e) {
    return { ok: false, reachable: false, reason: `確認のための再取得に失敗 (${e.message})`, live: null };
  }
}
