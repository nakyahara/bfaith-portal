/**
 * price-update / 実行フロー (要件定義 v1.0 F4・M2)
 *
 * 記録した run の中から「更新できる行」だけを、楽天へ順番に送る。
 *
 * ★ここが守ること (どれも「事故を小さく止める」ためのもの):
 *   ・実行するのは**保存済みの run**の内容だけ。画面から価格を再受領しない
 *     (画面を書き換えて別の値を送る、という抜け道を作らない)
 *   ・送る前にサーバ側でガードを**もう一度**評価する (記録時に通っていても、ここで通らなければ送らない)
 *   ・モール別の kill switch。env が明示的に有効でなければ 1件も送らない (fail-closed)
 *   ・**試運転**: 最初の1件だけ送って、API再取得で価格が変わったことを確かめてから残りを続ける
 *   ・**サーキットブレーカー**: 連続2件失敗、または想定外の応答が来たら残りを全部止める
 *   ・1件ずつ直列。並行して送らない (レート制御と、止めるべき時に止まれること優先)
 *   ・状態はすべてイベントとして追記する (pu_events)。行は書き換えない
 *
 * 楽観ロック (更新前価格の一致確認) と冪等 (operation_id) は miniPC 側が持つ。
 * ここは「何をどの順で送り、どこで止めるか」を決める。
 */
import { appendEvent } from './db.js';
import { evaluateRow } from './pricing.js';

/** 連続でこの回数失敗したら、そのモールの残りを止める */
export const BREAKER_CONSECUTIVE_FAILURES = 2;

const TRUE_VALUES = new Set(['1', 'true', 'on', 'yes']);

/**
 * モール別 kill switch。**明示的に有効でなければ送らない** (env 未設定 = 停止)。
 * @returns {{enabled:boolean, reason:string|null}}
 */
export function mallWriteEnabled(mall, env = process.env) {
  const key = { rakuten: 'PRICE_UPDATE_RAKUTEN_ENABLED', yahoo: 'PRICE_UPDATE_YAHOO_ENABLED' }[mall];
  if (!key) return { enabled: false, reason: `${mall} はこのツールから更新できません` };
  const on = TRUE_VALUES.has(String(env[key] ?? '').trim().toLowerCase());
  return on ? { enabled: true, reason: null } : { enabled: false, reason: `${key} が有効でないため送信しません (fail-closed)` };
}

/** run の operation 行 → ガード評価にかける形 */
function rowOf(op) {
  return {
    mall: op.mall,
    confidence: op.confidence,
    currentPrice: op.expected_current_price,
    newPrice: op.new_price,
    cost: op.cost_excl_tax,
    taxRate: op.tax_rate,
    shipping: op.shipping_cost,
    feeRate: op.fee_rate,
    isRecovery: false,
  };
}

/**
 * 同じ商品 (manageNumber) の SKU をまとめる。楽天は1商品1リクエストで複数SKUを送れる
 * (M0実測)。まとめた方がレート制御にも優しい。
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
 * run を実行する。
 *
 * @param {object} db
 * @param {object} run getRun() の戻り
 * @param {object} opts
 * @param {string} opts.actor
 * @param {object} opts.client { patchItemPrices, fetchItemDetail } (テストでは差し替える)
 * @param {object} [opts.env]
 * @returns {Promise<{summary:object, results:Array}>}
 */
export async function executeRun(db, run, { actor, client, env = process.env }) {
  const results = [];
  const summary = { sent: 0, applied: 0, noop: 0, conflict: 0, failed: 0, unknown: 0, skipped: 0, stopped: null };

  // 実行対象 = previewed の行だけ。blocked_preview / manual_* は対象外
  const targets = run.operations.filter((o) => o.state === 'previewed');
  if (targets.length === 0) {
    summary.stopped = '実行できる行がありません (記録時にガードを通った行だけが対象です)';
    return { summary, results };
  }

  const groups = groupOperations(targets);
  const failStreak = new Map();     // mall → 連続失敗数
  const stoppedMalls = new Map();   // mall → 止めた理由
  let trialDone = new Set();        // 試運転が済んだモール

  for (const group of groups) {
    const { mall, listingCode, ops } = group;

    if (stoppedMalls.has(mall)) {
      for (const op of ops) {
        summary.skipped++;
        results.push({ operationId: op.operation_id, state: 'skipped', reason: stoppedMalls.get(mall) });
        appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: 'skipped', detail: { reason: stoppedMalls.get(mall) } });
      }
      continue;
    }

    const gate = mallWriteEnabled(mall, env);
    if (!gate.enabled) {
      stoppedMalls.set(mall, gate.reason);
      for (const op of ops) {
        summary.skipped++;
        results.push({ operationId: op.operation_id, state: 'skipped', reason: gate.reason });
        appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: 'skipped', detail: { reason: gate.reason } });
      }
      continue;
    }

    // ★送る直前にもう一度ガードを評価する。記録時に通っていても、ここで通らなければ送らない
    const blocked = [];
    for (const op of ops) {
      const ev = evaluateRow(rowOf(op));
      if (!ev.canUpdate) blocked.push({ op, blocks: ev.blocks });
    }
    if (blocked.length > 0) {
      for (const b of blocked) {
        summary.skipped++;
        results.push({ operationId: b.op.operation_id, state: 'blocked', reason: b.blocks.join(' / ') });
        appendEvent(db, run.run_id, { operationId: b.op.operation_id, actor, event: 'blocked', detail: { blocks: b.blocks } });
      }
      // 同じ商品の他のSKUも送らない (部分的に送って中途半端な状態にしない)
      for (const op of ops.filter((o) => !blocked.some((b) => b.op.operation_id === o.operation_id))) {
        summary.skipped++;
        results.push({ operationId: op.operation_id, state: 'skipped', reason: '同じ商品にガードで止まった行があります' });
        appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: 'skipped', detail: { reason: 'same_item_blocked' } });
      }
      continue;
    }

    const expected = {};
    const prices = {};
    for (const op of ops) {
      expected[op.sku_code] = op.expected_current_price;
      prices[op.sku_code] = op.new_price;
    }
    // operation_id は行ごとに採ってあるので、まとめ送りの代表として先頭を使う。
    // 同じ内容の再送は miniPC が実行しない (冪等)
    const operationId = ops[0].operation_id;

    for (const op of ops) appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: 'executing' });
    summary.sent++;

    let res;
    try {
      res = await client.patchItemPrices(listingCode, { operationId, runId: run.run_id, expected, prices });
    } catch (e) {
      // 応答が返ってこなかった = 送られたか不明。★再送しない
      const reason = `送信結果が不明です (${e.message})。受領台帳を operation_id=${operationId} で照会してください`;
      for (const op of ops) {
        summary.unknown++;
        results.push({ operationId: op.operation_id, state: 'unknown', reason });
        appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: 'unknown', detail: { reason, operationId } });
      }
      stoppedMalls.set(mall, '送信結果が不明な行が出たため、残りを止めました');
      summary.stopped = stoppedMalls.get(mall);
      // ★break ではなく continue。残りの行も「送らなかった」として記録に残す
      continue;
    }

    const state = classify(res);
    if (state === 'applied' || state === 'noop') {
      failStreak.set(mall, 0);
      // ★API再取得で本当に変わったか確かめる (要件 F5 一次確認)
      const verified = await verifyPrices(client, listingCode, prices);
      for (const op of ops) {
        const okRow = verified.ok || state === 'noop';
        summary[okRow ? (state === 'noop' ? 'noop' : 'applied') : 'failed']++;
        results.push({
          operationId: op.operation_id,
          state: okRow ? (state === 'noop' ? 'noop' : 'confirmed') : 'failed',
          reason: okRow ? null : verified.reason,
        });
        appendEvent(db, run.run_id, {
          operationId: op.operation_id, actor,
          event: okRow ? (state === 'noop' ? 'noop' : 'confirmed') : 'failed',
          detail: { sent: prices[op.sku_code], verified: verified.live?.[op.sku_code] ?? null, reason: verified.reason ?? null },
        });
      }
      if (!verified.ok) {
        stoppedMalls.set(mall, `送った後の確認で価格が一致しませんでした (${verified.reason})`);
        summary.stopped = stoppedMalls.get(mall);
        continue;
      }
      trialDone.add(mall);   // 試運転OK。以降は続けてよい
      continue;
    }

    // 失敗 (conflict / ガード / 楽天側エラー / 結果不明)
    const reason = res.body?.message || res.body?.error || `HTTP ${res.status}`;
    for (const op of ops) {
      summary[state === 'conflict' ? 'conflict' : (state === 'unknown' ? 'unknown' : 'failed')]++;
      results.push({ operationId: op.operation_id, state, reason });
      appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: state, detail: { reason, httpStatus: res.status } });
    }

    const streak = (failStreak.get(mall) || 0) + 1;
    failStreak.set(mall, streak);
    if (!trialDone.has(mall)) {
      stoppedMalls.set(mall, '試運転の1件目が失敗したため、残りを送っていません');
    } else if (streak >= BREAKER_CONSECUTIVE_FAILURES) {
      stoppedMalls.set(mall, `${BREAKER_CONSECUTIVE_FAILURES} 件続けて失敗したため、残りを止めました`);
    } else if (state === 'unknown') {
      stoppedMalls.set(mall, '送信結果が不明な行が出たため、残りを止めました');
    }
    if (stoppedMalls.has(mall)) summary.stopped = stoppedMalls.get(mall);
  }

  appendEvent(db, run.run_id, { actor, event: 'run_executed', detail: summary });
  return { summary, results };
}

/** miniPC の応答を状態に落とす */
export function classify(res) {
  const s = res?.status;
  const state = res?.body?.state;
  if (s === 200 && (state === 'applied' || state === 'noop')) return state;
  if (s === 409 && (state === 'conflict' || res?.body?.error === 'CONFLICT')) return 'conflict';
  if (s === 409 && res?.body?.error === 'OPERATION_RESULT_UNKNOWN') return 'unknown';
  if (s === 409 && res?.body?.error === 'OPERATION_ID_REUSED') return 'failed';
  if (s === 400) return 'failed';
  // 想定していない応答は「不明」に倒す (成功扱いにしない)
  if (s === 502 || s === 503 || s == null) return 'unknown';
  return 'failed';
}

/** 送った後に取り直して、本当にその価格になっているか確かめる */
async function verifyPrices(client, manageNumber, prices) {
  try {
    const got = await client.fetchItemDetail(manageNumber);
    const item = got?.item;
    if (!item) return { ok: false, reason: '確認のための再取得ができませんでした', live: null };
    const live = {};
    const mismatched = [];
    for (const [sku, want] of Object.entries(prices)) {
      const v = item.variants?.[sku]?.standardPrice;
      const n = typeof v === 'string' ? (/^\d+$/.test(v.trim()) ? Number(v.trim()) : null)
        : (Number.isInteger(v) ? v : null);
      live[sku] = n;
      if (n !== want) mismatched.push(`${sku}: 期待 ${want} / 実際 ${n ?? '読めません'}`);
    }
    if (mismatched.length > 0) return { ok: false, reason: mismatched.join(', '), live };
    return { ok: true, reason: null, live };
  } catch (e) {
    return { ok: false, reason: `確認のための再取得に失敗 (${e.message})`, live: null };
  }
}
