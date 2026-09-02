/**
 * price-update / 実行フロー (要件定義 v1.0 F4・M2)
 *
 * 記録した run の中から「更新できる行」だけを、モールへ順番に送る。
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
import { EXECUTABLE_MALLS, ITEM_PRICE_MALLS, killSwitchKeyOf } from './mall-capabilities.js';

/** 連続でこの回数失敗したら、そのモールの残りを止める */
export const BREAKER_CONSECUTIVE_FAILURES = 2;

/**
 * 実際に送れるモール (楽天 / Yahoo / au PAY)。
 * ★正は mall-capabilities.js。ここでは読み直して公開するだけ。
 *   同じ事実を何か所にも書くと、モールを増やした時に片方だけ直し忘れる
 *   (2026-09-02: au PAY を足した時に pricing.js を直し忘れ、
 *    画面では選べるのに送信の手前で必ず弾かれる状態になった)。
 * ★送り先のクライアントが無いモールをここに入れてはいけない。
 *   入れると「別のモールの出品コードを送る」ことになる (Codex R1 High)
 */
export { EXECUTABLE_MALLS };

const TRUE_VALUES = new Set(['1', 'true', 'on', 'yes']);

/**
 * モール別 kill switch。**明示的に有効でなければ送らない** (env 未設定 = 停止)。
 * @returns {{enabled:boolean, reason:string|null}}
 */
export function mallWriteEnabled(mall, env = process.env) {
  if (!EXECUTABLE_MALLS.includes(mall)) {
    return { enabled: false, reason: `${mall} はこのバージョンからは更新できません (送信経路がありません)` };
  }
  const key = killSwitchKeyOf(mall);
  // ★スイッチの名前を決め忘れたモールは送らない。ここが undefined のまま進むと
  //   「undefined が有効でないため」という読めない理由になる
  if (!key) return { enabled: false, reason: `${mall} の送信スイッチが決まっていません (fail-closed)` };
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
    // ★大文字小文字を無視してまとめる。生の値のままだと、同じ商品が 'kara-1' と 'KARA-1' の
    //   2グループに割れて、同じ商品へ2回送ってしまう
    //   (1回目で価格が変わり、2回目は楽観ロックで conflict → 同じ指示なのに片方だけ失敗として残る)
    const key = JSON.stringify([op.mall, String(op.listing_code ?? '').trim().toLowerCase()]);
    // 送る時に使う出品コードは先頭行の生の値 (モールが返してきた表記)
    if (!groups.has(key)) groups.set(key, { mall: op.mall, listingCode: op.listing_code, ops: [] });
    groups.get(key).ops.push(op);
  }
  return [...groups.values()];
}

/**
 * **実際にモールへ書き込む単位** を表すキー。グループ分け (mall + listing_code) とは別物。
 *
 * ★Yahoo と au PAY は商品に1つの価格しか持たない。色は商品価格を継承するので、
 *   色が違っても書き込む先は同じ商品になる。
 *   出品コードの表記ゆれ (大文字小文字) で別グループに割れても、同じ商品なら同じ送り先として見る
 *   — でないと「同じ商品へ違う価格を続けて送る」を素通りさせる (Codex R1 高)。
 * ★楽天は SKU (variant) ごとに価格を持つので、SKU まで含めて1つの送り先。
 */
export function sendKeyOf(op) {
  const n = (v) => String(v ?? '').trim().toLowerCase();
  // ★Yahoo と au PAY は商品に1つの価格。楽天は SKU (variant) ごとに価格を持つ
  return ITEM_PRICE_MALLS.has(op.mall)
    ? [op.mall, n(op.listing_code)].join('|')
    : [op.mall, n(op.listing_code), n(op.sku_code)].join('|');
}

/**
 * 送信前の一括検証: **同じ送り先に、決められない指示が来ていないか**。
 *
 * ・新売価が違う → どちらを送るか決められない
 * ・記録した時の価格が違う → 引き当てかキャッシュが食い違っている。楽観ロックの基準を選べない
 *
 * どちらも「後から処理した行が黙って勝つ」形になるので、送る前に止める。
 * @param {Array<object>} ops 実行対象の operation
 * @returns {Map<string,string>} 送り先キー → 止める理由
 */
export function findSendConflicts(ops) {
  // ★送り先ごとに **全行を集めてから** 判定する。見つけた順に決めると、
  //   先に軽い食い違い (表記ゆれ) を拾った時点で以降を見なくなり、
  //   後ろにある「違う新売価」が隠れる (Codex R3)
  const byKey = new Map();
  for (const op of ops) {
    const k = sendKeyOf(op);
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k).push(op);
  }

  const conflicts = new Map();
  for (const [k, rows] of byKey) {
    if (rows.length < 2) continue;
    const uniq = (vals) => [...new Set(vals)];

    // 1. 送る値が決まらない — いちばん重い
    const newPrices = uniq(rows.map((o) => o.new_price));
    if (newPrices.length > 1) {
      conflicts.set(k, `同じ送り先に違う新売価が指定されています (${newPrices.map((v) => `${v} 円`).join(" と ")})。`
        + "どちらを送るか決められないため送りません");
      continue;
    }
    // 2. 楽観ロックの基準が決まらない
    const expects = uniq(rows.map((o) => o.expected_current_price));
    if (expects.length > 1) {
      conflicts.set(k, `同じ送り先なのに、記録した時の価格が行ごとに違います `
        + `(${expects.map((v) => `${v} 円`).join(" と ")})。引き当てが食い違っているため送りません`);
      continue;
    }
    // 3. 出品コードの書き方が揃っていない。
    //    引き当てはモールが返した表記をそのまま使うので、ここが揺れるのは記録が壊れている合図。
    //    まとめ送りのキーが2つに割れて「1商品に2つの価格」として弾かれるため、分かる理由で先に止める
    const codes = uniq(rows.map((o) => `${o.listing_code} / ${o.sku_code}`));
    if (codes.length > 1) {
      conflicts.set(k, `同じ商品を指す行で、出品コードの書き方が揃っていません (${codes.join(" と ")})。`
        + "取り違えを避けるため送りません");
    }
  }
  return conflicts;
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
 * @param {object} opts.clients モール別のクライアント { rakuten: {...}, yahoo: {...} }。
 *   どのモールも { patchItemPrices, fetchItemDetail } の同じ形にそろえてある
 *   (モールごとに判定を作らず、classify() / 試運転 / ブレーカー を共通に効かせるため)。
 *   テストでは差し替える
 * @param {object} [opts.env]
 * @param {boolean} [opts.skipClaim] テスト用: claim を取らない
 * @returns {Promise<{summary:object, results:Array}>}
 */
export async function executeRun(db, run, { actor, clients, client, env = process.env, skipClaim = false }) {
  // client 単体で渡されたら楽天のものとして扱う (既存の呼び出し・テストとの互換)
  const clientOf = (mall) => (clients ? clients[mall] : (mall === 'rakuten' ? client : null));
  const results = [];
  const conflictBlocked = [];   // 送り先が重なって決められなかった行 (記録は record 定義後)
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

  // ★送る前に run 全体を見て、同じ送り先へ決められない指示が来ていないか調べる。
  //   グループ (mall + listing_code) ごとに見ると、表記ゆれで別グループに割れた
  //   同じ商品を取りこぼす。
  // ★これは kill switch やクライアント不在より **先** に記録する。
  //   「この run の中身が決められない」は送信可否とは別の事実で、
  //   kill switch を入れ直せば送れる、と読まれてはいけないため (Codex R2)
  const sendConflicts = findSendConflicts(targets);
  const sendable = [];
  for (const op of targets) {
    const reason = sendConflicts.get(sendKeyOf(op));
    if (reason) { summary.skipped++; conflictBlocked.push({ op, reason }); }
    else sendable.push(op);
  }

  const groups = groupOperations(sendable);
  const failStreak = new Map();     // mall → 連続失敗数
  const stoppedMalls = new Map();   // mall → 止めた理由
  const trialDone = new Set();      // 試運転が済んだモール

  const record = (op, state, reason, detail = {}) => {
    results.push({ operationId: op.operation_id, state, reason: reason ?? null });
    appendEvent(db, run.run_id, { operationId: op.operation_id, actor, event: state, detail: { reason: reason ?? null, ...detail } });
  };

  for (const { op, reason } of conflictBlocked) record(op, 'blocked', reason, { blocks: [reason] });

  for (const group of groups) {
    const { mall, listingCode, ops } = group;

    if (stoppedMalls.has(mall)) {
      for (const op of ops) { summary.skipped++; record(op, 'skipped', stoppedMalls.get(mall)); }
      continue;
    }

    const mallClient = clientOf(mall);
    if (!mallClient) {
      const reason = `${mall} への送信口がありません`;
      stoppedMalls.set(mall, reason);
      for (const op of ops) { summary.skipped++; record(op, 'skipped', reason); }
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
      res = await mallClient.patchItemPrices(listingCode, { operationId, runId: run.run_id, expected, prices });
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
      const verified = await verifyPrices(mallClient, listingCode, prices);
      if (verified.ok) {
        for (const op of ops) {
          if (state === 'noop') { summary.noop++; record(op, 'noop', null, { live: verified.live?.[op.sku_code] ?? null }); }
          else {
            summary.applied++;
            // ★Yahoo は「管理側が変わった」だけでは客に見えない。反映を依頼できたかも残す
            //   (反映そのものは非同期。ここでは終わらないので、後から確かめる)
            const publish = res.body?.publish;
            record(op, 'confirmed', null, {
              sent: prices[op.sku_code], verified: verified.live?.[op.sku_code] ?? null,
              ...(publish ? { publishRequested: publish.requested === true, publishOk: publish.ok === true } : {}),
            });
          }
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
      // ★失敗でも「価格は変わった」と分かっているなら、それを記録に残す (Codex R6)。
      //   Yahoo の「更新は通ったが反映できていない」がこれ。残さないと復旧の対象から漏れる
      const sentPrice = res.body?.applied?.[op.sku_code];
      record(op, state === 'unexpected' ? 'unknown' : state, reason, {
        httpStatus: res.status, classified: state, mayHaveChanged,
        ...(sentPrice !== undefined ? { applied: sentPrice, mayHaveChanged: true } : {}),
        ...(res.body?.publish ? { publishRequested: res.body.publish.requested === true, publishOk: res.body.publish.ok === true } : {}),
      });
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
