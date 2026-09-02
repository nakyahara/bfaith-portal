/**
 * test-execute.mjs — 実行フローの検証 (要件 F4・M2)
 *
 * 楽天へは接続せず、miniPC クライアントを差し替えて「どこで止まるか」を確かめる。
 * ここが緩むと、1件の失敗に気づかないまま残り全部を送ってしまう。
 *
 * 実行: node apps/price-update/test-execute.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { createTables, insertRun, getRun } from './db.js';
import { executeRun, mallWriteEnabled, classify, groupOperations, findSendConflicts, sendKeyOf,
  BREAKER_CONSECUTIVE_FAILURES } from './execute.js';
import { executorGate } from './router.js';
import { MALL_CAPABILITIES, findCapabilityProblems, killSwitchKeyOf,
  ITEM_PRICE_MALLS, UPDATABLE_MALLS } from './mall-capabilities.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pu-exec-'));
const db = createTables(new Database(path.join(tmpDir, 'm.db')));
const ENV_ON = { PRICE_UPDATE_RAKUTEN_ENABLED: '1' };

/** 更新できる行 (ガードを通る値) を n 商品ぶん作る */
function makeRun(n, { newPrice = 620 } = {}) {
  const operations = [];
  for (let i = 0; i < n; i++) {
    operations.push({
      operationId: `puo-${i}-${Math.random().toString(36).slice(2, 8)}`,
      mall: 'rakuten', neCode: `ne-${i}`, rowKind: 'single',
      listingCode: `mn-${i}`, skuCode: `sku-${i}`,
      confidence: 'confirmed', expectedCurrentPrice: 577, newPrice,
      cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12,
      initialState: 'previewed',
    });
  }
  const runId = insertRun(db, { createdBy: 't@example.com', neCodes: ['ne'], limits: {}, operations });
  return getRun(db, runId);
}

/** 送信結果を指定できるクライアント */
function makeClient(plan) {
  const calls = [];
  return {
    calls,
    patchItemPrices: async (mn, body) => {
      calls.push({ mn, body });
      const r = plan.shift();
      if (!r) return { status: 200, body: { ok: true, state: 'applied', applied: body.prices } };
      if (r.throw) throw new Error(r.throw);
      return r;
    },
    fetchItemDetail: async (mn) => {
      // 直前に送った価格になっている、として返す (verify を通す)
      const last = [...calls].reverse().find((c) => c.mn === mn);
      const variants = {};
      for (const [sku, price] of Object.entries(last?.body?.prices || {})) {
        variants[sku] = { standardPrice: String(plan.verifyAs ?? price) };
      }
      return { item: { manageNumber: mn, variants }, status: 'found' };
    },
  };
}

console.log('\n── モール別 kill switch (明示的に有効でなければ送らない) ──');
{
  eq(mallWriteEnabled('rakuten', {}), { enabled: false, reason: 'PRICE_UPDATE_RAKUTEN_ENABLED が有効でないため送信しません (fail-closed)' },
    '★env 未設定なら送らない');
  eq(mallWriteEnabled('rakuten', { PRICE_UPDATE_RAKUTEN_ENABLED: '1' }).enabled, true, '1 で有効');
  eq(mallWriteEnabled('rakuten', { PRICE_UPDATE_RAKUTEN_ENABLED: 'false' }).enabled, false, 'false は無効');
  eq(mallWriteEnabled('amazon', ENV_ON).enabled, false, 'Amazon はそもそも更新できない');

  const run = makeRun(2);
  const client = makeClient([]);
  const { summary } = await executeRun(db, run, { actor: 't', client, env: {} });
  eq([summary.sent, summary.skipped], [0, 2], '★kill switch が無効なら1件も送らない');
  eq(client.calls.length, 0, '楽天を1度も叩いていない');
}

console.log('\n── 実行できる人 (名簿がすべて) ──');
{
  const withEnv = (v, session) => {
    const before = process.env.PRICE_UPDATE_EXECUTORS;
    if (v === null) delete process.env.PRICE_UPDATE_EXECUTORS; else process.env.PRICE_UPDATE_EXECUTORS = v;
    try { return executorGate({ session }); } finally {
      if (before === undefined) delete process.env.PRICE_UPDATE_EXECUTORS; else process.env.PRICE_UPDATE_EXECUTORS = before;
    }
  };
  const admin = { role: 'admin', email: 'admin@example.com' };
  eq(withEnv(null, admin).ok, false, '★名簿が未設定なら admin でも実行できない (誰も実行できない)');
  ok(withEnv(null, admin).message.includes('PRICE_UPDATE_EXECUTORS'), '未設定だと分かる理由を返す');
  eq(withEnv('a@example.com', admin).ok, false, '★名簿に無ければ admin でも実行できない');
  eq(withEnv('a@example.com', { role: 'user', email: 'A@Example.com ' }).ok, true, '名簿にあれば実行できる (大小文字・空白は無視)');
  eq(withEnv('a@example.com , b@example.com', { role: 'user', email: 'b@example.com' }).ok, true, 'カンマ区切りで複数人');
  eq(withEnv('a@example.com', { role: 'user' }).ok, false, 'メールが分からない相手は実行できない');
}

console.log('\n── 応答の解釈 ──');
{
  eq(classify({ status: 200, body: { state: 'applied' } }), 'applied', '200 applied');
  eq(classify({ status: 200, body: { state: 'noop' } }), 'noop', '200 noop');
  eq(classify({ status: 409, body: { state: 'conflict' } }), 'conflict', '409 conflict');
  eq(classify({ status: 409, body: { error: 'OPERATION_RESULT_UNKNOWN' } }), 'unknown', '結果不明');
  eq(classify({ status: 409, body: { error: 'OPERATION_ID_REUSED' } }), 'unexpected', '★ID使い回しは想定外 (採番が壊れている合図なので止める)');
  eq(classify({ status: 400, body: { error: 'INVALID_PRICE' } }), 'failed', '意味の分かる 400 は失敗 (続行してよい)');
  eq(classify({ status: 400, body: {} }), 'unexpected', '★理由の分からない 400 は想定外');
  eq(classify({ status: 502, body: {} }), 'unknown', '★502 は「不明」に倒す (成功にしない)');
  eq(classify({ status: 200, body: { state: 'なにか' } }), 'unexpected', '★未知の200は成功にしない (止める)');
  eq(classify({ status: 500, body: {} }), 'unexpected', '500 は想定外');
  eq(classify({ status: 401, body: {} }), 'unexpected', '401 も想定外');
  eq(classify({ status: 404, body: { error: 'ITEM_NOT_FOUND' } }), 'failed', '商品が無いのは意味の分かる失敗');
}

console.log('\n── 試運転: 1件目が失敗したら残りを送らない ──');
{
  const run = makeRun(3);
  const client = makeClient([{ status: 409, body: { state: 'conflict', message: '価格が違います' } }]);
  const { summary, results } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(client.calls.length, 1, '★1件だけ送って止まる');
  eq([summary.conflict, summary.skipped], [1, 2], '1件 conflict / 残り2件は送っていない');
  ok(summary.stopped.includes('試運転'), '止めた理由が試運転だと分かる');
  eq(results.filter((r) => r.state === 'skipped').length, 2, '残りは skipped として記録');
}

console.log('\n── 試運転が通れば続ける ──');
{
  const run = makeRun(3);
  const client = makeClient([]);   // すべて applied
  const { summary } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq([client.calls.length, summary.applied], [3, 3], '3件とも送って確認できた');
  eq(summary.stopped, null, '止まっていない');
}

console.log('\n── noop は試運転の合格にしない (書き込んでいないため) ──');
{
  const run = makeRun(3);
  const client = makeClient([
    { status: 200, body: { state: 'noop' } },                            // 送ったが書き換えは起きていない
    { status: 400, body: { error: 'INVALID_PRICE', message: '不正な価格' } },
  ]);
  const { summary } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(client.calls.length, 2, '★noop の次で失敗したら、そこで止まる (ブレーカーの2件目まで待たない)');
  eq([summary.noop, summary.failed, summary.skipped], [1, 1, 1], 'noop 1 / 失敗 1 / 未送信 1');
  ok(summary.stopped.includes('試運転'), '★止めた理由は「試運転が済んでいない」');
}

console.log('\n── サーキットブレーカー: 連続2件失敗で残りを止める ──');
{
  const run = makeRun(5);
  const client = makeClient([
    { status: 200, body: { state: 'applied' } },                       // 試運転OK
    { status: 400, body: { error: 'INVALID_PRICE', message: 'だめ' } }, // 失敗1
    { status: 400, body: { error: 'INVALID_PRICE', message: 'だめ' } }, // 失敗2 → 止まる
  ]);
  const { summary } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(client.calls.length, 3, `★${BREAKER_CONSECUTIVE_FAILURES} 件続けて失敗した時点で送信をやめる`);
  eq([summary.applied, summary.failed, summary.skipped], [1, 2, 2], '1成功 / 2失敗 / 2未送信');
  ok(summary.stopped.includes('続けて失敗'), '止めた理由がブレーカーだと分かる');
}

console.log('\n── 送信結果が不明なら、そこで止めて再送しない ──');
{
  const run = makeRun(3);
  const client = makeClient([{ throw: 'socket hang up' }]);
  const { summary, results } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(client.calls.length, 1, '1件送って例外 → そこで止まる');
  eq(summary.unknown, 1, '★不明として記録 (成功にも失敗にもしない)');
  ok(results[0].reason.includes('受領台帳'), '照会先を案内する');
  eq(summary.skipped, 2, '残りは送らない');
}

console.log('\n── 送った後の確認で価格が違えば止める ──');
{
  const run = makeRun(3);
  const plan = [];
  plan.verifyAs = 999;   // 送った値と違う価格が返る
  const client = makeClient(plan);
  const { summary } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq([summary.applied, summary.failed], [0, 1], '★確認が通らなければ成功にしない');
  ok(summary.stopped.includes('確認'), '止めた理由が照合の不一致だと分かる');
  eq(client.calls.length, 1, '残りは送らない');
}

console.log('\n── 実行直前のガード ──');
{
  // 記録時は通っていても、送る直前にもう一度評価する
  const run = makeRun(1, { newPrice: 100 });   // 577 → 100 は −82% でガードに掛かる
  const client = makeClient([]);
  const { summary, results } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(client.calls.length, 0, '★ガードに掛かる行は送らない');
  eq(summary.skipped, 1, 'skipped として数える');
  ok(results[0].reason.includes('値下げ幅'), '理由が分かる');
}

console.log('\n── 同じ商品の SKU はまとめて1回で送る ──');
{
  const ops = [
    { operationId: 'a1', mall: 'rakuten', neCode: 'ne-1', rowKind: 'single', listingCode: 'mn-x', skuCode: 's1',
      confidence: 'confirmed', expectedCurrentPrice: 577, newPrice: 620, cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12, initialState: 'previewed' },
    { operationId: 'a2', mall: 'rakuten', neCode: 'ne-1', rowKind: 'single', listingCode: 'mn-x', skuCode: 's2',
      confidence: 'confirmed', expectedCurrentPrice: 577, newPrice: 630, cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12, initialState: 'previewed' },
  ];
  const runId = insertRun(db, { createdBy: 't', neCodes: ['ne-1'], limits: {}, operations: ops });
  const run = getRun(db, runId);
  eq(groupOperations(run.operations).length, 1, '同じ商品は1グループ');
  const client = makeClient([]);
  const { summary } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(client.calls.length, 1, '★1リクエストにまとめる');
  eq(client.calls[0].body.prices, { s1: 620, s2: 630 }, '両SKUを1回で送る');
  eq(summary.applied, 2, '2行とも成功として数える');
}

console.log('\n── 同じ run は一度しか実行できない (二重クリック・複数インスタンス) ──');
{
  const run = makeRun(2);
  const c1 = makeClient([]);
  const first = await executeRun(db, run, { actor: 'a@example.com', client: c1, env: ENV_ON });
  eq(first.summary.applied, 2, '1回目は実行できる');

  const c2 = makeClient([]);
  let err = null;
  try { await executeRun(db, getRun(db, run.run_id), { actor: 'b@example.com', client: c2, env: ENV_ON }); }
  catch (e) { err = e; }
  ok(err?.code === 'ALREADY_EXECUTED', '★2回目は claim が取れず実行されない');
  ok(String(err?.message).includes('a@example.com'), '誰が実行したか分かる');
  eq(c2.calls.length, 0, '★楽天を1度も叩いていない (二重更新にならない)');
}

console.log('\n── 想定外の応答は試運転後でも即停止 ──');
{
  const run = makeRun(4);
  const client = makeClient([
    { status: 200, body: { state: 'applied' } },   // 試運転OK
    { status: 500, body: { message: 'なにか' } },  // 想定外 → 即停止 (ブレーカーの2件を待たない)
  ]);
  const { summary } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(client.calls.length, 2, '★想定外が返った時点で送信をやめる');
  eq([summary.applied, summary.unknown, summary.skipped], [1, 1, 2], '1成功 / 1不明 / 2未送信');
  ok(summary.stopped.includes('想定していない応答'), '止めた理由が想定外だと分かる');
}

console.log('\n── モール別の送信口 (Yahoo の出品コードを楽天へ送らない) ──');
{
  const yahooOps = () => ([{
    operationId: 'y' + Math.random().toString(36).slice(2, 8), mall: 'yahoo', neCode: 'ne-y', rowKind: 'single',
    listingCode: 'yahoo-item', skuCode: 'yahoo-item',
    confidence: 'confirmed', expectedCurrentPrice: 577, newPrice: 620, cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.1,
    initialState: 'previewed',
  }]);
  const ENV_BOTH = { PRICE_UPDATE_RAKUTEN_ENABLED: '1', PRICE_UPDATE_YAHOO_ENABLED: '1' };

  // ★Yahoo の行は Yahoo の送信口へ。楽天のクライアントは一度も呼ばれない
  const rk = makeClient([]);
  const yh = makeClient([]);
  const runA = insertRun(db, { createdBy: 't', neCodes: ['ne-y'], limits: {}, operations: yahooOps() });
  const a = await executeRun(db, getRun(db, runA), {
    actor: 't', clients: { rakuten: rk, yahoo: yh }, env: ENV_BOTH,
  });
  eq(rk.calls.length, 0, '★Yahoo の出品コードを楽天の管理番号として送らない');
  eq([yh.calls.length, a.summary.applied], [1, 1], 'Yahoo の送信口に届いて更新できた');

  // ★Yahoo の送信口が無い版では送らずに skipped (勝手に楽天へ流さない)
  const rk2 = makeClient([]);
  const runB = insertRun(db, { createdBy: 't', neCodes: ['ne-y'], limits: {}, operations: yahooOps() });
  const b = await executeRun(db, getRun(db, runB), { actor: 't', clients: { rakuten: rk2 }, env: ENV_BOTH });
  eq([rk2.calls.length, b.summary.skipped], [0, 1], '★送信口が無ければ1件も送らない');
  ok(b.results[0].reason.includes('送信口がありません'), '理由が分かる: ' + b.results[0].reason);

  // kill switch はモールごと
  eq(mallWriteEnabled('yahoo', { PRICE_UPDATE_YAHOO_ENABLED: '1' }).enabled, true, 'env を立てれば Yahoo も送れる');
  eq(mallWriteEnabled('yahoo', {}).enabled, false, '★env 未設定なら Yahoo は送らない (fail-closed)');
  eq(mallWriteEnabled('yahoo', { PRICE_UPDATE_RAKUTEN_ENABLED: '1' }).enabled, false,
    '★楽天の env では Yahoo は開かない');
  eq(mallWriteEnabled('aupay', { PRICE_UPDATE_AUPAY_ENABLED: '1' }).enabled, true,
    'env を立てれば au PAY も送れる (2026-09-02〜)');
  eq(mallWriteEnabled('aupay', {}).enabled, false, '★env 未設定なら au PAY は送らない (fail-closed)');
  eq(mallWriteEnabled('aupay', ENV_BOTH).enabled, false,
    '★楽天・Yahoo の env では au PAY は開かない');
  eq(mallWriteEnabled('qoo10', { PRICE_UPDATE_AUPAY_ENABLED: '1' }).enabled, false,
    '★送信経路の無いモールは何をしても開かない');
}

console.log('\n── noop も確認できたときだけ確定する ──');
{
  const run = makeRun(2);
  const plan = [{ status: 200, body: { state: 'noop', reason: '同価格' } }];
  plan.verifyAs = 999;   // 取り直したら違う価格だった
  const client = makeClient(plan);
  const { summary } = await executeRun(db, run, { actor: 't', client, env: ENV_ON });
  eq(summary.noop, 0, '★確認が通らなければ noop で確定させない');
  eq(summary.failed, 1, '一致しないので failed');
  ok(summary.stopped.includes('確認'), '止めた理由が確認の不一致だと分かる');
}

console.log('\n── ★「価格は変わったが反映できていない」を記録に残す (Yahoo・Codex R6) ──');
{
  const ops = [{
    operationId: 'pf-1', mall: 'yahoo', neCode: 'ne-pf', rowKind: 'single',
    listingCode: 'yh-1', skuCode: 'yh-1', confidence: 'confirmed',
    expectedCurrentPrice: 1000, newPrice: 1001, cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.1,
    initialState: 'previewed',
  }];
  const runId = insertRun(db, { createdBy: 't', neCodes: ['ne-pf'], limits: {}, operations: ops });
  // 更新は通ったが反映を依頼できなかった (yahoo-apply が返す形)
  const yh = {
    calls: [],
    patchItemPrices: async function (code, body) {
      this.calls.push({ code, body });
      return {
        status: 200,
        body: {
          ok: false, error: 'PUBLISH_FAILED',
          message: '価格は 1001 円に変わりましたが、フロント反映を依頼できていません',
          applied: { 'yh-1': 1001 },
          publish: { requested: true, ok: false },
        },
      };
    },
    fetchItemDetail: async () => ({ item: { manageNumber: 'yh-1', variants: { 'yh-1': { standardPrice: '1001' } } }, status: 'found' }),
  };
  const { summary, results } = await executeRun(db, getRun(db, runId), {
    actor: 't', clients: { yahoo: yh }, env: { PRICE_UPDATE_YAHOO_ENABLED: '1' },
  });
  eq(summary.applied, 0, '★反映できていないので「更新済み」に数えない');
  ok(summary.stopped, '★その場で止める (人が確かめるまで残りを送らない)');
  eq(results[0].state, 'unknown', '状態は「結果が不明」として残る');

  // ★イベントの詳細に「いくらに変わったか」と「反映の結果」が残っている
  const ev = getRun(db, runId).events.filter((e) => e.operation_id === 'pf-1').map((e) => JSON.parse(e.detail_json || '{}'));
  const last = ev[ev.length - 1];
  eq(last.applied, 1001, '★いくらに変わったかが記録に残る (復旧の対象にできる)');
  eq(last.mayHaveChanged, true, '★価格が変わった印が付く');
  eq([last.publishRequested, last.publishOk], [true, false], '★反映を依頼したが通らなかったと分かる');
  ok(/反映/.test(last.reason || ''), '理由に反映のことが書いてある: ' + last.reason);
}

console.log('\n── 状態はイベントとして残る (行は書き換えない) ──');
{
  const run = makeRun(1);
  const client = makeClient([]);
  await executeRun(db, run, { actor: 'exec@example.com', client, env: ENV_ON });
  const after = getRun(db, run.run_id);
  eq(after.operations[0].state, 'confirmed', '最終状態は confirmed');
  const events = after.events.filter((e) => e.operation_id === run.operations[0].operation_id).map((e) => e.event);
  eq(events, ['executing', 'confirmed'], '★executing → confirmed の順で追記されている');
  ok(after.events.some((e) => e.event === 'run_executed'), 'run 全体のイベントも残る');
}

console.log('\n── 同じ送り先に違う新売価が来たら送らない (Yahoo のカラバリ) ──');
{
  // Yahoo は商品に1つの価格しか持たないので、色が違っても送り先は同じ商品コードになる。
  // ここで色ごとに違う新売価が入っていると、後から入れたほうが黙って勝ってしまう。
  // どちらを送るか決められない、として送らない
  const mk = (neCode, newPrice) => ({
    operationId: `puo-dup-${neCode}-${Math.random().toString(36).slice(2, 8)}`,
    mall: 'yahoo', neCode, rowKind: 'single',
    listingCode: 'kara-1', skuCode: 'kara-1',   // ★色が違っても送り先は同じ
    confidence: 'confirmed', expectedCurrentPrice: 577, newPrice,
    cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12,
    initialState: 'previewed',
  });
  let sent = 0;
  const client = {
    patchItemPrices: async () => { sent++; return { status: 200, body: { state: "applied" } }; },
    fetchItemDetail: async () => ({ item: { variants: { "kara-1": { standardPrice: "600" } } } }),
  };

  const dupId = insertRun(db, { createdBy: 't', neCodes: ['ne-k1', 'ne-k2'], limits: {},
    operations: [mk('ne-k1', 600), mk('ne-k2', 650)] });
  const dup = await executeRun(db, getRun(db, dupId), { actor: 't', clients: { yahoo: client },
    env: { PRICE_UPDATE_YAHOO_ENABLED: '1' }, skipClaim: true });
  eq(sent, 0, "★1件も送らない (どちらの値を送るか決められない)");
  eq(dup.summary.sent, 0, "送信数 0");
  eq(dup.results.every((r) => r.state === "blocked"), true, "全行が blocked");
  ok(/違う新売価/.test(dup.results[0].reason || ""), "理由に「違う新売価」と書く");

  // 同じ値なら送ってよい (色をまとめて同じ価格にする、はふつうの操作)
  const sameId = insertRun(db, { createdBy: 't', neCodes: ['ne-k3', 'ne-k4'], limits: {},
    operations: [mk('ne-k3', 600), mk('ne-k4', 600)] });
  const same = await executeRun(db, getRun(db, sameId), { actor: 't', clients: { yahoo: client },
    env: { PRICE_UPDATE_YAHOO_ENABLED: '1' }, skipClaim: true });
  eq(sent, 1, "★同じ値なら 1回だけ送る (商品1つに1リクエスト)");
  eq(same.summary.applied, 2, "2行とも成功として記録される");
}

console.log('\n── 送り先キーと事前検証 (グループ分けとは別) ──');
{
  const y = (listing, sku) => ({ mall: "yahoo", listing_code: listing, sku_code: sku });
  const r = (listing, sku) => ({ mall: "rakuten", listing_code: listing, sku_code: sku });

  eq(sendKeyOf(y("kara-1", "kara-1")), sendKeyOf(y("KARA-1", "kara-1")),
    "★Yahoo: 出品コードの表記ゆれは同じ送り先");
  eq(sendKeyOf(y("kara-1", "a")) === sendKeyOf(y("kara-1", "b")), true,
    "★Yahoo: SKU が違っても送り先は同じ (商品に1つの価格)");
  eq(sendKeyOf(r("mn-1", "sku-a")) === sendKeyOf(r("mn-1", "sku-b")), false,
    "★楽天: SKU ごとに別の送り先 (variant ごとに価格を持つ)");

  // 表記ゆれで別グループに割れても、同じ商品への矛盾は見つかる
  const split = findSendConflicts([
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 577 },
    { ...y("KARA-1", "KARA-1"), new_price: 650, expected_current_price: 577 },
  ]);
  eq(split.size, 1, "★大文字小文字で割れても同じ商品として見つける");

  // 新売価が同じでも、記録した時の価格が違えば送らない (楽観ロックの基準を選べない)
  const exp = findSendConflicts([
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 577 },
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 580 },
  ]);
  eq(exp.size, 1, "★新売価が同じでも、記録時の価格が違えば送らない");
  ok(/記録した時の価格/.test([...exp.values()][0]), "理由に記録時の価格の食い違いと書く");

  // ★3行以上あっても「重い食い違い」が優先される。
  //   見つけた順で決めると、先に拾った表記ゆれの陰に「違う新売価」が隠れる (Codex R3)
  const order1 = findSendConflicts([
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 577 },
    { ...y("KARA-1", "KARA-1"), new_price: 600, expected_current_price: 577 },   // 表記ゆれ
    { ...y("kara-1", "kara-1"), new_price: 650, expected_current_price: 577 },   // 違う新売価
  ]);
  ok(/違う新売価/.test([...order1.values()][0]),
    "★表記ゆれを先に見ても、違う新売価のほうを理由にする");

  const order2 = findSendConflicts([
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 577 },
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 580 },   // 記録時の価格が違う
    { ...y("kara-1", "kara-1"), new_price: 650, expected_current_price: 577 },   // 違う新売価
  ]);
  ok(/違う新売価/.test([...order2.values()][0]),
    "★記録時の価格の食い違いを先に見ても、違う新売価のほうを理由にする");

  const order3 = findSendConflicts([
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 577 },
    { ...y("KARA-1", "KARA-1"), new_price: 600, expected_current_price: 580 },
  ]);
  ok(/記録した時の価格/.test([...order3.values()][0]),
    "★表記ゆれより、記録時の価格の食い違いを先に出す");

  eq(findSendConflicts([
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 577 },
    { ...y("kara-1", "kara-1"), new_price: 600, expected_current_price: 577 },
  ]).size, 0, "同じ指示が2行あるだけなら問題にしない");
}

console.log('\n── 表記ゆれ・kill switch との優先順位 ──');
{
  const mk = (id, listing, sku, newPrice, expected) => ({
    operationId: `puo-${id}-${Math.random().toString(36).slice(2, 8)}`,
    mall: "yahoo", neCode: `ne-${id}`, rowKind: "single",
    listingCode: listing, skuCode: sku,
    confidence: "confirmed", expectedCurrentPrice: expected, newPrice,
    cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12,
    initialState: "previewed",
  });
  const mkClient = () => {
    const sentTo = [];
    return {
      sentTo,
      patchItemPrices: async (code) => { sentTo.push(code); return { status: 200, body: { state: "applied" } }; },
      fetchItemDetail: async () => ({ item: { variants: { "kara-1": { standardPrice: "600" }, "KARA-1": { standardPrice: "600" } } } }),
    };
  };

  // ★表記ゆれ + 同じ指示 → 同じ商品へ2回送らない
  const c1 = mkClient();
  const id1 = insertRun(db, { createdBy: "t", neCodes: ["ne-a", "ne-b"], limits: {},
    operations: [mk("a", "kara-1", "kara-1", 600, 577), mk("b", "KARA-1", "KARA-1", 600, 577)] });
  const o1 = await executeRun(db, getRun(db, id1), { actor: "t", clients: { yahoo: c1.patchItemPrices ? c1 : c1 },
    env: { PRICE_UPDATE_YAHOO_ENABLED: "1" }, skipClaim: true });
  eq(c1.sentTo.length, 0, "★書き方が揃っていないので送らない (2回送信になる手前で止める)");
  ok(/書き方が揃っていません/.test(o1.results[0].reason || ""), "理由に書き方の不揃いと書く");

  // グループ分けも正規化されている (万一すり抜けても1リクエストにまとまる)
  eq(groupOperations([
    { mall: "yahoo", listing_code: "kara-1", sku_code: "kara-1" },
    { mall: "yahoo", listing_code: "KARA-1", sku_code: "KARA-1" },
  ]).length, 1, "★大文字小文字違いは1グループ (同じ商品へ2回送らない)");

  // ★kill switch が無効でも、run の中身が決められない行は blocked として残す
  const c2 = mkClient();
  const id2 = insertRun(db, { createdBy: "t", neCodes: ["ne-c", "ne-d", "ne-e"], limits: {},
    operations: [mk("c", "kara-2", "kara-2", 600, 577), mk("d", "kara-2", "kara-2", 650, 577),
      mk("e", "other-1", "other-1", 600, 577)] });
  const o2 = await executeRun(db, getRun(db, id2), { actor: "t", clients: { yahoo: c2 },
    env: {}, skipClaim: true });   // ← kill switch は入れていない
  eq(c2.sentTo.length, 0, "kill switch が無ければ1件も送らない");
  const st = Object.fromEntries(o2.results.map((r) => [r.operationId.split("-")[1], r.state]));
  eq([st.c, st.d], ["blocked", "blocked"], "★衝突行は blocked (kill switch を入れれば送れる、と読ませない)");
  eq(st.e, "skipped", "衝突していない行は kill switch の理由で skipped");

  // ★クライアントが無い時も同じ (送り口が無いことと、run の中身が壊れていることは別の事実)
  const id3 = insertRun(db, { createdBy: "t", neCodes: ["ne-f", "ne-g"], limits: {},
    operations: [mk("f", "kara-3", "kara-3", 600, 577), mk("g", "kara-3", "kara-3", 650, 577)] });
  const o3 = await executeRun(db, getRun(db, id3), { actor: "t", clients: {},
    env: { PRICE_UPDATE_YAHOO_ENABLED: "1" }, skipClaim: true });
  eq(o3.results.every((r) => r.state === "blocked"), true, "★送り口が無くても、衝突は衝突として残す");

  // ★全行が衝突して送るものが無くなった時
  eq(o3.summary.sent, 0, "送信数 0");
  eq(o3.summary.skipped, 2, "全行が対象外として数えられる");
}

console.log('\n── au PAY の送信 (商品に1つの価格) ──');
{
  const mk = (id, neCode, code, newPrice, expected) => ({
    operationId: `puo-au-${id}-${Math.random().toString(36).slice(2, 8)}`,
    mall: "aupay", neCode, rowKind: "single",
    listingCode: code, skuCode: code,   // ★au PAY は送り先 = 商品コード
    confidence: "confirmed", expectedCurrentPrice: expected, newPrice,
    cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12,
    initialState: "previewed",
  });
  const mkClient = (live) => {
    const sent = [];
    return {
      sent,
      patchItemPrices: async (code, { prices }) => {
        sent.push({ code, prices });
        return { status: 200, body: { state: "applied", applied: prices } };
      },
      fetchItemDetail: async (code) => ({ item: { variants: { [code]: { standardPrice: String(live) } } } }),
    };
  };

  // ★au PAY のスイッチだけで開き、au PAY のクライアントにだけ送られる
  const c = mkClient(600);
  const rakuten = mkClient(600);
  const id = insertRun(db, { createdBy: "t", neCodes: ["ne-au"], limits: {},
    operations: [mk("a", "ne-au", "au-1", 600, 577)] });
  const out = await executeRun(db, getRun(db, id), { actor: "t",
    clients: { aupay: c, rakuten },
    env: { PRICE_UPDATE_AUPAY_ENABLED: "1" }, skipClaim: true });
  eq(out.summary.applied, 1, "au PAY へ送れる");
  eq(c.sent.length, 1, "au PAY のクライアントに届く");
  eq(rakuten.sent.length, 0, "★別のモールのクライアントには送らない");
  eq(c.sent[0].code, "au-1", "送り先は商品コード");

  // ★同じ商品の2行が同じ値 → 1リクエストにまとまる
  const c2 = mkClient(650);
  const id2 = insertRun(db, { createdBy: "t", neCodes: ["ne-b1", "ne-b2"], limits: {},
    operations: [mk("b1", "ne-b1", "au-2", 650, 577), mk("b2", "ne-b2", "au-2", 650, 577)] });
  const out2 = await executeRun(db, getRun(db, id2), { actor: "t", clients: { aupay: c2 },
    env: { PRICE_UPDATE_AUPAY_ENABLED: "1" }, skipClaim: true });
  eq(c2.sent.length, 1, "★同じ商品への2行は1リクエストにまとまる");
  eq(out2.summary.applied, 2, "2行とも成功として記録される");

  // ★同じ商品に違う値 → 1件も送らない
  const c3 = mkClient(600);
  const id3 = insertRun(db, { createdBy: "t", neCodes: ["ne-c1", "ne-c2"], limits: {},
    operations: [mk("c1", "ne-c1", "au-3", 600, 577), mk("c2", "ne-c2", "au-3", 650, 577)] });
  const out3 = await executeRun(db, getRun(db, id3), { actor: "t", clients: { aupay: c3 },
    env: { PRICE_UPDATE_AUPAY_ENABLED: "1" }, skipClaim: true });
  eq(c3.sent.length, 0, "★同じ商品に違う新売価なら1件も送らない");
  eq(out3.results.every((r) => r.state === "blocked"), true, "全行が blocked");

  // ★送った後の照合が合わなければ成功にしない
  const c4 = mkClient(999);   // 送ったのは 600 なのに 999 が返る
  const id4 = insertRun(db, { createdBy: "t", neCodes: ["ne-d"], limits: {},
    operations: [mk("d", "ne-d", "au-4", 600, 577)] });
  const out4 = await executeRun(db, getRun(db, id4), { actor: "t", clients: { aupay: c4 },
    env: { PRICE_UPDATE_AUPAY_ENABLED: "1" }, skipClaim: true });
  eq(out4.summary.applied, 0, "★照合が合わなければ成功にしない");
  eq(out4.summary.failed, 1, "失敗として残る");
}

console.log('\n── モールの設定が食い違っていないか (増やし忘れ対策) ──');
{
  // ★2026-09-02: au PAY を足した時、execute と resolve は直したのに pricing.js を
  //   直し忘れ、画面では選べるのに送信の手前で必ず弾かれる状態になった。
  //   同じ事実が散らないよう mall-capabilities.js に集約し、食い違いを機械で見つける
  eq(findCapabilityProblems(), [], "★モールの設定に食い違いが無い");

  for (const [mall, c] of Object.entries(MALL_CAPABILITIES)) {
    if (!c.executable) continue;
    ok(killSwitchKeyOf(mall) !== null, `${mall}: 送信スイッチの名前がある`);
    ok(UPDATABLE_MALLS.includes(mall), `${mall}: 画面でも新売価を入れられる`);
    ok(c.priceScope === 'sku' || c.priceScope === 'item', `${mall}: 価格の単位が決まっている`);
    // ★スイッチを入れれば開く / 入れなければ開かない、が両方成り立つこと
    eq(mallWriteEnabled(mall, { [c.killSwitch]: "1" }).enabled, true, `${mall}: スイッチで開く`);
    eq(mallWriteEnabled(mall, {}).enabled, false, `${mall}: スイッチ無しでは開かない`);
  }
  // 更新できないモールは、ガードの側でも必ず理由つきで止まる
  for (const [mall, c] of Object.entries(MALL_CAPABILITIES)) {
    if (c.updatable) continue;
    eq(mallWriteEnabled(mall, { PRICE_UPDATE_RAKUTEN_ENABLED: "1" }).enabled, false,
      `${mall}: 更新できないモールは何をしても開かない`);
  }
  eq([...ITEM_PRICE_MALLS].sort(), ['aupay', 'qoo10', 'yahoo'],
    "★商品に1つの価格のモール = Yahoo / au PAY / Qoo10 (楽天だけ SKU ごと)");
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
