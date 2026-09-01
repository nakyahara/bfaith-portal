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
import { executeRun, mallWriteEnabled, classify, groupOperations, BREAKER_CONSECUTIVE_FAILURES } from './execute.js';
import { executorGate } from './router.js';

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

console.log('\n── Yahoo は送らない (送信経路が楽天しか無いため) ──');
{
  const ops = [{
    operationId: 'y1', mall: 'yahoo', neCode: 'ne-y', rowKind: 'single', listingCode: 'yahoo-item', skuCode: 'ys1',
    confidence: 'confirmed', expectedCurrentPrice: 577, newPrice: 620, cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.1,
    initialState: 'previewed',
  }];
  const runId = insertRun(db, { createdBy: 't', neCodes: ['ne-y'], limits: {}, operations: ops });
  const client = makeClient([]);
  const { summary, results } = await executeRun(db, getRun(db, runId), {
    actor: 't', client, env: { PRICE_UPDATE_RAKUTEN_ENABLED: '1', PRICE_UPDATE_YAHOO_ENABLED: '1' },
  });
  eq(client.calls.length, 0, '★Yahoo の出品コードを楽天の管理番号として送らない');
  eq(summary.skipped, 1, 'skipped として記録');
  ok(results[0].reason.includes('送信経路がありません'), '理由が分かる');
  eq(mallWriteEnabled('yahoo', { PRICE_UPDATE_YAHOO_ENABLED: '1' }).enabled, false, 'env を立てても Yahoo は無効');
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

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
