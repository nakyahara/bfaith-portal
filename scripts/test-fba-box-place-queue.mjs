/**
 * FBA箱詰め記録 — 投入の送信キュー (apps/fba-box/views/place-queue.js) のテスト
 * 実行: node scripts/test-fba-box-place-queue.mjs
 *
 * 通信断まわりは本番で一番壊れやすいのに、これまで画面側にテストが無かった
 * (Codex PR2.6-R5 medium#1)。post / storage / 時間を差し替えて、
 * 「応答喪失 → 送り直し → 押し直し」の分岐をここで固定する。
 *
 * サーバー役は本物の契約に合わせる (Codex PQ-R1 medium#3):
 *   - 業務エラー (箱が閉じている・予定数超え) は**投入を作らない**ので、あとで冪等応答にならない
 *   - 登録済みの request_id は、あとから業務エラーにはならず、必ず同じ成功結果を返す
 *   - 「サーバーに届く前に落ちた (lost)」と「登録できたが応答だけ消えた (commit_lost)」を区別する
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import url from 'node:url';
import vm from 'node:vm';

const here = path.dirname(url.fileURLToPath(import.meta.url));
const src = fs.readFileSync(path.join(here, '..', 'apps', 'fba-box', 'views', 'place-queue.js'), 'utf8');
const sandbox = { window: {} };
vm.createContext(sandbox);
vm.runInContext(src, sandbox, { filename: 'place-queue.js' });
const { createPlaceQueue } = sandbox.window;
assert.equal(typeof createPlaceQueue, 'function', 'place-queue.js が window.createPlaceQueue を出していない');

let passed = 0, failed = 0;
async function t(name, fn) {
  try { await fn(); passed++; console.log(`  ✅ ${name}`); }
  catch (e) { failed++; console.error(`  ❌ ${name}\n     ${(e.stack || e.message).split('\n').slice(0, 3).join('\n     ')}`); }
}

/** 端末の localStorage 代わり (1件だけ)。failWrite = 保存できない端末 (プライベートモード等) */
function makeStorage({ failWrite = false, seed = null } = {}) {
  let v = seed;
  return {
    get: () => v,
    set: (x) => { if (failWrite) throw new Error('QuotaExceeded'); v = x; },
    raw: () => v,
    force: (x) => { v = x; },      // 別の画面が書いた状況を作る
  };
}
/**
 * サーバー役。script に 'ok' / 'lost' (届く前に落ちた) / 'commit_lost' (登録後に応答が消えた) /
 * {fail:'…'} (業務エラー = 登録しない) を並べる。無ければ 'ok'
 */
function makeServer(script = []) {
  const posts = [], registered = new Map();
  const steps = script.slice();
  const register = (body) => {
    const r = { ok: true, placementId: registered.size + 1, placed: body.qty, checkWorker: 'りようしゃ', checkWorkerSource: 'auto' };
    registered.set(body.request_id, r);
    return r;
  };
  const post = async (body) => {
    posts.push(body);
    const step = steps.length ? steps.shift() : 'ok';
    if (registered.has(body.request_id)) {                       // 冪等: 同じ結果を返し続ける
      if (step === 'lost' || step === 'commit_lost') throw new Error('indeterminate');
      return Object.assign({ already: true }, registered.get(body.request_id));
    }
    if (step === 'lost') throw new Error('indeterminate');
    if (step === 'commit_lost') { register(body); throw new Error('indeterminate'); }
    if (step && step.fail) return { ok: false, message: step.fail, _status: 409 };   // 投入は作らない
    return register(body);
  };
  return { post, posts, registered, push: (...x) => steps.push(...x) };
}
const bodyOf = (over = {}) => Object.assign({ run_id: 1, row_id: 10, box_id: 20, qty: 5, layer: null, expiry: null, worker_id: 2 }, over);
let seq = 0;
const mk = (server, storage) => createPlaceQueue({
  post: server.post, storage, newId: () => `req-${++seq}`, wait: async () => {}, retryWaitMs: 0,
});

console.log('■ 投入の送信キュー');

await t('普通に送れる: resolved で結果が返り、ack するまで端末に残る', async () => {
  const server = makeServer(), storage = makeStorage();
  const q = mk(server, storage);
  const out = await q.send({ body: bodyOf(), meta: { productName: 'ロジン' } });
  assert.equal(out.kind, 'done');
  assert.equal(out.record.status, 'resolved');
  assert.equal(out.record.result.ok, true);
  assert.equal(server.registered.size, 1);
  assert.ok(storage.raw(), '結果を人に見せる前に消さない');
  await q.ack(out.record.requestId);
  assert.equal(storage.raw(), null);
});

await t('応答が返らないと sending のまま残り、押し直しても新しい request_id を発行しない', async () => {
  const server = makeServer(['commit_lost', 'lost', 'lost']), storage = makeStorage();
  const q = mk(server, storage);
  const first = await q.send({ body: bodyOf() });
  assert.equal(first.record.status, 'sending');
  assert.equal(server.registered.size, 1, 'サーバーには1件だけ入っている');
  const again = await q.send({ body: bodyOf() });
  assert.equal(again.kind, 'blocked');
  assert.equal(server.registered.size, 1, '押し直しで2件目を作らない');
  assert.equal(storage.raw().requestId, first.record.requestId, '同じ request_id を持ち続ける');
});

await t('裏で送り直して確定した分は flush が返し、ack するまで消えない (R5 high#2)', async () => {
  const server = makeServer(['commit_lost', 'lost']), storage = makeStorage();
  const q = mk(server, storage);
  const first = await q.send({ body: bodyOf(), meta: { productName: 'ロジン' } });
  assert.equal(first.record.status, 'sending');
  const resolved = await q.flush();                 // 電波が戻った
  assert.equal(resolved.status, 'resolved');
  assert.equal(resolved.result.ok, true);
  assert.equal(resolved.result.already, true, '冪等応答 = 二重に入っていない');
  assert.equal(resolved.meta.productName, 'ロジン', '結果を見せるための情報を持ち回る');
  assert.equal(server.registered.size, 1);
  assert.ok(storage.raw(), '人に見せる前に消さない');
  assert.equal((await q.flush()).requestId, resolved.requestId, '見せるまで何度でも返す');
  await q.ack(resolved.requestId);
  assert.equal(await q.flush(), null);
});

await t('確定済みが残っている同じ商品への投入は confirm (個数・期限・配置・箱が違っても)', async () => {
  const server = makeServer(['commit_lost']), storage = makeStorage();
  const q = mk(server, storage);
  await q.send({ body: bodyOf({ qty: 5, expiry: '2028-10', layer: 'bottom' }), meta: { productName: 'ロジン' } });
  // 開き直すと期限は既知になり (null)、配置も初期化される → 内容の比較では一致しない (R5 high#1)
  const out = await q.send({ body: bodyOf({ qty: 3 }), meta: { productName: 'ロジン' } });
  assert.equal(out.kind, 'confirm');
  assert.equal(out.prev.body.qty, 5);
  assert.equal(out.body.qty, 3);
  assert.equal(server.registered.size, 1, '人に聞く前に2件目を作らない');
  // 箱が違っても同じ商品なら聞く (箱が閉じられた・入れ直したで箱だけ変わる — PQ-R1 medium#2)
  const other = await q.send({ body: bodyOf({ box_id: 21 }), meta: { productName: 'ロジン' } });
  assert.equal(other.kind, 'confirm');
  assert.equal(server.registered.size, 1);
});

await t('confirm に「いいえ」= 前の1件を確認しただけ。記録は1件のまま', async () => {
  const server = makeServer(['commit_lost']), storage = makeStorage();
  const q = mk(server, storage);
  await q.send({ body: bodyOf() });
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.kind, 'confirm');
  await q.ack(out.prev.requestId);
  assert.equal(storage.raw(), null);
  assert.equal(server.registered.size, 1);
});

await t('confirm に「はい、さらに入れた」= 新しい request_id で2件目が入る (R5 high#4)', async () => {
  const server = makeServer(['commit_lost']), storage = makeStorage();
  const q = mk(server, storage);
  await q.send({ body: bodyOf() });
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.kind, 'confirm');
  const again = await q.ackAndSend({ ackRequestId: out.prev.requestId, body: out.body, meta: out.meta });
  assert.equal(again.kind, 'done');
  assert.equal(again.record.status, 'resolved');
  assert.equal(server.registered.size, 2, '本物の2回目は飲み込まない');
  assert.notEqual(again.record.requestId, out.prev.requestId);
});

await t('別の商品なら showFirst — 先に前の結果を見せる。見せる前に消さない (PQ-R1 high#2)', async () => {
  const server = makeServer(['commit_lost']), storage = makeStorage();
  const q = mk(server, storage);
  const first = await q.send({ body: bodyOf({ row_id: 10 }), meta: { productName: 'ロジン' } });
  const out = await q.send({ body: bodyOf({ row_id: 11 }), meta: { productName: 'アロマ' } });
  assert.equal(out.kind, 'showFirst');
  assert.equal(out.prev.meta.productName, 'ロジン');
  assert.equal(storage.raw().requestId, first.record.requestId, '見せる前に端末から消さない');
  assert.equal(server.registered.size, 1, '見せる前に次を送らない');
  await q.ack(out.prev.requestId);                       // 人が「わかりました」を押した
  const again = await q.send({ body: out.body, meta: out.meta });
  assert.equal(again.kind, 'done');
  assert.equal(server.registered.size, 2);
});

await t('前の1件が業務エラーなら「記録できています」と言わない (PQ-R1 high#1)', async () => {
  const server = makeServer([{ fail: 'この箱は閉じられています' }]), storage = makeStorage();
  const q = mk(server, storage);
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.record.status, 'resolved');
  assert.equal(out.record.result.ok, false);
  assert.ok(storage.raw(), '人に見せる前に消さない');
  const next = await q.send({ body: bodyOf() });
  assert.equal(next.kind, 'previousFailed', 'confirm にすると「登録できています」と嘘を言う');
  assert.equal(next.prev.result.message, 'この箱は閉じられています');
  assert.equal(server.registered.size, 0, '業務エラーは投入を作っていない');
});

await t('sending の送り直しが業務エラーで確定したときも previousFailed', async () => {
  const server = makeServer(['lost', 'lost', { fail: '予定数を超えます' }]), storage = makeStorage();
  const q = mk(server, storage);
  const first = await q.send({ body: bodyOf() });
  assert.equal(first.record.status, 'sending');
  const next = await q.send({ body: bodyOf() });
  assert.equal(next.kind, 'previousFailed');
  assert.equal(next.prev.result.message, '予定数を超えます');
  assert.equal(server.registered.size, 0);
});

await t('端末に保存できないときは送らない (PQ-R1 medium#1)', async () => {
  const server = makeServer(), storage = makeStorage({ failWrite: true });
  const q = mk(server, storage);
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.kind, 'storage_failed');
  assert.equal(server.posts.length, 0, '送り直せないので送らない (二重登録の元)');
});

await t('別の画面が書いた1件は上書きしない (CAS)。ackAndSend も conflict で止まる (PQ-R1 high#3)', async () => {
  const server = makeServer(['commit_lost']), storage = makeStorage();
  const q = mk(server, storage);
  const out = await q.send({ body: bodyOf() });
  const mine = out.record.requestId;
  storage.force({ requestId: 'other-tab', body: bodyOf({ row_id: 99 }), status: 'sending' });
  assert.equal(await q.ack(mine), false, '自分の1件でなければ消さない');
  assert.equal(storage.raw().requestId, 'other-tab');
  const conflict = await q.ackAndSend({ ackRequestId: mine, body: bodyOf(), meta: null });
  assert.equal(conflict.kind, 'conflict');
  assert.equal(storage.raw().requestId, 'other-tab', '他の画面の1件を消していない');
  assert.equal(server.registered.size, 1, '新しい request_id を発行していない');
});

await t('更新前の形 {requestId, body} は sending として拾う (PQ-R1 high#6)', async () => {
  const legacy = { requestId: 'old-1', body: bodyOf({ request_id: 'old-1' }) };   // status なし
  const server = makeServer(), storage = makeStorage({ seed: legacy });
  const q = mk(server, storage);
  assert.equal(q.peek().status, 'sending');
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.kind, 'confirm', '旧レコードを確定させてから聞く (無視して新規送信しない)');
  assert.equal(out.prev.requestId, 'old-1');
  assert.equal(server.posts.length, 1);
  assert.equal(server.posts[0].request_id, 'old-1', '同じ request_id で送り直す');
});

await t('読めない形が残っていたら上書きせず broken を返す', async () => {
  const server = makeServer(), storage = makeStorage({ seed: { junk: true } });
  const q = mk(server, storage);
  assert.equal(q.peek().broken, true);
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.kind, 'broken');
  assert.equal(server.posts.length, 0);
  assert.deepEqual(storage.raw(), { junk: true }, '消さない (中身が分からないので職員が判断する)');
  assert.equal(await q.discardBroken(), true);
  assert.equal(storage.raw(), null);
});

await t('送信と送り直しが並行しても、古いほうが新しい1件を消さない (R5 high#3)', async () => {
  const storage = makeStorage();
  let release;
  const gate = new Promise((r) => { release = r; });
  const registered = new Set();
  let firstCall = true;
  const post = async (body) => {
    if (firstCall) { firstCall = false; await gate; throw new Error('indeterminate'); }
    registered.add(body.request_id);
    return { ok: true, placementId: registered.size };
  };
  const q = createPlaceQueue({ post, storage, newId: () => `req-${++seq}`, wait: async () => {}, retryWaitMs: 0 });
  const slow = q.send({ body: bodyOf() });     // 1件目 (gate で止まる)
  const alsoSlow = q.flush();                  // 直列化されるので1件目の完了待ちになる
  release();
  await slow; await alsoSlow;
  const stuck = storage.raw();
  assert.ok(stuck, '未確定の1件は残っている');
  await q.ack(stuck.requestId);
  const fresh = await q.send({ body: bodyOf({ row_id: 12 }) });
  assert.equal(fresh.record.status, 'resolved');
  assert.equal(storage.raw().requestId, fresh.record.requestId, '新しい1件が残っている');
  await q.ack('req-999');                      // 他人の requestId では消えない (CAS)
  assert.equal(storage.raw().requestId, fresh.record.requestId);
});

await t('新規送信は1回だけ送り直す。裏の flush は1回だけ試す', async () => {
  const server = makeServer(['lost']), storage = makeStorage();
  const q = mk(server, storage);
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.record.status, 'resolved', '1回落ちても新規送信はその場で送り直す');
  assert.equal(server.posts.length, 2);
  const server2 = makeServer(['lost', 'lost', 'lost']), storage2 = makeStorage();
  const q2 = mk(server2, storage2);
  await q2.send({ body: bodyOf() });                 // 2回とも落ちる → sending のまま
  assert.equal(storage2.raw().status, 'sending');
  const before = server2.posts.length;
  assert.equal(await q2.flush(), null, 'まだ確定しない');
  assert.equal(server2.posts.length, before + 1, 'flush の再送は1回だけ (画面更新のたびに叩きすぎない)');
});

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
