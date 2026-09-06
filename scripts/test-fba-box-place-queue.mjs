/**
 * FBA箱詰め記録 — 投入の送信キュー (apps/fba-box/views/place-queue.js) のテスト
 * 実行: node scripts/test-fba-box-place-queue.mjs
 *
 * 通信断まわりは本番で一番壊れやすいのに、これまで画面側にテストが無かった
 * (Codex PR2.6-R5 medium#1)。post / storage / 時間を差し替えて、
 * 「応答喪失 → 送り直し → 押し直し」の分岐を全部ここで固定する。
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
  catch (e) { failed++; console.error(`  ❌ ${name}\n     ${e.stack?.split('\n').slice(0, 3).join('\n     ') || e.message}`); }
}

/** 端末の localStorage 代わり (1件だけ) */
function makeStorage() {
  let v = null;
  return { get: () => v, set: (x) => { v = x; }, raw: () => v };
}
/**
 * サーバー役。sent = 実際に届いた body (= 登録された投入)。
 * lose = 次の N 回は「サーバーは処理したが応答が返らない」。
 * fail = 次の1回だけ業務エラー (409 等) を返す
 */
function makeServer({ lose = 0, fail = null } = {}) {
  const sent = [], accepted = new Map();
  let toLose = lose, toFail = fail;
  const post = async (body) => {
    sent.push(body);
    const known = accepted.has(body.request_id);
    if (!known) accepted.set(body.request_id, accepted.size + 1);
    if (toLose > 0) { toLose--; throw new Error('indeterminate'); }
    if (toFail) { const f = toFail; toFail = null; return { ok: false, message: f, _status: 409 }; }
    return { ok: true, already: known, placementId: accepted.get(body.request_id), checkWorker: 'りようしゃ', checkWorkerSource: 'auto' };
  };
  return { post, sent, accepted, lose: (n) => { toLose = n; }, fail: (m) => { toFail = m; } };
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
  assert.equal(server.accepted.size, 1);
  assert.ok(storage.raw(), '結果を人に見せる前に消さない');
  await q.ack(out.record.requestId);
  assert.equal(storage.raw(), null);
});

await t('応答が返らないと sending のまま残り、押し直しても新しい request_id を発行しない', async () => {
  const server = makeServer({ lose: 99 }), storage = makeStorage();
  const q = mk(server, storage);
  const first = await q.send({ body: bodyOf() });
  assert.equal(first.record.status, 'sending');
  assert.equal(server.accepted.size, 1, 'サーバーには1件だけ届いている');
  const again = await q.send({ body: bodyOf() });
  assert.equal(again.kind, 'blocked');
  assert.equal(server.accepted.size, 1, '押し直しで2件目を作らない');
  assert.equal(storage.raw().requestId, first.record.requestId, '同じ request_id を持ち続ける');
});

await t('裏で送り直して確定した分は flush が返し、ack するまで消えない (R5 high#2)', async () => {
  const server = makeServer({ lose: 99 }), storage = makeStorage();
  const q = mk(server, storage);
  const first = await q.send({ body: bodyOf(), meta: { productName: 'ロジン' } });
  assert.equal(first.record.status, 'sending');
  server.lose(0);                                   // 電波が戻った
  const resolved = await q.flush();
  assert.equal(resolved.status, 'resolved');
  assert.equal(resolved.result.ok, true);
  assert.equal(resolved.meta.productName, 'ロジン', '結果を見せるための情報を持ち回る');
  assert.equal(server.accepted.size, 1);
  assert.ok(storage.raw(), '人に見せる前に消さない');
  assert.equal((await q.flush()).requestId, resolved.requestId, '見せるまで何度でも返す');
  await q.ack(resolved.requestId);
  assert.equal(await q.flush(), null);
});

await t('確定済みが残っている同じ商品・同じ箱への投入は confirm (個数・期限・配置が違っても)', async () => {
  const server = makeServer({ lose: 1 }), storage = makeStorage();
  const q = mk(server, storage);
  await q.send({ body: bodyOf({ qty: 5, expiry: '2028-10', layer: 'bottom' }), meta: { productName: 'ロジン' } });
  // 開き直すと期限は既知になり (null)、配置も初期化される → 内容の比較では一致しない (R5 high#1)
  const out = await q.send({ body: bodyOf({ qty: 3 }), meta: { productName: 'ロジン' } });
  assert.equal(out.kind, 'confirm');
  assert.equal(out.prev.body.qty, 5);
  assert.equal(out.body.qty, 3);
  assert.equal(server.accepted.size, 1, '人に聞く前に2件目を作らない');
});

await t('confirm に「いいえ」= 前の1件を確認しただけ。記録は1件のまま', async () => {
  const server = makeServer({ lose: 1 }), storage = makeStorage();
  const q = mk(server, storage);
  await q.send({ body: bodyOf() });
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.kind, 'confirm');
  await q.ack(out.prev.requestId);
  assert.equal(storage.raw(), null);
  assert.equal(server.accepted.size, 1);
});

await t('confirm に「はい、さらに入れた」= 新しい request_id で2件目が入る (R5 high#4)', async () => {
  const server = makeServer({ lose: 1 }), storage = makeStorage();
  const q = mk(server, storage);
  await q.send({ body: bodyOf() });
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.kind, 'confirm');
  const again = await q.confirmAndSend({ ackRequestId: out.prev.requestId, body: out.body, meta: out.meta });
  assert.equal(again.record.status, 'resolved');
  assert.equal(server.accepted.size, 2, '本物の2回目は飲み込まない');
  assert.notEqual(again.record.requestId, out.prev.requestId);
});

await t('別の商品・別の箱なら聞かずに送り、見せそびれた分は alsoResolved で返す', async () => {
  const server = makeServer({ lose: 1 }), storage = makeStorage();
  const q = mk(server, storage);
  await q.send({ body: bodyOf({ row_id: 10 }), meta: { productName: 'ロジン' } });
  const out = await q.send({ body: bodyOf({ row_id: 11 }), meta: { productName: 'アロマ' } });
  assert.equal(out.kind, 'done');
  assert.equal(out.record.status, 'resolved');
  assert.equal(out.alsoResolved.meta.productName, 'ロジン', '前の分も人に見せられる');
  assert.equal(server.accepted.size, 2);
  const out2 = await q.send({ body: bodyOf({ row_id: 11, box_id: 21 }), meta: { productName: 'アロマ' } });
  assert.equal(out2.kind, 'done', '同じ商品でも別の箱なら迷わない');
  assert.equal(server.accepted.size, 3);
});

await t('業務エラー (409 など) も確定として返る = 握り潰さない (R4 high#1)', async () => {
  const server = makeServer(), storage = makeStorage();
  const q = mk(server, storage);
  server.fail('予定数を超えます');
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.record.status, 'resolved');
  assert.equal(out.record.result.ok, false);
  assert.equal(out.record.result.message, '予定数を超えます');
  assert.ok(storage.raw(), '人に見せる前に消さない');
  // 裏で送り直したときも同じ (refresh 経由で人に伝えられる)
  const server2 = makeServer({ lose: 2 }), storage2 = makeStorage();   // send の2回とも落とす
  const q2 = mk(server2, storage2);
  await q2.send({ body: bodyOf() });
  assert.equal(storage2.raw().status, 'sending');
  server2.fail('この箱は閉じられています');
  const resolved = await q2.flush();
  assert.equal(resolved.result.ok, false);
  assert.equal(resolved.result.message, 'この箱は閉じられています');
});

await t('送信と送り直しが並行しても、古いほうが新しい1件を消さない (R5 high#3)', async () => {
  const storage = makeStorage();
  let release;
  const gate = new Promise((r) => { release = r; });
  const sent = [], accepted = new Set();
  let firstCall = true;
  const post = async (body) => {
    sent.push(body.request_id);
    if (firstCall) { firstCall = false; await gate; throw new Error('indeterminate'); }
    accepted.add(body.request_id);
    return { ok: true, placementId: accepted.size };
  };
  const q = createPlaceQueue({ post, storage, newId: () => `req-${++seq}`, wait: async () => {}, retryWaitMs: 0 });
  const slow = q.send({ body: bodyOf() });     // 1件目 (gate で止まる)
  const alsoSlow = q.flush();                  // 直列化されるので 1件目の完了待ちになる
  release();
  await slow; await alsoSlow;
  const stuck = storage.raw();
  assert.ok(stuck, '未確定の1件は残っている');
  await q.ack(stuck.requestId);                // 人が確認して消した体にする
  const fresh = await q.send({ body: bodyOf({ row_id: 12 }) });
  assert.equal(fresh.record.status, 'resolved');
  assert.equal(storage.raw().requestId, fresh.record.requestId, '新しい1件が残っている');
  await q.ack('req-999');                      // 他人の requestId では消えない (CAS)
  assert.equal(storage.raw().requestId, fresh.record.requestId);
});

await t('新規送信は1回だけ送り直す。裏の flush は1回だけ試す', async () => {
  const server = makeServer({ lose: 1 }), storage = makeStorage();
  const q = mk(server, storage);
  const out = await q.send({ body: bodyOf() });
  assert.equal(out.record.status, 'resolved', '1回落ちても新規送信はその場で送り直す');
  assert.equal(server.sent.length, 2);
  const server2 = makeServer({ lose: 3 }), storage2 = makeStorage();   // send の2回 + flush の1回
  const q2 = mk(server2, storage2);
  await q2.send({ body: bodyOf() });                 // 2回とも落ちる → sending のまま
  assert.equal(storage2.raw().status, 'sending');
  const before = server2.sent.length;
  assert.equal(await q2.flush(), null, 'まだ確定しない');
  assert.equal(server2.sent.length, before + 1, 'flush の再送は1回だけ (画面更新のたびに叩きすぎない)');
});

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed === 0 ? 0 : 1);
