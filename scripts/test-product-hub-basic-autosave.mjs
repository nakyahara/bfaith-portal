import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * 商品登録 (product-hub) — 基本情報の「入力を確定したら即保存」(2026-09-21 中原さん)
 *
 * 実行: node scripts/test-product-hub-basic-autosave.mjs
 *
 * 明示保存の欄は「ボタンを押すまで DB に行かない」ため、読み直しで打った値が消えていた
 * (#1397 で退避と書き戻しの関所を入れた)。この PR は**未保存でいる時間そのもの**を短くする。
 *
 * 見るところ:
 *   ① 送ったキーだけ更新し、送らなかった欄は変えない (部分保存)
 *   ② 保存できた値を返す — 画面はこれを「ここまで保存できた」の基準にする。
 *      売価は整数に丸められ、文字列は trim されるので、画面の値を基準にすると
 *      「未保存」の印がいつまでも消えない
 *   ③ 何を変えたかを履歴に残す (欄ごとに 1 行増えるので、「updated」だけでは読めない)
 *   ④ 空の商品名はサーバーが既存値で受け流す → 画面側は送らない (食い違わせない)
 *   ⑤ 画面: 確定した欄だけを送る / 失敗しても打った値を戻さない / 保存後の値で基準を進める /
 *      続けて確定しても順番どおりに送る
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import vm from 'vm';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ph-autosave-test-'));
}

let pass = 0, fail = 0;
const ok = (c, l, d = '') => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l} ${d}`); } };

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();   // product-hub の DB は NE ミラーを引くので先に用意する
const { getDB } = await import('../apps/product-hub/db.js');
const { default: phRouter } = await import('../apps/product-hub/router.js');

const db = getDB();

// ─── API を直に呼ぶ (express は起こさない) ───
const layerOf = (method, routePath) => {
  const l = phRouter.stack.find((x) => x.route && x.route.path === routePath && x.route.methods[method]);
  if (!l) throw new Error(`ルートが見つかりません: ${method} ${routePath}`);
  return l.route.stack[0].handle;
};
const saveBasic = layerOf('post', '/api/drafts/:id');

async function callSave(draftId, body) {
  const req = {
    params: { id: String(draftId) },
    body,
    session: { email: 'tester@example.com', role: 'admin' },
    headers: {},
  };
  let out = null, code = 200;
  const res = {
    status(c) { code = c; return this; },
    json(j) { out = j; return this; },
  };
  await saveBasic(req, res, (e) => { throw e || new Error('next が呼ばれました'); });
  return { code, json: out };
}

const rowOf = (id) => db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
const lastUpdatedEvent = (id) => db.prepare(
  `SELECT * FROM draft_events WHERE draft_id = ? AND event = 'updated' ORDER BY id DESC LIMIT 1`).get(id);

db.prepare(`INSERT INTO product_drafts (ne_code, name, price, memo, asin, created_by)
  VALUES ('AUTOSAVE-1', '元の商品名', 1000, '元のメモ', 'B00OLD', 'test')`).run();
const draftId = db.prepare(`SELECT id FROM product_drafts WHERE ne_code = 'AUTOSAVE-1'`).get().id;

console.log('\n── ① 部分保存: 送ったキーだけ更新する ──');
{
  const r = await callSave(draftId, { price: '1980' });
  const row = rowOf(draftId);
  ok(r.json?.ok === true, 'ok を返す');
  ok(row.price === 1980, '送った売価は保存される', String(row.price));
  ok(row.name === '元の商品名' && row.memo === '元のメモ' && row.asin === 'B00OLD',
    '送らなかった欄は変わらない', JSON.stringify({ name: row.name, memo: row.memo, asin: row.asin }));
}

console.log('\n── ② 保存できた値を返す (画面の基準) ──');
{
  // 売価は整数に丸める。画面の値 ('1980.4') を基準にすると「未保存」が消えなくなる
  const r = await callSave(draftId, { price: '1980.4' });
  ok(r.json?.saved?.price === 1980, '売価は丸めた後の値を返す', JSON.stringify(r.json?.saved));
  const r2 = await callSave(draftId, { memo: '  前後に空白  ' });
  ok(r2.json?.saved?.memo === '前後に空白', '文字列は trim した後の値を返す', JSON.stringify(r2.json?.saved));
  ok(rowOf(draftId).memo === '前後に空白', '返した値は DB の値と同じ');
  const r3 = await callSave(draftId, { asin: '' });
  ok(r3.json?.saved?.asin === null && rowOf(draftId).asin === null,
    '空にした欄は null を返す (画面では空欄)', JSON.stringify(r3.json?.saved));
  const r4 = await callSave(draftId, { tax_rate: ' 8% ' });
  ok(r4.json?.saved?.tax_rate === '8%', '税率も trim した後の値を返す', JSON.stringify(r4.json?.saved));
  ok(r4.json?.saved?.price === 1980, '送っていない欄も「いまの保存値」を返す (基準がずれない)');
}

console.log('\n── ③ 何を変えたかを履歴に残す ──');
{
  await callSave(draftId, { price: '2500' });
  const ev = lastUpdatedEvent(draftId);
  ok(ev && ev.detail === '売価', '変えた欄の名前が残る', JSON.stringify(ev && ev.detail));
  await callSave(draftId, { price: '2500' });
  const ev2 = lastUpdatedEvent(draftId);
  ok(ev2 && ev2.detail === null, '同じ値なら「変えた欄」は空 (履歴が水増しされない)', JSON.stringify(ev2 && ev2.detail));
  await callSave(draftId, { name: '新しい商品名', memo: '新しいメモ' });
  const ev3 = lastUpdatedEvent(draftId);
  ok(ev3 && ev3.detail === '商品名 / メモ', '複数まとめて変えたら並べる', JSON.stringify(ev3 && ev3.detail));
}

console.log('\n── ④ 空の商品名は既存値のまま (画面側は送らない) ──');
{
  const r = await callSave(draftId, { name: '   ' });
  ok(rowOf(draftId).name === '新しい商品名', '空の商品名では上書きしない');
  ok(r.json?.saved?.name === '新しい商品名', '返す値も既存値 (画面が「保存された」と誤解しない)',
    JSON.stringify(r.json?.saved?.name));
}

console.log('\n── ⑤ 画面: 確定した欄だけを送る ──');
{
  const src = fs.readFileSync(path.join(__dirname, '..', 'apps', 'product-hub', 'views', 'detail.ejs'), 'utf8');
  const start = src.indexOf('  function initBasicAutoSave() {');
  const end = src.indexOf('  // ここまでが「基本情報の即保存」の切り出し範囲', start);
  const chunk = start >= 0 && end > start ? src.slice(start, end) : '';
  ok(chunk.length > 800 && !chunk.includes('<%'), 'detail.ejs から initBasicAutoSave を切り出せる', `len=${chunk.length}`);

  // 保存の列と読み直しの関所も**本物を**動かす (テスト用に書き直すと、本物とずれても気づけない)
  const qStart = src.indexOf('  var phSaveTail = Promise.resolve();');
  const qEnd = src.indexOf('  // ここまでが「保存の列と読み直し」の切り出し範囲', qStart);
  const queue = qStart >= 0 && qEnd > qStart ? src.slice(qStart, qEnd) : '';
  ok(queue.includes('function phEnqueueSave') && queue.includes('function reloadSafely') && !queue.includes('<%'),
    'detail.ejs から「保存の列と読み直し」を切り出せる', `len=${queue.length}`);

  const makeEl = (value, tab) => {
    const el = {
      value, type: 'text', className: '', textContent: '', title: '', checked: false,
      style: {}, children: [], dataset: {},
      _classes: new Set(), _handlers: {},
      closest: (sel) => (sel === '.tab-panel' ? { id: tab || 'tab-basic' } : null),
      replaceChildren: () => { el.children.length = 0; },
      dispatchEvent: (ev) => { (el._handlers[ev.type] || []).forEach((fn) => fn(ev)); return true; },
      classList: {
        add: (c) => el._classes.add(c), remove: (c) => el._classes.delete(c),
        contains: (c) => el._classes.has(c),
        toggle: (c, on) => { if (on) el._classes.add(c); else el._classes.delete(c); },
      },
      addEventListener: (t, fn) => { (el._handlers[t] = el._handlers[t] || []).push(fn); },
      insertAdjacentElement: (_p, node) => { el.children.push(node); return node; },
      appendChild: (node) => { el.children.push(node); return node; },
      _change: () => (el._handlers.change || []).forEach((fn) => fn({ type: 'change' })),
      _mark: () => (el.children[0] ? el.children[0].textContent : ''),
      _bad: () => !!(el.children[0] && el.children[0]._classes.has('bad')),
    };
    return el;
  };
  function harness(values, responder) {
    const els = new Map();
    for (const [id, v] of Object.entries(values)) els.set(id, makeEl(v));
    const posts = [];
    const savedCalls = [];
    const stashCalls = [];
    const alerts = [];
    const ctx = {
      document: { getElementById: (id) => els.get(id) || null, createElement: () => makeEl(''), activeElement: null },
      post: async (url, body) => { posts.push(body); return responder(body, posts.length); },
      BASE: '/ph',
      phKeep: {
        savedValue: (id, v) => savedCalls.push(`${id}=${v}`),
        stash: (opts) => {
          const inf = (opts && opts.inflight) || {};
          stashCalls.push(Object.keys(inf).map((k) => k + '=' + inf[k]).join(','));
          return true;
        },
        mute: () => {},
      },
      updateTabBadges: () => {},
      alert: (m) => alerts.push(String(m)),
      Date, Promise, console,
    };
    ctx.location = { reload: () => { ctx._reloaded = (ctx._reloaded || 0) + 1; } };
    ctx.setTimeout = (fn, ms) => { ctx._timers.push({ fn, ms }); return ctx._timers.length; };
    ctx._timers = [];
    vm.createContext(ctx);
    new vm.Script(`${queue}\n${chunk}\ninitBasicAutoSave();`, { filename: 'basicAutoSave' }).runInContext(ctx);
    return { els, posts, savedCalls, stashCalls, alerts, ctx };
  }
  const settle = async (h) => { for (let i = 0; i < 8; i++) await Promise.resolve(h.ctx.phBasicSaveChain); };

  // 売価を打って確定 → その欄だけが送られる
  {
    const h = harness({ 'f-price': '1980', 'f-name': '商品A', 'f-memo': '', 'f-asin': '', 'y-tax': '' },
      () => ({ ok: true, saved: { price: 1980 } }));
    h.els.get('f-price')._change();
    await settle(h);
    ok(JSON.stringify(h.posts) === '[{"price":"1980"}]', '確定した欄だけを送る', JSON.stringify(h.posts));
    ok(h.savedCalls.join(',') === 'f-price=1980', '保存できた値で基準を進める', h.savedCalls.join(','));
    ok(h.els.get('f-price')._mark().includes('保存しました'), 'その欄のそばに「保存しました」と出す', h.els.get('f-price')._mark());
    ok(h.els.get('f-price').dataset.autosave === '1' && h.els.get('f-name').dataset.autosave === '1',
      '即保存の欄には印を付ける (画面で見て分かる / 配線できたことの確認にもなる)');
  }

  // 🚨 サーバーが丸めた値を画面にも入れる (基準と画面が食い違うと「未保存」が消えない)
  {
    const h = harness({ 'f-price': '1980.4', 'f-name': '商品A' }, () => ({ ok: true, saved: { price: 1980 } }));
    h.els.get('f-price')._change();
    await settle(h);
    ok(h.els.get('f-price').value === '1980', '丸められた値を画面にも反映する', h.els.get('f-price').value);
    ok(h.savedCalls.join(',') === 'f-price=1980', '基準も保存後の値', h.savedCalls.join(','));
  }

  // 🚨 保存に失敗しても打った値は戻さない (人の入力を消さない)
  {
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' }, () => ({ ok: false, error: 'DB エラー' }));
    h.els.get('f-price')._change();
    await settle(h);
    ok(h.els.get('f-price').value === '1980', '失敗しても打った値を戻さない', h.els.get('f-price').value);
    ok(h.savedCalls.length === 0, '失敗したら基準を進めない (未保存の印が残る)');
    ok(h.els.get('f-price')._bad() && h.els.get('f-price')._mark().includes('基本情報を保存'),
      '赤字で「基本情報を保存」を案内する', h.els.get('f-price')._mark());
  }

  // 通信エラー (例外) でも同じ
  {
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' }, () => { throw new Error('network'); });
    h.els.get('f-price')._change();
    await settle(h);
    ok(h.els.get('f-price').value === '1980' && h.savedCalls.length === 0, '通信エラーでも値を戻さず基準も進めない');
    ok(h.els.get('f-price')._bad(), '赤字で知らせる');
  }

  // 🚨 空の商品名は送らない (サーバーが既存値で受け流すため、画面と DB が食い違う)
  {
    const h = harness({ 'f-name': '   ', 'f-price': '1000' }, () => ({ ok: true, saved: {} }));
    h.els.get('f-name')._change();
    await settle(h);
    ok(h.posts.length === 0, '空の商品名は送らない', JSON.stringify(h.posts));
    ok(h.els.get('f-name')._bad() && h.els.get('f-name')._mark().includes('空にできません'),
      '空にできないと知らせる', h.els.get('f-name')._mark());
  }

  // 🚨 続けて確定しても順番どおりに送る (後から出した保存が先に着いて古い値で上書きしない)
  {
    const order = [];
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' }, async (body, n) => {
      order.push('start:' + Object.keys(body)[0] + n);
      await new Promise((r) => setTimeout(r, Object.keys(body)[0] === 'price' ? 20 : 0));
      order.push('end:' + Object.keys(body)[0] + n);
      return { ok: true, saved: body };
    });
    h.els.get('f-price')._change();
    h.els.get('f-name')._change();
    for (let i = 0; i < 20; i++) { await new Promise((r) => setTimeout(r, 5)); }
    ok(order.join(' ') === 'start:price1 end:price1 start:name2 end:name2',
      '1 本ずつ順番に送る (並行に飛ばさない)', order.join(' '));
  }

  // 🚨 応答を待っている間に打ち直したら、古い応答で画面を書き戻さない (Codex R1)
  {
    let release = null;
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' },
      (body) => new Promise((r) => { release = () => r({ ok: true, saved: { price: Number(body.price) } }); }));
    h.els.get('f-price')._change();            // 1980 を送った (応答待ち)
    await new Promise((r) => setTimeout(r, 5));
    h.els.get('f-price').value = '2500';       // 待っている間に打ち直した
    release();
    await new Promise((r) => setTimeout(r, 20));
    ok(h.els.get('f-price').value === '2500', '古い応答で打ち直した値を書き戻さない', h.els.get('f-price').value);
    ok(JSON.stringify(h.posts) === '[{"price":"1980"}]', '送ったのは確定した値だけ (再送しない)', JSON.stringify(h.posts));
    ok(h.savedCalls.join(',') === 'f-price=1980', '基準は保存できた 1980 (画面の 2500 は未保存のまま)', h.savedCalls.join(','));
    // 打ち直した分を確定すると、その値が送られる
    h.els.get('f-price')._change();
    await new Promise((r) => setTimeout(r, 5));
    release();
    await new Promise((r) => setTimeout(r, 20));
    ok(JSON.stringify(h.posts) === '[{"price":"1980"},{"price":"2500"}]', '確定すれば打ち直した値を送る', JSON.stringify(h.posts));
  }

  // 🚨 ボタンのまとめ保存と即保存が同じ列に並ぶ (並行に飛ぶと古い値が新しい値を上書きする)
  {
    const order = [];
    let release = null;
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' },
      (body) => new Promise((r) => { order.push('start:' + Object.keys(body)[0]); release = () => { order.push('end:' + Object.keys(body)[0]); r({ ok: true, saved: body }); }; }));
    h.els.get('f-price')._change();
    await new Promise((r) => setTimeout(r, 5));
    // ボタンの保存が同じ列に並ぶ (本物のボタンは切り出しの外なので、列に直接積んで確かめる)
    let buttonRan = false;
    h.ctx.phEnqueueSave(async () => { buttonRan = true; order.push('button'); });
    await new Promise((r) => setTimeout(r, 10));
    ok(buttonRan === false, 'あとから積んだ保存は、前の保存が終わるまで動かない', order.join(' '));
    release();
    await new Promise((r) => setTimeout(r, 20));
    ok(buttonRan === true && order.join(' ') === 'start:price end:price button',
      '列の順番どおりに走る', order.join(' '));
  }

  // 🚨 例外が出ても、次の保存は動く (列が切れたままにならない)
  {
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' }, () => ({ ok: true, saved: { price: 1980 } }));
    h.ctx.phEnqueueSave(async () => { throw new Error('わざと失敗'); });
    await new Promise((r) => setTimeout(r, 10));
    h.els.get('f-price')._change();
    await new Promise((r) => setTimeout(r, 20));
    ok(h.posts.length === 1, '前の保存が例外で終わっても、次の保存は送られる', JSON.stringify(h.posts));
  }

  // 🚨 読み直しは列の最後尾まで待つ。待っている間に増えた保存も待つ
  {
    const releases = [];
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' },
      (body) => new Promise((r) => releases.push(() => r({ ok: true, saved: body }))));
    h.els.get('f-price')._change();
    await new Promise((r) => setTimeout(r, 5));
    h.ctx.reloadSafely();
    await new Promise((r) => setTimeout(r, 10));
    ok(!h.ctx._reloaded, '保存が終わるまで読み直さない');
    // 待っている間にもう 1 件確定した
    h.els.get('f-name').value = '商品B';
    h.els.get('f-name')._change();
    releases.shift()();
    await new Promise((r) => setTimeout(r, 15));
    ok(!h.ctx._reloaded, '待っている間に増えた保存も待つ', String(h.ctx._reloaded));
    releases.shift()();
    await new Promise((r) => setTimeout(r, 15));
    ok(h.ctx._reloaded === 1, '列が空になったら読み直す', String(h.ctx._reloaded));
    ok(h.stashCalls.length === 1 && h.stashCalls[0] === '', '待ちきれた読み直しでは base を省かない', JSON.stringify(h.stashCalls));
  }

  // 🚨 待ちきれなかった (固まった) ときは、応答待ちの欄の base を省いて退避する
  {
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' }, () => new Promise(() => {}));   // 返らない
    h.els.get('f-price')._change();
    await new Promise((r) => setTimeout(r, 5));
    h.ctx.reloadSafely();
    await new Promise((r) => setTimeout(r, 10));
    ok(!h.ctx._reloaded, '待っている間は読み直さない');
    // 3 秒の打ち切りタイマーを手で動かす
    h.ctx._timers.forEach((t) => t.fn());
    await new Promise((r) => setTimeout(r, 5));
    ok(h.ctx._reloaded === 1, '固まっても読み直しは止めない', String(h.ctx._reloaded));
    ok(h.stashCalls.join(',') === 'f-price=1980',
      '応答待ちの欄は「送った値」も添えて退避する (自分の保存と他人の変更を見分ける)', JSON.stringify(h.stashCalls));
  }

  // ─── ここから: 本物の未保存ガードまでつないで、退避と書き戻しの結果を見る (Codex R2) ───
  // 🚨 phKeep をモックにしたままだと、「退避されたか」「戻ったか」を確かめられない。
  //    応答待ちのまま読み直したときの振る舞いは、ここでしか検出できない
  {
    const gStart = src.indexOf('  function initUnsavedGuard(KEY) {');
    const gEnd = src.indexOf('  // ここまでが「未保存ガード」の切り出し範囲', gStart);
    const guard = gStart >= 0 && gEnd > gStart ? src.slice(gStart, gEnd) : '';
    ok(guard.includes('function writeStash') && !guard.includes('<%'),
      'detail.ejs から未保存ガードを切り出せる', `len=${guard.length}`);

    const store = new Map();
    const session = {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => { store.set(k, String(v)); },
      removeItem: (k) => { store.delete(k); },
    };
    const tick = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise((r) => setTimeout(r, 2)); };

    /** 1 回ぶん「画面を開く」。dbValues = そのときサーバーから描かれた値 */
    function fullOpen(dbValues, responder) {
      const els = new Map();
      const zone = makeEl('');
      els.set('f-price', makeEl(dbValues['f-price'] === undefined ? '' : dbValues['f-price'], 'tab-basic'));
      els.set('f-name', makeEl(dbValues['f-name'] === undefined ? '商品A' : dbValues['f-name'], 'tab-basic'));
      els.set('save-basic-btn', makeEl(''));
      els.set('unsaved-zone', zone);
      const posts = [];
      const timers = [];
      const winHandlers = new Map();
      const ctx = {
        document: {
          getElementById: (id) => els.get(id) || null,
          createElement: () => makeEl(''),
          querySelectorAll: () => [],
          activeElement: null,
        },
        sessionStorage: session,
        window: { addEventListener: (t, fn) => { winHandlers.set(t, fn); } },
        Event: class { constructor(type) { this.type = type; } },
        post: async (url, body) => { posts.push(body); return responder(body, posts.length); },
        BASE: '/ph',
        updateTabBadges: () => {},
        alert: () => {},
        phKeep: null,
        location: { reload: () => { ctx._reloaded = (ctx._reloaded || 0) + 1; } },
        setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; },
        Date, Promise, console,
      };
      vm.createContext(ctx);
      new vm.Script(`${queue}\n${guard}\n${chunk}\nphKeep = initUnsavedGuard('ph-unsaved:9'); initBasicAutoSave();`,
        { filename: 'fullPage' }).runInContext(ctx);
      return {
        ctx, els, posts,
        val: (id) => els.get(id).value,
        type: (id, v) => { els.get(id).value = v; (els.get(id)._handlers.change || []).forEach((fn) => fn({ type: 'change' })); },
        set: (id, v) => { els.get(id).value = v; },
        reload: () => ctx.reloadSafely(),
        fireTimers: () => { const t = timers.splice(0); t.forEach((x) => x.fn()); },
        leave: () => { const fn = winHandlers.get('pagehide'); if (fn) fn(); },
        reloaded: () => ctx._reloaded || 0,
        banner: () => {
          const out = [];
          const walk = (el) => {
            if (!el) return;
            if (el.textContent) out.push(el.textContent);
            if (el.value) out.push(el.value);
            (el.children || []).forEach(walk);
          };
          zone.children.forEach(walk);
          return out.join(' | ');
        },
      };
    }

    // 🚨 応答を待っている間に元の値へ戻した — その意思が消えてはいけない
    {
      store.clear();
      let release = null;
      const a = fullOpen({ 'f-price': '1000' },
        (body) => new Promise((r) => { release = () => r({ ok: true, saved: { price: Number(body.price) } }); }));
      a.type('f-price', '1980');          // 確定 = 送信 (応答待ち)
      await tick();
      a.set('f-price', '1000');           // 待っている間に元へ戻した
      a.reload();                         // 別の操作で読み直し
      await tick();
      a.fireTimers();                     // 待ちきれず打ち切り
      await tick();
      ok(a.reloaded() === 1, '応答が返らなくても読み直しは進む');
      release();
      // サーバーには 1980 が入っている。読み直した画面は 1980 で描かれる
      const b = fullOpen({ 'f-price': '1980' }, () => ({ ok: true, saved: {} }));
      ok(b.val('f-price') === '1000', '🚨 応答待ちに元へ戻した値が、読み直しでも残る', b.val('f-price'));
    }

    // 🚨 応答を待っている間に**ほかの人**が変えていたら、自分の値で覆い隠さない
    {
      store.clear();
      const c = fullOpen({ 'f-price': '1000' }, () => new Promise(() => {}));   // 返らない
      c.type('f-price', '1980');
      await tick();
      c.set('f-price', '2500');
      c.reload();
      await tick();
      c.fireTimers();
      await tick();
      const d = fullOpen({ 'f-price': '3000' }, () => ({ ok: true, saved: {} }));   // 他の人が 3000 にした
      ok(d.val('f-price') === '3000', '🚨 ほかの人の変更を自分の値で隠さない', d.val('f-price'));
      ok(d.banner().includes('2500') && d.banner().includes('打っていた内容を見る'),
        '戻さなかった値は画面で見せる (拾い直せる)', d.banner());
    }

    // 自分の保存が通っていた場合は、打ち直した分がちゃんと戻る
    {
      store.clear();
      const e = fullOpen({ 'f-price': '1000' }, () => new Promise(() => {}));
      e.type('f-price', '1980');
      await tick();
      e.set('f-price', '2500');
      e.reload();
      await tick();
      e.fireTimers();
      await tick();
      const f = fullOpen({ 'f-price': '1980' }, () => ({ ok: true, saved: {} }));   // DB は自分が送った 1980
      ok(f.val('f-price') === '2500', '🚨 自分の保存のあとに打ち直した分は戻る', f.val('f-price'));
      ok(!f.banner().includes('戻さなかった'), '自分の保存を競合と言わない', f.banner());
    }

    // 🚨 通信エラーは「保存できたか分からない」。応答待ちに元へ戻した意思を消さない
    {
      store.clear();
      let fail = null;
      const h = fullOpen({ 'f-price': '1000' },
        () => new Promise((_r, reject) => { fail = () => reject(new Error('network')); }));
      h.type('f-price', '1980');
      await tick();
      h.set('f-price', '1000');   // 待っている間に元へ戻した
      fail();                     // 応答だけが落ちた (DB には入っているかもしれない)
      await tick();
      h.reload();
      await tick();
      h.fireTimers();
      await tick();
      const after = fullOpen({ 'f-price': '1980' }, () => ({ ok: true, saved: {} }));   // DB には入っていた
      ok(after.val('f-price') === '1000', '🚨 通信エラーのあとでも、元へ戻した値が残る', after.val('f-price'));
    }

    // 🚨 サーバーが丸めて保存した値を「ほかの人の変更」と間違えない
    {
      store.clear();
      const h = fullOpen({ 'f-price': '1000' }, () => new Promise(() => {}));   // 返らない
      h.type('f-price', '1980.4');   // サーバーは 1980 に丸める
      await tick();
      h.set('f-price', '2500');
      h.reload();
      await tick();
      h.fireTimers();
      await tick();
      const after = fullOpen({ 'f-price': '1980' }, () => ({ ok: true, saved: {} }));
      ok(after.val('f-price') === '2500', '🚨 丸められた自分の保存を競合と言わない', after.val('f-price'));
      ok(!after.banner().includes('戻さなかった'), '競合のバナーも出さない', after.banner());
    }

    // 🚨 戻さなかった長い入力は、切り詰めずに全文を残す (退避は 1 回で使い切るため)
    {
      store.clear();
      const long = 'あ'.repeat(120);
      const h = fullOpen({ 'f-name': '商品A' }, () => new Promise(() => {}));
      h.type('f-name', long);
      await tick();
      h.reload();
      await tick();
      h.fireTimers();
      await tick();
      const after = fullOpen({ 'f-name': 'ほかの人が付けた名前' }, () => ({ ok: true, saved: {} }));
      ok(after.val('f-name') === 'ほかの人が付けた名前', 'ほかの人の変更は残す', after.val('f-name'));
      ok(after.banner().includes(long), '打っていた内容は全文を残す (コピーして貼り直せる)',
        String(after.banner().length));
    }

    // 応答が返ってから読み直す場合は、これまでどおり (退避は要らない)
    {
      store.clear();
      const g = fullOpen({ 'f-price': '1000' }, (body) => ({ ok: true, saved: { price: Number(body.price) } }));
      g.type('f-price', '1980');
      await tick();
      g.reload();
      await tick();
      ok(g.reloaded() === 1 && store.size === 0, '保存が通っていれば退避するものが無い', String(store.size));
    }
  }

  // まとめ保存 (ボタン) も「送信中の欄と送った値」を記録する — 切り出しの外なので文面で確かめる
  ok(/doneSending = phSending\(basicSnap/.test(src), '「基本情報を保存」も送信中を記録する');
  ok(/const doneSending = phSending\(snap/.test(src), '「Yahoo!項目を保存」も送信中を記録する');
  ok(/doneSending\(\);/.test(src), '送信が終わったら記録を外す');

  // ステータスが下書きに戻ったら知らせる
  {
    const h = harness({ 'f-price': '1980', 'f-name': '商品A' },
      () => ({ ok: true, saved: { price: 1980 }, demoted: ['AIが参照できるURLがありません'] }));
    h.els.get('f-price')._change();
    await settle(h);
    ok(h.alerts.length === 1 && h.alerts[0].includes('下書き'), '自動差し戻しは黙って済ませない', JSON.stringify(h.alerts));
  }
}

console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'}: ${pass} ok / ${fail} ng`);
process.exitCode = fail === 0 ? 0 : 1;
