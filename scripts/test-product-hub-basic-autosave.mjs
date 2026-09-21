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

  const makeEl = (value) => {
    const el = {
      value, type: 'text', className: '', textContent: '', style: {}, children: [],
      _classes: new Set(), _handlers: {},
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
    const alerts = [];
    const ctx = {
      document: { getElementById: (id) => els.get(id) || null, createElement: () => makeEl(''), activeElement: null },
      post: async (url, body) => { posts.push(body); return responder(body, posts.length); },
      BASE: '/ph',
      phKeep: { savedValue: (id, v) => savedCalls.push(`${id}=${v}`) },
      updateTabBadges: () => {},
      alert: (m) => alerts.push(String(m)),
      phBasicSaveChain: null,
      Date, Promise, console,
    };
    vm.createContext(ctx);
    new vm.Script(`${chunk}\ninitBasicAutoSave();`, { filename: 'initBasicAutoSave' }).runInContext(ctx);
    return { els, posts, savedCalls, alerts, ctx };
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
