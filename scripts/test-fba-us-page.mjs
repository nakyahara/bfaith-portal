#!/usr/bin/env node
/**
 * 米国FBA在庫補充の画面 (views/fba-replenishment-us.ejs) の JS を、偽の document と fetch で実際に動かす試験。
 *   node scripts/test-fba-us-page.mjs
 * API の応答は本物の組み立て関数 (us-view.js / allocation.js) で作る = 形の取り違えを検出する (Codex #1452 R1 Low 2)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-us-page-'));
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);
const ejs = (await import('ejs')).default;
const { buildUsInventoryView } = await imp('apps/fba-replenishment-us/us-view.js');
const { computeUsAllocation } = await imp('apps/fba-replenishment-us/allocation.js');

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}\n     ${e.stack.split('\n').slice(0, 5).join('\n     ')}`); }
}

const html = await ejs.renderFile(path.join(root, 'views', 'fba-replenishment-us.ejs'), { username: 'u', displayName: '中原' });
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];

/** 画面を動かす。responses = { inventory: () => Promise<{status, body}>, allocation: ... } */
function mount(responses) {
  const els = new Map();
  const document = { getElementById: (id) => { if (!els.has(id)) els.set(id, { innerHTML: '', textContent: '', style: {}, disabled: false }); return els.get(id); } };
  const fetch = (url) => {
    const key = url.endsWith('/api/inventory') ? 'inventory' : url.endsWith('/api/allocation') ? 'allocation' : null;
    if (!key) throw new Error(`知らない URL: ${url}`);
    return responses[key]().then((r) => ({ status: r.status, json: async () => r.body }));
  };
  // eslint-disable-next-line no-new-func
  document.body = { appendChild: () => {} };
  document.createElement = () => ({ click: () => {}, remove: () => {} });
  const api = new Function('document', 'fetch', 'confirm', 'URL', `${script}\n;return { loadAll: loadAll, setStaQty: setStaQty, removeSta: removeSta, downloadSta: downloadSta };`)(
    document, (url, init) => (responses.fetch && /\/api\/sta-excel$/.test(url) ? responses.fetch(url, init) : fetch(url, init)), responses.confirm || (() => true), { createObjectURL: () => 'blob:x', revokeObjectURL: () => {} });
  return { el: (id) => document.getElementById(id), api };
}
const flush = async () => { for (let i = 0; i < 10; i++) await new Promise((r) => setTimeout(r, 0)); };
const ok = (body) => () => Promise.resolve({ status: 200, body: { ok: true, ...body } });
const ng = (status, message) => () => Promise.resolve({ status, body: { ok: false, message } });

// ── 本物の関数で API の応答を作る ──
const now = new Date('2026-09-25T03:00:00Z');
const rRow = (sku, over = {}) => ({ 'Merchant SKU': sku, 'Product Name': `name ${sku}`, FNSKU: 'X0' + sku, Available: '0', Working: '0', Shipped: '0', Receiving: '0', 'FC transfer': '0', 'FC Processing': '0', 'Customer Order': '0', Unfulfillable: '0', 'Units Sold Last 30 Days': '30', 'Recommended replenishment qty': '10', ...over });
const invPayload = (rows) => ({ last_attempt: null, file_errors: [], save_failure: null, latest: { business_date: '2026-09-25', reports: { restock: { ok: true, fetched_at: '2026-09-24T22:05:00Z', rows }, planning: { ok: false, rows: null } } } });
const mapping = (m) => (skus) => new Map(skus.map((s) => [s, m[s] || { route: 'none', components: [] }]));
const inventoryOf = (rows, m) => buildUsInventoryView(invPayload(rows), { now, resolveSkus: mapping(m) });
const allocOf = (view, over = {}) => computeUsAllocation({
  now, usRows: view.rows, usRestockFetchedAt: view.restock_fetched_at, usLastAttempt: view.last_attempt, usSaveFailure: view.save_failure, usDupKeys: view.dup_keys,
  jpRestock: [], jpTargetDaysOf: () => 60, jpMappings: [], jpExcluded: new Set(), warehouse: [],
  selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map() }, pending: { status: 'ok', byCode: new Map() },
  freshness: { jpRestockSourceAt: '2026-09-24 22:00:00', jpRestockSourceMissing: 0, warehouseUploadedAt: '2026-09-25 09:00:00' },
  ...over,
});
const master = (code, qty) => ({ route: 'master', components: [{ ne_code: code, qty }] });

await t('🚨 送るものが無いときの言い方: 判定できない SKU がある / 米国の SKU が無い / 全部足りている を分ける (Codex #1452 R1 Medium 1)', async () => {
  const v1 = inventoryOf([rRow('a'), rRow('b')], {});   // 両方 未登録 = 判定できない
  const p1 = mount({ inventory: ok(v1), allocation: ok(allocOf(v1)) }); await flush();
  assert.match(p1.el('sendCards').innerHTML, /判定できない SKU が 2 件あります/);
  assert.doesNotMatch(p1.el('sendCards').innerHTML, /送る必要のある SKU はありません/);
  const v2 = inventoryOf([], {});
  const p2 = mount({ inventory: ok(v2), allocation: ok(allocOf(v2)) }); await flush();
  assert.match(p2.el('sendCards').innerHTML, /米国の SKU がありません/);
  const v3 = inventoryOf([rRow('a', { Available: '100' })], { a: master('c', 1) });   // 100 日分ある
  const p3 = mount({ inventory: ok(v3), allocation: ok(allocOf(v3, { warehouse: [{ logizard_code: 'c', warehouse_available: 10 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) } })) }); await flush();
  assert.match(p3.el('sendCards').innerHTML, /全部足りているか、売れていません/);
});

await t('推奨のカード: 数・送ったあとの在庫日数 / 足りない理由は「日本の分と、先に配った米国 SKU の分」と残り・1 SKU の構成数を出す (言い切らない。Codex #1452 R1 Medium 4)', async () => {
  // c×20 と c×40 が取り合う。倉庫 700 = -20 が先に 30 個 (600 個) → 残り 100 → -40 は 2 個 (80 個)
  const v = inventoryOf([rRow('s-20', { 'Units Sold Last 30 Days': '10' }), rRow('s-40', { Available: '1', 'Units Sold Last 30 Days': '10' })], { 's-20': master('c', 20), 's-40': master('c', 40) });
  const a = allocOf(v, { warehouse: [{ logizard_code: 'c', warehouse_available: 700 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) } });
  const p = mount({ inventory: ok(v), allocation: ok(a) }); await flush();
  const h = p.el('sendCards').innerHTML;
  assert.match(h, /<span class="big">30<\/span><span class="unit">個 送る/);
  assert.match(h, /<span class="big">2<\/span>/);
  assert.match(h, /先に配った米国 SKU の分 \(600 個\)を引くと、c の残りが 100 個 \(この SKU は 1 個に 40 個使う\)/);
  assert.match(h, /送ったあと 90 日/);
  // 日本の分だけで足りないときは「先に配った」を言わない
  const v2 = inventoryOf([rRow('s-40', { 'Units Sold Last 30 Days': '10' })], { 's-40': master('c', 40) });
  const a2 = allocOf(v2, { warehouse: [{ logizard_code: 'c', warehouse_available: 30 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) } });
  const p2 = mount({ inventory: ok(v2), allocation: ok(a2) }); await flush();
  assert.match(p2.el('sendCards').innerHTML, /送れない[\s\S]*日本の分を引くと、c の残りが 30 個 \(この SKU は 1 個に 40 個使う\)/);
});

await t('在庫の行き先の棒: 全体 = 倉庫。日本の分が倉庫を超えたら、塗るのは倉庫の分まで・超えた分は文で (Codex #1452 R1 Low 1)', async () => {
  const v = inventoryOf([rRow('a')], { a: master('c', 1) });
  const a = allocOf(v, {
    warehouse: [{ logizard_code: 'c', warehouse_available: 100 }],
    selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 60]]) },   // 自社 60 日 = 120 個 > 倉庫 100
    pending: { status: 'ok', byCode: new Map([['c', 30]]) },
  });
  const p = mount({ inventory: ok(v), allocation: ok(a) }); await flush();
  const h = p.el('flows').innerHTML;
  const widths = [...h.matchAll(/class="s-[a-z]+" style="width:([\d.]+)%/g)].map((m) => Number(m[1]));
  assert.ok(Math.abs(widths.reduce((s, w) => s + w, 0) - 100) < 1e-9, `塗った幅の合計が 100% でない: ${widths}`);
  assert.deepEqual(widths, [30, 70], '出荷待ち 30 + 自社 70 (倉庫に収まる分) のはず');
  assert.match(h, /日本の分 150 個に対して倉庫は 100 個 = 日本の分だけで 50 個足りません/);
});

await t('🚨 米国のデータの軽い警告 (warn) も出す・札を黄色に・重複 SKU の行に札 (Codex #1452 R1 Medium 3)', async () => {
  const v = inventoryOf([rRow('dup', { Available: '0' }), rRow('DUP', { Available: '9' })], { dup: master('c', 1) });
  assert.ok(v.warnings.some((w) => w.level === 'warn'));
  const p = mount({ inventory: ok(v), allocation: ok(allocOf(v)) }); await flush();
  assert.match(p.el('alerts').innerHTML, /同じ SKU が 2 行/);
  assert.match(p.el('chips').innerHTML, /class="chip warn"><span class="dot"><\/span>米国 FBA 在庫/);
  assert.match(p.el('rows').innerHTML, /tag red" title="RESTOCK に同じ SKU が 2 行/);
});

await t('🚨 読み直し中は前の表を消す (新しい推奨と古い在庫を混ぜない)・片方だけ失敗したらその理由を出す (Codex #1452 R1 Medium 2)', async () => {
  const v = inventoryOf([rRow('a', { Available: '5' })], { a: master('c', 1) });
  let release;
  const slowInv = () => new Promise((r) => { release = () => r({ status: 200, body: { ok: true, ...v } }); });
  let first = true;
  const p = mount({ inventory: () => (first ? (first = false, Promise.resolve({ status: 200, body: { ok: true, ...v } })) : slowInv()), allocation: ok(allocOf(v)) });
  await flush();
  assert.match(p.el('rows').innerHTML, /<td class="r num">5<\/td>/);
  p.api.loadAll(); await flush();   // 2 回目: 在庫はまだ返らない・配分は返る
  assert.match(p.el('rows').innerHTML, /読み込み中/);
  assert.doesNotMatch(p.el('rows').innerHTML, /<td class="r num">5<\/td>/, '前の表が残っている');
  assert.equal(p.el('reloadBtn').disabled, true);
  release(); await flush();
  assert.match(p.el('rows').innerHTML, /<td class="r num">5<\/td>/);
  assert.equal(p.el('reloadBtn').disabled, false);
  const q = mount({ inventory: ok(v), allocation: ng(503, '日本の FBA在庫補充の DB がまだ準備中です') }); await flush();
  assert.match(q.el('alerts').innerHTML, /日本優先の配分を出せませんでした: 日本の FBA在庫補充の DB がまだ準備中です/);
  assert.match(q.el('sendCards').innerHTML, /—/);
  assert.match(q.el('rows').innerHTML, /<td class="r num">5<\/td>/, '在庫の表まで消えている');
});

await t('参考のときはカードに「参考」・理由を 1 つの枠に / 取れなかった値は 0 ではなく「—」 / SKU・理由の文字は逃がす (XSS)', async () => {
  const v = inventoryOf([rRow('<img src=x onerror=alert(1)>', { Available: '' })], { '<img src=x onerror=alert(1)>': master('c', 1) });
  const a = allocOf(v, { warehouse: [{ logizard_code: 'c', warehouse_available: 900 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) }, pending: { status: 'inbound_stale', byCode: new Map(), inbound_last_synced_at: '2026-08-05 14:53:14' } });
  const p = mount({ inventory: ok(v), allocation: ok(a) }); await flush();
  assert.match(p.el('alerts').innerHTML, /いまの数字は参考です[\s\S]*日本の納品実績 \(Amazon の shipment\) が 2 日以上古い[\s\S]*2026-08-05 14:53:14/);
  assert.doesNotMatch(p.el('alerts').innerHTML, /inbound_stale/, '内部の状態名を出している');
  assert.match(p.el('rows').innerHTML, /<td class="r unknown" title="[^"]*販売可能: 空">—<\/td>/);
  for (const id of ['rows', 'sendCards', 'flows', 'otherPills']) assert.doesNotMatch(p.el(id).innerHTML, /<img src=x/, `${id} に生の HTML`);
});

await t('🚨 構成が分からず影響先も分からない日本の SKU があれば、参考でなくてもカードより上に「米国に回せる数は多めに出ている」を出す (Codex #1452 R2 Medium) / 入荷中の「—」に理由 (R2 Low)', async () => {
  const v = inventoryOf([rRow('a', { Shipped: '' })], { a: master('c', 1) });
  const a = allocOf(v, {
    warehouse: [{ logizard_code: 'c', warehouse_available: 200 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) },
    jpRestock: [{ amazon_sku: 'jp-unknown', units_sold_30d: 100, fba_available: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 }],
  });
  assert.equal(a.reference, false);
  const p = mount({ inventory: ok(v), allocation: ok(a) }); await flush();
  assert.match(p.el('alerts').innerHTML, /構成が分からない日本の SKU が <b>1 件<\/b>[\s\S]*米国に回せる数は多めに出ています[\s\S]*jp-unknown/);
  assert.match(p.el('rows').innerHTML, /<td class="r unknown" title="輸送中: 空">—<\/td>/);
  // 影響先が分かって「判定できない」にした SKU だけなら、この注意は出さない
  const b = allocOf(v, {
    warehouse: [{ logizard_code: 'c', warehouse_available: 200 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) },
    jpRestock: [{ amazon_sku: 'jp-set', units_sold_30d: 100, fba_available: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 }],
    jpMappings: [{ amazon_sku: 'jp-set', ne_code: 'c', is_set: 1, set_components: '[]' }],
  });
  const q = mount({ inventory: ok(v), allocation: ok(b) }); await flush();
  assert.doesNotMatch(q.el('alerts').innerHTML, /多めに出ています/);
});
await t('件数は一覧 (先頭 200 件) からではなく切り詰め前の数で: 201 件すべて判定不能にしたもの → 注意なし / 混在 → 影響先の分からない数だけ (Codex #1452 R3 Low)', async () => {
  const v = inventoryOf([rRow('a')], { a: master('c', 1) });
  const base = { warehouse: [{ logizard_code: 'c', warehouse_available: 200 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) } };
  const sets = (n, prefix) => Array.from({ length: n }, (_, i) => `${prefix}${i}`);
  const blockedSkus = sets(201, 'set-');
  const a = allocOf(v, { ...base,
    jpRestock: blockedSkus.map((s) => ({ amazon_sku: s, units_sold_30d: 1, fba_available: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 })),
    jpMappings: blockedSkus.map((s) => ({ amazon_sku: s, ne_code: 'c', is_set: 1, set_components: '[]' })) });
  assert.deepEqual([a.unattributed_jp_count, a.unattributed_jp.length, a.unattributed_jp_loose_count, a.unattributed_jp_blocked_count], [201, 200, 0, 201]);
  const p = mount({ inventory: ok(v), allocation: ok(a) }); await flush();
  assert.doesNotMatch(p.el('alerts').innerHTML, /多めに出ています/);
  const loose = sets(3, 'free-');
  const b = allocOf(v, { ...base,
    jpRestock: [...blockedSkus, ...loose].map((s) => ({ amazon_sku: s, units_sold_30d: 1, fba_available: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 })),
    jpMappings: blockedSkus.map((s) => ({ amazon_sku: s, ne_code: 'c', is_set: 1, set_components: '[]' })) });
  const q = mount({ inventory: ok(v), allocation: ok(b) }); await flush();
  assert.match(q.el('alerts').innerHTML, /構成が分からない日本の SKU が <b>3 件<\/b>/);
});

await t('STA 用 Excel の欄: 推奨のある SKU を推奨数で入れる → 数を直す・外す → 送る行だけ POST / 数がおかしければ送らない / サーバの断りの理由を出す', async () => {
  const v = inventoryOf([rRow('s-20', { 'Units Sold Last 30 Days': '10' }), rRow('s-40', { Available: '1', 'Units Sold Last 30 Days': '10' }), rRow('other', { Available: '99' })], { 's-20': master('c', 20), 's-40': master('c', 40), other: master('d', 1) });
  const a = allocOf(v, { warehouse: [{ logizard_code: 'c', warehouse_available: 700 }, { logizard_code: 'd', warehouse_available: 10 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0], ['d', 0]]) } });
  const posts = [];
  let reply = () => Promise.resolve({ ok: true, status: 200, headers: { get: () => 'attachment; filename=US_STA_Manifest_2026-09-26.xlsx' }, blob: async () => ({}) });
  const p = mount({ inventory: ok(v), allocation: ok(a), fetch: (url, init) => { posts.push([url, JSON.parse(init.body)]); return reply(); } });
  await flush();
  const rows = p.el('staRows').innerHTML;
  assert.match(rows, /s-20[\s\S]*value="30"[\s\S]*s-40[\s\S]*value="2"/, '推奨数が入っていない');
  assert.doesNotMatch(rows, /other/, '推奨の無い SKU が最初から入っている');
  assert.match(p.el('staAdd').innerHTML, /<option value="other">other<\/option>/, '足せる SKU に出ていない');
  p.api.setStaQty(0, '25'); p.api.removeSta(1);
  p.api.downloadSta(); await flush();
  assert.deepEqual(posts, [['/apps/fba-replenishment-us/api/sta-excel', { items: [{ sku: 's-20', qty: 25 }] }]]);
  assert.match(p.el('staMsg').innerHTML, /US_STA_Manifest_2026-09-26\.xlsx \(1 SKU・25 個\) を作りました/);
  p.api.setStaQty(0, '0'); p.api.downloadSta(); await flush();
  assert.equal(posts.length, 1, '数がおかしいのに送った');
  assert.match(p.el('staMsg').innerHTML, /1 以上の整数にしてください: s-20/);
  reply = () => Promise.resolve({ ok: false, status: 400, json: async () => ({ ok: false, message: '1 行目 (s-20): 米国の RESTOCK に無い SKU' }) });
  p.api.setStaQty(0, '3'); p.api.downloadSta(); await flush();
  assert.match(p.el('staMsg').innerHTML, /Excel を作れませんでした: 1 行目 \(s-20\): 米国の RESTOCK に無い SKU/);
});
await t('STA 用 Excel: 参考のときは確認を出し、やめたら送らない', async () => {
  const v = inventoryOf([rRow('s-20', { 'Units Sold Last 30 Days': '10' })], { 's-20': master('c', 20) });
  const a = allocOf(v, { warehouse: [{ logizard_code: 'c', warehouse_available: 700 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) }, pending: { status: 'inbound_stale', byCode: new Map() } });
  const posts = [];
  const p = mount({ inventory: ok(v), allocation: ok(a), confirm: () => false, fetch: (url, init) => { posts.push(url); return Promise.reject(new Error('no')); } });
  await flush();
  p.api.downloadSta(); await flush();
  assert.equal(posts.length, 0);
});

await t('🚨 STA 用 Excel の作成中: 数を直しても・外しても・読み直しても効かない。完了の表示は送った時点の数 (Codex #1473 R1 Medium 1)', async () => {
  const v = inventoryOf([rRow('s-20', { 'Units Sold Last 30 Days': '10' })], { 's-20': master('c', 20) });
  const a = allocOf(v, { warehouse: [{ logizard_code: 'c', warehouse_available: 700 }], selfShip: { status: 'ok', as_of: '2026-09-24', map: new Map([['c', 0]]) } });
  const posts = []; let release;
  const p = mount({ inventory: ok(v), allocation: ok(a), fetch: (url, init) => { posts.push(JSON.parse(init.body)); return new Promise((r) => { release = () => r({ ok: true, status: 200, headers: { get: () => 'attachment; filename=US_STA_Manifest_2026-09-26.xlsx' }, blob: async () => ({}) }); }); } });
  await flush();
  p.api.setStaQty(0, '10');
  p.api.downloadSta(); await flush();
  assert.deepEqual(posts, [{ items: [{ sku: 's-20', qty: 10 }] }]);
  p.api.setStaQty(0, '99'); p.api.removeSta(0); p.api.loadAll(); p.api.downloadSta(); await flush();
  assert.equal(posts.length, 1, '作成中に 2 回目を送った');
  assert.equal(p.el('reloadBtn').disabled, true);
  assert.match(p.el('staRows').innerHTML, /value="10"[^>]*disabled/, '作成中に入力できる');
  release(); await flush();
  assert.match(p.el('staMsg').innerHTML, /\(1 SKU・10 個\) を作りました/);
  assert.match(p.el('staRows').innerHTML, /value="10"/, '作成中の編集が効いている');
  assert.equal(p.el('reloadBtn').disabled, false);
});

console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
