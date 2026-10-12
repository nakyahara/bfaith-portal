#!/usr/bin/env node
/**
 * test-fba-ne-unregistered.mjs — FBA 補充の NE 受注 CSV・手で足す画面に「NE にまだ無い商品コード」を入れない (#1694 Codex R1 Medium・2026-10-12)
 *
 * 確かめること:
 *   1 NE の商品の写し (warehouse-mirror.db の mirror_products) を読む・空 / 表が無い = 読めない (fail-closed)・コードは前後の空白と大文字小文字を除いて比べる
 *   2 /api/export-ne-csv: 構成品に写しに無いコードがある SKU だけ出さない (警告「NE に未登録の商品コード …」・ほかの SKU は出す = 1 つのために全部を捨てない)・
 *     セットの構成の一部だけが無い = その SKU ごと外す (一部だけ出すと数の合わない伝票になる)・全部が無い = 400 (CSV なし)・写しが読めない = 503 (CSV なし)
 *   3 /api/ne-unregistered?sku= (手で足す画面の確かめ): 無いコード・対応なし・写しが読めない = 503
 *   4 画面 (views/fba-replenishment.ejs の addManualItem) が確かめの API を呼び、無いコード・確かめられないときは足さない
 * 使い方: node scripts/test-fba-ne-unregistered.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-ne-unreg-'));
delete process.env.FBA_SKU_MAPPING_SOURCE;   // sheet = sku_mapping の表 (試験で入れる)
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ok  ${name}`); }
  catch (e) { fail++; console.log(`  NG  ${name}\n      ${e.stack.split('\n').slice(0, 5).join('\n      ')}`); }
}

const NR = await imp('apps/fba-replenishment/ne-registered.js');
const { initMirrorDB, getMirrorDB } = await imp('apps/warehouse-mirror/db.js');
initMirrorDB();
const mdb = getMirrorDB();
const putProducts = (codes) => {
  mdb.exec('DELETE FROM mirror_products');
  const st = mdb.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価状態, updated_at) VALUES (?, ?, '単品', 'COMPLETE', '2030-01-01')`);
  for (const c of codes) st.run(c, `商品 ${c}`);
};

console.log('写しを読む');
await t('[1] 写しのコード = 前後の空白・大文字小文字を除く / 空 = 読めない / 表が無い = 読めない (fail-closed)', async () => {
  putProducts(['ne001', ' NE002 ', 'set01']);
  const ne = NR.readNeProductCodes(mdb);
  assert.equal(ne.ok, true);
  assert.deepEqual([...ne.codes].sort(), ['ne001', 'ne002', 'set01']);
  assert.deepEqual(NR.missingNeCodes(ne, ['NE001', 'ne002 ', 'new-draft', 'new-draft', 'Set01']), ['new-draft'], '重なりは 1 つ');
  putProducts([]);
  const empty = NR.readNeProductCodes(mdb);
  assert.equal(empty.ok, false); assert.match(empty.error, /空/);
  const broken = NR.readNeProductCodes({ prepare: () => { throw new Error('no such table: mirror_products'); } });
  assert.equal(broken.ok, false); assert.match(broken.error, /読めません/);
  // 構成の展開 = NE 受注 CSV と同じ (set_components が無ければ ne_code)
  assert.deepEqual(NR.componentCodesOf({ ne_code: 'a', set_components: null }), ['a']);
  assert.deepEqual(NR.componentCodesOf({ ne_code: 'a', set_components: JSON.stringify([{ ne_code: 'x', qty: 2 }, { ne_code: 'y', qty: 1 }]) }), ['x', 'y']);
  assert.deepEqual(NR.componentCodesOf({ ne_code: 'a', set_components: '{壊れた' }), ['a']);
});

// ── 本物の router を express に載せる ──
const express = (await import('express')).default;
const { default: router } = await imp('apps/fba-replenishment/router.js');
const DB = await imp('apps/fba-replenishment/db.js');
const app = express();
app.use('/apps/fba-replenishment', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/fba-replenishment`;
// DB の初期化を待つ (router は import した時に initDb を始める)
for (let i = 0; i < 200; i++) { const r = await fetch(B + '/api/ne-unregistered?sku=x'); if (r.status !== 503 || !(await r.text()).includes('DB初期化中')) break; await new Promise((res) => setTimeout(res, 50)); }
DB.upsertSkuMappings([
  { amazon_sku: 'pr_ok', product_name: 'NE にある単品', ne_code: 'ne001' },
  { amazon_sku: 'pr_draft', product_name: 'まだ NE に無い単品', ne_code: 'new-draft' },
  { amazon_sku: 'pr_set_half', product_name: '構成の一部が NE に無いセット', ne_code: 'ne001', is_set: true, set_components: [{ ne_code: 'ne001', qty: 2 }, { ne_code: 'new-pend', qty: 1 }] },
  { amazon_sku: 'pr_set_ok', product_name: 'NE にあるセット', ne_code: 'ne002', is_set: true, set_components: [{ ne_code: 'ne002', qty: 3 }] },
]);
const exportCsv = (items) => fetch(B + '/api/export-ne-csv', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ items }) });
const csvText = async (r) => new TextDecoder('shift_jis').decode(await r.arrayBuffer());
const warningsOf = (r) => JSON.parse(decodeURIComponent(r.headers.get('X-NE-Warnings') || '%5B%5D'));

console.log('NE 受注 CSV');
await t('[2] NE に無いコードを含む SKU だけ出さない (警告)・ほかは出す・セットの一部だけ無い = SKU ごと外す', async () => {
  putProducts(['ne001', 'NE002']);
  const r = await exportCsv([{ amazon_sku: 'pr_ok', ship_qty: 2 }, { amazon_sku: 'pr_draft', ship_qty: 5 }, { amazon_sku: 'pr_set_half', ship_qty: 1 }, { amazon_sku: 'pr_set_ok', ship_qty: 1 }]);
  assert.equal(r.status, 200, await r.clone().text());
  const csv = await csvText(r);
  const codes = csv.split('\r\n').slice(1).map((l) => l.split(',')[51]);
  assert.deepEqual(codes, ['ne001', 'ne002'], 'NE にあるコードだけ (セットの一部 ne001 は外した SKU の分を足していない)');
  const qty = Object.fromEntries(csv.split('\r\n').slice(1).map((l) => [l.split(',')[51], l.split(',')[53]]));
  assert.deepEqual(qty, { ne001: '2', ne002: '3' }, 'pr_set_half の ne001 × 2 は入らない');
  assert.equal(r.headers.get('X-NE-Skipped-Count'), '2');
  const w = warningsOf(r);
  assert.ok(w.some((x) => /^pr_draft .*NE に未登録の商品コード new-draft/.test(x)), w.join(' / '));
  assert.ok(w.some((x) => /^pr_set_half .*NE に未登録の商品コード new-pend/.test(x)), w.join(' / '));
  assert.ok(!csv.includes('new-draft') && !csv.includes('new-pend'));
});
await t('[2] 全部が NE に無い = 400 (CSV なし・警告つき) / 写しが空 = 503 (CSV なし・確かめられない)', async () => {
  putProducts(['ne001']);
  let r = await exportCsv([{ amazon_sku: 'pr_draft', ship_qty: 1 }]);
  assert.equal(r.status, 400);
  let j = await r.json();
  assert.ok(j.warnings.some((x) => /NE に未登録/.test(x)));
  putProducts([]);
  r = await exportCsv([{ amazon_sku: 'pr_ok', ship_qty: 1 }]);
  assert.equal(r.status, 503, '写しが読めない = NE にある商品も出さない (fail-closed)');
  j = await r.json();
  assert.equal(j.reason, 'ne_products_unreadable'); assert.match(j.error, /CSV を出しません/);
  // 同期の後 (NE に入った) = 出る
  putProducts(['ne001', 'new-draft']);
  r = await exportCsv([{ amazon_sku: 'pr_draft', ship_qty: 4 }]);
  assert.equal(r.status, 200);
  assert.match(await csvText(r), /,new-draft,0,4,/);
});

console.log('手で足す画面');
await t('[3] /api/ne-unregistered: 無いコード・ある・対応なし・写しが読めない = 503', async () => {
  putProducts(['ne001', 'ne002']);
  let j = await (await fetch(B + '/api/ne-unregistered?sku=PR_SET_HALF')).json();
  assert.deepEqual([j.ok, j.mapped, j.missing], [true, true, ['new-pend']], '大文字の SKU でも同じ');
  j = await (await fetch(B + '/api/ne-unregistered?sku=pr_ok')).json();
  assert.deepEqual([j.mapped, j.missing], [true, []]);
  j = await (await fetch(B + '/api/ne-unregistered?sku=pr_none')).json();
  assert.deepEqual([j.mapped, j.missing], [false, []]);
  assert.equal((await fetch(B + '/api/ne-unregistered')).status, 400);
  putProducts([]);
  const r = await fetch(B + '/api/ne-unregistered?sku=pr_ok');
  assert.equal(r.status, 503);
});
await t('[4] 画面の addManualItem = 確かめの API を呼び、無いコード・確かめられない (応答が ok でない・通信) なら足さずに返る (足す処理より前)', async () => {
  const v = fs.readFileSync(path.join(root, 'views', 'fba-replenishment.ejs'), 'utf8');
  const f = v.slice(v.indexOf('async function addManualItem()'), v.indexOf('async function addManualItem()') + 6000);
  const call = f.indexOf('/api/ne-unregistered?sku=');
  assert.ok(call > 0, '確かめの API を呼んでいない');
  assert.ok(call < f.indexOf('existing.recommended_qty = qty'), '足す処理より前に確かめる');
  const block = f.slice(call, f.indexOf('existing.recommended_qty = qty'));
  // 3 つの道を区切って、それぞれの中に return がある (正規表現で次の道の return まで読み進めない)
  const iUnread = block.indexOf('if (!nr.ok || !nj.ok) {'), iMissing = block.indexOf('if (nj.missing && nj.missing.length) {'), iCatch = block.indexOf('} catch (e) {');
  assert.ok(iUnread > 0 && iMissing > iUnread && iCatch > iMissing, '確かめの 3 つの道が見つからない');
  assert.match(block.slice(iUnread, iMissing), /return;/, '確かめられない = 足さない');
  assert.match(block.slice(iMissing, iCatch), /NE に未登録[\s\S]*return;/, '無いコード = 足さない');
  assert.match(block.slice(iCatch), /return;/, '通信の失敗 = 足さない');
});

server.close();
try { mdb.close(); } catch { /* */ }
console.log(`\n${pass} ok${fail ? ` / NG ${fail}` : ''}`);
if (fail) process.exitCode = 1;
setTimeout(() => process.exit(process.exitCode || 0), 200).unref();
