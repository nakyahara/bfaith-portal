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
 *   5 写しの鮮度 (#1694 Codex R2 M1): 世代の received_at → 無ければ行の updated_at。36 時間より古い・分からない = 止めずに知らせる (X-NE-Notes・X-NE-Warnings・確かめの API の note)
 *   6 過去の NE 受注 CSV の再ダウンロード (#1694 Codex R2 M2): 止めない。今の写しに無いコード・古い形の履歴・写しが読めない = 知らせ (X-NE-Notes) だけ・ファイルは同じ
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
/** /api/sync と同じ形の時刻 ('YYYY-MM-DD HH:MM:SS'・UTC)。h = 何時間前 */
const syncTime = (h = 0) => new Date(Date.now() - h * 3600e3).toISOString().replace('T', ' ').slice(0, 19);
/** 写しを入れ替える (行の updated_at = 入れ替えた時刻・世代の記録 = 同じ時刻。gen = false で世代の記録なし = 古い送り手) */
const putProducts = (codes, { hoursAgo = 1, gen = true, rowsAt } = {}) => {
  mdb.exec('DELETE FROM mirror_products');
  const at = syncTime(hoursAgo);
  const st = mdb.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価状態, updated_at) VALUES (?, ?, '単品', 'COMPLETE', ?)`);
  for (const c of codes) st.run(c, `商品 ${c}`, rowsAt ?? at);
  mdb.exec("DELETE FROM mirror_material_generations WHERE entity = 'products'");
  if (gen) mdb.prepare(`INSERT INTO mirror_material_generations (entity, generation_id, content_hash, row_count, received_at) VALUES ('products', 'g1', ?, ?, ?)`).run('a'.repeat(64), codes.length, at);
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

const notesOf = (r) => JSON.parse(decodeURIComponent(r.headers.get('X-NE-Notes') || '%5B%5D'));

console.log('写しの鮮度 (止めない)');
await t('[5] 時刻の読み方・古さの文 / 世代の received_at が先・無ければ行の updated_at / 新しい = 知らせなし・36 時間より古い・分からない = 知らせ', async () => {
  assert.equal(NR.parseMirrorTime('2026-10-12 01:02:03'), Date.UTC(2026, 9, 12, 1, 2, 3), '時差なし = UTC (/api/sync の now)');
  assert.equal(NR.parseMirrorTime('2026-10-12T01:02:03.000Z'), Date.UTC(2026, 9, 12, 1, 2, 3));
  assert.equal(NR.parseMirrorTime(''), null); assert.equal(NR.parseMirrorTime('こわれた'), null);
  const now = Date.UTC(2026, 9, 12, 12);
  assert.equal(NR.freshnessNote(now - 35 * 3600e3, now), null);
  assert.match(NR.freshnessNote(now - 40 * 3600e3, now), /40 時間前のものです \(同期が止まっているかも\)/);
  assert.match(NR.freshnessNote(null, now), /いつの物か分かりません/);
  putProducts(['ne001'], { hoursAgo: 2 });
  let ne = NR.readNeProductCodes(mdb);
  assert.deepEqual([ne.ok, ne.asOfSource, ne.note], [true, 'generation', null]);
  putProducts(['ne001'], { hoursAgo: 50 });
  ne = NR.readNeProductCodes(mdb);
  assert.match(ne.note, /^NE の商品の写しが 50 時間前のものです/);
  putProducts(['ne001'], { hoursAgo: 2, rowsAt: syncTime(100) });
  assert.equal(NR.readNeProductCodes(mdb).note, null, '世代の記録 (入れ替えた時刻) が先');
  putProducts(['ne001'], { hoursAgo: 60, gen: false });
  ne = NR.readNeProductCodes(mdb);
  assert.deepEqual([ne.asOfSource, /60 時間前/.test(ne.note)], ['rows', true], '世代が無い = 行の updated_at');
  putProducts(['ne001'], { gen: false, rowsAt: '' });
  ne = NR.readNeProductCodes(mdb);
  assert.deepEqual([ne.ok, ne.asOf], [true, null]);
  assert.match(ne.note, /いつの物か分かりません/, '分からない = 知らせ (止めない)');
});
await t('[5] 古い写し = CSV は出る (200・スキップ 0)・知らせは X-NE-Notes だけ (X-NE-Warnings = スキップだけ) / 確かめの API にも note / 新しい写し = 知らせなし', async () => {
  putProducts(['ne001', 'ne002'], { hoursAgo: 48 });
  let r = await exportCsv([{ amazon_sku: 'pr_ok', ship_qty: 1 }]);
  assert.equal(r.status, 200, '古い写しでも止めない');
  assert.match(await csvText(r), /,ne001,0,1,/);
  assert.equal(r.headers.get('X-NE-Skipped-Count'), '0', '知らせはスキップの数に入れない');
  assert.match(notesOf(r)[0], /48 時間前/);
  assert.deepEqual(warningsOf(r), [], 'X-NE-Warnings = スキップだけ (知らせは X-NE-Notes = 別に数える・#1694 Codex R3 Low)');
  let j = await (await fetch(B + '/api/ne-unregistered?sku=pr_ok')).json();
  assert.deepEqual([j.ok, j.missing], [true, []]); assert.match(j.note, /48 時間前/);
  // 全部外れた (400) ときも知らせを付ける
  r = await exportCsv([{ amazon_sku: 'pr_draft', ship_qty: 1 }]);
  assert.equal(r.status, 400);
  j = await r.json();
  assert.match(j.notes[0], /48 時間前/);
  assert.ok(j.warnings.length === 1 && !j.warnings.some((x) => /時間前/.test(x)), 'warnings = スキップだけ');
  putProducts(['ne001', 'ne002'], { hoursAgo: 1 });
  r = await exportCsv([{ amazon_sku: 'pr_ok', ship_qty: 1 }]);
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('X-NE-Notes'), null);
  assert.equal(r.headers.get('X-NE-Warnings'), null);
  j = await (await fetch(B + '/api/ne-unregistered?sku=pr_ok')).json();
  assert.equal(j.note, null);
});

console.log('過去の NE 受注 CSV の再ダウンロード (止めない)');
await t('[6] 今の写しに無いコード = 知らせ・ファイルは同じ / 古い形の履歴 = 確かめられない知らせ / 写しが読めない = 知らせだけ (200) / 新しい写しにある = 知らせなし', async () => {
  putProducts(['ne001', 'ne002', 'new-draft'], { hoursAgo: 1 });
  let r = await exportCsv([{ amazon_sku: 'pr_ok', ship_qty: 1 }, { amazon_sku: 'pr_draft', ship_qty: 2 }]);
  assert.equal(r.status, 200);
  const original = Buffer.from(await r.arrayBuffer());
  const hist = await (await fetch(B + '/api/export-history')).json();
  const id = Math.max(...hist.filter((h) => h.type === 'ne_csv').map((h) => h.id));   // 同じ秒に作った履歴があっても今出した物 (番号が一番大きい)
  r = await fetch(B + `/api/export-history/${id}/download`);
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('X-NE-Notes'), null, '今の写しに全部ある = 知らせなし');
  assert.ok(Buffer.from(await r.arrayBuffer()).equals(original));
  // 出した後に NE から消えた (写しに無い)
  putProducts(['ne001', 'ne002'], { hoursAgo: 1 });
  r = await fetch(B + `/api/export-history/${id}/download`);
  assert.equal(r.status, 200, '止めない');
  assert.ok(Buffer.from(await r.arrayBuffer()).equals(original), 'ファイルは保存したまま');
  assert.match(notesOf(r)[0], /今の NE の商品の写しに無い商品コード new-draft/);
  assert.equal(r.headers.get('X-NE-Warnings'), null, '再ダウンロードの知らせは X-NE-Notes だけ');
  // 写しが古い = 鮮度の知らせも
  putProducts(['ne001', 'ne002', 'new-draft'], { hoursAgo: 72 });
  r = await fetch(B + `/api/export-history/${id}/download`);
  assert.deepEqual(notesOf(r).map((x) => /72 時間前/.test(x)), [true]);
  // 古い形の履歴 (構成の記録なし)
  DB.saveExportHistory('ne_csv', 'old-form.csv', 1, 1, Buffer.from('x'), ['pr_ok'], null);
  const oldId = (await (await fetch(B + '/api/export-history')).json()).find((h) => h.filename === 'old-form.csv').id;
  putProducts(['ne001'], { hoursAgo: 1 });
  r = await fetch(B + `/api/export-history/${oldId}/download`);
  assert.equal(r.status, 200);
  assert.match(notesOf(r)[0], /古い形 .*確かめられません/);
  // 写しが読めない = 知らせだけ (再ダウンロードは止めない)
  putProducts([]);
  r = await fetch(B + `/api/export-history/${id}/download`);
  assert.equal(r.status, 200);
  assert.match(notesOf(r)[0], /確かめられません/);
  // 納品 Excel の履歴は確かめない
  DB.saveExportHistory('manifest_excel', 'm.xlsx', 1, 1, Buffer.from('y'), ['pr_ok'], null);
  const mId = (await (await fetch(B + '/api/export-history')).json()).find((h) => h.filename === 'm.xlsx').id;
  r = await fetch(B + `/api/export-history/${mId}/download`);
  assert.deepEqual([r.status, r.headers.get('X-NE-Notes')], [200, null]);
});
await t('[6] 画面: 再ダウンロードは取ってきてから保存し X-NE-Notes を出す・CSV 出力も知らせ (X-NE-Notes) を出す・手で足す画面は note を出して足す', async () => {
  const v = fs.readFileSync(path.join(root, 'views', 'fba-replenishment.ejs'), 'utf8');
  const dl = v.slice(v.indexOf('async function downloadHistory('), v.indexOf('async function downloadHistory(') + 1500);
  assert.ok(dl.length > 100, 'downloadHistory が fetch の形でない');
  assert.match(dl, /await fetch\(`\$\{BASE\}\/api\/export-history\/\$\{id\}\/download`\)/);
  assert.match(dl, /X-NE-Notes[\s\S]*alert\(/);
  const ex = v.slice(v.indexOf("fetch(`${BASE}/api/export-ne-csv`"), v.indexOf("fetch(`${BASE}/api/export-ne-csv`") + 3500);
  assert.match(ex, /X-NE-Notes[\s\S]*else if \(neNotes\.length\)/);
  const add = v.slice(v.indexOf('async function addManualItem()'), v.indexOf('async function addManualItem()') + 7000);
  assert.match(add, /neNote = nj\.note \|\| null;/);
  assert.match(add, /if \(neNote\) \{ msgEl\.textContent \+=/);
});

console.log('知らせとスキップを別に数える・画面から落とさない (#1694 Codex R3)');
await t('[7] スキップの警告が 6KB で切れても、X-NE-Warnings はスキップだけ = 画面の「ほか N 件」(Skipped-Count − 出た数) が正しい / 知らせは X-NE-Notes に全部', async () => {
  const many = Array.from({ length: 80 }, (_, i) => ({ amazon_sku: `pr_many_${String(i).padStart(2, '0')}`, product_name: '長い名前の新商品 '.repeat(4) + i, ne_code: `new-many-${i}` }));
  DB.upsertSkuMappings(many);
  putProducts(['ne001', 'ne002'], { hoursAgo: 50 });
  const r = await exportCsv([{ amazon_sku: 'pr_ok', ship_qty: 1 }, ...many.map((m) => ({ amazon_sku: m.amazon_sku, ship_qty: 1 }))]);
  assert.equal(r.status, 200);
  const skipped = Number(r.headers.get('X-NE-Skipped-Count'));
  const w = warningsOf(r);
  assert.equal(skipped, 80, 'スキップの数 = 外した SKU の数 (知らせは入れない)');
  assert.ok(w.length > 0 && w.length < skipped, `6KB で切れている (${w.length} / ${skipped})`);
  assert.ok(w.every((x) => /^pr_many_\d+ .*NE に未登録/.test(x)), '切れた一覧もスキップだけ (知らせが 1 枠を取らない)');
  // 画面の「ほか N 件」= skippedCount − 出た数 = 本当に出ていないスキップの数
  assert.equal(skipped - w.length, 80 - w.length);
  assert.deepEqual(notesOf(r).map((x) => /50 時間前/.test(x)), [true]);
});
await t('[7] 画面: 手で足す「NE に未登録」の文に鮮度の知らせ (note) も入れる・全部外れた 400 の alert に notes も入れる・スキップの alert にも知らせ', async () => {
  const v = fs.readFileSync(path.join(root, 'views', 'fba-replenishment.ejs'), 'utf8');
  const add = v.slice(v.indexOf('async function addManualItem()'), v.indexOf('async function addManualItem()') + 7000);
  const missing = add.slice(add.indexOf('if (nj.missing && nj.missing.length) {'), add.indexOf('neNote = nj.note || null;'));
  assert.ok(missing.length > 50, '未登録の道が見つからない');
  assert.match(missing, /msgEl\.textContent = [^;]*NE に未登録[^;]*nj\.note \? `[^`]*\$\{nj\.note\}/, '未登録の文に note');
  assert.ok(missing.indexOf('nj.note') < missing.indexOf('return;'), 'return の前に note を入れる');
  const ex = v.slice(v.indexOf("fetch(`${BASE}/api/export-ne-csv`"), v.indexOf("fetch(`${BASE}/api/export-ne-csv`") + 4000);
  const notOk = ex.slice(ex.indexOf('if (!res.ok) {'), ex.indexOf('const skippedCount'));
  assert.match(notOk, /const errNotes = Array\.isArray\(err\.notes\)/);
  assert.match(notOk, /alert\('エラー: ' \+ \(err\.error \|\| 'CSV出力に失敗'\) \+ \(errNotes\.length \?/, '400 の alert に notes');
  const okSkip = ex.slice(ex.indexOf('if (skippedCount > 0) {'), ex.indexOf('} else if (neNotes.length) {'));
  assert.match(okSkip, /alert\(`NE受注CSVを出力しましたが[\s\S]*neNotes\.length \?/, 'スキップの alert にも知らせ');
});

server.close();
try { mdb.close(); } catch { /* */ }
console.log(`\n${pass} ok${fail ? ` / NG ${fail}` : ''}`);
if (fail) process.exitCode = 1;
setTimeout(() => process.exit(process.exitCode || 0), 200).unref();
