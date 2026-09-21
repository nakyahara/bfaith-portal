#!/usr/bin/env node
import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-company-db-inventory.mjs — ロジザード在庫の毎時写し・日の締め・整理 (apps/company-db/inventory/) の受入試験 (08 §3。D2)
 *
 * PGlite で 0001〜0011 を流し、行の配列を与えて確かめる (SQLite は readMirrorLogizardStock の fixture だけ):
 *   鍵と中身 / 最初の取込 / 同じ・古い世代は skipped / 変化・消滅・追加・鍵の付け替え / 重複鍵は failed で何も残らない /
 *   失敗した run は比較元にならない / 日の締め (complete・missing・二度目は何もしない) / 有効期限の日付化・未知の商品コード /
 *   整理と記録 / mirror の読み取り / 定期実行の包み (ping の出しかた)
 * 🚨 2 接続の並行 (advisory lock・表ロック) は PGlite では書けない → 本番で test-company-db-concurrency.mjs と同じ手で確かめる
 * 実行: node scripts/test-company-db-inventory.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import {
  businessKey, payloadOf, contentHashOf, captureLogizardInventory, closeStockDay, closeStockDays, maintainInventory,
  readMirrorLogizardStock, nextDay, safeDate, SCOPE, RAW_SRC,
} from '../apps/company-db/inventory/logizard.mjs';
import { runInventoryHourly, summarize, startCompanyDbInventoryHourlyCron, JOB_ID } from '../apps/company-db/inventory-hourly.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 3).join('\n      ')); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`); return threw; };
const quiet = () => {};

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
const co = (await one(`select company_id from core.companies order by 1 limit 1`)).company_id;
await pg.query(`insert into core.products (company_id, name) values ($1, '見本A'), ($1, '見本B')`, [co]);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', case name when '見本A' then 'AAA-1' else 'bbb-2' end, name from core.products`);
const skuA = (await one(`select sku_id from core.skus where code = 'AAA-1'`)).sku_id;

// 見本の行 (mirror_logizard_stock の列)
const row = (id, block, loke, qty, extra = {}) => ({ '商品ID': id, '商品名': `名前 ${id}`, 'バーコード': '490000000000' + qty, 'ブロック略称': block, 'ロケ': loke, '品質区分名': '良品', '有効期限': '', '入荷日': '2026/09/01', '在庫数': qty, '引当数': 0, 'ロケ業務区分': 'ピック', '最終入荷日': '2026/09/01', '最終出荷日': null, '在庫日': '2026/09/13', ...extra });
const G1 = '2026-09-11T01:00:00.000Z', G2 = '2026-09-11T03:00:00.000Z', G3 = '2026-09-12T09:00:00.000Z';   // JST 9/11 10:00, 12:00, 9/12 18:00

console.log('鍵と中身');
await t('business_key = 商品ID|ブロック略称|ロケ|品質区分名|有効期限|入荷日 (空は -、前後の空白は落とす)。payload は 13 列だけで volatile を含まない。hash は鍵の順に依らない', async () => {
  const r = row(' aaa-1 ', 'P3FA', '001-001-01', 5);
  assert.equal(businessKey(r), 'aaa-1|P3FA|001-001-01|良品|-|2026/09/01');
  const p = payloadOf(r);
  assert.deepEqual(Object.keys(p).sort(), ['バーコード', 'ブロック略称', 'ロケ', 'ロケ業務区分', '入荷日', '商品ID', '商品名', '品質区分名', '在庫数', '引当数', '最終入荷日'].sort());
  assert.equal(p['在庫数'], 5); assert.equal(p['商品ID'], 'aaa-1');
  assert.ok(!('在庫日' in p) && !('captured_at' in p));
  const p2 = Object.fromEntries(Object.entries(p).reverse());
  assert.equal(contentHashOf(p), contentHashOf(p2));
  assert.notEqual(contentHashOf(p), contentHashOf({ ...p, '在庫数': 6 }));
});

console.log('毎時の写し');
const rows1 = [row('AAA-1', 'P3FA', '001-001-01', 10), row('AAA-1', 'R1FA', '002-001-01', 3), row('bbb-2', 'P3FA', '003-001-01', 7), row('zzz-9', 'P3FB', '004-001-01', 1)];
await t('最初の取込: 全行が ok の観測になり、中身・ロケーションが増え、run は success/complete、現在庫 view に出る', async () => {
  const r = await captureLogizardInventory(db, { rows: rows1, capturedAt: G1, host: 'test', log: quiet });
  assert.equal(r.status, 'success'); assert.equal(r.seen, 4); assert.equal(r.added, 4); assert.equal(r.changed, 0); assert.equal(r.removed, 0);
  assert.equal(r.contentsNew, 4); assert.equal(r.locationsAdded, 4);
  assert.equal(await num(`select count(*) as n from raw.${RAW_SRC}_observations where fetch_status = 'ok'`), 4);
  const run = await one(`select status, complete, checksum, rows_seen, rows_inserted from ops.ingest_runs where ingest_run_id = $1`, [r.runId]);
  assert.equal(run.status, 'success'); assert.equal(run.complete, true); assert.equal(run.checksum, G1); assert.equal(run.rows_seen, 4); assert.equal(run.rows_inserted, 4);
  const loc = await one(`select block, building, company_id from core.locations where code = 'R1FA-002-001-01'`);
  assert.equal(loc.block, 'R1FA'); assert.equal(loc.building, 'iroha'); assert.equal(loc.company_id, co);
  const v = (await pg.query(`select line_key, qty, location_code from mart.v_warehouse_stock_current order by line_key`)).rows;
  assert.equal(v.length, 4); assert.equal(v[0].qty, 10); assert.equal(v[0].location_code, 'P3FA-001-001-01');
});
await t('同じ世代 → skipped の run だけ (観測は増えない)。古い世代も skipped', async () => {
  const r = await captureLogizardInventory(db, { rows: rows1, capturedAt: G1, log: quiet });
  assert.equal(r.status, 'skipped'); assert.equal(r.reasonCode, 'same_generation'); assert.match(r.reason, /同じ世代/);
  const r2 = await captureLogizardInventory(db, { rows: rows1, capturedAt: '2026-09-10T00:00:00Z', log: quiet });
  assert.equal(r2.status, 'skipped'); assert.equal(r2.reasonCode, 'old_generation'); assert.match(r2.reason, /古い世代/);
  assert.equal(await num(`select count(*) as n from ops.ingest_runs where status = 'skipped'`), 2);
  assert.equal(await num(`select count(*) as n from raw.${RAW_SRC}_observations`), 4);
});
await t('次の世代: 変化 (数量) / 消滅 / 追加 / 鍵の付け替え (ロケ移動 = 旧 not_found + 新 ok) だけが観測になる。変わらない行は書かない', async () => {
  const rows2 = [row('AAA-1', 'P3FA', '001-001-01', 8), /* R1FA の行が消えた */ row('bbb-2', 'P3FA', '003-002-01', 7) /* ロケ移動 */, row('zzz-9', 'P3FB', '004-001-01', 1) /* 変化なし */, row('new-3', 'P3FA', '005-001-01', 2)];
  const r = await captureLogizardInventory(db, { rows: rows2, capturedAt: G2, log: quiet });
  assert.equal(r.status, 'success'); assert.equal(r.changed, 1); assert.equal(r.added, 2); assert.equal(r.removed, 2);
  const obs = (await pg.query(`select business_key, fetch_status from raw.${RAW_SRC}_observations where ingest_run_id = $1 order by business_key, fetch_status`, [r.runId])).rows;
  assert.deepEqual(obs.map((o) => `${o.business_key.split('|').slice(0, 3).join('|')}:${o.fetch_status}`), [
    'AAA-1|P3FA|001-001-01:ok', 'AAA-1|R1FA|002-001-01:not_found', 'bbb-2|P3FA|003-001-01:not_found', 'bbb-2|P3FA|003-002-01:ok', 'new-3|P3FA|005-001-01:ok']);
  const v = (await pg.query(`select line_key, qty from mart.v_warehouse_stock_current order by line_key`)).rows;
  assert.deepEqual(v.map((x) => `${x.line_key.split('|')[0]}:${x.qty}`), ['AAA-1:8', 'bbb-2:7', 'new-3:2', 'zzz-9:1']);
  assert.equal(r.locationsAdded, 2);   // 003-002-01 と 005-001-01
});
await t('🚨 鍵が重複する世代は failed: 観測・中身・ロケーションは何も増えず、view も変わらない。空の rows も failed', async () => {
  const before = { obs: await num(`select count(*) as n from raw.${RAW_SRC}_observations`), c: await num(`select count(*) as n from raw.${RAW_SRC}_contents`), l: await num(`select count(*) as n from core.locations`) };
  const dup = [row('AAA-1', 'P3FA', '001-001-01', 8), row('AAA-1', 'P3FA', '001-001-01', 9), row('x', 'P9', '9', 1)];
  const e = await rejects(() => captureLogizardInventory(db, { rows: dup, capturedAt: '2026-09-11T04:00:00Z', log: quiet }), /重複/);
  assert.equal(e.code, 'DUPLICATE_KEY');
  const run = await one(`select status, complete, error from ops.ingest_runs where status = 'failed' order by started_at desc limit 1`);
  assert.equal(run.complete, false); assert.match(run.error, /重複/);
  await rejects(() => captureLogizardInventory(db, { rows: [], capturedAt: '2026-09-11T04:10:00Z', log: quiet }), /空/);
  const after = { obs: await num(`select count(*) as n from raw.${RAW_SRC}_observations`), c: await num(`select count(*) as n from raw.${RAW_SRC}_contents`), l: await num(`select count(*) as n from core.locations`) };
  assert.deepEqual(after, before);
});
await t('🚨 書いた後で失敗しても全部戻る: 別会社としてロケーションを足そうとすると例外 (観測・中身を書いた後) → 観測・中身・ロケは増えず、run は failed、view も変わらない', async () => {
  const other = (await one(`select max(company_id)::smallint + 1 as c from core.companies`)).c;
  await pg.query(`insert into core.companies (company_id, name, kind) values ($1, 'other', 'subsidiary')`, [other]);
  const before = { obs: await num(`select count(*) as n from raw.${RAW_SRC}_observations`), c: await num(`select count(*) as n from raw.${RAW_SRC}_contents`), l: await num(`select count(*) as n from core.locations`) };
  const rowsX = [row('AAA-1', 'P3FA', '001-001-01', 5), row('bbb-2', 'P3FA', '003-002-01', 7), row('zzz-9', 'P3FB', '004-001-01', 1), row('new-3', 'P3FA', '005-001-01', 2)];
  const e = await rejects(() => captureLogizardInventory(db, { rows: rowsX, capturedAt: '2026-09-11T04:30:00Z', companyId: other, log: quiet }), /belongs to company|foreign key|violates/i);
  assert.ok(e);
  const after = { obs: await num(`select count(*) as n from raw.${RAW_SRC}_observations`), c: await num(`select count(*) as n from raw.${RAW_SRC}_contents`), l: await num(`select count(*) as n from core.locations`) };
  assert.deepEqual(after, before);
  const run = await one(`select status, complete from ops.ingest_runs where checksum = '2026-09-11T04:30:00.000Z'`);
  assert.equal(run.status, 'failed'); assert.equal(run.complete, false);
  assert.equal((await one(`select qty from mart.v_warehouse_stock_current where line_key like 'AAA-1|P3FA|%'`)).qty, 8);
  // 新しいロケーションを本当に書いた後 (commit の直前) に失敗 → そのロケーションも観測も中身も戻る
  const rowsY = [row('AAA-1', 'P3FC', '777-001-01', 4), row('bbb-2', 'P3FA', '003-002-01', 7), row('zzz-9', 'P3FB', '004-001-01', 1), row('new-3', 'P3FA', '005-001-01', 2)];
  let sawLocation = null;
  await rejects(() => captureLogizardInventory(db, { rows: rowsY, capturedAt: '2026-09-11T04:40:00Z', log: quiet, afterWrite: async () => {
    sawLocation = await num(`select count(*) as n from core.locations where code = 'P3FC-777-001-01'`);
    throw new Error('boom after write');
  } }), /boom/);
  assert.equal(sawLocation, 1);   // 書いた (取引の中では見えた)
  assert.equal(await num(`select count(*) as n from core.locations where code = 'P3FC-777-001-01'`), 0);   // 戻った
  const after2 = { obs: await num(`select count(*) as n from raw.${RAW_SRC}_observations`), c: await num(`select count(*) as n from raw.${RAW_SRC}_contents`), l: await num(`select count(*) as n from core.locations`) };
  assert.deepEqual(after2, before);
  assert.equal((await one(`select status from ops.ingest_runs where checksum = '2026-09-11T04:40:00.000Z'`)).status, 'failed');
});
await t('🚨 数量が非負の int32 でない世代は failed (在庫数 -1 / 引当数 2^31 / 文字)。締めで落ちる行を success にしない', async () => {
  const before = await num(`select count(*) as n from raw.${RAW_SRC}_observations`);
  const base = [row('bbb-2', 'P3FA', '003-002-01', 7), row('zzz-9', 'P3FB', '004-001-01', 1), row('new-3', 'P3FA', '005-001-01', 2)];
  const e1 = await rejects(() => captureLogizardInventory(db, { rows: [row('AAA-1', 'P3FA', '001-001-01', -1), ...base], capturedAt: '2026-09-11T04:41:00Z', log: quiet }), /在庫数/);
  assert.equal(e1.code, 'BAD_QTY');
  await rejects(() => captureLogizardInventory(db, { rows: [row('AAA-1', 'P3FA', '001-001-01', 8, { '引当数': 2147483648 }), ...base], capturedAt: '2026-09-11T04:42:00Z', log: quiet }), /引当数/);
  await rejects(() => captureLogizardInventory(db, { rows: [row('AAA-1', 'P3FA', '001-001-01', 'abc'), ...base], capturedAt: '2026-09-11T04:43:00Z', log: quiet }), /在庫数/);
  // 各行は正常でも、商品ID 単位の合計 (sku_stock_daily の sum) が int32 を超える世代も failed (R3 #1)
  const e4 = await rejects(() => captureLogizardInventory(db, { rows: [row('AAA-1', 'P3FA', '001-001-01', 2147483647), row('AAA-1', 'R1FA', '002-001-01', 1), ...base], capturedAt: '2026-09-11T04:44:00Z', log: quiet }), /合計.*int32/);
  assert.equal(e4.code, 'BAD_QTY');
  await rejects(() => captureLogizardInventory(db, { rows: [row('AAA-1', 'P3FA', '001-001-01', 1, { '引当数': 2147483647 }), row('AAA-1', 'R1FA', '002-001-01', 1, { '引当数': 1 }), ...base], capturedAt: '2026-09-11T04:45:00Z', log: quiet }), /合計.*int32/);
  assert.equal(await num(`select count(*) as n from raw.${RAW_SRC}_observations`), before);
  assert.equal(await num(`select count(*) as n from ops.ingest_runs where status = 'failed' and (error like '%整数ではない%' or error like '%int32%')`), 5);
});
await t('🚨 失敗した run の観測・完走 run の error / skipped 観測は比較元にならない: 次の成功は「直前の成功の状態」と比べる (同じ内容なら観測は増えない)', async () => {
  const KA = 'AAA-1|P3FA|001-001-01|良品|-|2026/09/01';
  // 失敗した run に「AAA-1 = 99」の観測、完走 run に error / skipped の観測を手で入れる (どれも状態ではない)
  await pg.query(`insert into raw.${RAW_SRC}_contents (content_hash, payload) values ('h-99', '{"商品ID":"AAA-1","在庫数":99}')`);
  await pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, finished_at, status, complete, checksum) values ('fail-run', 'logizard', 'inventory', $1, now(), now(), 'failed', false, '2026-09-11T04:50:00.000Z'), ('err-run', 'logizard', 'inventory', $1, now(), now(), 'success', true, '2026-09-11T04:55:00.000Z')`, [SCOPE]);
  await pg.query(`insert into raw.${RAW_SRC}_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('fail-run', $1, $2, 'h-99', 'ok', '2026-09-11T04:50:00Z'), ('err-run', $1, $2, null, 'error', '2026-09-11T04:55:00Z'), ('err-run', $1, 'zzz-9|P3FB|004-001-01|良品|-|2026/09/01', null, 'skipped', '2026-09-11T04:55:00Z')`, [SCOPE, KA]);
  assert.equal((await one(`select qty from mart.v_warehouse_stock_current where line_key = $1`, [KA])).qty, 8);
  const rows2 = [row('AAA-1', 'P3FA', '001-001-01', 8), row('bbb-2', 'P3FA', '003-002-01', 7), row('zzz-9', 'P3FB', '004-001-01', 1), row('new-3', 'P3FA', '005-001-01', 2)];
  const r = await captureLogizardInventory(db, { rows: rows2, capturedAt: '2026-09-11T05:00:00Z', log: quiet });
  assert.equal(r.status, 'success'); assert.equal(r.added + r.changed + r.removed, 0);
  assert.equal(await num(`select count(*) as n from raw.${RAW_SRC}_observations where ingest_run_id = $1`, [r.runId]), 0);
});
await t('商品ID が空の行・captured_at が不正 → 失敗', async () => {
  await rejects(() => captureLogizardInventory(db, { rows: [row('', 'P3FA', '1', 1)], capturedAt: '2026-09-11T06:00:00Z', log: quiet }), /商品ID/);
  await rejects(() => captureLogizardInventory(db, { rows: rows1, capturedAt: 'not-a-date', log: quiet }), /不正/);
});

console.log('日の締め');
await t('nextDay は月末・年末をまたぐ', () => { assert.equal(nextDay('2026-09-30'), '2026-10-01'); assert.equal(nextDay('2026-12-31'), '2027-01-01'); assert.equal(nextDay('2028-02-28'), '2028-02-29'); });
await t('🚨 締め: 9/11 (JST) はその日の最後の完走世代 (05:00Z = 14:00 JST の run) の状態で complete、9/12 は取得が無いので missing、今日 (9/13) は締めない。二度目は何もしない', async () => {
  const r = await closeStockDays(db, { todayJst: '2026-09-13', log: quiet });
  assert.deepEqual(r.closed.map((c) => `${c.day}:${c.status}`), ['2026-09-11:complete', '2026-09-12:missing']);
  const d1 = r.closed[0];
  assert.equal(d1.generation, '2026-09-11T05:00:00.000Z'); assert.equal(d1.lines, 4); assert.equal(d1.skus, 4);
  const cap = await one(`select status, completed_at, ingest_run_id from snapshots.stock_capture_days where snapshot_date = date '2026-09-11'`);
  assert.equal(cap.status, 'complete'); assert.ok(cap.completed_at); assert.equal(cap.ingest_run_id, d1.runId);
  const wh = await one(`select sku_id, location_id, location_code, block_code, received_date::text rd, expiry_date, qty from snapshots.warehouse_stock_daily where snapshot_date = date '2026-09-11' and line_key like 'AAA-1|P3FA|%'`);
  assert.equal(wh.sku_id, skuA); assert.ok(wh.location_id); assert.equal(wh.location_code, 'P3FA-001-001-01'); assert.equal(wh.block_code, 'P3FA'); assert.equal(wh.rd, '2026-09-01'); assert.equal(wh.expiry_date, null); assert.equal(wh.qty, 8);
  const unk = await one(`select sku_id from snapshots.sku_stock_daily where snapshot_date = date '2026-09-11' and source_code = 'zzz-9'`);
  assert.equal(unk.sku_id, null);   // 未知の商品コードは sku_id null のまま残す (落とさない)
  const bb = await one(`select sku_id from snapshots.sku_stock_daily where snapshot_date = date '2026-09-11' and source_code = 'bbb-2'`);
  assert.ok(bb.sku_id);   // 大文字小文字違いも core.norm_code で当たる
  const v = await one(`select warehouse_as_of::text d, warehouse_qty from mart.v_sku_stock where sku_id = $1`, [skuA]);
  assert.equal(v.d, '2026-09-11'); assert.equal(v.warehouse_qty, 8);
  const again = await closeStockDays(db, { todayJst: '2026-09-13', log: quiet });
  assert.equal(again.closed.length, 0);
  assert.equal((await closeStockDay(db, '2026-09-11', { log: quiet })).status, 'exists');
});
await t('締めた日の日次表は変えられない (0011 の trigger)。日付の形が違えば例外', async () => {
  await rejects(() => pg.query(`update snapshots.sku_stock_daily set qty = 0 where snapshot_date = date '2026-09-11'`), /complete|building/);
  await rejects(() => closeStockDay(db, '2026/09/11', { log: quiet }), /YYYY-MM-DD/);
});
await t('safeDate: 実在する日付だけ (2027/03/31・2027-3-1・先頭に日付があれば OK。13 月・2/30・文字は null)', () => {
  assert.equal(safeDate('2027/03/31'), '2027-03-31'); assert.equal(safeDate('2027-3-1'), '2027-03-01'); assert.equal(safeDate(' 2028/02/29 00:00 '), '2028-02-29');
  assert.equal(safeDate('2027/13/01'), null); assert.equal(safeDate('2027/02/30'), null); assert.equal(safeDate('未定'), null); assert.equal(safeDate(''), null); assert.equal(safeDate(null), null);
});
await t('🚨 有効期限 2027/03/31 と 2027-3-1 は date になり、13 月・2/30・文字は null (1 行の不正で日の締めが止まらない。件数を数える)。商品コードは大文字小文字を区別せず SKU に当たる。品質区分 (良品 / Ｂ品) は分けずに SKU に合算', async () => {
  const rows3 = [row('AAA-1', 'P3FA', '001-001-01', 8, { '有効期限': '2027/03/31' }), row('AAA-1', 'P3FA', '001-001-01', 2, { '品質区分名': 'Ｂ品', '有効期限': '2027-3-1' }), row('BBB-2', 'P3FA', '003-002-01', 7, { '有効期限': '2027/13/01', '入荷日': '2027/02/30' }), row('zzz-9', 'P3FB', '004-001-01', 1, { '有効期限': '未定' })];
  const r = await captureLogizardInventory(db, { rows: rows3, capturedAt: G3, log: quiet });
  assert.equal(r.status, 'success');
  const c = await closeStockDays(db, { todayJst: '2026-09-14', log: quiet });
  assert.deepEqual(c.closed.map((x) => `${x.day}:${x.status}`), ['2026-09-13:missing']);   // 9/12 は既に missing、9/13 は取得なし (G3 は 9/12 18:00 JST)
  assert.equal(c.backlog, false);
  // 9/12 は最初の締めで missing にしたので、やり直しは capture 行を消してから (保守経路)。ここではその手順を試す
  await pg.exec(`begin; set local snapshots.maintenance = 'on'; delete from snapshots.stock_capture_days where snapshot_date = date '2026-09-12'; commit;`);
  const d = await closeStockDay(db, '2026-09-12', { log: quiet });
  assert.equal(d.status, 'complete'); assert.equal(d.generation, G3); assert.equal(d.lines, 4); assert.equal(d.skus, 3); assert.equal(d.badDates, 3);   // 13 月・2/30・未定
  const ex = (await pg.query(`select logizard_code, quality, expiry_date::text e, received_date::text rd, qty from snapshots.warehouse_stock_daily where snapshot_date = date '2026-09-12'`)).rows;
  assert.deepEqual(ex.map((x) => `${x.logizard_code}:${x.quality}:${x.e}:${x.rd}:${x.qty}`).sort(), ['AAA-1:Ｂ品:2027-03-01:2026-09-01:2', 'AAA-1:良品:2027-03-31:2026-09-01:8', 'BBB-2:良品:null:null:7', 'zzz-9:良品:null:2026-09-01:1'].sort());
  const sk = await one(`select qty from snapshots.sku_stock_daily where snapshot_date = date '2026-09-12' and source_code = 'AAA-1'`);
  assert.equal(sk.qty, 10);
  const skuB = (await one(`select sku_id from core.skus where code = 'bbb-2'`)).sku_id;
  assert.equal((await one(`select sku_id from snapshots.sku_stock_daily where snapshot_date = date '2026-09-12' and source_code = 'BBB-2'`)).sku_id, skuB);   // BBB-2 → bbb-2
});

console.log('整理と記録');
await t('maintainInventory: 30 日より古い置き換え済みの観測が消え、ops.job_runs に DB の大きさが残る', async () => {
  // 古い観測を作る: 60 日前の完走 run で AAA-1 が 99 → その後の世代で置き換わっている
  await pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, finished_at, status, complete, checksum) values ('old-run', 'logizard', 'inventory', $1, now() - interval '60 days', now() - interval '60 days', 'success', true, '2026-07-13T00:00:00.000Z')`, [SCOPE]);
  await pg.query(`insert into raw.${RAW_SRC}_contents (content_hash, payload) values ('h-old', '{"商品ID":"AAA-1","在庫数":99}')`);
  await pg.query(`insert into raw.${RAW_SRC}_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('old-run', $1, 'AAA-1|P3FA|001-001-01|良品|-|2026/09/01', 'h-old', 'ok', now() - interval '60 days')`, [SCOPE]);
  const before = await num(`select count(*) as n from raw.${RAW_SRC}_observations`);
  const m = await maintainInventory(db, { host: 'test', note: 'test' });
  assert.equal(m.purged, 1); assert.ok(m.dbBytes > 0);
  assert.equal(await num(`select count(*) as n from raw.${RAW_SRC}_observations`), before - 1);
  assert.equal(await num(`select count(*) as n from raw.${RAW_SRC}_contents where content_hash = 'h-old'`), 0);
  const jr = await one(`select job_id, status, summary from ops.job_runs order by job_run_id desc limit 1`);
  assert.equal(jr.job_id, JOB_ID); assert.equal(jr.status, 'ok'); assert.match(jr.summary, /"purged_observations":1/); assert.match(jr.summary, /db_mb/);
});

console.log('mirror の読み取り');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-inv-'));
process.on('exit', () => { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* 消せなくても結果は変わらない */ } });
await t('readMirrorLogizardStock: 表が無い → null、行があれば 13 列 + 世代 (captured_at は ISO に正規化)、世代が混ざっていれば例外', async () => {
  const file = path.join(tmp, 'warehouse-mirror.db');
  const m = new Database(file);
  assert.equal(await readMirrorLogizardStock(path.join(tmp, 'nope')), null);
  m.exec(`CREATE TABLE mirror_products (product_id INTEGER PRIMARY KEY)`);
  assert.equal(await readMirrorLogizardStock(tmp), null);
  m.exec(`CREATE TABLE mirror_logizard_stock (商品ID TEXT NOT NULL, 商品名 TEXT, バーコード TEXT, ブロック略称 TEXT, ロケ TEXT, 品質区分名 TEXT, 有効期限 TEXT, 入荷日 TEXT, 在庫数 INTEGER NOT NULL, 引当数 INTEGER NOT NULL, ロケ業務区分 TEXT, 最終入荷日 TEXT, 最終出荷日 TEXT, 在庫日 TEXT, captured_at TEXT NOT NULL, synced_at TEXT NOT NULL)`);
  assert.equal(await readMirrorLogizardStock(tmp), null);
  const ins = m.prepare(`INSERT INTO mirror_logizard_stock VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  ins.run('AAA-1', '名前', '4900', 'P3FA', '001-001-01', '良品', '', '2026/09/01', 10, 1, 'ピック', '2026/09/01', null, '2026/09/13', '2026-09-13T01:00:00+09:00', '2026-09-13 01:00:05');
  ins.run('bbb-2', '名前', '4901', 'P3FA', '003-001-01', '良品', '', '2026/09/01', 7, 0, 'ピック', '2026/09/01', null, '2026/09/13', '2026-09-13T01:00:00+09:00', '2026-09-13 01:00:05');
  m.close();
  const r = await readMirrorLogizardStock(tmp);
  assert.equal(r.capturedAt, '2026-09-12T16:00:00.000Z'); assert.equal(r.rows.length, 2); assert.equal(r.rows[0]['在庫数'], 10); assert.ok(!('synced_at' in r.rows[0]));
  const m2 = new Database(file);
  m2.prepare(`INSERT INTO mirror_logizard_stock VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run('c', null, null, 'P', '1', '良品', '', '', 1, 0, null, null, null, null, '2026-09-13T02:00:00+09:00', 'x');
  m2.close();
  await rejects(() => readMirrorLogizardStock(tmp), /複数の世代/);
});

console.log('定期実行の包み');
const spyPing = () => { const calls = []; const fn = (...a) => { calls.push(a); return true; }; fn.calls = calls; return fn; };
const withEnv = async (patch, fn) => {
  const before = {};
  for (const [k, v] of Object.entries(patch)) { before[k] = process.env[k]; if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  try { return await fn(); } finally { for (const [k, v] of Object.entries(before)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; } }
};
const fakeConnect = async () => ({ query: (sql, p) => pg.query(sql, p), end: async () => {} });
const mirrorOf = (capturedAt, rows) => async () => ({ capturedAt, rows });
await t('Render の外では黙って何もしない (ping も無し)。force で通す', async () => {
  const ping = spyPing();
  const r = await withEnv({ RENDER: undefined, DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping, log: quiet }));
  assert.equal(r.skipped, true); assert.equal(r.note, 'not-render'); assert.equal(ping.calls.length, 0);
});
await t('env が無ければ失敗を ping', async () => {
  const ping = spyPing();
  const r = await withEnv({ RENDER: 'true', DATA_DIR: undefined, COMPANY_DB_URL: undefined }, () => runInventoryHourly({ ping, log: quiet }));
  assert.equal(r.ok, false); assert.equal(ping.calls[0][1], 'fail'); assert.match(ping.calls[0][2], /DATA_DIR/);
});
await t('材料 (mirror の表) が無ければ失敗を ping', async () => {
  const ping = spyPing();
  const r = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping, log: quiet, readMirror: async () => null, connect: fakeConnect }));
  assert.equal(r.ok, false); assert.equal(ping.calls[0][1], 'fail'); assert.match(ping.calls[0][2], /mirror_logizard_stock/);
});
await t('新しい世代を取り込んだら ok を ping (note に取込の内訳)。同じ世代で締める日も無ければ ping しない', async () => {
  const ping = spyPing();
  const rowsN = [row('AAA-1', 'P3FA', '001-001-01', 11), row('bbb-2', 'P3FA', '003-002-01', 7)];
  const r = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping, log: quiet, readMirror: mirrorOf('2026-09-14T00:00:00Z', rowsN), connect: fakeConnect, now: () => new Date('2026-09-14T00:35:00Z') }));
  assert.equal(r.ok, true); assert.equal(r.skipped, false); assert.equal(ping.calls.length, 1); assert.equal(ping.calls[0][1], 'ok'); assert.match(ping.calls[0][2], /取込 \+2 ~0 -4/);   // 前の世代は 有効期限 つきの鍵 4 つ → 全部消えて、鍵の違う 2 行が新規
  const ping2 = spyPing();
  const r2 = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping: ping2, log: quiet, readMirror: mirrorOf('2026-09-14T00:00:00Z', rowsN), connect: fakeConnect, now: () => new Date('2026-09-14T01:35:00Z') }));
  assert.equal(r2.ok, true); assert.equal(r2.skipped, true); assert.equal(ping2.calls.length, 0);
});
await t('日付が変わった最初の回は前日を締めて ok を ping (世代が同じでも)。整理と DB の大きさも note に出る', async () => {
  const ping = spyPing();
  const rowsN = [row('AAA-1', 'P3FA', '001-001-01', 11), row('bbb-2', 'P3FA', '003-002-01', 7)];
  const r = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping, log: quiet, readMirror: mirrorOf('2026-09-14T00:00:00Z', rowsN), connect: fakeConnect, now: () => new Date('2026-09-14T15:35:00Z') /* 9/15 00:35 JST */ }));
  assert.equal(r.ok, true); assert.equal(r.skipped, false); assert.equal(ping.calls[0][1], 'ok');
  assert.match(ping.calls[0][2], /skipped \/ 締め 09-14:ok\(2\) \/ 差 09-14:prev_not_complete \/ 整理 -\d+ \/ DB \d+MB/);   // 差 = 締めた日どうしの差 (stock-diff.mjs)。9/13 が missing なので 9/14 は作らない
  assert.equal((await one(`select status from snapshots.stock_capture_days where snapshot_date = date '2026-09-14'`)).status, 'complete');
});
await t('取込が失敗したら fail を ping (run は failed)', async () => {
  const ping = spyPing();
  const bad = [row('AAA-1', 'P3FA', '001-001-01', 1), row('AAA-1', 'P3FA', '001-001-01', 2)];
  const r = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping, log: quiet, readMirror: mirrorOf('2026-09-15T00:00:00Z', bad), connect: fakeConnect, now: () => new Date('2026-09-15T00:35:00Z') }));
  assert.equal(r.ok, false); assert.equal(ping.calls[0][1], 'fail'); assert.match(ping.calls[0][2], /重複.*DUPLICATE_KEY/);
});
await t('🚨 未締めの日が残る (backlog) 回は整理しない: maxDays で打ち切ると note に「まだ残りあり」、ops.job_runs は増えない。追いついた回で整理する', async () => {
  const jr0 = await num(`select count(*) as n from ops.job_runs`);
  const ping = spyPing();
  const rowsN = [row('AAA-1', 'P3FA', '001-001-01', 11), row('bbb-2', 'P3FA', '003-002-01', 7)];
  const r = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping, log: quiet, readMirror: mirrorOf('2026-09-14T00:00:00Z', rowsN), connect: fakeConnect, now: () => new Date('2026-09-18T15:35:00Z') /* 9/19 00:35 JST */, maxDays: 1 }));
  assert.equal(r.ok, true); assert.equal(ping.calls[0][1], 'ok'); assert.match(ping.calls[0][2], /締め 09-15:missing \(まだ残りあり\)/); assert.doesNotMatch(ping.calls[0][2], /整理/);
  assert.equal(await num(`select count(*) as n from ops.job_runs`), jr0);
  const r2 = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({ ping, log: quiet, readMirror: mirrorOf('2026-09-14T00:00:00Z', rowsN), connect: fakeConnect, now: () => new Date('2026-09-18T16:35:00Z') }));
  assert.equal(r2.ok, true); assert.match(ping.calls[1][2], /締め 09-16:missing 09-17:missing 09-18:missing \/ 整理/);
  assert.equal(await num(`select count(*) as n from ops.job_runs`), jr0 + 1);
  const c = await closeStockDays(db, { todayJst: '2026-09-25', maxDays: 2, log: quiet });
  assert.equal(c.closed.length, 2); assert.equal(c.backlog, true);
});
await t('🚨 締めの本体でロックが取れない (adapter の pg_try_advisory_xact_lock だけ false): rollback して capture 行を作らず、後続日も打ち切り、locked/backlog を返す。取込も同じ adapter で skipped (locked)', async () => {
  const lockDb = { query: async (sql, p) => (/pg_try_advisory_xact_lock/.test(sql) ? { rows: [{ got: false }], rowCount: 1 } : db.query(sql, p)), exec: (sql) => db.exec(sql) };
  const daysBefore = await num(`select count(*) as n from snapshots.stock_capture_days`);
  const c = await closeStockDays(lockDb, { todayJst: '2026-09-25', log: quiet });   // 9/21 が最初の未締め
  assert.equal(c.closed.length, 0); assert.equal(c.locked, true); assert.equal(c.backlog, true);
  assert.equal(await num(`select count(*) as n from snapshots.stock_capture_days`), daysBefore);
  assert.equal(await num(`select count(*) as n from snapshots.stock_capture_days where snapshot_date = date '2026-09-21'`), 0);
  const d = await closeStockDay(lockDb, '2026-09-21', { log: quiet });
  assert.equal(d.status, 'locked');
  assert.equal((await one(`select count(*)::int as n from pg_stat_activity where state = 'idle in transaction'`)).n, 0);   // 取引を開いたままにしない
  const cap = await captureLogizardInventory(lockDb, { rows: [row('AAA-1', 'P3FA', '001-001-01', 1)], capturedAt: '2026-09-21T00:00:00Z', log: quiet });
  assert.equal(cap.status, 'skipped'); assert.equal(cap.reasonCode, 'locked');
  assert.equal((await one(`select status, error from ops.ingest_runs where ingest_run_id = $1`, [cap.runId])).status, 'skipped');
  assert.equal(await num(`select count(*) as n from raw.${RAW_SRC}_observations where ingest_run_id = $1`, [cap.runId]), 0);
  // ロックが取れれば同じ日は普通に締まる (上の見送りが状態を壊していない)
  const ok2 = await closeStockDay(db, '2026-09-21', { log: quiet });
  assert.equal(ok2.status, 'missing');
});
await t('🚨 別の取込が走っていて見送った (reasonCode=locked) 回は、締めも整理もしない・ping もしない', async () => {
  const ping = spyPing(); let closeCalled = 0, maintainCalled = 0;
  const r = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({
    ping, log: quiet, readMirror: mirrorOf('2026-09-30T00:00:00Z', [row('AAA-1', 'P3FA', '001-001-01', 1)]), connect: fakeConnect, now: () => new Date('2026-09-30T15:35:00Z'),
    capture: async () => ({ status: 'skipped', reasonCode: 'locked', reason: '別の取込が走っている (advisory lock)', generation: '2026-09-30T00:00:00.000Z', seen: 1 }),
    close: async () => { closeCalled++; return { closed: [], backlog: false }; },
    maintain: async () => { maintainCalled++; return { purged: 0, dbBytes: 1 }; },
  }));
  assert.equal(r.ok, true); assert.equal(r.skipped, true); assert.match(r.note, /別の取込/);
  assert.equal(closeCalled, 0); assert.equal(maintainCalled, 0); assert.equal(ping.calls.length, 0);
  // 締めの側でロックが取れなかった回も同じ (closeStockDays は locked を返して止まる → 整理しない・note に出る)
  const ping2 = spyPing(); let m2 = 0;
  const r2 = await withEnv({ RENDER: 'true', DATA_DIR: tmp, COMPANY_DB_URL: 'postgres://x' }, () => runInventoryHourly({
    ping: ping2, log: quiet, readMirror: mirrorOf('2026-09-30T00:00:00Z', [row('AAA-1', 'P3FA', '001-001-01', 1)]), connect: fakeConnect, now: () => new Date('2026-09-30T15:35:00Z'),
    capture: async () => ({ status: 'skipped', reasonCode: 'same_generation', reason: '同じ世代', generation: '2026-09-30T00:00:00.000Z', seen: 1 }),
    close: async () => ({ closed: [], backlog: true, locked: true }),
    maintain: async () => { m2++; return { purged: 0, dbBytes: 1 }; },
  }));
  assert.equal(r2.skipped, true); assert.equal(m2, 0); assert.equal(ping2.calls.length, 0); assert.match(r2.note, /締め見送り/);
});
await t('summarize / cron は env 未設定なら起動しない', async () => {
  assert.equal(summarize({}), '何もなし');
  assert.match(summarize({ cap: { status: 'success', generation: '2026-09-14T00:00:00.000Z', added: 1, changed: 2, removed: 3, seen: 9, locationsAdded: 0 } }), /取込 \+1 ~2 -3/);
  const task = await withEnv({ RENDER: 'true', COMPANY_DB_INVENTORY_CRON_ENABLED: undefined }, () => startCompanyDbInventoryHourlyCron());
  assert.equal(task, null);
  const bad = await withEnv({ RENDER: 'true', COMPANY_DB_INVENTORY_CRON_ENABLED: '1', COMPANY_DB_INVENTORY_CRON: 'not cron' }, () => startCompanyDbInventoryHourlyCron());
  assert.equal(bad, null);
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
