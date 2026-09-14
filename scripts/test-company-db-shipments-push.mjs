#!/usr/bin/env node
/**
 * test-company-db-shipments-push.mjs — D5a (NE 伝票 → Company DB の push) の受入試験 (08 §4.7 / §9 D5)
 *
 *   整形 (純粋関数): JST → +09:00 / '' → null / 明細の並び / 指紋は鍵順と時刻に依らない / 欠落は例外
 *   読み出し (SQLite :memory:): 伝票番号順の流し読みでヘッダと明細がそろう / ヘッダ無しの明細 / 範囲 (D-28 + 投入済み)
 *   台帳: 指紋の差分 (synced_at が古くても内容が変われば送る = Codex R1 #1) / 投入済みは範囲から外れても追跡 (R1 #2) / lock と世代の採番 (R1 #3)
 *   受け口 (PGlite): validateChunk / 1 chunk = 1 取引 (applied・same・stale) / 失敗の切り分け (savepoint) / 再送は保存した応答 (R1 #4) / run は last と全 chunk で閉じる / 期限超過 (R1 #5)
 *   通し (送り手 ⇄ 受け口。fetch を差し替え): chunk 分割と last / 台帳と世代 / 変更は次の世代 / 失敗は台帳に書かない / 応答を失った再送 / 5xx 再送と 4xx 即失敗 / 期限超過の分割 / dry-run / --force
 *   突合: diffDaily と reconcile の通し (旧 rebuild-shipments-daily.js を本物で呼ぶ) / 窓の分割
 * 実行: node scripts/test-company-db-shipments-push.mjs
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildShipment, contentHash, jstToIso, jstDateOnly, utcToIso, canonicalJson, TRANSFORM_VERSION } from '../apps/company-db/push/ne-shipments-transform.mjs';
import { iterateSlips, pushShipments, reconcileShipmentsDaily, diffDaily, splitWindows, fingerprint, summarizeResult, syncBase, newRunId } from '../apps/company-db/push/ne-shipments.mjs';
import { openLedger, SEQ_KEY, LOCK_KEY } from '../apps/company-db/push/ledger.mjs';
import { ingestShipmentChunk, validateChunk, payloadChecksum, MAX_ROWS_PER_CHUNK, MAX_LINES_PER_CHUNK } from '../apps/company-db/ingest/shipments.mjs';
import { rebuildShipmentsDaily } from '../apps/warehouse/rebuild-shipments-daily.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`); return threw; };
const quiet = () => {};

// ─── SQLite の見本 (warehouse.db と同じ列) ───
function openWarehouse() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_ne_orders (伝票番号 TEXT, 受注番号 TEXT, 受注状態区分 TEXT, 受注状態 TEXT, 受注キャンセル TEXT, 受注キャンセル日 TEXT, 受注日 TEXT, 店舗コード TEXT, 出荷確定日 TEXT,
    明細行番号 INTEGER, レコードナンバー TEXT, キャンセル区分 TEXT, 商品コード TEXT, 商品名 TEXT, 商品OP TEXT, 受注数 INTEGER, 引当数 INTEGER, 小計金額 REAL, synced_at TEXT, PRIMARY KEY (伝票番号, 明細行番号))`);
  db.exec(`CREATE TABLE raw_ne_order_base (伝票番号 TEXT PRIMARY KEY, 受注番号 TEXT, 店舗コード TEXT, 受注日 TEXT, 出荷確定日 TEXT, 受注状態区分 TEXT, 受注状態 TEXT, キャンセル区分 TEXT, 受注キャンセル日 TEXT,
    配送方法ID TEXT, 配送方法名 TEXT, 送り状番号 TEXT, synced_at TEXT)`);
  db.exec(`CREATE TABLE f_shipments_daily (ship_date TEXT NOT NULL, shop_code TEXT NOT NULL, delivery_id TEXT NOT NULL, delivery_name TEXT, slips INTEGER NOT NULL, cancelled_slips INTEGER NOT NULL DEFAULT 0, updated_at TEXT, PRIMARY KEY (ship_date, shop_code, delivery_id))`);
  db.exec(`CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)`);
  return db;
}
const base = (x) => ({ 伝票番号: x.slip, 受注番号: x.orderNo ?? `${x.slip}-ORD`, 店舗コード: x.shop ?? '1', 受注日: x.orderDate ?? '2025-03-01 10:00:00', 出荷確定日: x.shipped ?? '2025-03-01 15:00:00',
  受注状態区分: x.status ?? '50', 受注状態: '出荷確定済', キャンセル区分: x.cancelled ? 'キャンセル' : '有効', 受注キャンセル日: x.cancelledAt ?? '', 配送方法ID: x.deliv ?? '28', 配送方法名: x.delivName ?? 'ネコポス', 送り状番号: x.tracking ?? '', synced_at: x.synced ?? '2026-09-10 00:00:00' });
const line = (slip, no, x = {}) => ({ 伝票番号: slip, 受注番号: `${slip}-ORD`, 受注状態区分: '50', 受注状態: '出荷確定済', 受注キャンセル: '有効な受注です。', 受注キャンセル日: '', 受注日: '2025-03-01 10:00:00', 店舗コード: '1', 出荷確定日: '2025-03-01 15:00:00',
  明細行番号: no, レコードナンバー: '', キャンセル区分: x.cancelled ? 'キャンセル' : '有効', 商品コード: x.code ?? `sku-${no}`, 商品名: '見本', 商品OP: '', 受注数: x.qty === undefined ? 2 : x.qty, 引当数: x.alloc ?? 2, 小計金額: 1000, synced_at: x.synced ?? '2026-09-10 00:00:00' });
function insertBase(db, b) { db.prepare(`insert into raw_ne_order_base values (@伝票番号,@受注番号,@店舗コード,@受注日,@出荷確定日,@受注状態区分,@受注状態,@キャンセル区分,@受注キャンセル日,@配送方法ID,@配送方法名,@送り状番号,@synced_at)`).run(b); }
function insertLine(db, l) { db.prepare(`insert into raw_ne_orders values (@伝票番号,@受注番号,@受注状態区分,@受注状態,@受注キャンセル,@受注キャンセル日,@受注日,@店舗コード,@出荷確定日,@明細行番号,@レコードナンバー,@キャンセル区分,@商品コード,@商品名,@商品OP,@受注数,@引当数,@小計金額,@synced_at)`).run(l); }

// ─── PGlite (受け口) ───
const pg = new PGlite();
const pdb = pgliteAdapter(pg);
const applied0 = await applyMigrations(pdb, { log: quiet });
assert.ok(applied0.applied.includes('0015'), '0015 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
await pg.query(`insert into core.products (company_id, name) values (1, '見本')`);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select 1, product_id, 'single', 'sku-1', name from core.products`);
const RUN = (n) => `ship_202609140000000_${String(n).padStart(6, '0')}`;   // 15 桁 (ISO の ms の先頭 1 桁まで)
const BASE_URL = 'https://portal.example/apps/company-db/sync';
const row = (b, lines = []) => { const r = buildShipment(b, lines); return { ne_slip_no: r.ne_slip_no, header: r.header, lines: r.lines }; };
/** 送り手が叩く fetch の代わり: 受け口の関数を直接呼ぶ (HTTP 層だけを飛ばす)。before で割り込める (再送・期限・失敗の再現) */
function fakeFetch(opts = {}) {
  const calls = [];
  const f = async (url, init = {}) => {
    calls.push({ url, init });
    if (opts.before) { const r = await opts.before(calls.length, url, init); if (r) return r; }
    if (init.method === 'POST' && url === `${BASE_URL}/shipments`) {
      assert.equal(init.headers['x-sync-key'], 'k');
      let chunk; try { chunk = validateChunk(JSON.parse(init.body)); } catch (e) { return new Response(JSON.stringify({ error: e.message }), { status: 400 }); }
      try {
        const r = await ingestShipmentChunk(pdb, { ...chunk, log: quiet, ...(opts.ingest || {}) });
        if (opts.after) { const x = await opts.after(calls.length, r); if (x) return x; }
        return new Response(JSON.stringify(r), { status: 200 });
      } catch (e) {
        const status = e.code === 'CHUNK_DEADLINE' ? 503 : (e.code === 'RUN_MISMATCH' || e.code === 'CHUNK_MISMATCH' || e.code === 'RUN_CLOSED') ? 409 : 500;
        return new Response(JSON.stringify({ error: e.message, code: e.code || null }), { status });
      }
    }
    if (url.startsWith(`${BASE_URL}/shipments/daily?`)) {
      const u = new URL(url);
      const rows = (await pg.query(`select ship_date::text as ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips from mart.v_shipments_daily where company_id = 1 and ship_date between $1::date and $2::date`, [u.searchParams.get('from'), u.searchParams.get('to')])).rows;
      return new Response(JSON.stringify({ rows }), { status: 200 });
    }
    return new Response('not found', { status: 404 });
  };
  f.calls = calls;
  f.posts = () => calls.filter((c) => c.init.method === 'POST').map((c) => JSON.parse(c.init.body));
  return f;
}
const push = (w, l, f, x = {}) => pushShipments({ warehouse: w, ledger: l, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });

console.log('D5a: 整形');
await t('JST → +09:00 / 日付だけ / UTC の synced_at → Z / 空は null / 形が違えば例外', async () => {
  assert.equal(jstToIso('2025-03-01 15:04:05'), '2025-03-01T15:04:05+09:00');
  assert.equal(jstToIso('2025-03-01'), '2025-03-01T00:00:00+09:00');
  assert.equal(jstToIso(''), null); assert.equal(jstToIso(null), null); assert.equal(jstToIso('  '), null);
  assert.equal(jstDateOnly('2025-03-01 23:59:59'), '2025-03-01');
  assert.equal(utcToIso('2026-09-10 00:00:00'), '2026-09-10T00:00:00Z');
  await rejects(async () => jstToIso('2025/03/01'), /形が違う/);
  await rejects(async () => jstToIso('2025-13-01 00:00:00'), /不正/);
});
await t('1 伝票の整形: 列の対応 / 明細は行番号順 / 指紋は鍵順と source_updated_at に依らない / 内容が変われば指紋が変わる', async () => {
  const b = base({ slip: 'S1', tracking: 'T-1', cancelled: true, cancelledAt: '2025-03-02 09:00:00' });
  const r = buildShipment(b, [line('S1', 2, { code: 'sku-2', qty: 1, alloc: 0, cancelled: true, synced: '2026-09-11 00:00:00' }), line('S1', 1, { code: 'SKU-1', qty: 3 })]);
  assert.equal(r.ne_slip_no, 'S1');
  const h = r.header;
  assert.equal(h.ne_order_no, 'S1-ORD'); assert.equal(h.shop_code, '1'); assert.equal(h.ne_status_code, '50');
  assert.equal(h.is_cancelled, true); assert.equal(h.cancelled_at, '2025-03-02T09:00:00+09:00');
  assert.equal(h.shipped_at, '2025-03-01T15:00:00+09:00'); assert.equal(h.order_date_jst, '2025-03-01');
  assert.equal(h.delivery_method_code, '28'); assert.equal(h.delivery_method_name, 'ネコポス');
  assert.equal(h.tracking_no, 'T-1'); assert.equal(h.tracking_source, 'ne'); assert.equal(h.carrier, null); assert.equal(h.batch_code, null);
  assert.equal(h.source_updated_at, '2026-09-11T00:00:00Z');             // ヘッダと明細の synced_at の最大
  assert.equal(r.source_updated_at, '2026-09-11 00:00:00'); assert.equal(h.transform_version, TRANSFORM_VERSION);
  assert.deepEqual(r.lines, [{ line_no: '1', sku_code: 'SKU-1', qty: 3, allocated_qty: 2, is_cancelled: false }, { line_no: '2', sku_code: 'sku-2', qty: 1, allocated_qty: 0, is_cancelled: true }]);
  assert.equal(h.content_hash.length, 64);
  const reordered = Object.fromEntries(Object.entries(h).reverse());
  assert.equal(contentHash(reordered), h.content_hash);
  assert.equal(contentHash({ ...h, source_updated_at: '2030-01-01T00:00:00Z', transform_version: 'x' }), h.content_hash);
  assert.notEqual(buildShipment({ ...b, 送り状番号: 'T-2' }, []).header.content_hash, h.content_hash);
  assert.equal(canonicalJson({ b: [1, { z: 1, a: null }], a: 'x' }), '{"a":"x","b":[1,{"a":null,"z":1}]}');
  // 伝票の指紋 (台帳用) はヘッダと明細の両方を見る。synced_at だけ変わっても同じ
  const fp1 = fingerprint(r);
  assert.equal(fingerprint(buildShipment({ ...b, synced_at: '2026-12-31 00:00:00' }, [line('S1', 2, { code: 'sku-2', qty: 1, alloc: 0, cancelled: true }), line('S1', 1, { code: 'SKU-1', qty: 3 })])), fp1);
  assert.notEqual(fingerprint(buildShipment(b, [line('S1', 2, { code: 'sku-2', qty: 1, alloc: 0, cancelled: true }), line('S1', 1, { code: 'SKU-1', qty: 4 })])), fp1);   // 明細の数量
  const r2 = buildShipment(base({ slip: 'S2', orderNo: '', shop: '', shipped: '', deliv: '', delivName: '' }), []);
  assert.equal(r2.header.ne_order_no, null); assert.equal(r2.header.shop_code, null); assert.equal(r2.header.shipped_at, null); assert.equal(r2.header.delivery_method_code, null);
  assert.equal(r2.header.is_cancelled, false); assert.equal(r2.header.cancelled_at, null); assert.equal(r2.header.tracking_source, null); assert.deepEqual(r2.lines, []);
});
await t('整形の歯止め: 受注数が無い / 行番号が重複 / 別の伝票の明細 / 日時の形 / synced_at が無い (fallback が無ければ例外・あれば印)', async () => {
  await rejects(async () => buildShipment(base({ slip: 'S1' }), [line('S1', 1, { qty: null })]), /受注数が無い/);
  await rejects(async () => buildShipment(base({ slip: 'S1' }), [line('S1', 1), line('S1', 1)]), /重複/);
  await rejects(async () => buildShipment(base({ slip: 'S1' }), [line('S9', 1)]), /別の伝票/);
  await rejects(async () => buildShipment(base({ slip: 'S1', shipped: '2025/03/01' }), []), /出荷確定日の形が違う/);
  await rejects(async () => buildShipment(base({ slip: 'S1', synced: '' }), []), /synced_at が 1 つも無い/);
  const r = buildShipment(base({ slip: 'S1', synced: '' }), [], { fallbackSourceUpdatedAt: '2026-09-14T00:00:00.000Z' });
  assert.equal(r.no_synced_at, true); assert.equal(r.header.source_updated_at, '2026-09-14T00:00:00.000Z'); assert.equal(r.source_updated_at, null);
});

console.log('D5a: 読み出しと台帳 (SQLite)');
await t('伝票番号順の流し読み: ヘッダと明細がそろう (順序が違っても) / ヘッダ無しの明細は別に数える', async () => {
  const db = openWarehouse();
  insertBase(db, base({ slip: '30' })); insertBase(db, base({ slip: '10' })); insertBase(db, base({ slip: '20' }));
  insertLine(db, line('20', 2)); insertLine(db, line('20', 1)); insertLine(db, line('10', 1)); insertLine(db, line('05', 1)); insertLine(db, line('25', 1)); insertLine(db, line('99', 1)); insertLine(db, line('99', 2));
  const noBase = [];
  const got = [...iterateSlips(db, { onLinesWithoutBase: (s) => noBase.push(s) })].map((s) => [s.base.伝票番号, s.lines.map((l) => l.明細行番号)]);
  assert.deepEqual(got, [['10', [1]], ['20', [1, 2]], ['30', []]]);
  assert.deepEqual([...new Set(noBase)], ['05', '25', '99']);
  db.close();
});
await t('台帳: lock (奪えない・ttl を過ぎたら奪える・持ち主だけ外せる) / 世代は取引の中で +1 / 指紋の upsert / run の記録', async () => {
  const l = openLedger(null, { memory: true });
  const t0 = new Date('2026-09-14T00:00:00Z');
  assert.deepEqual(l.acquireLock({ owner: 'a', now: t0 }), { ok: true, held: null });
  const r2 = l.acquireLock({ owner: 'b', now: new Date(t0.getTime() + 3600000) });
  assert.equal(r2.ok, false); assert.equal(r2.held.owner, 'a');
  assert.equal(l.releaseLock('b'), false);                                                         // 持ち主でないと外せない
  assert.equal(l.acquireLock({ owner: 'b', now: new Date(t0.getTime() + 7 * 3600000) }).ok, true);   // 6 時間過ぎた lock は死んだとみなして奪う
  assert.equal(l.releaseLock('a'), false); assert.equal(l.releaseLock('b'), true); assert.equal(l.getMeta(LOCK_KEY), null);
  assert.equal(l.currentBatchSeq(), 0); assert.equal(l.nextBatchSeq(), 1); assert.equal(l.nextBatchSeq(), 2); assert.equal(l.getMeta(SEQ_KEY), '2');
  l.markSent([{ ne_slip_no: 'A', fp: 'x' }, { ne_slip_no: 'B', fp: 'y' }], 2); l.markSent([{ ne_slip_no: 'A', fp: 'z' }], 3);
  assert.deepEqual([...l.loadFingerprints()], [['A', 'z'], ['B', 'y']]); assert.equal(l.countSent(), 2);
  l.recordRun({ run_id: 'r1', mode: 'incremental', started_at: '2026-09-14T00:00:00Z', batch_seq: 2 });
  l.recordRun({ run_id: 'r1', mode: 'incremental', started_at: '2026-09-14T00:00:00Z', finished_at: '2026-09-14T00:01:00Z', batch_seq: 2, sent: 5, ok: 1 });
  assert.deepEqual(l.lastRuns(1).map((r) => [r.run_id, r.sent, r.ok, r.finished_at]), [['r1', 5, 1, '2026-09-14T00:01:00Z']]);
  l.close();
});

console.log('D5a: 受け口 (PGlite)');
await t('validateChunk: run_id の形 / 世代 / chunk_index と last / 伝票番号の形と重複 / transform_version の不一致 / 明細の型と上限', async () => {
  const good = { run_id: RUN(1), batch_seq: 1, chunk_index: 0, last: true, transform_version: TRANSFORM_VERSION, rows: [{ ne_slip_no: 'S1', header: { transform_version: TRANSFORM_VERSION }, lines: [] }] };
  const v = validateChunk(good); assert.equal(v.runId, RUN(1)); assert.equal(v.rows.length, 1); assert.equal(v.last, true);
  assert.equal(validateChunk({ ...good, rows: [{ ...good.rows[0], ne_slip_no: 1485081 }] }).rows[0].ne_slip_no, '1485081');   // 数字でも文字列に
  for (const [patch, re] of [[{ run_id: 'load_x' }, /run_id/], [{ batch_seq: 0 }, /batch_seq/], [{ chunk_index: -1 }, /chunk_index/], [{ last: 'yes' }, /last must be a boolean/], [{ transform_version: '' }, /transform_version is required/],
    [{ rows: [good.rows[0], good.rows[0]] }, /duplicate/], [{ rows: [{ ...good.rows[0], ne_slip_no: {} }] }, /ne_slip_no must be a string/], [{ rows: [{ ...good.rows[0], ne_slip_no: '' }] }, /bad form/], [{ rows: [{ ...good.rows[0], ne_slip_no: 'a b' }] }, /bad form/],
    [{ rows: [{ ...good.rows[0], header: { transform_version: 'other' } }] }, /differs/], [{ rows: [{ ne_slip_no: 'S1', header: {}, lines: {} }] }, /lines must be an array/], [{ rows: [{ ...good.rows[0], lines: [1] }] }, /contain objects/],
    [{ rows: [{ ...good.rows[0], lines: Array.from({ length: 501 }, () => ({})) }] }, /<= 500/],
    [{ rows: Array.from({ length: 11 }, (_, i) => ({ ne_slip_no: `S${i}`, header: { transform_version: TRANSFORM_VERSION }, lines: Array.from({ length: 500 }, () => ({})) })) }, new RegExp(`<= ${MAX_LINES_PER_CHUNK} per chunk`)],
    [{ rows: Array.from({ length: MAX_ROWS_PER_CHUNK + 1 }, (_, i) => ({ ne_slip_no: `S${i}`, header: { transform_version: TRANSFORM_VERSION }, lines: [] })) }, /<= 1000/]]) {
    await rejects(async () => validateChunk({ ...good, ...patch }), re);
  }
  assert.equal(payloadChecksum([{ a: 1, b: 2 }]), payloadChecksum([{ b: 2, a: 1 }]));
});
await t('1 chunk = 1 取引: applied → same (世代だけ進む) → stale (stale_slips に伝票)。列の対応 (ship_date_jst は JST の日付・状態は対応表・SKU の解決)。ops.ingest_runs / ingest_chunks', async () => {
  const rows = [row(base({ slip: 'P1', tracking: 'T1' }), [line('P1', 1, { code: 'sku-1', qty: 2 }), line('P1', 2, { code: 'nope', qty: 1 })]), row(base({ slip: 'P2', shipped: '', status: '20' })), row(base({ slip: 'P3', cancelled: true, cancelledAt: '2025-03-02 00:00:00' }), [line('P3', 1)])];
  const chunk = { runId: RUN(2), batchSeq: 5, chunkIndex: 0, last: true, transformVersion: TRANSFORM_VERSION, rows, log: quiet };
  const r1 = await ingestShipmentChunk(pdb, chunk);
  assert.deepEqual([r1.applied, r1.same, r1.stale, r1.failed.length, r1.stale_slips, r1.replay, r1.finished], [3, 0, 0, 0, [], false, true]);
  const s1 = await one(`select ship_date_jst::text as d, status, tracking_no, tracking_source, received_batch_seq::int as seq, shop_code, ne_order_no from core.shipments where ne_slip_no = 'P1'`);
  assert.deepEqual(s1, { d: '2025-03-01', status: 'shipped', tracking_no: 'T1', tracking_source: 'ne', seq: 5, shop_code: '1', ne_order_no: 'P1-ORD' });
  assert.equal((await one(`select status from core.shipments where ne_slip_no = 'P2'`)).status, 'ready');     // NE 20 = 納品書印刷待ち → ready
  assert.equal((await one(`select ship_date_jst from core.shipments where ne_slip_no = 'P2'`)).ship_date_jst, null);
  assert.equal((await one(`select status from core.shipments where ne_slip_no = 'P3'`)).status, 'cancelled');
  const lines = (await pg.query(`select l.line_no, l.qty, l.sku_id is not null as resolved, l.unresolved_code from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'P1' order by l.line_no`)).rows;
  assert.deepEqual(lines, [{ line_no: '1', qty: 2, resolved: true, unresolved_code: null }, { line_no: '2', qty: 1, resolved: false, unresolved_code: 'nope' }]);
  const run = await one(`select status, complete, rows_seen, rows_inserted, rows_skipped, checksum, format_version, pages, failed_ranges from ops.ingest_runs where ingest_run_id = $1`, [RUN(2)]);
  assert.deepEqual([run.status, run.complete, run.rows_seen, run.rows_inserted, run.rows_skipped, run.checksum, run.format_version, run.pages, run.failed_ranges], ['success', true, 3, 3, 0, '5', TRANSFORM_VERSION, 1, []]);
  const ch = await one(`select rows_seen, rows_applied, rows_same, rows_stale, rows_failed, payload_checksum, result from ops.ingest_chunks where ingest_run_id = $1 and chunk_index = 0`, [RUN(2)]);
  assert.deepEqual([ch.rows_seen, ch.rows_applied, ch.rows_same, ch.rows_stale, ch.rows_failed, ch.payload_checksum, ch.result.applied], [3, 3, 0, 0, 0, payloadChecksum(rows), 3]);
  await rejects(() => pg.query(`delete from ops.ingest_chunks where ingest_run_id = $1`, [RUN(2)]), /append-only/);
  const r2 = await ingestShipmentChunk(pdb, { ...chunk, runId: RUN(3), batchSeq: 6 });
  assert.deepEqual([r2.applied, r2.same, r2.stale], [0, 3, 0]);
  assert.equal((await one(`select received_batch_seq::int as s from core.shipments where ne_slip_no = 'P1'`)).s, 6);
  const r3 = await ingestShipmentChunk(pdb, { ...chunk, runId: RUN(4), batchSeq: 4 });
  assert.deepEqual([r3.applied, r3.same, r3.stale, r3.stale_slips], [0, 0, 3, ['P1', 'P2', 'P3']]);
  assert.equal((await one(`select rows_skipped from ops.ingest_runs where ingest_run_id = $1`, [RUN(4)])).rows_skipped, 3);
});
await t('失敗した伝票だけ切り分ける (savepoint)。run は last が来て全 chunk がそろったときだけ閉じる (partial + 失敗の総数)。順序が前後しても閉じる', async () => {
  const good = row(base({ slip: 'Q1' }), [line('Q1', 1)]);
  const bad1 = row(base({ slip: 'Q2' }), [line('Q2', 1)]); bad1.lines[0].qty = null;             // DB が拒む (qty 必須)
  const bad2 = row(base({ slip: 'Q3' })); delete bad2.header.content_hash;
  const c1 = await ingestShipmentChunk(pdb, { runId: RUN(5), batchSeq: 7, chunkIndex: 0, last: false, transformVersion: TRANSFORM_VERSION, rows: [good, bad1, bad2], log: quiet });
  assert.deepEqual([c1.applied, c1.failed.length, c1.finished], [1, 2, false]);
  assert.deepEqual(c1.failed.map((f) => f.ne_slip_no), ['Q2', 'Q3']);
  assert.match(c1.failed[0].error, /line_no and qty/); assert.match(c1.failed[1].error, /content_hash/);
  assert.equal(await num(`select count(*) as n from core.shipments where ne_slip_no in ('Q1','Q2','Q3')`), 1);
  // last (index 2) が先に来ても、index 1 がまだ無いので閉じない
  const c3 = await ingestShipmentChunk(pdb, { runId: RUN(5), batchSeq: 7, chunkIndex: 2, last: true, transformVersion: TRANSFORM_VERSION, rows: [row(base({ slip: 'Q5' }))], log: quiet });
  assert.equal(c3.finished, false);
  let run = await one(`select status, finished_at, rows_seen, pages from ops.ingest_runs where ingest_run_id = $1`, [RUN(5)]);
  assert.deepEqual([run.status, run.finished_at, run.rows_seen, run.pages], ['running', null, 4, 3]);
  const c2 = await ingestShipmentChunk(pdb, { runId: RUN(5), batchSeq: 7, chunkIndex: 1, last: false, transformVersion: TRANSFORM_VERSION, rows: [good], log: quiet });
  assert.deepEqual([c2.same, c2.finished], [1, true]);
  run = await one(`select status, complete, rows_seen, rows_inserted, rows_skipped, error, failed_ranges from ops.ingest_runs where ingest_run_id = $1`, [RUN(5)]);
  assert.deepEqual([run.status, run.complete, run.rows_seen, run.rows_inserted, run.rows_skipped], ['partial', true, 5, 2, 1]);
  assert.match(run.error, /2 slips failed/); assert.deepEqual(run.failed_ranges.map((f) => f.ne_slip_no), ['Q2', 'Q3']);
  // 閉じた run には入れない。番号の範囲外も入れない
  await rejects(() => ingestShipmentChunk(pdb, { runId: RUN(5), batchSeq: 7, chunkIndex: 3, last: false, transformVersion: TRANSFORM_VERSION, rows: [good], log: quiet }), /already partial|beyond/);
});
await t('再送 (同じ run + chunk_index + 同じ内容) は適用せず保存した応答を返す = 集計が二重にならない。違う内容 / 違う世代 / 違う版は 409 の code', async () => {
  const rows = [row(base({ slip: 'R1' }), [line('R1', 1)])];
  const a = await ingestShipmentChunk(pdb, { runId: RUN(6), batchSeq: 8, chunkIndex: 0, last: false, transformVersion: TRANSFORM_VERSION, rows, log: quiet });
  const b = await ingestShipmentChunk(pdb, { runId: RUN(6), batchSeq: 8, chunkIndex: 0, last: false, transformVersion: TRANSFORM_VERSION, rows, log: quiet });
  assert.deepEqual([a.applied, a.replay, b.applied, b.replay, b.run_id, b.chunk_index], [1, false, 1, true, RUN(6), 0]);
  assert.deepEqual(await one(`select rows_seen, rows_inserted from ops.ingest_runs where ingest_run_id = $1`, [RUN(6)]), { rows_seen: 1, rows_inserted: 1 });
  const e1 = await rejects(() => ingestShipmentChunk(pdb, { runId: RUN(6), batchSeq: 8, chunkIndex: 0, last: false, transformVersion: TRANSFORM_VERSION, rows: [row(base({ slip: 'R1', tracking: 'X' }), [line('R1', 1)])], log: quiet }), /different content/);
  assert.equal(e1.code, 'CHUNK_MISMATCH');
  const e2 = await rejects(() => ingestShipmentChunk(pdb, { runId: RUN(6), batchSeq: 9, chunkIndex: 1, last: false, transformVersion: TRANSFORM_VERSION, rows, log: quiet }), /was started with batch_seq 8/);
  assert.equal(e2.code, 'RUN_MISMATCH');
  const e3 = await rejects(() => ingestShipmentChunk(pdb, { runId: RUN(6), batchSeq: 8, chunkIndex: 1, last: false, transformVersion: 'ne-shipments-9', rows: [{ ...rows[0], header: { ...rows[0].header, transform_version: 'ne-shipments-9' } }], log: quiet }), /was started with/);
  assert.equal(e3.code, 'RUN_MISMATCH');
  assert.equal(await num(`select count(*) as n from ops.ingest_chunks where ingest_run_id = $1`, [RUN(6)]), 1);
});
await t('期限超過: chunk 全体の期限を過ぎたら全部 rollback して CHUNK_DEADLINE (伝票は 1 つも残らない)', async () => {
  let tick = 0;
  const rows = [row(base({ slip: 'D1' })), row(base({ slip: 'D2' })), row(base({ slip: 'D3' }))];
  const e = await rejects(() => ingestShipmentChunk(pdb, { runId: RUN(7), batchSeq: 10, chunkIndex: 0, last: true, transformVersion: TRANSFORM_VERSION, rows, log: quiet, deadlineMs: 100, now: () => (tick += 60) }), /exceeded 100 ms after 1 of 3/);
  assert.equal(e.code, 'CHUNK_DEADLINE');
  assert.equal(await num(`select count(*) as n from core.shipments where ne_slip_no in ('D1','D2','D3')`), 0);
  assert.equal(await num(`select count(*) as n from ops.ingest_runs where ingest_run_id = $1`, [RUN(7)]), 0);
});

console.log('D5a: 通し (送り手 ⇄ 受け口)');
await t('初回は範囲の全部を chunk に分けて送る (last で run が閉じる) → 台帳に指紋 → 2 回目は変化なし (何も送らない・世代は進まない) → 変更は次の世代で applied → dry-run と --force', async () => {
  const w = openWarehouse(), l = openLedger(null, { memory: true });
  insertBase(w, base({ slip: 'E1', synced: '2026-09-10 00:00:00' })); insertLine(w, line('E1', 1, { code: 'sku-1' }));
  insertBase(w, base({ slip: 'E2', synced: '2026-09-10 00:00:01' })); insertLine(w, line('E2', 1)); insertLine(w, line('E2', 2));
  insertBase(w, base({ slip: 'E3', synced: '2026-09-10 00:00:02', shipped: '', status: '2' }));
  insertBase(w, base({ slip: 'OLD', orderDate: '2024-12-20 10:00:00', shipped: '2024-12-22 10:00:00' }));                    // 範囲外 (受注も出荷も 2024)
  insertBase(w, base({ slip: 'EDGE', orderDate: '2024-12-30 10:00:00', shipped: '2025-01-02 10:00:00' }));                   // 2024 の注文だが 2025 に出荷 → 入る
  const f = fakeFetch();
  const dry = await push(w, l, f, { dryRun: true });
  assert.deepEqual([dry.scanned, dry.inScope, dry.changed, dry.sent, f.calls.length, l.currentBatchSeq(), l.countSent()], [5, 4, 4, 0, 0, 0, 0]);
  assert.equal(dry.example.ne_slip_no, 'E1');
  const r1 = await push(w, l, f, { chunkSize: 2 });
  assert.deepEqual([r1.ok, r1.inScope, r1.changed, r1.sent, r1.applied, r1.same, r1.chunks, r1.batchSeq, l.countSent()], [true, 4, 4, 4, 4, 0, 2, 1, 4]);
  assert.match(r1.runId, /^ship_[0-9]{15}_[0-9a-f]{6}$/);
  assert.deepEqual(f.posts().map((p) => [p.chunk_index, p.last, p.rows.length]), [[0, false, 2], [1, true, 2]]);
  assert.equal((await one(`select status, pages from ops.ingest_runs where ingest_run_id = $1`, [r1.runId])).status, 'success');
  assert.equal(await num(`select count(*) as n from core.shipments where ne_slip_no in ('E1','E2','E3','EDGE')`), 4);
  assert.equal(await num(`select count(*) as n from core.shipments where ne_slip_no = 'OLD'`), 0);
  assert.match(summarizeResult(r1), /✅ Company DB 出荷 push: 変わった 4 伝票を送った \(applied 4 \/ same 0/);
  assert.deepEqual(l.lastRuns(1).map((x) => [x.run_id, x.sent, x.applied, x.ok]), [[r1.runId, 4, 4, 1]]);
  // 2 回目: 何も変わっていない → 送らない・世代も進まない (lock は外れている)
  const r2 = await push(w, l, f, {});
  assert.deepEqual([r2.ok, r2.changed, r2.unchanged, r2.sent, r2.chunks, l.currentBatchSeq()], [true, 0, 4, 0, 0, 1]);
  assert.equal(l.getMeta(LOCK_KEY), null);
  // 明細が変わる (数量 2 → 5) が synced_at は**古いまま** → それでも送る (指紋で判定。Codex R1 #1)
  w.prepare(`update raw_ne_orders set 受注数 = 5, synced_at = '2020-01-01 00:00:00' where 伝票番号 = 'E1'`).run();
  const r3 = await push(w, l, f, {});
  assert.deepEqual([r3.ok, r3.changed, r3.applied, r3.batchSeq], [true, 1, 1, 2]);
  assert.equal((await one(`select l.qty from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'E1' and l.line_no = '1'`)).qty, 5);
  // --force: 指紋が同じでも送る → 'same'
  const r4 = await push(w, l, f, { force: true });
  assert.deepEqual([r4.ok, r4.changed, r4.same, r4.applied], [true, 4, 4, 0]);
  w.close(); l.close();
});
await t('投入済みの伝票は範囲の条件から外れても追跡する (出荷確定日を消す訂正が届き、Render の ship_date が null に戻る。Codex R1 #2)', async () => {
  const w = openWarehouse(), l = openLedger(null, { memory: true });
  insertBase(w, base({ slip: 'X1', orderDate: '2024-12-30 10:00:00', shipped: '2025-01-05 10:00:00' }));
  const f = fakeFetch();
  const r1 = await push(w, l, f, {}); assert.deepEqual([r1.inScope, r1.applied], [1, 1]);
  assert.equal((await one(`select ship_date_jst::text as d from core.shipments where ne_slip_no = 'X1'`)).d, '2025-01-05');
  w.prepare(`update raw_ne_order_base set 出荷確定日 = '' where 伝票番号 = 'X1'`).run();
  const r2 = await push(w, l, f, {}); assert.deepEqual([r2.inScope, r2.changed, r2.applied], [1, 1, 1]);
  assert.equal((await one(`select ship_date_jst from core.shipments where ne_slip_no = 'X1'`)).ship_date_jst, null);
  w.close(); l.close();
});
await t('失敗は台帳に書かない (整形できない / Render が拒んだ → ok=false、次回また送る)。stale も書かない。受注日の範囲は台帳に書くので後の incremental は残りだけ', async () => {
  const w = openWarehouse(), l = openLedger(null, { memory: true });
  insertBase(w, base({ slip: 'F1' })); insertLine(w, line('F1', 1));
  insertBase(w, base({ slip: 'F2' })); insertLine(w, line('F2', 1, { qty: null }));                                  // 整形できない
  insertBase(w, base({ slip: 'F3', orderDate: '2025-04-10 10:00:00' }));
  const f = fakeFetch();
  const r1 = await push(w, l, f, {});
  assert.deepEqual([r1.ok, r1.inScope, r1.transformErrors.length, r1.sent, r1.applied, l.countSent()], [false, 3, 1, 2, 2, 2]);
  assert.match(summarizeResult(r1), /^❌/);
  // Render 側で拒まれる伝票 (受け口に渡す直前に content_hash を消す) → failed、台帳には残らない → 次回また送る
  w.prepare(`update raw_ne_orders set 受注数 = 1 where 伝票番号 = 'F2'`).run();
  w.prepare(`update raw_ne_order_base set 送り状番号 = 'T-F1' where 伝票番号 = 'F1'`).run();
  const f2 = fakeFetch({ before: async (n, url, init) => {
    if (init.method !== 'POST') return null;
    const body = JSON.parse(init.body); const bad = body.rows.find((x) => x.ne_slip_no === 'F1'); if (bad) delete bad.header.content_hash;
    const chunk = validateChunk(body); const r = await ingestShipmentChunk(pdb, { ...chunk, log: quiet });
    return new Response(JSON.stringify(r), { status: 200 });
  } });
  const r2 = await push(w, l, f2, {});
  assert.deepEqual([r2.ok, r2.changed, r2.sent, r2.applied, r2.failed.map((x) => x.ne_slip_no), l.countSent()], [false, 2, 2, 1, ['F1'], 3]);
  const r3 = await push(w, l, fakeFetch(), {});
  assert.deepEqual([r3.ok, r3.changed, r3.applied], [true, 1, 1]);                                                          // F1 だけもう一度
  // stale: Render のほうが新しい世代 (別の送り手が先に世代 99 で送った) → 台帳に書かず、次回また送る
  const item = row(base({ slip: 'F3', orderDate: '2025-04-10 10:00:00', tracking: 'NEWER' }));
  await ingestShipmentChunk(pdb, { runId: RUN(8), batchSeq: 99, chunkIndex: 0, last: true, transformVersion: TRANSFORM_VERSION, rows: [item], log: quiet });
  w.prepare(`update raw_ne_order_base set 送り状番号 = 'MINE' where 伝票番号 = 'F3'`).run();
  const r4 = await push(w, l, fakeFetch(), {});
  assert.deepEqual([r4.ok, r4.changed, r4.stale, r4.staleSlips, r4.applied], [true, 1, 1, ['F3'], 0]);
  assert.equal(l.loadFingerprints().get('F3'), fingerprint(buildShipment(base({ slip: 'F3', orderDate: '2025-04-10 10:00:00' }))));   // 前回の指紋のまま
  // 受注日の範囲 (バックフィル): 台帳に書くので、その後の incremental は残りだけ送る
  const w2 = openWarehouse(), l2 = openLedger(null, { memory: true });
  insertBase(w2, base({ slip: 'G1', orderDate: '2025-03-05 10:00:00' })); insertBase(w2, base({ slip: 'G2', orderDate: '2025-04-05 10:00:00' }));
  const r5 = await push(w2, l2, fakeFetch(), { from: '2025-03-01', to: '2025-03-31' });
  assert.deepEqual([r5.ok, r5.mode, r5.inScope, r5.applied, l2.countSent()], [true, 'range', 1, 1, 1]);
  const r6 = await push(w2, l2, fakeFetch(), {});
  assert.deepEqual([r6.inScope, r6.unchanged, r6.changed, r6.applied], [2, 1, 1, 1]);
  w.close(); l.close(); w2.close(); l2.close();
});
await t('lock: 別の送り手が走っていれば見送る (exit 1 相当) / 途中で落ちた run の lock は 6 時間で奪える / 世代は失敗しても進む', async () => {
  const w = openWarehouse(), l = openLedger(null, { memory: true });
  insertBase(w, base({ slip: 'L1' }));
  const t0 = new Date('2026-09-14T00:00:00Z');
  l.acquireLock({ owner: 'other', now: t0 });
  const r1 = await push(w, l, fakeFetch(), { now: () => new Date(t0.getTime() + 60000) });
  assert.deepEqual([r1.ok, r1.lockedBy.owner, r1.sent, l.currentBatchSeq()], [false, 'other', 0, 0]);
  assert.match(summarizeResult(r1), /⏸️/);
  const r2 = await push(w, l, fakeFetch(), { now: () => new Date(t0.getTime() + 7 * 3600000), owner: 'me' });
  assert.deepEqual([r2.ok, r2.applied, l.currentBatchSeq(), l.getMeta(LOCK_KEY)], [true, 1, 1, null]);
  // 4xx で run が例外になっても lock は外れ、世代は進んでいる
  w.prepare(`update raw_ne_order_base set 送り状番号 = 'X' where 伝票番号 = 'L1'`).run();
  const denied = fakeFetch({ before: async () => new Response(JSON.stringify({ error: 'invalid_sync_key' }), { status: 401 }) });
  await rejects(() => push(w, l, denied, { now: () => new Date(t0.getTime() + 8 * 3600000) }), /HTTP 401 invalid_sync_key/);
  assert.deepEqual([denied.calls.length, l.currentBatchSeq(), l.getMeta(LOCK_KEY), l.lastRuns(1)[0].ok, l.lastRuns(1)[0].note], [1, 2, null, 0, 'HTTP 401 invalid_sync_key']);
  w.close(); l.close();
});
await t('応答を失った再送: chunk が commit された後に応答が届かず、送り手が再送 → 受け口は保存した応答 (replay) → 台帳も run の集計も 1 回分', async () => {
  const w = openWarehouse(), l = openLedger(null, { memory: true });
  insertBase(w, base({ slip: 'N1' })); insertBase(w, base({ slip: 'N2' }));
  let dropped = 0;
  const f = fakeFetch({ after: async (n, r) => { if (!dropped && r.chunk_index === 0) { dropped++; return new Response('gateway timeout', { status: 504 }); } return null; } });
  const r1 = await push(w, l, f, {});
  assert.deepEqual([r1.ok, r1.applied, r1.same, r1.sent, r1.chunks, l.countSent()], [true, 2, 0, 2, 1, 2]);
  const chunks = await pg.query(`select chunk_index, rows_seen, rows_applied from ops.ingest_chunks where ingest_run_id = $1 order by 1`, [r1.runId]);
  assert.deepEqual(chunks.rows, [{ chunk_index: 0, rows_seen: 2, rows_applied: 2 }]);
  assert.deepEqual(await one(`select rows_seen, rows_inserted, status from ops.ingest_runs where ingest_run_id = $1`, [r1.runId]), { rows_seen: 2, rows_inserted: 2, status: 'success' });
  assert.equal(f.posts().length, 2);                                                                                        // 2 回送って 1 回分
  w.close(); l.close();
});
await t('HTTP: 5xx / 通信エラーは 3 回まで再送 / 期限超過 (503 CHUNK_DEADLINE) は半分に割って送り直す (chunk_index は連番のまま・最後は last)', async () => {
  const w = openWarehouse(), l = openLedger(null, { memory: true });
  for (let i = 1; i <= 8; i++) insertBase(w, base({ slip: `H${i}` }));
  let n = 0;
  const flaky = fakeFetch({ before: async () => { n++; if (n === 1) return new Response('boom', { status: 503 }); if (n === 2) throw new Error('ECONNRESET'); return null; } });
  const r1 = await push(w, l, flaky, {});
  assert.deepEqual([r1.ok, r1.applied, n], [true, 8, 3]);
  for (let i = 1; i <= 8; i++) w.prepare(`update raw_ne_order_base set 送り状番号 = 'Z' where 伝票番号 = ?`).run(`H${i}`);
  let big = 0;
  const slow = fakeFetch({ before: async (k, url, init) => { const b = JSON.parse(init.body); if (b.rows.length > 2) { big++; return new Response(JSON.stringify({ error: 'exceeded', code: 'CHUNK_DEADLINE' }), { status: 503 }); } return null; } });
  const r2 = await push(w, l, slow, { chunkSize: 8, minSplit: 2 });
  assert.deepEqual([r2.ok, r2.applied, r2.chunks, big], [true, 8, 4, 3]);                                                  // 8 → 4+4 → 2+2+2+2 (期限超過 3 回)
  assert.deepEqual(slow.posts().filter((p) => p.rows.length <= 2).map((p) => [p.chunk_index, p.last, p.rows.length]), [[0, false, 2], [1, false, 2], [2, false, 2], [3, true, 2]]);
  assert.equal((await one(`select status, pages from ops.ingest_runs where ingest_run_id = $1`, [r2.runId])).pages, 4);
  await rejects(() => pushShipments({ warehouse: w, ledger: l, fetchImpl: fakeFetch(), base: '', syncKey: 'k', log: quiet }), /送り先が決まらない/);
  await rejects(() => pushShipments({ warehouse: w, ledger: l, fetchImpl: fakeFetch(), base: BASE_URL, syncKey: '', log: quiet }), /MIRROR_SYNC_KEY/);
  assert.equal(syncBase({ RENDER_MIRROR_URL: 'https://portal.example/apps/mirror' }), BASE_URL);
  assert.equal(syncBase({ RENDER_MIRROR_URL: 'http://portal.example/apps/mirror' }), '');
  assert.equal(syncBase({ RENDER_MIRROR_URL: 'https://portal.example/apps/mirror', RENDER_PORTAL_URL: 'https://other.example' }), '');
  assert.match(newRunId(new Date('2026-09-14T01:02:03.456Z')), /^ship_202609140102034_[0-9a-f]{6}$/);
  w.close(); l.close();
});

console.log('D5a: 突合');
await t('diffDaily: 一致 / 件数の不一致 / 名前の不一致 / 片側だけ。splitWindows: 366 日ごと', async () => {
  const L = [{ ship_date: '2025-03-01', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 3, cancelled_slips: 0 }, { ship_date: '2025-03-01', shop_code: '4', delivery_id: '', delivery_name: '(未設定)', slips: 1, cancelled_slips: 1 }, { ship_date: '2025-03-02', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 2, cancelled_slips: 0 }];
  const R = [{ ...L[0], slips: '3', cancelled_slips: '0' }, { ...L[1], delivery_name: 'x' }, { ship_date: '2025-03-03', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 1, cancelled_slips: 0 }];
  const d = diffDaily(L, R);
  assert.deepEqual([d.compared, d.matched, d.mismatched.length, d.onlyLocal.length, d.onlyRemote.length], [4, 1, 1, 1, 1]);
  assert.deepEqual(d.mismatched[0], { key: '2025-03-01|4|', diffs: ['delivery_name (未設定)≠x'] });
  assert.deepEqual(splitWindows('2025-01-01', '2026-09-14'), [['2025-01-01', '2026-01-01'], ['2026-01-02', '2026-09-14']]);
  assert.deepEqual(splitWindows('2025-01-01', '2025-01-01'), [['2025-01-01', '2025-01-01']]);
});
await t('通し: 旧 rebuild-shipments-daily.js (本物) の f_shipments_daily と mart.v_shipments_daily が一致する。窓をまたぐ期間も。miniPC 側で 1 伝票を取消にすると差が出る', async () => {
  const w = openWarehouse(), l = openLedger(null, { memory: true });
  const slips = [base({ slip: 'RC1', shipped: '2025-04-01 09:00:00' }), base({ slip: 'RC2', shipped: '2025-04-01 10:00:00', cancelled: true, cancelledAt: '2025-04-02 00:00:00' }), base({ slip: 'RC3', shipped: '2025-04-01 11:00:00', deliv: '30', delivName: 'ゆうパケット' }),
    base({ slip: 'RC4', shipped: '2025-04-02 23:59:59', shop: '4', deliv: '', delivName: '' }), base({ slip: 'RC5', shipped: '', status: '2' }), base({ slip: 'RC6', shipped: '2025-04-03 08:00:00', delivName: 'ネコポス(新)' }), base({ slip: 'RC7', orderDate: '2026-05-01 10:00:00', shipped: '2026-05-01 12:00:00' })];
  for (const b of slips) insertBase(w, b);
  rebuildShipmentsDaily(w, { all: true });
  const f = fakeFetch();
  const p = await push(w, l, f, {});
  assert.equal(p.applied, 7, JSON.stringify({ failed: p.failed, te: p.transformErrors, inScope: p.inScope, changed: p.changed }));
  const rc = await reconcileShipmentsDaily({ warehouse: w, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-04-01', to: '2026-05-31', log: quiet });
  assert.deepEqual([rc.ok, rc.windows.length, rc.matched, rc.mismatched.length, rc.onlyLocal.length, rc.onlyRemote.length, rc.localSlips, rc.remoteSlips], [true, 2, 5, 0, 0, 0, 6, 6]);   // 5 行 (4/1 店1 28: 2 件うち取消 1 / 4/1 店1 30 / 4/2 店4 '' / 4/3 店1 28 / 2026-5/1)。R5 は未出荷なので両方とも数えない
  const remote = (await pg.query(`select ship_date::text as d, shop_code, delivery_id, delivery_name, slips, cancelled_slips from mart.v_shipments_daily where company_id = 1 and ship_date between '2025-04-01' and '2025-04-30' order by 1,2,3`)).rows;
  assert.deepEqual(remote.find((r) => r.d === '2025-04-01' && r.delivery_id === '28'), { d: '2025-04-01', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 2, cancelled_slips: 1 });   // 名前は出荷確定日が一番新しい伝票 (RC7 = 2026-05-01) の名称 (RC6 の「ネコポス(新)」ではない)
  w.prepare(`update raw_ne_order_base set キャンセル区分 = 'キャンセル' where 伝票番号 = 'RC1'`).run();
  rebuildShipmentsDaily(w, { all: true });
  const rc2 = await reconcileShipmentsDaily({ warehouse: w, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-04-01', to: '2025-04-30', log: quiet });
  assert.deepEqual([rc2.ok, rc2.mismatched.length, rc2.mismatched[0].diffs], [false, 1, ['cancelled_slips 2≠1']]);
  w.close(); l.close();
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
