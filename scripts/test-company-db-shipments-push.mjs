#!/usr/bin/env node
/**
 * test-company-db-shipments-push.mjs — D5a (NE 伝票 → Company DB の push) の受入試験 (08 §4.7 / §9 D5)
 *
 *   整形 (純粋関数): JST → +09:00 / '' → null / 明細の並び / 指紋は鍵順と時刻に依らない / 欠落は例外
 *   選択とカーソル (SQLite :memory:): 初回 = 2025-01-01 以降 (受注日か出荷確定日) / カーソル以降にヘッダか明細が変わった伝票 / 受注日の範囲 / ヘッダ無しの明細
 *   受け口 (PGlite): validateChunk / 1 chunk = 1 取引 (applied・same・stale) / 失敗した伝票だけ切り分け (savepoint) / ops.ingest_runs
 *   通し (送り手 ⇄ 受け口。fetch を差し替え): chunk 分割 / 世代とカーソル / 再送は same / 変更は次の世代 / 失敗でカーソルが進まない / 再送 (5xx) と即失敗 (4xx) / dry-run
 *   突合: diffDaily と reconcile の通し (旧 f_shipments_daily の式 と mart.v_shipments_daily)
 * 実行: node scripts/test-company-db-shipments-push.mjs
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildShipment, contentHash, jstToIso, jstDateOnly, utcToIso, canonicalJson, TRANSFORM_VERSION } from '../apps/company-db/push/ne-shipments-transform.mjs';
import { selectSlips, pushShipments, reconcileShipmentsDaily, diffDaily, readMeta, summarizeResult, syncBase, CURSOR_KEY, SEQ_KEY } from '../apps/company-db/push/ne-shipments.mjs';
import { ingestShipmentChunk, validateChunk, MAX_ROWS_PER_CHUNK } from '../apps/company-db/ingest/shipments.mjs';

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
/** 旧 rebuild-shipments-daily.js と同じ式で f_shipments_daily を作る */
function rebuildDaily(db) {
  db.exec('delete from f_shipments_daily');
  const rows = db.prepare(`select substr(出荷確定日,1,10) as ship_date, coalesce(店舗コード,'') as shop_code, coalesce(配送方法ID,'') as delivery_id, count(*) as slips, sum(case when キャンセル区分 = 'キャンセル' then 1 else 0 end) as cancelled_slips
    from raw_ne_order_base where 出荷確定日 >= '0000-01-01 00:00:00' and 出荷確定日 < '9999-12-31 99' group by 1,2,3`).all();
  const names = new Map(db.prepare(`select delivery_id, delivery_name from (select coalesce(配送方法ID,'') as delivery_id, coalesce(nullif(配送方法名,''),'(未設定)') as delivery_name, row_number() over (partition by coalesce(配送方法ID,'') order by 出荷確定日 desc, 伝票番号 desc) as rn from raw_ne_order_base where 出荷確定日 <> '') where rn = 1`).all().map((r) => [r.delivery_id, r.delivery_name]));
  const ins = db.prepare('insert into f_shipments_daily values (?,?,?,?,?,?,?)');
  for (const r of rows) ins.run(r.ship_date, r.shop_code, r.delivery_id, names.get(r.delivery_id) || '(未設定)', r.slips, r.cancelled_slips, 'x');
}

// ─── PGlite (受け口) ───
const pg = new PGlite();
const pdb = pgliteAdapter(pg);
const applied0 = await applyMigrations(pdb, { log: quiet });
assert.ok(applied0.applied.includes('0013'), '0013 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
await pg.query(`insert into core.products (company_id, name) values (1, '見本')`);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select 1, product_id, 'single', 'sku-1', name from core.products`);
const RUN = (n) => `ship_202609140000000_${String(n).padStart(6, '0')}`;   // 15 桁 (ISO の ms の先頭 1 桁まで)
const BASE_URL = 'https://portal.example/apps/company-db/sync';
/** 送り手が叩く fetch の代わり: 受け口の関数を直接呼ぶ (HTTP 層だけを飛ばす) */
function fakeFetch(opts = {}) {
  const calls = [];
  const f = async (url, init = {}) => {
    calls.push({ url, init });
    if (opts.before) { const r = await opts.before(calls.length, url, init); if (r) return r; }
    if (init.method === 'POST' && url === `${BASE_URL}/shipments`) {
      assert.equal(init.headers['x-sync-key'], 'k');
      let chunk; try { chunk = validateChunk(JSON.parse(init.body)); } catch (e) { return new Response(JSON.stringify({ error: e.message }), { status: 400 }); }
      try { const r = await ingestShipmentChunk(pdb, { ...chunk, log: quiet }); return new Response(JSON.stringify(r), { status: 200 }); }
      catch (e) { return new Response(JSON.stringify({ error: e.message }), { status: 500 }); }
    }
    if (url.startsWith(`${BASE_URL}/shipments/daily?`)) {
      const u = new URL(url);
      const rows = (await pg.query(`select ship_date::text as ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips from mart.v_shipments_daily where company_id = 1 and ship_date between $1::date and $2::date`, [u.searchParams.get('from'), u.searchParams.get('to')])).rows;
      return new Response(JSON.stringify({ rows }), { status: 200 });
    }
    return new Response('not found', { status: 404 });
  };
  f.calls = calls;
  return f;
}

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
  // 指紋: 鍵の順を変えても、source_updated_at を変えても同じ。内容 (送り状番号) を変えれば違う
  const reordered = Object.fromEntries(Object.entries(h).reverse());
  assert.equal(contentHash(reordered), h.content_hash);
  assert.equal(contentHash({ ...h, source_updated_at: '2030-01-01T00:00:00Z', transform_version: 'x' }), h.content_hash);
  assert.notEqual(buildShipment({ ...b, 送り状番号: 'T-2' }, []).header.content_hash, h.content_hash);
  assert.equal(canonicalJson({ b: [1, { z: 1, a: null }], a: 'x' }), '{"a":"x","b":[1,{"a":null,"z":1}]}');
  // 空は null。取消でない伝票の cancelled_at は null。送り状が無ければ tracking_source も null
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

console.log('D5a: 選択とカーソル (SQLite)');
await t('初回 (カーソル無し) = 2025-01-01 以降 (受注日か出荷確定日) / カーソル以降にヘッダか明細が変わった伝票 / 受注日の範囲 / ヘッダ無しの明細は件数だけ', async () => {
  const db = openWarehouse();
  insertBase(db, base({ slip: 'OLD', orderDate: '2024-12-20 10:00:00', shipped: '2024-12-22 10:00:00', synced: '2026-09-13 00:00:00' }));   // 境界前 (受注も出荷も 2024)
  insertBase(db, base({ slip: 'EDGE', orderDate: '2024-12-30 10:00:00', shipped: '2025-01-02 10:00:00', synced: '2026-09-09 00:00:00' }));  // 2024 の注文だが 2025 に出荷 → 入る
  insertBase(db, base({ slip: 'A', orderDate: '2025-03-01 10:00:00', synced: '2026-09-10 00:00:00' }));
  insertBase(db, base({ slip: 'B', orderDate: '2025-05-01 10:00:00', synced: '2026-09-11 00:00:00' }));
  insertLine(db, line('B', 1, { synced: '2026-09-13 00:00:00' }));                                                  // 明細だけ後で変わった
  insertLine(db, line('NOBASE', 1, { synced: '2026-09-13 00:00:00' }));                                             // ヘッダがまだ無い
  const all = selectSlips(db, {});
  assert.deepEqual(all.slips.map((s) => s.base.伝票番号), ['A', 'B', 'EDGE']);
  assert.equal(all.maxSyncedAt, '2026-09-13 00:00:00'); assert.equal(all.linesWithoutBase, 1);
  assert.equal(all.slips.find((s) => s.base.伝票番号 === 'B').lines.length, 1);
  const inc = selectSlips(db, { cursor: '2026-09-12 00:00:00' });
  assert.deepEqual(inc.slips.map((s) => s.base.伝票番号), ['B']);                      // 明細の synced_at で拾う。OLD は境界前なので拾わない
  assert.equal(inc.linesWithoutBase, 1);
  const eq = selectSlips(db, { cursor: '2026-09-11 00:00:00' });
  assert.deepEqual(eq.slips.map((s) => s.base.伝票番号), ['B']);                       // >= (境界の伝票は次回も送る)
  const range = selectSlips(db, { from: '2025-01-01', to: '2025-03-31' });
  assert.deepEqual(range.slips.map((s) => s.base.伝票番号), ['A']);                    // 受注日の範囲 (EDGE は 2024-12 の受注)
  db.close();
});

console.log('D5a: 受け口 (PGlite)');
await t('validateChunk: run_id の形 / 世代 / chunk の整合 / 伝票の重複 / transform_version の不一致 / 上限', async () => {
  const good = { run_id: RUN(1), batch_seq: 1, chunk_index: 0, chunk_count: 1, transform_version: TRANSFORM_VERSION, rows: [{ ne_slip_no: 'S1', header: { transform_version: TRANSFORM_VERSION }, lines: [] }] };
  const v = validateChunk(good); assert.equal(v.runId, RUN(1)); assert.equal(v.rows.length, 1);
  for (const [patch, re] of [[{ run_id: 'load_x' }, /run_id/], [{ batch_seq: 0 }, /batch_seq/], [{ chunk_index: 1 }, /chunk_index/], [{ transform_version: '' }, /transform_version is required/],
    [{ rows: [good.rows[0], good.rows[0]] }, /duplicate/], [{ rows: [{ ...good.rows[0], header: { transform_version: 'other' } }] }, /differs/], [{ rows: [{ ne_slip_no: 'S1', header: {}, lines: {} }] }, /lines must be an array/],
    [{ rows: Array.from({ length: MAX_ROWS_PER_CHUNK + 1 }, (_, i) => ({ ne_slip_no: `S${i}`, header: { transform_version: TRANSFORM_VERSION }, lines: [] })) }, /<= 1000/]]) {
    await rejects(async () => validateChunk({ ...good, ...patch }), re);
  }
});
await t('1 chunk = 1 取引: applied → same (世代だけ進む) → stale。列の対応 (ship_date_jst は JST の日付・状態は対応表・SKU の解決)。ops.ingest_runs に run', async () => {
  const rows = [
    (() => { const r = buildShipment(base({ slip: 'P1', tracking: 'T1' }), [line('P1', 1, { code: 'sku-1', qty: 2 }), line('P1', 2, { code: 'nope', qty: 1 })]); return { ne_slip_no: r.ne_slip_no, header: r.header, lines: r.lines }; })(),
    (() => { const r = buildShipment(base({ slip: 'P2', shipped: '', status: '20' }), []); return { ne_slip_no: r.ne_slip_no, header: r.header, lines: r.lines }; })(),
    (() => { const r = buildShipment(base({ slip: 'P3', cancelled: true, cancelledAt: '2025-03-02 00:00:00' }), [line('P3', 1)]); return { ne_slip_no: r.ne_slip_no, header: r.header, lines: r.lines }; })(),
  ];
  const chunk = { runId: RUN(2), batchSeq: 5, chunkIndex: 0, chunkCount: 1, transformVersion: TRANSFORM_VERSION, rows, log: quiet };
  const r1 = await ingestShipmentChunk(pdb, chunk);
  assert.deepEqual([r1.applied, r1.same, r1.stale, r1.failed.length, r1.finished], [3, 0, 0, 0, true]);
  const s1 = await one(`select ship_date_jst::text as d, status, tracking_no, tracking_source, received_batch_seq::int as seq, shop_code, ne_order_no from core.shipments where ne_slip_no = 'P1'`);
  assert.deepEqual(s1, { d: '2025-03-01', status: 'shipped', tracking_no: 'T1', tracking_source: 'ne', seq: 5, shop_code: '1', ne_order_no: 'P1-ORD' });
  assert.equal((await one(`select status, ship_date_jst::text as d from core.shipments where ne_slip_no = 'P2'`)).status, 'ready');     // NE 20 = 納品書印刷待ち → ready
  assert.equal((await one(`select ship_date_jst from core.shipments where ne_slip_no = 'P2'`)).ship_date_jst, null);
  assert.equal((await one(`select status from core.shipments where ne_slip_no = 'P3'`)).status, 'cancelled');
  const lines = (await pg.query(`select l.line_no, l.qty, l.sku_id is not null as resolved, l.unresolved_code from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'P1' order by l.line_no`)).rows;
  assert.deepEqual(lines, [{ line_no: '1', qty: 2, resolved: true, unresolved_code: null }, { line_no: '2', qty: 1, resolved: false, unresolved_code: 'nope' }]);
  const run = await one(`select status, complete, rows_seen, rows_inserted, rows_skipped, checksum, format_version, failed_ranges from ops.ingest_runs where ingest_run_id = $1`, [RUN(2)]);
  assert.deepEqual([run.status, run.complete, run.rows_seen, run.rows_inserted, run.rows_skipped, run.checksum, run.format_version, run.failed_ranges], ['success', true, 3, 3, 0, '5', TRANSFORM_VERSION, []]);
  // 同じ内容を新しい世代で → same (世代だけ進む)。古い世代 → stale
  const r2 = await ingestShipmentChunk(pdb, { ...chunk, runId: RUN(3), batchSeq: 6 });
  assert.deepEqual([r2.applied, r2.same, r2.stale], [0, 3, 0]);
  assert.equal((await one(`select received_batch_seq::int as s from core.shipments where ne_slip_no = 'P1'`)).s, 6);
  const r3 = await ingestShipmentChunk(pdb, { ...chunk, runId: RUN(4), batchSeq: 4 });
  assert.deepEqual([r3.applied, r3.same, r3.stale], [0, 0, 3]);
  assert.equal((await one(`select status, rows_skipped from ops.ingest_runs where ingest_run_id = $1`, [RUN(4)])).rows_skipped, 3);
});
await t('失敗した伝票だけ切り分ける (savepoint): 受注数の無い明細・content_hash の無いヘッダ → failed、他は commit。run は partial + failed_ranges。複数 chunk は最後で閉じる', async () => {
  const good = buildShipment(base({ slip: 'Q1' }), [line('Q1', 1)]);
  const bad1 = buildShipment(base({ slip: 'Q2' }), [line('Q2', 1)]); bad1.lines[0].qty = null;             // DB が拒む (qty 必須)
  const bad2 = buildShipment(base({ slip: 'Q3' }), []); delete bad2.header.content_hash;
  const rows = [good, bad1, bad2].map((r) => ({ ne_slip_no: r.ne_slip_no, header: r.header, lines: r.lines }));
  const c1 = await ingestShipmentChunk(pdb, { runId: RUN(5), batchSeq: 7, chunkIndex: 0, chunkCount: 2, transformVersion: TRANSFORM_VERSION, rows, log: quiet });
  assert.deepEqual([c1.applied, c1.failed.length, c1.finished], [1, 2, false]);
  assert.deepEqual(c1.failed.map((f) => f.ne_slip_no), ['Q2', 'Q3']);
  assert.match(c1.failed[0].error, /line_no and qty/); assert.match(c1.failed[1].error, /content_hash/);
  assert.equal(await num(`select count(*) as n from core.shipments where ne_slip_no in ('Q1','Q2','Q3')`), 1);
  let run = await one(`select status, finished_at, rows_seen from ops.ingest_runs where ingest_run_id = $1`, [RUN(5)]);
  assert.deepEqual([run.status, run.finished_at, run.rows_seen], ['running', null, 3]);
  const c2 = await ingestShipmentChunk(pdb, { runId: RUN(5), batchSeq: 7, chunkIndex: 1, chunkCount: 2, transformVersion: TRANSFORM_VERSION, rows: [rows[0]], log: quiet });
  assert.deepEqual([c2.same, c2.finished], [1, true]);
  run = await one(`select status, complete, rows_seen, rows_inserted, rows_skipped, error, failed_ranges from ops.ingest_runs where ingest_run_id = $1`, [RUN(5)]);
  assert.deepEqual([run.status, run.complete, run.rows_seen, run.rows_inserted, run.rows_skipped], ['partial', true, 4, 1, 1]);
  assert.match(run.error, /2 slips failed/); assert.deepEqual(run.failed_ranges.map((f) => f.ne_slip_no), ['Q2', 'Q3']);
});

console.log('D5a: 通し (送り手 ⇄ 受け口)');
await t('chunk 分割 / 世代とカーソル / 再送は same / 変更は次の世代で applied / dry-run は送らない', async () => {
  const db = openWarehouse();
  insertBase(db, base({ slip: 'E1', synced: '2026-09-10 00:00:00' })); insertLine(db, line('E1', 1, { code: 'sku-1' }));
  insertBase(db, base({ slip: 'E2', synced: '2026-09-10 00:00:01' })); insertLine(db, line('E2', 1)); insertLine(db, line('E2', 2));
  insertBase(db, base({ slip: 'E3', synced: '2026-09-10 00:00:02', shipped: '' , status: '2' }));
  const f = fakeFetch();
  const dry = await pushShipments({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', chunkSize: 2, dryRun: true, log: quiet });
  assert.deepEqual([dry.ok, dry.selected, dry.sent, f.calls.length, readMeta(db, SEQ_KEY)], [true, 3, 0, 0, null]);
  const r1 = await pushShipments({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', chunkSize: 2, log: quiet });
  assert.deepEqual([r1.ok, r1.selected, r1.sent, r1.applied, r1.same, r1.chunks, r1.batchSeq, r1.cursorBefore, r1.cursorAfter], [true, 3, 3, 3, 0, 2, 1, null, '2026-09-10 00:00:02']);
  assert.equal(readMeta(db, CURSOR_KEY), '2026-09-10 00:00:02'); assert.equal(readMeta(db, SEQ_KEY), '1');
  assert.match(r1.runId, /^ship_[0-9]{15}_[0-9a-f]{6}$/);
  assert.equal(await num(`select count(*) as n from core.shipments where ne_slip_no like 'E%'`), 3);
  assert.match(summarizeResult(r1), /✅ Company DB 出荷 push: 伝票 3\/3 件 \(applied 3 \/ same 0 \/ stale 0 \/ failed 0/);
  // 2 回目: 境界の伝票 (synced_at = カーソル) だけ選ばれ same。世代は 2 に進む
  const r2 = await pushShipments({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', chunkSize: 2, log: quiet });
  assert.deepEqual([r2.ok, r2.selected, r2.same, r2.applied, r2.batchSeq, r2.cursorAfter], [true, 1, 1, 0, 2, '2026-09-10 00:00:02']);
  assert.equal((await one(`select received_batch_seq::int as s from core.shipments where ne_slip_no = 'E3'`)).s, 2);
  // 明細が変わる (数量 2 → 5、synced_at が進む) → 次の世代で applied、カーソルが進む。Render の明細も 5
  db.prepare(`update raw_ne_orders set 受注数 = 5, synced_at = '2026-09-12 00:00:00' where 伝票番号 = 'E1'`).run();
  const r3 = await pushShipments({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', chunkSize: 2, log: quiet });
  assert.deepEqual([r3.ok, r3.selected, r3.applied, r3.same, r3.batchSeq, r3.cursorAfter], [true, 2, 1, 1, 3, '2026-09-12 00:00:00']);
  assert.equal((await one(`select l.qty from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'E1' and l.line_no = '1'`)).qty, 5);
  // 送る物が無いときは世代もカーソルも動かない
  db.prepare(`update sync_meta set value = '2026-09-13 00:00:00' where key = ?`).run(CURSOR_KEY);
  const r4 = await pushShipments({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet });
  assert.deepEqual([r4.ok, r4.selected, r4.sent, r4.batchSeq, readMeta(db, SEQ_KEY)], [true, 0, 0, null, '3']);
  db.close();
});
await t('失敗があればカーソルは進まず ok=false (整形できない / Render が拒んだ)。受注日の範囲はカーソルを見ない・動かさない', async () => {
  const db = openWarehouse();
  insertBase(db, base({ slip: 'F1', synced: '2026-09-10 00:00:00' })); insertLine(db, line('F1', 1));
  insertBase(db, base({ slip: 'F2', synced: '2026-09-10 00:00:01' })); insertLine(db, line('F2', 1, { qty: null }));   // 整形できない
  const f = fakeFetch();
  const r1 = await pushShipments({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet });
  assert.deepEqual([r1.ok, r1.selected, r1.transformErrors.length, r1.sent, r1.applied, r1.cursorAfter, readMeta(db, CURSOR_KEY)], [false, 2, 1, 1, 1, null, null]);
  assert.match(summarizeResult(r1), /^❌/);
  // Render 側で拒まれる (ヘッダを壊す = 出荷確定日の無い伝票に ship_date を持たせる、はできないので content_hash を消す経路で)
  db.prepare(`update raw_ne_orders set 受注数 = 1 where 伝票番号 = 'F2'`).run();
  const f2 = fakeFetch({ before: async (n, url, init) => {
    if (init.method !== 'POST') return null;
    const body = JSON.parse(init.body); delete body.rows[0].header.content_hash;                                    // 1 伝票目を壊して受け口に渡す
    const chunk = validateChunk(body); const r = await ingestShipmentChunk(pdb, { ...chunk, log: quiet });
    return new Response(JSON.stringify(r), { status: 200 });
  } });
  const r2 = await pushShipments({ db, fetchImpl: f2, base: BASE_URL, syncKey: 'k', log: quiet });
  assert.deepEqual([r2.ok, r2.sent, r2.failed.length, r2.failed[0].ne_slip_no, r2.cursorAfter, readMeta(db, CURSOR_KEY)], [false, 2, 1, 'F1', null, null]);
  assert.equal(readMeta(db, SEQ_KEY), '2');                                                                       // 世代は失敗でも進む
  // 受注日の範囲 (バックフィル): カーソルは動かさない
  const r3 = await pushShipments({ db, fetchImpl: fakeFetch(), base: BASE_URL, syncKey: 'k', from: '2025-03-01', to: '2025-03-31', log: quiet });
  assert.deepEqual([r3.ok, r3.mode, r3.selected, r3.cursorAfter, readMeta(db, CURSOR_KEY)], [true, 'range', 2, null, null]);
  assert.match(summarizeResult(r3), /受注日の範囲/);
  db.close();
});
await t('HTTP: 5xx / 通信エラーは 3 回まで再送、4xx は即失敗 (世代は進んでいる)。送り先や鍵が無ければ送る前に止まる', async () => {
  const db = openWarehouse();
  insertBase(db, base({ slip: 'H1', synced: '2026-09-10 00:00:00' }));
  let n = 0;
  const flaky = fakeFetch({ before: async () => { n++; if (n === 1) return new Response('boom', { status: 503 }); if (n === 2) throw new Error('ECONNRESET'); return null; } });
  const r1 = await pushShipments({ db, fetchImpl: flaky, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {} });
  assert.deepEqual([r1.ok, r1.applied, n], [true, 1, 3]);
  const denied = fakeFetch({ before: async () => new Response(JSON.stringify({ error: 'invalid_sync_key' }), { status: 401 }) });
  db.prepare(`update raw_ne_order_base set synced_at = '2026-09-11 00:00:00' where 伝票番号 = 'H1'`).run();
  const e = await rejects(() => pushShipments({ db, fetchImpl: denied, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {} }), /HTTP 401 invalid_sync_key/);
  assert.equal(denied.calls.length, 1); assert.equal(e.fatal, true);
  assert.equal(readMeta(db, SEQ_KEY), '2'); assert.equal(readMeta(db, CURSOR_KEY), '2026-09-10 00:00:00');   // 4xx でも世代は進んでいる (送る前に +1)
  await rejects(() => pushShipments({ db, fetchImpl: fakeFetch(), base: '', syncKey: 'k', log: quiet }), /送り先が決まらない/);
  await rejects(() => pushShipments({ db, fetchImpl: fakeFetch(), base: BASE_URL, syncKey: '', log: quiet }), /MIRROR_SYNC_KEY/);
  assert.equal(syncBase({ RENDER_MIRROR_URL: 'https://portal.example/apps/mirror' }), BASE_URL.replace('portal.example', 'portal.example'));
  assert.equal(syncBase({ RENDER_MIRROR_URL: 'http://portal.example/apps/mirror' }), '');
  assert.equal(syncBase({ RENDER_MIRROR_URL: 'https://portal.example/apps/mirror', RENDER_PORTAL_URL: 'https://other.example' }), '');
  db.close();
});

console.log('D5a: 突合');
await t('diffDaily: 一致 / 件数の不一致 / 名前の不一致 / 片側だけ', async () => {
  const L = [{ ship_date: '2025-03-01', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 3, cancelled_slips: 0 }, { ship_date: '2025-03-01', shop_code: '4', delivery_id: '', delivery_name: '(未設定)', slips: 1, cancelled_slips: 1 }, { ship_date: '2025-03-02', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 2, cancelled_slips: 0 }];
  const R = [{ ...L[0], slips: '3', cancelled_slips: '0' }, { ...L[1], delivery_name: 'x' }, { ship_date: '2025-03-03', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス', slips: 1, cancelled_slips: 0 }];
  const d = diffDaily(L, R);
  assert.deepEqual([d.compared, d.matched, d.mismatched.length, d.onlyLocal.length, d.onlyRemote.length], [4, 1, 1, 1, 1]);
  assert.deepEqual(d.mismatched[0], { key: '2025-03-01|4|', diffs: ['delivery_name (未設定)≠x'] });
});
await t('通し: 旧 f_shipments_daily (旧の式) と mart.v_shipments_daily が一致する。miniPC 側で 1 伝票を取消にすると差が出る', async () => {
  const db = openWarehouse();
  const slips = [base({ slip: 'R1', shipped: '2025-04-01 09:00:00' }), base({ slip: 'R2', shipped: '2025-04-01 10:00:00', cancelled: true, cancelledAt: '2025-04-02 00:00:00' }), base({ slip: 'R3', shipped: '2025-04-01 11:00:00', deliv: '30', delivName: 'ゆうパケット' }),
    base({ slip: 'R4', shipped: '2025-04-02 23:59:59', shop: '4', deliv: '', delivName: '' }), base({ slip: 'R5', shipped: '', status: '2' }), base({ slip: 'R6', shipped: '2025-04-03 08:00:00', delivName: 'ネコポス(新)' })];
  for (const b of slips) insertBase(db, b);
  rebuildDaily(db);
  const f = fakeFetch();
  const p = await pushShipments({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet });
  assert.equal(p.applied, 6);
  const rc = await reconcileShipmentsDaily({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-04-01', to: '2025-04-30', log: quiet });
  assert.deepEqual([rc.ok, rc.matched, rc.mismatched.length, rc.onlyLocal.length, rc.onlyRemote.length, rc.localSlips, rc.remoteSlips], [true, 4, 0, 0, 0, 5, 5]);   // 4 行 (4/1 店1 28: 2 件うち取消 1 / 4/1 店1 30 / 4/2 店4 '' / 4/3 店1 28)。R5 は未出荷なので両方とも数えない
  const remote = (await pg.query(`select ship_date::text as d, shop_code, delivery_id, delivery_name, slips, cancelled_slips from mart.v_shipments_daily where company_id = 1 and ship_date between '2025-04-01' and '2025-04-30' order by 1,2,3`)).rows;
  assert.deepEqual(remote.find((r) => r.d === '2025-04-01' && r.delivery_id === '28'), { d: '2025-04-01', shop_code: '1', delivery_id: '28', delivery_name: 'ネコポス(新)', slips: 2, cancelled_slips: 1 });   // 名前は出荷確定日が一番新しい伝票の名称
  // miniPC 側だけ変えて再構築 → 差が出る (Render に送っていないので)
  db.prepare(`update raw_ne_order_base set キャンセル区分 = 'キャンセル' where 伝票番号 = 'R1'`).run();
  rebuildDaily(db);
  const rc2 = await reconcileShipmentsDaily({ db, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-04-01', to: '2025-04-30', log: quiet });
  assert.deepEqual([rc2.ok, rc2.mismatched.length, rc2.mismatched[0].diffs], [false, 1, ['cancelled_slips 2≠1']]);
  db.close();
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
