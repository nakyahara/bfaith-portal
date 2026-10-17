#!/usr/bin/env node
/**
 * test-ne-values-locks-pg.mjs — 0083 (今朝の NE の値・ロジザードの書き方。AI_reference CompanyDB構想/25 v3 §2.5 の PR-1) の鍵の順を本物の PostgreSQL の別々の接続で確かめる
 *
 * 固定する契約 (鍵の順 = CSV の鍵 (排他) → NE のコードの共有の鍵 → NE の値の排他の鍵 → 行):
 *   K1 CSV を作る取引 (マスタの判断の NE に取り込む CSV = CSV の鍵 → NE のコードの共有の鍵) の間は、照合の record_ne_values が待つ → 終われば書ける
 *   K2 照合の record_ne_codes (NE のコードの排他の鍵) の取引の間: record_ne_values は CSV の鍵を持ったまま NE のコードの共有の鍵で待ち、
 *      次の CSV を作る取引は CSV の鍵で待つ → 順に終わる (デッドロックしない)
 *   K3 NE のコードの共有の鍵だけを持つ取引 (0072・0079 などの関数) + 待っている record_ne_codes (排他) + record_ne_values = 順に終わる (デッドロックしない)
 *   K4 同じ回の record_ne_values を 2 つの接続で同時に = 1 つが書き、もう 1 つは待った後に unchanged / record_lz_codes も同じ
 * 使い方: node scripts/test-ne-values-locks-pg.mjs
 *   🚨 試験が自分で使い捨てのクラスタを起動する (test-company-db-profit-fn-revoke-pg.mjs と同じ): embedded-postgres (devDependencies の正確な版) で
 *      OS の一時フォルダに新しいクラスタを作り、ランダムのポートで起動し、最後に止めて消す。外の PostgreSQL には一切つながない。
 *      見つからない・版が違う・起動できない・クラスタのフォルダが消えない = 失敗 (exit 1)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PINNED_EMBEDDED_PG = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).devDependencies['embedded-postgres'];
/** embedded-postgres を探す (リポジトリの node_modules → 版が同じときだけ EMBEDDED_PG_DIR / C:/tmp/pg-embed)。test-company-db-profit-fn-revoke-pg.mjs と同じ */
async function loadEmbeddedPostgres() {
  const bases = [path.join(ROOT, 'package.json'), ...(process.env.EMBEDDED_PG_DIR ? [path.join(process.env.EMBEDDED_PG_DIR, 'package.json')] : []), 'C:/tmp/pg-embed/package.json'];
  const seen = [];
  for (const b of bases) {
    let main, ver;
    try { const req = createRequire(b); main = req.resolve('embedded-postgres'); let d = path.dirname(main); while (path.basename(d) !== 'embedded-postgres' && path.dirname(d) !== d) d = path.dirname(d); ver = JSON.parse(fs.readFileSync(path.join(d, 'package.json'), 'utf8')).version; } catch { continue; }
    if (ver !== PINNED_EMBEDDED_PG) { seen.push(path.dirname(b) + ' = ' + ver); continue; }
    return { EmbeddedPostgres: (await import(pathToFileURL(main).href)).default, from: path.dirname(b) };
  }
  return { why: seen.length ? '版が ' + PINNED_EMBEDDED_PG + ' でない (' + seen.join(' / ') + ')' : '見つからない' };
}
const loaded = await loadEmbeddedPostgres();
if (!loaded.EmbeddedPostgres) {
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の鍵の試験を流せない (飛ばさない)。リポジトリで npm ci (devDependencies に入っている)');
  process.exit(1);
}
const clusterDir = path.join(os.tmpdir(), `cdb-nev-pg-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
const cluster = new loaded.EmbeddedPostgres({ databaseDir: clusterDir, user: 'postgres', password: SU_PW, port, persistent: false, onLog: () => {}, onError: () => {} });
let cleanupFailed = false;
const stopCluster = async () => {
  try { await cluster.stop(); } catch (e) { console.error('使い捨てのクラスタを止めるときの誤り: ' + e.message); }
  for (let i = 0; i < 10 && fs.existsSync(clusterDir); i++) { try { fs.rmSync(clusterDir, { recursive: true, force: true }); } catch { await new Promise((r) => setTimeout(r, 500)); } }
  if (fs.existsSync(clusterDir)) { cleanupFailed = true; console.error('❌ 使い捨てのクラスタのフォルダが消えない: ' + clusterDir); }
};
const url = `postgres://postgres:${SU_PW}@127.0.0.1:${port}/postgres`;
console.log('test-ne-values-locks-pg (使い捨てのクラスタ: embedded-postgres ' + PINNED_EMBEDDED_PG + ')');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const hex = crypto.randomBytes(4).toString('hex');
const OWNER = `cdb_nevo_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`, DB1 = `cdb_nev_${hex}`;
const clients = [];
let setupError = null;

// ── 入力 ──
const H = 3600000;
const run = (i) => `mc_20301010T0000000${String(i).padStart(2, '0')}Z_abcdef`;
const iso = (ms) => new Date(ms).toISOString();
const BASE = Date.now() - 30 * 24 * H;
const side = (startMs, n, extra) => ({ started_at: iso(startMs), finished_at: iso(startMs + 60000), complete_rev: 1, fetched_rows: n, write_attempts: n, stored_rows: n,
  dropped_no_code: 0, dropped_missing_fields: 0, raw_rows: n, ...extra });
const nev = (i) => ({ compare_run_id: run(i), norm_version: 1,
  fetch: { fingerprint: 'f'.repeat(64), products: side(BASE + i * H - 600000, 1, { complete_rev: i, codes: 1, set_overlap: 0 }), sets: side(BASE + i * H - 590000, 0, { complete_rev: i, codes: 0 }) },
  entries: ['name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier'].map((col) => ({ code_norm: 'a-1', col, ne_kind: 'single', state: 'ok',
    value: { name: `A${i}`, handling: 'active', tax_rate: 0.1, standard_price_jpy: 1000, cost: 100 + i, primary_supplier: ['0001'] }[col] })) });
const codes = (i) => ({ compare_run_id: run(i), entries: [{ code_norm: 'a-1', kind: 'product', state: 'ok', ne_code: 'A-1', spellings: ['A-1'] }] });
const CSV_LOCKS = `select pg_advisory_xact_lock(hashtext('ops.ne_csv')), pg_advisory_xact_lock_shared(hashtext('ops.ne_codes'))`;   // apps/master-decisions/ne-csv.mjs の CSV を作る取引と同じ順

try {
  await cluster.initialise();
  await cluster.start();
  const su = await openPgClient(url);
  try {
    await su.query(`create role ${OWNER} login createrole password '${PW}'`);   // Render の default user と同じ: superuser でない・CREATEROLE
    await su.query(`create database ${DB1} owner ${OWNER}`);
    const open = async () => { const x = new URL(url); x.username = OWNER; x.password = PW; x.pathname = `/${DB1}`; const c = await openPgClient(x.toString()); c.on('error', () => {}); clients.push(c); return c; };
    const O = await open();
    await applyMigrations(pgAdapter(O), { log: () => {} });
    for (let i = 1; i <= 12; i++) await O.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)`, [run(i), iso(BASE + i * H)]);
    const [A, B, C] = [await open(), await open(), await open()];
    const pidOf = async (c) => (await c.query('select pg_backend_pid() as p')).rows[0].p;
    const [pa, pb, pc] = [await pidOf(A), await pidOf(B), await pidOf(C)];
    /** その接続が鍵を待つまで (5 秒まで) */
    const waitBlocked = async (pid) => {
      for (let i = 0; i < 100; i++) {
        const r = (await O.query(`select wait_event_type from pg_stat_activity where pid = $1`, [pid])).rows[0];
        if (r && r.wait_event_type === 'Lock') return true;
        await new Promise((res) => setTimeout(res, 50));
      }
      throw new Error(`pid ${pid} が鍵を待たない`);
    };
    const settled = (p) => p.then((v) => ({ ok: true, v }), (e) => ({ ok: false, e }));
    const recNe = (c, i) => c.query('select ops.record_ne_values($1::jsonb) as r', [JSON.stringify(nev(i))]).then((x) => x.rows[0].r);

    await t('[K1] CSV を作る取引 (CSV の鍵 → NE のコードの共有の鍵) の間は record_ne_values が待つ → 終われば書ける', async () => {
      await A.query('begin'); await A.query(CSV_LOCKS);
      const b = settled(recNe(B, 1));
      await waitBlocked(pb);
      await A.query('commit');
      const r = await b;
      assert.ok(r.ok, r.e && r.e.message);
      assert.equal(r.v.state, 'written');
    });

    await t('[K2] record_ne_codes の取引の間: record_ne_values は NE のコードの共有の鍵で待ち、次の CSV を作る取引は CSV の鍵で待つ → 順に終わる (デッドロックしない)', async () => {
      await A.query('begin'); await A.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify(codes(2))]);
      await B.query('begin');
      const b = settled(recNe(B, 2));
      await waitBlocked(pb);
      // B は CSV の鍵を持ったまま NE のコードの鍵で待つ = C (CSV の鍵) は B を待つ
      const held = (await O.query(`select count(*)::int as n from pg_locks where pid = $1 and locktype = 'advisory' and granted and objid = (hashtext('ops.ne_csv')::bigint & 4294967295)::oid`, [pb])).rows[0].n;
      assert.equal(held, 1, 'B は CSV の鍵を持っている');
      await C.query('begin');
      const c = settled(C.query(CSV_LOCKS));
      await waitBlocked(pc);
      await A.query('commit');
      const rb = await b;
      assert.ok(rb.ok, rb.e && rb.e.message);
      assert.equal(rb.v.state, 'written');
      assert.equal(rb.v.code_mark_same_run, true, '書き方を先に書いた回 = 同じ回');
      await B.query('commit');
      const rc = await c;
      assert.ok(rc.ok, rc.e && rc.e.message);
      await C.query('commit');
    });

    await t('[K3] NE のコードの共有の鍵だけを持つ取引 + 待っている record_ne_codes (排他) + record_ne_values = 順に終わる (デッドロックしない)', async () => {
      await A.query('begin'); await A.query(`select pg_advisory_xact_lock_shared(hashtext('ops.ne_codes'))`);
      const b = settled(B.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify(codes(3))]));
      await waitBlocked(pb);
      const c = settled(recNe(C, 3));
      await waitBlocked(pc);   // 排他の待ちの後ろに並ぶ (公平)
      await A.query('commit');
      const [rb, rc] = [await b, await c];
      assert.ok(rb.ok, rb.e && rb.e.message);
      assert.ok(rc.ok, rc.e && rc.e.message);
      assert.equal(rc.v.state, 'written');
      assert.equal(rc.v.code_mark_same_run, true, 'record_ne_codes の後に書いた = 同じ回');
    });

    await t('[K4] 同じ回の record_ne_values を 2 つの接続で同時に = 1 つが書き、もう 1 つは待った後に unchanged / record_lz_codes も同じ', async () => {
      await A.query('begin');
      const a1 = await recNe(A, 4);
      const b = settled(recNe(B, 4));
      await waitBlocked(pb);
      await A.query('commit');
      const rb = await b;
      assert.deepEqual([a1.state, rb.ok && rb.v.state], ['written', 'unchanged']);
      const ex = Date.now() - H;
      const lz = { lz_daily_run_id: 'lzd_20301010T000000001Z_abcdef', export: { exported_at: iso(ex), stamp_at: iso(ex + 45000), stamp_date: new Date(ex + 9 * H).toISOString().slice(0, 10), csv_sha256: 'a'.repeat(64), rows: 2 }, ids: ['A-1', 'B-2'] };
      await A.query('begin');
      const l1 = (await A.query('select ops.record_lz_codes($1::jsonb) as r', [JSON.stringify(lz)])).rows[0].r;
      const l2 = settled(B.query('select ops.record_lz_codes($1::jsonb) as r', [JSON.stringify({ ...lz, lz_daily_run_id: 'lzd_20301010T000000002Z_abcdef' })]).then((x) => x.rows[0].r));
      await waitBlocked(pb);
      await A.query('commit');
      const r2 = await l2;
      assert.deepEqual([l1.state, r2.ok && r2.v.state], ['written', 'unchanged']);
      // 古い回は待った後に断られる (印が先に進んだ)
      await A.query('begin');
      await recNe(A, 6);
      const old = settled(recNe(B, 5));
      await waitBlocked(pb);
      await A.query('commit');
      const ro = await old;
      assert.equal(ro.ok, false);
      assert.match(ro.e.message, /stale_run/);
    });
  } finally { await su.end().catch(() => {}); }
} catch (e) {
  setupError = e;
  console.error('❌ 準備か試験の外で落ちた (飛ばさない): ' + (e.stack || e.message));
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  await stopCluster();
}
console.log(`\n${ok} ok / ${ng} NG`);
// embedded-postgres の async-exit-hook が exitCode を上書きする = 明示で process.exit (test-company-db-profit-fn-revoke-pg.mjs と同じ)
process.exit(ng || setupError || cleanupFailed ? 1 : 0);
