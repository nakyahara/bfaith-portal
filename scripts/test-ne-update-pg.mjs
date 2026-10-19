#!/usr/bin/env node
/**
 * test-ne-update-pg.mjs — 0094 (NE へ送る・AI_reference CompanyDB構想/25 の PR-2) の鍵を本物の PostgreSQL の別々の接続で確かめる
 *
 * 固定する契約 (作る = ops.ne_upd (排他) → NE のコードの共有 → NE の値の共有 → ロジザードの書き方の共有):
 *   K1 照合の record_ne_values (CSV の鍵 → NE のコードの共有 → NE の値の排他) の取引の間は、作る (ops.ne_upd_build) が NE の値の鍵で待つ → 終われば新しい回で作る
 *   K2 作る取引の間は record_ne_values が NE の値の鍵で待つ (作る側が読んでいる回を入れ替えない) → 終われば書ける
 *   K3 照合の record_ne_codes (NE のコードの排他) を待っている間に、作る と record_ne_values が並んでも順に終わる (デッドロックしない)
 *   K4 同じ朝の自動を 2 つの接続で同時に = 1 つが作り、もう 1 つは待った後に前の答え (replayed = 2 回送らない)
 *   K5 今すぐの頼み (画面) を 2 つの接続で同時に = 頼みは 1 つ (後の方は前の頼みに乗る)
 * 使い方: node scripts/test-ne-update-pg.mjs
 *   🚨 試験が自分で使い捨てのクラスタを起動する (test-ne-values-locks-pg.mjs と同じ): embedded-postgres (devDependencies の正確な版)。外の PostgreSQL には一切つながない
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
const clusterDir = path.join(os.tmpdir(), `cdb-neupd-pg-${crypto.randomBytes(4).toString('hex')}`);
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
console.log('test-ne-update-pg (使い捨てのクラスタ: embedded-postgres ' + PINNED_EMBEDDED_PG + ')');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const hex = crypto.randomBytes(4).toString('hex');
const OWNER = `cdb_nuo_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`, DB1 = `cdb_nu_${hex}`;
const clients = [];
let setupError = null;

const iso = (ms) => new Date(ms).toISOString();
let tick = 0;
const runId = () => { tick++; return `mc_${iso(Date.now() + tick).replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`; };
const side = (startMs, n, extra) => ({ started_at: iso(startMs), finished_at: iso(startMs + 500), complete_rev: tick, fetched_rows: n, write_attempts: n, stored_rows: n,
  dropped_no_code: 0, dropped_missing_fields: 0, raw_rows: n, ...extra });
const nev = (run, startMs) => ({ compare_run_id: run, norm_version: 1,
  fetch: { fingerprint: 'f'.repeat(64), products: side(startMs, 1, { codes: 1, set_overlap: 0 }), sets: side(startMs + 1, 0, { codes: 0 }) },
  entries: ['name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier'].map((col) => ({ code_norm: 'a-1', col, ne_kind: 'single', state: 'ok',
    value: { name: 'A', handling: 'active', tax_rate: 0.1, standard_price_jpy: 1000, cost: 100, primary_supplier: ['0001'] }[col] })) });
const codes = (run) => ({ compare_run_id: run, entries: [{ code_norm: 'a-1', kind: 'product', state: 'ok', ne_code: 'A-1', spellings: ['A-1'] }] });

try {
  await cluster.initialise();
  await cluster.start();
  const su = await openPgClient(url);
  try {
    await su.query(`create role ${OWNER} login createrole password '${PW}'`);
    await su.query(`create database ${DB1} owner ${OWNER}`);
    const open = async () => { const x = new URL(url); x.username = OWNER; x.password = PW; x.pathname = `/${DB1}`; const c = await openPgClient(x.toString()); c.on('error', () => {}); clients.push(c); return c; };
    const O = await open();
    await applyMigrations(pgAdapter(O), { log: () => {} });
    const [A, B, C] = [await open(), await open(), await open()];
    const pidOf = async (c) => (await c.query('select pg_backend_pid() as p')).rows[0].p;
    const [pa, pb, pc] = [await pidOf(A), await pidOf(B), await pidOf(C)];
    const waitBlocked = async (pid) => {
      for (let i = 0; i < 100; i++) {
        const r = (await O.query(`select wait_event_type from pg_stat_activity where pid = $1`, [pid])).rows[0];
        if (r && r.wait_event_type === 'Lock') return true;
        await new Promise((res) => setTimeout(res, 50));
      }
      throw new Error(`pid ${pid} が鍵を待たない`);
    };
    const settled = (p) => p.then((v) => ({ ok: true, v }), (e) => ({ ok: false, e }));
    /** 照合の回 (照合の回の表 → NE のコードの書き方) を置く。NE の値は呼び手が */
    const newRun = async () => {
      const r = runId();
      await O.query('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)', [r, iso(Date.now() + 1000)]);
      await O.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify(codes(r))]);
      return r;
    };
    const recNe = (c, r) => c.query('select ops.record_ne_values($1::jsonb) as r', [JSON.stringify(nev(r, Date.now()))]).then((x) => x.rows[0].r);
    const build = (c, how) => c.query('select ops.ne_upd_build($1, $2, true) as r', [how, 'minipc-pg']).then((x) => x.rows[0].r);
    const markRun = async () => (await O.query('select compare_run_id from ops.master_ne_values_mark')).rows[0]?.compare_run_id ?? null;

    const r0 = await newRun();
    await recNe(O, r0);

    await t('[K1] record_ne_values の取引の間は 作る が NE の値の鍵で待つ → 終われば新しい回で作る', async () => {
      const r1 = await newRun();
      await A.query('begin');
      await recNe(A, r1);
      const b = settled(build(B, 'auto'));
      await waitBlocked(pb);
      await A.query('commit');
      const rb = await b;
      assert.ok(rb.ok, rb.e && rb.e.message);
      const s = (await O.query('select ne_run from ops.ne_upd_sends where send_id = $1', [rb.v.send_id])).rows[0];
      assert.equal(s.ne_run, r1, '待った後の新しい回で作る');
    });

    await t('[K2] 作る取引の間は record_ne_values が待つ → 終われば書ける', async () => {
      const r2 = await newRun();
      await A.query('begin');
      await build(A, 'now');
      const b = settled(recNe(B, r2));
      await waitBlocked(pb);
      await A.query('commit');
      const rb = await b;
      assert.ok(rb.ok, rb.e && rb.e.message);
      assert.equal(rb.v.state, 'written');
      assert.equal(await markRun(), r2);
    });

    await t('[K3] record_ne_codes (排他) を待っている間に 作る と record_ne_values が並んでも順に終わる (デッドロックしない)', async () => {
      const r3 = runId();
      await O.query('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)', [r3, iso(Date.now() + 1000)]);
      await A.query('begin');
      await build(A, 'now');   // ops.ne_upd → NE のコードの共有 → NE の値の共有 を持つ
      const b = settled(B.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify(codes(r3))]));
      await waitBlocked(pb);
      const c = settled(recNe(C, r3));
      await waitBlocked(pc);
      await A.query('commit');
      const [rb, rc] = [await b, await c];
      assert.ok(rb.ok, rb.e && rb.e.message);
      assert.ok(rc.ok, rc.e && rc.e.message);
      assert.equal(rc.v.code_mark_same_run, true);
    });

    await t('[K4] 同じ朝の自動を 2 つの接続で同時に = 1 つが作り、もう 1 つは待った後に前の答え (2 回送らない)', async () => {
      const r4 = await newRun();
      await recNe(O, r4);
      await A.query('begin');
      const a = await build(A, 'auto');
      const b = settled(build(B, 'auto'));
      await waitBlocked(pb);
      await A.query('commit');
      const rb = await b;
      assert.ok(rb.ok, rb.e && rb.e.message);
      assert.equal(rb.v.replayed, true);
      assert.equal(rb.v.send_id, a.send_id);
      assert.equal((await O.query("select count(*)::int as n from ops.ne_upd_sends where how = 'auto' and ne_run = $1", [r4])).rows[0].n, 1);
    });

    await t('[K5] 今すぐの頼みを 2 つの接続で同時に = 頼みは 1 つ', async () => {
      await A.query('begin');
      const a = (await A.query('select ops.ne_upd_request_now($1, $2) as r', [crypto.randomUUID(), 'a@test'])).rows[0].r;
      const b = settled(B.query('select ops.ne_upd_request_now($1, $2) as r', [crypto.randomUUID(), 'b@test']).then((x) => x.rows[0].r));
      await waitBlocked(pb);
      await A.query('commit');
      const rb = await b;
      assert.ok(rb.ok, rb.e && rb.e.message);
      assert.equal(rb.v.send_id, a.send_id);
      assert.equal(rb.v.joined, true);
      assert.equal((await O.query("select count(*)::int as n from ops.ne_upd_sends where how = 'now' and state = 'waiting'")).rows[0].n, 1);
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
process.exit(ng || setupError || cleanupFailed ? 1 : 0);
