#!/usr/bin/env node
/**
 * measure-no-temp-mem.mjs — 0057 (一時の表を使わない relink_shipments_bulk / merge_duplicate_suppliers) の backend のピークのメモリを測る (Codex PR #1605 R1 High)
 *
 * 使い捨ての PostgreSQL (embedded-postgres・試験と同じ版) を起動し、大きめの fixture で 旧 (0056 まで = 0017 / 0027 の一時の表) と 新 (0057) を呼んで、
 * 呼んだ接続の backend のプロセスの **ピークの private のメモリ** (Windows = PeakPagefileUsage = Get-Process の PeakPagedMemorySize64。共有メモリ (shared_buffers) は入らない) と
 * ピークの working set (共有メモリの触ったページも入る)・一時のファイルのバイト数 (pg_stat_database.temp_bytes の増え分)・時間を表にする。
 *   --r1-sql <file>   R1 の版 (配列・jsonb の変数) の 0057 の SQL も測る (git show d0e8e24e:db/company/migrations/0057_no_temp_relink_merge.sql > r1.sql)
 *   --work-mem 4MB    work_mem (既定 = PostgreSQL の既定 4MB)
 *   --cases a,b       測るもの (既定 = 全部: relink-short / relink-long / merge-small / merge-large)
 * 測り方:
 *   fixture は別の接続で入れて commit・analyze (本番と同じく統計がある)。測る接続は新しく開き (backend = 新しいプロセス)、接続の直後と `select 1` の後のピークを控えてから
 *   begin → 関数を 1 回 → rollback。ピーク − 控えた値 = 関数が増やしたピーク (backend の heap・一時の表の local buffers (temp_buffers)・work_mem の中間の結果・AFTER の trigger の待ち行列)
 * 限界: OS から見たプロセスの値 = palloc の文脈ごとの内訳は出ない / Windows 以外は /proc の VmHWM (共有メモリも入る) だけ / 1 回ずつ (ぶれは数 MB) /
 *   fixture は合成 (本番の行の幅・分布とは違う) / 本番 (Render の 1GB) の設定 (shared_buffers・work_mem) とは違う = 桁と「件数・文字の長さで増えるか」を見る
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { openPgClient, pgAdapter, applyMigrations } from './migrate.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const args = process.argv.slice(2);
const arg = (f, d = null) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : d; };
const R1_SQL = arg('--r1-sql') ? fs.readFileSync(arg('--r1-sql'), 'utf8') : null;
const WORK_MEM = arg('--work-mem', '4MB');
const CASES = (arg('--cases', 'relink-short,relink-long,merge-small,merge-large')).split(',');

const PINNED = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).devDependencies['embedded-postgres'];
let EmbeddedPostgres = null;
for (const b of [path.join(ROOT, 'package.json'), ...(process.env.EMBEDDED_PG_DIR ? [path.join(process.env.EMBEDDED_PG_DIR, 'package.json')] : []), 'C:/tmp/pg-embed/package.json']) {
  try {
    const req = createRequire(b); const main = req.resolve('embedded-postgres');
    let d = path.dirname(main); while (path.basename(d) !== 'embedded-postgres' && path.dirname(d) !== d) d = path.dirname(d);
    if (JSON.parse(fs.readFileSync(path.join(d, 'package.json'), 'utf8')).version !== PINNED) continue;
    EmbeddedPostgres = (await import(pathToFileURL(main).href)).default; break;
  } catch { /* 次 */ }
}
if (!EmbeddedPostgres) { console.error(`embedded-postgres ${PINNED} が見つからない (npm ci)`); process.exit(1); }

/** backend のプロセスのメモリ (バイト)。Windows = Get-Process / ほか = /proc/<pid>/status */
const procMem = (pid) => {
  if (process.platform === 'win32') {
    const r = spawnSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', `$p = Get-Process -Id ${pid}; "$($p.PeakPagedMemorySize64) $($p.PeakWorkingSet64)"`], { encoding: 'utf8' });
    const [priv, ws] = r.stdout.trim().split(/\s+/).map(Number);
    return { peakPrivate: priv, peakWs: ws };
  }
  const s = fs.readFileSync(`/proc/${pid}/status`, 'utf8');
  const kb = (k) => Number((s.match(new RegExp(`^${k}:\\s+(\\d+)`, 'm')) || [])[1] || NaN) * 1024;
  return { peakPrivate: NaN, peakWs: kb('VmHWM') };
};
const MB = (b) => (Number.isFinite(b) ? (b / 1048576).toFixed(1) : '-');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const dir = path.join(os.tmpdir(), `cdb-notemp-mem-${crypto.randomBytes(4).toString('hex')}`);
const port = 55000 + crypto.randomInt(4000);
const PW = `m_${crypto.randomBytes(8).toString('hex')}`;
const pg = new EmbeddedPostgres({ databaseDir: dir, user: 'postgres', password: PW, port, persistent: false, postgresFlags: ['-c', `work_mem=${WORK_MEM}`, '-c', 'autovacuum=off'], onLog: () => {}, onError: () => {} });
const base = `postgres://postgres:${PW}@127.0.0.1:${port}/`;

// ─── fixture ───
const pad = (expr, len) => `rpad(${expr}, ${len}, 'x')`;
const relinkFixture = (n, len) => `
  insert into core.ne_shops (company_id, shop_code, shop_name, platform, mall, scope_key, order_no_prefix) values (1, 'S1', '楽天', 'rakuten', 'rakuten', 'rk', '');
  insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
    select 1, 'rakuten', 'rk', ${pad(`'B' || g || '-'`, len)}, 'ne', now(), date '2026-09-01', 'new', 1, now(), 't', 'h' from generate_series(1, ${n}) g;
  insert into core.shipments (company_id, ne_slip_no, ne_order_no, shop_code, status, received_batch_seq, source_updated_at, transform_version, content_hash)
    select 1, 'BS' || g, ${pad(`'B' || g || '-'`, len)}, 'S1', 'new', 1, now(), 't', 'h' from generate_series(1, ${n}) g;`;
const mergeFixture = (k, len) => `
  insert into core.suppliers (company_id, code, name) values (1, '0001', '0001'), (1, '1', '寄せる側'), (1, '01', '01');
  insert into core.skus (company_id, sku_kind, code, name) select 1, 'set', 'K' || g, 'K' || g from generate_series(1, ${k}) g;
  insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, order_unit, unit_cost_jpy, created_by_type, created_at)
    select 1, s.supplier_id, k.sku_id, case when s.code = '0001' and k.sku_id % 2 = 0 then null else ${pad(`s.code || '-' || k.sku_id || '-'`, len)} end,
           ${pad(`'u' || s.code`, Math.min(len, 200))}, k.sku_id, 'system', timestamptz '2026-03-04 05:06:07+09' + make_interval(secs => k.sku_id)
      from core.suppliers s cross join core.skus k where s.code <> '0001' or k.sku_id % 3 <> 0;
  insert into docs.documents (company_id, document_type, storage, external_ref, title) select 1, 'contract', 'url', 'd' || g, 'd' || g from generate_series(1, ${k}) g;
  insert into docs.document_links (document_id, entity_type, entity_id, link_role, created_at)
    select d.document_id, 'supplier', s.supplier_id, case when s.code = '0001' and d.document_id % 2 = 0 then null else ${pad(`s.code || '-' || d.document_id || '-'`, len)} end, now()
      from docs.documents d cross join core.suppliers s where s.code <> '0001' or d.document_id % 3 <> 0;`;
const CASE_DEF = {
  'relink-short': { fx: relinkFixture(100000, 16), call: 'select * from core.relink_shipments_bulk(1::smallint, 0, 100000)', what: 'relink 100,000 件・注文番号 16 文字' },
  'relink-long': { fx: relinkFixture(100000, 1000), call: 'select * from core.relink_shipments_bulk(1::smallint, 0, 100000)', what: 'relink 100,000 件・注文番号 1,000 文字 (約 100 MB)' },
  'merge-small': { fx: mergeFixture(2000, 1000), call: 'select * from core.merge_duplicate_suppliers()', what: 'merge 仕入先 3→1・商品の行 約 5,300・文書の紐付け 約 5,300・各 1,000 文字' },
  'merge-large': { fx: mergeFixture(40000, 1000), call: 'select * from core.merge_duplicate_suppliers()', what: 'merge 仕入先 3→1・商品の行 約 107,000・文書の紐付け 約 107,000・各 1,000 文字 (約 200 MB)' },
};

const rows = [];
let su = null;
try {
  await pg.initialise(); await pg.start();
  su = await openPgClient(base + 'postgres');
  const variants = [['old', '0056', null], ['new', null, null], ...(R1_SQL ? [['r1', '0056', R1_SQL]] : [])];
  for (const [v, to, sql] of variants) {
    await su.query(`create database tpl_${v}`);
    const c = await openPgClient(base + `tpl_${v}`);
    await applyMigrations(pgAdapter(c), { log: () => {}, ...(to ? { to } : {}) });
    if (sql) { await c.query('begin'); await c.query(sql); await c.query('commit'); }
    await c.end();
  }
  for (const cs of CASES) {
    const def = CASE_DEF[cs]; if (!def) throw new Error(`知らない case: ${cs}`);
    for (const [v] of variants) {
      const db = `m_${v}_${cs.replace(/-/g, '_')}`;
      await su.query(`create database ${db} template tpl_${v}`);
      try {
        const l = await openPgClient(base + db);
        await l.query('begin'); await l.query(def.fx); await l.query('commit'); await l.query('analyze');
        await l.end();
        const m = await openPgClient(base + db);
        const pid = (await m.query('select pg_backend_pid() as p')).rows[0].p;
        await m.query('select 1');
        const before = procMem(pid);
        const tb0 = Number((await m.query(`select temp_bytes from pg_stat_database where datname = current_database()`)).rows[0].temp_bytes);
        await m.query('begin');
        const t0 = Date.now(); let res, err = null;
        try { res = (await m.query(def.call)).rows[0]; } catch (e) { err = `${e.code} ${e.message}`; }
        const ms = Date.now() - t0;
        await m.query('rollback');
        const after = procMem(pid);
        await m.query('select pg_stat_force_next_flush()'); await sleep(1200); await m.query('select 1'); await sleep(300);
        const tb1 = Number((await m.query(`select temp_bytes from pg_stat_database where datname = current_database()`)).rows[0].temp_bytes);
        await m.end();
        rows.push({ cs, what: def.what, v, res: err || JSON.stringify(res), ms, privBefore: before.peakPrivate, privAfter: after.peakPrivate, wsBefore: before.peakWs, wsAfter: after.peakWs, temp: tb1 - tb0 });
        const r = rows[rows.length - 1];
        console.log(`${cs.padEnd(13)} ${v.padEnd(4)} peak private ${MB(r.privAfter)} MB (+${MB(r.privAfter - r.privBefore)})  peak WS ${MB(r.wsAfter)} MB (+${MB(r.wsAfter - r.wsBefore)})  temp files ${MB(r.temp)} MB  ${ms} ms  ${r.res}`);
      } finally { await su.query(`drop database ${db}`); }
    }
  }
  console.log(`\n| 測ったもの | 版 | ピークの private (MB・関数で増えた分) | ピークの working set (MB・増えた分) | 一時のファイル (MB) | 時間 (ms) |\n|---|---|---|---|---|---|`);
  for (const r of rows) console.log(`| ${r.what} | ${r.v} | ${MB(r.privAfter)} (+${MB(r.privAfter - r.privBefore)}) | ${MB(r.wsAfter)} (+${MB(r.wsAfter - r.wsBefore)}) | ${MB(r.temp)} | ${r.ms} |`);
  console.log(`\n(work_mem = ${WORK_MEM}・PostgreSQL ${(await su.query('show server_version')).rows[0].server_version}・${process.platform})`);
} finally {
  if (su) { try { await su.end(); } catch { /* */ } }
  try { await pg.stop(); } catch { /* */ }
  for (let i = 0; i < 20 && fs.existsSync(dir); i++) { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { await sleep(500); } }
}
process.exit(0);
