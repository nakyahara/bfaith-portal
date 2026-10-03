#!/usr/bin/env node
/**
 * test-company-db-reresolve-batch-pg.mjs — 0057 (reresolve の batch・D-60 PR 1b-0r) を本物の PostgreSQL で確かめる
 *
 * 設計 = AI_reference CompanyDB構想/13 §3.10「reresolve の batch の契約」(v3.12) と Codex R-D60-v3-13 の L1 (bigint の上限)・L2 (cursor の行の CHECK)。
 * 持ち主は本番と同じ形 = superuser でない login の役割 (CREATEROLE あり) が DB を持ち、全部の migration を流す。
 * 夜の回し方は apps/company-db/load/reresolve-batch.mjs (1b-0e で engine が使う部品・この PR では engine から呼ばない) で流す。
 *
 * 固定する契約 (番号は出力の見出し):
 *   A 引数 (null・63 日・p_since ≧ p_until・上限の範囲の外・p_before ≦ p_after = 22023) / [p_since, p_until) = 終わりを含まない
 *   B 旧い 3 引数 (0024) と新しい関数で、同じ fixture の結果 (解かれた明細・数) が同じ (上限の中・小さい上限で cursor を回しても)
 *   C cursor と stop_reason (max_orders・max_lines・done)・1 つ目の注文は必ず含める・max_lines / max_orders で含めなかった次の注文は数えない (lock されていても)
 *   D skip → その batch の中で retry の表 → retry の関数 (attempts) → lock を外すと処理して消す / 消えた注文 / 窓の関数が retry の表の注文を処理したら消す / 2 つの接続で同じ窓
 *   E 夜: skip → retry → 20 batch の上限 → 日付をまたぐ → 翌晩に処理 (どの注文もちょうど 1 回) / lock された先頭の群 + 後ろ + 窓で何晩も前に進む /
 *     増え続ける末尾で high-water の周回が終わり低い id が再訪される / 空なら呼ばない / 巻き戻り / 予算 ≧ N は流す前に止まる / cycle_started_at
 *   F bigint の最大値 (high-water + 1 は null・精度) / CHECK
 *   G 1 周の見込みの晩の数 (1 注文 500 明細で過小にならない・ばらばらの明細の数で B ≧ 実際) / ⚠️ の 4 つ
 *   H 権限 (PUBLIC・watcher)・一時の表なし (TEMP の無い役割で新しい関数は動き、旧い関数は動かない)・engine は旧い署名のまま・manifest・dump / restore
 * 使い方: node scripts/test-company-db-reresolve-batch-pg.mjs   (npm run test:company-db にも入っている = 飛ばさない)
 *   🚨 試験が自分で使い捨てのクラスタを起動する (embedded-postgres・OS の一時フォルダ・ランダムのポート・最後に止めて消す)。外の PostgreSQL には一切つながない。
 *      embedded-postgres は devDependencies の正確な版 (探す順 = リポジトリの node_modules → 版が同じときだけ EMBEDDED_PG_DIR / C:/tmp/pg-embed)。
 *      見つからない・版が違う・起動できない・クラスタのフォルダが消えない = 失敗 (exit 1)
 */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { heavyEntryFindings, tempPrivilegeAudit, HEAVY_ENTRY_MANIFEST } from './company-db/heavy-entry-manifest.mjs';
import { dumpCompanyDb, restoreCompanyDb, listTables, listSequences } from '../apps/company-db/backup/dump.mjs';
import {
  runReresolveNight, runNightBody, reserveNight, readRetryState, estimateCycleNights, retryWarnings, retryReportLine, beforeOrderIdFor, nightWindow,
  PG_BIGINT_MAX, RERESOLVE_NIGHT_DEFAULTS,
} from '../apps/company-db/load/reresolve-batch.mjs';

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
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の試験を流せない (飛ばさない)。リポジトリで npm ci (devDependencies に入っている)');
  process.exit(1);
}
const { EmbeddedPostgres } = loaded;
const clusterDir = path.join(os.tmpdir(), `cdb-rr-pg-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
const cluster = new EmbeddedPostgres({ databaseDir: clusterDir, user: 'postgres', password: SU_PW, port, persistent: false, onLog: () => {}, onError: () => {} });
let cleanupFailed = false;
const stopCluster = async () => {
  try { await cluster.stop(); } catch (e) { console.error('使い捨てのクラスタを止めるときの誤り: ' + e.message); }
  for (let i = 0; i < 10 && fs.existsSync(clusterDir); i++) { try { fs.rmSync(clusterDir, { recursive: true, force: true }); } catch { await new Promise((r) => setTimeout(r, 500)); } }
  if (fs.existsSync(clusterDir)) { cleanupFailed = true; console.error('❌ 使い捨てのクラスタのフォルダが消えない: ' + clusterDir); }
};
const url = `postgres://postgres:${SU_PW}@127.0.0.1:${port}/postgres`;
console.log('使い捨てのクラスタ: embedded-postgres ' + PINNED_EMBEDDED_PG + ' (' + loaded.from + ')');

let ok = 0, ng = 0;
const t = async (name, fn) => { const t0 = Date.now(); try { await fn(); ok++; console.log('  ok  ' + name + ' (' + (Date.now() - t0) + ' ms)'); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const hex = crypto.randomBytes(4).toString('hex');
const OWNER = `cdb_rro_${hex}`, PROBE = `cdb_rrp_${hex}`, NOTEMP = `cdb_rrn_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const DB1 = `cdb_rr_${hex}`, DB2 = `cdb_rrb_${hex}`;
const MALL = 'rakuten';
const T0 = '2026-10-10';
const addDays = (d, n) => { const x = new Date(`${d}T00:00:00Z`); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); };
const range = (a, b) => Array.from({ length: b - a + 1 }, (_, i) => a + i);
const B = (v) => BigInt(String(v));
const sortB = (a) => [...a].sort((x, y) => (x < y ? -1 : x > y ? 1 : 0));
const W = (today) => ({ since: addDays(today, -35), until: addDays(today, 1) });   // 夜の窓 = [今日 − 35, 明日)
const NIGHT = { nightCap: 20, retryBudget: 5, maxOrders: 4, maxLines: 100 };

const clients = [];
let setupError = null;
try {
  await cluster.initialise();
  await cluster.start();
  const su = await openPgClient(url);
  try {
    const existing = (await su.query(`select rolname from pg_roles where rolname = 'watcher'`)).rows;
    if (existing.length) throw new Error('使い捨てのクラスタのはずが watcher が既にある = 止める');
    await su.query(`create role ${OWNER} login createrole password '${PW}'`);   // Render の default user と同じ: superuser でない・CREATEROLE
    await su.query(`create role ${PROBE} login password '${PW}'`);              // PUBLIC の権限だけ
    await su.query(`create role ${NOTEMP} login password '${PW}'`);             // TEMP の無い役割 (一時の表を使わないことの確かめ)
    await su.query(`create role watcher login password '${PW}'`);
    await su.query(`create database ${DB1} owner ${OWNER}`);
    await su.query(`create database ${DB2} owner ${OWNER}`);
    const open = async (role, dbName = DB1) => { const x = new URL(url); x.username = role; x.password = PW; x.pathname = `/${dbName}`; const c = await openPgClient(x.toString()); c.on('error', () => {}); clients.push(c); return c; };
    const O = await open(OWNER);
    const odb = pgAdapter(O);
    const migrated = await applyMigrations(odb, { log: quiet });
    assert.ok(migrated.applied.includes('0057'), '0057 が流れていない');
    // create-watch-roles.mjs と同じ: watcher は schema の USAGE と表の SELECT
    await O.query(`grant usage on schema core, ops, mart to watcher, ${PROBE}, ${NOTEMP}; grant select on all tables in schema core to watcher`);
    // 試験の記録: touch (core.touch_force = on の update) と明細の解決を数える (関数の search_path = pg_catalog, pg_temp でも動くよう schema で修飾)
    await O.query(`create schema t;
      create table t.touch_log (order_id bigint not null, at timestamptz not null default clock_timestamp());
      create function t.log_touch() returns trigger language plpgsql as $$ begin
        if coalesce(pg_catalog.current_setting('core.touch_force', true), '') = 'on' then insert into t.touch_log (order_id) values (new.order_id); end if; return null; end $$;
      create trigger trg_t_touch_log after update on core.orders for each row execute function t.log_touch();`);
    const W1 = await open('watcher'), P = await open(PROBE), N = await open(NOTEMP);
    const LB1 = await open(OWNER), LB2 = await open(OWNER), LB3 = await open(OWNER);   // 別の書き手 (lock を持つ)

    // ─── fixture ───
    let orderNo = 0;
    const companies = new Set();
    const ensureCompany = async (c) => {
      if (companies.has(c)) return; companies.add(c);
      await O.query(`insert into core.companies (company_id, name, kind) values ($1, $2, 'subsidiary') on conflict do nothing`, [c, `試験 ${c}`]);
      await O.query(`insert into core.listings (company_id, mall, shop_code, listing_code) select $1::smallint, $2, $3, unnest(array['S1','S2','S3'])`, [c, MALL, `shop${c}`]);
    };
    /** 注文 1 つ (明細のコードの配列)。id を渡すと overriding system value。戻り = BigInt の order_id */
    const addOrder = async (c, date, codes, { id = null, mall = MALL } = {}) => {
      await ensureCompany(c);
      const no = `o${++orderNo}`;
      const r = (await O.query(`insert into core.orders ${id === null ? '' : ''}(${id === null ? '' : 'order_id, '}company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
        ${id === null ? '' : 'overriding system value '}values (${id === null ? '' : '$5::bigint, '}$1, $2, 's', $3, 'mall_api', $4::date, $4::date, 'new', 1, now(), 't', 'h') returning order_id`,
        id === null ? [c, mall, no, date] : [c, mall, no, date, String(id)])).rows[0].order_id;
      if (codes.length) await O.query(`insert into core.order_lines (company_id, order_id, line_key, unresolved_code, qty, received_batch_seq) select $1, $2, 'L' || i, code, 1, 1 from unnest($3::text[]) with ordinality u(code, i)`, [c, r, codes]);
      return B(r);
    };
    /** 注文をまとめて (n 注文・明細の数 = linesOf(i)・全部 S1) */
    const addOrdersBulk = async (c, date, n, linesOf) => {
      await ensureCompany(c);
      const base = orderNo; orderNo += n;
      const ids = (await O.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
        select $1, $2, 's', 'o' || ($3::int + g), 'mall_api', $4::date, $4::date, 'new', 1, now(), 't', 'h' from generate_series(1, $5::int) g order by g returning order_id`, [c, MALL, base, date, n])).rows.map((r) => B(r.order_id));
      const sorted = sortB(ids);
      await O.query(`insert into core.order_lines (company_id, order_id, line_key, unresolved_code, qty, received_batch_seq)
        select $1, x.id, 'L' || i, 'S1', 1, 1 from unnest($2::bigint[], $3::int[]) as x(id, n), generate_series(1, x.n) i`, [c, sorted.map(String), sorted.map((_, i) => linesOf(i))]);
      return sorted;
    };
    const putRetry = async (c, ids, { attempts = 0, firstAgo = null } = {}) => O.query(`insert into ops.reresolve_retry_orders (company_id, mall, order_id, attempts, first_skipped_at)
      select $1, $2, unnest($3::bigint[]), $4, coalesce(now() - $5::interval, now())`, [c, MALL, ids.map(String), attempts, firstAgo]);
    const lockOrders = async (conn, ids) => { await conn.query('begin'); await conn.query('select 1 from core.orders where order_id = any($1::bigint[]) for update', [ids.map(String)]); };
    const lockMore = async (conn, ids) => conn.query('select 1 from core.orders where order_id = any($1::bigint[]) for update', [ids.map(String)]);
    const unresolvedOf = async (ids) => (await O.query(`select distinct order_id from core.order_lines where order_id = any($1::bigint[]) and unresolved_code is not null and removed_at is null order by 1`, [ids.map(String)])).rows.map((r) => B(r.order_id));
    const touchCounts = async (ids) => { const m = new Map(ids.map((i) => [String(i), 0])); for (const r of (await O.query(`select order_id, count(*)::int as n from t.touch_log where order_id = any($1::bigint[]) group by 1`, [ids.map(String)])).rows) m.set(String(r.order_id), r.n); return m; };
    const exactlyOnce = async (ids) => { const m = await touchCounts(ids); const bad = [...m].filter(([, n]) => n !== 1); return { ok: bad.length === 0, bad }; };
    const retryRows = async (c) => new Map((await O.query(`select order_id, attempts from ops.reresolve_retry_orders where company_id = $1 and mall = $2`, [c, MALL])).rows.map((r) => [String(r.order_id), r.attempts]));
    const cursorOf = async (c) => (await O.query(`select after_order_id, cycle_through_order_id, cycle_started_at, cycles_completed from ops.reresolve_retry_cursor where company_id = $1 and mall = $2`, [c, MALL])).rows[0];
    const backlogOf = async (c) => (await O.query(`select p_since::text as s, p_until::text as u, after_order_id, saved_at from ops.reresolve_backlog_windows where company_id = $1 and mall = $2 order by p_since`, [c, MALL])).rows;
    const maxRetryId = async (c) => B((await O.query(`select coalesce(max(order_id), 0) as m from ops.reresolve_retry_orders where company_id = $1 and mall = $2`, [c, MALL])).rows[0].m);
    const win = async (conn, c, since, until, after = 0n, mo = 2000, ml = 10000) => (await conn.query(`select * from core.reresolve_order_lines($1::smallint, $2, $3::date, $4::date, $5::bigint, $6, $7)`, [c, MALL, since, until, String(after), mo, ml])).rows[0];
    const retryFn = async (conn, c, after = 0n, before = null, mo = 2000, ml = 10000) => (await conn.query(`select * from core.reresolve_order_lines_retry($1::smallint, $2, $3::bigint, $4::bigint, $5, $6)`, [c, MALL, String(after), before === null ? null : String(before), mo, ml])).rows[0];
    const codeOf = async (conn, sql, params) => { await conn.query('begin'); try { await conn.query(sql, params); return 'ok'; } catch (e) { return e.code || e.message; } finally { await conn.query('rollback'); } };
    const night = (c, today, opts = {}) => runReresolveNight(odb, { company: c, mall: MALL, ...NIGHT, window: W(today), ...opts });
    /** 窓を cursor で has_more が false になるまで (1 走査)。戻り = { batches, sums, rows } */
    const scan = async (conn, c, since, until, mo, ml) => {
      const rows = []; let after = 0n;
      for (let i = 0; i < 1000; i++) { const r = await win(conn, c, since, until, after, mo, ml); rows.push(r); after = B(r.next_after_order_id); if (!r.has_more) break; }
      const sums = rows.reduce((a, r) => ({ candidates: a.candidates + r.candidates, resolved: a.resolved + r.resolved, orders_touched: a.orders_touched + r.orders_touched, skipped: a.skipped + r.orders_skipped_locked }), { candidates: 0, resolved: 0, orders_touched: 0, skipped: 0 });
      return { batches: rows.length, sums, rows };
    };

    console.log('A 引数');
    await t('窓の関数: null (7 つの引数のどれでも)・63 日・p_since ≧ p_until・上限の範囲の外・p_after < 0 は 22023 / 62 日・上限の端 (1・5000・20000) は通る', async () => {
      await ensureCompany(11);
      const call = `select * from core.reresolve_order_lines($1::smallint, $2::text, $3::date, $4::date, $5::bigint, $6::integer, $7::integer)`;
      const good = [11, MALL, T0, addDays(T0, 10), '0', 2000, 10000];
      for (let i = 0; i < 7; i++) { const a = [...good]; a[i] = null; assert.equal(await codeOf(O, call, a), '22023', `null の引数 ${i + 1}`); }
      for (const [s, u] of [[T0, addDays(T0, 63)], [T0, T0], [addDays(T0, 1), T0]]) assert.equal(await codeOf(O, call, [11, MALL, s, u, '0', 2000, 10000]), '22023', `${s}..${u}`);
      assert.equal(await codeOf(O, call, [11, MALL, T0, addDays(T0, 62), '0', 2000, 10000]), 'ok', '62 日');
      for (const [mo, ml] of [[0, 10], [5001, 10], [10, 0], [10, 20001]]) assert.equal(await codeOf(O, call, [11, MALL, T0, addDays(T0, 1), '0', mo, ml]), '22023', `${mo}/${ml}`);
      for (const [mo, ml] of [[1, 1], [5000, 20000]]) assert.equal(await codeOf(O, call, [11, MALL, T0, addDays(T0, 1), '0', mo, ml]), 'ok', `${mo}/${ml}`);
      assert.equal(await codeOf(O, call, [11, MALL, T0, addDays(T0, 1), '-1', 10, 10]), '22023', 'p_after < 0');
      // 既定の引数 (4 つだけ) で呼べる・3 つだけは旧い関数 (曖昧にならない)
      assert.equal(await codeOf(O, `select * from core.reresolve_order_lines(11::smallint, '${MALL}', '${T0}'::date, '${addDays(T0, 1)}'::date)`), 'ok');
    });
    await t('retry の関数: null (p_before 以外)・上限の範囲の外は 22023 / p_before ≦ p_after は 22023 (等しくても) / p_before = null は上限なし', async () => {
      const call = `select * from core.reresolve_order_lines_retry($1::smallint, $2::text, $3::bigint, $4::bigint, $5::integer, $6::integer)`;
      const good = [11, MALL, '0', null, 2000, 10000];
      for (const i of [0, 1, 2, 4, 5]) { const a = [...good]; a[i] = null; assert.equal(await codeOf(O, call, a), '22023', `null の引数 ${i + 1}`); }
      assert.equal(await codeOf(O, call, good), 'ok');
      for (const [a, b] of [['5', '5'], ['5', '4']]) assert.equal(await codeOf(O, call, [11, MALL, a, b, 10, 10]), '22023', `${a} / ${b}`);
      assert.equal(await codeOf(O, call, [11, MALL, '5', '6', 10, 10]), 'ok');
      for (const [mo, ml] of [[0, 10], [5001, 10], [10, 0], [10, 20001]]) assert.equal(await codeOf(O, call, [11, MALL, '0', null, mo, ml]), '22023', `${mo}/${ml}`);
    });
    await t('[p_since, p_until) = p_since の日は入り、p_until の日は入らない (前の日も入らない)・候補が無ければ done で cursor はそのまま', async () => {
      const before = await addOrder(12, addDays(T0, -1), ['S1']);
      const first = await addOrder(12, T0, ['S1']);
      const mid = await addOrder(12, addDays(T0, 5), ['S2']);
      const atUntil = await addOrder(12, addDays(T0, 10), ['S3']);
      const r = await win(O, 12, T0, addDays(T0, 10), 0n, 100, 100);
      assert.deepEqual([r.orders_examined, r.resolved, r.stop_reason, r.has_more, B(r.next_after_order_id)], [2, 2, 'done', false, mid]);
      assert.deepEqual(await unresolvedOf([before, first, mid, atUntil]), [before, atUntil]);
      const r2 = await win(O, 12, T0, addDays(T0, 10), 0n, 100, 100);
      assert.deepEqual([r2.orders_examined, r2.candidates, r2.stop_reason, r2.has_more, r2.next_after_order_id], [0, 0, 'done', false, '0']);
    });

    console.log('B 旧い 3 引数と同じ結果');
    await t('同じ fixture で、旧い 3 引数 (0024) と新しい関数 (既定の上限で 1 batch / 小さい上限で cursor を回す) の解いた明細・数が同じ', async () => {
      const c = 13;
      await addOrder(c, addDays(T0, -3), ['S1']);                       // 窓の前 (どちらも見ない)
      const ids = [];
      ids.push(await addOrder(c, T0, ['S1', 'S2']));                     // 全部解ける
      ids.push(await addOrder(c, addDays(T0, 1), ['S1', 'NX']));         // 一部
      ids.push(await addOrder(c, addDays(T0, 2), ['NX', 'NY']));         // 解けない
      ids.push(await addOrder(c, addDays(T0, 3), [' s3 ']));             // 正規化で当たる
      const rm = await addOrder(c, addDays(T0, 4), ['S1', 'S2']);         // 1 行は外れた明細 (removed_at)
      await O.query(`update core.order_lines set removed_at = now() where order_id = $1 and line_key = 'L1'`, [String(rm)]); ids.push(rm);
      const done = await addOrder(c, addDays(T0, 5), []);                 // 解けた明細だけ (候補ではない)
      const lid = (await O.query(`select listing_id from core.listings where company_id = $1 and listing_code = 'S1'`, [c])).rows[0].listing_id;
      await O.query(`insert into core.order_lines (company_id, order_id, line_key, listing_id, qty, received_batch_seq) values ($1, $2, 'L1', $3, 1, 1)`, [c, String(done), lid]); ids.push(done);
      for (let i = 0; i < 6; i++) ids.push(await addOrder(c, addDays(T0, 6 + i), i % 2 ? ['S2', 'S3', 'NX'] : ['S1']));
      await addOrder(c, addDays(T0, 2), ['S1'], { mall: 'yahoo' });       // 別のモール (どちらも見ない)
      const state = async () => (await O.query(`select o.mall_order_no, l.line_key, l.listing_id, l.unresolved_code, l.removed_at is null as cur, o.updated_at > o.created_at as touched
        from core.orders o join core.order_lines l on l.order_id = o.order_id where o.company_id = $1 order by 1, 2`, [c])).rows;
      const touchLog = async () => (await O.query(`select order_id::text, count(*)::int as n from t.touch_log where order_id in (select order_id from core.orders where company_id = $1) group by 1 order by 1`, [c])).rows;
      await O.query('begin');
      const rOld = (await O.query(`select candidates, resolved, orders_touched, orders_skipped_locked from core.reresolve_order_lines($1::smallint, $2, $3::date)`, [c, MALL, T0])).rows[0];
      const sOld = await state(); const tOld = await touchLog();
      await O.query('rollback');
      const runs = [];
      for (const [mo, ml] of [[2000, 10000], [2, 3], [1, 1], [3, 20000]]) {
        await O.query('begin');
        const s = await scan(O, c, T0, addDays(T0, 62), mo, ml);
        runs.push({ mo, ml, s, state: await state(), touch: await touchLog() });
        await O.query('rollback');
      }
      assert.ok(rOld.resolved > 0 && rOld.candidates > rOld.resolved, JSON.stringify(rOld));
      for (const r of runs) {
        assert.deepEqual(r.state, sOld, `明細 (${r.mo}/${r.ml})`);
        assert.deepEqual(r.touch, tOld, `touch (${r.mo}/${r.ml})`);
        assert.deepEqual([r.s.sums.candidates, r.s.sums.resolved, r.s.sums.orders_touched, r.s.sums.skipped], [rOld.candidates, rOld.resolved, rOld.orders_touched, rOld.orders_skipped_locked], `数 (${r.mo}/${r.ml})`);
        assert.equal(r.s.rows[r.s.rows.length - 1].stop_reason, 'done');
      }
      assert.equal(runs[0].s.batches, 1);
      assert.ok(runs[1].s.batches > 3 && runs[2].s.batches >= 10, runs.map((r) => r.s.batches).join(','));
      assert.ok(tOld.every((x) => x.n === 1));
    });

    console.log('C cursor と stop_reason');
    await t('注文の上限で止まると max_orders (has_more)・cursor は含めた最後の注文・全部の注文が 1 回ずつ処理され、最後は done / 候補がちょうど上限なら done', async () => {
      const ids = []; for (let i = 0; i < 7; i++) ids.push(await addOrder(14, T0, ['S1', 'S2']));
      const s = await scan(O, 14, T0, addDays(T0, 1), 3, 100);
      assert.deepEqual(s.rows.map((r) => [r.orders_examined, r.stop_reason, r.has_more, B(r.next_after_order_id)]), [[3, 'max_orders', true, ids[2]], [3, 'max_orders', true, ids[5]], [1, 'done', false, ids[6]]]);
      assert.ok((await exactlyOnce(ids)).ok);
      const ids2 = []; for (let i = 0; i < 3; i++) ids2.push(await addOrder(14, addDays(T0, 2), ['S1']));
      const r = await win(O, 14, addDays(T0, 2), addDays(T0, 3), 0n, 3, 100);
      assert.deepEqual([r.orders_examined, r.stop_reason, r.has_more], [3, 'done', false]);
    });
    await t('明細の上限で止まると max_lines・累計がちょうど上限は含める・1 注文の明細が上限を超えても 1 つ目は処理して前に進む', async () => {
      const a = await addOrder(15, T0, ['S1', 'S1', 'S1']), b = await addOrder(15, T0, ['S2', 'S2', 'S2', 'S2']), c3 = await addOrder(15, T0, ['S3', 'S3', 'S3']);
      const r1 = await win(O, 15, T0, addDays(T0, 1), 0n, 100, 7);
      assert.deepEqual([r1.orders_examined, r1.candidates, r1.stop_reason, r1.has_more, B(r1.next_after_order_id)], [2, 7, 'max_lines', true, b]);   // 3 + 4 = 7 (ちょうど) は含める・+3 = 10 > 7
      const r2 = await win(O, 15, T0, addDays(T0, 1), b, 100, 7);
      assert.deepEqual([r2.orders_examined, r2.stop_reason, B(r2.next_after_order_id)], [1, 'done', c3]);
      const big = await addOrder(15, addDays(T0, 2), Array(30).fill('S1')); const after = await addOrder(15, addDays(T0, 2), ['S2']);
      const r3 = await win(O, 15, addDays(T0, 2), addDays(T0, 3), 0n, 100, 10);
      assert.deepEqual([r3.orders_examined, r3.candidates, r3.resolved, r3.stop_reason, B(r3.next_after_order_id)], [1, 30, 30, 'max_lines', big]);
      const r4 = await win(O, 15, addDays(T0, 2), addDays(T0, 3), big, 100, 10);
      assert.deepEqual([r4.orders_examined, r4.stop_reason, B(r4.next_after_order_id)], [1, 'done', after]);
      assert.ok((await exactlyOnce([a, b, c3, big, after])).ok);
    });
    await t('🚨 max_lines / max_orders で含めなかった次の注文は、別の接続が lock していても orders_examined・skipped_order_ids・retry の表・cursor に入らない (次の batch の最初)', async () => {
      const a = await addOrder(16, T0, ['S1', 'S1', 'S1']), b = await addOrder(16, T0, ['S1', 'S1', 'S1']), c3 = await addOrder(16, T0, ['S1', 'S1', 'S1']);
      await lockOrders(LB1, [c3]);
      try {
        const r1 = await win(O, 16, T0, addDays(T0, 1), 0n, 100, 7);
        assert.deepEqual([r1.orders_examined, r1.orders_skipped_locked, r1.skipped_order_ids, r1.stop_reason, B(r1.next_after_order_id)], [2, 0, [], 'max_lines', b]);
        assert.equal((await retryRows(16)).size, 0);
        const r2 = await win(O, 16, T0, addDays(T0, 1), b, 100, 7);
        assert.deepEqual([r2.orders_examined, r2.orders_skipped_locked, r2.skipped_order_ids.map(B), r2.stop_reason, B(r2.next_after_order_id)], [1, 1, [c3], 'done', c3]);
        assert.deepEqual([...(await retryRows(16)).keys()], [String(c3)]);
        // max_orders の +1 件目も同じ (lock されていても数えない)
        const d1 = await addOrder(16, addDays(T0, 2), ['S1']), d2 = await addOrder(16, addDays(T0, 2), ['S1']);
        await lockMore(LB1, [d2]);
        const r3 = await win(O, 16, addDays(T0, 2), addDays(T0, 3), 0n, 1, 100);
        assert.deepEqual([r3.orders_examined, r3.orders_skipped_locked, r3.stop_reason, B(r3.next_after_order_id)], [1, 0, 'max_orders', d1]);
        assert.ok(!(await retryRows(16)).has(String(d2)));
      } finally { await LB1.query('rollback'); }
    });

    console.log('D skip と retry の表');
    await t('別の接続が lock した注文は skip され、その batch の中で retry の表へ (attempts 0)・cursor は越える / retry の関数は lock 中なら attempts + 1・外れたら処理して行を消す', async () => {
      const ids = []; for (let i = 0; i < 4; i++) ids.push(await addOrder(17, T0, ['S1']));
      await lockOrders(LB1, [ids[1]]);
      let r1;
      try {
        await O.query('begin');
        r1 = await win(O, 17, T0, addDays(T0, 1), 0n, 100, 100);
        const inTx = (await O.query(`select order_id, attempts from ops.reresolve_retry_orders where company_id = 17`)).rows;   // 同じ取引の中で見える = batch の中で書いた
        await O.query('commit');
        assert.deepEqual([r1.orders_examined, r1.orders_skipped_locked, r1.skipped_order_ids.map(B), B(r1.next_after_order_id), r1.resolved, r1.stop_reason], [4, 1, [ids[1]], ids[3], 3, 'done']);
        assert.deepEqual(inTx.map((x) => [B(x.order_id), x.attempts]), [[ids[1], 0]]);
        const before = (await O.query(`select last_skipped_at from ops.reresolve_retry_orders where company_id = 17`)).rows[0].last_skipped_at;
        const r2 = await retryFn(O, 17, 0n, null, 100, 100);
        assert.deepEqual([r2.orders_examined, r2.orders_skipped_locked, r2.skipped_order_ids.map(B), r2.stop_reason], [1, 1, [ids[1]], 'done']);
        const row = (await O.query(`select attempts, last_skipped_at from ops.reresolve_retry_orders where company_id = 17`)).rows[0];
        assert.ok(row.attempts === 1 && row.last_skipped_at > before, JSON.stringify(row));
        // 窓の関数の skip は attempts を増やさず last_skipped_at だけ進める (行が既にあれば)
        await O.query(`update core.order_lines set unresolved_code = 'S1', listing_id = null where order_id = $1`, [String(ids[0])]);   // 窓の候補に戻す (ids[1] は lock のまま)
        const r3 = await win(O, 17, T0, addDays(T0, 1), 0n, 100, 100);
        assert.deepEqual(r3.skipped_order_ids.map(B), [ids[1]]);
        assert.equal((await retryRows(17)).get(String(ids[1])), 1);
      } finally { await LB1.query('rollback'); }
      const r4 = await retryFn(O, 17, 0n, null, 100, 100);
      assert.deepEqual([r4.orders_examined, r4.orders_skipped_locked, r4.resolved], [1, 0, 1]);
      assert.equal((await retryRows(17)).size, 0);
      assert.deepEqual(await unresolvedOf(ids), []);
    });
    await t('窓の関数が retry の表にある注文を処理したら行を消す / 消えた注文の retry の行は skip に数えず消す (永久に残らない)', async () => {
      const y = await addOrder(17, addDays(T0, 3), ['S2']);
      await putRetry(17, [y, 987654321n]);
      const r = await win(O, 17, addDays(T0, 3), addDays(T0, 4), 0n, 100, 100);
      assert.deepEqual([r.orders_examined, r.resolved], [1, 1]);
      assert.deepEqual([...(await retryRows(17)).keys()], ['987654321']);
      const r2 = await retryFn(O, 17, 0n, null, 100, 100);
      assert.deepEqual([r2.orders_examined, r2.orders_skipped_locked, r2.skipped_order_ids, r2.stop_reason], [1, 0, [], 'done']);
      assert.equal((await retryRows(17)).size, 0);
    });
    await t('2 つの接続で同じ窓 = 後の接続は前の接続が lock した注文を skip (数と id)・retry の表を通って全部の注文がちょうど 1 回', async () => {
      const ids = []; for (let i = 0; i < 6; i++) ids.push(await addOrder(18, T0, ['S1', 'S3']));
      await LB2.query('begin');
      const a = await win(LB2, 18, T0, addDays(T0, 1), 0n, 3, 100);
      const b = await win(O, 18, T0, addDays(T0, 1), 0n, 100, 100);
      await LB2.query('commit');
      assert.deepEqual([a.orders_examined, a.resolved, a.has_more], [3, 6, true]);
      assert.deepEqual([b.orders_examined, b.orders_skipped_locked, b.skipped_order_ids.map(B), b.resolved], [6, 3, ids.slice(0, 3), 6]);
      const r = await retryFn(O, 18, 0n, null, 100, 100);   // 前の接続が解いた後 = lock は取れるが解く明細は無い → 行を消す
      assert.deepEqual([r.orders_examined, r.orders_skipped_locked, r.candidates, r.resolved], [3, 0, 0, 0]);
      assert.equal((await retryRows(18)).size, 0);
      assert.ok((await exactlyOnce(ids)).ok);
    });

    console.log('E 夜の回し方 (apps/company-db/load/reresolve-batch.mjs)');
    await t('🚨 skip → retry の表 → 1 晩の上限 20 batch で窓を持ち越し → 日付をまたぐ (skip した注文の日が今日の窓の外) → 翌晩に retry の関数が処理・持ち越しの窓を続きから・どの注文もちょうど 1 回', async () => {
      const c = 19; const ids = [];
      for (let i = 0; i < 30; i++) ids.push(await addOrder(c, addDays(T0, -35 + i), ['S1']));
      const A = ids[0];   // 窓の最古の日 (T0 − 35)
      await lockOrders(LB1, [A]);
      let n1;
      try { n1 = await night(c, T0, { nightCap: 20, retryBudget: 5, maxOrders: 1, maxLines: 100 }); } finally { await LB1.query('rollback'); }
      const bl = await backlogOf(c);
      assert.deepEqual([n1.committed, n1.retryBatches, n1.probes, n1.windowBatches, n1.windowsCarried, n1.skippedInWindows], [true, 0, 0, 20, 1, [A]]);
      assert.deepEqual(bl.map((x) => [x.s, x.u, B(x.after_order_id), !!x.saved_at]), [[addDays(T0, -35), addDays(T0, 1), ids[19], true]]);
      assert.deepEqual([...(await retryRows(c)).keys()], [String(A)]);
      const n2 = await night(c, addDays(T0, 1), { nightCap: 20, retryBudget: 5, maxOrders: 1, maxLines: 100 });   // 今日の窓 = [T0 − 34, T0 + 2) = A の日は外
      assert.deepEqual([n2.retryBatches, n2.cycleStarted, n2.cycleCompleted, n2.windowsDone, n2.windowsCarried], [1, true, true, 2, 0]);
      assert.equal(n2.batches, 1 + 10 + 1);
      assert.deepEqual(await unresolvedOf(ids), []);
      const once = await exactlyOnce(ids); assert.ok(once.ok, JSON.stringify(once.bad));
      assert.deepEqual([(await retryRows(c)).size, (await backlogOf(c)).length], [0, 0]);
      const cur = await cursorOf(c);
      assert.deepEqual([cur.after_order_id, cur.cycle_through_order_id, cur.cycles_completed], ['0', null, 1]);
    });
    await t('🚨 lock された retry の先頭の群 (90 > 20 batch × 4) + 処理できる後ろ 10 + 窓: 毎晩 窓に 15 batch 以上・cursor は 20 → 40 → 60 → 80 と夜をまたいで進み 5 晩目に後ろが解ける・lock を外すと全部ちょうど 1 回', async () => {
      const c = 20;
      const lockedIds = await addOrdersBulk(c, addDays(T0, -50), 90, () => 1);
      const backIds = await addOrdersBulk(c, addDays(T0, -50), 10, () => 1);
      await putRetry(c, [...lockedIds, ...backIds]);
      const winIds = []; for (let i = 0; i < 8; i++) winIds.push(await addOrder(c, addDays(T0, -10), ['S2', 'S3']));
      await lockOrders(LB3, lockedIds);
      const nights = []; const curs = []; const newIds = [];
      try {
        for (let k = 0; k < 5; k++) {
          if (k > 0 && k < 3) for (let j = 0; j < 2; j++) newIds.push(await addOrder(c, addDays(T0, k), ['S1']));
          nights.push(await night(c, addDays(T0, k))); curs.push(await cursorOf(c));
        }
      } finally { await LB3.query('rollback'); }
      assert.ok(nights.every((n) => n.committed && n.retryBatches <= 5 && n.windowBatches >= 1 && n.windowsCarried === 0), JSON.stringify(nights.map((n) => [n.retryBatches, n.windowBatches])));
      assert.deepEqual(await unresolvedOf([...winIds, ...newIds, ...backIds]), []);
      assert.deepEqual(curs.slice(0, 4).map((x) => B(x.after_order_id)), [lockedIds[19], lockedIds[39], lockedIds[59], lockedIds[79]]);
      assert.ok(curs.slice(0, 4).every((x) => B(x.cycle_through_order_id) === backIds[9]));
      assert.deepEqual([nights[4].cycleCompleted, curs[4].after_order_id, curs[4].cycle_through_order_id, curs[4].cycles_completed], [true, '0', null, 1]);
      const att = await retryRows(c); assert.ok(lockedIds.every((id) => att.get(String(id)) === 1), '1 周で attempts 1');
      for (let k = 5; k < 14 && (await retryRows(c)).size > 0; k++) nights.push(await night(c, addDays(T0, k)));
      const all = [...lockedIds, ...backIds, ...winIds, ...newIds];
      const once = await exactlyOnce(all); assert.ok(once.ok, JSON.stringify(once.bad.slice(0, 5)));
      assert.deepEqual([(await retryRows(c)).size, (await backlogOf(c)).length], [0, 0]);
    });
    await t('🚨 増え続ける retry の末尾 (毎晩 処理能力 8 < 新しい lock の注文 10): 周回の high-water で周回が終わり、通った低い id は lock を外すと有限の晩数で再訪・attempts は 1 晩に 1 まで・窓は毎晩進む', async () => {
      const c = 21; const GN = 40, GR = 2, GO = 4, TAIL = 10;
      const lowIds = await addOrdersBulk(c, addDays(T0, -50), 20, () => 1);
      await putRetry(c, lowIds);
      await lockOrders(LB1, lowIds);
      await LB2.query('begin');
      const log = []; let revisited = null; let maxStep = 0; let lowFreed = false;
      try {
        for (let k = 0; k < 12; k++) {
          const today = addDays(T0, k);
          const tail = []; for (let j = 0; j < TAIL; j++) tail.push(await addOrder(c, today, ['S1']));
          await lockMore(LB2, tail);   // 別の書き手が持ち続ける = 窓の関数が skip → retry の末尾へ
          const before = await retryRows(c);
          const rep = await night(c, today, { nightCap: GN, retryBudget: GR, maxOrders: GO, maxLines: 100 });
          const after = await retryRows(c);
          for (const [id, a] of after) if (before.has(id)) maxStep = Math.max(maxStep, a - before.get(id));
          const cur = await cursorOf(c); const unLow = (await unresolvedOf(lowIds)).length;
          log.push({ k: k + 1, retry: rep.retryBatches, win: rep.windowBatches, started: rep.cycleStarted, done: rep.cycleCompleted, through: cur.cycle_through_order_id === null ? null : B(cur.cycle_through_order_id),
            cursor: B(cur.after_order_id), cycles: cur.cycles_completed, maxId: await maxRetryId(c), unLow, startedAt: cur.cycle_started_at });
          if (k === 1) { await LB1.query('rollback'); lowFreed = true; }   // 2 晩目の後 = 低い id 1〜16 はもう通った (lock のまま skip)
          if (lowFreed && revisited === null && unLow === 0) revisited = k + 1;
        }
      } finally { try { await LB1.query('rollback'); } catch { /* */ } await LB2.query('rollback'); }
      const lowMax = lowIds[lowIds.length - 1];
      assert.ok(log[0].started && log[0].through === lowMax && !log[1].started && log[1].through === lowMax && log[1].maxId > lowMax, 'G2 high-water は周回の間 変わらない');
      assert.ok(log[2].done && log[2].cursor === 0n && log[2].through === null && log[2].cycles === 1 && log[3].started && log[3].through === log[2].maxId, 'G3 high-water に着いた晩に周回を終え、次の晩に新しい周回');
      assert.ok(revisited !== null && revisited <= 5, `G4 低い id の再訪 = ${revisited}`);
      assert.ok((await exactlyOnce(lowIds)).ok, 'G4 ちょうど 1 回');
      const c2 = log.find((x) => x.cycles >= 2);
      assert.ok(c2 && c2.k - 3 <= Math.ceil(46 / (GR * GO)) && log[c2.k].started, `G5 2 周目の終わり = ${c2 && c2.k}`);
      assert.ok(maxStep <= 1, `G6 attempts の 1 晩の増分の最大 = ${maxStep}`);
      assert.ok(log.every((x) => x.win >= 1 && x.retry <= GR) && log[log.length - 1].maxId > log[0].maxId, 'G7');
      assert.ok(log.filter((x) => x.started).every((x, i, a) => i === 0 || x.startedAt > a[i - 1].startedAt), 'cycle_started_at は新しい周回の始めにだけ進む');
    });
    await t('空なら呼ばない: retry の表が空の晩は retry の関数を 1 回も呼ばず (確かめも batch も 0)・周回を始めない / 周回の残りが空なら呼ばずに周回の完了 → 次の晩に high-water より大きい行を新しい周回で', async () => {
      const calls = [];
      const spy = { query: (sql, p) => { if (/reresolve_order_lines_retry\(/.test(sql)) calls.push(sql); return O.query(sql, p); } };
      await addOrder(22, T0, ['S1']);
      const e1 = await runReresolveNight(spy, { company: 22, mall: MALL, ...NIGHT, nightCap: 5, retryBudget: 2, window: W(T0) });
      const c22 = await cursorOf(22);
      assert.deepEqual([calls.length, e1.retryBatches, e1.probes, e1.cycleStarted, c22.cycle_started_at, c22.cycle_through_order_id, e1.windowBatches], [0, 0, 0, false, null, null, 1]);
      const ids = [await addOrder(23, addDays(T0, -60), ['S1']), await addOrder(23, addDays(T0, -60), ['S1']), await addOrder(23, addDays(T0, -60), ['S1'])];
      await putRetry(23, [ids[2]]);
      await O.query(`insert into ops.reresolve_retry_cursor (company_id, mall, after_order_id, cycle_through_order_id, cycle_started_at) values (23, $1, $2, $3, now() - interval '1 day')`, [MALL, String(ids[0]), String(ids[1])]);
      const e2 = await runReresolveNight(spy, { company: 23, mall: MALL, ...NIGHT, nightCap: 5, retryBudget: 2, window: W(T0) });
      const c23a = await cursorOf(23);
      assert.deepEqual([calls.length, e2.retryBatches, e2.probes, e2.cycleCompleted, c23a.cycles_completed, c23a.cycle_through_order_id, c23a.after_order_id], [0, 0, 1, true, 1, null, '0']);
      const e3 = await runReresolveNight(spy, { company: 23, mall: MALL, ...NIGHT, nightCap: 5, retryBudget: 2, window: W(addDays(T0, 1)) });
      assert.deepEqual([calls.length, e3.cycleStarted, e3.retryBatches, (await retryRows(23)).size], [1, true, 1, 0]);
    });
    await t('cycle_started_at は新しい周回の始め (1・4 晩目) にだけ更新・周回の途中と終えた晩は変えない / 本体の取引が巻き戻ると cursor・high-water・cycle_started_at・cycles も戻り、予約の窓 (cursor 0) は残って次の晩に最初から', async () => {
      const c = 40; const ids = await addOrdersBulk(c, addDays(T0, -60), 10, () => 1);
      await putRetry(c, ids);
      await lockOrders(LB1, ids);
      const st = []; const reps = [];
      try {
        for (let k = 0; k < 4; k++) { reps.push(await night(c, addDays(T0, k), { nightCap: 3, retryBudget: 1, maxOrders: 4 })); st.push(await cursorOf(c)); await new Promise((r) => setTimeout(r, 5)); }
        const ts = st.map((x) => x.cycle_started_at.getTime());
        assert.ok(reps[0].cycleStarted && !reps[1].cycleStarted && !reps[2].cycleStarted && reps[2].cycleCompleted && reps[3].cycleStarted, JSON.stringify(reps.map((r) => [r.cycleStarted, r.cycleCompleted])));
        assert.ok(ts[0] === ts[1] && ts[1] === ts[2] && ts[3] > ts[2] && st[2].cycle_through_order_id === null && B(st[3].cycle_through_order_id) === ids[9]);
        const before = await cursorOf(c);
        const winIds = [await addOrder(c, addDays(T0, 9), ['S1']), await addOrder(c, addDays(T0, 9), ['S2'])];
        const f = await night(c, addDays(T0, 9), { nightCap: 3, retryBudget: 1, maxOrders: 4, failBody: true });
        const afterF = await cursorOf(c);
        assert.equal(f.committed, false);
        assert.deepEqual([afterF.after_order_id, afterF.cycle_through_order_id, afterF.cycle_started_at.getTime(), afterF.cycles_completed], [before.after_order_id, before.cycle_through_order_id, before.cycle_started_at.getTime(), before.cycles_completed]);
        assert.deepEqual(await unresolvedOf(winIds), winIds);
        const bl = await backlogOf(c);
        assert.ok(bl.some((x) => x.s === W(addDays(T0, 9)).since && x.after_order_id === '0'), JSON.stringify(bl));
        const ok2 = await night(c, addDays(T0, 9), { nightCap: 3, retryBudget: 1, maxOrders: 4 });
        assert.ok(ok2.committed);
        assert.deepEqual(await unresolvedOf(winIds), []);
        assert.ok((await exactlyOnce(winIds)).ok);
      } finally { await LB1.query('rollback'); }
    });
    await t('retry の予算 R ≧ 1 晩の上限 N (窓に 1 batch も残らない) と R < 1 は、予約も本体も流す前に止まる (表に何も書かない)', async () => {
      await ensureCompany(41);
      for (const [cap, rb] of [[5, 5], [5, 6], [5, 0], [1, 1]]) await assert.rejects(() => night(41, T0, { nightCap: cap, retryBudget: rb }), (e) => e.code === 'RERESOLVE_BAD_OPTIONS', `${cap}/${rb}`);
      await assert.rejects(() => night(41, T0, { maxOrders: 5001 }), (e) => e.code === 'RERESOLVE_BAD_OPTIONS');
      assert.deepEqual([(await backlogOf(41)).length, await cursorOf(41)], [0, undefined]);
      await assert.rejects(() => runNightBody(odb, { company: 41, mall: MALL, ...NIGHT }), (e) => e.code === 'RERESOLVE_NOT_RESERVED');
      assert.deepEqual(nightWindow(new Date('2026-10-09T17:00:00Z')), { since: addDays('2026-10-10', -35), until: '2026-10-11' });   // 02:00 JST = JST の今日
      assert.equal(RERESOLVE_NIGHT_DEFAULTS.retryBudget < RERESOLVE_NIGHT_DEFAULTS.nightCap, true);
    });

    console.log('F bigint の最大値と CHECK');
    await t('🚨 high-water が bigint の最大値 = p_before_order_id は null (+ 1 は溢れる)・JS は BigInt で精度を保つ・retry の関数と窓の関数は最大値の cursor でも動く (L1)', async () => {
      assert.equal(beforeOrderIdFor(PG_BIGINT_MAX), null);
      assert.equal(beforeOrderIdFor('9223372036854775807'), null);
      assert.equal(beforeOrderIdFor(9223372036854775806n), PG_BIGINT_MAX);
      assert.equal(beforeOrderIdFor('9007199254740993'), 9007199254740994n);   // 2^53 + 1 (Number なら 9007199254740992 になる)
      for (const bad of [0, '0', -1n, null, PG_BIGINT_MAX + 1n]) assert.throws(() => beforeOrderIdFor(bad), (e) => e.code === 'RERESOLVE_BAD_HIGH_WATER', String(bad));
      const c = 24;
      const m1 = await addOrder(c, addDays(T0, -60), ['S1'], { id: PG_BIGINT_MAX - 1n });
      const m = await addOrder(c, addDays(T0, -60), ['S2'], { id: PG_BIGINT_MAX });
      await putRetry(c, [m1, m]);
      const retryCalls = [];
      const spy = { query: (sql, prm) => { if (/reresolve_order_lines_retry\(/.test(sql)) retryCalls.push(prm); return O.query(sql, prm); } };
      const nightSpy = (today, o) => runReresolveNight(spy, { company: c, mall: MALL, ...NIGHT, window: W(today), nightCap: 5, ...o });
      await lockOrders(LB1, [m1, m]);
      let nA, nB, curA, curB;
      try {
        nA = await nightSpy(T0, { retryBudget: 1, maxOrders: 1 });   // 新しい周回 (high-water = 最大値)・1 batch で m1 を skip → 予算で止まる
        curA = await cursorOf(c);
        nB = await nightSpy(addDays(T0, 1), { retryBudget: 1, maxOrders: 1 });   // cursor = 最大値 − 1 から・p_before = null で m を読む → 周回の完了
        curB = await cursorOf(c);
      } finally { await LB1.query('rollback'); }
      assert.deepEqual([nA.cycleStarted, nA.cycleCompleted, nA.retryBatches, nA.skippedInRetry], [true, false, 1, [m1]]);
      assert.deepEqual([curA.cycle_through_order_id, curA.after_order_id], [PG_BIGINT_MAX.toString(), (PG_BIGINT_MAX - 1n).toString()]);   // 文字のまま = 精度を落としていない
      assert.deepEqual([nB.cycleStarted, nB.cycleCompleted, nB.retryBatches, nB.skippedInRetry], [false, true, 1, [m]]);
      assert.deepEqual([curB.cycle_through_order_id, curB.after_order_id, curB.cycles_completed], [null, '0', 1]);
      assert.deepEqual(retryCalls.map((x) => [x[2], x[3]]), [['0', null], [(PG_BIGINT_MAX - 1n).toString(), null]]);   // high-water が最大値 = p_before_order_id は null
      assert.deepEqual([...(await retryRows(c))].sort(), [[String(m1), 1], [String(m), 1]].sort());
      const nC = await nightSpy(addDays(T0, 2), { retryBudget: 2, maxOrders: 1 });
      assert.deepEqual([nC.cycleStarted, nC.retryBatches, nC.cycleCompleted, (await retryRows(c)).size], [true, 2, true, 0]);
      assert.ok((await exactlyOnce([m1, m])).ok);
      const r = await win(O, c, addDays(T0, -61), addDays(T0, -59), PG_BIGINT_MAX, 10, 10);
      assert.deepEqual([r.orders_examined, r.stop_reason, B(r.next_after_order_id)], [0, 'done', PG_BIGINT_MAX]);
      const r2 = await retryFn(O, c, PG_BIGINT_MAX - 1n, null, 10, 10);
      assert.deepEqual([r2.orders_examined, B(r2.next_after_order_id)], [0, PG_BIGINT_MAX - 1n]);
      assert.equal(await codeOf(O, `update ops.reresolve_retry_cursor set after_order_id = $2::bigint, cycle_through_order_id = $2::bigint, cycle_started_at = now() where company_id = $1`, [c, PG_BIGINT_MAX.toString()]), 'ok');
    });
    await t('🚨 CHECK: cursor の行は high-water があれば cycle_started_at が要る (L2)・周回の外なら cursor 0・cursor ≦ high-water / 持ち越しの窓は 1〜62 日 / attempts ≧ 0', async () => {
      await ensureCompany(25);
      const ins = `insert into ops.reresolve_retry_cursor (company_id, mall, after_order_id, cycle_through_order_id, cycle_started_at) values (25, '${MALL}', $1::bigint, $2::bigint, $3::timestamptz)`;
      assert.equal(await codeOf(O, ins, ['0', '10', null]), '23514', 'high-water あり・cycle_started_at null');
      assert.equal(await codeOf(O, ins, ['5', null, null]), '23514', '周回の外で cursor ≠ 0');
      assert.equal(await codeOf(O, ins, ['11', '10', new Date().toISOString()]), '23514', 'cursor > high-water');
      assert.equal(await codeOf(O, ins, ['0', null, null]), 'ok');
      assert.equal(await codeOf(O, ins, ['10', '10', new Date().toISOString()]), 'ok');
      const cons = (await O.query(`select conname from pg_constraint where conrelid = 'ops.reresolve_retry_cursor'::regclass and contype = 'c' order by 1`)).rows.map((r) => r.conname);
      assert.ok(cons.includes('ck_reresolve_cursor_cycle_started'), cons.join(','));
      const bw = `insert into ops.reresolve_backlog_windows (company_id, mall, p_since, p_until) values (25, '${MALL}', $1::date, $2::date)`;
      assert.equal(await codeOf(O, bw, [T0, addDays(T0, 63)]), '23514');
      assert.equal(await codeOf(O, bw, [T0, T0]), '23514');
      assert.equal(await codeOf(O, bw, [T0, addDays(T0, 62)]), 'ok');
      assert.equal(await codeOf(O, `insert into ops.reresolve_retry_orders (company_id, mall, order_id, attempts) values (25, '${MALL}', 1, -1)`), '23514');
    });

    console.log('G 1 周の見込みと ⚠️');
    await t('🚨 1 注文 500 明細 × 300 注文 (既定の上限・R 5): 式の見込み 4 晩 ≧ 実際 3 晩 (旧式 ⌈Q ÷ (R × 2000)⌉ = 1 は過小)・実測 e = 20 で 3 晩・R 1 なら 16 晩 > 7 で ⚠️', async () => {
      const c = 26; const ids = await addOrdersBulk(c, addDays(T0, -60), 300, () => 500);
      await putRetry(c, ids);
      const st = await readRetryState(O, { company: c, mall: MALL });
      const est5 = estimateCycleNights({ ...st, R: 5, maxOrders: 2000, maxLines: 10000 });
      const est1 = estimateCycleNights({ ...st, R: 1, maxOrders: 2000, maxLines: 10000 });
      const nights = []; let n = 0;
      while (n < 10) { n++; const r = await night(c, addDays(T0, n), { nightCap: 20, retryBudget: 5, maxOrders: 2000, maxLines: 10000 }); nights.push(r); if (r.cycleCompleted) break; }
      const batches = nights.flatMap((x) => x.retryBatchLog);
      assert.deepEqual([st.Q, st.L, st.Lmax], [300, 150000, 500]);
      assert.equal(n, 3);
      assert.ok(est5.formula >= n && est5.worst >= n && Math.ceil(300 / (5 * 2000)) < n, JSON.stringify(est5));
      const e = Math.min(...batches.filter((b) => b.hasMore).map((b) => b.examined));
      const measured = estimateCycleNights({ Q: 300, L: 150000, Lmax: 500, R: 5, maxOrders: 2000, maxLines: 10000, recentBatches: batches });
      assert.deepEqual([e, measured.e, measured.measured], [20, 20, 3]);
      const w1 = retryWarnings({ att7: 0, old7: 0, cycleOld: false }, est1);
      assert.ok(est1.formula > 7 && w1.some((x) => x.kind === 'cycle_nights'), JSON.stringify(est1));
      assert.ok((await exactlyOnce(ids)).ok);
      assert.equal((await readRetryState(O, { company: c, mall: MALL })).Q, 0);
    });
    await t('明細の数がばらばら (1〜500) の queue で、4 つの上限の組のどれでも式の B (batch の数の見込み) ≧ 実際の batch の数 (p_max_lines < 1 注文の最大でも B = Q)', async () => {
      const c = 27; const nOf = (i) => 1 + (((i + 1) * 37) % 500);
      const ids = await addOrdersBulk(c, addDays(T0, -60), 60, nOf);
      await putRetry(c, ids);
      const st = await readRetryState(O, { company: c, mall: MALL });
      const checks = [];
      for (const [po, pl] of [[7, 1200], [2000, 10000], [3, 600], [50, 300]]) {
        await O.query('begin');
        let after = 0n; let b = 0; for (;;) { const r = await retryFn(O, c, after, null, po, pl); b++; after = B(r.next_after_order_id); if (!r.has_more) break; }
        await O.query('rollback');
        const est = estimateCycleNights({ ...st, R: 1, maxOrders: po, maxLines: pl });
        checks.push({ po, pl, actual: b, B: est.B });
      }
      assert.ok(checks.every((x) => x.B >= x.actual), JSON.stringify(checks));
      assert.equal(checks.find((x) => x.pl === 300).B, 60);
    });
    await t('⚠️ の 4 つ: ① 見込み > 7 晩 ② attempts ≧ 7 ③ first_skipped_at < 7 日前 (attempts 0 でも・3 でも) ④ 周回の途中で cycle_started_at < 7 日前 (周回の外では出さない) / 対照は出ない / 報告の 1 行', async () => {
      const mk = async (c, attempts, firstAgo) => { const id = await addOrder(c, addDays(T0, -60), ['NX']); await putRetry(c, [id], { attempts, firstAgo }); return id; };
      await mk(31, 0, '8 days'); await mk(32, 3, '7 days 1 hour'); await mk(33, 7, '1 minute'); const id34 = await mk(34, 6, '6 days');
      const opt = { R: 5, maxOrders: 2000, maxLines: 10000 };
      const w = {};
      for (const c of [31, 32, 33, 34]) { const s = await readRetryState(O, { company: c, mall: MALL }); w[c] = retryWarnings(s, estimateCycleNights({ ...s, ...opt })).map((x) => x.kind); }
      assert.deepEqual(w[31], ['first_skipped']); assert.deepEqual(w[32], ['first_skipped']); assert.deepEqual(w[33], ['attempts']); assert.deepEqual(w[34], []);
      await O.query(`insert into ops.reresolve_retry_cursor (company_id, mall, after_order_id, cycle_through_order_id, cycle_started_at) values (34, $1, 0, $2, now() - interval '8 days')`, [MALL, String(id34)]);
      const s34 = await readRetryState(O, { company: 34, mall: MALL });
      assert.deepEqual(retryWarnings(s34, estimateCycleNights({ ...s34, ...opt })).map((x) => x.kind), ['cycle_started']);
      const line = retryReportLine(MALL, s34, estimateCycleNights({ ...s34, ...opt }), retryWarnings(s34, estimateCycleNights({ ...s34, ...opt })));
      assert.match(line, /retry の表の残り 1 注文/); assert.match(line, /high-water/); assert.match(line, /⚠️/);
      await O.query(`update ops.reresolve_retry_cursor set cycle_through_order_id = null where company_id = 34`);
      const s34b = await readRetryState(O, { company: 34, mall: MALL });
      assert.deepEqual(retryWarnings(s34b, estimateCycleNights({ ...s34b, ...opt })), []);
      assert.deepEqual(retryWarnings({ att7: 0, old7: 0, cycleOld: false }, estimateCycleNights({ Q: 0, L: 0, Lmax: 0, ...opt })), []);
    });

    console.log('H 権限・一時の表・engine・manifest・バックアップ');
    await t('権限: 3 つの関数は PUBLIC・watcher から呼べない (42501)・持ち主は呼べる / watcher は 3 つの表を SELECT だけ / 旧い 3 引数の権限は変えていない / search_path と work_mem の固定', async () => {
      const sigs = ['core.reresolve_order_lines(smallint, text, date, date, bigint, integer, integer)', 'core.reresolve_order_lines_retry(smallint, text, bigint, bigint, integer, integer)', 'core._reresolve_order_batch(smallint, text, bigint[], boolean)'];
      for (const s of sigs) {
        const r = (await O.query(`select has_function_privilege('public', $1::regprocedure, 'execute') as pub, has_function_privilege('watcher', $1::regprocedure, 'execute') as w, has_function_privilege(current_user, $1::regprocedure, 'execute') as own, array_to_string(proconfig, ',') as cfg, prosecdef from pg_proc where oid = $1::regprocedure`, [s])).rows[0];
        assert.deepEqual([r.pub, r.w, r.own, r.prosecdef], [false, false, true, false], s);
        assert.match(r.cfg, /search_path=pg_catalog, pg_temp/); assert.match(r.cfg, /work_mem=4MB/);
      }
      assert.equal(await codeOf(P, `select * from core.reresolve_order_lines(11::smallint, '${MALL}', '${T0}'::date, '${addDays(T0, 1)}'::date)`), '42501');
      assert.equal(await codeOf(W1, `select * from core.reresolve_order_lines_retry(11::smallint, '${MALL}')`), '42501');
      assert.equal((await O.query(`select has_function_privilege('public', 'core.reresolve_order_lines(smallint, text, date)'::regprocedure, 'execute') as x`)).rows[0].x, true);
      for (const tb of ['ops.reresolve_retry_orders', 'ops.reresolve_backlog_windows', 'ops.reresolve_retry_cursor']) {
        const r = (await O.query(`select has_table_privilege('watcher', $1, 'select') as s, has_table_privilege('watcher', $1, 'insert,update,delete,truncate') as w, has_table_privilege($2, $1, 'select') as ps`, [tb, PROBE])).rows[0];
        assert.deepEqual([r.s, r.w, r.ps], [true, false, false], tb);
        assert.equal((await W1.query(`select count(*)::int as n from ${tb}`)).rows[0].n >= 0, true);
      }
    });
    await t('🚨 一時の表を使わない: 本体に create temp が無い / TEMP の権限の無い役割で新しい窓・retry の関数は動き、旧い 3 引数 (と engine の文) は 42501 (temporary tables)', async () => {
      const audit = await tempPrivilegeAudit(odb);
      assert.ok(audit.tempFunctions.includes('core.reresolve_order_lines(p_company smallint, p_mall text, p_since date)') || audit.tempFunctions.some((x) => /^core\.reresolve_order_lines\(p_company smallint, p_mall text, p_since date\)/.test(x)), audit.tempFunctions.join(' / '));
      assert.ok(!audit.tempFunctions.some((x) => /reresolve_order_lines_retry|_reresolve_order_batch|p_until/.test(x)), audit.tempFunctions.join(' / '));
      await O.query(`revoke temp on database ${DB1} from public`);
      try {
        await O.query(`grant select, update on core.orders, core.order_lines to ${NOTEMP}; grant select on core.listings, core.external_ids to ${NOTEMP};
          grant select, insert, update, delete on ops.reresolve_retry_orders, ops.reresolve_backlog_windows, ops.reresolve_retry_cursor to ${NOTEMP};
          grant usage on schema t to ${NOTEMP}; grant insert on t.touch_log to ${NOTEMP};
          grant execute on function core.reresolve_order_lines(smallint, text, date, date, bigint, integer, integer), core.reresolve_order_lines_retry(smallint, text, bigint, bigint, integer, integer),
            core._reresolve_order_batch(smallint, text, bigint[], boolean) to ${NOTEMP}`);
        assert.equal((await N.query(`select has_database_privilege(current_user, current_database(), 'temp') as x`)).rows[0].x, false);
        const ids = [await addOrder(28, T0, ['S1']), await addOrder(28, T0, ['S2', 'NX'])];
        const r = await win(N, 28, T0, addDays(T0, 1), 0n, 100, 100);
        assert.deepEqual([r.orders_examined, r.resolved, r.orders_touched], [2, 2, 2]);
        await putRetry(28, [ids[1]]);
        const r2 = await retryFn(N, 28, 0n, null, 100, 100);
        assert.deepEqual([r2.orders_examined, r2.candidates], [1, 1]);
        const engineSql = 'select candidates, resolved, orders_touched, orders_skipped_locked from core.reresolve_order_lines($1::smallint, $2, $3::date)';
        await N.query('begin');
        let err = null; try { await N.query(engineSql, [28, MALL, T0]); } catch (e) { err = e; } finally { await N.query('rollback'); }
        assert.ok(err && err.code === '42501' && /temporary/i.test(err.message), err && err.message);
      } finally { await O.query(`grant temp on database ${DB1} to public`); }
    });
    await t('🚨 engine (夜間ロード 8b) は旧い 3 引数のまま (feature detect も呼ぶ文も)・この PR の部品を読み込まない / engine の文は新しい署名があっても旧い関数に解決する', async () => {
      const src = fs.readFileSync(path.join(ROOT, 'apps/company-db/load/engine.mjs'), 'utf8');
      assert.ok(src.includes("to_regprocedure('core.reresolve_order_lines(smallint, text, date)')"));
      assert.ok(src.includes('core.reresolve_order_lines($1::smallint, $2, $3::date)'));
      assert.ok(!/reresolve_order_lines_retry|reresolve-batch|reresolve_backlog_windows|reresolve_retry/.test(src));
      const oid = (await O.query(`select 'core.reresolve_order_lines(smallint, text, date)'::regprocedure::oid::text as o`)).rows[0].o;
      const n = (await O.query(`select count(*)::int as n from pg_proc p join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'core' and p.proname = 'reresolve_order_lines'`)).rows[0].n;
      assert.equal(n, 2);
      await O.query('begin');
      try {
        const r = (await O.query('select candidates, resolved, orders_touched, orders_skipped_locked from core.reresolve_order_lines($1::smallint, $2, $3::date)', [11, MALL, T0])).rows[0];
        assert.deepEqual(Object.keys(r), ['candidates', 'resolved', 'orders_touched', 'orders_skipped_locked']);
      } finally { await O.query('rollback'); }
      assert.ok(oid);
    });
    await t('manifest: 規則にかかる関数は全部分けてある (新しい署名は guard_later・PUBLIC なし)・問題 0 件', async () => {
      assert.deepEqual(await heavyEntryFindings(odb), []);
      // manifest の migration = その関数を作る migration のファイルの番号 (番号を付け替えたら manifest も = 付け忘れると --verify が適用の前後で誤る)
      const dir = path.join(ROOT, 'db/company/migrations');
      const fileOf = (re) => fs.readdirSync(dir).filter((f) => /^\d{4}_.*\.sql$/.test(f) && re.test(fs.readFileSync(path.join(dir, f), 'utf8'))).map((f) => f.slice(0, 4));
      for (const [sig, re] of [['core.reresolve_order_lines(smallint, text, date, date, bigint, integer, integer)', /create function core\.reresolve_order_lines\(p_company smallint, p_mall text, p_since date, p_until date/],
        ['core.reresolve_order_lines_retry(smallint, text, bigint, bigint, integer, integer)', /create function core\.reresolve_order_lines_retry\(/],
        ['core._reresolve_order_batch(smallint, text, bigint[], boolean)', /create function core\._reresolve_order_batch\(/]]) {
        const e = HEAVY_ENTRY_MANIFEST.find((x) => x.sig === sig);
        assert.deepEqual([e.cls, e.public, [e.migration]], ['guard_later', false, fileOf(re)], sig);
      }
      // --verify (本適用の確かめ) は DB の適用済みの版で見る = 0057 の前の DB では新しい行を見ない (❌ にしない)・後では見る
      const x = new URL(url); x.username = OWNER; x.password = PW; x.pathname = `/${DB1}`;
      const v = spawnSync(process.execPath, ['scripts/company-db/heavy-entry-manifest.mjs', '--verify'], { cwd: ROOT, env: { ...process.env, COMPANY_DB_URL: x.toString() }, encoding: 'utf8', timeout: 60000 });
      assert.equal(v.status, 0, v.stdout + v.stderr); assert.ok(!/見ていない/.test(v.stdout), v.stdout);
    });
    await t('バックアップ (dump.mjs): 3 つの表は取って戻す (restore)・sequence なし・同じ DB へ dump → 中身を変える → restore で dump の時点の行に戻る (CHECK を満たしたまま)', async () => {
      const O2 = await open(OWNER, DB2); const db2 = pgAdapter(O2);
      await applyMigrations(db2, { log: quiet });
      const tables = await listTables(db2);
      const mine = tables.filter((x) => /reresolve_/.test(x.qualified)).map((x) => x.qualified).sort();
      assert.deepEqual(mine, ['"ops"."reresolve_backlog_windows"', '"ops"."reresolve_retry_cursor"', '"ops"."reresolve_retry_orders"']);
      const seqs = await listSequences(db2, tables);
      assert.ok(!seqs.some((s) => /reresolve/.test(s.sequence || s.table || '')), JSON.stringify(seqs.filter((s) => /reresolve/.test(JSON.stringify(s)))));
      await O2.query(`insert into ops.reresolve_retry_orders (company_id, mall, order_id, attempts) values (1, 'amazon', 5, 2), (1, 'amazon', 9, 0);
        insert into ops.reresolve_backlog_windows (company_id, mall, p_since, p_until, after_order_id) values (1, 'amazon', '2026-09-01', '2026-10-07', 5);
        insert into ops.reresolve_retry_cursor (company_id, mall, after_order_id, cycle_through_order_id, cycle_started_at, cycles_completed) values (1, 'amazon', 5, 9, now(), 3)`);
      const lines = []; await dumpCompanyDb(db2, (l) => lines.push(l), { log: quiet });
      const before = (await O2.query(`select (select json_agg(r order by order_id) from ops.reresolve_retry_orders r)::text as a, (select json_agg(w) from ops.reresolve_backlog_windows w)::text as b, (select json_agg(c) from ops.reresolve_retry_cursor c)::text as c`)).rows[0];
      await O2.query(`delete from ops.reresolve_retry_orders; update ops.reresolve_retry_cursor set after_order_id = 0, cycle_through_order_id = null; delete from ops.reresolve_backlog_windows`);
      await restoreCompanyDb(db2, lines.join('\n'), { log: quiet });
      const after = (await O2.query(`select (select json_agg(r order by order_id) from ops.reresolve_retry_orders r)::text as a, (select json_agg(w) from ops.reresolve_backlog_windows w)::text as b, (select json_agg(c) from ops.reresolve_retry_cursor c)::text as c`)).rows[0];
      assert.deepEqual(after, before);
    });
  } finally { await su.end().catch(() => {}); }
} catch (e) {
  setupError = e;
  console.error('❌ 準備か試験の外で落ちた (飛ばさない): ' + (e.stack || e.message));
} finally {
  for (const c of clients) { try { await Promise.race([c.end(), new Promise((r) => setTimeout(r, 2000))]); } catch { /* */ } }
  await stopCluster();
}
console.log(`\n${ok} ok / ${ng} NG${setupError ? ' / 準備で落ちた' : ''}${cleanupFailed ? ' / クラスタのフォルダが残った' : ''}`);
process.exit(ng || setupError || cleanupFailed ? 1 : 0);
