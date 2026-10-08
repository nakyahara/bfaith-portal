#!/usr/bin/env node
/**
 * test-company-db-profit-fn-revoke-pg.mjs — 0056 (D-60 の重い関数の権限の封鎖) を本物の PostgreSQL で確かめる
 *
 * 持ち主は本番と同じ形 = **superuser でない** login の役割 (CREATEROLE あり = Render の default user) で、その役割が DB を持ち、全部の migration を流す。
 * 固定する契約:
 *   1 0055 までの姿では、持ち主・watcher・PUBLIC だけの役割が重い関数に入れる (前提)
 *   2 0056 の後は、持ち主・watcher・profit_reader・PUBLIC だけの役割の全部が、10 の関数で 42501 (permission denied for function)。
 *     guard_later・light の権限は前と同じ・持ち主は移していない・TEMP の権限は監査を出すだけで同じ (PR 1a)
 *   3 正当な道は今までどおり: 財務の chunk の取込 → coverage の updating → complete (持ち主) → core.finance_coverage_state が complete_to を返す (持ち主・watcher)・
 *     mart.finance_daily_range (受け口 GET /order-finance/daily) が行を返す・月のそろい
 *   4 SECURITY DEFINER の関数 (持ち主が定義者) の中から呼んでも 42501 (定義者 = 持ち主の EXECUTE も無い = 抜け道にならない)
 *   5 閉じた関数の create or replace は持ち主でも 42501 (関数の検査) → 同じ取引で自分に GRANT → 作り直し → 外す の約束なら通り、権限の表は空に戻る
 *   6 0056 をもう一度流しても同じ・実行器は 0 本
 *   7 持ち主でない役割で 0056 を流すと止まる (d60_revoke_incomplete・何も変わらない) = 黙って「外したつもり」にならない
 * 使い方: node scripts/test-company-db-profit-fn-revoke-pg.mjs   (npm run test:company-db にも入っている = 飛ばさない)
 *   🚨 **試験が自分で使い捨てのクラスタを起動する** (#1601 Codex R1 M2): embedded-postgres で OS の一時フォルダに新しいクラスタを作り、ランダムのポートで起動し、
 *      最後に止めて消す。外の PostgreSQL (TEST_PG_URL など) には一切つながない。起動したクラスタに watcher / profit_reader が既にあれば止まる (作り直さない・password を変えない)。
 *   embedded-postgres は devDependencies に正確な版で入っている (lockfile で固定・#1601 Codex R2 M。Render の npm ci --production には入らない)。探す順:
 *      リポジトリの node_modules → 版が同じときだけ 環境変数 EMBEDDED_PG_DIR / C:/tmp/pg-embed (node_modules をジャンクションにした worktree のため)。
 *      **見つからない・版が違う・起動できない・クラスタのフォルダが消えない = 失敗 (exit 1)**
 */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { openPgClient, pgAdapter, applyMigrations, DEFAULT_DIR } from './company-db/migrate.mjs';
import { HEAVY_ENTRY_MANIFEST, revokeSigs, heavyEntryFindings, tempPrivilegeAudit } from './company-db/heavy-entry-manifest.mjs';
import { validateFinanceRows, orderFinanceChecksum } from '../apps/company-db/finance/order-finance-checksum.mjs';
import { receiptDigest } from '../apps/company-db/finance/coverage-manifest.mjs';
import { applyCoverage } from '../apps/company-db/ingest/finance-coverage.mjs';
import { validateFinanceChunk, ingestOrderFinanceChunk } from '../apps/company-db/ingest/order-finance.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

/** 使う embedded-postgres の版 = package.json の devDependencies の正確な版 (lockfile で固定・#1601 Codex R2 M) */
const PINNED_EMBEDDED_PG = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).devDependencies['embedded-postgres'];
/**
 * embedded-postgres を探す。① リポジトリの node_modules (npm ci で入る = 正) ② 外の置き場 (EMBEDDED_PG_DIR → C:/tmp/pg-embed) は **版が PINNED と同じときだけ**
 *   (node_modules を別の作業の木へのジャンクションにしている worktree で、npm ci をし直さずに流すため。違う版は使わない = 同じコミットで同じ版)。無ければ { why }
 */
async function loadEmbeddedPostgres() {
  const bases = [path.join(ROOT, 'package.json'), ...(process.env.EMBEDDED_PG_DIR ? [path.join(process.env.EMBEDDED_PG_DIR, 'package.json')] : []), 'C:/tmp/pg-embed/package.json'];
  const seen = [];
  for (const b of bases) {
    let main, ver;
    try { const req = createRequire(b); main = req.resolve('embedded-postgres'); let d = path.dirname(main); while (path.basename(d) !== 'embedded-postgres' && path.dirname(d) !== d) d = path.dirname(d); ver = JSON.parse(fs.readFileSync(path.join(d, 'package.json'), 'utf8')).version; /* exports が package.json を出さないので main から上へ */ } catch { continue; }
    if (ver !== PINNED_EMBEDDED_PG) { seen.push(path.dirname(b) + ' = ' + ver); continue; }
    return { EmbeddedPostgres: (await import(pathToFileURL(main).href)).default, from: path.dirname(b) };
  }
  return { why: seen.length ? '版が ' + PINNED_EMBEDDED_PG + ' でない (' + seen.join(' / ') + ')' : '見つからない' };
}
const loaded = await loadEmbeddedPostgres();
if (!loaded.EmbeddedPostgres) {
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の権限の試験を流せない (飛ばさない)。リポジトリで npm ci (devDependencies に入っている)');
  process.exit(1);
}
const { EmbeddedPostgres } = loaded;
const clusterDir = path.join(os.tmpdir(), `cdb-d60-pg-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
// persistent: false = 止めるときに embedded-postgres がフォルダを消す。残れば自分で消し、それでも残れば試験を失敗にする (#1601 Codex R2 Low 1)
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
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const H = (s) => crypto.createHash('sha256').update(s).digest('hex');
const CLOSED_FUNCTIONS = revokeSigs();
// 🆕 0066: 0056 より後の migration で足した関数 (since) は、この試験 (0055 → 0056) の対象の外
const KEEP = HEAVY_ENTRY_MANIFEST.filter((e) => e.cls !== 'revoke' && !(e.since && e.since > '0056')).map((e) => e.sig);
const FILE_0056 = path.join(DEFAULT_DIR, '0056_amazon_profit_fn_revoke.sql');
const SQL_0056 = fs.readFileSync(FILE_0056, 'utf8');

const hex = crypto.randomBytes(4).toString('hex');
const OWNER = `cdb_d60o_${hex}`, PROBE = `cdb_d60p_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const DB1 = `cdb_d60_${hex}`, DB2 = `cdb_d60b_${hex}`;
// 起動の後の全部 (役割・DB・接続の準備・試験) を外側の try/finally で覆い、どこで落ちても必ずクラスタを止めて消す (#1601 Codex R2 Low 1)
const clients = [];
let setupError = null;
try {
  await cluster.initialise();
  await cluster.start();
const su = await openPgClient(url);
try {
// 🚨 新しいクラスタのはず = watcher / profit_reader が既にあれば止まる (既存の役割の password・LOGIN を変えない。#1601 Codex R1 M2)
const existing = (await su.query(`select rolname from pg_roles where rolname in ('watcher', 'profit_reader') order by 1`)).rows.map((r) => r.rolname);
if (existing.length) throw new Error('使い捨てのクラスタのはずが ' + existing.join(', ') + ' が既にある = 止める (役割を変えない)');
await su.query(`create role ${OWNER} login createrole password '${PW}'`);   // Render の default user と同じ: superuser でない・CREATEROLE
await su.query(`create role ${PROBE} login password '${PW}'`);              // PUBLIC の権限だけの役割
await su.query(`create role watcher login password '${PW}'`);
await su.query(`create role profit_reader login password '${PW}'`);
assert.equal((await su.query(`select rolsuper from pg_roles where rolname = $1`, [OWNER])).rows[0].rolsuper, false);
await su.query(`create database ${DB1} owner ${OWNER}`);
await su.query(`create database ${DB2} owner ${OWNER}`);
const open = async (role, dbName) => { const x = new URL(url); x.username = role; x.password = PW; x.pathname = `/${dbName}`; const c = await openPgClient(x.toString()); c.on('error', () => {}); return c; };
const O = await open(OWNER, DB1), W = await open('watcher', DB1), P = await open(PROBE, DB1), R = await open('profit_reader', DB1);
clients.push(O, W, P, R);
const odb = pgAdapter(O);

/** 0055 までを流す (0056 の前 = 今の本番の姿) */
const migrateUpTo0055 = async (db) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-d60pg-'));
  for (const f of fs.readdirSync(DEFAULT_DIR)) if (/^\d{4}_.*\.sql$/.test(f) && f < '0056') fs.copyFileSync(path.join(DEFAULT_DIR, f), path.join(dir, f));
  try { return await applyMigrations(db, { dir, log: quiet }); } finally { fs.rmSync(dir, { recursive: true, force: true }); }
};
/** 引数を全部 null (型つき) にして呼ぶ SQL */
const callSql = async (sig) => {
  const r = (await O.query(`select n.nspname as s, p.proname as n, coalesce((select string_agg('null::' || format_type(t, null), ', ' order by i) from unnest(p.proargtypes::oid[]) with ordinality u(t, i)), '') as a
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.oid = $1::regprocedure`, [sig])).rows[0];
  return `select ${r.s}.${r.n}(${r.a})`;
};
/** その接続で呼ぶ (取引の中・必ず rollback)。戻り = 'ok' / 'fn_denied' (関数の EXECUTE が無い) / それ以外は SQLSTATE */
const callAs = async (c, sqlOrSig) => {
  const sql = /^select /i.test(sqlOrSig) ? sqlOrSig : await callSql(sqlOrSig);
  await c.query('begin');
  try { await c.query(`set local statement_timeout = '20s'`); await c.query(sql); return 'ok'; }
  catch (e) { return e.code === '42501' && /permission denied for function/.test(e.message) ? 'fn_denied' : (e.code || 'error'); }
  finally { await c.query('rollback'); }
};
const keepBefore = {};
let tempBefore = null;
const aclOf = async (c, sig) => (await c.query(`select proacl::text as acl, proacl is null as dflt, coalesce(cardinality(proacl), -1) as n from pg_proc where oid = $1::regprocedure`, [sig])).rows[0];

{   // 試験 (外側の finally が接続とクラスタを片づける)
  await migrateUpTo0055(odb);
  // create-watch-roles.mjs と同じ: watcher は schema の USAGE と表の SELECT。PUBLIC だけの役割は schema の USAGE だけ
  await O.query(`grant usage on schema core, mart to watcher, profit_reader, ${PROBE}; grant select on all tables in schema core, mart to watcher;
    grant execute on function mart.amazon_profit_daily_range(smallint, text, text, date, date) to profit_reader`);   // profit_reader はまだ本番に無い = あれば外すことを見る
  for (const s of KEEP) keepBefore[s] = await aclOf(O, s);
  tempBefore = await tempPrivilegeAudit(odb);

  console.log('0055 まで (前提 = 今の本番の姿・持ち主は superuser でない)');
  await t('持ち主・watcher は公開の関数に入れる (引数の確かめ 22023 で止まる)・watcher は finance_daily_sku_range・PUBLIC だけの役割は内部の部品に入れる', async () => {
    assert.equal(await callAs(O, 'mart.amazon_profit_daily_range(smallint, text, text, date, date)'), '22023');
    assert.equal(await callAs(W, 'mart.amazon_profit_day_totals_range(smallint, text, text, date, date)'), '22023');
    assert.equal(await callAs(R, 'mart.amazon_profit_daily_range(smallint, text, text, date, date)'), '22023');
    assert.notEqual(await callAs(W, 'mart.finance_daily_sku_range(smallint, text, text, date, date)'), 'fn_denied');
    assert.notEqual(await callAs(P, 'mart._amazon_profit_ad_days(smallint, text, text, date, date)'), 'fn_denied');
    assert.ok((await heavyEntryFindings(odb)).length > 0);
  });

  console.log('0056 (持ち主が実行器で流す)');
  await t('実行器で流れる (0056 だけ・superuser でない持ち主で)', async () => {
    assert.deepEqual((await applyMigrations(odb, { log: quiet, to: '0056' })).applied, ['0056']);   // 0056 まで (後の migration は対象の外)
  });
  await t('🚨 持ち主・watcher・profit_reader・PUBLIC だけの役割の全部が、10 の関数で 42501 (permission denied for function)', async () => {
    assert.equal(CLOSED_FUNCTIONS.length, 10);
    for (const s of CLOSED_FUNCTIONS) for (const [who, c] of [['owner', O], ['watcher', W], ['profit_reader', R], ['public', P]]) assert.equal(await callAs(c, s), 'fn_denied', `${who}: ${s}`);
  });
  await t('🚨 guard_later・light の関数の権限の表は 0056 の前と同じ・持ち主は変わらない (移していない)・TEMP の権限は監査を出すだけで同じ', async () => {
    for (const s of KEEP) assert.deepEqual(await aclOf(O, s), keepBefore[s], s);
    const owners = (await O.query(`select distinct pg_get_userbyid(proowner) as o from pg_proc where oid = any($1::regprocedure[])`, [HEAVY_ENTRY_MANIFEST.filter((e) => e.sig.startsWith('mart.')).map((e) => e.sig)])).rows.map((r) => r.o);
    assert.deepEqual(owners, [OWNER]);
    const a = await tempPrivilegeAudit(odb);
    assert.deepEqual(a, tempBefore);
    assert.equal(a.publicTemp, true);
    assert.ok(a.roles.some((r) => r.rolname === OWNER && r.temp));
  });
  await t('🚨 権限の表が空・superuser でない役割は全部 false (持ち主も)・確かめの関数の問題は 0 件', async () => {
    for (const s of CLOSED_FUNCTIONS) { const a = await aclOf(O, s); assert.deepEqual([s, a.dflt, Number(a.n)], [s, false, 0]); }
    assert.deepEqual(await heavyEntryFindings(odb), []);
    assert.equal((await O.query(`select has_function_privilege(current_user, 'mart.finance_daily_sku_range(smallint, text, text, date, date)'::regprocedure, 'execute') as x`)).rows[0].x, false);
  });

  console.log('正当な道 (今までどおり)');
  await t('財務の chunk → coverage の updating → complete (持ち主) → finance_coverage_state が complete_to (持ち主・watcher)・finance_daily_range が行を返す・月のそろい', async () => {
    const U = 'amazon_settlement_unified', V2 = 'amazon_finance_v2', TOK = 'tok-d60aaaaaaaaaaaaa';
    const C4 = { unclassified_component_count: 0, unclassified_mapped_jpy: 0, unclassified_abs_jpy: 0, unmapped_component_count: 0 };
    const lines = [{ economic_date_jst: '2026-06-05', seller_sku: 'LA', line_kind: 'sku', source: U, source_lines: 1, source_updated_at: '2026-06-05T00:00:00Z', content_hash: 'h', ...C4,
      units_ordered: 1, sales_principal_jpy: 1000, commission_jpy: -100 }];
    const setChecksum = orderFinanceChecksum(validateFinanceRows('A-1', lines));
    const c = validateFinanceChunk({ run_id: 'ship_202610030000001_abcdef', batch_seq: 1, chunk_index: 0, last: true, transform_version: V2,
      rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'A-1', header: { transform_version: V2, set_checksum: setChecksum }, lines }] });
    const r = await ingestOrderFinanceChunk(odb, { ...c, log: quiet });
    assert.equal(r.failed.length, 0, JSON.stringify(r));
    const key = { mall: 'amazon', scope: 'jp', source: U };
    const pfp = (await O.query(`select core.finance_policy_fingerprint(1::smallint, 'amazon', 'jp') as f`)).rows[0].f;
    assert.equal((await applyCoverage(odb, { state: 'updating', ...key, generation: 1, run_token: TOK }, { log: quiet })).status, 'applied');
    const rc = receiptDigest([{ mall_order_no: 'A-1', set_checksum: setChecksum, transform_version: V2, lines: 1 }]);
    const manifest = { complete_to: '2026-06-10', settlements_through: '2026-06-10T15:00:00Z', source_revision: 42,
      headers_count: 3, headers_checksum: H('headers'), receipt_count: rc.count, receipt_lines: rc.lines, receipt_digest: rc.digest,
      inventory_snapshot_id: '17', inventory_count: 5, inventory_digest: H('inventory'), inventory_completed_at: '2026-06-11T01:00:00Z',
      initial_marker_id: '1', initial_marker_digest: H('marker'), selected_documents_count: 3, selected_documents_digest: H('documents'),
      evidence_chain_from: '2025-12-01T00:00:00Z', evidence_chain_through: '2026-06-11T01:00:00Z', expected_report_count: 20, expected_report_digest: H('expected'), inventory_runs_digest: H('runs'), policy_fingerprint: pfp };
    assert.equal((await applyCoverage(odb, { state: 'complete', ...key, generation: 1, run_token: TOK, manifest }, { log: quiet })).status, 'applied');
    for (const c2 of [O, W]) {
      const st = (await c2.query(`select complete_to::text as d, generation::text as g from core.finance_coverage_state(1::smallint, 'amazon', 'jp', $1)`, [U])).rows;
      assert.deepEqual(st, [{ d: '2026-06-10', g: '1' }]);
    }
    const daily = (await O.query(`select economic_date_jst::text as d, seller_sku, sales_principal_jpy::int as p from mart.finance_daily_range(1::smallint, 'amazon', 'jp', '2026-06-01', '2026-06-30')`)).rows;
    assert.deepEqual(daily, [{ d: '2026-06-05', seller_sku: 'LA', p: 1000 }]);
    assert.equal(await callAs(W, `select core.finance_month_settled(1::smallint, 'amazon', 'jp', '2026-06-01')`), 'ok');
    assert.equal(await callAs(P, `select mart.amazon_profit_composition_audit_since()`), 'ok');
    // 正当な道を通した後でも、重い関数は閉じたまま
    assert.equal(await callAs(O, `select count(*) from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', '2026-06-01', '2026-06-30')`), 'fn_denied');
  });

  console.log('抜け道が無いこと・作り直しの約束');
  await t('🚨 SECURITY DEFINER の関数 (定義者 = 持ち主) の中から呼んでも 42501 = 抜け道にならない', async () => {
    await O.query('begin');
    try {
      await O.query(`create function mart.zz_d60_secdef_probe() returns bigint language sql security definer set search_path = pg_catalog, pg_temp
        as $$ select count(*) from mart._amazon_profit_ad_days(1::smallint, 'amazon', 'jp', '2026-06-01'::date, '2026-06-01'::date) $$`);
      await O.query(`grant execute on function mart.zz_d60_secdef_probe() to ${PROBE}`);
      await O.query('savepoint a');
      let code = null;
      try { await O.query(`select mart.zz_d60_secdef_probe()`); } catch (e) { code = e.code === '42501' && /permission denied for function _amazon_profit_ad_days/.test(e.message) ? 'fn_denied' : e.code; }
      assert.equal(code, 'fn_denied');
    } finally { await O.query('rollback'); }
  });
  await t('🚨 閉じた関数の create or replace は持ち主でも 42501 → 同じ取引で自分に GRANT → 作り直し → 外す、なら通り、権限の表は空に戻る', async () => {
    const sig = 'mart.amazon_profit_assert_args(smallint, text, text, date, date)';
    const def = (await O.query(`select pg_get_functiondef($1::regprocedure) as d`, [sig])).rows[0].d;
    await O.query('begin');
    let code = null;
    try { await O.query(def); } catch (e) { code = e.code; } finally { await O.query('rollback'); }
    assert.equal(code, '42501');
    await O.query('begin');
    try {
      await O.query(`grant execute on function ${sig} to current_user`);
      await O.query(def);
      await O.query(`revoke execute on function ${sig} from current_user`);
      await O.query('commit');
    } catch (e) { await O.query('rollback'); throw e; }
    const a = await aclOf(O, sig);
    assert.deepEqual([a.dflt, Number(a.n)], [false, 0]);
    assert.equal((await O.query(`select pg_get_functiondef($1::regprocedure) as d`, [sig])).rows[0].d, def);
    assert.deepEqual(await heavyEntryFindings(odb), []);
  });
  await t('2 回流しても同じ (0056 の本文を持ち主がもう一度 = 例外なし・権限の表は空 / 実行器は 0 本)', async () => {
    await O.query(SQL_0056);
    for (const s of CLOSED_FUNCTIONS) assert.equal(Number((await aclOf(O, s)).n), 0, s);
    assert.equal((await applyMigrations(odb, { log: quiet, to: '0056' })).applied.length, 0);
    assert.deepEqual(await heavyEntryFindings(odb), []);
  });

  await t('本適用の手順の --verify (heavy-entry-manifest.mjs・持ち主の URL・読むだけ) = ✅ と TEMP の監査を出して exit 0', async () => {
    const x = new URL(url); x.username = OWNER; x.password = PW; x.pathname = `/${DB1}`;
    const r = spawnSync(process.execPath, ['scripts/company-db/heavy-entry-manifest.mjs', '--verify'], { cwd: ROOT, env: { ...process.env, COMPANY_DB_URL: x.toString() }, encoding: 'utf8', timeout: 60000 });
    assert.equal(r.status, 0, r.stdout + r.stderr);
    assert.match(r.stdout, /✅ 重い入口: revoke 10 個は誰も呼べない/);
    assert.ok(r.stdout.includes('TEMP の権限の監査 (出すだけ'), r.stdout);
  });

  console.log('持ち主でない役割で流すと止まる');
  await t('🚨 PUBLIC だけの役割が 0056 を流す = d60_revoke_incomplete (42501) で止まり、何も変わらない → 持ち主が流せば通る', async () => {
    const O2 = await open(OWNER, DB2); clients.push(O2);
    await migrateUpTo0055(pgAdapter(O2));
    await O2.query(`grant usage on schema core, mart to ${PROBE}`);
    const P2 = await open(PROBE, DB2); clients.push(P2);
    const before = await aclOf(O2, CLOSED_FUNCTIONS[0]);
    await P2.query('begin');
    let err = null;
    try { await P2.query(SQL_0056); } catch (e) { err = e; } finally { await P2.query('rollback'); }
    assert.ok(err, '止まらなかった');
    assert.equal(err.code, '42501'); assert.match(err.message, /d60_revoke_incomplete/);
    assert.deepEqual(await aclOf(O2, CLOSED_FUNCTIONS[0]), before);
    assert.equal(Number(before.n) > 0, true);   // 0055 までの姿 = 持ち主 + watcher (0049 の GRANT)
    const rowsBefore = await aclOf(O2, CLOSED_FUNCTIONS[3]);
    assert.equal(rowsBefore.dflt, true);   // _amazon_profit_rows = 既定 (持ち主 + PUBLIC) のまま
    const x2 = new URL(url); x2.username = OWNER; x2.password = PW; x2.pathname = `/${DB2}`;
    const v0 = spawnSync(process.execPath, ['scripts/company-db/heavy-entry-manifest.mjs', '--verify'], { cwd: ROOT, env: { ...process.env, COMPANY_DB_URL: x2.toString() }, encoding: 'utf8', timeout: 60000 });
    assert.equal(v0.status, 1, '0056 の前の --verify は ❌ (exit 1)'); assert.match(v0.stdout, /❌ 重い入口/);
    // 本適用の手順の dry-run (0055 までの DB) = 0056 だけが出る・何も流さない
    const dry = await applyMigrations(pgAdapter(O2), { dryRun: true, log: quiet, to: '0056' });
    assert.deepEqual([dry.applied, dry.pending.filter((v) => v <= '0056')], [[], ['0056']]);   // 0057 から後は to の外 (pending に並ぶ)
    assert.deepEqual(await aclOf(O2, CLOSED_FUNCTIONS[0]), before);
    assert.deepEqual((await applyMigrations(pgAdapter(O2), { log: quiet, to: '0056' })).applied, ['0056']);
    assert.deepEqual(await heavyEntryFindings(pgAdapter(O2)), []);
  });
}
} finally { await su.end().catch(() => {}); }
} catch (e) {
  setupError = e;
  console.error('❌ 準備か試験の外で落ちた (飛ばさない): ' + (e.stack || e.message));
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  await stopCluster();
}
console.log(`\n${ok} ok / ${ng} NG`);
// 🚨 process.exitCode では足りない: embedded-postgres が入れる async-exit-hook が beforeExit で process.exit(0) を呼び、exitCode = 1 を上書きする
//    (失敗しても exit 0 = npm run test:company-db が緑になる・10/3 に見つけた) → 明示で process.exit
process.exit(ng || setupError || cleanupFailed ? 1 : 0);
