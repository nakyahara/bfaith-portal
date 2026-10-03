#!/usr/bin/env node
/**
 * test-company-db-profit-fn-revoke.mjs — 0056 (D-60 の重い関数の権限の封鎖 = PR 1a・Codex R-D60-v3-4 H2 / R-D60-v3-5) の試験 (PGlite)
 *
 *   ① 0055 まで流した姿 (watcher・profit_reader あり) では、watcher・profit_reader・PUBLIC が重い関数を呼べる (前提)
 *   ② 0056 の後: manifest の revoke の 10 の関数は権限の表が空・watcher・profit_reader・PUBLIC だけの役割は permission denied for function
 *   ③ guard_later・light の関数の権限は 0056 の前と後で 1 文字も変わらない (この PR は外すだけ・正当な呼び手を止めない)
 *   ④ 重い入口の棚卸し: 規則 (mart の関数・期間や件数の引数・本体が閉じた関数を呼ぶ) にかかる関数は全部 manifest にある / 新しい関数・あとからの GRANT は落ちる
 *   ⑤ 2 回流しても同じ・実行器は 0 本 / TEMP の権限は監査を出すだけ (0056 で変わらない)
 *   ⑥ 受け口 /amazon-profit/daily・/totals は 503 のまま・DB に接続しない (#1570 を一字も開けない)
 *   🚨 PGlite の接続は superuser (postgres) = 持ち主の拒否はここでは見えない。持ち主 (superuser でない Render の default user と同じ形) の拒否・
 *      coverage の complete の正当な道・create or replace の約束は本物の PG の試験 (test-company-db-profit-fn-revoke-pg.mjs) で見る
 * 実行: node scripts/test-company-db-profit-fn-revoke.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import { applyMigrations, pgliteAdapter, DEFAULT_DIR } from './company-db/migrate.mjs';
import { HEAVY_ENTRY_MANIFEST, revokeSigs, heavyEntryFindings, heavyCandidates, tempPrivilegeAudit } from './company-db/heavy-entry-manifest.mjs';
import companyDbRouter, { __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};

const pg = new PGlite();
const db = pgliteAdapter(pg);
const q = async (sql, p) => (await pg.query(sql, p)).rows;
const FILE_0056 = path.join(DEFAULT_DIR, '0056_amazon_profit_fn_revoke.sql');
const REVOKE = revokeSigs();
const KEEP = HEAVY_ENTRY_MANIFEST.filter((e) => e.cls !== 'revoke').map((e) => e.sig);

// 本番と同じ順: watcher は 0047 / 0049 / 0050 より前からある (create-watch-roles.mjs が作った) = その migration が watcher に EXECUTE を付けた。
// profit_reader はまだ本番に無いが、あれば外すことを見る (0055 の後に明示の GRANT を付けておく)
await pg.exec(`create role watcher nologin; create role profit_reader nologin; create role d60_public_only nologin`);
const upTo0055 = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-d60-'));
for (const f of fs.readdirSync(DEFAULT_DIR)) if (/^\d{4}_.*\.sql$/.test(f) && f < '0056') fs.copyFileSync(path.join(DEFAULT_DIR, f), path.join(upTo0055, f));
await applyMigrations(db, { dir: upTo0055, log: quiet });
fs.rmSync(upTo0055, { recursive: true, force: true });
// create-watch-roles.mjs と同じ: watcher は schema の USAGE と表の SELECT。PUBLIC だけの役割は schema の USAGE だけ
await pg.exec(`grant usage on schema core, mart to watcher, profit_reader, d60_public_only; grant select on all tables in schema core, mart to watcher;
  grant execute on function mart.amazon_profit_daily_range(smallint, text, text, date, date), mart._amazon_profit_totals(smallint, text, text, date, date) to profit_reader`);

const priv = async (role, sig) => (await q(`select has_function_privilege($1, $2::regprocedure, 'execute') as x`, [role, sig]))[0].x;
const aclText = async (sig) => (await q(`select coalesce(proacl::text, '(既定)') as a from pg_proc where oid = $1::regprocedure`, [sig]))[0].a;
/** 引数を全部 null (型つき) にして呼ぶ SQL (権限は本体より前に見られる) */
const callSql = async (sig) => {
  const r = (await q(`select n.nspname as s, p.proname as n, coalesce((select string_agg('null::' || format_type(t, null), ', ' order by i) from unnest(p.proargtypes::oid[]) with ordinality u(t, i)), '') as a
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.oid = $1::regprocedure`, [sig]))[0];
  return `select ${r.s}.${r.n}(${r.a})`;
};
/** 役割になって呼ぶ (取引の中で set local role → 必ず rollback)。戻り = 'ok' / 'fn_denied' (関数の EXECUTE が無い) / それ以外は SQLSTATE (表の権限・引数の確かめなど) */
const callAs = async (role, sig) => {
  const sql = await callSql(sig);
  await pg.exec('begin');
  try { await pg.exec(`set local role ${role}`); await pg.query(sql); return 'ok'; }
  catch (e) { return e.code === '42501' && /permission denied for function/.test(e.message) ? 'fn_denied' : (e.code || 'error'); }
  finally { await pg.exec('rollback'); }
};

console.log('0055 まで (前提 = 今の本番の姿)');
const keepBefore = {};
const tempBefore = await tempPrivilegeAudit(db);
for (const s of KEEP) keepBefore[s] = await aclText(s);
await t('watcher は公開の関数と finance_daily_sku_range・profit_reader は明示の GRANT・PUBLIC は内部の部品を呼べる (= 0056 が要る)', async () => {
  assert.equal(await priv('watcher', 'mart.amazon_profit_daily_range(smallint, text, text, date, date)'), true);
  assert.equal(await priv('watcher', 'mart.finance_daily_sku_range(smallint, text, text, date, date)'), true);
  assert.equal(await priv('public', 'mart._amazon_profit_rows(smallint, text, text, date, date, mart.amazon_profit_finance_day[], mart.amazon_profit_ad_day[], mart.amazon_profit_ad_child[], mart.amazon_easy_ship_alloc_row[])'), true);
  assert.equal(await callAs('watcher', 'mart.amazon_profit_daily_range(smallint, text, text, date, date)'), '22023');   // 関数に入れて、引数の確かめ (null) で止まる = 呼べている
  assert.notEqual(await callAs('profit_reader', 'mart._amazon_profit_totals(smallint, text, text, date, date)'), 'fn_denied');   // 関数は通る (表の SELECT が無いので中で止まる)
  assert.notEqual(await callAs('d60_public_only', 'mart._amazon_easy_ship_alloc(smallint, text, text, date, date)'), 'fn_denied');   // 関数は通る (中の表の権限で止まるのは別の話)
  assert.ok((await heavyEntryFindings(db)).length > 0);
});

console.log('0056');
await t('実行器で流れる (0056 だけ)', async () => {
  assert.deepEqual((await applyMigrations(db, { log: quiet })).applied, ['0056']);
});
await t('🚨 revoke の 10 の関数は権限の表が空 (null = 既定 でもない)・PUBLIC・watcher・profit_reader は false・棚卸しの問題は 0 件', async () => {
  assert.equal(REVOKE.length, 10);
  for (const s of REVOKE) {
    const r = (await q(`select proacl is null as dflt, coalesce(cardinality(proacl), -1) as n from pg_proc where oid = $1::regprocedure`, [s]))[0];
    assert.deepEqual([s, r.dflt, Number(r.n)], [s, false, 0]);
    for (const role of ['public', 'watcher', 'profit_reader']) assert.equal(await priv(role, s), false, `${role}: ${s}`);
  }
  assert.deepEqual(await heavyEntryFindings(db), []);
});
await t('🚨 superuser でない役割から呼ぶと permission denied for function (watcher・profit_reader・PUBLIC だけの役割 × 10 の関数)', async () => {
  for (const s of REVOKE) for (const role of ['watcher', 'profit_reader', 'd60_public_only']) assert.equal(await callAs(role, s), 'fn_denied', `${role}: ${s}`);
});
await t('🚨 guard_later・light の関数の権限の表は 0056 の前と同じ (1 文字も変えない)・正当な呼び手の関数は今までどおり呼べる', async () => {
  for (const s of KEEP) assert.equal(await aclText(s), keepBefore[s], s);
  assert.equal(await priv('watcher', 'mart.finance_daily_range(smallint, text, text, date, date)'), true);   // 0044 の明示の GRANT のまま (guard_later)
  for (const s of ['core.finance_coverage_state(smallint, text, text, text)', 'core.finance_month_settled(smallint, text, text, date)', 'core.finance_policy_fingerprint(smallint, text, text)',
    'mart.amazon_profit_composition_audit_since()', 'mart.amazon_account_fee_tax_rate(text)']) assert.equal(await callAs('watcher', s), 'ok', `watcher: ${s}`);
  assert.equal(await callAs('d60_public_only', 'mart.amazon_profit_composition_audit_since()'), 'ok');
  assert.equal((await q(`select count(*)::int as n from mart.finance_daily_range(1::smallint, 'amazon', 'jp', '2026-06-01', '2026-06-01')`))[0].n, 0);
});
await t('superuser (PGlite の接続 = 試験の持ち主) は今も呼べる = ほかの試験 (amazon-profit・finance-coverage) は変わらない', async () => {
  assert.equal((await q(`select count(*)::int as n from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-06-01', '2026-06-01')`))[0].n, 0);
});
await t('2 回流しても同じ (0056 の本文をもう一度 = 例外なし・全部の manifest の関数の権限の表が同じ / 実行器は 0 本)', async () => {
  const before = {}; for (const e of HEAVY_ENTRY_MANIFEST) before[e.sig] = await aclText(e.sig);
  await pg.exec(fs.readFileSync(FILE_0056, 'utf8'));
  for (const e of HEAVY_ENTRY_MANIFEST) assert.equal(await aclText(e.sig), before[e.sig], e.sig);
  assert.equal((await applyMigrations(db, { log: quiet })).applied.length, 0);
  assert.deepEqual(await heavyEntryFindings(db), []);
});
await t('TEMP の権限は監査を出すだけ (0056 の前と後で同じ・PUBLIC は TEMP あり = PR 1b で外す)・一時の表を作る関数を数える', async () => {
  const a = await tempPrivilegeAudit(db);
  assert.deepEqual(a, tempBefore);
  assert.equal(a.publicTemp, true);
  for (const s of ['core.relink_shipments_bulk', 'core.reresolve_order_lines', 'core.merge_duplicate_suppliers']) assert.ok(a.tempFunctions.some((x) => x.startsWith(s + '(')), `${s} が一時の表を作る関数に無い`);
});

console.log('重い入口の棚卸し (heavy_entry_manifest と pg_proc の突き合わせ)');
await t('規則にかかる関数は全部 manifest にあり、manifest の関数は全部 DB にある (mart.finance_daily_range・ad_efficiency・sku_activity は guard_later)', async () => {
  const cands = await heavyCandidates(db);
  const inManifest = new Set();
  for (const e of HEAVY_ENTRY_MANIFEST) inManifest.add((await q('select to_regprocedure($1)::oid::text as o', [e.sig]))[0].o);
  assert.deepEqual(cands.filter((c) => !inManifest.has(c.oid)).map((c) => c.sig), []);
  const cls = Object.fromEntries(HEAVY_ENTRY_MANIFEST.map((e) => [e.sig.slice(0, e.sig.indexOf('(')), e.cls]));
  for (const n of ['mart.finance_daily_range', 'mart.ad_efficiency', 'mart.ad_efficiency_coverage', 'mart.sku_activity', 'mart.sku_activity_gaps', 'mart.sales_expanded_to_skus']) assert.equal(cls[n], 'guard_later', n);
  for (const e of HEAVY_ENTRY_MANIFEST) assert.ok(e.reason && e.reason.length > 4, `理由: ${e.sig}`);
});
const inTx = async (sql, fn) => { await pg.exec('begin'); try { await pg.exec(sql); return await fn(); } finally { await pg.exec('rollback'); } };
await t('🚨 mart の新しい関数 (包む関数など) は、作った取引で REVOKE しても「分けていない」で落ちる (manifest に足すまで)', async () => {
  const f = await inTx(`create function mart.amazon_profit_month_daily(p date) returns int language sql as $$ select 1 $$;
    revoke execute on function mart.amazon_profit_month_daily(date) from public`, () => heavyEntryFindings(db));
  assert.equal(f.length, 1, f.join(' / ')); assert.match(f[0], /分けていない.*mart\.amazon_profit_month_daily\(p date\)/);
});
await t('🚨 期間の引数を持つ core の新しい関数・本体で閉じた関数を呼ぶ新しい関数も落ちる', async () => {
  const f1 = await inTx(`create function core.some_range(p_company_id smallint, p_from date, p_to date) returns int language sql as $$ select 1 $$`, () => heavyEntryFindings(db));
  assert.equal(f1.length, 1, f1.join(' / ')); assert.match(f1[0], /分けていない.*core\.some_range/);
  const f2 = await inTx(`create function ops.some_report(a date) returns bigint language sql as $$ select count(*) from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', a, a) $$`, () => heavyEntryFindings(db));
  assert.equal(f2.length, 1, f2.join(' / ')); assert.match(f2[0], /分けていない.*ops\.some_report/);
});
await t('🚨 閉じた関数にあとから GRANT すると落ちる (PUBLIC・watcher・profit_reader) / guard_later の PUBLIC を閉じたら manifest を直すまで落ちる', async () => {
  const f1 = await inTx(`grant execute on function mart._amazon_profit_totals(smallint, text, text, date, date) to public`, () => heavyEntryFindings(db));
  assert.ok(f1.some((x) => /_amazon_profit_totals.*空でない/.test(x)) && f1.some((x) => /_amazon_profit_totals.*PUBLIC が呼べる/.test(x)), f1.join(' / '));
  for (const role of ['watcher', 'profit_reader']) {
    const f2 = await inTx(`grant execute on function mart.finance_daily_sku_range(smallint, text, text, date, date) to ${role}`, () => heavyEntryFindings(db));
    assert.ok(f2.some((x) => new RegExp(`finance_daily_sku_range.*呼べる役割がある \\(${role}\\)`).test(x)), f2.join(' / '));
  }
  const f3 = await inTx(`revoke execute on function mart.sku_activity(smallint, date, date) from public`, () => heavyEntryFindings(db));
  assert.ok(f3.some((x) => /sku_activity.*manifest の期待 true/.test(x)), f3.join(' / '));
});

console.log('受け口 (#1570 の 503 を一字も開けない)');
await t('🚨 /amazon-profit/daily・/totals は 503 PROFIT_ROUTE_DISABLED・DB に接続しない (pg の client を作らない)', async () => {
  process.env.MIRROR_SYNC_KEY = 'k';
  process.env.COMPANY_DB_URL = 'pglite://test';   // 接続先が「ある」状態 (無いと別の 503 になり、DB に行く形に戻っても気づけない)
  let created = 0;
  __setPgClientFactory(async () => { created++; return { query: (text, params) => pg.query(text, params), end: async () => {} }; });
  const app = express();
  app.use('/apps/company-db/sync', companyDbRouter);
  const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  try {
    for (const kind of ['daily', 'totals']) for (const qs of ['mall=amazon&scope=jp&from=2026-06-01&to=2026-06-30', 'mall=amazon&scope=jp&from=2026-06-30&to=2026-09-30', '']) {
      const res = await fetch(`http://127.0.0.1:${server.address().port}/apps/company-db/sync/amazon-profit/${kind}${qs ? `?${qs}` : ''}`, { headers: { 'x-sync-key': 'k' } });
      const j = await res.json();
      assert.equal(res.status, 503, `${kind}?${qs}`);
      assert.deepEqual([j.ok, j.code], [false, 'PROFIT_ROUTE_DISABLED']);
    }
    assert.equal(created, 0, 'pg の client が作られた = 503 の前に DB に接続している');
    const src = fs.readFileSync(new URL('../apps/company-db/router.mjs', import.meta.url), 'utf8');
    for (const s of REVOKE) assert.ok(!src.includes(s.slice(0, s.indexOf('('))), `router に ${s} の名前がある`);
  } finally { await new Promise((r) => server.close(r)); }
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
if (ng) process.exitCode = 1;
