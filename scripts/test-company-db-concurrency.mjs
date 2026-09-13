#!/usr/bin/env node
/**
 * test-company-db-concurrency.mjs — 日次表の building 検査 (0011) を 2 接続で確かめる (08 §7.6 / §7.7。PR #1312 Codex R2)
 *
 * PGlite は 1 接続なので、実 Postgres (Render の Company DB) に対して流す。0011 が適用済みであること。
 * 使うのは専用の scope '__concurrency_test__' と日付 2000-01-01 (default パーティション) だけ。終わりに (失敗しても) 保守経路で消す。
 *   ① 日次行の書き込みが未 commit の間、完了処理 (status = 'complete') は待つ。書き込みが commit されると完了し、その後の書き込みは拒まれる
 *   ② 完了処理が未 commit の間、日次行の書き込みは待つ。完了が commit されると、待っていた書き込みは「complete」で拒まれる (READ COMMITTED の再評価)
 *   INSERT / UPDATE / DELETE × warehouse_stock_daily / sku_stock_daily の 12 通り
 * 使い方: COMPANY_DB_URL=postgres://... node scripts/test-company-db-concurrency.mjs   (または --url postgres://...)
 * 🚨 本番に一時的に行を足して消す (ops.ingest_runs 1 行・stock_capture_days 1 行・日次行 1〜2 行)。取込ジョブ (D2) が動く時間帯は避ける
 */
import { openPgClient, pgAdapter, migrationStatus } from './company-db/migrate.mjs';

const args = process.argv.slice(2);
const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const url = getArg('--url') || process.env.COMPANY_DB_URL;
if (!url) { console.error('COMPANY_DB_URL (または --url) が要る'); process.exit(2); }

const SCOPE = '__concurrency_test__';
const DATE = '2000-01-01';
const RUN = `ct-${Date.now()}`;
const WAIT_MS = 800;   // 「待っている」と判定するまでの時間
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const wrap = (p) => p.then((r) => ({ ok: r }), (e) => ({ err: e }));           // 後で await できる形にしておく (未処理の reject を作らない)
const isPending = async (w) => (await Promise.race([w.then(() => 'settled'), sleep(WAIT_MS).then(() => 'pending')])) === 'pending';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const expect = (cond, msg) => { if (!cond) throw new Error(msg); };

const A = await openPgClient(url);   // 書き込み側 (取込)
const B = await openPgClient(url);   // 完了処理側
for (const c of [A, B]) await c.query(`set statement_timeout = '20s'`);   // 何かが噛み合わなくても止まる
const one = async (c, sql, params = []) => (await c.query(sql, params)).rows[0];

const TABLES = {
  sku_stock_daily: {
    codeCol: 'source_code',
    insert: (c, code, qty) => c.query(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, captured_at, ingest_run_id) values ($1, 'logizard', $2, $3, $4, $5, $6, now(), $7)`, [DATE, SCOPE, code, ctx.co, ctx.sku, qty, RUN]),
  },
  warehouse_stock_daily: {
    codeCol: 'line_key',
    insert: (c, code, qty) => c.query(`insert into snapshots.warehouse_stock_daily (snapshot_date, source, scope_key, line_key, company_id, sku_id, logizard_code, location_code, qty, captured_at, ingest_run_id) values ($1, 'logizard', $2, $3, $4, $5, 'CT', 'CT-01', $6, now(), $7)`, [DATE, SCOPE, code, ctx.co, ctx.sku, qty, RUN]),
  },
};
const ctx = {};
const ops = (T) => ({
  insert: { pre: async () => {}, run: (c) => TABLES[T].insert(c, 'Y', 1), after: { applied: async (c) => expect((await count(c, T, 'Y')) === 1, 'Y が無い'), untouched: async (c) => expect((await count(c, T, 'Y')) === 0, 'Y が入っている') } },
  update: { pre: async () => TABLES[T].insert(A, 'X', 1), run: (c) => c.query(`update snapshots.${T} set qty = 2 where snapshot_date = $1 and scope_key = $2 and ${TABLES[T].codeCol} = 'X'`, [DATE, SCOPE]), after: { applied: async (c) => expect((await qtyOf(c, T, 'X')) === 2, 'X が 2 でない'), untouched: async (c) => expect((await qtyOf(c, T, 'X')) === 1, 'X が 1 でない') } },
  delete: { pre: async () => TABLES[T].insert(A, 'X', 1), run: (c) => c.query(`delete from snapshots.${T} where snapshot_date = $1 and scope_key = $2 and ${TABLES[T].codeCol} = 'X'`, [DATE, SCOPE]), after: { applied: async (c) => expect((await count(c, T, 'X')) === 0, 'X が残っている'), untouched: async (c) => expect((await count(c, T, 'X')) === 1, 'X が消えている') } },
});
const count = async (c, T, code) => (await one(c, `select count(*)::int n from snapshots.${T} where snapshot_date = $1 and scope_key = $2 and ${TABLES[T].codeCol} = $3`, [DATE, SCOPE, code])).n;
const qtyOf = async (c, T, code) => (await one(c, `select qty from snapshots.${T} where snapshot_date = $1 and scope_key = $2 and ${TABLES[T].codeCol} = $3`, [DATE, SCOPE, code]))?.qty;
const complete = (c) => c.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1 and source = 'logizard' and scope_key = $2`, [DATE, SCOPE]);
const captureStatus = async (c) => (await one(c, `select status from snapshots.stock_capture_days where snapshot_date = $1 and source = 'logizard' and scope_key = $2`, [DATE, SCOPE]))?.status;
// 次の場面のために building へ戻し、日次行を消す (保守経路)
const reset = async () => {
  await A.query('begin');
  await A.query(`set local snapshots.maintenance = 'on'`);
  for (const T of Object.keys(TABLES)) await A.query(`delete from snapshots.${T} where snapshot_date = $1 and scope_key = $2`, [DATE, SCOPE]);
  await A.query(`update snapshots.stock_capture_days set status = 'building', completed_at = null where snapshot_date = $1 and source = 'logizard' and scope_key = $2`, [DATE, SCOPE]);
  await A.query('commit');
};
const rollbackQuiet = async (c) => { try { await c.query('rollback'); } catch { /* 取引の外なら何もしない */ } };

try {
  const st = await migrationStatus(pgAdapter(A));
  const m11 = st.find((s) => s.version === '0011');
  if (!m11 || m11.state !== 'applied') { console.error(`0011 が適用されていない (${m11 ? m11.state : 'なし'})`); process.exitCode = 2; throw new Error('skip'); }

  ctx.co = (await one(A, `select company_id from core.companies order by company_id limit 1`)).company_id;
  ctx.sku = (await one(A, `select sku_id from core.skus where company_id = $1 order by sku_id limit 1`, [ctx.co])).sku_id;
  await A.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, complete) values ($1, 'logizard', 'inventory', $2, now(), 'running', false)`, [RUN, SCOPE]);
  await A.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1, 'logizard', $2, $3, 'building', $4)`, [DATE, SCOPE, ctx.co, RUN]);
  console.log(`2 接続の試験 (scope ${SCOPE}, run ${RUN})`);

  for (const T of Object.keys(TABLES)) {
    for (const [op, o] of Object.entries(ops(T))) {
      await t(`① ${T} ${op}: 書き込みが未 commit の間、完了処理は待つ → 書き込み commit 後に完了 → 以後の書き込みは拒まれる`, async () => {
        await o.pre();
        await A.query('begin');
        await o.run(A);                                    // capture 行を for share
        await B.query('begin');
        const pB = wrap(complete(B));                      // for update 相当 → 待つはず
        expect(await isPending(pB), '完了処理が待たなかった');
        await A.query('commit');
        const rB = await pB;
        expect(!rB.err, '完了処理が失敗: ' + (rB.err && rB.err.message));
        await B.query('commit');
        expect((await captureStatus(A)) === 'complete', 'complete になっていない');
        await o.after.applied(A);
        const again = await wrap(TABLES[T].insert(A, 'Z', 1));
        expect(again.err && /complete|building/.test(again.err.message), '完了後の書き込みが通った');
        await rollbackQuiet(A);
      });
      await reset();
      await t(`② ${T} ${op}: 完了処理が未 commit の間、書き込みは待つ → 完了 commit 後に「complete」で拒まれる (READ COMMITTED の再評価)`, async () => {
        await o.pre();
        await B.query('begin');
        await complete(B);                                 // 行ロックを持ったまま
        await A.query('begin');
        const pA = wrap(o.run(A));                         // trigger の for share が待つはず
        expect(await isPending(pA), '書き込みが待たなかった');
        await B.query('commit');
        const rA = await pA;
        expect(rA.err && /complete|building/.test(rA.err.message), '待っていた書き込みが通った: ' + (rA.err ? rA.err.message : 'ok'));
        await rollbackQuiet(A);
        await o.after.untouched(A);
      });
      await reset();
    }
  }
} catch (e) {
  if (e.message !== 'skip') { ng++; console.log('  NG  (試験の外) ' + (e.message || e)); }
} finally {
  // 失敗しても足した行は消す (保守経路)
  await rollbackQuiet(A); await rollbackQuiet(B);
  try {
    await A.query('begin');
    await A.query(`set local snapshots.maintenance = 'on'`);
    for (const T of Object.keys(TABLES)) await A.query(`delete from snapshots.${T} where snapshot_date = $1 and scope_key = $2`, [DATE, SCOPE]);
    await A.query(`delete from snapshots.stock_capture_days where snapshot_date = $1 and source = 'logizard' and scope_key = $2`, [DATE, SCOPE]);
    await A.query(`delete from ops.ingest_runs where ingest_run_id = $1`, [RUN]);
    await A.query('commit');
    const left = await one(A, `select (select count(*) from snapshots.stock_capture_days where scope_key = $1) + (select count(*) from ops.ingest_runs where scope_key = $1) as n`, [SCOPE]);
    console.log(`片付け: 残り ${left.n} 行`);
  } catch (e) { ng++; console.log('  NG  片付けに失敗: ' + (e.message || e)); }
  await A.end(); await B.end();
}
console.log(`\n${ok} ok / ${ng} NG`);
if (!process.exitCode) process.exitCode = ng ? 1 : 0;
