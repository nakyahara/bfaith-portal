#!/usr/bin/env node
/**
 * test-company-db-concurrency.mjs — 日次表の building 検査 (0011) を 2 接続で確かめる (08 §7.6 / §7.7。PR #1312 Codex R2〜R4)
 *
 * PGlite は 1 接続なので、実 Postgres (Render の Company DB) に対して流す。0011 が適用済みであること。
 * 使うのは専用の scope '__concurrency_test__' と日付 2000-01-01 (default パーティション) だけ。
 *   ① 日次行の書き込みが未 commit の間、完了処理 (status = 'complete') は待つ。書き込みが commit されると完了し、その後の書き込みは拒まれる
 *   ② 完了処理が未 commit の間、日次行の書き込みは待つ。完了が commit されると、待っていた書き込みは「complete」で拒まれる (READ COMMITTED の再評価)
 *   「待っている」は pg_blocking_pids() で相手の PID に塞がれていることを確かめてから commit する (時間切れの推測ではない。R3 #1)
 *   INSERT / UPDATE / DELETE × warehouse_stock_daily / sku_stock_daily の 12 通り
 * 接続は 3 本: L = advisory lock を持ち、開始時と終了時に専用 scope の残骸を 日次行 → capture → run の順で消す (残り 0 行が必須) / A = 書き込み側 / B = 完了処理側。
 *   ロックが取れなければ何も消さずに終わる (R4 #1)。中断 (SIGINT / SIGTERM / 接続障害) は 1 本の終了処理に集め、以後は新しい SQL を出さない (R4 #2, #3)。
 *   L の接続が切れてロックを失ったら、新しい接続で取り直せたときだけ回収する (別の実行が始まっていたら触らない)。
 * 使い方: COMPANY_DB_URL=postgres://... node scripts/test-company-db-concurrency.mjs   (または --url postgres://...)
 * 🚨 本番に一時的に行を足して消す (ops.ingest_runs 1 行・stock_capture_days 1 行・日次行 1〜2 行)。完了 commit の直後〜片付けまでの数秒は
 *   mart.v_sku_stock がその会社について 2000-01-01 の値を返す。取込ジョブ (D2) や読み手が動く時間帯は避ける
 */
import { openPgClient, pgAdapter, migrationStatus } from './company-db/migrate.mjs';

const args = process.argv.slice(2);
const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const url = getArg('--url') || process.env.COMPANY_DB_URL;
if (!url) { console.error('COMPANY_DB_URL (または --url) が要る'); process.exit(2); }

const SCOPE = '__concurrency_test__';
const DATE = '2000-01-01';
const RUN = `ct-${Date.now()}`;
const LOCK_KEY = 'test-company-db-concurrency';
const BLOCK_DEADLINE_MS = 5000;   // 「塞がれている」が観測できるまで待つ上限
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const wrap = (p) => p.then((r) => ({ ok: r }), (e) => ({ err: e }));   // 後で await できる形にしておく (未処理の reject を作らない)

let ok = 0, ng = 0;
let aborted = null;   // 中断の理由。入ったら試験側 (A / B) は新しい SQL を出さない
const abort = (why) => { if (!aborted) { aborted = why; console.log(`\n中断: ${why}`); } };
const t = async (name, fn) => {
  if (aborted) return;
  try { await fn(); ok++; console.log('  ok  ' + name); }
  catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); }
};
const expect = (cond, msg) => { if (!cond) throw new Error(msg); };
const one = async (c, sql, params = []) => (await c.query(sql, params)).rows[0];
const rollbackQuiet = async (c) => { try { await c.query('rollback'); } catch { /* 取引の外・接続が壊れているなら何もしない */ } };
const endQuiet = async (c) => { try { if (c) await c.end(); } catch { /* 壊れていても進む */ } };
// 試験側の接続: 中断後は SQL を出さない (R4 #2)。接続障害は中断にする (R4 #3)
const guard = (raw, name) => {
  raw.on('error', (e) => abort(`${name} の接続が切れた: ${e.message}`));
  return { name, raw, query: (sql, params) => (aborted ? Promise.reject(new Error(`中断中 (${aborted}) なので ${name} に SQL を出さない`)) : raw.query(sql, params)) };
};

const TABLES = ['sku_stock_daily', 'warehouse_stock_daily'];
const CODE_COL = { sku_stock_daily: 'source_code', warehouse_stock_daily: 'line_key' };
const ctx = {};
const insertRow = (c, T, code, qty) => T === 'sku_stock_daily'
  ? c.query(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, captured_at, ingest_run_id) values ($1, 'logizard', $2, $3, $4, $5, $6, now(), $7)`, [DATE, SCOPE, code, ctx.co, ctx.sku, qty, RUN])
  : c.query(`insert into snapshots.warehouse_stock_daily (snapshot_date, source, scope_key, line_key, company_id, sku_id, logizard_code, location_code, qty, captured_at, ingest_run_id) values ($1, 'logizard', $2, $3, $4, $5, 'CT', 'CT-01', $6, now(), $7)`, [DATE, SCOPE, code, ctx.co, ctx.sku, qty, RUN]);
const count = async (c, T, code) => Number((await one(c, `select count(*) as n from snapshots.${T} where snapshot_date = $1 and scope_key = $2 and ${CODE_COL[T]} = $3`, [DATE, SCOPE, code])).n);
const qtyOf = async (c, T, code) => (await one(c, `select qty from snapshots.${T} where snapshot_date = $1 and scope_key = $2 and ${CODE_COL[T]} = $3`, [DATE, SCOPE, code]))?.qty;
const ops = (T, A) => ({
  insert: { pre: async () => {}, run: (c) => insertRow(c, T, 'Y', 1), applied: async (c) => expect((await count(c, T, 'Y')) === 1, 'Y が無い'), untouched: async (c) => expect((await count(c, T, 'Y')) === 0, 'Y が入っている') },
  update: { pre: () => insertRow(A, T, 'X', 1), run: (c) => c.query(`update snapshots.${T} set qty = 2 where snapshot_date = $1 and scope_key = $2 and ${CODE_COL[T]} = 'X'`, [DATE, SCOPE]), applied: async (c) => expect((await qtyOf(c, T, 'X')) === 2, 'X が 2 でない'), untouched: async (c) => expect((await qtyOf(c, T, 'X')) === 1, 'X が 1 でない') },
  delete: { pre: () => insertRow(A, T, 'X', 1), run: (c) => c.query(`delete from snapshots.${T} where snapshot_date = $1 and scope_key = $2 and ${CODE_COL[T]} = 'X'`, [DATE, SCOPE]), applied: async (c) => expect((await count(c, T, 'X')) === 0, 'X が残っている'), untouched: async (c) => expect((await count(c, T, 'X')) === 1, 'X が消えている') },
});
const complete = (c) => c.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1 and source = 'logizard' and scope_key = $2`, [DATE, SCOPE]);
const captureStatus = async (c) => (await one(c, `select status from snapshots.stock_capture_days where snapshot_date = $1 and source = 'logizard' and scope_key = $2`, [DATE, SCOPE]))?.status;
const pidOf = async (c) => (await one(c, 'select pg_backend_pid() as pid')).pid;
// observer から見て「blocked が by に塞がれている」が観測できるまで待つ (期限つき)。observer は取引の中で idle の接続でよい
const waitBlocked = async (observer, blockedPid, byPid) => {
  const deadline = Date.now() + BLOCK_DEADLINE_MS;
  while (Date.now() < deadline) {
    if ((await one(observer, 'select $1::int = any(pg_blocking_pids($2::int)) as b', [byPid, blockedPid])).b) return true;
    await sleep(100);
  }
  return false;
};
// 次の場面のために building へ戻し、日次行を消す (保守経路)
const reset = async (A) => {
  if (aborted) return;
  await A.query('begin');
  await A.query(`set local snapshots.maintenance = 'on'`);
  for (const T of TABLES) await A.query(`delete from snapshots.${T} where snapshot_date = $1 and scope_key = $2`, [DATE, SCOPE]);
  await A.query(`update snapshots.stock_capture_days set status = 'building', completed_at = null where snapshot_date = $1 and source = 'logizard' and scope_key = $2`, [DATE, SCOPE]);
  await A.query('commit');
};

// ─── L: advisory lock と回収 ───
let L = null, haveLock = false, lockLost = false;
const openL = async () => {
  const c = await openPgClient(url);
  c.on('error', (e) => { if (haveLock) { lockLost = true; haveLock = false; } abort(`L の接続が切れた: ${e.message}`); });
  await c.query(`set statement_timeout = '20s'`);
  return c;
};
const tryLock = async (c) => (await one(c, `select pg_try_advisory_lock(hashtext($1)) as got`, [LOCK_KEY])).got;
// 専用 scope の残骸を消す (日次行 → capture → run の順。保守経路)。残り件数を返す。🚨 advisory lock を持つ接続からだけ呼ぶ
const sweepVia = async (c, label) => {
  await c.query('begin');
  await c.query(`set local snapshots.maintenance = 'on'`);
  for (const T of TABLES) await c.query(`delete from snapshots.${T} where scope_key = $1`, [SCOPE]);
  await c.query(`delete from snapshots.stock_capture_days where scope_key = $1`, [SCOPE]);
  await c.query(`delete from ops.ingest_runs where source_system = 'logizard' and entity = 'inventory' and scope_key = $1`, [SCOPE]);
  await c.query('commit');
  const left = Number((await one(c, `select (select count(*) from snapshots.stock_capture_days where scope_key = $1) + (select count(*) from ops.ingest_runs where scope_key = $1)
                                          + (select count(*) from snapshots.sku_stock_daily where scope_key = $1) + (select count(*) from snapshots.warehouse_stock_daily where scope_key = $1) as n`, [SCOPE])).n);
  console.log(`${label}: 専用 scope の残り ${left} 行`);
  return left;
};

const A = guard(await openPgClient(url), 'A');   // 書き込み側 (取込)
const B = guard(await openPgClient(url), 'B');   // 完了処理側

// 終了処理は 1 本だけ (シグナルが重なっても、finally と重なっても同じ Promise を待つ。R4 #2)
let finishPromise = null;
const finish = () => (finishPromise ??= (async () => {
  await rollbackQuiet(B.raw); await rollbackQuiet(A.raw);   // B のロックを先に外す (A の待ち中のクエリが終わる)
  try {
    if (haveLock && L) {
      if ((await sweepVia(L, '片付け')) !== 0) { ng++; console.log('  NG  片付け後も専用 scope に行が残っている'); }
    } else if (lockLost) {
      // ロックを持っていた L が切れた → 取り直せたときだけ回収 (別の実行が始まっていたら触らない)
      const L2 = await openPgClient(url);
      try {
        await L2.query(`set statement_timeout = '20s'`);
        if (await tryLock(L2)) { if ((await sweepVia(L2, '片付け (再接続)')) !== 0) { ng++; console.log('  NG  片付け後も専用 scope に行が残っている'); } }
        else { ng++; console.log('  NG  回収できない: 別の実行がロックを持っている (専用 scope に行が残っているかもしれない。次の実行の開始時に回収される)'); }
      } finally { await endQuiet(L2); }
    } else {
      console.log('ロックを持っていないので何も消さない');   // R4 #1: 先行の実行を壊さない
    }
  } catch (e) { ng++; console.log('  NG  片付けに失敗: ' + (e.message || e)); }
  await endQuiet(A.raw); await endQuiet(B.raw); await endQuiet(L);
})());
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { abort(sig); finish().finally(() => process.exit(130)); });

try {
  for (const c of [A, B]) await c.query(`set statement_timeout = '20s'`);   // 何かが噛み合わなくても止まる
  const st = await migrationStatus(pgAdapter(A.raw));
  const m11 = st.find((s) => s.version === '0011');
  if (!m11 || m11.state !== 'applied') { console.error(`0011 が適用されていない (${m11 ? m11.state : 'なし'})`); process.exitCode = 2; throw new Error('skip'); }
  L = await openL();
  haveLock = await tryLock(L);
  if (!haveLock) { console.error('別の実行が進行中 (advisory lock)。何もせず終わる'); process.exitCode = 2; throw new Error('skip'); }
  if ((await sweepVia(L, '開始時の回収')) !== 0) { console.error('前回の残骸が消せない'); process.exitCode = 1; throw new Error('skip'); }

  ctx.co = (await one(A, `select company_id from core.companies order by company_id limit 1`)).company_id;
  ctx.sku = (await one(A, `select sku_id from core.skus where company_id = $1 order by sku_id limit 1`, [ctx.co])).sku_id;
  const pidA = await pidOf(A), pidB = await pidOf(B);
  await A.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, complete) values ($1, 'logizard', 'inventory', $2, now(), 'running', false)`, [RUN, SCOPE]);
  await A.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1, 'logizard', $2, $3, 'building', $4)`, [DATE, SCOPE, ctx.co, RUN]);
  console.log(`2 接続の試験 (scope ${SCOPE}, run ${RUN}, pid A=${pidA} B=${pidB})`);

  for (const T of TABLES) {
    for (const [op, o] of Object.entries(ops(T, A))) {
      await t(`① ${T} ${op}: 書き込みが未 commit の間、完了処理は待つ (pg_blocking_pids) → 書き込み commit 後に完了 → 以後の書き込みは拒まれる`, async () => {
        await o.pre();
        await A.query('begin');
        await o.run(A);                                    // capture 行を for share
        await B.query('begin');
        const pB = wrap(complete(B));                      // for no key update → for share と衝突して待つはず
        expect(await waitBlocked(A, pidB, pidA), '完了処理が A に塞がれていない');
        await A.query('commit');
        const rB = await pB;
        expect(!rB.err, '完了処理が失敗: ' + (rB.err && rB.err.message));
        await B.query('commit');
        expect((await captureStatus(A)) === 'complete', 'complete になっていない');
        await o.applied(A);
        const again = await wrap(insertRow(A, T, 'Z', 1));
        expect(again.err && /complete|building/.test(again.err.message), '完了後の書き込みが通った');
        await rollbackQuiet(A);
      });
      await reset(A);
      await t(`② ${T} ${op}: 完了処理が未 commit の間、書き込みは待つ (pg_blocking_pids) → 完了 commit 後に「complete」で拒まれる (READ COMMITTED の再評価)`, async () => {
        await o.pre();
        await B.query('begin');
        await complete(B);                                 // 行ロックを持ったまま
        await A.query('begin');
        const pA = wrap(o.run(A));                         // trigger の for share が待つはず
        expect(await waitBlocked(B, pidA, pidB), '書き込みが B に塞がれていない');
        await B.query('commit');
        const rA = await pA;
        expect(rA.err && /complete|building/.test(rA.err.message), '待っていた書き込みが通った: ' + (rA.err ? rA.err.message : 'ok'));
        await rollbackQuiet(A);
        await o.untouched(A);
      });
      await reset(A);
    }
  }
} catch (e) {
  if (e.message !== 'skip') { ng++; console.log('  NG  (試験の外) ' + (e.message || e)); }
} finally {
  await finish();
}
if (aborted) { console.log(`\n中断のため未完 (${aborted})`); process.exitCode = process.exitCode || 130; }
console.log(`\n${ok} ok / ${ng} NG`);
if (!process.exitCode) process.exitCode = ng ? 1 : 0;
