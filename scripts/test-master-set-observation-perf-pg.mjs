/**
 * test-master-set-observation-perf-pg.mjs — 夜間ロードのセットの構成の観測 (apps/company-db/load/engine.mjs の recordSetObservations・0051 の ops.record_ne_set_observations = 0053 で集合の形に置き換えた) の
 * 時間を実 PostgreSQL で測る (#1571 Codex R1 Medium 2)。セット 1,000 / 5,000 (構成品 3 つずつ・単品 300) の DB を作り、ロードを 2 回流す:
 *   1 回目 = 持ち主 load (ロードが構成を NE に合わせる。観測は書く・上げる候補は 0)
 *   2 回目 = 持ち主 company・1 割のセットの構成を変えた材料 (観測 + 上げる候補 = 1 割 + 候補ごとの昇格 = 切替の前なので before_cutover で戻る)
 * 出す時間: ロード全体・観測の関数 (record)・上げる候補の計算 (candidates)・昇格 (promotions)。上限を超えたら落とす (取引の中の観測 + 候補 = 1,000 で 3 秒・5,000 で 10 秒 = 本番の statement_timeout 20 秒の半分。
 *   0053 の前 (セットごとのループ = セットの数の 2 乗) は 5,000 で 30 秒前後・0053 の後は 1 秒前後 (#1571 R1 の ⑤-1 への頼み)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-set-observation-perf-pg.mjs [1000,5000]
 *   (この PC では C:/tmp/pg-embed の run-conc.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む。package.json の試験には入れない (時間がかかる・PostgreSQL が要る)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { MASTER_OWNERSHIP } from '../config/master-ownership.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の時間の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
const SIZES = (process.argv[2] || '1000,5000').split(',').map(Number);
const LIMIT_MS = { 1000: 3000, 5000: 10000 };
const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const SINGLES = 300;

const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const setSku = (code) => ({ code, name: code, kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: null });
const pcode = (i) => `p${String(i % SINGLES).padStart(4, '0')}`;
/** セット i の構成 (changed = 1 割のセットだけ数量を変える) */
const compsOf = (n, changed) => {
  const rows = [];
  for (let i = 0; i < n; i++) {
    const code = `s${String(i).padStart(5, '0')}`;
    const bump = changed && i % 10 === 0 ? 1 : 0;
    for (let k = 0; k < 3; k++) rows.push({ parentCode: code, childCode: pcode(i * 3 + k), qty: 1 + k + bump, source: 'ne' });
  }
  return rows;
};
const hash = (s) => crypto.createHash('sha256').update(s).digest('hex');
const materialOf = (genId, rows) => {
  const at = new Date(Date.now() - 60000).toISOString().replace('T', ' ').slice(0, 19);
  const h = hash(JSON.stringify(rows));
  return {
    products: { status: 'no_generation', content_hash: hash('p'), row_count: 1, generation: null },
    set_components: { status: 'matched', content_hash: h, row_count: rows.length, generation: { generation_id: genId, content_hash: h, row_count: rows.length, source_complete_at: at, created_at: at } },
  };
};
const planOf = (n, rows, material) => ({
  skus: [...Array.from({ length: SINGLES }, (_, i) => single(pcode(i))), ...Array.from({ length: n }, (_, i) => setSku(`s${String(i).padStart(5, '0')}`))],
  variationGroups: [], setComponents: rows, listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], material,
});

const admin = await openPgClient(url);
const out = [];
let failed = false;
try {
  for (const n of SIZES) {
    const dbName = `cdb_perf_${n}_${crypto.randomBytes(3).toString('hex')}`;
    await admin.query(`create database ${dbName}`);
    const uu = new URL(url); uu.pathname = `/${dbName}`;
    const c = await openPgClient(uu.toString());
    try {
      const db = pgAdapter(c);
      await applyMigrations(db, { log: () => {} });
      const rows1 = compsOf(n, false);
      const t0 = Date.now();
      const r1 = await runInitialLoad(db, planOf(n, rows1, materialOf(`mat_perf_${n}_1`, rows1)), { log: () => {}, runId: `load_perf_${n}_1`, now: new Date() });
      const load1 = Date.now() - t0;
      assert.equal(r1.ok, true, r1.error);
      const so1 = r1.set_observations;
      assert.deepEqual([so1.state, so1.complete, so1.sets, so1.candidates.length], ['written', true, n, 0], JSON.stringify({ ...so1, candidates: so1.candidates.length }));
      const rows2 = compsOf(n, true);
      const t1 = Date.now();
      const r2 = await runInitialLoad(db, planOf(n, rows2, materialOf(`mat_perf_${n}_2`, rows2)), { log: () => {}, runId: `load_perf_${n}_2`, ownership: ALL_COMPANY, now: new Date() });
      const load2 = Date.now() - t1;
      assert.equal(r2.ok, true, r2.error);
      const so2 = r2.set_observations;
      assert.deepEqual([so2.state, so2.complete, so2.sets, so2.candidates.length], ['written', true, n, n / 10], JSON.stringify({ ...so2, candidates: so2.candidates.length }));
      const row = { sets: n, load1_ms: load1, record1_ms: so1.ms.record, candidates1_ms: so1.ms.candidates, load2_ms: load2, record2_ms: so2.ms.record, candidates2_ms: so2.ms.candidates,
        promotions: so2.promotions?.before_cutover ?? null, promotions_ms: so2.promotions?.ms ?? null };
      out.push(row);
      const inTx = Math.max(so1.ms.record + so1.ms.candidates, so2.ms.record + so2.ms.candidates);
      if (LIMIT_MS[n] && inTx > LIMIT_MS[n]) { failed = true; console.error(`  NG  セット ${n}: 取引の中の観測 + 候補 ${inTx} ms > ${LIMIT_MS[n]} ms`); }
      else console.log(`  ok  セット ${n}: ${JSON.stringify(row)}`);
    } finally {
      try { await c.end(); } catch { /* */ }
      try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
    }
  }
} catch (e) {
  failed = true;
  console.error(`  NG  ${e.stack || e.message}`);
} finally {
  try { await admin.end(); } catch { /* */ }
}
console.log(JSON.stringify(out));
process.exit(failed ? 1 : 0);
