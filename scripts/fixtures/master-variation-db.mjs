/**
 * master-variation-db.mjs — 試験だけ: 色違い・サイズ違いのまとまりの登録 (PR-7) の試験の Company DB (PGlite)
 *
 * 0066 まで流す → 夜間ロード (NE で作ったまとまり ws100 = 札・子 3 つ / 単品が代表のまとまり towel-gift = 子 2 つ / ふつうの単品) → 切替 (new_open・持ち主は全部 company =
 *   products.parent も company = PR-8 の後・widen の後の形) → backfill → 今日の照合の回と NE の元のコード・新規開始の許可 → 0067 を流す (今ある札・単品の代表を予約)。
 * 書き込みは画面のロール master_edit (本物の関数の権限)。🚨 試験だけ
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const MIG_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'db', 'company', 'migrations');
export const RUN1 = 'mc_20300110T000000000Z_aaaaaa';
/** 照合の回の「今日」(NE 登録の CSV の画面・作る = この日の回) */
export const NOW_MS = new Date('2030-01-10T03:00:00Z').getTime();
export const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }] };
const BUILDS = { render: ['r1'], minipc: ['m1'] };

const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 1100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 2980, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
export const BASE_SKUS = Object.freeze([
  sku('s001', '単品 1'), sku('s002', '単品 2'), sku('s003', '単品 3 (JAN あり)'),
  sku('ws100-BR', 'ウールストール【ブラウン】', { representativeCode: 'ws100', representativeState: 'value' }),
  sku('ws100-NV', 'ウールストール【ネイビー】', { representativeCode: 'ws100', representativeState: 'value' }),
  sku('ws100-GY', 'ウールストール【グレー】', { representativeCode: 'ws100', representativeState: 'value', standardPriceJpy: 3280 }),
  sku('towel-gift', '今治タオル ギフト'),
  sku('towel-gift-2p', '今治タオル ギフト 2 枚', { representativeCode: 'towel-gift', representativeState: 'value', standardPriceJpy: 3300 }),
  sku('towel-gift-3p', '今治タオル ギフト 3 枚', { representativeCode: 'towel-gift', representativeState: 'value', standardPriceJpy: 3300 }),
]);

/**
 * 試験の DB を作る。戻り値 { pg, db, q, one, asRole, asEditor, ALL_COMPANY, OWN, SUP1, SUP2, sessionUser }
 */
export async function setupVariationDb({ quiet = () => {}, extraSkus = [] } = {}) {
  const W2 = await import('./master-widen-pr1.mjs');
  const { PGlite } = await import('@electric-sql/pglite');
  const { applyMigrations, pgliteAdapter } = await import('../company-db/migrate.mjs');
  const { createRoles } = await import('../company-db/create-watch-roles.mjs');
  const { createMasterEditRoles } = await import('../company-db/create-master-edit-roles.mjs');
  const { runInitialLoad } = await import('../../apps/company-db/load/engine.mjs');
  const C = await import('../../lib/master-cutover.mjs');
  const { OWNED_COLUMNS } = await import('../../config/master-ownership.mjs');
  const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
  const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
  const files = fs.readdirSync(MIG_DIR).filter((x) => /^\d{4}_.*\.sql$/.test(x)).sort();
  const pre0067 = files.map((x) => x.slice(0, 4)).filter((v) => v < '0067').pop();

  const pg = new PGlite();
  const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet, to: pre0067 });
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  await W2.useReal0058(pg, { leases: ['single', 'set'], futureSetLease: true });
  const q = async (sql, params) => (await db.query(sql, params)).rows;
  const one = async (sql, params) => (await q(sql, params))[0];
  async function asRole(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
  async function asGate(host, fn) {
    await pg.query(`set session authorization master_gate_${host}`);
    try { return await fn(); } finally { await pg.query(`set session authorization ${sessionUser}`); await pg.query('set role deploy'); }
  }
  const plan = basePlan(extraSkus);
  const skus = plan.skus;
  const lr = await runInitialLoad(db, plan, { log: quiet, runId: 'load_vg7', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
  if (!lr.ok) throw new Error(`夜間ロードの失敗: ${lr.error}`);
  async function toPhase(to) {
    const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
    if (to !== 'frozen') await (await import('./master-epoch.mjs')).seedActiveEpoch(db, ALL_COMPANY);
    const own = to === 'frozen' ? MASTER_OWNERSHIP : ALL_COMPANY;
    for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) {
      await asGate(host, () => C.recordLegacyGateAck(db, { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
    }
    const mh = await C.manifestHashOf(db, MANIFEST);
    const now = new Date().toISOString();
    const evidence = to === 'frozen'
      ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(MASTER_OWNERSHIP), manual_entries_stopped: [{ id: 'ne.product_screen', by: 'naka@test', at: now }], drain: { done: true, checked_by: 'naka@test', checked_at: now } }
      : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL_COMPANY) };
    return asRole('master_ops', () => C.advanceCutoverPhase(db, { to, actor: 'naka@test', evidence }));
  }
  await toPhase('frozen');
  await toPhase('company_owner');
  const bp = await one('select * from ops.registration_backfill_plan()');
  await asRole('master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3, $4)', [bp.sku_count, bp.snapshot_hash, 'naka@test', '試験']));
  await toPhase('new_open');
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
  const entries = [...skus.map((s) => ({ code_norm: s.code.toLowerCase(), kind: 'product', state: 'ok', ne_code: s.code, spellings: [s.code] })),
    { code_norm: 'ws100', kind: 'rep', state: 'ok', ne_code: 'ws100', spellings: ['ws100'] }, { code_norm: 'towel-gift', kind: 'rep', state: 'ok', ne_code: 'towel-gift', spellings: ['towel-gift'] }];
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN1, entries })]);
  await (await import('./master-widen.mjs')).seedNewEntryLease(db, { runId: RUN1, withSet: true });
  await applyMigrations(db, { log: quiet });
  await createMasterEditRoles(pg, {});
  const SUP1 = (await one(`select supplier_id::text as id from core.suppliers where code = '0001'`)).id;
  // JAN を持つ単品 (s003) = ほかの商品の JAN の重なりの試験
  const OWN = JSON.stringify(ALL_COMPANY);
  return { pg, db, q, one, asRole, asEditor: (fn) => asRole('master_edit', fn), ALL_COMPANY, OWN, SUP1, sessionUser, MASTER_OWNERSHIP, W2 };
}

/** 夜間ロードの材料 (今の NE の形)。extraSkus = 足す商品 (切替の後に流すと NE で直接作られた商品 = quarantined) */
export function basePlan(extraSkus = [], extraGroups = []) {
  const skus = [...BASE_SKUS, ...extraSkus];
  return {
    skus,
    variationGroups: [{ code: 'ws100', name: 'ウールストール', childCodes: ['ws100-BR', 'ws100-NV', 'ws100-GY'], status: 'active' },
      { code: 'towel-gift', name: '今治タオル ギフト', childCodes: ['towel-gift-2p', 'towel-gift-3p'], status: 'active' }, ...extraGroups],
    setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0034', name: '三河テキスタイル' }],
    supplierSkus: skus.map((s) => ({ supplierCode: s.code === 'ws100-GY' ? '0034' : '0001', skuCode: s.code })),
    primarySuppliers: skus.map((s) => ({ skuCode: s.code, supplierCode: s.code === 'ws100-GY' ? '0034' : '0001' })), reorder: { available: true, runId: 'pml_vg7' },
  };
}
export const skuOf = sku;

/** JAN 13 けた (チェック数字つき) */
export const jan13 = (b) => { const d = b.split('').map(Number).reverse(); const s = d.reduce((a, x, i) => a + x * (i % 2 === 0 ? 3 : 1), 0); return b + ((10 - (s % 10)) % 10); };
