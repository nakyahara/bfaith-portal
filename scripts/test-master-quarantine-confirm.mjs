/**
 * test-master-quarantine-confirm.mjs — 要確認 (quarantined) の商品を確認済み (ne_confirmed) にする DB の道 (0078・Company DB構想 22 の Q-1・中原さんの決定 10/13 Q1〜Q5 = a) を PGlite で確かめる
 *
 * 固定する契約:
 *   [M]  migration の写し: ops.transition_sku_registration = 0073 の版 (前の最新) から写して 🆕 0078 のかたまりだけ足した (かたまりを外すと 0073 と 1 字も違わない)・
 *        約束の操作の CHECK = 0072 の全部 + quarantine_confirm・確かめ待ちの view = 0067 の版の品目の行のまま (要確認の行だけ替えた)・
 *        security definer の関数は search_path = pg_catalog, pg_temp・public の実行権なし・一時の表なし
 *   [V]  照合 ② の確かめ待ち (ops.v_ne_reg_targets) に要確認を全部 (単品もセットも・親のある子も・products.parent の持ち主を問わず)・確認済みにしたら外れる
 *   [C]  確かめ (ops.quarantine_confirm_check): ① 必須の欄 (単品・セット) の 1 つ 1 つ・直し方の組 ② 照合の回の新しさ・観測・判断の候補 (まだ / 承認 / 再発 / 却下 /
 *        写した / 列に当たらない) ③ 代表 (まとまりの記録・代表の採用) ④ 生きている CSV・check_hash (同じなら同じ・値が変われば変わる)
 *   [F]  確認済みにする関数 (ops.confirm_quarantined_sku): 画面のロール = 確認済み・記録 (登録の状態の履歴の根拠 = DB が作った値・保存の記録 done)・押し直し = 前の答え・
 *        同じ番号で違う中身 = request_id_reused・表が変わった = stale・だめ = not_confirmable・段階 = before_cutover・形の誤り = invalid_input
 *   [T]  ops.transition_sku_registration を直に呼んでも (運用のロール・持ち主・違う約束・呼び手の根拠) 要確認は確認済みにならない・ほかの道は今までどおり
 *   [A]  確認済みの後: 新しいセットの構成品・NE 登録の CSV の構成品に使える (今断っている 2 つが開く)
 *   [R]  ロール: master_edit = 確かめ・一覧・確認の実行 / watcher = 確かめ・一覧 (読むだけの取引でも) / master_ops = 確認は 42501 / 部品はだれも
 *   [H]  重い入口の棚卸し: 全部の migration を流した DB で heavyEntryFindings が空・manifest の since = その関数を作った migration の番号
 * 使い方: node scripts/test-master-quarantine-confirm.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles, QUARANTINE_CONFIRM_FUNCTIONS, QUARANTINE_CONFIRM_OWNER_ONLY_FUNCTIONS } from './company-db/create-master-edit-roles.mjs';
import { HEAVY_ENTRY_MANIFEST, heavyEntryFindings } from './company-db/heavy-entry-manifest.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { setActiveMapOnly } from './fixtures/master-widen-pr1.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIG = path.join(ROOT, 'db', 'company', 'migrations');

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const quiet = () => {};
const hex = (c) => c.repeat(64);

// ═══════════ [M] migration の写し (文字で) ═══════════
const migFiles = fs.readdirSync(MIG).filter((f) => /^\d{4}_.+\.sql$/.test(f)).sort();
const readMig = (f) => fs.readFileSync(path.join(MIG, f), 'utf8').replace(/\r\n/g, '\n');
const THIS = migFiles.find((f) => /_quarantine_confirm\.sql$/.test(f));
const THIS_V = THIS.slice(0, 4);
const M78 = readMig(THIS);
/** 関数の定義の文字 (create [or replace] function <name>( ... $$;)。見つからない = null */
function fnText(text, name) {
  const re = new RegExp(`create (?:or replace )?function ${name.replace(/\./g, '\\.')}\\(`);
  const i = text.search(re);
  if (i < 0) return null;
  const k = text.indexOf(' as $$', i);
  const j = text.indexOf('$$;', k + 6);
  return text.slice(i, j + 3).replace(/^create or replace function/, 'create function');
}
/** 「🆕 0078 ここから〜ここまで」のかたまりと「🆕 0078」の行を外す */
function strip0078(t) {
  const out = [];
  let skip = false;
  for (const l of t.split('\n')) {
    if (/-- 🆕 0078 ここから/.test(l)) { skip = true; continue; }
    if (/-- 🆕 0078 ここまで/.test(l)) { skip = false; continue; }
    if (skip || /🆕 0078/.test(l)) continue;
    out.push(l);
  }
  return out.join('\n');
}
const lastDefBefore = (name, before = THIS_V) => migFiles.filter((f) => f < before && fnText(readMig(f), name)).pop();

console.log('[M] migration の写し');
await ta('[M1] ops.transition_sku_registration = 0073 の版 (前の最新) から写して 🆕 0078 のかたまりだけ足した (かたまりの中は要確認 → NE 確認済みの道だけ)', async () => {
  const src = lastDefBefore('ops.transition_sku_registration');
  assert.ok(src && src.startsWith('0073'), `前の最新の版が ${src} (期待 0073)`);
  const before = fnText(readMig(src), 'ops.transition_sku_registration');
  const now = fnText(M78, 'ops.transition_sku_registration');
  assert.ok(now, `${THIS} に無い`);
  assert.notEqual(now, before, '何も足していない');
  assert.equal(strip0078(now), before, 'かたまりを外すと 0073 の版と違う (写し間違い・印の無い変更)');
  const block = now.slice(now.indexOf('-- 🆕 0078 ここから'), now.indexOf('-- 🆕 0078 ここまで'));
  assert.match(block, /if p_to = 'ne_confirmed' and r\.state = 'quarantined' then/);
  assert.equal((now.match(/-- 🆕 0078 ここから/g) || []).length, 1);
  // かたまりは one_way の確かめの後・not_ready の前 (地図の外へは進めない・distributable / available は今までどおり)
  assert.ok(now.indexOf('-- 🆕 0078 ここから') > now.indexOf("'one_way:") && now.indexOf('-- 🆕 0078 ここから') < now.indexOf("'not_ready:"));
});
await ta('[M2] 約束の操作の CHECK = 0072 の全部 + quarantine_confirm (どちらの表も)・0073〜前の番号は CHECK を変えていない', async () => {
  const ops = (text, tbl) => {
    const m = new RegExp(`alter table ops\\.${tbl} add constraint ck_\\w+ check \\(operation in \\(([^;]*?)\\)\\);`).exec(text);
    return m ? [...m[1].matchAll(/'([a-z_]+)'/g)].map((x) => x[1]) : null;
  };
  const src = readMig(migFiles.find((f) => f.startsWith('0072_')));
  for (const tbl of ['master_write_sessions', 'master_edit_requests']) {
    const before = ops(src, tbl); const now = ops(M78, tbl);
    assert.ok(before && now, tbl);
    assert.deepEqual(now, [...before, 'quarantine_confirm'], tbl);
    for (const f of migFiles.filter((x) => x.slice(0, 4) > '0072' && x.slice(0, 4) < THIS_V)) assert.equal(ops(readMig(f), tbl), null, `${f} が ${tbl} の CHECK を変えている (写す元が違う)`);
  }
});
await ta('[M3] 確かめ待ちの view = 0067 の版 (前の最新) の品目の行のまま・要確認の行だけ替えた・列は同じ', async () => {
  const viewText = (text) => { const i = text.indexOf('create or replace view ops.v_ne_reg_targets as'); return i < 0 ? null : text.slice(i, text.indexOf(';', i) + 1); };
  const src = migFiles.filter((f) => f < THIS_V && viewText(readMig(f))).pop();
  assert.ok(src.startsWith('0067'), `前の最新の版が ${src}`);
  const before = viewText(readMig(src)); const now = viewText(M78);
  const firstBranch = (t) => t.slice(0, t.indexOf('union all'));
  assert.equal(firstBranch(now), firstBranch(before));
  assert.match(now, /select null::bigint, null::bigint, s\.sku_id, s\.code_norm, s\.sku_kind, 'quarantined'::text/);
});

// ═══════════ DB の用意 (0078 まで・夜間ロード・切替・要確認を夜間ロードで作る) ═══════════
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const q = async (sql, p) => (await pg.query(sql, p)).rows;
const one = async (sql, p) => (await q(sql, p))[0];
async function as(role, fn) {
  await pg.query(`set role ${role}`);
  try { return await fn(); } finally { try { await pg.query('set role deploy'); } catch { /* 取引が止まった = 下の errOf の rollback to savepoint で役も戻る */ } }
}
/** 失敗を返す。取引の中なら savepoint で戻す (取引の外 = savepoint が断られる = 何もしない) */
const errOf = async (fn) => {
  let sp = true;
  try { await pg.query('savepoint err_of'); } catch { sp = false; }
  try { await fn(); if (sp) await pg.query('release savepoint err_of'); } catch (e) { if (sp) await pg.query('rollback to savepoint err_of'); return e; }
  return null;
};
async function rolledBack(fn) { await pg.query('begin'); try { return await fn(); } finally { await pg.query('rollback'); } }

const single = (code, x = {}) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...x });
const BASE = ['a01', 'a02', 'a03', 'grp'];
const planOf = (extra = []) => {
  const singles = [...BASE, ...extra.filter((s) => s.kind === 'single').map((s) => s.code)];
  return { skus: [...BASE.map((c) => single(c, c === 'a02' ? { taxRate: 0.08, taxClass: 'REDUCED_8', cost: { jpy: 50, source: 'ne', status: 'COMPLETE' } } : {})), ...extra],
    variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'B' }], supplierSkus: singles.map((c) => ({ supplierCode: '0001', skuCode: c })),
    primarySuppliers: singles.map((c) => ({ skuCode: c, supplierCode: '0001' })), reorder: { available: false, runId: null, reason: '試験' } };
};
const lr = await runInitialLoad(db, planOf(), { log: quiet, runId: 'qc_load_1', host: 'test', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
const ALL = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const OWN = JSON.stringify(ALL);
await forceNewOpen(db, ALL);
// 切替の後の夜間ロード = NE で直接作られた商品 (要確認)。夜間ロードは名前・売価・税率・取扱だけ入れる (原価・代表の仕入先・構成・推奨保有月数は入れない = 本番の 42 件と同じ形)
const Q_SINGLES = ['q01', 'q02', 'q03', 'q04', 'q05', 'q06', 'q07'];
const Q_SETS = ['qs1', 'qs2'];
const lr2 = await runInitialLoad(db, planOf([...Q_SINGLES.map((c) => single(c)), ...Q_SETS.map((c) => ({ ...single(c), kind: 'set', cost: { jpy: 1, source: 'set_calc', status: 'COMPLETE' } }))]),
  { log: quiet, runId: 'qc_load_2', host: 'test', now: new Date(Date.now() - 1 * 86400e3) });
assert.equal(lr2.ok, true, lr2.error);
const sid = async (code) => String((await one('select sku_id from core.skus where code = $1', [code])).sku_id);
const pidOf = async (code) => String((await one('select product_id from core.skus where code = $1', [code])).product_id);
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))?.state ?? null;
const TODAY = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
const SUP1 = String((await one(`select supplier_id from core.suppliers where code = '0001'`)).supplier_id);
// 持ち主の手で値を入れる (試験だけ = 本番は画面の保存 saveSku・Q-2)
const setPrimary = async (code, supplierId = SUP1) => q(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) values (1, $1, $2, true, 'human', 't')
  on conflict (supplier_id, sku_id) do update set is_primary = true`, [supplierId, await sid(code)]);
const setCost = async (code, jpy, source = 'manual', status = 'COMPLETE') => {
  const id = await sid(code);
  await q(`update core.sku_costs set valid_to = $2::date - 1 where sku_id = $1 and valid_to is null and valid_from < $2::date`, [id, TODAY]);
  await q(`delete from core.sku_costs where sku_id = $1 and valid_from = $2::date`, [id, TODAY]);
  await q(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id) values (1, $1, $2, $3, $4, $5::date, 't', 'human', 't')`,
    [id, jpy, source, status, TODAY]);
};
const setSku = async (code, sets) => { const keys = Object.keys(sets); await q(`update core.skus set ${keys.map((k, i) => `${k} = $${i + 2}`).join(', ')} where code = $1`, [code, ...keys.map((k) => sets[k])]); };
const setComp = async (setCode, comps) => {
  await q('delete from core.sku_components where parent_sku_id = $1', [await sid(setCode)]);
  let n = 0;
  for (const [c, qty] of comps) await q(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, sort_order, source) values (1, $1, $2, $3, $4, 'ne')`, [await sid(setCode), await sid(c), qty, n++]);
};
/** 単品を全部そろえる (原価 300・代表の仕入先 0001・推奨保有月数 2) */
const fillSingle = async (code, cost = 300) => { await setPrimary(code); await setCost(code, cost); await setSku(code, { reorder_months: 2 }); };
for (const c of Q_SINGLES) await fillSingle(c);
// セット: qs1 = a01 × 2 + a02 (原価 100 × 2 + 50 = 250・税率 10% と 8% = MIXED 8%)・qs2 = q01 (要確認の構成品)
await setComp('qs1', [['a01', 2], ['a02', 1]]);
await setSku('qs1', { tax_rate: 0.08, tax_class: 'MIXED', reorder_months: 1 });
await setCost('qs1', 250, 'set_calc', 'COMPLETE');
await setComp('qs2', [['q01', 1]]);
await setSku('qs2', { reorder_months: 1 });
await setCost('qs2', 300, 'set_calc', 'COMPLETE');

// ─── 照合 ② の回 (本物の関数: watch_writer = 判断の候補 → NE のコード → 確かめ待ちの写し → 観測 → 封) ───
let runSeq = 0;
const runIdAt = (at) => `mc_${at.toISOString().slice(0, 10).replace(/-/g, '')}T${String(++runSeq).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;
/** 今の Company DB の値と同じ観測 (ops.ne_reg_canonical の期待値と同じ形 = 全部の列が合う) */
async function obsLikeCdb(codeNorm) {
  const k = await one('select sku_id, sku_kind from core.skus where code_norm = $1', [codeNorm]);
  const exp = (await one('select ops.ne_reg_canonical($1, $2::date) as r', [k.sku_id, TODAY])).r.expected;
  const st = (v) => (v == null ? { st: 'no_value', v: null } : { st: 'ok', v });
  if (k.sku_kind === 'set') {
    return { code_norm: codeNorm, present: true, trusted: true, kind: 'set', ne_code: codeNorm, cols: { name: st(exp.values.name), price: st(exp.values.price) },
      children: exp.values.children.map((c) => ({ code_norm: c.code_norm, st: 'ok', v: c.qty })) };
  }
  const v = exp.values;
  return { code_norm: codeNorm, present: true, trusted: true, kind: 'single', ne_code: codeNorm,
    cols: { name: st(v.name), supplier: st(v.supplier), cost: st(v.cost), price: st(v.price), tax_rate: st(v.tax_rate), handling: st(v.handling), parent: st(v.parent) } };
}
const fp = (s) => crypto.createHash('sha256').update(s).digest('hex');
/** 判断の候補 (照合 ② が出す形) */
const cand = (code, col, { child = null, c = null, n = null, cls = 'rule', reason = 'company_owned', resolutions = ['accept_difference', 'fix_ne', 'fix_cdb'], tag = '' } = {}) => ({
  fingerprint: fp(`${code}|${col}|${child}|${tag}|${JSON.stringify(c)}|${JSON.stringify(n)}`), subject_key: `${col === 'cost' ? 'cost' : 'value'}:${code}`, code_norm: code, col, child, cls, reason_kind: reason,
  semantic: `${reason}@1`, print: { code_norm: code, col, child, c, n }, resolutions, proposal: { op: 'decide' } });
/**
 * 照合 ② の回を 1 つ置く。obs = { code_norm: 観測 | (今と同じ観測) => 観測 }・decisions = 候補・fetchAgoMs = 取得の古さ・neCodes = false なら NE のコードの印を進めない
 */
async function sealRun({ obs = {}, decisions = [], fetchAgoMs = 1000, neCodes = true, at = new Date() } = {}) {
  const run = runIdAt(at);
  await as('watch_writer', () => pg.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run, observed_at: at.toISOString(), decisions })]));
  if (neCodes) {
    const codes = await q("select code_norm from core.skus where company_id = 1 and sku_kind in ('single', 'set') order by 1");
    await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: codes.map((c) => ({ code_norm: c.code_norm, kind: 'product', state: 'ok', ne_code: c.code_norm, spellings: [c.code_norm] })) })]);
  }
  const snap = (await as('watch_writer', () => pg.query('select ops.snapshot_ne_reg_targets($1) as r', [run]))).rows[0].r;
  const observations = [];
  for (const t of snap.targets) {
    const base = await obsLikeCdb(t.code_norm);
    const o = obs[t.code_norm];
    observations.push(typeof o === 'function' ? o(base) : o ?? base);
  }
  const fetched = new Date(at.getTime() - fetchAgoMs).toISOString();
  const w = (await as('watch_writer', () => pg.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run,
    fetch: { generation_id: `gen_${run.slice(-6)}`, products_rev: '1', sets_rev: '1', raw_hash: hex('c') }, products_at: fetched, sets_at: fetched, absence_trusted: true, observations })]))).rows[0].r;
  await as('watch_writer', () => pg.query('select ops.seal_ne_registration_run($1, $2, $3)', [run, w.observation_hash, hex('e')]));
  return { run, targets: snap.targets.map((t) => t.code_norm) };
}
const check = async (code) => (await one('select ops.quarantine_confirm_check($1::bigint) as r', [await sid(code)])).r;
const bad = (c) => c.items.filter((x) => !x.ok).map((x) => x.key).sort();
const itemOf = (c, key) => c.items.find((x) => x.key === key);
const approve = async (fingerprint, resolution, target = null) => (await one(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor)
  values ($1, 'approved', $2, $3::jsonb, 'user', 'naka@test') returning event_id`, [fingerprint, resolution, target ? JSON.stringify(target) : null])).event_id;
const editor = (sql, p) => as('master_edit', async () => (await pg.query(sql, p)).rows[0]);
const confirm = async (code, { rid = crypto.randomUUID(), reason = null, hash = null, own = OWN } = {}) => {
  const h = hash ?? (await check(code)).check_hash ?? hex('0');
  return (await editor('select ops.confirm_quarantined_sku($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6) as r', [rid, 'naka@test', reason, own, await sid(code), h])).r;
};

// ═══════════ [V] 照合 ② の確かめ待ち ═══════════
console.log('[V] 確かめ待ち');
await ta('[V1] 要確認を全部 (単品もセットも・親のある子も・products.parent の持ち主を問わず) 確かめ待ちに入れる・利用可は入らない', async () => {
  const tg = async () => (await q(`select code_norm, sku_kind from ops.v_ne_reg_targets where state = 'quarantined' order by code_norm`)).map((r) => `${r.code_norm}:${r.sku_kind}`);
  const all = [...Q_SINGLES.map((c) => `${c}:single`), ...Q_SETS.map((c) => `${c}:set`)].sort();
  assert.deepEqual(await tg(), all);
  await rolledBack(async () => {
    // 親のある子 (帰属 load・夜間ロードが付けた形) も入る (0067 は「親の無い単品」だけだった)
    await pg.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
    await q(`update core.products set parent_product_id = $1, parent_set_by = 'load' where product_id = $2`, [await pidOf('grp'), await pidOf('q07')]);
    assert.deepEqual(await tg(), all);
  });
  // products.parent の持ち主が load でも入る (0067 は company のときだけだった)
  await setActiveMapOnly(pg, { ...ALL, 'products.parent': 'load' });
  try { assert.deepEqual(await tg(), all); } finally { await setActiveMapOnly(pg, ALL); }
  assert.equal((await one(`select count(*)::int as n from ops.v_ne_reg_targets where code_norm in ('a01', 'a02')`)).n, 0);
});

// ═══════════ [C] 確かめ ═══════════
console.log('[C] 確かめ');
let RUN_A;
await ta('[C1] 照合の回が無い = ne_run / ne_observed (明日の照合の後に)・ほかは全部 ok (単品 q01)', async () => {
  const c = await check('q01');
  assert.deepEqual(bad(c), ['ne_observed', 'ne_run']);
  assert.equal(itemOf(c, 'ne_run').fix, 'after_compare');
  assert.equal(c.evidence, null);
  assert.match(c.check_hash, /^[0-9a-f]{64}$/);
});
await ta('[C2] 封をした回で NE と全部同じ = ok・根拠 (照合の回・観測のハッシュ・check_hash・取得の時刻)・check_hash は読み直しても同じ', async () => {
  RUN_A = await sealRun();
  assert.ok(RUN_A.targets.includes('q01') && RUN_A.targets.includes('qs1'));
  const c = await check('q01');
  assert.deepEqual(bad(c), [], JSON.stringify(c.items.filter((x) => !x.ok)));
  assert.equal(c.ok, true);
  assert.equal(c.evidence.rule, 'quarantine_confirm_v1');
  assert.equal(c.evidence.compare_run_id, RUN_A.run);
  assert.equal(c.evidence.check_hash, c.check_hash);
  assert.match(c.evidence.observation_hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(c.evidence.decisions, []);
  assert.equal((await check('q01')).check_hash, c.check_hash);
  assert.equal(itemOf(c, 'ne.cost').text, 'NE と同じ');
  // セット qs1 (構成品 a01 × 2 + a02・MIXED 8%・原価 250 = 構成品の合計) も ok
  const s = await check('qs1');
  assert.deepEqual(bad(s), [], JSON.stringify(s.items.filter((x) => !x.ok)));
});
await ta('[C3] 単品の必須の欄の 1 つ 1 つ (取引の中で変えて巻き戻す): 推奨保有月数 (Q2 a)・売上分類・原価 (無い / 決まっていない / 0 円)・代表の仕入先 (無い / 形)・税率と税区分・取扱・名前・売価', async () => {
  const cases = [
    [() => setSku('q02', { reorder_months: null }), ['reorder_months'], 'one'],
    [async () => q('update core.products set sales_class = null where product_id = $1', [await pidOf('q02')]), ['sales_class'], 'one'],
    [async () => q('delete from core.sku_costs where sku_id = $1', [await sid('q02')]), ['cost', 'ne.cost'], 'ne_fill'],
    [() => setCost('q02', 300, 'manual', 'PARTIAL'), ['cost', 'ne.cost'], 'one'],
    [() => setCost('q02', 0, 'manual', 'COMPLETE'), ['cost', 'ne.cost'], 'one'],
    [async () => q('update core.supplier_skus set is_primary = false where sku_id = $1', [await sid('q02')]), ['ne.supplier', 'supplier'], 'ne_fill'],
    [async () => q(`update core.suppliers set code = 'X1' where code = '0001'`), ['ne.supplier', 'supplier'], 'one'],
    [() => setSku('q02', { tax_class: 'REDUCED_8' }), ['tax'], 'ne_fill'],
    [() => setSku('q02', { handling: 'unknown' }), ['handling', 'ne.handling'], 'ne_fill'],
    [() => setSku('q02', { name: 'Q02' }), ['name', 'ne.name'], 'ne_fill'],
    [() => setSku('q02', { standard_price_jpy: null }), ['ne.price', 'price'], 'ne_fill'],
  ];
  for (const [mut, keys, fix] of cases) {
    await rolledBack(async () => {
      await mut();
      const c = await check('q02');
      // 照合の答え (ne.*) = 今の社内の値と封をした回の観測を比べる (照合の後に値を変えた = 判断が無い = だめ・#1715 Codex R1 High)
      assert.deepEqual(bad(c), keys, `${mut.toString()} → ${JSON.stringify(c.items.filter((x) => !x.ok))}`);
      assert.equal(itemOf(c, keys.find((k) => !k.startsWith('ne.'))).fix, fix, mut.toString());
      assert.equal(c.ok, false); assert.equal(c.evidence, null);
    });
  }
  assert.equal((await check('q02')).ok, true);
});
await ta('[C4] セットの欄: 構成品が要確認 (qs2) / 開いている構成の依頼 / 税率・取扱が構成品から導いた値と違う / 売上分類が導けない (上書きで通る) / 原価 (合計と違う・例外原価で通る・例外原価 0 円はだめ) / 構成品が無い', async () => {
  const s2 = await check('qs2');
  assert.deepEqual(bad(s2), ['components']);
  assert.match(itemOf(s2, 'components').text, /q01 \(quarantined\)/);
  const cases = [
    [async () => q(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id) values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`,
      [await sid('qs1'), JSON.stringify([{ sku_id: Number(await sid('a01')), code: 'a01', qty: 1, sort: 1 }]), hex('9')]), ['component_request']],
    [() => setSku('qs1', { tax_rate: 0.1, tax_class: 'STANDARD_10' }), ['tax']],
    [() => setSku('a01', { handling: 'discontinued' }), ['handling']],
    [async () => { await setSku('a01', { handling: 'discontinued' }); await setSku('qs1', { handling: 'discontinued' }); }, []],
    [async () => { await setSku('qs1', { handling_own: 'discontinued' }); }, ['handling']],
    [async () => q('update core.products set sales_class = 4 where product_id = $1', [await pidOf('a01')]), ['sales_class']],
    [async () => { await q('update core.products set sales_class = 4 where product_id = $1', [await pidOf('a01')]); await setSku('qs1', { set_sales_class_override: 1 }); }, []],
    [() => setCost('qs1', 251, 'set_calc', 'COMPLETE'), ['cost']],
    [() => setCost('qs1', 999, 'manual', 'OVERRIDDEN'), []],
    [() => setCost('qs1', 0, 'manual', 'OVERRIDDEN'), ['cost']],   // #1715 Codex R1 M1: 例外原価 0 円は通さない (単品と同じ)
    [() => setCost('qs1', 0, 'set_calc', 'COMPLETE'), ['cost']],   // #1715 Codex R2 M1: 構成品の合計 (set_calc) でも 0 円は通さない
    [() => setCost('qs1', 250, 'manual', 'COMPLETE'), ['cost']],
    [async () => q(`update core.sku_costs set cost_status = 'MISSING' where sku_id = $1 and valid_to is null`, [await sid('a02')]), ['cost']],
    [async () => q('delete from core.sku_components where parent_sku_id = $1', [await sid('qs1')]), ['components', 'cost', 'ne.children', 'sales_class', 'tax']],
  ];
  for (const [mut, keys] of cases) {
    await rolledBack(async () => {
      await mut();
      const c = await check('qs1');
      assert.deepEqual(bad(c).filter((k) => !k.startsWith('ne.') || keys.includes(k)), keys, `${mut.toString()} → ${JSON.stringify(c.items.filter((x) => !x.ok))}`);
    });
  }
  assert.equal((await check('qs1')).ok, true);
});
await ta('[C5] 照合の回: NE の元のコードの回が先に進んだ (封なし) / 取得が 36 時間より古い / 観測が無い / NE に無い / 信用できない / 区分が違う', async () => {
  await rolledBack(async () => {
    const run = runIdAt(new Date());
    await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, now(), 0)`, [run]);
    await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: [] })]);
    const c = await check('q01');
    assert.deepEqual(bad(c), ['ne_observed', 'ne_run']);
    assert.match(itemOf(c, 'ne_run').text, /NE の元のコードの回/);
  });
  await rolledBack(async () => {
    await sealRun({ fetchAgoMs: 37 * 3600e3, at: new Date(Date.now() + 1000) });
    const c = await check('q01');
    assert.deepEqual(bad(c), ['ne_observed', 'ne_run']);
    assert.match(itemOf(c, 'ne_run').text, /36 時間より古い/);
  });
  for (const [o, keys, fix, re] of [
    [{ present: false, trusted: true, kind: null }, ['ne_observed'], 'one', /NE の完全な取得に無い/],
    [(b) => ({ ...b, trusted: false }), ['ne_observed'], 'after_compare', /信用できない/],
    [{ code_norm: 'q01', present: true, trusted: true, kind: 'set', cols: { name: { st: 'ok', v: 'x' }, price: { st: 'ok', v: 1000 } }, children: [] }, ['ne_observed'], 'one', /区分が NE/],
  ]) {
    await rolledBack(async () => {
      await sealRun({ obs: { q01: typeof o === 'function' ? o : { code_norm: 'q01', ...o } }, at: new Date(Date.now() + 1000) });
      const c = await check('q01');
      assert.deepEqual(bad(c), keys, JSON.stringify(c.items.filter((x) => !x.ok)));
      assert.equal(itemOf(c, 'ne_observed').fix, fix);
      assert.match(itemOf(c, 'ne_observed').text, re);
    });
  }
  // 確かめ待ちに入る前に封をした回 (観測が無い) = 明日の照合の後に
  await rolledBack(async () => {
    const pid = (await one(`insert into core.products (company_id, display_code, name, status, sales_class) values (1, 'q99', 'q99', 'active', 3) returning product_id`)).product_id;
    await q(`insert into core.skus (company_id, product_id, code, sku_kind, name, tax_rate, tax_class, handling, standard_price_jpy, reorder_months) values (1, $1, 'q99', 'single', '後から見つけた', 0.1, 'STANDARD_10', 'active', 1000, 1)`, [pid]);
    assert.equal((await one(`select ops.quarantine_unregistered_skus('t') as n`)).n, 1);
    const c = await check('q99');
    assert.ok(bad(c).includes('ne_observed'));
    assert.equal(itemOf(c, 'ne_observed').fix, 'after_compare');
    assert.match(itemOf(c, 'ne_observed').text, /観測が無い/);
  });
});
await ta('[C6] 判断の候補 (Q3 a): まだ決めていない = decision / 承認 (差を残す・NE を直す・社内を直す) = ok / 却下・取り消し = だめ / 直す承認の完了の後にまた出た = だめ / 写した (今の社内 = 観測) = ok / 列に当たらない候補 = ne.other / 照合の後・承認の後に値を変えた = だめ (#1715 R1 High)・承認の後に NE の値だけ変わった (単品・構成の子)・今日の原価と将来の原価 = だめ (R2 High)', async () => {
  // q03: NE の原価 280・社内 300 = 照合 ② が原価の差を候補に (company_owned)
  const cc = cand('q03', 'cost', { c: 300, n: 280 });
  const neCost = (v) => (b) => ({ ...b, cols: { ...b.cols, cost: { st: 'ok', v } } });
  await rolledBack(async () => {
    await sealRun({ obs: { q03: neCost(280) }, decisions: [cc], at: new Date(Date.now() + 1000) });
    let c = await check('q03');
    assert.deepEqual(bad(c), ['ne.cost']);
    assert.equal(itemOf(c, 'ne.cost').fix, 'decision');
    assert.match(itemOf(c, 'ne.cost').text, /マスタの判断で決める: cost \(company_owned・まだ決めていない\)/);
    assert.deepEqual([itemOf(c, 'ne.cost').cdb, itemOf(c, 'ne.cost').ne], [300, { st: 'ok', v: 280 }]);
    for (const res of ['accept_difference', 'fix_ne', 'fix_cdb']) {
      await pg.query('savepoint s');
      const ev = await approve(cc.fingerprint, res, res === 'accept_difference' ? null : { subject_key: cc.subject_key, col: 'cost', child: null, value: 300 });
      c = await check('q03');
      assert.equal(c.ok, true, `${res}: ${JSON.stringify(c.items.filter((x) => !x.ok))}`);
      assert.deepEqual(c.evidence.decisions, [{ fingerprint: cc.fingerprint, col: 'cost', child: null, how: 'approved', event_id: String(ev), resolution: res }]);
      assert.match(itemOf(c, 'ne.cost').text, /今の社内の値でマスタの判断で決めてある/);
      // 承認の後に社内の値を第三の値 (290) に変えた = 承認は効かない (承認した差は 300 ↔ 280) = 明日の照合の後に
      await pg.query('savepoint s3');
      await setCost('q03', 290);
      const c3 = await check('q03');
      assert.deepEqual(bad(c3), ['ne.cost'], res);
      assert.match(itemOf(c3, 'ne.cost').text, /承認の後に社内の値が変わった \(照合の時 300・今 290\)/);
      assert.equal(itemOf(c3, 'ne.cost').fix, 'after_compare');
      await pg.query('rollback to savepoint s3');
      // #1715 Codex R2 High 1: 承認のまま、次の回で NE の値だけ 280 → 270 (同じ候補 = 同じ指紋が次の回にも見えた) = 承認は効かない
      await pg.query('savepoint s4');
      await sealRun({ obs: { q03: neCost(270) }, decisions: [cc], at: new Date(Date.now() + 1500) });
      const c4 = await check('q03');
      assert.deepEqual(bad(c4), ['ne.cost'], res);
      assert.match(itemOf(c4, 'ne.cost').text, /承認の後に NE の値が変わった \(承認の時 280・今 270\)/);
      assert.equal(itemOf(c4, 'ne.cost').fix, 'after_compare');
      await pg.query('rollback to savepoint s4');
      if (res === 'fix_ne') {
        // 直す承認の完了 (action_done) の後にまた同じ差が出た = 再発 = だめ
        await q(`insert into ops.master_decision_events (fingerprint, kind, approved_event_id, actor_type, actor, observed) values ($1, 'action_done', $2, 'system', 'mc_x', '{}'::jsonb)`, [cc.fingerprint, ev]);
        c = await check('q03');
        assert.deepEqual(bad(c), ['ne.cost']);
        assert.match(itemOf(c, 'ne.cost').text, /直す承認の完了の後にまた出た/);
      }
      await pg.query('rollback to savepoint s');
    }
    for (const kind of ['rejected', 'revoked']) {
      await pg.query('savepoint s');
      await approve(cc.fingerprint, 'accept_difference');
      await q(`insert into ops.master_decision_events (fingerprint, kind, actor_type, actor) values ($1, $2, 'user', 'naka@test')`, [cc.fingerprint, kind]);
      assert.deepEqual(bad(await check('q03')), ['ne.cost'], kind);
      await pg.query('rollback to savepoint s');
    }
    // 解決が spec / fix_input は数えない
    await pg.query('savepoint s');
    const cs = cand('q03', 'cost', { c: 300, n: 280, tag: 'spec', resolutions: ['spec'] });
    const lastRun = (await one('select compare_run_id from ops.ne_reg_compare_receipts order by sealed_at desc limit 1')).compare_run_id;
    await as('watch_writer', () => pg.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: lastRun, observed_at: new Date().toISOString(), decisions: [cs] })]));
    await approve(cs.fingerprint, 'spec');
    assert.match(itemOf(await check('q03'), 'ne.cost').text, /解決 spec はこの確認では数えない/);
    await pg.query('rollback to savepoint s');
    // 社内の値を NE の値に写した (今の社内 280 = 観測 280) = ok (明日の照合で候補は消える)・違う値 (281) = だめ
    await setCost('q03', 280);
    c = await check('q03');
    assert.equal(c.ok, true, JSON.stringify(c.items.filter((x) => !x.ok)));
    assert.deepEqual(c.evidence.decisions, [{ col: 'cost', how: 'copied' }]);
    assert.match(itemOf(c, 'ne.cost').text, /社内の値を NE の値に合わせた/);
    await setCost('q03', 281);
    assert.deepEqual(bad(await check('q03')), ['ne.cost']);
  });
  // 前の回の候補 (この回に見えていない) は数えない・列に当たらない候補 (exists など) = ne.other
  await rolledBack(async () => {
    const old = cand('q03', 'cost', { c: 300, n: 280, tag: 'old' });
    await sealRun({ obs: { q03: neCost(280) }, decisions: [old], at: new Date(Date.now() + 1000) });
    await approve(old.fingerprint, 'accept_difference');
    await sealRun({ obs: { q03: neCost(280) }, decisions: [cand('q03', 'exists', { c: true, cls: 'reg_stale', reason: 'reg_stale' })], at: new Date(Date.now() + 2000) });
    const c = await check('q03');
    assert.deepEqual(bad(c), ['ne.cost', 'ne.other']);
    assert.match(itemOf(c, 'ne.cost').text, /照合の後に社内の値を変えた|NE \(照合の回/);
    assert.match(itemOf(c, 'ne.other').text, /exists \(reg_stale/);
  });
  // 構成 (components:子): NE の a02 は 2・社内は 1 = 子ごとに今の構成に結びついた承認が要る / 承認の後に社内を 3 にした = だめ
  await rolledBack(async () => {
    const neKids = (b) => ({ ...b, children: b.children.map((x) => (x.code_norm === 'a02' ? { ...x, v: 2 } : x)) });
    const ck = cand('qs1', 'components', { child: 'a02', c: 1, n: 2, reason: 'manual' });
    await sealRun({ obs: { qs1: neKids }, decisions: [ck], at: new Date(Date.now() + 1000) });
    assert.deepEqual(bad(await check('qs1')), ['ne.children']);
    assert.equal(itemOf(await check('qs1'), 'ne.children').fix, 'decision');
    await approve(ck.fingerprint, 'accept_difference');
    assert.equal((await check('qs1')).ok, true, JSON.stringify((await check('qs1')).items.filter((x) => !x.ok)));
    await q('update core.sku_components set qty = 3 where parent_sku_id = $1 and child_sku_id = $2', [await sid('qs1'), await sid('a02')]);
    const c = await check('qs1');
    assert.ok(bad(c).includes('ne.children'));
    assert.match(itemOf(c, 'ne.children').text, /承認の後に社内の値が変わった \(照合の時 1・今 3\)/);
    // #1715 Codex R2 High 1: 承認のまま、次の回で NE の子の数量だけ 2 → 4 (同じ候補が次の回にも見えた) = 承認は効かない
    await q('update core.sku_components set qty = 1 where parent_sku_id = $1 and child_sku_id = $2', [await sid('qs1'), await sid('a02')]);
    assert.equal((await check('qs1')).ok, true);
    await pg.query('savepoint r2');
    await sealRun({ obs: { qs1: (b) => ({ ...b, children: b.children.map((x) => (x.code_norm === 'a02' ? { ...x, v: 4 } : x)) }) }, decisions: [ck], at: new Date(Date.now() + 2000) });
    const c4 = await check('qs1');
    assert.deepEqual(bad(c4), ['ne.children']);
    assert.match(itemOf(c4, 'ne.children').text, /承認の後に NE の値が変わった \(承認の時 2・今 4\)/);
    assert.equal(itemOf(c4, 'ne.children').fix, 'after_compare');
    await pg.query('rollback to savepoint r2');
    // 照合の後に子を足した (候補が無い) = だめ
    await q('update core.sku_components set qty = 1 where parent_sku_id = $1 and child_sku_id = $2', [await sid('qs1'), await sid('a02')]);
    await q(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, sort_order, source) values (1, $1, $2, 1, 9, 'ne')`, [await sid('qs1'), await sid('a03')]);
    const c2 = await check('qs1');
    assert.ok(bad(c2).includes('ne.children'));
    assert.match(itemOf(c2, 'ne.children').text, /a03/);
  });
  // #1715 Codex R2 High 2: 今日の原価 500 (期限つき) と将来の原価 300 (期間の終わり無し) が並ぶ。照合 (print.c = 期間の終わりの無い行 300) の承認は今日の 500 に結びつかない
  await rolledBack(async () => {
    const id = await sid('q05');
    await q('delete from core.sku_costs where sku_id = $1', [id]);
    await q(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to, reason, created_by_type, created_by_id)
      values (1, $1, 500, 'manual', 'COMPLETE', $2::date, $2::date + 30, 't', 'human', 't'), (1, $1, 300, 'manual', 'COMPLETE', $2::date + 31, null, 't', 'human', 't')`, [id, TODAY]);
    const cf = cand('q05', 'cost', { c: 300, n: 280 });
    await sealRun({ obs: { q05: neCost(280) }, decisions: [cf], at: new Date(Date.now() + 1000) });
    await approve(cf.fingerprint, 'accept_difference');
    const c = await check('q05');
    assert.deepEqual(bad(c), ['ne.cost'], JSON.stringify(c.items.filter((x) => !x.ok)));
    assert.equal(itemOf(c, 'ne.cost').cdb, 500);
    assert.match(itemOf(c, 'ne.cost').text, /承認の後に社内の値が変わった \(照合の時 300・今 500\)/);
    assert.equal(itemOf(c, 'cost').ok, true);
  });
  // 照合の後に社内の値を NE と違う値に変えた (その回に候補が無い) = 判断なしでは通さない (#1715 Codex R1 High)
  for (const [mut, key] of [[() => setSku('q04', { name: '照合の後の名前' }), 'ne.name'], [() => setSku('q04', { standard_price_jpy: 1234 }), 'ne.price'],
    [() => setSku('q04', { tax_rate: 0.08, tax_class: 'REDUCED_8' }), 'ne.tax_rate'], [() => setSku('q04', { handling: 'discontinued' }), 'ne.handling'], [() => setCost('q04', 999), 'ne.cost']]) {
    await rolledBack(async () => {
      await mut();
      const c = await check('q04');
      assert.deepEqual(bad(c), [key], mut.toString());
      assert.equal(itemOf(c, key).fix, 'after_compare');
      assert.match(itemOf(c, key).text, /照合の後に社内の値を変えた/);
    });
  }
});
await ta('[C7] 代表 (まとまり): 親のある子 = まとまりの記録 (予約) が要る / 親の無い単品で NE に代表がある = 先に代表の採用 / 自分自身の代表 = なしと同じ', async () => {
  await rolledBack(async () => {
    await pg.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
    await q(`update core.products set parent_product_id = $1, parent_set_by = 'load' where product_id = $2`, [await pidOf('grp'), await pidOf('q04')]);
    let c = await check('q04');
    assert.ok(bad(c).includes('parent'));
    assert.match(itemOf(c, 'parent').text, /まとまりの記録 \(予約\) が無い/);
    await pg.query("select set_config('ops.variation_protocol', '1', true)");
    await q(`insert into ops.variation_group_codes (company_id, code_norm, code, group_product_id, source, reserved_by) values (1, 'grp', 'grp', $1, 'load', 't')`, [await pidOf('grp')]);
    c = await check('q04');
    assert.equal(itemOf(c, 'parent').ok, true);
    assert.match(itemOf(c, 'parent').text, /まとまり grp の子/);
  });
  const neParent = (v) => (b) => ({ ...b, cols: { ...b.cols, parent: { st: 'ok', v } } });
  await rolledBack(async () => {
    await sealRun({ obs: { q05: neParent('grp'), q06: neParent('q06') }, at: new Date(Date.now() + 1000) });
    const c = await check('q05');
    assert.ok(bad(c).includes('parent'));
    assert.match(itemOf(c, 'parent').text, /先に代表の採用/);
    assert.equal(itemOf(await check('q06'), 'parent').ok, true);
    assert.equal(itemOf(await check('q06'), 'ne.parent').ok, true);   // NE の「自分自身」= 親なし (照合 ② と同じ)
  });
});
await ta('[C8] 生きている NE 登録の CSV の品目 = live_file・check_hash は値が変われば変わる (推奨保有月数・照合の回)', async () => {
  await rolledBack(async () => {
    await pg.query('alter table ops.ne_reg_export_items disable trigger trg_ne_reg_items_parent_gate');
    await pg.query(`with e as (insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
        values ('products', 'ne-reg-single-v2', 'syohin_code', 'utf8', false, 1, 1, repeat('a', 64), repeat('b', 64), repeat('c', 64), '\\x00', gen_random_uuid(), 'x', current_date, 't') returning export_id)
      insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by, state)
      select e.export_id, k.sku_id, k.code_norm, k.code, k.sku_kind, repeat('e', 64), '{}'::jsonb, repeat('f', 64), 1, 1, 't', 'built' from e, core.skus k where k.code = 'q01'`);
    const c = await check('q01');
    assert.deepEqual(bad(c), ['live_file']);
    assert.match(itemOf(c, 'live_file').text, /built/);
  });
  const h0 = (await check('q01')).check_hash;
  await rolledBack(async () => { await setSku('q01', { reorder_months: 3 }); assert.notEqual((await check('q01')).check_hash, h0); });
  await rolledBack(async () => { await sealRun({ at: new Date(Date.now() + 1000) }); assert.notEqual((await check('q01')).check_hash, h0); });
  assert.equal((await check('q01')).check_hash, h0);
});
await ta('[C9] 一覧: null = 要確認の全部 (コードの順) / コード = 無いコードも・500 件まで', async () => {
  const all = (await one('select ops.quarantine_confirm_list(null) as r')).r;
  assert.deepEqual(all.map((x) => x.code), [...Q_SINGLES, ...Q_SETS].sort());
  const got = (await one('select ops.quarantine_confirm_list($1::text[]) as r', [['Q01', 'nope', 'q01', 'a01']])).r;
  assert.deepEqual(got.map((x) => [x.code, x.ok, bad(x).includes('state')]), [['a01', false, true], ['nope', false, true], ['q01', true, false]]);
  const e = await errOf(() => pg.query('select ops.quarantine_confirm_list($1::text[])', [Array.from({ length: 501 }, (_, i) => `c${i}`)]));
  assert.match(e?.message ?? '', /invalid_input/);
});

await ta('[C10] 一覧 (null) は要確認が 500 件を超えたら too_many (#1715 Codex R1 M3)・コードを渡せば読める', async () => {
  await rolledBack(async () => {
    await q(`with p as (insert into core.products (company_id, display_code, name, status, sales_class) select 1, 'bulk' || g, 'bulk' || g, 'active', 3 from generate_series(1, 495) g returning product_id, display_code)
      insert into core.skus (company_id, product_id, code, sku_kind, name, tax_rate, tax_class, handling, standard_price_jpy) select 1, p.product_id, p.display_code, 'single', p.display_code, 0.1, 'STANDARD_10', 'active', 100 from p`);
    assert.equal((await one(`select ops.quarantine_unregistered_skus('t') as n`)).n, 495);
    assert.equal((await one(`select count(*)::int as n from ops.master_registrations where state = 'quarantined'`)).n, 504);   // 9 + 495
    const e = await errOf(() => pg.query('select ops.quarantine_confirm_list(null)'));
    assert.match(e?.message ?? '', /^too_many/);
    assert.equal((await one('select jsonb_array_length(ops.quarantine_confirm_list($1::text[])) as n', [['bulk1', 'bulk2']])).n, 2);
  });
  await rolledBack(async () => {
    await q(`with p as (insert into core.products (company_id, display_code, name, status, sales_class) select 1, 'bulk' || g, 'bulk' || g, 'active', 3 from generate_series(1, 491) g returning product_id, display_code)
      insert into core.skus (company_id, product_id, code, sku_kind, name, tax_rate, tax_class, handling, standard_price_jpy) select 1, p.product_id, p.display_code, 'single', p.display_code, 0.1, 'STANDARD_10', 'active', 100 from p`);
    await one(`select ops.quarantine_unregistered_skus('t') as n`);
    assert.equal((await one('select jsonb_array_length(ops.quarantine_confirm_list(null)) as n')).n, 500);   // ちょうど 500 = 読める
  });
});
await ta('[C11] コードが 80 字を超える要確認 = code_length (照合できない・直し方)・確かめ待ちには入らない (#1715 Codex R1 L1)', async () => {
  await rolledBack(async () => {
    const code = `l${'x'.repeat(80)}`;
    const pid = (await one(`insert into core.products (company_id, display_code, name, status, sales_class) values (1, $1, 'long', 'active', 3) returning product_id`, [code])).product_id;
    await q(`insert into core.skus (company_id, product_id, code, sku_kind, name, tax_rate, tax_class, handling, standard_price_jpy, reorder_months) values (1, $1, $2, 'single', '長いコード', 0.1, 'STANDARD_10', 'active', 1000, 1)`, [pid, code]);
    await one(`select ops.quarantine_unregistered_skus('t') as n`);
    assert.equal((await one(`select count(*)::int as n from ops.v_ne_reg_targets where code_norm = $1`, [code])).n, 0);
    const c = await check(code);
    assert.ok(bad(c).includes('code_length'));
    assert.equal(itemOf(c, 'code_length').fix, 'one');
    assert.match(itemOf(c, 'code_length').text, /81 字 \(80 字を超える\) = 照合 ② で NE と照らせない/);
    assert.equal(itemOf(await check('q01'), 'code_length')?.ok ?? true, true);
  });
});
await ta('[V2] 生きている NE 登録の CSV の品目を持つ要確認の SKU は確かめ待ちに 1 行だけ (品目の行・#1715 Codex R1 M2)', async () => {
  await rolledBack(async () => {
    await pg.query('alter table ops.ne_reg_export_items disable trigger trg_ne_reg_items_parent_gate');
    await pg.query(`with e as (insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
        values ('products', 'ne-reg-single-v2', 'syohin_code', 'utf8', false, 1, 1, repeat('a', 64), repeat('b', 64), repeat('c', 64), '\\x00', gen_random_uuid(), 'x', current_date, 't') returning export_id)
      insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by, state)
      select e.export_id, k.sku_id, k.code_norm, k.code, k.sku_kind, repeat('e', 64), '{}'::jsonb, repeat('f', 64), 1, 1, 't', 'built' from e, core.skus k where k.code = 'q05'`);
    await q(`update ops.ne_reg_export_items set state = 'issued', state_changed_by = 't' where code_norm = 'q05'`);   // 作る = built → 配った = issued (生きている品目)
    const rows = await q(`select item_id, state from ops.v_ne_reg_targets where code_norm = 'q05'`);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].state, 'issued');
    assert.ok(rows[0].item_id != null);
  });
  assert.equal((await one(`select count(*)::int as n from ops.v_ne_reg_targets where code_norm = 'q05'`)).n, 1);
});

// ═══════════ [F] 確認済みにする ═══════════
console.log('[F] 確認済みにする');
await ta('[F1] 画面のロール: 確認済み (ne_confirmed)・登録の状態の履歴 (人・理由・request_id・根拠 = DB が作った確かめの表の根拠)・保存の記録 done・押し直し = 前の答え・確かめ待ちから外れる', async () => {
  const c = await check('q01');
  const rid = crypto.randomUUID();
  const r = await confirm('q01', { rid, reason: '要確認の確認: NE と合っている', hash: c.check_hash });
  assert.deepEqual([r.ok, r.code, r.from, r.state, r.check_hash], [true, 'q01', 'quarantined', 'ne_confirmed', c.check_hash]);
  assert.equal(await regOf('q01'), 'ne_confirmed');
  const ev = await one(`select from_state, to_state, actor_type, actor_id, reason, request_id, evidence from ops.master_registration_events where sku_id = $1 order by event_id desc limit 1`, [await sid('q01')]);
  assert.deepEqual([ev.from_state, ev.to_state, ev.actor_type, ev.actor_id, ev.reason, ev.request_id], ['quarantined', 'ne_confirmed', 'human', 'naka@test', '要確認の確認: NE と合っている', rid]);
  assert.deepEqual(ev.evidence, c.evidence);
  assert.equal(ev.evidence.compare_run_id, RUN_A.run);
  const mer = await one(`select operation, status, target_code, sku_id::text as sku_id, actor_id from ops.master_edit_requests where request_id = $1`, [rid]);
  assert.deepEqual(mer, { operation: 'quarantine_confirm', status: 'done', target_code: 'q01', sku_id: await sid('q01'), actor_id: 'naka@test' });
  const ses = await one(`select operation, versions from ops.master_write_sessions where request_id = $1`, [rid]);
  assert.deepEqual([ses.operation, ses.versions.check_hash], ['quarantine_confirm', c.check_hash]);
  const again = await confirm('q01', { rid, reason: '要確認の確認: NE と合っている', hash: c.check_hash });
  assert.equal(again.replayed, true); assert.equal(again.event_id, r.event_id);
  assert.equal((await one(`select count(*)::int as n from ops.v_ne_reg_targets where code_norm = 'q01'`)).n, 0);
  // もう要確認でない = 確かめも確認もだめ (一方向)
  assert.deepEqual(bad(await check('q01')).includes('state'), true);
  const e = await errOf(() => confirm('q01'));
  assert.match(e?.message ?? '', /^not_quarantined/);
});
await ta('[F2] 同じ request_id で違う中身 (SKU・理由・表) = request_id_reused / 表が変わった = stale / だめ = not_confirmable (理由を全部) / 形の誤り = invalid_input', async () => {
  const rid = crypto.randomUUID();
  const h = (await check('q02')).check_hash;
  await rolledBack(async () => {
    await confirm('q02', { rid, hash: h });
    assert.match((await errOf(() => confirm('q02', { rid, hash: h, reason: '別の理由' })))?.message ?? '', /^request_id_reused/);
  });
  await rolledBack(async () => {
    await confirm('q02', { rid, hash: h });
    await pg.query('savepoint s');
    assert.match((await errOf(() => confirm('q06', { rid })))?.message ?? '', /^request_id_reused/);
    await pg.query('rollback to savepoint s');
  });
  await rolledBack(async () => {
    await setSku('q02', { reorder_months: 5 });   // 画面で表を開いた後に値が変わった
    // 確認の関数が鍵の後に比べる (状態の関数も約束の check_hash で比べる = 二重の守り。ここは先の関数の文で断ること)
    assert.match((await errOf(() => confirm('q02', { hash: h })))?.message ?? '', /^stale: q02 の確かめの表が画面で見た時と違う/);
  });
  await rolledBack(async () => {
    await setSku('q02', { reorder_months: null });
    const e = await errOf(() => confirm('q02'));
    assert.match(e?.message ?? '', /^not_confirmable: q02 は確認済みにできない \(推奨保有月数が空/);
  });
  assert.match((await errOf(() => confirm('q02', { hash: 'x' })))?.message ?? '', /^invalid_input/);
  assert.match((await errOf(() => confirm('q02', { reason: 'a\nb' })))?.message ?? '', /^invalid_input/);
  assert.match((await errOf(() => confirm('q02', { reason: 'あ'.repeat(201) })))?.message ?? '', /^invalid_input/);
  assert.match((await errOf(() => confirm('q02', { own: JSON.stringify({ ...ALL, 'skus.name': 'load' }) })))?.message ?? '', /^before_cutover/);
  await rolledBack(async () => {
    await setComp('qs1', [['a01', 2], ['q03', 1]]);
    assert.match((await errOf(() => confirm('qs1')))?.message ?? '', /^not_confirmable: qs1 .*構成品 q03 \(quarantined\) が NE 確認済みでない/);
  });
  assert.equal(await regOf('q02'), 'quarantined');
});
await ta('[F3] 段階が new_open でない = before_cutover (何も書かない)', async () => {
  await rolledBack(async () => {
    await pg.query("select set_config('ops.cutover_protocol', '1', true)");
    await q(`update ops.master_cutover_state set phase = 'company_owner' where id = 1`);
    assert.match((await errOf(() => confirm('q02')))?.message ?? '', /^before_cutover/);
  });
});
await ta('[F4] 構成品を先に確認済みに → セットも確認済みにできる (qs2 = q01 が NE 確認済みになった後)', async () => {
  const s = await check('qs2');
  assert.equal(s.ok, true, JSON.stringify(s.items.filter((x) => !x.ok)));
  const r = await confirm('qs2');
  assert.equal(r.state, 'ne_confirmed');
});

// ═══════════ [T] 直に呼んでも確認済みにならない ═══════════
console.log('[T] 状態の関数');
await ta('[T1] ops.transition_sku_registration を直に呼ぶ (運用のロール master_ops・持ち主・呼び手の根拠つき) = confirm_session_required・状態は変わらない', async () => {
  const id = await sid('q02');
  const e1 = await errOf(() => as('master_ops', () => pg.query(`select ops.transition_sku_registration($1, 'ne_confirmed', 'human', 'naka@test', 'x', '{}'::jsonb, null)`, [id])));
  assert.match(e1?.message ?? '', /^confirm_session_required/);
  const e2 = await errOf(() => pg.query(`select ops.transition_sku_registration($1, 'ne_confirmed', 'system', 'ne_compare', null, '{}'::jsonb, null)`, [id]));
  assert.match(e2?.message ?? '', /^confirm_session_required/);
  const e3 = await errOf(() => pg.query(`select ops.transition_sku_registration($1, 'ne_confirmed', 'human', 'naka@test', null, '{"compare_run_id": "x"}'::jsonb, null)`, [id]));
  assert.match(e3?.message ?? '', /^confirm_session_required/);
  assert.equal(await regOf('q02'), 'quarantined');
});
await ta('[T2] 約束はあるが中身が違う (別の SKU・別の操作・別の人・system・呼び手の根拠・check_hash が違う・確かめがだめ) = 断る', async () => {
  const id2 = await sid('q02'); const id6 = await sid('q06');
  const h2 = (await check('q02')).check_hash;
  /** 約束を持ち主の手で置いて (試験だけ・表は追記だけ) transition を呼ぶ */
  const withSession = async ({ op = 'quarantine_confirm', sku = id2, actor = 'naka@test', hash = null, rid = crypto.randomUUID(), before = null, call }) => rolledBack(async () => {
    if (before) await before();
    hash ??= before ? (await check('q02')).check_hash : h2;
    const sess = crypto.randomUUID();
    await q(`insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions, actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
      values ($1, txid_current(), $2, $3, $4, '{}', '{}', repeat('0', 64), repeat('1', 64), $5::jsonb, $6, null, 'portal_master_edit', 'deploy', 'new_open', ops.ownership_hash($7::jsonb), $7::jsonb)`,
      [sess, rid, op, sku, JSON.stringify({ sku_id: String(sku), check_hash: hash }), actor, OWN]);
    await pg.query("select set_config('ops.master_write_session', $1, true)", [sess]);
    return errOf(() => pg.query(call.sql, call.p(rid)));
  });
  const tr = (sku, type = 'human', actor = 'naka@test', ev = '{}') => ({ sql: `select ops.transition_sku_registration($1, 'ne_confirmed', $2, $3, null, $4::jsonb, $5)`, p: (rid) => [sku, type, actor, ev, rid] });
  assert.match((await withSession({ sku: id6, call: tr(id2) }))?.message ?? '', /^confirm_session_required/);
  assert.match((await withSession({ op: 'sku_gone_cancel', call: tr(id2) }))?.message ?? '', /^confirm_session_required/);
  assert.match((await withSession({ actor: 'other@test', call: tr(id2) }))?.message ?? '', /^confirm_session_required/);
  assert.match((await withSession({ call: tr(id2, 'system') }))?.message ?? '', /^confirm_session_required/);
  assert.match((await withSession({ call: { sql: tr(id2).sql, p: () => [id2, 'human', 'naka@test', '{}', crypto.randomUUID()] } }))?.message ?? '', /^confirm_session_required/);
  assert.match((await withSession({ call: tr(id2, 'human', 'naka@test', '{"x": 1}') }))?.message ?? '', /^caller_evidence/);
  assert.match((await withSession({ hash: hex('a'), call: tr(id2) }))?.message ?? '', /^stale/);
  // 確かめがだめ (約束の check_hash は今の表と同じ = 確かめの ok だけで断る)
  const e = await withSession({ before: () => setSku('q02', { reorder_months: null }), call: tr(id2) });
  assert.match(e?.message ?? '', /^not_confirmable/);
  assert.equal(await regOf('q02'), 'quarantined');
});
await ta('[T3] ほかの道は今までどおり: 要確認 → やめた (人の理由) / distributable・available = not_ready / NE 確認済み → やめた = one_way', async () => {
  await rolledBack(async () => {
    const id = await sid('q07');
    const r = (await as('master_ops', () => pg.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', 'NE で消した', '{}'::jsonb, null) as r`, [id]))).rows[0].r;
    assert.equal(r.to, 'cancelled');
  });
  const id1 = await sid('q01');
  const e1 = await errOf(() => as('master_ops', () => pg.query(`select ops.transition_sku_registration($1, 'available', 'human', 'naka@test', null, '{}'::jsonb, null)`, [id1])));
  assert.match(e1?.message ?? '', /^(one_way|not_ready)/);
  const e2 = await errOf(() => as('master_ops', () => pg.query(`select ops.transition_sku_registration($1, 'distributable', 'system', 'x', null, '{}'::jsonb, null)`, [id1])));
  assert.match(e2?.message ?? '', /^not_ready/);
  const e3 = await errOf(() => as('master_ops', () => pg.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', 'x', '{}'::jsonb, null)`, [id1])));
  assert.match(e3?.message ?? '', /^one_way/);
});

// ═══════════ [A] 確認済みの後 ═══════════
console.log('[A] 確認済みの後');
await ta('[A1] 新しいセットの構成品に使える (ops.new_set_derivation が断らない)・要確認のままの商品は今までどおり断る', async () => {
  const rows = async (code) => JSON.stringify([{ sku_id: Number(await sid(code)), code, qty: 1, sort: 1 }]);
  const ok = await rolledBack(async () => (await one(`select ops.new_set_derivation($1::jsonb, 'active', $2::date) as r`, [await rows('q01'), TODAY])).r);
  assert.equal(Number(ok.tax_rate), 0.1);
  assert.equal(ok.cost_jpy, 300);
  const r2 = await rows('q02');
  const e = await rolledBack(() => errOf(() => pg.query(`select ops.new_set_derivation($1::jsonb, 'active', $2::date)`, [r2, TODAY])));
  assert.match(e?.message ?? '', /component_unusable: .*q02 \(状態 quarantined\)/);
});
await ta('[A2] NE 登録の CSV の構成品 (ops.ne_reg_canonical のセット): 確認済みの構成品 = 止めない / 要確認の構成品 = 止める', async () => {
  const blockers = async (code) => (await one('select ops.ne_reg_canonical($1, $2::date) as r', [await sid(code), TODAY])).r.blockers;
  assert.ok(!(await blockers('qs2')).some((b) => /NE 確認済みでない/.test(b)));
  await rolledBack(async () => {
    await setComp('qs1', [['a01', 2], ['q03', 1]]);
    assert.ok((await blockers('qs1')).some((b) => /構成品 q03 が NE 確認済みでない/.test(b)));
  });
});

// ═══════════ [R] ロール ═══════════
console.log('[R] ロール');
await ta('[R1] master_edit = 確認・確かめ・一覧 / watcher = 確かめ・一覧 (読むだけの取引でも) だけ / master_ops = 確認は 42501 / 部品 (表の 1 行) はだれも呼べない', async () => {
  for (const f of QUARANTINE_CONFIRM_FUNCTIONS) assert.equal((await one(`select has_function_privilege('master_edit', $1, 'execute') as ok`, [f])).ok, true, f);
  for (const f of QUARANTINE_CONFIRM_OWNER_ONLY_FUNCTIONS) {
    for (const r of ['master_edit', 'master_ops', 'watcher', 'watch_writer', 'public']) assert.equal((await one(`select has_function_privilege($2, $1, 'execute') as ok`, [f, r])).ok, false, `${r} ${f}`);
  }
  assert.equal((await one(`select has_function_privilege('watcher', 'ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text)', 'execute') as ok`)).ok, false);
  assert.equal((await one(`select has_function_privilege('master_ops', 'ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text)', 'execute') as ok`)).ok, false);
  assert.equal((await one(`select has_function_privilege('public', 'ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text)', 'execute') as ok`)).ok, false);
  const w = await as('watcher', async () => {
    await pg.query('begin read only');
    try { return [(await one('select ops.quarantine_confirm_check($1::bigint) as r', [await sid('q02')])).r.ok, (await one('select jsonb_array_length(ops.quarantine_confirm_list(null)) as n')).n]; }
    finally { await pg.query('rollback'); }
  });
  assert.deepEqual(w, [true, 7]);   // q01・qs2 は確認済み = 要確認は 9 - 2
  const id2 = await sid('q02');
  const e = await errOf(() => as('master_ops', () => pg.query('select ops.confirm_quarantined_sku($1::uuid, $2, null, $3::jsonb, $4::bigint, $5)', [crypto.randomUUID(), 'x', OWN, id2, hex('a')])));
  assert.equal(e?.code, '42501');
  // 流し直しても付いたまま (create-watch-roles.mjs / create-master-edit-roles.mjs)
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  assert.equal((await one(`select has_function_privilege('watcher', 'ops.quarantine_confirm_check(bigint)', 'execute') as ok`)).ok, true);
  assert.equal((await one(`select has_function_privilege('master_edit', 'ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text)', 'execute') as ok`)).ok, true);
});
await ta('[R2] 0078 の security definer の関数 = search_path は pg_catalog, pg_temp・public の実行権なし・一時の表を作らない', async () => {
  const rows = await q(`select p.proname, p.prosecdef, p.proconfig, has_function_privilege('public', p.oid, 'execute') as pub, p.prosrc ~* 'create\\s+(local\\s+)?temp' as tmp
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname in ('quarantine_confirm_check', 'quarantine_confirm_list', 'confirm_quarantined_sku', 'quarantine_confirm_item', 'transition_sku_registration')`);
  assert.equal(rows.length, 5);
  for (const r of rows) {
    assert.deepEqual(r.proconfig, ['search_path=pg_catalog, pg_temp'], r.proname);
    assert.equal(r.pub, false, r.proname); assert.equal(r.tmp, false, r.proname);
    if (r.proname !== 'quarantine_confirm_item') assert.equal(r.prosecdef, true, r.proname);
  }
});

// ═══════════ [H] 重い入口の棚卸し ═══════════
console.log('[H] 重い入口');
await ta('[H1] 全部の migration を流した DB で heavyEntryFindings が空・0078 の関数は manifest に分けてある (since = 作った migration の番号)', async () => {
  assert.deepEqual(await heavyEntryFindings(db), []);
  for (const f of ['ops.quarantine_confirm_check(bigint)', 'ops.quarantine_confirm_list(text[])', 'ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text)', 'ops.quarantine_confirm_item(text, boolean, jsonb, jsonb, text, text)']) {
    const e = HEAVY_ENTRY_MANIFEST.find((x) => x.sig === f);
    assert.ok(e, `manifest に無い: ${f}`);
    assert.equal(e.since, THIS_V, f);
  }
  for (const e of HEAVY_ENTRY_MANIFEST.filter((x) => x.since)) {
    const file = migFiles.find((f) => f.startsWith(`${e.since}_`) && fnText(readMig(f), e.sig.slice(0, e.sig.indexOf('('))));
    assert.ok(file, `manifest の since ${e.since} の migration に ${e.sig} が無い (番号を付け替えたら since も)`);
  }
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
