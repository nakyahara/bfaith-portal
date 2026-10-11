/**
 * test-master-gone.mjs — 業務で使える商品を「NE から消えた」でやめる (0070・中原さんの決定 10/11 Q1 = a) を PGlite で確かめる
 *
 * 固定する契約:
 *   [M]  migration の写し: ops.record_parent_gate (0068)・ops.transition_sku_registration (0063)・ops.registration_transition_allowed (0052) は、
 *        前の最新の版から写して「🆕 0070」の行だけ足した (印の行を外すと前の版と 1 字も違わない)・約束の操作の CHECK = 0067 の全部 + sku_gone_cancel・
 *        security definer の関数は search_path = pg_catalog, pg_temp・public の実行権なし
 *   [A]  続けて無い日数 (ops.master_ne_absence・#1683 Codex R1 High 1): 取得の時刻の順に数える・JST の取得の日ごとに最大 1 (3 回 = 3 日)・ある取得で消える・
 *        untrusted は無いに数えない・古い取得を後から記録 / 同じ取得の照合し直し = 何も変えない・記録しなかった回 (信用できない取得) は変えない・表は記録の関数の中でだけ書ける
 *   [C]  確かめ (ops.sku_gone_from_ne_check): 条件の 1 つ 1 つ (available・単品・日数・取得の完了の時刻の新しさ・数えた一番新しい取得・NE のコードの台帳・CSV の品目・
 *        セットの構成品・構成の依頼・出品の構成・ほかの親) と ok の根拠
 *   [F]  動かす関数 (ops.cancel_sku_gone_from_ne): 画面のロール = やめる・記録 (保存の記録 done・登録の状態の履歴の根拠 = DB が作った値)・押し直し = 前の答え・
 *        同じ番号で違う理由 = request_id_reused・理由が要る・条件を満たさない = not_gone_from_ne・段階が new_open でない = before_cutover
 *   [T]  ops.transition_sku_registration を直に呼んでも available はやめられない (約束が要る・呼び手の根拠は受けない)・下書きのやめる (今までの道) は変わらない
 *   [I]  やめた SKU への参照を新しく作らない (#1683 Codex R1 High 2): 構成の子・出品の構成・構成の依頼・代表 (親) = どの書き手も断る・参照を変えない更新は通す
 *   [L]  夜間ロード (同じ材料でもう一度・NE から作った SKU の quarantined) は cancelled を戻さない・0068 の数えは cancelled を数えない (今までどおり)
 *   [R]  ロール: master_edit・master_ops = 動かす・確かめるの実行 / watcher = 確かめだけ / 表と部品はだれも直接触れない
 *   [K]  CLI (scripts/company-db/master-gone-from-ne.mjs・#1683 Codex R1 Medium): 引数・dry-run の計画のハッシュ・ハッシュが違う / 途中で証拠が変わる / 1 件が断られる =
 *        1 つの取引ごと巻き戻す (1 件も変えない)
 *   [S]  画面 (本物の router): 条件を満たす商品だけボタン・名簿 MASTER_DECISION_APPROVERS の人だけ押せる・MASTER_EDIT_OPEN・印・理由・通ったら やめた
 *   [U]  画面の JS (Playwright・Chromium): 理由と印で押せる → 確かめの窓 → やめた → 読み直し (MASTER_EDIT_UI_SKIP=1 で飛ばす)
 * 使い方: node scripts/test-master-gone.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles, GONE_FUNCTIONS, GONE_OWNER_ONLY_FUNCTIONS } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import * as G from '../lib/master-gone.mjs';
import { parseGoneArgs, runGone, readCodesFile } from './company-db/master-gone-from-ne.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIG = path.join(ROOT, 'db', 'company', 'migrations');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);   // 試験の DB は全部の列を company にする = このコードの能力も全部
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'gone-'));
process.env.DATA_DIR = DATA_TMP;

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
/** 関数の定義の文字 (create [or replace] function <name>( ... $$;)。見つからない = null */
function fnText(text, name) {
  const re = new RegExp(`create (?:or replace )?function ${name.replace(/\./g, '\\.')}\\(`);
  const i = text.search(re);
  if (i < 0) return null;
  const k = text.indexOf(' as $$', i);
  const j = text.indexOf('$$;', k + 6);
  return text.slice(i, j + 3).replace(/^create or replace function/, 'create function');
}
/** 「🆕 0070」の印の行 (と ここから〜ここまで) を外す */
function strip0070(t) {
  const out = [];
  let skip = false;
  for (const l of t.split('\n')) {
    if (/-- 🆕 0070 ここから/.test(l)) { skip = true; continue; }
    if (/-- 🆕 0070 ここまで/.test(l)) { skip = false; continue; }
    if (skip || /🆕 0070/.test(l)) continue;
    out.push(l);
  }
  return out.join('\n');
}
/** 0070 より前で、その関数を最後に定義した migration */
const lastDefBefore = (name, before = '0070') => migFiles.filter((f) => f < before && fnText(readMig(f), name)).pop();

console.log('[M] migration の写し');
const M70 = readMig(migFiles.find((f) => f.startsWith('0070_')));
for (const [name, expectFrom] of [['ops.record_parent_gate', '0068'], ['ops.transition_sku_registration', '0063'], ['ops.registration_transition_allowed', '0052']]) {
  await ta(`[M1] ${name} = ${expectFrom} の版 (前の最新) から写して 🆕 0070 の行だけ足した`, async () => {
    const src = lastDefBefore(name);
    assert.ok(src && src.startsWith(expectFrom), `前の最新の版が ${src} (期待 ${expectFrom})`);
    const before = fnText(readMig(src), name);
    const now = fnText(M70, name);
    assert.ok(now, '0070 に無い');
    assert.notEqual(now, before, '何も足していない');
    assert.equal(strip0070(now), before, '印の行を外すと前の版と違う (写し間違い・印の無い変更)');
  });
}
await ta('[M2] 約束の操作の CHECK = 0067 の全部 + sku_gone_cancel (どちらの表も)', async () => {
  const ops = (text, tbl) => {
    const m = new RegExp(`alter table ops\\.${tbl} add constraint ck_\\w+ check \\(operation in \\(([^;]*?)\\)\\);`).exec(text);
    return m ? [...m[1].matchAll(/'([a-z_]+)'/g)].map((x) => x[1]) : null;
  };
  const src = readMig(migFiles.find((f) => f.startsWith('0067_')));
  for (const tbl of ['master_write_sessions', 'master_edit_requests']) {
    const before = ops(src, tbl); const now = ops(M70, tbl);
    assert.ok(before && now, tbl);
    assert.deepEqual(now, [...before, 'sku_gone_cancel'], tbl);
    for (const f of migFiles.filter((x) => x >= '0068' && x < '0070')) assert.equal(ops(readMig(f), tbl), null, `${f} が ${tbl} の CHECK を変えている (写す元が違う)`);
  }
});

// ═══════════ DB の用意 ═══════════
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
async function as(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
const errOf = async (fn) => { try { await fn(); } catch (e) { return e; } return null; };

const single = (code, name = `商品 ${code}`) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
const SINGLES = ['g01', 'g02', 'g03', 'g04', 'g05', 'g06', 'g07', 'g08', 'g09', 'g10', 'g11', 'g12', 'g13', 'g14', 'keep01', 'keep02', 'keep03'];
const PLAN = {
  skus: [...SINGLES.map((c) => single(c)), { ...single('set001'), kind: 'set', cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } }],
  // g06 は keep02 の代表 (親) = ほかの商品の親
  variationGroups: [{ code: 'g06', name: 'まとまり g06', childCodes: ['keep02'], status: 'active' }],
  setComponents: [{ parentCode: 'set001', childCode: 'keep01', qty: 1, source: 'ne' }, { parentCode: 'set001', childCode: 'g04', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: SINGLES.map((c) => ({ supplierCode: '0001', skuCode: c })), primarySuppliers: SINGLES.map((c) => ({ skuCode: c, supplierCode: '0001' })),
  reorder: { available: false, runId: null, reason: '試験' },
};
const lr = await runInitialLoad(db, PLAN, { log: quiet, runId: 'gone_load_1', host: 'test', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
const ALL = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
await forceNewOpen(db, ALL);
const sid = async (code) => String((await one('select sku_id from core.skus where code = $1', [code])).sku_id);
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))?.state ?? null;
// g05 = Amazon の出品の構成に使われている / g07 = 開いている構成の依頼に入っている (どちらも持ち主の直接の書き込み = 試験だけ)
{
  const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', 'pr-g05', 'active') returning listing_id`)).listing_id;
  await q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('g05')]);
  const rows = [{ sku_id: Number(await sid('g07')), code: 'g07', qty: 1, sort: 1 }];
  await q(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id) values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`,
    [await sid('set001'), JSON.stringify(rows), hex('9')]);
}

/** NE の完全な取得の観測 (今の Company DB と同じ = 0068 の試験の fixture と同じ作り) から gone のコードを外す。untrusted = 保持したコード */
async function obsWithout(gone = [], { untrusted = [] } = {}) {
  const rows = await q(`select k.code_norm, k.sku_kind, nullif(core.norm_code(pp.display_code), '') as rep, pp.display_code as raw
      from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
     where k.company_id = 1 and k.sku_kind in ('single', 'set') order by k.code_norm`);
  return { format: 'parent-obs-v1', complete: true, untrusted, rep_spellings: { state: 'ok' },
    trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' },
    rows: rows.filter((r) => !gone.includes(r.code_norm)).map((r) => (r.sku_kind === 'set' ? [r.code_norm, 'set', null, null, null] : [r.code_norm, 'single', 'ok', r.rep ?? null, r.rep ? r.raw : null])) };
}
let runN = 0;
const runId = () => `mc_20301011T${String(++runN).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;
/** 照合 ② の記録を 1 回 (watch_writer = 本番と同じロール・本物の関数。取得の完了 = 今 - 1 秒 = 今日) */
async function gateRun(gone, opts = {}) {
  const when = new Date(Date.now() - 1000).toISOString();
  const f = { generation_id: opts.gen || `ne_${crypto.randomBytes(3).toString('hex')}`, raw_hash: hex('c'), products_complete_at: when, setproducts_complete_at: when };
  const obs = await obsWithout(gone, opts);
  if (opts.mutate) opts.mutate(obs);
  return as('watch_writer', async () => (await pg.query('select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb) as r', [runId(), JSON.stringify(f), 'mat_x', hex('d'), JSON.stringify(obs)])).rows[0].r);
}
/**
 * 取得の日を選んで記録の回を置き、本物の数え (ops._record_ne_absence) を流す (試験だけ: ops.record_parent_gate は取得を今日に限る = 別の日の取得は表に直接置く)。
 *   day = JST の日 (今日 = 0・昨日 = -1 …)。時刻 = その日の 12:00 JST (今日は 今 - 20 分) + 呼んだ順の秒 (= 取得の時刻はいつも前より後)。at = 時刻を直に (古い取得)
 */
let tick = 0;
const jstToday = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
function fetchAt(day) {
  tick++;
  if (day === 0) return new Date(Math.max(Date.now() - 20 * 60e3, Date.parse(`${jstToday()}T00:01:00+09:00`)) + tick * 1000);
  return new Date(Date.parse(`${jstToday()}T12:00:00+09:00`) + day * 86400e3 + tick * 1000);
}
async function feed(gone, { day = 0, at = null, gen = null, untrusted = [] } = {}) {
  const when = (at || fetchAt(day)).toISOString();
  const obs = await obsWithout(gone, { untrusted });
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.parent_gate_protocol', '1', true)");
    const id = (await pg.query(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
        evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by)
      values (1, $1, $2, repeat('1', 64), $3, $3, 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`,
      [runId(), gen || `ne_${crypto.randomBytes(4).toString('hex')}`, when])).rows[0].result_id;
    const n = (await pg.query('select ops._record_ne_absence($1, $2::jsonb) as n', [id, JSON.stringify(obs)])).rows[0].n;
    await pg.query('commit');
    return { id: String(id), n };
  } catch (e) { await pg.query('rollback'); throw e; }
}
const absence = async () => Object.fromEntries((await q('select code_norm, streak from ops.master_ne_absence order by 1')).map((r) => [r.code_norm, r.streak]));
const mark = async () => one('select result_id::text as id, ne_generation_id as gen, fetch_day::text as day from ops.master_ne_absence_mark where id = 1');
const check = async (code) => (await one('select ops.sku_gone_from_ne_check($1::bigint) as r', [await sid(code)])).r;
const reasons = (c) => c.problems.map((p) => p.reason).sort();

// ═══════════ [A] 続けて無い日数 ═══════════
console.log('[A] 続けて無い日数');
const GONE = ['g01', 'g02', 'g03', 'g04', 'g05', 'g06', 'g07', 'g08', 'g10', 'g11', 'g12', 'g13', 'g14', 'keep03', 'set001'];
const GONE2 = [...GONE.filter((c) => c !== 'g03'), 'g09'];   // 3 日目から (g03 は NE にある・g09 が無い)
const FULL = ['g01', 'g02', 'g04', 'g05', 'g06', 'g07', 'g10', 'g11', 'g12', 'g13', 'g14', 'keep03', 'set001'];   // 3 日続けて無い
const after3 = () => ({ ...Object.fromEntries(FULL.map((c) => [c, 3])), g08: 2, g09: 1 });
let RUN1;
await ta('[A1] 取得の日ごとに +1 (3 日 = 3)・ある取得で消える・untrusted は無いに数えない', async () => {
  assert.deepEqual(await absence(), {});
  RUN1 = await feed(GONE, { day: -2, untrusted: ['g08'] });
  assert.deepEqual(await absence(), Object.fromEntries(GONE.filter((c) => c !== 'g08').map((c) => [c, 1])));
  await feed(GONE, { day: -1 });
  assert.deepEqual(await absence(), { ...Object.fromEntries(GONE.filter((c) => c !== 'g08').map((c) => [c, 2])), g08: 1 });
  const r3 = await feed(GONE2, { day: 0 });   // g03 がある取得 = 途切れる・g09 が初めて無い
  assert.deepEqual(await absence(), after3());
  assert.deepEqual(await mark(), { id: r3.id, gen: (await one('select ne_generation_id as g from ops.master_parent_gate_results where result_id = $1', [r3.id])).g, day: jstToday() });
});
await ta('[A2] 同じ日の 2 回目の取得 = 日数は増やさない (続きは切らない・最後の回は進む)', async () => {
  const r = await feed(GONE2, { day: 0 });
  assert.notEqual(r.n, -1);
  assert.deepEqual(await absence(), after3());
  assert.equal((await mark()).id, r.id);
  assert.equal((await one("select last_absent_result_id::text as id from ops.master_ne_absence where code_norm = 'g01'")).id, r.id);
});
await ta('[A3] 古い取得を後から記録 (取得の時刻が数えた取得より前) = 何も変えない (今 NE にある g03 を無いに数えない・g01 を消さない)', async () => {
  const before = await absence(); const m0 = await mark();
  const r = await feed([...GONE, 'g03'].filter((c) => c !== 'g01'), { at: fetchAt(-1) });
  assert.equal(r.n, -1);
  assert.deepEqual(await absence(), before); assert.deepEqual(await mark(), m0);
  const r2 = await feed(GONE, { at: new Date(Date.parse((await one('select products_complete_at::text as t from ops.master_ne_absence_mark')).t)) });   // 同じ時刻 = 後ではない
  assert.equal(r2.n, -1);
  assert.deepEqual(await absence(), before);
});
await ta('[A4] 同じ NE の取得 (世代) を照合し直した = 何も変えない (直前の回でなくても・時刻が後でも)', async () => {
  const before = await absence(); const m0 = await mark();
  const r = await feed(GONE2, { day: 0, gen: (await one('select ne_generation_id as g from ops.master_parent_gate_results where result_id = $1', [RUN1.id])).g });
  assert.equal(r.n, -1);
  assert.deepEqual(await absence(), before); assert.deepEqual(await mark(), m0);
});
await ta('[A5] 本物の照合 ② の記録 (watch_writer・今日の取得) も同じ数えを通る・記録しなかった回 (信用できない取得) は表を変えない', async () => {
  const r = await gateRun(GONE2);
  assert.equal((await mark()).id, r.result_id);
  assert.deepEqual(await absence(), after3());
  const before = await absence();
  const e = await errOf(() => gateRun(['g09'], { mutate: (o) => { o.trust.integrity = 'ne_rows_dropped'; o.complete = false; o.incomplete_reason = 'ne_rows_dropped'; } }));
  assert.match(e?.message ?? '', /ne_untrusted/);
  assert.deepEqual(await absence(), before);
});
await ta('[A6] 表と印は記録の関数の中でだけ書ける (持ち主でも直に書けない・消せない・TRUNCATE できない)', async () => {
  for (const sql of ["insert into ops.master_ne_absence (company_id, code_norm, first_absent_result_id, last_absent_result_id, first_absent_day, last_absent_day, streak) select 1, 'zz', max(result_id), max(result_id), current_date, current_date, 1 from ops.master_parent_gate_results",
    'update ops.master_ne_absence set streak = 99', 'delete from ops.master_ne_absence', 'truncate ops.master_ne_absence',
    "update ops.master_ne_absence_mark set fetch_day = current_date - 9", 'delete from ops.master_ne_absence_mark', 'truncate ops.master_ne_absence_mark']) {
    const e = await errOf(() => pg.query(sql));
    assert.ok(e, `${sql}: 通ってしまった`);
  }
});

// ═══════════ [C] 確かめ ═══════════
console.log('[C] 確かめ');
await ta('[C1] 条件を全部満たす = ok (g01・3 日)・根拠は DB の取得の回と日', async () => {
  const c = await check('g01');
  assert.equal(c.ok, true, JSON.stringify(c.problems));
  assert.equal(c.streak, 3); assert.equal(c.required_runs, 3);
  assert.equal(c.evidence.rule, 'gone_from_ne_v2'); assert.equal(c.evidence.streak_days, 3);
  assert.equal(c.evidence.first_absent.result_id, RUN1.id);
  assert.equal(c.evidence.last_absent.result_id, (await mark()).id);
  assert.equal(c.evidence.last_absent.day, jstToday());
});
await ta('[C2] 理由: 日数が足りない (g08 = 2・g09 = 1) / NE にある (g03・keep01) / セット / セットの構成品 / 出品の構成 / ほかの親 / 構成の依頼', async () => {
  assert.deepEqual(reasons(await check('g08')), ['absent_short']);
  assert.match((await check('g08')).problems[0].text, /2 日/);
  assert.deepEqual(reasons(await check('g09')), ['absent_short']);
  assert.deepEqual(reasons(await check('g03')), ['in_ne_latest']);
  assert.deepEqual(reasons(await check('keep01')), ['in_ne_latest', 'set_component']);
  assert.deepEqual(reasons(await check('set001')), ['not_single']);
  assert.deepEqual(reasons(await check('g04')), ['set_component']);
  assert.match((await check('g04')).problems[0].text, /set001/);
  assert.deepEqual(reasons(await check('g05')), ['listing_component']);
  assert.match((await check('g05')).problems[0].text, /amazon:pr-g05/);
  assert.deepEqual(reasons(await check('g06')), ['parent_of_others']);
  assert.deepEqual(reasons(await check('g07')), ['component_request']);
  assert.equal((await check('g08')).evidence, null);
});
await ta('[C3] 理由: 今の NE のコードの台帳にある・生きた NE 登録の CSV の品目・available でない・数えた取得の完了が 36 時間より古い (取引の中で変えて巻き戻す)', async () => {
  await pg.query('begin');
  try {
    await pg.query(`insert into ops.master_ne_codes (code_norm, kind, state, ne_code, spellings) values ('g01', 'product', 'ok', 'g01', '["g01"]'::jsonb)`);
    assert.deepEqual(reasons(await check('g01')), ['in_ne_codes']);
  } finally { await pg.query('rollback'); }
  await pg.query('begin');
  try {
    await pg.query('alter table ops.ne_reg_export_items disable trigger trg_ne_reg_items_parent_gate');   // 0068 の門 (代表の持ち主 = company の試験の DB) はこの試験の外 (取引ごと戻す)
    await pg.query(`with e as (insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
        values ('products', 'ne-reg-single-v2', 'syohin_code', 'utf8', false, 1, 1, repeat('a', 64), repeat('b', 64), repeat('c', 64), '\\x00', gen_random_uuid(), 'x', current_date, 't') returning export_id)
      insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by, state)
      select e.export_id, k.sku_id, k.code_norm, k.code, k.sku_kind, repeat('e', 64), '{}'::jsonb, repeat('f', 64), 1, 1, 't', 'built' from e, core.skus k where k.code = 'g01'`);
    const c = await check('g01');
    assert.deepEqual(reasons(c), ['live_file']); assert.match(c.problems[0].text, /built/);
  } finally { await pg.query('rollback'); }
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.registration_protocol', '1', true)");
    await pg.query("update ops.master_registrations set state = 'quarantined', state_changed_by = 't' where sku_id = $1", [await sid('g01')]);
    assert.deepEqual(reasons(await check('g01')), ['not_available']);
  } finally { await pg.query('rollback'); }
  // 数えた一番新しい取得 (の記録の回) の取得の完了が 40 時間前 = fetch_old (記録の時刻 created_at は今のまま = 記録の時刻では決めない)
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.parent_gate_protocol', '1', true)");
    const old = (await pg.query(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
        evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by)
      values (1, 'mc_old_x', 'ne_old', repeat('1', 64), now() - interval '40 hours', now() - interval '1 hour', 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`)).rows[0].result_id;
    await pg.query("select set_config('ops.ne_absence_protocol', '1', true)");
    await pg.query('update ops.master_ne_absence_mark set result_id = $1', [old]);
    await pg.query('update ops.master_ne_absence set last_absent_result_id = $1', [old]);
    assert.deepEqual(reasons(await check('g01')), ['fetch_old']);
  } finally { await pg.query('rollback'); }
  assert.equal((await check('g01')).ok, true);
});
await ta('[C4] 一覧 (CLI の dry-run): null = 候補 (available で数えた一番新しい取得に無い) / コード = 渡した順に無いコードも', async () => {
  const cand = (await one('select ops.sku_gone_from_ne_list(null) as r')).r;
  assert.deepEqual(cand.map((x) => x.code), ['g01', 'g02', 'g04', 'g05', 'g06', 'g07', 'g08', 'g09', 'g10', 'g11', 'g12', 'g13', 'g14', 'keep03', 'set001']);
  const got = (await one('select ops.sku_gone_from_ne_list($1::text[]) as r', [['G02', 'nope', 'g02']])).r;
  assert.deepEqual(got.map((x) => [x.code, x.ok, x.problems.map((p) => p.reason).join(',')]), [['g02', true, ''], ['nope', false, 'no_sku']]);
  const e = await errOf(() => pg.query('select ops.sku_gone_from_ne_list($1::text[])', [Array.from({ length: 501 }, (_, i) => `c${i}`)]));
  assert.match(e?.message ?? '', /invalid_input/);
});

// ═══════════ [T] transition を直に ═══════════
console.log('[T] ops.transition_sku_registration を直に');
await ta('[T1] available → cancelled は約束なしでは断る (持ち主・master_ops)・呼び手の根拠は受けない・ほかの行き先は今までどおり断る', async () => {
  const g02 = await sid('g02');
  for (const role of ['deploy', 'master_ops']) {
    const e = await errOf(() => as(role, () => pg.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', 'NE で消えた')`, [g02])));
    assert.match(e?.message ?? '', /gone_session_required/, role);
  }
  const e2 = await errOf(() => pg.query(`select ops.transition_sku_registration($1, 'draft', 'human', 'naka@test', 'x')`, [g02]));
  assert.match(e2?.message ?? '', /one_way/);
  assert.equal(await regOf('g02'), 'available');
});
await ta('[T2] 下書き → やめる (0052 からの道) は変わらない (約束も確かめも要らない)', async () => {
  const k3 = await sid('keep03');
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.registration_protocol', '1', true)");
    await pg.query("update ops.master_registrations set state = 'draft', state_changed_by = 't' where sku_id = $1", [k3]);
    await pg.query("select set_config('ops.registration_protocol', '', true)");
    const r = (await pg.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', '試験') as r`, [k3])).rows[0].r;
    assert.equal(r.to, 'cancelled');
    assert.deepEqual((await pg.query('select evidence from ops.master_registration_events where sku_id = $1 order by event_id desc limit 1', [k3])).rows[0].evidence, {});
  } finally { await pg.query('rollback'); }
});

// ═══════════ [F] 動かす関数 ═══════════
console.log('[F] ops.cancel_sku_gone_from_ne');
const cancelAs = (role, reqId, skuId, reason = 'NE で消えた (別のコードで作り直し)', actor = 'naka@test') =>
  as(role, async () => (await pg.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint) as r', [reqId, actor, reason, skuId])).rows[0].r);
await ta('[F1] 画面のロールでやめる = cancelled・履歴の根拠は DB の取得の回・保存の記録 done・押し直し = 前の答え・同じ番号で違う理由 = request_id_reused', async () => {
  const g01 = await sid('g01'); const rid = crypto.randomUUID();
  const r = await cancelAs('master_edit', rid, g01);
  assert.equal(r.ok, true); assert.equal(r.state, 'cancelled'); assert.equal(r.code, 'g01');
  assert.equal(await regOf('g01'), 'cancelled');
  const ev = await one('select from_state, to_state, actor_type, actor_id, reason, evidence, request_id from ops.master_registration_events where sku_id = $1 order by event_id desc limit 1', [g01]);
  assert.deepEqual([ev.from_state, ev.to_state, ev.actor_type, ev.actor_id, ev.request_id], ['available', 'cancelled', 'human', 'naka@test', rid]);
  assert.equal(ev.evidence.rule, 'gone_from_ne_v2'); assert.equal(ev.evidence.streak_days, 3);
  assert.deepEqual(r.evidence, ev.evidence);
  const mer = await one('select operation, status, sku_id::text as sku_id, actor_id from ops.master_edit_requests where request_id = $1', [rid]);
  assert.deepEqual(mer, { operation: 'sku_gone_cancel', status: 'done', sku_id: g01, actor_id: 'naka@test' });
  const again = await cancelAs('master_edit', rid, g01);
  assert.equal(again.replayed, true); assert.equal(again.event_id, r.event_id);
  const e = await errOf(() => cancelAs('master_edit', rid, g01, '違う理由'));
  assert.match(e?.message ?? '', /request_id_reused/);
  const e2 = await errOf(() => cancelAs('master_edit', crypto.randomUUID(), g01));
  assert.match(e2?.message ?? '', /not_gone_from_ne: .*available/);
});
await ta('[F2] 運用のロール (CLI) でもやめられる・条件を満たさない = not_gone_from_ne (理由を全部)・何も変えない', async () => {
  const r = await cancelAs('master_ops', crypto.randomUUID(), await sid('g02'));
  assert.equal(r.ok, true); assert.equal(await regOf('g02'), 'cancelled');
  const e = await errOf(async () => cancelAs('master_ops', crypto.randomUUID(), await sid('g05')));
  assert.match(e?.message ?? '', /not_gone_from_ne: g05 .*販路の出品の構成/);
  assert.equal(await regOf('g05'), 'available');
  assert.equal((await one("select count(*)::int as n from ops.master_edit_requests where operation = 'sku_gone_cancel' and sku_id = $1", [await sid('g05')])).n, 0);
});
await ta('[F3] 理由が要る (空・空白・201 字・改行)・人が要る・SKU が無い', async () => {
  const g09 = await sid('g09');
  for (const reason of ['', '   ', 'あ'.repeat(201), '改行\nあり']) {
    const e = await errOf(() => cancelAs('master_edit', crypto.randomUUID(), g09, reason));
    assert.match(e?.message ?? '', /invalid_input/, JSON.stringify(reason));
  }
  assert.match((await errOf(() => cancelAs('master_edit', crypto.randomUUID(), g09, '理由', '')))?.message ?? '', /invalid_input/);
  assert.match((await errOf(() => cancelAs('master_edit', crypto.randomUUID(), '999999')))?.message ?? '', /not_found/);
});
await ta('[F4] 段階が new_open でない = before_cutover (取引の中で段階を変えて巻き戻す)', async () => {
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.cutover_protocol', '1', true)");
    await pg.query("update ops.master_cutover_state set phase = 'frozen', owner_hash = null where id = 1");
    await pg.query("select set_config('ops.cutover_protocol', '', true)");
    const e = await errOf(async () => pg.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint)', [crypto.randomUUID(), 'naka@test', '理由', await sid('g10')]));
    assert.match(e?.message ?? '', /before_cutover/);
  } finally { await pg.query('rollback'); }
});

// ═══════════ [I] やめた SKU への参照を新しく作らない ═══════════
console.log('[I] やめた SKU への参照');
/** 取引の中で sql を流し、断られた誤りを返して巻き戻す (取引の中だけ = 試験の DB を汚さない) */
const refused = async (fn) => { await pg.query('begin'); try { await fn(); return null; } catch (e) { return e; } finally { await pg.query('rollback'); } };
await ta('[I1] 構成の子・出品の構成・構成の依頼・代表 (親) に、やめた g01 を新しく入れる = 持ち主の手でも sku_cancelled (23514)', async () => {
  const g01 = await sid('g01'); const set = await sid('set001');
  const cases = {
    sku_components: () => pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'manual')`, [set, g01]),
    sku_components_update: async () => pg.query(`update core.sku_components set child_sku_id = $1 where parent_sku_id = $2 and child_sku_id = $3`, [g01, set, Number(await sid('keep01'))]),
    listing_components: async () => {
      const lid = (await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', 'r-g01', 'active') returning listing_id`)).rows[0].listing_id;
      await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, g01]);
    },
    sku_component_requests: async () => pg.query(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id)
        values (1, $1, $2::jsonb, $3, '[]'::jsonb, 't', gen_random_uuid())`, [set, JSON.stringify([{ sku_id: Number(g01), code: 'g01', qty: 1, sort: 1 }]), hex('8')]),
    products_parent: async () => {
      await pg.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
      await pg.query(`update core.products set parent_product_id = (select product_id from core.skus where code = 'g01') where product_id = (select product_id from core.skus where code = 'keep01')`);
    },
  };
  for (const [name, fn] of Object.entries(cases)) {
    const e = await refused(async () => { if (name === 'sku_component_requests') await pg.query("update ops.sku_component_requests set status = 'cancelled', closed_at = now(), closed_by = 't', close_reason = 'withdrawn' where status = 'open'"); await fn(); });
    assert.equal(e?.code, '23514', `${name}: ${e?.message}`); assert.match(e.message, /sku_cancelled: .*g01/, name);
  }
});
await ta('[I2] やめていない SKU への参照・参照を変えない更新 (数量) は今までどおり通る', async () => {
  assert.equal(await refused(async () => pg.query(`update core.sku_components set qty = 2 where parent_sku_id = $1 and child_sku_id = $2`, [Number(await sid('set001')), Number(await sid('keep01'))])), null);
  assert.equal(await refused(async () => pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'manual')`, [await sid('set001'), await sid('g13')])), null);
});

// ═══════════ [L] 夜間ロード・0068 の数え ═══════════
console.log('[L] 夜間ロード・0068 の数え');
await ta('[L1] 夜間ロードをもう一度 (同じ材料 = 消えた商品も載っている) / NE から作った SKU の quarantined = cancelled は戻らない', async () => {
  const r = await runInitialLoad(db, PLAN, { log: quiet, runId: 'gone_load_2', host: 'test' });
  assert.equal(r.ok, true, r.error);
  await pg.query('select ops.quarantine_unregistered_skus($1)', ['gone_load_2']);
  assert.equal(await regOf('g01'), 'cancelled'); assert.equal(await regOf('g02'), 'cancelled');
  assert.equal(await regOf('g04'), 'available');
});
await ta('[L2] 0068 の数え: cancelled は数えない (excluded.cancelled)・数え方は変えない', async () => {
  const v = (await one('select ops.parent_raw_gate(1, $1::jsonb, true) as r', [JSON.stringify(await obsWithout(GONE))])).r;
  assert.equal(v.excluded.cancelled, 2);
  assert.ok(!v.items.some((i) => ['g01', 'g02'].includes(i.code)));
  assert.ok(v.items.some((i) => i.code === 'g04' && i.class === 'parent_incomparable' && i.reason === 'not_in_ne'));
});
await ta('[L3] やめた後の照合 ② の記録も今までどおり (やめた SKU も無いに数える = 表は SKU のコードで持つ・同じ日 = 日数は同じ)', async () => {
  await gateRun(GONE2);
  assert.deepEqual(await absence(), after3());
});

// ═══════════ [R] ロール ═══════════
console.log('[R] ロール');
await ta('[R1] master_edit・master_ops = 動かす・確かめの実行 / watcher = 確かめだけ / 部品と表はだれも触れない', async () => {
  const fnPriv = async (role, sig) => (await one('select has_function_privilege($1, $2, $3) as ok', [role, sig, 'execute'])).ok;
  for (const role of ['master_edit', 'master_ops']) for (const f of GONE_FUNCTIONS) assert.equal(await fnPriv(role, f), true, `${role} ${f}`);
  for (const role of ['master_edit', 'master_ops', 'watcher', 'watch_writer', 'master_gate', 'master_observer', 'new_entry_gate']) {
    for (const f of GONE_OWNER_ONLY_FUNCTIONS) assert.equal(await fnPriv(role, f), false, `${role} ${f}`);
  }
  for (const f of [...GONE_FUNCTIONS, ...GONE_OWNER_ONLY_FUNCTIONS]) assert.equal(await fnPriv('public', f), false, `public ${f}`);
  assert.equal(await fnPriv('watcher', 'ops.sku_gone_from_ne_check(bigint)'), true);
  assert.equal(await fnPriv('watcher', 'ops.cancel_sku_gone_from_ne(uuid, text, text, bigint)'), false);
  for (const role of ['master_edit', 'master_ops']) for (const t of ['ops.master_ne_absence', 'ops.master_ne_absence_mark']) {
    assert.equal((await one('select has_table_privilege($1, $2, $3) as ok', [role, t, 'select'])).ok, false, `${role} ${t}`);
  }
  const e = await errOf(() => as('watcher', () => pg.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint)', [crypto.randomUUID(), 'x@test', '理由', '1'])));
  assert.equal(e?.code, '42501');
});
await ta('[R2] security definer の 0070 の関数 = search_path は pg_catalog, pg_temp・public の実行権なし', async () => {
  const names = ['cancel_sku_gone_from_ne', 'sku_gone_from_ne_check', 'sku_gone_from_ne_list', 'record_parent_gate', 'transition_sku_registration', 'sku_ref_cancelled', 'guard_sku_ref_not_cancelled'];
  const rows = await q(`select p.proname as n, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub
    from pg_proc p join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname = any($1::text[]) order by 1`, [names]);
  assert.equal(rows.length, names.length);
  for (const r of rows) { assert.equal(r.d, true, r.n); assert.equal(r.c, 'search_path=pg_catalog, pg_temp', r.n); assert.equal(r.pub, false, r.n); }
});

// ═══════════ [K] CLI ═══════════
console.log('[K] CLI');
const H0 = hex('0');
await ta('[K1] 引数: --apply はコード・--expect-hash (64 桁)・人・理由・--yes が要る / 知らない引数 / コードのファイル', async () => {
  const bad = (argv, re) => { const e = (() => { try { parseGoneArgs(argv); } catch (x) { return x; } return null; })(); assert.ok(e && e.code === 'GONE_ARGS' && re.test(e.message), `${argv.join(' ')}: ${e?.message}`); };
  bad(['--apply'], /コード/);
  bad(['--apply', '--codes', 'a'], /--expect-hash/);
  bad(['--apply', '--codes', 'a', '--expect-hash', '18'], /64 桁/);
  bad(['--apply', '--codes', 'a', '--expect-hash', H0], /--actor/);
  bad(['--apply', '--codes', 'a', '--expect-hash', H0, '--actor', 'n@t'], /--reason/);
  bad(['--apply', '--codes', 'a', '--expect-hash', H0, '--actor', 'n@t', '--reason', 'r'], /--yes/);
  bad(['--codes', 'a', '--codes-file', 'b'], /どちらか/);
  bad(['--expect', '1'], /知らない/);
  const o = parseGoneArgs(['--apply', '--codes', 'a, b', '--expect-hash', H0, '--actor', 'n@t', '--reason', ' 理由 ', '--yes']);
  assert.deepEqual([o.codes, o.expectHash, o.reason], [['a', 'b'], H0, '理由']);
  const f = path.join(DATA_TMP, 'codes.txt'); fs.writeFileSync(f, '﻿g01\r\n# メモ\ng02  # 後ろのメモ\n\n');
  assert.deepEqual(readCodesFile(f), ['g01', 'g02']);
});
const runOps = (dbx, opts, lines) => as('master_ops', () => runGone(dbx, opts, { log: (l) => lines.push(l) }));
await ta('[K2] dry-run = 何も変えない・計画のハッシュ / ハッシュが違う = 1 件も変えない / 同じ = やめてよい件を 1 つの取引で全部', async () => {
  const lines = [];
  const d = await runOps(db, { apply: false, codes: ['g11', 'g12', 'g09', 'g05'] }, lines);
  assert.equal(d.exitCode, 0); assert.equal(d.checks.length, 4); assert.match(d.planHash, /^[0-9a-f]{64}$/);
  assert.ok(lines.some((l) => /⛔ やめない {2}g05/.test(l)) && lines.some((l) => l.includes(d.planHash)));
  assert.equal(await regOf('g11'), 'available');
  const x = await runOps(db, { apply: true, codes: ['g11', 'g12', 'g09', 'g05'], expectHash: H0, actor: 'naka@test', reason: 'NE で消えた' }, lines);
  assert.equal(x.exitCode, 1); assert.equal(x.error.reason, 'plan_mismatch');
  assert.equal(await regOf('g11'), 'available'); assert.equal(await regOf('g12'), 'available');
  const y = await runOps(db, { apply: true, codes: ['g11', 'g12', 'g09', 'g05'], expectHash: d.planHash, actor: 'naka@test', reason: 'NE で消えた' }, lines);
  assert.equal(y.exitCode, 0, JSON.stringify(y.error)); assert.deepEqual(y.applied.map((a) => [a.code, a.ok]), [['g11', true], ['g12', true]]);
  assert.equal(await regOf('g11'), 'cancelled'); assert.equal(await regOf('g12'), 'cancelled'); assert.equal(await regOf('g09'), 'available');
  const ev = await one("select actor_id, reason from ops.master_registration_events e join core.skus k on k.sku_id = e.sku_id where k.code = 'g11' order by event_id desc limit 1");
  assert.deepEqual(ev, { actor_id: 'naka@test', reason: 'NE で消えた' });
});
await ta('[K3] dry-run の後に対象が変わった (g14 に出品の構成が付いた) = 件数が同じでもハッシュが違う = 1 件も変えない', async () => {
  const lines = [];
  const d = await runOps(db, { apply: false, codes: ['g13', 'g14'] }, lines);
  assert.equal(d.checks.filter((r) => r.ok).length, 2);
  const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', 'r-g14', 'active') returning listing_id`)).listing_id;
  await q(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('g14')]);
  try {
    const x = await runOps(db, { apply: true, codes: ['g13', 'g14'], expectHash: d.planHash, actor: 'naka@test', reason: 'NE で消えた' }, lines);
    assert.equal(x.exitCode, 1); assert.equal(x.error.reason, 'plan_mismatch');
    assert.equal(await regOf('g13'), 'available');
  } finally {
    await q('delete from core.listing_components where listing_id = $1', [lid]);
  }
});
await ta('[K4] apply の途中で 1 件の証拠が変わった / 2 件目が断られた = 1 つの取引ごと巻き戻す (前にやめた件も戻る)', async () => {
  const lines = [];
  const d = await runOps(db, { apply: false, codes: ['g13', 'g14'] }, lines);
  // (a) 1 件目をやめる直前に、同じ日の新しい照合 ② の数えが入る (同じ取引の中) = DB の関数が返す証拠 (最後の回) が計画と違う
  let fired = false;
  const hookA = { query: async (t, p) => {
    if (!fired && /ops\.cancel_sku_gone_from_ne/.test(t)) {
      fired = true;
      await pg.query('set role deploy');
      const obs = await obsWithout(GONE2);
      await pg.query("select set_config('ops.parent_gate_protocol', '1', true)");
      const id = (await pg.query(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
          evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by) values (1, $1, $2, repeat('1', 64), now(), now(), 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`,
        [runId(), `ne_${crypto.randomBytes(4).toString('hex')}`])).rows[0].result_id;
      await pg.query('select ops._record_ne_absence($1, $2::jsonb)', [id, JSON.stringify(obs)]);
      await pg.query('set role master_ops');
    }
    return pg.query(t, p);
  } };
  const markBefore = await mark();
  const a = await runOps(hookA, { apply: true, codes: ['g13', 'g14'], expectHash: d.planHash, actor: 'naka@test', reason: 'NE で消えた' }, lines);
  assert.equal(a.exitCode, 1); assert.equal(a.error.reason, 'evidence_mismatch');
  assert.equal(await regOf('g13'), 'available'); assert.deepEqual(await mark(), markBefore);   // 取引ごと戻った (差し込んだ数えも)
  // (b) 2 件目 (g14) の前に出品の構成が付く = 2 件目が断られる → 1 件目 (g13) も戻る
  let n = 0;
  const hookB = { query: async (t, p) => {
    if (/ops\.cancel_sku_gone_from_ne/.test(t) && ++n === 2) {
      await pg.query('set role deploy');
      const lid = (await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', 'r-g14b', 'active') returning listing_id`)).rows[0].listing_id;
      await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid('g14')]);
      await pg.query('set role master_ops');
    }
    return pg.query(t, p);
  } };
  const b = await runOps(hookB, { apply: true, codes: ['g13', 'g14'], expectHash: d.planHash, actor: 'naka@test', reason: 'NE で消えた' }, lines);
  assert.equal(b.exitCode, 1); assert.equal(b.error.reason, 'not_gone_from_ne');
  assert.deepEqual(b.applied.map((x) => [x.code, x.ok, !!x.rolled_back]), [['g13', false, true], ['g14', false, true]]);
  assert.equal(await regOf('g13'), 'available'); assert.equal(await regOf('g14'), 'available');
  assert.equal((await one("select count(*)::int as n from ops.master_edit_requests where operation = 'sku_gone_cancel' and sku_id = $1", [await sid('g13')])).n, 0);
});

// ═══════════ [S] 画面 (本物の router) ═══════════
console.log('[S] 画面');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { default: router, __setPgClientFactory, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/gone';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/gone';
process.env.MASTER_EDITORS = 'naka@test,edit@test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
let EMAIL = 'naka@test';
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: EMAIL, displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const B = `${ORIGIN}/apps/master-edit`;
const page = async (code) => { const r = await fetch(`${B}/sku/${code}`); assert.equal(r.status, 200, code); return r.text(); };
const post = async (code, body) => { const r = await fetch(`${B}/api/sku/${code}/gone`, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: ORIGIN }, body: JSON.stringify(body) }); return { status: r.status, j: await r.json() }; };
await ta('[S1] 画面: 条件を満たす商品 = パネルとボタン (名簿の人) / 名簿に無い人 = ボタンなし / NE にある商品 = パネルなし / 使われている = 理由だけ', async () => {
  assert.deepEqual([G.goneView(null).show, G.goneView({ reg_state: 'cancelled', sku_kind: 'single', ok: false, problems: [] }).show], [false, false]);
  EMAIL = 'naka@test';
  const h = await page('g10');
  assert.match(h, /id="gone-box"/); assert.match(h, /id="gone-go"/); assert.match(h, /id="gone-bg"/); assert.match(h, /me-gone\.js/);
  assert.match(h, /NE に無い 3 \/ 3 日/);
  EMAIL = 'edit@test';
  const h2 = await page('g10');
  assert.match(h2, /id="gone-box"/); assert.doesNotMatch(h2, /id="gone-go"/); assert.doesNotMatch(h2, /me-gone\.js/); assert.match(h2, /id="gone-who"/);
  EMAIL = 'naka@test';
  assert.doesNotMatch(await page('keep01'), /id="gone-box"/);
  const h3 = await page('g05');
  assert.match(h3, /id="gone-box"/); assert.doesNotMatch(h3, /id="gone-go"/); assert.match(h3, /data-reason="listing_component"/);
  assert.doesNotMatch(await page('g01'), /id="gone-box"/);   // やめた商品 = 出さない
});
await ta('[S2] API: 名簿に無い = 403 / 印なし・理由なし = 400 / MASTER_EDIT_OPEN なし = 409 / 条件を満たさない = 409 / 通る = cancelled・押し直し = replayed', async () => {
  EMAIL = 'edit@test';
  assert.equal((await post('g10', { request_id: crypto.randomUUID(), reason: '理由', confirm: true })).status, 403);
  EMAIL = 'naka@test';
  assert.equal((await post('g10', { request_id: crypto.randomUUID(), reason: '理由' })).status, 400);
  assert.equal((await post('g10', { request_id: crypto.randomUUID(), reason: ' ', confirm: true })).status, 400);
  delete process.env.MASTER_EDIT_OPEN;
  try { assert.equal((await post('g10', { request_id: crypto.randomUUID(), reason: '理由', confirm: true })).status, 409); } finally { process.env.MASTER_EDIT_OPEN = '1'; }
  const ng = await post('g05', { request_id: crypto.randomUUID(), reason: '理由', confirm: true });
  assert.equal(ng.status, 409); assert.equal(ng.j.reason, 'not_gone_from_ne'); assert.match(ng.j.error, /販路の出品の構成/);
  assert.equal(await regOf('g10'), 'available');
  const rid = crypto.randomUUID();
  const ok = await post('g10', { request_id: rid, reason: 'NE で消えた (g10x で作り直し)', confirm: true });
  assert.equal(ok.status, 200, JSON.stringify(ok.j)); assert.equal(ok.j.state, 'cancelled');
  assert.equal(await regOf('g10'), 'cancelled');
  const again = await post('g10', { request_id: rid, reason: 'NE で消えた (g10x で作り直し)', confirm: true });
  assert.equal(again.status, 200); assert.equal(again.j.replayed, true);
  assert.equal((await one('select db_user from ops.master_write_sessions where request_id = $1', [rid])).db_user, 'master_edit');
  assert.doesNotMatch(await page('g10'), /id="gone-box"/);
});

// ═══════════ [U] 画面の JS ═══════════
console.log('[U] 画面の JS');
if (process.env.MASTER_EDIT_UI_SKIP === '1') console.log('  ⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の JS は飛ばす');
else {
  let browser;
  try { const { chromium } = await import('playwright'); browser = await chromium.launch(); }
  catch (e) { console.error(`  NG  画面の JS の試験を動かせない (Playwright / Chromium): ${e.message}`); process.exitCode = 1; }
  if (browser) {
    await ta('[U1] 理由と印で押せる → 確かめの窓 (やめない = 何もしない・Esc) → やめた にする → 読み直すと「やめた」', async () => {
      const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: 'ja-JP' });
      const p = await ctx.newPage();
      const errors = []; p.on('pageerror', (e) => errors.push(e.message));
      try {
        await p.goto(`${B}/sku/keep03`);
        const go = p.locator('#gone-go');
        assert.equal(await go.isDisabled(), true);
        await p.fill('#gone-reason', 'NE で消えた (試験)');
        assert.equal(await go.isDisabled(), true);
        await p.check('#gone-seen');
        assert.equal(await go.isDisabled(), false);
        await go.click();
        assert.equal(await p.locator('#gone-bg').evaluate((el) => el.classList.contains('on')), true);
        assert.equal(await p.locator('#gone-why').textContent(), 'NE で消えた (試験)');
        await p.keyboard.press('Escape');
        assert.equal(await p.locator('#gone-bg').evaluate((el) => el.classList.contains('on')), false);
        assert.equal(await regOf('keep03'), 'available');
        await go.click(); await p.click('#gone-stay');
        assert.equal(await regOf('keep03'), 'available');
        await go.click();
        await Promise.all([p.waitForNavigation({ timeout: 15000 }), p.click('#gone-ok')]);
        assert.equal(await regOf('keep03'), 'cancelled');
        assert.match(await p.content(), /やめた/);
        assert.equal(await p.locator('#gone-box').count(), 0);
        assert.deepEqual(errors, []);
      } finally { await ctx.close(); }
    });
    await browser.close();
  }
}

server.close();
await pg.close();
try { fs.rmSync(DATA_TMP, { recursive: true, force: true }); } catch { /* */ }
console.log(`${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
