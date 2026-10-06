/**
 * test-master-edit.mjs — マスタの入力 (apps/master-edit・lib/master-write.mjs・lib/master-cutover.mjs・0051。Company DB構想 14 §6 ⑤-1 / Codex ⑤-R0・R1・PR #1563 R1)
 *
 * Company DB = PGlite (Render と同じ条件の持ち主のロール deploy で migration)。保存は画面だけのロール master_edit で流す (権限が足りているかも確かめる)。
 * 本物の router を HTTP 越しにも通す (セッションは x-test-session で模擬)
 * 固定する契約:
 *   1 切替の段階: 一方向・1 段ずつ。進めるには証拠 (drain・手の入口を止めた一覧 / 持ち主表のハッシュ) と、全部の場所 (render・minipc) の門の記録が要る
 *     (⑤-1 では誰も門の記録を書かない = 進められない)。直接は書けない・記録が残る・読めない = 閉じている
 *   2 保存を開く = 段階 new_open **かつ** 持ち主表のハッシュが段階の記録と同じ **かつ** 列が 'company' **かつ** MASTER_EDIT_OPEN。欠ければ 409 切替前・何も書かない
 *     (変わる項目が無い保存も 409 = done を残さない)。構成の依頼も持ち主 sku_components が 'company' のときだけ
 *   3 単品の保存: 変わった列だけ・商品の行もそろう・代表 (親) は manual・代表の仕入先・変更の記録の actor / source / request_id / reason
 *   4 保存した値は夜間ロードを 2 回流しても残る (持ち主 company)。持ち主を load に戻すと夜間ロードが戻す
 *   5 同じ request_id: 同じ中身 = 前の結果 / 違う中身・違う人・違う SKU = 409 / 失敗 = 同じ誤り。記録は保存と同じ取引・追記だけ
 *   6 編集の印: 読んだ行の全部と「行が無いこと」(phantom) のどれかが変わった = 409 とその間の変更・何も書かない
 *   7 入力の検証 / 新しい商品コードの形 (⑤-2 用)
 *   8 単品の税率・取扱・原価を変えると、含むセットの導く値を同じ取引で計算し直す
 *   9 原価は今日だけ (東京の日付・取引の初めに 1 回)・期間を重ねない・重なりは DB が拒む (この画面と昇格だけ)
 *  10 NE に取り込む CSV が出ている列は変えない (409)
 *  11 セットの構成 = 依頼だけ。NE の観測 (DB に残した完全な回・依頼より後) が構成品・数量・並び・行の数まで同じときだけ上げる (同じ取引で構成・導く値・依頼・食い違い)。
 *     違えば食い違い (mismatch / stale / unrequested_diff) を残す。偽の観測・完全でない回・依頼より前の観測は上げない。観測を書く関数は画面のロールでは動かない
 *  12 セットの導く値: 今の構成と依頼の構成の両方で確かめる (上書きを外すのは両方が導けるときだけ)・例外原価をやめる = 今日から合計
 *  13 上げる処理と単品の保存の順番 (どちらが先でもセットの値が新しい単品の値になる・後の保存は含むセットが増えたことに気づく)
 *  14 DB のロール: master_edit = 画面の読み書きだけ (段階を進める・観測を書く・構成を上げる・門の記録は不可) / master_ops = 段階を進める関数だけ
 *  15 画面: 一覧・単品・セット・変更の記録・つかいかた・404 の描画と画面の JS / 名簿・Origin・Content-Type / 書き込み用の接続が無い = 見るだけ /
 *     Company DB が無い・届かない = 帯と 503 / 保存を開いていない = 帯・欄と保存のボタンが閉じている / server.js は Render だけ
 *  16 FBA (JP) の在庫 (参考): Company DB の在庫の日次の最新の complete の日・何時時点 (取得の時刻)・1 × 1 の出品だけ足す (まとめ売り・セットの出品は別)・
 *     partial の日は使わない・古い (26 時間) / 読めない (日次なし・権限なし)・master_edit は 2 つの表を読むだけ・流し直しで読める
 *  17 売れた数 (参考): 商品管理リストの公開の回 (Render の写し・発注アプリと同じ数)・いつまでの数か (前日まで)・FBA / FBA 以外・モール別・セットは構成品に入る (「—」)・
 *     商品管理リストに無い = 「—」・古い / 読めない・ページの分だけ索引で引く・Company DB の権限は広げない
 *  18 一覧の区分の列 (区分の札と同じ・区分の順)・コードのコピーのボタン・絞った一覧の全部のコード / CSV (BOM・CRLF・列・式の注入の対策・印 ?s=・並び・offset は無視・
 *     注文残は発注アプリの権限者だけ・件数 / 時間の上限・期限切れ) / ロジザードの写しの「古い」(写しの時間 09〜18 時の外 = 18 時台の写しは次の朝 10 時まで古くない)
 *  19 大きめの見本 (例外の SKU 1,500 件・含むセット 51 件): CSV・全部コピーは SQL で上限 + 1 件まで (中身の段に進まない)・JS で絞る条件・期限は参考の値と販売数の読みにも効く・含むセットの数
 * 使い方: node scripts/test-master-edit.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import vm from 'node:vm';
import express from 'express';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
// 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
// 0055 (④a): company_owner・new_open に進むのは持ち主の epoch が active で段階の持ち主表と同じときだけ = 進める直前に試験で置く
const EPOCH = (await import('./fixtures/master-epoch.mjs')).epochSeeder();
const R = await import('../apps/master-edit/read.mjs');
const { default: router, __setPgClientFactory, __setClock, __setOwnership, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const rejectsWith = async (p, status, reason) => {
  try { await p; } catch (e) {
    assert.ok(e instanceof W.MasterWriteError, `MasterWriteError でない: ${e && e.stack}`);
    assert.equal(e.status, status, `${e.reason}: ${e.message}`);
    if (reason) assert.equal(e.reason, reason, e.message);
    return e;
  }
  assert.fail(`${status} ${reason || ''} にならなかった`);
};
const pgCode = async (p) => { try { await p; return 'ok'; } catch (e) { return e.code || e.message; } };
/** 一覧の HTML の、その商品の行の data-col の欄の字 (タグを除く)。列の位置に頼らない */
const colCell = (html, code, col) => {
  const tr = html.split('<tr>').find((x) => x.includes(`href="sku/${code}"`));
  assert.ok(tr, `一覧に ${code} の行が無い`);
  const m = new RegExp(`<td[^>]*data-col="${col}"[^>]*>([\\s\\S]*?)<\\/td>`).exec(tr);
  return m ? m[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim() : undefined;
};

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
EPOCH.remember(ALL_COMPANY);
const withOwn = (over) => ({ ...MASTER_OWNERSHIP, ...over });
const LOAD_NOW = new Date('2030-01-05T03:00:00Z');   // 夜間ロードの日 (東京 2030-01-05)
const NOW = new Date('2030-01-10T03:00:00Z');        // 画面の今日 (東京 2030-01-10)
const TODAY = '2030-01-10';
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210.4 }], ['S02', { method: '宅急便', cost: 520 }]]);
/** ⑤-3 の古い入口の一覧 (manifest) の形の例。kind = manual = コードでは閉じられない入口 */
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }, { id: 'gas.logizard_sheet', kind: 'manual' }] };
/** 証拠の時刻は今の段階に入った後・サーバーの今以前 (#1563 R3 High 1) = 進める直前に作る */
const stamp = () => new Date().toISOString();
const manualStopped = () => [{ id: 'gas.logizard_sheet', by: 'naka@test', at: stamp() }, { id: 'ne.product_screen', by: 'naka@test', at: stamp() }];
const drainNow = () => ({ done: true, checked_by: 'naka@test', checked_at: stamp() });
const BUILDS = { render: ['r1'], minipc: ['m1'] };
const LEGACY_HASH = C.ownershipHash(MASTER_OWNERSHIP);

/** 夜間ロードの材料 (sources.mjs が作る形) */
function makePlan() {
  const sku = (code, name, kind, taxRate, salesClass, cost, extra = {}) => ({
    code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : taxRate === 0.1 ? 'STANDARD_10' : null, handling: 'active', salesClass,
    cost: cost == null ? null : { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' },
    standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...extra,
  });
  return {
    skus: [
      sku('s001', '単品 1', 'single', 0.1, 3, 100, { representativeCode: 'grp1', representativeState: 'value' }),
      sku('s002', '単品 2', 'single', 0.08, 2, 200),
      sku('s003', '単品 3', 'single', 0.1, 1, 50, { representativeCode: 'grp1', representativeState: 'value' }),
      sku('s004', '単品 4 (分類・原価なし)', 'single', 0.1, null, null),
      sku('s005', '単品 5 (税率なし)', 'single', null, 3, 80),
      sku('s006', '単品 6 (分類・原価なし)', 'single', 0.1, null, null),
      sku('set001', 'セット 1', 'set', 0.08, null, 400, { taxClass: 'MIXED' }),
      sku('set004', 'セット 4 (導けない)', 'set', 0.1, null, null),
      sku('set005', 'セット 5 (税率が決まらない)', 'set', null, null, 180),
      sku('set006', 'セット 6 (今の構成は導けない)', 'set', 0.1, null, null),
    ],
    variationGroups: [{ code: 'grp1', name: '名札', childCodes: ['s001', 's003'], status: 'active' }],
    setComponents: [
      { parentCode: 'set001', childCode: 's001', qty: 2, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' },
      { parentCode: 'set004', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set004', childCode: 's004', qty: 1, source: 'ne' },
      { parentCode: 'set005', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set005', childCode: 's005', qty: 1, source: 'ne' },
      { parentCode: 'set006', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set006', childCode: 's006', qty: 1, source: 'ne' },
    ],
    listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC', orderMethod: 'fax', leadTimeDays: 10 }, { code: '0002', name: 'ビーフリー', orderMethod: 'email', leadTimeDays: 5 }, { code: '0003', name: '止めた仕入先' }],
    supplierSkus: [{ supplierCode: '0001', skuCode: 's001', vendorCode: 'AMC-001' }],
    primarySuppliers: [{ skuCode: 's001', supplierCode: '0001' }],
    reorder: { available: true, runId: 'pml_test' },
  };
}

/** 1 つの Company DB (PGlite) を作る: 持ち主のロール deploy で migration・見張りと画面のロール・夜間ロード */
async function setupDb() {
  const pg = new PGlite();
  const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;   // 試験の接続のログイン (superuser)。門のログインから戻るときに使う
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: 'load_setup', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  await pg.query("update core.suppliers set active = false where code = '0003'");
  return { pg, db, sessionUser };
}
/** ロールを切り替えて動かす (試験は 1 つの接続 = SET ROLE。終わったら持ち主のロール deploy に戻す) */
async function asRole(E, role, fn) {
  await E.pg.query(`set role ${role}`);
  try { return await fn(); } finally { await E.pg.query('set role deploy'); }
}
/** 画面だけのロールで動かす (保存は必ずこれ = 権限が足りているかも確かめる) */
const asEditor = (E, fn) => asRole(E, 'master_edit', fn);
/**
 * 場所ごとの門のログイン (master_gate_render / master_gate_minipc) で動かす。DB の関数は session_user を見る = SET ROLE では足りない → SET SESSION AUTHORIZATION。
 * 終わったら試験の接続のログインに戻して、持ち主のロール deploy にする (PGlite は RESET SESSION AUTHORIZATION で戻らない)
 */
async function asGate(E, host, fn) {
  await E.pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await E.pg.query(`set session authorization ${E.sessionUser}`); await E.pg.query('set role deploy'); }
}
/** ⑤-3 の門が書く記録 (場所ごとのログイン・DB の関数だけ) */
const gateAck = (E, host, instanceId, buildId, ownership, phaseSeen, inflight = 0) => asGate(E, host, () => C.recordLegacyGateAck(E.db,
  { host, instanceId, buildId, manifest: MANIFEST, ownership, phaseSeen, inflightCount: inflight, oldestInflightAt: inflight ? new Date().toISOString() : null }));
/** 段階を進める (運用のロール master_ops) */
const advance = async (E, to, evidence, note = null) => {
  if (to === 'company_owner' || to === 'new_open') await EPOCH.seedFor(E.db, evidence && evidence.owner_hash);   // 本番 = ④a の activate
  return asRole(E, 'master_ops', () => C.advanceCutoverPhase(E.db, { to, actor: 'naka@test', evidence, note }));
};
/** 0052 (⑤-2a): new_open の前に既存の SKU の登録の状態 (backfill) が要る。運用のロール master_ops で計画を見て流す */
const backfill = (E) => asRole(E, 'master_ops', async () => {
  const p = (await E.db.query('select * from ops.registration_backfill_plan()')).rows[0];
  await E.db.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 'naka@test']);
});
/** 試験だけの切替: 門の記録を足しながら、証拠つきで new_open まで進める (本番の関数そのまま・門は弱めない)。プロセス r-a (render)・m-a (minipc) */
async function openCutover(E, ownership) {
  EPOCH.remember(ownership);
  const h = C.ownershipHash(ownership);
  const mh = await C.manifestHashOf(E.db, MANIFEST);
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, MASTER_OWNERSHIP, 'legacy_open');
  await advance(E, 'frozen', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: manualStopped(), drain: drainNow() });
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, ownership, 'frozen');
  await advance(E, 'company_owner', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, ownership, 'company_owner');
  await backfill(E);
  await advance(E, 'new_open', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
}

const E0 = await setupDb();
const { pg, db } = E0;
const q = async (sql, params) => (await db.query(sql, params)).rows;
const load = async (ownership) => {
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: `load_${crypto.randomBytes(3).toString('hex')}`, ownership, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  return r;
};

const skuId = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id;
const cur = async (code) => W.readCurrent(db, await skuId(code), TODAY);
const tokenOf = async (code) => W.editTokenOf(await cur(code));
const lastEvent = async () => (await q('select coalesce(max(event_id), 0)::text as id from events.master_change_events'))[0].id;
const nEvents = async () => Number((await q('select count(*)::int as n from events.master_change_events'))[0].n);
const uuid = () => crypto.randomUUID();
/** その DB の今の編集の印 (無い SKU = 形だけの印) */
async function tokenIn2(E, code) {
  const id = (await E.db.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0]?.id;
  return id ? W.editTokenOf(await W.readCurrent(E.db, id, TODAY)) : 'a'.repeat(64);
}
/** 画面と同じ形で保存 (seen は今の値から)・画面だけのロールで */
async function save(code, values, { E = E0, ownership = ALL_COMPANY, open = true, requestId = uuid(), reason = 'テスト', actor = 'Naka@Test', token, eventId, shippingRates = RATES, now = NOW } = {}) {
  const seen = { token: token ?? await tokenIn2(E, code), event_id: eventId ?? await lastEvent() };
  return asEditor(E, () => W.saveSku(E.db, { actor, requestId, code, reason, seen, values }, { ownership, open, now, shippingRates }));
}
const skuRow = async (code) => (await q(`select s.name, s.tax_rate::float8 as tax_rate, s.tax_class, s.handling, s.standard_price_jpy::int as price, s.shipping_code, s.shipping_method,
    s.shipping_cost_jpy::int as ship, s.reorder_months::float8 as months, s.set_sales_class_override as override, s.handling_own,
    p.name as pname, p.sales_class, p.status, pp.display_code as parent, p.parent_set_by
  from core.skus s left join core.products p on p.product_id = s.product_id left join core.products pp on pp.product_id = p.parent_product_id where s.code = $1`, [code]))[0];
const costsOf = async (code) => (await q(`select c.cost_jpy::int as jpy, c.cost_source as src, c.cost_status as st, c.valid_from::text as f, c.valid_to::text as t
  from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = $1 order by c.valid_from, c.sku_cost_id`, [code]));
const compsOf = async (code) => (await q(`select k.code, c.qty, c.sort_order as so, c.source from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
  where c.parent_sku_id = (select sku_id from core.skus where code = $1) order by c.sort_order, k.code_norm`, [code])).map((r) => [r.code, r.qty, r.so, r.source]);
const primaryOf = async (code) => (await q(`select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id join core.skus k on k.sku_id = x.sku_id
  where k.code = $1 and x.is_primary`, [code])).map((r) => r.code);
const reqRow = async (id) => (await q('select status, result, error, sku_id::text as sku_id, operation, target_code from ops.master_edit_requests where request_id = $1', [id]))[0];
let runSeq = 0;
/**
 * 観測の時刻 (NE から取った時刻)。前の観測より必ず後 (1 秒ずつ) で、今より 1 分先 (依頼の後にする)。観測を受ける窓 (5 分先まで) の中。
 * 🚨 昇格は「同じセットにもっと新しい完全な観測がある = 使わない」(#1563 仮レビュー M2) = 試験の観測の時刻は書いた順に進める
 */
let lastAt = 0;
const nextAt = () => { lastAt = Math.max(lastAt + 1000, Date.now() + 60000); return new Date(lastAt).toISOString(); };
/** NE のセットの構成の観測を 1 回分書く (観測のロール master_observer・DB の関数)。戻り値 = set_code → observation_id */
async function observe(sets, { complete = true, at = nextAt(), E = E0 } = {}) {
  const runId = `ne_run_${++runSeq}`;
  const payload = { run_id: runId, observed_at: at, complete, sets, ...(complete ? { requested: sets.length, fetched: sets.length, raw_hash: 'c'.repeat(64), source_generation: `gen_${runSeq}` } : {}) };
  await asRole(E, 'master_observer', () => W.recordNeSetObservations(E.db, payload));
  const rows = (await E.db.query('select o.observation_id::text as id, k.code from ops.ne_set_observations o join core.skus k on k.sku_id = o.set_sku_id where o.run_id = $1', [runId])).rows;
  return Object.fromEntries(rows.map((r) => [r.code, r.id]));
}
/** 観測の関数を通さずに入れる (持ち主のロールだけ・試験で「受けてから 40 時間たった」観測を作るため)。recordedAgo = 受けた時刻を今からどれだけ前にするか */
async function observeRaw(setCode, rows, at, { E = E0, recordedAgo = '0 seconds' } = {}) {
  const runId = `ne_raw_${++runSeq}`;
  await E.pg.query(`insert into ops.ne_set_observation_runs (run_id, observed_at, complete, requested_count, fetched_count, saved_count, skipped_count, raw_hash, source_generation, content_hash, recorded_at)
    values ($1, $2, true, 1, 1, 1, 0, repeat('d', 64), 'raw', md5($1), now() - $3::interval)`, [runId, at, recordedAgo]);
  const resolved = [];
  for (const r of rows) resolved.push({ sku_id: Number((await E.db.query('select sku_id from core.skus where code_norm = core.norm_code($1)', [r.code])).rows[0].sku_id), code: r.code, qty: r.qty, sort: r.sort });
  return (await E.db.query(`insert into ops.ne_set_observations (run_id, set_sku_id, rows) select $1, sku_id, $3::jsonb from core.skus where code = $2 returning observation_id::text as id`, [runId, setCode, JSON.stringify(resolved)])).rows[0].id;
}
const promote = (id, opts = {}) => W.promoteComponentRequest(opts.E ? opts.E.db : db, id, { ownership: ALL_COMPANY, now: NOW, ...opts });
/** ops.begin_master_write の引数 (直す SKU・版は DB の今の値 か versions)。#1563 R4 M2 */
const BEGIN_SQL = 'select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)';
async function beginArgs(E, code, { requestId = uuid(), actor = 'naka@test', reason = null, ownership = ALL_COMPANY, operation = 'sku_edit', token = 'b'.repeat(64), payloadHash = 'c'.repeat(64), versions } = {}) {
  const sid = (await E.db.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0].id;
  const v = versions ?? (await E.db.query('select ops.master_edit_versions($1::bigint) as v', [sid])).rows[0].v;
  return [requestId, actor, reason, JSON.stringify(ownership), operation, sid, token, payloadHash, JSON.stringify(v)];
}
/** 画面のロールの取引の中で動かして、必ず巻き戻す */
async function asEditorTx(E, fn) {
  await E.pg.query('set role master_edit');
  await E.pg.query('begin');
  try { return await fn(); } finally { await E.pg.query('rollback'); await E.pg.query('set role deploy'); }
}
const errOf = (p) => p.then(() => null, (e) => e);


console.log('切替の段階と門');

await ta('[1] 門の記録の関数: 時刻はサーバー・見た段階が今と違えば拒む・manifest の形・manifest はハッシュごとに 1 つ・画面のロールは書けない・追記だけ', async () => {
  const before = Date.now();
  const r = await gateAck(E0, 'render', 'r-a', 'r1', MASTER_OWNERSHIP, 'legacy_open');
  assert.equal(r.manifest_hash, await C.manifestHashOf(db, MANIFEST));
  assert.ok(Date.parse(r.acked_at) >= before - 5000, r.acked_at);
  await assert.rejects(() => gateAck(E0, 'render', 'r-a', 'r1', MASTER_OWNERSHIP, 'frozen'), /stale_phase/);
  const ack = (host, extra = {}) => C.recordLegacyGateAck(db, { host, instanceId: 'x', buildId: 'b', manifest: MANIFEST, ownership: MASTER_OWNERSHIP, phaseSeen: 'legacy_open', ...extra });
  await assert.rejects(() => asGate(E0, 'render', () => ack('render', { manifest: { entries: [] } })), /invalid_manifest/);
  await assert.rejects(() => asGate(E0, 'render', () => ack('render', { manifest: { entries: [{ id: 'a', kind: 'code' }, { id: 'a', kind: 'manual' }] } })), /重なって/);
  await assert.rejects(() => asGate(E0, 'render', () => ack('aws')), /知らない場所/);
  // 場所はログインで決まる (#1563 仮レビュー Low 3): Render のログインで minipc を名乗る・まとめのロールに SET ROLE しただけ (ログインは別) = 拒む (42501)
  await assert.rejects(() => asGate(E0, 'render', () => ack('minipc')), (e) => e.code === '42501' && /gate_host_mismatch/.test(e.message));
  await assert.rejects(() => asGate(E0, 'minipc', () => ack('render')), (e) => e.code === '42501' && /gate_host_mismatch/.test(e.message));
  await assert.rejects(() => asRole(E0, 'master_gate', () => ack('render')), (e) => e.code === '42501' && /gate_host_mismatch/.test(e.message));
  // 止まった記録は理由が要る・止まっていない記録に理由は付けない
  await assert.rejects(() => asGate(E0, 'render', () => ack('render', { stopped: true })), /stopped_reason/);
  await assert.rejects(() => asGate(E0, 'render', () => ack('render', { stoppedReason: '止めた' })), /止まった記録でない/);
  // 止まった記録は書きかけ 0 のときだけ (関数も表の CHECK も・#1563 R4 High 1)
  await assert.rejects(() => asGate(E0, 'render', () => ack('render', { stopped: true, stoppedReason: '止めた', inflightCount: 1, oldestInflightAt: new Date().toISOString() })), /書きかけ 0 のときだけ/);
  await assert.rejects(() => pg.query(`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, oldest_inflight_at, session_role, stopped, stopped_reason)
    select 'render', 'x', 'b', manifest_hash, repeat('a', 64), 'legacy_open', 1, now(), 'master_gate_render', true, '止めた' from ops.master_legacy_manifests limit 1`), /ck_mlga_stopped_drained/);
  await gateAck(E0, 'minipc', 'm-a', 'm1', MASTER_OWNERSHIP, 'legacy_open');
  assert.equal(Number((await q('select count(*)::int as n from ops.master_legacy_manifests'))[0].n), 1);
  assert.deepEqual((await q('select host, session_role, stopped from ops.master_legacy_gate_acks order by ack_id')).map((x) => [x.host, x.session_role, x.stopped]),
    [['render', 'master_gate_render', false], ['minipc', 'master_gate_minipc', false]]);
  assert.equal(await pgCode(asRole(E0, 'master_edit', () => pg.query("select ops.record_legacy_gate_ack('render', 'x', 'b', '{}'::jsonb, repeat('a', 64), 'legacy_open', 0, null)"))), '42501');
  await assert.rejects(() => pg.query('delete from ops.master_legacy_gate_acks'), /append-only/);
  await assert.rejects(() => pg.query('delete from ops.master_legacy_manifests'), /append-only/);
});

await ta('[1] legacy_open → frozen の門: 証拠の形・manifest・手の入口の集合・場所ごとの新しい記録・予定の build・持ち主表・差し込み口。どれか外れたら拒む (段階はそのまま)', async () => {
  const mh = await C.manifestHashOf(db, MANIFEST);
  const ev = { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: manualStopped(), drain: drainNow() };
  /** 一時の記録を足してから試す (取引ごと巻き戻す = 足した記録も残らない) */
  const refuse = async (evidence, re, setup = []) => {
    await pg.query('begin');
    try {
      for (const [sql, params] of setup) await pg.query(sql, params);
      await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['frozen', 'naka@test', JSON.stringify(evidence)]), re);
    } finally { await pg.query('rollback'); }
  };
  const rawAck = (host, inst, build, { manifest = mh, owner = LEGACY_HASH, phase = 'legacy_open', ago = '0 minutes', inflight = 0, stopped = false } = {}) =>
    [`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, oldest_inflight_at, acked_at, session_role, stopped, stopped_reason)
      values ($1, $2, $3, $4, $5, $6, $7, case when $7 > 0 then now() end, clock_timestamp() - $8::interval, 'master_gate_' || $1, $9, case when $9 then '試験' end)`,
    [host, inst, build, manifest, owner, phase, inflight, ago, stopped]];
  await assert.rejects(() => pg.query(`select ops.set_master_cutover_phase('frozen', 'naka@test', null)`), /evidence_required/);
  await refuse({ ...ev, manifest_hash: 'a'.repeat(64) }, /manifest_hash が記録された/);
  await refuse({ ...ev, owner_hash: 'x' }, /owner_hash/);
  await refuse({ ...ev, expected_builds: { render: ['r1'] } }, /expected_builds.minipc/);
  await refuse({ ...ev, drain: { ...ev.drain, done: false } }, /drain/);
  await refuse({ ...ev, drain: { ...ev.drain, checked_at: 'きのう' } }, /drain/);
  // 証拠の時刻: 先の日付・今の段階に入る前 (前の切替の試み) = 拒む (#1563 R3 High 1)
  await refuse({ ...ev, drain: { ...ev.drain, checked_at: new Date(Date.now() + 3600000).toISOString() } }, /drain\.checked_at .*今の段階に入った後/);
  await refuse({ ...ev, drain: { ...ev.drain, checked_at: '2026-01-01T00:00:00Z' } }, /drain\.checked_at .*今の段階に入った後/);
  await refuse({ ...ev, manual_entries_stopped: ev.manual_entries_stopped.map((x) => ({ ...x, at: '2030-01-09' })) }, /manual_entries_stopped の at は今の段階に入った後/);
  await refuse({ ...ev, manual_entries_stopped: ev.manual_entries_stopped.slice(0, 1) }, /手の入口/);                                           // 足りない
  await refuse({ ...ev, manual_entries_stopped: [...ev.manual_entries_stopped, { id: 'x.extra', by: 'a', at: stamp() }] }, /手の入口/);      // 多い
  await refuse({ ...ev, manual_entries_stopped: [ev.manual_entries_stopped[0], ev.manual_entries_stopped[0]] }, /重なって/);
  await refuse({ ...ev, manual_entries_stopped: [{ id: 'ne.product_screen' }] }, /id・by・at/);
  // 記録: 場所が無い (minipc が古い = 15 分より前) / 予定に無い build のプロセスが動いている / manifest が違う / 持ち主表が違う / 見た段階が違う
  await pg.query("insert into ops.master_legacy_manifests (manifest_hash, entries) values (repeat('e', 64), '{\"entries\":[{\"id\":\"z\",\"kind\":\"code\"}]}')");
  await refuse(ev, /予定に無い build rX/, [rawAck('render', 'r-z', 'rX')]);
  await refuse(ev, /古い入口の一覧が違う/, [rawAck('minipc', 'm-z', 'm1', { manifest: 'e'.repeat(64) })]);
  await refuse(ev, /持ち主表のハッシュが違う/, [rawAck('minipc', 'm-z', 'm1', { owner: 'f'.repeat(64) })]);
  await refuse(ev, /見た段階が frozen/, [rawAck('minipc', 'm-z', 'm1', { phase: 'frozen' })]);
  await refuse(ev, /minipc\/m-z: 書きかけが 2 件/, [rawAck('minipc', 'm-z', 'm1', { inflight: 2 })]);   // → frozen も書きかけ 0 (#1563 R3 High 1)
  // 前からある食い違った行 (止まった・書きかけ 1。CHECK の前に入った行のつもりで CHECK を外して入れる) も、止まったプロセスを外す前に書きかけを見て拒む (#1563 R4 High 1)
  await refuse(ev, /render\/r-z: 書きかけが 1 件/, [['alter table ops.master_legacy_gate_acks drop constraint ck_mlga_stopped_drained', []], rawAck('render', 'r-z', 'r1', { stopped: true, inflight: 1 })]);
  // 黙っているプロセス (今までに記録があり、最後の記録が 15 分より前・止まった記録なし) = 拒む。止まった記録が最後なら外す (#1563 仮レビュー Low 3)。年齢では外さない (R3 High 1)
  await refuse(ev, /render\/r-z: 黙っている/, [rawAck('render', 'r-z', 'r1', { ago: '1 hour' })]);
  await refuse(ev, /minipc\/m-z: 黙っている/, [rawAck('minipc', 'm-z', 'm1', { ago: '20 minutes' }), rawAck('minipc', 'm-z', 'm1', { ago: '30 minutes', stopped: true })]);   // 止まった後にまた動いて黙った
  const passes = async (setup) => {
    await pg.query('begin');
    try {
      for (const [sql, params] of setup) await pg.query(sql, params);
      return (await pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb) as r', ['frozen', 'naka@test', JSON.stringify(ev)])).rows[0].r;
    } finally { await pg.query('rollback'); }
  };
  const ok1 = await passes([rawAck('render', 'r-z', 'r1', { ago: '1 hour' }), rawAck('render', 'r-z', 'r1', { ago: '10 minutes', stopped: true })]);
  assert.deepEqual(ok1.acks.map((a) => [a.host, a.instance_id, a.stopped ?? false]), [['minipc', 'm-a', false], ['render', 'r-a', false], ['render', 'r-z', true]]);
  // 同じ場所に新しいプロセス (r-a) があっても、25 時間前に黙った古いプロセス (r-y) は止まった記録が要る (R3 High 1)
  await refuse(ev, /render\/r-y: 黙っている/, [rawAck('render', 'r-y', 'rOLD', { ago: '25 hours' })]);
  await refuse(ev, /render\/r-y: 黙っている/, [rawAck('render', 'r-y', 'rOLD', { ago: '400 days' })]);
  const ok2 = await passes([rawAck('render', 'r-y', 'rOLD', { ago: '25 hours' }), rawAck('render', 'r-y', 'rOLD', { ago: '1 minute', stopped: true })]);
  assert.deepEqual(ok2.acks.map((a) => [a.instance_id, a.stopped ?? false]), [['m-a', false], ['r-a', false], ['r-y', true]]);
  // 止まった記録だけの場所 = 生きている記録が無い = 拒む
  await refuse(ev, /minipc: 15 分以内の記録が無い/, [rawAck('minipc', 'm-a', 'm1', { stopped: true })]);
  // 前提の差し込み口 (#1563 R3): 後の migration が表に足した関数を全部呼ぶ (2 つ足せば 2 つとも効く・前の項目を消さない)
  const prereqFn = (name, body) => [`create function ops.${name}(p_from text, p_to text) returns text[] language plpgsql stable as $$ begin ${body} end $$`, []];
  await refuse(ev, /prereq_failed: t1_backfill: 構成の写しの作り直しがまだ \/ t2_epoch: 持ち主の世代が違う \(legacy_open→frozen\)/, [
    prereqFn('t_prereq_a', "return array['構成の写しの作り直しがまだ'];"),
    prereqFn('t_prereq_b', "return array[format('持ち主の世代が違う (%s→%s)', p_from, p_to)];"),
    prereqFn('t_prereq_ok', 'return array[]::text[];'),
    ["insert into ops.master_cutover_prereq_checks (name, fn) values ('t2_epoch', 'ops.t_prereq_b(text, text)'), ('t1_backfill', 'ops.t_prereq_a(text, text)'), ('t3_ok', 'ops.t_prereq_ok(text, text)')", []]]);
  // 足せない関数: ops の外・形が違う・集める関数そのもの (持ち主でも trigger が拒む) / 持ち主でないロールは表に書けない
  const regBad = async (sqlFn, fnSig, re) => {
    await pg.query('begin');
    try {
      if (sqlFn) await pg.query(sqlFn);
      await assert.rejects(() => pg.query('insert into ops.master_cutover_prereq_checks (name, fn) values ($1, $2::regprocedure)', ['t_bad', fnSig]), re);
    } finally { await pg.query('rollback'); }
  };
  await regBad("create function public.t_prereq_pub(p_from text, p_to text) returns text[] language sql as $$ select array[]::text[] $$", 'public.t_prereq_pub(text, text)', /prereq_check_invalid: 関数が ops の中に無い/);
  await regBad("create function ops.t_prereq_shape(p_from text) returns text[] language sql as $$ select array[]::text[] $$", 'ops.t_prereq_shape(text)', /prereq_check_invalid: 関数の形/);
  await regBad(null, 'ops.master_cutover_prereq_problems(text, text)', /prereq_check_invalid: 集める関数/);
  for (const role of ['master_ops', 'master_edit', 'master_gate', 'master_observer']) {
    assert.equal(await pgCode(asRole(E0, role, () => pg.query("insert into ops.master_cutover_prereq_checks (name, fn) values ('t_x', 'ops.master_cutover_ack_fresh_minutes()')"))), '42501', role);
  }
  assert.deepEqual((await q('select ops.master_cutover_prereq_problems($1, $2) as p', ['legacy_open', 'frozen']))[0].p, []);   // 巻き戻した = 何も足されていない
  // 前提の表は追記だけ (変える・消す・truncate = 拒む。前の項目を消さない・#1563 R4 Low 3)。関数の中身を変えるのは create or replace
  await pg.query('begin');
  try {
    await pg.query(prereqFn('t_prereq_c', 'return array[]::text[];')[0]);
    await pg.query("insert into ops.master_cutover_prereq_checks (name, fn) values ('t_c', 'ops.t_prereq_c(text, text)')");
    for (const sql of ["update ops.master_cutover_prereq_checks set name = 't_d'", "update ops.master_cutover_prereq_checks set fn = 'ops.t_prereq_c(text, text)'",
      'delete from ops.master_cutover_prereq_checks', 'truncate ops.master_cutover_prereq_checks']) {
      await pg.query('savepoint s');
      await assert.rejects(() => pg.query(sql), /append-only/, sql);
      await pg.query('rollback to savepoint s');
    }
  } finally { await pg.query('rollback'); }
  assert.equal((await C.readCutoverPhase(db)).phase, 'legacy_open');
  // 飛ばす・戻す・知らない段階・直接の UPDATE / DELETE・読めない = 閉じている
  await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['company_owner', 'naka@test', JSON.stringify(ev)]), /one_way/);
  await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['open', 'naka@test', JSON.stringify(ev)]), /知らない段階/);
  await assert.rejects(() => pg.query("update ops.master_cutover_state set phase = 'new_open'"), /set_master_cutover_phase/);
  await assert.rejects(() => pg.query('delete from ops.master_cutover_state'), /消さない/);
  const s = await C.readCutoverPhase(db);
  assert.deepEqual([s.readable, s.phase, s.owner_hash], [true, 'legacy_open', null]);
  assert.equal(C.newEntryWritable(s), false); assert.equal(C.legacyWritable(s), true);
  const broken = await C.readCutoverPhase({ query: async () => { throw new Error('connection terminated'); } });
  assert.deepEqual([broken.readable, broken.phase, C.newEntryWritable(broken), C.legacyWritable(broken)], [false, null, false, false]);
});


await ta('[2] 段階が new_open でない = 持ち主 company + MASTER_EDIT_OPEN でも 409 切替前・何も書かない。変わる項目が無い保存も 409 (done を残さない)', async () => {
  const before = await nEvents();
  const id = uuid();
  const e = await rejectsWith(save('s001', { name: '直した名前' }, { requestId: id }), 409, 'before_cutover');
  assert.equal(e.extra.phase, 'legacy_open');
  assert.match(e.message, /切替の段階が legacy_open/);
  assert.equal((await skuRow('s001')).name, '単品 1');
  assert.equal(await nEvents(), before);
  const r = await reqRow(id);
  assert.equal(r.status, 'failed'); assert.equal(r.error.reason, 'before_cutover'); assert.equal(r.target_code, 's001'); assert.ok(r.sku_id);
  const noop = uuid();
  await rejectsWith(save('s001', { name: '単品 1' }, { requestId: noop }), 409, 'before_cutover');
  assert.equal((await reqRow(noop)).status, 'failed');
  assert.equal(Number((await q("select count(*)::int as n from ops.master_edit_requests where status = 'done'"))[0].n), 0);
});

await ta('[2] 場所が足りない・古い記録だけ = 拒む → そろえば frozen。company_owner / new_open は段階に入った後の記録・書きかけ 0・持ち主表のハッシュが要る。保存は記録と同じ持ち主表だけ', async () => {
  const mh = await C.manifestHashOf(db, MANIFEST);
  const h = C.ownershipHash(ALL_COMPANY);
  // 場所が足りない / 古い記録だけ: 別の DB で (この DB は [1] で両方の場所の記録がある)。証拠の時刻は E2 の段階が始まった後
  const E2 = await setupDb();
  const ev = { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: manualStopped(), drain: drainNow() };
  await gateAck(E2, 'render', 'r-a', 'r1', MASTER_OWNERSHIP, 'legacy_open');
  await assert.rejects(() => advance(E2, 'frozen', ev), /minipc: 15 分以内の記録が無い/);
  await E2.pg.query(`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, acked_at, session_role)
    values ('minipc', 'm-a', 'm1', $1, $2, 'legacy_open', 0, clock_timestamp() - interval '20 minutes', 'master_gate_minipc')`, [mh, LEGACY_HASH]);
  await assert.rejects(() => advance(E2, 'frozen', ev), /minipc: 15 分以内の記録が無い/);
  await E2.pg.close();
  // この DB で進める
  const r1 = await advance(E0, 'frozen', ev, '試験');
  assert.deepEqual(r1.acks.map((a) => [a.host, a.instance_id, a.build_id]), [['minipc', 'm-a', 'm1'], ['render', 'r-a', 'r1']]);
  const ev2 = { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h };
  // frozen の前の記録 (legacy_open を見た) だけ = 拒む
  await assert.rejects(() => advance(E0, 'company_owner', ev2), /見た段階が legacy_open/);
  await gateAck(E0, 'render', 'r-a', 'r1', ALL_COMPANY, 'frozen');
  await gateAck(E0, 'minipc', 'm-a', 'm1', ALL_COMPANY, 'frozen', 2);   // minipc に書きかけが 2 件
  await assert.rejects(() => advance(E0, 'company_owner', ev2), /m-a: 書きかけが 2 件/);
  await gateAck(E0, 'minipc', 'm-a', 'm1', ALL_COMPANY, 'frozen');
  // 段階に入る前の記録 (時刻を frozen の前にした別のプロセス) = 拒む
  await pg.query('begin');
  await pg.query(`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, acked_at, session_role)
    select 'render', 'r-old', 'r1', $1, $2, 'frozen', 0, changed_at - interval '1 second', 'master_gate_render' from ops.master_cutover_state`, [mh, h]);
  await assert.rejects(() => pg.query('select ops.set_master_cutover_phase($1, $2, $3::jsonb)', ['company_owner', 'naka@test', JSON.stringify(ev2)]), /r-old: 記録が今の段階に入る前/);
  await pg.query('rollback');
  await assert.rejects(() => advance(E0, 'company_owner', { ...ev2, owner_hash: 'x' }), /owner_hash/);
  await advance(E0, 'company_owner', ev2);
  // DB でも段階を見る (#1563 R3 M2): 持ち主表が段階の記録と同じでも、new_open の前は画面のロールの書き込みを始められない
  const argsCo = await beginArgs(E0, 's001');
  await assert.rejects(() => asEditor(E0, () => pg.query(BEGIN_SQL, argsCo)), /before_cutover: 切替の段階が company_owner/);
  await gateAck(E0, 'render', 'r-a', 'r1', ALL_COMPANY, 'company_owner'); await gateAck(E0, 'minipc', 'm-a', 'm1', ALL_COMPANY, 'company_owner');
  await assert.rejects(() => advance(E0, 'new_open', ev2), /prereq_failed: 0052_registrations: backfill_missing/);   // 0052 (⑤-2a): backfill の前は new_open に進めない (差し込み口の表の 1 行)
  await backfill(E0);
  await assert.rejects(() => advance(E0, 'new_open', { ...ev2, owner_hash: 'a'.repeat(64) }), /company_owner のときと違う/);
  await advance(E0, 'new_open', ev2);
  const ev3 = await q('select from_phase, to_phase, evidence, jsonb_array_length(acks) as n from ops.master_cutover_events order by event_id');
  assert.deepEqual(ev3.map((x) => [x.from_phase, x.to_phase, x.n]), [['legacy_open', 'frozen', 2], ['frozen', 'company_owner', 2], ['company_owner', 'new_open', 2]]);
  assert.deepEqual(ev3[0].evidence, ev);
  await assert.rejects(() => advance(E0, 'legacy_open', ev2), /one_way/);
  await assert.rejects(() => pg.query('delete from ops.master_cutover_events'), /append-only/);
  const s = await C.readCutoverPhase(db);
  assert.deepEqual([s.phase, s.owner_hash], ['new_open', h]);
  assert.equal(C.newEntryWritable(s, ALL_COMPANY), true);
  assert.equal(C.newEntryWritable(s, MASTER_OWNERSHIP), false);
  // 動いているコードの持ち主表が記録と違う = 保存しない (今の本番の持ち主表 = 全部 load のまま動かした)
  const e = await rejectsWith(save('s001', { name: '直した名前' }, { ownership: MASTER_OWNERSHIP }), 409, 'before_cutover');
  assert.match(e.message, /持ち主表が切替のときの記録と違う/);
  const e2 = await rejectsWith(save('s001', { name: '直した名前' }, { open: false }), 409, 'before_cutover');
  assert.match(e2.message, /保存はまだ開いていません/);
  assert.equal((await skuRow('s001')).name, '単品 1');
});


await ta('[2] 一部だけ company で切り替えた DB: 名前 (company) + 税率 (load) = 全部断る・名前だけは通る。構成の依頼も sku_components が load なら 409', async () => {
  const own = withOwn({ 'skus.name': 'company', 'products.name': 'company', 'skus.shipping': 'company' });
  const E1 = await setupDb();
  await openCutover(E1, own);
  const tok = (code) => tokenIn2(E1, code);
  const e = await rejectsWith(save('s002', { name: '名前 2 改', tax_rate: '10' }, { E: E1, ownership: own, token: await tok('s002') }), 409, 'before_cutover');
  assert.deepEqual(e.extra.fields, ['税率']);
  const r = await save('s002', { name: '名前 2 改', tax_rate: '8' }, { E: E1, ownership: own, token: await tok('s002') });
  assert.deepEqual(r.changed.map((c) => c.field), ['name']);
  const e3 = await rejectsWith(save('set001', { components: [{ code: 's001', qty: 1 }] }, { E: E1, ownership: own, token: await tok('set001') }), 409, 'before_cutover');
  assert.deepEqual([e3.extra.fields, e3.extra.load_keys], [['構成の依頼'], ['sku_components']]);
  assert.equal(Number((await E1.db.query('select count(*)::int as n from ops.sku_component_requests')).rows[0].n), 0);
  await E1.pg.close();
});

console.log('\n単品の保存');

await ta('[3] 単品: 名前・取扱・売価・税率・分類・送料・月数・代表の仕入先・代表 (親) を 1 回で。変わった列だけ・商品の行もそろう (画面のロールで)', async () => {
  const id = uuid();
  const r = await save('s003', {
    name: '単品 3 改', handling: 'active', standard_price: '1,280', tax_rate: '8', sales_class: '2', shipping_code: 'S01', reorder_months: '1.5',
    primary_supplier: '2', parent_code: 's001',
  }, { requestId: id, reason: '棚卸で見直し' });
  assert.deepEqual(r.changed.map((c) => c.field).sort(), ['name', 'parent_code', 'primary_supplier', 'reorder_months', 'sales_class', 'shipping_code', 'standard_price', 'tax_rate'].sort());
  assert.deepEqual(await skuRow('s003'), {
    name: '単品 3 改', tax_rate: 0.08, tax_class: 'REDUCED_8', handling: 'active', price: 1280, shipping_code: 'S01', shipping_method: 'ゆうパケット', ship: 210, months: 1.5,
    override: null, handling_own: null, pname: '単品 3 改', sales_class: 2, status: 'active', parent: 's001', parent_set_by: 'manual',
  });
  assert.deepEqual(await primaryOf('s003'), ['0002']);
  assert.equal((await reqRow(id)).status, 'done');
  assert.deepEqual((await reqRow(id)).result.changed.length, 8);
  assert.ok(r.ne_steps.some((s) => /翌朝の照合/.test(s)));
});

await ta('[3] 変更の記録: トリガーが actor = human・メール・portal_master_edit・request_id・理由を残す。設定は取引の外に漏れない', async () => {
  const id = uuid();
  await save('s002', { name: '名前 2 改 2', handling: 'discontinued' }, { requestId: id, reason: '取扱をやめた', actor: 'Other@Test' });
  const ev = await q('select entity_type, attribute, actor_type, actor_id, source_system, reason_text, db_user from events.master_change_events where request_id = $1 order by event_id', [id]);
  assert.ok(ev.length >= 4, JSON.stringify(ev));
  for (const e of ev) assert.deepEqual([e.actor_type, e.actor_id, e.source_system, e.reason_text, e.db_user], ['human', 'other@test', 'portal_master_edit', '取扱をやめた', 'master_edit']);
  assert.deepEqual(ev.filter((e) => e.entity_type === 'product').map((e) => e.attribute).sort(), ['name', 'status']);
  const skuEv = await q("select attribute from events.master_change_events where request_id = $1 and entity_type = 'sku' and entity_id = $2", [id, await skuId('s002')]);
  assert.deepEqual(skuEv.map((e) => e.attribute).sort(), ['handling', 'name']);
  assert.equal((await q("select current_setting('core.actor_type', true) as a"))[0].a || '', '');
});

await ta('[4] 保存した値は夜間ロード (持ち主 company) を 2 回流しても残る。load に戻すと夜間ロードが戻す', async () => {
  const snap = async () => ({ s003: await skuRow('s003'), s002: await skuRow('s002'), prim: await primaryOf('s003') });
  const before = await snap();
  await load(ALL_COMPANY); await load(ALL_COMPANY);
  assert.deepEqual(await snap(), before);
  await load(MASTER_OWNERSHIP);
  assert.equal((await skuRow('s003')).name, '単品 3');
  assert.equal((await skuRow('s003')).tax_rate, 0.1);
  assert.equal((await skuRow('s003')).parent, 's001');   // 人が決めた親 (manual) は load でも触らない (0036)
  await save('s003', { name: '単品 3 改', tax_rate: '8' });   // 以降の試験の前提
});

console.log('\n同じ request_id');

await ta('[5] 同じ中身 = 前の結果 (記録は増えない) / 違う中身・違う人・違う SKU = 409 / 失敗 = 同じ誤り / 記録は追記だけ', async () => {
  const id = uuid();
  const token = await tokenOf('s004');
  const eventId = await lastEvent();
  const first = await save('s004', { sales_class: '3' }, { requestId: id, token, eventId });
  const n = await nEvents();
  const again = await save('s004', { sales_class: '3' }, { requestId: id, token, eventId });
  assert.equal(again.replayed, true);
  assert.deepEqual(again.changed, first.changed);
  assert.equal(await nEvents(), n);
  await rejectsWith(save('s004', { sales_class: '2' }, { requestId: id, token, eventId }), 409, 'request_id_reused');
  await rejectsWith(save('s004', { sales_class: '3' }, { requestId: id, token, eventId, actor: 'other@test' }), 409, 'request_id_reused');
  await rejectsWith(save('s002', { sales_class: '3' }, { requestId: id }), 409, 'request_id_reused');   // 違う SKU へ同じ番号
  const bad = uuid();
  const t2 = await tokenOf('s004'); const e2 = await lastEvent();
  await rejectsWith(save('s004', { sales_class: '1' }, { requestId: bad, token: t2, eventId: e2, open: false }), 409, 'before_cutover');
  const e = await rejectsWith(save('s004', { sales_class: '1' }, { requestId: bad, token: t2, eventId: e2, open: false }), 409, 'before_cutover');
  assert.equal(e.extra.replayed, true);
  assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [bad]))[0].n), 1);
  assert.deepEqual((await q('select distinct status from ops.master_edit_requests order by 1')).map((x) => x.status), ['done', 'failed']);
  await assert.rejects(() => pg.query(`update ops.master_edit_requests set status = 'failed' where request_id = $1`, [id]), /append-only/);
  await assert.rejects(() => pg.query('delete from ops.master_edit_requests where request_id = $1', [id]), /append-only/);
});

console.log('\n編集の印 (楽観ロック)');

await ta('[6] 画面を開いた後に別の保存が SKU を変えた = 409 とその間の変更・何も書かない', async () => {
  const token = await tokenOf('s004');
  const since = await lastEvent();
  await save('s004', { standard_price: '1500' }, { actor: 'other@test' });
  const n = await nEvents();
  const e = await rejectsWith(save('s004', { name: '上書き' }, { token, eventId: since, reason: null }), 409, 'version_conflict');
  assert.ok(e.extra.events.some((x) => x.attribute === 'standard_price_jpy' && x.actor_id === 'other@test'), JSON.stringify(e.extra.events));
  assert.equal(await nEvents(), n);
  assert.equal((await skuRow('s004')).name, '単品 4 (分類・原価なし)');
});

await ta('[6] 行が増えた・変わった (仕入先ごとの商品・JAN・商品の行・構成品の値・含むセット) でも編集の印は変わる', async () => {
  const s001 = await skuId('s001');
  const pid = (await q('select product_id::text as id from core.skus where code = $1', ['s001']))[0].id;
  const steps = [
    ["update core.supplier_skus set vendor_code = 'AMC-XXX' where sku_id = $1", [s001]],
    ["insert into core.supplier_skus (company_id, supplier_id, sku_id) select 1, supplier_id, $1 from core.suppliers where code = '0002'", [s001]],   // 行が増えた (phantom)
    ["insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4900000000001', 'manual', 'human')", [pid]],
    ['update core.products set sales_class = 1 where product_id = $1', [pid]],
  ];
  for (const [sql, params] of steps) {
    const t0 = await tokenOf('s001');
    await pg.query(sql, params);
    assert.notEqual(await tokenOf('s001'), t0, sql);
    await rejectsWith(save('s001', { name: '変える' }, { token: t0 }), 409, 'version_conflict');
  }
  await pg.query('update core.products set sales_class = 3 where product_id = $1', [pid]);
  const t1 = await tokenOf('set001');
  await pg.query("update core.skus set standard_price_jpy = 1001 where code = 's002'");
  assert.notEqual(await tokenOf('set001'), t1);
  const t2 = await tokenOf('s002');
  await pg.query("update core.skus set standard_price_jpy = 5000 where code = 'set001'");
  assert.notEqual(await tokenOf('s002'), t2);
});

await ta('[6] 0057 の最初の夜間ロードの登録日の埋め (version が 1 回進む): 開いていた単品と、それを含むセット (構成の依頼の画面も同じ印) の保存は 409・2 回目のロードは印を変えない (#1617 Codex R1 Low)', async () => {
  assert.equal((await q("select registered_on from core.skus where code = 's001'"))[0].registered_on, null);
  const tSet = await tokenOf('set001'); const tS = await tokenOf('s001'); const since = await lastEvent();
  const withReg = () => { const p = makePlan(); p.skus.find((x) => x.code === 's001').registeredOn = '2026-01-02'; p.registered = { available: true, runId: 'pml_test' }; return p; };
  const r = await runInitialLoad(db, withReg(), { log: quiet, runId: 'load_regdate_1', ownership: ALL_COMPANY, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  assert.equal((await q("select registered_on::text as d from core.skus where code = 's001'"))[0].d, '2026-01-02');
  assert.notEqual(await tokenOf('set001'), tSet); assert.notEqual(await tokenOf('s001'), tS);
  const n = await nEvents();
  // セットの「その間の変更」はセット自身の記録だけ = 構成品の登録日の埋めは並ばない (409 で開き直してもらう。中身は変わっていない)
  await rejectsWith(save('set001', { standard_price: '4321' }, { token: tSet, eventId: since }), 409, 'version_conflict');
  const e = await rejectsWith(save('s001', { name: '古い画面' }, { token: tS, eventId: since }), 409, 'version_conflict');
  assert.ok(e.extra.events.some((x) => x.attribute === 'registered_on'), JSON.stringify(e.extra.events));
  assert.equal(await nEvents(), n);
  // 2 回目 (同じ材料) は何も変えない = 開き直した画面の印はそのまま
  const tSet2 = await tokenOf('set001');
  assert.equal((await runInitialLoad(db, withReg(), { log: quiet, runId: 'load_regdate_2', ownership: ALL_COMPANY, now: LOAD_NOW })).ok, true);
  assert.equal(await tokenOf('set001'), tSet2);
});

console.log('\n入力の検証');

await ta('[7] 形の誤り (400): 名前・売価・税率・分類・月数・原価・構成・知らない項目・種類に無い項目・番号・編集の印', async () => {
  const b = async (code, values, re) => { const e = await rejectsWith(save(code, values), 400); if (re) assert.match(e.message, re); };
  await b('s001', { name: '' }, /空/);
  await b('s001', { name: 'EMPTY' }, /empty/);
  await b('s001', { name: 'a\nb' }, /改行/);
  await b('s001', { name: 'あ'.repeat(256) }, /255/);
  await b('s001', { standard_price: '0' }, /1〜/);
  await b('s001', { standard_price: '12.5' }, /整数/);
  await b('s001', { tax_rate: '5' }, /8% か 10%/);
  await b('s001', { tax_rate: '' }, /空にできません/);
  await b('s001', { sales_class: '5' }, /1〜4/);
  await b('s001', { reorder_months: '61' }, /0〜60/);
  await b('s001', { reorder_months: '1.25' }, /小数/);
  await b('s001', { cost: { jpy: '-1', reason: 'x' } }, /0〜/);
  await b('s001', { cost: { jpy: '100' } }, /理由/);
  await b('s001', { handling: 'paused' }, /取扱/);
  await b('s001', { primary_supplier: '0003' }, /取引停止/);
  await b('s001', { primary_supplier: '0099' }, /ありません/);
  await b('s001', { parent_code: 's001' }, /自分自身/);
  await b('s001', { parent_code: 'set001' }, /単品か代表の名札/);
  await b('s001', { parent_code: 'nope' }, /見つかりません/);
  await b('s001', { shipping_code: 'S99' }, /送料の表にありません/);
  await b('s001', { components: [{ code: 's002', qty: 1 }] }, /単品では直せません/);
  await b('set001', { tax_rate: '10' }, /セットでは直せません/);
  await b('set001', { components: [] }, /1〜20/);
  await b('set001', { components: Array.from({ length: 21 }, (_, i) => ({ code: `x${i}`, qty: 1 })) }, /1〜20/);
  await b('set001', { components: [{ code: 's001', qty: 0 }] }, /1〜999/);
  await b('set001', { components: [{ code: 's001', qty: 1000 }] }, /1〜999/);
  await b('set001', { components: [{ code: 's001', qty: 1 }, { code: 'S001', qty: 2 }] }, /2 回/);
  await b('set001', { components: [{ code: 'set001', qty: 1 }] }, /セット自身/);
  await b('set001', { components: [{ code: 'set004', qty: 1 }] }, /入れ子/);
  await b('set001', { components: [{ code: 'nope', qty: 1 }] }, /ありません/);
  await b('s001', { color: 'red' }, /知らない項目/);
  await rejectsWith(W.saveSku(db, { actor: 'naka@test', requestId: 'x', code: 's001', seen: { token: 'a'.repeat(64) }, values: {} }, { open: true, ownership: ALL_COMPANY }), 400);
  await rejectsWith(W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 's001', seen: {}, values: {} }, { open: true, ownership: ALL_COMPANY }), 400);
  await rejectsWith(W.saveSku(db, { actor: '', requestId: uuid(), code: 's001', seen: { token: 'a'.repeat(64) }, values: {} }, { open: true, ownership: ALL_COMPANY }), 400);
  await rejectsWith(save('nope', { name: 'x' }, { token: 'a'.repeat(64) }), 404, 'not_found');
  const e = await rejectsWith(save('s001', { shipping_code: 'S01' }, { shippingRates: null }), 503, 'shipping_rates_unavailable');
  assert.equal(e.extra.field, 'shipping_code');
});

await ta('[7] 新しい商品コードの形 (⑤-2 で使う): 小文字の英数字・- _・30 字まで・大文字は禁止・前後の空白・SET- で始まらない', () => {
  for (const ok of ['abc-01', 'a_b', '0', 'x'.repeat(30)]) assert.deepEqual(W.validateNewSkuCode(ok), { ok: true, code: ok, message: null }, ok);
  const bad = (v, re) => { const r = W.validateNewSkuCode(v); assert.equal(r.ok, false, String(v)); assert.match(r.message, re); };
  bad('', /入れて/); bad(null, /入れて/); bad(' abc', /空白/); bad('Abc', /大文字/); bad('ABC-01', /大文字/);
  bad('abc 01', /使える文字/); bad('ａｂｃ', /使える文字/); bad('x'.repeat(31), /30 字/); bad('abc.01', /使える文字/); bad('set-abc', /set-/);
});

console.log('\nセットの導く値');

await ta('[8] 単品の税率を変えると、含むセットの税率・税区分を同じ取引で (記録も同じ request_id)。NE でやることに出る', async () => {
  assert.deepEqual([(await skuRow('set001')).tax_rate, (await skuRow('set001')).tax_class], [0.08, 'MIXED']);
  const id = uuid();
  const r = await save('s002', { tax_rate: '10' }, { requestId: id });
  assert.deepEqual(r.derived.map((d) => [d.code, d.col, d.to]), [['set001', 'tax_rate', { rate: 0.1, class: 'STANDARD_10' }]]);
  assert.deepEqual([(await skuRow('set001')).tax_rate, (await skuRow('set001')).tax_class], [0.1, 'STANDARD_10']);
  const ev = await q("select e.attribute from events.master_change_events e where e.request_id = $1 and e.entity_type = 'sku' and e.entity_id = (select sku_id from core.skus where code = 'set001')", [id]);
  assert.deepEqual(ev.map((e) => e.attribute).sort(), ['tax_class', 'tax_rate']);
  assert.ok(r.ne_steps.some((s) => /set001 の税率/.test(s)));
});

await ta('[8] 単品を中止にすると含むセットも中止 / 取扱中に戻しても「セット自身の取扱」が決まっていないセットは中止のまま (気をつけること)', async () => {
  let r = await save('s001', { handling: 'discontinued' });
  assert.deepEqual(r.derived.filter((d) => d.col === 'handling').map((d) => d.code).sort(), ['set001', 'set004', 'set005', 'set006']);
  assert.equal((await skuRow('set001')).handling, 'discontinued');
  r = await save('s001', { handling: 'active' });
  assert.equal((await skuRow('set001')).handling, 'discontinued');
  assert.ok(r.warnings.some((w) => /セット set001 は「セット自身の取扱」が決まっていない/.test(w)), JSON.stringify(r.warnings));
  r = await save('set001', { handling_own: 'active' });
  assert.deepEqual(r.derived.map((d) => [d.col, d.to]), [['handling', 'active']]);
  assert.equal((await skuRow('set001')).handling, 'active');
});

console.log('\n原価');

await ta('[9] 単品の原価: 今日から (今の行は昨日で閉じる)・含むセットの合計も今日から・過去の行は変えない', async () => {
  assert.deepEqual((await costsOf('s001')).map((c) => [c.jpy, c.f, c.t]), [[100, '2030-01-05', null]]);
  const r = await save('s001', { cost: { jpy: '130', reason: '値上げ' } });
  assert.deepEqual((await costsOf('s001')).map((c) => [c.jpy, c.src, c.st, c.f, c.t]), [[100, 'ne', 'COMPLETE', '2030-01-05', '2030-01-09'], [130, 'manual', 'COMPLETE', TODAY, null]]);
  // set001 = s001×2 + s002×1 = 260 + 200 / set005 = s001 + s005 = 130 + 80 / set004・set006 は原価の無い構成品がある (行も無いので何もしない)
  assert.deepEqual(r.derived.filter((d) => d.col === 'cost').map((d) => [d.code, d.from, d.to]), [['set001', 400, 460], ['set005', 180, 210]]);
  assert.deepEqual((await costsOf('set001')).map((c) => [c.jpy, c.src, c.f, c.t]), [[400, 'set_calc', '2030-01-05', '2030-01-09'], [460, 'set_calc', TODAY, null]]);
});

await ta('[9] 今日 2 回目は今日の行を入れ替える (期間を重ねない)。同じ値なら変わりなし。先の日付の行があれば入れない', async () => {
  await save('s001', { cost: { jpy: '120', reason: '打ち間違い' } });
  assert.deepEqual((await costsOf('s001')).map((c) => [c.jpy, c.f, c.t]), [[100, '2030-01-05', '2030-01-09'], [120, TODAY, null]]);
  assert.deepEqual((await costsOf('set001')).map((c) => [c.jpy, c.f, c.t]), [[400, '2030-01-05', '2030-01-09'], [440, TODAY, null]]);
  assert.equal((await save('s001', { cost: { jpy: '120', reason: '同じ' } })).no_change, true);
  const overlap = await q(`select count(*)::int as n from core.sku_costs a join core.sku_costs b on a.sku_id = b.sku_id and a.sku_cost_id < b.sku_cost_id
    and a.valid_from <= coalesce(b.valid_to, 'infinity') and b.valid_from <= coalesce(a.valid_to, 'infinity')`);
  assert.equal(overlap[0].n, 0);
  await pg.query("update core.sku_costs set valid_to = '2030-01-14' where valid_to is null and sku_id = (select sku_id from core.skus where code = 's002')");
  await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 210, 'ne', 'COMPLETE', '2030-01-15' from core.skus where code = 's002'");
  await rejectsWith(save('s002', { cost: { jpy: '205', reason: 'x' } }), 400, 'cost_future');
  await pg.query("delete from core.sku_costs where valid_from = '2030-01-15'");
  await pg.query("update core.sku_costs set valid_to = null where valid_to = '2030-01-14'");
});

await ta('[9] 期間の重なりは DB が拒む (この画面・昇格の書き込み)。夜間ロード・ほかの書き手は見ない (今の動きを止めない = ⑥ の前提)', async () => {
  const sid = await skuId('s003');
  const asPortal = async (sql) => {
    await pg.query('begin');
    try { await pg.query("select set_config('core.source_system', 'portal_master_edit', true)"); await pg.query(sql, [sid]); await pg.query('commit'); }
    catch (e) { await pg.query('rollback'); throw e; }
  };
  await assert.rejects(() => asPortal("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 1, 'manual', 'COMPLETE', '2030-01-06', '2030-01-07')"), /sku_cost_overlap/);
  await asPortal("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 1, 'manual', 'COMPLETE', '2029-12-01', '2030-01-04')");
  await assert.rejects(() => asPortal("update core.sku_costs set valid_to = '2030-01-05' where sku_id = $1 and valid_from = '2029-12-01'"), /sku_cost_overlap/);
  await pg.query("delete from core.sku_costs where sku_id = $1 and valid_from = '2029-12-01'", [sid]);
  for (const src of ['company_db_load', '']) {
    await pg.query('begin');
    await pg.query("select set_config('core.source_system', $1, true)", [src]);
    await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 1, 'ne', 'COMPLETE', '2030-01-06', '2030-01-06')", [sid]);
    await pg.query('rollback');
  }
});

await ta('[9] 「今日」は東京の日付 (UTC 14:59:59 と 15:00 の境目・DB の Asia/Tokyo と同じ答え)', async () => {
  for (const [iso, want] of [['2030-01-10T14:59:59Z', '2030-01-10'], ['2030-01-10T15:00:00Z', '2030-01-11'], ['2030-12-31T15:00:00Z', '2031-01-01'], ['2030-01-10T00:00:00Z', '2030-01-10']]) {
    assert.equal(W.jstDate(new Date(iso)), want, iso);
    assert.equal((await q(`select (($1::timestamptz) at time zone 'Asia/Tokyo')::date::text as d`, [iso]))[0].d, want, `DB ${iso}`);
  }
  await save('s005', { cost: { jpy: '81', reason: '日の境目' } }, { now: new Date('2030-01-10T15:00:00Z') });
  assert.deepEqual((await costsOf('s005')).map((c) => [c.jpy, c.f, c.t]), [[80, '2030-01-05', '2030-01-10'], [81, '2030-01-11', null]]);
});

console.log('\nNE に取り込む CSV');

await ta('[10] CSV が出ている列は変えない (作った・確かめた / 申告して確かめ待ち)。CSV に無い列・void・確かめ済みは変えられる', async () => {
  const mk = async (state) => {
    const extra = state === 'declared' ? ', checked_at, checked_run, declared_at, declared_by' : '';
    const vals = state === 'declared' ? ", now(), 'mc_20300101T000000000Z_abcdef', now(), 'x@test'" : '';
    return (await q(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by, state${extra})
      values ('products', 'name', 'syohin_name', 'v1', 'utf8', true, 1, repeat('a', 64), decode('00', 'hex'), 'mc_20300101T000000000Z_abcdef', 'x@test', '${state}'${vals}) returning export_id::text as id`))[0].id;
  };
  const row = (id, code, col) => pg.query(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell, cdb_version, evidence)
    values ($1, 'to_ne', $2, $3, $2, '{"value":"a"}', 'a', 1, '{}')`, [id, code, col]);
  const made = await mk('made');
  await row(made, 's003', 'name');
  const monthsBefore = (await skuRow('s003')).months;
  let e = await rejectsWith(save('s003', { name: '直したい', reorder_months: '3' }), 409, 'csv_issued');
  assert.match(e.message, new RegExp(`#${made}`));
  assert.equal((await skuRow('s003')).months, monthsBefore);
  await save('s003', { reorder_months: '3' });
  await pg.query("update ops.ne_csv_exports set state = 'void', void_at = now(), void_by = 'x@test', void_reason = 'by_user' where export_id = $1", [made]);
  await pg.query("update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = 'void' where export_id = $1", [made]);
  await save('s003', { name: '単品 3 改 2' });
  const declared = await mk('declared');
  await row(declared, 's003', 'tax_rate');
  e = await rejectsWith(save('s003', { tax_rate: '10' }), 409, 'csv_issued');
  assert.deepEqual(e.extra.exports.map((x) => [x.col, x.state]), [['tax_rate', 'declared']]);
  await pg.query("update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = 'confirmed' where export_id = $1", [declared]);
  await save('s003', { tax_rate: '10' });
  assert.equal((await skuRow('s003')).tax_rate, 0.1);
});

console.log('\nセット: 構成の依頼・NE の観測・上げる・食い違い');

await ta('[11] 構成を変える = 依頼だけ (core は変えない)。NE でやること・印が変わる・置き換え・今の構成に戻す = 取り下げ・並べ替えも依頼', async () => {
  const before = await compsOf('set001');
  const t0 = await tokenOf('set001');
  const r = await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 2 }] }, { reason: '中身を変える' });
  assert.equal(r.changed[0].field, 'components');
  assert.ok(r.ne_steps.some((s) => /s002 を外して/.test(s)), JSON.stringify(r.ne_steps));
  assert.ok(r.ne_steps.some((s) => /s003×2 を足す/.test(s) && /s001 の数量を 2→1/.test(s)), JSON.stringify(r.ne_steps));
  assert.deepEqual(await compsOf('set001'), before);
  const open = await q("select rows, base_rows, status, requested_by, reason from ops.sku_component_requests where status = 'open'");
  assert.equal(open.length, 1);
  assert.deepEqual(open[0].rows.map((x) => [x.code, x.qty, x.sort]), [['s001', 1, 1], ['s003', 2, 2]]);
  assert.deepEqual(open[0].base_rows.map((x) => [x.code, x.qty]), [['s001', 2], ['s002', 1]]);
  assert.deepEqual([open[0].requested_by, open[0].reason], ['naka@test', '中身を変える']);
  assert.notEqual(await tokenOf('set001'), t0);
  await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 3 }] });
  assert.deepEqual((await q('select status, close_reason from ops.sku_component_requests order by component_request_id')).map((x) => [x.status, x.close_reason]), [['cancelled', 'superseded'], ['open', null]]);
  const w = await save('set001', { components: [{ code: 's001', qty: 2 }, { code: 's002', qty: 1 }] });
  assert.ok(w.ne_steps.some((s) => /取り下げ/.test(s)));
  assert.deepEqual((await q('select status, close_reason from ops.sku_component_requests order by component_request_id')).map((x) => [x.status, x.close_reason]), [['cancelled', 'superseded'], ['cancelled', 'withdrawn']]);
  const re = await save('set001', { components: [{ code: 's002', qty: 1 }, { code: 's001', qty: 2 }] });
  assert.ok(re.ne_steps.some((s) => /並びを s002 → s001/.test(s)), JSON.stringify(re.ne_steps));
  await save('set001', { components: [{ code: 's001', qty: 2 }, { code: 's002', qty: 1 }] });
  await assert.rejects(() => pg.query(`update ops.sku_component_requests set rows = '[{"sku_id":1,"qty":1,"sort":1}]'::jsonb`), /書き換えない|閉じた依頼/);
  await assert.rejects(() => pg.query('delete from ops.sku_component_requests'), /消さない/);
});

await ta('[11] NE の観測を書く: 完全な回は残せないセット・構成品・重なり・数の違い・原本のハッシュが無いと拒む / 完全でない回は飛ばして数える / 時刻の範囲 / 再送 / 観測のロールだけ', async () => {
  const at = nextAt();
  const full = (sets, extra = {}) => ({ run_id: `ne_w_${++runSeq}`, observed_at: at, complete: true, requested: sets.length, fetched: sets.length, raw_hash: 'c'.repeat(64), source_generation: 'g', sets, ...extra });
  const w = (p) => asRole(E0, 'master_observer', () => W.recordNeSetObservations(db, p));
  const ok = [{ set_code: 'SET001', rows: [{ code: 's001', qty: 2, sort: 1 }] }];
  await assert.rejects(() => w(full([...ok, { set_code: 'nope', rows: [] }])), /残せないセット.*知らないセット/);
  await assert.rejects(() => w(full([...ok, { set_code: 's001', rows: [] }])), /知らないセット・セットでない s001/);
  await assert.rejects(() => w(full([...ok, { set_code: 'set001', rows: [] }])), /同じセットが 2 回/);
  await assert.rejects(() => w(full([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 'zzz', qty: 1, sort: 2 }] }])), /知らない構成品/);
  await assert.rejects(() => w(full([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 's002', qty: 1, sort: 1 }] }])), /並び \(sort\) が 1〜行の数/);
  // 数は厳密な整数 (#1563 R3 M3): 0.6・1.4・1.0・-1・大きすぎ・文字・並びの抜け = 形の誤り (完全な回は拒む)
  const row1 = (extra) => [{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1, ...extra }] }];
  for (const [extra, label] of [[{ qty: 0.6 }, 'qty 0.6'], [{ qty: 1.4 }, 'qty 1.4'], [{ qty: -1 }, 'qty -1'], [{ qty: 0 }, 'qty 0'], [{ qty: 100000 }, 'qty 大きすぎ'],
    [{ qty: 1e12 }, 'qty とても大きい'], [{ qty: '1' }, 'qty 文字'], [{ sort: 0.6 }, 'sort 0.6'], [{ sort: -1 }, 'sort -1'], [{ sort: 2 }, 'sort 1 から始まらない']]) {
    await assert.rejects(() => w(full(row1(extra))), /行の形が違う|並び \(sort\) が 1〜行の数/, label);
  }
  await assert.rejects(() => w(full([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 's002', qty: 1, sort: 3 }] }])), /並び \(sort\) が 1〜行の数/);   // 抜け
  for (const [extra, label] of [[{ requested: 0.6, fetched: 1 }, 'requested 0.6'], [{ requested: 1, fetched: 1.4 }, 'fetched 1.4'], [{ requested: -1 }, 'requested -1'], [{ requested: 1e12, fetched: 1e12 }, 'requested 大きすぎ']]) {
    await assert.rejects(() => w(full(ok, extra)), /requested・fetched は 0〜1,000,000 の整数|requested = fetched/, label);
  }
  // 書くときに、残すセットの SKU ごとの鍵を取る (昇格と同じ鍵 = 古い観測の昇格と並ぶ・#1563 R3 M4)。知らないセット (飛ばす) の鍵は取らない
  const want = (await q("select hashtextextended('core.sku:' || sku_id::text, 0)::text as k from core.skus where code in ('set001', 'set004') order by 1")).map((r) => r.k);
  await pg.query('begin');
  try {
    await pg.query('set role master_observer');
    await W.recordNeSetObservations(db, { run_id: `ne_w_${++runSeq}`, observed_at: at, complete: false, sets: [...ok, { set_code: 'SET004', rows: [{ code: 's001', qty: 1, sort: 1 }] }, { set_code: 'nope', rows: [] }] });
    const held = (await pg.query(`select ((l.classid::bigint << 32) | l.objid::bigint)::text as k from pg_locks l
      where l.locktype = 'advisory' and l.pid = pg_backend_pid() and l.objsubid = 1 and l.granted order by 1`)).rows.map((r) => r.k);
    assert.deepEqual(want.filter((k) => held.includes(k)), want, `セットの鍵 ${JSON.stringify(want)} を持っていない (${JSON.stringify(held)})`);
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  // 完全でない回: 形の誤りのセットは飛ばして数える (回ごと止めない)・requested も同じ形
  const inc2 = await w({ run_id: `ne_w_${++runSeq}`, observed_at: at, complete: false, sets: [...ok, { set_code: 'set004', rows: [{ code: 's001', qty: 0.6, sort: 1 }] }] });
  assert.deepEqual([inc2.sets, inc2.skipped], [1, 1]);
  await assert.rejects(() => w({ run_id: `ne_w_${++runSeq}`, observed_at: at, complete: false, requested: 2.5, sets: ok }), /requested・fetched は 0〜1,000,000 の整数/);
  await assert.rejects(() => w(full([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 'S001', qty: 1, sort: 2 }] }])), /構成品・並びが重なる/);
  await assert.rejects(() => w(full(ok, { requested: 2 })), /requested = fetched/);
  await assert.rejects(() => w(full(ok, { raw_hash: null })), /raw_hash/);
  await assert.rejects(() => w(full(ok, { observed_at: new Date(Date.now() + 10 * 60000).toISOString() })), /未来/);
  await assert.rejects(() => w(full(ok, { observed_at: new Date(Date.now() - 40 * 3600000).toISOString() })), /古すぎる/);
  await assert.rejects(() => w(full(ok, { run_id: 'bad run' })), /run_id/);
  const p = full(ok);
  assert.deepEqual(await w(p), { state: 'written', run_id: p.run_id, sets: 1, skipped: 0 });
  assert.equal((await w(p)).state, 'unchanged');
  await assert.rejects(() => w({ ...p, source_generation: 'g2' }), /run_conflict/);
  const run = (await q('select complete, requested_count, fetched_count, saved_count, skipped_count, raw_hash, source_generation from ops.ne_set_observation_runs where run_id = $1', [p.run_id]))[0];
  assert.deepEqual(run, { complete: true, requested_count: 1, fetched_count: 1, saved_count: 1, skipped_count: 0, raw_hash: 'c'.repeat(64), source_generation: 'g' });
  // 完全でない回: 残せないものは飛ばして数える (上げる根拠には使わない)
  const inc = await w({ run_id: `ne_w_${++runSeq}`, observed_at: at, complete: false, sets: [...ok, { set_code: 'nope', rows: [] }, { set_code: 's001', rows: [] }, { set_code: 'set004', rows: [{ code: 'zzz', qty: 1, sort: 1 }] }] });
  assert.deepEqual([inc.sets, inc.skipped], [2, 2]);
  // 画面のロール・持ち主でも観測の表へ直接は書けない (持ち主は試験の作り方として書ける = 画面のロールだけ確かめる)
  assert.equal(await pgCode(asRole(E0, 'master_edit', () => pg.query("select ops.record_ne_set_observations('{}'::jsonb)"))), '42501');
  await assert.rejects(() => pg.query('delete from ops.ne_set_observations'), /append-only/);
  await assert.rejects(() => pg.query("update ops.ne_set_observation_runs set complete = false"), /append-only/);
});


await ta('[11] 上げない: 偽の番号・完全でない回・依頼より前の観測・並び / 行の数 / 数量が違う (食い違いを残す)・保存が開いていない', async () => {
  await save('set001', { components: [{ code: 's003', qty: 2 }, { code: 's001', qty: 1 }] }, { reason: '入れ替え' });
  const reqAt = Date.parse((await q("select created_at::text as t from ops.sku_component_requests where status = 'open'"))[0].t);
  const before = await compsOf('set001');
  const good = [{ code: 's003', qty: 2, sort: 1 }, { code: 'S001', qty: 1, sort: 2 }];
  for (const fake of ['999999', 'abc', null, { set_code: 'set001', complete: true, rows: good }]) assert.equal((await promote(fake)).reason, 'no_observation', JSON.stringify(fake));
  assert.equal((await promote((await observe([{ set_code: 'set001', rows: good }], { complete: false })).set001)).reason, 'incomplete_observation');
  assert.equal((await promote((await observe([{ set_code: 'set001', rows: good }], { at: new Date(reqAt - 3600000).toISOString() })).set001)).reason, 'stale_observation');
  assert.equal((await promote((await observe([{ set_code: 'set001', rows: good }])).set001, { ownership: MASTER_OWNERSHIP })).reason, 'before_cutover');
  const tries = [
    [[good[1], good[0]].map((r, i) => ({ ...r, sort: i + 1 })), 'mismatch'],   // 並びが違う
    [[...good, { code: 's002', qty: 1, sort: 3 }], 'mismatch'],                  // 行が多い
    [[good[0]], 'mismatch'],                                                      // 行が足りない
    [[{ ...good[0], qty: 3 }, good[1]], 'mismatch'],                              // 数量が違う
  ];
  for (const [rows, why] of tries) {
    const r = await promote((await observe([{ set_code: 'set001', rows }])).set001);
    assert.deepEqual([r.promoted, r.reason], [false, why], JSON.stringify(rows));
  }
  assert.deepEqual(await compsOf('set001'), before);
  // 食い違いは開いているのが 1 つ (中身が変われば前のを閉じて新しく)
  const br = await q("select kind, status, close_reason from ops.sku_component_breaches where set_sku_id = (select sku_id from core.skus where code = 'set001') order by breach_id");
  assert.deepEqual(br.filter((b) => b.status === 'open').map((b) => b.kind), ['mismatch']);
  assert.equal(br.filter((b) => b.close_reason === 'superseded').length, tries.length - 1);
  // 同じ中身の食い違いをもう一度 = 増やさない
  const n = br.length;
  await promote((await observe([{ set_code: 'set001', rows: tries.at(-1)[0] }])).set001);
  assert.equal(Number((await q('select count(*)::int as n from ops.sku_component_breaches'))[0].n), n);
  // 依頼から 7 日より後の観測でも違う = stale (mismatch は閉じる)。依頼を 9 日前に作ったことにする (持ち主のロールで同じ中身の依頼に置き換える)。
  //   🚨 未来の観測 (8 日後) で作らない: 同じセットにもっと新しい観測がある = 後の観測を上げない (#1563 仮レビュー M2) の試験を壊す
  await pg.query(`update ops.sku_component_requests set status = 'cancelled', closed_at = now(), closed_by = 'test', close_reason = 'superseded'
    where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set001')`);
  await pg.query(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id, created_at)
    select company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, gen_random_uuid(), created_at - interval '9 days' from ops.sku_component_requests
     where component_request_id = (select max(component_request_id) from ops.sku_component_requests where set_sku_id = (select sku_id from core.skus where code = 'set001'))`);
  const st = await promote((await observe([{ set_code: 'set001', rows: [good[0]] }])).set001);
  assert.equal(st.reason, 'stale');
  assert.deepEqual((await q("select kind from ops.sku_component_breaches where status = 'open'")).map((b) => b.kind), ['stale']);
  // 画面にも出る
  const page = await R.readSkuPage(db, 'set001', { now: NOW, ownership: ALL_COMPANY, open: true });
  assert.deepEqual(page.breaches.map((b) => b.kind), ['stale']);
  await assert.rejects(() => pg.query("update ops.sku_component_breaches set kind = 'mismatch' where status = 'open'"), /書き換えない/);
  await assert.rejects(() => pg.query("update ops.sku_component_breaches set closed_by = 'x' where status = 'closed'"), /閉じた食い違いは変えない/);
  await assert.rejects(() => pg.query('delete from ops.sku_component_breaches'), /消さない/);
});

await ta('[11] 上げる: 観測が完全・依頼より後・構成品 / 数量 / 並び / 行の数まで同じ。同じ取引で構成・導く値・依頼 (applied)・食い違い (resolved)', async () => {
  const good = [{ code: 's003', qty: 2, sort: 1 }, { code: 'S001', qty: 1, sort: 2 }];
  const obsId = (await observe([{ set_code: 'set001', rows: good }], { at: nextAt() })).set001;
  const r = await promote(obsId);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.deepEqual(await compsOf('set001'), [['s003', 2, 1, 'ne'], ['s001', 1, 2, 'ne']]);
  const req = (await q("select status, close_reason, closed_by, applied_observation_id::text as oid from ops.sku_component_requests where set_sku_id = (select sku_id from core.skus where code = 'set001') order by component_request_id desc limit 1"))[0];
  assert.deepEqual(req, { status: 'applied', close_reason: 'matched', closed_by: 'system', oid: obsId });
  assert.deepEqual((await q("select distinct status, close_reason from ops.sku_component_breaches where set_sku_id = (select sku_id from core.skus where code = 'set001') and close_reason <> 'superseded'")),
    [{ status: 'closed', close_reason: 'resolved' }]);
  // 導く値も同じ取引で (s003 50×2 + s001 120 = 220・税率 s003 10% (直した)・s001 10%)
  assert.ok(r.derived.some((d) => d.col === 'cost' && d.to === 220), JSON.stringify(r.derived));
  assert.equal((await costsOf('set001')).at(-1).jpy, 220);
  const ev = await q("select distinct actor_type, source_system, run_id from events.master_change_events where source_system = 'ne_observation'");
  assert.deepEqual(ev.map((x) => [x.actor_type, x.source_system]), [['system', 'ne_observation']]);
  assert.equal((await promote(obsId)).reason, 'no_open_request');
});

await ta('[11] 依頼が無いのに NE の構成が今の構成と違う = unrequested_diff を残す → NE が今の構成に戻った観測で resolved', async () => {
  let r = await promote((await observe([{ set_code: 'set005', rows: [{ code: 's001', qty: 1, sort: 1 }] }])).set005);
  assert.deepEqual([r.promoted, r.reason], [false, 'unrequested_diff']);
  assert.deepEqual((await q("select kind, component_request_id from ops.sku_component_breaches where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set005')")),
    [{ kind: 'unrequested_diff', component_request_id: null }]);
  r = await promote((await observe([{ set_code: 'set005', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 's005', qty: 1, sort: 2 }] }])).set005);
  assert.deepEqual([r.reason, r.closed_breaches], ['no_open_request', 1]);
  assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_breaches where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set005')"))[0].n), 0);
});

await ta('[11] 依頼の後に「依頼にだけある構成品」の原価が 0 になった = NE の構成が依頼どおりでも上げない (core は変えない・underivable を残す) → 直すと上げる', async () => {
  await save('set001', { components: [{ code: 's003', qty: 2 }, { code: 's001', qty: 1 }, { code: 's002', qty: 1 }] }, { reason: 's002 を足す' });
  const s2 = await save('s002', { cost: { jpy: '0', reason: '仕入先が無償に' } });   // s002 はまだどのセットにも入っていない = セットは計算し直さない
  assert.deepEqual(s2.derived, []);
  const before = { comps: await compsOf('set001'), cost: (await costsOf('set001')).at(-1) };
  const rows = [{ code: 's003', qty: 2, sort: 1 }, { code: 's001', qty: 1, sort: 2 }, { code: 's002', qty: 1, sort: 3 }];
  let r = await promote((await observe([{ set_code: 'set001', rows }], { at: nextAt() })).set001);
  assert.deepEqual([r.promoted, r.reason], [false, 'underivable']);
  assert.match(r.blockers.join(' '), /s002 の原価/);
  assert.deepEqual({ comps: await compsOf('set001'), cost: (await costsOf('set001')).at(-1) }, before);
  assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_requests where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set001')"))[0].n), 1);
  const br = await q("select kind, details from ops.sku_component_breaches where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'set001')");
  assert.deepEqual(br.map((b) => b.kind), ['underivable']);
  assert.ok(br[0].details.blockers.length > 0);
  await save('s002', { cost: { jpy: '200', reason: '戻した' } });
  r = await promote((await observe([{ set_code: 'set001', rows }], { at: nextAt() })).set001);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.deepEqual(await compsOf('set001'), [['s003', 2, 1, 'ne'], ['s001', 1, 2, 'ne'], ['s002', 1, 3, 'ne']]);
  assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_breaches where status = 'open'"))[0].n), 0);
});

await ta('[12] 導く値が決まらないセットは保存しない: 分類 → 上書きで・原価 → 例外原価で通る。税率が決まらない (上書きなし) は通らない', async () => {
  await pg.query("update core.products set sales_class = null where product_id = (select product_id from core.skus where code = 's004')");   // 構成品の分類が未入力
  let e = await rejectsWith(save('set004', { name: 'セット 4 改' }), 400, 'set_underivable');
  assert.equal(e.extra.blockers.length, 2);
  assert.match(e.extra.blockers.join(' '), /売上分類/); assert.match(e.extra.blockers.join(' '), /s004 の原価/);
  e = await rejectsWith(save('set004', { name: 'セット 4 改', set_sales_class_override: '3' }), 400, 'set_underivable');
  assert.equal(e.extra.blockers.length, 1);
  const r = await save('set004', { name: 'セット 4 改', set_sales_class_override: '3', exception_cost: { jpy: '999', reason: '仕入先の見積' } });
  assert.deepEqual(r.changed.map((c) => c.field).sort(), ['exception_cost', 'name', 'set_sales_class_override']);
  assert.deepEqual([(await skuRow('set004')).name, (await skuRow('set004')).override], ['セット 4 改', 3]);
  assert.deepEqual((await costsOf('set004')).map((c) => [c.jpy, c.src, c.st, c.f, c.t]), [[999, 'manual', 'OVERRIDDEN', TODAY, null]]);
  e = await rejectsWith(save('set005', { name: 'セット 5 改' }), 400, 'set_underivable');
  assert.deepEqual(e.extra.blockers.length, 1);
  assert.match(e.extra.blockers[0], /s005 の税率が未入力/);
  await pg.query("update core.products set sales_class = 1 where product_id = (select product_id from core.skus where code = 's004')");
  e = await rejectsWith(save('set004', { set_sales_class_override: '2' }), 400);
  assert.match(e.message, /導けるので、上書きはできません/);
});

await ta('[12] 今の構成は導けない + 依頼の構成は導ける: 上書き・例外原価を外すのは拒む (今の構成の値を壊さない)。依頼の構成が導けない依頼も拒む', async () => {
  let r = await save('set006', { set_sales_class_override: '3', exception_cost: { jpy: '500', reason: '見積' } });
  assert.deepEqual(r.changed.map((c) => c.field).sort(), ['exception_cost', 'set_sales_class_override']);
  r = await save('set006', { components: [{ code: 's001', qty: 1 }, { code: 's002', qty: 1 }] });   // 依頼の構成は上書きなしでも導ける
  assert.equal(r.changed[0].field, 'components');
  const before = { costs: await costsOf('set006'), row: await skuRow('set006') };
  let e = await rejectsWith(save('set006', { exception_cost: { clear: true, reason: 'やめる' } }), 400, 'set_underivable');
  assert.match(e.extra.blockers.join(' '), /今の構成: .*s006 の原価/);
  assert.ok(!e.extra.blockers.some((b) => /依頼の構成/.test(b)), JSON.stringify(e.extra.blockers));
  e = await rejectsWith(save('set006', { set_sales_class_override: '' }), 400, 'set_underivable');
  assert.match(e.extra.blockers.join(' '), /今の構成: .*売上分類/);
  assert.deepEqual({ costs: await costsOf('set006'), row: await skuRow('set006') }, before);
  // 逆: 今の構成は導ける・依頼の構成が導けない (s006 = 原価・分類なし) = 依頼を受けない
  e = await rejectsWith(save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's006', qty: 1 }] }), 400, 'set_underivable');
  assert.match(e.extra.blockers.join(' '), /依頼の構成: /);
});

await ta('[12] 例外原価をやめる = 今日の例外の行を消して、今日から構成品の合計 (合計できなければ保存しない)', async () => {
  const e = await rejectsWith(save('set004', { exception_cost: { clear: true, reason: 'やめる' } }), 400, 'set_underivable');
  assert.match(e.message, /原価/);
  await save('s004', { cost: { jpy: '70', reason: '入れた' } });
  const r = await save('set004', { exception_cost: { clear: true, reason: 'やめる' } });
  assert.deepEqual(r.derived.map((d) => [d.col, d.to]), [['cost', 190]]);   // s001 120 + s004 70
  assert.deepEqual((await costsOf('set004')).map((c) => [c.jpy, c.src, c.f, c.t]), [[190, 'set_calc', TODAY, null]]);
});

console.log('\n上げる処理と単品の保存の順番');

await ta('[13] 単品の保存 → 上げる: 上げたセットは新しい単品の値で計算する / 上げる → 単品の保存: 上げる前の画面は 409・新しい画面の保存は増えたセットも計算し直す', async () => {
  // set004 に s003 を足す依頼。s003 の原価を 50 → 55 に直してから上げる
  await save('set004', { components: [{ code: 's001', qty: 1 }, { code: 's004', qty: 1 }, { code: 's003', qty: 1 }] });
  await save('s003', { cost: { jpy: '55', reason: '上げる前に直した' } });
  const before = await tokenOf('s003');
  const r = await promote((await observe([{ set_code: 'set004', rows: [{ code: 's001', qty: 1, sort: 1 }, { code: 's004', qty: 1, sort: 2 }, { code: 's003', qty: 1, sort: 3 }] }], { at: nextAt() })).set004);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.equal((await costsOf('set004')).at(-1).jpy, 120 + 70 + 55);   // s001 + s004 + 直した s003
  // 上げた後: s003 を含むセットが増えた = 上げる前に開いた画面の保存は 409
  await rejectsWith(save('s003', { cost: { jpy: '56', reason: '古い画面' } }, { token: before }), 409, 'version_conflict');
  const s2 = await save('s003', { cost: { jpy: '56', reason: '新しい画面' } });
  assert.ok(s2.derived.some((d) => d.code === 'set004' && d.col === 'cost' && d.to === 120 + 70 + 56), JSON.stringify(s2.derived));
});

console.log('\n古い観測・前からの原価の重なり・構成品の種類 (#1563 仮レビュー M1 / M2 / Low 6。別の DB)');

const E3 = await setupDb();
await openCutover(E3, ALL_COMPANY);
const q3 = async (sql, params) => (await E3.db.query(sql, params)).rows;
const costs3 = async (code) => (await q3(`select c.cost_jpy::int as jpy, c.valid_from::text as f, c.valid_to::text as t from core.sku_costs c join core.skus s on s.sku_id = c.sku_id
  where s.code = $1 order by c.valid_from, c.valid_to nulls last, c.sku_cost_id`, [code])).map((c) => [c.jpy, c.f, c.t]);
const comps3 = async (code) => (await q3(`select k.code, c.qty from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
  where c.parent_sku_id = (select sku_id from core.skus where code = $1) order by c.sort_order`, [code])).map((r) => [r.code, r.qty]);
const promote3 = (id, opts = {}) => W.promoteComponentRequest(E3.db, id, { ownership: ALL_COMPANY, now: NOW, ...opts });
const openBreaches3 = async () => (await q3("select kind from ops.sku_component_breaches where status = 'open' order by breach_id")).map((b) => b.kind);
/** 開いている依頼を、同じ中身で ago だけ前に作ったことにする (持ち主のロール) */
const backdateRequest3 = async (code, ago) => {
  await E3.pg.query(`update ops.sku_component_requests set status = 'cancelled', closed_at = now(), closed_by = 'test', close_reason = 'superseded'
    where status = 'open' and set_sku_id = (select sku_id from core.skus where code = $1)`, [code]);
  await E3.pg.query(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id, created_at)
    select company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, gen_random_uuid(), created_at - $2::interval from ops.sku_component_requests
     where component_request_id = (select max(component_request_id) from ops.sku_component_requests where set_sku_id = (select sku_id from core.skus where code = $1))`, [code, ago]);
};

await ta('[11b] 受けてから 36 時間より前の観測は使わない (依頼より後・ほかに新しい観測なし・依頼どおりでも上げない = observation_too_old)', async () => {
  const want = [{ code: 's001', qty: 1 }, { code: 's002', qty: 1 }];
  await save('set001', { components: want }, { E: E3 });
  await backdateRequest3('set001', '3 days');
  const old = await observeRaw('set001', want.map((w, i) => ({ ...w, sort: i + 1 })), new Date(Date.now() - 40 * 3600000).toISOString(), { E: E3, recordedAgo: '40 hours' });
  const r = await promote3(old);
  assert.deepEqual([r.promoted, r.reason, r.max_age_hours], [false, 'observation_too_old', W.OBSERVATION_MAX_AGE_HOURS]);
  assert.deepEqual(await comps3('set001'), [['s001', 2], ['s002', 1]]);
  assert.deepEqual(await openBreaches3(), []);
  // 依頼を取り下げる (今の構成に戻す)
  await save('set001', { components: [{ code: 's001', qty: 2 }, { code: 's002', qty: 1 }] }, { E: E3 });
});

await ta('[9b] 前からの原価の重なり (夜間ロードが同じ日に 2 回付け替えた [d, d] と [d, 続く]): 次の日の昇格・保存は通る (今の行を昨日で閉じる = 縮めるだけ) / 同じ日は 409 cost_overlap (500 にしない・昇格は投げない)', async () => {
  const plan2 = makePlan();
  for (const s of plan2.skus) if (['s001', 's002', 'set001'].includes(s.code)) s.cost = { ...s.cost, jpy: s.cost.jpy + 10 };
  const lr = await runInitialLoad(E3.db, plan2, { log: quiet, runId: 'load_twice', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
  assert.equal(lr.ok, true, lr.error);
  assert.deepEqual(await costs3('s001'), [[100, '2030-01-05', '2030-01-05'], [110, '2030-01-05', null]]);   // 前からの重なり (1 日)
  assert.deepEqual(await costs3('set001'), [[400, '2030-01-05', '2030-01-05'], [410, '2030-01-05', null]]);
  const want = [{ code: 's001', qty: 1 }, { code: 's002', qty: 1 }];
  await save('set001', { components: want }, { E: E3 });
  const obsId = (await observe([{ set_code: 'set001', rows: want.map((w, i) => ({ ...w, sort: i + 1 })) }], { E: E3 })).set001;
  // 同じ日 (夜間ロードの日): 今日始まった行を入れ直すと前からの行に当たる = 上げない (理由を返す・投げない = 夜間の段を止めない)
  const same = await promote3(obsId, { now: LOAD_NOW });
  assert.deepEqual([same.promoted, same.reason], [false, 'cost_overlap'], JSON.stringify(same));
  assert.deepEqual(await comps3('set001'), [['s001', 2], ['s002', 1]]);
  // 次の日: 今の行を昨日で閉じる (縮めるだけ = 重なりの守りは見ない) → 今日から
  const next = await promote3(obsId);
  assert.equal(next.promoted, true, JSON.stringify(next));
  assert.deepEqual(await comps3('set001'), [['s001', 1], ['s002', 1]]);
  assert.deepEqual(await costs3('set001'), [[400, '2030-01-05', '2030-01-05'], [410, '2030-01-05', '2030-01-09'], [110 + 210, TODAY, null]]);
  // 単品の保存 (画面のロール): 次の日は通る・含むセットも計算し直す
  const s = await save('s002', { cost: { jpy: '220', reason: '重なりの後' } }, { E: E3 });
  assert.ok(s.derived.some((d) => d.code === 'set001' && d.col === 'cost' && d.to === 110 + 220), JSON.stringify(s.derived));
  assert.deepEqual(await costs3('s002'), [[200, '2030-01-05', '2030-01-05'], [210, '2030-01-05', '2030-01-09'], [220, TODAY, null]]);
  // 同じ日 (夜間ロードの日) の保存 = 409 cost_overlap・何も書かない・記録も 409
  const before = await costs3('s001');
  const id = uuid();
  const e = await rejectsWith(save('s001', { cost: { jpy: '125', reason: '同じ日' } }, { E: E3, now: LOAD_NOW, requestId: id }), 409, 'cost_overlap');
  assert.match(e.message, /重なって/);
  assert.deepEqual(await costs3('s001'), before);
  const rec = (await q3('select status, error from ops.master_edit_requests where request_id = $1', [id]))[0];
  assert.deepEqual([rec.status, rec.error.status, rec.error.reason], ['failed', 409, 'cost_overlap']);
});

await ta('[11c] 古い観測で上げない: A (依頼どおり) → B (NE がまた変わった) の後で A を上げない (superseded_observation)・B は食い違い → また A = 上げる', async () => {
  const A = [{ code: 's001', qty: 2, sort: 1 }, { code: 's002', qty: 1, sort: 2 }];
  const B = [{ code: 's001', qty: 3, sort: 1 }, { code: 's002', qty: 1, sort: 2 }];
  await save('set001', { components: A.map(({ code, qty }) => ({ code, qty })) }, { E: E3 });
  const a1 = (await observe([{ set_code: 'set001', rows: A }], { E: E3 })).set001;
  const b = (await observe([{ set_code: 'set001', rows: B }], { E: E3 })).set001;
  let r = await promote3(a1);
  assert.deepEqual([r.promoted, r.reason, r.newer_observation_id], [false, 'superseded_observation', b], JSON.stringify(r));
  assert.deepEqual(await comps3('set001'), [['s001', 1], ['s002', 1]]);
  assert.deepEqual(await openBreaches3(), []);
  r = await promote3(b);
  assert.deepEqual([r.promoted, r.reason], [false, 'mismatch']);
  const a2 = (await observe([{ set_code: 'set001', rows: A }], { E: E3 })).set001;
  assert.equal((await promote3(b)).reason, 'superseded_observation');   // B も A2 より古い = 食い違いを作り直さない
  assert.equal((await promote3(a1)).reason, 'superseded_observation');
  r = await promote3(a2);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.deepEqual(await comps3('set001'), [['s001', 2], ['s002', 1]]);
  assert.deepEqual(await openBreaches3(), []);
});

await ta('[11d] 上げる直前に構成品がまだ単品か確かめる: 夜間ロードが構成品をセットにした = 上げない (core は変えない・underivable)。単品に戻れば上げる', async () => {
  const D = [{ code: 's001', qty: 2, sort: 1 }, { code: 's002', qty: 1, sort: 2 }, { code: 's003', qty: 1, sort: 3 }];
  await save('set001', { components: D.map(({ code, qty }) => ({ code, qty })) }, { E: E3 });
  const obsId = (await observe([{ set_code: 'set001', rows: D }], { E: E3 })).set001;
  await E3.pg.query("update core.skus set sku_kind = 'set' where code = 's003'");   // 夜間ロードが NE の種類替えを写した
  let r = await promote3(obsId);
  assert.deepEqual([r.promoted, r.reason], [false, 'underivable'], JSON.stringify(r));
  assert.match(r.blockers.join(' '), /s003 はセットになった/);
  assert.deepEqual(await comps3('set001'), [['s001', 2], ['s002', 1]]);
  assert.deepEqual(await openBreaches3(), ['underivable']);
  await E3.pg.query("update core.skus set sku_kind = 'single' where code = 's003'");
  r = await promote3(obsId);
  assert.equal(r.promoted, true, JSON.stringify(r));
  assert.deepEqual(await comps3('set001'), [['s001', 2], ['s002', 1], ['s003', 1]]);
  assert.deepEqual(await openBreaches3(), []);
  await E3.pg.close();
});

console.log('\nDB のロール');

await ta('[14] master_edit = 画面の読み書きだけ (記録の偽造・過去の原価・版・キーの列も不可) / master_ops = 段階を進める関数 / master_observer = 観測 / master_gate = 門の記録', async () => {
  const as = async (role, sql, params = []) => {
    await pg.query(`set role ${role}`);
    try { return await pgCode(pg.query(sql, params)); } finally { await pg.query('set role deploy'); }
  };
  const deny = [
    [`select ops.set_master_cutover_phase('frozen', 'x', '{}'::jsonb)`, '42501'],
    [`select ops.record_ne_set_observations('{}'::jsonb)`, '42501'],
    ['insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, 1, 2, 1, \'manual\')', '42501'],
    ['delete from core.sku_components where false', '42501'],
    ["update ops.master_cutover_state set note = 'x'", '42501'],
    ["insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count) select 'render', 'x', 'x', manifest_hash, repeat('a', 64), 'new_open', 0 from ops.master_legacy_manifests limit 1", '42501'],
    ["select ops.record_legacy_gate_ack('render', 'x', 'b', '{}'::jsonb, repeat('a', 64), 'new_open', 0, null)", '42501'],
    ["insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_key, new_value, actor_type, source_system) values (1, gen_random_uuid(), 'INSERT', 'sku', '{}', '{}', 'human', 'fake')", '42501'],
    ['update core.skus set version = version where false', '42501'],
    ["insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code) values (1, 1, 1, 'x')", '42501'],
    ["update core.suppliers set active = false where false", '42501'],
    ['update core.suppliers set version = version where false', '42501'],
    ['select 1 from core.suppliers for share', '42501'],
    ["insert into ops.ne_set_observation_runs (run_id, observed_at, complete, saved_count, skipped_count, content_hash) values ('x', now(), false, 0, 0, repeat('a', 32))", '42501'],
    ['update core.skus set code = code where false', '42501'],
    ['delete from core.skus where false', '42501'],
    ['select 1 from core.orders limit 1', '42501'],
    ['update ops.sku_component_requests set rows = rows where false', '42501'],
  ];
  for (const [sql, want] of deny) assert.equal(await as('master_edit', sql), want, sql);
  for (const sql of ['select 1 from core.skus limit 1', 'select 1 from ops.master_cutover_state', "update core.skus set name = name where false", 'select 1 from ops.sku_component_breaches limit 1']) assert.equal(await as('master_edit', sql), 'ok', sql);
  // 原価: 今日より前に始まった行は消せない・閉じた行は変えられない (画面のロールでも)
  await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) select 1, sku_id, 10, 'ne', 'COMPLETE', '2026-01-01', '2026-01-31' from core.skus where code = 's006'");
  assert.equal(await as('master_edit', "delete from core.sku_costs where valid_from = '2026-01-01'"), '42501');
  assert.equal(await as('master_edit', "update core.sku_costs set valid_to = '2026-02-28' where valid_from = '2026-01-01'"), '42501');
  assert.equal(await as('master_edit', "insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 11, 'manual', 'COMPLETE', '2026-01-10' from core.skus where code = 's006'"), '42501');   // 始めていない = 書けない (R3 M2)
  const args6 = await beginArgs(E0, 's006', { reason: '重なりの試験' });
  await asEditorTx(E0, async () => {
    await pg.query(BEGIN_SQL, args6);
    assert.equal(await pgCode(pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 11, 'manual', 'COMPLETE', '2026-01-10' from core.skus where code = 's006'")), '23P01');   // 始めた後でも、画面のロールは source を偽っても重なりを拒む
  });
  await pg.query("delete from core.sku_costs where valid_from = '2026-01-01'");
  // master_observer / master_gate: それぞれの関数だけ
  assert.equal(await as('master_observer', 'select 1 from core.skus limit 1'), '42501');
  assert.equal(await as('master_observer', `select ops.set_master_cutover_phase('frozen', 'x', '{}'::jsonb)`), '42501');
  assert.equal(await as('master_gate_render', `select ops.record_ne_set_observations('{}'::jsonb)`), '42501');
  assert.equal(await as('master_gate_render', 'select phase from ops.master_cutover_state'), 'ok');   // まとめのロール master_gate の権限を INHERIT で使う
  assert.equal(await as('master_gate_minipc', 'select 1 from core.skus limit 1'), '42501');
  // master_ops: 関数は動く (段階はもう new_open = one_way で止まる = 権限では拒まれない)・表は直接書けない・商品は読めない
  assert.equal(await as('master_ops', `select ops.set_master_cutover_phase('frozen', 'x', '{}'::jsonb)`), 'P0001');
  assert.equal(await as('master_ops', "update ops.master_cutover_state set note = 'x'"), '42501');
  assert.equal(await as('master_ops', 'select 1 from core.skus limit 1'), '42501');
  assert.equal(await as('master_ops', 'select phase from ops.master_cutover_state'), 'ok');
  const roles = await q("select rolname, rolsuper, rolcreaterole, rolinherit, rolcanlogin from pg_roles where rolname like 'master\\_%' order by 1");
  assert.deepEqual(roles.map((r) => [r.rolname, r.rolsuper, r.rolcreaterole, r.rolinherit, r.rolcanlogin]), [
    ['master_edit', false, false, false, true], ['master_gate', false, false, false, false],
    ['master_gate_minipc', false, false, true, true], ['master_gate_render', false, false, true, true],
    ['master_observer', false, false, false, true], ['master_ops', false, false, false, true]]);
  // ロールの設定 (画面の接続と同じ lock_timeout・idle_in_transaction_session_timeout。#1563 仮レビュー Low 4)
  const conf = (await q("select coalesce(s.setconfig, '{}') as c from pg_db_role_setting s join pg_roles r on r.oid = s.setrole where r.rolname = 'master_edit' and s.setdatabase = 0"))[0].c;
  assert.deepEqual([...conf].sort(), ['idle_in_transaction_session_timeout=60s', 'lock_timeout=10s', 'statement_timeout=20s']);
  // 流し直し (#1563 仮レビュー Low 2): もうあるロールのパスワードは変えない (門のログインは Render と miniPC の両方が使う)。変えるのは --rotate-password のロールだけ
  const pwOf = async () => {   // pg_authid は superuser だけ読める = 試験の接続のログインに戻って読む
    await pg.query('reset role');
    try { return Object.fromEntries((await pg.query("select rolname, rolpassword from pg_authid where rolname like 'master\\_%' order by 1")).rows.map((r) => [r.rolname, r.rolpassword])); }
    finally { await pg.query('set role deploy'); }
  };
  const before = await pwOf();
  const again = await createMasterEditRoles(pg, {});
  assert.deepEqual(Object.keys(again.pw), []);
  assert.deepEqual(await pwOf(), before);
  const rot = await createMasterEditRoles(pg, { rotate: ['master_ops'] });
  assert.deepEqual(Object.keys(rot.pw), ['master_ops']);
  const after = await pwOf();
  assert.notEqual(after.master_ops, before.master_ops);
  assert.deepEqual({ ...after, master_ops: null }, { ...before, master_ops: null });
  assert.equal(after.master_gate, null);   // まとめのロールはパスワードなし
  await assert.rejects(() => createMasterEditRoles(pg, { rotate: ['master_gate'] }), /知らないロール/);
  const { parseRotateArgs } = await import('./company-db/create-master-edit-roles.mjs');
  assert.deepEqual(parseRotateArgs(['--dry-run', '--rotate-password', 'master_gate_render', '--rotate-password', 'master_ops']), ['master_gate_render', 'master_ops']);
  assert.throws(() => parseRotateArgs(['--rotate-password']), /ロールの名前/);
  const dry = await createMasterEditRoles(pg, { dryRun: true });
  assert.ok(!dry.stmts.some((x) => /password '/.test(x)), 'もうあるロールにはパスワードの文を出さない');
  // 変更の記録は画面のロールで保存しても db_user = master_edit (security definer の関数でも呼び手を残す)
  assert.equal(Number((await q("select count(*)::int as n from events.master_change_events where source_system = 'portal_master_edit' and request_id in (select request_id::text from ops.master_edit_requests) and db_user <> 'master_edit'"))[0].n), 0);
  assert.ok(Number((await q("select count(*)::int as n from events.master_change_events where db_user = 'master_edit'"))[0].n) > 10);
});

await ta('[14b] 画面のロールは DB でも守る (#1563 R3 M2・R4 M2): 始める前の書き込み・偽の core.actor_* = 42501 / 約束の相手でない行・操作・古い版・違う保存の記録 = 拒む / 保存の流れの記録の誰が = 約束の人・持ち主が load の列は 42501', async () => {
  // 持ち主表のハッシュは JS と DB で同じ (段階の記録と比べる)
  for (const own of [MASTER_OWNERSHIP, ALL_COMPANY, withOwn({ 'skus.name': 'company' })]) {
    assert.equal((await q('select ops.ownership_hash($1::jsonb) as h', [JSON.stringify(own)]))[0].h, C.ownershipHash(own));
  }
  const forge = "select set_config('core.actor_type', 'human', true), set_config('core.actor_id', 'forged@evil', true), set_config('core.source_system', 'portal_master_edit', true), set_config('core.request_id', 'forged', true)";
  const before = await nEvents();
  await pg.query('set role master_edit');
  await pg.query('begin');
  try {
    await pg.query(forge);
    assert.equal(await pgCode(pg.query("update core.skus set name = '偽の直し' where code = 's004'")), '42501');
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  for (const sql of ["update core.products set name = '偽' where product_id = (select product_id from core.skus where code = 's004')",
    "update core.supplier_skus set is_primary = is_primary where true", "delete from core.sku_costs where valid_from >= '2030-01-10'",
    "update ops.sku_component_breaches set status = status where true",
    "insert into ops.master_edit_requests (request_id, company_id, operation, target_code, actor_id, payload_hash, status, result, started_at) values (gen_random_uuid(), 1, 'sku_edit', 's004', 'forged@evil', repeat('a', 64), 'done', '{}', now())",
    "insert into ops.master_write_sessions (txid, request_id, actor_id, source_system, db_user, phase, owner_hash, ownership) values (txid_current(), gen_random_uuid(), 'x', 'portal_master_edit', 'x', 'new_open', repeat('a', 64), '{}')"]) {
    // 行がある = guard が「始めていない」で拒む (変更の記録・列の持ち主の検査より先)・sessions は権限が無い
    const err = await asRole(E0, 'master_edit', () => pg.query(sql).then(() => null, (e) => e));
    assert.equal(err?.code, '42501', sql);
    if (!/master_write_sessions/.test(sql)) assert.match(err.message, /master_write_session_required/, sql);
  }
  assert.equal(await nEvents(), before);
  // 約束は 直す SKU・操作・版・保存の中身 に結びつく (#1563 R4 M2): 直接 begin した後でも、約束の相手でない SKU・商品・原価 = 42501
  const a4 = await beginArgs(E0, 's004');
  for (const sql of ["update core.skus set name = '相手でない' where code = 's005'",
    "update core.products set name = '相手でない' where product_id = (select product_id from core.skus where code = 's005')",
    "insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id) select 1, sku_id, 1, 'manual', 'COMPLETE', '2031-01-01', 'x', 'human', 'x' from core.skus where code = 's005'",
    "update core.supplier_skus set is_primary = is_primary where sku_id = (select sku_id from core.skus where code = 's001')"]) {
    const e = await asEditorTx(E0, async () => { await pg.query(BEGIN_SQL, a4); return errOf(pg.query(sql)); });
    assert.equal(e?.code, '42501', sql); assert.match(e.message, /master_write_target/, sql);
  }
  // 約束の操作で書けない (表・書き方) = 42501。⑤-1 の画面のロールには skus の insert の権限が無い = 試験の取引の中だけ渡して、trigger が拒むことを見る
  const eOp = await (async () => {
    await pg.query('begin');
    try {
      await pg.query('grant insert on core.skus to master_edit');
      await pg.query('set role master_edit');
      await pg.query(BEGIN_SQL, a4);
      return await errOf(pg.query("insert into core.skus (company_id, sku_kind, code, name) values (1, 'single', 'zz-op', 'x')"));
    } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  })();
  assert.equal(eOp?.code, '42501'); assert.match(eOp.message, /master_write_operation/);
  assert.deepEqual((await q("select ops.master_write_allowed('sku_edit', 'core.skus', 'UPDATE') as u, ops.master_write_allowed('sku_edit', 'core.skus', 'INSERT') as i, ops.master_write_allowed('sku_edit', 'core.sku_costs', 'DELETE') as d, ops.master_write_allowed('sku_register', 'core.skus', 'UPDATE') as x"))[0],
    { u: true, i: false, d: true, x: false });
  // 知らない操作・同じ取引で 2 回 = 始められない
  assert.match(String((await asEditorTx(E0, async () => errOf(pg.query(BEGIN_SQL, await beginArgs(E0, 's004', { operation: 'sku_register' })))))?.message), /知らない操作/);
  assert.equal((await asEditorTx(E0, async () => { await pg.query(BEGIN_SQL, a4); return errOf(pg.query(BEGIN_SQL, a4)); }))?.code, '55000');
  // 保存の記録 (done) が約束と違う (保存の中身のハッシュ・SKU) = 42501
  for (const [hash, code] of [[`'d'`, 's004'], [`'c'`, 's005']]) {
    const e = await asEditorTx(E0, async () => {
      await pg.query(BEGIN_SQL, a4);
      return errOf(pg.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, started_at)
        select $1::uuid, 1, 'sku_edit', code, sku_id, 'naka@test', repeat(${hash}, 64), 'done', '{}', now() from core.skus where code = $2`, [a4[0], code]));
    });
    assert.equal(e?.code, '42501', code); assert.match(e.message, /master_write_session_mismatch/);
  }
  // 古い版 (画面が読んだ後に夜間の処理などが変えた) = 始められない (編集の印を DB でも確かめる)
  const stale = await beginArgs(E0, 's004');
  await pg.query("update core.skus set reorder_months = reorder_months + 1 where code = 's004'");   // 持ち主のロール = 版が上がる
  try {
    assert.match(String((await asEditorTx(E0, () => errOf(pg.query(BEGIN_SQL, stale))))?.message), /^version_conflict/);
  } finally { await pg.query("update core.skus set reorder_months = reorder_months - 1 where code = 's004'"); }
  // DB の版と JS の版 (editVersionsOf) は同じ形 (単品・セット・依頼のあるセット)
  for (const code of ['s001', 's004', 'set001', 'set004', 'set006']) {
    const sid = await skuId(code);
    assert.deepEqual((await q('select ops.master_edit_versions($1::bigint) as v', [sid]))[0].v, W.editVersionsOf(await cur(code)), code);
  }
  // 保存の流れ (画面のロール) で、約束の後に偽の core.actor_* を付けても、記録の誰が・request_id・理由は約束の値
  const id = uuid();
  const token4 = await tokenOf('s004');
  const forgeDb = { query: async (t, p) => { const r = await db.query(t, p); if (/ops\.begin_master_write/.test(t)) await db.query(forge); return r; }, exec: (t) => db.exec(t) };
  const r = await asEditor(E0, () => W.saveSku(forgeDb, { actor: 'naka@test', requestId: id, code: 's004', reason: '保存の流れ', seen: { token: token4, event_id: null }, values: { name: '保存の流れの直し' } },
    { ownership: ALL_COMPANY, open: true, now: NOW }));
  assert.equal(r.ok, true);
  const ev = await q("select actor_type, actor_id, source_system, request_id, reason_text, db_user from events.master_change_events where entity_type = 'sku' and attribute = 'name' and request_id = $1", [id]);
  assert.deepEqual(ev, [{ actor_type: 'human', actor_id: 'naka@test', source_system: 'portal_master_edit', request_id: id, reason_text: '保存の流れ', db_user: 'master_edit' }]);
  assert.equal(Number((await q("select count(*)::int as n from events.master_change_events where actor_id = 'forged@evil' or request_id = 'forged'"))[0].n), 0);
  const sess = (await q('select operation, sku_id::text as sku_id, payload_hash, edit_token from ops.master_write_sessions where request_id = $1', [id]))[0];
  assert.deepEqual([sess.operation, sess.sku_id, sess.edit_token], ['sku_edit', await skuId('s004'), token4]);
  assert.equal(sess.payload_hash, (await q('select payload_hash from ops.master_edit_requests where request_id = $1', [id]))[0].payload_hash);
  // 持ち主表が段階の記録と違う = 始められない
  assert.equal((await asEditorTx(E0, async () => errOf(pg.query(BEGIN_SQL, await beginArgs(E0, 's004', { ownership: MASTER_OWNERSHIP })))))?.code, 'P0001');
  // 持ち主が load の列: 段階の記録を load 入りの表にした別の DB で (名前 = company は保存の流れで通る・税率 = load は直接でも拒む)
  const own = withOwn({ 'skus.name': 'company', 'products.name': 'company' });
  const E4 = await setupDb();
  try {
    await openCutover(E4, own);
    const ok4 = await save('s004', { name: '名前は company' }, { E: E4, ownership: own, token: await tokenIn2(E4, 's004') });
    assert.equal(ok4.ok, true);
    const argsE4 = await beginArgs(E4, 's004', { ownership: own });
    const e = await asEditorTx(E4, async () => { await E4.pg.query(BEGIN_SQL, argsE4); return errOf(E4.pg.query("update core.skus set tax_rate = 0.08 where code = 's004'")); });
    assert.equal(e?.code, '42501'); assert.match(e.message, /owner_not_company: skus.tax_rate/);
  } finally { await E4.pg.close(); }
});

await ta('[14c] 約束の後の抜け道 (#1563 R5): 例外の SKU・含むセットの導く値でない列 / 人の原価・取引停止の代表の仕入先・形の違う構成の依頼・親子の輪 = 拒む / done の無い commit = 取引ごと拒む / 復元した約束の行では書けない', async () => {
  const sid = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id;
  /** 直接 begin した後に sql を流した誤り (無ければ null)。必ず巻き戻す */
  const afterBegin = (code, sql, params = [], opts = {}) => asEditorTx(E0, async () => { await pg.query(BEGIN_SQL, await beginArgs(E0, code, opts)); return errOf(pg.query(sql, params)); });
  // (a) 例外の SKU は約束できない
  await pg.query('begin');
  try {
    await pg.query("insert into core.skus (company_id, sku_kind, code, name) values (1, 'exception', 'ex-r5', '例外の SKU')");
    const args = await beginArgs(E0, 'ex-r5');
    await pg.query('set role master_edit');
    assert.match(String((await errOf(pg.query(BEGIN_SQL, args)))?.message), /例外の SKU/);
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  // (b) 単品 s001 を直す約束で、含むセット set001 は導く値 (税率・税区分・取扱区分・計算の原価) だけ
  for (const sql of ["update core.skus set name = '含むセットの名前' where code = 'set001'", "update core.skus set standard_price_jpy = 1 where code = 'set001'",
    "update core.skus set shipping_code = 'S01' where code = 'set001'"]) {
    const e = await afterBegin('s001', sql);
    assert.equal(e?.code, '42501', sql); assert.match(e.message, /master_write_derived_only/, sql);
  }
  assert.equal(await afterBegin('s001', "update core.skus set handling = 'discontinued' where code = 'set001'"), null);   // 導く値は通る
  const eCost = await afterBegin('s001', "insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id) select 1, sku_id, 1, 'manual', 'OVERRIDDEN', '2031-01-01', 'x', 'human', 'x' from core.skus where code = 'set001'");
  assert.equal(eCost?.code, '42501'); assert.match(eCost.message, /master_write_derived_only/);
  // (c) 業務の約束: 取引停止の仕入先を代表に・構成の依頼の構成品にセット自身 / セット・形の違う rows (表の CHECK)・親子の輪
  const eSup = await afterBegin('s002', "insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) select 1, supplier_id, (select sku_id from core.skus where code = 's002'), true, 'human', 'x' from core.suppliers where code = '0003'");
  assert.equal(eSup?.code, '42501'); assert.match(eSup.message, /代表の仕入先 .* は取引停止/);
  const reqSql = `insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id)
    select 1, sku_id, $1::jsonb, repeat('e', 64), '[]'::jsonb, 'x', 'naka@test', gen_random_uuid() from core.skus where code = 'set004'`;
  const [s1, s3, set4, set1] = [await sid('s001'), await sid('s003'), await sid('set004'), await sid('set001')];
  for (const [rows, want, label] of [
    [[{ sku_id: Number(set4), code: 'set004', qty: 1, sort: 1 }], /master_write_invariant: 構成の依頼の構成品/, 'セット自身'],
    [[{ sku_id: Number(set1), code: 'set001', qty: 1, sort: 1 }], /master_write_invariant: 構成の依頼の構成品/, 'セット'],
    [[{ sku_id: 99999999, code: 'nope', qty: 1, sort: 1 }], /master_write_invariant: 構成の依頼の構成品/, '無い SKU'],
    [[{ sku_id: Number(s1), qty: 0.6, sort: 1 }], /component_request_rows_ok|check constraint/, 'qty 0.6'],
    [[{ sku_id: Number(s1), qty: 100000, sort: 1 }], /component_request_rows_ok|check constraint/, 'qty 大きすぎ'],
    [[{ sku_id: Number(s1), qty: 1, sort: 1 }, { sku_id: Number(s1), qty: 2, sort: 2 }], /component_request_rows_ok|check constraint/, '重なる構成品'],
    [[{ sku_id: Number(s1), qty: 1, sort: 1 }, { sku_id: Number(s3), qty: 1, sort: 3 }], /component_request_rows_ok|check constraint/, '並びの抜け'],
    [[{ sku_id: String(s1), qty: 1, sort: 1 }], /component_request_rows_ok|check constraint/, 'sku_id が文字'],
    [[{ sku_id: Number(s1), qty: 1, sort: 1, extra: 'x' }], /component_request_rows_ok|check constraint/, '知らない鍵']]) {
    const e = await afterBegin('set004', reqSql, [JSON.stringify(rows)]);
    assert.ok(e && ['42501', '23514'].includes(e.code), `${label}: ${e && e.code} ${e && e.message}`);
    assert.match(e.message, want, label);
  }
  assert.equal(await afterBegin('set004', reqSql, [JSON.stringify([{ sku_id: Number(s1), code: 's001', qty: 1, sort: 1 }, { sku_id: Number(s3), code: 's003', qty: 2, sort: 2 }])]), null);   // 正しい形は通る
  // 親子の輪: 自分を親に / 子を親に (s003 の商品の親を s001 の商品にしてから、s001 の商品の親を s003 の商品に)
  const p1 = (await q("select product_id::text as id from core.skus where code = 's001'"))[0].id;
  const p3 = (await q("select product_id::text as id from core.skus where code = 's003'"))[0].id;
  const parentSql = "select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())";
  const eSelf = await asEditorTx(E0, async () => { await pg.query(BEGIN_SQL, await beginArgs(E0, 's001')); await pg.query(parentSql); return errOf(pg.query("update core.products set parent_product_id = $1, parent_set_by = 'manual' where product_id = $1", [p1])); });
  assert.equal(eSelf?.code, '42501'); assert.match(eSelf.message, /親子が輪/);
  const args1 = await beginArgs(E0, 's001');
  await pg.query('begin');
  try {
    await pg.query(parentSql);
    await pg.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [p3, p1]);   // 持ち主のロール: s003 の親 = s001
    await pg.query('set role master_edit');
    await pg.query(BEGIN_SQL, args1);
    const e = await errOf(pg.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [p1, p3]));
    assert.equal(e?.code, '42501'); assert.match(e.message, /親子が輪/);
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  // (R5 M2) 約束した取引は、done を書かずに commit できない (書いても・何も書かなくても)。done は保存の流れが書く
  for (const [label, sql] of [['書いて done なし', "update core.skus set reorder_months = reorder_months where code = 's004'"], ['何もせず', null]]) {
    const args = await beginArgs(E0, 's004');
    await pg.query('set role master_edit');
    await pg.query('begin');
    let e = null;
    try {
      await pg.query(BEGIN_SQL, args);
      if (sql) await pg.query(sql);
      e = await errOf(pg.query('commit'));
    } finally { try { await pg.query('rollback'); } catch { /* commit が失敗した取引はもう終わっている */ } await pg.query('set role deploy'); }
    assert.equal(e?.code, '42501', label); assert.match(e.message, /master_write_session_unfinished/, label);
  }
  assert.equal(Number((await q("select count(*)::int as n from ops.master_write_sessions s where not exists (select 1 from ops.master_edit_requests r where r.request_id = s.request_id and r.status = 'done')"))[0].n), 0);
  // (R5 M3) 復元した約束の行 (前の DB の乱数・今の取引の番号) があっても、begin していない書き込みは通らない・begin は止まらない
  // 持ち主表は今の epoch (active) と同じにする (0055 の入れる時の確かめ = 違う持ち主表の行はそもそも入らない)
  const args4 = await beginArgs(E0, 's004');
  await pg.query('begin');
  try {
    await pg.query(`insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
        actor_id, source_system, db_user, phase, owner_hash, ownership)
      select gen_random_uuid(), txid_current(), gen_random_uuid(), 'sku_edit', sku_id, '{}', '{}', repeat('a', 64), repeat('b', 64), '{}'::jsonb,
        'old@restored', 'portal_master_edit', 'master_edit', 'new_open', repeat('c', 64), ops.master_ownership_active_map() from core.skus where code = 's004'`);
    await pg.query('set role master_edit');
    await pg.query('savepoint s');
    const e = await errOf(pg.query("update core.skus set name = '復元した行で' where code = 's004'"));
    assert.equal(e?.code, '42501'); assert.match(e.message, /master_write_session_required/);
    await pg.query('rollback to savepoint s');
    await pg.query(BEGIN_SQL, args4);   // session_exists にならない
    assert.equal(await errOf(pg.query("update core.skus set name = '約束した後' where code = 's004'")), null);
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
});

console.log('\n画面 (router)');

process.env.COMPANY_DB_URL = 'postgres://owner@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test, other@test';
process.env.MASTER_EDIT_OPEN = '1';
let factoryMode = 'ok';
const opened = [];
const pgFactory = async (url) => {
  if (factoryMode === 'down') throw new Error('connect ECONNREFUSED');
  const role = /master_edit@/.test(url) ? 'master_edit' : 'deploy';
  opened.push(role);
  await pg.query(`set role ${role}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
};
__setPgClientFactory(pgFactory);
__setClock(() => NOW.getTime());
__setOwnership(ALL_COMPANY);
__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'editor' ? { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }
    : s === 'admin' ? { authenticated: true, email: 'admin@test', role: 'admin', allowedApps: '*' } : null;
  next();
});
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-edit`;
async function call(method, url, { body, session = 'editor', origin = true, ctype = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined && ctype) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
/**
 * 画面の JS が文法として読めること (描画の試験は通っても、画面の JS が壊れていることがある) と、EJS の出力が JS の中に混ざっていないこと。
 * 新しいデザインの画面 (一覧・1 つの商品) は JS を public/ のファイルに分けた = <script src> は中身を HTTP で取ってきて同じに確かめる。
 * <script type="application/json"> (画面の JS に渡す値) は JSON として読めること
 */
async function checkScripts(html, expected) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  assert.equal(all.length, [...html.matchAll(/<script\b/gi)].length, 'script の開きと閉じの数が合わない');
  const inline = all.filter((m) => !/\bsrc=/.test(m[1]) && !/type="application\/json"/.test(m[1])).map((m) => m[2]);
  assert.equal(inline.length, expected, `<script> の数 ${inline.length}`);
  for (const m of all.filter((x) => /type="application\/json"/.test(x[1]))) JSON.parse(m[2]);
  const files = [];
  for (const m of all.filter((x) => /\bsrc=/.test(x[1]))) {
    const src = /\bsrc="([^"]+)"/.exec(m[1])[1];
    assert.match(src, /^\/apps\/master-edit\/public\/[a-z-]+\.js\?v=[0-9a-f]{12}$/, src);
    const r = await fetch(ORIGIN + src.replace(/&amp;/g, '&'), { headers: { 'x-test-session': 'editor' } });
    assert.equal(r.status, 200, src); assert.match(r.headers.get('content-type') || '', /javascript/);
    files.push(await r.text());
  }
  for (const s of [...inline, ...files]) { new vm.Script(s); assert.ok(!/<%|%>/.test(s), 'EJS のタグが JS に残っている'); }
  return [...inline, ...files];
}
const tokenIn = (html) => /data-token="([0-9a-f]{64})"/.exec(html)?.[1];
const eventIn = (html) => /data-event-id="(\d+)"/.exec(html)?.[1];

await ta('[15] 一覧: 描画・検索・区分・状態・未入力 (売上分類はセットを導いてから)・導いた値の * ・末尾の /・つかいかた (画面のロールで読む)', async () => {
  opened.length = 0;
  let r = await call('GET', '/');
  assert.equal(r.status, 200); assert.match(r.text, /マスタの入力/);
  assert.deepEqual(opened, ['master_edit']);
  const listJs = await checkScripts(r.text, 0);
  assert.ok(listJs.some((s) => s.includes('requestNavigate')), '全体の JS (me-shell.js) を読んでいない');
  assert.match(r.text, /href="\/apps\/master-edit\/public\/master-edit\.css\?v=[0-9a-f]{12}"/);
  const css = await fetch(ORIGIN + '/apps/master-edit/public/master-edit.css', { headers: { 'x-test-session': 'editor' } });
  assert.equal(css.status, 200); assert.match(css.headers.get('content-type') || '', /css/);
  assert.equal((await fetch(ORIGIN + '/apps/master-edit/public/nope.js', { headers: { 'x-test-session': 'editor' } })).status, 404);
  assert.match(r.text, /href="sku\/set001"/);
  assert.match(r.text, /10%<span class="fx">計算<\/span>/);   // セットの税率 = 構成品から計算した値 (前の * の代わり)
  assert.ok(r.text.includes('<a class="chip on" href="./" aria-current="true">全部 <span class="n">'), '区分の札 (全部) がいま押されている');
  assert.match(r.text, /原価が未入力 <span class="n">\d+<\/span>/);   // 札の数 (会社全体)
  assert.ok(r.text.includes('この画面で絞る') && !r.text.includes('</span> 保存</div>'), '一覧は / の案内あり・Ctrl+S の案内なし');
  assert.ok(!/いまは保存できません/.test(r.text));
  r = await call('GET', '/?q=S00&kind=single');
  assert.ok(r.text.includes('sku/s001') && !r.text.includes('sku/set001'));
  // 参考の列: 発注アプリの台帳・ロジザードの写しが無い (この試験の DATA_DIR) = その列だけ「読めない」で一覧は出る (10/5 PR2)
  //   editor = 発注アプリの利用権が無い (allowedApps = master-edit だけ) = 注文残の列・絞り込みを出さない (#1620 Codex R1 M3) / admin (*) = 読めない列
  assert.ok(r.text.includes('在庫<span class="thsub">読めない</span>') && !r.text.includes('注文残<span class="thsub">') && r.text.includes('発注アプリの権限がないので出せません'), '利用権の無い人');
  const ra = await call('GET', '/?q=S00&kind=single', { session: 'admin' });
  assert.ok(ra.text.includes('注文残<span class="thsub">読めない</span>'), '読めない列');
  assert.ok((await call('GET', '/?po=1', { session: 'admin' })).text.includes('当てはまる商品がありません'), '注文残が読めない = 注文残ありでは当てない');
  r = await call('GET', '/?q=' + encodeURIComponent('セット 5'));
  assert.ok(r.text.includes('sku/set005') && !r.text.includes('sku/s001"'));
  await pg.query("update core.products set sales_class = null where product_id = (select product_id from core.skus where code = 's003')");
  assert.deepEqual((await R.listSkus(db, { missing: 'sales' }, { now: NOW })).rows.map((x) => x.code), ['s003', 's006', 'set001']);
  await pg.query("update core.products set sales_class = 1 where product_id = (select product_id from core.skus where code = 's003')");
  assert.deepEqual((await R.listSkus(db, { kind: 'set', state: 'available' }, { now: NOW })).rows.map((x) => [x.code, x.tax_derived]), [['set001', true]]);
  assert.deepEqual((await R.listSkus(db, { kind: 'set', state: 'discontinued' }, { now: NOW })).rows.map((x) => x.code), ['set004', 'set005', 'set006']);
  // 一覧の原価 (10/5 に 1 回の集合の走査に直した) = 1 つの商品の引き当て (costAsOfJoin の lateral) と同じ値・代表の仕入先も前の副問い合わせと同じ
  {
    const all = await R.listSkus(db, {}, { now: NOW });
    assert.ok(all.rows.length > 5);
    for (const x of all.rows) {
      const one = await R.lookupSku(db, x.code, { now: NOW });
      assert.equal(x.cost, one.cost_jpy, `原価 ${x.code}`);
      const ps = (await pg.query('select (select sp.code from core.supplier_skus y join core.suppliers sp on sp.supplier_id = y.supplier_id where y.sku_id = s.sku_id and y.is_primary order by sp.code limit 1) as c from core.skus s where s.code = $1', [x.code])).rows[0].c;
      assert.equal(x.primary_supplier, ps, `代表の仕入先 ${x.code}`);
    }
    // 件数 = 絞らなければ全部の商品 (軽い読みで数える)
    assert.equal(all.total, (await pg.query('select count(*)::int as n from core.skus where company_id = 1')).rows[0].n);
  }
  const bare =await fetch(`${ORIGIN}/apps/master-edit`, { headers: { 'x-test-session': 'editor' }, redirect: 'manual' });
  assert.equal(bare.status, 301); assert.equal(bare.headers.get('location'), '/apps/master-edit/');
  const m = await call('GET', '/manual');
  assert.equal(m.status, 200);
  for (const word of ['保存', '構成品を足す', '保存した後の値', 'メーカーからの値上げ通知', 'ひらがな・カタカナ・半角カナ', '商品コードを複数', '作れる数', '画面を開き直す', '例外原価をやめる (構成品の合計に戻す)', 'NE との差', '未入力', '切替前', 'NE でやること', 'Ctrl + K', '捨てて移る', '🔒 の値']) assert.ok(m.text.includes(word), `つかいかたに「${word}」が無い`);
});

await ta('[15] 詳細検索の印 (POST /api/search) の大きさの上限 (#1620 Codex R2 M1): 巨大な入力は速く 413・上限内は今までどおり・表の合計バイト数で古い印から消える', async () => {
  const T = await import('../apps/master-edit/search-token.mjs');
  const timed = async (body) => { const t0 = performance.now(); const r = await call('POST', '/api/search', { body }); return { ...r, ms: performance.now() - t0 }; };
  // 240,000 字の 1 つの欄 = 欄の字数で断る (分けない・溜めない)
  let r = await timed({ codes: 'x'.repeat(240000) });
  assert.equal(r.status, 413); assert.equal(r.j.error, 'too_large'); assert.match(r.j.message, /商品コード が長すぎます/);
  assert.ok(r.ms < 1000, `速く断る (${Math.round(r.ms)}ms)`);
  // 10 万の値 (区切りだらけ・20 万字。256KB の JSON の上限の内) も同じ
  r = await timed({ jans: Array(100000).fill('4').join(',') });
  assert.equal(r.status, 413); assert.match(r.j.message, /JAN が長すぎます/);
  assert.ok(r.ms < 1000, `速く断る (${Math.round(r.ms)}ms)`);
  // 分ける関数そのもの: 10 万の値でも上限 + 1 件で止める・重複は Set
  const t0 = performance.now();
  const xs = R.splitMulti(Array.from({ length: 100000 }, (_, i) => `c${i % 50000}`).join('\n'), R.MULTI_MAX + 1);
  assert.equal(xs.length, R.MULTI_MAX + 1);
  assert.ok(performance.now() - t0 < 200, '上限で走査を止める');
  assert.deepEqual(R.splitMulti('a, a\tb\r\nb、c', 10), ['a', 'b', 'c']);
  // 1 つの値の字数 (商品コード 64・JAN 32・名前 200)
  r = await call('POST', '/api/search', { body: { codes: `ok1\n${'a'.repeat(65)}` } });
  assert.equal(r.status, 413); assert.match(r.j.message, /商品コード は 1 つ 64 字まで/);
  r = await call('POST', '/api/search', { body: { jans: '4'.repeat(33) } });
  assert.equal(r.status, 413); assert.match(r.j.message, /JAN は 1 つ 32 字まで/);
  r = await call('POST', '/api/search', { body: { name: 'あ'.repeat(201) } });
  assert.equal(r.status, 413); assert.match(r.j.message, /商品名 は 1 つ 200 字まで/);
  assert.equal((await call('POST', '/api/search', { body: { codes: ['x'] } })).status, 400, '文字でない値');
  // #1620 Codex R3 Low: 数・真偽・null も 400 (文字だけ) / 商品名は 200 字まで検索に使う (61〜200 字を黙って切らない)
  for (const body of [{ codes: 123 }, { name: true }, { jans: null }]) assert.equal((await call('POST', '/api/search', { body })).status, 400, `文字でない値 ${JSON.stringify(body)}`);
  const { normalizeFilters: nf } = await import('../apps/master-edit/read.mjs');
  assert.equal(nf({ name: 'い'.repeat(150) }).name.length, 150, '商品名 150 字はそのまま使う');
  // 条件全体 64KB まで (各欄 500 件 × 64 字 = 4 欄で 128KB = 断る)
  const big = Array.from({ length: 500 }, (_, i) => `${String(i).padStart(3, '0')}${'z'.repeat(61)}`).join('\n');
  r = await call('POST', '/api/search', { body: { codes: big, parents: big, sups: big } });
  assert.equal(r.status, 413); assert.match(r.j.message, /検索の条件が大きすぎます/);
  // 上限内は今までどおり (500 件 × 64 字 = 1 欄 32KB は通る・印で開ける)
  r = await call('POST', '/api/search', { body: { codes: big, kind: 'single' } });
  assert.equal(r.status, 200); assert.match(r.j.url, /\?kind=single&s=[A-Za-z0-9_-]{22}$/);
  const page = await fetch(ORIGIN + r.j.url, { headers: { 'x-test-session': 'editor' } });
  assert.equal(page.status, 200); assert.match(await page.text(), /500 件 見つからない/);
  // 表の合計バイト数の上限: 古い (使っていない) 印から消える。使った印は残る (LRU)
  T.__clearSearchTokens();
  T.__setTokenLimits({ tokens: 5000, bytes: 3000 });
  try {
    const one = (i) => ({ codes: `${String(i).padStart(4, '0')}${'q'.repeat(1100)}` });   // 1 件 ≒ 1.2KB (3 件で 3,000 バイトを超える)
    const a = T.putSearch(one(1)), b = T.putSearch(one(2));
    assert.ok(T.getSearch(a), 'a を使う (新しい側へ)');
    const c = T.putSearch(one(3));
    assert.equal(T.getSearch(b), null, '一番古い (使っていない) b が消える');
    assert.ok(T.getSearch(a) && T.getSearch(c));
    assert.ok(T.__tokenStats().bytes <= 3000, `合計 ${T.__tokenStats().bytes}`);
    T.putSearch(one(4)); T.putSearch(one(5));
    assert.equal(T.__tokenStats().count, 2);
    assert.ok(T.__tokenStats().bytes <= 3000);
    // 件数の上限も
    T.__setTokenLimits({ tokens: 2, bytes: 1e9 });
    T.putSearch({ codes: 'x1' }); T.putSearch({ codes: 'x2' }); T.putSearch({ codes: 'x3' });
    assert.equal(T.__tokenStats().count, 2);
  } finally { T.__setTokenLimits(); T.__clearSearchTokens(); }
});

await ta('[15] 単品・セットの画面: 描画・画面の JS・編集の印・導く値・食い違い・JAN とロジザードは単品だけ・404', async () => {
  let r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200);
  const scripts = await checkScripts(r.text, 0);
  const skuJs = scripts.find((s) => s.includes('me-sku.js — 1 つの商品の画面')) || '';
  for (const api of ["'/api/sku/'", "'/api/lookup?code='"]) assert.ok(skuJs.includes(api), `画面が ${api} を呼んでいない`);
  assert.equal(tokenIn(r.text), await tokenOf('s001'));
  assert.match(r.text, /data-can-save="1"/);
  assert.match(r.text, /id="lab-jan">JAN</); assert.match(r.text, /ロジザードが正/);
  // 未保存に数えるのは保存する欄だけ (data-dirty-field)。保存の理由・全体から探す・一覧の絞る欄には付けない
  for (const f of ['name', 'handling', 'parent_code', 'standard_price', 'tax_rate', 'sales_class', 'primary_supplier', 'shipping_code', 'reorder_months', 'jan']) assert.match(r.text, new RegExp(`data-dirty-field="${f}"`), f);
  assert.ok(!/id="reason"[^>]*data-dirty-field|data-dirty-field[^>]*id="reason"/.test(r.text), '保存の理由を数えない');
  // s001 を使うセット set005 は、前の試験 ([9] 日の境目) で原価が 2030-01-11 (画面の今日の翌日) から始まる = s001 の原価はここでは閉じる (サーバーも set_cost_future)
  assert.ok(r.text.includes('id="cost-future"') && r.text.includes('set005') && !r.text.includes('data-dirty-field="cost"'), '先の日付の原価のあるセットを使う単品の原価は閉じる');
  const futRes = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) }, values: { cost: { jpy: '130', reason: '試験' } } } });
  assert.deepEqual([futRes.status, futRes.j.reason], [409, 'set_cost_future']);
  assert.match(r.text, /<span class="b mute">単品<\/span>/);
  assert.match(r.text, /<h2 id="h-save">保存すると変わること<\/h2>/);
  assert.match(r.text, /最後に直した人 /);   // 変更の記録から (日本時間)
  assert.match(r.text, /\d{1,2}\/\d{1,2} \([日月火水木金土]\) \d{2}:\d{2}/);
  assert.ok(!/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(r.text), 'DB の時刻の文字 (UTC) をそのまま出さない');
  const pageJson = JSON.parse(/<script type="application\/json" id="me-page">([\s\S]*?)<\/script>/.exec(r.text)[1]);
  assert.deepEqual([pageJson.code, pageJson.kind, pageJson.canSave], ['s001', 'single', true]);
  await save('set001', { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 2 }] });
  await promote((await observe([{ set_code: 'set001', rows: [{ code: 's001', qty: 1, sort: 1 }] }], { at: nextAt() })).set001);
  r = await call('GET', '/sku/set001');
  await checkScripts(r.text, 0);
  assert.ok(r.text.length > 5000 && r.text.includes('セット 1'), '描けている');
  assert.ok(!/id="lab-jan"/.test(r.text) && !/ロジザードが正/.test(r.text), 'セットに JAN・ロジザードの欄を出さない');
  assert.match(r.text, /計算で決まる値 \(今の構成から/); assert.match(r.text, /NE でやること \(構成の依頼\)/); assert.match(r.text, /依頼どおりなら/);
  assert.match(r.text, /<table class="comp" id="comp" data-field="components" data-dirty-field="components"/);
  assert.ok(!/id="set-mode"[^>]*data-dirty-field/.test(r.text), '構成の見せ方の切り替えは数えない');
  assert.match(r.text, /<tr class="(add|rm|qty)">/);   // くらべる (今 → 依頼)
  assert.match(r.text, /NE でやること \(食い違い: NE の構成が依頼と違う\)/);
  const hist = await call('GET', '/sku/set001/history');
  assert.equal(hist.status, 200); assert.match(hist.text, /構成の依頼/); assert.match(hist.text, /マスタの入力/);
  assert.equal((await call('GET', '/sku/nope')).status, 404);
  assert.equal((await call('GET', '/sku/nope/history')).status, 404);
  const lk = await call('GET', '/api/lookup?code=S002');
  assert.deepEqual([lk.status, lk.j.item.code, lk.j.item.kind], [200, 's002', 'single']);
  assert.equal((await call('GET', '/api/lookup?code=nope')).status, 404);
});

await ta('[15] 見せ方 (PR 画面の作り直し 1): 変更の記録は人の言葉・日本時間 / 誤りの画面 / 時刻と原価の帯の部品 / Amazon の未判定の日付', async () => {
  const U = await import('../apps/master-edit/ui-format.mjs');
  const hist = await call('GET', '/sku/s001/history');
  assert.equal(hist.status, 200);
  assert.ok(hist.text.includes('時刻 (日本時間)') && hist.text.includes('夜間の取り込み'), '描けている');
  for (const raw of ['standard_price_jpy', 'reorder_months', '{&#34;', 'portal_master_edit']) assert.ok(!hist.text.includes(raw), `変更の記録に生の ${raw} を出さない`);
  assert.ok(!/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(hist.text), 'DB の時刻の文字をそのまま出さない');
  const nf = await call('GET', '/sku/nope');
  assert.equal(nf.status, 404); assert.match(nf.text, /商品コード nope は Company DB にありません/); assert.match(nf.text, /master-edit\.css/);
  // 時刻: DB の文字 (+00 / +09)・ISO・Date を東京にそろえる。時間帯の無い文字は推測しない
  const now = Date.parse('2026-10-02T03:00:00Z');
  assert.equal(U.fmtJst('2026-10-02 01:54:00.123456+00', { nowMs: now }), '10/2 (金) 10:54');
  assert.equal(U.fmtJst('2026-10-02 10:54:00+09', { nowMs: now }), '10/2 (金) 10:54');
  assert.equal(U.fmtJst('2025-12-31T15:30:00Z', { nowMs: now }), '1/1 (木) 00:30');
  assert.equal(U.fmtJst('2025-12-30T15:30:00Z', { nowMs: now }), '2025/12/31 (水) 00:30');
  assert.equal(U.fmtJst(new Date('2026-10-02T03:00:00Z'), { nowMs: now }), '10/2 (金) 12:00');
  assert.equal(U.fmtJst('2026-10-02 01:54:00', { nowMs: now }), '2026-10-02 01:54:00');
  assert.equal(U.fmtDay('2026-11-01', '2026-10-02'), '11/1 (日)'); assert.equal(U.fmtDay('2027-01-04', '2026-10-02'), '2027/1/4 (月)');
  // 変更の記録の言葉
  assert.equal(U.eventWords({ operation: 'UPDATE', entity_type: 'sku', attribute: 'standard_price_jpy', old_value: 1680, new_value: 1780 }).text, '標準売価 1,680 円 → 1,780 円');
  assert.equal(U.eventWords({ operation: 'UPDATE', entity_type: 'sku', attribute: 'handling', old_value: 'active', new_value: 'discontinued' }).text, '取扱区分 取扱中 → 中止');
  assert.equal(U.eventWords({ operation: 'UPDATE', entity_type: 'sku', attribute: 'version', old_value: 1, new_value: 2 }), null);
  assert.equal(U.eventWords({ operation: 'INSERT', entity_type: 'external_id', new_value: { external_value: '4900000001013' } }).text, 'JAN を足した 4900000001013');
  assert.equal(U.eventWords({ operation: 'INSERT', entity_type: 'sku_cost', new_value: { cost_jpy: 860, valid_from: '2026-11-01', cost_source: 'manual' } }).text, '原価 860 円 (11/1 (日) から) · 手で入れた');
  assert.equal(U.actorWords({ actor_type: 'system', source_system: 'company_db_load' }), '夜間の取り込み');
  // 原価の帯: これまで・いま・先の日付 (今日を含む行が「いま」)
  const tl = U.costTimeline([{ cost_jpy: 780, valid_from: '2026-07-01', valid_to: '2026-09-26' }, { cost_jpy: 820, valid_from: '2026-09-27', valid_to: '2026-10-31' }, { cost_jpy: 860, valid_from: '2026-11-01', valid_to: null }], '2026-10-02');
  assert.deepEqual(tl.segs.map((s) => [s.kind, s.value]), [['past', '780 円'], ['now', '820 円'], ['fut', '860 円']]);
  assert.equal(tl.hasFuture, true); assert.equal(tl.months.length, 6);
  assert.ok(tl.segs.every((s) => s.left >= 0 && s.left + s.width <= 100.0001));
  // Amazon の未判定の日 (node-postgres の date = その日の 0 時の Date) を YYYY-MM-DD に (前は「Sat Sep 26 2026 00:00:00 GMT+0900」が出た)
  // 札の数 (listCounts = 原価の表を 1 回だけ走査する形・#1589 R1 M1) は一覧の絞り込みと同じ数 (原価・税率・区分・中止)
  const cnt = await R.listCounts(db, { now: NOW });
  for (const [k, f] of [['miss_cost', { missing: 'cost' }], ['miss_tax', { missing: 'tax' }], ['n_set', { kind: 'set' }], ['n_single', { kind: 'single' }], ['discontinued', { state: 'discontinued' }], ['n_all', {}]]) assert.equal(cnt[k], (await R.listSkus(db, f, { now: NOW })).total, k);
  const { dateOnly } = await import('../apps/master-edit/amazon-read.mjs');
  assert.equal(dateOnly(new Date(2026, 8, 26)), '2026-09-26');
  assert.equal(dateOnly('2026-09-26'), '2026-09-26');
});

await ta('[15] 先の日付の原価: 画面が閉じる原価の欄 = サーバーが断る (セット自身 = cost_future・使っているセット = set_cost_future)・ほかの欄は開いたまま (#1589 R2 M2)', async () => {
  const FUT = '2030-02-01';   // 画面の今日 2030-01-10 より先
  await q("update core.sku_costs set valid_to = $1::date - 1 where valid_to is null and sku_id = (select sku_id from core.skus where code = 'set006')", [FUT]);
  await q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 777, 'set_calc', 'COMPLETE', $1::date from core.skus where code = 'set006'", [FUT]);
  try {
    // 単品 s006 (set006 だけの構成品): 原価の欄は閉じる・理由に set006・ほかの欄 (名前) は直せる → 原価を送るとサーバーも断る
    let r = await call('GET', '/sku/s006');
    assert.equal(r.status, 200);
    assert.ok(r.text.includes('id="cost-future"') && /セット set006 \(2\/1 \(金\) から\)/.test(r.text), '使っているセットの先の原価を理由に出す');
    assert.ok(!r.text.includes('id="btn-cost-open"') && !r.text.includes('id="cost-jpy"'), '原価の欄を出さない');
    assert.ok(r.text.includes('data-field="name"'), 'ほかの欄は直せる');
    let res = await call('POST', '/api/sku/s006', { body: { request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) }, values: { cost: { jpy: '120', reason: '試験' } } } });
    assert.deepEqual([res.status, res.j.reason], [409, 'set_cost_future']);
    // セット set006: 例外原価の欄は閉じる・サーバーも cost_future
    r = await call('GET', '/sku/set006');
    assert.ok(r.text.includes('id="xcost-future"') && !r.text.includes('id="xcost-jpy"'), '例外原価の欄を出さない');
    res = await call('POST', '/api/sku/set006', { body: { request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) }, values: { exception_cost: { jpy: '500', reason: '試験' } } } });
    assert.deepEqual([res.status, res.j.reason], [400, 'cost_future']);
    // 先の原価の無い単品 (s002) は原価の欄が開いている (閉じすぎない)
    r = await call('GET', '/sku/s002');
    assert.ok(r.text.includes('id="cost-jpy"') && !r.text.includes('id="cost-future"'));
    // 使っているセットの先の原価が例外原価 (manual・override_zero) なら、セットの合計は計算し直さない = 単品の原価は閉じない (サーバーの OVERRIDE_SOURCES と同じ)
    for (const src of ['manual', 'override_zero']) {
      await q("update core.sku_costs set cost_source = $2 where valid_from = $1::date and sku_id = (select sku_id from core.skus where code = 'set006')", [FUT, src]);
      r = await call('GET', '/sku/s006');
      assert.ok(r.text.includes('id="cost-jpy"') && !r.text.includes('id="cost-future"'), `${src} の先の原価では閉じない`);
    }
  } finally {
    await q("delete from core.sku_costs where valid_from = $1::date and sku_id = (select sku_id from core.skus where code = 'set006')", [FUT]);
    await q("update core.sku_costs set valid_to = null where valid_to = $1::date - 1 and sku_id = (select sku_id from core.skus where code = 'set006')", [FUT]);
  }
});

await ta('[15] 保存の API: 画面と同じ形で通る (画面のロールで書く)・名簿・Origin・Content-Type・押し直し・印の違い', async () => {
  const page = await call('GET', '/sku/s002');
  const body = { request_id: uuid(), reason: '画面から', seen: { token: tokenIn(page.text), event_id: eventIn(page.text) }, values: { name: '画面から直した', reorder_months: '2' } };
  assert.equal((await call('POST', '/api/sku/s002', { body, session: 'admin' })).status, 403);
  const keep = process.env.MASTER_EDITORS;
  process.env.MASTER_EDITORS = ' , ';
  const none = await call('POST', '/api/sku/s002', { body });
  assert.equal(none.status, 403); assert.match(none.j.error, /誰も保存できません/);
  process.env.MASTER_EDITORS = keep;
  assert.equal((await call('POST', '/api/sku/s002', { body, origin: false })).j.error, 'origin_mismatch');
  assert.equal((await call('POST', '/api/sku/s002', { body, ctype: false })).status, 415);
  opened.length = 0;
  const ok = await call('POST', '/api/sku/s002', { body });
  assert.equal(ok.status, 200, ok.text);
  assert.deepEqual(opened, ['master_edit']);
  assert.deepEqual(ok.j.changed.map((c) => c.field), ['name']);
  assert.equal((await skuRow('s002')).name, '画面から直した');
  assert.equal((await call('POST', '/api/sku/s002', { body })).j.replayed, true);
  const conflict = await call('POST', '/api/sku/s002', { body: { ...body, request_id: uuid(), values: { name: 'もう一度' } } });
  assert.deepEqual([conflict.status, conflict.j.reason], [409, 'version_conflict']);
  assert.ok(Array.isArray(conflict.j.events) && conflict.j.events.length > 0);
  assert.equal((await call('POST', '/api/sku/s002', { body: { ...body, request_id: uuid(), values: { tax_rate: '5' } } })).status, 400);
});

await ta('[15] 保存を開いていない (MASTER_EDIT_OPEN なし / 持ち主表が記録と違う) = 帯・欄と保存のボタンが閉じる・API は 409', async () => {
  delete process.env.MASTER_EDIT_OPEN;
  let r = await call('GET', '/sku/s001');
  assert.match(r.text, /いまは保存できません \(保存を開くスイッチ/);
  assert.match(r.text, /data-can-save="0"/); assert.ok(!/id="save"/.test(r.text), '保存できない画面に保存のボタンを出さない');
  assert.ok(!/data-field="name"/.test(r.text), '直せない欄は入力欄にしない (🔒 の値で見せる)');
  assert.ok(r.text.includes('data-row="name"><div class="lab" id="lab-name">名前</div><div class="ctl"><span class="lockval">'), '名前は 🔒 の値');
  assert.match(r.text, /<span class="b warn" title="この項目の正はまだ NE・\/register です">切替前<\/span>/);
  const body = { request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) }, values: { name: 'x' } };
  assert.deepEqual([(await call('POST', '/api/sku/s001', { body })).status], [409]);
  process.env.MASTER_EDIT_OPEN = '1';
  __setOwnership(null);   // 本番の持ち主表 (全部 load) = 切替のときの記録と違う
  r = await call('GET', '/sku/s001');
  assert.match(r.text, /いまは保存できません \(持ち主表が切替のときの記録と違う\)/); assert.match(r.text, /data-can-save="0"/);
  const res2 = await call('POST', '/api/sku/s001', { body: { ...body, request_id: uuid(), seen: { token: tokenIn(r.text), event_id: eventIn(r.text) } } });
  assert.deepEqual([res2.status, res2.j.reason], [409, 'before_cutover']);
  __setOwnership(ALL_COMPANY);
  assert.ok(!/いまは保存できません/.test((await call('GET', '/sku/s001')).text));
});

await ta('[15] 書き込み用の接続 (COMPANY_DB_MASTER_EDIT_URL) が無い = 持ち主のロールで読むだけ・保存のボタンなし・API は 503', async () => {
  const keep = process.env.COMPANY_DB_MASTER_EDIT_URL;
  delete process.env.COMPANY_DB_MASTER_EDIT_URL;
  opened.length = 0;
  const r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200); assert.deepEqual(opened, ['deploy']);
  assert.match(r.text, /いまは保存できません \(書き込み用の接続/); assert.match(r.text, /data-can-save="0"/);
  const res = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tokenIn(r.text) }, values: { name: 'x' } } });
  assert.deepEqual([res.status, res.j.reason], [503, 'no_write_role']);
  process.env.COMPANY_DB_MASTER_EDIT_URL = keep;
});

await ta('[15] Company DB が無い・届かない = 画面は帯 (保存のボタンなし)・API は 503', async () => {
  factoryMode = 'down';
  let r = await call('GET', '/');
  assert.equal(r.status, 200); assert.match(r.text, /Company DB につながりません/);
  r = await call('GET', '/sku/s001');
  assert.equal(r.status, 200); assert.match(r.text, /Company DB につながりません/); assert.ok(!/id="save"/.test(r.text));
  const res = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: 'a'.repeat(64) }, values: { name: 'x' } } });
  assert.deepEqual([res.status, res.j.reason], [503, 'db_unreachable']);
  factoryMode = 'ok';
  const keep = [process.env.COMPANY_DB_URL, process.env.COMPANY_DB_MASTER_EDIT_URL];
  delete process.env.COMPANY_DB_URL; delete process.env.COMPANY_DB_MASTER_EDIT_URL;
  r = await call('GET', '/');
  assert.match(r.text, /COMPANY_DB_URL/);
  assert.equal((await call('GET', '/api/lookup?code=s001')).status, 503);
  [process.env.COMPANY_DB_URL, process.env.COMPANY_DB_MASTER_EDIT_URL] = keep;
});

await ta('[15] server.js: Render だけ (env + PORTAL_VARIANT)・requireAppAccess・共通の JSON parser を通さない・アプリ一覧に載る', async () => {
  const s = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  assert.match(s, /if \(process\.env\.MASTER_EDIT_ENABLED === '1' && PORTAL_VARIANT === 'render'\) \{\r?\n\s+app\.use\('\/apps\/master-edit', requireAppAccess\('master-edit'\), masterEditRouter\);/);
  assert.equal((s.match(/masterEditRouter/g) || []).length, 2);
  const skip = s.indexOf("if (normalizedPath.toLowerCase().startsWith('/apps/master-edit')) return next();");
  assert.ok(skip > 0 && skip < s.indexOf('return globalJsonParser(req, res, next);'), '共通の JSON parser の除外に master-edit が無い');
  const { apps } = await import('../lib/portal-apps.js');
  assert.equal(apps.find((a) => a.id === 'master-edit')?.path, '/apps/master-edit/');
});

await ta('[16] FBA (JP) の在庫 (10/5): 読み元 = Company DB の在庫の日次 (最新の complete の日)・何時時点 (取得の時刻)・1 × 1 の出品だけ足す (まとめ売り・セットの出品は別)・古い / 読めない・権限 (master_edit = 2 つの表を読むだけ)', async () => {
  const { ingestStockDay } = await import('../apps/company-db/ingest/stock-daily.mjs');
  const F = await import('../apps/master-edit/fba-stock.mjs');
  const sid = async (code) => Number((await q('select sku_id from core.skus where code = $1', [code]))[0].sku_id);
  const s1 = await sid('s001'), s2 = await sid('s002'), set1 = await sid('set001');
  const fbaTh = (html) => /id="th-fba">FBA \(JP\)<span class="thsub">([^<]*)<\/span>/.exec(html)?.[1];
  const fbaCell = (html, code) => colCell(html, code, 'fba');
  // ① 日次がまだ無い = 「読めない」(一覧・単品とも画面は出る)
  let r = await call('GET', '/?kind=single');
  assert.equal(r.status, 200);
  assert.equal(fbaTh(r.text), '読めない');
  assert.match((await call('GET', '/sku/s001')).text, /id="ref-fba-when">読めない \(FBA の在庫の日次 \(全部取れた日\) がまだありません\)/);
  // 出品: s001 × 1 が 2 つ (出品 SKU を足す)・s001 × 3 (まとめ売り)・s001 + s002 (セットの出品)・s002 × 1・NE のセット set001 × 1
  const listing = async (code, comps) => {
    const id = (await q(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', $1, 'active') returning listing_id`, [code]))[0].listing_id;
    let i = 0;
    for (const [skuId, qty] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, $3, $4, 'exact', 'system')`, [id, skuId, qty, i++]);
  };
  await listing('pr-s001-a', [[s1, 1]]); await listing('pr-s001-b', [[s1, 1]]); await listing('pr-s001-3p', [[s1, 3]]); await listing('pr-s1s2', [[s1, 1], [s2, 1]]);
  await listing('pr-s002', [[s2, 1]]); await listing('pr-set001', [[set1, 1]]);
  const row = (code, a, x, p, c, w = 0, s = 0, rc = 0) => ({ code, fba_available: a, fba_fc_transfer: x, fba_fc_processing: p, fba_customer_order: c, fba_inbound_working: w, fba_inbound_shipped: s, fba_inbound_received: rc });
  const opt = { todayJst: TODAY, now: NOW.getTime() };
  // 1/9 の朝 07:40 (JST) に取った日 = 画面の今 (1/10 12:00) から 28 時間 20 分前 = 古い
  await ingestStockDay(db, { source: 'fba_jp', snapshot_date: '2030-01-09', captured_at: '2030-01-08T22:40:00.000Z',
    rows: [row('PR-S001-A', 10, 1, 2, 3, 4, 5, 6), row('pr-s001-b', 5, null, null, null), row('pr-s001-3p', 7, 0, 0, 0), row('pr-s1s2', 2, 0, 0, 0), row('pr-set001', 4, 0, 0, 0), row('pr-unknown', 9, 0, 0, 0)] }, opt);
  r = await call('GET', '/?kind=single');
  assert.equal(fbaTh(r.text), '古い 1/9 07:40', '見出しの下 = 何時時点か (古い)');
  assert.match(r.text, /title="FBA \(日本\) の販売可能 · 1\/9 \(水\) 07:40 時点 \(朝のレポート\) \(26 時間より前 = 古い\)/);
  assert.match(r.text, /class="n ref stale" style="width:92px"/);
  assert.equal(fbaCell(r.text, 's001'), '15', 's001 = 1 × 1 の出品 2 つの販売可能の合計 (10 + 5)・まとめ売り 7 とセットの出品 2 は入れない');
  assert.equal(fbaCell(r.text, 's002'), '0出品なし', 's002 = その日のレポートに 1 × 1 の出品の行が無い = 販売可能 0 (mart.v_sku_stock と同じ)・「出品なし」と添える (#1625 Codex R1 M)');
  assert.equal(fbaCell(r.text, 's003'), '0出品なし', '出品が 1 つも無い SKU も 0');
  {
    // mart.v_sku_stock (商品の動き) と同じ数 (同じ complete の日・行の無い SKU は 0)
    const v = await q("select k.code, v.fba_jp_available::int as n from mart.v_sku_stock v join core.skus k on k.sku_id = v.sku_id where k.code in ('s001', 's002', 's003') order by k.code");
    assert.deepEqual(v.map((x) => [x.code, x.n]), [['s001', 15], ['s002', 0], ['s003', 0]]);
  }
  assert.equal(fbaCell((await call('GET', '/?kind=set')).text, 'set001'), '4', 'NE のセット = そのセットに 1 × 1 で当たる出品の数 (構成品へ展開しない)');
  // 1/10 は RESTOCK が取れなかった (partial) = 使わない (前の complete の日 1/9 のまま・古い)・そのことを見出しの説明に出す
  await ingestStockDay(db, { source: 'fba_jp', snapshot_date: '2030-01-10', captured_at: '2030-01-09T22:41:00.000Z', partial: true,
    rows: [row('PR-S001-A', 99, null, null, null), row('pr-s001-b', 1, null, null, null), row('pr-s001-3p', 7, null, null, null), row('pr-s1s2', 2, null, null, null), row('pr-set001', 4, null, null, null), row('pr-unknown', 9, null, null, null)] }, opt);
  r = await call('GET', '/?kind=single');
  assert.equal(fbaCell(r.text, 's001'), '15', 'partial の日は読まない');
  assert.match(r.text, /1\/10 \(木\) は一部だけ取れた \(FC 移管中・処理中・出荷待ちが分からない\)ので前の日の値/);
  // 単品の画面: 販売可能・何時時点・内訳 (FC の 3 区分が分からない出品 SKU があれば「+不明」)・出品 SKU・まとめ売り / セットの出品は別に
  let p = (await call('GET', '/sku/s001')).text;
  assert.match(p, /id="ref-fba">15</);
  assert.match(p, /id="ref-fba-when" style="display:block">古い · 1\/9 \(水\) 07:40 時点 \(Amazon の朝のレポート\) · 1\/10 \(木\) は一部だけ取れた/);
  const parts = /<table id="ref-fba-parts">([\s\S]*?)<\/table>/.exec(p)[1].replace(/\s*<[^>]+>\s*/g, '|').replace(/\|+/g, '|');
  assert.equal(parts, '|FBA の内訳|数|販売可能|15|FC 移管中|1 +不明|FC 処理中|2 +不明|出荷待ち (注文の引き当て)|3 +不明|入荷待ち (納品の途中)|15|');
  assert.match(p, /id="ref-fba-unknown">不明 = その出品 SKU が Amazon の RESTOCK レポートに無かった/);
  assert.match(p, /id="ref-fba-skus">出品 SKU 2 つの合計: PR-S001-A 10 · pr-s001-b 5</);
  assert.match(p, /id="ref-fba-bundles"[^>]*>まとめ売り・セットの出品 \(上の数に入れていない\): pr-s001-3p ×3 = 7 · pr-s1s2 \(ほか 1 品と\) = 2</);
  p = (await call('GET', '/sku/s002')).text;
  assert.match(p, /id="ref-fba">0</); assert.match(p, /id="ref-fba-none"[^>]*>FBA の出品なし/); assert.ok(!/id="ref-fba-parts"/.test(p), '行が無い = 内訳の表は出さない');
  assert.match(p, /pr-s1s2 \(ほか 1 品と\) = 2/, 's002 にもセットの出品を出す (合計には入れない)');
  // 1/10 が後から全部取れた (partial → complete) = 新しい日・新しい時刻・古くない
  await ingestStockDay(db, { source: 'fba_jp', snapshot_date: '2030-01-10', captured_at: '2030-01-09T23:05:00.000Z',
    rows: [row('PR-S001-A', 8, 0, 0, 1, 2), row('pr-s001-b', 3, 0, 1, 0), row('pr-s001-3p', 6, 0, 0, 0), row('pr-s1s2', 2, 0, 0, 0), row('pr-set001', 4, 0, 0, 0), row('pr-unknown', 9, 0, 0, 0), row('pr-s002', 0, 0, 0, 0)] }, opt);
  r = await call('GET', '/?kind=single');
  assert.equal(fbaTh(r.text), '1/10 08:05', '新しい complete の日 = 取得の時刻 (08:05 JST)');
  assert.match(r.text, /class="n ref" style="width:92px"/);
  assert.ok(!/は一部だけ取れた/.test(r.text));
  assert.deepEqual([fbaCell(r.text, 's001'), fbaCell(r.text, 's002')], ['11', '0'], 'レポートの行にある 0 は 0 だけ (「出品なし」は添えない)');
  p = (await call('GET', '/sku/s001')).text;
  assert.match(p, /id="ref-fba-when" style="display:block">1\/10 \(木\) 08:05 時点 \(Amazon の朝のレポート\)</);
  assert.ok(!/id="ref-fba-unknown"/.test(p));
  // 本体の関数: 古い = 26 時間より前
  const day = await asEditor(E0, () => F.readFbaDay(db, { now: Date.parse('2030-01-10T23:05:00.000Z') }));
  assert.deepEqual([day.ok, day.date, day.asOf, day.stale, day.newer], [true, '2030-01-10', '2030-01-09T23:05:00.000Z', false, null]);
  assert.equal((await asEditor(E0, () => F.readFbaDay(db, { now: Date.parse('2030-01-11T01:05:01.000Z') }))).stale, true, '26 時間を 1 秒過ぎた = 古い');
  // 最新の日が missing (取れなかった) = 前の complete の日の値・そのことを出す (#1625 Codex R1 Low)。complete より古い missing の日 (1/8) は案内しない
  await ingestStockDay(db, { source: 'fba_jp', snapshot_date: '2030-01-08', missing: true }, { todayJst: '2030-01-12', now: Date.parse('2030-01-12T03:00:00Z') });
  await ingestStockDay(db, { source: 'fba_jp', snapshot_date: '2030-01-11', missing: true }, { todayJst: '2030-01-12', now: Date.parse('2030-01-12T03:00:00Z') });
  const dm = await asEditor(E0, () => F.readFbaDay(db, { now: NOW.getTime() }));
  assert.deepEqual([dm.date, dm.asOf, dm.newer], ['2030-01-10', '2030-01-09T23:05:00.000Z', { date: '2030-01-11', status: 'missing', words: '取れなかった' }]);
  r = await call('GET', '/?kind=single');
  assert.equal(fbaCell(r.text, 's001'), '11', 'missing の日は使わない');
  assert.match(r.text, /1\/11 \(金\) は取れなかったので前の日の値/);
  assert.match((await call('GET', '/sku/s001')).text, /id="ref-fba-when" style="display:block">1\/10 \(木\) 08:05 時点 \(Amazon の朝のレポート\) · 1\/11 \(金\) は取れなかったので前の日の値</);
  // 権限: master_edit = 2 つの表を読むだけ (書けない・ほかの在庫の表・mart は読めない・分割を作る関数は CREATE が無いので動かない)
  for (const sql of ['select 1 from snapshots.sku_stock_daily limit 1', 'select 1 from snapshots.stock_capture_days limit 1']) assert.equal(await pgCode(asEditor(E0, () => pg.query(sql))), 'ok', sql);
  for (const sql of ['update snapshots.sku_stock_daily set qty = qty where false', 'delete from snapshots.stock_capture_days where false',
    "insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status) values ('2030-01-01', 'fba_jp', 'jp', 1, 'missing')",
    'select 1 from snapshots.warehouse_stock_daily limit 1', 'select 1 from snapshots.sku_stock_weekly limit 1', 'select 1 from mart.v_sku_stock limit 1', 'select 1 from ops.ingest_runs limit 1',
    "select snapshots.ensure_month_partitions_for(array['sku_stock_daily'], '2030-01-01', '2030-01-31')"]) {
    assert.equal(await pgCode(asEditor(E0, () => pg.query(sql))), '42501', sql);
  }
  // 権限が無い (流し直しの前の本番) = その欄だけ「読めない」(画面は出る) → create-master-edit-roles.mjs を流し直すと読める
  await pg.query('revoke select on snapshots.sku_stock_daily from master_edit');
  r = await call('GET', '/?kind=single');
  assert.equal(r.status, 200); assert.equal(fbaTh(r.text), '読めない');
  assert.match(r.text, /title="画面のロールに FBA の在庫を読む権限がまだ無い \(create-master-edit-roles\.mjs の流し直しが要る\)"/);
  assert.equal(fbaCell(r.text, 's001'), '—');
  assert.match((await call('GET', '/sku/s001')).text, /id="ref-fba-when">読めない \(画面のロールに FBA の在庫を読む権限がまだ無い/);
  await pg.query('revoke usage on schema snapshots from master_edit');
  assert.equal(fbaTh((await call('GET', '/?kind=single')).text), '読めない', 'schema の usage が無くても例外にしない');
  await createMasterEditRoles(pg, {});
  r = await call('GET', '/?kind=single');
  assert.equal(fbaCell(r.text, 's001'), '11', '流し直した = 読める');
});

await ta('[17] 売れた数 (10/5): 読み元 = 商品管理リストの公開の回 (発注アプリと同じ数)・いつまでの数か (前日まで)・FBA / FBA 以外・モール別・セットは構成品に入る (「—」)・商品管理リストに無い・古い / 読めない・ページの分だけ索引で引く・Company DB の権限は広げない', async () => {
  const S = await import('../apps/master-edit/sales-qty.mjs');
  const { default: Database } = await import('better-sqlite3');
  const salesTh = (html) => /id="th-sales">売れた 7日\/30日<span class="thsub">([^<]*)<\/span>/.exec(html)?.[1];
  const salesCell = (html, code) => colCell(html, code, 'sales');
  // ① 写しが無い (warehouse-mirror.db を開いていない) = 「読めない」(一覧・単品とも画面は出る)
  let r = await call('GET', '/?kind=single');
  assert.equal(r.status, 200);
  assert.equal(salesTh(r.text), '読めない');
  assert.equal(salesCell(r.text, 's001'), '—');
  assert.match((await call('GET', '/sku/s001')).text, /id="ref-sales-when">読めない \(販売数 \(商品管理リスト\) を読めません\)/);
  // ② 写し (Render の warehouse-mirror.db と同じ表・索引の部分)。公開の回 pml_b (1/9 まで)・古い回 pml_a は読まない
  const m = new Database(':memory:');
  m.exec(`create table mirror_pml_published (id integer primary key check (id = 1), run_id text not null, status text not null, as_of_date text, src_velocity_as_of text, synced_at text not null);
    create table mirror_pml_snapshot_rows (run_id text not null, 商品コード text not null, 販売数7日_FBA integer, 販売数7日_FBA以外 integer, 販売数7日_合計 integer,
      販売数30日_FBA integer, 販売数30日_FBA以外 integer, 販売数30日_合計 integer, primary key (run_id, 商品コード));
    create index idx_mpsr_run_code_norm on mirror_pml_snapshot_rows(run_id, LOWER(TRIM(商品コード)));
    create table mirror_f_sales_velocity_by_product_mall (商品コード text not null, mall text not null, qty_7d integer not null default 0, qty_30d integer not null default 0, as_of_date text not null, synced_at text not null, primary key (商品コード, mall));
    create table dim_mall (mall_key text primary key, label text not null, display_order integer not null);`);
  const sql = [];
  // swapAfterPointer = 次に公開の回のポインタを読んだ直後に 1 回だけ動かす (同期の切り替え = 古い回の明細を消してポインタを差し替える を、読みの間に挟む)
  let swapAfterPointer = null;
  S.__setSalesMirrorProvider(() => ({
    prepare: (t) => {
      sql.push(t);
      const st = m.prepare(t);
      if (!/from mirror_pml_published/.test(t)) return st;
      return { get: (...a) => { const r = st.get(...a); if (swapAfterPointer) { const f = swapAfterPointer; swapAfterPointer = null; f(); } return r; } };
    },
    transaction: (fn) => m.transaction(fn),
  }));
  try {
    r = await call('GET', '/?kind=single');
    assert.equal(salesTh(r.text), '読めない', '公開の回がまだ無い');
    assert.match(r.text, /title="商品管理リスト \(販売数\) の写しがまだありません"/);
    m.prepare(`insert into mirror_pml_published (id, run_id, status, as_of_date, src_velocity_as_of, synced_at) values (1, 'pml_b', 'ok', '2030-01-10', '2030-01-09', 'x')`).run();
    const ins = m.prepare('insert into mirror_pml_snapshot_rows values (?, ?, ?, ?, ?, ?, ?, ?)');
    ins.run('pml_a', 's001', 99, 99, 198, 99, 99, 198);   // 前の回 = 読まない
    ins.run('pml_b', 'S001', 5, 7, 12, 20, 28, 48);        // 大文字 (NE のコードの書き方) でも当たる
    ins.run('pml_b', ' s002 ', 0, 0, 0, 0, 0, 0);
    ins.run('pml_b', 'set001', 1, 1, 2, 2, 3, 5);           // 構成の欠けたセットは上流がセットのコードに数えることがある = 値があっても出さない (#1627 Codex R1 M2)
    const mi = m.prepare('insert into mirror_f_sales_velocity_by_product_mall values (?, ?, ?, ?, ?, ?)');
    mi.run('s001', 'amazon_fba', 5, 20, '2030-01-09', 'x'); mi.run('s001', 'rakuten', 4, 18, '2030-01-09', 'x'); mi.run('S001', 'wholesale', 3, 10, '2030-01-09', 'x');
    for (const [k, l, o] of [['amazon_fba', 'Amazon FBA', 110], ['rakuten', '楽天', 20], ['wholesale', '卸', 130]]) m.prepare('insert into dim_mall values (?, ?, ?)').run(k, l, o);
    sql.length = 0;
    r = await call('GET', '/?kind=single');
    assert.equal(salesTh(r.text), '1/9 まで', '見出しの下 = いつまでの数か (前日まで = 古くない)');
    assert.match(r.text, /title="売れた数 \(7 日 \/ 30 日\) · 1\/9 \(水\) まで \(7 日 = 1\/3〜・30 日 = 12\/11〜・今日は入れない\) · 参考 · 商品管理リスト・発注アプリと同じ数/);
    assert.match(r.text, /class="n ref" style="width:96px" title="売れた数/);
    assert.equal(salesCell(r.text, 's001'), '12 / 48', '公開の回の 7 日 / 30 日の合計 (前の回 pml_a は読まない)');
    assert.match(r.text, /title="7 日 12 \(FBA 5 · FBA 以外 7\) \/ 30 日 48 \(FBA 20 · FBA 以外 28\)"/);
    assert.equal(salesCell(r.text, 's002'), '0 / 0', '商品管理リストにあって売れていない = 0');
    assert.equal(salesCell(r.text, 's003'), '—', '商品管理リストに無い = 「—」(0 とは分ける)');
    assert.match(r.text, /title="商品管理リストに無い商品 \(NE にまだ無いなど\)">—</);
    {
      // ページの分だけ引く: 公開の回を 1 回 + そのページの商品コードだけ (in の ? の数 = 行の数)・索引を使う
      const hit = sql.filter((t) => /from mirror_pml_snapshot_rows/.test(t));
      const rows = (r.text.match(/<a class="rowlink"/g) || []).length;
      assert.equal(hit.length, 1);
      assert.equal((hit[0].match(/\?/g) || []).length - 1, rows, 'このページの行の数だけ');
      assert.equal(sql.filter((t) => /from mirror_pml_published/.test(t)).length, 2, '公開の回を読む (見出し) + 明細と同じ取引で読み直す');
      const plan = m.prepare(`explain query plan ${hit[0]}`).all('pml_b', ...Array.from({ length: rows }, (_, i) => `x${i}`)).map((x) => x.detail).join(' / ');
      assert.match(plan, /USING INDEX idx_mpsr_run_code_norm/, plan);
    }
    r = await call('GET', '/?kind=set');
    assert.equal(salesCell(r.text, 'set001'), '—', 'セットは値 (2 / 5) があっても「—」');
    assert.match(r.text, /title="セットで売れた分は構成品の数に入る \(商品管理リスト・発注アプリと同じ数え方\)">—</);
    {
      // CSV もセットの売れた数は空・単品は数
      const csv = await (await fetch(BASE + '/list.csv?codes=set001%0As001', { headers: { 'x-test-session': 'editor' } })).text();
      const rows = csv.replace(/^\ufeff/, '').trim().split('\r\n').map((line) => [...line.matchAll(/"((?:[^"]|"")*)"(?:,|$)/g)].map((x) => x[1]));
      const i7 = rows[0].indexOf('売れた 7 日 (1/9 まで)'); const i30 = rows[0].indexOf('売れた 30 日 (1/9 まで)');
      assert.ok(i7 > 0 && i30 > 0, rows[0].join('|'));
      const byCode = Object.fromEntries(rows.slice(1).map((x) => [x[0], [x[i7], x[i30]]]));
      assert.deepEqual(byCode, { s001: ['12', '48'], set001: ['', ''] });
    }
    // 単品の画面: 7 日 / 30 日・いつまで・FBA / FBA 以外・モール別 (多い順・名前は dim_mall)・この商品を含むセットの分も入っている
    {
      const ps = (await call('GET', '/sku/set001')).text;
      assert.match(ps, /id="ref-sales-set" style="display:block">セットで売れた分は構成品の数に入る/);
      assert.ok(!/id="ref-sales"/.test(ps) && !/id="ref-sales-parts"/.test(ps), 'セットの画面は値 (2 / 5) があっても数と内訳を出さない');
    }
    let p = (await call('GET', '/sku/s001')).text;
    assert.match(p, /<h2 id="h-ref">在庫・売れた数・注文残<\/h2>/);
    assert.match(p, /id="ref-sales">12 \/ 48</);
    assert.match(p, /id="ref-sales-asof" style="display:block">1\/9 \(水\) まで \(7 日 = 1\/3〜・30 日 = 12\/11〜・今日は入れない\) · 注文日・キャンセルを除く・全部のモール</);
    const flat = (id) => new RegExp(`<table id="${id}">([\\s\\S]*?)<\\/table>`).exec(p)[1].replace(/\s*<[^>]+>\s*/g, '|').replace(/\|+/g, '|');
    assert.equal(flat('ref-sales-parts'), '|売れた数の内訳|7 日|30 日|Amazon FBA|5|20|FBA 以外 (NE の受注)|7|28|');
    assert.equal(flat('ref-sales-malls'), '|モール別|7 日|30 日|Amazon FBA|5|20|楽天|4|18|卸|3|10|');
    assert.match(p, /id="ref-sales-sets">この商品を含むセット \d+ 件で売れた分も入っている \(セット経由の分だけは分けられない\)/);
    assert.ok(!/id="ref-sales-malls-lag"/.test(p));
    p = (await call('GET', '/sku/set001')).text;
    assert.match(p, /id="ref-sales-set" style="display:block">セットで売れた分は構成品の数に入る/);
    assert.ok(!/id="ref-sales-parts"/.test(p));
    p = (await call('GET', '/sku/s003')).text;
    assert.match(p, /id="ref-sales-missing" style="display:block">商品管理リストに無い商品/);
    assert.ok(!/id="ref-sales-parts"/.test(p));
    // モール別だけ前の朝のまま (FBA と NE が重なった朝は作り直さない) = モール別の日を出す
    m.prepare(`update mirror_f_sales_velocity_by_product_mall set as_of_date = '2030-01-08'`).run();
    p = (await call('GET', '/sku/s001')).text;
    assert.match(p, /<th scope="col">モール別 \(1\/8 まで\)<\/th>/);
    assert.match(p, /id="ref-sales-malls-lag">モール別は 1\/8 \(火\) までの数 \(合計とずれることがある\)/);
    // 古い: 前日までになっていない (正午から。正午までは前々日まで待つ = 朝の取込の前)
    m.prepare(`update mirror_pml_published set src_velocity_as_of = '2030-01-08'`).run();
    r = await call('GET', '/?kind=single');
    assert.equal(salesTh(r.text), '古い 1/8 まで');
    assert.match(r.text, /class="n ref stale" style="width:96px"/);
    assert.match(r.text, /\(前日までの数になっていない = 古い\)/);
    assert.match((await call('GET', '/sku/s001')).text, /id="ref-sales-asof" style="display:block">古い · 1\/8 \(火\) まで/);
    assert.equal(S.salesStale('2030-01-08', Date.parse('2030-01-10T02:59:59Z')), false, '1/10 11:59 = 朝の取込の再試行の間 = まだ古くない');
    assert.equal(S.salesStale('2030-01-08', Date.parse('2030-01-10T03:00:00Z')), true, '1/10 12:00 = 古い');
    assert.equal(S.salesStale('2030-01-07', Date.parse('2030-01-09T15:00:00Z')), true, '前々々日まで = 夜中でも古い');
    assert.equal(S.salesStale('2030-01-09', Date.parse('2030-01-10T14:59:59Z')), false);
    // 同期の切り替え (古い回 pml_b の明細を消してポインタを pml_c へ) を、見出しの読みと明細の読みの間に挟む (#1627 Codex R1 M1):
    //   明細は新しい回で読み、見出しの日付も新しい回に合わせる (古い回のまま「商品管理リストに無い」にしない)
    m.prepare(`update mirror_pml_published set src_velocity_as_of = '2030-01-09'`).run();
    swapAfterPointer = () => m.transaction(() => {
      m.prepare('insert into mirror_pml_snapshot_rows values (?, ?, ?, ?, ?, ?, ?, ?)').run('pml_c', 's001', 10, 20, 30, 40, 50, 90);
      m.prepare(`delete from mirror_pml_snapshot_rows where run_id = 'pml_b'`).run();
      m.prepare(`update mirror_pml_published set run_id = 'pml_c', src_velocity_as_of = '2030-01-10' where id = 1`).run();
    })();
    r = await call('GET', '/?kind=single');
    assert.equal(swapAfterPointer, null, '切り替えを挟んだ');
    assert.equal(salesCell(r.text, 's001'), '30 / 90', '新しい回の数');
    assert.equal(salesTh(r.text), '1/10 まで', '見出しの日付も新しい回');
    assert.equal(salesCell(r.text, 's002'), '—', '新しい回に無い = 無い');
    // 単品の画面も同じ (見出しの後に切り替わった)
    swapAfterPointer = () => m.transaction(() => {
      m.prepare('insert into mirror_pml_snapshot_rows values (?, ?, ?, ?, ?, ?, ?, ?)').run('pml_d', 's001', 1, 2, 3, 4, 5, 9);
      m.prepare(`delete from mirror_pml_snapshot_rows where run_id = 'pml_c'`).run();
      m.prepare(`update mirror_pml_published set run_id = 'pml_d' where id = 1`).run();
    })();
    p = (await call('GET', '/sku/s001')).text;
    assert.match(p, /id="ref-sales">3 \/ 9</);
    // 日付の無い回 = 読めない (いつまでの数か分からない数は出さない)
    m.prepare(`update mirror_pml_published set src_velocity_as_of = null`).run();
    r = await call('GET', '/?kind=single');
    assert.equal(salesTh(r.text), '読めない'); assert.equal(salesCell(r.text, 's001'), '—');
    // 表が無い (写しの作りが古い) = 読めない (例外にしない)
    m.exec('drop table mirror_pml_snapshot_rows');
    m.prepare(`update mirror_pml_published set src_velocity_as_of = '2030-01-09'`).run();
    r = await call('GET', '/?kind=single');
    assert.equal(r.status, 200); assert.equal(salesCell(r.text, 's001'), '—');
    assert.equal(salesTh(r.text), '読めない', '明細が読めない = 見出しも「読めない」(日付を出したまま全部「—」にしない・#1627 Codex R2 M2)');
    assert.match(r.text, /id="th-sales" title="販売数 \(商品管理リスト\) を読めません"|title="販売数 \(商品管理リスト\) を読めません"[^>]*id="th-sales"/);
    {
      // 関数: { ok: false, error } を返し、同じ run (見出し) も「読めない」に直す
      const run = await S.readSalesRun({ now: NOW.getTime() });
      assert.equal(run.ok, true);
      const got = await S.salesOfCodes(run, ['s001']);
      assert.deepEqual([got.ok, got.error, run.ok, run.error], [false, '販売数 (商品管理リスト) を読めません', false, '販売数 (商品管理リスト) を読めません']);
    }
    assert.match((await call('GET', '/sku/s001')).text, /id="ref-sales-when">読めない/);
  } finally { S.__setSalesMirrorProvider(null); m.close(); }
  // Company DB の権限は広げない (売れた数は Render の写しから読む): master_edit は mart の売上の日次・注文の表を読めない
  for (const t of ['select 1 from mart.sales_daily limit 1', 'select 1 from mart.v_sales_daily limit 1', 'select 1 from core.orders limit 1', 'select 1 from core.order_lines limit 1']) {
    assert.equal(await pgCode(asEditor(E0, () => pg.query(t))), '42501', t);
  }
});

await ta('[18] 一覧の区分の列・コードのコピー・絞った一覧の CSV と全部のコード (10/5): 区分の札と同じ・区分の順・BOM / CRLF / 列 / 式の注入の対策・絞り込み (印 ?s=・並び・offset は無視)・注文残は発注アプリの権限者だけ・件数 / 時間の上限・条件の期限切れ / ロジザードの写しの「古い」(写しの時間の外)', async () => {
  const { __setExportLimits } = await import('../apps/master-edit/router.mjs');
  const X = await import('../apps/master-edit/extras.mjs');
  const T = await import('../apps/master-edit/search-token.mjs');
  const raw = async (url, session = 'editor') => {
    const r = await fetch(BASE + url, { headers: { 'x-test-session': session }, redirect: 'manual' });
    return { status: r.status, type: r.headers.get('content-type'), disp: r.headers.get('content-disposition'), buf: Buffer.from(await r.arrayBuffer()) };
  };
  /** CSV (全部の欄が "…") を表に。BOM と CRLF を確かめてから */
  const table = (got) => {
    assert.equal(got.status, 200, got.buf.toString());
    assert.deepEqual([...got.buf.subarray(0, 3)], [0xef, 0xbb, 0xbf], 'UTF-8 の BOM');
    const text = got.buf.subarray(3).toString('utf8');
    assert.ok(text.endsWith('\r\n'));
    assert.ok(!/[^\r]\n/.test(text), '行の終わりは全部 CRLF');
    return text.slice(0, -2).split('\r\n').map((line) => [...line.matchAll(/"((?:[^"]|"")*)"(?:,|$)/g)].map((m) => m[1].replace(/""/g, '"')));
  };
  const listOrder = (html) => [...html.matchAll(/<a class="rowlink" href="sku\/([^"]+)"/g)].map((m) => decodeURIComponent(m[1]));
  // ── 区分の列 (単品 / セット (構成品の数) / 例外)・区分の札で絞った一覧と同じ ──
  let r = await call('GET', '/');
  assert.match(r.text, /<th scope="col" style="width:74px" id="th-kind">区分<\/th><th scope="col" style="width:150px">状態<\/th>/, '区分は名前の後・状態の前');
  assert.equal(colCell(r.text, 's001', 'kind'), '単品');
  assert.match(colCell(r.text, 'set001', 'kind'), /^セット \d+ 品$/);
  const allCodes = listOrder(r.text);
  for (const [kind, label] of [['single', '単品'], ['set', 'セット']]) {
    const h = (await call('GET', `/?kind=${kind}`)).text;
    const codes = listOrder(h);
    assert.ok(codes.length > 0);
    for (const c of codes) assert.ok(colCell(h, c, 'kind').startsWith(label), `${kind}: ${c}`);
  }
  // 区分の順 (単品 → セット → 例外・同じ区分はコード順)
  r = await call('GET', '/?sort=kind');
  const byKind = listOrder(r.text);
  assert.deepEqual([...byKind].sort(), [...allCodes].sort(), '同じ商品・並びだけ違う');
  const kinds = byKind.map((c) => colCell(r.text, c, 'kind').split(' ')[0]);
  const rank = { 単品: 0, セット: 1, 例外: 2 };
  for (let i = 1; i < kinds.length; i++) assert.ok(rank[kinds[i - 1]] < rank[kinds[i]] || (rank[kinds[i - 1]] === rank[kinds[i]] && byKind[i - 1] < byKind[i]), `${byKind[i - 1]} → ${byKind[i]}`);
  assert.match(r.text, /<b aria-current="true">区分の順 \(単品・セット・例外\)<\/b>/);
  // コードのコピーのボタン (行ごと・読み上げの名前)・全部コピー・CSV のリンク (今の絞り込みのまま。offset は付けない)
  r = await call('GET', '/?kind=single&sort=kind&offset=0');
  assert.match(r.text, /<a class="rowlink" href="sku\/s001">s001<\/a><button type="button" class="copybtn" data-copy="s001" aria-label="商品コード s001 をコピー" title="コピー">/);
  const nSingle = listOrder(r.text).length;
  assert.match(r.text, new RegExp(`<button type="button" class="btn sm ghost" id="copy-all" data-url="api/codes\\?kind=single&amp;sort=kind" data-n="${nSingle}" title="[^"]+"><svg[^>]*><use href="#i-copy"/></svg>コードを全部コピー \\(${nSingle} 件\\)</button>`));
  assert.match(r.text, new RegExp(`<a class="btn sm ghost" id="csv-link" href="list.csv\\?kind=single&amp;sort=kind" download title="[^"]+"><svg[^>]*><use href="#i-download"/></svg>CSV \\(${nSingle} 件\\)</a>`));
  // ── 全部のコード (コピーのボタンが読む): 一覧と同じ並び・ページに関係なく全件 ──
  let j = (await call('GET', '/api/codes?kind=single&sort=kind')).j;
  assert.deepEqual([j.ok, j.total, j.codes], [true, nSingle, listOrder(r.text)]);
  // ── CSV ──
  let got = await raw('/list.csv?kind=single&offset=5');
  assert.equal(got.type, 'text/csv; charset=utf-8');
  assert.equal(got.disp, `attachment; filename="master-list_20300110-1200.csv"; filename*=UTF-8''master-list_20300110-1200.csv`, 'ファイル名に日時 (日本の 1/10 12:00)');
  let t = table(got);
  assert.deepEqual(t[0], ['商品コード', '区分', '構成品の数', '名前', '状態', '登録の状態', '登録日', '売価', '原価', '原価は構成品から計算', '税率 (%)', '売上分類', '代表の仕入先コード', '代表の仕入先', 'JAN', '送料コード', '推奨月数',
    '在庫 (ロジザード 読めない)', 'FBA (JP) 販売可能 (1/10 08:05 時点)', '売れた 7 日 (読めない)', '売れた 30 日 (読めない)', '対応が必要'], '注文残の列は発注アプリの権限が無い人には無い・読めない参考の値は見出しに「読めない」');
  assert.deepEqual(t.slice(1).map((x) => x[0]), listOrder((await call('GET', '/?kind=single')).text), 'offset に関係なく絞った全件・一覧と同じ並び');
  const row = (code) => Object.fromEntries(t[0].map((h, i) => [h, t.find((x) => x[0] === code)[i]]));
  {
    const s1 = row('s001');
    assert.equal(s1['区分'], '単品'); assert.equal(s1['構成品の数'], ''); assert.equal(s1['状態'], '取扱中');
    assert.equal(s1['代表の仕入先コード'], '0001'); assert.equal(s1['代表の仕入先'], 'AMC');
    assert.equal(s1['在庫 (ロジザード 読めない)'], '', '読めない参考の値 = 空');
    assert.equal(s1['FBA (JP) 販売可能 (1/10 08:05 時点)'], '11', '画面の一覧と同じ FBA の数');
    const jan = (await q(`select string_agg(external_value, ' ' order by external_value) as j from core.external_ids e join core.skus k on k.product_id = e.entity_id
      where e.entity_type = 'product' and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null and k.code = 's001'`))[0].j || '';
    assert.equal(s1['JAN'], jan, 'JAN = 今の JAN (終わっていない)');
  }
  // 画面の一覧と同じ値 (売価・原価・税率・送料・推奨月数)
  {
    const s2 = row('s002');
    const db2 = (await q("select standard_price_jpy::int as p, tax_rate::float8 as t, shipping_code as sc, reorder_months::float8 as m from core.skus where code = 's002'"))[0];
    assert.deepEqual([s2['売価'], s2['税率 (%)'], s2['送料コード'], s2['推奨月数']], [String(db2.p), String(Math.round(db2.t * 100)), db2.sc || '', db2.m == null ? '' : String(db2.m)]);
  }
  // セットの行: 区分・構成品の数・作れる数の列 (読めない = 空)
  t = table(await raw('/list.csv?kind=set'));
  {
    const st = Object.fromEntries(t[0].map((h, i) => [h, t.find((x) => x[0] === 'set001')[i]]));
    assert.equal(st['区分'], 'セット'); assert.match(st['構成品の数'], /^\d+$/);
    assert.ok(t.slice(1).every((x) => x[1] === 'セット'), '区分の札で絞った CSV は全部セット');
  }
  // 式の注入の対策: 先頭 (空白・制御文字の後) が = + - @ の文字には ' を付ける。数の欄は数のまま
  const keepName = (await q("select name from core.skus where code = 's006'"))[0].name;
  await pg.query(`update core.skus set name = ' =HYPERLINK("http://x","y")' where code = 's006'`);
  try {
    t = table(await raw('/list.csv?kind=single'));
    const s6 = Object.fromEntries(t[0].map((h, i) => [h, t.find((x) => x[0] === 's006')[i]]));
    assert.equal(s6['名前'], `' =HYPERLINK("http://x","y")`);
  } finally { await pg.query('update core.skus set name = $1 where code = $2', [keepName, 's006']); }
  // 注文残 = 発注アプリの利用権がある人だけ (admin = 全部のアプリ)
  t = table(await raw('/list.csv?kind=single', 'admin'));
  assert.ok(t[0].includes('注文残 (発注アプリ 読めない)') || t[0].includes('注文残 (発注アプリ)'), t[0].join('|'));
  assert.equal(t[0].indexOf('対応が必要'), t[0].length - 1);
  // 詳細検索の印 (?s=) でも出せる・並び (区分の順) も一覧と同じ
  const sr = await call('POST', '/api/search', { body: { codes: 'set001\ns002\ns001', sort: 'kind' } });
  assert.equal(sr.status, 200, sr.text);
  const qs = sr.j.url.split('?')[1];
  assert.match(qs, /(^|&)s=/);
  t = table(await raw(`/list.csv?${qs}`));
  assert.deepEqual(t.slice(1).map((x) => x[0]), ['s001', 's002', 'set001'], '印の中身 (コード 3 つ)・区分の順');
  j = (await call('GET', `/api/codes?${qs}`)).j;
  assert.deepEqual(j.codes, ['s001', 's002', 'set001']);
  // 条件の期限切れ (印が消えた) = 410 (全件を出さない)
  T.__clearSearchTokens();
  got = await raw(`/list.csv?${qs}`);
  assert.equal(got.status, 410); assert.match(got.buf.toString(), /条件の期限が切れました/);
  assert.equal((await call('GET', `/api/codes?${qs}`)).status, 410);
  // 件数の上限: 超えたら 413 (途中で切った CSV を出さない)・一覧のボタンは押せない
  __setExportLimits({ max: 2 });
  try {
    got = await raw('/list.csv?kind=single');
    assert.equal(got.status, 413); assert.match(got.buf.toString(), /2 件より多くあります。CSV は 2 件までです。絞ってから出してください/, '上限 + 1 件まで読んで止める = 全部は数えない');
    r = await call('GET', '/api/codes?kind=single');
    assert.equal(r.status, 413); assert.match(r.j.error, /全部コピーは 2 件まで/);
    r = await call('GET', '/?kind=single');
    assert.match(r.text, /id="copy-all" data-url="api\/codes\?kind=single" data-n="\d+" disabled/);
    assert.match(r.text, /<span class="btn sm ghost" id="csv-link" aria-disabled="true"/);
    assert.equal(table(await raw('/list.csv?codes=s001%0As002')).length, 3, '2 件までは出せる');
  } finally { __setExportLimits(null); }
  // 時間の上限 (段ごとに確かめる) = 503
  __setExportLimits({ timeMs: 0 });
  try {
    got = await raw('/list.csv');
    assert.equal(got.status, 503); assert.match(got.buf.toString(), /時間がかかりすぎました/);
    const c0 = await call('GET', '/api/codes');
    assert.deepEqual([c0.status, c0.j.reason], [503, 'timeout'], '処理中の期限切れも reason = timeout (接続の待ちと同じ形)');
  } finally { __setExportLimits(null); }
  assert.equal((await raw('/list.csv')).status, 200);
  // つながらない = 503 (画面と同じ)
  factoryMode = 'down';
  try { assert.equal((await raw('/list.csv')).status, 503); } finally { factoryMode = 'ok'; }
  // ── ロジザードの写しの「古い」(10/5): 写しが動くのは毎日 09〜18 時 = その日の最後 (18 時台) の写しは次の朝 10:00 まで古くない ──
  const at = (jst) => Date.parse(`${jst}+09:00`);
  const c18 = new Date(at('2030-01-10T18:01:00')).toISOString();
  assert.equal(X.stockStale(c18, at('2030-01-10T19:30:00')), false, '2 時間以内');
  assert.equal(X.stockStale(c18, at('2030-01-10T21:30:00')), false, '夜 (前は「古い」と出ていた)');
  assert.equal(X.stockStale(c18, at('2030-01-11T09:59:59')), false, '次の朝 10 時の前');
  assert.equal(X.stockStale(c18, at('2030-01-11T10:00:00')), true, '次の朝 10 時 = 9 時の写しが来ていない');
  assert.equal(X.stockStale(new Date(at('2030-01-10T17:01:00')).toISOString(), at('2030-01-10T20:00:00')), true, '18 時の写しが抜けた夜 = 古い');
  assert.equal(X.stockStale(new Date(at('2030-01-10T10:00:00')).toISOString(), at('2030-01-10T11:59:00')), false);
  assert.equal(X.stockStale(new Date(at('2030-01-10T10:00:00')).toISOString(), at('2030-01-10T12:00:01')), true, '日中は 2 時間');
  assert.equal(X.stockStale('壊れた時刻', at('2030-01-10T12:00:00')), true);
});

await ta('[19] 大きめの見本 (#1627 Codex R1 M3 / Low): CSV・全部コピーは SQL で上限 + 1 件までしか読まない (中身の段に進まない)・JS で絞る条件のとき・期限は参考の値と販売数の読みにも効く・含むセット 51 件', async () => {
  const { __setExportLimits } = await import('../apps/master-edit/router.mjs');
  const S = await import('../apps/master-edit/sales-qty.mjs');
  const { ListTimeoutError } = await import('../apps/master-edit/deadline.mjs');
  const { default: Database } = await import('better-sqlite3');
  // 例外の SKU 1,500 件 + s003 を含むセット 51 件。登録の状態は同じ取引で関数 (ops.create_sku_registration) が作る (= commit の登録の確かめを通る)
  await pg.query('begin');
  try {
    await pg.query(`insert into core.skus (company_id, sku_kind, code, name) select 1, 'exception', 'bulk' || lpad(g::text, 5, '0'), 'まとめ ' || g from generate_series(1, 1500) g`);
    await pg.query(`insert into core.skus (company_id, sku_kind, code, name) select 1, 'set', 'bset' || lpad(g::text, 3, '0'), '含むセット ' || g from generate_series(1, 51) g`);
    await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source)
      select 1, k.sku_id, (select sku_id from core.skus where code = 's003'), 1, 'ne' from core.skus k where k.code like 'bset%'`);
    await pg.query(`select count(ops.create_sku_registration(sku_id, 'test')) from core.skus where code like 'bulk%' or code like 'bset%'`);
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
  const log = [];
  const tdb = { query: async (t, p) => { const r = await db.query(t, p); log.push({ t, n: r.rows.length }); return r; } };
  const counted = (mode, filters, max) => { log.length = 0; return R.listSkus(tdb, filters, { now: NOW, mode, max }); };
  const ROWS2 = /where s\.sku_id = any\(\$1::bigint\[\]\)/;
  for (const mode of ['all', 'codes']) {
    const d = await counted(mode, { kind: 'exception' }, 1000);
    assert.deepEqual([d.tooMany, d.atLeast, d.total, d.max], [true, true, 1000, 1000], mode);
    assert.ok(Math.max(...log.map((x) => x.n)) <= 1001, `${mode}: 1 つの問い合わせで 1,001 行より多く読まない (${Math.max(...log.map((x) => x.n))})`);
    assert.ok(log.some((x) => / limit 1001$/.test(x.t.trim())), '① の SQL に limit 上限 + 1');
    assert.ok(!log.some((x) => ROWS2.test(x.t)), `${mode}: 中身の段 (②) に進まない`);
  }
  // 上限の中 = 全件を読む (CSV の行 = 1,500 + 見出し)
  {
    const d = await counted('all', { kind: 'exception' }, 2000);
    assert.equal(d.rows.length, 1500); assert.ok(!d.tooMany);
    __setExportLimits({ max: 2000 });
    try {
      const t0 = Date.now();
      const csv = await (await fetch(BASE + '/list.csv?kind=exception', { headers: { 'x-test-session': 'editor' } })).text();
      assert.equal(csv.trim().split('\r\n').length, 1501);
      console.log(`      (CSV 1,500 件 = ${Date.now() - t0} ms・PGlite)`);
      const j = (await call('GET', '/api/codes?kind=exception')).j;
      assert.equal(j.codes.length, 1500); assert.equal(j.codes[0], 'bulk00001');
    } finally { __setExportLimits(null); }
  }
  // JS で絞る条件 (売上分類) = SQL では絞れない = 読みの上限は EXPORT_SCAN_MAX。絞った後の数で上限を見る
  {
    const d = await counted('all', { sales: '3' }, 1);
    assert.deepEqual([d.tooMany, d.atLeast], [true, false]);
    assert.ok(log.some((x) => new RegExp(` limit ${R.EXPORT_SCAN_MAX + 1}$`).test(x.t.trim())));
    assert.ok(!log.some((x) => ROWS2.test(x.t)));
  }
  // HTTP: 413 の言い方 (上限より多く)
  __setExportLimits({ max: 1000 });
  try {
    const got = await fetch(BASE + '/list.csv?kind=exception', { headers: { 'x-test-session': 'editor' } });
    assert.equal(got.status, 413); assert.match(await got.text(), /1,000 件より多くあります。CSV は 1,000 件までです/);
    const c = await call('GET', '/api/codes?kind=exception');
    assert.equal(c.status, 413); assert.equal(c.j.atLeast, true);
  } finally { __setExportLimits(null); }
  // 期限はリクエストの始めから = 参考の値 (売れた数の写し) の読みが遅いだけでも 503。販売数の読みの塊の間でも止まる
  const m = new Database(':memory:');
  m.exec(`create table mirror_pml_published (id integer primary key, run_id text, status text, as_of_date text, src_velocity_as_of text, synced_at text);
    create table mirror_pml_snapshot_rows (run_id text not null, 商品コード text not null, 販売数7日_FBA integer, 販売数7日_FBA以外 integer, 販売数7日_合計 integer, 販売数30日_FBA integer, 販売数30日_FBA以外 integer, 販売数30日_合計 integer, primary key (run_id, 商品コード));
    insert into mirror_pml_published values (1, 'r1', 'ok', '2030-01-10', '2030-01-09', 'x');`);
  S.__setSalesMirrorProvider(async () => { await new Promise((ok) => setTimeout(ok, 300)); return m; });
  __setExportLimits({ timeMs: 150 });
  try {
    const got = await fetch(BASE + '/list.csv?kind=single', { headers: { 'x-test-session': 'editor' } });
    assert.equal(got.status, 503); assert.match(await got.text(), /時間がかかりすぎました/);
  } finally { __setExportLimits(null); S.__setSalesMirrorProvider(null); }
  S.__setSalesMirrorProvider(() => m);
  try {
    const run = await S.readSalesRun({ now: NOW.getTime() });
    await assert.rejects(S.salesOfCodes(run, ['a', 'b'], { deadline: Date.now() - 1 }), ListTimeoutError);
    const okRes = await S.salesOfCodes(run, ['a'], { deadline: Date.now() + 60e3 });
    assert.deepEqual([okRes.ok, [...okRes.map.keys()]], [true, []], '期限の中 = 読む');
  } finally { S.__setSalesMirrorProvider(null); m.close(); }
  // 接続の待ちも期限に入る (#1627 Codex R2 M1): 期限はルートに入った直後から・残りの時間を connectionTimeoutMillis に渡す・遅れて来た接続は閉じる
  {
    const seen = [];
    let closedLate = 0;
    __setPgClientFactory(async (url, extra) => {
      seen.push(extra);
      await new Promise((ok) => setTimeout(ok, 400));
      const c = await pgFactory(url, extra);
      return { ...c, end: async () => { closedLate++; await c.end(); } };
    });
    __setExportLimits({ timeMs: 150 });
    try {
      let t0 = Date.now();
      const got = await fetch(BASE + '/list.csv?kind=single', { headers: { 'x-test-session': 'editor' } });
      const took = Date.now() - t0;
      assert.equal(got.status, 503); assert.match(await got.text(), /時間がかかりすぎました \(Company DB への接続を待つ間に期限を過ぎた\)/);
      assert.ok(took < 380, `接続 (400ms) を待たずに期限で返す (${took}ms)`);
      assert.ok(seen[0] && seen[0].connectionTimeoutMillis > 0 && seen[0].connectionTimeoutMillis <= 150, JSON.stringify(seen[0]));
      t0 = Date.now();
      const c = await call('GET', '/api/codes?kind=single');
      assert.equal(c.status, 503); assert.equal(c.j.reason, 'timeout');
      assert.ok(Date.now() - t0 < 380);
      await new Promise((ok) => setTimeout(ok, 600));   // 遅れて来た接続が閉じるのを待つ (共有の PGlite のロールを戻す)
      assert.equal(closedLate, 2, '遅れて来た接続は閉じる');
    } finally { __setExportLimits(null); __setPgClientFactory(pgFactory); }
    // 期限の無い画面 (一覧) は接続の時間の上限を付けない
    seen.length = 0;
    __setPgClientFactory(async (url, extra) => { seen.push(extra); return pgFactory(url, extra); });
    try {
      assert.equal((await call('GET', '/?kind=single')).status, 200);
      assert.equal(seen[0].connectionTimeoutMillis, undefined);
      assert.equal((await fetch(BASE + '/list.csv?codes=s001', { headers: { 'x-test-session': 'editor' } })).status, 200);
      assert.ok(seen.at(-1).connectionTimeoutMillis > 40e3, 'CSV = 残りの時間 (45 秒から)');
    } finally { __setPgClientFactory(pgFactory); }
  }
  // 実処理の SQL が遅い・statement_timeout で止まった (#1627 Codex R3 M1): 各 SQL の statement_timeout = min(20 秒, 残り)・57014 = 503 (reason timeout)
  {
    const sets = []; let mode = 'slow';
    const LIST1 = /from core\.skus s\s+left join core\.products p/;
    __setPgClientFactory(async (url, extra) => {
      const c = await pgFactory(url, extra);
      return { ...c, query: async (t, p) => {
        if (/^set statement_timeout/.test(t)) sets.push(t);
        if (LIST1.test(t) && /order by/.test(t)) {
          if (mode === 'slow') await new Promise((ok) => setTimeout(ok, 400));   // ① の SQL が遅い (PGlite は止めないので JS で遅らせる)
          if (mode === '57014') { const e = new Error('canceling statement due to statement timeout'); e.code = '57014'; throw e; }
        }
        return c.query(t, p);
      } };
    });
    __setExportLimits({ timeMs: 300 });
    try {
      let got = await fetch(BASE + '/list.csv?kind=single', { headers: { 'x-test-session': 'editor' } });
      assert.equal(got.status, 503); assert.match(await got.text(), /時間がかかりすぎました/);
      const ms = sets.filter((t) => /'\d+ms'/.test(t)).map((t) => Number(/'(\d+)ms'/.exec(t)[1]));
      assert.ok(ms.length && ms.every((x) => x > 0 && x <= 300), `statement_timeout = 残りの時間 (${sets.join(' / ')})`);
      let c = await call('GET', '/api/codes?kind=single');
      assert.deepEqual([c.status, c.j.reason], [503, 'timeout']);
      // 57014 (SQL が statement_timeout で止まった) = 500 にせず 503
      mode = '57014';
      __setExportLimits({ timeMs: 45e3 });
      got = await fetch(BASE + '/list.csv?kind=single', { headers: { 'x-test-session': 'editor' } });
      assert.equal(got.status, 503, '57014 = 503'); assert.match(await got.text(), /時間がかかりすぎました/);
      c = await call('GET', '/api/codes?kind=single');
      assert.deepEqual([c.status, c.j.reason], [503, 'timeout']);
      // 一覧の画面は今のまま (期限の db を使わない = statement_timeout は接続の 20s だけ)
      mode = 'none'; sets.length = 0;
      assert.equal((await call('GET', '/?kind=single')).status, 200);
      assert.deepEqual(sets, ["set statement_timeout = '20s'"]);
    } finally { __setExportLimits(null); __setPgClientFactory(pgFactory); await new Promise((ok) => setTimeout(ok, 50)); }
  }
  // 含むセットが 51 件 = 51 件と数える (表示は 50 件まで)
  const pg3 = (await call('GET', '/sku/s003')).text;
  const nSets = Number((await q("select count(*)::int as n from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id where k.code = 's003'"))[0].n);
  assert.ok(nSets >= 51, String(nSets));
  assert.match(pg3, new RegExp(`<span class="k">セット ${nSets} 件 \\(50 件を表示\\)</span>`), '数えた数 (表示の 50 件ではない)');
  assert.equal((pg3.match(/class="linkchip" href="[^"]*\/sku\/[^"]*" title=/g) || []).length, 50, '表示は 50 件まで');
});

server.close();
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
