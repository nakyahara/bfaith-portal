/**
 * test-sku-registered-on.mjs — 商品の登録日 (0057・lib/sku-registered-on.mjs・夜間ロード・マスタの入力画面)。2026-10-05 中原さんの要望
 *
 * Company DB = PGlite (持ち主のロール deploy で migration・画面のロール master_edit)。夜間ロードの材料 = 一時の warehouse-mirror.db。画面 = 本物の router を HTTP 越しに。
 * 固定する契約:
 *   P NE の作成日の読み方: 'YYYY-MM-DD HH:MM:SS' / 'YYYY/M/D …' / 日付だけ → 'YYYY-MM-DD'。無い日付・2000 年より前・未来・形の違う文字 = null
 *   M 0057 の前の DB: 夜間ロードは止まらない (見送りの印)・画面は登録日を出さずにコード順
 *   D DDL: 既にある行は空のまま (0057 の前のポータルの登録は B)・列の既定値は無い (列を書かない INSERT = 古い夜間ロードの形 = 空。#1617 Codex R1 Medium)・出どころは 3 つだけ・日付と出どころは両方空か両方あり・2000 年より前は入らない・
 *      一度入った登録日 (と出どころ) は変えられない (空にもできない)・空 → 値 は通る・ほかの列の UPDATE は通る・画面のロールは登録日を UPDATE できない
 *   L 夜間ロード: 空の行だけ NE の作成日 (商品管理リストの公開 snapshot の 登録日) で埋める・読めない / 未来 / 行の無い商品は空のまま・
 *      0057 の前からあるセットは空のまま・新しいセット = 初めて見た日 (first_seen)・新しい単品 = NE の作成日で作る (無ければ空)・
 *      NE の値が後で変わっても上書きしない・ポータルで登録した商品 (portal) も上書きしない・値が同じ 2 回目は何も変わらない (記録も増えない)・
 *      snapshot が使えない日は触らない (翌晩に埋める)・mirror_products の new_product_launch_date (人が直せる発売日) は使わない・
 *      セットの作成日 (NE の set_goods_creation_date) が snapshot に来た晩は、空のセットも ne で埋める・新しいセットも ne で作る (L7)
 *   F 登録の関数 (ops.register_new_sku) の 0057 の作り直しは 0052 の本文と「SKU の INSERT に登録日 = v_today・portal を足した」所だけが違う・
 *      security definer・search_path・持ち主・権限は同じ / 古い形の INSERT で空のまま作った単品は、次の夜間ロードが NE の作成日で埋める
 *   B 0057 の前にポータルで登録した SKU (登録の状態の行 origin = new_entry) は 0057 が行を作った日 (JST)・portal で埋める (単品・セット・状態は問わない・backfill は空)・
 *      記録は migration_0057・その後の夜間ロードで ne に変わらない (#1617 Codex R2 Medium)
 *   C 並び × ページ送り × 詳細検索: 登録日の新しい順は一覧の ① の SQL で並べてから 100 件に切る (2 ページ目は続き)・詳細検索の登録日の範囲 (空は入らない・読めない日付は捨てる)・
 *      ページ送り・?s= の印・札のリンク・3 つのフォームが並びを引き継ぐ・登録日と在庫の列が一緒に出る
 *   V 画面: 一覧に登録日の列・「登録日の新しい順」(空は最後・同じ日はコード順)・知らない並びはコード順・単品の画面の見出しに登録日と出どころ / 分からない
 * 使い方: node scripts/test-sku-registered-on.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';
import Database from 'better-sqlite3';
// 広げる道 PR-2: 画面は DB の active に従う。試験の DB は全部の列を company にする = このコードの能力も全部 (code_behind の試験だけ戻す)
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'regdate-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { buildPlanFromRender } = await import('../apps/company-db/load/sources.mjs');
const { runInitialLoad, registeredOnForNew } = await import('../apps/company-db/load/engine.mjs');
const { parseNeCreationDate, listOrderBy, hasRegisteredOn } = await import('../lib/sku-registered-on.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const R = await import('../apps/master-edit/read.mjs');
const { default: router, __setPgClientFactory, __setClock } = await import('../apps/master-edit/router.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const pgCode = async (p) => { try { await p; return 'ok'; } catch (e) { return e.code || e.message; } };

// ── P: NE の作成日の読み方 ──
await ta('[P] NE の作成日の読み方 (形・無い日付・2000 年より前・未来・空)', async () => {
  const today = '2026-10-05';
  assert.equal(parseNeCreationDate('2026-10-03 11:18:17', today), '2026-10-03');
  assert.equal(parseNeCreationDate('2019/12/11 20:12:52', today), '2019-12-11');
  assert.equal(parseNeCreationDate('2024/3/5 9:00:00', today), '2024-03-05');
  assert.equal(parseNeCreationDate('2026-10-05', today), '2026-10-05');            // 今日 = 入れる
  assert.equal(parseNeCreationDate('2026-10-05T01:00:00', today), '2026-10-05');
  assert.equal(parseNeCreationDate('2026-10-06 00:00:01', today), null);           // 未来
  assert.equal(parseNeCreationDate('2026-02-30 00:00:00', today), null);           // 無い日付
  assert.equal(parseNeCreationDate('2026-13-01', today), null);
  assert.equal(parseNeCreationDate('1999-12-31 23:59:59', today), null);           // 2000 年より前 (0057 の CHECK と同じ)
  assert.equal(parseNeCreationDate('20261003', today), null);
  assert.equal(parseNeCreationDate('2026-10-031', today), null);
  for (const v of [null, undefined, '', '   ', '0000-00-00 00:00:00']) assert.equal(parseNeCreationDate(v, today), null, String(v));
  assert.equal(parseNeCreationDate('2099-01-01'), '2099-01-01');                    // today を渡さない = 未来も見ない (呼び手が今日を渡す)
  assert.deepEqual(registeredOnForNew({ kind: 'single', registeredOn: '2026-01-02' }, '2026-10-05'), { registered_on: '2026-01-02', registered_on_source: 'ne' });
  assert.deepEqual(registeredOnForNew({ kind: 'single' }, '2026-10-05'), { registered_on: null, registered_on_source: null });
  assert.deepEqual(registeredOnForNew({ kind: 'set' }, '2026-10-05'), { registered_on: '2026-10-05', registered_on_source: 'first_seen' });
  assert.deepEqual(registeredOnForNew({ kind: 'exception' }, '2026-10-05'), { registered_on: '2026-10-05', registered_on_source: 'first_seen' });
  assert.equal(listOrderBy('reg_desc'), 's.registered_on desc nulls last, s.code_norm');
  assert.equal(listOrderBy('reg_desc', { hasColumn: false }), 's.code_norm');
  assert.equal(listOrderBy('x'), 's.code_norm');
  assert.equal(listOrderBy('kind', { hasColumn: false }), "case s.sku_kind when 'single' then 0 when 'set' then 1 else 2 end, s.code_norm", '区分の順は登録日の列が無くても使える');
  assert.equal(R.normalizeFilters({ sort: 'reg_desc' }).sort, 'reg_desc');
  assert.equal(R.normalizeFilters({ sort: 'constructor' }).sort, '');
  assert.equal(R.normalizeFilters({}).sort, '');
});

// ── 夜間ロードの材料 (Render の warehouse-mirror.db。列名は warehouse-mirror/db.js と同じ) ──
const mirrorPath = path.join(DATA_DIR, 'warehouse-mirror.db');
{
  const m = new Database(mirrorPath);
  m.exec(`
    CREATE TABLE mirror_products (product_id INTEGER PRIMARY KEY, 商品コード TEXT UNIQUE NOT NULL, 商品名 TEXT, 商品区分 TEXT NOT NULL, 取扱区分 TEXT, 標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT NOT NULL, 送料 REAL, 送料コード TEXT, 配送方法 TEXT, 消費税率 REAL, 税区分 TEXT, 在庫数 INTEGER, 引当数 INTEGER, 仕入先コード TEXT, セット構成品数 INTEGER, 売上分類 INTEGER, 代表商品コード TEXT, new_product_launch_date TEXT, updated_at TEXT NOT NULL);
    CREATE TABLE mirror_pml_published (id INTEGER PRIMARY KEY CHECK (id = 1), run_id TEXT NOT NULL, status TEXT NOT NULL, as_of_date TEXT, row_count INTEGER, synced_at TEXT NOT NULL);
    CREATE TABLE mirror_pml_snapshot_rows (run_id TEXT NOT NULL, 商品コード TEXT NOT NULL, 推奨保有月数 REAL, 登録日 TEXT, PRIMARY KEY (run_id, 商品コード));
  `);
  const ins = m.prepare('insert into mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 消費税率, 税区分, new_product_launch_date, updated_at) values (?,?,?,?,?,?,?,?,?)');
  for (const [code, kind, launch] of [
    ['r001', '単品', '2030-01-01'],   // 発売日 (人が直せる) は NE の作成日と違う = 使わない
    ['r002', '単品', null], ['r003', '単品', null], ['r004', '単品', null], ['r005', '単品', null], ['r006', '単品', null], ['rset1', 'セット', null],
  ]) ins.run(code, `商品 ${code}`, kind, '取扱中', 'MISSING', 0.1, 'STANDARD_10', launch, 'x');
  const sn = m.prepare('insert into mirror_pml_snapshot_rows (run_id, 商品コード, 推奨保有月数, 登録日) values (?,?,?,?)');
  for (const [code, reg] of [['r001', '2024-03-15 10:00:00'], ['r002', '2026-10-03 11:18:17'], ['r004', '2026-02-30 00:00:00'], ['r005', '2099-01-01 00:00:00'],
    ['r006', '2019/12/11 20:12:52'], ['rset1', null]]) sn.run('pml_1', code, null, reg);   // r003 は snapshot に行なし
  m.prepare('insert into mirror_pml_published values (1, ?, ?, ?, ?, ?)').run('pml_1', 'ok', '2026-10-05', 6, 'x');
  m.close();
}
const mirrorExec = (sql, params = []) => { const m = new Database(mirrorPath); try { m.prepare(sql).run(...params); } finally { m.close(); } };
const addProduct = (code, kind, reg, { snapshot = true } = {}) => {
  mirrorExec('insert into mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 消費税率, 税区分, updated_at) values (?,?,?,?,?,?,?,?)', [code, `商品 ${code}`, kind, '取扱中', 'MISSING', 0.1, 'STANDARD_10', 'x']);
  if (snapshot) {
    mirrorExec('insert into mirror_pml_snapshot_rows (run_id, 商品コード, 推奨保有月数, 登録日) values (?,?,?,?)', ['pml_1', code, null, reg]);
    mirrorExec('update mirror_pml_published set row_count = row_count + 1');
  }
};

// ── Company DB (持ち主のロール deploy で migration = 本番と同じ) ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
const q = async (sql, params) => (await db.query(sql, params)).rows;
const one = async (sql, params) => (await q(sql, params))[0];
const LOAD_NOW = new Date('2026-10-05T13:00:00Z');   // 夜間ロードの日 (東京 2026-10-05 22:00)
const LOAD_DAY = '2026-10-05';
const plan = (now = LOAD_NOW) => buildPlanFromRender({ dataDir: DATA_DIR, now, log: quiet });
const load = async (runId, now = LOAD_NOW) => { const r = await runInitialLoad(db, plan(now), { log: quiet, runId, host: 'test-host', ownership: MASTER_OWNERSHIP, now }); assert.equal(r.ok, true, r.error); return r; };
const regOf = async (code) => { const r = await one('select registered_on::text as d, registered_on_source as src from core.skus where code = $1', [code]); return r ? [r.d, r.src] : undefined; };
const events = async () => (await one('select count(*)::int as n from events.master_change_events')).n;

await applyMigrations(db, { log: quiet, to: '0056' });

await ta('[M] 0057 の前の DB: 夜間ロードは止まらない (見送りの印)・画面は登録日なしでコード順', async () => {
  assert.equal(await hasRegisteredOn(db), false);
  const r = await load('rl_0');
  assert.ok((r.notes || []).includes('0057 が未適用: 登録日は見送り'), JSON.stringify(r.notes));
  assert.equal((await one('select count(*)::int as n from core.skus')).n, 7);
  const list = await R.listSkus(db, { sort: 'reg_desc' }, { now: LOAD_NOW });
  assert.deepEqual(list.rows.map((x) => x.code), ['r001', 'r002', 'r003', 'r004', 'r005', 'r006', 'rset1']);
  assert.equal(list.registeredOnAvailable, false);
  assert.ok(list.rows.every((x) => x.registered_on === null && x.registered_on_source === null));
  assert.equal((await R.readSkuPage(db, 'r001', { now: LOAD_NOW })).registered, null);
});

await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg, { leases: ['single', 'set'], futureSetLease: true });   // 広げる道 PR-2: 本物の 0058 の上で試験の許可を置く (この DB は構成も C = セットの許可は将来の形)

await ta('[D] DDL: 既にある行は空・列を書かない INSERT (古い夜間ロードの形) = 空・CHECK・一度入ったら変えない・画面のロールは UPDATE できない', async () => {
  assert.equal(await hasRegisteredOn(db), true);
  assert.equal((await one('select count(*)::int as n from core.skus where registered_on is not null or registered_on_source is not null')).n, 0);
  const today = (await one("select core.jst_date(now())::text as d")).d;
  const id = (await one("insert into core.skus (company_id, sku_kind, code, name) values (1, 'exception', 'ddl-1', 'x') returning sku_id")).sku_id;
  assert.deepEqual(await regOf('ddl-1'), [null, null]);   // 既定値なし (#1617 Codex R1 Medium)
  assert.equal((await one("select count(*)::int as n from information_schema.columns where table_schema = 'core' and table_name = 'skus' and column_name like 'registered_on%' and column_default is not null")).n, 0);
  // 明示の空 (夜間ロードの新しい単品) は空のまま
  await q("insert into core.skus (company_id, sku_kind, code, name, registered_on, registered_on_source) values (1, 'exception', 'ddl-2', 'x', null, null)");
  assert.deepEqual(await regOf('ddl-2'), [null, null]);
  assert.equal(await pgCode(q("insert into core.skus (company_id, sku_kind, code, name, registered_on, registered_on_source) values (1, 'exception', 'ddl-3', 'x', '2026-01-01', 'manual')")), '23514');
  assert.equal(await pgCode(q("insert into core.skus (company_id, sku_kind, code, name, registered_on, registered_on_source) values (1, 'exception', 'ddl-3', 'x', null, 'ne')")), '23514');
  assert.equal(await pgCode(q("insert into core.skus (company_id, sku_kind, code, name, registered_on, registered_on_source) values (1, 'exception', 'ddl-3', 'x', '1999-12-31', 'ne')")), '23514');
  // 空 → 値 は通る・値 → 別の値 / 空 / 出どころだけ変える は止まる・ほかの列は直せる
  await q("update core.skus set registered_on = '2025-05-05', registered_on_source = 'ne' where code = 'ddl-2'");
  assert.deepEqual(await regOf('ddl-2'), ['2025-05-05', 'ne']);
  for (const set of ["registered_on = '2025-05-06'", "registered_on = null, registered_on_source = null", "registered_on_source = 'first_seen'"]) {
    const e = await (async () => { try { await q(`update core.skus set ${set} where code = 'ddl-2'`); return null; } catch (x) { return x; } })();
    assert.ok(e && e.code === '23514' && /registered_on_fixed/.test(e.message), `${set}: ${e && e.message}`);
  }
  await q("update core.skus set registered_on = '2025-05-05', name = 'y' where code = 'ddl-2'");   // 同じ値の再代入とほかの列 = 通る
  assert.equal((await one("select name from core.skus where code = 'ddl-2'")).name, 'y');
  // 画面のロール (master_edit) は登録日の列を UPDATE できない (列の権限に無い)
  await pg.query('set role master_edit');
  try { assert.equal(await pgCode(q("update core.skus set registered_on = '2025-01-01', registered_on_source = 'ne' where code = 'ddl-2'")), '42501'); }
  finally { await pg.query('set role deploy'); }
  await q('delete from core.skus where sku_id = $1 or code = $2', [id, 'ddl-2']);
});

await ta('[L1] 0057 の後の最初の夜間ロード: 空の単品を NE の作成日で埋める (読めない・未来・行なしは空)・前からあるセットは空のまま・発売日は使わない', async () => {
  const e0 = await events();
  const r = await load('rl_1');
  assert.deepEqual(await regOf('r001'), ['2024-03-15', 'ne']);   // mirror_products の発売日 2030-01-01 ではない
  assert.deepEqual(await regOf('r002'), ['2026-10-03', 'ne']);
  assert.deepEqual(await regOf('r003'), [null, null]);           // snapshot に行なし
  assert.deepEqual(await regOf('r004'), [null, null]);           // 2/30
  assert.deepEqual(await regOf('r005'), [null, null]);           // 未来
  assert.deepEqual(await regOf('r006'), ['2019-12-11', 'ne']);   // / 区切り
  assert.deepEqual(await regOf('rset1'), [null, null]);          // 0057 の前からあるセット = 分からない
  const note = r.sections.skus.notes.find((x) => /^登録日:/.test(x));
  assert.equal(note, '登録日: 空だった 3 件を NE の作成日で埋めた (NE の作成日あり 3 件・snapshot pml_1・読めない日付 2 件)');
  // 変更の記録 (0026): 埋めた 3 件 × 2 列 = 夜間ロードの UPDATE
  const ev = await q("select attribute, count(*)::int as n from events.master_change_events where run_id = 'rl_1' and entity_type = 'sku' and attribute like 'registered_on%' group by 1 order by 1");
  assert.deepEqual(ev, [{ attribute: 'registered_on', n: 3 }, { attribute: 'registered_on_source', n: 3 }]);
  assert.equal(await events() - e0, 6);
});

await ta('[L2] 値が同じ 2 回目は何も変わらない (記録も増えない)・NE の値が後で変わっても上書きしない', async () => {
  const e0 = await events();
  let r = await load('rl_2');
  assert.equal(await events(), e0);
  assert.ok(r.sections.skus.notes.includes('登録日: 空だった 0 件を NE の作成日で埋めた (NE の作成日あり 3 件・snapshot pml_1・読めない日付 2 件)'), JSON.stringify(r.sections.skus.notes));
  mirrorExec("update mirror_pml_snapshot_rows set 登録日 = '2025-01-01 00:00:00' where 商品コード = 'r001'");
  r = await load('rl_3');
  assert.deepEqual(await regOf('r001'), ['2024-03-15', 'ne']);
  assert.equal(await events(), e0);
  // 読めなかった日付が後で直れば、その晩に埋まる
  mirrorExec("update mirror_pml_snapshot_rows set 登録日 = '2026-02-28 09:00:00' where 商品コード = 'r004'");
  await load('rl_4');
  assert.deepEqual(await regOf('r004'), ['2026-02-28', 'ne']);
});

await ta('[L3] 新しい SKU: 単品は NE の作成日で作る (無ければ空)・セットは初めて見た日 (first_seen)・記録は INSERT 1 行', async () => {
  addProduct('r007', '単品', '2026-10-04 15:00:00');
  addProduct('r008', '単品', null, { snapshot: false });
  addProduct('rset2', 'セット', null);
  await load('rl_5');
  assert.deepEqual(await regOf('r007'), ['2026-10-04', 'ne']);
  assert.deepEqual(await regOf('r008'), [null, null]);
  assert.deepEqual(await regOf('rset2'), [LOAD_DAY, 'first_seen']);
  const ins = await one("select new_value ->> 'registered_on' as d, new_value ->> 'registered_on_source' as src from events.master_change_events where run_id = 'rl_5' and operation = 'INSERT' and entity_type = 'sku' and new_value ->> 'code' = 'r007'");
  assert.deepEqual(ins, { d: '2026-10-04', src: 'ne' });
  assert.equal((await one("select count(*)::int as n from events.master_change_events where run_id = 'rl_5' and operation = 'UPDATE' and attribute like 'registered_on%'")).n, 0);
  // 翌日のロードでも first_seen の日は変わらない
  await load('rl_6', new Date('2026-10-06T13:00:00Z'));
  assert.deepEqual(await regOf('rset2'), [LOAD_DAY, 'first_seen']);
});

await ta('[L4] ポータルで登録した商品 (portal) は、あとで NE に登録されても上書きしない', async () => {
  const today = (await one("select core.jst_date(now())::text as d")).d;
  const pid = (await one("insert into core.products (company_id, display_code, name) values (1, 'r009', '商品 r009') returning product_id")).product_id;
  // 登録の関数と同じ形 (今日 + portal を明示。関数そのものは test-master-register.mjs の R4 が確かめる)
  await q("insert into core.skus (company_id, product_id, sku_kind, code, name, registered_on, registered_on_source) values (1, $1, 'single', 'r009', '商品 r009', core.jst_date(now()), 'portal')", [pid]);
  assert.deepEqual(await regOf('r009'), [today, 'portal']);
  addProduct('r009', '単品', '2020-01-01 00:00:00');
  await load('rl_7');
  assert.deepEqual(await regOf('r009'), [today, 'portal']);
});

await ta('[L5] snapshot が使えない日 (status failed・行数が合わない・登録日の列が無い) は触らない = 新しい単品は空で作り、使える晩に埋める', async () => {
  addProduct('r010', '単品', '2026-10-05 08:00:00');
  mirrorExec("update mirror_pml_published set status = 'failed'");
  let r = await load('rl_8');
  assert.deepEqual(await regOf('r010'), [null, null]);
  assert.ok(r.sections.skus.notes.some((x) => /^登録日: NE の作成日は見送り \(snapshot が使えない \(公開 snapshot の status = failed\)\)$/.test(x)), JSON.stringify(r.sections.skus.notes));
  mirrorExec("update mirror_pml_published set status = 'ok', row_count = row_count + 5");
  r = await load('rl_9');
  assert.deepEqual(await regOf('r010'), [null, null]);
  assert.ok(r.sections.skus.notes.some((x) => /^登録日: NE の作成日は見送り \(snapshot が使えない \(行数が合わない/.test(x)), JSON.stringify(r.sections.skus.notes));
  mirrorExec('update mirror_pml_published set row_count = row_count - 5');
  await load('rl_10');
  assert.deepEqual(await regOf('r010'), ['2026-10-05', 'ne']);
  // 登録日の列が無い snapshot (古い写し) = 見送り
  const p = (() => {
    const m = new Database(mirrorPath);
    try { m.exec('alter table mirror_pml_snapshot_rows rename column 登録日 to 登録日_old'); } finally { m.close(); }
    try { return plan(); } finally { mirrorExec('alter table mirror_pml_snapshot_rows rename column 登録日_old to 登録日'); }
  })();
  assert.deepEqual([p.registered.available, p.registered.reason], [false, 'snapshot に 登録日 の列が無い']);
  assert.ok(p.skus.every((x) => x.registeredOn === undefined));
});

await ta('[L6] 正規化すると同じになる snapshot の行が 2 つ = 使わない (どちらの日か決められない)', async () => {
  addProduct('r011', '単品', '2026-09-01 00:00:00');
  mirrorExec('insert into mirror_pml_snapshot_rows (run_id, 商品コード, 推奨保有月数, 登録日) values (?,?,?,?)', ['pml_1', 'R011', null, '2026-09-02 00:00:00']);
  mirrorExec('update mirror_pml_published set row_count = row_count + 1');
  const p = plan();
  assert.equal(p.skus.find((x) => x.code === 'r011').registeredOn, undefined);
  assert.equal(p.registered.dup, 1);
  await load('rl_11');
  assert.deepEqual(await regOf('r011'), [null, null]);
});

await ta('[F1] 登録の関数の作り直し (0057): 0052 の本文との違いは SKU の INSERT に登録日 (v_today・portal) を足した所だけ', async () => {
  const MIG = path.join(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '..', 'db', 'company', 'migrations');
  const fnOf = (file, head) => {
    const lines = fs.readFileSync(path.join(MIG, file), 'utf8').replace(/\r\n/g, '\n').split('\n');
    const a = lines.findIndex((l) => l.startsWith(head)); const b = lines.findIndex((l, i) => i > a && l === 'end $$;');
    assert.ok(a >= 0 && b > a, file); return lines.slice(a, b + 1).join('\n');
  };
  const f52 = fnOf('0052_master_registrations.sql', 'create function ops.register_new_sku(');
  const f57 = fnOf('0057_sku_registered_on.sql', 'create or replace function ops.register_new_sku(');
  for (const file of fs.readdirSync(MIG).filter((x) => /^\d{4}_.*\.sql$/.test(x) && x.slice(0, 4) > '0052' && x.slice(0, 4) < '0057')) {
    assert.ok(!/function\s+ops\.register_new_sku\s*\(/i.test(fs.readFileSync(path.join(MIG, file), 'utf8')), file + ' が登録の関数を作り直している = 0057 の元にする定義を見直す');
  }
  const back = f57
    .replace('create or replace function ops.register_new_sku(', 'create function ops.register_new_sku(')
    .replace('  -- 0057: 登録日 = この取引の JST の今日 (v_today = 原価の valid_from と同じ日)・出どころ portal を明示する (列の既定値には頼らない = 既定は空)\n', '')
    .replace('created_by_type, created_by_id,\n                         registered_on, registered_on_source)', 'created_by_type, created_by_id)')
    .replace("v_own, 'human', p_actor_id,\n            v_today, 'portal');", "v_own, 'human', p_actor_id);");
  assert.equal(back, f52);
  const p = await one(`select prosecdef, proconfig, pg_get_userbyid(proowner) as owner, has_function_privilege('master_edit', oid, 'execute') as editor,
      has_function_privilege('watcher', oid, 'execute') as watcher from pg_proc where oid = 'ops.register_new_sku(uuid, text, text, jsonb, text, jsonb)'::regprocedure`);
  assert.deepEqual(p, { prosecdef: true, proconfig: ['search_path=pg_catalog, pg_temp'], owner: 'deploy', editor: true, watcher: false });
});

await ta('[F2] 古い夜間ロードの形 (2 つの列を書かない INSERT) で作った単品は空のまま = 次の晩に NE の作成日で埋まる (portal で確定しない)', async () => {
  const pid = (await one("insert into core.products (company_id, display_code, name, created_by_type, created_by_id) values (1, 'r012', '商品 r012', 'system', 'old_load') returning product_id")).product_id;
  await q("insert into core.skus (company_id, product_id, sku_kind, code, name, tax_rate, tax_class, handling, created_by_type, created_by_id) values (1, $1, 'single', 'r012', '商品 r012', 0.1, 'STANDARD_10', 'active', 'system', 'old_load')", [pid]);
  await q("insert into core.skus (company_id, sku_kind, code, name, handling, created_by_type, created_by_id) values (1, 'set', 'rset3', 'セット rset3', 'active', 'system', 'old_load')");
  assert.deepEqual(await regOf('r012'), [null, null]);
  assert.deepEqual(await regOf('rset3'), [null, null]);
  addProduct('r012', '単品', '2026-08-08 08:08:08');
  addProduct('rset3', 'セット', null);
  await load('rl_12');
  assert.deepEqual(await regOf('r012'), ['2026-08-08', 'ne']);
  assert.deepEqual(await regOf('rset3'), [null, null]);   // 古い形で作られたセットは「初めて見た日」も分からない = 空のまま (今日にはしない)
});

// ── V: 画面 (本物の router・画面のロールで読む) ──
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'naka@test';
__setPgClientFactory(async (url) => {
  const role = /master_edit@/.test(url) ? 'master_edit' : 'deploy';
  await pg.query(`set role ${role}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
__setClock(() => LOAD_NOW.getTime());
// 持ち主表 = DB の active (広げる道 PR-2・この DB は切替の前 = 見るだけ)
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const BASE = `http://127.0.0.1:${server.address().port}/apps/master-edit`;
const get = async (u) => { const r = await fetch(BASE + u); return { status: r.status, text: await r.text() }; };

await ta('[V1] 一覧の「登録日の新しい順」: 新しい順・空は最後・同じ日はコード順 / 既定はコード順', async () => {
  const sorted = (await R.listSkus(db, { sort: 'reg_desc' }, { now: LOAD_NOW })).rows.map((x) => [x.code, x.registered_on]);
  const today = (await one("select core.jst_date(now())::text as d")).d;
  const want = [['r009', today], ['r010', '2026-10-05'], ['rset2', '2026-10-05'], ['r007', '2026-10-04'], ['r002', '2026-10-03'], ['r004', '2026-02-28'], ['r001', '2024-03-15'], ['r006', '2019-12-11'],
    ['r003', null], ['r005', null], ['r008', null], ['r011', null], ['rset1', null], ['r012', '2026-08-08'], ['rset3', null]];
  want.sort((a, b) => (a[1] === b[1] ? (a[0] < b[0] ? -1 : 1) : a[1] == null ? 1 : b[1] == null ? -1 : a[1] < b[1] ? 1 : -1));
  assert.deepEqual(sorted, want);
  assert.deepEqual((await R.listSkus(db, {}, { now: LOAD_NOW })).rows.map((x) => x.code), ['r001', 'r002', 'r003', 'r004', 'r005', 'r006', 'r007', 'r008', 'r009', 'r010', 'r011', 'r012', 'rset1', 'rset2', 'rset3']);
  assert.deepEqual((await R.listSkus(db, { sort: 'nope' }, { now: LOAD_NOW })).rows.map((x) => x.code)[0], 'r001');
  const one1 = (await R.listSkus(db, { q: 'r001' }, { now: LOAD_NOW })).rows[0];
  assert.deepEqual([one1.registered_on, one1.registered_on_source], ['2024-03-15', 'ne']);
});

await ta('[V2] 一覧の画面: 登録日の列 (出どころは title)・並びの切り替え・絞り込みに並びが残る', async () => {
  let r = await get('/');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('<th scope="col" style="width:96px">登録日</th>'));
  assert.ok(r.text.includes('<td class="muted" title="NE の作成日">2024/03/15</td>'), '登録日の列');
  assert.ok(r.text.includes('<td class="muted">—</td>'), '分からない登録日 = —');
  assert.ok(r.text.includes('<b aria-current="true">コード順</b>') && r.text.includes('<a href="?sort=reg_desc">登録日の新しい順</a>'));
  assert.ok(r.text.indexOf('sku/r001') < r.text.indexOf('sku/r010'));
  assert.ok(r.text.includes('title="ポータルで登録"') && r.text.includes('title="夜間ロードで初めて見た日"'));
  r = await get('/?sort=reg_desc&kind=single');
  assert.ok(r.text.includes('<b aria-current="true">登録日の新しい順</b>') && r.text.includes('<a href="?kind=single">コード順</a>'));
  assert.ok(r.text.indexOf('sku/r010') < r.text.indexOf('sku/r001') && r.text.indexOf('sku/r001') < r.text.indexOf('sku/r003'));
  assert.ok(r.text.includes('<input type="hidden" name="sort" value="reg_desc">'), '検索の欄・もっと絞るの form が並びを残す');
  assert.ok(r.text.includes('href="?kind=set&amp;sort=reg_desc"') || r.text.includes('href="?sort=reg_desc&amp;kind=set"') || /href="\?[^"]*sort=reg_desc[^"]*"[^>]*>[^<]*<\/a>/.test(r.text), '札のリンクが並びを残す');
});

await ta('[V3] 単品の画面の見出しに登録日と出どころ・分からない = —', async () => {
  let r = await get('/sku/r001');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('<span id="registered-on">登録日 2024/03/15 (NE の作成日)</span>'), r.text.slice(r.text.indexOf('class="meta"'), r.text.indexOf('class="meta"') + 600));
  r = await get('/sku/rset1');
  assert.ok(r.text.includes('<span id="registered-on">登録日 — (分かりません)</span>'));
  r = await get('/sku/rset2');
  assert.ok(r.text.includes('<span id="registered-on">登録日 2026/10/05 (夜間ロードで初めて見た日)</span>'));
  const page = await R.readSkuPage(db, 'r009', { now: LOAD_NOW });
  assert.deepEqual([page.registered.source, page.registered.label], ['portal', 'ポータルで登録']);
});

// ── C: 並び × ページ送り × 詳細検索 (#1619 の 2 段の読み方・#1620 の詳細検索と ?s= の印) ──
//   130 件 (登録日 = 1/1 から 40 日を回す・最後の 10 件は空) を足す = 1 ページ 100 件を超える。上の V の試験の後 (一覧の数を変えない)
await ta('[C] 並び × ページ送り × 詳細検索: 登録日の新しい順は ① の SQL で並べてから 100 件に切る・登録日の範囲 (空は入らない)・ページ送り / ?s= の印 / 絞り込みのリンク / フォームが並びを引き継ぐ', async () => {
  await q(`insert into core.skus (company_id, sku_kind, code, name, handling, registered_on, registered_on_source)
           select 1, 'exception', 'pg' || lpad(i::text, 3, '0'), 'ページ試験 ' || i, 'active',
                  case when i <= 120 then date '2026-01-01' + (i % 40) end, case when i <= 120 then 'ne' end
             from generate_series(1, 130) i`);
  const all = (await q("select code, registered_on::text as d from core.skus where code like 'pg%'"));
  const byRegDesc = (xs) => [...xs].sort((a, b) => (a.d === b.d ? (a.code < b.code ? -1 : 1) : a.d == null ? 1 : b.d == null ? -1 : a.d < b.d ? 1 : -1)).map((x) => x.code);
  const inRange = all.filter((x) => x.d != null && x.d >= '2026-01-03');   // i % 40 が 0・1 の 6 件と空の 10 件を除く = 114 件
  assert.equal(inRange.length, 114);
  const want = byRegDesc(inRange);
  // 読む関数: ① で並べてから切る (JS で 100 件を並べ直すのではない) = 2 ページ目は続き
  const f = { name: 'ぺーじ', reg_from: '2026/1/3', sort: 'reg_desc' };   // 名前はかなの同一視・日付は / でも読む
  const p1 = await R.listSkus(db, f, { now: LOAD_NOW });
  const p2 = await R.listSkus(db, { ...f, offset: '100' }, { now: LOAD_NOW });
  assert.equal(p1.total, 114); assert.equal(p2.total, 114);
  assert.deepEqual([...p1.rows, ...p2.rows].map((x) => x.code), want);
  assert.equal(p1.filters.reg_from, '2026-01-03');
  assert.ok(p1.rows.every((x) => x.registered_on >= '2026-01-03' && x.registered_on_source === 'ne'));
  // コード順 (既定) は同じ件数でコード順・登録日の終わりだけ・空は範囲に入らない・読めない日付は捨てる (= 範囲なし)
  assert.deepEqual((await R.listSkus(db, { name: 'ページ', reg_from: '2026-01-03' }, { now: LOAD_NOW })).rows.map((x) => x.code), inRange.map((x) => x.code).sort().slice(0, 100));
  assert.equal((await R.listSkus(db, { name: 'ページ', reg_to: '2026-01-02' }, { now: LOAD_NOW })).total, 6);
  assert.equal((await R.listSkus(db, { name: 'ページ', reg_from: '2026-02-30' }, { now: LOAD_NOW })).total, 130);
  assert.equal((await R.listSkus(db, { name: 'ページ', reg_from: '2026-02-30' }, { now: LOAD_NOW })).filters.reg_from, '');
  // 画面 (HTTP): 1 ページ目 → 「次の 100 件」のリンク (並び・範囲を引き継ぐ) → 2 ページ目
  const codesOf = (html) => [...html.matchAll(/class="rowlink" href="sku\/([^"]+)"/g)].map((m) => decodeURIComponent(m[1]));
  const nextOf = (html) => { const m = /<a class="btn sm" href="([^"]+)">次の 100 件/.exec(html); return m ? m[1].replace(/&amp;/g, '&') : null; };
  let r = await get('/?' + new URLSearchParams({ name: 'ページ', reg_from: '2026-01-03', sort: 'reg_desc' }));
  assert.equal(r.status, 200);
  assert.deepEqual(codesOf(r.text), want.slice(0, 100));
  assert.ok(r.text.includes('<b aria-current="true">登録日の新しい順</b>'));
  assert.ok(r.text.includes('id="adv-regdate"') && r.text.includes('name="reg_from" value="2026-01-03"'), '詳細検索の板に登録日の範囲');
  let next = nextOf(r.text);
  assert.ok(next && /sort=reg_desc/.test(next) && /reg_from=2026-01-03/.test(next) && /offset=100/.test(next), next);
  r = await get('/' + next);
  assert.deepEqual(codesOf(r.text), want.slice(100));
  // 札のリンク・もっと絞るのフォーム・詳細検索のフォーム・絞る欄が並びを引き継ぐ
  assert.ok(/<a class="chip[^"]*" href="\?[^"]*sort=reg_desc[^"]*"[^>]*>[^]*?セット/.test(r.text), '区分の札');
  assert.equal((r.text.match(/<input type="hidden" name="sort" value="reg_desc">/g) || []).length, 3, '絞る欄・もっと絞る・詳細検索の 3 つのフォーム');
  // 列: 登録日・在庫・(注文残)・対応が必要 が一緒に出る
  assert.ok(/<th scope="col" style="width:96px">登録日<\/th>[^]*在庫<span class="thsub">/.test(r.text));
  // ?s= の印 (長い詳細検索の条件): 並びは印の外の URL・登録日の範囲は印の中。ページ送りも印と並びを引き継ぐ
  const origin = new URL(BASE).origin;
  const pr = await fetch(BASE + '/api/search', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json', Origin: origin }, body: JSON.stringify({ name: 'ページ', reg_from: '2026-01-03', sort: 'reg_desc' }) });
  const pj = await pr.json();
  assert.equal(pr.status, 200, JSON.stringify(pj));
  const u = new URL(pj.url, BASE);
  assert.deepEqual([u.searchParams.get('sort'), u.searchParams.get('reg_from'), u.searchParams.get('name'), !!u.searchParams.get('s')], ['reg_desc', null, null, true]);
  r = await get('/' + u.search);
  assert.deepEqual(codesOf(r.text), want.slice(0, 100));
  next = nextOf(r.text);
  assert.ok(next && /[?&]s=/.test(next) && /sort=reg_desc/.test(next) && !/reg_from=/.test(next) && /offset=100/.test(next), next);
  r = await get('/' + next);
  assert.deepEqual(codesOf(r.text), want.slice(100));
  // 印のときの「もっと絞る」は印と並びを引き継ぐ
  assert.ok(/<input type="hidden" name="s" value="[A-Za-z0-9_-]+">/.test(r.text));
  await q("delete from core.skus where code like 'pg%'");
});

// ── B: 0057 の前にポータルで登録した SKU (⑤-2a = ops.master_registrations の origin = 'new_entry') は 0057 が portal で埋める (#1617 Codex R2 Medium) ──
//   別の DB (0056 まで) で作る = 上の DB の一覧の試験を変えない。材料の mirror は同じ (この後に足す商品は上の試験に出ない)
await ta('[B] 0057 の前のポータルの登録: 0057 が登録の行の日 (JST)・portal で埋める (単品・セット・状態は問わない)・backfill の行は空・その後の夜間ロードで ne に変わらない', async () => {
  const pg2 = new PGlite();
  await pg2.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg2.query(`alter database ${(await pg2.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg2.query('set role deploy');
  const db2 = pgliteAdapter(pg2);
  const q2 = async (sql, params) => (await db2.query(sql, params)).rows;
  const reg2 = async (code) => { const r = (await q2('select registered_on::text as d, registered_on_source as src from core.skus where code = $1', [code]))[0]; return r ? [r.d, r.src] : undefined; };
  await applyMigrations(db2, { log: quiet, to: '0056' });
  // 0056 の DB: ⑤-2a の登録の関数が作った形 (SKU + 登録の状態の行 origin = new_entry)。行を作った時刻は JST の日付の境目をまたぐ値
  const mk = async (code, kind, origin, state, createdAt) => {
    await q2('begin');
    try {
      let pid = null;
      if (kind === 'single') pid = (await q2("insert into core.products (company_id, display_code, name) values (1, $1, $2) returning product_id", [code, '商品 ' + code]))[0].product_id;
      const id = (await q2("insert into core.skus (company_id, product_id, sku_kind, code, name, tax_rate, tax_class, handling) values (1, $1, $2, $3, $4, 0.1, 'STANDARD_10', 'active') returning sku_id", [pid, kind, code, '商品 ' + code]))[0].sku_id;
      await q2("select pg_catalog.set_config('ops.registration_protocol', '1', true)");
      await q2("insert into ops.master_registrations (sku_id, state, origin, created_at, created_by, state_changed_at, state_changed_by) values ($1, $2, $3, $4, 'u1', $4, 'u1')", [id, state, origin, createdAt]);
      await q2('commit');
    } catch (e) { await q2('rollback'); throw e; }
  };
  await mk('rp01', 'single', 'new_entry', 'ne_pending', '2026-10-04T15:30:00Z');   // JST 10/5 00:30 (UTC の日付 10/4 ではない)
  await mk('rp02', 'single', 'new_entry', 'cancelled', '2026-10-04T14:30:00Z');    // JST 10/4 23:30・やめた登録も portal で作った SKU
  await mk('rpset1', 'set', 'new_entry', 'draft', '2026-10-03T01:00:00Z');         // セット = NE の作成日を取らない = ここで埋めないとずっと空
  await mk('rb01', 'single', 'backfill', 'available', '2026-10-04T03:00:00Z');      // 切替の日の backfill = ポータルで作った SKU ではない
  const ver0 = Object.fromEntries((await q2("select code, version::text as v from core.skus where code in ('rp01', 'rpset1', 'rb01')")).map((x) => [x.code, x.v]));
  await applyMigrations(db2, { log: quiet });
  assert.deepEqual(await reg2('rp01'), ['2026-10-05', 'portal']);
  assert.deepEqual(await reg2('rp02'), ['2026-10-04', 'portal']);
  assert.deepEqual(await reg2('rpset1'), ['2026-10-03', 'portal']);
  assert.deepEqual(await reg2('rb01'), [null, null]);
  // 変更の記録 (0026): 埋めた 3 件 × 2 列・source_system = migration_0057・system。version は埋めた行だけ進む
  const ev = await q2("select attribute, actor_type, source_system, count(*)::int as n from events.master_change_events where attribute like 'registered_on%' group by 1, 2, 3 order by 1");
  assert.deepEqual(ev, [{ attribute: 'registered_on', actor_type: 'system', source_system: 'migration_0057', n: 3 }, { attribute: 'registered_on_source', actor_type: 'system', source_system: 'migration_0057', n: 3 }]);
  const ver1 = Object.fromEntries((await q2("select code, version::text as v from core.skus where code in ('rp01', 'rpset1', 'rb01')")).map((x) => [x.code, x.v]));
  assert.ok(BigInt(ver1.rp01) > BigInt(ver0.rp01) && BigInt(ver1.rpset1) > BigInt(ver0.rpset1), JSON.stringify([ver0, ver1]));
  assert.equal(ver1.rb01, ver0.rb01);
  // 設定は migration の取引の外に漏れない
  assert.deepEqual((await q2("select coalesce(current_setting('core.source_system', true), '') as s"))[0], { s: '' });
  // その後 NE に登録された (CSV を取り込んだ日 = NE の作成日) 夜間ロード: portal のまま・backfill の単品は NE の作成日
  addProduct('rp01', '単品', '2026-10-05 10:00:00');
  addProduct('rp02', '単品', '2026-10-05 10:00:00');
  addProduct('rpset1', 'セット', null);
  addProduct('rb01', '単品', '2023-06-01 12:00:00');
  const r = await runInitialLoad(db2, plan(), { log: quiet, runId: 'rb_1', host: 'test-host', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  assert.deepEqual(await reg2('rp01'), ['2026-10-05', 'portal']);
  assert.deepEqual(await reg2('rp02'), ['2026-10-04', 'portal']);
  assert.deepEqual(await reg2('rpset1'), ['2026-10-03', 'portal']);
  assert.deepEqual(await reg2('rb01'), ['2023-06-01', 'ne']);
  assert.equal((await q2("select count(*)::int as n from events.master_change_events where run_id = 'rb_1' and attribute like 'registered_on%' and entity_key ->> 'sku_id' in (select sku_id::text from core.skus where code in ('rp01', 'rp02', 'rpset1'))"))[0].n, 0);
  await pg2.close();
});

await ta('[L7] セットの作成日が snapshot の 登録日 に来た晩 (NE の set_goods_creation_date): 空のセットは ne で埋める・新しいセットは ne で作る・first_seen のセットは変えない', async () => {
  assert.deepEqual(await regOf('rset1'), [null, null]);   // 0057 の前からあるセット (空)
  assert.deepEqual(await regOf('rset3'), [null, null]);   // 古い夜間ロードの形で作ったセット (空)
  assert.deepEqual(await regOf('rset2'), [LOAD_DAY, 'first_seen']);
  mirrorExec("update mirror_pml_snapshot_rows set 登録日 = '2021-04-01 09:00:00' where 商品コード = 'rset1'");
  mirrorExec("update mirror_pml_snapshot_rows set 登録日 = '2022/5/6 07:00:00' where 商品コード = 'rset3'");
  mirrorExec("update mirror_pml_snapshot_rows set 登録日 = '2020-01-01 00:00:00' where 商品コード = 'rset2'");
  addProduct('rset4', 'セット', '2026-09-30 12:00:00');
  await load('rl_13');
  assert.deepEqual(await regOf('rset1'), ['2021-04-01', 'ne']);
  assert.deepEqual(await regOf('rset3'), ['2022-05-06', 'ne']);
  assert.deepEqual(await regOf('rset4'), ['2026-09-30', 'ne']);
  assert.deepEqual(await regOf('rset2'), [LOAD_DAY, 'first_seen']);   // 一度入った日は変えない
});

server.close();
console.log(`\n${process.exitCode ? 'NG' : 'OK'} test-sku-registered-on: ${passed} 件 ok`);
