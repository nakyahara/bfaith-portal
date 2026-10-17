/**
 * test-ne-values-lz-codes.mjs — 今朝の NE の値の表とロジザードの書き方の表 (migration 0083・AI_reference CompanyDB構想/25 v3 §2.5 の PR-1 = 影)
 *
 * 固定する契約 (関数 ops.record_ne_values / ops.record_lz_codes):
 *   N1 書ける・同じ回の再送 = 何もしない (unchanged)・同じ回で中身が違う = run_conflict・印と受け付けた回の記録・値と書き方が同じ回か (code_mark_same_run)
 *   N2 古い回 (stale_run)・古い取得 (stale_fetch・単品とセットのそれぞれ・通し番号)・同じ取得の始めで通し番号が違う (fetch_conflict)・照合の回の記録が無い (unknown_run)
 *      同じ取得で新しい回 = 受け付ける (同じ朝の 2 回目の照合)
 *   N3 取得の件数: 式 (fetched = write_attempts + 落とした行)・stored ≦ write_attempts・読んだ生の行 = stored_rows・整数の形・時刻の形・始め ≦ 完了・完了が照合の回より後 = 断る /
 *      落とした行・重なりがある取得 = 受け付けて clean = false
 *   N4 行の形: 空・重なり・区分の列がそろわない・単位の数が生の行のコードの数と合わない・正規化の版・code_norm が正規化の形でない・
 *      ok なのに値が無い / ok でないのに値がある・unknown に理由が無い・セットに単品の列・値の形 (取扱の語・税率・正の金額・仕入先の配列)
 *   L1 書ける (DB が core.norm_code で組み分け: one / many / invalid・削除の ID も)・同じ書き出し・同じ中身 = unchanged・同じ書き出しで中身が違う = run_conflict・
 *      前の書き出し = stale_export・36 時間より前 / 未来 = 断る・成功の印が 15 分の外 / 日付が違う = 断る・行数・重なり・空の ID・空の一覧
 *   P1 権限: watch_writer = 2 つの関数の実行だけ (表は読めない・書けない) / watcher = 表を読むだけ / master_edit (画面のロール) = 何もできない / public = 実行できない
 *      持ち主のロールでも表へ直接は書けない (印の trigger)・受け付けた回の記録は変えられない・消せない (追記だけ)
 *   P2 ロールが先・0083 が後でも同じ権限 (migration の中で付ける) / ロールの流し直しでも同じ
 *   P3 関数の中で取る鍵 = CSV の鍵 (排他)・NE のコードの鍵 (共有)・NE の値の鍵 (排他) / ロジザードの書き方の鍵 (排他)
 * 🚨 実 PostgreSQL の独立した 2 接続での待ち合い (鍵の順・デッドロックしない) は test-ne-values-locks-pg.mjs
 * 使い方: node scripts/test-ne-values-lz-codes.mjs
 */
import assert from 'node:assert/strict';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
console.log('test-ne-values-lz-codes');

/** 1 つの Company DB (PGlite): 持ち主のロール deploy。roleFirst = 0082 まで流してからロールを作り、その後に 0083 を流す */
async function setupDb({ roleFirst = false } = {}) {
  const pg = new PGlite();
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  if (roleFirst) {
    await applyMigrations(db, { log: quiet, to: '0082' });
    await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
    await createMasterEditRoles(pg, {});
    await applyMigrations(db, { log: quiet });
  } else {
    await applyMigrations(db, { log: quiet });
    await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
    await createMasterEditRoles(pg, {});
  }
  return { pg, db };
}
const E = await setupDb();
const q = (sql, p) => E.pg.query(sql, p);
const asRole = async (role, fn) => { await q(`set role ${role}`); try { return await fn(); } finally { await q('set role deploy'); } };

// ── NE の値の入力 ──
const run = (i) => `mc_20301010T0000000${String(i).padStart(2, '0')}Z_abcdef`;
const H = 3600000;
const BASE = Date.UTC(2030, 9, 10, 0, 0, 0);   // 2030-10-10 09:00 JST
/** 照合の回 (i が大きいほど新しい)。observed = 照合の回の時刻 */
const addRun = async (i, observedMs = BASE + i * 24 * H) => {
  await q(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0) on conflict do nothing`, [run(i), new Date(observedMs).toISOString()]);
  return observedMs;
};
const iso = (ms) => new Date(ms).toISOString();
/** 取得の 1 つの側。startMs = 取得の始め */
const side = (startMs, n, extra = {}) => ({ started_at: iso(startMs), finished_at: iso(startMs + 120000), complete_rev: 100, fetched_rows: n, write_attempts: n, stored_rows: n,
  dropped_no_code: 0, dropped_missing_fields: 0, raw_rows: n, ...extra });
const SINGLE_COLS = ['name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier'];
const singleRows = (code, over = {}) => SINGLE_COLS.map((col) => ({ code_norm: code, col, ne_kind: 'single', state: 'ok',
  value: { name: `商品${code}`, handling: 'active', tax_rate: 0.1, standard_price_jpy: 1000, cost: 300, primary_supplier: ['0001'] }[col], ...(over[col] || {}) }));
const setRows = (code) => [{ code_norm: code, col: 'name', ne_kind: 'set', state: 'ok', value: `セット${code}` }, { code_norm: code, col: 'standard_price_jpy', ne_kind: 'set', state: 'ok', value: 5000 }];
/** 入力。dayMs = 取得の始め (既定 = 照合の回の 2 時間前) */
const nev = (i, { startMs = BASE + i * 24 * H - 2 * H, prodRev = 100 + i, setRev = 200 + i, entries = null, pExtra = {}, sExtra = {}, normVersion = 1, fingerprint = 'f'.repeat(64) } = {}) => ({
  compare_run_id: run(i), norm_version: normVersion,
  fetch: { fingerprint, products: side(startMs, 3, { complete_rev: prodRev, codes: 3, set_overlap: 1, ...pExtra }), sets: side(startMs + 1000, 2, { complete_rev: setRev, codes: 1, ...sExtra }) },
  entries: entries ?? [...singleRows('a-1'), ...singleRows('b-2'), ...setRows('s-1')],
});
const recNe = async (p) => (await q('select ops.record_ne_values($1::jsonb) as r', [JSON.stringify(p)])).rows[0].r;
const neMark = async () => (await q('select compare_run_id, products_rev, sets_rev, content_hash, code_mark_run, clean from ops.master_ne_values_mark')).rows;
const neRows = async () => Number((await q('select count(*)::int as n from ops.master_ne_values')).rows[0].n);

await ta('[N1] 書ける・同じ回の再送 = unchanged・同じ回で中身が違う = run_conflict・印と受け付けた回の記録・値と書き方が同じ回か', async () => {
  await addRun(1);
  const r = await recNe(nev(1));
  assert.deepEqual([r.state, r.rows, r.counts, r.code_mark_same_run, r.clean], ['written', 14, { ok: 14, set: 1, rows: 14, single: 2, invalid: 0, unknown: 0 }, false, { products: true, sets: true }]);
  assert.equal(await neRows(), 14);
  const v = (await q(`select value from ops.master_ne_values where code_norm = 'a-1' and col = 'primary_supplier'`)).rows[0].value;
  assert.deepEqual(v, ['0001']);
  const m1 = await neMark();
  assert.deepEqual(m1.map((m) => [m.compare_run_id, Number(m.products_rev), Number(m.sets_rev)]), [[run(1), 101, 201]]);
  assert.deepEqual((await recNe(nev(1))), { state: 'unchanged', rows: 14 });
  // 行の順が違っても同じ (ハッシュは行の順に依らない)
  const p = nev(1); p.entries.reverse();
  assert.equal((await recNe(p)).state, 'unchanged');
  const changed = nev(1, { entries: [...singleRows('a-1', { cost: { value: 301 } }), ...singleRows('b-2'), ...setRows('s-1')] });
  await assert.rejects(recNe(changed), /run_conflict/);
  // 取得の印 (通し番号) だけ違う = 中身が違う
  await assert.rejects(recNe(nev(1, { prodRev: 999 })), /run_conflict/);
  // 取得の件数・完了の時刻だけ違う (式は合う) = 中身が違う (#PR-1 Codex R1 Medium: 取得の記録の全部をハッシュに入れる)
  await assert.rejects(recNe(nev(1, { pExtra: { fetched_rows: 4, dropped_no_code: 1 } })), /run_conflict/);
  const s1 = BASE + 24 * H - 2 * H;
  await assert.rejects(recNe(nev(1, { sExtra: { finished_at: iso(s1 + 1000 + 180000) } })), /run_conflict/);
  assert.deepEqual((await neMark())[0].clean, { products: true, sets: true });
  assert.equal((await q(`select value from ops.master_ne_values where code_norm = 'a-1' and col = 'cost'`)).rows[0].value, 300);
  // NE のコードの書き方の印がこの回 = 同じ回
  await addRun(2);
  await q('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run(2), entries: [{ code_norm: 'a-1', kind: 'product', state: 'ok', ne_code: 'A-1', spellings: ['A-1'] }] })]);
  const r2 = await recNe(nev(2));
  assert.equal(r2.code_mark_same_run, true);
  assert.deepEqual((await q('select compare_run_id from ops.master_ne_values_runs order by observed_at')).rows.map((x) => x.compare_run_id), [run(1), run(2)]);
  assert.equal((await neMark())[0].code_mark_run, run(2));
});

await ta('[N2] 古い回・古い取得 (単品 / セット / 通し番号)・同じ取得の始めで通し番号が違う・照合の回の記録が無い = 断る / 同じ取得で新しい回 = 受け付ける', async () => {
  await addRun(3);
  await recNe(nev(3));
  await assert.rejects(recNe(nev(2)), /stale_run/);   // 前の回 (もう印より前)
  // 新しい回でも取得が古い = stale_fetch (単品・セット・通し番号のそれぞれ)
  await addRun(4);
  const s3 = BASE + 3 * 24 * H - 2 * H;
  await assert.rejects(recNe(nev(4, { startMs: s3 - 1000 })), /stale_fetch/);
  await assert.rejects(recNe(nev(4, { startMs: s3, prodRev: 103, sExtra: { started_at: iso(s3 - 5000), finished_at: iso(s3) } })), /stale_fetch/);
  await assert.rejects(recNe(nev(4, { prodRev: 102 })), /stale_fetch/);
  await assert.rejects(recNe(nev(4, { startMs: s3, prodRev: 150, setRev: 203 })), /fetch_conflict/);   // 単品の始めが同じで通し番号が違う
  // 同じ取得 (始めも通し番号も同じ) で新しい回 = 受け付ける (同じ朝の 2 回目の照合)
  const same = await recNe(nev(4, { startMs: s3, prodRev: 103, setRev: 203 }));
  assert.equal(same.state, 'written');
  assert.equal((await neMark())[0].compare_run_id, run(4));
  // 照合の回の記録が無い
  await assert.rejects(recNe(nev(9)), /unknown_run/);
  // 形の違う回 ID
  await assert.rejects(recNe({ ...nev(4), compare_run_id: 'mc_x' }), /invalid_input/);
  assert.equal((await neMark())[0].compare_run_id, run(4));
});

await ta('[N3] 取得の件数: 式・stored ≦ write_attempts・読んだ生の行 = stored_rows・整数と時刻の形・始め ≦ 完了・完了が照合の回より後 = 断る / 落とした行・重なりがある取得 = clean = false', async () => {
  const i = 5; await addRun(i);
  const bad = [
    [{ pExtra: { fetched_rows: 4 } }, /fetch_counts_mismatch.*式/],
    [{ pExtra: { stored_rows: 4, raw_rows: 4 } }, /fetch_counts_mismatch.*stored_rows が write_attempts/],
    [{ pExtra: { raw_rows: 2 } }, /fetch_counts_mismatch.*読んだ生の行/],
    [{ sExtra: { codes: 1.5 } }, /invalid_input: fetch\.sets\.codes/],
    [{ pExtra: { stored_rows: '3' } }, /invalid_input: fetch\.products\.stored_rows/],
    [{ pExtra: { dropped_no_code: -1 } }, /invalid_input/],
    [{ pExtra: { started_at: '2030-10-15 07:00:00' } }, /invalid_input: fetch\.products\.started_at/],
    [{ pExtra: { finished_at: iso(BASE + 5 * 24 * H - 3 * H) } }, /完了が始めより前/],
    [{ startMs: BASE + 5 * 24 * H + 9 * 60000 }, /完了が照合の回より後/],   // 照合の回の 9 分後に始めて 2 分 = 11 分後に完了
    [{ pExtra: { set_overlap: 5 } }, /set_overlap/],
    [{ fingerprint: 'x' }, /fingerprint/],
  ];
  for (const [o, re] of bad) await assert.rejects(recNe(nev(i, o)), re, JSON.stringify(o));
  assert.notEqual((await neMark())[0].compare_run_id, run(i));
  // 落とした行 (コードが空の 1 行) と重なり (同じキーの 2 度目) = 式は合う = 受け付けて clean = false
  const r = await recNe(nev(i, { pExtra: { fetched_rows: 5, write_attempts: 4, dropped_no_code: 1 }, sExtra: { fetched_rows: 2, write_attempts: 2 } }));
  assert.deepEqual([r.state, r.clean], ['written', { products: false, sets: true }]);
  assert.deepEqual((await neMark())[0].clean, { products: false, sets: true });
});

await ta('[N4] 行の形: 空・重なり・区分の列がそろわない・単位の数・正規化の版・正規化の形・ok と値・unknown の理由・セットに単品の列・値の形 = 断る (表は前のまま)', async () => {
  const i = 6; await addRun(i);
  const before = await neRows();
  const base = () => [...singleRows('a-1'), ...singleRows('b-2'), ...setRows('s-1')];
  const withRow = (k, patch) => { const e = base(); e[k] = { ...e[k], ...patch }; return e; };
  const cases = [
    [[], /empty/],
    [[...base(), base()[0]], /重なり/],
    [base().filter((e) => !(e.code_norm === 'b-2' && e.col === 'cost')), /incomplete/],
    [[...base(), { code_norm: 's-1', col: 'cost', ne_kind: 'set', state: 'invalid' }], /形の違う行/],              // セットに単品の列
    [[...singleRows('a-1'), ...setRows('s-1')], /units_mismatch/],                                                   // 単品が 1 つ足りない (落とした)
    [[...base(), ...singleRows('c-3')], /units_mismatch/],
    [withRow(0, { code_norm: 'A-1' }), /形の違う行/],                                                              // 正規化の形でない
    [withRow(0, { ne_kind: 'set' }), /incomplete/],                                                                // 1 つのコードに区分が 2 つ
    [withRow(0, { state: 'ok', value: undefined }), /形の違う行/],                                                 // ok なのに値が無い (JSON で鍵が消える)
    [withRow(0, { state: 'invalid' }), /形の違う行/],                                                              // ok でないのに値がある
    [withRow(0, { state: 'unknown', value: undefined }), /形の違う行/],                                            // unknown に理由が無い
    [withRow(1, { value: '取扱中' }), /ck_mnv_value/],                                                             // 取扱の語 (正規化の前の形)
    [withRow(2, { value: 10 }), /ck_mnv_value/],                                                                   // 税率 10 (0.1 / 0.08 だけ)
    [withRow(4, { value: 0 }), /ck_mnv_value/],                                                                    // 原価 0 (値なしは null)
    [withRow(5, { value: '0001' }), /ck_mnv_value/],                                                               // 仕入先は配列
    [withRow(0, { state: 'unknown', reason: 'other', value: undefined }), /master_ne_values_reason_check|reason/],
  ];
  for (const [entries, re] of cases) await assert.rejects(recNe(nev(i, { entries })), re, JSON.stringify(entries).slice(0, 200));
  await assert.rejects(recNe(nev(i, { normVersion: 2 })), /norm_version_rejected/);
  await assert.rejects(recNe({ ...nev(i), entries: 'x' }), /entries が配列でない/);
  await assert.rejects(recNe(nev(i, { pExtra: { codes: 0, set_overlap: 0 } })), /empty/);
  assert.equal(await neRows(), before);
  // 比べられない値 (unknown の理由・invalid)・値なし (null) は通る
  const ok = await recNe(nev(i, { entries: [...singleRows('a-1', { name: { value: null }, cost: { value: null }, primary_supplier: { value: [] }, tax_rate: { value: 0.08 } }),
    ...singleRows('b-2', { handling: { state: 'invalid', value: undefined }, cost: { state: 'unknown', reason: 'raw_unknown', value: undefined } }),
    { code_norm: 's-1', col: 'name', ne_kind: 'set', state: 'unknown', reason: 'collided' }, { code_norm: 's-1', col: 'standard_price_jpy', ne_kind: 'set', state: 'unknown', reason: 'int_blocked' }] }));
  assert.deepEqual(ok.counts, { ok: 10, set: 1, rows: 14, single: 2, invalid: 1, unknown: 3 });
  assert.deepEqual((await q(`select state, reason, value from ops.master_ne_values where code_norm = 'a-1' and col = 'name'`)).rows[0], { state: 'ok', reason: null, value: null });
});

// ── ロジザードの書き方の入力 (時刻は本物の今 = DB の 36 時間の確かめ) ──
let lzSeq = 0;
const lzRun = () => `lzd_20301010T0000000${String(++lzSeq).padStart(2, '0')}Z_abcdef`;
const jstDate = (ms) => new Date(ms + 9 * H).toISOString().slice(0, 10);
const lzp = ({ exportedMs = Date.now() - 2 * H, stampLagMs = 45000, ids = ['U60E-10', 'u60e-05', 'U60E-05', 'A-1', 'b.2', 'Ｃ－３'], rows, sha = 'a'.repeat(64), stampDate, runId = lzRun() } = {}) => ({
  lz_daily_run_id: runId,
  export: { exported_at: iso(exportedMs), stamp_at: iso(exportedMs + stampLagMs), stamp_date: stampDate ?? jstDate(exportedMs), csv_sha256: sha, rows: rows ?? ids.length }, ids });
const recLz = async (p) => (await q('select ops.record_lz_codes($1::jsonb) as r', [JSON.stringify(p)])).rows[0].r;
const lzCodes = async () => Object.fromEntries((await q('select code_norm, state, lz_code, spellings from ops.master_lz_codes order by 1')).rows.map((r) => [r.code_norm, [r.state, r.lz_code, r.spellings]]));
const lzMark = async () => (await q('select lz_daily_run_id, exported_at, csv_sha256, csv_rows from ops.master_lz_codes_mark')).rows;

await ta('[L1] ロジザードの書き方: DB が core.norm_code で組み分け (one / many / invalid)・同じ書き出し = unchanged / 中身が違う = run_conflict・前の書き出し・36 時間・未来・成功の印・行数・重なり・空 = 断る', async () => {
  const t1 = Date.now() - 3 * H;
  const p1 = lzp({ exportedMs: t1 });
  const r = await recLz(p1);
  assert.deepEqual([r.state, r.counts], ['written', { ids: 6, one: 2, many: 1, codes: 5, invalid: 2 }]);
  const c = await lzCodes();
  assert.deepEqual(c['u60e-05'], ['many', null, ['U60E-05', 'u60e-05']]);   // 書き方が 2 つ = 作らない
  assert.deepEqual(c['u60e-10'], ['one', 'U60E-10', ['U60E-10']]);
  assert.deepEqual(c['a-1'], ['one', 'A-1', ['A-1']]);
  assert.deepEqual(c['b.2'], ['invalid', null, ['b.2']]);                    // NE のコードに使えない形
  assert.deepEqual(c['c-3'], ['invalid', null, ['Ｃ－３']]);                 // 全角 (正規化すると c-3 だが書き方としては使えない)
  assert.deepEqual((await lzMark()).map((m) => [m.lz_daily_run_id, new Date(m.exported_at).toISOString(), m.csv_rows]), [[p1.lz_daily_run_id, iso(t1), 6]]);
  // 同じ書き出しの再送 (別の lz-daily の回でも) = 何もしない
  const again = await recLz({ ...p1, lz_daily_run_id: lzRun(), ids: [...p1.ids].reverse() });
  assert.deepEqual([again.state, again.lz_daily_run_id], ['unchanged', p1.lz_daily_run_id]);
  await assert.rejects(recLz({ ...p1, lz_daily_run_id: lzRun(), ids: [...p1.ids.slice(1), 'Z-9'] }), /run_conflict/);
  await assert.rejects(recLz({ ...lzp({ exportedMs: t1 }), export: { ...p1.export, csv_sha256: 'b'.repeat(64) } }), /run_conflict/);
  // 前の書き出し = stale_export / 36 時間より前・未来 (5 分より先) = 断る
  await assert.rejects(recLz(lzp({ exportedMs: t1 - 60000 })), /stale_export/);
  await assert.rejects(recLz(lzp({ exportedMs: Date.now() - 37 * H })), /stale_export.*36 時間/);
  await assert.rejects(recLz(lzp({ exportedMs: Date.now() + 10 * 60000 })), /future_export/);
  // 成功の印: 書き出しの前・15 分より後・日付が違う
  await assert.rejects(recLz(lzp({ stampLagMs: -1000 })), /export_not_confirmed/);
  await assert.rejects(recLz(lzp({ stampLagMs: 16 * 60000 })), /export_not_confirmed/);
  await assert.rejects(recLz(lzp({ stampDate: '2000-01-01' })), /export_not_confirmed.*日付/);
  // 行数・重なり・空の ID・空の一覧・形
  await assert.rejects(recLz(lzp({ rows: 7 })), /rows_mismatch/);
  await assert.rejects(recLz(lzp({ ids: ['A-1', 'A-1'] })), /同じ商品 ID/);
  await assert.rejects(recLz(lzp({ ids: ['A-1', '  '] })), /使えない商品 ID/);
  await assert.rejects(recLz(lzp({ ids: ['A-1', 5] })), /使えない商品 ID/);
  await assert.rejects(recLz(lzp({ ids: [] })), /empty/);
  await assert.rejects(recLz({ ...lzp(), lz_daily_run_id: 'lzd_x' }), /invalid_input/);
  await assert.rejects(recLz({ ...lzp(), export: { ...lzp().export, exported_at: '2030-10-10 07:00:00' } }), /invalid_input/);
  assert.equal((await lzMark())[0].lz_daily_run_id, p1.lz_daily_run_id);
  // 新しい書き出し = 入れ替え (前の書き方は残らない)
  const p2 = lzp({ exportedMs: t1 + 60 * 60000, ids: ['U60E-05', 'U60E-10'] });
  assert.equal((await recLz(p2)).state, 'written');
  assert.deepEqual(Object.keys(await lzCodes()), ['u60e-05', 'u60e-10']);
  assert.deepEqual((await lzCodes())['u60e-05'], ['one', 'U60E-05', ['U60E-05']]);
  assert.deepEqual((await q('select lz_daily_run_id from ops.master_lz_codes_runs order by exported_at')).rows.map((x) => x.lz_daily_run_id), [p1.lz_daily_run_id, p2.lz_daily_run_id]);
});

/** ロールで試す (取引の中・必ず rollback)。戻り値 = 'ok' か SQLSTATE */
const tryAs = async (role, sql, params = []) => {
  await q('begin');
  try { await q(`set local role ${role}`); await q(sql, params); return 'ok'; }
  catch (e) { return e.code || 'error'; }
  finally { await q('rollback'); }
};
const TABLES = ['master_ne_values', 'master_ne_values_mark', 'master_ne_values_runs', 'master_lz_codes', 'master_lz_codes_mark', 'master_lz_codes_runs'];
async function permissionChecks(Ex) {
  const qq = (sql, p) => Ex.pg.query(sql, p);
  const t = async (role, sql, params = []) => {
    await qq('begin');
    try { await qq(`set local role ${role}`); await qq(sql, params); return 'ok'; }
    catch (e) { return e.code || 'error'; }
    finally { await qq('rollback'); }
  };
  for (const tb of TABLES) {
    assert.equal(await t('watcher', `select * from ops.${tb} limit 1`), 'ok', `watcher は ${tb} を読める`);
    assert.equal(await t('watch_writer', `select * from ops.${tb} limit 1`), '42501', `watch_writer は ${tb} を読めない`);
    assert.equal(await t('master_edit', `select * from ops.${tb} limit 1`), '42501', `master_edit は ${tb} を読めない`);
    assert.equal(await t('watch_writer', `delete from ops.${tb}`), '42501', `watch_writer は ${tb} を消せない`);
  }
  const fns = ['ops.record_ne_values(jsonb)', 'ops.record_lz_codes(jsonb)'];
  const can = async (role, f) => (await qq('select has_function_privilege($1, $2, \'execute\') as ok', [role, f])).rows[0].ok;
  for (const f of fns) {
    assert.equal(await can('watch_writer', f), true, `watch_writer は ${f} を実行できる`);
    assert.equal(await can('watcher', f), false, `watcher は ${f} を実行できない`);
    assert.equal(await can('master_edit', f), false, `master_edit は ${f} を実行できない`);
    assert.equal(await can('public', f), false, `public は ${f} を実行できない`);
  }
  for (const f of ['ops._nev_fetch_side(jsonb, text, timestamptz)', 'ops.guard_master_ne_lz()']) {
    for (const role of ['watch_writer', 'watcher', 'master_edit', 'public']) assert.equal(await can(role, f), false, `${role} は ${f} を実行できない`);
  }
  // security definer の 2 つ = search_path の決まり (pg_catalog, pg_temp)
  const cfg = (await qq(`select p.proname, p.prosecdef, p.proconfig from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname in ('record_ne_values', 'record_lz_codes') order by 1`)).rows;
  assert.deepEqual(cfg.map((r) => [r.proname, r.prosecdef, r.proconfig]), [['record_lz_codes', true, ['search_path=pg_catalog, pg_temp']], ['record_ne_values', true, ['search_path=pg_catalog, pg_temp']]]);
}

await ta('[P1] 権限: watch_writer = 関数の実行だけ / watcher = 読むだけ / master_edit・public = 何もできない / 持ち主でも表へ直接は書けない (印の trigger)・受け付けた回の記録は変えられない・消せない', async () => {
  await permissionChecks(E);
  // watch_writer は関数で書ける (security definer = 持ち主の権限)
  await addRun(7);
  assert.equal(await tryAs('watch_writer', 'select ops.record_ne_values($1::jsonb)', [JSON.stringify(nev(7))]), 'ok');
  assert.equal(await tryAs('watch_writer', 'select ops.record_lz_codes($1::jsonb)', [JSON.stringify(lzp({ exportedMs: Date.now() - H }))]), 'ok');
  assert.equal(await tryAs('watcher', 'select ops.record_ne_values($1::jsonb)', [JSON.stringify(nev(7))]), '42501');
  assert.equal(await tryAs('master_edit', 'select ops.record_lz_codes($1::jsonb)', [JSON.stringify(lzp({ exportedMs: Date.now() - H }))]), '42501');
  // 持ち主のロールでも直接は書けない (印 = 関数の中だけ)
  for (const sql of [`insert into ops.master_ne_values (code_norm, col, ne_kind, state, value) values ('x-1', 'name', 'single', 'ok', '"x"')`,
    `delete from ops.master_ne_values`, `update ops.master_ne_values set value = '"y"'`, `truncate ops.master_ne_values`, `delete from ops.master_ne_values_mark`,
    `insert into ops.master_lz_codes (code_norm, state, lz_code, spellings) values ('x-1', 'one', 'X-1', '["X-1"]')`, `delete from ops.master_lz_codes_mark`, `truncate ops.master_lz_codes`]) {
    await assert.rejects(q(sql), /ne_lz_guard/, sql);
  }
  // 受け付けた回の記録は追記だけ (印を立てても変えられない・消せない)
  await q('begin');
  try {
    await q(`select set_config('ops.ne_values_protocol', '1', true), set_config('ops.lz_codes_protocol', '1', true)`);
    await assert.rejects(q(`update ops.master_ne_values_runs set counts = '{}'`), /append-only|reject|追記|変更/i);
  } finally { await q('rollback'); }
  await q('begin');
  try {
    await q(`select set_config('ops.lz_codes_protocol', '1', true)`);
    await assert.rejects(q(`delete from ops.master_lz_codes_runs`), /append-only|reject|追記|変更/i);
  } finally { await q('rollback'); }
});

await ta('[P2] ロールが先・0083 が後 (migration の中で付ける) でも同じ権限 / ロールの流し直しでも同じ', async () => {
  const E2 = await setupDb({ roleFirst: true });
  try {
    await permissionChecks(E2);
    await createRoles(E2.pg, {});   // 流し直し (パスワードは変えない)
    await createMasterEditRoles(E2.pg, {});
    await permissionChecks(E2);
  } finally { await E2.pg.close(); }
});

await ta('[P3] 関数の中で取る鍵: record_ne_values = CSV の鍵 (排他)・NE のコードの鍵 (共有)・NE の値の鍵 (排他) / record_lz_codes = ロジザードの書き方の鍵 (排他)・ほかは取らない', async () => {
  const locks = async () => (await q(`select l.objid::int as k, l.mode from pg_locks l where l.locktype = 'advisory' and l.pid = pg_backend_pid() order by 1, 2`)).rows;
  const keys = (await q(`select hashtext('ops.ne_csv') as csv, hashtext('ops.ne_codes') as codes, hashtext('ops.ne_values') as vals, hashtext('ops.lz_codes') as lz`)).rows[0];
  // pg_locks の objid は oid (符号なし) = hashtext の負の数は 2^32 を足した数で出る
  const u = (n) => (n < 0 ? n + 2 ** 32 : n);
  const sorted = (a) => a.map(([k, m]) => `${u(k)}:${m}`).sort();
  await addRun(8);
  await q('begin');
  try {
    await recNe(nev(8));
    assert.deepEqual(sorted((await locks()).map((x) => [x.k, x.mode])), sorted([[keys.csv, 'ExclusiveLock'], [keys.codes, 'ShareLock'], [keys.vals, 'ExclusiveLock']]));
  } finally { await q('rollback'); }
  await q('begin');
  try {
    await recLz(lzp({ exportedMs: Date.now() - 30 * 60000 }));
    assert.deepEqual(sorted((await locks()).map((x) => [x.k, x.mode])), sorted([[keys.lz, 'ExclusiveLock']]));
  } finally { await q('rollback'); }
});

await E.pg.close();
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
