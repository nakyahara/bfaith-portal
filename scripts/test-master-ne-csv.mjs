/**
 * test-master-ne-csv.mjs — NE に取り込む CSV (apps/master-decisions/ne-csv.mjs・router・画面。③b-1。Company DB構想 10 §6.1.1「③b の契約 v3」・migration 0040)
 *
 * 本物の router を HTTP 越しに通す。Company DB = PGlite (持ち主のロール deploy で migration)。セッションは x-test-session で模擬。時計は __setClock で動かす (JST の日をまたぐ)
 * 固定する契約:
 *   1 列の表 (見出しは固定・在庫の列は無い) と値の書き方の境目 / 2 CSV の byte 列 (UTF-8・BOM なし・CRLF・引用符)
 *   3 対象: 単位の最後の判断が fix_ne の承認・未完了・今日の回に出ている・列の表にある・書ける値 (書けない = NE の画面で直す)・古い承認を生き返らせない (H1)
 *   4 今日 (JST) の照合の回が無い = 作れない・確かめられない (H2)
 *   5 試し用 (5 行まで)・予約 (同じ単位を 2 つのファイルに入れない)・選んだ承認だけで作る・ダウンロード
 *   6 実機の確かめの記録 (最後の記録が ok の組だけ 1,000 行まで)
 *   7 列ごとのファイルの中身 (単品 7 列・セット 2 列)
 *   8 確かめる → 申告 (確かめる前・申告の後・申告のし直し)
 *   9 判断の API: 新しい判断で予約を外し、まだ申告していないファイルを void (別の指紋でも同じ単位なら) → 作り直しに前の行を記録 (H3)
 *  10 直前の確かめで外れる (もう完了・今朝の照合に出ていない) = void
 *  11 行の届き方: 同じ日の回は数えない (確認できない) → 次の日の回で 反映されていない / 確かめが要る / 確認済み → 作るときに予約を外す (M1・M2)
 *  12 確かめは同じ日のうちだけ / 13 全部拒まれた = void / 14 使わない (void)・void は配らない
 *  15 表の守り (中身は書き換えない・消さない・予約の一意・watcher は読むだけ) / 16 名簿・Origin / 17 画面とつかいかた / 18 0040 の前の DB / 19 migration の watcher の権限
 *  20 候補の行を取った後に照合の回を読み直す / (9・11) 確かめた後の新しい回で申告できない・もう使えない申告済みのファイルは配らない・申告できない (#1495 Codex R1)
 *  (バックアップ → 復元の byte 列の往復は scripts/test-master-concurrency-pg.mjs [13] = 本物の Postgres。PGlite は byte 列の値を文字で受けない)
 * 使い方: node scripts/test-master-ne-csv.mjs
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
const { writeDecisions } = await import('../apps/company-db/master-compare/decisions.mjs');
const { default: router, __setPgClientFactory, __setClock } = await import('../apps/master-decisions/router.mjs');
const csvMod = await import('../apps/master-decisions/ne-csv.mjs');
const { COLUMNS, FORBIDDEN_HEADERS, buildCsv, specOf, headerOf, TRIAL_ROWS, csvSummary, createExport } = csvMod;
const { applyDecisions } = await import('../apps/master-decisions/decide.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const H = (label) => crypto.createHash('sha256').update(label).digest('hex');
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const at = (date, hh) => Date.parse(`${date}T${String(hh).padStart(2, '0')}:00:00+09:00`);
let runSeq = 0;
const runId = (date) => `mc_${date.replace(/-/g, '')}T${String(++runSeq).padStart(9, '0')}Z_abcdef`;

// ── Company DB ──
async function freshDb(opts = {}) {
  const pg = new PGlite();
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet, ...opts });
  return { pg, db };
}
const { pg, db } = await freshDb();
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });

// ── 候補 (label → 候補の形)。print.sku_kind = 照合の NE 側の種類 ──
const C = {};
const mk = (label, sk, col, kind, o = {}) => {
  const code = sk.split(':')[1];
  C[label] = { fingerprint: H(label), subject_key: sk, code_norm: code, col, child: o.child ?? null, cls: 'rule', reason_kind: 'none', semantic: 'none@1',
    print: { code_norm: code, sku_kind: kind, col, child: o.child ?? null, n: o.n ?? `N-${label}`, c: o.c ?? null, label }, resolutions: ['accept_difference', 'fix_ne', 'fix_cdb'], proposal: { op: 'decide' } };
  return C[label];
};
mk('a001', 'value:a001', 'name', 'single', { n: 'A旧' });
mk('b002', 'value:b002', 'tax_rate', 'single', { n: null });
mk('c003', 'value:c003', 'handling', 'single', { n: 'active' });
mk('d004', 'cost:d004', 'cost', 'single', { n: 1000 });
mk('e005', 'primary_supplier:e005', 'primary_supplier', 'single', { n: '0100' });
mk('f006', 'parent:f006', 'parent', 'single', { n: 'x-1' });
mk('g007', 'parent:g007', 'parent', 'single', { n: null });
mk('h008', 'value:h008', 'name', 'set', { n: '' });
mk('i009', 'value:i009', 'standard_price_jpy', 'set', { n: 2800 });
mk('j010', 'components:j010', 'components', 'set', { child: 'k011', n: 1 });
mk('k012', 'value:k012', 'name', 'single');
mk('l013', 'value:l013', 'tax_rate', 'set');
mk('m014', 'value:m014', 'name', 'single');
mk('n015', 'value:n015', 'name', 'single');
mk('P1', 'value:p016', 'name', 'single', { n: 'P1' });
mk('P2', 'value:p016', 'name', 'single', { n: 'P2' });
mk('q017', 'value:q017', 'name', 'single');
mk('x.y1', 'value:x.y1', 'name', 'single');   // NE のコードに使えない文字 (.)
mk('tm01', 'value:tm01', 'name', 'single');   // 承認の目標の単位が候補と違う (台帳の外から入った承認)
for (let i = 1; i <= 7; i++) mk(`t00${i}`, `value:t00${i}`, 'name', 'single');
mk('T4b', 'value:t004', 'name', 'single', { n: 'T4 の別の値' });
const BASE = Object.keys(C).filter((l) => !['n015', 'P2', 'T4b'].includes(l));
// NE の元の書き方 (③b-1b・0041): 照合の回ごとに記録する。商品のコード = 全部のラベルの norm (h008 だけ元は大文字 H008)・代表の名札 p-100 = 元は P-100
const NE_CODES = () => {
  const seen = new Set(), entries = [];
  for (const c of Object.values(C)) {
    const n = c.code_norm;
    if (seen.has(n) || !/^[a-z0-9_-]+$/.test(n)) continue;
    seen.add(n);
    const code = n === 'h008' ? 'H008' : n;
    entries.push({ code_norm: n, kind: 'product', state: 'ok', ne_code: code, spellings: [code] });
  }
  entries.push({ code_norm: 'p-100', kind: 'rep', state: 'ok', ne_code: 'P-100', spellings: ['P-100'] });
  return entries;
};
const recordCodes = (dbx, run, entries = NE_CODES()) => dbx.query('select ops.record_ne_codes($1::jsonb) as r', [JSON.stringify({ compare_run_id: run, entries })]);
const prod = (n, code = n) => ({ code_norm: n, kind: 'product', state: 'ok', ne_code: code, spellings: [code] });
const writeRun = async (date, hh, labels) => {
  const id = runId(date);
  await writeDecisions(db, { compareRunId: id, observedAt: new Date(at(date, hh)).toISOString(), decisions: labels.map((l) => C[l]) });
  await recordCodes(db, id);
  return id;
};
const EV = {};
const decideRaw = async (label, kind, resolution, value) => {
  const c = C[label];
  const target = resolution === 'fix_ne' || resolution === 'fix_cdb' ? JSON.stringify({ subject_key: c.subject_key, col: c.col, child: c.child, value }) : null;
  const id = Number((await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, $2, $3, $4::jsonb, 'user', 'setup@test') returning event_id`,
    [c.fingerprint, kind, resolution, target])).rows[0].event_id);
  EV[label] = id;
  return id;
};
const done = async (label, run) => {
  const c = C[label];
  const tg = (await db.query('select target from ops.master_decision_events where event_id = $1', [EV[label]])).rows[0].target;
  return (await db.query('select ops.record_decision_done($1::bigint, $2::text, $3::jsonb) as ok', [EV[label], run, JSON.stringify({ side: 'ne', subject_key: c.subject_key, col: c.col, child: c.child, value: tg.value })])).rows[0].ok;
};

// 0 日目: n015 (今日の回に出ない) と P2 (後で却下する別の指紋) / 1 日目 08:00: ほかの全部
const R0 = await writeRun('2030-01-09', 8, [...BASE, 'n015', 'P2']);
await decideRaw('a001', 'approved', 'fix_ne', 'A,名"前');
await decideRaw('b002', 'approved', 'fix_ne', 0.1);
await decideRaw('c003', 'approved', 'fix_ne', 'discontinued');
await decideRaw('d004', 'approved', 'fix_ne', 1200);
await decideRaw('e005', 'approved', 'fix_ne', '0135');
await decideRaw('f006', 'approved', 'fix_ne', null);
await decideRaw('g007', 'approved', 'fix_ne', 'p-100');
await decideRaw('h008', 'approved', 'fix_ne', 'セットH');
await decideRaw('i009', 'approved', 'fix_ne', 3000);
await decideRaw('j010', 'approved', 'fix_ne', 2);
await decideRaw('k012', 'approved', 'fix_ne', 'Empty');
await decideRaw('l013', 'approved', 'fix_ne', 0.08);
await decideRaw('m014', 'approved', 'accept_difference', null);
await decideRaw('n015', 'approved', 'fix_ne', 'N新');
await decideRaw('P1', 'approved', 'fix_ne', 'P新');
await decideRaw('P2', 'rejected', null, null);   // 同じ単位 (p016 の名前) に後から別の指紋で判断 = P1 の承認は置き換わった
await decideRaw('q017', 'approved', 'fix_ne', 'Q新');
await decideRaw('x.y1', 'approved', 'fix_ne', 'X新');
await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'setup@test')`,
  [C.tm01.fingerprint, JSON.stringify({ subject_key: 'value:other', col: 'name', child: null, value: 'TM新' })]);
for (let i = 1; i <= 7; i++) await decideRaw(`t00${i}`, 'approved', 'fix_ne', `T新${i}`);
const R1 = await writeRun('2030-01-10', 8, BASE);

// ── ポータル ──
let now = at('2030-01-10', 10);
__setClock(() => now);
process.env.COMPANY_DB_URL = 'postgres://test@localhost:5432/test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
__setPgClientFactory(async () => ({ query: (t, p) => pg.query(t, p), end: async () => {}, on: () => {} }));
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'approver' ? { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-decisions'] }
    : s === 'user' ? { authenticated: true, email: 'user@test', role: 'user', allowedApps: ['master-decisions'] } : null;
  next();
});
app.use('/apps/master-decisions', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASEURL = `${ORIGIN}/apps/master-decisions`;
async function call(method, url, { body, session = 'approver', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASEURL + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const buf = Buffer.from(await r.arrayBuffer());
  let j = null; try { j = JSON.parse(buf.toString('utf8')); } catch { /* HTML・CSV */ }
  return { status: r.status, j, text: buf.toString('utf8'), buf, r };
}
const summary = async () => (await call('GET', '/api/csv/summary')).j;
const create = (body, session) => call('POST', '/api/csv/exports', { body, session });
const check = (id) => call('POST', `/api/csv/exports/${id}/check`, { body: {} });
const declare = (id, result = 'ok', note = null) => call('POST', `/api/csv/exports/${id}/declare`, { body: { result, note } });
const detail = async (id) => (await call('GET', `/api/csv/exports/${id}`)).j;
const fileOf = async (id) => (await call('GET', `/api/csv/exports/${id}/file`)).buf.toString('utf8');
const byCode = (d) => Object.fromEntries(d.rows.map((r) => [r.code_norm, r]));
const group = (s, key) => s.groups.find((g) => g.key === key);
const csvCodes = (s, key) => s.csv_items.filter((x) => (x.sku_kind === 'set' ? 'sets' : 'products') + ':' + x.col === key && !x.export_id).map((x) => x.code_norm);

await ta('[1] 列の表: 見出しは固定・在庫の列は無い / 値の書き方の境目', async () => {
  assert.deepEqual(Object.keys(COLUMNS), ['products:name', 'products:handling', 'products:tax_rate', 'products:standard_price_jpy', 'products:cost', 'products:primary_supplier', 'products:parent', 'sets:name', 'sets:standard_price_jpy']);
  assert.deepEqual(Object.values(COLUMNS).map((s) => headerOf(s).join(',')), ['syohin_code,syohin_name', 'syohin_code,toriatukai_kbn', 'syohin_code,tax_rate', 'syohin_code,baika_tnk', 'syohin_code,genka_tnk',
    'syohin_code,sire_code', 'syohin_code,daihyo_syohin_code', 'set_syohin_code,set_syohin_name', 'set_syohin_code,set_baika_tnk']);
  for (const s of Object.values(COLUMNS)) for (const h of headerOf(s)) assert.ok(!FORBIDDEN_HEADERS.includes(h) && !/zaiko|nyusyukko|visible/.test(h), h);
  assert.throws(() => buildCsv({ code: 'syohin_code', ne: 'zaiko_su' }, []), /出してはいけない列/);
  const cell = (k, v) => { const r = COLUMNS[k].cell(v); return r.ok ? r.cell : `!${r.reason}`; };
  assert.equal(cell('products:name', 'x'.repeat(255)), 'x'.repeat(255));
  assert.equal(cell('products:name', 'x'.repeat(256)), '!name_length');
  assert.equal(cell('products:name', '字'.repeat(255)), '字'.repeat(255));
  for (const v of ['empty', 'EMPTY', 'Empty']) assert.equal(cell('products:name', v), '!empty_word');
  for (const code of [9, 10, 13, 0, 0x7f, 0x85, 0x2028, 0x2029]) assert.equal(cell('products:name', `a${String.fromCharCode(code)}b`), '!name_control', code);
  assert.equal(cell('products:name', `a${String.fromCodePoint(0x1f600)}`), '!name_astral');
  for (const v of ['', ' a', 'a ', null, 1]) assert.equal(cell('products:name', v), '!name_blank', String(v));
  assert.deepEqual(['active', 'discontinued', '取扱中', 1].map((v) => cell('products:handling', v)), ['0', '1', '!handling_value', '!handling_value']);
  assert.deepEqual([0.1, 0.08, 10, 8, null].map((v) => cell('products:tax_rate', v)), ['10', '8', '!tax_value', '!tax_value', '!tax_value']);
  assert.deepEqual([1, 999999999, 0, 1e9, 1.5, '100', -1].map((v) => cell('products:cost', v)), ['1', '999999999', '!yen_range', '!yen_range', '!yen_range', '!yen_range', '!yen_range']);
  assert.deepEqual(['0135', '9999', '135', '12345', 135, ['0135']].map((v) => cell('products:primary_supplier', v)), ['0135', '9999', '!supplier_format', '!supplier_format', '!supplier_format', '!supplier_format']);
  // 親の名札は元の書き方 (大文字も。③b-1b) で渡される
  assert.deepEqual([null, 'p-100', 'a_b', 'empty', 'P-100', 'a.b', 'x'.repeat(31), ''].map((v) => cell('products:parent', v)), ['empty', 'p-100', 'a_b', '!parent_code', 'P-100', '!parent_code', '!parent_code', '!parent_code']);
  assert.equal(specOf('sets', 'tax_rate'), null); assert.equal(specOf('products', 'components'), null); assert.equal(specOf('__proto__', 'x'), null);
});

await ta('[2] CSV の byte 列: UTF-8・BOM なし・CRLF (最後の行も)・カンマと引用符だけ囲む', async () => {
  const r = buildCsv(COLUMNS['products:name'], [{ ne_code: 'a001', cell: 'A,名"前' }, { ne_code: 'b002', cell: 'ふつう' }]);
  assert.equal(r.bytes.toString('utf8'), 'syohin_code,syohin_name\r\na001,"A,名""前"\r\nb002,ふつう\r\n');
  assert.notEqual(r.bytes[0], 0xef);
  assert.equal(r.sha256, sha(r.bytes));
  // 引用符だけ (カンマなし) でも囲む・前後の空白も囲む
  assert.equal(buildCsv(COLUMNS['sets:name'], [{ ne_code: 's1', cell: 'Q"引用' }, { ne_code: 's2', cell: ' 空白' }]).bytes.toString('utf8'), 'set_syohin_code,set_syohin_name\r\ns1,"Q""引用"\r\ns2," 空白"\r\n');
});

await ta('[3] 対象: 単位の最後の判断が fix_ne・今日の回・列の表・書ける値 / NE の画面で直す / 古い承認を生き返らせない (H1)', async () => {
  const s = await summary();
  assert.equal(s.applied, true); assert.equal(s.today, true); assert.equal(s.run, R1);
  assert.deepEqual(csvCodes(s, 'products:name'), ['a001', 'q017', 't001', 't002', 't003', 't004', 't005', 't006', 't007']);   // p016 (P1 は後で別の指紋に却下)・m014 (差を残す)・n015 (今日の回に無い) は入らない
  assert.equal(group(s, 'products:name').csv, 9);
  assert.deepEqual(['products:tax_rate', 'products:handling', 'products:cost', 'products:primary_supplier', 'sets:name', 'sets:standard_price_jpy'].map((k) => csvCodes(s, k)), [['b002'], ['c003'], ['d004'], ['e005'], ['h008'], ['i009']]);
  assert.deepEqual(csvCodes(s, 'products:parent'), ['f006', 'g007']);
  const scr = Object.fromEntries(s.ne_screen.map((x) => [x.code_norm, x.reason]));
  assert.deepEqual(scr, { j010: 'col_not_csv', k012: 'empty_word', l013: 'col_not_csv', 'x.y1': 'code_chars', tm01: 'target_mismatch' });
  assert.equal(s.waiting, 1);   // n015
  assert.ok(!s.csv_items.some((x) => x.code_norm === 'p016' || x.code_norm === 'm014'));
  for (const g of s.groups) assert.equal(g.verified, false);
});

await ta('[4] 今日 (JST) の照合の回が無い = 作れない・確かめられない', async () => {
  now = at('2030-01-11', 10);
  let r = await create({ kind: 'products', col: 'name' });
  assert.equal(r.status, 400); assert.equal(r.j.reason, 'no_today_run');
  assert.equal((await summary()).today, false);
  assert.equal((await db.query('select count(*)::int as n from ops.ne_csv_exports')).rows[0].n, 0);
  now = at('2030-01-10', 10);
});

let E1, E2;
await ta('[5] 試し用 (5 行まで)・予約・選んだ承認だけ・ダウンロード', async () => {
  let r = await create({ kind: 'products', col: 'name', fingerprints: ['a001', 'q017', 't001', 't002', 't003', 't004'].map((l) => C[l].fingerprint) });
  assert.equal(r.status, 400); assert.equal(r.j.reason, 'trial_limit');
  r = await create({ kind: 'products', col: 'name' });
  assert.equal(r.status, 200);
  E1 = r.j.export.export_id;
  assert.deepEqual([r.j.export.trial, r.j.export.row_count, r.j.left], [true, TRIAL_ROWS, 4]);
  const f = await call('GET', `/api/csv/exports/${E1}/file`);
  assert.equal(f.status, 200);
  assert.equal(f.r.headers.get('content-type'), 'text/csv; charset=utf-8');
  assert.match(f.r.headers.get('content-disposition'), new RegExp(`attachment; filename="ne_products_name_20300110_100000_${E1}_trial\\.csv"`));
  assert.equal(f.text, 'syohin_code,syohin_name\r\na001,"A,名""前"\r\nq017,Q新\r\nt001,T新1\r\nt002,T新2\r\nt003,T新3\r\n');
  assert.equal(sha(f.buf), r.j.export.sha256);
  r = await create({ kind: 'products', col: 'name' });
  E2 = r.j.export.export_id;
  assert.deepEqual([r.j.export.row_count, r.j.left], [4, 0]);
  assert.equal(await fileOf(E2), 'syohin_code,syohin_name\r\nt004,T新4\r\nt005,T新5\r\nt006,T新6\r\nt007,T新7\r\n');
  r = await create({ kind: 'products', col: 'name' });
  assert.equal(r.j.reason, 'nothing_to_export');
  r = await create({ kind: 'products', col: 'name', fingerprints: [C.a001.fingerprint, C.k012.fingerprint, C.m014.fingerprint, H('nothing'), C.b002.fingerprint] });
  assert.equal(r.j.reason, 'not_eligible');
  assert.deepEqual(r.j.details.map((d) => [d.status, d.reason, d.export_id]), [['reserved', null, E1], ['ne_screen', 'empty_word', null], ['not_fix_ne', null, null], ['not_fix_ne', null, null], ['other_column', null, null]]);   // 税率の承認は名前のファイルに入れない
  const s = await summary();
  assert.equal(group(s, 'products:name').reserved, 9);
  assert.equal(s.csv_items.find((x) => x.code_norm === 'a001').export_id, E1);
  assert.deepEqual(byCode(await detail(E1)).a001.state, 'reserved');
  r = await create({ kind: 'products', col: 'name', encoding: 'sjis' });   // API は文字コードを受け取らない (utf8 だけ)
  assert.equal(r.j.reason, 'nothing_to_export');
  r = await create({ kind: 'products', col: 'zaiko_su' });
  assert.equal(r.j.reason, 'col_not_csv');
});

await ta('[6] 実機の確かめ: 最後の記録が ok の組だけ 1,000 行まで / ファイルと違う組は残さない', async () => {
  const v = (body) => call('POST', '/api/csv/verified', { body });
  let r = await v({ kind: 'products', col: 'tax_rate', result: 'ok', export_id: E1 });
  assert.equal(r.j.reason, 'export_mismatch');
  r = await v({ kind: 'products', col: 'name', result: 'ok', note: '試しで名前が変わった', export_id: E1 });
  assert.equal(r.status, 200);
  assert.equal(group(await summary(), 'products:name').verified, true);
  assert.equal(group(await summary(), 'products:name').limit, 1000);
  await v({ kind: 'products', col: 'name', result: 'ng' });
  assert.equal(group(await summary(), 'products:name').verified, false);
  await v({ kind: 'products', col: 'name', result: 'ok' });
  const s = await summary();
  assert.equal(group(s, 'products:name').verified, true);
  assert.equal(group(s, 'sets:name').verified, false);
  const row = (await db.query(`select header, converter_version, encoding, verified_by from ops.ne_csv_verified order by verified_id desc limit 1`)).rows[0];
  assert.deepEqual(row, { header: 'syohin_code,syohin_name', converter_version: 'ne-csv-v2', encoding: 'utf8', verified_by: 'naka@test' });
  r = await v({ kind: 'products', col: 'name', result: 'maybe' });
  assert.equal(r.status, 400);
});

const EX = {};
await ta('[7] 列ごとのファイルの中身 (単品・セット)', async () => {
  const want = {
    f006: ['products', 'parent', 'syohin_code,daihyo_syohin_code\r\nf006,empty\r\n'],
    g007: ['products', 'parent', 'syohin_code,daihyo_syohin_code\r\ng007,P-100\r\n'],   // 親の名札は元の書き方 (③b-1b)
    h008: ['sets', 'name', 'set_syohin_code,set_syohin_name\r\nH008,セットH\r\n'],        // セットのコードも元の書き方
    i009: ['sets', 'standard_price_jpy', 'set_syohin_code,set_baika_tnk\r\ni009,3000\r\n'],
    b002: ['products', 'tax_rate', 'syohin_code,tax_rate\r\nb002,10\r\n'],
    c003: ['products', 'handling', 'syohin_code,toriatukai_kbn\r\nc003,1\r\n'],
    d004: ['products', 'cost', 'syohin_code,genka_tnk\r\nd004,1200\r\n'],
    e005: ['products', 'primary_supplier', 'syohin_code,sire_code\r\ne005,0135\r\n'],
  };
  for (const [label, [kind, col, text]] of Object.entries(want)) {
    const r = await create({ kind, col, fingerprints: [C[label].fingerprint] });
    assert.equal(r.status, 200, `${label} ${JSON.stringify(r.j)}`);
    EX[label] = r.j.export.export_id;
    assert.equal(await fileOf(EX[label]), text, label);
  }
  const rows = (await db.query(`select source, approved_event_id::int as approved_event_id, target, cell, reserved from ops.ne_csv_export_rows where export_id = $1`, [EX.f006])).rows;
  assert.deepEqual(rows, [{ source: 'fix_ne', approved_event_id: EV.f006, target: { subject_key: 'parent:f006', col: 'parent', child: null, value: null }, cell: 'empty', reserved: true }]);
});

await ta('[8] 確かめる → 申告 / 確かめる前は申告できない / 申告のし直しは試みを足すだけ / 申告の後は確かめ直さない・void にしない', async () => {
  const id = EX.f006;
  let r = await declare(id);
  assert.equal(r.j.reason, 'not_checked');
  r = await check(id);
  assert.deepEqual([r.status, r.j.ok, r.j.passed, r.j.voided, r.j.run], [200, true, true, false, R1]);
  let d = await detail(id);
  assert.deepEqual([d.export.state, d.export.checked_today, d.export.checked_run], ['checked', true, R1]);
  r = await check(id);   // 同じ日の確かめ直しはよい
  assert.equal(r.j.passed, true);
  r = await declare(id, 'ok', '進捗状況で成功');
  assert.deepEqual(r.j.state, 'declared');
  const first = (await detail(id)).export.declared_at;
  now = at('2030-01-10', 11);
  r = await declare(id, 'partial', 'もう一度取り込んだ');
  assert.equal(r.j.first_declared_at, first);
  d = await detail(id);
  assert.equal(d.export.declared_at, first);
  assert.deepEqual(d.attempts.map((a) => [a.result, a.note]), [['ok', '進捗状況で成功'], ['partial', 'もう一度取り込んだ']]);
  assert.equal((await call('POST', `/api/csv/exports/${id}/void`, { body: {} })).j.reason, 'already_declared');
  assert.equal((await check(id)).j.reason, 'already_declared');
  assert.equal(byCode(d).f006.state, 'unconfirmable');   // 申告の後の照合がまだ
  for (const l of ['b002', 'c003', 'd004']) { assert.equal((await check(EX[l])).j.passed, true); assert.equal((await declare(EX[l])).j.state, 'declared'); }
  assert.equal((await check(EX.i009)).j.passed, true);   // 申告は次の日 (12 で「確かめたのが今日ではない」)
  r = await declare(id, 'bad');
  assert.equal(r.status, 400);
});

let E11;
await ta('[9] 判断の API: 新しい判断で予約を外し、まだ申告していないファイルを void (別の指紋でも同じ単位) → 作り直しに前の行 (H3)', async () => {
  // a001 の承認を取り消す = E1 (made) は void・a001 の行は取り消し・ほかの行も予約が外れる
  let r = await call('POST', '/api/decisions', { body: { kind: 'revoked', items: [{ fingerprint: C.a001.fingerprint, shown_event_id: EV.a001 }] } });
  assert.equal(r.status, 200, JSON.stringify(r.j));
  assert.deepEqual(r.j.applied[0].csv_voided, [E1]);
  let d = await detail(E1);
  assert.deepEqual([d.export.state, d.export.void_reason, d.export.void_by], ['void', 'superseded', 'naka@test']);
  assert.deepEqual(d.rows.map((x) => [x.code_norm, x.state, x.reason]), [['a001', 'cancelled', 'superseded'], ['q017', 'cancelled', 'void'], ['t001', 'cancelled', 'void'], ['t002', 'cancelled', 'void'], ['t003', 'cancelled', 'void']]);
  assert.equal((await call('GET', `/api/csv/exports/${E1}/file`)).status, 410);
  const s = await summary();
  assert.deepEqual(csvCodes(s, 'products:name'), ['q017', 't001', 't002', 't003']);   // a001 は取り消した = もう対象でない
  r = await create({ kind: 'products', col: 'name' });
  E11 = r.j.export.export_id;
  assert.equal(r.j.export.trial, false);   // [6] で実機の確かめ済み
  const prev = Object.fromEntries((await db.query(`select code_norm, row_id from ops.ne_csv_export_rows where export_id = $1`, [E1])).rows.map((x) => [x.code_norm, Number(x.row_id)]));
  d = await detail(E11);
  assert.deepEqual(d.rows.map((x) => [x.code_norm, x.prev_row_id]), [['q017', prev.q017], ['t001', prev.t001], ['t002', prev.t002], ['t003', prev.t003]]);
  // 別の指紋 (T4b = t004 の名前の別の値) が今日の回に出て、それを却下 = 同じ単位の t004 の承認は置き換わった → E2 は void
  now = at('2030-01-10', 12);
  const R1b = await writeRun('2030-01-10', 11, [...BASE, 'T4b']);
  r = await call('POST', '/api/decisions', { body: { kind: 'rejected', items: [{ fingerprint: C.T4b.fingerprint, shown_last_seen_run: R1b, shown_event_id: null }] } });
  assert.equal(r.status, 200, JSON.stringify(r.j));
  assert.deepEqual(r.j.applied[0].csv_voided, [E2]);
  // 確かめた後に新しい照合の回 (R1b) = 申告できない (確かめ直す) / 申告したファイル (f006) も同じ日でも「もう使えない」(#1495 Codex R1 High)
  assert.equal((await declare(EX.i009)).j.reason, 'check_stale');
  assert.deepEqual([(await detail(EX.i009)).export.checked_today, (await detail(EX.i009)).export.checked_current], [true, false]);
  assert.equal((await declare(EX.f006, 'ok')).j.reason, 'retired');
  assert.equal((await detail(EX.f006)).export.reusable, false);
  assert.deepEqual([(await call('GET', `/api/csv/exports/${EX.f006}/file`)).status, (await call('GET', `/api/csv/exports/${EX.f006}/file`)).j.reason], [410, 'retired']);
  assert.equal((await detail(EX.f006)).attempts.length, 2);   // 断った申告は試みに残さない
  d = await detail(E2);
  assert.deepEqual(d.rows.map((x) => [x.code_norm, x.state, x.reason]), [['t004', 'cancelled', 'superseded'], ['t005', 'cancelled', 'void'], ['t006', 'cancelled', 'void'], ['t007', 'cancelled', 'void']]);
  assert.deepEqual(csvCodes(await summary(), 'products:name'), ['t005', 't006', 't007']);   // t004 は後から別の指紋で判断 = 生き返らない (H1)
  // 確かめた (checked) ファイルも、新しい判断で void (取り込む前なら申告できない)
  const Ex = (await create({ kind: 'products', col: 'name' })).j.export.export_id;
  assert.equal((await check(Ex)).j.passed, true);
  r = await call('POST', '/api/decisions', { body: { kind: 'revoked', items: [{ fingerprint: C.t005.fingerprint, shown_event_id: EV.t005 }] } });
  assert.deepEqual(r.j.applied[0].csv_voided, [Ex]);
  assert.deepEqual([(await detail(Ex)).export.state, (await declare(Ex)).j.reason], ['void', 'void']);
  assert.deepEqual(csvCodes(await summary(), 'products:name'), ['t006', 't007']);
  // 申告したファイルの単位に新しい判断 = 行は取り消し・ファイルは申告のまま
  r = await call('POST', '/api/decisions', { body: { kind: 'revoked', items: [{ fingerprint: C.d004.fingerprint, shown_event_id: EV.d004 }] } });
  assert.equal(r.j.applied[0].csv_voided, undefined);
  d = await detail(EX.d004);
  assert.deepEqual([d.export.state, d.rows[0].state, d.rows[0].reason], ['declared', 'cancelled', 'superseded']);
  // もう一度承認 (新しい出来事) = 対象に戻る
  const revokeId = Number((await db.query(`select max(event_id) as m from ops.master_decision_events where fingerprint = $1`, [C.d004.fingerprint])).rows[0].m);
  EV.d004 = (await call('POST', '/api/decisions', { body: { kind: 'approved', resolution: 'fix_ne', items: [{ fingerprint: C.d004.fingerprint, shown_last_seen_run: R1b, shown_event_id: revokeId, target_value: 1200 }] } })).j.applied[0].event_id;
  assert.deepEqual(csvCodes(await summary(), 'products:cost'), ['d004']);
});

let R2;
await ta('[10] 直前の確かめで外れる (もう完了・今朝の照合に出ていない) = void', async () => {
  // 1 日目 20:00 の回 (全部出ている) → 2 日目 08:00 の回: b002・h008 が出ていない / c003・g007 は NE が目標の値になった (完了)
  now = at('2030-01-10', 21);
  const R1c = await writeRun('2030-01-10', 20, [...BASE]);
  assert.equal(byCode(await detail(EX.f006)).f006.state, 'unconfirmable');   // 申告と同じ日の回は数えない
  now = at('2030-01-11', 10);
  assert.equal((await check(EX.i009)).j.reason, 'no_today_run');   // 2 日目の照合の前 = 確かめられない (i009 は 1 日目に確かめたまま)
  R2 = await writeRun('2030-01-11', 8, BASE.filter((l) => !['b002', 'h008', 'c003', 'g007'].includes(l)));
  assert.equal(await done('c003', R2), true);
  assert.equal(await done('g007', R2), true);
  let r = await check(EX.g007);
  // 外れたのは通信の失敗ではない = HTTP 200・ok = true・passed = false (画面は理由を出して読み直す。#1495 Codex R1 Medium)
  assert.deepEqual([r.status, r.j.ok, r.j.passed, r.j.voided, r.j.failures.map((f) => [f.code_norm, f.reason])], [200, true, false, true, [['g007', 'done']]]);
  r = await check(EX.h008);
  assert.deepEqual(r.j.failures.map((f) => [f.code_norm, f.reason]), [['h008', 'waiting:not_current']]);
  assert.equal((await detail(EX.h008)).export.void_reason, 'check_failed');
  assert.equal(R1c.length > 0, true);
});

await ta('[11] 行の届き方: 次の日の回で 反映されていない / 確かめが要る / 確認済み → 作るときに予約を外す (M1・M2)', async () => {
  assert.deepEqual([byCode(await detail(EX.f006)).f006.state, byCode(await detail(EX.f006)).f006.run], ['not_reflected', R2]);   // まだ承認のときの値 (同じ指紋が出ている)
  assert.deepEqual([byCode(await detail(EX.b002)).b002.state, byCode(await detail(EX.b002)).b002.run], ['needs_look', R2]);     // 出ていない・完了も無い
  assert.equal(byCode(await detail(EX.c003)).c003.state, 'confirmed');
  let s = await summary();
  assert.deepEqual(csvCodes(s, 'products:parent'), ['f006']);   // 反映されていない = 作り直せる (画面は外れたものとして数える)
  const counts = s.exports.find((e) => e.export_id === EX.c003).row_states;
  assert.deepEqual(counts, { confirmed: 1 });
  const r = await create({ kind: 'products', col: 'parent' });
  assert.equal(r.status, 200, JSON.stringify(r.j));
  const d = await detail(r.j.export.export_id);
  const old = (await db.query(`select row_id, reserved, release_reason from ops.ne_csv_export_rows where export_id = $1`, [EX.f006])).rows[0];
  assert.deepEqual([old.reserved, old.release_reason], [false, 'not_reflected']);
  assert.equal(d.rows[0].prev_row_id, Number(old.row_id));
  const rel = Object.fromEntries((await db.query(`select code_norm, reserved, release_reason from ops.ne_csv_export_rows where export_id = any($1::bigint[])`, [[EX.b002, EX.c003]])).rows.map((x) => [x.code_norm, [x.reserved, x.release_reason]]));
  assert.deepEqual(rel, { b002: [false, 'needs_look'], c003: [false, 'confirmed'] });
  assert.equal(byCode(await detail(EX.f006)).f006.state, 'not_reflected');   // 予約を外しても届き方は同じ
  // 予約を外した旧いファイル (f006 = 作り直した・b002 = 確かめが要る) は、配らない・申告できない (#1495 Codex R1 High)
  for (const id of [EX.f006, EX.b002]) {
    assert.deepEqual([(await call('GET', `/api/csv/exports/${id}/file`)).status, (await declare(id, 'ok')).j.reason], [410, 'retired'], String(id));
  }
  assert.equal((await call('GET', `/api/csv/exports/${d.export.export_id}/file`)).status, 200);   // 作り直したファイルは配る
  s = await summary();
  assert.equal(csvCodes(s, 'products:tax_rate').length, 0);   // b002 は今日の回に出ていない = 待ち
});

await ta('[12] 確かめは同じ日のうちだけ (日をまたいだら確かめ直す)', async () => {
  let r = await declare(EX.i009);
  assert.equal(r.j.reason, 'check_stale');
  assert.equal((await detail(EX.i009)).export.checked_today, false);
  r = await check(EX.i009);
  assert.equal(r.j.passed, true);
  r = await declare(EX.i009);
  assert.equal(r.j.state, 'declared');
});

await ta('[13] 全部拒まれた = void (予約を外す・作り直せる)', async () => {
  assert.equal((await check(EX.e005)).j.passed, true);
  const r = await declare(EX.e005, 'rejected_all', '仕入先コードが無いと言われた');
  assert.equal(r.j.state, 'void');
  const d = await detail(EX.e005);
  assert.deepEqual([d.export.state, d.export.void_reason, d.export.declared_at, d.rows[0].state, d.attempts.length], ['void', 'rejected_all', null, 'cancelled', 1]);
  assert.deepEqual(csvCodes(await summary(), 'products:primary_supplier'), ['e005']);
});

await ta('[14] 使わない (void)・void は配らない・申告できない', async () => {
  const r = await call('POST', `/api/csv/exports/${E11}/void`, { body: {} });
  assert.equal(r.j.state, 'void');
  assert.equal((await detail(E11)).export.void_reason, 'by_user');
  assert.equal((await call('GET', `/api/csv/exports/${E11}/file`)).status, 410);
  assert.equal((await declare(E11)).j.reason, 'void');
  assert.equal((await check(E11)).j.reason, 'void');
  assert.equal((await call('POST', `/api/csv/exports/${E11}/void`, { body: {} })).j.reason, 'void');
  assert.equal((await call('GET', '/api/csv/exports/999999')).status, 404);
  assert.equal((await check(0)).status, 400);
});

await ta('[15] 表の守り: 中身は書き換えない・消さない・予約の一意・watcher は読むだけ', async () => {
  const bad = async (sql, params, re) => { await assert.rejects(pg.query(sql, params), re, sql); };
  await bad(`update ops.ne_csv_exports set file_bytes = '\\x00' where export_id = $1`, [EX.f006], /書き換えない/);
  await bad(`update ops.ne_csv_exports set sha256 = repeat('0', 64) where export_id = $1`, [EX.f006], /書き換えない/);
  await bad(`delete from ops.ne_csv_exports where export_id = $1`, [EX.f006], /消さない/);
  await bad(`update ops.ne_csv_exports set state = 'made', declared_at = null where export_id = $1`, [EX.f006], /戻さない|ck_ne_csv/);
  await bad(`update ops.ne_csv_exports set declared_at = now() where export_id = $1`, [EX.f006], /動かさない/);
  await bad(`update ops.ne_csv_exports set void_reason = 'x' where export_id = $1`, [E1], /void のファイルは変えない/);
  await bad(`update ops.ne_csv_export_rows set cell = 'x' where export_id = $1`, [E11], /書き換えない/);
  await bad(`update ops.ne_csv_export_rows set reserved = true, released_at = null, release_reason = null where export_id = $1`, [E11], /戻さない/);
  await bad(`delete from ops.ne_csv_export_rows where export_id = $1`, [E11], /消さない/);
  await bad(`update ops.ne_csv_attempts set note = 'x'`, [], /./);
  await bad(`delete from ops.ne_csv_verified`, [], /./);
  // 有効な予約は (SKU・列・子) ごとに 1 つ (API を通らない書き込みでも)
  const live = (await pg.query(`select * from ops.ne_csv_export_rows where reserved limit 1`)).rows[0];
  await bad(`insert into ops.ne_csv_export_rows (export_id, source, approved_event_id, fingerprint, code_norm, col, child, ne_code, target, cell) values ($1, 'fix_ne', $2, $3, $4, $5, $6, $7, $8, $9)`,
    [live.export_id, live.approved_event_id, live.fingerprint, live.code_norm, live.col, live.child, live.ne_code, JSON.stringify(live.target), live.cell], /ux_ne_csv_rows_reserved|duplicate/);
  await bad(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell) values ($1, 'fix_ne', 'zz', 'name', 'zz', '{}', 'x')`, [live.export_id], /ck_ne_csv_row_fix_ne/);
  await bad(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell) values ($1, 'to_ne', 'zz', 'name', 'zz', '{}', 'x')`, [live.export_id], /ck_ne_csv_row_to_ne/);
  await bad(`insert into ops.ne_csv_export_rows (export_id, source, approved_event_id, fingerprint, code_norm, col, ne_code, target, cell) values ($1, 'fix_ne', $2, $3, 'zz', 'name', 'ZY', '{}', 'x')`, [live.export_id, live.approved_event_id, live.fingerprint], /ck_ne_csv_row_ne_code/);   // 大文字は通る (③b-1b)・小文字にして norm と合わないものは拒む
  await pg.query('set role watcher');
  try {
    assert.ok((await pg.query('select count(*)::int as n from ops.ne_csv_exports')).rows[0].n > 0);
    await assert.rejects(pg.query(`insert into ops.ne_csv_verified (kind, col, encoding, header, converter_version, result, verified_by) values ('products', 'name', 'utf8', 'h', 'v', 'ok', 'w')`), /permission denied/);
    await assert.rejects(pg.query(`update ops.ne_csv_export_rows set reserved = false`), /permission denied/);
    await assert.rejects(pg.query(`update ops.ne_csv_exports set state = 'void'`), /permission denied/);
  } finally { await pg.query('set role deploy'); }
});

await ta('[16] 名簿の人だけ (作る・配る・確かめる・申告・void・実機の確かめ)・Origin・見るのは誰でも', async () => {
  const s = await call('GET', '/api/csv/summary', { session: 'user' });
  assert.deepEqual([s.status, s.j.can_decide], [200, false]);
  assert.equal((await call('GET', `/api/csv/exports/${EX.f006}`, { session: 'user' })).status, 200);
  for (const [m, u, body] of [['POST', '/api/csv/exports', { kind: 'products', col: 'name' }], ['GET', `/api/csv/exports/${EX.f006}/file`], ['POST', `/api/csv/exports/${EX.f006}/check`, {}],
    ['POST', `/api/csv/exports/${EX.f006}/declare`, { result: 'ok' }], ['POST', `/api/csv/exports/${EX.f006}/void`, {}], ['POST', '/api/csv/verified', { kind: 'products', col: 'name', result: 'ok' }]]) {
    const r = await call(m, u, { body, session: 'user' });
    assert.equal(r.status, 403, `${m} ${u}`); assert.equal(r.j.reason, 'not_approver');
  }
  assert.equal((await call('POST', '/api/csv/exports', { body: { kind: 'products', col: 'name' }, origin: false })).status, 403);
  const saved = process.env.MASTER_DECISION_APPROVERS; process.env.MASTER_DECISION_APPROVERS = '';
  try { assert.equal((await call('GET', `/api/csv/exports/${EX.f006}/file`)).status, 403); } finally { process.env.MASTER_DECISION_APPROVERS = saved; }
});

await ta('[17] 画面とつかいかた: 画面の JS が読める・API を呼ぶ・ボタンの言葉が全部つかいかたにある・判断の画面からのリンク', async () => {
  const r = await call('GET', '/csv');
  assert.equal(r.status, 200); assert.match(r.text, /NE に取り込む CSV/); assert.match(r.text, /data-can-decide="1"/);
  const scripts = [...r.text.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((x) => x[1]);
  assert.equal(scripts.length, 1);
  new vm.Script(scripts[0]);
  for (const api of ['api/csv/summary', 'api/csv/exports', 'api/csv/exports/', 'api/csv/verified']) assert.ok(scripts[0].includes(`'${api}`), `画面が ${api} を呼んでいない`);
  for (const act of ["/check'", "/declare'", "/void'", "/file\""]) assert.ok(scripts[0].includes(act), act);
  for (const k of ['r.passed', 'e.reusable', 'e.checked_current']) assert.ok(scripts[0].includes(k), `画面が ${k} を見ていない`);
  assert.ok(!/r\.ok \?/.test(scripts[0]), '確かめの結果を HTTP の ok で見ている');
  assert.match((await call('GET', '/csv', { session: 'user' })).text, /data-can-decide="0"/);
  const page = fs.readFileSync(new URL('../apps/master-decisions/views/csv.ejs', import.meta.url), 'utf8');
  const buttons = ['CSV を作る', '選んだものだけで CSV を作る', 'ダウンロード', '取り込む直前に確かめる', '取り込んだと申告', '使わない (void)', '実機で確かめた結果を残す'];
  for (const b of buttons) assert.ok(page.includes(`>${b}<`), `画面に「${b}」が無い`);
  const m = (await call('GET', '/manual')).text;
  for (const b of [...buttons, '取り込まないでください', '反映されていない', '確かめが要る', '確認済み', '確認できない']) assert.ok(m.includes(b), `つかいかたに「${b}」が無い`);
  assert.match(m, /id="csv"/);
  assert.match((await call('GET', '/')).text, /href="csv"/);
});

await ta('[18] 0040 の前の DB: 判断の API は今までどおり・CSV は「まだ入っていない」', async () => {
  const old = await freshDb({ to: '0039' });   // CSV の表 (0040) の前 = 0039 (広告の効き目の直し) まで
  const c = C.q017;
  await writeDecisions(old.db, { compareRunId: 'mc_20300110T000000999Z_abcdef', observedAt: new Date(at('2030-01-10', 8)).toISOString(), decisions: [c] });
  const r = await applyDecisions(old.db, { actor: 'naka@test', kind: 'approved', resolution: 'fix_ne', items: [{ fingerprint: c.fingerprint, shown_last_seen_run: 'mc_20300110T000000999Z_abcdef', shown_event_id: null, target_value: 'Q新' }] });
  assert.equal(r.applied.length, 1); assert.equal(r.applied[0].csv_voided, undefined);
  assert.deepEqual(await csvSummary(old.db, { nowMs: at('2030-01-10', 10) }), { applied: false });
  await assert.rejects(createExport(old.db, { actor: 'naka@test', kind: 'products', col: 'name', nowMs: at('2030-01-10', 10) }), (e) => e.reason === 'not_applied');
  await old.pg.close();
});

await ta('[20] 候補の行を取った後に照合の回を読み直す: 確かめる・作るの途中に新しい回 (その指紋が出ない) が入ったら、古い回で通さない (#1495 Codex R1 High)', async () => {
  const f = await freshDb();
  const c1 = C.q017, c2 = C.t001;
  const run1 = 'mc_20300110T000000501Z_abcdef', run2 = 'mc_20300110T000000502Z_abcdef', run3 = 'mc_20300110T000000503Z_abcdef';
  await writeDecisions(f.db, { compareRunId: run1, observedAt: new Date(at('2030-01-10', 8)).toISOString(), decisions: [c1, c2] });
  await recordCodes(f.db, run1, [prod('q017'), prod('t001')]);
  for (const c of [c1, c2]) {
    await f.pg.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'setup@test')`,
      [c.fingerprint, JSON.stringify({ subject_key: c.subject_key, col: c.col, child: null, value: '新しい名前' })]);
  }
  const nowMs = at('2030-01-10', 10);
  // 候補の行を取る文の直前に、照合が新しい回を書いた (同じ接続の取引の中 = 後の読みに見える)
  const hooked = (runId, hh, decisions) => {
    let fired = false;
    return { query: async (t, p) => {
      if (!fired && /for update/.test(t) && /master_decision_candidates/.test(t)) {
        fired = true;
        await writeDecisions(f.db, { compareRunId: runId, observedAt: new Date(at('2030-01-10', hh)).toISOString(), decisions });
      }
      return f.db.query(t, p);
    } };
  };
  const ex = (await createExport(f.db, { actor: 'naka@test', kind: 'products', col: 'name', fingerprints: [c1.fingerprint], nowMs })).export.export_id;
  const r = await csvMod.checkExport(hooked(run2, 9, [c2]), { actor: 'naka@test', exportId: ex, nowMs });   // 新しい回に q017 は出ない
  assert.deepEqual([r.passed, r.run, r.failures.map((x) => x.reason)], [false, run2, ['waiting:not_current']]);
  await assert.rejects(createExport(hooked(run3, 10, []), { actor: 'naka@test', kind: 'products', col: 'name', nowMs }), (e) => e.reason === 'nothing_to_export');   // 新しい回に t001 も出ない
  assert.equal((await f.pg.query('select count(*)::int as n from ops.ne_csv_exports')).rows[0].n, 1);
  await f.pg.close();
});

await ta('[21] 申告したファイルをもう一度使える条件を 1 つずつ: 同じ日・確かめた後に新しい回が無い・予約が外れていない (#1495 Codex R1 High)', async () => {
  const f = await freshDb();
  const c = C.q017;
  const run1 = 'mc_20300110T000000601Z_abcdef';
  await writeDecisions(f.db, { compareRunId: run1, observedAt: new Date(at('2030-01-10', 8)).toISOString(), decisions: [c] });
  await recordCodes(f.db, run1, [prod('q017')]);
  const ev = Number((await f.pg.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'setup@test') returning event_id`,
    [c.fingerprint, JSON.stringify({ subject_key: c.subject_key, col: c.col, child: null, value: '新しい名前' })])).rows[0].event_id);
  const d1 = at('2030-01-10', 10), d2 = at('2030-01-11', 10);
  const ex = (await createExport(f.db, { actor: 'naka@test', kind: 'products', col: 'name', nowMs: d1 })).export.export_id;
  assert.equal((await csvMod.checkExport(f.db, { actor: 'naka@test', exportId: ex, nowMs: d1 })).passed, true);
  assert.equal((await csvMod.declareExport(f.db, { actor: 'naka@test', exportId: ex, result: 'partial', nowMs: d1 })).state, 'declared');
  const reuse = async (nowMs) => {
    const r = await csvMod.declareExport(f.db, { actor: 'naka@test', exportId: ex, result: 'ok', nowMs }).then((x) => x.state, (e) => e.reason);
    const file = (await csvMod.exportFile(f.db, ex, { nowMs })).state;
    return [r, file, (await csvMod.exportDetail(f.db, ex, { nowMs })).export.reusable];
  };
  assert.deepEqual(await reuse(d1), ['declared', 'declared', true]);        // 同じ日・同じ回・予約あり = 使える (取り込み直しの試み)
  assert.deepEqual(await reuse(d2), ['retired', 'retired', false]);         // 次の日 (新しい回はまだ無い) = 使えない
  // 同じ日のまま、予約だけ外れた (判断の画面で取り消し) = 使えない
  const r = await applyDecisions(f.db, { actor: 'naka@test', kind: 'revoked', items: [{ fingerprint: c.fingerprint, shown_event_id: ev }] });
  assert.equal(r.applied.length, 1);
  assert.deepEqual(await reuse(d1), ['retired', 'retired', false]);
  assert.equal((await f.pg.query('select count(*)::int as n from ops.ne_csv_attempts where export_id = $1', [ex])).rows[0].n, 2);   // 断った申告は残さない
  await f.pg.close();
});

await ta('[22] 画面の JS を動かす: ファイルの状態ごとのボタン・確かめで外れたら理由を出して読み直す (#1495 Codex R1 Medium)', async () => {
  const html = (await call('GET', '/csv')).text;
  const script = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((x) => x[1])[0];
  const els = new Map();
  const el = (id) => { if (!els.has(id)) els.set(id, { id, innerHTML: '', textContent: '', className: '', value: '', disabled: false, dataset: {}, onclick: null, onchange: null }); return els.get(id); };
  const clickable = [];
  const qsa = (sel) => {
    const m = sel.match(/^\[data-([a-z]+)\]$/);
    if (!m) return [];
    const all = [...els.values()].map((e) => e.innerHTML).join('\n');
    return [...all.matchAll(new RegExp(`data-${m[1]}="([^"]*)"`, 'g'))].map((x) => { const e = { dataset: { [m[1]]: x[1] }, onclick: null }; clickable.push(e); return e; });
  };
  const exp = (id, state, extra = {}) => ({ export_id: id, state, kind: 'products', col: 'name', trial: false, row_count: 1, sha256: 'a'.repeat(64), created_at: '2030-01-10 10:00:00+09', created_by: 'naka@test',
    file_name: `f${id}.csv`, checked_today: false, checked_current: false, reusable: false, row_states: { reserved: 1 }, void_reason: null, ...extra });
  const summaryBody = { ok: true, applied: true, today: true, run: 'mc_x', observed_at: '2030-01-10 08:00:00+09', today_jst: '2030-01-10', groups: [], csv_items: [], ne_screen: [], waiting: 0, verified: [], can_decide: true,
    exports: [exp(5, 'declared'), exp(6, 'declared', { reusable: true }), exp(7, 'checked', { checked_today: true, checked_current: true }), exp(8, 'checked', { checked_today: true }), exp(9, 'made')] };
  const calls = [];
  const fetchStub = async (url, opts = {}) => {
    calls.push([url, opts.method || 'GET']);
    const body = url === 'api/csv/summary' ? summaryBody
      : url === 'api/csv/exports/9/check' ? { ok: true, passed: false, voided: true, failures: [{ code_norm: 'x1', reason: 'done' }], run: 'mc_x' } : { ok: false, error: '?' };
    return { ok: true, status: 200, json: async () => body };
  };
  const document = { body: { dataset: { canDecide: '1', gateMessage: '' } }, getElementById: el, querySelectorAll: qsa, querySelector: () => ({ value: 'ok' }) };
  vm.runInNewContext(script, { document, fetch: fetchStub, URLSearchParams, confirm: () => true, prompt: () => null, console });
  const settle = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 5)); };
  await settle();
  const ex = el('exports').innerHTML;
  const has = (s) => ex.includes(s);
  // 5 = 使えない申告済み (配らない・申告しない) / 6 = まだ使える申告済み / 7 = 今の回で確かめた / 8 = 確かめた後に新しい回 / 9 = 作った
  assert.deepEqual([has('data-declare="5"'), has('/5/file'), has('data-declare="6"'), has('/6/file'), has('data-declare="7"'), has('data-check="7"'), has('data-declare="8"'), has('data-check="8"'), has('data-check="9"')],
    [false, false, true, true, true, false, false, true, true]);
  assert.ok(has('もう使えない') && has('確かめた後に新しい照合があった'));
  const before = calls.filter((c) => c[0] === 'api/csv/summary').length;
  clickable.find((e) => e.dataset.check === '9').onclick({ stopPropagation() {} });
  await settle();
  assert.equal(el('msg').className, 'msg err');
  assert.match(el('msg').textContent, /使えなくしました/); assert.match(el('msg').textContent, /x1/);
  assert.equal(calls.filter((c) => c[0] === 'api/csv/summary').length, before + 1, '確かめで外れた後に一覧を読み直していない');
});

await ta('[23] 一覧: まだ終わっていないファイル (作った・確かめた・予約が残る) は古くても出る + ほかは最近の 30 件 (#1495 Codex R2 Medium)', async () => {
  const f = await freshDb();
  const c1 = C.q017, c2 = C.t001;
  const run1 = 'mc_20300110T000000701Z_abcdef';
  await writeDecisions(f.db, { compareRunId: run1, observedAt: new Date(at('2030-01-10', 8)).toISOString(), decisions: [c1, c2] });
  await recordCodes(f.db, run1, [prod('q017'), prod('t001')]);
  for (const c of [c1, c2]) {
    await f.pg.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'setup@test')`,
      [c.fingerprint, JSON.stringify({ subject_key: c.subject_key, col: c.col, child: null, value: '新しい名前' })]);
  }
  const d1 = at('2030-01-10', 10);
  const made = (await createExport(f.db, { actor: 'naka@test', kind: 'products', col: 'name', fingerprints: [c1.fingerprint], nowMs: d1 })).export.export_id;
  const decl = (await createExport(f.db, { actor: 'naka@test', kind: 'products', col: 'name', fingerprints: [c2.fingerprint], nowMs: d1 })).export.export_id;
  await csvMod.checkExport(f.db, { actor: 'naka@test', exportId: decl, nowMs: d1 });
  await csvMod.declareExport(f.db, { actor: 'naka@test', exportId: decl, result: 'ok', nowMs: d1 });   // 申告済み・翌朝の照合待ち = 予約が残る
  // その後に 35 個のファイル (終わったもの = void) が作られた
  for (let i = 0; i < 35; i++) {
    await f.pg.query(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by, state, void_at, void_reason)
      values ('products', 'name', 'syohin_name', 'ne-csv-v1', 'utf8', true, 1, repeat('a', 64), '\\x41', $1, 'test', 'void', now(), 'by_user')`, [run1]);
  }
  const s = await csvSummary(f.db, { nowMs: d1 });
  const ids = s.exports.map((e) => e.export_id);
  assert.ok(ids.includes(made) && ids.includes(decl), JSON.stringify(ids));
  assert.equal(ids.length, 32);   // 全体の最近の 30 件 (全部 void) + まだ終わっていない 2 件 (古いので重ならない)
  assert.deepEqual(ids, [...ids].sort((a, b) => b - a));
  assert.equal(s.csv_items.find((x) => x.code_norm === 'q017').export_id, made);   // 「予約中 (ファイル N)」のファイルは一覧にある
  await f.pg.close();
});

await ta('[24] NE の元のコード (③b-1b・0041): 今日の回の記録だけ・分からない / 衝突 / 使えない / 古い = NE の画面で直す・大文字のコードと親の名札を元の書き方で書く・書き方が変わったら確かめで外れる', async () => {
  const f = await freshDb();
  mk('AB1', 'value:ab-1', 'name', 'single');
  mk('CD2', 'value:cd-2', 'name', 'single');
  mk('EF3', 'value:ef-3', 'name', 'single');
  mk('GH4', 'value:gh-4', 'name', 'single');
  mk('PA1', 'parent:pa-1', 'parent', 'single');
  mk('PA2', 'parent:pa-2', 'parent', 'single');
  mk('PA3', 'parent:pa-3', 'parent', 'single');
  mk('PA4', 'parent:pa-4', 'parent', 'single');
  mk('PA5', 'parent:pa-5', 'parent', 'single');
  const labels = ['AB1', 'CD2', 'EF3', 'GH4', 'PA1', 'PA2', 'PA3', 'PA4', 'PA5'];
  const run1 = 'mc_20300110T000000801Z_abcdef', run2 = 'mc_20300110T000000802Z_abcdef', run3 = 'mc_20300110T000000803Z_abcdef';
  const d1 = at('2030-01-10', 12);
  await writeDecisions(f.db, { compareRunId: run1, observedAt: new Date(at('2030-01-10', 8)).toISOString(), decisions: labels.map((l) => C[l]) });
  const ev = {};
  const approve = async (l, value) => { const c = C[l]; ev[l] = Number((await f.pg.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'setup@test') returning event_id`,
    [c.fingerprint, JSON.stringify({ subject_key: c.subject_key, col: c.col, child: null, value })])).rows[0].event_id); };
  for (const l of ['AB1', 'CD2', 'EF3', 'GH4']) await approve(l, `${l} の新しい名前`);
  await approve('PA1', 'grp-a'); await approve('PA2', 'grp-b'); await approve('PA3', 'grp-c'); await approve('PA4', 'ab-1'); await approve('PA5', 'pa-1');
  const reasons = async (nowMs = d1) => Object.fromEntries((await csvSummary(f.db, { nowMs })).ne_screen.filter((x) => labels.some((l) => C[l].code_norm === x.code_norm)).map((x) => [x.code_norm, x.reason]));
  // 0041 はあるが、まだ一度も記録が無い = 全部 NE の画面で直す (小文字で書かない)
  let s = await csvSummary(f.db, { nowMs: d1 });
  assert.deepEqual(s.ne_codes, { applied: true, run: null, current: false });
  assert.deepEqual(new Set(Object.values(await reasons())), new Set(['ne_code_pending']));
  const codes = [
    prod('ab-1', 'AB-1'), prod('pa-1', 'PA-1'), prod('pa-2'), prod('pa-3'), prod('pa-4'), prod('pa-5'),
    { code_norm: 'ab-1', kind: 'rep', state: 'ok', ne_code: 'aB-1', spellings: ['aB-1'] },                          // 名札 ab-1 の書き方は商品 AB-1 と違う = 名札が勝つ
    { code_norm: 'cd-2', kind: 'product', state: 'collided', ne_code: null, spellings: ['CD-2', 'cd-2'] },
    { code_norm: 'ef-3', kind: 'product', state: 'invalid', ne_code: null, spellings: ['ef-3 '] },
    { code_norm: 'grp-a', kind: 'rep', state: 'ok', ne_code: 'GRP-A', spellings: ['GRP-A'] },                        // 名札あり = 名札の書き方
    { code_norm: 'grp-b', kind: 'rep', state: 'collided', ne_code: null, spellings: ['GRP-B', 'grp-b'] },             // 名札が衝突 = 画面で
    // grp-c = 名札も商品も無い = 画面で / pa-1 = 名札は無いが商品がある = 商品の書き方 PA-1
  ];
  await recordCodes(f.db, run1, codes);
  s = await csvSummary(f.db, { nowMs: d1 });
  assert.equal(s.ne_codes.current, true);
  assert.deepEqual(await reasons(), { 'cd-2': 'ne_code_collided', 'ef-3': 'ne_code_invalid', 'gh-4': 'ne_code_unknown', 'pa-2': 'parent_code_unknown', 'pa-3': 'parent_code_unknown' });
  let r = await createExport(f.db, { actor: 'naka@test', kind: 'products', col: 'name', nowMs: d1 });
  assert.equal(csvMod.buildCsv(COLUMNS['products:name'], []).header, 'syohin_code,syohin_name');
  const nameEx = r.export.export_id;
  assert.equal((await csvMod.exportFile(f.db, nameEx, { nowMs: d1 })).bytes.toString('utf8'), 'syohin_code,syohin_name\r\nAB-1,AB1 の新しい名前\r\n');
  assert.deepEqual((await f.pg.query('select ne_code, code_norm from ops.ne_csv_export_rows where export_id = $1', [nameEx])).rows, [{ ne_code: 'AB-1', code_norm: 'ab-1' }]);
  r = await createExport(f.db, { actor: 'naka@test', kind: 'products', col: 'parent', nowMs: d1 });
  assert.equal((await csvMod.exportFile(f.db, r.export.export_id, { nowMs: d1 })).bytes.toString('utf8'), 'syohin_code,daihyo_syohin_code\r\nPA-1,GRP-A\r\npa-4,aB-1\r\npa-5,PA-1\r\n');
  // 次の回で ab-1 の書き方が変わった (Ab-1)・候補はそのまま = 直前の確かめで外れる (void)
  await writeDecisions(f.db, { compareRunId: run2, observedAt: new Date(at('2030-01-10', 9)).toISOString(), decisions: labels.map((l) => C[l]) });
  await recordCodes(f.db, run2, [prod('ab-1', 'Ab-1'), ...codes.slice(1)]);
  const ck = await csvMod.checkExport(f.db, { actor: 'naka@test', exportId: nameEx, nowMs: d1 });
  assert.deepEqual([ck.passed, ck.failures.map((x) => x.reason)], [false, ['value_changed']]);
  // 新しい回の元の書き方がまだ無い (照合は書けたが元の書き方は書けなかった) = 全部画面で (前の回の書き方を使わない)
  await writeDecisions(f.db, { compareRunId: run3, observedAt: new Date(at('2030-01-10', 10)).toISOString(), decisions: labels.map((l) => C[l]) });
  assert.deepEqual(new Set(Object.values(await reasons())), new Set(['ne_code_pending']));
  await f.pg.close();
});

await ta('[25] ops.record_ne_codes (0041): 照合の回の記録が要る・古い回は拒む・同じ回は同じ中身 (順に依らない) だけ・重複と形の誤りを拒む・watch_writer は実行だけ', async () => {
  const f = await freshDb();
  await createRoles(f.pg, { watcherPw: 'a', writerPw: 'b' });
  const rec = (run, entries) => f.pg.query('select ops.record_ne_codes($1::jsonb) as r', [JSON.stringify({ compare_run_id: run, entries })]).then((x) => x.rows[0].r);
  const runA = 'mc_20300110T000000901Z_abcdef', runB = 'mc_20300110T000000902Z_abcdef', runC = 'mc_20300110T000000903Z_abcdef', runD = 'mc_20300110T000000904Z_abcdef';
  const E = [prod('aa-1', 'AA-1'), prod('bb-2'), { code_norm: 'grp', kind: 'rep', state: 'ok', ne_code: 'Grp', spellings: ['Grp'] }];
  await assert.rejects(rec(runA, E), /unknown_run/);
  await writeDecisions(f.db, { compareRunId: runA, observedAt: new Date(at('2030-01-10', 8)).toISOString(), decisions: [] });
  await writeDecisions(f.db, { compareRunId: runB, observedAt: new Date(at('2030-01-10', 9)).toISOString(), decisions: [] });
  await writeDecisions(f.db, { compareRunId: runC, observedAt: new Date(at('2030-01-10', 9)).toISOString(), decisions: [] });   // B と同じ時刻・別の回
  await writeDecisions(f.db, { compareRunId: runD, observedAt: new Date(at('2030-01-10', 10)).toISOString(), decisions: [] });
  assert.equal((await rec(runB, E)).state, 'written');
  assert.equal((await rec(runB, [...E].reverse())).state, 'unchanged');                      // 同じ回・同じ中身 (順が違う) = 何もしない
  await assert.rejects(rec(runB, [prod('aa-1', 'Aa-1'), ...E.slice(1)]), /run_conflict/);     // 同じ回・中身が違う
  await assert.rejects(rec(runA, E), /stale_run/);                                            // 古い回
  assert.equal((await rec(runC, E)).state, 'written');                                        // 同じ時刻で ID が後 = 新しい (照合の回の最新の決め方と同じ)
  await assert.rejects(rec(runB, E), /stale_run/);                                            // 同じ時刻で ID が前 = 古い
  await assert.rejects(rec(runD, [prod('aa-1'), prod('aa-1')]), /重複/);
  await assert.rejects(rec(runD, [{ code_norm: 'x1', kind: 'product', state: 'ok', ne_code: null, spellings: [] }]), /ck_mnc_state/);
  await assert.rejects(rec(runD, [{ code_norm: 'x1', kind: 'product', state: 'ok', ne_code: 'Y1', spellings: ['Y1'] }]), /ck_mnc_code/);
  await assert.rejects(rec(runD, [{ code_norm: 'x1', kind: 'product', state: 'ok', ne_code: 'X 1', spellings: ['X 1'] }]), /ck_mnc_code/);
  await assert.rejects(rec(runD, [{ code_norm: 'x1', kind: 'product', state: 'ok', ne_code: 'X1' }]), /項目が足りない/);
  const mark = (await f.pg.query('select compare_run_id, observed_at::text as at from ops.master_ne_code_mark')).rows;
  assert.deepEqual(mark.map((m) => m.compare_run_id), [runC]);   // 拒まれた回は何も変えない
  assert.equal((await f.pg.query('select count(*)::int as n from ops.master_ne_codes')).rows[0].n, 3);
  // 権限: watch_writer は関数の実行だけ (表へ直接は書けない)・watcher は読むだけ・public は実行できない
  await f.pg.query('set role watch_writer');
  try {
    assert.equal((await rec(runD, E)).state, 'written');
    await assert.rejects(f.pg.query("insert into ops.master_ne_codes (code_norm, kind, state, ne_code, spellings) values ('z', 'product', 'ok', 'z', '[\"z\"]')"), /permission denied/);
  } finally { await f.pg.query('set role deploy'); }
  await f.pg.query('set role watcher');
  try {
    assert.equal((await f.pg.query('select count(*)::int as n from ops.master_ne_codes')).rows[0].n, 3);
    await assert.rejects(rec(runD, E), /permission denied/);
  } finally { await f.pg.query('set role deploy'); }
  const pub = (await f.pg.query(`select has_function_privilege('public', 'ops.record_ne_codes(jsonb)', 'execute') as x`)).rows[0].x;
  assert.equal(pub, false);
  // ne_csv_export_rows の ne_code は大文字も通る・norm と合わないものは拒む
  await assert.rejects(f.pg.query(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell) values (1, 'to_ne', 'ab', 'name', 'XY', '{}', 'x')`), /ck_ne_csv_row_ne_code|ck_ne_csv_row_to_ne|foreign key/);
  await f.pg.close();
});

await ta('[19] migration の権限: watcher が先にいる DB (本番と同じ) では 4 つの表を読むだけ', async () => {
  const p = new PGlite();
  await p.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await p.query(`create role watcher nologin`);
  await p.query(`alter database ${(await p.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await p.query('set role deploy');
  await applyMigrations(pgliteAdapter(p), { log: quiet });
  for (const t of ['ops.ne_csv_exports', 'ops.ne_csv_export_rows', 'ops.ne_csv_attempts', 'ops.ne_csv_verified', 'ops.master_ne_codes', 'ops.master_ne_code_mark']) {
    const r = (await p.query(`select has_table_privilege('watcher', $1, 'select') as s, has_table_privilege('watcher', $1, 'insert') as i, has_table_privilege('watcher', $1, 'update') as u, has_table_privilege('watcher', $1, 'delete') as d`, [t])).rows[0];
    assert.deepEqual(r, { s: true, i: false, u: false, d: false }, t);
  }
  await p.close();
});

await ta('[26] 2026-10-05: 名前 = 商品コード は承認されていても CSV に入れない (judge = NE の画面へ name_is_code・buildCsv も拒む) / 税率は % の整数 (10 / 8) だけ・0.1 のような値は CSV に出ない / 売価・原価の 0 は書かない', async () => {
  const run = 'mc_20300301T000000001Z_abcdef';
  const neCodes = { run, map: new Map([['product|akadama-big-2l-2', { state: 'ok', ne_code: 'akadama-big-2l-2' }], ['product|abc-1', { state: 'ok', ne_code: 'ABC-1' }], ['product|t-1', { state: 'ok', ne_code: 't-1' }]]) };
  const u = (code, col, value, kind = 'set') => ({ fingerprint: H(`26-${code}-${col}`), event_id: 1, subject_key: `value:${code}`, code_norm: code, col, child: null, last_seen_run: run, done: false,
    print: { sku_kind: kind, n: null, c: value }, target: { subject_key: `value:${code}`, col, child: null, value } });
  const j = (x) => csvMod.judge(x, { run, reservations: new Map(), neCodes });
  for (const [x, why] of [[u('akadama-big-2l-2', 'name', 'akadama-big-2l-2'), 'name_is_code'], [u('akadama-big-2l-2', 'name', 'AKADAMA-BIG-2L-2'), 'name_is_code'],
    [u('abc-1', 'name', 'ABC-1', 'single'), 'name_is_code'], [u('abc-1', 'name', 'ａｂｃ－１', 'single'), 'name_is_code'],
    [u('akadama-big-2l-2', 'standard_price_jpy', 0), 'yen_range'], [u('abc-1', 'cost', 0, 'single'), 'yen_range'], [u('abc-1', 'tax_rate', 10, 'single'), 'tax_value']]) {
    const r = j(x);
    assert.deepEqual([r.status, r.reason], ['ne_screen', why], `${x.code_norm} ${x.col} ${x.target.value}`);
  }
  assert.deepEqual([j(u('akadama-big-2l-2', 'name', '赤玉土 大粒 2L 2 個')).status, j(u('akadama-big-2l-2', 'name', '赤玉土 大粒 2L 2 個')).cell], ['csv', '赤玉土 大粒 2L 2 個']);
  // 税率: Company DB の 0.1 / 0.08 → NE の消費税率 (%) 10 / 8
  assert.deepEqual([0.1, 0.08].map((v) => j(u('t-1', 'tax_rate', v, 'single'))).map((r) => [r.status, r.cell]), [['csv', '10'], ['csv', '8']]);
  // buildCsv の二重の守り (judge の後で値が崩れても CSV にしない)
  assert.equal(buildCsv(COLUMNS['products:tax_rate'], [{ ne_code: 't-1', cell: '10' }, { ne_code: 't-2', cell: '8' }]).bytes.toString('utf8'), 'syohin_code,tax_rate\r\nt-1,10\r\nt-2,8\r\n');
  for (const bad of ['0.1', '0.08', '10.0', '10%', '', '0']) assert.throws(() => buildCsv(COLUMNS['products:tax_rate'], [{ ne_code: 't-1', cell: bad }]), /値の形が違う/, bad);
  for (const k of ['products:standard_price_jpy', 'products:cost', 'sets:standard_price_jpy']) for (const bad of ['0', '0.00', '-1', '1.5']) assert.throws(() => buildCsv(COLUMNS[k], [{ ne_code: 'x1', cell: bad }]), /値の形が違う/, `${k} ${bad}`);
  for (const k of ['products:name', 'sets:name']) assert.throws(() => buildCsv(COLUMNS[k], [{ ne_code: 'Akadama-1', cell: 'akadama-1' }]), /名前が商品コードと同じ/, k);
  // 列の表の全部の値の書き方が cellRe の形に合う (税率・円の列)
  for (const [k, s] of Object.entries(COLUMNS)) if (s.cellRe) for (const v of [0.1, 0.08, 1, 1980, 999999999]) { const c = s.cell(v); if (c.ok) assert.match(c.cell, s.cellRe, `${k} ${v}`); }
});

server.close();
await pg.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
