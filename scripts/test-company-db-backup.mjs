/**
 * test-company-db-backup.mjs — Company DB のバックアップと復元の受入試験 (PGlite で往復)
 *
 * 本物の Postgres は要らない。DDL を流した PGlite にデータを入れ、ダンプ → 別の空 PGlite に復元 → 一致を見る。
 * 使い方: node scripts/test-company-db-backup.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { dumpCompanyDb, restoreCompanyDb, listTables, tableMeta, listSequences, listTriggers, encodeCopyValue, decodeCopyValue, encodeRow, decodeRow, parseDump, verifyDumpText, dumpToFile, restoreFromFile, DUMP_VERSION } from '../apps/company-db/backup/dump.mjs';

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }
const quiet = () => {};
/** 表名はダンプでは引用付き ("core"."products") */
const T = (name) => '"' + name.split('.').join('"."') + '"';

console.log('COPY TEXT の書き方・読み方');
t('タブ・改行・バックスラッシュ・null', () => {
  assert.equal(encodeCopyValue(null), '\\N');
  assert.equal(encodeCopyValue(undefined), '\\N');
  assert.equal(encodeCopyValue('a\tb'), 'a\\tb');
  assert.equal(encodeCopyValue('a\nb'), 'a\\nb');
  assert.equal(encodeCopyValue('a\\b'), 'a\\\\b');
  assert.equal(encodeCopyValue('a\r\nb'), 'a\\r\\nb');
  assert.equal(decodeCopyValue('\\N'), null);
  assert.equal(decodeCopyValue('a\\tb'), 'a\tb');
  assert.equal(decodeCopyValue('a\\\\b'), 'a\\b');
  assert.equal(decodeCopyValue('a\\nb'), 'a\nb');
  for (const v of ['', 'ふつうの文字', 'タブ\tと改行\nと\\N という文字列', '{a,b}', '{"k": 1}', '\\N ではない']) {
    assert.equal(decodeCopyValue(encodeCopyValue(v)), v, JSON.stringify(v));
  }
  assert.deepEqual(decodeRow(encodeRow(['a', null, 'b\tc', ''])), ['a', null, 'b\tc', '']);
});

console.log('\nダンプ → 復元 (PGlite)');
const src = new PGlite(); const sdb = pgliteAdapter(src);
await applyMigrations(sdb, { log: quiet });
const sq = async (sql, p) => (await sdb.query(sql, p)).rows;

// 実データに近い形で入れる (親子・自己参照・生成列・identity・jsonb・配列・append-only・パーティション)
await sq(`insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values
  (1, 'parent-a', 'まとまり A', 'active', 'system', 'test'),
  (1, 'child-1', '子 1 【黒】', 'active', 'system', 'test'),
  (1, 'child-2', '子 2 【白】', 'discontinued', 'system', 'test')`);
await sq(`update core.products set parent_product_id = (select product_id from core.products where display_code = 'parent-a')
  where display_code in ('child-1', 'child-2')`);
await sq(`insert into core.skus (company_id, product_id, sku_kind, code, name, handling, created_by_type, created_by_id)
  select 1, product_id, 'single', display_code, name, 'active', 'system', 'test' from core.products where display_code like 'child-%'`);
await sq(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status, created_by_type, created_by_id) values
  (1, 'amazon', 'main@A1VC38T7YXB528', 'pr_child-1', 'タブ\tと改行\nを含む題名', 'active', 'system', 'test')`);
await sq(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type, resolved_by_id, evidence)
  select 1, l.listing_id, s.sku_id, 1, 'imported', 'system', 'test', '{"source": "test", "note": "日本語 \\"引用\\" と \\\\ 記号"}'::jsonb
  from core.listings l, core.skus s where l.listing_code = 'pr_child-1' and s.code = 'child-1'`);
await sq(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
  select 1, 'product', product_id, 'jan', 'jan', '4900000000011', 'imported', 'system', 'test' from core.products where display_code = 'child-1'`);
await sq(`insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_text, raw_text, source_system, source_ref, observed_at, content_hash)
  select 'test:1', 'product', product_id, 'brand', 'item', 'ブランド', 'ブランド', 'product_hub', 'test', now(), 'h1' from core.products where display_code = 'child-1'`);
await sq(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, rows_inserted, rows_skipped, source_tz, checksum, format_version)
  values ('test_run', 'test', 'products', 'render', 'h', now(), now(), 'success', true, 1, 1, 0, 'UTC', 'c', 'v1')`);
await sq(`select snapshots.ensure_month_partitions(date_trunc('month', now())::date, date_trunc('month', now())::date)`);
await sq(`insert into snapshots.listing_daily (snapshot_date, listing_id, status, price_jpy, complete, source_run_id, observed_at)
  select core.jst_date(now()), listing_id, 'active', 1980, true, 'test_run', now() from core.listings where listing_code = 'pr_child-1'`);

let dumpText = null; let dumpResult = null;
await ta('ダンプが取れる (表の順序・行数・ヘッダ)', async () => {
  const lines = [];
  dumpResult = await dumpCompanyDb(sdb, (l) => lines.push(l), { log: quiet });
  dumpText = lines.join('\n');
  assert.ok(dumpText.startsWith(`-- ${DUMP_VERSION}`));
  assert.match(dumpText, /-- migrations: 0001,/);
  // 入れたデータ + 参照データ (migrations の seed: 会社 2 / 倉庫 4 / 規則の版 1 / 解決規則 33)
  const byTable = Object.fromEntries(dumpResult.tables.map((x) => [x.table, x.rows]));
  assert.equal(byTable[T('core.products')], 3);
  assert.equal(byTable[T('core.skus')], 2);
  assert.equal(byTable[T('core.listings')], 1);
  assert.equal(byTable[T('core.listing_components')], 1);
  assert.equal(byTable[T('core.external_ids')], 1);
  assert.equal(byTable[T('core.product_attribute_observations')], 1);
  assert.equal(byTable[T('snapshots.listing_daily')], 1);
  assert.equal(byTable[T('core.companies')], 2);
  assert.ok(byTable[T('core.attribute_resolution_rules')] >= 30);
  assert.ok(dumpResult.totalRows >= 45, String(dumpResult.totalRows));
  const names = dumpResult.tables.map((x) => x.table);
  assert.ok(names.includes(T('core.products')) && names.includes(T('snapshots.listing_daily')));
  assert.ok(!names.some((n) => /listing_daily_\d{4}_\d{2}/.test(n)), 'パーティションの子は取らない');
  // 親が子より先
  assert.ok(names.indexOf(T('core.products')) < names.indexOf(T('core.skus')), 'products は skus より先');
  assert.ok(names.indexOf(T('core.listings')) < names.indexOf(T('core.listing_components')));
  // 生成列は入れない
  assert.ok(!/COPY "core"\."skus" \([^)]*code_norm/.test(dumpText), 'code_norm は入れない');
  assert.match(dumpText, /COPY "core"\."skus" \([^)]*"code"/);
  // セッション設定とシーケンスを記録している
  assert.match(dumpText, /-- session: datestyle=ISO, YMD/);
  assert.match(dumpText, /-- sequence: .*products_product_id_seq.* next=\d+/);
  assert.ok(dumpText.trimEnd().endsWith('-- end_of_dump'));
});

await ta('壊れていないか検証できる (verifyDumpText)', async () => {
  const v = verifyDumpText(dumpText);
  assert.equal(v.ok, true);
  assert.equal(v.totalRows, dumpResult.totalRows);
  assert.equal(v.declaredTotal, dumpResult.totalRows);
  assert.ok(v.migrations.length >= 8);
  assert.ok(v.sequences > 10, String(v.sequences));
  assert.match(v.session, /datestyle=ISO, YMD/);
  // 途中で切れたダンプ・辻褄の合わないダンプは受け付けない (復元で何かを消す前に止まる)
  const cut = dumpText.split('\n');
  const copyAt = cut.findIndex((l) => l.startsWith('COPY '));
  const isTruncated = (e) => e.code === 'DUMP_TRUNCATED' || e.code === 'DUMP_PARSE';   // どちらも「途中で切れている」の検出
  assert.throws(() => verifyDumpText(cut.slice(0, copyAt + 2).join('\n')), isTruncated);       // 表の途中で切れた
  assert.throws(() => verifyDumpText(dumpText.replace('-- end_of_dump', '')), (e) => e.code === 'DUMP_TRUNCATED');    // 末尾の印が無い
  const endAt = cut.findIndex((l) => l.startsWith('-- end: '));
  assert.throws(() => verifyDumpText(cut.slice(0, endAt).join('\n')), isTruncated);   // 表の終わりの印が欠けた
  assert.throws(() => verifyDumpText(cut.slice(0, endAt + 1).join('\n')), isTruncated);        // 表の切れ目で切れた (末尾の印が無い)
  assert.throws(() => verifyDumpText(dumpText.replace(/-- total_rows: \d+/, '-- total_rows: 999999')), (e) => e.code === 'DUMP_TOTAL_MISMATCH');
  assert.throws(() => verifyDumpText(dumpText.replace(/-- tables: \d+/, '-- tables: 3')), (e) => e.code === 'DUMP_TABLE_COUNT');
  assert.throws(() => verifyDumpText(dumpText.replace(`-- end: ${T('core.products')} rows=3`, `-- end: ${T('core.products')} rows=2`)), (e) => e.code === 'DUMP_ROW_MISMATCH');
  // 行の列数が合わない
  const broken = dumpText.split('\n');
  const prodCopy = broken.findIndex((l) => l.startsWith(`COPY ${T('core.products')} `));
  broken[prodCopy + 1] = broken[prodCopy + 1] + '\textra';
  assert.throws(() => verifyDumpText(broken.join('\n')), (e) => e.code === 'DUMP_COLUMN_MISMATCH');
  assert.throws(() => verifyDumpText('-- other-format\n'), /ダンプの版が違う/);
});

const dst = new PGlite(); const ddb = pgliteAdapter(dst);
await applyMigrations(ddb, { log: quiet });
const dq = async (sql, p) => (await ddb.query(sql, p)).rows;

const skipRows = () => dumpResult.tables.filter((x) => x.table === T('ops.schema_migrations')).reduce((n, x) => n + x.rows, 0);
await ta('空の DB に復元できて、中身が一致する (ID・親子・生成列・jsonb・タブ改行)', async () => {
  const r = await restoreCompanyDb(ddb, dumpText, { log: quiet });
  assert.equal(r.totalRows, dumpResult.totalRows - skipRows());   // schema_migrations は置換しない
  // 行数 (schema_migrations は置換しないので除く)
  for (const { table, rows } of dumpResult.tables) {
    if (table === T('ops.schema_migrations')) continue;
    const n = Number((await dq(`select count(*)::bigint as n from ${table}`))[0].n);
    assert.equal(n, rows, table);
  }
  assert.deepEqual(r.skipped, [T('ops.schema_migrations')]);
  // ID が保たれている
  const srcP = await sq('select product_id, display_code, parent_product_id from core.products order by product_id');
  const dstP = await dq('select product_id, display_code, parent_product_id from core.products order by product_id');
  assert.deepEqual(dstP.map((r2) => [Number(r2.product_id), r2.display_code, r2.parent_product_id == null ? null : Number(r2.parent_product_id)]),
    srcP.map((r2) => [Number(r2.product_id), r2.display_code, r2.parent_product_id == null ? null : Number(r2.parent_product_id)]));
  // 自己参照が埋まっている
  assert.equal((await dq("select count(*)::int as n from core.products where parent_product_id is not null"))[0].n, 2);
  // 生成列は自動で入る
  const sku = (await dq("select code, code_norm from core.skus where code = 'child-1'"))[0];
  assert.equal(sku.code_norm, 'child-1');
  // タブ・改行を含む文言
  assert.equal((await dq("select title from core.listings where listing_code = 'pr_child-1'"))[0].title, 'タブ\tと改行\nを含む題名');
  // jsonb
  const ev = (await dq('select evidence from core.listing_components limit 1'))[0].evidence;
  const evObj = typeof ev === 'string' ? JSON.parse(ev) : ev;
  assert.equal(evObj.note, '日本語 "引用" と \\ 記号');
  // パーティションの行は子に振り分けられている
  assert.equal((await dq('select count(*)::int as n from snapshots.listing_daily'))[0].n, 1);
});

await ta('復元のあとに採番が進んでいる (新しい行が既存 ID とぶつからない)', async () => {
  const maxBefore = Number((await dq('select max(product_id) as m from core.products'))[0].m);
  await dq(`insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, 'after-restore', '復元後の商品', 'active', 'system', 'test')`);
  const created = Number((await dq("select product_id from core.products where display_code = 'after-restore'"))[0].product_id);
  assert.ok(created > maxBefore, `${created} > ${maxBefore}`);
  await dq("delete from core.products where display_code = 'after-restore'");
});

await ta('もう一度戻しても増えない (入れ替えなので同じ状態になる)。migrations が足りない DB には戻さない', async () => {
  const r2 = await restoreCompanyDb(ddb, dumpText, { log: quiet });
  assert.equal(r2.totalRows, dumpResult.totalRows - skipRows());
  assert.equal(Number((await dq('select count(*)::bigint as n from core.products'))[0].n), 3);
  assert.equal(Number((await dq('select count(*)::bigint as n from core.companies'))[0].n), 2);
  const bare = new PGlite(); const bdb = pgliteAdapter(bare);
  await bdb.exec('create schema ops; create table ops.schema_migrations (version text primary key, applied_at timestamptz default now(), checksum text)');
  await bdb.query("insert into ops.schema_migrations (version) values ('0001_schemas_and_functions.sql')");
  await assert.rejects(() => restoreCompanyDb(bdb, dumpText, { log: quiet }), (e) => e.code === 'RESTORE_MIGRATIONS');
  await bare.close();
});

await ta('ファイル (gzip) に書いて、そこから戻せる', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-backup-'));
  const file = path.join(dir, 'company-db.dump.gz');
  const info = await dumpToFile(sdb, file, { log: quiet });
  assert.ok(info.bytes > 0 && info.bytes < info.rawBytes, `gzip で小さくなる (${info.bytes} < ${info.rawBytes})`);
  assert.match(info.sha256, /^[0-9a-f]{64}$/);
  const fresh = new PGlite(); const fdb = pgliteAdapter(fresh);
  await applyMigrations(fdb, { log: quiet });
  const r = await restoreFromFile(fdb, file, { log: quiet });
  assert.equal(r.totalRows, info.totalRows - skipRows());
  assert.equal(Number((await fdb.query('select count(*)::bigint as n from core.products')).rows[0].n), 3);
  await fresh.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

await ta('表の並び (親 → 子) と列の性質を読めている', async () => {
  const tables = await listTables(sdb);
  assert.ok(tables.length >= 70, String(tables.length));
  const names = tables.map((x) => x.qualified);
  assert.ok(names.indexOf(T('core.companies')) < names.indexOf(T('core.products')));
  const products = tables.find((x) => x.qualified === T('core.products'));
  const meta = await tableMeta(sdb, products);
  assert.ok(meta.identity.includes('product_id'));
  assert.ok(meta.selfRefs.includes('parent_product_id'));
  assert.ok(!meta.selfRefs.includes('company_id'), '複合 FK の not null 列は自己参照に数えない');
  assert.deepEqual(meta.primaryKey, ['product_id']);
  const skuMeta = await tableMeta(sdb, tables.find((x) => x.qualified === T('core.skus')));
  assert.ok(!skuMeta.columns.includes('code_norm'), '生成列は列に入れない');
});

await ta('保護 trigger を全部止めて入れ替え、元の状態に戻す (履歴の表・updated_at を触る表)', async () => {
  // 文言の履歴 (listing_texts) は DELETE を trigger が拒む表。ここに行があっても入れ替えできる
  await sq(`insert into core.listing_texts (listing_id, field, body, content_hash, observed_at, source_system, is_current)
    select listing_id, 'title', '古い題名', 'h-old', now() - interval '1 day', 'test', false from core.listings where listing_code = 'pr_child-1'`);
  await sq(`insert into core.listing_texts (listing_id, field, body, content_hash, observed_at, source_system, is_current)
    select listing_id, 'title', '今の題名', 'h-new', now(), 'test', true from core.listings where listing_code = 'pr_child-1'`);
  // 子商品の updated_at をわざと過去にして、復元で書き換わらないことを見る
  await sq(`update core.products set updated_at = timestamptz '2001-02-03 04:05:06+09' where display_code like 'child-%'`);
  const before = await sq("select display_code, updated_at::text as u from core.products where display_code like 'child-%' order by 1");
  const lines2 = [];
  const dump2 = await dumpCompanyDb(sdb, (l) => lines2.push(l), { log: quiet });
  const text2 = lines2.join('\n');
  const fresh = new PGlite(); const fdb2 = pgliteAdapter(fresh);
  await applyMigrations(fdb2, { log: quiet });
  const fq = async (sql) => (await fdb2.query(sql)).rows;
  await restoreCompanyDb(fdb2, text2, { log: quiet });
  assert.equal(Number((await fq('select count(*)::bigint as n from core.listing_texts'))[0].n), 2);
  assert.equal((await fq("select body from core.listing_texts where is_current"))[0].body, '今の題名');
  // updated_at が保存値のまま (自己参照の UPDATE で touch trigger が発火していない)
  const after = await fq("select display_code, updated_at::text as u from core.products where display_code like 'child-%' order by 1");
  assert.deepEqual(after.map((r) => [r.display_code, r.u]), before.map((r) => [r.display_code, r.u]));
  // trigger が元に戻っている (履歴の表は DELETE を拒む・観測は UPDATE を拒む)
  await assert.rejects(() => fdb2.query('delete from core.listing_texts'), /履歴|DELETE/);
  await assert.rejects(() => fdb2.query("update core.product_attribute_observations set value_text = 'x'"), /append-only|UPDATE/);
  // 入れ替えをもう一度やっても同じ (trigger が止まったままにならない)
  await restoreCompanyDb(fdb2, text2, { log: quiet });
  assert.equal(Number((await fq('select count(*)::bigint as n from core.listing_texts'))[0].n), 2);
  await fresh.close();
  void dump2;
});

await ta('シーケンスの次の値を保存して戻す (復元後に採番が飛ばない・巻き戻らない)', async () => {
  // 元の DB で ID を進めておく
  await sq(`insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, 'seq-probe', '採番確認', 'active', 'system', 'test')`);
  await sq(`delete from core.products where display_code = 'seq-probe'`);
  const seq = (await sq("select pg_get_serial_sequence('core.products','product_id') as s"))[0].s;
  const srcNext = Number((await sq(`select last_value + (case when is_called then 1 else 0 end) as n from ${seq}`))[0].n);
  const lines3 = [];
  await dumpCompanyDb(sdb, (l) => lines3.push(l), { log: quiet });
  const fresh = new PGlite(); const fdb3 = pgliteAdapter(fresh);
  await applyMigrations(fdb3, { log: quiet });
  await restoreCompanyDb(fdb3, lines3.join('\n'), { log: quiet });
  const dstNext = Number((await (await fdb3.query(`select last_value + (case when is_called then 1 else 0 end) as n from ${seq}`)).rows[0].n));
  assert.equal(dstNext, srcNext, '次に採番される値が元と同じ');
  await fresh.close();
});

await ta('ダンプの解析: COPY 行・\\. の対応・total_rows', async () => {
  const p = parseDump(dumpText);
  assert.ok(p.tables.length >= 70);
  assert.equal(p.header.totalRows, dumpResult.totalRows);
  assert.ok(p.header.generatedAt);
  const products = p.tables.find((x) => x.table === T('core.products'));
  assert.equal(products.rows.length, 3);
  assert.ok(products.columns.includes('display_code'));
});

await ta('大きな採番値 (2^53 超) が 1 も狂わずに往復する', async () => {
  // 🚨 bigint を JS の Number に入れると 9007199254740993 が ...992 になる。文字列のまま運ぶ (Codex 2026-09-10)
  const big = '9007199254740993';
  const a = new PGlite(); const adb = pgliteAdapter(a);
  await applyMigrations(adb, { log: quiet });
  await adb.exec(`alter sequence core.products_product_id_seq restart with ${big}`);
  const listed = await listSequences(adb, (await listTables(adb)).filter((x) => x.qualified === T('core.products')));
  assert.equal(listed.length, 1);
  assert.equal(listed[0].next, big, '桁が落ちていない');
  const lines = [];
  await dumpCompanyDb(adb, (l) => lines.push(l), { log: quiet });
  const text = lines.join('\n');
  assert.ok(text.includes(`-- sequence: ${T('core.products_product_id_seq')} next=${big}`), 'ダンプにそのままの値');
  const b = new PGlite(); const bdb = pgliteAdapter(b);
  await applyMigrations(bdb, { log: quiet });
  await restoreCompanyDb(bdb, text, { log: quiet });
  const after = (await bdb.query('select last_value::text as v, is_called from core.products_product_id_seq')).rows[0];
  assert.equal(after.v, big);
  assert.equal(after.is_called, false, '次に採番されるのがちょうどこの値');
  await a.close(); await b.close();
});

await ta('採番の記録が欠けたダンプは、何も消さずに拒否する', async () => {
  // シーケンスが抜けたまま復元すると、次の insert が主キー重複で落ちる。復元の前に気づく
  const before = Number((await dq('select count(*)::bigint as n from core.products'))[0].n);
  const dropped = dumpText.split('\n').filter((l) => !l.startsWith(`-- sequence: ${T('core.products_product_id_seq')} `)).join('\n');
  assert.equal(verifyDumpText(dropped).ok, true, '行数の辻褄は合っているので、検証だけでは気づけない');
  await assert.rejects(() => restoreCompanyDb(ddb, dropped, { log: quiet }), (e) => e.code === 'RESTORE_SEQUENCE_MISMATCH');
  assert.equal(Number((await dq('select count(*)::bigint as n from core.products'))[0].n), before, '拒否されたので中身はそのまま');
  // 知らないシーケンスが混ざっている場合も拒否
  const extra = dumpText.replace('-- table: ', '-- sequence: "core"."no_such_seq" next=1\n-- table: ');
  await assert.rejects(() => restoreCompanyDb(ddb, extra, { log: quiet }), (e) => e.code === 'RESTORE_SEQUENCE_MISMATCH');
});

await ta('パーティションの子だけ止めてある trigger は、子だけ止まったまま戻る', async () => {
  // 🚨 親に enable trigger user をかけると子にも波及する。親子それぞれの状態を覚えて ONLY で戻す (Codex 2026-09-10)
  const p = new PGlite(); const pdb = pgliteAdapter(p);
  await applyMigrations(pdb, { log: quiet });
  await pdb.exec(`create function ops.test_noop() returns trigger language plpgsql as $$ begin return new; end $$`);
  await pdb.exec(`create trigger trg_test_part after insert on snapshots.listing_daily for each row execute function ops.test_noop()`);
  await pdb.exec(`alter table only snapshots.listing_daily_default disable trigger trg_test_part`);
  const state = async () => Object.fromEntries((await pdb.query(`
    select c.relname as t, tg.tgenabled as e from pg_trigger tg join pg_class c on c.oid = tg.tgrelid
    where tg.tgname = 'trg_test_part' order by 1`)).rows.map((r) => [r.t, r.e]));
  const before = await state();
  assert.equal(before.listing_daily, 'O');
  assert.equal(before.listing_daily_default, 'D', '子だけ止めてある');
  // listTriggers は子孫まで見えている
  const parent = (await listTables(pdb)).find((x) => x.qualified === T('snapshots.listing_daily'));
  const tg = await listTriggers(pdb, parent.oid);
  assert.equal(tg.filter((x) => x.name === 'trg_test_part').length, 2, '親と子の 2 つ');
  await restoreCompanyDb(pdb, dumpText, { log: quiet });
  assert.deepEqual(await state(), before, '復元しても親子の状態が変わらない');
  await p.close();
});

await ta('終わりの印のあとに中身があるダンプは受け付けない', async () => {
  assert.throws(() => verifyDumpText(dumpText + '\nCOPY "core"."products" ("product_id") FROM stdin;'), (e) => e.code === 'DUMP_TRAILING');
  assert.throws(() => verifyDumpText(dumpText + '\n-- sequence: "core"."x" next=1'), (e) => e.code === 'DUMP_TRAILING');
  assert.equal(verifyDumpText(dumpText + '\n\n\n').ok, true, '末尾の空行は許す');
  // 名前が "schema"."name" の形でないシーケンス行は拒否 (この文字列は SQL に埋まる)
  assert.throws(() => verifyDumpText(dumpText.replace(/-- sequence: .*/, '-- sequence: core.products_x_seq next=1')), (e) => e.code === 'DUMP_PARSE');
});

await ta('serial の採番も収録する (identity だけでない)', async () => {
  const s2 = new PGlite(); const s2db = pgliteAdapter(s2);
  await applyMigrations(s2db, { log: quiet });
  await s2db.exec('create table ops.test_serial (id serial primary key, v text)');
  const t2 = (await listTables(s2db)).find((x) => x.qualified === T('ops.test_serial'));
  assert.ok(t2, '表が見えている');
  const seqs = await listSequences(s2db, [t2]);
  assert.deepEqual(seqs.map((x) => x.sequence), [T('ops.test_serial_id_seq')]);
  assert.equal(seqs[0].next, '1');
  await s2.close();
});

await ta('復元先にしかない表・列があれば、何も消さずに拒否する', async () => {
  // 🚨 ダンプ側だけを見ていると、ダンプに無い表は古い中身のまま残り、ダンプに無い列は null で埋まる (Codex 2026-09-10)
  const mkDb = async () => {
    const p = new PGlite(); const pdb = pgliteAdapter(p);
    await applyMigrations(pdb, { log: quiet });
    return { p, pdb, q: async (sql) => (await pdb.query(sql)).rows };
  };
  // (1) 復元先だけにある表
  {
    const { p, pdb, q } = await mkDb();
    await pdb.exec('create table ops.zz_extra (id int primary key, memo text)');
    await pdb.exec("insert into ops.zz_extra values (1, '消えては困る値')");
    await assert.rejects(() => restoreCompanyDb(pdb, dumpText, { log: quiet }), (e) => e.code === 'RESTORE_TABLE_MISMATCH');
    assert.equal((await q('select memo from ops.zz_extra'))[0].memo, '消えては困る値', '拒否されたので中身はそのまま');
    await p.close();
  }
  // (2) ダンプにあって復元先に無い表 (誰からも参照されていない表を 1 つ落とす)
  {
    const { p, pdb, q } = await mkDb();
    const leaf = (await q(`
      select n.nspname as s, c.relname as t from pg_class c join pg_namespace n on n.oid = c.relnamespace
      where c.relkind = 'r' and not c.relispartition and n.nspname = 'core'
        and not exists (select 1 from pg_constraint k where k.confrelid = c.oid and k.conrelid <> c.oid)
      order by 1, 2 limit 1`))[0];
    const before = Number((await q('select count(*)::bigint as n from core.companies'))[0].n);
    await pdb.exec(`drop table "${leaf.s}"."${leaf.t}" cascade`);
    await assert.rejects(() => restoreCompanyDb(pdb, dumpText, { log: quiet }), (e) => e.code === 'RESTORE_TABLE_MISMATCH');
    assert.equal(Number((await q('select count(*)::bigint as n from core.companies'))[0].n), before);
    await p.close();
  }
  // (3) 復元先だけにある列 (足した列が null で埋まらない)
  {
    const { p, pdb } = await mkDb();
    await pdb.exec('alter table core.products add column zz_memo text');
    await assert.rejects(() => restoreCompanyDb(pdb, dumpText, { log: quiet }), (e) => e.code === 'RESTORE_COLUMN_MISMATCH');
    await p.close();
  }
  // (4) ダンプにあって復元先に無い列
  {
    const { p, pdb } = await mkDb();
    await pdb.exec('alter table core.products drop column model_number cascade');
    await assert.rejects(() => restoreCompanyDb(pdb, dumpText, { log: quiet }), (e) => e.code === 'RESTORE_COLUMN_MISMATCH');
    await p.close();
  }
});

await src.close(); await dst.close();
console.log(`\n${passed} 件 PASS`);
