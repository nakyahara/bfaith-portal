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
import zlib from 'node:zlib';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { dumpCompanyDb, restoreCompanyDb, listTables, tableMeta, listSequences, listTriggers, encodeCopyValue, decodeCopyValue, encodeRow, decodeRow, parseDump, verifyDumpText, dumpToFile, dumpToGzipFile, restoreFromFile, verifyDumpFile, restoreFromLines, DUMP_VERSION } from '../apps/company-db/backup/dump.mjs';

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

console.log('\n大きい表 (読み込みの区切り 5,000 行をまたぐ) と gzip への直接書き出し');
await sq(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, rows_inserted, rows_skipped, source_tz, checksum, format_version)
  select 'bulk_' || lpad(i::text, 6, '0'), 'test', 'bulk', 'render', 'h', now(), now(), 'success', true, 1, 1, 0, 'UTC', 'c', 'v1'
  from generate_series(1, 12001) as i`);
let bulkLines = null;
await ta('カーソルで読む: 区切りをまたいでも抜けも重複も無く、主キーの順に並ぶ', async () => {
  const lines = [];
  const r = await dumpCompanyDb(sdb, (l) => lines.push(l), { log: quiet });
  bulkLines = lines;
  const start = lines.findIndex((l) => l.startsWith('COPY "ops"."ingest_runs"'));
  const end = lines.indexOf('\\.', start);
  const idCol = lines[start].match(/\((.*)\) FROM stdin;$/)[1].split(', ').indexOf('"ingest_run_id"');
  const ids = lines.slice(start + 1, end).map((l) => decodeRow(l)[idCol]);
  assert.equal(ids.length, 12002, '12,001 件 + 前からある 1 件');
  assert.equal(new Set(ids).size, ids.length, '重複が無い');
  assert.deepEqual(ids, [...ids].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)), '主キーの順');
  assert.equal(r.tables.find((x) => x.table === T('ops.ingest_runs')).rows, 12002);
  assert.ok(lines.includes('-- end: "ops"."ingest_runs" rows=12002'));
});
await ta('gzip へ直接書いた中身は、そのままのダンプと同じ / rawBytes は gzip 前の大きさ / そこから戻せる', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-backup-gz-'));
  const file = path.join(dir, 'company-db.dump.gz');
  const info = await dumpToGzipFile(sdb, file, { level: 5, log: quiet });
  const text = zlib.gunzipSync(fs.readFileSync(file)).toString('utf-8');
  const expected = bulkLines.map((l) => l + '\n').join('');
  // generated_at の行だけは取った時刻が違う
  const norm = (x) => x.replace(/^-- generated_at: .*$/m, '-- generated_at: X');
  assert.equal(norm(text), norm(expected));
  assert.equal(info.rawBytes, Buffer.byteLength(text, 'utf-8'));
  assert.equal(info.bytes, fs.statSync(file).size);
  assert.ok(info.bytes < info.rawBytes);
  assert.equal(info.totalRows, verifyDumpText(text).totalRows);
  const fresh = new PGlite(); const fdb = pgliteAdapter(fresh);
  await applyMigrations(fdb, { log: quiet });
  await restoreFromFile(fdb, file, { log: quiet });
  assert.equal(Number((await fdb.query("select count(*)::bigint as n from ops.ingest_runs where entity = 'bulk'")).rows[0].n), 12001);
  await fresh.close();
  fs.rmSync(dir, { recursive: true, force: true });
});
await ta('書き出し先に書けないときは投げる (黙って空のダンプにしない)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-backup-gz-'));
  await assert.rejects(() => dumpToGzipFile(sdb, path.join(dir, 'no-such-dir', 'x.gz'), { log: quiet }));
  fs.rmSync(dir, { recursive: true, force: true });
});
await ta('カーソルで読んでいる途中 (1 ページ目の後) で書き出しが失敗しても、同じ接続で次のダンプが取れる', async () => {
    let n = 0; let inBulk = false;
    await assert.rejects(() => dumpCompanyDb(sdb, (l) => {
      if (l.startsWith('COPY "ops"."ingest_runs"')) inBulk = true;
      else if (inBulk && ++n === 5001) throw new Error('書き出し失敗 (試験)');   // 1 ページ目 (5,000 行) の後
    }, { log: quiet }), /書き出し失敗/);
    assert.equal(n, 5001, '2 ページ目の途中で落ちている');
  });
await ta('打ち切り (signal) で止まり、一時ファイルを消せる (書き出しが閉じている)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-backup-gz-'));
  const file = path.join(dir, 'x.gz');
  const ac = new AbortController();
  const work = dumpToGzipFile(sdb, file, { log: quiet, signal: ac.signal });
  ac.abort(new Error('時間切れ (試験)'));
  await assert.rejects(() => work);
  fs.rmSync(dir, { recursive: true, force: true });   // Windows では開いたままのファイルは消せない
  assert.ok(!fs.existsSync(dir));
});
await ta('書き出しが失敗しても、次のダンプが取れる (カーソルとトランザクションが残らない)', async () => {
  const lines = [];
  const r = await dumpCompanyDb(sdb, (l) => lines.push(l), { log: quiet });
  assert.equal(r.tables.find((x) => x.table === T('ops.ingest_runs')).rows, 12002);
});

console.log('\n1 行ずつ読む検証と復元 (ダンプ全体を文字列にしない)');
/** 文字列を gzip ファイルに (試験用) */
function writeGz(dir, name, text) { const p = path.join(dir, name); fs.writeFileSync(p, zlib.gzipSync(Buffer.from(text, 'utf-8'))); return p; }
const productsNow = async (d) => (await d.query('select display_code, parent_product_id is not null as has_parent from core.products order by display_code')).rows;
await ta('ファイル版の検証は文字列版と同じ結果 (表ごとの行数・合計・採番)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  const file = path.join(dir, 'a.dump.gz');
  await dumpToFile(sdb, file, { log: quiet });
  const text = zlib.gunzipSync(fs.readFileSync(file)).toString('utf-8');
  assert.deepEqual(await verifyDumpFile(file), verifyDumpText(text));
  fs.rmSync(dir, { recursive: true, force: true });
});
await ta('dumpToFile の sha256 はファイルの中身と一致する', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  const file = path.join(dir, 'a.dump.gz');
  const info = await dumpToFile(sdb, file, { log: quiet });
  const crypto = await import('node:crypto');
  assert.equal(info.sha256, crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex'));
  assert.ok(info.rawBytes > info.bytes);
  fs.rmSync(dir, { recursive: true, force: true });
});
await ta('途中で切れたファイル: 検証も復元も止まり、復元先は何も消えない', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  const full = [];
  await dumpCompanyDb(sdb, (l) => full.push(l), { log: quiet });
  const cutText = full.slice(0, Math.floor(full.length * 0.6)).join('\n') + '\n';
  const cut = writeGz(dir, 'cut.dump.gz', cutText);
  // 止まり方 (エラーコード) は文字列版とファイル版で同じ (行の切り方が同じ)
  let textCode = null;
  try { verifyDumpText(cutText); } catch (e) { textCode = e.code; }
  assert.ok(['DUMP_TRUNCATED', 'DUMP_COLUMN_MISMATCH'].includes(textCode), String(textCode));
  await assert.rejects(() => verifyDumpFile(cut), (e) => e.code === textCode);
  // 改行で終わらない切れ方も
  const cut2 = writeGz(dir, 'cut2.dump.gz', full.slice(0, Math.floor(full.length * 0.6)).join('\n'));
  await assert.rejects(() => verifyDumpFile(cut2), (e) => e.code === 'DUMP_TRUNCATED');
  const p = new PGlite(); const pdb = pgliteAdapter(p);
  await applyMigrations(pdb, { log: quiet });
  await pdb.query("insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, 'keep-me', '残る', 'active', 'system', 'test')");
  await assert.rejects(() => restoreFromFile(pdb, cut, { log: quiet }), (e) => e.code === textCode);
  await assert.rejects(() => restoreFromFile(pdb, cut2, { log: quiet }), (e) => e.code === 'DUMP_TRUNCATED');
  assert.deepEqual((await productsNow(pdb)).map((r) => r.display_code), ['keep-me']);
  await p.close();
  fs.rmSync(dir, { recursive: true, force: true });
});
await ta('gzip として壊れたファイルは投げる (黙って空にしない)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  const bad = path.join(dir, 'bad.dump.gz');
  fs.writeFileSync(bad, Buffer.concat([zlib.gzipSync(Buffer.from('-- company-db-dump-v2\n')).subarray(0, 12), Buffer.from('garbage')]));
  await assert.rejects(() => verifyDumpFile(bad));
  fs.rmSync(dir, { recursive: true, force: true });
});
await ta('1 回目と 2 回目で中身が変わったら取り消す (復元先は元のまま)', async () => {
  const full = [];
  await dumpCompanyDb(sdb, (l) => full.push(l), { log: quiet });
  // 2 回目だけ ops.ingest_runs の行を 1 行減らし、その表の "-- end:" と合計も合わせる (= 2 回目単体では正しいダンプ)
  const start = full.findIndex((l) => l.startsWith('COPY "ops"."ingest_runs"'));
  const second = [...full];
  second.splice(start + 1, 1);
  const endIdx = second.findIndex((l, i) => i > start && l.startsWith('-- end: "ops"."ingest_runs"'));
  second[endIdx] = second[endIdx].replace(/rows=(\d+)$/, (_, n) => 'rows=' + (Number(n) - 1));
  const totIdx = second.findIndex((l) => l.startsWith('-- total_rows: '));
  second[totIdx] = second[totIdx].replace(/(\d+)$/, (n) => String(Number(n) - 1));
  verifyDumpText(second.join('\n'));   // 2 回目単体では正しい
  const p = new PGlite(); const pdb = pgliteAdapter(p);
  await applyMigrations(pdb, { log: quiet });
  await pdb.query("insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, 'keep-me', '残る', 'active', 'system', 'test')");
  let calls = 0;
  await assert.rejects(() => restoreFromLines(pdb, () => (++calls === 1 ? full : second), { log: quiet }), (e) => e.code === 'RESTORE_SOURCE_CHANGED');
  assert.equal(calls, 2);
  assert.deepEqual((await productsNow(pdb)).map((r) => r.display_code), ['keep-me'], '取り消されて元のまま');
  await p.close();
});
await ta('ファイルから戻した中身 = 元の DB (自己参照・区切りをまたぐ大きい表・採番。schema_migrations 以外は 1 行も違わない)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  const file = path.join(dir, 'a.dump.gz');
  const info = await dumpToFile(sdb, file, { log: quiet });
  const p = new PGlite(); const pdb = pgliteAdapter(p);
  await applyMigrations(pdb, { log: quiet });
  const r = await restoreFromFile(pdb, file, { log: quiet });
  assert.equal(r.totalRows, info.totalRows - skipRows());
  assert.deepEqual(await productsNow(pdb), await productsNow(sdb));
  assert.ok(r.selfFix.some((x) => x.table === T('core.products') && x.column === 'parent_product_id' && x.rows === 2));
  const again = []; const orig = [];
  await dumpCompanyDb(pdb, (l) => again.push(l), { log: quiet });
  await dumpCompanyDb(sdb, (l) => orig.push(l), { log: quiet });
  // schema_migrations は復元しない (applied_at が違う) ので、その表と generated_at を除いて比べる
  const strip = (ls) => {
    const out = []; let skip = false;
    for (const l of ls) {
      if (l.startsWith('COPY "ops"."schema_migrations"')) { skip = true; continue; }
      if (skip) { if (l.startsWith('-- end: "ops"."schema_migrations"')) skip = false; continue; }
      if (!l.startsWith('-- generated_at')) out.push(l);
    }
    return out.join('\n');
  };
  assert.equal(strip(again), strip(orig));
  await p.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

await ta('2 回目だけ「列の並び」「値」「採番」を変えても取り消す (行数は同じ = 行数の照合では見逃す差し替え)', async () => {
  const full = [];
  await dumpCompanyDb(sdb, (l) => full.push(l), { log: quiet });
  const variants = {};
  // (a) core.products の列の並びを入れ替え (COPY 行と全行を同じように入れ替える = 2 回目単体では正しいダンプ)
  {
    const v = [...full];
    const s = v.findIndex((l) => l.startsWith('COPY "core"."products"'));
    const m = v[s].match(/^(COPY "core"\."products" \()(.*)(\) FROM stdin;)$/);
    const cols = m[2].split(', ');
    const swap = (arr) => { const a = [...arr]; [a[0], a[1]] = [a[1], a[0]]; return a; };
    v[s] = m[1] + swap(cols).join(', ') + m[3];
    for (let i = s + 1; v[i] !== '\\.'; i++) v[i] = swap(v[i].split('\t')).join('\t');
    variants['列の並び'] = v;
  }
  // (b) 値だけ (core.products の 1 行目の最後の列でない文字列を 1 文字変える)
  {
    const v = [...full];
    const s = v.findIndex((l) => l.startsWith('COPY "core"."products"'));
    v[s + 1] = v[s + 1].replace('まとまり A', 'まとまり B');
    assert.notEqual(v[s + 1], full[s + 1], '値を変えられた');
    variants['値'] = v;
  }
  // (c) 採番だけ
  {
    const v = [...full];
    const i = v.findIndex((l) => l.startsWith('-- sequence: '));
    v[i] = v[i].replace(/next=(\d+)$/, (_, n) => 'next=' + (Number(n) + 1000));
    variants['採番'] = v;
  }
  for (const [name, second] of Object.entries(variants)) {
    verifyDumpText(second.join('\n'));   // 2 回目単体では正しい
    const p = new PGlite(); const pdb = pgliteAdapter(p);
    await applyMigrations(pdb, { log: quiet });
    await pdb.query("insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, 'keep-me', '残る', 'active', 'system', 'test')");
    let calls = 0;
    await assert.rejects(() => restoreFromLines(pdb, () => (++calls === 1 ? full : second), { log: quiet }), (e) => e.code === 'RESTORE_SOURCE_CHANGED', name);
    assert.deepEqual((await productsNow(pdb)).map((r) => r.display_code), ['keep-me'], `${name}: 取り消されて元のまま`);
    await p.close();
  }
});
await ta('ファイルが無い・gzip が途中で壊れている → 投げる (プロセスは落ちない)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  await assert.rejects(() => verifyDumpFile(path.join(dir, 'no-such.dump.gz')), (e) => e.code === 'ENOENT');
  const p = new PGlite(); const pdb = pgliteAdapter(p);
  await applyMigrations(pdb, { log: quiet });
  await assert.rejects(() => restoreFromFile(pdb, path.join(dir, 'no-such.dump.gz'), { log: quiet }), (e) => e.code === 'ENOENT');
  await p.close();
  // 正しい gzip の真ん中あたりを壊す (CRC / 中身の破損)
  const file = path.join(dir, 'a.dump.gz');
  await dumpToFile(sdb, file, { log: quiet });
  const buf = fs.readFileSync(file);
  for (let i = Math.floor(buf.length / 2); i < Math.floor(buf.length / 2) + 64; i++) buf[i] ^= 0xff;
  const broken = path.join(dir, 'broken.dump.gz');
  fs.writeFileSync(broken, buf);
  await assert.rejects(() => verifyDumpFile(broken));
  fs.rmSync(dir, { recursive: true, force: true });   // 途中で投げてもファイルは閉じている (Windows では開いたままだと消せない)
  assert.ok(!fs.existsSync(dir));
});
await ta('行の切り方は文字列版とファイル版で同じ (単独の CR は改行にしない / CRLF は LF と同じ)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  const full = [];
  await dumpCompanyDb(sdb, (l) => full.push(l), { log: quiet });
  const text = full.join('\n') + '\n';
  // 末尾の印の後ろに単独の CR → 文字列版は受け付けない。ファイル版も同じく受け付けない
  const crText = full.join('\n') + '\r';
  let textErr = null;
  try { verifyDumpText(crText); } catch (e) { textErr = e.code; }
  assert.ok(textErr, '文字列版は単独の CR で終わるダンプを拒否する');
  await assert.rejects(() => verifyDumpFile(writeGz(dir, 'cr.dump.gz', crText)), (e) => e.code === textErr);
  // CRLF のダンプ (大きい表があるので gzip の読み込みの区切りを何度もまたぐ) は、LF と同じ結果
  const crlfText = text.replace(/\n/g, '\r\n');
  assert.deepEqual(await verifyDumpFile(writeGz(dir, 'crlf.dump.gz', crlfText)), verifyDumpText(text));
  assert.deepEqual(verifyDumpText(crlfText), verifyDumpText(text));
  fs.rmSync(dir, { recursive: true, force: true });
});
await ta('dumpToFile の取り直しが途中で落ちても、前のダンプは残る (書きかけで上書きしない・一時ファイルも残らない)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-verify-'));
  const file = path.join(dir, 'a.dump.gz');
  await dumpToFile(sdb, file, { log: quiet });
  const before = fs.readFileSync(file);
  const failing = { ...sdb, exec: async () => { throw new Error('DB が落ちた (試験)'); }, query: async () => { throw new Error('DB が落ちた (試験)'); } };
  await assert.rejects(() => dumpToFile(failing, file, { log: quiet }), /DB が落ちた/);
  assert.ok(before.equals(fs.readFileSync(file)), '前のダンプがそのまま');
  assert.deepEqual(fs.readdirSync(dir), ['a.dump.gz'], '一時ファイルが残っていない');
  fs.rmSync(dir, { recursive: true, force: true });
});

// 🚨 実際の大きさの試験 (gzip 前 600 MB 超 = Node の文字列の上限を超えるダンプ)。時間がかかるので CDB_BACKUP_BIG_TEST=1 のときだけ
if (process.env.CDB_BACKUP_BIG_TEST === '1') {
  await ta('gzip 前 600 MB 超のダンプを検証できる (旧方式の「全体を文字列に」は上限で落ちる大きさ)', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-big-'));
    const file = path.join(dir, 'big.dump.gz');
    const N = 2_900_000; const pad = 'x'.repeat(200);
    const gz = zlib.createGzip({ level: 1 });
    const out = fs.createWriteStream(file);
    const done = new Promise((res, rej) => { out.on('finish', res); out.on('error', rej); gz.on('error', rej); });
    gz.pipe(out);
    let raw = 0;
    const w = async (l) => { const b = Buffer.from(l + '\n'); raw += b.length; if (!gz.write(b)) await new Promise((r) => gz.once('drain', r)); };
    for (const l of [`-- ${DUMP_VERSION}`, '-- generated_at: 2026-09-26T00:00:00.000Z', '-- session: x', '-- migrations: 0001', '-- tables: 1', 'COPY "core"."big" ("id", "v") FROM stdin;']) await w(l);
    for (let i = 0; i < N; i++) await w(`${i}\t${pad}`);
    for (const l of ['\\.', `-- end: "core"."big" rows=${N}`, `-- total_rows: ${N}`, '-- end_of_dump']) await w(l);
    gz.end(); await done;
    assert.ok(raw > 0x1fffffe8, `文字列の上限 (約 512 MB) を超える大きさで試す: ${raw}`);
    const t0 = Date.now();
    const v = await verifyDumpFile(file);
    console.log(`      (gzip 前 ${(raw / 1e6).toFixed(0)} MB / gzip ${(fs.statSync(file).size / 1e6).toFixed(1)} MB / 検証 ${((Date.now() - t0) / 1000).toFixed(1)} 秒)`);
    assert.equal(v.totalRows, N);
    assert.throws(() => zlib.gunzipSync(fs.readFileSync(file)).toString('utf-8'), '旧方式 (全体を文字列に) はこの大きさで落ちる');
    fs.rmSync(dir, { recursive: true, force: true });
  });
}

await src.close(); await dst.close();
console.log(`\n${passed} 件 PASS`);
