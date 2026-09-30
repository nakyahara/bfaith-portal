#!/usr/bin/env node
/**
 * test-company-db-finance-coverage.mjs — 決済のそろい (coverage・0050・D7b-1b-2 = Render 側) の受入試験
 *
 * 設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 §3.1。PGlite で全部の migration を流して確かめる:
 *   表と CHECK / receipt digest (JS の 1 行ずつ = 全体の正規の JSON・UTF-8 のバイトの順・墓石は除く・cursor の区切り) /
 *   状態の移り方の全部の場面 (古い世代 = stale・新しい世代は updating から・同じ世代・同じ token の再送 = same・違う token = 409・complete → updating = 409・
 *   complete は 1 回だけ・complete の再送は request_hash で same / 409・新しい世代の updating は前の complete を無効に) /
 *   receipt digest の一致 / 不一致 (数・行の数・digest) / manifest の形 (400) / policy /
 *   財務の chunk を世代に縛る (token 付き = その世代・token の updating のときだけ・complete の後・別の世代・別の token・別の source = 409・遅れた chunk の順序の逆転) /
 *   token の無い chunk (今の送り手) = 受ける・受領記録を変えたら complete → updating (無効の印・同じ世代では complete に戻れない)・same / stale では落とさない /
 *   lock = chunk (token あり / なし)・updating・complete が同じ advisory lock を取引の中で持つ / 0050 の前 (token 付きだけ 409・token 無しは今までどおり) /
 *   core.finance_coverage_state の差し替えで 0049 の mart が正式な値を出す (complete のときだけ) / 本物の router を HTTP で
 * 🚨 PGlite は 1 接続 = 「旧い chunk が lock を持ったまま止まる → 新しい世代の updating が待たされる」の 2 接続の待ちは書けない。
 *    代わりに ① 3 つの受け口が同じ lock を取引の中で持つこと (pg_locks) ② lock を取る前で止めた chunk が新しい世代の後に 409 になること を確かめる
 * 実行: node scripts/test-company-db-finance-coverage.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { validateFinanceRows, orderFinanceChecksum, pseudoOrderNo } from '../apps/company-db/finance/order-finance-checksum.mjs';
import { canonicalSha256 } from '../apps/company-db/canonical-hash.mjs';
import { createReceiptDigester, receiptDigest, coverageRequestHash, validateCoverageManifest, policyFingerprint, RECEIPT_DIGEST_FORMAT, MANIFEST_FIELDS } from '../apps/company-db/finance/coverage-manifest.mjs';
import { applyCoverage, computeReceiptDigest, coverageStatus, financeLockKey } from '../apps/company-db/ingest/finance-coverage.mjs';
import { validateFinanceChunk, ingestOrderFinanceChunk } from '../apps/company-db/ingest/order-finance.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const rejects = async (fn, code, re) => {
  let threw = null; try { await fn(); } catch (e) { threw = e; }
  if (!threw) throw new Error('did not throw');
  if (code && threw.code !== code) throw new Error(`wrong code: ${threw.code} (${threw.message})`);
  if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`);
  return threw;
};
const quiet = () => {};
const N = (x) => (x == null ? null : Number(x));
const H = (s) => crypto.createHash('sha256').update(s).digest('hex');

const pg = new PGlite();
const applied = await applyMigrations(pgliteAdapter(pg), { log: quiet });
assert.ok(applied.applied.includes('0050'), '0050 が流れていない');
const db = pgliteAdapter(pg);
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];

// ─── マスタ (0049 の正式な値を見るため: 出品 LA = SKU 1 個・原価 300) ───
const SKU = Number((await one(`with p as (insert into core.products (company_id, name) values (1, 'S') returning product_id)
  insert into core.skus (company_id, product_id, sku_kind, code, name) select 1, product_id, 'single', 'sku-1', 'S' from p returning sku_id`)).sku_id);
const LA = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', 'LA', 'active') returning listing_id`)).listing_id);
await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'manual', 'human')`, [LA, SKU]);
await pg.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (1, $1, 300, 'ne', 'COMPLETE', '2026-01-01')`, [SKU]);

// ─── 財務の chunk (受け口と同じ道: validateFinanceChunk → ingestOrderFinanceChunk) と、送り手の側の受領記録の写し (ledger) ───
const U = 'amazon_settlement_unified';
const V2 = 'amazon_finance_v2';
const C4 = { unclassified_component_count: 0, unclassified_mapped_jpy: 0, unclassified_abs_jpy: 0, unmapped_component_count: 0 };
const row = (date, sku, x = {}) => ({ economic_date_jst: date, seller_sku: sku, line_kind: sku === '-' ? 'unknown' : 'sku', source: U, source_lines: 1,
  source_updated_at: `${date}T00:00:00Z`, content_hash: 'h', ...C4, ...x });
let runSeq = 0, batch = 0;
const ledger = new Map();   // 送り手の側の受領記録 (mall_order_no → { set_checksum, transform_version, lines }) = digest を Render と別の道で作る
const chunkBody = (orders, extra = {}, { scope = 'jp' } = {}) => ({
  run_id: `ship_${202609300000000 + (++runSeq)}_abcdef`, batch_seq: ++batch, chunk_index: 0, last: true, transform_version: V2,
  rows: orders.map(([no, lines]) => ({ mall: 'amazon', scope_key: scope, mall_order_no: no, header: { transform_version: V2, set_checksum: orderFinanceChecksum(validateFinanceRows(no, lines)) }, lines })),
  ...extra,
});
const sendChunk = async (body, { hooks, recordLedger = true } = {}) => {
  const c = validateFinanceChunk(body);
  const r = await ingestOrderFinanceChunk(db, { ...c, hooks, log: quiet });
  if (recordLedger && r.failed.length === 0 && r.stale === 0 && c.scope === 'jp') {
    for (const x of c.rows) ledger.set(x.mall_order_no, { mall_order_no: x.mall_order_no, set_checksum: x.set_checksum, transform_version: V2, lines: x.lines.length });
  }
  return r;
};
const expected = () => receiptDigest([...ledger.values()]);

// ─── coverage の要求 ───
const TOK = 'tok-aaaaaaaaaaaaaaaa', TOK2 = 'tok-bbbbbbbbbbbbbbbb';
const key = { mall: 'amazon', scope: 'jp', source: U };
const policyFp = async () => (await one(`select core.finance_policy_fingerprint(1::smallint, 'amazon', 'jp') as f`)).f;
const PFP = await policyFp();   // 今の policy (amazon / jp = unified [2026-01-01, 無期限)) の指紋
const upd = (generation, run_token = TOK, x = {}) => applyCoverage(db, { state: 'updating', ...key, generation, run_token, ...x }, { log: quiet });
const M = (rc, x = {}) => ({
  complete_to: '2026-06-10', settlements_through: '2026-06-10T15:00:00Z', source_revision: 42,   // JST 6/11 00:00 = end → complete_to は前日の 6/10
  headers_count: 3, headers_checksum: H('headers'), receipt_count: rc.count, receipt_lines: rc.lines, receipt_digest: rc.digest,
  inventory_snapshot_id: '17', inventory_count: 5, inventory_digest: H('inventory'), inventory_completed_at: '2026-06-11T01:00:00Z',
  initial_marker_id: '1', initial_marker_digest: H('marker'), selected_documents_count: 3, selected_documents_digest: H('documents'),
  evidence_chain_from: '2025-12-01T00:00:00Z', evidence_chain_through: '2026-06-11T01:00:00Z', expected_report_count: 20, expected_report_digest: H('expected'), inventory_runs_digest: H('runs'), policy_fingerprint: PFP,
  ...x,
});
const comp = (generation, run_token = TOK, manifest = M(expected()), x = {}) => applyCoverage(db, { state: 'complete', ...key, generation, run_token, manifest, ...x }, { log: quiet });
const covRow = async () => (await pg.query(`select state, generation::text as g, run_token, complete_to::text as complete_to, source_revision::text as rev, receipt_digest, request_hash,
    completed_at, invalidated_at, invalidated_reason from core.finance_coverage where source = $1`, [U])).rows[0] || null;
const stateFn = async (source = U) => (await pg.query(`select complete_to::text as complete_to, generation::text as g, source_revision::text as rev from core.finance_coverage_state(1::smallint, 'amazon', 'jp', $1)`, [source])).rows;
const receiptOf = async (no) => (await pg.query(`select set_checksum, lines, received_batch_seq::text as seq from core.order_finance_receipts where mall_order_no = $1`, [no])).rows[0] || null;

// 最初の財務 (送り手の今の形 = token なし)
await sendChunk(chunkBody([
  ['A-1', [row('2026-06-05', 'LA', { units_ordered: 1, sales_principal_jpy: 1000, commission_jpy: -100 })]],
  ['A-2', [row('2026-06-12', 'LA', { units_ordered: 1, sales_principal_jpy: 1000 })]],
  [pseudoOrderNo('2026-06-05'), [row('2026-06-05', '-', { line_kind: 'storage', fba_storage_jpy: -50, account_fee_amount_jpy: -50 })]],
  ['B+/x', [row('2026-06-06', 'LA', { units_ordered: 1, sales_principal_jpy: 500 }), row('2026-06-07', 'LA', { refund_principal_jpy: -500, refund_principal_customer_jpy: -500 })]],
  ['Zed', [row('2026-06-06', 'LA', { units_ordered: 1, sales_principal_jpy: 10 })]],
  ['a-lower', [row('2026-06-06', 'LA', { units_ordered: 1, sales_principal_jpy: 10 })]],
]));
// 墓石 (lines = 0) と別の scope = digest に入らない
await sendChunk(chunkBody([['T-1', [row('2026-06-06', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]]));
await sendChunk(chunkBody([['T-1', []]]));
ledger.set('T-1', { mall_order_no: 'T-1', set_checksum: (await receiptOf('T-1')).set_checksum, transform_version: V2, lines: 0 });
await sendChunk(chunkBody([['OTHER-1', [row('2026-06-06', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]], {}, { scope: 'us' }));

console.log('0050: 表と差し込み口');
await t('表の列 = §3.1 の一覧 (manifest の列は共通の部品 MANIFEST_FIELDS と同じ名前)・主キー = 会社 × モール × scope × source・core.finance_coverage_state は同じ形のまま (plpgsql・戻りの型)', async () => {
  const cols = (await pg.query(`select column_name as c from information_schema.columns where table_schema = 'core' and table_name = 'finance_coverage'`)).rows.map((r) => r.c);
  for (const c of ['company_id', 'mall', 'scope_key', 'source', 'state', 'generation', 'run_token', 'updating_at', 'request_hash', 'completed_at', 'invalidated_at', 'invalidated_reason', ...Object.keys(MANIFEST_FIELDS)]) assert.ok(cols.includes(c), `列 ${c} が無い`);
  const pk = (await pg.query(`select a.attname as c from pg_index i join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey) where i.indrelid = 'core.finance_coverage'::regclass and i.indisprimary order by array_position(i.indkey, a.attnum)`)).rows.map((r) => r.c);
  assert.deepEqual(pk, ['company_id', 'mall', 'scope_key', 'source']);
  const f = await one(`select l.lanname as lang, pg_get_function_result(p.oid) as r from pg_proc p join pg_language l on l.oid = p.prolang where p.oid = 'core.finance_coverage_state(smallint,text,text,text)'::regprocedure`);
  assert.deepEqual([f.lang, f.r], ['plpgsql', 'TABLE(complete_to date, generation bigint, source_revision bigint)']);
  assert.deepEqual(await stateFn(), [{ complete_to: null, g: null, rev: null }]);   // 行なし = 1 行・全部 null (0049 と同じ)
});
await t('🚨 表の CHECK: manifest の欠けた complete・complete_to が end の JST の日の前日でない・無効の印つきの complete・形の違う token は入らない', async () => {
  const ins = (state, x = '') => pg.query(`insert into core.finance_coverage (company_id, mall, scope_key, source, state, generation, run_token, updating_at${x ? ', ' + x.split('=')[0] : ''})
    values (1, 'amazon', 'zz', '${U}', '${state}', 1, '${TOK}', now()${x ? ', ' + x.split('=')[1] : ''})`);
  await rejects(() => ins('complete'), null, /ck_finance_coverage_complete/);
  await rejects(() => pg.query(`insert into core.finance_coverage (company_id, mall, scope_key, source, state, generation, run_token, updating_at, complete_to, settlements_through)
    values (1, 'amazon', 'zz', '${U}', 'updating', 1, '${TOK}', now(), '2026-06-11', '2026-06-10T15:00:00Z')`), null, /ck_finance_coverage_complete_to/);
  await rejects(() => pg.query(`insert into core.finance_coverage (company_id, mall, scope_key, source, state, generation, run_token, updating_at) values (1, 'amazon', 'zz', '${U}', 'updating', 1, 'short', now())`), null, /run_token/);
  await rejects(() => ins('updating', 'invalidated_at=now()'), null, /ck_finance_coverage_invalidated/);
  assert.equal(Number((await one(`select count(*) as n from core.finance_coverage`)).n), 0);
});

console.log('receipt digest (送り手と受け口が同じ関数)');
await t('1 行ずつ足した digest = 全体の正規の JSON {format, receipts} の SHA-256・空の集合も同じ形', async () => {
  const rs = [{ mall_order_no: 'A', set_checksum: 'c1', transform_version: V2, lines: 2 }, { mall_order_no: 'B', set_checksum: 'c2', transform_version: null, lines: 1 }];
  const d = createReceiptDigester(); for (const r of rs) d.add(r);
  const got = d.finish();
  assert.deepEqual(got, { count: 2, lines: 3, digest: canonicalSha256({ format: RECEIPT_DIGEST_FORMAT, receipts: rs.map((r) => ({ lines: r.lines, mall_order_no: r.mall_order_no, set_checksum: r.set_checksum, transform_version: r.transform_version })) }) });
  // 手で書いた正規の JSON (canonical-hash.mjs を通さない)
  assert.equal(got.digest, H('{"format":"frd-v1","receipts":[{"lines":2,"mall_order_no":"A","set_checksum":"c1","transform_version":"amazon_finance_v2"},{"lines":1,"mall_order_no":"B","set_checksum":"c2","transform_version":null}]}'));
  assert.deepEqual(receiptDigest([]), { count: 0, lines: 0, digest: H('{"format":"frd-v1","receipts":[]}') });
});
await t('🚨 並びは UTF-8 のバイトの順 (UTF-16 の順と違う文字でも)・墓石 (lines = 0) は除く・順の崩れ / 同じ番号 2 回 / lines 0 を add = 例外', async () => {
  const a = { mall_order_no: '～', set_checksum: 'x', transform_version: V2, lines: 1 }, b = { mall_order_no: '\u{1F600}', set_checksum: 'y', transform_version: V2, lines: 1 };
  // UTF-16 では 😀 (D83D…) < ～ (FF5E) / UTF-8 では ～ (EF BD 9E) < 😀 (F0 9F…)
  assert.equal(receiptDigest([b, a, { mall_order_no: 'z', set_checksum: 'q', transform_version: V2, lines: 0 }]).digest, canonicalSha256({ format: RECEIPT_DIGEST_FORMAT, receipts: [a, b] }));
  const d = createReceiptDigester(); d.add(b);
  assert.throws(() => d.add(a), /UTF-8 のバイトの順でない/);
  const d2 = createReceiptDigester(); d2.add(a);
  assert.throws(() => d2.add(a), /UTF-8 のバイトの順でない/);
  assert.throws(() => createReceiptDigester().add({ ...a, lines: 0 }), /lines は 1 以上/);
});
await t('🚨 Render の cursor の計算 (collate "C"・区切りの大きさに依らない) = 送り手の写し (ledger) から JS で作った値: 墓石・別の scope は入らない・疑似注文は入る', async () => {
  const exp = expected();
  assert.equal(exp.count, 6); assert.equal(exp.lines, 7);
  for (const fetchSize of [1, 2, 3, 6, 10000]) {
    await pg.exec('begin');
    const got = await computeReceiptDigest(db, { mall: 'amazon', scope: 'jp', fetchSize });
    await pg.exec('commit');
    assert.deepEqual(got, exp, `fetchSize ${fetchSize}`);
  }
  // SQL の並び = バイトの順 ('+' < '-' < 'A' < 'B' < 'Z' < 'a')
  const nos = (await pg.query(`select mall_order_no from core.order_finance_receipts where scope_key = 'jp' and lines > 0 order by mall_order_no collate "C"`)).rows.map((r) => r.mall_order_no);
  assert.deepEqual(nos, ['-:2026-06-05', 'A-1', 'A-2', 'B+/x', 'Zed', 'a-lower']);
});

await t('🚨 policy の指紋: SQL (core.finance_policy_fingerprint) = JS (policyFingerprint)・手で書いた正規の JSON・並び (period_from → source)・null の period_to', async () => {
  const cur = [{ period_from: '2026-01-01', period_to: null, source: U }];
  assert.equal(PFP, policyFingerprint(cur));
  assert.equal(PFP, H(`{"format":"fpf-v1","policies":[{"period_from":"2026-01-01","period_to":null,"source":"${U}"}]}`));
  // 行が 2 つ (別の scope に置いて比べる・並びは period_from の順)
  await pg.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, period_to, source) values
    (1, 'amazon', 'fpt', '2026-03-01', null, 'amazon_settlement_flat_v2'), (1, 'amazon', 'fpt', '2025-06-01', '2026-03-01', 'amazon_settlement_flat_v1')`);
  const two = [{ period_from: '2026-03-01', period_to: null, source: 'amazon_settlement_flat_v2' }, { period_from: '2025-06-01', period_to: '2026-03-01', source: 'amazon_settlement_flat_v1' }];
  assert.equal((await one(`select core.finance_policy_fingerprint(1::smallint, 'amazon', 'fpt') as f`)).f, policyFingerprint(two));
  assert.equal((await one(`select core.finance_policy_fingerprint(1::smallint, 'amazon', 'none') as f`)).f, H('{"format":"fpf-v1","policies":[]}'));   // policy なし = 空の配列
  await pg.query(`delete from core.finance_source_policy where scope_key = 'fpt'`);
});

console.log('状態の移り方 (§3.1)');
await t('🚨 行なしの complete = 409 (新しい世代は updating から)・updating = applied → 同じ token の再送 = same → 違う token = 409・この世代の complete の違う token = 409・新しい世代の complete = 409', async () => {
  await rejects(() => comp(5), 'CONFLICT', /updating を受けていない/);
  assert.equal(await covRow(), null);
  assert.deepEqual(await upd(5), { status: 'applied', state: 'updating', generation: '5' });
  assert.deepEqual(await upd('5'), { status: 'same', state: 'updating', generation: '5' });   // 10 進の文字列でも同じ世代
  await rejects(() => upd(5, TOK2), 'CONFLICT', /別の token/);
  await rejects(() => comp(5, TOK2), 'CONFLICT', /token と違う/);
  await rejects(() => comp(6), 'CONFLICT', /updating を受けていない/);
  assert.deepEqual(await stateFn(), [{ complete_to: null, g: '5', rev: null }]);   // updating = complete_to null・世代は返す
});
await t('🚨 古い世代は stale (updating も complete も何もしない)', async () => {
  assert.deepEqual(await upd(4), { status: 'stale', state: 'updating', generation: '4', current_generation: '5' });
  assert.deepEqual(await comp(4), { status: 'stale', state: 'updating', generation: '4', current_generation: '5' });
  assert.equal((await covRow()).g, '5');
});
await t('🚨 receipt digest が manifest と違えば 409 (数・行の数・digest のどれか)・状態は updating のまま', async () => {
  const exp = expected();
  for (const bad of [{ receipt_count: exp.count + 1, receipt_lines: exp.lines + 1 }, { receipt_lines: exp.lines + 1 }, { receipt_digest: H('other') }]) {
    const e = await rejects(() => comp(5, TOK, M(exp, bad)), 'RECEIPT_MISMATCH', /受領記録が manifest と違う/);
    assert.deepEqual(e.detail.render, exp);
  }
  assert.deepEqual([(await covRow()).state, (await covRow()).complete_to], ['updating', null]);
});
await t('🚨 updating → complete は 1 回だけ・再送は request_hash が同じなら same (サーバーの列は比べない)・違えば 409・complete → updating は 409', async () => {
  const r = await comp(5);
  assert.deepEqual([r.status, r.state, r.generation, r.complete_to, r.receipt], ['applied', 'complete', '5', '2026-06-10', expected()]);
  const c1 = await covRow();
  assert.deepEqual([c1.state, c1.complete_to, c1.rev, c1.receipt_digest], ['complete', '2026-06-10', '42', expected().digest]);
  assert.equal(c1.request_hash, coverageRequestHash({ companyId: 1, mall: 'amazon', scopeKey: 'jp', source: U, generation: 5, runToken: TOK, manifest: validateCoverageManifest(M(expected())) }));
  assert.deepEqual(await stateFn(), [{ complete_to: '2026-06-10', g: '5', rev: '42' }]);
  const again = await comp(5, TOK, M(expected(), { source_revision: '42' }));   // 同じ中身 (bigint は数でも文字列でも同じ)
  assert.deepEqual([again.status, again.complete_to], ['same', '2026-06-10']);
  assert.equal(String((await covRow()).completed_at), String(c1.completed_at));   // completed_at は変えない
  await rejects(() => comp(5, TOK, M(expected(), { source_revision: 43 })), 'CONFLICT', /別の中身/);
  await rejects(() => comp(5, TOK2), 'CONFLICT', /別の中身/);   // token も hash に入る
  await rejects(() => upd(5), 'CONFLICT', /complete → updating はしない/);
  await rejects(() => upd(5, TOK2), 'CONFLICT', /complete → updating/);
  assert.equal((await covRow()).state, 'complete');
});
await t('🚨 新しい世代の updating = 前の complete を無効に (manifest の列を空に・complete_to は null と読む)・前の世代の complete の再送は stale', async () => {
  assert.deepEqual(await upd(6, TOK2), { status: 'applied', state: 'updating', generation: '6' });
  const c = await covRow();
  assert.deepEqual([c.state, c.g, c.run_token, c.complete_to, c.rev, c.receipt_digest, c.request_hash, c.completed_at], ['updating', '6', TOK2, null, null, null, null, null]);
  assert.deepEqual(await stateFn(), [{ complete_to: null, g: '6', rev: null }]);
  assert.equal((await comp(5)).status, 'stale');
  // 無効にした後に失敗した回 (complete が来ない) = updating のまま (前の complete に戻さない)
  assert.equal((await covRow()).state, 'updating');
});
await t('manifest の形 (400): complete_to が end の JST の日の前日でない・未来の時刻・欠けた列・知らない鍵・updating に manifest・token の形・世代 0・request_hash の食い違い・起点を覆わない・policy に無い source (409)', async () => {
  const rc = expected();
  await rejects(() => comp(6, TOK2, M(rc, { complete_to: '2026-06-11' })), 'BAD_REQUEST', /JST の日の前日/);
  await rejects(() => comp(6, TOK2, M(rc, { settlements_through: '2026-06-10T14:59:59Z' })), 'BAD_REQUEST', /JST の日の前日/);   // JST 6/10 23:59:59 → complete_to 6/09
  await rejects(() => comp(6, TOK2, M(rc, { settlements_through: '2099-01-01T15:00:00Z', complete_to: '2099-01-01' })), 'BAD_REQUEST', /未来/);
  const { inventory_digest, ...missing } = M(rc);
  await rejects(() => comp(6, TOK2, missing), 'BAD_REQUEST', /inventory_digest が無い/);
  await rejects(() => comp(6, TOK2, M(rc, { inventory_digest: null })), 'BAD_REQUEST', /inventory_digest が無い/);
  await rejects(() => comp(6, TOK2, M(rc, { surprise: 1 })), 'BAD_REQUEST', /知らない鍵/);
  await rejects(() => comp(6, TOK2, M(rc, { headers_count: 0, selected_documents_count: 0 })), 'BAD_REQUEST', /headers_count は 1 以上/);
  await rejects(() => comp(6, TOK2, M(rc, { selected_documents_count: 4 })), 'BAD_REQUEST', /selected_documents_count/);
  await rejects(() => comp(6, TOK2, M(rc, { receipt_digest: 'ABC' })), 'BAD_REQUEST', /64 桁/);
  await rejects(() => comp(6, TOK2, M(rc, { evidence_chain_from: '2026-07-01T00:00:00Z' })), 'BAD_REQUEST', /evidence_chain_from/);
  await rejects(() => comp(6, TOK2, M(rc, { settlements_through: '2026-06-10T15:00:00.000Z' })), 'BAD_REQUEST', /UTC の YYYY-MM-DDTHH:MM:SSZ/);
  await rejects(() => comp(6, TOK2, M(rc, { source_revision: -1 })), 'BAD_REQUEST', /source_revision/);
  await rejects(() => comp(6, TOK2, M(rc, { inventory_snapshot_id: 17 })), 'BAD_REQUEST', /inventory_snapshot_id/);   // ID は 10 進の文字列
  await rejects(() => comp(6, TOK2, M(rc), { request_hash: H('x') }), 'BAD_REQUEST', /request_hash が受け口の計算と違う/);
  await rejects(() => upd(6, TOK2, { manifest: M(rc) }), 'BAD_REQUEST', /updating に manifest/);
  await rejects(() => upd(6, 'short'), 'BAD_REQUEST', /run_token/);
  await rejects(() => upd(0), 'BAD_REQUEST', /generation/);
  await rejects(() => upd(1.5), 'BAD_REQUEST', /generation/);
  await rejects(() => applyCoverage(db, { state: 'updating', ...key, generation: 6, run_token: TOK2, extra: 1 }), 'BAD_REQUEST', /知らない鍵/);
  await rejects(() => applyCoverage(db, { state: 'done', ...key, generation: 6, run_token: TOK2 }), 'BAD_REQUEST', /state/);
  // 起点 (policy の 2026-01-01 の JST 00:00 = 2025-12-31T15:00:00Z) を覆わない end
  await rejects(() => comp(6, TOK2, M(rc, { complete_to: '2025-12-31', settlements_through: '2025-12-31T15:00:00Z' })), 'BAD_REQUEST', /policy の起点/);
  await rejects(() => applyCoverage(db, { state: 'updating', ...key, source: 'amazon_settlement_flat_v1', generation: 1, run_token: TOK }), 'NO_POLICY');
  // 🚨 送り手が検査した policy の指紋が今と違う = 409 POLICY_MISMATCH (#1561 Codex R2 High)・指紋の形は 400
  await rejects(() => comp(6, TOK2, M(rc, { policy_fingerprint: H('old policy') })), 'POLICY_MISMATCH', /今の policy の指紋/);
  await rejects(() => comp(6, TOK2, M(rc, { policy_fingerprint: 'x' })), 'BAD_REQUEST', /policy_fingerprint/);
  const { policy_fingerprint, ...noFp } = M(rc);
  await rejects(() => comp(6, TOK2, noFp), 'BAD_REQUEST', /policy_fingerprint が無い/);
  // 🚨 証拠の鎖が policy の起点まで届いていない (#1561 Codex R1 High 2): 起点 = 2026-01-01 の JST 00:00 = 2025-12-31T15:00:00Z (UTC の瞬間で比べる)
  await rejects(() => comp(6, TOK2, M(rc, { evidence_chain_from: '2025-12-31T15:00:01Z' })), 'BAD_REQUEST', /evidence_chain_from .*policy の起点 \(2025-12-31T15:00:00Z\) より後/);
  await rejects(() => comp(6, TOK2, M(rc, { evidence_chain_from: '2026-01-01T00:00:00Z' })), 'BAD_REQUEST', /evidence_chain_from/);   // UTC の 1/1 00:00 = JST の 1/1 09:00 = 起点より後
  assert.equal((await covRow()).state, 'updating');
  // 正しい request_hash を付ければ通る (送り手が同じ関数で作った値)・証拠の鎖の始まりが起点ちょうど (境目) も通る
  const mEdge = M(rc, { evidence_chain_from: '2025-12-31T15:00:00Z' });
  const rh = coverageRequestHash({ companyId: 1, mall: 'amazon', scopeKey: 'jp', source: U, generation: '6', runToken: TOK2, manifest: validateCoverageManifest(mEdge) });
  assert.equal((await comp(6, TOK2, mEdge, { request_hash: rh })).status, 'applied');
  assert.equal((await covRow()).request_hash, rh);
});

console.log('財務の chunk を coverage の世代に縛る (§3.1 R10 H1)');
const tokChunk = (orders, generation, run_token) => chunkBody(orders, { coverage_generation: generation, run_token });
await t('🚨 token 付きの chunk: complete の後 (遅れて届いた) = 409・何も書かない / coverage_generation と run_token は両方か両方なし (400)', async () => {
  const before = await receiptOf('A-1');
  await rejects(() => sendChunk(tokChunk([['A-1', [row('2026-06-05', 'LA', { units_ordered: 9, sales_principal_jpy: 9 })]]], 6, TOK2)), 'COVERAGE_MISMATCH', /updating が無い/);
  assert.deepEqual(await receiptOf('A-1'), before);
  assert.equal((await covRow()).state, 'complete');
  assert.throws(() => validateFinanceChunk(chunkBody([['A-1', []]], { coverage_generation: 6 })), /両方/);
  assert.throws(() => validateFinanceChunk(chunkBody([['A-1', []]], { run_token: TOK })), /両方/);
  assert.throws(() => validateFinanceChunk(chunkBody([['A-1', []]], { coverage_generation: 0, run_token: TOK })), /coverage_generation/);
  assert.throws(() => validateFinanceChunk(chunkBody([['A-1', []]], { coverage_generation: 6, run_token: 'x' })), /run_token/);
});
await t('🚨 token 付きの chunk: その世代・その token の updating のときだけ適用・別の token (別の送り手)・古い世代・別の source の行 = 409', async () => {
  await upd(7, TOK);
  const body = (gen, tok, lines) => tokChunk([['C-1', lines || [row('2026-06-08', 'LA', { units_ordered: 1, sales_principal_jpy: 300 })]]], gen, tok);
  await rejects(() => sendChunk(body(7, TOK2)), 'COVERAGE_MISMATCH');
  await rejects(() => sendChunk(body(6, TOK2)), 'COVERAGE_MISMATCH');
  await rejects(() => sendChunk(body(8, TOK)), 'COVERAGE_MISMATCH');
  await rejects(() => sendChunk(body(7, TOK, [row('2026-06-08', 'LA', { units_ordered: 1, sales_principal_jpy: 300, source: 'amazon_settlement_flat_v2' })])), 'COVERAGE_MISMATCH', /source/);
  assert.equal(await receiptOf('C-1'), null);
  const r = await sendChunk(body(7, TOK));
  assert.deepEqual([r.applied, r.coverage_invalidated], [1, 0]);
  assert.equal((await covRow()).state, 'updating');
  assert.equal((await comp(7, TOK, M(expected(), { source_revision: 50 }))).status, 'applied');
});
await t('🚨 順序の逆転: 旧い chunk を lock を取る前で止める → 新しい世代の updating → 旧い chunk を再開 = token が違うので 409 (旧い世代の書き込みは入らない)', async () => {
  await upd(8, TOK);
  let release; const gate = new Promise((res) => { release = res; });
  let reached; const paused = new Promise((res) => { reached = res; });
  const old = sendChunk(tokChunk([['D-1', [row('2026-06-09', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]], 8, TOK), { hooks: { beforeBegin: async () => { reached(); await gate; } } });
  const settled = old.then(() => null, (e) => e);
  await paused;
  await upd(9, TOK2);   // 旧い chunk が止まっている間に新しい世代
  release();
  const e = await settled;
  assert.ok(e && e.code === 'COVERAGE_MISMATCH', String(e && e.message));
  assert.equal(await receiptOf('D-1'), null);
  assert.deepEqual([(await covRow()).g, (await covRow()).state], ['9', 'updating']);
});
await t('🚨 lock: token 付き / なしの chunk・updating・complete が **同じ advisory lock** を取引の中で持つ (取引の後は放す)', async () => {
  const held = async () => (await pg.query(`select classid::text || ':' || objid::text || ':' || objsubid::text as k from pg_locks where locktype = 'advisory' and granted order by 1`)).rows.map((r) => r.k);
  const want = (await one(`select ((hashtext($1)::bigint >> 32) & 4294967295)::oid::text || ':' || (hashtext($1)::bigint & 4294967295)::oid::text || ':1' as k`, [financeLockKey(1, 'amazon', 'jp')])).k;
  const seen = {};
  await sendChunk(tokChunk([['E-1', [row('2026-06-09', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]], 9, TOK2), { hooks: { afterLock: async () => { seen.tokChunk = await held(); } } });
  await applyCoverage(db, { state: 'complete', ...key, generation: 9, run_token: TOK2, manifest: M(expected()) }, { hooks: { afterLock: async () => { seen.complete = await held(); } } });
  await sendChunk(chunkBody([['E-2', [row('2026-06-09', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]]), { hooks: { afterLock: async () => { seen.plainChunk = await held(); } } });
  await applyCoverage(db, { state: 'updating', ...key, generation: 10, run_token: TOK }, { hooks: { afterLock: async () => { seen.updating = await held(); } } });
  assert.deepEqual(seen, { tokChunk: [want], complete: [want], plainChunk: [want], updating: [want] });
  assert.deepEqual(await held(), []);   // 取引の lock = commit で放す
  // 別の scope は別の lock
  assert.notEqual(financeLockKey(1, 'amazon', 'jp'), financeLockKey(1, 'amazon', 'us'));
});

console.log('token の無い chunk (今の送り手・互換)');
await t('🚨 受け取る・受領記録を変えたら complete → updating (無効の印)・同じ世代では complete に戻れない (409)・次の世代から complete できる', async () => {
  assert.equal((await comp(10, TOK, M(expected()))).status, 'applied');
  const r = await sendChunk(chunkBody([['A-1', [row('2026-06-05', 'LA', { units_ordered: 1, sales_principal_jpy: 1000, commission_jpy: -110 })]]]));
  assert.deepEqual([r.applied, r.coverage_invalidated], [1, 1]);
  const c = await covRow();
  assert.deepEqual([c.state, c.g, c.invalidated_reason, c.invalidated_at != null, c.complete_to], ['updating', '10', 'untokened_finance_write', true, '2026-06-10']);   // manifest は監査に残す
  assert.deepEqual(await stateFn(), [{ complete_to: null, g: '10', rev: null }]);   // でも complete_to は null と読む
  await rejects(() => comp(10, TOK, M(expected())), 'CONFLICT', /無効にされた/);   // 同じ世代・同じ中身の再送でも (受領記録が合っていても) 戻さない
  assert.deepEqual(await upd(10, TOK), { status: 'same', state: 'updating', generation: '10' });
  // token の無い書き込みの後は complete が無い = 次もう 1 回書いても落とすものは無い
  assert.equal((await sendChunk(chunkBody([['A-3', [row('2026-06-05', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]]))).coverage_invalidated, 0);
  assert.equal((await upd(11, TOK2)).status, 'applied');
  const c2 = await covRow();
  assert.deepEqual([c2.invalidated_at, c2.invalidated_reason], [null, null]);
  assert.equal((await comp(11, TOK2, M(expected()))).status, 'applied');
});
await t('🚨 受領記録を変えない token の無い chunk (same = 同じ中身・stale = 古い世代・墓石の再送) は complete を落とさない', async () => {
  const lines = [row('2026-06-12', 'LA', { units_ordered: 1, sales_principal_jpy: 1000 })];
  const r1 = await sendChunk(chunkBody([['A-2', lines]]));
  assert.deepEqual([r1.applied, r1.same, r1.coverage_invalidated], [0, 1, 0]);
  const stale = chunkBody([['A-2', [row('2026-06-12', 'LA', { units_ordered: 5 })]]]); stale.batch_seq = 1;
  const r2 = await sendChunk(stale, { recordLedger: false });
  assert.deepEqual([r2.applied, r2.stale, r2.coverage_invalidated], [0, 1, 0]);
  assert.equal((await covRow()).state, 'complete');
  assert.deepEqual(await stateFn(), [{ complete_to: '2026-06-10', g: '11', rev: '42' }]);
});
await t('source ごと: 別の source の coverage は触らない読み方 (行なし = null)・token の無い書き込みはその key の全部の source の complete を落とす (受領記録は source を持たない = fail-closed)', async () => {
  assert.deepEqual(await stateFn('amazon_settlement_flat_v2'), [{ complete_to: null, g: null, rev: null }]);
  // 別の source の complete の行を直接置く (policy は 1 つなので受け口では作れない = 表に直接)
  await pg.query(`insert into core.finance_coverage select company_id, mall, scope_key, 'amazon_settlement_flat_v2', state, generation, run_token, updating_at, complete_to, settlements_through, source_revision,
      headers_count, headers_checksum, receipt_count, receipt_lines, receipt_digest, inventory_snapshot_id, inventory_count, inventory_digest, inventory_completed_at, initial_marker_id, initial_marker_digest,
      selected_documents_count, selected_documents_digest, evidence_chain_from, evidence_chain_through, expected_report_count, expected_report_digest, inventory_runs_digest, policy_fingerprint, request_hash, completed_at
    from core.finance_coverage where source = $1`, [U]);
  const r = await sendChunk(chunkBody([['A-4', [row('2026-06-05', 'LA', { units_ordered: 1, sales_principal_jpy: 2 })]]]));
  assert.equal(r.coverage_invalidated, 2);
  const st = (await pg.query(`select source, state from core.finance_coverage order by source`)).rows;
  assert.deepEqual(st, [{ source: 'amazon_settlement_flat_v2', state: 'updating' }, { source: U, state: 'updating' }]);
  await pg.query(`delete from core.finance_coverage where source = 'amazon_settlement_flat_v2'`);
});
await t('🚨 source が 2 つ (#1561 Codex R1 High 1): 過去の source A が complete・今の source B が updating → B の token で A の既存の行の置き換え・墓石 = 409 (消す前に) / B の token の追加・墓石 = A が updating に落ちる / same では落とさない', async () => {
  const A = 'amazon_settlement_flat_v1';
  // A の complete の行 (policy は 1 つなので受け口では作れない = 表に直接。manifest は形だけ)
  const putA = async () => {
    await pg.query(`delete from core.finance_coverage where source = $1`, [A]);
    await pg.query(`insert into core.finance_coverage (company_id, mall, scope_key, source, state, generation, run_token, updating_at, complete_to, settlements_through, source_revision,
        headers_count, headers_checksum, receipt_count, receipt_lines, receipt_digest, inventory_snapshot_id, inventory_count, inventory_digest, inventory_completed_at, initial_marker_id, initial_marker_digest,
        selected_documents_count, selected_documents_digest, evidence_chain_from, evidence_chain_through, expected_report_count, expected_report_digest, inventory_runs_digest, policy_fingerprint, request_hash, completed_at)
      values (1, 'amazon', 'jp', $1, 'complete', 3, $2, now(), '2026-06-10', '2026-06-10T15:00:00Z', 1, 1, $3, 0, 0, $3, '1', 0, $3, '2026-06-11T00:00:00Z', '1', $3, 1, $3,
        '2025-12-01T00:00:00Z', '2026-06-11T00:00:00Z', 0, $3, $3, $4, $3, now())`, [A, TOK2, H('a'), PFP]);
  };
  const stA = async () => (await pg.query(`select state, invalidated_reason from core.finance_coverage where source = $1`, [A])).rows[0];
  // A の既存の行 (token 無しで入れる = 今の送り手と同じ道)
  await sendChunk(chunkBody([['SA-1', [row('2026-06-25', 'ZZ', { units_ordered: 1, sales_principal_jpy: 7, source: A })]]]));
  await putA();
  await upd(12, TOK);   // B (= U) の今の世代
  const before = await receiptOf('SA-1');
  const nA = async () => Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'SA-1' and source = $1`, [A])).n);
  // ① B の token で A の既存の行を置き換える / 墓石にする = 409・何も消さない・A は complete のまま
  await rejects(() => sendChunk(tokChunk([['SA-1', [row('2026-06-25', 'ZZ', { units_ordered: 1, sales_principal_jpy: 7 })]]], 12, TOK)), 'COVERAGE_MISMATCH', /既存の行に別の source \(amazon_settlement_flat_v1\)/);
  await rejects(() => sendChunk(tokChunk([['SA-1', []]], 12, TOK)), 'COVERAGE_MISMATCH', /既存の行に別の source/);
  assert.deepEqual([await receiptOf('SA-1'), await nA()], [before, 1]);
  assert.deepEqual(await stA(), { state: 'complete', invalidated_reason: null });
  // ② B の token の追加 = 受領記録が変わる = A の complete は今の受領記録と食い違う → updating (ほかの coverage の書き込み)。B 自身は updating のまま (印なし)
  const r1 = await sendChunk(tokChunk([['SB-1', [row('2026-06-25', 'ZZ', { units_ordered: 1, sales_principal_jpy: 3 })]]], 12, TOK));
  assert.deepEqual([r1.applied, r1.coverage_invalidated], [1, 1]);
  assert.deepEqual(await stA(), { state: 'updating', invalidated_reason: 'other_coverage_finance_write' });
  const b = await covRow();
  assert.deepEqual([b.state, b.g, b.invalidated_at], ['updating', '12', null]);
  // ③ B の token の墓石 (Codex の例) も同じ
  await putA();
  const r2 = await sendChunk(tokChunk([['SB-1', []]], 12, TOK));
  assert.deepEqual([r2.applied, r2.coverage_invalidated], [1, 1]);
  assert.deepEqual(await stA(), { state: 'updating', invalidated_reason: 'other_coverage_finance_write' });
  // ④ 受領記録を変えない (same) token の chunk は落とさない
  await putA();
  const r3 = await sendChunk(tokChunk([['SB-1', []]], 12, TOK));
  assert.deepEqual([r3.applied, r3.same, r3.coverage_invalidated], [0, 1, 0]);
  assert.deepEqual(await stA(), { state: 'complete', invalidated_reason: null });
  await pg.query(`delete from core.finance_coverage where source = $1`, [A]);
});

console.log('core.finance_coverage_state の差し替え = 0049 の mart が正式な値を出す');
const profitRows = async () => (await pg.query(`select economic_date_jst::text as d, listing_id, day_finance_status, contribution_before_ad_incl_jpy, finance_coverage_generation::text as g,
    finance_source_revision::text as rev from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-06-01', '2026-06-30') where listing_id = $1 order by 1`, [LA])).rows;
await t('🚨 complete のときだけ: complete_to (6/10) 以前の日 = complete・正式な寄与が出る / それより後 = provisional・null / 世代と版も coverage から', async () => {
  // 今は updating (世代 12) = 全部 null
  let rows = await profitRows();
  assert.ok(rows.length > 0 && rows.every((r) => r.day_finance_status !== 'complete' && r.contribution_before_ad_incl_jpy == null), JSON.stringify(rows));
  assert.ok(rows.every((r) => r.g === '12' && r.rev == null));
  await upd(12, TOK);
  assert.equal((await comp(12, TOK, M(expected(), { source_revision: 77 }))).status, 'applied');
  rows = await profitRows();
  const d5 = rows.find((r) => r.d === '2026-06-05'), d12 = rows.find((r) => r.d === '2026-06-12');
  assert.equal(d5.day_finance_status, 'complete');
  // 6/5 LA = A-1 (1,000 − 110) + A-3 (1) + A-4 (2) の 3 個 − 原価 300 × 3
  assert.equal(N(d5.contribution_before_ad_incl_jpy), 1000 - 110 + 1 + 2 - 900);
  assert.deepEqual([d12.day_finance_status, d12.contribution_before_ad_incl_jpy], ['provisional', null]);
  assert.ok(rows.every((r) => r.g === '12' && r.rev === '77'));
  const tot = (await pg.query(`select row_kind, economic_date_jst::text as d, day_finance_status from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', '2026-06-09', '2026-06-11') where row_kind = 'day' order by 2`)).rows;
  assert.deepEqual(tot.map((x) => [x.d, x.day_finance_status]), [['2026-06-09', 'complete'], ['2026-06-10', 'complete'], ['2026-06-11', 'missing']]);   // 行の無い日も complete_to までは確定の 0
  // 新しい世代の updating = また全部 null
  await upd(13, TOK2);
  rows = await profitRows();
  assert.ok(rows.every((r) => r.day_finance_status !== 'complete' && r.contribution_before_ad_incl_jpy == null && r.g === '13' && r.rev == null));
});

console.log('policy を後から変える (#1561 Codex R2 High)・返品の状態の partial は coverage 基準 (R2 Medium)');
const setPolicy = (set) => pg.query(`update core.finance_source_policy set ${set} where company_id = 1 and mall = 'amazon' and scope_key = 'jp'`);
const ON = [{ complete_to: '2026-06-10', g: '13', rev: '42' }], OFF = [{ complete_to: null, g: '13', rev: null }];
const POLICY_CHANGES = [
  ['起点を広げる', `period_from = '2025-01-01'`, `period_from = '2026-01-01'`],
  ['起点を狭める', `period_from = '2026-02-01'`, `period_from = '2026-01-01'`],
  ['source を変える', `source = 'amazon_settlement_flat_v2'`, `source = '${U}'`],
  ['終わりを付ける', `period_to = '2027-01-01'`, `period_to = null`],
];
await t('🚨 complete の後に policy を変える (起点を広げる・狭める・source を変える・終わりを付ける) → complete_to は null・正式な値も null / 戻せば (同じ指紋) 出る', async () => {
  // 5 月の返品 (次の試験) を入れてから世代 13 を complete (policy の指紋 = PFP)
  await sendChunk(chunkBody([['MAY-1', [row('2026-05-10', 'MZ', { units_ordered: 2, sales_principal_jpy: 2000 }), row('2026-05-20', 'MZ', { refund_principal_jpy: -1000, refund_principal_customer_jpy: -1000 })]]]));
  assert.equal((await comp(13, TOK2, M(expected()))).status, 'applied');
  const d5 = async () => (await profitRows()).find((r) => r.d === '2026-06-05');
  const day2025 = async () => (await pg.query(`select day_finance_status from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', '2025-06-01', '2025-06-01') where row_kind = 'day'`)).rows[0].day_finance_status;
  assert.deepEqual(await stateFn(), ON);
  assert.equal((await d5()).day_finance_status, 'complete');
  for (const [name, change, back] of POLICY_CHANGES) {
    await setPolicy(change);
    assert.notEqual(await policyFp(), PFP, name);
    assert.deepEqual(await stateFn(), OFF, name);
    const r = await d5();   // source を変えると 6/5 の行は採られない (行が無い) = どちらでも正式な値は無い
    assert.ok(!r || (r.day_finance_status !== 'complete' && r.contribution_before_ad_incl_jpy == null), `${name} ${JSON.stringify(r)}`);
    if (name === '起点を広げる') assert.equal(await day2025(), 'missing', '財務も証拠も無い 2025 年の日が complete (確定の 0) にならない');
    await setPolicy(back);
    assert.equal(await policyFp(), PFP, `${name} を戻す`);
    assert.deepEqual(await stateFn(), ON, `${name} を戻す`);
    assert.equal((await d5()).day_finance_status, 'complete', `${name} を戻す`);
  }
  assert.equal((await covRow()).state, 'complete');   // 行は complete のまま (読み方で null にするだけ = policy を戻せば出る・新しい policy は次の回でやり直す)
});
await t('🚨 返品の状態の partial = coverage 基準 (0047 の差し替え): complete_to 6/10 → 5 月の返品は monthly・6 月は partial / complete_to が null → 5 月も partial・0049 の mart の返品の状態と同じ', async () => {
  const sku = async () => Object.fromEntries((await pg.query(`select economic_date_jst::text || ' ' || seller_sku_norm as k, refund_units_status as s
      from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', '2026-05-01', '2026-06-30') where refund_units_status <> 'no_refund'`)).rows.map((r) => [r.k, r.s]));
  const mart = async () => Object.fromEntries((await pg.query(`select economic_date_jst::text || ' ' || coalesce(listing_id::text, seller_sku_norm) as k, refund_units_status as s
      from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-05-01', '2026-06-30') where refund_units_status <> 'no_refund'`)).rows.map((r) => [r.k, r.s]));
  const M_ = 'estimated_monthly_unit_price', P_ = 'estimated_partial_month_unit_price';
  assert.deepEqual(await sku(), { '2026-05-20 mz': M_, '2026-06-07 la': P_ });   // 5/31 ≤ 6/10 = 月末までそろった / 6/30 > 6/10
  assert.deepEqual(await mart(), { '2026-05-20 mz': M_, [`2026-06-07 ${LA}`]: P_ });
  await setPolicy(`period_to = '2027-01-01'`);   // complete_to が null になる
  assert.deepEqual(await sku(), { '2026-05-20 mz': P_, '2026-06-07 la': P_ });
  assert.deepEqual(await mart(), { '2026-05-20 mz': P_, [`2026-06-07 ${LA}`]: P_ });
  await setPolicy('period_to = null');
  assert.deepEqual(await sku(), { '2026-05-20 mz': M_, '2026-06-07 la': P_ });
});

console.log('月の途中の source の切り替え (#1561 Codex R3 High 1)・policy の読みの競合 (R3 High 2) = 別の DB');
// 別の PGlite: policy = unified (A) [2026-01-01, 2026-07-15) → flat_v2 (B) [2026-07-15, 無期限)。7 月の単価は A の日 (7/5 の売上) と B の日の行から作られる
const p3 = new PGlite();
await applyMigrations(pgliteAdapter(p3), { log: quiet });
const d3 = pgliteAdapter(p3);
const one3 = async (sql, p = []) => (await p3.query(sql, p)).rows[0];
const B = 'amazon_settlement_flat_v2';
await p3.query(`update core.finance_source_policy set period_to = '2026-07-15' where company_id = 1 and mall = 'amazon' and scope_key = 'jp'`);
await p3.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, source) values (1, 'amazon', 'jp', '2026-07-15', $1)`, [B]);
const send3 = (body) => { const c = validateFinanceChunk(body); return ingestOrderFinanceChunk(d3, { ...c, log: quiet }); };
await send3(chunkBody([['J-1', [row('2026-07-05', 'JZ', { units_ordered: 2, sales_principal_jpy: 2000 })]]]));   // A の日の売上 (単価 1,000)
await send3(chunkBody([['J-2', [row('2026-07-20', 'JZ', { refund_principal_jpy: -1000, refund_principal_customer_jpy: -1000, source: B })]]]));   // B の日の返品 (1 個)
const fp3 = async () => (await one3(`select core.finance_policy_fingerprint(1::smallint, 'amazon', 'jp') as f`)).f;
/** complete の行を表に直接置く (受け口を通さない・manifest は形だけ・指紋は今の policy) */
const putCov3 = async (source, completeTo) => {
  await p3.query(`delete from core.finance_coverage where source = $1`, [source]);
  if (!completeTo) return;
  await p3.query(`insert into core.finance_coverage (company_id, mall, scope_key, source, state, generation, run_token, updating_at, complete_to, settlements_through, source_revision,
      headers_count, headers_checksum, receipt_count, receipt_lines, receipt_digest, inventory_snapshot_id, inventory_count, inventory_digest, inventory_completed_at, initial_marker_id, initial_marker_digest,
      selected_documents_count, selected_documents_digest, evidence_chain_from, evidence_chain_through, expected_report_count, expected_report_digest, inventory_runs_digest, policy_fingerprint, request_hash, completed_at)
    values (1, 'amazon', 'jp', $1, 'complete', 1, $2, now(), $3::date, (($3::date + 1)::timestamp at time zone 'Asia/Tokyo'), 1,
      1, $4, 0, 0, $4, '1', 0, $4, now(), '1', $4, 1, $4, '2025-12-01T00:00:00Z', now(), 0, $4, $4, $5, $4, now())`, [source, TOK, completeTo, H('x'), await fp3()]);
};
const julyState = async () => {
  const settled = (await one3(`select core.finance_month_settled(1::smallint, 'amazon', 'jp', '2026-07-01') as s`)).s;
  const sku = (await one3(`select refund_units_status as s from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', '2026-07-01', '2026-07-31') where economic_date_jst = '2026-07-20'`)).s;
  const m = await one3(`select refund_units_status as s, profit_incomplete_reasons as r from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-07-01', '2026-07-31') where economic_date_jst = '2026-07-20'`);
  return [settled, sku, m.s, m.r.includes('refund_units_partial_month')];
};
const MONTHLY = [true, 'estimated_monthly_unit_price', 'estimated_monthly_unit_price', false];
const PARTIAL = [false, 'estimated_partial_month_unit_price', 'estimated_partial_month_unit_price', true];
await t('🚨 月の途中で A → B: A が未完了・B が完了 → partial (0047 の差し替えと 0049 の mart の両方・正式な値は出ない) / 両方がその日まで完了 → monthly', async () => {
  await putCov3(U, '2026-07-10'); await putCov3(B, '2026-07-31');   // A は 7/11〜7/14 が未完了 (返品の日の source = B は月末まで完了)
  assert.deepEqual(await julyState(), PARTIAL, 'A 未完了');
  await putCov3(U, '2026-07-14');                                  // A の最後の日まで = 月の全部の日がそろった
  assert.deepEqual(await julyState(), MONTHLY, '両方完了');
  await putCov3(B, '2026-07-30');                                  // B の 7/31 が未完了
  assert.deepEqual(await julyState(), PARTIAL, 'B 未完了');
  await putCov3(B, '2026-07-31'); await putCov3(U, null);          // A の coverage が無い
  assert.deepEqual(await julyState(), PARTIAL, 'A なし');
  await putCov3(U, '2026-07-14');
  await p3.query(`update core.finance_source_policy set period_from = '2026-07-16' where source = $1`, [B]);   // 7/15 に policy が無い日 (指紋も変わる = どちらの complete_to も null)
  assert.deepEqual(await julyState(), PARTIAL, 'policy の無い日');
  await p3.query(`update core.finance_source_policy set period_from = '2026-07-15' where source = $1`, [B]);
  assert.deepEqual(await julyState(), MONTHLY, '戻す');
});
await t('🚨 complete の受け口は policy の起点・source の有無・指紋を 1 つの時点で読む: 読んだ後に policy が広がると、新しい指紋の manifest は 409 / 古い指紋の manifest は受けるが complete_to は null (保存した指紋 = 読んだ時点)・policy の lock を持つ', async () => {
  await putCov3(U, null); await putCov3(B, null);
  const oldFp = await fp3();
  // B の起点を 7/15 → 7/01 に広げる変更 (A の終わりも 7/01 に)
  const widen = async (db) => { await db.query(`update core.finance_source_policy set period_to = '2026-07-01' where source = $1`, [U]); await db.query(`update core.finance_source_policy set period_from = '2026-07-01' where source = $1`, [B]); };
  const narrow = async () => { await p3.query(`update core.finance_source_policy set period_from = '2026-07-15' where source = $1`, [B]); await p3.query(`update core.finance_source_policy set period_to = '2026-07-15' where source = $1`, [U]); };
  await widen(d3); const newFp = await fp3(); await narrow();
  assert.notEqual(newFp, oldFp); assert.equal(await fp3(), oldFp);
  const rc = (await coverageStatus(d3, { mall: 'amazon', scope: 'jp', source: B, withReceipts: true })).receipts;
  // 証拠の鎖は 7/10 から = 古い起点 (7/15) なら届いている・新しい起点 (7/01) なら届いていない
  const man = (fp) => M(rc, { complete_to: '2026-08-01', settlements_through: '2026-08-01T15:00:00Z', inventory_completed_at: '2026-08-02T00:00:00Z',
    evidence_chain_from: '2026-07-10T00:00:00Z', evidence_chain_through: '2026-08-02T00:00:00Z', policy_fingerprint: fp });
  await applyCoverage(d3, { state: 'updating', mall: 'amazon', scope: 'jp', source: B, generation: 1, run_token: TOK }, { log: quiet });
  // ① 送り手は新しい policy (指紋) を読んだ・受け口の読みの直後に policy が広がる → 起点と指紋は同じ時点 (古い) = 409 (古い起点で検査して新しい指紋を保存しない)
  let locks = null;
  const heldPolicyLock = async (db) => (await db.query(`select count(*)::int as n from pg_locks where locktype = 'advisory' and granted
      and classid::text || ':' || objid::text || ':' || objsubid::text
        = ((hashtext('core.finance_source_policy')::bigint >> 32) & 4294967295)::oid::text || ':' || (hashtext('core.finance_source_policy')::bigint & 4294967295)::oid::text || ':1'`)).rows[0].n;
  await rejects(() => applyCoverage(d3, { state: 'complete', mall: 'amazon', scope: 'jp', source: B, generation: 1, run_token: TOK, manifest: man(newFp) },
    { log: quiet, hooks: { afterPolicySnapshot: async (db) => { locks = await heldPolicyLock(db); await widen(db); } } }), 'POLICY_MISMATCH');
  assert.equal(locks, 1, 'policy の trigger と同じ advisory lock を持って読む');
  assert.equal(await fp3(), oldFp, '取引ごと戻る (policy の変更も)');
  assert.equal((await one3(`select state from core.finance_coverage where source = $1`, [B])).state, 'updating');
  // ② 送り手も受け口も古い policy を読んだ・読みの直後に policy が広がる → 受けるが、保存した指紋は古い = 今の policy と違う = complete_to は null
  const r = await applyCoverage(d3, { state: 'complete', mall: 'amazon', scope: 'jp', source: B, generation: 1, run_token: TOK, manifest: man(oldFp) },
    { log: quiet, hooks: { afterPolicySnapshot: async (db) => { await widen(db); } } });
  assert.equal(r.status, 'applied');
  assert.equal((await one3(`select policy_fingerprint as f from core.finance_coverage where source = $1`, [B])).f, oldFp);
  assert.equal(await fp3(), newFp);
  assert.equal((await one3(`select complete_to from core.finance_coverage_state(1::smallint, 'amazon', 'jp', $1)`, [B])).complete_to, null);
  await narrow();
  assert.equal(String((await one3(`select complete_to::text as c from core.finance_coverage_state(1::smallint, 'amazon', 'jp', $1)`, [B])).c), '2026-08-01');
  await p3.close();
});

console.log('0050 の前 (Render が先に deploy された朝)');
await t('🚨 0050 の前: token の無い chunk は今までどおり受ける・token 付きの chunk と coverage の要求は 409 NOT_MIGRATED', async () => {
  const p2 = new PGlite();
  await applyMigrations(pgliteAdapter(p2), { log: quiet, to: '0049' });
  const d2 = pgliteAdapter(p2);
  const send = (body) => { const c = validateFinanceChunk(body); return ingestOrderFinanceChunk(d2, { ...c, log: quiet }); };
  const r = await send(chunkBody([['P-1', [row('2026-06-05', '-', { line_kind: 'storage', fba_storage_jpy: -5, account_fee_amount_jpy: -5 })]]]));
  assert.deepEqual([r.applied, r.coverage_invalidated], [1, 0]);
  await rejects(() => send(tokChunk([['P-2', [row('2026-06-05', '-', { line_kind: 'storage', fba_storage_jpy: -5, account_fee_amount_jpy: -5 })]]], 1, TOK)), 'NOT_MIGRATED');
  await rejects(() => applyCoverage(d2, { state: 'updating', ...key, generation: 1, run_token: TOK }), 'NOT_MIGRATED');
  await p2.close();
});

console.log('受け口 (本物の router を HTTP で)');
process.env.MIRROR_SYNC_KEY = 'k';
process.env.COMPANY_DB_URL = 'postgres://test';
let target = pg;
__setPgClientFactory(async () => ({
  query: async (text, params) => {
    if (params && params.length) return target.query(text, params);
    if (text.includes(';')) { await target.exec(text); return { rows: [] }; }
    return target.query(text);
  },
  end: async () => {},
}));
const app = express();
app.use('/apps/company-db/sync', requireSyncKey);
app.use('/apps/company-db/sync', companyDbRouter);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const BASE_URL = `http://127.0.0.1:${server.address().port}/apps/company-db/sync`;
const http = async (method, p, { body: b, key: k = 'k' } = {}) => {
  const res = await fetch(`${BASE_URL}${p}`, { method, headers: { ...(k == null ? {} : { 'x-sync-key': k }), ...(b !== undefined ? { 'content-type': 'application/json' } : {}) }, body: b !== undefined ? JSON.stringify(b) : undefined });
  const text = await res.text(); let json = null; try { json = JSON.parse(text); } catch { /* JSON でない */ }
  return { status: res.status, json };
};
const ST = `/order-finance/coverage/status?mall=amazon&scope=jp&source=${U}`;
await t('POST /order-finance/coverage: 鍵なし 401・形 400・updating 200・digest 違い 409 (detail.render)・complete 200・同じ世代の updating 409・古い世代 200 stale', async () => {
  const b = (x) => ({ state: 'updating', ...key, generation: 14, run_token: TOK, ...x });
  assert.equal((await http('POST', '/order-finance/coverage', { body: b(), key: null })).status, 401);
  assert.equal((await http('POST', '/order-finance/coverage', { body: b(), key: 'wrong' })).status, 401);
  const bad = await http('POST', '/order-finance/coverage', { body: b({ run_token: 'x' }) });
  assert.deepEqual([bad.status, bad.json.code], [400, 'BAD_REQUEST']);
  const u = await http('POST', '/order-finance/coverage', { body: b() });
  assert.deepEqual([u.status, u.json.status, u.json.state, u.json.generation], [200, 'applied', 'updating', '14']);
  const mm = await http('POST', '/order-finance/coverage', { body: b({ state: 'complete', manifest: M(expected(), { receipt_digest: H('nope') }) }) });
  assert.deepEqual([mm.status, mm.json.code, mm.json.detail.render], [409, 'RECEIPT_MISMATCH', expected()]);
  const c = await http('POST', '/order-finance/coverage', { body: b({ state: 'complete', manifest: M(expected()) }) });
  assert.deepEqual([c.status, c.json.status, c.json.complete_to], [200, 'applied', '2026-06-10']);
  const back = await http('POST', '/order-finance/coverage', { body: b() });
  assert.deepEqual([back.status, back.json.code], [409, 'CONFLICT']);
  const st = await http('POST', '/order-finance/coverage', { body: b({ generation: 3 }) });
  assert.deepEqual([st.status, st.json.status, st.json.current_generation], [200, 'stale', '14']);
  // 上限を超えた body = 413 で、この口の上限 (64KB) を返す (#1561 Codex R1 Low: 前は共用の文で「12MB」)
  const big = await http('POST', '/order-finance/coverage', { body: b({ pad: 'x'.repeat(70000) }) });
  assert.deepEqual([big.status, big.json.error], [413, 'payload too large (64KB)']);
  const bigChunk = await http('POST', '/order-finance', { body: { pad: 'x'.repeat(13 * 1024 * 1024) } });
  assert.deepEqual([bigChunk.status, bigChunk.json.error], [413, 'payload too large (12MB)']);   // 財務の chunk の口は 12MB のまま
});
await t('GET /order-finance/coverage/status: 行・effective (core.finance_coverage_state)・receipts=1 の digest・source の形 400・鍵なし 401', async () => {
  const s = await http('GET', `${ST}&receipts=1`);
  assert.equal(s.status, 200);
  assert.deepEqual([s.json.coverage.state, s.json.coverage.generation, s.json.coverage.complete_to, s.json.coverage.settlements_through, s.json.coverage.source_revision, s.json.coverage.receipt_lines],
    ['complete', '14', '2026-06-10', '2026-06-10T15:00:00Z', '42', expected().lines]);
  assert.deepEqual(s.json.effective, { complete_to: '2026-06-10', generation: '14', source_revision: '42' });
  assert.deepEqual(s.json.receipts, expected());
  assert.deepEqual(s.json.policy, { fingerprint: PFP, rows: [{ period_from: '2026-01-01', period_to: null, source: U }] });   // 送り手は回の始めにこれを読む
  assert.equal((await http('GET', `/order-finance/coverage/status?mall=amazon&scope=jp&source=x`)).status, 400);
  assert.equal((await http('GET', ST, { key: null })).status, 401);
  const none = await http('GET', `/order-finance/coverage/status?mall=amazon&scope=us&source=${U}`);
  assert.deepEqual([none.status, none.json.coverage, none.json.effective], [200, null, { complete_to: null, generation: null, source_revision: null }]);
});
await t('POST /order-finance (chunk) を HTTP で: token 付きの complete の後 = 409 COVERAGE_MISMATCH / token 無し = 200・coverage_invalidated 1', async () => {
  const tk = await http('POST', '/order-finance', { body: tokChunk([['H-1', [row('2026-06-05', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]], 14, TOK) });
  assert.deepEqual([tk.status, tk.json.code], [409, 'COVERAGE_MISMATCH']);
  const pl = await http('POST', '/order-finance', { body: chunkBody([['H-1', [row('2026-06-05', 'LA', { units_ordered: 1, sales_principal_jpy: 1 })]]]) });
  assert.deepEqual([pl.status, pl.json.applied, pl.json.coverage_invalidated], [200, 1, 1]);
  assert.equal((await http('GET', ST)).json.effective.complete_to, null);
});
await t('0050 の前は coverage の口が 409 not_migrated (送り手が「未適用」と読める)', async () => {
  const p2 = new PGlite();
  await applyMigrations(pgliteAdapter(p2), { log: quiet, to: '0049' });
  target = p2;
  try {
    const s = await http('GET', ST);
    assert.deepEqual([s.status, s.json.error], [409, 'not_migrated']);
    const p = await http('POST', '/order-finance/coverage', { body: { state: 'updating', ...key, generation: 1, run_token: TOK } });
    assert.deepEqual([p.status, p.json.error], [409, 'not_migrated']);
  } finally { target = pg; await p2.close(); }
});

server.close();
await pg.close();
console.log(`\n${ok} 件 PASS${ng ? ` / ${ng} 件 NG` : ''}`);
if (ng) process.exit(1);
