/**
 * test-lz-barcode.mjs — ロジザードに送るバーコードの決め方 (CompanyDB構想 23・0078・lib/logizard-barcode.mjs)
 *
 * Company DB = PGlite (持ち主のロール deploy で migration・夜間ロードで商品・出品・FNSKU・JAN を入れる・画面は master_edit のロール)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   U 答えの決まり (純粋): auto = FNSKU 1 → FNSKU / FNSKU 2+ → 要確認 (JAN に落とさない) / FNSKU 0 → JAN 1 → JAN / JAN 2+ → 要確認 / 何も無い → 空欄・
 *     fnsku / jan = 無ければ「待つ」・2+ は選んだ 1 本 (pinned) があれば それ・選んだ値が候補から消えた = 要確認・lz_assign = 空欄・
 *     行が無い = 決め方なし (auto で計算した参考 = no_rule)・JAN はチェック数字まで・値はそろえて (空白・全角・小文字) 重ねて数える
 *   C CSV の部品: lz-csv.mjs の新商品のバーコードの列 = barcode・barcode_hold = 行を作らない (理由)・両方 / 形の違う値 / 知らない理由 = 例外・
 *     どちらも無い = 今と同じ空 (影運転)・毎日の商品マスタは変わらない・決め方なし = lz_barcode_no_rule
 *   D 候補 (Company DB): FNSKU = Amazon 日本の 1 個だけの出品・まとめ売り / 組んだ出品 / 消えた出品 / B0… の形 / 米国 = 数えない (理由つき)・
 *     JAN = この商品の有効な JAN (チェック数字が合わない = 数えない)
 *   F DB の関数 ops.set_sku_logizard_barcode_rule (0078): 書く・押し直し = 前の答え (開いた行番号が古くても = stale より先)・違う中身 = request_id_reused・
 *     開いた行番号が違う = stale・今と同じ = 書かない・行が無い商品に auto = 必ず 1 行・選んだ 1 本は今の候補だけ (JS と SQL の候補が同じ = 全部の候補で突き合わせ)・
 *     auto / lz_assign に選んだ 1 本 = 断る・セット = not_single
 *   S 守り: master_edit は表に直接書けない (insert / update / delete)・TRUNCATE できない・watcher は関数を実行できない・PUBLIC の実行権なし・search_path 固定
 *   H 画面: 単品の商品の画面に「ロジザードに送るバーコード」(今の答え・決め方・候補・候補にしないもの)・JSON と JS が読める (me-sku.js の後)・
 *     保存の API (名簿・Origin・形・stale)・名簿でない = 見るだけ (保存のボタンと JS なし)・セットには出さない・JAN の欄の文 (ロジザードに送っていない)・つかいかた
 * 使い方: node scripts/test-lz-barcode.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import iconv from 'iconv-lite';
import express from 'express';

const killer = setTimeout(() => { console.error('時間切れ (360 秒)'); process.exit(3); }, 360e3);
killer.unref();

const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbarcode-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
(await import('../lib/master-legacy-gate.mjs')).__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open' }));
const LB = await import('../lib/logizard-barcode.mjs');
const LZ = await import('../apps/master-decisions/lz-csv.mjs');
const W = await import('../lib/master-write.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const uuid = () => crypto.randomUUID();
const pgErr = async (p) => { try { await p; } catch (e) { return e; } return null; };

// ─── U 答えの決まり (純粋) ───
console.log('ロジザードに送るバーコード (決め方と答え)');
const F1 = 'X00LZB0001', F2 = 'X00LZB0002', J1 = '4901234567894', J2 = '4900000000009', JBAD = '4901234567890';
const c = (fn = [], jn = []) => ({ fnskus: fn.map((value) => ({ value })), jans: jn.map((value) => ({ value })) });
const ans = (rule, cands, pinned = null) => { const p = LB.pickLogizardBarcode(rule, cands, pinned); return [p.state, p.value]; };

await ta('[U1] auto: FNSKU → JAN → 空欄・FNSKU 2 つ以上 = 要確認 (JAN に落とさない)・JAN 2 つ以上 = 要確認', async () => {
  assert.deepEqual(ans('auto', c([F1], [J1])), ['fnsku', F1]);
  assert.deepEqual(ans('auto', c([], [J1])), ['jan', J1]);
  assert.deepEqual(ans('auto', c([], [])), ['blank', '']);
  assert.deepEqual(ans('auto', c([F1, F2], [J1])), ['ambiguous', null], 'FNSKU が 2 つで JAN に落ちた');
  assert.deepEqual(ans('auto', c([], [J1, J2])), ['ambiguous', null]);
  assert.match(LB.pickLogizardBarcode('auto', c([F1, F2])).reason, /FNSKUが 2 つ .*決め方を FNSKU にして 1 本を選んで/);
});
await ta('[U2] fnsku / jan = 無ければ待つ・2 つ以上は選んだ 1 本・選んだ値が消えた = 要確認・lz_assign = 空欄・行が無い = auto の参考 (no_rule)', async () => {
  assert.deepEqual(ans('fnsku', c([], [J1])), ['waiting', null], 'FNSKU だけの決め方で JAN に落ちた');
  assert.deepEqual(ans('jan', c([F1], [])), ['waiting', null]);
  assert.deepEqual(ans('jan', c([F1], [J1])), ['jan', J1], 'JAN の決め方で FNSKU を送った');
  assert.deepEqual(ans('fnsku', c([F1, F2]), F2), ['fnsku', F2]);
  assert.deepEqual(ans('fnsku', c([F1, F2])), ['ambiguous', null]);
  assert.deepEqual(ans('fnsku', c([F1]), F2), ['ambiguous', null], '選んだ値が候補に無いのに残りの 1 つにした');
  assert.match(LB.pickLogizardBarcode('fnsku', c([F1]), F2).reason, /選んだ FNSKU X00LZB0002 が今の候補 \(X00LZB0001\) にありません/);
  assert.deepEqual(ans('jan', c([], [J1, J2]), J2), ['jan', J2]);
  assert.deepEqual(ans('lz_assign', c([F1], [J1])), ['blank', '']);
  assert.deepEqual(ans('auto', c([F1, F2]), F1), ['ambiguous', null], 'auto の選んだ 1 本は使わない');
  const nr = LB.pickLogizardBarcode(null, c([], [J1]));
  assert.equal(nr.no_rule, true); assert.equal(nr.rule, 'auto'); assert.equal(nr.state, 'jan');
  assert.equal(LB.pickLogizardBarcode('auto', c()).no_rule, false);
  assert.throws(() => LB.pickLogizardBarcode('none', c()), /知らない決め方/);
});
await ta('[U3] 値の形・そろえ方: JAN はチェック数字まで・FNSKU は X0 + 8 字 (B0… = ASIN の形は数えない)・空白 / 全角 / 小文字をそろえて重ねる', async () => {
  assert.deepEqual(ans('auto', c(['B0ABCDEFGH'], [JBAD])), ['blank', ''], '形の違う値を送った');
  assert.deepEqual(ans('auto', c([' x00lzb0001 ', 'Ｘ００ＬＺＢ０００１'], [])), ['fnsku', F1], '同じ値を 2 つと数えた');
  assert.deepEqual(ans('jan', c([], [J1, ` ${J1}`])), ['jan', J1]);
  assert.equal(LB.lzJanOk(J1), true); assert.equal(LB.lzJanOk(JBAD), false); assert.equal(LB.lzJanOk('12345678901234'), false);
  assert.equal(LB.lzBarcodeValueOk(''), true); assert.equal(LB.lzBarcodeValueOk(F1), true); assert.equal(LB.lzBarcodeValueOk(JBAD), false); assert.equal(LB.lzBarcodeValueOk('B0ABCDEFGH'), false);
});
await ta('[U4] 入力の形: 決め方は 4 つだけ (送らない = null)・選んだ 1 本は fnsku / jan のときだけ・形まで', async () => {
  assert.equal(LB.lzBarcodeRuleIn(undefined), null); assert.equal(LB.lzBarcodeRuleIn(''), null);
  for (const r of ['auto', 'fnsku', 'jan', 'lz_assign']) assert.equal(LB.lzBarcodeRuleIn(r), r);
  for (const bad of ['none', 'AUTO', 1, {}, 'constructor', '__proto__']) assert.throws(() => LB.lzBarcodeRuleIn(bad), (e) => e.status === 400 && e.extra.field === 'logizard_barcode', String(bad));
  assert.equal(LB.lzBarcodePinnedIn(' x00lzb0001 ', 'fnsku'), F1);
  assert.equal(LB.lzBarcodePinnedIn(J1, 'jan'), J1);
  assert.equal(LB.lzBarcodePinnedIn(null, 'auto'), null);
  assert.throws(() => LB.lzBarcodePinnedIn(F1, 'auto'), /決め方が FNSKU か JAN のときだけ/);
  assert.throws(() => LB.lzBarcodePinnedIn(J1, 'fnsku'), /FNSKU の形が違います/);
  assert.throws(() => LB.lzBarcodePinnedIn(JBAD, 'jan'), /JAN の形が違います/);
});

// ─── C CSV の部品 ───
const item = (code, x = {}) => ({ code_norm: code.toLowerCase(), ne_code: code, name: `商品 ${code}`, cost_src: '100.00', supplier: '1', ...x });
const cells = (r) => r.rows.map((x) => x.cells.map((b) => iconv.decode(b, 'cp932')));
await ta('[C1] 新商品の CSV: バーコードの列 = barcode・hold = 作らない (理由)・どちらも無い = 空 (影運転と同じ)・答えから作る (lzCsvBarcodeOf)', async () => {
  const pk = (rule, cands, pinned) => LB.lzCsvBarcodeOf(LB.pickLogizardBarcode(rule, cands, pinned));
  const r = LZ.buildLzCsv([
    item('a1', pk('auto', c([F1], [J1]))), item('a2', pk('auto', c([], [J1]))), item('a3', pk('auto', c())), item('a4', pk('auto', c([F1, F2]))),
    item('a5', pk('fnsku', c())), item('a6', pk(null, c([F1]))), item('a7'),
  ], 'new');
  assert.deepEqual(cells(r).map((x) => [x[0], x[7]]), [['a1', F1], ['a2', J1], ['a3', ''], ['a7', '']]);
  assert.deepEqual(r.unmade, [{ code_norm: 'a4', reason: 'lz_barcode_ambiguous' }, { code_norm: 'a5', reason: 'lz_barcode_waiting' }, { code_norm: 'a6', reason: 'lz_barcode_no_rule' }]);
  assert.equal(r.counts.made, 4); assert.equal(r.counts.unmade, 3);
  assert.equal(LZ.NEW.header[7], 'バーコード'); assert.deepEqual([...LZ.NEW.human], [4, 5, 7], '影運転は人の 3 列を比べない (L-2 のまま)');
});
await ta('[C2] 渡し方の誤り = 例外 (両方・形の違う値・知らない理由)・毎日の商品マスタは barcode を見ない', async () => {
  assert.throws(() => LZ.buildLzCsv([item('b1', { barcode: F1, barcode_hold: 'lz_barcode_waiting' })], 'new'), /両方がある/);
  assert.throws(() => LZ.buildLzCsv([item('b2', { barcode: JBAD })], 'new'), /バーコードの形が違う/);
  assert.throws(() => LZ.buildLzCsv([item('b3', { barcode: 4901234567894 })], 'new'), /バーコードの形が違う/);
  assert.throws(() => LZ.buildLzCsv([item('b4', { barcode_hold: 'later' })], 'new'), /知らない barcode_hold/);
  const d = LZ.buildLzCsv([item('d1', { barcode: F1 })], 'daily');
  assert.deepEqual(cells(d)[0], ['d1', '商品 d1', '商品 d1', '100', '0001']);
  const plain = LZ.buildLzCsv([item('e1')], 'new');
  assert.equal(iconv.decode(plain.bytes, 'cp932'), '商品ID,商品名,検索名称,仕入単価,有効期限区分,入荷日管理フラグ,取引先コード,バーコード\r\ne1,商品 e1,商品 e1,100,,,0001,');
});

// ─── Company DB (PGlite・持ち主のロール deploy) ───
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg);
const q = async (sql, params) => (await db.query(sql, params)).rows;
async function asRole(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
const asEditor = (fn) => asRole('master_edit', fn);

const AMZ = 'main@A1VC38T7YXB528';
function makePlan() {
  const sku = (code, kind = 'single') => ({ code, name: `商品 ${code}`, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 1,
    cost: { jpy: 100, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
  const comp = (code, qty = 1) => ({ code, qty, resolution: 'exact', evidence: { source: 'test' } });
  const amazon = (listingCode, comps, fnsku, x = {}) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title: listingCode, status: 'active',
    components: comps, asinCandidates: [], fnskuCandidates: fnsku ? [{ fnsku, source: 'fba_sku_attrs' }] : [], evidenceSource: 'test', ...x });
  const jan = (skuCode, v) => ({ skuCode, attribute: 'jan', scope: 'item', valueText: v, source: 'product_hub', sourceRef: `test:${skuCode}:${v}`, observedAt: '2030-01-01T00:00:00.000Z' });
  return {
    skus: ['lz1', 'lz2', 'lz3', 'lz4', 'lz5', 'lz6', 'oth1'].map((x) => sku(x)).concat([sku('set1', 'set')]),
    variationGroups: [],
    setComponents: [{ parentCode: 'set1', childCode: 'lz1', qty: 2, source: 'ne' }],
    listings: [
      amazon('pr_lz1', [comp('lz1')], 'X00LZ10001'),
      amazon('pr_lz1_2p', [comp('lz1', 2)], 'X00LZ10002'),            // まとめ売り
      amazon('pr_mix', [comp('lz1'), comp('oth1')], 'X00MIX0001'),    // 組んだ出品
      amazon('pr_lz2_a', [comp('lz2')], 'X00LZ20001'),
      amazon('pr_lz2_b', [comp('lz2')], 'X00LZ20002'),                // lz2 = FNSKU が 2 つ
      amazon('pr_lz5', [comp('lz5')], 'B0LZ500001'),                  // ASIN の形 (メーカーのバーコード)
      { ...amazon('pr_lz6_us', [comp('lz6')], 'X00LZ6US01'), mall: 'amazon_us', shopCode: 'main@ATVPDKIKX0DER', marketplaceId: 'ATVPDKIKX0DER' },   // 米国
    ],
    observations: [jan('lz1', J1), jan('lz3', '4912345678904'), jan('lz5', '4987654321094'), jan('lz4', JBAD)],
    physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [],
    reorder: { available: true, runId: 'pml_test' },
  };
}
{
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: 'load_lzb', ownership: MASTER_OWNERSHIP, now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r.ok, true, r.error);
}
const sid = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0]?.id;
const ids = Object.fromEntries(await Promise.all(['lz1', 'lz2', 'lz3', 'lz4', 'lz5', 'lz6', 'oth1', 'set1'].map(async (k) => [k, await sid(k)])));
const janOf = async (code) => (await q(`select e.external_value as v from core.external_ids e join core.skus s on s.product_id = e.entity_id and e.entity_type = 'product'
   where s.code = $1 and e.system = 'jan' and e.valid_to is null order by 1`, [code])).map((r) => r.v);
const JAN3 = (await janOf('lz3'))[0];
const read = (codes) => asEditor(() => LB.readLogizardBarcodes(db, codes.map((k) => ids[k])));
const rowsOf = async (code) => q('select rule, pinned, via, actor_id, db_user from ops.sku_logizard_barcode_rules where sku_id = $1 order by rule_row', [ids[code]]);
const setRule = (code, rule, o = {}) => asEditor(async () => {
  await pg.query('begin');
  try {
    const r = await LB.setLzBarcodeRuleInTx(db, { requestId: o.requestId ?? uuid(), actor: o.actor ?? 'naka@test', skuId: ids[code], rule, pinned: o.pinned ?? null, via: o.via ?? 'sku_page', seenRow: o.seenRow ?? null });
    await pg.query('commit');
    return r;
  } catch (e) { await pg.query('rollback'); throw e; }
});
const latestRow = async (code) => (await q('select max(rule_row)::text as r from ops.sku_logizard_barcode_rules where sku_id = $1', [ids[code]]))[0].r;

await ta('[D1] 候補: Amazon 日本の 1 個だけの出品の FNSKU・JAN (チェック数字)・候補にしないもの (まとめ売り・組んだ出品・B0…・JAN のチェック数字)・米国は数えない と答え', async () => {
  assert.ok(JAN3 && LB.lzJanOk(JAN3), '試験の JAN が入っていない');
  assert.equal(await asEditor(() => LB.lzBarcodeReady(db)), true);
  const m = await read(['lz1', 'lz2', 'lz3', 'lz4', 'lz5', 'lz6', 'set1']);
  const s = (k) => m.get(ids[k]);
  assert.deepEqual(s('lz1').candidates.fnskus.map((x) => x.value), ['X00LZ10001']);
  assert.deepEqual(s('lz1').candidates.excluded.map((x) => [x.value, x.why]), [['X00LZ10002', 'まとめ売り (2 個)'], ['X00MIX0001', 'ほかの商品と組んだ出品']]);
  assert.deepEqual([s('lz1').pick.state, s('lz1').pick.value, s('lz1').pick.no_rule, s('lz1').rule], ['fnsku', 'X00LZ10001', true, null], '行が無い = 決め方なし (auto の参考)');
  assert.equal(s('lz2').pick.state, 'ambiguous');
  assert.deepEqual([s('lz3').pick.state, s('lz3').pick.value], ['jan', JAN3]);
  assert.deepEqual([s('lz4').pick.state, s('lz4').candidates.excluded.map((x) => x.why)], ['blank', ['チェック数字が合わない (ロジザードが付けた番号かもしれない)']]);
  assert.equal(s('lz5').pick.state, 'jan', 'B0… の FNSKU を数えた');
  assert.match(s('lz5').candidates.excluded[0].why, /Amazon のラベルの形 \(X0…\) でない/);
  assert.deepEqual([s('lz6').pick.state, s('lz6').candidates.fnskus.length, s('lz6').candidates.excluded.length], ['blank', 0, 0], '米国の FNSKU を数えた');
  assert.equal(s('set1').pick.state, 'blank');
});
await ta('[F1] 書く・押し直し = 前の答え (開いた行番号が古くても stale より先)・違う中身 = request_id_reused・開いた行番号が違う = stale・今と同じ = 書かない・行が無い + auto = 1 行', async () => {
  const rid = uuid();
  const r1 = await setRule('lz3', 'auto', { requestId: rid });
  assert.equal(r1.changed, true, '行が無い商品に auto を保存しても書かなかった (決め方なし ≠ auto)');
  const row1 = await latestRow('lz3');
  assert.deepEqual(await rowsOf('lz3'), [{ rule: 'auto', pinned: null, via: 'sku_page', actor_id: 'naka@test', db_user: 'master_edit' }]);
  const again = await setRule('lz3', 'auto', { requestId: rid });   // 開いた行番号は null のまま (古い) = 押し直しが先
  assert.equal(again.replayed, true);
  let e = await pgErr(setRule('lz3', 'jan', { requestId: rid }));
  assert.equal(e && e.reason, 'request_id_reused', e && e.message);
  e = await pgErr(setRule('lz3', 'jan', { seenRow: null }));
  assert.equal(e && e.reason, 'stale', e && e.message);
  const same = await setRule('lz3', 'auto', { seenRow: row1 });
  assert.equal(same.changed, false);
  const r2 = await setRule('lz3', 'jan', { seenRow: row1 });
  assert.equal(r2.changed, true);
  assert.deepEqual((await rowsOf('lz3')).map((x) => x.rule), ['auto', 'jan']);
  const m = await read(['lz3']);
  assert.deepEqual([m.get(ids.lz3).rule, m.get(ids.lz3).pick.state, m.get(ids.lz3).pick.no_rule], ['jan', 'jan', false]);
});
await ta('[F2] 選んだ 1 本: JS が出した候補は全部 DB でも選べる・候補にしない FNSKU / ほかの商品の値は DB が断る (pinned_not_candidate)・auto / lz_assign に選んだ 1 本 = 断る', async () => {
  const m = await read(['lz1', 'lz2', 'lz3', 'lz5']);
  for (const k of ['lz1', 'lz2', 'lz5']) {
    const x = m.get(ids[k]);
    for (const f of x.candidates.fnskus) {
      const r = await setRule(k, 'fnsku', { pinned: f.value, seenRow: await latestRow(k) });
      assert.equal(r.pinned, f.value, `${k} ${f.value}`);
    }
    for (const f of x.candidates.excluded.filter((y) => y.kind === 'fnsku' && LB.FNSKU_RE.test(y.value))) {
      const e = await pgErr(setRule(k, 'fnsku', { pinned: f.value, seenRow: await latestRow(k) }));
      assert.equal(e && e.reason, 'pinned_not_candidate', `${k} ${f.value} (${f.why}) を DB が受けた`);
    }
  }
  // ほかの商品だけの出品の FNSKU (lz2 のもの) を lz1 に選ぶ = 断る
  const e0 = await pgErr(setRule('lz1', 'fnsku', { pinned: 'X00LZ20001', seenRow: await latestRow('lz1') }));
  assert.equal(e0 && e0.reason, 'pinned_not_candidate', 'ほかの商品の FNSKU を DB が受けた');
  // 米国の出品の FNSKU = 日本だけ (JS も SQL も数えない)
  const e1 = await pgErr(setRule('lz6', 'fnsku', { pinned: 'X00LZ6US01', seenRow: await latestRow('lz6') }));
  assert.equal(e1 && e1.reason, 'pinned_not_candidate', '米国の FNSKU を DB が受けた');
  for (const j of m.get(ids.lz3).candidates.jans) assert.equal((await setRule('lz3', 'jan', { pinned: j.value, seenRow: await latestRow('lz3') })).pinned, j.value);
  let e = await pgErr(setRule('lz3', 'jan', { pinned: J1, seenRow: await latestRow('lz3') }));   // lz1 の JAN
  assert.equal(e && e.reason, 'pinned_not_candidate');
  e = await pgErr(setRule('lz2', 'auto', { pinned: 'X00LZ20001', seenRow: await latestRow('lz2') }));
  assert.equal(e && e.reason, 'invalid_input', '自動に選んだ 1 本を DB が受けた');
  // lz2 = FNSKU 2 つ → 選んだ 1 本で答えが決まる
  const lz2 = (await read(['lz2'])).get(ids.lz2);
  assert.deepEqual([lz2.rule, lz2.pick.state, lz2.pick.value], ['fnsku', 'fnsku', lz2.pinned]);
});
await ta('[F3] セット = not_single・無い SKU = invalid_input・人の形 = invalid_input', async () => {
  let e = await pgErr(setRule('set1', 'auto'));
  assert.equal(e && e.reason, 'not_single');
  e = await pgErr(asEditor(async () => { await pg.query('begin'); try { return await LB.setLzBarcodeRuleInTx(db, { requestId: uuid(), actor: 'naka@test', skuId: '999999999', rule: 'auto', via: 'sku_page' }); } finally { await pg.query('rollback'); } }));
  assert.equal(e && e.reason, 'invalid_input');
  e = await pgErr(setRule('lz4', 'auto', { actor: 'no-mail' }));
  assert.equal(e && e.reason, 'invalid_input');
});
await ta('[S1] 守り: master_edit は表に直接書けない・変えられない・消せない・TRUNCATE できない・watcher は関数を実行できない・PUBLIC の実行権なし・search_path 固定', async () => {
  const code = async (fn) => { const e = await pgErr(fn()); return e ? e.code : null; };
  assert.equal(await asEditor(() => code(() => pg.query(`insert into ops.sku_logizard_barcode_rules (sku_id, rule, via, actor_id, request_id, db_user) values ($1, 'auto', 'sku_page', 'x@y', $2, 'x')`, [ids.lz4, uuid()]))), '42501');
  assert.equal(await asEditor(() => code(() => pg.query("update ops.sku_logizard_barcode_rules set rule = 'jan'"))), '42501');
  assert.equal(await asEditor(() => code(() => pg.query('delete from ops.sku_logizard_barcode_rules'))), '42501');
  assert.equal(await code(() => pg.query("update ops.sku_logizard_barcode_rules set rule = 'jan'")), '42501', '持ち主でも変えられた (追記だけ)');
  assert.ok(await code(() => pg.query('truncate ops.sku_logizard_barcode_rules')), 'TRUNCATE できた');
  assert.equal(await code(() => pg.query(`insert into ops.sku_logizard_barcode_rules (sku_id, rule, via, actor_id, request_id, db_user) values ($1, 'auto', 'sku_page', 'x@y', $2, 'x')`, [ids.lz4, uuid()])), '42501', '持ち主が関数の外で書けた');
  assert.equal(await asRole('watcher', () => code(() => pg.query("select ops.set_sku_logizard_barcode_rule($1::uuid, 'x@y', $2::bigint, 'auto', null, 'sku_page', null)", [uuid(), ids.lz4]))), '42501');
  const p = (await q(`select proacl::text as acl, prosecdef, proconfig::text as cfg from pg_proc where oid = 'ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint)'::regprocedure`))[0];
  assert.equal(p.prosecdef, true); assert.match(p.cfg, /search_path=pg_catalog, pg_temp/);
  assert.ok(!/(^|[{,])=X/.test(p.acl), `PUBLIC に実行権がある: ${p.acl}`); assert.match(p.acl, /master_edit=X/);
  assert.equal((await asRole('watcher', () => q('select count(*)::int as n from ops.sku_logizard_barcode_rules')))[0].n >= 0, true, 'watcher が読めない');
});

// ─── H 画面 (本物の router) ───
const MR = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://deploy@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test';
MR.__setPgClientFactory(async (url) => {
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
MR.__setClock(() => Date.now());
MR.__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
const SESS = {
  editor: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] },
  viewer: { authenticated: true, email: 'viewer@test', role: 'user', allowedApps: ['master-edit'] },
};
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = SESS[req.headers['x-test-session']] || null; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual', signal: AbortSignal.timeout(30e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
async function scriptsOf(html) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  for (const m of all.filter((x) => /type="application\/json"/.test(x[1]))) JSON.parse(m[2]);
  const files = [];
  for (const m of all.filter((x) => /\bsrc="\/apps\/master-edit\/public\//.test(x[1]))) {
    const src = /\bsrc="([^"]+)"/.exec(m[1])[1];
    const r = await fetch(ORIGIN + src.replace(/&amp;/g, '&'), { headers: { 'x-test-session': 'editor' }, signal: AbortSignal.timeout(30e3) });
    assert.equal(r.status, 200, src);
    const t = await r.text();
    new vm.Script(t); assert.ok(!/<%|%>/.test(t), 'EJS のタグが JS に残っている');
    files.push({ src, text: t });
  }
  return files;
}
const section = (html) => (/<section class="panel" id="sec-lzbc"[\s\S]*?<\/section>/.exec(html) || [''])[0];
const lzData = (html) => JSON.parse((/<script type="application\/json" id="me-lzbc">([\s\S]*?)<\/script>/.exec(html) || [])[1] || 'null');
const api = (code) => `/apps/master-edit/api/sku/${code}/logizard-barcode`;

await ta('[H1] 単品の商品の画面: 今の答え・決め方・候補・候補にしないもの・決め方なしの帯・JSON と JS (me-sku.js の後)・JAN の欄の文 (ロジザードに送っていない)', async () => {
  const r = await call('GET', '/apps/master-edit/sku/lz1');   // lz1 = [F2] で決め方 fnsku (選んだ 1 本) を書いた
  assert.equal(r.status, 200, r.text.slice(0, 300));
  const s = section(r.text);
  for (const w of ['ロジザードに送るバーコード', '今の答え', 'X00LZ10001', 'id="lzbc-rule"', '候補にしないもの (2)', 'まとめ売り (2 個)', 'ほかの商品と組んだ出品',
    'id="lzbc-save" disabled>決め方を保存', 'GAS) の H 列に人が入れます', 'いまの決め方 = FNSKU (X00LZ10001) · 商品の画面']) assert.ok(s.includes(w), `欄に「${w}」が無い`);
  assert.ok(!s.includes('決め方がまだありません'));
  const d = lzData(r.text);
  assert.deepEqual([d.rule, d.pinned, !!d.seenRow, d.canWrite, d.answers.auto.state, d.answers.jan.state, d.answers.lz_assign.state], ['fnsku', 'X00LZ10001', true, true, 'fnsku', 'jan', 'blank']);
  // 決め方の行が無い商品 (lz6) = 帯・JSON の行番号なし・答えは auto の参考
  const r6 = await call('GET', '/apps/master-edit/sku/lz6');
  assert.ok(section(r6.text).includes('決め方がまだありません'), '決め方なしの帯が無い');
  assert.deepEqual([lzData(r6.text).rule, lzData(r6.text).seenRow], [null, null]);
  const files = await scriptsOf(r.text);
  const order = files.map((f) => f.src.replace(/\?.*$/, '').replace('/apps/master-edit/public/', ''));
  assert.ok(order.includes('me-lzbarcode.js') && order.indexOf('me-lzbarcode.js') > order.indexOf('me-sku.js'), `JS の順: ${order.join(', ')}`);
  // JAN の欄の文 (直せるときだけ出る = この DB は切替の前なので画面の元で確かめる)
  const skuEjs = fs.readFileSync(new URL('../apps/master-edit/views/sku.ejs', import.meta.url), 'utf8');
  assert.ok(!r.text.includes('ロジザード・product-hub でも使います') && !skuEjs.includes('ロジザード・product-hub でも使います'), 'JAN の欄に「ロジザードでも使います」が残っている');
  assert.ok(skuEjs.includes('product-hub (楽天の出品) で使います · ロジザードに送るバーコードは下の「ロジザードに送るバーコード」'), 'JAN の欄の新しい文が無い');
  const sku = files.find((f) => /me-sku\.js/.test(f.src)).text;
  assert.ok(!sku.includes('JAN はロジザード・product-hub でも使います'), 'me-sku.js の文が古い');
  // 決め方がある商品 (lz2 = fnsku + 選んだ 1 本) = 帯なし・JSON に行番号
  const r2 = await call('GET', '/apps/master-edit/sku/lz2');
  const d2 = lzData(r2.text);
  assert.ok(d2.seenRow && d2.rule === 'fnsku' && d2.pinned, JSON.stringify(d2));
  assert.ok(!section(r2.text).includes('決め方がまだありません'));
});
await ta('[H2] 名簿でない = 見るだけ (保存のボタンと JS なし)・セットには欄を出さない', async () => {
  const r = await call('GET', '/apps/master-edit/sku/lz1', { session: 'viewer' });
  const s = section(r.text);
  assert.ok(s.includes('見るだけです') && !s.includes('id="lzbc-save"'), '名簿でない人に保存のボタン');
  assert.ok(!r.text.includes('me-lzbarcode.js'), '名簿でない人に JS');
  const set = await call('GET', '/apps/master-edit/sku/set1');
  assert.equal(set.status, 200);
  assert.ok(!set.text.includes('id="sec-lzbc"'), 'セットに欄を出した');
});
await ta('[H3] 保存の API: 名簿・Origin・形 (決め方・選んだ 1 本・行番号・request_id)・保存 → 画面に出る・stale = 409・セット = 400・無い商品 = 404', async () => {
  const seen = (lzData((await call('GET', '/apps/master-edit/sku/lz4')).text) || {}).seenRow ?? null;
  assert.equal((await call('POST', api('lz4'), { body: { request_id: uuid(), rule: 'lz_assign', seen_row: seen }, session: 'viewer' })).status, 403);
  assert.notEqual((await call('POST', api('lz4'), { body: { request_id: uuid(), rule: 'lz_assign', seen_row: seen }, origin: false })).status, 200, 'Origin なしで保存できた');
  for (const [body, why] of [[{ request_id: uuid(), rule: 'none' }, 'rule'], [{ request_id: uuid() }, 'no rule'], [{ request_id: 'x', rule: 'auto' }, 'rid'],
    [{ request_id: uuid(), rule: 'auto', pinned: F1 }, 'pin on auto'], [{ request_id: uuid(), rule: 'auto', seen_row: 'abc' }, 'seen']]) {
    const r = await call('POST', api('lz4'), { body });
    assert.equal(r.status, 400, `${why}: ${r.status} ${r.text.slice(0, 200)}`);
  }
  let r = await call('POST', api('lz4'), { body: { request_id: uuid(), rule: 'lz_assign', seen_row: seen } });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.rule, r.j.changed], ['lz_assign', true]);
  const page = await call('GET', '/apps/master-edit/sku/lz4');
  assert.deepEqual([lzData(page.text).rule, lzData(page.text).seenRow], ['lz_assign', r.j.rule_row]);
  assert.match(section(page.text), /いまの決め方 = 空欄 \(ロジザードが番号を付ける\) · 商品の画面 · naka@test/);
  r = await call('POST', api('lz4'), { body: { request_id: uuid(), rule: 'auto', seen_row: seen } });
  assert.deepEqual([r.status, r.j.reason], [409, 'stale']);
  assert.deepEqual([(await call('POST', api('set1'), { body: { request_id: uuid(), rule: 'auto' } })).status], [400]);
  assert.equal((await call('POST', api('nope-x'), { body: { request_id: uuid(), rule: 'auto' } })).status, 404);
});
await ta('[H4] つかいかた: ロジザードに送るバーコードの節 (決め方・要確認・待つ・今は GAS の H 列 = 参考)', async () => {
  const r = await call('GET', '/apps/master-edit/manual');
  for (const w of ['id="lz-barcode"', 'JAN とロジザードに送るバーコードは別です', 'FNSKU があれば FNSKU・無ければ JAN・JAN も無ければ空欄', '要確認', '待つ', 'H 列 (バーコード) を入れています', '参考']) assert.ok(r.text.includes(w), `つかいかたに「${w}」が無い`);
});

server.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
