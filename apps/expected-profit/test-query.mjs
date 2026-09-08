/**
 * test-query.mjs — 画面用の読み取り 受入試験 (§9 / §15-9)
 *
 * Codex R3 の受入条件「表示時の失効再判定」を中心に固める。
 * 実行: node apps/expected-profit/test-query.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-q-'));

const { initExpectedProfitDB } = await import('./db.js');
const { applyFreshnessNow, sortRows, csvCell, queryPublished, summarize } = await import('./query.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const NOW = new Date('2026-09-10T00:00:00Z');
const FUTURE = '2099-01-01T00:00:00Z';
const PAST = '2026-09-08T00:00:00Z';

const row = (over = {}) => ({
  mall: 'rakuten', shop_id: '1', mall_item_key: 'k1', fulfillment: 'self',
  rank_eligible: 1, rank_exclusion_reason: null,
  expected_profit: 81, expected_margin_rate: 0.081,
  calculation_status: 'ok',
  listing_enum_valid_until: FUTURE, price_valid_until: FUTURE, fee_valid_until: null,
  cost_valid_until: FUTURE, shipping_master_valid_until: FUTURE,
  ...over,
});

console.log('表示時の失効再判定 (Codex R3)');

t('全部期限内なら適格のまま', () => {
  const r = applyFreshnessNow(row(), NOW);
  assert.equal(r.expired_now, 0);
  assert.equal(r.rank_eligible_now, 1);
});

t('[!] 保存時は ok でも、今日見たら価格が期限切れなら外す', () => {
  const r = applyFreshnessNow(row({ price_valid_until: PAST }), NOW);
  assert.equal(r.expired_now, 1);
  assert.equal(r.rank_eligible_now, 0);
  assert.equal(r.rank_exclusion_reason_now, 'price_expired_now');
});

t('[!] 原価マスタの期限切れも表示時に効く (バッチが止まった翌日以降)', () => {
  const r = applyFreshnessNow(row({ cost_valid_until: PAST }), NOW);
  assert.equal(r.rank_eligible_now, 0);
  assert.equal(r.rank_exclusion_reason_now, 'cost_expired_now');
});

t('[!] 出品列挙の期限切れも表示時に効く', () => {
  const r = applyFreshnessNow(row({ listing_enum_valid_until: PAST }), NOW);
  assert.equal(r.rank_eligible_now, 0);
});

t('[!] FBA では自社配送マスタの期限を見ない (not_applicable)', () => {
  const r = applyFreshnessNow(row({
    mall: 'amazon', fulfillment: 'FBA', fee_valid_until: FUTURE, shipping_master_valid_until: PAST,
  }), NOW);
  assert.equal(r.expired_now, 0);
  assert.equal(r.rank_eligible_now, 1);
});

t('[!] Amazon では手数料見積の期限を見る', () => {
  const r = applyFreshnessNow(row({
    mall: 'amazon', fulfillment: 'FBA', fee_valid_until: PAST,
  }), NOW);
  assert.equal(r.rank_eligible_now, 0);
  assert.equal(r.rank_exclusion_reason_now, 'fee_expired_now');
});

t('他モールは手数料の期限が無い (簡易料率) ので判定しない', () => {
  const r = applyFreshnessNow(row({ fee_valid_until: null }), NOW);
  assert.equal(r.expired_now, 0);
});

t('保存時に不適格だった行は、期限内でも適格にならない', () => {
  const r = applyFreshnessNow(row({ rank_eligible: 0, rank_exclusion_reason: 'listing_inactive' }), NOW);
  assert.equal(r.rank_eligible_now, 0);
  assert.equal(r.rank_exclusion_reason_now, 'listing_inactive');
});

console.log('\n並び替え');

t('[!] 利益率の降順。NULL は末尾', () => {
  const rows = [
    row({ mall_item_key: 'a', expected_margin_rate: 0.05 }),
    row({ mall_item_key: 'b', expected_margin_rate: null, expected_profit: null }),
    row({ mall_item_key: 'c', expected_margin_rate: 0.20 }),
  ];
  const s = sortRows(rows, 'margin', 'desc');
  assert.deepEqual(s.map(r => r.mall_item_key), ['c', 'a', 'b']);
});

t('[!] 昇順でも NULL は末尾 (先頭に来させない)', () => {
  const rows = [
    row({ mall_item_key: 'a', expected_margin_rate: 0.05 }),
    row({ mall_item_key: 'b', expected_margin_rate: null, expected_profit: null }),
    row({ mall_item_key: 'c', expected_margin_rate: 0.20 }),
  ];
  const s = sortRows(rows, 'margin', 'asc');
  assert.deepEqual(s.map(r => r.mall_item_key), ['a', 'c', 'b']);
});

t('NaN も NULL と同じく末尾', () => {
  const rows = [
    row({ mall_item_key: 'a', expected_margin_rate: 0.05 }),
    row({ mall_item_key: 'b', expected_margin_rate: NaN }),
  ];
  assert.deepEqual(sortRows(rows).map(r => r.mall_item_key), ['a', 'b']);
});

t('[!] 同率なら安定した順序 (利益額 → キー)', () => {
  const rows = [
    row({ mall_item_key: 'z', expected_margin_rate: 0.1, expected_profit: 100 }),
    row({ mall_item_key: 'a', expected_margin_rate: 0.1, expected_profit: 100 }),
    row({ mall_item_key: 'm', expected_margin_rate: 0.1, expected_profit: 200 }),
  ];
  const s1 = sortRows(rows).map(r => r.mall_item_key);
  const s2 = sortRows([...rows].reverse()).map(r => r.mall_item_key);
  assert.deepEqual(s1, ['m', 'a', 'z']);   // 利益額が大きい方が先、同額はキー順
  assert.deepEqual(s1, s2);                // 入力順に依存しない
});

t('利益額でも並べられる', () => {
  const rows = [
    row({ mall_item_key: 'a', expected_profit: 50, expected_margin_rate: 0.5 }),
    row({ mall_item_key: 'b', expected_profit: 500, expected_margin_rate: 0.05 }),
  ];
  assert.deepEqual(sortRows(rows, 'profit', 'desc').map(r => r.mall_item_key), ['b', 'a']);
});

console.log('\nCSV の安全性 (§9.4)');

t('[!] 外から来た文字列の数式インジェクションを防ぐ', () => {
  assert.equal(csvCell('=SUM(A1)', { isExternalText: true }), "'=SUM(A1)");
  assert.equal(csvCell('@cmd', { isExternalText: true }), "'@cmd");
  assert.equal(csvCell('+1', { isExternalText: true }), "'+1");
});

t('[!] 負の利益額を文字列化しない (数値列には適用しない)', () => {
  assert.equal(csvCell(-1234), '-1234');
  assert.equal(csvCell(-1234, { isExternalText: false }), '-1234');
});

t('カンマ・引用符・改行を含む値を正しく囲む', () => {
  assert.equal(csvCell('a,b'), '"a,b"');
  assert.equal(csvCell('a"b'), '"a""b"');
  assert.equal(csvCell('a\nb'), '"a\nb"');
});

t('null は空文字', () => {
  assert.equal(csvCell(null), '');
  assert.equal(csvCell(undefined), '');
});

console.log('\n公開世代の読み取り');

const db = initExpectedProfitDB();

function seedPublished(rows) {
  db.prepare("DELETE FROM mart_listing_expected_profit WHERE generation_id = 'g1'").run();
  db.prepare(`INSERT OR REPLACE INTO expected_profit_generation
    (generation_id, seq, built_at, local_status, remote_status, row_count, ok_count, incomplete_count,
     rank_eligible_count, content_hash, malls_included, malls_degraded)
    VALUES ('g1', 1, '2026-09-09T00:00:00Z', 'validated', 'published', ?, ?, 0, ?, 'h', '["rakuten"]', '[]')`)
    .run(rows.length, rows.length, rows.length);
  const ins = db.prepare(`INSERT OR REPLACE INTO mart_listing_expected_profit
    (generation_id, mall, shop_id, mall_item_key, fulfillment, expected_profit, expected_margin_rate,
     listing_enum_status, listing_enum_valid_until, price_status, price_valid_until, fee_status, fee_valid_until,
     cost_status, cost_valid_until, shipping_master_status, shipping_master_valid_until,
     scenario_fit, calculation_status, rank_eligible, expense_scope_version, input_snapshot,
     formula_version, scenario_version, fee_rate_version, code_version, built_at)
    VALUES ('g1', @mall, '1', @mall_item_key, @fulfillment, @expected_profit, @expected_margin_rate,
     'ok', @listing_enum_valid_until, 'ok', @price_valid_until, 'not_applicable', @fee_valid_until,
     'ok', @cost_valid_until, 'ok', @shipping_master_valid_until,
     'ok', 'ok', @rank_eligible, 'self_v1', '{}', 'v1', 'v1', 'v1', 'test', '2026-09-09T00:00:00Z')`);
  for (const r of rows) ins.run(r);
  db.prepare(`INSERT OR REPLACE INTO expected_profit_publish_pointer (id, generation_id, seq, published_at)
              VALUES (1, 'g1', 1, '2026-09-09T00:00:00Z')`).run();
}

t('公開中の世代から適格な行だけ返す', () => {
  seedPublished([
    row({ mall_item_key: 'hi', expected_margin_rate: 0.30 }),
    row({ mall_item_key: 'lo', expected_margin_rate: 0.05 }),
    row({ mall_item_key: 'ng', rank_eligible: 0 }),
  ]);
  const r = queryPublished({ db, now: NOW });
  assert.equal(r.rows.length, 2);
  assert.equal(r.rows[0].mall_item_key, 'hi');    // 利益率降順
  assert.equal(r.published.generation_id, 'g1');
});

t('[!] 表示時に期限切れになった行はランキングから消える', () => {
  seedPublished([
    row({ mall_item_key: 'fresh' }),
    row({ mall_item_key: 'stale', price_valid_until: PAST }),
  ]);
  const r = queryPublished({ db, now: NOW });
  assert.equal(r.rows.length, 1);
  assert.equal(r.rows[0].mall_item_key, 'fresh');
  assert.equal(r.summary.expiredNow, 1);          // 件数としては見える
});

t('rankOnly=false なら不適格な行も返す (別枠表示用)', () => {
  const r = queryPublished({ db, now: NOW, rankOnly: false });
  assert.equal(r.rows.length, 2);
});

t('公開世代が無ければ空を返す (エラーにしない)', () => {
  db.prepare('DELETE FROM expected_profit_publish_pointer').run();
  const r = queryPublished({ db, now: NOW });
  assert.equal(r.published, null);
  assert.equal(r.rows.length, 0);
});

t('summarize は除外理由の内訳を数える', () => {
  const s = summarize([
    { calculation_status: 'ok', rank_eligible_now: 1, expired_now: 0, mall: 'rakuten' },
    { calculation_status: 'incomplete', rank_eligible_now: 0, expired_now: 0, mall: 'rakuten', rank_exclusion_reason_now: 'cost_missing' },
    { calculation_status: 'ok', rank_eligible_now: 0, expired_now: 1, mall: 'amazon', rank_exclusion_reason_now: 'price_expired_now' },
  ]);
  assert.equal(s.total, 3);
  assert.equal(s.ok, 2);
  assert.equal(s.rankEligible, 1);
  assert.equal(s.expiredNow, 1);
  assert.equal(s.byExclusion.cost_missing, 1);
  assert.equal(s.byMall.amazon, 1);
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
