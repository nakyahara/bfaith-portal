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
const { applyFreshnessNow, sortRows, csvCell, queryPublished, summarize, HANDLING_ACTIVE } = await import('./query.js');

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
     formula_version, scenario_version, fee_rate_version, code_version, built_at,
     handling_class, stock_qty, stock_allocated_qty)
    VALUES ('g1', @mall, '1', @mall_item_key, @fulfillment, @expected_profit, @expected_margin_rate,
     'ok', @listing_enum_valid_until, 'ok', @price_valid_until, 'not_applicable', @fee_valid_until,
     'ok', @cost_valid_until, 'ok', @shipping_master_valid_until,
     'ok', 'ok', @rank_eligible, 'self_v1', '{}', 'v1', 'v1', 'v1', 'test', '2026-09-09T00:00:00Z',
     @handling_class, @stock_qty, @stock_allocated_qty)`);
  // 🚨 在庫・取扱区分は「入っていない世代」が実在する (2026-09-09 夜)。既定は null にして、
  //    絞り込みの試験だけが明示的に値を入れる。既定で埋めると「入っていない世代」を試験できない
  for (const r of rows) ins.run({ handling_class: null, stock_qty: null, stock_allocated_qty: null, ...r });
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

console.log('\n取扱区分・在庫数の絞り込み (2026-09-10 中原さん指示)');

// 🚨 実データにある取扱区分は 取扱中 / 取扱中止 / ﾒｰｶｰ取扱中止 の 3 つ (2026-09-10 実測)。
//    「取扱中でない」を値の列挙で書いていないことを、ﾒｰｶｰ取扱中止 まで入れて固定する
function seedStockRows() {
  seedPublished([
    row({ mall_item_key: 'a_on_stock', handling_class: HANDLING_ACTIVE, stock_qty: 40, stock_allocated_qty: 0 }),
    row({ mall_item_key: 'b_on_zero', handling_class: HANDLING_ACTIVE, stock_qty: 0, stock_allocated_qty: 0 }),
    row({ mall_item_key: 'c_off_stock', handling_class: '取扱中止', stock_qty: 5, stock_allocated_qty: 5 }),
    row({ mall_item_key: 'd_maker_off', handling_class: 'ﾒｰｶｰ取扱中止', stock_qty: 3, stock_allocated_qty: 0 }),
    row({ mall_item_key: 'e_unknown', handling_class: null, stock_qty: null, stock_allocated_qty: null }),
  ]);
}
const keysOf = (r) => r.rows.map(x => x.mall_item_key).sort();

t('[!] 取扱中だけに絞れる', () => {
  seedStockRows();
  const r = queryPublished({ db, now: NOW, handling: 'active' });
  assert.deepEqual(keysOf(r), ['a_on_stock', 'b_on_zero']);
});

t('[!] 取扱をやめたものは ﾒｰｶｰ取扱中止 も入る (値の列挙で書いていない)', () => {
  seedStockRows();
  const r = queryPublished({ db, now: NOW, handling: 'stopped' });
  assert.deepEqual(keysOf(r), ['c_off_stock', 'd_maker_off']);
});

t('[!] 取扱区分が未登録の行を「取扱をやめた」に混ぜない', () => {
  seedStockRows();
  assert.deepEqual(keysOf(queryPublished({ db, now: NOW, handling: 'unknown' })), ['e_unknown']);
});

t('[!] 在庫ありは 1 個以上だけ (0 個を混ぜない)', () => {
  seedStockRows();
  assert.deepEqual(keysOf(queryPublished({ db, now: NOW, stock: 'in_stock' })),
    ['a_on_stock', 'c_off_stock', 'd_maker_off']);
});

t('[!] 在庫なし (0 個) と 分からない (未取得) を同じ山にしない', () => {
  seedStockRows();
  assert.deepEqual(keysOf(queryPublished({ db, now: NOW, stock: 'none' })), ['b_on_zero']);
  assert.deepEqual(keysOf(queryPublished({ db, now: NOW, stock: 'unknown' })), ['e_unknown']);
});

t('[!] 取扱と在庫は重ねて効く (取扱中 かつ 在庫あり)', () => {
  seedStockRows();
  const r = queryPublished({ db, now: NOW, handling: 'active', stock: 'in_stock' });
  assert.deepEqual(keysOf(r), ['a_on_stock']);
});

t('[!] 読める在庫は必ず あり か なし のどちらかに入る (−1 / 0 / 1 / NULL)', () => {
  // 🚨 Codex R1。none を「0 と等しい」にすると、負の在庫がどの絞り込みにも出てこない。
  //    絞り込むだけで行が消えるのがいちばん怖い (「無い」と読んでしまう)
  seedPublished([
    row({ mall_item_key: 'minus', stock_qty: -1, handling_class: HANDLING_ACTIVE }),
    row({ mall_item_key: 'zero', stock_qty: 0, handling_class: HANDLING_ACTIVE }),
    row({ mall_item_key: 'one', stock_qty: 1, handling_class: HANDLING_ACTIVE }),
    row({ mall_item_key: 'nul', stock_qty: null, handling_class: HANDLING_ACTIVE }),
  ]);
  assert.deepEqual(keysOf(queryPublished({ db, now: NOW, stock: 'in_stock' })), ['one']);
  assert.deepEqual(keysOf(queryPublished({ db, now: NOW, stock: 'none' })), ['minus', 'zero']);
  assert.deepEqual(keysOf(queryPublished({ db, now: NOW, stock: 'unknown' })), ['nul']);
  // 3 つを足すと全部になる = どこにも入らない行が無い
  const n = ['in_stock', 'none', 'unknown']
    .reduce((a, s) => a + queryPublished({ db, now: NOW, stock: s }).rows.length, 0);
  assert.equal(n, 4, 'どの絞り込みにも入らない在庫がある');
});

t('[!] 在庫・取扱が「入っている」件数も数える (0 個も入っている扱い)', () => {
  // 🚨 Codex R1。これが無いと、集計を消しても試験が通り、
  //    値が入っている世代にまで「空です」の警告が出る退行に気づけない
  seedStockRows();
  const s = queryPublished({ db, now: NOW }).summary;
  assert.equal(s.handlingKnown, 4, '取扱区分が入っている件数');
  assert.equal(s.stockKnown, 4, '在庫数が入っている件数 (0 個も「入っている」)');
});

t('[!] 絞り込んでも「入っている」件数は動かない (絞り込み前の事実)', () => {
  seedStockRows();
  const s = queryPublished({ db, now: NOW, stock: 'in_stock' }).summary;
  assert.equal(s.handlingKnown, 4);
  assert.equal(s.stockKnown, 4);
});

t('[!] 逆検証: 絞り込みを外すと全部戻る (絞り込みが「効いていない」を検出する)', () => {
  seedStockRows();
  assert.equal(queryPublished({ db, now: NOW }).rows.length, 5);
});

t('[!] 絞り込んでも上の件数 (監視の山) は絞り込む前のまま', () => {
  seedStockRows();
  const r = queryPublished({ db, now: NOW, handling: 'active' });
  assert.equal(r.summary.total, 5, '集計まで絞られている (画面の件数が山の件数と食い違う)');
  assert.equal(r.summary.handlingFiltered, 'active');
});

t('[!] 世代に在庫・取扱が 1 件も入っていないことを件数で言える', () => {
  // 9/9 の夜に実際に起きた形 (夜間バッチが古い版で動いて列が入らなかった)
  seedPublished([row({ mall_item_key: 'x' }), row({ mall_item_key: 'y' })]);
  const r = queryPublished({ db, now: NOW });
  assert.equal(r.summary.handlingKnown, 0);
  assert.equal(r.summary.stockKnown, 0);
  assert.equal(r.rows.length, 2, '列が無いだけで行まで消してはいけない');
});

t('[!] 知らない絞り込みは投げる (黙って全件通さない)', () => {
  seedStockRows();
  assert.throws(() => queryPublished({ db, now: NOW, handling: 'nope' }), /取扱区分の絞り込み が不正/);
  assert.throws(() => queryPublished({ db, now: NOW, stock: 'nope' }), /在庫数の絞り込み が不正/);
  // 🚨 Object.prototype の名前で迂回できない (素の [] だと関数を拾って全行が通る)
  assert.throws(() => queryPublished({ db, now: NOW, handling: 'toString' }), /不正/);
  assert.throws(() => queryPublished({ db, now: NOW, stock: 'constructor' }), /不正/);
});

t('[!] 件数だけの呼び出しでも不正な絞り込みは投げる (件数と一覧で条件が食い違う)', () => {
  assert.throws(() => queryPublished({ db, now: NOW, countOnly: true, stock: 'nope' }), /不正/);
});

console.log('\n一覧と CSV で同じ条件を使う (Codex R1)');

const { expectedProfitFilters } = await import('../profit-analysis/router.js');

t('[!] 絞り込みの項目が全部そのまま渡る', () => {
  const f = expectedProfitFilters({
    mall: 'rakuten', scope: 'fba_v1', state: 'allowed',
    handling: 'active', stock: 'in_stock', sort: 'profit', order: 'asc',
  });
  assert.equal(f.mall, 'rakuten');
  assert.equal(f.expenseScope, 'fba_v1');
  assert.equal(f.state, 'allowed');
  assert.equal(f.handling, 'active');
  assert.equal(f.stock, 'in_stock');
  assert.equal(f.sort, 'profit');
  assert.equal(f.order, 'asc');
  assert.equal(f.rankOnly, true);
});

t('[!] 空文字は「絞り込まない」(空文字を渡すと不正扱いで落ちる)', () => {
  const f = expectedProfitFilters({ handling: '', stock: '' });
  assert.equal(f.handling, undefined);
  assert.equal(f.stock, undefined);
});

t('[!] 文字列でない値を素通しさせない (?stock=a&stock=b は配列で届く)', () => {
  const f = expectedProfitFilters({ stock: ['in_stock', 'none'], handling: { a: 1 } });
  assert.equal(f.stock, undefined);
  assert.equal(f.handling, undefined);
});

t('[!] 既定は「絞り込まない・利益率の良い順・適格な行だけ」(いまの画面の前提)', () => {
  const f = expectedProfitFilters({});
  assert.deepEqual(
    { ...f },
    { mall: undefined, expenseScope: undefined, state: undefined, handling: undefined, stock: undefined,
      rankOnly: true, sort: 'margin', order: 'desc' });
});

/**
 * 🚨 「同じ関数を通している」を文字列で見ても意味がない (Codex R1)。
 *    **両方の入口を実際に呼んで、出てくる行が同じか**を見る。
 *    サーバは起こさず、router に登録されたハンドラを直接呼ぶ
 */
const { default: router } = await import('../profit-analysis/router.js');

function callRoute(path, query) {
  const layer = router.stack.find(l => l.route && l.route.path === path);
  assert.ok(layer, `${path} が router に無い`);
  return new Promise((resolve, reject) => {
    const res = {
      statusCode: 200,
      setHeader() {},
      status(c) { this.statusCode = c; return this; },
      json(body) { resolve({ status: this.statusCode, body }); },
      send(body) { resolve({ status: this.statusCode, text: body }); },
    };
    try { layer.route.stack[0].handle({ query }, res, reject); } catch (e) { reject(e); }
  });
}

/**
 * CSV の「出品コード」列を拾う。ヘッダ行と BOM は落とす。
 * 🚨 列の位置を決め打ちしない (列が増えた日に、隣の列を品番だと思って比べ続ける)
 */
function csvKeys(text) {
  const lines = text.replace(/^﻿/, '').split('\r\n').filter(Boolean);
  const at = lines[0].split(',').indexOf('出品コード');
  assert.ok(at >= 0, 'CSV に「出品コード」の列が無い');
  return lines.slice(1).map(l => l.split(',')[at]).sort();
}

// 🚨 t() は同期用。async を渡すと、投げても捕まらないまま「ok」になる
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

await ta('[!] CSV と一覧が同じ行を出す (絞り込んだ CSV に絞る前の行を混ぜない)', async () => {
  seedStockRows();
  const q = { stock: 'in_stock', state: 'all' };
  const list = await callRoute('/api/expected-profit', q);
  const csv = await callRoute('/api/expected-profit.csv', q);
  assert.equal(list.status, 200, JSON.stringify(list.body));
  const listKeys = list.body.rows.map(r => r.mall_item_key).sort();
  assert.deepEqual(listKeys, ['a_on_stock', 'c_off_stock', 'd_maker_off'], '一覧が絞れていない');
  assert.deepEqual(csvKeys(csv.text), listKeys, 'CSV に一覧と違う行が入っている');
});

await ta('[!] 取扱の絞り込みも CSV に効く', async () => {
  seedStockRows();
  const q = { handling: 'active', state: 'all' };
  const list = await callRoute('/api/expected-profit', q);
  const csv = await callRoute('/api/expected-profit.csv', q);
  assert.deepEqual(csvKeys(csv.text), list.body.rows.map(r => r.mall_item_key).sort());
  assert.equal(csvKeys(csv.text).length, 2);
});

await ta('[!] 知らない絞り込みは一覧が 400 で返す (画面のバグと本番障害を混ぜない)', async () => {
  const r = await callRoute('/api/expected-profit', { stock: 'nope' });
  assert.equal(r.status, 400, '500 だと本番障害と見分けがつかない');
  assert.match(r.body.error, /在庫数の絞り込み が不正/);
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
