/**
 * test-allowance.mjs — 承知のうえの赤字 (許容記録) と 4 つの山 の受入試験
 *
 * ここで固めたいこと:
 *   ・判定できない行は、許容記録があっても「承知のうえ」にしない (安全側)
 *   ・期限切れ・上限超過は自動で要対応に戻る
 *   ・期限は JST の日付で切り替わる (UTC で 1 日ずれない)
 *   ・モールで絞り込んでも監視の件数は動かない
 *
 * 実行: node apps/expected-profit/test-allowance.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-a-'));

const { initExpectedProfitDB } = await import('./db.js');
const {
  classifyRow, normalizeAllowanceInput, upsertAllowance, revokeAllowance,
  loadAllowances, allowanceKey, isActionable, BREAKEVEN_MAX_RATE,
} = await import('./allowance.js');
const { queryPublished } = await import('./query.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

// JST 2026-09-09 12:00
const NOW = new Date('2026-09-09T03:00:00Z');

const judged = (over = {}) => ({
  mall: 'rakuten', shop_id: '1', mall_item_key: 'k1', expense_scope_version: 'self_v1',
  calculation_status: 'ok', expected_profit: -200, expected_margin_rate: -0.2,
  expired_now: 0, rank_eligible_now: 1, rank_exclusion_reason_now: null, incomplete_reason: null,
  ...over,
});

const allow = (over = {}) => ({
  mall: 'rakuten', shop_id: '1', mall_item_key: 'k1', expense_scope_version: 'self_v1',
  reason_code: 'stock_clearance', reason_note: '滞留 12 ヶ月',
  loss_cap_yen: 300, valid_from: '2026-09-01', valid_until: '2026-12-31',
  decided_by: '中原 大輔', review_by: null,
  ...over,
});

console.log('状態の判定 (classifyRow)');

t('赤字で許容記録が無ければ「未許容の赤字」', () => {
  const r = classifyRow(judged(), undefined, NOW);
  assert.equal(r.state, 'unallowed');
  assert.ok(isActionable(r.state));
});

t('許容の期限内かつ上限以内なら「承知のうえ」', () => {
  const r = classifyRow(judged(), allow(), NOW);
  assert.equal(r.state, 'allowed');
  assert.equal(isActionable(r.state), false);
});

t('[!] 期限が切れたら要対応に戻る (黙って隠れ続けない)', () => {
  const r = classifyRow(judged(), allow({ valid_until: '2026-08-31' }), NOW);
  assert.equal(r.state, 'returned');
  assert.equal(r.reason, 'allowance_expired');
  assert.ok(isActionable(r.state));
});

t('[!] 損失上限を超えたら要対応に戻る', () => {
  const r = classifyRow(judged({ expected_profit: -420 }), allow({ loss_cap_yen: 300 }), NOW);
  assert.equal(r.state, 'returned');
  assert.equal(r.reason, 'allowance_cap_exceeded');
});

t('[!] 円未満は切り上げて上限と比べる (−300.4 円は 300 円上限に収まらない)', () => {
  const over = classifyRow(judged({ expected_profit: -300.4 }), allow({ loss_cap_yen: 300 }), NOW);
  assert.equal(over.state, 'returned');
  const inside = classifyRow(judged({ expected_profit: -299.6 }), allow({ loss_cap_yen: 300 }), NOW);
  assert.equal(inside.state, 'allowed');
});

t('[!] 開始日が来ていない許容は効かせない (先の日付で今日から隠せない)', () => {
  const r = classifyRow(judged(), allow({ valid_from: '2026-10-01' }), NOW);
  assert.equal(r.state, 'unallowed');
  assert.equal(r.reason, 'allowance_not_started');
});

t('[!] 期限は JST の日付で切り替わる (UTC で 1 日ずれない)', () => {
  const a = allow({ valid_until: '2026-09-09' });
  // JST 9/9 23:59 = まだ有効
  assert.equal(classifyRow(judged(), a, new Date('2026-09-09T14:59:00Z')).state, 'allowed');
  // JST 9/10 00:00 = 期限切れ
  assert.equal(classifyRow(judged(), a, new Date('2026-09-09T15:00:00Z')).state, 'returned');
});

t('[!] 判定できない行は、許容記録があっても unknown のまま (安全側)', () => {
  for (const bad of [
    { calculation_status: 'incomplete' },
    { expected_profit: null },
    { expired_now: 1 },
    { rank_eligible_now: 0, rank_exclusion_reason_now: 'quantity_shipping_unknown' },
  ]) {
    const r = classifyRow(judged(bad), allow(), NOW);
    assert.equal(r.state, 'unknown', JSON.stringify(bad));
    assert.equal(r.allowance, null, '判定できない行に許容を付けない');
  }
});

t('黒字は ok、5% 未満の黒字は breakeven', () => {
  assert.equal(classifyRow(judged({ expected_profit: 300, expected_margin_rate: 0.3 }), null, NOW).state, 'ok');
  assert.equal(classifyRow(judged({ expected_profit: 10, expected_margin_rate: 0.01 }), null, NOW).state, 'breakeven');
  // 境界: ちょうど 5% は黒字側
  assert.equal(classifyRow(judged({ expected_profit: 50, expected_margin_rate: BREAKEVEN_MAX_RATE }), null, NOW).state, 'ok');
});

t('利益 0 円は赤字ではない (トントン)', () => {
  const r = classifyRow(judged({ expected_profit: 0, expected_margin_rate: 0 }), null, NOW);
  assert.equal(r.state, 'breakeven');
});

console.log('\n入力の検証 (normalizeAllowanceInput)');

const goodInput = (over = {}) => ({
  mall: 'rakuten', shop_id: '1', mall_item_key: 'k1', expense_scope_version: 'self_v1',
  reason_code: 'stock_clearance', reason_note: '滞留 12 ヶ月',
  loss_cap_yen: 300, valid_until: '2099-12-31', decided_by: '中原 大輔',
  ...over,
});

t('正しい入力は通る', () => {
  const { errors, value } = normalizeAllowanceInput(goodInput(), NOW);
  assert.deepEqual(errors, []);
  assert.equal(value.loss_cap_yen, 300);
});

t('[!] 無期限は登録できない (期限の無い許容は二度と見直されない)', () => {
  const { errors } = normalizeAllowanceInput(goodInput({ valid_until: '' }), NOW);
  assert.ok(errors.some(e => e.includes('期限は必須')), errors.join('/'));
});

t('[!] 過去の日付は期限にできない (登録した瞬間に切れているものを作らせない)', () => {
  const { errors } = normalizeAllowanceInput(goodInput({ valid_until: '2020-01-01' }), NOW);
  assert.ok(errors.some(e => e.includes('過去の日付')), errors.join('/'));
});

/* 🚨 2026-09-10。この 2 件は「書いた日を過ぎると勝手に落ちる」試験だった。
      normalizeAllowanceInput が開始日の既定に **実際の今日** を使っていたので、
      固定した NOW (9/9) より未来の開始日になり、9/10 以降は許容が
      allowance_not_started で効かなくなっていた (本番の不具合ではない)。
      検証にも「今」を渡せるようにして、以下で**時計に依らない**ことを固定する */
t('[!] 開始日の既定は「渡した今」(実時刻を見ない = 明日になっても結果が変わらない)', () => {
  const a = normalizeAllowanceInput(goodInput(), new Date('2026-01-15T03:00:00Z'));
  assert.deepEqual(a.errors, []);
  assert.equal(a.value.valid_from, '2026-01-15', '渡した今ではなく実時刻を見ている');
  // 何年か先を渡しても、その日が開始日になる (実時刻に引きずられない)
  const b = normalizeAllowanceInput(goodInput(), new Date('2030-07-04T03:00:00Z'));
  assert.equal(b.value.valid_from, '2030-07-04');
});

t('[!] 「過去の日付」の判定も渡した今で決まる', () => {
  // 2026-06-30 は 2026-01-15 から見れば未来、2030 から見れば過去
  const future = normalizeAllowanceInput(goodInput({ valid_until: '2026-06-30' }), new Date('2026-01-15T03:00:00Z'));
  assert.deepEqual(future.errors, []);
  const past = normalizeAllowanceInput(goodInput({ valid_until: '2026-06-30' }), new Date('2030-07-04T03:00:00Z'));
  assert.ok(past.errors.some(e => e.includes('過去の日付')), past.errors.join('/'));
});

t('[!] 登録したその日から効く (開始日の既定と classifyRow の今がそろっている)', () => {
  // 🚨 これが 2026-09-10 に落ちていた形。既定の開始日で登録して、同じ「今」で判定する
  const at = new Date('2026-09-09T03:00:00Z');
  const { value } = normalizeAllowanceInput(goodInput(), at);
  const { state } = classifyRow(judged(), { ...value, revoked_at: null }, at);
  assert.equal(state, 'allowed', '登録した当日に「まだ始まっていない」になっている');
});

t('[!] 上限は 0 以上の整数だけ (桁の打ち間違いで青天井にしない)', () => {
  assert.ok(normalizeAllowanceInput(goodInput({ loss_cap_yen: -1 }), NOW).errors.length);
  assert.ok(normalizeAllowanceInput(goodInput({ loss_cap_yen: 1.5 }), NOW).errors.length);
  assert.ok(normalizeAllowanceInput(goodInput({ loss_cap_yen: 999999 }), NOW).errors.length);
  assert.equal(normalizeAllowanceInput(goodInput({ loss_cap_yen: '1,200' }), NOW).errors.length, 0);
});

t('理由と狙いの説明は必須 (「その他」を選んだだけで通さない)', () => {
  assert.ok(normalizeAllowanceInput(goodInput({ reason_code: '' }), NOW).errors.length);
  assert.ok(normalizeAllowanceInput(goodInput({ reason_note: '   ' }), NOW).errors.length);
});

t('出荷区分は self_v1 / fba_v1 だけ', () => {
  assert.ok(normalizeAllowanceInput(goodInput({ expense_scope_version: 'all' }), NOW).errors.length);
});

console.log('\n登録と取り消し');

const db = initExpectedProfitDB();

t('登録すると有効な許容として読める', () => {
  const { value } = normalizeAllowanceInput(goodInput(), NOW);
  upsertAllowance(db, value, 'tester@example.com', NOW);
  const map = loadAllowances(db, { expenseScope: 'self_v1' });
  assert.equal(map.size, 1);
  assert.ok(map.has(allowanceKey('rakuten', '1', 'k1', 'self_v1')));
});

t('[!] 同じ出品に 2 本作らない (上書きでも作った人と作成時刻は残す)', () => {
  const { value } = normalizeAllowanceInput(goodInput({ loss_cap_yen: 500 }), NOW);
  const saved = upsertAllowance(db, value, 'other@example.com', new Date('2026-09-10T03:00:00Z'));
  const map = loadAllowances(db, { expenseScope: 'self_v1' });
  assert.equal(map.size, 1);
  assert.equal(map.get(allowanceKey('rakuten', '1', 'k1', 'self_v1')).loss_cap_yen, 500);
  assert.equal(saved.created_by, 'tester@example.com', '最初に作った人が残っていない');
});

t('[!] 出荷区分が違えば別の判断 (自社出荷の許容を FBA に効かせない)', () => {
  const { value } = normalizeAllowanceInput(goodInput({ expense_scope_version: 'fba_v1' }), NOW);
  upsertAllowance(db, value, 'tester@example.com', NOW);
  assert.equal(loadAllowances(db, { expenseScope: 'self_v1' }).size, 1);
  assert.equal(loadAllowances(db, { expenseScope: 'fba_v1' }).size, 1);
});

t('取り消すと有効な許容から外れる', () => {
  const r = revokeAllowance(db,
    { mall: 'rakuten', shop_id: '1', mall_item_key: 'k1', expense_scope_version: 'fba_v1' },
    'tester@example.com', NOW);
  assert.ok(r);
  assert.equal(loadAllowances(db, { expenseScope: 'fba_v1' }).size, 0);
});

t('取り消し済みをもう一度取り消しても落ちない (null が返る)', () => {
  const r = revokeAllowance(db,
    { mall: 'rakuten', shop_id: '1', mall_item_key: 'k1', expense_scope_version: 'fba_v1' },
    'tester@example.com', NOW);
  assert.equal(r, null);
});

t('[!] 誰がいつ何をしたかが履歴に残る (取り消しても消えない)', () => {
  const logs = db.prepare(`SELECT action, actor FROM expected_profit_allowance_log
    WHERE mall_item_key = 'k1' ORDER BY log_id`).all();
  const actions = logs.map(l => l.action);
  assert.deepEqual(actions, ['create', 'update', 'create', 'revoke']);
  assert.equal(logs[1].actor, 'other@example.com');
});

console.log('\n4 つの山 (queryPublished)');

function seed(rows, genId = 'g2', seq = 2, publish = true) {
  db.prepare('DELETE FROM mart_listing_expected_profit WHERE generation_id = ?').run(genId);
  db.prepare(`INSERT OR REPLACE INTO expected_profit_generation
    (generation_id, seq, built_at, local_status, remote_status, row_count, ok_count, incomplete_count,
     rank_eligible_count, content_hash, malls_included, malls_degraded)
    VALUES (?, ?, '2026-09-09T00:00:00Z', 'validated', 'published', ?, ?, 0, ?, 'h', '["rakuten","yahoo"]', '[]')`)
    .run(genId, seq, rows.length, rows.length, rows.length);
  const ins = db.prepare(`INSERT OR REPLACE INTO mart_listing_expected_profit
    (generation_id, mall, shop_id, mall_item_key, fulfillment, expected_profit, expected_margin_rate,
     listing_enum_status, price_status, fee_status, cost_status, shipping_master_status,
     scenario_fit, calculation_status, rank_eligible, rank_exclusion_reason, expense_scope_version,
     input_snapshot, formula_version, scenario_version, fee_rate_version, code_version, built_at)
    VALUES (?, @mall, '1', @mall_item_key, 'self', @expected_profit, @expected_margin_rate,
     'ok', 'ok', 'not_applicable', 'ok', 'ok',
     'ok', @calculation_status, @rank_eligible, @rank_exclusion_reason, 'self_v1',
     '{}', 'v1', 'v1', 'v1', 'test', '2026-09-09T00:00:00Z')`);
  for (const r of rows) {
    ins.run(genId, {
      mall: 'rakuten', calculation_status: 'ok', rank_eligible: 1, rank_exclusion_reason: null,
      expected_margin_rate: r.expected_profit == null ? null : r.expected_profit / 1000, ...r,
    });
  }
  if (publish) {
    db.prepare(`INSERT OR REPLACE INTO expected_profit_publish_pointer (id, generation_id, seq, published_at)
                VALUES (1, ?, ?, '2026-09-09T00:00:00Z')`).run(genId, seq);
  }
}

t('4 つの山を数える (赤字 / トントン / 判定できない / 承知のうえ)', () => {
  seed([
    { mall_item_key: 'neg1', expected_profit: -200 },
    { mall_item_key: 'neg2', expected_profit: -50 },
    { mall_item_key: 'k1', expected_profit: -100 },                       // 許容記録あり (上限 500)
    { mall_item_key: 'be', expected_profit: 10, expected_margin_rate: 0.01 },
    { mall_item_key: 'pos', expected_profit: 300, expected_margin_rate: 0.3 },
    { mall_item_key: 'ng', expected_profit: -900, calculation_status: 'incomplete', rank_eligible: 0,
      rank_exclusion_reason: 'cost_missing' },
  ]);
  const r = queryPublished({ db, now: NOW, state: 'actionable' });
  assert.equal(r.summary.actionable, 2, '要対応');
  assert.equal(r.summary.allowed, 1, '承知のうえ');
  assert.equal(r.summary.breakeven, 1);
  assert.equal(r.summary.positive, 1);
  assert.equal(r.summary.unknown, 1);
  assert.equal(r.rows.length, 2);
});

t('[!] 判定できない行は要対応にも黒字にも混ざらない', () => {
  const r = queryPublished({ db, now: NOW, state: 'unknown' });
  assert.equal(r.rows.length, 1);
  assert.equal(r.rows[0].mall_item_key, 'ng');
  assert.equal(r.rows[0].monitor_state, 'unknown');
});

t('[!] いちばん深い赤字は要対応の中から選ぶ (許容中のものを混ぜない)', () => {
  const r = queryPublished({ db, now: NOW, state: 'actionable' });
  assert.equal(r.summary.worst.mall_item_key, 'neg1');
  assert.equal(r.summary.worst.expected_profit, -200);
});

t('[!] モールで絞っても監視の件数は動かない (0 件を「赤字なし」と読み違えさせない)', () => {
  seed([
    { mall_item_key: 'r1', mall: 'rakuten', expected_profit: -200 },
    { mall_item_key: 'y1', mall: 'yahoo', expected_profit: -300 },
  ]);
  const all = queryPublished({ db, now: NOW, state: 'actionable' });
  const only = queryPublished({ db, now: NOW, state: 'actionable', mall: 'yahoo' });
  assert.equal(all.summary.actionable, 2);
  assert.equal(only.summary.actionable, 2, 'summary がモールで絞られている');
  assert.equal(only.rows.length, 1, '一覧はモールで絞る');
  assert.equal(only.summary.mallFiltered, 'yahoo');
});

t('[!] 前の世代が無ければ「今回はじめて」を出さない (0 件と「分からない」を混ぜない)', () => {
  db.prepare("DELETE FROM expected_profit_generation WHERE seq < 9").run();
  db.prepare("DELETE FROM mart_listing_expected_profit WHERE generation_id != 'gX'").run();
  seed([{ mall_item_key: 'a', expected_profit: -100 }], 'gX', 9);
  const r = queryPublished({ db, now: NOW, state: 'actionable' });
  assert.equal(r.previous, null);
  assert.equal(r.summary.newlyNegative, null);
  assert.equal(r.rows[0].is_newly_negative, null);
});

t('[!] 前の世代と比べて「今回はじめて」を出す', () => {
  seed([{ mall_item_key: 'old', expected_profit: -100 }], 'gPrev', 10);
  seed([
    { mall_item_key: 'old', expected_profit: -120 },      // 前もいた = 継続
    { mall_item_key: 'new', expected_profit: -80 },       // 今回はじめて
  ], 'gNow', 11);
  const r = queryPublished({ db, now: NOW, state: 'actionable' });
  assert.equal(r.previous.generation_id, 'gPrev');
  assert.equal(r.summary.newlyNegative, 1);
  assert.equal(r.summary.continuedNegative, 1);
  const byKey = Object.fromEntries(r.rows.map(x => [x.mall_item_key, x.is_newly_negative]));
  assert.equal(byKey.new, 1);
  assert.equal(byKey.old, 0);
});

t('state が不正なら黙って全件返さずに落ちる', () => {
  assert.throws(() => queryPublished({ db, now: NOW, state: 'いいかんじ' }), /state が不正/);
});

console.log('\nCodex レビュー 1 巡目の指摘');

t('[!] 実在しない日付を期限にできない (2027-02-30 は「ずっと先」として通ってしまう)', () => {
  for (const bad of ['2027-02-30', '2099-99-99', '2026-13-01', '2026-00-10']) {
    const { errors } = normalizeAllowanceInput(goodInput({ valid_until: bad }), NOW);
    assert.ok(errors.some(e => e.includes('実在する日付')), `${bad} が通った: ${errors.join('/')}`);
  }
  // うるう年は通す
  assert.equal(normalizeAllowanceInput(goodInput({ valid_until: '2028-02-29' }), NOW).errors.length, 0);
  assert.ok(normalizeAllowanceInput(goodInput({ valid_until: '2027-02-29' }), NOW).errors.length);
});

t('[!] 上限は整数表記でなければ通さない ("300.9" が 300 として登録されない)', () => {
  for (const bad of ['300.9', '300abc', '3e2', ' 30 0', '']) {
    assert.ok(normalizeAllowanceInput(goodInput({ loss_cap_yen: bad }), NOW).errors.length, `${bad} が通った`);
  }
});

t('[!] Object.prototype の名前を理由として通さない (reason_code=toString)', () => {
  for (const bad of ['toString', 'constructor', 'hasOwnProperty', '__proto__']) {
    const { errors } = normalizeAllowanceInput(goodInput({ reason_code: bad }), NOW);
    assert.ok(errors.some(e => e.includes('理由を選んで')), `${bad} が通った`);
  }
});

t('[!] state に Object.prototype の名前を渡しても全件が通らない', () => {
  // 素の [] だと STATE_FILTERS['toString'] が関数を返し、絞り込みが効かないうえ
  // rankOnly まで迂回する (Codex 指摘 4)
  for (const bad of ['toString', 'constructor', '__proto__', 'hasOwnProperty']) {
    assert.throws(() => queryPublished({ db, now: NOW, state: bad }), /state が不正/, bad);
  }
});

t('[!] 許容の条件が壊れていたら許容しない (上限が非数値だとどんな赤字も隠れる)', () => {
  for (const broken of [
    { loss_cap_yen: 'たくさん' }, { loss_cap_yen: null }, { loss_cap_yen: -1 },
    { valid_until: '2027-02-30' }, { valid_from: 'いつか' },
  ]) {
    const r = classifyRow(judged({ expected_profit: -100000 }), allow(broken), NOW);
    assert.equal(r.state, 'unallowed', JSON.stringify(broken));
    assert.equal(r.reason, 'allowance_invalid');
  }
});

t('[!] 比較相手は「公開まで到達し、明細が残っている世代」だけ', () => {
  db.prepare('DELETE FROM expected_profit_generation').run();
  db.prepare('DELETE FROM mart_listing_expected_profit').run();
  // 検証に落ちた世代 (明細あり・未公開) は比較相手にしない
  seed([{ mall_item_key: 'x', expected_profit: -10 }], 'gBad', 20);
  db.prepare("UPDATE expected_profit_generation SET remote_status = 'not_sent' WHERE generation_id = 'gBad'").run();
  // 明細を消したあとの世代も比較相手にしない
  seed([{ mall_item_key: 'y', expected_profit: -10 }], 'gEmpty', 21);
  db.prepare("DELETE FROM mart_listing_expected_profit WHERE generation_id = 'gEmpty'").run();
  seed([{ mall_item_key: 'z', expected_profit: -10 }], 'gCur', 22);
  const r = queryPublished({ db, now: NOW, state: 'actionable' });
  assert.equal(r.previous, null, '公開していない世代・明細の無い世代を選んでいる');
  assert.equal(r.summary.newlyNegative, null);
});

t('[!] 前の世代があれば、対象が 0 件でも「今回はじめて 0 件」と言える', () => {
  db.prepare('DELETE FROM expected_profit_generation').run();
  db.prepare('DELETE FROM mart_listing_expected_profit').run();
  seed([{ mall_item_key: 'p', expected_profit: 500, expected_margin_rate: 0.5 }], 'gP', 30);
  seed([{ mall_item_key: 'p', expected_profit: 500, expected_margin_rate: 0.5 }], 'gC', 31);
  const r = queryPublished({ db, now: NOW, state: 'actionable' });
  assert.equal(r.summary.actionable, 0);
  assert.equal(r.summary.newlyNegative, 0, '比較できているのに null になっている');
  assert.equal(r.summary.continuedNegative, 0);
});

t('[!] state=all で黒字も含めて全部返る (照合・CSV の入口)', () => {
  db.prepare('DELETE FROM expected_profit_generation').run();
  db.prepare('DELETE FROM mart_listing_expected_profit').run();
  seed([
    { mall_item_key: 'neg', expected_profit: -100 },
    { mall_item_key: 'pos', expected_profit: 500, expected_margin_rate: 0.5 },
    { mall_item_key: 'ng', expected_profit: null, calculation_status: 'incomplete',
      rank_eligible: 0, rank_exclusion_reason: 'cost_missing' },
  ], 'gAll', 50);
  const r = queryPublished({ db, now: NOW, state: 'all' });
  assert.equal(r.rows.length, 3, '判定できない行も含めて全部返す');
  assert.equal(r.summary.positive, 1);
});

t('countOnly は件数だけ返す (並び替えも一覧も作らない)', () => {
  db.prepare('DELETE FROM expected_profit_generation').run();
  db.prepare('DELETE FROM mart_listing_expected_profit').run();
  seed([
    { mall_item_key: 'a', expected_profit: -10 },
    { mall_item_key: 'b', expected_profit: -20 },
  ], 'gOnly', 40);
  const r = queryPublished({ db, now: NOW, countOnly: true });
  assert.equal(r.rows.length, 0);
  assert.equal(r.total, 0);
  assert.equal(r.summary.actionable, 2);
});

console.log('\nCSV の列 (改名漏れを検知する)');

// 🚨 router 側の列キーを行の実データと突き合わせる。
//    is_newly_actionable → is_newly_negative の改名で、CSV だけ旧名が残り
//    全行が空欄になっていた (Codex 3巡目)。静的な目視では見つからない
const { EXPECTED_PROFIT_CSV_COLS, expectedProfitCsvRow, EASYSHIP_STATUS_LABEL_FOR_TEST } =
  await import('../profit-analysis/router.js');
// 🚨 状態の一覧は正本から取る。ここに写すと足し忘れを検出できない
const { EASYSHIP_STATUSES } = await import('./easyship-rates.js');

t('[!] CSV の全列が、実際の行から値を取れる (存在しないキーは空欄になって気づけない)', () => {
  db.prepare('DELETE FROM expected_profit_generation').run();
  db.prepare('DELETE FROM mart_listing_expected_profit').run();
  seed([{ mall_item_key: 'csv1', expected_profit: -123 }], 'gCsv', 60);
  const { value } = normalizeAllowanceInput(goodInput({ mall_item_key: 'csv1' }), NOW);
  upsertAllowance(db, value, 'tester@example.com', NOW);

  const r = queryPublished({ db, now: NOW, state: 'all' });
  const flat = expectedProfitCsvRow(r.rows[0]);
  const missing = EXPECTED_PROFIT_CSV_COLS
    .map(([label, key]) => [label, key])
    .filter(([, key]) => !Object.hasOwn(flat, key))
    .map(([label, key]) => `${label} (${key})`);
  assert.deepEqual(missing, [], `行に存在しないキーを CSV が参照している: ${missing.join(', ')}`);
});

t('[!] 許容の情報と監視状態が CSV に出る', () => {
  const r = queryPublished({ db, now: NOW, state: 'all' });
  const flat = expectedProfitCsvRow(r.rows[0]);
  assert.equal(flat.monitor_state_label, '承知のうえ');
  assert.equal(flat.allowance_cap, 300);
  assert.equal(flat.allowance_decided_by, '中原 大輔');
});

t('[!] CSV も Easy Ship の状態を全部、日本語で出す (Codex P2)', () => {
  // 🚨 状態を足して言葉を足し忘れると、CSV に内部の英語がそのまま出る
  for (const status of EASYSHIP_STATUSES) {
    assert.ok(EASYSHIP_STATUS_LABEL_FOR_TEST[status], `CSV の言葉に ${status} が無い`);
  }
});

t('[!] CSV の「出せる在庫」は、在庫数と引当数が両方読めるときだけ出す', () => {
  db.prepare('DELETE FROM expected_profit_generation').run();
  db.prepare('DELETE FROM mart_listing_expected_profit').run();
  seed([{ mall_item_key: 'stk', expected_profit: -100 }], 'gStk', 70);
  const upd = db.prepare(`UPDATE mart_listing_expected_profit
    SET handling_class = ?, stock_qty = ?, stock_allocated_qty = ? WHERE mall_item_key = 'stk'`);

  upd.run('取扱中', 12, 3);
  let flat = expectedProfitCsvRow(queryPublished({ db, now: NOW, state: 'all' }).rows[0]);
  assert.equal(flat.handling_class, '取扱中');
  assert.equal(flat.stock_qty, 12);
  assert.equal(flat.stock_allocated_qty, 3);
  assert.equal(flat.stock_free, 9);

  // 🚨 引当が分からない行で 12 を「出せる在庫」にしない (分からないものを断定しない)
  upd.run('取扱中', 12, null);
  flat = expectedProfitCsvRow(queryPublished({ db, now: NOW, state: 'all' }).rows[0]);
  assert.equal(flat.stock_qty, 12);
  assert.equal(flat.stock_free, null);

  // 在庫0 は「分からない」ではない。0 のまま出す
  upd.run('取扱終了', 0, 0);
  flat = expectedProfitCsvRow(queryPublished({ db, now: NOW, state: 'all' }).rows[0]);
  assert.equal(flat.handling_class, '取扱終了');
  assert.equal(flat.stock_qty, 0);
  assert.equal(flat.stock_free, 0);
});

console.log(`\n${passed} 件 PASS`);
