#!/usr/bin/env node
/**
 * test-rakuten-point-campaign.mjs — 商品別ポイント変倍 月次自動設定の純ロジック検証 (ネットワーク不使用)
 * 実行: node apps/warehouse/test-rakuten-point-campaign.mjs
 */
import {
  jstParts, lastDayOfMonth, jstIso, parsePointRate, parseSheetRows,
  computeMonthlyPeriod, isRunDay, campaignEquals, campaignState, planPointCampaigns,
} from './rakuten-point-campaign-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

console.log('=== 1. 日付ユーティリティ ===');
{
  const p = jstParts('2026-07-31T15:30:00Z'); // JST 8/1 00:30
  check('jstParts: UTC 7/31 15:30 は JST 8/1 00:30', p.month === 8 && p.day === 1 && p.hour === 0 && p.minute === 30, JSON.stringify(p));
  check('jstParts: 不正日時は null', jstParts('なにか') === null);
  check('lastDayOfMonth: 2026-02 = 28', lastDayOfMonth(2026, 2) === 28);
  check('lastDayOfMonth: 2028-02 = 29 (うるう年)', lastDayOfMonth(2028, 2) === 29);
  check('lastDayOfMonth: 2026-08 = 31', lastDayOfMonth(2026, 8) === 31);
  check('jstIso 形式', jstIso(2026, 8, 1, 12, 0, 0) === '2026-08-01T12:00:00+09:00', jstIso(2026, 8, 1, 12, 0, 0));
}

console.log('=== 2. 倍率パース ===');
{
  check('"10倍" → 10', parsePointRate('10倍').rate === 10);
  check('"10" → 10', parsePointRate('10').rate === 10);
  check('全角 "１０倍" → 10', parsePointRate('１０倍').rate === 10);
  check('前後空白 " 5倍 " → 5', parsePointRate(' 5倍 ').rate === 5);
  check('"1倍" は範囲外エラー', parsePointRate('1倍').ok === false);
  check('"21倍" は範囲外エラー', parsePointRate('21倍').ok === false);
  check('"20倍" は上限OK', parsePointRate('20倍').rate === 20);
  check('"2倍" は下限OK', parsePointRate('2倍').rate === 2);
  check('"10%" はエラー', parsePointRate('10%').ok === false);
  check('空はエラー', parsePointRate('').ok === false);
  check('undefined はエラー', parsePointRate(undefined).ok === false);
}

console.log('=== 3. シート行パース ===');
{
  const noHeader = parseSheetRows([['aromaorb', 'x', '10倍']]);
  check('ヘッダー行が無ければ ok:false (別シート誤読の防止)', noHeader.ok === false && noHeader.errors.some((e) => e.includes('ヘッダー')), JSON.stringify(noHeader.errors));
  const badHeaderB = parseSheetRows([['商品コード', '別の列名', 'ポイント割引率'], ['aromaorb', 'x', '10倍']]);
  check('B列ヘッダー不一致も ok:false', badHeaderB.ok === false && badHeaderB.errors.some((e) => e.includes('ヘッダー')), JSON.stringify(badHeaderB.errors));

  const values = [
    ['商品コード', '商品名', 'ポイント割引率'],
    ['aromaorb', 'アットアロマ オーブ', '10倍'],
    ['chouyous5P', 'ちょうようせっけん', '10倍'], // 大文字混じり → 小文字化
    ['', '', ''],
  ];
  const r = parseSheetRows(values);
  check('ヘッダー・空行を除いて2件', r.ok && r.rows.length === 2, JSON.stringify(r));
  check('大文字は小文字に寄せる', r.rows?.[1]?.manageNumber === 'chouyous5p');
  check('商品名と倍率が取れる', r.rows?.[0]?.name === 'アットアロマ オーブ' && r.rows?.[0]?.pointRate === 10);

  const HEADER = ['商品コード', '商品名', 'ポイント割引率'];
  const bad = parseSheetRows([HEADER, ['aromaorb', 'x', '10倍'], ['日本語コード', 'y', '10倍']]);
  check('不正コードが1行でもあれば ok:false', bad.ok === false && bad.errors.some((e) => e.includes('日本語コード')), JSON.stringify(bad.errors));

  const badRate = parseSheetRows([HEADER, ['aromaorb', 'x', '30倍']]);
  check('範囲外倍率も ok:false', badRate.ok === false);

  const empty = parseSheetRows([HEADER]);
  check('データ0件は ok:false', empty.ok === false);

  const dup = parseSheetRows([HEADER, ['aromaorb', 'x', '10倍'], ['AROMAORB', 'y', '5倍']]);
  check('小文字化後の重複を検出', dup.ok === false && dup.errors.some((e) => e.includes('重複')), JSON.stringify(dup.errors));

  const many = parseSheetRows([HEADER, ...Array.from({ length: 101 }, (_, i) => [`item${i}`, 'x', '10倍'])]);
  check('安全上限 (100件) 超過は ok:false', many.ok === false);
}

console.log('=== 4. 当月期間の計算 ===');
{
  // 本番想定: 8/1 04:20 JST に実行
  const a = computeMonthlyPeriod('2026-07-31T19:20:00Z'); // JST 8/1 04:20
  check('1日深夜 → 1日12:00開始', a.ok && a.start === '2026-08-01T12:00:00+09:00' && !a.delayed, JSON.stringify(a));
  check('終了は月末 23:59:59', a.end === '2026-08-31T23:59:59+09:00');
  check('ym ラベル', a.ym === '2026-08');

  // リカバリ: 8/2 04:20 JST → 8/2 12:00 開始 (delayed)
  const b = computeMonthlyPeriod('2026-08-01T19:20:00Z');
  check('2日深夜のリカバリ → 2日12:00開始 (delayed)', b.ok && b.start === '2026-08-02T12:00:00+09:00' && b.delayed, JSON.stringify(b));

  // 1日でも 11:00 を過ぎたら当日12:00には間に合わない扱い → 2日12:00
  const c = computeMonthlyPeriod('2026-08-01T02:30:00Z'); // JST 8/1 11:30
  check('1日11:30 → 2日12:00開始', c.ok && c.start === '2026-08-02T12:00:00+09:00' && c.delayed, JSON.stringify(c));

  // 月末日の深夜はまだ間に合う
  const d = computeMonthlyPeriod('2026-08-30T19:00:00Z'); // JST 8/31 04:00
  check('月末日深夜 → 31日12:00開始', d.ok && d.start === '2026-08-31T12:00:00+09:00', JSON.stringify(d));

  // 月末日の 11:00 以降はもう当月に入れられない
  const e = computeMonthlyPeriod('2026-08-31T03:00:00Z'); // JST 8/31 12:00
  check('月末日昼以降は ok:false', e.ok === false, JSON.stringify(e));

  // 2月 (28日) の月末処理
  const f = computeMonthlyPeriod('2026-02-01T19:00:00+09:00');
  check('2月は 2/28 23:59:59 終了', f.ok && f.end === '2026-02-28T23:59:59+09:00', JSON.stringify(f));
}

console.log('=== 5. 実行日ゲート ===');
{
  check('1日は実行日', isRunDay('2026-07-31T19:20:00Z') === true); // JST 8/1
  check('3日は実行日', isRunDay('2026-08-02T19:20:00Z') === true); // JST 8/3
  check('4日は実行日でない', isRunDay('2026-08-03T19:20:00Z') === false); // JST 8/4
  check('15日は実行日でない', isRunDay('2026-08-15T00:00:00+09:00') === false);
}

console.log('=== 6. 一致・有効判定 ===');
{
  const desired = { pointRate: 10, start: '2026-08-01T12:00:00+09:00', end: '2026-08-31T23:59:59+09:00' };
  const same = { benefits: { pointRate: 10 }, applicablePeriod: { start: '2026-08-01T12:00:00+09:00', end: '2026-08-31T23:59:59+09:00' } };
  check('完全一致 → true', campaignEquals(same, desired) === true);
  check('倍率違い → false', campaignEquals({ ...same, benefits: { pointRate: 5 } }, desired) === false);
  check('期間違い → false', campaignEquals({ benefits: { pointRate: 10 }, applicablePeriod: { start: '2026-08-02T12:00:00+09:00', end: desired.end } }, desired) === false);
  check('未設定 (null) → false', campaignEquals(null, desired) === false);

  const aug = { benefits: { pointRate: 10 }, applicablePeriod: { start: '2026-08-01T12:00:00+09:00', end: '2026-08-31T23:59:59+09:00' } };
  check('期間内は blocking', campaignState(aug, '2026-08-15T00:00:00+09:00') === 'blocking');
  check('未来開始も blocking (先行予約的な手動設定を踏まない)', campaignState(aug, '2026-08-01T03:00:00+09:00') === 'blocking');
  check('終了後は expired', campaignState(aug, '2026-09-01T03:00:00+09:00') === 'expired');
  check('未設定は none', campaignState(null, '2026-08-15T00:00:00+09:00') === 'none');
  check('期間が読めない値は invalid (上書き側に落とさない)', campaignState({ benefits: { pointRate: 10 }, applicablePeriod: { start: 'x', end: 'y' } }, '2026-08-15T00:00:00+09:00') === 'invalid');
  check('start > end は invalid', campaignState({ benefits: { pointRate: 10 }, applicablePeriod: { start: '2026-08-31T00:00:00+09:00', end: '2026-08-01T00:00:00+09:00' } }, '2026-08-15T00:00:00+09:00') === 'invalid');
}

console.log('=== 7. 実行計画 ===');
{
  const nowIso = '2026-08-01T04:20:00+09:00';
  const period = { ok: true, ym: '2026-08', start: '2026-08-01T12:00:00+09:00', end: '2026-08-31T23:59:59+09:00', delayed: false };
  const rows = [
    { manageNumber: 'aromaorb', name: 'オーブ', pointRate: 10 },
    { manageNumber: 'aromasolo', name: 'ソロ', pointRate: 10 },
    { manageNumber: 'missing', name: '無い', pointRate: 10 },
    { manageNumber: 'already', name: '設定済み', pointRate: 10 },
    { manageNumber: 'manual', name: '手動設定中', pointRate: 10 },
    { manageNumber: 'future', name: '未来開始の手動設定', pointRate: 10 },
    { manageNumber: 'broken', name: '期間不正', pointRate: 10 },
    { manageNumber: 'neterr', name: 'GET失敗', pointRate: 10 },
  ];
  const augSet = { benefits: { pointRate: 10 }, applicablePeriod: { start: period.start, end: period.end } };
  const manualActive = { benefits: { pointRate: 5 }, applicablePeriod: { start: '2026-07-20T00:00:00+09:00', end: '2026-08-10T23:59:59+09:00' } };
  const currentByMn = new Map([
    ['aromaorb', { status: 200, item: {} }],                       // 未設定 → set
    ['aromasolo', { status: 200, item: { pointCampaign: { benefits: { pointRate: 2 }, applicablePeriod: { start: '2026-07-01T12:00:00+09:00', end: '2026-07-31T23:59:59+09:00' } } } }], // 先月分 (期限切れ) → set
    ['missing', { status: 404, item: null }],                      // → error
    ['already', { status: 200, item: { pointCampaign: augSet } }], // → skip
    ['manual', { status: 200, item: { pointCampaign: manualActive } }], // 有効中の別設定 → conflict
    ['future', { status: 200, item: { pointCampaign: { benefits: { pointRate: 15 }, applicablePeriod: { start: '2026-08-20T00:00:00+09:00', end: '2026-08-25T23:59:59+09:00' } } } }], // 未来開始 → conflict
    ['broken', { status: 200, item: { pointCampaign: { benefits: { pointRate: 10 }, applicablePeriod: { start: 'invalid', end: 'invalid' } } } }], // → error
    ['neterr', { status: 0, item: null, error: 'timeout' }], // GET失敗 → error (他商品は続行)
  ]);
  const plans = planPointCampaigns({ rows, currentByMn, period, nowIso });
  const by = Object.fromEntries(plans.map((p) => [p.manageNumber, p]));
  check('未設定 → set', by.aromaorb.action === 'set');
  check('patchBody が実測仕様どおり', JSON.stringify(by.aromaorb.patchBody) === JSON.stringify({
    pointCampaign: { applicablePeriod: { start: period.start, end: period.end }, benefits: { pointRate: 10 } },
  }), JSON.stringify(by.aromaorb.patchBody));
  check('先月分の残り (期限切れ) → set', by.aromasolo.action === 'set');
  check('404 → error', by.missing.action === 'error');
  check('今月分設定済み → skip', by.already.action === 'skip');
  check('有効中の別設定 → conflict (上書きしない)', by.manual.action === 'conflict');
  check('未来開始の別設定 → conflict', by.future.action === 'conflict');
  check('期間が読めない既存設定 → error (上書きしない)', by.broken.action === 'error');
  check('GET失敗 (status 0) → error で理由つき', by.neterr.action === 'error' && by.neterr.reason.includes('timeout'), by.neterr.reason);
  check('error/conflict が混ざっても set 対象は影響されない', by.aromaorb.action === 'set' && by.aromasolo.action === 'set');

  const plansOw = planPointCampaigns({ rows, currentByMn, period, nowIso, overwriteActive: true });
  check('--overwrite なら有効中でも set', plansOw.find((p) => p.manageNumber === 'manual').action === 'set');
  check('--overwrite でも期間不正は error のまま', plansOw.find((p) => p.manageNumber === 'broken').action === 'error');
}

console.log(`\n結果: ${passed} passed / ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
