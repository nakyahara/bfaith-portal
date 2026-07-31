#!/usr/bin/env node
/**
 * test-rakuten-store-coupon.mjs — 楽天ストアクーポン入れ替えの純ロジック検証 (ネットワーク不使用)
 * 実行: node apps/warehouse/test-rakuten-store-coupon.mjs
 */
import {
  STORE_COUPON_DEFS, STORE_COUPON_FIXED, nextAmount, renderCouponName, monthEndYmd, addDays, diffDays,
  isoToYmd, todayJst, computeNextPeriod, validatePeriod, pickSourceCoupon, findExistingForStart,
  planStoreCoupon, planToIssueParams,
} from './rakuten-store-coupon-lib.js';
import { buildIssueXml } from './rakuten-coupon-lib.js';

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { console.log(`  ✅ ${name}`); passed++; }
  else { console.log(`  ❌ ${name} ${detail}`); failed++; }
}

// 実行時刻を固定して決定的にする (2026-07-31 19:30 JST)
const NOW = Date.parse('2026-07-31T19:30:00+09:00');
const TODAY = '2026-07-31';

/** 現行4本 (2026-07-31 実測。3点はクーポン名と実額がズレている) */
const CURRENT = [
  { couponCode: 'AAAA-BBBB-CCCC-0003', couponName: '3点以上ご購入で100円引きクーポン', couponStartDate: '2026-05-01T00:00:00+09:00', couponEndDate: '2026-07-31T23:59:59+09:00', itemType: 4, discountType: 1, discountFactor: 110, displayFlag: 1, combineFlag: 0, issueCount: 0, memberAvailMaxCount: 0, orderCountCond: 3 },
  { couponCode: 'AAAA-BBBB-CCCC-0004', couponName: '4点以上ご購入で155円引きクーポン', couponStartDate: '2026-05-01T00:00:00+09:00', couponEndDate: '2026-07-31T23:59:59+09:00', itemType: 4, discountType: 1, discountFactor: 155, displayFlag: 1, combineFlag: 0, issueCount: 0, memberAvailMaxCount: 0, orderCountCond: 4 },
  { couponCode: 'AAAA-BBBB-CCCC-0005', couponName: '5点以上ご購入で210円引きクーポン', couponStartDate: '2026-05-01T00:00:00+09:00', couponEndDate: '2026-07-31T23:59:59+09:00', itemType: 4, discountType: 1, discountFactor: 210, displayFlag: 1, combineFlag: 0, issueCount: 0, memberAvailMaxCount: 0, orderCountCond: 5 },
  { couponCode: 'AAAA-BBBB-CCCC-0006', couponName: '6点以上ご購入で260円引きクーポン', couponStartDate: '2026-05-01T00:00:00+09:00', couponEndDate: '2026-07-31T23:59:59+09:00', itemType: 4, discountType: 1, discountFactor: 260, displayFlag: 1, combineFlag: 0, issueCount: 0, memberAvailMaxCount: 0, orderCountCond: 6 },
  // 別種のクーポン (定率5%・非公開) は対象外であること
  { couponCode: 'ZZZZ-YYYY-XXXX-0000', couponName: '次回購入に使える5％割引クーポン', couponStartDate: '2026-07-01T00:00:00+09:00', couponEndDate: '2026-09-30T23:59:59+09:00', itemType: 4, discountType: 2, discountFactor: 5, displayFlag: 0, combineFlag: 0, issueCount: 0, memberAvailMaxCount: 1, orderCountCond: null },
];

console.log('=== 1. 日付ユーティリティ ===');
{
  check('monthEndYmd: 8/1 の2ヶ月後の月末 = 10/31', monthEndYmd('2026-08-01', 2) === '2026-10-31', monthEndYmd('2026-08-01', 2));
  check('monthEndYmd: 12月をまたぐ (11/1 +2 → 2027-01-31)', monthEndYmd('2026-11-01', 2) === '2027-01-31', monthEndYmd('2026-11-01', 2));
  check('monthEndYmd: うるう年 (2027-12-01 +2 → 2028-02-29)', monthEndYmd('2027-12-01', 2) === '2028-02-29', monthEndYmd('2027-12-01', 2));
  check('addDays: 7/31 +1 = 8/1', addDays('2026-07-31', 1) === '2026-08-01');
  check('diffDays: 8/1〜10/31 = 91日', diffDays('2026-08-01', '2026-10-31') === 91, String(diffDays('2026-08-01', '2026-10-31')));
  check('isoToYmd', isoToYmd('2026-07-31T23:59:59+09:00') === '2026-07-31');
  check('todayJst: UTC 15:00 は翌日JST', todayJst(Date.parse('2026-07-31T15:00:00Z')) === '2026-08-01');
}

console.log('=== 2. 金額の巡回 (±10円の2値往復) ===');
{
  check('110 → 100', nextAmount([100, 110], 110).amount === 100);
  check('100 → 110', nextAmount([100, 110], 100).amount === 110);
  check('リスト外の155 → 150 にリセット', nextAmount([150, 160], 155).amount === 150 && nextAmount([150, 160], 155).reset === true);
  check('リストにある値では reset しない', nextAmount([150, 160], 150).reset === false);
  check('空リストは null', nextAmount([], 100).amount === null);
  check('クーポン名は実額から生成', renderCouponName({ orderCount: 3, amount: 110 }) === '3点以上ご購入で110円引きクーポン');
}

console.log('=== 3. 次期間の決定 (月初〜3ヶ月後の月末) ===');
{
  const p = computeNextPeriod({ prevEndYmd: '2026-07-31', todayYmd: TODAY, nowMs: NOW });
  check('開始 = 前回終了の翌日 00:00', p.startIso === '2026-08-01T00:00:00+09:00', p.startIso);
  check('終了 = 3ヶ月後の月末 23:59:59', p.endIso === '2026-10-31T23:59:59+09:00', p.endIso);
  check('91日 (現行 5/01〜7/31 と同じ形)', p.spanDays === 91, String(p.spanDays));
  check('遅延フラグなし', p.delayed === false);
  check('切り詰めなし', p.truncated === false);

  // 上限を90日に締めた場合は終了日を切り詰める
  const p90 = computeNextPeriod({ prevEndYmd: '2026-07-31', todayYmd: TODAY, nowMs: NOW, maxSpanDays: 90 });
  check('上限90日なら 10/30 に切り詰め', p90.endIso === '2026-10-30T23:59:59+09:00' && p90.truncated === true, p90.endIso);

  // 実行が遅れて開始日が過去になった場合
  const late = computeNextPeriod({ prevEndYmd: '2026-07-25', todayYmd: TODAY, nowMs: NOW });
  check('遅延時は今から90分後に繰り上げ', late.startIso === '2026-07-31T21:00:00+09:00', late.startIso);
  check('遅延フラグが立つ', late.delayed === true);
  check('遅延時の終了は開始月+2ヶ月の月末', late.endIso === '2026-09-30T23:59:59+09:00', late.endIso);
}

console.log('=== 4. 期間の検証 (楽天制約) ===');
{
  check('正常', validatePeriod({ startIso: '2026-08-01T00:00:00+09:00', endIso: '2026-10-31T23:59:59+09:00', nowMs: NOW }).length === 0);
  const soon = validatePeriod({ startIso: '2026-07-31T20:00:00+09:00', endIso: '2026-10-31T23:59:59+09:00', nowMs: NOW });
  check('開始が60分未満先ならNG', soon.some((e) => /60分/.test(e)), JSON.stringify(soon));
  const far = validatePeriod({ startIso: '2026-09-30T00:00:00+09:00', endIso: '2026-12-31T23:59:59+09:00', nowMs: NOW });
  check('開始が30日超先ならNG', far.some((e) => /30日/.test(e)), JSON.stringify(far));
  const long = validatePeriod({ startIso: '2026-08-01T00:00:00+09:00', endIso: '2026-11-30T23:59:59+09:00', nowMs: NOW });
  check('期間が上限超ならNG', long.some((e) => /超過/.test(e)), JSON.stringify(long));
  const rev = validatePeriod({ startIso: '2026-08-01T00:00:00+09:00', endIso: '2026-07-01T23:59:59+09:00', nowMs: NOW });
  check('終了が開始以前ならNG', rev.length > 0);
}

console.log('=== 5. 現況の読み取り (名前ではなく設定値を正とする) ===');
{
  const src = pickSourceCoupon(CURRENT, 3);
  check('3点のコピー元を特定', src && src.couponCode === 'AAAA-BBBB-CCCC-0003');
  check('金額は名前(100円)ではなく実額110円を採用', src.discountFactor === 110);
  check('定率クーポンは拾わない', pickSourceCoupon(CURRENT, null) === null);
  check('存在しない点数は null', pickSourceCoupon(CURRENT, 7) === null);
  check('二重発行の検出: 同じ開始日があれば見つかる', !!findExistingForStart(CURRENT, 3, '2026-05-01'));
  check('二重発行の検出: 別の開始日なら見つからない', findExistingForStart(CURRENT, 3, '2026-08-01') === null);

  // 新旧が混在していても、終了日が最も遅いものを選ぶ
  const withOld = [...CURRENT, {
    couponCode: 'OLD0-0000-0000-0003', couponName: '3点以上ご購入で100円引きクーポン', couponStartDate: '2026-02-01T00:00:00+09:00',
    couponEndDate: '2026-04-30T23:59:59+09:00', itemType: 4, discountType: 1, discountFactor: 100, displayFlag: 1, orderCountCond: 3,
  }];
  check('最新 (終了日が最も遅い) を選ぶ', pickSourceCoupon(withOld, 3).couponCode === 'AAAA-BBBB-CCCC-0003');
}

console.log('=== 6. 入れ替え計画 ===');
{
  const plans = STORE_COUPON_DEFS.map((def) => planStoreCoupon({ def, coupons: CURRENT, todayYmd: TODAY, nowMs: NOW, leadDays: 7 }));
  check('4本すべて ready', plans.every((p) => p.status === 'ready'), JSON.stringify(plans.map((p) => `${p.key}:${p.status}:${p.reason || ''}`)));
  const [q3, q4, q5, q6] = plans;
  check('3点: 110 → 100', q3.amount === 100 && q3.prevAmount === 110);
  check('3点: 名前が実額に一致', q3.couponName === '3点以上ご購入で100円引きクーポン', q3.couponName);
  check('4点: 155 (リスト外) → 150 にリセット', q4.amount === 150 && q4.amountReset === true);
  check('5点: 210 → 200', q5.amount === 200);
  check('6点: 260 → 250', q6.amount === 250);
  check('期間は全本 8/01〜10/31', plans.every((p) => p.period.startYmd === '2026-08-01' && p.period.endYmd === '2026-10-31'));

  // 期限までまだ日があるうちは作らない
  const early = planStoreCoupon({ def: STORE_COUPON_DEFS[0], coupons: CURRENT, todayYmd: '2026-07-01', nowMs: Date.parse('2026-07-01T09:30:00+09:00'), leadDays: 7 });
  check('30日前は skip (まだ早い)', early.status === 'skip' && /まだ早い/.test(early.reason), `${early.status}:${early.reason}`);

  // 次期間ぶんが作成済みなら何もしない
  const issued = [...CURRENT, {
    couponCode: 'NEW0-0000-0000-0003', couponName: '3点以上ご購入で100円引きクーポン', couponStartDate: '2026-08-01T00:00:00+09:00',
    couponEndDate: '2026-10-31T23:59:59+09:00', itemType: 4, discountType: 1, discountFactor: 100, displayFlag: 1, orderCountCond: 3,
  }];
  const dup = planStoreCoupon({ def: STORE_COUPON_DEFS[0], coupons: issued, todayYmd: TODAY, nowMs: NOW, leadDays: 7 });
  check('作成済みなら skip (二重発行しない)', dup.status === 'skip' && /作成済み/.test(dup.reason), `${dup.status}:${dup.reason}`);

  // コピー元が無い場合
  const none = planStoreCoupon({ def: { key: 'qty9', orderCount: 9, cycle: [900] }, coupons: CURRENT, todayYmd: TODAY, nowMs: NOW });
  check('コピー元が無ければ error', none.status === 'error');
}

console.log('=== 7. 発行XML (otherConditions RS004) ===');
{
  const plan = planStoreCoupon({ def: STORE_COUPON_DEFS[0], coupons: CURRENT, todayYmd: TODAY, nowMs: NOW, leadDays: 7 });
  const params = planToIssueParams(plan);
  check('固定値: 受注・定額・公開・併用不可', params.itemType === 4 && params.discountType === 1
    && params.displayFlag === 1 && params.combineFlag === 0);
  const r = buildIssueXml(params);
  check('XML生成できる', r.ok, JSON.stringify(r.errors));
  check('RS004 に点数が入る', /<conditionTypeCode>RS004<\/conditionTypeCode>\s*<startValue>3<\/startValue>/.test(r.xml));
  check('金額が discountFactor に入る', /<discountFactor>100<\/discountFactor>/.test(r.xml));
  check('otherConditions は displayFlag の後 (.NET公式クライアントの宣言順)',
    r.xml.indexOf('<otherConditions>') > r.xml.indexOf('<displayFlag>'));
  // 既存の月次クーポン (RS004なし) は otherConditions を出さない
  const noCond = buildIssueXml({ ...params, orderCountCond: undefined });
  check('orderCountCond 未指定なら otherConditions を出さない', noCond.ok && !noCond.xml.includes('otherConditions'));
  // 定額の金額検証
  check('定額で金額0はNG', buildIssueXml({ ...params, discountFactor: 0 }).ok === false);
  check('利用個数0はNG', buildIssueXml({ ...params, orderCountCond: 0 }).ok === false);
  check('利用個数が小数はNG', buildIssueXml({ ...params, orderCountCond: 1.5 }).ok === false);
  // 要素順 (既存の発行成功実績と同じ並び + 末尾 otherConditions)
  const order = ['couponName', 'couponCaption', 'couponStartDate', 'couponEndDate', 'issueCount',
    'itemType', 'discountType', 'discountFactor', 'memberAvailMaxCount', 'purchaseHistoryCond',
    'multiRankCond', 'genderCond', 'ageRangeCond', 'birthmonthCond', 'multiPrefectureCond',
    'combineFlag', 'displayFlag', 'otherConditions'];
  let lastIdx = -1, orderOk = true;
  for (const el of order) {
    const idx = r.xml.indexOf(`<${el}>`);
    if (idx < 0 || idx < lastIdx) { orderOk = false; break; }
    lastIdx = idx;
  }
  check('要素順が公式サンプルどおり', orderOk);
}

console.log(`\n=== 結果: ${passed} passed / ${failed} failed ===`);
process.exit(failed === 0 ? 0 : 1);
