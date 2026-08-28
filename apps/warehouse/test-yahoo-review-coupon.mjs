#!/usr/bin/env node
/** test-yahoo-review-coupon.mjs — PR-Y-C3 台帳と期間計算のスモーク。実行: node apps/warehouse/test-yahoo-review-coupon.mjs */
import Database from 'better-sqlite3';
import {
  ensureYahooCouponLedger, monthlyCouponPeriod, makeOperationId, couponDescription, isValidCouponUrl,
  reserveMonth, markSubmitting, markIssued, markReconcileRequired, escalateStale, usableCouponFor, getCouponRow, isValidMonth, findByOperationId,
  couponUrlMatchesId, isUsableCopySource,
} from './yahoo-review-coupon-lib.js';

let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };
const db = new Database(':memory:');
ensureYahooCouponLedger(db); ensureYahooCouponLedger(db);

console.log('=== 1. 期間計算 (vendor と同じ 月初〜翌月末) ===');
{
  const p = monthlyCouponPeriod('2026-09', '2026-08-28T00:00:00.000Z');
  check('翌月分を月初〜翌月末で', p.startYmd === '2026/09/01' && p.endYmd === '2026/10/31' && p.spanDays === 60 && p.startHour === '00' && p.endHour === '23', JSON.stringify(p));
  const cur = monthlyCouponPeriod('2026-08', '2026-08-28T00:00:00.000Z'); // JST 8/28
  check('当月の途中なら開始は当日 (過去日は不可)', cur.startYmd === '2026/08/28' && cur.endYmd === '2026/09/30', JSON.stringify(cur));
  const dec = monthlyCouponPeriod('2026-12', '2026-12-01T00:00:00.000Z');
  check('年跨ぎ (12月→翌年1月末)', dec.startYmd === '2026/12/01' && dec.endYmd === '2027/01/31', JSON.stringify(dec));
  const feb = monthlyCouponPeriod('2028-02', '2028-02-01T00:00:00.000Z');
  check('閏年の月末 (2月→3/31)', feb.endYmd === '2028/03/31');
  let past = false, badMonth = false;
  try { monthlyCouponPeriod('2026-07', '2026-08-28T00:00:00.000Z'); } catch { past = true; }
  try { monthlyCouponPeriod('2026-13', '2026-08-28T00:00:00.000Z'); } catch { badMonth = true; }
  check('過去月・不正な月は拒否', past && badMonth && isValidMonth('2026-09') && !isValidMonth('2026-00'));
  check('90日制約に収まる (どの月でも)', [1,2,3,4,5,6,7,8,9,10,11,12].every((m) => {
    const mm = `2027-${String(m).padStart(2, '0')}`;
    return monthlyCouponPeriod(mm, `2027-${String(m).padStart(2, '0')}-01T00:00:00.000Z`).spanDays <= 90;
  }));
}

console.log('=== 2. op-id / URL 検証 ===');
{
  const op = makeOperationId('2026-09', 'A1B2C3');
  check('op-id は英数と-のみ', op === 'RVW-202609-A1B2C3' && /^[A-Z0-9-]+$/.test(op));
  check('毎回変わる', makeOperationId('2026-09') !== makeOperationId('2026-09'));
  check('説明文に op-id が入る', couponDescription(op).includes(op) && couponDescription(op).includes('レビュー'));
  check('一覧から op-id で探せる', findByOperationId([{ title: 'x', description: `… ${op}）` }, { title: 'y', description: 'z' }], op).length === 1);
  check('URL 検証: 正 (実測形式)', isValidCouponUrl('https://shopping.yahoo.co.jp/coupon/interior/ZTM3NjEyYzBkOWVhZTgwNzZmMDhmY2ZiOWNm'));
  check('URL 検証: 誤 (http / 他ドメイン / 短すぎ / パス違い)',
    !isValidCouponUrl('http://shopping.yahoo.co.jp/coupon/interior/ZTM3NjEyYzBkOWVhZTgwNzZmMDhmY2ZiOWNm')
    && !isValidCouponUrl('https://evil.example.com/coupon/interior/ZTM3NjEyYzBkOWVhZTgwNzZmMDhmY2ZiOWNm')
    && !isValidCouponUrl('https://shopping.yahoo.co.jp/coupon/interior/abc')
    && !isValidCouponUrl('https://shopping.yahoo.co.jp/my/coupon/'));
}

console.log('=== 3. 状態機械 (二重発行より未発行を選ぶ) ===');
{
  const NOW = '2026-08-28T05:00:00.000Z';
  const period = monthlyCouponPeriod('2026-09', NOW);
  const op = makeOperationId('2026-09', 'ZZZ111');
  check('予約 (planned)', reserveMonth(db, { month: '2026-09', period, operationId: op, nowIso: NOW })?.operationId === op
    && getCouponRow(db, '2026-09').status === 'planned');
  check('二重予約は null (既に行がある)', reserveMonth(db, { month: '2026-09', period, operationId: makeOperationId('2026-09'), nowIso: NOW }) === null);
  check('planned → submitting', markSubmitting(db, '2026-09', NOW) === true && getCouponRow(db, '2026-09').status === 'submitting');
  check('submitting からの再 submitting は不可 (作成を二度走らせない)', markSubmitting(db, '2026-09', NOW) === false);
  check('結果不明 → reconcile_required', markReconcileRequired(db, { month: '2026-09', note: '一覧に0件', nowIso: NOW }) === true
    && getCouponRow(db, '2026-09').status === 'reconcile_required');
  check('URL とクーポンID の一致を要求', couponUrlMatchesId('https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', 'ABCDEF0123456789ABCD')
    && !couponUrlMatchesId('https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', 'OTHERID0123456789ABC'));
  let mismatch = false;
  try { markIssued(db, { month: '2026-09', couponId: 'OTHERID0123456789ABC', couponUrl: 'https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', nowIso: NOW }); } catch { mismatch = true; }
  check('URL とIDが食い違えば issued にしない', mismatch && getCouponRow(db, '2026-09').status === 'reconcile_required');
  check('コピー元は定率5%のものだけ', isUsableCopySource({ discountType: '2', discountRatio: '5' })
    && !isUsableCopySource({ discountType: '1', discountRatio: '5' }) && !isUsableCopySource({ discountType: '2', discountRatio: '10' }) && !isUsableCopySource(null));
  check('reconcile_required からも issued にできる (後から見つかった)',
    markIssued(db, { month: '2026-09', couponId: 'ABCDEF0123456789ABCD', couponUrl: 'https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', nowIso: NOW }) === true
    && getCouponRow(db, '2026-09').status === 'issued');
  check('issued からは submitting に戻らない', markSubmitting(db, '2026-09', NOW) === false && markReconcileRequired(db, { month: '2026-09', note: 'x', nowIso: NOW }) === false);
  let badUrl = false;
  try { markIssued(db, { month: '2026-09', couponId: 'x', couponUrl: 'https://evil.example.com/x', nowIso: NOW }); } catch { badUrl = true; }
  check('不正な URL は issued にしない', badUrl);

  // 24時間の据え置き → 人手へ
  const p10 = monthlyCouponPeriod('2026-10', '2026-10-01T00:00:00.000Z');
  reserveMonth(db, { month: '2026-10', period: p10, operationId: makeOperationId('2026-10', 'AAA'), nowIso: '2026-10-01T00:00:00.000Z' });
  markSubmitting(db, '2026-10', '2026-10-01T00:00:00.000Z');
  markReconcileRequired(db, { month: '2026-10', note: '応答不明', nowIso: '2026-10-01T00:00:00.000Z' });
  check('24時間以内は据え置き', escalateStale(db, { nowIso: '2026-10-01T12:00:00.000Z' }).length === 0);
  // 毎日の照合で再マークされても、最初に reconcile_required になった時刻を基準にする (Codex Y-C3 R1 High)
  markReconcileRequired(db, { month: '2026-10', note: '2日目も見つからない', nowIso: '2026-10-02T00:00:00.000Z' });
  check('再マークしても reconcile_since は動かない', getCouponRow(db, '2026-10').reconcile_since === '2026-10-01T00:00:00.000Z');
  check('24時間超で manual_intervention', escalateStale(db, { nowIso: '2026-10-03T00:00:00.000Z' }).join() === '2026-10'
    && getCouponRow(db, '2026-10').status === 'manual_intervention');
}

console.log('=== 4. 送信側が使えるクーポンの判定 ===');
{
  check('期間内の issued を返す', usableCouponFor(db, '2026-09-15T03:00:00.000Z')?.month === '2026-09');
  check('開始前は返さない (2026-09-01 00:00 より前)', usableCouponFor(db, '2026-08-31T14:00:00.000Z') === null);
  check('終了後は返さない (2026-10-31 23:59 より後)', usableCouponFor(db, '2026-11-01T00:00:00.000Z') === null);
  check('issued 以外は返さない', usableCouponFor(db, '2026-10-02T03:00:00.000Z')?.month === '2026-09');
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed > 0 ? 1 : 0);
