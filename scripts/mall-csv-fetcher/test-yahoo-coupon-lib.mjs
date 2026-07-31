/**
 * yahoo-coupon-lib.mjs のスモークテスト (依存なし・オフラインで完結)
 * 実行: node scripts/mall-csv-fetcher/test-yahoo-coupon-lib.mjs
 */
import {
  parseCouponTitle, nextAmount, addDays, diffDays, todayJst,
  computeNextPeriod, validatePeriod, renderTemplate, pickSourceRow,
  findExistingForPeriod, planCoupon, ymdToMs, msToYmd,
} from './yahoo-coupon-lib.mjs';

let pass = 0;
const fails = [];
function eq(actual, expected, label) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { pass++; } else { fails.push(`${label}: 期待 ${e} / 実際 ${a}`); }
}
function ok(cond, label) { if (cond) pass++; else fails.push(label); }

// ── クーポン名のパース ──
eq(parseCouponTitle('2点以上購入で55円割引'), { orderCount: 2, amount: 55 }, 'parse 2点55円');
eq(parseCouponTitle('4点以上購入で155円割引'), { orderCount: 4, amount: 155 }, 'parse 4点155円');
eq(parseCouponTitle('雑貨イズムYahoo!ショッピング店で次回購入に使える5％割引クーポン'), null, 'parse 5%クーポンは対象外');
eq(parseCouponTitle(''), null, 'parse 空文字');

// ── 金額の巡回 (55→50→60→55) ──
eq(nextAmount([55, 50, 60], 55), 50, 'cycle 55→50');
eq(nextAmount([55, 50, 60], 50), 60, 'cycle 50→60');
eq(nextAmount([55, 50, 60], 60), 55, 'cycle 60→55 (一周)');
eq(nextAmount([55, 50, 60], 999), null, 'cycle 未知の金額は null (安全側)');
eq(nextAmount([110, 105, 115], 110), 105, 'cycle 110→105');

// ── 日付ユーティリティ (UTC計算・JSTローカルに依存しない) ──
eq(addDays('2026/07/31', 1), '2026/08/01', 'addDays 月跨ぎ');
eq(addDays('2026/12/31', 1), '2027/01/01', 'addDays 年跨ぎ');
eq(addDays('2026/02/28', 1), '2026/03/01', 'addDays 2026は平年');
eq(diffDays('2026/05/03', '2026/07/31'), 89, 'diffDays 現行運用は89日');
eq(ymdToMs('2026/02/31'), null, 'ymdToMs 実在しない日は null');
eq(msToYmd(ymdToMs('2026/08/01')), '2026/08/01', 'ymd往復');
// JST変換: UTC 2026-07-31T15:30Z は JST 8/1 00:30 → 暦日は 8/1
eq(todayJst(Date.UTC(2026, 6, 31, 15, 30)), '2026/08/01', 'todayJst UTC15:30→JST翌日');
eq(todayJst(Date.UTC(2026, 6, 31, 14, 30)), '2026/07/31', 'todayJst UTC14:30→JST同日');

// ── 次期間の計算 ──
eq(computeNextPeriod('2026/07/31', 89), { startYmd: '2026/08/01', endYmd: '2026/10/29', delayed: false }, '次期間 8/1〜10/29');
eq(computeNextPeriod('2026/04/30', 89), { startYmd: '2026/05/01', endYmd: '2026/07/29', delayed: false }, '次期間 5/1起点');
eq(computeNextPeriod('2026/07/31', 89, '2026/07/25'), { startYmd: '2026/08/01', endYmd: '2026/10/29', delayed: false }, '期限前の実行はそのまま連続');
// 実行が遅れて開始日が過去になったら「明日から」に繰り上げる (空白は出るが復活はする)
eq(computeNextPeriod('2026/07/31', 89, '2026/08/05'), { startYmd: '2026/08/06', endYmd: '2026/11/03', delayed: true }, '遅延時は明日始まりへ繰り上げ');

// ── Yahoo制約の検証 ──
eq(validatePeriod({ startYmd: '2026/08/01', endYmd: '2026/10/29', todayYmd: '2026/07/31' }), [], '89日+前日登録=合格');
ok(validatePeriod({ startYmd: '2026/08/01', endYmd: '2026/10/31', todayYmd: '2026/07/31' })
  .some((e) => e.includes('90日')), '91日は90日制約で弾く');
ok(validatePeriod({ startYmd: '2026/07/30', endYmd: '2026/10/26', todayYmd: '2026/07/31' })
  .some((e) => e.includes('過去')), '開始が過去なら弾く');
ok(validatePeriod({ startYmd: '2026/10/31', endYmd: '2026/12/29', todayYmd: '2026/07/31' })
  .some((e) => e.includes('60日')), '開始が61日以上先なら弾く');

// ── 文言生成 ──
eq(renderTemplate('{orderCount}点以上購入で{amount}円割引', { orderCount: 2, amount: 50 }),
  '2点以上購入で50円割引', 'タイトル生成');
eq(renderTemplate('当店で{orderCount}点以上ご購入された場合、{amount}円引きになるクーポンです。', { orderCount: 3, amount: 105 }),
  '当店で3点以上ご購入された場合、105円引きになるクーポンです。', '説明文生成 (実額に追従)');

// ── コピー元の選択 ──
const ROWS = [
  { couponId: 'A', title: '2点以上購入で55円割引', useStartYmd: '2026/05/03', useEndYmd: '2026/07/31', state: '公開中' },
  { couponId: 'B', title: '2点以上購入で50円割引', useStartYmd: '2026/02/01', useEndYmd: '2026/05/02', state: '公開終了' },
  { couponId: 'C', title: '3点以上購入で110円割引', useStartYmd: '2026/05/03', useEndYmd: '2026/07/31', state: '公開中' },
  { couponId: 'D', title: '2点以上購入で60円割引', useStartYmd: '2026/08/01', useEndYmd: '2026/10/29', state: '削除済み' },
  { couponId: 'E', title: '雑貨イズムで使える5％割引クーポン', useStartYmd: '2026/07/01', useEndYmd: '2026/08/31', state: '公開中' },
];
eq(pickSourceRow(ROWS, 2).couponId, 'A', '最新(終了日が最も遅い)を選ぶ・削除済みは除外');
eq(pickSourceRow(ROWS, 3).couponId, 'C', '3点は C');
eq(pickSourceRow(ROWS, 9), null, '該当なしは null');
eq(pickSourceRow(ROWS, 2).amount, 55, 'コピー元の金額も返す');

// ── 二重作成の防止 ──
const ROWS2 = [...ROWS, { couponId: 'F', title: '2点以上購入で50円割引', useStartYmd: '2026/08/01', useEndYmd: '2026/10/29', state: '公開前' }];
ok(findExistingForPeriod(ROWS2, 2, '2026/08/01') !== null, '同じ開始日が既にあれば検出');
ok(findExistingForPeriod(ROWS, 2, '2026/08/01') === null, '削除済みは既存とみなさない');

// ── 計画の組み立て ──
const DEF2 = {
  key: 'qty2', orderCount: 2, cycle: [55, 50],
  images: { 50: 'coupon50.jpg', 55: 'coupon55.jpg' },
  titleTemplate: '{orderCount}点以上購入で{amount}円割引',
  descriptionTemplate: '当店で{orderCount}点以上ご購入された場合、{amount}円引きになるクーポンです。',
};
const p1 = planCoupon({ def: DEF2, rows: ROWS, periodDays: 89, todayYmd: '2026/07/31' });
eq(p1.status, 'ready', 'plan: ready');
eq(p1.amount, 50, 'plan: 55の次は50');
eq(p1.title, '2点以上購入で50円割引', 'plan: タイトル');
eq(p1.description, '当店で2点以上ご購入された場合、50円引きになるクーポンです。', 'plan: 説明文');
eq(p1.period, { startYmd: '2026/08/01', endYmd: '2026/10/29', delayed: false }, 'plan: 期間');
eq(p1.source.couponId, 'A', 'plan: コピー元');
eq(p1.image, 'coupon50.jpg', 'plan: 金額に対応する画像を選ぶ');

// 金額と画像は1対1。対応画像が無い金額では発行させない (ズレの構造的防止)
const DEF_NOIMG = { ...DEF2, images: { 55: 'coupon55.jpg' } }; // 50円ぶんの画像が無い
const p8 = planCoupon({ def: DEF_NOIMG, rows: ROWS, periodDays: 89, todayYmd: '2026/07/31' });
eq(p8.status, 'error', 'plan: 画像未登録の金額はerror');
ok(/画像が未登録/.test(p8.reason), 'plan: error理由が画像未登録');

const p2 = planCoupon({ def: DEF2, rows: ROWS2, periodDays: 89, todayYmd: '2026/07/31' });
eq(p2.status, 'skip', 'plan: 次期間ぶんが作成済みならskip');
ok(/作成済み/.test(p2.reason), 'plan: skip理由が「作成済み」');

// 期限までまだ日があるとき = 異常ではなく「まだ早い」でskip (毎日実行を想定)
const ROWS_EARLY = [{ couponId: 'G', title: '2点以上購入で55円割引', useStartYmd: '2026/05/03', useEndYmd: '2026/07/31', state: '公開中' }];
const p6 = planCoupon({ def: DEF2, rows: ROWS_EARLY, periodDays: 89, todayYmd: '2026/05/10' });
eq(p6.status, 'skip', 'plan: 期限まで余裕があればskip (エラーにしない)');
ok(/まだ早い/.test(p6.reason), 'plan: skip理由が「まだ早い」');

// リードタイム境界: 8日前はまだ作らない / 7日前になったら作る
eq(planCoupon({ def: DEF2, rows: ROWS_EARLY, periodDays: 89, todayYmd: '2026/07/24' }).status, 'skip',
  'plan: 8日前はまだ作らない');
eq(planCoupon({ def: DEF2, rows: ROWS_EARLY, periodDays: 89, todayYmd: '2026/07/25' }).status, 'ready',
  'plan: 7日前になったら作成対象');
eq(planCoupon({ def: DEF2, rows: ROWS_EARLY, periodDays: 89, todayYmd: '2026/07/31' }).status, 'ready',
  'plan: 前日でも作成対象');
eq(planCoupon({ def: DEF2, rows: ROWS_EARLY, periodDays: 89, todayYmd: '2026/06/10', leadDays: 60 }).status, 'ready',
  'plan: leadDaysを広げれば早めにも作れる');

// 実行が遅れて期限切れ後に回った場合も、明日始まりで復活させる
const p9 = planCoupon({ def: DEF2, rows: ROWS_EARLY, periodDays: 89, todayYmd: '2026/08/05' });
eq(p9.status, 'ready', 'plan: 期限切れ後でも作成する (空白を最小化)');
eq(p9.period.startYmd, '2026/08/06', 'plan: 遅延時は明日始まり');
ok(p9.period.delayed === true, 'plan: 遅延フラグが立つ');

const p3 = planCoupon({ def: DEF2, rows: [], periodDays: 89, todayYmd: '2026/07/31' });
eq(p3.status, 'error', 'plan: コピー元なしはerror');

const ROWS_ODD = [{ couponId: 'X', title: '2点以上購入で77円割引', useStartYmd: '2026/05/03', useEndYmd: '2026/07/31', state: '公開中' }];
const p4 = planCoupon({ def: DEF2, rows: ROWS_ODD, periodDays: 89, todayYmd: '2026/07/31' });
eq(p4.status, 'error', 'plan: 巡回リスト外の金額はerror (勝手に決めない)');

// 期間が90日制約に触れる設定は計画段階で止める
const p5 = planCoupon({ def: DEF2, rows: ROWS, periodDays: 91, todayYmd: '2026/07/31' });
eq(p5.status, 'error', 'plan: 91日指定はerror');

console.log(`\n${fails.length ? '❌' : '✅'} ${pass} passed, ${fails.length} failed`);
for (const f of fails) console.log(`  - ${f}`);
process.exitCode = fails.length ? 1 : 0;
