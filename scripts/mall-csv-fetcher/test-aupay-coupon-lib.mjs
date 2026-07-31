/**
 * aupay-coupon-lib.mjs のスモークテスト (依存なし・オフラインで完結)
 * 実行: node scripts/mall-csv-fetcher/test-aupay-coupon-lib.mjs
 */
import {
  parseCouponTitle, parseDiscountAmount, parseUsePeriod, nextAmount,
  addDays, diffDays, todayJst, ymdToMs, msToYmd,
  firstDayOfMonthAfter, lastDayOfMonth, computeNextMonthPeriod,
  renderTemplate, pickSourceRow, findExistingForMonth, findCreatedRow, hasActiveCoupon, planCoupon,
  checkListCoverage, findUnparsedInMonth,
} from './aupay-coupon-lib.mjs';

let pass = 0;
const fails = [];
function eq(actual, expected, label) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { pass++; } else { fails.push(`${label}: 期待 ${e} / 実際 ${a}`); }
}
function ok(cond, label) { if (cond) pass++; else fails.push(label); }

// ── クーポン名のパース (実データの表記) ──
eq(parseCouponTitle('ZACCA IZMで使える！商品2点以上で30円引きクーポン！'), { orderCount: 2, amount: 30 }, 'parse 2点30円');
eq(parseCouponTitle('ZACCA IZMで使える！商品5点以上で155円引きクーポン！ 獲得用URLコピー'), { orderCount: 5, amount: 155 }, 'parse 一覧の獲得用URLコピー付き');
eq(parseCouponTitle('ZACCA IZMで使える5％割引クーポン'), null, 'parse ％クーポンは対象外');
eq(parseCouponTitle(''), null, 'parse 空文字');

// ── 割引種別列 ──
eq(parseDiscountAmount('金額指定 30 円OFF'), 30, 'discount 金額指定30');
eq(parseDiscountAmount('金額指定 1,000 円OFF'), 1000, 'discount カンマ区切り');
eq(parseDiscountAmount('％指定 5 ％OFF'), null, 'discount ％指定は null');

// ── 利用期間列 ──
eq(parseUsePeriod('2026-07-02 ~ 2026-08-01'), { startYmd: '2026-07-02', endYmd: '2026-08-01' }, 'period 半角チルダ');
eq(parseUsePeriod('2026-07-02 〜 2026-08-01'), { startYmd: '2026-07-02', endYmd: '2026-08-01' }, 'period 全角チルダ');
eq(parseUsePeriod('未設定'), null, 'period パース不能は null');

// ── 金額の巡回 (2値の交互) ──
eq(nextAmount([30, 33], 30), 33, 'cycle 30→33');
eq(nextAmount([30, 33], 33), 30, 'cycle 33→30 (一周)');
eq(nextAmount([150, 155], 160), null, 'cycle 未知の金額は null (安全側)');

// ── 日付ユーティリティ (UTC計算・JSTローカルに依存しない) ──
eq(addDays('2026-07-31', 1), '2026-08-01', 'addDays 月跨ぎ');
eq(addDays('2026-12-31', 1), '2027-01-01', 'addDays 年跨ぎ');
eq(ymdToMs('2026-02-31'), null, 'ymdToMs 実在しない日は null');
eq(msToYmd(ymdToMs('2026-08-01')), '2026-08-01', 'ymd往復');
eq(diffDays('2026-08-01', '2026-09-01'), 31, 'diffDays 8月は31日');
// JST変換: UTC 2026-07-31T15:30Z は JST 8/1 00:30 → 暦日は 8/1
eq(todayJst(Date.UTC(2026, 6, 31, 15, 30)), '2026-08-01', 'todayJst UTC15:30はJST翌日');
eq(todayJst(Date.UTC(2026, 6, 31, 14, 30)), '2026-07-31', 'todayJst UTC14:30は同日');

// ── 月境界 ──
eq(firstDayOfMonthAfter('2026-07-25', 1), '2026-08-01', '翌月1日');
eq(firstDayOfMonthAfter('2026-12-25', 1), '2027-01-01', '翌月1日 年跨ぎ');
eq(firstDayOfMonthAfter('2026-11-25', 2), '2027-01-01', '翌々月1日 年跨ぎ');
eq(lastDayOfMonth('2026-08-01'), '2026-08-31', '月末 31日');
eq(lastDayOfMonth('2026-09-15'), '2026-09-30', '月末 30日');
eq(lastDayOfMonth('2028-02-01'), '2028-02-29', '月末 うるう年');

// ── 掲載期間 (配布は開始から30日以内・終了は月末23:59) ──
// 31日ある月は2日開始 (8/02 00:00〜8/31 23:59 = 29日23時間59分)。現行の 7/02〜7/31 と同じ形
eq(computeNextMonthPeriod('2026-07-25'), {
  monthLabel: '2026-08', distStartYmd: '2026-08-02', distEndYmd: '2026-08-31',
  useStartYmd: '2026-08-02', useEndYmd: '2026-09-01',
}, '期間 7/25実行 → 8月分 (31日月は2日開始)');
// 30日の月は1日開始で収まる
eq(computeNextMonthPeriod('2026-08-25'), {
  monthLabel: '2026-09', distStartYmd: '2026-09-01', distEndYmd: '2026-09-30',
  useStartYmd: '2026-09-01', useEndYmd: '2026-10-01',
}, '期間 8/25実行 → 9月分 (30日月は1日開始)');
eq(computeNextMonthPeriod('2026-12-25'), {
  monthLabel: '2027-01', distStartYmd: '2027-01-02', distEndYmd: '2027-01-31',
  useStartYmd: '2027-01-02', useEndYmd: '2027-02-01',
}, '期間 年跨ぎ');
eq(computeNextMonthPeriod('2026-01-31'), {
  monthLabel: '2026-02', distStartYmd: '2026-02-01', distEndYmd: '2026-02-28',
  useStartYmd: '2026-02-01', useEndYmd: '2026-03-01',
}, '期間 2月は28日まで');
// 配布期間は必ず30日以内に収まる (au PAY の制約)
for (const t of ['2026-01-15', '2026-02-25', '2026-03-25', '2026-04-25', '2026-05-25', '2026-06-25',
  '2026-07-25', '2026-08-25', '2026-09-25', '2026-10-25', '2026-11-25', '2026-12-25', '2028-01-25']) {
  const q = computeNextMonthPeriod(t);
  const span = diffDays(q.distStartYmd, q.distEndYmd);
  ok(span < 30, `配布期間30日以内 (${t} → ${q.distStartYmd}〜${q.distEndYmd} = ${span}日+23:59)`);
  ok(q.distEndYmd === lastDayOfMonth(q.distStartYmd), `配布終了は月末 (${t})`);
  ok(diffDays(q.distEndYmd, q.useEndYmd) === 1, `利用終了は配布終了の翌日 (${t})`);
}

// ── テンプレート ──
eq(renderTemplate('ZACCA IZMで使える！商品{orderCount}点以上で{amount}円引きクーポン！', { orderCount: 3, amount: 55 }),
  'ZACCA IZMで使える！商品3点以上で55円引きクーポン！', 'テンプレート展開');

// ── 一覧行を使う関数群 (実データを模した行) ──
const mkRow = (id, orderCount, amount, s, e, distributionState = '配布期間終了') => ({
  couponId: id,
  couponKey: `key${id}`,
  title: `ZACCA IZMで使える！商品${orderCount}点以上で${amount}円引きクーポン！`,
  parsed: { orderCount, amount },
  discountAmount: amount,
  useStartYmd: s,
  useEndYmd: e,
  distributionState,
});
// 実際の一覧と同じ「利用開始日の降順」で並べる (同じ月の4本は同一日で連続する)
const ROWS = [
  mkRow('987115', 2, 30, '2026-07-02', '2026-08-01'),
  mkRow('987113', 3, 50, '2026-07-02', '2026-08-01'),
  mkRow('987111', 5, 150, '2026-07-02', '2026-08-01'),
  mkRow('977565', 2, 33, '2026-06-01', '2026-07-01'),
  mkRow('967489', 2, 30, '2026-05-02', '2026-06-01'),
];

eq(pickSourceRow(ROWS, 2).couponId, '987115', 'コピー元は利用終了が最も遅い行');
eq(pickSourceRow(ROWS, 4), null, '該当なしは null');
// 配布停止したクーポンはコピー元にしない (止めたものを翌月また作らない)
const ROWS_STOPPED = [
  mkRow('S1', 2, 30, '2026-07-02', '2026-08-01', '配布停止'),
  mkRow('S2', 2, 33, '2026-06-01', '2026-07-01', '配布期間終了'),
];
eq(pickSourceRow(ROWS_STOPPED, 2).couponId, 'S2', '停止済みを飛ばして次に新しい行を選ぶ');
eq(pickSourceRow([ROWS_STOPPED[0]], 2), null, '全部停止ならコピー元なし');
ok(!hasActiveCoupon([mkRow('S3', 2, 30, '2026-07-02', '2026-08-01', '配布停止')], 2, '2026-07-31'),
  '停止済みは有効なクーポンとして数えない');
eq(findExistingForMonth(ROWS, 2, '2026-07').couponId, '987115', '同じ月の行を検出');
eq(findExistingForMonth(ROWS, 2, '2026-08'), null, '未作成なら null');
// 1日開始でも2日開始でも「その月は作成済み」と判定できる
eq(findExistingForMonth([mkRow('x', 2, 30, '2026-08-01', '2026-09-01')], 2, '2026-08').couponId, 'x', '1日開始でも検出');
eq(findExistingForMonth([mkRow('x', 2, 30, '2026-08-02', '2026-09-01')], 2, '2026-08').couponId, 'x', '2日開始でも検出');
// 作成後の実在確認: 一覧のクーポン名は配布中だと「獲得用URLコピー」が続くので名前一致では探せない
const CREATED_ROW = {
  couponId: '999309',
  title: 'ZACCA IZMで使える！商品2点以上で33円引きクーポン！ 獲得用URLコピー',
  parsed: { orderCount: 2, amount: 33 },
  discountAmount: 33, useStartYmd: '2026-08-02', useEndYmd: '2026-09-01',
};
eq(findCreatedRow([CREATED_ROW], { orderCount: 2, amount: 33, useStartYmd: '2026-08-02' }).couponId, '999309',
  '獲得用URLコピー付きの行でも見つかる');
eq(findCreatedRow([CREATED_ROW], { orderCount: 2, amount: 30, useStartYmd: '2026-08-02' }), null, '金額違いは別物');
eq(findCreatedRow([CREATED_ROW], { orderCount: 3, amount: 33, useStartYmd: '2026-08-02' }), null, '点数違いは別物');
eq(findCreatedRow([CREATED_ROW], { orderCount: 2, amount: 33, useStartYmd: '2026-09-01' }), null, '開始日違いは別物');

ok(hasActiveCoupon(ROWS, 2, '2026-07-31'), '7/31時点で2点クーポンは有効');
ok(!hasActiveCoupon(ROWS, 2, '2026-08-05'), '8/5時点では有効なクーポンが無い');
ok(hasActiveCoupon(ROWS, 2, '2026-08-01'), '利用終了日当日は有効 (境界)');

// ── 一覧が二重作成の判定に使えるか (Codexレビュー critical への対応) ──
// 対象月より古い行まで届いていて、降順に並んでいれば「対象月は全部載っている」と言える
eq(checkListCoverage(ROWS, '2026-08').ok, true, '2026-07の行まであるので8月分の判定に足りる');
eq(checkListCoverage([mkRow('a', 2, 30, '2026-08-02', '2026-09-01')], '2026-08').ok, false,
  '対象月の行しかない = 古い行まで届いていない → 判定できない');
eq(checkListCoverage([], '2026-08').ok, false, '空の一覧は判定できない');
// 日付を読めない行を黙って捨てない (それが対象月の既存クーポンだと見落とす)
const ROWS_UNDATED = [{ couponId: 'B1', usePeriodText: '未設定', useStartYmd: '', parsed: null }, ...ROWS];
eq(checkListCoverage(ROWS_UNDATED, '2026-08').ok, false, '利用期間を読めない行があれば一覧を信用しない');
ok(/読み取れない/.test(checkListCoverage(ROWS_UNDATED, '2026-08').reason), '理由は読み取れない行がある');
eq(checkListCoverage([{ ...ROWS[0], useStartYmd: '2026-02-31' }, ...ROWS.slice(1)], '2026-08').ok, false,
  '実在しない日付も読めない行として扱う');
// 並び順が変わった (昇順になった) ら止める
const ROWS_ASC = [mkRow('a', 2, 30, '2026-05-02', '2026-06-01'), mkRow('b', 2, 33, '2026-07-02', '2026-08-01')];
eq(checkListCoverage(ROWS_ASC, '2026-08').ok, false, '昇順に並んでいたら判定できない');
ok(/降順/.test(checkListCoverage(ROWS_ASC, '2026-08').reason), '理由は降順でない');
// 同じ日が続くのは降順として許容 (同月4本は同じ利用開始日)
eq(checkListCoverage([
  mkRow('a', 2, 33, '2026-08-02', '2026-09-01'), mkRow('b', 3, 55, '2026-08-02', '2026-09-01'),
  mkRow('c', 2, 30, '2026-07-02', '2026-08-01'),
], '2026-08').ok, true, '同一日の連続は降順として扱う');

// 対象月に名前を解析できない行があれば拾う (表記違いの二重作成を防ぐ)
const ROWS_UNPARSED = [
  { title: 'ZACCA IZMで使える5％割引クーポン', parsed: null, useStartYmd: '2026-08-02', couponId: 'X1' },
  ...ROWS,
];
eq(findUnparsedInMonth(ROWS_UNPARSED, '2026-08').length, 1, '対象月の解析不能行を検出');
eq(findUnparsedInMonth(ROWS_UNPARSED, '2026-09').length, 0, '別の月なら対象外');
eq(findUnparsedInMonth(ROWS, '2026-08').length, 0, '全部解析できていれば0件');

// ── 計画 ──
const DEF2 = {
  key: 'qty2', orderCount: 2, cycle: [30, 33],
  titleTemplate: 'ZACCA IZMで使える！商品{orderCount}点以上で{amount}円引きクーポン！',
  descriptionTemplate: 'ZACCA IZMで使える商品{orderCount}点以上購入の方限定の{amount}円引きクーポンです。',
};

// 25日より前 → 何もしない
eq(planCoupon({ def: DEF2, rows: ROWS, todayYmd: '2026-07-24', createDay: 25 }).status, 'skip', '24日はまだ作らない');
// 25日 → 8月分を33円で作る (7月が30円なので次は33円)
const p = planCoupon({ def: DEF2, rows: ROWS, todayYmd: '2026-07-25', createDay: 25 });
eq(p.status, 'ready', '25日は作成する');
eq(p.amount, 33, '30円の次は33円');
eq(p.title, 'ZACCA IZMで使える！商品2点以上で33円引きクーポン！', '新しいクーポン名');
eq(p.description, 'ZACCA IZMで使える商品2点以上購入の方限定の33円引きクーポンです。', '新しい説明文');
eq(p.period.distStartYmd, '2026-08-02', '配布開始 (31日月は2日)');
eq(p.period.distEndYmd, '2026-08-31', '配布終了');
eq(p.period.useEndYmd, '2026-09-01', '利用終了');
eq(p.gapWarning, null, '有効クーポンがあるので警告なし');
// 26日以降も未作成なら作る (25日に失敗した日の挽回)
eq(planCoupon({ def: DEF2, rows: ROWS, todayYmd: '2026-07-28', createDay: 25 }).status, 'ready', '28日でも未作成なら作る');
// 作成済みならスキップ
const ROWS_DONE = [mkRow('999999', 2, 33, '2026-08-02', '2026-09-01'), ...ROWS];
const pDone = planCoupon({ def: DEF2, rows: ROWS_DONE, todayYmd: '2026-07-28', createDay: 25 });
eq(pDone.status, 'skip', '8月分が既にあれば作らない');
ok(/作成済み/.test(pDone.reason), 'skip理由は作成済み');
// 巡回リストに無い金額 → 止める
const ROWS_ODD = [mkRow('111111', 2, 44, '2026-07-02', '2026-08-01')];
eq(planCoupon({ def: DEF2, rows: ROWS_ODD, todayYmd: '2026-07-25', createDay: 25 }).status, 'error', '未知の金額はerror');
// コピー元なし
eq(planCoupon({ def: { ...DEF2, orderCount: 9 }, rows: ROWS, todayYmd: '2026-07-25', createDay: 25 }).status, 'error', 'コピー元なしはerror');
// コピー元が古すぎる = 一覧の見え方が変わった疑い → 二重作成を避けるため止める
const ROWS_STALE = [mkRow('OLD', 2, 30, '2026-01-03', '2026-02-01')];
const pStale = planCoupon({ def: DEF2, rows: ROWS_STALE, todayYmd: '2026-07-25', createDay: 25 });
eq(pStale.status, 'error', '半年前のクーポンしか無ければerror');
ok(/古すぎる/.test(pStale.reason), '理由はコピー元が古すぎる');
// 25日実行時の通常ケース (前月分が当月1日に終了 = 24日前) は止めない
eq(planCoupon({
  def: DEF2, rows: [mkRow('OK', 2, 30, '2026-06-01', '2026-07-01')], todayYmd: '2026-07-25', createDay: 25,
}).status, 'ready', '24日前の終了は正常範囲');
// 名前と実額の不一致 → 止める
const ROWS_MISMATCH = [{ ...mkRow('222222', 2, 30, '2026-07-02', '2026-08-01'), discountAmount: 33 }];
const pMis = planCoupon({ def: DEF2, rows: ROWS_MISMATCH, todayYmd: '2026-07-25', createDay: 25 });
eq(pMis.status, 'error', '名前と実額の不一致はerror');
ok(/不一致/.test(pMis.reason), '不一致の理由が出る');
// 実額を読めない (画面表記が変わった) → 止める
const pNoAmt = planCoupon({
  def: DEF2, rows: [{ ...mkRow('333333', 2, 30, '2026-07-02', '2026-08-01'), discountAmount: null, discountText: '金額指定 -- 円OFF' }],
  todayYmd: '2026-07-25', createDay: 25,
});
eq(pNoAmt.status, 'error', '割引額を読めなければerror');
ok(/割引額を読み取れない/.test(pNoAmt.reason), '理由は割引額を読めない');
// 利用期間を読めない → 止める
const pNoDate = planCoupon({
  def: DEF2, rows: [{ ...mkRow('444444', 2, 30, '', ''), usePeriodText: '未設定' }],
  todayYmd: '2026-07-25', createDay: 25,
});
eq(pNoDate.status, 'error', '利用期間を読めなければerror');
ok(/利用期間を読み取れない/.test(pNoDate.reason), '理由は利用期間を読めない');
// 開始と終了が逆転 → 止める
const pRev = planCoupon({
  def: DEF2, rows: [mkRow('555555', 2, 30, '2026-08-01', '2026-07-02')],
  todayYmd: '2026-07-25', createDay: 25,
});
eq(pRev.status, 'error', '利用期間が逆転していたらerror');
ok(/逆転/.test(pRev.reason), '理由は逆転');
// 空白期間 (前月分が切れている) → 作るが警告を付ける
const pGap = planCoupon({ def: DEF2, rows: ROWS, todayYmd: '2026-08-25', createDay: 25 });
eq(pGap.status, 'ready', '空白でも9月分は作る');
ok(pGap.gapWarning !== null, '有効クーポンが無いことを警告する');
eq(pGap.period.monthLabel, '2026-09', '8/25実行なら9月分');

// ── 結果 ──
if (fails.length) {
  console.error(`❌ ${fails.length} 件失敗 / ${pass} 件成功`);
  for (const f of fails) console.error(`  - ${f}`);
  process.exit(1);
}
console.log(`✅ 全 ${pass} 件パス`);
