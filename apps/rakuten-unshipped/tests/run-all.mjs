/**
 * 楽天 未発送アラート のテスト。
 *   node apps/rakuten-unshipped/tests/run-all.mjs
 * RMS API には一切アクセスしない (判定・整形の純関数だけを検証する)。
 */
import {
  addDaysStr,
  parseRmsDatetime,
  buildContext,
  classifyOrder,
  summarizeOrder,
  buildMessage,
  MAX_LINES,
  UNSHIPPED_PROGRESS,
} from '../service.js';

let pass = 0;
let fail = 0;

function ok(cond, label) {
  if (cond) { pass++; return; }
  fail++;
  console.error(`  ✗ ${label}`);
}
function eq(actual, expected, label) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { pass++; return; }
  fail++;
  console.error(`  ✗ ${label}\n      期待: ${e}\n      実際: ${a}`);
}
function section(name) { console.log(`\n── ${name}`); }

// ─── addDaysStr ───
section('addDaysStr');
eq(addDaysStr('2026-08-09', -1), '2026-08-08', '1日前');
eq(addDaysStr('2026-08-01', -1), '2026-07-31', '月またぎ (前月末)');
eq(addDaysStr('2026-01-01', -1), '2025-12-31', '年またぎ');
eq(addDaysStr('2024-03-01', -1), '2024-02-29', 'うるう年');
eq(addDaysStr('2026-08-09', -30), '2026-07-10', '30日前');
ok((() => { try { addDaysStr('2026/08/09', -1); return false; } catch { return true; } })(), '不正な形式は例外');

// ─── parseRmsDatetime ───
section('parseRmsDatetime');
// ⚠RMS は "+0900" (コロン無し) を返す。ここを取りこぼすと全件 skip になる
eq(parseRmsDatetime('2026-08-05T16:30:12+0900')?.toISOString(), '2026-08-05T07:30:12.000Z', 'コロン無しオフセット');
eq(parseRmsDatetime('2026-08-05T16:30:12+09:00')?.toISOString(), '2026-08-05T07:30:12.000Z', 'コロン有りオフセット');
eq(parseRmsDatetime(null), null, 'null');
eq(parseRmsDatetime(''), null, '空文字');
eq(parseRmsDatetime('not-a-date'), null, '不正な文字列');
eq(parseRmsDatetime(12345), null, '文字列以外');

// ─── buildContext ───
section('buildContext (JST基準)');
{
  // Render は UTC で動く。JST 08:00 の実行時刻は UTC では前日 23:00
  const now = new Date('2026-08-08T23:00:00Z'); // = 2026-08-09 08:00 JST
  const ctx = buildContext(now, 12);
  eq(ctx.today, '2026-08-09', 'UTC実行でも today は JST の日付');
  eq(ctx.cutoffDate, '2026-08-08', '締め日 = 前日');
  eq(ctx.cutoff.toISOString(), '2026-08-08T03:00:00.000Z', '締め = 前日12:00 JST');
  eq(ctx.cutoffHour, 12, 'cutoffHour を持ち回る');
}
{
  // JST 00:30 (= UTC 前日 15:30) — toISOString 直だと前日に化ける時間帯
  const ctx = buildContext(new Date('2026-08-08T15:30:00Z'), 12);
  eq(ctx.today, '2026-08-09', 'JST 深夜0時台でも当日扱い');
  eq(ctx.cutoffDate, '2026-08-08', 'JST 深夜0時台の締め日');
}
{
  const ctx = buildContext(new Date('2026-08-01T00:00:00+09:00'), 12);
  eq(ctx.cutoffDate, '2026-07-31', '月初は前月末が締め日');
}

// ─── classifyOrder ───
section('classifyOrder');
const ctx = { ...buildContext(new Date('2026-08-09T08:00:00+09:00'), 12) };
const order = (over = {}) => ({
  orderNumber: '123456-20260805-0123456789',
  orderProgress: 300,
  orderDatetime: '2026-08-05T15:59:26+0900',
  orderFixDatetime: '2026-08-05T16:30:12+0900',
  deliveryDate: null,
  totalPrice: 1496,
  SettlementModel: { settlementMethod: 'クレジットカード' },
  PackageModelList: [{ ItemModelList: [{ itemName: 'テスト商品', itemNumber: 'aa0101', units: 1 }] }],
  ...over,
});

eq(classifyOrder(order(), ctx), 'alert', '締め前に入金確認済みで未発送 → alert');
eq(classifyOrder(order({ orderProgress: 500 }), ctx), 'skip', '発送済 (500) → skip');
eq(classifyOrder(order({ orderProgress: 900 }), ctx), 'skip', 'キャンセル (900) → skip');
eq(classifyOrder(order({ orderFixDatetime: null }), ctx), 'skip', '未入金 (受注未確定) → skip');
eq(classifyOrder(order({ orderFixDatetime: '2026-08-08T12:00:01+0900' }), ctx), 'skip', '締め1秒後の入金 → skip');
eq(classifyOrder(order({ orderFixDatetime: '2026-08-08T12:00:00+0900' }), ctx), 'alert', '締めちょうどの入金 → alert');
eq(classifyOrder(order({ orderFixDatetime: '2026-08-09T07:00:00+0900' }), ctx), 'skip', '今朝の入金 → skip');
eq(classifyOrder(order({ orderFixDatetime: 'garbage-value' }), ctx), 'bad_datetime', 'パース不能な確定日時 → bad_datetime (無音でskipしない)');
eq(classifyOrder(order({ deliveryDate: '2026-08-14' }), ctx), 'hold', 'お届け日が5日先 → hold');
eq(classifyOrder(order({ deliveryDate: '2026-08-12' }), ctx), 'hold', 'お届け日が3日先 (猶予2日の外) → hold');
eq(classifyOrder(order({ deliveryDate: '2026-08-11' }), ctx), 'alert', 'お届け日が2日先 (猶予内) → alert (もう出荷すべき)');
eq(classifyOrder(order({ deliveryDate: '2026-08-09' }), ctx), 'alert', 'お届け日が今日 → alert (出荷すべき)');
eq(classifyOrder(order({ deliveryDate: '2026-08-07' }), ctx), 'alert', 'お届け日が過去 → alert (すでに遅れている)');
eq(classifyOrder(order({ deliveryDate: '' }), ctx), 'alert', 'お届け日が空文字 → alert');
eq(classifyOrder(order({ deliveryDate: '2026-08-14' }), { ...ctx, leadDays: 0 }, ), 'hold', 'leadDays=0 なら未来のお届け日は全部 hold');
eq(classifyOrder(order({ deliveryDate: '2026-08-10' }), { ...ctx, leadDays: 0 }, ), 'hold', 'leadDays=0: 明日のお届け日も hold');
eq(classifyOrder(order({ orderProgress: 400 }), ctx), 'alert', '変更確定待ち (400) も未発送として拾う');
eq(classifyOrder(order({ orderProgress: 200 }), ctx), 'alert', '楽天処理中 (200) で確定済みなら拾う');
eq(classifyOrder(order({ orderProgress: 600 }), ctx), 'skip', '支払手続き中 (600) は対象外');
eq(classifyOrder({}, ctx), 'skip', '空オブジェクトでも落ちない');
ok(!UNSHIPPED_PROGRESS.includes(500) && !UNSHIPPED_PROGRESS.includes(800), '未発送の定義に発送済/キャンセルを含めない');

// 前払い (コンビニ・銀行振込) も入金確認済みなら同じ基準で拾う
eq(classifyOrder(order({
  SettlementModel: { settlementMethod: '銀行振込' },
  orderDatetime: '2026-08-01T08:21:15+0900',
  orderFixDatetime: '2026-08-05T14:07:13+0900',
}), ctx), 'alert', '銀行振込で入金確認済み → alert');
eq(classifyOrder(order({
  SettlementModel: { settlementMethod: 'セブンイレブン（前払）' },
  orderFixDatetime: null,
}), ctx), 'skip', 'コンビニ前払いで未入金 → skip (発送できなくて当然)');

// ─── summarizeOrder ───
section('summarizeOrder');
{
  const ctxNow = { ...ctx, now: new Date('2026-08-09T08:00:00+09:00') };
  const s = summarizeOrder(order(), ctxNow);
  eq(s.orderNumber, '123456-20260805-0123456789', '注文番号');
  eq(s.settlementMethod, 'クレジットカード', '決済方法');
  eq(s.progressLabel, '発送待ち', 'ステータス名');
  eq(s.elapsedHours, 87.5, '入金確認からの経過時間');
  eq(s.itemCount, 1, '明細数');
  eq(s.firstItemName, 'テスト商品', '先頭商品名');
  eq(s.totalUnits, 1, '合計点数');
  // 個人情報を持ち回っていないこと
  ok(!('OrdererModel' in s) && !JSON.stringify(s).includes('Orderer'), '注文者情報を含まない');
}
{
  const multi = order({
    PackageModelList: [
      { ItemModelList: [{ itemName: 'A', units: 2 }, { itemName: 'B', units: 3 }] },
      { ItemModelList: [{ itemName: 'C', units: 1 }] },
    ],
  });
  const s = summarizeOrder(multi, { ...ctx, now: new Date() });
  eq(s.itemCount, 3, '複数パッケージの明細を合算');
  eq(s.totalUnits, 6, '点数の合算');
}
{
  const s = summarizeOrder({ orderProgress: 300, PackageModelList: [] }, { ...ctx, now: new Date() });
  eq(s.elapsedHours, null, '確定日時が無ければ経過時間は null');
  eq(s.settlementMethod, '(不明)', '決済方法が無い場合');
  eq(s.firstItemName, '', '明細が無い場合');
}

// ─── buildMessage ───
section('buildMessage');
const mkCtx = () => ({ ...ctx, now: new Date('2026-08-09T08:00:00+09:00') });
{
  const text = buildMessage({ alerts: [], holds: [], ctx: mkCtx(), scanned: 42 });
  ok(text.includes('✅ 出荷漏れはありません (0件)'), '0件でも本文を作る');
  ok(text.includes('08/08 12:00'.replace('08/', '8/')) || text.includes('8/8 12:00'), '締め時刻を明記');
  ok(text.includes('42件'), '走査件数を出す');
  ok(!text.includes('結果が不完全'), '完全な結果に不完全の警告を出さない');
}
{
  // 結果が不完全な日はそのことを本文に出す (黙って「0件」に見せない)
  const text = buildMessage({ alerts: [], holds: [], ctx: mkCtx(), scanned: 42, truncated: true, badDatetimes: 2, missingDetails: 1 });
  ok(text.includes('⚠️ 結果が不完全です'), '不完全の警告を出す');
  ok(text.includes('打ち切られました'), '検索打ち切りを明示');
  ok(text.includes('2件'), 'パース不能件数を出す');
  ok(text.includes('1件'), '明細欠落件数を出す');
}
{
  const a = summarizeOrder(order(), mkCtx());
  const h = summarizeOrder(order({ deliveryDate: '2026-08-14', totalPrice: 1398 }), mkCtx());
  const text = buildMessage({ alerts: [a], holds: [h], ctx: mkCtx(), scanned: 10 });
  ok(text.includes('🚨 出荷漏れの可能性 *1件*'), '件数を出す');
  ok(text.includes(a.orderNumber), '注文番号を出す');
  ok(text.includes('¥1,496'), '金額を3桁区切りで出す');
  ok(text.includes('クレジットカード'), '決済方法を出す');
  ok(text.includes('87.5時間経過'), '経過時間を出す');
  ok(text.includes('お届け日指定がまだ先のため保留中 1件'), '保留は別枠');
  ok(text.includes('2026-08-14'), '保留のお届け日を出す');
}
{
  // 上限を超えたら「他N件」と明示する (黙って切らない)
  const many = Array.from({ length: MAX_LINES + 5 }, () => summarizeOrder(order(), mkCtx()));
  const text = buildMessage({ alerts: many, holds: [], ctx: mkCtx(), scanned: 100 });
  ok(text.includes(`他 5件`), '上限超過を明示');
  ok(text.includes(`*${MAX_LINES + 5}件*`), '総件数は正しく出す');
}
{
  const long = summarizeOrder(order({
    PackageModelList: [{ ItemModelList: [
      { itemName: 'あ'.repeat(80), units: 1 },
      { itemName: 'い', units: 1 },
    ] }],
  }), mkCtx());
  const text = buildMessage({ alerts: [long], holds: [], ctx: mkCtx(), scanned: 1 });
  ok(text.includes('…'), '長い商品名は省略記号付きで切る');
  ok(text.includes('ほか1点'), '2件目以降は点数で表す');
  ok(!text.includes('あ'.repeat(50)), '商品名を丸ごとは出さない');
}

console.log(`\n${fail === 0 ? '全テスト pass' : `${fail}件 失敗`} (pass=${pass} fail=${fail})`);
process.exit(fail === 0 ? 0 : 1);
