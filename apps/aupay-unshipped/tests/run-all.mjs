/**
 * au PAY 未発送アラート のテスト。
 *   node apps/aupay-unshipped/tests/run-all.mjs
 * auPAY API にも warehouse.db にもアクセスしない (判定・整形の純関数だけを検証する)。
 */
import { parseStringPromise } from 'xml2js';
import {
  addDaysStr,
  parseAupayDatetime,
  toApiYmd,
  buildContext,
  extractOrderInfo,
  isStillUnshipped,
  verifyCandidates,
  buildMessage,
  loadResolved,
  saveResolved,
  resolvedFilePath,
  shouldSkipByCache,
  READY_TO_SHIP_STATUS,
  MAX_LINES,
} from '../service.js';
import { exitCodeFor } from '../notify-job.js';

// verifyCandidates は fetch を差し替えて呼ぶが、プロキシ設定の検証はその手前で走るためダミーを入れる
process.env.AUPAY_PROXY_SECRET = process.env.AUPAY_PROXY_SECRET || 'test-secret';
process.env.AUPAY_PROXY_URL = 'http://proxy.invalid';

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

// ─── 日付ユーティリティ ───
section('日付ユーティリティ');
eq(addDaysStr('2026-08-10', -1), '2026-08-09', '1日前');
eq(addDaysStr('2026-03-01', -1), '2026-02-28', '月またぎ');
// auPAY の日時は 'YYYY/MM/DD HH:MM'。new Date() 任せだと実行環境のTZに引きずられるので自前パース
eq(parseAupayDatetime('2026/08/09 12:34')?.toISOString(), '2026-08-09T03:34:00.000Z', 'JST として解釈する');
eq(parseAupayDatetime('2026/08/09')?.toISOString(), '2026-08-08T15:00:00.000Z', '日付のみ (発送期限) は 00:00 JST');
eq(parseAupayDatetime('2026/08/09 12:34:56')?.toISOString(), '2026-08-09T03:34:56.000Z', '秒付きも読める');
eq(parseAupayDatetime(''), null, '空文字');
eq(parseAupayDatetime(null), null, 'null');
eq(parseAupayDatetime('2026-08-09 12:34'), null, 'ハイフン形式は対象外 (auPAYはスラッシュ)');
eq(toApiYmd('2026/08/09 12:34'), '20260809', 'API検索キー (YYYYMMDD)');
eq(toApiYmd('壊れた値'), null, '壊れた日付は null');

// ─── buildContext ───
section('buildContext (JST基準)');
{
  const ctx = buildContext(new Date('2026-08-09T23:00:00Z'), 12); // = 8/10 08:00 JST
  eq(ctx.today, '2026-08-10', 'UTC実行でも today は JST');
  eq(ctx.cutoffDate, '2026-08-09', '締め日 = 前日');
  eq(ctx.cutoffStr, '2026/08/09 12:00', 'DB比較用は order_date と同じスラッシュ形式');
  eq(ctx.cutoff.toISOString(), '2026-08-09T03:00:00.000Z', '締め = 前日12:00 JST');
}
{
  const ctx = buildContext(new Date('2026-08-01T00:30:00+09:00'), 12);
  eq(ctx.cutoffDate, '2026-07-31', '月初の締め日は前月末');
}
// order_date は 'YYYY/MM/DD HH:MM' で桁が揃うので文字列比較でよい
section('DB文字列比較の前提');
const CUT = '2026/08/09 12:00';
ok('2026/08/06 17:16' <= CUT, '締め前の注文は cutoffStr 以下');
ok('2026/08/09 12:00' <= CUT, '締めちょうどは含まれる');
ok(!('2026/08/09 12:01' <= CUT), '締め1分後は含まれない');
ok('2025/12/31 23:59' <= CUT, '年またぎでも順序が保たれる');

// ─── extractOrderInfo ───
section('extractOrderInfo');
const XML = `<?xml version="1.0" encoding="UTF-8"?><response><result><status>0</status></result><resultCount>1</resultCount>
<orderInfo><orderId>352729794</orderId><orderDate>2026/08/05 20:13</orderDate><orderStatus>発送待ち</orderStatus>
<shipStatus>N</shipStatus><cancelStatus>N</cancelStatus><paymentStatus>N</paymentStatus>
<settlementName>au PAY（auかんたん決済）</settlementName><totalPrice>1230</totalPrice>
<detail><orderDetailId>1</orderDetailId><itemName>テスト商品</itemName><unit>2</unit><shippingTimelimitDate>2026/08/06</shippingTimelimitDate></detail>
</orderInfo></response>`;
{
  const parsed = await parseStringPromise(XML, { explicitArray: false });
  const info = extractOrderInfo(parsed.response.orderInfo);
  eq(info.orderId, '352729794', '注文ID');
  eq(info.orderStatus, '発送待ち', 'order_status');
  eq(info.shipStatus, 'N', 'ship_status');
  eq(info.settlementName, 'au PAY（auかんたん決済）', '決済方法');
  eq(info.totalPrice, 1230, '金額');
  eq(info.itemCount, 1, '明細数 (単一detailでも配列化)');
  eq(info.firstItemName, 'テスト商品', '商品名');
  eq(info.shippingTimelimitDate, '2026/08/06', '発送期限');
  ok(!JSON.stringify(info).includes('orderer'), '注文者情報を持ち回らない');
}
{
  const multi = XML.replace('</detail>', '</detail><detail><orderDetailId>2</orderDetailId><itemName>B</itemName><unit>1</unit></detail>');
  const parsed = await parseStringPromise(multi, { explicitArray: false });
  eq(extractOrderInfo(parsed.response.orderInfo).itemCount, 2, '複数明細');
}
eq(extractOrderInfo(null), null, 'null は null');
eq(extractOrderInfo({}), null, 'orderId が無ければ null');

// ─── isStillUnshipped ───
section('isStillUnshipped (API最新値での確定判定)');
const info = (over = {}) => ({ orderStatus: READY_TO_SHIP_STATUS, shipStatus: 'N', cancelStatus: 'N', ...over });
eq(isStillUnshipped(info()), true, '発送待ち × 未発送 × キャンセルでない → 未発送');
eq(isStillUnshipped(info({ shipStatus: 'Y' })), false, '発送済み → 対象外');
eq(isStillUnshipped(info({ cancelStatus: 'C' })), false, 'キャンセル → 対象外');
eq(isStillUnshipped(info({ orderStatus: '完了' })), false, '完了 → 対象外');
eq(isStillUnshipped(info({ orderStatus: '発送前入金待ち' })), false,
  '🚨発送前入金待ち → 対象外 (入金がまだなので発送できなくて当然)');
eq(isStillUnshipped(info({ orderStatus: 'キャンセル' })), false, 'order_status=キャンセル → 対象外');
eq(isStillUnshipped(null), false, 'null でも落ちない');
// 🚨payment_status は判定に使わない (auPAYでは発送後に Y になるため)
eq(isStillUnshipped(info({ paymentStatus: 'N' })), true, 'payment_status=N でも発送待ちなら対象 (auPAYのYは発送後)');
eq(isStillUnshipped(info({ paymentStatus: 'Y' })), true, 'payment_status は判定に影響しない');

// ─── verifyCandidates ───
section('verifyCandidates');
const ctxNow = { ...buildContext(new Date('2026-08-10T08:00:00+09:00'), 12), now: new Date('2026-08-10T08:00:00+09:00') };
const cand = (id, date) => ({
  order_id: id, order_date: date, order_status: READY_TO_SHIP_STATUS, ship_status: 'N',
  cancel_status: 'N', settlement_name: 'クレジットカード決済', total_price: 1230,
  shipping_timelimit_date: '2026/08/06', item_count: 1, item_name: 'DBの商品名',
});
const xmlFor = (orders) => `<response><result><status>0</status></result><resultCount>${orders.length}</resultCount>${orders.join('')}</response>`;
const orderXml = (id, over = {}) => {
  const o = { orderStatus: '発送待ち', shipStatus: 'N', cancelStatus: 'N', orderDate: '2026/08/05 20:13', ...over };
  return `<orderInfo><orderId>${id}</orderId><orderDate>${o.orderDate}</orderDate><orderStatus>${o.orderStatus}</orderStatus>`
    + `<shipStatus>${o.shipStatus}</shipStatus><cancelStatus>${o.cancelStatus}</cancelStatus>`
    + `<settlementName>クレジットカード決済</settlementName><totalPrice>1230</totalPrice>`
    + `<detail><orderDetailId>1</orderDetailId><itemName>テスト商品</itemName><unit>1</unit>`
    + `<shippingTimelimitDate>2026/08/06</shippingTimelimitDate></detail></orderInfo>`;
};
const fakeFetch = (byDay) => async (url) => {
  const m = /startDate=(\d{8})/.exec(url);
  const xml = byDay[m?.[1]] ?? xmlFor([]);
  return { ok: true, text: async () => xml };
};
const noSleep = async () => {};

{
  const r = await verifyCandidates(
    [cand('A1', '2026/08/05 20:13'), cand('A2', '2026/08/05 21:00')],
    ctxNow,
    { fetchImpl: fakeFetch({ '20260805': xmlFor([orderXml('A1'), orderXml('A2', { shipStatus: 'Y' })]) }), sleepImpl: noSleep },
  );
  eq(r.alerts.length, 1, '発送済みになっていた候補は落ちる');
  eq(r.alerts[0].orderId, 'A1', '残るのは未発送のみ');
  eq(r.resolvedIds, ['A2'], '解消済みは記録して次回スキップできるようにする');
  eq(r.verifiedDays, 1, '同じ注文日は1リクエストにまとまる');
  eq(r.apiFailed, 0, '確認失敗なし');
  eq(r.alerts[0].elapsedHours, 107.8, '注文からの経過時間');
  eq(r.alerts[0].limitOverdueDays, 4, '発送期限からの超過日数');
}
{
  // API が返してこなかった注文は apiFailed (黙って「解消」にしない)
  const r = await verifyCandidates([cand('X1', '2026/08/05 20:13')], ctxNow,
    { fetchImpl: fakeFetch({}), sleepImpl: noSleep });
  eq(r.alerts.length, 0, '確認できなければ通知しない');
  eq(r.apiFailed, 1, '確認できなかった件数を数える');
  eq(r.resolvedIds, [], '確認できなかった注文は解消済みに入れない');
}
{
  // 注文日が壊れている候補は確認できない → apiFailed
  const r = await verifyCandidates([{ ...cand('B1', '壊れた日付') }], ctxNow,
    { fetchImpl: fakeFetch({}), sleepImpl: noSleep });
  eq(r.apiFailed, 1, '日付が壊れた候補は確認できずに数える');
  eq(r.verifiedDays, 0, 'API は呼ばない');
}
{
  const many = ['01', '02', '03', '04', '05'].map(d => cand(`C${d}`, `2026/08/${d} 10:00`));
  const r = await verifyCandidates(many, ctxNow, { maxVerifyDays: 2, fetchImpl: fakeFetch({}), sleepImpl: noSleep });
  eq(r.verifiedDays, 2, '注文日の上限で打ち切る');
  eq(r.truncated, true, '打ち切りを記録する');
}
{
  const r = await verifyCandidates([], ctxNow, { fetchImpl: fakeFetch({}), sleepImpl: noSleep });
  eq(r.verifiedDays, 0, '候補0件ならAPIを呼ばない');
  eq(r.truncated, false, '打ち切りでもない');
}
{
  // 1日分が落ちても他の日は続ける
  let calls = 0;
  const flaky = async (url) => {
    calls++;
    if (/startDate=20260805/.test(url)) return { ok: false, status: 500, text: async () => 'boom' };
    return { ok: true, text: async () => xmlFor([orderXml('D2', { orderDate: '2026/08/06 10:00' })]) };
  };
  const r = await verifyCandidates(
    [cand('D1', '2026/08/05 20:13'), cand('D2', '2026/08/06 10:00')],
    ctxNow, { fetchImpl: flaky, sleepImpl: noSleep },
  );
  eq(r.apiFailed, 1, '落ちた日の候補は確認できずに数える');
  eq(r.alerts.length, 1, '他の日は続行して検知できる');
  ok(calls >= 2, '2日分とも呼びに行く');
}
{
  // 発送前入金待ち (入金がまだ) は最新確認で落とす
  const r = await verifyCandidates([cand('E1', '2026/08/05 20:13')], ctxNow, {
    fetchImpl: fakeFetch({ '20260805': xmlFor([orderXml('E1', { orderStatus: '発送前入金待ち' })]) }),
    sleepImpl: noSleep,
  });
  eq(r.alerts.length, 0, '入金待ちに戻っていたら通知しない');
  eq(r.resolvedIds, ['E1'], '解消済み扱い');
}

// ─── 解消済みキャッシュ ───
section('解消済みキャッシュ');
{
  const os = await import('node:os');
  const fs = await import('node:fs');
  const path = await import('node:path');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'aupay-unshipped-'));
  const prev = process.env.DATA_DIR;
  process.env.DATA_DIR = tmp;
  try {
    eq(loadResolved(), {}, 'ファイルが無ければ空');
    saveResolved({ A: { resolvedAt: '2026-08-10' }, B: { resolvedAt: '2026-01-01' } }, '2026-08-10');
    const loaded = loadResolved();
    eq(loaded.A?.resolvedAt, '2026-08-10', '新しい記録は残る');
    eq(loaded.B, undefined, '保持日数を過ぎた記録は捨てる');
    fs.writeFileSync(resolvedFilePath(), '{壊れたJSON');
    eq(loadResolved(), {}, '壊れていても空で続行する');
    fs.writeFileSync(resolvedFilePath(), JSON.stringify({ OLD: '2026-08-10' }));
    eq(loadResolved().OLD, { resolvedAt: '2026-08-10' }, '旧形式 (値が文字列) も読める');
  } finally {
    if (prev === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prev;
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}
section('shouldSkipByCache');
{
  const row = { order_id: 'A' };
  eq(shouldSkipByCache(row, { A: { resolvedAt: '2026-08-09' } }, '2026-08-10'), true, '確認から間もなければスキップ');
  // 🚨auPAY には更新日時が無いので、時間経過だけが再確認のきっかけになる
  eq(shouldSkipByCache(row, { A: { resolvedAt: '2026-08-03' } }, '2026-08-10'), false,
    '確認から7日経ったら再確認する (発送取消などで戻っていても検出できる)');
  eq(shouldSkipByCache(row, {}, '2026-08-10'), false, 'キャッシュに無ければ確認する');
  eq(shouldSkipByCache(row, { A: {} }, '2026-08-10'), false, '壊れた記録は確認する');
}

// ─── buildMessage ───
section('buildMessage');
const mkAlert = (over = {}) => ({
  orderId: '352729794',
  orderedAt: new Date('2026-08-05T20:13:00+09:00'),
  elapsedHours: 83.8,
  orderStatus: '発送待ち',
  settlementName: 'au PAY（auかんたん決済）',
  totalPrice: 1230,
  itemCount: 1,
  firstItemName: 'テスト商品',
  shippingTimelimit: '2026/08/06',
  limitOverdueDays: 4,
  ...over,
});
{
  const text = buildMessage({ alerts: [], candidates: 7, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('*au PAY 未発送アラート*'), 'モール名が分かる見出し');
  ok(text.includes('✅ 出荷漏れはありません'), '0件でも本文を作る');
  ok(text.includes('候補7件はすべて発送済み・キャンセル済み'), '候補があった日はその旨を出す');
  ok(text.includes('08/09 12:00'), '締め時刻を明記');
}
{
  const text = buildMessage({ alerts: [mkAlert()], candidates: 3, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('🚨 出荷漏れの可能性 *1件*'), '件数');
  ok(text.includes('352729794'), '注文ID');
  ok(text.includes('¥1,230'), '金額');
  ok(text.includes('au PAY（auかんたん決済）'), '決済方法');
  ok(text.includes('83.8時間経過'), '経過時間');
  ok(text.includes('発送期限 2026/08/06 (4日超過)'), '発送期限の超過を出す');
}
{
  const text = buildMessage({ alerts: [mkAlert({ limitOverdueDays: -2 })], candidates: 1, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('発送期限 2026/08/06') && !text.includes('超過'), 'まだ期限内なら超過表記を出さない');
}
{
  const text = buildMessage({ alerts: [mkAlert({ shippingTimelimit: '' })], candidates: 1, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(!text.includes('発送期限'), '発送期限が無ければ出さない');
}
{
  const text = buildMessage({ alerts: [mkAlert()], candidates: 3, apiFailed: 2, truncated: true, ctx: ctxNow });
  ok(text.includes('2件は最新状態を確認できませんでした'), '確認不能を明示');
  ok(text.includes('上限'), '打ち切りを明示');
}
{
  const many = Array.from({ length: MAX_LINES + 3 }, () => mkAlert());
  const text = buildMessage({ alerts: many, candidates: 40, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('他 3件'), '表示上限の超過を明示');
  ok(text.includes(`*${MAX_LINES + 3}件*`), '総件数は正しい');
}
{
  const long = mkAlert({ firstItemName: 'あ'.repeat(80), itemCount: 3 });
  const text = buildMessage({ alerts: [long], candidates: 1, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('…'), '長い商品名は切る');
  ok(text.includes('ほか2点'), '2件目以降は点数で表す');
}

// ─── exitCodeFor ───
section('exitCodeFor');
eq(exitCodeFor({ apiFailed: 0, truncated: false }), 0, '完全 → 0');
eq(exitCodeFor({ apiFailed: 2, truncated: false }), 1, '確認できない注文がある → 1 (当日中にretry)');
eq(exitCodeFor({ apiFailed: 0, truncated: true }), 2, '上限打ち切り → 2 (blocked)');
eq(exitCodeFor({ apiFailed: 1, truncated: true }), 1, '両方なら retry を優先');
eq(exitCodeFor(null), 0, 'null でも落ちない');

// daily-sync 側の契約: finish() で終了したプロセスの終了コードが execFileSync の e.status に届くこと
section('終了コードの伝播 (自然終了)');
{
  const { execFileSync } = await import('node:child_process');
  const notifyUrl = new URL('../notify-job.js', import.meta.url).href;
  const statusOfFinish = (code) => {
    const script = `const m = await import(${JSON.stringify(notifyUrl)}); m.finish(${code});`;
    try {
      execFileSync(process.execPath, ['--input-type=module', '-e', script], { stdio: 'pipe' });
      return 0;
    } catch (e) {
      return e.status;
    }
  };
  eq(statusOfFinish(0), 0, 'finish(0) → 正常終了');
  eq(statusOfFinish(1), 1, 'finish(1) → e.status=1 (retry対象)');
  eq(statusOfFinish(2), 2, 'finish(2) → e.status=2 (blocked判定)');
  const started = Date.now();
  eq(statusOfFinish(2), 2, 'finish(2) を再度');
  ok(Date.now() - started < 4000, '強制終了タイマー (5秒) を待たずに終了する');
}

console.log(`\n${fail === 0 ? '全テスト pass' : `${fail}件 失敗`} (pass=${pass} fail=${fail})`);
process.exit(fail === 0 ? 0 : 1);
