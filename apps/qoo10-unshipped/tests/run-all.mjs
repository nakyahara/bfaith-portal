/**
 * Qoo10 未発送アラート のテスト。
 *   node apps/qoo10-unshipped/tests/run-all.mjs
 * 一時DBを作って検証する (本番の warehouse.db には触れない)。Qoo10 API も叩かない。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// ⚠️service.js は import 時点の DATA_DIR で DB を開くため、import より前に差し替える
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'qoo10-unshipped-'));
process.env.DATA_DIR = tmpDir;
process.env.GCHAT_WEBHOOK_SHIPPING = 'https://chat.googleapis.com/dummy';

const { initDB, getDB } = await import('../../warehouse/db.js');
const {
  addDaysStr,
  parseQoo10Datetime,
  buildContext,
  countSeenToday,
  findUnshipped,
  countAwaitingPayment,
  summarize,
  orderNosOf,
  findUnshippedOrders,
  buildMessage,
  MAX_LINES,
} = await import('../service.js');
const { exitCodeFor } = await import('../notify-job.js');

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
eq(parseQoo10Datetime('2026-08-09 12:34:56')?.toISOString(), '2026-08-09T03:34:56.000Z', 'JST として解釈する');
eq(parseQoo10Datetime('2026-08-09 12:34')?.toISOString(), '2026-08-09T03:34:00.000Z', '秒なしも読める');
eq(parseQoo10Datetime(''), null, '空文字');
eq(parseQoo10Datetime(null), null, 'null');
eq(parseQoo10Datetime('2026/08/09 12:34'), null, 'スラッシュ形式は対象外 (Qoo10はハイフン)');

section('buildContext (JST基準)');
{
  const ctx = buildContext(new Date('2026-08-09T23:00:00Z'), 12); // = 8/10 08:00 JST
  eq(ctx.today, '2026-08-10', 'UTC実行でも today は JST');
  eq(ctx.cutoffDate, '2026-08-09', '締め日 = 前日');
  eq(ctx.cutoffStr, '2026-08-09 12:00:00', 'DB比較用は order_date と同じ形式');
}
section('DB文字列比較の前提');
const CUT = '2026-08-09 12:00:00';
ok('2026-06-03 20:58:02' <= CUT, '締め前の注文は cutoffStr 以下');
ok('2026-08-09 12:00:00' <= CUT, '締めちょうどは含まれる');
ok(!('2026-08-09 12:00:01' <= CUT), '締め1秒後は含まれない');

// ─── DB を使う検証 ───
section('候補抽出 (一時DB)');
initDB();
const db = getDB();
db.exec(`CREATE TABLE IF NOT EXISTS raw_qoo10_orders (
  order_id TEXT, source_order_key TEXT, pack_no TEXT, shipping_status TEXT, payment_method TEXT, item_title TEXT,
  order_qty INTEGER, total REAL, tracking_no TEXT, order_date TEXT, last_seen_at TEXT,
  is_frozen_after_horizon INTEGER DEFAULT 0
)`);
const TODAY = '2026-08-10';
const ctx = { ...buildContext(new Date('2026-08-10T08:00:00+09:00'), 12), now: new Date('2026-08-10T08:00:00+09:00') };
const ins = db.prepare(`INSERT INTO raw_qoo10_orders
  (order_id, source_order_key, pack_no, shipping_status, payment_method, item_title, order_qty, total, tracking_no, order_date, last_seen_at, is_frozen_after_horizon)
  VALUES (@order_id, @source_order_key, @pack_no, @shipping_status, @payment_method, @item_title, @qty, @total, @tracking_no, @order_date, @last_seen_at, @frozen)`);
// source_order_key = Qoo10 の注文番号 (本番は 'api:<注文番号>' が order_id。テストは 'api:1' → '1')
const row = (o) => ins.run({
  order_id: 'api:1', pack_no: '100', shipping_status: 'Seller confirm(3)', payment_method: 'PayPay',
  item_title: '商品A', qty: 1, total: 1000, tracking_no: '', order_date: '2026-08-05 10:00:00',
  last_seen_at: `${TODAY}T07:58:00.000+09:00`, frozen: 0,
  source_order_key: String(o.order_id || 'api:1').replace(/^api:/, ''), ...o,
});

row({ order_id: 'api:1', pack_no: '100' });                                   // ← 検知対象
row({ order_id: 'api:2', pack_no: '100', item_title: '商品B', total: 500 });   // 同じ梱包 = まとめる (注文番号は別)
row({ order_id: 'api:3', pack_no: '200', shipping_status: 'Delivered(5)' });   // 発送済み
row({ order_id: 'api:4', pack_no: '201', shipping_status: 'On delivery(4)' }); // 配送中
row({ order_id: 'api:5', pack_no: '202', shipping_status: 'Awaiting shipping(1)', payment_method: 'コンビニ決済' }); // 入金待ち
row({ order_id: 'api:6', pack_no: '203', last_seen_at: '2026-06-01T07:00:00.000+09:00' }); // キャンセル済み (APIから消えた)
row({ order_id: 'api:7', pack_no: '204', frozen: 1 });                         // 90日境界を越えて凍結
row({ order_id: 'api:8', pack_no: '205', order_date: '2026-08-09 18:00:00' }); // 締め後の注文

eq(countSeenToday(TODAY), 6, '今日の同期で確認できた件数 (frozen と last_seen_atが古い行は除く)');
{
  const rows = findUnshipped(ctx);
  eq(rows.length, 1, '検知は1件 (梱包単位にまとまる)');
  eq(rows[0].pack_no, '100', '梱包番号');
  eq(orderNosOf(rows[0]), ['1', '2'], '同じ梱包の注文番号を全部集める (QSMは注文番号で検索する)');
  eq(rows[0].item_count, 2, '同じ梱包の明細をまとめる');
  eq(rows[0].total, 1500, '金額を合算する');
  // 🚨キャンセル済み (last_seen_at が古い) を拾わないことが Qoo10版の肝
  ok(!rows.some(r => r.pack_no === '203'), 'キャンセル済み (last_seen_atが古い) は拾わない');
  ok(!rows.some(r => r.pack_no === '204'), 'frozen は拾わない');
  ok(!rows.some(r => r.pack_no === '205'), '締め後の注文は拾わない');
  ok(!rows.some(r => r.pack_no === '202'), '入金待ちは拾わない (発送できなくて当然)');
  ok(!rows.some(r => ['200', '201'].includes(r.pack_no)), '発送済み・配送中は拾わない');
}
eq(countAwaitingPayment(ctx), 1, '入金待ちは参考として数える');
{
  const s = summarize(findUnshipped(ctx)[0], ctx);
  eq(s.packNo, '100', '梱包番号');
  eq(s.orderNos, ['1', '2'], '通知用にも注文番号が載る');
  eq(s.elapsedHours, 118, '注文からの経過時間');
  eq(s.hasTracking, false, '追跡番号なし');
  eq(s.qty, 2, '数量の合算');
}
{
  const r = findUnshippedOrders({ now: ctx.now, cutoffHour: 12 });
  eq(r.alerts.length, 1, '検知本体も1件');
  eq(r.stale, false, '今日の同期があれば stale ではない');
  eq(r.awaitingPayment, 1, '入金待ちの件数');
}

section('orderNosOf');
eq(orderNosOf({ order_keys: '1216505367,1216505366', order_id: 'api:1216505366' }), ['1216505366', '1216505367'], '昇順に揃える');
eq(orderNosOf({ order_keys: '', order_id: 'api:1218369750' }), ['1218369750'], 'source_order_key が無ければ order_id から');
eq(orderNosOf({ order_keys: '0', order_id: 'legacy:419962378:5' }), ['legacy:419962378:5'], "legacy の '0' は注文番号として使わない");
eq(orderNosOf({ order_keys: '5,5', order_id: 'api:5' }), ['5'], '重複は落とす');

// ─── 鮮度チェック (Qoo10版の肝) ───
section('同期が走っていない日は判定を見送る');
{
  // 今日の同期が1件も無い状態 = キャンセル済みの取り残しを掴んでしまう
  db.exec(`UPDATE raw_qoo10_orders SET last_seen_at = '2026-08-01T07:00:00.000+09:00'`);
  const r = findUnshippedOrders({ now: ctx.now, cutoffHour: 12 });
  eq(r.stale, true, '今日の同期が0件なら stale');
  eq(r.alerts.length, 0, '判定を見送るので通知はしない');
  eq(exitCodeFor(r), 1, 'exit 1 = retry で拾い直す');
  const text = buildMessage(r);
  ok(text.includes('判定できませんでした'), '本文でも判定できなかったことを伝える');
  ok(!text.includes('出荷漏れはありません'), '「ありません」と誤解される表現を出さない');
}

// ─── buildMessage ───
section('buildMessage');
const mkAlert = (over = {}) => ({
  packNo: '100', orderId: 'api:1', orderNos: ['1216801633'],
  orderedAt: new Date('2026-08-05T10:00:00+09:00'),
  elapsedHours: 118, shippingStatus: 'Seller confirm(3)', paymentMethod: 'PayPay',
  hasTracking: false, total: 1500, qty: 2, itemCount: 2, itemTitle: '商品A',
  ...over,
});
const mkCtx = () => ({ ...ctx });
{
  const text = buildMessage({ alerts: [], awaitingPayment: 0, seenToday: 767, stale: false, ctx: mkCtx() });
  ok(text.includes('*Qoo10 未発送アラート*'), 'モール名が分かる見出し');
  ok(text.includes('✅ 出荷漏れはありません'), '0件でも本文を作る');
  ok(text.includes('767件を確認'), '今朝の確認件数を出す (動いている証拠になる)');
  ok(text.includes('08/09 12:00'), '締め時刻を明記');
  // 🚨「発送できる状態になった時刻」は取れないので、通知の意味を正確に書く (Codexレビュー High)
  ok(text.includes('より前の注文で、まだ発送されていない'), '判定の意味を正確に書く');
  ok(text.includes('前払い・後払いは締めの後に入金された可能性'), '誤検知しうることを明示');
}
{
  const text = buildMessage({ alerts: [mkAlert()], awaitingPayment: 3, seenToday: 767, stale: false, ctx: mkCtx() });
  ok(text.includes('🚨 出荷漏れの可能性 *1件*'), '件数');
  ok(text.includes('注文番号 1216801633'), '注文番号を出す (QSMは注文番号で検索する)');
  ok(!text.includes('梱包番号'), '梱包番号は出さない (現場が探せない)');
  ok(!text.includes('100'), '梱包番号の値も出さない');
  ok(text.includes('¥1,500'), '金額');
  ok(text.includes('PayPay'), '決済方法');
  ok(text.includes('118時間経過'), '経過時間');
  ok(text.includes('商品A ほか1点'), '明細のまとめ表示');
  ok(text.includes('入金待ちで止まっている注文 3件'), '入金待ちを参考として出す');
}
{
  const text = buildMessage({ alerts: [mkAlert({ orderNos: ['1216505366', '1216505367'] })], awaitingPayment: 0, seenToday: 1, stale: false, ctx: mkCtx() });
  ok(text.includes('注文番号 1216505366, 1216505367 (同梱2件)'), '1梱包に複数注文なら全部並べて同梱と添える');
}
{
  const text = buildMessage({ alerts: [mkAlert({ orderNos: [] })], awaitingPayment: 0, seenToday: 1, stale: false, ctx: mkCtx() });
  ok(text.includes('注文番号 不明 (梱包番号 100)'), '注文番号が取れない行は「不明」と明示し梱包番号を添える (梱包番号を注文番号と偽らない)');
}
{
  const text = buildMessage({ alerts: [mkAlert({ hasTracking: true })], awaitingPayment: 0, seenToday: 1, stale: false, ctx: mkCtx() });
  ok(text.includes('[追跡番号あり]'), '追跡番号があれば印を出す (発送処理途中の可能性)');
}
{
  const many = Array.from({ length: MAX_LINES + 2 }, () => mkAlert());
  const text = buildMessage({ alerts: many, awaitingPayment: 0, seenToday: 1, stale: false, ctx: mkCtx() });
  ok(text.includes('他 2件'), '表示上限の超過を明示');
  ok(text.includes(`*${MAX_LINES + 2}件*`), '総件数は正しい');
}
{
  const long = mkAlert({ itemTitle: 'あ'.repeat(80), itemCount: 3 });
  const text = buildMessage({ alerts: [long], awaitingPayment: 0, seenToday: 1, stale: false, ctx: mkCtx() });
  ok(text.includes('…'), '長い商品名は切る');
  ok(text.includes('ほか2点'), '2件目以降は点数で表す');
}

// ─── exitCodeFor / 終了コードの伝播 ───
section('exitCodeFor');
eq(exitCodeFor({ stale: false }), 0, '正常 → 0');
eq(exitCodeFor({ stale: true }), 1, '判定を見送った → 1 (retry対象)');
eq(exitCodeFor(null), 0, 'null でも落ちない');

section('終了コードの伝播 (自然終了)');
{
  const { execFileSync } = await import('node:child_process');
  const notifyUrl = new URL('../notify-job.js', import.meta.url).href;
  const statusOfFinish = (code) => {
    const script = `const m = await import(${JSON.stringify(notifyUrl)}); m.finish(${code});`;
    try {
      execFileSync(process.execPath, ['--input-type=module', '-e', script], { stdio: 'pipe', env: { ...process.env } });
      return 0;
    } catch (e) {
      return e.status;
    }
  };
  eq(statusOfFinish(0), 0, 'finish(0) → 正常終了');
  eq(statusOfFinish(1), 1, 'finish(1) → e.status=1 (retry対象)');
  const started = Date.now();
  eq(statusOfFinish(1), 1, 'finish(1) を再度');
  ok(Date.now() - started < 4000, '強制終了タイマー (5秒) を待たずに終了する');
}

try { getDB().close(); } catch { /* 既に閉じていれば無視 */ }
fs.rmSync(tmpDir, { recursive: true, force: true });

console.log(`\n${fail === 0 ? '全テスト pass' : `${fail}件 失敗`} (pass=${pass} fail=${fail})`);
process.exit(fail === 0 ? 0 : 1);
